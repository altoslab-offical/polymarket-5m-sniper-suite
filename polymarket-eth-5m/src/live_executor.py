"""
Polymarket CLOB V2 Live Execution Engine (`pUSD` / `py-clob-client-v2`).
Strictly uses FAK (Fill-And-Kill) limit orders with hard price caps and a daily loss circuit breaker.
"""
from dataclasses import dataclass
import math
import os
from pathlib import Path
import sys
import time
from typing import Optional, Tuple

from dotenv import load_dotenv

_SHARED_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(_SHARED_ROOT))

from shared_risk import compute_safe_buy_usdc

CLOB_HOST = "https://clob.polymarket.com"
POLYGON_CHAIN_ID = 137


def floor_share_qty(qty: float) -> float:
    """Round share quantity strictly DOWN to 2 decimals so SELL orders never exceed 4/6-decimal wallet balances."""
    if qty <= 0.0:
        return 0.0
    return math.floor((float(qty) + 1e-9) * 100.0) / 100.0


def _is_deterministic_rejection(err_str: str) -> bool:
    """Return True when an error clearly proves the matching engine rejected the order."""
    low = err_str.lower()
    return any(
        tok in low
        for tok in (
            "not enough balance",
            "no orders found to match",
            "invalid order",
            "status_code=400",
            "status_code=401",
            "status_code=403",
            "fok",
            "min size",
        )
    )


def _parse_trade_ts(raw_ts: object) -> float:
    """Normalize CLOB trade timestamp (seconds or milliseconds) to epoch seconds."""
    if raw_ts is None:
        return 0.0
    try:
        val = float(raw_ts)
        return val / 1000.0 if val > 1e11 else val
    except (TypeError, ValueError):
        return 0.0


@dataclass
class LiveFillResult:
    filled: bool
    filled_shares: float
    avg_price: float
    order_id: str = ""
    error: Optional[str] = None
    latency_ms: float = 0.0


def parse_clob_fill_response(resp: object, requested_price: float) -> LiveFillResult:
    """Parse CLOB V2 post_order / get_order JSON payload into normalized fill shares & price."""
    if not isinstance(resp, dict):
        return LiveFillResult(False, 0.0, requested_price, error=f"Unexpected response: {resp}")

    if resp.get("errorMsg"):
        return LiveFillResult(False, 0.0, requested_price, error=str(resp.get("errorMsg")))

    order_id = str(resp.get("orderID") or resp.get("id") or "")
    # `takingAmount` / `makingAmount` or `size_matched` depending on endpoint
    size_matched = resp.get("size_matched")
    if size_matched is not None:
        try:
            sh = float(size_matched)
            px = float(resp.get("price") or requested_price)
            return LiveFillResult(sh > 0.0, sh, px, order_id=order_id)
        except (TypeError, ValueError) as exc:
            sys.stderr.write(f"[CLOB PARSE WARN] invalid size_matched={size_matched!r}: {exc}\n")

    making_raw = resp.get("makingAmount")
    taking_raw = resp.get("takingAmount")
    try:
        making = float(making_raw) if making_raw is not None else 0.0
        taking = float(taking_raw) if taking_raw is not None else 0.0
    except (TypeError, ValueError):
        making, taking = 0.0, 0.0

    # Polymarket V2 amounts may be 6-decimal base units (>= 1000) or human floats
    if taking > 0.0 and making > 0.0:
        if taking > 1000.0 or making > 1000.0:
            taking /= 1e6
            making /= 1e6
        # For BUY: making=pUSD spent, taking=shares received; for SELL: making=shares sold, taking=pUSD
        if requested_price > 0 and abs((making / taking) - requested_price) <= abs((taking / making) - requested_price):
            shares = taking
            avg_px = making / taking
        else:
            shares = making
            avg_px = taking / making
        return LiveFillResult(shares > 0.0, round(shares, 4), round(avg_px, 4), order_id=order_id)

    status = str(resp.get("status") or "").lower()
    if status in ("matched", "filled"):
        return LiveFillResult(True, 0.0, requested_price, order_id=order_id)
    return LiveFillResult(False, 0.0, requested_price, order_id=order_id)


class PolymarketLiveExecutor:
    """Live CLOB V2 execution wrapper with daily loss circuit breaker."""

    def __init__(
        self,
        env_path: Optional[Path] = None,
        max_daily_loss_usdc: float = 25.0,
    ) -> None:
        from py_clob_client_v2 import AssetType, BalanceAllowanceParams, ClobClient, TradeParams

        resolved_env = env_path or (Path(__file__).resolve().parent.parent / ".env")
        if not resolved_env.exists():
            raise FileNotFoundError(f"找不到 .env 設定檔: {resolved_env}")
        load_dotenv(dotenv_path=resolved_env)

        private_key = (os.getenv("POLY_PRIVATE_KEY") or "").strip()
        funder = (os.getenv("POLY_FUNDER_ADDRESS") or "").strip()
        sig_type = int((os.getenv("POLY_SIGNATURE_TYPE") or "2").strip())
        if not private_key:
            raise ValueError(".env 內缺少 POLY_PRIVATE_KEY")
        if not private_key.startswith("0x"):
            private_key = "0x" + private_key

        self.sig_type = sig_type
        self.max_daily_loss_usdc = max(5.0, float(max_daily_loss_usdc))
        self.daily_pnl_usdc: float = 0.0
        self.circuit_broken: bool = False
        self._day_utc: int = int(time.time() // 86400)
        self._tick_size_cache: dict[str, str] = {}
        self._cond_balance_cache: dict[str, float] = {}
        self._warmed_tokens: set[str] = set()
        self._last_keepalive_ts: float = 0.0
        self._reconcile_delay_sec: float = 0.6

        # Extend py-clob-client-v2 HTTP/2 keepalive from default 5s -> 60s with tight order timeouts
        try:
            import httpx
            from py_clob_client_v2.http_helpers import helpers as _clob_helpers

            _clob_helpers._http_client = httpx.Client(
                http2=True,
                limits=httpx.Limits(max_keepalive_connections=10, max_connections=20, keepalive_expiry=60.0),
                timeout=httpx.Timeout(connect=2.0, read=3.0, write=3.0, pool=2.0),
            )
        except Exception as exc:
            sys.stderr.write(f"[CLOB INIT WARN] httpx tuning skipped: {exc}\n")

        self.client = ClobClient(
            host=CLOB_HOST,
            chain_id=POLYGON_CHAIN_ID,
            key=private_key,
            signature_type=sig_type,
            funder=funder or None,
        )
        try:
            creds = self.client.derive_api_key()
        except Exception:
            creds = self.client.create_or_derive_api_key()
        self.client.set_api_creds(creds)

        # Sync collateral balance cache on startup
        self._AssetType = AssetType
        self._BalanceAllowanceParams = BalanceAllowanceParams
        self._TradeParams = TradeParams
        try:
            self.client.update_balance_allowance(
                params=BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=sig_type)
            )
        except Exception as exc:
            sys.stderr.write(f"[CLOB INIT WARN] collateral sync failed: {exc}\n")

    def get_pusd_balance(self) -> float:
        info = self.client.get_balance_allowance(
            params=self._BalanceAllowanceParams(
                asset_type=self._AssetType.COLLATERAL,
                signature_type=self.sig_type,
            )
        )
        return float(info.get("balance", 0)) / 1e6

    def record_realized_pnl(self, pnl_usdc: float) -> None:
        cur_day = int(time.time() // 86400)
        if cur_day != self._day_utc:
            self._day_utc = cur_day
            self.daily_pnl_usdc = 0.0
            self.circuit_broken = False
        self.daily_pnl_usdc += pnl_usdc
        if self.daily_pnl_usdc <= -self.max_daily_loss_usdc:
            self.circuit_broken = True

    def _get_tick_size_cached(self, token_id: str) -> str:
        if token_id not in self._tick_size_cache:
            try:
                self._tick_size_cache[token_id] = self.client.get_tick_size(token_id)
            except Exception:
                self._tick_size_cache[token_id] = "0.01"
        return self._tick_size_cache[token_id]

    def warmup_tokens(self, *token_ids: Optional[str]) -> None:
        """
        Pre-populate py-clob-client-v2 internal caches (tick_size, condition_id, market_info, fee_rate, version)
        during the early WAIT phase so live sniper entries and Maker exits fire in 1 warm HTTP/2 POST!
        """
        from py_clob_client_v2 import (
            MarketOrderArgs,
            OrderArgs,
            OrderType,
            PartialCreateOrderOptions,
            Side,
        )

        for tok in token_ids:
            if not tok or tok in self._warmed_tokens:
                continue
            try:
                ts = self._get_tick_size_cached(tok)
                opts = PartialCreateOrderOptions(tick_size=ts)
                # Dry-run build ONLY (never posted!) populates __token_condition_map, __fee_infos & __cached_version
                self.client.create_market_order(
                    MarketOrderArgs(
                        token_id=tok,
                        amount=5.0,
                        side=Side.BUY,
                        price=0.50,
                        order_type=OrderType.FAK,
                    ),
                    options=opts,
                )
                self.client.create_order(
                    OrderArgs(
                        token_id=tok,
                        price=0.99,
                        size=5.0,
                        side=Side.SELL,
                    ),
                    options=opts,
                )
                self._warmed_tokens.add(tok)
                self._last_keepalive_ts = time.time()
            except Exception as exc:
                sys.stderr.write(f"[WARMUP WARN] token={tok[:10]}: {exc}\n")
        if len(self._warmed_tokens) > 40:
            self._warmed_tokens.clear()

    def keepalive_if_needed(self, interval_sec: float = 25.0) -> None:
        """Keep HTTP/2 TLS socket to clob.polymarket.com hot during the late-window sniper zone."""
        now = time.time()
        if (now - self._last_keepalive_ts) < interval_sec:
            return
        self._last_keepalive_ts = now
        try:
            self.client.get_ok()
        except Exception as exc:
            sys.stderr.write(f"[KEEPALIVE WARN] get_ok failed: {exc}\n")

    def sync_conditional_balance(self, token_id: str) -> float:
        """Sync & cache conditional token wallet balance (floored to 2 decimals) so exits fire in 1 RTT."""
        try:
            params = self._BalanceAllowanceParams(
                asset_type=self._AssetType.CONDITIONAL,
                token_id=token_id,
                signature_type=self.sig_type,
            )
            self.client.update_balance_allowance(params=params)
            bal_info = self.client.get_balance_allowance(params=params)
            avail = floor_share_qty(float(bal_info.get("balance", 0)) / 1e6)
            if avail > 0.0:
                self._cond_balance_cache[token_id] = avail
            return avail
        except Exception:
            return 0.0

    def lookup_recent_buy_fill(self, token_id: str, since_ts: float) -> Tuple[float, float]:
        """Query CLOB recent BUY trades for `token_id` since `since_ts` and return (shares, avg_price)."""
        trade_params_cls = getattr(self, "_TradeParams", None)
        try:
            if trade_params_cls is not None:
                params = trade_params_cls(asset_id=token_id, after=max(0, int(since_ts - 5)))
                trades = self.client.get_trades(params=params, only_first_page=True)
            else:
                trades = self.client.get_trades()
        except Exception:
            return 0.0, 0.0

        if not isinstance(trades, list):
            return 0.0, 0.0

        total_sh = 0.0
        total_cost = 0.0
        for tr in trades:
            if not isinstance(tr, dict):
                continue
            aid = str(tr.get("asset_id") or "")
            if aid and aid != token_id:
                continue
            if str(tr.get("side") or "BUY").upper() != "BUY":
                continue
            tr_ts = _parse_trade_ts(tr.get("match_time") or tr.get("timestamp") or tr.get("created_at"))
            if tr_ts > 0.0 and tr_ts < (since_ts - 5.0):
                continue
            try:
                sh = float(tr.get("size") or tr.get("makingAmount") or 0.0)
                px = float(tr.get("price") or 0.0)
            except (TypeError, ValueError):
                continue
            if sh > 0.0:
                total_sh += sh
                if px > 0.0:
                    total_cost += sh * px

        if total_sh <= 0.0:
            return 0.0, 0.0
        avg_px = (total_cost / total_sh) if total_cost > 0.0 else 0.0
        return round(total_sh, 4), round(avg_px, 4)

    def reconcile_entry_after_error(
        self,
        token_id: str,
        fallback_price: float,
        since_ts: float,
        pre_balance: float = 0.0,
        err_name: str = "Timeout",
    ) -> Optional[LiveFillResult]:
        """Check conditional token balance and get_trades within ~1.2s after an ambiguous entry error."""
        delay_sec = max(0.0, float(getattr(self, "_reconcile_delay_sec", 0.6)))
        for _ in range(2):
            if delay_sec > 0.0:
                time.sleep(delay_sec)
            tr_sh, tr_px = self.lookup_recent_buy_fill(token_id, since_ts)
            bal_now = self.sync_conditional_balance(token_id)
            delta_bal = floor_share_qty(max(0.0, bal_now - max(0.0, pre_balance)))
            if tr_sh >= 0.1 or delta_bal >= 0.1:
                eff_sh = tr_sh if tr_sh >= 0.1 else delta_bal
                eff_px = tr_px if (tr_sh >= 0.1 and tr_px > 0.0) else fallback_price
                return LiveFillResult(
                    filled=True,
                    filled_shares=round(eff_sh, 4),
                    avg_price=round(eff_px, 4),
                    order_id="RECONCILED_POSITION",
                    error=f"RECONCILED_AFTER_{err_name}",
                )
        return None

    def _verify_fill_if_needed(
        self,
        res: LiveFillResult,
        fallback_price: float,
        est_shares: float,
    ) -> LiveFillResult:
        """Query get_order if post_order omitted inline fill amounts; never return 0 shares on a matched order."""
        if res.order_id and res.filled_shares <= 0.0:
            for attempt in range(2):
                if attempt == 1:
                    time.sleep(0.15)
                try:
                    detail = self.client.get_order(res.order_id)
                    res2 = parse_clob_fill_response(detail, fallback_price)
                    if res2.filled_shares > 0.0:
                        return res2
                    if not res.filled and not res2.filled:
                        break
                    if res2.filled:
                        res = res2
                except Exception:
                    break
        if res.filled and res.filled_shares <= 0.0 and est_shares > 0.0:
            return LiveFillResult(
                True,
                floor_share_qty(est_shares),
                fallback_price,
                order_id=res.order_id,
            )
        return res

    def _compute_entry_terms(
        self,
        ask_price: float,
        shares: float,
        max_price: float,
        p_true: float,
        fee_rate: float,
        available_pusd: float,
    ) -> Tuple[float, float]:
        """Compute EV-guarded limit cap and minimum-5-share-safe pUSD order notional."""
        from .fees import taker_fee_per_share

        ask_f = float(ask_price)
        base_cand = round(min(max_price, ask_f + 0.01), 2)
        limit_px = base_cand
        if p_true > 0.0 and base_cand > 0.82 + 1e-9:
            if (p_true - base_cand - taker_fee_per_share(base_cand, fee_rate)) < 0.045:
                limit_px = round(min(0.82, ask_f), 2)

        if p_true > 0.0:
            for extra in (0.04, 0.03, 0.02, 0.01):
                cand = round(min(max_price, ask_f + extra), 2)
                min_ev = 0.045 if cand > 0.82 + 1e-9 else 0.035
                if (p_true - cand - taker_fee_per_share(cand, fee_rate)) >= min_ev:
                    limit_px = cand
                    break
        if float(shares) >= 5.0:
            usdc_amount = compute_safe_buy_usdc(
                shares=shares,
                ask_price=ask_price,
                limit_price=limit_px,
                available_pusd=available_pusd,
            )
        else:
            usdc_amount = round(float(shares) * ask_f, 2)
        return limit_px, usdc_amount

    def execute_entry(
        self,
        token_id: str,
        ask_price: float,
        shares: float,
        max_price: float = 0.84,
        p_true: float = 0.0,
        fee_rate: float = 0.07,
        available_pusd: float = 0.0,
    ) -> LiveFillResult:
        """Submit an EV-capped FAK buy order and reconcile ambiguous network/timeout failures."""
        from py_clob_client_v2 import MarketOrderArgs, OrderType, PartialCreateOrderOptions, Side

        t0 = time.perf_counter()
        if self.circuit_broken:
            return LiveFillResult(
                False, 0.0, ask_price, error="DAILY_LOSS_CIRCUIT_BREAKER_ACTIVE",
                latency_ms=round((time.perf_counter() - t0) * 1000.0, 1),
            )

        limit_px, usdc_amount = self._compute_entry_terms(
            ask_price, shares, max_price, p_true, fee_rate, available_pusd
        )
        if usdc_amount < 1.0 or limit_px <= 0.01:
            return LiveFillResult(
                False, 0.0, limit_px, error="Invalid order size or price",
                latency_ms=round((time.perf_counter() - t0) * 1000.0, 1),
            )

        tick_size = self._get_tick_size_cached(token_id)
        entry_start_ts = time.time()
        pre_cond_bal = self._cond_balance_cache.get(token_id, 0.0)

        try:
            resp = self.client.create_and_post_market_order(
                order_args=MarketOrderArgs(
                    token_id=token_id, amount=usdc_amount, side=Side.BUY, price=limit_px, order_type=OrderType.FAK,
                ),
                options=PartialCreateOrderOptions(tick_size=tick_size),
                order_type=OrderType.FAK,
            )
            res = parse_clob_fill_response(resp, limit_px)
            res = self._verify_fill_if_needed(res, limit_px, est_shares=usdc_amount / limit_px)
            if res.filled and res.filled_shares > 0.0:
                self._cond_balance_cache.pop(token_id, None)
            res.latency_ms = round((time.perf_counter() - t0) * 1000.0, 1)
            return res
        except Exception as exc:
            err_str = str(exc)
            if "not enough balance" in err_str.lower():
                try:
                    self.client.update_balance_allowance(
                        params=self._BalanceAllowanceParams(
                            asset_type=self._AssetType.COLLATERAL, signature_type=self.sig_type,
                        )
                    )
                except Exception:
                    sys.stderr.write("[CLOB WARN] collateral allowance refresh failed\n")
                err_str = f"可用 pUSD 餘額不足（若有到期獲勝注單尚未領取，請至 Polymarket 點擊 Claim 領回 pUSD）| {err_str}"
            elif not _is_deterministic_rejection(err_str):
                reconciled = self.reconcile_entry_after_error(
                    token_id=token_id, fallback_price=limit_px, since_ts=entry_start_ts,
                    pre_balance=pre_cond_bal, err_name=type(exc).__name__,
                )
                if reconciled is not None:
                    self._cond_balance_cache.pop(token_id, None)
                    reconciled.latency_ms = round((time.perf_counter() - t0) * 1000.0, 1)
                    return reconciled
            return LiveFillResult(
                False, 0.0, limit_px, error=err_str,
                latency_ms=round((time.perf_counter() - t0) * 1000.0, 1),
            )

    def _update_cond_cache_after_exit(self, token_id: str, sell_qty: float, filled_shares: float) -> None:
        rem = floor_share_qty(max(0.0, sell_qty - filled_shares))
        if rem >= 0.1:
            self._cond_balance_cache[token_id] = rem
        else:
            self._cond_balance_cache.pop(token_id, None)

    def post_maker_tp_sell(
        self,
        token_id: str,
        shares: float,
        price: float = 0.99,
    ) -> LiveFillResult:
        """Post a GTC post-only Maker take-profit limit sell order (0% taker fee)."""
        from py_clob_client_v2 import OrderArgs, OrderType, PartialCreateOrderOptions, Side

        t0 = time.perf_counter()
        target_px = round(float(price), 2)
        cached_avail = self._cond_balance_cache.get(token_id, 0.0)
        sell_qty = floor_share_qty(min(float(shares), float(cached_avail)) if cached_avail > 0.0 else 0.0)
        if sell_qty < 5.0:
            return LiveFillResult(
                False,
                0.0,
                target_px,
                error="Conditional balance below 5-share GTC minimum",
                latency_ms=round((time.perf_counter() - t0) * 1000.0, 1),
            )

        tick_size = self._get_tick_size_cached(token_id)
        try:
            resp = self.client.create_and_post_order(
                order_args=OrderArgs(
                    token_id=token_id,
                    price=target_px,
                    size=sell_qty,
                    side=Side.SELL,
                ),
                options=PartialCreateOrderOptions(tick_size=tick_size),
                order_type=OrderType.GTC,
                post_only=True,
            )
            parsed = parse_clob_fill_response(resp, target_px)
            if parsed.error:
                parsed.latency_ms = round((time.perf_counter() - t0) * 1000.0, 1)
                return parsed
            if parsed.filled and parsed.filled_shares > 0.0:
                self._update_cond_cache_after_exit(token_id, sell_qty, parsed.filled_shares)
                parsed.latency_ms = round((time.perf_counter() - t0) * 1000.0, 1)
                return parsed
            return LiveFillResult(
                filled=False,
                filled_shares=0.0,
                avg_price=target_px,
                order_id=parsed.order_id,
                latency_ms=round((time.perf_counter() - t0) * 1000.0, 1),
            )
        except Exception as exc:
            return LiveFillResult(
                False,
                0.0,
                target_px,
                error=str(exc),
                latency_ms=round((time.perf_counter() - t0) * 1000.0, 1),
            )

    def check_maker_order_status(
        self,
        order_id: str,
        price: float = 0.99,
        token_id: str = "",
        posted_qty: float = 0.0,
    ) -> LiveFillResult:
        """Query CLOB order status for a resting Maker TP sell and update balance cache if filled."""
        target_px = round(float(price), 2)
        if not order_id:
            return LiveFillResult(False, 0.0, target_px, error="Missing order_id")
        try:
            detail = self.client.get_order(order_id)
            res = parse_clob_fill_response(detail, target_px)
            if res.filled_shares > 0.0:
                tok = token_id or (str(detail.get("asset_id") or "") if isinstance(detail, dict) else "")
                if tok:
                    base_qty = posted_qty if posted_qty > 0.0 else self._cond_balance_cache.get(tok, 0.0)
                    if base_qty > 0.0:
                        self._update_cond_cache_after_exit(tok, base_qty, res.filled_shares)
            return res
        except Exception as exc:
            return LiveFillResult(False, 0.0, target_px, order_id=order_id, error=str(exc))

    def cancel_order_safe(self, order_id: str) -> bool:
        """Cancel a resting CLOB order safely without raising on already-filled or expired orders."""
        if not order_id:
            return False
        try:
            self.client.cancel_orders([order_id])
            return True
        except Exception as exc:
            sys.stderr.write(f"[CLOB CANCEL WARN] order={order_id[:16]}: {exc}\n")
            return False

    def execute_exit(
        self,
        token_id: str,
        bid_price: float,
        shares: float,
        slippage_cents: float = 0.03,
        min_limit_px: float = 0.01,
    ) -> LiveFillResult:
        """Submit an FAK sell exit with cached conditional balance lookup and 1-shot stop-loss widen."""
        from py_clob_client_v2 import MarketOrderArgs, OrderType, PartialCreateOrderOptions, Side

        t0 = time.perf_counter()
        cached_avail = self._cond_balance_cache.get(token_id, 0.0) or self.sync_conditional_balance(token_id)
        sell_qty = floor_share_qty(min(shares, cached_avail) if cached_avail > 0.0 else shares)
        if sell_qty < 0.1:
            return LiveFillResult(
                False, 0.0, bid_price, error="No conditional balance to sell",
                latency_ms=round((time.perf_counter() - t0) * 1000.0, 1),
            )

        floor_px = float(min_limit_px)
        limit_px = round(max(floor_px, float(bid_price) - slippage_cents), 2)
        tick_size = self._get_tick_size_cached(token_id)

        for attempt in range(2):
            try:
                resp = self.client.create_and_post_market_order(
                    order_args=MarketOrderArgs(
                        token_id=token_id, amount=sell_qty, side=Side.SELL, price=limit_px, order_type=OrderType.FAK,
                    ),
                    options=PartialCreateOrderOptions(tick_size=tick_size),
                    order_type=OrderType.FAK,
                )
                res = self._verify_fill_if_needed(parse_clob_fill_response(resp, limit_px), limit_px, est_shares=sell_qty)
                if attempt == 0 and not res.filled and res.error and "no orders found to match" in res.error.lower() and slippage_cents >= 0.05:
                    limit_px = round(max(floor_px, float(bid_price) - (slippage_cents + 0.05)), 2)
                    continue
                if res.filled and res.filled_shares > 0.0:
                    self._update_cond_cache_after_exit(token_id, sell_qty, res.filled_shares)
                res.latency_ms = round((time.perf_counter() - t0) * 1000.0, 1)
                return res
            except Exception as exc:
                err_str = str(exc)
                if attempt == 0 and "balance" in err_str.lower():
                    fresh_avail = self.sync_conditional_balance(token_id)
                    if fresh_avail >= 0.1:
                        sell_qty = floor_share_qty(min(float(shares), fresh_avail))
                        continue
                elif attempt == 0 and "no orders found to match" in err_str.lower() and slippage_cents >= 0.05:
                    limit_px = round(max(floor_px, float(bid_price) - (slippage_cents + 0.05)), 2)
                    continue
                return LiveFillResult(
                    False, 0.0, limit_px, error=err_str,
                    latency_ms=round((time.perf_counter() - t0) * 1000.0, 1),
                )
        return LiveFillResult(
            False, 0.0, limit_px, error="Exit retry exhausted",
            latency_ms=round((time.perf_counter() - t0) * 1000.0, 1),
        )
