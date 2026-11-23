"""
Real-time Asynchronous Market Data Hub:
1. Chainlink XRP/USD Oracle Engine (`ChainlinkOracleFeed`) -> Authoritative source for
   Strike Price (S_0), Live Oracle Reference Price (S_t), and 60s Chainlink TWAP.
2. Binance + Coinbase WebSockets WITH automatic HTTPS REST fallback (`requests.Session`) ->
   Guarantees `cex_lead_price`, `oracle_gap_bps`, and `OBI` work even if `websockets` is missing!
3. Polymarket Gamma API + CLOB REST/WebSocket -> Active `xrp-updown-5m-{ts}` tokens, Best Bid/Ask,
   AND official Gamma `outcomePrices` settlement verification!
"""
import asyncio
from collections import deque
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime
import json
import math
from statistics import NormalDist
import time
from typing import Any, Deque, Dict, List, Optional, Tuple

import requests

from .chainlink_oracle import ChainlinkOracleFeed, has_websockets_installed
from .pricing_model import RollingMicrostructureTracker
from .strategy import OrderBookTop

_STD_NORM = NormalDist(0.0, 1.0)


GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
CLOB_BOOK_URL = "https://clob.polymarket.com/book"
POLY_CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BINANCE_WS_URL = "wss://stream.binance.com:9443/stream?streams=xrpusdt@aggTrade/xrpusdt@bookTicker"
BINANCE_REST_BOOK_URL = "https://data-api.binance.vision/api/v3/ticker/bookTicker?symbol=XRPUSDT"
BINANCE_REST_KLINES_URL = "https://data-api.binance.vision/api/v3/klines?symbol=XRPUSDT&interval=1s"
COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
COINBASE_REST_TICKER_URL = "https://api.exchange.coinbase.com/products/XRP-USD/ticker"


def parse_json_or_list(val: Any) -> list:
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _levels_to_map(levels: Any) -> Dict[float, float]:
    """Convert CLOB REST/WS level list into {rounded_price: size} keeping valid prices inside (0, 1)."""
    out: Dict[float, float] = {}
    for lvl in levels or ():
        if not isinstance(lvl, dict):
            continue
        try:
            px = round(float(lvl.get("price") or 0.0), 4)
            sz = float(lvl.get("size") or 0.0)
        except (TypeError, ValueError):
            continue
        if 0.0 < px < 1.0 and sz > 0.0:
            out[px] = sz
    return out


def extract_best_bid_ask(book_payload: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    """
    Safely compute best bid (highest bid price) and best ask (lowest ask price)
    regardless of how Polymarket CLOB sorts the arrays.
    """
    bids_map = _levels_to_map(book_payload.get("bids"))
    asks_map = _levels_to_map(book_payload.get("asks"))
    best_bid = max(bids_map) if bids_map else None
    best_ask = min(asks_map) if asks_map else None
    return best_bid, best_ask


def _sweep_vwap(levels: List[Tuple[float, float]], target_shares: float, max_slip_from_top: float = 0.03) -> Optional[float]:
    """
    Compute executable Volume-Weighted Average Price (VWAP) across `levels` [(price, size), ...]
    already sorted best-first, rejecting dust quotes (< 40% of target_shares within 3c of top).
    """
    if not levels:
        return None
    top_p = levels[0][0]
    # Allow settled/near-settled end-of-window quotes (>= 0.98 or <= 0.02) without strict size filter
    if top_p >= 0.98 or top_p <= 0.02:
        return top_p

    filled = 0.0
    cost = 0.0
    for p, sz in levels:
        if abs(p - top_p) > max_slip_from_top:
            break
        take = min(sz, target_shares - filled)
        filled += take
        cost += take * p
        if filled >= target_shares - 1e-6:
            break

    min_req = max(5.0, 0.40 * target_shares)
    if filled < min_req:
        return None
    return round(cost / filled, 4)


def extract_executable_bid_ask_from_maps(
    bids_map: Dict[float, float],
    asks_map: Dict[float, float],
    target_usdc: float = 25.0,
) -> Tuple[Optional[float], Optional[float]]:
    """Compute depth-aware VWAP Bid/Ask directly from live L2 price->size dictionaries."""
    if target_usdc <= 0:
        best_bid = max(bids_map) if bids_map else None
        best_ask = min(asks_map) if asks_map else None
        if best_bid is not None and best_ask is not None and best_bid >= best_ask:
            return None, None
        return best_bid, best_ask

    bids = sorted(((p, sz) for p, sz in bids_map.items() if sz > 0.0 and 0.0 < p < 1.0), key=lambda x: x[0], reverse=True)
    asks = sorted(((p, sz) for p, sz in asks_map.items() if sz > 0.0 and 0.0 < p < 1.0), key=lambda x: x[0])

    vwap_ask = None
    if asks:
        est_shares = target_usdc / max(0.20, asks[0][0])
        vwap_ask = _sweep_vwap(asks, target_shares=est_shares)

    vwap_bid = None
    if bids:
        est_shares = target_usdc / max(0.20, min(0.85, bids[0][0]))
        vwap_bid = _sweep_vwap(bids, target_shares=est_shares)

    if vwap_bid is not None and vwap_ask is not None and vwap_bid >= vwap_ask:
        return None, None
    return vwap_bid, vwap_ask


def extract_executable_bid_ask(
    book_payload: Dict[str, Any],
    target_usdc: float = 25.0,
) -> Tuple[Optional[float], Optional[float]]:
    """
    Depth-aware VWAP Bid/Ask for a `target_usdc` order (~30-35 shares), preventing paper-trading
    illusions from 0.5-share dust orders sitting at the top of the CLOB!
    """
    bids_map = _levels_to_map(book_payload.get("bids"))
    asks_map = _levels_to_map(book_payload.get("asks"))
    return extract_executable_bid_ask_from_maps(bids_map, asks_map, target_usdc=target_usdc)


def fetch_5m_market_metadata(
    window_ts: int,
    session: Optional[requests.Session] = None,
    timeout: float = 4.0,
) -> Optional[Dict[str, Any]]:
    """
    Query Polymarket Gamma API for `xrp-updown-5m-{window_ts}`:
      - Up / Down CLOB token IDs
      - Official `priceToBeat` (when populated)
      - Official settled winner (`resolved_winner` = "UP" or "DOWN" when `outcomePrices` settles)
    """
    slug = f"xrp-updown-5m-{window_ts}"
    http = session or requests
    try:
        resp = http.get(GAMMA_EVENTS_URL, params={"slug": slug}, timeout=timeout)
        if resp.status_code != 200:
            return None
        events = resp.json()
        if not events or not isinstance(events, list):
            return None

        event = events[0]
        markets = event.get("markets") or []
        if not markets:
            return None

        mkt = markets[0]
        outcomes = parse_json_or_list(mkt.get("outcomes"))
        token_ids = parse_json_or_list(mkt.get("clobTokenIds"))
        outcome_prices = parse_json_or_list(mkt.get("outcomePrices"))
        if len(outcomes) < 2 or len(token_ids) < 2:
            return None

        up_token, down_token = None, None
        up_idx, down_idx = 0, 1
        for idx, (label, tid) in enumerate(zip(outcomes, token_ids)):
            norm = str(label).strip().lower()
            if norm in ("up", "yes", "higher"):
                up_token = str(tid)
                up_idx = idx
            elif norm in ("down", "no", "lower"):
                down_token = str(tid)
                down_idx = idx

        if not up_token or not down_token:
            up_token, down_token = str(token_ids[0]), str(token_ids[1])

        meta = event.get("eventMetadata") or mkt.get("eventMetadata") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except json.JSONDecodeError:
                meta = {}
        price_to_beat = (
            meta.get("priceToBeat")
            or meta.get("startPrice")
            or mkt.get("priceToBeat")
            or mkt.get("startPrice")
        )
        official_strike = float(price_to_beat) if price_to_beat is not None else None
        final_price_raw = meta.get("finalPrice") or mkt.get("finalPrice")
        official_final_price = float(final_price_raw) if final_price_raw is not None else None

        resolved_winner: Optional[str] = None
        if len(outcome_prices) >= 2:
            try:
                p_up_out = float(outcome_prices[up_idx])
                p_dn_out = float(outcome_prices[down_idx])
                if p_up_out >= 0.98 and p_dn_out <= 0.02:
                    resolved_winner = "UP"
                elif p_dn_out >= 0.98 and p_up_out <= 0.02:
                    resolved_winner = "DOWN"
            except (TypeError, ValueError):
                pass

        return {
            "window_ts": window_ts,
            "slug": slug,
            "question": mkt.get("question", slug),
            "up_token": up_token,
            "down_token": down_token,
            "official_strike": official_strike,
            "official_final_price": official_final_price,
            "resolved_winner": resolved_winner,
        }
    except Exception:
        return None


def _parse_book_ts_sec(payload: Dict[str, Any]) -> float:
    raw = payload.get("timestamp")
    if raw is None:
        return 0.0
    try:
        v = float(raw)
        return v / 1000.0 if v > 1e11 else v
    except (TypeError, ValueError):
        return 0.0


def fetch_clob_book_raw(
    token_id: str,
    session: Optional[requests.Session] = None,
    timeout: float = 2.5,
) -> Optional[Dict[str, Any]]:
    http = session or requests
    try:
        resp = http.get(
            CLOB_BOOK_URL,
            params={"token_id": token_id},
            headers={"Cache-Control": "no-cache"},
            timeout=timeout,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def fetch_clob_book_top(
    token_id: str,
    session: Optional[requests.Session] = None,
    timeout: float = 2.5,
    target_usdc: float = 25.0,
) -> Tuple[Optional[float], Optional[float], float]:
    data = fetch_clob_book_raw(token_id, session=session, timeout=timeout)
    if data is None:
        return None, None, 0.0
    bid, ask = extract_executable_bid_ask(data, target_usdc=target_usdc)
    return bid, ask, _parse_book_ts_sec(data)


@dataclass
class FeedHealthStats:
    """Rolling latency percentiles and reconnect counters for real-time observability."""

    clob_ws_book_msgs: int = 0
    clob_ws_price_change_msgs: int = 0
    clob_rest_fallbacks: int = 0
    clob_ws_reconnects: int = 0
    binance_ws_reconnects: int = 0
    coinbase_ws_reconnects: int = 0
    stale_book_skips: int = 0
    hot_book_age_ms: Deque[float] = field(default_factory=lambda: deque(maxlen=2000))
    hot_book_lag_ms: Deque[float] = field(default_factory=lambda: deque(maxlen=2000))
    rollover_first_book_ms: Deque[float] = field(default_factory=lambda: deque(maxlen=100))

    @staticmethod
    def percentile(values: Deque[float], q: float) -> Optional[float]:
        if not values:
            return None
        ordered = sorted(values)
        idx = int(round((len(ordered) - 1) * max(0.0, min(1.0, q))))
        return round(float(ordered[idx]), 1)

    def record_hot_sample(self, age_ms: float, lag_ms: float) -> None:
        self.hot_book_age_ms.append(max(0.0, float(age_ms)))
        self.hot_book_lag_ms.append(max(0.0, float(lag_ms)))

    def snapshot_dict(self, chainlink: Any) -> Dict[str, Any]:
        return {
            "ws_book": self.clob_ws_book_msgs,
            "ws_pc": self.clob_ws_price_change_msgs,
            "rest_fallbacks": self.clob_rest_fallbacks,
            "clob_reconn": self.clob_ws_reconnects,
            "rtds_reconn": int(getattr(chainlink, "rtds_reconnects", 0)),
            "polybolt_reconn": int(getattr(chainlink, "polybolt_reconnects", 0)),
            "bn_reconn": self.binance_ws_reconnects,
            "cb_reconn": self.coinbase_ws_reconnects,
            "stale_skips": self.stale_book_skips,
            "book_age_p50_ms": self.percentile(self.hot_book_age_ms, 0.50),
            "book_age_p99_ms": self.percentile(self.hot_book_age_ms, 0.99),
            "book_lag_p50_ms": self.percentile(self.hot_book_lag_ms, 0.50),
            "book_lag_p99_ms": self.percentile(self.hot_book_lag_ms, 0.99),
            "rollover_p99_ms": self.percentile(self.rollover_first_book_ms, 0.99),
            "clock_raw_ms": float(getattr(chainlink, "raw_clock_offset_ms", 0.0)),
            "clock_eff_ms": float(getattr(chainlink, "effective_clock_offset_ms", 0.0)),
        }


@dataclass
class LiveFeedState:
    binance_price: Optional[float] = None
    binance_ts: float = 0.0
    coinbase_price: Optional[float] = None
    coinbase_ts: float = 0.0
    _ewma_bn_cb_ratio: float = 1.0
    _ratio_initialized: bool = False

    current_window_ts: int = 0
    slug: str = ""
    up_token: Optional[str] = None
    down_token: Optional[str] = None
    strike_price: Optional[float] = None
    strike_source: str = "UNSET"
    _initial_cl_strike: Optional[float] = None
    _implied_strike_samples: List[float] = field(default_factory=list)

    book: OrderBookTop = None  # type: ignore
    up_book_ts: float = 0.0
    down_book_ts: float = 0.0
    up_book_local_ts: float = 0.0
    down_book_local_ts: float = 0.0
    up_book_lag_sec: float = 0.0
    down_book_lag_sec: float = 0.0
    last_book_lag_sec: float = 0.0
    last_ws_book_local_ts: float = 0.0
    last_book_update_local_ts: float = 0.0
    _up_bids: Dict[float, float] = field(default_factory=dict)
    _up_asks: Dict[float, float] = field(default_factory=dict)
    _down_bids: Dict[float, float] = field(default_factory=dict)
    _down_asks: Dict[float, float] = field(default_factory=dict)
    _up_book_initialized: bool = False
    _down_book_initialized: bool = False
    _window_is_rollover: bool = False
    _window_first_book_recorded: bool = False
    _twap240_sum: float = 0.0
    _twap240_count: int = 0
    _twap240_last_sec: int = -1

    def __post_init__(self) -> None:
        if self.book is None:
            self.book = OrderBookTop()

    def record_twap240_spot(self, now_utc: float, cl_spot: Optional[float]) -> None:
        """Record 1Hz Chainlink spot samples during the final 60s TWAP settling window."""
        if cl_spot is None or cl_spot <= 0.0 or self.current_window_ts <= 0:
            return
        if (now_utc - self.current_window_ts) < 240.0:
            return
        sec_bucket = int(now_utc)
        if sec_bucket != self._twap240_last_sec:
            self._twap240_sum += float(cl_spot)
            self._twap240_count += 1
            self._twap240_last_sec = sec_bucket

    @property
    def realized_twap240(self) -> Tuple[Optional[float], float]:
        """Return (realized_mean, elapsed_samples) collected in `[window_ts + 240, now]`."""
        if self._twap240_count <= 0:
            return None, 0.0
        return self._twap240_sum / self._twap240_count, float(self._twap240_count)

    def calibrate_strike_from_opening_book(
        self,
        cl_spot: Optional[float],
        vol_1s: float,
        elapsed_sec: float,
    ) -> Optional[float]:
        """
        If `POLY_RTDS_CHAINLINK` at t=0s differs from Polymarket's internal `priceToBeat` due to a
        rollover price jump, the opening two-sided CLOB midpoint (`up_mid`) immediately reveals the
        market makers' true Strike. Calibrates `strike_price` when >=3 early samples show a >=$12 gap.
        """
        if (
            self.strike_source in ("GAMMA_OFFICIAL", "OFFICIAL_TWAP60")
            or cl_spot is None
            or self.strike_price is None
            or not (1.5 <= elapsed_sec <= 45.0)
            or len(self._implied_strike_samples) >= 8
        ):
            return None
        if self.book.up_bid is None or self.book.up_ask is None:
            return None
        spread = self.book.up_ask - self.book.up_bid
        if spread <= 0.0 or spread > 0.06:
            return None

        up_mid = 0.5 * (self.book.up_bid + self.book.up_ask)
        if not (0.22 <= up_mid <= 0.78):
            return None

        rem_sec = max(60.0, 300.0 - elapsed_sec)
        eff_vol = max(vol_1s, 0.000035)
        sigma_usd = max(cl_spot * eff_vol * math.sqrt(rem_sec), cl_spot * 0.00035)
        z_mkt = _STD_NORM.inv_cdf(up_mid)
        s_implied = cl_spot - z_mkt * sigma_usd
        self._implied_strike_samples.append(s_implied)

        if len(self._implied_strike_samples) >= 3:
            ordered = sorted(self._implied_strike_samples)
            med_implied = ordered[len(ordered) // 2]
            ref_strike = self._initial_cl_strike or self.strike_price
            if abs(med_implied - ref_strike) >= max(cl_spot * 0.0002, 0.28 * sigma_usd):
                self.strike_price = round(med_implied, 4)
                self.strike_source = "BOOK_CALIBRATED"
                return self.strike_price
        return None

    def update_usdt_basis(self) -> None:
        """Track XRPUSDT / XRP-USD ratio so exchange reconnects never cause a basis jump!"""
        now = time.time()
        if (
            self.coinbase_price
            and self.binance_price
            and self.coinbase_price > 0
            and (now - self.coinbase_ts) < 8.0
            and (now - self.binance_ts) < 8.0
        ):
            r = float(self.binance_price) / float(self.coinbase_price)
            if 0.98 < r < 1.02:
                if not self._ratio_initialized:
                    self._ewma_bn_cb_ratio = r
                    self._ratio_initialized = True
                else:
                    self._ewma_bn_cb_ratio = 0.02 * r + 0.98 * self._ewma_bn_cb_ratio

    @property
    def cex_lead_price(self) -> Optional[float]:
        now = time.time()
        cb_fresh = self.coinbase_price is not None and (now - self.coinbase_ts) < 10.0
        bn_fresh = self.binance_price is not None and (now - self.binance_ts) < 10.0
        bn_usd = (float(self.binance_price) / self._ewma_bn_cb_ratio) if bn_fresh else None

        if cb_fresh and bn_usd is not None:
            return 0.55 * float(self.coinbase_price) + 0.45 * bn_usd
        if cb_fresh:
            return float(self.coinbase_price)
        if bn_usd is not None:
            return bn_usd
        return None


class LiveMarketHub:
    def __init__(self, tracker: RollingMicrostructureTracker, target_usdc: float = 25.0):
        self.tracker = tracker
        self.target_usdc = max(5.0, target_usdc) if target_usdc > 0 else 25.0
        self._wake_event = asyncio.Event()
        self._last_eval_ts: float = 0.0
        self.chainlink = ChainlinkOracleFeed(on_tick_callback=self.notify_wake)
        self.state = LiveFeedState()
        self.health = FeedHealthStats()
        self._stop_event = asyncio.Event()
        self._http = requests.Session()
        self._http.headers.update(
            {
                "User-Agent": "polymarket-5m-sniper/4.6",
                "Accept": "application/json",
            }
        )
        self._prefetched_tokens: Dict[int, Tuple[str, str]] = {}
        self._last_gamma_fetch_ts: float = 0.0
        self._gamma_strike_checks: int = 0
        self._last_clock_sync_ts: float = 0.0

    def notify_wake(self) -> None:
        """Wake the monitor loop immediately on a fresh streaming oracle or orderbook tick."""
        self._wake_event.set()

    async def wait_next_tick(self, timeout_sec: float, min_interval_sec: float = 0.025) -> bool:
        """Wait for the next feed wakeup or `timeout_sec`, throttled by `min_interval_sec`."""
        slept_min = 0.0
        if min_interval_sec > 0.0 and self._last_eval_ts > 0.0:
            rem_min = min_interval_sec - (time.monotonic() - self._last_eval_ts)
            if rem_min > 0.0:
                slept_min = min(rem_min, max(0.0, float(timeout_sec)))
                await asyncio.sleep(slept_min)

        if self._wake_event.is_set():
            self._wake_event.clear()
            self._last_eval_ts = time.monotonic()
            return True

        rem_timeout = max(0.005, float(timeout_sec) - slept_min)
        try:
            await asyncio.wait_for(self._wake_event.wait(), timeout=rem_timeout)
            self._wake_event.clear()
            self._last_eval_ts = time.monotonic()
            return True
        except asyncio.TimeoutError:
            self._last_eval_ts = time.monotonic()
            return False

    def utc_now(self) -> float:
        """Authoritative server-aligned UTC timestamp (immune to local Windows clock drift)."""
        return self.chainlink.utc_now()

    def _sync_clock_from_http_date(self, resp: requests.Response) -> None:
        date_hdr = resp.headers.get("Date")
        if not date_hdr:
            return
        try:
            srv_ts = parsedate_to_datetime(date_hdr).timestamp()
            self.chainlink.update_clock_offset(srv_ts)
        except Exception:
            pass

    def sync_window_boundary(self, now_utc: float) -> None:
        """
        Synchronously transition 5m window state at exact server UTC boundary (t=0.0s) so `strike_price`
        never leaks from the previous window or drifts due to local PC clock skew!
        """
        window_ts = (int(now_utc) // 300) * 300
        if window_ts == self.state.current_window_ts:
            if (
                self.state.strike_source not in ("GAMMA_OFFICIAL", "OFFICIAL_TWAP60")
                and (now_utc - window_ts) <= 20.0
            ):
                twap_s0 = self.chainlink.get_twap_at_or_after_utc(float(window_ts), max_after_sec=5.0)
                if twap_s0 is not None:
                    self.state.strike_price = twap_s0
                    self.state._initial_cl_strike = twap_s0
                    self.state.strike_source = "OFFICIAL_TWAP60"
                    self.state._implied_strike_samples.clear()
                    return
            # Backfill exact UTC boundary price if Chainlink tick arrived a moment after rollover
            if self.state.strike_price is None:
                near_p = self.chainlink.get_price_near_utc(float(window_ts), max_diff_sec=5.0)
                if near_p is not None:
                    self.state.strike_price = near_p
                    self.state._initial_cl_strike = near_p
                    self.state.strike_source = self.chainlink.state.source
            return

        had_prev = self.state.current_window_ts > 0
        self.state.current_window_ts = window_ts
        self.state.slug = f"xrp-updown-5m-{window_ts}"
        self.state.book = OrderBookTop()
        self.state.up_book_ts = 0.0
        self.state.down_book_ts = 0.0
        self.state.up_book_local_ts = 0.0
        self.state.down_book_local_ts = 0.0
        self.state.up_book_lag_sec = 0.0
        self.state.down_book_lag_sec = 0.0
        self.state.last_book_lag_sec = 0.0
        self.state.last_ws_book_local_ts = 0.0
        self.state.last_book_update_local_ts = 0.0
        self.state._up_bids.clear()
        self.state._up_asks.clear()
        self.state._down_bids.clear()
        self.state._down_asks.clear()
        self.state._up_book_initialized = False
        self.state._down_book_initialized = False
        self.state._window_is_rollover = had_prev
        self.state._window_first_book_recorded = False
        self.state._implied_strike_samples.clear()
        self.state._twap240_sum = 0.0
        self.state._twap240_count = 0
        self.state._twap240_last_sec = -1
        self._gamma_strike_checks = 0

        if window_ts in self._prefetched_tokens:
            self.state.up_token, self.state.down_token = self._prefetched_tokens.pop(window_ts)
        else:
            self.state.up_token = None
            self.state.down_token = None

        twap_s0 = self.chainlink.get_twap_at_or_after_utc(float(window_ts), max_after_sec=5.0)
        if twap_s0 is not None:
            self.state.strike_price = twap_s0
            self.state._initial_cl_strike = twap_s0
            self.state.strike_source = "OFFICIAL_TWAP60"
        else:
            near_p = self.chainlink.get_price_near_utc(float(window_ts), max_diff_sec=5.0)
            cl_price = near_p or self.chainlink.state.settlement_reference_price
            if (now_utc - window_ts) < 4.5 and cl_price is not None:
                self.state.strike_price = cl_price
                self.state._initial_cl_strike = cl_price
                self.state.strike_source = self.chainlink.state.source
            else:
                self.state.strike_price = None
                self.state._initial_cl_strike = None
                self.state.strike_source = "WAITING_GAMMA_OR_NEXT_WINDOW"

    def _warmup_binance_1s_vol_once(self) -> int:
        """Fetch up to 1,850 historical Binance 1s closes via REST for 30m vol_1s parity on boot."""
        rows: List[Tuple[int, float]] = []
        try:
            r1 = self._http.get(f"{BINANCE_REST_KLINES_URL}&limit=1000", timeout=3.0)
            c1 = r1.json() if r1.status_code == 200 else []
            if isinstance(c1, list) and c1:
                first_ms = int(c1[0][0])
                for item in c1:
                    if isinstance(item, list) and len(item) >= 5:
                        rows.append((int(item[0]), float(item[4])))
                try:
                    r2 = self._http.get(
                        f"{BINANCE_REST_KLINES_URL}&endTime={first_ms - 1}&limit=850",
                        timeout=3.0,
                    )
                    c2 = r2.json() if r2.status_code == 200 else []
                    if isinstance(c2, list):
                        for item in c2:
                            if isinstance(item, list) and len(item) >= 5:
                                rows.append((int(item[0]), float(item[4])))
                except Exception:
                    pass
        except Exception:
            return 0
        rows.sort(key=lambda pair: pair[0])
        seen_ms = -1
        loaded = 0
        for open_ms, close_px in rows:
            if open_ms <= seen_ms or open_ms <= 0 or close_px <= 0.0:
                continue
            seen_ms = open_ms
            self.tracker.on_price(open_ms / 1000.0, close_px)
            loaded += 1
        return loaded

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._warmup_binance_1s_vol_once)
        tasks = [
            self.chainlink.start(),
            self._cex_http_fallback_loop(),
            self._polymarket_window_and_book_loop(),
        ]
        if has_websockets_installed():
            tasks.extend(
                [
                    self._binance_ws_loop(),
                    self._coinbase_ws_loop(),
                    self._polymarket_clob_ws_loop(),
                ]
            )
        await asyncio.gather(*tasks, return_exceptions=True)

    def stop(self) -> None:
        self._stop_event.set()
        self.chainlink.stop()

    def _record_book_side(
        self,
        side: str,
        bid: Optional[float],
        ask: Optional[float],
        ev_ts: float,
        now_u: float,
        via_ws: bool,
    ) -> None:
        local_ts = time.time()
        lag_sec = max(0.0, now_u - ev_ts) if ev_ts > 0.0 else 0.0
        if side == "UP":
            self.state.book.up_bid, self.state.book.up_ask = bid, ask
            if ev_ts > self.state.up_book_ts:
                self.state.up_book_ts = ev_ts
            self.state.up_book_local_ts = local_ts
            self.state.up_book_lag_sec = lag_sec
            if via_ws and bid is not None and bid >= 0.92 and (self.state.book.down_bid or 0.0) >= 0.20:
                self.state.book.down_bid = None
                self.state.book.down_ask = round(max(0.01, 1.0 - bid), 2)
        else:
            self.state.book.down_bid, self.state.book.down_ask = bid, ask
            if ev_ts > self.state.down_book_ts:
                self.state.down_book_ts = ev_ts
            self.state.down_book_local_ts = local_ts
            self.state.down_book_lag_sec = lag_sec
            if via_ws and bid is not None and bid >= 0.92 and (self.state.book.up_bid or 0.0) >= 0.20:
                self.state.book.up_bid = None
                self.state.book.up_ask = round(max(0.01, 1.0 - bid), 2)

        self.state.last_book_lag_sec = lag_sec
        if via_ws:
            self.state.last_ws_book_local_ts = local_ts
            self.state.last_book_update_local_ts = local_ts
            self.notify_wake()
        elif bid is not None or ask is not None:
            self.state.last_book_update_local_ts = local_ts

        if self.state._window_is_rollover and not self.state._window_first_book_recorded and self.state.current_window_ts > 0:
            self.state._window_first_book_recorded = True
            self.health.rollover_first_book_ms.append(max(0.0, (now_u - self.state.current_window_ts) * 1000.0))

    def apply_clob_book_payload(
        self,
        token_id: str,
        payload: Dict[str, Any],
        via_ws: bool = True,
    ) -> bool:
        """Apply a full L2 `book` snapshot (WS or REST) into incremental maps and recompute VWAP top."""
        if not token_id:
            return False
        if token_id == self.state.up_token:
            side = "UP"
            held_ts = self.state.up_book_ts
        elif token_id == self.state.down_token:
            side = "DOWN"
            held_ts = self.state.down_book_ts
        else:
            return False

        ev_ts = _parse_book_ts_sec(payload)
        now_u = self.utc_now()
        if via_ws and ev_ts > 0.0 and (now_u - ev_ts) > 2.5:
            return False
        if ev_ts > 0.0 and ev_ts < held_ts - (0.02 if via_ws else 0.05):
            return False

        bids_map = _levels_to_map(payload.get("bids"))
        asks_map = _levels_to_map(payload.get("asks"))
        if side == "UP":
            self.state._up_bids = bids_map
            self.state._up_asks = asks_map
            self.state._up_book_initialized = True
        else:
            self.state._down_bids = bids_map
            self.state._down_asks = asks_map
            self.state._down_book_initialized = True

        bid, ask = extract_executable_bid_ask_from_maps(bids_map, asks_map, target_usdc=self.target_usdc)
        self._record_book_side(side, bid, ask, ev_ts, now_u, via_ws=via_ws)
        if via_ws:
            self.health.clob_ws_book_msgs += 1
        return True

    def apply_clob_price_change_payload(self, payload: Dict[str, Any], via_ws: bool = True) -> bool:
        """Apply incremental CLOB WS `price_change` deltas (orders/cancels) to L2 maps without REST polling."""
        ev_ts = _parse_book_ts_sec(payload)
        now_u = self.utc_now()
        if via_ws and ev_ts > 0.0 and (now_u - ev_ts) > 2.5:
            return False

        changes = payload.get("price_changes") or payload.get("changes") or ()
        touched: set[str] = set()
        for ch in changes:
            if not isinstance(ch, dict):
                continue
            aid = str(ch.get("asset_id") or payload.get("asset_id") or "")
            if aid and aid == self.state.up_token:
                side = "UP"
                initialized = self.state._up_book_initialized
                held_ts = self.state.up_book_ts
                bids_map, asks_map = self.state._up_bids, self.state._up_asks
            elif aid and aid == self.state.down_token:
                side = "DOWN"
                initialized = self.state._down_book_initialized
                held_ts = self.state.down_book_ts
                bids_map, asks_map = self.state._down_bids, self.state._down_asks
            else:
                continue

            if not initialized:
                continue
            if ev_ts > 0.0 and ev_ts < held_ts - 0.05:
                continue
            try:
                px = round(float(ch.get("price") or 0.0), 4)
                sz = float(ch.get("size") or 0.0)
            except (TypeError, ValueError):
                continue
            if not (0.0 < px < 1.0):
                continue

            b_side = str(ch.get("side") or "").upper()
            target = bids_map if b_side == "BUY" else (asks_map if b_side == "SELL" else None)
            if target is None:
                continue
            if sz <= 0.0:
                target.pop(px, None)
            else:
                target[px] = sz
            touched.add(side)

        if not touched:
            return False

        for side in touched:
            bids_map = self.state._up_bids if side == "UP" else self.state._down_bids
            asks_map = self.state._up_asks if side == "UP" else self.state._down_asks
            if not bids_map and not asks_map:
                continue
            bid, ask = extract_executable_bid_ask_from_maps(bids_map, asks_map, target_usdc=self.target_usdc)
            self._record_book_side(side, bid, ask, ev_ts, now_u, via_ws=via_ws)
        if via_ws:
            self.health.clob_ws_price_change_msgs += 1
        return True

    def _poll_cex_rest_once(self) -> None:
        """HTTPS REST fallback + millisecond Binance /api/v3/time clock sync."""
        now = time.time()
        if self.chainlink._precise_clock_synced_ts <= 0.0 or (now - self.chainlink._precise_clock_synced_ts) >= 600.0:
            self.chainlink.sync_clock_precise(self._http)

        need_book = (now - self.state.binance_ts) >= 2.5
        need_clock = (
            self.chainlink._precise_clock_synced_ts <= 0.0
            and ((len(self.chainlink._clock_samples) < 3) or ((now - self._last_clock_sync_ts) >= 60.0))
        )
        if need_book or need_clock:
            try:
                r = self._http.get(BINANCE_REST_BOOK_URL, timeout=2.0)
                if r.status_code == 200:
                    self._sync_clock_from_http_date(r)
                    self._last_clock_sync_ts = time.time()
                    if need_book:
                        d = r.json()
                        bp, ap = float(d["bidPrice"]), float(d["askPrice"])
                        bq, aq = float(d["bidQty"]), float(d["askQty"])
                        self.state.binance_price = 0.5 * (bp + ap)
                        self.state.binance_ts = time.time()
                        self.state.update_usdt_basis()
                        self.tracker.on_book_ticker(bq, aq)
                        self.tracker.on_price(self.utc_now(), self.state.binance_price)
            except Exception:
                pass

        if (now - self.state.coinbase_ts) >= 2.5:
            try:
                r = self._http.get(COINBASE_REST_TICKER_URL, timeout=2.0)
                if r.status_code == 200:
                    self._sync_clock_from_http_date(r)
                    d = r.json()
                    if "price" in d:
                        self.state.coinbase_price = float(d["price"])
                        self.state.coinbase_ts = time.time()
                        self.state.update_usdt_basis()
            except Exception:
                pass

        # Realized vol_1s is driven solely by single-venue Binance 1s closes (REST warmup + aggTrade/bookTicker).

    async def _cex_http_fallback_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while not self._stop_event.is_set():
            await loop.run_in_executor(None, self._poll_cex_rest_once)
            await asyncio.sleep(1.5)

    async def _binance_ws_loop(self) -> None:
        import websockets

        while not self._stop_event.is_set():
            try:
                async with websockets.connect(
                    BINANCE_WS_URL,
                    ping_interval=15,
                    ping_timeout=10,
                    open_timeout=8,
                    close_timeout=2,
                ) as ws:
                    while not self._stop_event.is_set():
                        raw = await asyncio.wait_for(ws.recv(), timeout=8.0)
                        msg = json.loads(raw)
                        stream = msg.get("stream", "")
                        data = msg.get("data", {})
                        now_local = time.time()
                        ev_ms = data.get("E")
                        if ev_ms is not None and (now_local - self._last_clock_sync_ts) >= 5.0:
                            try:
                                self.chainlink.update_clock_offset(float(ev_ms) / 1000.0, local_ts=now_local)
                                self._last_clock_sync_ts = now_local
                            except (TypeError, ValueError):
                                pass
                        now_utc = self.utc_now()

                        if "aggTrade" in stream:
                            p = float(data["p"])
                            q = float(data["q"])
                            is_buyer_maker = bool(data["m"])
                            self.state.binance_price = p
                            self.state.binance_ts = now_local
                            self.state.update_usdt_basis()
                            self.tracker.on_trade(now_utc, p, q, is_buyer_maker)
                            self.tracker.on_price(now_utc, p)
                        elif "bookTicker" in stream:
                            bid_p, ask_p = float(data["b"]), float(data["a"])
                            bid_q, ask_q = float(data["B"]), float(data["A"])
                            self.state.binance_price = 0.5 * (bid_p + ask_p)
                            self.state.binance_ts = now_local
                            self.state.update_usdt_basis()
                            self.tracker.on_book_ticker(bid_q, ask_q)
                            self.tracker.on_price(now_utc, self.state.binance_price)
            except Exception:
                if not self._stop_event.is_set():
                    self.health.binance_ws_reconnects += 1
                await asyncio.sleep(2.5)

    async def _coinbase_ws_loop(self) -> None:
        import websockets

        sub_msg = {"type": "subscribe", "product_ids": ["XRP-USD"], "channels": ["ticker"]}
        while not self._stop_event.is_set():
            try:
                async with websockets.connect(
                    COINBASE_WS_URL,
                    ping_interval=15,
                    ping_timeout=10,
                    open_timeout=8,
                    close_timeout=2,
                ) as ws:
                    await ws.send(json.dumps(sub_msg))
                    while not self._stop_event.is_set():
                        raw = await asyncio.wait_for(ws.recv(), timeout=8.0)
                        data = json.loads(raw)
                        if data.get("type") == "ticker" and "price" in data:
                            self.state.coinbase_price = float(data["price"])
                            self.state.coinbase_ts = time.time()
                            self.state.update_usdt_basis()
            except Exception:
                if not self._stop_event.is_set():
                    self.health.coinbase_ws_reconnects += 1
                await asyncio.sleep(2.5)

    async def _polymarket_window_and_book_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while not self._stop_event.is_set():
            now_utc = self.utc_now()
            self.sync_window_boundary(now_utc)
            window_ts = self.state.current_window_ts
            elapsed = now_utc - window_ts

            # 1. Fetch Gamma metadata when tokens are missing OR up to twice after open (10s..60s)
            #    to capture `official_strike` (`priceToBeat`) even when tokens were pre-fetched!
            need_tokens = not self.state.up_token
            need_official_strike = (
                self.state.strike_source != "GAMMA_OFFICIAL"
                and 10.0 <= elapsed <= 65.0
                and self._gamma_strike_checks < 2
                and (now_utc - self._last_gamma_fetch_ts) >= 15.0
            )
            if (need_tokens and (now_utc - self._last_gamma_fetch_ts) >= 3.0) or need_official_strike:
                self._last_gamma_fetch_ts = now_utc
                if need_official_strike:
                    self._gamma_strike_checks += 1
                meta = await loop.run_in_executor(None, fetch_5m_market_metadata, window_ts, self._http)
                if meta and self.state.current_window_ts == window_ts:
                    self.state.up_token = meta["up_token"]
                    self.state.down_token = meta["down_token"]
                    if meta.get("official_strike"):
                        self.state.strike_price = meta["official_strike"]
                        self.state.strike_source = "GAMMA_OFFICIAL"
                    elif (
                        self.state.strike_price is None
                        and elapsed < 5.0
                        and self.chainlink.state.settlement_reference_price is not None
                    ):
                        self.state.strike_price = self.chainlink.state.settlement_reference_price
                        self.state.strike_source = self.chainlink.state.source

            # 2. Pre-fetch NEXT 5m window tokens 25s before rollover so t=0.0s book is instant!
            next_w = window_ts + 300
            if elapsed >= 275.0 and next_w not in self._prefetched_tokens and (now_utc - self._last_gamma_fetch_ts) >= 5.0:
                self._last_gamma_fetch_ts = now_utc
                nxt_meta = await loop.run_in_executor(None, fetch_5m_market_metadata, next_w, self._http)
                if nxt_meta and nxt_meta.get("up_token") and nxt_meta.get("down_token"):
                    self._prefetched_tokens[next_w] = (nxt_meta["up_token"], nxt_meta["down_token"])

            # 3. Poll depth-aware executable VWAP CLOB orderbook ONLY when WS is silent (>=2.5s) or book uninitialized
            ws_silent = (time.time() - self.state.last_ws_book_local_ts) >= 2.5
            book_empty = (
                (self.state.book.up_bid is None and self.state.book.up_ask is None)
                or (self.state.book.down_bid is None and self.state.book.down_ask is None)
            )
            if self.state.up_token and self.state.down_token and (ws_silent or book_empty):
                up_t, dn_t = self.state.up_token, self.state.down_token
                up_raw, down_raw = await asyncio.gather(
                    loop.run_in_executor(None, fetch_clob_book_raw, up_t, self._http, 2.5),
                    loop.run_in_executor(None, fetch_clob_book_raw, dn_t, self._http, 2.5),
                )
                if self.state.current_window_ts == window_ts:
                    self.health.clob_rest_fallbacks += 1
                    if up_raw is not None:
                        self.apply_clob_book_payload(up_t, up_raw, via_ws=False)
                    if down_raw is not None:
                        self.apply_clob_book_payload(dn_t, down_raw, via_ws=False)

            await asyncio.sleep(1.2)

    async def _open_clob_ws(self, tokens: List[str]) -> Any:
        import websockets

        ws = await websockets.connect(
            POLY_CLOB_WS_URL,
            ping_interval=15,
            ping_timeout=10,
            open_timeout=8,
            close_timeout=2,
        )
        await ws.send(json.dumps({"assets_ids": tokens, "type": "market"}))
        return ws

    async def _close_standby_task(self, standby_task: Optional[asyncio.Task]) -> None:
        if standby_task is None:
            return
        if not standby_task.done():
            standby_task.cancel()
        try:
            ws = await standby_task
            if ws is not None:
                await ws.close()
        except Exception:
            pass

    async def _polymarket_clob_ws_loop(self) -> None:
        subscribed_window = 0
        standby_window = 0
        standby_tokens: Tuple[str, str] = ("", "")
        standby_task: Optional[asyncio.Task] = None

        try:
            while not self._stop_event.is_set():
                if not self.state.up_token or not self.state.down_token:
                    await asyncio.sleep(0.25)
                    continue

                subscribed_window = self.state.current_window_ts
                tokens = [self.state.up_token, self.state.down_token]
                ws = None
                try:
                    if (
                        standby_task is not None
                        and standby_window == subscribed_window
                        and list(standby_tokens) == tokens
                    ):
                        task_ref = standby_task
                        standby_task = None
                        standby_window = 0
                        standby_tokens = ("", "")
                        ws = await task_ref
                    else:
                        if standby_task is not None and standby_window < subscribed_window:
                            await self._close_standby_task(standby_task)
                            standby_task = None
                            standby_window = 0
                            standby_tokens = ("", "")
                        ws = await self._open_clob_ws(tokens)

                    last_msg_ts = time.time()
                    while (
                        not self._stop_event.is_set()
                        and self.state.current_window_ts == subscribed_window
                    ):
                        next_w = subscribed_window + 300
                        if (
                            (self.utc_now() - subscribed_window) >= 290.0
                            and next_w in self._prefetched_tokens
                            and standby_task is None
                        ):
                            nxt_pair = self._prefetched_tokens[next_w]
                            standby_window = next_w
                            standby_tokens = nxt_pair
                            standby_task = asyncio.create_task(self._open_clob_ws(list(nxt_pair)))

                        if time.time() - last_msg_ts >= 12.0:
                            self.health.clob_ws_reconnects += 1
                            break
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            continue
                        last_msg_ts = time.time()
                        if not raw or raw in ("PING", "PONG"):
                            continue
                        try:
                            payload = json.loads(raw)
                        except ValueError:
                            continue
                        events = payload if isinstance(payload, list) else [payload]
                        for ev in events:
                            if not isinstance(ev, dict):
                                continue
                            ev_type = str(ev.get("event_type") or "")
                            if ev_type == "book":
                                self.apply_clob_book_payload(str(ev.get("asset_id") or ""), ev, via_ws=True)
                            elif ev_type == "price_change":
                                self.apply_clob_price_change_payload(ev, via_ws=True)
                except Exception:
                    if not self._stop_event.is_set():
                        self.health.clob_ws_reconnects += 1
                    await asyncio.sleep(1.5)
                finally:
                    if ws is not None:
                        try:
                            await ws.close()
                        except Exception:
                            pass
        finally:
            await self._close_standby_task(standby_task)
