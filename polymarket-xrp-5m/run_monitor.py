"""
Live Polymarket XRP 5m Data Recorder + Chainlink Fair-Value Monitor + Paper Scout.

Usage:
    python run_monitor.py                  # Default: $1,000 bankroll, $25 USDC fixed bet per signal
"""
import argparse
import asyncio
from dataclasses import replace
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import shutil
import sys
import time
from typing import Any, Callable, Optional
import unicodedata

_SHARED_ROOT = Path(__file__).resolve().parent.parent
if str(_SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(_SHARED_ROOT))

from shared_risk import SharedDailyRiskGuard, compute_proportional_shares
from src.chainlink_oracle import has_websockets_installed
from src.config import DEFAULT_CONFIG, StrategyConfig
from src.fees import effective_exit_proceeds, taker_fee_per_share
from src.feeds import LiveMarketHub, fetch_5m_market_metadata
from src.live_executor import LiveFillResult, PolymarketLiveExecutor
from src.pricing_model import RollingMicrostructureTracker, estimate_fair_value, estimate_twap_fair_value
from src.storage import MarketDataStorage
from src.strategy import ClosedTrade, HighProbSniperStrategy, Position
from src.tg_notifier import TelegramBotService

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def _setup_event_logger(coin: str, log_file: str) -> Optional[logging.Logger]:
    """Configure a 50MB x 5 RotatingFileHandler for structured monitor events."""
    path_str = (log_file or "").strip()
    if not path_str:
        return None
    abs_path = Path(path_str).expanduser().resolve()
    abs_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(f"poly5m.{coin.lower()}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    for h in list(logger.handlers):
        logger.removeHandler(h)
        h.close()
    handler = RotatingFileHandler(
        abs_path,
        maxBytes=50 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter("%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(handler)
    return logger


def _handle_feed_health_alerts(
    feed_silences: tuple[tuple[str, float], ...],
    now_wall: float,
    last_feed_log_ts: dict[str, float],
    last_feed_tg_ts: dict[str, float],
    local_active_alerts: dict[str, float],
    emit_fn: Any,
    tg: Any,
    shared_risk: Any,
    log_threshold_sec: float = 15.0,
    tg_threshold_sec: float = 30.0,
    log_cooldown_sec: float = 300.0,
    tg_cooldown_sec: float = 1800.0,
) -> None:
    """Emit local feed warnings at 15s and cross-coin deduped Telegram outage/recovery alerts at 30s."""
    for f_name, f_sil in feed_silences:
        if f_sil > log_threshold_sec and (now_wall - last_feed_log_ts.get(f_name, 0.0)) >= log_cooldown_sec:
            last_feed_log_ts[f_name] = now_wall
            emit_fn(f"[FEED WARN] {f_name} silent={f_sil:.1f}s (>{log_threshold_sec:.0f}s)")
        if f_sil >= tg_threshold_sec:
            if shared_risk is not None:
                should_send = shared_risk.claim_alert(
                    f"feed:{f_name}", cooldown_sec=tg_cooldown_sec, mark_active=True, now_ts=now_wall
                )
            else:
                should_send = (now_wall - last_feed_tg_ts.get(f_name, 0.0)) >= tg_cooldown_sec
                if should_send:
                    last_feed_tg_ts[f_name] = now_wall
                    local_active_alerts[f_name] = now_wall
            if should_send and tg.enabled and tg.get_recipient_ids():
                tg.notify_alert(
                    f"資料源斷線超過 {tg_threshold_sec:.0f} 秒",
                    f"{f_name} 已連續靜默 {f_sil:.1f} 秒（跨幣去重告警），請檢查網路或 VPN。",
                )
        elif f_sil < 5.0:
            if shared_risk is not None:
                dur = shared_risk.resolve_alert(f"feed:{f_name}", now_ts=now_wall)
            else:
                started = local_active_alerts.pop(f_name, None)
                dur = max(0.0, now_wall - started) if started is not None else None
            if dur is not None:
                emit_fn(f"[FEED RECOVERED] {f_name} recovered after ~{dur:.0f}s")
                if tg.enabled and tg.get_recipient_ids():
                    tg.notify_alert(
                        "資料源已恢復",
                        f"✅ {f_name} 已恢復即時串流（告警後約 {dur:.0f} 秒恢復）。",
                    )


def _fetch_startup_wallet_position(
    up_token: str,
    down_token: str,
    live_exec: PolymarketLiveExecutor,
    window_ts: int,
) -> Optional[tuple[str, float, str, float]]:
    """Fetch conditional wallet balances and fill price off-thread (zero SQLite calls)."""
    up_bal = live_exec.sync_conditional_balance(up_token)
    dn_bal = live_exec.sync_conditional_balance(down_token)
    if up_bal >= 0.5:
        side, shares, token_id = "UP", up_bal, up_token
    elif dn_bal >= 0.5:
        side, shares, token_id = "DOWN", dn_bal, down_token
    else:
        return None
    _, tr_px = live_exec.lookup_recent_buy_fill(token_id, float(window_ts) - 5.0)
    return side, shares, token_id, tr_px


def reconcile_startup_position(
    window_ts: int,
    elapsed_sec: float,
    up_token: str,
    down_token: str,
    live_exec: PolymarketLiveExecutor,
    storage: MarketDataStorage,
    strategy: HighProbSniperStrategy,
    cfg: StrategyConfig,
    now_ts: float,
    up_mid_fallback: Optional[float] = None,
    down_mid_fallback: Optional[float] = None,
    prefetched: Optional[tuple[str, float, str, float]] = None,
) -> Optional[int]:
    """Adopt any live 5m wallet position into SQLite & strategy state on the main thread."""
    fetched = prefetched if prefetched is not None else _fetch_startup_wallet_position(
        up_token, down_token, live_exec, window_ts
    )
    if fetched is None:
        return None
    side, shares, token_id, tr_px = fetched
    mid_fb = up_mid_fallback if side == "UP" else down_mid_fallback

    db_row = storage.conn.execute(
        """
        SELECT id, side, entry_ts, entry_elapsed_sec, shares, entry_price,
               entry_fee_per_share, p_true_at_entry, z_at_entry
        FROM paper_trades
        WHERE window_ts = ? AND exit_ts IS NULL
        ORDER BY id DESC LIMIT 1
        """,
        (window_ts,),
    ).fetchone()

    if tr_px > 0.0:
        entry_px = tr_px
    elif db_row and db_row[1] == side and float(db_row[5] or 0.0) > 0.0:
        entry_px = float(db_row[5])
    elif mid_fb is not None and 0.05 <= mid_fb <= 0.95:
        entry_px = float(mid_fb)
    else:
        entry_px = 0.78

    fee_ps = taker_fee_per_share(entry_px, cfg.crypto_taker_fee_rate)
    p_true = float(db_row[7]) if (db_row and db_row[1] == side) else max(cfg.min_true_prob, min(0.95, entry_px + 0.08))
    z_val = float(db_row[8]) if (db_row and db_row[1] == side) else cfg.min_z_score
    ent_ts = float(db_row[2]) if (db_row and db_row[1] == side) else now_ts
    ent_el = float(db_row[3]) if (db_row and db_row[1] == side) else elapsed_sec

    if db_row and db_row[1] == side:
        db_id = int(db_row[0])
    else:
        db_id = storage.insert_paper_trade(
            window_ts=window_ts,
            side=side,
            entry_ts=ent_ts,
            entry_elapsed_sec=ent_el,
            shares=shares,
            entry_price=entry_px,
            entry_fee_per_share=fee_ps,
            p_true_at_entry=p_true,
            z_at_entry=z_val,
        )

    strategy.active_position = Position(
        window_ts=window_ts,
        side=side,
        entry_ts=ent_ts,
        entry_elapsed_sec=ent_el,
        shares=shares,
        entry_price=entry_px,
        fee_per_share=fee_ps,
        total_cost_per_share=entry_px + fee_ps,
        p_true_at_entry=p_true,
        z_at_entry=z_val,
        db_trade_id=db_id,
    )
    strategy.traded_windows.add(window_ts)
    return db_id


def _fit_console_line(text: str) -> str:
    """Pad/clip single-line `\r` status output so it NEVER wraps and scrolls in narrow PowerShell windows!"""
    max_cols = max(60, shutil.get_terminal_size((120, 24)).columns - 2)
    out = []
    cols = 0
    for ch in text:
        w = 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
        if cols + w > max_cols:
            break
        out.append(ch)
        cols += w
    if cols < max_cols:
        out.append(" " * min(4, max_cols - cols))
    return "".join(out)


async def resolve_official_polymarket_outcome(
    hub: LiveMarketHub,
    storage: MarketDataStorage,
    strategy: HighProbSniperStrategy,
    prev_window_ts: int,
    prev_strike: Optional[float],
    cl_close: Optional[float],
    last_up_bid: Optional[float],
    last_down_bid: Optional[float],
    active_db_trade_id: Optional[int],
    live_exec: Optional[PolymarketLiveExecutor] = None,
    tg: Optional[TelegramBotService] = None,
    shared_risk: Optional[SharedDailyRiskGuard] = None,
    event_logger: Optional[logging.Logger] = None,
    tg_commands_only: bool = False,
) -> None:
    """
    Wait ~6-15s in background after 5m close to verify Polymarket Gamma API's OFFICIAL settled
    outcome (`outcomePrices`), avoiding false `UP` ties when an on-chain push feed didn't tick!
    """
    def _emit_res(msg: str) -> None:
        clean = str(msg).strip()
        if event_logger is not None and clean:
            event_logger.info(clean)
        if sys.stdout.isatty() or event_logger is None:
            print(msg, flush=True)

    loop = asyncio.get_running_loop()
    official_winner: Optional[str] = None

    for delay in (8.0, 15.0, 22.0):
        await asyncio.sleep(delay)
        meta = await loop.run_in_executor(None, fetch_5m_market_metadata, prev_window_ts, hub._http)
        if meta and meta.get("resolved_winner"):
            official_winner = meta["resolved_winner"]
            if meta.get("official_strike"):
                prev_strike = meta["official_strike"]
            break

    # Determine final winner priority:
    # 1. Polymarket Gamma official settled outcome (100% authoritative Chainlink Data Streams result)
    # 2. Final CLOB orderbook consensus comparing BOTH sides (whichever ended higher >= $0.94)
    # 3. Live Chainlink comparison ONLY if price actually moved (cl_close != prev_strike)
    winner_src = "GAMMA_OFFICIAL"
    if official_winner is None:
        u_bid = last_up_bid or 0.0
        d_bid = last_down_bid or 0.0
        if max(u_bid, d_bid) >= 0.94 and abs(u_bid - d_bid) >= 0.20:
            official_winner = "UP" if u_bid > d_bid else "DOWN"
            winner_src = "CLOB_CONSENSUS"
        elif cl_close is not None and prev_strike is not None and abs(cl_close - prev_strike) > 0.0001:
            official_winner = "UP" if cl_close >= prev_strike else "DOWN"
            winner_src = hub.chainlink.state.source
        else:
            _emit_res(f">>> [5m 結算略過 {prev_window_ts}] 無法確認官方結算方向 (預言機未跳動且 Gamma 尚未公佈)")
            return

    now_utc = hub.utc_now()
    eff_strike = prev_strike or cl_close or 0.0
    # Construct synthetic settlement price consistent with official_winner for strategy accounting
    if official_winner == "UP":
        eff_close = cl_close if (cl_close and cl_close >= eff_strike) else (eff_strike + 1.0)
    else:
        eff_close = cl_close if (cl_close and cl_close < eff_strike) else (eff_strike - 1.0)

    storage.finalize_window(prev_window_ts, cl_close or eff_close, official_winner)
    prior_partial_pnl = (
        strategy.active_position.realized_partial_pnl_usdc
        if (strategy.active_position and strategy.active_position.window_ts == prev_window_ts)
        else 0.0
    )
    settle_msg = strategy.settle_window(now_utc, prev_window_ts, eff_strike, eff_close)

    s0_str = f"{prev_strike:,.4f}" if prev_strike else "N/A"
    st_str = f"{cl_close:,.4f}" if cl_close else "N/A"
    _emit_res(
        f">>> [5m 官方結算 {prev_window_ts}] S0={s0_str} -> ST={st_str} "
        f"=> 結果: {official_winner} (裁判來源: {winner_src})"
    )
    if settle_msg:
        _emit_res(f">>> {settle_msg}")
        if active_db_trade_id is not None and strategy.closed_trades:
            last = strategy.closed_trades[-1]
            delta_pnl = last.net_pnl_usdc - prior_partial_pnl
            if live_exec is not None:
                live_exec.record_realized_pnl(delta_pnl)
                if shared_risk is not None:
                    r_state = shared_risk.record_pnl(delta_pnl, current_equity=strategy.bankroll)
                    live_exec.daily_pnl_usdc = float(r_state["daily_realized_pnl_usdc"])
                    live_exec.max_daily_loss_usdc = float(r_state["max_daily_loss_usdc"])
                    live_exec.circuit_broken = bool(r_state["circuit_broken"])
            storage.close_paper_trade(
                active_db_trade_id,
                now_utc,
                last.exit_reason,
                last.exit_payout_per_share,
                last.net_pnl_usdc,
                last.won,
            )
            if tg is not None and tg.enabled and not tg_commands_only:
                tg.notify_exit(
                    trade_id=active_db_trade_id,
                    side=last.side,
                    window_ts=last.window_ts,
                    exit_reason=last.exit_reason,
                    net_pnl_usdc=last.net_pnl_usdc,
                    won=last.won,
                    exit_payout_per_share=last.exit_payout_per_share,
                    entry_price=last.entry_price,
                    live_mode=(live_exec is not None),
                )
            if tg is not None and tg.enabled and live_exec is not None and live_exec.circuit_broken:
                tg.notify_alert(
                    "實盤風控熔斷觸發",
                    f"今日實盤累計虧損已達上限 (-${live_exec.max_daily_loss_usdc:.1f} pUSD)，已自動停止開新倉！",
                )
        if live_exec is not None:
            try:
                fresh_bal = await loop.run_in_executor(None, live_exec.get_pusd_balance)
                if fresh_bal > 0.0:
                    strategy.bankroll = fresh_bal
                    if shared_risk is not None:
                        shared_risk.sync_day_equity(fresh_bal)
                        shared_risk.record_balance(fresh_bal)
            except Exception:
                pass
        stats = strategy.summary_stats()
        tag = "累積實盤戰績" if live_exec is not None else "累積模擬戰績"
        unit = "pUSD" if live_exec is not None else "USDC"
        _emit_res(
            f">>> [{tag}] 總單數={stats['trades']} | "
            f"勝率={stats['win_rate_pct']:.1f}% | "
            f"總損益={stats['net_pnl_usdc']:+.2f} {unit} | "
            f"帳戶餘額=${stats['final_bankroll']:,.2f}"
        )
        if live_exec is not None and strategy.closed_trades and strategy.closed_trades[-1].won:
            _emit_res(">>> 💡 [實盤提示] 到期獲勝注單請記得定期至 Polymarket 網頁點擊 Claim 領回 pUSD 餘額。")


async def _bg_prefetch_cond_balance(live_exec: PolymarketLiveExecutor, token_id: str) -> None:
    """Retry conditional token balance sync (2s/5s/9s/14s) until Polygon settlement completes."""
    loop = asyncio.get_running_loop()
    for wait_s in (2.0, 3.0, 4.0, 5.0):
        await asyncio.sleep(wait_s)
        avail = await loop.run_in_executor(None, live_exec.sync_conditional_balance, token_id)
        if avail > 0.0:
            return


async def _bg_warmup_window(
    live_exec: PolymarketLiveExecutor,
    up_token: Optional[str],
    down_token: Optional[str],
) -> None:
    """Pre-warm CLOB V2 market metadata & HTTP/2 socket in background so sniper orders fire in 1 RTT."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, live_exec.warmup_tokens, up_token, down_token)


async def _bg_sync_balance(
    live_exec: PolymarketLiveExecutor,
    strategy: HighProbSniperStrategy,
    hub: LiveMarketHub,
    tg: TelegramBotService,
    shared_risk: Optional[SharedDailyRiskGuard],
    bet_pct: float,
) -> None:
    """Refresh pUSD balance asynchronously so hot-zone tick evaluation never stalls."""
    try:
        fresh_bal = await asyncio.get_running_loop().run_in_executor(None, live_exec.get_pusd_balance)
        if bet_pct > 0.0:
            strategy.bankroll = fresh_bal
            hub.target_usdc = max(4.5, round(fresh_bal * bet_pct, 2))
            tg.bet_usdc = max(4.2, round(fresh_bal * bet_pct, 2))
        if shared_risk is not None and fresh_bal > 0.0:
            shared_risk.sync_day_equity(fresh_bal)
            shared_risk.record_balance(fresh_bal)
    except Exception as exc:
        sys.stderr.write(f"[BAL SYNC WARN] {exc}\n")


def _commit_live_exit_accounting(
    *,
    last: ClosedTrade,
    final_leg_pnl: float,
    now_ts: float,
    active_db_trade_id: int,
    live_exec: Optional[PolymarketLiveExecutor],
    shared_risk: Optional[SharedDailyRiskGuard],
    strategy: HighProbSniperStrategy,
    storage: MarketDataStorage,
    tg: TelegramBotService,
    live_mode: bool,
    emit_fn: Callable[[str], None],
    tg_commands_only: bool = False,
) -> None:
    """Persist realized PnL, shared daily risk state, SQLite trade close, and Telegram exit alert."""
    if live_exec is not None:
        live_exec.record_realized_pnl(final_leg_pnl)
        if shared_risk is not None:
            r_state = shared_risk.record_pnl(final_leg_pnl, current_equity=strategy.bankroll)
            live_exec.daily_pnl_usdc = float(r_state["daily_realized_pnl_usdc"])
            live_exec.max_daily_loss_usdc = float(r_state["max_daily_loss_usdc"])
            live_exec.circuit_broken = bool(r_state["circuit_broken"])
        if live_exec.circuit_broken:
            emit_fn(
                f"🛑 [風控熔斷] 今日實盤累計虧損已達上限 "
                f"(-${live_exec.max_daily_loss_usdc:.1f})，已自動停止新開倉！"
            )
    storage.close_paper_trade(
        active_db_trade_id,
        now_ts,
        last.exit_reason,
        last.exit_payout_per_share,
        last.net_pnl_usdc,
        last.won,
    )
    if tg.enabled and not tg_commands_only:
        tg.notify_exit(
            trade_id=active_db_trade_id,
            side=last.side,
            window_ts=last.window_ts,
            exit_reason=last.exit_reason,
            net_pnl_usdc=last.net_pnl_usdc,
            won=last.won,
            exit_payout_per_share=last.exit_payout_per_share,
            entry_price=last.entry_price,
            live_mode=live_mode,
        )
    if tg.enabled and live_exec is not None and live_exec.circuit_broken:
        tg.notify_alert(
            "實盤風控熔斷觸發",
            f"今日實盤累計虧損已達上限 (-${live_exec.max_daily_loss_usdc:.1f} pUSD)，已自動停止開新倉！",
        )


def _acquire_single_instance_lock(db_path: str):
    """Acquire an exclusive OS-level file lock on `<db_path>.lock` to prevent double-running `run_monitor.py`."""
    abs_db = os.path.abspath(db_path)
    os.makedirs(os.path.dirname(abs_db), exist_ok=True)
    lock_path = abs_db + ".lock"
    fh = open(lock_path, "a+b")
    try:
        fh.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        fh.seek(0)
        fh.truncate()
        fh.write(f"{os.getpid()}\n".encode("ascii"))
        fh.flush()
        return fh
    except OSError:
        fh.close()
        raise RuntimeError(
            f"🛑 [防雙開鎖定] 偵測到已有另一個 run_monitor.py 正在執行 ({db_path})！為防止雙倍下單已自動攔截啟動。"
        )


async def monitor_loop(
    db_path: str,
    bankroll: float,
    bet_usdc: float,
    live_mode: bool = False,
    max_daily_loss: float = 25.0,
    bet_pct: float = 0.0,
    max_daily_loss_pct: float = 0.0,
    log_file: str = "",
    tg_commands_only: bool = False,
) -> None:
    instance_lock = _acquire_single_instance_lock(db_path)
    event_logger = _setup_event_logger("XRP", log_file)
    is_tty = bool(sys.stdout.isatty())

    def _emit(msg: str) -> None:
        clean = str(msg).strip()
        if event_logger is not None and clean:
            event_logger.info(clean)
        if is_tty or event_logger is None:
            print(msg, flush=True)
    live_exec: Optional[PolymarketLiveExecutor] = None
    shared_risk: Optional[SharedDailyRiskGuard] = None
    eff_daily_loss = float(max_daily_loss)
    if live_mode:
        live_exec = PolymarketLiveExecutor(max_daily_loss_usdc=max_daily_loss)
        live_bal = live_exec.get_pusd_balance()
        min_req_u = 4.2 if bet_pct > 0.0 else max(1.0, bet_usdc)
        if live_bal < min_req_u:
            raise RuntimeError(
                f"實盤 pUSD 餘額不足 (${live_bal:,.2f} < 最低門檻 ${min_req_u:,.2f})！請先用 python check_account.py 確認餘額。"
            )
        bankroll = live_bal
        if max_daily_loss_pct > 0.0:
            shared_risk_path = Path(__file__).resolve().parent.parent / "shared_live_risk.json"
            shared_risk = SharedDailyRiskGuard(shared_risk_path, max_daily_loss_pct=max_daily_loss_pct)
            r_state = shared_risk.sync_day_equity(live_bal)
            shared_risk.record_balance(live_bal)
            eff_daily_loss = float(r_state["max_daily_loss_usdc"])
            live_exec.max_daily_loss_usdc = eff_daily_loss
            live_exec.daily_pnl_usdc = float(r_state["daily_realized_pnl_usdc"])
            live_exec.circuit_broken = bool(r_state["circuit_broken"])

    eff_fixed_bet = 0.0 if bet_pct > 0.0 else bet_usdc
    cfg = replace(
        DEFAULT_CONFIG,
        initial_bankroll_usdc=bankroll,
        fixed_bet_usdc=eff_fixed_bet,
        proportional_bet_pct=bet_pct,
    )
    est_ticket_u = max(4.2, round(bankroll * bet_pct, 2)) if bet_pct > 0.0 else (bet_usdc if bet_usdc > 0 else 25.0)
    hub_target_u = max(4.5, round(bankroll * bet_pct, 2)) if bet_pct > 0.0 else (bet_usdc if bet_usdc > 0 else 25.0)
    tracker = RollingMicrostructureTracker(
        vol_lookback_sec=cfg.vol_lookback_sec,
        cvd_window_sec=cfg.cvd_window_sec,
    )
    hub = LiveMarketHub(tracker=tracker, target_usdc=hub_target_u)
    storage = MarketDataStorage(db_path=db_path)
    strategy = HighProbSniperStrategy(config=cfg)
    tg = TelegramBotService(
        db_path=db_path,
        live_mode=False,
        live_exec=None,
        default_bet_usdc=est_ticket_u,
        asset_symbol="XRP",
    )
    last_applied_bet_ts = time.time()
    if tg.enabled:
        tg.clear_stale_bet_override()

    ws_ok = has_websockets_installed()
    if bet_pct > 0.0:
        sizing_desc = f"等比 {bet_pct*100:.0f}% (目前約 ${est_ticket_u:.1f} U，最低 5 股保底)"
        tg_bet_desc = f"等比 {bet_pct*100:.0f}% (約 ${est_ticket_u:.1f} U · 最低 5 股保底)"
    elif bet_usdc > 0:
        sizing_desc = f"Fixed ${bet_usdc:.1f} USDC / trade"
        tg_bet_desc = f"${bet_usdc:.1f} U"
    else:
        sizing_desc = "1/5 Fractional Kelly"
        tg_bet_desc = f"1/5 Kelly (約 ${est_ticket_u:.1f} U)"
    if shared_risk is not None and max_daily_loss_pct > 0.0:
        risk_desc = f"三幣共用單日熔斷 -{max_daily_loss_pct*100:.0f}% (-${eff_daily_loss:.1f} pUSD)"
        tg_risk_desc = f"三幣共用 -{max_daily_loss_pct*100:.0f}% (-${eff_daily_loss:.1f} U)"
    else:
        risk_desc = f"單日最大虧損熔斷 -${eff_daily_loss:.1f} pUSD"
        tg_risk_desc = f"-${eff_daily_loss:.1f} U"
    mode_tag = "🔴 LIVE 實盤自動交易 (pUSD)" if live_mode else "🟢 PAPER 模擬盤與錄製"

    print("=" * 98)
    print(f" Polymarket XRP 5m [Chainlink-Oracle High-Prob Sniper] v4.6 | {mode_tag}")
    print(f" 資料庫路徑 : {db_path}")
    print(f" 交易本金   : ${bankroll:,.2f} {'pUSD' if live_mode else 'USDC'} | 下注模式: {sizing_desc}")
    if live_mode:
        print(f" 實盤風控鎖 : {risk_desc} | 全程 FAK 限價不追高 (上限 ${cfg.max_entry_price:.2f})")
    print(
        f" 連線模式   : {'WebSocket 即時串流 + 伺服器 UTC 自動校時 + VWAP 深度過濾' if ws_ok else '純 HTTPS REST 輪詢模式 (建議執行 pip install websockets)'}"
    )
    print(" 預言機來源 : Chainlink XRP/USD (RTDS + Gamma 官方結算核對)")
    if tg.enabled:
        tg_bind_msg = f"共用主 Bot 單向推播 (接收對象 {len(tg.get_recipient_ids())} 位)" if tg.get_recipient_ids() else "等待主 Bot 綁定 Chat ID"
        print(f" 📱 Telegram 監控 : 已啟用 | {tg_bind_msg}")
    else:
        print(" 📱 Telegram 監控 : 未設定 TG_BOT_TOKEN (可隨時執行 python tg_bot.py 設定)")
    print("=" * 98)
    _emit(f"[BOOT] Polymarket XRP 5m v5.1 | {mode_tag} | bankroll=${bankroll:,.2f}")

    feed_task = asyncio.create_task(hub.start())
    loop_init = asyncio.get_running_loop()
    init_clock_ms = await loop_init.run_in_executor(None, hub.chainlink.sync_clock_precise, hub._http)
    if init_clock_ms is not None:
        _emit(
            f"[CLOCK] raw_offset_ms={hub.chainlink.raw_clock_offset_ms:+.1f}ms "
            f"eff_offset_ms={hub.chainlink.effective_clock_offset_ms:+.1f}ms"
        )
        if abs(hub.chainlink.raw_clock_offset_ms) > 250.0 and tg.enabled and tg.get_recipient_ids():
            if shared_risk is None or shared_risk.claim_alert("clock:skew", cooldown_sec=3600.0):
                tg.notify_alert(
                    "系統時鐘偏差警告",
                    f"本機時鐘偏差達 {hub.chainlink.raw_clock_offset_ms:+.1f}ms (>250ms)，已啟動毫秒補償，建議同步 OS NTP。",
                )

    tg_task: Optional[asyncio.Task] = None
    if tg.enabled and tg.get_recipient_ids():
        # Push-only satellite mode: BTC owns `getUpdates` polling to avoid 409 Conflict
        tg.send_async(
            f"<b>[XRP | SYSTEM ONLINE // {'LIVE 實盤狙擊' if live_mode else 'PAPER 模擬監控'}]</b>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"▸ <b>單筆規模</b>：<b>{tg_bet_desc}</b>\n"
            f"▸ <b>單日熔斷</b>：<b>{tg_risk_desc}</b>"
        )
    prev_window_ts: Optional[int] = None
    prev_strike: Optional[float] = None
    last_up_bid: Optional[float] = None
    last_down_bid: Optional[float] = None
    active_db_trade_id: Optional[int] = None
    last_live_attempt_ts: float = 0.0
    last_live_attempt_cooldown: float = 5.0
    last_warmup_ts: float = 0.0
    last_ka_dispatch_ts: float = 0.0
    last_bal_sync_ts: float = time.time()
    bal_sync_task: Optional[asyncio.Task] = None
    last_1hz_ts: float = 0.0
    dyn_gap_bps: float = 0.0
    is_tg_paused: bool = False
    consecutive_loop_errors: int = 0
    startup_reconciled: bool = live_exec is None
    reconcile_in_flight: bool = False
    maker_tp_order_id: str = ""
    maker_tp_token: str = ""
    maker_tp_placed_window: int = 0
    last_maker_check_ts: float = 0.0
    maker_tp_inflight: bool = False
    monitor_start_ts: float = time.time()
    last_seen_clock_sync_ts: float = hub.chainlink._precise_clock_synced_ts
    last_clock_warn_ts: float = time.time() if abs(hub.chainlink.raw_clock_offset_ms) > 250.0 else 0.0
    last_feed_warn_ts: dict[str, float] = {}
    last_feed_tg_ts: dict[str, float] = {}
    local_active_feed_alerts: dict[str, float] = {}
    bg_tasks: set[asyncio.Task] = set()

    try:
        while True:
            try:
                now = hub.utc_now()
                st = hub.state
                cl = hub.chainlink.state
                window_ts = (int(now) // 300) * 300
                elapsed = now - window_ts
                remaining = max(0.0, 300.0 - elapsed)

                hot_zone = (148.0 <= elapsed <= 258.0) or (strategy.active_position is not None)
                loop_sleep = 0.25 if hot_zone else 1.0
                is_1hz_tick = (
                    (not hot_zone)
                    or ((now - last_1hz_ts) >= 0.98)
                    or (prev_window_ts is not None and window_ts != prev_window_ts)
                )
                if is_1hz_tick:
                    last_1hz_ts = now

                if live_exec is not None and (now - last_bal_sync_ts) >= 15.0:
                    last_bal_sync_ts = now
                    if bal_sync_task is None or bal_sync_task.done():
                        bal_sync_task = asyncio.create_task(
                            _bg_sync_balance(live_exec, strategy, hub, tg, shared_risk, bet_pct)
                        )
                        bg_tasks.add(bal_sync_task)
                        bal_sync_task.add_done_callback(bg_tasks.discard)
                    if shared_risk is not None:
                        broken, dpnl, dcap = shared_risk.is_broken()
                        live_exec.daily_pnl_usdc = dpnl
                        live_exec.max_daily_loss_usdc = dcap
                        live_exec.circuit_broken = broken

                if is_1hz_tick:
                    ctrl = tg.get_control_state()
                    is_tg_paused = bool(ctrl.get("paused", False))
                    ctrl_bet = ctrl.get("bet_usdc")
                    ctrl_bet_ts = float(ctrl.get("bet_updated_at") or 0.0)
                    if (
                        bet_pct <= 0.0
                        and ctrl_bet is not None
                        and float(ctrl_bet) > 0.0
                        and ctrl_bet_ts > last_applied_bet_ts
                        and abs(float(ctrl_bet) - cfg.fixed_bet_usdc) > 1e-6
                    ):
                        last_applied_bet_ts = ctrl_bet_ts
                        new_bet = float(ctrl_bet)
                        cfg = replace(cfg, fixed_bet_usdc=new_bet)
                        strategy.cfg = cfg
                        hub.target_usdc = new_bet
                        tg.bet_usdc = new_bet
                        print(f"\n📱 [Telegram 遠端調整] 單筆下注金額已更新為 ${new_bet:.1f} U")

                # Trigger non-blocking official Gamma + Chainlink settlement verification on rollover
                if prev_window_ts is not None and window_ts != prev_window_ts:
                    if live_exec is not None and maker_tp_order_id:
                        _cancel_oid = maker_tp_order_id
                        _ct = asyncio.create_task(
                            asyncio.get_running_loop().run_in_executor(
                                None, live_exec.cancel_order_safe, _cancel_oid
                            )
                        )
                        bg_tasks.add(_ct)
                        _ct.add_done_callback(bg_tasks.discard)
                    maker_tp_order_id = ""
                    maker_tp_token = ""
                    maker_tp_placed_window = 0
                    last_maker_check_ts = 0.0
                    maker_tp_inflight = False
                    hs = hub.health.snapshot_dict(hub.chainlink)
                    _emit(
                        f"[HEALTH] window={prev_window_ts} | cl_src={cl.source} | "
                        f"book_age_p50_ms={hs['book_age_p50_ms']} book_age_p99_ms={hs['book_age_p99_ms']} | "
                        f"book_lag_p50_ms={hs['book_lag_p50_ms']} book_lag_p99_ms={hs['book_lag_p99_ms']} | "
                        f"rollover_p99_ms={hs['rollover_p99_ms']} | "
                        f"ws_book={hs['ws_book']} ws_pc={hs['ws_pc']} rest_fallbacks={hs['rest_fallbacks']} | "
                        f"reconn(clob={hs['clob_reconn']},rtds={hs['rtds_reconn']},pb={hs['polybolt_reconn']},bn={hs['bn_reconn']},cb={hs['cb_reconn']}) | "
                        f"stale_skips={hs['stale_skips']} | clock_raw_ms={hs['clock_raw_ms']:+.1f} clock_eff_ms={hs['clock_eff_ms']:+.1f}"
                    )
                    settle_close = (
                        cl.official_twap_60s
                        if (
                            cl.official_twap_60s is not None
                            and cl.official_twap_recv_ts > 0.0
                            and (time.time() - cl.official_twap_recv_ts) < 10.0
                        )
                        else cl.settlement_reference_price
                    )
                    t = asyncio.create_task(
                        resolve_official_polymarket_outcome(
                            hub=hub,
                            storage=storage,
                            strategy=strategy,
                            prev_window_ts=prev_window_ts,
                            prev_strike=prev_strike,
                            cl_close=settle_close,
                            last_up_bid=last_up_bid,
                            last_down_bid=last_down_bid,
                            active_db_trade_id=active_db_trade_id,
                            live_exec=live_exec,
                            tg=tg,
                            shared_risk=shared_risk,
                            event_logger=event_logger,
                            tg_commands_only=tg_commands_only,
                        )
                    )
                    bg_tasks.add(t)
                    t.add_done_callback(bg_tasks.discard)
                    active_db_trade_id = None
                    prev_strike = None
                    last_up_bid, last_down_bid = None, None

                prev_window_ts = window_ts
                # Synchronously align 5m window state BEFORE reading strike_price (prevents stale strike leak!)
                hub.sync_window_boundary(now)
                if (
                    not startup_reconciled
                    and not reconcile_in_flight
                    and live_exec is not None
                    and st.up_token
                    and st.down_token
                ):
                    reconcile_in_flight = True
                    try:
                        prefetched = await asyncio.get_running_loop().run_in_executor(
                            None,
                            _fetch_startup_wallet_position,
                            st.up_token,
                            st.down_token,
                            live_exec,
                            window_ts,
                        )
                        rec_id = (
                            reconcile_startup_position(
                                window_ts,
                                elapsed,
                                st.up_token,
                                st.down_token,
                                live_exec,
                                storage,
                                strategy,
                                cfg,
                                now,
                                st.book.up_ask,
                                st.book.down_ask,
                                prefetched=prefetched,
                            )
                            if prefetched is not None
                            else None
                        )
                        if rec_id is not None and strategy.active_position is not None:
                            active_db_trade_id = rec_id
                            rp = strategy.active_position
                            _emit(
                                f"🔄 [RECONCILED_POSITION // STARTUP] window={rp.window_ts} {rp.side} "
                                f"shares={rp.shares:.2f} @ ${rp.entry_price:.3f}"
                            )
                            if tg.enabled and tg.get_recipient_ids():
                                tg.notify_alert(
                                    "重啟對帳接管部位",
                                    f"已接管 {rp.side} {rp.shares:.2f} sh @ ${rp.entry_price:.3f} (window={rp.window_ts})",
                                )
                    finally:
                        reconcile_in_flight = False
                        startup_reconciled = True
                if st.strike_price is not None:
                    prev_strike = st.strike_price
                if st.book.up_bid is not None:
                    last_up_bid = st.book.up_bid
                elif st.book.up_ask is not None and st.book.up_ask <= 0.10:
                    last_up_bid = 0.0
                if st.book.down_bid is not None:
                    last_down_bid = st.book.down_bid
                elif st.book.down_ask is not None and st.book.down_ask <= 0.10:
                    last_down_bid = 0.0

                cl_spot = cl.settlement_reference_price
                if cl_spot is not None and elapsed >= 240.0:
                    st.record_twap240_spot(now, cl_spot)
                cex_lead = st.cex_lead_price
                vol_1s = tracker.get_realized_vol_1s(floor=cfg.min_1s_vol_floor)
                cvd_imb, obi, flow_score = tracker.get_orderflow_metrics(now)
                if is_1hz_tick:
                    calibrated_strike = st.calibrate_strike_from_opening_book(cl_spot, vol_1s, elapsed)
                    if calibrated_strike is not None:
                        prev_strike = calibrated_strike
                        print(
                            f"\n🎯 [開盤盤口自動校正 Strike] {window_ts} 基準價已校準為 ${calibrated_strike:,.4f} (BOOK_CALIBRATED)"
                        )
                        if cfg.skip_book_calibrated_windows and window_ts not in strategy.traded_windows:
                            # [2026-11-17 fix] 基準價不確定的盤不開新倉 (不影響已持倉部位的停利/結算)
                            strategy.traded_windows.add(window_ts)
                            print(f"⏭️  [基準價不確定] {window_ts} 盤口與 Chainlink 基準價分歧，本盤不開新倉")
                    dyn_gap_bps = tracker.update_oracle_basis(cl_spot, cex_lead)
                    if (
                        st.binance_price is not None
                        and st.binance_price > 0.0
                        and (now - st.binance_ts) <= 5.0
                        and cl_spot is not None
                        and cl_spot > 0.0
                        and cl.is_alive(max_silence_sec=5.0)
                    ):
                        tracker.record_basis_sample(now, st.binance_price, cl_spot)

                    storage.upsert_window(
                        window_ts=window_ts,
                        slug=f"xrp-updown-5m-{window_ts}",
                        up_token_id=st.up_token,
                        down_token_id=st.down_token,
                        strike_price=st.strike_price,
                        strike_source=st.strike_source,
                    )

                # Pre-warm new 5m token metadata in background during WAIT phase & keep HTTP/2 socket hot in SNIPE phase
                if live_exec is not None:
                    if (
                        st.up_token
                        and st.down_token
                        and (
                            st.up_token not in live_exec._warmed_tokens
                            or st.down_token not in live_exec._warmed_tokens
                        )
                        and (now - last_warmup_ts) >= 5.0
                    ):
                        last_warmup_ts = now
                        wt = asyncio.create_task(_bg_warmup_window(live_exec, st.up_token, st.down_token))
                        bg_tasks.add(wt)
                        wt.add_done_callback(bg_tasks.discard)
                    elif 135.0 <= elapsed <= 295.0 and (now - last_ka_dispatch_ts) >= 10.0:
                        last_ka_dispatch_ts = now
                        asyncio.get_running_loop().run_in_executor(None, live_exec.keepalive_if_needed, 10.0)

                fv = None
                if cl_spot is not None and st.strike_price is not None:
                    if cfg.use_twap_projection:
                        fv = estimate_twap_fair_value(
                            strike_price=st.strike_price,
                            chainlink_price=cl_spot,
                            remaining_sec=remaining,
                            vol_1s=vol_1s,
                            orderflow_score=flow_score,
                            cex_lead_price=cex_lead,
                            dynamic_gap_bps=dyn_gap_bps,
                            cvd_imbalance=cvd_imb,
                            realized_240=st.realized_twap240,
                            twap_gaussian_weight=cfg.twap_gaussian_weight,
                            twap_student_t_df=cfg.twap_student_t_df,
                        )
                    else:
                        fv = estimate_fair_value(
                            strike_price=st.strike_price,
                            chainlink_price=cl_spot,
                            remaining_sec=remaining,
                            vol_1s=vol_1s,
                            orderflow_score=flow_score,
                            cex_lead_price=cex_lead,
                            dynamic_gap_bps=dyn_gap_bps,
                            student_t_df=cfg.student_t_df,
                            cvd_imbalance=cvd_imb,
                        )
                if fv is not None and cfg.e4_dual_consensus and cfg.use_twap_projection:
                    basis_60s = tracker.mean_basis_60s(now)
                    if basis_60s is not None and st.binance_price is not None and (now - st.binance_ts) <= 5.0:
                        bn_adj = float(st.binance_price) - basis_60s
                        if bn_adj > 0.0:
                            fv_bn = estimate_twap_fair_value(
                                strike_price=st.strike_price,
                                chainlink_price=bn_adj,
                                remaining_sec=remaining,
                                vol_1s=vol_1s,
                                orderflow_score=flow_score,
                                cex_lead_price=cex_lead,
                                dynamic_gap_bps=dyn_gap_bps,
                                cvd_imbalance=cvd_imb,
                                realized_240=st.realized_twap240,
                                twap_gaussian_weight=cfg.twap_gaussian_weight,
                                twap_student_t_df=cfg.twap_student_t_df,
                            )
                            if fv_bn is not None:
                                fv.bn_raw_z = fv_bn.raw_z_score
                                fv.bn_p_true_up = fv_bn.p_true_up
                                fv.bn_p_true_down = fv_bn.p_true_down

                if is_1hz_tick:
                    if hub.chainlink._precise_clock_synced_ts > last_seen_clock_sync_ts:
                        last_seen_clock_sync_ts = hub.chainlink._precise_clock_synced_ts
                        _emit(
                            f"[CLOCK] raw_offset_ms={hub.chainlink.raw_clock_offset_ms:+.1f}ms "
                            f"eff_offset_ms={hub.chainlink.effective_clock_offset_ms:+.1f}ms"
                        )
                        if (
                            abs(hub.chainlink.raw_clock_offset_ms) > 250.0
                            and (time.time() - last_clock_warn_ts) >= 3600.0
                        ):
                            last_clock_warn_ts = time.time()
                            _emit(f"[CLOCK WARN] raw_offset_ms={hub.chainlink.raw_clock_offset_ms:+.1f}ms > 250ms")
                            if (
                                hub.chainlink._consecutive_clock_warn_count >= 2
                                and tg.enabled
                                and tg.get_recipient_ids()
                                and (shared_risk is None or shared_risk.claim_alert("clock:skew", cooldown_sec=3600.0))
                            ):
                                tg.notify_alert(
                                    "系統時鐘偏差警告",
                                    f"本機時鐘偏差 {hub.chainlink.raw_clock_offset_ms:+.1f}ms (>250ms)",
                                )

                    if 150.0 <= elapsed <= 255.0 and st.last_book_update_local_ts > 0.0:
                        hub.health.record_hot_sample(
                            age_ms=(time.time() - st.last_book_update_local_ts) * 1000.0,
                            lag_ms=st.last_book_lag_sec * 1000.0,
                        )

                    if (time.time() - monitor_start_ts) >= 30.0:
                        now_wall = time.time()
                        feed_silences = (
                            ("chainlink", now_wall - cl.local_recv_ts if cl.local_recv_ts > 0 else 999.0),
                            (
                                "clob",
                                (now_wall - st.last_book_update_local_ts)
                                if st.last_book_update_local_ts > 0.0
                                else (elapsed if st.up_token else 0.0),
                            ),
                            ("binance", now_wall - st.binance_ts if st.binance_ts > 0 else 999.0),
                        )
                        _handle_feed_health_alerts(
                            feed_silences=feed_silences,
                            now_wall=now_wall,
                            last_feed_log_ts=last_feed_warn_ts,
                            last_feed_tg_ts=last_feed_tg_ts,
                            local_active_alerts=local_active_feed_alerts,
                            emit_fn=_emit,
                            tg=tg,
                            shared_risk=shared_risk,
                        )

                    storage.record_snapshot(
                        ts=now,
                        window_ts=window_ts,
                        elapsed_sec=elapsed,
                        chainlink_price=cl_spot,
                        chainlink_twap_60s=cl.twap_60s,
                        chainlink_source=cl.source,
                        cex_lead_price=cex_lead,
                        oracle_gap_bps=fv.oracle_gap_bps if fv else None,
                        binance_price=st.binance_price,
                        coinbase_price=st.coinbase_price,
                        vol_1s=vol_1s,
                        cvd_imb=cvd_imb,
                        obi=obi,
                        z_score=fv.raw_z_score if fv else None,
                        p_true_up=fv.p_true_up if fv else None,
                        up_bid=st.book.up_bid,
                        up_ask=st.book.up_ask,
                        down_bid=st.book.down_bid,
                        down_ask=st.book.down_ask,
                    )

                if fv is not None:
                    maker_filled_closed = False
                    if (
                        live_exec is not None
                        and maker_tp_order_id
                        and strategy.active_position is not None
                        and strategy.active_position.window_ts == window_ts
                        and active_db_trade_id is not None
                        and (now - last_maker_check_ts) >= 2.0
                        and not maker_tp_inflight
                    ):
                        last_maker_check_ts = now
                        maker_tp_inflight = True
                        try:
                            m_res = await asyncio.get_running_loop().run_in_executor(
                                None,
                                live_exec.check_maker_order_status,
                                maker_tp_order_id,
                                0.99,
                                maker_tp_token,
                                strategy.active_position.shares,
                            )
                        finally:
                            maker_tp_inflight = False
                        if m_res.filled and m_res.filled_shares >= strategy.active_position.shares * 0.90:
                            pos_now = strategy.active_position
                            fill_px = m_res.avg_price if m_res.avg_price > 0 else 0.99
                            real_payout = effective_exit_proceeds(
                                fill_px, is_maker=True, fee_rate=cfg.crypto_taker_fee_rate
                            )
                            prior_partial = pos_now.realized_partial_pnl_usdc
                            close_msg = strategy._close_active(now, real_payout, "MAKER_TAKE_PROFIT")
                            last = strategy.closed_trades[-1]
                            final_leg_pnl = last.net_pnl_usdc - prior_partial
                            _emit(
                                f"🟢 [MAKER 停利成交] order={maker_tp_order_id[:19]} | "
                                f"filled={m_res.filled_shares:.2f} @ ${fill_px:.3f} (0% fee) | "
                                f"實得淨損益={last.net_pnl_usdc:+.2f} pUSD"
                            )
                            _emit(f"*** {close_msg} ***")
                            last_bal_sync_ts = 0.0
                            _commit_live_exit_accounting(
                                last=last,
                                final_leg_pnl=final_leg_pnl,
                                now_ts=now,
                                active_db_trade_id=active_db_trade_id,
                                live_exec=live_exec,
                                shared_risk=shared_risk,
                                strategy=strategy,
                                storage=storage,
                                tg=tg,
                                live_mode=live_mode,
                                emit_fn=_emit,
                                tg_commands_only=tg_commands_only,
                            )
                            active_db_trade_id = None
                            maker_tp_order_id = ""
                            maker_tp_token = ""
                            maker_filled_closed = True

                    prev_pos = strategy.active_position
                    action = (
                        None
                        if maker_filled_closed
                        else strategy.evaluate_tick(now, window_ts, elapsed, fv, st.book, strike_source=st.strike_source)
                    )
                    if action:
                        if (
                            prev_pos is None
                            and strategy.active_position is not None
                            and strategy.active_position.window_ts == window_ts
                            and active_db_trade_id is None
                        ):
                            if is_tg_paused:
                                strategy.active_position = None
                                strategy.traded_windows.discard(window_ts)
                                consecutive_loop_errors = 0
                                await asyncio.sleep(loop_sleep)
                                continue

                            p = strategy.active_position
                            side_book_ts = st.up_book_local_ts if p.side == "UP" else st.down_book_local_ts
                            ref_book_ts = side_book_ts or st.last_book_update_local_ts
                            book_age = (time.time() - ref_book_ts) if ref_book_ts > 0.0 else 999.0
                            book_lag = st.up_book_lag_sec if p.side == "UP" else st.down_book_lag_sec
                            live_order_id = ""
                            if live_exec is not None:
                                abort_live_entry = False
                                keep_window_blocked = False
                                if not startup_reconciled:
                                    abort_live_entry = True
                                elif not cl.is_alive(max_silence_sec=4.5):
                                    abort_live_entry = True
                                elif book_age > 1.5 or book_lag > 1.0:
                                    hub.health.stale_book_skips += 1
                                    _emit(
                                        f"⚠️ [LIVE 盤口過舊攔截] book_age={book_age:.2f}s book_lag={book_lag:.2f}s (門檻 1.5s/1.0s)，跳過進場。"
                                    )
                                    abort_live_entry = True
                                elif (now - last_live_attempt_ts) < last_live_attempt_cooldown:
                                    abort_live_entry = True
                                elif shared_risk is not None:
                                    broken, dpnl, dcap = shared_risk.is_broken()
                                    live_exec.daily_pnl_usdc = dpnl
                                    live_exec.max_daily_loss_usdc = dcap
                                    live_exec.circuit_broken = broken
                                    if broken:
                                        print(
                                            f"\n🛑 [LIVE 熔斷攔截] 三幣今日累計損益 ({dpnl:+.2f}) "
                                            f"已達熔斷上限 (-${dcap:.1f})，跳過開倉。"
                                        )
                                        abort_live_entry = True
                                if not abort_live_entry:
                                    last_live_attempt_ts = now
                                    target_token = st.up_token if p.side == "UP" else st.down_token
                                    if not target_token:
                                        abort_live_entry = True
                                    else:
                                        from contextlib import nullcontext

                                        def _locked_live_entry() -> LiveFillResult:
                                            lock_ctx = shared_risk.entry_lock() if shared_risk is not None else nullcontext()
                                            with lock_ctx:
                                                if shared_risk is not None:
                                                    brk, dpnl_i, dcap_i = shared_risk.is_broken()
                                                    live_exec.daily_pnl_usdc = dpnl_i
                                                    live_exec.max_daily_loss_usdc = dcap_i
                                                    live_exec.circuit_broken = brk
                                                    if brk:
                                                        return LiveFillResult(False, 0.0, p.entry_price, error="SHARED_DAILY_LOSS_CIRCUIT_BREAKER")
                                                avail = 0.0
                                                if bet_pct > 0.0:
                                                    fast_bal = (
                                                        shared_risk.get_fast_balance(max_age_sec=30.0, min_safe_usdc=10.0)
                                                        if shared_risk is not None
                                                        else None
                                                    )
                                                    if fast_bal is not None:
                                                        avail = fast_bal
                                                    else:
                                                        avail = live_exec.get_pusd_balance()
                                                        if shared_risk is not None and avail > 0.0:
                                                            shared_risk.record_balance(avail)
                                                    strategy.bankroll = avail
                                                    adj_sh = compute_proportional_shares(
                                                        avail, p.total_cost_per_share, bet_pct, cfg.min_order_shares
                                                    )
                                                    if adj_sh < cfg.min_order_shares:
                                                        return LiveFillResult(
                                                            False,
                                                            0.0,
                                                            p.entry_price,
                                                            error=f"可用 pUSD 餘額不足 5 股 (${avail:.2f})",
                                                        )
                                                    p.shares = adj_sh
                                                slot_acquired = False
                                                if shared_risk is not None:
                                                    slot_acquired = shared_risk.try_acquire_window_slot(
                                                        window_ts, "XRP", max_coins=1
                                                    )
                                                    if not slot_acquired:
                                                        return LiveFillResult(
                                                            False,
                                                            0.0,
                                                            p.entry_price,
                                                            error="WINDOW_SLOT_LIMIT_1_COIN",
                                                        )
                                                res = live_exec.execute_entry(
                                                    token_id=target_token,
                                                    ask_price=p.entry_price,
                                                    shares=p.shares,
                                                    max_price=round(min(0.84, cfg.max_entry_price + 0.02), 2),
                                                    p_true=p.p_true_at_entry,
                                                    fee_rate=cfg.crypto_taker_fee_rate,
                                                    available_pusd=avail,
                                                )
                                                if res.filled and res.filled_shares > 0.0:
                                                    if shared_risk is not None:
                                                        fill_px = res.avg_price if res.avg_price > 0 else p.entry_price
                                                        shared_risk.record_spend(res.filled_shares * fill_px)
                                                elif slot_acquired and shared_risk is not None:
                                                    shared_risk.release_window_slot(window_ts, "XRP")
                                                return res

                                        fill = await asyncio.get_running_loop().run_in_executor(None, _locked_live_entry)
                                        if not fill.filled or fill.filled_shares <= 0.0:
                                            if fill.error == "WINDOW_SLOT_LIMIT_1_COIN":
                                                _emit(
                                                    f"🔒 [跨幣單槽保護] 窗口 {window_ts} 已有他幣成交持倉，本窗口跳過不重複進場。"
                                                )
                                                keep_window_blocked = True
                                                abort_live_entry = True
                                            else:
                                                err_low = str(fill.error or "").lower()
                                                last_live_attempt_cooldown = 2.0 if "no orders found to match" in err_low else 5.0
                                                _emit(f"⚠️ [LIVE FAK 未成交] {fill.error or '流動性不足'}，已自動取消不追價。")
                                                abort_live_entry = True
                                        else:
                                            last_live_attempt_cooldown = 5.0
                                            live_order_id = fill.order_id
                                            p.shares = fill.filled_shares
                                            if fill.avg_price > 0:
                                                p.entry_price = fill.avg_price
                                                p.fee_per_share = taker_fee_per_share(
                                                    p.entry_price, cfg.crypto_taker_fee_rate
                                                )
                                                p.total_cost_per_share = p.entry_price + p.fee_per_share
                                            pf_task = asyncio.create_task(
                                                _bg_prefetch_cond_balance(live_exec, target_token)
                                            )
                                            bg_tasks.add(pf_task)
                                            pf_task.add_done_callback(bg_tasks.discard)
                                            _emit(
                                                f"🔴 [LIVE 實盤成交] order={fill.order_id[:19]} | "
                                                f"shares={p.shares:.2f} @ ${p.entry_price:.3f} (含費成本 ${p.total_cost_per_share:.4f}) | "
                                                f"耗時 {fill.latency_ms:.0f}ms | book_age={book_age*1000:.0f}ms book_lag={book_lag*1000:.0f}ms"
                                            )
                                            if fill.order_id == "RECONCILED_POSITION":
                                                _emit(
                                                    f"🔄 [RECONCILED_POSITION] window={p.window_ts} {p.side} "
                                                    f"shares={p.shares:.2f} @ ${p.entry_price:.3f} ({fill.error or 'timeout'})"
                                                )
                                                if tg.enabled and tg.get_recipient_ids():
                                                    tg.notify_alert(
                                                        "逾時對帳接管部位",
                                                        f"{p.side} {p.shares:.2f} sh @ ${p.entry_price:.3f} ({fill.error or 'timeout'})",
                                                    )
                                if abort_live_entry:
                                    strategy.active_position = None
                                    if not keep_window_blocked:
                                        strategy.traded_windows.discard(window_ts)
                                    consecutive_loop_errors = 0
                                    await asyncio.sleep(loop_sleep)
                                    continue

                            _emit(f"*** {action} | book_age={book_age*1000:.0f}ms book_lag={book_lag*1000:.0f}ms ***")
                            active_db_trade_id = storage.insert_paper_trade(
                                window_ts=p.window_ts,
                                side=p.side,
                                entry_ts=p.entry_ts,
                                entry_elapsed_sec=p.entry_elapsed_sec,
                                shares=p.shares,
                                entry_price=p.entry_price,
                                entry_fee_per_share=p.fee_per_share,
                                p_true_at_entry=p.p_true_at_entry,
                                z_at_entry=p.z_at_entry,
                            )
                            if tg.enabled and not tg_commands_only:
                                tg.notify_entry(
                                    trade_id=active_db_trade_id,
                                    side=p.side,
                                    window_ts=p.window_ts,
                                    elapsed_sec=p.entry_elapsed_sec,
                                    shares=p.shares,
                                    entry_price=p.entry_price,
                                    total_cost_per_share=p.total_cost_per_share,
                                    p_true_at_entry=p.p_true_at_entry,
                                    z_at_entry=p.z_at_entry,
                                    live_mode=live_mode,
                                    order_id=live_order_id,
                                )
                        elif prev_pos is not None and strategy.active_position is None and active_db_trade_id is not None and strategy.closed_trades:
                            last = strategy.closed_trades[-1]
                            final_leg_pnl_commit = last.net_pnl_usdc - prev_pos.realized_partial_pnl_usdc
                            if live_exec is not None:
                                target_token = st.up_token if last.side == "UP" else st.down_token
                                bid_px = st.book.up_bid if last.side == "UP" else st.book.down_bid
                                prior_partial = prev_pos.realized_partial_pnl_usdc
                                rem_est_pnl = last.net_pnl_usdc - prior_partial
                                maker_already_filled: Optional[LiveFillResult] = None
                                if maker_tp_order_id:
                                    _oid_pre = maker_tp_order_id
                                    _tok_pre = maker_tp_token or target_token or ""
                                    await asyncio.get_running_loop().run_in_executor(
                                        None, live_exec.cancel_order_safe, _oid_pre
                                    )
                                    m_pre = await asyncio.get_running_loop().run_in_executor(
                                        None,
                                        live_exec.check_maker_order_status,
                                        _oid_pre,
                                        0.99,
                                        _tok_pre,
                                        prev_pos.shares,
                                    )
                                    maker_tp_order_id = ""
                                    maker_tp_token = ""
                                    if m_pre.filled and m_pre.filled_shares >= prev_pos.shares * 0.90:
                                        maker_already_filled = m_pre

                                if maker_already_filled is not None:
                                    fill_px = maker_already_filled.avg_price if maker_already_filled.avg_price > 0 else 0.99
                                    real_payout = effective_exit_proceeds(
                                        fill_px, is_maker=True, fee_rate=cfg.crypto_taker_fee_rate
                                    )
                                    old_pnl = last.net_pnl_usdc
                                    last.exit_reason = "MAKER_TAKE_PROFIT"
                                    last.exit_payout_per_share = real_payout
                                    final_leg_pnl_commit = last.shares * (real_payout - last.total_cost_per_share)
                                    last.net_pnl_usdc = prior_partial + final_leg_pnl_commit
                                    last.won = last.net_pnl_usdc > 0
                                    strategy.bankroll += (last.net_pnl_usdc - old_pnl)
                                    _emit(
                                        f"🟢 [MAKER 停利已先成交] filled={maker_already_filled.filled_shares:.2f} "
                                        f"@ ${fill_px:.3f} (0% fee) | 實得淨損益={last.net_pnl_usdc:+.2f} pUSD"
                                    )
                                    last_bal_sync_ts = 0.0
                                else:
                                    is_sl = "STOP_LOSS" in last.exit_reason
                                    slip = 0.05 if is_sl else 0.00
                                    min_lim = 0.01 if is_sl else cfg.take_profit_bid
                                    exit_fill = (
                                        await asyncio.get_running_loop().run_in_executor(
                                            None,
                                            lambda: live_exec.execute_exit(
                                                token_id=target_token,
                                                bid_price=bid_px,
                                                shares=last.shares,
                                                slippage_cents=slip,
                                                min_limit_px=min_lim,
                                            ),
                                        )
                                        if (target_token and bid_px)
                                        else None
                                    )
                                    if exit_fill is None or not exit_fill.filled or exit_fill.filled_shares <= 0.0:
                                        err_msg = exit_fill.error if exit_fill else "無可用買盤"
                                        _emit(f"⚠️ [LIVE 平倉未成交] {err_msg}，保留真實持倉繼續盯盤！")
                                        strategy.closed_trades.pop()
                                        strategy.bankroll -= rem_est_pnl
                                        strategy.active_position = prev_pos
                                        consecutive_loop_errors = 0
                                        await asyncio.sleep(loop_sleep)
                                        continue
                                    if exit_fill.filled_shares < prev_pos.shares * 0.90:
                                        part_px = exit_fill.avg_price if exit_fill.avg_price > 0 else bid_px
                                        part_payout = effective_exit_proceeds(
                                            part_px,
                                            is_maker=False,
                                            fee_rate=cfg.crypto_taker_fee_rate,
                                        )
                                        part_pnl = exit_fill.filled_shares * (part_payout - prev_pos.total_cost_per_share)
                                        _emit(
                                            f"🔴 [LIVE 部分平倉] filled={exit_fill.filled_shares:.2f}/{prev_pos.shares:.2f} "
                                            f"@ ${part_px:.3f} (已實現={part_pnl:+.2f} pUSD)，剩餘部位繼續盯盤！"
                                        )
                                        strategy.closed_trades.pop()
                                        strategy.bankroll = strategy.bankroll - rem_est_pnl + part_pnl
                                        prev_pos.realized_partial_pnl_usdc += part_pnl
                                        prev_pos.shares = round(prev_pos.shares - exit_fill.filled_shares, 2)
                                        live_exec.record_realized_pnl(part_pnl)
                                        if shared_risk is not None:
                                            r_state = shared_risk.record_pnl(part_pnl, current_equity=strategy.bankroll)
                                            live_exec.daily_pnl_usdc = float(r_state["daily_realized_pnl_usdc"])
                                            live_exec.max_daily_loss_usdc = float(r_state["max_daily_loss_usdc"])
                                            live_exec.circuit_broken = bool(r_state["circuit_broken"])
                                        strategy.active_position = prev_pos
                                        consecutive_loop_errors = 0
                                        await asyncio.sleep(loop_sleep)
                                        continue
                                    final_leg_pnl_commit = rem_est_pnl
                                    if exit_fill.avg_price > 0:
                                        real_payout = effective_exit_proceeds(
                                            exit_fill.avg_price,
                                            is_maker=False,
                                            fee_rate=cfg.crypto_taker_fee_rate,
                                        )
                                        old_pnl = last.net_pnl_usdc
                                        last.exit_payout_per_share = real_payout
                                        final_leg_pnl_commit = last.shares * (real_payout - last.total_cost_per_share)
                                        last.net_pnl_usdc = prior_partial + final_leg_pnl_commit
                                        last.won = last.net_pnl_usdc > 0
                                        strategy.bankroll += (last.net_pnl_usdc - old_pnl)
                                    _emit(
                                        f"🔴 [LIVE 實盤平倉] {last.exit_reason} | filled={exit_fill.filled_shares:.2f} "
                                        f"@ ${exit_fill.avg_price:.3f} | 實得淨損益={last.net_pnl_usdc:+.2f} pUSD | "
                                        f"耗時 {exit_fill.latency_ms:.0f}ms"
                                    )
                                    last_bal_sync_ts = 0.0

                            _emit(f"*** {action} ***")
                            _commit_live_exit_accounting(
                                last=last,
                                final_leg_pnl=final_leg_pnl_commit,
                                now_ts=now,
                                active_db_trade_id=active_db_trade_id,
                                live_exec=live_exec,
                                shared_risk=shared_risk,
                                strategy=strategy,
                                storage=storage,
                                tg=tg,
                                live_mode=live_mode,
                                emit_fn=_emit,
                                tg_commands_only=tg_commands_only,
                            )
                            active_db_trade_id = None
                            maker_tp_order_id = ""
                            maker_tp_token = ""

                    if (
                        live_exec is not None
                        and not maker_tp_order_id
                        and maker_tp_placed_window != window_ts
                        and not maker_tp_inflight
                        and strategy.active_position is not None
                        and strategy.active_position.window_ts == window_ts
                        and active_db_trade_id is not None
                        and elapsed < 292.0
                    ):
                        pos_open = strategy.active_position
                        cur_bid = st.book.up_bid if pos_open.side == "UP" else st.book.down_bid
                        cur_p = fv.p_true_up if pos_open.side == "UP" else fv.p_true_down
                        tok_open = st.up_token if pos_open.side == "UP" else st.down_token
                        if (
                            tok_open
                            and cur_bid is not None
                            and 0.94 <= cur_bid < 0.99
                            and cur_p >= 0.95
                            and live_exec._cond_balance_cache.get(tok_open, 0.0) >= 5.0
                        ):
                            maker_tp_placed_window = window_ts
                            maker_tp_inflight = True
                            try:
                                m_post = await asyncio.get_running_loop().run_in_executor(
                                    None,
                                    live_exec.post_maker_tp_sell,
                                    tok_open,
                                    pos_open.shares,
                                    0.99,
                                )
                            finally:
                                maker_tp_inflight = False
                            if m_post.order_id and not m_post.error:
                                maker_tp_order_id = m_post.order_id
                                maker_tp_token = tok_open
                                last_maker_check_ts = now
                                _emit(
                                    f"📌 [MAKER TP 掛單] order={maker_tp_order_id[:19]} @ $0.990 "
                                    f"({pos_open.shares:.2f} sh, 0% fee)"
                                )

                    if is_1hz_tick:
                        up_ask_str = f"{st.book.up_ask:.2f}" if st.book.up_ask else " -- "
                        dn_ask_str = f"{st.book.down_ask:.2f}" if st.book.down_ask else " -- "
                        up_edge = (
                            (
                                fv.p_true_up
                                - st.book.up_ask
                                - taker_fee_per_share(st.book.up_ask, fee_rate=cfg.crypto_taker_fee_rate)
                            )
                            * 100
                            if (st.book.up_ask and st.book.up_ask >= 0.05)
                            else -99.0
                        )
                        dn_edge = (
                            (
                                fv.p_true_down
                                - st.book.down_ask
                                - taker_fee_per_share(st.book.down_ask, fee_rate=cfg.crypto_taker_fee_rate)
                            )
                            * 100
                            if (st.book.down_ask and st.book.down_ask >= 0.05)
                            else -99.0
                        )
                        best_edge = max(up_edge, dn_edge)
                        edge_str = f"{best_edge:+5.1f}%" if best_edge > -90 else "  --  "
                        diff = cl_spot - st.strike_price
                        zone_tag = (
                            "SNIPE"
                            if (cfg.entry_start_elapsed_sec <= elapsed <= cfg.entry_end_elapsed_sec)
                            else "WAIT "
                        )
                        if is_tty:
                            line = (
                                f"[{zone_tag} 剩{int(remaining):3d}s] CL={cl_spot:,.4f} "
                                f"(差={diff:+7.4f} | Gap={fv.oracle_gap_bps:+5.1f}bp | Z={fv.raw_z_score:+5.2f}) | "
                                f"P(Up)={fv.p_true_up*100:5.1f}% | Ask={up_ask_str}/{dn_ask_str} | Edge={edge_str}"
                            )
                            print("\r" + _fit_console_line(line), end="", flush=True)
                elif is_1hz_tick and is_tty:
                    cl_str = f"${cl_spot:,.4f}" if cl_spot else "斷線重連中..."
                    cex_str = f"${cex_lead:,.4f}" if cex_lead else "連線中..."
                    line = f"[等待整點鎖定 Strike | 剩 {int(remaining):3d}s 開新盤] Chainlink={cl_str} ({cl.source}) | CEX={cex_str}"
                    print("\r" + _fit_console_line(line), end="", flush=True)

                consecutive_loop_errors = 0
                await hub.wait_next_tick(
                    loop_sleep,
                    min_interval_sec=0.025 if hot_zone else 0.95,
                )
            except (KeyboardInterrupt, asyncio.CancelledError):
                raise
            except Exception as exc:
                consecutive_loop_errors += 1
                err_line = f"[MONITOR LOOP WARN] {type(exc).__name__}: {exc}"
                if event_logger is not None:
                    event_logger.warning(err_line)
                sys.stderr.write(f"\n{err_line}\n")
                await asyncio.sleep(min(5.0, 1.0 * consecutive_loop_errors))
    except KeyboardInterrupt:
        print("\n已安全停止模擬盤監控。")
    finally:
        hub.stop()
        tg.stop()
        feed_task.cancel()
        if tg_task is not None:
            tg_task.cancel()
        storage.close()
        if event_logger is not None:
            for h in list(event_logger.handlers):
                event_logger.removeHandler(h)
                h.close()
        try:
            instance_lock.close()
        except Exception:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Polymarket XRP 5m Chainlink 模擬盤觀測")
    parser.add_argument("--db", default="data/live_xrp_5m.db", help="SQLite 資料庫路徑")
    parser.add_argument("--bankroll", type=float, default=1000.0, help="模擬初始總本金 (USDC)")
    parser.add_argument("--bet", type=float, default=25.0, help="每筆固定下注金額 USDC (設 0 則改用 1/5 凱利公式)")
    parser.add_argument("--live", action="store_true", help="啟用真實 pUSD 實盤自動下單 (需先通過 check_account.py)")
    parser.add_argument("--max-daily-loss", type=float, default=50.0, help="實盤單日最大虧損熔斷金額 (pUSD)")
    parser.add_argument("--bet-pct", type=float, default=0.0, help="按即時權益等比下注比例 (例: 0.10 = 10%，優先於 --bet)")
    parser.add_argument("--max-daily-loss-pct", type=float, default=0.0, help="三幣共用單日最大虧損比例熔斷 (例: 0.30 = 30%)")
    parser.add_argument("--log-file", default="", help="結構化日誌檔路徑 (50MB 輪替保留 5 份；非終端機停止 \\r 狀態輸出)")
    parser.add_argument("--tg-commands-only", action="store_true", help="保留 TG 指令輪詢與健康告警，但靜音本幣別進出場成交推播")
    args = parser.parse_args()
    asyncio.run(
        monitor_loop(
            args.db,
            args.bankroll,
            args.bet,
            live_mode=args.live,
            max_daily_loss=args.max_daily_loss,
            bet_pct=args.bet_pct,
            max_daily_loss_pct=args.max_daily_loss_pct,
            log_file=args.log_file,
            tg_commands_only=args.tg_commands_only,
        )
    )
