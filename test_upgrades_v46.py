"""Unit tests for v4.6 execution speed, Maker @ $0.99 TP, window slot guard, and XRP/DOGE scouts."""
from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path
import sqlite3
import sys
import time
import types
from typing import Any, Dict

import pytest

FRIEND_ROOT = Path(__file__).resolve().parent
if str(FRIEND_ROOT) not in sys.path:
    sys.path.insert(0, str(FRIEND_ROOT))

import daily_review  # noqa: E402
from shared_risk import SharedDailyRiskGuard  # noqa: E402


def _load_coin_src(coin_dir: str, with_monitor: bool = False) -> Dict[str, types.ModuleType]:
    """Load coin `src` modules (and optional `run_monitor`) under isolated package names."""
    pkg_dir = FRIEND_ROOT / coin_dir / "src"
    pkg_name = f"_v46_{coin_dir.replace('-', '_')}"
    pkg = types.ModuleType(pkg_name)
    pkg.__path__ = [str(pkg_dir)]
    sys.modules[pkg_name] = pkg

    names = (
        "config",
        "fees",
        "pricing_model",
        "strategy",
        "storage",
        "chainlink_oracle",
        "feeds",
        "live_executor",
        "tg_notifier",
    )
    mods: Dict[str, types.ModuleType] = {}
    for mod_name in names:
        spec = importlib.util.spec_from_file_location(f"{pkg_name}.{mod_name}", pkg_dir / f"{mod_name}.py")
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        mod.__package__ = pkg_name
        sys.modules[f"{pkg_name}.{mod_name}"] = mod
        spec.loader.exec_module(mod)
        mods[mod_name] = mod

    if with_monitor:
        saved = {"src": sys.modules.get("src"), **{f"src.{n}": sys.modules.get(f"src.{n}") for n in names}}
        src_alias = types.ModuleType("src")
        src_alias.__path__ = [str(pkg_dir)]
        sys.modules["src"] = src_alias
        for mod_name in names:
            sys.modules[f"src.{mod_name}"] = mods[mod_name]
        try:
            rm_spec = importlib.util.spec_from_file_location(f"{pkg_name}_rm", FRIEND_ROOT / coin_dir / "run_monitor.py")
            assert rm_spec is not None and rm_spec.loader is not None
            rm_mod = importlib.util.module_from_spec(rm_spec)
            rm_spec.loader.exec_module(rm_mod)
            mods["run_monitor"] = rm_mod
        finally:
            for k, old in saved.items():
                if old is None:
                    sys.modules.pop(k, None)
                else:
                    sys.modules[k] = old
    return mods


class _FakeClobClient:
    """Minimal mock py-clob-client-v2 client capturing market, limit, status, and cancel calls."""

    def __init__(self) -> None:
        self.market_calls: list[dict[str, Any]] = []
        self.limit_calls: list[dict[str, Any]] = []
        self.cancelled_batches: list[list[str]] = []
        self.order_detail: dict[str, Any] = {}
        self.limit_exc: Exception | None = None

    def get_tick_size(self, token_id: str) -> str:
        del token_id
        return "0.01"

    def create_and_post_market_order(self, order_args: Any, options: Any = None, order_type: Any = None) -> dict[str, Any]:
        self.market_calls.append({"args": order_args, "options": options, "order_type": order_type})
        return {"orderID": "0xexit1", "size_matched": str(order_args.amount), "price": str(order_args.price)}

    def create_and_post_order(
        self,
        order_args: Any,
        options: Any = None,
        order_type: Any = None,
        post_only: bool = False,
    ) -> dict[str, Any]:
        self.limit_calls.append(
            {"args": order_args, "options": options, "order_type": order_type, "post_only": post_only}
        )
        if self.limit_exc is not None:
            raise self.limit_exc
        return {"orderID": "0xmaker99", "status": "live"}

    def get_order(self, order_id: str) -> dict[str, Any]:
        del order_id
        return dict(self.order_detail)

    def cancel_orders(self, order_hashes: list[str]) -> dict[str, Any]:
        self.cancelled_batches.append(list(order_hashes))
        return {"canceled": list(order_hashes)}


def _build_stub_executor(le_mod: types.ModuleType) -> tuple[Any, _FakeClobClient]:
    exec_obj = le_mod.PolymarketLiveExecutor.__new__(le_mod.PolymarketLiveExecutor)
    client = _FakeClobClient()
    exec_obj.client = client
    exec_obj.sig_type = 2
    exec_obj.max_daily_loss_usdc = 25.0
    exec_obj.daily_pnl_usdc = 0.0
    exec_obj.circuit_broken = False
    exec_obj._day_utc = 0
    exec_obj._tick_size_cache = {"tok_up": "0.01"}
    exec_obj._cond_balance_cache = {}
    exec_obj._warmed_tokens = set()
    exec_obj._last_keepalive_ts = 0.0
    exec_obj._reconcile_delay_sec = 0.0
    return exec_obj, client


def test_wait_next_tick_wakes_immediately_on_book_and_chainlink() -> None:
    mods = _load_coin_src("polymarket-btc-5m")
    feeds_mod = mods["feeds"]
    pm_mod = mods["pricing_model"]

    async def _run() -> tuple[tuple[bool, float], tuple[bool, float], tuple[bool, float]]:
        hub = feeds_mod.LiveMarketHub(pm_mod.RollingMicrostructureTracker())
        win_ts = (int(time.time()) // 300) * 300
        hub.sync_window_boundary(float(win_ts + 160))
        hub.state.up_token = "tok_up"
        hub.apply_clob_book_payload(
            "tok_up",
            {"bids": [{"price": "0.77", "size": "50"}], "asks": [{"price": "0.79", "size": "50"}]},
            via_ws=True,
        )
        hub._wake_event.clear()
        hub._last_eval_ts = 0.0

        async def _push_pc() -> None:
            await asyncio.sleep(0.01)
            hub.apply_clob_price_change_payload(
                {"price_changes": [{"asset_id": "tok_up", "side": "SELL", "price": "0.80", "size": "40"}]},
                via_ws=True,
            )

        t_pc = asyncio.create_task(_push_pc())
        t0 = time.monotonic()
        woke_pc = await hub.wait_next_tick(0.25, min_interval_sec=0.005)
        dt_pc = time.monotonic() - t0
        await t_pc

        async def _push_cl() -> None:
            await asyncio.sleep(0.01)
            hub.chainlink.on_chainlink_tick(59123.45, "POLY_RTDS_CHAINLINK", oracle_ts=time.time())

        t_cl = asyncio.create_task(_push_cl())
        t1 = time.monotonic()
        woke_cl = await hub.wait_next_tick(0.25, min_interval_sec=0.005)
        dt_cl = time.monotonic() - t1
        await t_cl

        t2 = time.monotonic()
        woke_to = await hub.wait_next_tick(0.03, min_interval_sec=0.01)
        dt_to = time.monotonic() - t2
        return (woke_pc, dt_pc), (woke_cl, dt_cl), (woke_to, dt_to)

    res_pc, res_cl, res_to = asyncio.run(_run())
    assert res_pc[0] is True and res_pc[1] < 0.08
    assert res_cl[0] is True and res_cl[1] < 0.08
    assert res_to[0] is False and res_to[1] < 0.08


def test_compute_entry_terms_ev_guarded_084_buffer() -> None:
    mods = _load_coin_src("polymarket-btc-5m")
    exec_obj, _ = _build_stub_executor(mods["live_executor"])

    lim_high, _ = exec_obj._compute_entry_terms(
        ask_price=0.82, shares=5.2, max_price=0.84, p_true=0.91, fee_rate=0.07, available_pusd=42.0
    )
    assert lim_high == 0.84

    lim_tight, _ = exec_obj._compute_entry_terms(
        ask_price=0.82, shares=5.2, max_price=0.84, p_true=0.875, fee_rate=0.07, available_pusd=42.0
    )
    assert lim_tight == 0.82

    lim_mid, _ = exec_obj._compute_entry_terms(
        ask_price=0.78, shares=5.2, max_price=0.84, p_true=0.90, fee_rate=0.07, available_pusd=42.0
    )
    assert lim_mid == 0.82


def test_execute_exit_tp_strict_floor_vs_sl_sweep() -> None:
    mods = _load_coin_src("polymarket-btc-5m")
    exec_obj, client = _build_stub_executor(mods["live_executor"])
    exec_obj._cond_balance_cache["tok_up"] = 5.5

    r_tp = exec_obj.execute_exit("tok_up", bid_price=0.96, shares=5.5, slippage_cents=0.0, min_limit_px=0.98)
    assert r_tp.filled is True
    assert client.market_calls[-1]["args"].price == 0.98

    exec_obj._cond_balance_cache["tok_up"] = 5.5
    r_sl = exec_obj.execute_exit("tok_up", bid_price=0.40, shares=5.5, slippage_cents=0.05, min_limit_px=0.01)
    assert r_sl.filled is True
    assert client.market_calls[-1]["args"].price == 0.35


def test_maker_tp_lifecycle_and_safe_cancel() -> None:
    mods = _load_coin_src("polymarket-btc-5m")
    exec_obj, client = _build_stub_executor(mods["live_executor"])

    exec_obj._cond_balance_cache["tok_up"] = 4.9
    r_small = exec_obj.post_maker_tp_sell("tok_up", shares=4.9, price=0.99)
    assert r_small.filled is False and "5-share" in (r_small.error or "")
    assert client.limit_calls == []

    exec_obj._cond_balance_cache["tok_up"] = 5.5
    r_post = exec_obj.post_maker_tp_sell("tok_up", shares=5.5, price=0.99)
    assert r_post.filled is False
    assert r_post.order_id == "0xmaker99"
    assert client.limit_calls[-1]["post_only"] is True
    assert client.limit_calls[-1]["args"].price == 0.99
    assert client.limit_calls[-1]["args"].size == 5.5

    client.limit_exc = RuntimeError("post_only order would cross")
    r_err = exec_obj.post_maker_tp_sell("tok_up", shares=5.5, price=0.99)
    assert r_err.filled is False and "cross" in (r_err.error or "")

    client.order_detail = {"orderID": "0xmaker99", "size_matched": "5.5", "price": "0.99", "asset_id": "tok_up"}
    r_chk = exec_obj.check_maker_order_status("0xmaker99", price=0.99, token_id="tok_up", posted_qty=5.5)
    assert r_chk.filled is True and r_chk.filled_shares == 5.5 and r_chk.avg_price == 0.99
    assert "tok_up" not in exec_obj._cond_balance_cache

    assert exec_obj.cancel_order_safe("0xmaker99") is True
    assert client.cancelled_batches == [["0xmaker99"]]


def test_shared_window_slot_cap_release_and_rollover_prune(tmp_path: Path) -> None:
    guard = SharedDailyRiskGuard(tmp_path / "shared_live_risk.json", max_daily_loss_pct=0.30)
    guard.sync_day_equity(42.0, now_ts=1795320000.0)
    wts = 1795320000

    assert guard.try_acquire_window_slot(wts, "btc", max_coins=2) is True
    assert guard.try_acquire_window_slot(wts, "eth", max_coins=2) is True
    assert guard.try_acquire_window_slot(wts, "eth", max_coins=2) is True
    assert guard.try_acquire_window_slot(wts, "sol", max_coins=2) is False

    guard.release_window_slot(wts, "eth")
    assert guard.try_acquire_window_slot(wts, "sol", max_coins=2) is True

    next_day = guard.sync_day_equity(45.0, now_ts=1795406400.0)
    assert next_day["active_window_slots"].get(str(wts)) == ["BTC", "SOL"]

    assert guard.try_acquire_window_slot(wts + 900, "eth", max_coins=2) is True
    st = guard._read_state_unlocked()
    assert str(wts) not in st["active_window_slots"]
    assert st["active_window_slots"][str(wts + 900)] == ["ETH"]


def test_xrp_doge_normalizers_calibration_and_daily_review_scouts(tmp_path: Path) -> None:
    xrp_mods = _load_coin_src("polymarket-xrp-5m")
    doge_mods = _load_coin_src("polymarket-doge-5m")

    assert xrp_mods["chainlink_oracle"].normalize_xrp_usd_price(0.9219) == 0.9219
    assert xrp_mods["chainlink_oracle"].normalize_xrp_usd_price(999.0) is None
    assert doge_mods["chainlink_oracle"].normalize_doge_usd_price(0.05442) == 0.05442
    assert doge_mods["chainlink_oracle"].normalize_doge_usd_price(50.0) is None

    for mods, cl_px, dec in ((xrp_mods, 0.9200, 4), (doge_mods, 0.05400, 5)):
        st = mods["feeds"].LiveFeedState(
            strike_price=cl_px,
            _initial_cl_strike=cl_px,
            strike_source="POLY_RTDS_CHAINLINK",
            book=mods["strategy"].OrderBookTop(up_bid=0.69, up_ask=0.71),
        )
        for _ in range(3):
            out = st.calibrate_strike_from_opening_book(cl_px, vol_1s=0.0001, elapsed_sec=5.0)
        assert out is not None and st.strike_source == "BOOK_CALIBRATED"
        assert round(out, dec) == out

    db_dir = tmp_path / "polymarket-xrp-5m" / "data"
    db_dir.mkdir(parents=True)
    con = sqlite3.connect(db_dir / "live_xrp_5m.db")
    con.execute("CREATE TABLE windows (window_ts INTEGER PRIMARY KEY)")
    con.execute(
        "CREATE TABLE paper_trades (id INTEGER PRIMARY KEY, entry_ts REAL, entry_elapsed_sec REAL, "
        "z_at_entry REAL, exit_ts REAL, exit_reason TEXT, net_pnl_usdc REAL, won INTEGER)"
    )
    con.execute("INSERT INTO windows VALUES (1795320000)")
    con.execute("INSERT INTO paper_trades VALUES (1, 1795320180, 180.0, 1.4, 1795320260, 'EARLY_TAKE_PROFIT', 0.95, 1)")
    con.commit()
    con.close()

    scouts = daily_review._collect_paper_scouts(tmp_path)
    assert len(scouts) == 1
    assert scouts[0]["coin"] == "XRP" and scouts[0]["wins"] == 1 and scouts[0]["net_pnl_usdc"] == 0.95


def test_reconcile_startup_position_executor_thread_safe(tmp_path: Path) -> None:
    """Confirm startup balance fetch runs off-thread while SQLite adoption stays on main thread."""
    mods = _load_coin_src("polymarket-sol-5m", with_monitor=True)
    rm = mods["run_monitor"]
    storage = mods["storage"].MarketDataStorage(str(tmp_path / "sol_test.db"))
    strategy = mods["strategy"].HighProbSniperStrategy(mods["config"].DEFAULT_CONFIG)
    wts = 1795322100
    existing_id = storage.insert_paper_trade(wts, "DOWN", wts + 231.0, 231.0, 5.33, 0.80, 0.0112, 0.953, 1.97)

    class _StubExec:
        def sync_conditional_balance(self, tok: str) -> float:
            return 5.33 if tok == "tok_dn" else 0.0

        def lookup_recent_buy_fill(self, tok: str, since_ts: float) -> tuple[float, float]:
            del tok, since_ts
            return 5.33, 0.80

    async def _run() -> int | None:
        prefetched = await asyncio.get_running_loop().run_in_executor(
            None, rm._fetch_startup_wallet_position, "tok_up", "tok_dn", _StubExec(), wts
        )
        assert prefetched == ("DOWN", 5.33, "tok_dn", 0.80)
        return rm.reconcile_startup_position(
            wts, 242.0, "tok_up", "tok_dn", _StubExec(), storage, strategy,
            mods["config"].DEFAULT_CONFIG, wts + 242.0, 0.19, 0.81, prefetched=prefetched,
        )

    adopted_id = asyncio.run(_run())
    storage.close()
    assert adopted_id == existing_id
    assert strategy.active_position is not None
    assert strategy.active_position.side == "DOWN"
    assert strategy.active_position.shares == 5.33
    assert wts in strategy.traded_windows


def test_shared_alert_dedupe_and_recovery_across_processes(tmp_path: Path) -> None:
    """Confirm cross-process alert dedupe, rollover preservation, and single recovery."""
    guard_a = SharedDailyRiskGuard(tmp_path / "shared_live_risk.json", max_daily_loss_pct=0.30)
    guard_b = SharedDailyRiskGuard(tmp_path / "shared_live_risk.json", max_daily_loss_pct=0.30)
    t0 = 1795320000.0
    guard_a.sync_day_equity(42.0, now_ts=t0)

    assert guard_a.claim_alert("feed:clob", cooldown_sec=1800.0, mark_active=True, now_ts=t0 + 100.0) is True
    assert guard_b.claim_alert("feed:clob", cooldown_sec=1800.0, mark_active=True, now_ts=t0 + 105.0) is False
    assert guard_b.claim_alert("feed:binance", cooldown_sec=1800.0, mark_active=True, now_ts=t0 + 105.0) is True

    next_day = guard_a.sync_day_equity(45.0, now_ts=t0 + 86400.0)
    assert "feed:clob" in next_day["alert_dedupe"]
    assert "feed:clob" in next_day["alert_active"]

    assert guard_b.resolve_alert("feed:clob", now_ts=t0 + 145.0) == 45.0
    assert guard_a.resolve_alert("feed:clob", now_ts=t0 + 150.0) is None
    assert guard_b.claim_alert("feed:clob", cooldown_sec=1800.0, mark_active=True, now_ts=t0 + 1901.0) is True


def test_clock_sync_rejects_high_rtt_and_tracks_consecutive_warns(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reject >0.6s congested RTT probes and require two consecutive clean skews before warn latch."""
    co_mod = _load_coin_src("polymarket-btc-5m")["chainlink_oracle"]
    feed = co_mod.ChainlinkOracleFeed()
    clock = [1_795_320_000.0]
    monkeypatch.setattr(co_mod.time, "time", lambda: clock[0])

    class _ScriptedSession:
        def __init__(self, probes: list[tuple[float, float]]) -> None:
            self._probes = list(probes)
            self._idx = 0

        def get(self, url: str, timeout: float = 2.0) -> Any:
            del url, timeout
            rtt, offset = self._probes[self._idx]
            self._idx += 1
            t_req = clock[0]
            clock[0] = t_req + rtt
            srv_ms = int((t_req + 0.5 * rtt + offset) * 1000.0)
            return types.SimpleNamespace(status_code=200, json=lambda m=srv_ms: {"serverTime": m})

    assert feed.sync_clock_precise(_ScriptedSession([(0.85, 0.70), (0.95, 0.75), (1.10, 0.80)])) is None
    assert feed.clock_offset_sec == 0.0 and feed._consecutive_clock_warn_count == 0

    raw_clean = feed.sync_clock_precise(_ScriptedSession([(0.90, 0.65), (0.04, 0.02), (0.06, 0.02)]))
    assert raw_clean is not None and abs(raw_clean) < 50.0 and feed._consecutive_clock_warn_count == 0

    assert feed.sync_clock_precise(_ScriptedSession([(0.04, 0.31)] * 3)) > 250.0
    assert feed._consecutive_clock_warn_count == 1
    assert feed.sync_clock_precise(_ScriptedSession([(0.04, 0.32)] * 3)) > 250.0
    assert feed._consecutive_clock_warn_count == 2
    assert abs(feed.sync_clock_precise(_ScriptedSession([(0.04, 0.01)] * 3))) < 50.0
    assert feed._consecutive_clock_warn_count == 0


def test_rtds_standby_zombie_timeout_and_backoff() -> None:
    """Relax RTDS zombie threshold and exponential backoff while primary stream is healthy."""
    co_mod = _load_coin_src("polymarket-btc-5m")["chainlink_oracle"]
    feed = co_mod.ChainlinkOracleFeed()
    t_now = time.time()

    assert feed._rtds_zombie_timeout_sec(t_now) == 3.5
    feed.on_chainlink_tick(59100.0, "POLYBOLT_CHAINLINK", oracle_ts=t_now)
    assert feed._rtds_zombie_timeout_sec(t_now + 1.0) == 15.0
    assert feed._rtds_zombie_timeout_sec(t_now + 3.5) == 3.5

    assert co_mod.ChainlinkOracleFeed._rtds_reconnect_delay_sec(1, True) == 2.0
    assert co_mod.ChainlinkOracleFeed._rtds_reconnect_delay_sec(2, True) == 4.0
    assert co_mod.ChainlinkOracleFeed._rtds_reconnect_delay_sec(4, True) == 15.0
    assert co_mod.ChainlinkOracleFeed._rtds_reconnect_delay_sec(1, False) == 1.0
    assert co_mod.ChainlinkOracleFeed._rtds_reconnect_delay_sec(2, False) == 1.5
    assert co_mod.ChainlinkOracleFeed._rtds_reconnect_delay_sec(9, False) == 5.0


def test_feed_health_alert_30s_gate_dedupe_and_recovery(tmp_path: Path) -> None:
    """Keep local log at 15s, gate TG alerts at >=30s with cross-coin dedupe, and emit 1 recovery."""
    rm = _load_coin_src("polymarket-btc-5m", with_monitor=True)["run_monitor"]
    guard = SharedDailyRiskGuard(tmp_path / "shared_live_risk.json")
    t0 = 1_795_320_000.0
    guard.sync_day_equity(44.0, now_ts=t0)

    class _StubTg:
        def __init__(self) -> None:
            self.enabled = True
            self.alerts: list[tuple[str, str]] = []

        def get_recipient_ids(self) -> list[str]:
            return ["111111111"]

        def notify_alert(self, title: str, detail: str) -> None:
            self.alerts.append((title, detail))

    tg_btc, tg_eth = _StubTg(), _StubTg()
    emits_btc, emits_eth = [], []
    log_btc, log_eth = {}, {}
    tg_ts_btc, tg_ts_eth, act_btc, act_eth = {}, {}, {}, {}

    rm._handle_feed_health_alerts((("clob", 12.0),), t0 + 12, log_btc, tg_ts_btc, act_btc, emits_btc.append, tg_btc, guard)
    assert emits_btc == [] and tg_btc.alerts == []

    rm._handle_feed_health_alerts((("clob", 16.0),), t0 + 16, log_btc, tg_ts_btc, act_btc, emits_btc.append, tg_btc, guard)
    assert len(emits_btc) == 1 and "[FEED WARN] clob silent=16.0s (>15s)" in emits_btc[0]
    assert tg_btc.alerts == []

    rm._handle_feed_health_alerts((("clob", 32.0),), t0 + 32, log_btc, tg_ts_btc, act_btc, emits_btc.append, tg_btc, guard)
    rm._handle_feed_health_alerts((("clob", 33.0),), t0 + 33, log_eth, tg_ts_eth, act_eth, emits_eth.append, tg_eth, guard)
    assert len(tg_btc.alerts) == 1 and "超過 30 秒" in tg_btc.alerts[0][0]
    assert tg_eth.alerts == []

    rm._handle_feed_health_alerts((("clob", 1.0),), t0 + 50, log_eth, tg_ts_eth, act_eth, emits_eth.append, tg_eth, guard)
    rm._handle_feed_health_alerts((("clob", 1.0),), t0 + 51, log_btc, tg_ts_btc, act_btc, emits_btc.append, tg_btc, guard)
    assert len(tg_eth.alerts) == 1 and "已恢復" in tg_eth.alerts[0][0]
    assert len(tg_btc.alerts) == 1

    tg_scout = _StubTg()
    emits_s, log_s, ts_s, act_s = [], {}, {}, {}
    rm._handle_feed_health_alerts((("chainlink", 35.0),), t0 + 35, log_s, ts_s, act_s, emits_s.append, tg_scout, None)
    rm._handle_feed_health_alerts((("chainlink", 40.0),), t0 + 40, log_s, ts_s, act_s, emits_s.append, tg_scout, None)
    rm._handle_feed_health_alerts((("chainlink", 0.5),), t0 + 55, log_s, ts_s, act_s, emits_s.append, tg_scout, None)
    assert [a[0] for a in tg_scout.alerts] == ["資料源斷線超過 30 秒", "資料源已恢復"]
