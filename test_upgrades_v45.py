"""Unit tests for v4.5 reliability, freshness, clock sync, reconciliation, and health upgrades."""
from __future__ import annotations

import importlib.util
import sqlite3
import sys
import time
import types
from pathlib import Path
from typing import Any, Dict, List

import pytest

FRIEND_ROOT = Path(__file__).resolve().parent
COIN_DIRS = ("polymarket-btc-5m", "polymarket-eth-5m", "polymarket-sol-5m")

if str(FRIEND_ROOT) not in sys.path:
    sys.path.insert(0, str(FRIEND_ROOT))

import daily_review  # noqa: E402


def _load_coin_modules(coin_dir_name: str, with_monitor: bool = False) -> Dict[str, types.ModuleType]:
    """Load coin `src` modules (and optional `run_monitor`) under isolated package names."""
    pkg_dir = FRIEND_ROOT / coin_dir_name / "src"
    pkg_name = f"_v45_{coin_dir_name.replace('-', '_')}"
    pkg = types.ModuleType(pkg_name)
    pkg.__path__ = [str(pkg_dir)]
    sys.modules[pkg_name] = pkg

    mods: Dict[str, types.ModuleType] = {}
    names = ("config", "fees", "pricing_model", "strategy", "storage", "chainlink_oracle", "feeds", "live_executor", "tg_notifier")
    for mod_name in names:
        spec = importlib.util.spec_from_file_location(f"{pkg_name}.{mod_name}", pkg_dir / f"{mod_name}.py")
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        mod.__package__ = pkg_name
        sys.modules[f"{pkg_name}.{mod_name}"] = mod
        spec.loader.exec_module(mod)
        mods[mod_name] = mod

    if with_monitor:
        saved_src: Dict[str, Any] = {"src": sys.modules.get("src")}
        for mod_name in names:
            saved_src[f"src.{mod_name}"] = sys.modules.get(f"src.{mod_name}")
        try:
            sys.modules["src"] = pkg
            for mod_name, mod in mods.items():
                sys.modules[f"src.{mod_name}"] = mod
            mon_spec = importlib.util.spec_from_file_location(f"{pkg_name}_run_monitor", FRIEND_ROOT / coin_dir_name / "run_monitor.py")
            assert mon_spec is not None and mon_spec.loader is not None
            mon_mod = importlib.util.module_from_spec(mon_spec)
            mon_spec.loader.exec_module(mon_mod)
            mods["run_monitor"] = mon_mod
        finally:
            for key, prev in saved_src.items():
                if prev is None:
                    sys.modules.pop(key, None)
                else:
                    sys.modules[key] = prev
    return mods


@pytest.mark.parametrize("coin_dir", COIN_DIRS)
def test_p0_1_incremental_l2_book_and_price_change(coin_dir: str) -> None:
    """Confirm `book` snapshots + `price_change` deltas update VWAP top cleanly while filtering dust."""
    mods = _load_coin_modules(coin_dir)
    RollingMicrostructureTracker = mods["pricing_model"].RollingMicrostructureTracker
    LiveMarketHub = mods["feeds"].LiveMarketHub

    hub = LiveMarketHub(RollingMicrostructureTracker(), target_usdc=25.0)
    win_ts = (int(time.time()) // 300) * 300
    hub.sync_window_boundary(float(win_ts + 200))
    hub.state.up_token = "tok_up_1"
    hub.state.down_token = "tok_dn_1"
    ev_ms = str(int(hub.utc_now() * 1000))

    assert hub.apply_clob_book_payload(
        "tok_up_1",
        {
            "event_type": "book",
            "asset_id": "tok_up_1",
            "timestamp": ev_ms,
            "bids": [{"price": "0.78", "size": "60"}],
            "asks": [{"price": "0.79", "size": "50"}, {"price": "0.80", "size": "50"}],
        },
        via_ws=True,
    )
    assert hub.state.book.up_bid == pytest.approx(0.78)
    assert hub.state.book.up_ask == pytest.approx(0.79)

    assert hub.apply_clob_price_change_payload(
        {
            "event_type": "price_change",
            "timestamp": ev_ms,
            "price_changes": [
                {"asset_id": "tok_up_1", "price": "0.79", "size": "0", "side": "SELL"},
                {"asset_id": "tok_up_1", "price": "0.81", "size": "45", "side": "SELL"},
            ],
        },
        via_ws=True,
    )
    assert hub.state.book.up_ask == pytest.approx(0.80)
    assert hub.health.clob_ws_price_change_msgs == 1
    assert hub.state.last_ws_book_local_ts > 0.0

    # Adding 0.5-share dust 4c below best deep ask triggers VWAP dust filter (returns None), deleting it restores 0.80
    assert hub.apply_clob_price_change_payload(
        {"event_type": "price_change", "timestamp": ev_ms, "price_changes": [{"asset_id": "tok_up_1", "price": "0.76", "size": "0.5", "side": "SELL"}]},
        via_ws=True,
    )
    assert hub.state.book.up_ask is None
    assert hub.apply_clob_price_change_payload(
        {"event_type": "price_change", "timestamp": ev_ms, "price_changes": [{"asset_id": "tok_up_1", "price": "0.76", "size": "0", "side": "SELL"}]},
        via_ws=True,
    )
    assert hub.state.book.up_ask == pytest.approx(0.80)
    assert hub.apply_clob_price_change_payload(
        {"event_type": "price_change", "timestamp": ev_ms, "price_changes": [{"asset_id": "tok_dn_1", "price": "0.20", "size": "50", "side": "SELL"}]},
        via_ws=True,
    ) is False


@pytest.mark.parametrize("coin_dir", COIN_DIRS)
def test_p0_3_onchain_isolated_from_trading_and_stale_streams_rejected(coin_dir: str) -> None:
    """Ensure 60s-old on-chain rounds never feed signals and live streams require fresh origin timestamps."""
    mods = _load_coin_modules(coin_dir)
    ChainlinkOracleFeed = mods["chainlink_oracle"].ChainlinkOracleFeed
    Strategy = mods["strategy"].HighProbSniperStrategy
    feed = ChainlinkOracleFeed()
    ref_price = 95000.0 if "btc" in coin_dir else (3200.0 if "eth" in coin_dir else 140.0)

    feed.on_chainlink_tick(ref_price, "ARB_CHAINLINK", oracle_ts=time.time() - 60.0)
    assert feed.state.settlement_reference_price is None
    assert feed.state.onchain_display_price == pytest.approx(ref_price)

    feed.on_chainlink_tick(ref_price + 1.0, "POLY_CHAINLINK", oracle_ts=time.time())
    assert feed.state.settlement_reference_price is None

    feed.on_chainlink_tick(ref_price + 2.0, "POLY_RTDS_CHAINLINK", oracle_ts=time.time() - 60.0)
    assert feed.state.is_alive() is False
    assert feed.state.settlement_reference_price is None

    feed.on_chainlink_tick(ref_price + 3.0, "POLY_RTDS_CHAINLINK", oracle_ts=time.time() - 1.8)
    assert feed.state.is_alive() is True
    assert feed.state.settlement_reference_price == pytest.approx(ref_price + 3.0)
    assert Strategy().active_position is None


class _TimeoutThenTradesClient:
    """Mock CLOB client simulating ambiguous timeout on order post while fill landed."""

    def __init__(self, exc: Exception, trades: List[Dict[str, Any]], bal_raw: int) -> None:
        self._exc = exc
        self._trades = trades
        self._bal_raw = bal_raw
        self.trade_queries = 0

    def create_and_post_market_order(self, order_args: Any, options: Any, order_type: Any) -> Any:
        raise self._exc

    def get_trades(self, params: Any = None, only_first_page: bool = False) -> List[Dict[str, Any]]:
        self.trade_queries += 1
        return list(self._trades)

    def update_balance_allowance(self, params: Any = None) -> None:
        self.trade_queries += 0

    def get_balance_allowance(self, params: Any = None) -> Dict[str, str]:
        return {"balance": str(self._bal_raw)}


@pytest.mark.parametrize("coin_dir", COIN_DIRS)
def test_p0_4_entry_timeout_reconciliation_and_fast_fail(coin_dir: str) -> None:
    """Recover filled orders within 3s on timeout and fast-fail deterministic 400 rejections."""
    mods = _load_coin_modules(coin_dir)
    ExecCls = mods["live_executor"].PolymarketLiveExecutor

    def _build_exec(client: Any) -> Any:
        obj = ExecCls.__new__(ExecCls)
        obj.sig_type = 3
        obj.max_daily_loss_usdc = 15.0
        obj.daily_pnl_usdc = 0.0
        obj.circuit_broken = False
        obj._day_utc = 20779
        obj._tick_size_cache = {"tok_u": "0.01"}
        obj._cond_balance_cache = {}
        obj._warmed_tokens = {"tok_u"}
        obj._last_keepalive_ts = 0.0
        obj._reconcile_delay_sec = 0.01
        obj._TradeParams = None
        obj._AssetType = types.SimpleNamespace(CONDITIONAL="CONDITIONAL", COLLATERAL="COLLATERAL")
        obj._BalanceAllowanceParams = lambda **kw: kw
        obj.client = client
        return obj

    ex_timeout = _build_exec(
        _TimeoutThenTradesClient(
            TimeoutError("read timed out"),
            [{"asset_id": "tok_u", "side": "BUY", "size": "5.5", "price": "0.79", "match_time": int(time.time() * 1000)}],
            bal_raw=5_500_000,
        )
    )
    t_start = time.perf_counter()
    res = ex_timeout.execute_entry("tok_u", ask_price=0.78, shares=5.5, max_price=0.82, p_true=0.90)
    assert (time.perf_counter() - t_start) < 3.0
    assert res.filled is True
    assert res.order_id == "RECONCILED_POSITION"
    assert res.filled_shares == pytest.approx(5.5)
    assert res.avg_price == pytest.approx(0.79)

    rej_client = _TimeoutThenTradesClient(RuntimeError("status_code=400 no orders found to match"), [], bal_raw=0)
    res_rej = _build_exec(rej_client).execute_entry("tok_u", ask_price=0.78, shares=5.5, max_price=0.82, p_true=0.90)
    assert res_rej.filled is False
    assert rej_client.trade_queries == 0


@pytest.mark.parametrize("coin_dir", COIN_DIRS)
def test_p0_4_startup_reconcile_adopts_live_balance(coin_dir: str, tmp_path: Path) -> None:
    """Confirm startup reconciliation adopts existing wallet conditional shares into active management."""
    mods = _load_coin_modules(coin_dir, with_monitor=True)
    store = mods["storage"].MarketDataStorage(str(tmp_path / f"rec_{coin_dir}.db"))
    strat = mods["strategy"].HighProbSniperStrategy(mods["config"].DEFAULT_CONFIG)

    class _FakeExec:
        def sync_conditional_balance(self, token_id: str) -> float:
            return 5.25 if token_id == "tok_up" else 0.0

        def lookup_recent_buy_fill(self, token_id: str, since_ts: float) -> tuple[float, float]:
            return (5.25, 0.77)

    win_ts = 1795200000
    try:
        db_id = mods["run_monitor"].reconcile_startup_position(
            window_ts=win_ts, elapsed_sec=190.0, up_token="tok_up", down_token="tok_dn",
            live_exec=_FakeExec(), storage=store, strategy=strat, cfg=mods["config"].DEFAULT_CONFIG, now_ts=float(win_ts + 190),
        )
        assert db_id is not None and strat.active_position is not None
        assert strat.active_position.side == "UP" and strat.active_position.shares == pytest.approx(5.25)
        assert strat.active_position.entry_price == pytest.approx(0.77)
        assert win_ts in strat.traded_windows
    finally:
        store.close()


@pytest.mark.parametrize("coin_dir", COIN_DIRS)
def test_p0_5_precise_ms_clock_sync_and_deadband(coin_dir: str) -> None:
    """Validate RTT/2 millisecond Binance sync, +/-50ms deadband, and coarse Date protection."""
    mods = _load_coin_modules(coin_dir)
    feed = mods["chainlink_oracle"].ChainlinkOracleFeed()

    class _MockTimeSession:
        def get(self, url: str, timeout: float = 2.0) -> Any:
            return types.SimpleNamespace(status_code=200, json=lambda: {"serverTime": int((time.time() + 0.165) * 1000)})

    raw_ms = feed.sync_clock_precise(session=_MockTimeSession(), samples=3)
    assert raw_ms is not None and raw_ms > 100.0
    assert abs(feed.effective_clock_offset_ms) < 50.0
    saved_offset = feed.clock_offset_sec

    feed.update_clock_offset(time.time() + 4.0)
    assert feed.clock_offset_sec == pytest.approx(saved_offset)

    feed.update_clock_offset_precise(0.020)
    assert feed.clock_offset_sec == 0.0 and abs(feed.effective_clock_offset_ms) < 50.0


@pytest.mark.parametrize(("coin_dir", "sym_filter"), (("polymarket-btc-5m", "btc/usd"), ("polymarket-eth-5m", "eth/usd"), ("polymarket-sol-5m", "sol/usd")))
def test_p0_2_and_p1_wiring_across_all_coins(coin_dir: str, sym_filter: str) -> None:
    """Check P0-2 gates and P1-2/P1-3/P1-4/P1-5 wiring across BTC/ETH/SOL sources."""
    oracle_src = (FRIEND_ROOT / coin_dir / "src" / "chainlink_oracle.py").read_text(encoding="utf-8")
    feeds_src = (FRIEND_ROOT / coin_dir / "src" / "feeds.py").read_text(encoding="utf-8")
    mon_src = (FRIEND_ROOT / coin_dir / "run_monitor.py").read_text(encoding="utf-8")

    assert '"filters": ""' not in oracle_src
    assert f'"filters": \'{{"symbol":"{sym_filter}"}}\'' in oracle_src
    assert "slot=0" in oracle_src and "slot=1" in oracle_src
    assert "_rtds_zombie_timeout_sec(" in oracle_src and "else 3.5" in oracle_src
    assert "_open_clob_ws" in feeds_src and ">= 290.0" in feeds_src and "polymarket-5m-sniper/4.6" in feeds_src
    assert "book_age > 1.5 or book_lag > 1.0" in mon_src and "RotatingFileHandler" in mon_src and "if is_tty:" in mon_src
    assert "else (elapsed if st.up_token else 0.0)" in mon_src


def test_daily_review_parses_v45_health_and_stale_violations(tmp_path: Path) -> None:
    """Confirm daily_review extracts v4.5 health percentiles, clock residual, and flags stale ENTERs."""
    (tmp_path / "logs").mkdir(parents=True, exist_ok=True)
    for coin in ("btc", "eth", "sol"):
        db_p = tmp_path / f"polymarket-{coin}-5m" / "data" / f"live_{coin}_5m.db"
        db_p.parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(db_p)
        con.executescript(
            "CREATE TABLE windows (window_ts INTEGER PRIMARY KEY, slug TEXT NOT NULL);"
            "CREATE TABLE paper_trades (id INTEGER PRIMARY KEY, window_ts INTEGER, side TEXT, entry_ts REAL, "
            "entry_elapsed_sec REAL, shares REAL, entry_price REAL, entry_fee_per_share REAL, p_true_at_entry REAL, "
            "z_at_entry REAL, exit_ts REAL, exit_reason TEXT, exit_payout_per_share REAL, net_pnl_usdc REAL, won INTEGER);"
        )
        con.close()

    (tmp_path / "logs" / "btc_live.log").write_text(
        "[CLOCK] raw_offset_ms=+163.0ms eff_offset_ms=+0.0ms\n"
        "[HEALTH] window=1795200000 | cl_src=POLYBOLT_CHAINLINK | book_age_p50_ms=42.0 book_age_p99_ms=240.0 | "
        "book_lag_p50_ms=35.0 book_lag_p99_ms=190.0 | rollover_p99_ms=210.0 | ws_book=12 ws_pc=450 rest_fallbacks=0 | "
        "reconn(clob=0,rtds=0,pb=0,bn=0,cb=0) | stale_skips=0 | clock_raw_ms=+163.0 clock_eff_ms=+0.0\n"
        "*** [ENTRY UP] elapsed=190s | book_age=180ms book_lag=90ms ***\n",
        encoding="utf-8",
    )
    (tmp_path / "logs" / "eth_live.log").write_text("*** [ENTRY UP] elapsed=195s | book_age=1800ms book_lag=120ms ***\n", encoding="utf-8")
    (tmp_path / "logs" / "sol_live.log").write_text("", encoding="utf-8")

    rep = daily_review.run_daily_review(root=tmp_path, hours=24.0, no_live_api=True)
    bh = rep["per_coin_logs"]["btc"]["health"]
    assert bh["book_age_p99_ms"] == pytest.approx(240.0) and bh["clock_eff_ms"] == pytest.approx(0.0) and bh["stale_enter_cnt"] == 0
    assert rep["per_coin_logs"]["eth"]["health"]["stale_enter_cnt"] == 1
    assert any("[STALE]" in w for w in rep["watchlist"])
    assert "## 3. 資料源品質、盤口新鮮度與時鐘健康表" in daily_review.render_markdown_report(rep)
