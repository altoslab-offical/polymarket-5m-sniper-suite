"""Unit tests for v4.4 execution speed, reliability, and concurrency upgrades."""
from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from typing import Any, Dict, List

import pytest

FRIEND_ROOT = Path(__file__).resolve().parent
COIN_DIRS = ("polymarket-btc-5m", "polymarket-eth-5m", "polymarket-sol-5m")

if str(FRIEND_ROOT) not in sys.path:
    sys.path.insert(0, str(FRIEND_ROOT))

from shared_risk import SharedDailyRiskGuard  # noqa: E402


def _load_v44_modules(coin_dir_name: str) -> Dict[str, types.ModuleType]:
    """Load `config`, `fees`, `pricing_model`, `strategy`, `storage`, and `live_executor` cleanly."""
    pkg_dir = FRIEND_ROOT / coin_dir_name / "src"
    pkg_name = f"_v44_{coin_dir_name.replace('-', '_')}"
    pkg = types.ModuleType(pkg_name)
    pkg.__path__ = [str(pkg_dir)]
    sys.modules[pkg_name] = pkg

    mods: Dict[str, types.ModuleType] = {}
    for mod_name in ("config", "fees", "pricing_model", "strategy", "storage", "live_executor"):
        spec = importlib.util.spec_from_file_location(
            f"{pkg_name}.{mod_name}", pkg_dir / f"{mod_name}.py"
        )
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        mod.__package__ = pkg_name
        sys.modules[f"{pkg_name}.{mod_name}"] = mod
        spec.loader.exec_module(mod)
        mods[mod_name] = mod
    return mods


def test_shared_risk_fast_balance_spend_and_utc_rollover(tmp_path: Path) -> None:
    """Validate Zero-RTT balance caching, cross-coin spend deduction, TTL/floor, and UTC rollover."""
    guard = SharedDailyRiskGuard(tmp_path / "shared_live_risk.json", max_daily_loss_pct=0.30)
    t0 = 1795200000.0
    guard.sync_day_equity(41.63, now_ts=t0)
    guard.record_balance(41.63, now_ts=t0)

    assert guard.get_fast_balance(max_age_sec=25.0, min_safe_usdc=10.0, now_ts=t0 + 10.0) == pytest.approx(41.63)

    guard.record_spend(4.50, now_ts=t0 + 11.0)
    assert guard.get_fast_balance(max_age_sec=25.0, min_safe_usdc=10.0, now_ts=t0 + 12.0) == pytest.approx(37.13)

    # Stale cache (>25s) must return None to trigger a live RPC refresh
    assert guard.get_fast_balance(max_age_sec=25.0, min_safe_usdc=10.0, now_ts=t0 + 26.0) is None

    # Cache preserved across UTC day rollover cleanly
    rollover_ts = t0 + 86400.0
    guard.record_balance(37.13, now_ts=rollover_ts - 2.0)
    guard.record_spend(2.00, now_ts=rollover_ts - 1.0)
    guard.sync_day_equity(35.13, now_ts=rollover_ts)
    assert guard.get_fast_balance(max_age_sec=25.0, min_safe_usdc=10.0, now_ts=rollover_ts) == pytest.approx(35.13)

    # Dropping below min_safe_usdc ($10) forces live RPC check
    guard.record_spend(28.00, now_ts=rollover_ts + 0.5)
    assert guard.get_fast_balance(max_age_sec=25.0, min_safe_usdc=10.0, now_ts=rollover_ts + 1.0) is None


@pytest.mark.parametrize("coin_dir", COIN_DIRS)
def test_storage_wal_sync_normal_and_busy_timeout(coin_dir: str, tmp_path: Path) -> None:
    """Confirm SQLite storage sets WAL, synchronous=NORMAL (1), and busy_timeout >= 5000ms."""
    mods = _load_v44_modules(coin_dir)
    MarketDataStorage = mods["storage"].MarketDataStorage

    store = MarketDataStorage(str(tmp_path / f"test_{coin_dir}.db"))
    try:
        j_mode = store.conn.execute("PRAGMA journal_mode;").fetchone()[0]
        sync_mode = int(store.conn.execute("PRAGMA synchronous;").fetchone()[0])
        busy_ms = int(store.conn.execute("PRAGMA busy_timeout;").fetchone()[0])
        assert str(j_mode).lower() == "wal"
        assert sync_mode == 1
        assert busy_ms >= 5000
    finally:
        store.close()


class _FakeClobClient:
    """Minimal synchronous mock for py-clob-client-v2 calls inside exit/entry tests."""

    def __init__(self, steps: List[Any]) -> None:
        self._steps = list(steps)
        self.prices_posted: List[float] = []

    def create_and_post_market_order(self, order_args: Any, options: Any, order_type: Any) -> Any:
        """Record posted limit price and pop next scripted outcome or exception."""
        self.prices_posted.append(float(order_args.price))
        nxt = self._steps.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt


@pytest.mark.parametrize("coin_dir", COIN_DIRS)
def test_live_executor_exit_latency_and_stop_loss_retry(coin_dir: str) -> None:
    """Verify stop-loss widens once (+5c) on FAK mismatch, TP does not widen, and latency_ms is set."""
    mods = _load_v44_modules(coin_dir)
    PolymarketLiveExecutor = mods["live_executor"].PolymarketLiveExecutor

    def _make_exec(steps: List[Any]) -> Any:
        obj = PolymarketLiveExecutor.__new__(PolymarketLiveExecutor)
        obj.sig_type = 3
        obj.max_daily_loss_usdc = 12.5
        obj.daily_pnl_usdc = 0.0
        obj.circuit_broken = False
        obj._day_utc = 20779
        obj._tick_size_cache = {"tok_1": "0.01"}
        obj._cond_balance_cache = {"tok_1": 10.0}
        obj._warmed_tokens = {"tok_1"}
        obj._last_keepalive_ts = 0.0
        obj.client = _FakeClobClient(steps)
        return obj

    # Stop-loss (slippage=0.05): widens from 0.25 -> 0.20 on FAK mismatch
    ex_sl = _make_exec(
        [
            RuntimeError("PolyApiException[status_code=400, no orders found to match with FAK order]"),
            {"orderID": "ord_sl_2", "makingAmount": "5.5", "takingAmount": "1.1"},
        ]
    )
    res_sl = ex_sl.execute_exit("tok_1", bid_price=0.30, shares=5.5, slippage_cents=0.05)
    assert res_sl.filled is True
    assert res_sl.latency_ms >= 0.0
    assert ex_sl.client.prices_posted == [pytest.approx(0.25), pytest.approx(0.20)]

    # Early take-profit (slippage=0.03): does NOT widen price on FAK mismatch
    ex_tp = _make_exec(
        [
            RuntimeError("PolyApiException[status_code=400, no orders found to match with FAK order]"),
            {"orderID": "unused", "makingAmount": "5.5", "takingAmount": "5.0"},
        ]
    )
    res_tp = ex_tp.execute_exit("tok_1", bid_price=0.96, shares=5.5, slippage_cents=0.03)
    assert res_tp.filled is False
    assert res_tp.latency_ms >= 0.0
    assert ex_tp.client.prices_posted == [pytest.approx(0.93)]

    # Entry latency_ms populated on success
    ex_ent = _make_exec([{"orderID": "ord_ent_1", "makingAmount": "4.0", "takingAmount": "5.0"}])
    res_ent = ex_ent.execute_entry("tok_1", ask_price=0.79, shares=5.0, max_price=0.82, p_true=0.90)
    assert res_ent.filled is True
    assert res_ent.latency_ms >= 0.0


@pytest.mark.parametrize(
    ("coin_dir", "tick_var"),
    (
        ("polymarket-btc-5m", "last_btc_tick_ts"),
        ("polymarket-eth-5m", "last_eth_tick_ts"),
        ("polymarket-sol-5m", "last_sol_tick_ts"),
    ),
)
def test_chainlink_rtds_and_run_monitor_v44_wiring(coin_dir: str, tick_var: str) -> None:
    """Confirm v4.4 wiring across oracle RTDS watchdog and run_monitor while strategy thresholds stay intact."""
    oracle_src = (FRIEND_ROOT / coin_dir / "src" / "chainlink_oracle.py").read_text(encoding="utf-8")
    idx_var = oracle_src.find(f"{tick_var} = time.time()")
    idx_skip = oracle_src.find('self.state.source in ("CHAINLINK_DATA_STREAMS", "POLYBOLT_CHAINLINK")')
    assert idx_var != -1 and idx_skip != -1 and idx_var < idx_skip

    mon_src = (FRIEND_ROOT / coin_dir / "run_monitor.py").read_text(encoding="utf-8")
    assert "get_fast_balance(" in mon_src
    assert "record_spend(" in mon_src
    assert "record_balance(" in mon_src
    assert "loop_sleep = 0.25 if hot_zone else 1.0" in mon_src
    assert "if is_1hz_tick:" in mon_src
    assert "[MONITOR LOOP WARN]" in mon_src
    assert "for wait_s in (2.0, 3.0, 4.0, 5.0):" in mon_src
    assert "live_exec.keepalive_if_needed, 10.0" in mon_src

    feeds_src = (FRIEND_ROOT / coin_dir / "src" / "feeds.py").read_text(encoding="utf-8")
    assert "OFFICIAL_TWAP60" in feeds_src and "get_twap_at_or_after_utc(" in feeds_src
