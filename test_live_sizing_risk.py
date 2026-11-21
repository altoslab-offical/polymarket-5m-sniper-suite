"""Unit tests for shared proportional sizing, safe 5-share billing, and cross-coin 30% daily risk guard."""
from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import types

import pytest

FRIEND_ROOT = Path(__file__).resolve().parent
if str(FRIEND_ROOT) not in sys.path:
    sys.path.insert(0, str(FRIEND_ROOT))

from shared_risk import (  # noqa: E402
    SharedDailyRiskGuard,
    compute_proportional_shares,
    compute_safe_buy_usdc,
)


def _load_coin_strategy_classes(coin_dir_name: str):
    """Load `StrategyConfig`, `HighProbSniperStrategy`, `OrderBookTop`, and `FairValueEstimate` cleanly per folder."""
    pkg_dir = FRIEND_ROOT / coin_dir_name / "src"
    pkg_name = f"_coin_{coin_dir_name.replace('-', '_')}"
    pkg = types.ModuleType(pkg_name)
    pkg.__path__ = [str(pkg_dir)]
    sys.modules[pkg_name] = pkg

    mods = {}
    for mod_name in ("config", "fees", "pricing_model", "strategy"):
        spec = importlib.util.spec_from_file_location(f"{pkg_name}.{mod_name}", pkg_dir / f"{mod_name}.py")
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        mod.__package__ = pkg_name
        sys.modules[f"{pkg_name}.{mod_name}"] = mod
        spec.loader.exec_module(mod)
        mods[mod_name] = mod

    return (
        mods["config"].StrategyConfig,
        mods["strategy"].HighProbSniperStrategy,
        mods["strategy"].OrderBookTop,
        mods["pricing_model"].FairValueEstimate,
    )


def test_proportional_shares_scales_from_42_to_10000_with_floor() -> None:
    cost = 0.78  # $0.78/sh total cost

    # $42.53 equity -> 10% = $4.253 -> 5.45 shares (>5.0 floor)
    sh_42 = compute_proportional_shares(42.53, cost, 0.10)
    assert sh_42 == pytest.approx(5.45, abs=0.01)

    # $35.00 equity -> 10% = $3.50 < 5 * 0.78 ($3.90) -> floored up to $3.90 = 5.0 shares
    sh_35 = compute_proportional_shares(35.00, cost, 0.10)
    assert sh_35 == pytest.approx(5.00, abs=0.01)

    # $100 -> $10 outlay -> 12.82 shares
    sh_100 = compute_proportional_shares(100.0, cost, 0.10)
    assert sh_100 == pytest.approx(12.82, abs=0.01)

    # $1,000 -> $100 outlay -> 128.21 shares
    sh_1000 = compute_proportional_shares(1000.0, cost, 0.10)
    assert sh_1000 == pytest.approx(128.21, abs=0.02)

    # $10,000 -> $1,000 outlay -> 1282.05 shares
    sh_10000 = compute_proportional_shares(10000.0, cost, 0.10)
    assert sh_10000 == pytest.approx(1282.05, abs=0.02)

    # $3.00 -> cannot afford 5 shares ($3.00 * 0.96 = $2.88 < $3.90) -> 0.0 skip
    assert compute_proportional_shares(3.00, cost, 0.10) == 0.0


def test_safe_buy_usdc_protects_five_share_floor_under_limit_slippage() -> None:
    # 5.10 sh * $0.78 ask = $3.978, which at $0.82 limit would be 4.85 sh (<5 -> rejected!)
    # Safe billing lifts USDC outlay to 5.05 * $0.82 = $4.14 -> $4.14 / $0.82 = 5.049 >= 5.0 shares.
    usdc = compute_safe_buy_usdc(shares=5.10, ask_price=0.78, limit_price=0.82, available_pusd=42.53)
    assert usdc / 0.82 >= 5.0
    assert usdc == pytest.approx(4.14, abs=0.01)

    # Respects 98% cash cap and aborts cleanly if remaining cash cannot cover 5 * limit
    assert compute_safe_buy_usdc(shares=20.0, ask_price=0.78, limit_price=0.82, available_pusd=10.0) == pytest.approx(9.80, abs=0.01)
    assert compute_safe_buy_usdc(shares=5.10, ask_price=0.78, limit_price=0.82, available_pusd=4.00) == 0.0


def test_shared_daily_risk_guard_accumulates_and_resets_on_utc_day_rollover(tmp_path: Path) -> None:
    guard = SharedDailyRiskGuard(tmp_path / "shared_live_risk.json", max_daily_loss_pct=0.30, min_loss_cap_usd=5.0)
    day0_ts = 1795219200.0 + 100.0  # 2026-11-21 UTC

    s0 = guard.sync_day_equity(42.53, now_ts=day0_ts)
    assert s0["max_daily_loss_usdc"] == pytest.approx(12.76, abs=0.01)
    assert s0["circuit_broken"] is False

    # Re-syncing within same UTC day preserves existing cap
    s0_again = guard.sync_day_equity(38.00, now_ts=day0_ts + 60.0)
    assert s0_again["max_daily_loss_usdc"] == pytest.approx(12.76, abs=0.01)

    # ETH loses -$4.50, SOL wins +$1.20 -> cumulative -$3.30 (not broken)
    guard.record_pnl(-4.50, current_equity=38.03, now_ts=day0_ts + 300.0)
    guard.record_pnl(1.20, current_equity=39.23, now_ts=day0_ts + 600.0)
    broken, pnl, cap = guard.is_broken(now_ts=day0_ts + 605.0)
    assert broken is False
    assert pnl == pytest.approx(-3.30, abs=1e-4)
    assert cap == pytest.approx(12.76, abs=0.01)

    # BTC + ETH drop another -$9.46 -> cumulative -$12.76 hits -30% cap -> circuit broken!
    s_hit = guard.record_pnl(-9.46, current_equity=29.77, now_ts=day0_ts + 900.0)
    assert s_hit["circuit_broken"] is True
    assert guard.is_broken(now_ts=day0_ts + 905.0)[0] is True

    # Next UTC day rollover resets PnL & rescales 30% cap to new equity ($200 -> -$60)
    day1_ts = day0_ts + 86400.0
    assert guard.is_broken(now_ts=day1_ts)[0] is False
    s1 = guard.sync_day_equity(200.0, now_ts=day1_ts + 5.0)
    assert s1["daily_realized_pnl_usdc"] == 0.0
    assert s1["max_daily_loss_usdc"] == pytest.approx(60.0, abs=0.01)
    assert s1["circuit_broken"] is False


@pytest.mark.parametrize("coin_dir", ["polymarket-btc-5m", "polymarket-eth-5m", "polymarket-sol-5m"])
def test_strategy_proportional_sizing_works_across_all_three_coins(coin_dir: str) -> None:
    StrategyConfig, HighProbSniperStrategy, OrderBookTop, FairValueEstimate = _load_coin_strategy_classes(coin_dir)
    cfg = StrategyConfig(initial_bankroll_usdc=42.53, fixed_bet_usdc=0.0, proportional_bet_pct=0.10, min_order_shares=5.0)
    strat = HighProbSniperStrategy(cfg)

    fv = FairValueEstimate(
        strike_price=100.0,
        chainlink_price=100.2,
        cex_lead_price=100.2,
        oracle_gap_bps=0.0,
        remaining_sec=100.0,
        vol_1s=0.0001,
        horizon_vol_pct=0.10,
        raw_z_score=1.80,
        adjusted_z_score=1.80,
        p_true_up=0.90,
        p_true_down=0.10,
        orderflow_score=0.5,
        cvd_imbalance=0.3,
    )
    book = OrderBookTop(up_bid=0.77, up_ask=0.78, down_bid=0.21, down_ask=0.22)
    action = strat.evaluate_tick(ts=1795219400.0, window_ts=1795219200, elapsed_sec=200.0, fv=fv, book=book)
    assert action is not None and action.startswith("[ENTRY UP]")
    assert strat.active_position is not None
    # Cost per share at 0.78 + 0.07*0.78*0.22 = 0.79201 -> $4.253 / 0.79201 = 5.37 sh
    assert strat.active_position.shares == pytest.approx(5.37, abs=0.02)
