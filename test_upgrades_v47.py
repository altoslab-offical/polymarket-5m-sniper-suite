"""Unit tests for v4.7 OFFICIAL_TWAP60 strike, TWAP projection, anti-OBI spoof, and BTC paper TG controller."""
from __future__ import annotations

import importlib.util
from pathlib import Path
import stat
import sys
import types
from typing import Any, Dict, List

import pytest

FRIEND_ROOT = Path(__file__).resolve().parent
ALL_COINS: tuple[tuple[str, float], ...] = (
    ("polymarket-btc-5m", 65000.0),
    ("polymarket-eth-5m", 1680.0),
    ("polymarket-sol-5m", 140.0),
    ("polymarket-xrp-5m", 0.92),
    ("polymarket-doge-5m", 0.054),
)


def _load_coin(coin_dir: str, with_monitor: bool = False) -> Dict[str, types.ModuleType]:
    """Load coin `src` modules (and optional `run_monitor`) under isolated package names."""
    pkg_dir = FRIEND_ROOT / coin_dir / "src"
    pkg_name = f"_v47_{coin_dir.replace('-', '_')}"
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
        sys.modules["src"] = pkg
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


@pytest.mark.parametrize(("coin_dir", "ref_px"), ALL_COINS)
def test_official_twap_rejects_pre_boundary_and_blocks_local_overwrite(coin_dir: str, ref_px: float) -> None:
    """Reject `T - 1s` TWAP ticks, pick earliest tick >= `window_ts`, and isolate `_compute_local_twap`."""
    co = _load_coin(coin_dir)["chainlink_oracle"]
    feed = co.ChainlinkOracleFeed()
    wts = 1795336200.0

    feed.on_official_twap_tick(ref_px * 0.990, wts - 1.0, "RTDS_TWAP60")
    assert feed.get_twap_at_or_after_utc(wts, max_after_sec=5.0) is None

    feed.on_chainlink_tick(ref_px * 1.050, "POLYBOLT_CHAINLINK", oracle_ts=wts)
    assert feed.get_twap_at_or_after_utc(wts, max_after_sec=5.0) is None
    assert feed.state.twap_60s == pytest.approx(ref_px * 0.990)

    feed.on_official_twap_tick(ref_px * 1.002, wts + 1.0, "POLYBOLT_TWAP60")
    feed.on_official_twap_tick(ref_px * 1.001, wts, "RTDS_TWAP60")
    assert feed.get_twap_at_or_after_utc(wts, max_after_sec=5.0) == pytest.approx(ref_px * 1.001)

    feed.on_chainlink_tick(ref_px * 0.950, "POLYBOLT_CHAINLINK", oracle_ts=wts + 2.0)
    assert feed.state.twap_60s == pytest.approx(ref_px * 1.002)
    assert co._parse_ts_sec(int(wts * 1000)) == pytest.approx(wts)
    assert co._parse_ts_sec("bad") is None


@pytest.mark.parametrize(("coin_dir", "ref_px"), ALL_COINS)
def test_twap60_strike_lock_backfill_and_calibration_guard(coin_dir: str, ref_px: float) -> None:
    """Lock `OFFICIAL_TWAP60` on rollover or <=20s backfill and never let `BOOK_CALIBRATED` overwrite it."""
    del ref_px
    mods = _load_coin(coin_dir)
    hub = mods["feeds"].LiveMarketHub(mods["pricing_model"].RollingMicrostructureTracker())
    wts = 1795336500.0

    hub.chainlink.on_chainlink_tick(100.0, "POLY_RTDS_CHAINLINK", oracle_ts=wts)
    hub.sync_window_boundary(wts + 1.0)
    assert hub.state.strike_source == "POLY_RTDS_CHAINLINK"

    hub.chainlink.on_official_twap_tick(101.25, wts, "RTDS_TWAP60")
    hub.sync_window_boundary(wts + 2.0)
    assert hub.state.strike_price == pytest.approx(101.25)
    assert hub.state.strike_source == "OFFICIAL_TWAP60"

    hub.state.book = mods["strategy"].OrderBookTop(up_bid=0.25, up_ask=0.27, down_bid=0.73, down_ask=0.75)
    for sec in (3.0, 4.0, 5.0):
        assert hub.state.calibrate_strike_from_opening_book(101.25, 0.0001, sec) is None
    assert hub.state.strike_price == pytest.approx(101.25)
    assert hub.state.strike_source == "OFFICIAL_TWAP60"


@pytest.mark.parametrize(("coin_dir", "ref_px"), ALL_COINS)
def test_twap_projection_math_and_anti_obi_spoof(coin_dir: str, ref_px: float) -> None:
    """Validate `rem>60` and `rem<=60` TWAP math, 1s dedup, CVD-only orderflow, and v4.7 config defaults."""
    mods = _load_coin(coin_dir)
    cfg = mods["config"].DEFAULT_CONFIG
    assert cfg.min_z_score == pytest.approx(1.30)
    assert cfg.vol_lookback_sec == 1800
    assert cfg.min_1s_vol_floor == pytest.approx(0.000035)
    assert cfg.e4_dual_consensus is True
    assert cfg.max_model_market_divergence == pytest.approx(0.25)
    assert cfg.use_twap_projection is True
    assert cfg.twap_gaussian_weight == pytest.approx(0.70)
    assert cfg.twap_student_t_df == pytest.approx(6.0)

    pm = mods["pricing_model"]
    fv_far = pm.estimate_twap_fair_value(
        strike_price=ref_px,
        chainlink_price=ref_px * 1.001,
        remaining_sec=100.0,
        vol_1s=0.0001,
        orderflow_score=0.2,
    )
    assert fv_far is not None
    expected_hvol = 0.0001 * (60.0**0.5)
    expected_z = 0.001 / expected_hvol
    assert fv_far.raw_z_score == pytest.approx(expected_z, rel=1e-6)

    st = mods["feeds"].LiveFeedState(current_window_ts=1795336500)
    st.record_twap240_spot(1795336500.0 + 240.1, ref_px * 1.002)
    st.record_twap240_spot(1795336500.0 + 240.4, ref_px * 1.002)
    st.record_twap240_spot(1795336500.0 + 241.2, ref_px * 1.004)
    mean240, cnt240 = st.realized_twap240
    assert cnt240 == pytest.approx(2.0)
    assert mean240 == pytest.approx(ref_px * 1.003)

    fv_near = pm.estimate_twap_fair_value(
        strike_price=ref_px,
        chainlink_price=ref_px * 1.001,
        remaining_sec=40.0,
        vol_1s=0.0001,
        realized_240=(ref_px * 1.003, 20.0),
    )
    assert fv_near is not None and fv_near.raw_z_score > 0.0

    tracker = pm.RollingMicrostructureTracker()
    tracker.on_trade(1000.0, ref_px, 5000.0 / ref_px, is_buyer_maker=False)
    tracker.on_book_ticker(bid_qty=1.0, ask_qty=10000.0)
    cvd, obi, combined = tracker.get_orderflow_metrics(1001.0)
    assert cvd == pytest.approx(1.0)
    assert obi < -0.95
    assert combined == pytest.approx(cvd)


@pytest.mark.parametrize(("coin_dir", "ref_px"), ALL_COINS)
def test_dual_twap60_feeds_wired_in_oracle(coin_dir: str, ref_px: float) -> None:
    """Verify RTDS and PolyBolt TWAP60 snapshot/tick parsers and wiring across all 5 coins."""
    mods = _load_coin(coin_dir)
    oracle_src = (FRIEND_ROOT / coin_dir / "src" / "chainlink_oracle.py").read_text(encoding="utf-8")
    assert "crypto_prices_twap_sixty" in oracle_src
    assert 'if isinstance(data.get("data"), list):' in oracle_src
    assert 'if isinstance(payload.get("data"), list):' in oracle_src

    feed = mods["chainlink_oracle"].ChainlinkOracleFeed()
    wts = 1795336800.0
    pb_snap = {
        "channel": "price.crypto.twap",
        "snapshot": True,
        "payload": {
            "data": [
                {"timestamp": int((wts - 1) * 1000), "value": ref_px * 0.99},
                {"timestamp": int(wts * 1000), "value": ref_px * 1.001},
            ]
        },
    }
    feed._handle_polybolt_message(pb_snap)
    assert feed.get_twap_at_or_after_utc(wts, max_after_sec=5.0) == pytest.approx(ref_px * 1.001)

    coin_slug = coin_dir.split("-")[1]
    assert feed._handle_rtds_twap_payload({"symbol": "other/usd", "value": ref_px, "timestamp": int(wts * 1000)}) is False
    rtds_snap = {
        "symbol": f"{coin_slug}/usd",
        "data": [{"timestamp": int((wts + 300) * 1000), "value": ref_px * 1.004}],
    }
    assert feed._handle_rtds_twap_payload(rtds_snap) is True
    assert feed.get_twap_at_or_after_utc(wts + 300, max_after_sec=5.0) == pytest.approx(ref_px * 1.004)


class _StubNotifier:
    """Minimal Telegram stub capturing entries, exits, and alerts."""

    def __init__(self) -> None:
        self.enabled = True
        self.entries: List[int] = []
        self.exits: List[tuple[int, str]] = []
        self.alerts: List[str] = []

    def get_recipient_ids(self) -> List[str]:
        return ["111111111"]

    def notify_entry(self, **kwargs: Any) -> None:
        self.entries.append(int(kwargs["trade_id"]))

    def notify_exit(self, **kwargs: Any) -> None:
        self.exits.append((int(kwargs["trade_id"]), str(kwargs["exit_reason"])))

    def notify_alert(self, title: str, detail: str) -> None:
        del detail
        self.alerts.append(title)


class _StubLiveExec:
    """Minimal live executor stub triggering circuit-breaker accounting."""

    def __init__(self) -> None:
        self.daily_pnl_usdc = 0.0
        self.max_daily_loss_usdc = 10.0
        self.circuit_broken = True

    def record_realized_pnl(self, delta: float) -> None:
        self.daily_pnl_usdc += delta


def test_tg_commands_only_silences_paper_trades_and_keeps_pusd_balance(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Mute entry/exit TG pushes under `--tg-commands-only` while preserving `/balance` pUSD reporting."""
    mods = _load_coin("polymarket-btc-5m", with_monitor=True)
    rm = mods["run_monitor"]
    store = mods["storage"].MarketDataStorage(str(tmp_path / "paper_btc.db"))
    strat = mods["strategy"].HighProbSniperStrategy(mods["config"].DEFAULT_CONFIG)
    stub_tg = _StubNotifier()
    try:
        tid = store.insert_paper_trade(1795336500, "UP", 1795336680.0, 180.0, 5.5, 0.78, 0.0117, 0.90, 1.50)
        closed = mods["strategy"].ClosedTrade(
            window_ts=1795336500,
            side="UP",
            shares=5.5,
            entry_price=0.78,
            total_cost_per_share=0.7917,
            exit_payout_per_share=1.0,
            exit_reason="SETTLED_WIN",
            net_pnl_usdc=1.145,
            won=True,
            p_true_at_entry=0.90,
            z_at_entry=1.50,
        )
        rm._commit_live_exit_accounting(
            last=closed,
            final_leg_pnl=1.145,
            now_ts=1795336800.0,
            active_db_trade_id=tid,
            live_exec=_StubLiveExec(),
            shared_risk=None,
            strategy=strat,
            storage=store,
            tg=stub_tg,
            live_mode=False,
            emit_fn=lambda _msg: None,
            tg_commands_only=True,
        )
        assert stub_tg.exits == []
        assert stub_tg.alerts == ["實盤風控熔斷觸發"]
    finally:
        store.close()

    tgn = mods["tg_notifier"]
    monkeypatch.setenv("POLY_FUNDER_ADDRESS", "0x1234567890abcdef1234567890abcdef12345678")
    monkeypatch.setattr(tgn, "_fetch_onchain_pusd_balance", lambda _addr: 39.79)
    svc = tgn.TelegramBotService(db_path=str(tmp_path / "paper_btc.db"), live_mode=False, live_exec=None)
    bal_msg = svc._get_wallet_balance_message()
    assert "$39.79 pUSD" in bal_msg
    assert "USDC" not in bal_msg


def test_runner_script_runs_btc_eth_sol_live() -> None:
    """Confirm local launchd coin runner runs BTC, ETH, and SOL in LIVE mode."""
    runner = Path.home() / ".local" / "bin" / "poly5m-run-coin.sh"
    if not runner.exists():
        pytest.skip("local launchd runner script not installed on this host")
    assert bool(runner.stat().st_mode & stat.S_IXUSR)
    text = runner.read_text(encoding="utf-8")
    assert "--db data/paper_btc_5m.db" not in text
    assert "--tg-commands-only" not in text
    assert "--live" in text
    assert "--bet 0" in text
    assert "--bet-pct 0.10" in text
    assert "--max-daily-loss-pct 0.30" in text
    assert '--log-file "${BASE}/logs/${COIN}_live.log"' in text


@pytest.mark.parametrize("coin_dir,ref_px", ALL_COINS)
def test_storage_upsert_window_strike_precedence(coin_dir: str, ref_px: float, tmp_path: Path) -> None:
    """Enforce GAMMA_OFFICIAL > OFFICIAL_TWAP60 > BOOK_CALIBRATED > spot strike authority in SQLite."""
    mods = _load_coin(coin_dir)
    store = mods["storage"].MarketDataStorage(str(tmp_path / "windows.db"))
    wts = 1_795_337_400
    slug = f"{coin_dir.split('-')[1]}-updown-5m-{wts}"
    def _row() -> Any:
        return store.conn.execute(
            "SELECT strike_price, strike_source FROM windows WHERE window_ts = ?",
            (wts,),
        ).fetchone()

    try:
        store.upsert_window(
            window_ts=wts,
            slug=slug,
            up_token_id="up-1",
            down_token_id="dn-1",
            strike_price=None,
            strike_source="UNSET",
        )
        store.upsert_window(wts, slug, strike_price=ref_px, strike_source="POLYBOLT_CHAINLINK")
        assert _row() == (ref_px, "POLYBOLT_CHAINLINK")

        twap_px = ref_px * 1.001
        store.upsert_window(wts, slug, strike_price=twap_px, strike_source="OFFICIAL_TWAP60")
        assert _row() == (twap_px, "OFFICIAL_TWAP60")

        store.upsert_window(wts, slug, strike_price=ref_px * 0.99, strike_source="BOOK_CALIBRATED")
        store.upsert_window(wts, slug, strike_price=ref_px * 0.98, strike_source="POLY_RTDS_CHAINLINK")
        assert _row() == (twap_px, "OFFICIAL_TWAP60")

        gamma_px = ref_px * 1.002
        store.upsert_window(wts, slug, strike_price=gamma_px, strike_source="GAMMA_OFFICIAL")
        assert _row() == (gamma_px, "GAMMA_OFFICIAL")

        store.upsert_window(wts, slug, strike_price=ref_px * 1.005, strike_source="OFFICIAL_TWAP60")
        store.upsert_window(wts, slug, strike_price=ref_px * 0.97, strike_source="BOOK_CALIBRATED")
        assert _row() == (gamma_px, "GAMMA_OFFICIAL")
    finally:
        store.close()


def test_single_coin_window_slot_cap(tmp_path: Path) -> None:
    """Enforce single-coin-per-5m-window concurrency (`max_coins=1`) and zero-spam guard in all 5 monitors."""
    if str(FRIEND_ROOT) not in sys.path:
        sys.path.insert(0, str(FRIEND_ROOT))
    from shared_risk import SharedDailyRiskGuard

    guard = SharedDailyRiskGuard(tmp_path / "shared_live_risk.json", max_daily_loss_pct=0.30)
    wts = 1_795_344_000
    assert guard.try_acquire_window_slot(wts, "ETH") is True
    assert guard.try_acquire_window_slot(wts, "ETH") is True
    assert guard.try_acquire_window_slot(wts, "BTC") is False
    guard.release_window_slot(wts, "ETH")
    assert guard.try_acquire_window_slot(wts, "BTC") is True
    assert guard.try_acquire_window_slot(wts + 300, "ETH") is True

    for coin_dir, _ in ALL_COINS:
        src = (FRIEND_ROOT / coin_dir / "run_monitor.py").read_text(encoding="utf-8")
        assert "max_coins=1" in src
        assert 'error="WINDOW_SLOT_LIMIT_1_COIN"' in src
        assert "keep_window_blocked" in src
        assert "5m v5.1" in src


@pytest.mark.parametrize(("coin_dir", "ref_px"), ALL_COINS)
def test_v50_e4_dual_consensus_and_binance_1800s_vol(coin_dir: str, ref_px: float) -> None:
    """Enforce Phase 4.2 E4 dual-consensus, causal 60s basis pruning, and Binance 1s vol wiring."""
    feeds_src = (FRIEND_ROOT / coin_dir / "src" / "feeds.py").read_text(encoding="utf-8")
    assert "BINANCE_REST_KLINES_URL" in feeds_src
    assert "_warmup_binance_1s_vol_once" in feeds_src
    assert "on_price_callback" not in feeds_src
    assert "self.tracker.on_price(now_utc, p)" in feeds_src
    assert all("cex_lead_price" not in ln for ln in feeds_src.splitlines() if "on_price(" in ln)

    mods = _load_coin(coin_dir)
    pm = mods["pricing_model"]
    tracker = pm.RollingMicrostructureTracker(vol_lookback_sec=1800)
    hub = mods["feeds"].LiveMarketHub(tracker=tracker)
    t0 = 1_795_400_000.0
    hub._http = type(
        "_StubHttp",
        (),
        {
            "get": staticmethod(
                lambda url, timeout=3.0: type(
                    "_Resp",
                    (),
                    {
                        "status_code": 200,
                        "json": staticmethod(
                            lambda: []
                            if "endTime=" in url
                            else [[int((t0 - 10 + i) * 1000), "0", "0", "0", str(ref_px)] for i in range(10)]
                        ),
                    },
                )()
            )
        },
    )()
    assert hub._warmup_binance_1s_vol_once() == 10
    assert len(tracker._sec_prices) == 10
    tracker._sec_prices.clear()
    for i in range(9):
        tracker.record_basis_sample(t0 + i, ref_px + 2.0, ref_px)
    assert tracker.mean_basis_60s(t0 + 8) is None
    tracker.record_basis_sample(t0 + 9, ref_px + 4.0, ref_px)
    assert tracker.mean_basis_60s(t0 + 9) == pytest.approx(2.2)
    tracker.record_basis_sample(t0 + 69, ref_px + 10.0, ref_px)
    assert tracker.mean_basis_60s(t0 + 69) is None

    strat = mods["strategy"].HighProbSniperStrategy(mods["config"].DEFAULT_CONFIG)
    book = mods["strategy"].OrderBookTop(0.79, 0.80, 0.19, 0.20)
    fv_no_bn = pm.FairValueEstimate(ref_px, ref_px * 1.01, None, 0.0, 120.0, 0.0001, 0.1, 1.72, 1.72, 0.2, 0.95, 0.05, 0.2)
    assert strat.evaluate_tick(t0 + 180, int(t0), 180.0, fv_no_bn, book) is None
    fv_div = pm.FairValueEstimate(ref_px, ref_px * 1.01, None, 0.0, 120.0, 0.0001, 0.1, 1.72, 1.72, 0.2, 0.95, 0.05, 0.2, 0.99, 0.84, 0.16)
    assert strat.evaluate_tick(t0 + 181, int(t0), 181.0, fv_div, book) is None
    fv_ok = pm.FairValueEstimate(ref_px, ref_px * 1.01, None, 0.0, 120.0, 0.0001, 0.1, 1.60, 1.60, 0.2, 0.95, 0.05, 0.2, 1.40, 0.91, 0.09)
    entry_msg = strat.evaluate_tick(t0 + 182, int(t0), 182.0, fv_ok, book)
    assert entry_msg is not None and strat.active_position is not None
    assert strat.active_position.z_at_entry == pytest.approx(1.40)
    assert strat.active_position.p_true_at_entry == pytest.approx(0.91)

    fv_sl = pm.FairValueEstimate(ref_px, ref_px * 0.99, None, 0.0, 80.0, 0.0001, 0.1, -0.50, -0.50, -0.5, 0.30, 0.70, -0.5)
    sl_msg = strat.evaluate_tick(t0 + 220, int(t0), 220.0, fv_sl, mods["strategy"].OrderBookTop(0.25, 0.27, 0.72, 0.75))
    assert sl_msg is not None and "EMERGENCY_STOP_LOSS" in sl_msg


@pytest.mark.parametrize(("coin_dir", "ref_px"), ALL_COINS)
def test_v51_official_oracle_strike_and_live_fee_schedule(coin_dir: str, ref_px: float) -> None:
    """Enforce OFFICIAL_TWAP60/GAMMA_OFFICIAL strike gate and live Polymarket CLOB fee schedule."""
    mods = _load_coin(coin_dir)
    cfg = mods["config"].DEFAULT_CONFIG
    expected_fee = 0.05 if "btc" in coin_dir else 0.01
    assert cfg.crypto_taker_fee_rate == pytest.approx(expected_fee)
    assert cfg.skip_book_calibrated_windows is True
    assert cfg.require_official_oracle_strike is True

    pm = mods["pricing_model"]
    strat_cls = mods["strategy"].HighProbSniperStrategy
    book = mods["strategy"].OrderBookTop(0.79, 0.80, 0.19, 0.20)
    t0 = 1_795_440_000.0
    fv_ok = pm.FairValueEstimate(
        ref_px, ref_px * 1.01, None, 0.0, 120.0, 0.0001, 0.1, 1.60, 1.60, 0.2, 0.95, 0.05, 0.2, 1.45, 0.92, 0.08
    )

    strat = strat_cls(cfg)
    assert strat.evaluate_tick(t0 + 180, int(t0), 180.0, fv_ok, book, strike_source="BOOK_CALIBRATED") is None
    assert strat.evaluate_tick(t0 + 181, int(t0), 181.0, fv_ok, book, strike_source="POLY_RTDS_CHAINLINK") is None
    assert strat.active_position is None

    msg_twap = strat.evaluate_tick(t0 + 182, int(t0), 182.0, fv_ok, book, strike_source="OFFICIAL_TWAP60")
    assert msg_twap is not None and strat.active_position is not None

    fv_tp = pm.FairValueEstimate(
        ref_px, ref_px * 1.02, None, 0.0, 80.0, 0.0001, 0.1, 2.5, 2.5, 0.3, 0.99, 0.01, 0.3, 2.5, 0.99, 0.01
    )
    tp_msg = strat.evaluate_tick(
        t0 + 220, int(t0), 220.0, fv_tp, mods["strategy"].OrderBookTop(0.99, 0.999, 0.001, 0.01), strike_source="BOOK_CALIBRATED"
    )
    assert tp_msg is not None and "EARLY_TAKE_PROFIT" in tp_msg

    strat_g = strat_cls(cfg)
    assert strat_g.evaluate_tick(t0 + 183, int(t0), 183.0, fv_ok, book, strike_source="GAMMA_OFFICIAL") is not None

    mon_src = (FRIEND_ROOT / coin_dir / "run_monitor.py").read_text(encoding="utf-8")
    assert "strike_source=st.strike_source" in mon_src
    assert "5m v5.1" in mon_src
