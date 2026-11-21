"""Unit tests for v4.3 reliability and accuracy upgrades across BTC/ETH/SOL."""
from __future__ import annotations

import importlib.util
import sys
import time
import types
from pathlib import Path
from typing import Any, Dict, List

import pytest
import requests

FRIEND_ROOT = Path(__file__).resolve().parent
COIN_DIRS = ("polymarket-btc-5m", "polymarket-eth-5m", "polymarket-sol-5m")


def _load_coin_modules(coin_dir_name: str) -> Dict[str, types.ModuleType]:
    """Load coin `src` modules under a unique isolated package name."""
    pkg_dir = FRIEND_ROOT / coin_dir_name / "src"
    pkg_name = f"_v43_{coin_dir_name.replace('-', '_')}"
    pkg = types.ModuleType(pkg_name)
    pkg.__path__ = [str(pkg_dir)]
    sys.modules[pkg_name] = pkg

    mods: Dict[str, types.ModuleType] = {}
    for mod_name in (
        "config",
        "fees",
        "pricing_model",
        "strategy",
        "storage",
        "chainlink_oracle",
        "feeds",
        "tg_notifier",
    ):
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


@pytest.mark.parametrize("coin_dir", COIN_DIRS)
def test_twap_near_utc_and_boundary_priority(coin_dir: str) -> None:
    mods = _load_coin_modules(coin_dir)
    ChainlinkOracleFeed = mods["chainlink_oracle"].ChainlinkOracleFeed
    LiveMarketHub = mods["feeds"].LiveMarketHub
    RollingMicrostructureTracker = mods["pricing_model"].RollingMicrostructureTracker

    boundary_ts = 1795200000.0
    feed = ChainlinkOracleFeed()
    feed.on_chainlink_tick(
        price=90100.0,
        source="POLYBOLT_CHAINLINK",
        oracle_ts=boundary_ts - 4.0,
        explicit_twap_60s=90010.0,
    )
    feed.on_chainlink_tick(
        price=90120.0,
        source="POLYBOLT_CHAINLINK",
        oracle_ts=boundary_ts - 0.8,
        explicit_twap_60s=90055.25,
    )
    feed.on_chainlink_tick(
        price=90140.0,
        source="POLYBOLT_CHAINLINK",
        oracle_ts=boundary_ts + 2.5,
        explicit_twap_60s=90075.0,
    )

    assert feed.get_twap_near_utc(boundary_ts, max_diff_sec=6.0) == pytest.approx(90055.25)
    assert feed.get_twap_near_utc(boundary_ts + 30.0, max_diff_sec=6.0) is None

    hub = LiveMarketHub(RollingMicrostructureTracker())
    hub.chainlink.on_chainlink_tick(
        price=90200.0,
        source="POLYBOLT_CHAINLINK",
        oracle_ts=boundary_ts - 0.5,
        explicit_twap_60s=90042.5,
    )
    hub.sync_window_boundary(boundary_ts + 0.4)
    assert hub.state.current_window_ts == int(boundary_ts)
    assert hub.state.strike_price == pytest.approx(90042.5)
    assert hub.state.strike_source == "POLYBOLT_TWAP_T0"


@pytest.mark.parametrize("coin_dir", COIN_DIRS)
@pytest.mark.parametrize(
    "protected_source",
    ("GAMMA_OFFICIAL", "POLYBOLT_TWAP_T0", "CHAINLINK_TWAP_T0"),
)
def test_calibrate_strike_preserves_official_and_twap_t0(
    coin_dir: str, protected_source: str
) -> None:
    mods = _load_coin_modules(coin_dir)
    LiveFeedState = mods["feeds"].LiveFeedState
    OrderBookTop = mods["strategy"].OrderBookTop

    st = LiveFeedState()
    st.current_window_ts = 1795200000
    st.strike_price = 100.25
    st.strike_source = protected_source
    st.book = OrderBookTop(up_bid=0.60, up_ask=0.62, down_bid=0.38, down_ask=0.40)

    res = st.calibrate_strike_from_opening_book(
        cl_spot=101.0, vol_1s=0.0005, elapsed_sec=5.0
    )
    assert res is None
    assert st.strike_price == pytest.approx(100.25)
    assert st.strike_source == protected_source


@pytest.mark.parametrize("coin_dir", COIN_DIRS)
def test_fetch_5m_market_metadata_extracts_official_final_price(coin_dir: str) -> None:
    mods = _load_coin_modules(coin_dir)
    fetch_5m_market_metadata = mods["feeds"].fetch_5m_market_metadata

    class _FakeResp:
        def __init__(self, status_code: int, payload: Any) -> None:
            self.status_code = status_code
            self._payload = payload

        def json(self) -> Any:
            return self._payload

    class _FakeSession:
        def __init__(self, payload: Any) -> None:
            self._payload = payload
            self.calls: List[Dict[str, Any]] = []

        def get(self, url: str, params: Dict[str, Any], timeout: float) -> _FakeResp:
            self.calls.append({"url": url, "params": params, "timeout": timeout})
            return _FakeResp(200, self._payload)

    sess = _FakeSession(
        [
            {
                "eventMetadata": {"priceToBeat": "2950.12", "finalPrice": "2954.88"},
                "markets": [
                    {
                        "question": "Up or Down",
                        "outcomes": ["Up", "Down"],
                        "clobTokenIds": ["tok_up", "tok_dn"],
                        "outcomePrices": ["1", "0"],
                    }
                ],
            }
        ]
    )
    meta = fetch_5m_market_metadata(1795200000, session=sess)  # type: ignore[arg-type]
    assert meta is not None
    assert meta["official_strike"] == pytest.approx(2950.12)
    assert meta["official_final_price"] == pytest.approx(2954.88)
    assert meta["resolved_winner"] == "UP"

    bad_sess = _FakeSession(
        [
            {
                "eventMetadata": {"priceToBeat": "2950.12", "finalPrice": "not_a_number"},
                "markets": [
                    {
                        "outcomes": ["Up", "Down"],
                        "clobTokenIds": ["tok_up", "tok_dn"],
                        "outcomePrices": ["1", "0"],
                    }
                ],
            }
        ]
    )
    assert fetch_5m_market_metadata(1795200000, session=bad_sess) is None  # type: ignore[arg-type]


@pytest.mark.parametrize("coin_dir", COIN_DIRS)
def test_last_book_update_local_ts_default_zero(coin_dir: str) -> None:
    mods = _load_coin_modules(coin_dir)
    LiveFeedState = mods["feeds"].LiveFeedState

    st = LiveFeedState()
    assert st.last_book_update_local_ts == 0.0
    st.last_book_update_local_ts = 1795200001.5
    assert st.last_book_update_local_ts == pytest.approx(1795200001.5)


def test_tg_send_message_sync_retries_and_resets_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    mods = _load_coin_modules("polymarket-btc-5m")
    TelegramBotService = mods["tg_notifier"].TelegramBotService

    monkeypatch.setenv("TG_BOT_TOKEN", "123456:DUMMY_TEST_TOKEN")
    monkeypatch.setenv("TG_CHAT_ID", "999000111")
    svc = TelegramBotService(
        db_path=str(tmp_path / "live_test.db"),
        live_mode=True,
    )
    monkeypatch.setattr(time, "sleep", lambda *_: None)

    class _Resp:
        def __init__(self, status_code: int, text: str = "err") -> None:
            self.status_code = status_code
            self.text = text

        def json(self) -> Dict[str, Any]:
            return {}

    reset_calls = 0

    class _FakeSession:
        def __init__(self, steps: List[Any]) -> None:
            self._steps = steps
            self.closed = False

        def post(self, url: str, json: Dict[str, Any], timeout: float) -> Any:
            step = self._steps.pop(0)
            if isinstance(step, Exception):
                raise step
            return step

        def close(self) -> None:
            self.closed = True

    steps: List[Any] = [
        requests.ConnectionError("stale keepalive"),
        _Resp(502, "bad gateway"),
        _Resp(200, '{"ok": true}'),
    ]
    shared_steps_session = _FakeSession(steps)
    svc._send_http = shared_steps_session  # type: ignore[assignment]

    def _fake_reset() -> None:
        nonlocal reset_calls
        reset_calls += 1
        svc._send_http = _FakeSession(steps)  # type: ignore[assignment]

    monkeypatch.setattr(svc, "_reset_send_session", _fake_reset)

    ok = svc.send_message_sync("hello v4.3", with_keyboard=False)
    assert ok is True
    assert reset_calls == 2
    assert len(steps) == 0

    fail_steps: List[Any] = [
        requests.ConnectionError("down 1"),
        requests.ConnectionError("down 2"),
        requests.ConnectionError("down 3"),
    ]
    svc._send_http = _FakeSession(fail_steps)  # type: ignore[assignment]
    monkeypatch.setattr(
        svc,
        "_reset_send_session",
        lambda: setattr(svc, "_send_http", _FakeSession(fail_steps)),
    )
    ok_fail = svc.send_message_sync("fail v4.3", with_keyboard=False)
    assert ok_fail is False
    captured = capsys.readouterr()
    assert "[TG SEND WARN]" in captured.err


@pytest.mark.parametrize("coin_dir", COIN_DIRS)
def test_tg_channel_omits_reply_keyboard_and_400_fallback(
    coin_dir: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ensure channels omit REPLY_KEYBOARD while HTTP 400 strips parse_mode and reply_markup."""
    mods = _load_coin_modules(coin_dir)
    TelegramBotService = mods["tg_notifier"].TelegramBotService

    monkeypatch.setenv("TG_BOT_TOKEN", "123456:DUMMY_TEST_TOKEN")
    monkeypatch.setenv("TG_CHAT_ID", "999000111")
    svc = TelegramBotService(db_path=str(tmp_path / "live_tg.db"), live_mode=True)
    monkeypatch.setattr(time, "sleep", lambda *_: None)

    class _Resp:
        def __init__(self, status_code: int, text: str = "ok") -> None:
            self.status_code = status_code
            self.text = text

        def json(self) -> Dict[str, Any]:
            return {}

    sent_payloads: List[Dict[str, Any]] = []
    responses: List[_Resp] = [_Resp(200), _Resp(200), _Resp(400, "bad"), _Resp(200)]

    class _RecordingSession:
        def __init__(self) -> None:
            self.closed = False

        def post(self, url: str, json: Dict[str, Any], timeout: float) -> _Resp:
            sent_payloads.append(dict(json))
            return responses.pop(0)

        def close(self) -> None:
            self.closed = True

    svc._send_http = _RecordingSession()  # type: ignore[assignment]

    assert svc.send_message_sync("channel msg", chat_id="-1001234567890", with_keyboard=True) is True
    assert "reply_markup" not in sent_payloads[0]
    assert sent_payloads[0].get("parse_mode") == "HTML"

    assert svc.send_message_sync("dm msg", chat_id="999000111", with_keyboard=True) is True
    assert "reply_markup" in sent_payloads[1]

    assert svc.send_message_sync("fallback msg", chat_id="999000111", with_keyboard=True) is True
    assert len(sent_payloads) == 4
    assert "parse_mode" not in sent_payloads[3]
    assert "reply_markup" not in sent_payloads[3]


@pytest.mark.parametrize("coin_dir", COIN_DIRS)
def test_run_monitor_v43_guards_present(coin_dir: str) -> None:
    text = (FRIEND_ROOT / coin_dir / "run_monitor.py").read_text(encoding="utf-8")
    assert "GAMMA_PREV_FINAL" in text
    assert "last_book_update_local_ts" in text
    assert "book_age > 5.0" in text
    assert "fee_rate=cfg.crypto_taker_fee_rate" in text
    assert "live_exec.get_pusd_balance" in text
