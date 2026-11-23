"""Unit tests for Surfshark & Polymarket VPN Watchdog."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import vpn_watchdog as vw


def _write_fake_env(root: Path, token: str = "bot_tok_123", chat_id: str = "111222333") -> None:
    btc_dir = root / "polymarket-btc-5m"
    btc_dir.mkdir(parents=True, exist_ok=True)
    (btc_dir / ".env").write_text(
        f"TG_BOT_TOKEN={token}\nTG_CHAT_ID={chat_id}\n",
        encoding="utf-8",
    )


def _patch_probes(
    monkeypatch: Any,
    *,
    scutil_seq: list[str],
    geo: dict[str, Any],
    clob: dict[str, Any] | None = None,
    binance: dict[str, Any] | None = None,
    reconn_result: bool = True,
) -> dict[str, Any]:
    calls: dict[str, Any] = {"scutil_start": 0, "tg_alerts": []}
    statuses = iter(scutil_seq)
    last_status = scutil_seq[-1]

    def fake_status(_service: str) -> str:
        nonlocal last_status
        val = next(statuses, last_status)
        last_status = val
        return val

    def fake_start(_service: str) -> bool:
        calls["scutil_start"] += 1
        return reconn_result

    clob_resp = clob if clob is not None else {"ok": True, "status_code": 200, "rtt_ms": 235.0}
    bn_resp = binance if binance is not None else {"ok": True, "status_code": 200, "rtt_ms": 95.0}

    def fake_geo(_session: Any, timeout: float = 4.0) -> dict[str, Any]:
        del timeout
        return dict(geo)

    def fake_rtt(_session: Any, url: str, timeout: float = 4.0) -> dict[str, Any]:
        del timeout
        if "binance" in url:
            return dict(bn_resp)
        return dict(clob_resp)

    def fake_tg(
        token: str, chat_id: str, text: str, session: Any = None
    ) -> bool:
        del session
        calls["tg_alerts"].append({"token": token, "chat_id": chat_id, "text": text})
        return True

    monkeypatch.setattr(vw, "check_scutil_status", fake_status)
    monkeypatch.setattr(vw, "trigger_scutil_start", fake_start)
    monkeypatch.setattr(vw, "probe_geoblock", fake_geo)
    monkeypatch.setattr(vw, "probe_endpoint_rtt", fake_rtt)
    monkeypatch.setattr(vw, "send_tg_alert", fake_tg)
    return calls


def test_healthy_hk_run_and_same_ip_increments_checks(tmp_path: Path, monkeypatch: Any) -> None:
    _write_fake_env(tmp_path)
    geo = {"ok": True, "blocked": False, "country": "HK", "ip": "198.51.100.14", "rtt_ms": 180.0}
    calls = _patch_probes(monkeypatch, scutil_seq=["Connected"], geo=geo)

    s1 = vw.run_once(root=tmp_path, reconnect_wait_sec=0.0, now_ts=1795320000.0)
    assert s1["degraded"] is False
    assert s1["severity"] == "ok"
    assert s1["ip_changed"] is False
    assert len(s1["ip_history"]) == 1
    assert s1["ip_history"][0]["checks"] == 1
    assert calls["scutil_start"] == 0
    assert calls["tg_alerts"] == []

    saved = json.loads((tmp_path / "logs" / "vpn_health.json").read_text(encoding="utf-8"))
    assert saved["country"] == "HK"
    assert saved["clob_rtt_ms"] == 235.0
    assert saved["binance_rtt_ms"] == 95.0

    s2 = vw.run_once(root=tmp_path, reconnect_wait_sec=0.0, now_ts=1795320060.0)
    assert s2["ip_changed"] is False
    assert len(s2["ip_history"]) == 1
    assert s2["ip_history"][0]["checks"] == 2


def test_ip_change_detection_within_hk(tmp_path: Path, monkeypatch: Any) -> None:
    _write_fake_env(tmp_path)
    _patch_probes(
        monkeypatch,
        scutil_seq=["Connected"],
        geo={"ok": True, "blocked": False, "country": "HK", "ip": "198.51.100.14", "rtt_ms": 180.0},
    )
    vw.run_once(root=tmp_path, reconnect_wait_sec=0.0, now_ts=1795320000.0)

    _patch_probes(
        monkeypatch,
        scutil_seq=["Connected"],
        geo={"ok": True, "blocked": False, "country": "HK", "ip": "198.51.100.99", "rtt_ms": 175.0},
    )
    s2 = vw.run_once(root=tmp_path, reconnect_wait_sec=0.0, now_ts=1795320060.0)
    assert s2["ip_changed"] is True
    assert [entry["ip"] for entry in s2["ip_history"]] == ["198.51.100.14", "198.51.100.99"]
    assert s2["degraded"] is False


def test_disconnected_auto_reconnect_success_and_failure(tmp_path: Path, monkeypatch: Any) -> None:
    _write_fake_env(tmp_path)
    geo_ok = {"ok": True, "blocked": False, "country": "HK", "ip": "198.51.100.14", "rtt_ms": 180.0}
    calls_ok = _patch_probes(
        monkeypatch,
        scutil_seq=["Disconnected", "Connected"],
        geo=geo_ok,
        reconn_result=True,
    )
    s_rec = vw.run_once(root=tmp_path, reconnect_wait_sec=0.0, now_ts=1795320000.0)
    assert calls_ok["scutil_start"] == 1
    assert s_rec["reconnect_attempted"] is True
    assert s_rec["vpn_status"] == "Connected"
    assert s_rec["degraded"] is False
    assert calls_ok["tg_alerts"] == []

    calls_fail = _patch_probes(
        monkeypatch,
        scutil_seq=["Disconnected", "Disconnected"],
        geo={"ok": False, "error": "timeout"},
        reconn_result=False,
    )
    s_fail = vw.run_once(root=tmp_path, reconnect_wait_sec=0.0, now_ts=1795320060.0)
    assert calls_fail["scutil_start"] == 1
    assert s_fail["reconnect_attempted"] is True
    assert s_fail["degraded"] is True
    assert s_fail["severity"] == "critical"
    assert s_fail["alert_sent"] is True
    assert len(calls_fail["tg_alerts"]) == 1


def test_no_reconnect_flag_skips_scutil_start(tmp_path: Path, monkeypatch: Any) -> None:
    _write_fake_env(tmp_path)
    calls = _patch_probes(
        monkeypatch,
        scutil_seq=["Disconnected"],
        geo={"ok": False, "error": "down"},
    )
    state = vw.run_once(
        root=tmp_path,
        allow_reconnect=False,
        reconnect_wait_sec=0.0,
        now_ts=1795320000.0,
    )
    assert calls["scutil_start"] == 0
    assert state["reconnect_attempted"] is False
    assert state["degraded"] is True
    assert state["severity"] == "critical"


def test_geoblock_blocked_triggers_immediate_critical_alert(tmp_path: Path, monkeypatch: Any) -> None:
    _write_fake_env(tmp_path)
    calls = _patch_probes(
        monkeypatch,
        scutil_seq=["Connected"],
        geo={"ok": True, "blocked": True, "country": "TW", "ip": "1.2.3.4", "rtt_ms": 50.0},
    )
    state = vw.run_once(root=tmp_path, reconnect_wait_sec=0.0, now_ts=1795320000.0)
    assert state["degraded"] is True
    assert state["severity"] == "critical"
    assert "geoblock_blocked:TW" in state["reason"]
    assert state["alert_sent"] is True
    assert len(calls["tg_alerts"]) == 1


def test_country_drift_triggers_immediate_warning(tmp_path: Path, monkeypatch: Any) -> None:
    _write_fake_env(tmp_path)
    calls = _patch_probes(
        monkeypatch,
        scutil_seq=["Connected"],
        geo={"ok": True, "blocked": False, "country": "NL", "ip": "9.9.9.9", "rtt_ms": 220.0},
    )
    state = vw.run_once(root=tmp_path, reconnect_wait_sec=0.0, now_ts=1795320000.0)
    assert state["degraded"] is True
    assert state["severity"] == "warning"
    assert state["reason"] == "country_drift:NL!=HK"
    assert state["alert_sent"] is True
    assert len(calls["tg_alerts"]) == 1


def test_transient_http_failure_hysteresis_and_recovery(tmp_path: Path, monkeypatch: Any) -> None:
    _write_fake_env(tmp_path)
    calls1 = _patch_probes(
        monkeypatch,
        scutil_seq=["Connected"],
        geo={"ok": True, "blocked": False, "country": "HK", "ip": "198.51.100.14", "rtt_ms": 180.0},
        clob={"ok": False, "error": "timeout"},
    )
    s1 = vw.run_once(root=tmp_path, reconnect_wait_sec=0.0, now_ts=1795320000.0)
    assert s1["consecutive_failures"] == 1
    assert s1["degraded"] is False
    assert calls1["tg_alerts"] == []

    calls2 = _patch_probes(
        monkeypatch,
        scutil_seq=["Connected"],
        geo={"ok": True, "blocked": False, "country": "HK", "ip": "198.51.100.14", "rtt_ms": 180.0},
        clob={"ok": False, "error": "timeout"},
    )
    s2 = vw.run_once(root=tmp_path, reconnect_wait_sec=0.0, now_ts=1795320060.0)
    assert s2["consecutive_failures"] == 2
    assert s2["degraded"] is True
    assert s2["severity"] == "critical"
    assert s2["alert_sent"] is True
    assert len(calls2["tg_alerts"]) == 1

    calls3 = _patch_probes(
        monkeypatch,
        scutil_seq=["Connected"],
        geo={"ok": True, "blocked": False, "country": "HK", "ip": "198.51.100.14", "rtt_ms": 180.0},
        clob={"ok": True, "status_code": 200, "rtt_ms": 230.0},
    )
    s3 = vw.run_once(root=tmp_path, reconnect_wait_sec=0.0, now_ts=1795320120.0)
    assert s3["consecutive_failures"] == 0
    assert s3["degraded"] is False
    assert s3["alert_kind"] == "recovered"
    assert s3["alert_sent"] is True
    assert len(calls3["tg_alerts"]) == 1
    assert "RECOVERED" in calls3["tg_alerts"][0]["text"]


def test_alert_cooldown_suppresses_within_900s_and_resends_after(
    tmp_path: Path, monkeypatch: Any
) -> None:
    _write_fake_env(tmp_path)
    geo_blocked = {"ok": True, "blocked": True, "country": "TW", "ip": "1.2.3.4", "rtt_ms": 40.0}

    calls1 = _patch_probes(monkeypatch, scutil_seq=["Connected"], geo=geo_blocked)
    s1 = vw.run_once(root=tmp_path, reconnect_wait_sec=0.0, now_ts=1795320000.0)
    assert s1["alert_sent"] is True
    assert len(calls1["tg_alerts"]) == 1

    calls2 = _patch_probes(monkeypatch, scutil_seq=["Connected"], geo=geo_blocked)
    s2 = vw.run_once(root=tmp_path, reconnect_wait_sec=0.0, now_ts=1795320600.0)
    assert s2["degraded"] is True
    assert s2["should_alert"] is False
    assert s2["alert_sent"] is False
    assert calls2["tg_alerts"] == []

    calls3 = _patch_probes(monkeypatch, scutil_seq=["Connected"], geo=geo_blocked)
    s3 = vw.run_once(root=tmp_path, reconnect_wait_sec=0.0, now_ts=1795320901.0)
    assert s3["degraded"] is True
    assert s3["should_alert"] is True
    assert s3["alert_sent"] is True
    assert len(calls3["tg_alerts"]) == 1


def test_load_primary_tg_config_ignores_tg_control_json(tmp_path: Path) -> None:
    _write_fake_env(tmp_path, token="tok_xyz", chat_id="111111111, 99999999")
    data_dir = tmp_path / "polymarket-btc-5m" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / ".tg_control.json").write_text(
        json.dumps({"subscribers": ["88888888", "77777777"]}),
        encoding="utf-8",
    )

    token, chat_id = vw.load_primary_tg_config(tmp_path)
    assert token == "tok_xyz"
    assert chat_id == "111111111"
