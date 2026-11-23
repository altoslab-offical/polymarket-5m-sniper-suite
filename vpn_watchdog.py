#!/usr/bin/env python3
"""Lightweight Surfshark WireGuard & Polymarket geoblock watchdog and RTT benchmark."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import logging
from logging.handlers import RotatingFileHandler
import math
import os
from pathlib import Path
import statistics
import subprocess
import time
from typing import Any

from dotenv import dotenv_values
import requests

DEFAULT_ROOT = Path(__file__).resolve().parent
DEFAULT_SERVICE = "Surfshark. WireGuard"
DEFAULT_EXPECTED_COUNTRY = "HK"
GEOBLOCK_URL = "https://polymarket.com/api/geoblock"
CLOB_OK_URL = "https://clob.polymarket.com/ok"
GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events?limit=1"
BINANCE_TIME_URL = "https://api.binance.com/api/v3/time"
TG_SEND_URL = "https://api.telegram.org/bot{token}/sendMessage"
DEFAULT_ALERT_COOLDOWN_SEC = 900.0
DEFAULT_CLOB_SLOW_MS = 1500.0
TZ_TAIPEI = timezone(timedelta(hours=8))


def check_scutil_status(service: str = DEFAULT_SERVICE) -> str:
    """Return the first line of `scutil --nc status <service>`, or an `Error:...` tag."""
    try:
        proc = subprocess.run(
            ["/usr/sbin/scutil", "--nc", "status", service],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return f"Error:{type(exc).__name__}"

    lines = [line.strip() for line in (proc.stdout or "").splitlines() if line.strip()]
    if lines:
        return lines[0]
    if proc.returncode != 0:
        return f"Error:rc={proc.returncode}"
    return "Unknown"


def trigger_scutil_start(service: str = DEFAULT_SERVICE) -> bool:
    """Attempt to bring up the macOS NetworkExtension VPN via `scutil --nc start`."""
    try:
        proc = subprocess.run(
            ["/usr/sbin/scutil", "--nc", "start", service],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        return proc.returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def probe_geoblock(session: requests.Session, timeout: float = 4.0) -> dict[str, Any]:
    """Query Polymarket `/api/geoblock` and return status, country, IP, and RTT ms."""
    t0 = time.perf_counter()
    try:
        resp = session.get(GEOBLOCK_URL, timeout=timeout)
        rtt_ms = round((time.perf_counter() - t0) * 1000.0, 1)
        if resp.status_code != 200:
            return {
                "ok": False,
                "status_code": int(resp.status_code),
                "rtt_ms": rtt_ms,
                "error": f"http_{resp.status_code}",
            }
        data = resp.json()
        if not isinstance(data, dict):
            return {"ok": False, "rtt_ms": rtt_ms, "error": "invalid_json_type"}
        return {
            "ok": True,
            "blocked": bool(data.get("blocked", False)),
            "country": str(data.get("country") or "").upper(),
            "ip": str(data.get("ip") or ""),
            "rtt_ms": rtt_ms,
        }
    except (requests.RequestException, ValueError, TypeError) as exc:
        return {"ok": False, "error": f"{type(exc).__name__}:{exc}"}


def probe_endpoint_rtt(
    session: requests.Session, url: str, timeout: float = 4.0
) -> dict[str, Any]:
    """Send a GET probe to `url` and measure status code plus RTT in milliseconds."""
    t0 = time.perf_counter()
    try:
        resp = session.get(url, timeout=timeout)
        rtt_ms = round((time.perf_counter() - t0) * 1000.0, 1)
        return {
            "ok": resp.status_code == 200,
            "status_code": int(resp.status_code),
            "rtt_ms": rtt_ms,
        }
    except requests.RequestException as exc:
        return {"ok": False, "error": f"{type(exc).__name__}:{exc}"}


def load_primary_tg_config(root: Path) -> tuple[str, str]:
    """Read primary owner Telegram bot token and chat ID solely from BTC `.env`."""
    env_path = root / "polymarket-btc-5m" / ".env"
    if not env_path.exists():
        return "", ""
    env = dotenv_values(env_path)
    token = str(env.get("TG_BOT_TOKEN") or "").strip()
    raw_chat = str(env.get("TG_CHAT_ID") or "").strip()
    chat_id = raw_chat.split(",")[0].strip() if raw_chat else ""
    if not token or not chat_id:
        return "", ""
    return token, chat_id


def send_tg_alert(
    token: str,
    chat_id: str,
    text: str,
    session: requests.Session | None = None,
) -> bool:
    """Send a single Telegram operational alert; returns False silently on failure."""
    if not token or not chat_id or not text:
        return False
    url = TG_SEND_URL.format(token=token)
    payload = {"chat_id": chat_id, "text": text}
    try:
        if session is not None:
            resp = session.post(url, json=payload, timeout=5.0)
            return resp.status_code == 200
        with requests.Session() as temp_session:
            resp = temp_session.post(url, json=payload, timeout=5.0)
            return resp.status_code == 200
    except requests.RequestException:
        return False


def load_state(path: Path) -> dict[str, Any]:
    """Load persisted JSON watchdog state, returning `{}` if missing or corrupt."""
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def save_state_atomic(path: Path, state: dict[str, Any]) -> None:
    """Atomically write watchdog JSON state via PID-scoped temporary file + `os.replace`."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    tmp_path.write_text(
        json.dumps(state, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    os.replace(tmp_path, path)


def update_ip_history(
    history: list[dict[str, Any]],
    ip: str,
    country: str,
    now_iso: str,
    max_entries: int = 10,
) -> tuple[list[dict[str, Any]], bool]:
    """Update rolling recent exit IP history; return `(history, ip_changed)`."""
    updated = [dict(entry) for entry in history if isinstance(entry, dict)]
    if not ip:
        return updated[-max_entries:], False

    if updated and updated[-1].get("ip") == ip and updated[-1].get("country") == country:
        updated[-1]["last_seen"] = now_iso
        updated[-1]["checks"] = int(updated[-1].get("checks") or 0) + 1
        return updated[-max_entries:], False

    ip_changed = bool(updated)
    updated.append(
        {
            "ip": ip,
            "country": country,
            "first_seen": now_iso,
            "last_seen": now_iso,
            "checks": 1,
        }
    )
    return updated[-max_entries:], ip_changed


def _classify_health_issue(
    *,
    vpn_status: str,
    geo: dict[str, Any],
    clob: dict[str, Any],
    expected_country: str,
    prev_failures: int,
    prev_slow: int,
    clob_slow_ms: float,
) -> tuple[bool, str, str, int, int]:
    """Classify probe outputs into `(degraded, severity, reason, failures, slow)`."""
    http_ok = bool(geo.get("ok")) and bool(clob.get("ok"))
    failures = 0 if http_ok else prev_failures + 1

    clob_rtt = float(clob.get("rtt_ms") or 0.0)
    slow = (prev_slow + 1) if (http_ok and clob_rtt > clob_slow_ms) else 0

    if vpn_status != "Connected":
        return True, "critical", f"vpn_disconnected:{vpn_status}", failures, slow

    if geo.get("ok") and geo.get("blocked") is True:
        country = str(geo.get("country") or "unknown")
        return True, "critical", f"geoblock_blocked:{country}", failures, slow

    if (
        geo.get("ok")
        and geo.get("blocked") is False
        and expected_country
        and str(geo.get("country") or "") != expected_country
    ):
        country = str(geo.get("country") or "unknown")
        return True, "warning", f"country_drift:{country}!={expected_country}", failures, slow

    if failures >= 2:
        return True, "critical", f"http_probe_failed:{failures}", failures, slow

    if slow >= 3:
        return True, "warning", f"clob_high_latency:{clob_rtt:.1f}ms", failures, slow

    return False, "ok", "", failures, slow


def evaluate_health(
    *,
    vpn_status: str,
    geo: dict[str, Any],
    clob: dict[str, Any],
    binance: dict[str, Any],
    expected_country: str,
    prev_state: dict[str, Any],
    now_ts: float,
    alert_cooldown_sec: float = DEFAULT_ALERT_COOLDOWN_SEC,
    clob_slow_ms: float = DEFAULT_CLOB_SLOW_MS,
) -> dict[str, Any]:
    """Compute deterministic health state, hysteresis counters, and cooldown-gated alerts."""
    del binance  # Retained in signature for telemetry parity and future threshold rules.
    prev_degraded = bool(prev_state.get("degraded", False))
    prev_severity = str(prev_state.get("severity") or "ok")
    prev_reason = str(prev_state.get("reason") or "")
    last_alert_ts = float(prev_state.get("last_alert_ts") or 0.0)

    degraded, severity, reason, failures, slow = _classify_health_issue(
        vpn_status=vpn_status,
        geo=geo,
        clob=clob,
        expected_country=expected_country,
        prev_failures=int(prev_state.get("consecutive_failures") or 0),
        prev_slow=int(prev_state.get("consecutive_slow") or 0),
        clob_slow_ms=clob_slow_ms,
    )

    should_alert = False
    alert_kind: str | None = None
    next_alert_ts = last_alert_ts

    if degraded:
        cooldown_elapsed = last_alert_ts <= 0.0 or (now_ts - last_alert_ts) >= alert_cooldown_sec
        escalated = prev_severity != "critical" and severity == "critical"
        new_critical = severity == "critical" and prev_degraded and reason != prev_reason
        if (not prev_degraded) or cooldown_elapsed or escalated or new_critical:
            should_alert = True
            alert_kind = severity
            next_alert_ts = now_ts
    elif prev_degraded:
        should_alert = True
        alert_kind = "recovered"
        next_alert_ts = 0.0

    return {
        "degraded": degraded,
        "severity": severity,
        "reason": reason,
        "consecutive_failures": failures,
        "consecutive_slow": slow,
        "should_alert": should_alert,
        "alert_kind": alert_kind,
        "last_alert_ts": next_alert_ts,
    }


def _configure_logger(logs_dir: Path) -> logging.Logger:
    """Configure a rotating file logger bound to `logs_dir/vpn_watchdog.log`."""
    logger = logging.getLogger("poly5m.vpn_watchdog")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    target = (logs_dir / "vpn_watchdog.log").resolve()

    for existing in list(logger.handlers):
        base = getattr(existing, "baseFilename", None)
        if base and Path(base).resolve() == target:
            return logger
        logger.removeHandler(existing)
        existing.close()

    handler = RotatingFileHandler(
        target,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    return logger


def _format_tg_alert(state: dict[str, Any]) -> str:
    """Format a concise alert message without exposing sensitive credentials."""
    kind = str(state.get("alert_kind") or "info").upper()
    header = (
        "[VPN RECOVERED // Surfshark 恢復正常]"
        if kind == "RECOVERED"
        else f"[VPN {kind} // Surfshark 異常通知]"
    )
    geo_str = f"{state.get('country') or '--'} / blocked={state.get('geoblock_blocked')}"
    clob_rtt = state.get("clob_rtt_ms")
    bn_rtt = state.get("binance_rtt_ms")
    clob_str = f"{clob_rtt}ms" if clob_rtt is not None else "ERR"
    bn_str = f"{bn_rtt}ms" if bn_rtt is not None else "ERR"
    lines = [
        header,
        f"• 服務：{state.get('service')} ({state.get('vpn_status')})",
        f"• 出口：{geo_str} | IP: {state.get('ip') or '--'}",
        f"• 延遲：CLOB {clob_str} | Binance {bn_str}",
    ]
    if state.get("reason"):
        lines.append(f"• 原因：{state['reason']}")
    if state.get("reconnect_attempted"):
        lines.append("• 自動處置：已觸發 scutil --nc start 重連")
    lines.append(f"• 時間：{state.get('checked_at')}")
    return "\n".join(lines)


def _build_state_payload(
    *,
    now_iso: str,
    ts_now: float,
    service: str,
    expected_country: str,
    vpn_status: str,
    reconnect_attempted: bool,
    geo: dict[str, Any],
    clob: dict[str, Any],
    binance: dict[str, Any],
    eval_res: dict[str, Any],
    ip_history: list[dict[str, Any]],
    ip_changed: bool,
) -> dict[str, Any]:
    """Assemble the serialized `vpn_health.json` snapshot."""
    return {
        "checked_at": now_iso,
        "checked_ts": round(ts_now, 3),
        "service": service,
        "expected_country": expected_country,
        "vpn_status": vpn_status,
        "reconnect_attempted": reconnect_attempted,
        "geoblock_ok": bool(geo.get("ok", False)),
        "geoblock_blocked": geo.get("blocked") if geo.get("ok") else None,
        "country": geo.get("country") if geo.get("ok") else None,
        "ip": geo.get("ip") if geo.get("ok") else None,
        "ip_changed": ip_changed,
        "geoblock_rtt_ms": geo.get("rtt_ms"),
        "clob_ok": bool(clob.get("ok", False)),
        "clob_rtt_ms": clob.get("rtt_ms"),
        "binance_ok": bool(binance.get("ok", False)),
        "binance_rtt_ms": binance.get("rtt_ms"),
        "degraded": eval_res["degraded"],
        "severity": eval_res["severity"],
        "reason": eval_res["reason"],
        "consecutive_failures": eval_res["consecutive_failures"],
        "consecutive_slow": eval_res["consecutive_slow"],
        "last_alert_ts": eval_res["last_alert_ts"],
        "alert_kind": eval_res["alert_kind"],
        "should_alert": eval_res["should_alert"],
        "alert_sent": False,
        "ip_history": ip_history,
    }


def _ensure_vpn_connected(
    service: str,
    allow_reconnect: bool,
    reconnect_wait_sec: float,
    logger: logging.Logger,
) -> tuple[str, bool]:
    """Check VPN connection status and optionally trigger `scutil --nc start`."""
    vpn_status = check_scutil_status(service)
    if vpn_status == "Connected" or not allow_reconnect:
        return vpn_status, False
    logger.warning("VPN status=%s != Connected; running scutil --nc start", vpn_status)
    started = trigger_scutil_start(service)
    if started and reconnect_wait_sec > 0:
        time.sleep(reconnect_wait_sec)
    return check_scutil_status(service), True


def _log_watchdog_summary(
    logger: logging.Logger, eval_res: dict[str, Any], state: dict[str, Any]
) -> None:
    """Emit a single structured status line to `vpn_watchdog.log`."""
    tag = str(eval_res["alert_kind"] or eval_res["severity"]).upper()
    logger.info(
        "[%s] vpn=%s country=%s blocked=%s ip=%s clob=%sms binance=%sms reconn=%s reason=%s",
        tag,
        state["vpn_status"],
        state["country"],
        state["geoblock_blocked"],
        state["ip"],
        state["clob_rtt_ms"],
        state["binance_rtt_ms"],
        state["reconnect_attempted"],
        state["reason"] or "-",
    )


def run_once(
    *,
    root: Path = DEFAULT_ROOT,
    service: str = DEFAULT_SERVICE,
    expected_country: str = DEFAULT_EXPECTED_COUNTRY,
    allow_reconnect: bool = True,
    notify: bool = True,
    reconnect_wait_sec: float = 6.0,
    alert_cooldown_sec: float = DEFAULT_ALERT_COOLDOWN_SEC,
    clob_slow_ms: float = DEFAULT_CLOB_SLOW_MS,
    now_ts: float | None = None,
) -> dict[str, Any]:
    """Execute one watchdog pass: check VPN, reconnect if needed, probe APIs, save state."""
    logs_dir = root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    logger = _configure_logger(logs_dir)

    ts_now = time.time() if now_ts is None else float(now_ts)
    now_iso = datetime.fromtimestamp(ts_now, tz=TZ_TAIPEI).isoformat(timespec="seconds")
    state_path = logs_dir / "vpn_health.json"
    prev_state = load_state(state_path)
    vpn_status, reconnect_attempted = _ensure_vpn_connected(
        service, allow_reconnect, reconnect_wait_sec, logger
    )

    with requests.Session() as session:
        geo = probe_geoblock(session)
        clob = probe_endpoint_rtt(session, CLOB_OK_URL)
        binance = probe_endpoint_rtt(session, BINANCE_TIME_URL)
        eval_res = evaluate_health(
            vpn_status=vpn_status,
            geo=geo,
            clob=clob,
            binance=binance,
            expected_country=expected_country,
            prev_state=prev_state,
            now_ts=ts_now,
            alert_cooldown_sec=alert_cooldown_sec,
            clob_slow_ms=clob_slow_ms,
        )
        prev_hist = list(prev_state.get("ip_history") or [])
        ip_history, ip_changed = (
            update_ip_history(prev_hist, str(geo["ip"]), str(geo.get("country") or ""), now_iso)
            if (geo.get("ok") and geo.get("ip"))
            else (prev_hist, False)
        )
        state = _build_state_payload(
            now_iso=now_iso,
            ts_now=ts_now,
            service=service,
            expected_country=expected_country,
            vpn_status=vpn_status,
            reconnect_attempted=reconnect_attempted,
            geo=geo,
            clob=clob,
            binance=binance,
            eval_res=eval_res,
            ip_history=ip_history,
            ip_changed=ip_changed,
        )
        if eval_res["should_alert"] and notify:
            token, chat_id = load_primary_tg_config(root)
            if token and chat_id:
                state["alert_sent"] = send_tg_alert(
                    token, chat_id, _format_tg_alert(state), session=session
                )

    save_state_atomic(state_path, state)
    _log_watchdog_summary(logger, eval_res, state)
    return state


def _sample_warm_rtts(
    session: requests.Session, url: str, samples: int
) -> tuple[list[float], int | None, str | None]:
    """Perform 1 warmup GET plus `samples` timed GETs and return `(rtts_ms, last_code, err)`."""
    warm = probe_endpoint_rtt(session, url)
    if not warm.get("ok"):
        return [], warm.get("status_code"), str(warm.get("error") or "probe_failed")

    rtts: list[float] = []
    last_code: int | None = warm.get("status_code")
    for _ in range(max(1, samples)):
        res = probe_endpoint_rtt(session, url)
        if not res.get("ok"):
            return rtts, res.get("status_code"), str(res.get("error") or "probe_failed")
        rtts.append(float(res["rtt_ms"]))
        last_code = res.get("status_code")
    return rtts, last_code, None


def run_benchmark(
    service: str = DEFAULT_SERVICE,
    expected_country: str = DEFAULT_EXPECTED_COUNTRY,
    samples: int = 5,
) -> int:
    """Print VPN state, Polymarket geoblock, and warm RTT percentile benchmark."""
    vpn_status = check_scutil_status(service)
    print(f"VPN Service : {service} -> {vpn_status}")
    with requests.Session() as session:
        geo = probe_geoblock(session)
        if geo.get("ok"):
            print(
                f"Geoblock    : country={geo['country']} blocked={geo['blocked']} "
                f"ip={geo['ip']} rtt={geo['rtt_ms']}ms (expected={expected_country})"
            )
        else:
            print(f"Geoblock    : ERROR {geo.get('error')}")

        targets = [
            ("CLOB /ok", CLOB_OK_URL),
            ("Gamma /events", GAMMA_EVENTS_URL),
            ("Binance /time", BINANCE_TIME_URL),
        ]
        for label, url in targets:
            rtts, status_code, err = _sample_warm_rtts(session, url, samples)
            if not rtts:
                print(f"{label:<14}: ERROR status={status_code} err={err}")
                continue
            ordered = sorted(rtts)
            p90_idx = min(len(ordered) - 1, max(0, math.ceil(0.9 * len(ordered)) - 1))
            stdev = statistics.stdev(rtts) if len(rtts) > 1 else 0.0
            print(
                f"{label:<14}: p50={statistics.median(rtts):.1f}ms "
                f"p90={ordered[p90_idx]:.1f}ms min={ordered[0]:.1f}ms "
                f"max={ordered[-1]:.1f}ms stdev={stdev:.1f}ms "
                f"status={status_code} (n={len(rtts)})"
            )

    healthy = vpn_status == "Connected" and bool(geo.get("ok")) and not geo.get("blocked")
    return 0 if healthy else 1


def main() -> int:
    """CLI entrypoint for `vpn_watchdog.py`."""
    parser = argparse.ArgumentParser(description="Surfshark & Polymarket VPN Watchdog")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--service", default=DEFAULT_SERVICE)
    parser.add_argument("--expected-country", default=DEFAULT_EXPECTED_COUNTRY)
    parser.add_argument("--no-reconnect", action="store_true")
    parser.add_argument("--no-notify", action="store_true")
    parser.add_argument("--benchmark", action="store_true")
    parser.add_argument("--samples", type=int, default=5)
    args = parser.parse_args()

    if args.benchmark:
        return run_benchmark(
            service=args.service,
            expected_country=args.expected_country,
            samples=args.samples,
        )

    state = run_once(
        root=args.root,
        service=args.service,
        expected_country=args.expected_country,
        allow_reconnect=not args.no_reconnect,
        notify=not args.no_notify,
    )
    return 1 if state.get("degraded") else 0


if __name__ == "__main__":
    raise SystemExit(main())
