#!/usr/bin/env python3
"""Lightweight read-only daily review engine for the BTC/ETH/SOL 5m live sniper suite."""
from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
from typing import Any

from dotenv import dotenv_values
import requests

COINS: tuple[str, ...] = ("btc", "eth", "sol")
SCOUT_COINS: tuple[str, ...] = ("xrp", "doge")
TZ_TAIPEI = timezone(timedelta(hours=8))
GEOBLOCK_URL = "https://polymarket.com/api/geoblock"
TG_SEND_URL = "https://api.telegram.org/bot{token}/sendMessage"

_FILL_RE = re.compile(r"LIVE 實盤成交.*?\$\s*([0-9]+(?:\.[0-9]+)?)")
_ENTRY_ASK_RE = re.compile(r"\[ENTRY\s+(?:UP|DOWN)\].*?ask=\$([0-9]+(?:\.[0-9]+)?)")
_CHAINLINK_SRC_RE = re.compile(r"\(([A-Z0-9_]*CHAINLINK[A-Z0-9_]*)\)")
_BOOK_FRESH_RE = re.compile(r"book_age=([0-9]+(?:\.[0-9]+)?)ms\s+book_lag=([0-9]+(?:\.[0-9]+)?)ms")
_CLOCK_RE = re.compile(r"raw_offset_ms=([+-]?[0-9]+(?:\.[0-9]+)?)ms\s+eff_offset_ms=([+-]?[0-9]+(?:\.[0-9]+)?)ms")
_HEALTH_RE = re.compile(
    r"\[HEALTH\].*?cl_src=(\S+).*?"
    r"book_age_p50_ms=(\S+)\s+book_age_p99_ms=(\S+).*?"
    r"book_lag_p50_ms=(\S+)\s+book_lag_p99_ms=(\S+).*?"
    r"rollover_p99_ms=(\S+).*?"
    r"ws_book=(\d+)\s+ws_pc=(\d+)\s+rest_fallbacks=(\d+).*?"
    r"reconn\(([^)]*)\).*?stale_skips=(\d+).*?"
    r"clock_raw_ms=([+-]?[0-9]+(?:\.[0-9]+)?)\s+clock_eff_ms=([+-]?[0-9]+(?:\.[0-9]+)?)"
)


@dataclass(frozen=True)
class BucketStats:
    """Trade win/loss summary for an analytical bucket."""

    trades: int
    wins: int
    losses: int
    win_rate_pct: float | None
    net_pnl_usdc: float


def _apply_low_priority() -> None:
    """Lower CPU scheduling priority so review jobs never contend with live bots."""
    try:
        os.nice(19)
    except OSError:
        return


def _summarize_rows(rows: list[tuple[Any, ...]]) -> BucketStats:
    if not rows:
        return BucketStats(0, 0, 0, None, 0.0)
    wins = sum(1 for r in rows if int(r[6] or 0) == 1)
    total = len(rows)
    pnl = round(sum(float(r[5] or 0.0) for r in rows), 4)
    return BucketStats(
        trades=total,
        wins=wins,
        losses=total - wins,
        win_rate_pct=round((wins / total) * 100.0, 1),
        net_pnl_usdc=pnl,
    )


def _read_lock_pid(lock_path: Path) -> int | None:
    if not lock_path.exists():
        return None
    try:
        raw = lock_path.read_text(encoding="utf-8").strip().splitlines()
        return int(raw[0]) if raw else None
    except (OSError, ValueError):
        return None


def _get_live_monitor_pids() -> set[int]:
    try:
        out = subprocess.check_output(["ps", "-eo", "pid,args"], text=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return set()
    pids: set[int] = set()
    for line in out.splitlines():
        if "run_monitor.py" in line and "--live" in line:
            parts = line.strip().split(None, 1)
            if parts and parts[0].isdigit():
                pids.add(int(parts[0]))
    return pids


def parse_execution_slippage(log_text: str) -> dict[str, Any]:
    """Extract live entry fill slippage (`fill_price - quoted_ask`) in cents from monitor logs."""
    normalized = log_text.replace("\r", "\n")
    pending_fill: float | None = None
    diffs_cents: list[float] = []
    for line in normalized.splitlines():
        m_fill = _FILL_RE.search(line)
        if m_fill:
            pending_fill = float(m_fill.group(1))
            continue
        if pending_fill is not None:
            m_ask = _ENTRY_ASK_RE.search(line)
            if m_ask:
                diffs_cents.append(round((pending_fill - float(m_ask.group(1))) * 100.0, 2))
                pending_fill = None
    if not diffs_cents:
        return {"fills_matched": 0, "mean_slippage_cents": None, "max_slippage_cents": None}
    return {
        "fills_matched": len(diffs_cents),
        "mean_slippage_cents": round(sum(diffs_cents) / len(diffs_cents), 2),
        "max_slippage_cents": round(max(diffs_cents), 2),
    }


def _opt_float(token: str) -> float | None:
    if not token or token == "None":
        return None
    try:
        return float(token)
    except ValueError:
        return None


def _empty_health_metrics() -> dict[str, Any]:
    return {
        "book_age_p50_ms": None,
        "book_age_p99_ms": None,
        "book_lag_p50_ms": None,
        "book_lag_p99_ms": None,
        "rollover_p99_ms": None,
        "ws_book": 0,
        "ws_pc": 0,
        "rest_fallbacks": 0,
        "reconn_total": 0,
        "stale_skips": 0,
        "clock_raw_ms": None,
        "clock_eff_ms": None,
        "enter_fresh_cnt": 0,
        "stale_enter_cnt": 0,
        "reconciled_cnt": 0,
        "feed_warn_cnt": 0,
        "cl_src_health": None,
    }


def _parse_health_metrics(lines: list[str]) -> dict[str, Any]:
    """Extract v4.5 health percentiles, reconnect totals, clock offsets, and stale ENTER violations."""
    m_data = _empty_health_metrics()
    for ln in lines:
        if "RECONCILED_POSITION" in ln:
            m_data["reconciled_cnt"] += 1
        if "[FEED WARN]" in ln:
            m_data["feed_warn_cnt"] += 1
        if "LIVE 實盤成交" in ln or "[ENTRY " in ln:
            mf = _BOOK_FRESH_RE.search(ln)
            if mf:
                age_ms, lag_ms = float(mf.group(1)), float(mf.group(2))
                m_data["enter_fresh_cnt"] += 1
                if age_ms > 1500.0 or lag_ms > 1000.0:
                    m_data["stale_enter_cnt"] += 1
        mc = _CLOCK_RE.search(ln)
        if mc:
            m_data["clock_raw_ms"] = float(mc.group(1))
            m_data["clock_eff_ms"] = float(mc.group(2))
        mh = _HEALTH_RE.search(ln)
        if mh:
            reconn_pairs = [p.split("=") for p in mh.group(10).split(",") if "=" in p]
            m_data.update(
                {
                    "cl_src_health": mh.group(1),
                    "book_age_p50_ms": _opt_float(mh.group(2)),
                    "book_age_p99_ms": _opt_float(mh.group(3)),
                    "book_lag_p50_ms": _opt_float(mh.group(4)),
                    "book_lag_p99_ms": _opt_float(mh.group(5)),
                    "rollover_p99_ms": _opt_float(mh.group(6)),
                    "ws_book": int(mh.group(7)),
                    "ws_pc": int(mh.group(8)),
                    "rest_fallbacks": int(mh.group(9)),
                    "reconn_total": sum(int(kv[1]) for kv in reconn_pairs if kv[1].isdigit()),
                    "stale_skips": int(mh.group(11)),
                    "clock_raw_ms": float(mh.group(12)),
                    "clock_eff_ms": float(mh.group(13)),
                }
            )
    return m_data


def _audit_single_log(log_path: Path, lock_pid: int | None, active_pids: set[int]) -> dict[str, Any]:
    daemon_alive = bool(lock_pid is not None and lock_pid in active_pids)
    if not log_path.exists():
        return {
            "daemon_alive": daemon_alive,
            "pid": lock_pid,
            "traceback_cnt": 0,
            "tg_409_cnt": 0,
            "fak_miss_cnt": 0,
            "exit_miss_cnt": 0,
            "risk_block_cnt": 0,
            "latest_chainlink_src": None,
            "log_size_mb": 0.0,
            "health": _empty_health_metrics(),
            "slippage": {"fills_matched": 0, "mean_slippage_cents": None, "max_slippage_cents": None},
        }
    text = log_path.read_text(encoding="utf-8", errors="replace").replace("\r", "\n")
    lines = text.splitlines()
    lower_lines = [ln.lower() for ln in lines]
    cl_sources = _CHAINLINK_SRC_RE.findall(text)
    health = _parse_health_metrics(lines)
    latest_src = health["cl_src_health"] or (cl_sources[-1] if cl_sources else None)
    size_mb = round(log_path.stat().st_size / (1024.0 * 1024.0), 2)
    return {
        "daemon_alive": daemon_alive,
        "pid": lock_pid,
        "traceback_cnt": sum(1 for ln in lower_lines if "traceback" in ln),
        "tg_409_cnt": sum(1 for ln in lower_lines if ("409" in ln and "conflict" in ln) or "telegram" in ln and "409" in ln),
        "fak_miss_cnt": sum(1 for ln in lines if "LIVE FAK 未成交" in ln),
        "exit_miss_cnt": sum(1 for ln in lines if "LIVE 平倉未成交" in ln),
        "risk_block_cnt": sum(1 for ln in lines if "LIVE 熔斷攔截" in ln),
        "latest_chainlink_src": latest_src,
        "log_size_mb": size_mb,
        "health": health,
        "slippage": parse_execution_slippage(text),
    }


def _audit_coin_db(db_path: Path, cutoff_ts: float) -> tuple[dict[str, Any], list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    empty = {
        "windows": 0,
        "open_trades": 0,
        "all_time": asdict(BucketStats(0, 0, 0, None, 0.0)),
        "recent_window": asdict(BucketStats(0, 0, 0, None, 0.0)),
        "exit_reasons": {},
    }
    if not db_path.exists():
        return empty, [], []
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        windows = int(con.execute("SELECT COUNT(*) FROM windows").fetchone()[0])
        open_cnt = int(con.execute("SELECT COUNT(*) FROM paper_trades WHERE exit_ts IS NULL").fetchone()[0])
        closed_rows = con.execute(
            """
            SELECT id, entry_ts, entry_elapsed_sec, z_at_entry, exit_reason, net_pnl_usdc, won
            FROM paper_trades
            WHERE exit_ts IS NOT NULL AND net_pnl_usdc IS NOT NULL
            ORDER BY id ASC
            """
        ).fetchall()
    except sqlite3.Error:
        return empty, [], []
    finally:
        con.close()

    recent_rows = [r for r in closed_rows if float(r[1] or 0.0) >= cutoff_ts]
    reasons: dict[str, int] = {}
    for row in closed_rows:
        key = str(row[4] or "UNKNOWN")
        reasons[key] = reasons.get(key, 0) + 1
    summary = {
        "windows": windows,
        "open_trades": open_cnt,
        "all_time": asdict(_summarize_rows(closed_rows)),
        "recent_window": asdict(_summarize_rows(recent_rows)),
        "exit_reasons": reasons,
    }
    return summary, closed_rows, recent_rows


def _read_shared_risk(root: Path) -> dict[str, Any]:
    path = root / "shared_live_risk.json"
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _fetch_live_wallet_and_geo(root: Path) -> dict[str, Any]:
    info: dict[str, Any] = {"pusd_balance": None, "geo_country": None, "geo_blocked": None, "errors": []}
    btc_dir = root / "polymarket-btc-5m"
    if str(btc_dir) not in sys.path:
        sys.path.insert(0, str(btc_dir))
    try:
        from src.live_executor import PolymarketLiveExecutor

        executor = PolymarketLiveExecutor(env_path=btc_dir / ".env")
        info["pusd_balance"] = round(float(executor.get_pusd_balance()), 4)
    except Exception as exc:
        info["errors"].append(f"wallet: {exc}")
    try:
        resp = requests.get(GEOBLOCK_URL, timeout=5.0)
        if resp.status_code == 200:
            geo = resp.json()
            info["geo_country"] = geo.get("country")
            info["geo_blocked"] = geo.get("blocked")
        else:
            info["errors"].append(f"geoblock_http_{resp.status_code}")
    except requests.RequestException as exc:
        info["errors"].append(f"geoblock: {exc}")
    return info


def _build_optimization_watchlist(
    total_stats: BucketStats,
    elapsed_buckets: dict[str, BucketStats],
    per_coin_logs: dict[str, dict[str, Any]],
    slippage_summary: dict[str, Any],
    wallet_geo: dict[str, Any],
) -> list[str]:
    """Generate strict non-rewrite, evidence-gated optimization watchlist items."""
    notes: list[str] = []
    for coin, log_st in per_coin_logs.items():
        if log_st["traceback_cnt"] > 0 or log_st["tg_409_cnt"] > 0:
            notes.append(
                f"[INFRA] {coin.upper()} 偵測到錯誤事件 (traceback={log_st['traceback_cnt']}, tg_409={log_st['tg_409_cnt']})，建議優先排除程式例外或 TG 衝突。"
            )
        if log_st["exit_miss_cnt"] > 0:
            notes.append(
                f"[EXEC] {coin.upper()} 出現 {log_st['exit_miss_cnt']} 次平倉未成交，可檢查止損/止盈滑價容許幅。"
            )
        h_st = log_st.get("health") or {}
        if int(h_st.get("stale_enter_cnt") or 0) > 0:
            notes.append(
                f"[STALE] {coin.upper()} 偵測到 {h_st['stale_enter_cnt']} 次超標盤口 ENTER（>1.5s/1.0s），需立即複查。"
            )
    if wallet_geo.get("geo_blocked") is True:
        notes.append("[NET] Polymarket Geoblock 回報受限，請立即檢查 VPN 節點。")

    mean_slip = slippage_summary.get("mean_slippage_cents")
    fills = int(slippage_summary.get("fills_matched") or 0)
    total_misses = sum(int(v["fak_miss_cnt"]) for v in per_coin_logs.values())
    if fills >= 5 and mean_slip is not None and mean_slip > 1.5:
        notes.append(f"[SLIP] 實盤平均進場滑價 {mean_slip:+.2f}¢ > +1.5¢，建議微調 EV 限價滑點階梯（不做結構大改）。")
    if (fills + total_misses) >= 5 and total_misses / max(1, fills + total_misses) > 0.25:
        notes.append(f"[FILL] FAK 未成交率偏高 ({total_misses}/{fills + total_misses})，可觀察是否需檢視盤口深度過濾門檻。")

    if total_stats.trades < 20:
        notes.append(
            f"[OBSERVE ONLY] 目前總樣本 n={total_stats.trades} (< 20 筆最小調參門檻)，統計檢定力不足；維持現有策略基底不動，繼續收集數據。"
        )
        return notes

    early = elapsed_buckets["150-180s"]
    late = elapsed_buckets["180-255s"]
    if (
        early.trades >= 15
        and late.trades >= 15
        and early.win_rate_pct is not None
        and late.win_rate_pct is not None
        and early.win_rate_pct < 75.0
        and (late.win_rate_pct - early.win_rate_pct) >= 10.0
    ):
        notes.append(
            f"[TUNE CANDIDATE] 早段 150–180s 勝率 {early.win_rate_pct}% 明顯落後尾段 180–255s ({late.win_rate_pct}%)，可小幅將 `entry_start_elapsed_sec` 由 150s 微調至 170–180s。"
        )
    notes.append(
        f"[FEE ALIGNMENT] 實盤累計 n={total_stats.trades}，可評估將 `crypto_taker_fee_rate` 從保守 0.07 對齊至 11/20 新費率（BTC 0.05 / ETH,SOL 0.01）以提升真實 EV 準確度。"
    )
    return notes


def _build_buckets_and_slippage(
    all_rows: list[tuple[Any, ...]],
    per_coin_logs: dict[str, dict[str, Any]],
    all_slip_diffs: list[float],
) -> tuple[dict[str, BucketStats], dict[str, BucketStats], dict[str, Any]]:
    elapsed_buckets = {
        "150-180s": _summarize_rows([r for r in all_rows if 150.0 <= float(r[2] or 0.0) < 180.0]),
        "180-255s": _summarize_rows([r for r in all_rows if float(r[2] or 0.0) >= 180.0]),
    }
    z_buckets = {
        "1.30-1.50": _summarize_rows([r for r in all_rows if 1.30 <= float(r[3] or 0.0) <= 1.50]),
        ">1.50": _summarize_rows([r for r in all_rows if float(r[3] or 0.0) > 1.50]),
    }
    max_slip_vals = [
        float(v["slippage"]["max_slippage_cents"])
        for v in per_coin_logs.values()
        if v["slippage"]["max_slippage_cents"] is not None
    ]
    total_fills = sum(int(v["slippage"]["fills_matched"]) for v in per_coin_logs.values())
    total_misses = sum(int(v["fak_miss_cnt"]) for v in per_coin_logs.values())
    attempts = total_fills + total_misses
    slippage_summary = {
        "fills_matched": total_fills,
        "fak_misses": total_misses,
        "fill_rate_pct": round((total_fills / attempts) * 100.0, 1) if attempts else None,
        "mean_slippage_cents": round(sum(all_slip_diffs) / len(all_slip_diffs), 2) if all_slip_diffs else None,
        "max_slippage_cents": round(max(max_slip_vals), 2) if max_slip_vals else None,
    }
    return elapsed_buckets, z_buckets, slippage_summary


def run_daily_review(
    root: Path,
    hours: float = 24.0,
    no_live_api: bool = False,
) -> dict[str, Any]:
    """Collect read-only metrics from DBs, logs, process table, shared risk, and optional wallet API."""
    now_ts = datetime.now(tz=TZ_TAIPEI).timestamp()
    cutoff_ts = now_ts - max(1.0, float(hours)) * 3600.0
    active_pids = _get_live_monitor_pids()

    per_coin_db: dict[str, Any] = {}
    per_coin_logs: dict[str, Any] = {}
    all_rows: list[tuple[Any, ...]] = []
    recent_rows: list[tuple[Any, ...]] = []
    all_slip_diffs: list[float] = []

    for coin in COINS:
        c_dir = root / f"polymarket-{coin}-5m"
        db_path = c_dir / "data" / f"live_{coin}_5m.db"
        lock_pid = _read_lock_pid(Path(str(db_path) + ".lock"))
        db_summary, c_all, c_recent = _audit_coin_db(db_path, cutoff_ts)
        per_coin_db[coin] = db_summary
        all_rows.extend(c_all)
        recent_rows.extend(c_recent)

        log_st = _audit_single_log(root / "logs" / f"{coin}_live.log", lock_pid, active_pids)
        per_coin_logs[coin] = log_st
        mean_s = log_st["slippage"]["mean_slippage_cents"]
        matched_n = int(log_st["slippage"]["fills_matched"])
        if matched_n > 0 and mean_s is not None:
            all_slip_diffs.extend([float(mean_s)] * matched_n)

    total_all = _summarize_rows(all_rows)
    total_recent = _summarize_rows(recent_rows)
    elapsed_buckets, z_buckets, slippage_summary = _build_buckets_and_slippage(
        all_rows, per_coin_logs, all_slip_diffs
    )
    wallet_geo = (
        {"pusd_balance": None, "geo_country": None, "geo_blocked": None, "errors": []}
        if no_live_api
        else _fetch_live_wallet_and_geo(root)
    )
    shared_risk = _read_shared_risk(root)
    watchlist = _build_optimization_watchlist(total_all, elapsed_buckets, per_coin_logs, slippage_summary, wallet_geo)

    return {
        "generated_at_taipei": datetime.fromtimestamp(now_ts, tz=TZ_TAIPEI).isoformat(timespec="seconds"),
        "hours": hours,
        "total_all": asdict(total_all),
        "total_recent": asdict(total_recent),
        "per_coin_db": per_coin_db,
        "per_coin_logs": per_coin_logs,
        "elapsed_buckets": {k: asdict(v) for k, v in elapsed_buckets.items()},
        "z_buckets": {k: asdict(v) for k, v in z_buckets.items()},
        "slippage_summary": slippage_summary,
        "shared_risk": shared_risk,
        "paper_scouts": _collect_paper_scouts(root, cutoff_ts),
        "wallet_geo": wallet_geo,
        "watchlist": watchlist,
    }


def _collect_paper_scouts(root: Path, cutoff_ts: float = 0.0) -> list[dict[str, Any]]:
    """Collect read-only paper scout stats for XRP/DOGE when their SQLite DBs exist."""
    scouts: list[dict[str, Any]] = []
    for coin in SCOUT_COINS:
        db_path = root / f"polymarket-{coin}-5m" / "data" / f"live_{coin}_5m.db"
        if not db_path.exists():
            continue
        cdb, _, _ = _audit_coin_db(db_path, cutoff_ts)
        st = cdb["all_time"]
        scouts.append(
            {
                "coin": coin.upper(),
                "windows": cdb["windows"],
                "trades": st["trades"],
                "wins": st["wins"],
                "losses": st["losses"],
                "win_rate_pct": st["win_rate_pct"],
                "net_pnl_usdc": st["net_pnl_usdc"],
            }
        )
    return scouts


def _fmt_ms(val: float | None) -> str:
    return f"{val:.1f}ms" if val is not None else "--"


def _render_health_table_lines(report: dict[str, Any]) -> list[str]:
    """Render P0/P1 connection freshness, clock skew, and log size metrics per coin."""
    lines = [
        "## 3. 資料源品質、盤口新鮮度與時鐘健康表",
        "| 幣種 | book_age P50/P99 | book_lag P50/P99 | 換窗首單 P99 | REST 補抓 | 重連次數 | 時鐘殘差(原始) | 舊盤ENTER/對帳 | 日誌(MB) |",
        "|---|---|---|---|---:|---:|---|---|---:|",
    ]
    for coin in COINS:
        clg = report["per_coin_logs"][coin]
        h = clg.get("health") or _empty_health_metrics()
        age_s = f"{_fmt_ms(h.get('book_age_p50_ms'))}/{_fmt_ms(h.get('book_age_p99_ms'))}"
        lag_s = f"{_fmt_ms(h.get('book_lag_p50_ms'))}/{_fmt_ms(h.get('book_lag_p99_ms'))}"
        clk_s = (
            f"{h['clock_eff_ms']:+.1f} ({h['clock_raw_ms']:+.1f}ms)"
            if h.get("clock_eff_ms") is not None and h.get("clock_raw_ms") is not None
            else "--"
        )
        lines.append(
            f"| **{coin.upper()}** | `{age_s}` | `{lag_s}` | `{_fmt_ms(h.get('rollover_p99_ms'))}` | "
            f"{h.get('rest_fallbacks', 0)} | {h.get('reconn_total', 0)} | `{clk_s}` | "
            f"`{h.get('stale_enter_cnt', 0)}/{h.get('reconciled_cnt', 0)}` | {clg.get('log_size_mb', 0.0):.2f} |"
        )
    return lines


def render_markdown_report(report: dict[str, Any]) -> str:
    """Format daily audit dictionary into a clean Traditional Chinese Markdown report."""
    tot = report["total_all"]
    rec = report["total_recent"]
    wr = f"{tot['win_rate_pct']:.1f}%" if tot["win_rate_pct"] is not None else "--"
    rec_wr = f"{rec['win_rate_pct']:.1f}%" if rec["win_rate_pct"] is not None else "--"
    wg = report["wallet_geo"]
    bal_str = f"${wg['pusd_balance']:.2f} pUSD" if wg.get("pusd_balance") is not None else "(未連線)"
    geo_str = f"{wg.get('geo_country') or '--'} / blocked={wg.get('geo_blocked')}"
    sr = report.get("shared_risk") or {}
    slip = report["slippage_summary"]
    mean_slip = f"{slip['mean_slippage_cents']:+.2f}¢" if slip["mean_slippage_cents"] is not None else "--"
    fill_r = f"{slip['fill_rate_pct']:.1f}%" if slip["fill_rate_pct"] is not None else "--"

    lines = [
        f"# Polymarket 5m 三幣實盤每日健檢 ({report['generated_at_taipei']})",
        "",
        "## 1. 整體戰績與錢包風控",
        f"- **累計戰績**：`{tot['wins']}W - {tot['losses']}L`（共 `{tot['trades']}` 筆｜勝率 `{wr}`｜淨損益 `{tot['net_pnl_usdc']:+.2f} pUSD`）",
        f"- **近 {report['hours']:.0f} 小時**：`{rec['wins']}W - {rec['losses']}L`（共 `{rec['trades']}` 筆｜勝率 `{rec_wr}`｜淨損益 `{rec['net_pnl_usdc']:+.2f} pUSD`）",
        f"- **即時錢包 / VPN**：`{bal_str}` ｜ `{geo_str}`",
        f"- **三幣共用日熔斷**：日初基準 `${float(sr.get('day_start_equity') or 0.0):.2f}`｜今日累計 `{float(sr.get('daily_realized_pnl_usdc') or 0.0):+.2f} pUSD`｜上限 `-${float(sr.get('max_daily_loss_usdc') or 0.0):.2f}`｜觸發=`{bool(sr.get('circuit_broken', False))}`",
        "",
        "## 2. 各幣種進程、連線與損益明細",
        "| 幣種 | 進程 | 視窗數 | 累計勝敗 | 淨損益 (pUSD) | 平均滑價 | 錯誤/409 | 預言機來源 |",
        "|---|---|---:|---|---:|---:|---|---|",
    ]
    for coin in COINS:
        cdb = report["per_coin_db"][coin]
        clg = report["per_coin_logs"][coin]
        st = "ALIVE" if clg["daemon_alive"] else "STOPPED"
        c_all = cdb["all_time"]
        c_slip = clg["slippage"]["mean_slippage_cents"]
        c_slip_s = f"{c_slip:+.2f}¢" if c_slip is not None else "--"
        err_s = f"{clg['traceback_cnt']}/{clg['tg_409_cnt']}"
        lines.append(
            f"| **{coin.upper()}** | `{st}` (`{clg['pid'] or '--'}`) | {cdb['windows']} | "
            f"`{c_all['wins']}W-{c_all['losses']}L` | `{c_all['net_pnl_usdc']:+.2f}` | "
            f"`{c_slip_s}` | `{err_s}` | `{clg['latest_chainlink_src'] or '--'}` |"
        )

    lines.extend(["", *_render_health_table_lines(report)])
    lines.extend(["", "## 4. 進場時段、Z 值分桶與執行滑價"])
    lines.append(f"- **成交品質**：已配對 `{slip['fills_matched']}` 筆｜未成交 `{slip['fak_misses']}` 次｜成交率 `{fill_r}`｜平均滑價 `{mean_slip}`")
    for title, group in (("進場秒數分桶", report["elapsed_buckets"]), ("Z 值強度分桶", report["z_buckets"])):
        parts = []
        for b_name, b_st in group.items():
            b_wr = f"{b_st['win_rate_pct']:.1f}%" if b_st["win_rate_pct"] is not None else "--"
            parts.append(f"`{b_name}`: `{b_st['wins']}W-{b_st['losses']}L` ({b_wr}, `{b_st['net_pnl_usdc']:+.2f}`)")
        lines.append(f"- **{title}**：" + " ｜ ".join(parts))

    lines.extend(["", "## 5. 漸進微調觀察清單（維持原技術基底、不爆改）"])
    for item in report["watchlist"]:
        lines.append(f"- {item}")

    scouts = report.get("paper_scouts") or []
    if scouts:
        lines.extend(["", "## 6. Paper Scouts (`XRP` / `DOGE`)"])
        for sc in scouts:
            sc_wr = f"{sc['win_rate_pct']:.1f}%" if sc["win_rate_pct"] is not None else "--"
            lines.append(
                f"- **{sc['coin']} (Paper)**: `windows={sc['windows']}` ｜ "
                f"`{sc['wins']}W-{sc['losses']}L` ({sc_wr}) ｜ `{sc['net_pnl_usdc']:+.2f} USDC`"
            )
    return "\n".join(lines) + "\n"


def _send_telegram_push(root: Path, report: dict[str, Any]) -> bool:
    btc_dir = root / "polymarket-btc-5m"
    env = dotenv_values(btc_dir / ".env")
    token = (env.get("TG_BOT_TOKEN") or "").strip()
    if not token:
        return False

    recipients: list[str] = []
    primary = (env.get("TG_CHAT_ID") or "").strip()
    if primary:
        recipients.append(primary)
    ctrl_path = btc_dir / "data" / ".tg_control.json"
    if ctrl_path.exists():
        try:
            ctrl = json.loads(ctrl_path.read_text(encoding="utf-8"))
            for cid in [ctrl.get("chat_id"), *(ctrl.get("subscriber_ids") or [])]:
                s = str(cid or "").strip()
                if s and s not in recipients:
                    recipients.append(s)
        except (OSError, json.JSONDecodeError) as exc:
            sys.stderr.write(f"[daily_review warn] failed reading .tg_control.json: {exc}\n")

    if not recipients:
        return False
    tot = report["total_all"]
    wr = f"{tot['win_rate_pct']:.1f}%" if tot["win_rate_pct"] is not None else "--"
    bal = report["wallet_geo"].get("pusd_balance")
    bal_s = f"${bal:.2f} pUSD" if bal is not None else "N/A"
    msg = (
        "<b>[DAILY REVIEW // 每日 08:00 實盤健檢]</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"▸ <b>累計戰績</b>：<code>{tot['wins']}W - {tot['losses']}L</code> ({wr}) | <b>{tot['net_pnl_usdc']:+.2f} pUSD</b>\n"
        f"▸ <b>錢包餘額</b>：<code>{bal_s}</code> | <b>{report['watchlist'][0][:90]}</b>"
    )
    url = TG_SEND_URL.format(token=token)
    ok_any = False
    for cid in recipients:
        try:
            resp = requests.post(
                url,
                json={"chat_id": cid, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True},
                timeout=6.0,
            )
            if resp.status_code == 200:
                ok_any = True
        except requests.RequestException:
            continue
    return ok_any


def main() -> int:
    _apply_low_priority()
    default_root = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description="Daily non-intrusive live review for BTC/ETH/SOL 5m bots.")
    parser.add_argument("--root", type=Path, default=default_root, help="Root folder containing live suite")
    parser.add_argument("--hours", type=float, default=24.0, help="Lookback window in hours")
    parser.add_argument("--out", type=Path, default=None, help="Output Markdown report path")
    parser.add_argument("--no-live-api", action="store_true", help="Skip live wallet and geoblock queries")
    parser.add_argument("--send-tg", action="store_true", help="Push short summary to Telegram (no polling)")
    parser.add_argument("--json", action="store_true", help="Print JSON summary to stdout")
    args = parser.parse_args()

    report = run_daily_review(root=args.root, hours=args.hours, no_live_api=args.no_live_api)
    date_tag = datetime.now(tz=TZ_TAIPEI).strftime("%Y-%m-%d")
    out_path = args.out or (args.root / "reports" / f"daily_review_{date_tag}.md")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    md = render_markdown_report(report)
    out_path.write_text(md, encoding="utf-8")

    if args.send_tg:
        _send_telegram_push(args.root, report)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(md)
        print(f"Saved report -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
