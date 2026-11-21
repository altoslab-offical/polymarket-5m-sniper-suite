"""
Telegram Remote Monitor & Control Bot for Polymarket BTC 5m Sniper.

Zero extra third-party dependencies (uses `requests` + `asyncio.run_in_executor`):
1. Runs seamlessly EITHER embedded inside `run_monitor.py` OR standalone via `python tg_bot.py`.
2. Auto-binds the owner's `chat_id` on first `/start` and persists shared control state
   (`chat_id`, `paused`, `bet_usdc`, `bet_updated_at`) in `data/.tg_control.json`.
3. Provides persistent bottom reply-keyboard buttons for mobile monitoring & control:
   - `/status` (`▸ 即時盤況`)
   - `/report` (`▸ 戰績報表`)
   - `/trades` (`▸ 最近10筆`)
   - `/balance` (`▸ 帳戶餘額`)
   - `/pause` / `/resume` (`■ 暫停狙擊` / `▶ 恢復狙擊`)
   - `/bet <金額>` (遠端調整單筆下注金額)
"""
import asyncio
from datetime import datetime
import html
import json
import os
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from dotenv import dotenv_values, load_dotenv
import requests


TG_API_BASE = "https://api.telegram.org/bot{token}/{method}"
POLYGON_RPC_URL = "https://polygon-bor-rpc.publicnode.com"
PUSD_CONTRACT = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"

REPLY_KEYBOARD: Dict[str, Any] = {
    "keyboard": [
        [{"text": "▸ 即時盤況"}, {"text": "▸ 戰績報表"}],
        [{"text": "▸ 最近10筆"}, {"text": "▸ 帳戶餘額"}],
        [{"text": "■ 暫停狙擊"}, {"text": "▶ 恢復狙擊"}],
    ],
    "resize_keyboard": True,
    "is_persistent": True,
}


def load_dotenv_file(env_path: Optional[Path] = None) -> Path:
    """Load project `.env` if present without overriding explicit process environment variables."""
    resolved = env_path or (Path(__file__).resolve().parent.parent / ".env")
    if resolved.exists():
        load_dotenv(dotenv_path=resolved, override=False)
    return resolved


class TelegramControlState:
    """File-backed shared state between `run_monitor.py` and standalone `tg_bot.py`."""

    def __init__(self, state_path: Optional[Path] = None):
        self.path = state_path or (Path(__file__).resolve().parent.parent / "data" / ".tg_control.json")

    def load(self) -> Dict[str, Any]:
        default = {
            "chat_id": None,
            "subscriber_ids": [],
            "paused": False,
            "bet_usdc": None,
            "bet_updated_at": 0.0,
            "last_notified_entry_id": 0,
            "last_notified_exit_id": 0,
            "embedded_bot_heartbeat_ts": 0.0,
        }
        if not self.path.exists():
            return default
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                return default
            raw_subs = raw.get("subscriber_ids")
            subs: List[str] = []
            if isinstance(raw_subs, list):
                for x in raw_subs:
                    sx = str(x).strip()
                    if sx and sx not in subs:
                        subs.append(sx)
            return {
                "chat_id": str(raw["chat_id"]) if raw.get("chat_id") else None,
                "subscriber_ids": subs,
                "paused": bool(raw.get("paused", False)),
                "bet_usdc": float(raw["bet_usdc"]) if raw.get("bet_usdc") is not None else None,
                "bet_updated_at": float(raw.get("bet_updated_at") or 0.0),
                "last_notified_entry_id": int(raw.get("last_notified_entry_id") or 0),
                "last_notified_exit_id": int(raw.get("last_notified_exit_id") or 0),
                "embedded_bot_heartbeat_ts": float(raw.get("embedded_bot_heartbeat_ts") or 0.0),
            }
        except Exception:
            return default

    def update(self, **kwargs: Any) -> Dict[str, Any]:
        state = self.load()
        for k, v in kwargs.items():
            if k in state:
                state[k] = v
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
            tmp.replace(self.path)
        except Exception:
            pass
        return state

    def add_subscriber(self, cid: str) -> bool:
        cid = str(cid).strip()
        if not cid:
            return False
        state = self.load()
        if cid == state.get("chat_id"):
            return False
        subs = list(state.get("subscriber_ids") or [])
        if cid in subs:
            return False
        subs.append(cid)
        self.update(subscriber_ids=subs)
        return True

    def remove_subscriber(self, cid: str) -> bool:
        cid = str(cid).strip()
        state = self.load()
        subs = list(state.get("subscriber_ids") or [])
        if cid not in subs:
            return False
        subs = [x for x in subs if x != cid]
        self.update(subscriber_ids=subs)
        return True

    def claim_entry_notification(self, trade_id: int) -> bool:
        tid = int(trade_id or 0)
        if tid <= 0:
            return True
        state = self.load()
        if tid <= int(state.get("last_notified_entry_id") or 0):
            return False
        self.update(last_notified_entry_id=tid)
        return True

    def claim_exit_notification(self, trade_id: int) -> bool:
        tid = int(trade_id or 0)
        if tid <= 0:
            return True
        state = self.load()
        if tid <= int(state.get("last_notified_exit_id") or 0):
            return False
        self.update(last_notified_exit_id=tid)
        return True


def _connect_readonly(db_path: str) -> Optional[sqlite3.Connection]:
    if not db_path or not os.path.exists(db_path):
        return None
    return sqlite3.connect(db_path, timeout=3.0)


def _fmt_window_range(window_ts: int) -> str:
    t0 = datetime.fromtimestamp(window_ts).strftime("%H:%M")
    t1 = datetime.fromtimestamp(window_ts + 300).strftime("%H:%M")
    return f"{t0}~{t1}"


def _infer_asset_symbol(db_path: str) -> str:
    low = os.path.basename(db_path or "").lower()
    if "eth" in low:
        return "ETH"
    if "sol" in low:
        return "SOL"
    return "BTC"


def _discover_sibling_dbs(db_path: str) -> List[Tuple[str, str]]:
    """Find sibling BTC/ETH/SOL live SQLite DBs under the same scratch parent folder."""
    if not db_path:
        return []
    try:
        cur_abs = Path(db_path).resolve()
        scratch_dir = cur_abs.parent.parent.parent
    except Exception:
        return []
    candidates = [
        ("BTC", scratch_dir / "polymarket-btc-5m" / "data" / "live_btc_5m.db"),
        ("ETH", scratch_dir / "polymarket-eth-5m" / "data" / "live_eth_5m.db"),
        ("SOL", scratch_dir / "polymarket-sol-5m" / "data" / "live_sol_5m.db"),
    ]
    found: List[Tuple[str, str]] = []
    for sym, path in candidates:
        try:
            resolved = path.resolve()
            if resolved != cur_abs and resolved.exists():
                found.append((sym, str(resolved)))
        except Exception:
            continue
    return found


def _build_sibling_status_lines(db_path: str) -> List[str]:
    siblings = _discover_sibling_dbs(db_path)
    if not siblings:
        return []
    out: List[str] = []
    for sym, sdb in siblings:
        conn = _connect_readonly(sdb)
        if conn is None:
            continue
        try:
            snap = conn.execute(
                "SELECT ts, chainlink_price, z_score FROM snapshots ORDER BY id DESC LIMIT 1"
            ).fetchone()
            open_row = conn.execute(
                "SELECT id, side, entry_price FROM paper_trades WHERE exit_reason IS NULL ORDER BY id DESC LIMIT 1"
            ).fetchone()
        except Exception:
            snap, open_row = None, None
        finally:
            conn.close()
        if not snap:
            continue
        age = max(0.0, time.time() - float(snap[0]))
        px_fmt = f"${float(snap[1]):,.4f}" if (sym == "SOL" and snap[1] is not None) else (
            f"${float(snap[1]):,.2f}" if snap[1] is not None else "--"
        )
        z_fmt = f"{float(snap[2]):+.2f}" if snap[2] is not None else "--"
        pos_txt = f"持倉 {open_row[1]} @${float(open_row[2]):.2f}" if open_row else "FLAT"
        stale_txt = f" [離線 {int(age)}s]" if age > 15.0 else ""
        out.append(f"▪ <b>{sym}</b>：<code>{px_fmt}</code> | Z <code>{z_fmt}</code> | {pos_txt}{stale_txt}")
    if not out:
        return []
    return ["━━━━━━━━━━━━━━━━━━", "<b>[MULTI-COIN // 其他幣種即時狀態]</b>"] + out


def _discover_portfolio_dbs(db_path: str) -> List[Tuple[str, str]]:
    """Return all existing BTC/ETH/SOL live DBs (including `db_path` itself) in BTC -> ETH -> SOL order."""
    if not db_path:
        return []
    primary_sym = _infer_asset_symbol(db_path)
    siblings = _discover_sibling_dbs(db_path)
    if not siblings:
        return [(primary_sym, db_path)]
    by_sym: Dict[str, str] = {primary_sym: db_path}
    for sym, path in siblings:
        by_sym[sym] = path
    order = ["BTC", "ETH", "SOL"]
    out: List[Tuple[str, str]] = [(s, by_sym[s]) for s in order if s in by_sym]
    for s, p in by_sym.items():
        if s not in order:
            out.append((s, p))
    return out


def _load_portfolio_summary(db_path: str) -> Dict[str, Any]:
    """Aggregate closed/open trades and today's PnL across all discovered portfolio DBs."""
    dbs = _discover_portfolio_dbs(db_path)
    today = datetime.now().date()
    per_coin: List[Dict[str, Any]] = []
    all_closed: List[Tuple[str, int, int, str, float, float, float, str, float, int]] = []
    tot_w = tot_l = today_w = today_l = tot_open = tot_windows = 0
    tot_pnl = today_pnl = 0.0

    for sym, path in dbs:
        conn = _connect_readonly(path)
        if conn is None:
            continue
        try:
            closed = conn.execute(
                """
                SELECT id, window_ts, side, entry_ts, entry_price, entry_fee_per_share,
                       exit_reason, net_pnl_usdc, won
                FROM paper_trades
                WHERE exit_reason IS NOT NULL
                ORDER BY id ASC
                """
            ).fetchall()
            open_cnt = int(conn.execute("SELECT COUNT(*) FROM paper_trades WHERE exit_reason IS NULL").fetchone()[0] or 0)
            win_cnt = int(conn.execute("SELECT COUNT(*) FROM windows").fetchone()[0] or 0)
        except Exception:
            closed, open_cnt, win_cnt = [], 0, 0
        finally:
            conn.close()

        n = len(closed)
        w = sum(1 for r in closed if r[8] == 1)
        l = n - w
        pnl = sum(float(r[7] or 0.0) for r in closed)
        td_rows = [r for r in closed if datetime.fromtimestamp(float(r[3])).date() == today]
        td_w = sum(1 for r in td_rows if r[8] == 1)
        td_l = len(td_rows) - td_w
        td_pnl = sum(float(r[7] or 0.0) for r in td_rows)

        per_coin.append(
            {
                "sym": sym,
                "wins": w,
                "losses": l,
                "pnl": pnl,
                "today_wins": td_w,
                "today_losses": td_l,
                "today_pnl": td_pnl,
                "open_cnt": open_cnt,
                "win_cnt": win_cnt,
            }
        )
        for r in closed:
            all_closed.append((sym, *r))

        tot_w += w
        tot_l += l
        tot_pnl += pnl
        today_w += td_w
        today_l += td_l
        today_pnl += td_pnl
        tot_open += open_cnt
        tot_windows += win_cnt

    all_closed.sort(key=lambda x: float(x[4]))
    return {
        "multi": len(dbs) > 1,
        "per_coin": per_coin,
        "all_closed": all_closed,
        "wins": tot_w,
        "losses": tot_l,
        "total_pnl": tot_pnl,
        "today_wins": today_w,
        "today_losses": today_l,
        "today_pnl": today_pnl,
        "open_cnt": tot_open,
        "total_windows": tot_windows,
    }


def _build_sibling_report_lines(
    db_path: str,
    primary_sym: str,
    primary_pnl: float,
    primary_today_pnl: float,
    unit: str,
) -> List[str]:
    siblings = _discover_sibling_dbs(db_path)
    if not siblings:
        return []
    today = datetime.now().date()
    sub_lines: List[str] = []
    comb_total = primary_pnl
    comb_today = primary_today_pnl
    for sym, sdb in siblings:
        conn = _connect_readonly(sdb)
        if conn is None:
            continue
        try:
            trades = conn.execute(
                "SELECT entry_ts, net_pnl_usdc, won FROM paper_trades WHERE exit_reason IS NOT NULL ORDER BY id ASC"
            ).fetchall()
            open_cnt = conn.execute("SELECT COUNT(*) FROM paper_trades WHERE exit_reason IS NULL").fetchone()[0]
            win_cnt = conn.execute("SELECT COUNT(*) FROM windows").fetchone()[0]
        except Exception:
            trades, open_cnt, win_cnt = [], 0, 0
        finally:
            conn.close()
        if not trades and win_cnt == 0:
            continue
        n = len(trades)
        w = sum(1 for t in trades if t[2] == 1)
        tot = sum(float(t[1] or 0.0) for t in trades)
        td_pnl = sum(float(t[1] or 0.0) for t in trades if datetime.fromtimestamp(float(t[0])).date() == today)
        comb_total += tot
        comb_today += td_pnl
        open_tag = f" (持倉 {open_cnt})" if open_cnt else ""
        if n > 0:
            sub_lines.append(
                f"▪ <b>{sym}</b>：<b>{w}W-{n-w}L</b> ({w/n*100:.0f}%) | 累計 <b>{tot:+.2f}</b> | 今日 <b>{td_pnl:+.2f}</b>{open_tag}"
            )
        else:
            sub_lines.append(f"▪ <b>{sym}</b>：尚無已結算單 ({win_cnt} 盤){open_tag}")
    if not sub_lines:
        return []
    return (
        ["━━━━━━━━━━━━━━━━━━", "<b>[PORTFOLIO // 三幣同步戰績]</b>"]
        + sub_lines
        + [f"▸ <b>全倉合計</b>：累計 <b>{comb_total:+.2f} {unit}</b> | 今日 <b>{comb_today:+.2f} {unit}</b>"]
    )


def build_status_message(
    db_path: str,
    live_mode: bool = True,
    bet_usdc: float = 25.0,
    paused: bool = False,
) -> str:
    """Format current market snapshot, oracle health, and open position for `/status`."""
    conn = _connect_readonly(db_path)
    if conn is None:
        return f"[ERROR] 找不到資料庫：<code>{html.escape(db_path)}</code>"

    try:
        cur = conn.cursor()
        snap = cur.execute(
            """
            SELECT ts, window_ts, elapsed_sec, chainlink_price, chainlink_source,
                   cex_lead_price, oracle_gap_bps, z_score, p_true_up,
                   up_ask, down_ask, cvd_imb, vol_1s
            FROM snapshots
            ORDER BY id DESC LIMIT 1
            """
        ).fetchone()
        if not snap:
            return "[WAIT] 尚無即時盤口快照資料，請稍候再查。"

        (
            ts,
            window_ts,
            elapsed_sec,
            cl_price,
            cl_src,
            cex_price,
            gap_bps,
            z_score,
            p_true_up,
            up_ask,
            down_ask,
            cvd_imb,
            vol_1s,
        ) = snap

        win_row = cur.execute(
            "SELECT strike_price, strike_source FROM windows WHERE window_ts = ?",
            (window_ts,),
        ).fetchone()
        open_row = cur.execute(
            """
            SELECT id, window_ts, side, shares, entry_price, entry_fee_per_share, entry_elapsed_sec
            FROM paper_trades
            WHERE exit_reason IS NULL
            ORDER BY id DESC LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    age_sec = max(0.0, time.time() - float(ts))
    rem_sec = max(0, 300 - int(round(float(elapsed_sec or 0.0))))
    zone = "SNIPE 狙擊區" if 150.0 <= float(elapsed_sec or 0.0) <= 255.0 else "WAIT 觀察區"
    mode_tag = "LIVE 實盤" if live_mode else "PAPER 模擬"
    status_flag = "PAUSED 暫停中" if paused else "ACTIVE 狙擊待命"

    strike = win_row[0] if win_row else None
    strike_src = win_row[1] if win_row else "?"

    lines: List[str] = [
        "<b>[TERMINAL // BTC-5M 即時盤況]</b>",
        "━━━━━━━━━━━━━━━━━━",
        f"▸ <b>系統</b>：{mode_tag}  |  {status_flag}  |  單注 <b>${bet_usdc:.1f}</b>",
        f"▸ <b>盤口</b>：<code>{_fmt_window_range(int(window_ts))}</code>  |  <b>{zone}</b> (剩 <b>{rem_sec}s</b>)",
    ]

    if age_sec > 12.0:
        lines.append(f"▸ <b>警告</b>：<b>監控程式未更新</b> (最後紀錄於 <b>{int(age_sec)}s 前</b>)")

    if cl_price is not None and strike is not None:
        diff = float(cl_price) - float(strike)
        lines.append(f"▸ <b>基準</b>：Strike <code>${float(strike):,.2f}</code> (<code>{html.escape(str(strike_src))}</code>)")
        lines.append(f"▸ <b>現價</b>：Chainlink <code>${float(cl_price):,.2f}</code> (價差 <b>{diff:+.1f}</b>)")
    elif cl_price is not None:
        lines.append(f"▸ <b>現價</b>：Chainlink <code>${float(cl_price):,.2f}</code> (等待鎖定 Strike)")

    if cl_src != "POLY_RTDS_CHAINLINK":
        lines.append(f"▸ <b>預言機</b>：<code>{html.escape(str(cl_src))}</code> [非RTDS 鎖定保護]")
    else:
        lines.append(f"▸ <b>預言機</b>：<code>{html.escape(str(cl_src))}</code> (延遲 {age_sec:.1f}s)")

    if p_true_up is not None and z_score is not None:
        p_dn = 1.0 - float(p_true_up)
        ua_s = f"${float(up_ask):.2f}" if up_ask is not None else "--"
        da_s = f"${float(down_ask):.2f}" if down_ask is not None else "--"
        gap_s = f"{float(gap_bps):+.1f}bp" if gap_bps is not None else "--"
        cvd_s = f"{float(cvd_imb):+.2f}" if cvd_imb is not None else "--"
        vol_s = f"{float(vol_1s) * 1e4:.2f}e-4" if vol_1s is not None else "--"
        lines.append(
            f"▸ <b>模型</b>：Z <code>{float(z_score):+.2f}</code>  |  UP <b>{float(p_true_up)*100:.1f}%</b> / DN <b>{p_dn*100:.1f}%</b>"
        )
        lines.append(f"▸ <b>報價</b>：Ask UP <code>{ua_s}</code>  /  DN <code>{da_s}</code>")
        lines.append(f"▸ <b>動能</b>：CVD <code>{cvd_s}</code>  |  Vol <code>{vol_s}</code>  |  Gap <code>{gap_s}</code>")

    lines.append("━━━━━━━━━━━━━━━━━━")
    if open_row:
        tid, w_ts, side, shares, epx, efee, eel = open_row
        cost = float(epx) + float(efee)
        lines.append(
            f"▸ <b>持倉中 #{tid}</b>：{side} <code>{float(shares):.2f} sh</code> @ <code>${float(epx):.3f}</code> (含費 ${cost:.4f}, {float(eel):.0f}s進)"
        )
    else:
        lines.append("▸ <b>持倉狀態</b>：FLAT (空手待命)")

    lines.extend(_build_sibling_status_lines(db_path))
    return "\n".join(lines)


def build_report_message(db_path: str) -> str:
    """Format win rate, cumulative PnL, today's PnL, streak, and recent trades for `/report` (multi-coin consolidated)."""
    is_live = "live" in os.path.basename(db_path).lower()
    unit = "pUSD" if is_live else "USDC"
    mode_title = "實盤交易戰績" if is_live else "模擬盤戰績"

    pf = _load_portfolio_summary(db_path)
    if not pf["per_coin"]:
        return f"[ERROR] 找不到資料庫：<code>{html.escape(db_path)}</code>"

    all_closed = pf["all_closed"]
    if not all_closed:
        return (
            f"<b>[PERFORMANCE // {mode_title}]</b>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"▸ <b>已掃描盤數</b>：{pf['total_windows']} 盤\n"
            f"▸ <b>已結算注單</b>：尚無（持倉中：{pf['open_cnt']} 筆）"
        )

    wins = pf["wins"]
    losses = pf["losses"]
    n = wins + losses
    wr = (wins / n) * 100.0 if n > 0 else 0.0
    total_pnl = pf["total_pnl"]
    today_wins = pf["today_wins"]
    today_losses = pf["today_losses"]
    today_pnl = pf["today_pnl"]

    streak_dir = all_closed[-1][9]
    streak_cnt = 0
    for t in reversed(all_closed):
        if t[9] == streak_dir:
            streak_cnt += 1
        else:
            break
    streak_str = f"{streak_cnt} 連勝" if streak_dir == 1 else f"{streak_cnt} 連敗"

    if pf["multi"]:
        lines = [
            f"<b>[PORTFOLIO // 三幣統整{mode_title}]</b>",
            "━━━━━━━━━━━━━━━━━━",
            f"▸ <b>全倉戰績</b>：<b>{wins} 勝 {losses} 敗</b> (勝率 <b>{wr:.1f}%</b>)  |  <b>{streak_str}</b>",
            f"▸ <b>全倉淨損益</b>：<b>{total_pnl:+.2f} {unit}</b>",
            f"▸ <b>今日全倉</b>：<b>{today_wins}W - {today_losses}L</b> (<b>{today_pnl:+.2f} {unit}</b>)  |  持倉：<b>{pf['open_cnt']}</b> 筆",
            "━━━━━━━━━━━━━━━━━━",
            "<b>[BREAKDOWN // 各幣種明細]</b>",
        ]
        for c in pf["per_coin"]:
            cn = c["wins"] + c["losses"]
            open_tag = f" (持倉 {c['open_cnt']})" if c["open_cnt"] else ""
            if cn > 0:
                cwr = (c["wins"] / cn) * 100.0
                lines.append(
                    f"▪ <b>{c['sym']}</b>：<b>{c['wins']}W-{c['losses']}L</b> ({cwr:.1f}%) | 累計 <b>{c['pnl']:+.2f}</b> | 今日 <b>{c['today_pnl']:+.2f}</b>{open_tag}"
                )
            else:
                lines.append(f"▪ <b>{c['sym']}</b>：尚無已結算單 ({c['win_cnt']} 盤){open_tag}")

        lines.extend(["━━━━━━━━━━━━━━━━━━", "<b>[RECENT // 全倉最近 5 筆結算]</b>"])
        for sym, tid, wts, side, ets, epx, efee, reason, pnl, won in all_closed[-5:]:
            dt_s = datetime.fromtimestamp(float(ets)).strftime("%m-%d %H:%M")
            tag = "WIN " if won == 1 else "LOSS"
            lines.append(
                f"▪ <b>{tag}</b> [{sym}] #{tid} <code>{dt_s}</code> {side:<4} @<code>${float(epx):.3f}</code> ➔ <b>{float(pnl):+.2f} U</b>"
            )
        return "\n".join(lines)

    lines = [
        f"<b>[PERFORMANCE // {mode_title}]</b>",
        "━━━━━━━━━━━━━━━━━━",
        f"▸ <b>總戰績</b>：<b>{wins} 勝 {losses} 敗</b> (勝率 <b>{wr:.2f}%</b>)  |  <b>{streak_str}</b>",
        f"▸ <b>累積淨損益</b>：<b>{total_pnl:+.2f} {unit}</b>",
        f"▸ <b>今日戰績</b>：<b>{today_wins}W - {today_losses}L</b> (<b>{today_pnl:+.2f} {unit}</b>)",
        f"▸ <b>已掃描盤數</b>：{pf['total_windows']} 盤 (持倉中：{pf['open_cnt']} 筆)",
        "━━━━━━━━━━━━━━━━━━",
        "<b>[RECENT // 最近 5 筆結算]</b>",
    ]
    for sym, tid, wts, side, ets, epx, efee, reason, pnl, won in all_closed[-5:]:
        dt_s = datetime.fromtimestamp(float(ets)).strftime("%m-%d %H:%M")
        tag = "WIN " if won == 1 else "LOSS"
        lines.append(
            f"▪ <b>{tag}</b> #{tid} <code>{dt_s}</code> {side:<4} @<code>${float(epx):.3f}</code> ➔ <b>{float(pnl):+.2f} U</b>"
        )
    return "\n".join(lines)


def build_trades_message(db_path: str, limit: int = 10) -> str:
    """Format the latest `limit` trades across all discovered portfolio DBs for `/trades`."""
    dbs = _discover_portfolio_dbs(db_path)
    multi = len(dbs) > 1
    merged: List[Tuple[str, int, float, str, float, float, Optional[str], Optional[float], Optional[int]]] = []
    any_db = False

    for sym, path in dbs:
        conn = _connect_readonly(path)
        if conn is None:
            continue
        any_db = True
        try:
            rows = conn.execute(
                """
                SELECT id, entry_ts, side, shares, entry_price, exit_reason, net_pnl_usdc, won
                FROM paper_trades
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            for r in rows:
                merged.append((sym, *r))
        except Exception:
            pass
        finally:
            conn.close()

    if not any_db:
        return f"[ERROR] 找不到資料庫：<code>{html.escape(db_path)}</code>"

    if not merged:
        return "[LEDGER] 目前尚無任何成交紀錄。"

    merged.sort(key=lambda x: float(x[2]), reverse=True)
    merged = merged[:limit]

    title = f"三幣最近 {len(merged)} 筆成交明細" if multi else f"最近 {len(merged)} 筆成交明細"
    lines = [
        f"<b>[LEDGER // {title}]</b>",
        "━━━━━━━━━━━━━━━━━━",
    ]
    for sym, tid, ets, side, shares, epx, reason, pnl, won in merged:
        dt_s = datetime.fromtimestamp(float(ets)).strftime("%m-%d %H:%M:%S")
        sym_prefix = f"[{sym}] " if multi else ""
        if reason is None:
            lines.append(
                f"▪ <b>OPEN</b> {sym_prefix}#{tid} <code>{dt_s}</code> {side} <code>{float(shares):.2f}sh</code> @<code>${float(epx):.3f}</code> | <b>持倉中</b>"
            )
        else:
            tag = "WIN " if won == 1 else "LOSS"
            short_reason = {
                "EARLY_TAKE_PROFIT": "止盈",
                "STOP_LOSS_REVERSAL": "止損",
                "EMERGENCY_STOP_LOSS": "急殺止損",
            }.get(str(reason), str(reason).replace("SETTLED_", "結算"))
            lines.append(
                f"▪ <b>{tag}</b> {sym_prefix}#{tid} <code>{dt_s}</code> {side:<4} @<code>${float(epx):.3f}</code> ({short_reason}) ➔ <b>{float(pnl or 0.0):+.2f} U</b>"
            )
    return "\n".join(lines)


def _fetch_onchain_pusd_balance(wallet_addr: str) -> Optional[float]:
    if not wallet_addr or not wallet_addr.startswith("0x") or len(wallet_addr) != 42:
        return None
    data = "0x70a08231" + wallet_addr[2:].lower().rjust(64, "0")
    try:
        r = requests.post(
            POLYGON_RPC_URL,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_call",
                "params": [{"to": PUSD_CONTRACT, "data": data}, "latest"],
            },
            timeout=4.0,
        )
        res = r.json().get("result")
        if res and str(res).startswith("0x"):
            return int(res, 16) / 1e6
    except Exception:
        pass
    return None


class TelegramBotService:
    """Async/Executor Telegram notification + interactive remote command service."""

    def __init__(
        self,
        db_path: str,
        live_mode: bool = False,
        live_exec: Optional[Any] = None,
        default_bet_usdc: float = 25.0,
        env_path: Optional[Path] = None,
        control_state: Optional[TelegramControlState] = None,
        asset_symbol: str = "",
        shared_control_path: Optional[Path] = None,
    ) -> None:
        self.resolved_env = load_dotenv_file(env_path)
        self.db_path = db_path
        self.asset_symbol = (asset_symbol or _infer_asset_symbol(db_path)).upper()
        self.live_mode = live_mode
        self.live_exec = live_exec
        self.default_bet_usdc = float(default_bet_usdc)
        self.bet_usdc = float(default_bet_usdc)

        if control_state is not None:
            self.control = control_state
        else:
            state_file = Path(os.path.dirname(os.path.abspath(db_path))) / ".tg_control.json"
            self.control = TelegramControlState(state_file)

        self.shared_control: Optional[TelegramControlState] = (
            TelegramControlState(shared_control_path) if shared_control_path is not None else None
        )

        self.bot_token = (os.getenv("TG_BOT_TOKEN") or "").strip()
        if not self.bot_token and shared_control_path is not None:
            shared_env = shared_control_path.parent.parent / ".env"
            if shared_env.exists():
                try:
                    self.bot_token = (dotenv_values(shared_env).get("TG_BOT_TOKEN") or "").strip()
                except Exception:
                    pass

        env_chat = (os.getenv("TG_CHAT_ID") or "").strip()
        ctrl = self.control.load()
        shared_chat = self.shared_control.load().get("chat_id") if self.shared_control is not None else None
        self.chat_id: Optional[str] = env_chat or shared_chat or ctrl.get("chat_id")
        if env_chat and ctrl.get("chat_id") != env_chat:
            self.control.update(chat_id=env_chat)

        self._send_http = requests.Session()
        self._poll_http = requests.Session()
        self._send_lock = threading.Lock()
        self._stop_event = asyncio.Event()
        self._update_offset: int = 0
        self._bot_start_ts: float = time.time()

    @property
    def enabled(self) -> bool:
        return bool(self.bot_token)

    def stop(self) -> None:
        self._stop_event.set()

    def get_control_state(self) -> Dict[str, Any]:
        state = self.control.load()
        if self.shared_control is not None:
            shared = self.shared_control.load()
            state["paused"] = bool(state.get("paused", False) or shared.get("paused", False))
        return state

    def clear_stale_bet_override(self) -> None:
        """Reset old `.tg_control.json` bet override when monitor starts with explicit CLI `--bet`."""
        self.control.update(bet_usdc=None, bet_updated_at=time.time())

    def send_message_sync(
        self,
        text: str,
        chat_id: Optional[str] = None,
        with_keyboard: bool = True,
    ) -> bool:
        target = chat_id or self.chat_id
        if not self.bot_token or not target:
            return False

        payload: Dict[str, Any] = {
            "chat_id": target,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if with_keyboard:
            payload["reply_markup"] = REPLY_KEYBOARD

        url = TG_API_BASE.format(token=self.bot_token, method="sendMessage")
        with self._send_lock:
            try:
                r = self._send_http.post(url, json=payload, timeout=6.0)
                if r.status_code == 200:
                    return True
                # Fallback to plain text if HTML entity parsing failed
                payload.pop("parse_mode", None)
                r2 = self._send_http.post(url, json=payload, timeout=6.0)
                return r2.status_code == 200
            except Exception:
                return False

    def get_recipient_ids(self) -> List[str]:
        ctrl = self.shared_control.load() if self.shared_control is not None else self.control.load()
        ids: List[str] = []
        primary = ctrl.get("chat_id") or self.chat_id
        if primary:
            ids.append(str(primary))
        for sub in ctrl.get("subscriber_ids") or []:
            s = str(sub).strip()
            if s and s not in ids:
                ids.append(s)
        return ids

    def broadcast_message_sync(self, text: str, with_keyboard: bool = True) -> bool:
        targets = self.get_recipient_ids()
        if not self.bot_token or not targets:
            return False
        ok_any = False
        for tid in targets:
            if self.send_message_sync(text, chat_id=tid, with_keyboard=with_keyboard):
                ok_any = True
        return ok_any

    def send_async(self, text: str) -> None:
        if not self.enabled or not self.get_recipient_ids():
            return
        try:
            loop = asyncio.get_running_loop()
            loop.run_in_executor(None, self.broadcast_message_sync, text, True)
        except RuntimeError:
            self.broadcast_message_sync(text, True)

    def notify_entry(
        self,
        trade_id: int,
        side: str,
        window_ts: int,
        elapsed_sec: float,
        shares: float,
        entry_price: float,
        total_cost_per_share: float,
        p_true_at_entry: float,
        z_at_entry: float,
        live_mode: Optional[bool] = None,
        order_id: str = "",
    ) -> None:
        if not self.control.claim_entry_notification(trade_id):
            return
        is_live = self.live_mode if live_mode is None else live_mode
        head = "SNIPE EXECUTED // 實盤進場" if is_live else "PAPER ENTRY // 模擬進場"
        dir_str = "LONG [UP 買漲]" if side == "UP" else "SHORT [DOWN 買跌]"
        spent = shares * total_cost_per_share
        oid_line = f"\n▸ <b>訂單單號</b>：<code>{html.escape(order_id[:18])}</code>" if order_id else ""
        msg = (
            f"<b>[{self.asset_symbol} | {head} #{trade_id}]</b>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"▸ <b>狙擊方向</b>：<b>{dir_str}</b>\n"
            f"▸ <b>目標盤口</b>：<code>{_fmt_window_range(window_ts)}</code> (進場秒數 <code>{elapsed_sec:.0f}s</code>)\n"
            f"▸ <b>成交均價</b>：<code>${entry_price:.3f}</code> (含費成本 <code>${total_cost_per_share:.4f}</code>)\n"
            f"▸ <b>部位規模</b>：<code>{shares:.2f} sh</code> (投入 <b>${spent:.2f}</b>)\n"
            f"▸ <b>模型指標</b>：P_true <b>{p_true_at_entry*100:.1f}%</b>  |  Z <code>{z_at_entry:+.2f}</code>"
            f"{oid_line}"
        )
        self.send_async(msg)

    def notify_exit(
        self,
        trade_id: int,
        side: str,
        window_ts: int,
        exit_reason: str,
        net_pnl_usdc: float,
        won: bool,
        exit_payout_per_share: float,
        entry_price: float,
        live_mode: Optional[bool] = None,
    ) -> None:
        if not self.control.claim_exit_notification(trade_id):
            return
        is_live = self.live_mode if live_mode is None else live_mode
        unit = "pUSD" if is_live else "USDC"
        if "TAKE_PROFIT" in exit_reason:
            head = "TARGET HIT // 獲利止盈"
            res_tag = "WIN [提前止盈]"
        elif "STOP_LOSS" in exit_reason:
            head = "STOPPED OUT // 止損平倉"
            res_tag = "LOSS [止損離場]"
        elif won:
            head = "SETTLED WIN // 結算獲勝"
            res_tag = "WIN [官方結算]"
        else:
            head = "SETTLED LOSS // 結算虧損"
            res_tag = "LOSS [官方結算]"

        stats_line = ""
        conn = _connect_readonly(self.db_path)
        if conn is not None:
            try:
                rows = conn.execute(
                    "SELECT net_pnl_usdc, won FROM paper_trades WHERE exit_reason IS NOT NULL ORDER BY id ASC"
                ).fetchall()
                if rows:
                    n = len(rows)
                    w = sum(1 for r in rows if r[1] == 1)
                    tot_pnl = sum(float(r[0] or 0.0) for r in rows)
                    streak_dir = rows[-1][1]
                    scnt = 0
                    for r in reversed(rows):
                        if r[1] == streak_dir:
                            scnt += 1
                        else:
                            break
                    s_txt = f"{scnt} 連勝" if streak_dir == 1 else f"{scnt} 連敗"
                    stats_line = (
                        "\n━━━━━━━━━━━━━━━━━━\n"
                        f"▸ <b>{self.asset_symbol} 累計</b>：<b>{w}W - {n-w}L</b> ({w/n*100:.1f}%) | <b>{tot_pnl:+.2f} {unit}</b> | <b>{s_txt}</b>"
                    )
            finally:
                conn.close()

        pf = _load_portfolio_summary(self.db_path)
        if pf["multi"] and (pf["wins"] + pf["losses"]) > 0:
            pn = pf["wins"] + pf["losses"]
            pwr = (pf["wins"] / pn) * 100.0
            prefix = "\n" if stats_line else "\n━━━━━━━━━━━━━━━━━━\n"
            stats_line += (
                f"{prefix}▸ <b>全倉合計</b>：<b>{pf['wins']}W - {pf['losses']}L</b> ({pwr:.1f}%)"
                f"\n▸ <b>全倉損益</b>：累計 <b>{pf['total_pnl']:+.2f} {unit}</b>  |  今日 <b>{pf['today_pnl']:+.2f} {unit}</b>"
            )

        msg = (
            f"<b>[{self.asset_symbol} | {head} #{trade_id}]  {net_pnl_usdc:+.2f} {unit}</b>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"▸ <b>結算結果</b>：<b>{res_tag}</b>  |  淨利 <b>{net_pnl_usdc:+.2f} {unit}</b>\n"
            f"▸ <b>交易部位</b>：<b>{side}</b> (盤口 <code>{_fmt_window_range(window_ts)}</code>)\n"
            f"▸ <b>進出場價</b>：<code>${entry_price:.3f}</code> ➔ <code>${exit_payout_per_share:.4f}</code>"
            f"{stats_line}"
        )
        self.send_async(msg)

    def notify_alert(self, title: str, detail: str) -> None:
        msg = f"<b>[{self.asset_symbol} | ALERT // {html.escape(title)}]</b>\n━━━━━━━━━━━━━━━━━━\n{html.escape(detail)}"
        self.send_async(msg)

    def _get_wallet_balance_message(self) -> str:
        unit = "pUSD" if self.live_mode else "USDC"
        bal: Optional[float] = None
        source = ""

        if self.live_exec is not None:
            try:
                bal = float(self.live_exec.get_pusd_balance())
                source = "CLOB V2 即時餘額"
            except Exception:
                bal = None

        if bal is None:
            funder = (os.getenv("POLY_FUNDER_ADDRESS") or "").strip()
            if funder:
                bal = _fetch_onchain_pusd_balance(funder)
                source = f"Polygon 鏈上錢包 (<code>{funder[:6]}...{funder[-4:]}</code>)"

        ctrl = self.control.load()
        cur_bet = ctrl.get("bet_usdc") or self.bet_usdc
        paused = bool(ctrl.get("paused", False))

        lines = [
            "<b>[VAULT // 帳戶與風控狀態]</b>",
            "━━━━━━━━━━━━━━━━━━",
        ]
        if bal is not None:
            lines.append(f"▸ <b>可用餘額</b>：<b>${bal:,.2f} {unit}</b>")
            lines.append(f"▸ <b>查詢來源</b>：{source}")
        else:
            lines.append("▸ <b>餘額狀態</b>：[WARN] 無法即時讀取鏈上餘額（請確認 <code>.env</code> 內的 <code>POLY_FUNDER_ADDRESS</code>）。")

        pf = _load_portfolio_summary(self.db_path)
        if pf["multi"]:
            parts = [f"{c['sym']} {c['today_pnl']:+.2f}" for c in pf["per_coin"]]
            lines.append(
                f"▸ <b>今日全倉</b>：<b>{pf['today_pnl']:+.2f} {unit}</b> ({' | '.join(parts)})"
            )
            lines.append(
                f"▸ <b>累計全倉</b>：<b>{pf['total_pnl']:+.2f} {unit}</b> ({pf['wins']}W-{pf['losses']}L)"
            )
        elif self.live_exec is not None:
            lines.append(
                f"▸ <b>今日累計</b>：<b>{self.live_exec.daily_pnl_usdc:+.2f} pUSD</b> "
                f"(熔斷線 -${self.live_exec.max_daily_loss_usdc:.1f})"
            )
        if self.live_exec is not None and self.live_exec.circuit_broken:
            lines.append("▸ <b>風控狀態</b>：[CIRCUIT BREAK] <b>已觸發今日虧損熔斷</b>")

        lines.append(
            f"▸ <b>目前設定</b>：單注 <b>${float(cur_bet):.1f} U</b>  |  {'PAUSED 暫停中' if paused else 'ACTIVE 運行中'}"
        )
        return "\n".join(lines)

    def _fetch_updates_sync(self) -> List[Dict[str, Any]]:
        url = TG_API_BASE.format(token=self.bot_token, method="getUpdates")
        params: Dict[str, Any] = {"timeout": 10, "allowed_updates": ["message"]}
        if self._update_offset > 0:
            params["offset"] = self._update_offset
        try:
            r = self._poll_http.get(url, params=params, timeout=15.0)
            if r.status_code != 200:
                return []
            data = r.json()
            if not data.get("ok"):
                return []
            return data.get("result") or []
        except Exception:
            return []

    def _handle_message_sync(self, msg: Dict[str, Any]) -> None:
        chat = msg.get("chat") or {}
        cid = str(chat.get("id") or "")
        chat_title = chat.get("title") or chat.get("first_name") or chat.get("username") or cid
        text = str(msg.get("text") or "").strip()
        msg_date = float(msg.get("date") or 0.0)
        if not cid or not text:
            return

        # Normalize commands (handle `/status@YourBotName`)
        cmd_token = text.split()[0]
        if cmd_token.startswith("/") and "@" in cmd_token:
            cmd_token = cmd_token.split("@", 1)[0]
        norm = cmd_token.lower()

        # Auto-bind owner chat_id on first interaction if unset
        if self.chat_id is None:
            if msg_date < (self._bot_start_ts - 300.0):
                return
            self.chat_id = cid
            self.control.update(chat_id=cid)
            print(f"\n[Telegram] 已自動綁定管理員 Chat ID: {cid}")

        is_owner = (cid == self.chat_id)
        ctrl = self.control.load()

        if not is_owner:
            if norm in ("/unsub", "/stop"):
                self.control.remove_subscriber(cid)
                self.send_message_sync(
                    "<b>[UNSUBSCRIBED // 已取消推播]</b>\n"
                    "━━━━━━━━━━━━━━━━━━\n"
                    "▸ 您已退出實盤推播名單。若需恢復請輸入 <code>/start</code>。",
                    chat_id=cid,
                )
                return

            if norm in ("/start", "/help", "/sub", "選單", "menu"):
                if msg_date < (self._bot_start_ts - 300.0):
                    return
                added = self.control.add_subscriber(cid)
                if added:
                    print(f"\n[Telegram] 新增推播訂閱者: {chat_title} (Chat ID: {cid})")
                    if self.chat_id:
                        self.send_message_sync(
                            "<b>[SUBSCRIBER JOINED // 新增推播訂閱]</b>\n"
                            "━━━━━━━━━━━━━━━━━━\n"
                            f"▸ <b>對象</b>：{html.escape(str(chat_title))} (<code>{cid}</code>)\n"
                            "▸ <b>權限</b>：唯讀（同步接收開單/結算推播與查看戰績，無權修改設定）\n"
                            f"▸ <b>移除指令</b>：<code>/unsub {cid}</code>",
                            chat_id=self.chat_id,
                        )
                reply = (
                    "<b>[TERMINAL // BTC-5M SNIPER FEED]</b>\n"
                    "━━━━━━━━━━━━━━━━━━\n"
                    "▸ <b>狀態</b>：<b>已訂閱即時實盤推播（唯讀權限）</b>\n"
                    "▸ 開倉與止盈/結算時將自動同步推播至此對話。\n"
                    "━━━━━━━━━━━━━━━━━━\n"
                    "▸ <code>/status</code> — 即時 BTC / Chainlink / Z值 / 報價\n"
                    "▸ <code>/report</code> — 累計勝率 / 今日損益 / 連勝紀錄\n"
                    "▸ <code>/trades</code> — 最近 10 筆下單明細\n"
                    "▸ <code>/unsub</code> — 取消接收推播通知\n"
                    "━━━━━━━━━━━━━━━━━━\n"
                    f"▸ <b>訂閱 Chat ID</b>：<code>{cid}</code>"
                )
                self.send_message_sync(reply, chat_id=cid)
                return

            if cid not in (ctrl.get("subscriber_ids") or []):
                return

        # Skip stale state-changing commands left in queue before bot started (allow 180s grace)
        is_old_backlog = msg_date < (self._bot_start_ts - 180.0)
        cur_bet = float(ctrl.get("bet_usdc") or self.bet_usdc)
        paused = bool(ctrl.get("paused", False))

        if norm in ("/start", "/help", "選單", "menu"):
            reply = (
                "<b>[TERMINAL // BTC-5M SNIPER COMMAND]</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "▸ <code>/status</code> — 即時 BTC / Chainlink / Z值 / 報價\n"
                "▸ <code>/report</code> — 累計勝率 / 今日損益 / 連勝紀錄\n"
                "▸ <code>/trades</code> — 最近 10 筆下單明細\n"
                "▸ <code>/balance</code> — 錢包可用餘額與風控狀態\n"
                "▸ <code>/pause</code> / <code>/resume</code> — 暫停或恢復開新倉\n"
                "▸ <code>/bet 25</code> — 動態調整單注金額 (U)\n"
                "▸ <code>/subs</code> — 管理推播訂閱名單（好友／群組）\n"
                "━━━━━━━━━━━━━━━━━━\n"
                f"▸ <b>授權 Chat ID</b>：<code>{cid}</code>"
            )
            self.send_message_sync(reply, chat_id=cid)
            return

        if text in ("▸ 即時盤況", "📊 即時盤況") or norm == "/status":
            self.send_message_sync(
                build_status_message(self.db_path, live_mode=self.live_mode, bet_usdc=cur_bet, paused=paused),
                chat_id=cid,
            )
            return

        if text in ("▸ 戰績報表", "💰 戰績報表") or norm == "/report":
            self.send_message_sync(build_report_message(self.db_path), chat_id=cid)
            return

        if text in ("▸ 最近10筆", "📜 最近10筆") or norm == "/trades":
            self.send_message_sync(build_trades_message(self.db_path, limit=10), chat_id=cid)
            return

        if not is_owner:
            self.send_message_sync(
                "<b>[ACCESS DENIED // 權限不足]</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "▸ 此帳號為<b>唯讀推播訂閱者</b>，僅管理員可查看錢包餘額或修改交易設定。",
                chat_id=cid,
            )
            return

        if norm == "/subs":
            subs = ctrl.get("subscriber_ids") or []
            sub_lines = [f"▪ <code>{html.escape(str(x))}</code>" for x in subs] if subs else ["▸ 目前無其他推播訂閱者"]
            msg_out = (
                "<b>[SUBSCRIBERS // 推播接收名單]</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                f"▸ <b>管理員</b>：<code>{self.chat_id}</code>\n"
                f"▸ <b>訂閱人數</b>：{len(subs)} 位\n"
                + "\n".join(sub_lines)
                + "\n━━━━━━━━━━━━━━━━━━\n"
                "▸ 請朋友直接點開此機器人傳送 <code>/start</code>（或把機器人拉進群組傳 <code>/start</code>）即自動加入推播。\n"
                "▸ 移除指令：<code>/unsub ChatID</code>"
            )
            self.send_message_sync(msg_out, chat_id=cid)
            return

        if norm == "/unsub":
            parts = text.split()
            if len(parts) < 2:
                self.send_message_sync(
                    "<b>[COMMAND // UNSUB]</b>\n━━━━━━━━━━━━━━━━━━\n"
                    "▸ 管理員移除訂閱格式：<code>/unsub ChatID</code>（可用 <code>/subs</code> 查詢 ID）",
                    chat_id=cid,
                )
                return
            target_id = parts[1].strip()
            if self.control.remove_subscriber(target_id):
                self.send_message_sync(
                    f"<b>[COMMAND // SUBSCRIBER REMOVED]</b>\n━━━━━━━━━━━━━━━━━━\n▸ 已移除推播對象：<code>{html.escape(target_id)}</code>",
                    chat_id=cid,
                )
            else:
                self.send_message_sync(
                    f"<b>[WARN // NOT FOUND]</b>\n━━━━━━━━━━━━━━━━━━\n▸ 名單中找不到 Chat ID：<code>{html.escape(target_id)}</code>",
                    chat_id=cid,
                )
            return

        if text in ("▸ 帳戶餘額", "👛 錢包餘額") or norm == "/balance":
            self.send_message_sync(self._get_wallet_balance_message(), chat_id=cid)
            return

        if is_old_backlog:
            return

        if text in ("■ 暫停狙擊", "⏸️ 暫停下單") or norm == "/pause":
            self.control.update(paused=True)
            self.send_message_sync(
                "<b>[COMMAND // PAUSED]</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "▸ <b>狀態</b>：已暫停開新倉（現有持倉仍會自動盯盤止盈/止損）。",
                chat_id=cid,
            )
            return

        if text in ("▶ 恢復狙擊", "▶️ 恢復下單") or norm == "/resume":
            self.control.update(paused=False)
            self.send_message_sync(
                "<b>[COMMAND // RESUMED]</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                f"▸ <b>狀態</b>：已恢復自動掃描下單（單注 <b>${cur_bet:.1f} U</b>）。",
                chat_id=cid,
            )
            return

        if norm == "/bet":
            parts = text.split()
            if len(parts) < 2:
                self.send_message_sync(
                    f"<b>[COMMAND // BET SIZE]</b>\n━━━━━━━━━━━━━━━━━━\n"
                    f"▸ 目前單筆下注：<b>${cur_bet:.1f} U</b>\n"
                    f"▸ 調整範例：<code>/bet 25</code> 或 <code>/bet 15</code>",
                    chat_id=cid,
                )
                return
            try:
                new_bet = round(float(parts[1]), 2)
                if not (1.0 <= new_bet <= 1000.0):
                    raise ValueError("out of range")
            except ValueError:
                self.send_message_sync(
                    "<b>[ERROR // INVALID BET]</b>\n━━━━━━━━━━━━━━━━━━\n"
                    "▸ 金額無效，請輸入 <code>1 ~ 1000</code> 之間的數字，例如 <code>/bet 25</code>。",
                    chat_id=cid,
                )
                return

            self.bet_usdc = new_bet
            self.control.update(bet_usdc=new_bet, bet_updated_at=time.time())
            self.send_message_sync(
                "<b>[COMMAND // BET UPDATED]</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                f"▸ 單筆下注金額已更新為 <b>${new_bet:.1f} U</b>（交易程式將於 1 秒內自動套用）。",
                chat_id=cid,
            )

    async def _db_watch_loop(self) -> None:
        """Used only in standalone `tg_bot.py` mode to push live entry/exit alerts by watching SQLite."""
        known_max_id = 0
        open_ids: set[int] = set()
        conn = _connect_readonly(self.db_path)
        if conn is not None:
            try:
                r = conn.execute("SELECT COALESCE(MAX(id), 0) FROM paper_trades").fetchone()
                known_max_id = int(r[0] or 0)
                open_rows = conn.execute("SELECT id FROM paper_trades WHERE exit_reason IS NULL").fetchall()
                open_ids = {int(x[0]) for x in open_rows}
            finally:
                conn.close()

        is_live = "live" in os.path.basename(self.db_path).lower()
        while not self._stop_event.is_set():
            await asyncio.sleep(2.0)
            if not self.get_recipient_ids():
                continue
            ctrl = self.control.load()
            embedded_alive = (time.time() - float(ctrl.get("embedded_bot_heartbeat_ts") or 0.0)) < 25.0
            conn = _connect_readonly(self.db_path)
            if conn is None:
                continue
            try:
                # 1. Detect newly opened or newly opened+settled trades
                new_rows = conn.execute(
                    """
                    SELECT id, window_ts, side, entry_elapsed_sec, shares, entry_price,
                           entry_fee_per_share, p_true_at_entry, z_at_entry,
                           exit_reason, exit_payout_per_share, net_pnl_usdc, won
                    FROM paper_trades
                    WHERE id > ?
                    ORDER BY id ASC
                    """,
                    (known_max_id,),
                ).fetchall()
                for row in new_rows:
                    (
                        tid, wts, side, eel, sh, epx, efee, pt, zv,
                        ex_reason, ex_pay, pnl, won,
                    ) = row
                    known_max_id = max(known_max_id, int(tid))
                    cost = float(epx) + float(efee)
                    if not embedded_alive:
                        self.notify_entry(tid, side, wts, eel, sh, epx, cost, pt, zv, live_mode=is_live)
                    if ex_reason is None:
                        open_ids.add(int(tid))
                    elif not embedded_alive:
                        self.notify_exit(tid, side, wts, ex_reason, float(pnl or 0.0), won == 1, float(ex_pay or 0.0), epx, live_mode=is_live)

                # 2. Detect closed trades among previously open positions
                if open_ids:
                    placeholders = ",".join("?" for _ in open_ids)
                    closed = conn.execute(
                        f"""
                        SELECT id, window_ts, side, entry_price, exit_reason,
                               exit_payout_per_share, net_pnl_usdc, won
                        FROM paper_trades
                        WHERE id IN ({placeholders}) AND exit_reason IS NOT NULL
                        """,
                        tuple(open_ids),
                    ).fetchall()
                    for tid, wts, side, epx, ex_reason, ex_pay, pnl, won in closed:
                        open_ids.discard(int(tid))
                        if not embedded_alive:
                            self.notify_exit(tid, side, wts, ex_reason, float(pnl or 0.0), won == 1, float(ex_pay or 0.0), epx, live_mode=is_live)
            except Exception:
                pass
            finally:
                conn.close()

    async def poll_loop(self, poll_commands: bool = True, watch_db_changes: bool = False) -> None:
        """Main background loop: polls Telegram long-poll updates and optionally watches SQLite."""
        if not self.enabled:
            return

        loop = asyncio.get_running_loop()
        watch_task: Optional[asyncio.Task] = None
        if watch_db_changes:
            watch_task = asyncio.create_task(self._db_watch_loop())

        try:
            while not self._stop_event.is_set():
                if not watch_db_changes:
                    self.control.update(embedded_bot_heartbeat_ts=time.time())
                else:
                    embedded_alive = (
                        time.time() - float(self.control.load().get("embedded_bot_heartbeat_ts") or 0.0)
                    ) < 25.0
                    if embedded_alive:
                        await asyncio.sleep(2.0)
                        continue
                if poll_commands:
                    updates = await loop.run_in_executor(None, self._fetch_updates_sync)
                    for upd in updates:
                        uid = int(upd.get("update_id") or 0)
                        if uid >= self._update_offset:
                            self._update_offset = uid + 1
                        msg = upd.get("message")
                        if isinstance(msg, dict):
                            await loop.run_in_executor(None, self._handle_message_sync, msg)
                else:
                    await asyncio.sleep(2.0)
        finally:
            if watch_task is not None:
                watch_task.cancel()
