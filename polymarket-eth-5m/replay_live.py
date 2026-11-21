"""
真實資料重播回測 (Replay Backtest on recorded live data)

讀取 run_monitor.py 錄下的 data/live_eth_5m.db，逐 tick 重播策略：
  - 價格 / 波動率 / 訂單流 / 盤口 全部用當時實際錄到的數值
  - 勝負用 Polymarket 官方結算結果 (windows.resolved_outcome)
  - 停利 / 停損以當時錄到的盤口 bid 成交 (未考慮深度滑價，屬樂觀估計)

用法:
  python replay_live.py --db data/live_eth_5m.db            # 新版 (預設設定)
  python replay_live.py --db data/live_eth_5m.db --legacy   # 舊版邏輯 (盤口校正會改寫基準價)
"""
import argparse
import math
import sqlite3
import sys
from dataclasses import replace
from statistics import NormalDist

from src.config import DEFAULT_CONFIG, StrategyConfig
from src.pricing_model import estimate_fair_value
from src.strategy import HighProbSniperStrategy, OrderBookTop

_STD_NORM = NormalDist()


def implied_strike_from_book(cl_spot, vol_1s, elapsed, up_bid, up_ask):
    """與 feeds.WindowState.calibrate_strike_from_opening_book 相同的盤口反推基準價公式。"""
    if cl_spot is None or up_bid is None or up_ask is None:
        return None
    if not (1.5 <= elapsed <= 45.0):
        return None
    spread = up_ask - up_bid
    if spread <= 0.0 or spread > 0.06:
        return None
    up_mid = 0.5 * (up_bid + up_ask)
    if not (0.22 <= up_mid <= 0.78):
        return None
    rem_sec = max(60.0, 300.0 - elapsed)
    eff_vol = max(vol_1s or 0.0, 0.000035)
    sigma_usd = max(cl_spot * eff_vol * math.sqrt(rem_sec), cl_spot * 0.00035)
    return cl_spot - _STD_NORM.inv_cdf(up_mid) * sigma_usd, sigma_usd


def load(db_path):
    c = sqlite3.connect(db_path)
    windows = {
        w: (k, src, o)
        for w, k, src, o in c.execute(
            "SELECT window_ts, strike_price, strike_source, resolved_outcome FROM windows "
            "WHERE resolved_outcome IS NOT NULL"
        )
    }
    snaps = {}
    for row in c.execute(
        "SELECT window_ts, ts, elapsed_sec, chainlink_price, oracle_gap_bps, vol_1s, cvd_imb, obi, "
        "z_score, up_bid, up_ask, down_bid, down_ask FROM snapshots ORDER BY window_ts, elapsed_sec"
    ):
        snaps.setdefault(row[0], []).append(row[1:])
    return windows, snaps


def initial_cl_strike(rows):
    """還原開盤 t=0 時系統用的 Chainlink 基準價 (從最早幾筆 snapshot 的 z 反推)。"""
    for ts, el, cl, gap, vol, cvd, obi, z, *_ in rows[:15]:
        if cl is None or z is None or el > 4.5:
            continue
        tau = max(0.5, 300.0 - el)
        return cl / math.exp(z * vol * math.sqrt(tau)) if vol else cl
    return None


def run(cfg: StrategyConfig, windows, snaps, legacy: bool, verbose=False):
    strat = HighProbSniperStrategy(cfg)
    skipped_uncertain = 0
    for w in sorted(snaps):
        if w not in windows:
            continue
        final_strike, src, outcome = windows[w]
        rows = snaps[w]
        # 與 run_monitor 相同: 基準價 = 系統最後採用的值 (含 BOOK_CALIBRATED)
        strike = final_strike
        # 新版: 盤口校正過基準價的盤 => 基準價不確定，整盤不開新倉
        uncertain = (not legacy) and cfg.skip_book_calibrated_windows and src == "BOOK_CALIBRATED"
        if strike is None:
            continue
        if uncertain:
            skipped_uncertain += 1
            strat.traded_windows.add(w)  # 這盤不開新倉
        for ts, el, cl, gap, vol, cvd, obi, z, ub, ua, db_, da in rows:
            if cl is None or vol is None:
                continue
            eff_vol = max(vol, cfg.min_1s_vol_floor)
            flow = max(-1.0, min(1.0, 0.70 * (cvd or 0.0) + 0.30 * (obi or 0.0)))
            fv = estimate_fair_value(
                strike_price=strike, chainlink_price=cl, remaining_sec=max(0.0, 300.0 - el),
                vol_1s=eff_vol, orderflow_score=flow, dynamic_gap_bps=gap,
                student_t_df=cfg.student_t_df, cvd_imbalance=cvd or 0.0,
            )
            if fv is None:
                continue
            strat.evaluate_tick(ts, w, el, fv, OrderBookTop(ub, ua, db_, da))
        if strat.active_position and strat.active_position.window_ts == w:
            # 官方結算: 用 resolved_outcome 決定勝負
            eff_close = strike + 1.0 if outcome == "UP" else strike - 1.0
            strat.settle_window(w + 300, w, strike, eff_close)
    return strat, skipped_uncertain


def report(name, strat, skipped=0):
    s = strat.summary_stats()
    t = strat.closed_trades
    if not t:
        print(f"{name}: 0 筆交易"); return s
    from collections import Counter
    reasons = Counter(x.exit_reason.split('_')[0] if x.exit_reason.startswith('SETTLED') else x.exit_reason for x in t)
    gp = sum(x.net_pnl_usdc for x in t if x.net_pnl_usdc > 0)
    gl = -sum(x.net_pnl_usdc for x in t if x.net_pnl_usdc < 0)
    worst_streak = cur = 0
    for x in t:
        cur = cur + 1 if not x.won else 0
        worst_streak = max(worst_streak, cur)
    print(
        f"{name:28s} 交易={s['trades']:3d}  勝率={s['win_rate_pct']:5.1f}%  總損益={s['net_pnl_usdc']:+8.2f}  "
        f"每筆={s['net_pnl_usdc']/s['trades']:+6.2f}  PF={gp/gl if gl else float('inf'):5.2f}  "
        f"最大回撤={s['max_drawdown_pct']:4.1f}%  最長連虧={worst_streak}  "
        f"出場={dict(reasons)}" + (f"  跳過不可信基準價={skipped}盤" if skipped else "")
    )
    return s


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/live_eth_5m.db")
    ap.add_argument("--legacy", action="store_true")
    a = ap.parse_args()
    W, S = load(a.db)
    if a.legacy:  # 舊版邏輯: 基準價被盤口校正的盤照樣進場
        st, sk = run(DEFAULT_CONFIG, W, S, legacy=True)
        report("舊版", st, sk)
    else:
        st, sk = run(DEFAULT_CONFIG, W, S, legacy=False)
        report("新版", st, sk)
