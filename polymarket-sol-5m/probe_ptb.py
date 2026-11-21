"""
測試 Polymarket 網站內部 API 能否取得官方 Price to Beat (基準價)

用法 (PowerShell):
  python probe_ptb.py            # 1) 驗證過去 30 盤  2) 即時量測新一盤開盤後幾秒拿得到
  python probe_ptb.py --past 100 # 驗證更多盤

會做兩件事:
  [A] 抓過去 N 盤的 openPrice / closePrice，和 data/live_sol_5m.db 裡官方結算方向比對
      => 如果 100% 一致，代表這個 API 就是官方基準價
  [B] 等下一盤開盤，從 t=0 開始每秒查一次，量測 openPrice 幾秒後出現
      => 決定能不能拿來即時交易
"""
import argparse
import datetime as dt
import sqlite3
import time

import requests

URL = "https://polymarket.com/api/crypto/crypto-price"
HDR = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


def iso(ts):
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch(window_ts, s):
    p = {"symbol": "SOL", "eventStartTime": iso(window_ts), "endDate": iso(window_ts + 300), "variant": "fiveminute"}
    r = s.get(URL, params=p, headers=HDR, timeout=5)
    return r.status_code, (r.json() if r.headers.get("content-type", "").startswith("application/json") else r.text[:200])


def part_a(s, n, db):
    print(f"\n[A] 驗證過去 {n} 盤 (和資料庫裡的官方結算比對)")
    c = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    rows = c.execute(
        "SELECT window_ts, strike_price, resolved_outcome FROM windows "
        "WHERE resolved_outcome IS NOT NULL ORDER BY window_ts DESC LIMIT ?", (n,)
    ).fetchall()
    ok = bad = miss = 0
    errs = []
    for w, our_k, outcome in rows:
        code, js = fetch(w, s)
        if code != 200 or not isinstance(js, dict) or js.get("openPrice") is None or js.get("closePrice") is None:
            miss += 1
            if miss <= 3:
                print(f"   {w} 取不到: HTTP {code} {js}")
            continue
        o, cl = float(js["openPrice"]), float(js["closePrice"])
        pred = "UP" if cl >= o else "DOWN"
        if pred == outcome:
            ok += 1
        else:
            bad += 1
        errs.append(abs(o - our_k) if our_k else None)
        time.sleep(0.25)
    e = [x for x in errs if x is not None]
    print(f"   方向與官方一致: {ok} / 不一致: {bad} / 取不到: {miss}")
    if e:
        e.sort()
        print(f"   我們系統用的基準價 vs API openPrice 誤差: 中位數 ${e[len(e)//2]:.2f}, 最大 ${e[-1]:.2f}")


def part_b(s):
    now = time.time()
    nxt = (int(now) // 300 + 1) * 300
    print(f"\n[B] 等待下一盤開盤 ({iso(nxt)})，約 {nxt - now:.0f} 秒...")
    time.sleep(max(0, nxt - time.time() - 1))
    t0 = time.time()
    while time.time() - nxt < 60:
        code, js = fetch(nxt, s)
        el = time.time() - nxt
        op = js.get("openPrice") if isinstance(js, dict) else None
        print(f"   t={el:5.1f}s  HTTP {code}  openPrice={op}")
        if op is not None:
            print(f"   => 開盤後 {el:.1f} 秒拿到官方基準價 ${float(op):,.2f}")
            return
        time.sleep(1.0)
    print("   => 60 秒內都拿不到 openPrice")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--past", type=int, default=30)
    ap.add_argument("--db", default="data/live_sol_5m.db")
    a = ap.parse_args()
    with requests.Session() as s:
        part_a(s, a.past, a.db)
        part_b(s)
