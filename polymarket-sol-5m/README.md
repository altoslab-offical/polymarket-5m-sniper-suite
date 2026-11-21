# Polymarket SOL 5m 漲跌盤量化系統 (Chainlink-Oracle High-Prob Sniper)

由 `polymarket-btc-5m`（2026-11-17 修正版）獨立拆出的 **SOL 專用版**，交易 Polymarket **`sol-updown-5m-{ts}`** 盤口。
策略邏輯與 BTC 版完全相同，只更換資料源與幣種相關常數。

## 策略邏輯（與 BTC 版相同）
1. 每 5 分鐘一盤，開盤用 Chainlink SOL/USD 價格當基準價。
2. 前 150 秒不動作（五五波、手續費最高）。
3. 150～255 秒之間，同時滿足才進場（每盤最多 1 筆）：
   - Chainlink 現價離基準價夠遠：|Z| ≥ 1.30
   - Student-t 厚尾模型勝率 ≥ 80%
   - 盤口買價 0.68～0.82、價差 ≤ 0.06
   - 扣手續費後淨期望值 ≥ 3.5%、模型與市場差距 ≤ 15%
   - 幣安/Coinbase 訂單流（CVD）不反向、交易所沒有領先 Chainlink 反向超過 6 bps
   - 基準價沒有被開盤盤口校正（BOOK_CALIBRATED 的盤整盤跳過）
4. 出場：盤口 bid ≥ 0.98 提前停利；Z 跌破 -0.35 且還賣得掉時緊急停損；其餘持有到結算。

## 和 BTC 版不同的地方
| 項目 | BTC 版 | SOL 版 |
|---|---|---|
| 市場代號 | `btc-updown-5m-` | `sol-updown-5m-` |
| Chainlink Data Streams Feed ID | `0x00039d9e...` | `0x0003b778d3f6b2ac4991302b89cb313f99a42467d6c9c5f96f57c29c0d2bc24f` |
| Polygon 合約 | `0xc907E116...` | **不使用**（查不到可確認的 SOL/USD 地址） |
| Arbitrum 合約 | `0x6ce18586...` | `0x24ceA4b8ce57cdA5058b924B9B9987992450590c` |
| RTDS / PolyBolt 訂閱 | `btc/usd`, `btcusd` | `sol/usd`, `solusd` |
| 幣安 / Coinbase | `BTCUSDT`, `BTC-USD` | `SOLUSDT`, `SOL-USD` |
| 價格合理範圍 | $1,000～$2,000,000 | $1～$10,000（小數位判斷門檻同步下修） |
| 盤口校正基準價門檻 | 固定 $12 | 現價 × 1.85 bps（等比例換算） |
| 自訂鏈上 RPC 環境變數 | `POLYGON_RPC_URL` | `ARBITRUM_RPC_URL` |
| 預設資料庫 | `data/live_btc_5m.db` | `data/live_sol_5m.db` |

其餘參數（Z 門檻、進場價、停利停損、波動率下限）**沿用 BTC 版，尚未針對 SOL 調整**。

## 安裝與啟動（PowerShell）
```powershell
cd polymarket-sol-5m
chcp 65001
pip install -r requirements.txt
python -m unittest discover -s tests

# 模擬盤（先跑這個錄資料）
python run_monitor.py --db data/live_sol_5m.db --bankroll 1000 --bet 25
```

## 錄滿幾天後，用真實資料回測
```powershell
python replay_live.py --db data/live_sol_5m.db
python replay_live.py --db data/live_sol_5m.db --legacy   # 不跳過 BOOK_CALIBRATED 盤的對照組
```

## 注意事項
- `.env` 沒有一起複製（裡面有私鑰）。需要的話自行從 BTC 資料夾複製過來。
- **Telegram 不能和 BTC / ETH 版共用同一個機器人 token 同時執行**（兩個程式搶同一個 getUpdates 會互相衝突）。請另外向 BotFather 申請一個 SOL 專用機器人，填進 SOL 的 `.env` 的 `TG_BOT_TOKEN`。
- 若和 BTC / ETH 版同時跑實盤，各版本用同一個錢包、各自計算每日熔斷，總風險是兩者相加。
- PolyBolt `price.crypto`（Tier 2）依 Polymarket 文件已改為 Pyth 來源，不是 Chainlink，建議不要設定 `POLY_API_KEY` 等金鑰啟用它。
- SOL 的 5 分鐘盤口通常比 BTC 薄，實際成交價可能較差；波動也較大，Z 門檻與波動率下限很可能需要重調。
