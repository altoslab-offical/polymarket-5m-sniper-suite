# Polymarket 5m Sniper Suite (BTC / ETH / SOL)

針對 Polymarket 每 5 分鐘漲跌盤（`{btc|eth|sol}-updown-5m`）打造的三幣整合自動交易與每日健檢套件。在原版 Chainlink 預言機尾盤狙擊架構上，整合 **10% 等比複利倉位管理**、**跨幣共用單日虧損熔斷**、**CLOB V2 授權自動對齊** 與 **每日唯讀健檢報表**。

---

## 相較基礎版（v4.2）的核心升級

1. **等比複利與 Polymarket 5 股最低限額雙保險（[`shared_risk.py`](./shared_risk.py)）**
   - 支援 `--bet-pct`（預設 `0.10` = 即時可用餘額 `10%` 等比複利），從小資金（如 `$40U`）隨權益成長平滑放大至目標規模，不必手動改固定金額。
   - **最低 5 股保護（`compute_safe_buy_usdc`）**：因買單滑價緩衝（`ask + 1¢~4¢`）可能導致送出金額在最高限價換算低於 5 股而被拒單，送單前會自動保底 `5.05 * limit_px`。
   - **跨幣下單互斥鎖（`entry_lock`）**：三幣同秒出現訊號時依序鎖定並即時重讀可用 `pUSD`，避免並發超押。
2. **三幣共用單日最大虧損熔斷（`SharedDailyRiskGuard`）**
   - 支援 `--max-daily-loss-pct`（預設 `0.30` = 當日權益 `-30%`），由 `BTC`、`ETH`、`SOL` 共用同一份原子鎖定狀態檔，任何一幣達線即全倉停止開新倉，隔日 `UTC 00:00` 按最新權益自動重算重置。
3. **CLOB V2 實盤連線與預言機串流修正**
   - **`POLY_SIGNATURE_TYPE=3` 文件化與驗證**：支援 Polymarket V2 Deposit Wallet（見 `.env.example` 與 `check_account.py`）。
   - **乾淨衍生 API 憑證**：`PolymarketLiveExecutor` 優先呼叫 `derive_api_key()`（失敗才退回 `create_or_derive_api_key()`），消除啟動時的 `400` 噪音。
   - **完整預言機串流放行**：實盤下單閘門同時承認 `CHAINLINK_DATA_STREAMS`、`POLYBOLT_CHAINLINK` 與 `POLY_RTDS_CHAINLINK`，避免帶有 API 憑證時因切換至 PolyBolt 高優先級串流而被誤擋。
   - **`httpx[http2]`（`h2`）連線保活**：維持 CLOB HTTP/2 長連線，減少尾盤熱區冷啟動 TLS 延遲。
4. **零衝突 Telegram 控盤 + 每日健檢工具（[`daily_review.py`](./daily_review.py)）**
   - BTC 進程負責 `getUpdates` 互動指令（`/status`、`/report`、`/balance`、`/pause`、`/resume`），ETH / SOL 自動走單向推播模式避免 `409 Conflict`。
   - `daily_review.py` 以 `nice(19)` 低優先權唯讀掃描三幣 SQLite、日誌滑價（`fill - ask`）、成交率、進場時段／Z 值分桶與即時錢包，並內建嚴格小樣本防呆（`n < 20` 僅觀察不改參）。

---

## v4.3 穩定度與精準度升級（不影響正常開單頻率）

> 本次升級僅強化**預言機 Strike 精度、WebSocket／Telegram 連線強韌性、過舊盤口保護與餘額即時同步**，**未更動任何進場門檻、Z 值、勝率或停利／止損規則**。

1. **精準 Strike 雙層鎖定（`POLYBOLT_TWAP_T0` ➔ `GAMMA_OFFICIAL`）**
   - 開盤 `t=0` 優先取窗口邊界 Chainlink 60s TWAP（`POLYBOLT_TWAP_T0` / `CHAINLINK_TWAP_T0`），避免開盤現貨跳動誤差（實測較 `t=0` 現貨與 `BOOK_CALIBRATED` 反推誤差降低 ~12x–18x），並保護此精準基準不被開盤盤口反推蓋寫。
   - 若 Gamma 已公布上一盤 `finalPrice`（`GAMMA_PREV_FINAL`）或於盤中重啟，自動無縫升級鎖定 `GAMMA_OFFICIAL`（實測 `24/24` 窗口誤差 `$0.0000`）。
2. **Telegram 推播高可靠投遞＋頻道／群組相容修復**
   - `send_message_sync` 升級為 3 次指數退避重試，遇 `5xx`／連線異常自動重建 `requests.Session`；針對 `-100...` 頻道與群組自動省略 `ReplyKeyboardMarkup`（並於 HTTP 400 fallback 一併清除），避免 `inline keyboard expected` 失敗，且全路徑異常皆輸出 `[TG SEND WARN]` / `[TG NOTIFY FAILED]` 至 `stderr`。
3. **PolyBolt WebSocket 殭屍連線看門狗**
   - `_polybolt_v2_ws_loop` 啟用主動心跳（`ping_interval=15`、`ping_timeout=10`）與 `12s` `recv()` 超時看門狗，斷流即刻重連不卡死。
4. **實盤過舊盤口防護（`book_age > 5.0s`）**
   - 實盤下單閘門除既有 `cl_age > 4.5s` 外，新增檢查最後盤口更新時間（WS + REST fallback），僅在兩者同時斷更逾 `5.0s` 時攔截進場，不影響正常頻率。
5. **平倉／結算後即時 `pUSD` 餘額同步**
   - 盤中完整止盈／止損與 5m 官方結算完成後，立即刷新實盤 `pUSD` 可用餘額並同步更新跨幣風控權益基準，不等候 `20s` 週期輪詢。
6. **主控台 `Edge` 與下單手續費口徑一致**
   - 主控台即時顯示的 `up_edge`／`dn_edge` 同步傳入 `fee_rate=cfg.crypto_taker_fee_rate`，與策略內部決策模型完全對齊。

---

## 目錄結構

```text
polymarket-5m-sniper-suite/
├── shared_risk.py          # 10% 等比算股數、5 股安全保底與跨幣日熔斷 / 互斥鎖
├── daily_review.py         # 每日唯讀健檢引擎（進程、滑價、分桶勝率與漸進建議）
├── start_all.sh            # 一鍵啟動 BTC / ETH / SOL（預設模擬盤，加 --live 進實盤）
├── requirements.txt        # 統一依賴（含 py-clob-client-v2 與 httpx[http2]）
├── test_live_sizing_risk.py
├── test_daily_review.py
├── test_upgrades_v43.py
├── polymarket-btc-5m/      # BTC 5m 監控＋下單（Telegram 主控）
├── polymarket-eth-5m/      # ETH 5m 監控＋下單（單向推播）
└── polymarket-sol-5m/      # SOL 5m 監控＋下單（單向推播）
```

---

## 安裝與啟動步驟

### 1. 建立獨立虛擬環境與安裝套件（Python 3.10+）

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

### 2. 設定各幣 `.env` 與確認 `signature_type`

將三個資料夾內的 `.env.example` 各自複製為 `.env`並填入錢包與 API 資訊（**切勿提交或外流 `.env`**）：

```bash
cp polymarket-btc-5m/.env.example polymarket-btc-5m/.env
cp polymarket-eth-5m/.env.example polymarket-eth-5m/.env
cp polymarket-sol-5m/.env.example polymarket-sol-5m/.env
chmod 600 polymarket-*/.env
```

> **關鍵提示**：一般 Polymarket.com 儲值錢包（Deposit Wallet）在 CLOB V2 通常使用 `POLY_SIGNATURE_TYPE=3`；若設定 `1` 或 `2` 卻顯示餘額 `$0` 或 `0/4` 授權，請務必切換後用下方檢查腳本驗證。

### 3. 唯讀帳戶與授權檢查（不會下單）

```bash
cd polymarket-btc-5m
../.venv/bin/python check_account.py
```

確認輸出顯示 **可用 `pUSD` 餘額正確** 且 **`4/4` 個交換合約已授權**、伺服器連線 `OK`。

### 4. 先跑模擬盤確認連線，再決定是否進實盤

```bash
# 先以模擬盤跑三幣（預設 10% 等比、$40U 模擬本金）
./start_all.sh --paper --bankroll 40 --bet-pct 0.10

# 確認 Geoblock、WebSocket 與預言機正常後，再開啟真金實盤（30% 共用日熔斷）
./start_all.sh --live --bet-pct 0.10 --max-daily-loss-pct 0.30
```

### 5. 執行單元測試與每日唯讀健檢

```bash
# 執行全部測試（等比倉位、日熔斷、健檢、v4.3 升級與三幣核心邏輯）
.venv/bin/pytest -q test_live_sizing_risk.py test_daily_review.py test_upgrades_v43.py
(cd polymarket-btc-5m && ../.venv/bin/pytest -q tests/)
(cd polymarket-eth-5m && ../.venv/bin/pytest -q tests/)
(cd polymarket-sol-5m && ../.venv/bin/pytest -q tests/)

# 手動跑一次每日健檢（產生 reports/daily_review_YYYY-MM-DD.md；可加 --send-tg 推播摘要）
.venv/bin/python daily_review.py
```

---

## 風險聲明

本專案包含真實資金（`pUSD`）CLOB 自動下單模組。加密貨幣 5 分鐘二元市場波動極快，存在滑價、網路延遲、預言機偏差與連續虧損風險；過去測試或模擬績效不代表未來獲利，請務必先以模擬盤與最小單位（5 股）充分驗證後再使用，並自行承擔全部交易風險。
