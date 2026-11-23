# Polymarket 5m Sniper Suite (BTC / ETH / SOL + XRP / DOGE Paper Scouts)

> **📋 Claude 策略與實戰健檢包（`2026-11-24 v5.1`）**：完整架構規格、近三日 22 筆實盤 / 8 筆模擬逐筆明細、虧損鑑識與 Phase 4.2 純 OOS 對比請見 **[`CLAUDE_REVIEW_V51_20261124.md`](./CLAUDE_REVIEW_V51_20261124.md)** 及 [`results/`](./results/)。

針對 Polymarket 每 5 分鐘漲跌盤（`{btc|eth|sol|xrp|doge}-updown-5m`）打造的多幣 Chainlink 60s TWAP 尾盤狙擊與實盤風控套件。在 **10% 等比複利**、**跨幣共用單日虧損熔斷** 與 **毫秒級預言機/L2 盤口衛生** 上，於 **`v5.1`** 全面升級為 **Phase 4.2 `E4` 雙源共識策略＋官方預言機硬性守門＋Gamma 即時分幣種手續費率**。

---

## v5.0 – v5.1 核心升級（Oracle-Anchored `E4` Dual-Consensus Sniper）

1. **官方預言機基準價 Fail-Closed 硬性守門（[`strategy.py`](./polymarket-btc-5m/src/strategy.py) / [`config.py`](./polymarket-btc-5m/src/config.py)）**
   - 新增 `require_official_oracle_strike=True` 與 `skip_book_calibrated_windows=True`：開新倉**僅允許** `strike_source in ("OFFICIAL_TWAP60", "GAMMA_OFFICIAL")`（經 Live Gamma API 比對 15 盤 `priceToBeat` 誤差 **`0.000000 bps`**），徹底排除 `BOOK_CALIBRATED` 或現貨猜測基準價；且該檢查置於 `_manage_open_position()` 之後，絕不阻斷持倉止損／停利。
2. **精準對齊 Polymarket Live 分幣種手續費率（`crypto_taker_fee_rate`）**
   - 依 Polymarket Gamma `feeSchedule` 實測拆分為：**`BTC = 0.05`（`crypto_btc_5m_15m_fees`）**、**`ETH / SOL / XRP / DOGE = 0.01`（`crypto_fees_v2`）**，取代舊版寫死 `0.07`（先前低估山寨幣淨 EV 約 `0.96¢/sh`）。
3. **Phase 4.2 `E4` 雙源共識過濾 ＋ 30 分鐘純 Binance 1s 波動率（[`pricing_model.py`](./polymarket-btc-5m/src/pricing_model.py) / [`feeds.py`](./polymarket-btc-5m/src/feeds.py)）**
   - 啟動即以 `data-api.binance.vision` REST 預熱最多 `1,850` 根 1s K 線計算 30 分鐘（`1800s`）單市場波動率（地板 `0.000035`），維護 60 秒因果 `CL - BN` 基差 `mean_basis_60s()`（至少 10 筆樣本）。
   - 進場強制 `min(|z_CL|, |z_BN_adj|) >= 1.30` 且取保守 `min(P_CL, P_BN)`，搭配 60s TWAP 尾段 Brownian bridge 積分（`0.70` Gaussian + `0.30` Student-t `df=6.0`）、CVD-only 訂單流過濾與 `max_model_market_divergence = 0.25`。
4. **跨幣單槽保護（`max_coins=1`）＋ Maker `@ $0.990` 停利＋ VPN 看門狗（[`shared_risk.py`](./shared_risk.py) / [`vpn_watchdog.py`](./vpn_watchdog.py)）**
   - 同一 5m 窗口三幣互斥僅允許最多 1 幣成交；持倉在 bid `>= 0.94` 且 `p_true >= 0.95` 時自動掛 `0% fee` Maker `$0.990` 賣單提前回收 `pUSD`。
   - 新增 `polymarket-xrp-5m` 與 `polymarket-doge-5m` **純模擬觀察節點（PAPER）**；實證評估後確認 `XRP`/`DOGE` 尚不具實盤條件，僅留作數據蒐集。

---

## v4.2 – v4.5 基礎架構演進（歷史版本說明）

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

## v4.5 盤口即時性、異常對帳、毫秒校時與行程韌性升級

> 策略門檻與進場頻率 **100% 維持原版 v4.2（`t=0` Spot Strike + `BOOK_CALIBRATED`）**，專注消滅實盤「用舊盤口進場 / 逾時失控持倉 / 時鐘偏移」：

1. **CLOB 增量 `price_change` L2 盤口引擎 + 嚴格過期閘門（`1.5s / 1.0s`）**
   - `feeds.py` 同時處理 `book` 全量與 `price_change` 增量掛撤單事件，逐價位更新 `price -> size` L2 表並重算深度 VWAP 買賣價，消除兩次成交間長達 `2.5s+` 的盤口靜止與頻繁 REST 補抓。
   - 實盤下單閘門收緊為 `book_age > 1.5s or book_lag > 1.0s`，每筆進場日誌紀錄毫秒級 `book_age` / `book_lag`。
2. **鏈上備援與交易完全隔離（Strict Origin Freshness）**
   - `chainlink_oracle.py` 將 Polygon/Arbitrum JSON-RPC 輪詢嚴格隔離至 `onchain_display_price` 僅作展示比對，絕不寫入 `settlement_reference_price`；`is_alive()` 驗證串流來源且檢查 `oracle_ts` 新鮮度。
   - 移除 RTDS 萬用字元 `filters: ""`，改為精確幣種過濾 `'{"symbol":"{btc|eth|sol}/usd"}'` 並啟用雙連線熱備援與 `3.5s` 斷流重連。
3. **下單逾時自動對帳（`RECONCILED_POSITION`）與啟動接管**
   - `PolymarketLiveExecutor` 收緊為 `httpx.Timeout(connect=2, read=3, write=3, pool=2)`；非明確 4xx 拒單例外（如讀取逾時）立即於 `1~2s` 內輪詢 `get_trades` 與條件代幣餘額，若已成交則標記 `RECONCILED_POSITION` 立即納入止盈／止損管理。啟動時同步掃描當窗未平倉代幣餘額自動接管。
4. **毫秒級 Binance `/api/v3/time` RTT/2 校時（`±50ms` 死區）與換窗預連線**
   - 啟動及每 10 分鐘量測 `serverTime - (t0 + RTT/2)` 精準校準 `utc_now()`，誤差 `> 250ms` 發出 Telegram 警報；`290s` 預連下一窗 CLOB WebSocket 以縮短跨窗盤口真空。
5. **非終端機日誌輪替（`50MB x 5`）＋ macOS `launchd` / Linux `systemd` 守護**
   - `--log-file` 啟用 `RotatingFileHandler` 且偵測 `not isatty()` 時自動關閉每秒 `\r` 洗版；`daily_review.py` 新增 `book_age P50/P99`、`book_lag`、換窗首單延遲與時鐘殘差健檢表；新增 [`requirements.lock`](./requirements.lock) 鎖定全部執行期套件版本。

---

## v4.4 執行速度與穩定度強化（Zero-Side-Effect，100% 保留 v4.2 策略與頻率）

> `config.py`、`pricing_model.py`、`strategy.py`、`feeds.py` 完全未動，**0 影響開單門檻與頻率**，專注壓低實盤進／出場延遲並消除連線與檔案鎖風險：

1. **狙擊熱區 4Hz（`250ms`）高頻掃描 ＋ 嚴格 1Hz 寫盤／指標節流**
   - `148s~258s` 狙擊窗口及持倉盯盤期間主迴圈切換為 `0.25s` 評估，縮短進場確認與停利／止損觸發反應時間；EWMA 基差、TG 控制檔、SQLite 寫入與主控台輸出嚴格鎖定 `1Hz`（`is_1hz_tick`），不改變指標動態。
2. **進場前 Zero-RTT 共用 `pUSD` 快取（省下 ~`240ms` RPC 來回）**
   - [shared_risk.py](./shared_risk.py) 新增原子檔案鎖快取（`record_balance` / `record_spend` / `get_fast_balance`），餘額距今 `<= 25s` 且扣除已成交花費後仍 `>= $10` 時跳過下單鎖內的同步 RPC 查詢，讓三幣並發搶單不超押且更快成交。
3. **RTDS 預言機熱備援不空斷（消除每日約 25,920 次無意義斷線重連）**
   - `_rtds_chainlink_ws_loop` 在確認 `PolyBolt` 健康而略過重複報價前先更新心跳時間戳，維持 RTDS WebSocket 常駐熱備援，PolyBolt 一斷線可 `0ms` 無縫接手。
4. **CLOB HTTP/2 保活提頻（`15s`）＋ 條件代幣餘額重試預抓（`4s / 8s / 13s`）**
   - `90s~295s` 每 `15s` 主動保活 CLOB HTTP/2 長連線，避免 `5m` 尾盤停利前連線冷卻；成交後於背景連試 3 次預抓條件代幣餘額，並於進／平倉日誌實測顯示 `耗時 XXms`。
5. **緊急止損 FAK 滑價加寬重試 ＋ 迴圈異常看門狗 ＋ SQLite 防鎖死**
   - `EMERGENCY_STOP_LOSS`（`slippage >= 5¢`）遇 `no orders found to match` 立即加寬 `5¢` 原地重送 1 次並於 `250ms` 後再掃描（`EARLY_TAKE_PROFIT` 保持嚴格不退讓）；主迴圈包覆 `[MONITOR LOOP WARN]` 退避重試不閃退；SQLite 統一開啟 `timeout=5.0`、`WAL`、`synchronous=NORMAL`、`busy_timeout=5000`。

---

## v4.3.1 穩定度升級（100% 保留 v4.2 原版 Strike 與進場頻率）

> **重要回歸說明**：原版 v4.2 策略參數（如 `min_1s_vol_floor`、Student-t 肥尾與 `7%` Taker 手續費）是針對 **`t=0` Chainlink Spot + `BOOK_CALIBRATED`** 所校準。v4.3 曾短暫改為 `POLYBOLT_TWAP_T0` 導致模型 Z 值與盤口基準錯位、觸發頻率歸零；**`v4.3.1` 已 100% 還原 v4.2 原版 Strike 與進場邏輯**，僅保留以下五項零副作用可靠性升級：

1. **Telegram 推播高可靠投遞＋頻道／群組相容修復**
   - `send_message_sync` 升級為 3 次指數退避重試，遇 `5xx`／連線異常自動重建 `requests.Session`；針對 `-100...` 頻道與群組自動省略 `ReplyKeyboardMarkup`（並於 HTTP 400 fallback 一併清除），避免 `inline keyboard expected` 失敗，且全路徑異常皆輸出 `[TG SEND WARN]` / `[TG NOTIFY FAILED]` 至 `stderr`。
2. **PolyBolt WebSocket 殭屍連線看門狗**
   - `_polybolt_v2_ws_loop` 啟用主動心跳（`ping_interval=15`、`ping_timeout=10`）與 `12s` `recv()` 超時看門狗，斷流即刻重連不卡死。
3. **實盤過舊盤口防護（`book_age > 5.0s`）**
   - 實盤下單閘門除既有 `cl_age > 4.5s` 外，新增檢查最後盤口更新時間（WS + REST fallback），僅在兩者同時斷更逾 `5.0s` 時攔截進場，不影響正常頻率。
4. **平倉／結算後即時 `pUSD` 餘額同步**
   - 盤中完整止盈／止損與 5m 官方結算完成後，立即刷新實盤 `pUSD` 可用餘額並同步更新跨幣風控權益基準，不等候 `20s` 週期輪詢。
5. **主控台 `Edge` 與下單手續費口徑一致**
   - 主控台即時顯示的 `up_edge`／`dn_edge` 同步傳入 `fee_rate=cfg.crypto_taker_fee_rate`，與策略內部決策模型完全對齊。

---

## 目錄結構

```text
polymarket-5m-sniper-suite/
├── CLAUDE_REVIEW_V51_20261124.md  # v5.1 完整架構、22 筆實盤交易明細、虧損鑑識與 OOS 驗證報告
├── results/                       # 去識別化實盤／模擬盤逐筆成交匯出（CSV）
├── shared_risk.py                 # 10% 等比算股數、5 股安全保底、跨幣單槽與日熔斷互斥鎖
├── daily_review.py                # 每日唯讀健檢引擎（進程、滑價、分桶勝率、P99新鮮度與時鐘健檢）
├── vpn_watchdog.py                # Surfshark HK VPN / Polymarket geoblock 與 RTT 健康看門狗
├── start_all.sh                   # 一鍵啟動 BTC / ETH / SOL（預設模擬盤，加 --live 進實盤）
├── requirements.txt               # 基礎依賴清單
├── requirements.lock              # 完整鎖定執行期版本（py-clob-client-v2 / websockets / httpx）
├── test_*.py                      # 跨幣整合測試套件（v4.3–v5.1 共 115 項測試）
├── polymarket-btc-5m/             # BTC 5m 監控＋實盤下單（Telegram 主控，fee=5%）
├── polymarket-eth-5m/             # ETH 5m 監控＋實盤下單（單向推播，fee=1%）
├── polymarket-sol-5m/             # SOL 5m 監控＋實盤下單（單向推播，fee=1%）
├── polymarket-xrp-5m/             # XRP 5m 模擬觀察＋錄製（PAPER only，fee=1%）
└── polymarket-doge-5m/            # DOGE 5m 模擬觀察＋錄製（PAPER only，fee=1%）
```

---

## 安裝與啟動步驟

### 1. 建立獨立虛擬環境與安裝套件（Python 3.10+）

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.lock
```

*(macOS 正式實盤建議改用純 ASCII 路徑部署並以 `launchd` KeepAlive + `caffeinate -dimsu` 託管，避免睡眠或終端關閉中斷；Linux VPS 建議以 `systemd` `Restart=always` + `chrony` 校時託管。)*

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
# 執行全部根目錄測試（含 v5.1 E4 雙源共識、官方預言機守門、分幣種費率與 VPN 看門狗）
.venv/bin/pytest -q test_*.py
(cd polymarket-btc-5m && ../.venv/bin/pytest -q tests/)
(cd polymarket-eth-5m && ../.venv/bin/pytest -q tests/)
(cd polymarket-sol-5m && ../.venv/bin/pytest -q tests/)

# 手動跑一次每日健檢（產生 reports/daily_review_YYYY-MM-DD.md；可加 --send-tg 推播摘要）
.venv/bin/python daily_review.py
```

---

## 風險聲明

本專案包含真實資金（`pUSD`）CLOB 自動下單模組。加密貨幣 5 分鐘二元市場波動極快，存在滑價、網路延遲、預言機偏差與連續虧損風險；過去測試或模擬績效不代表未來獲利，請務必先以模擬盤與最小單位（5 股）充分驗證後再使用，並自行承擔全部交易風險。
