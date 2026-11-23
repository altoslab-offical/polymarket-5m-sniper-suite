# Polymarket 5m Sniper Suite `v5.1` — Architecture & Multi-Day Validation Package for Claude Review

> **Status (`2026-11-24 00:25 +08:00`)**: Deployed **`v5.1`** across all 5 coin monitors (`BTC/ETH/SOL` LIVE + `XRP/DOGE` PAPER), running cleanly in **manual Telegram `/pause` (`paused: true`)** awaiting final review & owner `/resume`.

---

## 1. Executive Summary & Questions for Claude

Between **2026-11-21 and 2026-11-23**, the live suite ran under the previous `v4.3–v4.9` iterations (`22` LIVE trades: `16W / 6L = 72.7% WR`, `-7.25 pUSD` cumulative trade PnL; current wallet balance `$36.26 pUSD`). Following a forensic loss audit and comparison against Phase 4.2 pure OOS research (`live_3coin_42.db`), we retired the pre-`E4` setup and promoted **`v5.1` (Phase 4.2 `E4` Dual-Consensus TWAP60 Sniper)** with two critical production hardening fixes discovered during oracle auditing:

1. **Fail-Closed Official Oracle Strike Guard (`require_official_oracle_strike = True`)**: New entries are strictly restricted to `strike_source in ("OFFICIAL_TWAP60", "GAMMA_OFFICIAL")` (`0.000000 bps` error vs Gamma `priceToBeat`), eliminating spot/`BOOK_CALIBRATED` guesses that accounted for **all 3 of 3 BTC live losses**. Open-position management (`_manage_open_position`) runs before this gate so exits are never blocked.
2. **Exact Live Polymarket CLOB Fee Schedule (`BTC = 0.05`, `ETH/SOL/XRP/DOGE = 0.01`)**: Verified live against `gamma-api.polymarket.com` (`crypto_btc_5m_15m_fees` vs `crypto_fees_v2`), replacing the stale hardcoded `0.07` fee rate that previously understated altcoin net EV by `~0.96¢/share`.
3. **`E4` Dual-Consensus Guard (`min(|z_CL|, |z_BN_adj|) >= 1.30`) + 1,800s Binance-Only Volatility**: Pre-warms up to 1,850 1s Binance klines on boot via REST, maintains a causal 60s rolling `(binance - chainlink)` basis (`mean_basis_60s()`, `>= 10` samples), and requires both Chainlink TWAP Z and basis-adjusted Binance TWAP Z to confirm `>= 1.30` (taking conservative `min(p_CL, p_BN)`).

### Specific Review Questions for Claude

1. **Live Parity Audit (`v5.1` vs Phase 4.2 `E4`)**: Please review [`pricing_model.py`](./polymarket-btc-5m/src/pricing_model.py), [`strategy.py`](./polymarket-btc-5m/src/strategy.py), [`feeds.py`](./polymarket-btc-5m/src/feeds.py), and [`run_monitor.py`](./polymarket-btc-5m/run_monitor.py) to confirm there are no remaining divergences from Phase 4.2 `E4` semantics (excluding intentional live safety guards: `book_age <= 1.5s`, `book_lag <= 1.0s`, `cl.is_alive(4.5s)`, `max_coins = 1`).
2. **Universe Selection (`BTC+ETH+SOL` vs `BTC+ETH` Only)**: In Phase 4.2 OOS `731`-window validation, all `11/11` `E4` fills came from `ETH` (`8`) and `BTC` (`3`), with `0` from `SOL`. On live `v4.5–v4.9`, `SOL` produced `10` trades (`7W/3L`, `-3.27 pUSD`), including `#9` (`z_CL=-1.72` vs `z_BN=-0.99`, which `E4` blocks) and `#10` (`SETTLED_DOWN`, `-4.29 pUSD`). Under `v5.1` (`max_coins=1` + `E4` + `0.01` fee), should we keep `SOL` LIVE alongside `BTC/ETH` or move `SOL` to PAPER-only until `E4` SOL OOS reaches meaningful `n`?
3. **Maker TP `@ $0.990` / Taker TP `@ $0.98` vs Hold to Settlement (`$1.000`, `0%` fee)**: With `~5.1–5.7` shares per ticket (`$36.26` wallet, `10%` proportional sizing floored at 5 shares), Maker `@ $0.990` forfeits `1¢/sh` (`~$0.05/win`) in exchange for eliminating final-second pin risk and recycling `pUSD` before window close. Is keeping Maker `@ $0.990` + EV-consistent Taker `@ $0.98` optimal for small-bankroll compounding?
4. **Exploratory Sample Caveat (`n = 30` < `300`)**: Across pre-`11/21` (`19/19`) and `11/21–11/23` (`11/11`), `E4` stands at `30/30 (100%)` in Phase 4.2 recordings—still well below the pre-registered `n >= 300` Bonferroni gate. Are `10%` proportional sizing (~5.3 sh min-order floor) + `30%` daily shared circuit breaker + `1-coin-per-window` slot lock sufficient risk containment while accumulating the next 100+ live windows?

---

## 2. Official Oracle Alignment & `v5.1` Strategy Specification

### Live Polymarket Gamma Verification (`2026-11-24 00:08–00:12 +08:00`)

| Asset | Gamma `resolutionSource` | Live Gamma `feeType` | Live `rate` | Live `v5.1` `crypto_taker_fee_rate` | `OFFICIAL_TWAP60` vs Gamma `priceToBeat` (5 recent settled windows) |
| :--- | :--- | :--- | :---: | :---: | :--- |
| **BTC** (LIVE) | `.../btc-usd-twap-60s-streams` | `crypto_btc_5m_15m_fees` | `0.05` | **`0.05`** | `0.000000 bps` error (`5/5` exact) |
| **ETH** (LIVE) | `.../eth-usd-twap-60s-streams` | `crypto_fees_v2` | `0.01` | **`0.01`** | `0.000000 bps` error (`5/5` exact) |
| **SOL** (LIVE) | `.../sol-usd-twap-60s-streams` | `crypto_fees_v2` | `0.01` | **`0.01`** | `0.000000 bps` error (`5/5` exact) |
| **XRP** (PAPER) | `.../xrp-usd-twap-60s-streams` | `crypto_fees_v2` | `0.01` | **`0.01`** | `OFFICIAL_TWAP60` active |
| **DOGE** (PAPER) | `.../doge-usd-twap-60s-streams` | `crypto_fees_v2` | `0.01` | **`0.01`** | `OFFICIAL_TWAP60` active |

Across `31` continuous live windows recorded after the `v5.0` cutover (`2026-11-23 21:35` – `2026-11-24 00:08 +08:00`), `BTC/ETH/SOL` achieved `31/31 OFFICIAL_TWAP60` strikes and `0` `close_price` vs `resolved_outcome` mismatches. Spot parity against Phase 4.2 (`live_3coin_42.db`) at `ts=1795449780` matched `cl_spot` to 10 significant digits (`BTC 62599.6949423572`, `ETH 1763.51821171186`, `SOL 75.294013994515`).

### `v5.1` Core Parameters ([`config.py`](./polymarket-btc-5m/src/config.py))

| Parameter | Value | Role / Rationale |
| :--- | :--- | :--- |
| `entry_start_elapsed_sec` / `entry_end_elapsed_sec` | `150.0s` / `255.0s` | Late-window zone (`[T+150s, T+255s]`) |
| `min_z_score` | `1.30` | Applied to **both** `|z_CL|` and `|z_BN_adj|` |
| `e4_dual_consensus` | `True` | Fail-closed: requires `bn_raw_z` (`>=10` basis samples in 60s) & matching sign |
| `vol_lookback_sec` / `min_1s_vol_floor` | `1800` / `0.000035` | 30m pure Binance 1s closes (REST warmup `<= 1,850` bars on boot) |
| `use_twap_projection` | `True` (`0.70` Gauss + `0.30` t`df=6`) | Models final-60s TWAP variance reduction & `realized_twap240` bridge |
| `skip_book_calibrated_windows` / `require_official_oracle_strike` | `True` / `True` | Only `OFFICIAL_TWAP60` or `GAMMA_OFFICIAL` may open new entries |
| `min_true_prob` / `max_model_market_divergence` | `0.80` / `0.25` | Matches Phase 4.2 `v2_filtered` / `E4` |
| `min_entry_price` / `max_entry_price` / `max_entry_spread` | `0.68` / `0.82` / `0.06` | Depth-aware VWAP ask band & vacuum book rejection |
| `min_net_edge` | `0.035` (`3.5¢/sh`) | Computed with exact coin fee (`0.05` BTC / `0.01` alts) |
| `min_orderflow_score` / `min_directional_cvd` / `max_adverse_oracle_gap_bps` | `0.0` / `-0.05` / `6.0` | CVD-only orderflow + max 6 bps adverse CEX-Chainlink gap |
| `take_profit_bid` / Maker TP | `0.98` / `$0.990` post-only | EV-consistent exit + 0% fee Maker resting TP once bid `>= 0.94` |
| `stop_loss_z_score` / `enable_book_stop` | `-0.35` / `False` | Confirmed breakdown (`dir_z < -0.35` & `dir_flow < -0.25` & `spread <= 8¢`) |
| Sizing & Shared Risk (`shared_risk.py`) | `10%` proportional (min `5.0` sh), `max_coins=1`, `-30%` daily cap | Cross-coin mutex lock + atomic shared daily drawdown guard |

---

## 3. Multi-Day Live & Paper Performance (`2026-11-21` – `2026-11-23`)

Full per-trade records are exported (without wallet/order/secret identifiers) in:
- Live CSV (`22` trades): [`results/live_trades_20261121_20261123.csv`](./results/live_trades_20261121_20261123.csv)
- XRP/DOGE Paper CSV (`8` trades): [`results/paper_trades_xrp_doge_20261122_20261123.csv`](./results/paper_trades_xrp_doge_20261122_20261123.csv)

### Summary by Asset (Pre-`v5.1` Baseline Archive)

| Asset & Mode | Trades | Wins / Losses | Win Rate | Net PnL (pUSD) | Note |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **BTC (LIVE)** | `8` | `5W / 3L` | `62.5%` | `-7.4267` | All 3 losses had non-`OFFICIAL_TWAP60` strikes |
| **ETH (LIVE)** | `4` | `4W / 0L` | `100.0%` | `+3.4433` | Clean `4/4` (`EARLY_TP` + `MAKER_TP`) |
| **SOL (LIVE)** | `10` | `7W / 3L` | `70.0%` | `-3.2668` | `#9` (`-$1.86`) blocked by `E4`; `#10` (`-$4.29`) settled DOWN |
| **LIVE TOTAL (`BTC+ETH+SOL`)** | **`22`** | **`16W / 6L`** | **`72.7%`** | **`-7.2502`** | Live wallet `pUSD`: **`$36.26`** (Nov 23 start `$42.23`, daily `-6.15`) |
| **XRP (PAPER)** | `8` | `7W / 1L` | `87.5%` | `+2.1856` | Includes 1st `v5.0 E4` trade `#8` (`+$0.86`) |
| **DOGE (PAPER)** | `0` | `0W / 0L` | `—` | `0.0000` | Liquidity/spread starved |

### Complete 22 LIVE Trades (`BTC/ETH/SOL`)

| Coin | # | Entry (`+08:00`) | Window TS | Side | Elapsed | Shares | Ask / Fee | `P_true` / `Z` | Exit Reason (`Pay`) | Outcome | PnL (`pUSD`) | `strike_source` |
| :--- | :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :--- | :---: | :---: | :--- |
| BTC | 1 | `11-21 13:58:30` | `1795240500` | DOWN | `210.4s` | `5.51` | `0.760 / .0128` | `87.9% / 1.42` | `EARLY_TAKE_PROFIT` (`0.968`) | DOWN | `+1.0761` | `POLYBOLT_CHAINLINK` |
| SOL | 1 | `11-21 17:32:46` | `1795253400` | DOWN | `166.7s` | `5.44` | `0.790 / .0116` | `88.4% / 1.31` | `EARLY_TAKE_PROFIT` (`0.989`) | DOWN | `+1.0217` | `POLYBOLT_CHAINLINK` |
| BTC | 2 | `11-21 17:33:06` | `1795253400` | DOWN | `186.6s` | `5.23` | `0.790 / .0116` | `88.6% / 1.32` | `EARLY_TAKE_PROFIT` (`0.979`) | DOWN | `+0.9254` | `POLYBOLT_CHAINLINK` |
| BTC | 3 | `11-21 20:42:43` | `1795264800` | DOWN | `163.2s` | `5.69` | `0.791 / .0116` | `88.4% / 1.33` | `EMERGENCY_STOP_LOSS` (`0.198`) | UP | **`-3.4376`** | `BOOK_CALIBRATED` |
| ETH | 1 | `11-21 20:43:18` | `1795264800` | DOWN | `198.9s` | `5.33` | `0.780 / .0120` | `87.7% / 1.31` | `EARLY_TAKE_PROFIT` (`0.979`) | DOWN | `+0.9953` | `BOOK_CALIBRATED` |
| BTC | 4 | `11-21 22:13:41` | `1795270200` | DOWN | `221.8s` | `5.68` | `0.750 / .0131` | `88.9% / 1.31` | `EARLY_TAKE_PROFIT` (`0.989`) | DOWN | `+1.2847` | `BOOK_CALIBRATED` |
| ETH | 2 | `11-21 23:52:37` | `1795276200` | DOWN | `158.0s` | `5.37` | `0.820 / .0103` | `89.2% / 1.33` | `EARLY_TAKE_PROFIT` (`0.979`) | DOWN | `+0.7958` | `POLYBOLT_CHAINLINK` |
| BTC | 5 | `11-22 00:22:52` | `1795278000` | DOWN | `173.0s` | `5.53` | `0.810 / .0108` | `89.2% / 1.33` | `SETTLED_UP` (`0.000`) | UP | **`-4.5396`** | `BOOK_CALIBRATED` |
| SOL | 2 | `11-22 00:58:05` | `1795280100` | DOWN | `185.6s` | `5.06` | `0.816 / .0105` | `86.4% / 1.33` | `EARLY_TAKE_PROFIT` (`0.957`) | DOWN | `+0.6609` | `POLYBOLT_TWAP_T0` |
| BTC | 6 | `11-22 11:08:20` | `1795316700` | UP | `200.1s` | `5.04` | `0.820 / .0103` | `91.1% / 1.62` | `EARLY_TAKE_PROFIT` (`0.979`) | UP | `+0.7469` | `POLYBOLT_CHAINLINK` |
| ETH | 3 | `11-22 12:17:56` | `1795320900` | DOWN | `176.7s` | `5.16` | `0.810 / .0108` | `89.8% / 1.53` | `EARLY_TAKE_PROFIT` (`0.957`) | DOWN | `+0.7046` | `BOOK_CALIBRATED` |
| SOL | 3 | `11-22 12:38:51` | `1795322100` | DOWN | `231.7s` | `5.33` | `0.800 / .0112` | `95.3% / 1.97` | `SETTLED_DOWN` (`1.000`) | DOWN | `+1.0054` | `POLYBOLT_CHAINLINK` |
| BTC | 7 | `11-22 15:58:13` | `1795334100` | DOWN | `193.7s` | `5.66` | `0.770 / .0124` | `88.7% / 1.37` | `SETTLED_UP` (`0.000`) | UP | **`-4.4302`** | `POLYBOLT_CHAINLINK` |
| SOL | 4 | `11-22 16:18:38` | `1795335300` | UP | `218.5s` | `5.05` | `0.810 / .0108` | `88.6% / 1.33` | `EARLY_TAKE_PROFIT` (`0.989`) | UP | `+0.8510` | `BOOK_CALIBRATED` |
| SOL | 5 | `11-22 17:03:02` | `1795338000` | DOWN | `182.4s` | `5.44` | `0.780 / .0120` | `93.8% / 1.54` | `EMERGENCY_STOP_LOSS` (`0.160`) | UP | **`-3.4349`** | `OFFICIAL_TWAP60` |
| BTC | 8 | `11-22 18:37:32` | `1795343700` | UP | `152.2s` | `5.30` | `0.800 / .0112` | `90.2% / 1.32` | `MAKER_TAKE_PROFIT` (`0.990`) | UP | `+0.9476` | `OFFICIAL_TWAP60` |
| ETH | 4 | `11-22 18:37:39` | `1795343700` | UP | `159.1s` | `5.30` | `0.800 / .0112` | `92.1% / 1.41` | `MAKER_TAKE_PROFIT` (`0.990`) | UP | `+0.9476` | `OFFICIAL_TWAP60` |
| SOL | 6 | `11-22 18:43:32` | `1795344000` | UP | `212.3s` | `5.11` | `0.830 / .0099` | `91.1% / 1.33` | `MAKER_TAKE_PROFIT` (`0.990`) | UP | `+0.7669` | `OFFICIAL_TWAP60` |
| SOL | 7 | `11-22 19:13:03` | `1795345800` | UP | `183.6s` | `5.17` | `0.820 / .0103` | `94.9% / 1.65` | `EARLY_TAKE_PROFIT` (`0.979`) | UP | `+0.7668` | `OFFICIAL_TWAP60` |
| SOL | 8 | `11-22 19:19:05` | `1795346100` | DOWN | `245.2s` | `5.51` | `0.750 / .0131` | `97.3% / 1.99` | `MAKER_TAKE_PROFIT` (`0.990`) | DOWN | `+1.2493` | `OFFICIAL_TWAP60` |
| SOL | 9 | `11-23 09:08:02` | `1795395900` | DOWN | `182.2s` | `5.33` | `0.780 / .0120` | `96.4% / 1.82` | `EMERGENCY_STOP_LOSS` (`0.443`) | DOWN | **`-1.8635`** | `OFFICIAL_TWAP60` |
| SOL | 10 | `11-23 11:37:46` | `1795404900` | UP | `166.5s` | `5.11` | `0.830 / .0099` | `94.5% / 1.64` | `SETTLED_DOWN` (`0.000`) | DOWN | **`-4.2904`** | `OFFICIAL_TWAP60` |

### Key Takeaways from Loss Forensics

- **100% of BTC Losses Had Unofficial Strikes**: Every BTC loss (`#3 -$3.44`, `#5 -$4.54`, `#7 -$4.43`) occurred under `BOOK_CALIBRATED` or `POLYBOLT_CHAINLINK` spot before `OFFICIAL_TWAP60` / `require_official_oracle_strike=True`.
- **Single-Oracle Fakeout (`SOL #9`)**: At `1795395900` (`09:08:02`), `z_CL` hit `-1.72` while `z_BN` was only `-0.99`. The trade entered at `0.78`, whipsawed into `EMERGENCY_STOP_LOSS` (`-$1.86`), and ultimately settled `DOWN`. `E4` dual-consensus (`min(|z_CL|, |z_BN|) >= 1.30`) directly blocks this class of single-venue divergence.
- **Cross-Coin Concurrent Entry Fix (`11-22 18:37`)**: Prior to adding `try_acquire_window_slot(..., max_coins=1)`, `BTC #8` and `ETH #4` fired 7 seconds apart inside `1795343700`. Window slot locking is now enforced under `entry_lock`.

---

## 4. Phase 4.2 Pure OOS Exploration Grid (`11/21 00:00 UTC` – `11/23 20:55 +08:00`)

Replayed over `731` post-cutover windows (`2.54` days) in `live_3coin_42.db` (`exploration_grid.py --start-ts 1795219200`, +1s depth-aware FAK fills, 3-model `frozen_config.json` parity `100.0%` verified: `v2_filtered 106/106`, `v2_pure 192/192`, `v1 10/10`):

| Variant | Signals | Filled (`+1s`) | Symbol Mix | Trades/Day | Win Rate | Mean PnL/Share | SE | EV / Stake | MaxDD |
| :--- | :---: | :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **`E4` (`min(\|z_CL\|, \|z_BN\|) >= 1.30`)** | **`13`** | **`11`** | `8 ETH + 3 BTC` | `4.33` | **`100.0%` (`11/11`)** | **`+0.20043`** | `0.00856` | **`+25.07%`** | **`0.00%`** |
| `BASE` (`v2_filtered`) | `41` | `27` | `22 ETH + 5 BTC` | `10.64` | `81.48%` (`22/27`) | `+0.04376` | `0.06893` | `+5.54%` | `8.76%` |

- Combined with `19/19` pre-`11/21` `E4` sample = **`30/30 (100%)`** across Phase 4.2 recordings (still exploratory: `n=30 < 300` Bonferroni gate; `Null Max` across the grid is `0.0166` / `1.94*SE`).

---

## 5. Companion Evaluations (`XRP` / `DOGE` & Weather `NO-GO`)

- **`XRP` & `DOGE` (Evaluated on `339` `OFFICIAL_TWAP60` recorded windows -> `NO-GO` for LIVE, kept `PAPER v5.1`)**:
  - `XRP`: `spread_p50 = 2.2¢`, `p90 = 4.8¢`, `<= 6¢ = 94.5%`; `v2_filtered` `4/6 = 66.7%` (`-0.1263/sh` at `1%` fee); **`E4` `0/0`**.
  - `DOGE`: `spread_p50 = 5.1¢`, `p90 = 11.7¢`, `<= 6¢ = 58.6%`, 5m Gamma vol `~$17` (illiquid book); `v2_filtered` `1/1 = 100%`; **`E4` `0/0`**.
- **Polymarket Weather (`route1_weather` `$40` Paper Audit -> `NO-GO`)**:
  - Stopped scanning after `2026-11-20 01:50:23 UTC`; `$23.99` locked across `4` open bets hit `SKIPPED_RISK_CAP` for all `36` remaining candidates.
  - Official Gamma settlements across `4` open bets (`2W/2L`, `+$1.06`) + top `12` skipped candidates (`5W/7L`, `+$1.71`, carried by 1 `Beijing +$24.34` outlier): **`7/16 = 43.8%` actual WR vs `78.2%` mean model probability**.
  - **Brier Score: `0.3354` (model) vs `0.1943` (Polymarket market)** — model severely overconfident vs market pricing (`NO-GO`).

---

## 6. Verification Proof (`v5.1`)

- **Root test suite** (`pytest -q test_*.py`): **`115 passed in 1.17s`** (includes parametrized cross-5-coin `test_v51_official_oracle_strike_and_live_fee_schedule`).
- **Coin suites** (`polymarket-{btc,eth,sol}-5m`): **`12 passed in 0.05s`** each.
- **Strict static quality gate** (`audit-code --strict` on `test_upgrades_v47.py`, `test_live_sizing_risk.py`, `polymarket-btc-5m/src/config.py`): **`PASS (100/100)`**.
- **Production soak check (`2026-11-24 00:20 +08:00`)**: All 5 `launchd` agents (`BTC/ETH/SOL` LIVE + `XRP/DOGE` PAPER) booted clean as `v5.1` (`recent_errs=0`, `open_trades=0`, HK VPN geoblock `blocked=false`), locked `OFFICIAL_TWAP60` on rollover `1795450800` with pre-warmed 30m `vol_1s` and non-null `z_score`/`p_true_up`, while preserving `.tg_control.json` `"paused": true`.
