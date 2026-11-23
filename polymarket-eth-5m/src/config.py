"""
Configuration parameters for Polymarket ETH 5m "Up or Down" High-Win-Rate Strategy.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class StrategyConfig:
    # --- Polymarket Window & Fee Mechanics ---
    window_seconds: int = 300               # 5-minute candles (288 windows/day)
    crypto_taker_fee_rate: float = 0.01     # Fee = C * 0.01 * p * (1 - p) [Polymarket crypto_fees_v2]
    maker_rebate_share: float = 0.20        # 20% of taker fee pool redistributed to makers

    # --- Entry Window (Calibrated on 400+ live Polymarket 5m windows) ---
    # Wait out the noisy first 2.5 minutes (110s-150s had 100% of early-reversal losses in live replay)
    entry_start_elapsed_sec: float = 150.0  # Start scanning at 2m30s elapsed (<=150s left)
    entry_end_elapsed_sec: float = 255.0    # Stop new entries at 4m15s elapsed (45s left)

    # --- High-Probability Spatial Cushion & Pricing Filters ---
    min_z_score: float = 1.30               # E4 dual-consensus |Z_CL| & |Z_BN| >= 1.30 (aligned w/ Phase 4.2 frozen_config)
    min_true_prob: float = 0.80             # Model fat-tail win prob must be >= 80%
    min_entry_price: float = 0.68           # Avoid low-conviction chop
    max_entry_price: float = 0.82           # Cap <= $0.82 (~$0.83 w/ fee) to prevent 6.5:1 adverse payoff skew
    max_entry_spread: float = 0.06          # Reject entry if CLOB bid-ask spread > 6c (illiquid/vacuum book)
    min_net_edge: float = 0.035             # P_true - (Ask + TakerFee) >= 3.5%
    max_model_market_divergence: float = 0.25  # Reject entry if P_true - Ask > 25% (aligned with OFFICIAL_TWAP60)

    # --- Strike (Price-to-Beat) Uncertainty Guard [2026-11-18 consensus] ---
    # 跨盤實證: BOOK_CALIBRATED 在 ETH 與官方結算一致率為 92.6% (高於未校正 t=0 的 86.3%)，
    # 保留盤口基準價校正並關閉整盤封鎖 (搭配 0.000070 波動率地板過濾假突破)。
    skip_book_calibrated_windows: bool = True
    require_official_oracle_strike: bool = True  # v5.1: Enforce OFFICIAL_TWAP60 / GAMMA_OFFICIAL strike only

    # --- Heavy-Tail / Volatility Calibration ---
    student_t_df: float = 4.5               # Degrees of freedom for ETH 5m fat-tail jumps
    vol_lookback_sec: int = 1800            # 30-minute rolling 1s Binance close realized vol (Phase 4.2 parity)
    min_1s_vol_floor: float = 0.000035      # Phase 4.2 v2_filtered vol floor
    use_twap_projection: bool = True        # Exact 60s TWAP variance-reduction & final-60s integral
    twap_gaussian_weight: float = 0.70      # Blend weight on Gaussian CDF in TWAP projection
    twap_student_t_df: float = 6.0          # Student-t tail df for TWAP projection
    e4_dual_consensus: bool = True          # Phase 4.2 E4: min(|z_CL|, |z_BN_adj|) >= min_z_score

    # --- CEX Orderflow & Chainlink Oracle-Gap Confirmation ---
    cvd_window_sec: int = 20                # 20-second Cumulative Volume Delta lookback
    min_orderflow_score: float = 0.0        # Must be >= 0 in trade direction (no counter-dump)
    min_directional_cvd: float = -0.05      # Block entry if real CEX market-order CVD opposes trade (anti-OBI spoof)
    max_adverse_oracle_gap_bps: float = 6.0 # Block entry if CEX leads against Chainlink by > 6 bps (0.06%)

    # --- Intraday Exit & Smart Take-Profit ---
    enable_emergency_exit: bool = True
    stop_loss_z_score: float = -0.35        # Exit ONLY on confirmed structural break below strike (-0.35 sigma)
    # 盤口停損 (實驗性，預設關閉): 持倉方向 bid 跌破 book_stop_bid 就賣出。
    # 真實資料重播顯示 0.40 停損會把 62 筆中 12 筆砍掉，其中多數最後會贏，總損益 +170 -> +102，故不啟用。
    enable_book_stop: bool = False
    book_stop_bid: float = 0.40
    book_stop_max_spread: float = 0.10
    take_profit_bid: float = 0.98          # Lock profit at >=$0.98 ($0.979 net, saves ~55c/win vs $0.95 while auto-recycling pUSD)

    # --- Bankroll & Risk Management (Fixed Bet or Fractional Kelly) ---
    initial_bankroll_usdc: float = 1000.0
    fixed_bet_usdc: float = 25.0            # Fixed 25 USDC/pUSD per trade (if <= 0, falls back to Fractional Kelly)
    kelly_fraction: float = 0.20            # 1/5th Kelly for smooth equity curve
    max_risk_per_window_pct: float = 0.05   # Cap outlay at 5% of bankroll per 5m window
    proportional_bet_pct: float = 0.0       # Proportional stake fraction of live equity (e.g. 0.10 = 10%)
    min_order_shares: float = 5.0           # Polymarket minimum order size (shares)


DEFAULT_CONFIG = StrategyConfig()
