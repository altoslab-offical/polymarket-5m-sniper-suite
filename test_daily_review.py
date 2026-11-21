"""Unit tests for daily non-intrusive review metrics, slippage parsing, and optimization watchlist."""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import sys
import time

import pytest

FRIEND_ROOT = Path(__file__).resolve().parent
if str(FRIEND_ROOT) not in sys.path:
    sys.path.insert(0, str(FRIEND_ROOT))

from daily_review import (  # noqa: E402
    parse_execution_slippage,
    render_markdown_report,
    run_daily_review,
)


def _create_coin_db(db_path: Path, windows_count: int, trades: list[tuple[Any, ...]]) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    con.executescript(
        """
        CREATE TABLE windows (window_ts INTEGER PRIMARY KEY, slug TEXT NOT NULL);
        CREATE TABLE paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            window_ts INTEGER NOT NULL,
            side TEXT NOT NULL,
            entry_ts REAL NOT NULL,
            entry_elapsed_sec REAL NOT NULL,
            shares REAL NOT NULL,
            entry_price REAL NOT NULL,
            entry_fee_per_share REAL NOT NULL,
            p_true_at_entry REAL NOT NULL,
            z_at_entry REAL NOT NULL,
            exit_ts REAL,
            exit_reason TEXT,
            exit_payout_per_share REAL,
            net_pnl_usdc REAL,
            won INTEGER
        );
        """
    )
    for i in range(windows_count):
        con.execute("INSERT INTO windows VALUES (?, ?)", (1795240000 + i * 300, f"slug-{i}"))
    for row in trades:
        con.execute(
            """
            INSERT INTO paper_trades (
                window_ts, side, entry_ts, entry_elapsed_sec, shares, entry_price,
                entry_fee_per_share, p_true_at_entry, z_at_entry,
                exit_ts, exit_reason, exit_payout_per_share, net_pnl_usdc, won
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            row,
        )
    con.commit()
    con.close()


def test_parse_execution_slippage_handles_cr_and_negative_slippage() -> None:
    sample_log = (
        "[WAIT  剩10s] ...\r[SNIPE 剩80s] ...\r"
        "🔴 [LIVE 實盤成交] order=0xabc | shares=5.51 @ $0.760 (含費成本 $0.7728)\n"
        "*** [ENTRY DOWN] elapsed=210s | ask=$0.770 (+fee $0.0124) | P_true=87.9% ***\n"
        "🔴 [LIVE 實盤成交] order=0xdef | shares=5.23 @ $0.790 (含費成本 $0.8016)\n"
        "*** [ENTRY DOWN] elapsed=187s | ask=$0.780 (+fee $0.0120) | P_true=88.6% ***\n"
    )
    res = parse_execution_slippage(sample_log)
    assert res["fills_matched"] == 2
    assert res["mean_slippage_cents"] == pytest.approx(0.0, abs=1e-6)
    assert res["max_slippage_cents"] == pytest.approx(1.0, abs=1e-6)


def test_daily_review_aggregates_dbs_logs_and_small_sample_guard(tmp_path: Path) -> None:
    now = time.time()
    (tmp_path / "logs").mkdir(parents=True, exist_ok=True)
    (tmp_path / "shared_live_risk.json").write_text(
        json.dumps(
            {
                "day_utc": 20778,
                "day_start_equity": 42.53,
                "daily_realized_pnl_usdc": -1.5,
                "max_daily_loss_usdc": 12.76,
                "circuit_broken": False,
            }
        ),
        encoding="utf-8",
    )

    _create_coin_db(
        tmp_path / "polymarket-btc-5m" / "data" / "live_btc_5m.db",
        windows_count=10,
        trades=[
            (1795240500, "DOWN", now - 3600, 163.0, 5.68, 0.791, 0.012, 0.88, 1.33, now - 3500, "EMERGENCY_STOP_LOSS", 0.19, -3.44, 0),
            (1795241400, "DOWN", now - 2400, 210.0, 5.51, 0.760, 0.012, 0.88, 1.55, now - 2300, "EARLY_TAKE_PROFIT", 0.98, 1.08, 1),
        ],
    )
    _create_coin_db(
        tmp_path / "polymarket-eth-5m" / "data" / "live_eth_5m.db",
        windows_count=10,
        trades=[
            (1795241700, "UP", now - 1800, 199.0, 5.33, 0.780, 0.012, 0.88, 1.31, now - 1700, "EARLY_TAKE_PROFIT", 0.98, 1.00, 1),
        ],
    )
    _create_coin_db(
        tmp_path / "polymarket-sol-5m" / "data" / "live_sol_5m.db",
        windows_count=10,
        trades=[],
    )

    (tmp_path / "logs" / "btc_live.log").write_text(
        "[WAIT] Chainlink=$58,000 (POLYBOLT_CHAINLINK)\n"
        "🔴 [LIVE 實盤成交] order=0x1 | shares=5.51 @ $0.760\n"
        "*** [ENTRY DOWN] elapsed=210s | ask=$0.770 | P_true=88% ***\n"
        "⚠️ [LIVE FAK 未成交] 流動性不足\n",
        encoding="utf-8",
    )
    (tmp_path / "logs" / "eth_live.log").write_text(
        "Traceback (most recent call last): fake\n409 Conflict telegram\n",
        encoding="utf-8",
    )
    (tmp_path / "logs" / "sol_live.log").write_text(
        "[WAIT] Chainlink=$68.1 (POLY_RTDS_CHAINLINK)\n",
        encoding="utf-8",
    )

    rep = run_daily_review(root=tmp_path, hours=24.0, no_live_api=True)
    assert rep["total_all"]["trades"] == 3
    assert rep["total_all"]["wins"] == 2
    assert rep["total_all"]["losses"] == 1
    assert rep["total_all"]["net_pnl_usdc"] == pytest.approx(-1.36, abs=1e-4)
    assert rep["elapsed_buckets"]["150-180s"]["trades"] == 1
    assert rep["elapsed_buckets"]["180-255s"]["trades"] == 2
    assert rep["z_buckets"]["1.30-1.50"]["trades"] == 2
    assert rep["z_buckets"][">1.50"]["trades"] == 1
    assert rep["slippage_summary"]["fills_matched"] == 1
    assert rep["slippage_summary"]["fak_misses"] == 1
    assert rep["slippage_summary"]["mean_slippage_cents"] == pytest.approx(-1.0, abs=1e-4)
    assert rep["per_coin_logs"]["eth"]["traceback_cnt"] == 1
    assert rep["per_coin_logs"]["eth"]["tg_409_cnt"] == 1
    assert any("OBSERVE ONLY" in note for note in rep["watchlist"])
    assert any("[INFRA]" in note for note in rep["watchlist"])

    md = render_markdown_report(rep)
    assert "Polymarket 5m 三幣實盤每日健檢" in md
    assert "`2W - 1L`" in md


def test_daily_review_emits_tune_candidate_only_with_sufficient_sample(tmp_path: Path) -> None:
    now = time.time()
    (tmp_path / "logs").mkdir(parents=True, exist_ok=True)
    early_losses = [
        (1795240000 + i * 300, "UP", now - 3600 + i * 10, 160.0, 5.0, 0.78, 0.01, 0.86, 1.35, now - 3500 + i * 10, "SETTLED", 0.0, -3.9, 0)
        for i in range(15)
    ]
    late_wins = [
        (1795250000 + i * 300, "UP", now - 1800 + i * 10, 205.0, 5.0, 0.78, 0.01, 0.90, 1.65, now - 1700 + i * 10, "EARLY_TAKE_PROFIT", 0.98, 1.0, 1)
        for i in range(15)
    ]
    _create_coin_db(tmp_path / "polymarket-btc-5m" / "data" / "live_btc_5m.db", 50, early_losses)
    _create_coin_db(tmp_path / "polymarket-eth-5m" / "data" / "live_eth_5m.db", 50, late_wins)
    _create_coin_db(tmp_path / "polymarket-sol-5m" / "data" / "live_sol_5m.db", 50, [])

    rep = run_daily_review(root=tmp_path, hours=24.0, no_live_api=True)
    assert rep["total_all"]["trades"] == 30
    assert not any("OBSERVE ONLY" in note for note in rep["watchlist"])
    assert any("[TUNE CANDIDATE]" in note for note in rep["watchlist"])
    assert any("[FEE ALIGNMENT]" in note for note in rep["watchlist"])
