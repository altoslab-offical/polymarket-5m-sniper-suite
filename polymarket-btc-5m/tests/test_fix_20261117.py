"""Regression tests for the 2026-11-17 fixes (strike-uncertainty skip, optional book stop)."""
import os
import tempfile
import unittest
from dataclasses import replace

from run_monitor import _acquire_single_instance_lock
from src.config import DEFAULT_CONFIG
from src.pricing_model import estimate_fair_value
from src.strategy import HighProbSniperStrategy, OrderBookTop


def _fv(strike, cl, remaining):
    return estimate_fair_value(strike_price=strike, chainlink_price=cl, remaining_sec=remaining,
                               vol_1s=0.00005, orderflow_score=0.0, dynamic_gap_bps=0.0)


class TestFix20261117(unittest.TestCase):
    def test_defaults(self):
        self.assertFalse(DEFAULT_CONFIG.skip_book_calibrated_windows)
        self.assertFalse(DEFAULT_CONFIG.enable_book_stop)

    def test_single_instance_lock_blocks_duplicate_monitor(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_file = os.path.join(tmp, "live_btc_5m.db")
            lock1 = _acquire_single_instance_lock(db_file)
            try:
                with self.assertRaises(RuntimeError):
                    _acquire_single_instance_lock(db_file)
            finally:
                lock1.close()
            lock2 = _acquire_single_instance_lock(db_file)
            lock2.close()

    def _enter(self, cfg):
        s = HighProbSniperStrategy(cfg)
        w = 1_794_859_500
        # DOWN setup: Chainlink $60 below strike at 200s elapsed, DOWN ask $0.80
        msg = s.evaluate_tick(w + 200, w, 200.0, _fv(65_060.0, 65_000.0, 100.0), OrderBookTop(0.18, 0.19, 0.79, 0.80))
        self.assertIsNotNone(msg)
        self.assertEqual(s.active_position.side, "DOWN")
        return s, w

    def test_window_marked_uncertain_never_enters(self):
        s = HighProbSniperStrategy(DEFAULT_CONFIG)
        w = 1_794_859_500
        s.traded_windows.add(w)  # what run_monitor does on BOOK_CALIBRATED
        self.assertIsNone(s.evaluate_tick(w + 200, w, 200.0, _fv(65_060.0, 65_000.0, 100.0),
                                          OrderBookTop(0.18, 0.19, 0.79, 0.80)))
        self.assertIsNone(s.active_position)

    def test_book_stop_off_by_default_holds_position(self):
        s, w = self._enter(DEFAULT_CONFIG)
        # market flips: DOWN bid collapses to 0.30, model (wrong strike) still says DOWN
        s.evaluate_tick(w + 255, w, 255.0, _fv(65_060.0, 65_040.0, 45.0), OrderBookTop(0.69, 0.70, 0.30, 0.31))
        self.assertIsNotNone(s.active_position)

    def test_book_stop_when_enabled_exits(self):
        s, w = self._enter(replace(DEFAULT_CONFIG, enable_book_stop=True))
        msg = s.evaluate_tick(w + 255, w, 255.0, _fv(65_060.0, 65_040.0, 45.0), OrderBookTop(0.69, 0.70, 0.30, 0.31))
        self.assertIn("BOOK_STOP_LOSS", msg)
        self.assertIsNone(s.active_position)


if __name__ == "__main__":
    unittest.main()
