"""Shared proportional position sizing, safe minimum-share billing, and cross-process daily risk guard."""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import json
from pathlib import Path
import time
from typing import Any, Iterator


def compute_proportional_shares(
    bankroll_usdc: float,
    cost_per_share: float,
    bet_pct: float,
    min_shares: float = 5.0,
    cash_buffer_ratio: float = 0.96,
) -> float:
    """Compute share quantity targeting `bet_pct` of bankroll, floored at `min_shares` when affordable."""
    if bankroll_usdc <= 0.0 or cost_per_share <= 0.0 or bet_pct <= 0.0:
        return 0.0
    min_cost = float(min_shares) * float(cost_per_share)
    max_affordable = float(bankroll_usdc) * float(cash_buffer_ratio)
    if max_affordable < min_cost:
        return 0.0
    target_outlay = max(float(bankroll_usdc) * float(bet_pct), min_cost)
    capped_outlay = min(target_outlay, max_affordable)
    shares = round(capped_outlay / float(cost_per_share), 2)
    return shares if shares >= float(min_shares) else 0.0


def compute_safe_buy_usdc(
    shares: float,
    ask_price: float,
    limit_price: float,
    available_pusd: float,
    min_shares: float = 5.05,
) -> float:
    """Compute USDC outlay ensuring filled shares at worst-case `limit_price` stay above Polymarket's 5-share floor."""
    if shares <= 0.0 or ask_price <= 0.0 or limit_price <= 0.0:
        return 0.0
    base_usdc = float(shares) * float(ask_price)
    floor_usdc = float(min_shares) * float(limit_price)
    raw = max(base_usdc, floor_usdc)
    if available_pusd > 0.0:
        capped = min(raw, float(available_pusd) * 0.98)
        if capped < 5.0 * float(limit_price):
            return 0.0
        return round(capped, 2)
    return round(raw, 2)


class SharedDailyRiskGuard:
    """Cross-process file-locked daily loss circuit breaker and order mutex for BTC/ETH/SOL runners."""

    def __init__(
        self,
        state_path: Path,
        max_daily_loss_pct: float = 0.30,
        min_loss_cap_usd: float = 5.0,
    ) -> None:
        self.state_path = Path(state_path)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.state_path.with_suffix(".lock")
        self.order_lock_path = self.state_path.parent / "shared_order.lock"
        self.max_daily_loss_pct = max(0.05, min(0.95, float(max_daily_loss_pct)))
        self.min_loss_cap_usd = max(1.0, float(min_loss_cap_usd))

    @contextmanager
    def _state_lock(self) -> Iterator[None]:
        fh = open(self.lock_path, "a+b")
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
            finally:
                fh.close()

    @contextmanager
    def entry_lock(self) -> Iterator[None]:
        """Serialize concurrent live balance checks and FAK buy submissions across BTC/ETH/SOL."""
        fh = open(self.order_lock_path, "a+b")
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
            finally:
                fh.close()

    def _read_state_unlocked(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {}
        try:
            raw = json.loads(self.state_path.read_text(encoding="utf-8"))
            return raw if isinstance(raw, dict) else {}
        except (OSError, json.JSONDecodeError):
            return {}

    def _write_state_unlocked(self, state: dict[str, Any]) -> None:
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(self.state_path)

    def _build_new_day_state(self, cur_day: int, equity: float, ts: float) -> dict[str, Any]:
        eq = max(0.0, float(equity))
        cap = max(self.min_loss_cap_usd, round(eq * self.max_daily_loss_pct, 2))
        return {
            "day_utc": cur_day,
            "day_start_equity": round(eq, 4),
            "daily_realized_pnl_usdc": 0.0,
            "max_daily_loss_usdc": cap,
            "circuit_broken": False,
            "last_updated_ts": ts,
        }

    def sync_day_equity(self, live_equity: float, now_ts: float | None = None) -> dict[str, Any]:
        """Initialize or reset the shared daily loss cap at UTC 00:00 rollover."""
        ts = time.time() if now_ts is None else float(now_ts)
        cur_day = int(ts // 86400)
        with self._state_lock():
            state = self._read_state_unlocked()
            if (
                not state
                or int(state.get("day_utc", -1)) != cur_day
                or float(state.get("day_start_equity", 0.0)) <= 0.0
            ):
                state = self._build_new_day_state(cur_day, live_equity, ts)
                self._write_state_unlocked(state)
            return dict(state)

    def record_pnl(
        self,
        pnl_usdc: float,
        current_equity: float | None = None,
        now_ts: float | None = None,
    ) -> dict[str, Any]:
        """Accumulate realized PnL across all runners and latch daily circuit breaker when breached."""
        ts = time.time() if now_ts is None else float(now_ts)
        cur_day = int(ts // 86400)
        with self._state_lock():
            state = self._read_state_unlocked()
            if not state or int(state.get("day_utc", -1)) != cur_day:
                fallback_eq = float(state.get("day_start_equity") or 40.0)
                base_eq = float(current_equity) if (current_equity is not None and current_equity > 0.0) else fallback_eq
                state = self._build_new_day_state(cur_day, base_eq, ts)
            new_pnl = round(float(state.get("daily_realized_pnl_usdc") or 0.0) + float(pnl_usdc), 4)
            cap = float(state.get("max_daily_loss_usdc") or self.min_loss_cap_usd)
            broken = bool(state.get("circuit_broken", False)) or (new_pnl <= -cap)
            state["daily_realized_pnl_usdc"] = new_pnl
            state["circuit_broken"] = broken
            state["last_updated_ts"] = ts
            self._write_state_unlocked(state)
            return dict(state)

    def is_broken(self, now_ts: float | None = None) -> tuple[bool, float, float]:
        """Return `(circuit_broken, daily_realized_pnl_usdc, max_daily_loss_usdc)` for current UTC day."""
        ts = time.time() if now_ts is None else float(now_ts)
        cur_day = int(ts // 86400)
        with self._state_lock():
            state = self._read_state_unlocked()
            if not state or int(state.get("day_utc", -1)) != cur_day:
                cap = float(state.get("max_daily_loss_usdc") or self.min_loss_cap_usd)
                return False, 0.0, cap
            return (
                bool(state.get("circuit_broken", False)),
                float(state.get("daily_realized_pnl_usdc") or 0.0),
                float(state.get("max_daily_loss_usdc") or self.min_loss_cap_usd),
            )
