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

    def _build_new_day_state(
        self,
        cur_day: int,
        equity: float,
        ts: float,
        prev_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        eq = max(0.0, float(equity))
        cap = max(self.min_loss_cap_usd, round(eq * self.max_daily_loss_pct, 2))
        prev = prev_state or {}
        slots = prev.get("active_window_slots")
        dedupe = self._prune_ts_map(prev.get("alert_dedupe"), ts - 86400.0)
        active = self._prune_ts_map(prev.get("alert_active"), ts - 86400.0)
        return {
            "day_utc": cur_day,
            "day_start_equity": round(eq, 4),
            "daily_realized_pnl_usdc": 0.0,
            "max_daily_loss_usdc": cap,
            "circuit_broken": False,
            "cached_balance_usdc": float(prev.get("cached_balance_usdc") or 0.0),
            "cached_balance_ts": float(prev.get("cached_balance_ts") or 0.0),
            "pending_spend_usdc": float(prev.get("pending_spend_usdc") or 0.0),
            "active_window_slots": dict(slots) if isinstance(slots, dict) else {},
            "alert_dedupe": dedupe,
            "alert_active": active,
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
                state = self._build_new_day_state(cur_day, live_equity, ts, prev_state=state)
                self._write_state_unlocked(state)
            return dict(state)

    def record_balance(self, balance_usdc: float, now_ts: float | None = None) -> None:
        """Record a fresh pUSD wallet balance snapshot and clear pending intra-batch spend."""
        ts = time.time() if now_ts is None else float(now_ts)
        cur_day = int(ts // 86400)
        bal = round(max(0.0, float(balance_usdc)), 4)
        with self._state_lock():
            state = self._read_state_unlocked()
            if not state:
                state = self._build_new_day_state(cur_day, bal, ts)
            state["cached_balance_usdc"] = bal
            state["cached_balance_ts"] = ts
            state["pending_spend_usdc"] = 0.0
            state["last_updated_ts"] = ts
            self._write_state_unlocked(state)

    def record_spend(self, spent_usdc: float, now_ts: float | None = None) -> None:
        """Deduct filled order spend immediately so sibling runners avoid stale cached equity."""
        spend = max(0.0, float(spent_usdc))
        if spend <= 0.0:
            return
        ts = time.time() if now_ts is None else float(now_ts)
        cur_day = int(ts // 86400)
        with self._state_lock():
            state = self._read_state_unlocked()
            if not state:
                state = self._build_new_day_state(cur_day, 0.0, ts)
            prev_spend = float(state.get("pending_spend_usdc") or 0.0)
            state["pending_spend_usdc"] = round(prev_spend + spend, 4)
            state["last_updated_ts"] = ts
            self._write_state_unlocked(state)

    def get_fast_balance(
        self,
        max_age_sec: float = 25.0,
        min_safe_usdc: float = 10.0,
        now_ts: float | None = None,
    ) -> float | None:
        """Return fresh cached pUSD minus pending spend if recent and safely above `min_safe_usdc`."""
        ts = time.time() if now_ts is None else float(now_ts)
        with self._state_lock():
            state = self._read_state_unlocked()
            cached_ts = float(state.get("cached_balance_ts") or 0.0)
            if cached_ts <= 0.0 or (ts - cached_ts) > float(max_age_sec):
                return None
            eff = float(state.get("cached_balance_usdc") or 0.0) - float(
                state.get("pending_spend_usdc") or 0.0
            )
            if eff < float(min_safe_usdc):
                return None
            return round(eff, 4)

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
                state = self._build_new_day_state(cur_day, base_eq, ts, prev_state=state)
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

    @staticmethod
    def _prune_ts_map(raw: Any, cutoff: float) -> dict[str, float]:
        """Keep valid numeric timestamps at or above `cutoff`."""
        cleaned: dict[str, float] = {}
        if not isinstance(raw, dict):
            return cleaned
        for k, val in raw.items():
            try:
                num = float(val)
            except (TypeError, ValueError):
                continue
            if num >= cutoff:
                cleaned[str(k)] = num
        return cleaned

    def claim_alert(
        self,
        key: str,
        cooldown_sec: float = 1800.0,
        mark_active: bool = False,
        now_ts: float | None = None,
    ) -> bool:
        """Atomically claim a cross-process alert slot honoring `cooldown_sec`."""
        norm_key = str(key).strip().lower()
        if not norm_key:
            return False
        ts = time.time() if now_ts is None else float(now_ts)
        cur_day = int(ts // 86400)
        cd = max(0.0, float(cooldown_sec))
        with self._state_lock():
            state = self._read_state_unlocked()
            if not state:
                state = self._build_new_day_state(cur_day, 0.0, ts)
            dedupe = self._prune_ts_map(state.get("alert_dedupe"), ts - 86400.0)
            prev = dedupe.get(norm_key, 0.0)
            if prev > 0.0 and (ts - prev) < cd:
                return False
            dedupe[norm_key] = ts
            state["alert_dedupe"] = dedupe
            if mark_active:
                active = self._prune_ts_map(state.get("alert_active"), ts - 86400.0)
                active[norm_key] = ts
                state["alert_active"] = active
            state["last_updated_ts"] = ts
            self._write_state_unlocked(state)
            return True

    def resolve_alert(self, key: str, now_ts: float | None = None) -> float | None:
        """Clear an active cross-process alert and return outage duration in seconds."""
        norm_key = str(key).strip().lower()
        if not norm_key:
            return None
        ts = time.time() if now_ts is None else float(now_ts)
        with self._state_lock():
            state = self._read_state_unlocked()
            active = state.get("alert_active")
            if not isinstance(active, dict) or norm_key not in active:
                return None
            started_raw = active.pop(norm_key, None)
            state["alert_active"] = active
            state["last_updated_ts"] = ts
            self._write_state_unlocked(state)
        try:
            started_ts = float(started_raw)
        except (TypeError, ValueError):
            return 0.0
        return round(max(0.0, ts - started_ts), 1)

    @staticmethod
    def _prune_window_slots(raw_slots: Any, cutoff: int) -> dict[str, list[str]]:
        active: dict[str, list[str]] = {}
        if not isinstance(raw_slots, dict):
            return active
        for k, vals in raw_slots.items():
            try:
                if int(k) >= cutoff and isinstance(vals, list):
                    active[str(k)] = [str(v).upper() for v in vals]
            except (TypeError, ValueError):
                continue
        return active

    def try_acquire_window_slot(self, window_ts: int, coin: str, max_coins: int = 1) -> bool:
        """Atomically acquire a correlated 5m window slot up to `max_coins` simultaneous coins."""
        win_i = int(window_ts)
        win_key = str(win_i)
        coin_key = str(coin).strip().upper()
        cap = max(1, int(max_coins))
        cutoff = win_i - 600
        with self._state_lock():
            state = self._read_state_unlocked()
            active = self._prune_window_slots(state.get("active_window_slots"), cutoff)
            slots = list(active.get(win_key, []))
            if coin_key in slots:
                state["active_window_slots"] = active
                self._write_state_unlocked(state)
                return True
            if len(slots) >= cap:
                state["active_window_slots"] = active
                self._write_state_unlocked(state)
                return False
            slots.append(coin_key)
            active[win_key] = slots
            state["active_window_slots"] = active
            self._write_state_unlocked(state)
            return True

    def release_window_slot(self, window_ts: int, coin: str) -> None:
        """Release a reserved 5m window slot after an unfilled entry or completed cycle."""
        win_key = str(int(window_ts))
        coin_key = str(coin).strip().upper()
        with self._state_lock():
            state = self._read_state_unlocked()
            raw_slots = state.get("active_window_slots")
            if not isinstance(raw_slots, dict) or win_key not in raw_slots:
                return
            vals = raw_slots.get(win_key)
            if not isinstance(vals, list):
                return
            updated = [str(v).upper() for v in vals if str(v).upper() != coin_key]
            if updated:
                raw_slots[win_key] = updated
            else:
                raw_slots.pop(win_key, None)
            state["active_window_slots"] = raw_slots
            self._write_state_unlocked(state)
