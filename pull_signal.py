"""Pure streaming equivalent of risk_bt.Feats.pull_at (all quantities in lots).

Call on_book only after the receiver's evaluation lag. Trades must precede that
evaluation, not necessarily the arrival of the book. No clocks, locks or I/O here.
"""
from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass, replace
from datetime import time
import math


@dataclass(frozen=True)
class PullExitConfig:
    mode: str = "off"
    window_sec: float = 10.0
    min_pct: float = 30.0
    trade_mult: float = 2.0
    min_lots: float = 20.0
    grace_sec: float = 5.0
    eval_lag_sec: float = 1.0
    max_gap_sec: float = 5.0
    fleet_nodes: int = 3
    cutoff: str = "13:20:00"

    def to_dict(self):
        return asdict(self)

    def updated(self, values):
        allowed = {"mode", "grace_sec", "eval_lag_sec", "max_gap_sec", "fleet_nodes", "cutoff"}
        # The four backtest thresholds are fixed; accept their unchanged values
        # when restoring a complete persisted snapshot.
        for key, value in values.items():
            if key not in allowed and (key not in self.to_dict() or value != getattr(self, key)):
                raise ValueError(f"不可設定 pull_exit.{key}")
        cfg = replace(self, **values)
        if cfg.mode not in ("off", "shadow", "on"):
            raise ValueError("mode 必須是 off / shadow / on")
        for key, low, high in (("grace_sec", 0, 300), ("eval_lag_sec", 0.01, 30),
                               ("max_gap_sec", 0.1, 60)):
            v = getattr(cfg, key)
            if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or not low <= v <= high:
                raise ValueError(f"{key} 必須介於 {low} 與 {high}")
        if isinstance(cfg.fleet_nodes, bool) or not isinstance(cfg.fleet_nodes, int) or not 1 <= cfg.fleet_nodes <= 100:
            raise ValueError("fleet_nodes 必須是 1..100 的整數")
        try:
            cutoff = time.fromisoformat(cfg.cutoff)
            if len(cfg.cutoff) != 8 or not time(9) < cutoff <= time(13, 20):
                raise ValueError
        except (TypeError, ValueError):
            raise ValueError("cutoff 必須是 HH:MM:SS，晚於 09:00 且不晚於 13:20:00") from None
        return cfg


class PullDetector:
    def __init__(self, window_sec=10, min_pct=30, trade_mult=2, min_lots=20, grace_sec=5):
        self.window_sec = window_sec
        self.min_pct = min_pct
        self.trade_mult = trade_mult
        self.min_lots = min_lots
        self.grace_sec = grace_sec
        self.reset()

    def reset(self):
        self.armed_at = None
        self.fired = False
        self.last_book_ts = None
        self.last_trade_ts = None
        self.late_trades = 0
        self._trades = deque()
        self._steps = deque()
        self._volume = 0.0
        self._prev = None
        self._q_before = None
        self._cancels = self._window_trades = 0.0

    def on_trade(self, ts, size_lots, cum_volume_lots=None):
        """Return accepted / late / unordered. Late prints never revise history.

        Each channel is monotonic. An out-of-order print *inside* the unsealed
        interval requests a conservative new baseline from the caller (O(1), no
        hot-path sorting). Cross-channel arrival order is unrestricted.
        """
        if self.last_book_ts is not None and ts <= self.last_book_ts:
            self.late_trades += 1
            return "late"
        if self.last_trade_ts is not None and ts < self.last_trade_ts:
            return "unordered"
        self.last_trade_ts = ts
        self._trades.append((ts, max(0, size_lots), cum_volume_lots))
        return "accepted"

    def on_book(self, ts, mkt_queue_lots, *, own_cancels=0, fleet_lots=0):
        if self.fired:
            return None
        if self.last_book_ts is not None and ts < self.last_book_ts:
            raise ValueError("books must be chronological")
        while self._trades and self._trades[0][0] <= ts:
            _, size, cumulative = self._trades.popleft()
            self._volume = (self._volume + size if cumulative is None
                            else max(self._volume, cumulative))
        q = max(0, mkt_queue_lots)
        if self.armed_at is None and q >= 1:
            self.armed_at = ts
        ready = self.armed_at is not None and ts >= self.armed_at + self.grace_sec
        c = v = 0.0
        if self._prev is not None and ready:
            prev_q, prev_v = self._prev
            v = max(0, self._volume - prev_v)
            c = max(0, prev_q - q - v)
        self._prev = (q, self._volume)
        self.last_book_ts = ts
        self._steps.append((ts, q, c, v))
        self._cancels += c
        self._window_trades += v
        while self._steps and self._steps[0][0] <= ts - self.window_sec:
            _, self._q_before, old_c, old_v = self._steps.popleft()
            self._cancels -= old_c
            self._window_trades -= old_v
        # Feats uses current Q if there is no reading at/before T-W.
        qb = q if self._q_before is None else self._q_before
        net = max(0, self._cancels - own_cancels)
        if (ready and qb >= self.min_lots and net >= self.min_lots
                and net >= self.min_pct / 100 * qb
                and net >= self.trade_mult * self._window_trades and net >= fleet_lots):
            self.fired = True
            return {"ts": ts, "q_before": qb, "cancels": net,
                    "trades": self._window_trades, "window_sec": self.window_sec,
                    "armed_s": ts - self.armed_at, "raw_cancels": self._cancels,
                    "own_cancels": own_cancels, "fleet_lots": fleet_lots}
        return None


class CancelWindow:
    """Unique order quantities in (T-12, T], amortized O(1).

    A request and its confirmation overlap but only the largest outstanding
    quantity per order counts. Both can outlive the other in the window. Caller
    serializes access. T advances on the same epoch clock as local cancellations.
    """
    def __init__(self, seconds=12):
        self.seconds = seconds
        self.pending = deque()
        self.active = deque()
        self.orders = {}
        self.total = 0
        self._seq = 0
        self._last_ts = float("-inf")

    def add(self, ts, order_no, lots):
        self._seq += 1
        self.pending.append((ts, self._seq, order_no, lots))
        # Off mode may never query. Older events cannot affect a fresh baseline.
        while self.pending and self.pending[0][0] < ts - 120:
            self.pending.popleft()

    def at(self, ts):
        if ts < self._last_ts:
            # A restarted receiver may rewind exchange time. Suppress signals
            # until it catches up rather than under-subtract our cancellations.
            return float("inf")
        self._last_ts = ts
        while self.pending and self.pending[0][0] <= ts:
            event = self.pending.popleft()
            when, seq, no, lots = event
            if when <= ts - self.seconds:
                continue
            amounts = self.orders.setdefault(no, deque())
            old = amounts[0][1] if amounts else 0
            while amounts and amounts[-1][1] <= lots:
                amounts.pop()
            amounts.append((seq, lots))
            self.total += amounts[0][1] - old
            self.active.append(event)
        while self.active and self.active[0][0] <= ts - self.seconds:
            _, seq, no, _ = self.active.popleft()
            amounts = self.orders[no]
            if amounts[0][0] == seq:
                self.total -= amounts.popleft()[1]
                if amounts:
                    self.total += amounts[0][1]
                else:
                    del self.orders[no]
        return self.total
