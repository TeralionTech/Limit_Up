"""Receipt-time buffering and background audit output for the pull signal."""
from __future__ import annotations

from collections import deque
import csv
from datetime import datetime, timedelta, timezone
import logging
import math
from pathlib import Path
import queue
import threading
import time

from pull_signal import PullDetector

TAIPEI = timezone(timedelta(hours=8))
logger = logging.getLogger(__name__)
CSV_FIELDS = ("ts", "symbol", "mode", "q_before", "cancels", "trades", "armed_s",
              "filled_lots", "has_exposure", "action")


class PullEvents:
    """Register receipts/timers before taking any strategy or consumer lock.

    The short ingress lock only timestamps/enqueues; it never runs detection,
    session calls or I/O. One consumer processes this FIFO under the pull lock.
    Thus a queued timer cannot overtake an already registered market event.
    """
    def __init__(self):
        self._lock = threading.Lock()
        self._queue = deque()

    def put(self, kind, args=(), *, wall=None, mono=None):
        with self._lock:
            wall = time.time() if wall is None else wall
            mono = time.monotonic() if mono is None else mono
            self._queue.append((kind, args, wall, mono))

    def pop(self):
        with self._lock:
            return self._queue.popleft() if self._queue else None

    def clear(self):
        with self._lock:
            self._queue.clear()


def exchange_time(value):
    """Fubon books/trades `time` is Unix microseconds (official dump/examples).

    Invalid/missing values deliberately select receipt time for the whole stream.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not math.isfinite(value) or not 1e15 <= value < 1e16:
        return None
    return value / 1_000_000


def trading_day(ts, cutoff):
    now = datetime.fromtimestamp(ts, TAIPEI)
    clock = now.strftime("%H:%M:%S")
    return now.date().isoformat() if "09:00:00" <= clock < cutoff else None


class PullStream:
    """Caller holds Trader's pull lock; no session calls or I/O."""
    def __init__(self, cfg):
        self.cfg = cfg
        self.clock_source = None
        self.epoch = None
        self.late_count = 0
        self.reset()

    def reset(self):
        self.detector = PullDetector(self.cfg.window_sec, self.cfg.min_pct,
                                     self.cfg.trade_mult, self.cfg.min_lots, self.cfg.grace_sec)
        self.pending = deque()
        self.last_book_ts = None
        self.last_book_mono = None
        self.first_trade_mono = None

    def timestamp(self, raw, received):
        ts = exchange_time(raw)
        if self.clock_source is None:
            self.clock_source = "exchange" if ts is not None else "receipt"
        elif self.clock_source == "exchange" and ts is None:
            self.reset()
            self.clock_source = "receipt"  # Never mix or switch back during this stream.
        return ts if self.clock_source == "exchange" else received

    def check_epoch(self, epoch):
        if self.epoch is not None and epoch != self.epoch:
            self.reset()
        self.epoch = epoch

    def book(self, ts, lots, received_mono):
        if self.last_book_ts is not None:
            if ts < self.last_book_ts:
                self.reset()
                return False
            if (ts - self.last_book_ts > self.cfg.max_gap_sec
                    or received_mono - self.last_book_mono > self.cfg.max_gap_sec):
                self.reset()
        self.last_book_ts, self.last_book_mono = ts, received_mono
        self.pending.append((received_mono + self.cfg.eval_lag_sec, ts, lots))
        return True


class PullSignalWriter:
    """All new logging/CSV I/O runs here, never in a quote callback/session lock."""
    def __init__(self, output_dir):
        self.output_dir = Path(output_dir)
        self.queue = queue.Queue()
        self.thread = None
        self._lock = threading.Lock()
        self.errors = 0

    def submit(self, message, row=None):
        with self._lock:
            self.queue.put_nowait((message, row))
            if self.thread is None:
                self.thread = threading.Thread(target=self._run, name="pull-audit", daemon=True)
                self.thread.start()

    def _run(self):
        while True:
            item = self.queue.get()
            try:
                if item is None:
                    return
                message, row = item
                logger.warning(message)
                if row is not None:
                    day = datetime.fromtimestamp(row["ts"], TAIPEI).date().isoformat()
                    path = self.output_dir / f"{day}_pull_signals.csv"
                    path.parent.mkdir(parents=True, exist_ok=True)
                    header = not path.exists() or path.stat().st_size == 0
                    with path.open("a", encoding="utf-8", newline="") as handle:
                        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS, extrasaction="ignore")
                        if header:
                            writer.writeheader()
                        writer.writerow(row)
            except Exception:
                self.errors += 1
                logger.exception("[pull] 訊號紀錄寫入失敗")
            finally:
                self.queue.task_done()

    def close(self):
        with self._lock:
            thread = self.thread
            if thread is not None:
                self.queue.put_nowait(None)
        if thread is not None:
            thread.join(timeout=5)
