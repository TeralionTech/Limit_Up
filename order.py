"""Order Client — 目前只做「純 log 模擬」，未來換真富邦 API 只換 class impl.

記錄每筆 order 事件到 CSV，含 API 送出 → 被接受的時間差 (latency_ms)。
模擬階段 latency 是 code overhead 近乎 0，但保留欄位讓未來 A/B 對比。
"""
from __future__ import annotations

import csv
import logging
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class SimulatedOrderClient:
    """所有 order call 只 log 不打真 API。CSV 存於 output/YYYY-MM-DD_orders.csv"""

    def __init__(self, log_path: Path):
        self.log_path = log_path
        self._lock = threading.Lock()
        self._order_id_counter = 0
        # 開檔寫 header (若不存在)
        new_file = not log_path.exists()
        self._file = open(log_path, "a", newline="", encoding="utf-8")
        self._writer = csv.writer(self._file)
        if new_file:
            self._writer.writerow([
                "order_id", "ts_sent", "ts_accepted", "latency_ms",
                "action", "symbol", "lots", "price_type", "extra",
            ])
            self._file.flush()
        logger.info(f"[order] SimulatedOrderClient 開檔 {log_path}")

    def _next_order_id(self) -> str:
        with self._lock:
            self._order_id_counter += 1
            return f"SIM-{self._order_id_counter:06d}"

    def _write_row(self, order_id: str, ts_sent: float, ts_accepted: float,
                    action: str, symbol: str, lots: int, price_type: str,
                    extra: str = ""):
        latency_ms = round((ts_accepted - ts_sent) * 1000, 3)
        row = [
            order_id,
            datetime.fromtimestamp(ts_sent).isoformat(timespec="microseconds"),
            datetime.fromtimestamp(ts_accepted).isoformat(timespec="microseconds"),
            latency_ms, action, symbol, lots, price_type, extra,
        ]
        with self._lock:
            self._writer.writerow(row)
            self._file.flush()

    # ─── Public API ────────────────────────────────────────

    def place_market_buy(self, symbol: str, lots: int) -> str:
        order_id = self._next_order_id()
        ts_sent = time.time()
        # 模擬 broker 處理 = 立刻回 (真 API 會有 network + broker delay)
        ts_accepted = time.time()
        self._write_row(order_id, ts_sent, ts_accepted,
                        "BUY", symbol, lots, "MARKET")
        logger.info(f"[SIM ORDER] BUY {symbol} × {lots} 張 @ MARKET → {order_id}")
        return order_id

    def place_market_sell(self, symbol: str, lots: int, reason: str = "") -> str:
        order_id = self._next_order_id()
        ts_sent = time.time()
        ts_accepted = time.time()
        self._write_row(order_id, ts_sent, ts_accepted,
                        "SELL", symbol, lots, "MARKET", reason)
        logger.info(f"[SIM ORDER] SELL {symbol} × {lots} 張 @ MARKET ({reason}) → {order_id}")
        return order_id

    def cancel(self, order_id: str, symbol: str, reason: str = "") -> None:
        ts_sent = time.time()
        ts_accepted = time.time()
        self._write_row(order_id, ts_sent, ts_accepted,
                        "CANCEL", symbol, 0, "-", reason)
        logger.info(f"[SIM ORDER] CANCEL {order_id} ({symbol}) reason={reason}")

    def close(self):
        with self._lock:
            try:
                self._file.flush()
                self._file.close()
            except Exception:
                pass
