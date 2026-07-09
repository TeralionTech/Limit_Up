"""TickRecorder — 把 books + trades raw tick 存成 JSONL 檔。

- 每行一個 record: {"channel", "ts", "data"}
- Thread-safe (多 socket callback 併發寫同個 file)
- 每 1000 筆 flush 一次 (避免 crash 掉太多)

輸出檔: output/YYYY-MM-DD_ticks.jsonl
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from threading import Lock

logger = logging.getLogger(__name__)


class TickRecorder:
    def __init__(self, output_path: Path, flush_every: int = 1):
        self.output_path = output_path
        self.flush_every = flush_every
        self._lock = Lock()
        # buffering=1 = line buffered — 每個 \n 都立刻 flush 到 disk
        # 這樣你在別的視窗 tail -f 或用文字編輯器 refresh 都看得到最新 tick
        self._file = open(output_path, "a", encoding="utf-8", buffering=1)
        self._count = 0
        logger.info(f"[recorder] 開始寫 {output_path} (line-buffered，即時可 tail)")

    def record(self, channel: str, data):
        """存一筆 tick。data 可以是 dict 或 object (自動轉 dict)。"""
        try:
            payload = self._to_serializable(data)
        except Exception as e:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"[recorder] serialize fail: {e}")
            return

        record = {
            "channel": channel,
            "ts": datetime.now().isoformat(timespec="microseconds"),
            "data": payload,
        }

        try:
            line = json.dumps(record, ensure_ascii=False, default=str)
        except Exception as e:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"[recorder] json dump fail: {e}")
            return

        with self._lock:
            self._file.write(line + "\n")
            self._count += 1
            if self._count % self.flush_every == 0:
                self._file.flush()

    def close(self):
        with self._lock:
            try:
                self._file.flush()
                self._file.close()
            except Exception:
                pass
        logger.info(f"[recorder] 關檔，共寫 {self._count} 筆 tick 到 {self.output_path}")

    def count(self) -> int:
        with self._lock:
            return self._count

    @staticmethod
    def _to_serializable(obj):
        """把 SDK 回來的 object 轉可 JSON 化的 dict / list / primitives。"""
        if obj is None:
            return None
        if isinstance(obj, (str, int, float, bool)):
            return obj
        if isinstance(obj, dict):
            return {k: TickRecorder._to_serializable(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [TickRecorder._to_serializable(x) for x in obj]
        # SDK object — 抓 __dict__ 或用 vars()
        try:
            return TickRecorder._to_serializable(vars(obj))
        except TypeError:
            return str(obj)
