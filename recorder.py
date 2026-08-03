"""TickRecorder — 把 books + trades raw tick 存成 JSONL 檔。

- 每行一個 record: {"channel", "ts", "data"}
- **快路徑零 I/O**: record() 只把 (channel, ts, data) 丟進記憶體佇列就返回 —
  序列化 (json.dumps) 與磁碟寫入/flush 全在背景 writer thread 做,
  行情 callback thread 不再等磁碟 (2026-08 修: 原本每筆 tick 都同步寫檔+flush,
  觸發市價追的那筆 tick 要先等一次磁碟 I/O)。
- 批次 flush: writer 每 drain 一批寫完 flush 一次 (最多 ~1s 延遲可 tail)。
- 佇列滿 (磁碟跟不上) → 丟棄該筆 + 計數,不 block 行情線程。

輸出檔: output/YYYY-MM-DD_ticks.jsonl
"""
from __future__ import annotations

import json
import logging
import queue
import threading
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# 佇列上限 — 正常磁碟永遠追得上;滿了代表磁碟卡死,寧可丟 tick 也不卡行情
_QUEUE_MAX = 200_000
# writer 空轉時等多久再醒 (也是最壞 flush 延遲)
_DRAIN_INTERVAL_SEC = 1.0


class TickRecorder:
    def __init__(self, output_path: Path, flush_every: int = 1):
        # flush_every 參數保留相容舊呼叫 (批次模式下已無意義)
        self.output_path = output_path
        self._queue: "queue.Queue[tuple]" = queue.Queue(maxsize=_QUEUE_MAX)
        self._count = 0            # 已收筆數 (GIL 下 += 足夠精確,顯示用)
        self._dropped = 0          # 佇列滿丟棄筆數
        self._stop = threading.Event()
        self._file = open(output_path, "a", encoding="utf-8")
        self._writer_thread = threading.Thread(
            target=self._writer_loop, name="recorder-writer", daemon=True)
        self._writer_thread.start()
        logger.info(f"[recorder] 開始寫 {output_path} (背景批次寫檔,最壞延遲 ~1s)")

    def record(self, channel: str, data):
        """存一筆 tick — **零 I/O 零序列化**,只入佇列 (行情 callback thread 安全)。"""
        try:
            self._queue.put_nowait(
                (channel, datetime.now().isoformat(timespec="microseconds"), data))
            self._count += 1
        except queue.Full:
            self._dropped += 1
            if self._dropped == 1 or self._dropped % 10_000 == 0:
                logger.error(f"[recorder] ⚠ 佇列滿 (磁碟跟不上?) — 已丟 {self._dropped} 筆 tick")

    def _writer_loop(self):
        while not (self._stop.is_set() and self._queue.empty()):
            wrote = self._drain_once(timeout=_DRAIN_INTERVAL_SEC)
            if wrote:
                try:
                    self._file.flush()
                except Exception as e:
                    logger.error(f"[recorder] flush 失敗: {e}")

    def _drain_once(self, timeout: float) -> int:
        """把佇列現有的 tick 全部序列化寫出。回寫出筆數。"""
        try:
            item = self._queue.get(timeout=timeout)
        except queue.Empty:
            return 0
        wrote = 0
        while True:
            try:
                line = self._serialize(*item)
                if line is not None:
                    self._file.write(line + "\n")
                    wrote += 1
            except Exception as e:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"[recorder] 寫入失敗: {e}")
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                return wrote

    @staticmethod
    def _serialize(channel: str, ts: str, data):
        try:
            payload = TickRecorder._to_serializable(data)
            return json.dumps({"channel": channel, "ts": ts, "data": payload},
                              ensure_ascii=False, default=str)
        except Exception:
            return None

    def close(self):
        self._stop.set()
        self._writer_thread.join(timeout=5)   # writer 會把殘餘 drain 完才退出
        try:
            self._file.flush()
            self._file.close()
        except Exception:
            pass
        msg = f"[recorder] 關檔，共收 {self._count} 筆 tick 到 {self.output_path}"
        if self._dropped:
            msg += f" (佇列滿丟棄 {self._dropped} 筆)"
        logger.info(msg)

    def count(self) -> int:
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
