"""中控轉發通知 (2026-10-13) — 把重要事件送到 teralion-central，由中控統一發 Signal 到手機。

為什麼用 logging handler 而不是在交易路徑逐處呼叫:
  斷線 / 重連 / 出場 / 隔日賣 / 緊急全平 這些事件既有程式碼**已經**用 logger 記成 CRITICAL / WARNING，
  掛一個 handler 挑規則轉發，交易路徑一行都不用動 (零相依、零鎖、失敗只會少一則通知)。

.env:
  CENTRAL_FORWARD_URL=https://central.teraliontech.com   # 空 = 整個模組 no-op
  CENTRAL_VPS_ID=21                                       # 中控 fleet 裡這台 (搶漲停那一列) 的 id (中控用它找通知標頭)
  CENTRAL_LABEL=kled                                      # 找不到 id 時的備援標頭
  CENTRAL_NOTIFY_MIN_LEVEL=INFO                           # 低於此等級的規則一律不轉 (預設 INFO)

轉發規則 (``should_forward``):
  CRITICAL  全部
  ERROR     主流程例外 (08:00 登入失敗) / 連線失敗 / 出場 / 隔日賣 / 今日不交易 / 預掛 / 盲送 / 撤單例外 / 快照失敗
  WARNING   出場 — / 出場賣單 / 隔日賣 觸發賣出 / 🌙 隔日賣 / 緊急全平 / armed = / 拒絕連線 / 策略外賣出成交 /
            凍結 marked / 交易 WS / 重連成功 / 連線數達上限
  INFO      broker 連線完成 / mode = / 成交回報 (買進與賣出成交，含「哪一支出場了」)

節流:
  * 同一則訊息 30 秒內只送一次 (去重 key 先把數字正規化 → 「已斷線 12 分鐘」「已斷線 13 分鐘」算同一則)；
    成交回報不去重 (每筆都要)。
  * 「交易 WS 已斷線 N 分鐘」這類每 5 秒重複的 CRITICAL 另有 10 分鐘冷卻 (_SLOW_REPEAT)。
  * 2 秒內的多則合併成一封 (09:00 盲送成交會一次來好幾筆)；一封最多 20 行，超過先留高等級的。
  * 每分鐘最多 60 則 **INFO / WARNING**；CRITICAL / ERROR 不受上限 (斷線告警不能被成交洪水擠掉)。
送出走背景 thread，呼叫端永不阻塞；HTTP 用標準庫 urllib (不依賴 requests)。
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from typing import Deque, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_LEVEL_NAME = {logging.CRITICAL: "critical", logging.ERROR: "error",
               logging.WARNING: "warning", logging.INFO: "info"}
_LEVEL_ORDER = {"critical": 0, "error": 1, "warning": 2, "info": 3}

# (最低 levelno, 正規式) — 訊息符合任一條就轉發；CRITICAL 不看正規式
_RULES: List[Tuple[int, re.Pattern]] = [
    (logging.ERROR, re.compile(r"主流程例外|連線失敗|出場|隔日賣|今日不交易|預掛|盲送|撤單例外|撤單失敗|快照失敗|pre-order")),
    (logging.WARNING, re.compile(
        r"出場 —|出場賣單|隔日賣 \S+ (觸發賣出|中止賣出)|隔日賣 \S+ 連 \d+ 次|🌙 隔日賣|緊急全平|armed = |拒絕連線|"
        r"策略外賣出成交|凍結 marked|交易 WS|連線數達上限|重連成功|已換全新 session")),
    (logging.INFO, re.compile(r"broker 連線完成|\[session\] mode = |成交回報 \S+ (buy|sell) ")),
]

# 不去重的訊息 (每筆都要送): 成交回報 (同一檔同價同張數的兩筆成交不是重複)
_NO_DEDUP = re.compile(r"成交回報 ")
# 每幾秒就重複一次的告警 → 同 pattern 10 分鐘內只送一次
_SLOW_REPEAT: List[Tuple[re.Pattern, float]] = [
    (re.compile(r"已斷線 \d+ 分鐘"), 600.0),
]
_DIGITS = re.compile(r"\d+")


def should_forward(levelno: int, message: str, min_level: int = logging.INFO) -> bool:
    """純函式：這一則 log 要不要轉到中控。"""
    if levelno >= logging.CRITICAL:
        return True
    if levelno < min_level:
        return False
    for rule_level, pattern in _RULES:
        # 規則只往上套用：WARNING 規則不會把 INFO 撈進來，但 ERROR 訊息符合 WARNING 規則也轉 (例如 ERROR 的出場失敗)
        if levelno >= rule_level and pattern.search(message):
            return True
    return False


def _env(name: str, default: str = "") -> str:
    # python-dotenv 不剝行尾註解 → 自己截斷 (這個 repo 第三次踩的坑)
    raw = os.environ.get(name, default) or ""
    return raw.split("#", 1)[0].strip()


class CentralNotifier:
    """背景送到 <CENTRAL_FORWARD_URL>/api/central/notify。``notify()`` 永不阻塞、永不 raise。"""

    DEDUP_SEC = 30.0
    BATCH_SEC = 2.0
    RATE_PER_MIN = 60                 # 只算 info / warning；critical / error 不受限
    MAX_LINES_PER_MSG = 20

    def __init__(self, url: str = "", vps_id: str = "", label: str = "", node_name: str = "",
                 send_fn=None, clock=time.monotonic):
        self.url = (url or "").rstrip("/")
        self.vps_id = (vps_id or "").strip()
        self.label = (label or "").strip()
        self.node_name = (node_name or "").strip()
        self._send_fn = send_fn or self._http_send
        self._clock = clock
        self._lock = threading.Lock()
        self._last_sent: Dict[str, float] = {}
        self._queue: Deque[Tuple[str, str]] = deque()      # (level, message)
        self._low_ts: Deque[float] = deque()               # 最近一分鐘排入的 info / warning 時間
        self._dropped = 0
        self._wake = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.stats = {"queued": 0, "sent_batches": 0, "dropped": 0, "failed": 0, "deduped": 0}

    # ─── 設定 ───
    @property
    def configured(self) -> bool:
        return bool(self.url)

    @classmethod
    def from_env(cls) -> "CentralNotifier":
        return cls(url=_env("CENTRAL_FORWARD_URL"), vps_id=_env("CENTRAL_VPS_ID"),
                   label=_env("CENTRAL_LABEL"), node_name=_env("NODE_NAME"))

    # ─── 入口 ───
    @staticmethod
    def _dedup_key(level: str, message: str) -> Optional[str]:
        """去重 key。成交回報不去重；「已斷線 N 分鐘」這類慢重複訊息把數字正規化（12 分鐘 = 13 分鐘）；
        其他訊息用原文 — 股票代號是數字，不能把 2330 與 2317 的出場混成同一則。"""
        if _NO_DEDUP.search(message):
            return None
        for pat, _sec in _SLOW_REPEAT:
            if pat.search(message):
                return f"{level}:{_DIGITS.sub('#', message[:200])}"
        return f"{level}:{message[:200]}"

    @staticmethod
    def _dedup_window(message: str) -> float:
        for pat, sec in _SLOW_REPEAT:
            if pat.search(message):
                return sec
        return CentralNotifier.DEDUP_SEC

    def notify(self, level: str, message: str) -> bool:
        """排進佇列。回 True = 有排 (之後批次送)；False = 未設定 / 被節流。"""
        if not self.configured or not message:
            return False
        level = (level or "critical").lower()
        key = self._dedup_key(level, message)
        now = self._clock()
        with self._lock:
            if key is not None:
                last = self._last_sent.get(key, -1e18)
                if now - last < self._dedup_window(message):
                    self.stats["deduped"] += 1
                    return False
            urgent = level in ("critical", "error")
            if not urgent:
                while self._low_ts and now - self._low_ts[0] > 60.0:
                    self._low_ts.popleft()
                if len(self._low_ts) >= self.RATE_PER_MIN:
                    self._dropped += 1
                    self.stats["dropped"] += 1
                    return False                 # 丟掉就不記去重戳記 (之後同訊息還有機會)
                self._low_ts.append(now)
            if key is not None:
                self._last_sent[key] = now
                if len(self._last_sent) > 2000:          # 不讓它無限長大
                    cutoff = now - 600.0
                    for k in [k for k, t in self._last_sent.items() if t < cutoff]:
                        del self._last_sent[k]
            self._queue.append((level, message))
            self.stats["queued"] += 1
        self._ensure_thread()
        self._wake.set()
        return True

    # ─── 背景送出 ───
    def _ensure_thread(self) -> None:
        t = self._thread
        if t is not None and t.is_alive():
            return
        t = threading.Thread(target=self._run, name="central-notifier", daemon=True)
        self._thread = t
        t.start()

    def _run(self) -> None:
        while True:
            self._wake.wait()
            self._wake.clear()
            time.sleep(self.BATCH_SEC)            # 合併 2 秒內的多則
            self.flush()

    def flush(self) -> None:
        """把佇列裡的訊息合併送出 (最高等級決定整封的 level；超過 20 行先留高等級的)。測試可直接呼叫。"""
        with self._lock:
            items = list(self._queue)
            self._queue.clear()
            dropped, self._dropped = self._dropped, 0
        if not items:
            return
        level = min((lv for lv, _ in items), key=lambda lv: _LEVEL_ORDER.get(lv, 9))
        if len(items) > self.MAX_LINES_PER_MSG:
            # 穩定排序：高等級先、同等級維持時間序
            kept = sorted(items, key=lambda it: _LEVEL_ORDER.get(it[0], 9))[: self.MAX_LINES_PER_MSG]
            order = {id(it): i for i, it in enumerate(items)}
            kept.sort(key=lambda it: order[id(it)])
            lines = [m for _, m in kept]
            lines.append(f"…另有 {len(items) - self.MAX_LINES_PER_MSG} 則 (低等級)")
        else:
            lines = [m for _, m in items]
        if dropped:
            lines.append(f"(⚠ 超過每分鐘 {self.RATE_PER_MIN} 則上限，另丟棄 {dropped} 則 info/warning)")
        text = "\n".join(lines)
        ok = False
        try:
            ok = bool(self._send_fn(level, text))
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[notifier] 送中控失敗: {e}")
        with self._lock:
            if ok:
                self.stats["sent_batches"] += 1
            else:
                self.stats["failed"] += 1

    def _http_send(self, level: str, text: str) -> bool:
        vps_id: Optional[int] = int(self.vps_id) if self.vps_id.isdigit() else None
        payload = {"vps_id": vps_id, "fallback_source": self.label or self.node_name or "LimitUp",
                   "level": level, "message": text, "system": "limitup"}
        req = urllib.request.Request(f"{self.url}/api/central/notify", data=json.dumps(payload).encode("utf-8"),
                                     headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return 200 <= r.status < 300
        except urllib.error.HTTPError as e:
            logger.warning(f"[notifier] 中控回 {e.code}: {(e.read() or b'')[:200]!r}")
            return False


class CentralLogHandler(logging.Handler):
    """掛在 root logger：符合規則的紀錄轉給 notifier。handler 內任何例外都吞掉 (logging 不能炸交易路徑)。"""

    def __init__(self, notifier: CentralNotifier, min_level: int = logging.INFO):
        super().__init__(level=logging.INFO)
        self.notifier = notifier
        self.min_level = min_level

    def emit(self, record: logging.LogRecord) -> None:
        try:
            if record.name.startswith("notifier"):          # 自己的 log 不轉 (避免迴圈)
                return
            msg = record.getMessage()
            if not should_forward(record.levelno, msg, self.min_level):
                return
            lv = _LEVEL_NAME.get(record.levelno) or ("critical" if record.levelno > logging.CRITICAL else "info")
            self.notifier.notify(lv, msg)
        except Exception:  # noqa: BLE001
            pass


_singleton: Optional[CentralNotifier] = None
_handler: Optional[CentralLogHandler] = None


def get_notifier() -> CentralNotifier:
    global _singleton
    if _singleton is None:
        _singleton = CentralNotifier.from_env()
    return _singleton


def install_logging_handler() -> Optional[CentralLogHandler]:
    """server 啟動時呼叫一次。未設 CENTRAL_FORWARD_URL → 不掛、回 None。"""
    global _handler
    n = get_notifier()
    if not n.configured:
        logger.info("[notifier] CENTRAL_FORWARD_URL 未設 — 不轉發通知到中控")
        return None
    if _handler is not None:
        return _handler
    lvl_name = _env("CENTRAL_NOTIFY_MIN_LEVEL", "INFO").upper()
    min_level = getattr(logging, lvl_name, logging.INFO) if isinstance(lvl_name, str) else logging.INFO
    h = CentralLogHandler(n, min_level=min_level)
    logging.getLogger().addHandler(h)
    _handler = h
    logger.info(f"[notifier] 中控轉發已啟用 → {n.url} vps_id={n.vps_id or '-'} label={n.label or n.node_name or '-'} "
                f"min_level={lvl_name}")
    return h
