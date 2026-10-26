"""Subscriber — 多 socket 訂閱 books channel。

富邦官方: 單一 WS 連線 200 訂閱數上限；同帳號可同時開 5 連線
→ 5 socket × 200 = 1000 檔平行監控

實作策略 (2 層):

1. **首選 multi-SDK (預設)**: 開 5 個 FubonSDK instance，各自 login/init_realtime →
   每個 SDK 拿一個 websocket_client.stock → 5 個獨立 WS 連線
   母體 2600 檔 → 5×200=1000/次 → 3 批 × 30s = 90s 全體監完一次

2. **保底 loop single-socket**: 若 multi-SDK 失敗 (例: 富邦擋同帳號 5 login)，
   fallback 到單 socket 每 30s 換一批 subscribe
"""
from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
from datetime import datetime
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)

# ─── 行情連線健康 / 自動重連 (2026-10-12: hub 8 條 socket 08:48 全部停擺、其中 5 條之後斷線且從未重連,
#     hub 拿著過期的「鎖漲停」標記凍結名單 → 五台 node 對 08:57:48 已打開的 6872 預掛) ───
# 做法比照 day-trade 系統的生產分支 (teralion day-trade-system: fubon_adapter._handle_disconnect /
# _md_connect_with_timeout,以及之後的 fugle_connect_patch),每條 socket 各自一套:
#   disconnect 事件 → single-flight 的重連 thread → 先 disconnect() 舊的 (重設認證狀態、取消舊計時器) → 0.5 s →
#   開連線 → 重新訂閱;**不設次數上限**,退避 2,4,8,16,32,60,60… s。
# day-trade 踩過、這裡一併處理的坑:
#   1. 函式庫的 connect() 是沒有 sleep、沒有期限的忙等 (等 auth_status 變成已認證 / 認證失敗):TCP 根本連不上
#      時永遠不返回、吃滿一顆 CPU (day-trade 一台 1 vCPU 主機上留過 11 條)。→ 不呼叫它: 自己起收訊 thread
#      (函式庫 connect() 做的同一件事)、用 sleep 輪詢認證狀態、有期限 (_open_socket)。**開機第一次連線也走
#      這條** — node 是 08:59:50 才連行情,那時候忙等會把唯一的一顆 CPU 佔到開盤。
#   2. 連線斷掉後 auth_status 仍停在「已認證」→ 不先 disconnect() 就 connect() 會立刻返回,接著訂閱全炸。
#   3. Linux 上關掉 socket 叫不醒正在 select() 的舊收訊 thread,它要等自己的 10 s 逾時才發現 → 強制關閉停擺的
#      連線後最多等 MD_CLOSE_WAIT_SEC;舊的沒結束就**不在同一個物件上開第二條** (兩條會搶同一個 socket)。
#   4. 同一組 token 重連可能一直「authentication timeout」(realtime session 失效;官方的斷線重連作法是重新登入)
#      → 認證類失敗 / 舊收訊迴圈不結束 / 連續 MD_REBUILD_AFTER_FAILS 次失敗 → 這條 socket 換**全新 session**
#      (新 SDK login + init_realtime,_rebuild_socket),每條 MD_REBUILD_COOLDOWN_SEC 最多一次;換下來的 SDK 要
#      logout (否則留一條原生忙等 thread + 佔一個連線名額)。
#   5. 「Maximum number of connections reached」: 只開不關會把券商的連線名額塞滿、連上就被踢 (2026-08-25 的死亡
#      螺旋)。→ 一律先關舊的;看到這個錯誤就等 MD_CONN_CAP_WAIT_SEC 讓券商回收名額,再用全新 session 連。
#   6. **不可以只靠 disconnect 事件**: 舊版 websocket-client 在同一個物件第二次以後的連線結束時不再發這個事件
#      (實測 1.6.4;生產機的版本沒有 pin)。→ 收訊 thread 是我們自己起的,thread 結束 = 連線結束 (_rx_main 的
#      finally),不管事件有沒有來;看門狗也會檢查收訊 thread 還在不在。連線層的致命錯誤 (逾時 / 連線中斷)
#      一出現就先標斷線 — 關閉事件要等函式庫跟對方握手,會晚約 3 s。
# 另加看門狗: 一條 socket 安靜 MD_IDLE_PING_SEC 秒就送 ping (伺服器回 pong = 一則訊息),連 pong 都沒有、
# 總共約 MD_STALL_SEC 秒沒有任何訊息 → 視為停擺,強制重連 (SDK 內建的 30 s 心跳要 60~90 s 才會發現;
# day-trade 的看門狗是 120 s,但盤前凍結名單等不了那麼久)。
# 重連之後**沒有初始畫面** (盤前試撮五檔只在變動時推送) → 該連線上的檔在收到新的 books 之前都算「過期」
# (is_stale),hub 的 marked 快照會把它們排除 (runner.marked_snapshot)。
MD_IDLE_PING_SEC_DEFAULT = 5.0      # 一條 socket 這麼久沒任何訊息 → 開始送 ping 探測
MD_STALL_SEC_DEFAULT = 20.0         # 這麼久沒任何訊息 (含 pong) → 停擺,強制重連;0 = 停用看門狗
MD_CONNECT_TIMEOUT_SEC = 12.0       # 重連時開連線 + 認證最多等多久 (TCP 逾時 5 s + 函式庫認證計時 5 s)
MD_CLOSE_WAIT_SEC = 12.0            # 強制關閉後等舊收訊迴圈結束的上限 (Linux 要 ~10 s,見上 3)
MD_RECONNECT_STEP_SEC = 2.0         # 重連退避 = min(2 × 2^(n−1), 60) s (同 day-trade 生產分支)
MD_RECONNECT_MAX_SEC = 60.0
MD_REBUILD_AFTER_FAILS = 3          # 同一個物件連續失敗幾次 → 換全新 session
MD_REBUILD_COOLDOWN_SEC = 60.0      # 每條 socket 換 session 的最短間隔 (防洗登入)
MD_CONN_CAP_WAIT_SEC = 20.0         # 連線數達上限: 等券商回收名額多久再連
MD_FLAP_SEC = 10.0                  # 重連成功後這麼快又斷 = 沒撐住 → 接著上一輪繼續退避 / 累計失敗 (不從 2 s 重來)。
#                                     要小於 MD_STALL_SEC: 看門狗誤判停擺最快也要 20 s 後,那種只該花一次 2 s 重連
_AUTH_OK = 2                        # fugle_marketdata AuthenticationState.AUTHENTICATED
_AUTH_FAILED = 3                    # fugle_marketdata AuthenticationState.UNAUTHENTICATED
_CONN_CAP_TEXT = "Maximum number of connections"


class _MdAuthError(RuntimeError):
    """連線開了但認證失敗 / 認證逾時 — 這組 token (realtime session) 可能已失效 → 換全新 session。"""


class _MdNotEstablished(RuntimeError):
    """連線根本沒建立 (連不上 / 被拒 / 認證前就關了) — 多半是對方或網路的問題,照退避重試。"""


class _MdOldLoopAlive(RuntimeError):
    """舊連線的收訊迴圈還沒結束 — 不可以在同一個物件上再開一條 (兩條會搶同一個 socket)。"""


def _env_float(name: str, default: float) -> float:
    try:
        raw = os.environ.get(name, "")
        return float(raw) if str(raw).strip() != "" else default
    except (TypeError, ValueError):
        return default


class Subscriber:
    """訂閱 books channel + 分派 tick 給 on_book callback。

    on_book(symbol, bids, asks) 由 filter.py 決定 mark/unmark 邏輯。
    """

    def __init__(self, sdk, universe: List[str],
                 on_book: Callable[[str, list, list], None],
                 login_cfg,
                 on_trade: Optional[Callable[[str, dict], None]] = None,
                 recorder=None,
                 batch_size: int = 200,
                 batch_rotate_sec: int = 30,
                 socket_count: int = 4,
                 debug: bool = False):
        self.primary_sdk = sdk
        self.universe = universe
        self.on_book = on_book
        self.on_trade = on_trade             # 9:00 後 trader 掛的 trade handler; None = 忽略
        self._context_book = self._context_trade = None
        self.login_cfg = login_cfg          # config.Config — 給 multi-SDK 用 login 資訊
        self.recorder = recorder             # TickRecorder — 存 raw tick，可 None
        self.batch_size = batch_size
        self.batch_rotate_sec = batch_rotate_sec
        self.socket_count = socket_count
        self.debug = debug

        self._stop = threading.Event()
        self._threads: List[threading.Thread] = []
        self._extra_sdks: List = []          # 額外 login 的 SDK instance (keep reference)
        self._sockets: List = []              # 實際訂閱用的 WS clients
        self._tick_counter = 0
        # heartbeat 分 channel 統計 — 每 10 秒印一次，讓你 8:30 就能知道 tick 有沒進來
        self._books_count = 0
        self._trades_count = 0
        # 每 symbol 最新 tick snapshot (給 API /api/tick/{symbol} 查)
        self._latest_books: dict = {}    # symbol → last books dict {bids, asks, ts}
        self._latest_trade: dict = {}    # symbol → last trade dict {price, size, ts}
        self._snap_lock = threading.Lock()
        # error message dedup — 每種 error 只印前 3 次，避免 log 洪水
        self._error_count: dict = {}   # error_msg → count
        # 富邦 subscribe 送出後 async 收到 event=subscribed，才帶 id
        # unsubscribe 時要傳 id，所以要記住 (channel, symbol) → id
        self._sub_ids: dict = {}       # (channel, symbol) → subscription_id
        self._sub_ids_lock = threading.Lock()
        # 單檔退訂 queue — message handler thread 不能直接呼叫 ws.unsubscribe
        # (SDK 內部 thread deadlock 風險)，改由 pruner thread 處理
        self._unsub_queue: "queue.Queue[tuple]" = queue.Queue()   # (symbol, retry_count)
        # 已淘汰股票 — socket loop 每輪 re-subscribe 時必須跳過，否則退訂又被訂回
        self._removed: set = set()
        self._removed_lock = threading.Lock()
        # symbol → universe 位置 (O(1) 路由;原本 list.index O(n),9:00 keep_only
        # 退訂 ~1900 檔 × 線性掃描會在開盤那一秒燒 CPU)
        self._symbol_idx: dict = {}
        # 盤中補訂的檔 (add_symbol: 隔日賣手動加入 / 凍結後補收的 marked) — socket loop 每輪
        # 只 re-subscribe 開機算好的 batches,補訂的檔也要跟著每輪重訂,否則 socket 重連後就漏掉
        self._extra_symbols: set = set()
        self._extra_lock = threading.Lock()
        # socket 已可收訂閱 (socket loop thread 起跑時 set — fugle 2.4.1 的 connect() 是同步到認證完成);
        # runner 的凍結後補收 (2026-10-06) 等這個 event 才 add_symbol,免得送進還沒連上的 socket
        self._ready = threading.Event()
        self._connect_grace_sec = 2.0        # 開機批次送出前的保守等待 (舊行為;測試可調短)
        # ─── 連線健康 / 自動重連 (檔頭 MD_*) ───
        # 每條 socket 一個 dict (見 _init_socket_health);空 list = 尚未初始化 (舊測試的替身 socket) →
        # 一律視為正常、沒有過期的檔
        self._sock_state: List[dict] = []
        self._sock_lock = threading.Lock()
        # symbol → 最後一筆 books 屬於所屬 socket 的第幾段連線 (st["epoch"])。用計數而不是時間比較:
        # 時鐘解析度粗的平台 (Windows ~16 ms) 上「斷線前最後一筆」與「重連」可能落在同一個時間刻度
        self._last_book_epoch: dict = {}
        self._trade_subs: set = set()        # 訂過 trades 的檔 (重連後要一併重訂)
        self._cur_batch: dict = {}           # socket_idx → 這條 socket 目前訂的檔 (socket loop 每輪更新)
        self._sock_sdks: List = []           # 每條 socket 的資料 SDK (第 1 條 = primary_sdk)
        self._sock_creds: List = []          # 每條 socket 的登入資訊 (換全新 session 用);None = 不能換
        self._rebuild_lock = threading.Lock()    # 換 session (登入) 一次只做一條
        self._stall_sec = _env_float("MD_STALL_SEC", MD_STALL_SEC_DEFAULT)
        self._idle_ping_sec = max(0.5, _env_float("MD_IDLE_PING_SEC", MD_IDLE_PING_SEC_DEFAULT))
        self._connect_timeout_sec = MD_CONNECT_TIMEOUT_SEC
        self._close_wait_sec = MD_CLOSE_WAIT_SEC
        self._reconnect_step_sec = MD_RECONNECT_STEP_SEC
        self._reconnect_max_sec = MD_RECONNECT_MAX_SEC
        self._reconnect_settle_sec = 0.5     # disconnect() 之後、開新連線之前 (同 day-trade)
        self._rebuild_after_fails = MD_REBUILD_AFTER_FAILS
        self._rebuild_cooldown_sec = MD_REBUILD_COOLDOWN_SEC
        self._conn_cap_wait_sec = MD_CONN_CAP_WAIT_SEC
        self._flap_sec = MD_FLAP_SEC
        self._watchdog_tick_sec = 1.0

    # ─── 對外 API ────────────────────────────────────────────

    def start(self):
        """優先 multi-SDK，失敗 fallback loop single-socket。"""
        if self._try_multi_sdk():
            logger.info(f"[subscriber] 使用 multi-SDK 模式 ({len(self._sockets)} sockets)")
            self._start_multi_socket_loop()
        else:
            logger.warning("[subscriber] multi-SDK 失敗，fallback loop single-socket")
            self._sockets = [self.primary_sdk.marketdata.websocket_client.stock]
            plan = self._account_plan(self.login_cfg)
            self._sock_sdks = [self.primary_sdk]
            self._sock_creds = [tuple(plan[0][:5])] if plan else [None]
            self._start_loop_single_socket()
        # heartbeat thread — 每 10 秒印一次 tick 統計 (即使 0 也印，方便診斷)
        hb = threading.Thread(target=self._heartbeat_loop, name="subscriber-heartbeat",
                              daemon=True)
        hb.start()
        self._threads.append(hb)
        # pruner thread — 處理單檔退訂 (unmark 淘汰 / keep_only)
        pr = threading.Thread(target=self._pruner_loop, name="subscriber-pruner",
                              daemon=True)
        pr.start()
        self._threads.append(pr)
        # watchdog thread — 收訊 thread 死掉就標斷線;安靜的 socket 送 ping 探測,停擺就強制重連
        # (MD_STALL_SEC=0 只停用「停擺」判定)
        if self._sock_state:
            wd = threading.Thread(target=self._watchdog_loop, name="subscriber-watchdog",
                                  daemon=True)
            wd.start()
            self._threads.append(wd)
            if self._stall_sec <= 0:
                logger.warning("[subscriber] MD_STALL_SEC=0 → 行情停擺看門狗停用 (只靠連線結束事件重連)")

    def stop(self):
        """乾淨關閉:
        1. set stop event → 各 loop thread 跳出
        2. 對每個 socket 呼叫 ws.disconnect() (Alex 用同 pattern)
        3. join threads (帶 timeout 避免死等)
        Fubon SDK 內部有自己的 WS thread — 沒 disconnect 的話 Python process 不會 exit。
        """
        logger.info("[subscriber] stop 開始 — 通知 loop threads")
        self._stop.set()

        # 明確 disconnect 每個 socket (Alex fubon_adapter.py:129-133 pattern)
        for i, ws in enumerate(self._sockets):
            try:
                ws.disconnect()
                logger.info(f"[subscriber] socket #{i+1} disconnect() 已呼叫")
            except Exception as e:
                logger.warning(f"[subscriber] socket #{i+1} disconnect fail: {e}")

        # 給 SDK 內部 thread 時間清乾淨 (Alex line 627 也 sleep 0.5)
        import time as _t
        _t.sleep(0.5)

        # join 自己的 loop threads
        for t in self._threads:
            t.join(timeout=3)
            if t.is_alive():
                logger.warning(f"[subscriber] thread {t.name} 3 秒內沒 join 完 (skip)")

        logger.info("[subscriber] stop 完成")

    def _heartbeat_loop(self):
        """每 10 秒印一次 tick 統計 — 即使 0 也印，方便診斷有沒收到 tick."""
        last_books = 0
        last_trades = 0
        while not self._stop.is_set():
            self._stop.wait(10)
            if self._stop.is_set():
                return
            b = self._books_count
            t = self._trades_count
            db = b - last_books
            dt = t - last_trades
            logger.info(f"[heartbeat] 累計 books={b} trades={t} | "
                        f"最近 10s: +{db} books / +{dt} trades")
            last_books = b
            last_trades = t

    def set_handlers(self, on_book=None, on_trade=None, *, with_context=False):
        """9:00 handoff — trader 換掉 on_book/on_trade 邏輯，訂閱不動."""
        if on_book is not None:
            self._context_book = on_book if with_context else None
            self.on_book = on_book
        if on_trade is not None:
            self._context_trade = on_trade if with_context else None
            self.on_trade = on_trade
        logger.info("[subscriber] handlers 已 handoff 給 trader")

    def subscribe_trades_for(self, symbols: List[str]):
        """9:00 後對 watchlist 加訂 trades channel。

        全母體 books-only 常駐是為了省 sub 額度；watchlist 通常 <30 檔，
        額外 +N trades subs 不會撞上限。每檔落到跟自己 books 相同 socket
        (對齊 round-robin universe index % n_sockets) 讓 handler 分派一致。
        """
        if not self._sockets:
            logger.error("[subscriber] subscribe_trades_for: 尚無 sockets")
            return
        n = len(self._sockets)
        ok = 0
        fail = 0
        first_err = None
        for sym in symbols:
            idx = self._symbol_idx.get(sym, 0) % n
            self._trade_subs.add(sym)            # 送失敗也記 — 該 socket 重連後一併補訂
            try:
                self._sockets[idx].subscribe({"channel": "trades", "symbol": sym})
                ok += 1
            except Exception as e:
                fail += 1
                if first_err is None:
                    first_err = (sym, str(e)[:150])
        # 全部送失敗 = 這些檔的成交完全收不到 (2026-10-12 09:00 hub 14 檔 0 ok / 14 fail 只記了一行 INFO)
        log = logger.critical if (fail and not ok) else (logger.warning if fail else logger.info)
        log(f"[subscriber] subscribe_trades_for: {ok} ok / {fail} fail "
            f"(watchlist {len(symbols)} 檔) first_err={first_err}"
            f"{' — 送失敗的檔會在該行情連線重連後補訂' if fail else ''}")

    def add_symbol(self, symbol: str) -> bool:
        """盤中手動加訂一檔 (隔日賣手動輸入用) — 立即訂 books+trades。

        socket loop 只 re-subscribe 開機算好的 batches,盤中 append 的不會被 loop 帶到,
        所以這裡一次性直接訂 (券商端會保留訂閱)。回 True=有送出訂閱。
        """
        symbol = str(symbol or "").strip()
        if not symbol:
            return False
        if not self._sockets:
            logger.error("[subscriber] add_symbol: 尚無 sockets")
            return False
        # 從 removed 移除 (若曾被淘汰),並確保 universe 有它 (_socket_for/index 需要)
        with self._removed_lock:
            self._removed.discard(symbol)
        if symbol not in self._symbol_idx and symbol not in self.universe:
            self.universe.append(symbol)
            self._symbol_idx[symbol] = len(self.universe) - 1
        ws = self._socket_for(symbol)
        if ws is None:
            return False
        with self._extra_lock:
            self._extra_symbols.add(symbol)      # 之後 socket loop 每輪一起重訂 (重連後不漏)
        self._trade_subs.add(symbol)             # trades 也要在重連後補訂
        ok = True
        sent = []
        for ch in ("books", "trades"):
            if self.has_ack(ch, symbol):
                continue        # 已訂到 (例: 隔日賣標的又被補收) → 不重複訂 (舊 id 會丟失、tick 雙份)
            try:
                ws.subscribe({"channel": ch, "symbol": symbol})
                sent.append(ch)
            except Exception as e:
                ok = False
                logger.warning(f"[subscriber] add_symbol {symbol} {ch} 失敗: {str(e)[:150]}")
        logger.warning(f"[subscriber] 手動加訂 {symbol} 送出 {sent or '無 (已訂)'} ok={ok}")
        return ok

    def is_ready(self) -> bool:
        """socket 已連線並認證完成 (socket loop thread 已起跑) — 此後 add_symbol 的訂閱伺服器才收。"""
        return self._ready.is_set()

    def has_ack(self, channel: str, symbol: str) -> bool:
        """伺服器是否已回 event=subscribed (帶 subscription id) — 補訂成功的真正依據。"""
        with self._sub_ids_lock:
            return (channel, symbol) in self._sub_ids

    # ─── 連線健康 / 過期判定 (對外) ──────────────────────────

    def _socket_index(self, symbol: str) -> int:
        """symbol 所屬 socket 的 index (與 _socket_for 同一套 round-robin)。"""
        n = len(self._sockets)
        return (self._symbol_idx.get(symbol, 0) % n) if n else 0

    def is_stale(self, symbol: str) -> bool:
        """這檔的五檔是否「過期」= 不能拿來判斷它現在還鎖不鎖漲停:
          - 它所屬的行情連線目前斷線 / 停擺中,或
          - 該連線重連過,而這檔在重連之後還沒收到任何一筆 books。
        重連後**沒有初始畫面** (盤前試撮五檔只在變動時推送),所以第二種不能省 — 斷線期間它可能已經打開
        (2026-10-12 的 6872)。收到新的 books (filter handler 照常 mark / unmark) 就自動恢復。
        連線健康尚未初始化 (測試替身 socket) → 一律 False。"""
        ss = self._sock_state
        if not ss:
            return False
        idx = self._socket_index(symbol)
        if idx >= len(ss):
            return False
        st = ss[idx]
        if not st["up"]:
            return True
        epoch = st["epoch"]                      # 這條 socket 重連成功過幾次;0 = 沒斷過
        return epoch > 0 and self._last_book_epoch.get(symbol, -1) < epoch

    def feed_epoch(self, symbol: str):
        """Connection identity remains changed after the first fresh book.

        is_stale alone is already false when that book reaches Trader, so a
        consumer must also compare this identity to reset across quick reconnects.
        """
        idx = self._socket_index(symbol)
        if idx >= len(self._sock_state):
            return None
        st = self._sock_state[idx]
        return idx, st["gen"], st["epoch"]

    def stale_symbols(self, symbols) -> set:
        """symbols 當中目前過期的 (見 is_stale)。"""
        return {s for s in symbols if self.is_stale(s)}

    def is_down(self, symbol: str) -> bool:
        """這檔所屬的行情連線**目前**斷線 / 停擺中 (看不到它的任何變化)。與 is_stale 的差別: 不含
        「重連後尚未收到新五檔」— node 的行情 08:59:50 才訂,沒有畫面是常態,只有連線本身斷了才算瞎。"""
        ss = self._sock_state
        if not ss:
            return False
        idx = self._socket_index(symbol)
        return idx < len(ss) and not ss[idx]["up"]

    def down_symbols(self, symbols) -> set:
        return {s for s in symbols if self.is_down(s)}

    def socket_health(self) -> dict:
        """各條行情連線的狀態 — get_tick_stats()["sockets"] (/api/health 看得到)。"""
        now_m = time.monotonic()
        rows = []
        for i, st in enumerate(self._sock_state):
            up = bool(st["up"])
            rows.append({
                "idx": i + 1,
                "up": up,
                "down_sec": 0.0 if up else round(now_m - st["down_mono"], 1),
                "reason": "" if up else st["down_reason"],
                "reconnects": st["reconnects"],
                "rebuilds": st["rebuilds"],
                "idle_sec": round(now_m - st["last_msg_mono"], 1),
            })
        return {"total": len(rows), "down": sum(1 for r in rows if not r["up"]),
                "reconnects": sum(r["reconnects"] for r in rows), "detail": rows}

    def request_unsubscribe(self, symbol: str):
        """單檔退訂 (unmark 淘汰時呼叫) — thread-safe，可在 message handler 內呼叫。

        實際 ws.unsubscribe 由 pruner thread 執行，這裡只進 queue + 標 removed
        (removed 讓 socket loop 每輪 re-subscribe 時跳過此股)。
        """
        with self._removed_lock:
            if symbol in self._removed:
                return
            self._removed.add(symbol)
        self._unsub_queue.put((symbol, 0))

    def keep_only(self, symbols: List[str]):
        """9:00 轉場 — 只留 symbols 的訂閱，其餘全部退訂。"""
        keep = set(symbols)
        to_remove = [s for s in self.universe if s not in keep]
        for sym in to_remove:
            self.request_unsubscribe(sym)
        logger.info(f"[subscriber] keep_only: 保留 {len(keep)} 檔，退訂 {len(to_remove)} 檔")

    def _pruner_loop(self):
        """每 1s drain unsub queue — 對每檔用 sub_id 呼叫 ws.unsubscribe。
        sub_id 尚未 ack (不在 _sub_ids) → re-queue 最多 10 次後放棄
        (removed 已標，socket loop 不會再訂回，broker 端殘留訂閱無害)。
        """
        MAX_RETRY = 10
        while not self._stop.is_set():
            self._stop.wait(1)
            if self._stop.is_set():
                return
            requeue = []
            done = 0
            while True:
                try:
                    sym, retry = self._unsub_queue.get_nowait()
                except queue.Empty:
                    break
                ws = self._socket_for(sym)
                if ws is None:
                    continue
                for ch in ("books", "trades"):
                    with self._sub_ids_lock:
                        sub_id = self._sub_ids.pop((ch, sym), None)
                    if sub_id is None:
                        if ch == "books" and retry < MAX_RETRY:
                            requeue.append((sym, retry + 1))
                        continue
                    try:
                        ws.unsubscribe({"id": sub_id})
                        done += 1
                    except Exception as e:
                        logger.warning(f"[pruner] unsubscribe {ch}/{sym} fail: {e}")
            for item in requeue:
                self._unsub_queue.put(item)
            if done:
                logger.info(f"[pruner] 已退訂 {done} 個 subscription "
                            f"(queue 剩 {self._unsub_queue.qsize()})")

    def _rebuild_symbol_index(self):
        """建 symbol → universe 位置 dict — universe 定案 (truncate) 後呼叫一次。"""
        self._symbol_idx = {sym: i for i, sym in enumerate(self.universe)}

    def _socket_for(self, symbol: str):
        """round-robin 定位 symbol 所屬 socket (跟 _split_universe_per_socket 同法,
        O(1) dict 查表;查無 → socket 0,同舊版 ValueError fallback)。"""
        n = len(self._sockets)
        if n == 0:
            return None
        idx = self._symbol_idx.get(symbol, 0) % n
        return self._sockets[idx]

    # ─── 連線健康: 斷線偵測 / 自動重連 / 看門狗 (檔頭 MD_*) ────

    def _init_socket_health(self):
        """每條 socket 建健康狀態並掛 disconnect / error 事件 — 在第一次 connect() 之前呼叫 (重連沿用同一個
        client 物件,事件一直在)。掛不上 (替身沒有 on) 只警告: 那條 socket 就沒有事件驅動的重連。"""
        now_m = time.monotonic()
        self._sock_state = [{
            "up": True,                     # False = 斷線 / 停擺中 (該連線上的檔全部過期)
            "down_since": None, "down_mono": 0.0, "down_reason": "",
            "epoch": 0,                     # 第幾段連線 (每次重連 +1);books 記下當時的值,較舊 = 過期
            "last_msg_mono": now_m,         # 最後一則訊息 (任何 event,含 pong)
            "last_ping_mono": 0.0,          # 看門狗最後一次送 ping
            "first_ping_mono": 0.0,         # 最後一則訊息之後的第一個 ping (還沒被回應的最早那個)
            "reconnects": 0,
            "rebuilds": 0,                  # 換過幾次全新 session
            "reconnecting": False,          # 這條的重連 thread 在跑 (single-flight)
            "gen": 0,                       # client 物件的世代;換 session 後舊物件晚到的事件依此丟棄
            "loop_closed": False,           # 目前這個物件的收訊迴圈已結束 (收過 disconnect 事件)
            "closed_ev": threading.Event(),
            "rx_thread": None,              # 我們自己起的收訊 thread (_open_socket);開機那條是函式庫起的
            "want_rebuild": False,          # 下一次嘗試要換全新 session
            "last_rebuild_mono": 0.0,
            "cap_until": 0.0,               # 連線數達上限 → 這個時刻之前不連
            "up_mono": 0.0,                 # 最近一次標回正常的時刻 (判斷「連上就又斷」用)
            "carry_attempt": 0,             # 那一次重連用到第幾次嘗試 / 累計幾次失敗 — 沒撐住的話接著算
            "carry_fail": 0,
        } for _ in self._sockets]
        n = len(self._sockets)
        self._sock_sdks = (list(self._sock_sdks) + [None] * n)[:n]
        self._sock_creds = (list(self._sock_creds) + [None] * n)[:n]
        for i, ws in enumerate(self._sockets):
            try:
                ws.on("disconnect", self._make_disconnect_handler(i, 0))
                ws.on("error", self._make_error_handler(i, 0))
            except Exception as e:
                logger.warning(f"[subscriber] socket #{i+1} 掛 disconnect/error 事件失敗 "
                               f"(這條沒有事件驅動的自動重連): {e}")

    def _socket_is_up(self, idx: int) -> bool:
        ss = self._sock_state
        return True if idx >= len(ss) else bool(ss[idx]["up"])

    def _symbols_on_socket(self, idx: int) -> list:
        """這條 socket 目前該訂的檔: socket loop 這一輪的批次 + 補訂的 extras − 已淘汰。"""
        with self._removed_lock:
            removed = set(self._removed)
        with self._extra_lock:
            extras = [s for s in sorted(self._extra_symbols) if self._socket_index(s) == idx]
        base = list(self._cur_batch.get(idx) or [])
        return [s for s in dict.fromkeys(base + extras) if s not in removed]

    def _make_disconnect_handler(self, idx: int, gen: int = 0):
        def _on_disconnect(code=None, message=None, *_a):
            st = self._sock_state[idx]
            if st["gen"] != gen:
                return                       # 已換掉的舊物件晚到的關閉事件
            st["loop_closed"] = True
            st["closed_ev"].set()
            if self._stop.is_set():
                return                       # 我們自己 stop() 關的
            if message is not None and _CONN_CAP_TEXT in str(message):
                self._note_conn_cap(idx, message)    # 舊版 websocket-client: 關閉 frame 的原因只出現在這裡
            self._socket_down(idx, f"disconnect code={code} msg={message}")
        return _on_disconnect

    def _note_conn_cap(self, idx: int, detail) -> None:
        """券商端連線名額滿了 (多半是之前的連線沒關乾淨)。繼續一直連只會一直被踢 → 等券商回收名額,
        下一次用全新 session (舊的 logout 才會釋放名額)。"""
        st = self._sock_state[idx]
        first = st["cap_until"] <= time.monotonic()
        st["cap_until"] = time.monotonic() + self._conn_cap_wait_sec
        st["want_rebuild"] = True
        if first:
            logger.critical(f"[subscriber] ⚠⚠ 行情 socket #{idx+1} 被券商拒絕: 連線數達上限 ({detail}) — "
                            f"等 {self._conn_cap_wait_sec:g} s 讓券商回收名額,再用全新 session 重連")

    def _make_error_handler(self, idx: int, gen: int = 0):
        def _on_error(err=None, *_a):
            # 只記錄 (去重);斷線一律以 disconnect 事件 / 看門狗為準。沒人聽 error 事件的話,事件發射器會把它
            # 當例外丟回 websocket thread,斷線原因 (例「Connection timed out」) 就不會有我們自己的一行 log
            st = self._sock_state[idx]
            if st["gen"] != gen:
                return
            text = f"{type(err).__name__}: {err}"
            if _CONN_CAP_TEXT in text:
                self._note_conn_cap(idx, err)
                return
            key = f"ws-error:{idx}:{text[:80]}"
            cnt = self._error_count.get(key, 0) + 1
            self._error_count[key] = cnt
            if cnt <= 3 or cnt % 500 == 0:
                logger.warning(f"[socket#{idx}] 連線層錯誤 #{cnt}: {text}")
            # 連線已經不能用的錯誤 (讀取逾時 / 連線中斷) → 現在就標斷線。關閉事件要等函式庫跟對方握手
            # (對方沒回應時約 3 s),這段時間訂閱已經送不出去、資料也不會再來
            if self._is_fatal_ws_error(err) and not self._stop.is_set():
                self._socket_down(idx, f"error {text[:120]}")
        return _on_error

    @staticmethod
    def _is_fatal_ws_error(err) -> bool:
        """連線層的致命錯誤: OS / 逾時 / 連線中斷,以及 websocket-client 自己的 WebSocket*Exception。
        (handler 內的例外不會走到這裡 — 訊息處理全程自己接;保守起見其他型別一律不當致命)"""
        if isinstance(err, (OSError, TimeoutError, ConnectionError, EOFError)):
            return True
        return any(c.__name__.startswith("WebSocket") and c.__name__.endswith("Exception")
                   for c in type(err).__mro__)

    def _socket_down(self, idx: int, reason: str):
        """把一條 socket 標成斷線 / 停擺 (只有從正常轉過來那一次 CRITICAL),並確保有一條重連 thread 在跑。"""
        if self._stop.is_set() or idx >= len(self._sock_state):
            return
        st = self._sock_state[idx]
        with self._sock_lock:
            was_up = st["up"]
            if was_up:
                st["up"] = False
                st["down_since"] = time.time()
                st["down_mono"] = time.monotonic()
                st["down_reason"] = reason
            start = not st["reconnecting"]
            if start:
                st["reconnecting"] = True
        if was_up:
            logger.critical(f"[subscriber] ⚠⚠ 行情 socket #{idx+1} 斷線/停擺 ({reason}) — 這條連線上 "
                            f"{len(self._symbols_on_socket(idx))} 檔收不到資料,已視為過期 "
                            f"(hub 名單會排除);自動重連中")
        if start:
            try:
                threading.Thread(target=self._reconnect_loop, args=(idx,),
                                 name=f"subscriber-reconnect-{idx}", daemon=True).start()
            except BaseException as e:      # noqa: BLE001 — 例「can't start new thread」
                # 旗標已經立了、thread 卻沒起來 → 不放掉的話這條 socket 整天沒人重連 (day-trade 生產環境發生過)
                with self._sock_lock:
                    st["reconnecting"] = False
                logger.critical(f"[subscriber] ⚠⚠ 行情 socket #{idx+1} 重連 thread 起不來 "
                                f"({type(e).__name__}: {e}) — 看門狗下一輪再試")

    def _reconnect_delay(self, attempt: int) -> float:
        """第 attempt 次嘗試前等多久: 2,4,8,16,32,60,60… s (同 day-trade 生產分支;不設次數上限)。"""
        return min(self._reconnect_step_sec * (2 ** (min(attempt, 6) - 1)), self._reconnect_max_sec)

    def _should_rebuild(self, idx: int, consec_fail: int) -> bool:
        """這一次嘗試要不要換全新 session (新 SDK 登入)。有登入資訊、過了冷卻,而且: 上一次是認證類失敗 /
        舊收訊迴圈不結束 / 連線數達上限 (want_rebuild),或同一個物件已連續失敗 MD_REBUILD_AFTER_FAILS 次。"""
        st = self._sock_state[idx]
        if idx >= len(self._sock_creds) or not self._sock_creds[idx]:
            return False
        if not (st["want_rebuild"] or consec_fail >= self._rebuild_after_fails):
            return False
        last = st["last_rebuild_mono"]
        return last <= 0 or time.monotonic() - last >= self._rebuild_cooldown_sec

    def _reconnect_loop(self, idx: int):
        """單條 socket 的重連迴圈 (single-flight = st["reconnecting"];比照 day-trade,不設次數上限)。
        連上、重訂完,而且這期間沒有再收到關閉事件,才把 socket 標回正常;標回之後若又斷了就留在迴圈。"""
        st = self._sock_state[idx]
        attempt = 0
        consec_fail = 0
        # 上一次「重連成功」沒撐過 MD_FLAP_SEC 就又斷了 (連上、認證過、馬上被踢) → 接著上一輪的次數繼續退避、
        # 累計失敗 (夠了就換全新 session)。每次都從 2 s 重來的話: 一條 socket 每分鐘開 20 幾條連線、永遠不換
        # session,而券商對短時間大量連線是會擋的
        if st["up_mono"] > 0 and st["down_mono"] - st["up_mono"] < self._flap_sec:
            attempt = st["carry_attempt"]
            consec_fail = st["carry_fail"] + 1
        handed_back = False                      # True = 已在鎖內把 single-flight 旗標交回去了
        try:
            while True:
                attempt += 1
                delay = max(self._reconnect_delay(attempt), st["cap_until"] - time.monotonic())
                if self._stop.wait(delay):
                    return
                how = ""
                try:
                    if self._should_rebuild(idx, consec_fail):
                        self._rebuild_socket(idx)
                        how = ",全新 session"
                        consec_fail = 0
                    else:
                        self._close_old_connection(idx)
                    if self._stop.is_set():
                        return                   # stop() 之後不再開新連線
                    self._open_socket(idx)
                    nb, nt, nf = self._resubscribe_socket(idx)
                except (KeyboardInterrupt, SystemExit):
                    raise
                except BaseException as e:      # noqa: BLE001
                    # BaseException: 換 session 會呼叫 FubonSDK() / login() / init_realtime() (Rust),panic 是
                    # pyo3 的 PanicException (繼承 BaseException)。漏接的話這條 thread 無聲死掉、旗標卡住 →
                    # 這條 socket 整天沒人重連。當成又一次失敗的嘗試,照退避繼續
                    consec_fail += 1
                    if isinstance(e, (_MdAuthError, _MdOldLoopAlive)):
                        st["want_rebuild"] = True
                    log = logger.critical if attempt % 10 == 0 else logger.warning
                    log(f"[subscriber] 行情 socket #{idx+1} 重連失敗 (第 {attempt} 次{how},"
                        f"已斷 {time.monotonic() - st['down_mono']:.0f} s): {type(e).__name__}: {e}")
                    continue
                with self._sock_lock:
                    closed_again = st["loop_closed"]     # 重訂期間新連線又關了 → 這次不算成功
                    if not closed_again:
                        down_for = time.monotonic() - st["down_mono"]
                        st["up"] = True
                        st["down_since"] = None
                        st["reconnects"] += 1
                        st["want_rebuild"] = False
                        st["last_msg_mono"] = st["up_mono"] = time.monotonic()
                        st["last_ping_mono"] = st["first_ping_mono"] = 0.0
                        st["carry_attempt"] = attempt
                        st["carry_fail"] = consec_fail
                if closed_again:
                    consec_fail += 1
                    logger.warning(f"[subscriber] 行情 socket #{idx+1} 剛連上就又斷了 (第 {attempt} 次) → 再試")
                    continue
                logger.critical(f"[subscriber] ✅ 行情 socket #{idx+1} 重連成功 (第 {attempt} 次嘗試{how},斷線 "
                                f"{down_for:.1f} s),已重訂 books {nb} 檔 / trades {nt} 檔"
                                f"{f'、{nf} 筆送失敗' if nf else ''} — 這些檔在收到新的五檔之前仍視為過期")
                with self._sock_lock:
                    if st["up"] or self._stop.is_set():
                        st["reconnecting"] = False
                        handed_back = True
                        return
                consec_fail += 1                 # 標回正常後馬上又斷 → 算一次失敗,接著退避 (不歸零)
        finally:
            if not handed_back:
                # stop / thread 異常結束: 放掉 single-flight (還斷著的話看門狗會重新啟動重連)
                with self._sock_lock:
                    st["reconnecting"] = False

    def _old_loop_running(self, idx: int) -> bool:
        """目前這個物件的收訊迴圈還在跑嗎。收訊 thread 是我們起的 → 只看 thread (不依賴 disconnect 事件,
        舊版 websocket-client 重用物件時不會再發);沒有 handle (公開 connect() 那條路) 才看關閉事件。"""
        st = self._sock_state[idx]
        th = st["rx_thread"]
        if th is not None:
            return th.is_alive()
        return not st["loop_closed"]

    def _rx_main(self, idx: int, gen: int, run):
        """我們自己起的收訊 thread: 跑函式庫的 run_forever,結束時 (不管怎麼結束) 當成這條連線已關閉。"""
        try:
            run()
        except BaseException as e:      # noqa: BLE001 — run_forever 自己丟出來的也只是「連線結束」
            logger.warning(f"[socket#{idx}] 收訊迴圈例外結束: {type(e).__name__}: {e}")
        finally:
            st = self._sock_state[idx]
            if st["gen"] == gen:
                st["loop_closed"] = True
                st["closed_ev"].set()
                if not self._stop.is_set():
                    self._socket_down(idx, "收訊迴圈結束 (連線已關閉)")

    def _close_old_connection(self, idx: int):
        """重連前先 disconnect() 舊連線 (比照 day-trade): 重設函式庫的認證狀態、取消舊的認證 / 心跳計時器。
        舊連線的收訊迴圈還活著 (停擺被看門狗抓到、或上一次嘗試剛失敗) → 等它結束,最多 MD_CLOSE_WAIT_SEC
        (Linux 上要 ~10 s 才醒);還不結束就 raise _MdOldLoopAlive — 絕不在同一個物件上開第二條。"""
        ws = self._sockets[idx]
        st = self._sock_state[idx]
        running = self._old_loop_running(idx)
        try:
            ws.disconnect()
        except Exception as e:
            logger.info(f"[subscriber] socket #{idx+1} 重連前 disconnect() 例外 (略過): {e}")
        if running:
            deadline = time.monotonic() + self._close_wait_sec
            while self._old_loop_running(idx) and time.monotonic() < deadline:
                if self._stop.wait(0.05):
                    break
            if self._old_loop_running(idx):
                if st["rx_thread"] is not None:
                    raise _MdOldLoopAlive(f"舊連線的收訊迴圈 {self._close_wait_sec:g} s 內沒有結束")
                # 沒有 thread handle (拿不到函式庫內部欄位、走公開 connect() 的退路): 只能靠關閉事件,而它
                # 不一定會來 → 等過上限就當它已結束 (盡力而為;正常的路有 handle,不會走到這裡)
                st["loop_closed"] = True
        self._stop.wait(self._reconnect_settle_sec)

    def _rebuild_socket(self, idx: int):
        """這條 socket 換**全新 session**: 新的 FubonSDK login + init_realtime → 新的 client 物件,掛上同一套
        事件 (新世代;舊物件晚到的事件丟棄),取代 self._sockets[idx]。之後由 _open_socket 連線。
        先把舊連線關掉 (釋放券商端的連線名額);換下來的 SDK 若不是 primary_sdk 就 logout (背景、盡力而為 —
        logout 才會停掉它的原生忙等 thread,也才釋放名額;primary_sdk 主流程還要用來打 REST,不動)。
        登入失敗 → raise (算一次失敗的嘗試;冷卻照算,不會每輪都重登)。"""
        st = self._sock_state[idx]
        creds = self._sock_creds[idx]
        label, acct, pwd, pfx, pfxpwd = creds
        with self._rebuild_lock:
            st["last_rebuild_mono"] = time.monotonic()
            from fubon_neo.sdk import FubonSDK
            old_ws = self._sockets[idx]
            old_sdk = self._sock_sdks[idx]
            try:
                old_ws.disconnect()
            except Exception:
                pass
            if st["cap_until"] > 0 and old_sdk is not None and old_sdk is not self.primary_sdk:
                self._logout_sdk(old_sdk, idx, wait=True)      # 名額滿: 先確實登出舊的再登入
                old_sdk = None
            sdk = FubonSDK()
            try:
                if pfxpwd == "":
                    accounts = sdk.login(acct, pwd, pfx)
                else:
                    accounts = sdk.login(acct, pwd, pfx, pfxpwd)
                if not accounts:
                    raise RuntimeError("換 session: login 回空 accounts")
                if getattr(accounts, "is_success", None) is False:
                    raise RuntimeError(f"換 session: login is_success=False: {getattr(accounts, 'message', '?')}")
                sdk.init_realtime()
                ws = sdk.marketdata.websocket_client.stock
                gen = st["gen"] + 1
                ws.on("message", self._make_msg_handler(socket_idx=idx, gen=gen))
                ws.on("disconnect", self._make_disconnect_handler(idx, gen))
                ws.on("error", self._make_error_handler(idx, gen))
            except BaseException:
                # 登入之後的任何一步失敗 (換行情 token 失敗、Rust panic …): 這個新 SDK 沒有人會用到,一定要登出 —
                # 不登出就留下一條原生忙等 thread + 佔一個連線名額,而且每 60 s 冷卻又來一個 (同 broker.re_login
                # 對失敗的新 SDK 的做法)
                self._logout_sdk(sdk, idx, wait=False)
                raise
            with self._sock_lock:
                st["gen"] = gen
                st["rx_thread"] = None
                st["loop_closed"] = True                     # 新物件還沒有收訊迴圈
                st["closed_ev"].clear()
                st["rebuilds"] += 1
                st["cap_until"] = 0.0
                self._sockets[idx] = ws
                self._sock_sdks[idx] = sdk
            self._extra_sdks.append(sdk)
        logger.critical(f"[subscriber] 行情 socket #{idx+1} 已換全新 session ({label} "
                        f"{str(acct)[:3]}***,第 {st['rebuilds']} 次) — 接著連線、重訂")
        if old_sdk is not None and old_sdk is not self.primary_sdk:
            self._logout_sdk(old_sdk, idx, wait=False)
        self._stop.wait(self._reconnect_settle_sec)

    def _logout_sdk(self, sdk, idx: int, wait: bool):
        """登出換下來的資料 SDK (不是 primary_sdk 才呼叫)。logout 偶爾會卡很久 → 放背景 thread;
        wait=True 時最多等 10 s (連線名額滿的情況要先確實釋放)。"""
        try:
            self._extra_sdks.remove(sdk)
        except ValueError:
            pass

        def _do():
            try:
                sdk.logout()
            except BaseException as e:      # noqa: BLE001 — 盡力而為;原生 SDK 的 panic 也只記一行
                logger.info(f"[subscriber] socket #{idx+1} 舊 session logout 例外 (略過): "
                            f"{type(e).__name__}: {e}")

        t = threading.Thread(target=_do, name=f"md-logout-{idx}", daemon=True)
        t.start()
        if wait:
            t.join(10.0)

    def _open_socket(self, idx: int):
        """開連線並等到認證完成 (開機第一次與之後每次重連都走這裡);失敗 / 逾時 raise。
        **一定會返回** (函式庫的 connect() 連不上時不會,見檔頭 1)。
        首選: 自己起收訊 thread (函式庫 connect() 做的同一件事) 再用 sleep 輪詢認證狀態,沒有忙等;
        拿不到函式庫內部的 WebSocketApp (版本不同 / 測試替身) → 退回公開的 connect(),放在 helper thread 並
        在逾時 / 連線先關掉時把 auth_status 改成「認證失敗」讓它跳出。
        成功 = 已認證 + 沒有收到關閉事件。失敗分三種 (決定要不要換 session):
          _MdAuthError       連線開了但認證失敗 / 逾時
          _MdNotEstablished  連線根本沒建立
          _MdOldLoopAlive    舊收訊 thread 還在 (呼叫前應先 _close_old_connection)"""
        ws = self._sockets[idx]
        st = self._sock_state[idx]
        prev = st["rx_thread"]
        if prev is not None and prev.is_alive():
            raise _MdOldLoopAlive("舊連線的收訊 thread 還在")
        inner = getattr(ws, "_client", ws)                      # fubon_neo 的 wrapper 包了一層
        app = getattr(inner, "_WebSocketClient__ws", None)       # fugle_marketdata 2.4.1 的 WebSocketApp
        run = getattr(app, "run_forever", None) if app is not None else None
        st["closed_ev"].clear()
        st["loop_closed"] = False
        deadline = time.monotonic() + self._connect_timeout_sec
        if callable(run) and hasattr(inner, "auth_status"):
            th = threading.Thread(target=self._rx_main, args=(idx, st["gen"], run),
                                  name=f"md-ws-{idx}", daemon=True)
            st["rx_thread"] = th
            th.start()
            fail: Optional[Exception] = None
            while True:
                status = getattr(inner, "auth_status", None)
                if status in (_AUTH_OK, _AUTH_FAILED):
                    break
                if st["closed_ev"].is_set() or not th.is_alive():
                    fail = _MdNotEstablished("連線沒有建立 (連不上 / 被拒 / 認證完成前就關閉)")
                    break
                if time.monotonic() >= deadline:
                    fail = _MdAuthError(f"{self._connect_timeout_sec:g} s 內未完成認證")
                    break
                if self._stop.wait(0.02):
                    fail = _MdNotEstablished("subscriber 已停止")
                    break
            err = getattr(inner, "error", None)                 # 關連線之前先抓住 (disconnect() 會清掉)
            status = getattr(inner, "auth_status", None)
            if fail is None and err is None and status == _AUTH_OK and not st["closed_ev"].is_set():
                return
            if fail is None:
                fail = (_MdAuthError(f"認證失敗: {err}") if err is not None or status == _AUTH_FAILED
                        else _MdNotEstablished("連線在認證完成時已關閉"))
            try:
                ws.disconnect()                                 # 同函式庫 connect() 失敗時: 關掉 + 取消計時器
            except Exception:
                pass
            raise fail
        box: dict = {}

        def _run():
            try:
                ws.connect()
                box["ok"] = True
            except BaseException as e:      # noqa: BLE001 — 原樣帶回
                box["err"] = e

        th = threading.Thread(target=_run, name=f"md-connect-{idx}", daemon=True)
        th.start()
        fail = None
        while th.is_alive():
            if st["closed_ev"].is_set():
                fail = _MdNotEstablished("連線沒有建立 (認證完成前就關閉)")
            elif time.monotonic() >= deadline:
                fail = _MdAuthError(f"{self._connect_timeout_sec:g} s 內 connect() 未返回")
            elif self._stop.is_set():
                fail = _MdNotEstablished("subscriber 已停止")
            if fail is not None:
                try:                                            # 讓函式庫的忙等迴圈跳出
                    if getattr(inner, "error", None) is None:
                        inner.error = fail
                    inner.auth_status = _AUTH_FAILED
                except Exception:
                    pass
                th.join(2.0)
                break
            th.join(0.02)
        if box.get("ok") and fail is None:
            return
        if fail is None:
            err = box.get("err")
            text = str(err) if err is not None else "connect() 未返回"
            fail = (_MdAuthError(text) if "auth" in text.lower() or "認證" in text
                    else _MdNotEstablished(text))
        raise fail

    def _resubscribe_socket(self, idx: int) -> tuple:
        """重連後把這條 socket 的訂閱補回來: books (這一輪的批次 + extras − 已淘汰) 與訂過的 trades。
        舊連線的 subscription id 全部作廢 (has_ack 要等新的 ack)。回 (books 數, trades 數, 送失敗數)。
        ⚠ 先把 epoch +1 再送訂閱: 送出後馬上回來的 books 才算「重連後的新資料」。"""
        ws = self._sockets[idx]
        st = self._sock_state[idx]
        syms = self._symbols_on_socket(idx)
        with self._sub_ids_lock:
            for key in [k for k in self._sub_ids if self._socket_index(k[1]) == idx]:
                self._sub_ids.pop(key, None)
        st["epoch"] += 1
        nb = nt = nf = 0
        for sym in syms:
            try:
                ws.subscribe({"channel": "books", "symbol": sym})
                nb += 1
            except Exception:
                nf += 1
        with self._removed_lock:
            removed = set(self._removed)
        for sym in sorted(self._trade_subs):
            if self._socket_index(sym) != idx or sym in removed:
                continue
            try:
                ws.subscribe({"channel": "trades", "symbol": sym})
                nt += 1
            except Exception:
                nf += 1
        if nb + nt == 0 and nf > 0:
            raise _MdNotEstablished(f"重訂全部送失敗 ({nf} 筆) — 連線可能又斷了")
        return nb, nt, nf

    def _watchdog_loop(self):
        while not self._stop.is_set():
            self._stop.wait(self._watchdog_tick_sec)
            if self._stop.is_set():
                return
            try:
                self._watchdog_once()
            except Exception as e:
                logger.exception(f"[subscriber] 行情看門狗例外 (下一輪繼續): {e}")

    def _watchdog_once(self, now_m: Optional[float] = None):
        """看門狗一輪: 安靜的 socket 送 ping (伺服器回 pong = 一則訊息);第一個沒被回應的 ping 送出後又過了
        (MD_STALL_SEC − MD_IDLE_PING_SEC) 秒仍然沒有任何訊息 → 停擺,強制重連。以「沒被回應的 ping」計時而不是
        單看安靜多久: 整個 process 被凍住 (例如全清單查詢) 之後醒來,要先給它一次回應的機會。"""
        now_m = time.monotonic() if now_m is None else now_m
        grace = max(self._stall_sec - self._idle_ping_sec, self._idle_ping_sec)
        for idx, st in enumerate(self._sock_state):
            if not st["up"] and not st["reconnecting"]:
                # 斷線中卻沒有重連 thread 在跑 (thread 起不來 / 異常結束) — 不可以就這樣停在斷線
                self._socket_down(idx, st["down_reason"] or "重連中斷")
                continue
            if not st["up"] or st["reconnecting"]:
                continue
            th = st["rx_thread"]
            if th is not None and not th.is_alive():
                self._socket_down(idx, "收訊 thread 已結束")      # 正常由 _rx_main 的 finally 先標;這裡兜底
                continue
            last_msg = st["last_msg_mono"]
            idle = now_m - last_msg
            waiting = st["first_ping_mono"] > last_msg           # 有還沒被回應的 ping
            if self._stall_sec > 0 and waiting and now_m - st["first_ping_mono"] >= grace:
                self._socket_down(idx, f"{idle:.0f} s 沒有任何訊息 (ping 也沒有回應)")
                continue
            if idle >= self._idle_ping_sec and now_m - st["last_ping_mono"] >= self._idle_ping_sec:
                st["last_ping_mono"] = now_m
                if not waiting:
                    st["first_ping_mono"] = now_m
                try:
                    self._sockets[idx].ping("wd")
                except Exception as e:
                    self._socket_down(idx, f"ping 送不出去: {type(e).__name__}: {e}")

    # ─── Multi-SDK: 開 N 個 login sessions ───────────────────

    @staticmethod
    def _account_plan(cfg) -> list:
        """收資料帳號計畫 [(label, account_id, password, pfx_path, pfx_password, n_sockets)]。

        主帳號 + 有設定的副帳號們 (最多兩個);每帳號 socket 數各自可調
        (SOCKET_COUNT / SOCKET_COUNT_2 / SOCKET_COUNT_3,富邦每帳號上限 5 條)。
        純函式 — 可離線測試。"""
        if cfg is None:
            return []
        primary_n = max(1, int(getattr(cfg, "socket_count", 5) or 5))
        plan = [("primary", cfg.account_id, cfg.password,
                 cfg.pfx_path, cfg.pfx_password, primary_n)]
        for label, sfx in (("secondary", "_2"), ("tertiary", "_3")):
            acct = getattr(cfg, f"account_id{sfx}", "") or ""
            pfx = getattr(cfg, f"pfx_path{sfx}", "") or ""
            if acct and pfx:
                n = int(getattr(cfg, f"socket_count{sfx}", 0) or 0) or primary_n
                plan.append((label, acct,
                             getattr(cfg, f"password{sfx}", "") or "",
                             pfx, getattr(cfg, f"pfx_password{sfx}", "") or "",
                             max(1, n)))
        return plan

    def _try_multi_sdk(self) -> bool:
        """照 account plan 開 sockets — 主帳號 socket #1 沿用已登入的 primary_sdk,
        其餘每條各自 login 一個新 SDK。單條登入失敗自動重試 (見 _login_extra_sdk);
        某帳號 0 條成功 → CRITICAL 點名 (2026-08-10: 副帳號憑證異常只有 WARNING,
        無聲少收一半資料才被人工發現)。"""
        try:
            from fubon_neo.sdk import FubonSDK
        except ImportError:
            logger.error("[subscriber] fubon_neo 未安裝")
            return False

        sockets = []
        sdks = []                    # 與 sockets 對齊: 每條 socket 的資料 SDK
        creds = []                   # 與 sockets 對齊: 換全新 session 用的登入資訊 (不寫進任何 log)
        summary = []
        for label, acct, pwd, pfx, pfxpwd, n in self._account_plan(self.login_cfg):
            opened = 0
            for i in range(n):
                if label == "primary" and i == 0:
                    sockets.append(self.primary_sdk.marketdata.websocket_client.stock)
                    sdks.append(self.primary_sdk)
                    creds.append((label, acct, pwd, pfx, pfxpwd))
                    opened += 1
                    continue
                ws = self._login_extra_sdk(FubonSDK, acct, pwd, pfx, pfxpwd,
                                           label=f"{label} #{i+1}/{n}")
                if ws is None:
                    logger.error(f"[subscriber] {label} 只開 {opened}/{n} 條就放棄")
                    break
                sockets.append(ws)
                sdks.append(self._extra_sdks[-1] if self._extra_sdks else None)
                creds.append((label, acct, pwd, pfx, pfxpwd))
                opened += 1
            if opened == 0:
                logger.critical(f"[subscriber] ⚠⚠ {label} 帳號 ({str(acct)[:3]}***) 一條 socket "
                                f"都沒開成 — 監控容量少 {n * self.batch_size} 檔,"
                                f"盡快檢查該帳號憑證/密碼!")
            summary.append(f"{opened} {label}")
        self._sockets = sockets
        self._sock_sdks = sdks
        self._sock_creds = creds
        logger.info(f"[subscriber] 共開 {len(sockets)} 個 sockets ({' + '.join(summary)})")
        return len(sockets) >= 2

    def _login_extra_sdk(self, FubonSDK, account_id, password, pfx_path, pfx_password,
                         label="", retries=2):
        """開一個新 SDK login + init_realtime + 回 ws client。
        失敗自動重試 retries 次 (間隔 2s) — 暫時性登入失敗不再一次就放棄整條
        (2026-08-10 教訓)。全失敗回 None。"""
        import time as _t
        for attempt in range(1 + retries):
            try:
                sdk = FubonSDK()
                if pfx_password == "":
                    accounts = sdk.login(account_id, password, pfx_path)
                else:
                    accounts = sdk.login(account_id, password, pfx_path, pfx_password)
                if not accounts:
                    raise RuntimeError("login 回空 accounts")
                if getattr(accounts, "is_success", None) is False:
                    raise RuntimeError(f"is_success=False: {getattr(accounts, 'message', '?')}")
                sdk.init_realtime()
                self._extra_sdks.append(sdk)
                logger.info(f"[subscriber] {label} login OK"
                            f"{f' (第 {attempt + 1} 次嘗試)' if attempt else ''}")
                return sdk.marketdata.websocket_client.stock
            except Exception as e:
                if attempt < retries:
                    logger.warning(f"[subscriber] {label} login 失敗 (第 {attempt + 1} 次,"
                                   f"2 秒後重試): {e}")
                    _t.sleep(2)
                else:
                    logger.error(f"[subscriber] {label} login 失敗 (共試 {attempt + 1} 次後放棄): {e}")
        return None

    # ─── Multi-socket 主 loop ────────────────────────────────

    def _start_multi_socket_loop(self):
        """N 個 sockets 平行監控。若母體 > N×batch_size 仍需循環切批。

        關鍵: ws.connect() 是非同步 — subscribe 必須等 connect callback fire 才生效。
        (Alex worker.py 也是在 on_connected 內做 subscribe，之前 0 tick 就是這個坑。)
        """
        # 檢查 universe vs total capacity — 若母體太大要 truncate 或警告
        n_sockets = len(self._sockets)
        capacity = self.batch_size * n_sockets
        if len(self.universe) > capacity:
            # CRITICAL — 2026-08-10: 副帳號憑證掛掉只有 WARNING → 無聲少收一半資料
            logger.critical(f"[subscriber] ⚠⚠ universe {len(self.universe)} 檔 > 監控容量 "
                            f"{capacity} 檔 ({n_sockets} sockets × {self.batch_size}) — "
                            f"有帳號沒開成 socket?")
            logger.critical(f"[subscriber] ⚠⚠ 只監控前 {capacity} 檔 (按 symbol 排序)，"
                            f"剩下 {len(self.universe) - capacity} 檔**收不到資料**")
            self.universe = self.universe[:capacity]
        else:
            logger.info(f"[subscriber] universe {len(self.universe)} 檔 <= 容量 {capacity} 檔，"
                        f"全部一次性 subscribe 常駐 (不 rotation)")
        self._rebuild_symbol_index()      # universe 定案 (truncate 後) → 建 O(1) 路由表
        self._init_socket_health()        # connect 之前掛 disconnect/error 事件 (斷線自動重連)
        connected_events = []
        for i, ws in enumerate(self._sockets):
            ev = threading.Event()
            connected_events.append(ev)
            try:
                # closure 捕獲 i / ev
                def _on_conn(_e=ev, _i=i):
                    _e.set()
                    logger.info(f"[subscriber] socket #{_i+1} connected event fired ✓")
                ws.on("connect", _on_conn)
                ws.on("message", self._make_msg_handler(socket_idx=i))
                self._open_socket(i)              # 有期限、不忙等;返回時已認證
                logger.info(f"[subscriber] socket #{i+1} connect() 已呼叫 (等 broker ready)")
            except Exception as e:
                logger.error(f"[subscriber] socket #{i+1} connect 失敗: {e}")
                self._socket_down(i, f"開機連線失敗: {e}")   # 交給自動重連 (舊版這條就整天不收)

        per_socket = self._split_universe_per_socket()

        for i, syms_for_socket in enumerate(per_socket):
            batches = self._make_batches(syms_for_socket, self.batch_size)
            t = threading.Thread(
                target=self._socket_loop_thread,
                args=(self._sockets[i], batches, i, connected_events[i]),
                name=f"subscriber-socket-{i}", daemon=True,
            )
            t.start()
            self._threads.append(t)

        total_batches = sum(len(self._make_batches(x, self.batch_size)) for x in per_socket)
        max_per_socket = max((len(b) for b in per_socket), default=0)
        rounds_per_full_cycle = (max_per_socket + self.batch_size - 1) // self.batch_size
        logger.info(f"[subscriber] 全母體 {len(self.universe)} 檔切成 "
                    f"{total_batches} 批 (每 socket 最多 {rounds_per_full_cycle} 批)")

    def _split_universe_per_socket(self) -> List[List[str]]:
        """把 universe 平分給 N 個 socket。
        用 round-robin 讓每個 socket 拿到「散開」的 symbol (避免某 socket 全拿 0-1000 檔)。
        """
        n = len(self._sockets)
        per_socket: List[List[str]] = [[] for _ in range(n)]
        for i, sym in enumerate(self.universe):
            per_socket[i % n].append(sym)
        return per_socket

    def _socket_loop_thread(self, ws, batches: List[List[str]], socket_idx: int,
                             connected_event: Optional[threading.Event] = None):
        """單個 socket 的循環 thread — 若只有 1 批就常駐訂閱，多批就輪播。

        Alex worker.py 首次啟動 pattern: connect() 後立刻 subscribe，不等 event。
        (on_connected callback 只用於重連時。)

        connected_event 只作為診斷用: 若 30 秒都沒 fire → warn，幫助定位問題。
        """
        # 診斷: 開個背景 thread 等 event，30 秒後若沒 fire 就 warn (不阻塞 subscribe)
        if connected_event is not None:
            def _diagnose():
                if not connected_event.wait(timeout=30):
                    logger.warning(f"[socket#{socket_idx}] ⚠ 30 秒內 on('connect') callback "
                                   f"從未觸發 — SDK 內部可能異常，但仍會嘗試 subscribe")
                else:
                    logger.info(f"[socket#{socket_idx}] on_connect 有觸發 ✓")
            threading.Thread(target=_diagnose, name=f"diag-{socket_idx}", daemon=True).start()

        # 給 broker 一點時間完成 handshake (跟 Alex 一致 — connect 後不等就 subscribe，
        # 但至少 sleep 2s 給 TLS + auth 時間)
        # fugle-marketdata 2.4.1 (pin) 的 ws.connect() 是同步的: 忙等到 auth_status 為
        # AUTHENTICATED/UNAUTHENTICATED 才返回 (失敗 raise) — 進到這裡 socket 已完成認證,
        # add_symbol 的補訂現在就送得進去;下面的 grace 只是開機批次的保守等待 (維持舊行為)
        self._ready.set()
        self._stop.wait(self._connect_grace_sec)
        if not batches:
            # 開機母體空 (node 首份快照 0 檔且無隔日賣 — 2026-10-06 六台即此) → 以一個空批常駐:
            # 每輪只重訂 add_symbol 補進來的 extras 然後 wait。舊版 for 迴圈體從不執行、沒有任何
            # wait → 100% CPU 忙轉到收盤,跟補收/預掛/行情 callback 搶 GIL
            batches = [[]]
            logger.info(f"[socket#{socket_idx}] 開機母體空 — 常駐等待 add_symbol 補訂 "
                        f"(每 {self.batch_rotate_sec}s 重訂 extras)")

        round_num = 0
        while not self._stop.is_set():
            round_num += 1
            for i, batch in enumerate(batches, 1):
                # 這條 socket 斷線 / 重連中 → 這一輪先不送 (送了只會整批失敗;重連成功時 _resubscribe_socket
                # 會把訂閱補回來),等它回來再繼續
                while not self._stop.is_set() and not self._socket_is_up(socket_idx):
                    self._stop.wait(1.0)
                if self._stop.is_set():
                    return
                if socket_idx < len(self._sockets):
                    ws = self._sockets[socket_idx]      # 換全新 session 後 client 物件會換,每輪重拿
                # subscribe 這批 — 只 books，不訂 trades (省一半 sub 額度，每檔佔 1 sub)
                # 已淘汰 (removed) 的股票跳過 — 否則 pruner 退訂完又被這裡訂回
                sub_ok = 0
                sub_fail = 0
                first_err_sample = None
                with self._removed_lock:
                    removed_snapshot = set(self._removed)
                # 盤中補訂的檔 (add_symbol) 歸這條 socket 的也一起重訂 (每輪一次,跟著第 1 批) —
                # 否則 socket 重連後漏掉 (開機 batches 是固定的)
                extras: list = []
                if i == 1:
                    with self._extra_lock:
                        extras = [s for s in sorted(self._extra_symbols)
                                  if s not in batch and self._socket_index(s) == socket_idx]
                self._cur_batch[socket_idx] = list(batch)   # 重連時照這份補訂 (extras 另外即時算)
                for sym in list(batch) + extras:
                    if sym in removed_snapshot:
                        continue
                    try:
                        ws.subscribe({"channel": "books", "symbol": sym})
                        sub_ok += 1
                    except Exception as e:
                        sub_fail += 1
                        if first_err_sample is None:
                            first_err_sample = (sym, str(e)[:150])
                if sub_fail > 0:
                    logger.warning(f"[socket#{socket_idx}] round {round_num} batch {i}: "
                                   f"subscribe {sub_ok} ok / {sub_fail} fail | "
                                   f"first_err={first_err_sample}")

                if len(batches) == 1:
                    # 只有 1 批 → 常駐，不必輪播
                    logger.info(f"[socket#{socket_idx}] round {round_num} 訂 {len(batch)} 檔"
                                f"{f' + 補訂 {len(extras)} 檔' if extras else ''} (常駐)")
                    self._stop.wait(self.batch_rotate_sec)
                    continue

                logger.info(f"[socket#{socket_idx}] round {round_num} batch {i}/{len(batches)}: "
                            f"訂 {len(batch)} 檔 (停 {self.batch_rotate_sec}s)")
                self._stop.wait(self.batch_rotate_sec)
                if self._stop.is_set():
                    return

                # 多批要 unsubscribe 換下一批 — 只 books channel
                for sym in batch:
                    with self._sub_ids_lock:
                        sub_id = self._sub_ids.pop(("books", sym), None)
                    if not sub_id:
                        continue
                    try:
                        ws.unsubscribe({"id": sub_id})
                    except Exception:
                        pass

    # ─── Fallback: 保底單 socket 循環 ────────────────────────

    def _start_loop_single_socket(self):
        ws = self._sockets[0]
        self._init_socket_health()        # connect 之前掛 disconnect/error 事件 (斷線自動重連)
        connected_event = threading.Event()
        try:
            def _on_conn(_e=connected_event):
                _e.set()
                logger.info("[subscriber] 保底 socket connected event fired ✓")
            ws.on("connect", _on_conn)
            ws.on("message", self._make_msg_handler(socket_idx=0))
            self._open_socket(0)                  # 有期限、不忙等;返回時已認證
            logger.info("[subscriber] 保底 socket connect() 已呼叫")
        except Exception as e:
            # 舊版在這裡 raise → 整台當天不收行情 (隔日賣也賣不掉)。現在交給自動重連;連回來之前這條連線上的
            # 檔都算斷線中,runner 不會為它們預掛 / 盲送 (runner._stale_marked)
            logger.error(f"[subscriber] 保底 socket connect 失敗: {e}")
            self._socket_down(0, f"開機連線失敗: {e}")

        self._rebuild_symbol_index()      # 保底路線也要建 O(1) 路由表
        batches = self._make_batches(self.universe, self.batch_size)
        t = threading.Thread(
            target=self._socket_loop_thread,
            args=(ws, batches, 0, connected_event),
            name="subscriber-fallback", daemon=True,
        )
        t.start()
        self._threads.append(t)

    # ─── Message 分派 ───────────────────────────────────────

    def _make_msg_handler(self, socket_idx: int, gen: int = 0):
        """Per-socket message handler.
        gen = 這個 handler 掛在第幾代的 client 物件上;換全新 session 後舊物件晚到的訊息直接丟掉。

        富邦 SDK 傳給 handler 的 message 是 JSON string (Alex fubon_adapter.py:665)。
        Schema: {"event": "data"|"pong"|"subscribed"|..., "data": {channel, symbol, bids, asks, price, size}}
        只有 event=="data" 才是 tick，其他是 subscribed ack / pong / error 之類。
        """
        def _handler(msg):
            self._tick_counter += 1
            # 連線活著的證據: 任何一則訊息 (data / subscribed / pong …) — 看門狗據此判停擺
            now_m = time.monotonic()
            now_wall = time.time()
            ss = self._sock_state
            message_epoch = None
            if socket_idx < len(ss):
                if ss[socket_idx]["gen"] != gen:
                    return
                message_epoch = (socket_idx, gen, ss[socket_idx]["epoch"])
                ss[socket_idx]["last_msg_mono"] = now_m
            try:
                # 富邦 message 是 JSON string — 先 parse (若已是 dict 也接受)
                if isinstance(msg, str):
                    parsed = json.loads(msg)
                elif isinstance(msg, dict):
                    parsed = msg
                else:
                    return

                event = parsed.get("event")

                # 非 data event (subscribed / pong / unsubscribed / error)
                if event != "data":
                    if event == "error":
                        # error dedup — 同樣 message 前 3 次印，之後每 500 次補印一次
                        err_msg = str(parsed.get("data", {}).get("message", "?"))
                        cnt = self._error_count.get(err_msg, 0) + 1
                        self._error_count[err_msg] = cnt
                        if cnt <= 3 or cnt % 500 == 0:
                            logger.warning(f"[socket#{socket_idx}] error #{cnt}: {err_msg}")
                    elif event == "subscribed":
                        # ★ 存 subscription id — unsubscribe 需要它
                        d = parsed.get("data", {})
                        sym = d.get("symbol")
                        ch = d.get("channel")
                        sub_id = d.get("id")
                        if sym and ch and sub_id:
                            with self._sub_ids_lock:
                                self._sub_ids[(ch, sym)] = sub_id
                        if self._tick_counter <= 20:
                            logger.info(f"[socket#{socket_idx}] event=subscribed "
                                        f"{ch}/{sym} id={sub_id[:12] if sub_id else '?'}...")
                    elif event == "unsubscribed":
                        # 清 dict — 已經被 broker 端撤了
                        d = parsed.get("data", {})
                        sym = d.get("symbol")
                        ch = d.get("channel")
                        if sym and ch:
                            with self._sub_ids_lock:
                                self._sub_ids.pop((ch, sym), None)
                        if self._tick_counter <= 20:
                            logger.info(f"[socket#{socket_idx}] event=unsubscribed {ch}/{sym}")
                    return

                # event == "data" → tick
                # 富邦 schema (llms-full.txt:5054): channel 跟 id 在頂層，symbol 在 data 內
                # {"event": "data", "data": {"symbol": ..., "bids": [], "asks": []},
                #  "id": "xxx", "channel": "books"}
                channel = parsed.get("channel")       # ← 從頂層拿
                data = parsed.get("data") or {}
                symbol = data.get("symbol")
                if not symbol:
                    return

                # 分 channel 計數 給 heartbeat log 用
                if channel == "books":
                    self._books_count += 1
                elif channel == "trades":
                    self._trades_count += 1

                # ⚡ handler **最先跑** — 觸發市價追的那筆 tick 不等 snapshot 鎖/落檔
                # (2026-08 修: 原順序 record→snapshot→handler,每 tick 先付一次磁碟+鎖)。
                # handler 例外獨立捕捉 → 掛掉也不影響後面的 snapshot/落檔。
                if channel == "books":
                    bids = data.get("bids") or []
                    asks = data.get("asks") or []
                    if socket_idx < len(ss):                 # 這檔在目前這段連線上收到過五檔 (is_stale 用)
                        self._last_book_epoch[symbol] = ss[socket_idx]["epoch"]
                    try:
                        # isContinuous = 交易所權威「已逐筆(開盤後)」旗標 → 傳給 handler
                        # (filter 靠它退場,免拿開盤市價列 bids[0]=0 反推;2026-08-27 8105/1312)
                        callback = self.on_book
                        context = ({"timestamp": data.get("time"), "received_at": now_wall,
                                    "received_mono": now_m, "feed_epoch": message_epoch}
                                   if callback is self._context_book else {})
                        callback(symbol, bids, asks, bool(data.get("isContinuous")), **context)
                    except Exception as e:
                        self._log_handler_exc(socket_idx, e)
                    # 存 snapshot 給 API /api/tick/{symbol} 用
                    with self._snap_lock:
                        self._latest_books[symbol] = {
                            "bids": self._serialize_book_side(bids),
                            "asks": self._serialize_book_side(asks),
                            "ts": datetime.now().isoformat(timespec="microseconds"),
                        }
                elif channel == "trades":
                    # trader on_trade dispatch (08:59:58 起就掛上)
                    if self.on_trade:
                        try:
                            callback = self.on_trade
                            context = ({"received_at": now_wall, "received_mono": now_m,
                                        "feed_epoch": message_epoch}
                                       if callback is self._context_trade else {})
                            callback(symbol, data, **context)
                        except Exception as e:
                            self._log_handler_exc(socket_idx, e)
                    # 存 snapshot (is_trial: 盤前試撮 tick 標記 — 9:00 種子化時要跳過)
                    with self._snap_lock:
                        self._latest_trade[symbol] = {
                            "price": data.get("price"),
                            "size": data.get("size") or data.get("qty"),
                            "is_trial": bool(data.get("isTrial")),
                            "time": data.get("time"),
                            "ts": datetime.now().isoformat(timespec="microseconds"),
                        }

                # 落檔最後 (record 已是零 I/O 入佇列,順序仍讓 handler 絕對優先)
                if self.recorder and channel in ("books", "trades"):
                    self.recorder.record(channel, data)
            except Exception as e:
                # parse/snapshot/record 層例外 (handler 例外已在內層各自捕捉)
                self._log_handler_exc(socket_idx, e)
        return _handler

    def _log_handler_exc(self, socket_idx: int, e: Exception):
        """handler/分派例外絕不吞 — filter/trader 整套邏輯都跑在這裡,吞掉 = 盤中
        邏輯掛了完全無感。永遠記 ERROR+traceback (dedup: 前 3 次全印,之後每 500 次一次)。
        (需在 except 區塊內呼叫 — logger.exception 才抓得到 traceback。)"""
        key = f"handler:{type(e).__name__}:{str(e)[:80]}"
        cnt = self._error_count.get(key, 0) + 1
        self._error_count[key] = cnt
        if cnt <= 3 or cnt % 500 == 0:
            logger.exception(f"[socket#{socket_idx}] handle_msg 例外 #{cnt}: {e}")

    @staticmethod
    def _get(obj, key):
        if isinstance(obj, dict):
            return obj.get(key)
        return getattr(obj, key, None)

    @staticmethod
    def _to_dict(obj):
        """SDK object 轉 dict — 給 on_trade callback 統一格式。"""
        if isinstance(obj, dict):
            return obj
        try:
            return vars(obj)
        except TypeError:
            return {}

    def _serialize_book_side(self, side_list):
        """把 bids/asks list of dict/object 轉純 dict [{price, size}, ...]."""
        out = []
        for item in side_list[:5]:   # 最多五檔
            out.append({
                "price": self._get(item, "price"),
                "size": self._get(item, "size"),
            })
        return out

    def get_latest_snapshot(self, symbol: str) -> dict:
        """給 API /api/tick/{symbol} 用 — 回該 symbol 最新 books + last trade snapshot."""
        with self._snap_lock:
            return {
                "symbol": symbol,
                "books": self._latest_books.get(symbol),
                "last_trade": self._latest_trade.get(symbol),
            }

    def get_tick_stats(self) -> dict:
        return {
            "total_tick_count": self._tick_counter,
            "books_count": self._books_count,
            "trades_count": self._trades_count,
            "sockets": self.socket_health(),     # 幾條 / 幾條斷線中 / 各條狀態
        }

    @staticmethod
    def _make_batches(items: List[str], batch_size: int) -> List[List[str]]:
        return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]
