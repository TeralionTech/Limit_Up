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
import queue
import threading
from datetime import datetime
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)


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

    # ─── 對外 API ────────────────────────────────────────────

    def start(self):
        """優先 multi-SDK，失敗 fallback loop single-socket。"""
        if self._try_multi_sdk():
            logger.info(f"[subscriber] 使用 multi-SDK 模式 ({len(self._sockets)} sockets)")
            self._start_multi_socket_loop()
        else:
            logger.warning("[subscriber] multi-SDK 失敗，fallback loop single-socket")
            self._sockets = [self.primary_sdk.marketdata.websocket_client.stock]
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

    def set_handlers(self, on_book=None, on_trade=None):
        """9:00 handoff — trader 換掉 on_book/on_trade 邏輯，訂閱不動."""
        if on_book is not None:
            self.on_book = on_book
        if on_trade is not None:
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
            try:
                self._sockets[idx].subscribe({"channel": "trades", "symbol": sym})
                ok += 1
            except Exception as e:
                fail += 1
                if first_err is None:
                    first_err = (sym, str(e)[:150])
        logger.info(f"[subscriber] subscribe_trades_for: {ok} ok / {fail} fail "
                    f"(watchlist {len(symbols)} 檔) first_err={first_err}")

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
        ok = True
        for ch in ("books", "trades"):
            try:
                ws.subscribe({"channel": ch, "symbol": symbol})
            except Exception as e:
                ok = False
                logger.warning(f"[subscriber] add_symbol {symbol} {ch} 失敗: {str(e)[:150]}")
        logger.warning(f"[subscriber] 手動加訂 {symbol} (books+trades) ok={ok}")
        return ok

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
        summary = []
        for label, acct, pwd, pfx, pfxpwd, n in self._account_plan(self.login_cfg):
            opened = 0
            for i in range(n):
                if label == "primary" and i == 0:
                    sockets.append(self.primary_sdk.marketdata.websocket_client.stock)
                    opened += 1
                    continue
                ws = self._login_extra_sdk(FubonSDK, acct, pwd, pfx, pfxpwd,
                                           label=f"{label} #{i+1}/{n}")
                if ws is None:
                    logger.error(f"[subscriber] {label} 只開 {opened}/{n} 條就放棄")
                    break
                sockets.append(ws)
                opened += 1
            if opened == 0:
                logger.critical(f"[subscriber] ⚠⚠ {label} 帳號 ({str(acct)[:3]}***) 一條 socket "
                                f"都沒開成 — 監控容量少 {n * self.batch_size} 檔,"
                                f"盡快檢查該帳號憑證/密碼!")
            summary.append(f"{opened} {label}")
        self._sockets = sockets
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
                ws.connect()
                logger.info(f"[subscriber] socket #{i+1} connect() 已呼叫 (等 broker ready)")
            except Exception as e:
                logger.error(f"[subscriber] socket #{i+1} connect 失敗: {e}")

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
        self._stop.wait(2)

        round_num = 0
        while not self._stop.is_set():
            round_num += 1
            for i, batch in enumerate(batches, 1):
                if self._stop.is_set():
                    return
                # subscribe 這批 — 只 books，不訂 trades (省一半 sub 額度，每檔佔 1 sub)
                # 已淘汰 (removed) 的股票跳過 — 否則 pruner 退訂完又被這裡訂回
                sub_ok = 0
                sub_fail = 0
                first_err_sample = None
                with self._removed_lock:
                    removed_snapshot = set(self._removed)
                for sym in batch:
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
                    logger.info(f"[socket#{socket_idx}] round {round_num} 訂 {len(batch)} 檔 (常駐)")
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
        connected_event = threading.Event()
        try:
            def _on_conn(_e=connected_event):
                _e.set()
                logger.info("[subscriber] 保底 socket connected event fired ✓")
            ws.on("connect", _on_conn)
            ws.on("message", self._make_msg_handler(socket_idx=0))
            ws.connect()
            logger.info("[subscriber] 保底 socket connect() 已呼叫")
        except Exception as e:
            logger.error(f"[subscriber] 保底 socket connect 失敗: {e}")
            raise

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

    def _make_msg_handler(self, socket_idx: int):
        """Per-socket message handler.

        富邦 SDK 傳給 handler 的 message 是 JSON string (Alex fubon_adapter.py:665)。
        Schema: {"event": "data"|"pong"|"subscribed"|..., "data": {channel, symbol, bids, asks, price, size}}
        只有 event=="data" 才是 tick，其他是 subscribed ack / pong / error 之類。
        """
        def _handler(msg):
            self._tick_counter += 1
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
                    try:
                        self.on_book(symbol, bids, asks)
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
                            self.on_trade(symbol, data)
                        except Exception as e:
                            self._log_handler_exc(socket_idx, e)
                    # 存 snapshot (is_trial: 盤前試撮 tick 標記 — 9:00 種子化時要跳過)
                    with self._snap_lock:
                        self._latest_trade[symbol] = {
                            "price": data.get("price"),
                            "size": data.get("size") or data.get("qty"),
                            "is_trial": bool(data.get("isTrial")),
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
        }

    @staticmethod
    def _make_batches(items: List[str], batch_size: int) -> List[List[str]]:
        return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]
