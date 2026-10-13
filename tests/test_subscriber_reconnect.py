"""行情連線斷線 → CRITICAL、自動重連、重新訂閱;斷線 / 重連後尚未收到新五檔的檔 = 過期 (2026-10-12)。

事故: hub 8 條行情 socket 08:48 全部停擺,其中 5 條之後斷線且從未重連 (subscriber 沒有重連邏輯)。hub 拿著
08:47 的「鎖漲停」畫面在 08:59:50 凍結名單 → 五台 node 對 08:57:48 已打開的 6872 預掛。
盤前試撮五檔只在變動時推送、訂閱後沒有初始畫面 → 「重連成功」不等於那條連線上的檔可以再相信。

做法比照 day-trade 系統的生產分支 (teralion day-trade-system fubon_adapter / fugle_connect_patch):
disconnect 事件 → single-flight 重連 thread → 先 disconnect() 舊連線 → 開連線 → 重訂;退避 2,4,8,…,60 s、不設次數
上限;開連線有期限、不忙等;舊收訊迴圈沒結束不在同一個物件上開第二條;認證類失敗 / 連續失敗 → 換全新 session;
連線數達上限 → 等券商回收名額。另有 ping 看門狗抓「沒有斷線事件的停擺」。"""
import json
import logging
import sys
import threading
import time
from types import ModuleType, SimpleNamespace

import pytest

import subscriber as sub_mod
from subscriber import Subscriber
from fakes_cancel import wait_until


# ─────────────────────────── 假行情連線 (只有公開介面) ───────────────────────────
class FakeWS:
    """假行情連線 (公開介面): on / connect / disconnect / subscribe / unsubscribe / ping。
    沒有函式庫的內部欄位 → subscriber 走「公開 connect() + helper thread + 逾時踢出」那條路。
    connect_script: 每次 connect() 消耗一個 —
        None       → 連上 + 認證成功
        Exception  → 連線關閉 (disconnect 事件,同函式庫失敗時會關 socket) 後 raise
        "hang"     → 仿函式庫的忙等: 一直等到有人把 auth_status 改掉 (TCP 連不上、沒有任何事件)
        "refused"  → 連線馬上被拒: 收訊迴圈結束 (disconnect 事件) 但 connect() 仍然在等
    """

    def __init__(self, connected=True):
        self.handlers = {}
        self.subscribed = []
        self.unsubscribed = []
        self.pings = []
        self.connect_calls = 0
        self.disconnect_calls = 0
        self.connected = connected
        self.connect_script = []
        self.auth_status = 2 if connected else 0
        self.error = None
        self.pong = True                     # ping → 立刻回 pong 訊息
        self.on_subscribe = None             # 測試掛的 hook
        self.lk = threading.Lock()

    def on(self, ev, fn):
        self.handlers.setdefault(ev, []).append(fn)

    def emit(self, ev, *a):
        for fn in list(self.handlers.get(ev, [])):
            fn(*a)

    def _wait_auth(self):
        self.auth_status = 0
        while self.auth_status not in (2, 3):
            time.sleep(0.003)
        if self.error is not None:
            raise self.error

    def connect(self):
        with self.lk:
            self.connect_calls += 1
            step = self.connect_script.pop(0) if self.connect_script else None
        if isinstance(step, BaseException):
            self.emit("disconnect", None, None)
            raise step
        if step == "hang":
            self._wait_auth()
        if step == "refused":
            self.auth_status = 0
            self.emit("disconnect", None, None)
            self._wait_auth()
        self.connected = True
        self.auth_status = 2
        self.emit("connect")

    def disconnect(self):
        self.disconnect_calls += 1
        was = self.connected
        self.connected = False
        self.auth_status = 0
        self.error = None
        if was:
            self.emit("disconnect", None, None)

    def subscribe(self, p):
        if not self.connected:
            raise RuntimeError("socket is already closed.")
        hook = self.on_subscribe
        if hook is not None:
            hook(p)
        self.subscribed.append((p["channel"], p["symbol"]))

    def unsubscribe(self, p):
        self.unsubscribed.append(p)

    def ping(self, state):
        if not self.connected:
            raise RuntimeError("socket is already closed.")
        self.pings.append(state)
        if self.pong:
            self.emit("message", json.dumps({"event": "pong", "data": {"state": state}}))

    # 測試小工具
    def drop(self, code=None, msg=None):
        """伺服器 / 網路把連線斷掉 (10-12: 收訊拿到 timeout → error 事件 + disconnect 事件)。"""
        self.connected = False
        self.emit("error", OSError("Connection timed out"))
        self.emit("disconnect", code, msg)

    def book(self, sym, bids=(), asks=()):
        self.emit("message", json.dumps({"event": "data", "channel": "books",
                                         "data": {"symbol": sym, "bids": list(bids), "asks": list(asks)}}))

    def ack(self, ch, sym, sid):
        self.emit("message", json.dumps({"event": "subscribed",
                                         "data": {"channel": ch, "symbol": sym, "id": sid}}))


UNIVERSE = ("1101", "1102", "1103", "1104")          # 2 條 socket: #0 = 1101 1103;#1 = 1102 1104


def _fast(sub):
    """測試用的短時間 (正式值見 subscriber.MD_*)。"""
    sub._reconnect_step_sec = 0.01
    sub._reconnect_max_sec = 0.04
    sub._reconnect_settle_sec = 0.0
    sub._close_wait_sec = 0.5
    sub._connect_timeout_sec = 0.4
    sub._rebuild_cooldown_sec = 0.0
    sub._conn_cap_wait_sec = 0.15


def _sub(n_sockets=2, universe=UNIVERSE, books=None):
    sub = Subscriber(sdk=None, universe=list(universe), login_cfg=None,
                     on_book=(lambda s, b, a, c=False: books.append(s)) if books is not None
                     else (lambda *a: None))
    socks = [FakeWS() for _ in range(n_sockets)]
    sub._sockets = socks
    sub._rebuild_symbol_index()
    sub._init_socket_health()
    for i, ws in enumerate(socks):
        ws.on("message", sub._make_msg_handler(socket_idx=i))
        sub._cur_batch[i] = [s for j, s in enumerate(universe) if j % n_sockets == i]
    _fast(sub)
    return sub, socks


@pytest.fixture
def subs():
    made = []

    def _make(**kw):
        sub, socks = _sub(**kw)
        made.append(sub)
        return sub, socks
    yield _make
    for sub in made:
        sub._stop.set()


def _up(sub, idx):
    return sub._sock_state[idx]["up"] and not sub._sock_state[idx]["reconnecting"]


def _crit(caplog, needle):
    return [r for r in caplog.records if r.levelno == logging.CRITICAL and needle in r.getMessage()]


# ═══ 斷線 → CRITICAL → 重連 → 重訂 ═══════════════════════════════════

class TestDisconnectReconnect:
    def test_drop_is_critical_and_socket_reconnects_and_resubscribes(self, subs, caplog):
        sub, (a, b) = subs()
        a.drop()
        assert len(_crit(caplog, "行情 socket #1 斷線")) == 1
        assert "2 檔收不到資料" in _crit(caplog, "行情 socket #1 斷線")[0].getMessage()
        assert wait_until(lambda: _up(sub, 0))
        assert a.connect_calls == 1 and a.disconnect_calls >= 1, "比照 day-trade: 先 disconnect() 再連"
        assert a.subscribed == [("books", "1101"), ("books", "1103")]
        assert len(_crit(caplog, "行情 socket #1 重連成功")) == 1
        assert b.connect_calls == 0 and b.subscribed == [], "別條連線不受影響"
        assert sub._sock_state[0]["reconnects"] == 1

    def test_connection_error_reason_is_logged(self, subs, caplog):
        caplog.set_level(logging.WARNING, logger="subscriber")
        sub, (a, _b) = subs()
        for _ in range(6):
            a.emit("error", ValueError("something odd"))     # 不是連線中斷類 → 只記錄
        hits = [r for r in caplog.records if "連線層錯誤" in r.getMessage()]
        assert len(hits) == 3 and "something odd" in hits[0].getMessage(), "去重: 同一種只記前 3 次"
        assert _up(sub, 0) and a.connect_calls == 0

    def test_fatal_connection_error_marks_the_socket_down_before_the_close_event(self, subs, caplog):
        """10-12: 「Connection timed out」的 error 事件先到,關閉事件要等函式庫跟對方握手 (約 3 s 後)。
        這 3 s 訂閱已經送不出去 → error 一到就標斷線、開始重連。"""
        sub, (a, _b) = subs()
        a.connect_script = [RuntimeError("boom")] * 1000
        a.emit("error", OSError("Connection timed out"))     # 只有 error,關閉事件還沒來
        assert not sub._sock_state[0]["up"] and sub.is_stale("1101")
        assert len(_crit(caplog, "行情 socket #1 斷線")) == 1
        assert "Connection timed out" in _crit(caplog, "行情 socket #1 斷線")[0].getMessage()

    def test_fatal_error_classification(self):
        class WebSocketTimeoutException(Exception):
            pass

        class WebSocketConnectionClosedException(Exception):
            pass
        fatal = Subscriber._is_fatal_ws_error
        assert fatal(OSError("x")) and fatal(TimeoutError()) and fatal(ConnectionResetError())
        assert fatal(WebSocketTimeoutException("Connection timed out"))
        assert fatal(WebSocketConnectionClosedException("Connection to remote host was lost."))
        assert not fatal(ValueError("x")) and not fatal(KeyError("x")) and not fatal("a close frame object")

    def test_trades_and_added_symbols_come_back_and_removed_ones_do_not(self, subs):
        sub, (a, _b) = subs()
        sub.subscribe_trades_for(["1101", "1102"])           # 1101 在 #0、1102 在 #1
        sub.add_symbol("9998")                               # 盤中補訂 (落在哪一條看 index)
        idx_new = sub._socket_index("9998")
        sub.request_unsubscribe("1103")                      # 已淘汰
        a.subscribed.clear()
        a.drop()
        assert wait_until(lambda: _up(sub, 0))
        assert ("books", "1101") in a.subscribed and ("trades", "1101") in a.subscribed
        assert ("books", "1103") not in a.subscribed, "淘汰的檔重連後不可訂回"
        assert ("trades", "1102") not in a.subscribed, "別條 socket 的檔不會訂到這條"
        if idx_new == 0:
            assert ("books", "9998") in a.subscribed and ("trades", "9998") in a.subscribed

    def test_trade_subscription_that_failed_while_down_is_sent_after_reconnect(self, subs, caplog):
        """10-12 09:00: hub 對 14 檔訂 trades 全部失敗 (連線已關),只記了一行 INFO、之後也沒補。"""
        sub, (a, _b) = subs()
        a.connect_script = [RuntimeError("boom")] * 3
        a.drop()
        sub.subscribe_trades_for(["1101", "1103"])           # 兩檔都在斷線中的 #0
        assert len(_crit(caplog, "subscribe_trades_for: 0 ok / 2 fail")) == 1
        assert wait_until(lambda: _up(sub, 0), timeout=5)
        assert ("trades", "1101") in a.subscribed and ("trades", "1103") in a.subscribed

    def test_partial_trade_subscription_failure_is_a_warning(self, subs, caplog):
        caplog.set_level(logging.INFO, logger="subscriber")
        sub, (a, _b) = subs()
        a.connected = False
        sub.subscribe_trades_for(["1101", "1102"])           # #0 失敗、#1 成功
        rec = [r for r in caplog.records if "subscribe_trades_for" in r.getMessage()]
        assert len(rec) == 1 and rec[0].levelno == logging.WARNING
        caplog.clear()
        sub.subscribe_trades_for(["1102"])
        rec = [r for r in caplog.records if "subscribe_trades_for" in r.getMessage()]
        assert rec[0].levelno == logging.INFO

    def test_old_subscription_ids_are_dropped_for_that_socket_only(self, subs):
        sub, (a, b) = subs()
        a.ack("books", "1101", "old-a")
        b.ack("books", "1102", "old-b")
        assert sub.has_ack("books", "1101") and sub.has_ack("books", "1102")
        a.drop()
        assert wait_until(lambda: _up(sub, 0))
        assert not sub.has_ack("books", "1101"), "舊連線的 id 作廢,要等新的 ack"
        assert sub.has_ack("books", "1102")

    def test_flapping_disconnect_events_start_one_reconnect_only(self, subs):
        sub, (a, _b) = subs()
        sub._reconnect_step_sec = 0.15                       # 重連 thread 還在等退避時連續來 5 個事件
        sub._reconnect_max_sec = 0.15
        started = []
        real = sub._reconnect_loop
        sub._reconnect_loop = lambda idx: (started.append(idx), real(idx))
        for _ in range(5):
            a.drop()
        assert wait_until(lambda: _up(sub, 0))
        assert started == [0] and a.connect_calls == 1

    def test_keeps_retrying_past_the_old_limit_of_five(self, subs):
        """day-trade 舊版寫死 5 次,30 秒內撞上券商維護就永久放棄 — hub 盤前不能這樣。"""
        sub, (a, _b) = subs()
        a.connect_script = [RuntimeError("boom")] * 7
        a.drop()
        assert wait_until(lambda: _up(sub, 0), timeout=5)
        assert a.connect_calls == 8
        assert sub.is_stale("1101")

    def test_backoff_doubles_up_to_a_minute(self):
        sub = Subscriber(sdk=None, universe=[], on_book=lambda *a: None, login_cfg=None)
        assert [sub._reconnect_delay(n) for n in range(1, 9)] == [2, 4, 8, 16, 32, 60, 60, 60]

    def test_every_tenth_failure_is_critical(self, subs, caplog):
        sub, (a, _b) = subs()
        a.connect_script = [RuntimeError("boom")] * 11
        a.drop()
        assert wait_until(lambda: _up(sub, 0), timeout=6)
        assert len(_crit(caplog, "重連失敗 (第 10 次")) == 1

    def test_stop_ends_the_reconnect_loop_and_ignores_later_events(self, subs):
        sub, (a, _b) = subs()
        a.connect_script = [RuntimeError("boom")] * 1000
        a.drop()
        assert wait_until(lambda: a.connect_calls >= 2)
        sub._stop.set()
        assert wait_until(lambda: not sub._sock_state[0]["reconnecting"])
        n = a.connect_calls
        time.sleep(0.15)
        assert a.connect_calls == n
        sub2, (c, _d) = subs()
        sub2._stop.set()
        c.drop()                                             # stop() 之後自己關連線產生的事件
        assert sub2._sock_state[0]["up"] and not sub2._sock_state[0]["reconnecting"]

    def test_new_connection_that_drops_during_resubscribe_is_not_marked_up(self, subs):
        sub, (a, _b) = subs()
        seen = []

        def _drop_on_second(p):
            seen.append(p["symbol"])
            if len(seen) == 2:                               # 第一筆重訂送成功,第二筆時連線又斷
                a.connected = False
                a.emit("disconnect", 1006, "abnormal")
                raise RuntimeError("socket is already closed.")
        a.on_subscribe = _drop_on_second
        a.drop()
        assert wait_until(lambda: _up(sub, 0))
        assert a.connect_calls == 2, "剛連上就又斷 → 不可以標成正常 (有一筆訂閱送成功也一樣),要再連一次"
        assert a.subscribed.count(("books", "1101")) == 2 and a.subscribed.count(("books", "1103")) == 1


# ═══ 開連線一定會返回 (公開 connect() 那條路) ═══════════════════════════

class TestOpenSocketIsBounded:
    def test_connect_that_never_returns_is_timed_out(self, subs):
        """TCP 連不上、沒有任何事件: 函式庫的 connect() 永遠不返回 → 逾時後把它踢出來,下一次再試。"""
        sub, (a, _b) = subs()
        sub._close_wait_sec = 0.1
        a.connect_script = ["hang"]
        t0 = time.monotonic()
        a.drop()
        assert wait_until(lambda: _up(sub, 0), timeout=5)
        assert a.connect_calls == 2 and time.monotonic() - t0 < 3
        assert not [t for t in threading.enumerate() if t.name.startswith("md-connect-") and t.is_alive()]

    def test_connection_refused_is_abandoned_without_waiting_for_the_timeout(self, subs):
        sub, (a, _b) = subs()
        sub._connect_timeout_sec = 30.0                      # 不靠逾時: 收到關閉事件就放棄這一次
        a.connect_script = ["refused"]
        t0 = time.monotonic()
        a.drop()
        assert wait_until(lambda: _up(sub, 0), timeout=5)
        assert a.connect_calls == 2 and time.monotonic() - t0 < 3


# ─────────────────────────── 有函式庫內部欄位的假連線 ───────────────────────────
class _App:
    """仿 websocket.WebSocketApp: run_forever() 跑到連線結束為止。"""

    def __init__(self, inner):
        self.inner = inner

    def run_forever(self):
        self.inner._run()


class FakeInner(FakeWS):
    """仿 fugle_marketdata 2.4.1 的 WebSocketClient: 內部有 WebSocketApp (__ws) 與 auth_status。
    subscriber 會自己起收訊 thread、用 sleep 輪詢認證狀態,不呼叫會忙等的公開 connect()。
    收訊迴圈結束時才發 disconnect 事件 (同 websocket-client 的 teardown);disconnect() 只是關 socket。
    mode (字串或逐次消耗的 list): ok / refused (連不上) / silent (連上但永遠不認證) / authfail。
    linger: 關 socket 之後收訊迴圈多久才結束 (Linux 上叫不醒 select,要 ~10 s)。"""

    def __init__(self, mode="ok"):
        super().__init__(connected=False)
        self._WebSocketClient__ws = _App(self)
        self.run_calls = 0
        self.closed = threading.Event()
        self.mode = mode
        self.linger = 0.0
        self.running = 0
        self.max_running = 0
        self.disconnect_events = None        # None = 每次都發;N = 只有前 N 次連線結束時發 (舊版只發第一次)
        self.ended = 0

    def connect(self):                       # 不該被呼叫
        raise AssertionError("有內部 WebSocketApp 時不可以走會忙等的公開 connect()")

    def _run(self):
        with self.lk:
            self.run_calls += 1
            self.running += 1
            self.max_running = max(self.max_running, self.running)
            mode = self.mode if isinstance(self.mode, str) else (self.mode.pop(0) if self.mode else "ok")
        self.closed.clear()
        try:
            if mode == "refused":
                return
            if mode == "ok":
                self.connected = True
                self.auth_status = 2
                self.emit("connect")
            elif mode == "authfail":
                self.error = Exception("Invalid authentication credentials")
                self.auth_status = 3
            self.closed.wait(10)
            if self.linger:
                time.sleep(self.linger)
        finally:
            self.connected = False
            with self.lk:
                self.running -= 1
                self.ended += 1
                fire = self.disconnect_events is None or self.ended <= self.disconnect_events
            if fire:
                self.emit("disconnect", None, None)

    def disconnect(self):
        self.disconnect_calls += 1
        self.auth_status = 0
        self.error = None
        self.connected = False
        self.closed.set()

    def drop(self, code=None, msg=None):
        self.emit("error", OSError("Connection timed out"))
        self.closed.set()


class Wrapper:
    """仿 fubon_neo 的 WebSocketStockClientWrapper: 真的 client 在 _client,其餘 __getattr__ 轉手。"""

    def __init__(self, inner):
        self._client = inner

    def __getattr__(self, name):
        return getattr(self._client, name)


def _sub_inner(mode="ok"):
    sub = Subscriber(sdk=None, universe=["1101", "1103"], on_book=lambda *a: None, login_cfg=None)
    inner = FakeInner(mode)
    sub._sockets = [Wrapper(inner)]
    sub._rebuild_symbol_index()
    sub._init_socket_health()
    inner.on("message", sub._make_msg_handler(socket_idx=0))
    sub._cur_batch[0] = ["1101", "1103"]
    _fast(sub)
    return sub, inner


@pytest.fixture
def inner_sub():
    made = []

    def _make(mode="ok", start=True):
        sub, inner = _sub_inner(mode)
        made.append((sub, inner))
        if start:
            sub._open_socket(0)              # 開機第一次連線 (與重連同一條路)
            assert inner.connected and inner.run_calls == 1
        return sub, inner
    yield _make
    for sub, inner in made:
        sub._stop.set()
        inner.linger = 0.0
        inner.closed.set()


class TestOpenSocketWithoutBusyWait:
    def test_reconnects_through_the_wrapper_without_calling_connect(self, inner_sub):
        sub, inner = inner_sub()
        inner.drop()
        assert wait_until(lambda: _up(sub, 0))
        assert inner.run_calls == 2 and inner.subscribed == [("books", "1101"), ("books", "1103")]
        assert inner.max_running == 1

    def test_never_authenticating_connection_times_out_without_burning_cpu(self, inner_sub):
        sub, inner = inner_sub()
        inner.mode = ["silent", "ok"]
        cpu0 = time.process_time()
        inner.drop()
        assert wait_until(lambda: _up(sub, 0), timeout=5)
        assert inner.run_calls == 3 and inner.max_running == 1
        assert time.process_time() - cpu0 < 0.25, "等認證要用 sleep 輪詢,不可忙等"

    def test_refused_connection_is_abandoned_at_once(self, inner_sub):
        sub, inner = inner_sub()
        sub._connect_timeout_sec = 30.0
        inner.mode = ["refused", "ok"]
        t0 = time.monotonic()
        inner.drop()
        assert wait_until(lambda: _up(sub, 0), timeout=5)
        assert inner.run_calls == 3 and time.monotonic() - t0 < 3

    def test_authentication_failure_is_reported_with_the_library_error(self, inner_sub, caplog):
        caplog.set_level(logging.WARNING, logger="subscriber")
        sub, inner = inner_sub()
        inner.mode = ["authfail", "ok"]
        inner.drop()
        assert wait_until(lambda: _up(sub, 0), timeout=5)
        assert "Invalid authentication credentials" in caplog.text and "_MdAuthError" in caplog.text

    def test_second_death_is_caught_without_a_disconnect_event(self, inner_sub, caplog):
        """舊版 websocket-client (實測 1.6.4) 在同一個物件第二次以後的連線結束時**不再發 disconnect 事件**。
        只靠事件的話: 重連過一次之後再斷就沒人知道,而且每次重連都卡在「等舊迴圈的關閉事件」。
        收訊 thread 是我們自己起的 → thread 結束就是連線結束。"""
        sub, inner = inner_sub()
        inner.disconnect_events = 1          # 只有第一次連線結束會發事件
        inner.drop()
        assert wait_until(lambda: _up(sub, 0))
        assert inner.run_calls == 2
        caplog.clear()
        inner.drop()                         # 第二次: 沒有 disconnect 事件
        assert wait_until(lambda: len(_crit(caplog, "行情 socket #1 斷線")) == 1, timeout=3)
        assert wait_until(lambda: _up(sub, 0), timeout=5)
        assert inner.run_calls == 3 and inner.max_running == 1
        assert "_MdOldLoopAlive" not in caplog.text
        caplog.clear()
        inner.closed.set()                   # 第三次: 連 error 事件都沒有,收訊迴圈就這樣結束了
        assert wait_until(lambda: len(_crit(caplog, "收訊迴圈結束")) == 1, timeout=3)
        assert wait_until(lambda: _up(sub, 0) and inner.run_calls == 4, timeout=5)

    def test_watchdog_notices_a_dead_receive_thread(self, inner_sub, caplog):
        sub, inner = inner_sub()
        st = sub._sock_state[0]
        st["rx_thread"] = threading.Thread(target=lambda: None)      # 已結束、但沒人標斷線的收訊 thread
        st["rx_thread"].start()
        st["rx_thread"].join()
        inner.closed.set()                                           # 真的那條也收掉 (不發事件)
        inner.disconnect_events = 0
        sub._watchdog_once()
        assert not st["up"] or wait_until(lambda: _up(sub, 0))
        assert len(_crit(caplog, "收訊 thread 已結束")) + len(_crit(caplog, "收訊迴圈結束")) >= 1

    def test_stalled_connection_is_closed_and_its_loop_awaited_before_reopening(self, inner_sub):
        """看門狗抓到的停擺: 舊連線還開著。關掉之後要等它的收訊迴圈真的結束才開新的 (Linux 上要 ~10 s),
        絕不可以兩條收訊迴圈同時跑在同一個物件上。"""
        sub, inner = inner_sub()
        sub._close_wait_sec = 2.0
        inner.linger = 0.3
        t0 = time.monotonic()
        sub._socket_down(0, "20 s 沒有任何訊息 (ping 也沒有回應)")
        assert wait_until(lambda: _up(sub, 0), timeout=5)
        assert time.monotonic() - t0 >= 0.3, "要等舊迴圈結束"
        assert inner.max_running == 1 and inner.run_calls == 2

    def test_old_loop_that_will_not_end_blocks_reopening_on_the_same_object(self, inner_sub, caplog):
        caplog.set_level(logging.WARNING, logger="subscriber")
        sub, inner = inner_sub()
        sub._close_wait_sec = 0.1
        inner.linger = 0.6                                   # 等待上限內不會結束
        sub._socket_down(0, "停擺")
        assert wait_until(lambda: "_MdOldLoopAlive" in caplog.text, timeout=3)
        assert inner.run_calls == 1, "舊迴圈還在 → 這一次不開新連線"
        assert wait_until(lambda: _up(sub, 0), timeout=5), "舊迴圈結束後照常連回來"
        assert inner.max_running == 1


# ═══ 換全新 session (新 SDK 登入) ═══════════════════════════════════════

class FakeSDK:
    """假 FubonSDK: login / init_realtime / marketdata.websocket_client.stock / logout。"""
    made = []
    login_script = []                        # 每次 login 消耗一個: None=成功 / Exception=raise / "fail"
    realtime_script = []                     # 每次 init_realtime 消耗一個: None=成功 / BaseException=raise
    events = []                              # 全域事件序 (login / logout) — 驗先後

    def __init__(self):
        self.inner = FakeInner("ok")
        self.marketdata = SimpleNamespace(websocket_client=SimpleNamespace(stock=Wrapper(self.inner)))
        self.logged_out = False
        self.login_args = None
        self.realtime = False
        FakeSDK.made.append(self)

    def login(self, *a):
        self.login_args = a
        FakeSDK.events.append("login")
        step = FakeSDK.login_script.pop(0) if FakeSDK.login_script else None
        if isinstance(step, BaseException):
            raise step
        if step == "fail":
            return SimpleNamespace(is_success=False, message="帳號密碼錯誤")
        return SimpleNamespace(is_success=True, data=[object()])

    def init_realtime(self):
        step = FakeSDK.realtime_script.pop(0) if FakeSDK.realtime_script else None
        if isinstance(step, BaseException):
            raise step
        self.realtime = True

    def logout(self):
        self.logged_out = True
        FakeSDK.events.append("logout")


CREDS = ("secondary", "A123456789", "s3cret-pw", "/certs/a.pfx", "")


@pytest.fixture
def fake_sdk(monkeypatch):
    pkg, mod = ModuleType("fubon_neo"), ModuleType("fubon_neo.sdk")
    mod.FubonSDK = FakeSDK
    monkeypatch.setitem(sys.modules, "fubon_neo", pkg)
    monkeypatch.setitem(sys.modules, "fubon_neo.sdk", mod)
    FakeSDK.made, FakeSDK.login_script, FakeSDK.realtime_script, FakeSDK.events = [], [], [], []
    yield FakeSDK
    for sdk in FakeSDK.made:
        sdk.inner.closed.set()


def _with_creds(sub, old_sdk=None, creds=CREDS):
    sub._sock_creds = [creds]
    sub._sock_sdks = [old_sdk]


class TestRebuildSession:
    def test_auth_failure_on_the_old_token_switches_to_a_fresh_login(self, inner_sub, fake_sdk, caplog):
        """day-trade 的教訓: 光重連舊 ws,realtime session 失效時會一直 authentication timeout;
        官方的斷線重連作法是重新登入。"""
        caplog.set_level(logging.INFO, logger="subscriber")
        sub, old = inner_sub()
        old_sdk = SimpleNamespace(logout=lambda: fake_sdk.events.append("logout-old"))
        _with_creds(sub, old_sdk)
        sub._extra_sdks.append(old_sdk)
        old.mode = ["authfail"] * 50                         # 舊 token 怎麼連都認證失敗
        old.drop()
        assert wait_until(lambda: _up(sub, 0), timeout=5)
        new = fake_sdk.made[0]
        assert sub._sockets[0] is new.marketdata.websocket_client.stock and new.realtime
        assert new.login_args == ("A123456789", "s3cret-pw", "/certs/a.pfx"), "pfx 密碼空字串 → 只傳 3 個參數"
        assert new.inner.subscribed == [("books", "1101"), ("books", "1103")], "訂閱送到新連線"
        st = sub._sock_state[0]
        assert st["rebuilds"] == 1 and st["gen"] == 1 and old.run_calls == 2, "舊 token 只白試一次就換"
        assert wait_until(lambda: "logout-old" in fake_sdk.events), "換下來的 SDK 要 logout (忙等 thread + 名額)"
        assert old_sdk not in sub._extra_sdks and new in sub._extra_sdks
        assert len(_crit(caplog, "已換全新 session")) == 1
        for secret in ("s3cret-pw", "/certs/a.pfx", "A123456789"):
            assert secret not in caplog.text, "登入資訊不可以進 log"
        assert sub.is_stale("1101")
        new.inner.book("1101")
        assert not sub.is_stale("1101"), "新物件的訊息照常處理"

    def test_events_from_the_replaced_object_are_ignored(self, inner_sub, fake_sdk):
        books = []
        sub, old = inner_sub()
        sub.on_book = lambda s, b, a, c=False: books.append(s)
        _with_creds(sub)
        old.mode = ["authfail"] * 50
        old.drop()
        assert wait_until(lambda: _up(sub, 0), timeout=5)
        n = sub._tick_counter
        old.book("1103")                                     # 舊物件晚到的訊息
        old.emit("disconnect", None, None)                   # 舊物件晚到的關閉事件
        old.emit("error", OSError("late"))
        assert books == [] and _up(sub, 0) and sub.is_stale("1103")
        assert sub._tick_counter == n + 1

    def test_primary_sdk_is_never_logged_out(self, inner_sub, fake_sdk):
        """第 1 條 socket 用的是主流程的 SDK (還要用來打 REST) — 換 session 只換行情連線,不登出它。"""
        sub, old = inner_sub()
        primary = SimpleNamespace(logout=lambda: fake_sdk.events.append("logout-primary"))
        sub.primary_sdk = primary
        _with_creds(sub, primary)
        old.mode = ["authfail"] * 50
        old.drop()
        assert wait_until(lambda: _up(sub, 0), timeout=5)
        time.sleep(0.1)
        assert "logout-primary" not in fake_sdk.events and sub._sock_sdks[0] is fake_sdk.made[0]

    def test_three_failures_in_a_row_also_switch(self, inner_sub, fake_sdk):
        sub, old = inner_sub()
        _with_creds(sub)
        old.mode = ["refused"] * 50                          # 不是認證問題,但同一個物件怎麼連都不行
        old.drop()
        assert wait_until(lambda: _up(sub, 0), timeout=5)
        assert old.run_calls == 1 + 3 and sub._sock_state[0]["rebuilds"] == 1

    def test_without_credentials_it_keeps_retrying_the_same_object(self, inner_sub, fake_sdk):
        sub, old = inner_sub()
        old.mode = ["authfail"] * 4 + ["ok"]
        old.drop()
        assert wait_until(lambda: _up(sub, 0), timeout=5)
        assert fake_sdk.made == [] and sub._sock_state[0]["rebuilds"] == 0 and old.run_calls == 6

    def test_cooldown_limits_fresh_logins(self, inner_sub, fake_sdk):
        sub, old = inner_sub()
        _with_creds(sub)
        sub._rebuild_cooldown_sec = 60.0
        fake_sdk.login_script = [RuntimeError("login server down")]
        old.mode = ["authfail"] * 500
        old.drop()
        assert wait_until(lambda: old.run_calls >= 6, timeout=5)
        assert fake_sdk.events.count("login") == 1, "登入失敗後冷卻內不再重登 (防洗登入)"
        assert not sub._sock_state[0]["up"]

    def test_failed_login_is_just_another_failed_attempt(self, inner_sub, fake_sdk, caplog):
        caplog.set_level(logging.WARNING, logger="subscriber")
        sub, old = inner_sub()
        _with_creds(sub)
        fake_sdk.login_script = ["fail", RuntimeError("boom"), None]
        old.mode = ["authfail"] * 500
        old.drop()
        assert wait_until(lambda: _up(sub, 0), timeout=5)
        assert fake_sdk.events.count("login") == 3 and "is_success=False" in caplog.text
        assert sub._sock_state[0]["rebuilds"] == 1

    def test_old_loop_that_never_ends_is_abandoned_for_a_fresh_object(self, inner_sub, fake_sdk):
        sub, old = inner_sub()
        _with_creds(sub)
        sub._close_wait_sec = 0.1
        old.linger = 30.0                                    # 舊收訊迴圈就是不結束
        sub._socket_down(0, "停擺")
        assert wait_until(lambda: _up(sub, 0), timeout=5)
        assert old.run_calls == 1 and sub._sockets[0] is fake_sdk.made[0].marketdata.websocket_client.stock

    def test_socket_loop_uses_the_new_object_after_a_switch(self, inner_sub, fake_sdk):
        sub, old = inner_sub()
        _with_creds(sub)
        sub._connect_grace_sec = 0.01
        sub.batch_rotate_sec = 0.05
        old.mode = ["authfail"] * 50
        t = threading.Thread(target=sub._socket_loop_thread,
                             args=(sub._sockets[0], [["1101", "1103"]], 0, None), daemon=True)
        t.start()
        try:
            assert wait_until(lambda: old.subscribed.count(("books", "1101")) >= 1)
            old.drop()
            assert wait_until(lambda: _up(sub, 0), timeout=5)
            new = fake_sdk.made[0].inner
            n_old = len(old.subscribed)
            assert wait_until(lambda: new.subscribed.count(("books", "1101")) >= 3, timeout=3)
            assert len(old.subscribed) == n_old, "換掉的舊物件不可以再被拿來送訂閱"
        finally:
            sub._stop.set()
            t.join(3)


class FakePanic(BaseException):
    """仿 pyo3_runtime.PanicException (Rust panic): 繼承 BaseException,不是 Exception。"""


class TestRebuildFailureCleanup:
    """換 session 失敗時新建的 SDK 一定要登出: 登入過、沒登出的實例會留一條原生忙等 thread 並佔一個連線名額,
    而換 session 每 60 s 冷卻就會再試一次。"""

    def test_token_exchange_failure_logs_the_new_sdk_out(self, inner_sub, fake_sdk):
        sub, old = inner_sub()
        _with_creds(sub)
        fake_sdk.realtime_script = [RuntimeError("exchange_realtime_token failed")]
        old.mode = ["authfail"] * 500
        old.drop()
        assert wait_until(lambda: _up(sub, 0), timeout=5)
        assert len(fake_sdk.made) == 2
        abandoned, used = fake_sdk.made
        assert wait_until(lambda: abandoned.logged_out), "登入成功、換 token 失敗的那個新 SDK 要登出"
        assert not used.logged_out and sub._sock_sdks[0] is used and abandoned not in sub._extra_sdks

    def test_failed_login_also_logs_the_new_sdk_out(self, inner_sub, fake_sdk):
        sub, old = inner_sub()
        _with_creds(sub)
        fake_sdk.login_script = ["fail", RuntimeError("login server down"), None]
        old.mode = ["authfail"] * 500
        old.drop()
        assert wait_until(lambda: _up(sub, 0), timeout=5)
        assert wait_until(lambda: [x.logged_out for x in fake_sdk.made] == [True, True, False])

    def test_sdk_panic_during_a_switch_does_not_kill_the_reconnect_loop(self, inner_sub, fake_sdk, caplog):
        """Rust panic 是 BaseException。漏接的話重連 thread 無聲死掉、旗標卡住 → 這條 socket 整天沒人重連
        (day-trade 生產環境發生過「重連鎖永遠不放」)。"""
        caplog.set_level(logging.WARNING, logger="subscriber")
        sub, old = inner_sub()
        _with_creds(sub)
        fake_sdk.login_script = [FakePanic("called `Result::unwrap()` on an `Err` value")]
        old.mode = ["authfail"] * 500
        old.drop()
        assert wait_until(lambda: _up(sub, 0), timeout=5), "panic 只算一次失敗的嘗試,之後照樣連回來"
        assert "FakePanic" in caplog.text and fake_sdk.events.count("login") == 2


class TestReconnectLoopSurvives:
    def test_reconnect_thread_that_cannot_start_is_retried_by_the_watchdog(self, subs, caplog, monkeypatch):
        sub, (a, _b) = subs()
        real = threading.Thread
        fails = [1]

        class _T(real):
            def start(self):
                if self.name.startswith("subscriber-reconnect") and fails:
                    fails.pop()
                    raise RuntimeError("can't start new thread")
                return super().start()
        monkeypatch.setattr(sub_mod.threading, "Thread", _T)
        a.connected = False
        a.emit("disconnect", None, None)                     # 不可以 raise 到行情 thread 上
        st = sub._sock_state[0]
        assert not st["up"] and not st["reconnecting"], "thread 沒起來 → 旗標要放掉"
        assert len(_crit(caplog, "重連 thread 起不來")) == 1
        sub._watchdog_once()                                 # 斷線中又沒人在重連 → 重新啟動
        assert wait_until(lambda: _up(sub, 0)) and a.connect_calls == 1

    @pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
    def test_loop_exiting_abnormally_releases_the_flag(self, subs, monkeypatch):
        sub, (a, _b) = subs()

        def _boom(idx):
            raise SystemExit()
        monkeypatch.setattr(sub, "_close_old_connection", _boom)
        a.drop()
        assert wait_until(lambda: not sub._sock_state[0]["reconnecting"])
        assert not sub._sock_state[0]["up"]
        monkeypatch.undo()
        sub._watchdog_once()
        assert wait_until(lambda: _up(sub, 0))

    def test_no_connection_is_opened_after_stop(self, inner_sub):
        sub, inner = inner_sub()
        sub._close_wait_sec = 2.0
        inner.linger = 0.3                                   # 重連 thread 正在等舊迴圈結束
        sub._socket_down(0, "停擺")
        time.sleep(0.1)
        sub._stop.set()
        assert wait_until(lambda: not sub._sock_state[0]["reconnecting"], timeout=3)
        time.sleep(0.15)
        assert inner.run_calls == 1, "stop() 之後不可以再開連線"


    def test_stop_during_the_settle_pause_does_not_open_a_connection(self, subs):
        sub, (a, _b) = subs()
        sub._reconnect_settle_sec = 0.4                      # disconnect() 之後、開新連線之前的停頓
        a.drop()
        assert wait_until(lambda: a.disconnect_calls >= 1)   # 重連 thread 已走到停頓
        sub._stop.set()
        assert wait_until(lambda: not sub._sock_state[0]["reconnecting"], timeout=3)
        time.sleep(0.1)
        assert a.connect_calls == 0, "stop() 之後不可以再開連線"


class TestFlapping:
    """連上、認證過、馬上又被踢: 每次都從 2 s 重來的話,一條 socket 每分鐘開 20 幾條連線、永遠不換 session
    (券商對短時間大量連線是會擋的)。沒撐過 MD_FLAP_SEC 的「成功」要接著上一輪繼續退避、累計失敗。"""

    @staticmethod
    def _kick_after_connect(inner, times, after=0.02):
        left = [times]
        real_run = inner._run

        def _run():
            if left[0] > 0:
                left[0] -= 1
                threading.Timer(after, inner.drop).start()
            real_run()
        inner._run = _run

    def test_backoff_and_failure_count_carry_over_a_recovery_that_did_not_last(self, inner_sub, caplog):
        sub, inner = inner_sub()
        sub._flap_sec = 5.0
        self._kick_after_connect(inner, times=3)
        inner.drop()
        assert wait_until(lambda: _up(sub, 0) and sub._sock_state[0]["reconnects"] == 4, timeout=8)
        ok = [r.getMessage() for r in _crit(caplog, "重連成功")]
        assert ["第 1 次" in ok[0], "第 2 次" in ok[1], "第 3 次" in ok[2], "第 4 次" in ok[3]] == [True] * 4

    def test_persistent_kick_moves_to_a_fresh_session(self, inner_sub, fake_sdk):
        sub, old = inner_sub()
        _with_creds(sub)
        sub._flap_sec = 5.0
        self._kick_after_connect(old, times=50)              # 舊 session 怎麼連都是連上就被踢
        old.drop()
        assert wait_until(lambda: _up(sub, 0) and sub._sock_state[0]["rebuilds"] == 1, timeout=8)
        time.sleep(0.15)
        assert _up(sub, 0) and sub._sockets[0] is fake_sdk.made[0].marketdata.websocket_client.stock

    def test_connection_that_lasted_starts_from_scratch(self, inner_sub, caplog):
        sub, inner = inner_sub()
        sub._flap_sec = 0.05
        inner.drop()
        assert wait_until(lambda: _up(sub, 0))
        time.sleep(0.12)                                     # 撐過 flap 窗口
        caplog.clear()
        inner.drop()
        assert wait_until(lambda: _up(sub, 0))
        assert "第 1 次嘗試" in _crit(caplog, "重連成功")[0].getMessage()

    def test_default_window_is_shorter_than_the_stall_watchdog(self):
        """看門狗誤判停擺最快也要約 20 s 之後 — 那種情況只該花一次 2 s 的重連,不可以被當成 flap 一路退避。"""
        sub = Subscriber(sdk=None, universe=[], on_book=lambda *a: None, login_cfg=None)
        assert 0 < sub._flap_sec < sub._stall_sec


class TestConnectionCap:
    def test_cap_text_that_only_arrives_with_the_close_event_is_recognised(self, inner_sub, fake_sdk, caplog):
        """舊版 websocket-client: 伺服器的關閉 frame 不走 error 事件,原因只在 disconnect 事件的參數裡。"""
        sub, old = inner_sub()
        _with_creds(sub)
        sub._conn_cap_wait_sec = 0.3
        t0 = time.monotonic()
        old.emit("disconnect", 1001, "Maximum number of connections reached")
        assert len(_crit(caplog, "連線數達上限")) == 1
        assert wait_until(lambda: _up(sub, 0), timeout=5)
        assert time.monotonic() - t0 >= 0.3 and sub._sock_state[0]["rebuilds"] == 1

    def test_cap_error_waits_for_slots_then_uses_a_fresh_session(self, inner_sub, fake_sdk, caplog):
        """2026-08-25 day-trade 的死亡螺旋: 連線名額滿 → 連上就被踢 → 立刻再連 → 永遠爬不出來。"""
        sub, old = inner_sub()
        old_sdk = SimpleNamespace(logout=lambda: fake_sdk.events.append("logout-old"))
        _with_creds(sub, old_sdk)
        sub._conn_cap_wait_sec = 0.4
        t0 = time.monotonic()
        old.emit("error", Exception("Maximum number of connections reached"))
        old.emit("error", Exception("Maximum number of connections reached"))
        old.drop()
        assert len(_crit(caplog, "連線數達上限")) == 1
        assert wait_until(lambda: _up(sub, 0), timeout=5)
        assert time.monotonic() - t0 >= 0.4, "先等券商回收名額"
        assert old.run_calls == 1, "不在舊 session 上再連"
        assert fake_sdk.events[:2] == ["logout-old", "login"], "名額滿: 先登出舊的再登入新的"
        assert sub._sock_state[0]["cap_until"] == 0.0


# ═══ 開機第一次連線 ═══════════════════════════════════════════════════

class TestInitialConnect:
    def _boot(self, socks, universe=UNIVERSE):
        sub = Subscriber(sdk=None, universe=list(universe), on_book=lambda *a: None, login_cfg=None)
        sub._sockets = socks
        _fast(sub)
        sub._connect_grace_sec = 0.01
        sub.batch_rotate_sec = 0.05
        return sub

    def test_startup_connects_without_the_library_busy_wait(self):
        """開機也不呼叫函式庫的 connect() (連不上時它是不會返回的忙等;node 是 08:59:50 才連行情)。"""
        a, b = FakeInner("ok"), FakeInner("ok")
        sub = self._boot([Wrapper(a), Wrapper(b)])
        try:
            sub._start_multi_socket_loop()               # FakeInner.connect() 被呼叫會直接 AssertionError
            assert a.connected and b.connected and _up(sub, 0) and _up(sub, 1)
            assert wait_until(lambda: ("books", "1101") in a.subscribed and ("books", "1102") in b.subscribed)
        finally:
            sub._stop.set()
            a.closed.set()
            b.closed.set()

    def test_unreachable_endpoint_at_startup_does_not_hang_or_spin(self, caplog):
        """開機時行情主機連不上: 舊版主流程永遠卡在 connect() 的忙等、吃滿一顆 CPU,後面的 socket 也開不了。"""
        a, b = FakeInner(["refused", "refused", "ok"]), FakeInner("ok")
        sub = self._boot([Wrapper(a), Wrapper(b)])
        try:
            cpu0, t0 = time.process_time(), time.monotonic()
            sub._start_multi_socket_loop()
            assert time.monotonic() - t0 < 2 and time.process_time() - cpu0 < 0.3
            assert b.connected, "第一條連不上不影響後面的"
            assert len(_crit(caplog, "行情 socket #1 斷線")) == 1
            assert wait_until(lambda: _up(sub, 0), timeout=5), "由自動重連接手 (舊版: 這條就整天不收)"
            assert wait_until(lambda: ("books", "1101") in a.subscribed, timeout=3)
        finally:
            sub._stop.set()
            a.closed.set()
            b.closed.set()

    def test_single_socket_startup_failure_is_handed_to_the_reconnect_loop(self, caplog):
        """node 唯一的一條開機連不上: 舊版直接 raise → 整台當天不收行情 (隔日賣也賣不掉)。現在繼續重連;
        連回來之前這條連線上的檔都算斷線中 (runner 不會為它們預掛 / 盲送)。"""
        a = FakeInner(["authfail", "authfail", "ok"])
        sub = self._boot([Wrapper(a)], universe=["1101"])
        try:
            sub._start_loop_single_socket()              # 不可以 raise
            assert sub.down_symbols(["1101"]) == {"1101"}
            assert len(_crit(caplog, "行情 socket #1 斷線")) == 1
            assert wait_until(lambda: _up(sub, 0), timeout=5)
            assert sub.down_symbols(["1101"]) == set()
            assert wait_until(lambda: ("books", "1101") in a.subscribed, timeout=3)
        finally:
            sub._stop.set()
            a.closed.set()

    def test_public_connect_fallback_at_startup_is_bounded_too(self, caplog):
        """拿不到函式庫內部欄位 (版本不同) 時退回公開的 connect(): 一樣有期限,卡住會被踢出來。"""
        a, b = FakeWS(connected=False), FakeWS(connected=False)
        a.connect_script = ["hang"]
        sub = self._boot([a, b])
        try:
            t0 = time.monotonic()
            sub._start_multi_socket_loop()
            assert time.monotonic() - t0 < 3 and b.connected
            assert not a.connected, "開機那次被踢出來,還沒連上"
            assert wait_until(lambda: a.connected and a.connect_calls >= 2 and _up(sub, 0),
                              timeout=5), "交給自動重連,之後真的連上"
            assert wait_until(lambda: ("books", "1101") in a.subscribed, timeout=3)
        finally:
            sub._stop.set()


# ═══ 過期判定 ═══════════════════════════════════════════════════════

class TestStale:
    def test_everything_on_a_down_socket_is_stale_and_other_sockets_are_not(self, subs):
        sub, (a, b) = subs()
        a.book("1101")
        b.book("1102")
        assert sub.stale_symbols(UNIVERSE) == set() and sub.down_symbols(UNIVERSE) == set()
        a.connect_script = [RuntimeError("boom")] * 1000     # 一直連不回來
        a.drop()
        assert sub.stale_symbols(UNIVERSE) == {"1101", "1103"}
        assert sub.down_symbols(UNIVERSE) == {"1101", "1103"}

    def test_after_reconnect_a_symbol_stays_stale_until_its_own_fresh_book(self, subs):
        """重連後沒有初始畫面 — 斷線前收過五檔不算數,要重連**之後**這一檔自己再來一筆。"""
        books = []
        sub, (a, _b) = subs(books=books)
        a.book("1101")
        a.book("1103")
        a.drop()
        assert wait_until(lambda: _up(sub, 0))
        assert sub.stale_symbols(UNIVERSE) == {"1101", "1103"}
        assert sub.down_symbols(UNIVERSE) == set(), "連線已回來 → 不算斷線中 (node 只看這個)"
        a.book("1101")
        assert sub.stale_symbols(UNIVERSE) == {"1103"}, "只有收到新五檔的那一檔恢復"
        assert books.count("1101") == 2, "新五檔照常交給 handler (由它決定還鎖不鎖)"
        a.book("1103")
        assert sub.stale_symbols(UNIVERSE) == set()

    def test_book_that_answers_the_resubscribe_immediately_counts_as_fresh(self, subs):
        sub, (a, _b) = subs()
        a.on_subscribe = lambda p: a.book(p["symbol"]) if p["channel"] == "books" else None
        a.drop()
        assert wait_until(lambda: _up(sub, 0))
        assert sub.stale_symbols(UNIVERSE) == set()

    def test_uninitialised_health_means_nothing_is_stale(self):
        sub = Subscriber(sdk=None, universe=["1101"], on_book=lambda *a: None, login_cfg=None)
        sub._sockets = [object()]
        sub._rebuild_symbol_index()
        assert sub.is_stale("1101") is False and sub.stale_symbols(["1101"]) == set()
        assert sub.is_down("1101") is False and sub.down_symbols(["1101"]) == set()
        assert sub.socket_health() == {"total": 0, "down": 0, "reconnects": 0, "detail": []}

    def test_health_summary(self, subs):
        sub, (a, _b) = subs()
        a.connect_script = [RuntimeError("boom")] * 1000
        a.connected = False
        a.emit("disconnect", 1006, "abnormal closure")
        h = sub.get_tick_stats()["sockets"]
        assert h["total"] == 2 and h["down"] == 1
        assert h["detail"][0]["up"] is False and "1006" in h["detail"][0]["reason"]
        assert h["detail"][1]["up"] is True and h["detail"][1]["reason"] == ""
        assert h["detail"][0]["rebuilds"] == 0


# ═══ 看門狗: 沒有斷線事件的停擺 ═══════════════════════════════════════

class TestWatchdog:
    def test_quiet_socket_is_pinged_and_a_pong_keeps_it_up(self, subs):
        sub, (a, b) = subs()
        now = time.monotonic()
        sub._sock_state[0]["last_msg_mono"] = now - 6        # #0 安靜了 6 s (> 5 s)
        sub._watchdog_once()
        assert a.pings == ["wd"] and b.pings == [], "有資料在進來的連線不必 ping"
        assert sub._sock_state[0]["last_msg_mono"] >= now, "pong 也是訊息 = 連線活著"
        sub._watchdog_once(time.monotonic() + 60)
        assert _up(sub, 0)

    def test_no_message_and_no_pong_forces_a_reconnect(self, subs, caplog):
        sub, (a, _b) = subs()
        a.pong = False
        now = time.monotonic()
        sub._sock_state[0]["last_msg_mono"] = now - 6
        for dt in range(15):                                 # 看門狗每秒一輪
            sub._watchdog_once(now + dt)
        assert sub._sock_state[0]["up"], "第一個沒回應的 ping 之後未滿 15 s"
        assert len(a.pings) == 3, "安靜期間每 5 s 探一次 (第 0 / 5 / 10 s)"
        sub._watchdog_once(now + 15)
        assert len(_crit(caplog, "沒有任何訊息")) == 1
        assert wait_until(lambda: _up(sub, 0))
        assert a.disconnect_calls >= 1 and a.connect_calls == 1, "停擺的舊連線要先關掉再連"
        assert sub.stale_symbols(UNIVERSE) == {"1101", "1103"}, "重連後仍要等新五檔"

    def test_long_silence_without_an_outstanding_ping_gets_one_chance_first(self, subs, caplog):
        """整個 process 被凍住之後醒來 (安靜很久但其實還沒 ping 過): 先 ping,不可以直接判停擺。"""
        sub, (a, _b) = subs()
        a.pong = False
        now = time.monotonic()
        sub._sock_state[0]["last_msg_mono"] = now - 300
        sub._watchdog_once(now)
        assert a.pings == ["wd"] and sub._sock_state[0]["up"] and _crit(caplog, "沒有任何訊息") == []

    def test_ping_that_cannot_be_sent_marks_the_socket_down(self, subs, caplog):
        sub, (a, _b) = subs()
        a.connected = False                                  # 連線其實已關,但沒有事件進來
        sub._sock_state[0]["loop_closed"] = True
        sub._sock_state[0]["last_msg_mono"] = time.monotonic() - 6
        sub._watchdog_once()
        assert len(_crit(caplog, "ping 送不出去")) == 1
        assert wait_until(lambda: _up(sub, 0))

    def test_down_or_reconnecting_sockets_are_left_alone(self, subs):
        sub, (a, _b) = subs()
        a.connect_script = [RuntimeError("boom")] * 1000
        a.drop()
        n = len(a.pings)
        sub._watchdog_once(time.monotonic() + 100)
        assert len(a.pings) == n

    def test_env_can_tune_or_disable(self, monkeypatch):
        monkeypatch.setenv("MD_STALL_SEC", "0")
        monkeypatch.setenv("MD_IDLE_PING_SEC", "9")
        sub = Subscriber(sdk=None, universe=[], on_book=lambda *a: None, login_cfg=None)
        assert sub._stall_sec == 0 and sub._idle_ping_sec == 9
        monkeypatch.setenv("MD_STALL_SEC", "abc")
        assert Subscriber(sdk=None, universe=[], on_book=lambda *a: None, login_cfg=None)._stall_sec == 20.0

    def test_defaults(self):
        sub = Subscriber(sdk=None, universe=[], on_book=lambda *a: None, login_cfg=None)
        assert (sub._stall_sec, sub._idle_ping_sec) == (sub_mod.MD_STALL_SEC_DEFAULT, sub_mod.MD_IDLE_PING_SEC_DEFAULT)
        assert sub._close_wait_sec > 10, "Linux 上舊收訊迴圈要 ~10 s 才結束 (day-trade 實測)"
        assert sub._rebuild_after_fails == 3 and sub._rebuild_cooldown_sec == 60.0


# ═══ socket loop 與重連並存 ═══════════════════════════════════════════

class TestSocketLoopWhileDown:
    def test_loop_does_not_hammer_a_down_socket_and_resumes_afterwards(self, subs, caplog):
        caplog.set_level(logging.WARNING, logger="subscriber")
        sub, (a, _b) = subs()
        sub._connect_grace_sec = 0.01
        sub.batch_rotate_sec = 0.05
        sub._reconnect_step_sec = 0.05
        sub._reconnect_max_sec = 0.1
        a.connect_script = [RuntimeError("boom")] * 4
        a.drop()
        t = threading.Thread(target=sub._socket_loop_thread, args=(a, [["1101", "1103"]], 0, None),
                             daemon=True)
        t.start()
        try:
            time.sleep(0.12)
            assert a.subscribed == [], "斷線中: socket loop 不送訂閱"
            assert "subscribe 0 ok" not in caplog.text
            assert wait_until(lambda: _up(sub, 0), timeout=5)
            assert wait_until(lambda: a.subscribed.count(("books", "1101")) >= 2, timeout=3), \
                "連回來之後 loop 照常每輪重訂"
            assert sub._cur_batch[0] == ["1101", "1103"]
        finally:
            sub._stop.set()
            t.join(3)
