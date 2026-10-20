"""淘汰 (UNMARK) 的那一刻就停止市價盲送 — 不等撤單往返回來 (2026-10-12 6872)。

事故: node 在 08:59:59.2 看到賣單出現 → 淘汰 → 撤預掛單。撤單是同步的券商往返 (當天 0.2~0.4 s),而
「停止進場」的旗標 (st.stopped_reason) 要等撤單**回來**才設 → 這段時間盲送 cadence 照送,五台 node 在淘汰
之後又送了 56 筆市價單。當時是集合競價所以全部被退;同樣的事發生在 09:00:00 之後就是真的成交。"""
import threading
import time
from datetime import time as dtime

from test_session_money import make_session
from fakes_cancel import FakeSnapshotBroker, wait_until


class SlowCancelBroker(FakeSnapshotBroker):
    """市價單一律被退 (集合競價,非致命 → cadence 續送) 並記下送出時刻;撤單要等 gate (模擬券商往返)。"""

    def __init__(self):
        super().__init__(default_status="", user_def="hitlimit", lenient_cancel=True)
        self.ev = []                         # [(symbol, monotonic)]
        self.gate = threading.Event()
        self.cancel_started = threading.Event()
        self.flag_at_cancel = []

    def place_market_buy(self, sym, lots):
        with self._lk:
            self.ev.append((sym, time.monotonic()))
        raise Exception("集合競價時段不可輸入市價委託")

    def cancel(self, order_no, symbol="", reason=""):
        self.cancel_started.set()
        self.gate.wait(3)
        return super().cancel(order_no, symbol, reason)

    def get_order_filled_lots(self, o):
        return 0


def _session(symbols=("S0",), broker_cls=None):
    s = make_session(total=500_000_000, per_symbol=300_000)
    s.broker = (broker_cls or SlowCancelBroker)()
    s.cancel_pending_time = dtime(23, 59, 59)
    s.place_pre_orders(list(symbols), {x: 20.0 for x in symbols})
    return s


def _chase(s, sym):
    th = threading.Thread(target=s._market_chase_worker,
                          args=(sym, dtime(0, 0, 0), dtime(23, 59, 59)), daemon=True)
    th.start()
    return th


class TestStopEntry:
    def test_flag_is_set_at_once_without_touching_the_broker(self):
        s = _session()
        s.stop_entry("S0", "unmarked")
        assert s.trades["S0"].stopped_reason == "unmarked"
        assert not s.broker.cancel_started.is_set()

    def test_first_reason_wins_and_unknown_symbol_or_empty_reason_is_a_no_op(self):
        s = _session()
        s.stop_entry("S0", "unmarked")
        s.stop_entry("S0", "first_check: x")
        assert s.trades["S0"].stopped_reason == "unmarked"
        s.stop_entry("NOPE", "unmarked")                 # 今天沒預掛這檔
        s2 = _session()
        s2.stop_entry("S0", "")
        assert not s2.trades["S0"].stopped_reason

    def test_async_cancel_sets_the_flag_before_the_broker_answers(self):
        s = _session()
        try:
            s.cancel_symbol_orders_async("S0", "unmarked")
            assert s.trades["S0"].stopped_reason == "unmarked", "呼叫的當下就要設,不等背景 thread"
            assert s.broker.cancel_started.wait(2)
            assert s.trades["S0"].order_status == "pending", "撤單還沒回來"
        finally:
            s.broker.gate.set()
        assert wait_until(lambda: s.trades["S0"].order_status == "cancelled")

    def test_async_cancel_sets_the_flag_on_the_calling_thread(self):
        """旗標不可以依賴背景 thread 起跑 (行情 thread 上 spawn 一條 thread 也要時間)。"""
        s = _session()
        hold = threading.Event()
        s.cancel_symbol_orders = lambda *a: hold.wait(3)     # 背景那支完全不做事
        try:
            s.cancel_symbol_orders_async("S0", "unmarked")
            assert s.trades["S0"].stopped_reason == "unmarked"
        finally:
            hold.set()

    def test_sync_cancel_sets_the_flag_before_calling_the_broker(self):
        s = _session()
        seen = []
        real = s.broker.cancel

        def _cancel(order_no, symbol="", reason=""):
            seen.append(s.trades["S0"].stopped_reason)
            s.broker.gate.set()
            return real(order_no, symbol, reason)
        s.broker.cancel = _cancel
        s.cancel_symbol_orders("S0", "unmarked")
        assert seen == ["unmarked"] and s.trades["S0"].order_status == "cancelled"


class TestBlindChaseStopsOnUnmark:
    def test_no_market_order_goes_out_while_the_cancel_is_still_in_flight(self):
        """重演: 盲送中 → 淘汰 → 撤單往返 0.4 s。往返期間不可以再送任何一筆市價單。"""
        s = _session()
        th = _chase(s, "S0")
        try:
            assert wait_until(lambda: len(s.broker.ev) >= 5), "盲送要先跑起來"
            s.cancel_symbol_orders_async("S0", "unmarked")
            t_unmark = time.monotonic()
            assert s.broker.cancel_started.wait(2)
            time.sleep(0.4)                              # 撤單往返中 (舊版這段照送,約 45/s)
            late = [t - t_unmark for _sym, t in s.broker.ev if t - t_unmark > 0.03]
            assert late == [], f"淘汰之後還送了 {len(late)} 筆市價單"
            th.join(2)
            assert not th.is_alive(), "cadence 迴圈要結束"
        finally:
            s.broker.gate.set()
        assert wait_until(lambda: s.trades["S0"].order_status == "cancelled")

    def test_other_symbols_keep_chasing(self):
        s = _session(("S0", "S1"))
        t0, t1 = _chase(s, "S0"), _chase(s, "S1")
        try:
            assert wait_until(lambda: {x for x, _ in s.broker.ev} == {"S0", "S1"})
            s.cancel_symbol_orders_async("S0", "unmarked")
            t_unmark = time.monotonic()
            time.sleep(0.3)
            after = [sym for sym, t in s.broker.ev if t - t_unmark > 0.03]
            assert after and set(after) == {"S1"}, "只停被淘汰的那一檔"
        finally:
            s.trades["S1"].stopped_reason = "stop"
            s.broker.gate.set()
            t0.join(2)
            t1.join(2)


class TestSendIsSkippedOnceStopped:
    def test_stopped_exited_or_breached_means_no_send(self):
        for how in ("stopped", "exited", "breach"):
            s = _session()
            st = s.trades["S0"]
            if how == "stopped":
                st.stopped_reason = "unmarked"
            elif how == "exited":
                st.exited = True
            else:
                s._budget_breached = True
            assert s._chase_send_one("S0", 1) == "stopped", how
            assert s.broker.ev == [], f"{how}: 已經決定停了就不可以再送"

    def test_still_sends_when_nothing_stopped_it(self):
        s = _session()
        assert s._chase_send_one("S0", 1) == "rejected"   # 集合競價被退 = 有送出去
        assert len(s.broker.ev) == 1


class SlowAcceptBroker(SlowCancelBroker):
    """開盤後: 市價單會被受理,但券商回覆要 0.25 s (管線化 → 同時有好幾筆在飛)。"""

    def place_market_buy(self, sym, lots):
        with self._lk:
            self.ev.append((sym, time.monotonic()))
        time.sleep(0.25)
        return FakeSnapshotBroker.place_market_buy(self, sym, lots)


class TestStopAfterTheOpen:
    def test_first_check_discard_stops_new_market_orders_at_once(self):
        """真正會花錢的情境: 09:00 之後市價單會被受理。Trader 首盤檢查淘汰 (first_check) 走同一支
        cancel_symbol_orders_async — 舊版在撤單往返期間照送,而且這時候送出去的是會成交的單。"""
        s = _session(broker_cls=SlowAcceptBroker)
        th = _chase(s, "S0")
        try:
            assert wait_until(lambda: len(s.broker.ev) >= 3), "盲送要先跑起來 (都還在等券商回覆)"
            s.cancel_symbol_orders_async("S0", "first_check: 委賣一出現")
            t_stop = time.monotonic()
            time.sleep(0.5)
            late = [t - t_stop for _sym, t in s.broker.ev if t - t_stop > 0.03]
            assert late == [], f"淘汰之後還送了 {len(late)} 筆會被受理的市價單"
            assert s.trades["S0"].stopped_reason == "first_check: 委賣一出現"
        finally:
            s.broker.gate.set()
            th.join(3)
        # 淘汰當下已經在券商那邊的單收不回來: 被受理的要撤掉 (既有的「送出後發現已停」那條)
        assert wait_until(lambda: all(r["status"] != "pending" or r.get("cancel_state")
                                      for r in s.order_log.values() if r["kind"] == "market_buy"))


class TestUnmarkCallbackOrder:
    """runner 的淘汰回呼: 先停進場 + 撤單,再退訂;任何一步丟例外都不可以讓另一步沒做。"""

    def _runner(self):
        from types import SimpleNamespace
        from runner import Runner
        r = Runner()
        calls = []
        r.session = SimpleNamespace(
            cancel_symbol_orders_async=lambda sym, reason: calls.append(("cancel", sym, reason)),
            has_overnight=lambda sym: False)
        r.subscriber = SimpleNamespace(request_unsubscribe=lambda sym: calls.append(("unsub", sym)))
        return r, calls

    def test_stop_comes_first(self):
        r, calls = self._runner()
        r._unmark_stop_and_unsub("6872", "node")
        assert calls == [("cancel", "6872", "unmarked"), ("unsub", "6872")]

    def test_unsubscribe_failure_does_not_skip_the_stop(self):
        r, calls = self._runner()

        def _boom(sym):
            raise RuntimeError("socket is already closed.")
        r.subscriber.request_unsubscribe = _boom
        r._unmark_stop_and_unsub("6872")                     # 不可以 raise
        assert calls == [("cancel", "6872", "unmarked")]

    def test_cancel_failure_does_not_skip_the_unsubscribe(self):
        r, calls = self._runner()

        def _boom(sym, reason):
            raise RuntimeError("boom")
        r.session.cancel_symbol_orders_async = _boom
        r._unmark_stop_and_unsub("6872")
        assert calls == [("unsub", "6872")]

    def test_marks_exit_on_fill_first_and_keeps_feed_while_buy_live(self):
        """2026-10-20 8089 同型 (盤前 unmark): 先標「成交落地即出場」,再撤單;買進委託在途 → 不退訂。"""
        r, calls = self._runner()
        r.session.mark_exit_on_fill = lambda sym, reason: calls.append(("mark", sym, reason))
        r.session.has_live_buy_order = lambda sym: True
        r.session.get_filled_lots = lambda sym: 0
        r._unmark_stop_and_unsub("6872", "node")
        assert calls == [("mark", "6872", "unmarked:node"), ("cancel", "6872", "unmarked")]

    def test_unsubscribes_when_no_order_and_no_position(self):
        r, calls = self._runner()
        r.session.mark_exit_on_fill = lambda sym, reason: calls.append(("mark", sym, reason))
        r.session.has_live_buy_order = lambda sym: False
        r.session.get_filled_lots = lambda sym: 0
        r._unmark_stop_and_unsub("6872", "node")
        assert calls == [("mark", "6872", "unmarked:node"), ("cancel", "6872", "unmarked"), ("unsub", "6872")]

    def test_mark_failure_does_not_skip_cancel(self):
        r, calls = self._runner()

        def _boom(sym, reason):
            raise RuntimeError("boom")
        r.session.mark_exit_on_fill = _boom
        r._unmark_stop_and_unsub("6872")
        assert calls == [("cancel", "6872", "unmarked"), ("unsub", "6872")]
