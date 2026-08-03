"""filter.make_on_book_handler — 8:30–9:00 mark/unmark 判斷 (合成五檔餵進去)。

時間分支不 mock 時鐘,用 cfg 的時間字串控制:
- final_check_start="23:59:58" → 測試全程都在「只記錄 max」窗口
- final_check_start="00:00:00" → 測試全程都在「final check 量減半」窗口
"""
import json
from types import SimpleNamespace

from filter import make_on_book_handler
from state import State


def _cfg(final_check_start="23:59:58", pre_order_time="23:59:59"):
    # handler 只讀這兩個欄位 (closure 建構時 parse)
    return SimpleNamespace(final_check_start=final_check_start,
                           pre_order_time=pre_order_time)


def _mk(state, limit_ups, cfg=None, unsub_calls=None):
    unsub_ref = {"fn": unsub_calls.append if unsub_calls is not None else None}
    return make_on_book_handler(state, limit_ups, cfg or _cfg(), unsub_ref)


BID_LOCK = [{"price": 100.0, "size": 50}]      # 買一 = 漲停價
NO_ASKS = []


class TestMark:
    def test_mark_when_bid_at_limit_and_no_asks(self):
        state = State()
        h = _mk(state, {"2330": 100.0})
        h("2330", BID_LOCK, NO_ASKS)
        assert state.is_marked("2330")

    def test_no_mark_below_limit(self):
        state = State()
        h = _mk(state, {"2330": 100.0})
        h("2330", [{"price": 99.5, "size": 50}], NO_ASKS)
        assert not state.is_marked("2330")

    def test_no_mark_when_ask_present(self):
        state = State()
        h = _mk(state, {"2330": 100.0})
        h("2330", BID_LOCK, [{"price": 100.5, "size": 3}])
        assert not state.is_marked("2330")

    def test_symbol_without_limit_up_ignored(self):
        state = State()
        h = _mk(state, {"2330": 100.0})
        h("9999", BID_LOCK, NO_ASKS)
        assert not state.is_marked("9999")

    def test_first_quote_lock_in_badge(self):
        state = State()
        h = _mk(state, {"2330": 100.0, "1101": 50.0})
        # 2330 第一筆真實報價就鎖 → 開盤即鎖
        h("2330", BID_LOCK, NO_ASKS)
        assert state.is_first_tick("2330")
        # 1101 第一筆沒鎖,第二筆才鎖 → 非開盤即鎖
        h("1101", [{"price": 49.5, "size": 10}], NO_ASKS)
        h("1101", [{"price": 50.0, "size": 10}], NO_ASKS)
        assert state.is_marked("1101")
        assert not state.is_first_tick("1101")


class TestUnmark:
    def test_ask_appeared_unmarks_and_unsubscribes(self):
        state = State()
        calls = []
        h = _mk(state, {"2330": 100.0}, unsub_calls=calls)
        h("2330", BID_LOCK, NO_ASKS)
        h("2330", BID_LOCK, [{"price": 100.5, "size": 2}])
        assert state.is_discarded("2330")
        assert calls == ["2330"]
        # 淘汰後殘餘 tick 直接略過,不會重複退訂
        h("2330", BID_LOCK, NO_ASKS)
        assert calls == ["2330"]

    def test_bid_below_limit_unmarks(self):
        state = State()
        calls = []
        h = _mk(state, {"2330": 100.0}, unsub_calls=calls)
        h("2330", BID_LOCK, NO_ASKS)
        h("2330", [{"price": 99.5, "size": 50}], NO_ASKS)
        assert state.is_discarded("2330")
        assert calls == ["2330"]


class TestBidDropWindows:
    def test_record_window_only_tracks_max(self):
        # now < final_check_start → 量掉再多也不淘汰,只記 max
        state = State()
        h = _mk(state, {"2330": 100.0}, cfg=_cfg("23:59:58", "23:59:59"))
        h("2330", [{"price": 100.0, "size": 100}], NO_ASKS)
        h("2330", [{"price": 100.0, "size": 5}], NO_ASKS)
        assert state.is_marked("2330")

    def test_final_check_window_unmarks_on_half_drop(self):
        # final_check_start="00:00:00" → 全程在 final check 窗口
        state = State(bid_drop_ratio=0.5)
        calls = []
        h = _mk(state, {"2330": 100.0}, cfg=_cfg("00:00:00", "23:59:59"),
                unsub_calls=calls)
        h("2330", [{"price": 100.0, "size": 100}], NO_ASKS)   # mark, max=100
        h("2330", [{"price": 100.0, "size": 40}], NO_ASKS)    # 40 < 100*0.5 → 淘汰
        assert state.is_discarded("2330")
        assert calls == ["2330"]

    def test_after_pre_order_time_list_is_frozen(self):
        # now >= pre_order_time → 量減半不再淘汰 (預掛單已出);賣單 unmark 照常
        state = State(bid_drop_ratio=0.5)
        h = _mk(state, {"2330": 100.0}, cfg=_cfg("00:00:00", "00:00:00"))
        h("2330", [{"price": 100.0, "size": 100}], NO_ASKS)
        h("2330", [{"price": 100.0, "size": 5}], NO_ASKS)     # 凍結 → 不淘汰
        assert state.is_marked("2330")
        h("2330", BID_LOCK, [{"price": 100.5, "size": 1}])    # 賣單出現照常淘汰
        assert state.is_discarded("2330")


class TestSubscriberRouting:
    """symbol→socket O(1) dict 路由 — 必須與舊的 universe.index % n 公式完全一致。"""

    def test_dict_routing_matches_index_formula(self):
        from subscriber import Subscriber
        universe = [f"{i:04d}" for i in range(1, 21)]
        sub = Subscriber(sdk=None, universe=list(universe), on_book=None, login_cfg=None)
        sub._sockets = ["SOCK_A", "SOCK_B", "SOCK_C"]
        sub._rebuild_symbol_index()
        for sym in universe:
            expected = sub._sockets[universe.index(sym) % 3]   # 舊公式
            assert sub._socket_for(sym) is expected
        # 查無的 symbol → socket 0 (同舊 ValueError fallback)
        assert sub._socket_for("9999") is sub._sockets[0]

    def test_add_symbol_extends_index(self):
        from subscriber import Subscriber
        sub = Subscriber(sdk=None, universe=["1101", "2330"], on_book=None, login_cfg=None)
        sub._sockets = ["A", "B"]
        sub._rebuild_symbol_index()
        # add_symbol 需要 ws.subscribe — 用假 socket 物件
        class _WS:
            def subscribe(self, arg): pass
        sub._sockets = [_WS(), _WS()]
        sub._rebuild_symbol_index()
        assert sub.add_symbol("5555")
        assert sub._symbol_idx["5555"] == 2          # append 在位置 2
        assert sub._socket_for("5555") is sub._sockets[0]   # 2 % 2 = 0


class TestSubscriberHandlerNeverSwallows:
    """Fix: handler 例外不再靜默吞掉 (subscriber._make_msg_handler)。"""

    def test_on_book_exception_is_caught_and_counted(self):
        from subscriber import Subscriber

        def _raiser(symbol, bids, asks):
            raise RuntimeError("boom")

        sub = Subscriber(sdk=None, universe=[], on_book=_raiser, login_cfg=None)
        h = sub._make_msg_handler(0)
        msg = json.dumps({"event": "data", "channel": "books",
                          "data": {"symbol": "2330", "bids": [], "asks": []}})
        h(msg)      # 不能往外拋
        keys = [k for k in sub._error_count if k.startswith("handler:RuntimeError")]
        assert keys and sub._error_count[keys[0]] == 1
