"""node 端到端核心 (中心過濾架構): Hub 快照 → 兩步 seed(峰值+last)→ node 用**與 standalone
相同的完整 filter handler**(2026-09-01 對齊:輕量 handler 不 unmark → 鎖破檔照預掛,已移除)
續墊峰值 + unmark 鎖破檔 → 08:59:58 量減半(state.final_check_all)→ 存活 marked = 預掛清單。
時間分支不 mock 時鐘 (同 test_filter_handler): pre_order_time="23:59:59" → 全程在記錄 max/last 窗口。"""
from types import SimpleNamespace

from filter import make_on_book_handler
from state import State


def _cfg():
    return SimpleNamespace(pre_order_time="23:59:59", final_check_start="23:59:58")


def _mk(state, limit_ups, unsub_calls=None):
    unsub_ref = {"fn": unsub_calls.append if unsub_calls is not None else None}
    return make_on_book_handler(state, limit_ups, _cfg(), unsub_ref)


def _book(price, size):
    return [{"price": price, "size": size}], []          # (bids, asks) — 無賣單


def _seed_from_snapshot(state, rows):
    """模擬 node 拉到 Hub 快照後兩步 seed: mark(峰值) → update_max_bid(last)。"""
    for sym, limit_up, max_bid_vol, last_bid_vol, first_tick in rows:
        state.mark(sym, limit_up, max_bid_vol, limit_up, first_tick=first_tick)
        state.update_max_bid(sym, last_bid_vol)


class TestNodeSeedAndFinalCheck:
    def test_final_check_drops_weakened_keeps_strong(self):
        st = State(bid_drop_ratio=0.5)
        _seed_from_snapshot(st, [("A", 100.0, 1000, 1000, True), ("B", 50.0, 800, 800, False)])
        on_book = _mk(st, {"A": 100.0, "B": 50.0})
        # node 08:59:50–58 收到當前量: A 900 (≥ 1000×0.5 保留)、B 300 (< 800×0.5 剔除)
        on_book("A", *_book(100.0, 900))
        on_book("B", *_book(50.0, 300))
        dropped = st.final_check_all()
        assert [d[0] for d in dropped] == ["B"]
        assert st.get_marked_list() == ["A"]              # 存活 = 預掛清單

    def test_node_extends_peak_in_last_8s(self):
        # 峰值不凍結 — node 最後 8 秒看到更高量 → 墊高峰值 → 量減半更嚴
        st = State(bid_drop_ratio=0.5)
        _seed_from_snapshot(st, [("A", 100.0, 800, 800, False)])
        on_book = _mk(st, {"A": 100.0})
        on_book("A", *_book(100.0, 1200))                 # 峰值墊到 1200
        assert st.get_max_bid_size("A") == 1200
        on_book("A", *_book(100.0, 500))                  # 當前掉到 500
        dropped = st.final_check_all()
        assert [d[0] for d in dropped] == ["A"]           # 500 < 1200×0.5=600

    def test_seeded_last_culls_without_new_ticks(self):
        # 兩步 seed 語意坑回歸: Hub last=300 < 峰值 820×0.5 → node 8 秒內**零新 tick** 也剔得掉
        # (舊 seed last=max → 永遠剔不掉)
        st = State(bid_drop_ratio=0.5)
        _seed_from_snapshot(st, [("A", 100.0, 820, 300, False)])
        assert [d[0] for d in st.final_check_all()] == ["A"]

    def test_ask_appeared_unmarks_and_cancels(self):
        # 2026-09-01 對齊: 鎖破 (賣單出現) → unmark + 觸發退訂/撤單 callback (standalone 同款)
        st = State(bid_drop_ratio=0.5)
        _seed_from_snapshot(st, [("A", 100.0, 800, 800, False)])
        calls = []
        on_book = _mk(st, {"A": 100.0}, unsub_calls=calls)
        on_book("A", [{"price": 100.0, "size": 500}], [{"price": 100.5, "size": 3}])
        assert not st.is_marked("A") and calls == ["A"]

    def test_bid_below_limit_unmarks(self):
        # 2026-09-01 對齊: 委買一跌下漲停 → unmark (舊輕量 handler 只會不更新峰值、不剔)
        st = State(bid_drop_ratio=0.5)
        _seed_from_snapshot(st, [("A", 100.0, 800, 800, False)])
        calls = []
        on_book = _mk(st, {"A": 100.0}, unsub_calls=calls)
        on_book("A", *_book(99.0, 500))
        assert not st.is_marked("A") and calls == ["A"]

    def test_market_row_price0_guarded_after_open(self):
        # 開盤市價列 price=0 + isContinuous=True → mark_opened → return,不誤 unmark (6243 型防護)
        st = State(bid_drop_ratio=0.5)
        _seed_from_snapshot(st, [("A", 100.0, 800, 800, False)])
        on_book = _mk(st, {"A": 100.0})
        on_book("A", [{"price": 0.0, "size": 5}], [], True)
        assert st.is_marked("A")

    def test_first_tick_priority_preserved(self):
        # 快照的開盤即鎖 → seed 後 get_marked_prioritized 仍把它排前面 (預掛送單優先)
        st = State(bid_drop_ratio=0.5)
        _seed_from_snapshot(st, [("6933", 260.0, 100, 100, False), ("8105", 16.65, 500, 500, True)])
        order = st.get_marked_prioritized()
        assert order.index("8105") < order.index("6933")   # 開盤即鎖 8105 優先
