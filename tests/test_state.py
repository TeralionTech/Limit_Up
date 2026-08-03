"""state.py — mark/unmark 狀態機 (冪等、永久淘汰、量減半判斷、預掛優先序)。"""
from state import State


class TestMarkUnmark:
    def test_mark_idempotent(self):
        s = State()
        assert s.mark("2330", 100.0, 50, 100.0) is True
        assert s.mark("2330", 100.0, 60, 100.0) is False   # 重複 mark 回 False
        assert s.is_marked("2330")

    def test_unmark_is_permanent_discard(self):
        s = State()
        s.mark("2330", 100.0, 50, 100.0)
        assert s.unmark_ask_appeared("2330", 100.5, 3) is True
        assert not s.is_marked("2330")
        assert s.is_discarded("2330")
        # 淘汰後不能 re-mark
        assert s.mark("2330", 100.0, 50, 100.0) is False
        assert not s.is_marked("2330")

    def test_unmark_idempotent(self):
        s = State()
        s.mark("2330", 100.0, 50, 100.0)
        assert s.unmark_bid_below_limit("2330", 99.5, 100.0) is True
        assert s.unmark_bid_below_limit("2330", 99.0, 100.0) is False  # 已淘汰,再 unmark 無效

    def test_unmark_unmarked_symbol_noop(self):
        s = State()
        assert s.unmark_ask_appeared("9999", 10.0, 1) is False
        assert not s.is_discarded("9999")


class TestBidDrop:
    def test_final_check_ratio(self):
        s = State(bid_drop_ratio=0.5)
        s.mark("2330", 100.0, 100, 100.0)
        s.update_max_bid("2330", 150)
        s.update_max_bid("2330", 120)         # 不會往下更新 max
        # 74 < 150*0.5 → 該淘汰;75 == 150*0.5 不淘汰 (嚴格小於)
        assert s.check_final_bid_drop("2330", 74) == (True, 150)
        assert s.check_final_bid_drop("2330", 75) == (False, 150)
        assert s.unmark_bid_dropped("2330", 74, 150) is True
        assert s.is_discarded("2330")

    def test_final_check_unmarked_symbol(self):
        s = State()
        assert s.check_final_bid_drop("9999", 10) == (False, 0)


class TestPrioritized:
    def test_first_tick_symbols_come_first(self):
        s = State()
        s.mark("2330", 100.0, 50, 100.0, first_tick=False)
        s.mark("1101", 50.0, 30, 50.0, first_tick=False)
        s.mark("9999", 10.0, 99, 10.0, first_tick=True)
        # 開盤即鎖優先 (搶送單時間+預算),組內按代號
        assert s.get_marked_prioritized() == ["9999", "1101", "2330"]
        assert s.is_first_tick("9999")
        assert not s.is_first_tick("2330")


class TestStats:
    def test_stats_counts_by_reason(self):
        s = State()
        s.mark("A", 10.0, 5, 10.0)
        s.mark("B", 10.0, 5, 10.0)
        s.mark("C", 10.0, 5, 10.0)
        s.unmark_ask_appeared("A", 10.5, 1)
        s.unmark_bid_below_limit("B", 9.9, 10.0)
        st = s.stats()
        assert st["currently_marked"] == 1
        assert st["total_mark_events"] == 3
        assert st["total_unmark_events"] == 2
        assert st["unmark_by_ask_appeared"] == 1
        assert st["unmark_by_bid_below_limit"] == 1
        assert st["unique_symbols_touched"] == 3
