"""trader.on_book — 委買一三用途拆分 (2026-08-04 定案):
顯示 = 原始第一列 / 量減半規則 = 市價列+限價列合計 / 定價 = 第一個 price>0 檔位。
"""
from types import SimpleNamespace

from trader import Trader, Holding


def _cfg():
    return SimpleNamespace(first_trade_min_lots=10,
                           bid_decline_sample_sec=60,
                           bid_decline_minutes=5)


def _mk_trader(symbol="2330", limit_up=121.5):
    return Trader(watchlist=[symbol], limit_ups={symbol: limit_up}, cfg=_cfg())


MKT_QUEUED = [{"price": 0.0, "size": 1561}, {"price": 121.5, "size": 800}]  # 市價排隊中
LIMIT_ONLY = [{"price": 121.5, "size": 800}]                                # 無市價列


class TestDisplayRaw:
    def test_display_shows_raw_first_row_with_market_orders(self):
        t = _mk_trader()
        t.on_book("2330", MKT_QUEUED, [])
        h = t.holdings["2330"]
        # 顯示 = 原始第一列: 市價排隊時 0 × 1561 (忠實盤面,不跳列)
        assert h.last_bid1_price == 0.0
        assert h.last_bid1_size == 1561
        assert h.first_books["bid1_price"] == 0.0
        assert h.first_books["bid1_size"] == 1561

    def test_display_without_market_row(self):
        t = _mk_trader()
        t.on_book("2330", LIMIT_ONLY, [])
        h = t.holdings["2330"]
        assert h.last_bid1_price == 121.5
        assert h.last_bid1_size == 800


class TestQtyRuleUsesTotal:
    def _enter_tracking(self, t, symbol="2330", trades=3):
        """首筆 books (市價排隊盤面) + N 筆成交 → TRACKING、暖機完成。"""
        t.on_book(symbol, MKT_QUEUED, [])                   # first_books, prev=2361
        for _ in range(trades):
            t.on_trade(symbol, {"price": 121.5, "size": 50})   # 50 張 >= 門檻 10

    def test_rule_baseline_is_market_plus_limit(self):
        t = _mk_trader()
        t.on_book("2330", MKT_QUEUED, [])
        assert t.holdings["2330"].prev_bid1_size == 2361    # 1561 + 800 合計

    def test_no_double_count_without_market_row(self):
        t = _mk_trader()
        t.on_book("2330", LIMIT_ONLY, [])
        assert t.holdings["2330"].prev_bid1_size == 800     # 合計 == 限價列 (不重複計)

    def test_market_queue_drain_triggers_pull(self):
        # 鑑別案例: 合計 2361 → 市價列消失、限價仍 800 → 合計 800 < 1180.5 → 撤單。
        # 只看限價列的話 800→800 不會觸發 — 證明規則用的是合計。
        t = _mk_trader()
        self._enter_tracking(t)
        h = t.holdings["2330"]
        assert h.status == Holding.TRACKING
        t.on_book("2330", LIMIT_ONLY, [])
        assert h.status == Holding.PULLED
        assert h.pulled_reason.startswith("qty_drop_half (2361 → 800")

    def test_warmup_prevents_early_pull(self):
        # 未滿 3 筆成交 → 同樣盤面不撤 (開盤瞬間基準未穩)
        t = _mk_trader()
        self._enter_tracking(t, trades=2)
        h = t.holdings["2330"]
        t.on_book("2330", LIMIT_ONLY, [])
        assert h.status == Holding.TRACKING
        assert h.prev_bid1_size == 800                      # 基準已更新

    def test_stable_total_no_pull(self):
        t = _mk_trader()
        self._enter_tracking(t)
        h = t.holdings["2330"]
        # 市價列變小但限價列補上 → 合計 2361 → 1600,沒掉一半 → 不撤
        t.on_book("2330", [{"price": 0.0, "size": 800}, {"price": 121.5, "size": 800}], [])
        assert h.status == Holding.TRACKING
        assert h.prev_bid1_size == 1600


class TestFirstBooksAskCheck:
    def test_market_sell_row_still_discards(self):
        # 首筆五檔有委賣 (price=0 市價賣列也算真賣單) → 淘汰
        t = _mk_trader()
        t.on_book("2330", MKT_QUEUED, [{"price": 0.0, "size": 30}])
        assert t.holdings["2330"].status == Holding.DISCARDED_FIRST
