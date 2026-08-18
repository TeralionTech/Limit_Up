"""trader.on_book — 委買一三用途拆分 (2026-08-04 定案 v2;2026-08-19 量減半撤單移除):
顯示 = 原始第一列 / 出場規則 = 一般股**只看市價列**、處置股看限價列 /
定價 = 第一個 price>0 檔位。
"""
from types import SimpleNamespace

from trader import Trader, Holding


def _cfg():
    return SimpleNamespace(first_trade_min_lots=10,
                           bid_decline_sample_sec=60,
                           bid_decline_minutes=5)


def _mk_trader(symbol="2330", limit_up=121.5, dispositions=None):
    return Trader(watchlist=[symbol], limit_ups={symbol: limit_up}, cfg=_cfg(),
                  dispositions=dispositions)


MKT_QUEUED = [{"price": 0.0, "size": 1561}, {"price": 121.5, "size": 800}]  # 市價排隊中
LIMIT_ONLY = [{"price": 121.5, "size": 800}]                                # 無市價列


def _enter_tracking(t, symbol="2330", first_books=MKT_QUEUED, trades=3):
    """首筆 books + N 筆成交 → TRACKING。"""
    t.on_book(symbol, first_books, [])
    for _ in range(trades):
        t.on_trade(symbol, {"price": 121.5, "size": 50})   # 50 張 >= 門檻 10


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


class TestExitRouting:
    """出場訊號 (2026-08-19 定案,唯一動作;量減半撤單已移除): 支撐隊伍消失。
    一般股 = 市價列曾出現後歸零;處置股 (名單分流) = 漲停價買單消失。
    無 session → 只驗 Holding 狀態 (status=PULLED 「出場」+ pulled_reason)。"""

    def test_market_queue_gone_marks_exit(self):
        t = _mk_trader()
        _enter_tracking(t, trades=1)
        h = t.holdings["2330"]
        t.on_book("2330", LIMIT_ONLY, [])                  # 市價列 1561 → 0
        assert h.status == Holding.PULLED
        assert h.pulled_reason == "mkt_queue_gone"

    def test_market_queue_halved_no_action(self):
        # 量減半規則已移除的回歸保護: 1561 → 700 (腰斬) 但隊伍還在 → 不動作
        t = _mk_trader()
        _enter_tracking(t, trades=1)
        h = t.holdings["2330"]
        t.on_book("2330", [{"price": 0.0, "size": 700}, {"price": 121.5, "size": 800}], [])
        assert h.status == Holding.TRACKING
        t.on_book("2330", [{"price": 0.0, "size": 5}, {"price": 121.5, "size": 800}], [])
        assert h.status == Holding.TRACKING                # 剩 5 張仍是「在」

    def test_limit_row_changes_ignored_while_queue_present(self):
        # 一般股只看市價列: 限價列腰斬 / 買牆消失,市價隊伍在就不出場
        t = _mk_trader()
        _enter_tracking(t, trades=1)
        h = t.holdings["2330"]
        t.on_book("2330", [{"price": 0.0, "size": 1561}, {"price": 121.5, "size": 100}], [])
        t.on_book("2330", [{"price": 0.0, "size": 1561}, {"price": 120.5, "size": 100}], [])
        assert h.status == Holding.TRACKING

    def test_disposition_limit_up_bid_gone(self):
        # 處置股 (名單): 沒有市價列 → 漲停價買單消失才出場
        t = _mk_trader(dispositions={"2330": True})
        _enter_tracking(t, first_books=LIMIT_ONLY, trades=1)
        h = t.holdings["2330"]
        t.on_book("2330", [{"price": 121.5, "size": 350}], [])   # 量減少但仍在漲停價
        assert h.status == Holding.TRACKING
        t.on_book("2330", [{"price": 120.5, "size": 350}], [])   # 跌下漲停!
        assert h.status == Holding.PULLED
        assert h.pulled_reason == "limit_up_bid_gone"

    def test_normal_stock_without_market_queue_never_arms(self):
        # 一般股全程沒有市價排隊 → 基準恆 0 → 不 arm、不出場 (定案接受: 無此保護)
        t = _mk_trader()
        _enter_tracking(t, first_books=LIMIT_ONLY, trades=1)
        h = t.holdings["2330"]
        t.on_book("2330", [{"price": 120.5, "size": 100}], [])   # 連漲停買牆都沒了
        assert h.status == Holding.TRACKING

    def test_first_tick_without_support_does_not_arm(self):
        # 「沒有了」= 曾經有: 首 tick 就沒市價列 → 之後市價列出現又消失才出場
        t = _mk_trader()
        _enter_tracking(t, first_books=LIMIT_ONLY, trades=1)
        h = t.holdings["2330"]
        t.on_book("2330", LIMIT_ONLY, [])
        assert h.status == Holding.TRACKING
        t.on_book("2330", MKT_QUEUED, [])                        # 隊伍出現 (arm)
        t.on_book("2330", LIMIT_ONLY, [])                        # 隊伍歸零 → 出場
        assert h.status == Holding.PULLED

    def test_exit_marks_only_once_and_stays(self):
        # 出場後隊伍回來又消失 → 狀態維持 PULLED、原因不變
        t = _mk_trader()
        _enter_tracking(t, trades=1)
        h = t.holdings["2330"]
        t.on_book("2330", LIMIT_ONLY, [])
        t.on_book("2330", MKT_QUEUED, [])
        t.on_book("2330", LIMIT_ONLY, [])
        assert h.status == Holding.PULLED
        assert h.pulled_reason == "mkt_queue_gone"


class TestFirstBooksAskCheck:
    def test_market_sell_row_still_discards(self):
        # 首筆五檔有委賣 (price=0 市價賣列也算真賣單) → 淘汰
        t = _mk_trader()
        t.on_book("2330", MKT_QUEUED, [{"price": 0.0, "size": 30}])
        assert t.holdings["2330"].status == Holding.DISCARDED_FIRST
