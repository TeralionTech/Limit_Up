"""trader.on_book — 委買一三用途拆分 (2026-08-04 定案 v2):
顯示 = 原始第一列 / 量減半規則 = 一般股**只看市價列**、處置股看限價列 /
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
    """首筆 books + N 筆成交 → TRACKING、暖機完成。"""
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


class TestQtyRuleMarketOnly:
    """一般股: 規則基準 = 市價列 (限價列波動不觸發撤單)。"""

    def test_baseline_is_market_row_only(self):
        t = _mk_trader()
        t.on_book("2330", MKT_QUEUED, [])
        assert t.holdings["2330"].prev_bid1_size == 1561   # 只算市價列,不含限價 800

    def test_limit_row_halving_does_not_pull(self):
        # 使用者的情境: 市價單排得好好的,限價列突然減半 → 不准撤
        t = _mk_trader()
        _enter_tracking(t)
        h = t.holdings["2330"]
        assert h.status == Holding.TRACKING
        t.on_book("2330", [{"price": 0.0, "size": 1561}, {"price": 121.5, "size": 350}], [])
        assert h.status == Holding.TRACKING                # 市價 1561→1561 沒動 → 不撤
        assert h.prev_bid1_size == 1561

    def test_market_queue_eaten_triggers_pull(self):
        # 市價列被吃光 (1561 → 0) = 積極買盤流失 → 撤單
        t = _mk_trader()
        _enter_tracking(t)
        h = t.holdings["2330"]
        t.on_book("2330", LIMIT_ONLY, [])
        assert h.status == Holding.PULLED
        assert h.pulled_reason.startswith("qty_drop_half (1561 → 0")

    def test_market_queue_halved_triggers_pull(self):
        t = _mk_trader()
        _enter_tracking(t)
        h = t.holdings["2330"]
        t.on_book("2330", [{"price": 0.0, "size": 700}, {"price": 121.5, "size": 800}], [])
        assert h.status == Holding.PULLED                  # 700 < 1561×0.5

    def test_warmup_prevents_early_pull(self):
        # 未滿 3 筆成交 → 同樣盤面不撤 (開盤瞬間基準未穩)
        t = _mk_trader()
        _enter_tracking(t, trades=2)
        h = t.holdings["2330"]
        t.on_book("2330", LIMIT_ONLY, [])
        assert h.status == Holding.TRACKING
        assert h.prev_bid1_size == 0                       # 基準已更新 (市價列消失)

    def test_no_market_queue_rule_inert(self):
        # 一般股全程沒有市價排隊 → 基準恆 0 → 規則不啟動 (定案接受: 無此保護)
        t = _mk_trader()
        _enter_tracking(t, first_books=LIMIT_ONLY)
        h = t.holdings["2330"]
        t.on_book("2330", [{"price": 121.5, "size": 100}], [])   # 限價 800→100 大減
        assert h.status == Holding.TRACKING                       # 仍不觸發


class TestQtyRuleDisposition:
    """處置股: 禁下市價單 → 永遠沒有市價列 → 基準 = 限價列。"""

    def test_disposition_uses_limit_row(self):
        t = _mk_trader(dispositions={"2330": True})
        _enter_tracking(t, first_books=LIMIT_ONLY)
        h = t.holdings["2330"]
        assert h.prev_bid1_size == 800                     # 限價列
        t.on_book("2330", [{"price": 121.5, "size": 350}], [])
        assert h.status == Holding.PULLED                  # 350 < 800×0.5
        assert h.pulled_reason.startswith("qty_drop_half (800 → 350")


class TestFirstBooksAskCheck:
    def test_market_sell_row_still_discards(self):
        # 首筆五檔有委賣 (price=0 市價賣列也算真賣單) → 淘汰
        t = _mk_trader()
        t.on_book("2330", MKT_QUEUED, [{"price": 0.0, "size": 30}])
        assert t.holdings["2330"].status == Holding.DISCARDED_FIRST
