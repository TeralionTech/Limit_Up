"""花費表 spending_summary — 各檔實際花費/超額 + 總預算超額 (只看成交)。
超額 = 實際買進現金 (buy_cost_actual,單調) − 目標金額 (target×漲停×1000);沒超過=0。"""
from test_session_money import make_session, _fill


class TestSpendingSummary:
    def test_exact_target_no_over(self):
        s = make_session(total=1_000_000, per_symbol=300_000)
        s.place_pre_orders(["2330"], {"2330": 100.0})       # target = 300k/(100×1000) = 3
        st = s.trades["2330"]
        assert st.target_lots == 3
        s._on_fill(_fill(st.order_no, "2330", 3, 100.0))    # 剛好足額
        sp = s.spending_summary()
        row = next(r for r in sp["symbols"] if r["symbol"] == "2330")
        assert row["spent"] == 300_000 and row["over"] == 0
        assert sp["total_spent"] == 300_000 and sp["total_over"] == 0

    def test_overbuy_two_orders_shows_over(self):
        # 管線多送: 預掛 P (3) + 市價 M1 (3) 都成交 → 超買 3 張
        s = make_session(total=1_000_000, per_symbol=300_000)
        s.place_pre_orders(["2330"], {"2330": 100.0})       # target 3
        st = s.trades["2330"]
        s._on_fill(_fill(st.order_no, "2330", 3, 100.0))
        s._log_order("M1", "2330", "buy", "market_buy", 3, 0)
        s._on_fill(_fill("M1", "2330", 3, 100.0, filled_no="F2"))
        sp = s.spending_summary()
        row = next(r for r in sp["symbols"] if r["symbol"] == "2330")
        assert row["filled_lots"] == 6
        assert row["spent"] == 600_000
        assert row["over"] == 300_000                       # 600k − 目標 300k

    def test_total_over_budget(self):
        s = make_session(total=300_000, per_symbol=300_000)
        s.place_pre_orders(["2330"], {"2330": 100.0})       # target 3
        st = s.trades["2330"]
        s._on_fill(_fill(st.order_no, "2330", 3, 100.0))
        s._log_order("M1", "2330", "buy", "market_buy", 3, 0)
        s._on_fill(_fill("M1", "2330", 3, 100.0, filled_no="F2"))
        sp = s.spending_summary()
        assert sp["total_spent"] == 600_000
        assert sp["total_over"] == 300_000                  # 600k − 300k 預算
        assert sp["budget_breached"] is True

    def test_spent_survives_sell(self):
        # buy_cost_actual 單調: 賣出後「花費」不縮水 (反映實際買了多少錢)
        s = make_session(total=1_000_000, per_symbol=300_000)
        s.place_pre_orders(["2330"], {"2330": 100.0})
        st = s.trades["2330"]
        s._on_fill(_fill(st.order_no, "2330", 3, 100.0))    # 買 3 → 花 300k
        s._log_order("SX", "2330", "sell", "market_sell", 3, 0)
        s._on_fill(_fill("SX", "2330", 3, 90.0, action="sell", filled_no="FS"))  # 賣 3 → filled 0
        sp = s.spending_summary()
        row = next(r for r in sp["symbols"] if r["symbol"] == "2330")
        assert row["spent"] == 300_000 and row["filled_lots"] == 0
