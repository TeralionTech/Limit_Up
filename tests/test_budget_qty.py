"""使用者情境驗證 (2026-08-11): 依金額 10 萬/檔 → 下 2 張 → 撤單/出場張數正確性。

設定: per_symbol_budget=100,000、漲停價 45 元 → _calc_lots = 100k // 45k = **2 張**,
下成**一張委託 2000 股** (不是兩張 1 張的單)。
撤單不帶數量 — 交易所語意 = 撤「該委託的未成交餘量」;
出場賣 filled_lots 全量 (2026-08-12 起 = 跌停價限價賣)。
"""
import time
from types import SimpleNamespace

from test_session_money import make_session, _fill
from trader import Trader

LIMIT = 45.0
DOWN = 36.9                                       # 跌停價 (出場限價賣用)
COST_PER_LOT = 45_000


def _mk():
    s = make_session(per_symbol=100_000)          # 依金額: 每檔 10 萬
    s.set_limit_downs({"1101": DOWN})
    s.place_pre_orders(["1101"], {"1101": LIMIT})
    return s


class TestBudget100kTwoLots:
    def test_100k_budget_places_exactly_2_lots(self):
        # 前提驗證: 10萬 ÷ 4.5萬 = 2 張,單一委託
        s = _mk()
        assert s.trades["1101"].target_lots == 2
        assert s.broker.placed == [("limit_buy", "1101", LIMIT, 2)]
        assert s.budget_used == 2 * COST_PER_LOT              # 保留 9 萬

    def test_cancel_before_fill_releases_both_lots(self):
        # 未成交就撤 → 整單撤 (交易所撤 2 張餘量),9 萬保留全釋放
        s = _mk()
        order_no = s.trades["1101"].order_no
        s.cancel_symbol_orders("1101", "test")
        assert [c[0] for c in s.broker.cancelled] == [order_no]
        assert s.trades["1101"].order_status == "cancelled"
        assert s.budget_used == 0

    def test_cancel_after_1_of_2_filled_then_exit_sells_1(self):
        # 成交 1 張後撤 → 交易所只撤餘量 1 張;帳內留 1 張成本
        # 之後出場 → 賣「已成交的 1 張」,不是 2 張
        s = _mk()
        s._on_fill(_fill(s.trades["1101"].order_no, "1101", 1, LIMIT))
        s.cancel_symbol_orders("1101", "test")
        st = s.trades["1101"]
        assert st.filled_lots == 1
        assert s.budget_used == 1 * COST_PER_LOT
        s._exit_worker("1101", "mkt_queue_gone")
        assert ("limit_sell", "1101", DOWN, 1) in s.broker.placed

    def test_exit_sells_exactly_2_after_full_fill(self):
        # 2 張全成 → 出場 → 跌停價限價賣 2 張 (張數 = filled_lots,不多不少)
        s = _mk()
        s._on_fill(_fill(s.trades["1101"].order_no, "1101", 2, LIMIT))
        s._exit_worker("1101", "mkt_queue_gone")
        sells = [c for c in s.broker.placed if c[0] == "limit_sell"]
        assert sells == [("limit_sell", "1101", DOWN, 2)]

    def test_end_to_end_queue_gone_sells_2(self):
        # 端到端 (2026-08-12 訊號): 市價買隊伍曾出現後歸零 → trader 觸發出場
        # → 背景 thread 跌停價限價賣 2 張
        s = _mk()
        s._on_fill(_fill(s.trades["1101"].order_no, "1101", 2, LIMIT))
        t = Trader(watchlist=["1101"], limit_ups={"1101": LIMIT},
                   cfg=SimpleNamespace(first_trade_min_lots=10,
                                       bid_decline_sample_sec=60,
                                       bid_decline_minutes=5),
                   session=s)
        t.on_book("1101", [{"price": 0.0, "size": 300},
                           {"price": LIMIT, "size": 500}], [])       # 市價隊伍在 (arm)
        t.on_book("1101", [{"price": LIMIT, "size": 500}], [])       # 市價隊伍沒有了!
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and not any(
                c[0] == "limit_sell" for c in s.broker.placed):
            time.sleep(0.01)
        assert ("limit_sell", "1101", DOWN, 2) in s.broker.placed
        assert s.trades["1101"].exited is True

    def test_overbuy_2x_exit_sells_all_4(self):
        # 超買情境 (定案「多下到沒關係」): 預掛 2 張 + 市價追 2 張**都**成交
        # → 部位 4 張 → 出場必須賣 4 張全量,不是 target 的 2 張
        s = _mk()
        pre_no = s.trades["1101"].order_no
        s._first_trade_worker("1101", True)          # 市價優先: 追 2 張、再撤預掛
        chase_no = s.trades["1101"].order_no
        assert ("market_buy", "1101", None, 2) in s.broker.placed    # 差額 = 2 張
        # 撤單與成交在券商端賽跑 — 結果兩張單都成交
        s._on_fill(_fill(pre_no, "1101", 2, LIMIT))
        s._on_fill(_fill(chase_no, "1101", 2, LIMIT, filled_no="F2"))
        st = s.trades["1101"]
        assert st.filled_lots == 4
        s._exit_worker("1101", "mkt_queue_gone")
        assert ("limit_sell", "1101", DOWN, 4) in s.broker.placed
        # 附註: 超買時 budget_used 低估實際花費 (保留只做過一份) — 已接受的近似
        assert s.budget_used == 2 * COST_PER_LOT

    def test_1323_cancel_all_cancels_whole_order(self):
        # 13:23 全撤: 2 張 pending 整單撤、預算歸零、持倉概念不受影響
        s = _mk()
        s.cancel_all_pending("cancel_pending_time")
        assert len(s.broker.cancelled) == 1
        assert s.trades["1101"].order_status == "cancelled"
        assert s.budget_used == 0
