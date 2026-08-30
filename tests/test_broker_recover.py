"""broker.RealOrderClient._recover_order_no — 缺書號反查認領 (place 成功但回傳無書號的防禦路徑)。

2026-08-28 管線化: 同標同量可有多筆市價買在飛,打破舊「每檔僅一張活躍委託」假設 → 反查會撞到多筆
一樣的候選。修法: 每筆下單成功的書號記進 _claimed_order_nos,反查時排除已認領的 → 仍能唯一認出
剛送、還沒書號的那筆 (免落 UNKNOWN 變隱形裸單)。這裡用假 sdk 直接驗反查邏輯 (不 login)。
"""
from types import SimpleNamespace

from broker import RealOrderClient


def _order(no, symbol="2330", buy=True, qty=2000, user_def="hitlimit"):
    return SimpleNamespace(order_no=no, stock_no=symbol,
                           buy_sell="Buy" if buy else "Sell",
                           quantity=qty, user_def=user_def)


def _client(orders):
    c = RealOrderClient.__new__(RealOrderClient)     # 不跑 __init__/login
    c._claimed_order_nos = set()
    c.account = object()
    c.sdk = SimpleNamespace(stock=SimpleNamespace(
        get_order_results=lambda account: SimpleNamespace(is_success=True, data=orders)))
    return c


class TestRecoverOrderNoPipelined:
    def test_single_candidate_recovers(self):
        c = _client([_order("A1")])
        assert c._recover_order_no("2330", True, 2) == "A1"

    def test_two_identical_one_claimed_recovers_unclaimed(self):
        # 管線: A1 先前在飛已認領、A2 剛送還沒書號 → 反查排除 A1 → 唯一認 A2 (修法核心)
        c = _client([_order("A1"), _order("A2")])
        c._claimed_order_nos.add("A1")
        assert c._recover_order_no("2330", True, 2) == "A2"

    def test_two_unclaimed_refuses(self):
        # 兩筆都沒認領 (罕見雙缺書號) → 不亂認,回 "" (上層落唯一 UNKNOWN key + 13:23 掃單兜底)
        c = _client([_order("A1"), _order("A2")])
        assert c._recover_order_no("2330", True, 2) == ""

    def test_filtered_by_symbol_qty_side_userdef(self):
        # 不同標的/量/買賣別/非策略單都不算候選 → 只認 A5
        c = _client([_order("A1", symbol="9999"), _order("A2", qty=3000),
                     _order("A3", buy=False), _order("A4", user_def="other"), _order("A5")])
        assert c._recover_order_no("2330", True, 2) == "A5"
