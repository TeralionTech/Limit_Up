"""撤單/出場邏輯正確性驗證 (2026-08-04 建立;2026-08-12 出場規則改版) — 六組路徑:

A. 支撐隊伍消失 → 出場 (trader.on_book 端到端;取代舊「委賣出現」訊號)
   一般股 = 市價買隊伍曾出現後歸零;處置股 = 漲停價買單消失
B. _exit_worker 內部流程 (撤剩餘→賣持倉、失敗旗標、跌停價限價賣)
C. 賣出成交記帳
D. 撤單路徑 (量減半→撤、13:23 全撤、撤失敗兜底)
E. close_all 緊急全平 (含未 armed 回歸;查無跌停價 → 市價賣兜底)
F. 隔日賣出場 (超賣保護、skip;跌停價限價賣,查無 → 委買一價公式兜底)

thread 類路徑用輪詢 (≤2s) 等背景結果;確定性案例直呼 worker。
"""
import time
from types import SimpleNamespace

from test_session_money import FakeBroker, make_session, _fill
from trader import Trader, Holding


def _cfg():
    return SimpleNamespace(first_trade_min_lots=10,
                           bid_decline_sample_sec=60,
                           bid_decline_minutes=5)


def _wait(cond, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(0.01)
    return cond()


def _sells(broker):
    return [c for c in broker.placed if c[0] in ("market_sell", "limit_sell")]


LIMIT = 121.5
DOWN = 99.5                                          # 跌停價 (出場限價賣用)
MKT_PRESENT = [{"price": 0.0, "size": 800}, {"price": LIMIT, "size": 500}]  # 市價隊伍在
MKT_GONE = [{"price": LIMIT, "size": 500}]           # 市價隊伍沒有了 (限價列還在)
ASK_LIMIT = [{"price": 122.0, "size": 5}]            # 委賣限價列 (舊訊號,已不觸發出場)


def _session_with_position(lots=2, disposition=False, with_down=True):
    """預掛 2330 (每檔預算 300k ÷ 121.5k = target 2 張) 並成交 lots 張。"""
    s = make_session(per_symbol=300_000)
    if disposition:
        s.set_dispositions({"2330": True})
    if with_down:
        s.set_limit_downs({"2330": DOWN})
    s.place_pre_orders(["2330"], {"2330": LIMIT})
    if lots:
        s._on_fill(_fill(s.trades["2330"].order_no, "2330", lots, LIMIT))
    return s


# ═══ A. 支撐隊伍消失 → 出場 (trader.on_book 端到端) ═══════════════

class TestSupportGoneExit:
    def _trader(self, s, disp=False):
        return Trader(watchlist=["2330"], limit_ups={"2330": LIMIT},
                      cfg=_cfg(), session=s,
                      dispositions={"2330": True} if disp else None)

    def test_market_queue_gone_triggers_exit(self):
        # 一般股: 市價買隊伍曾出現 → 歸零 → 跌停價限價賣全部
        s = _session_with_position(lots=2)
        t = self._trader(s)
        t.on_book("2330", MKT_PRESENT, [])            # 支撐出現 (arm)
        t.on_book("2330", MKT_GONE, [])               # 市價隊伍沒有了!
        assert _wait(lambda: _sells(s.broker)), "市價隊伍消失後 2 秒內沒有賣單"
        assert _sells(s.broker) == [("limit_sell", "2330", DOWN, 2)]
        assert s.trades["2330"].exited is True

    def test_ask_appearing_alone_does_not_exit(self):
        # ⚠ 人造盤面 (盤中不可能: 市價買在排隊時賣單會立刻撮合,掛不上委賣側) —
        # 這是**程式碼回歸保護**,不是市場情境: 證明「ask 觸發出場」已移除,
        # 誰把 asks_any → exit 加回去這裡就會紅。真實時序中市價列歸零必先於委賣掛出,
        # 新訊號嚴格早於舊訊號。
        s = _session_with_position(lots=2)
        t = self._trader(s)
        t.on_book("2330", MKT_PRESENT, [])
        t.on_book("2330", MKT_PRESENT, ASK_LIMIT)     # 資料層強塞委賣,市價隊伍仍在
        time.sleep(0.3)
        assert _sells(s.broker) == []

    def test_never_armed_no_exit(self):
        # 「沒有了」= 曾經有 — 從頭就沒有市價隊伍,不觸發 (開盤隊伍未成形防誤觸)
        s = _session_with_position(lots=2)
        t = self._trader(s)
        t.on_book("2330", MKT_GONE, [])               # 首筆就沒有市價列
        t.on_book("2330", MKT_GONE, [])
        time.sleep(0.3)
        assert _sells(s.broker) == []

    def test_disposition_limit_up_bid_gone(self):
        # 處置股 (沒有市價列): 漲停價買單消失 → 出場
        s = _session_with_position(lots=2, disposition=True)
        t = self._trader(s, disp=True)
        t.on_book("2330", [{"price": LIMIT, "size": 300}], [])     # 漲停價買單在 (arm)
        t.on_book("2330", [{"price": 120.5, "size": 300}], [])     # 跌下漲停!
        assert _wait(lambda: _sells(s.broker))
        assert _sells(s.broker) == [("limit_sell", "2330", DOWN, 2)]

    def test_no_exposure_no_exit(self):
        # 沒下過單的檔 → has_exposure False → 不觸發、不下賣單
        s = make_session()
        t = self._trader(s)
        t.on_book("2330", MKT_PRESENT, [])
        t.on_book("2330", MKT_GONE, [])
        time.sleep(0.3)
        assert _sells(s.broker) == []

    def test_no_duplicate_sell_after_exit(self):
        s = _session_with_position(lots=2)
        t = self._trader(s)
        t.on_book("2330", MKT_PRESENT, [])
        t.on_book("2330", MKT_GONE, [])
        assert _wait(lambda: _sells(s.broker))
        t.on_book("2330", MKT_PRESENT, [])            # 隊伍回來又消失
        t.on_book("2330", MKT_GONE, [])
        time.sleep(0.3)
        assert len(_sells(s.broker)) == 1             # exited 去重,不重複賣


# ═══ B. _exit_worker 內部流程 (直呼,確定性) ═══════════════════════

class TestExitWorker:
    def test_cancel_remainder_then_sell_filled(self):
        # 部分成交 1/2: 先撤剩餘 (釋放預算) → 再跌停價限價賣已成交 1 張
        s = make_session(per_symbol=300_000)          # target 2 張,成交 1 → 剩餘 pending
        s.set_limit_downs({"2330": DOWN})
        s.place_pre_orders(["2330"], {"2330": LIMIT})
        s._on_fill(_fill(s.trades["2330"].order_no, "2330", 1, LIMIT))
        s._exit_worker("2330", "mkt_queue_gone")
        kinds = [c[0] for c in s.broker.calls]
        assert kinds.index("cancel") < kinds.index("limit_sell")
        assert ("limit_sell", "2330", DOWN, 1) in s.broker.placed
        assert s.trades["2330"].exited is True
        assert s.budget_used == 121_500               # 只剩已成交 1 張的消耗

    def test_sell_failure_sets_flag_and_allows_retrigger(self):
        s = _session_with_position(lots=2)
        st = s.trades["2330"]
        orig = s.broker.place_limit_sell

        def _fail(symbol, price, lots, reason=""):
            raise RuntimeError("rejected")
        s.broker.place_limit_sell = _fail
        s._exit_worker("2330", "mkt_queue_gone")
        assert st.exited is False                     # 未賣成 → 下個 tick 可再觸發
        assert st.sell_failed is True
        # broker 恢復 → 再觸發 → 賣成、旗標解除
        s.broker.place_limit_sell = orig
        s._exit_worker("2330", "mkt_queue_gone")
        assert st.exited is True
        assert st.sell_failed is False
        assert ("limit_sell", "2330", DOWN, 2) in s.broker.placed

    def test_cancel_failure_does_not_block_sell(self):
        # 撤單 broker 拋例外 → 賣出不能被擋住 (持倉照賣)
        s = make_session()
        s.set_limit_downs({"2330": DOWN})
        s.place_pre_orders(["2330"], {"2330": LIMIT})
        s._on_fill(_fill(s.trades["2330"].order_no, "2330", 1, LIMIT))

        def _cancel_fail(order_no, symbol, reason=""):
            raise RuntimeError("cancel timeout")
        s.broker.cancel = _cancel_fail
        s._exit_worker("2330", "mkt_queue_gone")
        assert ("limit_sell", "2330", DOWN, 1) in s.broker.placed
        assert s.trades["2330"].exited is True

    def test_disposition_sells_at_limit_down_too(self):
        # 處置股: 跌停價**限價**賣完全合法 (處置股只是不能市價) → 與一般股同路徑
        s = _session_with_position(lots=2, disposition=True)
        s._exit_worker("2330", "limit_up_bid_gone")
        assert ("limit_sell", "2330", DOWN, 2) in s.broker.placed
        assert not [c for c in s.broker.placed if c[0] == "market_sell"]

    def test_disposition_without_limit_down_needs_manual(self):
        # 處置股查無跌停價 → 不可市價兜底 → 不下單 + sell_failed 需人工
        s = _session_with_position(lots=2, disposition=True, with_down=False)
        s._exit_worker("2330", "limit_up_bid_gone")
        assert _sells(s.broker) == []
        assert s.trades["2330"].sell_failed is True
        assert s.trades["2330"].exited is False       # 跌停價補上後可再觸發

    def test_normal_without_limit_down_falls_back_to_market(self):
        # 一般股查無跌停價 → 市價賣兜底 (盤中合法),不會賣不掉
        s = _session_with_position(lots=2, with_down=False)
        s._exit_worker("2330", "mkt_queue_gone")
        assert ("market_sell", "2330", None, 2) in s.broker.placed
        assert s.trades["2330"].exited is True


# ═══ C. 賣出成交記帳 ═══════════════════════════════════════════════

class TestSellFillAccounting:
    def test_sell_fill_reduces_position_keeps_budget(self):
        s = _session_with_position(lots=2)
        s._exit_worker("2330", "ask_appeared")
        sell_no = f"O{s.broker._n}"                   # 最後一張單 = 賣單
        s._on_fill(_fill(sell_no, "2330", 2, 121.5, action="sell", filled_no="F9"))
        assert s.trades["2330"].filled_lots == 0      # 部位歸零
        assert s.budget_used == 243_000               # 2 張買進成本;賣出不退 (保守日預算,設計如此)


# ═══ D. 撤單路徑 ═══════════════════════════════════════════════════

class TestCancelPaths:
    def test_qty_drop_pull_cancels_pending(self):
        # 端到端: TRACKING + 市價列減半 → _pull → async 撤單到 broker
        s = make_session()
        s.place_pre_orders(["2330"], {"2330": 121.5})
        s.trades["2330"].first_trade_fired = True     # 擋掉市價追,單測撤單路徑
        t = Trader(watchlist=["2330"], limit_ups={"2330": 121.5},
                   cfg=_cfg(), session=s)
        t.on_book("2330", [{"price": 0.0, "size": 1561}, {"price": 121.5, "size": 800}], [])
        for _ in range(3):
            t.on_trade("2330", {"price": 121.5, "size": 50})
        t.on_book("2330", [{"price": 0.0, "size": 700}, {"price": 121.5, "size": 800}], [])
        h = t.holdings["2330"]
        assert h.status == Holding.PULLED
        assert _wait(lambda: s.broker.cancelled), "撤單 2 秒內沒到 broker"
        assert s.trades["2330"].order_status == "cancelled"
        assert s.trades["2330"].stopped_reason.startswith("qty_drop_half")
        assert s.budget_used == 0                     # 未成交保留全釋放

    def test_cancel_all_pending_only_touches_pending(self):
        s = make_session(total=10_000_000)
        s.place_pre_orders(["1111", "2222", "3333"],
                           {"1111": 100.0, "2222": 100.0, "3333": 100.0})
        s._on_fill(_fill(s.trades["2222"].order_no, "2222", 2, 100.0))   # 2222 全成 done
        s._on_order({"order_no": s.trades["3333"].order_no, "symbol": "3333",
                     "error_message": "rejected"})                        # 3333 拒單
        s.cancel_all_pending("cancel_pending_time")
        cancelled_orders = [c[0] for c in s.broker.cancelled]
        assert len(cancelled_orders) == 1             # 只撤 1111 (唯一 pending)
        assert s.trades["2222"].filled_lots == 2      # 持倉不動

    def test_cancel_failure_recovered_by_cancel_all(self):
        # 單檔撤失敗 → st 留 pending → 13:23 cancel_all_pending 兜底補撤
        s = make_session()
        s.place_pre_orders(["2330"], {"2330": 121.5})
        orig = s.broker.cancel

        def _fail(order_no, symbol, reason=""):
            raise RuntimeError("timeout")
        s.broker.cancel = _fail
        s.cancel_symbol_orders("2330", "qty_drop_half")
        assert s.trades["2330"].order_status == "pending"    # 沒撤成,狀態保留
        s.broker.cancel = orig
        s.cancel_all_pending("cancel_pending_time")           # 兜底
        assert s.trades["2330"].order_status == "cancelled"
        assert s.budget_used == 0


# ═══ E. close_all 緊急全平 ═════════════════════════════════════════

class TestCloseAll:
    def _setup(self):
        s = make_session(total=10_000_000)
        s.place_pre_orders(["1111", "2222", "3333"],
                           {"1111": 100.0, "2222": 100.0, "3333": 100.0})
        s._on_fill(_fill(s.trades["1111"].order_no, "1111", 2, 100.0))   # 全成持倉
        s._on_fill(_fill(s.trades["2222"].order_no, "2222", 1, 100.0))   # 部分成交
        return s                                                          # 3333 純 pending

    def test_close_all_cancels_and_sells_everything(self):
        s = self._setup()
        sold = s.close_all()
        assert sold == 2                              # 1111 + 2222 (3333 無持倉)
        sells = _sells(s.broker)
        assert ("market_sell", "1111", None, 2) in sells
        assert ("market_sell", "2222", None, 1) in sells
        assert len(s.broker.cancelled) == 2           # 2222 剩餘 + 3333 (1111 已 done 無單)
        assert s.trades["1111"].exited and s.trades["2222"].exited

    def test_close_all_works_when_disarmed(self):
        # 關 kill switch 後緊急全平必須照樣賣得掉 (_can_manage 修正回歸)
        s = self._setup()
        s.set_armed(False)
        sold = s.close_all()
        assert sold == 2


# ═══ F. 隔日賣出場 ═════════════════════════════════════════════════

class _InvBroker(FakeBroker):
    def __init__(self, inventories=None):
        super().__init__()
        self.inventories = inventories or []

    def get_inventories(self):
        return self.inventories


def _overnight_session(list_lots=5, held_lots=2, bid1=50.0, ask1=0.0,
                       skip=False, reconciled=True):
    s = make_session()
    s.broker = _InvBroker([{"symbol": "9999", "lots": held_lots}] if held_lots else [])
    s.load_overnight([{"symbol": "9999", "lots": list_lots, "avg_cost": 10.0}])
    o = s.overnight["9999"]
    o["reconciled"] = reconciled
    o["skip"] = skip
    s.update_overnight_book("9999", bid1, ask1)
    return s


class TestOvernightExit:
    def test_oversell_guard_sells_min_of_list_and_inventory(self):
        # 清單 5 張 / 庫存 2 張 → 只賣 2 (2026-08-03 超賣保護)
        s = _overnight_session(list_lots=5, held_lots=2, bid1=50.0)
        s.on_overnight_first_trade("9999")
        assert _wait(lambda: _sells(s.broker)), "隔日賣 2 秒內沒下單"
        assert _sells(s.broker) == [("limit_sell", "9999", 50.0, 2)]

    def test_skip_blocks_sell(self):
        s = _overnight_session(skip=True)
        s.on_overnight_first_trade("9999")
        time.sleep(0.3)
        assert _sells(s.broker) == []

    def test_unreconciled_blocks_sell(self):
        s = _overnight_session(reconciled=False)
        s.on_overnight_first_trade("9999")
        time.sleep(0.3)
        assert _sells(s.broker) == []

    def test_zero_inventory_no_sell_and_retriggerable(self):
        s = _overnight_session(list_lots=5, held_lots=0)
        o = s.overnight["9999"]
        o["sell_placed"] = True                       # 模擬觸發端已佔位
        s._overnight_sell_worker("9999")              # 直呼 (確定性)
        assert _sells(s.broker) == []
        assert o["sell_placed"] is False              # 重置 → 下一筆成交可再試

    def test_wide_spread_sells_one_tick_below_ask(self):
        # (兜底路徑) 查無跌停價 → 退回舊委買一價公式: 價差 ≥5 tick → 賣一往下一檔
        s = _overnight_session(list_lots=5, held_lots=5, bid1=50.0, ask1=50.9)
        o = s.overnight["9999"]
        o["sell_placed"] = True
        s._overnight_sell_worker("9999")
        assert _sells(s.broker) == [("limit_sell", "9999", 50.8, 5)]

    def test_overnight_sells_at_limit_down_when_available(self):
        # 2026-08-12 定案: 隔日賣也改**跌停價限價賣** (不再管委買/委賣價差)
        s = _overnight_session(list_lots=5, held_lots=5, bid1=50.0, ask1=50.9)
        s.set_limit_downs({"9999": 41.5})
        o = s.overnight["9999"]
        o["sell_placed"] = True
        s._overnight_sell_worker("9999")
        assert _sells(s.broker) == [("limit_sell", "9999", 41.5, 5)]   # 跌停價,非 50.8
