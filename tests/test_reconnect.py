"""交易 WS 斷線 → 自動重連 → 權威對帳補收 (2026-08-05 3587 事故的修復)。

補收規則 (2026-08-06 定案): 僅斷線補收情境、僅策略單、以券商回傳覆寫非累加;
_on_fill 的「單筆委託封頂」保證補收與晚到/重複回報不會雙算。
"""
import time

from test_session_money import FakeBroker, make_session, _fill
from broker import RealOrderClient


class MapBroker(FakeBroker):
    """get_filled_map 可設定 — 模擬券商權威成交量。"""

    def __init__(self):
        super().__init__()
        self.filled_map = {}

    def get_filled_map(self):
        return dict(self.filled_map)


def _mk_session_with_pre_order():
    """預掛 2330 (limit 100, per-symbol 200k → target 2 張),order_no=O1。"""
    s = make_session()
    s.broker = MapBroker()
    s.place_pre_orders(["2330"], {"2330": 100.0})
    return s


class TestReconcile:
    def test_recover_missed_buy_fill(self):
        # 斷線期間 O1 全成 2 張、回報遺失 → 補收後部位/預算/委託表全正確
        s = _mk_session_with_pre_order()
        s.broker.filled_map = {"O1": 2}
        s.reconcile_orders()
        st = s.trades["2330"]
        assert st.filled_lots == 2
        assert st.order_status == "done" and st.order_no == ""
        assert st.budget_reserved == 0            # 保留全轉消耗
        assert s.budget_used == 200_000
        assert st.avg_price == 100.0              # 無回報成交價 → 漲停價近似
        row = s.order_log["O1"]
        assert row["filled_lots"] == 2 and row["status"] == "filled"

    def test_reconcile_is_idempotent(self):
        s = _mk_session_with_pre_order()
        s.broker.filled_map = {"O1": 2}
        s.reconcile_orders()
        s.reconcile_orders()                      # 再跑一次 → 覆寫非累加,無變化
        assert s.trades["2330"].filled_lots == 2
        assert s.budget_used == 200_000

    def test_late_report_after_reconcile_no_double_count(self):
        # 補收已入帳後,真的回報晚到 → 單筆委託封頂 → 不雙算
        s = _mk_session_with_pre_order()
        s.broker.filled_map = {"O1": 2}
        s.reconcile_orders()
        s._on_fill(_fill("O1", "2330", 2, 100.0))
        assert s.trades["2330"].filled_lots == 2  # 仍是 2,不是 4
        assert s.budget_used == 200_000

    def test_recover_partial_then_late_report_for_rest(self):
        # 斷線遺失 1 張 → 補收 1;之後另 1 張的回報正常到 → 合計 2,不重複
        s = _mk_session_with_pre_order()
        s.broker.filled_map = {"O1": 1}
        s.reconcile_orders()
        st = s.trades["2330"]
        assert st.filled_lots == 1
        assert s.budget_used == 200_000           # 1 張消耗 + 1 張仍保留
        s._on_fill(_fill("O1", "2330", 1, 100.0, filled_no="F2"))
        assert st.filled_lots == 2
        assert st.budget_reserved == 0

    def test_recover_missed_sell_fill(self):
        # 出場賣單成交、回報遺失 → 補收後部位歸零
        s = _mk_session_with_pre_order()
        s._on_fill(_fill("O1", "2330", 2, 100.0))          # 買進正常
        st = s.trades["2330"]
        assert s._sell_position("2330", st, 2, "test", max_tries=1) is True   # O2
        s.broker.filled_map = {"O1": 2, "O2": 2}           # 賣單回報遺失
        s.reconcile_orders()
        assert st.filled_lots == 0

    def test_no_diff_is_noop(self):
        s = _mk_session_with_pre_order()
        s._on_fill(_fill("O1", "2330", 2, 100.0))
        s.broker.filled_map = {"O1": 2}
        before = s.budget_used
        s.reconcile_orders()
        assert s.trades["2330"].filled_lots == 2
        assert s.budget_used == before

    def test_reconnected_hook_runs_reconcile(self):
        s = _mk_session_with_pre_order()
        s.broker.filled_map = {"O1": 2}
        s._on_broker_reconnected()                # broker relogin thread 的入口
        assert s.trades["2330"].filled_lots == 2


class TestOnFillPerOrderCap:
    def test_over_report_capped_at_order_lots(self):
        # 單一委託回報總量超過委託量 (異常/重複) → 只記到封頂
        s = _mk_session_with_pre_order()
        s._on_fill(_fill("O1", "2330", 3, 100.0))          # 委託只有 2 張
        assert s.trades["2330"].filled_lots == 2
        s._on_fill(_fill("O1", "2330", 1, 100.0, filled_no="F3"))   # 已滿再來
        assert s.trades["2330"].filled_lots == 2


class TestBrokerReloginLifecycle:
    def test_disconnect_stops_relogin_timer(self, tmp_path):
        import threading
        c = RealOrderClient(tmp_path / "orders.csv")
        t = threading.Timer(60, lambda: None)
        t.daemon = True
        t.start()
        c._relogin_timer = t
        c.disconnect()
        assert c._stopping is True
        time.sleep(0.05)
        assert not t.is_alive()                   # timer 已被 cancel
        c.close()

    def test_no_retry_scheduled_after_manual_disconnect(self, tmp_path):
        c = RealOrderClient(tmp_path / "orders2.csv")
        c._stopping = True
        c._schedule_relogin_retry()
        assert c._relogin_timer is None           # 手動斷線後不再排重試
        c.close()
