"""委託回報 last_time — 富邦「最後異動時間」(委託被接受/異動的富邦時戳,毫秒) 正確記進 order_log。
2026-09-02: broker._handle_order 補抓 last_time → session._on_order 記進 order_log,
補齊「送單→委託被接受→成交」時間鏈裡『被接受』那一環。"""
from test_session_money import make_session


def _place(s, order_no, symbol="2881"):
    """建一筆 order_log entry (模擬送單後)。"""
    s._log_order(order_no, symbol, "buy", "pre_limit", 5, 66.0)


class TestOrderReportLastTime:
    def test_accept_report_records_last_time(self):
        # 新單接受主動回報 (無 error) 帶 last_time → 記進 order_log + get_orders 看得到
        s = make_session()
        _place(s, "O1")
        s._on_order({"order_no": "O1", "symbol": "2881", "status": "8",
                     "filled_qty": 0, "error_message": "", "last_time": "10:44:05.796"})
        row = s.order_log["O1"]
        assert row["last_time"] == "10:44:05.796"
        assert row["status"] == "pending"            # 未拒單,狀態不變
        assert any(o["order_no"] == "O1" and o["last_time"] == "10:44:05.796"
                   for o in s.get_orders())          # 前端委託表也帶

    def test_reject_report_still_records_last_time(self):
        # 拒單回報: 記 last_time + 標 rejected (原邏輯不變)
        s = make_session()
        _place(s, "O2")
        s._on_order({"order_no": "O2", "symbol": "2881", "status": "4", "filled_qty": 0,
                     "error_message": "集合競價時段不可輸入市價", "last_time": "09:00:01.123"})
        row = s.order_log["O2"]
        assert row["last_time"] == "09:00:01.123"
        assert row["status"] == "rejected"

    def test_missing_last_time_is_safe(self):
        # 舊/空回報無 last_time → 不炸, 欄位維持初始空字串
        s = make_session()
        _place(s, "O3")
        s._on_order({"order_no": "O3", "symbol": "2881", "error_message": ""})
        assert s.order_log["O3"]["last_time"] == ""

    def test_order_log_schema_has_last_time(self):
        # _log_order 建立時就有 last_time 欄 (schema 一致)
        s = make_session()
        _place(s, "O4")
        assert "last_time" in s.order_log["O4"] and s.order_log["O4"]["last_time"] == ""
