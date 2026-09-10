"""_on_order 依 function_type 分流 (2026-09-09 node3 事故 A3)。

富邦委託回報: ft 0/10 = 新單/改單;ft 30 = 撤單 — 第一筆 status 4 是撤單請求的回聲 (不是撤成),
status 30|40 才是券商確認撤單;撤單失敗 (status 39,「取消單已不允許取消」等) 走 callback 的 err 參數。
規則:
  ft ∈ {0,10,''/None} 且有 error 且 row 存在 → 既有 rejected 路徑;row 不存在 → 只 log,不動 st
  ft==30: status 4 → 忽略;30|40 → row cancelled + _close_cancel + 出佇列;
          error / status 39 → 撤單失敗分類 (絕不標 rejected、絕不清 st.order_no)
  任一無 error 回報且該書號在佇列 → next_ts=now 喚醒
"""
import logging

import pytest

import trading_session as ts_mod
from fakes_cancel import (MSG_FILLED, MSG_ALREADY_CANCELLED, install_fake_clock, today_at,
                          make_session, run_once)

SYM = "2881"
LU = 66.0


@pytest.fixture
def clock(monkeypatch):
    return install_fake_clock(monkeypatch, today_at(9, 0, 20))


def _pre(s):
    """預掛 2881 (66 × 3 張 = 198k 保留);回 (st, P)。"""
    s.place_pre_orders([SYM], {SYM: LU})
    st = s.trades[SYM]
    assert st.order_no and st.order_status == "pending"
    return st, st.order_no


def _rpt(order_no, ft, status="", err="", symbol=SYM, last_time="", filled_qty=0):
    return {"order_no": order_no, "symbol": symbol, "status": status, "filled_qty": filled_qty,
            "error_message": err, "function_type": ft, "last_time": last_time}


def _queue(s, order_no):
    s.broker.snapshot_missing.add(order_no)
    s.cancel_symbol_orders(SYM, "unmarked")
    assert order_no in s._cancel_queue
    return s._cancel_queue[order_no]


class TestFt30Cancel:
    def test_status_4_echo_ignored(self, clock):
        s = make_session()
        st, p = _pre(s)
        budget = s.budget_used
        s._on_order(_rpt(p, "30", status="4", last_time="09:00:11.2"))
        assert s.order_log[p]["status"] == "pending"
        assert st.order_no == p and st.order_status == "pending" and s.budget_used == budget
        assert s.order_log[p]["last_time"] == "09:00:11.2"       # last_time 照記

    @pytest.mark.parametrize("status", ["30", "40"])
    def test_status_30_40_confirms_cancel(self, clock, status):
        s = make_session()
        st, p = _pre(s)
        it = _queue(s, p)
        s._on_order(_rpt(p, "30", status=status))
        assert s.order_log[p]["status"] == "cancelled"
        assert st.order_status == "cancelled" and st.order_no == ""
        assert s.budget_used == 0 and st.budget_reserved == 0    # _close_cancel 釋預算
        assert st.last_buy_cancel_ts > 0
        assert p not in s._cancel_queue                          # 出佇列
        assert s.order_log[p]["cancel_state"] == ""

    def test_int_function_type_accepted(self, clock):
        s = make_session()
        st, p = _pre(s)
        s._on_order(_rpt(p, 30, status=30))                      # ft/status 皆 int → 轉 str 比較
        assert s.order_log[p]["status"] == "cancelled" and st.order_no == ""

    def test_status_39_error_not_rejected_keeps_order_no(self, clock):
        s = make_session()
        st, p = _pre(s)
        _queue(s, p)
        budget = s.budget_used
        s._on_order(_rpt(p, "30", status="39", err="[115]證券委託目前狀態不允許取消"))
        row = s.order_log[p]
        assert row["status"] == "pending"                        # 絕不標 rejected
        assert st.order_no == p and st.order_status == "pending" # 絕不清 st.order_no
        assert s.budget_used == budget
        assert "不允許取消" in row["cancel_err"]
        assert p in s._cancel_queue                              # 仍由 worker 續試

    def test_status_39_without_queue_still_not_rejected(self, clock):
        s = make_session()
        st, p = _pre(s)
        s._on_order(_rpt(p, "30", status="39", err="[115]撤單失敗"))
        assert s.order_log[p]["status"] == "pending" and st.order_no == p
        assert "撤單失敗" in s.order_log[p]["cancel_err"]

    def test_already_cancelled_message_confirms(self, clock):
        s = make_session()
        st, p = _pre(s)
        _queue(s, p)
        s._on_order(_rpt(p, "30", status="39", err=MSG_ALREADY_CANCELLED))
        assert s.order_log[p]["status"] == "cancelled" and st.order_no == ""
        assert s.budget_used == 0 and p not in s._cancel_queue

    def test_filled_before_cancel_message_not_cancelled_not_rejected(self, clock):
        s = make_session()
        st, p = _pre(s)
        _queue(s, p)
        s._on_order(_rpt(p, "30", status="39", err=MSG_FILLED))
        row = s.order_log[p]
        assert row["status"] not in ("cancelled", "rejected")
        assert st.order_status != "rejected" and st.order_status != "cancelled"
        assert st.budget_reserved > 0                            # 不釋放預算 (成交會轉消耗)
        # 回報沒帶 filled_qty → 無法核實 → 留佇列由 worker 用快照 status 結案 (成交回報遺失也補得到;審查 #11)
        assert p in s._cancel_queue and "FILLED_UNVERIFIED" in row["cancel_err"]
        s.broker.snapshot_missing.discard(p)
        s.broker.snapshot_status[p] = "50"
        s.broker.snapshot_filled[p] = st.target_lots * 1000
        clock.advance(1.0)
        out = run_once(s)
        assert out["filled_before_cancel"] == [p] and p not in s._cancel_queue
        assert row["status"] == "filled" and st.filled_lots == st.target_lots and st.budget_reserved == 0
        assert s.broker.cancel_by_obj_calls == []                # status 50 預分類 → 不再送撤單

    def test_symbol_missing_resolved_from_order_log(self, clock):
        s = make_session()
        st, p = _pre(s)
        s._on_order({"order_no": p, "symbol": "", "status": "30", "function_type": "30",
                     "error_message": "", "filled_qty": 0, "last_time": ""})
        assert st.order_no == "" and st.order_status == "cancelled"

    def test_unknown_order_no_ft30_is_harmless(self, clock):
        s = make_session()
        st, p = _pre(s)
        s._on_order(_rpt("ZZZ", "30", status="30"))
        s._on_order(_rpt("ZZZ", "30", status="39", err="x"))
        assert st.order_no == p and s.order_log[p]["status"] == "pending"
        assert "ZZZ" not in s.order_log

    def test_ft30_report_for_queued_missing_order_then_worker(self, clock):
        # 回報 30 先到 (券商確認),worker 隨後跑 → 出佇列、不重複撤
        s = make_session()
        st, p = _pre(s)
        _queue(s, p)
        s._on_order(_rpt(p, "30", status="30"))
        out = run_once(s)
        assert p not in out["pending"] and s.broker.cancel_by_obj_calls == []


class TestFt0Reject:
    @pytest.mark.parametrize("ft", ["0", "10", 0, 10, "", None])
    def test_new_order_error_with_row_rejected(self, clock, ft):
        s = make_session()
        st, p = _pre(s)
        s._on_order(_rpt(p, ft, status="4", err="集合競價時段不可輸入市價、IOC、FOK委託"))
        assert s.order_log[p]["status"] == "rejected"
        assert st.order_status == "rejected" and st.order_no == ""
        assert s.budget_used == 0                                # 拒單釋放保留 (既有)

    def test_reject_dequeues_if_queued(self, clock):
        s = make_session()
        st, p = _pre(s)
        _queue(s, p)
        s._on_order(_rpt(p, "0", status="4", err="拒單"))
        assert p not in s._cancel_queue and s.order_log[p]["status"] == "rejected"

    def test_error_without_row_only_logs(self, clock, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")
        s = make_session()
        st, p = _pre(s)
        budget = s.budget_used
        s._on_order(_rpt("NOT-IN-LOG", "0", status="4", err="拒單", symbol=SYM))
        assert st.order_no == p and st.order_status == "pending" and s.budget_used == budget
        assert "NOT-IN-LOG" not in s.order_log
        assert any("order_log 無此書號" in r.getMessage() for r in caplog.records)

    @pytest.mark.parametrize("order_no", [None, ""])
    def test_9049_call_auction_reject_order_no_none(self, clock, order_no):
        # 集合競價 9049 拒單: order_no None/空 → 不影響任何 st (09-09 集合競價期 ~130 筆這種拒單)
        s = make_session()
        st, p = _pre(s)
        budget = s.budget_used
        s._on_order({"order_no": order_no, "symbol": SYM, "status": "4", "filled_qty": 0,
                     "error_message": "證券集合競價時段不可輸入市價、IOC、FOK委託",
                     "function_type": 0, "last_time": "09:00:07.8"})
        assert st.order_no == p and st.order_status == "pending" and s.budget_used == budget
        assert s.order_log[p]["status"] == "pending"
        assert None not in s.order_log and "" not in s.order_log

    def test_accept_report_wakes_queued_entry(self, clock):
        s = make_session()
        st, p = _pre(s)
        it = _queue(s, p)
        it["next_ts"] = ts_mod.time.time() + 100
        s._on_order(_rpt(p, "10", status="10", last_time="09:00:11.040"))   # 無 error 新單回報
        assert s._cancel_queue[p]["next_ts"] <= ts_mod.time.time() + 0.01
        assert s.order_log[p]["last_time"] == "09:00:11.040"
        assert s.order_log[p]["status"] == "pending"

    def test_legacy_reject_path_unchanged(self, clock):
        # test_order_last_time 語意: 舊格式 (無 function_type) 拒單 → rejected + last_time
        s = make_session()
        st, p = _pre(s)
        s._on_order({"order_no": p, "symbol": SYM, "status": "4", "filled_qty": 0,
                     "error_message": "集合競價時段不可輸入市價", "last_time": "09:00:01.123"})
        assert s.order_log[p]["status"] == "rejected" and s.order_log[p]["last_time"] == "09:00:01.123"
