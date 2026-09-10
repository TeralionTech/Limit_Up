"""撤單佇列 worker (2026-09-09 node3「管線多送市價單未撤」事故 A2) — 重演 + 退避/告警/放棄/分類/去重。

核心承諾: **cancelled 只能由券商確認寫入**;查無 (不在快照) / 查詢失敗 (流量控管) / 撤單被拒 (非終端)
→ row 維持 pending、cancel_state=queued/unconfirmed、不清 st.order_no、不釋放預算,由 worker 重試到確認。
測試以 TradingSession.auto_cancel_worker=False + _cancel_worker_run_once() 同步驅動;
時間用假時鐘 (trading_session.time / datetime) 讓退避序列與 40 s drain 瞬間走完。
"""
import logging
import threading
import time as _real_time
from datetime import time as dtime

import pytest

import trading_session as ts_mod
from trading_session import classify_cancel_error
from fakes_cancel import (FakeSnapshotBroker, OrderNotFound, OrderLookupError,
                          MSG_FILLED, MSG_PARTIAL_FILLED, MSG_ALREADY_CANCELLED, MSG_RATE_LIMIT,
                          install_fake_clock, today_at, make_session, run_once, fill, wait_until,
                          build_node3)

SYM = "5386"
LU = 283.0


@pytest.fixture
def clock(monkeypatch):
    """09:00:15 起的假時鐘 (盤中;離 13:34 放棄點很遠)。"""
    return install_fake_clock(monkeypatch, today_at(9, 0, 15))


def _one_missing_order(s, symbol="2330"):
    """一筆 fake 下過、但快照查無的市價買 (row pending) → 入佇列。回 order_no。"""
    b = s.broker
    no = b.place_market_buy(symbol, 1)
    s._log_order(no, symbol, "buy", "market_buy", 1, 0)
    b.snapshot_missing.add(no)
    s.request_cancel(no, symbol, "chase_extra")
    return no


def _crit(caplog, needle=""):
    return [r for r in caplog.records if r.levelno == logging.CRITICAL and needle in r.getMessage()]


def _warn(caplog, needle=""):
    return [r for r in caplog.records if r.levelno == logging.WARNING and needle in r.getMessage()]


# ═══ 2026-09-09 node3 5386 重演 ══════════════════════════════════════

class TestNode3Replay:
    def test_extras_go_to_queue_not_thread_per_cancel(self, clock):
        s, p, m1, extras = build_node3()
        b = s.broker
        assert len(extras) == 7
        assert set(extras) <= set(s._cancel_queue)             # 7 筆管線多送 M 全入佇列
        assert m1 not in s._cancel_queue and p not in s._cancel_queue
        assert s.order_log[p]["status"] == "cancelled"          # 預掛 P 同步撤成功 (快照有)
        for no in extras:
            row = s.order_log[no]
            assert row["status"] == "pending" and row["cancel_state"] == "queued"
            assert row["cancel_attempts"] == 0 and row["cancel_reason"] == "chase_extra"
        assert s.trades[SYM].order_no == m1 and s.trades[SYM].order_status == "pending"
        st = s.status()
        assert st["n_inflight"] == 8 and st["n_cancel_queued"] == 7 and st["n_cancel_unconfirmed"] == 0
        assert b.snapshot_calls == 1                            # 只有撤 P 那次同步查詢 (多送不逐筆查)

    def test_four_missing_stay_pending_single_snapshot_per_round(self, clock, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")
        s, p, m1, extras = build_node3()
        b = s.broker
        missing = set(extras[:4])                               # KT08n/m/i/l: 快照查無
        b.snapshot_missing = set(missing)
        b.snapshot_calls = 0
        out = run_once(s)
        assert b.snapshot_calls == 1                            # 整輪一次快照 (不是 7 次)
        assert out["snapshot_ok"] is True
        assert set(out["cancelled"]) == set(extras[4:]) and set(out["pending"]) == missing
        for no in extras[4:]:
            assert s.order_log[no]["status"] == "cancelled" and s.order_log[no]["cancel_state"] == ""
            assert no not in s._cancel_queue
        for no in missing:
            row = s.order_log[no]
            assert row["status"] == "pending"                   # ⭐ 不再誤標 cancelled
            assert row["cancel_state"] == "unconfirmed" and row["cancel_attempts"] == 1
            assert "NOT_IN_SNAPSHOT" in row["cancel_err"]
            assert s._cancel_queue[no]["attempts"] == 1
        assert {c[0] for c in b.cancelled} == {p} | set(extras[4:])
        assert s.trades[SYM].order_no == m1                     # 第一筆 M 不動
        st = s.status()
        assert st["n_cancel_unconfirmed"] == 4 and st["n_cancel_queued"] == 4 and st["n_inflight"] == 5
        assert "查無委託 (可能已成交/已撤)" not in caplog.text
        # 後檯同步上了 → 退避到期後全部撤到
        b.snapshot_missing.clear()
        clock.advance(1.0)
        out2 = run_once(s)
        assert set(out2["cancelled"]) == missing and s._cancel_queue == {}
        for no in extras:
            assert s.order_log[no]["status"] == "cancelled" and s.order_log[no]["cancel_attempts"] in (0, 1)
        assert s.trades[SYM].order_no == m1 and s.order_log[m1]["status"] == "pending"
        assert s.status()["n_inflight"] == 1 and s.status()["n_cancel_unconfirmed"] == 0
        assert s.trades[SYM].last_buy_cancel_ts > 0

    def test_all_seven_cancelled_when_snapshot_complete(self, clock):
        s, p, m1, extras = build_node3()
        out = run_once(s)
        assert set(out["cancelled"]) == set(extras) and s._cancel_queue == {}
        assert {c[0] for c in s.broker.cancelled} == {p} | set(extras)
        assert s.budget_used == LU * 1000                      # M1 保留不受多送撤單影響


# ═══ 退避 / 告警 / 放棄 ═══════════════════════════════════════════

class TestBackoffAndAlerts:
    def test_backoff_sequence_then_every_5s(self, clock):
        s = make_session()
        b = s.broker
        no = _one_missing_order(s)
        expected = [0.3, 0.5, 1.0, 2.0, 3.0, 5.0, 5.0, 5.0]
        for i, back in enumerate(expected, start=1):
            calls = b.snapshot_calls
            out = run_once(s)                                   # 到期 → 一次快照 → 查無 → attempts=i
            assert b.snapshot_calls == calls + 1 and no in out["pending"]
            assert s._cancel_queue[no]["attempts"] == i
            assert s.order_log[no]["cancel_attempts"] == i
            clock.advance(back * 0.5)                           # 退避未到 → 不查
            run_once(s)
            assert b.snapshot_calls == calls + 1
            clock.advance(back * 0.5 + 0.05)
        row = s.order_log[no]
        assert row["status"] == "pending" and row["cancel_state"] == "unconfirmed"
        assert s.status()["n_cancel_unconfirmed"] == 1

    def test_not_due_round_does_not_query(self, clock):
        s = make_session()
        no = _one_missing_order(s)
        run_once(s)
        calls = s.broker.snapshot_calls
        run_once(s)                                             # 0.3 s 內再跑 → 不到期不查
        assert s.broker.snapshot_calls == calls
        assert s._cancel_queue[no]["attempts"] == 1

    def test_warning_at_3_critical_at_6_repeat_every_30s(self, clock, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")
        s = make_session()
        no = _one_missing_order(s)
        for n in range(1, 12):
            run_once(s)
            assert s._cancel_queue[no]["attempts"] == n
            warns = _warn(caplog, no)
            crits = _crit(caplog, no)
            if n < 3:
                assert warns == [] and crits == []
            elif n < 6:
                assert len(warns) == 1 and "第 3 次" in warns[0].getMessage() and crits == []
            elif n < 11:
                assert len(crits) == 1 and f"第 6 次" in crits[0].getMessage()
            else:                                               # 第 6 次後每 30 s 重複 (6 s × 5 = 30 s)
                assert len(crits) == 2
            clock.advance(6.0)                                  # 退避 ≤5 s → 每輪都到期
        assert s.order_log[no]["status"] == "pending"

    def test_give_up_after_trading_end_plus_10min(self, monkeypatch, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")
        install_fake_clock(monkeypatch, today_at(13, 35, 30))    # 13:24 + 10 = 13:34 已過
        s = make_session()
        s.trading_end_time = dtime(13, 24, 0)
        no = _one_missing_order(s)
        run_once(s)
        assert no not in s._cancel_queue                         # 放棄 → 出佇列
        row = s.order_log[no]
        assert row["status"] == "pending"                        # 留 pending (絕不標 cancelled)
        assert row["cancel_state"] == "unconfirmed"
        assert row["cancel_err"].startswith("GIVE_UP")
        crits = _crit(caplog, no)
        assert crits and "撤單放棄" in crits[-1].getMessage()
        assert s.status()["n_cancel_unconfirmed"] == 1           # 放棄的仍算未確認 (UI 看得到)
        assert s.status()["n_cancel_queued"] == 0

    def test_no_give_up_before_deadline(self, monkeypatch, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")
        install_fake_clock(monkeypatch, today_at(13, 30, 0))     # 13:24 後但未到 13:34
        s = make_session()
        s.trading_end_time = dtime(13, 24, 0)
        no = _one_missing_order(s)
        run_once(s)
        assert no in s._cancel_queue and s._cancel_queue[no]["attempts"] == 1
        assert not _crit(caplog, "撤單放棄")


# ═══ 閘門 / 快照失敗 / 逾時 ═══════════════════════════════════════

class TestGates:
    def test_unhealthy_broker_skips_without_attempts(self, clock):
        s = make_session()
        no = _one_missing_order(s)
        s.broker.healthy = False
        calls = s.broker.snapshot_calls
        out = run_once(s)
        assert out.get("skipped") is True or no in out["pending"]
        assert s.broker.snapshot_calls == calls                  # 不查
        assert s._cancel_queue[no]["attempts"] == 0              # 不計 attempts
        assert s.order_log[no]["cancel_attempts"] == 0
        # 略過的到期項延後 _CANCEL_SKIP_BACKOFF_SEC (審查 #1/#4/#13: 否則 worker / 收盤 drain 零延遲忙迴圈);
        # 生產環境由 _on_broker_reconnected 把 next_ts 拉回 now 喚醒,復原延遲不變
        assert s._cancel_queue[no]["next_ts"] > ts_mod.time.time() + 0.5
        s.broker.healthy = True
        run_once(s)                                              # 未到期 → 本輪不查、不計
        assert s.broker.snapshot_calls == calls and s._cancel_queue[no]["attempts"] == 0
        s._on_broker_reconnected()                               # 重連 hook → next_ts=now
        out = run_once(s)
        assert s._cancel_queue[no]["attempts"] == 1

    def test_cannot_manage_skips(self, clock):
        s = make_session()
        no = _one_missing_order(s)
        s.set_mode("sim")                                        # _can_manage False
        calls = s.broker.snapshot_calls
        run_once(s)
        assert s.broker.snapshot_calls == calls
        assert s._cancel_queue[no]["attempts"] == 0

    def test_snapshot_failure_backs_off_without_attempts(self, clock):
        s = make_session()
        b = s.broker
        no = _one_missing_order(s)
        b.fail_snapshot_times = 1
        out = run_once(s)
        assert out["snapshot_ok"] is False and no in out["pending"]
        assert s._cancel_queue[no]["attempts"] == 0              # 流量控管不計 attempts
        assert s.order_log[no]["cancel_err"].startswith("QUERY:")
        assert "流量控管" in s.order_log[no]["cancel_err"]
        calls = b.snapshot_calls
        clock.advance(0.5)
        run_once(s)                                              # +1 s 未到 → 不查
        assert b.snapshot_calls == calls
        clock.advance(0.6)
        run_once(s)
        assert b.snapshot_calls == calls + 1 and s._cancel_queue[no]["attempts"] == 1

    def test_snapshot_timeout_is_unconfirmed_not_attempt(self, clock, monkeypatch):
        monkeypatch.setattr(ts_mod, "_CANCEL_SDK_TIMEOUT_SEC", 0.2)
        s = make_session()
        b = s.broker
        no = _one_missing_order(s)
        orig = b.get_order_snapshot

        def slow():
            _real_time.sleep(0.6)
            return orig()
        b.get_order_snapshot = slow
        out = run_once(s)
        assert out["snapshot_ok"] is False and no in out["pending"]
        assert s._cancel_queue[no]["attempts"] == 0

    def test_cancel_timeout_is_unconfirmed_not_attempt(self, clock, monkeypatch):
        monkeypatch.setattr(ts_mod, "_CANCEL_SDK_TIMEOUT_SEC", 0.2)
        s = make_session()
        b = s.broker
        no = b.place_market_buy("2330", 1)
        s._log_order(no, "2330", "buy", "market_buy", 1, 0)
        s.request_cancel(no, "2330", "x")

        def slow_cancel(obj, order_no, symbol="", reason=""):
            _real_time.sleep(0.6)
        b.cancel_by_obj = slow_cancel
        out = run_once(s)
        assert no in out["pending"] and no not in out["cancelled"]
        assert s._cancel_queue[no]["attempts"] == 0
        row = s.order_log[no]
        assert row["status"] == "pending" and row["cancel_state"] == "unconfirmed"
        assert "逾時" in row["cancel_err"]


# ═══ 撤單失敗訊息分類 ═══════════════════════════════════════════════

class TestClassification:
    def test_classify_cancel_error(self):
        assert classify_cancel_error(MSG_FILLED) == "filled_before_cancel"
        assert classify_cancel_error(MSG_PARTIAL_FILLED) == "filled_before_cancel"
        assert classify_cancel_error(MSG_ALREADY_CANCELLED) == "already_cancelled"
        assert classify_cancel_error("撤單失敗 K1: 系統忙碌中") == "retry"
        assert classify_cancel_error("") == "retry" and classify_cancel_error(None) == "retry"

    def test_filled_before_cancel_settles_fill_from_snapshot(self, clock):
        s, p, m1, extras = build_node3(n_extra=1)
        b = s.broker
        no = extras[0]
        b.cancel_fail_msg[no] = MSG_FILLED
        b.snapshot_filled[no] = 1000                             # 快照: 已成交 1 張 (股數)
        cost0 = s._buy_cost_actual
        out = run_once(s)
        assert out["filled_before_cancel"] == [no] and no not in out["cancelled"]
        assert no not in s._cancel_queue
        row = s.order_log[no]
        assert row["status"] == "filled" and row["filled_lots"] == 1  # 不標 cancelled;用快照補成交
        assert row["cancel_state"] == ""
        st = s.trades[SYM]
        assert st.filled_lots == 1                                # 超買計入部位 (出場全量賣)
        assert s._buy_cost_actual == cost0 + LU * 1000
        assert not any(c[0] == no for c in b.cancelled)

    def test_filled_before_cancel_sync_then_report_counted_once(self, clock):
        # docs 明定同一拒撤同時出現在同步回傳與 ft30 status 39 主動回報 → 只計一次、hook 一次 (審查 #6/#9)
        s, p, m1, (no,) = build_node3(n_extra=1)
        b = s.broker
        b.cancel_fail_msg[no] = MSG_FILLED
        b.snapshot_filled[no] = 1000
        hooks = []
        s.on_late_confirm = lambda: hooks.append(1)
        s._after_trading_end = lambda: True                      # 視同收盤後 (驗 hook 次數)
        run_once(s)                                               # worker: cancel_by_obj 被拒 → 結案
        assert s._cancel_stats["filled_before_cancel"] == 1 and hooks == [1]
        s._on_order({"order_no": no, "symbol": SYM, "status": "39", "filled_qty": 0,
                     "error_message": MSG_FILLED, "function_type": "30", "last_time": ""})
        assert s._cancel_stats["filled_before_cancel"] == 1 and hooks == [1]   # 重複 → 不再計
        assert s.order_log[no]["status"] == "filled" and s.trades[SYM].filled_lots == 1
        assert no not in s._cancel_queue

    def test_filled_before_cancel_then_late_report_no_double_count(self, clock):
        s, p, m1, extras = build_node3(n_extra=1)
        no = extras[0]
        s.broker.cancel_fail_msg[no] = MSG_FILLED
        s.broker.snapshot_filled[no] = 1000
        run_once(s)
        s._on_fill(fill(no, SYM, 1, LU, filled_no="LATE"))       # 真回報晚到 → 單筆封頂不雙算
        assert s.trades[SYM].filled_lots == 1

    def test_already_cancelled_confirms_idempotently(self, clock):
        s, p, m1, extras = build_node3(n_extra=1)
        no = extras[0]
        s.broker.cancel_fail_msg[no] = MSG_ALREADY_CANCELLED
        out = run_once(s)
        assert out["cancelled"] == [no] and no not in s._cancel_queue
        assert s.order_log[no]["status"] == "cancelled"
        assert s.trades[SYM].order_no == m1                      # 不動第一筆 M

    def test_unknown_error_retries(self, clock):
        s, p, m1, extras = build_node3(n_extra=1)
        no = extras[0]
        s.broker.cancel_fail_msg[no] = "系統忙碌中,請稍後再試"
        out = run_once(s)
        assert no in out["pending"] and no in s._cancel_queue
        assert s._cancel_queue[no]["attempts"] == 1
        row = s.order_log[no]
        assert row["status"] == "pending" and row["cancel_state"] == "unconfirmed"
        assert "系統忙碌中" in row["cancel_err"]
        del s.broker.cancel_fail_msg[no]
        clock.advance(1.0)
        assert run_once(s)["cancelled"] == [no]


# ═══ 快照 status 預分類: 已終結 (30/40/50/90) 不送撤單 (審查 #12) ═══════════

class TestSnapshotStatusPreclassify:
    def test_status_30_confirms_without_cancel_call(self, clock):
        s, p, m1, (no,) = build_node3(n_extra=1)
        b = s.broker
        b.snapshot_status[no] = "30"
        out = run_once(s)
        assert out["cancelled"] == [no] and s.order_log[no]["status"] == "cancelled"
        assert not any(c[0] == no for c in b.cancel_by_obj_calls)
        assert s._cancel_queue == {}

    def test_status_40_partial_fill_then_cancelled(self, clock):
        # 2 張預掛 P: 快照 40 (部分成交 1 張、剩餘取消) → 補 1 張成交 + cancelled 結案、釋放剩餘保留
        s = make_session(total=10_000_000, per_symbol=0, sizing_mode="fixed_lots", fixed_lots=2)
        b = s.broker
        s.place_pre_orders([SYM], {SYM: LU})
        st = s.trades[SYM]
        p = st.order_no
        b.snapshot_status[p] = "40"
        b.snapshot_filled[p] = 1000
        s.request_cancel(p, SYM, "unmarked")
        out = run_once(s)
        assert out["cancelled"] == [p]
        row = s.order_log[p]
        assert row["status"] == "cancelled" and row["filled_lots"] == 1
        assert st.filled_lots == 1 and st.order_no == "" and st.order_status == "cancelled"
        assert st.budget_reserved == 0 and s.budget_used == LU * 1000     # 1 張消耗,剩餘保留釋放
        assert not any(c[0] == p for c in b.cancel_by_obj_calls)

    def test_status_50_settles_filled(self, clock):
        s, p, m1, (no,) = build_node3(n_extra=1)
        b = s.broker
        b.snapshot_status[no] = "50"
        b.snapshot_filled[no] = 1000
        out = run_once(s)
        assert out["filled_before_cancel"] == [no]
        assert s.order_log[no]["status"] == "filled" and s.trades[SYM].filled_lots == 1
        assert not any(c[0] == no for c in b.cancel_by_obj_calls)

    def test_status_90_rejected_releases_budget_no_false_critical(self, clock, caplog):
        # 交易所拒單 (ft90 回報漏收) → 快照 90 → row rejected、清 st.order_no、釋放預算;
        # 不送撤單、不累 attempts、不假 CRITICAL (舊碼: 6 次撤單被拒 → CRITICAL「券商端可能仍 live」)
        caplog.set_level(logging.INFO, logger="trading_session")
        s = make_session()
        b = s.broker
        s.place_pre_orders(["2330"], {"2330": 100.0})
        st = s.trades["2330"]
        p = st.order_no
        b.snapshot_status[p] = "90"
        b.cancel_fail_msg[p] = "[999]證券委託目前狀態失敗單已不允許取消交易"   # 未知文案 (若真送撤單會被當 retry)
        s.request_cancel(p, "2330", "unmarked")
        for _ in range(6):
            run_once(s)
            clock.advance(6.0)
        assert s.order_log[p]["status"] == "rejected" and p not in s._cancel_queue
        assert st.order_no == "" and st.order_status == "rejected" and s.budget_used == 0
        assert not any(c[0] == p for c in b.cancel_by_obj_calls)
        assert not _crit(caplog, p)

    def test_unknown_reject_wording_logs_critical_once(self, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")
        msg = "[115]證券委託目前狀態預約單已不允許取消交易"
        assert classify_cancel_error(msg) == "retry"
        assert classify_cancel_error(msg) == "retry"
        assert len(_crit(caplog, "文案未知")) == 1


# ═══ 佇列語意 ═══════════════════════════════════════════════════════

class TestQueueSemantics:
    def test_dedup_updates_reason(self, clock):
        s = make_session()
        no = _one_missing_order(s)
        s.request_cancel(no, "2330", "budget_breached")
        s.request_cancel(no, "2330", "trading_end")
        assert list(s._cancel_queue) == [no]
        assert s._cancel_queue[no]["reason"] == "trading_end"
        assert s.order_log[no]["cancel_reason"] == "trading_end"

    def test_queue_entry_shape(self, clock):
        s = make_session()
        no = _one_missing_order(s)
        it = s._cancel_queue[no]
        assert set(it) >= {"symbol", "reason", "first_ts", "attempts", "next_ts"}
        assert it["symbol"] == "2330" and it["attempts"] == 0 and it["next_ts"] <= it["first_ts"] + 1e-6

    def test_terminal_row_not_in_snapshot_dequeues(self, clock):
        # 不在快照但 row 已終結 (回報先到 filled) → 出佇列、不計未確認
        s = make_session()
        no = _one_missing_order(s)
        s._on_fill(fill(no, "2330", 1, 100.0))
        assert s.order_log[no]["status"] == "filled"
        out = run_once(s)
        assert no not in s._cancel_queue and no not in out["pending"]
        assert s.status()["n_cancel_unconfirmed"] == 0

    def test_request_cancel_ignores_non_pending_rows(self, clock):
        s = make_session()
        no = s.broker.place_market_buy("2330", 1)
        s._log_order(no, "2330", "buy", "market_buy", 1, 0)
        s._on_fill(fill(no, "2330", 1, 100.0))
        s.request_cancel(no, "2330", "x")
        assert s._cancel_queue == {}

    def test_roll_day_clears_queue(self, clock):
        s = make_session()
        _one_missing_order(s)
        s.roll_day("2026-09-10")
        assert s._cancel_queue == {}

    def test_empty_queue_round_does_not_query(self, clock):
        s = make_session()
        out = run_once(s)
        assert s.broker.snapshot_calls == 0 and out["cancelled"] == []

    def test_sim_mode_never_starts_worker(self):
        s = make_session()
        s.auto_cancel_worker = True
        s.set_mode("sim")
        no = s.broker.place_market_buy("2330", 1)
        s._log_order(no, "2330", "buy", "market_buy", 1, 0)
        s.request_cancel(no, "2330", "x")
        assert s.status()["cancel_worker_alive"] is False

    def test_real_mode_lazy_starts_worker_and_drains(self):
        s = make_session()
        s.auto_cancel_worker = True
        no = s.broker.place_market_buy("2330", 1)
        s._log_order(no, "2330", "buy", "market_buy", 1, 0)
        s.request_cancel(no, "2330", "x")
        assert s.status()["cancel_worker_alive"] is True
        assert wait_until(lambda: s.order_log[no]["status"] == "cancelled", 3.0)
        assert s._cancel_queue == {}

    def test_status_and_get_orders_carry_new_fields(self, clock):
        s = make_session()
        no = _one_missing_order(s)
        run_once(s)
        st = s.status()
        for k in ("n_cancel_unconfirmed", "n_cancel_queued", "n_inflight", "cancel_worker_alive"):
            assert k in st
        assert st["n_cancel_unconfirmed"] == 1 and st["n_cancel_queued"] == 1 and st["n_inflight"] == 1
        row = next(r for r in s.get_orders() if r["order_no"] == no)
        assert row["cancel_state"] == "unconfirmed" and row["cancel_attempts"] == 1
        assert row["cancel_reason"] == "chase_extra" and "NOT_IN_SNAPSHOT" in row["cancel_err"]
        assert row["status"] == "pending"                        # status 值域不變

    def test_late_confirm_hook_after_trading_end_only(self, monkeypatch):
        install_fake_clock(monkeypatch, today_at(13, 30, 0))
        s = make_session()
        s.trading_end_time = dtime(13, 24, 0)
        calls = []
        s.on_late_confirm = lambda: calls.append(1)
        no = s.broker.place_market_buy("2330", 1)
        s._log_order(no, "2330", "buy", "market_buy", 1, 0)
        s.request_cancel(no, "2330", "trading_end")
        assert run_once(s)["cancelled"] == [no]
        assert calls == [1]                                      # 收盤後結案 → 重寫隔日賣清單

    def test_late_confirm_hook_not_before_trading_end(self, clock):
        s = make_session()
        s.trading_end_time = dtime(13, 24, 0)
        calls = []
        s.on_late_confirm = lambda: calls.append(1)
        no = s.broker.place_market_buy("2330", 1)
        s._log_order(no, "2330", "buy", "market_buy", 1, 0)
        s.request_cancel(no, "2330", "x")
        run_once(s)
        assert calls == []


# ═══ 同步呼叫端: 查無/查詢失敗 → row 保持 pending + 入佇列,回傳語意不變 ═══

class TestSyncCallers:
    def _pre(self, s, symbol="2330", limit_up=100.0):
        s.place_pre_orders([symbol], {symbol: limit_up})
        return s.trades[symbol]

    def test_cancel_symbol_orders_not_found_keeps_pending(self, clock, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")
        s = make_session()
        b = s.broker
        st = self._pre(s)
        p = st.order_no
        b.snapshot_missing.add(p)
        budget = s.budget_used
        s.cancel_symbol_orders("2330", "unmarked")
        assert st.order_no == p and st.order_status == "pending"   # ⭐ 不清 order_no、不標 cancelled
        assert s.budget_used == budget and st.budget_reserved > 0  # 不釋放預算
        assert st.stopped_reason == "unmarked"                     # 停止進場意圖照記
        row = s.order_log[p]
        assert row["status"] == "pending" and row["cancel_state"] == "queued"
        assert "NOT_FOUND" in row["cancel_err"]
        assert p in s._cancel_queue
        assert b.cancelled == []
        # 快照同步後 worker 確認 → 才翻 st / 釋預算
        b.snapshot_missing.clear()
        assert run_once(s)["cancelled"] == [p]
        assert st.order_no == "" and st.order_status == "cancelled" and s.budget_used == 0
        assert s.order_log[p]["status"] == "cancelled"

    def test_cancel_symbol_orders_lookup_error_keeps_pending(self, clock):
        s = make_session()
        st = self._pre(s)
        p = st.order_no
        s.broker.fail_snapshot_times = 1
        s.cancel_symbol_orders("2330", "unmarked")
        assert st.order_no == p and s.order_log[p]["status"] == "pending"
        assert "LOOKUP" in s.order_log[p]["cancel_err"] and p in s._cancel_queue

    def test_cancel_symbol_orders_generic_error_keeps_pending(self, clock):
        s = make_session()
        st = self._pre(s)
        p = st.order_no

        def _boom(order_no, symbol="", reason=""):
            raise RuntimeError("timeout")
        s.broker.cancel = _boom
        s.cancel_symbol_orders("2330", "unmarked")
        assert st.order_no == p and s.order_log[p]["status"] == "pending"
        assert p in s._cancel_queue

    def test_cancel_symbol_orders_already_cancelled_is_terminal(self, clock):
        s = make_session()
        st = self._pre(s)
        p = st.order_no
        s.broker.cancel_fail_msg[p] = MSG_ALREADY_CANCELLED
        s.cancel_symbol_orders("2330", "unmarked")
        assert st.order_no == "" and st.order_status == "cancelled" and s.budget_used == 0
        assert s._cancel_queue == {}

    def test_cancel_symbol_orders_filled_before_cancel_settles_from_reject(self, clock):
        # 券商拒撤「成交單已不允許取消」+ 拒撤例外帶的快照 filled_qty = 委託量 → 立刻補成交、row filled、出佇列
        # (審查 #11: 舊碼只丟訊息不帶 filled_qty → 補成交要等 13:23 再撤一次)
        s = make_session()
        st = self._pre(s)
        p = st.order_no
        lots = st.target_lots
        s.broker.cancel_fail_msg[p] = MSG_FILLED
        s.broker.snapshot_filled[p] = lots * 1000
        s.cancel_symbol_orders("2330", "unmarked")
        assert s.order_log[p]["status"] == "filled" and s.order_log[p]["filled_lots"] == lots
        assert st.filled_lots == lots and st.order_status == "done" and st.order_no == ""
        assert st.stopped_reason == "unmarked" and s._cancel_queue == {}
        assert st.budget_reserved == 0 and s._cancel_stats["filled_before_cancel"] == 1

    def test_filled_before_cancel_unverifiable_stays_queued_until_snapshot(self, clock):
        # 拒撤說已成交,但手上物件 (撤單前快照) 仍 status 10 / filled 0 (replica 落後) → 無法核實 → 留佇列;
        # 下一輪快照 status 50 + filled_qty → 結案 filled (不靠成交回報;status 50 預分類不再送撤單)
        s = make_session()
        b = s.broker
        st = self._pre(s)
        p = st.order_no
        lots = st.target_lots
        b.cancel_fail_msg[p] = MSG_FILLED
        s.cancel_symbol_orders("2330", "unmarked")
        row = s.order_log[p]
        assert row["status"] == "pending" and p in s._cancel_queue
        assert row["cancel_state"] == "unconfirmed" and "FILLED_UNVERIFIED" in row["cancel_err"]
        assert st.order_no == p and st.filled_lots == 0 and s._cancel_stats["filled_before_cancel"] == 0
        b.snapshot_status[p] = "50"
        b.snapshot_filled[p] = lots * 1000
        clock.advance(1.0)
        out = run_once(s)
        assert out["filled_before_cancel"] == [p] and s._cancel_queue == {}
        assert len([c for c in b.cancel_by_obj_calls if c[0] == p]) == 1      # 第二輪不再送撤單
        assert row["status"] == "filled" and st.filled_lots == lots and st.order_no == ""
        assert s._cancel_stats["filled_before_cancel"] == 1

    def test_partial_fill_message_closes_row_and_releases_remainder(self, clock):
        # 「部分成交單已不允許取消」= 富邦 status 40 (部分成交、剩餘取消 = 終結) → 補 1 張 + cancelled 結案、
        # 釋放剩餘保留、清 st.order_no (審查 #2: 舊碼留 pending、保留洩漏、每次收盤再撤一次)
        s = make_session()
        st = self._pre(s)                                        # target 2 (200k / 100k)
        p = st.order_no
        assert st.target_lots == 2
        s.broker.cancel_fail_msg[p] = MSG_PARTIAL_FILLED
        s.broker.snapshot_filled[p] = 1000
        s.broker.snapshot_status[p] = "40"
        s.cancel_symbol_orders("2330", "unmarked")
        row = s.order_log[p]
        assert row["status"] == "cancelled" and row["filled_lots"] == 1
        assert st.filled_lots == 1 and st.order_no == "" and st.order_status == "cancelled"
        assert st.budget_reserved == 0 and s.budget_used == 100_000 and s._cancel_queue == {}
        assert s._cancel_stats["confirmed"] == 1 and s._cancel_stats["filled_before_cancel"] == 0

    def test_stray_buys_dedup_queued_and_single_snapshot(self, clock):
        # 已在佇列的多送 M (chase_extra) → 只喚醒不查;新 stray → 入列後同步跑一輪 = 一次快照扇出 (審查 #5/#10)
        s, p, m1, extras = build_node3(n_extra=4)
        b = s.broker
        b.snapshot_calls = 0
        assert s._cancel_stray_buys(SYM, "exit") == 4
        assert b.snapshot_calls == 0                              # 4 筆都在佇列 → 零查詢
        assert all(no in s._cancel_queue for no in extras)
        n1 = b.place_market_buy(SYM, 1)
        s._log_order(n1, SYM, "buy", "market_buy", 1, 0)
        n2 = b.place_market_buy(SYM, 1)
        s._log_order(n2, SYM, "buy", "market_buy", 1, 0)
        b.snapshot_calls = 0
        assert s._cancel_stray_buys(SYM, "exit") == 6
        assert b.snapshot_calls == 1                              # 6 筆一次快照 (非逐筆 6 次)
        assert all(s.order_log[no]["status"] == "cancelled" for no in extras + [n1, n2])
        assert s._cancel_queue == {} and s.trades[SYM].last_buy_cancel_ts > 0

    def test_orphan_pre_not_found_returns_true_and_queues(self, clock):
        s, p, m1, extras = build_node3(n_extra=0)
        # 重建「孤兒 P 仍 pending」: 把 P 翻回 pending 且快照查無
        s.order_log[p]["status"] = "pending"
        s.broker.cancelled_nos.discard(p)
        s.broker.snapshot_missing.add(p)
        st = s.trades[SYM]
        st.last_buy_cancel_ts = 0.0
        assert s._cancel_orphan_pre(SYM, "exit") is True         # 回傳語意不變 (要等窗口)
        assert s.order_log[p]["status"] == "pending" and p in s._cancel_queue
        assert st.last_buy_cancel_ts > 0

    def test_stray_buys_not_found_returns_count_and_queues(self, clock):
        s, p, m1, extras = build_node3(n_extra=2)
        b = s.broker
        run_once(s)                                              # 先讓 2 筆多送撤掉
        no = b.place_market_buy(SYM, 1)
        s._log_order(no, SYM, "buy", "market_buy", 1, 0)         # 新的 stray
        b.snapshot_missing.add(no)
        st = s.trades[SYM]
        st.last_buy_cancel_ts = 0.0
        assert s._cancel_stray_buys(SYM, "exit") == 1
        assert s.order_log[no]["status"] == "pending" and no in s._cancel_queue
        assert st.last_buy_cancel_ts > 0

    def test_overnight_skip_not_found_keeps_sell_order_until_confirmed(self, clock):
        s = make_session()
        b = s.broker
        s.load_overnight([{"symbol": "2330", "lots": 1, "avg_cost": 90.0}])
        o = s.overnight["2330"]
        o.update(reconciled=True)
        sell_no = b.place_limit_sell("2330", 81.0, 1, "overnight_sell")
        s._log_order(sell_no, "2330", "sell", "overnight_sell", 1, 81.0)
        o.update(sell_placed=True, sell_order_no=sell_no)
        b.snapshot_missing.add(sell_no)
        s.set_overnight_skip("2330", True)
        assert o["skip"] is True
        assert s.order_log[sell_no]["status"] == "pending" and sell_no in s._cancel_queue
        assert o["sell_order_no"] == sell_no                     # 確認前不清 (免重複掛賣)
        b.snapshot_missing.clear()
        assert run_once(s)["cancelled"] == [sell_no]
        assert o["sell_placed"] is False and o["sell_order_no"] == ""
        assert s.order_log[sell_no]["status"] == "cancelled"

    def test_manual_cancel_requeues_when_already_cancelling(self, clock):
        s = make_session()
        st = self._pre(s)
        p = st.order_no
        s.broker.snapshot_missing.add(p)
        s.cancel_symbol_orders("2330", "unmarked")
        run_once(s)
        s._cancel_queue[p]["next_ts"] = ts_mod.time.time() + 100
        s.cancel_order_by_no(p)                                  # 撤單中 → 再入佇列,不 raise
        assert p in s._cancel_queue
        assert s._cancel_queue[p]["next_ts"] <= ts_mod.time.time() + 0.01
        assert s.order_log[p]["status"] == "pending"
        s.broker.snapshot_missing.clear()
        assert run_once(s)["cancelled"] == [p]

    def test_manual_cancel_not_found_queues_without_raise(self, clock):
        s = make_session()
        st = self._pre(s)
        p = st.order_no
        s.broker.snapshot_missing.add(p)
        s.cancel_order_by_no(p)
        assert s.order_log[p]["status"] == "pending" and p in s._cancel_queue
        assert st.order_no == p and st.stopped_reason == "manual_cancel"

    def test_manual_cancel_success_unchanged(self, clock):
        s = make_session()
        st = self._pre(s)
        p = st.order_no
        s.cancel_order_by_no(p)
        assert s.order_log[p]["status"] == "cancelled" and st.order_no == ""
        assert st.stopped_reason == "manual_cancel" and s.budget_used == 0

    def test_chase_extra_uses_queue(self, clock):
        # _chase_send_one 的管線多送 → 直接入佇列 (不再 thread-per-cancel 各自查一次)
        s, p, m1, extras = build_node3(n_extra=3)
        assert set(extras) == set(s._cancel_queue)
        assert not any(t.name.startswith("chase-xcancel") and t.is_alive()
                       for t in threading.enumerate())


# ═══ 併發: 23 筆多送 + breach cancel_all + 手動刪單 + worker 同跑,不死鎖 ═══

class TestConcurrency:
    def test_no_deadlock_under_concurrent_cancel_paths(self):
        s, p, m1, extras = build_node3(n_extra=23)
        errors = []

        def _guard(fn):
            try:
                fn()
            except ValueError:
                pass                                             # 已被別條路徑撤掉 → 不可刪,合理
            except Exception as e:                               # noqa: BLE001
                errors.append(e)

        def _manual():
            for no in extras[:5]:
                s.cancel_order_by_no(no)

        def _worker():
            for _ in range(5):
                run_once(s)
                _real_time.sleep(0.02)
        ths = [threading.Thread(target=_guard, args=(lambda: s.cancel_all_pending("budget_breached"),),
                                daemon=True, name="t-cancel-all"),
               threading.Thread(target=_guard, args=(_manual,), daemon=True, name="t-manual"),
               threading.Thread(target=_guard, args=(_worker,), daemon=True, name="t-worker")]
        for t in ths:
            t.start()
        for t in ths:
            t.join(timeout=20)
        assert not any(t.is_alive() for t in ths), "撤單路徑併發死鎖/逾時"
        assert errors == []
        run_once(s)
        assert s._cancel_queue == {}
        for no in extras:
            assert s.order_log[no]["status"] == "cancelled"
        assert s.order_log[m1]["status"] == "cancelled"          # breach 撤所有 pending 買單 (含第一筆 M)
        assert s.trades[SYM].order_no == "" and s.budget_used == 0
        cancelled = [c[0] for c in s.broker.cancelled]
        assert len(cancelled) == len(set(cancelled))             # 每筆恰撤一次
