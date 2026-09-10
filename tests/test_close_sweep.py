"""券商權威掃單 (2026-09-09 node3 事故 A4): 13:23/13:24 cancel_all_pending + 盤中低頻對帳。

原語: 一次 get_order_snapshot(),凡 user_def=="hitlimit" 且 Buy 且 status ∈ {0,4,8,10} 且
(after_qty is None or after_qty>filled_qty) 的委託 = 券商端仍 live;不在本地 pending/佇列者 →
CRITICAL「本地標 X 但券商仍 live」+ request_cancel (不撤賣單、不撤非 hitlimit;盤中不撤合法在途單)。
09-09 根因之三: order_log 被誤標 cancelled 後沒有任何以券商為權威的掃單 → 4 筆 live 一整天。
"""
import logging
import re

import pytest

import trading_session as ts_mod
from fakes_cancel import (FakeSnapshotBroker, install_fake_clock, today_at, make_session, run_once,
                          fill)


@pytest.fixture
def clock(monkeypatch):
    monkeypatch.setattr(ts_mod, "_CANCEL_DRAIN_MAX_SEC", 3.0, raising=False)   # 無法確認的單不拖 40 s
    return install_fake_clock(monkeypatch, today_at(13, 23, 5))


def _crit(caplog, needle=""):
    return [r.getMessage() for r in caplog.records
            if r.levelno == logging.CRITICAL and needle in r.getMessage()]


def _seed_scenarios(s):
    """(a)~(g) 六類快照 + 本地狀態。回 dict(name → order_no)。"""
    b = s.broker
    ids = {}
    # (a) 本地誤標 cancelled、券商 status 10 (09-09 KT08n/m/i/l 型)
    a = b.place_market_buy("AAAA", 1)
    s._log_order(a, "AAAA", "buy", "market_buy", 1, 0)
    s._mark_order(a, "cancelled")
    ids["a"] = a
    # (b) order_log 沒有的 hitlimit 買單 (重啟遺失)
    ids["b"] = b.add_order("B1", "BBBB", buy=True, lots=1, status="10")
    # (c) 本地只有 UNKNOWN- 書號 (place 回傳缺書號),券商端是真書號 C1
    s._log_order("UNKNOWN-1725000000-1", "CCCC", "buy", "market_buy", 1, 0)
    ids["c"] = b.add_order("C1", "CCCC", buy=True, lots=1, status="10")
    # (d) hitlimit 賣單 → 不撤
    ids["d"] = b.add_order("D1", "DDDD", buy=False, lots=1, status="10")
    # (e) user_def 非 hitlimit (手動單/他策略) → 不撤
    ids["e"] = b.add_order("E1", "EEEE", buy=True, lots=1, status="10", user_def="other")
    # (f) 已終結 status 30/50/90 → 不撤
    ids["f30"] = b.add_order("F30", "FFFF", buy=True, lots=1, status="30")
    ids["f50"] = b.add_order("F50", "FFFF", buy=True, lots=1, status="50", filled_lots=1)
    ids["f90"] = b.add_order("F90", "FFFF", buy=True, lots=1, status="90")
    # (g) status 10 但 after_qty == filled_qty (改量後已全成) → 不撤
    ids["g"] = b.add_order("G1", "GGGG", buy=True, lots=2, status="10", filled_lots=1, after_lots=1)
    return ids


class TestCloseSweep:
    def test_only_live_hitlimit_buys_are_cancelled(self, clock, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")
        s = make_session()
        b = s.broker
        ids = _seed_scenarios(s)
        s.cancel_all_pending("trading_end")
        cancelled = {c[0] for c in b.cancelled}
        assert cancelled == {ids["a"], "B1", "C1"}                # 只撤 a/b/c
        assert s.order_log[ids["a"]]["status"] == "cancelled"
        assert s.order_log["B1"]["status"] == "cancelled" and s.order_log["B1"]["kind"] == "orphan_buy"
        assert s.order_log["C1"]["status"] == "cancelled"
        for k in ("d", "e", "f30", "f50", "f90", "g"):
            assert ids[k] not in s.order_log                      # 不撤也不建列
        # UNKNOWN 書號永遠對不上快照 → 留 pending / unconfirmed,CRITICAL 點名
        u = s.order_log["UNKNOWN-1725000000-1"]
        assert u["status"] == "pending" and u["cancel_state"] == "unconfirmed"
        assert _crit(caplog, "UNKNOWN-1725000000-1")
        # 券商權威掃單逐筆 CRITICAL「本地標 X 但券商仍 live」
        live_msgs = _crit(caplog, "券商仍 live")
        assert any(ids["a"] in m and "本地標 cancelled" in m for m in live_msgs)
        assert any("B1" in m and "不存在" in m for m in live_msgs)
        assert any("C1" in m for m in live_msgs)
        assert not any(x in m for m in live_msgs for x in ("D1", "E1", "F30", "F50", "F90", "G1"))
        # 收盤摘要
        summary = [r.getMessage() for r in caplog.records if "收盤撤單" in r.getMessage()
                   and r.levelno == logging.WARNING]
        assert summary, caplog.text
        assert re.search(r"已確認 3 / 未確認 1 / order_log 外孤兒 2", summary[-1]), summary[-1]

    def test_local_pending_orders_go_through_queue_and_snapshot(self, clock):
        # 既有掃描 (st.order_no / 孤兒 P / 額外 M) 全改 request_cancel → 一次快照扇出撤
        s = make_session(total=10_000_000, per_symbol=0, sizing_mode="fixed_lots", fixed_lots=1)
        b = s.broker
        s.place_pre_orders(["5386", "2330"], {"5386": 283.0, "2330": 100.0})
        extra = b.place_market_buy("5386", 1)
        s._log_order(extra, "5386", "buy", "market_buy", 1, 0)
        b.snapshot_calls = 0
        s.cancel_all_pending("cancel_pending_time")
        assert {c[0] for c in b.cancelled} == {s.trades["5386"].pre_order_no,
                                                s.trades["2330"].pre_order_no, extra}
        assert all(r["status"] == "cancelled" for r in s.order_log.values())
        assert s.budget_used == 0 and s._cancel_queue == {}
        assert b.snapshot_calls <= 3                              # 掃單 1 + drain 1~2 (非逐筆查)
        assert b.cancel_by_obj_calls                              # 走 cancel_by_obj 扇出

    def test_missing_order_reported_unconfirmed_not_cancelled(self, clock, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")
        s = make_session()
        b = s.broker
        no = b.place_market_buy("5386", 1)
        s._log_order(no, "5386", "buy", "market_buy", 1, 0)
        b.snapshot_missing.add(no)
        s.cancel_all_pending("trading_end")
        row = s.order_log[no]
        assert row["status"] == "pending" and row["cancel_state"] == "unconfirmed"
        assert row["cancel_attempts"] >= 1
        assert _crit(caplog, no)
        assert re.search(r"已確認 0 / 未確認 1", caplog.text)
        assert s.status()["n_cancel_unconfirmed"] == 1
        # 之後後檯同步 → worker 補撤
        b.snapshot_missing.clear()
        clock.advance(10)
        assert run_once(s)["cancelled"] == [no]

    def test_sim_mode_with_orders_is_critical_not_silent(self, clock, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")
        s = make_session()
        no = s.broker.place_market_buy("5386", 1)
        s._log_order(no, "5386", "buy", "market_buy", 1, 0)
        s.set_mode("sim")
        s.cancel_all_pending("trading_end")
        msgs = _crit(caplog, "收盤撤單")
        assert msgs and "mode=sim" in msgs[-1] and "1 筆" in msgs[-1]
        assert s.broker.cancelled == []

    def test_no_broker_with_orders_is_critical(self, clock, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")
        s = make_session()
        no = s.broker.place_market_buy("5386", 1)
        s._log_order(no, "5386", "buy", "market_buy", 1, 0)
        s.broker = None
        s.cancel_all_pending("trading_end")
        assert _crit(caplog, "收盤撤單")

    def test_sim_mode_empty_log_silent(self, clock, caplog, tmp_path):
        caplog.set_level(logging.INFO, logger="trading_session")
        s = make_session()
        s._output_dir = tmp_path                                  # 當日 orders.csv 也沒有 → 才靜默
        s.set_mode("sim")
        s.cancel_all_pending("trading_end")
        assert not _crit(caplog)

    def test_sim_mode_empty_log_but_orders_csv_is_critical(self, clock, caplog, tmp_path):
        # 盤中重啟: order_log 空、mode 重置 sim、broker None,但 broker 落的當日 orders.csv 有列 → CRITICAL
        # (審查 #16: 計畫 A4(a)「當日 orders.csv 非空」,舊碼只看記憶體 order_log → 重啟後靜默)
        caplog.set_level(logging.INFO, logger="trading_session")
        s = make_session()
        s._output_dir = tmp_path
        (tmp_path / f"{ts_mod.datetime.now().strftime('%Y-%m-%d')}_orders.csv").write_text(
            "order_id,ts_sent,ts_accepted,latency_ms,action,symbol,lots,price_type,extra\n"
            "KT08n,2026-09-09T09:00:07,2026-09-09T09:00:11,3100,BUY,5386,1,Market,\n", encoding="utf-8")
        s.set_mode("sim")
        s.broker = None
        s.cancel_all_pending("trading_end")
        msgs = _crit(caplog, "收盤撤單")
        assert msgs and "orders.csv 1 列" in msgs[-1] and "重啟後 order_log 空" in msgs[-1]

    def test_sim_mode_header_only_csv_silent(self, clock, caplog, tmp_path):
        caplog.set_level(logging.INFO, logger="trading_session")
        s = make_session()
        s._output_dir = tmp_path
        (tmp_path / f"{ts_mod.datetime.now().strftime('%Y-%m-%d')}_orders.csv").write_text(
            "order_id,ts_sent,ts_accepted,latency_ms,action,symbol,lots,price_type,extra\n", encoding="utf-8")
        s.set_mode("sim")
        s.cancel_all_pending("trading_end")
        assert not _crit(caplog)

    def test_unhealthy_broker_drain_does_not_busy_loop(self, clock, monkeypatch):
        # 交易 WS 斷線 (healthy=False) 時收盤 drain 不可零延遲忙迴圈 (審查 #4/#13: 1 s 內 48 萬次 run_once)
        monkeypatch.setattr(ts_mod, "_CANCEL_DRAIN_MAX_SEC", 1.0, raising=False)
        s = make_session()
        b = s.broker
        no = b.place_market_buy("5386", 1)
        s._log_order(no, "5386", "buy", "market_buy", 1, 0)
        b.healthy = False
        calls = []
        orig = s._cancel_worker_run_once

        def counted(*a, **kw):
            calls.append(1)
            return orig(*a, **kw)
        monkeypatch.setattr(s, "_cancel_worker_run_once", counted)
        s.cancel_all_pending("trading_end")
        assert len(calls) <= 5, len(calls)
        assert s.order_log[no]["status"] == "pending" and no in s._cancel_queue
        assert s._cancel_queue[no]["attempts"] == 0              # 不計 attempts (不健康)

    def test_sweep_failure_retried_during_drain_cancels_orphan(self, clock, caplog):
        # 13:23 掃單快照撞流量控管 → drain 期間重試 → order_log 外孤兒 B1 仍被發現並撤掉 (審查 #19)
        caplog.set_level(logging.INFO, logger="trading_session")
        s = make_session()
        b = s.broker
        b.add_order("B1", "BBBB", buy=True, lots=1, status="10")
        b.fail_snapshot_times = 1
        s.cancel_all_pending("trading_end")
        assert "B1" in {c[0] for c in b.cancelled}
        assert s.order_log["B1"]["status"] == "cancelled" and s.order_log["B1"]["kind"] == "orphan_buy"
        assert re.search(r"order_log 外孤兒 1", caplog.text)
        assert re.search(r"掃單 ok/2 次", caplog.text)
        assert not _crit(caplog, "皆失敗")

    def test_sweep_never_succeeds_is_critical_in_summary(self, clock, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")
        s = make_session()
        s.broker.fail_snapshot_times = 99
        s.cancel_all_pending("trading_end")
        crit = _crit(caplog, "券商權威掃單")
        assert crit and "未檢查" in crit[-1]
        assert re.search(r"掃單 失敗/\d+ 次", caplog.text)

    def test_snapshot_failure_during_sweep_still_cancels_local_pending_later(self, clock, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")
        s = make_session()
        b = s.broker
        no = b.place_market_buy("5386", 1)
        s._log_order(no, "5386", "buy", "market_buy", 1, 0)
        b.fail_snapshot_times = 1                                 # 掃單那次快照被流量控管
        s.cancel_all_pending("trading_end")
        assert s.order_log[no]["status"] == "cancelled"           # drain 後續輪補到
        assert "流量控管" in caplog.text


class TestIntradaySweep:
    def test_dry_run_only_logs(self, clock, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")
        s = make_session()
        ids = _seed_scenarios(s)
        clock.advance(ts_mod._SWEEP_TERMINAL_GRACE_SEC + 1)      # (a) 剛誤標 → 過 replica 寬限才視為異常
        out = s.intraday_reconcile_once(dry_run=True)
        assert set(out["flagged"]) == {ids["a"], "B1", "C1"} and out["queued"] == []
        assert s._cancel_queue == {} and s.broker.cancelled == []
        assert s.order_log[ids["a"]]["status"] == "cancelled"     # dry-run 不動本地
        assert "B1" not in s.order_log
        assert all("dry-run" in m for m in _crit(caplog, "券商仍 live"))

    def test_real_run_queues_and_worker_cancels(self, clock, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")
        s = make_session()
        ids = _seed_scenarios(s)
        clock.advance(ts_mod._SWEEP_TERMINAL_GRACE_SEC + 1)
        out = s.intraday_reconcile_once(dry_run=False)
        assert set(out["queued"]) == {ids["a"], "B1", "C1"}
        assert s.order_log[ids["a"]]["status"] == "pending"       # 券商為權威 → 翻回 pending 讓撤單接手
        assert s.order_log["B1"]["status"] == "pending" and s.order_log["B1"]["kind"] == "orphan_buy"
        res = run_once(s)
        assert set(res["cancelled"]) == {ids["a"], "B1", "C1"}
        assert {c[0] for c in s.broker.cancelled} == {ids["a"], "B1", "C1"}

    def test_protects_active_orders_intraday_but_not_at_close(self, clock):
        # 盤中: st.order_no / pre_order_no 合法在途 (即使 row 狀態異常) 不撤;收盤掃單則一律撤
        s = make_session()
        b = s.broker
        s.place_pre_orders(["2330"], {"2330": 100.0})
        p = s.trades["2330"].order_no
        s._mark_order(p, "cancelled")                             # 本地誤標,但 st.order_no 仍指向 P
        b.add_order("B1", "BBBB", buy=True, lots=1, status="10")
        out = s.intraday_reconcile_once(dry_run=False)
        assert out["queued"] == ["B1"] and p not in s._cancel_queue
        assert s.order_log[p]["status"] == "cancelled"            # 盤中不動合法在途單
        s.cancel_all_pending("trading_end")
        assert p in {c[0] for c in b.cancelled}                   # 收盤: 券商 live 的 hitlimit 買單全撤

    def test_local_pending_not_flagged(self, clock, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")
        s = make_session()
        s.place_pre_orders(["2330"], {"2330": 100.0})
        out = s.intraday_reconcile_once(dry_run=False)
        assert out["flagged"] == [] and out["live"] == [s.trades["2330"].order_no]
        assert not _crit(caplog)

    def test_queued_order_not_double_flagged(self, clock):
        s = make_session()
        b = s.broker
        no = b.place_market_buy("5386", 1)
        s._log_order(no, "5386", "buy", "market_buy", 1, 0)
        s.request_cancel(no, "5386", "chase_extra")
        out = s.intraday_reconcile_once(dry_run=False)
        assert out["flagged"] == []

    def test_dry_run_env_default_true(self, monkeypatch):
        monkeypatch.delenv("INTRADAY_SWEEP_DRY_RUN", raising=False)
        assert ts_mod._intraday_sweep_dry_run() is True
        monkeypatch.setenv("INTRADAY_SWEEP_DRY_RUN", "false")
        assert ts_mod._intraday_sweep_dry_run() is False
        monkeypatch.setenv("INTRADAY_SWEEP_DRY_RUN", "0")
        assert ts_mod._intraday_sweep_dry_run() is False

    def test_not_manageable_returns_empty(self, clock):
        s = make_session()
        s.set_mode("sim")
        out = s.intraday_reconcile_once(dry_run=False)
        assert out["ok"] is False and out["flagged"] == []

    def test_recently_terminated_row_skipped_then_flagged(self, clock, caplog):
        # 本地剛由回報標 filled (<2 s)、快照 replica 仍 status 10/filled 0 → 本輪略過 (不假 CRITICAL、
        # 不翻回 pending);2 s 後仍 live → 才視為異常 (審查 #17)
        caplog.set_level(logging.INFO, logger="trading_session")
        s = make_session()
        b = s.broker
        no = b.place_market_buy("5386", 1)
        s._log_order(no, "5386", "buy", "market_buy", 1, 0)
        s._on_fill(fill(no, "5386", 1, 283.0))                   # 回報先到 → filled (快照 replica 未同步)
        assert s.order_log[no]["status"] == "filled"
        out = s.intraday_reconcile_once(dry_run=False)
        assert out["flagged"] == [] and s.order_log[no]["status"] == "filled" and not _crit(caplog)
        clock.advance(3.0)
        out = s.intraday_reconcile_once(dry_run=False)
        assert out["flagged"] == [no]                             # 寬限過後仍 live → 異常,照舊流程

    def test_close_sweep_has_no_grace_window(self, clock):
        # 收盤掃單只跑一次 → 剛誤標 cancelled 的列 (09-09 型) 不留寬限,立刻撤
        s = make_session()
        b = s.broker
        no = b.place_market_buy("5386", 1)
        s._log_order(no, "5386", "buy", "market_buy", 1, 0)
        s._mark_order(no, "cancelled")
        s.cancel_all_pending("trading_end")
        assert no in {c[0] for c in b.cancelled} and s.order_log[no]["status"] == "cancelled"

    def test_after_qty_none_counts_as_live(self, clock):
        s = make_session()
        s.broker.add_order("N1", "NNNN", buy=True, lots=1, status="10")
        s.broker.snapshot_after_qty["N1"] = None
        out = s.intraday_reconcile_once(dry_run=True)
        assert out["flagged"] == ["N1"]

    def test_fill_after_orphan_row_created_is_accounted(self, clock):
        # 孤兒建列後成交回報到 → order_no ∈ order_log → 不再被當「非策略單」忽略
        s = make_session()
        s.broker.add_order("B1", "BBBB", buy=True, lots=1, status="10")
        s.intraday_reconcile_once(dry_run=False)
        s._on_fill(fill("B1", "BBBB", 1, 50.0))
        assert s.order_log["B1"]["filled_lots"] == 1 and s.order_log["B1"]["status"] == "filled"
