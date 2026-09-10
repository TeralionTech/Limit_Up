"""出場與撤單佇列的連鎖 (2026-09-09 事故 A4b;3587 同型漏洞):
  - 管線多送的 M 查無 (佇列中、券商端其實 live) → 出場照樣等窗口、賣已成交張數;row 維持 pending;事後撤到
  - 出場後那筆 M 晚成交 → _on_fill 對**這批**張數再賣一次 (不讀 st.filled_lots 免超賣)
  - 出場 worker 進行中 (窗口內) 的成交由 worker 一次賣掉,不重複
"""
import threading
import time as _real_time

import pytest

import trading_session as ts_mod
from fakes_cancel import make_session, run_once, fill, wait_until, MSG_FILLED

SYM = "5386"
LU = 283.0
DOWN = 255.0


def _sells(b):
    return [c for c in b.placed if c[0] in ("limit_sell", "market_sell")]


def _setup(n_extra=1):
    """P → 第一筆 M (撤 P) → n_extra 筆多送 M (入佇列);回 (s, p, m1, extras)。"""
    s = make_session(total=10_000_000, per_symbol=0, sizing_mode="fixed_lots", fixed_lots=1)
    s.set_limit_downs({SYM: DOWN})
    s.place_pre_orders([SYM], {SYM: LU})
    st = s.trades[SYM]
    p = st.pre_order_no
    assert s._chase_send_one(SYM, 1) == "accepted"
    m1 = st.order_no
    extras = []
    for _ in range(n_extra):
        before = set(s.order_log)
        assert s._chase_send_one(SYM, 1) == "accepted_extra"
        extras.append((set(s.order_log) - before).pop())
    return s, p, m1, extras


class TestStrayMissingThenExit:
    def test_exit_sells_filled_keeps_stray_pending_then_cancels_later(self, monkeypatch):
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p, m1, (mx,) = _setup()
        b = s.broker
        st = s.trades[SYM]
        b.snapshot_missing.add(mx)                               # 多送的 M 快照查無 (後檯延遲;券商端其實 live)
        s._on_fill(fill(m1, SYM, 1, LU))                         # 第一筆 M 成交 1 張 = target
        assert st.order_status == "done" and st.order_no == ""
        t0 = _real_time.perf_counter()
        s._exit_worker(SYM, "mkt_queue_gone")
        # had_stray → 等窗口 (perf_counter + 放寬: Windows sleep(0.1) 受 15.6 ms 時鐘解析度影響可提早醒)
        assert _real_time.perf_counter() - t0 >= 0.08
        assert _sells(b) == [("limit_sell", SYM, DOWN, 1)]       # 賣已成交 1 張
        assert st.exited is True
        row = s.order_log[mx]
        assert row["status"] == "pending" and row["cancel_state"] in ("queued", "unconfirmed")
        assert mx in s._cancel_queue
        assert st.last_buy_cancel_ts > 0
        # 事後快照同步 → worker 撤到
        b.snapshot_missing.clear()
        out = run_once(s, now=ts_mod.time.time() + 10)
        assert mx in out["cancelled"] and s.order_log[mx]["status"] == "cancelled"
        assert s._cancel_queue == {}

    def test_late_fill_on_stray_after_exit_sells_delta(self, monkeypatch):
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p, m1, (mx,) = _setup()
        b = s.broker
        st = s.trades[SYM]
        s.order_log[mx]["status"] = "pending"
        b.cancelled_nos.discard(mx)
        b.snapshot_missing.add(mx)
        s._on_fill(fill(m1, SYM, 1, LU))
        s._exit_worker(SYM, "mkt_queue_gone")
        assert _sells(b) == [("limit_sell", SYM, DOWN, 1)]
        # 出場後,查無的那筆 M 晚成交 1 張 → 沒有出場保護 → 對這 1 張再賣一次
        s._on_fill(fill(mx, SYM, 1, LU, filled_no="LATE"))
        assert wait_until(lambda: len(_sells(b)) == 2, 3.0), "出場後晚成交沒有再賣"
        assert _sells(b)[-1] == ("limit_sell", SYM, DOWN, 1)     # 只賣這批 1 張 (不是 st.filled_lots=2)
        assert st.filled_lots == 2 and st.exited is True
        assert s.order_log[mx]["status"] == "filled"
        # 佇列: row 已終結 → worker 出列,不再撤
        out = run_once(s, now=ts_mod.time.time() + 10)
        assert mx not in s._cancel_queue and mx not in out["cancelled"]

    def test_late_fill_on_orphan_pre_after_exit_sells_delta(self, monkeypatch):
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p, m1, extras = _setup(n_extra=0)
        b = s.broker
        st = s.trades[SYM]
        s._on_fill(fill(m1, SYM, 1, LU))
        s._exit_worker(SYM, "mkt_queue_gone")
        assert len(_sells(b)) == 1
        # P 已標 cancelled 但券商端其實成交 (撤單/成交競態) → 晚回報 → 再賣 1
        s._on_fill(fill(p, SYM, 1, LU, filled_no="LATE-P"))
        assert wait_until(lambda: len(_sells(b)) == 2, 3.0)
        assert _sells(b)[-1][3] == 1

    def test_fill_inside_exit_window_sold_once_by_worker(self, monkeypatch):
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.6)
        s, p, m1, (mx,) = _setup()
        b = s.broker
        st = s.trades[SYM]
        s.order_log[mx]["status"] = "pending"
        b.cancelled_nos.discard(mx)
        b.snapshot_missing.add(mx)
        s._on_fill(fill(m1, SYM, 1, LU))
        threading.Timer(0.15, lambda: s._on_fill(fill(mx, SYM, 1, LU, filled_no="IN-WIN"))).start()
        s._exit_worker(SYM, "mkt_queue_gone")
        assert _sells(b) == [("limit_sell", SYM, DOWN, 2)]       # 窗口內落地 → 一次賣 2
        _real_time.sleep(0.3)
        assert len(_sells(b)) == 1                               # 不再另起 late-fill 賣
        assert st.filled_lots == 2

    def test_late_fill_dedup_no_double_sell(self, monkeypatch):
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p, m1, (mx,) = _setup()
        b = s.broker
        s.order_log[mx]["status"] = "pending"
        b.cancelled_nos.discard(mx)
        b.snapshot_missing.add(mx)
        s._on_fill(fill(m1, SYM, 1, LU))
        s._exit_worker(SYM, "mkt_queue_gone")
        f = fill(mx, SYM, 1, LU, filled_no="LATE")
        s._on_fill(f)
        s._on_fill(dict(f))                                      # 重複回報 → 去重
        assert wait_until(lambda: len(_sells(b)) == 2, 3.0)
        _real_time.sleep(0.3)
        assert len(_sells(b)) == 2

    def test_late_fill_when_broker_gone_is_critical_not_crash(self, monkeypatch, caplog):
        import logging
        caplog.set_level(logging.INFO, logger="trading_session")
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p, m1, (mx,) = _setup()
        b = s.broker
        s.order_log[mx]["status"] = "pending"
        b.cancelled_nos.discard(mx)
        b.snapshot_missing.add(mx)
        s._on_fill(fill(m1, SYM, 1, LU))
        s._exit_worker(SYM, "mkt_queue_gone")
        s.broker = None
        s._on_fill(fill(mx, SYM, 1, LU, filled_no="LATE"))
        assert wait_until(lambda: any(r.levelno == logging.CRITICAL and "晚成交" in r.getMessage()
                                      for r in caplog.records), 3.0)
        assert s.trades[SYM].filled_lots == 2


class TestLateFillFromAuthoritativeSources:
    """出場後才由券商權威 (撤單前已成交 / 補收對帳) 補進的買進,也要再賣 (審查 #14):
    舊碼只補 st.filled_lots 不賣,之後真回報被單筆封頂成 0 → 永遠不賣、也不進隔日賣清單 (隱形裸部位)。"""

    def test_filled_before_cancel_after_exit_triggers_late_sell(self, monkeypatch):
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p, m1, (mx,) = _setup()
        b = s.broker
        st = s.trades[SYM]
        b.snapshot_missing.add(mx)                               # 出場時多送 M 查無 → 留佇列
        s._on_fill(fill(m1, SYM, 1, LU))
        s._exit_worker(SYM, "mkt_queue_gone")
        assert _sells(b) == [("limit_sell", SYM, DOWN, 1)] and st.exited
        # 出場後 worker 再撤: 快照回來了但已成交 → 撤單被拒 + 快照 filled_qty 1 張 → 補成交 + 對這 1 張再賣
        b.snapshot_missing.clear()
        b.cancel_fail_msg[mx] = MSG_FILLED
        b.snapshot_filled[mx] = 1000
        out = run_once(s, now=ts_mod.time.time() + 10)
        assert mx in out["filled_before_cancel"]
        assert wait_until(lambda: len(_sells(b)) == 2, 3.0), "撤單前已成交補入後沒有再賣"
        assert _sells(b)[-1] == ("limit_sell", SYM, DOWN, 1) and st.filled_lots == 2
        # 之後真成交回報晚到 → 單筆封頂 0 → 不再賣第三次
        s._on_fill(fill(mx, SYM, 1, LU, filled_no="LATE"))
        _real_time.sleep(0.3)
        assert len(_sells(b)) == 2 and st.filled_lots == 2

    def test_reconcile_after_exit_triggers_late_sell(self, monkeypatch):
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p, m1, (mx,) = _setup()
        b = s.broker
        st = s.trades[SYM]
        b.snapshot_missing.add(mx)
        s._on_fill(fill(m1, SYM, 1, LU))
        s._exit_worker(SYM, "mkt_queue_gone")
        assert len(_sells(b)) == 1
        b.filled_map = {mx: 1}                                   # 斷線期間 M2 成交、回報遺失 → 重連補收
        s.reconcile_orders()
        assert wait_until(lambda: len(_sells(b)) == 2, 3.0), "補收對帳補進出場後成交沒有再賣"
        assert _sells(b)[-1][3] == 1 and st.filled_lots == 2


class TestNoOversellWithLiveLateFillSell:
    def test_main_sell_failure_then_retrigger_only_sells_uncovered(self, monkeypatch):
        """主出場賣單暫時失敗期間多送 M 晚成交 → late-fill 賣 1 張已掛;主 worker 用盡重試 → exited 回退 →
        再觸發出場只賣未被在途賣單覆蓋的 1 張 (不是 filled_lots=2) → 總賣出 == 持有 (審查 #15)。"""
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p, m1, (mx,) = _setup()
        b = s.broker
        st = s.trades[SYM]
        s._on_fill(fill(m1, SYM, 1, LU))
        orig = b.place_limit_sell
        gate = threading.Event()

        def sell(symbol, price, lots, reason=""):
            if reason == "exit1":                                # 主出場賣單: 等測試放行後失敗 (暫時性錯)
                gate.wait(3.0)
                raise RuntimeError("temporary")
            return orig(symbol, price, lots, reason)
        b.place_limit_sell = sell
        t = threading.Thread(target=s._exit_worker, args=(SYM, "exit1"), daemon=True)
        t.start()
        assert wait_until(lambda: st.exited and not st.exit_in_progress, 3.0)   # 主 worker 已讀張數、正在賣
        s._on_fill(fill(mx, SYM, 1, LU, filled_no="LATE"))                     # 出場後晚成交 1 張
        assert wait_until(lambda: len(_sells(b)) == 1, 3.0)                    # late-fill 賣 1 張已掛
        gate.set()
        t.join(10)
        assert not t.is_alive() and st.exited is False and st.sell_failed is True
        assert st.filled_lots == 2
        s._exit_worker(SYM, "exit2")                                            # 再觸發出場
        sells = _sells(b)
        assert sum(x[3] for x in sells) == 2, sells                            # 總賣出 == 持有,不超賣
        assert sells[-1][3] == 1 and st.exited is True

    def test_fully_covered_position_does_not_reset_exited(self, monkeypatch):
        # 持倉已全數被在途賣單覆蓋 → 再觸發出場不賣、也不把 exited 回退 (否則每 tick 重觸發)
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p, m1, extras = _setup(n_extra=0)
        b = s.broker
        st = s.trades[SYM]
        s._on_fill(fill(m1, SYM, 1, LU))
        s._exit_worker(SYM, "mkt_queue_gone")
        assert len(_sells(b)) == 1 and st.exited
        st.exited = False                                        # 模擬回退
        s._exit_worker(SYM, "again")
        assert len(_sells(b)) == 1 and st.exited is True


class TestManualAbandonUnaffected:
    def test_abandoned_symbol_late_fill_not_auto_sold(self, monkeypatch):
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p, m1, (mx,) = _setup()
        b = s.broker
        s.abandon_symbol(SYM)                                    # 使用者取消追蹤 → 一切自動化停止
        s._on_fill(fill(mx, SYM, 1, LU, filled_no="LATE"))
        _real_time.sleep(0.3)
        assert _sells(b) == []
