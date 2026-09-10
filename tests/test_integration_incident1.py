"""整合補強 (2026-09-09 事故 1 整合階段;三位實作者契約之外的縫隙):

1. A4b(b) 隔早 refresh_overnight_inventory: **庫存有、隔日賣清單沒有** → CRITICAL 點名 (不自動加入);
   清單為空也照查 (空清單正是「檔案漏掉」最需要對帳的情況)。
2. _confirm_cancelled 冪等: 同步撤成後又收到 ft30 status 30 回報 (或反之) → 不重複計數/告警,
   但 _close_cancel 仍依本次 reason 補齊 (overnight_skip 的 sell_placed 解除不能漏)。
3. 撤單 worker 隨 broker 連線啟動 (_ensure_cancel_worker) — 盤中對帳不依賴當天有無撤單請求。
"""
import logging
import threading
import time as _real_time

import pytest

import trading_session as ts_mod
from fakes_cancel import FakeSnapshotBroker, make_session, run_once, wait_until, fill


def _crits(caplog):
    return [r.getMessage() for r in caplog.records if r.levelno == logging.CRITICAL]


def _report(order_no, symbol, status, ft=30, err=""):
    return {"order_no": order_no, "symbol": symbol, "status": status, "filled_qty": 0,
            "error_message": err, "function_type": ft, "last_time": ""}


# ═══ 1. A4b(b): 庫存有、檔案沒有 ═══

class TestInventoryNotInOvernightFile:
    def test_inventory_without_file_entry_is_critical_and_not_auto_added(self, caplog):
        b = FakeSnapshotBroker()
        b.inventories = [{"symbol": "5386", "lots": 1}, {"symbol": "2330", "lots": 2}]
        s = make_session(broker=b)
        s.load_overnight([{"symbol": "2330", "lots": 2, "avg_cost": 100.0}])
        with caplog.at_level(logging.INFO, logger="trading_session"):
            s.refresh_overnight_inventory()
        crit = _crits(caplog)
        assert any("5386 1 張" in m and "隔日賣清單沒有" in m for m in crit)
        assert not any("2330" in m for m in crit)
        assert "5386" not in s.overnight                      # 只點名,不自動加入
        assert s.overnight["2330"]["reconciled"] is True      # 既有對帳行為不變
        assert s.overnight["2330"]["lots"] == 2

    def test_empty_overnight_list_still_reconciles(self, caplog):
        b = FakeSnapshotBroker()
        b.inventories = [{"symbol": "5386", "lots": 1}]
        s = make_session(broker=b)
        assert s.overnight == {}
        with caplog.at_level(logging.INFO, logger="trading_session"):
            s.refresh_overnight_inventory()
        assert any("5386 1 張" in m for m in _crits(caplog))

    def test_consistent_or_zero_lots_no_critical(self, caplog):
        b = FakeSnapshotBroker()
        b.inventories = [{"symbol": "2330", "lots": 2}, {"symbol": "0000", "lots": 0}]
        s = make_session(broker=b)
        s.load_overnight([{"symbol": "2330", "lots": 2, "avg_cost": 100.0}])
        with caplog.at_level(logging.INFO, logger="trading_session"):
            s.refresh_overnight_inventory()
        assert _crits(caplog) == []

    def test_broker_not_ready_skips(self, caplog):
        b = FakeSnapshotBroker()
        b.inventories = [{"symbol": "5386", "lots": 1}]
        s = make_session(broker=b)
        b.connected = False
        with caplog.at_level(logging.INFO, logger="trading_session"):
            s.refresh_overnight_inventory()
        assert _crits(caplog) == []

    def test_todays_strategy_position_not_flagged(self, caplog):
        # 今日策略自己買到的部位 (trades 有成交) 在庫存裡、不在隔日賣清單 → 不是遺漏,不 CRITICAL
        # (審查 #18: 盤中重連 / 手動 add_overnight 也呼叫這裡,每檔今日持股一條假警報稀釋真訊號)
        b = FakeSnapshotBroker()
        b.inventories = [{"symbol": "2330", "lots": 1}, {"symbol": "5386", "lots": 1}]
        s = make_session(broker=b)
        s.place_pre_orders(["2330"], {"2330": 100.0})
        s._on_fill(fill(s.trades["2330"].order_no, "2330", 1, 100.0))
        with caplog.at_level(logging.INFO, logger="trading_session"):
            s.refresh_overnight_inventory()
        crit = _crits(caplog)
        assert any("5386 1 張" in m for m in crit)
        assert not any("2330" in m for m in crit)


# ═══ 2. _confirm_cancelled 冪等 ═══

class TestConfirmCancelledIdempotent:
    def test_report_after_sync_confirm_not_double_counted(self, caplog):
        s = make_session()
        no = s.broker.place_market_buy("2330", 1)
        s._log_order(no, "2330", "buy", "market_buy", 1, 0)
        s.request_cancel(no, "2330", "chase_extra")
        assert run_once(s)["cancelled"] == [no]
        assert s._cancel_stats["confirmed"] == 1
        caplog.clear()                                         # 只看重複回報那段的 log
        with caplog.at_level(logging.INFO, logger="trading_session"):
            s._on_order(_report(no, "2330", "30"))            # 券商撤單確認回報晚到
        assert s._cancel_stats["confirmed"] == 1               # 不雙算
        assert s.order_log[no]["status"] == "cancelled"
        assert no not in s._cancel_queue
        assert not any(r.levelno == logging.WARNING and "撤單確認" in r.getMessage()
                       for r in caplog.records)                # 重複確認不再 WARNING

    def test_late_confirm_hook_fires_once_for_duplicate(self, monkeypatch):
        from datetime import time as dtime
        from fakes_cancel import install_fake_clock, today_at
        install_fake_clock(monkeypatch, today_at(13, 30, 0))
        s = make_session()
        s.trading_end_time = dtime(13, 24, 0)
        calls = []
        s.on_late_confirm = lambda: calls.append(1)
        no = s.broker.place_market_buy("2330", 1)
        s._log_order(no, "2330", "buy", "market_buy", 1, 0)
        s.request_cancel(no, "2330", "trading_end")
        assert run_once(s)["cancelled"] == [no]
        s._on_order(_report(no, "2330", "30"))
        assert calls == [1]

    def test_overnight_sell_cancel_report_frees_slot_regardless_of_reason(self):
        """券商回報 30 先到 (reason=broker_report) → 認單不認 reason: sell_placed/sell_order_no 立刻釋放
        (審查 #3/#8: 舊碼只認 overnight_skip → 槽位卡 True 指向已撤單、整天不再賣);非使用者刪單 → 不 skip。
        之後 set_overnight_skip 沒有賣單可撤,只設 skip;確認只算一次。"""
        s = make_session()
        s.load_overnight([{"symbol": "9999", "lots": 1, "avg_cost": 10.0}])
        no = s.broker.place_limit_sell("9999", 9.0, 1, "overnight_sell")
        s._log_order(no, "9999", "sell", "overnight_sell", 1, 9.0)
        o = s.overnight["9999"]
        o["sell_placed"], o["sell_order_no"] = True, no
        s._on_order(_report(no, "9999", "30"))
        assert s.order_log[no]["status"] == "cancelled"
        assert o["sell_placed"] is False and o["sell_order_no"] == ""   # 槽位釋放 (不看 reason)
        assert o["skip"] is False                                      # 規則再觸發會重掛
        s.set_overnight_skip("9999", True)
        assert o["skip"] is True and o["sell_placed"] is False
        assert s._cancel_stats["confirmed"] == 1

    def test_unknown_order_report_is_harmless(self, caplog):
        s = make_session()
        with caplog.at_level(logging.INFO, logger="trading_session"):
            s._on_order(_report("ZZZ", "2330", "30"))
        assert s._cancel_stats["confirmed"] == 0
        assert "ZZZ" not in s.order_log


# ═══ 3. worker 隨連線啟動 ═══

class TestWorkerStartsOnConnect:
    def test_connect_async_calls_ensure_cancel_worker(self, monkeypatch, tmp_path):
        import broker as broker_mod

        class _FakeClient:
            def __init__(self, log_path):
                self.on_fill = self.on_order = self.on_disconnect = self.on_reconnected = None
                self.connected = True
                self.healthy = True

            def connect(self, *a, **kw):
                return None

            def disconnect(self):
                return None

            def get_inventories(self):
                return []

        monkeypatch.setattr(broker_mod, "RealOrderClient", _FakeClient)
        s = ts_mod.TradingSession(auto_cancel_worker=False)   # 不真的起 thread,只驗有被叫到
        s.set_mode("real")
        calls = []
        monkeypatch.setattr(s, "_ensure_cancel_worker", lambda: calls.append(threading.current_thread().name))
        s.connect_async("A", "p", "x.pfx", "", False, tmp_path)
        assert wait_until(lambda: not s.connecting, timeout=5.0)
        assert s.connect_error == ""
        assert calls == ["broker-connect"]
        assert isinstance(s.broker, _FakeClient)

    def test_ensure_cancel_worker_noop_in_sim(self):
        s = ts_mod.TradingSession(auto_cancel_worker=True)
        s.set_mode("sim")
        s.broker = FakeSnapshotBroker()
        s._ensure_cancel_worker()
        assert s._cancel_worker_alive() is False

    def test_set_armed_starts_worker_when_connected_in_sim_first(self):
        # 先在 sim 連線 (connect 時不起 worker) → 切 real → arm → worker 必須起來,否則盤中對帳整天不跑 (審查 #7)
        s = ts_mod.TradingSession(auto_cancel_worker=True)
        s.set_mode("sim")
        s.broker = FakeSnapshotBroker()
        s._ensure_cancel_worker()
        assert s._cancel_worker_alive() is False
        s.set_mode("real")
        s.set_params(total_budget=1_000_000, per_symbol_budget=200_000)
        s.set_armed(True)
        assert s._cancel_worker_alive() is True
        assert s.status()["cancel_worker_alive"] is True


# ═══ 4. 隔日賣單被撤 → 槽位釋放 (認單不認 reason;審查 #3/#8) ═══

class TestOvernightSellSlotFreedOnCancel:
    def _overnight_ready(self, lots=1):
        b = FakeSnapshotBroker()
        b.inventories = [{"symbol": "9999", "lots": lots, "order_type": "Stock"}]
        s = make_session(broker=b)
        s.set_limit_downs({"9999": 81.0})
        s.set_overnight_limit_ups({"9999": 99.0})
        s.load_overnight([{"symbol": "9999", "lots": lots, "avg_cost": 90.0}])
        s.refresh_overnight_inventory()
        assert s.overnight["9999"]["reconciled"] is True
        return s, b

    @staticmethod
    def _open_book(s):
        # 委買一 90 < 漲停 99、無市價列 → 打開 → 觸發隔日賣
        s.update_overnight_book("9999", 90.0, 90.5, mkt_bid_size=0, limit_bid1_price=90.0)

    @staticmethod
    def _n_sells(b):
        return len([c for c in b.placed if c[0] == "limit_sell"])

    def test_manual_delete_sets_skip_and_resume_resells(self):
        s, b = self._overnight_ready()
        o = s.overnight["9999"]
        self._open_book(s)
        assert wait_until(lambda: o["sell_order_no"] != "", 3.0)
        first = o["sell_order_no"]
        s.cancel_order_by_no(first)                              # 使用者在委託表右鍵刪隔日賣單
        assert s.order_log[first]["status"] == "cancelled"
        assert o["sell_placed"] is False and o["sell_order_no"] == "" and o["skip"] is True
        self._open_book(s)                                       # skip 中 → 不重掛
        _real_time.sleep(0.2)
        assert self._n_sells(b) == 1
        s.set_overnight_skip("9999", False)                      # 恢復賣出 → 下一 tick 重掛
        self._open_book(s)
        assert wait_until(lambda: self._n_sells(b) == 2, 3.0)
        assert o["sell_placed"] is True and o["sell_order_no"] not in ("", first)

    def test_broker_report_cancel_resells_on_next_tick(self):
        s, b = self._overnight_ready()
        o = s.overnight["9999"]
        self._open_book(s)
        assert wait_until(lambda: o["sell_order_no"] != "", 3.0)
        first = o["sell_order_no"]
        s._on_order(_report(first, "9999", "30"))                # 券商端撤掉 (回報先到、佇列無 reason)
        assert o["sell_placed"] is False and o["skip"] is False and s.is_overnight("9999")
        self._open_book(s)
        assert wait_until(lambda: self._n_sells(b) == 2, 3.0)
