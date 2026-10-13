"""「開始隔日賣」(2026-10-14): set_armed(True, overnight_only=True) = armed 但不搶今天的單。

- 不要求預算 pre-flight;仍要真實模式 + 券商連線 + 本節點帳號。
- 新進場路徑 (08:59:58 預掛 / 08:59:59 盲送 cadence / _chase_send_one) 一律不送;預掛的今日額度不燒掉。
- 隔日賣 (update_overnight_book → _overnight_sell_worker) 與既有部位的管理路徑照常 (看 is_live)。
- roll_day / 停止交易 重設;再按「開始交易」= 正常進場 (要過預算 pre-flight)。
"""
import time
from datetime import datetime, timedelta
from datetime import time as _t
from types import SimpleNamespace

import pytest

from fakes_cancel import FakeSnapshotBroker, make_session
from trading_session import TradingSession

LIMIT = 45.0


class InvBroker(FakeSnapshotBroker):
    def __init__(self, inv=None):
        super().__init__()
        self.inventories = inv or []

    def get_inventories(self):
        return self.inventories


def _buys(b):
    return [p for p in b.placed if p[0] in ("limit_buy", "market_buy")]


def _sells(b):
    return [p for p in b.placed if p[0] in ("limit_sell", "market_sell")]


def _wait(cond, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(0.02)
    return bool(cond())


def _overnight_only(broker=None, total=1_000_000, per_symbol=200_000):
    """正常 arm 的 session → 停止 → 以隔日賣模式重新 arm。"""
    s = make_session(broker=broker, total=total, per_symbol=per_symbol)
    s.set_armed(False)
    s.set_armed(True, overnight_only=True)
    return s


def _overnight_session(broker, lots=5, limit_up=55.0, limit_down=45.5):
    s = _overnight_only(broker=broker)
    s.load_overnight([{"symbol": "9999", "lots": lots, "avg_cost": 50.0}])
    s.overnight["9999"]["reconciled"] = True
    s.set_limit_downs({"9999": limit_down})
    s.set_overnight_limit_ups({"9999": limit_up})
    return s


class TestArming:
    def test_overnight_only_does_not_need_budget(self):
        s = TradingSession(auto_cancel_worker=False)
        s.auto_cancel_worker = False
        s.roll_day("2026-10-14")
        s.set_mode("real")
        s.broker = FakeSnapshotBroker()
        with pytest.raises(RuntimeError):          # 總預算 0 → 正常「開始交易」被拒
            s.set_armed(True)
        s.set_armed(True, overnight_only=True)     # 隔日賣模式不看預算
        assert s.armed is True
        assert s.is_live() is True                 # 隔日賣 / 出場 / 撤單的閘門開著
        assert s.entry_allowed() is False          # 新進場的閘門關著
        st = s.status()
        assert st["arm_mode"] == "overnight_only" and st["entry_enabled"] is False and st["armed"] is True

    def test_still_requires_real_mode_and_connection(self):
        s = TradingSession(auto_cancel_worker=False)
        s.roll_day("2026-10-14")
        with pytest.raises(RuntimeError):          # 模擬模式
            s.set_armed(True, overnight_only=True)
        s.set_mode("real")
        with pytest.raises(RuntimeError):          # 未連線
            s.set_armed(True, overnight_only=True)
        assert s.armed is False and s.entry_enabled is True

    def test_disarm_clears_and_normal_arm_restores_entry(self):
        s = _overnight_only()
        s.set_armed(False)
        assert s.armed is False and s.entry_enabled is True
        assert s.status()["arm_mode"] == "off"
        s.set_armed(True)                          # 「開始交易」= 正常進場
        assert s.entry_allowed() is True and s.status()["arm_mode"] == "trade"

    def test_switch_to_trade_requires_budget_and_keeps_mode_on_failure(self):
        s = _overnight_only()
        s.set_params(total_budget=0)
        with pytest.raises(RuntimeError):
            s.set_armed(True)
        assert s.armed is True and s.entry_enabled is False   # pre-flight 失敗不動現狀

    def test_roll_day_resets_mode(self):
        s = _overnight_only()
        s.roll_day("2026-10-15")
        assert s.armed is False and s.entry_enabled is True

    def test_switching_mid_day_leaves_broker_orders_alone(self):
        # 正常 arm + 預掛後切成隔日賣模式: 只停之後的送單,券商端的 P 不撤 (只 WARNING 點名)
        s = make_session()
        s.place_pre_orders(["1101"], {"1101": LIMIT})
        assert len(_buys(s.broker)) == 1
        s.set_armed(True, overnight_only=True)
        assert s.broker.cancelled == []
        assert s.trades["1101"].order_status == "pending"
        assert s.status()["arm_mode"] == "overnight_only"


class TestNoNewEntries:
    def test_pre_orders_skipped_and_daily_slot_not_burned(self):
        s = _overnight_only()
        s.place_pre_orders(["1101", "2330"], {"1101": LIMIT, "2330": 100.0})
        assert _buys(s.broker) == []
        assert s.budget_used == 0 and s.trades == {}
        assert s._pre_orders_date != s._trade_date   # 今日額度沒燒掉
        s.set_armed(True)                            # 改為搶單 → 預掛照常可做
        s.place_pre_orders(["1101"], {"1101": LIMIT})
        assert [p[:2] for p in _buys(s.broker)] == [("limit_buy", "1101")]

    def test_chase_send_one_is_stopped_without_state(self):
        s = _overnight_only()
        assert s._chase_send_one("1101", 3) == "stopped"
        assert _buys(s.broker) == []

    def test_chase_send_one_is_stopped_even_with_target(self):
        # 正常預掛後才切隔日賣模式: cadence / 首盤訊號路徑進到 _chase_send_one 也不送市價
        s = make_session()
        s.place_pre_orders(["1101"], {"1101": LIMIT})
        s.set_armed(True, overnight_only=True)
        assert s.trades["1101"].target_lots > 0
        assert s._chase_send_one("1101", 2) == "stopped"
        assert [p for p in s.broker.placed if p[0] == "market_buy"] == []

    def test_market_chase_worker_sends_nothing(self):
        # 08:59:59 的真實情況: 還沒有開盤訊號 → cadence 迴圈本身就要被 entry_allowed 擋住
        # (不能只靠下游 _chase_send_one 的閘門;這裡用 spy 看迴圈有沒有真的跑起來)
        s = make_session()
        s.place_pre_orders(["1101"], {"1101": LIMIT})
        s.set_armed(True, overnight_only=True)
        assert s.trades["1101"].target_lots > 0 and s.is_live()
        sends, acquires = [], []
        s._chase_send_one = lambda sym, sf: sends.append((sym, sf)) or "stopped"
        s._rate.acquire = lambda *a, **k: acquires.append(1)
        cutoff = (datetime.now() + timedelta(seconds=0.6)).time()
        t0 = time.monotonic()
        s._market_chase_worker("1101", _t(0, 0, 0), cutoff)   # start_time 已過 → 立刻進迴圈
        assert time.monotonic() - t0 < 0.4                    # 第一輪就結束,不是跑到 cutoff
        assert sends == [] and acquires == []
        assert "1101" not in s._active_chase

    def test_market_chase_worker_with_open_signal_sends_nothing(self):
        s = make_session()
        s.place_pre_orders(["1101"], {"1101": LIMIT})
        s.set_armed(True, overnight_only=True)
        s.on_first_trade("1101")                    # 開盤訊號旗標 (本身不下單)
        s._market_chase_worker("1101", _t(0, 0, 0), _t(23, 59, 59))
        assert [p for p in s.broker.placed if p[0] == "market_buy"] == []
        assert "1101" not in s._active_chase

    def test_pre_orders_stop_at_next_symbol_after_switch(self):
        # 預掛迴圈逐檔阻塞往返;第一檔送出後切成隔日賣模式 → 其餘檔不送、不建 st、不保留預算
        s = make_session()
        orig = s.broker.place_limit_buy

        def flip(symbol, price, lots):
            no = orig(symbol, price, lots)
            s.set_armed(True, overnight_only=True)
            return no

        s.broker.place_limit_buy = flip
        s.place_pre_orders(["1101", "2330", "2454"], {"1101": LIMIT, "2330": 100.0, "2454": 50.0})
        assert [p[1] for p in _buys(s.broker)] == ["1101"]
        assert "2330" not in s.trades and "2454" not in s.trades
        assert s.budget_used == s.trades["1101"].target_lots * LIMIT * 1000

    def test_market_order_accepted_after_switch_is_cancelled(self):
        # SDK 往返期間操作者按了「開始隔日賣」→ 受理的 M 視同淘汰: 回 aborted,M 與預掛 P 都進撤單
        s = make_session()
        s.place_pre_orders(["1101"], {"1101": LIMIT})
        p_no = s.trades["1101"].order_no
        orig = s.broker.place_market_buy

        def flip(symbol, lots):
            s.set_armed(True, overnight_only=True)
            return orig(symbol, lots)

        s.broker.place_market_buy = flip
        assert s._chase_send_one("1101", 2) == "aborted"
        m_no = [no for no, r in s.order_log.items() if r["kind"] == "market_buy"][0]
        handled = set(s._cancel_queue) | {c[0] for c in s.broker.cancelled} \
            | {no for no, r in s.order_log.items() if r.get("status") == "cancelled"
               or r.get("cancel_state") in ("queued", "sent", "unconfirmed")}
        assert {p_no, m_no} <= handled, (p_no, m_no, handled)

    def test_entry_allowed_false_but_is_live_true(self):
        s = _overnight_only()
        assert s.is_live() is True and s.entry_allowed() is False
        s.broker.healthy = False                     # 連線不健康 → 兩者都 False
        assert s.is_live() is False and s.entry_allowed() is False


class TestOvernightSellStillWorks:
    def test_sell_fires_when_lock_opens(self):
        b = InvBroker([{"symbol": "9999", "lots": 5}])
        s = _overnight_session(b)
        # 委買一 50 < 今日漲停 55、無市價列 → 漲停打開 → 跌停價限價賣 min(清單, 庫存)
        s.update_overnight_book("9999", 50.0, 50.5, 0, 50.0)
        assert _wait(lambda: _sells(b)), "隔日賣模式下隔日賣沒有觸發"
        assert _sells(b) == [("limit_sell", "9999", 45.5, 5)]
        assert _buys(b) == []

    def test_locked_holds(self):
        b = InvBroker([{"symbol": "9999", "lots": 5}])
        s = _overnight_session(b)
        s.update_overnight_book("9999", 55.0, 0.0, 800, 55.0)   # 市價隊伍 + 買牆 → 鎖著續抱
        time.sleep(0.3)
        assert _sells(b) == [] and s.overnight["9999"]["locked_now"] is True

    def test_sell_fires_even_if_symbol_is_marked_today(self):
        # 昨天的持股今天又被標記: 預掛被隔日賣模式跳過 (不建 st) → 隔日賣規則看到「今日不活躍」照常賣
        b = InvBroker([{"symbol": "9999", "lots": 5}])
        s = _overnight_session(b)
        s.place_pre_orders(["9999"], {"9999": 55.0})
        assert _buys(b) == [] and "9999" not in s.trades
        s.update_overnight_book("9999", 50.0, 50.5, 0, 50.0)
        assert _wait(lambda: _sells(b)), "隔日賣沒有觸發"
        assert _sells(b) == [("limit_sell", "9999", 45.5, 5)]

    def test_disarmed_blocks_sell_as_before(self):
        b = InvBroker([{"symbol": "9999", "lots": 5}])
        s = _overnight_session(b)
        s.set_armed(False)
        s.update_overnight_book("9999", 50.0, 50.5, 0, 50.0)
        time.sleep(0.3)
        assert _sells(b) == []


class TestApi:
    def test_arm_endpoint_passes_overnight_only(self, monkeypatch):
        import api
        from runner import Runner
        calls = []

        class S:
            def set_armed(self, armed, overnight_only=False):
                calls.append((armed, overnight_only))

        monkeypatch.setattr(Runner, "get", staticmethod(lambda: SimpleNamespace(session=S())))
        r = api.trading_arm(api.TradingArmReq(armed=True, overnight_only=True))
        assert calls == [(True, True)] and r == {"ok": True, "armed": True, "overnight_only": True}
        r = api.trading_arm(api.TradingArmReq(armed=True))            # 預設 = 正常「開始交易」(舊簽名呼叫)
        assert calls[-1] == (True, False) and r == {"ok": True, "armed": True}
        r = api.trading_arm(api.TradingArmReq(armed=False, overnight_only=True))
        assert calls[-1] == (False, True) and r == {"ok": True, "armed": False}

    def test_plain_arm_does_not_upgrade_overnight_mode(self, monkeypatch):
        # 隔日賣模式中,沒帶 overnight_only 的 {armed:true} (舊前端 / central 自動 arm) 不得升級成搶單;
        # 明確 overnight_only=false 才切 (前端「⚡ 改為搶單」)
        import api
        from runner import Runner
        calls = []

        class S:
            armed = True
            entry_enabled = False

            def set_armed(self, armed, overnight_only=False):
                calls.append((armed, overnight_only))

        monkeypatch.setattr(Runner, "get", staticmethod(lambda: SimpleNamespace(session=S())))
        r = api.trading_arm(api.TradingArmReq(armed=True))
        assert calls == [] and r == {"ok": True, "armed": True, "overnight_only": True, "unchanged": True}
        r = api.trading_arm(api.TradingArmReq(armed=True, overnight_only=False))
        assert calls == [(True, False)] and r == {"ok": True, "armed": True}
        r = api.trading_arm(api.TradingArmReq(armed=False))               # 停止交易永遠可以
        assert calls[-1] == (False, False) and r == {"ok": True, "armed": False}

    def test_plain_arm_on_real_session_in_overnight_mode_keeps_mode(self, monkeypatch):
        import api
        from runner import Runner
        s = _overnight_only()
        monkeypatch.setattr(Runner, "get", staticmethod(lambda: SimpleNamespace(session=s)))
        r = api.trading_arm(api.TradingArmReq(armed=True))
        assert r["unchanged"] is True and s.entry_allowed() is False
        api.trading_arm(api.TradingArmReq(armed=True, overnight_only=False))
        assert s.entry_allowed() is True and s.status()["arm_mode"] == "trade"

    def test_arm_endpoint_maps_preflight_error_to_400(self, monkeypatch):
        import api
        from fastapi import HTTPException
        from runner import Runner

        class S:
            def set_armed(self, armed, overnight_only=False):
                raise RuntimeError("券商未連線或連線不健康")

        monkeypatch.setattr(Runner, "get", staticmethod(lambda: SimpleNamespace(session=S())))
        with pytest.raises(HTTPException) as ei:
            api.trading_arm(api.TradingArmReq(armed=True, overnight_only=True))
        assert ei.value.status_code == 400
