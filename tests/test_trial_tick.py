"""試撮 tick 過濾 (2026-08-10 2491 事故: 集合競價試撮 tick 誤觸發市價追 →
競價時段連 13 筆市價單被拒,真正進場晚了 4 秒)。

規則: isTrial=true 的 tick 一律忽略;逐筆撮合開始前 (預設 09:00) 的 trades
一律不觸發 (isTrial 欄位缺失時的雙保險)。
"""
import json
from datetime import time as dtime
from types import SimpleNamespace

from runner import Runner
from trader import Trader, Holding
from subscriber import Subscriber


def _mk_runner():
    r = Runner()                                   # 直接建構 (不走 get() singleton)
    r.cfg = SimpleNamespace()                       # _monitor_on_trade 已不讀任何 cfg
    r._trade_start_time = dtime(0, 0)              # 測試不受牆鐘影響 (預設全放行)
    calls = []
    r.session.on_first_trade = lambda sym: calls.append(sym)   # 2026-08-29 單參,無量門檻
    return r, calls


REAL_TICK = {"price": 100.0, "size": 500}          # size 單位是張
TRIAL_TICK = {"price": 100.0, "size": 500, "isTrial": True}


class TestMonitorOnTrade:
    def test_trial_tick_ignored(self):
        r, calls = _mk_runner()
        r._monitor_on_trade("2491", TRIAL_TICK)
        assert calls == []                         # 試撮不觸發市價追

    def test_real_tick_triggers(self):
        r, calls = _mk_runner()
        r._monitor_on_trade("2491", REAL_TICK)
        assert calls == ["2491"]                    # 首筆真成交到 → 立開盤訊號 (無量門檻)

    def test_tiny_real_tick_still_triggers(self):
        # 首筆量門檻已移除 (2026-08-29): 再小的首筆真成交也照樣立旗、不淘汰
        r, calls = _mk_runner()
        r._monitor_on_trade("2491", {"price": 100.0, "size": 1})
        assert calls == ["2491"]

    def test_time_gate_blocks_before_open(self):
        # 逐筆撮合開始前,連沒帶 isTrial 的 tick 也擋 (欄位缺失保險)
        r, calls = _mk_runner()
        r._trade_start_time = dtime(23, 59, 59)
        r._monitor_on_trade("2491", REAL_TICK)
        assert calls == []


class TestTraderTrialGuard:
    def test_trial_tick_not_counted_as_first_trade(self):
        t = Trader(watchlist=["2491"], limit_ups={"2491": 34.9},
                   cfg=SimpleNamespace(bid_decline_sample_sec=60, bid_decline_minutes=5))
        t.on_trade("2491", TRIAL_TICK)
        h = t.holdings["2491"]
        assert h.first_trade_seen is False         # 試撮不算首筆
        assert h.trade_count == 0
        t.on_trade("2491", REAL_TICK)              # 真成交照常處理
        assert h.first_trade_seen is True


class TestSnapshotTrialFlag:
    def test_snapshot_records_is_trial(self):
        sub = Subscriber(sdk=None, universe=[], on_book=None, login_cfg=None)
        h = sub._make_msg_handler(0)
        h(json.dumps({"event": "data", "channel": "trades",
                      "data": {"symbol": "2491", "price": 34.9, "size": 94000,
                               "isTrial": True}}))
        lt = sub.get_latest_snapshot("2491")["last_trade"]
        assert lt["is_trial"] is True              # 種子化迴圈靠這個跳過試撮殘留
        h(json.dumps({"event": "data", "channel": "trades",
                      "data": {"symbol": "2491", "price": 34.9, "size": 94000}}))
        assert sub.get_latest_snapshot("2491")["last_trade"]["is_trial"] is False
