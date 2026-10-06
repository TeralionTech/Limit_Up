"""2026-10-06 4716 事故: 凍結後才鎖上的檔,node 看不到。

Hub 08:59:50 的「凍結」只是旗標,名單本身還是活的;4716 在 08:59:54 才符合「無賣單 + 買一=漲停」
被 Hub 標記,但六台 node 在 08:59:50 拿到首份 final 快照 (0 檔) 之後就不再問 → 沒預掛、沒追價,
整天零委託;IDC 系統 08:59:58 掛單、09:00:14 成交,4716 整天鎖死。

修法: node 拿到首份快照後繼續問 Hub 到 08:59:57 (node_client.poll_marked_snapshot_until),
新增的檔補灌狀態 + 補訂閱 (runner._catch_up_late_marks);08:59:58 預掛計時器讀的是當下 marked
清單,會一併預掛。安全邊界 (2026-10-07 審查後): 請求含逾時都在死線前結束、晚到的回應丟棄;
一檔壞資料只跳過那一檔;補訂閱以券商 ack 為準、socket 就緒才送;補收的檔被 Hub 拿掉就跟著淘汰
(首份快照的檔仍由 node 自己判);本機已淘汰的不復活;整段失敗退回舊行為。
"""
import json
import threading
import time
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

import runner as runner_mod
from node_client import poll_marked_snapshot_until
from runner import Runner
from state import State
from test_session_money import make_session


# ─────────────────────────── 假物件 ───────────────────────────
class _Resp:
    def __init__(self, payload):
        self._raw = json.dumps(payload).encode("utf-8")

    def read(self):
        return self._raw

    def close(self):
        pass


class _Clock:
    def __init__(self, t=0.0):
        self.t = t
        self.sleeps = []

    def time(self):
        return self.t

    def sleep(self, s):
        self.sleeps.append(s)
        self.t += s


class _Seq:
    """假 Hub: 依序回傳 responses,最後一份之後重複;Exception 代表 HTTP 失敗;callable 代表副作用後再回。"""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def __call__(self, url, timeout):
        r = self.responses[min(self.calls, len(self.responses) - 1)]
        self.calls += 1
        if callable(r):
            r = r()
        if isinstance(r, Exception):
            raise r
        return _Resp(r)


class _SlowSeq(_Seq):
    """回應前把假時鐘往前推 delay 秒 (模擬 Hub 慢回應)。"""

    def __init__(self, responses, clk, delay):
        super().__init__(responses)
        self.clk, self.delay = clk, delay

    def __call__(self, url, timeout):
        self.clk.t += self.delay
        return super().__call__(url, timeout)


class _FakeSub:
    """假 subscriber。fail_first=N: 前 N 次 add_symbol 回 False (送出例外,socket 已斷);
    ready=False: is_ready 回 False (socket 還沒就緒);ack_after=N: 第 N 次成功送出後 has_ack 才回 True
    (預設 1 = 送出即 ack);on_add: add_symbol 時的副作用 (模擬行情 thread 同時淘汰)。"""

    def __init__(self, fail_first=0, ready=True, ack_after=1):
        self.added, self.calls, self.fail_first = [], 0, fail_first
        self.ready, self.ack_after = ready, ack_after
        self.universe: list = []
        self.unsubscribed: list = []
        self.trades_subscribed: list = []
        self.books: dict = {}
        self.on_add = None

    def add_symbol(self, sym):
        self.calls += 1
        if self.calls <= self.fail_first:
            return False
        self.added.append(sym)
        if self.on_add:
            self.on_add(sym)
        return True

    def is_ready(self):
        return self.ready

    def has_ack(self, channel, sym):
        return self.added.count(sym) >= self.ack_after

    def request_unsubscribe(self, sym):
        self.unsubscribed.append(sym)

    def get_latest_snapshot(self, sym):
        return {"symbol": sym, "books": self.books.get(sym), "last_trade": None}

    def subscribe_trades_for(self, syms):
        self.trades_subscribed = list(syms)

    def set_handlers(self, **kw):
        pass


def _entry(sym, up=20.95, down=17.15, max_vol=165, last_vol=165, first_tick=False,
           t30=False, dispo=False, day_tradable=True):
    return {"symbol": sym, "limit_up": up, "limit_down": down, "max_bid_vol": max_vol,
            "last_bid_vol": last_vol, "first_tick": first_tick, "is_t30": t30,
            "is_disposition": dispo, "day_tradable": day_tradable, "priority": 0}


def _snap(*entries, final=True, ts="2026-10-06T08:59:54"):
    return {"ts": ts, "final": final, "role": "hub", "symbols": list(entries)}


def _node(*initial_entries):
    r = Runner()
    r.session = make_session()
    r.state = State(bid_drop_ratio=0.5)
    r.cfg = SimpleNamespace(role="node", hub_url="http://hub:8100")
    r._apply_marked_snapshot(_snap(*initial_entries, ts="2026-10-06T08:59:50"))   # 08:59:50 首份快照
    r.subscriber = _FakeSub()
    return r


def _catch(r, responses, deadline=7.0, fail_first=None, urlopen=None):
    if fail_first is not None:
        r.subscriber = _FakeSub(fail_first=fail_first)
    clk = _Clock()
    return r._catch_up_late_marks(deadline, time_fn=clk.time, sleep_fn=clk.sleep,
                                  urlopen=urlopen or _Seq(responses), poll_interval=0.5)


# ─────────────────────────── node_client 輪詢 ───────────────────────────
class TestPollUntil:
    def test_every_final_snapshot_until_deadline(self):
        got = []
        clk = _Clock()
        seq = _Seq([_snap(final=False), _snap(), _snap(_entry("4716"))])
        stats = poll_marked_snapshot_until("http://hub:8100", 3.0, got.append, poll_interval=1.0,
                                           urlopen=seq, time_fn=clk.time, sleep_fn=clk.sleep)
        assert stats["polls"] == 3 and stats["snapshots"] == 2      # final=false 那份不算
        assert stats["non_final"] == 1
        assert [len(s["symbols"]) for s in got] == [0, 1]
        assert clk.t >= 3.0, "要輪詢到死線才停"

    def test_request_that_cannot_finish_before_deadline_is_not_started(self):
        """請求含逾時要在死線前結束: t=2 時 2+1.0 > 2.9 → 不再發 (舊版會發到 t<2.9)。"""
        clk = _Clock()
        seq = _Seq([_snap()])
        stats = poll_marked_snapshot_until("http://hub:8100", 2.9, lambda s: None, poll_interval=1.0,
                                           timeout=1.0, urlopen=seq, time_fn=clk.time, sleep_fn=clk.sleep)
        assert stats["polls"] == 2 and seq.calls == 2

    def test_slow_response_after_deadline_is_dropped(self):
        """Hub 回應落在死線之後 → 丟棄不套用 (08:59:58 預掛計時器已在讀清單)。"""
        got = []
        clk = _Clock()
        seq = _SlowSeq([_snap(_entry("4716"))], clk, delay=1.2)
        stats = poll_marked_snapshot_until("http://hub:8100", 1.1, got.append, poll_interval=0.5,
                                           timeout=1.0, urlopen=seq, time_fn=clk.time, sleep_fn=clk.sleep)
        assert stats["polls"] == 1 and stats["late_drops"] == 1 and stats["snapshots"] == 0
        assert got == []

    def test_sleep_never_exceeds_remaining_time(self):
        clk = _Clock()
        poll_marked_snapshot_until("http://hub:8100", 0.8, lambda s: None, poll_interval=0.5,
                                   timeout=0.2, urlopen=_Seq([_snap()]), time_fn=clk.time, sleep_fn=clk.sleep)
        assert clk.sleeps == [0.5, pytest.approx(0.3)]

    def test_stop_fn_ends_polling(self):
        flag = threading.Event()

        def _cb(snap):
            flag.set()
        clk = _Clock()
        stats = poll_marked_snapshot_until("http://hub:8100", 5.0, _cb, poll_interval=0.5,
                                           urlopen=_Seq([_snap()]), time_fn=clk.time, sleep_fn=clk.sleep,
                                           stop_fn=flag.is_set)
        assert stats["polls"] == 1

    def test_callback_exception_does_not_stop_polling(self):
        n = {"calls": 0}

        def _cb(snap):
            n["calls"] += 1
            raise RuntimeError("補收邏輯壞了")
        clk = _Clock()
        stats = poll_marked_snapshot_until("http://hub:8100", 2.0, _cb, poll_interval=1.0,
                                           urlopen=_Seq([_snap()]), time_fn=clk.time, sleep_fn=clk.sleep)
        assert n["calls"] == 2 and stats["errors"] == 2 and "補收邏輯壞了" in str(stats["last_error"])

    def test_http_errors_tolerated_and_first_error_kept(self):
        got = []
        clk = _Clock()
        seq = _Seq([ConnectionError("refused"), _snap(_entry("4716"))])
        stats = poll_marked_snapshot_until("http://hub:8100", 2.0, got.append, poll_interval=1.0,
                                           urlopen=seq, time_fn=clk.time, sleep_fn=clk.sleep)
        assert stats["errors"] == 1 and len(got) == 1
        assert isinstance(stats["first_error"], ConnectionError)

    def test_returns_immediately_when_deadline_passed(self):
        clk = _Clock(10.0)
        seq = _Seq([_snap()])
        stats = poll_marked_snapshot_until("http://hub:8100", 5.0, lambda s: None,
                                           urlopen=seq, time_fn=clk.time, sleep_fn=clk.sleep)
        assert stats["polls"] == 0 and seq.calls == 0


# ─────────────────────────── runner 補收 ───────────────────────────
class TestCatchUpLateMarks:
    def test_incident_4716_marked_after_freeze_is_picked_up(self):
        """事故重現: 首份快照 0 檔 → 08:59:54 Hub 名單多了 4716 → node 補灌 + 補訂閱。"""
        r = _node()                                         # 08:59:50: 0 檔
        res = _catch(r, [_snap(), _snap(), _snap(_entry("4716"))])

        assert res["added"] == ["4716"] and res["unsubscribed"] == [] and res["dropped"] == []
        assert r.state.get_marked_prioritized() == ["4716"]
        assert r.limit_ups["4716"] == 20.95 and r.limit_downs["4716"] == 17.15
        assert r.day_tradable["4716"] is True and r.dispositions["4716"] is False
        assert r._node_bid_vol_fallback["4716"] == 165, "預掛的 20% 母數要有 Hub 的兜底量"
        assert r.session.limit_downs["4716"] == 17.15, "風控資料要 rebind 給交易會話"
        assert r.subscriber.added == ["4716"]

    def test_pre_order_timer_inputs_include_late_symbol(self):
        """08:59:58 預掛計時器讀 state.final_check_all + get_marked_prioritized — 補收的檔要在裡面。"""
        r = _node(_entry("8227", max_vol=1000, last_vol=1000))
        _catch(r, [_snap(_entry("8227", max_vol=1000, last_vol=1000), _entry("4716"))])
        assert r.state.final_check_all() == []            # 兩檔 last==max,都不會被量減半刷掉
        assert set(r.state.get_marked_prioritized()) == {"8227", "4716"}

    def test_symbols_from_first_snapshot_not_reapplied(self):
        """已在首份快照的檔: 不重 seed (Hub 之後回的量可能更小,不能把峰值洗掉)、不重複訂閱。"""
        r = _node(_entry("8227", max_vol=1000, last_vol=1000))
        res = _catch(r, [_snap(_entry("8227", max_vol=50, last_vol=50))])
        assert res["added"] == [] and r.subscriber.added == []
        assert r.state.final_check_all() == []            # 峰值仍是 1000、last 仍是 1000 → 不刷
        assert r._node_bid_vol_fallback["8227"] == 1000

    def test_locally_discarded_symbol_is_not_revived(self):
        """node 自己的 book handler 已淘汰 (出現賣單) 的檔,就算 Hub 名單還有它也不復活。"""
        r = _node(_entry("8227"))
        r.state.unmark_ask_appeared("8227", 21.0, 5)      # 08:59:52 node 看到賣單 → 淘汰
        res = _catch(r, [_snap(_entry("8227"), _entry("4716"))])
        assert res["added"] == ["4716"]
        assert r.state.get_marked_prioritized() == ["4716"]
        assert r.subscriber.added == ["4716"]

    def test_hub_removal_does_not_unmark_first_snapshot_symbol(self):
        """首份快照的檔只加不減: Hub 凍結後把它拿掉,node 這邊不動 (本機已有 ≥8 秒行情,淘汰交給自己的 handler)。"""
        r = _node(_entry("8227"))
        res = _catch(r, [_snap(_entry("4716"))])          # Hub 名單已沒有 8227
        assert res["added"] == ["4716"] and res["dropped"] == []
        assert set(r.state.get_marked_prioritized()) == {"8227", "4716"}

    def test_late_symbol_dropped_by_hub_is_unmarked_and_unsubscribed(self):
        """補收的檔 Hub 之後又拿掉 (Hub 看到鎖破、本機行情才剛訂看不到) → 跟著淘汰 + 退訂;之後 Hub 再送也不復活。"""
        r = _node(_entry("8227"))
        res = _catch(r, [_snap(_entry("8227"), _entry("4716")), _snap(_entry("8227")),
                         _snap(_entry("8227"), _entry("4716"))])
        assert res["added"] == ["4716"] and res["dropped"] == ["4716"]
        assert r.state.get_marked_prioritized() == ["8227"]
        assert r.state.is_discarded("4716")
        assert r.subscriber.unsubscribed == ["4716"]
        assert r.subscriber.calls == 1, "淘汰後 Hub 再送同一檔也不再訂閱"
        assert r.state.history["4716"][-1]["reason"] == "hub_dropped"

    def test_late_symbol_discarded_locally_is_not_revived_by_hub_resend(self):
        """補收後本機行情 thread 自己淘汰了 (add_symbol 之後看到賣單) → Hub 再送也不復活、不重訂。"""
        r = _node()
        r.subscriber.on_add = lambda sym: r.state.unmark_ask_appeared(sym, 21.0, 5)
        res = _catch(r, [_snap(_entry("4716")), _snap(_entry("4716"))])
        assert res["added"] == ["4716"] and res["dropped"] == []
        assert r.state.get_marked_prioritized() == [] and r.subscriber.calls == 1

    def test_hub_mark_unmark_remark_keeps_first_seed(self):
        """同一檔 Hub 標記→拿掉→再送: 第二次出現時已淘汰 (hub_dropped) 不復活;峰值只 seed 一次。"""
        r = _node()
        res = _catch(r, [_snap(_entry("4716")), _snap(), _snap(_entry("4716", max_vol=50, last_vol=50))])
        assert res["added"] == ["4716"] and res["dropped"] == ["4716"]
        assert r.state.get_marked_prioritized() == []

    def test_subscribe_retried_until_send_succeeds(self):
        """送出失敗 (socket 例外) → 下份快照再試,成功 (ack) 後不再重複。"""
        r = _node()
        res = _catch(r, [_snap(_entry("4716"))], fail_first=2)
        assert res["added"] == ["4716"] and res["unsubscribed"] == []
        assert r.subscriber.added == ["4716"] and r.subscriber.calls == 3

    def test_subscribe_waits_until_socket_ready(self):
        """socket 還沒就緒 (is_ready False) → 不送 (送了伺服器也不收);就緒後下一份快照就送。"""
        r = _node()
        sub = _FakeSub(ready=False)
        r.subscriber = sub

        def _ready_now():
            sub.ready = True
            return _snap(_entry("4716"))
        res = _catch(r, [_snap(_entry("4716")), _snap(_entry("4716")), _ready_now])
        assert res["added"] == ["4716"] and res["unsubscribed"] == []
        assert sub.calls == 1 and sub.added == ["4716"]

    def test_resend_until_ack_then_stop(self):
        """送出成功但券商沒 ack → 每 1 秒再送一次,ack 後停。"""
        r = _node()
        r.subscriber = _FakeSub(ack_after=2)
        res = _catch(r, [_snap(_entry("4716"))])
        assert res["unsubscribed"] == [] and r.subscriber.calls == 2

    def test_no_ack_bounded_resends_and_final_push(self):
        """整段都沒 ack → 最多 3 次 + 迴圈結束再補送 1 次;點名未訂到,但 state 仍標記 (預掛照做,20% 母數用 Hub 兜底量)。"""
        r = _node()
        r.subscriber = _FakeSub(ack_after=10 ** 9)
        res = _catch(r, [_snap(_entry("4716"))])
        assert res["added"] == ["4716"] and res["unsubscribed"] == ["4716"]
        assert r.subscriber.calls == 4
        assert r.state.get_marked_prioritized() == ["4716"]
        assert r._node_bid_vol_fallback["4716"] == 165

    def test_never_sent_is_reported_but_still_marked(self):
        """到死線都送不出去 (socket 已斷) → 點名,但 state 仍標記。"""
        r = _node()
        res = _catch(r, [_snap(_entry("4716"))], fail_first=99)
        assert res["added"] == ["4716"] and res["unsubscribed"] == ["4716"]
        assert r.state.get_marked_prioritized() == ["4716"]

    def test_bad_entry_does_not_poison_sibling_entries(self):
        """同一份快照一檔資料壞掉 → 只跳過那一檔;前面的好檔照樣 seed + 訂閱 + T30 rebind。"""
        r = _node()
        bad = {"symbol": "9999", "limit_up": 10.0, "max_bid_vol": "abc"}
        res = _catch(r, [_snap(_entry("4716", t30=True), bad)])
        assert res["added"] == ["4716"] and res["failed"] == ["9999"]
        assert r.subscriber.added == ["4716"]
        assert "4716" in r.session.untradable
        assert not r.state.is_marked("9999") and "9999" not in r.limit_ups

    def test_bad_entry_first_then_good_entry_same_snapshot(self):
        r = _node()
        bad = {"symbol": "9999", "limit_up": 10.0, "max_bid_vol": "abc"}
        res = _catch(r, [_snap(bad, _entry("4716", t30=True))])
        assert res["added"] == ["4716"] and res["failed"] == ["9999"]
        assert r.subscriber.added == ["4716"] and "4716" in r.session.untradable

    def test_bad_last_bid_vol_leaves_no_partial_state(self):
        """先解析後寫入: last_bid_vol 壞掉時,不可以已經 mark / 寫 limit_ups 卻沒兜底量。"""
        r = _node()
        res = _catch(r, [_snap({**_entry("9999"), "last_bid_vol": "abc"})])
        assert res["added"] == [] and res["failed"] == ["9999"]
        assert not r.state.is_marked("9999")
        assert "9999" not in r.limit_ups and "9999" not in r._node_bid_vol_fallback

    def test_late_entry_without_limit_up_is_skipped(self):
        """無漲停價 (None/0) = 壞資料 → 不 mark (否則 0 價 mark、永遠 unmark 不掉、還進市價盲送)。"""
        r = _node()
        res = _catch(r, [_snap({**_entry("4716"), "limit_up": None})])
        assert res["added"] == [] and res["failed"] == ["4716"]
        assert not r.state.is_marked("4716") and "4716" not in r.limit_ups

    def test_bad_entry_not_retried_every_poll(self):
        r = _node()
        bad = {"symbol": "9999", "limit_up": 10.0, "max_bid_vol": "abc"}
        res = _catch(r, [_snap(bad)])
        assert res["failed"] == ["9999"], "同一份壞資料只記一次,不是每 0.5 秒重試"

    def test_risk_fields_flow_to_session(self):
        r = _node(_entry("8227", t30=True))
        _catch(r, [_snap(_entry("8227", t30=True),
                         _entry("4716", t30=True, dispo=True, day_tradable=False, first_tick=True))])
        assert r.session.untradable == {"8227", "4716"}, "T30 名單要合併,不能把首份快照的洗掉"
        assert r.session.dispositions["4716"] is True
        assert r.session.day_tradable["4716"] is False
        assert "4716" in r.state.first_tick_limit_up

    def test_no_new_symbols_is_quiet(self):
        r = _node(_entry("8227"))
        res = _catch(r, [_snap(_entry("8227"))])
        assert res["added"] == [] and res["stats"]["polls"] > 0

    def test_poll_failure_falls_back_to_old_behaviour(self, monkeypatch):
        """整段例外只記 log → 主流程照舊 (最壞 = 沒有補收)。"""
        r = _node(_entry("8227"))

        def _boom(*a, **k):
            raise RuntimeError("hub 爆了")
        monkeypatch.setattr("node_client.poll_marked_snapshot_until", _boom)
        res = r._catch_up_late_marks(7.0)
        assert res["added"] == [] and r.state.get_marked_prioritized() == ["8227"]

    def test_no_hub_url_is_noop(self):
        r = _node()
        r.cfg = SimpleNamespace(role="node", hub_url="")
        assert _catch(r, [_snap(_entry("4716"))])["added"] == []

    def test_stop_event_aborts_polling(self):
        r = _node()
        r._stop_event.set()
        res = _catch(r, [_snap(_entry("4716"))])
        assert res["added"] == [] and res["stats"]["polls"] == 0

    def test_without_subscriber_still_marks(self):
        """subscriber 還沒建好 (極端) → 仍 seed,訂閱留到點名。"""
        r = _node()
        r.subscriber = None
        res = _catch(r, [_snap(_entry("4716"))])
        assert res["added"] == ["4716"] and res["unsubscribed"] == ["4716"]

    def test_reset_daily_state_clears_catch_up_memory(self):
        """Runner 是常駐 singleton: 隔天不能把昨天的代號當「已知」跳過。"""
        r = _node(_entry("8227"))
        _catch(r, [_snap(_entry("8227"), _entry("4716"))])
        assert r._node_known_syms == {"8227", "4716"}
        r._reset_daily_state()
        assert r._node_known_syms == set() and r._node_t30_set == set()
        assert r._node_bid_vol_fallback == {} and r._node_unsub_and_cancel is None
        r.state = State(bid_drop_ratio=0.5)
        r._apply_marked_snapshot(_snap())
        assert _catch(r, [_snap(_entry("4716"))])["added"] == ["4716"]


# ─────────────────────────── 08:59:58 預掛計時器 (真的跑) ───────────────────────────
def _arm_timer_cfg(r):
    target = (datetime.now() + timedelta(seconds=1.2)).replace(microsecond=0)
    r.cfg.pre_order_time = target.strftime("%H:%M:%S")
    r.cfg.market_chase_start = "08:59:59"
    r.cfg.market_chase_cutoff = "09:03:00"


class TestPreOrderTimerWithLateSymbol:
    def test_late_symbol_is_pre_ordered_with_hub_fallback_volume(self):
        """修法的目的: 補收的檔要真的進 place_pre_orders / start_market_chase,
        20% 母數在本機沒 book 時用 Hub 兜底量 (補收檔 08:59:58 前幾乎不會有本機 book)。"""
        r = _node(_entry("8227", max_vol=1000, last_vol=1000))
        _catch(r, [_snap(_entry("8227", max_vol=1000, last_vol=1000), _entry("4716"))])
        _arm_timer_cfg(r)
        captured = {}
        r.session.place_pre_orders = lambda symbols, limit_ups, stop_event=None, limit_up_bid_vols=None: \
            captured.update(pre=list(symbols), vols=dict(limit_up_bid_vols or {}))
        r.session.start_market_chase = lambda symbols, start, cutoff: captured.update(chase=list(symbols))
        # 模擬補收在 timer 跑的同時插入兜底 dict (以前會 RuntimeError 殺掉 timer)
        orig = r.subscriber.subscribe_trades_for
        r.subscriber.subscribe_trades_for = lambda syms: (orig(syms), r._node_bid_vol_fallback.__setitem__("9999", 1))

        r._start_pre_order_timer()
        r._timer_threads[-1].join(3)
        assert not r._timer_threads[-1].is_alive()
        assert set(captured["pre"]) == {"8227", "4716"}
        assert captured["vols"] == {"8227": 1000, "4716": 165}
        assert set(captured["chase"]) == {"8227", "4716"}
        assert "4716" in r.subscriber.trades_subscribed

    def test_late_t30_symbol_is_not_pre_ordered(self):
        """補收的 T30 檔: session.untradable 有 rebind → 真的 place_pre_orders 會標 full_cash_delivery 跳過。"""
        r = _node(_entry("8227"))
        _catch(r, [_snap(_entry("8227"), _entry("4716", t30=True))])
        _arm_timer_cfg(r)
        r.session.start_market_chase = lambda symbols, start, cutoff: None
        r._start_pre_order_timer()
        r._timer_threads[-1].join(3)
        assert r.session.trades["4716"].stopped_reason == "full_cash_delivery"
        assert r.session.trades["8227"].stopped_reason != "full_cash_delivery"

    def test_late_symbol_culled_by_final_check_is_not_pre_ordered(self):
        r = _node()
        _catch(r, [_snap(_entry("4716", max_vol=1000, last_vol=100))])    # Hub 看到牆已崩一半以上
        _arm_timer_cfg(r)
        captured = {"pre": None}
        r.session.place_pre_orders = lambda symbols, limit_ups, stop_event=None, limit_up_bid_vols=None: \
            captured.update(pre=list(symbols))
        r.session.start_market_chase = lambda symbols, start, cutoff: None
        r._start_pre_order_timer()
        r._timer_threads[-1].join(3)
        assert captured["pre"] is None and r.state.is_discarded("4716")


# ─────────────────────────── node 主流程接線 ───────────────────────────
def _node_runner(monkeypatch, first_snapshot, late_snapshot):
    """跑真的 _run_node_phases;外部依賴換假的。驗證補收迴圈被接在預掛計時器之後、收盤等待之前,
    而且傳的是預掛時點 − 1 秒的死線。"""
    r = Runner()
    r.session = make_session()
    r.sdk = object()
    r.cfg = SimpleNamespace(role="node", hub_url="http://hub:8100", hub_freeze_time="08:59:50",
                            pre_order_time="08:59:58", bid_drop_ratio=0.5, batch_size=199,
                            batch_rotate_sec=30, socket_count=1, debug=False)
    seen = {"deadline": None, "marked_at_trade": None, "events": []}
    events = seen["events"]

    class FakeSub(_FakeSub):
        def __init__(self, **kw):
            super().__init__()
            self.universe = list(kw["universe"])

        def start(self):
            events.append("subscriber.start")

    monkeypatch.setattr("subscriber.Subscriber", FakeSub)
    monkeypatch.setattr("recorder.TickRecorder", lambda path: SimpleNamespace(close=lambda: None))
    monkeypatch.setattr("filter.make_on_book_handler", lambda *a, **k: (lambda *x, **y: None))
    monkeypatch.setattr("filter.wait_until_end_time", lambda state, cfg: events.append("wait_end"))
    monkeypatch.setattr("node_client.pull_marked_snapshot", lambda url, deadline: first_snapshot)

    def _fake_poll(url, deadline_ts, on_snapshot, **kw):
        seen["deadline"] = deadline_ts
        assert r.subscriber is not None, "補收要在 subscriber 建好之後"
        events.append("catch_up")
        on_snapshot(late_snapshot)
        return {"polls": 1, "snapshots": 1, "non_final": 0, "late_drops": 0, "errors": 0,
                "first_error": None, "last_error": None}
    monkeypatch.setattr("node_client.poll_marked_snapshot_until", _fake_poll)

    r._prepare_overnight = lambda output_dir: []
    r._wait_until_clock = lambda t: None
    r._refresh_overnight_prices = lambda **kw: {}
    r._start_pre_order_timer = lambda: events.append("pre_order_timer")
    r._start_cancel_pending_timer = lambda: events.append("cancel_timer")

    def _trade(watchlist, overnight):
        events.append("trade_phase")
        seen["marked_at_trade"] = list(watchlist)
    r._trade_phase = _trade
    return r, seen


class TestNodePathWiring:
    def test_late_symbol_reaches_subscription_and_trade_phase(self, monkeypatch):
        r, seen = _node_runner(monkeypatch, first_snapshot=_snap(_entry("8227")),
                               late_snapshot=_snap(_entry("8227"), _entry("4716")))
        r._run_node_phases()
        assert r.subscriber.added == ["4716"], "凍結後新增的檔要補訂閱"
        assert set(r.state.get_marked_prioritized()) == {"8227", "4716"}
        assert set(seen["marked_at_trade"]) == {"8227", "4716"}
        expected = datetime.now().replace(hour=8, minute=59, second=58, microsecond=0).timestamp() - 1.0
        assert seen["deadline"] == expected, "補收死線 = 預掛時點 − 1 秒 (08:59:57)"
        assert r._node_unsub_and_cancel is not None, "補收的淘汰路徑要綁到 book handler 同一條"

    def test_catch_up_runs_after_timers_and_before_wait_end(self, monkeypatch):
        """順序是安全前提: timer 先排好 (補收會阻塞主執行緒到 08:59:57),補收在收盤等待之前。"""
        r, seen = _node_runner(monkeypatch, first_snapshot=_snap(_entry("8227")),
                               late_snapshot=_snap(_entry("8227")))
        r._run_node_phases()
        assert seen["events"] == ["subscriber.start", "pre_order_timer", "cancel_timer",
                                  "catch_up", "wait_end", "trade_phase"]

    def test_nothing_late_is_unchanged(self, monkeypatch):
        r, seen = _node_runner(monkeypatch, first_snapshot=_snap(_entry("8227")),
                               late_snapshot=_snap(_entry("8227")))
        r._run_node_phases()
        assert r.subscriber.added == [] and seen["marked_at_trade"] == ["8227"]

    def test_catch_up_exception_does_not_kill_the_day(self, monkeypatch):
        r, seen = _node_runner(monkeypatch, first_snapshot=_snap(_entry("8227")),
                               late_snapshot=_snap(_entry("8227")))

        def _boom(self_, deadline_ts, **kw):
            raise RuntimeError("補收炸了")
        monkeypatch.setattr(Runner, "_catch_up_late_marks", _boom)
        r._run_node_phases()                                 # 不可以 raise
        assert seen["marked_at_trade"] == ["8227"], "補收失敗 → 照舊走到轉場"


# ─────────────────────────── Subscriber 補訂 (socket loop / add_symbol) ───────────────────────────
class _WS:
    def __init__(self):
        self.subscribed = []
        self.unsubscribed = []

    def subscribe(self, p):
        self.subscribed.append((p["channel"], p["symbol"]))

    def unsubscribe(self, p):
        self.unsubscribed.append(p)


def _subscriber(universe=()):
    from subscriber import Subscriber
    sub = Subscriber(sdk=None, universe=list(universe), on_book=None, login_cfg=None)
    ws = _WS()
    sub._sockets = [ws]
    sub._rebuild_symbol_index()
    sub._connect_grace_sec = 0.01
    sub.batch_rotate_sec = 0.05
    return sub, ws


class TestSubscriberLateSubscribe:
    def test_empty_universe_loop_waits_and_resubscribes_extras(self):
        """首份快照 0 檔 (10-06 六台即此): 舊版 socket loop 沒有任何 wait → 100% CPU 忙轉到收盤。
        新版以空批常駐,每輪重訂 add_symbol 補進來的檔 (socket 重連後不漏)。"""
        sub, ws = _subscriber()
        t = threading.Thread(target=sub._socket_loop_thread, args=(ws, [], 0, None), daemon=True)
        cpu0 = time.process_time()
        t.start()
        time.sleep(0.1)
        assert sub.is_ready()
        assert sub.add_symbol("4716") is True
        assert ws.subscribed[:2] == [("books", "4716"), ("trades", "4716")]
        time.sleep(0.25)
        sub._stop.set()
        t.join(2)
        cpu = time.process_time() - cpu0
        assert cpu < 0.15, f"socket loop 不可忙等 (用了 {cpu:.3f}s CPU)"
        assert ws.subscribed.count(("books", "4716")) >= 2, "loop 每輪要重訂補進來的檔"
        assert ws.subscribed.count(("trades", "4716")) == 1

    def test_removed_extra_is_not_resubscribed(self):
        sub, ws = _subscriber()
        sub.add_symbol("4716")
        sub.request_unsubscribe("4716")                     # 淘汰 → removed
        t = threading.Thread(target=sub._socket_loop_thread, args=(ws, [], 0, None), daemon=True)
        t.start()
        time.sleep(0.15)
        sub._stop.set()
        t.join(2)
        assert ws.subscribed.count(("books", "4716")) == 1, "淘汰的檔 loop 不可再訂回"

    def test_add_symbol_skips_channels_already_acked(self):
        """隔日賣標的又被補收: books 已有 ack → 不重訂 (舊 id 會丟失、tick 雙份),只補 trades。"""
        sub, ws = _subscriber(["4716"])
        with sub._sub_ids_lock:
            sub._sub_ids[("books", "4716")] = "id-books"
        assert sub.has_ack("books", "4716") and not sub.has_ack("trades", "4716")
        assert sub.add_symbol("4716") is True
        assert ws.subscribed == [("trades", "4716")]

    def test_is_ready_false_before_loop_thread(self):
        sub, ws = _subscriber()
        assert not sub.is_ready()
        assert sub.add_symbol("4716") is True               # 送得出去,但 runner 端會等 is_ready 才送
