"""marked 快照 (中心過濾 Hub → 交易節點) — build_marked_snapshot payload 正確性。
node 靠它預掛+訂閱,不自己 filter,故欄位/優先序/final 旗標都要對。"""
from types import SimpleNamespace

from runner import Runner
from state import State


def build_marked_snapshot(r):
    return r.marked_snapshot()


def _runner():
    r = Runner()                       # 直接建構 (不走 get() singleton;不 login)
    r.cfg = SimpleNamespace(role="hub")
    r.state = State(bid_drop_ratio=0.5)
    r.limit_ups = {"8105": 16.65, "6933": 260.0, "2330": 600.0}
    r.dispositions = {"6933": True}
    # 風控三欄來源 (2026-09-01): 跌停價 / T30 / 禁現沖
    r.limit_downs = {"8105": 13.65}
    r.day_tradable = {"8105": True, "6933": False}
    r.session.set_untradable({"8105"})
    # 8105 & 2330 開盤即鎖 (first_tick), 6933 盤中鎖
    r.state.mark("8105", 16.65, 500, 16.65, first_tick=True)
    r.state.mark("6933", 260.0, 100, 260.0, first_tick=False)
    r.state.mark("2330", 600.0, 200, 600.0, first_tick=True)
    r.state.update_max_bid("8105", 820)      # 8105 峰值升到 820 (mark 時是 500)
    r.state.update_max_bid("8105", 300)      # 之後掉到 300 → 峰值仍留 820
    return r


def test_payload_fields():
    r = _runner()
    r._marked_frozen = True
    snap = build_marked_snapshot(r)
    assert snap["role"] == "hub" and snap["final"] is True
    m = {s["symbol"]: s for s in snap["symbols"]}
    assert set(m) == {"8105", "6933", "2330"}
    assert m["8105"]["limit_up"] == 16.65 and m["8105"]["first_tick"] is True
    assert m["6933"]["first_tick"] is False and m["6933"]["is_disposition"] is True
    assert m["2330"]["is_disposition"] is False
    # max_bid_vol = mark 以來峰值 (node 判量減半用): 8105 峰值 820 (非 mark 時 500,也非後來的 300)
    assert m["8105"]["max_bid_vol"] == 820
    assert m["6933"]["max_bid_vol"] == 100 and m["2330"]["max_bid_vol"] == 200
    # last_bid_vol = 最後已知量 (node 兩步 seed 用): 8105 最後掉到 300 (≠ 峰值 820)
    assert m["8105"]["last_bid_vol"] == 300
    assert m["6933"]["last_bid_vol"] == 100 and m["2330"]["last_bid_vol"] == 200


def test_risk_fields():
    # 風控三欄 (2026-09-01): limit_down / is_t30 / day_tradable;缺資料 → 安全預設
    m = {s["symbol"]: s for s in build_marked_snapshot(_runner())["symbols"]}
    assert m["8105"]["limit_down"] == 13.65 and m["8105"]["is_t30"] is True
    assert m["6933"]["day_tradable"] is False
    # 2330 三欄全缺來源 → 預設 0.0 / False / True (= standalone 查無資料時的行為)
    assert m["2330"]["limit_down"] == 0.0
    assert m["2330"]["is_t30"] is False
    assert m["2330"]["day_tradable"] is True


def test_priority_first_tick_first():
    # 優先序: 開盤即鎖 (2330,8105) 先於盤中鎖 (6933);priority = index
    r = _runner()
    order = [s["symbol"] for s in build_marked_snapshot(r)["symbols"]]
    assert order.index("2330") < order.index("6933")
    assert order.index("8105") < order.index("6933")
    assert build_marked_snapshot(r)["symbols"][0]["priority"] == 0


def test_final_defaults_false_until_frozen():
    r = _runner()                       # 未凍結
    assert build_marked_snapshot(r)["final"] is False


def test_no_state_empty_safe():
    r = Runner()
    r.cfg = SimpleNamespace(role="node")
    r.state = None
    snap = build_marked_snapshot(r)
    assert snap["symbols"] == [] and snap["final"] is False and snap["role"] == "node"


# ─────────────── 行情過期的標記不進名單 (2026-10-12 hub 行情斷線 → 6872) ───────────────
# hub 的 socket 斷線 / 停擺,或重連後這一檔還沒收到新的五檔 → 它的「鎖漲停」是斷線前的畫面,不能交給 node。

class _Sub:
    """只有 stale_symbols 的假 subscriber (真的判定在 tests/test_subscriber_reconnect.py)。"""

    def __init__(self, stale=(), boom=False, down=()):
        self.stale = set(stale)
        self.down = set(down)
        self.boom = boom
        self.asked = []
        self.asked_down = []

    def stale_symbols(self, symbols):
        self.asked.append(list(symbols))
        if self.boom:
            raise RuntimeError("boom")
        return {s for s in symbols if s in self.stale}

    def down_symbols(self, symbols):
        self.asked_down.append(list(symbols))
        return {s for s in symbols if s in self.down}


def test_stale_marks_are_left_out_and_listed_separately():
    r = _runner()
    r.subscriber = _Sub(stale={"8105"})
    snap = build_marked_snapshot(r)
    syms = [s["symbol"] for s in snap["symbols"]]
    assert "8105" not in syms and set(syms) == {"2330", "6933"}
    assert snap["stale_excluded"] == ["8105"]
    assert [s["priority"] for s in snap["symbols"]] == [0, 1], "優先序照留下來的重新編號"
    assert r.state.is_marked("8105"), "只是不交給 node,標記本身不動 (收到新五檔由 handler 決定)"


def test_stale_mark_returns_once_it_is_fresh_again():
    r = _runner()
    sub = r.subscriber = _Sub(stale={"8105", "6933"})
    assert [s["symbol"] for s in build_marked_snapshot(r)["symbols"]] == ["2330"]
    sub.stale.discard("8105")                           # 收到新五檔、仍鎖住
    snap = build_marked_snapshot(r)
    assert {s["symbol"] for s in snap["symbols"]} == {"2330", "8105"} and snap["stale_excluded"] == ["6933"]


def test_nothing_stale_keeps_the_payload_as_before():
    r = _runner()
    r.subscriber = _Sub()
    snap = build_marked_snapshot(r)
    assert {s["symbol"] for s in snap["symbols"]} == {"8105", "6933", "2330"} and snap["stale_excluded"] == []
    r.subscriber = None                                 # 還沒建 subscriber
    assert len(build_marked_snapshot(r)["symbols"]) == 3
    r.subscriber = object()                             # 沒有 stale_symbols 的替身
    assert len(build_marked_snapshot(r)["symbols"]) == 3


def test_node_does_not_apply_the_fresh_book_rule():
    """node 的行情是 08:59:50 才訂的 — 「還沒收到五檔」是常態,名單以 hub 為準,不套「重連後要新五檔」。"""
    r = _runner()
    r.cfg = SimpleNamespace(role="node")
    sub = r.subscriber = _Sub(stale={"8105", "6933", "2330"})
    assert len(build_marked_snapshot(r)["symbols"]) == 3 and sub.asked == []
    assert r._stale_marked(["8105"]) == set()


def test_node_excludes_symbols_whose_own_feed_connection_is_down():
    """但 node 自己的行情連線**斷線中**就是瞎的 (看不到賣單出現、看不到開盤) → 那些檔不預掛、不盲送。"""
    r = _runner()
    r.cfg = SimpleNamespace(role="node")
    sub = r.subscriber = _Sub(stale={"8105", "6933"}, down={"2330"})
    assert r._stale_marked(["8105", "6933", "2330"]) == {"2330"}
    assert sub.asked == [] and sub.asked_down == [["8105", "6933", "2330"]]


def test_standalone_filters_like_the_hub():
    r = _runner()
    r.cfg = SimpleNamespace(role="standalone")
    r.subscriber = _Sub(stale={"6933"})
    assert r._stale_marked(["8105", "6933"]) == {"6933"}


def test_check_failure_is_loud_and_does_not_empty_the_list(caplog):
    import logging
    r = _runner()
    r.subscriber = _Sub(boom=True)
    snap = build_marked_snapshot(r)
    assert len(snap["symbols"]) == 3 and snap["stale_excluded"] == []
    assert [x for x in caplog.records if x.levelno == logging.CRITICAL and "過期檢查例外" in x.getMessage()]


def test_freeze_reports_what_was_excluded(caplog):
    import logging
    r = _runner()
    r.cfg = SimpleNamespace(role="hub", hub_freeze_time="00:00:00")     # 時點已過 → 立刻凍結
    r.subscriber = _Sub(stale={"8105", "6933"})
    r._start_freeze_timer()
    for t in r._timer_threads:
        t.join(3)
    assert r._marked_frozen is True
    text = "\n".join(x.getMessage() for x in caplog.records)
    assert "凍結 marked (1 檔)" in text, "檔數是實際交給 node 的"
    crit = [x.getMessage() for x in caplog.records if x.levelno == logging.CRITICAL]
    assert len(crit) == 1 and "排除 2 檔" in crit[0] and "6933" in crit[0] and "8105" in crit[0]


def test_freeze_without_stale_marks_has_no_alarm(caplog):
    import logging
    r = _runner()
    r.cfg = SimpleNamespace(role="hub", hub_freeze_time="00:00:00")
    r.subscriber = _Sub()
    r._start_freeze_timer()
    for t in r._timer_threads:
        t.join(3)
    assert r._marked_frozen is True
    assert not [x for x in caplog.records if x.levelno == logging.CRITICAL]
    assert "凍結 marked (3 檔)" in "\n".join(x.getMessage() for x in caplog.records)


import pytest


@pytest.mark.parametrize("role,kw", [("standalone", {"stale": {"6933"}}), ("node", {"down": {"6933"}})])
def test_pre_orders_and_blind_chase_skip_symbols_with_a_stale_feed(caplog, role, kw):
    """standalone: 行情過期的標記不預掛、不盲送、也不為它訂 trades。
    node: 自己的行情連線斷線中的檔同樣不下 (hub 給的名單再對,瞎著也不能下)。"""
    import logging
    from datetime import datetime, timedelta
    now = datetime.now()
    if now.hour == 23 and now.minute >= 59:
        return                                           # 跨日邊界不跑 (時點以當日 HH:MM:SS 計)
    r = _runner()
    r.cfg = SimpleNamespace(role=role,
                            pre_order_time=(now + timedelta(seconds=2)).strftime("%H:%M:%S"),
                            market_chase_start="08:59:59", market_chase_cutoff="09:03:00")
    calls = {}

    class _Sess:
        def place_pre_orders(self, marked, limit_ups, stop_event, limit_up_bid_vols=None):
            calls["pre"] = list(marked)

        def start_market_chase(self, marked, start, cutoff):
            calls["chase"] = list(marked)

        def cancel_symbol_orders(self, *a):
            pass

        def has_overnight(self, s):
            return False

    class _S(_Sub):
        def subscribe_trades_for(self, syms):
            calls["trades"] = list(syms)

        def set_handlers(self, **kw):
            pass

        def get_latest_snapshot(self, s):
            return None

        def request_unsubscribe(self, s):
            pass

    r.session = _Sess()
    r.subscriber = _S(**kw)
    r._start_pre_order_timer()
    for t in r._timer_threads:
        t.join(8)
    assert "2330" in calls["pre"] and "6933" not in calls["pre"]
    assert "6933" not in calls["chase"] and "6933" not in calls["trades"]
    crit = [x.getMessage() for x in caplog.records if x.levelno == logging.CRITICAL]
    assert len(crit) == 1 and "預掛**排除 1 檔**" in crit[0] and "6933" in crit[0]


@pytest.mark.parametrize("own_book,expect_skipped", [(False, True), (True, False)])
def test_node_skips_hub_stale_symbols_it_has_no_book_for(caplog, own_book, expect_skipped):
    """hub 最後一份快照說 6933 行情過期 (hub 的連線斷了):
    本機也還沒收到它的五檔 → 沒有任何一邊能確認它還鎖著 → 不預掛、不盲送;
    本機有自己的五檔 → 照本機的 (打開了 filter 早就淘汰它),照常下。"""
    import logging
    from datetime import datetime, timedelta
    now = datetime.now()
    if now.hour == 23 and now.minute >= 59:
        return
    r = _runner()
    r.cfg = SimpleNamespace(role="node",
                            pre_order_time=(now + timedelta(seconds=2)).strftime("%H:%M:%S"),
                            market_chase_start="08:59:59", market_chase_cutoff="09:03:00")
    r._node_hub_stale = {"6933", "9999"}
    calls = {}

    class _Sess:
        def place_pre_orders(self, marked, limit_ups, stop_event, limit_up_bid_vols=None):
            calls["pre"] = list(marked)

        def start_market_chase(self, marked, start, cutoff):
            calls["chase"] = list(marked)

        def cancel_symbol_orders(self, *a):
            pass

        def has_overnight(self, s):
            return False

    class _S(_Sub):
        def subscribe_trades_for(self, syms):
            calls["trades"] = list(syms)

        def set_handlers(self, **kw):
            pass

        def get_latest_snapshot(self, s):
            if own_book and s == "6933":
                return {"symbol": s, "books": {"bids": [{"price": 260.0, "size": 90}], "asks": []}}
            return None

        def request_unsubscribe(self, s):
            pass

    r.session = _Sess()
    r.subscriber = _S()
    r._start_pre_order_timer()
    for t in r._timer_threads:
        t.join(8)
    crit = [x.getMessage() for x in caplog.records if x.levelno == logging.CRITICAL]
    assert "2330" in calls["pre"]
    if expect_skipped:
        assert "6933" not in calls["pre"] and "6933" not in calls["chase"]
        assert len(crit) == 1 and "hub 判定行情過期" in crit[0] and "6933" in crit[0] and "9999" not in crit[0]
        assert r.state.is_marked("6933"), "不淘汰"
    else:
        assert "6933" in calls["pre"] and "6933" in calls["chase"] and crit == []
