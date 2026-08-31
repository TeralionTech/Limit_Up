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
