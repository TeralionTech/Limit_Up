"""鎖住 scripts/replay_hub_node.py — hub 凍結/快照四欄/node 兩步 seed/T30 防線/鎖破 unmark
+ A/B 對 standalone 等價 (合成迷你 JSONL)。

情境 (2026-09-01):
  AAAA 08:30 鎖到底 → 兩邊都預掛
  BBBB 08:30 鎖 (峰值 800) → 08:59:40 量崩到 100,凍結後**零新 tick** → 語意坑回歸:
       node 靠快照 last_bid_vol=100 也剔得掉 (舊 seed last=max 剔不掉)
  CCCC 08:30 鎖 → 08:59:52 (node 視窗內) 出賣單 → node unmark (2026-09-01 對齊)
  DDDD T30 全額交割,鎖到底 → 快照 is_t30=True → node 不預掛 (standalone 基準無 T30 名單會掛 → 預期差)
  EEEE 08:59:52 (凍結後) 才鎖 → hub 快照沒有 → node 天生看不到 (standalone 會掛 → 預期差)
"""
import json
from types import SimpleNamespace

import pytest

import filter as filter_mod
import trader as trader_mod
import trading_session as ts_mod
import state as state_mod
from scripts.replay_day import run as run_standalone
from scripts.replay_hub_node import run_hub_node, diff_report


def _tick(channel, ts, **data):
    return json.dumps({"channel": channel, "ts": ts, "data": data}, ensure_ascii=False)


def _write_ticks(path):
    L = []
    # 08:30 全鎖
    L.append(_tick("books", "2026-09-01T08:30:00.000000", symbol="AAAA",
                   bids=[{"price": 100.0, "size": 500}], asks=[]))
    L.append(_tick("books", "2026-09-01T08:30:01.000000", symbol="BBBB",
                   bids=[{"price": 50.0, "size": 800}], asks=[]))
    L.append(_tick("books", "2026-09-01T08:30:02.000000", symbol="CCCC",
                   bids=[{"price": 30.0, "size": 600}], asks=[]))
    L.append(_tick("books", "2026-09-01T08:30:03.000000", symbol="DDDD",
                   bids=[{"price": 20.0, "size": 900}], asks=[]))
    # 08:59:40 BBBB 量崩 (last=100 < 峰值 800×0.5) — 之後 BBBB 零 tick
    L.append(_tick("books", "2026-09-01T08:59:40.000000", symbol="BBBB",
                   bids=[{"price": 50.0, "size": 100}], asks=[]))
    # 08:59:52 (凍結後): CCCC 出賣單 (鎖破);EEEE 這時才鎖 (hub 快照沒有)
    L.append(_tick("books", "2026-09-01T08:59:52.000000", symbol="CCCC",
                   bids=[{"price": 30.0, "size": 600}], asks=[{"price": 30.0, "size": 5}]))
    L.append(_tick("books", "2026-09-01T08:59:52.500000", symbol="EEEE",
                   bids=[{"price": 40.0, "size": 700}], asks=[]))
    # 08:59:53 AAAA/DDDD node 視窗內 tick (量不變 → 20% 母數兩邊一致)
    L.append(_tick("books", "2026-09-01T08:59:53.000000", symbol="AAAA",
                   bids=[{"price": 100.0, "size": 500}], asks=[]))
    L.append(_tick("books", "2026-09-01T08:59:53.500000", symbol="DDDD",
                   bids=[{"price": 20.0, "size": 900}], asks=[]))
    # 08:59:58.5 觸發預掛;09:00 收尾
    L.append(_tick("books", "2026-09-01T08:59:58.500000", symbol="AAAA",
                   bids=[{"price": 100.0, "size": 500}], asks=[]))
    L.append(_tick("books", "2026-09-01T09:00:00.100000", symbol="AAAA",
                   bids=[{"price": 0.0, "size": 50}, {"price": 100.0, "size": 500}], asks=[],
                   isContinuous=True))
    path.write_text("\n".join(L), encoding="utf-8")


def _args(tmp_path, t30="DDDD"):
    ticks = tmp_path / "t.jsonl"
    _write_ticks(ticks)
    lu = tmp_path / "lu.json"
    lu.write_text(json.dumps({"AAAA": 100.0, "BBBB": 50.0, "CCCC": 30.0,
                              "DDDD": 20.0, "EEEE": 40.0}), encoding="utf-8")
    ld = tmp_path / "ld.json"
    ld.write_text(json.dumps({"AAAA": 82.0, "DDDD": 16.4}), encoding="utf-8")
    dt = tmp_path / "dt.json"
    dt.write_text(json.dumps({"AAAA": True, "CCCC": False}), encoding="utf-8")
    return SimpleNamespace(
        ticks=str(ticks), limit_ups=str(lu), date="2026-09-01",
        dispositions="", limit_downs=str(ld), day_tradable=str(dt),
        fills="", orders="", t30=t30,
        total_budget=3_700_000, per_symbol=400_000, sizing_mode="budget", fixed_lots=0,
        bid_drop_ratio=0.5, report="",
        swap_delay=0.0, chase_cutoff="09:03:00")


@pytest.fixture
def restore_clock():
    orig = (filter_mod.datetime, trader_mod.datetime, ts_mod.datetime,
            state_mod.datetime, ts_mod._EXIT_FILL_WAIT_SEC, ts_mod.time)
    yield
    (filter_mod.datetime, trader_mod.datetime, ts_mod.datetime,
     state_mod.datetime, ts_mod._EXIT_FILL_WAIT_SEC, ts_mod.time) = orig


class TestHubNodeReplay:
    def test_snapshot_four_fields(self, tmp_path, restore_clock):
        ctx = run_hub_node(_args(tmp_path))
        m = {s["symbol"]: s for s in ctx["snap"]["symbols"]}
        # 凍結時 marked: AAAA/BBBB/CCCC/DDDD (EEEE 還沒鎖;CCCC 賣單在凍結後才出現)
        assert set(m) == {"AAAA", "BBBB", "CCCC", "DDDD"}
        assert m["BBBB"]["max_bid_vol"] == 800 and m["BBBB"]["last_bid_vol"] == 100
        assert m["AAAA"]["limit_down"] == 82.0 and m["AAAA"]["day_tradable"] is True
        assert m["CCCC"]["day_tradable"] is False
        assert m["DDDD"]["is_t30"] is True and m["AAAA"]["is_t30"] is False

    def test_semantic_pit_culled_without_window_ticks(self, tmp_path, restore_clock):
        # BBBB 凍結後零 tick — 靠快照 last=100 < 800×0.5 剔掉 (舊 seed last=max 會漏)
        ctx = run_hub_node(_args(tmp_path))
        assert "BBBB" in [c[0] for c in ctx["node_culled"]]
        assert "BBBB" not in ctx["node_pre_marked"]

    def test_lock_break_in_window_unmarked(self, tmp_path, restore_clock):
        # CCCC 08:59:52 出賣單 → node 完整 handler unmark + 退訂 (輕量 handler 時代會照預掛)
        ctx = run_hub_node(_args(tmp_path))
        assert "CCCC" in ctx["node_unsubbed"]
        assert "CCCC" not in ctx["node_pre_marked"]
        assert not any(s == "CCCC" for (k, s, p, l) in ctx["node_broker"].placed)

    def test_t30_not_preordered(self, tmp_path, restore_clock):
        ctx = run_hub_node(_args(tmp_path))
        assert ctx["node_session"].trades["DDDD"].stopped_reason == "full_cash_delivery"
        assert not any(s == "DDDD" and k == "limit_buy"
                       for (k, s, p, l) in ctx["node_broker"].placed)
        # 風控欄位到位
        assert ctx["node_session"].untradable == {"DDDD"}
        assert ctx["node_session"].limit_downs["AAAA"] == 82.0
        assert ctx["node_session"].day_tradable["CCCC"] is False

    def test_ab_verdict_pass(self, tmp_path, restore_clock):
        # 全 A/B: 差異只有預期類 (EEEE 凍結後才鎖 / DDDD T30 防線) → PASS
        args = _args(tmp_path)
        std = run_standalone(args)
        ctx = run_hub_node(args)
        _report, ok = diff_report(args, std, ctx)
        assert ok is True
        # AAAA 兩邊都預掛且相同
        assert ("AAAA" in ctx["node_pre_marked"]) and ("AAAA" in std.marked)
        # EEEE: standalone 有、node 沒 (凍結後才鎖 → 預期差)
        assert "EEEE" in std.marked and "EEEE" not in ctx["node_pre_marked"]
