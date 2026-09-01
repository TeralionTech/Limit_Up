"""node 端 _apply_marked_snapshot — Hub 快照灌 state/session (風控三欄 + 兩步 seed)。
2026-09-01: node 之前全缺 T30/跌停價/禁現沖 → 上真單前補齊;含語意坑 (seed last=max
→ 8 秒無 tick 的量崩檔剔不掉) 回歸。"""
from types import SimpleNamespace

from runner import Runner
from state import State


def _node_runner():
    r = Runner()                       # 直接建構 (不走 get() singleton;不 login)
    r.cfg = SimpleNamespace(role="node")
    r.state = State(bid_drop_ratio=0.5)
    return r


def _snap(symbols):
    return {"ts": "2026-09-01T08:59:50", "final": True, "role": "hub", "symbols": symbols}


ROW = {
    "symbol": "8105", "limit_up": 16.65, "is_disposition": False, "first_tick": True,
    "priority": 0, "max_bid_vol": 820, "last_bid_vol": 300,
    "limit_down": 13.65, "is_t30": False, "day_tradable": True,
}


class TestApplySnapshot:
    def test_risk_fields_reach_session(self):
        r = _node_runner()
        rows = [
            dict(ROW),
            {**ROW, "symbol": "9103", "is_t30": True, "day_tradable": False,
             "limit_down": 40.0, "first_tick": False, "priority": 1},
        ]
        marked = r._apply_marked_snapshot(_snap(rows))
        assert marked == ["8105", "9103"]           # 開盤即鎖優先
        assert r.session.untradable == {"9103"}     # T30 → place_pre_orders 標 full_cash_delivery
        assert r.session.limit_downs == {"8105": 13.65, "9103": 40.0}
        assert r.session.day_tradable == {"8105": True, "9103": False}

    def test_two_step_seed_last_neq_max(self):
        # 語意坑回歸: seed 後 last=Hub 最後量 (300)、max=峰值 (820) — 不是 last=max
        r = _node_runner()
        r._apply_marked_snapshot(_snap([dict(ROW)]))
        assert r.state.get_max_bid_size("8105") == 820
        assert r.state.get_last_bid_size("8105") == 300
        # 20% cap 兜底: subscriber 零 tick 時 pre-order timer 退用 Hub last
        assert r._node_bid_vol_fallback == {"8105": 300}

    def test_collapsed_stock_culled_without_new_ticks(self):
        # 盤前尾段量崩 (last 300 < 820×0.5=410) + node 8 秒內無新 tick → final_check_all 仍剔得掉
        r = _node_runner()
        r._apply_marked_snapshot(_snap([dict(ROW)]))
        dropped = r.state.final_check_all()
        assert [d[0] for d in dropped] == ["8105"]

    def test_backward_compat_old_snapshot(self):
        # 舊 hub 快照 (無新四欄) → last=max (原行為,不剔)、無 T30、day_tradable 預設 True
        r = _node_runner()
        old_row = {"symbol": "8105", "limit_up": 16.65, "is_disposition": False,
                   "first_tick": False, "priority": 0, "max_bid_vol": 820}
        r._apply_marked_snapshot(_snap([old_row]))
        assert r.state.get_last_bid_size("8105") == 820      # 缺欄退回峰值
        assert r.state.final_check_all() == []
        assert r.session.untradable == set()
        assert r.session.day_tradable == {"8105": True}
        assert r.session.limit_downs == {}

    def test_no_snapshot_safe(self):
        # 拉不到快照 → 空清單 (今日不交易),session 拿到空風控集,不炸
        r = _node_runner()
        assert r._apply_marked_snapshot(None) == []
        assert r.session.untradable == set()
