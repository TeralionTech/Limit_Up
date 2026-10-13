"""月均量硬限制 — 審查 (2026-10-14) 補強的案例:跨日重設、檔優先於 REST 補抓、0/NaN/inf 視為未知、
hub 不論自己的 pct 都讀檔補抓、已知檔後到的 avg_lots、個股覆寫 + 未知仍擋、CRITICAL 等級、cfg → session 接線、
standalone 預掛前的同步。"""
import json
import logging
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from fakes_cancel import make_session
from runner import Runner
from state import State

LIMIT = 45.0


def _s(pct=1.0):
    s = make_session(total=2_000_000, per_symbol=2_000_000)
    s.configure(avgvol_cap_pct=pct)
    return s


class TestDailyReset:
    def test_reset_clears_cache_and_file_wins_over_rest(self, tmp_path, monkeypatch):
        from test_marked_snapshot import _runner
        r = _runner()
        r.cfg = SimpleNamespace(role="hub", avgvol_cap_pct=1.0)
        r._avg_lots_fetched = {"6933": 3000.0}                 # 昨天 REST 補抓的
        r._avg_lots_attempts = {"6933": 2, "2330": 2}
        r.avg_lots = {"8105": 999.0}
        r._reset_daily_state()
        assert r._avg_lots_fetched == {} and r._avg_lots_attempts == {} and r.avg_lots == {} and r._avg_lots_map == {}
        f = tmp_path / "avg_volume.json"
        f.write_text(json.dumps({"date": "2026-10-13", "generated_at": "x", "avg_lots": {"6933": 1000.0, "8105": 1200.0}}),
                     encoding="utf-8")
        monkeypatch.setenv("AVG_VOLUME_FILE", str(f))
        r._load_avg_lots_map()
        assert r._avg_lots_map == {"6933": 1000.0, "8105": 1200.0}
        # 就算同一天又補抓到別的值,檔是權威
        r._avg_lots_fetched = {"6933": 3000.0, "2330": 50.0}
        assert r._avg_lots_for("6933") == 1000.0 and r._avg_lots_for("2330") == 50.0
        assert r._avg_lots_all() == {"6933": 1000.0, "8105": 1200.0, "2330": 50.0}
        got = {e["symbol"]: e["avg_lots"] for e in r.marked_snapshot()["symbols"]}
        assert got == {"8105": 1200.0, "6933": 1000.0, "2330": 50.0}

    def test_hub_loads_file_even_when_its_own_pct_is_zero_and_node_never_loads(self, tmp_path, monkeypatch):
        from test_marked_snapshot import _runner
        f = tmp_path / "avg_volume.json"
        f.write_text(json.dumps({"8105": 1200.0, "6933": 0.0, "2330": "abc", "1101": -5}), encoding="utf-8")  # 舊平面形 + 壞值
        monkeypatch.setenv("AVG_VOLUME_FILE", str(f))
        r = _runner()
        r.cfg = SimpleNamespace(role="hub", avgvol_cap_pct=0)
        r._load_avg_lots_map()
        assert r._avg_lots_map == {"8105": 1200.0}               # 0 / 字串 / 負 都不收
        n = _runner()
        n.cfg = SimpleNamespace(role="node", avgvol_cap_pct=1.0)
        n._load_avg_lots_map()
        assert n._avg_lots_map == {}

    def test_missing_file_is_critical_and_leaves_map_empty(self, tmp_path, monkeypatch, caplog):
        from test_marked_snapshot import _runner
        monkeypatch.setenv("AVG_VOLUME_FILE", str(tmp_path / "nope.json"))
        r = _runner()
        r.cfg = SimpleNamespace(role="hub", avgvol_cap_pct=1.0)
        r._load_avg_lots_map()
        assert r._avg_lots_map == {}
        assert [x for x in caplog.records if x.levelno == logging.CRITICAL and "月均量檔缺" in x.getMessage()]


class TestUnknownValues:
    @pytest.mark.parametrize("bad", [0, -1, float("nan"), float("inf"), "abc", None, 1e12])
    def test_avg_lots_for_treats_bad_values_as_unknown(self, bad):
        from test_marked_snapshot import _runner
        r = _runner()
        r._avg_lots_map = {"8105": bad}
        assert r._avg_lots_for("8105") is None
        assert r.marked_snapshot()["symbols"][0]["avg_lots"] is None or "8105" not in {
            e["symbol"] for e in r.marked_snapshot()["symbols"] if e["avg_lots"] is not None}

    def test_session_rejects_non_finite_and_sizing_never_overflows(self):
        s = _s()
        s.set_avg_lots({"1101": float("inf"), "2330": float("nan"), "2454": 1e12, "3008": 2864.1})
        assert s.avg_lots == {"3008": 2864.1}
        s.place_pre_orders(["1101", "3008"], {"1101": LIMIT, "3008": LIMIT})   # inf 的檔 = 未知 → 不下、不炸
        assert s.trades["1101"].stopped_reason == "avgvol_unknown" and s.trades["3008"].target_lots == 28

    def test_node_seed_ignores_zero_and_inf(self):
        from test_node_late_marks import _entry, _node
        e0, einf = _entry("4716"), _entry("8227")
        e0["avg_lots"], einf["avg_lots"] = 0.0, float("inf")
        r = _node(e0, einf)
        assert r.avg_lots == {} and r.session.avg_lots == {}


class TestLateValueForKnownSymbol:
    def test_catch_up_updates_avg_lots_of_a_known_symbol(self):
        from test_node_late_marks import _catch, _entry, _node, _snap
        first = _entry("4716")                                     # 首份快照: hub 還沒補抓到 → 沒有 avg_lots
        r = _node(first)
        assert r.session.avg_lots == {}
        later = _entry("4716")
        later["avg_lots"] = 1974.4
        _catch(r, [_snap(later)])
        assert r.avg_lots == {"4716": 1974.4} and r.session.avg_lots == {"4716": 1974.4}

    def test_catch_up_does_not_overwrite_a_known_value(self):
        from test_node_late_marks import _catch, _entry, _node, _snap
        first = _entry("4716")
        first["avg_lots"] = 1974.4
        r = _node(first)
        later = _entry("4716")
        later["avg_lots"] = 5.0
        _catch(r, [_snap(later)])
        assert r.session.avg_lots == {"4716": 1974.4}


class TestFailClosedStrict:
    def test_override_with_unknown_avg_is_blocked(self, caplog):
        s = _s()
        s.set_symbol_budgets({"1101": 10_000})
        s.place_pre_orders(["1101"], {"1101": LIMIT})
        st = s.trades["1101"]
        assert st.target_lots == 0 and st.stopped_reason == "avgvol_unknown" and s.budget_used == 0
        assert [p for p in s.broker.placed if p[0] in ("limit_buy", "market_buy")] == []
        assert [x for x in caplog.records if x.levelno == logging.CRITICAL and "月均量未知" in x.getMessage()]

    def test_cap_zero_restores_old_sizing_without_logs(self, caplog):
        s = _s(pct=0)
        s.set_symbol_budgets({"1101": 10_000})
        s.place_pre_orders(["1101"], {"1101": LIMIT})
        assert s.trades["1101"].target_lots == 1                   # 「最少一張」照舊
        assert not [x for x in caplog.records if "月均量" in x.getMessage() and x.levelno >= logging.WARNING]


class TestWiring:
    def test_runner_configures_session_from_cfg(self):
        r = Runner()
        r.session = make_session()
        r.cfg = SimpleNamespace(max_stock_price=500, order_min_interval_sec=0.2, cancel_pending_time="13:23:00",
                                order_max_per_sec=45, avgvol_cap_pct=0.5)
        r._configure_session()
        assert r.session.avgvol_cap_pct == 0.5
        r.cfg.avgvol_cap_pct = 0
        r._configure_session()
        assert r.session.avgvol_cap_pct == 0.0

    def test_load_config_reads_env(self, monkeypatch, tmp_path):
        import config
        pfx = tmp_path / "a.pfx"
        pfx.write_bytes(b"x")
        monkeypatch.setenv("FUBON_ACCOUNT_ID", "x")
        monkeypatch.setenv("FUBON_PASSWORD", "x")
        monkeypatch.setenv("FUBON_PFX_PATH", str(pfx))
        monkeypatch.setenv("AVGVOL_CAP_PCT", "0.5")
        assert config.load_config().avgvol_cap_pct == 0.5
        monkeypatch.delenv("AVGVOL_CAP_PCT")
        assert config.load_config().avgvol_cap_pct == 1.0           # 預設 1%


class TestStandaloneTimer:
    def test_pre_order_timer_pushes_avg_lots_and_caps(self):
        from test_node_late_marks import _FakeSub, _arm_timer_cfg
        r = Runner()
        r.session = make_session()                                  # 每檔 20 萬 / 20.95 → 9 張 (不綁)
        r.session.configure(avgvol_cap_pct=1.0)
        r.state = State(bid_drop_ratio=0.5)
        r.state.mark("8227", 20.95, 165, 20.95, first_tick=True)
        r.limit_ups = {"8227": 20.95}
        r.cfg = SimpleNamespace(role="standalone", hub_url="", avgvol_cap_pct=1.0)
        r.subscriber = _FakeSub()
        r._avg_lots_map = {"8227": 500.0}                          # 1% = 5 張 < 9 → 月均量綁
        r._avg_lots_fetched = {"9999": 50.5}
        _arm_timer_cfg(r)
        r.session.start_market_chase = lambda symbols, start, cutoff: None
        r._start_pre_order_timer()
        r._timer_threads[-1].join(4)
        assert not r._timer_threads[-1].is_alive()
        assert r.session.avg_lots == {"8227": 500.0, "9999": 50.5}
        st = r.session.trades["8227"]
        assert st.target_lots == 5 and st.size_cap_reason == "avgvol_cap" and st.avgvol_cap_lots == 5
