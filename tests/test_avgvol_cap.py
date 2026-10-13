"""月均量硬限制 (2026-10-14 使用者定案): 每檔張數 ≤ floor(近 20 日均量(張) × AVGVOL_CAP_PCT/100)。

- _sized_lots 最後一道 (疊在預算 / 個股金額覆寫「最少一張」/ 禁現沖減半 / 20% 委託量上限之後);
- 月均量未知 → 0 張、stopped_reason=avgvol_unknown (fail-closed,CRITICAL);月均量 × pct 不足 1 張 → 0 張、avgvol_cap;
- AVGVOL_CAP_PCT=0 → 停用 (行為同修法前);
- hub 的 marked 快照每檔帶 avg_lots (檔 → REST 補抓),node 由快照累積並交給 session (首份 + 凍結後補收)。
"""
from types import SimpleNamespace

import pytest

from fakes_cancel import make_session

LIMIT = 45.0            # 每張 45,000


def _s(pct=1.0, per_symbol=2_000_000, total=2_000_000):
    s = make_session(total=total, per_symbol=per_symbol)      # 200 萬 / 4.5 萬 = 44 張 (預算不綁)
    s.configure(avgvol_cap_pct=pct)
    return s


def _buys(b):
    return [p for p in b.placed if p[0] in ("limit_buy", "market_buy")]


class TestSizing:
    def test_cap_binds_and_floors(self):
        s = _s()
        s.set_avg_lots({"1101": 2864.1})                      # 1% = 28.641 → 28 張
        s.place_pre_orders(["1101"], {"1101": LIMIT})
        st = s.trades["1101"]
        assert st.target_lots == 28 and st.chase_lots == 28
        assert st.avg_lots == 2864.1 and st.avgvol_cap_lots == 28 and st.size_cap_reason == "avgvol_cap"
        assert _buys(s.broker) == [("limit_buy", "1101", LIMIT, 28)]

    def test_not_binding_leaves_budget_sizing(self):
        s = _s()
        s.set_avg_lots({"1101": 20000.0})                     # 1% = 200 > 44 → 預算綁
        s.place_pre_orders(["1101"], {"1101": LIMIT})
        assert s.trades["1101"].target_lots == 44 and s.trades["1101"].size_cap_reason == ""
        assert s.trades["1101"].avgvol_cap_lots == 200

    def test_unknown_avg_blocks_the_symbol(self, caplog):
        s = _s()
        s.set_avg_lots({"2330": 23142.4})                     # 1101 不在名單
        s.place_pre_orders(["1101"], {"1101": LIMIT})
        st = s.trades["1101"]
        assert st.target_lots == 0 and st.stopped_reason == "avgvol_unknown" and st.size_cap_reason == "avgvol_unknown"
        assert _buys(s.broker) == [] and s.budget_used == 0
        assert "月均量未知" in caplog.text
        # 盲送也不會送: stopped_reason 擋住
        assert s._chase_send_one("1101", 5) == "stopped"

    def test_below_one_lot_blocks(self):
        s = _s()
        s.set_avg_lots({"1101": 99.9})                        # 1% = 0.999 → 0 張
        s.place_pre_orders(["1101"], {"1101": LIMIT})
        assert s.trades["1101"].target_lots == 0 and s.trades["1101"].stopped_reason == "avgvol_cap"
        assert _buys(s.broker) == []

    def test_cap_disabled_when_pct_zero(self):
        s = _s(pct=0)
        s.place_pre_orders(["1101"], {"1101": LIMIT})         # 沒有月均量也照常
        assert s.trades["1101"].target_lots == 44 and s.trades["1101"].avgvol_cap_lots is None

    def test_symbol_override_min_one_lot_is_still_capped(self):
        # 個股金額覆寫「最少一張」不能突破硬限制: 月均量 50 張 × 1% = 0 → 不下
        s = _s()
        s.set_symbol_budgets({"1101": 10_000})
        s.set_avg_lots({"1101": 50.0})
        s.place_pre_orders(["1101"], {"1101": LIMIT})
        assert s.trades["1101"].target_lots == 0 and _buys(s.broker) == []

    def test_stacks_with_day_trade_halving_and_wall_cap(self):
        # 預算 44 → 禁現沖 22 → 20% × 50 = 10 → 月均量 1% × 2864 = 28 → 最小 10
        s = _s()
        s.set_day_tradable({"1101": False})
        s.set_avg_lots({"1101": 2864.1})
        s.place_pre_orders(["1101"], {"1101": LIMIT}, limit_up_bid_vols={"1101": 50})
        assert s.trades["1101"].target_lots == 10 and s.trades["1101"].size_cap_reason == ""
        # 反過來: 月均量 500 × 1% = 5 < 10 → 月均量綁
        s2 = _s()
        s2.set_day_tradable({"1101": False})
        s2.set_avg_lots({"1101": 500.0})
        s2.place_pre_orders(["1101"], {"1101": LIMIT}, limit_up_bid_vols={"1101": 50})
        assert s2.trades["1101"].target_lots == 5 and s2.trades["1101"].size_cap_reason == "avgvol_cap"

    def test_pct_other_than_one(self):
        s = _s(pct=0.5)
        s.set_avg_lots({"1101": 2864.1})                      # 0.5% = 14.3 → 14
        s.place_pre_orders(["1101"], {"1101": LIMIT})
        assert s.trades["1101"].target_lots == 14

    def test_set_avg_lots_drops_bad_values(self):
        s = _s()
        s.set_avg_lots({"1101": None, "2330": "abc", "2454": 0, "3008": "1234.5"})
        assert s.avg_lots == {"3008": 1234.5}

    def test_status_exposes_cap(self):
        s = _s()
        s.set_avg_lots({"1101": 2864.1})
        st = s.status()
        assert st["avgvol_cap_pct"] == 1.0 and st["avg_lots_known"] == 1
        assert set(st["params"]) == {"sizing_mode", "fixed_lots", "total_budget", "per_symbol_budget"}  # 中控 shape 不變


class TestHubSnapshot:
    def test_snapshot_carries_avg_lots_from_file_and_rest_fill(self):
        from test_marked_snapshot import _runner
        r = _runner()
        r._avg_lots_map = {"8105": 1200.0, "2330": 23142.4}
        r._avg_lots_fetched = {"6933": 50.5}                  # 檔裡沒有、盤前 REST 補抓到
        got = {e["symbol"]: e["avg_lots"] for e in r.marked_snapshot()["symbols"]}
        assert got == {"8105": 1200.0, "2330": 23142.4, "6933": 50.5}

    def test_snapshot_reports_none_when_unknown(self):
        from test_marked_snapshot import _runner
        r = _runner()
        r._avg_lots_map = {"8105": 1200.0}
        got = {e["symbol"]: e["avg_lots"] for e in r.marked_snapshot()["symbols"]}
        assert got["8105"] == 1200.0 and got["6933"] is None and got["2330"] is None


class TestNodeFromSnapshot:
    def test_first_snapshot_feeds_session(self):
        from test_node_late_marks import _entry, _node
        e = _entry("4716")
        e["avg_lots"] = 1974.4
        e2 = _entry("8227")                                     # 沒帶 → 未知
        r = _node(e, e2)
        assert r.avg_lots == {"4716": 1974.4}
        assert r.session.avg_lots == {"4716": 1974.4}

    def test_late_mark_catch_up_feeds_session(self):
        from test_node_late_marks import _catch, _entry, _node, _snap
        r = _node()
        e = _entry("4716")
        e["avg_lots"] = 1974.4
        res = _catch(r, [_snap(), _snap(e)])
        assert res["added"] == ["4716"]
        assert r.session.avg_lots["4716"] == 1974.4

    def test_snapshot_without_any_avg_lots_is_loud_only_when_cap_on(self, caplog):
        import logging
        from runner import Runner
        from state import State
        from test_node_late_marks import _entry, _snap
        r = Runner()
        r.session = make_session()
        r.state = State(bid_drop_ratio=0.5)
        r.cfg = SimpleNamespace(role="node", hub_url="http://hub:8100", avgvol_cap_pct=1.0)
        r._apply_marked_snapshot(_snap(_entry("4716")))
        assert r.session.avg_lots == {}
        crit = [x for x in caplog.records if x.levelno == logging.CRITICAL and "avg_lots" in x.getMessage()]
        assert crit, "cap 開著、快照全無 avg_lots → 要 CRITICAL"
        caplog.clear()
        r2 = Runner()
        r2.session = make_session()
        r2.state = State(bid_drop_ratio=0.5)
        r2.cfg = SimpleNamespace(role="node", hub_url="http://hub:8100", avgvol_cap_pct=0)
        r2._apply_marked_snapshot(_snap(_entry("4716")))
        assert not [x for x in caplog.records if x.levelno == logging.CRITICAL], "cap 關著不可假警報"

    def test_bad_avg_value_is_treated_as_unknown(self):
        from test_node_late_marks import _entry, _node
        e = _entry("4716")
        e["avg_lots"] = "abc"
        r = _node(e)
        assert "4716" in r.state.get_marked_prioritized()        # 其他欄位照常 seed
        assert r.avg_lots == {}


class TestAvgVolumeHelpers:
    def test_compute_avg_lots_matches_script(self):
        import avg_volume
        from scripts.compute_avg_volume import compute_avg_lots as script_fn
        candles = [{"date": f"2026-09-{d:02d}", "volume": 1000 * d} for d in range(1, 31)]
        assert avg_volume.compute_avg_lots(candles, 20) == script_fn(candles, 20)
        assert avg_volume.compute_avg_lots([], 20) == 0.0

    def test_fetch_avg_lots_uses_rest_and_handles_failure(self):
        import avg_volume

        class _Hist:
            def __init__(self, data=None, raise_=False):
                self.data, self.raise_, self.calls = data, raise_, []

            def candles(self, **kw):
                self.calls.append(kw)
                if self.raise_:
                    raise RuntimeError("rate limit")
                return {"data": self.data}

        ok = SimpleNamespace(historical=_Hist([{"date": "2026-10-13", "volume": 2_000_000},
                                               {"date": "2026-10-10", "volume": 1_000_000}]))
        assert avg_volume.fetch_avg_lots(ok, "9103") == 1500.0
        assert ok.historical.calls[0]["symbol"] == "9103" and ok.historical.calls[0]["timeframe"] == "D"
        bad = SimpleNamespace(historical=_Hist(raise_=True))
        assert avg_volume.fetch_avg_lots(bad, "9103") is None
        empty = SimpleNamespace(historical=_Hist([]))
        assert avg_volume.fetch_avg_lots(empty, "9103") is None


class TestRunnerFiller:
    def test_filler_fetches_only_unknown_marked_symbols(self, monkeypatch):
        """hub 補抓迴圈: marked 裡有檔的不抓、沒檔的抓一次 (最多 2 次),結果進 _avg_lots_fetched。"""
        import avg_volume
        from test_marked_snapshot import _runner
        r = _runner()
        r.cfg = SimpleNamespace(role="hub", avgvol_cap_pct=1.0)
        r._avg_lots_map = {"8105": 1200.0}
        calls = []
        monkeypatch.setattr(avg_volume, "fetch_avg_lots",
                            lambda stock, sym, **kw: calls.append(sym) or (50.5 if sym == "6933" else None))
        r.sdk = SimpleNamespace(marketdata=SimpleNamespace(rest_client=SimpleNamespace(stock=object())))
        from datetime import time as _t
        times = iter([_t(8, 40), _t(8, 42), _t(8, 44), _t(9, 1)])      # 三輪有效 + 第四輪到 09:01 結束
        t = r._start_avg_lots_filler(period=0.05, call_gap=0.01, now_fn=lambda: next(times))
        t.join(5)
        assert not t.is_alive(), "09:00:05 (注入的 now_fn) 之後迴圈要結束"
        # 第一輪: 6933 抓到 / 2330 失敗;第二輪: 2330 再試一次;第三輪: 2330 已到上限 2 次,不再抓;8105 在檔裡不抓
        assert sorted(calls) == ["2330", "2330", "6933"]
        assert r._avg_lots_fetched == {"6933": 50.5}
        assert r._avg_lots_attempts == {"2330": 2, "6933": 1}
        assert r._avg_lots_all() == {"8105": 1200.0, "6933": 50.5}

    def test_filler_throttles_rest_calls(self, monkeypatch):
        import time as _time
        import avg_volume
        from test_marked_snapshot import _runner
        r = _runner()
        r.cfg = SimpleNamespace(role="hub", avgvol_cap_pct=1.0)
        stamps = []
        monkeypatch.setattr(avg_volume, "fetch_avg_lots", lambda stock, sym, **kw: stamps.append(_time.monotonic()) or 10.0)
        r.sdk = SimpleNamespace(marketdata=SimpleNamespace(rest_client=SimpleNamespace(stock=object())))
        from datetime import time as _t
        times = iter([_t(9, 1)])
        t = r._start_avg_lots_filler(period=0.05, call_gap=0.2, now_fn=lambda: next(times))
        t.join(5)
        assert not t.is_alive() and len(stamps) == 3                   # 8105 / 6933 / 2330 都不在檔裡
        gaps = [b - a for a, b in zip(stamps, stamps[1:])]
        assert min(gaps) >= 0.18, gaps                                   # 每次 REST 之間至少 call_gap

    def test_filler_runs_on_hub_regardless_of_its_own_pct_but_not_on_node(self):
        from test_marked_snapshot import _runner
        r = _runner()
        r.cfg = SimpleNamespace(role="node", avgvol_cap_pct=1.0)
        n = len(r._timer_threads)
        assert r._start_avg_lots_filler() is None and len(r._timer_threads) == n
        r.cfg = SimpleNamespace(role="hub", avgvol_cap_pct=0)          # hub 的 pct 對快照沒意義 → 照跑
        r.sdk = None                                                     # 沒 sdk → 迴圈空轉到 end_time
        from datetime import time as _t
        t = r._start_avg_lots_filler(period=0.01, now_fn=lambda: _t(9, 1))
        assert t is not None
        t.join(3)
        assert not t.is_alive()
