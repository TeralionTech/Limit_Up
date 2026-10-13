"""A3 GET /api/central/status — central poller 的單一彙整 (形狀 + ready pre-flight + 子項例外不 500)。
每測試用**全新 Runner()** (不走 get() singleton) 注入 api.Runner.get,互不污染。"""
from types import SimpleNamespace

import pytest

pytest.importorskip("fastapi")

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import api  # noqa: E402
from runner import Runner  # noqa: E402
from state import State  # noqa: E402

TOP_KEYS = {"ts", "role", "node_name", "version", "config", "runner", "marked", "trading",
            "spending", "orders", "overnight", "t30", "ready"}
READY_KEYS = {"mode_real", "connected", "healthy", "budget_set", "armed", "is_live", "block_reason"}


def _fake_broker(connected=True, healthy=True):
    return SimpleNamespace(
        connected=connected, healthy=healthy,
        status=lambda: {"connected": connected, "healthy": healthy,
                        "account_masked": "F12***", "is_test": False, "error": ""},
    )


@pytest.fixture
def r(monkeypatch):
    runner = Runner()
    monkeypatch.setattr(api.Runner, "get", staticmethod(lambda: runner))
    return runner


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(api.router)
    return TestClient(app)


def _get(client):
    resp = client.get("/api/central/status")
    assert resp.status_code == 200, resp.text
    return resp.json()


class TestShape:
    def test_top_level_keys_idle_runner(self, client, r, monkeypatch):
        # 08:00 前 / 重啟後: runner 沒跑、cfg=None、無 state → 每個子項都要有、不能 500
        monkeypatch.setenv("NODE_NAME", "node9")
        d = _get(client)
        assert set(d) == TOP_KEYS
        assert d["node_name"] == "node9"
        assert isinstance(d["version"], str) and d["version"]
        assert d["runner"] == {
            "phase": "idle", "is_running": False, "error": "", "watchlist_size": 0,
            "limit_up_progress": {"done": 0, "total": 0, "ok": 0, "fail": 0},
            "recorder_tick_count": 0,
        }
        assert d["marked"]["final"] is False and d["marked"]["count"] == 0
        assert d["marked"]["symbols"] == [] and d["marked"]["ts"]
        assert set(d["trading"]) >= {"mode", "armed", "connecting", "connect_error", "connected",
                                     "healthy", "account_masked", "is_test", "error", "params",
                                     "budget_used", "buy_cost_actual", "budget_breached",
                                     "n_symbols", "n_positions"}
        assert set(d["trading"]["params"]) == {"sizing_mode", "fixed_lots", "total_budget",
                                               "per_symbol_budget"}
        assert d["spending"] == {"total_budget": 0, "total_spent": 0, "total_over": 0,
                                 "budget_breached": False, "n_over_symbols": 0}
        assert d["orders"] == {"total": 0, "pending": 0, "filled": 0, "rejected": 0,
                               "cancelled": 0, "pre_limit": 0, "market_buy": 0}
        assert d["overnight"] == {"count": 0, "pending_sell": 0}
        assert d["t30"] == {"count": 0}
        assert set(d["ready"]) == READY_KEYS

    def test_role_and_config_from_env_when_cfg_none(self, client, r, monkeypatch):
        monkeypatch.setenv("ROLE", "Node")
        monkeypatch.setenv("HUB_URL", "http://hub:8100")
        monkeypatch.setenv("HUB_FREEZE_TIME", "08:59:50")
        monkeypatch.setenv("PRE_ORDER_TIME", "08:59:58")
        monkeypatch.setenv("CANCEL_PENDING_TIME", "13:23:00")
        monkeypatch.setenv("TRADING_END_TIME", "13:24:00")
        d = _get(client)
        assert d["role"] == "node"
        assert d["config"] == {"hub_url": "http://hub:8100", "hub_freeze_time": "08:59:50",
                               "pre_order_time": "08:59:58", "cancel_pending_time": "13:23:00",
                               "trading_end_time": "13:24:00"}

    def test_role_and_config_from_cfg(self, client, r):
        r.cfg = SimpleNamespace(role="hub", hub_url="", hub_freeze_time="08:59:40",
                                pre_order_time="08:59:57", cancel_pending_time="13:22:00",
                                trading_end_time="13:25:00")
        d = _get(client)
        assert d["role"] == "hub"
        assert d["config"]["hub_freeze_time"] == "08:59:40"
        assert d["config"]["pre_order_time"] == "08:59:57"
        assert d["config"]["cancel_pending_time"] == "13:22:00"
        assert d["config"]["trading_end_time"] == "13:25:00"

    def test_node_name_falls_back_to_hostname(self, client, r, monkeypatch):
        import socket
        monkeypatch.delenv("NODE_NAME", raising=False)
        assert _get(client)["node_name"] == socket.gethostname()

    def test_marked_symbols_only_codes_prioritized(self, client, r):
        r.cfg = SimpleNamespace(role="hub", hub_url="", hub_freeze_time="", pre_order_time="",
                                cancel_pending_time="", trading_end_time="")
        r.state = State(bid_drop_ratio=0.5)
        r.limit_ups = {"8105": 16.65, "6933": 260.0}
        r.state.mark("6933", 260.0, 100, 260.0, first_tick=False)
        r.state.mark("8105", 16.65, 500, 16.65, first_tick=True)
        r._marked_frozen = True
        d = _get(client)
        assert d["marked"]["final"] is True and d["marked"]["count"] == 2
        assert d["marked"]["symbols"] == ["8105", "6933"]        # 開盤即鎖優先 (同 marked_snapshot)
        assert d["runner"]["watchlist_size"] == 2

    def test_orders_counts(self, client, r):
        s = r.session
        s._log_order("A1", "8105", "buy", "pre_limit", 2, 16.65)
        s._log_order("A2", "6933", "buy", "market_buy", 1, 0)
        s._log_order("A3", "6933", "buy", "market_buy", 1, 0)
        s._log_order("A4", "2330", "sell", "market_sell", 1, 0)
        s._log_order("A5", "8105", "buy", "market_buy", 1, 0)
        s._mark_order("A1", "filled")
        s._mark_order("A2", "rejected")
        s._mark_order("A3", "cancelled")
        d = _get(client)
        assert d["orders"] == {"total": 5, "pending": 2, "filled": 1, "rejected": 1,
                               "cancelled": 1, "pre_limit": 1, "market_buy": 3}

    def test_spending_drops_symbol_rows_counts_over(self, client, r, monkeypatch):
        monkeypatch.setattr(r.session, "spending_summary", lambda: {
            "total_budget": 1000000, "total_spent": 1200000, "total_over": 200000,
            "budget_breached": True,
            "symbols": [{"symbol": "8105", "over": 5000}, {"symbol": "6933", "over": 0},
                        {"symbol": "2330", "over": 1}],
        })
        d = _get(client)
        assert d["spending"] == {"total_budget": 1000000, "total_spent": 1200000,
                                 "total_over": 200000, "budget_breached": True,
                                 "n_over_symbols": 2}
        assert "symbols" not in d["spending"]

    def test_overnight_pending_sell(self, client, r):
        def row(lots, sold_no=None, skip=False):
            return {"lots": lots, "avg_cost": 10.0, "reconciled": True, "note": "",
                    "sell_placed": bool(sold_no), "sell_price": 0.0, "sell_order_no": sold_no,
                    "skip": skip}
        s = r.session
        s.overnight = {"1101": row(2), "1102": row(1, skip=True), "1103": row(3, "S1")}
        s._log_order("S1", "1103", "sell", "market_sell", 3, 0)
        s.order_log["S1"]["filled_lots"] = 3
        s._mark_order("S1", "filled")
        d = _get(client)
        # 1101 待賣 / 1102 skip / 1103 已賣完 → pending_sell 1
        assert d["overnight"] == {"count": 3, "pending_sell": 1}

    def test_t30_count(self, client, r):
        r.session.set_untradable({"9999", "8888"})
        assert _get(client)["t30"] == {"count": 2}

    def test_version_fallback_when_git_fails(self, monkeypatch):
        api._app_version.cache_clear()
        try:
            def _boom(*a, **kw):
                raise FileNotFoundError("git")
            monkeypatch.setattr(api.subprocess, "run", _boom)
            assert api._app_version() == "0.1"
        finally:
            api._app_version.cache_clear()


class TestReadyBlockReason:
    """複製 set_armed pre-flight 順序: mode → 連線健康 → 總預算 → 每檔上限/張數。不真的 arm。"""

    def test_sim_mode(self, client, r):
        d = _get(client)["ready"]
        assert d["block_reason"] == "模擬模式"
        assert d["mode_real"] is False and d["is_live"] is False and d["armed"] is False

    def test_real_not_connected(self, client, r):
        r.session.set_mode("real")
        d = _get(client)["ready"]
        assert d["block_reason"] == "券商未連線或不健康"
        assert d["mode_real"] is True and d["connected"] is False and d["healthy"] is False

    def test_real_connected_but_unhealthy(self, client, r):
        r.session.set_mode("real")
        r.session.broker = _fake_broker(connected=True, healthy=False)
        d = _get(client)["ready"]
        assert d["block_reason"] == "券商未連線或不健康"
        assert d["connected"] is True and d["healthy"] is False

    def test_no_total_budget(self, client, r):
        r.session.set_mode("real")
        r.session.broker = _fake_broker()
        d = _get(client)["ready"]
        assert d["block_reason"] == "未設總預算"
        assert d["budget_set"] is False

    def test_no_per_symbol_budget(self, client, r):
        r.session.set_mode("real")
        r.session.broker = _fake_broker()
        r.session.set_params(total_budget=1_000_000, sizing_mode="budget", per_symbol_budget=0)
        d = _get(client)["ready"]
        assert d["block_reason"] == "未設每檔上限或張數"
        assert d["budget_set"] is False

    def test_fixed_lots_zero(self, client, r):
        r.session.set_mode("real")
        r.session.broker = _fake_broker()
        r.session.set_params(total_budget=1_000_000, sizing_mode="fixed_lots", fixed_lots=0,
                             per_symbol_budget=50_000)     # per_symbol 有值也不算 (fixed_lots 模式看張數)
        assert _get(client)["ready"]["block_reason"] == "未設每檔上限或張數"

    def test_all_green_not_armed(self, client, r):
        r.session.set_mode("real")
        r.session.broker = _fake_broker()
        r.session.set_params(total_budget=1_000_000, sizing_mode="fixed_lots", fixed_lots=1)
        d = _get(client)["ready"]
        assert d["block_reason"] == ""
        assert d["budget_set"] is True and d["armed"] is False and d["is_live"] is False
        assert r.session.armed is False       # 突變必殺: 求值時偷 arm

    def test_all_green_armed_is_live(self, client, r):
        r.session.set_mode("real")
        r.session.broker = _fake_broker()
        r.session.set_params(total_budget=1_000_000, sizing_mode="budget", per_symbol_budget=50_000)
        r.session.set_armed(True)
        d = _get(client)["ready"]
        assert d == {"mode_real": True, "connected": True, "healthy": True, "budget_set": True,
                     "armed": True, "is_live": True, "block_reason": ""}

    def test_armed_but_broker_gone_not_live(self, client, r):
        # 交易 WS 斷線: armed 仍 True 但 healthy False → is_live False + block 連線
        r.session.set_mode("real")
        r.session.broker = _fake_broker()
        r.session.set_params(total_budget=1_000_000, sizing_mode="fixed_lots", fixed_lots=1)
        r.session.set_armed(True)
        r.session.broker = _fake_broker(healthy=False)
        d = _get(client)["ready"]
        assert d["armed"] is True and d["is_live"] is False
        assert d["block_reason"] == "券商未連線或不健康"


class TestSubItemExceptions:
    def test_marked_exception_does_not_500(self, client, r, monkeypatch):
        def _boom():
            raise RuntimeError("state 壞了")
        monkeypatch.setattr(r, "marked_snapshot", _boom)
        d = _get(client)
        assert d["marked"] is None
        assert d["runner"] is not None and d["trading"] is not None and d["ready"] is not None

    def test_runner_status_exception_does_not_500(self, client, r, monkeypatch):
        def _boom():
            raise RuntimeError("x")
        monkeypatch.setattr(r, "get_status", _boom)
        d = _get(client)
        assert d["runner"] is None and d["marked"] is not None

    def test_trading_status_exception_only_nulls_trading_and_ready(self, client, r, monkeypatch):
        def _boom():
            raise RuntimeError("broker.status 炸")
        monkeypatch.setattr(r.session, "status", _boom)
        d = _get(client)
        assert d["trading"] is None and d["ready"] is None
        assert d["orders"] is not None and d["spending"] is not None and d["overnight"] is not None

    def test_config_exception_does_not_500(self, client, r):
        r.cfg = SimpleNamespace(role="hub")        # 缺其他欄位 → AttributeError
        d = _get(client)
        assert d["config"] is None and d["role"] == "hub"
