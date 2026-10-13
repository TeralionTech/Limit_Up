"""A1 token 中介層 (server.py) — POST/PUT/DELETE /api/* 需 X-Limitup-Token (fail-closed);
GET / OPTIONS 開放;真本機 loopback (無 CF-Connecting-IP) 放行。

走 fastapi TestClient 打 server.app (含 CORS 外層 + token 內層),不觸發 lifespan (不用 with)。"""
import importlib.util
import sys
import types

import pytest

pytest.importorskip("fastapi")


def _stub_apscheduler_if_missing():
    """本機開發環境可能沒裝 apscheduler (CI 有,requirements.txt pin 3.10.4)。
    server.py 只在 lifespan 用它,TestClient 不 with → 永不呼叫 → 塞空殼讓 import 過。"""
    if importlib.util.find_spec("apscheduler") is not None:
        return
    pkg = types.ModuleType("apscheduler")
    sched = types.ModuleType("apscheduler.schedulers")
    bg = types.ModuleType("apscheduler.schedulers.background")
    bg.BackgroundScheduler = object
    trig = types.ModuleType("apscheduler.triggers")
    cron = types.ModuleType("apscheduler.triggers.cron")
    cron.CronTrigger = object
    for name, mod in (("apscheduler", pkg), ("apscheduler.schedulers", sched),
                      ("apscheduler.schedulers.background", bg),
                      ("apscheduler.triggers", trig), ("apscheduler.triggers.cron", cron)):
        sys.modules.setdefault(name, mod)


_stub_apscheduler_if_missing()

import server  # noqa: E402  (需在 stub 之後)
from fastapi.testclient import TestClient  # noqa: E402

TOKEN = "t0ken-abc"
# 無副作用的受保護 endpoint: /trading/params 空 body → set_params 全 None → 什麼都不改
POST_PATH = "/api/trading/params"


def _with_client(app, host: str):
    """把 ASGI scope 的 client 改成指定來源 IP (starlette 0.27 TestClient 沒有 client= 參數)。"""
    async def wrapped(scope, receive, send):
        if scope["type"] == "http":
            scope = dict(scope)
            scope["client"] = (host, 54321)
        await app(scope, receive, send)
    return wrapped


@pytest.fixture
def remote():
    """遠端來源 (TestClient 預設 client host = "testclient",非 loopback)。"""
    return TestClient(server.app)


@pytest.fixture
def loopback():
    return TestClient(_with_client(server.app, "127.0.0.1"))


@pytest.fixture
def loopback6():
    return TestClient(_with_client(server.app, "::1"))


class TestGuardedMethods:
    def test_post_without_token_401(self, remote, monkeypatch):
        monkeypatch.setenv("LIMITUP_API_TOKEN", TOKEN)
        resp = remote.post(POST_PATH, json={})
        assert resp.status_code == 401
        assert resp.json() == {"detail": "unauthorized"}

    def test_post_wrong_token_401(self, remote, monkeypatch):
        # 突變必殺: 比對弱化成「有 header 就過」
        monkeypatch.setenv("LIMITUP_API_TOKEN", TOKEN)
        resp = remote.post(POST_PATH, json={}, headers={"X-Limitup-Token": TOKEN + "x"})
        assert resp.status_code == 401

    def test_post_correct_token_passes(self, remote, monkeypatch):
        monkeypatch.setenv("LIMITUP_API_TOKEN", TOKEN)
        resp = remote.post(POST_PATH, json={}, headers={"X-Limitup-Token": TOKEN})
        assert resp.status_code == 200
        assert resp.json() == {"ok": True}

    def test_header_name_case_insensitive(self, remote, monkeypatch):
        monkeypatch.setenv("LIMITUP_API_TOKEN", TOKEN)
        resp = remote.post(POST_PATH, json={}, headers={"x-limitup-token": TOKEN})
        assert resp.status_code == 200

    def test_put_and_delete_guarded_before_routing(self, remote, monkeypatch):
        # 中介層在 routing 之前: 就算 route 不存在,無 token 也先 401 (不是 404/405)
        monkeypatch.setenv("LIMITUP_API_TOKEN", TOKEN)
        assert remote.put("/api/whatever", json={}).status_code == 401
        assert remote.delete("/api/whatever").status_code == 401
        # 帶對 token → 過中介層,到 router 才 404/405 (不再是 401)
        assert remote.delete("/api/whatever", headers={"X-Limitup-Token": TOKEN}).status_code != 401

    def test_env_unset_fail_closed_401(self, remote, monkeypatch):
        # 突變必殺: env 未設改成「開放」→ 這裡會 200
        monkeypatch.delenv("LIMITUP_API_TOKEN", raising=False)
        assert remote.post(POST_PATH, json={}).status_code == 401
        # 連「空字串 token」也不能配到空 env
        assert remote.post(POST_PATH, json={}, headers={"X-Limitup-Token": ""}).status_code == 401

    def test_env_empty_string_fail_closed_401(self, remote, monkeypatch):
        monkeypatch.setenv("LIMITUP_API_TOKEN", "")
        assert remote.post(POST_PATH, json={}, headers={"X-Limitup-Token": ""}).status_code == 401

    def test_env_read_per_request(self, remote, monkeypatch):
        # 免重啟語意: 同一 app 換 env 立刻生效
        monkeypatch.setenv("LIMITUP_API_TOKEN", "one")
        assert remote.post(POST_PATH, json={}, headers={"X-Limitup-Token": "two"}).status_code == 401
        monkeypatch.setenv("LIMITUP_API_TOKEN", "two")
        assert remote.post(POST_PATH, json={}, headers={"X-Limitup-Token": "two"}).status_code == 200


class TestOpenMethods:
    def test_get_open_without_token(self, remote, monkeypatch):
        monkeypatch.setenv("LIMITUP_API_TOKEN", TOKEN)
        assert remote.get("/api/trading/status").status_code == 200
        assert remote.get("/api/status").status_code == 200
        assert remote.get("/api/marked-snapshot").status_code == 200

    def test_get_open_even_when_env_unset(self, remote, monkeypatch):
        monkeypatch.delenv("LIMITUP_API_TOKEN", raising=False)
        assert remote.get("/api/trading/status").status_code == 200

    def test_get_unknown_route_404_not_401(self, remote, monkeypatch):
        monkeypatch.setenv("LIMITUP_API_TOKEN", TOKEN)
        assert remote.get("/api/nope").status_code == 404

    def test_options_open(self, remote, monkeypatch):
        monkeypatch.setenv("LIMITUP_API_TOKEN", TOKEN)
        # 純 OPTIONS (無 CORS header) → 進到 router,不是 401
        assert remote.options(POST_PATH).status_code != 401

    def test_cors_preflight_handled_outside_token_guard(self, remote, monkeypatch):
        # CORS 在外層: dev 前端 preflight 不帶 token 也要 200 + allow-origin
        monkeypatch.setenv("LIMITUP_API_TOKEN", TOKEN)
        resp = remote.options(POST_PATH, headers={
            "Origin": "http://localhost:5173",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "x-limitup-token,content-type",
        })
        assert resp.status_code == 200
        assert resp.headers.get("access-control-allow-origin") == "http://localhost:5173"


class TestLoopback:
    def test_true_loopback_allowed_without_token(self, loopback, monkeypatch):
        # scripts/connect_real_nodes.py 打 localhost 不帶 header 要能過
        monkeypatch.setenv("LIMITUP_API_TOKEN", TOKEN)
        assert loopback.post(POST_PATH, json={}).status_code == 200

    def test_true_loopback_allowed_even_env_unset(self, loopback, monkeypatch):
        monkeypatch.delenv("LIMITUP_API_TOKEN", raising=False)
        assert loopback.post(POST_PATH, json={}).status_code == 200

    def test_ipv6_loopback_allowed(self, loopback6, monkeypatch):
        monkeypatch.setenv("LIMITUP_API_TOKEN", TOKEN)
        assert loopback6.post(POST_PATH, json={}).status_code == 200

    def test_loopback_with_cf_header_not_trusted(self, loopback, monkeypatch):
        # cloudflared 也從 loopback 進來但帶 CF-Connecting-IP → 視為遠端,無 token 401
        monkeypatch.setenv("LIMITUP_API_TOKEN", TOKEN)
        resp = loopback.post(POST_PATH, json={}, headers={"CF-Connecting-IP": "1.2.3.4"})
        assert resp.status_code == 401

    def test_loopback_with_cf_header_and_token_passes(self, loopback, monkeypatch):
        monkeypatch.setenv("LIMITUP_API_TOKEN", TOKEN)
        resp = loopback.post(POST_PATH, json={},
                             headers={"CF-Connecting-IP": "1.2.3.4", "X-Limitup-Token": TOKEN})
        assert resp.status_code == 200

    def test_non_loopback_private_ip_not_trusted(self, monkeypatch):
        # 突變必殺: 放行條件放寬成「私網 IP」
        monkeypatch.setenv("LIMITUP_API_TOKEN", TOKEN)
        c = TestClient(_with_client(server.app, "10.0.0.5"))
        assert c.post(POST_PATH, json={}).status_code == 401


def _mw_kwargs(m) -> dict:
    """starlette Middleware 物件: 0.27 是 .options,新版是 .kwargs。"""
    return getattr(m, "options", None) or getattr(m, "kwargs", {}) or {}


class TestMiddlewareOrder:
    def test_token_guard_inside_cors(self):
        # add_middleware 是 insert(0) → 後加的在外層;token guard 必須比 CORS 先註冊 (= 內層)
        from fastapi.middleware.cors import CORSMiddleware
        mws = server.app.user_middleware
        cors_idx = next(i for i, m in enumerate(mws) if m.cls is CORSMiddleware)
        guard_idx = next(i for i, m in enumerate(mws)
                         if _mw_kwargs(m).get("dispatch") is server.limitup_token_guard)
        assert cors_idx < guard_idx, "CORS 應在外層 (index 較小)"
