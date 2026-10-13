"""A2 POST /api/trading/auto_connect — 讀機上 .env 憑證自己連線 (central 08:30 用,零機密)。
只掛 api.router 到乾淨 FastAPI app (不含 server.py 的 token 中介層;那個另測)。"""
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("fastapi")

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import api  # noqa: E402
from config import ConfigError  # noqa: E402
from runner import Runner  # noqa: E402

FAKE_CFG = SimpleNamespace(account_id="F123456789", password="pw-secret",
                           pfx_path="/opt/hit_limit_up/certs/F123456789.pfx",
                           pfx_password="pfxpw")


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(api.router)
    return TestClient(app)


@pytest.fixture
def session(monkeypatch):
    """singleton session,每測試確保 connecting=False 起手。"""
    s = Runner.get().session
    monkeypatch.setattr(s, "connecting", False)
    return s


@pytest.fixture
def capture(monkeypatch, session):
    """攔 connect_async 參數 (不真的連線)。"""
    calls = []
    monkeypatch.setattr(session, "connect_async",
                        lambda *a, **kw: calls.append((a, kw)))
    monkeypatch.setattr(api, "load_config", lambda: FAKE_CFG)
    return calls


class TestHappyPath:
    def test_uses_env_credentials_and_returns_masked(self, client, capture):
        resp = client.post("/api/trading/auto_connect", json={"is_test": False})
        assert resp.status_code == 200
        assert resp.json() == {"status": "connecting", "account_masked": "F12***"}
        assert len(capture) == 1
        args, kw = capture[0]
        assert kw == {}
        account_id, password, pfx_path, pfx_password, is_test, output_dir = args
        # 突變必殺: 傳錯欄位 / 順序 (connect_async 簽名 account, pwd, pfx_path, pfx_pwd, is_test, out)
        assert account_id == "F123456789"
        assert password == "pw-secret"
        assert pfx_path == "/opt/hit_limit_up/certs/F123456789.pfx"
        assert pfx_password == "pfxpw"
        assert is_test is False
        assert isinstance(output_dir, Path) and output_dir.name == "output"

    def test_is_test_true_forwarded(self, client, capture):
        resp = client.post("/api/trading/auto_connect", json={"is_test": True})
        assert resp.status_code == 200
        assert capture[0][0][4] is True

    def test_body_optional_defaults_is_test_false(self, client, capture):
        # central 可能不帶 body → is_test 預設 False (正式環境)
        resp = client.post("/api/trading/auto_connect")
        assert resp.status_code == 200
        assert capture[0][0][4] is False

    def test_response_never_leaks_secrets(self, client, capture):
        body = client.post("/api/trading/auto_connect", json={}).text
        assert "pw-secret" not in body and "pfxpw" not in body
        assert "F123456789" not in body      # 只給 masked


class TestRejections:
    def test_connecting_in_progress_400(self, client, monkeypatch, session):
        calls = []
        monkeypatch.setattr(session, "connect_async", lambda *a, **kw: calls.append(a))
        monkeypatch.setattr(api, "load_config", lambda: FAKE_CFG)
        monkeypatch.setattr(session, "connecting", True)
        resp = client.post("/api/trading/auto_connect", json={"is_test": False})
        assert resp.status_code == 400
        assert resp.json()["detail"] == "連線進行中"
        assert calls == []                   # 沒有二次連線

    def test_config_error_400(self, client, monkeypatch, session):
        calls = []
        monkeypatch.setattr(session, "connect_async", lambda *a, **kw: calls.append(a))

        def _boom():
            raise ConfigError("[config] .env 缺少必填欄位: FUBON_ACCOUNT_ID")
        monkeypatch.setattr(api, "load_config", _boom)
        resp = client.post("/api/trading/auto_connect", json={"is_test": False})
        assert resp.status_code == 400
        assert "FUBON_ACCOUNT_ID" in resp.json()["detail"]
        assert calls == []

    def test_session_race_runtime_error_400(self, client, monkeypatch, session):
        # 兩個 auto_connect 幾乎同時: 第一個把 connecting 翻 True 後第二個才進 connect_async
        # → session 自己 raise RuntimeError("連線進行中") 也要收成 400,不 500
        def _race(*a, **kw):
            raise RuntimeError("連線進行中")
        monkeypatch.setattr(session, "connect_async", _race)
        monkeypatch.setattr(api, "load_config", lambda: FAKE_CFG)
        resp = client.post("/api/trading/auto_connect", json={})
        assert resp.status_code == 400
        assert resp.json()["detail"] == "連線進行中"

    def test_invalid_body_422(self, client, capture):
        resp = client.post("/api/trading/auto_connect", json={"is_test": "not-a-bool"})
        assert resp.status_code == 422
        assert capture == []


class TestIdempotentAndGuards:
    """2026-10-13 審查補強：已連線不重連、自動重連中回 409、hub 不給 auto_connect。"""

    def _broker(self, connected=True, healthy=True, relogin=False):
        import threading
        lock = threading.Lock()
        if relogin:
            lock.acquire()
        return SimpleNamespace(connected=connected, healthy=healthy, account_no="F126450042", _relogin_lock=lock)

    def test_already_connected_returns_200_without_new_client(self, client, capture, session, monkeypatch):
        monkeypatch.setattr(session, "broker", self._broker())
        resp = client.post("/api/trading/auto_connect", json={})
        assert resp.status_code == 200
        assert resp.json() == {"status": "already_connected", "account_masked": "F12***"}
        assert capture == []

    def test_relogin_in_progress_409(self, client, capture, session, monkeypatch):
        monkeypatch.setattr(session, "broker", self._broker(connected=True, healthy=False, relogin=True))
        resp = client.post("/api/trading/auto_connect", json={})
        assert resp.status_code == 409
        assert "重連" in resp.json()["detail"]
        assert capture == []

    def test_unhealthy_but_not_reconnecting_proceeds(self, client, capture, session, monkeypatch):
        # broker 物件在但已放棄重連 (手動 disconnect 後殘留 / 從未健康) → 照常重新連線
        monkeypatch.setattr(session, "broker", self._broker(connected=False, healthy=False))
        resp = client.post("/api/trading/auto_connect", json={})
        assert resp.status_code == 200
        assert len(capture) == 1

    def test_hub_role_400(self, client, capture, session, monkeypatch):
        monkeypatch.setenv("ROLE", "hub   # 行尾註解")
        resp = client.post("/api/trading/auto_connect", json={})
        assert resp.status_code == 400
        assert "hub" in resp.json()["detail"]
        assert capture == []
