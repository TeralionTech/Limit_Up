"""trading_config.py — mode / 預算 / 張數 跨重啟保留；API 改完就寫檔；啟動套回永不 arm。"""
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import threading

import pytest

pytest.importorskip("fastapi")

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import api  # noqa: E402
import trading_config as TC  # noqa: E402
from runner import Runner  # noqa: E402
from trading_session import TradingSession  # noqa: E402


@pytest.fixture
def out(tmp_path, monkeypatch):
    """把落檔目錄導到 tmp (persist/restore 預設用 repo/output)。"""
    monkeypatch.setattr(TC, "config_path", lambda output_dir=None: tmp_path / TC.FILENAME)
    return tmp_path


def _fresh_session():
    return TradingSession()


class TestLoadRestore:
    def test_no_file_returns_none_and_leaves_defaults(self, out):
        s = _fresh_session()
        assert TC.load() is None
        assert TC.restore(s) is None
        assert s.mode == "sim" and s.total_budget == 0 and s.armed is False

    def test_roundtrip_persist_then_restore(self, out):
        s = _fresh_session()
        s.set_mode("real")
        s.set_params(total_budget=1_000_000, per_symbol_budget=300_000, sizing_mode="budget", fixed_lots=0)
        assert TC.persist(s) is True
        data = json.loads((out / TC.FILENAME).read_text(encoding="utf-8"))
        assert data == {"mode": "real", "sizing_mode": "budget", "fixed_lots": 0,
                        "total_budget": 1_000_000.0, "per_symbol_budget": 300_000.0,
                        "pull_exit": s.pull_exit.to_dict()}
        assert "armed" not in data

        s2 = _fresh_session()
        applied = TC.restore(s2)
        assert applied == {"mode": "real", "total_budget": 1_000_000.0, "per_symbol_budget": 300_000.0,
                           "sizing_mode": "budget", "fixed_lots": 0, "pull_exit": s.pull_exit.to_dict()}
        assert s2.mode == "real" and s2.total_budget == 1_000_000 and s2.per_symbol_budget == 300_000
        assert s2.armed is False                       # 突變必殺: 套回不可以 arm

    def test_restore_never_arms_even_if_file_says_so(self, out):
        (out / TC.FILENAME).write_text(json.dumps({"mode": "real", "armed": True, "total_budget": 5}), encoding="utf-8")
        s = _fresh_session()
        TC.restore(s)
        assert s.armed is False and s.mode == "real" and s.total_budget == 5

    def test_corrupt_file_ignored(self, out):
        (out / TC.FILENAME).write_text("{not json", encoding="utf-8")
        s = _fresh_session()
        assert TC.load() is None
        assert TC.restore(s) is None
        assert s.mode == "sim"

    @pytest.mark.parametrize("bad", [
        {"mode": "observe"},                     # 不合法 mode
        {"sizing_mode": "lots"},                 # 不合法 sizing_mode
        {"total_budget": -1},                    # 負數
        {"fixed_lots": True},                    # bool 不算數字
        {"total_budget": "100"},                 # 字串
        [],                                      # 不是 dict
    ])
    def test_invalid_fields_dropped(self, out, bad):
        (out / TC.FILENAME).write_text(json.dumps(bad), encoding="utf-8")
        assert TC.load() is None
        s = _fresh_session()
        assert TC.restore(s) is None
        assert s.mode == "sim" and s.total_budget == 0 and s.fixed_lots == 0

    def test_partial_file_applies_only_valid_fields(self, out):
        (out / TC.FILENAME).write_text(json.dumps({"mode": "sim", "fixed_lots": 3, "sizing_mode": "fixed_lots",
                                                   "total_budget": "oops"}), encoding="utf-8")
        s = _fresh_session()
        applied = TC.restore(s)
        assert applied == {"fixed_lots": 3, "sizing_mode": "fixed_lots"}       # mode 已是 sim → 不算變更
        assert s.sizing_mode == "fixed_lots" and s.fixed_lots == 3 and s.total_budget == 0

    def test_persist_failure_returns_false(self, out, monkeypatch):
        monkeypatch.setattr(TC, "config_path", lambda output_dir=None: Path(out / "no_such_dir" / "x" / "f.json"))
        monkeypatch.setattr(Path, "mkdir", lambda *a, **k: (_ for _ in ()).throw(OSError("ro")))
        assert TC.persist(_fresh_session()) is False


class TestApiPersists:
    @pytest.fixture
    def client(self, monkeypatch):
        runner = Runner()
        monkeypatch.setattr(api.Runner, "get", staticmethod(lambda: runner))
        app = FastAPI()
        app.include_router(api.router)
        return TestClient(app), runner

    def test_mode_endpoint_writes_file(self, out, client):
        c, runner = client
        assert c.post("/api/trading/mode", json={"mode": "real"}).status_code == 200
        assert json.loads((out / TC.FILENAME).read_text(encoding="utf-8"))["mode"] == "real"

    def test_params_endpoint_writes_file(self, out, client):
        c, runner = client
        r = c.post("/api/trading/params", json={"total_budget": 500000, "per_symbol_budget": 100000})
        assert r.status_code == 200
        data = json.loads((out / TC.FILENAME).read_text(encoding="utf-8"))
        assert data["total_budget"] == 500000 and data["per_symbol_budget"] == 100000
        assert data["mode"] == runner.session.mode

    def test_invalid_mode_does_not_write(self, out, client):
        c, _ = client
        assert c.post("/api/trading/mode", json={"mode": "observe"}).status_code == 400
        assert not (out / TC.FILENAME).exists()

    def test_invalid_params_do_not_write(self, out, client):
        c, _ = client
        assert c.post("/api/trading/params", json={"total_budget": -5}).status_code == 400
        assert not (out / TC.FILENAME).exists()

    @pytest.mark.parametrize("first_endpoint", ["pull_exit", "mode", "params"])
    @pytest.mark.parametrize("second_endpoint", ["pull_exit", "mode", "params"])
    def test_concurrent_writers_serialize_and_restart_matches_runtime(
            self, out, client, monkeypatch, first_endpoint, second_endpoint):
        from test_pull_signal import make_trader
        c, runner = client
        runner.trader, _, _, _ = make_trader("off", session=runner.session)
        first_written, release_write, second_attempt = (threading.Event() for _ in range(3))
        writes, acquired_by_second = [], []
        writer_ident = []
        original_lock = TC.CONFIG_LOCK
        original_write = Path.write_text

        class ObservedLock:
            def __enter__(self):
                if writer_ident and threading.get_ident() != writer_ident[0]:
                    acquired = original_lock.acquire(blocking=False)
                    acquired_by_second.append(acquired)
                    second_attempt.set()
                    if not acquired:
                        original_lock.acquire()
                else:
                    original_lock.acquire()
                return self

            def __exit__(self, *args):
                original_lock.release()

        def write(path, data, *args, **kwargs):
            if path == out / "trading_config.json.tmp":
                writes.append(json.loads(data))
                if len(writes) == 1:
                    writer_ident.append(threading.get_ident())
                    first_written.set()
                    assert release_write.wait(5)
            return original_write(path, data, *args, **kwargs)

        monkeypatch.setattr(TC, "CONFIG_LOCK", ObservedLock())
        monkeypatch.setattr(Path, "write_text", write)
        first_body = {"pull_exit": {"mode": "on"}, "mode": {"mode": "real"},
                      "params": {"total_budget": 500000}}[first_endpoint]
        second_body = {"pull_exit": {"mode": "off"}, "mode": {"mode": "sim"},
                       "params": {"total_budget": 100000}}[second_endpoint]
        expected = TC.snapshot(runner.session)
        for endpoint, body in ((first_endpoint, first_body), (second_endpoint, second_body)):
            if endpoint == "pull_exit":
                expected["pull_exit"].update(body)
            else:
                expected.update(body)

        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(c.post, f"/api/trading/{first_endpoint}", json=first_body)
            try:
                assert first_written.wait(3)
                # While file I/O is blocked, both market/session locks stay free.
                for lock in (runner.trader._pull_lock, runner.session._lock):
                    assert lock.acquire(blocking=False)
                    lock.release()
                second = pool.submit(c.post, f"/api/trading/{second_endpoint}", json=second_body)
                assert second_attempt.wait(3)
                assert acquired_by_second == [False]
                assert TC.snapshot(runner.session) == writes[0]
            finally:
                release_write.set()
            for response in (first.result(timeout=3), second.result(timeout=3)):
                assert response.status_code == 200
                assert response.json().get("persisted", True)

        persisted = json.loads((out / TC.FILENAME).read_text(encoding="utf-8"))
        fresh = TradingSession(auto_cancel_worker=False)
        TC.restore(fresh)
        assert len(writes) == 2
        assert persisted == TC.snapshot(runner.session) == TC.snapshot(fresh) == expected
        assert not fresh.armed
