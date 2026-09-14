"""帳號防呆 (2026-09-14 node3/node4 事故 ITEM A)。

事故: node3 的 .env FUBON_ACCOUNT_ID 是 D12…,操作員卻在 UI 以 B22… 連線;node4 反之 →
兩台都在「別台節點的帳號」上交易 (出場賣到非策略部位、隔日賣清單記錯張數)。

契約:
  1. connect_async: .env FUBON_ACCOUNT_ID 有設,且送來的登入 ID 不同 (去空白、不分大小寫) →
     不連線、不建 RealOrderClient、不呼叫 SDK login、不 raise;connect_error 寫中文原因
     (前端每 2 秒輪詢 /api/trading/status,紅框顯示 connect_error);WARNING log 只含遮罩 ID (前 3 碼 + ***)。
  2. set_armed(True): 已連線 broker 的登入 ID ≠ FUBON_ACCOUNT_ID → RuntimeError (部署版 api.py 轉 HTTP 400)。
     set_armed(False) 永遠可以。
  3. FUBON_ACCOUNT_ID 未設 (或空白)、或 broker 沒有可比對的登入 ID → 行為與舊版完全相同。

測試用 ID 全是假字串 (非真實身分證字號/帳號)。富邦 SDK 以 sys.modules 假模組替身,不連任何伺服器。
"""
import base64
import logging
import sys
import types
from types import SimpleNamespace

import pytest

import trading_session as ts_mod
from fakes_cancel import FakeSnapshotBroker, make_session, wait_until

NODE_ID = "Z90TEST0001"       # 本節點 .env 的登入 ID (假)
OTHER_ID = "Y80TEST0002"      # 別台節點的登入 ID (假)
NODE_MASK = "Z90***"
OTHER_MASK = "Y80***"


# ─── fixtures ────────────────────────────────────────────────

@pytest.fixture
def fake_sdk(monkeypatch):
    """假 fubon_neo.sdk.FubonSDK — 記錄建立次數與 login 參數;login 一律成功。"""
    rec = SimpleNamespace(instances=[])

    class FakeSDK:
        def __init__(self, *a, **kw):
            self.logins = []
            self.accounting = SimpleNamespace(
                inventories=lambda account: SimpleNamespace(is_success=True, message="", data=[]))
            rec.instances.append(self)

        def login(self, *args):
            self.logins.append(args)
            return SimpleNamespace(is_success=True, message="",
                                   data=[SimpleNamespace(account="0000000", branch_no="0000")])

        def set_on_filled(self, h):
            return None

        set_on_order = set_on_order_changed = set_on_filled

        def set_on_event(self, h):
            return None

        def logout(self):
            return None

    pkg = types.ModuleType("fubon_neo")
    pkg.__path__ = []
    sdk_mod = types.ModuleType("fubon_neo.sdk")
    sdk_mod.FubonSDK = FakeSDK
    pkg.sdk = sdk_mod
    monkeypatch.setitem(sys.modules, "fubon_neo", pkg)
    monkeypatch.setitem(sys.modules, "fubon_neo.sdk", sdk_mod)
    return rec


@pytest.fixture
def client_spy(monkeypatch):
    """包住真 RealOrderClient — 記錄是否被建立 (session._do 的第一步)。"""
    import broker as broker_mod
    made = []
    real_cls = broker_mod.RealOrderClient

    class SpyClient(real_cls):
        def __init__(self, log_path):
            made.append(log_path)
            super().__init__(log_path)

    monkeypatch.setattr(broker_mod, "RealOrderClient", SpyClient)
    return made


@pytest.fixture
def sessions():
    """收集測試建的 session;結束時斷線 + 關 orders.csv 檔 (Windows tmp 清理)。"""
    made = []
    yield made
    for s in made:
        b = s.broker
        if b is None:
            continue
        for fn in ("disconnect", "close"):
            try:
                getattr(b, fn)()
            except Exception:
                pass


def _new_session(sessions, mode="real"):
    s = ts_mod.TradingSession(auto_cancel_worker=False)
    s.set_mode(mode)
    sessions.append(s)
    return s


def _connect_and_wait(s, login_id, out_dir):
    s.connect_async(login_id, "pw", "x.pfx", "", False, out_dir)
    assert wait_until(lambda: not s.connecting, timeout=5.0)


def _set_budget(s):
    s.set_params(total_budget=1_000_000, per_symbol_budget=200_000)


def _no_full_ids_logged(caplog):
    return all(NODE_ID not in r.getMessage() and OTHER_ID not in r.getMessage()
               for r in caplog.records)


# ═══ 1. connect_async 防呆 ═══

class TestConnectGuard:
    def test_other_nodes_login_rejected_before_any_broker_or_sdk_login(
            self, monkeypatch, tmp_path, caplog, fake_sdk, client_spy, sessions):
        monkeypatch.setenv("FUBON_ACCOUNT_ID", NODE_ID)
        s = _new_session(sessions)
        out_dir = tmp_path / "out"
        caplog.set_level(logging.DEBUG)

        ret = s.connect_async(OTHER_ID, "pw", "x.pfx", "", False, out_dir)

        assert ret is None                                  # 不 raise
        assert s.connecting is False                        # 同步結束,沒有背景連線在跑
        assert s.broker is None
        err = s.connect_error
        assert "不屬於本節點" in err and "已拒絕連線" in err
        assert OTHER_MASK in err and NODE_MASK in err
        assert OTHER_ID not in err and NODE_ID not in err  # UI 也只顯示遮罩
        # 背景連線 thread 沒起: 沒建 client、沒建 SDK、沒 login、output_dir 沒被 mkdir
        assert not wait_until(lambda: client_spy or fake_sdk.instances, timeout=0.3)
        assert not out_dir.exists()
        # 前端輪詢的 status 看得到原因
        st = s.status()
        assert st["connect_error"] == err
        assert st["connecting"] is False and st["connected"] is False
        # WARNING 帶遮罩 ID;任何 log 都不含完整 ID
        warns = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert any(OTHER_MASK in m and NODE_MASK in m for m in warns)
        assert _no_full_ids_logged(caplog)

    @pytest.mark.parametrize("env_value,submitted", [
        (NODE_ID, NODE_ID),
        ("  " + NODE_ID.lower() + " ", NODE_ID),            # .env 小寫 + 空白
        (NODE_ID, " " + NODE_ID.lower() + "  "),            # 送來的小寫 + 空白
    ])
    def test_matching_login_connects_case_and_whitespace_insensitive(
            self, monkeypatch, tmp_path, fake_sdk, client_spy, sessions, env_value, submitted):
        monkeypatch.setenv("FUBON_ACCOUNT_ID", env_value)
        s = _new_session(sessions)
        _connect_and_wait(s, submitted, tmp_path / "out")
        assert s.connect_error == ""
        assert len(client_spy) == 1
        assert s.broker is not None and s.broker.connected is True
        assert len(fake_sdk.instances) == 1
        assert fake_sdk.instances[0].logins[0][0] == submitted   # 原樣交給 SDK (不改寫 ID)

    @pytest.mark.parametrize("env_value", [None, "", "   "])
    def test_unset_or_blank_env_connects_any_login_as_before(
            self, monkeypatch, tmp_path, fake_sdk, client_spy, sessions, env_value):
        if env_value is None:
            monkeypatch.delenv("FUBON_ACCOUNT_ID", raising=False)
        else:
            monkeypatch.setenv("FUBON_ACCOUNT_ID", env_value)
        s = _new_session(sessions)
        _connect_and_wait(s, OTHER_ID, tmp_path / "out")
        assert s.connect_error == ""
        assert s.broker is not None and s.broker.connected is True
        assert fake_sdk.instances[0].logins[0][0] == OTHER_ID

    def test_env_read_at_call_time(self, monkeypatch, tmp_path, fake_sdk, client_spy, sessions):
        # session 建立時未設,連線當下才設 → 仍要擋 (不可在 __init__ 快取)
        monkeypatch.delenv("FUBON_ACCOUNT_ID", raising=False)
        s = _new_session(sessions)
        monkeypatch.setenv("FUBON_ACCOUNT_ID", NODE_ID)
        s.connect_async(OTHER_ID, "pw", "x.pfx", "", False, tmp_path / "out")
        assert "不屬於本節點" in s.connect_error
        assert not wait_until(lambda: client_spy or fake_sdk.instances, timeout=0.3)

    def test_connecting_in_progress_still_raises_and_keeps_state(self, monkeypatch, tmp_path, sessions):
        monkeypatch.setenv("FUBON_ACCOUNT_ID", NODE_ID)
        s = _new_session(sessions)
        s.connecting = True
        s.connect_error = "(前一次連線的狀態)"
        with pytest.raises(RuntimeError, match="連線進行中"):
            s.connect_async(OTHER_ID, "pw", "x.pfx", "", False, tmp_path / "out")
        assert s.connecting is True                         # 不動進行中那次的狀態
        assert s.connect_error == "(前一次連線的狀態)"

    def test_rejected_connect_leaves_existing_broker_and_armed_untouched(self, monkeypatch, tmp_path):
        monkeypatch.setenv("FUBON_ACCOUNT_ID", NODE_ID)
        b = FakeSnapshotBroker()
        b._account_id = NODE_ID
        calls = []
        b.disconnect = lambda: calls.append("disconnect")
        s = make_session(broker=b)                          # 本節點帳號已連線 + armed
        assert s.armed is True
        s.connect_async(OTHER_ID, "pw", "x.pfx", "", False, tmp_path / "out")
        assert s.broker is b and calls == []                # 不換掉、不斷線正確的連線
        assert s.armed is True
        assert "不屬於本節點" in s.connect_error

    def test_error_cleared_by_next_correct_connect(
            self, monkeypatch, tmp_path, fake_sdk, client_spy, sessions):
        monkeypatch.setenv("FUBON_ACCOUNT_ID", NODE_ID)
        s = _new_session(sessions)
        s.connect_async(OTHER_ID, "pw", "x.pfx", "", False, tmp_path / "out")
        assert "不屬於本節點" in s.connect_error
        _connect_and_wait(s, NODE_ID, tmp_path / "out")
        assert s.connect_error == ""
        assert s.broker is not None and s.broker.connected is True
        assert [sdk.logins[0][0] for sdk in fake_sdk.instances] == [NODE_ID]


# ═══ 2. set_armed 防呆 ═══

class TestArmGuard:
    def test_arm_refused_when_real_client_logged_in_with_other_nodes_id(
            self, monkeypatch, tmp_path, caplog, fake_sdk, client_spy, sessions):
        # 連線當下 .env 還沒載入 (未設) → 用別台 ID 連上;之後 FUBON_ACCOUNT_ID 生效 → arm 必須被擋
        monkeypatch.delenv("FUBON_ACCOUNT_ID", raising=False)
        s = _new_session(sessions)
        _connect_and_wait(s, OTHER_ID, tmp_path / "out")
        assert s.broker is not None and s.connect_error == ""
        _set_budget(s)
        monkeypatch.setenv("FUBON_ACCOUNT_ID", NODE_ID)
        caplog.set_level(logging.DEBUG)

        with pytest.raises(RuntimeError) as ei:
            s.set_armed(True)

        msg = str(ei.value)
        assert "不屬於本節點" in msg and OTHER_MASK in msg and NODE_MASK in msg
        assert OTHER_ID not in msg and NODE_ID not in msg
        assert s.armed is False and s.is_live() is False
        assert any(r.levelno == logging.WARNING and OTHER_MASK in r.getMessage() for r in caplog.records)
        assert _no_full_ids_logged(caplog)

    def test_arm_allowed_when_real_client_login_matches(
            self, monkeypatch, tmp_path, fake_sdk, client_spy, sessions):
        monkeypatch.setenv("FUBON_ACCOUNT_ID", " " + NODE_ID.lower())
        s = _new_session(sessions)
        _connect_and_wait(s, NODE_ID, tmp_path / "out")
        _set_budget(s)
        s.set_armed(True)
        assert s.armed is True and s.is_live() is True

    def test_disarm_always_works_even_when_login_mismatches(self, monkeypatch):
        monkeypatch.setenv("FUBON_ACCOUNT_ID", NODE_ID)
        b = FakeSnapshotBroker()
        b._account_id = OTHER_ID
        s = ts_mod.TradingSession(auto_cancel_worker=False)
        s.set_mode("real")
        s.broker = b
        s.armed = True                                      # 例: 防呆上線前就已 arm / .env 事後才改
        s.set_armed(False)
        assert s.armed is False
        # broker 斷線/不健康時照樣能解除
        s.armed = True
        b.connected = False
        s.set_armed(False)
        assert s.armed is False

    def test_arm_unchanged_when_env_unset(self, monkeypatch):
        monkeypatch.delenv("FUBON_ACCOUNT_ID", raising=False)
        b = FakeSnapshotBroker()
        b._account_id = OTHER_ID
        s = make_session(broker=b)                          # make_session 內部 set_armed(True)
        assert s.armed is True and s.is_live() is True

    @pytest.mark.parametrize("login_id", ["<missing>", "", "   ", None, 12345])
    def test_arm_unchanged_when_broker_has_no_comparable_login_id(self, monkeypatch, login_id):
        # 測試替身 / 無登入 ID 可比 → 維持原行為 (broker 資料不可得時不改變行為)
        monkeypatch.setenv("FUBON_ACCOUNT_ID", NODE_ID)
        b = FakeSnapshotBroker()
        if login_id != "<missing>":
            b._account_id = login_id
        s = make_session(broker=b)
        assert s.armed is True

    def test_existing_preflight_order_kept(self, monkeypatch):
        # 未連線 + 帳號不符 → 仍先回舊的「券商未連線」訊息 (pre-flight 順序不變)
        monkeypatch.setenv("FUBON_ACCOUNT_ID", NODE_ID)
        b = FakeSnapshotBroker()
        b._account_id = OTHER_ID
        b.connected = False
        s = ts_mod.TradingSession(auto_cancel_worker=False)
        s.set_mode("real")
        s.broker = b
        _set_budget(s)
        with pytest.raises(RuntimeError, match="券商未連線"):
            s.set_armed(True)
        # 模擬模式照舊先擋
        s.set_mode("sim")
        with pytest.raises(RuntimeError, match="模擬模式"):
            s.set_armed(True)


# ═══ 3. 部署版 API 呈現 (api.py 未改動;直接呼叫 endpoint 函式,同 test_avg_volume 寫法) ═══

class TestApiSurface:
    @staticmethod
    def _stub_runner(monkeypatch, s):
        import runner as runner_mod
        monkeypatch.setattr(runner_mod.Runner, "_instance", SimpleNamespace(session=s))

    def test_connect_endpoint_returns_and_status_shows_reason(
            self, monkeypatch, tmp_path, fake_sdk, client_spy, sessions):
        import api
        monkeypatch.setenv("FUBON_ACCOUNT_ID", NODE_ID)
        # trading_connect 依 __file__ 決定 pfx 存放處 → 導到 tmp_path,不在 repo 目錄落檔
        fake_file = tmp_path / "app" / "repo" / "api.py"
        monkeypatch.setattr(api, "__file__", str(fake_file))
        s = _new_session(sessions)
        self._stub_runner(monkeypatch, s)

        req = api.TradingConnectReq(account_id=" " + OTHER_ID + " ", password="pw",
                                    pfx_b64=base64.b64encode(b"not-a-real-pfx").decode(),
                                    pfx_filename="t.pfx")
        assert api.trading_connect(req) == {"status": "connecting"}
        st = api.trading_status()                            # 前端 2 秒輪詢的 endpoint
        assert st["connecting"] is False and st["connected"] is False
        assert "不屬於本節點" in st["connect_error"]
        assert OTHER_MASK in st["connect_error"] and NODE_MASK in st["connect_error"]
        assert not wait_until(lambda: client_spy or fake_sdk.instances, timeout=0.3)
        assert not (fake_file.parent / "output").exists()

    def test_arm_endpoint_maps_mismatch_to_http_400_and_disarm_ok(self, monkeypatch):
        import api
        from fastapi import HTTPException
        monkeypatch.setenv("FUBON_ACCOUNT_ID", NODE_ID)
        b = FakeSnapshotBroker()
        b._account_id = OTHER_ID
        s = ts_mod.TradingSession(auto_cancel_worker=False)
        s.set_mode("real")
        s.broker = b
        _set_budget(s)
        self._stub_runner(monkeypatch, s)

        with pytest.raises(HTTPException) as ei:
            api.trading_arm(api.TradingArmReq(armed=True))
        assert ei.value.status_code == 400
        assert "不屬於本節點" in ei.value.detail
        assert s.armed is False
        assert api.trading_arm(api.TradingArmReq(armed=False)) == {"ok": True, "armed": False}
