"""本節點帳號顯示 (2026-09-15;延伸 09-14 node3/node4 互填帳號事故的防呆)。

需求: 前端交易連線表單旁顯示「本節點帳號 Z90…999」,已連線列也附上,讓操作員連線前/後都能比對,
不要拿別台節點的帳號連線 (後端 de53cb3 起已會拒絕不符的登入)。

契約:
  1. TradingSession.status() 一律帶 node_login_masked (未連線 / 已連線 / FUBON_ACCOUNT_ID 未設 都有此 key)。
  2. 值 = .env FUBON_ACCOUNT_ID 每次呼叫現讀 os.environ → 去空白、轉大寫 (同 _norm_login_id / _mask_login_id)
     → 前 3 碼 + … + 後 3 碼;長度 < 7 只給前 3 碼 + …;未設/空白 → ""。
  3. 完整 ID 絕不出現在 status() (含 /api/trading/status endpoint 的回傳)。
  4. status() 帶 node_role (= env ROLE,strip+lower,預設 standalone);ROLE=hub 不交易 → node_login_masked 恆為 ""
     (hub 的 FUBON_ACCOUNT_ID 是行情帳號,不公開 ID 片段;前端依 node_role 顯示「hub 不交易」)。

測試用 ID 全是假字串 (非真實身分證字號/帳號);不連任何伺服器。
"""
import json
from types import SimpleNamespace

import pytest

import trading_session as ts_mod
from fakes_cancel import FakeSnapshotBroker, make_session

NODE_ID = "Z90TEST999"        # 本節點 .env 的登入 ID (假,10 碼同身分證長度)
NODE_DISPLAY = "Z90…999"


@pytest.fixture(autouse=True)
def _no_role_env(monkeypatch):
    """預設 ROLE 未設 (= standalone),避免跑測試的環境變數影響結果;hub/node 測試自行 setenv。"""
    monkeypatch.delenv("ROLE", raising=False)


def _dump(st: dict) -> str:
    return json.dumps(st, ensure_ascii=False, default=str)


def _assert_no_full_id(st: dict, full_id: str = NODE_ID):
    text = _dump(st)
    assert full_id not in text
    assert full_id.lower() not in text


# ═══ 1. 遮罩格式 ═══

class TestMasking:
    @pytest.mark.parametrize("env_value,expected", [
        (NODE_ID, NODE_DISPLAY),                        # 一般 10 碼
        ("  " + NODE_ID + "\t\n", NODE_DISPLAY),        # 前後空白 (.env 手滑)
        (NODE_ID.lower(), NODE_DISPLAY),                # 小寫 → 轉大寫 (同 _mask_login_id / 連線防呆比對)
        (" z90Test042 ", NODE_DISPLAY),                 # 大小寫混雜 + 空白
        ("ABCDEFG", "ABC…EFG"),                         # 剛好 7 碼 → 前 3 + 後 3
        ("ABCDEF", "ABC…"),                             # 6 碼 → 只給前 3 碼
        ("AB", "AB…"),                                  # 極短
        ("", ""),                                       # 空字串 = 未設
        ("   ", ""),                                    # 全空白 = 未設
    ])
    def test_mask_format(self, monkeypatch, env_value, expected):
        monkeypatch.setenv("FUBON_ACCOUNT_ID", env_value)
        assert ts_mod._node_login_masked() == expected

    def test_unset_env_returns_empty(self, monkeypatch):
        monkeypatch.delenv("FUBON_ACCOUNT_ID", raising=False)
        assert ts_mod._node_login_masked() == ""

    def test_reads_env_at_call_time(self, monkeypatch):
        monkeypatch.setenv("FUBON_ACCOUNT_ID", NODE_ID)
        assert ts_mod._node_login_masked() == NODE_DISPLAY
        monkeypatch.setenv("FUBON_ACCOUNT_ID", "Y80TEST777")     # os.environ 變動即時反映
        assert ts_mod._node_login_masked() == "Y80…777"
        monkeypatch.delenv("FUBON_ACCOUNT_ID")
        assert ts_mod._node_login_masked() == ""

    def test_prefix_consistent_with_log_mask(self, monkeypatch):
        """UI 顯示與 log / connect_error 用的 _mask_login_id (前 3 碼 + ***) 同一套正規化。"""
        raw = " " + NODE_ID.lower() + " "
        monkeypatch.setenv("FUBON_ACCOUNT_ID", raw)
        assert ts_mod._node_login_masked()[:3] == ts_mod._mask_login_id(raw)[:3] == "Z90"

    @pytest.mark.parametrize("value", [NODE_ID, "ABCDEFG", "ABCDEFGHIJKLMN"])
    def test_full_id_never_returned(self, monkeypatch, value):
        monkeypatch.setenv("FUBON_ACCOUNT_ID", value)
        out = ts_mod._node_login_masked()
        assert value not in out and "…" in out


# ═══ 2. status() 一律帶欄位 ═══

class TestStatusField:
    def test_disconnected_status_has_field(self, monkeypatch):
        monkeypatch.setenv("FUBON_ACCOUNT_ID", "  " + NODE_ID.lower())
        s = ts_mod.TradingSession(auto_cancel_worker=False)
        assert s.broker is None
        st = s.status()
        assert st["connected"] is False
        assert st["node_login_masked"] == NODE_DISPLAY
        _assert_no_full_id(st)

    def test_disconnected_real_mode_status_has_field(self, monkeypatch):
        """真實模式、尚未連線 = 前端顯示連線表單的狀態。"""
        monkeypatch.setenv("FUBON_ACCOUNT_ID", NODE_ID)
        s = ts_mod.TradingSession(auto_cancel_worker=False)
        s.set_mode("real")
        st = s.status()
        assert st["mode"] == "real" and st["connected"] is False and st["connecting"] is False
        assert st["node_login_masked"] == NODE_DISPLAY
        _assert_no_full_id(st)

    def test_disconnected_status_env_unset_has_empty_field(self, monkeypatch):
        monkeypatch.delenv("FUBON_ACCOUNT_ID", raising=False)
        s = ts_mod.TradingSession(auto_cancel_worker=False)
        st = s.status()
        assert "node_login_masked" in st and st["node_login_masked"] == ""

    def test_connected_fake_broker_status_has_field(self, monkeypatch):
        monkeypatch.setenv("FUBON_ACCOUNT_ID", NODE_ID)
        b = FakeSnapshotBroker()
        b._account_id = NODE_ID                      # 本節點帳號已連線 (防呆通過)
        s = make_session(broker=b)
        st = s.status()
        assert st["connected"] is True and st["healthy"] is True and st["armed"] is True
        assert st["account_masked"] == "****"         # broker.status() 欄位照舊
        assert st["node_login_masked"] == NODE_DISPLAY
        _assert_no_full_id(st)

    def test_connected_fake_broker_env_unset_has_empty_field(self, monkeypatch):
        monkeypatch.delenv("FUBON_ACCOUNT_ID", raising=False)
        s = make_session(broker=FakeSnapshotBroker())
        st = s.status()
        assert st["connected"] is True
        assert "node_login_masked" in st and st["node_login_masked"] == ""

    def test_broker_status_cannot_override_field(self, monkeypatch):
        """broker.status() 若帶同名 key (例如帶完整 ID 的錯誤實作) 也不得蓋掉 session 的遮罩值。"""
        monkeypatch.setenv("FUBON_ACCOUNT_ID", NODE_ID)
        b = FakeSnapshotBroker()
        b.status = lambda: {"connected": True, "healthy": True, "account_masked": "****",
                            "is_test": True, "error": "", "node_login_masked": NODE_ID}
        s = make_session(broker=b)
        st = s.status()
        assert st["node_login_masked"] == NODE_DISPLAY
        _assert_no_full_id(st)

    def test_status_reflects_environ_change(self, monkeypatch):
        """os.environ 變動即時反映 (實機 .env 只在服務啟動時載入,改 .env 仍需 systemctl restart)。"""
        monkeypatch.setenv("FUBON_ACCOUNT_ID", NODE_ID)
        s = ts_mod.TradingSession(auto_cancel_worker=False)
        assert s.status()["node_login_masked"] == NODE_DISPLAY
        monkeypatch.setenv("FUBON_ACCOUNT_ID", "")
        assert s.status()["node_login_masked"] == ""


# ═══ 3. /api/trading/status endpoint 原樣回傳 (api.py 不動) ═══

class TestEndpoint:
    def test_trading_status_endpoint_carries_masked_field(self, monkeypatch):
        import api
        import runner as runner_mod
        monkeypatch.setenv("FUBON_ACCOUNT_ID", NODE_ID)
        s = ts_mod.TradingSession(auto_cancel_worker=False)
        s.set_mode("real")
        monkeypatch.setattr(runner_mod.Runner, "_instance", SimpleNamespace(session=s))
        st = api.trading_status()
        assert st["node_login_masked"] == NODE_DISPLAY
        _assert_no_full_id(st)


# ═══ 4. node_role / hub 不顯示 ID 片段 (review R6) ═══

class TestNodeRole:
    def test_role_default_standalone(self, monkeypatch):
        monkeypatch.setenv("FUBON_ACCOUNT_ID", NODE_ID)
        st = ts_mod.TradingSession(auto_cancel_worker=False).status()
        assert st["node_role"] == "standalone"
        assert st["node_login_masked"] == NODE_DISPLAY

    def test_role_node_normalized(self, monkeypatch):
        monkeypatch.setenv("FUBON_ACCOUNT_ID", NODE_ID)
        monkeypatch.setenv("ROLE", "  Node ")
        st = make_session(broker=FakeSnapshotBroker()).status()
        assert st["node_role"] == "node"
        assert st["node_login_masked"] == NODE_DISPLAY

    @pytest.mark.parametrize("connected", [False, True])
    def test_hub_hides_login_fragment(self, monkeypatch, connected):
        monkeypatch.setenv("FUBON_ACCOUNT_ID", NODE_ID)
        monkeypatch.setenv("ROLE", "HUB")
        if connected:
            s = make_session(broker=FakeSnapshotBroker())
        else:
            s = ts_mod.TradingSession(auto_cancel_worker=False)
            s.set_mode("real")
        st = s.status()
        assert st["connected"] is connected
        assert st["node_role"] == "hub"
        assert st["node_login_masked"] == ""
        text = _dump(st)
        assert "Z90" not in text and "…" not in text          # 前 3 碼 / 遮罩片段都不回
        _assert_no_full_id(st)
