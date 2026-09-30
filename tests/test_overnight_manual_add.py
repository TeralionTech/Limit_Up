"""2026-09-30 node1/node4 事故: 08:00 之後手動加入的隔日賣標的,09:00 沒有被賣出。

經過 (production journal):
  08:00  node1/node4 載入隔日賣清單 0 檔 — 2030 是同帳號的 IDC 系統前一天買的,不在我們的清單檔
  08:32  連線對帳 → CRITICAL「券商庫存有但隔日賣清單沒有: 2030 1 張 … 需要隔日賣請手動加入」
  08:33  使用者照提示手動加入 → 對帳 1 張 ✓ (node 已 armed)
  08:59:58 當日兩檔 marked (8227/2030) 都被量減半淘汰 → watchlist 空
  09:00  「轉場交易,watchlist 0 檔 + 隔日賣 **0 檔**」→「篩選結果空 + 無隔日賣 → 直接 finished」
         → subscriber.stop() → 沒有行情 → 賣出規則永遠不會被觸發
  同時間 node2/node3 (2030 是我們自己買的,08:00 就在清單裡) 正常建 trader、09:00:01 賣出。

根因: runner 在 08:00 把清單讀進區域變數 overnight_syms,之後訂閱 / 價格重查閘門 / 09:00 轉場
全都用那份快照,不問 session 的即時清單。
修法: 一律 _overnight_now();手動加入當下補查價格;主流程已停時加入要明講「今天不會自動賣」。
"""
import logging
import time
from datetime import datetime
from types import SimpleNamespace

import pytest

import runner as runner_mod
from runner import Phase, Runner
from test_exit_logic import _InvBroker, _sells, _wait
from test_session_money import make_session


# ─────────────────────────── 測試用假物件 ───────────────────────────
class _FakeSubscriber:
    def __init__(self, events=None, universe=None):
        self.events = events if events is not None else []
        self.universe = list(universe or [])
        self.added = []

    def set_handlers(self, **kw):
        self.events.append("set_handlers")

    def stop(self):
        self.events.append("subscriber.stop")

    def get_latest_snapshot(self, sym):
        return None

    def add_symbol(self, sym):
        self.added.append(sym)
        return True

    def request_unsubscribe(self, sym):
        self.events.append(("unsub", sym))


def _session_with_inventory(symbol="2030", lots=1):
    s = make_session()                                   # real + armed + 假 broker
    s.broker = _InvBroker([{"symbol": symbol, "lots": lots}] if lots else [])
    return s


def _trade_runner(tmp_path, session):
    """能跑 _trade_phase 的最小 Runner (行情/錄檔/檔案都換成假的,不碰 repo 的 output/)。"""
    r = Runner()
    r.session = session
    r.cfg = SimpleNamespace(bid_decline_sample_sec=60, bid_decline_minutes=5,
                            end_time="09:00:00", trading_end_time="13:24:00")
    events = []
    r.subscriber = _FakeSubscriber(events)
    r.recorder = SimpleNamespace(close=lambda: events.append("recorder.close"))
    r._overnight_file = lambda: tmp_path / "overnight_holdings.json"
    r._append_positions_history = lambda: None
    return r, events


class _FrozenDT(datetime):
    """把 runner 看到的「現在」固定住 (只影響 runner 模組內的 datetime.now())。"""
    _now = None

    @classmethod
    def now(cls, tz=None):
        return cls._now


def _freeze(monkeypatch, y, m, d, hh, mm):
    _FrozenDT._now = datetime(y, m, d, hh, mm, 0)
    monkeypatch.setattr(runner_mod, "datetime", _FrozenDT)


WED = (2026, 9, 30)      # 事故當天,週三
SAT = (2026, 10, 3)


# ─────────────────────────── 09:00 轉場 ───────────────────────────
class TestTradePhaseUsesLiveList:
    def test_incident_manual_add_after_0800_is_sold_at_open(self, monkeypatch, tmp_path):
        """事故重現: 08:00 快照是空的、08:33 手動加入、watchlist 空 → 必須建 trader 並賣出。"""
        s = _session_with_inventory("2030", 1)
        r, events = _trade_runner(tmp_path, s)

        stale_0800 = []                                   # 08:00 _prepare_overnight 回的空清單
        s.add_overnight("2030")                           # 08:33 手動加入 → 立即對帳庫存
        assert s.overnight["2030"]["lots"] == 1 and s.overnight["2030"]["reconciled"]
        s.set_overnight_limit_ups({"2030": 36.7})         # 今日漲停 / 跌停 (production 實值)
        s.set_limit_downs({"2030": 30.1})

        def _open_tick(end_time, trader, cfg):            # 取代「等到 13:24」: 開盤漲停打開的第一筆 book
            events.append("tick")
            r.trader.on_book("2030",
                             [{"price": 36.25, "size": 50}, {"price": 36.2, "size": 30}],
                             [{"price": 36.3, "size": 10}])
            assert _wait(lambda: _sells(s.broker)), "隔日賣 2 秒內沒下單"
        monkeypatch.setattr("filter._wait_until", _open_tick)

        r._trade_phase([], stale_0800)

        assert _sells(s.broker) == [("limit_sell", "2030", 30.1, 1)], "要用跌停價限價賣出 1 張"
        # trader 的 handler 要在 tick 之前掛上,而且 subscriber 不能在那之前被停掉
        assert events.index("set_handlers") < events.index("tick") < events.index("subscriber.stop")

    def test_nothing_to_do_still_finishes_early(self, tmp_path, caplog):
        """沒有 watchlist、session 也沒有隔日賣 → 維持原行為: 直接收工。"""
        r, events = _trade_runner(tmp_path, make_session())
        with caplog.at_level(logging.INFO):
            r._trade_phase([], [])
        assert r.phase == Phase.FINISHED and r.trader is None
        assert events == ["subscriber.stop", "recorder.close"]
        assert "直接 finished" in caplog.text

    def test_caller_list_is_not_trusted(self, tmp_path):
        """呼叫端傳進來的清單裡有 session 已經沒有的標的 (被移除/庫存 0 被清掉) → 以 session 為準。"""
        r, events = _trade_runner(tmp_path, make_session())
        r._trade_phase([], ["GHOST"])
        assert r.phase == Phase.FINISHED and r.trader is None

    def test_with_overnight_keeps_order_and_dedups(self):
        r = Runner()
        r.session = _session_with_inventory("2030", 1)
        r.session.add_overnight("2030")
        assert r._with_overnight(["8227"]) == ["8227", "2030"]
        assert r._with_overnight(["2030", "8227"]) == ["2030", "8227"]     # 已在 base → 不重複
        assert r._with_overnight([]) == ["2030"]


# ─────────────────────────── node 路徑 (訂閱 / 價格重查 / 轉場) ───────────────────────────
def _node_runner(monkeypatch, marked, add_at):
    """跑真的 _run_node_phases,外部依賴全換假的。add_at = 使用者在哪個「等待時點」期間手動加入。"""
    r = Runner()
    r.session = _session_with_inventory("2030", 1)
    r.sdk = object()
    r.cfg = SimpleNamespace(role="node", hub_url="http://hub:8100", hub_freeze_time="08:59:50",
                            pre_order_time="08:59:58", bid_drop_ratio=0.5, batch_size=199,
                            batch_rotate_sec=30, socket_count=1, debug=False)
    seen = {"subscribed": None, "trades": None, "refresh": [], "trade_phase": None}

    class FakeSub:
        def __init__(self, **kw):
            seen["subscribed"] = list(kw["universe"])

        def start(self):
            pass

        def subscribe_trades_for(self, syms):
            seen["trades"] = list(syms)

        def set_handlers(self, **kw):
            pass

    monkeypatch.setattr("subscriber.Subscriber", FakeSub)
    monkeypatch.setattr("recorder.TickRecorder", lambda path: SimpleNamespace(close=lambda: None))
    monkeypatch.setattr("filter.make_on_book_handler", lambda *a, **k: (lambda *x, **y: None))
    monkeypatch.setattr("filter.wait_until_end_time", lambda state, cfg: None)
    monkeypatch.setattr("node_client.pull_marked_snapshot",
                        lambda url, deadline: {"final": True, "symbols": []})
    r._prepare_overnight = lambda output_dir: []                       # 08:00 清單是空的
    r._apply_marked_snapshot = lambda snap: list(marked)
    r._start_pre_order_timer = lambda: None
    r._start_cancel_pending_timer = lambda: None
    r._refresh_overnight_prices = (
        lambda attempts=1, sleep_sec=0.0, label="":
        seen["refresh"].append((label, r._overnight_now())) or {})

    def _fake_wait(t):
        if t == add_at:
            r.session.add_overnight("2030")                            # 使用者在 UI 手動加入
    r._wait_until_clock = _fake_wait
    r._trade_phase = lambda watchlist, overnight: seen.update(
        trade_phase=(list(watchlist), list(overnight)))
    return r, seen


class TestNodePathUsesLiveList:
    def test_manual_add_before_freeze_is_subscribed_priced_and_traded(self, monkeypatch):
        # 2030 **不在**當日 marked 裡 — 事故當天它剛好也被 mark 才有訂到行情;這裡驗沒那麼幸運的情況
        r, seen = _node_runner(monkeypatch, marked=["8227"], add_at="08:59:50")
        r._run_node_phases()

        assert seen["subscribed"] == ["8227", "2030"], "手動加入的標的要訂閱 (即使不在 marked)"
        assert seen["trades"] == ["8227", "2030"]
        # 08:00 清單是空的,兩次價格重查也不能被跳過;凍結那次要看得到剛加入的 2030
        assert seen["refresh"] == [("08:30", []), ("freeze", ["2030"])]
        assert seen["trade_phase"] == ([], ["2030"]), "09:00 轉場要帶即時清單"

    def test_manual_add_before_0830_gets_priced_at_0830(self, monkeypatch):
        r, seen = _node_runner(monkeypatch, marked=[], add_at="08:30:00")
        r._run_node_phases()
        assert seen["refresh"] == [("08:30", ["2030"]), ("freeze", ["2030"])]
        assert seen["subscribed"] == ["2030"]
        assert seen["trade_phase"] == ([], ["2030"])

    def test_no_overnight_at_all_is_unchanged(self, monkeypatch):
        r, seen = _node_runner(monkeypatch, marked=["8227"], add_at="never")
        r._run_node_phases()
        assert seen["subscribed"] == ["8227"]
        assert seen["refresh"] == [("08:30", []), ("freeze", [])]      # 空清單 → 重查函式自己跳過
        assert seen["trade_phase"] == ([], [])


# ─────────────────────────── 手動加入 (track_overnight) ───────────────────────────
def _track_runner(phase, running):
    r = Runner()
    r.session = _session_with_inventory("2030", 1)
    r.phase = phase
    r.is_running = lambda: running
    r.cfg = SimpleNamespace(trading_end_time="13:24:00")
    writes = []
    r._write_overnight_file = lambda: writes.append(1)
    return r, writes


class TestTrackOvernight:
    def test_running_runner_refreshes_prices_immediately(self, monkeypatch):
        _freeze(monkeypatch, *WED, 8, 33)
        r, writes = _track_runner(Phase.SUBSCRIBE, running=True)
        r.sdk = object()
        calls = []
        r._refresh_overnight_prices = (
            lambda attempts=1, sleep_sec=0.0, label="": calls.append(label) or {})

        assert r.track_overnight("2030") is True
        assert _wait(lambda: calls == ["manual-add"]), "加入當下要背景補查漲停/跌停價"
        assert writes == [1] and r.session.has_overnight("2030")

    def test_no_price_refresh_when_runner_not_logged_in(self, monkeypatch):
        _freeze(monkeypatch, *WED, 7, 30)                  # 08:00 前: runner 還沒啟動
        r, writes = _track_runner(Phase.IDLE, running=False)
        calls = []
        r._refresh_overnight_prices = lambda **kw: calls.append(kw) or {}
        assert r.track_overnight("2030") is True           # 不警告 — 08:00 會正常載入
        time.sleep(0.1)
        assert calls == [] and writes == [1]

    def test_added_after_early_finish_warns_but_persists(self, monkeypatch, caplog):
        """09:00 當下沒有任何標的而提早收工,之後才手動加入 → 存檔 + 明講今天不會賣。"""
        _freeze(monkeypatch, *WED, 9, 5)
        r, writes = _track_runner(Phase.FINISHED, running=False)
        with caplog.at_level(logging.INFO), pytest.raises(RuntimeError) as ei:
            r.track_overnight("2030")
        assert "今天不會自動賣出" in str(ei.value) and "已加入清單並存檔" in str(ei.value)
        assert r.session.has_overnight("2030"), "清單仍要加入 (留給下個交易日)"
        assert writes == [1], "仍要存檔"
        assert [x for x in caplog.records if x.levelno >= logging.CRITICAL]

    def test_runner_never_started_today_warns(self, monkeypatch):
        _freeze(monkeypatch, *WED, 8, 40)                  # 服務在 08:00 之後才重啟 → 今天沒有主流程
        r, _ = _track_runner(Phase.IDLE, running=False)
        with pytest.raises(RuntimeError, match="沒有在執行"):
            r.track_overnight("2030")

    def test_runner_error_warns(self, monkeypatch):
        _freeze(monkeypatch, *WED, 10, 0)
        r, _ = _track_runner(Phase.ERROR, running=False)
        with pytest.raises(RuntimeError, match="出錯"):
            r.track_overnight("2030")

    @pytest.mark.parametrize("when,phase,running", [
        ((*WED, 14, 0), Phase.FINISHED, False),            # 收盤後 → 替下個交易日加的
        ((*WED, 20, 0), Phase.FINISHED, False),            # 晚上
        ((*SAT, 10, 0), Phase.IDLE, False),                # 假日
        ((*WED, 9, 30), Phase.TRADING, True),              # 盤中、主流程在跑
        ((*WED, 8, 45), Phase.SUBSCRIBE, True),            # 盤前、主流程在跑
    ])
    def test_no_warning_when_not_applicable(self, monkeypatch, when, phase, running):
        _freeze(monkeypatch, *when)
        r, writes = _track_runner(phase, running)
        r._refresh_overnight_prices = lambda **kw: {}
        assert r.track_overnight("2030") is True
        assert writes == [1]

    def test_adds_to_live_subscriber(self, monkeypatch):
        _freeze(monkeypatch, *WED, 8, 59)
        r, _ = _track_runner(Phase.SUBSCRIBE, running=True)
        r.subscriber = _FakeSubscriber()
        r.track_overnight("2030")
        assert r.subscriber.added == ["2030"]


# ─────────────────────────── 價格重查 ───────────────────────────
class TestRefreshPrices:
    def test_empty_live_list_does_not_query(self):
        r = Runner()
        r.session = make_session()
        r._query_overnight_prices = lambda *a, **k: pytest.fail("空清單不該查")
        assert r._refresh_overnight_prices(label="x") == {}

    def test_manual_symbol_gets_limit_up_and_down(self):
        """手動加入的標的經重查後 session 要有漲停價 — 事故當天 UI 顯示 limit_up: null。"""
        r = Runner()
        r.session = _session_with_inventory("2030", 1)
        r.session.add_overnight("2030")
        r.sdk = SimpleNamespace(marketdata=SimpleNamespace(rest_client=SimpleNamespace(stock=object())))

        def _fake_query(stock, sym, require_today=False):
            assert require_today is True
            r.limit_downs[sym] = 30.1
            return 36.7
        r._query_limit_up = _fake_query

        assert r._refresh_overnight_prices(attempts=1, label="manual-add") == {"2030": 36.7}
        assert r.session.overnight_limit_ups["2030"] == 36.7
        assert r.session.limit_downs["2030"] == 30.1
