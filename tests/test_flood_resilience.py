"""委託洪水韌性 (2026-09-14 node1 事故) — ITEM C。

事故: 同登入的第三方程式狂送 28,393 筆被拒委託 → 帳戶委託清單 33,940 筆;一次 get_order_results ≈540 MB RSS
(不歸還)、兩個重疊 ≈1.09 GB。hit_limit_up 09:02:28 兩個快照重疊 (11.1 s / 5.6 s)、盤中對帳每 ~60 s 一次 4~11 s,
主機 RAM+swap 耗盡當機;另外 3 分鐘內 ~15k 行「order_log 無此書號」WARNING + ~15k 行 broker ERROR。

修法 (本檔驗證):
  1. broker._query_order_results single-flight: 全 process 同時最多一個 SDK 查詢在飛,同時呼叫者共用結果、
     等待有上限 (逾時 raise OrderLookupError ≠ 查無)、完成後不快取;fresh_after 要求「之後才開始」的查詢
     (_recover_order_no / 收盤掃單 / 重連補收);仍守 5/s
  2. cancel_all_pending drain: 掃單返回後才起算重試間隔、同一輪照樣 drain
  3. cancel_all_pending 序列化 (後到者等前一次,有上限),各自摘要
  4. 盤中對帳: 已有全清單查詢在飛 → 略過;上次清單 >1 萬筆 → 間隔 300 s (env 現讀)
  5. 非本策略委託的新單拒單回報: 首見文案立即 log (保留「order_log 無此書號」)、其餘定期彙總;
     撤單回報 / 與本策略委託相符者絕不折疊;broker 只對 session 判定非本策略者略過逐筆 ERROR
"""
import logging
import re
import threading
import time as _real_time
from types import SimpleNamespace

import pytest

import broker as broker_mod
import trading_session as ts_mod
from broker import OrderLookupError, OrderNotFound, RealOrderClient
from fakes_cancel import (FakeSnapshotBroker, install_fake_clock, today_at, make_session, wait_until)

RATE_MSG = "Login Error, 業務系統流量控管"
FLOOD_MSG = "委託價格超過漲跌停範圍"
FLOOD_MSG_2 = "證券帳號委託數量超過上限"


# ─── 共用假 SDK / 工具 ────────────────────────────────────────────

def _res(ok=True, data=None, message=""):
    return SimpleNamespace(is_success=ok, data=list(data or []), message=message)


def _order(no, symbol="5386", buy=True, qty=1000, filled=0, status=10, user_def="hitlimit", after=None):
    kw = dict(order_no=no, stock_no=symbol, buy_sell="Buy" if buy else "Sell", quantity=qty,
              filled_qty=filled, status=status, user_def=user_def)
    if after is not None:
        kw["after_qty"] = after
    return SimpleNamespace(**kw)


class _Tracker:
    """跨 SDK 物件的「同時在飛 SDK 查詢數」追蹤 (驗全 process 不重疊)。"""

    def __init__(self):
        self.lk = threading.Lock()
        self.active = 0
        self.max_active = 0


class BlockingSDK:
    """get_order_results 假物件。results 依序回 (剩一個就重複);第 i 次呼叫若 i ∈ block_calls → 卡在 gate。
    記錄每次呼叫開始的 perf_counter、最大同時在飛數;cancel_order 一律成功。"""

    def __init__(self, results=None, block_calls=(), delay=0.0, tracker=None):
        self.results = list(results or [_res(True, [])])
        self.block_calls = set(block_calls)
        self.delay = delay
        self.gate = threading.Event()
        self.entered = threading.Event()
        self.calls = []
        self.cancel_calls = []
        self.tracker = tracker or _Tracker()
        self._lk = threading.Lock()
        self.stock = SimpleNamespace(get_order_results=self._get, cancel_order=self._cancel)

    @property
    def max_active(self):
        return self.tracker.max_active

    def _get(self, account):
        with self._lk:
            idx = len(self.calls)
            self.calls.append(_real_time.perf_counter())
            res = self.results.pop(0) if len(self.results) > 1 else self.results[0]
        with self.tracker.lk:
            self.tracker.active += 1
            self.tracker.max_active = max(self.tracker.max_active, self.tracker.active)
        try:
            self.entered.set()
            if idx in self.block_calls and not self.gate.wait(10):
                raise RuntimeError("test gate 逾時")
            if self.delay:
                _real_time.sleep(self.delay)
            if isinstance(res, BaseException):
                raise res
            return res
        finally:
            with self.tracker.lk:
                self.tracker.active -= 1

    def _cancel(self, account, obj):
        with self._lk:
            self.cancel_calls.append(obj)
        return _res(True, [])


def _bg(fn, *a, **kw):
    box = {}

    def _run():
        try:
            box["v"] = fn(*a, **kw)
        except BaseException as e:      # noqa: BLE001
            box["e"] = e
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return t, box


@pytest.fixture(autouse=True)
def fresh_single_flight(monkeypatch):
    """每個測試獨立的 single-flight 狀態 (不受其他測試殘留的在飛查詢影響)。"""
    sf = broker_mod._OrderQuerySingleFlight()
    monkeypatch.setattr(RealOrderClient, "_order_query_sf", sf)
    return sf


@pytest.fixture
def clients(tmp_path):
    made = []

    def _make(name="orders.csv"):
        c = RealOrderClient(tmp_path / name)
        c.account = object()
        c.connected = True
        c.healthy = True
        made.append(c)
        return c
    yield _make
    for c in made:
        sdk = c.sdk
        if isinstance(sdk, BlockingSDK):
            sdk.gate.set()                 # 放掉任何還卡著的 SDK 呼叫
        c.close()


@pytest.fixture
def client(clients):
    return clients()


def _msgs(caplog, level=None, needle=""):
    return [r.getMessage() for r in caplog.records
            if (level is None or r.levelno == level) and needle in r.getMessage()]


# ═══ 1. broker single-flight ═══════════════════════════════════════

class TestSingleFlight:
    def test_concurrent_callers_share_one_sdk_call(self, client):
        # node1 09:02:28: 兩個快照重疊 → 記憶體翻倍。8 條同時查 → 只打 1 次 SDK,各自拿到同一份結果
        sdk = BlockingSDK([_res(True, [_order("K1"), _order("K2")])], block_calls={0})
        client.sdk = sdk
        workers = [_bg(client.get_order_snapshot) for _ in range(8)]
        assert sdk.entered.wait(2)
        _real_time.sleep(0.3)                          # 其餘 7 條進入等待
        assert len(sdk.calls) == 1
        sdk.gate.set()
        for t, _ in workers:
            t.join(5)
        assert all("e" not in box for _, box in workers), [box.get("e") for _, box in workers]
        lists = [box["v"] for _, box in workers]
        assert all([r["order_no"] for r in lst] == ["K1", "K2"] for lst in lists)
        assert len(sdk.calls) == 1 and sdk.max_active == 1
        assert len({id(lst) for lst in lists}) == 8    # 各自一份 list (淺拷貝)
        assert lists[0][0]["_obj"] is lists[7][0]["_obj"]   # SDK 物件唯讀共用

    def test_no_caching_after_completion_and_gate_kept(self, client, caplog):
        caplog.set_level(logging.INFO, logger="broker")
        sdk = BlockingSDK([_res(True, [_order("K1")])])
        client.sdk = sdk
        for _ in range(3):
            client.get_order_snapshot()
        assert len(sdk.calls) == 3                     # 完成後不快取 → 每次重查
        gaps = [sdk.calls[i] - sdk.calls[i - 1] for i in range(1, 3)]
        assert min(gaps) >= 0.18                       # 5/s 閘門照舊
        assert len(_msgs(caplog, logging.INFO, "order_results ok=True n=1")) == 3   # 每次 SDK 呼叫一行

    def test_joiners_share_failure_as_lookup_error(self, client):
        sdk = BlockingSDK([_res(False, [], RATE_MSG)], block_calls={0})
        client.sdk = sdk
        workers = [_bg(client.get_order_snapshot) for _ in range(4)]
        assert sdk.entered.wait(2)
        _real_time.sleep(0.2)
        sdk.gate.set()
        for t, _ in workers:
            t.join(5)
        errs = [box.get("e") for _, box in workers]
        assert all(isinstance(e, OrderLookupError) and "流量控管" in str(e) for e in errs), errs
        assert not any(isinstance(e, OrderNotFound) for e in errs)
        assert len({id(e) for e in errs}) == 4         # 每位呼叫者各自的例外物件
        assert len(sdk.calls) == 1

    def test_join_wait_is_bounded_and_is_lookup_error_not_not_found(self, client):
        client.query_join_max_wait = 0.3
        sdk = BlockingSDK([_res(True, [_order("K1")])], block_calls={0})
        client.sdk = sdk
        leader, box = _bg(client.get_order_snapshot)
        assert sdk.entered.wait(2)
        t0 = _real_time.perf_counter()
        with pytest.raises(OrderLookupError, match="逾時"):
            client.get_order_snapshot()
        assert 0.25 <= _real_time.perf_counter() - t0 < 2.0
        # cancel facade: 等待逾時 = 查詢失敗 (FAIL:QUERY),絕不是查無/已撤
        with pytest.raises(OrderLookupError):
            client.cancel("K1", "5386", reason="x")
        assert sdk.cancel_calls == [] and len(sdk.calls) == 1
        sdk.gate.set()
        leader.join(5)
        assert [r["order_no"] for r in box["v"]] == ["K1"]

    def test_fresh_after_waits_for_older_flight_then_queries_again(self, client):
        sdk = BlockingSDK([_res(True, [_order("K1")]), _res(True, [_order("K1"), _order("K2")])],
                          block_calls={0})
        client.sdk = sdk
        leader, box_a = _bg(client.get_order_snapshot)
        assert sdk.entered.wait(2)
        mark = client.query_clock()                    # 在飛那次開始得比 mark 早
        follower, box_b = _bg(client.get_order_snapshot, fresh_after=mark)
        _real_time.sleep(0.25)
        assert len(sdk.calls) == 1                     # 不共用、也不並行 → 等
        sdk.gate.set()
        leader.join(5)
        follower.join(5)
        assert [r["order_no"] for r in box_a["v"]] == ["K1"]
        assert [r["order_no"] for r in box_b["v"]] == ["K1", "K2"]
        assert len(sdk.calls) == 2 and sdk.calls[1] >= mark and sdk.max_active == 1

    def test_fresh_after_joins_flight_that_started_later(self, client):
        sdk = BlockingSDK([_res(True, [_order("K1")])], block_calls={0})
        client.sdk = sdk
        mark = client.query_clock()
        leader, box_a = _bg(client.get_order_snapshot)
        assert sdk.entered.wait(2)
        follower, box_b = _bg(client.get_order_snapshot, fresh_after=mark)
        _real_time.sleep(0.2)
        sdk.gate.set()
        leader.join(5)
        follower.join(5)
        assert [r["order_no"] for r in box_b["v"]] == ["K1"] and len(sdk.calls) == 1

    def test_other_client_waits_instead_of_overlapping(self, clients):
        tracker = _Tracker()
        c1, c2 = clients("a.csv"), clients("b.csv")
        c1.sdk = BlockingSDK([_res(True, [_order("A1")])], block_calls={0}, tracker=tracker)
        c2.sdk = BlockingSDK([_res(True, [_order("B1")])], tracker=tracker)
        t1, box1 = _bg(c1.get_order_snapshot)
        assert c1.sdk.entered.wait(2)
        t2, box2 = _bg(c2.get_order_snapshot)
        _real_time.sleep(0.25)
        assert c2.sdk.calls == []                      # 別的 client 的查詢不共用 → 等它結束 (全 process 不重疊)
        c1.sdk.gate.set()
        t1.join(5)
        t2.join(5)
        assert [r["order_no"] for r in box1["v"]] == ["A1"]
        assert [r["order_no"] for r in box2["v"]] == ["B1"]
        assert tracker.max_active == 1

    def test_stuck_flight_abandoned_after_stale_threshold(self, client, caplog, fresh_single_flight):
        caplog.set_level(logging.INFO, logger="broker")
        client.query_flight_stale = 0.3
        sdk = BlockingSDK([_res(True, [_order("OLD")]), _res(True, [_order("NEW")])], block_calls={0})
        client.sdk = sdk
        stuck, box_a = _bg(client.get_order_snapshot)
        assert sdk.entered.wait(2)
        assert client.order_query_in_flight() is True
        _real_time.sleep(0.4)
        assert client.order_query_in_flight() is False     # 卡死超過門檻不算在飛
        assert [r["order_no"] for r in client.get_order_snapshot()] == ["NEW"]
        assert _msgs(caplog, logging.WARNING, "視為卡死")
        sdk.gate.set()
        stuck.join(5)
        assert [r["order_no"] for r in box_a["v"]] == ["OLD"]
        assert fresh_single_flight.flight is None          # 舊 flight 結束不會亂清

    def test_flight_on_replaced_sdk_not_waited_by_reconnect_reconcile(self, client, caplog, fresh_single_flight):
        # 審查 C1: 重連 atomic swap 後,舊 SDK 上卡住的查詢不可擋住新 SDK 的補收
        # (舊版等到 query_join_max_wait 逾時 → reconcile_orders 失敗且不重試 → 斷線期間成交永遠補不回)
        caplog.set_level(logging.INFO, logger="broker")
        client.query_join_max_wait = 5.0
        old = BlockingSDK([_res(True, [])], block_calls={0})
        client.sdk = old
        stuck, box_old = _bg(client.get_order_snapshot)
        assert old.entered.wait(2)
        new = BlockingSDK([_res(True, [_order("O1", symbol="2330", filled=1000)])], block_calls={1})
        client.sdk = new                                               # re_login: self.sdk = new_sdk
        client.account = object()                                      # self.account = 新帳戶物件
        s = make_session(broker=client)
        s._log_order("O1", "2330", "buy", "market_buy", 1, 0)
        t0 = _real_time.perf_counter()
        s.reconcile_orders()
        assert _real_time.perf_counter() - t0 < 2.0
        assert s.order_log["O1"]["filled_lots"] == 1
        assert len(new.calls) == 1 and len(old.calls) == 1
        assert _msgs(caplog, logging.WARNING, "舊 SDK")
        # 新 SDK 上再起一個在飛查詢 → 舊查詢結束時不可把它清掉
        t_new, box_new = _bg(client.get_order_snapshot)
        assert wait_until(lambda: len(new.calls) == 2, 3)
        old.gate.set()
        stuck.join(5)
        assert box_old.get("v") == []
        assert fresh_single_flight.flight is not None and fresh_single_flight.flight.sdk is new
        new.gate.set()
        t_new.join(5)
        assert [r["order_no"] for r in box_new["v"]] == ["O1"]
        assert fresh_single_flight.flight is None

    def test_recover_order_no_does_not_use_flight_started_before(self, client):
        # 缺書號反查: 送單前就在飛的清單不會有剛送的單 → 必須等新查詢 (否則誤判 0 候選落 UNKNOWN)
        client._claimed_order_nos = set()
        sdk = BlockingSDK([_res(True, []), _res(True, [_order("A9", symbol="2330", qty=2000)])],
                          block_calls={0})
        client.sdk = sdk
        leader, _ = _bg(client.get_order_snapshot)
        assert sdk.entered.wait(2)
        rec, box = _bg(client._recover_order_no, "2330", True, 2)
        _real_time.sleep(0.25)
        assert len(sdk.calls) == 1
        sdk.gate.set()
        leader.join(5)
        rec.join(5)
        assert box["v"] == "A9" and len(sdk.calls) == 2

    def test_in_flight_and_last_rows(self, client):
        sdk = BlockingSDK([_res(True, [_order("K1"), _order("K2")]), _res(False, [], RATE_MSG)],
                          block_calls={0})
        client.sdk = sdk
        assert client.order_query_in_flight() is False and client.last_order_query_rows() is None
        t, _ = _bg(client.get_order_snapshot)
        assert sdk.entered.wait(2)
        assert client.order_query_in_flight() is True
        sdk.gate.set()
        t.join(5)
        assert client.order_query_in_flight() is False and client.last_order_query_rows() == 2
        with pytest.raises(OrderLookupError):
            client.get_order_snapshot()
        assert client.last_order_query_rows() == 2         # 失敗不覆寫

    def test_new_instance_without_init_uses_class_defaults(self):
        c = RealOrderClient.__new__(RealOrderClient)       # tests / 替身常用
        c.account = object()
        c.sdk = BlockingSDK([_res(True, [_order("K1")])])
        assert [o.order_no for o in c._query_order_results(fresh_after=c.query_clock())] == ["K1"]
        assert c.order_query_in_flight() is False and c.is_claimed_order_no("K1") is False

    def test_fresh_callers_never_overlap_and_stay_within_five_per_second(self, client):
        sdk = BlockingSDK([_res(True, [_order("K1")])], delay=0.02)
        client.sdk = sdk
        errors = []

        def _loop():
            try:
                for _ in range(2):
                    client._query_order_results(fresh_after=client.query_clock())
            except Exception as e:   # noqa: BLE001
                errors.append(e)
        ths = [threading.Thread(target=_loop, daemon=True) for _ in range(4)]
        for t in ths:
            t.start()
        for t in ths:
            t.join(15)
        assert errors == [] and not any(t.is_alive() for t in ths)
        ts = sorted(sdk.calls)
        assert 2 <= len(ts) <= 8 and sdk.max_active == 1
        assert min(ts[i] - ts[i - 1] for i in range(1, len(ts))) >= 0.18
        for t0 in ts:
            assert sum(1 for t in ts if t0 <= t < t0 + 1.0) <= 5


class TestBrokerForeignReportLog:
    @pytest.mark.parametrize("hook,expect_error", [
        (None, True),
        (lambda rpt: True, False),
        (lambda rpt: False, True),
        (lambda rpt: 1, True),                               # 非 True → 照舊 ERROR
        (lambda rpt: (_ for _ in ()).throw(RuntimeError("boom")), True),
    ])
    def test_err_line_suppressed_only_when_hook_returns_true(self, client, caplog, hook, expect_error):
        caplog.set_level(logging.INFO, logger="broker")
        got = []
        client.on_order = got.append
        client.is_foreign_order_report = hook
        content = SimpleNamespace(order_no="Z1", stock_no="9999", status=90, function_type=0,
                                  error_message=FLOOD_MSG, filled_qty=0, last_time="")
        client._handle_order(FLOOD_MSG, content)
        assert len(got) == 1 and got[0]["error_message"] == FLOOD_MSG     # 一律照轉發
        errs = [r for r in caplog.records if r.levelno == logging.ERROR and "委託回報 err=" in r.getMessage()]
        assert bool(errs) is expect_error


# ═══ 2. 收盤 drain 迴圈 ═══════════════════════════════════════════

@pytest.fixture
def close_clock(monkeypatch):
    monkeypatch.setattr(ts_mod, "_CANCEL_DRAIN_MAX_SEC", 40.0)
    return install_fake_clock(monkeypatch, today_at(13, 23, 5))


class _LateSnapshotBroker(FakeSnapshotBroker):
    """指定書號在 release_at (假時鐘 epoch) 之前不在快照 (後檯延遲)。"""

    def __init__(self, clock):
        super().__init__()
        self.clock = clock
        self.late = {}

    def get_order_snapshot(self):
        now = self.clock.time()
        for no, at in list(self.late.items()):
            if now >= at:
                self.snapshot_missing.discard(no)
                self.late.pop(no, None)
        return super().get_order_snapshot()


class TestDrainLoop:
    def test_slow_failing_sweep_does_not_starve_drain(self, close_clock, caplog, monkeypatch):
        caplog.set_level(logging.INFO, logger="trading_session")
        b = _LateSnapshotBroker(close_clock)
        s = make_session(broker=b)
        no = b.place_market_buy("5386", 1)
        s._log_order(no, "5386", "buy", "market_buy", 1, 0)
        b.snapshot_missing.add(no)
        b.late[no] = close_clock.time() + 8.0          # 8 s 後才出現在快照
        sweeps = []

        def slow_failing_sweep(reason, dry_run, protect_active, fresh_after=None):
            start = ts_mod.time.time()
            close_clock.advance(5.0)                   # 每次掃單卡滿 5 s 逾時
            sweeps.append((start, ts_mod.time.time()))
            return {"ok": False, "live": [], "queued": [], "orphans": [], "flagged": []}
        monkeypatch.setattr(s, "_broker_sweep", slow_failing_sweep)
        s.cancel_all_pending("trading_end")
        assert s.order_log[no]["status"] == "cancelled", s.order_log[no]   # 舊版: 掃單連環重跑 → 永遠沒 drain
        assert len(sweeps) >= 3
        for (_, end0), (start1, _) in zip(sweeps, sweeps[1:]):
            assert start1 - end0 >= ts_mod._CLOSE_SWEEP_RETRY_SEC - 0.05, sweeps   # 間隔從返回後起算
        summary = _msgs(caplog, logging.WARNING, "收盤撤單 (trading_end): 已確認")
        assert summary and re.search(r"已確認 1 / 未確認 0", summary[-1]), summary
        assert re.search(rf"掃單 失敗/{len(sweeps)} 次", summary[-1]), summary[-1]
        assert _msgs(caplog, logging.CRITICAL, "皆失敗")

    def test_retry_success_drains_orphans_on_same_pass(self, close_clock, caplog, monkeypatch):
        caplog.set_level(logging.INFO, logger="trading_session")
        s = make_session()
        b = s.broker
        b.add_order("B1", "BBBB", buy=True, lots=1, status="10")
        orig = s._broker_sweep
        calls = []

        def flaky(reason, dry_run, protect_active, fresh_after=None):
            calls.append(1)
            if len(calls) == 1:
                close_clock.advance(5.0)
                return {"ok": False, "live": [], "queued": [], "orphans": [], "flagged": []}
            return orig(reason, dry_run, protect_active, fresh_after=fresh_after)
        monkeypatch.setattr(s, "_broker_sweep", flaky)
        s.cancel_all_pending("trading_end")
        assert s.order_log["B1"]["status"] == "cancelled" and len(calls) == 2
        assert re.search(r"掃單 ok/2 次", caplog.text)


# ═══ 3. cancel_all_pending 序列化 ═══════════════════════════════════

class TestCancelAllSerialized:
    def _blocking_sweep(self, s, monkeypatch, block_reason):
        gate = threading.Event()
        events = []
        orig = s._broker_sweep

        def sweep(reason, dry_run, protect_active, fresh_after=None):
            events.append(("start", reason))
            if reason == block_reason:
                assert gate.wait(10)
            out = orig(reason, dry_run, protect_active, fresh_after=fresh_after)
            events.append(("end", reason))
            return out
        monkeypatch.setattr(s, "_broker_sweep", sweep)
        return gate, events

    def test_second_run_waits_for_first_and_each_logs_summary(self, close_clock, caplog, monkeypatch):
        caplog.set_level(logging.INFO, logger="trading_session")
        s = make_session()
        gate, events = self._blocking_sweep(s, monkeypatch, "budget_breached")
        t1, box1 = _bg(s.cancel_all_pending, "budget_breached")
        assert wait_until(lambda: ("start", "budget_breached") in events, 3)
        t2, box2 = _bg(s.cancel_all_pending, "cancel_pending_time")
        _real_time.sleep(0.3)
        assert ("start", "cancel_pending_time") not in events     # 不並行
        assert _msgs(caplog, logging.WARNING, "等待前一次撤單全清跑完")
        gate.set()
        t1.join(10)
        t2.join(10)
        assert "e" not in box1 and "e" not in box2
        assert events == [("start", "budget_breached"), ("end", "budget_breached"),
                          ("start", "cancel_pending_time"), ("end", "cancel_pending_time")]
        for reason in ("budget_breached", "cancel_pending_time"):
            assert len(_msgs(caplog, logging.WARNING, f"收盤撤單 ({reason}): 已確認")) == 1

    def test_wait_is_bounded_then_runs_anyway(self, close_clock, caplog, monkeypatch):
        caplog.set_level(logging.INFO, logger="trading_session")
        monkeypatch.setattr(ts_mod, "_CANCEL_ALL_WAIT_SEC", 0.3)
        s = make_session()
        gate, events = self._blocking_sweep(s, monkeypatch, "budget_breached")
        t1, _ = _bg(s.cancel_all_pending, "budget_breached")
        assert wait_until(lambda: ("start", "budget_breached") in events, 3)
        t0 = _real_time.monotonic()
        s.cancel_all_pending("trading_end")                        # 等 0.3 s 後仍照跑
        assert 0.25 <= _real_time.monotonic() - t0 < 5
        assert ("end", "trading_end") in events and ("end", "budget_breached") not in events
        assert _msgs(caplog, logging.CRITICAL, "仍照跑")
        gate.set()
        t1.join(10)
        caplog.clear()
        s.cancel_all_pending("trading_end")                        # 鎖已正確釋放 → 不必等
        assert not _msgs(caplog, logging.WARNING, "等待前一次撤單全清跑完")


# ═══ 1b. 新鮮度接到 session: 收盤掃單 / 重連補收 ═══════════════════════════

class TestFreshnessInSession:
    def test_close_sweep_ignores_query_started_before_run(self, close_clock, client, caplog):
        # 13:23 前就在飛的舊查詢 (清單還沒有 B1) 不可拿來當收盤掃單 → 等它結束、自己再查到 B1 並撤掉
        caplog.set_level(logging.INFO, logger="trading_session")
        sdk = BlockingSDK([_res(True, []), _res(True, [_order("B1", symbol="BBBB", after=1000)])],
                          block_calls={0})
        client.sdk = sdk
        s = make_session(broker=client)
        old, _ = _bg(client.get_order_snapshot)
        assert sdk.entered.wait(2)
        run, box = _bg(s.cancel_all_pending, "trading_end")
        _real_time.sleep(0.3)
        assert len(sdk.calls) == 1
        sdk.gate.set()
        old.join(5)
        run.join(15)
        assert "e" not in box, box.get("e")
        assert s.order_log["B1"]["status"] == "cancelled" and s.order_log["B1"]["kind"] == "orphan_buy"
        assert [o.order_no for o in sdk.cancel_calls] == ["B1"]
        assert sdk.max_active == 1
        assert re.search(r"order_log 外孤兒 1", caplog.text)

    def test_reconcile_orders_ignores_query_started_before_reconnect(self, client):
        sdk = BlockingSDK([_res(True, [_order("O1", symbol="2330", filled=0)]),
                           _res(True, [_order("O1", symbol="2330", filled=1000)])], block_calls={0})
        client.sdk = sdk
        s = make_session(broker=client)
        s._log_order("O1", "2330", "buy", "market_buy", 1, 0)
        old, _ = _bg(client.get_order_snapshot)
        assert sdk.entered.wait(2)
        rec, box = _bg(s.reconcile_orders)
        _real_time.sleep(0.3)
        assert len(sdk.calls) == 1
        sdk.gate.set()
        old.join(5)
        rec.join(10)
        assert "e" not in box
        assert s.order_log["O1"]["filled_lots"] == 1 and len(sdk.calls) == 2

    def test_broker_without_fresh_after_param_unchanged(self, close_clock):
        class ClockOnlyBroker(FakeSnapshotBroker):
            def query_clock(self):
                return 123.0                                       # 有時鐘但快照不收 fresh_after
        s = make_session(broker=ClockOnlyBroker())
        s.broker.add_order("B1", "BBBB", buy=True, lots=1, status="10")
        s.cancel_all_pending("trading_end")
        assert s.order_log["B1"]["status"] == "cancelled"

    def test_with_fresh_after_helper(self):
        seen = []

        def with_kw(fresh_after=None):
            seen.append(fresh_after)

        def without_kw():
            seen.append("plain")
        ts_mod._with_fresh_after(with_kw, 5.0)()
        ts_mod._with_fresh_after(without_kw, 5.0)()
        ts_mod._with_fresh_after(with_kw, None)()
        assert seen == [5.0, "plain", None]


# ═══ 4. 盤中對帳 ═══════════════════════════════════════════════════

class _QueryInfoBroker(FakeSnapshotBroker):
    def __init__(self):
        super().__init__()
        self.busy = False
        self.rows = None

    def order_query_in_flight(self):
        return self.busy

    def last_order_query_rows(self):
        return self.rows


@pytest.fixture
def intraday(monkeypatch):
    monkeypatch.delenv("INTRADAY_SWEEP_BIG_LIST_ROWS", raising=False)
    monkeypatch.delenv("INTRADAY_SWEEP_BIG_LIST_INTERVAL_SEC", raising=False)
    clock = install_fake_clock(monkeypatch, today_at(10, 0, 0))

    def _make(broker=None):
        s = make_session(broker=broker if broker is not None else _QueryInfoBroker())
        calls = []

        def fake_reconcile(dry_run=True):
            calls.append(dry_run)
            return {"ok": True, "live": [], "queued": [], "orphans": [], "flagged": []}
        monkeypatch.setattr(s, "intraday_reconcile_once", fake_reconcile)
        return s, calls
    return clock, _make


class TestIntradaySweep:
    def test_skipped_while_full_list_query_in_flight(self, intraday, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")
        clock, make = intraday
        s, calls = make()
        s.broker.busy = True
        for _ in range(3):
            s._maybe_intraday_sweep()
        assert calls == []
        assert len(_msgs(caplog, logging.INFO, "盤中對帳略過")) == 1   # 每段只 log 一次
        s.broker.busy = False
        s._maybe_intraday_sweep()                                     # 不算一輪 → 查詢一結束馬上可掃
        assert len(calls) == 1

    def test_big_list_lengthens_interval_to_300s(self, intraday, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")
        clock, make = intraday
        s, calls = make()
        s.broker.rows = 33_940                                        # node1 事故清單筆數
        s._maybe_intraday_sweep()
        assert len(calls) == 1
        clock.advance(61)
        s._maybe_intraday_sweep()
        assert len(calls) == 1
        clock.advance(240)
        s._maybe_intraday_sweep()
        assert len(calls) == 2
        assert len(_msgs(caplog, logging.WARNING, "盤中對帳間隔拉長")) == 1

    def test_small_list_keeps_60s(self, intraday):
        clock, make = intraday
        s, calls = make()
        s.broker.rows = 9_000
        s._maybe_intraday_sweep()
        clock.advance(61)
        s._maybe_intraday_sweep()
        assert len(calls) == 2

    def test_knobs_read_from_env_at_call_time(self, intraday, monkeypatch):
        clock, make = intraday
        s, calls = make()
        s.broker.rows = 9_000
        monkeypatch.setenv("INTRADAY_SWEEP_BIG_LIST_ROWS", "5000")
        s._maybe_intraday_sweep()
        clock.advance(61)
        s._maybe_intraday_sweep()
        assert len(calls) == 1                                        # 9000 > 5000 → 大清單
        monkeypatch.setenv("INTRADAY_SWEEP_BIG_LIST_INTERVAL_SEC", "100")
        clock.advance(40)
        s._maybe_intraday_sweep()
        assert len(calls) == 2

    def test_broker_without_query_info_unchanged(self, intraday):
        clock, make = intraday
        s, calls = make(broker=FakeSnapshotBroker())
        s._maybe_intraday_sweep()
        clock.advance(61)
        s._maybe_intraday_sweep()
        assert len(calls) == 2

    def test_real_client_reports_in_flight(self, intraday, client):
        clock, make = intraday
        sdk = BlockingSDK([_res(True, [_order("K1")])], block_calls={0})
        client.sdk = sdk
        s, calls = make(broker=client)
        t, _ = _bg(client.get_order_snapshot)
        assert sdk.entered.wait(2)
        s._maybe_intraday_sweep()
        assert calls == []
        sdk.gate.set()
        t.join(5)
        s._maybe_intraday_sweep()
        assert len(calls) == 1


# ═══ 5. 非本策略委託的拒單回報彙總 ═══════════════════════════════════

def _rpt(order_no, ft="0", status="90", err=FLOOD_MSG, symbol="9999", user_def=None):
    d = {"order_no": order_no, "symbol": symbol, "status": status, "filled_qty": 0,
         "error_message": err, "function_type": ft, "last_time": ""}
    if user_def is not None:
        d["user_def"] = user_def
    return d


@pytest.fixture
def rclock(monkeypatch):
    return install_fake_clock(monkeypatch, today_at(9, 2, 30))


def _no_log_warnings(caplog):
    return _msgs(caplog, logging.WARNING, "order_log 無此書號")


class TestForeignRejectAggregation:
    def test_first_occurrence_per_message_then_periodic_summary(self, rclock, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")
        s = make_session()
        s.place_pre_orders(["2330"], {"2330": 100.0})
        before = {k: dict(v) for k, v in s.order_log.items()}
        for i in range(200):
            s._on_order(_rpt(f"X{i}"))
        for i in range(50):
            s._on_order(_rpt(f"Y{i}", err=FLOOD_MSG_2, symbol="9998"))
        warns = _no_log_warnings(caplog)
        assert len(warns) == 2                                        # 每種文案首見一行
        assert "X0" in warns[0] and FLOOD_MSG in warns[0] and "Y0" in warns[1]
        assert {k: dict(v) for k, v in s.order_log.items()} == before   # 不動任何 st / order_log
        assert not _msgs(caplog, logging.WARNING, "拒單回報彙總")
        rclock.advance(61)
        s._maybe_log_foreign_reject_summary()
        summ = _msgs(caplog, logging.WARNING, "非本策略委託拒單回報彙總")
        assert len(summ) == 1
        assert "共 250 筆" in summ[0] and "未逐條 log 248 筆" in summ[0]
        assert f"200 筆「{FLOOD_MSG}」" in summ[0] and f"50 筆「{FLOOD_MSG_2}」" in summ[0]
        s._maybe_log_foreign_reject_summary()
        assert len(_msgs(caplog, logging.WARNING, "非本策略委託拒單回報彙總")) == 1   # 已重置

    def test_summary_emitted_inline_while_flood_continues(self, rclock, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")
        s = make_session()
        for i in range(10):
            s._on_order(_rpt(f"X{i}"))
        rclock.advance(61)
        s._on_order(_rpt("X10"))
        summ = _msgs(caplog, logging.WARNING, "非本策略委託拒單回報彙總")
        assert len(summ) == 1 and "共 11 筆" in summ[0] and "今日累計 11" in summ[0]

    def test_single_reject_no_redundant_summary(self, rclock, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")
        s = make_session()
        s._on_order(_rpt("X1"))
        rclock.advance(61)
        s._maybe_log_foreign_reject_summary()
        assert len(_no_log_warnings(caplog)) == 1
        assert not _msgs(caplog, logging.WARNING, "拒單回報彙總")      # 全都逐條 log 過 → 不另彙總

    def test_first_log_cap_bounds_distinct_messages(self, rclock, caplog, monkeypatch):
        caplog.set_level(logging.INFO, logger="trading_session")
        monkeypatch.setattr(ts_mod, "_FOREIGN_REJECT_FIRST_LOG_MAX", 5)
        s = make_session()
        for i in range(20):
            s._on_order(_rpt(f"X{i}", err=f"拒單流水 {i}"))
        assert len(_no_log_warnings(caplog)) == 5
        s._maybe_log_foreign_reject_summary(force=True)
        assert "未逐條 log 15 筆" in _msgs(caplog, logging.WARNING, "拒單回報彙總")[0]

    def test_roll_day_logs_first_occurrence_again(self, rclock, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")
        s = make_session()
        s._on_order(_rpt("X1"))
        s._on_order(_rpt("X2"))
        s.roll_day("2026-09-16")
        s._on_order(_rpt("X3"))
        assert len(_no_log_warnings(caplog)) == 2

    def test_cancel_reports_and_our_orders_never_folded(self, rclock, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")

        class ClaimBroker(FakeSnapshotBroker):
            def is_claimed_order_no(self, no):
                if no == "BOOM":
                    raise RuntimeError("x")
                return no == "C1"
        s = make_session(broker=ClaimBroker())
        s.place_pre_orders(["2330"], {"2330": 100.0})
        p = s.trades["2330"].order_no
        s.request_cancel("Q1", "5386", "x")                           # 佇列中 (券商掃單孤兒,無 row)
        per_report = [
            _rpt("Q1"),                                               # 在撤單佇列
            _rpt("C1"),                                               # broker 已認領 (剛下單還沒進 order_log)
            _rpt("BOOM"),                                             # 查認領失敗 → 不確定 → 不折疊
            _rpt("", symbol="2330"),                                  # 無書號、本策略標的 (9049 型)
            _rpt(None, symbol=""),                                    # 無書號、無標的
            _rpt("Z39", ft=None, status="39"),                        # 撤單失敗型 status
            _rpt("Z30", ft="", status="30"),
            _rpt("K77", symbol="2330", user_def="hitlimit"),          # 本策略委託但書號沒追蹤到 (UNKNOWN-* / 重啟後)
            _rpt("K78", symbol="9999", user_def="hitlimit"),          # 同上,標的也不在 trades
        ]
        for rpt in per_report:
            assert s._is_foreign_order_report(rpt) is False, rpt
            for _ in range(3):
                s._on_order(dict(rpt))
        assert len(_no_log_warnings(caplog)) == 3 * len(per_report)   # 全部逐筆照舊
        # ft 30 撤單回報 (未知書號): 走撤單分支,不計入非本策略彙總
        ft30 = _rpt("ZZZ", ft="30", status="39", err="[115]撤單失敗")
        assert s._is_foreign_order_report(ft30) is False
        s._on_order(ft30)
        # 本策略委託 (在 order_log) 的拒單 → 原 rejected 路徑
        assert s._is_foreign_order_report(_rpt(p, symbol="2330")) is False
        s._on_order(_rpt(p, symbol="2330"))
        assert s.order_log[p]["status"] == "rejected"
        assert s._foreign_rej_total == 0 and s._foreign_rej_day_total == 0
        s._maybe_log_foreign_reject_summary(force=True)
        assert not _msgs(caplog, logging.WARNING, "拒單回報彙總")

    def test_no_order_no_unrelated_symbol_is_folded(self, rclock, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")
        s = make_session()
        s.place_pre_orders(["2330"], {"2330": 100.0})
        for _ in range(5):
            s._on_order(_rpt("", symbol="9999"))
        assert len(_no_log_warnings(caplog)) == 1 and s._foreign_rej_total == 5

    def test_broker_error_line_suppressed_only_for_foreign(self, rclock, client, caplog):
        caplog.set_level(logging.INFO)
        s = make_session(broker=client)
        client.on_order = s._on_order
        client.is_foreign_order_report = s._is_foreign_order_report
        client._claimed_order_nos.add("C9")
        s._log_order("O1", "2330", "buy", "market_buy", 1, 0)

        def content(no, sym):
            return SimpleNamespace(order_no=no, stock_no=sym, status=90, function_type=0,
                                   error_message=FLOOD_MSG, filled_qty=0, last_time="")

        def broker_errors(no):
            return [r for r in caplog.records if r.name == "broker" and r.levelno == logging.ERROR
                    and f"order={no}" in r.getMessage()]
        for i in range(5):
            client._handle_order(FLOOD_MSG, content(f"F{i}", "9999"))
        assert all(broker_errors(f"F{i}") == [] for i in range(5))
        assert len(_no_log_warnings(caplog)) == 1
        client._handle_order(FLOOD_MSG, content("O1", "2330"))       # 本策略委託
        assert len(broker_errors("O1")) == 1 and s.order_log["O1"]["status"] == "rejected"
        client._handle_order(FLOOD_MSG, content("C9", "2330"))       # 已認領
        assert len(broker_errors("C9")) == 1

    def test_hitlimit_report_never_folded_even_after_same_text_flood(self, rclock, client, caplog):
        # 審查 P4: 第三方同文案洪水之後,本策略委託 (user_def=hitlimit) 書號沒追蹤到 (UNKNOWN-*) 的拒單 → 照舊逐筆
        caplog.set_level(logging.INFO)
        s = make_session(broker=client)
        client.on_order = s._on_order
        client.is_foreign_order_report = s._is_foreign_order_report
        s._log_order("UNKNOWN-1-1", "2330", "buy", "market_buy", 1, 0)

        def content(no, sym, user_def):
            return SimpleNamespace(order_no=no, stock_no=sym, status=90, function_type=0, error_message=FLOOD_MSG,
                                   filled_qty=0, last_time="", user_def=user_def)
        for i in range(3):
            client._handle_order(FLOOD_MSG, content(f"F{i}", "9999", "other"))
        n_warn = len(_no_log_warnings(caplog))
        assert n_warn == 1 and s._foreign_rej_total == 3
        client._handle_order(FLOOD_MSG, content("K77", "2330", "hitlimit"))
        assert [r for r in caplog.records if r.name == "broker" and r.levelno == logging.ERROR
                and "order=K77" in r.getMessage()]
        warns = _no_log_warnings(caplog)
        assert len(warns) == n_warn + 1 and "K77" in warns[-1]
        assert s._foreign_rej_total == 3                              # 沒被計入非本策略彙總

    def test_broker_report_carries_user_def(self, client):
        got = []
        client.on_order = got.append
        client._handle_order(None, SimpleNamespace(order_no="K1", stock_no="2330", status=10, function_type=0,
                                                   error_message="", filled_qty=0, last_time="",
                                                   user_def="hitlimit"))
        client._handle_order("err", SimpleNamespace(order_no="K2", stock_no="2330", status=90, function_type=0,
                                                    error_message="x", filled_qty=0, last_time=""))
        assert [r["user_def"] for r in got] == ["hitlimit", ""]

    def test_connect_async_installs_hook(self, monkeypatch, tmp_path):
        class _FakeClient:
            def __init__(self, log_path):
                self.on_fill = self.on_order = self.on_disconnect = self.on_reconnected = None
                self.is_foreign_order_report = None
                self.connected = self.healthy = True

            def connect(self, *a, **kw):
                return None

            def disconnect(self):
                return None

            def get_inventories(self):
                return []
        monkeypatch.setattr(broker_mod, "RealOrderClient", _FakeClient)
        monkeypatch.delenv("FUBON_ACCOUNT_ID", raising=False)
        s = ts_mod.TradingSession(auto_cancel_worker=False)
        s.set_mode("real")
        s.connect_async("A", "p", "x.pfx", "", False, tmp_path)
        assert wait_until(lambda: not s.connecting, timeout=5.0)
        assert s.broker.is_foreign_order_report == s._is_foreign_order_report

    def test_worker_loop_emits_periodic_summary(self, caplog, monkeypatch):
        caplog.set_level(logging.INFO, logger="trading_session")
        monkeypatch.setattr(ts_mod, "_FOREIGN_REJECT_SUMMARY_SEC", 0.3)
        s = make_session()
        for i in range(4):
            s._on_order(_rpt(f"X{i}"))
        assert not _msgs(caplog, logging.WARNING, "拒單回報彙總")
        s.auto_cancel_worker = True
        s._ensure_cancel_worker()
        assert wait_until(lambda: bool(_msgs(caplog, logging.WARNING, "拒單回報彙總")), 5.0)
        assert "共 4 筆" in _msgs(caplog, logging.WARNING, "拒單回報彙總")[0]
