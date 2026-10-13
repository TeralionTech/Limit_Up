"""免查詢撤單 (2026-10-07 node1 隔日賣 4716 比 node4 晚 3.7 s 送單)。

根因: sdk.stock.get_order_results 整段呼叫期間**整個 Python process 停擺** — 行情 tick 不處理、回報不處理、
其他下單的回應擠到呼叫結束那一瞬間才回來 (三台 node 的 tick 空窗與該次查詢耗時吻合到 10 ms 內;同登入
家族清單 1500~2400 筆時每次 1.1~1.8 s)。撤單原本每次都要先拉全清單找委託物件 → 開盤第一筆市價成功後
撤預掛 P / 撤管線多送 M、出場前撤單,全部會凍住 process。

修法契約 (含 2026-10-12 對抗審查後的補強):
  - broker._place 成功且回傳物件自帶書號 → 記下 result.data;撤這筆直接用它 (cancel_cached),**不呼叫
    get_order_results**。成功才算撤掉 (cancelled 仍只由券商確認寫入)。
  - 被券商拒:
      終結文案 (…已不允許取消 / 已無剩餘量) → 丟掉物件 (不算「已證實」);「已成交」者 filled_qty 未知 →
        session 留佇列: 成交回報已到 (row filled) 就直接結案,否則下一輪走快照核實。
      在途文案 (「檢查成交回報」— 撤單請求已受理、等交易所) → 物件留著繼續免查詢重試,自第一次被拒起
        拖過時間上限 (證實前 5 s / 證實後 10 s) 才放棄、退回快照。
      其他未知文案 → 證實前 1 次就放棄;證實後每筆給 3 次。
  - 「已證實」= 本連線曾有一筆免查詢撤單真的撤成。證實前每放棄一筆記一次,滿 2 筆整個停用 (CRITICAL);
    停用後才回來的撤成 (同一批併發) 會重新啟用。
  - SDK 例外 (物件送不出去;含 PyO3 panic 這種 BaseException — broker.cancel_by_obj 轉成 RuntimeError) → 丟掉;
    證實前連續 2 次 → 整個停用 (CRITICAL);證實後視為傳輸層暫態不停用。途中斷線 / 已換 SDK 的失敗不算。
    交易 WS 重連 (re_login) → 停用旗標與計數重設 (已證實保留)。
  - 同一書號的免查詢撤單往返中 → 不重送、**也不查清單** (CancelInFlight;入佇列稍後看結果);
    往返超過門檻 (卡死) 才改走快照。
  - 沒物件的單 (缺書號反查認領 / 非本次連線下的 / 掃單孤兒) 照舊走快照。
  - env CANCEL_DIRECT_OBJ=0 → 全部走舊路徑。
這裡用**真的** RealOrderClient + 假 SDK (不 login),session 層測試直接掛真 client → 兩層一起驗。
"""
import csv
import logging
import sys
import threading
import time
from datetime import time as dtime
from types import ModuleType, SimpleNamespace

import pytest

import broker as broker_mod
import trading_session as ts_mod
from broker import RealOrderClient, CancelRejected, CancelInFlight, OrderLookupError
from trading_session import classify_cancel_error, _classify_direct_cancel_error
from fakes_cancel import (MSG_FILLED, MSG_ALREADY_CANCELLED, make_session, run_once, fill,
                          install_fake_clock, today_at, wait_until)

SYM = "3265"
LU = 217.0
MSG_INFLIGHT = "證券檢查成交回報是否已成交"        # 2026-10-07 node1 MH014: 撤單請求已受理、等交易所 (4.1 s)
MSG_UNKNOWN = "[999]委託資料不符"                  # 沒見過的非終結文案 (物件有問題時最可能的樣子)
MSG_NO_REMAINING = "該筆委託已無剩餘量(交)[9050-後檯]"   # 2026-10-06 node4 MH01p: 已全數成交的撤單拒絕
MSG_FAILED_ORDER = "[115]證券委託目前狀態失敗單已不允許取消交易"   # 假設的 status 90 文案 (含終結片段、非已知兩組)


# ─────────────────────────── 假 fubon_neo / 假 SDK ───────────────────────────
def _res(ok=True, data=None, message=None):
    return SimpleNamespace(is_success=ok, data=data, message=message)


class FakePanic(BaseException):
    """仿 pyo3_runtime.PanicException: 繼承 BaseException,不是 Exception。"""


class _MonoClock:
    """替換 broker.time: monotonic 可控,其餘照真的 time (broker 的在途/被拒時間上限都用 monotonic)。"""

    def __init__(self):
        self.t = 1000.0

    def monotonic(self):
        return self.t

    def __getattr__(self, name):
        return getattr(time, name)


class FakeFubon:
    """假富邦: 券商端委託簿 + 下單/撤單/全清單查詢。
    placed_objs[no] = place_order 回傳的物件;book[no] = 券商端那筆 (全清單查詢回的是**另一個**物件) —
    cancel_calls 記 (書號, 是否用下單回傳物件),據此分辨走的是免查詢還是快照路徑。
    cancel_script[no] = 依序消耗的回應: str → is_success=False + message;Exception → raise;
                        callable → 先呼叫 (做副作用) 再以其回傳值當回應。
    refuse_placed_objs=True → 凡是拿下單回傳物件來撤都 raise TypeError (模擬「這版 SDK 不收」)。
    reject_placed_msg="…" → 凡是拿下單回傳物件來撤都回 is_success=False + 這段文案 (模擬後檯以回應式拒收)。
    block_placed[no]=Event → 拿下單回傳物件撤這筆時卡住,直到 Event set (模擬 SDK 呼叫卡死)。
    panic_placed_objs=True → 凡是拿下單回傳物件來撤都 raise FakePanic (BaseException 子類;模擬 PyO3 的
                              PanicException — `except Exception` 接不到)。"""

    def __init__(self):
        self.lk = threading.Lock()
        self.n = 0
        self.book = {}
        self.placed_objs = {}
        self.query_calls = 0
        self.cancel_calls = []
        self.cancel_script = {}
        self.refuse_placed_objs = False
        self.reject_placed_msg = None
        self.panic_placed_objs = False
        self.block_placed = {}
        self.place_without_no = False
        self.stock = SimpleNamespace(place_order=self._place, cancel_order=self._cancel,
                                     get_order_results=self._get)
        self.accounting = SimpleNamespace(inventories=lambda account: _res(True, []))

    # re_login 用得到的最小介面
    def login(self, *a):
        return _res(True, [SimpleNamespace(account="A1", branch_no="B1")])

    def logout(self):
        pass

    def set_on_filled(self, h):
        pass

    set_on_order = set_on_order_changed = set_on_event = set_on_filled

    def _place(self, account, order):
        with self.lk:
            self.n += 1
            no = f"KT{self.n:03d}"
            fields = dict(order_no=no, seq_no=f"{self.n:011d}", stock_no=order.symbol,
                          buy_sell=order.buy_sell, quantity=order.quantity, filled_qty=0,
                          after_qty=order.quantity, status=10, user_def=order.user_def)
            self.book[no] = SimpleNamespace(**fields)
            placed = SimpleNamespace(**fields)
            if self.place_without_no:
                placed.order_no = None
            else:
                self.placed_objs[no] = placed
            return _res(True, placed)

    def _cancel(self, account, obj):
        with self.lk:
            no = obj.order_no
            from_place = obj is self.placed_objs.get(no)
            self.cancel_calls.append((no, from_place))
            gate = self.block_placed.get(no) if from_place else None
        if gate is not None:
            gate.wait(5)
        with self.lk:
            if from_place and self.panic_placed_objs:
                raise FakePanic("called `Option::unwrap()` on a `None` value")
            if from_place and self.refuse_placed_objs:
                raise TypeError("argument 'order_result': cannot convert placed OrderResult")
            if from_place and self.reject_placed_msg:
                return _res(False, None, self.reject_placed_msg)
            script = self.cancel_script.get(no)
            step = script.pop(0) if script else None
        if callable(step):
            step = step()
        if isinstance(step, BaseException):
            raise step
        if step is not None:
            return _res(False, None, step)
        with self.lk:
            srv = self.book[no]
            if srv.status == 30:
                return _res(False, None, MSG_ALREADY_CANCELLED)
            if srv.status == 50:
                return _res(False, None, MSG_FILLED)
            if srv.status == 90:
                return _res(False, None, MSG_FAILED_ORDER)
            srv.status = 30
            return _res(True, srv)

    def _get(self, account):
        with self.lk:
            self.query_calls += 1
            return _res(True, list(self.book.values()))

    # 測試小工具
    def fill_at_broker(self, no, lots=None):
        with self.lk:
            srv = self.book[no]
            srv.filled_qty = srv.quantity if lots is None else int(lots) * 1000
            if srv.filled_qty >= srv.quantity:
                srv.status = 50

    def direct(self, no=None):
        return [c for c in self.cancel_calls if c[1] and (no is None or c[0] == no)]

    def via_snapshot(self, no=None):
        return [c for c in self.cancel_calls if not c[1] and (no is None or c[0] == no)]


@pytest.fixture(autouse=True)
def fubon_stub(monkeypatch):
    """本機沒有 fubon_neo — broker 內的 import 換成替身 (Order = 欄位袋;enum 值 = 名稱字串;FubonSDK = 假富邦)。"""
    pkg = ModuleType("fubon_neo")
    sdk_mod = ModuleType("fubon_neo.sdk")
    const = ModuleType("fubon_neo.constant")
    sdk_mod.Order = lambda **kw: SimpleNamespace(**kw)
    sdk_mod.FubonSDK = lambda *a, **k: FakeFubon()
    const.BSAction = SimpleNamespace(Buy="Buy", Sell="Sell")
    const.TimeInForce = SimpleNamespace(ROD="ROD")
    const.OrderType = SimpleNamespace(Stock="Stock")
    const.PriceType = SimpleNamespace(Limit="Limit", Market="Market")
    const.MarketType = SimpleNamespace(Common="Common")
    monkeypatch.setitem(sys.modules, "fubon_neo", pkg)
    monkeypatch.setitem(sys.modules, "fubon_neo.sdk", sdk_mod)
    monkeypatch.setitem(sys.modules, "fubon_neo.constant", const)
    monkeypatch.setenv("CANCEL_DIRECT_OBJ", "1")      # 2026-10-14 起預設關;這裡的測試驗的是「啟用時」的行為
    monkeypatch.setattr(ts_mod, "_UNKNOWN_CANCEL_REJECT_SEEN", set())   # 「未知拒撤文案」去重集合每個測試重來


@pytest.fixture
def client(tmp_path):
    c = RealOrderClient(tmp_path / "orders.csv")
    c.account = SimpleNamespace(account="A1", branch_no="B1")
    c.account_no = "A1"
    # 注意: 不設 c._account_id — session 的帳號防呆會拿它比對 .env FUBON_ACCOUNT_ID (整套測試會載入 .env),
    # 設了假 ID 會被當成「別台節點的帳號」而拒絕 arm。要測 re_login 的測試自己設。
    c.connected = True
    c.healthy = True
    c.query_min_interval = 0.0            # 測試不等 5/s 時槽
    c.sdk = FakeFubon()
    yield c
    c._stopping = True                    # 測試結束不留重連 timer
    c.close()


def _cancel_rows(c) -> list:
    with open(c.log_path, encoding="utf-8", newline="") as f:
        return [r for r in csv.DictReader(f) if r["action"] == "CANCEL"]


def _crit(caplog, needle=""):
    return [r for r in caplog.records if r.levelno == logging.CRITICAL and needle in r.getMessage()]


def _prove(client):
    """先撤成一筆 → 本連線「已證實」券商收下單回傳物件。"""
    no = client.place_market_buy(SYM, 1)
    client.cancel(no, SYM)
    assert client._direct_cancel_proven
    return no


# ═══ broker: 記物件 / 直接撤 ══════════════════════════════════════════

class TestBrokerDirectCancel:
    def test_place_remembers_object_and_cancel_skips_list_query(self, client):
        sdk = client.sdk
        no = client.place_limit_buy(SYM, LU, 1)
        assert client.has_cached_order(no)
        client.cancel(no, SYM, reason="chase_sent_cancel_pre")
        assert sdk.query_calls == 0, "撤單不可以再拉全清單 (那個查詢會凍住整個 process)"
        assert sdk.cancel_calls == [(no, True)], "要用下單當下回傳的那個物件撤"
        assert sdk.book[no].status == 30
        assert not client.has_cached_order(no), "撤成 = 終結,物件丟掉"
        row = _cancel_rows(client)[-1]
        assert row["order_id"] == no and row["extra"] == "chase_sent_cancel_pre"

    def test_order_not_placed_by_this_client_still_uses_snapshot(self, client):
        sdk = client.sdk
        sdk.book["OLD1"] = SimpleNamespace(order_no="OLD1", stock_no=SYM, buy_sell="Buy", quantity=1000,
                                           filled_qty=0, after_qty=1000, status=10, user_def="hitlimit")
        assert not client.has_cached_order("OLD1")
        client.cancel("OLD1", SYM, reason="manual_cancel")
        assert sdk.query_calls == 1 and sdk.cancel_calls == [("OLD1", False)]

    def test_recovered_order_no_is_not_cached(self, client):
        """place 回傳物件缺書號 → 靠反查認領書號;那個物件不完整,不記 → 撤單照舊走快照。"""
        sdk = client.sdk
        sdk.place_without_no = True
        no = client.place_market_buy(SYM, 1)
        assert no == "KT001" and sdk.query_calls == 1          # 反查認領用了一次查詢
        assert not client.has_cached_order(no)
        client.cancel(no, SYM, reason="chase_extra")
        assert sdk.query_calls == 2 and sdk.cancel_calls == [(no, False)]

    def test_filled_reject_reports_unknown_fill_and_drops_object(self, client):
        """被拒「成交單已不允許取消」: 手上物件是下單當下的值 (filled 0) → 不可以當成交依據 → 回 None。"""
        sdk = client.sdk
        no = client.place_market_buy(SYM, 1)
        sdk.fill_at_broker(no)
        with pytest.raises(CancelRejected) as ei:
            client.cancel(no, SYM, reason="chase_extra")
        assert "成交單已不允許取消" in str(ei.value)
        assert ei.value.filled_qty is None and ei.value.status == ""
        assert sdk.query_calls == 0
        assert not client.has_cached_order(no), "終結文案 → 之後 (核實成交) 走快照"
        assert not client._direct_cancel_proven, "終結文案不算已證實: 認得這筆 ≠ 能用這個物件撤掉 live 單"
        assert _cancel_rows(client)[-1]["extra"].startswith("FAIL:")

    def test_already_cancelled_reject_drops_object(self, client):
        sdk = client.sdk
        no = client.place_limit_buy(SYM, LU, 1)
        sdk.book[no].status = 30
        with pytest.raises(CancelRejected) as ei:
            client.cancel(no, SYM)
        assert "取消單已不允許取消" in str(ei.value) and not client.has_cached_order(no)
        assert sdk.query_calls == 0

    def test_no_remaining_quantity_message_is_terminal(self, client):
        """「該筆委託已無剩餘量」(已全數成交的撤單拒絕) 不含「已不允許取消」,但同樣是終結 → 立刻丟物件。"""
        sdk = client.sdk
        no = client.place_market_buy(SYM, 1)
        sdk.cancel_script[no] = [MSG_NO_REMAINING]
        with pytest.raises(CancelRejected):
            client.cancel(no, SYM)
        assert not client.has_cached_order(no)

    def test_env_kill_switch_restores_snapshot_path(self, client, monkeypatch):
        sdk = client.sdk
        no = client.place_limit_buy(SYM, LU, 1)
        monkeypatch.setenv("CANCEL_DIRECT_OBJ", "0")
        assert not client.has_cached_order(no) and client.status()["direct_cancel"] == "env_off"
        client.cancel(no, SYM)
        assert sdk.query_calls == 1 and sdk.cancel_calls == [(no, False)]

    def test_object_from_another_day_is_ignored(self, client, monkeypatch):
        """書號隔日可能重複 — 只認今日記下的物件。"""
        no = client.place_limit_buy(SYM, LU, 1)
        monkeypatch.setattr(RealOrderClient, "_obj_day", staticmethod(lambda: "20991231"))
        assert not client.has_cached_order(no)

    def test_cancel_cached_without_object_raises_without_counting(self, client):
        with pytest.raises(RuntimeError) as ei:
            client.cancel_cached("NOPE", SYM)
        assert "下單回傳物件" in str(ei.value)
        assert client._direct_cancel_exc_streak == 0 and client.sdk.cancel_calls == []

    def test_remember_never_raises_and_cache_is_bounded(self, client, monkeypatch):
        client._remember_order_obj("", object())               # 空書號 / None 物件 → 靜默略過
        client._remember_order_obj("X1", None)
        assert not client.has_cached_order("X1")
        monkeypatch.setattr(broker_mod, "DIRECT_CANCEL_CACHE_MAX", 3)
        for i in range(5):
            client._remember_order_obj(f"N{i}", SimpleNamespace(order_no=f"N{i}"))
        assert [client.has_cached_order(f"N{i}") for i in range(5)] == [False, False, True, True, True]

    def test_full_cache_evicts_other_days_first(self, client, monkeypatch):
        monkeypatch.setattr(broker_mod, "DIRECT_CANCEL_CACHE_MAX", 3)
        client._order_objs["OLD"] = ("20200101", SimpleNamespace(order_no="OLD"))
        for i in range(3):
            client._remember_order_obj(f"N{i}", SimpleNamespace(order_no=f"N{i}"))
        assert "OLD" not in client._order_objs
        assert all(client.has_cached_order(f"N{i}") for i in range(3)), "先丟非今日的,今日的都留著"

    def test_instance_built_without_init_is_safe(self):
        """tests / 替身常用 RealOrderClient.__new__ — 類別層預設 = 沒有物件;記的時候才建 instance 專屬 dict。"""
        c = RealOrderClient.__new__(RealOrderClient)
        assert c.has_cached_order("K1") is False
        c._remember_order_obj("K1", SimpleNamespace(order_no="K1"))
        assert c.has_cached_order("K1") is True
        assert RealOrderClient._order_objs is None, "不可以寫進類別層 (會跨 instance 共用)"
        assert RealOrderClient._order_obj_inflight is None


# ═══ broker: 何時放棄物件 / 何時整個停用 ══════════════════════════════

class TestBrokerFallbackPolicy:
    def test_inflight_message_keeps_object_and_never_trips(self, client):
        """10-07 MH014「證券檢查成交回報是否已成交」= 撤單請求已受理、等交易所 (4.1 s)。不是物件問題 →
        不因次數放棄 (換快照物件結果一樣、只是多凍結);之後券商確認 → 直接撤拿到終結文案結案。"""
        sdk = client.sdk
        no = client.place_limit_buy(SYM, LU, 1)
        sdk.cancel_script[no] = [MSG_INFLIGHT] * 6
        for _ in range(6):
            with pytest.raises(CancelRejected):
                client.cancel(no, SYM, reason="chase_sent_cancel_pre")
            assert client.has_cached_order(no)
        assert sdk.query_calls == 0 and sdk.direct(no) == [(no, True)] * 6
        assert not client._direct_cancel_off and client._direct_cancel_unproven_drops == 0
        client.cancel(no, SYM, reason="chase_sent_cancel_pre")          # 第 7 次: 照樣直接撤,撤成
        assert sdk.query_calls == 0 and sdk.book[no].status == 30

    def test_inflight_message_falls_back_after_time_limit(self, client, monkeypatch, caplog):
        """在途拖太久 (撤單回報可能遺失) → 丟物件,下一次走快照讓券商 status 結案。"""
        caplog.set_level(logging.INFO, logger="broker")
        sdk = client.sdk
        no = client.place_limit_buy(SYM, LU, 1)
        sdk.cancel_script[no] = [MSG_INFLIGHT]
        with pytest.raises(CancelRejected):
            client.cancel(no, SYM)
        assert client.has_cached_order(no)
        monkeypatch.setattr(broker_mod, "DIRECT_CANCEL_INFLIGHT_MAX_SEC", 0.0)
        monkeypatch.setattr(broker_mod, "DIRECT_CANCEL_INFLIGHT_UNPROVEN_MAX_SEC", 0.0)
        client.cancel(no, SYM)                                 # 已過上限 → 這次直接走快照,不再白送一次
        assert "仍未結案" in caplog.text
        assert sdk.direct(no) == [(no, True)] and sdk.query_calls == 1
        assert sdk.via_snapshot(no) == [(no, False)] and sdk.book[no].status == 30
        assert client._direct_cancel_unproven_drops == 1, "證實前放棄一筆要記一次"

    def test_unknown_reject_before_proven_drops_at_once_and_trips_after_two(self, client, caplog):
        """前提 (券商收下單回傳物件) 若以「回應式拒絕」不成立: 每筆只白撞 1 次就退回快照,2 筆就整個停用 —
        最壞多 2 次往返,之後行為 = 修法前。(舊版: 每筆白撞 3 次、延後 0.8~1.8 s、永不停用。)"""
        caplog.set_level(logging.INFO, logger="broker")
        sdk = client.sdk
        a, b, c = (client.place_market_buy(SYM, 1) for _ in range(3))
        sdk.reject_placed_msg = MSG_UNKNOWN
        with pytest.raises(CancelRejected):
            client.cancel(a, SYM)
        assert not client.has_cached_order(a) and client.has_cached_order(b)
        assert not client._direct_cancel_off
        with pytest.raises(CancelRejected):
            client.cancel(b, SYM)
        assert client._direct_cancel_off and not client.has_cached_order(c)
        assert client.status()["direct_cancel"] == "off"
        crit = _crit(caplog, "尚無任何一筆撤成")
        assert len(crit) == 1 and "委託資料不符" in crit[0].getMessage()
        for no in (a, b, c):                                   # 之後全部走快照 — 撤單照樣做得到
            client.cancel(no, SYM)
        assert sdk.query_calls == 3 and sdk.direct() == [(a, True), (b, True)]
        assert all(sdk.book[no].status == 30 for no in (a, b, c))

    def test_unknown_reject_after_proven_gets_three_tries_per_order(self, client, caplog):
        caplog.set_level(logging.INFO, logger="broker")
        sdk = client.sdk
        _prove(client)
        no = client.place_limit_buy(SYM, LU, 1)
        sdk.cancel_script[no] = [MSG_UNKNOWN] * 3
        for i in range(3):
            with pytest.raises(CancelRejected):
                client.cancel(no, SYM)
            assert client.has_cached_order(no) is (i < 2)
        assert not client._direct_cancel_off and "被券商以未知文案拒 3 次" in caplog.text
        client.cancel(no, SYM)                                 # 第 4 次: 快照舊路徑
        assert sdk.query_calls == 1 and sdk.via_snapshot(no) == [(no, False)]

    def test_sdk_exception_drops_object_and_disables_after_two_in_a_row(self, client, caplog):
        """這版 SDK 若不收下單回傳物件 (例外,不是券商拒單) → 連續 2 次就整個停用,全部退回快照舊路徑。"""
        caplog.set_level(logging.INFO, logger="broker")
        sdk = client.sdk
        a = client.place_market_buy(SYM, 1)
        b = client.place_market_buy(SYM, 1)
        c = client.place_market_buy(SYM, 1)
        sdk.refuse_placed_objs = True
        with pytest.raises(RuntimeError):
            client.cancel(a, SYM)
        assert not client.has_cached_order(a) and client.has_cached_order(b)
        assert not client._direct_cancel_off
        with pytest.raises(RuntimeError):
            client.cancel(b, SYM)
        assert client._direct_cancel_off and not client.has_cached_order(c)
        assert len(_crit(caplog, "免查詢撤單連續")) == 1
        for no in (a, b, c):
            client.cancel(no, SYM)
        assert sdk.query_calls == 3 and sdk.via_snapshot() == [(a, False), (b, False), (c, False)]
        assert all(sdk.book[no].status == 30 for no in (a, b, c))

    def test_exception_after_proven_never_disables(self, client):
        """已有一筆撤成 = 物件格式沒問題 → 之後的例外是傳輸層暫態: 只那一筆退回快照,不停用 (一次連線抖動
        不該讓整天又回到每次撤單都凍結)。"""
        sdk = client.sdk
        _prove(client)
        nos = [client.place_market_buy(SYM, 1) for _ in range(4)]
        for no in nos[:3]:
            sdk.cancel_script[no] = [TimeoutError("read timed out")]
            with pytest.raises(RuntimeError):
                client.cancel(no, SYM)
            assert not client.has_cached_order(no)
        assert not client._direct_cancel_off and client.has_cached_order(nos[3])

    def test_reject_resets_exception_streak(self, client):
        sdk = client.sdk
        a, b, c = (client.place_market_buy(SYM, 1) for _ in range(3))
        sdk.cancel_script[a] = [TimeoutError("read timed out")]
        sdk.cancel_script[b] = [MSG_INFLIGHT]
        sdk.cancel_script[c] = [TimeoutError("read timed out")]
        with pytest.raises(RuntimeError):
            client.cancel(a, SYM)
        with pytest.raises(CancelRejected):
            client.cancel(b, SYM)                              # 券商有回應 → 連續例外計數歸零
        with pytest.raises(RuntimeError):
            client.cancel(c, SYM)
        assert client._direct_cancel_exc_streak == 1 and not client._direct_cancel_off

    def test_not_ready_keeps_object_and_does_not_count(self, client):
        sdk = client.sdk
        no = client.place_limit_buy(SYM, LU, 1)
        client.healthy = False
        with pytest.raises(RuntimeError):
            client.cancel(no, SYM)
        client.healthy = True
        assert client.has_cached_order(no) and client._direct_cancel_exc_streak == 0
        assert sdk.cancel_calls == []
        client.cancel(no, SYM)
        assert sdk.query_calls == 0 and sdk.cancel_calls == [(no, True)]

    def test_midflight_disconnect_exception_keeps_object_and_streak(self, client):
        """SDK 呼叫途中交易 WS 斷線 → 例外與物件無關: 不丟、不計次。"""
        sdk = client.sdk
        no = client.place_limit_buy(SYM, LU, 1)

        def _drop_then_fail():
            client.healthy = False
            return ConnectionError("socket closed")
        sdk.cancel_script[no] = [_drop_then_fail]
        with pytest.raises(RuntimeError):
            client.cancel(no, SYM)
        client.healthy = True
        assert client.has_cached_order(no) and client._direct_cancel_exc_streak == 0
        assert not client._direct_cancel_off

    def test_exception_after_sdk_swap_is_ignored(self, client):
        """重連已換新 SDK、舊 SDK 上卡住的呼叫才丟例外 → 不丟物件、不計次 (re_login 會把 healthy 設回 True,
        只看 healthy 擋不住)。"""
        sdk = client.sdk
        no = client.place_limit_buy(SYM, LU, 1)
        new_sdk = FakeFubon()

        def _swap_then_fail():
            client.sdk = new_sdk
            return ConnectionError("old sdk logged out")
        sdk.cancel_script[no] = [_swap_then_fail]
        with pytest.raises(RuntimeError):
            client.cancel(no, SYM)
        assert client._direct_cancel_exc_streak == 0 and not client._direct_cancel_off
        assert client.has_cached_order(no)

    def test_relogin_reenables_direct_cancel(self, client, caplog):
        """停用後交易 WS 自動重連成功 → 重新啟用、計數歸零 (新連線重新驗證);重連後下的單走免查詢。"""
        caplog.set_level(logging.INFO, logger="broker")
        sdk = client.sdk
        client._account_id, client._password, client._pfx_path, client._pfx_password = "X123", "pw", "x.pfx", ""
        a, b = client.place_market_buy(SYM, 1), client.place_market_buy(SYM, 1)
        sdk.refuse_placed_objs = True
        for no in (a, b):
            with pytest.raises(RuntimeError):
                client.cancel(no, SYM)
        assert client._direct_cancel_off
        client.re_login()
        assert client.sdk is not sdk and client.healthy
        assert not client._direct_cancel_off and client._direct_cancel_exc_streak == 0
        assert client._direct_cancel_unproven_drops == 0 and "免查詢撤單重新啟用" in caplog.text
        no = client.place_market_buy(SYM, 1)
        assert client.has_cached_order(no)
        client.cancel(no, SYM)
        assert client.sdk.query_calls == 0 and client.sdk.cancel_calls == [(no, True)]

    def test_hung_direct_cancel_is_not_resent_and_falls_to_snapshot(self, client, monkeypatch):
        """免查詢撤單的 SDK 呼叫**卡死** (往返超過門檻) 時: 這筆不再給免查詢 (has_cached_order False)、
        cancel_cached 不重送;facade 改走快照 (= 修法前對卡住撤單的行為)。卡住的呼叫回來後不影響結果。"""
        monkeypatch.setattr(broker_mod, "DIRECT_CANCEL_ROUNDTRIP_MAX_SEC", 0.0)     # 一在途就算卡死
        sdk = client.sdk
        no = client.place_market_buy(SYM, 1)
        gate = threading.Event()
        sdk.block_placed[no] = gate
        errs = []

        def _bg():
            try:
                client.cancel_cached(no, SYM, "chase_extra")
            except Exception as e:                # noqa: BLE001
                errs.append(e)
        t = threading.Thread(target=_bg, daemon=True)
        t.start()
        for _ in range(200):
            if sdk.direct(no):
                break
            time.sleep(0.005)
        assert sdk.direct(no) == [(no, True)]
        assert not client.has_cached_order(no), "在途中不可再走免查詢"
        with pytest.raises(RuntimeError):
            client.cancel_cached(no, SYM, "chase_extra")
        assert sdk.direct(no) == [(no, True)], "同一個物件不可重送"
        client.cancel(no, SYM, "chase_extra")                  # facade → 快照路徑
        assert sdk.query_calls == 1 and sdk.via_snapshot(no) == [(no, False)]
        assert sdk.book[no].status == 30
        gate.set()
        t.join(3)
        assert not t.is_alive() and len(errs) == 1             # 卡住那筆回來: 「取消單已不允許取消」
        assert "取消單已不允許取消" in str(errs[0]) and not client._direct_cancel_off


# ═══ session + 真 client: 熱路徑不再拉全清單 ══════════════════════════════

@pytest.fixture
def clock(monkeypatch):
    return install_fake_clock(monkeypatch, today_at(9, 0, 12))


def _session(client, fixed_lots=1):
    return make_session(broker=client, total=10_000_000, per_symbol=0, sizing_mode="fixed_lots",
                        fixed_lots=fixed_lots)


def _open_with_extras(client, n_extra=7):
    """重演 2026-10-07 開盤: 預掛 P → 第一筆市價 M 成功 (同步撤 P) → n_extra 筆管線多送 M (入撤單佇列)。"""
    s = _session(client)
    s.place_pre_orders([SYM], {SYM: LU})
    st = s.trades[SYM]
    p = st.pre_order_no
    assert s._chase_send_one(SYM, 1) == "accepted"
    m1 = st.order_no
    extras = []
    for _ in range(n_extra):
        before = set(s.order_log)
        assert s._chase_send_one(SYM, 1) == "accepted_extra"
        extras.append((set(s.order_log) - before).pop())
    return s, p, m1, extras


def _orphan(client, s, no="ORPH1"):
    """一筆不是本 client 下的 hitlimit 買單 (沒有下單回傳物件) — 入撤單佇列。"""
    client.sdk.book[no] = SimpleNamespace(order_no=no, stock_no=SYM, buy_sell="Buy", quantity=1000,
                                          filled_qty=0, after_qty=1000, status=10, user_def="hitlimit")
    s._log_order(no, SYM, "buy", "market_buy", 1, 0)
    s.request_cancel(no, SYM, "chase_extra")
    return no


class TestSessionHotPath:
    def test_incident_1007_open_cancels_without_any_list_query(self, client, clock):
        """10-07 node1: 第一筆市價成功後撤 P 就拉了一次全清單 (1525 筆、凍 1.4 s),撤多送 M 又一次 (2237 筆、
        凍 1.6 s) → 4716 開盤 tick 晚 1.9 s 才處理、送單前查庫存被壓 1.6 s。修法後整段零查詢。"""
        sdk = client.sdk
        s, p, m1, extras = _open_with_extras(client)
        assert s.order_log[p]["status"] == "cancelled", "預掛 P 同步撤成功"
        assert sdk.query_calls == 0, "撤 P 不可以拉全清單"
        assert set(extras) == set(s._cancel_queue)
        out = run_once(s)
        assert sdk.query_calls == 0, "撤管線多送 M 不可以拉全清單"
        assert sorted(out["cancelled"]) == sorted(extras) and out["snapshot_ok"]
        assert all(s.order_log[no]["status"] == "cancelled" for no in extras)
        assert s.order_log[m1]["status"] == "pending" and sdk.book[m1].status == 10, "搶到的那筆不可以被撤"
        assert s._cancel_queue == {}
        assert sdk.via_snapshot() == [] and len(sdk.direct()) == 1 + len(extras)

    def test_exit_path_cancels_without_list_query(self, client, clock):
        """出場 worker 送賣單前的三個撤單動作 (孤兒 P / 管線多送 M / st.order_no) 都不再拉全清單。"""
        sdk = client.sdk
        s = _session(client)
        s.place_pre_orders([SYM], {SYM: LU})
        st = s.trades[SYM]
        p = st.pre_order_no
        strays = []
        for _ in range(3):                                   # 只在 order_log、不在 st 欄位的市價買
            no = client.place_market_buy(SYM, 1)
            s._log_order(no, SYM, "buy", "market_buy", 1, 0)
            strays.append(no)
        assert s._cancel_stray_buys(SYM, "mkt_queue_gone") == 3
        s.cancel_symbol_orders(SYM, "mkt_queue_gone")
        assert sdk.query_calls == 0
        assert all(s.order_log[no]["status"] == "cancelled" for no in strays + [p])
        assert sdk.via_snapshot() == []

    def test_direct_cancels_go_through_the_send_rate_limiter(self, client, clock):
        """免查詢撤單也要過 45/s 共用送單額度 (與進場/出場共用)。"""
        s, p, m1, extras = _open_with_extras(client, n_extra=5)
        calls = []
        orig = s._rate.acquire
        s._rate.acquire = lambda *a, **k: (calls.append(1), orig(*a, **k))[1]
        run_once(s)
        assert len(calls) == len(extras)

    def test_only_orders_without_object_trigger_one_snapshot(self, client, clock):
        """有物件的直接撤、沒物件的 (例: 重連前/別的程序下的 hitlimit 孤兒) 才查清單 — 一輪最多一次。"""
        sdk = client.sdk
        s, p, m1, extras = _open_with_extras(client, n_extra=2)
        _orphan(client, s)
        out = run_once(s)
        assert sdk.query_calls == 1
        assert sorted(out["cancelled"]) == sorted(extras + ["ORPH1"])
        assert sdk.via_snapshot() == [("ORPH1", False)]
        assert sorted(c[0] for c in sdk.direct()) == sorted(extras + [p])

    def test_env_kill_switch_uses_old_path_end_to_end(self, client, clock, monkeypatch):
        monkeypatch.setenv("CANCEL_DIRECT_OBJ", "0")
        sdk = client.sdk
        s, p, m1, extras = _open_with_extras(client, n_extra=2)
        run_once(s)
        assert sdk.query_calls >= 2 and sdk.direct() == []
        assert all(s.order_log[no]["status"] == "cancelled" for no in extras + [p])

    def test_snapshot_failure_does_not_block_direct_cancels(self, client, clock):
        """快照查詢失敗 (流量控管) 只影響沒物件的那筆;有物件的照樣撤掉。"""
        sdk = client.sdk
        s, p, m1, extras = _open_with_extras(client, n_extra=2)
        _orphan(client, s)
        sdk.stock.get_order_results = lambda account: _res(False, [], "Login Error, 業務系統流量控管")
        out = run_once(s)
        assert sorted(out["cancelled"]) == sorted(extras) and out["snapshot_ok"] is False
        assert out["pending"] == ["ORPH1"] and s.order_log["ORPH1"]["status"] == "pending"

    def test_snapshot_failure_keeps_direct_leftovers_in_pending(self, client, clock):
        """混合輪次且快照失敗: 免查詢那段留佇列的書號不可以從回傳的 pending 消失。"""
        sdk = client.sdk
        s, p, m1, extras = _open_with_extras(client, n_extra=2)
        x = extras[0]
        sdk.cancel_script[x] = [MSG_INFLIGHT]
        _orphan(client, s)
        sdk.stock.get_order_results = lambda account: _res(False, [], "Login Error, 業務系統流量控管")
        out = run_once(s)
        assert sorted(out["pending"]) == sorted([x, "ORPH1"]) and out["cancelled"] == [extras[1]]
        assert set(s._cancel_queue) == {x, "ORPH1"}


class TestSessionFilledBeforeCancel:
    def test_fill_report_first_settles_without_query_or_cancel(self, client, clock):
        """多送的 M 在撤之前成交、成交回報已到 (本地 row filled) → 不送撤單、不查清單,直接以「撤單前已成交」結案。"""
        sdk = client.sdk
        s, p, m1, extras = _open_with_extras(client, n_extra=2)
        x = extras[0]
        sdk.fill_at_broker(x)
        s._on_fill(fill(x, SYM, 1, LU, filled_no="F-x"))
        assert s.order_log[x]["status"] == "filled" and x in s._cancel_queue
        n_before = len(sdk.cancel_calls)
        out = run_once(s)
        assert x in out["filled_before_cancel"] and x not in s._cancel_queue
        assert x not in [c[0] for c in sdk.cancel_calls[n_before:]], "本地已知成交的單不必再送撤單"
        assert sdk.query_calls == 0
        assert s.order_log[x]["status"] == "filled" and s.order_log[extras[1]]["status"] == "cancelled"
        assert s.trades[SYM].filled_lots == 1

    def test_filled_reject_then_fill_report_settles_without_query(self, client, clock):
        """撤單時成交回報還沒到 → 券商回「成交單已不允許取消」→ 留佇列;0.3 s 內成交回報到了 (row filled) →
        下一輪直接結案,**不拉全清單** (券商已親口說成交、成交量也由回報補齊,那次查詢是多餘的凍結)。"""
        sdk = client.sdk
        s, p, m1, extras = _open_with_extras(client, n_extra=2)
        x = extras[0]
        sdk.fill_at_broker(x)
        out = run_once(s)
        assert x in out["pending"] and s._cancel_queue[x].get("fbc_unverified") is True
        assert s.order_log[x]["status"] == "pending" and sdk.query_calls == 0
        s._on_fill(fill(x, SYM, 1, LU, filled_no="F-x"))
        out = run_once(s, now=clock.time() + 1)
        assert sdk.query_calls == 0, "成交回報已到就不該再查清單"
        assert out["filled_before_cancel"] == [x] and s._cancel_queue == {}
        assert s.order_log[x]["status"] == "filled" and s.trades[SYM].filled_lots == 1
        assert s._cancel_stats["filled_before_cancel"] == 1

    def test_sync_pre_order_cancel_filled_then_report_no_query(self, client, clock):
        """10-07 型: 預掛 P 在開盤集合競價成交、回報還沒處理完,第一筆市價成功後同步撤 P 被拒「成交單」→
        成交回報隨後到 → 不拉全清單。"""
        sdk = client.sdk
        s = _session(client)
        s.place_pre_orders([SYM], {SYM: LU})
        p = s.trades[SYM].pre_order_no
        sdk.fill_at_broker(p)
        assert s._chase_send_one(SYM, 1) == "accepted"
        assert s.order_log[p]["status"] == "pending" and s._cancel_queue[p].get("fbc_unverified")
        s._on_fill(fill(p, SYM, 1, LU, filled_no="F-p"))
        out = run_once(s, now=clock.time() + 1)
        assert sdk.query_calls == 0 and out["filled_before_cancel"] == [p] and s._cancel_queue == {}

    def test_cancel_fail_report_before_fill_report_still_no_query(self, client, clock):
        """回報順序相反 (ft=30 status 39 撤單失敗回報先於成交回報) 也一樣: 成交回報到了就不查清單。"""
        sdk = client.sdk
        s, p, m1, extras = _open_with_extras(client, n_extra=1)
        x = extras[0]
        sdk.fill_at_broker(x)
        run_once(s)
        s._on_order({"order_no": x, "symbol": SYM, "status": "39", "filled_qty": 0,
                     "error_message": MSG_FILLED, "function_type": 30, "last_time": "09:00:12.500"})
        assert x in s._cancel_queue and s.order_log[x]["status"] == "pending"
        s._on_fill(fill(x, SYM, 1, LU, filled_no="F-x"))
        out = run_once(s, now=clock.time() + 5)
        assert sdk.query_calls == 0 and x in out["filled_before_cancel"] and s._cancel_queue == {}
        assert s._cancel_stats["filled_before_cancel"] == 1

    def test_filled_before_cancel_without_report_is_verified_by_snapshot(self, client, clock):
        """多送的 M 已成交但成交回報一直沒到: 直接撤 → 券商回「成交單已不允許取消」→ **不標 cancelled、不當 0 成交** →
        留佇列;下一輪走快照,用券商 filled_qty 補成交結案 (唯一需要查清單的情況)。"""
        sdk = client.sdk
        s, p, m1, extras = _open_with_extras(client, n_extra=2)
        x = extras[0]
        sdk.fill_at_broker(x)
        out = run_once(s)
        assert x in out["pending"] and s.order_log[x]["status"] == "pending"
        assert sdk.query_calls == 0 and not client.has_cached_order(x)
        assert s.trades[SYM].filled_lots == 0
        out = run_once(s, now=clock.time() + 5)
        assert sdk.query_calls == 1
        assert x in out["filled_before_cancel"] and s.order_log[x]["status"] == "filled"
        assert s.trades[SYM].filled_lots == 1, "用快照的券商成交量補成交"
        assert s._cancel_queue == {}

    def test_locally_filled_row_without_broker_word_still_goes_to_snapshot(self, client, clock):
        """沒有券商終結文案背書的書號 (這裡: 沒物件的孤兒),就算本地 row 是 filled 也要走快照以券商 status 為準 —
        本地可能是雙算。預篩只認 fbc_unverified。"""
        sdk = client.sdk
        s = _session(client)
        s.place_pre_orders([SYM], {SYM: LU})
        _orphan(client, s)
        with s._lock:
            s.order_log["ORPH1"]["status"] = "filled"          # 本地誤以為已成交,券商仍 live (status 10)
        out = run_once(s)
        assert sdk.query_calls == 1 and sdk.via_snapshot("ORPH1") == [("ORPH1", False)]
        assert sdk.book["ORPH1"].status == 30 and "ORPH1" in out["cancelled"]

    def test_double_counted_filled_row_is_still_cancelled_at_broker(self, client, clock):
        """重連補收 + 晚到回報把本地雙算成 filled 2/2,券商其實只成交 1 張、剩 1 張還 live。
        這列帶 auth_filled → 免查詢路徑不可憑本地 filled 就不送撤單 (舊路徑以券商 status 為準會撤)。"""
        sdk = client.sdk
        s = _session(client, fixed_lots=2)
        s.place_pre_orders([SYM], {SYM: LU})
        p = s.trades[SYM].pre_order_no
        sdk.fill_at_broker(p, lots=1)                          # 券商: 2 張成交 1 張,status 仍 10
        s.request_cancel(p, SYM, "unmarked")
        assert s._apply_auth_fill(p, 1, "重連補收") == 1
        s._on_fill(fill(p, SYM, 1, LU, filled_no="F-late"))    # 同一張的回報晚到 → 本地 2/2
        assert s.order_log[p]["status"] == "filled" and s.order_log[p].get("auth_filled") is True
        out = run_once(s)
        assert sdk.direct(p) == [(p, True)] and sdk.book[p].status == 30, "剩餘那張必須真的撤掉"
        assert sdk.query_calls == 0 and p in out["cancelled"] and s._cancel_queue == {}

    def test_locally_cancelled_or_rejected_row_is_dequeued_without_send(self, client, clock):
        """入佇列後回報先到、本地已終結 (cancelled / rejected) → 不送撤單、不查清單,出佇列。"""
        sdk = client.sdk
        s, p, m1, extras = _open_with_extras(client, n_extra=2)
        a, b = extras
        with s._lock:
            s.order_log[a]["status"] = "rejected"
            s.order_log[b]["status"] = "cancelled"
        n_before = len(sdk.cancel_calls)
        out = run_once(s)
        assert sdk.cancel_calls[n_before:] == [] and sdk.query_calls == 0
        assert s._cancel_queue == {} and out["cancelled"] == []


class TestSessionFallback:
    def test_inflight_reject_keeps_retrying_directly_until_broker_confirms(self, client, clock):
        """10-07 node1 MH014: 撤 P 回「證券檢查成交回報是否已成交」(請求已受理、交易所 4.1 s 後才確認)。
        舊版每次重試都先拉全清單 (2237 筆、凍 1.6 s);現在直接再試,全程零查詢、row 維持 pending 直到券商確認。"""
        sdk = client.sdk
        s = _session(client)
        s.place_pre_orders([SYM], {SYM: LU})
        p = s.trades[SYM].pre_order_no
        sdk.cancel_script[p] = [MSG_INFLIGHT] * 4
        assert s._chase_send_one(SYM, 1) == "accepted"         # 同步撤 P: 第 1 次被拒 → 入佇列
        assert s.order_log[p]["status"] == "pending" and p in s._cancel_queue
        for _ in range(3):
            out = run_once(s, now=clock.time() + 10)
            assert out["pending"] == [p] and s.order_log[p]["status"] == "pending"
            clock.advance(10)
        out = run_once(s, now=clock.time() + 10)               # 券商那頭結束 → 直接撤成
        assert out["cancelled"] == [p] and s.order_log[p]["status"] == "cancelled"
        assert sdk.query_calls == 0 and sdk.direct(p) == [(p, True)] * 5 and sdk.via_snapshot() == []

    def test_inflight_reject_resolved_by_cancel_report(self, client, clock):
        """在途期間券商的撤單確認回報 (ft=30 status 30) 到了 → 結案出佇列,不必再撤、不查清單。"""
        sdk = client.sdk
        s = _session(client)
        s.place_pre_orders([SYM], {SYM: LU})
        p = s.trades[SYM].pre_order_no
        sdk.cancel_script[p] = [MSG_INFLIGHT] * 9
        assert s._chase_send_one(SYM, 1) == "accepted"
        run_once(s, now=clock.time() + 1)
        s._on_order({"order_no": p, "symbol": SYM, "status": "30", "filled_qty": 0, "error_message": "",
                     "function_type": 30, "last_time": "09:00:19.250"})
        assert s.order_log[p]["status"] == "cancelled" and s._cancel_queue == {}
        assert sdk.query_calls == 0

    def test_inflight_reject_falls_back_to_snapshot_after_time_limit(self, client, clock, monkeypatch):
        sdk = client.sdk
        monkeypatch.setattr(broker_mod, "DIRECT_CANCEL_INFLIGHT_MAX_SEC", 0.0)
        monkeypatch.setattr(broker_mod, "DIRECT_CANCEL_INFLIGHT_UNPROVEN_MAX_SEC", 0.0)
        s = _session(client)
        s.place_pre_orders([SYM], {SYM: LU})
        p = s.trades[SYM].pre_order_no
        sdk.cancel_script[p] = [MSG_INFLIGHT]
        assert s._chase_send_one(SYM, 1) == "accepted"
        assert not client.has_cached_order(p)
        out = run_once(s, now=clock.time() + 1)
        assert sdk.query_calls == 1 and out["cancelled"] == [p] and sdk.via_snapshot(p) == [(p, False)]

    def test_response_style_refusal_trips_fuse_and_everything_still_cancels(self, client, clock, caplog):
        """未實測前提的另一種失敗形態: 後檯以 is_success=False + 沒見過的文案拒收下單回傳物件 (不是例外)。
        每筆只白撞一次、2 筆就停用,之後全部走快照;所有該撤的單最終都由券商確認撤掉,且之後的新單不再嘗試。"""
        caplog.set_level(logging.INFO)
        sdk = client.sdk
        sdk.reject_placed_msg = MSG_UNKNOWN
        s, p, m1, extras = _open_with_extras(client, n_extra=4)
        assert s.order_log[p]["status"] == "pending" and set(s._cancel_queue) == set(extras) | {p}
        for _ in range(6):
            run_once(s, now=clock.time() + 10)
            clock.advance(10)
            if not s._cancel_queue:
                break
        assert client._direct_cancel_off and len(_crit(caplog, "尚無任何一筆撤成")) == 1
        assert s._cancel_queue == {}
        assert all(s.order_log[no]["status"] == "cancelled" for no in extras + [p])
        assert all(sdk.book[no].status == 30 for no in extras + [p])
        assert sdk.book[m1].status == 10 and s.order_log[m1]["status"] == "pending"
        assert max(len(sdk.direct(no)) for no in extras + [p]) == 1, "每筆最多白撞一次"
        assert len(sdk.via_snapshot()) == len(extras) + 1
        n = len(sdk.direct())
        late = client.place_market_buy(SYM, 1)                 # 停用後的新單: 不再嘗試免查詢
        s._log_order(late, SYM, "buy", "market_buy", 1, 0)
        s.request_cancel(late, SYM, "chase_extra")
        run_once(s, now=clock.time() + 10)
        assert len(sdk.direct()) == n and s.order_log[late]["status"] == "cancelled"

    def test_sdk_refusing_placed_objects_degrades_to_old_behaviour(self, client, clock, caplog):
        """最壞情況: 這版 SDK 根本不收下單回傳物件 (每次都例外)。系統不可以卡住、不可以誤標 —
        2 次例外後自動停用,之後全部走快照;所有該撤的單最終都由券商確認撤掉。"""
        caplog.set_level(logging.INFO)
        sdk = client.sdk
        sdk.refuse_placed_objs = True
        s, p, m1, extras = _open_with_extras(client, n_extra=4)
        assert s.order_log[p]["status"] == "pending", "撤 P 例外 → 不可標 cancelled,入佇列"
        assert set(s._cancel_queue) == set(extras) | {p}
        for _ in range(6):
            run_once(s, now=clock.time() + 10)
            clock.advance(10)
            if not s._cancel_queue:
                break
        assert client._direct_cancel_off and len(_crit(caplog, "免查詢撤單連續")) == 1
        assert s._cancel_queue == {}
        assert all(s.order_log[no]["status"] == "cancelled" for no in extras + [p])
        assert all(sdk.book[no].status == 30 for no in extras + [p])
        assert sdk.book[m1].status == 10 and s.order_log[m1]["status"] == "pending"
        assert sdk.query_calls >= 1 and len(sdk.via_snapshot()) == len(extras) + 1

    def test_hung_direct_cancel_times_out_then_snapshot_path_takes_over(self, client, clock, monkeypatch):
        """免查詢撤單 SDK 呼叫卡住: 該輪逾時 = 未確認 (不標 cancelled、不計 attempts);下一輪這筆不再走免查詢
        (不拿同一個物件重送、不堆 thread),改由快照路徑處理 — 與修法前對卡住撤單的行為相同。"""
        monkeypatch.setattr(ts_mod, "_CANCEL_SDK_TIMEOUT_SEC", 0.15)
        monkeypatch.setattr(broker_mod, "DIRECT_CANCEL_ROUNDTRIP_MAX_SEC", 0.1)      # 扇出逾時 (0.15 s) 後已算卡死
        sdk = client.sdk
        s, p, m1, extras = _open_with_extras(client, n_extra=2)
        x = extras[0]
        gate = threading.Event()
        sdk.block_placed[x] = gate
        try:
            out = run_once(s)
            assert out["pending"] == [x] and out["cancelled"] == [extras[1]]
            row = s.order_log[x]
            assert row["status"] == "pending" and row["cancel_state"] == "unconfirmed"
            assert row["cancel_attempts"] == 0 and sdk.query_calls == 0
            assert not client.has_cached_order(x)
            out = run_once(s, now=clock.time() + 5)
            assert sdk.direct(x) == [(x, True)], "卡住期間不可再用同一個物件送"
            assert sdk.query_calls == 1 and out["cancelled"] == [x] and sdk.via_snapshot(x) == [(x, False)]
            assert s.order_log[x]["status"] == "cancelled" and s._cancel_queue == {}
        finally:
            gate.set()

    def test_row_shows_sent_while_direct_cancel_is_in_flight(self, client, clock):
        """免查詢撤單送出期間 row.cancel_state = sent (前端「撤單中」顯示用),被拒後 = unconfirmed。"""
        sdk = client.sdk
        s, p, m1, extras = _open_with_extras(client, n_extra=1)
        x = extras[0]
        seen = []

        def _peek():
            seen.append(s.order_log[x]["cancel_state"])
            return MSG_INFLIGHT
        sdk.cancel_script[x] = [_peek]
        run_once(s)
        assert seen == ["sent"] and s.order_log[x]["cancel_state"] == "unconfirmed"

    def test_all_direct_round_still_runs_giveup_check(self, client, monkeypatch):
        """整輪都走免查詢 (沒拉快照) 也要跑放棄檢查: trading_end+10 分後仍撤不到 → 出佇列、row 留 pending、
        CRITICAL 點名 (不可以因為沒走快照就永遠留在佇列裡重試)。"""
        clock = install_fake_clock(monkeypatch, today_at(13, 40))
        sdk = client.sdk
        s = _session(client)
        s.trading_end_time = dtime(13, 24)                     # 不依賴環境的 TRADING_END_TIME
        x = client.place_market_buy(SYM, 1)
        s._log_order(x, SYM, "buy", "market_buy", 1, 0)
        s.request_cancel(x, SYM, "trading_end")
        sdk.cancel_script[x] = [MSG_INFLIGHT] * 3
        out = run_once(s, now=clock.time())
        assert out["pending"] == [x] and sdk.query_calls == 0
        assert s._cancel_queue == {}, "13:34 之後試過仍未確認 → 放棄"
        row = s.order_log[x]
        assert row["status"] == "pending" and str(row["cancel_err"]).startswith("GIVE_UP")
        assert s._cancel_stats["given_up"] == 1

    def test_retry_backoff_counts_from_now_not_round_start(self, client, clock):
        """退避從「現在」起算: 一輪含一次全清單查詢 (1~2 s) 時,從該輪開始時刻起算的 0.3 s 早已過期 = 零退避、
        連續拉全清單 (10-07 node1 第二次查詢緊接著第一輪)。"""
        s, p, m1, extras = _open_with_extras(client, n_extra=1)
        x = extras[0]
        s._cancel_retry_later(x, clock.time() - 5.0, "NOT_IN_SNAPSHOT")
        assert s._cancel_queue[x]["next_ts"] >= clock.time() + 0.29


class TestSessionBrokerTerminalWithoutReport:
    """免查詢路徑沒有快照 status 可預分類 — 券商端已終結、本地回報還沒到的單,直接撤會拿到什麼、怎麼結案。"""

    def test_already_cancelled_at_broker_is_confirmed_without_query(self, client, clock):
        sdk = client.sdk
        s, p, m1, extras = _open_with_extras(client, n_extra=1)
        x = extras[0]
        sdk.book[x].status = 30
        out = run_once(s)
        assert out["cancelled"] == [x] and sdk.query_calls == 0 and s.order_log[x]["status"] == "cancelled"

    def test_fully_filled_message_without_known_keyword_goes_to_snapshot(self, client, clock):
        """「已無剩餘量」不在 session 的兩組已知關鍵字內 (分類 = retry),broker 視為終結丟物件 → 下一輪快照以
        status 50 補成交結案 — 認列時間與修法前相同 (不是白撞 3 次後才查)。"""
        sdk = client.sdk
        s, p, m1, extras = _open_with_extras(client, n_extra=1)
        x = extras[0]
        sdk.fill_at_broker(x)
        sdk.cancel_script[x] = [MSG_NO_REMAINING]
        out = run_once(s)
        assert out["pending"] == [x] and not client.has_cached_order(x) and sdk.query_calls == 0
        out = run_once(s, now=clock.time() + 1)
        assert sdk.query_calls == 1 and out["filled_before_cancel"] == [x]
        assert s.order_log[x]["status"] == "filled" and s.trades[SYM].filled_lots == 1

    def test_failed_orders_at_broker_settle_as_rejected_with_one_critical(self, client, clock, caplog):
        """券商端 status 90 (失敗單)、本地還 pending: 直接撤被拒 (含「已不允許取消」的未知文案) → 下一輪快照
        結案為 rejected。「撤單被拒文案未知」同一種文案不論幾個書號只 CRITICAL 一次。"""
        caplog.set_level(logging.INFO)
        sdk = client.sdk
        s, p, m1, extras = _open_with_extras(client, n_extra=3)
        for x in extras:
            sdk.book[x].status = 90
        out = run_once(s)
        assert sorted(out["pending"]) == sorted(extras) and sdk.query_calls == 0
        assert len(_crit(caplog, "撤單被拒文案未知")) == 1
        out = run_once(s, now=clock.time() + 1)
        assert sdk.query_calls == 1 and sorted(out["rejected"]) == sorted(extras)
        assert all(s.order_log[x]["status"] == "rejected" for x in extras) and s._cancel_queue == {}


def test_unknown_cancel_text_is_deduped_by_broker_text_not_order_no(caplog):
    """broker 例外字串是「撤單失敗 <書號>: <原文>」— 去重 key 要剝掉前綴,否則每個書號各 CRITICAL 一次。"""
    caplog.set_level(logging.CRITICAL)
    for no in ("KT001", "KT002", "MH9zz"):
        assert classify_cancel_error(f"撤單失敗 {no}: {MSG_FAILED_ORDER}") == "retry"
    assert len(_crit(caplog, "撤單被拒文案未知")) == 1


# ═══ 第二輪審查 (2026-10-12) 補強 ══════════════════════════════════════

class TestBrokerPanicAndInFlight:
    def test_basexception_from_sdk_is_converted_to_runtime_error(self, client):
        """PyO3 的 PanicException 繼承 BaseException — 不轉的話撤單 thread 會無聲死掉 (被當成逾時每秒重送、
        不計 attempts、永不退回快照;同步路徑上出場 worker 會死在送賣單之前)。"""
        sdk = client.sdk
        no = client.place_market_buy(SYM, 1)
        sdk.panic_placed_objs = True
        with pytest.raises(RuntimeError) as ei:
            client.cancel(no, SYM, reason="chase_extra")
        assert not isinstance(ei.value, CancelRejected) and "FakePanic" in str(ei.value)
        assert not client.has_cached_order(no) and client._direct_cancel_exc_streak == 1
        assert _cancel_rows(client)[-1]["extra"].startswith("FAIL:例外: FakePanic")

    def test_keyboard_interrupt_is_not_swallowed(self, client):
        sdk = client.sdk
        no = client.place_market_buy(SYM, 1)
        sdk.cancel_script[no] = [KeyboardInterrupt()]
        with pytest.raises(KeyboardInterrupt):
            client.cancel(no, SYM)

    def test_second_cancel_while_round_trip_in_progress_neither_resends_nor_queries(self, client):
        """第一筆免查詢撤單還在正常往返中 (10-07 開盤 0.3~2.3 s),別的 thread 又來撤同一筆 → CancelInFlight:
        不重送、**不查清單**。(舊版: 第二個呼叫一律拉全清單再重送一次 — 出場賣單前又凍一次。)"""
        sdk = client.sdk
        no = client.place_limit_buy(SYM, LU, 1)
        gate = threading.Event()
        sdk.block_placed[no] = gate
        t = threading.Thread(target=lambda: client.cancel(no, SYM, "chase_sent_cancel_pre"), daemon=True)
        t.start()
        for _ in range(200):
            if sdk.direct(no):
                break
            time.sleep(0.005)
        assert client.direct_cancel_in_flight(no) and not client.has_cached_order(no)
        with pytest.raises(CancelInFlight):
            client.cancel(no, SYM, "mkt_queue_gone")
        with pytest.raises(CancelInFlight):
            client.cancel_cached(no, SYM, "mkt_queue_gone")
        assert sdk.query_calls == 0 and sdk.cancel_calls == [(no, True)]
        gate.set()
        t.join(3)
        assert sdk.book[no].status == 30 and not client.direct_cancel_in_flight(no)
        assert sdk.query_calls == 0 and sdk.cancel_calls == [(no, True)]

    def test_in_flight_marker_is_cleared_after_failure_too(self, client):
        sdk = client.sdk
        no = client.place_market_buy(SYM, 1)
        sdk.cancel_script[no] = [MSG_INFLIGHT]
        with pytest.raises(CancelRejected):
            client.cancel(no, SYM)
        assert not client.direct_cancel_in_flight(no) and client._order_obj_inflight == {}
        assert client.has_cached_order(no)

    def test_late_success_reenables_after_fuse_tripped_in_same_batch(self, client, caplog):
        """同一批併發: 2 筆未知拒因先回 → 停用;稍後同批的另一筆**撤成** → 前提其實成立 → 重新啟用
        (否則事實相同、只因回應先後不同就整天退回每次撤單都凍結)。"""
        caplog.set_level(logging.INFO, logger="broker")
        sdk = client.sdk
        a, b, c = (client.place_market_buy(SYM, 1) for _ in range(3))
        sdk.cancel_script[a] = [MSG_UNKNOWN]
        sdk.cancel_script[b] = [MSG_UNKNOWN]
        gate = threading.Event()
        sdk.block_placed[c] = gate
        t = threading.Thread(target=lambda: client.cancel_cached(c, SYM, "chase_extra"), daemon=True)
        t.start()
        for _ in range(200):
            if sdk.direct(c):
                break
            time.sleep(0.005)
        for no in (a, b):
            with pytest.raises(CancelRejected):
                client.cancel_cached(no, SYM, "chase_extra")
        assert client._direct_cancel_off
        gate.set()
        t.join(3)
        assert client._direct_cancel_proven and not client._direct_cancel_off
        assert "重新啟用" in caplog.text and client.status()["direct_cancel"] == "on"
        d = client.place_market_buy(SYM, 1)
        assert client.has_cached_order(d)

    def test_fuse_does_not_trip_once_proven_even_if_failure_was_counted_before(self, client):
        """證實之後才處理到的失敗不可以再把它停用 (停用判斷在鎖內重讀 proven)。"""
        sdk = client.sdk
        a, b = client.place_market_buy(SYM, 1), client.place_market_buy(SYM, 1)
        sdk.cancel_script[a] = [MSG_UNKNOWN]
        with pytest.raises(CancelRejected):
            client.cancel_cached(a, SYM)
        assert client._direct_cancel_unproven_drops == 1
        _prove(client)
        sdk.cancel_script[b] = [MSG_UNKNOWN]
        with pytest.raises(CancelRejected):
            client.cancel_cached(b, SYM)
        assert not client._direct_cancel_off and client.has_cached_order(b), "已證實: 未知文案給 3 次、不停用"

    def test_reject_from_a_swapped_out_sdk_is_ignored(self, client):
        """重連已換新 SDK 後,舊 SDK 上晚回的回應式拒絕 (非終結文案) 不可以算在新連線頭上。"""
        sdk = client.sdk
        no = client.place_market_buy(SYM, 1)
        new_sdk = FakeFubon()

        def _swap_then_reject():
            client.sdk = new_sdk
            return MSG_UNKNOWN
        sdk.cancel_script[no] = [_swap_then_reject]
        with pytest.raises(CancelRejected):
            client.cancel_cached(no, SYM)
        assert client._direct_cancel_unproven_drops == 0 and not client._direct_cancel_off
        assert client.has_cached_order(no)

    def test_status_and_journal_marker(self, client, caplog):
        caplog.set_level(logging.INFO, logger="broker")
        st = client.status()
        assert st["direct_cancel"] == "on" and st["direct_cancel_proven"] is False
        no = client.place_limit_sell(SYM, LU, 1, "overnight_sell")     # 賣單的回傳物件也記
        assert client.has_cached_order(no)
        client.cancel(no, SYM, reason="overnight_skip")
        assert "(免查詢)" in caplog.text and client.status()["direct_cancel_proven"] is True

    @pytest.mark.parametrize("value", ["0", "false", "No", " OFF "])
    def test_env_off_values(self, client, monkeypatch, value):
        no = client.place_limit_buy(SYM, LU, 1)
        monkeypatch.setenv("CANCEL_DIRECT_OBJ", value)
        assert not client.has_cached_order(no) and client.status()["direct_cancel"] == "env_off"

    def test_default_is_off_when_env_unset(self, client, monkeypatch):
        # 2026-10-14 使用者定案: 券商文件沒示範用下單回傳物件撤單 → 未設 env = 舊路徑 (先拉全清單再撤)
        monkeypatch.delenv("CANCEL_DIRECT_OBJ", raising=False)
        no = client.place_limit_buy(SYM, LU, 1)
        assert client.status()["direct_cancel"] == "env_off"
        assert client.direct_cancel_route(no) == "snapshot"
        assert not client.has_cached_order(no)

    @pytest.mark.parametrize("value", ["1", "true", "Yes", " ON "])
    def test_env_on_values(self, client, monkeypatch, value):
        monkeypatch.setenv("CANCEL_DIRECT_OBJ", value)
        no = client.place_limit_buy(SYM, LU, 1)
        assert client.status()["direct_cancel"] == "on" and client.direct_cancel_route(no) == "direct"


class TestBrokerRejectTimeLimit:
    """在途文案的時間上限: 自**第一次**被拒起算;證實前 5 s、證實後 10 s (用可控的 monotonic,不改常數)。"""

    def _reject_at(self, client, clk, no, t):
        clk.t = 1000.0 + t
        with pytest.raises(CancelRejected):
            client.cancel_cached(no, SYM, "chase_sent_cancel_pre")

    def test_unproven_limit_is_five_seconds_from_first_reject(self, client, monkeypatch, caplog):
        caplog.set_level(logging.INFO, logger="broker")
        clk = _MonoClock()
        monkeypatch.setattr(broker_mod, "time", clk)
        sdk = client.sdk
        no = client.place_limit_buy(SYM, LU, 1)
        sdk.cancel_script[no] = [MSG_INFLIGHT] * 9
        for t in (0.0, 1.8, 3.8):
            self._reject_at(client, clk, no, t)
            assert client.has_cached_order(no)
        clk.t = 1000.0 + 4.99
        assert client.has_cached_order(no)
        clk.t = 1000.0 + 5.0
        assert not client.has_cached_order(no), "上限到了就不再給免查詢 (不必再白送一次)"
        assert client._direct_cancel_unproven_drops == 1 and "5 s 仍未結案" in caplog.text

    def test_proven_limit_is_ten_seconds_and_does_not_count_toward_fuse(self, client, monkeypatch, caplog):
        caplog.set_level(logging.INFO, logger="broker")
        clk = _MonoClock()
        monkeypatch.setattr(broker_mod, "time", clk)
        sdk = client.sdk
        _prove(client)
        no = client.place_limit_buy(SYM, LU, 1)
        sdk.cancel_script[no] = [MSG_INFLIGHT] * 9
        for t in (0.0, 3.8, 6.8, 9.9):
            self._reject_at(client, clk, no, t)
            assert client.has_cached_order(no)
        clk.t = 1000.0 + 10.0
        assert not client.has_cached_order(no)
        assert client._direct_cancel_unproven_drops == 0 and not client._direct_cancel_off

    def test_reject_arriving_after_the_limit_gives_up(self, client, monkeypatch, caplog):
        """撤單往返本身很慢: 送出時還在上限內、回來時已過 → 這次被拒當場放棄。"""
        caplog.set_level(logging.INFO, logger="broker")
        clk = _MonoClock()
        monkeypatch.setattr(broker_mod, "time", clk)
        sdk = client.sdk
        no = client.place_limit_buy(SYM, LU, 1)

        def _slow():
            clk.t += 6.0
            return MSG_INFLIGHT
        sdk.cancel_script[no] = [MSG_INFLIGHT, _slow]
        self._reject_at(client, clk, no, 0.0)
        with pytest.raises(CancelRejected):
            client.cancel_cached(no, SYM)
        assert not client.has_cached_order(no) and "撤單在途超過 5 s" in caplog.text

    def test_inflight_rejects_do_not_use_up_the_unknown_reject_budget(self, client):
        """在途文案只看時間;未知文案才數次數 — 兩者不共用計數。"""
        sdk = client.sdk
        _prove(client)
        no = client.place_limit_buy(SYM, LU, 1)
        sdk.cancel_script[no] = [MSG_INFLIGHT, MSG_INFLIGHT, MSG_INFLIGHT, MSG_UNKNOWN, MSG_UNKNOWN]
        for _ in range(5):
            with pytest.raises(CancelRejected):
                client.cancel_cached(no, SYM)
        assert client.has_cached_order(no), "未知文案才 2 次,還沒到 3 次"


class TestBrokerRelogin:
    def test_relogin_keeps_proof_and_objects_and_clears_counters(self, client):
        sdk = client.sdk
        client._account_id, client._password, client._pfx_path, client._pfx_password = "X123", "pw", "x.pfx", ""
        _prove(client)
        keep = client.place_limit_buy(SYM, LU, 1)
        client._direct_cancel_unproven_drops = 1
        client._direct_cancel_exc_streak = 1
        client.re_login()
        assert client.sdk is not sdk
        assert client._direct_cancel_proven, "已證實是 SDK 的性質,重連保留"
        assert client._direct_cancel_unproven_drops == 0 and client._direct_cancel_exc_streak == 0
        assert client.has_cached_order(keep), "同一個 client,重連前記下的物件保留"


class TestSessionRound2:
    def test_panic_in_sdk_degrades_to_snapshot_and_nothing_dies(self, client, clock, caplog):
        """SDK 對下單回傳物件 panic (BaseException): 不可以有 thread 無聲死掉。2 筆後停用、全部改走快照撤成。"""
        caplog.set_level(logging.INFO)
        sdk = client.sdk
        sdk.panic_placed_objs = True
        s, p, m1, extras = _open_with_extras(client, n_extra=3)       # 同步撤 P 也 panic — 不可以往上炸
        assert s.order_log[p]["status"] == "pending" and p in s._cancel_queue
        for _ in range(6):
            run_once(s, now=clock.time() + 10)
            clock.advance(10)
            if not s._cancel_queue:
                break
        assert client._direct_cancel_off and len(_crit(caplog, "免查詢撤單連續")) == 1
        assert s._cancel_queue == {}
        assert all(s.order_log[no]["status"] == "cancelled" for no in extras + [p])
        assert all(sdk.book[no].status == 30 for no in extras + [p])
        assert all(s.order_log[no]["cancel_attempts"] >= 1 for no in extras), "要計 attempts,不是被當成逾時"

    def test_exit_cancels_survive_sdk_panic(self, client, clock):
        """出場 worker 先標 exited 再撤單 — 撤單若往上丟 BaseException,賣單就永遠不送、部位失去出場保護。"""
        sdk = client.sdk
        s = _session(client)
        s.place_pre_orders([SYM], {SYM: LU})
        p = s.trades[SYM].pre_order_no
        sdk.panic_placed_objs = True
        assert s._cancel_orphan_pre(SYM, "mkt_queue_gone") is False      # P 還是 st.order_no,不是孤兒
        s.cancel_symbol_orders(SYM, "mkt_queue_gone")                    # 不可以 raise
        assert s.order_log[p]["status"] == "pending" and p in s._cancel_queue
        run_once(s, now=clock.time() + 1)
        assert s.order_log[p]["status"] == "cancelled" and sdk.via_snapshot(p) == [(p, False)]

    def test_exit_while_pre_order_cancel_is_in_flight_does_not_query(self, client, clock):
        """市價成功後同步撤 P 還在往返中,出場 worker 又來撤同一個 P → 不重送、不查清單 (出場賣單不被凍住);
        在途那筆回來撤成後正常結案。"""
        sdk = client.sdk
        s = _session(client)
        s.place_pre_orders([SYM], {SYM: LU})
        p = s.trades[SYM].pre_order_no
        gate = threading.Event()
        sdk.block_placed[p] = gate
        t = threading.Thread(target=lambda: s._chase_send_one(SYM, 1), daemon=True)
        t.start()
        for _ in range(400):
            if sdk.direct(p):
                break
            time.sleep(0.005)
        assert sdk.direct(p) == [(p, True)]
        # 不入佇列的呼叫法: 回 retry、什麼都不留 (沒送、沒查、不記錯誤)
        assert s._try_cancel_sync(p, SYM, "probe", enqueue=False) == "retry"
        assert p not in s._cancel_queue and sdk.query_calls == 0 and sdk.direct(p) == [(p, True)]
        assert s._cancel_orphan_pre(SYM, "mkt_queue_gone") is True       # 出場路徑: P 正在撤 → 要等成交窗口
        assert sdk.query_calls == 0 and sdk.direct(p) == [(p, True)], "不查清單、不重送"
        q = s._cancel_queue[p]
        assert q["attempts"] == 0 and q["next_ts"] > clock.time(), "入佇列但延後,不是立刻再試"
        assert p not in s._last_cancel_err and not s.order_log[p].get("cancel_err"), "往返中不是錯誤"
        out = run_once(s)                                                # 還沒到期 → worker 不碰
        assert out["pending"] == [p] and sdk.query_calls == 0 and sdk.direct(p) == [(p, True)]
        clock.advance(0.5)                                               # 到期了,但那筆還在往返
        assert s._cancel_queue[p]["next_ts"] <= clock.time()
        out = run_once(s)                                                # worker 自己看到往返中 → 再延後
        assert out["pending"] == [p] and sdk.query_calls == 0 and sdk.direct(p) == [(p, True)], \
            "到期也不重送、不為它拉全清單"
        q = s._cancel_queue[p]
        assert q["attempts"] == 0 and q["next_ts"] > clock.time()
        assert p not in s._last_cancel_err and not s.order_log[p].get("cancel_err")
        gate.set()
        t.join(3)
        assert s.order_log[p]["status"] == "cancelled" and s._cancel_queue == {} and sdk.query_calls == 0

    def test_retried_item_without_broker_filled_word_is_not_prefiltered(self, client, clock):
        """預篩只認券商親口說「已成交」的佇列項。被拒過 (在途文案) 重試中的書號,就算本地之後因補收 + 晚到
        回報雙算成 filled,也不可以被預篩結案 — 券商端還有剩餘量 live,必須真的撤掉。"""
        sdk = client.sdk
        s = _session(client, fixed_lots=2)
        s.place_pre_orders([SYM], {SYM: LU})
        p = s.trades[SYM].pre_order_no
        sdk.fill_at_broker(p, lots=1)
        sdk.cancel_script[p] = [MSG_INFLIGHT]
        s.request_cancel(p, SYM, "unmarked")
        out = run_once(s)
        assert out["pending"] == [p] and s._cancel_queue[p]["attempts"] == 1
        assert not s._cancel_queue[p].get("fbc_unverified")
        s._apply_auth_fill(p, 1, "重連補收")
        s._on_fill(fill(p, SYM, 1, LU, filled_no="F-late"))
        assert s.order_log[p]["status"] == "filled"
        out = run_once(s, now=clock.time() + 5)
        assert sdk.book[p].status == 30 and p in out["cancelled"]

    def test_snapshot_failure_retry_counts_from_query_end(self, client, clock):
        """快照查詢花 3 s 才失敗: 下次重試要從查詢**結束**起算 +1 s,不是從該輪開始 (那早已過期 = 立刻再拉)。"""
        s = _session(client)
        s.place_pre_orders([SYM], {SYM: LU})
        _orphan(client, s)

        def slow_fail(account):
            clock.advance(3.0)
            return _res(False, [], "Login Error, 業務系統流量控管")
        client.sdk.stock.get_order_results = slow_fail
        t0 = clock.time()
        out = run_once(s, now=t0)
        assert out["snapshot_ok"] is False
        assert s._cancel_queue["ORPH1"]["next_ts"] >= t0 + 3.0 + 0.99
        assert s._cancel_queue["ORPH1"]["attempts"] == 0

    def test_cancel_timeout_retry_counts_from_fanout_end(self, client, clock):
        s, p, m1, extras = _open_with_extras(client, n_extra=1)
        x = extras[0]
        it = dict(s._cancel_queue[x])
        out = {"pending": [], "cancelled": [], "filled_before_cancel": []}
        t_before = clock.time()
        s._apply_cancel_results([(x, it, {"_direct": True})], {}, t_before - 5.0, out, source="t")
        assert s._cancel_queue[x]["next_ts"] >= t_before + 1.0 and out["pending"] == [x]

    def test_auth_filled_is_only_set_when_something_was_backfilled(self, client, clock):
        s, p, m1, extras = _open_with_extras(client, n_extra=1)
        x = extras[0]
        s._on_fill(fill(x, SYM, 1, LU, filled_no="F-x"))
        assert s._apply_auth_fill(x, 1, "對帳") == 0, "回報已記滿 → 沒有補入"
        assert not s.order_log[x].get("auth_filled")

    def test_worker_thread_survives_a_basexception_from_the_broker(self, client, clock, monkeypatch):
        """縱深防禦: 就算 broker 那層漏接了 BaseException,扇出 thread 也要留下結果 (計 attempts、照規則重試),
        不可以無聲死掉被當成「逾時」。"""
        s, p, m1, extras = _open_with_extras(client, n_extra=1)
        x = extras[0]

        def _boom(order_no, symbol="", reason=""):
            raise FakePanic("panic that escaped the broker layer")
        monkeypatch.setattr(client, "cancel_cached", _boom)
        out = run_once(s)
        assert out["pending"] == [x]
        row = s.order_log[x]
        assert row["cancel_attempts"] == 1 and "FakePanic" in row["cancel_err"]


# ═══ 第三輪審查 (2026-10-12) ══════════════════════════════════════════

def _bg(fn):
    """背景 thread 跑 fn,例外吞掉 (測試只看券商端 / 計數的結果)。"""
    def _run():
        try:
            fn()
        except Exception:      # noqa: BLE001
            pass
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return t


class TestRound3Unresponsive:
    """SDK 對下單回傳物件**完全不回應** (不拒、不丟例外): 被拒 / 例外兩條計數都看不到。證實前要算進熔斷,
    同步撤單不可以讓 caller 無限期卡住 (出場 worker 會卡在送賣單之前)。"""

    def test_unproven_sync_cancel_is_bounded_and_counts_toward_fuse(self, client, monkeypatch, caplog):
        monkeypatch.setattr(broker_mod, "DIRECT_CANCEL_ROUNDTRIP_MAX_SEC", 0.15)
        sdk = client.sdk
        a, b = client.place_limit_buy(SYM, LU, 1), client.place_market_buy(SYM, 1)
        gates = {a: threading.Event(), b: threading.Event()}
        sdk.block_placed.update(gates)
        try:
            t0 = time.monotonic()
            with pytest.raises(RuntimeError) as ei:
                client.cancel(a, SYM, reason="mkt_queue_gone")
            assert time.monotonic() - t0 < 2.0, "還沒證實的呼叫不可以讓 caller 一直等"
            assert "未回" in str(ei.value) and not isinstance(ei.value, (CancelRejected, CancelInFlight))
            assert sdk.query_calls == 0 and sdk.direct(a) == [(a, True)]
            assert client._direct_cancel_unproven_drops == 1 and not client._direct_cancel_off
            time.sleep(0.05)
            assert client.direct_cancel_route(a) == "snapshot" and client.direct_cancel_route(a) == "snapshot"
            assert client._direct_cancel_unproven_drops == 1, "同一筆只計一次"
            with pytest.raises(RuntimeError):
                client.cancel(b, SYM, reason="chase_extra")
            assert client._direct_cancel_off and client.direct_cancel_state() == "off"
            assert len(_crit(caplog, "沒有回應")) == 1
            c = client.place_market_buy(SYM, 1)                 # 停用後 = 修法前的路徑
            client.cancel(c, SYM)
            assert sdk.query_calls == 1 and sdk.via_snapshot(c) == [(c, False)]
            assert sdk.direct(a) == [(a, True)] and sdk.direct(b) == [(b, True)], "卡住的物件絕不重送"
        finally:
            for g in gates.values():
                g.set()
        # 卡住的那兩筆後來回來撤成 → 前提其實成立 → 重新啟用
        assert wait_until(lambda: client._direct_cancel_proven and not client._direct_cancel_off)
        assert wait_until(lambda: sdk.book[a].status == 30 and sdk.book[b].status == 30)

    def test_unproven_runs_on_helper_thread_proven_on_caller_thread(self, client):
        """證實後的同步撤單照舊在 caller 的 thread 上直接呼叫 (與修法前相同);helper 只用在證實之前。"""
        sdk = client.sdk
        seen = []

        def _note():
            seen.append(threading.current_thread().name)
            return None                                         # 之後照正常流程撤成

        a = client.place_limit_buy(SYM, LU, 1)
        sdk.cancel_script[a] = [_note]
        client.cancel(a, SYM)
        b = client.place_limit_buy(SYM, LU, 1)
        sdk.cancel_script[b] = [_note]
        client.cancel(b, SYM)
        assert seen[0].startswith("direct-cancel-") and seen[1] == threading.current_thread().name
        assert sdk.query_calls == 0

    def test_helper_passes_rejection_through_unchanged(self, client):
        """證實前走 helper thread,被拒的例外型別與內容要原樣回到 caller (session 靠它分類)。"""
        sdk = client.sdk
        a = client.place_limit_buy(SYM, LU, 1)
        sdk.fill_at_broker(a)
        with pytest.raises(CancelRejected) as ei:
            client.cancel(a, SYM)
        assert ei.value.direct is True and ei.value.filled_qty is None and MSG_FILLED in str(ei.value)

    def test_snapshot_path_rejection_is_not_marked_direct(self, client):
        sdk = client.sdk
        sdk.book["OLD1"] = SimpleNamespace(order_no="OLD1", stock_no=SYM, buy_sell="Buy", quantity=1000,
                                           filled_qty=1000, after_qty=1000, status=50, user_def="hitlimit")
        with pytest.raises(CancelRejected) as ei:
            client.cancel("OLD1", SYM)
        assert ei.value.direct is False and ei.value.filled_qty == 1000

    def test_stuck_call_is_not_counted_once_proven(self, client, monkeypatch):
        monkeypatch.setattr(broker_mod, "DIRECT_CANCEL_ROUNDTRIP_MAX_SEC", 0.1)
        sdk = client.sdk
        _prove(client)
        a = client.place_limit_buy(SYM, LU, 1)
        gate = threading.Event()
        sdk.block_placed[a] = gate
        t = _bg(lambda: client.cancel_cached(a, SYM, "chase_extra"))
        try:
            assert wait_until(lambda: sdk.direct(a))
            time.sleep(0.15)
            assert client.direct_cancel_route(a) == "snapshot"
            assert client._direct_cancel_unproven_drops == 0 and not client._direct_cancel_off
        finally:
            gate.set()
            t.join(3)

    def test_stuck_marker_is_cleared_when_the_call_returns(self, client, monkeypatch):
        """卡住的呼叫回來 (被拒在途文案、物件還留著) → 「卡住」登記拿掉、這筆可以再直接撤;計數維持 1。"""
        monkeypatch.setattr(broker_mod, "DIRECT_CANCEL_ROUNDTRIP_MAX_SEC", 0.1)
        sdk = client.sdk
        a = client.place_limit_buy(SYM, LU, 1)
        gate = threading.Event()
        sdk.block_placed[a] = gate
        sdk.cancel_script[a] = [MSG_INFLIGHT]
        t = _bg(lambda: client.cancel_cached(a, SYM, "chase_extra"))
        assert wait_until(lambda: sdk.direct(a))
        time.sleep(0.15)
        assert client.direct_cancel_route(a) == "snapshot" and client._direct_cancel_unproven_drops == 1
        gate.set()
        t.join(3)
        assert client._order_obj_stuck == set() and client.direct_cancel_route(a) == "direct"
        assert client._direct_cancel_unproven_drops == 1

    def test_one_order_stuck_then_rejected_is_charged_once(self, client, monkeypatch, caplog):
        """一筆單先「沒回應」、晚回來又被未知文案拒 = 還是只有一筆單有問題 → 計 1,不可以單靠它就停用;
        另一筆沒試過的單仍然走免查詢。第二筆**不同的**單出問題才停用。"""
        monkeypatch.setattr(broker_mod, "DIRECT_CANCEL_ROUNDTRIP_MAX_SEC", 0.15)
        sdk = client.sdk
        a, other = client.place_limit_buy(SYM, LU, 1), client.place_market_buy(SYM, 1)
        gate = threading.Event()
        sdk.block_placed[a] = gate
        sdk.cancel_script[a] = [MSG_UNKNOWN]
        with pytest.raises(RuntimeError):
            client.cancel(a, SYM, reason="chase_sent_cancel_pre")
        assert client._direct_cancel_unproven_drops == 1
        gate.set()
        assert wait_until(lambda: not client._order_obj_inflight)
        assert client.direct_cancel_route(a) == "snapshot", "被未知文案拒 → 這筆的物件丟掉"
        assert client._direct_cancel_unproven_drops == 1 and not client._direct_cancel_off
        assert _crit(caplog, "尚無任何一筆撤成") == [] and client.direct_cancel_route(other) == "direct"
        sdk.cancel_script[other] = [MSG_UNKNOWN]
        with pytest.raises(CancelRejected):
            client.cancel(other, SYM, reason="chase_extra")
        assert client._direct_cancel_unproven_drops == 2 and client._direct_cancel_off
        assert len(_crit(caplog, "尚無任何一筆撤成")) == 1

    def test_one_order_stuck_then_inflight_text_times_out_is_charged_once(self, client, monkeypatch, caplog):
        clk = _MonoClock()
        monkeypatch.setattr(broker_mod, "time", clk)
        sdk = client.sdk
        a = client.place_limit_buy(SYM, LU, 1)
        gate = threading.Event()
        sdk.block_placed[a] = gate
        sdk.cancel_script[a] = [MSG_INFLIGHT]
        t = _bg(lambda: client.cancel_cached(a, SYM, "chase_extra"))
        assert wait_until(lambda: sdk.direct(a))
        clk.t += 5.0
        assert client.direct_cancel_route(a) == "snapshot" and client._direct_cancel_unproven_drops == 1
        gate.set()
        t.join(3)
        assert client.direct_cancel_route(a) == "direct"            # 在途文案: 物件留著
        clk.t += 5.01                                               # 自被拒起又拖過上限 → 放棄這筆
        assert client.direct_cancel_route(a) == "snapshot"
        assert client._direct_cancel_unproven_drops == 1 and not client._direct_cancel_off
        assert _crit(caplog, "尚無任何一筆撤成") == []

    def test_relogin_clears_the_charged_set_with_the_counter(self, client):
        client._account_id, client._password, client._pfx_path, client._pfx_password = "X123", "pw", "x.pfx", ""
        a = client.place_limit_buy(SYM, LU, 1)
        client._give_up_object(a, SYM, "x")
        assert client._direct_cancel_charged == {a}
        client.re_login()
        assert client._direct_cancel_charged == set() and client._direct_cancel_unproven_drops == 0

    def test_first_market_success_does_not_hang_on_unresponsive_pre_order_cancel(self, client, clock,
                                                                                  monkeypatch):
        """開盤第一筆市價成功後同步撤 P,SDK 不回: 送單 thread 在門檻內回來、P 留佇列 (不是當成已撤),
        worker 改走快照把它撤掉。"""
        monkeypatch.setattr(broker_mod, "DIRECT_CANCEL_ROUNDTRIP_MAX_SEC", 0.15)
        sdk = client.sdk
        s = _session(client)
        s.place_pre_orders([SYM], {SYM: LU})
        p = s.trades[SYM].pre_order_no
        gate = threading.Event()
        sdk.block_placed[p] = gate
        try:
            t0 = time.monotonic()
            assert s._chase_send_one(SYM, 1) == "accepted"
            assert time.monotonic() - t0 < 2.0
            assert s.order_log[p]["status"] == "pending" and p in s._cancel_queue
            assert sdk.query_calls == 0
            time.sleep(0.05)
            out = run_once(s, now=clock.time() + 1)
            assert sdk.direct(p) == [(p, True)] and sdk.via_snapshot(p) == [(p, False)]
            assert out["cancelled"] == [p] and s.order_log[p]["status"] == "cancelled" and sdk.query_calls == 1
        finally:
            gate.set()

    def test_exit_cancel_is_not_stuck_behind_unresponsive_direct_cancel(self, client, clock, monkeypatch):
        """出場 worker 自己是第一個撞上不回應的呼叫: cancel_symbol_orders 要在門檻內回來 (賣單才送得出去)。"""
        monkeypatch.setattr(broker_mod, "DIRECT_CANCEL_ROUNDTRIP_MAX_SEC", 0.15)
        sdk = client.sdk
        s = _session(client)
        s.place_pre_orders([SYM], {SYM: LU})
        p = s.trades[SYM].pre_order_no
        gate = threading.Event()
        sdk.block_placed[p] = gate
        try:
            t0 = time.monotonic()
            s.cancel_symbol_orders(SYM, "mkt_queue_gone")
            assert time.monotonic() - t0 < 2.0
            assert s.order_log[p]["status"] == "pending" and p in s._cancel_queue
            assert client._direct_cancel_unproven_drops == 1
        finally:
            gate.set()

    def test_unresponsive_fanout_cancels_trip_the_fuse(self, client, clock, monkeypatch, caplog):
        """撤單 worker 扇出的免查詢撤單都不回: 逾時後下一輪分流時把它們算進熔斷 (2 筆 → 停用 + CRITICAL),
        改走快照撤掉;卡住的物件不重送。"""
        monkeypatch.setattr(ts_mod, "_CANCEL_SDK_TIMEOUT_SEC", 0.15)
        monkeypatch.setattr(broker_mod, "DIRECT_CANCEL_ROUNDTRIP_MAX_SEC", 0.1)
        sdk = client.sdk
        s = _session(client)
        a, b = client.place_market_buy(SYM, 1), client.place_market_buy(SYM, 1)
        gates = {a: threading.Event(), b: threading.Event()}
        sdk.block_placed.update(gates)
        for no in (a, b):
            s._log_order(no, SYM, "buy", "market_buy", 1, 0)
            s.request_cancel(no, SYM, "chase_extra")
        try:
            out = run_once(s)
            assert sorted(out["pending"]) == sorted([a, b]) and sdk.query_calls == 0
            assert client._direct_cancel_unproven_drops == 0
            out = run_once(s, now=clock.time() + 5)
            assert client._direct_cancel_off and len(_crit(caplog, "沒有回應")) == 1
            assert sorted(out["cancelled"]) == sorted([a, b]) and sdk.query_calls == 1
            assert sdk.direct(a) == [(a, True)] and sdk.direct(b) == [(b, True)]
        finally:
            for g in gates.values():
                g.set()


class TestRound3Relogin:
    """重連 (re_login) 之後: 斷線期間根本無法重試 → 不可以拿斷線前的時間讓新連線一上來就熔斷。"""

    @staticmethod
    def _creds(client):
        client._account_id, client._password, client._pfx_path, client._pfx_password = "X123", "pw", "x.pfx", ""

    def test_reject_clocks_restart_on_the_new_connection(self, client, monkeypatch, caplog):
        clk = _MonoClock()
        monkeypatch.setattr(broker_mod, "time", clk)
        self._creds(client)
        sdk = client.sdk
        a, b = client.place_limit_buy(SYM, LU, 1), client.place_market_buy(SYM, 1)
        for no in (a, b):
            sdk.cancel_script[no] = [MSG_INFLIGHT]
            with pytest.raises(CancelRejected):
                client.cancel_cached(no, SYM, "chase_extra")
        clk.t += 8.0                                            # 斷線 8 s
        client.re_login()
        assert client.has_cached_order(a) and client.has_cached_order(b), "新連線還沒試過,不可以就放棄"
        assert client._direct_cancel_unproven_drops == 0 and not client._direct_cancel_off
        assert _crit(caplog, "尚無任何一筆撤成") == []
        clk.t += 4.99
        assert client.has_cached_order(a)
        clk.t += 0.02                                           # 新連線上滿 5 s 仍未結案 → 熔斷照舊,沒有變鬆
        assert not client.has_cached_order(a) and not client.has_cached_order(b)
        assert client._direct_cancel_off and len(_crit(caplog, "尚無任何一筆撤成")) == 1

    def test_unknown_reject_count_survives_relogin(self, client, monkeypatch):
        """重新起算的只有時鐘;未知文案被拒的次數保留 (那是券商對這個物件的回答,與連線無關)。"""
        self._creds(client)
        sdk = client.sdk
        _prove(client)
        a = client.place_limit_buy(SYM, LU, 1)
        sdk.cancel_script[a] = [MSG_UNKNOWN, MSG_UNKNOWN]
        for _ in range(2):
            with pytest.raises(CancelRejected):
                client.cancel_cached(a, SYM, "chase_extra")
        client.re_login()
        assert client._order_obj_rejects[a][0] == 2 and client.has_cached_order(a)

    def test_call_in_flight_at_reconnect_is_not_counted_as_stuck(self, client, monkeypatch):
        clk = _MonoClock()
        monkeypatch.setattr(broker_mod, "time", clk)
        self._creds(client)
        sdk = client.sdk
        a = client.place_limit_buy(SYM, LU, 1)
        gate = threading.Event()
        sdk.block_placed[a] = gate
        t = _bg(lambda: client.cancel_cached(a, SYM, "chase_sent_cancel_pre"))
        try:
            assert wait_until(lambda: sdk.direct(a))
            client.re_login()
            clk.t += 6.0
            assert client.direct_cancel_route(a) == "snapshot"          # 卡住 → 走快照,不重送
            assert client._direct_cancel_unproven_drops == 0, "卡住是因為斷線,不是因為物件 → 不計入熔斷"
        finally:
            gate.set()
            t.join(3)

    def test_stuck_is_not_counted_while_connection_is_unhealthy(self, client, monkeypatch):
        clk = _MonoClock()
        monkeypatch.setattr(broker_mod, "time", clk)
        sdk = client.sdk
        a = client.place_limit_buy(SYM, LU, 1)
        gate = threading.Event()
        sdk.block_placed[a] = gate
        t = _bg(lambda: client.cancel_cached(a, SYM, "chase_sent_cancel_pre"))
        try:
            assert wait_until(lambda: sdk.direct(a))
            client.healthy = False
            clk.t += 6.0
            assert client.direct_cancel_route(a) == "snapshot" and client._direct_cancel_unproven_drops == 0
            client.healthy = True
            assert client.direct_cancel_route(a) == "snapshot" and client._direct_cancel_unproven_drops == 1
        finally:
            gate.set()
            t.join(3)


class TestRound3Route:
    """該怎麼撤要在 broker 一個鎖區塊內決定 — 「在途嗎」「有物件嗎」分兩次問,中間被別的 thread 認領
    就會掉進快照查詢 + 重複送撤單。"""

    def test_route_values(self, client, monkeypatch):
        sdk = client.sdk
        assert client.direct_cancel_route("NOPE") == "snapshot"
        a = client.place_limit_buy(SYM, LU, 1)
        assert client.direct_cancel_route(a) == "direct"
        gate = threading.Event()
        sdk.block_placed[a] = gate
        t = _bg(lambda: client.cancel_cached(a, SYM, "chase_extra"))
        try:
            assert wait_until(lambda: sdk.direct(a))
            assert client.direct_cancel_route(a) == "in_flight"
            assert client.direct_cancel_in_flight(a) and not client.has_cached_order(a)
        finally:
            gate.set()
            t.join(3)
        b = client.place_limit_buy(SYM, LU, 1)
        monkeypatch.setenv("CANCEL_DIRECT_OBJ", "0")
        assert client.direct_cancel_route(b) == "snapshot"
        monkeypatch.delenv("CANCEL_DIRECT_OBJ")                  # 未設 = 預設關 (2026-10-14)
        assert client.direct_cancel_route(b) == "snapshot"
        monkeypatch.setenv("CANCEL_DIRECT_OBJ", "1")
        assert client.direct_cancel_route(b) == "direct"
        client._direct_cancel_off = True
        assert client.direct_cancel_route(b) == "snapshot"

    def test_claim_between_route_and_send_neither_queries_nor_resends(self, client, monkeypatch):
        """facade 讀到 'direct' 之後、送出之前被別的 thread 搶先認領 → CancelInFlight (沒送、沒查清單)。"""
        sdk = client.sdk
        _prove(client)
        a = client.place_limit_buy(SYM, LU, 1)
        gate = threading.Event()
        sdk.block_placed[a] = gate
        real = client.direct_cancel_route
        other = []

        def racy(order_no):
            r = real(order_no)
            if not other:
                other.append(_bg(lambda: client.cancel_cached(a, SYM, "chase_sent_cancel_pre")))
                assert wait_until(lambda: sdk.direct(a))
            return r

        monkeypatch.setattr(client, "direct_cancel_route", racy)
        try:
            with pytest.raises(CancelInFlight):
                client.cancel(a, SYM, reason="mkt_queue_gone")
            assert sdk.query_calls == 0 and sdk.direct(a) == [(a, True)] and sdk.via_snapshot(a) == []
        finally:
            gate.set()
            other[0].join(3)

    def test_worker_asks_broker_once_per_item(self, client, clock, monkeypatch):
        s, p, m1, extras = _open_with_extras(client, n_extra=2)

        def _boom(no):
            raise AssertionError("分兩次問會有空窗 — 要用 direct_cancel_route")

        monkeypatch.setattr(client, "has_cached_order", _boom)
        monkeypatch.setattr(client, "direct_cancel_in_flight", _boom)
        out = run_once(s)
        assert sorted(out["cancelled"]) == sorted(extras) and client.sdk.query_calls == 0

    def test_split_falls_back_to_two_calls_for_stand_ins_without_route(self, client, clock):
        s = _session(client)
        s.broker = SimpleNamespace(has_cached_order=lambda no: no == "D",
                                   direct_cancel_in_flight=lambda no: no == "F",
                                   cancel_cached=lambda *a, **k: None)
        items = {n: {"symbol": SYM, "reason": "x", "first_ts": 0.0, "attempts": 0, "next_ts": 0.0,
                     "last_crit_ts": 0.0} for n in "DFS"}
        s._cancel_queue.update(items)
        out = {"pending": []}
        direct, rest = s._split_direct_cancels([(n, items[n]) for n in "DFS"], clock.time(), out)
        assert [n for n, _ in direct] == ["D"] and [n for n, _ in rest] == ["S"] and out["pending"] == ["F"]
        assert items["F"]["next_ts"] > clock.time() and items["F"]["attempts"] == 0

    def test_worker_claim_race_is_deferred_not_counted(self, client, clock, monkeypatch):
        """worker 分流時讀到 'direct',扇出 thread 送出之前被同步撤單搶先認領 → 這次沒送: 延後再看,
        不計 attempts、不記錯誤、不查清單。"""
        sdk = client.sdk
        s, p, m1, extras = _open_with_extras(client, n_extra=1)
        x = extras[0]
        gate = threading.Event()
        sdk.block_placed[x] = gate
        t = _bg(lambda: client.cancel_cached(x, SYM, "chase_extra"))
        try:
            assert wait_until(lambda: sdk.direct(x))
            monkeypatch.setattr(client, "direct_cancel_route", lambda no: "direct")   # 分流當下讀到的舊狀態
            out = run_once(s)
            assert out["pending"] == [x] and sdk.query_calls == 0 and sdk.direct(x) == [(x, True)]
            row, q = s.order_log[x], s._cancel_queue[x]
            assert q["attempts"] == 0 and q["next_ts"] > clock.time()
            assert not row.get("cancel_err") and row.get("cancel_attempts", 0) == 0
        finally:
            gate.set()
            t.join(3)

    def test_give_up_counts_an_order_once(self, client):
        a = client.place_limit_buy(SYM, LU, 1)
        client._give_up_object(a, SYM, "x")
        client._give_up_object(a, SYM, "x")
        assert client._direct_cancel_unproven_drops == 1 and not client._direct_cancel_off

    def test_sync_cancel_skips_broker_when_row_was_confirmed_meanwhile(self, client, clock):
        """caller 讀到「P pending」之後在送單額度上排隊,排到時別的 thread 已撤成 (券商確認、物件已丟) →
        不再問券商,更不為一筆已撤的單拉全清單 (出場 worker 撤 P 時會凍在賣單之前)。"""
        sdk = client.sdk
        s = _session(client)
        s.place_pre_orders([SYM], {SYM: LU})
        p = s.trades[SYM].pre_order_no
        client.cancel(p, SYM, reason="chase_sent_cancel_pre")
        s._confirm_cancelled(p, SYM, "chase_sent_cancel_pre", source="sync:ok")
        calls = list(sdk.cancel_calls)
        assert s._try_cancel_sync(p, SYM, "mkt_queue_gone") == "already_cancelled"
        assert sdk.query_calls == 0 and sdk.cancel_calls == calls and p not in s._cancel_queue

    def test_kill_switch_keeps_asking_the_broker_for_a_confirmed_row(self, client, clock, monkeypatch):
        """CANCEL_DIRECT_OBJ=0 = 撤單路徑與修法前完全相同: 上面那條捷徑不生效,照舊先查清單、問券商。"""
        monkeypatch.setenv("CANCEL_DIRECT_OBJ", "0")
        sdk = client.sdk
        s = _session(client)
        s.place_pre_orders([SYM], {SYM: LU})
        p = s.trades[SYM].pre_order_no
        client.cancel(p, SYM, reason="chase_sent_cancel_pre")
        s._confirm_cancelled(p, SYM, "chase_sent_cancel_pre", source="sync:ok")
        assert s._try_cancel_sync(p, SYM, "mkt_queue_gone") == "already_cancelled"
        assert sdk.query_calls == 2 and sdk.via_snapshot(p) == [(p, False), (p, False)]

    def test_tripped_fuse_keeps_asking_the_broker_for_a_confirmed_row(self, client, clock):
        sdk = client.sdk
        s = _session(client)
        s.place_pre_orders([SYM], {SYM: LU})
        p = s.trades[SYM].pre_order_no
        client._direct_cancel_off = True
        client.cancel(p, SYM, reason="chase_sent_cancel_pre")
        s._confirm_cancelled(p, SYM, "chase_sent_cancel_pre", source="sync:ok")
        assert s._try_cancel_sync(p, SYM, "mkt_queue_gone") == "already_cancelled"
        assert sdk.query_calls == 2 and len(sdk.via_snapshot(p)) == 2

    def test_sync_cancel_still_asks_broker_for_a_filled_row(self, client, clock):
        """只擋 cancelled (券商確認寫入的);filled 的列照問券商 (成交量可能是補入而雙算,券商端仍有剩餘量)。"""
        sdk = client.sdk
        s = _session(client)
        s.place_pre_orders([SYM], {SYM: LU})
        p = s.trades[SYM].pre_order_no
        sdk.fill_at_broker(p)
        s._on_fill(fill(p, SYM, 1, LU, filled_no="F-p"))
        assert s.order_log[p]["status"] == "filled"
        assert s._try_cancel_sync(p, SYM, "mkt_queue_gone") == "filled_before_cancel"
        assert sdk.direct(p) == [(p, True)] and sdk.query_calls == 0


class TestRound3NoRemaining:
    """「[9050]該筆委託已無剩餘量」: 免查詢撤單被這樣拒 = 已終結、成交量待核實 → 比照「成交單已不允許取消」,
    成交回報到了就不查清單。快照路徑 / 關閉開關時不變。"""

    def test_classifiers(self):
        assert classify_cancel_error(MSG_NO_REMAINING) == "retry"
        assert _classify_direct_cancel_error(MSG_NO_REMAINING) == "filled_before_cancel"
        assert _classify_direct_cancel_error(MSG_FILLED) == "filled_before_cancel"
        assert _classify_direct_cancel_error(MSG_ALREADY_CANCELLED) == "already_cancelled"
        assert _classify_direct_cancel_error(MSG_INFLIGHT) == "retry"

    def test_sync_pre_order_cancel_then_fill_report_no_query(self, client, clock):
        sdk = client.sdk
        s = _session(client)
        s.place_pre_orders([SYM], {SYM: LU})
        p = s.trades[SYM].pre_order_no
        sdk.cancel_script[p] = [MSG_NO_REMAINING]
        assert s._chase_send_one(SYM, 1) == "accepted"
        assert s.order_log[p]["status"] == "pending" and s._cancel_queue[p].get("fbc_unverified")
        assert s._cancel_queue[p]["next_ts"] > clock.time(), "有退避,不是立刻為它查清單"
        s._on_fill(fill(p, SYM, 1, LU, filled_no="F-p"))
        out = run_once(s, now=clock.time() + 1)
        assert sdk.query_calls == 0 and out["filled_before_cancel"] == [p] and s._cancel_queue == {}
        assert s.order_log[p]["status"] == "filled"

    def test_worker_extra_then_fill_report_no_query(self, client, clock):
        sdk = client.sdk
        s, p, m1, extras = _open_with_extras(client, n_extra=1)
        x = extras[0]
        sdk.cancel_script[x] = [MSG_NO_REMAINING]
        out = run_once(s)
        assert out["pending"] == [x] and s._cancel_queue[x].get("fbc_unverified") and sdk.query_calls == 0
        s._on_fill(fill(x, SYM, 1, LU, filled_no="F-x"))
        out = run_once(s, now=clock.time() + 1)
        assert sdk.query_calls == 0 and out["filled_before_cancel"] == [x] and s._cancel_queue == {}

    def test_without_fill_report_snapshot_decides_and_cancelled_stays_cancelled(self, client, clock):
        """沒有成交回報就不假設成交: 下一輪用快照核實;券商端其實是已撤 (status 30) → 結成 cancelled、成交 0。"""
        sdk = client.sdk
        s, p, m1, extras = _open_with_extras(client, n_extra=1)
        x = extras[0]
        sdk.cancel_script[x] = [MSG_NO_REMAINING]
        run_once(s)
        assert s.order_log[x]["status"] == "pending"
        sdk.book[x].status = 30
        out = run_once(s, now=clock.time() + 1)
        assert sdk.query_calls == 1 and out["cancelled"] == [x]
        assert s.order_log[x]["status"] == "cancelled" and s.order_log[x]["filled_lots"] == 0

    def test_snapshot_path_keeps_old_classification(self, client, clock, monkeypatch):
        monkeypatch.setenv("CANCEL_DIRECT_OBJ", "0")
        sdk = client.sdk
        s, p, m1, extras = _open_with_extras(client, n_extra=1)
        x = extras[0]
        sdk.cancel_script[x] = [MSG_NO_REMAINING]
        out = run_once(s)
        q = s._cancel_queue[x]
        assert out["pending"] == [x] and not q.get("fbc_unverified") and q["attempts"] == 1

    def test_sync_snapshot_path_keeps_old_classification(self, client, clock, monkeypatch):
        monkeypatch.setenv("CANCEL_DIRECT_OBJ", "0")
        sdk = client.sdk
        s = _session(client)
        s.place_pre_orders([SYM], {SYM: LU})
        p = s.trades[SYM].pre_order_no
        sdk.cancel_script[p] = [MSG_NO_REMAINING]
        assert s._try_cancel_sync(p, SYM, "mkt_queue_gone") == "queued"
        assert sdk.query_calls == 1 and sdk.via_snapshot(p) == [(p, False)]
        assert not s._cancel_queue[p].get("fbc_unverified")


class TestRound3QueryPanic:
    """get_order_results 丟 BaseException (PyO3 panic): 這條是免查詢撤單停用後的退路,不可以讓撤單 worker /
    出場 worker 無聲死掉 → 轉成 OrderLookupError (查詢失敗,不是查無)。"""

    @staticmethod
    def _panic(client):
        def _boom(account):
            raise FakePanic("panic in get_order_results")
        client.sdk.stock.get_order_results = _boom

    def test_broker_converts_to_lookup_error(self, client):
        self._panic(client)
        with pytest.raises(OrderLookupError) as ei:
            client.get_order_snapshot()
        assert "FakePanic" in str(ei.value)
        with pytest.raises(OrderLookupError):
            client.cancel("OLD1", SYM)
        assert not client.order_query_in_flight()

    def test_keyboard_interrupt_still_propagates(self, client):
        def _kb(account):
            raise KeyboardInterrupt()
        client.sdk.stock.get_order_results = _kb
        with pytest.raises(KeyboardInterrupt):
            client.get_order_snapshot()

    def test_worker_round_survives(self, client, clock):
        s = _session(client)
        _orphan(client, s)
        self._panic(client)
        out = run_once(s)                                       # 不可以 raise
        assert out["snapshot_ok"] is False and out["pending"] == ["ORPH1"]
        assert s.order_log["ORPH1"]["status"] == "pending" and "ORPH1" in s._cancel_queue

    def test_sync_cancel_survives(self, client, clock):
        s = _session(client)
        _orphan(client, s)
        self._panic(client)
        assert s._try_cancel_sync("ORPH1", SYM, "mkt_queue_gone") == "queued"
        assert s.order_log["ORPH1"]["status"] == "pending"


def test_retry_backoff_counts_from_round_end_in_fallback_mode_too(client, clock, monkeypatch):
    """定案 (第三輪審查): 關閉開關 / 熔斷還原的是撤單**路徑**;退避「從該輪結束起算」是獨立修改,任何模式都生效
    (查詢比退避久時,從該輪開始起算 = 零間隔連續拉全清單)。"""
    monkeypatch.setenv("CANCEL_DIRECT_OBJ", "0")
    s, p, m1, extras = _open_with_extras(client, n_extra=1)
    x = extras[0]
    s._cancel_retry_later(x, clock.time() - 5.0, "NOT_IN_SNAPSHOT")
    assert s._cancel_queue[x]["next_ts"] >= clock.time() + 0.29
