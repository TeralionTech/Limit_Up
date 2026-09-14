"""出場賣單風暴 + 過期張數 (2026-09-14 node3 / node4 事故) — ITEM B:

  node3: 策略買 4556 × 8 → 09:12 策略外賣 2 張 → 09:19 出場送 8 張跌停限價賣被拒「證券可賣不足」×8 (~16 s)
         → CRITICAL → 每 ~21 s 新一輪 8 筆被拒,disarm 後照跑,直到人工賣掉剩 6 張;13:24 隔日賣檔仍記 8 張
  node4: 買 × 10 → 策略外賣 3 張 → 出場送 10 張被拒「委託超過庫存,或未符合當沖資格或洽營業員[4385045]」同型

修法 (本檔驗證):
  1. broker.get_sellable_lots: 嚴格可賣張數查詢 (過 5/s 閘門、失敗 raise OrderLookupError、只看 tradable_qty)
  2. _sell_position 被拒「庫存不足」才查 (不預先封頂): 可賣 < 送出 → 改張數重送;查詢失敗 → 原重試;
     可賣 0 連續兩次 → SELL_NOTHING_LEFT;隔日賣清單剩餘張數要扣掉
  3. 出場 worker / 晚成交賣 / 緊急全平: nothing-left → exited 維持 True、清 sell_failed、CRITICAL 一次
  4. 跨 tick 風暴: 出場失敗輪次冷卻 30→60→120→300 s,由 exit_position 執行 (直呼 _exit_worker 不受限)
  5. 出場賣單被券商非同步退 → ERROR + exited 回退 (受冷卻)
  6. 策略外賣出成交 (策略持有該檔) → WARNING,不動 filled_lots
  2026-09-15 審查修正:
  7. nothing-left 只在「可委託 0 **且** 整股餘額 0 (扣隔日保留)」連續 ≥2 次、跨 ≥5 s 才成立;禁現沖股整段跳過;
     可委託 >0 只是被隔日保留扣光 → 未知;nothing-left 的部位仍列入 13:24 隔日賣檔 (隔早以庫存對帳)
  8. 隔日賣保留量以第一次對帳值 (lots_open) 封頂 (盤中 refresh 會把今日買進算進 lots)
  9. 送單成功只清冷卻 (且賣單仍 pending 才清),輪次到策略賣單成交才歸零 → 非同步退單冷卻逐輪升級
 10. broker 斷線造成的失敗輪次不冷卻;重連清冷卻
"""
import logging
import threading
import time as _real_time
from types import SimpleNamespace

import pytest

import trading_session as ts_mod
from broker import OrderLookupError, RealOrderClient
from fakes_cancel import FakeSnapshotBroker, install_fake_clock, make_session, fill, wait_until
from trader import Trader

SYM = "4556"
LU = 94.6
DOWN = 85.2                                     # 事故當日跌停價 (SELL_LMT 8 @85.2)
MSG_NODE3 = "證券可賣不足"
MSG_NODE4 = "委託超過庫存,或未符合當沖資格或洽營業員[4385045]"
MSG_RATE_LIMIT = "Login Error, 業務系統流量控管"
MKT_PRESENT = [{"price": 0.0, "size": 800}, {"price": LU, "size": 500}]
MKT_GONE = [{"price": LU, "size": 500}]


class InvBroker(FakeSnapshotBroker):
    """券商端有「可賣股數」概念的假 broker:
    held = 券商端目前可賣張數 (送賣單當下檢查;送出成功即扣 — 在途賣單佔用可賣);
    sellable = get_sellable_lots 回傳 (None → 回 held;list → 依序取、取到剩最後一個重複用;例外物件 → raise)。
    sell_attempts 記**所有**賣單嘗試 (含被拒);placed 只記成功 (FakeSnapshotBroker 原語意)。
    balance = get_sellable_position 的整股餘額 (None → 同本次可委託數;例外物件 → raise)。
    get_sellable_position 的可委託數經由 get_sellable_lots 取 (測試覆寫 get_sellable_lots 兩者一起生效)。"""

    def __init__(self, held=0, short_msg=MSG_NODE3, sellable=None, with_query=True, balance=None):
        super().__init__()
        self.held = held
        self.short_msg = short_msg
        self.sellable = sellable
        self.balance = balance
        self.sell_attempts = []
        self.sellable_calls = []
        self.other_reject = None          # 非庫存類拒因 (str) — 設了就所有賣單以此訊息被拒
        if not with_query:
            self.get_sellable_lots = None     # 模擬舊 broker 無嚴格查詢 (session 以 getattr 判斷)
            self.get_sellable_position = None

    def place_limit_sell(self, symbol, price, lots, reason=""):
        self.sell_attempts.append((symbol, price, lots, reason))
        if self.other_reject:
            raise RuntimeError(f"下單被拒 {symbol}: {self.other_reject}")
        if lots > self.held:
            raise RuntimeError(f"下單被拒 {symbol}: {self.short_msg}")
        self.held -= lots
        return super().place_limit_sell(symbol, price, lots, reason)

    def get_sellable_lots(self, symbol):
        self.sellable_calls.append(symbol)
        v = self.sellable
        if isinstance(v, list):
            v = v.pop(0) if len(v) > 1 else v[0]
        if isinstance(v, BaseException):
            raise v
        return self.held if v is None else v

    def get_sellable_position(self, symbol):
        tradable = self.get_sellable_lots(symbol)
        bal = self.balance
        if isinstance(bal, BaseException):
            raise bal
        return {"tradable": tradable, "balance": tradable if bal is None else bal}


def _position(broker, lots=8, symbol=SYM, down=DOWN):
    """策略買進 lots 張並全數成交 (預掛 P 成交 → order done)。券商端 held 由測試自己設。"""
    s = make_session(broker=broker, total=10_000_000, per_symbol=0,
                     sizing_mode="fixed_lots", fixed_lots=lots)
    s.set_limit_downs({symbol: down})
    s.place_pre_orders([symbol], {symbol: LU})
    st = s.trades[symbol]
    s._on_fill(fill(st.order_no, symbol, lots, LU, filled_no="BUY"))
    assert st.filled_lots == lots and st.order_status == "done"
    return s, st


def _external_sell(s, lots, symbol=SYM, no="EXT1"):
    """策略外賣出成交回報 (order_no 不在 order_log)。"""
    s._on_fill(fill(no, symbol, lots, LU, action="sell", filled_no=f"F-{no}"))


def _lots(attempts):
    return [a[2] for a in attempts]


def _crit(caplog, text=""):
    return [r for r in caplog.records
            if r.levelno == logging.CRITICAL and text in r.getMessage()]


@pytest.fixture
def logs(caplog):
    caplog.set_level(logging.INFO, logger="trading_session")
    return caplog


@pytest.fixture
def confirm_now(monkeypatch):
    """nothing-left 的時間跨度門檻歸零 (測試退避為 0;專測時間跨度者見 TestNothingLeftConfirmWindow)。"""
    monkeypatch.setattr(ts_mod, "_SELLABLE_ZERO_CONFIRM_SEC", 0.0)


# ═══ 1. broker.get_sellable_lots 嚴格查詢 ══════════════════════════════════

def _inv(symbol, tradable=None, lastday=0, today=0, order_type="OrderType.Stock", **kw):
    d = dict(stock_no=symbol, lastday_qty=lastday, today_qty=today, order_type=order_type, **kw)
    if tradable is not None:
        d["tradable_qty"] = tradable
    return SimpleNamespace(**d)


class _InvSDK:
    """sdk.accounting.inventories / sdk.stock.get_order_results 假物件;每次查詢記 perf_counter。"""

    def __init__(self, result=None, raise_exc=None):
        self.result = result
        self.raise_exc = raise_exc
        self.query_ts = []
        self.accounting = SimpleNamespace(inventories=self._inv)
        self.stock = SimpleNamespace(get_order_results=self._orders)

    def _inv(self, account):
        self.query_ts.append(("inv", _real_time.perf_counter()))
        if self.raise_exc is not None:
            raise self.raise_exc
        return self.result

    def _orders(self, account):
        self.query_ts.append(("orders", _real_time.perf_counter()))
        return SimpleNamespace(is_success=True, data=[], message="")


@pytest.fixture
def client(tmp_path):
    c = RealOrderClient(tmp_path / "orders.csv")
    c.account = object()
    c.connected = True
    c.healthy = True
    c.query_min_interval = 0.0
    yield c
    c.close()


class TestBrokerSellableQuery:
    def test_returns_tradable_lots_for_symbol_only(self, client):
        client.sdk = _InvSDK(SimpleNamespace(is_success=True, message=None, data=[
            _inv("2330", tradable=5000),
            _inv(SYM, tradable=6000, lastday=0, today=8000),
            _inv(SYM, tradable=3000, order_type="OrderType.Margin"),   # 融資列 — 現股賣單賣不到
        ]))
        assert client.get_sellable_lots(SYM) == 6

    def test_no_lastday_fallback_and_absent_symbol_is_zero(self, client):
        client.sdk = _InvSDK(SimpleNamespace(is_success=True, message=None, data=[
            _inv(SYM, tradable=0, lastday=8000, today=0),              # get_inventories 會退回 lastday → 8
            _inv("2330", tradable=1000)]))
        assert client.get_sellable_lots(SYM) == 0
        assert client.get_sellable_lots("9999") == 0                   # 查詢成功、庫存無此檔 → 0
        assert client.get_inventories() == [{"symbol": SYM, "lots": 8, "order_type": "Stock"},
                                            {"symbol": "2330", "lots": 1, "order_type": "Stock"}]

    def test_is_success_false_raises_with_broker_message(self, client):
        client.sdk = _InvSDK(SimpleNamespace(is_success=False, message=MSG_RATE_LIMIT, data=None))
        with pytest.raises(OrderLookupError, match="流量控管"):
            client.get_sellable_lots(SYM)

    def test_sdk_exception_raises_lookup_error(self, client):
        client.sdk = _InvSDK(raise_exc=TimeoutError("socket timeout"))
        with pytest.raises(OrderLookupError, match="socket timeout"):
            client.get_sellable_lots(SYM)

    @pytest.mark.parametrize("result", [
        None,                                                          # result 回空
        SimpleNamespace(is_success=True, message=None, data=None),      # 成功但無 data
        SimpleNamespace(is_success=True, message=None, data=[_inv(SYM)]),            # 缺 tradable_qty
        SimpleNamespace(is_success=True, message=None, data=[_inv(SYM, tradable="x")]),  # 無法解析
    ])
    def test_ambiguous_results_are_lookup_errors_not_zero(self, client, result):
        client.sdk = _InvSDK(result)
        with pytest.raises(OrderLookupError):
            client.get_sellable_lots(SYM)

    def test_not_ready_raises_without_calling_sdk(self, client):
        client.sdk = _InvSDK(SimpleNamespace(is_success=True, message=None, data=[]))
        client.healthy = False
        with pytest.raises(OrderLookupError):
            client.get_sellable_lots(SYM)
        assert client.sdk.query_ts == []

    def test_shares_query_gate_with_order_results(self, client):
        client.sdk = _InvSDK(SimpleNamespace(is_success=True, message=None, data=[_inv(SYM, tradable=1000)]))
        client.query_min_interval = 0.1
        client._query_order_results()
        client.get_sellable_lots(SYM)
        client._query_order_results()
        ts = [t for _, t in client.sdk.query_ts]
        assert [k for k, _ in client.sdk.query_ts] == ["orders", "inv", "orders"]
        # 同一閘門 = 同一條時槽序列: 第 i 筆 (不論哪種查詢) 不早於 第 0 筆 + i×interval。
        # 閘門保證的是時槽間距而非相鄰實際間隔 (Windows sleep 超睡會讓前一筆晚到、下一筆仍準時槽) → 不比相鄰差
        offsets = [t - ts[0] for t in ts]
        assert all(off >= i * 0.1 - 0.005 for i, off in enumerate(offsets)), offsets

    def test_position_returns_tradable_and_balance(self, client):
        client.sdk = _InvSDK(SimpleNamespace(is_success=True, message=None, data=[
            _inv(SYM, tradable=0, lastday=0, today=5000),              # 禁現沖今日買進: 可委託 0、餘額 5 張
            _inv(SYM, tradable=0, today=3000, order_type="OrderType.Margin"),
            _inv("2330", tradable=1000, today=1000)]))
        assert client.get_sellable_position(SYM) == {"tradable": 0, "balance": 5}
        assert client.get_sellable_position("9999") == {"tradable": 0, "balance": 0}
        assert client.get_sellable_lots(SYM) == 0

    def test_position_missing_today_qty_is_lookup_error(self, client):
        row = SimpleNamespace(stock_no=SYM, tradable_qty=0, order_type="OrderType.Stock")
        client.sdk = _InvSDK(SimpleNamespace(is_success=True, message=None, data=[row]))
        with pytest.raises(OrderLookupError, match="today_qty"):
            client.get_sellable_position(SYM)
        assert client.get_sellable_lots(SYM) == 0                      # 只看可委託數的查詢不受影響

    def test_get_inventories_still_swallows_errors(self, client):
        client.sdk = _InvSDK(raise_exc=RuntimeError("boom"))
        assert client.get_inventories() == []                          # 既有呼叫端行為不變


# ═══ 2. node3 / node4 重演: 被拒後依券商可賣張數重算 ════════════════════════

class TestResizeOnInventoryShortage:
    def test_node3_external_sell_2_exit_resizes_8_to_6(self, logs):
        b = InvBroker()
        s, st = _position(b, lots=8)
        b.held = 6
        _external_sell(s, 2)                                           # 策略外賣 2 張
        assert st.filled_lots == 8                                     # 帳上不調整
        assert any(r.levelno == logging.WARNING and "策略外賣出成交 2 張" in r.getMessage()
                   for r in logs.records)
        s._exit_worker(SYM, "mkt_queue_gone")
        assert _lots(b.sell_attempts) == [8, 6]                        # 第一筆照原張數送 (不預先封頂)
        assert b.placed[-1] == ("limit_sell", SYM, DOWN, 6)
        assert b.sellable_calls == [SYM]                               # 只查一次
        assert st.exited is True and st.sell_failed is False
        assert st.exit_fail_cycles == 0 and st.exit_cooldown_until == 0.0
        sell_rows = [r for r in s.order_log.values() if r["action"] == "sell"]
        assert [r["lots"] for r in sell_rows] == [6]
        assert _crit(logs) == []

    def test_node4_message_resizes_10_to_7(self):
        b = InvBroker(short_msg=MSG_NODE4)
        s, st = _position(b, lots=10)
        b.held = 7
        _external_sell(s, 3)
        s._exit_worker(SYM, "mkt_queue_gone")
        assert _lots(b.sell_attempts) == [10, 7]
        assert st.exited is True and st.sell_failed is False

    def test_first_send_is_never_pre_capped(self):
        # 庫存查詢可能落後剛成交 → 正常出場不查、照帳上張數送
        b = InvBroker(sellable=0)
        s, st = _position(b, lots=8)
        b.held = 8
        s._exit_worker(SYM, "mkt_queue_gone")
        assert _lots(b.sell_attempts) == [8] and b.sellable_calls == []

    def test_resize_on_last_attempt_still_sends_resized(self):
        b = InvBroker()
        s, st = _position(b, lots=8)
        b.held = 6
        assert s._sell_position(SYM, st, 8, "test", max_tries=1) is True
        assert _lots(b.sell_attempts) == [8, 6]

    def test_close_all_resizes_and_counts_sold(self):
        b = InvBroker()
        s, st = _position(b, lots=8)
        b.held = 5
        assert s.close_all() == 1
        assert _lots(b.sell_attempts) == [8, 5] and st.exited is True

    def test_query_shows_enough_keeps_same_lots(self):
        # 查詢顯示足量 (查詢落後) → 不改張數,照原重試
        b = InvBroker(sellable=8)
        s, st = _position(b, lots=8)
        b.held = 6
        assert s._sell_position(SYM, st, 8, "test", max_tries=3) is False
        assert _lots(b.sell_attempts) == [8, 8, 8]

    def test_non_inventory_reject_does_not_query(self):
        b = InvBroker()
        s, st = _position(b, lots=8)
        b.other_reject = "連線逾時"
        assert s._sell_position(SYM, st, 8, "test", max_tries=2) is False
        assert b.sellable_calls == [] and _lots(b.sell_attempts) == [8, 8]

    def test_fatal_reject_still_stops_immediately_without_query(self):
        b = InvBroker()
        s, st = _position(b, lots=8)
        b.other_reject = "證券委託觸及價格穩定措施上、下限價格 (庫存不足)"   # 致命關鍵字優先
        assert s._sell_position(SYM, st, 8, "test") is False
        assert _lots(b.sell_attempts) == [8] and b.sellable_calls == []


# ═══ 3. 券商已無可賣 (nothing-left) ═══════════════════════════════════════════

class TestNothingLeft:
    def test_all_sold_externally_stops_after_two_confirmations(self, logs, confirm_now):
        b = InvBroker()
        s, st = _position(b, lots=8)
        b.held = 0
        _external_sell(s, 8)
        s._exit_worker(SYM, "mkt_queue_gone")
        assert _lots(b.sell_attempts) == [8, 8]                        # 第二次確認後停,非 8 次
        assert b.sellable_calls == [SYM, SYM]
        assert st.exited is True and st.sell_failed is False
        assert st.exit_fail_cycles == 0 and st.exit_cooldown_until == 0.0
        crit = _crit(logs, "無今日股")
        assert len(crit) == 1 and "策略外" in crit[0].getMessage()
        assert not _crit(logs, "連續失敗")
        # 之後的出場訊號: has_exposure False → trader 不再觸發;直呼 exit_position 也不再送單
        assert s.has_exposure(SYM) is False
        s.exit_position(SYM, "mkt_queue_gone")
        _real_time.sleep(0.2)
        assert len(b.sell_attempts) == 2
        # 2026-09-15 審查: nothing-left 仍列入 13:24 隔日賣檔 (判定錯誤時帳上部位不可從隔日賣消失);
        # 券商真 0 張 → 隔早 refresh_overnight_inventory 以庫存清掉 (見 TestNothingLeftCarriedOvernight)
        assert st.exit_no_sellable is True
        assert {c["symbol"]: c["lots"] for c in s.get_overnight_candidates()} == {SYM: 8}

    def test_sell_position_returns_nothing_left_sentinel(self, confirm_now):
        b = InvBroker(sellable=0)
        s, st = _position(b, lots=8)
        b.held = 0
        res = s._sell_position(SYM, st, 8, "test")
        assert res == ts_mod.SELL_NOTHING_LEFT and res is not True and bool(res)

    def test_single_zero_reading_is_not_trusted(self):
        # 第一次查 0 (查詢落後),第二次查到 6 → 改賣 6,不誤判 nothing-left
        b = InvBroker(sellable=[0, 6])
        s, st = _position(b, lots=8)
        b.held = 6
        assert s._sell_position(SYM, st, 8, "test") is True
        assert _lots(b.sell_attempts) == [8, 8, 6]

    def test_zero_then_query_failure_resets_confirmation(self, confirm_now):
        b = InvBroker(sellable=[0, OrderLookupError(MSG_RATE_LIMIT), 0, 0])
        s, st = _position(b, lots=8)
        b.held = 0
        assert s._sell_position(SYM, st, 8, "test", max_tries=3) is False   # 0 → 未知 → 0 : 未連續兩次
        assert _lots(b.sell_attempts) == [8, 8, 8]

    def test_max_tries_one_cannot_confirm_returns_false(self, confirm_now):
        b = InvBroker(sellable=0)
        s, st = _position(b, lots=8)
        b.held = 0
        assert s._sell_position(SYM, st, 8, "test", max_tries=1) is False

    def test_late_fill_worker_nothing_left(self, logs, confirm_now):
        b = InvBroker()
        s, st = _position(b, lots=8)
        st.exited = True
        st.sell_failed = True
        b.held = 0
        s._late_fill_exit_worker(SYM, 2, "late_fill_after_exit")
        assert _lots(b.sell_attempts) == [2, 2]
        assert st.sell_failed is False and st.exited is True and st.exit_no_sellable is True
        assert len(_crit(logs, "無今日股")) == 1

    def test_close_all_nothing_left_keeps_exited_not_counted(self, logs, monkeypatch):
        # 緊急全平只試 3 次 (退避 0) → 時間跨度門檻歸零才可能確認
        monkeypatch.setattr(ts_mod, "_SELLABLE_ZERO_CONFIRM_SEC", 0.0)
        b = InvBroker()
        s, st = _position(b, lots=8)
        b.held = 0
        assert s.close_all() == 0
        assert st.exited is True and st.sell_failed is False
        assert len(_crit(logs, "無今日股")) == 1


# ═══ 4. 查詢失敗 / 無嚴格查詢 → 原行為 ═══════════════════════════════════════

class TestQueryUnavailableKeepsOldBehavior:
    def test_query_failure_retries_same_lots_then_cooldown(self, logs):
        b = InvBroker(sellable=OrderLookupError(MSG_RATE_LIMIT))
        s, st = _position(b, lots=8)
        b.held = 6
        s._exit_worker(SYM, "mkt_queue_gone")
        assert _lots(b.sell_attempts) == [8] * ts_mod.DEFAULT_SELL_MAX_TRIES
        assert st.exited is False and st.sell_failed is True           # 部位保護仍在 (無永久鎖)
        assert st.exit_fail_cycles == 1
        left = st.exit_cooldown_until - ts_mod.time.monotonic()
        assert 25 < left <= 30
        assert len(_crit(logs, "連續失敗")) == 1

    def test_query_timeout_is_unknown(self, monkeypatch):
        monkeypatch.setattr(ts_mod, "_SELLABLE_QUERY_TIMEOUT_SEC", 0.05)
        b = InvBroker()
        release = threading.Event()

        def _slow(symbol):
            release.wait(2.0)
            return 0
        b.get_sellable_lots = _slow
        s, st = _position(b, lots=8)
        b.held = 0
        try:
            assert s._sell_position(SYM, st, 8, "test", max_tries=2) is False   # 逾時 → 未知,不當 0
        finally:
            release.set()
        assert _lots(b.sell_attempts) == [8, 8]

    @pytest.mark.parametrize("bad", [-1, True, "x", None])
    def test_bad_query_values_are_unknown(self, bad):
        b = InvBroker()
        b.get_sellable_lots = lambda symbol: bad
        s, st = _position(b, lots=8)
        b.held = 0
        assert s._sell_position(SYM, st, 8, "test", max_tries=2) is False
        assert _lots(b.sell_attempts) == [8, 8]

    def test_broker_without_strict_query_unchanged(self):
        b = InvBroker(with_query=False)
        s, st = _position(b, lots=8)
        b.held = 6
        assert s._sell_position(SYM, st, 8, "test", max_tries=3) is False
        assert _lots(b.sell_attempts) == [8, 8, 8]


# ═══ 5. 隔日賣清單剩餘張數要扣掉 ═════════════════════════════════════════════

class TestOvernightReserve:
    def _with_overnight(self, b, o_lots=5, reconciled=True, sold=0):
        s, st = _position(b, lots=8)
        s.load_overnight([{"symbol": SYM, "lots": o_lots, "avg_cost": 80.0}])
        o = s.overnight[SYM]
        o["reconciled"] = reconciled
        o["sold_lots"] = sold
        return s, st, o

    def test_overnight_remaining_not_sold_by_today_exit(self):
        b = InvBroker()
        s, st, o = self._with_overnight(b, o_lots=5)
        b.held = 2 + 5                                                 # 今日剩 2 張 + 昨日 5 張
        s._exit_worker(SYM, "mkt_queue_gone")
        assert _lots(b.sell_attempts) == [8, 2]                        # 只賣今日的 2 張

    def test_only_overnight_lots_left_is_unknown_not_nothing_left(self, confirm_now):
        # 2026-09-15 審查 B4: 可委託 >0 只是被隔日保留扣成 0 → 未知 (保留量可能失真),不判 nothing-left;
        # 走原重試 + 冷卻,部位仍受保護且列入隔日賣
        b = InvBroker()
        s, st, o = self._with_overnight(b, o_lots=5)
        b.held = 5
        s._exit_worker(SYM, "mkt_queue_gone")
        assert _lots(b.sell_attempts) == [8] * ts_mod.DEFAULT_SELL_MAX_TRIES
        assert st.exited is False and st.sell_failed is True and st.exit_no_sellable is False
        assert SYM in {c["symbol"] for c in s.get_overnight_candidates()}

    def test_intraday_refresh_does_not_inflate_reserve(self):
        # 審查 S4: 昨日 5 張 (早上對帳) + 今日策略買 8 → 盤中重連 refresh 把 lots 覆寫成 13 (含今日);
        # 策略外賣 6 → 券商剩 7 (今日 2 + 昨日 5) → 今日出場改賣 2 張,不可被灌大的保留量吃掉
        b = InvBroker()
        s, st = _position(b, lots=8)
        s.load_overnight([{"symbol": SYM, "lots": 5, "avg_cost": 80.0}])
        b.inventories = [{"symbol": SYM, "lots": 5, "order_type": "Stock"}]
        s.refresh_overnight_inventory()
        b.inventories = [{"symbol": SYM, "lots": 13, "order_type": "Stock"}]
        s.refresh_overnight_inventory()
        o = s.overnight[SYM]
        assert o["lots"] == 13 and o["lots_open"] == 5
        _external_sell(s, 6)
        b.held = 7
        s._exit_worker(SYM, "mkt_queue_gone")
        assert _lots(b.sell_attempts) == [8, 2]
        assert st.exited is True and st.sell_failed is False and st.exit_no_sellable is False
        with s._lock:
            assert s._overnight_reserved_lots_locked(SYM) == 5

    def test_unreconciled_overnight_lots_not_reserved(self):
        # 未對帳張數來自昨日檔案可能失真 (09-14 記 8 實 0) → 不保留,免誤判無可賣
        b = InvBroker()
        s, st, o = self._with_overnight(b, o_lots=8, reconciled=False)
        b.held = 6
        s._exit_worker(SYM, "mkt_queue_gone")
        assert _lots(b.sell_attempts) == [8, 6]

    def test_reserve_formula(self):
        b = InvBroker()
        s, st, o = self._with_overnight(b, o_lots=5, sold=1)
        with s._lock:
            assert s._overnight_reserved_lots_locked(SYM) == 4
            o["skip"] = True
            assert s._overnight_reserved_lots_locked(SYM) == 4         # 使用者續抱 → 仍保留
            s._log_order("ON1", SYM, "sell", "overnight_sell", 4, DOWN)
            s.order_log["ON1"]["filled_lots"] = 1
            o["sell_order_no"] = "ON1"
            o["sell_placed"] = True
            assert s._overnight_reserved_lots_locked(SYM) == 1         # 在途賣單未成交 3 張已佔可賣
            s.order_log["ON1"]["status"] = "rejected"
            assert s._overnight_reserved_lots_locked(SYM) == 4
            assert s._overnight_reserved_lots_locked("0000") == 0
            s.order_log["ON1"]["status"] = "pending"
            assert s._overnight_reserved_lots_locked(SYM, for_balance=True) == 4   # 整股餘額不因在途賣單減少
            o["lots_open"] = 3                                                     # 第一次對帳值封頂
            assert s._overnight_reserved_lots_locked(SYM, for_balance=True) == 2
            assert s._overnight_reserved_lots_locked(SYM) == 0


# ═══ 6. 跨 tick 風暴: 出場失敗冷卻 (exit_position 執行) ════════════════════════

class TestExitCooldown:
    def _failed_cycle(self, logs=None):
        b = InvBroker(sellable=OrderLookupError(MSG_RATE_LIMIT))
        s, st = _position(b, lots=8)
        b.held = 6
        s._exit_worker(SYM, "mkt_queue_gone")
        assert st.exited is False and st.exit_fail_cycles == 1
        return s, st, b

    def test_exit_position_suppressed_during_cooldown_logs_once(self, logs):
        s, st, b = self._failed_cycle()
        n = len(b.sell_attempts)
        for _ in range(20):                                            # trader 每 tick 都進來
            s.exit_position(SYM, "mkt_queue_gone")
        _real_time.sleep(0.3)
        assert len(b.sell_attempts) == n                               # 冷卻中不起 thread
        assert not [t for t in threading.enumerate() if t.name == f"exit-{SYM}" and t.is_alive()]
        skip_logs = [r for r in logs.records if "冷卻中" in r.getMessage()]
        assert len(skip_logs) == 1 and skip_logs[0].levelno == logging.INFO

    def test_cooldown_expiry_refires_and_success_resets(self):
        s, st, b = self._failed_cycle()
        b.sellable = None                                              # 查詢恢復
        st.exit_cooldown_until = ts_mod.time.monotonic() - 0.01        # 冷卻到期
        s.exit_position(SYM, "mkt_queue_gone")
        assert wait_until(lambda: st.exited and b.placed and b.placed[-1][0] == "limit_sell"
                          and st.exit_cooldown_until == 0.0, 3.0)
        assert b.placed[-1] == ("limit_sell", SYM, DOWN, 6)
        # 送出成功只清冷卻/失敗旗標;輪次保留到賣單真的成交 (審查: 送出後被非同步退才能逐輪升級)
        assert st.exit_cooldown_until == 0.0 and st.sell_failed is False and st.exit_fail_cycles == 1
        sell_no = next(no for no, r in s.order_log.items() if r["action"] == "sell" and r["status"] == "pending")
        s._on_fill(fill(sell_no, SYM, 6, DOWN, action="sell", filled_no="S-OK"))
        assert st.exit_fail_cycles == 0 and st.exit_cooldown_until == 0.0

    def test_cooldown_escalates_and_caps(self):
        b = InvBroker(sellable=OrderLookupError(MSG_RATE_LIMIT))
        s, st = _position(b, lots=8)
        b.held = 0
        got = []
        for _ in range(6):
            s._exit_worker(SYM, "mkt_queue_gone")                      # 直呼不受冷卻限制 (既有測試契約)
            got.append(round(st.exit_cooldown_until - ts_mod.time.monotonic()))
        assert got == [30, 60, 120, 300, 300, 300]
        assert st.exit_fail_cycles == 6 and st.exited is False         # 無永久鎖: 冷卻後仍可再觸發

    def test_direct_exit_worker_not_gated(self):
        # 既有測試 (test_sell_failure_sets_flag_and_allows_retrigger) 直呼 _exit_worker 期望立刻再觸發
        s, st, b = self._failed_cycle()
        b.sellable = None
        s._exit_worker(SYM, "mkt_queue_gone")
        assert st.exited is True and b.placed[-1] == ("limit_sell", SYM, DOWN, 6)

    def test_first_exit_never_gated(self):
        b = InvBroker()
        s, st = _position(b, lots=8)
        b.held = 8
        s.exit_position(SYM, "mkt_queue_gone")
        assert wait_until(lambda: b.placed and b.placed[-1][0] == "limit_sell", 3.0)

    def test_trader_ticks_one_failed_cycle_only(self, monkeypatch):
        # 端到端: 事故型連續出場訊號 tick — 一輪失敗 (8 筆) 後冷卻,後續 tick 不再開新一輪
        from types import SimpleNamespace as NS
        b = InvBroker(sellable=OrderLookupError(MSG_RATE_LIMIT))
        s, st = _position(b, lots=8)
        b.held = 6
        t = Trader(watchlist=[SYM], limit_ups={SYM: LU},
                   cfg=NS(bid_decline_sample_sec=60, bid_decline_minutes=5), session=s)
        t.on_book(SYM, MKT_PRESENT, [])
        deadline = _real_time.monotonic() + 1.0
        while _real_time.monotonic() < deadline:
            t.on_book(SYM, MKT_GONE, [])
            _real_time.sleep(0.01)
        assert wait_until(lambda: st.exit_fail_cycles == 1 and not st.exited, 3.0)
        for _ in range(30):
            t.on_book(SYM, MKT_GONE, [])
        _real_time.sleep(0.3)
        assert len(b.sell_attempts) == ts_mod.DEFAULT_SELL_MAX_TRIES

    def test_trader_ticks_node3_resize_two_orders_total(self):
        from types import SimpleNamespace as NS
        b = InvBroker()
        s, st = _position(b, lots=8)
        b.held = 6
        _external_sell(s, 2)
        t = Trader(watchlist=[SYM], limit_ups={SYM: LU},
                   cfg=NS(bid_decline_sample_sec=60, bid_decline_minutes=5), session=s)
        t.on_book(SYM, MKT_PRESENT, [])
        for _ in range(50):
            t.on_book(SYM, MKT_GONE, [])
        assert wait_until(lambda: st.exited and b.placed[-1][0] == "limit_sell", 3.0)
        _real_time.sleep(0.3)
        assert _lots(b.sell_attempts) == [8, 6]


# ═══ 7. 出場賣單被券商非同步退 ════════════════════════════════════════════════

class TestAsyncSellRejection:
    def _sold(self):
        b = InvBroker()
        s, st = _position(b, lots=8)
        b.held = 8
        s._exit_worker(SYM, "mkt_queue_gone")
        sell_no = next(no for no, r in s.order_log.items() if r["action"] == "sell")
        assert st.exited is True and s.order_log[sell_no]["status"] == "pending"
        return s, st, b, sell_no

    def test_rejected_exit_sell_reopens_exit_with_cooldown(self, logs):
        s, st, b, sell_no = self._sold()
        s._on_order({"order_no": sell_no, "symbol": SYM, "status": "90", "function_type": 0,
                     "error_message": MSG_NODE3})
        assert s.order_log[sell_no]["status"] == "rejected"
        assert st.exited is False and s.has_exposure(SYM) is True       # 部位重新受保護
        assert st.exit_fail_cycles == 1 and st.exit_cooldown_until > ts_mod.time.monotonic() + 25
        assert any(r.levelno == logging.ERROR and sell_no in r.getMessage() and "被券商退" in r.getMessage()
                   for r in logs.records)
        n = len(b.sell_attempts)
        s.exit_position(SYM, "mkt_queue_gone")                          # 冷卻中 → 不送
        _real_time.sleep(0.2)
        assert len(b.sell_attempts) == n
        st.exit_cooldown_until = ts_mod.time.monotonic() - 0.01
        b.held = 8                                                      # 被退的賣單不再佔可賣
        s.exit_position(SYM, "mkt_queue_gone")
        assert wait_until(lambda: len(b.sell_attempts) == n + 1 and st.exited, 3.0)
        assert b.sell_attempts[-1][2] == 8                              # 被退那筆不再算覆蓋 → 全量重賣

    def test_rejection_during_exit_in_progress_not_reopened(self):
        s, st, b, sell_no = self._sold()
        st.exit_in_progress = True
        s._on_order({"order_no": sell_no, "symbol": SYM, "function_type": 0, "error_message": "rejected"})
        assert st.exited is True and st.exit_fail_cycles == 0

    def test_duplicate_rejection_for_non_pending_sell_ignored(self):
        s, st, b, sell_no = self._sold()
        s._on_fill(fill(sell_no, SYM, 8, DOWN, action="sell", filled_no="S1"))
        assert s.order_log[sell_no]["status"] == "filled"
        s._on_order({"order_no": sell_no, "symbol": SYM, "function_type": 0, "error_message": "rejected"})
        assert st.exited is True and st.exit_fail_cycles == 0

    def test_manual_abandon_not_reopened(self):
        s, st, b, sell_no = self._sold()
        st.stopped_reason = "manual_abandon"
        s._on_order({"order_no": sell_no, "symbol": SYM, "function_type": 0, "error_message": "rejected"})
        assert st.exited is True

    def test_overnight_sell_rejection_does_not_touch_today_exit(self):
        s, st, b, _ = self._sold()
        s._log_order("ON9", SYM, "sell", "overnight_sell", 2, DOWN)
        s._on_order({"order_no": "ON9", "symbol": SYM, "function_type": 0, "error_message": "rejected"})
        assert s.order_log["ON9"]["status"] == "rejected"
        assert st.exited is True and st.exit_fail_cycles == 0

    def test_buy_rejection_path_unchanged(self):
        b = InvBroker()
        s = make_session(broker=b, total=10_000_000, per_symbol=0, sizing_mode="fixed_lots", fixed_lots=2)
        s.place_pre_orders([SYM], {SYM: LU})
        st = s.trades[SYM]
        no = st.order_no
        s._on_order({"order_no": no, "symbol": SYM, "function_type": 0, "error_message": "9049"})
        assert st.order_status == "rejected" and st.order_no == "" and s.budget_used == 0
        assert st.exited is False and st.exit_fail_cycles == 0


# ═══ 8. 策略外賣出成交告警 ═══════════════════════════════════════════════════

class TestExternalSellWarning:
    def test_warning_only_for_held_symbol_sell(self, logs):
        b = InvBroker()
        s, st = _position(b, lots=8)
        _external_sell(s, 2, no="EXT-A")
        s._on_fill(fill("EXT-B", SYM, 1, LU, action="buy", filled_no="FB"))     # 策略外買 → INFO
        s._on_fill(fill("EXT-C", "2330", 1, 500.0, action="sell", filled_no="FC"))  # 未持有 → INFO
        warns = [r for r in logs.records if "策略外賣出成交" in r.getMessage()]
        assert len(warns) == 1 and warns[0].levelno == logging.WARNING
        assert "2 張" in warns[0].getMessage() and "8 張" in warns[0].getMessage()
        infos = [r for r in logs.records if "非策略單成交" in r.getMessage()]
        assert len(infos) == 2 and all(r.levelno == logging.INFO for r in infos)
        assert st.filled_lots == 8

    def test_no_warning_when_strategy_position_flat(self, logs):
        b = InvBroker()
        s, st = _position(b, lots=8)
        st.filled_lots = 0
        _external_sell(s, 2)
        assert not [r for r in logs.records if "策略外賣出成交" in r.getMessage()]


# ═══ 9. 審查 2026-09-15: nothing-left 只在「確定部位已不在」時成立 ═════════════════

MSG_BROKER_DOWN = "券商未連線或連線不健康"


class TestNothingLeftOnlyWhenSharesGone:
    def test_no_daytrade_stock_node4_message_keeps_old_failure_path(self, logs, confirm_now):
        # 審查 B1 探針: 禁現沖股今日買 5 張今日本就不能賣 → 券商回 node4 同文案、可委託 0,但帳上 5 張仍在
        b = InvBroker(held=0, short_msg=MSG_NODE4, sellable=0, balance=5)
        s, st = _position(b, lots=5)
        st.day_tradable = False
        s._exit_worker(SYM, "mkt_queue_gone")
        assert _lots(b.sell_attempts) == [5] * ts_mod.DEFAULT_SELL_MAX_TRIES
        assert b.sellable_calls == []                                  # 禁現沖 → 整段重算跳過
        assert st.exited is False and st.sell_failed is True and st.exit_no_sellable is False
        assert st.exit_fail_cycles == 1
        assert {c["symbol"]: c["lots"] for c in s.get_overnight_candidates()} == {SYM: 5}
        assert not _crit(logs, "無今日股")
        assert len(_crit(logs, "連續失敗")) == 1

    def test_no_daytrade_never_queries_even_if_balance_zero(self, confirm_now):
        b = InvBroker(held=0, short_msg=MSG_NODE4, sellable=0, balance=0)
        s, st = _position(b, lots=5)
        st.day_tradable = False
        assert s._sell_position(SYM, st, 5, "test", max_tries=3) is False
        assert b.sellable_calls == [] and _lots(b.sell_attempts) == [5, 5, 5]

    def test_tradable_zero_but_balance_held_is_unknown(self, logs, confirm_now):
        # 可委託 0 但整股餘額 5 (當沖資格不符 / 被其他賣單佔用) → 未知,不 resize、不判 nothing-left
        b = InvBroker(held=0, short_msg=MSG_NODE4, sellable=0, balance=5)
        s, st = _position(b, lots=5)
        s._exit_worker(SYM, "mkt_queue_gone")
        assert _lots(b.sell_attempts) == [5] * ts_mod.DEFAULT_SELL_MAX_TRIES
        assert len(b.sellable_calls) == ts_mod.DEFAULT_SELL_MAX_TRIES
        assert st.exited is False and st.sell_failed is True and st.exit_no_sellable is False
        assert SYM in {c["symbol"] for c in s.get_overnight_candidates()}
        assert not _crit(logs, "無今日股")

    def test_legacy_query_without_balance_can_resize_but_never_nothing_left(self, confirm_now):
        b = InvBroker(sellable=0)
        b.get_sellable_position = None                                 # 只有 get_sellable_lots (無整股餘額)
        s, st = _position(b, lots=8)
        b.held = 0
        assert s._sell_position(SYM, st, 8, "test", max_tries=3) is False
        assert _lots(b.sell_attempts) == [8, 8, 8]
        b2 = InvBroker()
        b2.get_sellable_position = None
        s2, st2 = _position(b2, lots=8)
        b2.held = 6
        assert s2._sell_position(SYM, st2, 8, "test") is True
        assert _lots(b2.sell_attempts) == [8, 6]

    @pytest.mark.parametrize("bad", [{"tradable": 0}, {"tradable": 0, "balance": None},
                                     {"tradable": 0, "balance": -1}, {"tradable": 0, "balance": True},
                                     (0, 0), None])
    def test_malformed_position_is_unknown(self, bad, confirm_now):
        b = InvBroker()
        b.get_sellable_position = lambda symbol: bad
        s, st = _position(b, lots=8)
        b.held = 0
        assert s._sell_position(SYM, st, 8, "test", max_tries=3) is False
        assert _lots(b.sell_attempts) == [8, 8, 8]

    def test_balance_query_failure_is_unknown(self, confirm_now):
        b = InvBroker(sellable=0, balance=OrderLookupError("inventories 例外: timeout"))
        s, st = _position(b, lots=8)
        b.held = 0
        assert s._sell_position(SYM, st, 8, "test", max_tries=3) is False
        assert _lots(b.sell_attempts) == [8, 8, 8]


class TestNothingLeftConfirmWindow:
    def test_two_quick_zero_reads_are_not_enough(self):
        # 審查 S1: 兩次讀 0 只隔一次退避 (~0.2 s) → 不可判 nothing-left (成交後庫存後台可能短暫落後)
        assert ts_mod._SELLABLE_ZERO_CONFIRM_SEC >= 5.0
        b = InvBroker(held=0, short_msg=MSG_NODE4, sellable=0)
        s, st = _position(b, lots=8)
        assert s._sell_position(SYM, st, 8, "test") is False           # 測試退避為 0 → 跨不到 5 s
        assert _lots(b.sell_attempts) == [8] * ts_mod.DEFAULT_SELL_MAX_TRIES

    def test_production_backoff_confirms_only_after_window(self, monkeypatch, logs):
        b = InvBroker(held=0, short_msg=MSG_NODE4, sellable=0)
        s, st = _position(b, lots=8)
        clock = install_fake_clock(monkeypatch)                        # 退避 sleep 只推進虛擬時鐘
        s.order_min_interval = 0.2                                     # 正式環境 ORDER_MIN_INTERVAL_SEC
        t0 = clock.monotonic()
        s._exit_worker(SYM, "mkt_queue_gone")
        # 退避 0.2/0.4/0.8/1.6/3.2 → 第 6 次送單 (距第一次 ≈6.2 s) 才滿足「≥2 次且跨 ≥5 s」
        assert _lots(b.sell_attempts) == [8] * 6
        assert clock.monotonic() - t0 >= 5.0
        assert st.exited is True and st.exit_no_sellable is True and st.sell_failed is False
        assert s.has_exposure(SYM) is False
        assert {c["symbol"]: c["lots"] for c in s.get_overnight_candidates()} == {SYM: 8}
        assert len(_crit(logs, "無今日股")) == 1


class TestNothingLeftCarriedOvernight:
    def test_next_morning_reconcile_decides(self, confirm_now):
        b = InvBroker()
        s, st = _position(b, lots=8)
        b.held = 0
        _external_sell(s, 8)
        s._exit_worker(SYM, "mkt_queue_gone")
        assert st.exit_no_sellable is True
        cands = s.get_overnight_candidates()
        assert {c["symbol"]: c["lots"] for c in cands} == {SYM: 8}
        # 隔天: 券商真 0 張 → 對帳移除 (09-14 x8 幽靈部位也這樣清)
        b2 = InvBroker()
        s2 = make_session(broker=b2, date="2026-09-16")
        s2.load_overnight(cands)
        s2.refresh_overnight_inventory()
        assert SYM not in s2.overnight
        # 隔天: 判定錯誤 (假 0,帳上其實有 8) → 照常對帳到 8 張,自動隔日賣
        b3 = InvBroker()
        b3.inventories = [{"symbol": SYM, "lots": 8, "order_type": "Stock"}]
        s3 = make_session(broker=b3, date="2026-09-16")
        s3.load_overnight(cands)
        s3.refresh_overnight_inventory()
        assert s3.overnight[SYM]["lots"] == 8 and s3.overnight[SYM]["reconciled"] is True


# ═══ 10. 審查 2026-09-15: 冷卻升級 / 競態 / 斷線 ═══════════════════════════════════

def _pending_sells(s):
    return [no for no, r in s.order_log.items() if r["action"] == "sell" and r["status"] == "pending"]


class TestCooldownEscalationAndRaces:
    def test_async_rejects_escalate_30_60_120_300(self):
        b = InvBroker()
        s, st = _position(b, lots=8)
        b.held = 8
        s._exit_worker(SYM, "mkt_queue_gone")
        got = []
        for i in range(4):
            no = _pending_sells(s)[-1]
            s._on_order({"order_no": no, "symbol": SYM, "status": "90", "function_type": 0,
                         "error_message": "委託價格超過漲跌停範圍"})
            got.append((st.exit_fail_cycles, round(st.exit_cooldown_until - ts_mod.time.monotonic())))
            assert st.exited is False and s.has_exposure(SYM) is True
            st.exit_cooldown_until = ts_mod.time.monotonic() - 0.01    # 冷卻到期
            b.held = 8
            n = len(b.sell_attempts)
            s.exit_position(SYM, "mkt_queue_gone")
            assert wait_until(lambda: len(b.sell_attempts) == n + 1 and st.exited
                              and st.exit_cooldown_until == 0.0, 3.0)
            assert st.exit_fail_cycles == i + 1                        # 送出成功不歸零
        assert got == [(1, 30), (2, 60), (3, 120), (4, 300)]

    def test_async_reject_racing_send_success_keeps_cooldown(self):
        # 審查 P2: 券商退單回報落在 _log_order 與送單成功路徑之間 → 冷卻不可被清掉、下一個 tick 不可重送
        b = InvBroker()
        s, st = _position(b, lots=8)
        b.held = 8
        orig_log = s._log_order

        def log_then_reject(no, symbol, action, kind, lots, price):
            orig_log(no, symbol, action, kind, lots, price)
            if action == "sell":
                s._log_order = orig_log                                # 只注入一次
                s._on_order({"order_no": no, "symbol": symbol, "status": "90", "function_type": 0,
                             "error_message": "委託價格超過漲跌停範圍"})
        s._log_order = log_then_reject
        s._exit_worker(SYM, "mkt_queue_gone")
        assert st.exited is False and s.has_exposure(SYM) is True
        assert st.exit_fail_cycles == 1
        assert 25 < st.exit_cooldown_until - ts_mod.time.monotonic() <= 30
        n = len(b.sell_attempts)
        s.exit_position(SYM, "mkt_queue_gone")
        _real_time.sleep(0.3)
        assert len(b.sell_attempts) == n


class TestDisconnectCooldown:
    def test_cycle_failed_while_broker_down_does_not_cool_down(self, logs):
        b = InvBroker()
        s, st = _position(b, lots=8)
        b.held = 8
        b.healthy = False
        b.other_reject = MSG_BROKER_DOWN
        s._exit_worker(SYM, "mkt_queue_gone")
        assert st.exited is False and st.sell_failed is True
        assert st.exit_fail_cycles == 0 and st.exit_cooldown_until == 0.0
        assert len(_crit(logs, "不計出場失敗輪次")) == 1
        b.healthy = True                                               # 重連
        b.other_reject = None
        s.exit_position(SYM, "mkt_queue_gone")
        assert wait_until(lambda: b.placed and b.placed[-1] == ("limit_sell", SYM, DOWN, 8), 3.0)

    def test_reconnect_clears_cooldown_keeps_cycles(self, logs):
        b = InvBroker(sellable=OrderLookupError(MSG_RATE_LIMIT))
        s, st = _position(b, lots=8)
        b.held = 6
        s._exit_worker(SYM, "mkt_queue_gone")
        assert st.exit_fail_cycles == 1 and st.exit_cooldown_until > ts_mod.time.monotonic() + 25
        s._on_broker_reconnected()
        assert st.exit_cooldown_until == 0.0 and st.exit_fail_cycles == 1
        assert any("清除出場失敗冷卻" in r.getMessage() for r in logs.records)
        b.sellable = None
        s.exit_position(SYM, "mkt_queue_gone")                         # 重連後出場訊號立刻送單
        assert wait_until(lambda: b.placed and b.placed[-1] == ("limit_sell", SYM, DOWN, 6), 3.0)
