"""broker.RealOrderClient 撤單查詢語意 (2026-09-09 node3「管線多送市價單未撤」事故 A1/A1b)。

根因: 舊 _find_order_obj 把「get_order_results 回 is_success=False (5/s 流量控管)」與「清單無此書號」
一視同仁、丟掉 result.message;cancel() 對 None 靜默 return → 上層標 cancelled → 4 筆隱形裸單。
修法契約:
  - 查詢失敗 (is_success 非 True / SDK 例外) → OrderLookupError (訊息含富邦原文),CSV FAIL:QUERY:<msg>
  - 查詢成功但無此書號 → OrderNotFound,CSV FAIL:NOT_FOUND;**絕不靜默 return**
  - 所有 get_order_results 過 broker 內 5/s 閘門 (最小間隔 ≥0.2 s;8 thread 併發任一 1 s 窗 ≤5 次)
  - 每次查詢一行 log「order_results ok=<bool> n=<筆數> ms=<耗時> msg=<message或->」
  - get_order_snapshot / cancel_by_obj / _find_order_obj 三元組 / get_pending_orders 含 user_def+after_qty
  - _handle_order 的 err 分支不再 return (撤單失敗回報走 err 參數)
用假 sdk (SimpleNamespace) 直接驗,不 login。
"""
import csv
import logging
import re
import threading
import time
from types import SimpleNamespace

import pytest

import broker as broker_mod
from broker import RealOrderClient, OrderLookupError, OrderNotFound

RATE_MSG = "Login Error, 業務系統流量控管"
ALREADY_MSG = "[115]證券委託目前狀態取消單已不允許取消交易"


def _res(ok=True, data=None, message=""):
    return SimpleNamespace(is_success=ok, data=list(data or []), message=message)


def _order(no, symbol="5386", buy=True, qty=1000, filled=0, status=10,
           user_def="hitlimit", after=None, **extra):
    kw = dict(order_no=no, stock_no=symbol, buy_sell="Buy" if buy else "Sell",
              quantity=qty, filled_qty=filled, status=status, user_def=user_def)
    if after is not None:
        kw["after_qty"] = after
    kw.update(extra)
    return SimpleNamespace(**kw)


class FakeSDK:
    """sdk.stock.get_order_results / cancel_order 假物件。results 依序回 (用完重複最後一個)。"""

    def __init__(self, results=None, cancel_ok=True, cancel_msg=""):
        self.results = list(results or [])
        self.query_ts = []
        self.cancel_calls = []
        self.cancel_ok = cancel_ok
        self.cancel_msg = cancel_msg
        self.raise_on_query = None
        self._lk = threading.Lock()
        self.stock = SimpleNamespace(get_order_results=self._get, cancel_order=self._cancel)

    def _get(self, account):
        with self._lk:
            self.query_ts.append(time.perf_counter())    # 與 broker 閘門同一時鐘 (Windows monotonic 只有 15.6 ms)
            if self.raise_on_query is not None:
                raise self.raise_on_query
            if len(self.results) > 1:
                return self.results.pop(0)
            return self.results[0] if self.results else _res(True, [])

    def _cancel(self, account, obj):
        with self._lk:
            self.cancel_calls.append(obj)
        return _res(self.cancel_ok, [], self.cancel_msg)


@pytest.fixture
def client(tmp_path):
    c = RealOrderClient(tmp_path / "orders.csv")
    c.account = object()
    c.connected = True
    c.healthy = True
    yield c
    c.close()


def _csv_rows(c) -> list:
    with open(c.log_path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _cancel_rows(c) -> list:
    return [r for r in _csv_rows(c) if r["action"] == "CANCEL"]


# ═══ cancel() facade: 查詢失敗 vs 查無 ═══════════════════════════

class TestCancelLookup:
    def test_rate_limited_twice_then_third_found(self, client, caplog):
        # 前 2 次查詢被流量控管 (is_success=False,非例外) → OrderLookupError 帶原文;第 3 次含書號 → 撤成功
        caplog.set_level(logging.INFO, logger="broker")
        sdk = FakeSDK([_res(False, [], RATE_MSG), _res(False, [], RATE_MSG),
                       _res(True, [_order("KT08n")])])
        client.sdk = sdk
        for _ in range(2):
            with pytest.raises(OrderLookupError) as ei:
                client.cancel("KT08n", "5386", reason="chase_extra")
            assert "業務系統流量控管" in str(ei.value)
            assert sdk.cancel_calls == []                     # 查詢失敗絕不送撤單
        client.cancel("KT08n", "5386", reason="chase_extra")
        assert len(sdk.cancel_calls) == 1                     # cancel_order 恰 1 次
        assert sdk.cancel_calls[0].order_no == "KT08n"
        assert "業務系統流量控管" in caplog.text               # message 不再被丟掉
        rows = _cancel_rows(client)
        assert [r["extra"][:11] for r in rows[:2]] == ["FAIL:QUERY:", "FAIL:QUERY:"]
        assert all("流量控管" in r["extra"] for r in rows[:2])
        assert rows[2]["extra"] == "chase_extra" and rows[2]["order_id"] == "KT08n"

    def test_not_found_raises_and_writes_csv(self, client, caplog):
        # 全程查無 → OrderNotFound + CSV FAIL:NOT_FOUND;不送撤單;舊的靜默訊息不再出現
        caplog.set_level(logging.INFO, logger="broker")
        sdk = FakeSDK([_res(True, [_order("OTHER")])])
        client.sdk = sdk
        with pytest.raises(OrderNotFound):
            client.cancel("KT08n", "5386", reason="chase_extra")
        assert sdk.cancel_calls == []
        rows = _cancel_rows(client)
        assert len(rows) == 1 and rows[0]["extra"] == "FAIL:NOT_FOUND"
        assert rows[0]["order_id"] == "KT08n" and rows[0]["symbol"] == "5386"
        assert "查無委託 (可能已成交/已撤)" not in caplog.text   # 2026-09-09 誤導訊息已移除

    def test_sdk_exception_raises_lookup_error(self, client):
        sdk = FakeSDK()
        sdk.raise_on_query = RuntimeError("socket closed")
        client.sdk = sdk
        with pytest.raises(OrderLookupError) as ei:
            client.cancel("KT08n", "5386", reason="x")
        assert "socket closed" in str(ei.value)
        rows = _cancel_rows(client)
        assert len(rows) == 1 and rows[0]["extra"].startswith("FAIL:QUERY:")

    def test_not_found_is_not_lookup_error(self, client):
        # 兩種例外可區分 (session 據此決定「重試」vs「不計 attempts 退避」)
        client.sdk = FakeSDK([_res(True, [])])
        with pytest.raises(OrderNotFound) as ei:
            client.cancel("X1")
        assert not isinstance(ei.value, OrderLookupError)
        assert issubclass(OrderNotFound, RuntimeError) and issubclass(OrderLookupError, RuntimeError)

    def test_cancel_rejected_by_broker_raises_with_message(self, client):
        # 查到了但券商拒撤 (已撤/已成交) → RuntimeError 帶原文 (session classify 用) + CSV FAIL:<msg>
        client.sdk = FakeSDK([_res(True, [_order("KT08n")])], cancel_ok=False, cancel_msg=ALREADY_MSG)
        with pytest.raises(RuntimeError) as ei:
            client.cancel("KT08n", "5386", reason="x")
        assert "撤單失敗 KT08n" in str(ei.value) and "取消單已不允許取消" in str(ei.value)
        assert not isinstance(ei.value, (OrderNotFound, OrderLookupError))
        rows = _cancel_rows(client)
        assert rows[-1]["extra"].startswith("FAIL:") and "取消單已不允許取消" in rows[-1]["extra"]

    def test_not_ready_raises_not_silent(self, client):
        client.sdk = FakeSDK([_res(True, [_order("K1")])])
        client.healthy = False
        with pytest.raises(RuntimeError):
            client.cancel("K1")
        assert client.sdk.cancel_calls == []

    def test_reject_carries_filled_qty_and_status(self, client):
        # 撤單被拒 → CancelRejected 帶撤單當下物件的 filled_qty/status (session 立刻補成交;審查 #11);
        # 仍是 RuntimeError 子類、訊息格式不變、不是查無/查詢失敗
        client.sdk = FakeSDK([_res(True, [_order("K1", filled=1000, status=50)])], cancel_ok=False,
                             cancel_msg="[115]證券委託目前狀態成交單已不允許取消交易")
        with pytest.raises(broker_mod.CancelRejected) as ei:
            client.cancel("K1", "5386", reason="x")
        e = ei.value
        assert isinstance(e, RuntimeError) and str(e).startswith("撤單失敗 K1:")
        assert "成交單已不允許取消" in str(e)
        assert e.filled_qty == 1000 and e.status == "50"
        assert not isinstance(e, (OrderNotFound, OrderLookupError))


# ═══ 查詢閘門 5/s ═══════════════════════════════════════════════

class TestQueryGate:
    def test_eight_threads_single_flight_and_never_exceed_five_per_second(self, client):
        # 09-09 node3: 8 條撤單 thread 0.66 s 內 8 次查詢 (≈12/s) → 流量控管。
        # 2026-09-15 single-flight (09-14 node1 清單 3.4 萬筆、每次查詢 ≈540 MB、重疊 → 當機):
        #   (a) 8 條同時查 → SDK 只被呼叫 1 次、8 條共用同一份結果
        #   (b) 各自要求新查詢 (fresh_after) 時仍過 5/s 閘門: 相鄰 ≥0.2 s、任一 1 s 滑動窗 ≤5、SDK 同時最多 1 個在飛
        sdk = FakeSDK([_res(True, [_order("K1")])])
        inflight = {"now": 0, "max": 0}
        lk = threading.Lock()
        orig_get = sdk._get

        def slow_get(account):
            with lk:
                inflight["now"] += 1
                inflight["max"] = max(inflight["max"], inflight["now"])
            try:
                time.sleep(0.3)                    # 大清單查詢耗時 → 其餘 thread 必定撞上在飛那次
                return orig_get(account)
            finally:
                with lk:
                    inflight["now"] -= 1
        sdk.stock.get_order_results = slow_get
        client.sdk = sdk
        errors, results = [], []

        def _q():
            try:
                results.append(client.get_order_snapshot())
            except Exception as e:      # noqa: BLE001
                errors.append(e)
        ths = [threading.Thread(target=_q, daemon=True) for _ in range(8)]
        for t in ths:
            t.start()
        for t in ths:
            t.join(timeout=10)
        assert not any(t.is_alive() for t in ths) and errors == []
        assert len(sdk.query_ts) == 1                                   # (a) single-flight
        assert len(results) == 8 and all([r["order_no"] for r in res] == ["K1"] for res in results)

        # (b) 每條都要新查詢 → 排隊一個一個查,仍守 5/s
        sdk.query_ts.clear()
        errors.clear()

        def _fresh():
            try:
                client._query_order_results(fresh_after=client.query_clock())
            except Exception as e:      # noqa: BLE001
                errors.append(e)
        # 仍用慢查詢 (0.3 s > 閘門間隔 0.21 s) → 8 條 fresh 呼叫端必定撞上在飛那次;計數器歸零只量 (b)
        # (審查 T1: 舊版 (b) 換回零延遲 SDK 且沿用 (a) 的計數 → 並行打 SDK 的回歸抓不到)
        with lk:
            inflight.update(now=0, max=0)
        ths = [threading.Thread(target=_fresh, daemon=True) for _ in range(8)]
        for t in ths:
            t.start()
        for t in ths:
            t.join(timeout=30)
        assert not any(t.is_alive() for t in ths) and errors == []
        ts = sorted(sdk.query_ts)
        assert 2 <= len(ts) <= 8
        assert inflight["max"] == 1                                     # (b) SDK 同時最多 1 個在飛
        gaps = [ts[i] - ts[i - 1] for i in range(1, len(ts))]
        # 0.18 (非 0.21): sleep 喚醒抖動 + 鎖內預約到實際呼叫的時差;主斷言是下方「任一 1 s 滑動窗 ≤5 次」
        assert min(gaps) >= 0.18, f"相鄰查詢間隔應 ≥0.2 s,實得 {min(gaps):.3f}"
        for i, t0 in enumerate(ts):
            n = sum(1 for t in ts if t0 <= t < t0 + 1.0)
            assert n <= 5, f"1 s 窗內 {n} 次查詢 (>5) 起點 #{i}"

    def test_gate_applies_to_all_query_paths(self, client):
        # _find_order_obj / get_filled_map / get_pending_orders / get_order_filled_lots 全過同一閘門
        sdk = FakeSDK([_res(True, [_order("K1", filled=2000)])])
        client.sdk = sdk
        client._find_order_obj("K1")
        client.get_filled_map()
        client.get_pending_orders()
        client.get_order_filled_lots("K1")
        ts = sdk.query_ts
        assert len(ts) == 4
        assert min(ts[i] - ts[i - 1] for i in range(1, 4)) >= 0.19

    def test_query_log_line_format(self, client, caplog):
        caplog.set_level(logging.INFO, logger="broker")
        client.sdk = FakeSDK([_res(True, [_order("K1"), _order("K2")])])
        client.get_order_snapshot()
        assert re.search(r"order_results ok=True n=2 ms=[\d.]+ msg=-", caplog.text), caplog.text
        caplog.clear()
        client.sdk = FakeSDK([_res(False, [], RATE_MSG)])
        with pytest.raises(OrderLookupError):
            client.get_order_snapshot()
        assert re.search(r"order_results ok=False n=0 ms=[\d.]+ msg=.*流量控管", caplog.text), caplog.text


# ═══ 快照 / 撤單物件 / 其他查詢 ═══════════════════════════════════

class TestSnapshotShape:
    def test_snapshot_fields_and_normalization(self, client):
        o1 = _order("K1", buy_sell="BSAction.Buy", status=10, filled=1000, after=2000, quantity=2000)
        o2 = SimpleNamespace(order_no="K2", symbol="2330", buy_sell="BSAction.Sell", quantity=1000,
                             filledQty=0, status="50", userDef="other")   # camel 欄位、無 after_qty
        client.sdk = FakeSDK([_res(True, [o1, o2])])
        snap = client.get_order_snapshot()
        assert [r["order_no"] for r in snap] == ["K1", "K2"]
        r1, r2 = snap
        assert r1["buy_sell"] == "Buy" and r2["buy_sell"] == "Sell"       # enum 前綴剝掉
        assert r1["quantity"] == 2000 and r1["filled_qty"] == 1000 and r1["after_qty"] == 2000
        assert r1["status"] == "10" and r1["user_def"] == "hitlimit"
        assert r1["_obj"] is o1 and r2["_obj"] is o2                      # 原 SDK 物件供 cancel_by_obj
        assert r2["symbol"] == "2330" and r2["status"] == "50" and r2["user_def"] == "other"
        assert r2["after_qty"] is None                                    # 缺欄 → None (非 0)
        assert set(r1) >= {"order_no", "symbol", "buy_sell", "quantity", "filled_qty",
                           "after_qty", "status", "user_def", "_obj"}

    def test_snapshot_failure_raises(self, client):
        client.sdk = FakeSDK([_res(False, [], RATE_MSG)])
        with pytest.raises(OrderLookupError) as ei:
            client.get_order_snapshot()
        assert "流量控管" in str(ei.value)

    def test_get_pending_orders_has_user_def_and_after_qty(self, client):
        client.sdk = FakeSDK([_res(True, [_order("K1", after=1000)])])
        rows = client.get_pending_orders()
        assert rows and rows[0]["user_def"] == "hitlimit" and rows[0]["after_qty"] == 1000
        assert "_obj" not in rows[0]

    def test_find_order_obj_triple(self, client):
        o = _order("K1")
        client.sdk = FakeSDK([_res(True, [o])])
        obj, ok, msg = client._find_order_obj("K1")
        assert obj is o and ok is True
        obj, ok, msg = client._find_order_obj("NOPE")
        assert obj is None and ok is True                                # 查無 ≠ 查詢失敗
        client.sdk = FakeSDK([_res(False, [], RATE_MSG)])
        obj, ok, msg = client._find_order_obj("K1")
        assert obj is None and ok is False and "流量控管" in msg

    def test_get_order_filled_lots_semantics(self, client):
        client.sdk = FakeSDK([_res(True, [_order("K1", filled=2000)])])
        assert client.get_order_filled_lots("K1") == 2
        assert client.get_order_filled_lots("NOPE") == -1                # 查無回 -1 (語意不變)
        client.sdk = FakeSDK([_res(False, [], RATE_MSG)])
        assert client.get_order_filled_lots("K1") == -1                  # 查詢失敗也回 -1 (保守)


class TestCancelByObj:
    def test_success_writes_csv_with_reason(self, client, caplog):
        caplog.set_level(logging.INFO, logger="broker")
        sdk = FakeSDK()
        client.sdk = sdk
        o = _order("K1")
        client.cancel_by_obj(o, "K1", "5386", reason="chase_extra")
        assert sdk.cancel_calls == [o]
        rows = _cancel_rows(client)
        assert len(rows) == 1 and rows[0]["extra"] == "chase_extra" and rows[0]["order_id"] == "K1"
        assert "CANCEL K1" in caplog.text

    def test_failure_raises_and_writes_fail_row(self, client):
        client.sdk = FakeSDK(cancel_ok=False, cancel_msg="成交單已不允許取消")
        with pytest.raises(RuntimeError) as ei:
            client.cancel_by_obj(_order("K1"), "K1", "5386", reason="x")
        assert str(ei.value).startswith("撤單失敗 K1") and "成交單已不允許取消" in str(ei.value)
        rows = _cancel_rows(client)
        assert rows[0]["extra"] == "FAIL:成交單已不允許取消"

    def test_sdk_exception_raises_and_writes_fail_row(self, client):
        def _boom(account, obj):
            raise RuntimeError("timeout")
        client.sdk = SimpleNamespace(stock=SimpleNamespace(cancel_order=_boom))
        with pytest.raises(RuntimeError) as ei:
            client.cancel_by_obj(_order("K1"), "K1", "5386")
        assert "撤單失敗 K1" in str(ei.value)
        assert _cancel_rows(client)[0]["extra"].startswith("FAIL:")


# ═══ _handle_order: err 分支照樣轉發 ═══════════════════════════════

class TestHandleOrderErrBranch:
    def test_err_branch_forwards_report(self, client, caplog):
        # 撤單失敗 (ft=30 status=39) 走 callback 的 err 參數,content 多數欄位 None → 仍組 rpt 轉發
        caplog.set_level(logging.INFO, logger="broker")
        got = []
        client.on_order = got.append
        content = SimpleNamespace(order_no="KT08n", stock_no=None, status=39, function_type=30,
                                  error_message=None, filled_qty=None, last_time=None, account=None)
        client._handle_order(ALREADY_MSG, content)
        assert len(got) == 1
        rpt = got[0]
        assert rpt["order_no"] == "KT08n" and rpt["status"] == "39"
        assert rpt["function_type"] == 30                                # 原值 (int) 保留
        assert "取消單已不允許取消" in rpt["error_message"]              # error_message = str(err)
        assert "KT08n" in caplog.text and "取消單已不允許取消" in caplog.text

    def test_err_branch_prefers_content_error_message(self, client):
        got = []
        client.on_order = got.append
        content = SimpleNamespace(order_no="K1", stock_no="5386", status=39, function_type="30",
                                  error_message="content 版原文", filled_qty=0, last_time="")
        client._handle_order("[115]err 原文", content)
        assert got[0]["error_message"] == "content 版原文"
        assert got[0]["symbol"] == "5386" and got[0]["function_type"] == "30"

    def test_normal_report_carries_function_type_and_last_time(self, client):
        got = []
        client.on_order = got.append
        content = SimpleNamespace(order_no="K1", stock_no="5386", status=10, function_type=0,
                                  error_message="", filled_qty=0, last_time="09:00:11.040")
        client._handle_order(None, content)
        assert got[0]["function_type"] == 0 and got[0]["last_time"] == "09:00:11.040"
        assert got[0]["error_message"] == "" and got[0]["status"] == "10"

    def test_handler_exception_does_not_propagate(self, client):
        client.on_order = None
        client._handle_order("err", None)     # content None → 全容錯,不炸 (SDK thread 不可拋)


class TestModuleConstants:
    def test_query_min_interval_at_least_point_two(self):
        assert getattr(broker_mod, "QUERY_MIN_INTERVAL_SEC", 0.2) >= 0.2
