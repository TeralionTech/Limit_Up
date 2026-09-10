"""共用假 broker — 券商「委託快照」語意 (2026-09-09 node3「管線多送市價單未撤」事故 A7 測試基礎)。

契約 (broker.RealOrderClient 同形):
  get_order_snapshot() -> list[dict]  每項 {order_no, symbol, buy_sell, quantity(股), filled_qty(股),
                                       after_qty(股|None), status(str), user_def(str), _obj}
                                       預設回「所有已下且未取消的單」;失敗 raise OrderLookupError
  cancel_by_obj(obj, order_no, symbol, reason)  成功記 (order_no, symbol, reason) 到 cancelled +
                                       ("cancel", order_no) 到 calls (與舊 cancel 記錄格式一致);
                                       失敗 raise FakeCancelRejected("撤單失敗 <書號>: <msg>", filled_qty=
                                       快照 filled_qty(股), status=快照 status) — 與 broker.CancelRejected 同形
                                       (RuntimeError 子類;session 以 getattr 取 filled_qty/status 補成交)
  get_order_snapshot() 未連線/不健康 (connected/healthy False) → raise OrderLookupError (同真 broker _require_ready)
  cancel(order_no, symbol, reason)     facade: 快照 → 找 obj → cancel_by_obj;查無 raise OrderNotFound
  get_pending_orders()                 = 快照去 _obj (含 user_def / after_qty)

可調旋鈕 (直接設屬性):
  snapshot_missing: set[order_no]      這些書號**不在**快照 (後檯延遲/亂序;≠ 已成交/已撤)
  snapshot_status: {order_no: str}     覆寫快照 status (富邦: 0/4/8/10 未終結、30 已撤、50 全成、90 失敗)
  snapshot_filled: {order_no: 股數}    覆寫快照 filled_qty (**股數**,1 張 = 1000)
  snapshot_after_qty: {order_no: 股數} 覆寫 after_qty
  snapshot_keep_cancelled: bool        True = 已撤的單仍留在快照 (status 30;真實券商行為)
  cancel_fail_msg: {order_no: str}     cancel_by_obj 對此書號 raise RuntimeError(含此訊息)
  fail_snapshot_times: int             接下來 N 次快照查詢 raise OrderLookupError (流量控管)
  snapshot_calls: int                  快照查詢累計次數 (含失敗的)
  lenient_cancel: bool                 True = 未經本 fake 下單的書號 (測試直接 _log_order 灌的) 視為存在
                                       → 舊測試 (test_session_money.FakeBroker) 用;新測試一律 False (嚴格)
  mirror_session: TradingSession|None  lenient 時的鏡射來源: session.order_log 裡 pending、但本 fake 沒下過
                                       的書號也放進快照 (status 預設值) — 舊測試直接 _log_order 灌的孤兒單
                                       (如 "Mextra") 才撤得到 (cancel_all_pending 已改走佇列+快照,不再叫 facade)

例外類: 優先用 broker.OrderLookupError / OrderNotFound (session 端以類名判斷,fake 即使自訂同名類也通)。
"""
import threading
import time as _real_time
from datetime import datetime as _dt
from types import SimpleNamespace

try:
    from broker import OrderLookupError, OrderNotFound
except Exception:                                   # pragma: no cover — broker 尚未就緒時的同名備援
    class OrderLookupError(RuntimeError):
        """查詢失敗 (is_success 非 True / SDK 例外)。"""

    class OrderNotFound(RuntimeError):
        """查詢成功但清單無此書號。"""

from trading_session import TradingSession


class FakeCancelRejected(RuntimeError):
    """撤單被券商拒 — 同 broker.CancelRejected 形狀 (filled_qty 股 / status),測試不必 import broker。"""

    def __init__(self, msg, filled_qty=None, status=""):
        super().__init__(msg)
        self.filled_qty = filled_qty
        self.status = status


# 富邦原文 (2026-09-09 journal / llms-full.txt)
MSG_RATE_LIMIT = "Login Error, 業務系統流量控管"
MSG_FILLED = "[115]證券委託目前狀態成交單已不允許取消交易"
MSG_PARTIAL_FILLED = "[115]證券委託目前狀態部分成交單已不允許取消交易"
MSG_ALREADY_CANCELLED = "[115]證券委託目前狀態取消單已不允許取消交易"
LIVE_STATUSES = ("0", "4", "8", "10")


class FakeSnapshotBroker:
    """券商快照語意的假 broker (撤單佇列 worker / 收盤掃單 / 撤單失敗分類 測試共用)。"""

    def __init__(self, default_status: str = "10", user_def: str = "hitlimit",
                 lenient_cancel: bool = False):
        self.connected = True
        self.healthy = True
        self.placed = []             # (kind, symbol, price, lots)
        self.cancelled = []          # (order_no, symbol, reason) — 撤單成功才記
        self.calls = []              # (kind, ref) 依序 — 驗證「市價單先於撤單」等順序
        self.book = {}               # order_no → 券商端 row (dict)
        self.cancelled_nos = set()
        self.inventories = []
        self.filled_map = {}
        # 旋鈕
        self.snapshot_missing = set()
        self.snapshot_status = {}
        self.snapshot_filled = {}
        self.snapshot_after_qty = {}
        self.snapshot_keep_cancelled = False
        self.cancel_fail_msg = {}
        self.fail_snapshot_times = 0
        self.snapshot_calls = 0
        self.cancel_by_obj_calls = []   # (order_no, symbol, reason) — 含失敗的
        self.default_status = default_status
        self.user_def = user_def
        self.lenient_cancel = lenient_cancel
        self.mirror_session = None
        self._n = 0
        self._lk = threading.RLock()

    # ─── 書號 / 券商端登記 ───
    def _next(self) -> str:
        with self._lk:
            self._n += 1
            return f"O{self._n}"

    def _register(self, order_no: str, symbol: str, buy: bool, lots: int, price,
                  status: str = None, user_def: str = None, filled_lots: int = 0,
                  after_lots: int = None) -> dict:
        row = {
            "order_no": order_no,
            "symbol": symbol,
            "buy_sell": "Buy" if buy else "Sell",
            "quantity": int(lots) * 1000,
            "filled_qty": int(filled_lots) * 1000,
            "after_qty": (int(lots) if after_lots is None else int(after_lots)) * 1000,
            "status": self.default_status if status is None else str(status),
            "user_def": self.user_def if user_def is None else str(user_def),
            "price": price,
        }
        with self._lk:
            self.book[order_no] = row
        return row

    def add_order(self, order_no: str, symbol: str, buy: bool = True, lots: int = 1,
                  status: str = None, user_def: str = None, filled_lots: int = 0,
                  after_lots: int = None, price=0) -> str:
        """直接在券商端塞一筆委託 (重啟後 order_log 沒有、手動單、他策略單 等情境)。"""
        self._register(order_no, symbol, buy, lots, price, status=status, user_def=user_def,
                       filled_lots=filled_lots, after_lots=after_lots)
        return order_no

    def fill(self, order_no: str, lots: int):
        """券商端成交 (只動快照 filled_qty;session 的 _on_fill 由測試自己餵)。"""
        with self._lk:
            row = self.book.get(order_no)
            if row is not None:
                row["filled_qty"] = min(row["quantity"], row["filled_qty"] + int(lots) * 1000)

    # ─── 下單 ───
    def place_limit_buy(self, symbol, price, lots):
        no = self._next()
        self._register(no, symbol, True, lots, price)
        self.placed.append(("limit_buy", symbol, price, lots))
        self.calls.append(("limit_buy", symbol))
        return no

    def place_market_buy(self, symbol, lots):
        no = self._next()
        self._register(no, symbol, True, lots, None)
        self.placed.append(("market_buy", symbol, None, lots))
        self.calls.append(("market_buy", symbol))
        return no

    def place_market_sell(self, symbol, lots, reason=""):
        no = self._next()
        self._register(no, symbol, False, lots, None)
        self.placed.append(("market_sell", symbol, None, lots))
        self.calls.append(("market_sell", symbol))
        return no

    def place_limit_sell(self, symbol, price, lots, reason=""):
        no = self._next()
        self._register(no, symbol, False, lots, price)
        self.placed.append(("limit_sell", symbol, price, lots))
        self.calls.append(("limit_sell", symbol))
        return no

    # ─── 快照 ───
    def _snapshot_row(self, no: str, row: dict) -> dict:
        status = self.snapshot_status.get(no)
        if status is None:
            status = "30" if no in self.cancelled_nos else row["status"]
        fields = {
            "order_no": no,
            "symbol": row["symbol"],
            "buy_sell": row["buy_sell"],
            "quantity": row["quantity"],
            "filled_qty": int(self.snapshot_filled.get(no, row["filled_qty"])),
            "after_qty": self.snapshot_after_qty.get(no, row["after_qty"]),
            "status": str(status),
            "user_def": row["user_def"],
        }
        obj = SimpleNamespace(stock_no=row["symbol"], **fields)   # 仿 SDK 物件 (cancel_by_obj 用)
        return {**fields, "_obj": obj}

    def _mirror_pending_rows(self):
        """lenient: 把 session.order_log 裡 pending 但本 fake 沒下過的書號登記進券商簿 (不取 session 鎖,
        dict 變動就重讀;只讀不寫 session)。"""
        sess = self.mirror_session
        if sess is None or not self.lenient_cancel:
            return
        for _ in range(5):
            try:
                rows = [(no, dict(r)) for no, r in sess.order_log.items()]
                break
            except RuntimeError:            # dict changed size during iteration → 重讀
                continue
        else:
            return
        with self._lk:
            for no, r in rows:
                if r.get("status") != "pending" or no in self.book:
                    continue
                self._register(no, r.get("symbol", ""), r.get("action") == "buy",
                               int(r.get("lots") or 0), r.get("price"),
                               filled_lots=int(r.get("filled_lots") or 0))

    def get_order_snapshot(self) -> list:
        self._mirror_pending_rows()
        with self._lk:
            self.snapshot_calls += 1
            if not self.connected or not self.healthy:
                raise OrderLookupError("broker 未連線/連線不健康 (交易 WS 斷線?)")   # 同真 broker _require_ready
            if self.fail_snapshot_times > 0:
                self.fail_snapshot_times -= 1
                raise OrderLookupError(MSG_RATE_LIMIT)
            out = []
            for no, row in self.book.items():
                if no in self.snapshot_missing:
                    continue
                if no in self.cancelled_nos and not self.snapshot_keep_cancelled:
                    continue
                out.append(self._snapshot_row(no, row))
            return out

    def get_pending_orders(self) -> list:
        return [{k: v for k, v in r.items() if k != "_obj"} for r in self.get_order_snapshot()]

    def snapshot_nos(self) -> set:
        """目前快照會回哪些書號 (測試斷言用;不計入 snapshot_calls)。"""
        with self._lk:
            return {no for no in self.book
                    if no not in self.snapshot_missing
                    and (no not in self.cancelled_nos or self.snapshot_keep_cancelled)}

    # ─── 撤單 ───
    def _reject(self, order_no: str, msg: str) -> FakeCancelRejected:
        """撤單被拒例外 — 帶本 fake 快照當下的 filled_qty/status (同真 broker.CancelRejected)。"""
        row = self.book.get(order_no)
        fq = None if row is None else int(self.snapshot_filled.get(order_no, row["filled_qty"]))
        stt = self.snapshot_status.get(order_no)
        if stt is None and row is not None:
            stt = "30" if order_no in self.cancelled_nos else row["status"]
        return FakeCancelRejected(f"撤單失敗 {order_no}: {msg}", filled_qty=fq,
                                  status="" if stt is None else str(stt))

    def cancel_by_obj(self, order_obj, order_no: str, symbol: str = "", reason: str = ""):
        with self._lk:
            self.cancel_by_obj_calls.append((order_no, symbol, reason))
            msg = self.cancel_fail_msg.get(order_no)
            if msg is not None:
                raise self._reject(order_no, msg)
            if order_no in self.cancelled_nos:
                raise self._reject(order_no, MSG_ALREADY_CANCELLED)
            self.cancelled_nos.add(order_no)
            row = self.book.get(order_no)
            if row is not None:
                row["status"] = "30"
            self.cancelled.append((order_no, symbol, reason))
            self.calls.append(("cancel", order_no))

    def cancel(self, order_no: str, symbol: str = "", reason: str = ""):
        """facade: 快照 → 找 obj → cancel_by_obj。查無 raise OrderNotFound、查詢失敗 raise OrderLookupError。"""
        snap = self.get_order_snapshot()
        entry = next((r for r in snap if r["order_no"] == order_no), None)
        if entry is None:
            with self._lk:
                known = order_no in self.book
            if self.lenient_cancel and order_no not in self.snapshot_missing and not known:
                # 舊測試相容: 測試直接 _log_order 灌進 session 的書號 (fake 沒下過) 視為存在
                row = self._register(order_no, symbol, True, 0, None)
                self.cancel_by_obj(self._snapshot_row(order_no, row)["_obj"], order_no, symbol, reason)
                return
            if order_no in self.cancelled_nos and not self.snapshot_keep_cancelled \
                    and order_no not in self.snapshot_missing:
                # 已由本 fake 撤過 → 券商會回「取消單已不允許取消」(冪等分類用)
                raise self._reject(order_no, MSG_ALREADY_CANCELLED)
            raise OrderNotFound(f"查無委託 {order_no} (快照內無此書號)")
        self.cancel_by_obj(entry["_obj"], order_no, symbol, reason)

    # ─── 其他查詢 ───
    def get_order_filled_lots(self, order_no):
        for r in self.get_order_snapshot():
            if r["order_no"] == order_no:
                return r["filled_qty"] // 1000
        return -1

    def get_inventories(self):
        return list(self.inventories)

    def get_filled_map(self):
        return dict(self.filled_map)

    def status(self):
        return {"connected": self.connected, "healthy": self.healthy,
                "account_masked": "****", "is_test": True, "error": ""}


# ─── 假時鐘 (worker 退避 / 收盤 40 s drain 不等真時間) ───

class FakeClock:
    """替換 trading_session.time: sleep 不真睡、只推進虛擬時鐘;time()/monotonic() = 真實流逝 + 虛擬推進。
    anchor 給定 → time() 以該 datetime 為起點 (datetime.fromtimestamp(time()) 也落在同一時刻)。"""

    def __init__(self, anchor: _dt = None):
        self._lk = threading.Lock()
        self._offset = 0.0
        self._t0_mono = _real_time.monotonic()
        self._epoch0 = anchor.timestamp() if anchor is not None else _real_time.time()

    def _elapsed(self) -> float:
        return _real_time.monotonic() - self._t0_mono + self._offset

    def time(self) -> float:
        return self._epoch0 + self._elapsed()

    def monotonic(self) -> float:
        return self._t0_mono + self._elapsed()

    def perf_counter(self) -> float:
        return self.monotonic()

    def sleep(self, sec):
        self.advance(sec)

    def advance(self, sec):
        with self._lk:
            self._offset += max(0.0, float(sec))

    def now_dt(self) -> _dt:
        return _dt.fromtimestamp(self.time())


def install_fake_clock(monkeypatch, anchor: _dt = None) -> FakeClock:
    """把 trading_session 的 time 模組與 datetime 類都換成同一個假時鐘 (sleep 瞬間過、now() 跟著走)。"""
    import trading_session as ts_mod
    clock = FakeClock(anchor)

    class _ClockDatetime(_dt):
        @classmethod
        def now(cls, tz=None):
            d = clock.now_dt()
            return cls(d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond)

    monkeypatch.setattr(ts_mod, "time", clock)
    monkeypatch.setattr(ts_mod, "datetime", _ClockDatetime)
    return clock


def today_at(hh: int, mm: int, ss: int = 0) -> _dt:
    d = _dt.now()
    return d.replace(hour=hh, minute=mm, second=ss, microsecond=0)


# ─── session 工廠 (worker 不自動啟動,測試以 _cancel_worker_run_once 同步驅動) ───

def make_session(broker=None, total=1_000_000, per_symbol=200_000, sizing_mode="budget",
                 fixed_lots=0, date="2026-09-09") -> TradingSession:
    try:
        s = TradingSession(auto_cancel_worker=False)
    except TypeError:                                   # 舊簽名 (尚未實作 kwarg)
        s = TradingSession()
    s.auto_cancel_worker = False
    s.roll_day(date)
    s.set_mode("real")
    s.broker = broker if broker is not None else FakeSnapshotBroker()
    s.set_params(total_budget=total, per_symbol_budget=per_symbol,
                 sizing_mode=sizing_mode, fixed_lots=fixed_lots)
    s.set_armed(True)
    s.order_min_interval = 0.0
    return s


def run_once(s: TradingSession, **kw) -> dict:
    """同步驅動撤單 worker 一輪。"""
    return s._cancel_worker_run_once(**kw)


def fill(order_no, symbol, lots, price, action="buy", filled_no="F1"):
    return {"order_no": order_no, "symbol": symbol, "lots": lots, "price": price,
            "action": action, "filled_no": filled_no, "filled_time": "09:00:00",
            "quantity": lots * 1000}


def wait_until(cond, timeout=2.0) -> bool:
    deadline = _real_time.monotonic() + timeout
    while _real_time.monotonic() < deadline:
        if cond():
            return True
        _real_time.sleep(0.01)
    return bool(cond())


def build_node3(broker=None, n_extra: int = 7, symbol: str = "5386", limit_up: float = 283.0):
    """重演 2026-09-09 node3 5386: 預掛 P (1 張) → 第一筆市價 M (委託成功 → 撤 P) → n_extra 筆管線多送 M
    (每筆也委託成功 → 依契約進撤單佇列)。回 (session, P, M1, [extras])。"""
    s = make_session(broker=broker, total=10_000_000, per_symbol=0,
                     sizing_mode="fixed_lots", fixed_lots=1)
    s.place_pre_orders([symbol], {symbol: limit_up})
    st = s.trades[symbol]
    p_no = st.pre_order_no
    assert p_no and st.order_status == "pending"
    assert s._chase_send_one(symbol, 1) == "accepted"
    m1 = st.order_no
    assert m1 != p_no and st.chase_done
    extras = []
    for _ in range(n_extra):
        before = set(s.order_log)
        assert s._chase_send_one(symbol, 1) == "accepted_extra"
        new = set(s.order_log) - before
        assert len(new) == 1
        extras.append(new.pop())
    return s, p_no, m1, extras
