"""Broker — 富邦真單 client (交易帳號專用)。

只負責「下單/撤單/回報」— 不呼叫 init_realtime (行情由 .env 兩個資料帳號的
subscriber 負責,交易帳號不佔行情額度)。

參考: teralion_WEB/backend/app/core/fubon_adapter.py (實戰範本) 與
teralion_WEB/llms-full.txt (官方文件 dump)。易踩雷點見 CLAUDE.md。

v1 斷線策略: 交易 WS 斷線 (event code=300) 只標 healthy=False + 通知 UI,
不做自動 re-login (day-trade 的 atomic swap 複雜,後續再加)。
"""
from __future__ import annotations

import csv
import logging
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger(__name__)

FUBON_TEST_URL = "wss://neoapitest.fbs.com.tw/TASP/XCPXWS"

# 富邦「帳務查詢 5/秒」— 所有 get_order_results 一律過 broker 內部閘門 (最小間隔 ≥0.2 s)。
# 超限**不是**例外,是 Result{is_success:False, message:"Login Error, 業務系統流量控管"}
# (2026-09-09 node3 事故: 8 條撤單 thread 0.66 s 內打 8 次查詢 → 限流 → 被當成「查無」→ 標 cancelled)。
# 0.2 s = 剛好 5/s 零餘裕;多留 10 ms 抗 sleep 喚醒/網路抖動 (任一 1 s 滑動窗 ≤5 次)。
QUERY_MIN_INTERVAL_SEC = 0.21

# 全清單委託查詢 single-flight (2026-09-14 node1: 同登入的第三方程式狂送 28k 單 → 委託清單 33,940 筆,
# 一次 get_order_results ≈540 MB RSS 且不歸還,兩個重疊 ≈1.09 GB → 主機 RAM+swap 耗盡當機)。
# 全 process 同時最多一個 get_order_results 在飛;其他呼叫者共用它的結果 (唯讀),完成後不快取。
QUERY_JOIN_MAX_WAIT_SEC = 30.0    # 等別人在飛查詢的上限 (含 fresh_after 等前一個結束) — 逾時 raise OrderLookupError
QUERY_FLIGHT_STALE_SEC = 90.0     # 在飛超過此秒數視為卡死 → 不再讓人排隊等它,允許另起一個 (免單一卡死查詢鎖死全部)


class _QueryFlight:
    """一個在飛的全清單 get_order_results (single-flight 共用單位)。時間皆 time.perf_counter()。"""

    __slots__ = ("client", "sdk", "account", "created", "started", "done", "data", "error")

    def __init__(self, client, sdk, account, created: float):
        self.client = client
        self.sdk = sdk
        self.account = account
        self.created = created           # 登記為在飛的時刻 (卡死判斷)
        self.started = None              # 實際呼叫 SDK 前一刻 (新鮮度判斷);None = 還在等 5/s 時槽 (之後才開始)
        self.done = threading.Event()
        self.data = None                 # 成功: list (共用、唯讀 — 各呼叫者拿淺拷貝)
        self.error = None                # 失敗: str (富邦原文)


class _OrderQuerySingleFlight:
    """全 process 共用的委託查詢 single-flight 狀態 (RealOrderClient 類別層持有;鎖內只改指標,絕不呼叫 SDK)。"""

    def __init__(self):
        self.lock = threading.Lock()
        self.flight: Optional[_QueryFlight] = None
        self.last_rows: Optional[int] = None     # 最近一次**成功**全清單查詢的筆數 (盤中對帳調頻用)


class OrderLookupError(RuntimeError):
    """委託查詢失敗 — get_order_results 回 is_success 非 True、回空、或 SDK 例外。
    str(e) 含富邦原文 (例「Login Error, 業務系統流量控管」)。**查詢失敗 ≠ 查無**。"""


class OrderNotFound(RuntimeError):
    """委託查詢成功但清單無此書號 (後檯快照延遲/逐筆亂序皆可能;**不等於已成交/已撤**)。"""


class CancelRejected(RuntimeError):
    """撤單被券商拒 (cancel_order 回 is_success=False;「成交單/部分成交單/取消單已不允許取消」等)。
    帶上撤單當下手上委託物件的 filled_qty (股) 與 status — session 對「已成交」可立刻補成交,不必再查一次
    (2026-09-09 A2「立刻用快照 filled_qty 補成交」;審查 #11)。⚠ 物件來自撤單前的快照,可能落後於券商實況
    (replica 延遲) — session 端以訊息 / 委託量核實,不符就留佇列下一輪再看。
    仍是 RuntimeError 子類、str(e) 格式「撤單失敗 <書號>: <富邦原文>」不變 (既有 isinstance / classify 契約)。"""

    def __init__(self, msg: str, filled_qty=None, status: str = ""):
        super().__init__(msg)
        self.filled_qty = filled_qty
        self.status = status


def _fmt_price(price: float) -> str:
    """Order.price 是字串;整數去小數點 (66.0 → "66"),否則 %g。"""
    return str(int(price)) if float(price) == int(price) else f"{price:g}"


def _norm_enum(value) -> str:
    """SDK enum str() 帶前綴 ('BSAction.Buy' → 'Buy')。"""
    s = str(value)
    return s.split(".")[-1] if "." in s else s


def _attr(o, *names, default=None):
    """依序試多個屬性名 (SDK 欄位 snake/camel 混用)。"""
    for n in names:
        v = getattr(o, n, None)
        if v is not None:
            return v
    return default


class RealOrderClient:
    """富邦真單 client。所有動作寫 CSV (沿用 order.py 欄位格式,含 latency_ms)。

    caller 可掛的 callbacks:
      on_fill(dict)    — 成交回報 {symbol, action, price, lots, quantity, order_no,
                          filled_no, filled_time}
      on_order(dict)   — 委託回報 {order_no, symbol, status, filled_qty, error_message,
                          function_type, last_time} (撤單失敗走 err 參數的回報也轉發,不吞)
      on_disconnect()  — 交易 WS 斷線 (event 300)
    """

    # 查詢閘門的類別層預設 — 讓不經 __init__ 建的替身 (tests 用 RealOrderClient.__new__) 也走閘門;
    # __init__ 會換成 instance 專屬鎖。
    _query_lock = threading.Lock()
    _query_next_ts = 0.0
    query_min_interval = QUERY_MIN_INTERVAL_SEC
    # 全清單查詢 single-flight — **刻意只在類別層** (不在 __init__ 覆寫): 全 process 同時最多一個
    # get_order_results 在飛 (換 client / 重連期間新舊 client 也不重疊)。__new__ 建的替身同樣適用。
    _order_query_sf = _OrderQuerySingleFlight()
    query_join_max_wait = QUERY_JOIN_MAX_WAIT_SEC
    query_flight_stale = QUERY_FLIGHT_STALE_SEC
    _claimed_order_nos: set = frozenset()      # __new__ 替身的預設 (真 client 在 __init__ 換成 set)
    # session 掛的判斷 hook: rpt → True = 確定「非本策略委託」的拒單回報 → 略過逐筆 ERROR (session 彙總)
    is_foreign_order_report: Optional[Callable[[dict], bool]] = None

    def __init__(self, log_path: Path):
        self.sdk = None
        self.account = None            # accounts.data 裡挑出的股票帳戶物件
        self.account_no: str = ""      # 帳號過濾用
        self.connected = False
        self.healthy = False
        self.error_msg = ""
        self.is_test = False
        # 已認領/回傳過的書號 (每筆下單成功後記) — 缺書號反查時排除,讓管線化同標同量多筆在飛時
        # 仍能唯一認領那筆剛送、還沒書號的 (2026-08-28: 管線化打破「每檔同時僅一張活躍委託」假設)
        self._claimed_order_nos: set = set()
        self._unknown_ctr = 0          # UNKNOWN 書號流水號 — 免同秒兩筆撞同 key 覆寫掉一筆 live 單
        # 委託查詢閘門 (富邦帳務查詢 5/s) — 鎖內只預約時槽、鎖外 sleep;所有 get_order_results 過這裡
        self._query_lock = threading.Lock()
        self._query_next_ts = 0.0          # perf_counter;下一個可查詢時槽
        self.query_min_interval = QUERY_MIN_INTERVAL_SEC
        # callbacks
        self.on_fill: Optional[Callable[[dict], None]] = None
        self.on_order: Optional[Callable[[dict], None]] = None
        self.on_disconnect: Optional[Callable[[], None]] = None
        self.on_reconnected: Optional[Callable[[], None]] = None   # 重連成功 → session 補收
        self.is_foreign_order_report = None   # session connect_async 掛 (非本策略拒單回報不逐筆 ERROR)
        # 自動重連 (2026-08-05 實盤事故: 交易 WS 盤中斷線 → 回報遺失+撤單全失敗,
        # v1「不自動重連」決策廢除)。憑證存記憶體供重登入。
        self._account_id = ""
        self._password = ""
        self._pfx_path = ""
        self._pfx_password = ""
        self._relogin_lock = threading.Lock()
        self._relogin_timer: Optional[threading.Timer] = None
        self._relogin_first_fail: Optional[float] = None
        self._stopping = False
        # CSV log
        self._lock = threading.Lock()
        self.log_path = log_path
        new_file = not log_path.exists()
        self._file = open(log_path, "a", newline="", encoding="utf-8")
        self._writer = csv.writer(self._file)
        if new_file:
            self._writer.writerow([
                "order_id", "ts_sent", "ts_accepted", "latency_ms",
                "action", "symbol", "lots", "price_type", "extra",
            ])
            self._file.flush()
        logger.info(f"[broker] RealOrderClient 開檔 {log_path}")

    # ─── 連線 ──────────────────────────────────────────────

    def connect(self, account_id: str, password: str, pfx_path: str,
                pfx_password: str = "", is_test: bool = False):
        """Login + 挑股票帳戶 + 註冊回報。失敗 raise RuntimeError。"""
        from fubon_neo.sdk import FubonSDK

        self.is_test = is_test
        # 憑證存記憶體 — 交易 WS 斷線 (code=300) 自動重登入用
        self._account_id = account_id
        self._password = password
        self._pfx_path = pfx_path
        self._pfx_password = pfx_password
        self._stopping = False
        self._relogin_first_fail = None
        if is_test:
            sdk = FubonSDK(30, 2, url=FUBON_TEST_URL)
            logger.info("[broker] 使用富邦【測試環境】")
        else:
            sdk = FubonSDK()

        # pfx 密碼空字串 → 只傳 3 參數 (踩雷點 #7)
        if pfx_password == "":
            accounts = sdk.login(account_id, password, pfx_path)
        else:
            accounts = sdk.login(account_id, password, pfx_path, pfx_password)

        if not accounts or getattr(accounts, "is_success", None) is not True:
            msg = getattr(accounts, "message", None) or "login 回空/失敗"
            raise RuntimeError(f"富邦 login 失敗: {msg}")

        data = getattr(accounts, "data", None) or []
        if not data:
            raise RuntimeError("login 成功但無帳戶 (accounts.data 空)")
        # 挑股票帳戶 — 一般帳號只有一個;多個時取第一個並 log 全部
        self.account = data[0]
        self.account_no = str(getattr(self.account, "account", ""))
        for i, a in enumerate(data):
            logger.info(f"[broker] 帳戶 #{i+1}: {getattr(a, 'account', '?')} "
                        f"分行 {getattr(a, 'branch_no', '?')}"
                        f"{'  ← 使用' if i == 0 else ''}")

        # 主動回報 (交易回報不需 init_realtime)
        sdk.set_on_filled(self._handle_filled)
        sdk.set_on_order(self._handle_order)
        sdk.set_on_order_changed(self._handle_order)   # 改/撤單回報同格式
        sdk.set_on_event(self._handle_event)

        self.sdk = sdk
        self.connected = True
        self.healthy = True
        self.error_msg = ""
        logger.info(f"[broker] 連線成功 account={account_id[:3]}*** "
                    f"({'測試' if is_test else '正式'}環境)")

    def disconnect(self):
        # 手動斷線 → 停掉自動重連 (timer 不准把連線拉回來)
        self._stopping = True
        if self._relogin_timer is not None:
            try:
                self._relogin_timer.cancel()
            except Exception:
                pass
        self.connected = False
        self.healthy = False
        if self.sdk:
            try:
                self.sdk.logout()
            except Exception:
                pass
        self.sdk = None
        self.account = None
        logger.info("[broker] 已斷線 (logout)")

    def status(self) -> dict:
        return {
            "connected": self.connected,
            "healthy": self.healthy,
            "account_masked": (self.account_no[:3] + "***") if self.account_no else "",
            "is_test": self.is_test,
            "error": self.error_msg,
        }

    def _require_ready(self):
        if not self.sdk or not self.connected:
            raise RuntimeError("broker 未連線")
        if not self.healthy:
            raise RuntimeError("broker 連線不健康 (交易 WS 斷線?) — 請重新連線")

    # ─── 自動重連 (2026-08-06,仿 day-trade fubon_adapter re_login) ──────

    def re_login(self):
        """交易 WS 斷線 (code=300) 自動重連 — **atomic swap**。

        關鍵: 用臨時 new_sdk 嘗試 login,**成功後才**替換 self.sdk — 失敗時舊 SDK
        保留 (不會換成空殼害後續全掛),排 5 秒後重試直到成功或手動 disconnect。
        成功後呼叫 on_reconnected (session 的權威對帳補收 hook — 斷線期間遺失的
        成交回報永遠不會補送,不補收部位就隱形;2026-08-05 3587 事故)。"""
        if not self._relogin_lock.acquire(blocking=False):
            logger.warning("[broker] 已有重連程序執行中 — 略過")
            return
        try:
            if self._stopping:
                return
            logger.critical("[broker] ⚠ 交易 WS 自動重連開始...")
            from fubon_neo.sdk import FubonSDK
            new_sdk = None
            try:
                new_sdk = FubonSDK(30, 2, url=FUBON_TEST_URL) if self.is_test else FubonSDK()
                if self._pfx_password == "":
                    accounts = new_sdk.login(self._account_id, self._password, self._pfx_path)
                else:
                    accounts = new_sdk.login(self._account_id, self._password,
                                             self._pfx_path, self._pfx_password)
            except Exception as e:
                logger.error(f"[broker] 重連 login 例外: {e} (舊 SDK 保留,5 秒後重試)")
                self._safe_logout(new_sdk)
                self._schedule_relogin_retry()
                return
            if not accounts or getattr(accounts, "is_success", None) is not True:
                reason = getattr(accounts, "message", None) or "login 回空/失敗"
                logger.error(f"[broker] 重連 login 失敗: {reason} (舊 SDK 保留,5 秒後重試)")
                self._safe_logout(new_sdk)
                self._schedule_relogin_retry()
                return
            data = getattr(accounts, "data", None) or []
            if not data:
                logger.error("[broker] 重連成功但無帳戶 — 5 秒後重試")
                self._safe_logout(new_sdk)
                self._schedule_relogin_retry()
                return
            # 用原帳號比對找回同一帳戶 (多帳戶時不能亂挑)
            account = next((a for a in data
                            if str(getattr(a, "account", "")) == self.account_no), None)
            if account is None:
                logger.warning(f"[broker] 重連找不到原帳戶 {self.account_no} — 用第一個帳戶")
                account = data[0]

            # login 成功 → 才登出舊 SDK + atomic swap + 重註冊回報
            self._safe_logout(self.sdk)
            new_sdk.set_on_filled(self._handle_filled)
            new_sdk.set_on_order(self._handle_order)
            new_sdk.set_on_order_changed(self._handle_order)
            new_sdk.set_on_event(self._handle_event)
            self.sdk = new_sdk
            self.account = account
            self.account_no = str(getattr(account, "account", ""))
            self.connected = True
            self.healthy = True
            self.error_msg = ""
            self._relogin_first_fail = None
            logger.critical("[broker] ✅ 交易 WS 自動重連成功 — 觸發權威對帳補收")
            if self.on_reconnected:
                try:
                    self.on_reconnected()
                except Exception as e:
                    logger.exception(f"[broker] on_reconnected hook 例外: {e}")
        except Exception as e:
            logger.exception(f"[broker] 重連流程異常: {e} — 5 秒後重試")
            self._schedule_relogin_retry()
        finally:
            self._relogin_lock.release()

    @staticmethod
    def _safe_logout(sdk):
        if sdk is not None:
            try:
                sdk.logout()
            except Exception:
                pass

    def _schedule_relogin_retry(self):
        """重連失敗 → 5 秒後再試 (直到成功或手動 disconnect)。>10 分鐘未恢復 → CRITICAL。"""
        if self._stopping:
            return
        if self._relogin_first_fail is None:
            self._relogin_first_fail = time.time()
        elapsed = time.time() - self._relogin_first_fail
        if elapsed > 600:
            logger.critical(f"[broker] ⚠⚠ 交易 WS 已斷線 {int(elapsed / 60)} 分鐘,"
                            f"自動重連持續失敗 — 需人工檢查")
        t = threading.Timer(5.0, self.re_login)
        t.daemon = True
        self._relogin_timer = t
        t.start()

    # ─── 下單 ──────────────────────────────────────────────

    def place_limit_buy(self, symbol: str, price: float, lots: int) -> str:
        """漲停價限價買 (08:59:58 預掛用)。回 order_no。"""
        return self._place(symbol, lots, buy=True,
                           price=_fmt_price(price), market=False)

    def place_market_buy(self, symbol: str, lots: int) -> str:
        return self._place(symbol, lots, buy=True, price=None, market=True)

    def place_market_sell(self, symbol: str, lots: int, reason: str = "") -> str:
        return self._place(symbol, lots, buy=False, price=None, market=True, extra=reason)

    def place_limit_sell(self, symbol: str, price: float, lots: int, reason: str = "") -> str:
        """限價賣 (處置股出場用 — 委買一價,積極限價立即成交;處置股不能下市價)。回 order_no。"""
        return self._place(symbol, lots, buy=False,
                           price=_fmt_price(price), market=False, extra=reason)

    def _place(self, symbol: str, lots: int, buy: bool, price,
               market: bool, extra: str = "") -> str:
        self._require_ready()
        from fubon_neo.sdk import Order
        from fubon_neo.constant import TimeInForce, OrderType, PriceType, MarketType, BSAction

        order = Order(
            buy_sell=BSAction.Buy if buy else BSAction.Sell,
            symbol=symbol,
            # 限價=字串;市價=**None** — 2026-07-22 實測: 帶 "0" 會被 SDK 本地驗證拒
            # "Price should be empty" (委託沒送出)。day_trade worker.py 三處市價單全用 None。
            price=None if market else price,
            quantity=lots * 1000,              # 股數 (踩雷點 #2)
            market_type=MarketType.Common,
            price_type=PriceType.Market if market else PriceType.Limit,
            time_in_force=TimeInForce.ROD,
            order_type=OrderType.Stock,
            user_def="hitlimit",
        )
        action = ("BUY" if buy else "SELL") + ("_MKT" if market else "_LMT")
        ts_sent = time.time()
        result = self.sdk.stock.place_order(self.account, order)
        ts_accepted = time.time()

        if not result or getattr(result, "is_success", None) is not True:
            msg = getattr(result, "message", None) or "place_order 回空"
            self._write_row("REJECTED", ts_sent, ts_accepted, action, symbol, lots,
                            "MARKET" if market else price, str(msg)[:80])
            raise RuntimeError(f"下單被拒 {symbol}: {msg}")

        order_no = str(_attr(getattr(result, "data", None), "order_no", default="") or "")
        if not order_no or order_no in ("?", "None"):
            # 委託已被接受但回傳缺書號 (防禦路徑) — 反查認領,消除追蹤破口。
            # 不 raise: raise 會觸發上層重試 → 重複下單,比丟追蹤更糟。
            order_no = self._recover_order_no(symbol, buy, lots)
            if not order_no:
                self._unknown_ctr += 1
                logger.critical(f"[broker] ⚠⚠ {symbol} 委託已送出但無法取得書號!"
                                f"未追蹤曝險 — 請立即人工至券商查驗 (user_def=hitlimit)")
                order_no = f"UNKNOWN-{int(ts_sent)}-{self._unknown_ctr}"  # 唯一 → 不覆寫掉另一筆 live 單
        self._claimed_order_nos.add(order_no)   # 記已認領 → 之後同標同量缺書號反查能排除本筆
        self._write_row(order_no, ts_sent, ts_accepted, action, symbol, lots,
                        "MARKET" if market else price, extra)
        logger.info(f"[broker] {action} {symbol} × {lots} 張 @ "
                    f"{'MKT' if market else price} → order_no={order_no}")
        return order_no

    def _recover_order_no(self, symbol: str, buy: bool, lots: int) -> str:
        """place 回傳缺書號時,用 get_order_results 反查認領。

        候選 = user_def=="hitlimit" (策略單權威識別) + 標的 + 買賣別 + 股數 全相符、書號有值、
        **且尚未被認領** (不在 self._claimed_order_nos)。**恰好 1 筆才認領** — 0 或多筆一律回 ""。
        2026-08-28 管線化: 同標同量可有多筆在飛 (打破「每檔僅一張活躍委託」),但先前那 N-1 筆的書號
        都已 add 進 _claimed → 排除後只剩剛送、還沒書號的這一筆 → 仍唯一可認。
        2026-09-15 single-flight: 要求**本函式開始之後才開始**的查詢 (fresh_after) — 不共用委託送出前就
        在飛的舊查詢 (那份清單不會有剛送的這筆 → 誤判 0 候選落 UNKNOWN)。
        """
        fresh_after = time.perf_counter()
        try:
            candidates = []
            # 過 5/s 閘門 + single-flight;查詢失敗 raise → 下方 except 回 ""
            for o in self._query_order_results(fresh_after=fresh_after):
                if str(_attr(o, "user_def", "userDef", default="") or "") != "hitlimit":
                    continue
                if str(_attr(o, "stock_no", "symbol", default="")) != symbol:
                    continue
                if _norm_enum(getattr(o, "buy_sell", "")) != ("Buy" if buy else "Sell"):
                    continue
                if int(_attr(o, "quantity", default=0) or 0) != lots * 1000:
                    continue
                no = str(getattr(o, "order_no", "") or "")
                if no and no not in self._claimed_order_nos:   # 排除先前已認領的管線在飛單
                    candidates.append(no)
            if len(candidates) == 1:
                logger.warning(f"[broker] {symbol} 書號反查認領成功: {candidates[0]}")
                return candidates[0]
            logger.error(f"[broker] {symbol} 書號反查候選 {len(candidates)} 筆 — 不認領")
            return ""
        except Exception as e:
            logger.error(f"[broker] {symbol} 書號反查失敗: {e}")
            return ""

    # ─── 委託查詢 (所有 get_order_results 的唯一入口) ─────────────

    def _await_query_slot(self):
        """富邦「帳務查詢 5/秒」閘門 — get_order_results 與 inventories (get_sellable_lots) **共用同一額度**。
        鎖內只預約下一個時槽 (算等待),**鎖外 sleep** — 多條 thread 同時查是排隊而非互相卡鎖。"""
        # 時鐘用 perf_counter (Windows 3.10 的 monotonic 只有 ~15.6 ms 解析度;Linux 兩者皆 ns)
        with self._query_lock:
            now = time.perf_counter()
            slot = max(now, self._query_next_ts)
            self._query_next_ts = slot + self.query_min_interval
        wait = slot - time.perf_counter()
        while wait > 0:                  # sleep 到時槽為止 (sleep 可能提早醒 → 迴圈保證不早於時槽)
            time.sleep(wait)
            wait = slot - time.perf_counter()

    @staticmethod
    def query_clock() -> float:
        """single-flight 新鮮度用的時鐘 (與 _QueryFlight.started 同源) — caller 取「此刻」當 fresh_after。"""
        return time.perf_counter()

    def _query_order_results(self, fresh_after: Optional[float] = None) -> list:
        """呼叫 sdk.stock.get_order_results 的**唯一入口** — 過 5/s 閘門 + single-flight + 每次 SDK 呼叫一行 log。

        閘門: 鎖內只預約下一個時槽 (算等待),**鎖外 sleep** — 多條 thread 同時查是排隊而非
        互相卡鎖;8 條撤單 thread 同瞬間進來會被攤成 ≥0.2 s 一筆 (≤5/s)。
        single-flight (2026-09-14 node1 清單 33,940 筆、每次查詢 ≈540 MB): 全 process 同時最多一個 SDK 查詢在飛;
        同 client 的呼叫者**共用**在飛那次的結果 (淺拷貝 list,SDK 物件唯讀共用),完成後不快取 (下一個呼叫者重查)。
          - fresh_after (query_clock() 值): 只接受**在此之後才開始**的查詢 — 在飛那次開始得更早 → 等它結束再自己查
          - 不同 client / 不同 sdk 物件 (重連換 SDK) 的在飛查詢不共用,等它結束再查 (仍不重疊)
          - 等待有上限 query_join_max_wait → raise OrderLookupError (結果未知 ≠ 查無)
          - 在飛超過 query_flight_stale 視為卡死 → 不再等它,另起新查詢
        成功回 list (result.data 或 [])。失敗 (is_success 非 True / result 回空 / SDK 例外 / 等待逾時)
        raise OrderLookupError,訊息含富邦原文 — caller **絕不可**把它當「查無」。"""
        sf = self._order_query_sf
        max_wait = float(self.query_join_max_wait)
        deadline = time.perf_counter() + max_wait
        abandoned_age = None
        while True:
            with sf.lock:
                now = time.perf_counter()
                f = sf.flight
                if f is not None and f.done.is_set():
                    sf.flight = f = None                      # 防禦: 已完成卻未清 (正常路徑先清再 set)
                if f is not None and now - f.created > float(self.query_flight_stale):
                    abandoned_age = now - f.created
                    sf.flight = f = None                      # 卡死 → 放棄等它 (它結束時不會清掉新 flight)
                if (f is not None and f.client is self
                        and (f.sdk is not self.sdk or f.account is not self.account)):
                    # 2026-09-15 審查: 同 client 已重連換 SDK/帳戶 (re_login atomic swap) → 舊 SDK 上的在飛查詢
                    # 視為放棄、不等它 (舊連線斷網卡住時,等 30 s 逾時會讓重連補收 reconcile_orders 失敗且不重試)。
                    # 舊 flight 結束時 `sf.flight is f` 不成立 → 不會誤清新 flight;卡住的舊查詢沒收到資料,並行風險可接受
                    abandoned_age = now - f.created
                    sf.flight = f = None
                if f is None:
                    mine = _QueryFlight(self, self.sdk, self.account, now)
                    sf.flight = mine
                    break
                begun = f.started if f.started is not None else now   # 未開始 = 之後才會開始
                joinable = (f.client is self and f.sdk is self.sdk and f.account is self.account
                            and (fresh_after is None or begun >= fresh_after))
            # ── 鎖外等在飛那次 (有上限) ──
            if not f.done.wait(max(0.0, deadline - time.perf_counter())):
                logger.warning(f"[broker] order_results 等待進行中的查詢逾時 ({max_wait:g}s) — 查詢結果未知")
                raise OrderLookupError(f"等待進行中的 get_order_results 逾時 ({max_wait:g}s) — 查詢結果未知 (≠ 查無)")
            if joinable:
                if f.error is not None:
                    raise OrderLookupError(f.error)           # 每位呼叫者各自一個例外物件 (不跨 thread 共用)
                logger.debug(f"[broker] order_results 共用進行中查詢 n={len(f.data or [])}")
                return list(f.data or [])
            # 不可共用 (不夠新 / 別的 client) → 它已結束,回迴圈自己查
        if abandoned_age is not None:
            logger.warning(f"[broker] ⚠ 前一個 get_order_results 已在飛 {abandoned_age:.1f}s 視為卡死"
                           f" (> {self.query_flight_stale:g}s) 或屬於重連前的舊 SDK — 另起新查詢")
        return self._run_query_flight(mine)

    def _run_query_flight(self, f: _QueryFlight) -> list:
        """single-flight 的領頭者: 過 5/s 時槽 → 呼叫 SDK → 結果/錯誤寫進 flight → 清指標 → 喚醒共用者。"""
        sf = self._order_query_sf
        try:
            self._await_query_slot()
            with sf.lock:
                f.started = time.perf_counter()
            t0 = f.started
            try:
                result = f.sdk.stock.get_order_results(f.account)
            except Exception as e:
                ms = round((time.perf_counter() - t0) * 1000, 1)
                logger.info(f"[broker] order_results ok=False n=0 ms={ms} msg=例外: {e}")
                f.error = f"get_order_results 例外: {e}"
                raise OrderLookupError(f.error) from e
            ms = round((time.perf_counter() - t0) * 1000, 1)
            ok = bool(result) and getattr(result, "is_success", None) is True
            data = list(getattr(result, "data", None) or []) if ok else []
            msg = str(getattr(result, "message", None) or "") if result else "result 回空"
            logger.info(f"[broker] order_results ok={ok} n={len(data)} ms={ms} msg={msg or '-'}")
            if not ok:
                # 例「Login Error, 業務系統流量控管」(5/s 超限) — 是查詢失敗,不是清單為空
                f.error = msg or "get_order_results 回 is_success=False"
                raise OrderLookupError(f.error)
            f.data = data
            with sf.lock:
                sf.last_rows = len(data)
            return list(data)
        finally:
            if f.data is None and f.error is None:
                f.error = "get_order_results 未完成 (查詢中斷)"
            with sf.lock:
                if sf.flight is f:
                    sf.flight = None
            f.done.set()

    def order_query_in_flight(self) -> bool:
        """目前是否有全清單 get_order_results 在飛 (全 process;卡死超過 query_flight_stale 的不算)。"""
        sf = self._order_query_sf
        with sf.lock:
            f = sf.flight
            return bool(f is not None and not f.done.is_set()
                        and time.perf_counter() - f.created <= float(self.query_flight_stale))

    def last_order_query_rows(self) -> Optional[int]:
        """最近一次成功全清單查詢的筆數 (尚未成功查過 → None)。"""
        sf = self._order_query_sf
        with sf.lock:
            return sf.last_rows

    def is_claimed_order_no(self, order_no) -> bool:
        """此書號是否為本 client 下單成功回傳/認領過的 (session 判斷「非本策略拒單回報」用)。"""
        return bool(order_no) and str(order_no) in self._claimed_order_nos

    @staticmethod
    def _order_row(o) -> dict:
        """SDK 委託物件 → dict (snapshot / pending 共用;欄位 snake/camel 容錯;數量皆股數)。"""
        after_qty = _attr(o, "after_qty", "afterQty", default=None)
        try:
            after_qty = int(after_qty) if after_qty is not None else None
        except (TypeError, ValueError):
            after_qty = None
        status = _attr(o, "status", default=None)
        return {
            "order_no": str(getattr(o, "order_no", "") or ""),
            "symbol": str(_attr(o, "stock_no", "symbol", default="") or ""),
            "buy_sell": _norm_enum(getattr(o, "buy_sell", "") or ""),
            "quantity": int(_attr(o, "quantity", default=0) or 0),
            "filled_qty": int(_attr(o, "filled_qty", "filledQty", default=0) or 0),
            "after_qty": after_qty,              # 改量後委託股數;缺欄回 None (caller 視同「未知」)
            "status": "" if status is None else str(status),
            "user_def": str(_attr(o, "user_def", "userDef", default="") or ""),
            "_obj": o,                           # 原始 SDK 物件 — cancel_by_obj 免再查
        }

    def get_order_snapshot(self, fresh_after: Optional[float] = None) -> list:
        """一次查回券商**全部**委託 (撤單 worker / 券商權威掃單 / 同步撤單試一次共用)。

        每項 {order_no, symbol, buy_sell, quantity, filled_qty, after_qty, status, user_def, _obj}。
        fresh_after (query_clock() 值) → 只用在此之後才開始的查詢 (收盤掃單用;見 _query_order_results)。
        失敗 raise OrderLookupError (含未連線/不健康 — 一律「查不到 ≠ 沒有」)。"""
        try:
            self._require_ready()
        except RuntimeError as e:
            raise OrderLookupError(str(e)) from e
        return [self._order_row(o) for o in self._query_order_results(fresh_after=fresh_after)]

    def _find_order_obj(self, order_no: str) -> tuple:
        """回 (obj|None, snapshot_ok, message)。
        snapshot_ok False = 查詢失敗 (message 含富邦原文,例「Login Error, 業務系統流量控管」);
        snapshot_ok True 且 obj None = 清單無此書號 (message "NOT_FOUND")。"""
        try:
            data = self._query_order_results()
        except OrderLookupError as e:
            return None, False, str(e)
        for o in data:
            if str(getattr(o, "order_no", "") or "") == str(order_no):
                return o, True, ""
        return None, True, "NOT_FOUND"

    # ─── 撤單 ──────────────────────────────────────────────

    def cancel_by_obj(self, order_obj, order_no: str, symbol: str = "",
                      reason: str = "") -> None:
        """用已拿到的委託物件直接撤 (worker 一次快照 → 多筆撤,免每筆再查)。
        成功寫 CSV CANCEL 列 (extra=reason);失敗寫 extra=FAIL:<msg> 並
        raise CancelRejected("撤單失敗 <書號>: <富邦原文>", filled_qty=, status=) — 原文供 session
        classify_cancel_error 分類 (成交單/部分成交單/取消單已不允許取消),filled_qty/status 供立刻補成交。
        SDK 例外 (傳輸層) 仍 raise 純 RuntimeError。CSV latency = 撤單 REST 往返 (不含查詢)。"""
        self._require_ready()
        ts_sent = time.time()
        try:
            result = self.sdk.stock.cancel_order(self.account, order_obj)
        except Exception as e:
            msg = f"例外: {e}"
            self._write_row(order_no, ts_sent, time.time(), "CANCEL", symbol, 0, "-",
                            f"FAIL:{msg[:60]}")
            logger.error(f"[broker] CANCEL {order_no} ({symbol}) 例外: {e}")
            raise RuntimeError(f"撤單失敗 {order_no}: {msg}") from e
        ts_accepted = time.time()
        # 與下單一致用 is True 判斷 (原 is not False 會把 None/缺失誤判為成功)
        ok = bool(result) and getattr(result, "is_success", None) is True
        if not ok:
            msg = str((getattr(result, "message", None) if result else None) or "cancel_order 回空")
            self._write_row(order_no, ts_sent, ts_accepted, "CANCEL", symbol, 0, "-",
                            f"FAIL:{msg[:60]}")
            logger.error(f"[broker] CANCEL {order_no} ({symbol}) 失敗: {msg}")
            # 帶上手上物件的 filled_qty/status (撤單前快照值) — session 對「已成交」立刻補成交
            try:
                fq = int(_attr(order_obj, "filled_qty", "filledQty", default=0) or 0)
            except (TypeError, ValueError):
                fq = None
            stt = _attr(order_obj, "status", default=None)
            raise CancelRejected(f"撤單失敗 {order_no}: {msg}", filled_qty=fq,
                                 status="" if stt is None else str(stt))
        self._write_row(order_no, ts_sent, ts_accepted, "CANCEL", symbol, 0, "-", reason)
        logger.info(f"[broker] CANCEL {order_no} ({symbol}) reason={reason}")

    def cancel(self, order_no: str, symbol: str = "", reason: str = "") -> None:
        """撤單 facade: 快照找 order object (踩雷點 #6) → cancel_by_obj。**絕不靜默返回、絕不回 None 當成功**:
          查詢失敗 (限流/斷線/例外) → CSV 列 FAIL:QUERY:<msg> + raise OrderLookupError
          查詢成功但清單無此書號   → CSV 列 FAIL:NOT_FOUND     + raise OrderNotFound
          撤單被券商拒            → CSV 列 FAIL:<msg>         + raise RuntimeError (cancel_by_obj)
        2026-09-09 node3: 舊版對 None 只 log「查無委託」不 raise → 上層標 cancelled → 4 筆隱形裸單。
        「查無」可能是後檯快照延遲或 5/s 限流,**不等於已成交/已撤** — 由 session 撤單佇列重試。"""
        self._require_ready()
        ts_sent = time.time()
        obj, snapshot_ok, msg = self._find_order_obj(order_no)
        if not snapshot_ok:
            self._write_row(order_no, ts_sent, time.time(), "CANCEL", symbol, 0, "-",
                            f"FAIL:QUERY:{msg[:60]}")
            logger.error(f"[broker] cancel {order_no} ({symbol}): 委託查詢失敗 — {msg} (未送撤單)")
            raise OrderLookupError(msg)
        if obj is None:
            self._write_row(order_no, ts_sent, time.time(), "CANCEL", symbol, 0, "-",
                            "FAIL:NOT_FOUND")
            logger.warning(f"[broker] cancel {order_no} ({symbol}): 快照查無此書號 "
                           f"(≠ 已成交/已撤;可能後檯延遲,交由佇列重試)")
            raise OrderNotFound(f"查無委託 {order_no} (快照內無此書號)")
        self.cancel_by_obj(obj, order_no, symbol, reason)

    def get_inventories(self) -> list:
        """查庫存 (隔日賣標的用 — 隔天以券商庫存為準對帳)。
        回 [{symbol, lots, order_type}];只回**現股多單** (Stock/DayTrade,net>0),
        跳過融資融券借券 (Short/Margin/SBL 私人部位)。"""
        self._require_ready()
        try:
            result = self.sdk.accounting.inventories(self.account)
        except Exception as e:
            logger.error(f"[broker] 查庫存例外: {e}")
            return []
        if not result or not getattr(result, "is_success", False) or not result.data:
            return []
        out = []
        for inv in result.data:
            sym = str(_attr(inv, "stock_no", "symbol", default="") or "")
            if not sym:
                continue
            otype = _norm_enum(getattr(inv, "order_type", ""))
            # 可賣張數 = tradable_qty (可賣量),退回 lastday_qty。**不可用 lastday+today 相加** —
            # 留倉部位在富邦會同時出現在 lastday_qty 與 today_qty,相加 → 1 張變 2 張 (2026-08-03
            # 實測 bug,對齊 day_trade fubon_adapter 的 `qty = tradable or lastday` 寫法)。
            tradable = int(_attr(inv, "tradable_qty", default=0) or 0)
            lastday = int(_attr(inv, "lastday_qty", default=0) or 0)
            today = int(_attr(inv, "today_qty", default=0) or 0)
            qty = tradable or lastday
            logger.info(f"[broker] 庫存明細 {sym}: 可賣={tradable} 昨日={lastday} 今日={today} "
                        f"→ 採用 {qty} ({qty // 1000} 張) type={otype}")
            if otype in ("Short", "Margin", "SBL"):
                continue                          # 私人部位不碰
            if qty <= 0:
                continue                          # 只處理多單 (可賣)
            out.append({"symbol": sym, "lots": qty // 1000, "order_type": otype})
        logger.info(f"[broker] get_inventories → {out}")
        return out

    def get_sellable_lots(self, symbol: str) -> int:
        """**嚴格版**可賣張數查詢 (出場賣單被拒「可賣不足/超過庫存」後重算張數用;2026-09-14 node3/node4)。

        與 get_inventories 的差別 (get_inventories 行為不變,隔日賣對帳照用):
          - 過帳務查詢 5/s 閘門 (_await_query_slot,與 get_order_results 共用額度)
          - 失敗**一律 raise OrderLookupError** (含富邦原文): 未連線/不健康、SDK 例外、result 回空、
            is_success 非 True、data 為 None、該檔庫存列缺 tradable_qty 或無法解析 —
            **查詢失敗 ≠ 可賣 0**,caller 必須當「未知」處理
          - 只看 tradable_qty (可委託整股庫存數),**不退回 lastday_qty**;跳過 Short/Margin/SBL 列
        查詢成功且庫存無此檔 → 0 (券商權威: 目前沒有可賣股數)。回張數 (股 // 1000)。"""
        return self._query_sellable(symbol, need_balance=False)["tradable"]

    def get_sellable_position(self, symbol: str) -> dict:
        """同 get_sellable_lots (同一次 inventories 查詢、同閘門、同失敗語意),另回整股餘額:
        {"tradable": tradable_qty 張 (可委託), "balance": today_qty 張 (整股餘額)}。
        2026-09-15 審查: tradable 0 有歧義 — 禁現沖/當沖資格不符的今日買進、或被其他賣單佔用時 tradable 也是 0,
        但帳上仍有股 (today_qty>0)。session 只有「tradable 0 **且** 餘額 0」才可判定部位已不在。
        該檔庫存列缺 today_qty 或無法解析 → raise OrderLookupError (無從判斷)。"""
        return self._query_sellable(symbol, need_balance=True)

    def _query_sellable(self, symbol: str, need_balance: bool) -> dict:
        try:
            self._require_ready()
        except RuntimeError as e:
            raise OrderLookupError(str(e)) from e
        self._await_query_slot()
        t0 = time.perf_counter()
        try:
            result = self.sdk.accounting.inventories(self.account)
        except Exception as e:
            ms = round((time.perf_counter() - t0) * 1000, 1)
            logger.info(f"[broker] sellable {symbol} ok=False ms={ms} msg=例外: {e}")
            raise OrderLookupError(f"inventories 例外: {e}") from e
        ms = round((time.perf_counter() - t0) * 1000, 1)
        ok = bool(result) and getattr(result, "is_success", None) is True
        msg = str(getattr(result, "message", None) or "") if result else "result 回空"
        if not ok:
            logger.info(f"[broker] sellable {symbol} ok=False ms={ms} msg={msg or '-'}")
            raise OrderLookupError(msg or "inventories 回 is_success=False")
        data = getattr(result, "data", None)
        if data is None:
            # 成功卻無 data — 無從區分「沒庫存」與「回傳異常」→ 當查詢失敗 (免誤判可賣 0 而停止出場)
            logger.info(f"[broker] sellable {symbol} ok=False ms={ms} msg=data None")
            raise OrderLookupError("inventories 回 is_success=True 但 data 為 None")
        shares = 0
        balance = 0
        rows = 0
        try:
            for inv in data:
                if str(_attr(inv, "stock_no", "symbol", default="") or "") != symbol:
                    continue
                otype = _norm_enum(getattr(inv, "order_type", "") or "")
                if otype in ("Short", "Margin", "SBL"):
                    continue                          # 私人融資融券借券部位 — 現股賣單賣不到
                raw = _attr(inv, "tradable_qty", "tradableQty", default=None)
                if raw is None:
                    raise OrderLookupError(f"{symbol} 庫存列缺 tradable_qty (無從判斷可賣張數)")
                shares += max(0, int(raw))
                if need_balance:
                    raw_bal = _attr(inv, "today_qty", "todayQty", default=None)
                    if raw_bal is None:
                        raise OrderLookupError(f"{symbol} 庫存列缺 today_qty (無從判斷整股餘額)")
                    balance += max(0, int(raw_bal))
                rows += 1
        except OrderLookupError:
            raise
        except Exception as e:
            raise OrderLookupError(f"{symbol} 庫存列解析失敗: {e}") from e
        lots = shares // 1000
        out = {"tradable": lots}
        extra = ""
        if need_balance:
            out["balance"] = balance // 1000
            extra = f" 整股餘額={balance} 股 → {balance // 1000} 張"
        logger.info(f"[broker] sellable {symbol} ok=True ms={ms} tradable={shares} 股 → {lots} 張{extra} "
                    f"(庫存列 {rows})")
        return out

    def get_order_filled_lots(self, order_no: str) -> int:
        """向券商查該委託的權威已成交張數。查無此單**或查詢失敗**回 -1 (caller 保守處理)。
        用途: 撤預掛後、市價追差額前,防「fill 回報晚到 → 差額算全額 → 雙倍買」競態。"""
        self._require_ready()
        obj, snapshot_ok, msg = self._find_order_obj(order_no)
        if obj is None:
            if not snapshot_ok:
                logger.error(f"[broker] get_order_filled_lots {order_no}: 查詢失敗 — {msg} → 回 -1")
            return -1
        filled_qty = int(_attr(obj, "filled_qty", "filledQty", default=0) or 0)
        return filled_qty // 1000

    def get_filled_map(self, fresh_after: Optional[float] = None) -> dict:
        """一次查回**所有**委託的權威已成交張數 {order_no: lots} — 斷線補收對帳用
        (一次 REST 拿全部,不逐單查)。查詢失敗 raise OrderLookupError,caller 處理
        (不再對 is_success=False 靜默回空 map — 空 map 會被當「補收 0 筆」)。
        fresh_after → 只用在此之後才開始的查詢 (重連補收: 不共用重連前就在飛的舊查詢)。"""
        self._require_ready()
        out = {}
        for o in self._query_order_results(fresh_after=fresh_after):
            no = str(getattr(o, "order_no", "") or "")
            if no:
                out[no] = int(_attr(o, "filled_qty", "filledQty", default=0) or 0) // 1000
        return out

    def get_pending_orders(self) -> list:
        """回當前委託清單 (重連/重啟重建 pending 用)。每項 dict = get_order_snapshot 去 _obj
        (含 user_def / after_qty)。查詢失敗 raise OrderLookupError — 不再靜默回 []
        (空清單會被當「沒有 pending」)。"""
        return [{k: v for k, v in row.items() if k != "_obj"}
                for row in self.get_order_snapshot()]

    # ─── SDK 回報 handlers (SDK thread 進來) ────────────────

    def _handle_filled(self, err, content):
        if err:
            logger.error(f"[broker] filled 回報 err: {err}")
            return
        try:
            # 帳號不符只 warning、不 drop — 帳號格式若有差 (前導零/分行前綴/int-vs-str)
            # silent drop 成交回報是致命的。真正的安全網在下游 session._on_fill 的
            # 「order_no ∈ order_log」過濾 (order_no 天然唯一,非策略單自然被擋)。
            acct = str(_attr(content, "account", default=""))
            if acct and self.account_no and acct != self.account_no:
                logger.warning(f"[broker] 成交回報帳號 {acct} != 登入 {self.account_no} "
                               f"— 仍轉發 (下游 order_no 過濾把關)")
            qty = int(_attr(content, "filled_qty", "filledQty", default=0) or 0)
            fill = {
                "symbol": str(_attr(content, "stock_no", "symbol", default="")),
                "action": "buy" if _norm_enum(getattr(content, "buy_sell", "")) == "Buy" else "sell",
                "price": float(_attr(content, "filled_price", "filledPrice", default=0) or 0),
                "quantity": qty,                       # 股數
                "lots": qty // 1000,
                "order_no": str(_attr(content, "order_no", "orderNo", default="")),
                "filled_no": str(_attr(content, "filled_no", "filledNo", default="")),
                "filled_time": str(_attr(content, "filled_time", "filledTime", default="")),
            }
            logger.info(f"[broker] 成交回報 {fill['symbol']} {fill['action']} "
                        f"{fill['lots']} 張 @ {fill['price']} (order {fill['order_no']})")
            if self.on_fill:
                self.on_fill(fill)
        except Exception as e:
            logger.exception(f"[broker] filled handler 例外: {e}")

    def _handle_order(self, err, content):
        """委託/改撤單回報 (SDK thread)。**err 分支照樣轉發、不 return** — 撤單失敗
        (ft=30 status=39,「取消單已不允許取消」等) 是走 err 參數進來的,舊版直接 return →
        session 永遠看不到撤單失敗 (2026-09-09)。status 39 的 content 多數欄位 None,取值全容錯;
        function_type 保留原值 (int/str/None),由 session 轉 str 比較。"""
        try:
            acct = str(_attr(content, "account", default="") or "")
            if acct and self.account_no and acct != self.account_no:
                return
            status = _attr(content, "status", default=None)
            rpt = {
                "order_no": str(_attr(content, "order_no", "orderNo", default="") or ""),
                "symbol": str(_attr(content, "stock_no", "symbol", default="") or ""),
                "status": "" if status is None else str(status),
                "filled_qty": int(_attr(content, "filled_qty", "filledQty", default=0) or 0),
                "error_message": str(_attr(content, "error_message", "errorMessage", default="") or ""),
                "function_type": _attr(content, "function_type", "functionType", default=None),
                # 富邦回報「最後異動時間」(毫秒,如 "10:44:05.796") — 新單接受回報 = 委託被接受時戳
                "last_time": str(_attr(content, "last_time", "lastTime", default="") or ""),
                # 委託自訂欄位 (本策略下單帶 "hitlimit") — session 據此絕不把本策略委託的拒單當第三方單折疊
                "user_def": str(_attr(content, "user_def", "userDef", default="") or ""),
            }
            if err:
                # err 原文例 "[115]證券委託目前狀態取消單已不允許取消交易";content.error_message 通常同文
                if not rpt["error_message"]:
                    rpt["error_message"] = str(err)
                # session 判定「確定非本策略委託」的拒單回報 → 不逐筆 ERROR (session 首見文案 log + 定期彙總;
                # 2026-09-14 node1 第三方程式 28k 拒單 → 3 分鐘 15k 行 ERROR)。其他一律照舊逐筆 ERROR。
                if not self._report_is_foreign(rpt):
                    logger.error(f"[broker] 委託回報 err={err} order={rpt['order_no'] or '-'} "
                                 f"{rpt['symbol'] or '-'} ft={rpt['function_type']} "
                                 f"status={rpt['status'] or '-'} msg={rpt['error_message']}")
            else:
                logger.info(f"[broker] 委託回報 {rpt['symbol']} order={rpt['order_no']} "
                            f"ft={rpt['function_type']} status={rpt['status']} "
                            f"last_time={rpt['last_time'] or '-'} err={rpt['error_message'] or '-'}")
            if self.on_order:
                self.on_order(rpt)
        except Exception as e:
            logger.exception(f"[broker] order handler 例外: {e}")

    def _report_is_foreign(self, rpt: dict) -> bool:
        """is_foreign_order_report hook 明確回 True 才算;沒掛 / 例外 / 非 True → False (照舊逐筆 ERROR)。"""
        hook = self.is_foreign_order_report
        if hook is None:
            return False
        try:
            return hook(rpt) is True
        except Exception:
            return False

    def _handle_event(self, code, content):
        logger.warning(f"[broker] 事件 code={code}: {content}")
        if str(code) == "300":     # 交易 WS 斷線
            self.healthy = False
            self.error_msg = "交易 WS 斷線 (event 300) — 自動重連中"
            logger.critical("[broker] ⚠ 交易 WS 斷線 (code=300) — 啟動自動重連")
            if self.on_disconnect:
                try:
                    self.on_disconnect()
                except Exception:
                    pass
            # cheap guard: 重連已在跑就不再開 thread (re_login 內部 lock 也會擋)
            if self._relogin_lock.locked():
                logger.warning("[broker] code=300 但重連已在進行 — skip")
                return
            if not self._stopping:
                threading.Thread(target=self.re_login,
                                 name="broker-relogin", daemon=True).start()

    # ─── CSV ──────────────────────────────────────────────

    def _write_row(self, order_id, ts_sent, ts_accepted, action, symbol, lots,
                   price_type, extra=""):
        latency_ms = round((ts_accepted - ts_sent) * 1000, 3)
        row = [
            order_id,
            datetime.fromtimestamp(ts_sent).isoformat(timespec="microseconds"),
            datetime.fromtimestamp(ts_accepted).isoformat(timespec="microseconds"),
            latency_ms, action, symbol, lots, price_type, extra,
        ]
        with self._lock:
            self._writer.writerow(row)
            self._file.flush()

    def close(self):
        with self._lock:
            try:
                self._file.flush()
                self._file.close()
            except Exception:
                pass
