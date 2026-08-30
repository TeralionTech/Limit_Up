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
                          function_type}
      on_disconnect()  — 交易 WS 斷線 (event 300)
    """

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
        # callbacks
        self.on_fill: Optional[Callable[[dict], None]] = None
        self.on_order: Optional[Callable[[dict], None]] = None
        self.on_disconnect: Optional[Callable[[], None]] = None
        self.on_reconnected: Optional[Callable[[], None]] = None   # 重連成功 → session 補收
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
        """
        try:
            result = self.sdk.stock.get_order_results(self.account)
            if not result or not getattr(result, "is_success", False):
                return ""
            candidates = []
            for o in (result.data or []):
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

    # ─── 撤單 ──────────────────────────────────────────────

    def cancel(self, order_no: str, symbol: str = "", reason: str = ""):
        """撤單 — 先 get_order_results 找 order object (踩雷點 #6)。
        查無此委託 (已成交/已撤) → log 後靜默返回,不 raise。"""
        self._require_ready()
        ts_sent = time.time()
        obj = self._find_order_obj(order_no)
        if obj is None:
            logger.warning(f"[broker] cancel {order_no}: 查無委託 (可能已成交/已撤)")
            return
        result = self.sdk.stock.cancel_order(self.account, obj)
        ts_accepted = time.time()
        # 與下單一致用 is True 判斷 (原 is not False 會把 None/缺失誤判為成功)
        ok = bool(result) and getattr(result, "is_success", None) is True
        self._write_row(order_no, ts_sent, ts_accepted, "CANCEL", symbol, 0, "-",
                        reason if ok else f"FAIL:{getattr(result, 'message', '?')}")
        if not ok:
            raise RuntimeError(f"撤單失敗 {order_no}: {getattr(result, 'message', '?')}")
        logger.info(f"[broker] CANCEL {order_no} ({symbol}) reason={reason}")

    def _find_order_obj(self, order_no: str):
        result = self.sdk.stock.get_order_results(self.account)
        if result and getattr(result, "is_success", False):
            for o in (result.data or []):
                if str(getattr(o, "order_no", "")) == str(order_no):
                    return o
        return None

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

    def get_order_filled_lots(self, order_no: str) -> int:
        """向券商查該委託的權威已成交張數。查無此單回 -1 (caller 保守處理)。
        用途: 撤預掛後、市價追差額前,防「fill 回報晚到 → 差額算全額 → 雙倍買」競態。"""
        self._require_ready()
        obj = self._find_order_obj(order_no)
        if obj is None:
            return -1
        filled_qty = int(_attr(obj, "filled_qty", "filledQty", default=0) or 0)
        return filled_qty // 1000

    def get_filled_map(self) -> dict:
        """一次查回**所有**委託的權威已成交張數 {order_no: lots} — 斷線補收對帳用
        (一次 REST 拿全部,不逐單查)。查詢失敗 raise,caller 處理。"""
        self._require_ready()
        result = self.sdk.stock.get_order_results(self.account)
        out = {}
        if result and getattr(result, "is_success", False):
            for o in (result.data or []):
                no = str(getattr(o, "order_no", ""))
                if no:
                    out[no] = int(_attr(o, "filled_qty", "filledQty", default=0) or 0) // 1000
        return out

    def get_pending_orders(self) -> list:
        """回當前委託清單 (重連/重啟重建 pending 用)。每項 dict。"""
        self._require_ready()
        result = self.sdk.stock.get_order_results(self.account)
        out = []
        if result and getattr(result, "is_success", False):
            for o in (result.data or []):
                out.append({
                    "order_no": str(getattr(o, "order_no", "")),
                    "symbol": str(_attr(o, "stock_no", "symbol", default="")),
                    "buy_sell": _norm_enum(getattr(o, "buy_sell", "")),
                    "quantity": int(_attr(o, "quantity", default=0) or 0),
                    "filled_qty": int(_attr(o, "filled_qty", "filledQty", default=0) or 0),
                    "status": str(getattr(o, "status", "")),
                })
        return out

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
        if err:
            logger.error(f"[broker] order 回報 err: {err}")
            return
        try:
            acct = str(_attr(content, "account", default=""))
            if acct and self.account_no and acct != self.account_no:
                return
            rpt = {
                "order_no": str(_attr(content, "order_no", "orderNo", default="")),
                "symbol": str(_attr(content, "stock_no", "symbol", default="")),
                "status": str(getattr(content, "status", "")),
                "filled_qty": int(_attr(content, "filled_qty", "filledQty", default=0) or 0),
                "error_message": str(_attr(content, "error_message", "errorMessage", default="") or ""),
                "function_type": _attr(content, "function_type", "functionType", default=None),
            }
            logger.info(f"[broker] 委託回報 {rpt['symbol']} order={rpt['order_no']} "
                        f"status={rpt['status']} err={rpt['error_message'] or '-'}")
            if self.on_order:
                self.on_order(rpt)
        except Exception as e:
            logger.exception(f"[broker] order handler 例外: {e}")

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
