"""TradingSession — 模式(模擬/真實)/連線/預算/per-symbol 交易 state。

掛在 Runner singleton 屬性上,**活過 runner 每日重啟**(不進 _run_all_phases 重建)。
state 模式參考 day-trade-system worker.py: per-symbol dict 純記憶體、fill 去重、
kill switch (armed) + pre-flight、預算進場前檢查。

安全預設: mode=sim、armed=False、重啟後不自動重連不自動 arm。
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# 富邦 API 明定速率上限: 下單 50/s、批次下單 10/s、帳務查詢 5/s、連線數 10。
# 所有 broker 呼叫 (place/cancel) 過全域送單閘門,硬底線 0.02s (= 50/s)。
_HARD_MIN_INTERVAL = 0.02


class SymbolTrade:
    """單股交易 state (flag-dict 模式)。"""

    def __init__(self, symbol: str, limit_up: float):
        self.symbol = symbol
        self.limit_up = limit_up
        self.target_lots = 0
        self.order_no: str = ""        # 當前有效委託 (一次一張單)
        self.order_kind: str = ""      # "pre_limit" / "market_buy" / "market_sell"
        self.order_status: str = ""    # "" / pending / cancelled / rejected / done
        self.filled_lots = 0
        self.avg_price = 0.0
        self.stopped_reason: str = ""  # 非空 = 此檔不再進場
        self.exited = False            # 已出場 (委賣出現)

    def to_dict(self) -> dict:
        return {
            "target_lots": self.target_lots,
            "order_no": self.order_no,
            "order_kind": self.order_kind,
            "order_status": self.order_status,
            "filled_lots": self.filled_lots,
            "avg_price": round(self.avg_price, 2),
            "stopped_reason": self.stopped_reason,
            "exited": self.exited,
        }


class TradingSession:
    """交易會話 — broker 連線 + 模式 + 預算 + 交易 state。"""

    def __init__(self):
        self._lock = threading.RLock()
        self.mode: str = "sim"            # "sim" / "real"
        self.armed: bool = False          # kill switch — True 才會真下單
        self.broker = None                # broker.RealOrderClient
        self.connecting = False
        self.connect_error = ""
        # 下單量配置:
        #   sizing_mode="budget"    → 每檔 min(每檔上限, 總預算餘額)/成本 (雙層金額)
        #   sizing_mode="fixed_lots"→ 每檔固定 fixed_lots 張 (仍受總預算餘額 cap)
        self.sizing_mode: str = "budget"
        self.fixed_lots: int = 0
        self.total_budget: float = 0.0
        self.per_symbol_budget: float = 0.0
        self.budget_used: float = 0.0     # 下單時保留,撤單釋放未成交部分
        # per-symbol state
        self.trades: Dict[str, SymbolTrade] = {}
        self._processed_fills: set = set()   # 去重 key
        # 委託總表 (前端委託狀態顯示 + 右鍵刪單) — key=order_no, 插入序
        self.order_log: Dict[str, dict] = {}
        # 策略參數 (runner 啟動時從 cfg configure;預設值供測試/未 configure 時用)
        self.max_stock_price: float = 500.0        # 只做漲停價 <= 此價 (0=不限)
        self.order_min_interval: float = 0.2       # 市價追單最小送單間隔
        self.cancel_pending_time = None            # datetime.time;此時後不再追單
        # 全域送單閘門 (所有 place/cancel 過這裡,硬底線 50/s)
        self._send_lock = threading.Lock()
        self._last_send = 0.0
        # 交易日 (roll_day 每日重置用)
        self._trade_date: str = ""

    def roll_day(self, date_str: str):
        """每日重置 (runner 每天 8:00 開跑時呼叫)。

        日期變了 → 清掉前一日的 per-day state (trades/order_log/fill去重/預算),
        並強制 **armed=False** — 每天都必須手動重新按「開始交易」(安全預設)。
        同日重複呼叫 (盤中手動重啟 runner) → 不清,保留當日委託/持倉 state
        (券商端委託仍有效,清了會失去追蹤)。連線 (broker) 不動。
        """
        with self._lock:
            if self._trade_date == date_str:
                logger.info(f"[session] roll_day({date_str}) — 同日重啟,state 保留")
                return
            had = len(self.trades)
            self._trade_date = date_str
            self.trades.clear()
            self.order_log.clear()
            self._processed_fills.clear()
            self.budget_used = 0.0
            self.armed = False
        logger.warning(f"[session] roll_day({date_str}) — 新交易日: 清 {had} 檔前日 state,"
                       f"armed=False (要交易請重新 arm)")

    def configure(self, max_stock_price: float = None,
                  order_min_interval_sec: float = None,
                  cancel_pending_time: str = None):
        """runner 啟動時從 cfg 塞策略參數。"""
        from datetime import time as _time_cls
        with self._lock:
            if max_stock_price is not None:
                self.max_stock_price = float(max_stock_price)
            if order_min_interval_sec is not None:
                self.order_min_interval = max(float(order_min_interval_sec), _HARD_MIN_INTERVAL)
            if cancel_pending_time:
                try:
                    parts = [int(x) for x in cancel_pending_time.split(":")]
                    while len(parts) < 3:
                        parts.append(0)
                    self.cancel_pending_time = _time_cls(*parts[:3])
                except Exception:
                    logger.warning(f"[session] CANCEL_PENDING_TIME 格式錯: {cancel_pending_time!r}")
        logger.info(f"[session] configure: max_price={self.max_stock_price} "
                    f"interval={self.order_min_interval}s cancel_at={self.cancel_pending_time}")

    def _gate(self, min_interval: float = _HARD_MIN_INTERVAL):
        """全域送單閘門 — 距上次「送單」不足 min_interval 就等 (跨 thread 序列化)。"""
        with self._send_lock:
            wait = self._last_send + min_interval - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._last_send = time.monotonic()

    def _log_order(self, order_no: str, symbol: str, action: str, kind: str,
                   lots: int, price: float):
        """記進委託總表 (前端顯示用)。caller 不必持鎖。"""
        with self._lock:
            self.order_log[order_no] = {
                "order_no": order_no,
                "symbol": symbol,
                "action": action,          # buy / sell
                "kind": kind,              # pre_limit / market_buy / market_sell
                "lots": lots,
                "price": price,            # 0 = 市價
                "status": "pending",       # pending / filled / cancelled / rejected
                "filled_lots": 0,
                "ts": datetime.now().isoformat(timespec="seconds"),
            }

    def _mark_order(self, order_no: str, status: str):
        with self._lock:
            row = self.order_log.get(order_no)
            if row is not None and row["status"] == "pending":
                row["status"] = status

    # ─── 連線 ──────────────────────────────────────────────

    def connect_async(self, account_id: str, password: str, pfx_path: str,
                      pfx_password: str, is_test: bool, output_dir: Path):
        """背景 thread 連線 (照 day-trade /api/auth/connect 模式,前端輪詢 status)。"""
        with self._lock:
            if self.connecting:
                raise RuntimeError("連線進行中")
            self.connecting = True
            self.connect_error = ""

        def _do():
            try:
                from broker import RealOrderClient
                from datetime import datetime as _dt
                output_dir.mkdir(exist_ok=True)
                log_path = output_dir / f"{_dt.now().strftime('%Y-%m-%d')}_orders.csv"
                client = RealOrderClient(log_path)
                client.on_fill = self._on_fill
                client.on_order = self._on_order
                client.on_disconnect = lambda: logger.error("[session] 交易 WS 斷線!")
                client.connect(account_id, password, pfx_path, pfx_password, is_test)
                with self._lock:
                    # 換掉舊 client (若有)
                    if self.broker:
                        try:
                            self.broker.disconnect()
                        except Exception:
                            pass
                    self.broker = client
                logger.info("[session] broker 連線完成")
            except Exception as e:
                logger.exception(f"[session] 連線失敗: {e}")
                with self._lock:
                    self.connect_error = str(e)
            finally:
                with self._lock:
                    self.connecting = False

        threading.Thread(target=_do, name="broker-connect", daemon=True).start()

    def disconnect(self):
        with self._lock:
            self.armed = False
            if self.broker:
                try:
                    self.broker.disconnect()
                except Exception:
                    pass
                self.broker = None

    def set_mode(self, mode: str):
        """切模式。切 real **不**要求先連線 — 切過去才看得到連線表單
        (連線要求放這裡會跟前端「real 模式才顯示表單」互鎖)。
        真正的安全閘門在 set_armed (連線健康 + 預算才准開始交易)。"""
        if mode not in ("sim", "real"):
            raise ValueError("mode 必須是 sim 或 real")
        with self._lock:
            self.mode = mode
            if mode == "sim":
                self.armed = False
        logger.info(f"[session] mode = {mode}")

    def set_params(self, total_budget: Optional[float] = None,
                   per_symbol_budget: Optional[float] = None,
                   sizing_mode: Optional[str] = None,
                   fixed_lots: Optional[int] = None):
        with self._lock:
            if sizing_mode is not None:
                if sizing_mode not in ("budget", "fixed_lots"):
                    raise ValueError("sizing_mode 必須是 budget 或 fixed_lots")
                self.sizing_mode = sizing_mode
            if fixed_lots is not None:
                if fixed_lots < 0:
                    raise ValueError("fixed_lots >= 0")
                self.fixed_lots = int(fixed_lots)
            if total_budget is not None:
                if total_budget < 0:
                    raise ValueError("total_budget >= 0")
                self.total_budget = float(total_budget)
            if per_symbol_budget is not None:
                if per_symbol_budget < 0:
                    raise ValueError("per_symbol_budget >= 0")
                self.per_symbol_budget = float(per_symbol_budget)
        logger.info(f"[session] 配置: mode={self.sizing_mode} 固定 {self.fixed_lots} 張 / "
                    f"總預算 {self.total_budget:,.0f} / 每檔 {self.per_symbol_budget:,.0f}")

    def set_armed(self, armed: bool):
        """kill switch。開啟前 pre-flight (day-trade 模式): 連線健康 + 預算已設。"""
        if armed:
            if self.mode != "real":
                raise RuntimeError("模擬模式不能開始交易 (先切真實模式)")
            if not self._broker_ready():
                raise RuntimeError("券商未連線或連線不健康")
            if self.total_budget <= 0:
                raise RuntimeError("先設定總預算 (> 0)")
            if self.sizing_mode == "fixed_lots":
                if self.fixed_lots <= 0:
                    raise RuntimeError("固定張數模式: 先設定每檔張數 (> 0)")
            elif self.per_symbol_budget <= 0:
                raise RuntimeError("依金額模式: 先設定每檔上限 (> 0)")
        with self._lock:
            self.armed = armed
        logger.warning(f"[session] ⚡ armed = {armed}")

    def _broker_ready(self) -> bool:
        return bool(self.broker and self.broker.connected and self.broker.healthy)

    def is_live(self) -> bool:
        """True = 真實模式 + armed + 連線健康 → 才會真下單。"""
        with self._lock:
            return self.mode == "real" and self.armed and self._broker_ready()

    # ─── 預算/張數 ─────────────────────────────────────────

    def _calc_lots(self, limit_up: float) -> int:
        """算該檔下單張數。caller 持鎖。總預算餘額永遠是硬上限。
        - budget 模式: floor(min(每檔上限, 總預算餘額) / (漲停價×1000))
        - fixed_lots 模式: min(fixed_lots, 總預算餘額能買的張數)"""
        cost_per_lot = limit_up * 1000
        if cost_per_lot <= 0:
            return 0
        remaining = self.total_budget - self.budget_used
        budget_cap = int(remaining // cost_per_lot)     # 總預算餘額能買幾張 (硬上限)
        if self.sizing_mode == "fixed_lots":
            return max(0, min(self.fixed_lots, budget_cap))
        # budget 模式 (預設): 再受每檔上限約束
        alloc = min(self.per_symbol_budget, remaining)
        return max(0, int(alloc // cost_per_lot))

    # ─── 08:59:58 預掛限價單 ───────────────────────────────

    def place_pre_orders(self, symbols: list, limit_ups: dict, stop_event=None):
        """08:59:58 對 marked 清單逐檔掛漲停價限價買單 (集合競價排隊)。

        **不重試** — 精準時點一次丟出 (使用者定案 2026-07-16)。失敗只記 log +
        釋放預算、**不設 stopped_reason** — 9:00 首筆成交時市價追會救回這檔。
        送單只過 50/s 硬底線閘門 (預掛窗口只有 2 秒,不能用 0.2s 慢節奏)。
        """
        if not self.is_live():
            logger.info("[session] 未 armed/未連線 — 跳過預掛")
            return
        logger.warning(f"[session] ⚡ 預掛限價單開始 — {len(symbols)} 檔候選")
        for sym in symbols:
            if stop_event is not None and stop_event.is_set():
                return
            limit_up = float(limit_ups.get(sym) or 0)
            with self._lock:
                if not limit_up:
                    continue
                st = self.trades.get(sym) or SymbolTrade(sym, limit_up)
                self.trades[sym] = st
                if st.stopped_reason or st.order_no:
                    continue
                # 只做漲停價 <= MAX_STOCK_PRICE 的股票 (0 = 不限)
                if self.max_stock_price > 0 and limit_up > self.max_stock_price:
                    st.stopped_reason = "price_above_max"
                    logger.info(f"[session] {sym} 漲停價 {limit_up} > {self.max_stock_price} → 跳過")
                    continue
                lots = self._calc_lots(limit_up)
                if lots <= 0:
                    st.stopped_reason = "budget_exhausted"
                    logger.info(f"[session] {sym} 預算不足 → 跳過")
                    continue
                st.target_lots = lots
                self.budget_used += lots * limit_up * 1000   # 下單即保留
            self._gate()                                     # 硬底線 50/s
            try:
                order_no = self.broker.place_limit_buy(sym, limit_up, lots)
                with self._lock:
                    st.order_no = order_no
                    st.order_kind = "pre_limit"
                    st.order_status = "pending"
                self._log_order(order_no, sym, "buy", "pre_limit", lots, limit_up)
            except Exception as e:
                logger.error(f"[session] {sym} 預掛失敗 (不重試,9:00 市價追會救): {e}")
                with self._lock:
                    self.budget_used -= lots * limit_up * 1000   # 釋放 (追單時重新保留)
        logger.warning("[session] 預掛限價單完成")

    # ─── 9:00 後事件 (trader 呼叫) ─────────────────────────

    def on_first_trade(self, symbol: str, passed_first_check: bool):
        """首筆成交資訊到達: 預掛單未(全)成交 → 撤剩餘;通過檢查 → 市價追差額。
        (在背景 thread 跑,不 block subscriber callback。)"""
        if not self.is_live():
            return
        threading.Thread(target=self._first_trade_worker,
                         args=(symbol, passed_first_check),
                         name=f"first-trade-{symbol}", daemon=True).start()

    def _first_trade_worker(self, symbol: str, passed: bool):
        with self._lock:
            st = self.trades.get(symbol)
            if st is None or st.exited:
                return
            order_no = st.order_no if st.order_status == "pending" else ""
            filled = st.filled_lots
            target = st.target_lots
            limit_up = st.limit_up

        # 1) 撤掉未成交的預掛單 (部分成交 → 撤剩餘、留已成交)
        if order_no and filled < target:
            self._gate()
            try:
                self.broker.cancel(order_no, symbol, reason="first_trade_unfilled")
                with self._lock:
                    st.order_status = "cancelled"
                    st.order_no = ""
                    # 釋放未成交部分預算 (市價追會重新保留)
                    self.budget_used -= (target - filled) * limit_up * 1000
            except Exception as e:
                logger.error(f"[session] {symbol} 撤預掛單失敗: {e}")
                return
            # 1b) 向券商查該單「權威成交量」— 防競態: 開盤已(部分)成交但 fill 回報
            #     比首筆成交 tick 慢 → 記憶體 filled_lots 偏低 → 差額算太大 → 雙倍買。
            self._gate(0.2)      # 委託/帳務查詢 5/s
            try:
                auth = self.broker.get_order_filled_lots(order_no)
            except Exception as e:
                logger.error(f"[session] {symbol} 查權威成交量失敗 ({e}) — 保守不追")
                return
            if auth < 0:
                logger.error(f"[session] {symbol} 查無委託 {order_no} — 保守不追")
                return
            with self._lock:
                if auth > st.filled_lots:
                    delta = auth - st.filled_lots
                    st.filled_lots = auth
                    self.budget_used += delta * limit_up * 1000   # 補回這部分保留
                    logger.warning(f"[session] {symbol} 權威成交量 {auth} 張 "
                                   f"(記憶體 {auth - delta}) — 已校正,防雙倍買")

        # 2) 沒通過第一盤檢查 → 停止 (淘汰路徑 trader 會另呼叫 on_discard)
        if not passed:
            with self._lock:
                st.stopped_reason = st.stopped_reason or "first_check_failed"
            return

        # 3) 通過 → 市價單追差額。**重試無上限直到成功** (使用者定案 2026-07-16):
        #    - 必等上一筆「委託結果」回來 (place_order 是阻塞呼叫,return = 結果回來)
        #    - 距上次送單 >= order_min_interval (0.2s;富邦下單上限 50/s,不搞退避)
        #    - 停止條件: kill switch 關 / 該檔淘汰或出場 / CANCEL_PENDING_TIME 已到
        with self._lock:
            shortfall = st.target_lots - st.filled_lots
            if shortfall <= 0 or st.stopped_reason:
                return
            self.budget_used += shortfall * limit_up * 1000
        attempt = 0
        abort_reason = ""
        while True:
            if not self.is_live():
                abort_reason = "kill_switch_off"
                break
            with self._lock:
                if st.stopped_reason or st.exited:
                    abort_reason = st.stopped_reason or "exited"
                    break
            if (self.cancel_pending_time is not None
                    and datetime.now().time() >= self.cancel_pending_time):
                abort_reason = "cancel_pending_time"
                break
            self._gate(self.order_min_interval)
            attempt += 1
            try:
                new_no = self.broker.place_market_buy(symbol, shortfall)
                with self._lock:
                    st.order_no = new_no
                    st.order_kind = "market_buy"
                    st.order_status = "pending"
                self._log_order(new_no, symbol, "buy", "market_buy", shortfall, 0)
                logger.warning(f"[session] {symbol} 市價追 {shortfall} 張 → 委託成功 "
                               f"(第 {attempt} 次)")
                return
            except Exception as e:
                logger.error(f"[session] {symbol} 市價追失敗 (第 {attempt} 次,續試): {e}")
        # 中止 → 釋放保留的預算
        with self._lock:
            self.budget_used -= shortfall * limit_up * 1000
        logger.warning(f"[session] {symbol} 市價追中止 ({abort_reason}, 共試 {attempt} 次)")

    def cancel_symbol_orders(self, symbol: str, reason: str):
        """撤該檔 pending 委託 (unmark/首盤淘汰/qty_drop_half 用)。不賣持倉。"""
        if not self.is_live():
            return
        with self._lock:
            st = self.trades.get(symbol)
            if st is None or not st.order_no or st.order_status != "pending":
                if st is not None and reason:
                    st.stopped_reason = st.stopped_reason or reason
                return
            order_no = st.order_no
            unfilled = max(0, st.target_lots - st.filled_lots)
            limit_up = st.limit_up
        self._gate()
        try:
            self.broker.cancel(order_no, symbol, reason=reason)
            with self._lock:
                st.order_status = "cancelled"
                st.order_no = ""
                st.stopped_reason = st.stopped_reason or reason
                self.budget_used -= unfilled * limit_up * 1000
            self._mark_order(order_no, "cancelled")
            logger.warning(f"[session] {symbol} 撤單 ({reason})")
        except Exception as e:
            logger.error(f"[session] {symbol} 撤單失敗: {e}")

    def exit_position(self, symbol: str, reason: str):
        """委賣出現 → 立刻出場: 撤 pending + 市價賣出全部已成交。(背景 thread)"""
        if not self.is_live():
            return
        threading.Thread(target=self._exit_worker, args=(symbol, reason),
                         name=f"exit-{symbol}", daemon=True).start()

    def _exit_worker(self, symbol: str, reason: str):
        with self._lock:
            st = self.trades.get(symbol)
            if st is None or st.exited:
                return
            st.exited = True            # 先標,防重複觸發
        self.cancel_symbol_orders(symbol, reason)
        with self._lock:
            lots = st.filled_lots
        if lots > 0:
            # 出場賣單: 試 3 次 (每次過閘門+等回報);全失敗 → exited 還原,
            # 下一個委賣 tick 會再觸發 (等效持續重試,但不在這裡空轉)
            for attempt in range(1, 4):
                self._gate(self.order_min_interval)
                try:
                    sell_no = self.broker.place_market_sell(symbol, lots, reason)
                    self._log_order(sell_no, symbol, "sell", "market_sell", lots, 0)
                    logger.warning(f"[session] ⚠ {symbol} 出場 — 市價賣 {lots} 張 ({reason})")
                    return
                except Exception as e:
                    logger.error(f"[session] {symbol} 出場賣單失敗 (第 {attempt}/3 次): {e}")
            with self._lock:
                st.exited = False   # 賣失敗,允許下一次觸發重試
            logger.error(f"[session] {symbol} 出場賣單 3 次全失敗!部位 {lots} 張仍在")

    def cancel_all_pending(self, reason: str):
        """撤所有 pending,不賣持倉 (留倉)。13:23 (CANCEL_PENDING_TIME) 主跑,13:24 收盤保險再跑。"""
        if not self.is_live():
            return
        with self._lock:
            syms = [s for s, st in self.trades.items()
                    if st.order_no and st.order_status == "pending"]
        for sym in syms:
            self.cancel_symbol_orders(sym, reason)
        logger.warning(f"[session] 收盤撤單完成 ({len(syms)} 檔) — 持倉保留")

    def close_all(self):
        """緊急全平: 撤全部 pending + 市價賣出全部持倉。"""
        if not self._broker_ready():
            raise RuntimeError("券商未連線")
        with self._lock:
            snapshot = [(s, st.filled_lots) for s, st in self.trades.items()]
        for sym, _ in snapshot:
            self.cancel_symbol_orders(sym, "close_all")
        sold = 0
        for sym, lots in snapshot:
            if lots > 0:
                self._gate()
                try:
                    sell_no = self.broker.place_market_sell(sym, lots, "close_all")
                    self._log_order(sell_no, sym, "sell", "market_sell", lots, 0)
                    with self._lock:
                        self.trades[sym].exited = True
                    sold += 1
                except Exception as e:
                    logger.error(f"[session] close_all 賣 {sym} 失敗: {e}")
        logger.warning(f"[session] 🚨 緊急全平: 賣出 {sold} 檔")
        return sold

    # ─── broker 回報 ───────────────────────────────────────

    def _on_fill(self, fill: dict):
        """成交回報 → 更新 filled_lots/avg_price。去重 (day-trade 模式)。"""
        key = f"{fill['order_no']}:{fill['filled_no']}:{fill['filled_time']}:{fill['lots']}"
        with self._lock:
            if key in self._processed_fills:
                return
            self._processed_fills.add(key)
            # 只認「策略下過的單」(order_no ∈ order_log) — 同帳號手動單的成交
            # 不能混進策略部位 (否則差額算錯 + 出場連手動部位一起賣)
            if fill["order_no"] not in self.order_log:
                logger.info(f"[session] 非策略單成交,忽略: {fill['symbol']} "
                            f"order={fill['order_no']} {fill['lots']} 張")
                return
            # 委託總表同步 (前端顯示)
            row = self.order_log.get(fill["order_no"])
            if row is not None:
                row["filled_lots"] += fill["lots"]
                if row["filled_lots"] >= row["lots"]:
                    row["status"] = "filled"
            st = self.trades.get(fill["symbol"])
            if st is None:
                logger.warning(f"[session] 未知標的成交回報: {fill}")
                return
            if fill["action"] == "buy":
                prev_cost = st.avg_price * st.filled_lots
                st.filled_lots += fill["lots"]
                if st.filled_lots > 0:
                    st.avg_price = (prev_cost + fill["price"] * fill["lots"]) / st.filled_lots
                if st.filled_lots >= st.target_lots:
                    st.order_status = "done"
                    st.order_no = ""
            else:   # sell (出場)
                st.filled_lots = max(0, st.filled_lots - fill["lots"])

    def _on_order(self, rpt: dict):
        """委託回報 — 交易所拒單時標 rejected (place_order 同步成功但交易所退)。"""
        if not rpt.get("error_message"):
            return
        self._mark_order(rpt.get("order_no", ""), "rejected")
        with self._lock:
            st = self.trades.get(rpt.get("symbol", ""))
            if st is not None and st.order_no == rpt.get("order_no"):
                st.order_status = "rejected"
                st.order_no = ""
                logger.error(f"[session] {st.symbol} 交易所拒單: {rpt['error_message']}")

    # ─── 委託總表 (前端顯示 + 右鍵刪單) ────────────────────

    def get_orders(self) -> list:
        """全部委託 (新的在前) — 前端委託狀態表用。"""
        with self._lock:
            return [dict(r) for r in reversed(list(self.order_log.values()))]

    def cancel_order_by_no(self, order_no: str):
        """手動刪單 (前端右鍵)。刪的是進場買單時 → 該檔停止進場 (manual_cancel)。"""
        if not self._broker_ready():
            raise RuntimeError("券商未連線")
        with self._lock:
            row = self.order_log.get(order_no)
            if row is None:
                raise ValueError(f"查無委託 {order_no}")
            if row["status"] != "pending":
                raise ValueError(f"委託 {order_no} 狀態 {row['status']},不可刪")
            symbol = row["symbol"]
            is_buy = row["action"] == "buy"
        self._gate()
        self.broker.cancel(order_no, symbol, reason="manual_cancel")
        self._mark_order(order_no, "cancelled")
        with self._lock:
            st = self.trades.get(symbol)
            if st is not None and st.order_no == order_no:
                unfilled = max(0, st.target_lots - st.filled_lots)
                st.order_status = "cancelled"
                st.order_no = ""
                if is_buy:
                    st.stopped_reason = st.stopped_reason or "manual_cancel"
                    self.budget_used -= unfilled * st.limit_up * 1000
        logger.warning(f"[session] 手動刪單 {order_no} ({symbol})")

    # ─── 查詢 ──────────────────────────────────────────────

    def has_exposure(self, symbol: str) -> bool:
        """該檔有未成交委託或持倉 (且未出場) — trader 判斷要不要觸發出場用。"""
        with self._lock:
            st = self.trades.get(symbol)
            if st is None or st.exited:
                return False
            return st.filled_lots > 0 or st.order_status == "pending"

    def has_pending(self, symbol: str) -> bool:
        """該檔有排隊中委託 — trader「排隊中量減半→撤單」判斷用。"""
        with self._lock:
            st = self.trades.get(symbol)
            return st is not None and st.order_status == "pending"

    def get_filled_lots(self, symbol: str) -> int:
        """該檔已成交張數 (trader 淘汰時判斷要不要先市價賣掉部位)。"""
        with self._lock:
            st = self.trades.get(symbol)
            return st.filled_lots if st else 0

    def get_symbol_state(self, symbol: str) -> Optional[dict]:
        with self._lock:
            st = self.trades.get(symbol)
            return st.to_dict() if st else None

    def status(self) -> dict:
        with self._lock:
            b = self.broker.status() if self.broker else {
                "connected": False, "healthy": False, "account_masked": "",
                "is_test": False, "error": ""}
            return {
                "mode": self.mode,
                "armed": self.armed,
                "connecting": self.connecting,
                "connect_error": self.connect_error,
                **b,
                "params": {
                    "sizing_mode": self.sizing_mode,
                    "fixed_lots": self.fixed_lots,
                    "total_budget": self.total_budget,
                    "per_symbol_budget": self.per_symbol_budget,
                },
                "budget_used": round(self.budget_used, 0),
                "n_symbols": len(self.trades),
                "n_positions": sum(1 for s in self.trades.values() if s.filled_lots > 0),
            }
