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
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)

_PLACE_RETRY = 3          # 下單失敗重試次數
_RETRY_SLEEP = 0.5        # 重試間隔秒


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
        # 預算 (雙層): 進場前 used + cost <= total 才下
        self.total_budget: float = 0.0
        self.per_symbol_budget: float = 0.0
        self.budget_used: float = 0.0     # 下單時保留,撤單釋放未成交部分
        # per-symbol state
        self.trades: Dict[str, SymbolTrade] = {}
        self._processed_fills: set = set()   # 去重 key

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
        if mode not in ("sim", "real"):
            raise ValueError("mode 必須是 sim 或 real")
        if mode == "real" and not self._broker_ready():
            raise RuntimeError("切真實模式前要先連線券商")
        with self._lock:
            self.mode = mode
            if mode == "sim":
                self.armed = False
        logger.info(f"[session] mode = {mode}")

    def set_params(self, total_budget: Optional[float] = None,
                   per_symbol_budget: Optional[float] = None):
        with self._lock:
            if total_budget is not None:
                if total_budget < 0:
                    raise ValueError("total_budget >= 0")
                self.total_budget = float(total_budget)
            if per_symbol_budget is not None:
                if per_symbol_budget < 0:
                    raise ValueError("per_symbol_budget >= 0")
                self.per_symbol_budget = float(per_symbol_budget)
        logger.info(f"[session] 預算: 總 {self.total_budget:,.0f} / 每檔 {self.per_symbol_budget:,.0f}")

    def set_armed(self, armed: bool):
        """kill switch。開啟前 pre-flight (day-trade 模式): 連線健康 + 預算已設。"""
        if armed:
            if self.mode != "real":
                raise RuntimeError("模擬模式不能開始交易 (先切真實模式)")
            if not self._broker_ready():
                raise RuntimeError("券商未連線或連線不健康")
            if self.total_budget <= 0 or self.per_symbol_budget <= 0:
                raise RuntimeError("先設定總預算與每檔上限 (皆需 > 0)")
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
        """雙層預算: floor(min(每檔上限, 總預算餘額) / (漲停價×1000))。caller 持鎖。"""
        cost_per_lot = limit_up * 1000
        if cost_per_lot <= 0:
            return 0
        remaining = self.total_budget - self.budget_used
        alloc = min(self.per_symbol_budget, remaining)
        return max(0, int(alloc // cost_per_lot))

    # ─── 08:59:58 預掛限價單 ───────────────────────────────

    def place_pre_orders(self, symbols: list, limit_ups: dict, stop_event=None):
        """對 marked 清單逐檔掛漲停價限價買單 (集合競價排隊)。
        依序下、等 place_order 同步結果、失敗重試 ≤3。"""
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
                lots = self._calc_lots(limit_up)
                if lots <= 0:
                    st.stopped_reason = "budget_exhausted"
                    logger.info(f"[session] {sym} 預算不足 → 跳過")
                    continue
                st.target_lots = lots
                self.budget_used += lots * limit_up * 1000   # 下單即保留
            order_no = self._place_with_retry(
                lambda: self.broker.place_limit_buy(sym, limit_up, lots), sym)
            with self._lock:
                if order_no:
                    st.order_no = order_no
                    st.order_kind = "pre_limit"
                    st.order_status = "pending"
                else:
                    st.stopped_reason = "pre_order_failed"
                    self.budget_used -= lots * limit_up * 1000   # 釋放
        logger.warning("[session] 預掛限價單完成")

    def _place_with_retry(self, fn, sym: str) -> str:
        for attempt in range(1, _PLACE_RETRY + 1):
            try:
                return fn()
            except Exception as e:
                logger.error(f"[session] {sym} 下單失敗 (第 {attempt}/{_PLACE_RETRY} 次): {e}")
                if attempt < _PLACE_RETRY:
                    time.sleep(_RETRY_SLEEP)
        return ""

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

        # 2) 沒通過第一盤檢查 → 停止 (淘汰路徑 trader 會另呼叫 on_discard)
        if not passed:
            with self._lock:
                st.stopped_reason = st.stopped_reason or "first_check_failed"
            return

        # 3) 通過 → 市價單追差額
        with self._lock:
            shortfall = st.target_lots - st.filled_lots
            if shortfall <= 0 or st.stopped_reason:
                return
            self.budget_used += shortfall * limit_up * 1000
        order_no = self._place_with_retry(
            lambda: self.broker.place_market_buy(symbol, shortfall), symbol)
        with self._lock:
            if order_no:
                st.order_no = order_no
                st.order_kind = "market_buy"
                st.order_status = "pending"
                logger.warning(f"[session] {symbol} 預掛未滿 → 市價追 {shortfall} 張")
            else:
                st.stopped_reason = "market_chase_failed"
                self.budget_used -= shortfall * limit_up * 1000

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
        try:
            self.broker.cancel(order_no, symbol, reason=reason)
            with self._lock:
                st.order_status = "cancelled"
                st.order_no = ""
                st.stopped_reason = st.stopped_reason or reason
                self.budget_used -= unfilled * limit_up * 1000
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
            order_no = self._place_with_retry(
                lambda: self.broker.place_market_sell(symbol, lots, reason), symbol)
            if order_no:
                logger.warning(f"[session] ⚠ {symbol} 出場 — 市價賣 {lots} 張 ({reason})")
            else:
                with self._lock:
                    st.exited = False   # 賣失敗,允許重試 (下一次觸發)
                logger.error(f"[session] {symbol} 出場賣單失敗!部位 {lots} 張仍在")

    def cancel_all_pending(self, reason: str):
        """13:24 收盤: 撤所有 pending,不賣持倉 (留倉)。"""
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
                try:
                    self.broker.place_market_sell(sym, lots, "close_all")
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
        with self._lock:
            st = self.trades.get(rpt.get("symbol", ""))
            if st is not None and st.order_no == rpt.get("order_no"):
                st.order_status = "rejected"
                st.order_no = ""
                logger.error(f"[session] {st.symbol} 交易所拒單: {rpt['error_message']}")

    # ─── 查詢 ──────────────────────────────────────────────

    def has_exposure(self, symbol: str) -> bool:
        """該檔有未成交委託或持倉 (且未出場) — trader 判斷要不要觸發出場用。"""
        with self._lock:
            st = self.trades.get(symbol)
            if st is None or st.exited:
                return False
            return st.filled_lots > 0 or st.order_status == "pending"

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
                    "total_budget": self.total_budget,
                    "per_symbol_budget": self.per_symbol_budget,
                },
                "budget_used": round(self.budget_used, 0),
                "n_symbols": len(self.trades),
                "n_positions": sum(1 for s in self.trades.values() if s.filled_lots > 0),
            }
