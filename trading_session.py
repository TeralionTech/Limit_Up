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
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# 富邦 API 明定速率上限: 下單 50/s、批次下單 10/s、帳務查詢 5/s、連線數 10。
# 送單 (place/cancel) 走 SendRateLimiter 爆發式滑動窗口 (預設 45/s,留 margin);
# 帳務/委託查詢另走 _query_gate 間隔閘門 (5/s)。
_HARD_MIN_INTERVAL = 0.02      # order_min_interval (追單失敗退避) 的下限

# 出場賣重試上限 (進場市價追**無上限** — 使用者定案 2026-07-27,試到成功為止;
# 出場失敗後下一個委賣 tick 會再觸發,行情節奏自然重試,故有限次即可。
# 隔日賣 5 次上限見 _overnight_sell_worker, fd451ce):
DEFAULT_SELL_MAX_TRIES = 8     # 出場賣: 指數退避 0.2→…→5s;用盡記 CRITICAL + sell_failed 旗標

# 出場 worker 讀到 0 張時「等成交回報」的窗口 — 支撐消失觸發出場的同一瞬間可能
# 剛好成交 (回報比行情 tick 慢 ~百 ms 級);等到就照樣賣掉,沒等到 → exited 回退,
# 晚到的部位由下一個出場訊號接手 (2026-08-12 使用者確認的競態處理)
_EXIT_FILL_WAIT_SEC = 3.0

# 停止拒因關鍵字 — 拒單訊息含這些字 = 重試無意義,第一筆被拒就停止該檔:
#   全額/預收/圈存: 全額交割/處置股,API 下單今日必不成功 (2026-08-13 6225 事故,
#                   T30 名單漏抓時的第二層保險)
#   價格穩定: 「證券委託觸及價格穩定措施上、下限價格」— 瞬間價格穩定措施冷卻期內市價單
#             必被拒,重試只會狂送 (2026-08-21 6144 事故: 26 秒狂送 100 筆)
_FATAL_REJECT_KEYWORDS = ("全額", "預收", "圈存", "價格穩定")


def _is_fatal_reject(err) -> bool:
    msg = str(err)
    return any(k in msg for k in _FATAL_REJECT_KEYWORDS)


class SendRateLimiter:
    """爆發式送單風控 — 滑動窗口: 過去 1 秒內 < max_per_sec 筆就**立刻放行**,
    滿了才等最舊的一筆滑出窗口。

    與「每筆間隔 0.02s」均勻鋪開不同: 45 筆可以在一秒的最前面全部送出
    (搶 08:59:58 預掛 2 秒窗口),仍保證任一 1 秒窗口內不超過 max_per_sec。
    進場/出場/撤單全 thread 共用同一額度。"""

    def __init__(self, max_per_sec: int = 45):
        self._lock = threading.Lock()
        self._stamps: deque = deque()       # monotonic 送出時間戳 (只留最近 1 秒)
        self.max_per_sec = max_per_sec

    def acquire(self):
        while True:
            with self._lock:
                now = time.monotonic()
                while self._stamps and now - self._stamps[0] >= 1.0:
                    self._stamps.popleft()
                if len(self._stamps) < self.max_per_sec:
                    self._stamps.append(now)
                    return
                wait = self._stamps[0] + 1.0 - now
            time.sleep(max(wait, 0.001))    # 鎖外睡 — 不擋其他 thread 檢查窗口


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
        self.budget_reserved = 0.0     # 該檔目前保留中的預算 (下單保留,成交轉消耗,撤/拒釋放)
        self.sell_failed = False       # 出場賣單重試用盡仍沒送出 → 需人工 (前端顯示)
        self.first_trade_fired = False  # 首筆成交已處理 — 早期觸發/trader/重複 tick 冪等用
        self.is_disposition = False    # 處置股: 不撤預掛/不下市價買、出場改委買一價限價賣
        self.last_bid1_price = 0.0     # 最近委買一價 (處置股出場限價賣用)

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
            "is_disposition": self.is_disposition,
            "sell_failed": self.sell_failed,
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
        self._output_dir: Optional[Path] = None   # 連線後設 — 成交台帳 fills.csv 落檔用
        # 委託總表 (前端委託狀態顯示 + 右鍵刪單) — key=order_no, 插入序
        self.order_log: Dict[str, dict] = {}
        # 處置股名單 (runner 從 ticker.isDisposition 填) — 影響下單機制
        self.dispositions: Dict[str, bool] = {}
        # 跌停價名單 (runner 從 ticker.limitDownPrice 填) — 所有出場含隔日賣
        # 都掛「跌停價限價賣」(2026-08-12 定案: 成交優先權等同市價,但集合競價
        # 時段合法、處置股合法、永不被「不可市價」拒單)
        self.limit_downs: Dict[str, float] = {}
        # T30 禁單名單 (全額交割 SETTYPE≠0 / 每筆需 100% 預收 MARK-W=2) —
        # API 下單必被拒,預掛/市價追一律跳過 (2026-08-12 狂送單事故)
        self.untradable: set = set()
        # 隔日賣標的 (昨天買到、收盤未出場的持倉) — 純盤面規則 (2026-08-16 定案):
        # 委買一跌下今日漲停 → 跌停價限價賣;鎖著 (市價列在/買牆在) → 抱著
        self.overnight: Dict[str, dict] = {}
        # 隔日賣標的「今日漲停價」(鎖漲停續抱判斷用) — ⚠ 獨立於 runner.limit_ups,
        # 絕不可混入 (filter closure 捕獲該 dict,混入會把昨日持倉當新標的預掛加碼)
        self.overnight_limit_ups: Dict[str, float] = {}
        # 策略參數 (runner 啟動時從 cfg configure;預設值供測試/未 configure 時用)
        self.max_stock_price: float = 500.0        # 只做漲停價 <= 此價 (0=不限)
        self.order_min_interval: float = 0.2       # 市價追單最小送單間隔
        self.cancel_pending_time = None            # datetime.time;此時後不再追單
        # 爆發式送單風控 — 進場/出場/撤單共用 45/s 滑動窗口 (一秒最前面可全部送出)
        self._rate = SendRateLimiter(45)
        # 帳務/委託查詢閘門 (富邦 5/s) — 與送單額度分開計
        self._send_lock = threading.Lock()
        self._last_send = 0.0
        # fills.csv 寫檔鎖 — SDK 回報可能多 thread 併發,防雙表頭/行交錯
        self._fills_lock = threading.Lock()
        # 交易日 (roll_day 每日重置用)
        self._trade_date: str = ""
        # 今日已預掛過的日期 — place_pre_orders 冪等保護 (防重複 timer 雙倍下單)
        self._pre_orders_date: str = ""

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
            self.dispositions = {}
            self.limit_downs = {}    # 每日價格不同,新日清空由 runner 重填
            self.untradable = set()  # T30 名單每日重載
            self.overnight = {}      # 隔日賣清單由 runner 讀檔重建 (roll_day 後才 load)
            self.overnight_limit_ups = {}   # 每日漲停價不同,runner 補查重填
            self._pre_orders_date = ""   # 新交易日 → 允許今日預掛
        logger.warning(f"[session] roll_day({date_str}) — 新交易日: 清 {had} 檔前日 state,"
                       f"armed=False (要交易請重新 arm)")

    def set_dispositions(self, dispositions: Dict[str, bool]):
        """runner 抓完 ticker 後把處置股名單交進來。"""
        with self._lock:
            self.dispositions = dict(dispositions or {})
        n = sum(1 for v in self.dispositions.values() if v)
        logger.info(f"[session] 處置股名單: {n} 檔")

    def set_limit_downs(self, limit_downs: Dict[str, float]):
        """runner 抓完 ticker 後把跌停價名單交進來 (出場跌停限價賣用)。"""
        with self._lock:
            self.limit_downs = dict(limit_downs or {})
        logger.info(f"[session] 跌停價名單: {len(self.limit_downs)} 檔")

    def set_overnight_limit_ups(self, ups: Dict[str, float]):
        """runner 補查隔日賣標的今日漲停價後交進來 (鎖漲停續抱判斷用)。"""
        with self._lock:
            self.overnight_limit_ups = dict(ups or {})
        logger.info(f"[session] 隔日賣漲停價名單: {len(self.overnight_limit_ups)} 檔")

    def set_untradable(self, symbols: set):
        """runner 從 T30 檔載入禁單名單 (全額交割 / 每筆需 100% 預收)。"""
        with self._lock:
            self.untradable = set(symbols or ())
        logger.info(f"[session] T30 禁單名單 (全額交割/需預收): {len(self.untradable)} 檔")

    def update_bid1(self, symbol: str, bid1_price: float):
        """trader 每 tick 更新委買一價 (處置股出場限價賣用)。只對有下單的檔記錄。"""
        if bid1_price <= 0:
            return
        with self._lock:
            st = self.trades.get(symbol)
            if st is not None:
                st.last_bid1_price = bid1_price

    def configure(self, max_stock_price: float = None,
                  order_min_interval_sec: float = None,
                  cancel_pending_time: str = None,
                  order_max_per_sec: int = None):
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
            if order_max_per_sec is not None:
                v = int(order_max_per_sec)
                if v > 50:
                    logger.warning(f"[session] ORDER_MAX_PER_SEC={v} 超過富邦下單上限 50 → 強制 50")
                    v = 50
                self._rate.max_per_sec = max(1, v)
        logger.info(f"[session] configure: max_price={self.max_stock_price} "
                    f"backoff={self.order_min_interval}s rate={self._rate.max_per_sec}/s "
                    f"cancel_at={self.cancel_pending_time}")

    def _query_gate(self, min_interval: float = 0.2):
        """帳務/委託查詢閘門 (富邦 5/s) — 距上次查詢不足 min_interval 就等。
        送單**不走這裡** (送單走 self._rate 爆發式窗口)。"""
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
        self._output_dir = output_dir            # 成交台帳 fills.csv 落檔用
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
                client.on_disconnect = lambda: logger.critical("[session] ⚠ 交易 WS 斷線! (自動重連中)")
                client.on_reconnected = self._on_broker_reconnected   # 重連成功 → 補收
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
                # 連線後對帳庫存 → 隔日賣清單以券商實際庫存為準
                try:
                    self.refresh_overnight_inventory()
                except Exception as e:
                    logger.error(f"[session] 連線後對帳庫存例外: {e}")
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
        """True = 真實模式 + armed + 連線健康 → 才會真下單。**只給進場路徑用**。"""
        with self._lock:
            return self.mode == "real" and self.armed and self._broker_ready()

    def _can_manage(self) -> bool:
        """出場/撤單閘門 — 真實模式 + broker 物件在就嘗試 (不看 armed、不看連線健康)。

        armed 只擋「新進場」;kill switch 關掉或交易 WS 斷線時,已有的持倉/委託
        **仍必須可管理** — 否則關 kill switch = 13:23 撤單失效 + 部位賣不掉。
        連線不健康時照樣嘗試送出,失敗記 CRITICAL (不預先擋掉)。"""
        with self._lock:
            return self.mode == "real" and self.broker is not None

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

    # 預算不變式: budget_used == Σ(買進已成交 × 漲停價 × 1000) + Σ(st.budget_reserved)。
    # 下單前 _reserve_budget 保留;買成交在 _on_fill 把保留轉消耗;撤單/拒單/追單中止
    # _release_budget 釋放剩餘保留。賣出成交**不退**預算 (保守日預算,使用者定案)。
    # 所有 budget_used 增減只准走這兩支 — 別再散落手算。

    def _reserve_budget(self, st: "SymbolTrade", lots: int):
        """下單前保留預算 (caller 持鎖)。"""
        amt = lots * st.limit_up * 1000
        self.budget_used += amt
        st.budget_reserved += amt

    def _release_budget(self, st: "SymbolTrade"):
        """釋放該檔剩餘保留 (撤單/拒單/追單中止;caller 持鎖)。冪等 — 重複呼叫無害。"""
        amt = st.budget_reserved
        st.budget_reserved = 0.0
        self.budget_used = max(0.0, self.budget_used - amt)

    # ─── 08:59:58 預掛限價單 ───────────────────────────────

    def place_pre_orders(self, symbols: list, limit_ups: dict, stop_event=None):
        """08:59:58 對 marked 清單逐檔掛漲停價限價買單 (集合競價排隊)。

        **不重試** — 精準時點一次丟出 (使用者定案 2026-07-16)。失敗只記 log +
        釋放預算、**不設 stopped_reason** — 9:00 首筆成交時市價追會救回這檔。
        送單過爆發式風控 (45/s 滑動窗口) — 一秒最前面可全部送出,搶預掛 2 秒窗口。
        """
        if not self.is_live():
            logger.info("[session] 未 armed/未連線 — 跳過預掛")
            return
        # 冪等保護 (check-and-set 原子): 就算 timer 生命週期修壞了、兩個 timer 同時到,
        # 也只有一個能預掛 (放在 is_live 之後 — 未 armed 的早退不燒掉今日額度)。
        with self._lock:
            if self._trade_date and self._pre_orders_date == self._trade_date:
                logger.warning("[session] 今日已預掛過 — 跳過重複預掛 (冪等保護)")
                return
            self._pre_orders_date = self._trade_date
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
                st.is_disposition = self.dispositions.get(sym, False)
                # T30 禁單: 全額交割/每筆需 100% 預收 — API 下單必被拒,整檔跳過
                # (stopped_reason 同時擋掉 9:00 的市價追,狂送單根絕)
                if sym in self.untradable:
                    st.stopped_reason = "full_cash_delivery"
                    logger.warning(f"[session] {sym} 全額交割/需預收款券 (T30) → 跳過不下單")
                    continue
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
                self._reserve_budget(st, lots)               # 下單即保留
            self._rate.acquire()                             # 爆發式 45/s 窗口
            try:
                order_no = self.broker.place_limit_buy(sym, limit_up, lots)
                with self._lock:
                    st.order_no = order_no
                    st.order_kind = "pre_limit"
                    st.order_status = "pending"
                self._log_order(order_no, sym, "buy", "pre_limit", lots, limit_up)
            except Exception as e:
                if _is_fatal_reject(e):
                    # 致命拒因 (全額交割/預收圈存) — 今日必不成功,市價追也不准救
                    with self._lock:
                        st.stopped_reason = "fatal_reject"
                        self._release_budget(st)
                    logger.critical(f"[session] ⚠ {sym} 預掛致命拒因 → 今日停止此檔 "
                                    f"(T30 名單可能漏抓,請檢查): {e}")
                else:
                    logger.error(f"[session] {sym} 預掛失敗 (不重試,9:00 市價追會救): {e}")
                    with self._lock:
                        self._release_budget(st)             # 釋放 (追單時重新保留)
        logger.warning("[session] 預掛限價單完成")

    # ─── 9:00 後事件 (trader 呼叫) ─────────────────────────

    def on_first_trade(self, symbol: str, passed_first_check: bool):
        """首筆成交資訊到達 → **市價單優先**搶進 (背景 thread,不 block subscriber callback)。

        冪等: 08:59:58 起的早期 handler 每個 trade tick 都會呼叫,trader 接手後
        也會再觸發 — 這裡便宜預過濾 (st 不在 / 已處理過 → 不開 thread),
        權威去重旗標在 worker 內鎖內 check-and-set。"""
        if not self.is_live():
            return
        with self._lock:
            st = self.trades.get(symbol)
            if st is None or st.first_trade_fired:
                return
        threading.Thread(target=self._first_trade_worker,
                         args=(symbol, passed_first_check),
                         name=f"first-trade-{symbol}", daemon=True).start()

    def _first_trade_worker(self, symbol: str, passed: bool):
        """**市價單優先** (使用者定案 2026-08-03,速度至上):

        首筆成交 tick → **立刻**下市價單追差額 — 差額只看記憶體 filled_lots,
        **不先撤預掛、不向券商查權威成交量** (舊 2026-07-22「查無→保守不追」定案廢除)。
        市價單出手後才回頭撤預掛剩餘。單筆下單往返 ~60ms (orders CSV latency_ms),
        故 tick → 市價單送出 ≈ 一次往返內。

        超買風險 (使用者接受「多下到沒關係」): 預掛+市價都成交時最多持有 2×target;
        出場賣 filled_lots 全量,兩單成交都會累進,超買部位一樣被完整賣掉。
        預算為近似值: 差額轉移保留,兩單都成交時實際花費可超過保留額。"""
        with self._lock:
            st = self.trades.get(symbol)
            if st is None or st.exited:
                return
            if st.first_trade_fired:
                return              # 權威去重 (早期觸發/trader/重複 tick 只處理一次)
            st.first_trade_fired = True
            # 處置股: 保留 08:59:58 的漲停價預掛限價單 — 不撤、不下市價追
            # (處置股不能下市價單)。留著等分盤集合競價成交。
            if st.is_disposition:
                logger.warning(f"[session] {symbol} 處置股 — 保留預掛限價單,不撤不追市價")
                return
            pre_order_no = st.order_no if st.order_status == "pending" else ""

        # 沒通過第一盤檢查 → 撤預掛 + 停止,不下市價單
        if not passed:
            with self._lock:
                st.stopped_reason = st.stopped_reason or "first_check_failed"
            if pre_order_no:
                self._rate.acquire()
                try:
                    self.broker.cancel(pre_order_no, symbol, reason="first_check_failed")
                    with self._lock:
                        st.order_status = "cancelled"
                        st.order_no = ""
                        self._release_budget(st)
                    # 同步委託總表 (漏這行前端會永遠顯示排隊中 — 2026-07-22 實測 bug)
                    self._mark_order(pre_order_no, "cancelled")
                except Exception as e:
                    logger.error(f"[session] {symbol} 撤預掛單失敗: {e}")
            return

        # 通過 → 差額只看記憶體 (fill 回報通常還沒到 → shortfall ≈ target),先搶單
        with self._lock:
            if st.stopped_reason:
                return
            shortfall = st.target_lots - st.filled_lots
            if shortfall > 0:
                # 預算轉移: 預掛剩餘保留 → 市價追保留 (不等撤單完成,速度優先)
                self._release_budget(st)
                self._reserve_budget(st, shortfall)

        # 市價追: **重試到成功為止** (使用者定案 2026-07-27)。送單全程過爆發式風控
        # (45/s 滑動窗口,多標的共用,不超富邦上限);失敗後單檔退避 order_min_interval
        # (0.2s),避免單一失敗標的獨佔共用額度。
        # 中止只在: kill switch 關 / 該檔淘汰或出場 / 13:23 (CANCEL_PENDING_TIME) 到。
        if shortfall > 0:
            attempt = 0
            abort_reason = ""
            chased_ok = False
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
                self._rate.acquire()      # 爆發式 45/s 窗口 (多標的共用)
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
                    chased_ok = True
                    break
                except Exception as e:
                    if _is_fatal_reject(e):
                        # 停止拒因 — 第一筆被拒就停,不再狂送 (全額交割 6225 / 價格穩定 6144)。
                        # 成因見拒單訊息 {e} (全額交割→查 T30 名單;價格穩定→冷卻期市價單必拒)。
                        with self._lock:
                            st.stopped_reason = "fatal_reject"
                        abort_reason = "fatal_reject"
                        logger.critical(f"[session] ⚠ {symbol} 市價追遇停止拒因 → 立即放棄"
                                        f"此檔: {e}")
                        break
                    # log 防洪: 前 5 次全印,之後每 50 次一次 (無上限重試最壞 5 log/s/檔)
                    if attempt <= 5 or attempt % 50 == 0:
                        logger.error(f"[session] {symbol} 市價追失敗 (第 {attempt} 次,"
                                     f"續試至成功): {e}")
                    time.sleep(self.order_min_interval)   # 單檔退避 (不佔共用額度)
            if not chased_ok:
                # 中止 → 釋放保留的預算
                with self._lock:
                    self._release_budget(st)
                logger.warning(f"[session] {symbol} 市價追中止 ({abort_reason}, 共試 {attempt} 次)")

        # 市價單已出手 (成功或中止) → 才回頭撤預掛剩餘。預算已轉移,這裡**不再動預算**、
        # 不動 st.order_no/order_status (已屬市價單);撤失敗由 13:23 cancel_all 兜底。
        if pre_order_no:
            self._rate.acquire()
            try:
                self.broker.cancel(pre_order_no, symbol, reason="first_trade_unfilled")
                self._mark_order(pre_order_no, "cancelled")
                logger.info(f"[session] {symbol} 預掛剩餘已撤 (市價單已先出手)")
            except Exception as e:
                logger.error(f"[session] {symbol} 撤預掛剩餘失敗 (13:23 cancel_all 兜底): {e}")

    def cancel_symbol_orders_async(self, symbol: str, reason: str):
        """撤單非同步版 — **行情 callback thread 專用** (撤單 REST 往返 ~60ms,
        同步呼叫會卡住同 socket 其他股票的 tick,包括正要觸發市價追的那筆)。"""
        threading.Thread(target=self.cancel_symbol_orders, args=(symbol, reason),
                         name=f"cancel-{symbol}", daemon=True).start()

    def cancel_symbol_orders(self, symbol: str, reason: str):
        """撤該檔 pending 委託 (unmark/首盤淘汰/出場 worker 用)。不賣持倉。
        閘門 = _can_manage (非 is_live) — 關 kill switch 後撤單仍要能動。
        ⚠ 同步阻塞 (REST 往返) — 行情 thread 上請用 cancel_symbol_orders_async。"""
        with self._lock:
            st = self.trades.get(symbol)
            if st is None or not st.order_no or st.order_status != "pending":
                if st is not None and reason:
                    st.stopped_reason = st.stopped_reason or reason
                return
            order_no = st.order_no
        # 真的有 pending 才檢查閘門 — 沒單就不必叫,也不會誤鳴 CRITICAL
        if not self._can_manage():
            if self.mode == "real":
                logger.critical(f"[session] ⚠ {symbol} 撤單請求但 broker 未連線 — "
                                f"委託可能仍掛在券商端,需人工處理 ({reason})")
            return
        self._rate.acquire()
        try:
            self.broker.cancel(order_no, symbol, reason=reason)
            with self._lock:
                st.order_status = "cancelled"
                st.order_no = ""
                st.stopped_reason = st.stopped_reason or reason
                self._release_budget(st)
            self._mark_order(order_no, "cancelled")
            logger.warning(f"[session] {symbol} 撤單 ({reason})")
        except Exception as e:
            logger.critical(f"[session] ⚠ {symbol} 撤單失敗 — 委託可能仍掛在券商端: {e}")

    def exit_position(self, symbol: str, reason: str):
        """出場: **跌停價限價賣**出全部已成交 (限價預掛 pending 先撤;市價買單 pending
        視為已成交不撤,直接賣)。(背景 thread)
        閘門 = _can_manage (非 is_live) — 關 kill switch / WS 不健康時出場仍要能動。
        使用者「取消追蹤」(manual_abandon) 的檔 → 一切自動化停止,不出場 (自負)。"""
        with self._lock:
            st = self.trades.get(symbol)
            if st is not None and st.stopped_reason == "manual_abandon":
                return
        if not self._can_manage():
            if self.mode == "real":
                logger.critical(f"[session] ⚠ {symbol} 出場請求但 broker 未連線 — "
                                f"部位無法賣出,需人工處理 ({reason})")
            return
        threading.Thread(target=self._exit_worker, args=(symbol, reason),
                         name=f"exit-{symbol}", daemon=True).start()

    def abandon_symbol(self, symbol: str) -> bool:
        """前端「取消追蹤」(2026-08-12): 停止該檔**一切**自動化 — 使用者自負。

        - stopped_reason=manual_abandon → 殺市價追迴圈 (狂送單的單檔煞車) + 擋後續進場
        - exited=True → 關出場自動化 (支撐消失訊號不再賣;持倉由使用者自行處理)
        - 撤掉 pending 委託 (同步,API thread 一次往返)
        回 True = session 有這檔的交易紀錄。"""
        with self._lock:
            st = self.trades.get(symbol)
            if st is None:
                return False
            st.stopped_reason = "manual_abandon"     # 覆寫 — 優先權最高
            st.exited = True
            lots = st.filled_lots
        try:
            self.cancel_symbol_orders(symbol, "manual_abandon")
        except Exception as e:
            logger.error(f"[session] {symbol} 取消追蹤撤單例外: {e}")
        logger.warning(f"[session] ⚠ {symbol} 使用者取消追蹤 — 停止該檔一切自動化 (含出場),"
                       f"持倉 {lots} 張使用者自負")
        return True

    def _sell_position(self, symbol: str, st: "SymbolTrade", lots: int, reason: str,
                       max_tries: Optional[int] = None) -> bool:
        """賣出 lots 張 — **跌停價限價賣** (使用者定案 2026-08-12,不分股種):
        限價=跌停 → 可 cross 任何買價,成交優先權等同市價單,但集合競價時段合法、
        處置股合法、永不被「不可市價」拒單。
        查無跌停價: 一般股退回市價賣兜底 (盤中合法);處置股 CRITICAL 需人工。
        max_tries=None → 預設 DEFAULT_SELL_MAX_TRIES 次;給整數 → 最多試幾次
        (緊急全平用)。成功回 True。失敗後單檔指數退避 (0.2→…→5s)。"""
        disp = st.is_disposition
        down = float(self.limit_downs.get(symbol) or 0)
        if down <= 0 and disp:
            logger.critical(f"[session] ⚠ {symbol} 處置股出場但查無跌停價 → 無法限價賣 "
                            f"(處置股不可市價),部位 {lots} 張需人工處理")
            return False
        tries = max_tries if max_tries is not None else DEFAULT_SELL_MAX_TRIES
        attempt = 0
        while attempt < tries:
            # _can_manage (非 is_live): 關 kill switch 不該讓賣單送不出去 (2026-08 修)
            if not self._can_manage():
                return False
            self._rate.acquire()      # 爆發式 45/s 窗口
            attempt += 1
            try:
                if down > 0:
                    sell_no = self.broker.place_limit_sell(symbol, down, lots, reason)
                    self._log_order(sell_no, symbol, "sell", "limit_sell", lots, down)
                    logger.warning(f"[session] ⚠ {symbol} 出場 — 跌停價 {down} 限價賣 "
                                   f"{lots} 張 ({reason})")
                else:
                    sell_no = self.broker.place_market_sell(symbol, lots, reason)
                    self._log_order(sell_no, symbol, "sell", "market_sell", lots, 0)
                    logger.warning(f"[session] ⚠ {symbol} 出場 — 查無跌停價,市價賣兜底 "
                                   f"{lots} 張 ({reason})")
                st.sell_failed = False    # 賣單送出成功 → 解除先前失敗旗標
                return True
            except Exception as e:
                if _is_fatal_reject(e):
                    # 致命拒因 — 重試無意義,立即交人工 (caller 的 sell_failed 路徑接手)
                    logger.critical(f"[session] ⚠ {symbol} 出場賣單致命拒因 → 停止重試,"
                                    f"需人工處理: {e}")
                    return False
                logger.error(f"[session] {symbol} 出場賣單失敗 (第 {attempt}/{tries} 次): {e}")
                time.sleep(min(self.order_min_interval * (2 ** (attempt - 1)), 5.0))  # 指數退避
        return False    # 重試用盡

    def _exit_worker(self, symbol: str, reason: str):
        # 出場統一流程 (2026-08-24 定案,取代 skip-cancel):
        #   1. 標「要賣」+ 能撤的就撤 (市價/限價一律試撤;已成交 → 券商拒「已成交」,無妨)
        #   2. 有掛過單就等成交回報落地收斂 (撤單前已成交/正在成交的都等它到)
        #   3. **依成交回報張數賣**,filled_lots 由 _on_fill 逐單封頂 → 絕不超過下單量、不超賣
        # 這樣「預掛部分成交 X + 市價追 shortfall 晚落地」不會只賣 X 漏掉 shortfall (2026-08-23 HIGH)。
        import time as _t
        with self._lock:
            st = self.trades.get(symbol)
            if st is None or st.exited:
                return
            st.exited = True            # 先標「要賣」,防重複觸發
            st.stopped_reason = st.stopped_reason or reason
            had_pending = bool(st.order_no) and st.order_status == "pending"
        if had_pending:
            self.cancel_symbol_orders(symbol, reason)     # 能撤就撤 (已成交→券商拒,無妨)
            # 撤單前已成交/正在成交的 → **固定等滿窗口讓在途成交回報全部落地**再賣。
            # 不提早 break: overbuy 時 (預掛+市價都成交) filled 分兩批落地,任何提早收斂
            # 判斷都可能漏掉第二批 → 又變成 exited 鎖住漏賣。回報比 tick 慢僅百 ms 級,
            # 窗口 (_EXIT_FILL_WAIT_SEC) 綽綽有餘。只有「出場當下還有 pending 單」才等
            # (一般出場時進場單早已 done → had_pending False → 不等,快)。
            _t.sleep(_EXIT_FILL_WAIT_SEC)
        with self._lock:
            lots = st.filled_lots
        if lots > 0:
            if not self._sell_position(symbol, st, lots, reason):
                with self._lock:
                    if st.stopped_reason != "manual_abandon":   # 取消追蹤的檔不回退
                        st.exited = False   # 未賣成 → 下一個委賣 tick 可再觸發 (行情節奏)
                    st.sell_failed = True   # 前端顯示「需人工」
                logger.critical(f"[session] ⚠⚠ {symbol} 出場賣單連續失敗,部位 {lots} 張仍在 — "
                                f"需人工處理 ({reason})")
        else:
            # 等滿窗口仍無部位 → exited 回退 — 否則回報更晚落地時,部位會永遠
            # 沒有出場保護 (has_exposure 被 exited=True 擋死;2026-08-05 3587 教訓)。
            # 取消追蹤 (manual_abandon) 的檔不回退 — 自動化已由使用者關閉。
            with self._lock:
                if st.stopped_reason != "manual_abandon":
                    st.exited = False

    def cancel_all_pending(self, reason: str):
        """撤所有 pending,不賣持倉 (留倉)。13:23 (CANCEL_PENDING_TIME) 主跑,13:24 收盤保險再跑。
        閘門 = _can_manage (非 is_live) — 關 kill switch 也必須能撤 13:23 的單。"""
        with self._lock:
            syms = [s for s, st in self.trades.items()
                    if st.order_no and st.order_status == "pending"]
        if not syms:
            return
        if not self._can_manage():
            if self.mode == "real":
                logger.critical(f"[session] ⚠ 13:23/收盤撤單但 broker 未連線 — "
                                f"{len(syms)} 檔未成交委託可能仍掛在券商端,需人工處理")
            return
        for sym in syms:
            self.cancel_symbol_orders(sym, reason)
        logger.warning(f"[session] 收盤撤單完成 ({len(syms)} 檔) — 持倉保留")

    def close_all(self):
        """緊急全平: 撤全部 pending + 市價賣出全部持倉。"""
        if not self._broker_ready():
            raise RuntimeError("券商未連線")
        with self._lock:
            # 略過已在出場中的檔 (exited=True) — 否則與自動出場並行會把同一批
            # filled_lots 賣第二次超賣 (2026-08-24 審查 LOW)
            snapshot = [(s, st.filled_lots) for s, st in self.trades.items()
                        if not st.exited]
        for sym, _ in snapshot:
            self.cancel_symbol_orders(sym, "close_all")
        sold = 0
        for sym, lots in snapshot:
            if lots > 0:
                st = self.trades[sym]
                with self._lock:
                    if st.exited:      # 兩次 snapshot 之間被自動出場搶先 → 跳過
                        continue
                    st.exited = True   # 先佔位,防自動出場並行重複賣
                # 緊急全平從 API thread 呼叫 → 限 3 次,避免卡死請求
                if self._sell_position(sym, st, lots, "close_all", max_tries=3):
                    sold += 1
                else:
                    with self._lock:   # 賣不成 → 回退,交回自動出場/人工
                        st.exited = False
        logger.warning(f"[session] 🚨 緊急全平: 賣出 {sold} 檔 (處置股用委買一價限價)")
        return sold

    # ─── 隔日賣標的 (昨天買到、未出場的持倉,隔天賣掉) ──────────

    def get_overnight_candidates(self) -> list:
        """收盤 (13:24) 寫檔用的隔日賣清單 — 聯集,逐日往前帶到真的賣掉為止:

        1) 今日新成交 (session.trades filled>0 且未出場)
        2) 昨天帶過來、今天還沒賣完的 (session.overnight remaining = lots-sold_lots > 0)
           + 手動加入還沒對到庫存的 (lots=0 但 reconciled=False,待隔天對帳)

        張數僅供隔天連線前顯示;refresh_overnight_inventory 會以券商庫存校正。
        """
        with self._lock:
            out = {}
            for s, st in self.trades.items():
                if st.filled_lots > 0 and not st.exited:
                    out[s] = {"symbol": s, "lots": st.filled_lots,
                              "avg_cost": round(st.avg_price, 2)}
            for s, o in self.overnight.items():
                if s in out:
                    continue  # 今日成交已涵蓋,不重複
                remaining = int(o.get("lots") or 0) - int(o.get("sold_lots") or 0)
                # 還握著的 (remaining>0) 或 手動加入待對帳的 (未 reconciled) 都保留帶下去
                if remaining > 0 or not o.get("reconciled"):
                    out[s] = {"symbol": s, "lots": max(remaining, 0),
                              "avg_cost": round(float(o.get("avg_cost") or 0), 2)}
            return list(out.values())

    def load_overnight(self, items: list):
        """隔天開盤前 runner 從檔案載入昨日持倉 (張數待 reconcile 庫存後才確定)。"""
        with self._lock:
            self.overnight = {}
            for it in (items or []):
                sym = str(it.get("symbol") or "")
                if not sym:
                    continue
                self.overnight[sym] = {
                    "symbol": sym,
                    "lots": int(it.get("lots") or 0),      # 暫用檔案值,reconcile 後覆寫
                    "avg_cost": float(it.get("avg_cost") or 0),
                    "reconciled": False,
                    "bid1": 0.0, "ask1": 0.0,
                    "sell_placed": False,
                    "sell_order_no": "",
                    "sell_price": 0.0,
                    "sold_lots": 0,
                    "skip": False,                    # 使用者按「不要賣」→ 暫停自動賣
                    "manual": False,                  # 昨日檔案帶入 → 非手動
                    "note": "待確認 (未對帳庫存)",
                    "locked_now": False,              # 目前鎖漲停中 (顯示用)
                }
        logger.warning(f"[session] 載入隔日賣清單: {len(self.overnight)} 檔 {list(self.overnight)}")

    def refresh_overnight_inventory(self):
        """券商連線後以庫存為準對帳: 有庫存→用庫存張數;無庫存 (已賣/沒了)→移除。"""
        if not self.overnight or not self._broker_ready():
            return
        try:
            inv = {r["symbol"]: r for r in self.broker.get_inventories()}
        except Exception as e:
            logger.error(f"[session] 對帳庫存失敗: {e}")
            return
        with self._lock:
            for sym in list(self.overnight):
                o = self.overnight[sym]
                if sym in inv:
                    o["lots"] = inv[sym]["lots"]
                    o["reconciled"] = True
                    o["note"] = ""
                else:
                    # 庫存沒這檔 → 已無部位,移除
                    # (但已下賣單 or 手動加入的保留顯示: 手動加的沒庫存也不刪,由使用者自行移除)
                    if not o["sell_placed"] and not o.get("manual"):
                        logger.info(f"[session] 隔日賣 {sym} 庫存為 0 → 移除")
                        del self.overnight[sym]
                    elif o.get("manual"):
                        o["note"] = "手動加入,庫存查無 (不會下賣單)"
        logger.warning(f"[session] 隔日賣對帳完成: {len(self.overnight)} 檔實有庫存")

    def add_overnight(self, symbol: str) -> bool:
        """手動加入一檔隔日賣標的 (前端輸入)。張數以券商庫存為準。

        回 True=新加入、False=已在清單。連線中會立即對帳庫存拿實際張數;
        沒庫存 (沒真的持有) → lots=0 → 顯示但不會下賣單。
        """
        symbol = str(symbol or "").strip()
        if not symbol:
            raise ValueError("代號不可空白")
        with self._lock:
            if symbol in self.overnight:
                return False
            self.overnight[symbol] = {
                "symbol": symbol,
                "lots": 0,                     # 待對帳庫存
                "avg_cost": 0.0,
                "reconciled": False,
                "bid1": 0.0, "ask1": 0.0,
                "sell_placed": False,
                "sell_order_no": "",
                "sell_price": 0.0,
                "sold_lots": 0,
                "skip": False,
                "manual": True,                # 手動加入 → 對帳庫存 0 也不自動移除
                "note": "手動加入,待對帳庫存",
                "locked_now": False,
            }
        logger.warning(f"[session] 手動加入隔日賣: {symbol}")
        # 連線中 → 立即對帳庫存 (拿實際張數;沒庫存會被移除)
        self.refresh_overnight_inventory()
        return True

    def remove_overnight(self, symbol: str) -> bool:
        """從隔日賣清單移除一檔 (誤加可刪)。回 True=有移除。"""
        symbol = str(symbol or "").strip()
        with self._lock:
            if symbol not in self.overnight:
                return False
            del self.overnight[symbol]
        logger.warning(f"[session] 移除隔日賣: {symbol}")
        return True

    def overnight_symbols(self) -> list:
        """要保留訂閱 (收五檔+成交) 的隔日賣標的。"""
        with self._lock:
            return list(self.overnight)

    def is_overnight(self, symbol: str) -> bool:
        with self._lock:
            o = self.overnight.get(symbol)
            return o is not None and not o["sell_placed"]

    def has_overnight(self, symbol: str) -> bool:
        """在隔日賣清單裡 (不看 skip/sell_placed) — 退訂保護 + 收資料判斷用。"""
        with self._lock:
            return symbol in self.overnight

    def update_overnight_book(self, symbol: str, bid1: float, ask1: float,
                              mkt_bid_size: int = 0, limit_bid1_price: float = None):
        """trader/monitor 每 tick 更新隔日賣標的的委買/委賣一價 (算賣價用),
        並跑**純盤面無狀態**賣出規則 (2026-08-16 定案,取代 hold_mode 版):

          鎖著 = 市價買隊伍在 (mkt_bid_size>0;市價列 price=0 **絕不可當跌破**)
                 或 限價委買一 >= 今日漲停價-0.001 (買牆在)
          鎖著 → 抱著 (locked_now 顯示用);委買一跌下漲停 (含買單全空) → 跌停價限價賣。

        - 開盤沒鎖的標的第一筆 book 即觸發賣 (等效舊開盤即賣);全天鎖著 → 不賣帶明天。
        - 查無漲停價: 有市價列仍判鎖著;無市價列 → 觸發賣 (舊 fallback)。
        - limit_bid1_price=None (3-arg 舊簽名) = 純報價更新,不跑規則 (防舊呼叫誤觸)。
        - 今日活躍閘門 (「只賣非今日搶單」): 該檔今日單還活著 → 不觸發;今日出場
          (exited) 後下一筆 tick 自然接手賣昨日的量。今日 _exit_worker 的 3 秒等回報
          窗口內可能與隔日賣同刻並行掛兩張跌停賣單 — 正確 (今日賣 filled_lots、
          隔日賣 min(清單,庫存),總量 ≤ 庫存)。
        - 已知 parity 漏洞 (沿襲舊 trade 閘門,不修): 預掛非致命失敗 + SKIP_TRADER 時
          st 無 stopped_reason 也不會 exited → 該檔隔日賣全日被壓制。
        - 呼叫端 (trader/_monitor on_book) 9:00:00 起才掛上 → 試撮 book 天然到不了這裡。"""
        trigger = False
        with self._lock:
            o = self.overnight.get(symbol)
            if o is None:
                return
            if bid1 > 0:
                o["bid1"] = bid1
            if ask1 > 0:
                o["ask1"] = ask1
            if limit_bid1_price is None:
                return                    # 3-arg 舊簽名 → 純報價更新
            if bid1 <= 0 and ask1 <= 0 and mkt_bid_size <= 0:
                return                    # 空 book 雜訊 → 不判,locked_now 不動
            limit_up = float(self.overnight_limit_ups.get(symbol) or 0)
            locked = (mkt_bid_size > 0
                      or (limit_up > 0 and limit_bid1_price >= limit_up - 0.001))
            o["locked_now"] = locked
            if not locked and not o["sell_placed"]:
                st = self.trades.get(symbol)
                active_today = (st is not None and not st.stopped_reason
                                and not st.exited)
                if not active_today:
                    trigger = True
        if trigger:
            self._try_start_overnight_sell(
                symbol, "overnight_bid_below_limit_up" if limit_up > 0
                else "overnight_open_no_limit_up")

    def set_overnight_skip(self, symbol: str, skip: bool):
        """前端「不要賣 / 恢復賣出」— skip=True 暫停自動賣;若已下賣單則一併撤掉。"""
        with self._lock:
            o = self.overnight.get(symbol)
            if o is None:
                raise ValueError(f"隔日賣清單無 {symbol}")
            o["skip"] = bool(skip)
            pending_no = o["sell_order_no"] if (skip and o["sell_placed"]) else ""
        if pending_no:
            try:
                self._rate.acquire()   # 補漏: 原本這條撤單完全沒過送單風控
                self.broker.cancel(pending_no, symbol, reason="overnight_skip")
                self._mark_order(pending_no, "cancelled")
            except Exception as e:
                logger.error(f"[session] 隔日賣 {symbol} 撤賣單失敗: {e}")
            with self._lock:
                o = self.overnight.get(symbol)
                if o is not None:
                    o["sell_placed"] = False    # 撤掉後可恢復 (取消 skip 時再賣)
                    o["sell_order_no"] = ""
        logger.warning(f"[session] 隔日賣 {symbol} skip={skip}"
                       f"{' (已撤賣單)' if pending_no else ''}")

    def _try_start_overnight_sell(self, symbol: str, reason: str):
        """隔日賣觸發共用閘門+佔位 (book 支撐消失 / trade 決策兩路共用)。

        閘門重查一次 (呼叫端釋鎖後才進來,狀態可能已變);sell_placed 佔位冪等 —
        重複觸發 (每 tick 訊號持續發) 不會重複下單。"""
        if not self.is_live():
            return
        with self._lock:
            o = self.overnight.get(symbol)
            if (o is None or o["sell_placed"] or o["lots"] <= 0
                    or not o["reconciled"] or o["skip"]):
                return
            o["sell_placed"] = True      # 先佔位防重複觸發
        logger.warning(f"[session] 隔日賣 {symbol} 觸發賣出 ({reason})")
        threading.Thread(target=self._overnight_sell_worker, args=(symbol,),
                         name=f"overnight-{symbol}", daemon=True).start()

    def _held_lots(self, symbol: str) -> int:
        """查券商庫存中該檔現股張數 (賣出上限用)。連線中查詢失敗回 -1;不在庫存回 0。"""
        if not self._broker_ready():
            return -1
        try:
            for r in self.broker.get_inventories():
                if r["symbol"] == symbol:
                    return int(r["lots"])
            return 0
        except Exception as e:
            logger.error(f"[session] 查 {symbol} 庫存張數失敗: {e}")
            return -1

    def _overnight_sell_worker(self, symbol: str):
        import ticks
        with self._lock:
            o = self.overnight.get(symbol)
            if o is None:
                return
            bid1, ask1, want_lots = o["bid1"], o["ask1"], o["lots"]
        # 安全上限: 賣出張數以「當下券商實際庫存」為準,絕不超賣。
        # (防清單張數被灌大 → 超賣被拒 → 無限重試狂送單;2026-08-03 實測 bug)
        held = self._held_lots(symbol)
        lots = want_lots if held < 0 else min(want_lots, held)
        if lots <= 0:
            logger.critical(f"[session] 隔日賣 {symbol} 可賣張數 0 (清單 {want_lots}/庫存 {held}) → 不賣")
            with self._lock:
                if symbol in self.overnight:
                    self.overnight[symbol]["sell_placed"] = False
            return
        # 賣價 = 跌停價限價 (2026-08-12 定案,不再管委買/委賣價差);
        # 查無跌停價 → 退回舊委買一價公式兜底
        price = float(self.limit_downs.get(symbol) or 0)
        if price <= 0:
            price = ticks.overnight_sell_price(bid1, ask1)
            if price > 0:
                logger.warning(f"[session] 隔日賣 {symbol} 查無跌停價 → 退回委買一價公式 {price}")
        if price <= 0:
            logger.critical(f"[session] ⚠ 隔日賣 {symbol} 查無跌停價也無委買一價 → 無法賣,"
                            f"{lots} 張需人工")
            with self._lock:
                if symbol in self.overnight:
                    self.overnight[symbol]["sell_placed"] = False   # 允許下一筆成交再試
            return
        # 賣單: **有限次**重試 (非無限);kill switch / 使用者暫停(skip) / 標的移除 都會即刻停。
        # (2026-08-03 修: 原本 while True 只看 kill switch → 超賣被拒時狂送單、按暫停也停不下來)
        MAX_ATTEMPTS = 5
        attempt = 0
        while attempt < MAX_ATTEMPTS:
            with self._lock:
                o = self.overnight.get(symbol)
                paused = (o is None) or o["skip"]
            if not self.is_live() or paused:
                with self._lock:
                    if symbol in self.overnight:
                        self.overnight[symbol]["sell_placed"] = False
                logger.warning(f"[session] 隔日賣 {symbol} 中止賣出 (kill switch / 暫停 / 已移除)")
                return
            self._rate.acquire()
            attempt += 1
            try:
                no = self.broker.place_limit_sell(symbol, price, lots, "overnight_sell")
                self._log_order(no, symbol, "sell", "overnight_sell", lots, price)
                with self._lock:
                    o = self.overnight.get(symbol)
                    if o is not None:
                        o["sell_order_no"] = no
                        o["sell_price"] = price
                logger.warning(f"[session] 🌙 隔日賣 {symbol} — 跌停價 {price} 限價賣 {lots} 張")
                return
            except Exception as e:
                logger.error(f"[session] 隔日賣 {symbol} 委託失敗 (第 {attempt}/{MAX_ATTEMPTS} 次): {e}")
                time.sleep(self.order_min_interval)
        # 重試用盡 → 停手 (sell_placed 保持 True,不再被下一筆成交觸發),等人工
        logger.critical(f"[session] 隔日賣 {symbol} 連 {MAX_ATTEMPTS} 次委託失敗 → 停手,需人工檢查")

    def overnight_status(self) -> list:
        """給前端「隔日賣標的」分頁 — 每檔含賣出狀態 (五檔由 API 端補)。"""
        with self._lock:
            out = []
            for sym, o in self.overnight.items():
                row = self.order_log.get(o["sell_order_no"]) if o["sell_order_no"] else None
                out.append({
                    "symbol": sym,
                    "lots": o["lots"],
                    "avg_cost": o["avg_cost"],
                    "reconciled": o["reconciled"],
                    "note": o["note"],
                    "sell_placed": o["sell_placed"],
                    "sell_price": o["sell_price"],
                    "sold_lots": row["filled_lots"] if row else 0,
                    "sell_status": row["status"] if row else "",
                    "skip": o["skip"],
                    "locked_now": o.get("locked_now", False),
                })
            return sorted(out, key=lambda x: x["symbol"])

    # ─── 斷線補收 (2026-08-06,3587 事故) ───────────────────

    def _on_broker_reconnected(self):
        """交易 WS 自動重連成功 (broker relogin thread 上執行) → 補收 + 庫存對帳。"""
        logger.critical("[session] ⚠ 交易 WS 已自動重連 — 開始補收斷線期間遺失的回報")
        try:
            self.reconcile_orders()
        except Exception as e:
            logger.exception(f"[session] 補收對帳例外: {e}")
        try:
            self.refresh_overnight_inventory()
        except Exception as e:
            logger.error(f"[session] 重連後庫存對帳例外: {e}")

    def reconcile_orders(self):
        """斷線補收 — 以券商權威成交量校正策略單。

        放寬 2026-07-29「絕不從查詢寫回 filled_lots」的定案 (使用者定案 2026-08-06):
        **僅限此情境**、僅策略單 (order_log 內)、以券商回傳**覆寫非累加**。
        理由: 斷線期間遺失的成交回報永遠不會補送,不從查詢補就永遠隱形
        (2026-08-05 3587: 市價買成交但回報遺失 → 部位對系統隱形一整天)。
        與晚到回報不會雙算 — _on_fill 有單筆委託封頂 (見該處註解)。"""
        if not self._broker_ready():
            return
        self._query_gate()
        try:
            auth_map = self.broker.get_filled_map()
        except Exception as e:
            logger.error(f"[session] 補收查詢失敗: {e}")
            return
        recovered = 0
        with self._lock:
            for order_no, row in self.order_log.items():
                auth = auth_map.get(order_no)
                if auth is None:
                    continue
                delta = auth - row["filled_lots"]
                if delta <= 0:
                    continue
                row["filled_lots"] = auth                     # 覆寫非累加
                if row["filled_lots"] >= row["lots"]:
                    row["status"] = "filled"
                st = self.trades.get(row["symbol"])
                if st is not None:
                    if row["action"] == "buy":
                        prev_cost = st.avg_price * st.filled_lots
                        st.filled_lots += delta
                        # 回報遺失 → 無成交價可用,以漲停價近似 (保守偏高)
                        st.avg_price = (prev_cost + st.limit_up * delta) / st.filled_lots
                        st.budget_reserved = max(
                            0.0, st.budget_reserved - delta * st.limit_up * 1000)
                        if st.order_no == order_no and st.filled_lots >= st.target_lots:
                            st.order_status = "done"
                            st.order_no = ""
                    else:
                        st.filled_lots = max(0, st.filled_lots - delta)
                recovered += delta
                logger.critical(f"[session] ⚠ 補收 {row['symbol']} {row['action']} "
                                f"{delta} 張 (order {order_no},斷線期間回報遺失)")
        if recovered:
            logger.critical(f"[session] 補收對帳完成 — 共補 {recovered} 張")
        else:
            logger.warning("[session] 補收對帳完成 — 無差異")

    # ─── broker 回報 ───────────────────────────────────────

    def _append_fill_csv(self, fill: dict, is_strategy: bool):
        """每筆成交回報落檔 output/YYYY-MM-DD_fills.csv (重啟不丟;每日戰績台帳)。

        含非策略單 (strategy=0) → 完整成交紀錄。IO 在 _on_fill 的鎖外呼叫。
        """
        if not self._output_dir:
            return
        try:
            import csv as _csv
            from datetime import datetime as _dt
            self._output_dir.mkdir(exist_ok=True)
            f = self._output_dir / f"{_dt.now().strftime('%Y-%m-%d')}_fills.csv"
            with self._fills_lock:
                new = not f.exists()
                with f.open("a", newline="", encoding="utf-8") as fh:
                    w = _csv.writer(fh)
                    if new:
                        w.writerow(["recv_time", "symbol", "action", "price", "lots",
                                    "quantity", "order_no", "filled_no",
                                    "broker_filled_time", "strategy"])
                    w.writerow([
                        _dt.now().isoformat(timespec="seconds"),
                        fill.get("symbol", ""), fill.get("action", ""),
                        fill.get("price", 0), fill.get("lots", 0), fill.get("quantity", 0),
                        fill.get("order_no", ""), fill.get("filled_no", ""),
                        fill.get("filled_time", ""), 1 if is_strategy else 0,
                    ])
        except Exception as e:
            logger.error(f"[session] 寫成交台帳失敗: {e}")

    def _on_fill(self, fill: dict):
        """成交回報 → 更新 filled_lots/avg_price。去重 (day-trade 模式) + 落檔台帳。"""
        key = f"{fill['order_no']}:{fill['filled_no']}:{fill['filled_time']}:{fill['lots']}"
        with self._lock:
            if key in self._processed_fills:
                return
            self._processed_fills.add(key)
            # 只認「策略下過的單」(order_no ∈ order_log) — 同帳號手動單的成交
            # 不能混進策略部位 (否則差額算錯 + 出場連手動部位一起賣)
            is_strategy = fill["order_no"] in self.order_log
        # 每筆成交都落檔 (鎖外 IO;含非策略單 → 完整成交台帳,重啟不丟)
        self._append_fill_csv(fill, is_strategy)
        if not is_strategy:
            logger.info(f"[session] 非策略單成交,忽略 (僅落檔): {fill['symbol']} "
                        f"order={fill['order_no']} {fill['lots']} 張")
            return
        with self._lock:
            # 委託總表同步 (前端顯示)
            row = self.order_log.get(fill["order_no"])
            # **單筆委託封頂** (2026-08-06): 一張委託的成交總量不可能超過委託量 —
            # 以 row 剩餘量封頂記帳,讓「斷線補收對帳」與晚到/重複回報天然冪等,
            # 不會雙算 (也根絕 2026-07-29 那類重複計算)。
            lots = fill["lots"]
            if row is not None:
                lots = max(0, min(lots, row["lots"] - row["filled_lots"]))
                row["filled_lots"] += lots
                if row["filled_lots"] >= row["lots"]:
                    row["status"] = "filled"
            if lots <= 0:
                return      # 該委託已記滿 (補收已入帳的晚到回報) → 不再動部位
            # 隔日賣單成交 → 記 sold_lots (13:24 get_overnight_candidates 算 remaining 用;
            # 2026-08-14 補 — 原本恆 0,賣光的檔會以原張數帶到明天,靠隔早對帳才清)
            o = self.overnight.get(fill["symbol"])
            if (o is not None and fill["action"] == "sell"
                    and fill["order_no"] == o["sell_order_no"]):
                o["sold_lots"] = min(o["lots"], o["sold_lots"] + lots)
            st = self.trades.get(fill["symbol"])
            if st is None:
                if o is None:
                    logger.warning(f"[session] 未知標的成交回報: {fill}")
                return
            if fill["action"] == "buy":
                prev_cost = st.avg_price * st.filled_lots
                st.filled_lots += lots
                if st.filled_lots > 0:
                    st.avg_price = (prev_cost + fill["price"] * lots) / st.filled_lots
                # 保留轉消耗 (預算不變式;晚到的 fill 若保留已被釋放,floor 0 保底)
                st.budget_reserved = max(
                    0.0, st.budget_reserved - lots * st.limit_up * 1000)
                # 達 target 才清 order_no/標 done — 但**必須是 st 現在追的那張單**成交才清
                # (守衛同 reconcile_orders): 進場 race 可能同時有預掛 P + 市價追 M 兩張 live,
                # st.order_no 已被 _first_trade_worker 蓋成 M;若 P 的成交補到 target 就清掉,
                # 會把還 live 的 M 追蹤清空 → 出場 had_pending 誤判 False → M 晚成交漏賣被
                # exited 鎖死 (2026-08-24 審查 HIGH)。P 成交時 order_no 是 M 不相符 → 不清,
                # 續為 pending → 出場照撤 M + 等窗口 + 賣全量;13:23 cancel_all 也看得到 M。
                if st.filled_lots >= st.target_lots and st.order_no == fill["order_no"]:
                    st.order_status = "done"
                    st.order_no = ""
            else:   # sell (出場)
                st.filled_lots = max(0, st.filled_lots - lots)

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
                # 拒單釋放保留預算 — 不釋放的話每次拒單都永久吃掉當日額度。
                # st.order_no 只會是買單 (賣單不寫 order_no),order_kind 再保險一層。
                if st.order_kind in ("pre_limit", "market_buy"):
                    self._release_budget(st)
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
        self._rate.acquire()
        self.broker.cancel(order_no, symbol, reason="manual_cancel")
        self._mark_order(order_no, "cancelled")
        with self._lock:
            st = self.trades.get(symbol)
            if st is not None and st.order_no == order_no:
                st.order_status = "cancelled"
                st.order_no = ""
                if is_buy:
                    st.stopped_reason = st.stopped_reason or "manual_cancel"
                    self._release_budget(st)
        logger.warning(f"[session] 手動刪單 {order_no} ({symbol})")

    # ─── 查詢 ──────────────────────────────────────────────

    def has_exposure(self, symbol: str) -> bool:
        """該檔有未成交委託或持倉 (且未出場) — trader 判斷要不要觸發出場用。"""
        with self._lock:
            st = self.trades.get(symbol)
            if st is None or st.exited:
                return False
            return st.filled_lots > 0 or st.order_status == "pending"

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
