"""TradingSession — 模式(模擬/真實)/連線/預算/per-symbol 交易 state。

掛在 Runner singleton 屬性上,**活過 runner 每日重啟**(不進 _run_all_phases 重建)。
state 模式參考 day-trade-system worker.py: per-symbol dict 純記憶體、fill 去重、
kill switch (armed) + pre-flight、預算進場前檢查。

安全預設: mode=sim、armed=False、重啟後不自動重連不自動 arm。
"""
from __future__ import annotations

import functools
import inspect
import logging
import os
import threading
import time
from collections import deque
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Callable, Dict, Optional

logger = logging.getLogger(__name__)

# 撤單查詢/撤單失敗的例外類 (broker.py 定義;2026-09-09 node3 事故 A1)。
# 測試的 FakeBroker 不 import broker,session 端一律以 **類名** 判斷 (_exc_kind),
# 這裡 import 只是讓 `from trading_session import OrderNotFound` 也能用。
try:
    from broker import OrderLookupError, OrderNotFound   # noqa: F401
except Exception:                                        # pragma: no cover — broker 尚未就緒
    class OrderLookupError(RuntimeError):
        """查詢失敗 (is_success 非 True / SDK 例外) — 與 broker.OrderLookupError 同名備援。"""

    class OrderNotFound(RuntimeError):
        """查詢成功但清單無此書號 — 與 broker.OrderNotFound 同名備援。"""


def _exc_kind(e: BaseException) -> str:
    """撤單例外分類 (以 MRO 類名判斷,tests 的 fake 例外不必 import broker):
    'not_found' (查詢成功但無此書號) / 'lookup' (查詢失敗:流量控管/SDK 例外) / 'other'。"""
    names = {c.__name__ for c in type(e).__mro__}
    if "OrderNotFound" in names:
        return "not_found"
    if "OrderLookupError" in names:
        return "lookup"
    return "other"


# 富邦撤單失敗訊息的終端分類 (2026-09-09 事故 A2):
#   「成交單已不允許取消」/「部分成交單已不允許取消」→ 已成交 (不標 cancelled,用快照補成交)
#   「取消單已不允許取消」→ 早已撤掉 (冪等標 cancelled)
_CANCEL_FILLED_KEYWORDS = ("成交單已不允許取消", "部分成交單已不允許取消")
_CANCEL_ALREADY_KEYWORDS = ("取消單已不允許取消",)


_UNKNOWN_CANCEL_REJECT_SEEN: set = set()   # 已告警過的未知拒撤文案 (每種文案只 CRITICAL 一次)


def classify_cancel_error(msg) -> str:
    """撤單失敗訊息 → 'filled_before_cancel' | 'already_cancelled' | 'retry'。
    含「已不允許取消」但非兩組已知關鍵字 (富邦日後改文案 / 其他終結狀態) → 仍回 retry,但 CRITICAL 一次
    提示新文案 (審查: 未知終結文案被當 retry 會 attempts 累加成假 CRITICAL)。"""
    s = str(msg or "")
    if any(k in s for k in _CANCEL_ALREADY_KEYWORDS):
        return "already_cancelled"
    if any(k in s for k in _CANCEL_FILLED_KEYWORDS):
        return "filled_before_cancel"
    if "已不允許取消" in s:
        key = s[-80:]
        if key not in _UNKNOWN_CANCEL_REJECT_SEEN:
            _UNKNOWN_CANCEL_REJECT_SEEN.add(key)
            logger.critical(f"[session] ⚠ 撤單被拒文案未知 (非「成交單/部分成交單/取消單已不允許取消」) → "
                            f"視為可重試;請確認是否為新終結文案並補進 classify_cancel_error: {s[:120]}")
    return "retry"


# 撤單佇列 worker 參數 (2026-09-09 node3「管線多送市價單未撤」事故 A2):
#   退避序列 (秒) — 不在快照/撤單失敗後第 n 次重試前等多久;超過序列長度後每 5 s 一次
_CANCEL_BACKOFF = (0.3, 0.5, 1.0, 2.0, 3.0, 5.0)
_CANCEL_WARN_ATTEMPTS = 3          # attempts 到此 → WARNING
_CANCEL_CRIT_ATTEMPTS = 6          # attempts 到此 → CRITICAL,之後每 30 s 重複
_CANCEL_CRIT_REPEAT_SEC = 30.0
_CANCEL_FANOUT_THREADS = 8         # 快照內的撤單扇出併發上限
_CANCEL_SDK_TIMEOUT_SEC = 5.0      # 快照/撤單 SDK 呼叫逾時 → 視為未確認、不計 attempts
_CANCEL_GIVEUP_AFTER_END_MIN = 10  # trading_end + 10 分鐘仍未確認 → 放棄 (留 pending + CRITICAL)
_CANCEL_DRAIN_MAX_SEC = 40.0       # cancel_all_pending 同步 drain 上限
_INTRADAY_SWEEP_INTERVAL_SEC = 60.0
_INTRADAY_SWEEP_START = dtime(9, 5, 0)
_INTRADAY_SWEEP_END = dtime(13, 20, 0)
_HITLIMIT_USER_DEF = "hitlimit"
_BROKER_LIVE_STATUSES = ("0", "4", "8", "10")   # 富邦委託 status: 尚未終結 (可撤)
# 富邦委託 status 終結值 (llms-full.txt「委託單狀態」): 30 未成交刪單成功、40 部分成交剩餘取消、
# 50 完全成交、90 失敗 — 撤單 worker 對這些**不送撤單**,直接依券商狀態結案 (審查 2026-09-10)
_BROKER_TERMINAL_CANCELLED = ("30", "40")
_CANCEL_SKIP_BACKOFF_SEC = 1.0     # broker 不健康 / 不可管理時到期項延後多久 (不計 attempts;免忙迴圈)
_CLOSE_SWEEP_RETRY_SEC = 2.0       # 收盤券商權威掃單失敗 → drain 期間每 N s 重試到成功或逾時
_FBC_ERR_PREFIX = "FILLED_BEFORE_CANCEL: "   # row.cancel_err 前綴 = 「撤單前已成交」已結案 (冪等標記)
_SWEEP_TERMINAL_GRACE_SEC = 2.0    # 券商權威掃單: 本地剛終結 (<N s) 的列本輪略過 — 查詢 replica 落後於 WS 回報
# ── 委託洪水韌性 (2026-09-14 node1: 同登入第三方程式 28k 拒單、清單 33,940 筆、每次全清單查詢 ≈540 MB) ──
_CANCEL_ALL_WAIT_SEC = 120.0       # cancel_all_pending 序列化: 後到者等前一次跑完的上限 (逾時仍照跑 + CRITICAL)
_INTRADAY_SWEEP_BIG_LIST_ROWS = 10_000         # env INTRADAY_SWEEP_BIG_LIST_ROWS (每次現讀)
_INTRADAY_SWEEP_BIG_LIST_INTERVAL_SEC = 300.0  # env INTRADAY_SWEEP_BIG_LIST_INTERVAL_SEC (每次現讀)
_FOREIGN_REJECT_SUMMARY_SEC = 60.0     # 非本策略拒單回報: 彙總 log 週期
_FOREIGN_REJECT_FIRST_LOG_MAX = 50     # 每日逐條 log「首見文案」的上限 (文案含流水號時防洪)
_FOREIGN_REJECT_KEYS_MAX = 200         # 單一彙總窗內分文案計數的上限 (超過併入「其他文案」)


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.environ.get(name, "")).strip() or default)
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(str(os.environ.get(name, "")).strip() or default)
    except (TypeError, ValueError):
        return default


def _with_fresh_after(fn, fresh_after):
    """broker 查詢函式有 fresh_after 參數才帶上 (RealOrderClient);測試/replay 替身沒有 → 原樣呼叫。"""
    if fresh_after is None:
        return fn
    try:
        params = inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return fn
    if "fresh_after" not in params:
        return fn
    return functools.partial(fn, fresh_after=fresh_after)


def _parse_hhmmss(s: str, default: dtime) -> dtime:
    try:
        parts = [int(x) for x in str(s).strip().split(":")]
        while len(parts) < 3:
            parts.append(0)
        return dtime(*parts[:3])
    except Exception:
        return default


def _intraday_sweep_dry_run() -> bool:
    """盤中低頻對帳是否只 log 不撤 — env INTRADAY_SWEEP_DRY_RUN (預設 true;每次現讀,免重啟)。"""
    v = os.environ.get("INTRADAY_SWEEP_DRY_RUN", "true").strip().lower()
    return v not in ("0", "false", "no", "off")


# ─── 帳號防呆 (2026-09-14 node3/node4 在 UI 互填對方帳號 → 在別台節點的帳號上交易) ───

def _norm_login_id(v) -> str:
    """登入 ID 正規化 (去空白、不分大小寫);非字串 (測試替身/未知) → ""。"""
    return v.strip().upper() if isinstance(v, str) else ""


def _mask_login_id(v) -> str:
    """log/UI 用遮罩: 前 3 碼 + ***。"""
    s = _norm_login_id(v)
    return f"{s[:3]}***" if s else "(空)"


def _node_login_id() -> str:
    """本節點的券商登入 ID = .env FUBON_ACCOUNT_ID (每次呼叫現讀 os.environ);"" = 未設 → 不檢查。"""
    return _norm_login_id(os.environ.get("FUBON_ACCOUNT_ID", ""))


def _node_login_masked() -> str:
    """UI「本節點帳號」顯示用遮罩 (2026-09-15): .env FUBON_ACCOUNT_ID 前 3 碼 + … + 後 3 碼 (例 Z90…999)。

    讓操作員在連線表單旁一眼比對,不要拿別台節點的帳號連線。每次呼叫現讀 os.environ
    (.env 於服務啟動時載入;改 .env 需 systemctl restart 才生效);
    正規化同 _norm_login_id (去空白、轉大寫,與連線防呆比對一致)。未設/空白 → "";
    長度 < 7 → 只給前 3 碼 + … (前後 3 碼會重疊,等於露出全部)。完整 ID 絕不出現在回傳值。"""
    s = _node_login_id()
    if not s:
        return ""
    if len(s) < 7:
        return f"{s[:3]}…"
    return f"{s[:3]}…{s[-3:]}"


def _node_role() -> str:
    """本機角色 = env ROLE (同 config.load_config 正規化: strip + lower,預設 standalone;每次現讀 os.environ)。"""
    return os.environ.get("ROLE", "standalone").strip().lower()


def _call_with_timeout(fn, timeout: float, name: str):
    """在 daemon thread 跑 fn(),最多等 timeout 秒;逾時 raise TimeoutError (thread 留在背景自然結束)。
    SDK 呼叫卡死不能拖垮撤單 worker (A1b: 逾時 = 未確認、不計 attempts)。"""
    box: dict = {}

    def _run():
        try:
            box["v"] = fn()
        except BaseException as e:      # noqa: BLE001 — 原例外原樣轉回 caller
            box["e"] = e

    t = threading.Thread(target=_run, name=name, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        raise TimeoutError(f"{name} 逾時 {timeout}s")
    if "e" in box:
        raise box["e"]
    return box.get("v")

# 富邦 API 明定速率上限: 下單 50/s、批次下單 10/s、帳務查詢 5/s、連線數 10。
# 送單 (place/cancel) 走 SendRateLimiter 爆發式滑動窗口 (預設 45/s,留 margin);
# 帳務/委託查詢的 5/s 閘門**在 broker 層** (broker._query_order_results,所有 get_order_results
# 唯一入口;2026-09-09 A1b) — session 的 _query_gate 只剩 reconcile_orders 用的粗閘門。
_HARD_MIN_INTERVAL = 0.02      # order_min_interval (追單失敗退避) 的下限

# 出場賣重試上限 (進場市價追**無上限** — 使用者定案 2026-07-27,試到成功為止;
# 出場失敗後下一個委賣 tick 會再觸發,行情節奏自然重試,故有限次即可。
# 隔日賣 5 次上限見 _overnight_sell_worker, fd451ce):
DEFAULT_SELL_MAX_TRIES = 8     # 出場賣: 指數退避 0.2→…→5s;用盡記 CRITICAL + sell_failed 旗標

# 出場 worker 讀到 0 張時「等成交回報」的窗口 — 支撐消失觸發出場的同一瞬間可能
# 剛好成交 (回報比行情 tick 慢 ~百 ms 級);等到就照樣賣掉,沒等到 → exited 回退,
# 晚到的部位由下一個出場訊號接手 (2026-08-12 使用者確認的競態處理)
_EXIT_FILL_WAIT_SEC = 3.0

# 市價盲送管線化 (2026-08-28): 每檔一條 cadence thread,不等券商回覆就送下一筆 (非同步),
# 由 45/s SendRateLimiter 當唯一節拍器 → 少檔時也能衝到接近 45/s (同步版被 REST 往返卡在 ~5/s)。
# CHASE_MAX_INFLIGHT = 同檔在飛送單的併發安全帽 (真正節流是 45/s rate;此值只防 thread 爆量)。
CHASE_MAX_INFLIGHT = 24

# 停止拒因關鍵字 — 拒單訊息含這些字 = 重試無意義,第一筆被拒就停止該檔:
#   全額/預收/圈存: 全額交割/處置股,API 下單今日必不成功 (2026-08-13 6225 事故,
#                   T30 名單漏抓時的第二層保險)
#   價格穩定: 「證券委託觸及價格穩定措施上、下限價格」— 瞬間價格穩定措施冷卻期內市價單
#             必被拒,重試只會狂送 (2026-08-21 6144 事故: 26 秒狂送 100 筆)
_FATAL_REJECT_KEYWORDS = ("全額", "預收", "圈存", "價格穩定")


def _is_fatal_reject(err) -> bool:
    msg = str(err)
    return any(k in msg for k in _FATAL_REJECT_KEYWORDS)


# 出場賣單「庫存不足」拒因 (2026-09-14 node3「證券可賣不足」/ node4「委託超過庫存,或未符合當沖資格或洽營業員」):
# 策略外賣掉部分部位 → 出場仍送策略帳上張數 → 每筆都被拒。**被拒後才**向券商查可賣張數重算
# (事後重算、不預先封頂: 剛成交時庫存查詢可能落後,第一筆出場賣單照原張數送)。
_INVENTORY_SHORT_KEYWORDS = ("可賣不足", "超過庫存", "庫存不足")


def _is_inventory_short_reject(err) -> bool:
    msg = str(err)
    return any(k in msg for k in _INVENTORY_SHORT_KEYWORDS)


# _sell_position 的第三種結果: 券商**確定已無今日股** (可委託 0 且整股餘額扣隔日保留 ≤0) 連續確認 → 停止出場此檔。
# 刻意用 truthy 字串 — 舊式 `if not _sell_position(...)` 呼叫端不會把它當失敗去回退 exited / 設 sell_failed。
# 2026-09-15 審查: 「可委託 0」本身有歧義 (禁現沖今日買進 / 當沖資格不符 / 被其他賣單佔用 → 帳上仍有股),
# 故另需整股餘額為 0;且 nothing-left 的部位**仍列入 13:24 隔日賣檔** (exit_no_sellable),隔早以券商庫存對帳清掉。
SELL_NOTHING_LEFT = "nothing_left"
_SELLABLE_ZERO_CONFIRM = 2          # 「無今日股」須連續幾次「庫存不足拒單 + 查詢確認」才認定 (防單次查詢落後)
_SELLABLE_ZERO_CONFIRM_SEC = 5.0    # 且第一次確認到最後一次須相隔 ≥N 秒 (退避總長 ~16 s 內;防成交後庫存後台短暫落後)
_SELLABLE_QUERY_TIMEOUT_SEC = 5.0   # 可賣張數查詢逾時 → 視為未知 (維持原重試)
# 出場失敗輪次冷卻 (跨 tick 風暴煞車): 第 n 輪失敗後 exit_position 在此秒數內不再起出場 thread (超過取最後值)
_EXIT_COOLDOWN_SEC = (30.0, 60.0, 120.0, 300.0)


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
        self.order_no: str = ""        # 當前有效委託 (預掛→市價盲送成功後被蓋成市價單)
        self.pre_order_no: str = ""    # 08:59:58 預掛單 P (市價盲送蓋掉 order_no 後,靠此追蹤撤 P)
        self.order_kind: str = ""      # "pre_limit" / "market_buy" / "market_sell"
        self.order_status: str = ""    # "" / pending / cancelled / rejected / done
        self.filled_lots = 0
        self.avg_price = 0.0
        self.buy_cost_actual = 0.0     # 該檔實際買進現金累計 (單調;賣出不減) — 花費表/超額用
        self.stopped_reason: str = ""  # 非空 = 此檔不再進場
        self.exited = False            # 已出場 (委賣出現)
        self.exit_in_progress = False  # _exit_worker 進行中 (等回報窗口→賣) — 此期間的晚成交由它接手;
        #   結束後 (exited 仍 True) 再來的買進成交 = 出場後晚成交 → _on_fill 另起 late-fill 賣 (A4b)
        self.last_buy_cancel_ts = 0.0  # 最近撤掉 live 買單的時刻 (time.time()) — 出場據此判斷
        #   要不要等成交回報窗口: 別處先撤了 (如硬上限 breach) → 撤單的在途成交仍可能晚到 (審查 #3)
        self.budget_reserved = 0.0     # 該檔目前保留中的預算 (下單保留,成交轉消耗,撤/拒釋放)
        self.sell_failed = False       # 出場賣單重試用盡仍沒送出 → 需人工 (前端顯示)
        self.first_trade_fired = False  # 首筆成交已處理 — 早期觸發/trader/重複 tick 冪等用
        self.is_disposition = False    # 處置股: 不撤預掛/不下市價買、出場改委買一價限價賣
        self.last_bid1_price = 0.0     # 最近委買一價 (處置股出場限價賣用)
        self.day_tradable = True       # 可現股當沖 (canDayTrade);False=禁現沖 → 部位減半
        self.limit_up_bid_vol = 0      # 08:59:58 漲停價委託量快照 (張) — 20% 上限母數
        self.chase_lots = 0            # = sized target (與 target_lots 同值;風控後上限)。
        #   實際盲送每筆送 shortfall=target−已成交,非直接用此值 (2026-08-25 修正後保留作上限紀錄)
        self.chase_done = False        # 市價盲送已有一筆委託成功 (管線化: cadence 據此停送;
        #   之後在飛的多送筆若也成功 = 管線多送,由 _chase_send_one 撤掉/超買由出場賣)
        self.chase_next_send_at = 0.0  # 該檔下一次可送市價的 monotonic 時刻 (公平均分節拍用;
        #   0=第一筆立刻送,之後按 N×(1/45)s interval 均勻鋪,N=目前還在搶的檔數)
        # 出場失敗冷卻 (2026-09-14 node3/node4 出場賣單風暴): 每輪出場失敗 (賣單重試用盡 / 出場賣單被券商
        # 非同步退) +1 並設冷卻,exit_position 冷卻中不起 thread。賣單送出成功只清冷卻 (輪次保留 — 送出後
        # 被非同步退才能 30→60→120→300 升級);策略出場賣單**成交**或券商已無今日股 → 輪次歸零。
        self.exit_fail_cycles = 0
        self.exit_cooldown_until = 0.0     # time.monotonic() 時刻;0 = 無冷卻
        self.exit_cooldown_logged = False  # 本輪冷卻已記過一次「略過」log
        self.exit_no_sellable = False      # SELL_NOTHING_LEFT 停止出場 — exited 維持 True 但**仍列入隔日賣檔**
        #   (券商若真 0 張,隔早 refresh_overnight_inventory 以「庫存沒這檔 → 移除」清掉;假 0 則隔天照常自動賣)

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

    def __init__(self, auto_cancel_worker: bool = True):
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
        self.budget_used: float = 0.0     # 下單時保留,撤單釋放未成交部分 (sizing 用;超買 race 會低估)
        # 總曝險硬上限 (2026-08-25): 實際買進成交累計現金 (每筆 fill 都加、不 floor → 含超買 race)。
        # 超過 total_budget → _budget_breached 單向煞車:停所有市價盲送 + 撤所有 pending 買單。
        self._buy_cost_actual: float = 0.0
        self._budget_breached: bool = False
        # per-symbol state
        self.trades: Dict[str, SymbolTrade] = {}
        self._processed_fills: set = set()   # 去重 key
        self._output_dir: Optional[Path] = None   # 連線後設 — 成交台帳 fills.csv 落檔用
        # 委託總表 (前端委託狀態顯示 + 右鍵刪單) — key=order_no, 插入序
        self.order_log: Dict[str, dict] = {}
        # 處置股名單 (runner 從 ticker.isDisposition 填) — 影響下單機制
        self.dispositions: Dict[str, bool] = {}
        # 可現股當沖名單 (runner 從 ticker.canDayTrade 填) — False=禁現沖 → 下單減半
        self.day_tradable: Dict[str, bool] = {}
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
        # 個股金額覆寫 (symbol → 專屬下單金額,元) — 跨日保留的使用者設定,
        # 篩選清單含此檔時依專屬金額下 (_calc_lots),其餘用全域參數。開機從檔載入。
        try:
            import symbol_budget as _sb
            self.symbol_budgets: Dict[str, float] = _sb.load()
        except Exception:
            self.symbol_budgets = {}
        # 策略參數 (runner 啟動時從 cfg configure;預設值供測試/未 configure 時用)
        self.max_stock_price: float = 500.0        # 只做漲停價 <= 此價 (0=不限)
        self.order_min_interval: float = 0.2       # 市價追單最小送單間隔
        self.cancel_pending_time = None            # datetime.time;此時後不再追單
        # 爆發式送單風控 — 進場/出場/撤單共用 45/s 滑動窗口 (一秒最前面可全部送出);
        # 當「狂送公平均分」的全域硬上限 backstop。
        self._rate = SendRateLimiter(45)
        # 目前還在跑 cadence 的狂送標的集合 (公平均分母數 N;worker 進迴圈 add、退出 discard)。
        # 用 self._lock 保護。1 檔→每 22.2ms、2 檔→每檔 44.4ms、搶到後剩餘檔即刻加速。
        self._active_chase: set = set()
        # 帳務/委託查詢閘門 (富邦 5/s) — 與送單額度分開計
        self._send_lock = threading.Lock()
        self._last_send = 0.0
        # fills.csv 寫檔鎖 — SDK 回報可能多 thread 併發,防雙表頭/行交錯
        self._fills_lock = threading.Lock()
        # 交易日 (roll_day 每日重置用)
        self._trade_date: str = ""
        # 今日已預掛過的日期 — place_pre_orders 冪等保護 (防重複 timer 雙倍下單)
        self._pre_orders_date: str = ""
        self._chase_started_date: str = ""   # 市價盲送冪等 (防 timer 重觸發開兩次 thread)
        # ── 撤單佇列 worker (2026-09-09 node3「管線多送市價單未撤」事故 A2) ──
        # 非同步撤單的唯一出口:order_no → {symbol, reason, first_ts, attempts, next_ts, last_crit_ts}
        # (dict 以書號去重)。同步呼叫端先 broker.cancel 試一次,失敗 (查無/查詢失敗/非終端錯) → 入佇列,
        # **row 保持 pending、不清 st.order_no、不釋放預算** — cancelled 只能由券商確認寫入。
        self.auto_cancel_worker: bool = bool(auto_cancel_worker)   # False = 測試/replay 同步驅動 run_once
        self._cancel_queue: Dict[str, dict] = {}
        self._cancel_run_lock = threading.Lock()      # run_once 互斥 (worker thread vs 收盤 drain)
        self._cancel_wakeup = threading.Event()       # enqueue/回報 → 喚醒 worker
        self._cancel_worker_thread: Optional[threading.Thread] = None
        self._cancel_stats = {"confirmed": 0, "filled_before_cancel": 0, "given_up": 0}
        self._last_cancel_err: Dict[str, str] = {}   # order_no → 最近一次同步撤單失敗原因
        # 收盤時點 (與 runner cfg.trading_end_time 同源 env;放棄撤單 = 此時 +10 分)
        self.trading_end_time: dtime = _parse_hhmmss(
            os.environ.get("TRADING_END_TIME", "13:24:00"), dtime(13, 24, 0))
        # 晚結案 hook (A4b): trading_end 之後每次撤單結案 (cancelled / filled_before_cancel) 呼叫 —
        # runner 設成 _write_overnight_file,讓收盤後才確認的單也反映到隔日賣清單
        self.on_late_confirm: Optional[Callable[[], None]] = None
        self._last_intraday_sweep = 0.0
        # ── 委託洪水韌性 (2026-09-14 node1) ──
        self._cancel_all_lock = threading.Lock()   # cancel_all_pending 序列化 (budget_breached / 13:23 / 13:24 不並行)
        self._intraday_skip_logged = False          # 盤中對帳因查詢在飛而略過 — 每段只 log 一次
        self._intraday_big_list = False             # 目前是否用大清單長間隔 (切換時 log 一次)
        # 非本策略委託的拒單回報彙總 (不逐筆 WARNING;首見文案立即 log、其餘定期彙總)
        self._foreign_rej_lock = threading.Lock()
        self._foreign_rej_seen: set = set()         # 今日已逐條 log 過的文案
        self._foreign_rej_counts: Dict[str, int] = {}   # 本彙總窗: 文案 → 筆數
        self._foreign_rej_total = 0                 # 本彙總窗總筆數
        self._foreign_rej_suppressed = 0            # 本彙總窗內未逐條 log 的筆數
        self._foreign_rej_window_start = 0.0        # 本彙總窗起點 (time.monotonic)
        self._foreign_rej_day_total = 0             # 今日累計 (已彙總者)

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
            self._buy_cost_actual = 0.0       # 每日重置實際買進累計
            self._budget_breached = False     # 每日重置硬上限煞車
            self.armed = False
            self.dispositions = {}
            self.day_tradable = {}   # 每日重抓 (canDayTrade)
            self.limit_downs = {}    # 每日價格不同,新日清空由 runner 重填
            self.untradable = set()  # T30 名單每日重載
            self.overnight = {}      # 隔日賣清單由 runner 讀檔重建 (roll_day 後才 load)
            self.overnight_limit_ups = {}   # 每日漲停價不同,runner 補查重填
            self._pre_orders_date = ""   # 新交易日 → 允許今日預掛
            self._chase_started_date = ""
            self._cancel_queue.clear()   # 前日未確認撤單佇列不帶到今天 (書號隔日失效)
            self._cancel_stats = {"confirmed": 0, "filled_before_cancel": 0, "given_up": 0}
            with self._foreign_rej_lock:   # 非本策略拒單回報彙總: 新的一天首見文案重新逐條 log
                self._foreign_rej_seen.clear()
                self._foreign_rej_counts = {}
                self._foreign_rej_total = self._foreign_rej_suppressed = self._foreign_rej_day_total = 0
        logger.warning(f"[session] roll_day({date_str}) — 新交易日: 清 {had} 檔前日 state,"
                       f"armed=False (要交易請重新 arm)")

    def set_dispositions(self, dispositions: Dict[str, bool]):
        """runner 抓完 ticker 後把處置股名單交進來。"""
        with self._lock:
            self.dispositions = dict(dispositions or {})
        n = sum(1 for v in self.dispositions.values() if v)
        logger.info(f"[session] 處置股名單: {n} 檔")

    def set_day_tradable(self, day_tradable: Dict[str, bool]):
        """runner 抓完 ticker 後把可現股當沖名單交進來 (canDayTrade)。
        False = 禁現沖 → 下單張數減半 (風控①,2026-08-24)。"""
        with self._lock:
            self.day_tradable = dict(day_tradable or {})
        n = sum(1 for v in self.day_tradable.values() if v is False)
        logger.info(f"[session] 禁現沖名單: {n} 檔 (canDayTrade=False → 部位減半)")

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

    def set_symbol_budgets(self, budgets: Dict[str, float]):
        """個股金額覆寫更新 (API 增刪後同步記憶體;下單依此判專屬金額)。"""
        with self._lock:
            self.symbol_budgets = {str(k): float(v) for k, v in (budgets or {}).items()}
        logger.info(f"[session] 個股金額覆寫: {len(self.symbol_budgets)} 檔")

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
        送單**不走這裡** (送單走 self._rate 爆發式窗口)。
        2026-09-09 A1b: 查詢閘門主體已下沉到 broker (所有 get_order_results 都過 broker 內部 5/s);
        這裡保留作 session 端的粗閘門,**鎖內只算槽位、鎖外 sleep** (原本在 _send_lock 內 sleep)。"""
        with self._send_lock:
            now = time.monotonic()
            slot = max(now, self._last_send + min_interval)
            self._last_send = slot
            wait = slot - now
        if wait > 0:
            time.sleep(wait)

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
                "last_time": "",           # 富邦委託回報「最後異動時間」(_on_order 收到回報時填)
                # 撤單進度 (2026-09-09 A5 可見性;status 值域不變,cancelled 只由券商確認寫入):
                #   cancel_state: '' | queued (入佇列待撤) | sent (撤單已送) | unconfirmed (撤不到/未確認)
                "cancel_state": "",
                "cancel_attempts": 0,
                "cancel_reason": "",
                "cancel_err": "",
                "terminal_ts": 0.0,        # 離開 pending 的時刻 (掃單對「剛終結」的列留 replica 同步寬限)
            }

    def _mark_order(self, order_no: str, status: str):
        with self._lock:
            row = self.order_log.get(order_no)
            if row is not None and row["status"] == "pending":
                row["status"] = status
                row["terminal_ts"] = time.time()

    # ─── 連線 ──────────────────────────────────────────────

    def connect_async(self, account_id: str, password: str, pfx_path: str,
                      pfx_password: str, is_test: bool, output_dir: Path):
        """背景 thread 連線 (照 day-trade /api/auth/connect 模式,前端輪詢 status)。

        帳號防呆 (2026-09-14 node3/node4 在 UI 互填對方帳號): .env FUBON_ACCOUNT_ID 有設、且送來的
        登入 ID 不同 (去空白、不分大小寫) → **不連線、不建 client、不呼叫 SDK login、不 raise**;
        原因寫進 connect_error (前端每 2 秒輪詢 /api/trading/status 紅框顯示)。未設 → 與舊版相同。"""
        expected = _node_login_id()
        foreign = bool(expected) and _norm_login_id(account_id) != expected
        with self._lock:
            if self.connecting:
                raise RuntimeError("連線進行中")
            if foreign:
                self.connect_error = (
                    f"登入帳號 {_mask_login_id(account_id)} 不屬於本節點 "
                    f"(本節點 .env FUBON_ACCOUNT_ID = {_mask_login_id(expected)}),已拒絕連線 — "
                    f"請確認沒有填成其他節點的帳號")
            else:
                self.connecting = True
                self.connect_error = ""
        if foreign:
            logger.warning(f"[session] ⛔ 拒絕連線: 登入帳號 {_mask_login_id(account_id)} ≠ 本節點 "
                           f"FUBON_ACCOUNT_ID {_mask_login_id(expected)} — 未呼叫券商 login")
            return
        self._output_dir = output_dir            # 成交台帳 fills.csv 落檔用

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
                # 非本策略委託的拒單回報 → broker 不逐筆 ERROR (session 彙總;2026-09-14 node1)
                client.is_foreign_order_report = self._is_foreign_order_report
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
                # 撤單 worker 隨連線啟動 (real 模式才真的起 thread;hub/sim 不起) — 讓 09:05 起的
                # 盤中券商權威對帳不依賴「當天有沒有撤單請求」(盤中重啟、order_log 空白時也能發現孤兒單)
                self._ensure_cancel_worker()
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
            foreign = self._foreign_login_error()
            if foreign:
                logger.warning(f"[session] ⛔ 拒絕開始交易: {foreign}")
                raise RuntimeError(foreign)
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
        if armed:
            # 每日 arm = 交易日入口: 連線時若還是 sim (之後才切 real),connect_async 那次沒起 worker →
            # 這裡補起 (pre-flight 已過 = is_live 成立);否則 09:05 起的盤中券商權威對帳整天不跑 (審查)
            self._ensure_cancel_worker()

    def _broker_ready(self) -> bool:
        return bool(self.broker and self.broker.connected and self.broker.healthy)

    def _foreign_login_error(self) -> str:
        """已連線 broker 的登入 ID ≠ 本節點 .env FUBON_ACCOUNT_ID → 回拒絕原因;相符/未設/無從比對 → ""。
        登入 ID 取 RealOrderClient.connect 存的 _account_id (重登入也用它);替身/非字串/空 → 維持原行為。"""
        expected = _node_login_id()
        if not expected:
            return ""
        got = _norm_login_id(getattr(self.broker, "_account_id", None))
        if not got or got == expected:
            return ""
        return (f"目前連線的券商帳號 {_mask_login_id(got)} 不屬於本節點 "
                f"(本節點 .env FUBON_ACCOUNT_ID = {_mask_login_id(expected)}),拒絕開始交易 — "
                f"請先斷線,改用本節點帳號重新連線")

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

    def _calc_lots(self, limit_up: float, symbol: str = None) -> int:
        """算該檔下單張數。caller 持鎖。總預算餘額永遠是硬上限。
        - **個股金額覆寫**: 該檔有專屬金額 (symbol_budgets) → 一律用該金額算 (依金額),
          無視全域 budget/fixed_lots 模式;仍受總預算餘額上限 (2026-08-24)。
        - budget 模式: floor(min(每檔上限, 總預算餘額) / (漲停價×1000))
        - fixed_lots 模式: min(fixed_lots, 總預算餘額能買的張數)"""
        cost_per_lot = limit_up * 1000
        if cost_per_lot <= 0:
            return 0
        remaining = self.total_budget - self.budget_used
        budget_cap = int(remaining // cost_per_lot)     # 總預算餘額能買幾張 (硬上限)
        override = self.symbol_budgets.get(symbol) if symbol else None
        if override and override > 0:
            # 專屬金額: 依金額下 (min(專屬金額, 總預算餘額) / 每張成本)。
            # **最少一張** (2026-08-24): 使用者明確指定此檔就是要買,金額不足一張也下 1 張
            # (仍受總預算硬上限 budget_cap — 總預算連 1 張都買不起才回 0)。
            lots = int(min(float(override), remaining) // cost_per_lot)
            return max(0, min(max(lots, 1), budget_cap))
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

    def _sized_lots(self, st: "SymbolTrade") -> int:
        """最終下單張數 (caller 持鎖) — 依序疊加 (2026-08-24 風控①②):
          base = _calc_lots (個股金額覆寫 or 全域 budget/fixed_lots;已含總預算硬上限)
          → 禁現沖 (day_tradable=False) 減半
          → 不超過漲停價委託量 20% (limit_up_bid_vol>0 才套)
        預掛 (target_lots) 與市價盲送 (chase_lots) 共用此值。"""
        lots = self._calc_lots(st.limit_up, st.symbol)
        if st.day_tradable is False:              # 禁現沖 → 部位減半
            lots = lots // 2
        if st.limit_up_bid_vol > 0:               # 20% 委託量上限 (預掛+盲送都套)
            lots = min(lots, int(st.limit_up_bid_vol * 0.2))
        return max(0, lots)

    # ─── 08:59:58 預掛限價單 ───────────────────────────────

    def place_pre_orders(self, symbols: list, limit_ups: dict, stop_event=None,
                         limit_up_bid_vols: dict = None):
        """08:59:58 對 marked 清單逐檔掛漲停價限價買單 (集合競價排隊)。
        limit_up_bid_vols: {sym: 漲停價委託量(張)} 快照 — 20% 上限母數 (風控②)。

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
                st.day_tradable = self.day_tradable.get(sym, True)   # 風控①: 禁現沖減半
                st.limit_up_bid_vol = int((limit_up_bid_vols or {}).get(sym, 0))  # 風控②母數
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
                lots = self._sized_lots(st)                  # 覆寫/全域 → 禁現沖//2 → 20%上限
                if lots <= 0:
                    st.stopped_reason = "budget_exhausted"
                    logger.info(f"[session] {sym} 張數算出 0 (預算/禁現沖/20%上限) → 跳過")
                    continue
                st.target_lots = lots
                st.chase_lots = lots                         # = target_lots (盲送上限;實送 shortfall)
                self._reserve_budget(st, lots)               # 下單即保留
            self._rate.acquire()                             # 爆發式 45/s 窗口
            try:
                order_no = self.broker.place_limit_buy(sym, limit_up, lots)
                with self._lock:
                    st.order_no = order_no
                    st.pre_order_no = order_no    # 追蹤 P (市價盲送蓋 order_no 後靠此撤孤兒單)
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

    def on_first_trade(self, symbol: str):
        """首筆真成交 (isTrial 已濾) → 立**開盤訊號**旗標 (冪等)。

        市價搶單走 **9:00 時間驅動盲送** (start_market_chase);盲送迴圈讀 first_trade_fired,
        一旦立旗就送最後一筆市價後停送 (開盤訊號停送)。這裡不做任何量門檻淘汰
        (2026-08-29 移除首筆最小張數判斷 — size 單位是張非股,舊 //1000 門檻會誤丟大量開盤股)。"""
        if not self.is_live():
            return
        with self._lock:
            st = self.trades.get(symbol)
            if st is None or st.first_trade_fired:
                return
            st.first_trade_fired = True

    def start_market_chase(self, symbols: list, start_time, cutoff_time):
        """9:00:00 起對每檔**盲送**市價單搶進 (2026-08-24 定案,取代首筆成交觸發;2026-08-28 管線化):
          - start_time (09:00:00) 起,每檔一 cadence daemon thread 精準等到時點才開始
          - **管線化 (不等券商回覆就送下一筆)**: cadence 每輪 rate.acquire (45/s) → 生一條 sender thread
            送一筆 → 立刻回圈 → 唯一節拍器是 45/s。少檔時也衝到近 45/s (同步版被 REST ~100ms 卡 ~5/s/檔)
          - **每輪重算** shortfall = target − 已成交;shortfall≤0 → 停 (已足額,防超 target)
          - **第一筆委託成功** (chase_done) → 蓋 order_no=M + 預算轉移 (釋預掛保留、改保留市價,等額 →
            budget_used 不變) + 撤還沒成交的預掛 P (rule A) → cadence 停送
          - **管線多送也成功** (已有一筆) → 撤掉那筆多送的 M (還 pending 就撤;已成交=超買,由 order_log→
            _on_fill 計入 filled_lots,出場全量賣掉)。使用者定案 (2026-08-28): 為求最快排到、接受此殘餘超買
          - 2026-08-28: **移除 rule B(第一盤成交來就停)** — 免快開盤股(集合競價全被退→第一盤成交一到
            就停)從沒真送進逐筆 (6226)
          - 停止: chase_done / shortfall≤0 / 致命拒單 / cutoff (09:03) / kill switch / 淘汰或出場 / 硬上限 / 13:23
          - 處置股禁市價 → 不盲送 (只留 08:59:58 漲停價預掛限價單)
        超買防線: chase_done 停送 + 管線多送 M 自撤 → 一般不超 target;runaway 由總曝險硬上限
        (_buy_cost_actual > total_budget → breach → 撤所有 pending 買單) 兜底。每筆 M 都進 order_log →
        超買部位不會變隱形裸單,出場全量賣掉。孤兒 P 出場/13:23 一併撤 (_cancel_orphan_pre)。"""
        with self._lock:
            if self._trade_date and self._chase_started_date == self._trade_date:
                logger.warning("[session] 今日已啟動市價盲送 — 跳過重複 (冪等保護)")
                return
            self._chase_started_date = self._trade_date
        for sym in symbols:
            threading.Thread(target=self._market_chase_worker,
                             args=(sym, start_time, cutoff_time),
                             name=f"chase-{sym}", daemon=True).start()

    def _market_chase_worker(self, symbol: str, start_time, cutoff_time):
        # 精準等到 start_time (09:00:00) — 骨架仿 pre_order timer 細等待
        while True:
            now = datetime.now()
            target = now.replace(hour=start_time.hour, minute=start_time.minute,
                                 second=start_time.second, microsecond=0)
            remaining = (target - now).total_seconds()
            if remaining <= 0:
                break
            time.sleep(0.01 if remaining < 1 else remaining - 0.5)
        # 盲送 cadence 迴圈 (2026-08-28 管線化): **不等券商回覆就送下一筆**。每輪 rate.acquire (45/s
        # 節拍) → 生一條 sender daemon thread (self._chase_send_one) 送一筆 → 立刻回圈。少檔時也能衝到
        # 接近 45/s (同步版被 REST 往返 ~100ms 卡在 ~5/s/檔);inflight 號誌當併發安全帽,防 thread 爆量。
        # 停送: chase_done (已有一筆委託成功,由 sender 設) / shortfall≤0 (每輪重算,防超 target) /
        #       淘汰·出場·硬上限·處置股 / cutoff (09:03) / 13:23。移除 rule B (見 2026-08-28 定案)。
        inflight = threading.Semaphore(CHASE_MAX_INFLIGHT)

        def _send_and_release(sf):
            try:
                self._chase_send_one(symbol, sf)
            finally:
                inflight.release()

        # 登記「還在搶」→ 公平均分母數 N (見 _chase_pace_gate)。任何出口都經 finally 退出登記。
        with self._lock:
            self._active_chase.add(symbol)
        try:
            while True:
                if not self.is_live():
                    return
                inflight.acquire()        # 併發安全帽 (真正節流是公平均分節拍 + 45/s rate;高延遲自然回壓)
                with self._lock:
                    st = self.trades.get(symbol)
                    stop = (st is None or st.chase_done or st.stopped_reason or st.exited
                            or self._budget_breached or st.is_disposition)
                    first_came = bool(st and st.first_trade_fired)   # 開盤訊號 (on_trade 收首筆真成交時設)
                    shortfall = 0 if st is None else st.target_lots - st.filled_lots
                    projected_over = (not stop and self.total_budget > 0 and self._buy_cost_actual
                                      + shortfall * st.limit_up * 1000 > self.total_budget)
                now_t = datetime.now().time()
                past_cancel = (self.cancel_pending_time is not None and now_t >= self.cancel_pending_time)
                if stop or shortfall <= 0 or projected_over or now_t >= cutoff_time or past_cancel:
                    inflight.release()
                    if projected_over:
                        logger.warning(f"[session] {symbol} 盲送前投影超總預算 → 停 (總曝險硬上限;靠預掛/出場守)")
                    elif now_t >= cutoff_time:
                        logger.warning(f"[session] {symbol} 市價盲送到 {cutoff_time} 仍未成功 → 停 (靠預掛守)")
                    return
                if first_came:
                    # 開盤訊號 (第一盤成交) 到 → 送**最後一筆**市價 (逐筆、非致命即委託成功=進場) 後**停送**
                    # (2026-08-28 使用者定案: 停在「收到開盤訊號」而非「等委託成功回報」——後者會多噴一個
                    #  REST 往返的量。開盤後送出的只剩 [開盤→訊號傳到我方] 窗內在飛的那幾筆,其超買使用者接受)。
                    # 最後一筆**不套公平均分節拍** — 決定性的搶單要最快 (只過全域 45/s 上限)。
                    inflight.release()
                    self._rate.acquire()
                    logger.warning(f"[session] {symbol} 開盤訊號到 → 送最後一筆市價 {shortfall} 張後停送")
                    self._chase_send_one(symbol, shortfall)   # 同步送最後一筆 (cadence thread 反正要結束)
                    return
                # 盲送: 先過**公平均分節拍** (依還在搶的檔數 N → 每檔 N×(1/45)s 一張),再過全域 45/s 上限
                self._chase_pace_gate(st)
                self._rate.acquire()      # 全域爆發式 45/s 上限 (多標的+出場+撤單共用) — backstop
                threading.Thread(target=_send_and_release, args=(shortfall,), daemon=True,
                                 name=f"chase-send-{symbol}").start()
        finally:
            with self._lock:
                self._active_chase.discard(symbol)

    def _chase_pace_gate(self, st: "SymbolTrade"):
        """狂送公平均分節拍: 把 45/s 依「目前還在搶的檔數 N」平均分給每檔 →
        每檔 interval = N/max_per_sec (1檔22.2ms、2檔各44.4ms…),搶到一檔→N−1→剩下即刻加速。
        每檔各自按 st.chase_next_send_at 均勻鋪 (非爆發);睡在鎖外不擋別檔。"""
        with self._lock:
            n = max(1, len(self._active_chase))
            interval = n / max(1, self._rate.max_per_sec)
            now = time.monotonic()
            slot = max(now, st.chase_next_send_at)
            st.chase_next_send_at = slot + interval
            wait = slot - now
        if wait > 0:
            time.sleep(wait)

    def _chase_send_one(self, symbol: str, shortfall: int) -> str:
        """送**一筆**市價 + 處理結果。可同步呼叫 (replay/測試);production 由 cadence 迴圈在 daemon
        thread 內呼叫 (管線化,不等回覆就送下一筆)。回結果字串。

        - 委託成功 · 第一筆 (chase_done 未立): 蓋 order_no=M + 預算轉移 (釋預掛保留、改保留市價 shortfall)
          + 撤預掛剩餘 P (rule A) + 設 chase_done → cadence 停送。
        - 委託成功 · 管線多送 (chase_done 已立): 已有一筆搶到 → **撤掉這筆多送的 M** (還 pending 就撤、
          免多部位;已成交 = 超買,已由 _log_order→_on_fill 計入 filled_lots,出場全量賣掉;硬上限兜底
          runaway)。使用者定案 (2026-08-28): 為求最快排到市價、接受此殘餘超買。
        - 致命拒因 → 設 stopped_reason (保留 P);非致命 (含集合競價「不可市價」) → 什麼都不做,cadence 續送。
        """
        try:
            no = self.broker.place_market_buy(symbol, shortfall)
        except Exception as e:
            if _is_fatal_reject(e):
                with self._lock:
                    st = self.trades.get(symbol)
                    if st is not None:
                        st.stopped_reason = st.stopped_reason or "fatal_reject"
                logger.critical(f"[session] ⚠ {symbol} 市價盲送遇停止拒因 → 放棄市價 (保留預掛 P 續守): {e}")
                return "fatal"
            return "rejected"          # 非致命 (集合競價/暫時) → cadence 繼續送
        # 委託成功 — 每筆都記 order_log (超買部位/對帳/硬上限都看得到,不會變隱形裸單)
        self._log_order(no, symbol, "buy", "market_buy", shortfall, 0)
        with self._lock:
            st = self.trades.get(symbol)
            if st is None:
                return "orphan"
            first = not st.chase_done
            if first:
                st.chase_done = True
                st.order_no = no
                st.order_kind = "market_buy"
                st.order_status = "pending"
                self._release_budget(st)              # 釋放預掛 P 剩餘保留
                self._reserve_budget(st, shortfall)   # 改保留市價 M (等額 → budget_used 守恆)
            # aborted 含 _budget_breached: 送單是放鎖後做的,這期間別檔成交可能觸發硬上限 latch。
            aborted = bool(st.stopped_reason) or st.exited or self._budget_breached
        if first:
            if aborted:
                logger.warning(f"[session] {symbol} 市價盲送成功後發現已停 → 撤 M={no}+預掛 P")
                self.cancel_symbol_orders_async(symbol, "chase_aborted")
                self._cancel_orphan_pre(symbol, "chase_aborted")
                return "aborted"
            logger.warning(f"[session] {symbol} 市價盲送 {shortfall} 張 → 委託成功 → 停送;撤預掛剩餘 P")
            self._cancel_orphan_pre(symbol, "chase_sent_cancel_pre")   # 撤 P (rule A)
            return "accepted"
        # 管線多送也成功 → 撤掉這筆 (已有一筆;還 pending 就撤、免多部位,已成交=超買由出場守)
        logger.warning(f"[session] {symbol} 管線多送 M={no} 也委託成功 → 撤 (已搶到一筆;超買靠出場全量賣)")
        self._cancel_one_order_async(no, symbol, "chase_extra")
        return "accepted_extra"

    def _cancel_one_order(self, order_no: str, symbol: str, reason: str):
        """撤單一指定 order_no (管線多送的額外 M 用;同步試一次)。閘門 = _can_manage;走 45/s 額度。
        撤了 live 買單 → 記 last_buy_cancel_ts,讓隨後出場等在途成交回報窗口 (審查 #3,2026-08-28 補:
        免撤單與成交競race時、出場提早讀 filled_lots 漏賣那筆超買)。
        2026-09-09: 查無/查詢失敗/非終端錯 → **不再標 cancelled**,入佇列由 worker 重試到券商確認。"""
        if not order_no or not self._can_manage():
            return
        res = self._try_cancel_sync(order_no, symbol, reason)
        if res in ("ok", "already_cancelled"):
            self._confirm_cancelled(order_no, symbol, reason, source=f"sync:{res}")
        elif res == "filled_before_cancel":
            logger.warning(f"[session] {symbol} 撤管線多送 M={order_no}: 已成交 (=超買,靠出場全量賣)")
        else:
            logger.warning(f"[session] {symbol} 撤管線多送 M={order_no} 未確認 → 已入撤單佇列重試")
        with self._lock:
            st = self.trades.get(symbol)
            if st is not None:
                st.last_buy_cancel_ts = time.time()

    def _cancel_one_order_async(self, order_no: str, symbol: str, reason: str):
        """行情/sender thread 專用 — **入撤單佇列** (2026-09-09: 取代 thread-per-cancel。
        8 條撤單 thread 在 0.66 s 內各打一次 get_order_results 超過富邦帳務查詢 5/s →
        「業務系統流量控管」被當成查無 → 誤標 cancelled,node3 4 筆裸單事故)。
        worker 一輪一次快照 + ≤8 thread 扇出 cancel_by_obj,成功才標 cancelled。
        測試/replay 可把此屬性換成 _cancel_one_order 同步化。"""
        self.request_cancel(order_no, symbol, reason)

    # ─── 撤單佇列 worker (2026-09-09 node3 事故 A2) ────────────────

    classify_cancel_error = staticmethod(classify_cancel_error)

    def request_cancel(self, order_no: str, symbol: str = "", reason: str = ""):
        """把書號排進撤單佇列 (去重: 已在佇列只更新 reason)。row 存在且已非 pending → 不必撤。
        row 不存在 (券商權威掃單發現的 order_log 外孤兒) 也可入列 — 結案時 row None 容錯。
        懶啟動 worker (auto_cancel_worker 且 mode=real 且 broker 在);hub/sim 不啟動。"""
        if not order_no:
            return
        now = time.time()
        with self._lock:
            row = self.order_log.get(order_no)
            if row is not None and row["status"] != "pending":
                return
            if not symbol and row is not None:
                symbol = row.get("symbol", "")
            item = self._cancel_queue.get(order_no)
            if item is None:
                self._cancel_queue[order_no] = {
                    "symbol": symbol, "reason": reason, "first_ts": now,
                    "attempts": 0, "next_ts": now, "last_crit_ts": 0.0,
                }
            else:
                item["reason"] = reason or item["reason"]
                item["next_ts"] = min(item["next_ts"], now)   # 再次請求 = 立刻再試
            if row is not None:
                if row.get("cancel_state") != "unconfirmed":
                    row["cancel_state"] = "queued"
                row["cancel_reason"] = reason or row.get("cancel_reason", "")
        self._ensure_cancel_worker()

    def _note_cancel_err(self, order_no: str, msg: str, state: str = ""):
        with self._lock:
            row = self.order_log.get(order_no)
            if row is not None:
                row["cancel_err"] = str(msg or "")[:160]
                if state:
                    row["cancel_state"] = state

    def _cancel_worker_alive(self) -> bool:
        t = self._cancel_worker_thread
        return bool(t is not None and t.is_alive())

    def _ensure_cancel_worker(self):
        """懶啟動 (或重啟死掉的) 撤單 worker daemon thread;已活著 → 只喚醒。"""
        if not self.auto_cancel_worker:
            return
        with self._lock:
            if self.mode != "real" or self.broker is None:
                return
            if not self._cancel_worker_alive():
                t = threading.Thread(target=self._cancel_worker_loop,
                                     name="cancel-worker", daemon=True)
                self._cancel_worker_thread = t
                t.start()
                logger.info("[session] 撤單 worker 啟動")
        self._cancel_wakeup.set()

    def _cancel_worker_loop(self):
        """單一 daemon worker: 有到期項就跑一輪 run_once;每輪 try/except 不會死。
        另在 09:05~13:20 每 60 s 跑一次盤中低頻對帳 (券商權威掃單,預設 dry-run)。"""
        while True:
            try:
                with self._lock:
                    nxt = min((it["next_ts"] for it in self._cancel_queue.values()), default=None)
                if nxt is None:
                    timeout = 1.0
                else:
                    timeout = min(0.5, max(0.005, nxt - time.time()))
                self._cancel_wakeup.wait(timeout)
                self._cancel_wakeup.clear()
                with self._lock:
                    has_items = bool(self._cancel_queue)
                if has_items:
                    self._cancel_worker_run_once()
                self._maybe_intraday_sweep()
                self._maybe_log_foreign_reject_summary()
            except Exception as e:
                logger.exception(f"[session] 撤單 worker 例外 (續跑): {e}")
                time.sleep(0.5)

    def _maybe_intraday_sweep(self):
        """09:05~13:20 每 60 s 一次券商權威掃單 (遠低於 5/s)。dry_run 由 env 決定 (預設 true)。
        2026-09-14 node1 (清單 33,940 筆、每次全清單查詢 ≈540 MB、4~11 s): 已有全清單查詢在飛 → 本輪略過
        (不算一輪,下個 worker 迴圈再看);最近一次查詢 > INTRADAY_SWEEP_BIG_LIST_ROWS (預設 1 萬) 筆 →
        間隔拉長為 INTRADAY_SWEEP_BIG_LIST_INTERVAL_SEC (預設 300 s)。broker 無此資訊 (替身) → 照舊。"""
        now_m = time.monotonic()
        if now_m - self._last_intraday_sweep < self._intraday_sweep_interval():
            return
        t = datetime.now().time()
        if not (_INTRADAY_SWEEP_START <= t <= _INTRADAY_SWEEP_END):
            return
        if not self._can_manage() or not getattr(self.broker, "healthy", True):
            return
        if self._broker_order_query_busy():
            if not self._intraday_skip_logged:
                self._intraday_skip_logged = True
                logger.info("[session] 盤中對帳略過: 已有全清單委託查詢在飛 (等它結束再掃,免重疊查詢撐爆記憶體)")
            return
        self._intraday_skip_logged = False
        self._last_intraday_sweep = now_m
        try:
            self.intraday_reconcile_once(dry_run=_intraday_sweep_dry_run())
        except Exception as e:
            logger.error(f"[session] 盤中對帳例外: {e}")

    def _broker_order_query_busy(self) -> bool:
        """broker 目前是否有全清單委託查詢在飛 (RealOrderClient.order_query_in_flight;替身沒有 → False)。"""
        fn = getattr(self.broker, "order_query_in_flight", None) if self.broker is not None else None
        if not callable(fn):
            return False
        try:
            return fn() is True
        except Exception:
            return False

    def _broker_last_query_rows(self) -> Optional[int]:
        """最近一次成功全清單查詢筆數 (RealOrderClient.last_order_query_rows;替身/未知 → None)。"""
        fn = getattr(self.broker, "last_order_query_rows", None) if self.broker is not None else None
        if not callable(fn):
            return None
        try:
            v = fn()
        except Exception:
            return None
        return v if isinstance(v, int) and not isinstance(v, bool) else None

    def _intraday_sweep_interval(self) -> float:
        """盤中對帳間隔: 清單大 (> env INTRADAY_SWEEP_BIG_LIST_ROWS) → env INTRADAY_SWEEP_BIG_LIST_INTERVAL_SEC,
        否則 _INTRADAY_SWEEP_INTERVAL_SEC。env 每次現讀 (免重啟)。"""
        rows = self._broker_last_query_rows()
        limit = _env_int("INTRADAY_SWEEP_BIG_LIST_ROWS", _INTRADAY_SWEEP_BIG_LIST_ROWS)
        big = rows is not None and rows > limit
        if big != self._intraday_big_list:
            self._intraday_big_list = big
            if big:
                logger.warning(f"[session] 委託清單 {rows} 筆 > {limit} → 盤中對帳間隔拉長為 "
                               f"{_env_float('INTRADAY_SWEEP_BIG_LIST_INTERVAL_SEC', _INTRADAY_SWEEP_BIG_LIST_INTERVAL_SEC):g} s"
                               f" (同登入可能有其他程式大量下單)")
            else:
                logger.info(f"[session] 委託清單 {rows} 筆 ≤ {limit} → 盤中對帳間隔恢復 {_INTRADAY_SWEEP_INTERVAL_SEC:g} s")
        if big:
            return _env_float("INTRADAY_SWEEP_BIG_LIST_INTERVAL_SEC", _INTRADAY_SWEEP_BIG_LIST_INTERVAL_SEC)
        return _INTRADAY_SWEEP_INTERVAL_SEC

    def _broker_query_mark(self) -> Optional[float]:
        """「此刻」的 broker 查詢時鐘值 (RealOrderClient.query_clock) — 當 fresh_after 用;替身沒有 → None。"""
        fn = getattr(self.broker, "query_clock", None) if self.broker is not None else None
        if not callable(fn):
            return None
        try:
            v = fn()
        except Exception:
            return None
        return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None

    def _cancel_giveup_time(self) -> dtime:
        base = datetime.combine(datetime.today(), self.trading_end_time)
        return (base + timedelta(minutes=_CANCEL_GIVEUP_AFTER_END_MIN)).time()

    def _after_trading_end(self) -> bool:
        return datetime.now().time() >= self.trading_end_time

    def _cancel_worker_run_once(self, now: Optional[float] = None) -> dict:
        """跑**一輪**撤單佇列 (可同步驅動 — 測試/replay/收盤 drain 用;production 由 worker thread 叫)。
        now = epoch 秒 (time.time());None = 現在。
        演算法: due = next_ts<=now → **一次** get_order_snapshot() → 在快照者 ≤8 thread 扇出
        cancel_by_obj (各過 45/s _rate) → 成功才標 cancelled + _close_cancel + 出佇列;
        撤單失敗訊息走 classify_cancel_error;不在快照者: row 已終結 → 出佇列,否則 attempts+1 退避
        (row 維持 pending、cancel_state=unconfirmed);快照失敗 (流量控管/逾時) → 全部 +1 s 不計 attempts。
        _can_manage() False 或 broker 不健康 → 本輪略過不計 attempts。
        回 {"snapshot_ok", "cancelled", "pending", "filled_before_cancel", "skipped"}。"""
        now = time.time() if now is None else float(now)
        out = {"snapshot_ok": True, "cancelled": [], "pending": [],
               "filled_before_cancel": [], "rejected": [], "skipped": False}
        with self._cancel_run_lock:
            self._cancel_run_once_inner(now, out)
        return out

    def _cancel_run_once_inner(self, now: float, out: dict):
        with self._lock:
            due = [(no, dict(it)) for no, it in self._cancel_queue.items() if it["next_ts"] <= now]
        if not due:
            out["pending"] = self._queued_order_nos()
            return
        if not self._can_manage() or not getattr(self.broker, "healthy", True):
            # 本輪略過不計 attempts — 但到期項必須延後,否則 worker 迴圈 / 收盤 drain 看到 next_ts 仍在
            # 過去會零延遲重跑 (審查: 交易 WS 斷線時 40 s 內 48 萬次搶鎖)。_on_broker_reconnected 會把
            # next_ts 拉回 now 喚醒,復原延遲不受影響。
            with self._lock:
                for no, _ in due:
                    it = self._cancel_queue.get(no)
                    if it is not None:
                        it["next_ts"] = now + _CANCEL_SKIP_BACKOFF_SEC
            out["skipped"] = True
            out["pending"] = [no for no, _ in due]
            return
        broker = self.broker
        snap_fn = getattr(broker, "get_order_snapshot", None)
        if snap_fn is None:
            # 舊式 broker/fake 無快照 API → 退回逐筆 broker.cancel (facade) 路徑
            self._cancel_run_legacy(due, now, out)
            return
        # ── 一次快照 ──
        try:
            snapshot = _call_with_timeout(snap_fn, _CANCEL_SDK_TIMEOUT_SEC, "cancel-snapshot")
        except Exception as e:
            kind = _exc_kind(e)
            out["snapshot_ok"] = False
            with self._lock:
                for no, _ in due:
                    it = self._cancel_queue.get(no)
                    if it is not None:
                        it["next_ts"] = now + 1.0     # 不計 attempts
            for no, _ in due:
                self._note_cancel_err(no, f"QUERY:{e}")
            out["pending"] = [no for no, _ in due]
            logger.warning(f"[session] 撤單 worker 快照失敗 ({kind}: {e}) → {len(due)} 筆 1 s 後重試"
                           f" (不計 attempts)")
            return
        by_no = {}
        for e in (snapshot or []):
            try:
                by_no[str(e.get("order_no", ""))] = e
            except Exception:
                continue
        not_in_snap = [(no, it) for no, it in due if no not in by_no]
        # ── 在快照者: 先依快照 status 預分類 — 已終結 (30/40/50/90) 者不送撤單,直接結案 ──
        in_snap = []
        for no, it in due:
            if no not in by_no:
                continue
            entry = by_no[no]
            if not self._settle_by_snapshot_status(no, it, entry, out):
                in_snap.append((no, it, entry))
        # ── 在快照者 (status 未終結): 扇出撤單 ──
        if in_snap:
            with self._lock:
                for no, _, _ in in_snap:
                    row = self.order_log.get(no)
                    if row is not None:
                        row["cancel_state"] = "sent"
            results = self._fanout_cancel(in_snap, _CANCEL_SDK_TIMEOUT_SEC)
            for no, it, entry in in_snap:
                r = results.get(no)
                if r is None:
                    # 逾時 = 未確認、不計 attempts
                    self._note_cancel_err(no, "撤單 SDK 逾時", state="unconfirmed")
                    with self._lock:
                        q = self._cancel_queue.get(no)
                        if q is not None:
                            q["next_ts"] = now + 1.0
                    out["pending"].append(no)
                    continue
                status, msg = r
                if status == "ok":
                    self._confirm_cancelled(no, it["symbol"], it["reason"], source="worker")
                    out["cancelled"].append(no)
                    continue
                cls = classify_cancel_error(msg)
                if cls == "already_cancelled":
                    self._confirm_cancelled(no, it["symbol"], it["reason"], source=f"worker:{msg[:40]}")
                    out["cancelled"].append(no)
                elif cls == "filled_before_cancel":
                    fq = entry.get("filled_qty")
                    if self._settle_filled_before_cancel(
                            no, it["symbol"], it["reason"], None if fq is None else int(fq), msg,
                            snapshot_status=str(entry.get("status") or "")):
                        out["filled_before_cancel"].append(no)
                    else:
                        out["pending"].append(no)     # 快照落後無法核實 → 留佇列,下一輪用新快照結案
                else:
                    self._cancel_retry_later(no, now, msg)
                    out["pending"].append(no)
        # ── 不在快照者 ──
        for no, it in not_in_snap:
            with self._lock:
                row = self.order_log.get(no)
                terminal = row is not None and row["status"] != "pending"
            if terminal:
                self._dequeue_cancel(no, f"row 已 {row['status']} (回報先到)")
                continue
            self._cancel_retry_later(no, now, "NOT_IN_SNAPSHOT (查無此書號;可能後檯延遲)")
            out["pending"].append(no)
        self._cancel_giveup_check(now)

    def _cancel_run_legacy(self, due: list, now: float, out: dict):
        """無 get_order_snapshot 的 broker (舊 fake/replay) — 逐筆 broker.cancel 同步試。"""
        for no, it in due:
            res = self._try_cancel_sync(no, it["symbol"], it["reason"], enqueue=False)
            if res in ("ok", "already_cancelled"):
                self._confirm_cancelled(no, it["symbol"], it["reason"], source=f"legacy:{res}")
                out["cancelled"].append(no)
            elif res == "filled_before_cancel":
                out["filled_before_cancel"].append(no)    # 結案已由 _try_cancel_sync 內處理 (legacy 無快照可核實)
            elif res == "lookup":
                with self._lock:
                    q = self._cancel_queue.get(no)
                    if q is not None:
                        q["next_ts"] = now + 1.0
                out["snapshot_ok"] = False
                out["pending"].append(no)
            else:
                with self._lock:
                    row = self.order_log.get(no)
                    terminal = row is not None and row["status"] != "pending"
                if terminal:
                    self._dequeue_cancel(no, f"row 已 {row['status']}")
                    continue
                self._cancel_retry_later(no, now, self._last_cancel_err.get(no, res))
                out["pending"].append(no)
        self._cancel_giveup_check(now)

    def _fanout_cancel(self, work: list, timeout: float) -> dict:
        """≤_CANCEL_FANOUT_THREADS 條 daemon thread 扇出 cancel_by_obj (各過 self._rate)。
        回 {order_no: ("ok"|"err", msg)};逾時未回者不在 dict 內 (caller 視為未確認)。"""
        results: dict = {}
        rlock = threading.Lock()
        sem = threading.Semaphore(_CANCEL_FANOUT_THREADS)
        broker = self.broker
        cancel_by_obj = getattr(broker, "cancel_by_obj", None)

        def _one(no, it, entry):
            try:
                self._rate.acquire()
                if cancel_by_obj is not None:
                    cancel_by_obj(entry.get("_obj"), no, it["symbol"], it["reason"])
                else:
                    broker.cancel(no, it["symbol"], reason=it["reason"])
                r = ("ok", "")
            except Exception as e:           # noqa: BLE001
                r = ("err", str(e))
            finally:
                sem.release()
            with rlock:
                results[no] = r

        threads = []
        deadline = time.monotonic() + timeout
        for no, it, entry in work:
            if not sem.acquire(timeout=max(0.0, deadline - time.monotonic())):
                break
            t = threading.Thread(target=_one, args=(no, it, entry), daemon=True,
                                 name=f"cancel-fan-{no}")
            t.start()
            threads.append(t)
        for t in threads:
            t.join(max(0.0, deadline - time.monotonic()))
        with rlock:
            return dict(results)

    def _queued_order_nos(self) -> list:
        with self._lock:
            return list(self._cancel_queue)

    def _dequeue_cancel(self, order_no: str, why: str):
        with self._lock:
            self._cancel_queue.pop(order_no, None)
            row = self.order_log.get(order_no)
            if row is not None and row.get("cancel_state"):
                row["cancel_state"] = ""
        logger.info(f"[session] 撤單佇列出列 {order_no}: {why}")

    def _cancel_retry_later(self, order_no: str, now: float, err: str):
        """撤不到/未確認 → attempts+1、退避、row 維持 pending + cancel_state=unconfirmed;升級告警。"""
        with self._lock:
            it = self._cancel_queue.get(order_no)
            if it is None:
                return
            it["attempts"] += 1
            n = it["attempts"]
            back = _CANCEL_BACKOFF[n - 1] if n - 1 < len(_CANCEL_BACKOFF) else _CANCEL_BACKOFF[-1]
            it["next_ts"] = now + back
            row = self.order_log.get(order_no)
            if row is not None:
                row["cancel_state"] = "unconfirmed"
                row["cancel_attempts"] = n
                row["cancel_err"] = str(err or "")[:160]
            sym, reason = it["symbol"], it["reason"]
            crit = False
            if n >= _CANCEL_CRIT_ATTEMPTS and now - it["last_crit_ts"] >= _CANCEL_CRIT_REPEAT_SEC:
                it["last_crit_ts"] = now
                crit = True
        msg = (f"[session] ⚠ {sym} 撤單未確認 order={order_no} 第 {n} 次 ({reason}): {err}"
               f" → {back}s 後重試 (row 維持 pending,絕不標 cancelled)")
        if crit:
            logger.critical(msg + " — 券商端可能仍 live,請人工核對")
        elif n == _CANCEL_WARN_ATTEMPTS:
            logger.warning(msg)
        else:
            logger.info(msg)

    def _cancel_giveup_check(self, now: float):
        """trading_end+10 分仍未確認 → 放棄 (留 order_log pending、cancel_state=unconfirmed) 並 CRITICAL 點名。"""
        try:
            t = datetime.fromtimestamp(now).time()
        except Exception:
            return
        if t < self._cancel_giveup_time():
            return
        with self._lock:
            # 只放棄「真的試過、仍未確認」的項 (attempts>=1) — 剛入列還沒試的 (收盤 drain 晚跑/手動刪單)
            # 至少讓 worker 打一次快照+撤單,免 13:34 後的新請求零嘗試就被丟掉
            gone = [(no, it) for no, it in self._cancel_queue.items() if it["attempts"] >= 1]
            for no, it in gone:
                self._cancel_queue.pop(no, None)
                row = self.order_log.get(no)
                if row is not None:
                    row["cancel_state"] = "unconfirmed"
                    row["cancel_err"] = (f"GIVE_UP after {it['attempts']} attempts: "
                                         + str(row.get("cancel_err") or ""))[:160]
            self._cancel_stats["given_up"] += len(gone)
        for no, it in gone:
            logger.critical(f"[session] ⚠⚠ 撤單放棄 order={no} {it['symbol']} ({it['reason']}) — "
                            f"{it['attempts']} 次未確認,券商端可能仍 live,**需人工到券商端撤單**")

    def _close_cancel(self, order_no: str, symbol: str, reason: str):
        """券商確認撤單後同步 st/預算/隔日賣 (caller 持鎖與否皆可;內部取鎖)。冪等。"""
        freed_overnight = ""
        with self._lock:
            row = self.order_log.get(order_no)
            if row is not None:
                row["cancel_state"] = ""
            self._cancel_queue.pop(order_no, None)
            st = self.trades.get(symbol) if symbol else None
            if st is None and row is not None:
                st = self.trades.get(row.get("symbol", ""))
            if st is not None:
                if st.order_no == order_no:
                    st.order_status = "cancelled"
                    st.order_no = ""
                    st.stopped_reason = st.stopped_reason or reason
                    if st.order_kind in ("pre_limit", "market_buy"):
                        self._release_budget(st)
                if row is None or row.get("action") == "buy":
                    st.last_buy_cancel_ts = time.time()   # 撤了 live 買單 → 出場等在途成交 (審查 #3)
            # 隔日賣單被券商確認撤掉 → **認單不認 reason**: 撤成 = 沒掛,槽位必須釋放 (審查: 原本只認
            # overnight_skip;手動刪單 / 券商回報 30 先到 → sell_placed 卡 True 指向已撤單 → 整天不再賣)。
            # 使用者在委託表刪單 (manual_cancel) = 「不要賣」→ 一併 skip,免下一 tick 又自動重掛
            # (「恢復賣出」可解);其他來源只釋放槽位,規則觸發時照常重掛。
            for o in self.overnight.values():
                if o.get("sell_order_no") == order_no:
                    o["sell_placed"] = False
                    o["sell_order_no"] = ""
                    if reason == "manual_cancel":
                        o["skip"] = True
                    freed_overnight = o.get("symbol", "")
        if freed_overnight:
            logger.warning(f"[session] 隔日賣 {freed_overnight} 賣單 {order_no} 已撤 ({reason}) → 解除 sell_placed"
                           f"{';使用者手動刪單 → skip=True (按「恢復賣出」才會再掛)' if reason == 'manual_cancel' else ' (規則再觸發會重掛)'}")

    def _confirm_cancelled(self, order_no: str, symbol: str, reason: str, source: str = ""):
        """券商確認撤單 (cancel 成功 / 回報 30|40 / 「取消單已不允許取消」) → 唯一寫 cancelled 的地方。
        冪等: 同一書號的重複確認 (同步撤成後又收到 ft30 status 30 回報、或回報先到再同步撤) 仍跑
        _close_cancel (依本次 reason 補齊 st/隔日賣欄位,如 overnight_skip 的 sell_placed),
        但不重複計數「已確認」、不重複告警、不重複觸發晚結案 hook。"""
        with self._lock:
            row = self.order_log.get(order_no)
            fresh = ((row is not None and row["status"] == "pending")
                     or order_no in self._cancel_queue)
            self._mark_order(order_no, "cancelled")
            self._close_cancel(order_no, symbol, reason)     # 內部取鎖 (RLock 可重入)
            if fresh:
                self._cancel_stats["confirmed"] += 1
        if not fresh:
            logger.info(f"[session] {symbol} 撤單確認 order={order_no} 重複 ({reason}"
                        f"{'; ' + source if source else ''}) — 已結案,略過計數")
            return
        logger.warning(f"[session] {symbol} 撤單確認 order={order_no} ({reason}"
                       f"{'; ' + source if source else ''})")
        self._fire_late_confirm()

    def _fire_late_confirm(self):
        """trading_end 之後的結案 → 呼叫 on_late_confirm (runner: 重寫隔日賣清單)。鎖外。"""
        hook = self.on_late_confirm
        if hook is None or not self._after_trading_end():
            return
        try:
            hook()
        except Exception as e:
            logger.error(f"[session] on_late_confirm 例外: {e}")

    def _apply_auth_fill_locked(self, order_no: str, row: dict, auth_lots: int) -> int:
        """以券商權威成交張數覆寫一列 (reconcile_orders 與 filled_before_cancel 共用;caller 持鎖)。
        回補進的 delta (≤0 = 無差異)。硬上限 breach 由 caller 依 self._budget_breached 轉變偵測。"""
        delta = auth_lots - row["filled_lots"]
        if delta <= 0:
            return 0
        row["filled_lots"] = auth_lots                    # 覆寫非累加
        if row["filled_lots"] >= row["lots"] and row["status"] == "pending":
            row["status"] = "filled"
            row["terminal_ts"] = time.time()
        st = self.trades.get(row["symbol"])
        if st is not None:
            if row["action"] == "buy":
                prev_cost = st.avg_price * st.filled_lots
                st.filled_lots += delta
                # 回報遺失 → 無成交價可用,以漲停價近似 (保守偏高)
                st.avg_price = (prev_cost + st.limit_up * delta) / st.filled_lots
                st.budget_reserved = max(
                    0.0, st.budget_reserved - delta * st.limit_up * 1000)
                # 硬上限: 補收的買進也要進實際買進累計 + 重查 breach — 否則斷線期間
                # (正是 reconcile 存在的 3587 情境) 狂買回報遺失 → 硬上限被繞過 (審查 #2/#5)。
                self._buy_cost_actual += delta * st.limit_up * 1000
                st.buy_cost_actual += delta * st.limit_up * 1000
                if (not self._budget_breached and self.total_budget > 0
                        and self._buy_cost_actual > self.total_budget):
                    self._budget_breached = True
                if st.order_no == order_no and st.filled_lots >= st.target_lots:
                    st.order_status = "done"
                    st.order_no = ""
            else:
                st.filled_lots = max(0, st.filled_lots - delta)
        return delta

    def _late_fill_lots_locked(self, row: dict, delta: int) -> int:
        """出場後晚成交判定 (caller 持鎖;與 _on_fill 同條件): 買進補入 delta>0 且該檔已出場、出場 worker
        不在進行中、非使用者取消追蹤 → 這批沒有任何出場保護 (has_exposure 被 exited 擋死),回要再賣的張數。
        審查: 撤單前已成交 / 補收對帳只補 filled_lots 不再賣,之後真回報被單筆封頂成 0 → 永遠不賣 (隱形裸部位)。"""
        if delta <= 0 or row.get("action") != "buy":
            return 0
        st = self.trades.get(row.get("symbol", ""))
        if (st is None or not st.exited or st.exit_in_progress
                or st.stopped_reason == "manual_abandon"):
            return 0
        return delta

    def _start_late_fill_exit(self, symbol: str, lots: int, order_no: str, why: str = ""):
        """鎖外: 出場後晚成交 → 對**這批**張數另起 late-fill 賣 (A4b;_on_fill / 撤單前成交 / 補收共用)。"""
        logger.critical(f"[session] ⚠ {symbol} 出場後晚成交 {lots} 張 (order {order_no}"
                        f"{'; ' + why if why else ''}) → 重新觸發出場賣掉這批")
        threading.Thread(target=self._late_fill_exit_worker,
                         args=(symbol, lots, "late_fill_after_exit"),
                         name=f"late-exit-{symbol}", daemon=True).start()

    def _trigger_budget_breach(self, prefix: str):
        """總曝險硬上限剛被觸發 (鎖外): CRITICAL + 背景撤所有 pending 買單 (已成交部位不動,由出場賣)。"""
        logger.critical(
            f"[session] ⚠⚠ {prefix}總曝險超上限 — 實際買進 {self._buy_cost_actual:,.0f} "
            f"> 總預算 {self.total_budget:,.0f} → 停所有市價盲送 + 撤所有 pending 買單")
        threading.Thread(target=self.cancel_all_pending, args=("budget_breached",),
                         name="budget-breach-cancel", daemon=True).start()

    def _apply_auth_fill(self, order_no: str, auth_lots: int, why: str) -> int:
        """以券商權威成交張數補一列 (鎖內 _apply_auth_fill_locked) + 鎖外連鎖: 硬上限 breach → 撤所有
        pending 買單;已出場的檔補進買進 → late-fill 賣 (A4b)。回補進 delta。"""
        with self._lock:
            row = self.order_log.get(order_no)
            if row is None:
                return 0
            was = self._budget_breached
            delta = self._apply_auth_fill_locked(order_no, row, auth_lots)
            breach_now = self._budget_breached and not was
            late = self._late_fill_lots_locked(row, delta)
            symbol = row["symbol"]
        if delta > 0:
            logger.warning(f"[session] {symbol} 依券商權威補成交 {delta} 張 (order {order_no}; {why})")
        if late > 0:
            self._start_late_fill_exit(symbol, late, order_no, why)
        if breach_now:
            self._trigger_budget_breach(f"{why}補入後")
        return delta

    def _settle_rejected(self, order_no: str, symbol: str, err: str):
        """交易所拒單 (ft 0/10 回報有 error) / 快照 status 90 (失敗) → row rejected + 出佇列 + 清
        st.order_no + 釋放保留預算 (_on_order 拒單路徑抽出,撤單 worker 快照預分類共用)。
        策略出場賣單 (limit_sell / market_sell) 由 pending 被退 (2026-09-14) → ERROR + exited 回退 (出場可再觸發,
        部位才不會被 exited 鎖住沒有保護) + 記一輪出場失敗冷卻;出場 worker 進行中 / 取消追蹤者不回退。"""
        sell_refire = None
        with self._lock:
            row = self.order_log.get(order_no)
            was_pending = row is not None and row["status"] == "pending"
            self._mark_order(order_no, "rejected")
            self._cancel_queue.pop(order_no, None)      # 被拒 = 已終結,不必再撤
            if row is not None:
                row["cancel_state"] = ""
            st = self.trades.get(symbol) if symbol else None
            if st is None and row is not None:
                st = self.trades.get(row.get("symbol", ""))
            if st is not None and st.order_no == order_no:
                st.order_status = "rejected"
                st.order_no = ""
                # 拒單釋放保留預算 — 不釋放的話每次拒單都永久吃掉當日額度。
                # st.order_no 只會是買單 (賣單不寫 order_no),order_kind 再保險一層。
                if st.order_kind in ("pre_limit", "market_buy"):
                    self._release_budget(st)
                logger.error(f"[session] {st.symbol} 交易所拒單: {err}")
            if (was_pending and st is not None and row.get("action") == "sell"
                    and row.get("kind") in ("limit_sell", "market_sell")):
                reopened = (st.exited and not st.exit_in_progress
                            and st.stopped_reason != "manual_abandon")
                cd = 0.0
                if reopened:
                    st.exited = False                    # 賣單沒了 → 部位重新受出場保護 (冷卻後再觸發)
                    cd = self._note_exit_failed_locked(st)
                sell_refire = (st.symbol, int(row.get("lots") or 0), reopened, cd, st.exit_fail_cycles)
        if sell_refire is not None:
            sym, sell_lots, reopened, cd, n_failed = sell_refire
            logger.error(f"[session] ⚠ {sym} 出場賣單 order={order_no} {sell_lots} 張被券商退: {err} — "
                         + (f"exited 回退,第 {n_failed} 輪出場失敗 → {cd:g}s 後出場訊號可再觸發"
                            if reopened else "出場 worker 進行中 / 未出場 / 取消追蹤 → 不回退"))

    def _settle_by_snapshot_status(self, order_no: str, it: dict, entry: dict, out: dict) -> bool:
        """快照 status 已終結者**不送撤單**,直接依券商狀態結案 (審查: 對 30/40/50/90 再送撤單只會被拒,
        且非兩組關鍵字的拒撤文案會被當 retry → attempts 累加 → 假 WARNING/CRITICAL、白耗 45/s 額度):
          30 未成交刪單成功 / 40 部分成交剩餘取消 → (40 先補成交) 走券商確認撤單結案
          50 完全成交 → filled_before_cancel 結案 (用快照 filled_qty 補成交)
          90 失敗     → 拒單路徑 (row rejected、清 st.order_no、釋放保留)
        只有 status ∈ _BROKER_LIVE_STATUSES (或缺/未知) 才回 False 讓 caller 送撤單。"""
        stt = str(entry.get("status") or "")
        if not stt or stt in _BROKER_LIVE_STATUSES:
            return False
        fq = entry.get("filled_qty")
        fq = None if fq is None else int(fq)
        sym, reason = it["symbol"], it["reason"]
        if stt in _BROKER_TERMINAL_CANCELLED:
            if fq:
                self._apply_auth_fill(order_no, fq // 1000, f"快照 status {stt}")
            self._confirm_cancelled(order_no, sym, reason, source=f"snapshot status {stt}")
            out["cancelled"].append(order_no)
            return True
        if stt == "50":
            if self._settle_filled_before_cancel(order_no, sym, reason, fq, "快照 status 50 (完全成交)",
                                                 snapshot_status="50"):
                out["filled_before_cancel"].append(order_no)
            else:
                out["pending"].append(order_no)
            return True
        if stt == "90":
            self._settle_rejected(order_no, sym, "快照 status 90 (失敗)")
            out["rejected"].append(order_no)
            return True
        return False        # 其他 (9 連線逾時等) → 照送撤單讓券商判

    def _settle_filled_before_cancel(self, order_no: str, symbol: str, reason: str,
                                     filled_qty_shares: Optional[int], msg: str,
                                     snapshot_status: str = "") -> bool:
        """撤單回「成交單/部分成交單已不允許取消」(或快照 status 50) → **絕不標 cancelled**。
        以券商權威 filled_qty (股) 補成交 (同 reconcile_orders 逐 row 邏輯),再依訊息/快照 status 結案:
          全成 (fq ≥ 委託量 / status 50)                 → row filled (掃單翻回 pending 的列也在此結案)
          部分成交且剩餘已取消 (「部分成交單」/ status 40,0<fq<委託量) → 補成交 + 走券商確認撤單結案
                                                        (釋放剩餘保留、清 st.order_no;富邦 40 = 終結)
          fq 未知 / 為 0 / 與訊息不符 (撤單當下手上的快照 replica 落後於實況) → **無法核實 → 留佇列**
            (不在佇列者入列),worker 下一輪用新快照的 status 結案;計 attempts 讓 WARNING@3/CRITICAL@6
            照升級。legacy broker (無快照) 無從核實 → 出列、row 留 pending 等成交回報 (舊行為)。
        回 True = 已結案 (出佇列);False = 留佇列待核實。
        冪等 (docs 明定同一拒撤同時出現在同步回傳與 ft30 status 39 主動回報;審查 #6/#9): 只有**第一次
        結案**計數 / WARNING / 晚結案 hook,重複者 INFO;成交補入照做 (覆寫冪等)。row.cancel_err 前綴
        FILLED_BEFORE_CANCEL = 已結案標記。連鎖: 硬上限 breach → 撤所有 pending 買單;已出場的檔補進
        買進 → late-fill 賣 (A4b;審查 #14)。"""
        text = str(msg or "")
        partial = ("部分成交單" in text) or snapshot_status == "40"
        auth_lots = None if filled_qty_shares is None else int(filled_qty_shares) // 1000
        can_verify = getattr(self.broker, "get_order_snapshot", None) is not None
        delta = late = 0
        breach_now = False
        resolved = closed_partial = False
        row = None
        with self._lock:
            row = self.order_log.get(order_no)
            was_queued = order_no in self._cancel_queue
            already = bool(row is not None
                           and str(row.get("cancel_err") or "").startswith(_FBC_ERR_PREFIX))
            if row is None:
                self._cancel_queue.pop(order_no, None)
                resolved = True
            else:
                if auth_lots is not None and auth_lots > 0:
                    was_b = self._budget_breached
                    delta = self._apply_auth_fill_locked(order_no, row, auth_lots)
                    breach_now = self._budget_breached and not was_b
                    late = self._late_fill_lots_locked(row, delta)
                if row["status"] != "pending":
                    resolved = True                    # 回報先到已終結 → 沒有東西要核實
                elif auth_lots is not None and auth_lots >= row["lots"]:
                    row["status"] = "filled"           # delta 可為 0 (掃單翻回 pending 的列) → 仍要結案
                    row["terminal_ts"] = time.time()
                    resolved = True
                elif partial and auth_lots is not None and 0 < auth_lots < row["lots"]:
                    resolved = closed_partial = True
                if resolved or not can_verify:
                    self._cancel_queue.pop(order_no, None)
                    row["cancel_state"] = ""
                    row["cancel_err"] = (_FBC_ERR_PREFIX + text)[:160]
            final = resolved or not can_verify
            first = (final and not already and not closed_partial
                     and (row is not None or was_queued))
            if first:
                self._cancel_stats["filled_before_cancel"] += 1
        lots_txt = f" → 依券商權威補成交 {delta} 張" if delta > 0 else ""
        if closed_partial:
            # 部分成交、剩餘已取消 → 券商確認撤單路徑結案 (計「已確認」、釋放剩餘保留、清 st.order_no、
            # 隔日賣槽位);不另計 filled_before_cancel,免收盤摘要對同一筆雙算
            logger.warning(f"[session] {symbol} 撤單前部分成交 order={order_no} ({reason}): {text}"
                           f"{lots_txt};剩餘已由券商取消 → 結案")
            self._confirm_cancelled(order_no, symbol, reason,
                                    source=f"partial-fill {auth_lots}/{row['lots']}")
        elif not final:
            # 無法核實 → 留佇列 (不在佇列者入列),下一輪用新快照的 status 結案;計 attempts 讓告警升級
            self.request_cancel(order_no, symbol, reason)
            self._cancel_retry_later(order_no, time.time(),
                                     f"FILLED_UNVERIFIED (fq={filled_qty_shares}, "
                                     f"status={snapshot_status or '-'}): {text}")
            logger.warning(f"[session] {symbol} 撤單回已成交 order={order_no} ({reason}) 但快照無法核實 "
                           f"(fq={filled_qty_shares} status={snapshot_status or '-'}) → 留佇列下一輪核實: {text}")
        elif first:
            logger.warning(f"[session] {symbol} 撤單前已成交 order={order_no} ({reason}): {text}{lots_txt}")
        else:
            logger.info(f"[session] {symbol} 撤單前已成交 order={order_no} 重複 ({reason}) — 已結案,"
                        f"略過計數{lots_txt}")
        if late > 0:
            self._start_late_fill_exit(symbol or (row or {}).get("symbol", ""), late, order_no,
                                       "撤單前已成交")
        if breach_now:
            self._trigger_budget_breach("撤單前成交補入後")
        if first:
            self._fire_late_confirm()
        return final

    def _try_cancel_sync(self, order_no: str, symbol: str, reason: str,
                         enqueue: bool = True) -> str:
        """同步 broker.cancel 試一次 (過 45/s _rate)。回:
        'ok' / 'already_cancelled' / 'filled_before_cancel' (終端;**結案已在此處理** —
        broker.CancelRejected 帶撤單當下委託物件的 filled_qty/status → _settle_filled_before_cancel
        立刻補成交或留佇列核實,caller 不必再呼叫) /
        'queued' (查無/查詢失敗/非終端錯 → 已入佇列;enqueue=False 時回 'not_found'|'lookup'|'retry')。
        **任何非終端失敗都不標 cancelled、不動 st.order_no、不釋放預算。**"""
        self._rate.acquire()
        try:
            self.broker.cancel(order_no, symbol, reason=reason)
            return "ok"
        except Exception as e:     # noqa: BLE001 — 例外/回傳失敗都要接 (踩雷點 #8)
            kind = _exc_kind(e)
            msg = str(e)
            if kind == "other":
                cls = classify_cancel_error(msg)
                if cls == "already_cancelled":
                    return cls
                if cls == "filled_before_cancel":
                    # 舊式例外沒有 filled_qty → None,由 _settle_filled_before_cancel 決定留佇列核實或結案
                    self._settle_filled_before_cancel(
                        order_no, symbol, reason, getattr(e, "filled_qty", None), msg,
                        snapshot_status=str(getattr(e, "status", "") or ""))
                    return cls
                kind = "retry"
            self._last_cancel_err[order_no] = f"{kind.upper()}:{msg}"[:160]
            self._note_cancel_err(order_no, f"{kind.upper()}:{msg}")
            if not enqueue:
                return kind
            self.request_cancel(order_no, symbol, reason)
            return "queued"

    def _broker_sweep(self, reason: str, dry_run: bool, protect_active: bool,
                      fresh_after: Optional[float] = None) -> dict:
        """券商權威掃單原語 (A4): 一次 get_order_snapshot(),凡 user_def=="hitlimit" 且 Buy 且
        status ∈ {0,4,8,10} 且 (after_qty is None 或 after_qty>filled_qty) 的委託 = 券商端仍 live;
        不在本地 pending/佇列者 → CRITICAL「本地標 X 但券商仍 live」+ request_cancel
        (**不撤賣單、不撤 user_def 非 hitlimit**;dry_run 只 log)。
        protect_active=True (盤中): st.order_no/pre_order_no 合法在途的單略過不撤。
        fresh_after (broker query_clock 值;收盤掃單 = 本次 cancel_all_pending 開始時刻): 快照只用在此之後
        才開始的查詢 (broker single-flight 不共用更早在飛的舊查詢);broker 不支援 → 照舊。
        回 {"ok": bool, "live": [...], "queued": [...], "orphans": [...], "flagged": [...]}。"""
        out = {"ok": False, "live": [], "queued": [], "orphans": [], "flagged": []}
        snap_fn = getattr(self.broker, "get_order_snapshot", None) if self.broker else None
        if snap_fn is None:
            logger.info("[session] 券商權威掃單略過 (broker 無 get_order_snapshot)")
            return out
        try:
            snapshot = _call_with_timeout(_with_fresh_after(snap_fn, fresh_after),
                                          _CANCEL_SDK_TIMEOUT_SEC, "sweep-snapshot")
        except Exception as e:
            logger.error(f"[session] 券商權威掃單快照失敗 ({_exc_kind(e)}): {e}")
            return out
        out["ok"] = True
        with self._lock:
            local_pending = {no for no, r in self.order_log.items() if r["status"] == "pending"}
            queued = set(self._cancel_queue)
            active = set()
            for st in self.trades.values():
                if st.order_no:
                    active.add(st.order_no)
                if st.pre_order_no:
                    active.add(st.pre_order_no)
        for e in (snapshot or []):
            try:
                no = str(e.get("order_no", "") or "")
                if not no:
                    continue
                if str(e.get("user_def", "") or "") != _HITLIMIT_USER_DEF:
                    continue
                if str(e.get("buy_sell", "") or "") != "Buy":
                    continue
                if str(e.get("status", "") or "") not in _BROKER_LIVE_STATUSES:
                    continue
                filled = int(e.get("filled_qty") or 0)
                after = e.get("after_qty")
                if after is not None and int(after) <= filled:
                    continue
            except Exception:
                continue
            out["live"].append(no)
            if no in local_pending or no in queued:
                continue          # 本地知道它 live (pending) 或已在撤 → 由既有路徑處理
            if protect_active and no in active:
                continue          # 合法在途單 (row 狀態異常也不撤;盤中不動合法單)
            with self._lock:
                row = self.order_log.get(no)
                local = row["status"] if row is not None else "不存在"
                # 盤中對帳: 本地剛由 WS 回報終結 (<2 s) 而快照仍 live = 查詢 replica 落後 (事故實證 >0.5 s、
                # 非單調) → 本輪略過不發假 CRITICAL,60 s 後下一輪 replica 必已同步 (審查 #17)。
                # 收盤掃單 (protect_active=False) **不留寬限** — 13:23 只掃一次成功就不重跑,漏掉剛誤標的列
                # 就是 09-09 型裸單;重複撤已終結的單由快照 status 預分類冪等吸收 (30 → 直接確認,不送撤單)。
                recent_terminal = (protect_active and row is not None and local != "pending"
                                   and time.time() - float(row.get("terminal_ts") or 0)
                                   < _SWEEP_TERMINAL_GRACE_SEC)
            sym = str(e.get("symbol", "") or (row or {}).get("symbol", ""))
            if recent_terminal:
                logger.info(f"[session] 券商權威掃單: order={no} {sym} 本地 {local} 剛終結 (<{_SWEEP_TERMINAL_GRACE_SEC:g} s)"
                            f" 但快照仍 live — replica 未同步,本輪略過")
                continue
            out["flagged"].append(no)
            logger.critical(f"[session] ⚠⚠ 券商權威掃單: order={no} {sym} 本地標 {local} 但券商仍 live "
                            f"(status={e.get('status')} qty={e.get('quantity')} filled={filled} "
                            f"after={after}){' [dry-run 不撤]' if dry_run else ' → 入撤單佇列'}")
            if dry_run:
                continue
            with self._lock:
                if row is None:
                    # order_log 外孤兒 (重啟遺失/UNKNOWN 書號): 建列讓撤單/成交/UI 都看得到
                    qty = int(e.get("quantity") or 0)
                    self.order_log[no] = {
                        "order_no": no, "symbol": sym, "action": "buy", "kind": "orphan_buy",
                        "lots": max(1, qty // 1000) if qty else 0, "price": 0,
                        "status": "pending", "filled_lots": filled // 1000,
                        "ts": datetime.now().isoformat(timespec="seconds"), "last_time": "",
                        "cancel_state": "", "cancel_attempts": 0, "cancel_reason": "",
                        "cancel_err": "", "terminal_ts": 0.0,
                    }
                    out["orphans"].append(no)
                elif row["status"] != "pending":
                    # 本地誤標終結但券商 live → 券商為權威,翻回 pending 讓撤單/成交路徑接手
                    row["status"] = "pending"
                    row["cancel_err"] = f"local={local} but broker live"
            self.request_cancel(no, sym, reason)
            out["queued"].append(no)
        return out

    def intraday_reconcile_once(self, dry_run: bool = True) -> dict:
        """盤中低頻對帳 (A4b): 同 _broker_sweep 原語,合法在途單 (st.order_no/pre_order_no) 不撤;
        dry_run=True 只 CRITICAL 不撤 (首日預設)。worker 在 09:05~13:20 每 60 s 呼叫一次。"""
        if not self._can_manage():
            return {"ok": False, "live": [], "queued": [], "orphans": [], "flagged": []}
        return self._broker_sweep("intraday_sweep", dry_run=dry_run, protect_active=True)

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
            already_queued = order_no in self._cancel_queue
        # 真的有 pending 才檢查閘門 — 沒單就不必叫,也不會誤鳴 CRITICAL
        if not self._can_manage():
            if self.mode == "real":
                logger.critical(f"[session] ⚠ {symbol} 撤單請求但 broker 未連線 — "
                                f"委託可能仍掛在券商端,需人工處理 ({reason})")
            return
        if already_queued:
            # worker 正在處理 (退避中) → 只喚醒立刻重試,不再同步查一次 (免重複打 5/s 閘門;審查 #5/#10)
            with self._lock:
                st.stopped_reason = st.stopped_reason or reason
                st.last_buy_cancel_ts = time.time()
            self.request_cancel(order_no, symbol, reason)
            self._wake_cancel(order_no)
            logger.warning(f"[session] {symbol} 撤單 ({reason}) — order={order_no} 已在撤單佇列,喚醒 worker 重試")
            return
        # 同步試一次;成功/已撤 → 翻 st + 釋預算;已成交 → 只停進場 (部位由出場賣;結案在 _try_cancel_sync);
        # 查無/查詢失敗/非終端錯 → **row 保持 pending、st.order_no 不清、預算不釋放**,入佇列由 worker
        # 重試到券商確認 (2026-09-09 node3 事故: 「查無」曾被當已撤 → 4 筆裸單)
        res = self._try_cancel_sync(order_no, symbol, reason)
        if res in ("ok", "already_cancelled"):
            self._confirm_cancelled(order_no, symbol, reason, source=f"sync:{res}")
            logger.warning(f"[session] {symbol} 撤單 ({reason})")
        elif res == "filled_before_cancel":
            with self._lock:
                st.stopped_reason = st.stopped_reason or reason
        else:
            with self._lock:
                st.stopped_reason = st.stopped_reason or reason   # 停止進場意圖照記 (盲送據此停)
                st.last_buy_cancel_ts = time.time()               # 保守: 出場等在途成交窗口
            logger.error(f"[session] ⚠ {symbol} 撤單未確認 order={order_no} ({reason}) — "
                         f"已入撤單佇列重試,row 維持 pending: "
                         f"{self._last_cancel_err.get(order_no, '?')}")

    def exit_position(self, symbol: str, reason: str):
        """出場: **跌停價限價賣**出全部已成交 (限價預掛 pending 先撤;市價買單 pending
        視為已成交不撤,直接賣)。(背景 thread)
        閘門 = _can_manage (非 is_live) — 關 kill switch / WS 不健康時出場仍要能動。
        使用者「取消追蹤」(manual_abandon) 的檔 → 一切自動化停止,不出場 (自負)。
        出場失敗冷卻 (2026-09-14 node3/node4): 上一輪出場失敗後的冷卻期內直接返回 — trader 每 tick 都會
        呼叫,冷卻中不起 thread、每輪只記一次 INFO。冷卻不影響 _exit_worker 直呼 / 晚成交賣 / 緊急全平。"""
        cooldown_left = 0.0
        log_cooldown = False
        n_failed = 0
        with self._lock:
            st = self.trades.get(symbol)
            if st is not None and st.stopped_reason == "manual_abandon":
                return
            if st is not None and st.exit_cooldown_until > 0:
                cooldown_left = st.exit_cooldown_until - time.monotonic()
                if cooldown_left > 0:
                    log_cooldown = not st.exit_cooldown_logged
                    st.exit_cooldown_logged = True
                    n_failed = st.exit_fail_cycles
        if cooldown_left > 0:
            if log_cooldown:
                logger.info(f"[session] {symbol} 出場訊號 ({reason}) — 前 {n_failed} 輪出場失敗冷卻中,"
                            f"{cooldown_left:.0f}s 後才再觸發 (本輪冷卻只記這一次)")
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
        (緊急全平用)。成功回 True。失敗後單檔指數退避 (0.2→…→5s)。

        庫存不足拒因 (「可賣不足/超過庫存/庫存不足」;2026-09-14 node3/node4 策略外賣掉部分部位) →
        向券商嚴格查可賣張數 (_exit_sellable_lots,已扣隔日賣保留;禁現沖股一律未知):
          未知 (查詢失敗/逾時/broker 無此查詢/禁現沖) → 維持原重試;
          0 < 可賣 < lots → 改賣可賣張數立刻重送 (張數已變 → 至少再送一次);
          可賣 ≥ lots (查詢落後?) 或 可賣 0 但帳上仍有今日股 (禁現沖/當沖資格/被佔用/全被隔日保留扣光) → 維持原重試;
          無今日股 (可委託 0 且整股餘額扣隔日保留 ≤0) → 退避後再確認,**連續 _SELLABLE_ZERO_CONFIRM 次且跨
          ≥_SELLABLE_ZERO_CONFIRM_SEC 秒**皆如此 → 回 SELL_NOTHING_LEFT (不再送)。
        回 True (送出成功) / False (失敗) / SELL_NOTHING_LEFT (券商已無今日股;caller 停止出場此檔)。"""
        disp = st.is_disposition
        down = float(self.limit_downs.get(symbol) or 0)
        if down <= 0 and disp:
            logger.critical(f"[session] ⚠ {symbol} 處置股出場但查無跌停價 → 無法限價賣 "
                            f"(處置股不可市價),部位 {lots} 張需人工處理")
            return False
        tries = max_tries if max_tries is not None else DEFAULT_SELL_MAX_TRIES
        attempt = 0
        zero_reads = 0            # 連續「庫存不足拒單 + 券商確認無今日股」次數
        first_zero_ts = 0.0       # 本串連續確認的第一次時刻 (time.monotonic)
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
                with self._lock:
                    # 2026-09-15 審查: 只在這筆賣單**仍 pending** 時才解除失敗旗標/清冷卻 — 券商非同步退單回報若搶在
                    # _log_order 與這裡之間落地 (_settle_rejected 已回退 exited + 設冷卻),不可把冷卻清掉。
                    # exit_fail_cycles 不在送出時歸零 (送出後被退才能逐輪升級;成交/無今日股才歸零)
                    row = self.order_log.get(sell_no)
                    if row is None or row.get("status") == "pending":
                        st.sell_failed = False    # 賣單送出成功 → 解除先前失敗旗標
                        st.exit_cooldown_until = 0.0
                        st.exit_cooldown_logged = False
                return True
            except Exception as e:
                if _is_fatal_reject(e):
                    # 致命拒因 — 重試無意義,立即交人工 (caller 的 sell_failed 路徑接手)
                    logger.critical(f"[session] ⚠ {symbol} 出場賣單致命拒因 → 停止重試,"
                                    f"需人工處理: {e}")
                    return False
                logger.error(f"[session] {symbol} 出場賣單失敗 (第 {attempt}/{tries} 次): {e}")
                if _is_inventory_short_reject(e):
                    info = self._exit_sellable_lots(symbol, st)
                    sellable, gone = info if info is not None else (None, False)
                    if gone:
                        now_m = time.monotonic()
                        if zero_reads == 0:
                            first_zero_ts = now_m
                        zero_reads += 1
                        waited = now_m - first_zero_ts
                        if zero_reads >= _SELLABLE_ZERO_CONFIRM and waited >= _SELLABLE_ZERO_CONFIRM_SEC:
                            logger.warning(f"[session] {symbol} 出場賣單庫存不足且券商確認無今日股已連續 "
                                           f"{zero_reads} 次 (跨 {waited:.1f}s) → 不再送賣單 ({reason})")
                            return SELL_NOTHING_LEFT
                        logger.warning(f"[session] {symbol} 出場賣單庫存不足,券商無今日股 (第 {zero_reads} 次,"
                                       f"跨 {waited:.1f}s) → 退避後重送再確認 ({reason})")
                    elif sellable is not None and 0 < sellable < lots:
                        logger.warning(f"[session] ⚠ {symbol} 出場賣單庫存不足: 券商可賣 {sellable} 張 < 送出 "
                                       f"{lots} 張 (疑策略外已賣出部分) → 改賣 {sellable} 張立刻重送 ({reason})")
                        zero_reads = 0
                        lots = sellable
                        tries = max(tries, attempt + 1)   # 張數已改 → 至少再送一次 (lots 嚴格遞減,有界)
                        continue
                    else:
                        zero_reads = 0    # 未知 / 可賣足量 (查詢落後?) / 可賣 0 但帳上仍有今日股 → 維持原重試
                else:
                    zero_reads = 0
                time.sleep(min(self.order_min_interval * (2 ** (attempt - 1)), 5.0))  # 指數退避
        return False    # 重試用盡

    @staticmethod
    def _strict_lots(v, name: str) -> int:
        """券商查詢回傳的張數: 非負整數才收;bool / None / 無法轉換 / 負數 → raise (caller 視為未知)。"""
        if isinstance(v, bool) or v is None:
            raise TypeError(f"{name} 回傳型別異常: {v!r}")
        n = int(v)
        if n < 0:
            raise ValueError(f"{name} 回傳負值 {n}")
        return n

    def _exit_sellable_lots(self, symbol: str, st: Optional["SymbolTrade"] = None) -> Optional[tuple]:
        """出場賣單被拒「庫存不足」後向券商嚴格查可賣 (過帳務查詢 5/s 閘門),回 (sellable, gone) 或 None:
          sellable = 券商可委託張數 (tradable_qty) − 隔日賣保留 (_overnight_reserved_lots_locked)
          gone     = 券商**確定無今日股**: 可委託 **本身** 為 0 (非被隔日保留扣光) **且** 整股餘額 (today_qty)
                     − 隔日賣保留 (不扣在途賣單) ≤ 0。只有 gone 才可累計 nothing-left。
        None = 未知 → caller 維持原重試。未知的情況: 禁現沖股 (st.day_tradable is False — 今日買進今日本就不能賣,
        券商回同一句「超過庫存,或未符合當沖資格」,可委託 0 但帳上有股;整段重算跳過,維持舊行為)、
        broker 無嚴格查詢 [舊 fake / replay]、查詢失敗 / 逾時、回傳非整數或負數。
        broker 只有 get_sellable_lots (無整股餘額) → 可 resize,但 gone 恆 False (無從確認帳上沒股)。
        **查詢失敗絕不當 0** (同踩雷點 9「查無 ≠ 已成交」)。"""
        if st is not None and st.day_tradable is False:
            logger.warning(f"[session] {symbol} 禁現沖股出場賣單庫存不足 → 今日買進今日可能本就不可賣,"
                           f"不重算張數、不判定無可賣,維持原重試 (失敗則列入隔日賣)")
            return None
        broker = self.broker
        pos_fn = getattr(broker, "get_sellable_position", None) if broker is not None else None
        lots_fn = getattr(broker, "get_sellable_lots", None) if broker is not None else None
        if not callable(pos_fn) and not callable(lots_fn):
            return None
        balance = None
        try:
            if callable(pos_fn):
                raw = _call_with_timeout(lambda: pos_fn(symbol), _SELLABLE_QUERY_TIMEOUT_SEC,
                                         f"sellable-{symbol}")
                if not isinstance(raw, dict):
                    raise TypeError(f"可賣查詢回傳型別異常: {raw!r}")
                tradable = self._strict_lots(raw.get("tradable"), "可賣張數")
                balance = self._strict_lots(raw.get("balance"), "整股餘額")
            else:
                raw = _call_with_timeout(lambda: lots_fn(symbol), _SELLABLE_QUERY_TIMEOUT_SEC,
                                         f"sellable-{symbol}")
                tradable = self._strict_lots(raw, "可賣張數")
        except Exception as e:           # noqa: BLE001 — 任何失敗都是「未知」
            logger.error(f"[session] {symbol} 查券商可賣張數失敗 ({_exc_kind(e)}): {e} → 視為未知,維持原重試")
            return None
        with self._lock:
            reserve = self._overnight_reserved_lots_locked(symbol)
            reserve_bal = self._overnight_reserved_lots_locked(symbol, for_balance=True)
        sellable = max(0, tradable - reserve)
        gone = tradable == 0 and balance is not None and balance - reserve_bal <= 0
        if gone:
            verdict = "券商無今日股"
        elif sellable == 0:
            verdict = "帳上仍有股或可賣被隔日保留扣光 → 視為未知"
        else:
            verdict = f"今日出場可賣 {sellable} 張"
        bal_txt = f" / 整股餘額 {balance} 張" if balance is not None else ""
        logger.warning(f"[session] {symbol} 券商可委託 {tradable} 張{bal_txt}"
                       f"{f' (隔日賣保留 {reserve}/{reserve_bal} 張)' if (reserve or reserve_bal) else ''}"
                       f" → {verdict}")
        return sellable, gone

    def _overnight_reserved_lots_locked(self, symbol: str, for_balance: bool = False) -> int:
        """(caller 持鎖) 今日出場重算可賣張數時保留給隔日賣的昨日張數 =
        已對帳清單剩餘 (min(lots, lots_open) − sold_lots) − 隔日賣在途賣單未成交量。
        假設券商 tradable_qty 已扣掉在途賣單 → 已掛出的那部分不重複扣 (重複扣會誤判「已無可賣」)。
        for_balance=True (對整股餘額 today_qty 用): 在途賣單不減餘額 → 不扣在途量。
        lots_open = 第一次對帳的庫存張數 (2026-09-15 審查: 盤中重連 / add_overnight 會以當下整體庫存覆寫 lots,
        連今日策略買進都算進去 → 保留量灌大把今日張數吃掉;保留量不超過第一次對帳值)。
        未對帳 (reconciled False) 的張數來自昨日檔案、可能失真 (09-14 就把 0 張記成 8 張) → 不保留。
        skip (使用者暫停賣) 照樣保留 — 那是使用者要續抱的昨日部位。"""
        o = self.overnight.get(symbol)
        if o is None or not o.get("reconciled"):
            return 0
        base = int(o.get("lots") or 0)
        if o.get("lots_open") is not None:
            base = min(base, int(o.get("lots_open") or 0))
        remaining = max(0, base - int(o.get("sold_lots") or 0))
        if for_balance:
            return remaining
        no = o.get("sell_order_no") or ""
        row = self.order_log.get(no) if no else None
        if row is not None and row.get("status") == "pending":
            remaining -= max(0, int(row.get("lots") or 0) - int(row.get("filled_lots") or 0))
        return max(0, remaining)

    def _note_exit_failed_locked(self, st: "SymbolTrade") -> float:
        """(caller 持鎖) 記一輪出場失敗 → exit_fail_cycles+1、設冷卻 (30→60→120→300 s 封頂);回冷卻秒數。"""
        st.exit_fail_cycles += 1
        cd = _EXIT_COOLDOWN_SEC[min(st.exit_fail_cycles, len(_EXIT_COOLDOWN_SEC)) - 1]
        st.exit_cooldown_until = time.monotonic() + cd
        st.exit_cooldown_logged = False
        return cd

    @staticmethod
    def _reset_exit_backoff_locked(st: "SymbolTrade"):
        """(caller 持鎖) 出場失敗輪次/冷卻歸零 (策略出場賣單成交 / 券商確認無今日股)。"""
        st.exit_fail_cycles = 0
        st.exit_cooldown_until = 0.0
        st.exit_cooldown_logged = False

    def _exit_nothing_left(self, symbol: str, st: "SymbolTrade", lots: int, reason: str):
        """出場賣單庫存不足且券商確認無今日股 (SELL_NOTHING_LEFT) → 停止出場此檔: exited 不回退 (caller 保持 True)、
        清 sell_failed、冷卻/輪次歸零、CRITICAL 一次。st.filled_lots 不調整 (策略帳 ≠ 券商實況,交人工核對)。
        exit_no_sellable=True → **仍列入 13:24 隔日賣檔** (2026-09-15 審查: 判定錯誤時不可讓帳上部位從隔日賣消失;
        券商真 0 張 → 隔早 refresh_overnight_inventory「庫存沒這檔 → 移除」)。"""
        with self._lock:
            st.sell_failed = False
            st.exit_no_sellable = True
            self._reset_exit_backoff_locked(st)
            held = st.filled_lots
        logger.critical(f"[session] ⚠⚠ {symbol} 出場賣 {lots} 張被拒「庫存不足」,券商可委託 0 張且整股餘額無今日股"
                        f" (見上一行明細) — 應已在策略外賣出;停止自動出場此檔,策略帳上 {held} 張未調整"
                        f" (仍列入隔日賣檔,隔早以券商庫存對帳),請人工核對券商庫存 ({reason})")

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
            st.exit_in_progress = True  # 窗口內的晚成交由本 worker 接手 (讀 filled_lots 時一併賣)
            st.stopped_reason = st.stopped_reason or reason
            had_pending = bool(st.order_no) and st.order_status == "pending"
            # 別處剛撤過 live 買單 (如硬上限 breach 的 cancel_all_pending) → 撤單的在途成交仍可能
            # 晚到;此時 order_no 已被清、had_pending/had_orphan 皆 False,不等窗口會漏賣 (審查 #3)。
            recently_cancelled = (time.time() - st.last_buy_cancel_ts) < _EXIT_FILL_WAIT_SEC
        # 出場也撤孤兒預掛單 P (2026-08-25 HIGH-1): 賣出時不該讓 P 繼續買進;
        # 有撤到 live P → 也要等窗口 (P 可能正在成交,回報晚到,不等會漏賣變隱形部位)
        had_orphan = self._cancel_orphan_pre(symbol, reason)
        had_stray = self._cancel_stray_buys(symbol, reason) > 0   # 管線多送的額外市價買也撤 (2026-08-28)
        if had_pending or had_orphan or had_stray or recently_cancelled:
            if had_pending:
                self.cancel_symbol_orders(symbol, reason)     # 能撤就撤 (已成交→券商拒,無妨)
            # 撤單前已成交/正在成交的 → **固定等滿窗口讓在途成交回報全部落地**再賣。
            # 不提早 break: overbuy 時 (預掛+市價都成交) filled 分兩批落地,任何提早收斂
            # 判斷都可能漏掉第二批 → 又變成 exited 鎖住漏賣。回報比 tick 慢僅百 ms 級,
            # 窗口 (_EXIT_FILL_WAIT_SEC) 綽綽有餘。只有「出場當下還有 pending 單」才等
            # (一般出場時進場單早已 done → had_pending False → 不等,快)。
            _t.sleep(_EXIT_FILL_WAIT_SEC)
        with self._lock:
            held = st.filled_lots
            # 只賣**尚未被 live 賣單覆蓋**的張數 — 出場後晚成交的 late-fill 賣單已掛著時,主賣失敗回退
            # 再觸發不可把那批再賣一次 (審查 #15: 超賣 → 現股被拒 / 可現沖帳號裸空)
            lots = self._uncovered_lots_locked(symbol, st)
            # 讀完要賣的張數即結束「進行中」— 之後 (exited 仍 True) 才落地的買進成交 = 出場後晚成交,
            # 由 _on_fill 另起 late-fill 賣 delta (A4b;3587 同型漏洞)。與這裡讀值同一鎖內切換,無縫。
            st.exit_in_progress = False
        if lots > 0:
            res = self._sell_position(symbol, st, lots, reason)
            if res == SELL_NOTHING_LEFT:
                # 券商已無可賣 → exited 維持 True (has_exposure 擋掉後續 tick,不再出場此檔)
                self._exit_nothing_left(symbol, st, lots, reason)
            elif not res:
                # broker 未連線/不健康造成的失敗 = 賣單根本沒打到券商,不是拒單風暴 → 不計輪次、不冷卻
                # (2026-09-15 審查: 否則重連後真實部位還要等冷卻才出場)
                broker_down = not self._broker_ready()
                with self._lock:
                    if st.stopped_reason != "manual_abandon":   # 取消追蹤的檔不回退
                        st.exited = False   # 未賣成 → 冷卻過後下一個出場訊號 tick 可再觸發 (行情節奏)
                    st.sell_failed = True   # 前端顯示「需人工」
                    # 出場失敗冷卻 (2026-09-14): 冷卻由 exit_position 執行 — 直呼 _exit_worker 不受限
                    cd = 0.0 if broker_down else self._note_exit_failed_locked(st)
                    n_failed = st.exit_fail_cycles
                tail = (f"broker 未連線/不健康 → 不計出場失敗輪次、不冷卻" if broker_down
                        else f"第 {n_failed} 輪失敗 → {cd:g}s 內不再自動觸發出場")
                logger.critical(f"[session] ⚠⚠ {symbol} 出場賣單連續失敗,部位 {lots} 張仍在 — "
                                f"需人工處理 ({reason});{tail}")
        elif held > 0:
            # 持倉已全數被在途賣單覆蓋 (late-fill 賣單掛著) → 不重複賣;exited 維持,賣單成交後歸零
            logger.warning(f"[session] {symbol} 出場: 持倉 {held} 張已由在途賣單全數覆蓋 → 不重複賣 ({reason})")
        else:
            # 等滿窗口仍無部位 → exited 回退 — 否則回報更晚落地時,部位會永遠
            # 沒有出場保護 (has_exposure 被 exited=True 擋死;2026-08-05 3587 教訓)。
            # 取消追蹤 (manual_abandon) 的檔不回退 — 自動化已由使用者關閉。
            with self._lock:
                if st.stopped_reason != "manual_abandon":
                    st.exited = False

    def _late_fill_exit_worker(self, symbol: str, lots: int, reason: str):
        """出場後晚成交 (A4b): 對**這批** lots 張再賣一次 (跌停價限價賣;冪等 — 每筆成交回報去重後
        只觸發一次,賣的是該筆的張數,不讀 st.filled_lots 免與先前出場賣單的成交回報競態超賣)。
        exit_position 對 exited=True 會早退,故獨立 worker。"""
        with self._lock:
            st = self.trades.get(symbol)
            if st is None or lots <= 0:
                return
        if not self._can_manage():
            if self.mode == "real":
                logger.critical(f"[session] ⚠ {symbol} 出場後晚成交 {lots} 張但 broker 未連線 — "
                                f"部位無法賣出,需人工處理 ({reason})")
            return
        res = self._sell_position(symbol, st, lots, reason)
        if res == SELL_NOTHING_LEFT:
            self._exit_nothing_left(symbol, st, lots, reason)
        elif not res:
            with self._lock:
                st.sell_failed = True
            logger.critical(f"[session] ⚠⚠ {symbol} 出場後晚成交 {lots} 張賣單連續失敗 — "
                            f"部位仍在,需人工處理 ({reason})")

    def _uncovered_lots_locked(self, symbol: str, st: "SymbolTrade") -> int:
        """持倉中**尚未被 live 賣單覆蓋**的張數 (caller 持鎖) = filled_lots − Σ(該檔 pending 賣單剩餘量;
        隔日賣單不算,那賣的是昨日部位)。出場 / 緊急全平只賣這些 — 免「出場後晚成交再賣」與「主賣失敗
        回退再觸發」對同一批部位各賣一次 (審查 #15: 超賣 → 現股被拒或裸空)。賣單成交時 filled_lots 與
        row 剩餘量同步遞減;賣單被撤/拒 → 不再 pending → 覆蓋量自動回吐。"""
        covered = 0
        for r in self.order_log.values():
            if (r.get("symbol") == symbol and r.get("action") == "sell"
                    and r.get("status") == "pending" and r.get("kind") != "overnight_sell"):
                covered += max(0, int(r.get("lots") or 0) - int(r.get("filled_lots") or 0))
        return max(0, st.filled_lots - covered)

    def _has_orphan_pre(self, st: "SymbolTrade") -> bool:
        """該檔有「被市價盲送蓋掉、仍 pending 的孤兒預掛單 P」(caller 持鎖)。"""
        p = st.pre_order_no
        if not p or p == st.order_no:
            return False
        row = self.order_log.get(p)
        return row is not None and row["status"] == "pending"

    def _cancel_orphan_pre(self, symbol: str, reason: str) -> bool:
        """撤掉孤兒預掛單 P (2026-08-25 審查 HIGH-1): 市價盲送成功後 st.order_no 從 P 蓋成 M,
        P 只剩在 order_log → 出場/13:23 一併撤,免留到收盤或出場後晚成交變隱形部位。
        回 True = 有撤到 live 的 P (出場端據此判斷要不要等成交回報窗口接 P 的在途成交)。
        預算不動 (超買帳務本為近似,見 _sized/_on_fill 註)。"""
        with self._lock:
            st = self.trades.get(symbol)
            if st is None or not self._has_orphan_pre(st):
                return False
            p = st.pre_order_no
            already_queued = p in self._cancel_queue
        if not self._can_manage():
            return False
        if already_queued:
            res = "queued"                       # worker 正在處理 → 只喚醒,不重複查詢 (審查 #5/#10)
            self.request_cancel(p, symbol, reason)
            self._wake_cancel(p)
        else:
            res = self._try_cancel_sync(p, symbol, reason)
        with self._lock:
            st2 = self.trades.get(symbol)
            if st2 is not None:
                st2.last_buy_cancel_ts = time.time()   # 撤了/正在撤 live P → 出場要等在途成交 (審查 #3)
        if res in ("ok", "already_cancelled"):
            self._confirm_cancelled(p, symbol, reason, source=f"sync:{res}")
            logger.info(f"[session] {symbol} 撤孤兒預掛單 P={p} ({reason})")
        elif res == "filled_before_cancel":
            logger.warning(f"[session] {symbol} 撤孤兒預掛單 P={p}: 已成交 (結案由 _try_cancel_sync 處理)")
        else:
            logger.error(f"[session] {symbol} 撤孤兒預掛單 P={p} 未確認 → 已入撤單佇列重試 "
                         f"(row 維持 pending): {self._last_cancel_err.get(p, '?')}")
        return True    # 撤了/撤失敗 = P 可能已成交/仍 live → 仍要等窗口接晚成交

    def _cancel_stray_buys(self, symbol: str, reason: str) -> int:
        """撤該檔 order_log 裡**還 pending、且不在 st.order_no/pre_order_no** 的買單 = 管線盲送的
        額外市價買 (2026-08-28 管線化審查)。這些額外 M 只存在 order_log,不在任何 st 欄位;若不由掃單
        權威地從 order_log 撤,一旦它自撤失敗就變成收盤/出場/硬上限都撤不到的隱形裸買單。回撤單筆數。
        2026-09-10 審查 #5/#10: 不再逐筆 broker.cancel (每筆各打一次 5/s 查詢閘門,N 筆串行 ≥ N×0.21 s
        全擋在出場賣單前) — 已在撤單佇列者 (chase_extra 剛入列 / worker 退避中) 只喚醒;其餘入列後
        **同步跑一輪 worker** (一次快照 + cancel_by_obj 扇出) = 計畫 A2「同步用 snapshot+cancel_by_obj 試一次」
        的多筆版。回傳語意不變 (筆數;last_buy_cancel_ts 照記)。"""
        with self._lock:
            st = self.trades.get(symbol)
            covered = {st.order_no, st.pre_order_no} if st is not None else set()
            strays = [no for no, row in self.order_log.items()
                      if row.get("symbol") == symbol and row.get("action") == "buy"
                      and row.get("status") == "pending" and no not in covered]
            queued = [no for no in strays if no in self._cancel_queue]
        if not strays or not self._can_manage():
            return 0
        for no in queued:
            self._wake_cancel(no)            # worker 正在處理 → 只喚醒立刻重試,不重複查詢
        if queued:
            logger.warning(f"[session] {symbol} 孤兒市價買 {len(queued)} 筆已在撤單佇列 → 喚醒 worker ({reason})")
        fresh = [no for no in strays if no not in queued]
        if fresh:
            for no in fresh:
                self.request_cancel(no, symbol, reason)
            out = self._cancel_worker_run_once()      # 同步試一次: 一次快照 + 扇出 (非逐筆查)
            for no in fresh:
                if no in out["cancelled"]:
                    logger.info(f"[session] {symbol} 撤孤兒市價買 M={no} ({reason})")
                elif no in out["filled_before_cancel"]:
                    logger.warning(f"[session] {symbol} 撤孤兒市價買 M={no}: 已成交 (=超買,靠出場全量賣)")
                elif no in out.get("rejected", []):
                    logger.warning(f"[session] {symbol} 撤孤兒市價買 M={no}: 券商端已失敗 (拒單)")
                else:
                    with self._lock:
                        err = (self.order_log.get(no) or {}).get("cancel_err") or "?"
                    logger.warning(f"[session] {symbol} 撤孤兒市價買 M={no} 未確認 → 留撤單佇列重試 "
                                   f"(row 維持 pending): {err}")
        with self._lock:                     # 撤了 live 買單 → 出場等在途成交回報 (審查 #3,2026-08-28)
            st = self.trades.get(symbol)
            if st is not None:
                st.last_buy_cancel_ts = time.time()
        return len(strays)

    def _orders_csv_path(self) -> Path:
        """broker 落的當日委託台帳路徑 (連線時 runner 給的 output_dir;未連線退回 repo 的 output/)。"""
        base = self._output_dir or (Path(__file__).parent / "output")
        return Path(base) / f"{datetime.now().strftime('%Y-%m-%d')}_orders.csv"

    def _todays_orders_csv_rows(self) -> int:
        """當日 orders.csv 的資料列數 (不含表頭;檔不存在/讀不到 → 0)。process 重啟後 order_log 是空的,
        但 broker 落的 CSV 還在 — 收盤撤單以此判斷「今天有沒有下過單」(A4a;審查 #16)。"""
        try:
            p = self._orders_csv_path()
            if not p.exists():
                return 0
            with p.open(encoding="utf-8") as fh:
                return max(0, sum(1 for line in fh if line.strip()) - 1)
        except Exception as e:
            logger.warning(f"[session] 讀當日 orders.csv 失敗: {e}")
            return 0

    def cancel_all_pending(self, reason: str):
        """撤所有 pending,不賣持倉 (留倉)。13:23 (CANCEL_PENDING_TIME) 主跑,13:24 收盤保險再跑。
        閘門 = _can_manage (非 is_live) — 關 kill switch 也必須能撤 13:23 的單。
        含: st.order_no (M/P) + 孤兒預掛 P (2026-08-25 HIGH-1) + **管線盲送的額外市價買** (只在
        order_log、不在 st 欄位;2026-08-28 管線化審查 — 免額外 M 撤不到變隔夜隱形裸單)。
        2026-09-09 A4: (1) 本地 pending 買單全部 request_cancel;(2) **券商權威掃單** (快照中 hitlimit
        買單仍 live、本地卻不 pending/不存在者 → CRITICAL + 入佇列);(3) 同步 drain 佇列 ≤40 s;
        (4) 摘要「已確認 X / 未確認 Y / order_log 外孤兒 Z」+ 未確認逐筆 CRITICAL;
        (5) 非 real / 無 broker 但當日 order_log 非空 → CRITICAL 而非靜默。
        2026-09-15 (node1 委託洪水): 多次呼叫**序列化** (budget_breached thread / 13:23 timer / 13:24 收盤) —
        後到者等前一次跑完 (≤_CANCEL_ALL_WAIT_SEC,逾時仍照跑 + CRITICAL),各自記自己的摘要。"""
        lock = self._cancel_all_lock
        got = lock.acquire(blocking=False)
        if not got:
            t_wait = time.monotonic()
            logger.warning(f"[session] cancel_all_pending ({reason}) 等待前一次撤單全清跑完 "
                           f"(≤{_CANCEL_ALL_WAIT_SEC:g}s,不並行)")
            got = lock.acquire(timeout=_CANCEL_ALL_WAIT_SEC)
            if got:
                logger.info(f"[session] cancel_all_pending ({reason}) 前一次已結束 (等 "
                            f"{time.monotonic() - t_wait:.1f}s) → 開始")
            else:
                logger.critical(f"[session] ⚠ cancel_all_pending ({reason}) 前一次撤單全清超過 "
                                f"{_CANCEL_ALL_WAIT_SEC:g}s 仍未結束 → 本次仍照跑 (可能並行)")
        try:
            self._cancel_all_pending_run(reason)
        finally:
            if got:
                lock.release()

    def _cancel_all_pending_run(self, reason: str):
        """cancel_all_pending 本體 (caller 已序列化)。"""
        fresh_mark = self._broker_query_mark()   # 本次開始時刻 — 收盤掃單只用此後才開始的委託查詢
        with self._lock:
            pending_buys = [(no, row["symbol"]) for no, row in self.order_log.items()
                            if row.get("action") == "buy" and row.get("status") == "pending"]
            # st.order_no pending 但 row 不在 order_log 的 (理論上不會;保險)
            for s, st in self.trades.items():
                if st.order_no and st.order_status == "pending" and st.order_no not in self.order_log:
                    pending_buys.append((st.order_no, s))
            n_log = len(self.order_log)
            queued_before = len(self._cancel_queue)
        if self.mode != "real" or self.broker is None:
            # A4(a): process 重啟後 order_log 是空的、mode 重置 sim、broker None,但券商端可能還掛著今天
            # 下過的單 — 以 broker 落的當日 orders.csv 為「今天有沒有下過單」的第二證據 (審查 #16)
            csv_rows = self._todays_orders_csv_rows()
            if n_log or csv_rows:
                logger.critical(f"[session] ⚠⚠ 收盤撤單 ({reason}) 但 mode={self.mode} / broker="
                                f"{'無' if self.broker is None else '有'} — 當日 order_log {n_log} 筆"
                                f" (pending 買單 {len(pending_buys)}) / 當日 orders.csv {csv_rows} 列"
                                f"{' (重啟後 order_log 空但當日已下過單)' if not n_log else ''}"
                                f" 無法撤,券商端可能仍 live,需人工到券商端核對")
            return
        if not self._can_manage():
            logger.critical(f"[session] ⚠ 13:23/收盤撤單但 broker 未連線 — {len(pending_buys)} 筆 pending "
                            f"買單可能仍掛券商端,需人工處理")
            return
        # (1) 本地 pending 買單全部入佇列 (含 st.order_no M/P、孤兒 P、管線多送額外 M)
        for no, sym in pending_buys:
            self.request_cancel(no, sym, reason)
        # (2) 券商權威掃單 — 不看 order_log 標成什麼;賣單/非 hitlimit 不碰
        sweep = self._broker_sweep(reason, dry_run=False, protect_active=False, fresh_after=fresh_mark)
        # (3) 同步 drain (只在 timer/背景 thread 內同步跑;絕不在 SDK callback / worker thread 內叫)
        #     掃單失敗 (流量控管/斷線) → drain 期間每 _CLOSE_SWEEP_RETRY_SEC 重跑到成功或逾時 (審查 #19:
        #     原本只跑一次、失敗靜默,摘要看起來像乾淨);broker 不健康 / 快照失敗的輪次至少睡 0.5 s
        #     (審查 #13: 零延遲忙迴圈 40 s 內 48 萬次搶鎖)
        #     2026-09-15: 重試間隔從「掃單**返回後**」起算,且同一輪照樣 drain 佇列 — 舊版在掃單前記時戳 +
        #     continue,掃單逾時 5 s (> 2 s 間隔) 就立刻再掃,佇列整個 drain 期間都不被處理
        t0 = time.time()
        stats0 = dict(self._cancel_stats)
        last_sweep_ts = t0
        sweep_tries = 1
        while True:
            now = time.time()
            if now - t0 > _CANCEL_DRAIN_MAX_SEC:
                break
            if not sweep["ok"] and now - last_sweep_ts >= _CLOSE_SWEEP_RETRY_SEC:
                sweep_tries += 1
                retry = self._broker_sweep(reason, dry_run=False, protect_active=False,
                                           fresh_after=fresh_mark)
                last_sweep_ts = time.time()          # 掃單返回後才起算下一次重試
                if retry["ok"]:
                    sweep = retry
                    logger.warning(f"[session] 收盤券商權威掃單第 {sweep_tries} 次重試成功 "
                                   f"(券商 live {len(sweep['live'])} / 孤兒 {len(sweep['orphans'])})")
                now = time.time()                    # 不 continue — 同一輪照樣 drain 佇列
            with self._lock:
                nxt = min((it["next_ts"] for it in self._cancel_queue.values()), default=None)
            if nxt is None:
                if sweep["ok"]:
                    break
                time.sleep(0.5)                # 佇列空、掃單未成 → 等下一次掃單重試
                continue
            wait = nxt - now
            if wait > 0:
                time.sleep(min(wait, 0.5))
            out = self._cancel_worker_run_once()
            if out.get("skipped") or not out.get("snapshot_ok", True):
                time.sleep(0.5)
        # (4) 摘要
        with self._lock:
            unconfirmed = [(no, dict(it)) for no, it in self._cancel_queue.items()]
            confirmed = ((self._cancel_stats["confirmed"] - stats0["confirmed"])
                         + (self._cancel_stats["filled_before_cancel"] - stats0["filled_before_cancel"]))
            given_up = self._cancel_stats["given_up"] - stats0["given_up"]
        logger.warning(f"[session] 收盤撤單 ({reason}): 已確認 {confirmed} / 未確認 {len(unconfirmed)} / "
                       f"order_log 外孤兒 {len(sweep['orphans'])} (本地 pending 買單 {len(pending_buys)}, "
                       f"佇列原有 {queued_before}, 券商 live {len(sweep['live'])}, 放棄 {given_up}, "
                       f"掃單 {'ok' if sweep['ok'] else '失敗'}/{sweep_tries} 次) — 持倉保留")
        if not sweep["ok"]:
            logger.critical(f"[session] ⚠⚠ 收盤撤單 ({reason}): 券商權威掃單 {sweep_tries} 次皆失敗 — "
                            f"order_log 外孤兒**未檢查**,券商端可能仍有 live hitlimit 買單,請人工到券商端核對")
        for no, it in unconfirmed:
            logger.critical(f"[session] ⚠⚠ 收盤撤單未確認 order={no} {it['symbol']} ({it['reason']}) "
                            f"{it['attempts']} 次 — 券商端可能仍 live,worker 續試,請人工核對")

    def close_all(self):
        """緊急全平: 撤全部 pending + 市價賣出全部持倉。"""
        if not self._broker_ready():
            raise RuntimeError("券商未連線")
        with self._lock:
            # 略過已在出場中的檔 (exited=True) — 否則與自動出場並行會把同一批
            # filled_lots 賣第二次超賣 (2026-08-24 審查 LOW)
            snapshot = [(s, self._uncovered_lots_locked(s, st)) for s, st in self.trades.items()
                        if not st.exited]          # 扣掉在途賣單已覆蓋的張數 (免超賣;審查 #15)
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
                res = self._sell_position(sym, st, lots, "close_all", max_tries=3)
                if res == SELL_NOTHING_LEFT:
                    self._exit_nothing_left(sym, st, lots, "close_all")   # exited 維持 True (券商已無可賣)
                elif res:
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
                # exit_no_sellable: 出場判定「券商無今日股」而停止的檔照樣帶 (判定錯誤時不可讓帳上部位從隔日賣消失;
                # 真 0 張由隔早 refresh_overnight_inventory 以庫存清掉)
                if st.filled_lots > 0 and (not st.exited or st.exit_no_sellable):
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
        """券商連線後以庫存為準對帳: 有庫存→用庫存張數;無庫存 (已賣/沒了)→移除。
        2026-09-09 A4b: **庫存有、清單沒有**的現股多單 → CRITICAL 點名 (收盤後晚確認的成交/
        回報遺失沒寫進 overnight_holdings.json 的訊號;不自動加入,由使用者核對後手動加入)。
        清單為空也照查 — 空清單正是「檔案漏掉」最需要對帳的情況 (一次帳務查詢,成本可忽略)。"""
        if not self._broker_ready():
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
                    # 第一次對帳值 (今日出場重算時隔日賣保留量的上限;盤中再對帳會把今日買進也算進 lots)
                    o.setdefault("lots_open", inv[sym]["lots"])
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
            # 今日策略自己買到的部位 (trades 有成交) 不算「清單沒有」— 盤中重連 / 手動 add_overnight 也會
            # 呼叫這裡,對每檔今日持股誤發 CRITICAL 會稀釋真正的隔日賣遺漏訊號 (審查 #18)
            missing = [(sym, int(r.get("lots") or 0)) for sym, r in inv.items()
                       if sym not in self.overnight and int(r.get("lots") or 0) > 0
                       and not (sym in self.trades and self.trades[sym].filled_lots > 0)]
            n_over = len(self.overnight)
        if missing:
            logger.critical(f"[session] ⚠⚠ 券商庫存有但隔日賣清單沒有: "
                            f"{', '.join(f'{s} {n} 張' for s, n in missing)} — "
                            f"可能是收盤後晚確認的成交/回報遺失未寫進清單 (或非策略持股);"
                            f"請人工核對,需要隔日賣請手動加入")
        if n_over or missing:
            logger.warning(f"[session] 隔日賣對帳完成: {n_over} 檔實有庫存")

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
            # 2026-09-10 node4 事故: 漲停價若是昨日殘值 (283 vs 今日 311),委買一 ≥ 283 恆判「鎖著」
            # → 漲停打開也永不賣。盤面不變量: 今日任何委買/委賣價都不可能高於今日漲停價,
            # 出現就代表漲停價是殘值 → 視為未知 (只看市價列判鎖),CRITICAL 一次。
            suspect_msg = ""
            if limit_up > 0 and (limit_bid1_price > limit_up + 0.001 or ask1 > limit_up + 0.001):
                if not o.get("limit_up_suspect"):
                    o["limit_up_suspect"] = True
                    suspect_msg = (f"[session] ⚠ 隔日賣 {symbol} 漲停價 {limit_up} 低於盤面 "
                                   f"(委買一 {limit_bid1_price} / 委賣一 {ask1}) — 疑昨日殘值,"
                                   f"鎖漲停判斷改只看市價列")
                limit_up = 0.0
            locked = (mkt_bid_size > 0
                      or (limit_up > 0 and limit_bid1_price >= limit_up - 0.001))
            o["locked_now"] = locked
            if not locked and not o["sell_placed"]:
                st = self.trades.get(symbol)
                active_today = (st is not None and not st.stopped_reason
                                and not st.exited)
                if not active_today:
                    trigger = True
        if suspect_msg:
            logger.critical(suspect_msg)
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
        res = ""
        if pending_no:
            if not self._can_manage():
                logger.critical(f"[session] ⚠ 隔日賣 {symbol} 撤賣單但 broker 未連線 — "
                                f"賣單 {pending_no} 可能仍掛券商端,需人工處理")
            else:
                # 同步試一次;成功/已撤 → 解除 sell_placed (取消 skip 時再賣);已成交 → 維持 (已賣掉);
                # 未確認 → 入佇列,sell_placed 等券商確認後由 _close_cancel 解除 (免確認前重複掛賣)
                res = self._try_cancel_sync(pending_no, symbol, "overnight_skip")
                if res in ("ok", "already_cancelled"):
                    self._confirm_cancelled(pending_no, symbol, "overnight_skip", source=f"sync:{res}")
                elif res == "filled_before_cancel":
                    logger.warning(f"[session] 隔日賣 {symbol} 賣單 {pending_no} 已成交,不可撤 "
                                   f"(結案由 _try_cancel_sync 處理)")
                else:
                    logger.error(f"[session] 隔日賣 {symbol} 撤賣單未確認 → 已入撤單佇列重試: "
                                 f"{self._last_cancel_err.get(pending_no, '?')}")
        logger.warning(f"[session] 隔日賣 {symbol} skip={skip}"
                       f"{' (已撤賣單)' if res in ('ok', 'already_cancelled') else ''}"
                       f"{' (撤賣單待確認)' if res == 'queued' else ''}")

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
        # 2026-09-10: 跌停價若是昨日殘值,掛出去會超出今日漲跌幅被交易所退。盤面不變量:
        # 今日跌停價 ≤ 委買一,且 委買一 ≤ 漲停 = 跌停×1.1/0.9 → 跌停價 ≥ 委買一×0.818;
        # 超出區間 (取 0.80 留 margin) 即視為殘值 → 退回委買一價公式。
        if price > 0 and bid1 > 0 and (price > bid1 + 0.001 or price < bid1 * 0.80):
            logger.critical(f"[session] ⚠ 隔日賣 {symbol} 跌停價 {price} 與委買一 {bid1} 不相容 "
                            f"(疑昨日殘值) → 改用委買一價公式")
            price = 0.0
        if price <= 0:
            price = ticks.overnight_sell_price(bid1, ask1)
            if price > 0:
                logger.warning(f"[session] 隔日賣 {symbol} 查無/不採用跌停價 → 退回委買一價公式 {price}")
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
                    # 2026-09-10: 讓 UI 看得到續抱判斷用的漲停價 (昨日殘值事故的可見度)
                    "limit_up": self.overnight_limit_ups.get(sym),
                    "limit_up_suspect": bool(o.get("limit_up_suspect", False)),
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
        # 撤單佇列: 斷線期間退避中的項全部喚醒 (worker 下一輪先做一次快照對帳)
        with self._lock:
            now = time.time()
            for it in self._cancel_queue.values():
                it["next_ts"] = min(it["next_ts"], now)
            # 出場失敗冷卻清掉 (輪次保留): 斷線期間的失敗多半是賣單沒送到券商,重連後部位要立刻受出場保護
            cleared = []
            for sym, st in self.trades.items():
                if not st.exited and st.filled_lots > 0 and st.exit_cooldown_until > 0:
                    st.exit_cooldown_until = 0.0
                    st.exit_cooldown_logged = False
                    cleared.append(sym)
        if cleared:
            logger.warning(f"[session] 重連 → 清除出場失敗冷卻 {cleared} (下一個出場訊號立刻可再觸發)")
        self._cancel_wakeup.set()
        self._ensure_cancel_worker()
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
        # 重連後的補收只用「此刻之後才開始」的委託查詢 (broker single-flight 不共用重連前就在飛的舊查詢)
        fresh_mark = self._broker_query_mark()
        self._query_gate()
        try:
            auth_map = _with_fresh_after(self.broker.get_filled_map, fresh_mark)()
        except Exception as e:
            logger.error(f"[session] 補收查詢失敗: {e}")
            return
        recovered = 0
        breach_now = False
        late_sells = []
        with self._lock:
            was_breached = self._budget_breached
            for order_no, row in self.order_log.items():
                auth = auth_map.get(order_no)
                if auth is None:
                    continue
                # 逐 row 覆寫邏輯抽成 _apply_auth_fill_locked (撤單回「成交單已不允許取消」時共用)
                delta = self._apply_auth_fill_locked(order_no, row, auth)
                if delta <= 0:
                    continue
                recovered += delta
                late = self._late_fill_lots_locked(row, delta)   # 已出場的檔補進買進 → 沒有出場保護 (審查 #14)
                if late > 0:
                    late_sells.append((row["symbol"], late, order_no))
                logger.critical(f"[session] ⚠ 補收 {row['symbol']} {row['action']} "
                                f"{delta} 張 (order {order_no},斷線期間回報遺失)")
            breach_now = self._budget_breached and not was_breached
        for sym, lots, no in late_sells:
            self._start_late_fill_exit(sym, lots, no, "補收對帳")
        if recovered:
            logger.critical(f"[session] 補收對帳完成 — 共補 {recovered} 張")
        else:
            logger.warning("[session] 補收對帳完成 — 無差異")
        # 補收後實際買進累計若已超總預算 → 觸發硬上限煞車 (撤所有 pending 買單)
        if breach_now:
            logger.critical(
                f"[session] ⚠⚠ 補收對帳後發現總曝險超上限 — 實際買進 {self._buy_cost_actual:,.0f} "
                f"> 總預算 {self.total_budget:,.0f} → 停所有市價盲送 + 撤所有 pending 買單")
            threading.Thread(target=self.cancel_all_pending, args=("budget_breached",),
                             name="budget-breach-cancel", daemon=True).start()

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
            held = 0
            if fill.get("action") == "sell":
                with self._lock:
                    st_ext = self.trades.get(fill.get("symbol", ""))
                    held = st_ext.filled_lots if st_ext is not None else 0
            if held > 0:
                # 2026-09-14 node3/node4: 策略持倉被策略外賣掉一部分 → 出場仍送帳上張數被拒。只告警不動帳
                # (filled_lots 不調整;出場被拒「可賣不足」時由 _sell_position 依券商可賣張數重算)
                logger.warning(f"[session] ⚠ {fill['symbol']} 策略外賣出成交 {fill['lots']} 張 "
                               f"(order={fill['order_no']}) — 策略帳上持倉 {held} 張不調整;"
                               f"出場若被拒「可賣不足」會依券商可賣張數重算")
            else:
                logger.info(f"[session] 非策略單成交,忽略 (僅落檔): {fill['symbol']} "
                            f"order={fill['order_no']} {fill['lots']} 張")
            return
        breach_now = False
        late_fill_lots = 0
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
                if row["filled_lots"] >= row["lots"] and row["status"] == "pending":
                    row["status"] = "filled"
                    row["terminal_ts"] = time.time()
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
                # 總曝險硬上限: 實際買進現金累計 (每筆都加、不 floor → 超買 race 也真實反映)。
                # 超過 total_budget → 一次性 breach:出鎖後停盲送 + 撤所有 pending 買單。
                self._buy_cost_actual += lots * fill["price"] * 1000
                st.buy_cost_actual += lots * fill["price"] * 1000    # 該檔實際買進累計 (花費表用)
                if (not self._budget_breached and self.total_budget > 0
                        and self._buy_cost_actual > self.total_budget):
                    self._budget_breached = True
                    breach_now = True
                # 達 target 才清 order_no/標 done — 但**必須是 st 現在追的那張單**成交才清
                # (守衛同 reconcile_orders): 進場 race 可能同時有預掛 P + 市價追 M 兩張 live,
                # st.order_no 已被市價盲送 (_market_chase_worker) 蓋成 M;若 P 的成交補到 target 就清掉,
                # 會把還 live 的 M 追蹤清空 → 出場 had_pending 誤判 False → M 晚成交漏賣被
                # exited 鎖死 (2026-08-24 審查 HIGH)。P 成交時 order_no 是 M 不相符 → 不清,
                # 續為 pending → 出場照撤 M + 等窗口 + 賣全量;13:23 cancel_all 也看得到 M。
                if st.filled_lots >= st.target_lots and st.order_no == fill["order_no"]:
                    st.order_status = "done"
                    st.order_no = ""
                # 出場後晚成交 (A4b;3587 同型漏洞): 出場 worker 已賣完 (exited 且不在進行中) 才落地的
                # 買進 → 這幾張沒有任何出場保護 (has_exposure 被 exited 擋死) → 對 delta 另起賣單。
                # 進行中 (窗口內) 的由 _exit_worker 讀 filled_lots 一併賣;取消追蹤者使用者自負。
                if (st.exited and not st.exit_in_progress
                        and st.stopped_reason != "manual_abandon"):
                    late_fill_lots = lots
            else:   # sell (出場)
                st.filled_lots = max(0, st.filled_lots - lots)
                if row is not None and row.get("kind") in ("limit_sell", "market_sell"):
                    self._reset_exit_backoff_locked(st)   # 策略出場賣單真的成交 → 出場失敗輪次/冷卻歸零
        if late_fill_lots > 0:
            self._start_late_fill_exit(fill["symbol"], late_fill_lots, fill["order_no"])
        # ── 硬上限觸發 (鎖外): 撤所有 pending 買單止血;已成交部位不動,靠出場全量賣。 ──
        if breach_now:
            logger.critical(
                f"[session] ⚠⚠ 總曝險硬上限觸發 — 實際買進 {self._buy_cost_actual:,.0f} "
                f"> 總預算 {self.total_budget:,.0f} → 停所有市價盲送 + 撤所有 pending 買單 "
                f"(已成交部位不動,由出場賣出;當日不再買進)")
            threading.Thread(target=self.cancel_all_pending, args=("budget_breached",),
                             name="budget-breach-cancel", daemon=True).start()

    def _on_order(self, rpt: dict):
        """委託回報 — 記富邦「最後異動時間」last_time (委託被接受/異動的富邦時戳,毫秒);
        交易所拒單時標 rejected (place_order 同步成功但交易所退)。
        2026-09-09 A3 依 function_type 分流 (ft 轉 str 比較;symbol 缺時以 order_no → order_log 反查):
          ft ∈ {0,10,''/None} 且有 error 且 row 存在 → 既有 rejected 路徑 (row 不存在 → 只 log,不動 st);
          ft==30 (撤單回報): status 4 → 忽略 (撤單請求回聲);status 30|40 → 券商確認撤單 → row cancelled
          + _close_cancel + 出佇列;有 error 或 status 39 → 撤單失敗分類 (**絕不標 rejected、絕不清
          st.order_no**;filled_before_cancel/already_cancelled 照分類結案);
          任一無 error 回報且該書號在佇列 → next_ts=now 喚醒 worker (只是加速;timer 才是主路徑)。"""
        order_no = str(rpt.get("order_no") or "")
        last_time = rpt.get("last_time", "")
        err = str(rpt.get("error_message") or "")
        status = str(rpt.get("status") if rpt.get("status") is not None else "").strip()
        ft_raw = rpt.get("function_type")
        ft = "" if ft_raw is None else str(ft_raw).strip()
        with self._lock:
            row = self.order_log.get(order_no) if order_no else None
            if row is not None and last_time:
                row["last_time"] = last_time   # 新單接受回報 = 委託被接受時戳
            symbol = str(rpt.get("symbol") or (row or {}).get("symbol", "") or "")
            in_queue = order_no in self._cancel_queue
            queued_reason = self._cancel_queue[order_no]["reason"] if in_queue else ""
            row_exists = row is not None
        # ── 撤單回報 (ft 30) 或 佇列中書號的 30/40 (ft 缺時容錯) ──
        is_cancel_rpt = (ft == "30") or (ft == "" and in_queue and status in ("30", "40"))
        if is_cancel_rpt:
            if not err and status == "4":
                return                                   # 撤單請求 ACK 回聲 — 不是確認
            if not err and status in ("30", "40"):
                reason = queued_reason or "broker_report"
                self._confirm_cancelled(order_no, symbol, reason, source=f"report ft30 status {status}")
                return
            if err or status == "39":
                cls = classify_cancel_error(err)
                reason = queued_reason or "broker_report"
                logger.warning(f"[session] {symbol} 撤單回報失敗 order={order_no} status={status} "
                               f"ft={ft or '-'} → {cls}: {err or '-'}")
                if cls == "already_cancelled":
                    self._confirm_cancelled(order_no, symbol, reason, source=f"report:{err[:40]}")
                elif cls == "filled_before_cancel":
                    self._settle_filled_before_cancel(order_no, symbol, reason, None, err)
                else:
                    self._note_cancel_err(order_no, f"REPORT:{err or status}",
                                          state="unconfirmed" if in_queue else "")
                return
            # 無 error 的其他撤單回報 (如 status 10) → 只喚醒 worker
            if in_queue:
                self._wake_cancel(order_no)
            return
        # ── 新單/改單回報 (ft 0/10/缺) ──
        if err:
            if not row_exists:
                # 確定非本策略委託 (2026-09-14 node1 同登入第三方程式 28k 拒單 → 3 分鐘 15k 行) →
                # 首見文案立即 log、其餘只計數定期彙總;行為同下 (只 log,不動任何 st)
                if not in_queue and self._is_foreign_order_report(rpt):
                    self._note_foreign_reject(order_no, symbol, ft, status, err)
                    return
                # 集合競價 9049 拒單等 order_no None/不在 order_log → 只 log,不動 st
                logger.warning(f"[session] 委託回報拒單但 order_log 無此書號 ({order_no or '-'} "
                               f"{symbol or '-'} ft={ft or '-'} status={status}): {err}")
                return
            self._settle_rejected(order_no, symbol, err)
            return
        if in_queue:
            self._wake_cancel(order_no)

    # ─── 非本策略委託的拒單回報彙總 (2026-09-14 node1 委託洪水) ─────────────

    def _is_foreign_order_report(self, rpt: dict) -> bool:
        """委託回報是否為「**確定不是本策略**」的新單拒單 → True 才彙總 (broker 也據此略過逐筆 ERROR)。
        全部成立才 True: 有 error;ft 非 30 (撤單回報一律不折疊);status 非 30/39/40 (撤單相關);
        有書號時: 不在 order_log、不在撤單佇列、不是 broker 已認領的書號 (剛下單成功、還沒進 order_log);
        無書號時 (集合競價 9049 等): 回報帶 symbol 且該檔不在本策略 trades / 隔日賣清單。
        任何無法判斷 (例外 / broker 查認領失敗) → False (照舊逐筆 log)。
        回報帶 user_def == "hitlimit" (本策略下單標記) → 一律 False (書號沒追蹤到的本策略委託 [UNKNOWN-* /
        重啟後 / 回報早於認領] 的拒單絕不藏進彙總);user_def 缺值 → 照上列判斷。"""
        try:
            if str(rpt.get("user_def") or "") == _HITLIMIT_USER_DEF:
                return False
            err = str(rpt.get("error_message") or "")
            if not err:
                return False
            ft_raw = rpt.get("function_type")
            if ("" if ft_raw is None else str(ft_raw).strip()) == "30":
                return False
            st_raw = rpt.get("status")
            if ("" if st_raw is None else str(st_raw).strip()) in ("30", "39", "40"):
                return False
            order_no = str(rpt.get("order_no") or "")
            symbol = str(rpt.get("symbol") or "")
            with self._lock:
                if order_no:
                    if order_no in self.order_log or order_no in self._cancel_queue:
                        return False
                elif not symbol or symbol in self.trades or symbol in self.overnight:
                    return False
                broker = self.broker
            if order_no:
                fn = getattr(broker, "is_claimed_order_no", None) if broker is not None else None
                if callable(fn):
                    try:
                        if fn(order_no) is not False:
                            return False
                    except Exception:
                        return False
            return True
        except Exception:
            return False

    def _note_foreign_reject(self, order_no: str, symbol: str, ft: str, status: str, err: str):
        """非本策略拒單回報計數: 首見文案 (每日 ≤_FOREIGN_REJECT_FIRST_LOG_MAX 種) 立即 WARNING (保留
        「order_log 無此書號」字樣),其餘只計數,由 _maybe_log_foreign_reject_summary 定期彙總。"""
        key = err.strip()[:120]
        now = time.monotonic()
        with self._foreign_rej_lock:
            if self._foreign_rej_total == 0:
                self._foreign_rej_window_start = now
            self._foreign_rej_total += 1
            if key in self._foreign_rej_counts or len(self._foreign_rej_counts) < _FOREIGN_REJECT_KEYS_MAX:
                self._foreign_rej_counts[key] = self._foreign_rej_counts.get(key, 0) + 1
            else:
                other = "(其他文案)"
                self._foreign_rej_counts[other] = self._foreign_rej_counts.get(other, 0) + 1
            first = (key not in self._foreign_rej_seen
                     and len(self._foreign_rej_seen) < _FOREIGN_REJECT_FIRST_LOG_MAX)
            if first:
                self._foreign_rej_seen.add(key)
            else:
                self._foreign_rej_suppressed += 1
        if first:
            logger.warning(f"[session] 委託回報拒單但 order_log 無此書號 ({order_no or '-'} "
                           f"{symbol or '-'} ft={ft or '-'} status={status}): {err} — 判定非本策略委託,"
                           f"同文案之後只計數 (每 {_FOREIGN_REJECT_SUMMARY_SEC:g} s 彙總)")
        self._maybe_log_foreign_reject_summary(now)

    def _maybe_log_foreign_reject_summary(self, now: Optional[float] = None, force: bool = False):
        """彙總窗滿 _FOREIGN_REJECT_SUMMARY_SEC (或 force) → 一行 WARNING 彙總後重置窗。
        窗內全部都已逐條 log 過 (無省略) → 靜默重置。撤單 worker 迴圈與每筆計數後各呼叫一次。"""
        now = time.monotonic() if now is None else float(now)
        with self._foreign_rej_lock:
            n = self._foreign_rej_total
            if n == 0:
                return
            span = now - self._foreign_rej_window_start
            if not force and span < _FOREIGN_REJECT_SUMMARY_SEC:
                return
            suppressed = self._foreign_rej_suppressed
            counts = sorted(self._foreign_rej_counts.items(), key=lambda kv: -kv[1])
            self._foreign_rej_total = 0
            self._foreign_rej_suppressed = 0
            self._foreign_rej_counts = {}
            self._foreign_rej_day_total += n
            day_total = self._foreign_rej_day_total
        if suppressed <= 0:
            return
        top = "; ".join(f"{c} 筆「{k}」" for k, c in counts[:5])
        more = f" …另 {len(counts) - 5} 種文案" if len(counts) > 5 else ""
        logger.warning(f"[session] 非本策略委託拒單回報彙總: 近 {span:.0f} s 共 {n} 筆 (未逐條 log {suppressed} 筆;"
                       f"今日累計 {day_total}) — {top}{more}")

    def _wake_cancel(self, order_no: str):
        """回報顯示該書號在券商端已存在 → 佇列項 next_ts=now,worker 立刻再試 (加速,不改 attempts)。"""
        with self._lock:
            it = self._cancel_queue.get(order_no)
            if it is not None:
                it["next_ts"] = min(it["next_ts"], time.time())
        self._cancel_wakeup.set()

    # ─── 委託總表 (前端顯示 + 右鍵刪單) ────────────────────

    def get_orders(self) -> list:
        """全部委託 (新的在前) — 前端委託狀態表用。"""
        with self._lock:
            return [dict(r) for r in reversed(list(self.order_log.values()))]

    def cancel_order_by_no(self, order_no: str):
        """手動刪單 (前端右鍵)。刪的是進場買單時 → 該檔停止進場 (manual_cancel)。
        2026-09-09: 撤單中 (cancel_state 非空且仍 pending) 的單 = **再入佇列** (不 raise;09-09 就是靠手動);
        同步撤失敗 (查無/查詢失敗) → 入佇列、row 維持 pending,不 raise;已成交 → raise 告知 UI。"""
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
            requeue = bool(row.get("cancel_state")) or order_no in self._cancel_queue
            st = self.trades.get(symbol)
            if st is not None and is_buy and st.order_no == order_no:
                st.stopped_reason = st.stopped_reason or "manual_cancel"   # 停止進場意圖立刻生效
        if requeue:
            self.request_cancel(order_no, symbol, "manual_cancel")
            self._wake_cancel(order_no)
            logger.warning(f"[session] 手動刪單 {order_no} ({symbol}) — 撤單中,再入佇列立即重試")
            return
        res = self._try_cancel_sync(order_no, symbol, "manual_cancel")
        if res in ("ok", "already_cancelled"):
            self._confirm_cancelled(order_no, symbol, "manual_cancel", source=f"sync:{res}")
            logger.warning(f"[session] 手動刪單 {order_no} ({symbol})")
        elif res == "filled_before_cancel":
            raise RuntimeError(f"委託 {order_no} 已成交,不可撤 (部位由出場/人工處理)")
        else:
            logger.warning(f"[session] 手動刪單 {order_no} ({symbol}) 未確認 → 已入撤單佇列重試: "
                           f"{self._last_cancel_err.get(order_no, '?')}")

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

    def spending_summary(self) -> dict:
        """花費表 (前端顯示;只看實際成交)。各檔實際花費 + 超額;總實際花費 + 總預算超額。
        - 各檔花費 = 該檔實際買進現金累計 (buy_cost_actual,單調不因賣出減)
        - 各檔超額 = max(0, 花費 − 目標金額);目標金額 = target_lots × 漲停價 × 1000 (沒超過=0)
        - 總實際花費 = _buy_cost_actual (硬上限同源);總超額 = max(0, 總花費 − total_budget)"""
        with self._lock:
            rows = []
            for sym, st in self.trades.items():
                if st.target_lots <= 0 and st.buy_cost_actual <= 0:
                    continue
                intended = st.target_lots * st.limit_up * 1000
                over = max(0.0, st.buy_cost_actual - intended)
                rows.append({
                    "symbol": sym,
                    "filled_lots": st.filled_lots,
                    "target_lots": st.target_lots,
                    "avg_price": round(st.avg_price, 2),
                    "spent": round(st.buy_cost_actual, 0),
                    "intended": round(intended, 0),
                    "over": round(over, 0),
                })
            rows.sort(key=lambda r: (-r["over"], -r["spent"]))   # 超額大的、花最多的排前面
            total_over = (max(0.0, self._buy_cost_actual - self.total_budget)
                          if self.total_budget > 0 else 0.0)
            return {
                "total_budget": round(self.total_budget, 0),
                "total_spent": round(self._buy_cost_actual, 0),
                "total_over": round(total_over, 0),
                "budget_breached": self._budget_breached,
                "symbols": rows,
            }

    def status(self) -> dict:
        node_role = _node_role()
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
                # 2026-09-15 本節點帳號顯示 (連線表單旁 / 已連線列): .env FUBON_ACCOUNT_ID 遮罩,
                # 連線與否都帶;"" = 未設 (或 hub)。放在 **b 之後 → broker.status() 不可能蓋掉。
                # hub 不交易 (runner: role==hub 不預掛/不進 trader) → 不回 ID 片段 (hub 的 FUBON_ACCOUNT_ID
                # 是行情帳號,UI 不公開),前端依 node_role 顯示「hub 不交易」
                "node_role": node_role,
                "node_login_masked": "" if node_role == "hub" else _node_login_masked(),
                "params": {
                    "sizing_mode": self.sizing_mode,
                    "fixed_lots": self.fixed_lots,
                    "total_budget": self.total_budget,
                    "per_symbol_budget": self.per_symbol_budget,
                },
                "budget_used": round(self.budget_used, 0),
                "buy_cost_actual": round(self._buy_cost_actual, 0),   # 實際買進累計 (硬上限用)
                "budget_breached": self._budget_breached,             # 總曝險硬上限已觸發
                "n_symbols": len(self.trades),
                "n_positions": sum(1 for s in self.trades.values() if s.filled_lots > 0),
                # 2026-09-09 A5 可見性: 撤單佇列 / 未確認 / 在飛市價買 / worker 存活
                "n_cancel_queued": len(self._cancel_queue),
                "n_cancel_unconfirmed": (
                    sum(1 for it in self._cancel_queue.values() if it["attempts"] > 0)
                    + sum(1 for no, r in self.order_log.items()
                          if r.get("cancel_state") == "unconfirmed" and no not in self._cancel_queue)),
                "n_inflight": sum(1 for r in self.order_log.values()
                                  if r.get("status") == "pending" and r.get("kind") == "market_buy"),
                "cancel_worker_alive": self._cancel_worker_alive(),
            }
