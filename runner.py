"""Runner — 把 filter.py 主流程包成 background thread，讓 FastAPI/APScheduler 觸發。

單一 process 內只有 1 個 Runner instance (single-instance guard)。
外部 (API endpoint) 透過 runner 讀 state / progress / holdings。
"""
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, time as dtime
from enum import Enum
from pathlib import Path
from typing import Dict, Optional

from config import Config, load_config
from trader import (_first_priced, _overnight_book_fields,
                    _pick)   # 輕量純函式 (monitor handler 每 tick 用,不 lazy import)

logger = logging.getLogger(__name__)

# 漲停價抓取:每趟重試之間等多久 (讓交易所繼續把當日漲停價上架)
_LIMIT_UP_RETRY_SLEEP_SEC = 20


class Phase(str, Enum):
    IDLE = "idle"                    # 未啟動
    LOGIN = "login"                  # 登入中
    FETCH_UNIVERSE = "fetch_universe"  # 抓股票母體
    FETCH_LIMIT_UPS = "fetch_limit_ups"  # 抓漲停價
    SUBSCRIBE = "subscribe"          # 訂閱 WS
    FILTERING = "filtering"          # 8:30-9:00 篩選
    LIVE_SUBSCRIBE = "live_subscribe"  # 常駐收 tick (SKIP_TRADER=true or 9:00 後才啟動)
    TRADING = "trading"              # 9:00-13:24 交易 (SKIP_TRADER=false)
    FINISHED = "finished"            # 收盤結束
    ERROR = "error"                  # 出錯


class Runner:
    """單一 process 常駐 runner。API 透過 shared state 讀 progress/state。"""

    _instance: Optional["Runner"] = None
    _instance_lock = threading.Lock()

    @classmethod
    def get(cls) -> "Runner":
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = Runner()
            return cls._instance

    def __init__(self):
        self.cfg: Optional[Config] = None
        self.phase: Phase = Phase.IDLE
        self.error_msg: str = ""

        # 主流程 handles
        self.sdk = None
        self.universe: list = []
        self.limit_ups: Dict[str, float] = {}
        self.limit_downs: Dict[str, float] = {}   # symbol → 跌停價 (出場限價賣用,2026-08-12)
        self.dispositions: Dict[str, bool] = {}   # symbol → isDisposition (處置股)
        self._marked_frozen: bool = False   # hub 模式 08:59:50 凍結 marked 後 = True (marked-snapshot final 旗標)
        self.day_tradable: Dict[str, bool] = {}   # symbol → canDayTrade (禁現沖減半用,風控①)
        self._t30_meta: dict = {}        # T30 載入 meta (檔案 ok/stale/missing) — /api/t30 顯示用
        self._avgvol: dict = {}          # 月均量篩選結果 (今天 ran/dropped/kept/meta) — /api/avg-volume 顯示用
        self.state = None                # state.State
        self.subscriber = None
        self.trader = None
        self.recorder = None
        self.order_client = None

        # 交易會話 (模擬/真實) — singleton 屬性,活過每日 runner 重啟。
        # 連線/armed 由 /api/trading/* 控制;預設 sim + 未 armed (絕不自動下單)。
        from trading_session import TradingSession
        self.session = TradingSession()

        # Progress tracking
        self._limit_up_progress: Dict[str, int] = {"done": 0, "total": 0, "ok": 0, "fail": 0}
        self._started_at: Optional[datetime] = None

        # 執行 thread
        self._main_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        # timer threads (預掛/13:23 撤單) — 每輪 start() 前先終結上一輪殘留
        self._timer_threads: list = []
        # overnight_holdings.json 檔案鎖 — 13:24 主流程與前端 add/remove API 都會寫
        self._overnight_file_lock = threading.Lock()
        # 逐筆撮合開始時間 (= cfg.end_time,_run_all_phases 覆寫) — 此前的 trades
        # 都是試撮,不可觸發任何下單 (2026-08-10 2491 事故)
        self._trade_start_time = dtime(9, 0)

    # ─── 對外 API ────────────────────────────────────────────

    def start(self) -> bool:
        """啟動主流程 background thread。回 True = 已啟動 / False = 已在跑."""
        if self._main_thread and self._main_thread.is_alive():
            logger.warning("[runner] 已在跑，忽略 start()")
            return False
        # 終結上一輪殘留 timer threads,再換**全新** event。
        # 不能 clear() 重用: 舊 pre-order-timer 還在等同一個 event,clear 會把它復活
        # → 兩個 timer 都到 08:59:58 → 預掛單重複下 (雙倍買+雙倍預算)。
        self._stop_event.set()
        for t in self._timer_threads:
            t.join(timeout=2)
            if t.is_alive():
                logger.warning(f"[runner] 舊 timer {t.name} 2 秒內沒結束 (它抓的是舊 event,不會復活)")
        self._timer_threads = []
        self._stop_event = threading.Event()
        self.phase = Phase.IDLE
        self.error_msg = ""
        self._started_at = datetime.now()
        self._main_thread = threading.Thread(target=self._main, name="runner-main", daemon=True)
        self._main_thread.start()
        logger.info("[runner] main thread started")
        return True

    def stop(self):
        """通知主流程停止 (不平倉 — 持倉/委託不動,平倉走 /api/trading/close_all)."""
        logger.info("[runner] stop() 被呼叫")
        self._stop_event.set()
        for t in self._timer_threads:
            t.join(timeout=2)
            if t.is_alive():
                logger.warning(f"[runner] timer {t.name} 2 秒內沒 join 完 (skip)")
        # 各元件單獨 stop (順序: trader → subscriber → recorder → order_client)
        if self.trader:
            try:
                self.trader.stop()
            except Exception as e:
                logger.warning(f"[runner] trader.stop 例外: {e}")
        if self.subscriber:
            try:
                self.subscriber.stop()
            except Exception as e:
                logger.warning(f"[runner] subscriber.stop 例外: {e}")
        if self.recorder:
            try:
                self.recorder.close()
            except Exception:
                pass
        if self.order_client:
            try:
                self.order_client.close()
            except Exception:
                pass

    def is_running(self) -> bool:
        return self._main_thread is not None and self._main_thread.is_alive()

    def marked_snapshot(self) -> dict:
        """中心過濾 Hub → 交易節點的 marked 快照 (見架構 plan)。node 靠它預掛+訂閱,不自己 filter。
        每檔帶: 漲停價 / 處置旗標 / 開盤即鎖 / 優先序。final=True = hub 已於 08:59:50 凍結。"""
        role = self.cfg.role if self.cfg else "standalone"
        now = datetime.now().isoformat(timespec="seconds")
        if not self.state:
            return {"ts": now, "final": False, "role": role, "symbols": []}
        marked = self.state.get_marked_prioritized()          # 已排序: 開盤即鎖優先
        first_tick = self.state.first_tick_limit_up
        # session.untradable 在 runner-main 執行緒被整組 rebind、這裡在 FastAPI 執行緒讀 —
        # 先 bind local (單次屬性讀取原子),list comprehension 全程看同一組
        untradable = self.session.untradable if self.session else set()
        symbols = [{
            "symbol": s,
            "limit_up": float(self.limit_ups.get(s, 0.0)),
            "is_disposition": bool(self.dispositions.get(s, False)),
            "first_tick": s in first_tick,
            "priority": i,
            # mark 以來最大委買量 (峰值) — node 08:59:58 用「自己當前 < 此 × ratio」判量減半 (見架構 plan)
            "max_bid_vol": self.state.get_max_bid_size(s),
            # 最後已知當前量 — node 兩步 seed 用 (last≠max,盤前尾段量崩檔才剔得掉)
            "last_bid_vol": self.state.get_last_bid_size(s),
            # ─── 風控三欄 (2026-09-01) ───
            "limit_down": float(self.limit_downs.get(s, 0.0)),   # 出場=跌停價限價賣 (處置股必要)
            "is_t30": s in untradable,                           # 全額交割禁單 (08-12 事故防線)
            "day_tradable": bool(self.day_tradable.get(s, True)),  # 禁現沖 → 減半 (is False 嚴格比對,務必 bool)
        } for i, s in enumerate(marked)]
        return {"ts": now, "final": bool(self._marked_frozen), "role": role, "symbols": symbols}

    def _wait_until_clock(self, time_str: str):
        """精準等到今天的 HH:MM:SS (已過則立即返回);stop_event 可中斷。"""
        try:
            hh, mm, ss = map(int, time_str.split(":"))
        except Exception:
            logger.warning(f"[runner] 時點格式錯 ({time_str!r}) — 不等待")
            return
        stop_event = self._stop_event
        while not stop_event.is_set():
            now = datetime.now()
            remaining = (now.replace(hour=hh, minute=mm, second=ss, microsecond=0) - now).total_seconds()
            if remaining <= 0:
                return
            stop_event.wait(0.05 if remaining < 1 else remaining - 0.5)

    def _start_freeze_timer(self):
        """hub 模式: 等到 HUB_FREEZE_TIME (08:59:50) → _marked_frozen=True
        (marked-snapshot final=true,通知各 node 來拉)。背景 thread。"""
        def _timer():
            self._wait_until_clock(self.cfg.hub_freeze_time)
            if self._stop_event.is_set():
                return
            n = len(self.state.get_marked_list()) if self.state else 0
            self._marked_frozen = True
            logger.warning(f"[runner] ⏸ {self.cfg.hub_freeze_time} 凍結 marked ({n} 檔) "
                           f"→ final=true,node 可拉")
        t = threading.Thread(target=_timer, name="hub-freeze-timer", daemon=True)
        t.start()
        self._timer_threads.append(t)

    def _apply_marked_snapshot(self, snap) -> list:
        """node: 把 Hub 快照灌進本地 state/session。回 marked 清單 (優先序);無快照 → 空 (今日不交易)。

        每檔: limit_ups/處置 + 風控三欄 (limit_down 出場跌停價 / is_t30 全額交割禁單 /
        day_tradable 禁現沖減半;2026-09-01 補 — 之前 node 全缺 → T30 狂送單風險 + 處置股卡倉)
        + 兩步 seed: mark(峰值) → update_max_bid(last)。只設 last=max 的話,
        08:59:50–58 無新 tick 的量崩檔 final_check_all (last < max×ratio) 永遠剔不掉。
        舊 hub 快照缺欄 → 各預設 = 原行為,不炸。"""
        marked: list = []
        t30_set: set = set()
        self._node_bid_vol_fallback = {}     # sym → Hub 最後已知漲停層委買量 (20% cap 兜底)
        if snap and snap.get("symbols"):
            for s in snap["symbols"]:
                sym = s["symbol"]
                lu = float(s.get("limit_up") or 0.0)
                self.limit_ups[sym] = lu
                self.dispositions[sym] = bool(s.get("is_disposition"))
                down = float(s.get("limit_down") or 0.0)
                if down > 0:
                    self.limit_downs[sym] = down
                self.day_tradable[sym] = bool(s.get("day_tradable", True))
                if s.get("is_t30"):
                    t30_set.add(sym)
                # seed Hub 峰值進 state._max_bid_size;node book handler 之後續墊高 → final_check_all 不變
                max_vol = int(s.get("max_bid_vol") or 0)
                self.state.mark(sym, lu, max_vol, lu,
                                first_tick=bool(s.get("first_tick")))
                last_vol = s.get("last_bid_vol")
                self.state.update_max_bid(sym, max_vol if last_vol is None else int(last_vol))
                # 20% 風控②母數兜底: node 08:59:58 若 subscriber 還沒有該檔 book
                # (8 秒內零 tick、訂閱 snapshot 也沒到),退用 Hub 最後已知漲停層委買量
                # → 有 cap 總比無 cap 好 (無 cap = 純預算算張,可能超過隊伍 20%)
                self._node_bid_vol_fallback[sym] = int(
                    max_vol if last_vol is None else int(last_vol))
            marked = self.state.get_marked_prioritized()
            logger.info(f"[node] 由 Hub 快照建 marked {len(marked)} 檔 (hub ts={snap.get('ts')}; "
                        f"T30 {len(t30_set)} 檔, 跌停價 {len(self.limit_downs)} 檔)")
        else:
            logger.warning("[node] 無 marked 快照 → 今日不交易 (仍常駐收 tick 供查詢)")

        # 風控資料交給交易會話 (與 standalone runner 的 set 流程對齊)
        self.session.set_dispositions(self.dispositions)
        self.session.set_limit_downs(self.limit_downs)      # 出場=跌停價限價賣 (處置股必要)
        self.session.set_day_tradable(self.day_tradable)    # 禁現沖 → 部位減半 (風控①)
        self.session.set_untradable(t30_set)                # T30 全額交割禁單 (08-12 事故防線)
        return marked

    def _prepare_overnight(self, output_dir) -> list:
        """載入隔日賣清單 (昨天買到未出場的持倉) + 補查今日漲停/跌停價 → 交給 session。
        standalone 與 node 共用 (避免兩路徑漂移)。回 overnight_syms。

        出場=跌停價限價賣;漲停價=鎖漲停續抱判斷 (隔日賣標的可能不在今日 limit_ups 抓取範圍)。
        查無漲停價 → 該檔續抱功能停用退回開盤即賣。
        ⚠ 漲停價收進獨立 _overnight_ups 傳 session,**絕不寫 self.limit_ups** — filter handler closure
        捕獲該 dict,混入會讓 8:30 篩選把昨日持倉當新標的 mark → 預掛買單加碼!"""
        self._load_overnight_file(output_dir)
        overnight_syms = self.session.overnight_symbols()
        _stock = self.sdk.marketdata.rest_client.stock
        _overnight_ups: dict = {}
        for _s in overnight_syms:
            up = self.limit_ups.get(_s)            # universe 抓過的直接複用 (node 為空 → 走補查)
            if up is None or _s not in self.limit_downs:
                for _try in range(3):              # 小重試 (原本單次靜默失敗)
                    got = self._query_limit_up(_stock, _s)   # 副作用寫 dispositions/limit_downs
                    if got:
                        up = up or got
                        break
                    time.sleep(1)
            if up:
                _overnight_ups[_s] = float(up)
            else:
                logger.warning(f"[runner] 隔日賣 {_s} 查無今日漲停價 → "
                               f"鎖漲停判斷停用 (無市價列時開盤即賣)")
        # 跌停價/處置股/隔日賣漲停價交給交易會話 (出場=跌停價限價賣,2026-08-12 定案)
        self.session.set_limit_downs(self.limit_downs)
        self.session.set_dispositions(self.dispositions)   # 補查後含隔日賣標的的處置股資訊
        self.session.set_day_tradable(self.day_tradable)   # 補查後含隔日賣標的的禁現沖資訊
        self.session.set_overnight_limit_ups(_overnight_ups)
        return overnight_syms

    def _run_node_phases(self):
        """ROLE=node: 略過 universe/limit_ups/filter;等到 HUB_FREEZE_TIME 向 Hub 拉 marked 快照 →
        seed state (含 Hub 峰值) → 只訂 marked (1 socket) → 08:59:58 預掛 (量減半 final_check_all 不變:
        seed 的 Hub 峰值 + node book handler 08:59:50–58 續墊高) → 09:00 狂送/首盤/出場 (共用 _trade_phase)。"""
        from state import State
        from recorder import TickRecorder
        from subscriber import Subscriber
        from filter import make_on_book_handler, wait_until_end_time
        from node_client import pull_marked_snapshot

        self.phase = Phase.SUBSCRIBE
        self.state = State(bid_drop_ratio=self.cfg.bid_drop_ratio)
        output_dir = Path(__file__).parent / "output"
        output_dir.mkdir(exist_ok=True)
        today = datetime.now().strftime("%Y-%m-%d")
        self.recorder = TickRecorder(output_dir / f"{today}_ticks.jsonl")

        # 隔日賣清單 + 補查 — 趁 08:00~08:59:50 空檔先做 (REST 補查不吃 08:59:50→58 預掛時窗)。
        # 08:59:50 _apply_marked_snapshot 會再以 self.* 合併重設 marked+overnight,最後狀態正確。
        overnight_syms = self._prepare_overnight(output_dir)
        if overnight_syms:
            logger.info(f"[node] 隔日賣標的 {len(overnight_syms)} 檔 → 一併訂閱、9:00 開盤賣")

        # 等到 Hub 凍結時點才拉 (免 08:00–08:59:50 空輪詢);死線 = 預掛前 1 秒 (08:59:57)
        self._wait_until_clock(self.cfg.hub_freeze_time)
        pre_t = self._parse_time_hhmm(self.cfg.pre_order_time) or dtime(8, 59, 58)
        now = datetime.now()
        deadline_ts = now.replace(hour=pre_t.hour, minute=pre_t.minute,
                                  second=pre_t.second, microsecond=0).timestamp() - 1.0

        snap = None
        if self.cfg.hub_url:
            snap = pull_marked_snapshot(self.cfg.hub_url, deadline_ts)
        else:
            logger.error("[node] ROLE=node 但 HUB_URL 未設 → 今日不交易")

        marked = self._apply_marked_snapshot(snap)

        # 只訂 marked (1 socket);handler 用**與 standalone 相同的完整 filter handler**
        # (2026-09-01 對齊: 原輕量 handler 只更新峰值、不 unmark → 08:59:50–開盤間鎖破
        # (賣單出現/跌下漲停) 的檔 node 照樣預掛且不撤,standalone 卻 unmark+撤單。
        # 完整 handler 對 marked 檔跑 unmark 檢查 + 08:59:58 前記 max/last;
        # 未 marked 檔沒訂閱、也沒 limit_up → mark 分支自然不觸發,清單仍由 Hub 快照固定)
        unsub_ref = {"fn": None}
        on_book = make_on_book_handler(self.state, self.limit_ups, self.cfg, unsub_ref)
        # 訂閱 = marked + 隔日賣標的 (隔日賣標的無 limit_up → filter handler 不會 mark;09:00 由 trader 賣出)
        sub_syms = list(marked) + [s for s in overnight_syms if s not in marked]
        self.subscriber = Subscriber(
            sdk=self.sdk, universe=sub_syms, on_book=on_book, login_cfg=self.cfg,
            recorder=self.recorder, batch_size=self.cfg.batch_size,
            batch_rotate_sec=self.cfg.batch_rotate_sec,
            socket_count=self.cfg.socket_count, debug=self.cfg.debug,
        )
        self.subscriber.start()
        if sub_syms:
            self.subscriber.subscribe_trades_for(sub_syms)
        # 首筆成交 → 開盤訊號 (mark_opened + on_first_trade),與 standalone 同
        self.subscriber.set_handlers(on_trade=self._monitor_on_trade)

        # unmark 淘汰 → 立即退訂 + 撤該檔預掛/pending 委託 (與 standalone _unsub_and_cancel 同款;
        # session 未 armed 時撤單為 no-op)
        def _unsub_and_cancel(symbol: str):
            self._unsub_symbol(symbol)
            try:
                self.session.cancel_symbol_orders_async(symbol, "unmarked")
            except Exception as e:
                logger.warning(f"[node] 撤 {symbol} 委託失敗: {e}")
        unsub_ref["fn"] = _unsub_and_cancel

        # 08:59:58 預掛 (量減半 final_check_all 用 seed 峰值 + node 當前) + 13:23 撤單
        self._start_pre_order_timer()
        self._start_cancel_pending_timer()

        # 等到收盤 → 交易 (共用 _trade_phase);watchlist = 量減半後存活的 marked
        wait_until_end_time(self.state, self.cfg)
        watchlist = self.state.get_marked_list()
        logger.info(f"[node] 轉場交易,watchlist {len(watchlist)} 檔 (量減半後) + 隔日賣 {len(overnight_syms)} 檔")
        self._trade_phase(watchlist, overnight_syms)

    def get_status(self) -> dict:
        """給 API /api/status 用的整體狀態 snapshot."""
        stats = self.state.stats() if self.state else {}
        return {
            "phase": self.phase.value,
            "is_running": self.is_running(),
            "started_at": self._started_at.isoformat() if self._started_at else None,
            "now": datetime.now().isoformat(timespec="seconds"),
            "error": self.error_msg,
            "limit_up_progress": dict(self._limit_up_progress),
            "universe_size": len(self.universe),
            "filter_stats": stats,
            "watchlist_size": stats.get("currently_marked", 0),
            "recorder_tick_count": self.recorder.count() if self.recorder else 0,
        }

    # ─── 主流程 ─────────────────────────────────────────────

    def _main(self):
        """runner main — 從 login 到 trading 收盤."""
        try:
            self._run_all_phases()
        except BaseException as e:
            # BaseException: SystemExit 在非主 thread 會被 threading 靜默丟棄
            # (phase 凍結、error_msg 空 → 早上壞掉沒人知道),一併攔下標 ERROR。
            logger.exception(f"[runner] 主流程例外: {e}")
            self.phase = Phase.ERROR
            self.error_msg = str(e) or type(e).__name__

    def _run_all_phases(self):
        self.cfg = load_config()
        self._trade_start_time = self._parse_time_hhmm(self.cfg.end_time) or dtime(9, 0)

        # 每日重置交易 state (新交易日 → 清前日 trades/委託表/預算 + armed=False;
        # 同日手動重啟 → 保留當日 state。連線不動。)
        self.session.roll_day(datetime.now().strftime("%Y-%m-%d"))

        # 交易會話策略參數 (500 上限 / 送單間隔 / 13:23 撤單時點)
        self.session.configure(
            max_stock_price=self.cfg.max_stock_price,
            order_min_interval_sec=self.cfg.order_min_interval_sec,
            cancel_pending_time=self.cfg.cancel_pending_time,
            order_max_per_sec=self.cfg.order_max_per_sec,
        )

        # Phase 1: Login
        self.phase = Phase.LOGIN
        from filter import login_fubon
        self.sdk, _ = login_fubon(self.cfg)

        # ─── ROLE 分岔 (中心過濾架構) ───
        # node: 不自己 filter,08:59:50 向 Hub 拉 marked 快照 → 只訂那幾檔 → 交易。
        if self.cfg.role == "node":
            self._run_node_phases()
            return

        # Phase 2: Universe
        self.phase = Phase.FETCH_UNIVERSE
        from universe import get_universe, fetch_limit_ups
        self.universe = get_universe(self.sdk, self.cfg.universe)
        if not self.universe:
            raise RuntimeError("universe 空")
        self._filter_low_volume()        # 月均量 < 門檻 盤前剔除 (縮母體省下游 REST)

        # Phase 3: Limit ups (慢 — 12-15 分鐘) — 有當日 cache 就秒讀
        self.phase = Phase.FETCH_LIMIT_UPS
        self._limit_up_progress = {"done": 0, "total": len(self.universe), "ok": 0, "fail": 0}
        self.limit_ups = self._load_or_fetch_limit_ups()
        if not self.limit_ups:
            raise RuntimeError("limit_ups 全 fail")
        # 把處置股名單交給交易會話 (下單機制: 處置股不撤預掛/不下市價買、出場改委買一價賣)
        self.session.set_dispositions(self.dispositions)
        self.session.set_day_tradable(self.day_tradable)   # 禁現沖減半 (風控①)

        # T30 禁單名單 (全額交割/每筆需 100% 預收) — API 下單必被拒,預掛/市價追
        # 一律跳過 (2026-08-12 全額交割股狂送單事故)。取檔由 fetch_t30 timer 08:05 負責
        # (券商 08:00 更新檔,留 5 分鐘餘裕);漲停價要抓 ~8 分鐘以上,
        # 走到這裡新檔已就位,單次載入即可。
        self._load_t30_untradable()

        # Phase 4: Subscribe
        self.phase = Phase.SUBSCRIBE
        from state import State
        from recorder import TickRecorder
        from subscriber import Subscriber
        self.state = State(bid_drop_ratio=self.cfg.bid_drop_ratio)
        output_dir = Path(__file__).parent / "output"
        output_dir.mkdir(exist_ok=True)
        today = datetime.now().strftime("%Y-%m-%d")
        self.recorder = TickRecorder(output_dir / f"{today}_ticks.jsonl")

        from filter import make_on_book_handler
        unsub_ref = {"fn": None}
        on_book = make_on_book_handler(self.state, self.limit_ups, self.cfg, unsub_ref)

        # 載入隔日賣清單 (昨天買到未出場的持倉) + 補查今日漲停/跌停價 → 交給 session。
        # standalone 與 node 共用同 helper (避免兩路徑漂移)。把它們也放進訂閱母體,
        # 8:30 起就收得到五檔+成交 (隔日賣標的可能不在漲停母體裡)。
        overnight_syms = self._prepare_overnight(output_dir)
        sub_universe = [s for s in self.universe if s in self.limit_ups]
        for s in overnight_syms:
            if s not in sub_universe:
                sub_universe.append(s)

        self.subscriber = Subscriber(
            sdk=self.sdk,
            universe=sub_universe,
            on_book=on_book,
            login_cfg=self.cfg,
            recorder=self.recorder,
            batch_size=self.cfg.batch_size,
            batch_rotate_sec=self.cfg.batch_rotate_sec,
            socket_count=self.cfg.socket_count,
            debug=self.cfg.debug,
        )
        self.subscriber.start()

        # unmark 淘汰 → 立即退訂 + 撤該檔預掛/pending 委託 (session 未 armed 時為 no-op)
        def _unsub_and_cancel(symbol: str):
            self._unsub_symbol(symbol)
            try:
                # async — unmark 在行情 thread 上觸發,08:59:58 後有預掛單時
                # 同步 REST 會卡住同 socket 其他股票的 tick
                self.session.cancel_symbol_orders_async(symbol, "unmarked")
            except Exception as e:
                logger.warning(f"[runner] 撤 {symbol} 委託失敗: {e}")
        unsub_ref["fn"] = _unsub_and_cancel

        # 預掛限價單 timer (08:59:58) — real mode + armed 才會真的下;
        # 對「當下還在 marked 清單」的標的掛漲停價限價買單 (搶開盤集合競價排隊)。
        # hub 模式不交易 → 不預掛,改 08:59:50 凍結 marked (通知 node 來拉)。
        if self.cfg.role == "hub":
            self._start_freeze_timer()
        else:
            self._start_pre_order_timer()
        # 13:23 (CANCEL_PENDING_TIME) 撤所有未成交委託 timer (持倉不動、留倉)
        self._start_cancel_pending_timer()

        # Phase 5: 篩選 (8:30-9:00)
        # 若現在時間 >= END_TIME (例: 9:00 後手動啟動)，wait_until_end_time 內部
        # 會 log warning 立即 return，不阻塞。
        self.phase = Phase.FILTERING
        from filter import wait_until_end_time, write_output
        wait_until_end_time(self.state, self.cfg)
        write_output(self.state, self.cfg)

        # 9:00 起 filter handler 必須**立刻**卸下 — 盤中市價買單佔五檔第一列且 price=0,
        # filter 的「跌下漲停」規則會 mass-unmark → 連鎖撤預掛單 (6243 事件同型)。
        # 換 monitor handler: 不做 mark/unmark,只服務隔日賣標的的報價/成交觸發
        # (snapshot/recorder 在 subscriber 內部,不受 handler 更換影響)。
        # SKIP_TRADER=false 時稍後會再被 trader handlers 蓋掉。
        self.subscriber.set_handlers(on_book=self._monitor_on_book,
                                     on_trade=self._monitor_on_trade)

        watchlist = [row["symbol"] for row in self.state.snapshot()]
        logger.info(f"[runner] 篩選階段結束，watchlist {len(watchlist)} 檔")

        # 9:00 轉場: 只留 marked + 隔日賣標的 — 其餘全退訂,並加訂 trades
        keep = watchlist + [s for s in overnight_syms if s not in watchlist]
        if keep:
            self.subscriber.keep_only(keep)
            self.subscriber.subscribe_trades_for(keep)
        else:
            logger.warning("[runner] watchlist + 隔日賣皆空 — 不退訂 (留全母體訂閱供查詢)")

        # ── 分岔: SKIP_TRADER (或 hub 不交易) 決定進 LIVE_SUBSCRIBE 或 TRADING ──

        if self.cfg.skip_trader or self.cfg.role == "hub":
            logger.info("[runner] SKIP_TRADER=true → 進 LIVE_SUBSCRIBE (subscriber 常駐收 tick，"
                        "不下單。第二個分頁可查任何股票 tick)")
            self.phase = Phase.LIVE_SUBSCRIBE
            self._live_subscribe_loop()
            # 收盤 / user 手動 stop 都會跳出
            # armed 下 SKIP_TRADER 也可能有成交 (預掛/隔日賣) — 一樣要持久化,
            # 原本只有 TRADING 分支寫,SKIP_TRADER 的持倉會直接消失 (2026-08 修)
            self._write_overnight_file()
            self._append_positions_history()
            self.subscriber.stop()
            self.recorder.close()
            self.phase = Phase.FINISHED
            logger.info("[runner] LIVE_SUBSCRIBE 結束")
            return

        self._trade_phase(watchlist, overnight_syms)

    def _trade_phase(self, watchlist, overnight_syms):
        """9:00 起交易 (建 Trader + 首筆種子化 + 等到 13:24) + 收盤 (撤單/存隔日賣/收檔) —
        standalone 與 node 共用同一段,避免兩條路徑漂移。"""
        if not watchlist and not overnight_syms:
            logger.info("[runner] 篩選結果空 + 無隔日賣 + SKIP_TRADER=false → 直接 finished")
            self.subscriber.stop()
            self.recorder.close()
            self.phase = Phase.FINISHED
            return

        # Phase 6: Trading (9:00-13:24) — 監控;session 為 real+armed 時掛真單。
        # watchlist 空但有隔日賣標的時,trader 仍要建 (才能分派隔日賣標的的 tick)。
        self.phase = Phase.TRADING
        from trader import Trader
        self.trader = Trader(
            watchlist=watchlist,
            limit_ups=self.limit_ups,
            cfg=self.cfg,
            recorder=self.recorder,
            state=self.state,                                   # 第一盤淘汰同步 unmark
            unsub_fn=self._unsub_symbol,                        # + 退訂 (隔日賣標的不退)
            session=self.session,                               # 交易會話 (sim = 純監控)
            dispositions=self.dispositions,                     # 處置股量減半基準用限價列
        )
        # trades 已在 9:00 轉場時對 watchlist 加訂 (keep_only 之後)
        self.subscriber.set_handlers(on_book=self.trader.on_book, on_trade=self.trader.on_trade)
        self.trader.start()

        # 種子化首筆成交 — 開盤撮合 trade 可能在 handler 掛上前就到了
        # (08:59:58 已先訂 trades,subscriber buffer 有存);on_trade 冪等 (first_trade_seen)
        for _sym in watchlist:
            try:
                _snap = self.subscriber.get_latest_snapshot(_sym)
                _lt = (_snap or {}).get("last_trade")
                if not _lt or not _lt.get("price"):
                    continue
                # 盤前試撮殘留的 snapshot 不能種子化成首筆成交 (2026-08-10)
                if _lt.get("is_trial"):
                    continue
                _ts = str(_lt.get("ts") or "")
                if len(_ts) >= 19 and _ts[11:19] < self.cfg.end_time:
                    continue
                self.trader.on_trade(_sym, _lt)
            except Exception:
                pass

        from filter import _wait_until
        _wait_until(self.cfg.trading_end_time, self.trader, self.cfg)

        # Phase 7: 收盤 — 撤所有未成交委託 (不賣持倉,留倉隔日; 使用者定案 2026-07-16)
        try:
            self.session.cancel_all_pending("trading_end")
        except Exception as e:
            logger.warning(f"[runner] 收盤撤單例外: {e}")
        # 存「今日買到、未出場」的持倉 → 隔日賣清單 (13:24 當下持倉算隔夜)
        self._write_overnight_file()
        self._append_positions_history()   # 每日收盤 append 一行 (不覆蓋, 累積歷史台帳)
        self.trader.stop()
        self.subscriber.stop()
        # 收盤即把今日成交載回 session.overnight → 隔日賣頁立刻顯示 (庫存對帳為準)。
        # 放在 trader/subscriber stop 之後 → 已無 tick,不會誤觸發今日賣單。
        try:
            self._load_overnight_file()
        except Exception as e:
            logger.warning(f"[runner] 收盤載入隔日賣顯示失敗: {e}")
        self.recorder.close()
        self.phase = Phase.FINISHED
        logger.info("[runner] 收盤結束")

    def _unsub_symbol(self, symbol: str):
        """退訂單檔 — 但隔日賣清單的標的**不退訂** (要持續收五檔+成交才能賣掉)。"""
        if self.session.has_overnight(symbol):
            logger.info(f"[runner] {symbol} 在隔日賣清單 → 不退訂 (保留收資料)")
            return
        self.subscriber.request_unsubscribe(symbol)

    # ─── 9:00 後 monitor handlers (LIVE_SUBSCRIBE 全程 / trader 建立前的空窗) ───

    def _monitor_on_book(self, symbol: str, bids: list, asks: list, is_continuous: bool = False):
        """9:00 後的看盤 handler — **不做 mark/unmark** (filter 規則對盤中 price=0
        市價列會誤判)。is_continuous 不用 (本 handler 本就不 mark/unmark),簽名對齊 subscriber。
        轉送隔日賣標的的 book → session 跑純盤面賣出規則
        (委買一跌下今日漲停 → 跌停價限價賣;鎖著 → 抱)。"""
        if self.session.has_overnight(symbol):
            self.session.update_overnight_book(
                symbol, *_overnight_book_fields(bids, asks))

    def _monitor_on_trade(self, symbol: str, trade_data):
        """monitor 期間的 trades handler — **08:59:58 就掛上** (見 _start_pre_order_timer),
        9:00:00 開盤撮合 tick 一到就觸發,不等 9:00 轉場建 Trader (轉場要 1~3 秒)。

        今日標的首筆成交 → 瞬間觸發市價追 (session.on_first_trade;
        冪等旗標在 session 內 — 重複 tick / trader 接手後都不會重複下單)。
        armed/is_live 閘門都在 session 內,sim/未 armed 完全不動作。"""
        # 試撮 tick 不是真成交 — 集合競價時段 (8:30-9:00) 富邦會推 isTrial=true 的
        # 模擬撮合 tick;誤當首筆成交會在競價時段狂下市價單被拒 (2026-08-10 2491 事故)
        if _pick(trade_data, "isTrial"):
            return
        # 逐筆撮合開始前不可能有真成交 — isTrial 欄位缺失時的雙保險
        if datetime.now().time() < self._trade_start_time:
            return
        price = float(_pick(trade_data, "price") or 0)
        if price <= 0:
            return
        # 首筆真成交 (isTrial 已濾、逐筆已開) → 標記該檔「已開盤」→ filter 退場,不再誤判開盤市價列
        # (2026-08-27 8105/1312;成交與 book 同一 socket、成交先到 → 開盤後 book 進來時旗標已立)
        if self.state is not None:
            self.state.mark_opened(symbol)
        # 搶市價單優先 (速度至上): 開盤訊號一到 → 盲送送最後一筆後停 (不再判首筆量門檻)
        self.session.on_first_trade(symbol)
        # (隔日賣不再由成交觸發 — 2026-08-16 改純 book 驅動,見 _monitor_on_book
        #  → session.update_overnight_book;今日活躍閘門在 session 端)

    # ─── 隔日賣標的檔案 (跨日持久化;固定檔名,非日期戳) ───────

    def _overnight_file(self) -> Path:
        return Path(__file__).parent / "output" / "overnight_holdings.json"

    def _load_overnight_file(self, output_dir: Path = None):
        """讀存的持倉 → session.load_overnight;連線中則順便對帳庫存。

        兩處呼叫: (1) 早上啟動載入昨日清單 (2) 收盤即把今日成交載回顯示。
        output_dir 未用到 (實際讀 self._overnight_file());保留參數相容既有呼叫。
        """
        import json as _json
        f = self._overnight_file()
        if not f.exists():
            return
        try:
            with self._overnight_file_lock:
                with f.open(encoding="utf-8") as fh:
                    data = _json.load(fh)
                items = data.get("holdings", []) if isinstance(data, dict) else data
                self.session.load_overnight(items)
            # REST 對帳放鎖外 — 不讓 broker 查詢佔住檔案鎖 (broker 連線中才有效)
            self.session.refresh_overnight_inventory()
        except Exception as e:
            logger.warning(f"[runner] 讀隔日賣清單失敗: {e}")

    def _write_overnight_file(self):
        """收盤把今日持倉 (filled>0 未出場) 存成隔日賣清單。空則清空檔案。

        temp + os.replace **原子寫** — 這檔是隔天早上唯一的賣單依據,"w" 直接截斷
        寫到一半掛掉 = 清單毀掉;多寫入端 (13:24 主流程 + 前端 add/remove) 共用檔案鎖。"""
        import json as _json
        with self._overnight_file_lock:
            try:
                holdings = self.session.get_overnight_candidates()
                payload = {
                    "saved_at": datetime.now().isoformat(timespec="seconds"),
                    "holdings": holdings,
                }
                f = self._overnight_file()
                f.parent.mkdir(exist_ok=True)   # 全新環境經 API add/remove 寫入時 output/ 可能還沒建
                tmp = f.parent / (f.name + ".tmp")
                with tmp.open("w", encoding="utf-8") as fh:
                    _json.dump(payload, fh, ensure_ascii=False, indent=2)
                # Windows: 目標被外部讀取端短暫佔用時 os.replace 會 PermissionError
                # (Linux rename 無此問題) — 短重試;最後一次失敗交外層 except 記 warning,
                # 殘留的 tmp 下次寫入會直接覆寫,無害。
                import time as _t
                for _retry in range(4):
                    try:
                        os.replace(tmp, f)
                        break
                    except PermissionError:
                        _t.sleep(0.05)
                else:
                    os.replace(tmp, f)
                logger.warning(f"[runner] 隔日賣清單已存: {len(holdings)} 檔 "
                               f"{[h['symbol'] for h in holdings]}")
            except Exception as e:
                logger.warning(f"[runner] 存隔日賣清單失敗: {e}")

    def _append_positions_history(self):
        """每日收盤 append 一行當日持倉快照到 positions_history.jsonl (不覆蓋,累積歷史台帳)。

        與 overnight_holdings.json (每天覆蓋) 不同 — 這支保留每天的持倉,供事後 P&L/檢討。
        """
        import json as _json
        try:
            holdings = self.session.get_overnight_candidates()
            line = {
                "date": datetime.now().strftime("%Y-%m-%d"),
                "saved_at": datetime.now().isoformat(timespec="seconds"),
                "holdings": holdings,
            }
            f = Path(__file__).parent / "output" / "positions_history.jsonl"
            f.parent.mkdir(exist_ok=True)
            with f.open("a", encoding="utf-8") as fh:
                fh.write(_json.dumps(line, ensure_ascii=False) + "\n")
            logger.warning(f"[runner] 持倉歷史 append: {len(holdings)} 檔 "
                           f"{[h['symbol'] for h in holdings]}")
        except Exception as e:
            logger.warning(f"[runner] 寫持倉歷史失敗: {e}")

    def track_overnight(self, symbol: str) -> bool:
        """前端手動加入一檔隔日賣標的: 加清單 + 盤中即時訂閱 + 立即寫回檔案 (持久化)。

        回 True=新加入、False=已在清單。
        """
        added = self.session.add_overnight(symbol)
        if self.subscriber:
            try:
                self.subscriber.add_symbol(symbol)
            except Exception as e:
                logger.warning(f"[runner] 手動加訂 {symbol} 失敗: {e}")
        self._write_overnight_file()   # 立即持久化,重啟仍在
        return added

    def untrack_overnight(self, symbol: str) -> bool:
        """前端移除一檔隔日賣標的 + 立即寫回檔案。回 True=有移除。"""
        removed = self.session.remove_overnight(symbol)
        self._write_overnight_file()
        return removed

    def _filter_low_volume(self):
        """月均量 < cfg.avg_volume_min_lots(張) 的股票盤前剔除 (縮母體省下游 REST)。
        資料來自 scripts/compute_avg_volume.py 夜間產的 input/avg_volume.json。
        fail-open (2026-08-24 定案): 檔缺/某檔無資料 → 不剔除,照常交易。門檻 0 → 停用。"""
        thr = self.cfg.avg_volume_min_lots
        if thr <= 0:
            self._avgvol = {"ran": False, "reason": "disabled", "threshold": thr}
            return
        import avg_volume as _av
        path = os.environ.get("AVG_VOLUME_FILE",
                              str(Path(__file__).parent / "input" / "avg_volume.json"))
        data, meta = _av.load(path)
        if not meta["exists"]:
            self._avgvol = {"ran": False, "reason": "file_missing", "meta": meta, "threshold": thr}
            logger.critical(f"[runner] ⚠ 月均量檔缺 ({path}) — 不做低量篩選 (fail-open),"
                            f"全母體照跑。請檢查 avg_volume timer")
            return
        if meta["stale"]:
            logger.critical(f"[runner] ⚠ 月均量檔非近期 (mtime {meta['mtime_date']}) — 照用,"
                            f"請檢查 avg_volume timer")
        universe_before = len(self.universe)
        kept, dropped = _av.filter_universe(self.universe, data, thr)
        # /api/avg-volume 顯示用 — 記今天實際跑的結果 (dropped_sample 限 100 檔避免整包)
        self._avgvol = {
            "ran": True, "reason": "ok", "meta": meta, "threshold": thr,
            "universe_before": universe_before, "kept": len(kept), "dropped": len(dropped),
            "dropped_sample": sorted(dropped)[:100],
        }
        logger.info(f"[runner] 月均量篩選: {universe_before} → {len(kept)} 檔 "
                    f"(剔除 {len(dropped)} 檔日均量 < {thr} 張;"
                    f"檔內 {meta['count']} 檔有量資料)")
        self.universe = kept

    def _load_t30_untradable(self):
        """載入 T30 禁單名單 → session (漲停價抓完後單次載入 — 正常流程此時已晚於
        timer 08:05 取檔)。檔案是舊的 → 照用 + CRITICAL;全缺 → 空名單無保護 +
        CRITICAL (使用者定案 2026-08-12)。"""
        import t30 as _t30
        t30_dir = os.environ.get("T30_DIR", str(Path(__file__).parent / "input" / "t30"))
        untradable, t30_meta = _t30.load_untradable(t30_dir)
        self._t30_meta = t30_meta        # /api/t30 顯示用 (檔案 ok/stale/missing)
        if t30_meta["missing_all"]:
            logger.critical(f"[runner] ⚠⚠ T30 檔案全缺 ({t30_dir}) — 今日**無全額交割名單保護**,"
                            f"全額交割股可能觸發狂送單!請檢查 hit-limit-up-t30.timer")
        else:
            stale = [n for n, i in t30_meta["files"].items() if i["ok"] and i["stale"]]
            if stale:
                logger.critical(f"[runner] ⚠ T30 檔非今日 ({', '.join(stale)}) — "
                                f"全額交割名單可能過時 (照用,請檢查取檔 timer)")
            logger.info(f"[runner] T30 禁單名單: {len(untradable)} 檔 (全額交割/需預收)")
        self.session.set_untradable(untradable)

    def abandon_symbol(self, symbol: str) -> bool:
        """前端「取消追蹤」(2026-08-12) — 停止該檔一切自動化 (含出場),使用者自負。
        場景: 某檔一直下單失敗 (如全額交割股) 的單檔煞車,比全域暫停精準。"""
        found = self.session.abandon_symbol(symbol)
        if self.trader is not None:
            from trader import Holding
            h = self.trader.holdings.get(symbol)
            if h is not None:
                h.status = Holding.ABANDONED
                h.pulled_reason = "manual_abandon"
                found = True
        return found

    def _start_pre_order_timer(self):
        """背景 thread: 等到 PRE_ORDER_TIME (08:59:58) → 對當下 marked 清單預掛限價單。
        現在時間已過 PRE_ORDER_TIME → 不掛 (只在正常 8:00 流程有效)。"""
        from datetime import time as _time_cls
        # 抓「本輪專屬」的 event 進 closure — timer 內只用這個 local,
        # 之後 start() 換新 event 也不會把舊 timer 復活 (防預掛重複下單)。
        stop_event = self._stop_event

        def _timer():
            try:
                hh, mm, ss = map(int, self.cfg.pre_order_time.split(":"))
                target = _time_cls(hh, mm, ss)
            except Exception:
                logger.warning(f"[runner] PRE_ORDER_TIME 格式錯 ({self.cfg.pre_order_time!r}) — 停用預掛")
                return
            if datetime.now().time() >= target:
                logger.info("[runner] 已過 PRE_ORDER_TIME — 跳過預掛")
                return
            # 精準等到時點 (最後 1 秒改 10ms 細顆粒,誤差 ~10ms)
            while not stop_event.is_set():
                now = datetime.now()
                remaining = (now.replace(hour=target.hour, minute=target.minute,
                                         second=target.second, microsecond=0) - now
                             ).total_seconds()
                if remaining <= 0:
                    break
                stop_event.wait(0.01 if remaining < 1 else remaining - 0.5)
            if stop_event.is_set():
                return
            # 量減半 final check — 一次性批次判 (2026-08-13 定案,取代逐 tick 判):
            # 最新委買量 < mark 以來最高量 × ratio → 刷掉;判完清單即定案。
            # 與逐 tick 淘汰共用 State 鎖 + unmark 冪等,不打架。
            if self.state is not None:
                for _sym, _last, _max in self.state.final_check_all():
                    logger.warning(f"[runner] ✗ final check {_sym} 量減半 "
                                   f"({_max} → {_last} 張) → 淘汰+退訂")
                    try:
                        self._unsub_symbol(_sym)
                    except Exception:
                        pass
            # 開盤即鎖 (first_tick) 優先下單+吃預算,再盤中鎖;各組內照代號
            marked = self.state.get_marked_prioritized() if self.state else []
            if not marked:
                logger.info("[runner] PRE_ORDER_TIME 到但 marked 清單空 — 不預掛")
                return
            # 先訂 trades — 9:00 才訂會漏掉開盤撮合那筆成交 (訂閱來回要百毫秒級,
            # 不重播)。此刻訂好,開盤 trade 會進 subscriber buffer,9:00 轉場後種子化。
            try:
                self.subscriber.subscribe_trades_for(marked)
            except Exception as e:
                logger.warning(f"[runner] 預掛時訂 trades 失敗: {e}")
            # 搶市價單: **現在**就掛上 on_trade — 9:00:00 開盤撮合 tick 一到就能
            # 瞬間觸發市價追,不等 9:00 轉場建 Trader (轉場要 1~3 秒,錯過最快進場時機)。
            # on_book 不動 (filter 的 unmark 規則到 9:00 前照跑)。
            try:
                self.subscriber.set_handlers(on_trade=self._monitor_on_trade)
            except Exception as e:
                logger.warning(f"[runner] 預掛時掛 on_trade 失敗: {e}")
            # 風控②母數: 08:59:58 當下「漲停價那一檔」的委託量快照 (張) → 20% 上限
            bid_vols = {}
            for _s in marked:
                _up = float(self.limit_ups.get(_s) or 0)
                _snap = self.subscriber.get_latest_snapshot(_s) if self.subscriber else None
                for _lv in ((_snap or {}).get("books") or {}).get("bids", []):
                    if _up and abs(float(_pick(_lv, "price") or 0) - _up) < 0.001:
                        bid_vols[_s] = int(_pick(_lv, "size") or 0)
                        break
            # node 兜底 (2026-09-01): subscriber 沒該檔 book (訂閱後零 tick) → 退用
            # Hub 快照最後已知量,20% cap 不失效 (standalone 此 dict 恆空 → 零行為變化)
            for _s, _v in getattr(self, "_node_bid_vol_fallback", {}).items():
                if _s not in bid_vols and _s in marked and _v > 0:
                    bid_vols[_s] = _v
            try:
                self.session.place_pre_orders(marked, self.limit_ups, stop_event,
                                              limit_up_bid_vols=bid_vols)
            except Exception as e:
                logger.exception(f"[runner] 預掛例外: {e}")
            # 封口 sweep: 下單期間被 tick 淘汰的檔,其撤單當時可能 no-op (單還沒掛)
            # → 這裡補撤 (沒單的檔 cancel 自然 no-op)。sweep 之後才淘汰的,
            # 由淘汰自身的撤單路徑處理 (st 已存在) — 兩側無縫覆蓋。
            self._sweep_unmarked_pre_orders(marked)
            # 市價盲送搶進 (時間驅動,不等首筆成交 tick;首筆委託成功即停)。開始時點 08:59:59
            # (早於 09:00 開盤暖機:cadence 先跑,集合競價期市價被拒→續送,一開盤第一筆最快排到)。
            # ⚠ 注意這是「盲送開始」,非「首筆成交 gate」— 後者 (_monitor_on_trade) 仍守 _trade_start_time=09:00。
            try:
                chase_start = self._parse_time_hhmm(self.cfg.market_chase_start) or dtime(8, 59, 59)
                cutoff = self._parse_time_hhmm(self.cfg.market_chase_cutoff) or dtime(9, 3)
                self.session.start_market_chase(marked, chase_start, cutoff)
                logger.info(f"[runner] 市價盲送已排程 — {len(marked)} 檔,"
                            f"{chase_start}~{cutoff}")
            except Exception as e:
                logger.exception(f"[runner] 啟動市價盲送失敗: {e}")

        t = threading.Thread(target=_timer, name="pre-order-timer", daemon=True)
        t.start()
        self._timer_threads.append(t)

    def _sweep_unmarked_pre_orders(self, symbols):
        """預掛完成後的封口對帳 — 下單進行中被 tick 淘汰的檔,淘汰當下的撤單是
        no-op (單還沒掛出),這裡補撤。與逐 tick 淘汰路徑兩側覆蓋,無時間縫隙。"""
        for sym in symbols:
            try:
                if self.state is not None and self.state.is_discarded(sym):
                    self.session.cancel_symbol_orders(sym, "unmarked_during_preorder")
            except Exception as e:
                logger.error(f"[runner] 預掛後 sweep {sym} 補撤例外: {e}")

    def _start_cancel_pending_timer(self):
        """背景 thread: CANCEL_PENDING_TIME (13:23) 到 → 撤所有未成交委託 (持倉不動)。"""
        from datetime import time as _time_cls
        stop_event = self._stop_event    # 本輪專屬 (同 pre-order timer,防舊 thread 復活)

        def _timer():
            try:
                hh, mm, ss = map(int, self.cfg.cancel_pending_time.split(":"))
                target = _time_cls(hh, mm, ss)
            except Exception:
                logger.warning(f"[runner] CANCEL_PENDING_TIME 格式錯 "
                               f"({self.cfg.cancel_pending_time!r}) — 停用 13:23 撤單")
                return
            if datetime.now().time() >= target:
                return
            while not stop_event.is_set():
                if datetime.now().time() >= target:
                    break
                stop_event.wait(1)
            if stop_event.is_set():
                return
            try:
                self.session.cancel_all_pending("cancel_pending_time")
            except Exception as e:
                logger.exception(f"[runner] 13:23 撤單例外: {e}")

        t = threading.Thread(target=_timer, name="cancel-pending-timer", daemon=True)
        t.start()
        self._timer_threads.append(t)

    def _live_subscribe_loop(self):
        """LIVE_SUBSCRIBE — 保持 subscriber 常駐，直到 stop event 或 TRADING_END_TIME.
        subscriber 內部 rotation loop 會持續跑 (每 30s 換一批 subscribe)，這裡只是
        主 thread 停在這裡等 stop signal，避免 runner main thread 結束把 subscriber 一起收掉。
        """
        from datetime import time as _time
        import time as _t
        # 直到 TRADING_END_TIME (預設 13:24) 或 stop event
        end_hh, end_mm, end_ss = map(int, self.cfg.trading_end_time.split(":"))
        end_target = _time(end_hh, end_mm, end_ss)
        last_log = 0
        while not self._stop_event.is_set():
            now = datetime.now().time()
            if now >= end_target:
                logger.info(f"[runner] TRADING_END_TIME {self.cfg.trading_end_time} 到 — LIVE_SUBSCRIBE 結束")
                return
            # 每 60s 印一次 heartbeat (跟 subscriber heartbeat 分開，更 high-level)
            if _t.time() - last_log >= 60:
                stats = self.subscriber.get_tick_stats() if self.subscriber else {}
                logger.info(f"[runner LIVE] tick 累計 books={stats.get('books_count',0)} "
                            f"trades={stats.get('trades_count',0)}")
                last_log = _t.time()
            self._stop_event.wait(1)

    def _load_or_fetch_limit_ups(self) -> Dict[str, float]:
        """有當日 cache → 秒讀，否則抓完寫 cache. cache path = output/YYYY-MM-DD_limit_ups.json"""
        import json as _json
        output_dir = Path(__file__).parent / "output"
        output_dir.mkdir(exist_ok=True)
        today = datetime.now().strftime("%Y-%m-%d")
        cache_file = output_dir / f"{today}_limit_ups.json"
        disp_file = output_dir / f"{today}_dispositions.json"
        down_file = output_dir / f"{today}_limit_downs.json"
        dt_file = output_dir / f"{today}_day_tradable.json"

        # 讀 cache
        if cache_file.exists():
            try:
                with cache_file.open(encoding="utf-8") as f:
                    cached = _json.load(f)
                if isinstance(cached, dict) and len(cached) > 0:
                    logger.info(f"[runner] 讀 cache limit_ups: {cache_file.name} "
                                f"({len(cached)} 檔) — 跳過 REST 抓取")
                    self._limit_up_progress = {
                        "done": len(cached), "total": len(cached),
                        "ok": len(cached), "fail": 0,
                    }
                    # 處置股 cache (best-effort;缺 → 全視為非處置股,安全預設)
                    if disp_file.exists():
                        try:
                            with disp_file.open(encoding="utf-8") as f:
                                self.dispositions = {k: bool(v) for k, v in _json.load(f).items()}
                        except Exception:
                            pass
                    # 跌停價 cache (best-effort;缺 → 出場走市價賣兜底)
                    if down_file.exists():
                        try:
                            with down_file.open(encoding="utf-8") as f:
                                self.limit_downs = {k: float(v) for k, v in _json.load(f).items()}
                        except Exception:
                            pass
                    # 禁現沖 cache (best-effort;缺 → 全視為可現沖,減半風控①不觸發。
                    # day_tradable 只在 _query_limit_up 填 — cache hit 不補讀的話
                    # hub 盤中重啟後快照 day_tradable 欄全預設 True)
                    if dt_file.exists():
                        try:
                            with dt_file.open(encoding="utf-8") as f:
                                self.day_tradable = {k: bool(v) for k, v in _json.load(f).items()}
                        except Exception:
                            pass
                    return {k: float(v) for k, v in cached.items()}
            except Exception as e:
                logger.warning(f"[runner] cache 讀失敗 ({e})，改走 REST 重抓")

        # cache miss → 抓 (含重試補齊) + 存 (self.dispositions 在 _query_limit_up 內填)
        logger.info(f"[runner] 無當日 cache，開始抓 {len(self.universe)} 檔漲停價 "
                    f"(盤前分批上架，會重試補齊到 {self.cfg.limit_up_fetch_deadline})...")
        result = self._fetch_limit_ups_with_progress()

        if result:
            try:
                with cache_file.open("w", encoding="utf-8") as f:
                    _json.dump(result, f, ensure_ascii=False, indent=2)
                n_disp = sum(1 for v in self.dispositions.values() if v)
                with disp_file.open("w", encoding="utf-8") as f:
                    _json.dump(self.dispositions, f, ensure_ascii=False, indent=2)
                with down_file.open("w", encoding="utf-8") as f:
                    _json.dump(self.limit_downs, f, ensure_ascii=False, indent=2)
                with dt_file.open("w", encoding="utf-8") as f:
                    _json.dump(self.day_tradable, f, ensure_ascii=False, indent=2)
                logger.info(f"[runner] cache 已寫 {cache_file.name} ({len(result)} 檔,"
                            f"其中處置股 {n_disp} 檔,跌停價 {len(self.limit_downs)} 檔,"
                            f"禁現沖 {sum(1 for v in self.day_tradable.values() if not v)} 檔)")
            except Exception as e:
                logger.warning(f"[runner] cache 寫失敗: {e}")
        return result

    def _fetch_limit_ups_with_progress(self) -> Dict[str, float]:
        """抓每檔當日漲停價，更新 progress 讓 API/前端讀。

        兩個關鍵設計:
        1. **節流** — 富邦日內行情 REST 限 300/min。之前一趟 1943 檔 130 秒打完 (~900/min) 撞限流，
           配額榨乾後後段全被擋 (回空 response)，只成功 746 檔。故每次呼叫間插 sleep，
           把速率壓到 ≤ cfg.limit_up_max_per_min (預設 250)。
        2. **重試到截止** — 對還沒抓到的股票反覆重試 (含被限流擋掉的、盤前才上架的)，
           直到全部補齊，或時間到 LIMIT_UP_FETCH_DEADLINE (盤前試撮 8:30 前留 buffer)。
        """
        stock = self.sdk.marketdata.rest_client.stock
        total = len(self.universe)
        deadline = self._parse_time_hhmm(self.cfg.limit_up_fetch_deadline)
        # 節流間隔: 每次呼叫後等這麼久，把速率壓在上限內
        throttle_sec = 60.0 / max(1, self.cfg.limit_up_max_per_min)
        logger.info(f"[runner] 漲停價抓取節流 ≤{self.cfg.limit_up_max_per_min}/min "
                    f"(每次呼叫間隔 {throttle_sec:.3f}s)")

        result: Dict[str, float] = {}
        missing = list(self.universe)
        attempt = 0

        while missing and not self._stop_event.is_set():
            attempt += 1
            still_missing: list = []
            for sym in missing:
                if self._stop_event.is_set():
                    break
                up = self._query_limit_up(stock, sym)
                if up:
                    result[sym] = up
                else:
                    still_missing.append(sym)
                # progress: done 在單趟內從 已成功數 長到 total
                self._limit_up_progress["ok"] = len(result)
                self._limit_up_progress["fail"] = total - len(result)
                self._limit_up_progress["done"] = len(result) + len(still_missing)
                # 節流 (可被 stop_event 立即中斷)
                self._stop_event.wait(throttle_sec)
            missing = still_missing
            logger.info(f"[runner] 漲停價抓取 第 {attempt} 趟 — 成功 {len(result)}/{total}，"
                        f"還缺 {len(missing)}")

            if not missing or self._stop_event.is_set():
                break
            # 到抓取截止時間就停止補齊 (盤前試撮前)
            if deadline is not None and datetime.now().time() >= deadline:
                logger.warning(f"[runner] 到抓取截止 {self.cfg.limit_up_fetch_deadline}，"
                               f"仍缺 {len(missing)} 檔 → 停止重試 (今日監控 {len(result)} 檔)")
                break
            logger.info(f"[runner] {_LIMIT_UP_RETRY_SLEEP_SEC}s 後重試剩餘 {len(missing)} 檔 "
                        f"(等交易所繼續上架漲停價)...")
            self._stop_event.wait(_LIMIT_UP_RETRY_SLEEP_SEC)

        self._limit_up_progress["done"] = total
        self._limit_up_progress["ok"] = len(result)
        self._limit_up_progress["fail"] = total - len(result)
        return result

    def _query_limit_up(self, stock, sym: str):
        """查單檔漲停價 — 回 float 或 None (空值/例外都回 None，交給重試補)。
        順便記 isDisposition (處置股,下單機制用) 與 limitDownPrice (跌停價,
        出場跌停限價賣用,2026-08-12 — 同一個回應,零額外 REST 成本)。"""
        try:
            resp = stock.intraday.ticker(symbol=sym)
            up = resp.get("limitUpPrice") or resp.get("limit_up")
            if up:
                self.dispositions[sym] = bool(resp.get("isDisposition"))
                # canDayTrade=False = 禁現沖 → 下單減半 (風控①,官方 key 是 canDayTrade)
                self.day_tradable[sym] = bool(resp.get("canDayTrade"))
                down = resp.get("limitDownPrice")
                if down:
                    self.limit_downs[sym] = float(down)
            return float(up) if up else None
        except Exception:
            return None

    @staticmethod
    def _parse_time_hhmm(s: str):
        """'08:28' / '08:28:00' → datetime.time；格式錯回 None (不啟用截止)。"""
        from datetime import time as _time
        try:
            parts = [int(x) for x in s.split(":")]
            while len(parts) < 3:
                parts.append(0)
            return _time(parts[0], parts[1], parts[2])
        except Exception:
            logger.warning(f"[runner] LIMIT_UP_FETCH_DEADLINE 格式錯 ({s!r}) — 不啟用重試截止")
            return None
