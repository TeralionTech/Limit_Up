"""Runner — 把 filter.py 主流程包成 background thread，讓 FastAPI/APScheduler 觸發。

單一 process 內只有 1 個 Runner instance (single-instance guard)。
外部 (API endpoint) 透過 runner 讀 state / progress / holdings。
"""
from __future__ import annotations

import logging
import os
import threading
from datetime import datetime, time as dtime
from enum import Enum
from pathlib import Path
from typing import Dict, Optional

from config import Config, load_config
from trader import _first_priced, _pick   # 輕量純函式 (monitor handler 每 tick 用,不 lazy import)

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
        # API runtime 調參 (例 first_trade_min_lots) — 蓋過 .env，重啟 runner 也保留
        self._param_overrides: Dict[str, object] = {}

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

    def set_param_override(self, key: str, value):
        """API runtime 調參 — 立即生效 (cfg 是 trader 共用的同一物件)。"""
        self._param_overrides[key] = value
        if self.cfg is not None:
            setattr(self.cfg, key, value)
        logger.info(f"[runner] param override: {key} = {value}")

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
        # 讀 config + 套 API runtime overrides
        self.cfg = load_config()
        for k, v in self._param_overrides.items():
            setattr(self.cfg, k, v)
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

        # Phase 2: Universe
        self.phase = Phase.FETCH_UNIVERSE
        from universe import get_universe, fetch_limit_ups
        self.universe = get_universe(self.sdk, self.cfg.universe)
        if not self.universe:
            raise RuntimeError("universe 空")

        # Phase 3: Limit ups (慢 — 12-15 分鐘) — 有當日 cache 就秒讀
        self.phase = Phase.FETCH_LIMIT_UPS
        self._limit_up_progress = {"done": 0, "total": len(self.universe), "ok": 0, "fail": 0}
        self.limit_ups = self._load_or_fetch_limit_ups()
        if not self.limit_ups:
            raise RuntimeError("limit_ups 全 fail")
        # 把處置股名單交給交易會話 (下單機制: 處置股不撤預掛/不下市價買、出場改委買一價賣)
        self.session.set_dispositions(self.dispositions)

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

        # 載入隔日賣清單 (昨天買到未出場的持倉);把它們也放進訂閱母體,
        # 8:30 起就收得到五檔+成交 (隔日賣標的可能不在漲停母體裡)
        self._load_overnight_file(output_dir)
        overnight_syms = self.session.overnight_symbols()
        # 隔日賣標的補查跌停價 (出場=跌停價限價賣;它們可能不在今日 limit_ups 抓取範圍)
        _stock = self.sdk.marketdata.rest_client.stock
        for _s in overnight_syms:
            if _s not in self.limit_downs:
                self._query_limit_up(_stock, _s)
        # 跌停價/處置股名單交給交易會話 (所有出場含隔日賣 = 跌停價限價賣,2026-08-12 定案)
        self.session.set_limit_downs(self.limit_downs)
        self.session.set_dispositions(self.dispositions)   # 補查後含隔日賣標的的處置股資訊
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

        # ── 分岔: SKIP_TRADER 決定進 LIVE_SUBSCRIBE 或 TRADING ──

        if self.cfg.skip_trader:
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

    def _monitor_on_book(self, symbol: str, bids: list, asks: list):
        """9:00 後的看盤 handler — **不做 mark/unmark** (filter 規則對盤中 price=0
        市價列會誤判)。只更新隔日賣標的的委買/委賣一價 (算賣價用)。"""
        if self.session.has_overnight(symbol):
            self.session.update_overnight_book(
                symbol, _first_priced(bids), _first_priced(asks))

    def _monitor_on_trade(self, symbol: str, trade_data):
        """monitor 期間的 trades handler — **08:59:58 就掛上** (見 _start_pre_order_timer),
        9:00:00 開盤撮合 tick 一到就觸發,不等 9:00 轉場建 Trader (轉場要 1~3 秒)。

        1. 今日標的首筆成交 → 瞬間觸發市價追 (session.on_first_trade;
           冪等旗標在 session 內 — 重複 tick / trader 接手後都不會重複下單)
        2. 隔日賣標的收到成交 → 觸發隔日賣 (今日活躍標的不觸發,尊重「只賣非今日搶單」)
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
        # 搶市價單優先 (速度至上): 有些 API 給股數、有的給張數 — >=1000 視為股數換算
        qty = int(_pick(trade_data, "size") or _pick(trade_data, "qty") or 0)
        lots = qty // 1000 if qty >= 1000 else qty
        self.session.on_first_trade(symbol, lots >= self.cfg.first_trade_min_lots)
        # 隔日賣觸發 — 該檔今日仍活躍 (有下單且未淘汰未出場) 時不觸發
        if self.session.has_overnight(symbol):
            stt = self.session.get_symbol_state(symbol)
            active_today = bool(stt) and not stt["stopped_reason"] and not stt["exited"]
            if not active_today:
                self.session.on_overnight_first_trade(symbol)

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
            try:
                self.session.place_pre_orders(marked, self.limit_ups, stop_event)
            except Exception as e:
                logger.exception(f"[runner] 預掛例外: {e}")

        t = threading.Thread(target=_timer, name="pre-order-timer", daemon=True)
        t.start()
        self._timer_threads.append(t)

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
                logger.info(f"[runner] cache 已寫 {cache_file.name} ({len(result)} 檔,"
                            f"其中處置股 {n_disp} 檔,跌停價 {len(self.limit_downs)} 檔)")
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
