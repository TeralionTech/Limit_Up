"""Runner — 把 filter.py 主流程包成 background thread，讓 FastAPI/APScheduler 觸發。

單一 process 內只有 1 個 Runner instance (single-instance guard)。
外部 (API endpoint) 透過 runner 讀 state / progress / holdings。
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, Optional

from config import Config, load_config

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

    # ─── 對外 API ────────────────────────────────────────────

    def start(self) -> bool:
        """啟動主流程 background thread。回 True = 已啟動 / False = 已在跑."""
        if self._main_thread and self._main_thread.is_alive():
            logger.warning("[runner] 已在跑，忽略 start()")
            return False
        self._stop_event.clear()
        self.phase = Phase.IDLE
        self.error_msg = ""
        self._started_at = datetime.now()
        self._main_thread = threading.Thread(target=self._main, name="runner-main", daemon=True)
        self._main_thread.start()
        logger.info("[runner] main thread started")
        return True

    def stop(self):
        """通知主流程停止 + 平倉 (若已進 trading 階段)."""
        logger.info("[runner] stop() 被呼叫")
        self._stop_event.set()
        # 各元件單獨 stop (順序: trader → subscriber → recorder → order_client)
        if self.trader:
            try:
                self.trader.stop(do_final_close=True)
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
            "watchlist_size": len([1 for s in (self.state.marked if self.state else set())]),
            "recorder_tick_count": self.recorder.count() if self.recorder else 0,
        }

    # ─── 主流程 ─────────────────────────────────────────────

    def _main(self):
        """runner main — 從 login 到 trading 收盤."""
        try:
            self._run_all_phases()
        except Exception as e:
            logger.exception(f"[runner] 主流程例外: {e}")
            self.phase = Phase.ERROR
            self.error_msg = str(e)

    def _run_all_phases(self):
        # 讀 config + 套 API runtime overrides
        self.cfg = load_config()
        for k, v in self._param_overrides.items():
            setattr(self.cfg, k, v)

        # 每日重置交易 state (新交易日 → 清前日 trades/委託表/預算 + armed=False;
        # 同日手動重啟 → 保留當日 state。連線不動。)
        self.session.roll_day(datetime.now().strftime("%Y-%m-%d"))

        # 交易會話策略參數 (500 上限 / 送單間隔 / 13:23 撤單時點)
        self.session.configure(
            max_stock_price=self.cfg.max_stock_price,
            order_min_interval_sec=self.cfg.order_min_interval_sec,
            cancel_pending_time=self.cfg.cancel_pending_time,
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
            self.subscriber.request_unsubscribe(symbol)
            try:
                self.session.cancel_symbol_orders(symbol, "unmarked")
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
            unsub_fn=self.subscriber.request_unsubscribe,       # + 退訂
            session=self.session,                               # 交易會話 (sim = 純監控)
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
                if _lt and _lt.get("price"):
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
        self.trader.stop()
        self.subscriber.stop()
        self.recorder.close()
        self.phase = Phase.FINISHED
        logger.info("[runner] 收盤結束")

    # ─── 隔日賣標的檔案 (跨日持久化;固定檔名,非日期戳) ───────

    def _overnight_file(self) -> Path:
        return Path(__file__).parent / "output" / "overnight_holdings.json"

    def _load_overnight_file(self, output_dir: Path):
        """讀昨日存的持倉 → session.load_overnight;連線中則順便對帳庫存。"""
        import json as _json
        f = self._overnight_file()
        if not f.exists():
            return
        try:
            with f.open(encoding="utf-8") as fh:
                data = _json.load(fh)
            items = data.get("holdings", []) if isinstance(data, dict) else data
            self.session.load_overnight(items)
            self.session.refresh_overnight_inventory()   # broker 連線中才有效
        except Exception as e:
            logger.warning(f"[runner] 讀隔日賣清單失敗: {e}")

    def _write_overnight_file(self):
        """收盤把今日持倉 (filled>0 未出場) 存成隔日賣清單。空則清空檔案。"""
        import json as _json
        try:
            holdings = self.session.get_overnight_candidates()
            payload = {
                "saved_at": datetime.now().isoformat(timespec="seconds"),
                "holdings": holdings,
            }
            with self._overnight_file().open("w", encoding="utf-8") as fh:
                _json.dump(payload, fh, ensure_ascii=False, indent=2)
            logger.warning(f"[runner] 隔日賣清單已存: {len(holdings)} 檔 "
                           f"{[h['symbol'] for h in holdings]}")
        except Exception as e:
            logger.warning(f"[runner] 存隔日賣清單失敗: {e}")

    def _start_pre_order_timer(self):
        """背景 thread: 等到 PRE_ORDER_TIME (08:59:58) → 對當下 marked 清單預掛限價單。
        現在時間已過 PRE_ORDER_TIME → 不掛 (只在正常 8:00 流程有效)。"""
        from datetime import time as _time_cls

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
            while not self._stop_event.is_set():
                now = datetime.now()
                remaining = (now.replace(hour=target.hour, minute=target.minute,
                                         second=target.second, microsecond=0) - now
                             ).total_seconds()
                if remaining <= 0:
                    break
                self._stop_event.wait(0.01 if remaining < 1 else remaining - 0.5)
            if self._stop_event.is_set():
                return
            marked = self.state.get_marked_list() if self.state else []
            if not marked:
                logger.info("[runner] PRE_ORDER_TIME 到但 marked 清單空 — 不預掛")
                return
            # 先訂 trades — 9:00 才訂會漏掉開盤撮合那筆成交 (訂閱來回要百毫秒級,
            # 不重播)。此刻訂好,開盤 trade 會進 subscriber buffer,9:00 轉場後種子化。
            try:
                self.subscriber.subscribe_trades_for(marked)
            except Exception as e:
                logger.warning(f"[runner] 預掛時訂 trades 失敗: {e}")
            try:
                self.session.place_pre_orders(marked, self.limit_ups, self._stop_event)
            except Exception as e:
                logger.exception(f"[runner] 預掛例外: {e}")

        threading.Thread(target=_timer, name="pre-order-timer", daemon=True).start()

    def _start_cancel_pending_timer(self):
        """背景 thread: CANCEL_PENDING_TIME (13:23) 到 → 撤所有未成交委託 (持倉不動)。"""
        from datetime import time as _time_cls

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
            while not self._stop_event.is_set():
                if datetime.now().time() >= target:
                    break
                self._stop_event.wait(1)
            if self._stop_event.is_set():
                return
            try:
                self.session.cancel_all_pending("cancel_pending_time")
            except Exception as e:
                logger.exception(f"[runner] 13:23 撤單例外: {e}")

        threading.Thread(target=_timer, name="cancel-pending-timer", daemon=True).start()

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
                logger.info(f"[runner] cache 已寫 {cache_file.name} ({len(result)} 檔,"
                            f"其中處置股 {n_disp} 檔)")
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
        順便把 isDisposition (處置股) 記進 self.dispositions (下單機制要用)。"""
        try:
            resp = stock.intraday.ticker(symbol=sym)
            up = resp.get("limitUpPrice") or resp.get("limit_up")
            if up:
                self.dispositions[sym] = bool(resp.get("isDisposition"))
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
