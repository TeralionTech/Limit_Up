"""Trader — 9:00 開盤後監控 (做多);session 為 real+armed 時同步掛真單。

兩階段 (對應前端模擬/真實執行頁兩區塊):

  第一盤檢查 (block 1) — 每檔 watchlist 股票收「開盤第一筆」:
    - 首筆 books: 委賣一出現單子 → unmark 丟棄 (+ 退訂 + 撤委託)
    - 首筆 trades: 立開盤訊號旗標 → 進入盤中追蹤 (2026-08-29 移除首筆量門檻)
      [real] 首筆成交到達時: 預掛單未(全)成交 → 撤剩餘留已成交;盲送市價單追差額

  盤中追蹤 (block 2) — 通過第一盤的標的:
    - 顯示 委買一價 / 委買一量
    - 出場 (唯一動作,2026-08-19 定案;「委買量 tick 間減半 → 撤單」已移除 —
      單子一定會被成交,不用管撤單):
        支撐隊伍消失 → 立刻出場 — 一般股 = 市價買隊伍曾出現後歸零;
        處置股 (名單分流,禁市價單) = 漲停價買單消失
        [real] 撤委託 + **跌停價限價賣**全部已成交;狀態標 "出場" (pulled) 供前端顯示
      (「委買一跌下漲停」條件早已移除 — 盤中市價單佔五檔第一列且 price=0 會誤判)
    - 警示 (不動作): 委買一量每 BID_DECLINE_SAMPLE_SEC 取樣,
      連續 BID_DECLINE_MINUTES 分鐘遞減 → warning 顯示於前端
    - 出場後**持續收資料** (不退訂)

session=None (模擬模式) = 純監控,零下單 — 行為與加入交易功能前完全相同。
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Optional

from pull_runtime import PullEvents, PullSignalWriter, PullStream, exchange_time, trading_day
from pull_signal import PullExitConfig

logger = logging.getLogger(__name__)


class Holding:
    """單股 tracking state."""

    # status 值
    WAITING = "waiting"                  # 等第一盤資料
    DISCARDED_FIRST = "discarded_first"  # 第一盤檢查淘汰
    TRACKING = "tracking"                # 追蹤中
    PULLED = "pulled"                    # 撤單 (仍持續收資料)
    ABANDONED = "abandoned"              # 使用者取消追蹤 (一切自動化停止,自負)

    def __init__(self, symbol: str, limit_up: float):
        self.symbol = symbol
        self.limit_up = limit_up
        self.status = self.WAITING
        # 第一盤資料
        self.first_books_seen = False
        self.first_books: dict = {}      # {bid1_price, bid1_size, ask1_price, ask1_size, ts}
        self.first_trade_seen = False
        self.first_trade: dict = {}      # {price, qty, lots, ts}
        self.first_fail_reason = ""      # 非空 = 第一盤淘汰原因
        # 盤中追蹤
        self.pulled_reason = ""          # 出場原因 (mkt_queue_gone / limit_up_bid_gone / manual_abandon)
        self.support_seen = False        # 支撐隊伍出現過 (出場訊號「沒有了」的轉移語意)
        self.trade_count = 0             # 累計成交筆數 (統計)
        # 警示: 委買量持續遞減 (每 sample_sec 取樣,連續 decline_minutes 分鐘遞減 → warning)
        self.warning = ""
        self._sample_ts = 0.0            # 上次取樣時間 (epoch)
        self._elim_retry_ts = 0.0        # 淘汰後出場意圖未完成時,上次用 tick 重試 exit_position 的時間 (epoch)
        self._sample_size: Optional[int] = None
        self._decline_count = 0
        self.last_bid1_price = 0.0
        self.last_bid1_size = 0
        self.last_ask1_price = 0.0
        self.last_ask1_size = 0
        self.last_trade_price = 0.0
        self.last_book_ts = ""
        self.pull_signal = None
        self.pull_stream = None
        self._pull_open_day = None
        self._pull_retry_mono = 0.0


class Trader:
    """9:00 之後的主控。"""

    def __init__(self, watchlist, limit_ups, cfg,
                 recorder=None, state=None, unsub_fn: Optional[Callable] = None,
                 session=None, dispositions: Optional[dict] = None,
                 subscriber=None, pull_writer=None):
        """
        watchlist: List[str] — filter 結束時的 marked 名單
        limit_ups: Dict[str, float] — 每檔漲停價
        cfg: config.Config
        state: state.State — 第一盤淘汰要同步 unmark (可 None)
        unsub_fn: callable(symbol) — 第一盤淘汰要退訂 (可 None)
        session: trading_session.TradingSession — real+armed 時同步掛真單 (可 None = 純監控)
        dispositions: Dict[str, bool] — 處置股名單 (量減半基準用限價列;可 None)
        """
        self.holdings: Dict[str, Holding] = {
            s: Holding(s, limit_ups.get(s, 0.0)) for s in watchlist
        }
        self.cfg = cfg
        self.dispositions: dict = dispositions or {}
        self.recorder = recorder
        self.state = state
        self.unsub_fn = unsub_fn
        self.session = session
        self.subscriber = subscriber
        self._pull_lock = threading.RLock()
        self._pull_events = PullEvents()
        self._pull_thread = None
        output_dir = getattr(cfg, "output_dir", None) or Path(__file__).parent / "output"
        self._pull_writer = pull_writer or PullSignalWriter(output_dir)
        self._stop = threading.Event()
        live = bool(session and session.is_live())
        logger.info(f"[trader] 建立 — {'真實交易' if live else '純監控'} {len(self.holdings)} 檔")

    # ─── 對外 API ──────────────────────────────────────────

    def start(self):
        """無獨立 thread — 邏輯全在 on_book/on_trade handlers (下單動作交給 session)。"""
        logger.info("[trader] 啟動 (下單與否由 session mode/armed 決定)")
        self._pull_thread = threading.Thread(target=self._pull_loop, name="pull-evaluator", daemon=True)
        self._pull_thread.start()

    def stop(self):
        self._stop.set()
        if self._pull_thread is not None:
            self._pull_thread.join(timeout=2)
        with self._pull_lock:
            self._pull_writer.close()

    # ─── 掛 subscriber 的 handlers ─────────────────────────

    def on_book(self, symbol: str, bids: list, asks: list, is_continuous: bool = False,
                *, timestamp=None, received_at=None, received_mono=None, feed_epoch=None):
        # is_continuous: subscriber 傳來的交易所「已逐筆」旗標 — trader 不需要 (它本就是開盤後
        # 的 handler、盤中規則已防市價列),簽名對齊 subscriber 呼叫即可。
        h = self.holdings.get(symbol)
        if h is not None:
            mkt_lots = (int(_pick(bids[0], "size") or 0)
                        if bids and float(_pick(bids[0], "price") or 0) <= 0 else 0)
            self._pull_events.put("book", (h, mkt_lots, timestamp, feed_epoch),
                                  wall=received_at, mono=received_mono)
        # 隔日賣標的: 只要在清單裡就轉送 book (顯示+算賣價),賣出規則在 session 端
        # (純盤面無狀態: 委買一跌下今日漲停 → 賣;市價列在/買牆在 = 鎖著 → 抱)。
        # 即使它同時是今日標的 (今日出場後仍要能賣昨天的量)。
        if self.session is not None and self.session.has_overnight(symbol):
            self.session.update_overnight_book(
                symbol, *_overnight_book_fields(bids, asks))
        if h is None:
            return

        # 委買一拆三用途 (2026-08-04 定案 — 市價彙總列 price=0 佔第一列的處理):
        #   顯示 = 原始第一列 (市價排隊時顯示 0 × 彙總量,忠實盤面)
        #   規則 = 一般股**只看市價列** (搶漲停的積極買盤;限價列波動不觸發撤單)
        #          處置股 = 限價列 (處置股禁下市價單,永遠沒有市價列)
        #   定價 = 第一個 price>0 限價檔位 (處置股出場限價賣不能拿 0;6243 教訓)
        raw_bid1_price = float(_pick(bids[0], "price") or 0) if bids else 0.0
        raw_bid1_size = int(_pick(bids[0], "size") or 0) if bids else 0
        raw_ask1_price = float(_pick(asks[0], "price") or 0) if asks else 0.0
        raw_ask1_size = int(_pick(asks[0], "size") or 0) if asks else 0
        limit_bid1_price, limit_bid1_size = _first_priced_level(bids)
        # 市價彙總列只會佔第一列 (price=0)
        mkt_bid_size = raw_bid1_size if (bids and raw_bid1_price <= 0) else 0
        # 注意: 一般股若全程沒有市價排隊,基準恆為 0 → 支撐消失規則不 arm (無此保護,定案接受)
        is_disp = bool(self.dispositions.get(symbol))
        bid_basis = limit_bid1_size if is_disp else mkt_bid_size
        # 委賣「任一檔」有量 (市價賣單 price=0 也算真賣單)
        asks_any = any(int(_pick(a, "size") or 0) > 0 for a in asks) if asks else False

        # 最新快照 (顯示用 raw) — 淘汰/撤單後也持續更新 (資料持續收)
        h.last_bid1_price = raw_bid1_price
        h.last_bid1_size = raw_bid1_size
        h.last_ask1_price = raw_ask1_price
        h.last_ask1_size = raw_ask1_size
        h.last_book_ts = datetime.now().isoformat(timespec="seconds")

        # [real] 更新委買一價給 session (處置股出場限價賣 — 用限價列價格,>0 才記)
        if self.session is not None:
            self.session.update_bid1(symbol, limit_bid1_price)

        # === [real] 出場訊號 (2026-08-12 定案): 支撐隊伍「消失」→ 立刻出場,
        # 不再等委賣出現 (賣壓走市價單吃穿時委賣可能不掛出,訊號會晚):
        #   一般股: 市價買隊伍曾出現後歸零 = 漲停要打開的前兆
        #   處置股: 漲停價買單消失 (限價列跌下漲停/清空;處置股本來就沒有市價列)
        # support_seen 轉移語意 —「沒有了」代表曾經有,避免開盤首 tick 隊伍未成形就誤觸。
        # exited flag 在 session 內防重複;has_exposure 先擋掉無單無倉的檔避免每 tick 開 thread
        if is_disp:
            support_now = h.limit_up > 0 and limit_bid1_price >= h.limit_up - 0.001
        else:
            support_now = mkt_bid_size > 0
        if support_now:
            h.support_seen = True
        elif h.support_seen:
            reason = "limit_up_bid_gone" if is_disp else "mkt_queue_gone"
            if h.status == Holding.TRACKING:
                # 標「出場」狀態 (sim/real 都顯示;冪等 — 之後每 tick 重複進來只是重設同值)
                h.status = Holding.PULLED
                h.pulled_reason = reason
                logger.warning(f"[trader] {symbol} 出場訊號 — {reason}")
            if self.session is not None:
                if h.status == Holding.DISCARDED_FIRST:
                    # 淘汰檔 (行情因「有委託在途」留著): pending 單交給撤單佇列,只有真的有部位才出場 —
                    # 否則撤單往返期間 has_exposure 恆 True,每 tick 起一個等 3 s 窗口的出場 worker (2026-10-20 審查)
                    go = self.session.get_filled_lots(symbol) > 0
                else:
                    go = self.session.has_exposure(symbol)
                if go:
                    self.session.exit_position(symbol, reason)

        # === 警示: 委買量持續遞減 (取樣式,只顯示不動作;基準與支撐規則同) ===
        self._update_decline_warning(h, bid_basis)

        # === 淘汰後的出場意圖沒完成 → 靠行情節奏重試 (2026-10-20 審查): 第一次 exit_position 被冷卻擋下 /
        # 賣單失敗回退 exited=False 之後,淘汰檔多半沒有 support_seen (首盤就有委賣),上面的支撐消失規則不會
        # 替它重試;行情已因「有委託在途不退訂」留著,這裡每 5 s 最多問一次 (exit_position 自帶 exited 防重與冷卻)。
        if h.status == Holding.DISCARDED_FIRST and self.session is not None:
            import time as _t
            now = _t.time()
            if now - h._elim_retry_ts >= 5.0:
                h._elim_retry_ts = now
                try:
                    why = self.session.exit_pending_after_elimination(symbol)
                    if why:
                        self.session.exit_position(symbol, f"fill_after_elimination: {why}")
                except Exception as e:
                    logger.error(f"[trader] {symbol} 淘汰後出場重試失敗: {e}")

        # === 第一盤: 開盤第一筆 books ===
        if not h.first_books_seen:
            h.first_books_seen = True
            h.first_books = {
                "bid1_price": raw_bid1_price, "bid1_size": raw_bid1_size,
                "ask1_price": raw_ask1_price, "ask1_size": raw_ask1_size,
                "ts": h.last_book_ts,
            }
            # 檢查: 委賣出現 (任一檔有量;市價賣單 price=0 列也算真賣單) → 淘汰
            if asks_any:
                self._fail_first(h, f"first_books_ask_appeared ({raw_ask1_price} × {raw_ask1_size})")
                return
            self._maybe_enter_tracking(h)
            self._pull_process_events()
            return

        # (原「委買量 tick 間減半 → 撤單/出場」規則已於 2026-08-19 移除:
        #  出場只看上面的支撐隊伍消失;單子一定會被成交,不需撤單保護)
        self._pull_process_events()

    def _update_decline_warning(self, h: Holding, bid1_size: int):
        """每 BID_DECLINE_SAMPLE_SEC 取樣一次委買一量;
        連續 BID_DECLINE_MINUTES 個樣本遞減 → 設 warning (顯示用,不動作)。"""
        import time as _t
        now = _t.time()
        if now - h._sample_ts < self.cfg.bid_decline_sample_sec:
            return
        h._sample_ts = now
        if h._sample_size is not None:
            if bid1_size < h._sample_size:
                h._decline_count += 1
            else:
                h._decline_count = 0
                if h.warning:
                    h.warning = ""      # 回升 → 解除警示
        h._sample_size = bid1_size
        if h._decline_count >= self.cfg.bid_decline_minutes and not h.warning:
            h.warning = (f"委買量連續 {h._decline_count} 次取樣遞減 "
                         f"(每 {self.cfg.bid_decline_sample_sec}s 一次)")
            logger.warning(f"[trader] ⚠ {h.symbol} {h.warning}")

    def on_trade(self, symbol: str, trade_data: dict, *, received_at=None,
                 received_mono=None, feed_epoch=None):
        # 試撮 tick (集合競價的模擬撮合,isTrial=true) 不是真成交 —
        # 不記首筆、不觸發任何單 (2026-08-10 2491 事故;深度防禦,主要過濾在 runner)
        if _pick(trade_data, "isTrial"):
            return
        h = self.holdings.get(symbol)
        # (隔日賣不再由成交觸發 — 2026-08-16 改純 book 驅動,規則在
        #  session.update_overnight_book;今日活躍閘門也在 session 端)
        if h is None:
            return

        price = float(_pick(trade_data, "price") or 0)
        pull_seed_duplicate = bool(_pick(trade_data, "_pull_seed") and h.first_trade_seen)
        qty = int(_pick(trade_data, "size") or _pick(trade_data, "qty") or 0)
        if price <= 0:
            return
        if not pull_seed_duplicate:
            self._pull_events.put("trade", (h, {"time": _pick(trade_data, "time")}, qty, feed_epoch),
                                  wall=received_at, mono=received_mono)
        # 首筆真成交到 → 標記「已開盤」(filter 退場,不再誤判開盤市價列;2026-08-27 8105/1312)。
        # 冪等;開盤撮種子化 (runner 建 trader 後補餵 buffer 首筆) 也走這條 → 一併立旗標。
        if self.state is not None:
            self.state.mark_opened(symbol)
        h.last_trade_price = price
        h.trade_count += 1               # 累計每一筆成交 (統計用)

        # === 第一盤: 開盤第一筆成交 ===
        if not h.first_trade_seen:
            h.first_trade_seen = True
            # size 單位是張 (2026-08-29 實證: 整股成交量非 1000 倍數 → 非股數)。純顯示/log 用。
            lots = qty
            h.first_trade = {
                "price": price, "qty": qty, "lots": lots,
                "ts": datetime.now().isoformat(timespec="seconds"),
            }
            if h.status == Holding.DISCARDED_FIRST:
                return   # books 那邊已淘汰 (五檔賣單出現),只補記錄 (委託已在 _fail_first 撤)
            # 首筆量門檻判斷已移除 (2026-08-29) — 首筆成交到就進追蹤 + 送最後一筆市價
            self._maybe_enter_tracking(h)
            # [real] 首筆成交到達: 預掛單未(全)成交 → 撤剩餘、留已成交,再市價單追差額
            # (session 盲送 thread 讀 first_trade_fired 送最後一筆後停)
            if self.session is not None:
                self.session.on_first_trade(symbol)

        self._pull_process_events()

    # ─── 大單抽掉訊號 (只新增觸發點;既有出場/撤單語意不動) ───

    def configure_pull_exit(self, **values):
        # API and evaluation share this lock: once mode=off returns, no queued
        # pull evaluation can still start an exit using the old configuration.
        with self._pull_lock:
            result = self.session.set_pull_exit(**values)
            self._pull_events.clear()
            for h in self.holdings.values():
                h.pull_stream = None
            return result

    def _pull_config(self):
        cfg = getattr(self.session, "pull_exit", None)
        return cfg if isinstance(cfg, PullExitConfig) else PullExitConfig()

    def _pull_eligible(self, h, cfg):
        return (not self._stop.is_set() and cfg.mode != "off"
                and h.status in (Holding.TRACKING, Holding.PULLED)
                and not self.dispositions.get(h.symbol)
                and not (self.session.has_overnight(h.symbol)
                         and self.session.get_symbol_state(h.symbol) is None))

    def _pull_feed(self, h):
        if self.subscriber is None:
            return False, None
        return self.subscriber.is_stale(h.symbol), self.subscriber.feed_epoch(h.symbol)

    def _pull_stream(self, h, cfg, epoch):
        if h.pull_signal is None:
            h.pull_signal = self.session.get_pull_signal(h.symbol)
        if h.pull_stream is None or h.pull_stream.cfg != cfg:
            h.pull_stream = PullStream(cfg)
        stale, current_epoch = self._pull_feed(h)
        stream = h.pull_stream
        stream.check_epoch(current_epoch if current_epoch is not None else epoch)
        if stale or (epoch is not None and current_epoch is not None and epoch != current_epoch):
            stream.reset()
            return None
        return stream

    def _pull_book(self, h, lots, raw_ts, received_at, received_mono, epoch):
        wall = time.time() if received_at is None else received_at
        mono = time.monotonic() if received_mono is None else received_mono
        with self._pull_lock:
            cfg = self._pull_config()
            if not self._pull_eligible(h, cfg):
                h.pull_stream = None
                return
            stream = self._pull_stream(h, cfg, epoch)
            if stream is None:
                return
            ts = stream.timestamp(raw_ts, wall)
            day = trading_day(ts, cfg.cutoff)
            if not day or day != trading_day(wall, cfg.cutoff) or h._pull_open_day != day:
                stream.reset()
                return
            self._pull_retry(h, mono)
            if h.pull_signal is not None:
                return
            if stream.book(ts, lots, mono):
                self._pull_drain(h, stream, mono)

    def _pull_trade(self, h, data, size, received_at, received_mono, epoch):
        wall = time.time() if received_at is None else received_at
        mono = time.monotonic() if received_mono is None else received_mono
        with self._pull_lock:
            cfg = self._pull_config()
            raw_ts = _pick(data, "time")
            ts = exchange_time(raw_ts)
            day = trading_day(wall if ts is None else ts, cfg.cutoff)
            if day and day == trading_day(wall, cfg.cutoff):
                h._pull_open_day = day  # Includes off mode, for a later manual enable.
            if not self._pull_eligible(h, cfg):
                return
            stream = self._pull_stream(h, cfg, epoch)
            if stream is None:
                return
            ts = stream.timestamp(raw_ts, wall)
            if not day or day != trading_day(wall, cfg.cutoff):
                stream.reset()
                return
            if h.pull_signal is None:
                anchor = (stream.last_book_mono if stream.last_book_mono is not None
                          else stream.first_trade_mono)
                if anchor is not None and mono - anchor > cfg.max_gap_sec:
                    stream.reset()
                if stream.first_trade_mono is None:
                    stream.first_trade_mono = mono
                # Seal overdue books before accepting this print. Receipt lag
                # is a deadline even if the evaluator was descheduled.
                self._pull_drain(h, stream, mono)
            if h.pull_signal is not None:
                # A late print that would have prevented a fired signal is the
                # most important lag diagnostic; continue counting it, no replay.
                result = "late" if ts <= h.pull_signal["ts"] else "accepted"
            else:
                result = stream.detector.on_trade(ts, size)
            # `volume` unit is not verified against a captured production tape;
            # use the locally evidenced lots-valued `size` only, per spec.
            if result == "late":
                stream.late_count += 1
                if stream.late_count == 1 or stream.late_count % 100 == 0:
                    self._pull_writer.submit(f"[pull] {h.symbol} 成交晚於判斷 lag，不追溯 count={stream.late_count}")
            elif result == "unordered":
                stream.reset()
                self._pull_writer.submit(f"[pull] {h.symbol} 成交時間倒退，清空視窗重新 arm")

    def _pull_drain(self, h, stream, mono):
        while stream.pending and stream.pending[0][0] <= mono:
            _, ts, lots = stream.pending.popleft()
            own, target = self.session.pull_inputs(h.symbol, ts)
            signal = stream.detector.on_book(ts, lots, own_cancels=own,
                                             fleet_lots=stream.cfg.fleet_nodes * target)
            if signal is not None:
                h._pull_retry_mono = mono
                self._pull_fire(h, signal, stream.cfg.mode)
                stream.pending.clear()
                return

    def _pull_fire(self, h, signal, mode):
        action = "shadow" if mode == "shadow" else "no_exposure"
        if mode == "on":
            exposure, filled = self.session.prepare_pull_exit(h.symbol)
            if exposure:
                self.session.exit_position(h.symbol, "mkt_queue_pulled")
                action = "exit_position"
        else:
            exposure = self.session.has_exposure(h.symbol)
            filled = self.session.get_filled_lots(h.symbol)
        row = {**signal, "symbol": h.symbol, "mode": mode, "filled_lots": filled,
               "has_exposure": exposure, "action": action}
        h.pull_signal = row
        self.session.record_pull_signal(h.symbol, row)
        self._pull_writer.submit(
            f"[pull] {h.symbol} 大單抽掉訊號({mode}) — {signal['window_sec']:g} s "
            f"抽 {signal['cancels']:g} 張 / 隊伍 {signal['q_before']:g} 張 "
            f"({signal['cancels'] / signal['q_before'] * 100:.1f}%) / "
            f"成交 {signal['trades']:g} 張 / 起算後 {signal['armed_s']:g} s / "
            f"持倉 {filled} 張 / 有委託 {exposure}", row)

    def _pull_retry(self, h, mono):
        if (h.pull_signal and h.pull_signal["mode"] == "on"
                and mono - h._pull_retry_mono >= 5):
            h._pull_retry_mono = mono
            if self.session.exit_pending_after_elimination(h.symbol):
                self.session.exit_position(h.symbol, "mkt_queue_pulled")

    def poll_pull_signals(self, *, now_mono=None, now_wall=None):
        self._pull_events.put("poll", wall=now_wall, mono=now_mono)
        self._pull_process_events()

    def _pull_process_events(self):
        # Only this consumer may ingest prints/books or seal expired readings.
        # Registration remains available even while a legacy callback or this
        # consumer is waiting for state/session locks.
        with self._pull_lock:
            while (event := self._pull_events.pop()) is not None:
                kind, args, wall, mono = event
                if kind == "trade":
                    h, data, size, epoch = args
                    self._pull_trade(h, data, size, wall, mono, epoch)
                elif kind == "book":
                    h, lots, raw_ts, epoch = args
                    self._pull_book(h, lots, raw_ts, wall, mono, epoch)
                else:
                    self._pull_poll(mono, wall)

    def _pull_poll(self, mono, wall):
        with self._pull_lock:
            cfg = self._pull_config()
            for h in self.holdings.values():
                if not self._pull_eligible(h, cfg):
                    h.pull_stream = None
                    continue
                self._pull_retry(h, mono)
                stream = h.pull_stream
                if stream is None:
                    continue
                stale, epoch = self._pull_feed(h)
                if epoch is not None:
                    stream.check_epoch(epoch)
                if (stale or not trading_day(wall, cfg.cutoff)
                        or (stream.last_book_mono is not None
                            and mono - stream.last_book_mono > cfg.max_gap_sec)):
                    stream.reset()
                    continue
                self._pull_drain(h, stream, mono)

    def _pull_loop(self):
        # Flush the last queued reading even when there is no subsequent tick.
        while not self._stop.wait(0.05):
            try:
                self.poll_pull_signals()
            except Exception:
                logger.exception("[pull] 延後判斷失敗")

    # ─── 狀態轉換 ──────────────────────────────────────────

    def _maybe_enter_tracking(self, h: Holding):
        """第一盤兩筆 (books + trades) 都到齊且沒淘汰 → 進入追蹤。"""
        if h.status != Holding.WAITING:
            return
        if h.first_books_seen and h.first_trade_seen:
            h.status = Holding.TRACKING
            logger.info(f"[trader] {h.symbol} 第一盤通過 "
                        f"(成交 {h.first_trade.get('price')} × {h.first_trade.get('lots')} 張) → 追蹤")

    def _fail_first(self, h: Holding, reason: str):
        """第一盤檢查淘汰 — 永久丟棄。[real] 有部位→市價出場;沒部位→撤 pending。

        ⚠️ 關鍵: 開盤預掛單可能已成交 (持倉)。若無腦退訂 → 抱著部位停收行情,
        之後委賣出現偵測不到 → 出不了場。所以:有已成交部位時**市價賣出 + 不退訂**
        (留著收行情直到賣掉);只有無部位才退訂。
        """
        h.status = Holding.DISCARDED_FIRST
        h.first_fail_reason = reason
        has_position = False
        live_buy = False
        if self.session is not None:
            try:
                # 先記「淘汰後任何買進成交都要出場」— 2026-10-20 8089 (node2/3/6): 預掛單 09:00:07.085 成交,
                # 委賣五檔比成交回報早 97~300 ms 到 → 下面 get_filled_lots 讀到 0 → 只撤單 + 退訂,之後
                # 回報 / 撤單被拒「已成交」把張數記上卻沒人出場 → 抱到 13:24。旗標設好後成交一落地就出場。
                self.session.mark_exit_on_fill(h.symbol, f"first_check: {reason}")
                has_position = self.session.get_filled_lots(h.symbol) > 0
                if has_position:
                    # 撤 pending + 市價賣出全部已成交 (同「出現賣單→出場」路徑)
                    self.session.exit_position(h.symbol, f"first_check: {reason}")
                else:
                    # async — 行情 thread 上不做同步 REST
                    self.session.cancel_symbol_orders_async(h.symbol, f"first_check: {reason}")
                live_buy = self.session.has_live_buy_order(h.symbol)
                # 撤單 thread 可能已經跑完 (撤單被拒「已成交」→ 依快照補成交) → 退訂前再讀一次部位
                has_position = has_position or self.session.get_filled_lots(h.symbol) > 0
            except Exception as e:
                logger.error(f"[trader] {h.symbol} 淘汰處理委託/部位失敗: {e}")
        # 同步 filter state (FilterPage 丟棄清單看得到)
        if self.state:
            try:
                self.state.unmark_first_check(h.symbol, reason)
            except Exception:
                pass
        # 退訂只在「無已成交部位、也沒有任何買進委託在途」時做 — 有部位要留著收行情直到賣掉確認;
        # 有單在途 (預掛 P / 市價 M 還沒撤成) 時它隨時可能成交,退訂了之後的出場就沒有盤面
        # (2026-10-20 8089: 退訂後委賣一直在、行情卻斷了)。多留幾檔在 socket 上成本很低。
        keep_feed = has_position or live_buy
        if self.unsub_fn and not keep_feed:
            try:
                self.unsub_fn(h.symbol)
            except Exception as e:
                logger.warning(f"[trader] 退訂 {h.symbol} 失敗: {e}")
        tail = (" (持倉→市價出場,不退訂)" if has_position
                else " (買進委託在途,不退訂;成交落地即出場)" if live_buy else "")
        logger.warning(f"[trader] {h.symbol} 第一盤淘汰 — {reason}{tail}")

    # ─── summary (API / 前端兩區塊) ────────────────────────

    def summary(self) -> dict:
        first_stage = []
        tracking = []
        for s, h in self.holdings.items():
            # 「開盤即鎖」徽章 (8:30 首筆真實報價就漲停) — 看強勢股是否更會留到最後
            first_tick = self.state.is_first_tick(s) if self.state else False
            # [real] 該檔交易 state (委託/成交/出場) — sim 模式為 None
            trade = self.session.get_symbol_state(s) if self.session else None
            first_stage.append({
                "symbol": s,
                "limit_up": h.limit_up,
                "status": h.status,
                "first_tick": first_tick,
                "first_books": h.first_books or None,
                "first_trade": h.first_trade or None,
                "fail_reason": h.first_fail_reason,
                "trade": trade,
                "pull_signal": h.pull_signal,
            })
            if h.status in (Holding.TRACKING, Holding.PULLED, Holding.ABANDONED):
                tracking.append({
                    "symbol": s,
                    "limit_up": h.limit_up,
                    "first_tick": first_tick,
                    "bid1_price": h.last_bid1_price,
                    "bid1_size": h.last_bid1_size,
                    "ask1_price": h.last_ask1_price,
                    "ask1_size": h.last_ask1_size,
                    "status": h.status,
                    "pulled_reason": h.pulled_reason,
                    "warning": h.warning,
                    "last_trade_price": h.last_trade_price,
                    "last_book_ts": h.last_book_ts,
                    "trade": trade,
                    "pull_signal": h.pull_signal,
                })
        n_track = sum(1 for h in self.holdings.values() if h.status == Holding.TRACKING)
        n_pulled = sum(1 for h in self.holdings.values() if h.status == Holding.PULLED)
        n_failed = sum(1 for h in self.holdings.values() if h.status == Holding.DISCARDED_FIRST)
        return {
            "watchlist_total": len(self.holdings),
            "n_tracking": n_track,
            "n_pulled": n_pulled,
            "n_first_failed": n_failed,
            "trading": self.session.status() if self.session else None,
            "first_stage": sorted(first_stage, key=lambda x: x["symbol"]),
            "tracking": sorted(tracking, key=lambda x: x["symbol"]),
        }


def _pick(o, k):
    if isinstance(o, dict):
        return o.get(k)
    return getattr(o, k, None)


def _first_priced(levels: list) -> float:
    """回第一個 price>0 的檔位價格 (跳過盤中市價單的 price=0 彙總列)。無則回 0。"""
    for lv in (levels or []):
        p = float(_pick(lv, "price") or 0)
        if p > 0:
            return p
    return 0.0


def _first_priced_level(levels: list) -> tuple:
    """回第一個 price>0 檔位的 (price, size) — 跳過盤中市價單 price=0 彙總列。無則 (0.0, 0)。"""
    for lv in (levels or []):
        p = float(_pick(lv, "price") or 0)
        if p > 0:
            return p, int(_pick(lv, "size") or 0)
    return 0.0, 0


def _overnight_book_fields(bids: list, asks: list) -> tuple:
    """隔日賣標的的 book 欄位: (bid1, ask1, mkt_bid_size, limit_bid1_price)。

    市價彙總列判斷與 on_book 內當日邏輯同款 (price<=0 的第一列 = 盤中市價單彙總);
    trader.on_book 與 runner._monitor_on_book 兩個呼叫端共用,避免各算走樣。"""
    raw_bid1_price = float(_pick(bids[0], "price") or 0) if bids else 0.0
    raw_bid1_size = int(_pick(bids[0], "size") or 0) if bids else 0
    mkt_bid_size = raw_bid1_size if (bids and raw_bid1_price <= 0) else 0
    limit_bid1_price = _first_priced(bids)
    return limit_bid1_price, _first_priced(asks), mkt_bid_size, limit_bid1_price
