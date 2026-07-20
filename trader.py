"""Trader — 9:00 開盤後監控 (做多);session 為 real+armed 時同步掛真單。

兩階段 (對應前端模擬/真實執行頁兩區塊):

  第一盤檢查 (block 1) — 每檔 watchlist 股票收「開盤第一筆」:
    - 首筆 books: 委賣一出現單子 → unmark 丟棄 (+ 退訂 + 撤委託)
    - 首筆 trades: 成交量 < first_trade_min_lots 張 (前端可調) → unmark 丟棄 (+ 退訂 + 撤委託)
    - 兩者都通過 → 進入盤中追蹤 (status=tracking)
      [real] 首筆成交到達時: 預掛單未(全)成交 → 撤剩餘留已成交;通過檢查 → 市價單追差額

  盤中追蹤 (block 2) — 通過第一盤的標的:
    - 顯示 委買一價 / 委買一量
    - 狀態 "追蹤" → "撤單" 條件 (單一):
        委買一量在兩個 tick 之間減少 1/2 以上 (第 3 筆成交後才開始判)
      (原「委買一價格不是漲停價」條件已移除 — 盤中市價單佔五檔第一列且 price=0 會誤判)
      [real] 撤單同步撤掉該檔 pending 委託
    - [real] 任一檔委賣出現 → 立刻出場 (撤委託 + 市價賣出全部已成交)
    - 警示 (不動作): 委買一量每 BID_DECLINE_SAMPLE_SEC 取樣,
      連續 BID_DECLINE_MINUTES 分鐘遞減 → warning 顯示於前端
    - 撤單後**持續收資料** (不退訂，狀態停在撤單)

session=None (模擬模式) = 純監控,零下單 — 行為與加入交易功能前完全相同。
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime
from typing import Callable, Dict, Optional

logger = logging.getLogger(__name__)


class Holding:
    """單股 tracking state."""

    # status 值
    WAITING = "waiting"                  # 等第一盤資料
    DISCARDED_FIRST = "discarded_first"  # 第一盤檢查淘汰
    TRACKING = "tracking"                # 追蹤中
    PULLED = "pulled"                    # 撤單 (仍持續收資料)

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
        self.pulled_reason = ""
        self.trade_count = 0             # 累計成交筆數 (委買量減半檢查暖機用: 第 3 筆成交後才判)
        self.book_tick_count = 0         # 累計 books tick 數 (排隊中量減半檢查的暖機: >=3)
        self.prev_bid1_size: Optional[int] = None   # 上一個 tick 的委買一量 (tick 間比較)
        # 警示: 委買量持續遞減 (每 sample_sec 取樣,連續 decline_minutes 分鐘遞減 → warning)
        self.warning = ""
        self._sample_ts = 0.0            # 上次取樣時間 (epoch)
        self._sample_size: Optional[int] = None
        self._decline_count = 0
        self.last_bid1_price = 0.0
        self.last_bid1_size = 0
        self.last_ask1_price = 0.0
        self.last_ask1_size = 0
        self.last_trade_price = 0.0
        self.last_book_ts = ""


class Trader:
    """9:00 之後的主控。"""

    def __init__(self, watchlist, limit_ups, cfg,
                 recorder=None, state=None, unsub_fn: Optional[Callable] = None,
                 session=None):
        """
        watchlist: List[str] — filter 結束時的 marked 名單
        limit_ups: Dict[str, float] — 每檔漲停價
        cfg: config.Config (first_trade_min_lots 可被 API 在 runtime 改)
        state: state.State — 第一盤淘汰要同步 unmark (可 None)
        unsub_fn: callable(symbol) — 第一盤淘汰要退訂 (可 None)
        session: trading_session.TradingSession — real+armed 時同步掛真單 (可 None = 純監控)
        """
        self.holdings: Dict[str, Holding] = {
            s: Holding(s, limit_ups.get(s, 0.0)) for s in watchlist
        }
        self.cfg = cfg
        self.recorder = recorder
        self.state = state
        self.unsub_fn = unsub_fn
        self.session = session
        self._stop = threading.Event()
        live = bool(session and session.is_live())
        logger.info(f"[trader] 建立 — {'真實交易' if live else '純監控'} {len(self.holdings)} 檔 "
                    f"(第一盤最小成交 {cfg.first_trade_min_lots} 張)")

    # ─── 對外 API ──────────────────────────────────────────

    def start(self):
        """無獨立 thread — 邏輯全在 on_book/on_trade handlers (下單動作交給 session)。"""
        logger.info("[trader] 啟動 (下單與否由 session mode/armed 決定)")

    def stop(self, do_final_close: bool = True):
        self._stop.set()

    # ─── 掛 subscriber 的 handlers ─────────────────────────

    def on_book(self, symbol: str, bids: list, asks: list):
        h = self.holdings.get(symbol)
        if h is None:
            return

        bid1_price = float(_pick(bids[0], "price") or 0) if bids else 0.0
        bid1_size = int(_pick(bids[0], "size") or 0) if bids else 0
        ask1_price = float(_pick(asks[0], "price") or 0) if asks else 0.0
        ask1_size = int(_pick(asks[0], "size") or 0) if asks else 0
        # 委賣「任一檔」有量 (市價賣單 price=0 也算真賣單)
        asks_any = any(int(_pick(a, "size") or 0) > 0 for a in asks) if asks else False

        # 最新快照 — 淘汰/撤單後也持續更新 (資料持續收)
        h.last_bid1_price = bid1_price
        h.last_bid1_size = bid1_size
        h.last_ask1_price = ask1_price
        h.last_ask1_size = ask1_size
        h.last_book_ts = datetime.now().isoformat(timespec="seconds")

        # === [real] 委賣出現 → 立刻出場 (撤委託 + 市價賣出全部已成交) ===
        # exited flag 在 session 內防重複;has_exposure 先擋掉無單無倉的檔避免每 tick 開 thread
        if asks_any and self.session is not None and self.session.has_exposure(symbol):
            self.session.exit_position(symbol, "ask_appeared")

        # === 警示: 委買量持續遞減 (取樣式,只顯示不動作) ===
        self._update_decline_warning(h, bid1_size)

        # === 第一盤: 開盤第一筆 books ===
        if not h.first_books_seen:
            h.first_books_seen = True
            h.first_books = {
                "bid1_price": bid1_price, "bid1_size": bid1_size,
                "ask1_price": ask1_price, "ask1_size": ask1_size,
                "ts": h.last_book_ts,
            }
            # 檢查: 委賣一出現 → 淘汰
            if ask1_size > 0:
                self._fail_first(h, f"first_books_ask_appeared ({ask1_price} × {ask1_size})")
                return
            self._maybe_enter_tracking(h)
            h.prev_bid1_size = bid1_size
            return

        h.book_tick_count += 1

        # === 盤中追蹤: 撤單條件 (只剩量減半) ===
        # 註: 原「委買一跌下漲停」條件已移除 — 盤中市價單會佔五檔第一列且 price=0
        # (例 0.00 × 1561, 漲停價買單在第二列), 拿 bids[0] 價格比會誤判跌下漲停。
        qty_halved = bool(h.prev_bid1_size) and bid1_size < h.prev_bid1_size * 0.5
        if h.status == Holding.TRACKING:
            # 委買一量在兩 tick 之間減少 1/2 以上
            #   暖機: 第 3 筆成交後才判 — 避免開盤瞬間拿「盤前累積量」當基準造成誤撤。
            #   (prev_bid1_size 每 tick 都更新，到第 3 筆成交時基準已是盤後即時量)
            if h.trade_count >= 3 and qty_halved:
                self._pull(h, f"qty_drop_half ({h.prev_bid1_size} → {bid1_size})")
        elif (h.status == Holding.WAITING and qty_halved and h.book_tick_count >= 3
              and self.session is not None and self.session.has_pending(symbol)):
            # [real] 全鎖死無成交股 (WAITING, 沒有首筆成交) 排隊中的預掛單也要受
            # 「量減半→撤單」保護 (規格 #3 主場景)。暖機改用 book tick 數 (≥3)。
            # 只撤委託、Holding 狀態不動 (監控續跑)。
            logger.warning(f"[trader] {symbol} 排隊中量減半 "
                           f"({h.prev_bid1_size} → {bid1_size}) → 撤預掛單")
            try:
                self.session.cancel_symbol_orders(symbol, "qty_drop_half_queued")
            except Exception as e:
                logger.error(f"[trader] {symbol} 撤排隊委託失敗: {e}")

        h.prev_bid1_size = bid1_size

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

    def on_trade(self, symbol: str, trade_data: dict):
        h = self.holdings.get(symbol)
        if h is None:
            return

        price = float(_pick(trade_data, "price") or 0)
        qty = int(_pick(trade_data, "size") or _pick(trade_data, "qty") or 0)
        if price <= 0:
            return
        h.last_trade_price = price
        h.trade_count += 1               # 累計每一筆成交 (委買量減半檢查暖機用)

        # === 第一盤: 開盤第一筆成交 ===
        if not h.first_trade_seen:
            h.first_trade_seen = True
            # 有些 API 給股數、有的給張數 — >= 1000 視為股數換算
            lots = qty // 1000 if qty >= 1000 else qty
            h.first_trade = {
                "price": price, "qty": qty, "lots": lots,
                "ts": datetime.now().isoformat(timespec="seconds"),
            }
            if h.status == Holding.DISCARDED_FIRST:
                return   # books 那邊已淘汰，只補記錄 (委託已在 _fail_first 撤)
            # 檢查: 成交量 < 最小張數 (cfg 值 API 可 runtime 調) → 淘汰
            if lots < self.cfg.first_trade_min_lots:
                self._fail_first(h, f"first_trade_qty_too_small "
                                    f"({lots} 張 < {self.cfg.first_trade_min_lots})")
                return
            self._maybe_enter_tracking(h)
            # [real] 首筆成交資訊到達且通過檢查: 預掛單未(全)成交 → 撤剩餘、
            # 留已成交,再市價單追差額 (session 內背景 thread 處理)
            if self.session is not None:
                self.session.on_first_trade(symbol, passed_first_check=True)

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
        """第一盤檢查淘汰 — 同 filter unmark: 永久丟棄 + 退訂 + [real] 撤委託。"""
        h.status = Holding.DISCARDED_FIRST
        h.first_fail_reason = reason
        # [real] 已掛委託 → 撤;還沒掛 → session 標 stopped 不再下 (no-op when sim)
        if self.session is not None:
            try:
                self.session.cancel_symbol_orders(h.symbol, f"first_check: {reason}")
            except Exception as e:
                logger.error(f"[trader] {h.symbol} 撤委託失敗: {e}")
        # 同步 filter state (FilterPage 丟棄清單看得到) + 退訂
        if self.state:
            try:
                self.state.unmark_first_check(h.symbol, reason)
            except Exception:
                pass
        if self.unsub_fn:
            try:
                self.unsub_fn(h.symbol)
            except Exception as e:
                logger.warning(f"[trader] 退訂 {h.symbol} 失敗: {e}")
        logger.warning(f"[trader] {h.symbol} 第一盤淘汰 — {reason}")

    def _pull(self, h: Holding, reason: str):
        """盤中撤單 — 標狀態 + [real] 撤該檔 pending 委託。持續收資料 (不退訂)。"""
        h.status = Holding.PULLED
        h.pulled_reason = reason
        if self.session is not None:
            try:
                self.session.cancel_symbol_orders(h.symbol, reason)
            except Exception as e:
                logger.error(f"[trader] {h.symbol} 撤委託失敗: {e}")
        logger.warning(f"[trader] {h.symbol} 撤單 — {reason}")

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
            })
            if h.status in (Holding.TRACKING, Holding.PULLED):
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
                })
        n_track = sum(1 for h in self.holdings.values() if h.status == Holding.TRACKING)
        n_pulled = sum(1 for h in self.holdings.values() if h.status == Holding.PULLED)
        n_failed = sum(1 for h in self.holdings.values() if h.status == Holding.DISCARDED_FIRST)
        return {
            "watchlist_total": len(self.holdings),
            "n_tracking": n_track,
            "n_pulled": n_pulled,
            "n_first_failed": n_failed,
            "min_lots": self.cfg.first_trade_min_lots,
            "trading": self.session.status() if self.session else None,
            "first_stage": sorted(first_stage, key=lambda x: x["symbol"]),
            "tracking": sorted(tracking, key=lambda x: x["symbol"]),
        }


def _pick(o, k):
    if isinstance(o, dict):
        return o.get(k)
    return getattr(o, k, None)
