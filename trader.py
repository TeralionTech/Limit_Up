"""Trader — 9:00 開盤後模擬監控 (做多)。

兩階段 (對應前端模擬執行頁兩區塊):

  第一盤檢查 (block 1) — 每檔 watchlist 股票收「開盤第一筆」:
    - 首筆 books: 委賣一出現單子 → unmark 丟棄 (+ 退訂)
    - 首筆 trades: 成交量 < first_trade_min_lots 張 (前端可調) → unmark 丟棄 (+ 退訂)
    - 兩者都通過 → 進入盤中追蹤 (status=tracking)

  盤中追蹤 (block 2) — 通過第一盤的標的:
    - 顯示 委買一價 / 委買一量
    - 狀態 "追蹤" → "撤單" 條件:
        1. 委買一量在兩個 tick 之間減少 1/2 以上
        2. 委買一價格不是漲停價
    - 撤單後**持續收資料** (不退訂，狀態停在撤單)

純監控 — 不下單 (含模擬單也不下)。
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
        self.prev_bid1_size: Optional[int] = None   # 上一個 tick 的委買一量 (tick 間比較)
        self.last_bid1_price = 0.0
        self.last_bid1_size = 0
        self.last_ask1_price = 0.0
        self.last_ask1_size = 0
        self.last_trade_price = 0.0
        self.last_book_ts = ""


class Trader:
    """9:00 之後的主控。"""

    def __init__(self, watchlist, limit_ups, cfg,
                 recorder=None, state=None, unsub_fn: Optional[Callable] = None):
        """
        watchlist: List[str] — filter 結束時的 marked 名單
        limit_ups: Dict[str, float] — 每檔漲停價
        cfg: config.Config (first_trade_min_lots 可被 API 在 runtime 改)
        state: state.State — 第一盤淘汰要同步 unmark (可 None)
        unsub_fn: callable(symbol) — 第一盤淘汰要退訂 (可 None)
        """
        self.holdings: Dict[str, Holding] = {
            s: Holding(s, limit_ups.get(s, 0.0)) for s in watchlist
        }
        self.cfg = cfg
        self.recorder = recorder
        self.state = state
        self.unsub_fn = unsub_fn
        self._stop = threading.Event()
        logger.info(f"[trader] 建立 — 純監控 {len(self.holdings)} 檔 "
                    f"(第一盤最小成交 {cfg.first_trade_min_lots} 張)")

    # ─── 對外 API ──────────────────────────────────────────

    def start(self):
        """純監控 — 無下單 thread，邏輯全在 on_book/on_trade handlers。"""
        logger.info("[trader] 純監控模式啟動 (不下單)")

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

        # 最新快照 — 淘汰/撤單後也持續更新 (資料持續收)
        h.last_bid1_price = bid1_price
        h.last_bid1_size = bid1_size
        h.last_ask1_price = ask1_price
        h.last_ask1_size = ask1_size
        h.last_book_ts = datetime.now().isoformat(timespec="seconds")

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

        # === 盤中追蹤: 兩個撤單條件 ===
        if h.status == Holding.TRACKING:
            # 條件 1: 委買一量在兩 tick 之間減少 1/2 以上
            #   暖機: 第 3 筆成交後才判 — 避免開盤瞬間拿「盤前累積量」當基準造成誤撤。
            #   (prev_bid1_size 每 tick 都更新，到第 3 筆成交時基準已是盤後即時量)
            if h.trade_count >= 3 and h.prev_bid1_size and bid1_size < h.prev_bid1_size * 0.5:
                self._pull(h, f"qty_drop_half ({h.prev_bid1_size} → {bid1_size})")
            # 條件 2: 委買一價格不是漲停價 (即時判，不受暖機影響)
            elif bid1_price < h.limit_up - 0.001:
                self._pull(h, f"price_below_limit ({bid1_price} < {h.limit_up})")

        h.prev_bid1_size = bid1_size

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
                return   # books 那邊已淘汰，只補記錄
            # 檢查: 成交量 < 最小張數 (cfg 值 API 可 runtime 調) → 淘汰
            if lots < self.cfg.first_trade_min_lots:
                self._fail_first(h, f"first_trade_qty_too_small "
                                    f"({lots} 張 < {self.cfg.first_trade_min_lots})")
                return
            self._maybe_enter_tracking(h)

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
        """第一盤檢查淘汰 — 同 filter unmark: 永久丟棄 + 退訂。"""
        h.status = Holding.DISCARDED_FIRST
        h.first_fail_reason = reason
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
        """盤中撤單 — 只標狀態。持續收資料 (不退訂)。"""
        h.status = Holding.PULLED
        h.pulled_reason = reason
        logger.warning(f"[trader] {h.symbol} 撤單 — {reason}")

    # ─── summary (API / 前端兩區塊) ────────────────────────

    def summary(self) -> dict:
        first_stage = []
        tracking = []
        for s, h in self.holdings.items():
            first_stage.append({
                "symbol": s,
                "limit_up": h.limit_up,
                "status": h.status,
                "first_books": h.first_books or None,
                "first_trade": h.first_trade or None,
                "fail_reason": h.first_fail_reason,
            })
            if h.status in (Holding.TRACKING, Holding.PULLED):
                tracking.append({
                    "symbol": s,
                    "limit_up": h.limit_up,
                    "bid1_price": h.last_bid1_price,
                    "bid1_size": h.last_bid1_size,
                    "ask1_price": h.last_ask1_price,
                    "ask1_size": h.last_ask1_size,
                    "status": h.status,
                    "pulled_reason": h.pulled_reason,
                    "last_trade_price": h.last_trade_price,
                    "last_book_ts": h.last_book_ts,
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
            "first_stage": sorted(first_stage, key=lambda x: x["symbol"]),
            "tracking": sorted(tracking, key=lambda x: x["symbol"]),
        }


def _pick(o, k):
    if isinstance(o, dict):
        return o.get(k)
    return getattr(o, k, None)
