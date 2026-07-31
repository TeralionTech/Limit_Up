"""State — 追蹤 marked set + 每股 mark/unmark 歷史 + bid1_size 監控。

Thread-safe (單 lock 保護所有變動；不同 socket callback 會併發跑)。

**unmark = 永久淘汰**: unmark 後進 discarded 黑名單，不能 re-mark，
caller 應同時退訂該股 (subscriber.request_unsubscribe)。

**bid 減半檢查分兩時段** (caller 依時間呼叫):
- 08:30–08:59: update_max_bid() 只記錄委買一(漲停價)最大單量
- 08:59–09:00: check_final_bid_drop() 當下量 < max × ratio → unmark
"""
from __future__ import annotations

from datetime import datetime
from threading import Lock
from typing import Dict, List, Set


class State:
    def __init__(self, bid_drop_ratio: float = 0.5):
        """
        bid_drop_ratio: final check 時 bid1_size 低於歷史最大 × 此比例 → unmark (預設 0.5)
        """
        self._lock = Lock()
        self.marked: Set[str] = set()
        self.discarded: Set[str] = set()                 # unmark 過 = 永久淘汰
        # 「開盤即鎖」— 第一筆真實報價 (委買非空) 就滿足 mark 條件的強勢股 (顯示/分析用)
        self.first_tick_limit_up: Set[str] = set()
        self.history: Dict[str, List[dict]] = {}         # symbol → [event, ...]
        # mark 之後追蹤該 symbol 看過的 bid1 max size (只在 8:30–8:59 更新)
        self._max_bid_size: Dict[str, int] = {}
        self._bid_drop_ratio = bid_drop_ratio

    # ─── Mark / Unmark ─────────────────────────────────────

    def mark(self, symbol: str, bid_price: float, bid1_size: int, limit_up: float,
             first_tick: bool = False) -> bool:
        """Mark 一檔為「試撮已鎖漲停」。回 True = 新標記。
        已淘汰 (discarded) 的股票不能 re-mark → 回 False。

        first_tick=True: 該股第一筆真實報價 (委買非空) 就滿足 mark 條件 =「開盤即鎖」強勢股。
        """
        with self._lock:
            if symbol in self.discarded:
                return False
            was_new = symbol not in self.marked
            self.marked.add(symbol)
            if was_new:
                self._max_bid_size[symbol] = max(bid1_size, 0)
                if first_tick:
                    self.first_tick_limit_up.add(symbol)
                self.history.setdefault(symbol, []).append({
                    "event": "mark",
                    "ts": datetime.now().isoformat(timespec="seconds"),
                    "bid_price": bid_price,
                    "bid1_size": bid1_size,
                    "limit_up": limit_up,
                    "first_tick": first_tick,
                })
            return was_new

    def unmark_ask_appeared(self, symbol: str, ask_price: float, ask_size: int) -> bool:
        """賣單出現 → unmark。"""
        return self._unmark(symbol, "ask_appeared", {
            "ask_price": ask_price,
            "ask_size": ask_size,
        })

    def unmark_bid_below_limit(self, symbol: str, bid_price: float, limit_up: float) -> bool:
        """委買一價格跌下漲停 → unmark。"""
        return self._unmark(symbol, "bid_below_limit", {
            "bid_price": bid_price,
            "limit_up": limit_up,
        })

    def unmark_first_check(self, symbol: str, detail: str) -> bool:
        """9:00 第一盤檢查淘汰 (委賣一出現 / 首筆成交量太小) → unmark。"""
        return self._unmark(symbol, "first_check_failed", {"detail": detail})

    def unmark_bid_dropped(self, symbol: str, current_bid1_size: int, max_bid_size: int) -> bool:
        """final check (08:59–09:00): bid1 size < 8:30–8:59 最大量一半 → unmark。"""
        return self._unmark(symbol, "bid_dropped_half", {
            "current_bid1_size": current_bid1_size,
            "max_bid1_size_since_mark": max_bid_size,
            "drop_ratio": round(current_bid1_size / max(max_bid_size, 1), 3),
        })

    def _unmark(self, symbol: str, reason: str, extra: dict) -> bool:
        with self._lock:
            if symbol not in self.marked:
                return False
            self.marked.discard(symbol)
            self.discarded.add(symbol)          # 永久淘汰，不能 re-mark
            self._max_bid_size.pop(symbol, None)
            event = {
                "event": "unmark",
                "reason": reason,
                "ts": datetime.now().isoformat(timespec="seconds"),
            }
            event.update(extra)
            self.history.setdefault(symbol, []).append(event)
            return True

    # ─── bid1_size 追蹤 (分時段) ────────────────────────────

    def update_max_bid(self, symbol: str, current_bid1_size: int):
        """08:30–08:59 用 — 只更新委買一(漲停價)最大單量，不觸發 unmark。"""
        with self._lock:
            if symbol not in self.marked:
                return
            if current_bid1_size > self._max_bid_size.get(symbol, 0):
                self._max_bid_size[symbol] = current_bid1_size

    def check_final_bid_drop(self, symbol: str, current_bid1_size: int):
        """08:59–09:00 final check — 比較當下量 vs 8:30–8:59 最大量，不更新 max。

        回 (should_unmark, prev_max):
          should_unmark=True → caller 拿 prev_max 呼叫 unmark_bid_dropped
        """
        with self._lock:
            if symbol not in self.marked:
                return False, 0
            prev_max = self._max_bid_size.get(symbol, 0)
            if prev_max > 0 and current_bid1_size < prev_max * self._bid_drop_ratio:
                return True, prev_max
            return False, prev_max

    # ─── 查詢 / 統計 ────────────────────────────────────────

    def is_marked(self, symbol: str) -> bool:
        with self._lock:
            return symbol in self.marked

    def is_discarded(self, symbol: str) -> bool:
        with self._lock:
            return symbol in self.discarded

    def snapshot(self) -> List[dict]:
        with self._lock:
            return [
                {
                    "symbol": s,
                    "first_tick_limit_up": s in self.first_tick_limit_up,
                    "history": list(self.history.get(s, [])),
                }
                for s in sorted(self.marked)
            ]

    def stats(self) -> dict:
        with self._lock:
            mark_ev = sum(1 for evs in self.history.values() for e in evs if e["event"] == "mark")
            unmark_ev = sum(1 for evs in self.history.values() for e in evs if e["event"] == "unmark")
            ask_unmarks = sum(
                1 for evs in self.history.values() for e in evs
                if e.get("event") == "unmark" and e.get("reason") == "ask_appeared"
            )
            bid_below_unmarks = sum(
                1 for evs in self.history.values() for e in evs
                if e.get("event") == "unmark" and e.get("reason") == "bid_below_limit"
            )
            bid_drop_unmarks = sum(
                1 for evs in self.history.values() for e in evs
                if e.get("event") == "unmark" and e.get("reason") == "bid_dropped_half"
            )
            return {
                "currently_marked": len(self.marked),
                "total_mark_events": mark_ev,
                "total_unmark_events": unmark_ev,
                "unmark_by_ask_appeared": ask_unmarks,
                "unmark_by_bid_below_limit": bid_below_unmarks,
                "unmark_by_bid_dropped": bid_drop_unmarks,
                "unique_symbols_touched": len(self.history),
            }

    def get_marked_list(self) -> List[str]:
        """給 API 用 — 當前 marked symbols (sorted)."""
        with self._lock:
            return sorted(self.marked)

    def get_first_tick_marked_list(self) -> List[str]:
        """給 API 用 — 當前 marked 中「開盤即鎖」的 symbols (sorted)."""
        with self._lock:
            return sorted(self.marked & self.first_tick_limit_up)

    def get_marked_prioritized(self) -> List[str]:
        """預掛下單順序用 — 開盤即鎖 (first_tick) 優先,再盤中鎖;各組內照代號。

        place_pre_orders 照清單順序下單+扣預算,故重排即給開盤即鎖送單時間優先
        (試撮排隊) + 預算優先。
        """
        with self._lock:
            first = sorted(self.marked & self.first_tick_limit_up)
            rest = sorted(self.marked - self.first_tick_limit_up)
            return first + rest

    def is_first_tick(self, symbol: str) -> bool:
        """該股是否「開盤即鎖」(第一筆真實報價就漲停) — trader/SimPage 徽章用."""
        with self._lock:
            return symbol in self.first_tick_limit_up

    def get_discarded_list(self) -> List[dict]:
        """給 API 用 — 永久淘汰的股票 + 各自 unmark 原因."""
        with self._lock:
            result = []
            for sym in self.discarded:
                evs = self.history.get(sym, [])
                last_unmark = next(
                    (e for e in reversed(evs) if e["event"] == "unmark"), None
                )
                result.append({
                    "symbol": sym,
                    "reason": last_unmark.get("reason") if last_unmark else "?",
                    "ts": last_unmark.get("ts") if last_unmark else None,
                })
            return sorted(result, key=lambda x: x.get("ts") or "", reverse=True)
