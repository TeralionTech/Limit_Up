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
        # 「已開盤」— 該檔首筆真成交 (isOpen) 到 / book 轉逐筆 (isContinuous) → filter 對它退場,
        # 不再 mark/unmark (盤中盤面交給 trader/monitor;免開盤市價列誤判,2026-08-27 8105/1312)
        self._opened: Set[str] = set()
        # 「開盤即鎖」— 第一筆真實報價 (委買非空) 就滿足 mark 條件的強勢股 (顯示/分析用)
        self.first_tick_limit_up: Set[str] = set()
        self.history: Dict[str, List[dict]] = {}         # symbol → [event, ...]
        # mark 之後追蹤該 symbol 的 bid1 量: max (歷史最高) + last (最新一筆)
        # — 08:59:58 批次 final check 用 (2026-08-13 定案)
        self._max_bid_size: Dict[str, int] = {}
        self._last_bid1_size: Dict[str, int] = {}
        self._bid_drop_ratio = bid_drop_ratio
        # stats 增量計數器 — mark/_unmark 時維護,stats() 變 O(1)
        # (原本每次五趟全掃 history、拿的又是跟 tick handler 同一把鎖,
        #  前端每 2s 輪詢會在 8:30–9:00 高峰跟 mark/unmark 搶鎖)
        self._mark_events = 0
        self._unmark_events = 0
        self._unmark_reasons: Dict[str, int] = {}

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
                self._mark_events += 1
                self._max_bid_size[symbol] = max(bid1_size, 0)
                self._last_bid1_size[symbol] = max(bid1_size, 0)
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

    def unmark_manual(self, symbol: str) -> bool:
        """使用者盤前手動剔除 (①頁移除鈕) → 永久淘汰,不會再標記,
        08:59:58 預掛也不會下這檔。回 True=有移除、False=不在標記清單。"""
        return self._unmark(symbol, "manual_remove", {})

    def _unmark(self, symbol: str, reason: str, extra: dict) -> bool:
        with self._lock:
            if symbol not in self.marked:
                return False
            self.marked.discard(symbol)
            self.discarded.add(symbol)          # 永久淘汰，不能 re-mark
            self._max_bid_size.pop(symbol, None)
            self._last_bid1_size.pop(symbol, None)
            self._unmark_events += 1
            self._unmark_reasons[reason] = self._unmark_reasons.get(reason, 0) + 1
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
        """8:30 起每 tick 呼叫 — 記錄 max (只往上) 與 last (最新量),不觸發 unmark。
        08:59:58 的 final_check_all 拿這兩個值批次判。"""
        with self._lock:
            if symbol not in self.marked:
                return
            self._last_bid1_size[symbol] = current_bid1_size
            if current_bid1_size > self._max_bid_size.get(symbol, 0):
                self._max_bid_size[symbol] = current_bid1_size

    def final_check_all(self) -> list:
        """08:59:58 預掛前的**一次性批次判** (2026-08-13 定案,取代逐 tick final check):
        每檔 marked — 最新委買一量 (last) < mark 以來最高量 (max) × ratio → 淘汰。

        兩階段避免鎖重入 (Lock 非 RLock): 鎖內取快照選受害者 → 鎖外逐檔
        unmark_bid_dropped (冪等 — 若 tick 在兩階段之間搶先淘汰,這裡自然 no-op)。
        回 [(symbol, last, max), ...] = 實際被刷掉的,供 caller log/退訂。"""
        with self._lock:
            candidates = []
            for symbol in self.marked:
                prev_max = self._max_bid_size.get(symbol, 0)
                last = self._last_bid1_size.get(symbol, 0)
                if prev_max > 0 and last < prev_max * self._bid_drop_ratio:
                    candidates.append((symbol, last, prev_max))
        dropped = []
        for symbol, last, prev_max in candidates:
            if self.unmark_bid_dropped(symbol, last, prev_max):
                dropped.append((symbol, last, prev_max))
        return dropped

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

    def mark_opened(self, symbol: str):
        """該股已開盤 (首筆真成交到 = isOpen,或 book 轉逐筆 = isContinuous)。
        冪等;filter 之後對它退場,不再 mark/unmark (交給 trader/monitor)。"""
        with self._lock:
            self._opened.add(symbol)

    def is_opened(self, symbol: str) -> bool:
        with self._lock:
            return symbol in self._opened

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
        """O(1) — 純計數器組裝 (增量維護於 mark/_unmark),不掃 history。"""
        with self._lock:
            return {
                "currently_marked": len(self.marked),
                "total_mark_events": self._mark_events,
                "total_unmark_events": self._unmark_events,
                "unmark_by_ask_appeared": self._unmark_reasons.get("ask_appeared", 0),
                "unmark_by_bid_below_limit": self._unmark_reasons.get("bid_below_limit", 0),
                "unmark_by_bid_dropped": self._unmark_reasons.get("bid_dropped_half", 0),
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

    def get_max_bid_size(self, symbol: str) -> int:
        """該檔 mark 以來最大委買一量 (峰值) — Hub marked 快照帶給 node 判量減半用
        (node 08:59:50 才開訂,看不到 8:30–9:00 峰值,故須由 Hub 提供)。"""
        with self._lock:
            return self._max_bid_size.get(symbol, 0)

    def get_last_bid_size(self, symbol: str) -> int:
        """該檔最後已知委買一量 (last) — Hub 快照帶給 node 兩步 seed 用。
        若 node seed 只把 last 設成峰值,08:59:50–58 之間無新 tick 的量崩檔
        final_check_all (last < max×ratio) 永遠剔不掉 → 量減半失效。"""
        with self._lock:
            return self._last_bid1_size.get(symbol, 0)

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
