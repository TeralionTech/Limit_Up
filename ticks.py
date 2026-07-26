"""台股價格 tick 級距表 (與 day_trade_system cost_sheet.PRICE_TIERS 一致)。

隔日賣標的出場時算「賣一往下一個 tick」用。
"""
from __future__ import annotations

# [下界, 上界) → tick 大小
PRICE_TIERS = [
    (0.01, 10, 0.01),
    (10, 50, 0.05),
    (50, 100, 0.1),
    (100, 500, 0.5),
    (500, 1000, 1.0),
    (1000, 99999, 5.0),
]


def get_tick_size(price: float) -> float:
    """回該價格所屬級距的 tick 大小。"""
    for lo, hi, tick in PRICE_TIERS:
        if lo <= price < hi:
            return tick
    return 5.0


def _round_to_tick(price: float) -> float:
    """把價格對齊到合法 tick (四捨五入到最近檔位),避免浮點誤差被 SDK 拒。"""
    tick = get_tick_size(price)
    # 用整數運算避免浮點 (tick 最小 0.01)
    steps = round(price / tick)
    return round(steps * tick, 2)


def spread_ticks(bid1: float, ask1: float) -> int:
    """(賣一 − 買一) 換算成幾個 tick。tick 大小以買一價所屬級距為準。"""
    if bid1 <= 0 or ask1 <= 0 or ask1 <= bid1:
        return 0
    tick = get_tick_size(bid1)
    return int(round((ask1 - bid1) / tick))


def overnight_sell_price(bid1: float, ask1: float, spread_threshold: int = 5) -> float:
    """隔日賣標的出場價 (使用者定案 2026-07-27):
    - (賣一 − 買一) ≥ spread_threshold 個 tick → 賣一往下一個 tick (積極賣但不砸買一)
    - 否則 → 直接掛買一價
    ask1 無效 (無賣單/鎖漲停) → 退回買一價。回對齊 tick 後的價格;無效回 0。
    """
    if bid1 <= 0:
        return 0.0
    if ask1 <= 0 or ask1 <= bid1:
        return _round_to_tick(bid1)
    if spread_ticks(bid1, ask1) >= spread_threshold:
        price = ask1 - get_tick_size(ask1)      # 賣一往下一檔
        if price < bid1:                        # 不砸破買一
            price = bid1
        return _round_to_tick(price)
    return _round_to_tick(bid1)
