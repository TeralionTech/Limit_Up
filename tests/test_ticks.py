"""ticks.py — tick 級距 / 對齊 / 隔日賣出場價 (純函式,直接決定賣價)。"""
import ticks
from ticks import get_tick_size, _round_to_tick, spread_ticks, overnight_sell_price


class TestGetTickSize:
    def test_tier_boundaries(self):
        # [下界, 上界) → tick;上界屬於下一級距
        assert get_tick_size(5.0) == 0.01
        assert get_tick_size(9.99) == 0.01
        assert get_tick_size(10.0) == 0.05
        assert get_tick_size(49.95) == 0.05
        assert get_tick_size(50.0) == 0.1
        assert get_tick_size(99.9) == 0.1
        assert get_tick_size(100.0) == 0.5
        assert get_tick_size(499.5) == 0.5
        assert get_tick_size(500.0) == 1.0
        assert get_tick_size(999.0) == 1.0
        assert get_tick_size(1000.0) == 5.0
        assert get_tick_size(5000.0) == 5.0

    def test_below_lowest_tier_falls_through(self):
        # < 0.01 不在任何級距 → fallback 5.0 (現行行為,鎖住以防意外改動)
        assert get_tick_size(0.005) == 5.0


class TestRoundToTick:
    def test_round_to_nearest(self):
        assert _round_to_tick(50.13) == 50.1
        assert _round_to_tick(50.17) == 50.2
        assert _round_to_tick(10.02) == 10.0
        assert _round_to_tick(10.03) == 10.05

    def test_float_noise(self):
        assert _round_to_tick(21.150000000000002) == 21.15
        assert _round_to_tick(0.1 + 0.2) == 0.3


class TestSpreadTicks:
    def test_invalid_inputs_zero(self):
        assert spread_ticks(0, 50.5) == 0
        assert spread_ticks(50.0, 0) == 0
        assert spread_ticks(50.5, 50.5) == 0
        assert spread_ticks(50.5, 50.0) == 0    # ask <= bid

    def test_tick_from_bid_tier(self):
        assert spread_ticks(50.0, 50.5) == 5           # tick 0.1
        assert spread_ticks(99.9, 100.4) == 5          # 跨級距: 用買一級距 0.1
        assert spread_ticks(10.0, 10.05) == 1          # tick 0.05


class TestOvernightSellPrice:
    def test_invalid_bid_returns_zero(self):
        assert overnight_sell_price(0, 50.0) == 0.0
        assert overnight_sell_price(-1, 50.0) == 0.0

    def test_no_ask_falls_back_to_bid(self):
        # 無賣單/鎖漲停 (ask 無效) → 掛買一價
        assert overnight_sell_price(50.0, 0) == 50.0
        assert overnight_sell_price(50.0, 49.9) == 50.0   # ask <= bid

    def test_wide_spread_sells_one_tick_below_ask(self):
        # spread >= 5 ticks → 賣一往下一檔 (積極賣但不砸買一)
        assert overnight_sell_price(50.0, 50.9) == 50.8

    def test_wide_spread_floors_at_bid(self):
        # 賣一往下一檔 < 買一 → 不砸破買一,掛買一價
        # bid=99.7 ask=100.18: spread 4.8→round 5;ask tick 0.5 → 99.68 < 99.7 → floor
        assert overnight_sell_price(99.7, 100.18) == 99.7

    def test_narrow_spread_sells_at_bid(self):
        assert overnight_sell_price(50.0, 50.2) == 50.0

    def test_result_is_tick_aligned(self):
        p = overnight_sell_price(33.33, 0)
        assert p == _round_to_tick(p)
