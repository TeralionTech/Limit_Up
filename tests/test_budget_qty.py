"""使用者情境驗證 (2026-08-11): 依金額 10 萬/檔 → 下 2 張 → 撤單/出場張數正確性。

設定: per_symbol_budget=100,000、漲停價 45 元 → _calc_lots = 100k // 45k = **2 張**,
下成**一張委託 2000 股** (不是兩張 1 張的單)。
撤單不帶數量 — 交易所語意 = 撤「該委託的未成交餘量」;
出場賣 filled_lots 全量 (2026-08-12 起 = 跌停價限價賣)。
"""
import time
from types import SimpleNamespace

from test_session_money import make_session, _fill, _fire_chase
from trader import Trader

LIMIT = 45.0
DOWN = 36.9                                       # 跌停價 (出場限價賣用)
COST_PER_LOT = 45_000


def _mk():
    s = make_session(per_symbol=100_000)          # 依金額: 每檔 10 萬
    s.set_limit_downs({"1101": DOWN})
    s.place_pre_orders(["1101"], {"1101": LIMIT})
    return s


class TestBudget100kTwoLots:
    def test_100k_budget_places_exactly_2_lots(self):
        # 前提驗證: 10萬 ÷ 4.5萬 = 2 張,單一委託
        s = _mk()
        assert s.trades["1101"].target_lots == 2
        assert s.broker.placed == [("limit_buy", "1101", LIMIT, 2)]
        assert s.budget_used == 2 * COST_PER_LOT              # 保留 9 萬

    def test_cancel_before_fill_releases_both_lots(self):
        # 未成交就撤 → 整單撤 (交易所撤 2 張餘量),9 萬保留全釋放
        s = _mk()
        order_no = s.trades["1101"].order_no
        s.cancel_symbol_orders("1101", "test")
        assert [c[0] for c in s.broker.cancelled] == [order_no]
        assert s.trades["1101"].order_status == "cancelled"
        assert s.budget_used == 0

    def test_cancel_after_1_of_2_filled_then_exit_sells_1(self):
        # 成交 1 張後撤 → 交易所只撤餘量 1 張;帳內留 1 張成本
        # 之後出場 → 賣「已成交的 1 張」,不是 2 張
        s = _mk()
        s._on_fill(_fill(s.trades["1101"].order_no, "1101", 1, LIMIT))
        s.cancel_symbol_orders("1101", "test")
        st = s.trades["1101"]
        assert st.filled_lots == 1
        assert s.budget_used == 1 * COST_PER_LOT
        s._exit_worker("1101", "mkt_queue_gone")
        assert ("limit_sell", "1101", DOWN, 1) in s.broker.placed

    def test_exit_sells_exactly_2_after_full_fill(self):
        # 2 張全成 → 出場 → 跌停價限價賣 2 張 (張數 = filled_lots,不多不少)
        s = _mk()
        s._on_fill(_fill(s.trades["1101"].order_no, "1101", 2, LIMIT))
        s._exit_worker("1101", "mkt_queue_gone")
        sells = [c for c in s.broker.placed if c[0] == "limit_sell"]
        assert sells == [("limit_sell", "1101", DOWN, 2)]

    def test_end_to_end_queue_gone_sells_2(self):
        # 端到端 (2026-08-12 訊號): 市價買隊伍曾出現後歸零 → trader 觸發出場
        # → 背景 thread 跌停價限價賣 2 張
        s = _mk()
        s._on_fill(_fill(s.trades["1101"].order_no, "1101", 2, LIMIT))
        t = Trader(watchlist=["1101"], limit_ups={"1101": LIMIT},
                   cfg=SimpleNamespace(first_trade_min_lots=10,
                                       bid_decline_sample_sec=60,
                                       bid_decline_minutes=5),
                   session=s)
        t.on_book("1101", [{"price": 0.0, "size": 300},
                           {"price": LIMIT, "size": 500}], [])       # 市價隊伍在 (arm)
        t.on_book("1101", [{"price": LIMIT, "size": 500}], [])       # 市價隊伍沒有了!
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and not any(
                c[0] == "limit_sell" for c in s.broker.placed):
            time.sleep(0.01)
        assert ("limit_sell", "1101", DOWN, 2) in s.broker.placed
        assert s.trades["1101"].exited is True

    def test_overbuy_2x_exit_sells_all_4(self):
        # 超買殘餘 race: 委託成功後撤預掛 P,但 P 撤單與成交在券商端賽跑 — 撤輸 →
        # 預掛 2 張 + 市價 2 張**都**成交 → 部位 4 張 → 出場必須賣 4 張全量 (非 target 2)
        s = _mk()
        pre_no = s.trades["1101"].order_no
        _fire_chase(s, "1101")                       # 市價盲送 shortfall=2 (filled=0) + 撤預掛 P
        chase_no = s.trades["1101"].order_no
        assert ("market_buy", "1101", None, 2) in s.broker.placed    # 差額 = 2 張
        # 撤單與成交在券商端賽跑 — 結果兩張單都成交
        s._on_fill(_fill(pre_no, "1101", 2, LIMIT))
        s._on_fill(_fill(chase_no, "1101", 2, LIMIT, filled_no="F2"))
        st = s.trades["1101"]
        assert st.filled_lots == 4
        s._exit_worker("1101", "mkt_queue_gone")
        assert ("limit_sell", "1101", DOWN, 4) in s.broker.placed
        # 附註: 超買 race 時 budget_used 低估實際花費 (保留轉移只做過一份) — 已接受的近似
        assert s.budget_used == 2 * COST_PER_LOT

    def test_1323_cancel_all_cancels_whole_order(self):
        # 13:23 全撤: 2 張 pending 整單撤、預算歸零、持倉概念不受影響
        s = _mk()
        s.cancel_all_pending("cancel_pending_time")
        assert len(s.broker.cancelled) == 1
        assert s.trades["1101"].order_status == "cancelled"
        assert s.budget_used == 0


class TestSymbolBudgetOverride:
    """個股金額覆寫 (2026-08-24): 有專屬金額的檔依專屬金額下,其餘用全域。"""

    def test_override_sizes_by_own_amount(self):
        # 全域每檔 10 萬 (=2 張),但 1101 專屬 20 萬 → 依 20萬 = 4 張
        s = make_session(per_symbol=100_000)
        s.set_symbol_budgets({"1101": 200_000})
        s.place_pre_orders(["1101"], {"1101": LIMIT})
        assert s.trades["1101"].target_lots == 4        # 20萬 ÷ 4.5萬 = 4
        assert s.broker.placed == [("limit_buy", "1101", LIMIT, 4)]

    def test_non_override_symbol_uses_global(self):
        # 有覆寫的是 9999,清單裡的 1101 沒覆寫 → 用全域 10萬 = 2 張
        s = make_session(per_symbol=100_000)
        s.set_symbol_budgets({"9999": 200_000})
        s.place_pre_orders(["1101"], {"1101": LIMIT})
        assert s.trades["1101"].target_lots == 2

    def test_override_beats_fixed_lots_mode(self):
        # 全域是 fixed_lots=1,但 1101 有專屬金額 20萬 → 仍依金額 = 4 張 (覆寫無視全域模式)
        s = make_session(sizing_mode="fixed_lots", fixed_lots=1)
        s.set_symbol_budgets({"1101": 200_000})
        s.place_pre_orders(["1101"], {"1101": LIMIT})
        assert s.trades["1101"].target_lots == 4

    def test_override_still_capped_by_total_budget(self):
        # 專屬金額 500萬 但總預算只剩夠買 3 張 → 受總預算硬上限
        s = make_session(total=3 * COST_PER_LOT, per_symbol=100_000)
        s.set_symbol_budgets({"1101": 5_000_000})
        s.place_pre_orders(["1101"], {"1101": LIMIT})
        assert s.trades["1101"].target_lots == 3

    def test_override_below_one_lot_still_buys_one(self):
        # 2026-08-24: 專屬金額不足一張 (1萬 < 4.5萬/張) → 最少買 1 張 (使用者明確指定要買)
        s = make_session(per_symbol=100_000)
        s.set_symbol_budgets({"1101": 10_000})
        s.place_pre_orders(["1101"], {"1101": LIMIT})
        assert s.trades["1101"].target_lots == 1
        assert s.broker.placed == [("limit_buy", "1101", LIMIT, 1)]

    def test_override_min_one_still_needs_total_budget(self):
        # 「最少一張」仍受總預算: 連 1 張都買不起 → 0 (跳過)
        s = make_session(total=10_000, per_symbol=100_000)   # 總預算 < 一張 4.5萬
        s.set_symbol_budgets({"1101": 10_000})
        s.place_pre_orders(["1101"], {"1101": LIMIT})
        assert s.trades["1101"].target_lots == 0


class TestRiskControlSizing:
    """風控①禁現沖減半 + ②20%委託量上限 (2026-08-24;疊在 _calc_lots 之後)。"""

    def test_day_trade_ban_halves(self):
        # 全域 10萬=2張;禁現沖 → 減半 = 1 張
        s = make_session(per_symbol=100_000)
        s.set_day_tradable({"1101": False})
        s.place_pre_orders(["1101"], {"1101": LIMIT})
        assert s.trades["1101"].target_lots == 1
        assert s.trades["1101"].chase_lots == 1        # 盲送每筆張數同 target

    def test_day_tradable_true_no_halve(self):
        s = make_session(per_symbol=100_000)
        s.set_day_tradable({"1101": True})
        s.place_pre_orders(["1101"], {"1101": LIMIT})
        assert s.trades["1101"].target_lots == 2

    def test_20pct_cap(self):
        # 全域 100萬=22張,但漲停價委託量 50 張 → 上限 20% = 10 張
        s = make_session(per_symbol=1_000_000)
        s.place_pre_orders(["1101"], {"1101": LIMIT}, limit_up_bid_vols={"1101": 50})
        assert s.trades["1101"].target_lots == 10     # min(22, floor(50*0.2)=10)

    def test_20pct_not_applied_when_snapshot_zero(self):
        # 沒抓到快照 (0) → 不套 20% (靠其他上限)
        s = make_session(per_symbol=100_000)
        s.place_pre_orders(["1101"], {"1101": LIMIT}, limit_up_bid_vols={"1101": 0})
        assert s.trades["1101"].target_lots == 2

    def test_combined_ban_then_20pct(self):
        # 疊加順序: base 22張 → 禁現沖//2 = 11 → 20%(委託量30→6) → min = 6
        s = make_session(per_symbol=1_000_000)
        s.set_day_tradable({"1101": False})
        s.place_pre_orders(["1101"], {"1101": LIMIT}, limit_up_bid_vols={"1101": 30})
        assert s.trades["1101"].target_lots == 6      # min(22//2=11, 30*0.2=6)

    def test_override_then_risk_controls(self):
        # 個股金額覆寫 (base) 也吃風控: 覆寫 100萬=22張 → 禁現沖//2 = 11
        s = make_session(per_symbol=100_000)
        s.set_symbol_budgets({"1101": 1_000_000})
        s.set_day_tradable({"1101": False})
        s.place_pre_orders(["1101"], {"1101": LIMIT})
        assert s.trades["1101"].target_lots == 11
