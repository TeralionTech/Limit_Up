"""TradingSession 錢相關邏輯 — 張數計算 / 預算保留-轉消耗-釋放不變式 / 預掛冪等 / 隔日賣清單。

FakeBroker 滿足 _broker_ready (connected/healthy) — 不需要富邦 SDK。
預算不變式: budget_used == Σ(買進已成交×漲停×1000) + Σ(st.budget_reserved)。
"""
import pytest

from trading_session import TradingSession


class FakeBroker:
    def __init__(self):
        self.connected = True
        self.healthy = True
        self.placed = []          # (kind, symbol, price, lots)
        self.cancelled = []       # (order_no, symbol, reason)
        self.calls = []           # 依序記錄 (kind, ref) — 驗證「市價單先於撤單」
        self._n = 0

    def _next(self):
        self._n += 1
        return f"O{self._n}"

    def place_limit_buy(self, symbol, price, lots):
        no = self._next()
        self.placed.append(("limit_buy", symbol, price, lots))
        self.calls.append(("limit_buy", symbol))
        return no

    def place_market_buy(self, symbol, lots):
        no = self._next()
        self.placed.append(("market_buy", symbol, None, lots))
        self.calls.append(("market_buy", symbol))
        return no

    def place_market_sell(self, symbol, lots, reason=""):
        no = self._next()
        self.placed.append(("market_sell", symbol, None, lots))
        self.calls.append(("market_sell", symbol))
        return no

    def place_limit_sell(self, symbol, price, lots, reason=""):
        no = self._next()
        self.placed.append(("limit_sell", symbol, price, lots))
        self.calls.append(("limit_sell", symbol))
        return no

    def cancel(self, order_no, symbol, reason=""):
        self.cancelled.append((order_no, symbol, reason))
        self.calls.append(("cancel", order_no))

    def get_order_filled_lots(self, order_no):
        # 2026-08-03 定案: 首筆成交快路徑不再查權威成交量 (速度優先) — 有人呼叫就是退化
        raise AssertionError("快路徑不應查權威成交量")

    def get_inventories(self):
        return []


def make_session(total=1_000_000, per_symbol=200_000,
                 sizing_mode="budget", fixed_lots=0):
    s = TradingSession()
    s.roll_day("2026-08-03")
    s.set_mode("real")
    s.broker = FakeBroker()
    s.set_params(total_budget=total, per_symbol_budget=per_symbol,
                 sizing_mode=sizing_mode, fixed_lots=fixed_lots)
    s.set_armed(True)
    s.order_min_interval = 0.0      # 測試不等送單間隔
    return s


def _fill(order_no, symbol, lots, price, action="buy", filled_no="F1"):
    return {"order_no": order_no, "symbol": symbol, "lots": lots, "price": price,
            "action": action, "filled_no": filled_no, "filled_time": "09:00:00",
            "quantity": lots * 1000}


def _fire_chase(s, symbol):
    """直接跑一次市價盲送 worker (start 過去 / cutoff 遠未來 → 略過等待、立刻送,
    首筆委託成功/致命拒單即停)。取代舊的 _first_trade_worker 直呼。"""
    from datetime import time as _tm
    s._market_chase_worker(symbol, _tm(0, 0, 0), _tm(23, 59, 59))


class TestCalcLots:
    def test_budget_mode_min_of_caps(self):
        s = make_session(total=1_000_000, per_symbol=200_000)
        assert s._calc_lots(100.0) == 2      # min(200k, 1M) // 100k
        assert s._calc_lots(50.0) == 4

    def test_total_budget_is_hard_cap(self):
        s = make_session(total=150_000, per_symbol=200_000)
        assert s._calc_lots(100.0) == 1      # 餘額 150k 只夠 1 張

    def test_fixed_lots_capped_by_budget(self):
        s = make_session(total=300_000, per_symbol=0,
                         sizing_mode="fixed_lots", fixed_lots=5)
        assert s._calc_lots(100.0) == 3      # min(5, 300k//100k)

    def test_zero_cost_returns_zero(self):
        s = make_session()
        assert s._calc_lots(0) == 0


class TestPlacePreOrders:
    def test_reserves_budget_per_symbol(self):
        s = make_session(total=1_000_000, per_symbol=200_000)
        s.place_pre_orders(["2330", "1101"], {"2330": 100.0, "1101": 50.0})
        assert len(s.broker.placed) == 2
        assert s.budget_used == 400_000                       # 2×100k + 4×50k
        assert s.trades["2330"].budget_reserved == 200_000
        assert s.trades["1101"].budget_reserved == 200_000
        assert s.trades["2330"].order_status == "pending"

    def test_skips_price_above_max(self):
        s = make_session()
        s.configure(max_stock_price=500)
        s.place_pre_orders(["9999"], {"9999": 600.0})
        assert s.broker.placed == []
        assert s.trades["9999"].stopped_reason == "price_above_max"
        assert s.budget_used == 0

    def test_skips_when_budget_exhausted(self):
        s = make_session(total=100_000, per_symbol=200_000)
        s.place_pre_orders(["2330", "1101"], {"2330": 100.0, "1101": 100.0})
        assert len(s.broker.placed) == 1                      # 第二檔沒錢
        assert s.trades["1101"].stopped_reason == "budget_exhausted"
        assert s.budget_used == 100_000

    def test_idempotent_same_day(self):
        # Fix: 重複 timer / 重複呼叫也只預掛一次 (冪等保護)
        s = make_session()
        s.place_pre_orders(["2330"], {"2330": 100.0})
        n = len(s.broker.placed)
        s.place_pre_orders(["2330", "1101"], {"2330": 100.0, "1101": 50.0})
        assert len(s.broker.placed) == n
        assert s.budget_used == 200_000

    def test_not_armed_skips_without_burning_idempotence(self):
        s = make_session()
        s.set_armed(False)
        s.place_pre_orders(["2330"], {"2330": 100.0})
        assert s.broker.placed == []
        s.set_armed(True)                                     # 之後 arm 回來仍可預掛
        s.place_pre_orders(["2330"], {"2330": 100.0})
        assert len(s.broker.placed) == 1


class TestBudgetInvariant:
    def test_rejection_releases_reservation(self):
        # Fix: 交易所拒單必須釋放保留 — 原本每次拒單永久吃掉當日額度
        s = make_session()
        s.place_pre_orders(["2330"], {"2330": 100.0})
        order_no = s.trades["2330"].order_no
        assert s.budget_used == 200_000
        s._on_order({"order_no": order_no, "symbol": "2330",
                     "error_message": "insufficient"})
        assert s.budget_used == 0
        assert s.trades["2330"].budget_reserved == 0
        assert s.trades["2330"].order_status == "rejected"

    def test_partial_fill_then_cancel_keeps_filled_cost(self):
        s = make_session()
        s.place_pre_orders(["2330"], {"2330": 100.0})         # 2 張,保留 200k
        order_no = s.trades["2330"].order_no
        s._on_fill(_fill(order_no, "2330", 1, 100.0))         # 成交 1 張: 保留 100k→消耗
        assert s.trades["2330"].filled_lots == 1
        assert s.trades["2330"].budget_reserved == 100_000
        s.cancel_symbol_orders("2330", "test")                # 撤剩餘 → 釋放 100k
        assert s.budget_used == 100_000                       # 只剩已成交的消耗
        assert s.trades["2330"].budget_reserved == 0

    def test_fill_dedup(self):
        s = make_session()
        s.place_pre_orders(["2330"], {"2330": 100.0})
        order_no = s.trades["2330"].order_no
        f = _fill(order_no, "2330", 1, 100.0)
        s._on_fill(f)
        s._on_fill(dict(f))                                   # 重複回報 → 去重
        assert s.trades["2330"].filled_lots == 1

    def test_sell_rejection_does_not_touch_budget(self):
        s = make_session()
        s.place_pre_orders(["2330"], {"2330": 100.0})
        order_no = s.trades["2330"].order_no
        s._on_fill(_fill(order_no, "2330", 2, 100.0))         # 全成 → done
        assert s.budget_used == 200_000
        st = s.trades["2330"]
        assert s._sell_position("2330", st, 2, "test", max_tries=1) is True
        sell_no = f"O{s.broker._n}"
        s._on_order({"order_no": sell_no, "symbol": "2330", "error_message": "x"})
        assert s.budget_used == 200_000                       # 賣單拒單不動買方預算


class TestOvernightCandidates:
    def test_union_and_filtering(self):
        s = make_session()
        # 今日成交未出場 → 帶
        s.place_pre_orders(["2330"], {"2330": 100.0})
        s._on_fill(_fill(s.trades["2330"].order_no, "2330", 2, 100.0))
        # 今日已出場 → 不帶
        from trading_session import SymbolTrade
        st_exited = SymbolTrade("4444", 50.0)
        st_exited.filled_lots = 1
        st_exited.exited = True
        s.trades["4444"] = st_exited
        # 昨日帶來: 還有剩 → 帶;賣完且已對帳 → 不帶;手動未對帳 → 帶
        s.load_overnight([{"symbol": "5555", "lots": 3, "avg_cost": 10.0},
                          {"symbol": "6666", "lots": 2, "avg_cost": 20.0},
                          {"symbol": "7777", "lots": 0, "avg_cost": 0.0}])
        s.overnight["5555"].update(reconciled=True, sold_lots=1)
        s.overnight["6666"].update(reconciled=True, sold_lots=2)
        # 7777: lots 0, reconciled False → 待對帳,保留
        out = {row["symbol"]: row for row in s.get_overnight_candidates()}
        assert set(out) == {"2330", "5555", "7777"}
        assert out["2330"]["lots"] == 2
        assert out["5555"]["lots"] == 2                       # 3 - 1
        assert out["7777"]["lots"] == 0

    def test_today_trade_wins_over_carryover(self):
        s = make_session()
        s.place_pre_orders(["2330"], {"2330": 100.0})
        s._on_fill(_fill(s.trades["2330"].order_no, "2330", 2, 100.0))
        s.load_overnight([{"symbol": "2330", "lots": 9, "avg_cost": 90.0}])
        out = {row["symbol"]: row for row in s.get_overnight_candidates()}
        assert out["2330"]["lots"] == 2                       # 今日成交優先,不重複


class TestManageGates:
    def test_cancel_works_when_disarmed(self):
        # Fix: 關 kill switch 後撤單仍要能動 (_can_manage 而非 is_live)
        s = make_session()
        s.place_pre_orders(["2330"], {"2330": 100.0})
        s.set_armed(False)
        s.cancel_symbol_orders("2330", "kill_switch_test")
        assert len(s.broker.cancelled) == 1
        assert s.trades["2330"].order_status == "cancelled"
        assert s.budget_used == 0

    def test_cancel_all_pending_works_when_disarmed(self):
        s = make_session()
        s.place_pre_orders(["2330", "1101"], {"2330": 100.0, "1101": 50.0})
        s.set_armed(False)
        s.cancel_all_pending("cancel_pending_time")
        assert len(s.broker.cancelled) == 2

    def test_sell_position_works_when_disarmed(self):
        s = make_session()
        s.place_pre_orders(["2330"], {"2330": 100.0})
        s._on_fill(_fill(s.trades["2330"].order_no, "2330", 2, 100.0))
        s.set_armed(False)
        st = s.trades["2330"]
        assert s._sell_position("2330", st, 2, "test", max_tries=1) is True
        assert ("market_sell", "2330", None, 2) in s.broker.placed

    def test_entry_still_requires_armed(self):
        s = make_session()
        s.set_armed(False)
        s.place_pre_orders(["2330"], {"2330": 100.0})
        assert s.broker.placed == []                          # 進場仍看 kill switch


class TestMarketBlindChase:
    """9:00 市價盲送 (2026-08-24 定案 + 2026-08-25 修正: 時間驅動、送 shortfall(=target−已成交)、
    首筆委託成功即停、**委託成功即撤預掛剩餘 P**、預算轉移 (釋放預掛保留、改保留市價))。"""

    def test_fires_shortfall_cancels_preorder(self):
        s = make_session()
        s.place_pre_orders(["2330"], {"2330": 100.0})     # target=2, 預掛保留 200k
        pre_no = s.trades["2330"].pre_order_no
        _fire_chase(s, "2330")
        assert ("market_buy", "2330", None, 2) in s.broker.placed   # filled=0 → shortfall=2
        assert [c[0] for c in s.broker.cancelled] == [pre_no]       # 委託成功 → 撤預掛 P
        st = s.trades["2330"]
        assert st.order_kind == "market_buy" and st.order_status == "pending"
        # 預算轉移等額 (釋放預掛 200k、改保留市價 200k) → budget_used 不變
        assert s.budget_used == 200_000

    def test_sends_shortfall_not_full_target(self):
        # 「扣已買」: 預掛已成 1 張 → 盲送只送差額 shortfall=1 (非整包 target=2),免 2× 超買
        s = make_session()
        s.place_pre_orders(["2330"], {"2330": 100.0})
        s._on_fill(_fill(s.trades["2330"].order_no, "2330", 1, 100.0))
        _fire_chase(s, "2330")
        assert [c for c in s.broker.placed if c[0] == "market_buy"] == \
            [("market_buy", "2330", None, 1)]             # 差額 1 張,非固定 2

    def test_disposition_no_market_chase(self):
        s = make_session()
        s.set_dispositions({"2330": True})
        s.place_pre_orders(["2330"], {"2330": 100.0})
        _fire_chase(s, "2330")
        assert not [c for c in s.broker.placed if c[0] == "market_buy"]   # 處置股禁市價,不盲送

    def test_stopped_reason_aborts(self):
        s = make_session()
        s.place_pre_orders(["2330"], {"2330": 100.0})
        s.trades["2330"].stopped_reason = "first_check_failed"
        _fire_chase(s, "2330")
        assert not [c for c in s.broker.placed if c[0] == "market_buy"]

    def test_retries_transient_until_success(self):
        # 暫時性拒單 → 每 0.1s 重試到成功;首筆委託成功即停
        s = make_session()
        s.place_pre_orders(["2330"], {"2330": 100.0})
        fails = {"n": 0}
        orig = s.broker.place_market_buy
        def flaky(symbol, lots):
            if fails["n"] < 5:
                fails["n"] += 1
                raise RuntimeError("busy")
            return orig(symbol, lots)
        s.broker.place_market_buy = flaky
        _fire_chase(s, "2330")
        assert fails["n"] == 5
        assert s.trades["2330"].order_kind == "market_buy"

    def test_fatal_reject_stops(self):
        # 價格穩定/致命拒單 → 送一筆即停 (6144 事故防護)
        s = make_session()
        s.place_pre_orders(["6144"], {"6144": 100.0})
        calls = {"n": 0}
        def reject(symbol, lots):
            calls["n"] += 1
            raise RuntimeError("證券委託觸及價格穩定措施上、下限價格")
        s.broker.place_market_buy = reject
        _fire_chase(s, "6144")
        assert calls["n"] == 1
        assert s.trades["6144"].stopped_reason == "fatal_reject"


class TestFirstTradeGating:
    """on_first_trade 重寫 (2026-08-24): 只做第一盤淘汰,不再觸發市價追 (改時間驅動盲送)。"""

    def test_passed_only_sets_flag_no_order(self):
        s = make_session()
        s.place_pre_orders(["2330"], {"2330": 100.0})
        s.on_first_trade("2330", True)
        assert s.trades["2330"].first_trade_fired is True
        assert not [c for c in s.broker.placed if c[0] == "market_buy"]

    def test_not_passed_stops_and_cancels(self):
        import time as _t
        s = make_session()
        s.place_pre_orders(["2330"], {"2330": 100.0})
        s.on_first_trade("2330", False)                   # 首筆成交量太小 → 淘汰 + 撤預掛
        assert s.trades["2330"].stopped_reason == "first_check_failed"
        deadline = _t.monotonic() + 2
        while _t.monotonic() < deadline and not s.broker.cancelled:
            _t.sleep(0.01)
        assert s.broker.cancelled                          # async 撤預掛
        assert s.budget_used == 0

    def test_idempotent(self):
        s = make_session()
        s.place_pre_orders(["2330"], {"2330": 100.0})
        s.on_first_trade("2330", True)
        s.on_first_trade("2330", False)                    # 已 fired → 第二次不動作
        assert s.trades["2330"].stopped_reason == ""


class TestAsyncCancel:
    def test_cancel_async_completes_in_background(self):
        # 行情 thread 用的非同步撤單 — 呼叫立即返回,撤單在背景 thread 完成
        import time as _t
        s = make_session()
        s.place_pre_orders(["2330"], {"2330": 100.0})
        s.cancel_symbol_orders_async("2330", "unmarked")
        deadline = _t.monotonic() + 2.0
        while _t.monotonic() < deadline and not s.broker.cancelled:
            _t.sleep(0.01)
        assert len(s.broker.cancelled) == 1
        assert s.trades["2330"].order_status == "cancelled"
        assert s.budget_used == 0                    # 保留已釋放


class TestFatalReject:
    """致命拒因 (全額交割/預收圈存) — 第一筆被拒就停,不再狂送。
    2026-08-13 6225 事故: T30 名單漏抓時的第二層保險。"""

    FATAL_MSG = "全額處置股,預收或圈存不足,請洽營業員[4385166]"

    def test_pre_order_fatal_reject_stops_symbol(self):
        s = make_session()

        def _fatal(symbol, price, lots):
            raise RuntimeError(self.FATAL_MSG)
        s.broker.place_limit_buy = _fatal
        s.place_pre_orders(["6225"], {"6225": 36.75})
        assert s.trades["6225"].stopped_reason == "fatal_reject"
        assert s.budget_used == 0
        # 9:00 首筆成交來了也不追 (stopped_reason 擋)
        _fire_chase(s, "6225")
        assert not [c for c in s.broker.placed if c[0] == "market_buy"]

    def test_chase_stops_after_first_fatal_reject(self):
        s = make_session()
        s.place_pre_orders(["6225"], {"6225": 36.75})
        calls = {"n": 0}

        def _fatal(symbol, lots):
            calls["n"] += 1
            raise RuntimeError(self.FATAL_MSG)
        s.broker.place_market_buy = _fatal
        _fire_chase(s, "6225")
        assert calls["n"] == 1                           # 只送 1 筆就停 (原本會無限狂送)
        assert s.trades["6225"].stopped_reason == "fatal_reject"
        assert s.trades["6225"].order_kind == "pre_limit"   # 致命拒單 → 市價放棄、預掛 P 續守 (不撤)

    # 2026-08-21 6144 事故: 「價格穩定措施」拒單原本不在停止清單 → 26 秒狂送 100 筆
    PRICE_STABLE_MSG = "證券委託觸及價格穩定措施上、下限價格"

    def test_chase_stops_after_price_stable_reject(self):
        s = make_session()
        s.place_pre_orders(["6144"], {"6144": 14.65})
        calls = {"n": 0}

        def _reject(symbol, lots):
            calls["n"] += 1
            raise RuntimeError(self.PRICE_STABLE_MSG)
        s.broker.place_market_buy = _reject
        _fire_chase(s, "6144")
        assert calls["n"] == 1                            # 只送 1 筆就停 (原本狂送 100 筆)
        assert s.trades["6144"].stopped_reason == "fatal_reject"
        assert s.trades["6144"].order_kind == "pre_limit"   # 致命拒單 → 市價放棄、預掛 P 續守 (不撤)

    def test_non_fatal_reject_keeps_retrying(self):
        # 對照組: 非致命拒因照舊「試到成功為止」
        s = make_session()
        s.place_pre_orders(["1101"], {"1101": 50.0})
        fails = {"n": 0}
        orig = s.broker.place_market_buy

        def _flaky(symbol, lots):
            if fails["n"] < 5:
                fails["n"] += 1
                raise RuntimeError("busy")               # 無致命關鍵字
            return orig(symbol, lots)
        s.broker.place_market_buy = _flaky
        _fire_chase(s, "1101")
        assert fails["n"] == 5                           # 有重試
        assert s.trades["1101"].order_kind == "market_buy"   # 最終成功

    def test_sell_fatal_reject_stops_immediately(self):
        s = make_session()
        s.place_pre_orders(["1101"], {"1101": 50.0})
        s._on_fill(_fill(s.trades["1101"].order_no, "1101", 4, 50.0))
        st = s.trades["1101"]
        calls = {"n": 0}

        def _fatal(symbol, lots, reason=""):
            calls["n"] += 1
            raise RuntimeError(self.FATAL_MSG)
        s.broker.place_market_sell = _fatal              # 無跌停價 → market 兜底路徑
        assert s._sell_position("1101", st, 4, "test") is False
        assert calls["n"] == 1                           # 1 筆即停,不燒滿 8 次重試


class TestSellRetryBounded:
    def test_sell_gives_up_after_max_tries(self):
        # Fix: 賣單重試有上限 (原 while True 會無限狂送)
        s = make_session()
        s.place_pre_orders(["2330"], {"2330": 100.0})
        s._on_fill(_fill(s.trades["2330"].order_no, "2330", 2, 100.0))
        st = s.trades["2330"]

        calls = []
        def _always_fail(symbol, lots, reason=""):
            calls.append(symbol)
            raise RuntimeError("rejected")
        s.broker.place_market_sell = _always_fail

        assert s._sell_position("2330", st, 2, "test", max_tries=3) is False
        assert len(calls) == 3
