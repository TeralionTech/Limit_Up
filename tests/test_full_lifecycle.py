"""全生命週期整合測試 (2026-08-13 總驗收) — 真的 State + filter handler + Trader +
TradingSession(FakeBroker) 走完整天:

案例 1: 8:30 篩選 → 08:59:58 預掛 → 9:00 試撮忽略/首筆成交 → 市價盲送 (送 shortfall、委託成功撤預掛 P)
        → 成交 → 盤中市價隊伍歸零 → 跌停價賣出 → 部位歸零
案例 2: 市價隊伍歸零觸發出場的瞬間「其實已成交、回報晚 0.3 秒才到」→ 等待窗口接住 → 照樣賣掉
案例 3: 市價隊伍歸零觸發出場、真的沒成交 → 只撤不賣、exited 回退、預算全釋放
案例 4: 隔日賣標的今日又鎖漲停 → 抱著;委買一跌下漲停 → 跌停價賣 → 收盤不帶明天
"""
import threading
import time
from types import SimpleNamespace

from test_session_money import make_session, _fill, _fire_chase
from filter import make_on_book_handler
from state import State
from trader import Trader, Holding

A, B, C = "1101", "2330", "3008"
LIMIT_UPS = {A: 100.0, B: 50.0, C: 30.0}
DOWN_A = 81.9                                   # A 的跌停價 (出場限價賣用)


def _trader_cfg():
    return SimpleNamespace(bid_decline_sample_sec=60, bid_decline_minutes=5)


def _filter_cfg():
    # final check 窗口推到深夜 → 測試全程都在「只記錄 max」時段,量減半規則不干擾篩選
    return SimpleNamespace(final_check_start="23:59:58", pre_order_time="23:59:59")


def _wait(cond, timeout=4.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(0.01)
    return cond()


def _sells(broker):
    return [c for c in broker.placed if c[0] in ("market_sell", "limit_sell")]


def _lock_book(price):
    return [{"price": price, "size": 500}]      # 漲停價買單,無賣單


def _run_filter_phase(session):
    """8:30 篩選: A 鎖住 mark、B 鎖了又出賣單 (淘汰+撤單 callback)、C 沒鎖。"""
    state = State(bid_drop_ratio=0.5)
    unsubbed = []
    unsub_ref = {"fn": lambda sym: (unsubbed.append(sym),
                                    session.cancel_symbol_orders(sym, "unmarked"))}
    on_book = make_on_book_handler(state, LIMIT_UPS, _filter_cfg(), unsub_ref)

    on_book(A, _lock_book(100.0), [])                        # A 鎖漲停 → mark
    on_book(B, _lock_book(50.0), [])                         # B 先鎖
    on_book(B, _lock_book(50.0), [{"price": 50.5, "size": 3}])   # B 出賣單 → 淘汰
    on_book(C, [{"price": 29.5, "size": 100}], [])           # C 沒鎖 → 不 mark

    assert state.get_marked_prioritized() == [A]
    assert state.is_discarded(B)
    assert not state.is_marked(C)
    assert unsubbed == [B]                                   # 淘汰即退訂 callback
    return state


class TestFullLifecycle:
    def test_happy_path_full_day(self):
        s = make_session()                                   # 每檔 20萬 → A@100 = 2 張
        s.set_limit_downs({A: DOWN_A})

        # ── 8:30 篩選 ──
        state = _run_filter_phase(s)

        # ── 08:59:58 批次 final check (2026-08-13): D 曾衝 100 張、最後剩 30 → 刷掉 ──
        state.mark("5678", 20.0, 100, 20.0)
        state.update_max_bid("5678", 30)
        assert state.final_check_all() == [("5678", 30, 100)]
        marked = state.get_marked_prioritized()
        assert marked == [A]                                 # 清單就此定案

        # ── 08:59:58 預掛 (只有 A;B 已淘汰、C 沒 mark、D 量減半刷掉) ──
        s.place_pre_orders(marked, LIMIT_UPS)
        assert [c[1] for c in s.broker.placed] == [A]
        assert s.trades[A].target_lots == 2
        assert s.budget_used == 200_000
        pre_no = s.trades[A].order_no

        # ── 9:00 ── trader 接手
        t = Trader(watchlist=marked, limit_ups=LIMIT_UPS, cfg=_trader_cfg(),
                   state=state, session=s)
        # 試撮 tick 必須被忽略 (2026-08-10 2491 事故的修復)
        t.on_trade(A, {"price": 100.0, "size": 94000, "isTrial": True})
        assert t.holdings[A].first_trade_seen is False
        assert s.trades[A].first_trade_fired is False
        # 真首筆成交到 → on_first_trade 設旗標 (無量門檻;不再觸發市價追)
        t.on_trade(A, {"price": 100.0, "size": 94000})
        assert s.trades[A].first_trade_fired is True
        # 9:00 市價盲送 (時間驅動,取代首筆成交觸發) — 送 shortfall=2 (filled=0),委託成功後撤預掛 P
        pre_no_A = s.trades[A].pre_order_no
        _fire_chase(s, A)
        assert ("market_buy", A, None, 2) in s.broker.placed
        kinds = [c[0] for c in s.broker.calls]
        assert kinds.index("limit_buy") < kinds.index("market_buy")   # 預掛先、盲送後
        assert kinds.index("market_buy") < kinds.index("cancel")      # 送 M 後才撤預掛 P
        assert [c[0] for c in s.broker.cancelled] == [pre_no_A]       # 委託成功 → 撤預掛剩餘
        chase_no = s.trades[A].order_no

        # ── 市價單成交 2 張 ──
        s._on_fill(_fill(chase_no, A, 2, 100.0))
        assert s.trades[A].filled_lots == 2
        assert s.trades[A].budget_reserved == 0              # 保留全轉消耗

        # ── 盤中: 市價買隊伍出現 → 歸零 → 出場 (跌停價限價賣) ──
        t.on_book(A, [{"price": 0.0, "size": 800}, {"price": 100.0, "size": 500}], [])
        t.on_book(A, [{"price": 100.0, "size": 500}], [])    # 市價隊伍沒有了!
        assert _wait(lambda: _sells(s.broker)), "支撐消失後沒有賣單"
        assert ("limit_sell", A, DOWN_A, 2) in s.broker.placed
        assert s.trades[A].exited is True

        # ── 賣出成交 → 部位歸零;預算不退 (保守日預算) ──
        sell_no = f"O{s.broker._n}"
        s._on_fill(_fill(sell_no, A, 2, 100.0, action="sell", filled_no="F9"))
        assert s.trades[A].filled_lots == 0
        assert s.budget_used == 200_000

    def test_race_fill_during_exit(self, monkeypatch):
        """使用者情境: 市價隊伍歸零觸發出場,但歸零正是自己的單被吃掉 — 回報晚 0.3 秒才到。
        exit worker 的等待窗口要接住,照樣跌停價賣掉。"""
        import trading_session as ts
        monkeypatch.setattr(ts, "_EXIT_FILL_WAIT_SEC", 1.5)
        s = make_session()
        s.set_limit_downs({A: DOWN_A})
        state = _run_filter_phase(s)
        s.place_pre_orders(state.get_marked_prioritized(), LIMIT_UPS)
        pre_no = s.trades[A].order_no
        s.trades[A].first_trade_fired = True                 # 擋市價追,單測撤單線

        t = Trader(watchlist=[A], limit_ups=LIMIT_UPS, cfg=_trader_cfg(),
                   state=state, session=s)
        # 首筆 books (市價隊伍 800) + 首筆成交 → TRACKING
        t.on_book(A, [{"price": 0.0, "size": 800}, {"price": 100.0, "size": 500}], [])
        t.on_trade(A, {"price": 100.0, "size": 50000})
        assert t.holdings[A].status == Holding.TRACKING

        # 成交回報 0.3 秒後才到 (模擬「出場瞬間其實已成交」)
        threading.Timer(0.3, lambda: s._on_fill(_fill(pre_no, A, 2, 100.0))).start()
        # 市價隊伍歸零 tick → 出場 → exit worker: 撤單 + 等回報 + 賣
        t.on_book(A, [{"price": 100.0, "size": 500}], [])
        assert t.holdings[A].status == Holding.PULLED
        assert _wait(lambda: ("limit_sell", A, DOWN_A, 2) in s.broker.placed), \
            "搶到的 2 張部位沒有被賣掉 (競態沒接住)"
        assert s.trades[A].exited is True
        kinds = [c[0] for c in s.broker.calls]
        assert kinds.index("cancel") < kinds.index("limit_sell")   # 先撤 (被拒也無妨) 後賣

    def test_exit_no_fill_cancel_only(self, monkeypatch):
        """市價隊伍歸零但真的沒成交 → 只撤單不賣、exited 回退 (晚到部位仍有保護)、預算全釋放。"""
        import trading_session as ts
        monkeypatch.setattr(ts, "_EXIT_FILL_WAIT_SEC", 0.3)
        s = make_session()
        s.set_limit_downs({A: DOWN_A})
        state = _run_filter_phase(s)
        s.place_pre_orders(state.get_marked_prioritized(), LIMIT_UPS)
        s.trades[A].first_trade_fired = True

        t = Trader(watchlist=[A], limit_ups=LIMIT_UPS, cfg=_trader_cfg(),
                   state=state, session=s)
        t.on_book(A, [{"price": 0.0, "size": 800}, {"price": 100.0, "size": 500}], [])
        t.on_trade(A, {"price": 100.0, "size": 50000})
        t.on_book(A, [{"price": 100.0, "size": 500}], [])          # 市價隊伍歸零
        assert _wait(lambda: s.broker.cancelled), "撤單沒到 broker"
        assert _wait(lambda: s.trades[A].exited is False, timeout=2), "exited 沒回退"
        assert _sells(s.broker) == []                        # 沒部位 → 不賣
        assert s.budget_used == 0                            # 未成交保留全釋放
        assert s.trades[A].order_status == "cancelled"

    def test_overnight_lock_hold_then_bid_drop_sell(self):
        """案例 4 (2026-08-16 純盤面規則): 隔日賣標的今天又鎖漲停 → 抱著不賣;
        盤中委買一跌下漲停 → 跌停價限價賣 → 成交 → 清單保留但收盤不帶明天。"""
        from test_exit_logic import _InvBroker
        D, UP_D, DOWN_D = "5285", 52.0, 41.5
        s = make_session()
        s.broker = _InvBroker([{"symbol": D, "lots": 3}])
        s.load_overnight([{"symbol": D, "lots": 3, "avg_cost": 47.0}])
        s.refresh_overnight_inventory()                      # 對帳 → reconciled + 張數以庫存為準
        assert s.overnight[D]["reconciled"] is True
        s.set_overnight_limit_ups({D: UP_D})
        s.set_limit_downs({D: DOWN_D})

        t = Trader(watchlist=[], limit_ups={}, cfg=_trader_cfg(), session=s)
        # 9:00 開盤鎖著 (市價隊伍+買牆) → 抱著;成交 tick 不觸發任何隔日賣
        t.on_book(D, [{"price": 0.0, "size": 900}, {"price": UP_D, "size": 400}], [])
        t.on_trade(D, {"price": UP_D, "size": 120000})
        time.sleep(0.2)
        assert _sells(s.broker) == []
        assert s.overnight[D]["locked_now"] is True
        # 盤中委買一跌下漲停 → 跌停價限價賣全部持倉
        t.on_book(D, [{"price": 51.5, "size": 200}], [])
        assert _wait(lambda: _sells(s.broker)), "跌下漲停後沒賣"
        assert _sells(s.broker) == [("limit_sell", D, DOWN_D, 3)]
        assert s.overnight[D]["locked_now"] is False
        # 賣單成交 → sold_lots 記帳;清單不移除,但收盤不再帶到明天
        sell_no = s.overnight[D]["sell_order_no"]
        s._on_fill(_fill(sell_no, D, 3, DOWN_D, action="sell", filled_no="F41"))
        assert s.overnight[D]["sold_lots"] == 3
        assert D in s.overnight                              # 賣掉不移除清單
        assert all(c["symbol"] != D for c in s.get_overnight_candidates())
