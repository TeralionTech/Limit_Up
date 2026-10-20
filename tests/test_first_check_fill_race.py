"""2026-10-20 8089 (node2/node3/node6): 第一盤淘汰 vs 成交回報的競態。

事故: 預掛單 09:00:07.085 在券商端成交;第一筆五檔 (委賣 26.0 × 7) 09:00:07.455 先到 → _fail_first 讀本機
filled_lots = 0 → 判「沒部位」只撤單 + 退訂行情;成交回報 09:00:07.552 才到、撤單被券商拒「成交單已不允許取消」
→ 張數補進帳上,但沒有任何人觸發出場 (晚成交保護要 exited 才動) → 抱到 13:24 才進隔日賣清單。

修法: (1) 淘汰一進來就標 exit_on_fill;(2) 之後任何買進成交落地 (成交回報 / 撤單被拒「已成交」依快照補成交 /
重連補收) 都觸發整檔出場;(3) 有買進委託在途就不退訂行情。
"""
import time as _real_time
from types import SimpleNamespace

import pytest

import trading_session as ts_mod
from fakes_cancel import make_session, run_once, fill, wait_until, MSG_FILLED, MSG_PARTIAL_FILLED
from trader import Trader, Holding

SYM = "8089"
LU = 26.15
DOWN = 21.45
ASK_BOOK_BIDS = [{"price": 25.95, "size": 30}]
ASK_BOOK_ASKS = [{"price": 26.0, "size": 7}]


def _cfg():
    return SimpleNamespace(bid_decline_sample_sec=60, bid_decline_minutes=5)


def _sells(b):
    return [c for c in b.placed if c[0] in ("limit_sell", "market_sell")]


def _session(lots_target=3):
    """預掛 8089 (fixed_lots) — 回 (session, 預掛書號)。"""
    s = make_session(total=10_000_000, per_symbol=0, sizing_mode="fixed_lots", fixed_lots=lots_target)
    s.set_limit_downs({SYM: DOWN})
    s.place_pre_orders([SYM], {SYM: LU})
    return s, s.trades[SYM].order_no


def _trader(s, unsubbed):
    return Trader(watchlist=[SYM], limit_ups={SYM: LU}, cfg=_cfg(), session=s,
                  unsub_fn=lambda sym: unsubbed.append(sym))


class TestSessionExitOnFill:
    def test_fill_report_after_elimination_triggers_full_exit(self, monkeypatch):
        """淘汰時回報沒到 (filled 0) → 標旗標;成交回報落地 → 整檔出場 (跌停價限價賣全部)。"""
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p = _session()
        st = s.trades[SYM]
        assert s.mark_exit_on_fill(SYM, "first_check: ask") is True
        assert st.exit_on_fill == "first_check: ask" and st.stopped_reason == "first_check: ask"
        assert st.filled_lots == 0
        s._on_fill(fill(p, SYM, 3, 26.0))
        assert wait_until(lambda: _sells(s.broker), 3.0), "淘汰後成交落地沒有觸發出場"
        assert _sells(s.broker) == [("limit_sell", SYM, DOWN, 3)]
        assert st.exited is True and st.filled_lots == 3

    def test_cancel_rejected_as_filled_settles_from_snapshot_and_exits(self, monkeypatch):
        """事故原路徑: 淘汰 → 撤單 → 券商拒「成交單已不允許取消」+ 快照 status 50 → 依券商權威補 3 張 → 出場。"""
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p = _session()
        b = s.broker
        st = s.trades[SYM]
        b.cancel_fail_msg[p] = MSG_FILLED
        b.snapshot_filled[p] = 3000
        b.snapshot_status[p] = "50"
        s.mark_exit_on_fill(SYM, "first_check: ask")
        s.cancel_symbol_orders(SYM, "first_check: ask")        # 同步版 (事故中是 async thread)
        assert st.filled_lots == 3, "撤單被拒「已成交」應依快照補成交"
        assert s.order_log[p]["status"] == "filled"
        assert wait_until(lambda: _sells(s.broker), 3.0), "補成交後沒有觸發出場"
        assert _sells(s.broker) == [("limit_sell", SYM, DOWN, 3)]
        assert st.exited is True

    def test_fill_report_then_auth_fill_sells_once(self, monkeypatch):
        """回報先到 (觸發出場),之後快照又補同一筆 (delta 0) → 不再觸發第二次。"""
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p = _session()
        st = s.trades[SYM]
        s.mark_exit_on_fill(SYM, "first_check: ask")
        s._on_fill(fill(p, SYM, 3, 26.0))
        assert wait_until(lambda: st.exited and not st.exit_in_progress, 3.0)
        n = len(_sells(s.broker))
        assert n == 1
        assert s._apply_auth_fill(p, 3, "快照") == 0            # 已記滿 → delta 0
        _real_time.sleep(0.2)
        assert len(_sells(s.broker)) == n

    def test_without_flag_fill_does_not_exit(self, monkeypatch):
        """沒淘汰的檔成交 → 照舊不出場 (回歸保護: 旗標才是觸發條件)。"""
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p = _session()
        s._on_fill(fill(p, SYM, 3, 26.0))
        _real_time.sleep(0.3)
        assert _sells(s.broker) == [] and s.trades[SYM].exited is False

    def test_manual_abandon_wins(self, monkeypatch):
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p = _session()
        s.mark_exit_on_fill(SYM, "first_check: ask")
        s.abandon_symbol(SYM)
        s._on_fill(fill(p, SYM, 3, 26.0))
        _real_time.sleep(0.3)
        assert _sells(s.broker) == []

    def test_partial_fill_snapshot_then_same_report_not_double_counted(self, monkeypatch):
        """審查 P1 (2026-10-20): 委託 3 張、實際成交 1 張其餘撤掉 → 撤單被拒「部分成交單」+ 快照 status 40 補 1 張
        → 出場;同一筆成交回報晚到 (出場窗口內) → 不能再加一次 → 只賣 1 張。"""
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.4)
        s, p = _session()
        b = s.broker
        st = s.trades[SYM]
        b.cancel_fail_msg[p] = MSG_PARTIAL_FILLED
        b.snapshot_filled[p] = 1000
        b.snapshot_status[p] = "40"
        s.mark_exit_on_fill(SYM, "first_check: ask")
        s.cancel_symbol_orders(SYM, "first_check: ask")
        assert st.filled_lots == 1 and s.order_log[p]["filled_lots"] == 1
        assert s.order_log[p]["status"] == "cancelled"            # 剩餘已由券商取消
        _real_time.sleep(0.1)
        s._on_fill(fill(p, SYM, 1, 26.0))                          # 同一批成交的回報晚到 (窗口內)
        assert st.filled_lots == 1, "快照補入後同筆回報不可再加"
        assert wait_until(lambda: st.exited and not st.exit_in_progress, 3.0)
        assert _sells(b) == [("limit_sell", SYM, DOWN, 1)]
        # 券商終結快照已說終量 1 張 (auth_final) → 之後任何回報 (即使 filled_no 不同) 都不加、不再賣
        assert s.order_log[p]["auth_final"] == 1
        s._on_fill(fill(p, SYM, 1, 26.0, filled_no="DUP"))
        _real_time.sleep(0.2)
        assert st.filled_lots == 1 and len(_sells(b)) == 1

    def test_worker_snapshot_status_40_sets_final_quantity_and_exits(self, monkeypatch):
        """實盤最常走的路徑: 撤單查無 → 入佇列 → worker 下一輪快照 status 40 (部分成交 1 張、剩餘已取消)
        → 補 1 張 + 記終量 + 結案 + 出場;之後同筆回報晚到不再加 → 只賣 1 張。"""
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p = _session()
        b = s.broker
        st = s.trades[SYM]
        s.mark_exit_on_fill(SYM, "first_check: ask")
        b.snapshot_missing.add(p)
        s.cancel_symbol_orders(SYM, "first_check: ask")            # 查無 → 入佇列
        assert p in s._cancel_queue and st.filled_lots == 0
        b.snapshot_missing.discard(p)
        b.snapshot_status[p] = "40"
        b.snapshot_filled[p] = 1000
        out = run_once(s, now=ts_mod.time.time() + 10)
        assert p in out["cancelled"] and s.order_log[p]["status"] == "cancelled"
        assert st.filled_lots == 1 and s.order_log[p]["auth_final"] == 1
        assert wait_until(lambda: st.exited and not st.exit_in_progress, 3.0)
        assert _sells(b) == [("limit_sell", SYM, DOWN, 1)]
        s._on_fill(fill(p, SYM, 1, 26.0))                          # 同筆回報晚到 → 終量封頂,不加
        _real_time.sleep(0.2)
        assert st.filled_lots == 1 and len(_sells(b)) == 1

    def test_report_then_partial_snapshot_not_double_counted(self):
        """反向時序: 回報 1 張先到,快照 (活單,1 張) 後到 → delta 0;之後另外的新成交回報照加 (重連補收語意不變)。"""
        s, p = _session()
        st = s.trades[SYM]
        s._on_fill(fill(p, SYM, 1, 26.0))
        assert st.filled_lots == 1
        assert s._apply_auth_fill(p, 1, "快照 status 10") == 0
        assert st.filled_lots == 1
        s._on_fill(fill(p, SYM, 1, 26.0, filled_no="F2"))          # 第二筆真成交 → 2
        assert st.filled_lots == 2
        assert s._apply_auth_fill(p, 1, "落後的快照") == 0           # 落後的快照不會把部位減少
        assert st.filled_lots == 2
        assert s.order_log[p].get("auth_final") is None             # 活單補入不設終量

    def test_reconcile_orders_credits_and_exits(self, monkeypatch):
        """重連補收 (reconcile_orders) 補進淘汰檔的買進 → 同樣觸發整檔出場。"""
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p = _session()
        b = s.broker
        st = s.trades[SYM]
        s.mark_exit_on_fill(SYM, "first_check: ask")
        b.filled_map[p] = 3
        s.reconcile_orders()
        assert st.filled_lots == 3
        assert wait_until(lambda: _sells(b), 3.0), "補收後沒有觸發出場"
        assert _sells(b) == [("limit_sell", SYM, DOWN, 3)]

    def test_late_fill_after_elimination_exit_sells_only_delta(self, monkeypatch):
        """旗標路徑出場完成後,另一張市價追單才成交 1 張 → 走既有 late-fill 只賣這 1 張 (總賣 = 總買)。"""
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p = _session()
        b = s.broker
        st = s.trades[SYM]
        m = b.place_market_buy(SYM, 1)
        s._log_order(m, SYM, "buy", "market_buy", 1, 0)
        s.mark_exit_on_fill(SYM, "first_check: ask")
        s._on_fill(fill(p, SYM, 3, 26.0))
        assert wait_until(lambda: st.exited and not st.exit_in_progress, 3.0)
        assert _sells(b) == [("limit_sell", SYM, DOWN, 3)]
        s._on_fill(fill(m, SYM, 1, 26.0, filled_no="M1"))          # 出場後晚成交 → late-fill 賣 delta
        assert wait_until(lambda: len(_sells(b)) == 2, 3.0)
        assert _sells(b)[-1] == ("limit_sell", SYM, DOWN, 1)
        assert st.filled_lots == 4 and st.exited is True

    def test_retry_elimination_exits_after_cooldown(self, monkeypatch):
        """session 端的定期重試 (盤前 unmark 的檔沒有 trader tick) — 冷卻中不賣,冷卻到期後 retry 賣掉。"""
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p = _session()
        b = s.broker
        st = s.trades[SYM]
        s.mark_exit_on_fill(SYM, "unmarked:node")
        st.exit_cooldown_until = ts_mod.time.monotonic() + 0.4
        s._on_fill(fill(p, SYM, 3, 26.0))
        _real_time.sleep(0.2)
        assert _sells(b) == [] and st.exited is False
        assert s.retry_elimination_exits("test") == [SYM]        # 冷卻中: 有列入但 exit_position 早退
        _real_time.sleep(0.3)
        assert s.retry_elimination_exits("test") == [SYM]
        assert wait_until(lambda: _sells(b), 3.0), "冷卻到期後 retry 沒有賣"
        assert _sells(b) == [("limit_sell", SYM, DOWN, 3)]
        assert wait_until(lambda: st.exited and not st.exit_in_progress, 3.0)
        assert s.retry_elimination_exits("test") == []           # 已出場 → 不再列入

    def test_mark_unknown_symbol_is_noop(self):
        s, _p = _session()
        assert s.mark_exit_on_fill("2330", "first_check: ask") is False
        assert s.mark_exit_on_fill(SYM, "") is False

    def test_has_live_buy_order(self):
        s, p = _session()
        assert s.has_live_buy_order(SYM) is True
        s.order_log[p]["status"] = "cancelled"
        assert s.has_live_buy_order(SYM) is False
        assert s.has_live_buy_order("2330") is False


class TestTraderFirstCheck:
    def test_ask_before_fill_report_keeps_feed_and_exits_on_fill(self, monkeypatch):
        """端到端重演: 五檔委賣先到 → 淘汰 (filled 0) → 不退訂;撤單被拒「已成交」→ 補成交 → 出場。"""
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p = _session()
        b = s.broker
        st = s.trades[SYM]
        b.cancel_fail_msg[p] = MSG_FILLED
        b.snapshot_filled[p] = 3000
        b.snapshot_status[p] = "50"
        unsubbed = []
        t = _trader(s, unsubbed)
        t.on_book(SYM, ASK_BOOK_BIDS, ASK_BOOK_ASKS)             # 第一筆五檔就有委賣 → 淘汰
        h = t.holdings[SYM]
        assert h.status == Holding.DISCARDED_FIRST
        assert st.exit_on_fill.startswith("first_check: first_books_ask_appeared")
        assert unsubbed == [], "買進委託在途時不可退訂行情"
        # cancel_symbol_orders_async 在背景 thread: 撤單被拒「已成交」→ 快照 status 50 補 3 張 → 出場
        assert wait_until(lambda: _sells(b), 3.0), "淘汰後成交落地沒有出場"
        assert _sells(b) == [("limit_sell", SYM, DOWN, 3)]
        assert st.exited is True and st.filled_lots == 3
        assert unsubbed == []

    def test_ask_before_fill_report_then_report_arrives(self, monkeypatch):
        """同上,但成交回報比撤單結果先到 (撤單在佇列查無)。"""
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p = _session()
        b = s.broker
        st = s.trades[SYM]
        b.snapshot_missing.add(p)                                  # 撤單: 快照查無 → 入佇列重試
        unsubbed = []
        t = _trader(s, unsubbed)
        t.on_book(SYM, ASK_BOOK_BIDS, ASK_BOOK_ASKS)
        assert wait_until(lambda: p in s._cancel_queue or s.order_log[p]["status"] != "pending", 3.0)
        assert unsubbed == []
        s._on_fill(fill(p, SYM, 3, 26.0))                          # 回報到
        assert wait_until(lambda: _sells(b), 3.0)
        assert _sells(b) == [("limit_sell", SYM, DOWN, 3)]
        assert st.exited is True

    def test_position_already_known_exits_once(self, monkeypatch):
        """回報先到 (filled 3) 再淘汰 → 舊路徑 (has_position → exit_position);旗標不造成第二次賣。"""
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p = _session()
        b = s.broker
        st = s.trades[SYM]
        s._on_fill(fill(p, SYM, 3, 26.0))
        unsubbed = []
        t = _trader(s, unsubbed)
        t.on_book(SYM, ASK_BOOK_BIDS, ASK_BOOK_ASKS)
        assert wait_until(lambda: st.exited and not st.exit_in_progress, 3.0)
        assert _sells(b) == [("limit_sell", SYM, DOWN, 3)]
        assert unsubbed == []
        # 快照又補同一筆 → delta 0 → 不再賣
        s._apply_auth_fill(p, 3, "快照")
        _real_time.sleep(0.2)
        assert len(_sells(b)) == 1

    def test_no_order_today_still_unsubscribes(self):
        """這檔今天沒下過單 (st None) → 照舊退訂。"""
        s = make_session(total=10_000_000, per_symbol=0, sizing_mode="fixed_lots", fixed_lots=1)
        unsubbed = []
        t = _trader(s, unsubbed)
        t.on_book(SYM, ASK_BOOK_BIDS, ASK_BOOK_ASKS)
        assert t.holdings[SYM].status == Holding.DISCARDED_FIRST
        assert unsubbed == [SYM]

    def test_cancel_confirmed_no_fill_no_sell(self, monkeypatch):
        """淘汰 → 撤單成功、從沒成交 → 不賣任何東西 (旗標無害)。退訂與否取決於撤單 thread 是否已跑完
        (fake broker 瞬間撤成 → 無單無倉 → 退訂也正確;真券商往返 ~60 ms 以上 → 淘汰當下單在途 → 不退訂)。"""
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p = _session()
        b = s.broker
        st = s.trades[SYM]
        unsubbed = []
        t = _trader(s, unsubbed)
        t.on_book(SYM, ASK_BOOK_BIDS, ASK_BOOK_ASKS)
        assert wait_until(lambda: s.order_log[p]["status"] == "cancelled", 3.0)
        _real_time.sleep(0.2)
        assert _sells(b) == [] and st.filled_lots == 0 and st.exited is False
        assert unsubbed in ([], [SYM])
        assert s.has_live_buy_order(SYM) is False

    def test_slow_cancel_keeps_feed(self, monkeypatch):
        """真實節奏: 撤單往返還沒回來 (單仍 pending) 時做退訂判斷 → 不退訂。"""
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p = _session()
        st = s.trades[SYM]
        orig = s.cancel_symbol_orders

        def slow_cancel(symbol, reason):
            _real_time.sleep(0.3)          # 模擬 REST 往返
            return orig(symbol, reason)

        monkeypatch.setattr(s, "cancel_symbol_orders", slow_cancel)
        unsubbed = []
        t = _trader(s, unsubbed)
        t.on_book(SYM, ASK_BOOK_BIDS, ASK_BOOK_ASKS)
        assert unsubbed == [], "撤單在途時不可退訂"
        assert st.stopped_reason.startswith("first_check")     # 停止進場旗標在呼叫當下就設 (不等往返)
        assert wait_until(lambda: s.order_log[p]["status"] == "cancelled", 3.0)
        assert _sells(s.broker) == [] and unsubbed == []

    def test_exit_blocked_by_cooldown_is_retried_on_ticks(self, monkeypatch):
        """審查 P1 (2026-10-20): 第一次出場被冷卻擋下 → 之後沒有新成交也要靠 tick 重試 (行情已保留)。"""
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p = _session()
        b = s.broker
        st = s.trades[SYM]
        b.snapshot_missing.add(p)
        unsubbed = []
        t = _trader(s, unsubbed)
        t.on_book(SYM, ASK_BOOK_BIDS, ASK_BOOK_ASKS)              # 淘汰 (撤單查無 → 佇列)
        assert wait_until(lambda: p in s._cancel_queue, 3.0)
        st.exit_cooldown_until = ts_mod.time.monotonic() + 0.4    # 模擬前一輪出場失敗的冷卻
        s._on_fill(fill(p, SYM, 3, 26.0))                          # 成交落地 → 出場被冷卻擋下
        _real_time.sleep(0.2)
        assert _sells(b) == [] and st.exited is False and st.filled_lots == 3
        assert s.exit_pending_after_elimination(SYM).startswith("first_check")
        _real_time.sleep(0.3)                                      # 冷卻到期
        t.holdings[SYM]._elim_retry_ts = 0.0                       # 跳過 5 s 節流
        t.on_book(SYM, ASK_BOOK_BIDS, ASK_BOOK_ASKS)              # 下一個 tick → 重試
        assert wait_until(lambda: _sells(b), 3.0), "冷卻到期後 tick 沒有重試出場"
        assert _sells(b) == [("limit_sell", SYM, DOWN, 3)]
        assert st.exited is True
        t.holdings[SYM]._elim_retry_ts = 0.0
        t.on_book(SYM, ASK_BOOK_BIDS, ASK_BOOK_ASKS)              # 已出場 → 不再重試
        _real_time.sleep(0.2)
        assert len(_sells(b)) == 1

    def test_real_timing_slow_cancel_rejected_filled(self, monkeypatch):
        """釘住事故的真實時序: 淘汰 → 退訂判斷時撤單還在往返 (filled 0、單在途 → 不退訂) → 撤單被拒「已成交」+
        快照 50 補 3 張 → 出場。"""
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p = _session()
        b = s.broker
        st = s.trades[SYM]
        b.cancel_fail_msg[p] = MSG_FILLED
        b.snapshot_filled[p] = 3000
        b.snapshot_status[p] = "50"
        orig = s.cancel_symbol_orders

        def slow_cancel(symbol, reason):
            _real_time.sleep(0.3)
            return orig(symbol, reason)

        monkeypatch.setattr(s, "cancel_symbol_orders", slow_cancel)
        unsubbed = []
        t = _trader(s, unsubbed)
        t.on_book(SYM, ASK_BOOK_BIDS, ASK_BOOK_ASKS)
        assert st.filled_lots == 0 and unsubbed == [] and st.exit_on_fill.startswith("first_check")
        assert wait_until(lambda: _sells(b), 3.0)
        assert _sells(b) == [("limit_sell", SYM, DOWN, 3)]
        assert st.filled_lots == 3 and st.exited is True and unsubbed == []

    def test_discarded_support_gone_only_exits_with_position(self, monkeypatch):
        """淘汰檔: 撤單在途 (pending) 時支撐消失規則不起出場 worker;有部位才出場。"""
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s, p = _session()
        b = s.broker
        st = s.trades[SYM]
        b.snapshot_missing.add(p)                                  # 撤單查無 → 單一直 pending (在佇列)
        unsubbed = []
        t = _trader(s, unsubbed)
        mkt = [{"price": 0.0, "size": 50}, {"price": LU, "size": 10}]
        t.on_book(SYM, mkt, ASK_BOOK_ASKS)                         # 首盤: 市價隊伍在 + 委賣 → 淘汰 (support_seen)
        assert t.holdings[SYM].status == Holding.DISCARDED_FIRST
        assert wait_until(lambda: p in s._cancel_queue, 3.0)
        t.on_book(SYM, [{"price": LU, "size": 10}], ASK_BOOK_ASKS)   # 市價隊伍沒有了 → 沒部位 → 不出場
        _real_time.sleep(0.2)
        assert st.exited is False and st.exit_in_progress is False and _sells(b) == []
        s._on_fill(fill(p, SYM, 3, 26.0))                          # 部位落地 → 旗標路徑出場
        assert wait_until(lambda: _sells(b), 3.0)
        assert _sells(b) == [("limit_sell", SYM, DOWN, 3)]

    def test_sim_mode_without_session_unsubscribes(self):
        unsubbed = []
        t = Trader(watchlist=[SYM], limit_ups={SYM: LU}, cfg=_cfg(), session=None,
                   unsub_fn=lambda sym: unsubbed.append(sym))
        t.on_book(SYM, ASK_BOOK_BIDS, ASK_BOOK_ASKS)
        assert unsubbed == [SYM]
