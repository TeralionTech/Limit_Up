"""撤單/出場邏輯正確性驗證 (2026-08-04 建立;2026-08-12 出場規則改版;
2026-08-19 量減半撤單移除,出場只看支撐消失) — 六組路徑:

A. 支撐隊伍消失 → 出場 (trader.on_book 端到端;唯一出場訊號)
   一般股 = 市價買隊伍曾出現後歸零;處置股 (名單) = 漲停價買單消失
B. _exit_worker 內部流程 (撤剩餘→賣持倉、失敗旗標、跌停價限價賣)
C. 賣出成交記帳
D. 撤單路徑 (排隊中支撐消失→撤預掛、13:23 全撤、撤失敗兜底)
E. close_all 緊急全平 (含未 armed 回歸;查無跌停價 → 市價賣兜底)
F. 隔日賣出場 (超賣保護、skip;跌停價限價賣,查無 → 委買一價公式兜底)

thread 類路徑用輪詢 (≤2s) 等背景結果;確定性案例直呼 worker。
"""
import time
from types import SimpleNamespace

from test_session_money import FakeBroker, make_session, _fill
from trader import Trader, Holding


def _cfg():
    return SimpleNamespace(first_trade_min_lots=10,
                           bid_decline_sample_sec=60,
                           bid_decline_minutes=5)


def _wait(cond, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(0.01)
    return cond()


def _sells(broker):
    return [c for c in broker.placed if c[0] in ("market_sell", "limit_sell")]


LIMIT = 121.5
DOWN = 99.5                                          # 跌停價 (出場限價賣用)
MKT_PRESENT = [{"price": 0.0, "size": 800}, {"price": LIMIT, "size": 500}]  # 市價隊伍在
MKT_GONE = [{"price": LIMIT, "size": 500}]           # 市價隊伍沒有了 (限價列還在)
ASK_LIMIT = [{"price": 122.0, "size": 5}]            # 委賣限價列 (舊訊號,已不觸發出場)


def _session_with_position(lots=2, disposition=False, with_down=True):
    """預掛 2330 (每檔預算 300k ÷ 121.5k = target 2 張) 並成交 lots 張。"""
    s = make_session(per_symbol=300_000)
    if disposition:
        s.set_dispositions({"2330": True})
    if with_down:
        s.set_limit_downs({"2330": DOWN})
    s.place_pre_orders(["2330"], {"2330": LIMIT})
    if lots:
        s._on_fill(_fill(s.trades["2330"].order_no, "2330", lots, LIMIT))
    return s


# ═══ A. 支撐隊伍消失 → 出場 (trader.on_book 端到端) ═══════════════

class TestSupportGoneExit:
    def _trader(self, s, disp=False):
        return Trader(watchlist=["2330"], limit_ups={"2330": LIMIT},
                      cfg=_cfg(), session=s,
                      dispositions={"2330": True} if disp else None)

    def test_market_queue_gone_triggers_exit(self):
        # 一般股: 市價買隊伍曾出現 → 歸零 → 跌停價限價賣全部
        s = _session_with_position(lots=2)
        t = self._trader(s)
        t.on_book("2330", MKT_PRESENT, [])            # 支撐出現 (arm)
        t.on_book("2330", MKT_GONE, [])               # 市價隊伍沒有了!
        assert _wait(lambda: _sells(s.broker)), "市價隊伍消失後 2 秒內沒有賣單"
        assert _sells(s.broker) == [("limit_sell", "2330", DOWN, 2)]
        assert s.trades["2330"].exited is True

    def test_ask_appearing_alone_does_not_exit(self):
        # ⚠ 人造盤面 (盤中不可能: 市價買在排隊時賣單會立刻撮合,掛不上委賣側) —
        # 這是**程式碼回歸保護**,不是市場情境: 證明「ask 觸發出場」已移除,
        # 誰把 asks_any → exit 加回去這裡就會紅。真實時序中市價列歸零必先於委賣掛出,
        # 新訊號嚴格早於舊訊號。
        s = _session_with_position(lots=2)
        t = self._trader(s)
        t.on_book("2330", MKT_PRESENT, [])
        t.on_book("2330", MKT_PRESENT, ASK_LIMIT)     # 資料層強塞委賣,市價隊伍仍在
        time.sleep(0.3)
        assert _sells(s.broker) == []

    def test_never_armed_no_exit(self):
        # 「沒有了」= 曾經有 — 從頭就沒有市價隊伍,不觸發 (開盤隊伍未成形防誤觸)
        s = _session_with_position(lots=2)
        t = self._trader(s)
        t.on_book("2330", MKT_GONE, [])               # 首筆就沒有市價列
        t.on_book("2330", MKT_GONE, [])
        time.sleep(0.3)
        assert _sells(s.broker) == []

    def test_disposition_limit_up_bid_gone(self):
        # 處置股 (沒有市價列): 漲停價買單消失 → 出場
        s = _session_with_position(lots=2, disposition=True)
        t = self._trader(s, disp=True)
        t.on_book("2330", [{"price": LIMIT, "size": 300}], [])     # 漲停價買單在 (arm)
        t.on_book("2330", [{"price": 120.5, "size": 300}], [])     # 跌下漲停!
        assert _wait(lambda: _sells(s.broker))
        assert _sells(s.broker) == [("limit_sell", "2330", DOWN, 2)]

    def test_no_exposure_no_exit(self):
        # 沒下過單的檔 → has_exposure False → 不觸發、不下賣單
        s = make_session()
        t = self._trader(s)
        t.on_book("2330", MKT_PRESENT, [])
        t.on_book("2330", MKT_GONE, [])
        time.sleep(0.3)
        assert _sells(s.broker) == []

    def test_no_duplicate_sell_after_exit(self):
        s = _session_with_position(lots=2)
        t = self._trader(s)
        t.on_book("2330", MKT_PRESENT, [])
        t.on_book("2330", MKT_GONE, [])
        assert _wait(lambda: _sells(s.broker))
        t.on_book("2330", MKT_PRESENT, [])            # 隊伍回來又消失
        t.on_book("2330", MKT_GONE, [])
        time.sleep(0.3)
        assert len(_sells(s.broker)) == 1             # exited 去重,不重複賣


# ═══ B. _exit_worker 內部流程 (直呼,確定性) ═══════════════════════

class TestExitWorker:
    def test_cancel_remainder_then_sell_filled(self):
        # 部分成交 1/2: 先撤剩餘 (釋放預算) → 再跌停價限價賣已成交 1 張
        s = make_session(per_symbol=300_000)          # target 2 張,成交 1 → 剩餘 pending
        s.set_limit_downs({"2330": DOWN})
        s.place_pre_orders(["2330"], {"2330": LIMIT})
        s._on_fill(_fill(s.trades["2330"].order_no, "2330", 1, LIMIT))
        s._exit_worker("2330", "mkt_queue_gone")
        kinds = [c[0] for c in s.broker.calls]
        assert kinds.index("cancel") < kinds.index("limit_sell")
        assert ("limit_sell", "2330", DOWN, 1) in s.broker.placed
        assert s.trades["2330"].exited is True
        assert s.budget_used == 121_500               # 只剩已成交 1 張的消耗

    def test_sell_failure_sets_flag_and_allows_retrigger(self):
        s = _session_with_position(lots=2)
        st = s.trades["2330"]
        orig = s.broker.place_limit_sell

        def _fail(symbol, price, lots, reason=""):
            raise RuntimeError("rejected")
        s.broker.place_limit_sell = _fail
        s._exit_worker("2330", "mkt_queue_gone")
        assert st.exited is False                     # 未賣成 → 下個 tick 可再觸發
        assert st.sell_failed is True
        # broker 恢復 → 再觸發 → 賣成、旗標解除
        s.broker.place_limit_sell = orig
        s._exit_worker("2330", "mkt_queue_gone")
        assert st.exited is True
        assert st.sell_failed is False
        assert ("limit_sell", "2330", DOWN, 2) in s.broker.placed

    def test_cancel_failure_does_not_block_sell(self):
        # 撤單 broker 拋例外 → 賣出不能被擋住 (持倉照賣)
        s = make_session()
        s.set_limit_downs({"2330": DOWN})
        s.place_pre_orders(["2330"], {"2330": LIMIT})
        s._on_fill(_fill(s.trades["2330"].order_no, "2330", 1, LIMIT))

        def _cancel_fail(order_no, symbol, reason=""):
            raise RuntimeError("cancel timeout")
        s.broker.cancel = _cancel_fail
        s._exit_worker("2330", "mkt_queue_gone")
        assert ("limit_sell", "2330", DOWN, 1) in s.broker.placed
        assert s.trades["2330"].exited is True

    # ── 市價買單 pending 的出場 (2026-08-19 skip-cancel;2026-08-23 審查修正: 等收斂再賣) ──
    def _session_with_pending_market_buy(self, filled=0):
        """預掛 2 張 → 首筆成交 → 市價追 (真路徑): 預掛被撤、市價買 pending;filled 張已回報。
        target=2 (per_symbol 300k ÷ 121.5k)。filled<2 → order_status 維持 pending。"""
        s = _session_with_position(lots=0)
        s.on_first_trade("2330", passed_first_check=True)
        assert _wait(lambda: s.trades["2330"].order_kind == "market_buy"), "市價追沒出手"
        assert _wait(lambda: s.broker.cancelled), "預掛沒被撤"
        st = s.trades["2330"]
        assert st.order_status == "pending"
        if filled:
            s._on_fill(_fill(st.order_no, "2330", filled, LIMIT, filled_no="F5"))
        s.broker.cancelled.clear()      # 只看出場路徑的撤單
        return s

    def test_mkt_converged_sells_full_no_cancel(self):
        # 快路徑: 市價單已滿量成交 (order_status=done) → 不撤、直接賣全量 (速度不變)
        s = self._session_with_pending_market_buy(filled=2)  # 2/2 → done
        st = s.trades["2330"]
        assert st.order_status == "done"
        s._exit_worker("2330", "mkt_queue_gone")
        assert s.broker.cancelled == []                      # 收斂 → 不撤
        assert ("limit_sell", "2330", DOWN, 2) in s.broker.placed
        assert st.exited is True

    def test_mkt_partial_prefill_late_fill_sells_target(self, monkeypatch):
        # ⭐ 問題 1 (HIGH) 回歸: 已成交 1 張、市價追剩 1 張晚 0.3s 才落地 →
        # 必須等收斂到 2 張再賣 2 (舊 bug 只賣 1、剩 1 被 exited 鎖住漏賣)
        import threading
        import trading_session as ts
        monkeypatch.setattr(ts, "_EXIT_FILL_WAIT_SEC", 1.5)
        s = self._session_with_pending_market_buy(filled=1)  # 1/2 → pending
        no = s.trades["2330"].order_no
        threading.Timer(0.3, lambda: s._on_fill(
            _fill(no, "2330", 1, LIMIT, filled_no="F6"))).start()   # 補到 2/2 → done
        s._exit_worker("2330", "mkt_queue_gone")
        assert ("limit_sell", "2330", DOWN, 2) in s.broker.placed   # 賣 2 張,非 1
        assert s.broker.cancelled == []                             # 收斂了 → 不撤
        assert s.trades["2330"].filled_lots == 2
        assert s.trades["2330"].exited is True

    def test_mkt_never_fills_cancels_and_rolls_back(self, monkeypatch):
        # ⭐ 問題 2 (MED) 回歸: 市價單完全沒成交 → deadline 撤掉那張 live 單 → exited 回退
        import trading_session as ts
        monkeypatch.setattr(ts, "_EXIT_FILL_WAIT_SEC", 0.3)
        s = self._session_with_pending_market_buy(filled=0)
        s._exit_worker("2330", "mkt_queue_gone")
        assert _wait(lambda: s.broker.cancelled), "未成交的市價單沒被撤"
        assert _sells(s.broker) == []                        # 無部位 → 不賣
        assert s.trades["2330"].exited is False              # 回退 (晚到成交仍有出場保護)

    def test_mkt_stall_partial_cancels_remainder_sells_partial(self, monkeypatch):
        # 市價單只部分成交 1/2 後卡住不動 → deadline 撤未成交殘量 → 賣已成交的 1 張
        import trading_session as ts
        monkeypatch.setattr(ts, "_EXIT_FILL_WAIT_SEC", 0.3)
        s = self._session_with_pending_market_buy(filled=1)  # 1/2 → pending,無後續
        s._exit_worker("2330", "mkt_queue_gone")
        assert _wait(lambda: s.broker.cancelled), "殘量沒被撤"
        assert ("limit_sell", "2330", DOWN, 1) in s.broker.placed
        assert s.trades["2330"].exited is True

    def test_pending_pre_limit_still_cancelled_first(self):
        # 限價預掛 pending (處置股 / 首筆成交前隊伍就歸零) → 漲停破後會被砸成交 → 仍要先撤
        s = _session_with_position(lots=1)                    # 預掛 2 張成交 1,剩 1 pending
        assert s.trades["2330"].order_kind == "pre_limit"
        s._exit_worker("2330", "mkt_queue_gone")
        assert len(s.broker.cancelled) == 1
        kinds = [c[0] for c in s.broker.calls]
        assert kinds.index("cancel") < kinds.index("limit_sell")
        assert ("limit_sell", "2330", DOWN, 1) in s.broker.placed

    def test_disposition_sells_at_limit_down_too(self):
        # 處置股: 跌停價**限價**賣完全合法 (處置股只是不能市價) → 與一般股同路徑
        s = _session_with_position(lots=2, disposition=True)
        s._exit_worker("2330", "limit_up_bid_gone")
        assert ("limit_sell", "2330", DOWN, 2) in s.broker.placed
        assert not [c for c in s.broker.placed if c[0] == "market_sell"]

    def test_disposition_without_limit_down_needs_manual(self):
        # 處置股查無跌停價 → 不可市價兜底 → 不下單 + sell_failed 需人工
        s = _session_with_position(lots=2, disposition=True, with_down=False)
        s._exit_worker("2330", "limit_up_bid_gone")
        assert _sells(s.broker) == []
        assert s.trades["2330"].sell_failed is True
        assert s.trades["2330"].exited is False       # 跌停價補上後可再觸發

    def test_normal_without_limit_down_falls_back_to_market(self):
        # 一般股查無跌停價 → 市價賣兜底 (盤中合法),不會賣不掉
        s = _session_with_position(lots=2, with_down=False)
        s._exit_worker("2330", "mkt_queue_gone")
        assert ("market_sell", "2330", None, 2) in s.broker.placed
        assert s.trades["2330"].exited is True


# ═══ C. 賣出成交記帳 ═══════════════════════════════════════════════

class TestSellFillAccounting:
    def test_sell_fill_reduces_position_keeps_budget(self):
        s = _session_with_position(lots=2)
        s._exit_worker("2330", "ask_appeared")
        sell_no = f"O{s.broker._n}"                   # 最後一張單 = 賣單
        s._on_fill(_fill(sell_no, "2330", 2, 121.5, action="sell", filled_no="F9"))
        assert s.trades["2330"].filled_lots == 0      # 部位歸零
        assert s.budget_used == 243_000               # 2 張買進成本;賣出不退 (保守日預算,設計如此)


# ═══ D. 撤單路徑 ═══════════════════════════════════════════════════

class TestCancelPaths:
    def test_queue_halved_no_longer_cancels(self):
        # 2026-08-19 回歸保護: 市價列腰斬 (1561 → 700) 不再撤單也不出場
        s = make_session()
        s.place_pre_orders(["2330"], {"2330": 121.5})
        s.trades["2330"].first_trade_fired = True
        t = Trader(watchlist=["2330"], limit_ups={"2330": 121.5},
                   cfg=_cfg(), session=s)
        t.on_book("2330", [{"price": 0.0, "size": 1561}, {"price": 121.5, "size": 800}], [])
        t.on_trade("2330", {"price": 121.5, "size": 50})
        t.on_book("2330", [{"price": 0.0, "size": 700}, {"price": 121.5, "size": 800}], [])
        time.sleep(0.3)
        assert t.holdings["2330"].status == Holding.TRACKING
        assert s.broker.cancelled == [] and _sells(s.broker) == []
        assert s.trades["2330"].order_status == "pending"

    def test_queued_preorder_support_gone_cancels_pending(self, monkeypatch):
        # WAITING (全鎖死無成交,預掛排隊中) + 市價隊伍歸零 → 走 exit_position:
        # 撤 pending、設 stopped_reason、沒部位不賣;之後首筆成交不再市價追
        import trading_session as ts
        monkeypatch.setattr(ts, "_EXIT_FILL_WAIT_SEC", 0.2)
        s = make_session()
        s.place_pre_orders(["2330"], {"2330": 121.5})
        t = Trader(watchlist=["2330"], limit_ups={"2330": 121.5},
                   cfg=_cfg(), session=s)
        t.on_book("2330", [{"price": 0.0, "size": 1561}, {"price": 121.5, "size": 800}], [])
        t.on_book("2330", [{"price": 121.5, "size": 800}], [])         # 隊伍歸零 (無成交)
        assert _wait(lambda: s.broker.cancelled), "撤預掛單 2 秒內沒到 broker"
        assert _wait(lambda: s.trades["2330"].order_status == "cancelled")
        assert s.trades["2330"].stopped_reason == "mkt_queue_gone"
        assert _wait(lambda: s.trades["2330"].exited is False, timeout=2)   # 沒部位 → 回退
        assert _sells(s.broker) == []
        assert s.budget_used == 0
        assert t.holdings["2330"].status == Holding.WAITING              # 狀態不動 (未通過第一盤)
        # 之後首筆成交到 → stopped_reason 擋市價追,不會再買
        t.on_trade("2330", {"price": 121.5, "size": 50})
        time.sleep(0.3)
        assert not [c for c in s.broker.placed if c[0] == "market_buy"]

    def test_cancel_all_pending_only_touches_pending(self):
        s = make_session(total=10_000_000)
        s.place_pre_orders(["1111", "2222", "3333"],
                           {"1111": 100.0, "2222": 100.0, "3333": 100.0})
        s._on_fill(_fill(s.trades["2222"].order_no, "2222", 2, 100.0))   # 2222 全成 done
        s._on_order({"order_no": s.trades["3333"].order_no, "symbol": "3333",
                     "error_message": "rejected"})                        # 3333 拒單
        s.cancel_all_pending("cancel_pending_time")
        cancelled_orders = [c[0] for c in s.broker.cancelled]
        assert len(cancelled_orders) == 1             # 只撤 1111 (唯一 pending)
        assert s.trades["2222"].filled_lots == 2      # 持倉不動

    def test_cancel_failure_recovered_by_cancel_all(self):
        # 單檔撤失敗 → st 留 pending → 13:23 cancel_all_pending 兜底補撤
        s = make_session()
        s.place_pre_orders(["2330"], {"2330": 121.5})
        orig = s.broker.cancel

        def _fail(order_no, symbol, reason=""):
            raise RuntimeError("timeout")
        s.broker.cancel = _fail
        s.cancel_symbol_orders("2330", "mkt_queue_gone")
        assert s.trades["2330"].order_status == "pending"    # 沒撤成,狀態保留
        s.broker.cancel = orig
        s.cancel_all_pending("cancel_pending_time")           # 兜底
        assert s.trades["2330"].order_status == "cancelled"
        assert s.budget_used == 0


# ═══ E. close_all 緊急全平 ═════════════════════════════════════════

class TestCloseAll:
    def _setup(self):
        s = make_session(total=10_000_000)
        s.place_pre_orders(["1111", "2222", "3333"],
                           {"1111": 100.0, "2222": 100.0, "3333": 100.0})
        s._on_fill(_fill(s.trades["1111"].order_no, "1111", 2, 100.0))   # 全成持倉
        s._on_fill(_fill(s.trades["2222"].order_no, "2222", 1, 100.0))   # 部分成交
        return s                                                          # 3333 純 pending

    def test_close_all_cancels_and_sells_everything(self):
        s = self._setup()
        sold = s.close_all()
        assert sold == 2                              # 1111 + 2222 (3333 無持倉)
        sells = _sells(s.broker)
        assert ("market_sell", "1111", None, 2) in sells
        assert ("market_sell", "2222", None, 1) in sells
        assert len(s.broker.cancelled) == 2           # 2222 剩餘 + 3333 (1111 已 done 無單)
        assert s.trades["1111"].exited and s.trades["2222"].exited

    def test_close_all_works_when_disarmed(self):
        # 關 kill switch 後緊急全平必須照樣賣得掉 (_can_manage 修正回歸)
        s = self._setup()
        s.set_armed(False)
        sold = s.close_all()
        assert sold == 2


# ═══ F. 隔日賣出場 ═════════════════════════════════════════════════

class TestPullFillRace:
    """支撐消失×剛好成交的競態 (2026-08-12 使用者確認):
    出場走 exit_position (撤+賣);出場 worker 讀到 0 張時等成交回報
    (_EXIT_FILL_WAIT_SEC),搶到的部位照樣跌停價賣掉。"""

    def test_exit_worker_waits_for_late_fill_then_sells(self, monkeypatch):
        import threading
        import trading_session as ts
        monkeypatch.setattr(ts, "_EXIT_FILL_WAIT_SEC", 1.0)
        s = _session_with_position(lots=0)             # 預掛 2 張 pending,未成交
        pre_no = s.trades["2330"].order_no
        # 0.3 秒後成交回報才到 (模擬「撤單瞬間其實已成交」)
        threading.Timer(0.3, lambda: s._on_fill(_fill(pre_no, "2330", 2, LIMIT))).start()
        s._exit_worker("2330", "mkt_queue_gone")       # 同步呼叫 — 內含等待窗口
        assert ("limit_sell", "2330", DOWN, 2) in s.broker.placed   # 搶到的 2 張賣掉了
        assert s.trades["2330"].exited is True
        kinds = [c[0] for c in s.broker.calls]
        assert kinds.index("cancel") < kinds.index("limit_sell")    # 先撤後賣

    def test_exit_worker_rolls_back_when_no_fill(self, monkeypatch):
        import trading_session as ts
        monkeypatch.setattr(ts, "_EXIT_FILL_WAIT_SEC", 0.3)
        s = _session_with_position(lots=0)
        pre_no = s.trades["2330"].order_no
        s._exit_worker("2330", "mkt_queue_gone")
        st = s.trades["2330"]
        assert _sells(s.broker) == []                  # 沒部位 → 不賣
        assert st.exited is False                      # 回退 — 晚到的成交仍有出場保護
        # 回報更晚才落地 → 部位出現 → 下一次出場觸發能正常賣掉 (鏈路自癒)
        s._on_fill(_fill(pre_no, "2330", 2, LIMIT))
        s._exit_worker("2330", "mkt_queue_gone")
        assert ("limit_sell", "2330", DOWN, 2) in s.broker.placed

    def test_support_gone_exits_position_end_to_end(self, monkeypatch):
        import threading
        import trading_session as ts
        monkeypatch.setattr(ts, "_EXIT_FILL_WAIT_SEC", 1.5)
        s = _session_with_position(lots=0)
        s.trades["2330"].first_trade_fired = True      # 擋市價追,單測出場路徑
        pre_no = s.trades["2330"].order_no
        t = Trader(watchlist=["2330"], limit_ups={"2330": LIMIT}, cfg=_cfg(), session=s)
        t.on_book("2330", [{"price": 0.0, "size": 1561}, {"price": LIMIT, "size": 500}], [])
        t.on_trade("2330", {"price": LIMIT, "size": 50})
        # 市價隊伍歸零 tick → 出場 → 背景 exit worker (撤 + 等回報 + 賣)
        threading.Timer(0.3, lambda: s._on_fill(_fill(pre_no, "2330", 2, LIMIT))).start()
        t.on_book("2330", [{"price": LIMIT, "size": 500}], [])
        assert t.holdings["2330"].status == Holding.PULLED
        assert t.holdings["2330"].pulled_reason == "mkt_queue_gone"
        assert _wait(lambda: _sells(s.broker), timeout=4), "pull 後搶到的部位沒被賣掉"
        assert ("limit_sell", "2330", DOWN, 2) in s.broker.placed


class TestAbandon:
    """前端「取消追蹤」(2026-08-12): 停止該檔一切自動化,使用者自負 —
    狂送單的單檔煞車 (全額交割股事故的手動出口)。"""

    def test_abandon_kills_chase_spam(self):
        # 場景重現: 市價追持續被拒狂送 → 取消追蹤 → 立刻停
        s = make_session()
        s.place_pre_orders(["9103"], {"9103": 50.0})
        calls = {"n": 0}

        def _always_fail(symbol, lots):
            calls["n"] += 1
            # 注意: 訊息不能含「全額/預收/圈存」— 那會觸發致命拒因偵測 1 筆即停,
            # 這裡要測的是「非致命持續失敗的狂送」被手動取消追蹤煞停
            raise RuntimeError("委託失敗 (連線逾時)")
        s.broker.place_market_buy = _always_fail
        s.on_first_trade("9103", True)                 # 背景 thread 開始狂送
        assert _wait(lambda: calls["n"] >= 3), "狂送沒發生 (測試前置失敗)"
        assert s.abandon_symbol("9103") is True
        assert _wait(lambda: s.budget_used == 0)       # 追單迴圈中止 → 預算全釋放
        time.sleep(0.3)
        n_after = calls["n"]
        time.sleep(0.4)
        assert calls["n"] == n_after                   # 不再送單
        assert s.trades["9103"].stopped_reason == "manual_abandon"
        # 預掛也撤了 (abandon 撤一次;chase worker 中止後照流程再補撤同一單,無害)
        assert len(s.broker.cancelled) >= 1

    def test_abandon_disables_exit_automation(self):
        # 取消追蹤後: 有持倉也不再自動出場 (使用者自負)
        s = _session_with_position(lots=2)
        s.abandon_symbol("2330")
        s.exit_position("2330", "mkt_queue_gone")      # 支撐消失訊號進來
        time.sleep(0.3)
        assert _sells(s.broker) == []                  # 不賣 — 自動化已停
        assert s.trades["2330"].exited is True

    def test_abandon_unknown_symbol(self):
        s = make_session()
        assert s.abandon_symbol("0000") is False

    def test_runner_abandon_marks_holding(self):
        from runner import Runner
        from trader import Holding as H
        r = Runner()
        r.trader = Trader(watchlist=["2330"], limit_ups={"2330": 100.0}, cfg=_cfg())
        assert r.abandon_symbol("2330") is True
        assert r.trader.holdings["2330"].status == H.ABANDONED
        assert r.abandon_symbol("9999") is False       # 兩邊都查無 → False


class TestPreorderSweep:
    """封口 sweep (2026-08-13): 預掛下單進行中被 tick 淘汰 → 淘汰當下撤單 no-op
    (單還沒掛) → 預掛完成後 sweep 補撤,無時間縫隙。"""

    def test_sweep_cancels_raced_unmark(self):
        from runner import Runner
        from state import State
        r = Runner()
        r.session = make_session()
        r.state = State()
        r.state.mark("1101", 100.0, 50, 100.0)
        r.session.place_pre_orders(["1101"], {"1101": 100.0})   # 單掛出去了
        r.state.unmark_ask_appeared("1101", 100.5, 3)           # 下單後才被 tick 淘汰
        r._sweep_unmarked_pre_orders(["1101"])
        assert len(r.session.broker.cancelled) == 1             # sweep 補撤到了
        assert r.session.trades["1101"].order_status == "cancelled"
        assert r.session.budget_used == 0

    def test_sweep_noop_when_still_marked(self):
        from runner import Runner
        from state import State
        r = Runner()
        r.session = make_session()
        r.state = State()
        r.state.mark("1101", 100.0, 50, 100.0)
        r.session.place_pre_orders(["1101"], {"1101": 100.0})
        r._sweep_unmarked_pre_orders(["1101"])                  # 沒被淘汰 → 不動
        assert r.session.broker.cancelled == []
        assert r.session.trades["1101"].order_status == "pending"


class _InvBroker(FakeBroker):
    def __init__(self, inventories=None):
        super().__init__()
        self.inventories = inventories or []

    def get_inventories(self):
        return self.inventories


def _overnight_session(list_lots=5, held_lots=2, bid1=50.0, ask1=0.0,
                       skip=False, reconciled=True, limit_up=None):
    s = make_session()
    s.broker = _InvBroker([{"symbol": "9999", "lots": held_lots}] if held_lots else [])
    s.load_overnight([{"symbol": "9999", "lots": list_lots, "avg_cost": 10.0}])
    o = s.overnight["9999"]
    o["reconciled"] = reconciled
    o["skip"] = skip
    if limit_up:
        s.set_overnight_limit_ups({"9999": limit_up})
    s.update_overnight_book("9999", bid1, ask1)
    return s


class TestOvernightExit:
    def test_oversell_guard_sells_min_of_list_and_inventory(self):
        # 清單 5 張 / 庫存 2 張 → 只賣 2 (2026-08-03 超賣保護)
        # 觸發 = book 驅動 (無漲停價資料+無市價列 → 開盤即賣 fallback)
        s = _overnight_session(list_lots=5, held_lots=2, bid1=50.0)
        s.update_overnight_book("9999", 50.0, 0.0, 0, 50.0)
        assert _wait(lambda: _sells(s.broker)), "隔日賣 2 秒內沒下單"
        assert _sells(s.broker) == [("limit_sell", "9999", 50.0, 2)]

    def test_skip_blocks_sell(self):
        s = _overnight_session(skip=True)
        s.update_overnight_book("9999", 50.0, 0.0, 0, 50.0)
        time.sleep(0.3)
        assert _sells(s.broker) == []

    def test_unreconciled_blocks_sell(self):
        s = _overnight_session(reconciled=False)
        s.update_overnight_book("9999", 50.0, 0.0, 0, 50.0)
        time.sleep(0.3)
        assert _sells(s.broker) == []

    def test_zero_inventory_no_sell_and_retriggerable(self):
        s = _overnight_session(list_lots=5, held_lots=0)
        o = s.overnight["9999"]
        o["sell_placed"] = True                       # 模擬觸發端已佔位
        s._overnight_sell_worker("9999")              # 直呼 (確定性)
        assert _sells(s.broker) == []
        assert o["sell_placed"] is False              # 重置 → 下一筆成交可再試

    def test_wide_spread_sells_one_tick_below_ask(self):
        # (兜底路徑) 查無跌停價 → 退回舊委買一價公式: 價差 ≥5 tick → 賣一往下一檔
        s = _overnight_session(list_lots=5, held_lots=5, bid1=50.0, ask1=50.9)
        o = s.overnight["9999"]
        o["sell_placed"] = True
        s._overnight_sell_worker("9999")
        assert _sells(s.broker) == [("limit_sell", "9999", 50.8, 5)]

    def test_overnight_sells_at_limit_down_when_available(self):
        # 2026-08-12 定案: 隔日賣也改**跌停價限價賣** (不再管委買/委賣價差)
        s = _overnight_session(list_lots=5, held_lots=5, bid1=50.0, ask1=50.9)
        s.set_limit_downs({"9999": 41.5})
        o = s.overnight["9999"]
        o["sell_placed"] = True
        s._overnight_sell_worker("9999")
        assert _sells(s.broker) == [("limit_sell", "9999", 41.5, 5)]   # 跌停價,非 50.8


# ─── 隔日賣純盤面規則 (2026-08-16 定案): 委買一跌下漲停才賣,鎖著就抱 ──────

UP_ON = 52.0       # 隔日賣標的今日漲停價
DOWN_ON = 41.5     # 今日跌停價 (出場限價賣)
# 鎖著 = 市價買隊伍在 (price=0 列) 或 限價委買一 >= 漲停價
BOOK_MKT_AND_WALL = ([{"price": 0.0, "size": 800}, {"price": UP_ON, "size": 500}], [])
BOOK_WALL_ONLY = ([{"price": UP_ON, "size": 500}], [])       # 限價買牆單獨鎖著
BOOK_MKT_ONLY = ([{"price": 0.0, "size": 800}], [])          # 純市價列 (無限價檔)
# price=0 防呆核心盤面: 市價隊伍在,但限價一檔已跌下漲停 → 仍算鎖著
BOOK_MKT_WALL_BELOW = ([{"price": 0.0, "size": 800}, {"price": 51.5, "size": 300}], [])
BOOK_NO_SUPPORT = ([{"price": 51.5, "size": 300}], [])       # 委買一跌下漲停


def _hold_session(**kw):
    kw.setdefault("held_lots", 5)
    kw.setdefault("list_lots", 5)
    kw.setdefault("limit_up", UP_ON)
    s = _overnight_session(**kw)
    s.set_limit_downs({"9999": DOWN_ON})
    return s


def _feed_book(s, book):
    """模擬呼叫端 (trader/monitor) 的 book tick — 用同一個 helper 算欄位。"""
    from trader import _overnight_book_fields
    s.update_overnight_book("9999", *_overnight_book_fields(*book))


class TestOvernightHold:
    def test_locked_open_first_book_no_sell(self):
        # 開盤鎖著 (市價隊伍+買牆) → 抱著不賣
        s = _hold_session()
        _feed_book(s, BOOK_MKT_AND_WALL)
        time.sleep(0.3)
        assert _sells(s.broker) == []
        assert s.overnight["9999"]["locked_now"] is True
        assert s.overnight["9999"]["sell_placed"] is False

    def test_bid_below_limit_up_first_book_sells(self):
        # 開盤沒鎖 (委買一低於漲停) → 第一筆 book 即賣 (等效舊開盤即賣)
        s = _hold_session()
        _feed_book(s, BOOK_NO_SUPPORT)
        assert _wait(lambda: _sells(s.broker))
        assert _sells(s.broker) == [("limit_sell", "9999", DOWN_ON, 5)]
        assert s.overnight["9999"]["locked_now"] is False

    def test_wall_only_locked_no_sell(self):
        # 限價大買單鎖漲停 (市價列恆 0) — 買牆本身就是鎖著,不誤賣
        s = _hold_session()
        for _ in range(3):
            _feed_book(s, BOOK_WALL_ONLY)
        time.sleep(0.3)
        assert _sells(s.broker) == []
        assert s.overnight["9999"]["locked_now"] is True

    def test_mkt_row_present_wall_below_limit_no_sell(self):
        # price=0 防呆核心: 市價隊伍在就是鎖著,限價列跌下漲停也不賣
        s = _hold_session()
        _feed_book(s, BOOK_MKT_WALL_BELOW)
        time.sleep(0.3)
        assert _sells(s.broker) == []
        assert s.overnight["9999"]["locked_now"] is True

    def test_mkt_only_book_no_sell(self):
        # 純市價列 book (無任何限價檔) → 鎖著 (不觸發空 book guard)
        s = _hold_session()
        _feed_book(s, BOOK_MKT_ONLY)
        time.sleep(0.3)
        assert _sells(s.broker) == []
        assert s.overnight["9999"]["locked_now"] is True

    def test_lock_opens_then_bid_drops_sells(self):
        # 鎖著一段時間 → 委買一跌下漲停 → 跌停價限價賣
        s = _hold_session()
        _feed_book(s, BOOK_MKT_AND_WALL)
        _feed_book(s, BOOK_WALL_ONLY)
        time.sleep(0.2)
        assert _sells(s.broker) == []
        _feed_book(s, BOOK_NO_SUPPORT)
        assert _wait(lambda: _sells(s.broker))
        assert _sells(s.broker) == [("limit_sell", "9999", DOWN_ON, 5)]

    def test_missing_limit_up_with_mkt_row_no_sell(self):
        # 查無漲停價但市價隊伍在 → 仍判鎖著 (比舊 fallback 聰明)
        s = _hold_session(limit_up=None)
        _feed_book(s, BOOK_MKT_AND_WALL)
        time.sleep(0.3)
        assert _sells(s.broker) == []

    def test_missing_limit_up_without_mkt_row_sells(self):
        # 查無漲停價且無市價列 (買牆無從判斷) → 開盤即賣 (舊 fallback)
        s = _hold_session(limit_up=None)
        _feed_book(s, BOOK_WALL_ONLY)
        assert _wait(lambda: _sells(s.broker))

    def test_skip_blocks_then_resume_sells(self):
        # skip 中訊號每 tick 持續但被閘門擋住 (不 spam);恢復後下一 tick 賣
        s = _hold_session(skip=True)
        for _ in range(3):
            _feed_book(s, BOOK_NO_SUPPORT)
        time.sleep(0.3)
        assert _sells(s.broker) == []
        s.set_overnight_skip("9999", False)
        _feed_book(s, BOOK_NO_SUPPORT)
        assert _wait(lambda: _sells(s.broker))

    def test_disposition_same_rule(self):
        # 處置股 (無市價列) 同一條規則 — 不需分流 (防未來加分流的回歸)
        s = _hold_session()
        s.set_dispositions({"9999": True})
        _feed_book(s, BOOK_WALL_ONLY)
        time.sleep(0.2)
        assert _sells(s.broker) == []
        _feed_book(s, BOOK_NO_SUPPORT)
        assert _wait(lambda: _sells(s.broker))
        assert _sells(s.broker) == [("limit_sell", "9999", DOWN_ON, 5)]

    def test_all_day_lock_carries_to_next_day(self):
        # 全天鎖著沒賣 → 13:24 get_overnight_candidates 以原張數帶到明天
        s = _hold_session()
        for _ in range(5):
            _feed_book(s, BOOK_MKT_AND_WALL)
        time.sleep(0.2)
        assert _sells(s.broker) == []
        cands = {c["symbol"]: c for c in s.get_overnight_candidates()}
        assert cands["9999"]["lots"] == 5

    def test_sold_stays_in_list_and_drops_from_candidates(self):
        # 賣掉 → sold_lots 記帳、**清單不移除** (顯示留到隔早對帳)、candidates 不帶
        s = _hold_session()
        _feed_book(s, BOOK_NO_SUPPORT)
        assert _wait(lambda: _sells(s.broker))
        sell_no = s.overnight["9999"]["sell_order_no"]
        s._on_fill(_fill(sell_no, "9999", 5, DOWN_ON, action="sell", filled_no="F77"))
        assert s.overnight["9999"]["sold_lots"] == 5
        assert "9999" in s.overnight                     # 賣掉不移除清單
        assert all(c["symbol"] != "9999" for c in s.get_overnight_candidates())

    def test_active_today_gate(self):
        # 同檔今日有活單 → 隔日賣不觸發;今日出場 (exited) 後下一 tick 接手
        s = _hold_session()
        s.place_pre_orders(["9999"], {"9999": UP_ON})    # 今日預掛 → st 活著
        _feed_book(s, BOOK_NO_SUPPORT)
        time.sleep(0.3)
        assert _sells(s.broker) == []                    # 閘門壓住
        s.trades["9999"].exited = True                   # 今日已出場
        _feed_book(s, BOOK_NO_SUPPORT)
        assert _wait(lambda: _sells(s.broker))
        assert _sells(s.broker) == [("limit_sell", "9999", DOWN_ON, 5)]

    def test_empty_book_tick_no_action(self):
        # 空 book tick → 不判不賣,locked_now 保持上一值
        s = _hold_session()
        _feed_book(s, BOOK_MKT_AND_WALL)
        assert s.overnight["9999"]["locked_now"] is True
        s.update_overnight_book("9999", 0.0, 0.0, 0, 0.0)
        time.sleep(0.2)
        assert _sells(s.broker) == []
        assert s.overnight["9999"]["locked_now"] is True

    def test_update_overnight_book_three_arg_compat(self):
        # 3 參數舊簽名 = 純報價更新,不跑規則 (bid1 低於漲停也不誤觸)
        s = _hold_session()
        s.update_overnight_book("9999", 49.5, 50.5)
        time.sleep(0.2)
        assert _sells(s.broker) == []
        o = s.overnight["9999"]
        assert o["bid1"] == 49.5 and o["ask1"] == 50.5

    def test_handler_plumbing_via_trader(self):
        # handler 層驗證: 真 Trader.on_book 串 session;on_trade 不再觸發隔日賣
        s = _hold_session()
        t = Trader(watchlist=[], limit_ups={}, cfg=_cfg(), session=s)
        t.on_trade("9999", {"price": 51.5, "size": 120000})    # 成交 tick → 無任何效果
        time.sleep(0.2)
        assert _sells(s.broker) == []
        t.on_book("9999", *BOOK_MKT_AND_WALL)                  # 鎖著 → 抱
        time.sleep(0.1)
        assert _sells(s.broker) == []
        assert s.overnight["9999"]["locked_now"] is True
        t.on_book("9999", *BOOK_NO_SUPPORT)                    # 跌下漲停 → 賣
        assert _wait(lambda: _sells(s.broker))
        assert _sells(s.broker) == [("limit_sell", "9999", DOWN_ON, 5)]
