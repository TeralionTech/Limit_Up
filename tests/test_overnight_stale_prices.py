"""2026-09-10 node4 事故: 隔日賣「鎖漲停續抱」用到**昨日**漲停價 (283 vs 今日 311) → 打開沒賣。

根因: Runner 是常駐 singleton,node 路徑不像 standalone/hub 每天重建 limit_ups/limit_downs,
昨日快照種進來的殘值活到今天,_prepare_overnight 拿到快取就跳過補查。
三道防線: (1) 每輪 _reset_daily_state 清空 (2) _prepare_overnight 一律重查 + 08:30/08:59:50 refresh
(require_today) (3) session 端盤面不變量 — 委買/委賣高於「漲停價」= 殘值 → 只看市價列;
跌停價與委買一不相容 → 退回委買一價公式。"""
import time
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime, timedelta

from runner import Runner
from trader import _overnight_book_fields
from test_exit_logic import _overnight_session, _sells, _wait

TODAY = datetime.now().strftime("%Y-%m-%d")
YESTERDAY = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def _bare_runner():
    r = Runner()
    r.sdk = SimpleNamespace(marketdata=SimpleNamespace(rest_client=SimpleNamespace(stock=object())))
    r._load_overnight_file = lambda output_dir=None: None
    return r


class TestDailyReset:
    def test_reset_clears_node_residue(self):
        # 昨日 node 快照殘值 → 新的一輪全部清空 (含 hub 凍結旗標與 20% 兜底表)
        r = _bare_runner()
        r.limit_ups = {"5386": 283.0}
        r.limit_downs = {"5386": 231.5}
        r.dispositions = {"5386": False}
        r.day_tradable = {"5386": True}
        r.universe = ["5386"]
        r._node_bid_vol_fallback = {"5386": 100}
        r._marked_frozen = True
        r._limit_up_progress = {"done": 5, "total": 5, "ok": 5, "fail": 0}

        r._reset_daily_state()

        assert r.limit_ups == {} and r.limit_downs == {} and r.dispositions == {}
        assert r.day_tradable == {} and r.universe == [] and r._node_bid_vol_fallback == {}
        assert r._marked_frozen is False
        assert r._limit_up_progress == {"done": 0, "total": 0, "ok": 0, "fail": 0}


class TestPrepareOvernightAlwaysRequeries:
    def test_cached_stale_value_is_not_trusted(self):
        # 即使 self.limit_ups / limit_downs 已有該檔 (昨日殘值),仍重查並以重查結果為準
        r = _bare_runner()
        r.limit_ups = {"5386": 283.0}
        r.limit_downs = {"5386": 231.5}
        r.session.overnight_symbols = lambda: ["5386"]
        calls = []

        def _fake_query(stock, sym, require_today=False):
            calls.append((sym, require_today))
            r.limit_downs[sym] = 255.0
            r.dispositions[sym] = False
            r.day_tradable[sym] = True
            return 311.0
        r._query_limit_up = _fake_query

        syms = r._prepare_overnight(Path("."))

        assert syms == ["5386"]
        assert calls and calls[0][0] == "5386"
        assert r.session.overnight_limit_ups["5386"] == 311.0
        assert r.session.limit_downs["5386"] == 255.0

    def test_refresh_requires_today_and_merges(self, monkeypatch):
        # refresh: require_today=True 傳到 _query_limit_up;查失敗的檔保留先前值 (合併不洗掉)
        import runner as _runner_mod
        monkeypatch.setattr(_runner_mod.time, "sleep", lambda *_: None)
        r = _bare_runner()
        r.session.overnight_symbols = lambda: ["5386", "9999"]
        r.session.set_overnight_limit_ups({"5386": 283.0, "9999": 50.0})
        seen = []

        def _fake_query(stock, sym, require_today=False):
            seen.append((sym, require_today))
            if sym == "5386":
                r.limit_downs[sym] = 255.0
                return 311.0
            return None                              # 9999 重查失敗
        r._query_limit_up = _fake_query

        ups = r._refresh_overnight_prices(attempts=1, label="test")

        assert ups == {"5386": 311.0}
        assert all(flag is True for _, flag in seen)
        assert r.session.overnight_limit_ups == {"5386": 311.0, "9999": 50.0}


class TestQueryLimitUpDateGuard:
    def _stock(self, date, up=311.0, down=255.0):
        resp = {"date": date, "limitUpPrice": up, "limitDownPrice": down,
                "isDisposition": False, "canDayTrade": True}
        return SimpleNamespace(intraday=SimpleNamespace(ticker=lambda symbol: resp))

    def test_stale_date_rejected_only_when_required(self):
        r = _bare_runner()
        stock = self._stock(YESTERDAY)
        # require_today → 回 None 且不寫任何副作用
        assert r._query_limit_up(stock, "5386", require_today=True) is None
        assert "5386" not in r.limit_downs
        # 預設 (hub 全母體迴圈) 行為不變: 照舊採用
        assert r._query_limit_up(stock, "5386") == 311.0
        assert r.limit_downs["5386"] == 255.0

    def test_today_date_accepted(self):
        r = _bare_runner()
        assert r._query_limit_up(self._stock(TODAY), "5386", require_today=True) == 311.0
        assert r.limit_downs["5386"] == 255.0

    def test_missing_date_field_is_tolerated(self):
        # 回應沒有 date 欄 → 不擋 (向前相容)
        r = _bare_runner()
        resp = {"limitUpPrice": 311.0, "limitDownPrice": 255.0}
        stock = SimpleNamespace(intraday=SimpleNamespace(ticker=lambda symbol: resp))
        assert r._query_limit_up(stock, "5386", require_today=True) == 311.0


class TestSessionInvariantGuards:
    def test_stale_limit_up_detected_from_book_and_sells(self):
        # node4 09-10 重演: 漲停價殘值 283,打開後五檔 買 305.5 / 賣 306.5、無市價列 → 必須賣
        s = _overnight_session(list_lots=1, held_lots=1, bid1=305.5, ask1=306.5, limit_up=283.0)
        s.limit_downs = {"9999": 255.0}
        book = ([{"price": 305.5, "size": 7}, {"price": 305, "size": 8}],
                [{"price": 306.5, "size": 1}, {"price": 307, "size": 1}])
        s.update_overnight_book("9999", *_overnight_book_fields(*book))
        o = s.overnight["9999"]
        assert o["limit_up_suspect"] is True
        assert o["locked_now"] is False
        assert _wait(lambda: _sells(s.broker)), "殘值漲停價下沒有觸發隔日賣"
        assert _sells(s.broker) == [("limit_sell", "9999", 255.0, 1)]
        assert s.overnight_status()[0]["limit_up_suspect"] is True

    def test_stale_limit_up_still_holds_when_market_queue_present(self):
        # 殘值被偵測後只看市價列: 有市價買隊伍 → 仍鎖著 (不賣)
        s = _overnight_session(list_lots=1, held_lots=1, bid1=311.0, ask1=0.0, limit_up=283.0)
        book = ([{"price": 0, "size": 2314}, {"price": 311, "size": 482}], [])
        s.update_overnight_book("9999", *_overnight_book_fields(*book))
        time.sleep(0.3)
        assert s.overnight["9999"]["locked_now"] is True
        assert _sells(s.broker) == []

    def test_correct_limit_up_keeps_locked_semantics(self):
        # 對照組: 漲停價正確 311,委買一 311 (買牆) → 鎖著;不誤判為殘值
        s = _overnight_session(list_lots=1, held_lots=1, bid1=311.0, ask1=0.0, limit_up=311.0)
        s.update_overnight_book("9999", 311.0, 0.0, 0, 311.0)
        time.sleep(0.3)
        assert s.overnight["9999"]["locked_now"] is True
        assert not s.overnight["9999"].get("limit_up_suspect")
        assert _sells(s.broker) == []

    def test_stale_limit_down_falls_back_to_bid_formula(self):
        # 跌停價殘值 231.5 vs 委買一 305 (比例 0.76 < 0.80) → 不掛殘值 (會被交易所退),改委買一價公式
        s = _overnight_session(list_lots=1, held_lots=1, bid1=305.0, ask1=306.0)
        s.limit_downs = {"9999": 231.5}
        s.overnight["9999"]["sell_placed"] = True
        s._overnight_sell_worker("9999")
        sells = _sells(s.broker)
        assert len(sells) == 1 and sells[0][2] != 231.5
        assert sells[0][2] == 305.0                      # 價差 < 5 tick → 委買一價

    def test_plausible_limit_down_is_used(self):
        # 對照組: 跌停 255 vs 委買一 305 (比例 0.836) → 正常用跌停價
        s = _overnight_session(list_lots=1, held_lots=1, bid1=305.0, ask1=306.0)
        s.limit_downs = {"9999": 255.0}
        s.overnight["9999"]["sell_placed"] = True
        s._overnight_sell_worker("9999")
        assert _sells(s.broker) == [("limit_sell", "9999", 255.0, 1)]
