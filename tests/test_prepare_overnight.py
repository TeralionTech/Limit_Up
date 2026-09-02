"""_prepare_overnight — 隔日賣清單 + 補查今日漲停/跌停價 → 交給 session。
standalone 與 node 共用同 helper (2026-09-02: node 補上隔日賣)。用裸 Runner + 假 sdk/query,不 login。"""
from pathlib import Path
from types import SimpleNamespace

from runner import Runner


def _bare_runner():
    r = Runner()                       # 直接建構 (不走 get() singleton;不 login)
    r.limit_ups = {}
    r.limit_downs = {}
    r.dispositions = {}
    r.day_tradable = {}
    # 假 sdk (只需 marketdata.rest_client.stock 存取得到)
    r.sdk = SimpleNamespace(marketdata=SimpleNamespace(
        rest_client=SimpleNamespace(stock=object())))
    return r


class TestPrepareOvernight:
    def test_backfills_prices_and_sets_session(self):
        r = _bare_runner()
        # 留倉清單: session 有一檔隔日賣標的
        r._load_overnight_file = lambda output_dir=None: None
        r.session.overnight_symbols = lambda: ["2881"]
        # 補查: 回今日漲停價 + 副作用寫跌停/處置/現沖 (模擬 _query_limit_up)
        def _fake_query(stock, sym):
            r.limit_downs[sym] = 60.0
            r.dispositions[sym] = False
            r.day_tradable[sym] = True
            return 66.0
        r._query_limit_up = _fake_query

        syms = r._prepare_overnight(Path("."))

        assert syms == ["2881"]
        assert r.session.limit_downs.get("2881") == 60.0                 # 出場跌停價到位
        assert r.session.overnight_limit_ups.get("2881") == 66.0         # 續抱判斷漲停價到位

    def test_no_overnight_is_safe(self):
        # 無留倉 → 回空、不呼叫補查、session 隔日賣清單空
        r = _bare_runner()
        r._load_overnight_file = lambda output_dir=None: None
        r.session.overnight_symbols = lambda: []
        called = []
        r._query_limit_up = lambda stock, sym: called.append(sym)

        syms = r._prepare_overnight(Path("."))

        assert syms == [] and called == []
        assert r.session.overnight_limit_ups == {}

    def test_missing_limit_up_still_returns_symbol(self, monkeypatch):
        # 補查不到漲停價 → 該檔仍在 overnight_syms (退回開盤即賣),只是無續抱漲停價
        import runner as _runner_mod
        monkeypatch.setattr(_runner_mod.time, "sleep", lambda *_: None)  # 免 3× retry 真睡 2s
        r = _bare_runner()
        r._load_overnight_file = lambda output_dir=None: None
        r.session.overnight_symbols = lambda: ["9999"]
        r._query_limit_up = lambda stock, sym: None      # 查無

        syms = r._prepare_overnight(Path("."))

        assert syms == ["9999"]
        assert "9999" not in r.session.overnight_limit_ups   # 無漲停價 → 不進續抱名單
