"""鎖住 scripts/replay_day.py 的 replay 引擎邏輯 — 用合成迷你 JSONL (不含真實大檔)。

情境: A 開盤即鎖→保留+出場、B 鎖了又出賣單→discard、C 量減半→final_check 刷掉、
       試撮 isTrial trade 被忽略。驗:篩選 / 預掛 / 9:00 盲送 / 支撐消失出場 全鏈。
"""
import json
from datetime import datetime
from types import SimpleNamespace

import pytest

import filter as filter_mod
import trader as trader_mod
import trading_session as ts_mod
from scripts.replay_day import run


def _tick(channel, ts, **data):
    return json.dumps({"channel": channel, "ts": ts, "data": data}, ensure_ascii=False)


def _write_ticks(path):
    L = []
    # 08:30 A 開盤即鎖 (bid=漲停100, 無賣單)
    L.append(_tick("books", "2026-08-21T08:30:00.000000", symbol="AAAA",
                   bids=[{"price": 100.0, "size": 500}], asks=[]))
    # 08:30 B 鎖 (bid=漲停50) 然後出賣單 → discard
    L.append(_tick("books", "2026-08-21T08:30:01.000000", symbol="BBBB",
                   bids=[{"price": 50.0, "size": 500}], asks=[]))
    L.append(_tick("books", "2026-08-21T08:30:02.000000", symbol="BBBB",
                   bids=[{"price": 50.0, "size": 500}], asks=[{"price": 50.5, "size": 3}]))
    # 08:30 C 鎖 (bid=漲停30, 量 800) → 之後量掉到 200 (<½) → final_check 刷掉
    L.append(_tick("books", "2026-08-21T08:30:03.000000", symbol="CCCC",
                   bids=[{"price": 30.0, "size": 800}], asks=[]))
    L.append(_tick("books", "2026-08-21T08:40:00.000000", symbol="CCCC",
                   bids=[{"price": 30.0, "size": 200}], asks=[]))
    # 08:59:58 觸發 do_pre_order (A、C 還鎖著;C 會被量減半刷掉 → 只剩 A 預掛)
    L.append(_tick("books", "2026-08-21T08:59:58.000000", symbol="AAAA",
                   bids=[{"price": 100.0, "size": 500}], asks=[]))
    # 09:00 觸發 do_open (盲送 A + 假設成交建部位);A 有市價買隊伍 (price=0 列) → support 出現
    L.append(_tick("books", "2026-08-21T09:00:00.000000", symbol="AAAA",
                   bids=[{"price": 0.0, "size": 800}, {"price": 100.0, "size": 500}], asks=[]))
    # 09:00 試撮 trade (isTrial) → 應被忽略,不算首筆
    L.append(_tick("trades", "2026-08-21T09:00:00.500000", symbol="AAAA",
                   price=100.0, size=94000, isTrial=True))
    # 09:00 真首筆成交 (94 張 ≥ 10) → TRACKING
    L.append(_tick("trades", "2026-08-21T09:00:01.000000", symbol="AAAA",
                   price=100.0, size=94000))
    # 09:01 市價買隊伍消失 (無 price=0 列) → 支撐消失 → 出場
    L.append(_tick("books", "2026-08-21T09:01:00.000000", symbol="AAAA",
                   bids=[{"price": 100.0, "size": 500}], asks=[]))
    path.write_text("\n".join(L), encoding="utf-8")


def _args(tmp_path):
    ticks = tmp_path / "t.jsonl"
    _write_ticks(ticks)
    lu = tmp_path / "lu.json"
    lu.write_text(json.dumps({"AAAA": 100.0, "BBBB": 50.0, "CCCC": 30.0}), encoding="utf-8")
    ld = tmp_path / "ld.json"
    ld.write_text(json.dumps({"AAAA": 90.0}), encoding="utf-8")
    return SimpleNamespace(
        ticks=str(ticks), limit_ups=str(lu), date="2026-08-21",
        dispositions="", limit_downs=str(ld), day_tradable="", fills="", orders="",
        total_budget=3_700_000, per_symbol=400_000, sizing_mode="budget", fixed_lots=0,
        first_trade_min_lots=10, bid_drop_ratio=0.5, report="")


@pytest.fixture
def restore_clock():
    """replay 會全域 patch 三個模組的 datetime + _EXIT_FILL_WAIT_SEC — 測完還原,免污染別的測試。"""
    orig = (filter_mod.datetime, trader_mod.datetime, ts_mod.datetime, ts_mod._EXIT_FILL_WAIT_SEC)
    yield
    (filter_mod.datetime, trader_mod.datetime, ts_mod.datetime, ts_mod._EXIT_FILL_WAIT_SEC) = orig


class TestReplayEngine:
    def test_full_pipeline(self, tmp_path, restore_clock):
        r = run(_args(tmp_path))

        # ── 篩選 ──
        assert "AAAA" in r.marked                          # 開盤即鎖保留
        assert r.state.is_first_tick("AAAA")               # first_tick (開盤即鎖)
        assert r.state.is_discarded("BBBB")                # 出賣單 → discard
        assert "BBBB" not in r.marked
        assert "CCCC" not in r.marked                      # 量減半 final_check 刷掉

        # ── 進場 ──
        pre = [(s, l) for (k, s, p, l) in r.broker.placed if k == "limit_buy"]
        assert any(s == "AAAA" for s, l in pre)            # A 預掛限價
        assert all(s != "CCCC" and s != "BBBB" for s, l in pre)
        mkt = [s for (k, s, p, l) in r.broker.placed if k == "market_buy"]
        assert "AAAA" in mkt                               # 9:00 盲送 A
        # 委託成功 → 撤預掛 P
        assert any(c[1] == "AAAA" for c in r.broker.cancelled)

        # ── 出場 (假設成交 → 支撐消失偵測) ──
        assert any(sym == "AAAA" for sym, _r, _f in r.exits)     # A 觸發出場
        sells = [(s, p, l) for (k, s, p, l) in r.broker.placed if k == "limit_sell"]
        assert ("AAAA", 90.0, r.session.trades["AAAA"].target_lots) in sells  # 跌停價賣全部

    def test_istrial_not_counted_as_first_trade(self, tmp_path, restore_clock):
        # 試撮 tick 不該被當首筆 (只有真成交才 TRACKING) — 用 trader holding 狀態驗
        r = run(_args(tmp_path))
        h = r.ctx["trader"].holdings.get("AAAA")
        assert h is not None
        # 首筆真成交 (非 isTrial) 有記到
        assert h.first_trade_seen is True
        assert h.first_trade.get("lots") == 94            # 94000 股 → 94 張
