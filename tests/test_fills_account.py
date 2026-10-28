"""成交回報帳號比對 (2026-10-28 spec limitup_fills_account_v1)。

node1 2026-10-20: 同登入看得到的**別的子帳號**成交也從 set_on_filled 進來 (broker 只 warning、照樣轉發),
fills.csv 沒有帳號資訊 → 收集端分不出「本帳號手動單」與「別的子帳號成交」;別的子帳號的書號若剛好撞到
本策略 order_log 的書號,還會被當策略成交動到部位。

契約:
  - broker._handle_filled: fill 帶 account_match (True 同帳號 / False 不同 / None 無法判斷:回報沒帶帳號或
    尚無登入帳號) 與 account_masked (回報帳號前 3 碼 + ***,沒帶 → "");**永不丟棄成交**。
  - session._on_fill: account_match False → 不是策略成交 (書號在 order_log 也一樣:WARNING、不動
    filled_lots / 均價 / 預算 / 出場);None → 與舊版完全相同。非策略分支的 False 只 info「其他帳號成交」,
    不發「策略外賣出成交」告警。
  - session._append_fill_csv: v2 表頭 12 欄;account_match 寫 "1" / "0" / "";已存在的 v1 表頭檔照接 12 格列。

測試帳號全是假字串。富邦 SDK 不登入 (RealOrderClient 直接建、直接呼叫 handler)。
"""
import csv
import logging
import time as _real_time
from datetime import datetime
from types import SimpleNamespace

import pytest

import trading_session as ts_mod
from broker import RealOrderClient
from test_session_money import make_session, _fill

SYM = "1101"
LIMIT = 45.0
DOWN = 36.9
COST_PER_LOT = 45_000
ACCT = "abc1234"            # 本節點登入帳號 (假)
OTHER = "xyz9876"           # 同登入看得到的別的子帳號 (假)
V1 = ["recv_time", "symbol", "action", "price", "lots", "quantity",
      "order_no", "filled_no", "broker_filled_time", "strategy"]
V2 = V1 + ["account_match", "account_masked"]


# ═══ broker._handle_filled ═══════════════════════════════════════

@pytest.fixture
def client(tmp_path):
    c = RealOrderClient(tmp_path / "orders.csv")
    c.account_no = ACCT
    got = []
    c.on_fill = got.append
    yield c, got
    c.close()


def _content(**kw):
    base = dict(stock_no=SYM, buy_sell="Buy", filled_price=45.0, filled_qty=2000,
                order_no="K0001", filled_no="F0001", filled_time="09:00:01.123")
    base.update(kw)
    return SimpleNamespace(**base)


class TestBrokerAccountMatch:
    def test_same_account_true_and_masked(self, client):
        c, got = client
        c._handle_filled(None, _content(account=ACCT))
        assert len(got) == 1
        f = got[0]
        assert f["account_match"] is True and f["account_masked"] == "abc***"
        # 既有欄位不變
        assert (f["symbol"], f["action"], f["price"], f["quantity"], f["lots"], f["order_no"],
                f["filled_no"], f["filled_time"]) == (SYM, "buy", 45.0, 2000, 2, "K0001",
                                                      "F0001", "09:00:01.123")

    def test_other_account_false_still_forwarded_and_warned(self, client, caplog):
        caplog.set_level(logging.INFO, logger="broker")
        c, got = client
        c._handle_filled(None, _content(account=OTHER))
        assert len(got) == 1, "別的子帳號成交仍要轉發 (永不丟棄成交回報)"
        assert got[0]["account_match"] is False and got[0]["account_masked"] == "xyz***"
        assert "仍轉發" in caplog.text

    @pytest.mark.parametrize("kw", [{}, {"account": None}, {"account": ""}])
    def test_no_account_on_callback_none(self, client, kw):
        c, got = client
        c._handle_filled(None, _content(**kw))
        assert len(got) == 1
        assert got[0]["account_match"] is None and got[0]["account_masked"] == ""

    def test_no_login_account_none_but_masked(self, client):
        # 還沒有登入帳號可比 → 無法判斷 (None);回報有帶帳號 → 照樣遮罩記下
        c, got = client
        c.account_no = ""
        c._handle_filled(None, _content(account=OTHER))
        assert got[0]["account_match"] is None and got[0]["account_masked"] == "xyz***"

    def test_full_account_never_forwarded(self, client):
        c, got = client
        c._handle_filled(None, _content(account=OTHER))
        assert OTHER not in repr(got[0])


# ═══ session._append_fill_csv ════════════════════════════════════

def _acct_fill(order_no, lots, match, masked="xyz***", **kw):
    f = _fill(order_no, SYM, lots, LIMIT, **kw)
    f["account_match"] = match
    f["account_masked"] = masked
    return f


def _fills_path(tmp_path):
    return tmp_path / f"{datetime.now().strftime('%Y-%m-%d')}_fills.csv"


def _rows(path):
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.reader(fh))


def _mk():
    s = make_session(per_symbol=100_000)          # 10 萬 ÷ 4.5 萬 = 2 張
    s.set_limit_downs({SYM: DOWN})
    s.place_pre_orders([SYM], {SYM: LIMIT})
    return s


class TestFillCsvV2:
    def test_v2_header_and_twelve_cells(self, tmp_path):
        s = _mk()
        s._output_dir = tmp_path
        s._append_fill_csv(_acct_fill("K1", 1, True, "abc***", filled_no="F1"), True)
        s._append_fill_csv(_acct_fill("K2", 1, False, "xyz***", filled_no="F2"), False)
        s._append_fill_csv(_acct_fill("K3", 1, None, "", filled_no="F3"), True)
        rows = _rows(_fills_path(tmp_path))
        assert rows[0] == V2
        assert all(len(r) == 12 for r in rows[1:])
        assert [(r[7], r[9], r[10], r[11]) for r in rows[1:]] == [
            ("F1", "1", "1", "abc***"), ("F2", "0", "0", "xyz***"), ("F3", "1", "", "")]

    def test_legacy_fill_dict_without_account_keys(self, tmp_path):
        # 沒帶帳號欄的 fill (scripts/replay_day 等) → 兩格空字串
        s = _mk()
        s._output_dir = tmp_path
        s._append_fill_csv(_fill("K1", SYM, 1, LIMIT), False)
        assert _rows(_fills_path(tmp_path))[1][10:] == ["", ""]

    def test_existing_v1_header_keeps_appending_twelve_cells(self, tmp_path):
        # 同日部署後重啟: 檔案已有 10 欄表頭 → 不重寫表頭,照接 12 格列
        path = _fills_path(tmp_path)
        with path.open("w", encoding="utf-8", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(V1)
            w.writerow(["2026-10-28T09:00:00", SYM, "buy", 45.0, 1, 1000, "K0", "F0", "09:00:00", 1])
        s = _mk()
        s._output_dir = tmp_path
        s._append_fill_csv(_acct_fill("K1", 1, False, filled_no="F1"), False)
        rows = _rows(path)
        assert rows[0] == V1 and len(rows) == 3
        assert len(rows[1]) == 10 and len(rows[2]) == 12
        assert rows[2][10:] == ["0", "xyz***"]

    def test_written_rows_round_trip_through_live_days(self, tmp_path):
        pytest.importorskip("fastapi")
        import live_days as ld
        s = _mk()
        s._output_dir = tmp_path
        for i, m in enumerate((True, False, None)):
            s._append_fill_csv(_acct_fill(f"K{i}", 1, m, "" if m is None else "abc***",
                                          filled_no=f"F{i}"), m is True)
        path = _fills_path(tmp_path)
        day = path.name.removesuffix("_fills.csv")
        data = ld.read_fills(path, day, ld.Budget())
        assert data["header_valid"] is True and data["skipped_rows"] == 0
        assert [(f["account_match"], f["account_masked"], f["strategy"]) for f in data["fills"]] == [
            (True, "abc***", 1), (False, "abc***", 0), (None, "", 0)]


# ═══ session._on_fill ═══════════════════════════════════════════

def _sells(s):
    return [c for c in s.broker.placed if c[0] in ("limit_sell", "market_sell")]


class TestOnFillAccountMatch:
    def test_other_account_colliding_order_no_not_strategy(self, tmp_path, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")
        s = _mk()
        s._output_dir = tmp_path
        p = s.trades[SYM].order_no
        st = s.trades[SYM]
        reserved = st.budget_reserved
        s._on_fill(_acct_fill(p, 2, False))
        assert st.filled_lots == 0 and st.avg_price == 0
        assert st.budget_reserved == reserved
        assert s.budget_used == 2 * COST_PER_LOT                     # 只有原本的保留
        assert s._buy_cost_actual == 0 and st.buy_cost_actual == 0
        assert s.order_log[p]["filled_lots"] == 0 and s.order_log[p]["status"] == "pending"
        assert st.order_no == p and st.order_status == "pending"
        assert "別的子帳號成交" in caplog.text and p in caplog.text
        assert _rows(_fills_path(tmp_path))[1][9:] == ["0", "0", "xyz***"]
        # 之後本帳號自己的成交照常記帳 (別帳號那筆沒吃掉單筆委託的封頂額度)
        s._on_fill(_acct_fill(p, 2, True, "abc***", filled_no="F2"))
        assert st.filled_lots == 2 and st.avg_price == LIMIT
        assert s.order_log[p]["status"] == "filled"

    @pytest.mark.parametrize("match", [None, True, "missing"])
    def test_none_true_and_legacy_behave_as_before(self, tmp_path, match):
        s = _mk()
        s._output_dir = tmp_path
        p = s.trades[SYM].order_no
        f = _fill(p, SYM, 2, LIMIT) if match == "missing" else _acct_fill(p, 2, match)
        s._on_fill(f)
        st = s.trades[SYM]
        assert st.filled_lots == 2 and st.avg_price == LIMIT
        assert st.budget_reserved == 0
        assert s.budget_used == 2 * COST_PER_LOT
        assert s._buy_cost_actual == 2 * COST_PER_LOT
        assert s.order_log[p]["status"] == "filled"
        assert st.order_status == "done" and st.order_no == ""
        assert _rows(_fills_path(tmp_path))[1][9] == "1"

    @pytest.mark.parametrize("match,breached", [(False, False), (None, True)])
    def test_budget_breach_only_for_strategy_fill(self, match, breached):
        s = make_session(total=100_000, per_symbol=100_000)
        s.set_limit_downs({SYM: DOWN})
        s.place_pre_orders([SYM], {SYM: LIMIT})
        p = s.trades[SYM].order_no
        s._on_fill(dict(_acct_fill(p, 2, match), price=60.0))   # 2 張 × 6 萬 = 12 萬 > 總預算 10 萬
        assert s._budget_breached is breached

    @pytest.mark.parametrize("match,exits", [(False, False), (None, True)])
    def test_exit_on_fill_only_for_strategy_fill(self, monkeypatch, match, exits):
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s = _mk()
        p = s.trades[SYM].order_no
        assert s.mark_exit_on_fill(SYM, "first_check: ask") is True
        s._on_fill(_acct_fill(p, 2, match))
        if exits:
            deadline = _real_time.monotonic() + 3
            while _real_time.monotonic() < deadline and not _sells(s):
                _real_time.sleep(0.01)
            assert _sells(s) == [("limit_sell", SYM, DOWN, 2)]
        else:
            _real_time.sleep(0.3)
            assert _sells(s) == [] and s.trades[SYM].exited is False

    @pytest.mark.parametrize("match,late", [(False, []), (None, [(SYM, 1)])])
    def test_late_fill_after_exit_only_for_strategy_fill(self, monkeypatch, match, late):
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s = _mk()
        p = s.trades[SYM].order_no
        s._on_fill(_acct_fill(p, 1, True, "abc***"))
        s._exit_worker(SYM, "mkt_queue_gone")                  # 賣掉已成交的 1 張,exited
        st = s.trades[SYM]
        assert st.exited is True and st.exit_in_progress is False
        started = []
        monkeypatch.setattr(s, "_start_late_fill_exit",
                            lambda sym, lots, no, why="": started.append((sym, lots)))
        s._on_fill(_acct_fill(p, 1, match, filled_no="F9"))
        assert started == late

    @pytest.mark.parametrize("match,left", [(False, 2), (None, 0)])
    def test_colliding_strategy_sell_order(self, match, left):
        s = _mk()
        s._on_fill(_acct_fill(s.trades[SYM].order_no, 2, True, "abc***"))
        s._exit_worker(SYM, "mkt_queue_gone")
        sell_no = [no for no, r in s.order_log.items() if r.get("kind") == "limit_sell"]
        assert len(sell_no) == 1
        s._on_fill(_acct_fill(sell_no[0], 2, match, action="sell", filled_no="S1"))
        assert s.trades[SYM].filled_lots == left

    def test_other_account_sell_no_external_sell_warning(self, caplog):
        caplog.set_level(logging.INFO, logger="trading_session")
        s = _mk()
        s._on_fill(_acct_fill(s.trades[SYM].order_no, 2, True, "abc***"))
        s._on_fill(_acct_fill("X999", 1, False, action="sell", filled_no="S1"))
        assert "策略外賣出成交" not in caplog.text
        assert "其他帳號成交" in caplog.text and "X999" in caplog.text
        assert s.trades[SYM].filled_lots == 2

    @pytest.mark.parametrize("match", [None, True])
    def test_own_account_external_sell_still_warns(self, caplog, match):
        caplog.set_level(logging.INFO, logger="trading_session")
        s = _mk()
        s._on_fill(_acct_fill(s.trades[SYM].order_no, 2, True, "abc***"))
        s._on_fill(_acct_fill("X999", 1, match, action="sell", filled_no="S1"))
        assert "策略外賣出成交" in caplog.text
        assert "其他帳號成交" not in caplog.text
        assert s.trades[SYM].filled_lots == 2


class TestDedupeAccountSpace:
    """Codex 審查 P1 (2026-10-28): 去重 key (書號:成交序號:時間:張數) 不含帳號 → 別的子帳號成交先到、
    被忽略後仍佔住 key,本帳號同 key 的真成交被當重複丟掉 (部位 / 買進金額 / 台帳 / 出場保護全漏)。
    修法: account_match False 的成交另立去重空間;本帳號與無法判斷 (None) 共用原本的空間 (同舊版)。"""

    @pytest.mark.parametrize("own", [True, None])
    @pytest.mark.parametrize("filled_no", ["F1", ""])
    def test_other_first_then_own_identical_key_processed(self, tmp_path, own, filled_no):
        s = _mk()
        s._output_dir = tmp_path
        p = s.trades[SYM].order_no
        s._on_fill(_acct_fill(p, 2, False, "xyz***", filled_no=filled_no))
        s._on_fill(_acct_fill(p, 2, own, "abc***" if own else "", filled_no=filled_no))
        st = s.trades[SYM]
        assert st.filled_lots == 2 and st.avg_price == LIMIT
        assert s._buy_cost_actual == 2 * COST_PER_LOT and st.buy_cost_actual == 2 * COST_PER_LOT
        assert s.order_log[p]["status"] == "filled"
        rows = _rows(_fills_path(tmp_path))[1:]
        assert [(r[7], r[9], r[10]) for r in rows] == [
            (filled_no, "0", "0"), (filled_no, "1", "1" if own else "")]

    def test_other_first_then_own_identical_key_exit_protection(self, monkeypatch):
        # 淘汰後成交落地的出場保護不能因別帳號同 key 成交而漏掉
        monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 0.1)
        s = _mk()
        p = s.trades[SYM].order_no
        assert s.mark_exit_on_fill(SYM, "first_check: ask") is True
        s._on_fill(_acct_fill(p, 2, False, filled_no=""))
        s._on_fill(_acct_fill(p, 2, None, "", filled_no=""))
        deadline = _real_time.monotonic() + 3
        while _real_time.monotonic() < deadline and not _sells(s):
            _real_time.sleep(0.01)
        assert _sells(s) == [("limit_sell", SYM, DOWN, 2)]

    def test_own_first_then_other_identical_key_recorded_not_counted(self, tmp_path):
        # 反向: 本帳號先到 → 別帳號同 key 也照樣落台帳 ("0"),但不動部位
        s = _mk()
        s._output_dir = tmp_path
        p = s.trades[SYM].order_no
        s._on_fill(_acct_fill(p, 2, True, "abc***"))
        s._on_fill(_acct_fill(p, 2, False, "xyz***"))
        assert s.trades[SYM].filled_lots == 2
        assert s._buy_cost_actual == 2 * COST_PER_LOT
        assert [(r[9], r[10]) for r in _rows(_fills_path(tmp_path))[1:]] == [("1", "1"), ("0", "0")]

    def test_other_account_identical_reports_deduped_distinct_kept(self, tmp_path):
        # 別帳號之間: 完全相同的回報 (重複推送) 仍去重,同舊版;不同的回報每一筆都落台帳
        s = _mk()
        s._output_dir = tmp_path
        s._on_fill(_acct_fill("X1", 1, False, filled_no="G1"))
        s._on_fill(_acct_fill("X1", 1, False, filled_no="G1"))
        s._on_fill(_acct_fill("X1", 1, False, filled_no="G2"))
        s._on_fill(_acct_fill("X1", 1, False, filled_no=""))
        s._on_fill(_acct_fill("X1", 1, False, filled_no=""))
        rows = _rows(_fills_path(tmp_path))[1:]
        assert [r[7] for r in rows] == ["G1", "G2", ""]
        assert all(r[10] == "0" for r in rows)

    def test_own_and_unknown_identical_key_still_deduped(self, tmp_path):
        # 本帳號 (True) 與無法判斷 (None) 共用去重空間 — 舊版行為不變
        s = _mk()
        s._output_dir = tmp_path
        p = s.trades[SYM].order_no
        s._on_fill(_acct_fill(p, 1, None, ""))
        s._on_fill(_acct_fill(p, 1, True, "abc***"))
        assert s.trades[SYM].filled_lots == 1
        assert len(_rows(_fills_path(tmp_path))) == 2
