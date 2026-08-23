"""6144 事故重放回歸測試 (2026-08-21)。

用當天真實委託台帳 (tests/fixtures/incident_2026-08-21_6144_orders.csv) 重放:
舊程式碼市價追 while-True 迴圈,遇「證券委託觸及價格穩定措施上、下限價格」拒單
(不在停止清單) → 26 秒狂送 100 筆。修正 (a818a83) 把「價格穩定」列入停止拒因後,
第一筆被拒就停。此測試讀真實拒單字串驅動修正後的程式碼,鎖住這個行為。
"""
import csv
from pathlib import Path

import pytest

from test_session_money import make_session

FIXTURE = Path(__file__).parent / "fixtures" / "incident_2026-08-21_6144_orders.csv"
SYMBOL = "6144"
LIMIT_UP = 14.65
SAFETY_CAP = 500        # 修正若失效 (仍狂送) → 在此攔下判 FAIL,不讓測試卡死


def _load_incident():
    """回 (歷史市價單筆數, 真實拒單訊息, 預掛張數)。"""
    mkt, reject_msgs, pre_lots = 0, set(), None
    with open(FIXTURE, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["symbol"] != SYMBOL:
                continue
            if row["action"] == "BUY_MKT":
                mkt += 1
                if row["order_id"] == "REJECTED":
                    reject_msgs.add(row["extra"].strip())
            elif row["action"] == "BUY_LMT":
                pre_lots = int(row["lots"])
    msg = next((m for m in reject_msgs if "價格穩定" in m), None)
    return mkt, msg, pre_lots


class TestReplay6144:
    def test_incident_fixture_facts(self):
        # 先確認 fixture 忠實反映事故: 大量市價單全被「價格穩定」拒
        hist_mkt, msg, pre_lots = _load_incident()
        assert hist_mkt >= 50, f"事故當天應有大量市價單狂送, 實得 {hist_mkt}"
        assert msg is not None, "拒單原因應含『價格穩定』"
        assert pre_lots == 20

    def test_price_stable_reject_stops_after_one_send(self):
        # 用真實拒單字串重放,跑修正後的市價追 → 只送 1 筆就停
        hist_mkt, msg, pre_lots = _load_incident()

        s = make_session(sizing_mode="fixed_lots", fixed_lots=pre_lots)
        s.place_pre_orders([SYMBOL], {SYMBOL: LIMIT_UP})

        sends = {"n": 0}

        def _replay_reject(symbol, lots):
            sends["n"] += 1
            assert sends["n"] <= SAFETY_CAP, (
                f"市價追送超過 {SAFETY_CAP} 筆仍未停 — 狂送 bug 又回來了")
            raise RuntimeError(msg)     # 重放當天『價格穩定措施』拒單
        s.broker.place_market_buy = _replay_reject

        # 首筆成交進場 (fill 尚未入帳 → shortfall>0,重現事故條件)
        s._first_trade_worker(SYMBOL, True)

        assert sends["n"] == 1, (
            f"修正後應只送 1 筆即停 (當天舊碼送了 {hist_mkt} 筆), 實得 {sends['n']}")
        assert s.trades[SYMBOL].stopped_reason == "fatal_reject"
        assert s.budget_used == 0        # 保留全釋放
