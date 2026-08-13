"""帳務台帳 pnl.py 測試 — upsert/覆寫/刪除/累積計算/日期驗證/原子寫入。"""
import json
from pathlib import Path

import pytest

import pnl


@pytest.fixture(autouse=True)
def tmp_ledger(tmp_path, monkeypatch):
    """每個測試用獨立 tmp ledger 檔,不碰真實 output/。"""
    f = tmp_path / "output" / "pnl_ledger.json"
    monkeypatch.setattr(pnl, "_ledger_file", lambda: f)
    return f


class TestUpsert:
    def test_insert_new(self):
        pnl.upsert("2026-08-13", 12500, "首日")
        recs = pnl.load()
        assert len(recs) == 1
        assert recs[0] == {"date": "2026-08-13", "pnl": 12500.0,
                           "note": "首日", "cumulative": 12500.0}

    def test_same_date_overwrite(self):
        """同日期再存 = 覆寫 (隨意修改某一日)。"""
        pnl.upsert("2026-08-13", 12500, "原始")
        pnl.upsert("2026-08-13", -3000, "修正")
        recs = pnl.load()
        assert len(recs) == 1
        assert recs[0]["pnl"] == -3000.0
        assert recs[0]["note"] == "修正"

    def test_bad_date_raises(self):
        for bad in ("2026/08/13", "20260813", "", "2026-8-13", "abc"):
            with pytest.raises(ValueError):
                pnl.upsert(bad, 100)

    def test_note_optional(self):
        pnl.upsert("2026-08-13", 500)
        assert pnl.load()[0]["note"] == ""


class TestRemove:
    def test_remove_existing(self):
        pnl.upsert("2026-08-13", 100)
        assert pnl.remove("2026-08-13") is True
        assert pnl.load() == []

    def test_remove_missing_returns_false(self):
        assert pnl.remove("2026-08-13") is False


class TestLoad:
    def test_missing_file_empty(self):
        assert pnl.load() == []

    def test_sorted_and_cumulative_with_negatives(self):
        """亂序輸入 → 依日期排序;累積含負值正確。"""
        pnl.upsert("2026-08-13", -8000)
        pnl.upsert("2026-08-11", 10000)
        pnl.upsert("2026-08-12", 5000)
        recs = pnl.load()
        assert [r["date"] for r in recs] == ["2026-08-11", "2026-08-12", "2026-08-13"]
        assert [r["cumulative"] for r in recs] == [10000.0, 15000.0, 7000.0]

    def test_corrupt_file_empty(self, tmp_ledger):
        tmp_ledger.parent.mkdir(exist_ok=True)
        tmp_ledger.write_text("not json", encoding="utf-8")
        assert pnl.load() == []


class TestAtomicWrite:
    def test_no_tmp_residue_and_valid_json(self, tmp_ledger):
        pnl.upsert("2026-08-13", 100)
        pnl.upsert("2026-08-14", 200)
        leftovers = list(tmp_ledger.parent.glob("*.tmp"))
        assert leftovers == []
        data = json.loads(tmp_ledger.read_text(encoding="utf-8"))
        assert set(data) == {"2026-08-13", "2026-08-14"}
