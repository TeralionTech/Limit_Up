"""個股金額覆寫 symbol_budget.py 持久化測試。"""
import json

import pytest

import symbol_budget


@pytest.fixture(autouse=True)
def tmp_file(tmp_path, monkeypatch):
    f = tmp_path / "output" / "symbol_budgets.json"
    monkeypatch.setattr(symbol_budget, "_budget_file", lambda: f)
    return f


class TestUpsert:
    def test_insert_and_load(self):
        symbol_budget.upsert("2330", 500000)
        assert symbol_budget.load() == {"2330": 500000.0}

    def test_overwrite(self):
        symbol_budget.upsert("2330", 500000)
        symbol_budget.upsert("2330", 300000)
        assert symbol_budget.load()["2330"] == 300000.0

    def test_symbol_normalized_upper(self):
        symbol_budget.upsert("2330a", 100000)
        assert "2330A" in symbol_budget.load()

    def test_bad_symbol(self):
        for bad in ("", "ab", "12", "12345678", "!!"):
            with pytest.raises(ValueError):
                symbol_budget.upsert(bad, 100000)

    def test_bad_amount(self):
        for bad in (0, -5, "abc"):
            with pytest.raises(ValueError):
                symbol_budget.upsert("2330", bad)


class TestRemove:
    def test_remove(self):
        symbol_budget.upsert("2330", 500000)
        assert symbol_budget.remove("2330") is True
        assert symbol_budget.load() == {}

    def test_remove_missing(self):
        assert symbol_budget.remove("2330") is False


class TestLoad:
    def test_missing_file_empty(self):
        assert symbol_budget.load() == {}

    def test_corrupt_empty(self, tmp_file):
        tmp_file.parent.mkdir(exist_ok=True)
        tmp_file.write_text("nope", encoding="utf-8")
        assert symbol_budget.load() == {}

    def test_atomic_no_tmp_residue(self, tmp_file):
        symbol_budget.upsert("2330", 500000)
        symbol_budget.upsert("2454", 300000)
        assert list(tmp_file.parent.glob("*.tmp")) == []
        data = json.loads(tmp_file.read_text(encoding="utf-8"))
        assert set(data) == {"2330", "2454"}
