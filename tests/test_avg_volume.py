"""月均量:離線腳本純計算核心 (股→張、取近 N 日均) + 盤前篩選 (fail-open) 正確性。"""
import json

import pytest

import avg_volume
from scripts.compute_avg_volume import compute_avg_lots


def _c(date, vol):
    return {"date": date, "volume": vol}


class TestComputeAvgLots:
    def test_unit_shares_to_lots(self):
        # ⚠ 單位核心: volume 是「股」, ÷1000 = 張。0050 例: 9,239,321 股 = 9239.3 張
        assert compute_avg_lots([_c("2026-08-20", 9_239_321)], days=20) == 9239.3

    def test_average_over_days(self):
        # 3 天 volume 1000/2000/3000 股 → 均 2000 股 = 2.0 張
        candles = [_c("2026-08-18", 1000), _c("2026-08-19", 2000), _c("2026-08-20", 3000)]
        assert compute_avg_lots(candles, days=20) == 2.0

    def test_takes_most_recent_n_days(self):
        # 給 5 天, 只取最近 2 天 (日期最新) 算均
        candles = [_c("2026-08-16", 100_000_000), _c("2026-08-17", 100_000_000),
                   _c("2026-08-18", 100_000_000),
                   _c("2026-08-19", 1_000_000), _c("2026-08-20", 3_000_000)]
        # 近 2 天 = (1,000,000 + 3,000,000)/2 = 2,000,000 股 = 2000 張
        assert compute_avg_lots(candles, days=2) == 2000.0

    def test_unsorted_input_sorted_by_date(self):
        candles = [_c("2026-08-20", 3_000_000), _c("2026-08-18", 1_000_000),
                   _c("2026-08-19", 2_000_000)]
        assert compute_avg_lots(candles, days=2) == 2500.0   # (2M+3M)/2 /1000

    def test_short_history_averages_available(self):
        # 只有 3 天資料但要 20 天 → 用現有 3 天算 (不足不補零)
        candles = [_c("2026-08-18", 500_000), _c("2026-08-19", 500_000),
                   _c("2026-08-20", 500_000)]
        assert compute_avg_lots(candles, days=20) == 500.0

    def test_empty_and_no_date(self):
        assert compute_avg_lots([], days=20) == 0.0
        assert compute_avg_lots([{"volume": 5_000_000}], days=20) == 0.0   # 無 date 略過

    def test_missing_volume_treated_zero(self):
        candles = [_c("2026-08-19", None), _c("2026-08-20", 2_000_000)]
        assert compute_avg_lots(candles, days=20) == 1000.0   # (0+2M)/2 /1000


class TestLoad:
    def test_missing_file(self, tmp_path):
        data, meta = avg_volume.load(tmp_path / "nope.json")
        assert data == {} and meta["exists"] is False

    def test_load_and_meta(self, tmp_path):
        f = tmp_path / "avg_volume.json"
        f.write_text(json.dumps({"2330": 30000.0, "1101": 8000.5}), encoding="utf-8")
        data, meta = avg_volume.load(f)
        assert data == {"2330": 30000.0, "1101": 8000.5}
        assert meta["exists"] and meta["count"] == 2 and meta["stale"] is False

    def test_corrupt_file(self, tmp_path):
        f = tmp_path / "avg_volume.json"
        f.write_text("nope", encoding="utf-8")
        data, meta = avg_volume.load(f)
        assert data == {} and meta["exists"] is True


class TestFilterUniverse:
    def test_drops_only_known_low(self):
        # 2330 高量保留、1101 低量剔除、9999 無資料保留 (fail-open)
        avg = {"2330": 30000.0, "1101": 100.0}
        kept, dropped = avg_volume.filter_universe(["2330", "1101", "9999"], avg, 500)
        assert kept == ["2330", "9999"]
        assert dropped == ["1101"]

    def test_boundary_500_kept(self):
        # 剛好 500 張 → 保留 (< 才剔除)
        kept, dropped = avg_volume.filter_universe(["2330"], {"2330": 500.0}, 500)
        assert kept == ["2330"] and dropped == []

    def test_empty_avg_keeps_all(self):
        # 完全沒資料 → 全保留 (fail-open)
        kept, dropped = avg_volume.filter_universe(["2330", "1101"], {}, 500)
        assert kept == ["2330", "1101"] and dropped == []


class TestRunnerAvgVolCapture:
    """runner._filter_low_volume 保存今天結果到 self._avgvol (/api/avg-volume 顯示用)。"""

    @staticmethod
    def _runner(universe, thr=500):
        from types import SimpleNamespace
        from runner import Runner
        r = Runner.__new__(Runner)                 # 不跑 __init__,只測單一方法
        r.cfg = SimpleNamespace(avg_volume_min_lots=thr)
        r.universe = list(universe)
        r._avgvol = {}
        return r

    def test_capture_dropped_kept(self, tmp_path, monkeypatch):
        f = tmp_path / "avg_volume.json"
        f.write_text(json.dumps({"2330": 30000.0, "1101": 100.0}), encoding="utf-8")
        monkeypatch.setenv("AVG_VOLUME_FILE", str(f))
        r = self._runner(["2330", "1101", "9999"])
        r._filter_low_volume()
        av = r._avgvol
        assert av["ran"] is True
        assert av["universe_before"] == 3
        assert av["kept"] == 2 and av["dropped"] == 1        # 1101 剔除;2330/9999 保留
        assert "1101" in av["dropped_sample"]
        assert av["meta"]["count"] == 2
        assert r.universe == ["2330", "9999"]                 # fail-open: 9999 無資料保留

    def test_capture_disabled(self):
        r = self._runner([], thr=0)
        r._filter_low_volume()
        assert r._avgvol == {"ran": False, "reason": "disabled", "threshold": 0}

    def test_capture_file_missing_fail_open(self, tmp_path, monkeypatch):
        monkeypatch.setenv("AVG_VOLUME_FILE", str(tmp_path / "nope.json"))
        r = self._runner(["2330"])
        r._filter_low_volume()
        assert r._avgvol["ran"] is False and r._avgvol["reason"] == "file_missing"
        assert r.universe == ["2330"]                          # 檔缺 → 不剔 (fail-open)
