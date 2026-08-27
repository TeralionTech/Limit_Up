"""月均量:離線腳本純計算核心 (股→張、取近 N 日均) + 盤前篩選 (fail-open) 正確性。"""
import json

import pytest

import avg_volume
from scripts.compute_avg_volume import compute_avg_lots, _write_guard_ok, MIN_SANE_UNIVERSE


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


_WRAPPED = {"date": "2026-08-26", "generated_at": "2026-08-26T07:31:00+08:00",
            "avg_lots": {"2330": 25000.0, "1101": 800.5}}


class TestLoadWrappedShape:
    """Stage A5 包裝形 {date, generated_at, avg_lots} — 兩半分開部署,先到哪半都不能壞。"""

    def test_wrapped_shape(self, tmp_path):
        # 突變必殺: load 不解包 (包裝形判斷改永不成立) → data=={} 且 meta["date"] None
        f = tmp_path / "avg_volume.json"
        f.write_text(json.dumps(_WRAPPED), encoding="utf-8")
        data, meta = avg_volume.load(f)
        assert data == {"2330": 25000.0, "1101": 800.5}
        assert meta["count"] == 2
        assert meta["date"] == "2026-08-26"       # 檔內日期,非 mtime

    def test_legacy_flat_still_loads(self, tmp_path):
        # 迴歸: 舊平面形照舊載入 (VPS 腳本先/後部署都行);檔內無日期 → meta["date"] None
        # 突變必殺: load 把所有 dict 都當包裝形解 (拿掉 key 檢查) → data=={}
        f = tmp_path / "avg_volume.json"
        f.write_text(json.dumps({"2330": 25000.0}), encoding="utf-8")
        data, meta = avg_volume.load(f)
        assert data == {"2330": 25000.0}
        assert meta["date"] is None


class TestAvgVolumeFullEndpoint:
    """GET /api/avg-volume/full — IDC 端每日 08:05 拉全量 map。
    route 不經 Runner,直接呼叫函式測 (免起 app)。"""

    @pytest.fixture(autouse=True)
    def _needs_fastapi(self):
        # 只窄跳 fastapi 沒裝的環境 (CI 裝 requirements 一定跑);
        # 不 importorskip("api") — 那會把 api.py 本身壞掉偽裝成 skip
        pytest.importorskip("fastapi")

    @staticmethod
    def _call(monkeypatch, path, token=None, auth=None):
        import api
        monkeypatch.setenv("AVG_VOLUME_FILE", str(path))
        if token is None:
            monkeypatch.delenv("AVGVOL_TOKEN", raising=False)
        else:
            monkeypatch.setenv("AVGVOL_TOKEN", token)
        return api.get_avg_volume_full(authorization=auth)

    def test_wrapped_file_served(self, tmp_path, monkeypatch):
        # 突變必殺: route 的 count 改回 0 (或 date/avg_lots 不從檔取)
        # 也蓋「沒設 AVGVOL_TOKEN = 開放」: _call 未給 token → 不帶 header 也 200
        f = tmp_path / "avg_volume.json"
        f.write_text(json.dumps(_WRAPPED), encoding="utf-8")
        resp = self._call(monkeypatch, f)
        assert resp["exists"] is True
        assert resp["date"] == "2026-08-26"
        assert resp["generated_at"] == "2026-08-26T07:31:00+08:00"
        assert resp["count"] == 2
        assert resp["avg_lots"] == {"2330": 25000.0, "1101": 800.5}

    def test_missing_file_exists_false(self, tmp_path, monkeypatch):
        # 突變必殺: 拿掉缺檔 early-return → 讀檔 raise (=500) 而非 {"exists": False}
        resp = self._call(monkeypatch, tmp_path / "nope.json")
        assert resp == {"exists": False}

    def test_401_when_token_set_and_header_wrong(self, tmp_path, monkeypatch):
        # 突變必殺: 比對弱化成「只檢查有沒有 header」→ 錯 token 也 200
        from fastapi import HTTPException
        f = tmp_path / "avg_volume.json"
        f.write_text(json.dumps(_WRAPPED), encoding="utf-8")
        with pytest.raises(HTTPException) as ei:               # 沒帶 header
            self._call(monkeypatch, f, token="s3cret", auth=None)
        assert ei.value.status_code == 401
        with pytest.raises(HTTPException) as ei:               # 帶錯 token
            self._call(monkeypatch, f, token="s3cret", auth="Bearer wrong")
        assert ei.value.status_code == 401

    def test_200_with_correct_bearer(self, tmp_path, monkeypatch):
        # 突變必殺: 有設 token 就一律 401 (正確 Bearer 也擋) → 這裡 raise
        f = tmp_path / "avg_volume.json"
        f.write_text(json.dumps(_WRAPPED), encoding="utf-8")
        resp = self._call(monkeypatch, f, token="s3cret", auth="Bearer s3cret")
        assert resp["exists"] is True and resp["count"] == 2


class TestWriteGuard:
    """sanity 防護 (2026-08-27 事故): 抓到的檔數太少 → 不覆蓋既有好檔,沿用舊檔。"""

    def test_enough_writes(self):
        assert _write_guard_ok(1944, MIN_SANE_UNIVERSE) is True
        assert _write_guard_ok(500, 500) is True          # 剛好門檻 → 可寫

    def test_too_few_blocks(self):
        assert _write_guard_ok(1, MIN_SANE_UNIVERSE) is False   # 今早的 1 檔殘檔 → 擋下
        assert _write_guard_ok(0, MIN_SANE_UNIVERSE) is False
        assert _write_guard_ok(499, 500) is False

    def test_default_threshold_sane(self):
        # 母體正常 ~1944 → 門檻 500 遠低於正常、遠高於殘檔 (0~1),不會誤擋也不會漏接
        assert 1 < MIN_SANE_UNIVERSE < 1900
