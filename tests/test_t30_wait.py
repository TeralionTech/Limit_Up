"""T30 取檔時序修正 (2026-09-28)。

事故背景: runner 走到「讀全額交割名單」那一行的時間取決於漲停價抓取耗時
(月均量篩選把母體砍到 911 檔後只要 ~4 分鐘 → 08:04:55),而 fetch_t30.timer 固定 08:05 才取檔
→ **每天**都讀到前一交易日的名單 (production journal 自 09-15 起天天 CRITICAL,無一例外)。
2026-08-12 全額交割股狂送單事故就是靠這份名單擋的。

修法: 讀之前先等今日檔 (最晚 T30_WAIT_UNTIL,預設 08:10);等不到 → 照用舊檔 + CRITICAL,
並排一次背景重讀 (fetch_t30.sh 失敗會每 3 分鐘重試到 08:25),趕在 08:30 篩選前補正。
不把 timer 提前的理由: fetch_t30.sh 自己註明「太早取會拿到昨日內容」,而新舊判斷只看 mtime
→ 提前取檔會把昨日內容蓋上今日 mtime,連 CRITICAL 都不叫 (安靜地用舊資料)。
"""
import logging
import os
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest

import t30
from runner import Runner


@pytest.fixture(autouse=True)
def _restore_env():
    """這些測試會設 T30_* 環境變數 — 用完還原,免得污染同一個 pytest 程序裡的其他測試。"""
    keys = ("T30_DIR", "T30_WAIT_UNTIL", "T30_RECHECK_AT", "T30_RECHECK_POLL_SEC")
    saved = {k: os.environ.get(k) for k in keys}
    yield
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def _rec(stock_no: str, settype: str = "0", mark_w: str = "0") -> bytes:
    b = bytearray(b"0" * t30.RECORD_SIZE)
    b[0:6] = f"{stock_no:<6}".encode("ascii")
    b[41] = ord(settype)
    b[42] = ord(mark_w)
    return bytes(b)


def _write(t30_dir: Path, syms_tse, syms_otc, days_ago: int = 0) -> None:
    """寫兩個 T30 檔;days_ago>0 → 把 mtime 設成前幾天 (模擬還沒取今日檔)。"""
    t30_dir.mkdir(parents=True, exist_ok=True)
    for name, syms in (("T30V.TSE", syms_tse), ("T30V.OTC", syms_otc)):
        p = t30_dir / name
        p.write_bytes(b"".join(_rec(s, settype="1") for s in syms) or _rec("0000"))
        if days_ago:
            ts = (datetime.now() - timedelta(days=days_ago)).timestamp()
            os.utime(p, (ts, ts))


def _runner(t30_dir: Path) -> Runner:
    r = Runner()                       # 直接建構 (不走 get() singleton;不 login)
    os.environ["T30_DIR"] = str(t30_dir)
    return r


class TestFilesState:
    def test_today_vs_stale_vs_missing(self, tmp_path):
        _write(tmp_path, ["1101"], ["6547"])
        st = t30.files_state(tmp_path)
        assert st["T30V.TSE"]["today"] and st["T30V.OTC"]["today"]

        ts = (datetime.now() - timedelta(days=3)).timestamp()
        os.utime(tmp_path / "T30V.OTC", (ts, ts))
        st = t30.files_state(tmp_path)
        assert st["T30V.TSE"]["today"] is True
        assert st["T30V.OTC"]["today"] is False
        assert st["T30V.OTC"]["mtime_date"] == (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")

        (tmp_path / "T30V.OTC").unlink()
        st = t30.files_state(tmp_path)
        assert st["T30V.OTC"] == {"exists": False, "mtime_date": None, "today": False}

    def test_does_not_parse_content(self, tmp_path):
        """壞掉的檔案也不該讓 files_state 爆 — 它只看 mtime。"""
        tmp_path.mkdir(parents=True, exist_ok=True)
        (tmp_path / "T30V.TSE").write_bytes(b"not-a-valid-t30-file")
        (tmp_path / "T30V.OTC").write_bytes(b"x" * 17)
        st = t30.files_state(tmp_path)
        assert st["T30V.TSE"]["today"] and st["T30V.OTC"]["today"]


class TestWaitForToday:
    def test_no_wait_when_already_today(self, tmp_path, caplog):
        _write(tmp_path, ["9103"], ["2491"])
        r = _runner(tmp_path)
        t0 = time.time()
        with caplog.at_level(logging.INFO):
            r._load_t30_untradable(wait_until="23:59", poll_sec=5.0)
        assert time.time() - t0 < 1.0, "今日檔已在 → 不該等"
        assert r.session.untradable == {"9103", "2491"}
        assert not [x for x in caplog.records if x.levelno >= logging.CRITICAL]

    def test_waits_until_file_becomes_today(self, tmp_path, caplog):
        """開始時是舊檔 → 等;取檔 timer 完成後立刻往下,且不叫 CRITICAL。"""
        _write(tmp_path, ["1111"], ["2222"], days_ago=1)
        r = _runner(tmp_path)

        def _fetch_later():
            time.sleep(0.3)
            _write(tmp_path, ["9103", "6547"], ["2491"])     # 今日檔到位 (mtime=現在)

        threading.Thread(target=_fetch_later, daemon=True).start()
        t0 = time.time()
        with caplog.at_level(logging.INFO):
            r._load_t30_untradable(wait_until="23:59", poll_sec=0.05)
        waited = time.time() - t0
        assert 0.25 < waited < 5.0, f"應該等到檔案更新才往下 (等了 {waited:.2f}s)"
        assert r.session.untradable == {"9103", "6547", "2491"}, "要用今日檔的內容"
        msgs = " ".join(x.getMessage() for x in caplog.records)
        assert "T30 今日檔已就位" in msgs
        assert "非今日" not in msgs, "等到今日檔就不該再叫過時"

    def test_deadline_passed_uses_stale_and_criticals(self, tmp_path, caplog):
        """等不到今日檔 → 不卡住主流程,照用舊檔並 CRITICAL 點名。"""
        _write(tmp_path, ["1111"], ["2222"], days_ago=1)
        r = _runner(tmp_path)
        os.environ["T30_RECHECK_AT"] = "00:00"          # 重讀也立刻放棄,本測試只看主路徑
        t0 = time.time()
        with caplog.at_level(logging.INFO):
            r._load_t30_untradable(wait_until="00:00", poll_sec=0.05)
        assert time.time() - t0 < 1.0, "死線已過 → 不該再等"
        assert r.session.untradable == {"1111", "2222"}, "舊檔照用 (有保護總比沒有好)"
        crits = " ".join(x.getMessage() for x in caplog.records if x.levelno >= logging.CRITICAL)
        assert "非今日" in crits

    def test_stop_event_breaks_wait(self, tmp_path):
        _write(tmp_path, ["1111"], ["2222"], days_ago=1)
        r = _runner(tmp_path)
        r._stop_event.set()
        t0 = time.time()
        assert r._wait_t30_today(str(tmp_path), "23:59", 0.05) is False
        assert time.time() - t0 < 1.0, "stop_event 要能立刻中斷等待"

    def test_missing_files_still_criticals_and_does_not_hang(self, tmp_path, caplog):
        r = _runner(tmp_path)                            # 目錄空的
        os.environ["T30_RECHECK_AT"] = "00:00"
        t0 = time.time()
        with caplog.at_level(logging.INFO):
            r._load_t30_untradable(wait_until="00:00", poll_sec=0.05)
        assert time.time() - t0 < 1.0
        assert r.session.untradable == set()
        assert "T30 檔案全缺" in " ".join(x.getMessage() for x in caplog.records)


class TestRecheck:
    def test_recheck_replaces_stale_list(self, tmp_path, caplog):
        """第一次拿到舊檔 → 背景重讀在取檔成功後把名單補正 (08:30 篩選前)。"""
        _write(tmp_path, ["1111"], ["2222"], days_ago=1)
        r = _runner(tmp_path)
        os.environ["T30_RECHECK_AT"] = "23:59"
        os.environ["T30_RECHECK_POLL_SEC"] = "0.05"
        try:
            with caplog.at_level(logging.INFO):
                r._load_t30_untradable(wait_until="00:00", poll_sec=0.05)
                assert r.session.untradable == {"1111", "2222"}      # 先用舊的
                _write(tmp_path, ["9103", "6547"], ["2491"])          # 取檔 timer 重試成功
                deadline = time.time() + 5.0
                while time.time() < deadline and r.session.untradable != {"9103", "6547", "2491"}:
                    time.sleep(0.05)
            assert r.session.untradable == {"9103", "6547", "2491"}, "重讀要把名單換成今日的"
            msgs = " ".join(x.getMessage() for x in caplog.records)
            assert "T30 重讀成功" in msgs
            assert "新增 3" in msgs and "移除 2" in msgs
        finally:
            r._stop_event.set()
            os.environ.pop("T30_RECHECK_POLL_SEC", None)

    def test_no_recheck_when_first_load_is_today(self, tmp_path):
        _write(tmp_path, ["9103"], ["2491"])
        r = _runner(tmp_path)
        before = {t.name for t in threading.enumerate()}
        r._load_t30_untradable(wait_until="23:59", poll_sec=0.05)
        after = {t.name for t in threading.enumerate()}
        assert "t30-recheck" not in (after - before), "今日檔就不該再排重讀"
