"""TickRecorder — 背景批次寫檔 (快路徑零 I/O,writer thread 序列化落檔)。"""
import json

from recorder import TickRecorder


class TestRecorder:
    def test_records_flush_to_file_on_close(self, tmp_path):
        f = tmp_path / "ticks.jsonl"
        rec = TickRecorder(f)
        rec.record("books", {"symbol": "2330", "bids": [{"price": 100.0, "size": 50}]})
        rec.record("trades", {"symbol": "2330", "price": 100.0, "size": 30})
        assert rec.count() == 2
        rec.close()      # close 會等 writer 把佇列 drain 完

        lines = f.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 2
        rows = [json.loads(ln) for ln in lines]      # 每行都是合法 JSON
        assert rows[0]["channel"] == "books"
        assert rows[0]["data"]["symbol"] == "2330"
        assert rows[1]["channel"] == "trades"
        assert "ts" in rows[0]

    def test_record_is_nonblocking_and_serializes_objects(self, tmp_path):
        class FakeTick:      # 模擬 SDK object (vars() 可取)
            def __init__(self):
                self.symbol = "1101"
                self.price = 50.0

        f = tmp_path / "ticks.jsonl"
        rec = TickRecorder(f)
        for _ in range(1000):
            rec.record("books", FakeTick())
        assert rec.count() == 1000
        rec.close()
        lines = f.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 1000
        assert json.loads(lines[0])["data"]["symbol"] == "1101"

    def test_queue_full_drops_without_raising(self, tmp_path):
        import queue as _q
        f = tmp_path / "ticks.jsonl"
        rec = TickRecorder(f)
        rec.close()                        # writer 已退出 → 換小佇列後不會被 drain
        rec._queue = _q.Queue(maxsize=2)
        rec.record("books", {"a": 1})
        rec.record("books", {"a": 2})
        rec.record("books", {"a": 3})      # 滿 → 丟棄 + 計數,絕不能 raise / block
        assert rec._dropped == 1
