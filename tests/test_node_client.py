"""node_client.pull_marked_snapshot — 輪詢/重試/死線 (中心過濾架構 node 端)。
用假時鐘 + 假 HTTP,不真連網。"""
import json

from node_client import pull_marked_snapshot


class _Resp:
    def __init__(self, payload):
        self._raw = json.dumps(payload).encode("utf-8")
    def read(self):
        return self._raw
    def close(self):
        pass


class _Clock:
    def __init__(self):
        self.t = 0.0
    def time(self):
        return self.t
    def sleep(self, s):
        self.t += s


def test_returns_when_final_true():
    snap = {"final": True, "role": "hub", "symbols": [{"symbol": "8105"}]}
    clk = _Clock()
    out = pull_marked_snapshot("http://hub:8100", deadline_ts=10,
                               urlopen=lambda u, timeout: _Resp(snap),
                               time_fn=clk.time, sleep_fn=clk.sleep)
    assert out == snap


def test_retries_until_final():
    # 前兩次 final=false (Hub 未凍結) → 第三次 final=true
    seq = [{"final": False, "symbols": []}, {"final": False, "symbols": []},
           {"final": True, "symbols": [{"symbol": "6933"}]}]
    calls = {"n": 0}
    def _open(u, timeout):
        r = _Resp(seq[min(calls["n"], len(seq) - 1)]); calls["n"] += 1; return r
    clk = _Clock()
    out = pull_marked_snapshot("http://hub:8100", deadline_ts=10,
                               retry_interval=0.5, urlopen=_open,
                               time_fn=clk.time, sleep_fn=clk.sleep)
    assert out["final"] is True and out["symbols"][0]["symbol"] == "6933"
    assert calls["n"] == 3


def test_retries_on_exception_then_success():
    calls = {"n": 0}
    def _open(u, timeout):
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("hub 還沒起來")
        return _Resp({"final": True, "symbols": []})
    clk = _Clock()
    out = pull_marked_snapshot("http://hub:8100", deadline_ts=10, retry_interval=0.5,
                               urlopen=_open, time_fn=clk.time, sleep_fn=clk.sleep)
    assert out is not None and out["final"] is True


def test_deadline_returns_none():
    # 一直 final=false → 過死線回 None (node 今日不交易,安全)
    clk = _Clock()
    out = pull_marked_snapshot("http://hub:8100", deadline_ts=3, retry_interval=0.5,
                               urlopen=lambda u, timeout: _Resp({"final": False, "symbols": []}),
                               time_fn=clk.time, sleep_fn=clk.sleep)
    assert out is None


def test_deadline_returns_none_on_persistent_error():
    def _open(u, timeout):
        raise ConnectionError("hub 掛了")
    clk = _Clock()
    out = pull_marked_snapshot("http://hub:8100", deadline_ts=3, retry_interval=0.5,
                               urlopen=_open, time_fn=clk.time, sleep_fn=clk.sleep)
    assert out is None
