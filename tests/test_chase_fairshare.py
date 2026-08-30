"""狂送公平均分節拍 (2026-08-30) — 45/s 依「目前還在搶的檔數 N」平均分,每檔均勻送 (非爆發):
  1 檔 → 該檔每 ~22.2ms 一張 (45/s);2 檔 → 每檔 ~44.4ms (各 22.5/s),全域仍 ~45/s;
  搶到一檔 (chase_done) 退出 → 剩餘檔即刻加速。全域任一 1 秒窗 ≤ 富邦 50/s。

執行緒計時測試 → 用寬鬆界(比率/上限/相對變化為主),避免負載抖動誤判。
"""
import bisect
import threading
import time
from datetime import time as dtime

from test_session_money import make_session


class RejectBroker:
    """reject-forever: 記 (symbol, monotonic ts),送出即以非致命拒單 → cadence 續送,量純節拍。"""
    connected = True
    healthy = True

    def __init__(self, lat=0.0):
        self.lat = lat
        self.ev = []                       # [(symbol, ts)]
        self._lk = threading.Lock()
        self._n = 0

    def _no(self):
        with self._lk:
            self._n += 1
            return f"O{self._n}"

    def place_limit_buy(self, *a):
        return self._no()

    def place_market_buy(self, sym, lots):
        with self._lk:
            self.ev.append((sym, time.monotonic()))
        if self.lat:
            time.sleep(self.lat)
        raise Exception("集合競價時段不可輸入市價委託")   # 非致命 → 續送

    def place_market_sell(self, *a, **k):
        return self._no()

    def place_limit_sell(self, *a, **k):
        return self._no()

    def cancel(self, *a, **k):
        pass

    def get_order_filled_lots(self, o):
        return 0

    def get_inventories(self):
        return []

    def get_filled_map(self):
        return {}

    def status(self):
        return {"connected": True, "healthy": True, "account_masked": "x",
                "is_test": True, "error": ""}


def _run(nsym, dur=1.0, lat=0.0):
    s = make_session(total=500_000_000, per_symbol=300_000)
    s.broker = RejectBroker(lat)
    s.cancel_pending_time = dtime(23, 59, 59)
    syms = [f"S{i}" for i in range(nsym)]
    s.place_pre_orders(syms, {x: 20.0 for x in syms})
    t0 = time.monotonic()
    ths = []
    for x in syms:
        th = threading.Thread(target=s._market_chase_worker,
                              args=(x, dtime(0, 0, 0), dtime(23, 59, 59)), daemon=True)
        th.start()
        ths.append(th)
    time.sleep(dur)
    for x in syms:
        st = s.trades.get(x)
        if st:
            st.stopped_reason = "stop"
    for th in ths:
        th.join(timeout=3)
    return s, [(sym, t - t0) for sym, t in s.broker.ev if t - t0 <= dur]


def _counts(ev, syms):
    c = {x: 0 for x in syms}
    for sym, _ in ev:
        if sym in c:
            c[sym] += 1
    return c


class TestChaseFairShare:
    def test_single_symbol_uses_full_rate(self):
        # 1 檔獨佔 → ~45/s (非爆發到上百、也非序列化卡在個位數)
        _s, ev = _run(1)
        assert 30 <= len(ev) <= 55, f"1 檔應 ~45/s,實得 {len(ev)}"

    def test_two_symbols_split_evenly(self):
        # 2 檔 → 全域仍 ~45/s、且兩檔平均分
        _s, ev = _run(2)
        c = _counts(ev, ["S0", "S1"])
        total = c["S0"] + c["S1"]
        assert 30 <= total <= 55, f"2 檔全域應 ~45/s,實得 {total}"
        assert c["S0"] > 5 and c["S1"] > 5
        assert abs(c["S0"] - c["S1"]) <= max(5, total * 0.30), f"兩檔應平均分: {c}"

    def test_per_symbol_rate_drops_with_more_symbols(self):
        # 公平均分核心: 2 檔時單檔速率應明顯低於 1 檔 (約一半)
        _s1, ev1 = _run(1)
        _s2, ev2 = _run(2)
        per1 = len(ev1)
        per2 = _counts(ev2, ["S0", "S1"])["S0"]
        assert per2 < per1 * 0.75, f"2 檔單檔({per2})應明顯 < 1 檔({per1})"

    def test_never_exceeds_broker_cap_per_window(self):
        # 全域任一 1 秒窗 ≤ 48 (< 富邦 50)
        _s, ev = _run(3, dur=1.5)
        ts = sorted(t for _, t in ev)
        worst = 0
        for t in ts:
            lo = bisect.bisect_left(ts, t - 1.0)
            hi = bisect.bisect_right(ts, t)
            worst = max(worst, hi - lo)
        assert worst <= 48, f"任一 1 秒窗 {worst} 筆應 ≤48 (<富邦 50)"

    def test_remaining_speed_up_when_one_grabbed(self):
        # 3 檔跑 → 1 檔 chase_done 退出 → 剩 2 檔間隔變短 (加速)
        s = make_session(total=500_000_000, per_symbol=300_000)
        s.broker = RejectBroker(0.0)
        s.cancel_pending_time = dtime(23, 59, 59)
        syms = ["A", "B", "C"]
        s.place_pre_orders(syms, {x: 20.0 for x in syms})
        t0 = time.monotonic()
        ths = []
        for x in syms:
            th = threading.Thread(target=s._market_chase_worker,
                                  args=(x, dtime(0, 0, 0), dtime(23, 59, 59)), daemon=True)
            th.start()
            ths.append(th)
        time.sleep(0.8)
        tcut = time.monotonic() - t0
        s.trades["C"].chase_done = True            # C 搶到 → 退出 → N 3→2
        time.sleep(0.8)
        for x in syms:
            s.trades[x].stopped_reason = "stop"
        for th in ths:
            th.join(timeout=3)

        def med_gap(x, before):
            ts = sorted(t - t0 for sym, t in s.broker.ev if sym == x)
            g = [(ts[i] - ts[i - 1]) * 1000 for i in range(1, len(ts))
                 if (ts[i] < tcut if before else ts[i - 1] >= tcut)]
            g.sort()
            return g[len(g) // 2] if g else 0.0

        for x in ["A", "B"]:
            before, after = med_gap(x, True), med_gap(x, False)
            assert before > 0 and after > 0
            assert after < before * 0.85, f"{x} 退出後({after:.0f}ms)應快於退出前({before:.0f}ms)"
