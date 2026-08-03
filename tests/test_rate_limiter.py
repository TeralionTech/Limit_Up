"""SendRateLimiter — 爆發式滑動窗口送單風控。

核心保證: (1) 一秒內未滿額度**立刻放行** (可爆發,45 筆可在一秒最前面送完);
(2) 任一 1 秒窗口內放行數 ≤ max_per_sec (不超富邦 50/s)。
"""
import threading
import time

from trading_session import SendRateLimiter


class TestBurst:
    def test_full_burst_is_immediate(self):
        # 45 筆一口氣放行 — 不是每筆隔 0.02s 鋪開 (那要 0.9s)
        rl = SendRateLimiter(45)
        t0 = time.monotonic()
        for _ in range(45):
            rl.acquire()
        assert time.monotonic() - t0 < 0.2

    def test_over_cap_waits_for_window_slide(self):
        # 第 cap+1 筆要等最舊一筆滑出 1 秒窗口
        rl = SendRateLimiter(5)
        t0 = time.monotonic()
        for _ in range(6):
            rl.acquire()
        assert time.monotonic() - t0 >= 0.95


class TestWindowCap:
    def test_concurrent_never_exceeds_cap_per_window(self):
        # 3 threads × 4 = 12 筆,cap 5/s → 任一 1 秒窗口 ≤ 5,總耗時 ≥ ~2s
        rl = SendRateLimiter(5)
        stamps = []
        stamps_lock = threading.Lock()

        def worker():
            for _ in range(4):
                rl.acquire()
                with stamps_lock:
                    stamps.append(time.monotonic())

        threads = [threading.Thread(target=worker) for _ in range(3)]
        t0 = time.monotonic()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.monotonic() - t0

        assert len(stamps) == 12
        assert elapsed >= 1.9          # 12 筆 @5/s → 三個窗口 (5+5+2)
        stamps.sort()
        # 若任一 1 秒窗口超過 5 筆,必存在 i 使 stamps[i+5] - stamps[i] < 1.0
        for i in range(len(stamps) - 5):
            assert stamps[i + 5] - stamps[i] >= 0.99, \
                f"1 秒窗口內超過 5 筆: {stamps[i + 5] - stamps[i]:.3f}s"

    def test_window_refills_after_idle(self):
        # 用完額度 → 閒置 1 秒 → 又可全額爆發
        rl = SendRateLimiter(5)
        for _ in range(5):
            rl.acquire()
        time.sleep(1.05)
        t0 = time.monotonic()
        for _ in range(5):
            rl.acquire()
        assert time.monotonic() - t0 < 0.2
