"""Pull rule, receipt lag, clock/connection boundaries and audit hot path."""
import ast
from bisect import bisect_right
import csv
from datetime import datetime
import logging
import os
from pathlib import Path
import random
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from pull_runtime import PullSignalWriter, TAIPEI, exchange_time
from pull_signal import CancelWindow, PullDetector, PullExitConfig
from trader import Holding, Trader
from trading_session import SymbolTrade, TradingSession

SYM = "5386"
BASE = datetime(2026, 10, 26, 9, 1, tzinfo=TAIPEI).timestamp()


class Audit:
    def __init__(self):
        self.items = []

    def submit(self, message, row=None):
        self.items.append((message, row))

    def close(self):
        pass


def make_trader(mode="shadow", *, target=1, filled=1, session=None, **cfg):
    s = session or TradingSession(auto_cancel_worker=False)
    s.set_pull_exit(mode=mode, **cfg)
    if session is None:
        st = SymbolTrade(SYM, 100)
        st.target_lots, st.filled_lots = target, filled
        s.trades[SYM] = st
        s.exit_position = Mock()
    audit = Audit()
    t = Trader([SYM], {SYM: 100}, SimpleNamespace(bid_decline_sample_sec=60, bid_decline_minutes=5),
               session=s, pull_writer=audit)
    h = t.holdings[SYM]
    h.first_books_seen = h.first_trade_seen = True
    h.status = Holding.TRACKING
    h._pull_open_day = "2026-10-26"
    return t, s, h, audit


def book(t, sec, q, *, received=None, raw=True, epoch=None):
    received = sec if received is None else received
    bids = [{"price": 0, "size": q}, {"price": 100, "size": 1000}]
    if q == 0:
        bids = bids[1:]
    t.on_book(SYM, bids, [], timestamp=int((BASE + sec) * 1e6) if raw else None,
              received_at=BASE + received, received_mono=received, feed_epoch=epoch)


def trade(t, sec, size, *, received=None, raw=True, **data):
    received = sec if received is None else received
    tick = {"price": 100, "size": size, **data}
    if raw:
        tick["time"] = int((BASE + sec) * 1e6)
    t.on_trade(SYM, tick, received_at=BASE + received, received_mono=received)


def poll(t, sec):
    t.poll_pull_signals(now_mono=sec, now_wall=BASE + sec)


def arm(t):
    for sec in (0, 2, 4, 5):
        book(t, sec, 100)
        poll(t, sec + 1)


class TestDetector:
    def test_pull_without_trades_and_only_once(self):
        d = PullDetector()
        for ts in (0, 2, 4, 5):
            assert d.on_book(ts, 100) is None
        sig = d.on_book(6, 70)
        assert sig == {"ts": 6, "q_before": 70, "cancels": 30, "trades": 0,
                       "window_sec": 10, "armed_s": 6, "raw_cancels": 30,
                       "own_cancels": 0, "fleet_lots": 0}
        assert d.on_book(7, 20) is None
        d.reset()
        assert not d.fired and d.armed_at is None

    @pytest.mark.parametrize("volume", [30, 15])
    def test_queue_eaten_or_trade_ratio_blocks(self, volume):
        d = PullDetector(grace_sec=0)
        d.on_book(0, 100)
        d.on_trade(1, volume)
        assert d.on_book(1, 60) is None

    def test_slow_drain_expires_and_small_quantities_block(self):
        d = PullDetector(grace_sec=0)
        assert all(d.on_book(i, 500 - i) is None for i in range(100))
        for before, after in ((19, 0), (100, 81)):
            d = PullDetector(grace_sec=0)
            d.on_book(0, before)
            assert d.on_book(1, after) is None

    def test_requeues_do_not_erase_per_step_cancels(self):
        d = PullDetector(grace_sec=0)
        d.on_book(0, 100)
        assert d.on_book(1, 80) is None
        assert d.on_book(2, 110) is None
        assert d.on_book(3, 90)["cancels"] == 40

    def test_grace_masks_steps_but_includes_exact_boundary(self):
        d = PullDetector()
        d.on_book(0, 0)
        d.on_book(1, 1000)
        assert d.on_book(4, 100) is None
        assert d.on_book(5, 100) is None
        sig = d.on_book(6, 70)
        assert sig["cancels"] == 30 and sig["armed_s"] == 5

    def test_q_before_is_last_reading_at_or_before_window_start(self):
        d = PullDetector(grace_sec=0)
        d.on_book(0, 1000)
        d.on_book(1, 1100)
        d.on_book(5, 1200)
        # 30% of current 900 would pass, 30% of q_before 1100 must not.
        assert d.on_book(11, 900) is None
        sig = d.on_book(12, 850)
        assert sig["q_before"] == 1100 and sig["cancels"] == 350

    def test_future_prints_not_used_and_cumulative_volume_preferred(self):
        d = PullDetector(grace_sec=0)
        d.on_trade(0, 1, 100)
        d.on_book(0, 100)
        d.on_trade(1, 1, 140)  # missing prints: cumulative delta=40
        d.on_trade(10, 50, 190)
        assert d.on_book(1, 60) is None
        assert d._prev == (60, 140)
        sig = d.on_book(2, 20)
        assert sig is None  # cancel 40 < 2*40 trades

    def test_trade_at_sealed_book_timestamp_is_late(self):
        d = PullDetector()
        d.on_book(5, 100)
        assert d.on_trade(5, 50) == "late"
        assert d.late_trades == 1 and not d._trades

    def test_reference_randomized_window_and_grace(self):
        rng = random.Random(261026)
        for _ in range(100):
            times, qs, vols, cs, vs = [], [], [], [], []
            d = PullDetector()
            q, v, ts = 500, 0, 0
            expected = actual = None
            for i in range(120):
                ts += rng.choice((0.25, 1, 3))
                v += rng.randint(0, 12)
                q = max(1, q + rng.randint(-50, 30))
                times.append(ts)
                qs.append(q)
                vols.append(v)
                ready = ts >= times[0] + 5
                dv = max(0, v - vols[-2]) if i and ready else 0
                cc = max(0, qs[-2] - q - dv) if i and ready else 0
                cs.append(cc)
                vs.append(dv)
                j = bisect_right(times, ts - 10)
                qb = qs[j - 1] if j else q
                c, tv = sum(cs[j:]), sum(vs[j:])
                if expected is None and ready and qb >= 20 and c >= max(20, 0.3 * qb, 2 * tv):
                    expected = i
                d.on_trade(ts, 0, v)
                sig = d.on_book(ts, q)
                if sig is not None and actual is None:
                    actual = i
                assert actual == expected


class TestLagAndClock:
    def test_received_trade_blocked_on_state_lock_is_counted_before_deadline(self, monkeypatch):
        from state import State
        t, s, h, _ = make_trader("on")
        arm(t)
        book(t, 6, 60)
        t.state = State()
        entered = threading.Event()
        original = t.state.mark_opened

        def mark_opened(symbol):
            entered.set()
            original(symbol)

        monkeypatch.setattr(t.state, "mark_opened", mark_opened)
        callback = threading.Thread(target=lambda: trade(t, 6, 40, received=6.8))
        with t.state._lock:
            callback.start()
            assert entered.wait(3)
            # The callback cannot return, but its print must already be queued.
            poll(t, 7)
            assert h.pull_signal is None
            assert h.pull_stream.detector._prev == (60, 40)
            assert h.pull_stream.late_count == 0
            s.exit_position.assert_not_called()
        callback.join(3)
        assert not callback.is_alive()
        assert h.pull_stream.late_count == 0

    def test_book_trade_and_timer_keep_receipt_order_while_consumer_is_blocked(self, monkeypatch):
        t, s, h, _ = make_trader("on")
        arm(t)
        registered = {kind: threading.Event() for kind in ("book", "trade", "poll")}
        original = t._pull_events.put

        def register(kind, *args, **kwargs):
            original(kind, *args, **kwargs)
            registered[kind].set()

        monkeypatch.setattr(t._pull_events, "put", register)
        callbacks = [
            threading.Thread(target=lambda: book(t, 6, 60)),
            threading.Thread(target=lambda: trade(t, 6, 40, received=6.8)),
            threading.Thread(target=lambda: poll(t, 7)),
        ]
        try:
            # No consumer may run until all three receipts are registered.
            with t._pull_lock:
                for callback, kind in zip(callbacks, registered):
                    callback.start()
                    assert registered[kind].wait(3)
        finally:
            for callback in callbacks:
                if callback.ident is not None:
                    callback.join(3)
        assert all(not callback.is_alive() for callback in callbacks)
        assert h.pull_stream.detector._prev == (60, 40)
        assert h.pull_stream.late_count == 0 and h.pull_signal is None
        s.exit_position.assert_not_called()

    def test_handoff_seed_does_not_count_an_already_received_print_twice(self):
        t, _, h, _ = make_trader()
        arm(t)
        trade(t, 6, 40, received=6.1)
        trade(t, 6, 40, received=6.2, _pull_seed=True)
        book(t, 6, 60, received=6.3)
        poll(t, 7.4)
        assert h.pull_signal is None
        assert h.pull_stream.detector._prev == (60, 40)

    def test_trade_after_book_within_lag_prevents_false_pull(self):
        t, s, h, _ = make_trader("on")
        arm(t)
        book(t, 6, 60)
        poll(t, 6.5)
        assert h.pull_signal is None
        trade(t, 6, 40, received=6.8)
        poll(t, 7)
        assert h.pull_signal is None
        s.exit_position.assert_not_called()

    def test_trade_after_lag_warns_counts_and_never_revises(self):
        t, _, h, audit = make_trader()
        arm(t)
        book(t, 6, 90)  # no signal, so we can observe late accounting
        poll(t, 7)
        trade(t, 6, 10, received=7.1)
        assert h.pull_stream.late_count == 1
        assert "count=1" in audit.items[-1][0]
        assert h.pull_stream.detector._prev == (90, 0)

    def test_late_print_after_fired_signal_still_warns_without_retraction(self):
        t, _, h, audit = make_trader()
        arm(t)
        book(t, 6, 60)
        poll(t, 7)
        signal = dict(h.pull_signal)
        trade(t, 6, 40, received=7.1)
        assert h.pull_signal == signal
        assert h.pull_stream.late_count == 1 and "不追溯" in audit.items[-1][0]

    def test_elapsed_lag_is_sealed_before_a_delayed_callback(self):
        t, _, h, _ = make_trader()
        arm(t)
        book(t, 6, 90)
        trade(t, 6, 10, received=7.2)  # no poll between; deadline still applies
        assert h.pull_stream.late_count == 1

    def test_future_trade_is_not_subtracted_from_earlier_book(self):
        t, _, h, _ = make_trader()
        arm(t)
        book(t, 6, 60)
        trade(t, 7, 40, received=6.8)
        poll(t, 7)
        assert h.pull_signal["cancels"] == 40
        assert h.pull_signal["trades"] == 0

    def test_last_book_flushes_without_next_tick(self):
        t, _, h, _ = make_trader()
        arm(t)
        book(t, 6, 60)
        assert h.pull_signal is None
        poll(t, 7)
        assert h.pull_signal is not None

    def test_missing_timestamp_resets_and_sticks_to_receipt_clock(self):
        t, _, h, _ = make_trader()
        arm(t)
        book(t, 6, 60, received=8, raw=False)
        poll(t, 9)
        assert h.pull_signal is None
        assert h.pull_stream.clock_source == "receipt"
        book(t, 7, 55, received=10)
        poll(t, 11)
        assert h.pull_stream.detector.last_book_ts == BASE + 10

    def test_unordered_trades_reset_without_sorting_or_false_pull(self):
        t, _, h, audit = make_trader()
        arm(t)
        book(t, 6, 60)
        trade(t, 6.5, 1, received=6.1)
        trade(t, 6, 40, received=6.2)
        poll(t, 7)
        assert h.pull_signal is None and not h.pull_stream.pending
        assert "時間倒退" in audit.items[-1][0]

    @pytest.mark.parametrize("gap_kind", ["receive", "exchange"])
    def test_gap_resets_before_draining_pending_books(self, gap_kind):
        t, _, h, _ = make_trader()
        arm(t)
        book(t, 6, 60)
        book(t, 12 if gap_kind == "exchange" else 7, 20,
             received=7 if gap_kind == "exchange" else 12)
        poll(t, 8 if gap_kind == "exchange" else 13)
        assert h.pull_signal is None
        assert h.pull_stream.detector.armed_at == BASE + (12 if gap_kind == "exchange" else 7)

    def test_stale_and_fast_reconnect_rearm_even_first_fresh_book(self):
        t, _, h, _ = make_trader()
        feed = SimpleNamespace(stale=False, epoch=(0, 0, 0))
        t.subscriber = SimpleNamespace(is_stale=lambda _: feed.stale, feed_epoch=lambda _: feed.epoch)
        arm(t)
        book(t, 6, 60)
        feed.stale = True
        poll(t, 7)
        assert h.pull_signal is None and h.pull_stream.detector.armed_at is None
        feed.stale = False
        feed.epoch = (0, 0, 1)
        book(t, 8, 100)
        poll(t, 9)
        for sec in (10, 12):
            book(t, sec, 100)
            poll(t, sec + 1)
        book(t, 13, 60)
        poll(t, 14)
        assert h.pull_signal["armed_s"] == 5
        # New instance: a very quick reconnect never observed as stale.
        t, _, h, _ = make_trader()
        t.subscriber = SimpleNamespace(is_stale=lambda _: False, feed_epoch=lambda _: feed.epoch)
        arm(t)
        feed.epoch = (0, 0, 2)
        book(t, 6, 20)
        poll(t, 7)
        assert h.pull_signal is None and h.pull_stream.detector.armed_at == BASE + 6


class TestGatesAndAudit:
    def test_off_never_builds_detector_or_changes_entry(self):
        t, s, h, audit = make_trader("off")
        arm(t)
        book(t, 6, 60)
        poll(t, 7)
        assert h.pull_stream is None and not audit.items
        assert s.trades[SYM].stopped_reason == ""
        s.exit_position.assert_not_called()

    def test_shadow_only_audit_on_exposure_calls_exit_once_after_stop(self):
        for mode in ("shadow", "on"):
            t, s, h, audit = make_trader(mode)
            def exit_check(*args):
                assert s.trades[SYM].stopped_reason == "mkt_queue_pulled"
                assert s.trades[SYM].exit_on_fill == "mkt_queue_pulled"
            s.exit_position.side_effect = exit_check
            arm(t)
            book(t, 6, 60)
            poll(t, 7)
            book(t, 7, 60)
            poll(t, 8)
            rows = [r for _, r in audit.items if r]
            assert len(rows) == 1 and rows[0]["mode"] == mode
            if mode == "shadow":
                s.exit_position.assert_not_called()
                assert s.trades[SYM].stopped_reason == "" and s.trades[SYM].exit_on_fill == ""
            else:
                # Retry is for incomplete exits only, not a second detector fire.
                assert s.exit_position.call_args.args == (SYM, "mkt_queue_pulled")
            assert t.summary()["tracking"][0]["pull_signal"] == h.pull_signal
            assert s.get_symbol_state(SYM)["pull_signal"] == h.pull_signal

    def test_on_without_exposure_does_not_exit(self):
        t, s, h, audit = make_trader("on", filled=0)
        arm(t)
        book(t, 6, 60)
        poll(t, 7)
        s.exit_position.assert_not_called()
        assert h.pull_signal["action"] == "no_exposure"

    @pytest.mark.parametrize("kind", ["disposition", "discarded", "abandoned", "overnight"])
    def test_excluded_targets(self, kind):
        t, _, h, audit = make_trader()
        if kind == "disposition":
            t.dispositions[SYM] = True
        elif kind == "discarded":
            h.status = Holding.DISCARDED_FIRST
        elif kind == "abandoned":
            h.status = Holding.ABANDONED
        else:
            t.session.trades.clear()
            t.session.overnight[SYM] = {"symbol": SYM, "lots": 1}
            t.session.update_overnight_book = Mock()
        arm(t)
        book(t, 6, 60)
        poll(t, 7)
        assert h.pull_stream is None and not audit.items

    def test_fleet_floor_and_exact_threshold(self):
        t, _, h, _ = make_trader(target=20)
        arm(t)
        book(t, 6, 50)  # rule passes but 50 < fleet 60
        poll(t, 7)
        assert h.pull_signal is None
        book(t, 7, 40)
        poll(t, 8)
        assert h.pull_signal["cancels"] == h.pull_signal["fleet_lots"] == 60

    @pytest.mark.parametrize("offset", [-61, 15540, 15720, 15780])
    def test_only_open_trade_to_cutoff(self, offset):
        # BASE is 09:01; offsets cover 08:59:59, 13:20, 13:23, 13:24.
        t, _, h, audit = make_trader(grace_sec=0)
        book(t, offset, 100)
        book(t, offset + 0.1, 60)
        poll(t, offset + 1.2)
        assert h.pull_signal is None and not audit.items

    def test_no_first_real_trade_and_trial_do_not_arm(self):
        t, _, h, _ = make_trader()
        h._pull_open_day = None
        trade(t, 0, 1, isTrial=True)
        arm(t)
        assert h.pull_stream.detector.armed_at is None
        trade(t, 6, 1)
        book(t, 6, 100)
        poll(t, 7)
        assert h.pull_stream.detector.armed_at == BASE + 6

    def test_cutoff_discards_queued_reading_even_if_exchange_before_cutoff(self):
        t, _, h, _ = make_trader(grace_sec=0)
        book(t, 15538, 100)
        poll(t, 15539)
        book(t, 15539.5, 60)
        poll(t, 15540.5)
        assert h.pull_signal is None

    def test_disable_clears_pending_and_reenable_has_fresh_grace(self):
        t, s, h, _ = make_trader()
        arm(t)
        book(t, 6, 60)
        t.configure_pull_exit(mode="off")
        poll(t, 7)
        assert h.pull_signal is None
        t.configure_pull_exit(mode="on")
        book(t, 8, 40)
        poll(t, 9)
        assert h.pull_signal is None
        s.exit_position.assert_not_called()

    def test_one_signal_survives_mode_change_and_trader_restart_until_roll_day(self):
        t, s, h, _ = make_trader()
        arm(t)
        book(t, 6, 60)
        poll(t, 7)
        first = dict(h.pull_signal)
        t.configure_pull_exit(mode="on")
        arm(t)
        book(t, 6, 30)
        poll(t, 7)
        assert h.pull_signal == first
        again, _, h2, audit = make_trader("on", session=s)
        arm(again)
        book(again, 6, 30)
        poll(again, 7)
        assert h2.pull_signal == first and not audit.items
        s.roll_day("2026-10-27")
        assert s.get_pull_signal(SYM) is None and not s._pull_cancels

    def test_writer_file_io_runs_on_background_thread(self, tmp_path, monkeypatch, caplog):
        caplog.set_level(logging.WARNING)
        writer = PullSignalWriter(tmp_path)
        thread_names = []
        original = Path.open
        def open_checked(path, *args, **kwargs):
            thread_names.append(threading.current_thread().name)
            return original(path, *args, **kwargs)
        monkeypatch.setattr(Path, "open", open_checked)
        row = dict(zip(("ts", "symbol", "mode", "q_before", "cancels", "trades", "armed_s",
                        "filled_lots", "has_exposure", "action"),
                       (BASE, SYM, "shadow", 100, 40, 0, 6, 1, True, "shadow")))
        writer.submit("[pull] test shadow", row)
        writer.submit("[pull] test shadow", row)
        writer.close()
        assert thread_names == ["pull-audit", "pull-audit"]
        assert writer.errors == 0 and "[pull] test shadow" in caplog.text
        with (tmp_path / "2026-10-26_pull_signals.csv").open(encoding="utf-8") as f:
            records = list(csv.DictReader(f))
        assert len(records) == 2 and records[0]["symbol"] == SYM

    def test_quote_path_has_no_file_or_broker_io(self, monkeypatch):
        t, s, _, _ = make_trader("on")
        monkeypatch.setattr(Path, "open", Mock(side_effect=AssertionError("quote file I/O")))
        s.broker = SimpleNamespace(cancel=Mock(side_effect=AssertionError("quote broker I/O")))
        arm(t)
        book(t, 6, 60)
        poll(t, 7)
        s.broker.cancel.assert_not_called()
        s.exit_position.assert_called_once_with(SYM, "mkt_queue_pulled")

    def test_notifier_forwards_shadow_warning(self):
        from notifier import should_forward
        assert should_forward(logging.WARNING, "[pull] 5386 大單抽掉訊號(shadow)")
        assert not should_forward(logging.INFO, "[pull] detail")


class TestOwnCancels:
    def test_boundary_future_and_duplicate_confirmation(self):
        w = CancelWindow()
        w.add(0, "M", 30)
        w.add(1, "M", 20)  # confirmation after partial fill: max active amount only
        w.add(5, "P", 10)
        assert w.at(0) == 30
        assert w.at(1) == 30
        assert w.at(5) == 40
        assert w.at(12) == 30  # first request expired, confirmation 20 + P10
        assert w.at(13) == 10
        assert w.at(17) == 0

    def test_session_records_requests_and_confirmations_once_and_buy_only(self, monkeypatch):
        s = TradingSession(auto_cancel_worker=False)
        s._log_order("M", SYM, "buy", "market_buy", 50, 0)
        s.order_log["M"]["filled_lots"] = 10
        s._log_order("S", SYM, "sell", "overnight_sell", 50, 90)
        monkeypatch.setattr("trading_session.time.time", lambda: BASE + 5.5)
        s.request_cancel("M", SYM, "routine")
        s.request_cancel("M", SYM, "retry")
        s.request_cancel("S", SYM, "manual")
        assert s.pull_inputs(SYM, BASE + 5)[0] == 0
        assert s.pull_inputs(SYM, BASE + 6)[0] == 40
        s._confirm_cancelled("M", SYM, "routine")
        s._confirm_cancelled("M", SYM, "duplicate")
        assert s.pull_inputs(SYM, BASE + 6)[0] == 40

    def test_own_chase_cancel_after_grace_masks_signal(self, monkeypatch):
        t, s, h, _ = make_trader("on")
        arm(t)
        s._log_order("M", SYM, "buy", "market_buy", 40, 0)
        monkeypatch.setattr("trading_session.time.time", lambda: BASE + 5.5)
        s.request_cancel("M", SYM, "chase_extra")
        book(t, 6, 60)
        poll(t, 7)
        assert h.pull_signal is None
        s.exit_position.assert_not_called()

    @pytest.mark.parametrize("path", ["sync", "worker"])
    def test_actual_send_refreshes_old_queued_request_without_double_count(self, monkeypatch, path):
        from fakes_cancel import make_session
        s = make_session()
        no = s.broker.place_market_buy(SYM, 3)
        s._log_order(no, SYM, "buy", "market_buy", 3, 0)
        s.order_log[no]["filled_lots"] = 1
        monkeypatch.setattr("trading_session.time.time", lambda: BASE)
        s.request_cancel(no, SYM, "routine")
        assert s.pull_inputs(SYM, BASE + 13)[0] == 0
        monkeypatch.setattr("trading_session.time.time", lambda: BASE + 20)
        if path == "sync":
            s._try_cancel_sync(no, SYM, "retry")
        else:
            entry = next(e for e in s.broker.get_order_snapshot() if e["order_no"] == no)
            s._fanout_cancel([(no, {"symbol": SYM, "reason": "retry"}, entry)], 2)
        s._confirm_cancelled(no, SYM, "routine")
        assert s.pull_inputs(SYM, BASE + 21)[0] == 2

    def test_long_stream_storage_is_bounded_by_window(self):
        d = PullDetector(min_pct=10000)
        for i in range(20000):
            d.on_trade(i / 10, 1)
            d.on_book(i / 10, 100)
        assert len(d._steps) <= 101 and not d._trades


RISK = Path(r"C:\Users\user\Desktop\finance\experiment\limitup_research\exit_signal_bt\risk_bt.py")


@pytest.mark.skipif(not RISK.exists(), reason="local research module absent")
def test_actual_risk_bt_feats_parity_without_importing_research_dependencies():
    tree = ast.parse(RISK.read_text(encoding="utf-8"))
    nodes = [n for n in tree.body if isinstance(n, (ast.ClassDef, ast.FunctionDef))
             and n.name in ("Feats", "cond_series")]
    scope = {"GRACE_US": 5_000_000, "WINDOWS": (2, 10, 60)}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(RISK), "exec"), scope)
    rng = random.Random(30)
    for _ in range(100):
        q, volume = 1000, 0
        readings = []
        d = PullDetector()
        actual = None
        for i in range(80):
            q = max(20, q + rng.randint(-100, 70))
            volume += rng.randint(0, 20)
            readings.append((i * 250000, q, volume))
            d.on_trade(i / 4, 0, volume)
            if d.on_book(i / 4, q) is not None and actual is None:
                actual = i
        feats = scope["Feats"](readings, 1000)
        cond = scope["cond_series"](feats, {"kind": "pull", "a": 30, "W": 10}, 0)
        expected = next((i for i, v in enumerate(cond) if v), None)
        assert actual == expected


BT_STORE = Path(os.environ.get("LIMITUP_PULL_BT_STORE", str(RISK.parent / "warroom_data")))
BT_DAYS = ("2026-10-14", "2026-10-15", "2026-10-16", "2026-10-19")
BT_TAPES = [d for d in BT_DAYS if (BT_STORE / "tails" / f"{d}.jsonl.gz").is_file()]


@pytest.mark.skipif(not BT_TAPES or not RISK.exists(),
                    reason="10/14-10/19 local tape absent; set LIMITUP_PULL_BT_STORE")
def test_local_tapes_against_saved_pull_30_10s_positions(monkeypatch):
    """Optional raw-rule parity, before runtime fleet/own-cancel/gap guards.

    Build the rule's per-reading condition series, then let the unchanged study
    select the first eligible run for each entry, just as positions.csv did.
    """
    import importlib.util
    monkeypatch.syspath_prepend(str(RISK.parent))
    spec = importlib.util.spec_from_file_location("pull_research_reference", RISK)
    risk = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(risk)
    except ModuleNotFoundError as exc:
        pytest.skip(f"local research dependency absent: {exc.name}")
    path = RISK.parent / "out_risk" / "positions.csv"
    with path.open(encoding="utf-8") as f:
        expected = {(r["date"], r["symbol"], r["size"]): r for r in csv.DictReader(f)
                    if r["rule"] == "pull_30_10s" and r["date"] in BT_TAPES}
    days = risk.qb.bt.load_days(str(BT_STORE))
    compared = 0
    for date in BT_TAPES:
        cands = days.get(date, [])
        tape = risk.qb.ob.read_tape(BT_STORE / "tails" / f"{date}.jsonl.gz", {c["symbol"] for c in cands})
        for size in ("A_deployed", "B_avgvol1pct"):
            for position in risk.qb.entries_for_day(cands, size):
                c = position["c"]
                saved = expected.get((date, c["symbol"], size))
                if saved is None or c["symbol"] not in tape:
                    continue
                t0 = risk.os_.iso_to_us(c["t0"])
                close = risk.os_.iso_to_us(f"{date}T13:23:00+08:00")
                ticks = tape[c["symbol"]]
                readings = risk.pb.readings(ticks, t0, close)
                d, cond = PullDetector(), []
                for ts, q, v in readings:
                    d.on_trade(ts / 1e6, 0, v)
                    sig = d.on_book(ts / 1e6, q)
                    cond.append(q < 1 or sig is not None)
                    d.fired = False  # Inspect each condition, not just its first run.
                features = risk.Feats(readings, c.get("avg_lots"))
                result = risk.price_rule(position, ticks, t0, features, cond)
                want = float(saved["trigger_s"]) if saved["trigger_s"] else None
                got = result.get("trigger_s")
                if want is None:
                    assert got is None
                else:
                    assert got is not None
                    stamps = [r[0] / 1e6 for r in readings]
                    assert abs(bisect_right(stamps, got) - bisect_right(stamps, want)) <= 1
                compared += 1
    assert compared > 0, "tapes present but no matching stored candidate/position rows"
