"""Spec 3.6: real session lifecycle with a local fake broker; no SDK/network."""
import json
import queue
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import trading_session as ts_mod
from fakes_cancel import fill, make_session, run_once, wait_until
from test_pull_signal import BASE, SYM, arm, book, make_trader, poll, trade


def sells(s):
    return [c for c in s.broker.placed if c[0] in ("limit_sell", "market_sell")]


def session(lots=3):
    s = make_session(total=100_000_000, per_symbol=0, sizing_mode="fixed_lots", fixed_lots=lots)
    s.set_limit_downs({SYM: 90})
    s.place_pre_orders([SYM], {SYM: 100})
    return s, s.trades[SYM].pre_order_no


def trigger(t, *, q=60):
    arm(t)
    book(t, 6, q)
    poll(t, 7)


@pytest.fixture
def exit_workers(monkeypatch):
    """Wait for the entire worker, including its final exited rollback."""
    def watch(s):
        completed = queue.Queue()
        original = s._exit_worker

        def worker(*args):
            try:
                original(*args)
            finally:
                completed.put(threading.current_thread())

        monkeypatch.setattr(s, "_exit_worker", worker)

        def join_next():
            thread = completed.get(timeout=3)
            thread.join(3)
            assert not thread.is_alive()

        return join_next
    return watch


@pytest.fixture
def exit_window(monkeypatch):
    """Use events, not sleep guesses, to put callbacks inside/outside the window."""
    entered, release = threading.Event(), threading.Event()
    original_sleep = time.sleep
    monkeypatch.setattr(ts_mod, "_EXIT_FILL_WAIT_SEC", 12345)
    def controlled_sleep(seconds):
        if seconds == 12345:
            entered.set()
            assert release.wait(5), "test did not release exit window"
        else:
            original_sleep(seconds)
    monkeypatch.setattr(time, "sleep", controlled_sleep)
    yield entered, release
    release.set()


def test_trigger_stops_inflight_chase_then_accepted_order_is_cancelled(exit_window, monkeypatch):
    s, p = session(1)
    entered, release = exit_window
    send_entered, send_release = threading.Event(), threading.Event()
    original_buy = s.broker.place_market_buy
    def delayed_buy(*args):
        send_entered.set()
        assert send_release.wait(5)
        return original_buy(*args)
    monkeypatch.setattr(s.broker, "place_market_buy", delayed_buy)
    result = []
    sender = threading.Thread(target=lambda: result.append(s._chase_send_one(SYM, 1)))
    sender.start()
    try:
        assert send_entered.wait(2)
        t, _, _, _ = make_trader("on", session=s)
        trigger(t)
        assert s.trades[SYM].stopped_reason == "mkt_queue_pulled"
        assert s._chase_send_one(SYM, 1) == "stopped"
        assert entered.wait(2)
        send_release.set()
        sender.join(3)
        assert result == ["aborted"]
        assert wait_until(lambda: any(no != p for no, _, _ in s.broker.cancelled))
        assert p in {no for no, _, _ in s.broker.cancelled}
        m = next(no for no, row in s.order_log.items() if row["kind"] == "market_buy")
        # Fill was already executed at broker before cancellation, report arrives now.
        s._on_fill(fill(m, SYM, 1, 100, filled_no="INFLIGHT"))
        release.set()
        assert wait_until(lambda: len(sells(s)) == 1)
        assert sells(s) == [("limit_sell", SYM, 90, 1)]
    finally:
        send_release.set()
        release.set()
        sender.join(3)


def test_fills_inside_window_aggregate_once_then_late_delta_only(exit_window):
    s, p = session(3)
    entered, release = exit_window
    t, _, _, _ = make_trader("on", session=s)
    trigger(t)
    assert entered.wait(2)
    s._on_fill(fill(p, SYM, 1, 100, filled_no="A"))
    s._on_fill(fill(p, SYM, 1, 100, filled_no="B"))
    assert sells(s) == []
    release.set()
    assert wait_until(lambda: len(sells(s)) == 1)
    assert sells(s) == [("limit_sell", SYM, 90, 2)]
    last = fill(p, SYM, 1, 100, filled_no="C")
    s._on_fill(last)
    assert wait_until(lambda: len(sells(s)) == 2)
    s._on_fill(last)
    assert sells(s) == [("limit_sell", SYM, 90, 2), ("limit_sell", SYM, 90, 1)]
    assert sum(c[3] for c in sells(s)) == s.trades[SYM].filled_lots == 3


def test_empty_window_then_late_fill_still_has_exit_intent(exit_window, exit_workers):
    s, p = session(2)
    join_exit = exit_workers(s)
    entered, release = exit_window
    t, _, _, _ = make_trader("on", session=s)
    trigger(t)
    assert entered.wait(2)
    release.set()
    join_exit()
    assert not s.trades[SYM].exited and sells(s) == []
    s._on_fill(fill(p, SYM, 1, 100, filled_no="VERY-LATE"))
    join_exit()
    assert sells(s) == [("limit_sell", SYM, 90, 1)]


@pytest.mark.parametrize("fill_order", ["before_intent", "after_snapshot"])
def test_fill_at_pull_intent_boundary_is_exited_immediately(fill_order, monkeypatch, exit_workers):
    s, p = session(1)
    st = s.trades[SYM]
    # No visible exposure, but an already executed buy report may still arrive.
    st.order_no = st.pre_order_no = ""
    st.order_status = "done"
    s.order_log[p]["status"] = "cancelled"
    join_exit = exit_workers(s)
    t, _, h, _ = make_trader("on", session=s)
    if fill_order == "before_intent":
        s._on_fill(fill(p, SYM, 1, 100))
        trigger(t)
    else:
        snapshot_read, release_snapshot, fill_started = (threading.Event() for _ in range(3))
        original = s.has_exposure

        def snapshot(symbol):
            result = original(symbol)
            # The lock/flags must already protect the gap after this read.
            assert s._lock._is_owned()
            assert st.stopped_reason == st.exit_on_fill == "mkt_queue_pulled"
            snapshot_read.set()
            assert release_snapshot.wait(3)
            return result

        monkeypatch.setattr(s, "has_exposure", snapshot)

        def deliver_fill():
            fill_started.set()
            s._on_fill(fill(p, SYM, 1, 100))

        evaluator = threading.Thread(target=lambda: trigger(t))
        callback = threading.Thread(target=deliver_fill)
        evaluator.start()
        try:
            assert snapshot_read.wait(3)
            callback.start()
            assert fill_started.wait(3)
        finally:
            release_snapshot.set()
            evaluator.join(3)
            if callback.ident is not None:
                callback.join(3)
        assert not evaluator.is_alive() and not callback.is_alive()
        assert h.pull_signal["has_exposure"] is False
    # No later quote/poll/retry is sent: the transaction or fill must sell now.
    join_exit()
    assert st.stopped_reason == st.exit_on_fill == "mkt_queue_pulled"
    assert sells(s) == [("limit_sell", SYM, 90, 1)]


def test_pull_and_zero_same_tick_only_one_worker_sells(monkeypatch):
    s, p = session(2)
    s._on_fill(fill(p, SYM, 2, 100))
    t, _, h, _ = make_trader("on", session=s)
    arm(t)
    book(t, 6, 60)  # pending lag; next callback also observes queue zero
    # Force both exit_position calls to enqueue before either worker can run.
    workers = []
    original_thread = threading.Thread
    class DeferredExit:
        def __init__(self, **kw):
            self.kw = kw
        def start(self):
            workers.append((self.kw["target"], self.kw["args"]))
    monkeypatch.setattr(ts_mod.threading, "Thread", DeferredExit)
    book(t, 7, 0)
    assert h.pull_signal is not None
    assert len(workers) == 2
    monkeypatch.setattr(ts_mod.threading, "Thread", original_thread)
    barrier = threading.Barrier(3)
    def run(fn, args):
        barrier.wait()
        fn(*args)
    threads = [threading.Thread(target=run, args=w) for w in workers]
    for th in threads:
        th.start()
    barrier.wait()
    for th in threads:
        th.join(3)
        assert not th.is_alive()
    assert sells(s) == [("limit_sell", SYM, 90, 2)]


def test_overnight_is_never_included_in_today_pull_sale():
    s, p = session(2)
    s._on_fill(fill(p, SYM, 2, 100))
    s.load_overnight([{"symbol": SYM, "lots": 9, "avg_price": 95}])
    s.set_overnight_limit_ups({SYM: 100})
    t, _, _, _ = make_trader("on", session=s)
    trigger(t)
    assert wait_until(lambda: len(sells(s)) == 1)
    assert sells(s) == [("limit_sell", SYM, 90, 2)]
    assert s.overnight[SYM]["sold_lots"] == 0


@pytest.mark.parametrize("overnight_fill_order", ["inside_window", "after_window"])
def test_overnight_fill_does_not_consume_today_position(overnight_fill_order, exit_window, exit_workers):
    s, p = session(2)
    s._on_fill(fill(p, SYM, 2, 100))
    s.load_overnight([{"symbol": SYM, "lots": 9, "avg_price": 95}])
    s.set_overnight_limit_ups({SYM: 100})
    s.broker.inventories = [{"symbol": SYM, "lots": 11}]
    # Run the actual overnight worker synchronously to get its owned order no.
    s._overnight_sell_worker(SYM)
    overnight_no = s.overnight[SYM]["sell_order_no"]
    assert overnight_no and sells(s) == [("limit_sell", SYM, 90, 9)]
    # Force the existing recent-cancel window, without leaving any buy unfilled.
    s.trades[SYM].last_buy_cancel_ts = time.time()
    join_exit = exit_workers(s)
    entered, release = exit_window
    t, _, _, _ = make_trader("on", session=s)
    trigger(t)
    assert entered.wait(3)
    overnight_fill = fill(overnight_no, SYM, 9, 90, action="sell", filled_no="OVERNIGHT")
    if overnight_fill_order == "inside_window":
        s._on_fill(overnight_fill)
        assert s.trades[SYM].filled_lots == 2
    release.set()
    join_exit()
    assert sells(s) == [("limit_sell", SYM, 90, 9), ("limit_sell", SYM, 90, 2)]
    if overnight_fill_order == "after_window":
        s._on_fill(overnight_fill)
        assert s.trades[SYM].filled_lots == 2
    today_no = next(no for no, row in s.order_log.items() if row["kind"] == "limit_sell")
    s._on_fill(fill(today_no, SYM, 2, 90, action="sell", filled_no="TODAY"))
    s._on_fill(overnight_fill)  # duplicate reports still cannot touch today's book
    assert s.overnight[SYM]["sold_lots"] == 9
    assert s.trades[SYM].filled_lots == 0
    assert sum(c[3] for c in sells(s)) == 11
    assert sum(row["filled_lots"] for row in s.order_log.values() if row["action"] == "sell") == 11


def test_terminal_partial_auth_final_then_duplicate_fill_does_not_oversell(exit_window):
    s, p = session(3)
    entered, release = exit_window
    s.broker.snapshot_filled[p] = 1000
    s.broker.snapshot_status[p] = "40"  # cancelled with one lot filled
    s.request_cancel(p, SYM, "routine")
    run_once(s)
    assert s.trades[SYM].filled_lots == 1
    assert s.order_log[p]["auth_final"] == 1
    t, _, _, _ = make_trader("on", session=s)
    # Local cancellation is timestamped by actual wall clock (outside synthetic T).
    trigger(t)
    assert entered.wait(2)
    s._on_fill(fill(p, SYM, 1, 100, filled_no="ALREADY-IN-AUTH"))
    assert s.trades[SYM].filled_lots == 1
    release.set()
    assert wait_until(lambda: len(sells(s)) == 1)
    s._on_fill(fill(p, SYM, 1, 100, filled_no="ANOTHER-LATE-CALLBACK"))
    assert s.trades[SYM].filled_lots == 1
    assert sells(s) == [("limit_sell", SYM, 90, 1)]


def test_missing_cancel_stays_queued_after_pull_exit(exit_window):
    s, p = session(2)
    entered, release = exit_window
    s._on_fill(fill(p, SYM, 1, 100))
    s.broker.snapshot_missing.add(p)
    t, _, _, _ = make_trader("on", session=s)
    trigger(t)
    assert entered.wait(2)
    assert s.order_log[p]["status"] == "pending" and p in s._cancel_queue
    release.set()
    assert wait_until(lambda: len(sells(s)) == 1)
    assert sells(s) == [("limit_sell", SYM, 90, 1)]
    s.broker.snapshot_missing.clear()
    run_once(s, now=time.time() + 10)
    assert s.order_log[p]["status"] == "cancelled" and p not in s._cancel_queue


def test_failed_exit_keeps_intent_and_retries_existing_worker(monkeypatch):
    s, p = session(1)
    s._on_fill(fill(p, SYM, 1, 100))
    # API only enqueues while disconnected. Reconnect restores the existing exit.
    broker = s.broker
    s.broker = None
    t, _, h, _ = make_trader("on", session=s)
    trigger(t)
    assert h.pull_signal and s.trades[SYM].exit_on_fill == "mkt_queue_pulled"
    assert not [c for c in broker.placed if c[0] == "limit_sell"]
    s.broker = broker
    book(t, 11, 60)
    poll(t, 12)
    assert wait_until(lambda: len(sells(s)) == 1)
    assert sells(s) == [("limit_sell", SYM, 90, 1)]


def test_book_exchange_time_and_reconnect_epoch_reach_trader(monkeypatch):
    from subscriber import Subscriber
    sub = Subscriber(None, [SYM], lambda *a: None, login_cfg=None)
    sub._sockets = [object()]
    sub._init_socket_health()
    sub._sock_state[0]["up"] = True
    t, _, h, _ = make_trader()
    t.subscriber = sub
    sub.set_handlers(on_book=t.on_book, on_trade=t.on_trade, with_context=True)
    handler = sub._make_msg_handler(0)
    wall = [0.0]
    monkeypatch.setattr("subscriber.time.time", lambda: BASE + wall[0])
    monkeypatch.setattr("subscriber.time.monotonic", lambda: wall[0])
    def emit(sec, q):
        wall[0] = sec
        handler({"event": "data", "channel": "books", "data": {
            "symbol": SYM, "time": int((BASE + sec) * 1e6),
            "bids": [{"price": 0, "size": q}], "asks": []}})
    for sec in (0, 2, 4, 5):
        emit(sec, 100)
        poll(t, sec + 1)
    assert h.pull_stream.detector.last_book_ts == BASE + 5
    sub._sock_state[0]["epoch"] += 1
    emit(6, 30)  # subscriber clears stale before dispatch; epoch still forces reset
    poll(t, 7)
    assert not sub.is_stale(SYM)
    assert h.pull_signal is None and h.pull_stream.detector.armed_at == BASE + 6


def test_legacy_subscriber_callback_signature_is_unchanged():
    from subscriber import Subscriber
    called = []
    sub = Subscriber(None, [SYM], lambda s, b, a, c: called.append((s, c)), login_cfg=None)
    sub._make_msg_handler(0)({"event": "data", "channel": "books", "data": {
        "symbol": SYM, "time": int(BASE * 1e6), "isContinuous": True}})
    assert called == [(SYM, True)]


def test_message_keeps_origin_epoch_if_connection_changes_during_dispatch(monkeypatch):
    from subscriber import Subscriber
    sub = Subscriber(None, [SYM], lambda *a: None, login_cfg=None)
    sub._sockets = [object()]
    sub._init_socket_health()
    sub._sock_state[0]["up"] = True
    t, _, h, _ = make_trader()
    t.subscriber = sub
    observed = []
    def handoff(*args, **kwargs):
        # Old callback was already running when a new connection became ready.
        sub._sock_state[0]["epoch"] += 1
        observed.append(kwargs["feed_epoch"])
        t.on_book(*args, **kwargs)
    sub.set_handlers(on_book=handoff, with_context=True)
    monkeypatch.setattr("subscriber.time.time", lambda: BASE)
    monkeypatch.setattr("subscriber.time.monotonic", lambda: 0)
    sub._make_msg_handler(0)({"event": "data", "channel": "books", "data": {
        "symbol": SYM, "time": int(BASE * 1e6), "bids": [{"price": 0, "size": 100}]}})
    assert observed == [(0, 0, 0)]
    poll(t, 1)
    assert h.pull_signal is None and h.pull_stream.detector.armed_at is None


class TestPullApi:
    @pytest.fixture
    def client(self, tmp_path, monkeypatch):
        import api
        import trading_config
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from runner import Runner
        r = Runner()
        monkeypatch.setattr(api.Runner, "get", staticmethod(lambda: r))
        path = tmp_path / "trading_config.json"
        monkeypatch.setattr(trading_config, "config_path", lambda output_dir=None: path)
        app = FastAPI()
        app.include_router(api.router)
        return TestClient(app), r, path

    def test_endpoint_persists_restores_status_and_preserves_central_shape(self, client):
        import trading_config
        from trading_session import TradingSession
        c, r, path = client
        initial = c.get("/api/central/status").json()
        old_params = r.session.status()["params"]
        for mode in ("shadow", "on", "off"):
            resp = c.post("/api/trading/pull_exit", json={"mode": mode, "grace_sec": 8, "fleet_nodes": 6})
            assert resp.status_code == 200 and resp.json()["persisted"]
            data = json.loads(path.read_text(encoding="utf-8"))
            assert data["pull_exit"]["mode"] == mode
            assert "armed" not in data and "pull_exit" not in data.get("params", {})
            fresh = TradingSession(auto_cancel_worker=False)
            trading_config.restore(fresh)
            assert fresh.pull_exit.to_dict() == r.session.pull_exit.to_dict()
            assert fresh.armed is False
            assert c.get("/api/trading/status").json()["pull_exit"]["mode"] == mode
            assert fresh.status()["params"] == old_params
        after = c.get("/api/central/status").json()
        assert set(after) == set(initial)
        assert set(after["trading"]) == set(initial["trading"])
        assert "pull_exit" not in after["trading"]

    @pytest.mark.parametrize("bad", [{}, {"mode": "bad"}, {"mode": "on", "grace_sec": -1},
        {"mode": "on", "grace_sec": True}, {"mode": "on", "grace_sec": "5"},
        {"mode": "on", "fleet_nodes": 0}, {"mode": "on", "fleet_nodes": 1.5},
        {"mode": "on", "eval_lag_sec": 0}, {"mode": "on", "max_gap_sec": -5},
        {"mode": "on", "cutoff": "13:23:00"}, {"mode": "on", "surprise": 1}])
    def test_invalid_is_atomic_and_does_not_persist(self, client, bad):
        c, r, path = client
        before = r.session.pull_exit
        assert c.post("/api/trading/pull_exit", json=bad).status_code == 400
        assert r.session.pull_exit == before and not path.exists()

    def test_old_or_invalid_config_keeps_default_off(self, client):
        import trading_config
        from trading_session import TradingSession
        _, _, path = client
        for data in ({"mode": "real"}, {"mode": "real", "pull_exit": {"mode": "on", "grace_sec": float("nan")}}):
            path.write_text(json.dumps(data), encoding="utf-8")
            s = TradingSession(auto_cancel_worker=False)
            trading_config.restore(s)
            assert s.mode == "real" and s.pull_exit.mode == "off" and not s.armed

    def test_existing_token_middleware_protects_new_endpoint(self, client, monkeypatch):
        from test_central_token import server
        from fastapi.testclient import TestClient
        c = TestClient(server.app)  # no lifespan, no runner/network
        monkeypatch.setenv("LIMITUP_API_TOKEN", "local-test-token")
        assert c.post("/api/trading/pull_exit", json={"mode": "on"}).status_code == 401
        assert c.post("/api/trading/pull_exit", json={"mode": "shadow"},
                      headers={"X-Limitup-Token": "local-test-token"}).status_code == 200
