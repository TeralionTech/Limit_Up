"""全流程 replay — 用真實 tick JSONL 驗現行系統 (部署前 gate)。

把某交易日錄下的 books/trades raw tick (recorder.py 產的 {date}_ticks.jsonl) 依 ts 順序
餵進**現行 production handler** (filter._on_book / trader.on_book / trader.on_trade /
TradingSession),用假時鐘重現 篩選 → 08:59:58 預掛 → 09:00 盲送 → 盤中出場,產出報告。
因 handler 是正式碼,這等於用真實行情驗整條決策鏈。**只讀+餵,不改任何 production 邏輯。**

忠實度 (2026-08-28 補,為重現 08-27 開盤事故):
  - **換手延遲** `--swap-delay` (預設 1.0s): production 的 on_book 從 filter 換成 trader 要等
    09:00 等待迴圈回來 + 寫快照 (~1s);這 1 秒內 filter 仍在線、會看到開盤後的市價列。
    replay 照樣讓 filter 多活 swap-delay 秒 (0 = 立刻換手,舊行為)。
  - **集合競價拒市價**: ReplayBroker 對「送出當下該檔尚未開盤 (還沒收到首筆非試撮成交)」的
    市價單回拒單 (訊息同交易所),開盤後才接受。
  - **市價追改 step-driven** (2026-08-28 管線化後): production 是 cadence thread 不等回覆就送
    (45/s 節拍) + sender thread 跑 _chase_send_one;replay 單執行緒逐步同步呼叫 _chase_send_one
    (每 CHASE_SEND_INTERVAL 一筆),決定 accept/拒 依**送出當下**該檔是否已開盤。管線化下送單不等
    回覆、回報晚到不擋下一筆,故 replay 不再模型化「回報延遲」。
  - **unmark → 撤單**: 仿 runner._unsub_and_cancel,unmark 同步呼叫 session.cancel_symbol_orders
    (原 replay 只記退訂、不撤單 → 看不到誤撤)。
  - **開盤後 trades**: 仿 runner._monitor_on_trade (08:59:58 掛上、09:00 起生效): 首筆非試撮成交
    → state.mark_opened (若程式有) + session.on_first_trade;換手後改由 trader.on_trade,
    並仿 runner 種子化最後一筆成交。

用法:
  python scripts/replay_day.py --ticks replay_data/2026-08-21_ticks.jsonl \
      --limit-ups replay_data/2026-08-21_limit_ups.json --date 2026-08-21 \
      [--dispositions ...] [--limit-downs ...] [--fills ...] [--orders ...] \
      [--swap-delay 1.0] [--chase-cutoff 09:03:00] \
      [--total-budget 3700000] [--per-symbol 400000] [--report out.txt]

出場區塊為「假設 marked 檔依 sized 張數成交」→ 驗出場**偵測邏輯**在真實五檔上的表現
(漲停鎖死常不會成交,故此為 if-filled 邏輯檢查,非宣稱真成交)。real 成交對照用 --fills 的台帳 diff。
"""
import argparse
import csv
import inspect
import json
import sys
import time as _real_time
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import filter as filter_mod          # noqa: E402
import trader as trader_mod          # noqa: E402
import trading_session as ts_mod     # noqa: E402
import state as state_mod            # noqa: E402
from state import State              # noqa: E402
from filter import make_on_book_handler  # noqa: E402
from trader import Trader            # noqa: E402
from trading_session import TradingSession  # noqa: E402

PRE_ORDER_TIME = dtime(8, 59, 58)
OPEN_TIME = dtime(9, 0, 0)
CALL_AUCTION_REJECT = "證券集合競價時段不可輸入市價、IOC、FOK委託"


# ─── 假時鐘: 讓 production 的 datetime.now() 回「當前 tick 的 ts」 ───
class FrozenDatetime(datetime):
    _now = datetime(2000, 1, 1)

    @classmethod
    def now(cls, tz=None):
        return cls._now


def _set_clock(dt):
    FrozenDatetime._now = dt
    filter_mod.datetime = FrozenDatetime
    trader_mod.datetime = FrozenDatetime
    ts_mod.datetime = FrozenDatetime
    state_mod.datetime = FrozenDatetime    # mark/unmark 歷史 ts 用假時鐘 → 開盤後 unmark 判定才準


CHASE_SEND_INTERVAL = 0.05   # replay 模型的管線送單節奏 (production 是 45/s≈22ms;此處夠覆蓋開盤轉換)


# ─── ReplayBroker: 仿 tests FakeBroker,只記錄,不打真 API ───
class ReplayBroker:
    def __init__(self):
        self.connected = True
        self.healthy = True
        self.placed = []
        self.cancelled = []
        self.rejected = []        # (symbol, lots, send_ts) 集合競價拒市價
        self._decision = {}       # symbol → "accept"/"reject" (driver 於送出當下決定)
        self.opened = set()       # 已開盤 (無 driver 決定時的 fallback)
        self._n = 0

    def _next(self):
        self._n += 1
        return f"R{self._n}"

    def place_limit_buy(self, symbol, price, lots):
        no = self._next()
        self.placed.append(("limit_buy", symbol, price, lots))
        return no

    def place_market_buy(self, symbol, lots):
        decision = self._decision.pop(symbol, None)
        if decision is None:
            decision = "accept" if symbol in self.opened else "reject"
        if decision == "reject":
            self.rejected.append((symbol, lots))
            raise RuntimeError(f"下單被拒 {symbol}: {CALL_AUCTION_REJECT}")
        no = self._next()
        self.placed.append(("market_buy", symbol, None, lots))
        return no

    def place_market_sell(self, symbol, lots, reason=""):
        no = self._next()
        self.placed.append(("market_sell", symbol, None, lots))
        return no

    def place_limit_sell(self, symbol, price, lots, reason=""):
        no = self._next()
        self.placed.append(("limit_sell", symbol, price, lots))
        return no

    def cancel(self, order_no, symbol, reason=""):
        self.cancelled.append((order_no, symbol, reason))

    def get_inventories(self):
        return []

    def get_filled_map(self):
        return {}

    def status(self):
        return {"connected": True, "healthy": True,
                "account_masked": "replay", "is_test": True, "error": ""}


def _pick(d, *keys):
    for k in keys:
        if isinstance(d, dict) and d.get(k) is not None:
            return d[k]
    return None


def _load_json(path):
    if not path:
        return {}
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _load_ground_truth_symbols(path):
    """從 fills/orders CSV 抓有出現的 symbol 集合 (對照組)。"""
    if not path or not Path(path).exists():
        return set()
    syms = set()
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            s = (row.get("symbol") or "").strip()
            if s:
                syms.add(s)
    return syms


def _accepts_kw(fn, name):
    try:
        return name in inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return False


def _parse_hms(s, default):
    try:
        hh, mm, ss = map(int, str(s).split(":"))
        return dtime(hh, mm, ss)
    except Exception:
        return default


def run(args):
    swap_delay = float(getattr(args, "swap_delay", 1.0) or 0.0)
    cutoff = _parse_hms(getattr(args, "chase_cutoff", "09:03:00"), dtime(9, 3, 0))

    # 假時鐘關掉出場等待窗口 (免真 3s)
    ts_mod._EXIT_FILL_WAIT_SEC = 0.0

    limit_ups = {str(k): float(v) for k, v in _load_json(args.limit_ups).items() if v}
    dispositions = {str(k): bool(v) for k, v in _load_json(args.dispositions).items()}
    limit_downs = {str(k): float(v) for k, v in _load_json(args.limit_downs).items() if v}
    day_tradable = {str(k): bool(v) for k, v in _load_json(args.day_tradable).items()} \
        if args.day_tradable else {}

    # ── 建 State + Session (ReplayBroker) ──
    state = State(bid_drop_ratio=args.bid_drop_ratio)
    session = TradingSession()
    session.roll_day(args.date)
    session.set_mode("real")
    broker = ReplayBroker()
    session.broker = broker
    session.set_params(total_budget=args.total_budget, per_symbol_budget=args.per_symbol,
                       sizing_mode=args.sizing_mode, fixed_lots=args.fixed_lots)
    session.set_armed(True)
    session.order_min_interval = 0.0
    session.set_dispositions(dispositions)
    if day_tradable:
        session.set_day_tradable(day_tradable)
    session.set_limit_downs(limit_downs)

    # replay 決定性: 把非同步/背景 thread 路徑改成同步 (單執行緒重放,免 race + 免 rate-limiter 真時間節流)
    session.cancel_symbol_orders_async = session.cancel_symbol_orders
    session.exit_position = lambda symbol, reason: session._exit_worker(symbol, reason)
    if hasattr(session, "_cancel_one_order_async"):
        session._cancel_one_order_async = session._cancel_one_order   # 管線多送自撤 → 同步 (2026-08-28)
    if hasattr(session, "_rate"):
        session._rate.acquire = lambda: None

    ctx = {"phase": "filter", "trader": None, "last_bid_at_limit": {},
           "pre_marked": [], "exits": [],
           "swap_delay": swap_delay, "chase": {}, "opened_at": {}, "last_trade": {}}

    # replay 不等真時間: 所有 time.sleep 都 no-op (出場重試等)。市價追改逐步驅動 (見 chase_send)。
    def _sleep(sec):
        return None
    time_proxy = SimpleNamespace(sleep=_sleep, time=_real_time.time,
                                 monotonic=_real_time.monotonic,
                                 perf_counter=_real_time.perf_counter)
    orig_time_mod = ts_mod.time
    ts_mod.time = time_proxy

    filter_cfg = SimpleNamespace(pre_order_time="08:59:58", final_check_start="08:59:58")
    trader_cfg = SimpleNamespace(bid_decline_sample_sec=60, bid_decline_minutes=5)

    unsubbed = []

    # 仿 runner._unsub_and_cancel: unmark → 退訂 + 撤該檔委託 (reason "unmarked")
    def _unsub_and_cancel(sym):
        unsubbed.append(sym)
        try:
            session.cancel_symbol_orders(sym, "unmarked")
        except Exception:
            pass
    unsub_ref = {"fn": _unsub_and_cancel}
    filter_on_book = make_on_book_handler(state, limit_ups, filter_cfg, unsub_ref)
    filter_takes_cont = _accepts_kw(filter_on_book, "is_continuous")
    mark_opened = getattr(state, "mark_opened", None)   # 舊碼沒有 → 不呼叫 (忠實重現舊行為)


    def do_pre_order():
        marked = state.get_marked_prioritized()
        culled = state.final_check_all()          # 量減半批次剔除
        marked = state.get_marked_prioritized()   # 定案
        ctx["pre_marked"] = list(marked)
        ctx["culled"] = culled
        # 20% 母數: 用最近一次「漲停價那檔」的委買量快照
        bid_vols = {s: ctx["last_bid_at_limit"].get(s, 0) for s in marked}
        session.place_pre_orders(marked, limit_ups, limit_up_bid_vols=bid_vols)

    def do_open(open_dt):
        """09:00: 建 Trader (production 此刻建,但 on_book 要 swap_delay 後才換手) + 排程市價追。"""
        marked = ctx["pre_marked"]
        tr = Trader(watchlist=marked, limit_ups=limit_ups, cfg=trader_cfg,
                    state=state, session=session, dispositions=dispositions)
        ctx["trader"] = tr
        for sym in marked:
            st = session.trades.get(sym)
            if st is None or st.stopped_reason or not st.order_no:
                continue
            ctx["chase"][sym] = {"next_at": open_dt, "sends": 0, "rejects": 0,
                                 "done": None, "accepted_at": None}

    def _fake_fill(sym):
        # 假設成交: 依 target 建部位 (label: if-filled 出場邏輯檢查) — 對 st.order_no 那張 (M 或留守的 P)
        st = session.trades.get(sym)
        if st and st.order_no and st.target_lots > 0 and st.filled_lots < st.target_lots:
            session._on_fill({"order_no": st.order_no, "symbol": sym,
                              "lots": st.target_lots - st.filled_lots,
                              "price": limit_ups.get(sym, 0),
                              "action": "buy", "filled_no": f"RF{sym}",
                              "filled_time": "09:00:00",
                              "quantity": (st.target_lots - st.filled_lots) * 1000})

    def chase_send(sym, c, cur):
        """管線化 (2026-08-28): 逐步送**一筆**市價 (同步呼叫 _chase_send_one)。決定 accept/reject 依
        送出當下 (next_at) 該檔是否已開盤。不模型化「回報延遲」——管線化下送單不等回覆、回報晚到不擋下一筆。
        停送依 cadence 相同條件: chase_done / stopped / shortfall≤0 / 淘汰出場硬上限處置 / cutoff。"""
        send_at = c["next_at"]
        oa = ctx["opened_at"].get(sym)
        broker._decision[sym] = "accept" if (oa is not None and oa <= send_at) else "reject"
        with session._lock:
            st = session.trades.get(sym)
            sf = 0 if st is None else st.target_lots - st.filled_lots
            stop = (st is None or st.chase_done or st.stopped_reason or st.exited
                    or session._budget_breached or st.is_disposition)
            first_came = bool(st and st.first_trade_fired)   # 開盤訊號 (monitor_on_trade 設)
        if stop or sf <= 0 or send_at.time() >= cutoff:
            broker._decision.pop(sym, None)
            c["done"] = ("chase_done" if (st is not None and st.chase_done)
                         else f"stopped:{st.stopped_reason}" if (st is not None and st.stopped_reason)
                         else "target_filled" if sf <= 0 else "cutoff")
            _fake_fill(sym)
            return
        n_placed, n_rej = len(broker.placed), len(broker.rejected)
        _set_clock(send_at)
        try:
            outcome = session._chase_send_one(sym, sf)
        finally:
            broker._decision.pop(sym, None)
            _set_clock(cur)
        if len(broker.placed) > n_placed:
            c["sends"] += 1
        if len(broker.rejected) > n_rej:
            c["rejects"] += 1
        if outcome == "accepted":
            c["done"] = "accepted"
            c["accepted_at"] = send_at
            _fake_fill(sym)
        elif outcome in ("fatal", "aborted"):
            c["done"] = outcome
        elif first_came:
            # 開盤訊號已到 → 這筆即 final,送完就停 (production: cadence 收到 first_trade_fired
            # → 送最後一筆 → return,不再噴;2026-08-28 使用者定案)
            c["done"] = "final_after_open"
            _fake_fill(sym)
        else:                                   # blind-fire rejected / accepted_extra → 排下一筆
            c["next_at"] = send_at + timedelta(seconds=CHASE_SEND_INTERVAL)

    def run_due_chase(cur):
        for sym, c in ctx["chase"].items():
            while c["done"] is None and cur >= c["next_at"]:
                chase_send(sym, c, cur)

    def monitor_on_trade(sym, data, dt):
        """仿 runner._monitor_on_trade (08:59:58 掛上,09:00 起生效)。"""
        if _pick(data, "isTrial"):
            return
        if dt.time() < OPEN_TIME:
            return
        price = float(_pick(data, "price") or 0)
        if price <= 0:
            return
        if mark_opened is not None:
            mark_opened(sym)
        session.on_first_trade(sym)

    def note_trade(sym, data, dt):
        """replay 內部: 記該檔開盤時刻 (首筆非試撮真成交 = isOpen) + 最後一筆 (換手種子化用)。"""
        if _pick(data, "isTrial") or dt.time() < OPEN_TIME:
            return
        if float(_pick(data, "price") or 0) <= 0:
            return
        if sym not in ctx["opened_at"]:
            ctx["opened_at"][sym] = dt
            broker.opened.add(sym)
        ctx["last_trade"][sym] = data

    def do_swap():
        """filter → trader 換手 (production: 09:00 等待迴圈回來+寫快照後)。仿 runner 種子化首筆成交。"""
        tr = ctx["trader"]
        if tr is None:
            return
        for sym in ctx["pre_marked"]:
            lt = ctx["last_trade"].get(sym)
            if lt is not None:
                try:
                    tr.on_trade(sym, lt)
                except Exception:
                    pass

    trader_takes_cont = None

    # ── 串流讀 tick ──
    n_books = n_trades = n_lines = 0
    swap_dt = None
    try:
        with open(args.ticks, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                channel = obj.get("channel")
                data = obj.get("data") or {}
                ts_str = obj.get("ts")
                if channel not in ("books", "trades") or not ts_str:
                    continue
                try:
                    dt = datetime.fromisoformat(ts_str)
                except Exception:
                    continue
                n_lines += 1
                _set_clock(dt)
                t = dt.time()

                # 相位推進
                if ctx["phase"] == "filter" and t >= PRE_ORDER_TIME:
                    do_pre_order()
                    ctx["phase"] = "preopen"
                if ctx["phase"] in ("filter", "preopen") and t >= OPEN_TIME:
                    open_dt = datetime.combine(dt.date(), OPEN_TIME)
                    do_open(open_dt)
                    swap_dt = open_dt + timedelta(seconds=swap_delay)
                    ctx["phase"] = "opening"
                if ctx["phase"] == "opening" and swap_dt is not None and dt >= swap_dt:
                    do_swap()
                    ctx["phase"] = "trading"

                # 市價追: 先結算到 dt 為止已回來的送單結果 (worker thread 與行情 thread 並行)
                run_due_chase(dt)

                symbol = _pick(data, "symbol")
                if not symbol:
                    continue
                symbol = str(symbol)

                if channel == "books":
                    n_books += 1
                    bids = data.get("bids") or []
                    asks = data.get("asks") or []
                    # 記漲停價委買量 (20% 母數用) — 篩選期間
                    lu = limit_ups.get(symbol)
                    if lu:
                        for b in bids:
                            bp = _pick(b, "price")
                            if bp is not None and abs(float(bp) - lu) < 0.001:
                                ctx["last_bid_at_limit"][symbol] = int(_pick(b, "size") or 0)
                                break
                    is_cont = bool(_pick(data, "isContinuous"))   # 交易所「已逐筆」旗標 (開盤訊號底線)
                    if ctx["phase"] in ("filter", "preopen", "opening"):
                        # opening = 換手前 filter 仍在線 (production 競態窗口)
                        if filter_takes_cont:
                            filter_on_book(symbol, bids, asks, is_cont)
                        else:
                            filter_on_book(symbol, bids, asks)
                    elif ctx["trader"] is not None:
                        tr = ctx["trader"]
                        if trader_takes_cont is None:
                            trader_takes_cont = _accepts_kw(tr.on_book, "is_continuous")
                        if trader_takes_cont:
                            tr.on_book(symbol, bids, asks, is_cont)   # 支撐消失 → 同步出場
                        else:
                            tr.on_book(symbol, bids, asks)
                elif channel == "trades":
                    n_trades += 1
                    note_trade(symbol, data, dt)
                    if ctx["phase"] in ("preopen", "opening"):
                        monitor_on_trade(symbol, data, dt)
                    elif ctx["phase"] == "trading" and ctx["trader"] is not None:
                        ctx["trader"].on_trade(symbol, data)
        # 檔尾: 把還在等結果的市價追收尾 (標 eof;部位假設同 done 語意)
        for sym, c in ctx["chase"].items():
            if c["done"] is None:
                c["done"] = "eof"
                _fake_fill(sym)
    finally:
        ts_mod.time = orig_time_mod

    # 出場清單: 重放結束後直接由 session 狀態算 (同步出場,已定案)
    ctx["exits"] = [(s, st.stopped_reason, st.filled_lots)
                    for s, st in session.trades.items() if st.exited]
    report = _report(args, session, state, ctx, unsubbed, n_lines, n_books, n_trades)
    return SimpleNamespace(session=session, state=state, ctx=ctx,
                           broker=session.broker, report=report,
                           marked=ctx.get("pre_marked", []), exits=ctx["exits"])


def _unmarks_after_open(state, date):
    """開盤 (≥09:00) 後被 filter unmark 的檔 (= 競態窗口誤撤候選)。"""
    out = []
    cut = f"{date}T09:00:00"
    for sym, evs in state.history.items():
        for e in evs:
            if e.get("event") == "unmark" and str(e.get("ts") or "") >= cut \
                    and e.get("reason") in ("bid_below_limit", "ask_appeared"):
                out.append((sym, e.get("reason"), str(e.get("ts"))[11:19]))
    return sorted(out)


def _report(args, session, state, ctx, unsubbed, n_lines, n_books, n_trades):
    broker = session.broker
    marked = ctx.get("pre_marked", [])
    first_tick = sorted(state.first_tick_limit_up & set(marked)) if hasattr(state, "first_tick_limit_up") else []
    intraday = sorted(set(marked) - set(first_tick))
    pre_orders = [(s, p, l) for (k, s, p, l) in broker.placed if k == "limit_buy"]
    mkt = [(s, l) for (k, s, _p, l) in broker.placed if k == "market_buy"]
    sells = [(k, s, p, l) for (k, s, p, l) in broker.placed if k in ("limit_sell", "market_sell")]

    lines = []
    def w(x=""):
        lines.append(x)

    w("=" * 64)
    w(f"REPLAY {args.date}  —  ticks={n_lines} (books={n_books} trades={n_trades})")
    w(f"預算: total={args.total_budget:,} per_symbol={args.per_symbol:,} mode={args.sizing_mode}")
    w("=" * 64)

    w("\n── 篩選 ──")
    w(f"marked 定案: {len(marked)} 檔  |  開盤即鎖 {len(first_tick)} / 盤中鎖上 {len(intraday)}")
    w(f"  開盤即鎖: {' '.join(first_tick) or '—'}")
    w(f"  盤中鎖上: {' '.join(intraday) or '—'}")
    culled = ctx.get("culled") or []
    w(f"量減半剔除 (08:59:58): {len(culled)} 檔  {' '.join(sorted(c[0] for c in culled)) or '—'}")
    try:
        disc = state.get_discarded_list()
        w(f"discarded 累計: {len(disc)} 檔")
        for d in sorted(disc, key=lambda x: x.get("symbol") or "")[:40]:
            w(f"    {d.get('symbol')}  {d.get('reason')}")
        if len(disc) > 40:
            w(f"    ... 另 {len(disc)-40} 檔")
    except Exception:
        pass

    w("\n── 進場 ──")
    w(f"08:59:58 預掛限價單: {len(pre_orders)} 檔")
    for s, p, l in pre_orders:
        w(f"    {s}  @{p}  {l} 張")
    w(f"09:00 市價追委託成功: {len(mkt)} 檔  |  集合競價拒單: {len(getattr(broker, 'rejected', []))} 筆")
    for s, l in mkt:
        w(f"    market_buy {s} {l} 張")
    w(f"撤單筆數 (含撤 P): {len(broker.cancelled)}")
    for no, s, reason in broker.cancelled:
        w(f"    cancel {s} {no} ({reason})")
    w(f"budget_used={session.budget_used:,.0f}  buy_cost_actual={session._buy_cost_actual:,.0f}"
      f"  breached={session._budget_breached}")

    w(f"\n── 開盤競態 (swap-delay={ctx.get('swap_delay', 0):.1f}s;集合競價拒市價;市價追管線化逐步送) ──")
    late = _unmarks_after_open(state, args.date)
    late_bbl = [x for x in late if x[1] == "bid_below_limit"]
    marked_set = set(marked)
    # 重點: 09:00 後被 filter unmark 的**預掛標的** (= 誤撤,因為它們 08:59:58 還鎖著、已下 P)
    marked_late = [x for x in late if x[0] in marked_set]
    w(f"開盤 (≥09:00) 後 filter unmark: 共 {len(late)} 檔 (其中 bid_below_limit {len(late_bbl)} 檔)")
    w(f"其中**預掛標的**被開盤後 unmark: {len(marked_late)} 檔"
      + ("  ← 競態誤撤 (它們 08:59:58 已下預掛 P、當下仍鎖漲停)" if marked_late else "  ✅ 無 (修法生效)"))
    for sym, reason, ts in marked_late:
        w(f"    {sym}  {reason}  {ts}")
    chase = ctx.get("chase") or {}
    if chase:
        w("市價追 (逐檔):")
        for sym in sorted(chase):
            c = chase[sym]
            oa = ctx["opened_at"].get(sym)
            oa_s = oa.strftime("%H:%M:%S.%f")[:-3] if oa else "—"
            acc = c["accepted_at"].strftime("%H:%M:%S.%f")[:-3] if c["accepted_at"] else "—"
            w(f"    {sym}  送 {c['sends']} 拒 {c['rejects']}  → {c['done']}  "
              f"(該檔開盤 {oa_s};市價委託成功 {acc})")

    w("\n── 出場 (假設 marked 依 sized 張數成交 → 驗出場偵測邏輯) ──")
    if ctx["exits"]:
        for sym, reason, filled in ctx["exits"]:
            w(f"    {sym}  觸發 {reason}")
        w(f"賣單送出: {len(sells)} 筆")
        for k, s, p, l in sells:
            w(f"    {k} {s} @{p} {l} 張")
    else:
        w("    無出場觸發 (marked 檔盤中支撐都沒消失,或無 marked)")

    # 對照組
    gt_fills = _load_ground_truth_symbols(args.fills)
    gt_orders = _load_ground_truth_symbols(args.orders)
    gt = gt_fills | gt_orders
    if gt:
        new_syms = set(marked)
        w("\n── 對照真實台帳 (fills+orders CSV) ──")
        w(f"真實台帳出現 symbol: {len(gt)}  |  新系統 marked: {len(new_syms)}")
        w(f"  舊有、新沒 marked: {' '.join(sorted(gt - new_syms)) or '—'}")
        w(f"  新 marked、舊台帳沒: {' '.join(sorted(new_syms - gt)) or '—'}")
        w(f"  兩邊都有: {' '.join(sorted(new_syms & gt)) or '—'}")

    w("\n" + "=" * 64)
    report = "\n".join(lines)
    print(report)
    if args.report:
        Path(args.report).write_text(report, encoding="utf-8")
        print(f"\n[報告已寫 {args.report}]")
    return report


def main():
    ap = argparse.ArgumentParser(description="全流程 replay — 真實 tick 驗現行系統")
    ap.add_argument("--ticks", required=True, help="{date}_ticks.jsonl")
    ap.add_argument("--limit-ups", required=True, dest="limit_ups")
    ap.add_argument("--date", required=True)
    ap.add_argument("--dispositions", default="")
    ap.add_argument("--limit-downs", default="", dest="limit_downs")
    ap.add_argument("--day-tradable", default="", dest="day_tradable")
    ap.add_argument("--fills", default="")
    ap.add_argument("--orders", default="")
    ap.add_argument("--total-budget", type=float, default=3_700_000, dest="total_budget")
    ap.add_argument("--per-symbol", type=float, default=400_000, dest="per_symbol")
    ap.add_argument("--sizing-mode", default="budget", dest="sizing_mode")
    ap.add_argument("--fixed-lots", type=int, default=0, dest="fixed_lots")
    ap.add_argument("--bid-drop-ratio", type=float, default=0.5, dest="bid_drop_ratio")
    ap.add_argument("--swap-delay", type=float, default=1.0, dest="swap_delay",
                    help="09:00 後 filter handler 多活幾秒才換 trader (production ~1s;0=立刻)")
    ap.add_argument("--chase-cutoff", default="09:03:00", dest="chase_cutoff")
    ap.add_argument("--report", default="")
    run(ap.parse_args())


if __name__ == "__main__":
    main()
