"""全流程 replay — 用真實 tick JSONL 驗現行系統 (部署前 gate)。

把某交易日錄下的 books/trades raw tick (recorder.py 產的 {date}_ticks.jsonl) 依 ts 順序
餵進**現行 production handler** (filter._on_book / trader.on_book / trader.on_trade /
TradingSession),用假時鐘重現 篩選 → 08:59:58 預掛 → 09:00 盲送 → 盤中出場,產出報告。
因 handler 是正式碼,這等於用真實行情驗整條決策鏈。**只讀+餵,不改任何 production 邏輯。**

用法:
  python scripts/replay_day.py --ticks replay_data/2026-08-21_ticks.jsonl \
      --limit-ups replay_data/2026-08-21_limit_ups.json --date 2026-08-21 \
      [--dispositions ...] [--limit-downs ...] [--fills ...] [--orders ...] \
      [--total-budget 3700000] [--per-symbol 400000] [--report out.txt]

出場區塊為「假設 marked 檔在 9:00 依 sized 張數成交」→ 驗出場**偵測邏輯**在真實五檔上的表現
(漲停鎖死常不會成交,故此為 if-filled 邏輯檢查,非宣稱真成交)。real 成交對照用 --fills 的台帳 diff。
"""
import argparse
import json
import sys
from datetime import datetime, time as dtime
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import filter as filter_mod          # noqa: E402
import trader as trader_mod          # noqa: E402
import trading_session as ts_mod     # noqa: E402
from state import State              # noqa: E402
from filter import make_on_book_handler  # noqa: E402
from trader import Trader            # noqa: E402
from trading_session import TradingSession  # noqa: E402

PRE_ORDER_TIME = dtime(8, 59, 58)
OPEN_TIME = dtime(9, 0, 0)


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


# ─── ReplayBroker: 仿 tests FakeBroker,只記錄,不打真 API ───
class ReplayBroker:
    def __init__(self):
        self.connected = True
        self.healthy = True
        self.placed = []
        self.cancelled = []
        self._n = 0

    def _next(self):
        self._n += 1
        return f"R{self._n}"

    def place_limit_buy(self, symbol, price, lots):
        no = self._next()
        self.placed.append(("limit_buy", symbol, price, lots))
        return no

    def place_market_buy(self, symbol, lots):
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
    import csv
    syms = set()
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            s = (row.get("symbol") or "").strip()
            if s:
                syms.add(s)
    return syms


def run(args):
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
    session.broker = ReplayBroker()
    session.set_params(total_budget=args.total_budget, per_symbol_budget=args.per_symbol,
                       sizing_mode=args.sizing_mode, fixed_lots=args.fixed_lots)
    session.set_armed(True)
    session.order_min_interval = 0.0
    session.set_dispositions(dispositions)
    if day_tradable:
        session.set_day_tradable(day_tradable)
    session.set_limit_downs(limit_downs)

    filter_cfg = SimpleNamespace(pre_order_time="08:59:58", final_check_start="08:59:58")
    trader_cfg = SimpleNamespace(first_trade_min_lots=args.first_trade_min_lots,
                                 bid_decline_sample_sec=60, bid_decline_minutes=5)

    unsubbed = []
    unsub_ref = {"fn": lambda sym: unsubbed.append(sym)}
    filter_on_book = make_on_book_handler(state, limit_ups, filter_cfg, unsub_ref)

    ctx = {"phase": "filter", "trader": None, "last_bid_at_limit": {},
           "pre_marked": [], "exits": []}

    def do_pre_order():
        marked = state.get_marked_prioritized()
        culled = state.final_check_all()          # 量減半批次剔除
        marked = state.get_marked_prioritized()   # 定案
        ctx["pre_marked"] = list(marked)
        ctx["culled"] = culled
        # 20% 母數: 用最近一次「漲停價那檔」的委買量快照
        bid_vols = {s: ctx["last_bid_at_limit"].get(s, 0) for s in marked}
        session.place_pre_orders(marked, limit_ups, limit_up_bid_vols=bid_vols)

    def do_open():
        marked = ctx["pre_marked"]
        tr = Trader(watchlist=marked, limit_ups=limit_ups, cfg=trader_cfg,
                    state=state, session=session, dispositions=dispositions)
        ctx["trader"] = tr
        # 9:00 盲送 (同步) — ReplayBroker 即接受 → 走 A:撤 P。之後假設成交建部位驗出場。
        for sym in marked:
            st = session.trades.get(sym)
            if st is None or st.stopped_reason or not st.order_no:
                continue
            session._market_chase_worker(sym, dtime(0, 0, 0), dtime(23, 59, 59))
            # 假設成交: 依 target 建部位 (label: if-filled 出場邏輯檢查)
            st = session.trades.get(sym)
            if st and st.order_no and st.target_lots > 0:
                session._on_fill({"order_no": st.order_no, "symbol": sym,
                                  "lots": st.target_lots, "price": limit_ups.get(sym, 0),
                                  "action": "buy", "filled_no": f"RF{sym}",
                                  "filled_time": "09:00:00",
                                  "quantity": st.target_lots * 1000})

    # ── 串流讀 tick ──
    n_books = n_trades = n_lines = 0
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
                do_open()
                ctx["phase"] = "trading"

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
                if ctx["phase"] in ("filter", "preopen"):
                    filter_on_book(symbol, bids, asks)
                elif ctx["trader"] is not None:
                    before = _exit_count(session)
                    ctx["trader"].on_book(symbol, bids, asks)
                    _record_new_exits(session, ctx, before)
            elif channel == "trades":
                n_trades += 1
                if ctx["phase"] == "trading" and ctx["trader"] is not None:
                    ctx["trader"].on_trade(symbol, data)

    report = _report(args, session, state, ctx, unsubbed, n_lines, n_books, n_trades)
    return SimpleNamespace(session=session, state=state, ctx=ctx,
                           broker=session.broker, report=report,
                           marked=ctx.get("pre_marked", []), exits=ctx["exits"])


def _exit_count(session):
    return sum(1 for st in session.trades.values() if st.exited)


def _record_new_exits(session, ctx, before):
    if _exit_count(session) > before:
        for sym, st in session.trades.items():
            if st.exited and sym not in {e[0] for e in ctx["exits"]}:
                ctx["exits"].append((sym, st.stopped_reason, st.filled_lots))


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
    w(f"量減半剔除 (08:59:58): {len(culled)} 檔  {' '.join(c[0] for c in culled) or '—'}")
    try:
        disc = state.get_discarded_list()
        w(f"discarded 累計: {len(disc)} 檔")
        for d in disc[:40]:
            w(f"    {d.get('symbol')}  {d.get('reason')}")
        if len(disc) > 40:
            w(f"    ... 另 {len(disc)-40} 檔")
    except Exception:
        pass

    w("\n── 進場 ──")
    w(f"08:59:58 預掛限價單: {len(pre_orders)} 檔")
    for s, p, l in pre_orders:
        w(f"    {s}  @{p}  {l} 張")
    w(f"09:00 市價盲送送出: {len(mkt)} 檔  (ReplayBroker 即接受 → 撤預掛 P)")
    w(f"撤單筆數 (含撤 P): {len(broker.cancelled)}")
    w(f"budget_used={session.budget_used:,.0f}  buy_cost_actual={session._buy_cost_actual:,.0f}"
      f"  breached={session._budget_breached}")

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
    ap.add_argument("--first-trade-min-lots", type=int, default=10, dest="first_trade_min_lots")
    ap.add_argument("--bid-drop-ratio", type=float, default=0.5, dest="bid_drop_ratio")
    ap.add_argument("--report", default="")
    run(ap.parse_args())


if __name__ == "__main__":
    main()
