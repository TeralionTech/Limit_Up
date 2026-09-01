"""Hub/Node 架構歷史重放 A/B — 用真實 tick 驗「中心過濾 Hub → node」與 standalone 等價。

兩趟重放同一天的 {date}_ticks.jsonl:
  A) standalone 基準: 直接跑 scripts/replay_day.run() (現行單機整條鏈)。
  B) hub/node: 08:30–08:59:50 餵 hub filter (真 make_on_book_handler) → 08:59:50 凍結,
     裸建 Runner 呼**真 marked_snapshot()** (含 limit_down/is_t30/day_tradable/last_bid_vol 四欄)
     → JSON round-trip → 第二個裸 Runner 呼**真 _apply_marked_snapshot()** (node 兩步 seed)
     → 08:59:50–58 只餵 marked 檔給 node 的完整 filter handler (unmark 鎖破檔 → 撤單,
       與 production node 2026-09-01 對齊) → 08:59:58 final_check_all + place_pre_orders。
最後 diff: marked 集合 / 量減半剔除 / 預掛清單。09:00 後兩邊共用同一 _trade_phase 程式碼,
watchlist 相同即行為相同,故 A/B 範圍到預掛定案為止。

已知未建模: node 08:59:50 開訂的 WS 訂閱延遲 (~百 ms,可能漏最前面幾筆);
production node bid_vols 讀 subscriber.get_latest_snapshot — 此處以 node 視窗內
最後一筆漲停層委買量等價模擬。

預期差異只有一類: 08:59:50 凍結後才鎖上的檔 (hub 快照沒有 → node 天生看不到,
standalone 會 mark+預掛)。報告會把每筆差異分類;出現其他類 → FAIL。

用法:
  python scripts/replay_hub_node.py --ticks output/2026-08-28_ticks.jsonl \
      --limit-ups output/2026-08-28_limit_ups.json --date 2026-08-28 \
      [--dispositions ...] [--limit-downs ...] [--day-tradable ...] \
      [--t30 9103,1234] [--total-budget 3700000] [--per-symbol 400000] [--report out.txt]
"""
import argparse
import json
import sys
import time as _real_time
from datetime import datetime, time as dtime
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.replay_day import (FrozenDatetime, _set_clock, ReplayBroker,  # noqa: E402,F401
                                _pick, _load_json, run as run_standalone)
import trading_session as ts_mod     # noqa: E402
from state import State              # noqa: E402
from filter import make_on_book_handler  # noqa: E402
from trading_session import TradingSession  # noqa: E402
from runner import Runner            # noqa: E402

HUB_FREEZE_TIME = dtime(8, 59, 50)
PRE_ORDER_TIME = dtime(8, 59, 58)
OPEN_TIME = dtime(9, 0, 0)


def _mk_session(args):
    """node 端 session — 與 replay_day.run() 的 session 設定完全同款 (公平 A/B)。"""
    s = TradingSession()
    s.roll_day(args.date)
    s.set_mode("real")
    s.broker = ReplayBroker()
    s.set_params(total_budget=args.total_budget, per_symbol_budget=args.per_symbol,
                 sizing_mode=args.sizing_mode, fixed_lots=args.fixed_lots)
    s.set_armed(True)
    s.order_min_interval = 0.0
    s.cancel_symbol_orders_async = s.cancel_symbol_orders
    if hasattr(s, "_rate"):
        s._rate.acquire = lambda: None
    return s


def run_hub_node(args):
    """B 趟: hub 段 → 凍結/快照/seed → node 視窗 → 08:59:58 預掛。回 ctx dict。"""
    ts_mod._EXIT_FILL_WAIT_SEC = 0.0
    limit_ups = {str(k): float(v) for k, v in _load_json(args.limit_ups).items() if v}
    dispositions = {str(k): bool(v) for k, v in _load_json(args.dispositions).items()}
    limit_downs = {str(k): float(v) for k, v in _load_json(args.limit_downs).items() if v}
    day_tradable = {str(k): bool(v) for k, v in _load_json(args.day_tradable).items()} \
        if args.day_tradable else {}
    t30 = {s.strip() for s in (args.t30 or "").split(",") if s.strip()}

    # ── hub 段: 真 filter handler + 自己的 State ──
    hub_state = State(bid_drop_ratio=args.bid_drop_ratio)
    hub_cfg = SimpleNamespace(pre_order_time="08:59:58", final_check_start="08:59:58")
    hub_unsubbed = []
    hub_on_book = make_on_book_handler(hub_state, limit_ups, hub_cfg,
                                       {"fn": hub_unsubbed.append})

    # ── node 端: 裸 Runner (真 _apply_marked_snapshot) + replay session ──
    node_session = _mk_session(args)
    node_runner = Runner()
    node_runner.cfg = SimpleNamespace(role="node")
    node_runner.state = State(bid_drop_ratio=args.bid_drop_ratio)
    node_runner.session = node_session

    ctx = {"phase": "hub", "snap": None, "hub_marked_at_freeze": [],
           "node_pre_marked": [], "node_culled": [], "node_unsubbed": [],
           "node_on_book": None, "node_syms": set(), "node_bid_at_limit": {},
           "n_hub_books": 0, "n_node_books": 0}

    def _node_unsub_and_cancel(sym):
        ctx["node_unsubbed"].append(sym)
        try:
            node_session.cancel_symbol_orders(sym, "unmarked")
        except Exception:
            pass

    def do_freeze():
        """08:59:50: hub 凍結 → 真 marked_snapshot() → JSON round-trip → 真 node apply。"""
        hub_runner = Runner()
        hub_runner.cfg = SimpleNamespace(role="hub")
        hub_runner.state = hub_state
        hub_runner.limit_ups = limit_ups
        hub_runner.dispositions = dispositions
        hub_runner.limit_downs = limit_downs
        hub_runner.day_tradable = day_tradable
        hub_runner.session.set_untradable(t30)
        hub_runner._marked_frozen = True
        snap = json.loads(json.dumps(hub_runner.marked_snapshot()))   # 驗 JSON 序列化
        assert snap["final"] is True
        ctx["snap"] = snap
        ctx["hub_marked_at_freeze"] = [s["symbol"] for s in snap["symbols"]]
        marked = node_runner._apply_marked_snapshot(snap)             # 真 production apply
        ctx["node_syms"] = set(marked)
        ctx["node_on_book"] = make_on_book_handler(
            node_runner.state, node_runner.limit_ups,
            SimpleNamespace(pre_order_time="08:59:58", final_check_start="08:59:58"),
            {"fn": _node_unsub_and_cancel})

    def do_node_pre_order():
        """08:59:58: 量減半 → 預掛。bid_vols = node 視窗內最後一筆漲停層委買量;
        視窗內零 tick 的檔退用 Hub 快照 last_bid_vol (與 production _start_pre_order_timer
        的 _node_bid_vol_fallback 兜底一致,2026-09-01)。"""
        ctx["node_culled"] = node_runner.state.final_check_all()
        marked = node_runner.state.get_marked_prioritized()
        ctx["node_pre_marked"] = list(marked)
        bid_vols = {s: ctx["node_bid_at_limit"][s] for s in marked
                    if s in ctx["node_bid_at_limit"]}
        for s, v in getattr(node_runner, "_node_bid_vol_fallback", {}).items():
            if s not in bid_vols and s in marked and v > 0:
                bid_vols[s] = v
        node_session.place_pre_orders(marked, node_runner.limit_ups,
                                      limit_up_bid_vols=bid_vols)

    # 同 replay_day: time.sleep 全 no-op (place_pre_orders/撤單重試不等真時間)
    time_proxy = SimpleNamespace(sleep=lambda sec: None, time=_real_time.time,
                                 monotonic=_real_time.monotonic,
                                 perf_counter=_real_time.perf_counter)
    orig_time_mod = ts_mod.time
    ts_mod.time = time_proxy
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
                _set_clock(dt)
                t = dt.time()

                # 相位推進
                if ctx["phase"] == "hub" and t >= HUB_FREEZE_TIME:
                    do_freeze()
                    ctx["phase"] = "node_window"
                if ctx["phase"] == "node_window" and t >= PRE_ORDER_TIME:
                    do_node_pre_order()
                    ctx["phase"] = "auction"
                if t >= OPEN_TIME:
                    break        # A/B 範圍到預掛定案;09:00 後兩邊共用同一程式碼

                if channel != "books":
                    continue
                symbol = str(_pick(data, "symbol") or "")
                if not symbol:
                    continue
                bids = data.get("bids") or []
                asks = data.get("asks") or []
                is_cont = bool(_pick(data, "isContinuous"))

                if ctx["phase"] == "hub":
                    ctx["n_hub_books"] += 1
                    hub_on_book(symbol, bids, asks, is_cont)
                elif symbol in ctx["node_syms"]:
                    # node 只訂 marked;auction 段 (08:59:58–09:00) filter unmark 照 production 續跑
                    ctx["n_node_books"] += 1
                    ctx["node_on_book"](symbol, bids, asks, is_cont)
                    if ctx["phase"] == "node_window":
                        lu = limit_ups.get(symbol)
                        if lu:
                            for b in bids:
                                bp = _pick(b, "price")
                                if bp is not None and abs(float(bp) - lu) < 0.001:
                                    ctx["node_bid_at_limit"][symbol] = int(_pick(b, "size") or 0)
                                    break

        # 檔案在 09:00 前就結束 (不完整資料) 的保護
        if ctx["phase"] == "hub":
            do_freeze()
            ctx["phase"] = "node_window"
        if ctx["phase"] == "node_window":
            do_node_pre_order()
    finally:
        ts_mod.time = orig_time_mod

    ctx["node_session"] = node_session
    ctx["node_broker"] = node_session.broker
    ctx["node_state"] = node_runner.state
    return ctx


def _pre_orders(broker):
    return {s: (p, l) for (k, s, p, l) in broker.placed if k == "limit_buy"}


def diff_report(args, std, hub_node):
    lines = []
    w = lines.append
    w("=" * 64)
    w(f"HUB/NODE A/B REPLAY {args.date}")
    w("=" * 64)

    snap = hub_node["snap"] or {}
    hub_marked = hub_node["hub_marked_at_freeze"]
    w(f"\n── B 趟 (hub/node) ──")
    w(f"hub 凍結 (08:59:50) marked: {len(hub_marked)} 檔  {' '.join(hub_marked) or '—'}")
    for s in (snap.get("symbols") or []):
        w(f"    {s['symbol']}  漲停 {s['limit_up']}  峰值 {s['max_bid_vol']}  last {s['last_bid_vol']}"
          f"  跌停 {s['limit_down']}  T30 {s['is_t30']}  現沖 {s['day_tradable']}"
          f"  處置 {s['is_disposition']}  即鎖 {s['first_tick']}")
    w(f"node 視窗 (50→58s) books: {hub_node['n_node_books']} 筆"
      f"  |  unmark(鎖破): {' '.join(hub_node['node_unsubbed']) or '—'}")
    node_culled = [c[0] for c in hub_node["node_culled"]]
    w(f"node 量減半剔除: {' '.join(node_culled) or '—'}")
    node_pre = _pre_orders(hub_node["node_broker"])
    w(f"node 預掛: {len(node_pre)} 檔")
    for s, (p, l) in sorted(node_pre.items()):
        w(f"    {s}  @{p}  {l} 張")

    w(f"\n── A 趟 (standalone 基準) ──")
    std_marked = list(std.marked)
    std_culled = [c[0] for c in (std.ctx.get("culled") or [])]
    std_pre = _pre_orders(std.broker)
    w(f"standalone 預掛定案 marked: {len(std_marked)} 檔  {' '.join(std_marked) or '—'}")
    w(f"standalone 量減半剔除: {' '.join(std_culled) or '—'}")
    w(f"standalone 預掛: {len(std_pre)} 檔")
    for s, (p, l) in sorted(std_pre.items()):
        w(f"    {s}  @{p}  {l} 張")

    # ── DIFF 分類 ──
    w(f"\n── DIFF ──")
    verdict_fail = []
    node_set, std_set = set(hub_node["node_pre_marked"]), set(std_marked)
    only_std = std_set - node_set
    only_node = node_set - std_set
    hub_hist = hub_node["node_state"].history      # node state 只有 marked 檔歷史
    for s in sorted(only_std):
        if s not in set(hub_marked):
            w(f"  {s}: standalone 有、node 沒 — 08:59:50 凍結後才鎖上 (架構天生差,預期內)")
        else:
            w(f"  {s}: standalone 有、node 沒 — 在 hub 快照內但 node 端被剔 ⚠ 需解釋")
            verdict_fail.append(s)
    for s in sorted(only_node):
        w(f"  {s}: node 有、standalone 沒 ⚠ 需解釋 (node 不該多出檔)")
        verdict_fail.append(s)
    snap_t30 = {x["symbol"] for x in (snap.get("symbols") or []) if x.get("is_t30")}
    for s in sorted(node_set & std_set):
        np_, sp_ = node_pre.get(s), std_pre.get(s)
        if np_ != sp_:
            if np_ is None and s in snap_t30:
                w(f"  {s}: node 不預掛 — T30 防線生效 (standalone 基準無 T30 名單) — 預期內")
            else:
                w(f"  {s}: 預掛不同 node={np_} std={sp_} ⚠")
                verdict_fail.append(s)
    if not only_std and not only_node:
        w("  marked 集合完全一致 ✅")

    # 風控欄位流通驗證
    w(f"\n── 風控欄位到位 (node session) ──")
    ns = hub_node["node_session"]
    w(f"  untradable(T30): {sorted(ns.untradable) or '—'}")
    w(f"  limit_downs: {len(ns.limit_downs)} 檔"
      f"  {' '.join(f'{k}@{v}' for k, v in sorted(ns.limit_downs.items())) or ''}")
    w(f"  day_tradable=False: {sorted(k for k, v in ns.day_tradable.items() if v is False) or '—'}")

    w(f"\nVERDICT: {'❌ FAIL — ' + ' '.join(sorted(set(verdict_fail))) if verdict_fail else '✅ PASS (差異全屬預期類)'}")
    w("=" * 64)
    report = "\n".join(lines)
    print(report)
    if args.report:
        Path(args.report).write_text(report, encoding="utf-8")
        print(f"[報告已寫 {args.report}]")
    return report, not verdict_fail


def main():
    ap = argparse.ArgumentParser(description="Hub/Node 架構歷史重放 A/B")
    ap.add_argument("--ticks", required=True)
    ap.add_argument("--limit-ups", required=True, dest="limit_ups")
    ap.add_argument("--date", required=True)
    ap.add_argument("--dispositions", default="")
    ap.add_argument("--limit-downs", default="", dest="limit_downs")
    ap.add_argument("--day-tradable", default="", dest="day_tradable")
    ap.add_argument("--t30", default="", help="逗號分隔 T30 名單 (歷史 T30V 檔通常沒留,可合成注入)")
    ap.add_argument("--total-budget", type=float, default=3_700_000, dest="total_budget")
    ap.add_argument("--per-symbol", type=float, default=400_000, dest="per_symbol")
    ap.add_argument("--sizing-mode", default="budget", dest="sizing_mode")
    ap.add_argument("--fixed-lots", type=int, default=0, dest="fixed_lots")
    ap.add_argument("--bid-drop-ratio", type=float, default=0.5, dest="bid_drop_ratio")
    ap.add_argument("--report", default="")
    args = ap.parse_args()

    # A 趟 standalone 基準 (replay_day 原封不動;fills/orders 對照不用)
    std_args = SimpleNamespace(
        ticks=args.ticks, limit_ups=args.limit_ups, date=args.date,
        dispositions=args.dispositions, limit_downs=args.limit_downs,
        day_tradable=args.day_tradable, fills="", orders="",
        total_budget=args.total_budget, per_symbol=args.per_symbol,
        sizing_mode=args.sizing_mode, fixed_lots=args.fixed_lots,
        bid_drop_ratio=args.bid_drop_ratio, swap_delay=1.0,
        chase_cutoff="09:03:00", report="")
    print("[A] standalone 基準重放中 ...")
    std = run_standalone(std_args)

    print("\n[B] hub/node 重放中 ...")
    hub_node = run_hub_node(args)

    _, ok = diff_report(args, std, hub_node)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
