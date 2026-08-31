"""搶漲停策略 — 篩選階段主 script。

用法:
    cd experiment/hit_limit_up
    python filter.py

流程:
    1. Load .env → login 富邦
    2. 抓上市/上櫃全 symbol universe (~2600)
    3. 抓每檔漲停價 (limitUpPrice) 開盤前一次拿齊
    4. 啟動 Subscriber (loop single-socket 保底路線)
    5. tick 進來 → 判斷 mark / unmark
    6. 到 END_TIME (預設 09:00:00) → snapshot 寫 JSON
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
from datetime import datetime
from pathlib import Path

from config import Config, load_config
from state import State
from universe import fetch_limit_ups, get_universe


# ─── SIGINT 雙擊保護 ─────────────────────────────────────────
# Ctrl-C 第 1 次: 走 KeyboardInterrupt clean shutdown
# Ctrl-C 第 2 次: 強制 os._exit (若 fubon SDK internal thread 卡住 process)
_sigint_count = 0
_orig_sigint = signal.getsignal(signal.SIGINT)

def _sigint_handler(signum, frame):
    global _sigint_count
    _sigint_count += 1
    if _sigint_count >= 2:
        print("\n[main] 第 2 次 Ctrl-C — 強制退出\n", file=sys.stderr)
        os._exit(130)
    print("\n[main] Ctrl-C 收到 — 準備乾淨 shutdown (再按一次強制退出)\n", file=sys.stderr)
    # 讓 Python 走預設 KeyboardInterrupt 拋出流程
    if callable(_orig_sigint):
        _orig_sigint(signum, frame)

# signal.signal 只能在主 thread 執行。當 filter 被 runner background thread
# import 時會拋 ValueError → 用 try/except 保護。CLI 模式 (python filter.py)
# 一定在主 thread 內註冊成功；server 模式下 SIGINT 由 uvicorn 自己處理。
try:
    signal.signal(signal.SIGINT, _sigint_handler)
except ValueError:
    pass

# ─── logging setup ───────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("filter")


# ─── 富邦 login ──────────────────────────────────────────────

class LoginError(RuntimeError):
    """登入失敗 — 用可捕捉例外 (SystemExit 在 runner background thread 會靜默死亡)。"""


def login_fubon(cfg: Config):
    """Login + init_realtime。回 (sdk, accounts).

    重要細節 (跟 Alex fubon_adapter.py:96-99 對齊):
    - PFX 密碼空字串 → 傳 3 參數 (不是 4 個帶空字串)
    - 用 accounts.is_success 判成功 (不是 truthy)
    """
    from fubon_neo.sdk import FubonSDK

    sdk = FubonSDK()
    logger.info(f"[login] account={cfg.account_id[:3]}*** pfx={cfg.pfx_path}")

    if cfg.pfx_password == "":
        accounts = sdk.login(cfg.account_id, cfg.password, cfg.pfx_path)
    else:
        accounts = sdk.login(cfg.account_id, cfg.password,
                             cfg.pfx_path, cfg.pfx_password)

    if not accounts:
        raise LoginError("[login] 富邦 login 失敗 (無 accounts 回傳)")
    is_success = getattr(accounts, "is_success", None)
    if is_success is False:
        raise LoginError(f"[login] 富邦 login is_success=False: "
                         f"{getattr(accounts, 'message', '?')}")

    sdk.init_realtime()
    data = getattr(accounts, "data", None) or []
    logger.info(f"[login] OK, {len(data)} 個帳戶")
    return sdk, accounts


# ─── mark/unmark 判斷邏輯 ────────────────────────────────────

def make_node_book_handler(state: State, limit_ups: dict):
    """ROLE=node 用的輕量 book handler (中心過濾架構):**只更新漲停價委買層的峰值/當前量**
    (state.update_max_bid),**不做 mark/unmark**(marked 清單由 Hub 快照固定 seed)。
    08:59:50–08:59:58 一路墊高 Hub 帶來的峰值 → node 08:59:58 量減半用 state.final_check_all 不動即可。"""
    def _on_book(symbol: str, bids: list, asks: list, is_continuous: bool = False):
        limit_up = limit_ups.get(symbol)
        if not limit_up or not bids:
            return
        bid1_price = _pick(bids[0], "price")
        bid1_size = int(_pick(bids[0], "size") or 0)
        # 只認漲停價那一層 (避開開盤市價列 price=0;集合競價期漲停鎖死股 bid1 即漲停)
        if bid1_price and bid1_price >= limit_up - 0.001:
            state.update_max_bid(symbol, bid1_size)
    return _on_book


def make_on_book_handler(state: State, limit_ups: dict, cfg: Config,
                         unsub_ref: dict):
    """建 on_book callback — closure 捕獲 state / limit_ups / unsub_ref。

    unsub_ref = {"fn": None or callable(symbol)} — subscriber 建好後由 caller 填
    subscriber.request_unsubscribe。unmark 成功時呼叫，讓被淘汰股立即退訂。

    邏輯 (user 定案 2026-07-09; 2026-08-13 量減半改批次判):
    - Mark: 只有委買單 (asks 全空) 且委買一 == 漲停價
    - Unmark (永久淘汰 + 退訂): 委賣一出現單子，或委買一價格 != 漲停 (全程逐 tick)
    - bid 量: 8:30–PRE_ORDER_TIME (08:59:58) 每 tick 記錄 max+last;
      量減半淘汰**不再逐 tick 判** — 08:59:58 預掛前由 runner 呼叫
      state.final_check_all() 一次性批次判,判完清單即定案
    - PRE_ORDER_TIME 之後: 量不再影響;賣單/跌下漲停 unmark 照常跑
      (→ runner 會同步撤該檔預掛單)
    """
    from datetime import time as _time_cls
    po_hh, po_mm, po_ss = map(int, cfg.pre_order_time.split(":"))
    pre_order_time = _time_cls(po_hh, po_mm, po_ss)

    def _unsubscribe(symbol: str):
        fn = unsub_ref.get("fn")
        if fn:
            try:
                fn(symbol)
            except Exception as e:
                logger.warning(f"[filter] 退訂 {symbol} 失敗: {e}")

    # 「開盤即鎖」判定 — 記錄哪些 symbol 已看過第一筆真實報價 (委買非空)。
    # 空簿 tick (訂閱瞬間的空 snapshot) 不算首筆。每 symbol 只綁一個 socket，
    # 同 symbol tick 序列單執行緒，closure set 安全。
    seen_first_quote: set = set()

    def _on_book(symbol: str, bids: list, asks: list, is_continuous: bool = False):
        # 永久淘汰的股票 — 退訂生效前的殘餘 tick 直接略過
        if state.is_discarded(symbol):
            return
        # 沒漲停價資料就跳過 (universe.fetch_limit_ups 抓不到那批)
        limit_up = limit_ups.get(symbol)
        if not limit_up:
            return

        # 已開盤 → filter 退場,盤中 mark/unmark 交給 trader/monitor (別再拿盤中盤面判)。
        # 開盤訊號用「交易所權威旗標」,不是拿 bids[0] 反推 (2026-08-27 8105/1312: 開盤市價列
        # bids[0]=0 被誤判成委買一跌破漲停 → 誤撤預掛 P):
        #   主: 該檔首筆真成交 (isOpen) 已到 → runner/trader on_trade 已呼叫 state.mark_opened
        #       (成交與 book 同一 socket、成交永遠先於開盤後 book,故 book 進來時旗標已立)
        #   底線: book 已逐筆 (isContinuous) → 開盤零成交鎖死股沒有 isOpen 時接手,順手立旗標
        if is_continuous:
            state.mark_opened(symbol)
        if state.is_opened(symbol):
            return

        # 解析 bid1 / ask1 (兼容 dict 跟 object)
        bid1_price = _pick(bids[0], "price") if bids else 0.0
        bid1_size = int(_pick(bids[0], "size") or 0) if bids else 0
        ask1_price = _pick(asks[0], "price") if asks else 0.0
        ask1_size = int(_pick(asks[0], "size") or 0) if asks else 0
        # 「只有委買單」= asks 每一檔都沒單
        asks_empty = all(int(_pick(a, "size") or 0) == 0 for a in asks) if asks else True

        # 這筆是不是該股的「第一筆真實報價」(委買非空才算)
        is_first_quote = bool(bids) and symbol not in seen_first_quote
        if bids:
            seen_first_quote.add(symbol)

        if not state.is_marked(symbol):
            # Mark 條件: 只有委買單 + bid1 == 漲停 (浮點寬鬆)
            if bids and bid1_price >= limit_up - 0.001 and asks_empty:
                if state.mark(symbol, bid1_price, bid1_size, limit_up,
                              first_tick=is_first_quote):
                    tag = " [開盤即鎖]" if is_first_quote else ""
                    logger.info(f"✓ MARK {symbol} @ {bid1_price} × {bid1_size} 張 "
                                f"(漲停 {limit_up}) 無賣單{tag}")
            return

        # === 已 marked → unmark 檢查 (unmark 一律永久淘汰 + 退訂) ===

        # Unmark 條件 1: 委賣出現單子
        if not asks_empty:
            if state.unmark_ask_appeared(symbol, ask1_price, ask1_size):
                logger.info(f"✗ UNMARK {symbol} 出現賣單 @ {ask1_price} × {ask1_size} → 淘汰+退訂")
                _unsubscribe(symbol)
            return

        # Unmark 條件 2: 委買一價格不是漲停
        if bid1_price < limit_up - 0.001:
            if state.unmark_bid_below_limit(symbol, bid1_price, limit_up):
                logger.info(f"✗ UNMARK {symbol} 委買一跌下漲停 {bid1_price} < {limit_up} → 淘汰+退訂")
                _unsubscribe(symbol)
            return

        # bid 量記錄 — 08:59:58 (預掛) 前每 tick 記 max+last;量減半淘汰改由
        # runner 在 08:59:58 預掛前呼叫 state.final_check_all() 一次性批次判
        # (2026-08-13 定案,取代原 08:59:00 起的逐 tick 判)
        if datetime.now().time() < pre_order_time:
            state.update_max_bid(symbol, bid1_size)
        # 08:59:58 之後: 名單凍結,量不再影響 (預掛單已出;賣單/跌下漲停 unmark 照常在上面跑)

    return _on_book


def _pick(o, k):
    if isinstance(o, dict):
        return o.get(k)
    return getattr(o, k, None)


# ─── 主控 (等到 END_TIME 期間顯示 stats) ─────────────────────

def wait_until_end_time(state: State, cfg: Config):
    """Sleep 到 END_TIME，期間每 30s 印一次 stats。
    (bid 減半 final check 的時段切換由 handler 內部按 FINAL_CHECK_START 判斷。)
    """
    import time as _time

    end_hh, end_mm, end_ss = map(int, cfg.end_time.split(":"))
    now = datetime.now()
    end_dt = now.replace(hour=end_hh, minute=end_mm, second=end_ss, microsecond=0)
    if end_dt <= now:
        # END_TIME 已過 → 加一天 (跨日 / 已過)，但 script 意圖是同日
        logger.warning(f"[main] END_TIME {cfg.end_time} 已過，script 立即結束")
        return

    logger.info(f"[main] 目標結束時間: {end_dt.strftime('%H:%M:%S')} "
                f"(final check 從 {cfg.final_check_start} 起)")

    last_stats = 0
    while True:
        now = datetime.now()
        remaining = (end_dt - now).total_seconds()
        if remaining <= 0:
            return

        # 每 30s 印 stats
        if _time.time() - last_stats >= 30:
            s = state.stats()
            logger.info(f"[stats] 剩 {int(remaining)}s | 當前標記 {s['currently_marked']} 檔 / "
                        f"淘汰 {s['total_unmark_events']} 檔")
            last_stats = _time.time()

        _time.sleep(1)


# ─── 輸出 ────────────────────────────────────────────────────

def write_output(state: State, cfg: Config) -> Path:
    output_dir = Path(__file__).parent / "output"
    output_dir.mkdir(exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    out_file = output_dir / f"{today}.json"

    snapshot = state.snapshot()
    stats = state.stats()
    payload = {
        "snapshot_at": datetime.now().isoformat(timespec="seconds"),
        "end_time": cfg.end_time,
        "universe": cfg.universe,
        "stats": stats,
        "marked": snapshot,   # 最終仍在 marked 的 symbol + 各自 history
    }

    with out_file.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info(f"[output] 寫入 {out_file} — 最終 {len(snapshot)} 檔標記")
    return out_file


# ─── main ────────────────────────────────────────────────────

def main():
    try:
        cfg = load_config()
    except Exception as e:      # ConfigError 等 (原 SystemExit 已改為可捕捉例外)
        print(e, file=sys.stderr)
        return 1

    if cfg.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    sdk, accounts = login_fubon(cfg)

    universe = get_universe(sdk, cfg.universe)
    if not universe:
        logger.error("[main] Universe 空，無法繼續")
        return 1

    # 開盤前拿漲停價 (每檔一次)
    logger.info(f"[main] 開始抓 {len(universe)} 檔漲停價 (可能要幾分鐘)...")
    limit_ups = fetch_limit_ups(sdk, universe)
    if not limit_ups:
        logger.error("[main] 沒抓到任何漲停價，無法比對")
        return 1

    state = State(bid_drop_ratio=cfg.bid_drop_ratio)
    unsub_ref = {"fn": None}
    on_book = make_on_book_handler(state, limit_ups, cfg, unsub_ref)

    # Recorder — 存 raw books + trades tick 到 JSONL
    from recorder import TickRecorder
    output_dir = Path(__file__).parent / "output"
    output_dir.mkdir(exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    ticks_file = output_dir / f"{today}_ticks.jsonl"
    recorder = TickRecorder(ticks_file)

    from subscriber import Subscriber
    sub = Subscriber(
        sdk=sdk,
        universe=[s for s in universe if s in limit_ups],   # 只監控有漲停價的
        on_book=on_book,
        login_cfg=cfg,
        recorder=recorder,
        batch_size=cfg.batch_size,
        batch_rotate_sec=cfg.batch_rotate_sec,
        socket_count=cfg.socket_count,
        debug=cfg.debug,
    )
    sub.start()
    unsub_ref["fn"] = sub.request_unsubscribe   # handler unmark 時退訂

    # === 篩選階段: 8:25 - 9:00 ===
    try:
        wait_until_end_time(state, cfg)
    except KeyboardInterrupt:
        logger.warning("[main] 篩選階段 Ctrl-C 中止")
        sub.stop()
        recorder.close()
        write_output(state, cfg)
        return 0

    # 寫篩選結果 JSON (即使進入 trading，也留一份 snapshot)
    out_file = write_output(state, cfg)
    final = state.snapshot()
    watchlist = [row["symbol"] for row in final]
    print(f"\n=== 篩選結束 — 最終標記 {len(watchlist)} 檔 ===")
    for sym in watchlist:
        print(f"  {sym}")

    if not watchlist:
        logger.info("[main] 篩選結果空 — 不進入 trading 階段")
        sub.stop()
        recorder.close()
        return 0

    # 9:00 轉場: 只留 marked 股票 — 其餘全退訂，watchlist 加訂 trades
    sub.keep_only(watchlist)
    sub.subscribe_trades_for(watchlist)

    # === Trading 階段: 9:00 - TRADING_END_TIME (純監控，不下單) ===
    logger.info("[main] === 進入 trading 階段 (純監控) ===")
    from trader import Trader

    trader = Trader(
        watchlist=watchlist,
        limit_ups=limit_ups,
        cfg=cfg,
        recorder=recorder,
        state=state,
        unsub_fn=sub.request_unsubscribe,
    )

    # handoff — subscriber 保持連線，換 handler 給 trader
    sub.set_handlers(on_book=trader.on_book, on_trade=trader.on_trade)
    trader.start()

    try:
        _wait_until(cfg.trading_end_time, trader, cfg)
    except KeyboardInterrupt:
        logger.warning("[main] Trading 階段 Ctrl-C 中止")

    logger.info("[main] 收盤時間到 — trader stop")
    trader.stop()
    sub.stop()
    recorder.close()

    # 最終 summary
    summary = trader.summary()
    summary_file = output_dir / f"{today}_trading_summary.json"
    import json as _json
    with summary_file.open("w", encoding="utf-8") as f:
        _json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f"\n=== Trading 結束 ===")
    print(f"  Watchlist: {summary['watchlist_total']}")
    print(f"  追蹤中: {summary['n_tracking']}")
    print(f"  撤單: {summary['n_pulled']}")
    print(f"  第一盤淘汰: {summary['n_first_failed']}")
    print(f"\n寫入:")
    print(f"  {out_file}")
    print(f"  {summary_file}")
    return 0


def _wait_until(target_hhmmss: str, trader, cfg):
    """Sleep 到 target_hhmmss，每 60s 印 trader 進度."""
    import time as _time
    hh, mm, ss = map(int, target_hhmmss.split(":"))
    now = datetime.now()
    target = now.replace(hour=hh, minute=mm, second=ss, microsecond=0)
    if target <= now:
        logger.warning(f"[main] TRADING_END_TIME {target_hhmmss} 已過")
        return

    last_progress = 0
    while True:
        now = datetime.now()
        remaining = (target - now).total_seconds()
        if remaining <= 0:
            return
        if _time.time() - last_progress >= 60:
            s = trader.summary()
            logger.info(f"[trading progress] 剩 {int(remaining/60)}min | "
                        f"追蹤 {s['n_tracking']} / 撤單 {s['n_pulled']} / "
                        f"第一盤淘汰 {s['n_first_failed']}")
            last_progress = _time.time()
        _time.sleep(1)


if __name__ == "__main__":
    sys.exit(main())
