"""API endpoints — 給前端 3 個分頁讀資料."""
from __future__ import annotations

from datetime import datetime, time as _time
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from runner import Runner

router = APIRouter(prefix="/api", tags=["hit-limit-up"])


# ─── 全域狀態 ────────────────────────────────────────────────


@router.get("/status")
def get_status():
    """整體狀態 — 給頁首用 (phase / progress / 有沒 error 等)."""
    r = Runner.get()
    return r.get_status()


@router.post("/start")
def start_now():
    """手動立刻觸發 runner (測試用；正式跑靠 APScheduler 8:00)."""
    r = Runner.get()
    if r.is_running():
        return {"ok": False, "reason": "already_running"}
    ok = r.start()
    return {"ok": ok}


@router.post("/stop")
def stop_now():
    """手動 stop (會平倉 + 關 WS)."""
    r = Runner.get()
    r.stop()
    return {"ok": True}


# ─── 分頁 1: 篩選頁 ─────────────────────────────────────────


@router.get("/filter/progress")
def get_filter_progress():
    """抓漲停資料進度 + phase."""
    r = Runner.get()
    p = r._limit_up_progress
    return {
        "phase": r.phase.value,
        "limit_up_done": p.get("done", 0),
        "limit_up_total": p.get("total", 0),
        "limit_up_ok": p.get("ok", 0),
        "limit_up_fail": p.get("fail", 0),
        "universe_size": len(r.universe),
        "recorder_tick_count": r.recorder.count() if r.recorder else 0,
        "tick_stats": r.subscriber.get_tick_stats() if r.subscriber else {},
        "filter_stats": r.state.stats() if r.state else {},
    }


@router.get("/filter/watchlist")
def get_watchlist():
    """當前標記 + 丟棄清單。first_tick_marked = marked 中「開盤即鎖」(首筆真實報價就漲停) 子集."""
    r = Runner.get()
    if not r.state:
        return {"marked": [], "first_tick_marked": [], "discarded": []}
    return {
        "marked": r.state.get_marked_list(),
        "first_tick_marked": r.state.get_first_tick_marked_list(),
        "discarded": r.state.get_discarded_list(),
    }


# ─── 分頁 2: 股票資料頁 ─────────────────────────────────────


@router.get("/tick/{symbol}")
def get_tick(symbol: str):
    """查 symbol 最新 books + last trade + 試撮判斷."""
    r = Runner.get()
    if not r.subscriber:
        raise HTTPException(400, "subscriber 未啟動")
    snap = r.subscriber.get_latest_snapshot(symbol)
    # 試撮判斷: 現在時間介於 8:30-9:00 或 13:20-13:30 都是試撮
    now = datetime.now().time()
    pre_open = _time(8, 30) <= now < _time(9, 0)
    pre_close = _time(13, 20) <= now < _time(13, 30)
    snap["is_pre_match"] = pre_open or pre_close
    snap["pre_match_kind"] = ("pre_open" if pre_open else
                               "pre_close" if pre_close else None)
    snap["limit_up"] = r.limit_ups.get(symbol)
    snap["is_watched"] = r.state.is_marked(symbol) if r.state else False
    snap["is_discarded"] = r.state.is_discarded(symbol) if r.state else False
    return snap


# ─── 分頁 3: 模擬執行 ────────────────────────────────────────


@router.get("/trader/summary")
def get_trader_summary():
    """Trader 兩區塊資料: first_stage (第一盤檢查) + tracking (盤中追蹤)."""
    r = Runner.get()
    if not r.trader:
        return {"trader_active": False}
    summary = r.trader.summary()
    summary["trader_active"] = True
    return summary


class TraderParams(BaseModel):
    first_trade_min_lots: Optional[int] = None


@router.get("/trader/params")
def get_trader_params():
    """目前 trader 參數 (前端顯示用)."""
    r = Runner.get()
    if r.cfg is not None:
        v = r.cfg.first_trade_min_lots
    else:
        v = r._param_overrides.get("first_trade_min_lots", 10)
    return {"first_trade_min_lots": v}


@router.post("/trader/params")
def set_trader_params(p: TraderParams):
    """調 trader 參數 — runtime 立即生效 (trader 共用同一 cfg 物件)."""
    r = Runner.get()
    applied = {}
    if p.first_trade_min_lots is not None:
        if p.first_trade_min_lots < 1:
            raise HTTPException(400, "first_trade_min_lots 必須 >= 1")
        r.set_param_override("first_trade_min_lots", p.first_trade_min_lots)
        applied["first_trade_min_lots"] = p.first_trade_min_lots
    return {"ok": True, "applied": applied}
