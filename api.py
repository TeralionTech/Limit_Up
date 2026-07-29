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


# ─── 交易 (模擬/真實執行頁) ──────────────────────────────────
# pfx 憑證用 base64 JSON 傳 (避免 multipart 需額外裝 python-multipart)


class TradingConnectReq(BaseModel):
    account_id: str
    password: str
    pfx_password: str = ""
    is_test: bool = False
    pfx_b64: str            # .pfx 檔案內容 base64
    pfx_filename: str = "trading.pfx"


@router.post("/trading/connect")
def trading_connect(req: TradingConnectReq):
    """存憑證 + 背景連線券商。前端輪詢 /api/trading/status 看結果。
    帳密只進記憶體 (連線用完即丟);.pfx 存 certs/uploaded/ (chmod 600)。"""
    import base64
    import os
    import re
    from pathlib import Path

    r = Runner.get()
    if r.session.connecting:
        raise HTTPException(400, "連線進行中")

    # 存 pfx (檔名消毒,固定放 certs/uploaded/)
    safe_name = re.sub(r"[^A-Za-z0-9._-]", "_", req.pfx_filename) or "trading.pfx"
    upload_dir = Path(__file__).parent.parent.parent / "certs" / "uploaded"
    if not upload_dir.parent.exists():                     # 非 VPS 佈局 → 存 app 目錄下
        upload_dir = Path(__file__).parent / "certs_uploaded"
    upload_dir.mkdir(parents=True, exist_ok=True)
    pfx_path = upload_dir / safe_name
    try:
        pfx_path.write_bytes(base64.b64decode(req.pfx_b64))
        os.chmod(pfx_path, 0o600)
    except Exception as e:
        raise HTTPException(400, f"憑證存檔失敗: {e}")

    output_dir = Path(__file__).parent / "output"
    r.session.connect_async(req.account_id.strip(), req.password,
                            str(pfx_path), req.pfx_password,
                            req.is_test, output_dir)
    return {"status": "connecting"}


@router.get("/trading/status")
def trading_status():
    return Runner.get().session.status()


@router.post("/trading/disconnect")
def trading_disconnect():
    Runner.get().session.disconnect()
    return {"ok": True}


class TradingModeReq(BaseModel):
    mode: str    # "sim" / "real"


@router.post("/trading/mode")
def trading_mode(req: TradingModeReq):
    try:
        Runner.get().session.set_mode(req.mode)
    except (ValueError, RuntimeError) as e:
        raise HTTPException(400, str(e))
    return {"ok": True, "mode": req.mode}


class TradingParamsReq(BaseModel):
    total_budget: Optional[float] = None
    per_symbol_budget: Optional[float] = None
    sizing_mode: Optional[str] = None       # "budget" / "fixed_lots"
    fixed_lots: Optional[int] = None


@router.post("/trading/params")
def trading_params(req: TradingParamsReq):
    try:
        Runner.get().session.set_params(req.total_budget, req.per_symbol_budget,
                                        req.sizing_mode, req.fixed_lots)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"ok": True}


class TradingArmReq(BaseModel):
    armed: bool


@router.post("/trading/arm")
def trading_arm(req: TradingArmReq):
    """kill switch — 開啟前 pre-flight (real mode + 連線健康 + 預算已設)。"""
    try:
        Runner.get().session.set_armed(req.armed)
    except RuntimeError as e:
        raise HTTPException(400, str(e))
    return {"ok": True, "armed": req.armed}


@router.post("/trading/close_all")
def trading_close_all():
    """🚨 緊急全平: 撤所有 pending + 市價賣出全部持倉。"""
    try:
        sold = Runner.get().session.close_all()
    except RuntimeError as e:
        raise HTTPException(400, str(e))
    return {"ok": True, "sold_symbols": sold}


@router.get("/trading/orders")
def trading_orders():
    """委託總表 (真實模式) — 新的在前。前端委託狀態表 + 右鍵刪單用。"""
    return {"orders": Runner.get().session.get_orders()}


class CancelOrderReq(BaseModel):
    order_no: str


@router.post("/trading/cancel_order")
def trading_cancel_order(req: CancelOrderReq):
    """手動刪單 (前端右鍵選單)。"""
    try:
        Runner.get().session.cancel_order_by_no(req.order_no)
    except (ValueError, RuntimeError) as e:
        raise HTTPException(400, str(e))
    return {"ok": True}


@router.get("/trading/overnight")
def trading_overnight():
    """隔日賣標的 (昨天買到未出場的持倉) — 含賣出狀態 + 最新五檔。"""
    r = Runner.get()
    rows = r.session.overnight_status()
    for row in rows:
        snap = r.subscriber.get_latest_snapshot(row["symbol"]) if r.subscriber else None
        row["books"] = (snap or {}).get("books")
        row["last_trade"] = (snap or {}).get("last_trade")
    return {"overnight": rows}


class OvernightSkipReq(BaseModel):
    symbol: str
    skip: bool = True


@router.post("/trading/overnight/skip")
def trading_overnight_skip(req: OvernightSkipReq):
    """隔日賣標的「不要賣 / 恢復賣出」。skip=True 暫停 (已下賣單則撤掉)。"""
    try:
        Runner.get().session.set_overnight_skip(req.symbol, req.skip)
    except (ValueError, RuntimeError) as e:
        raise HTTPException(400, str(e))
    return {"ok": True}
