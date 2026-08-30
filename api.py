"""API endpoints — 給前端 3 個分頁讀資料."""
from __future__ import annotations

import json
import os
from datetime import datetime, time as _time
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

import pnl
import symbol_budget
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
    """手動 stop (停止監控 + 關 WS;**不平倉** — 持倉/委託不動,平倉用 /trading/close_all)."""
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


class FilterRemoveReq(BaseModel):
    symbol: str


@router.post("/filter/remove")
def filter_remove(req: FilterRemoveReq):
    """盤前 (篩選階段) 手動剔除一檔 — 從標記清單移除 + 永久淘汰,08:59:58 預掛不會下。
    使用者自行判斷 (例: 不想搶某檔)。已淘汰/未標記 → 回 400。"""
    r = Runner.get()
    if not r.state:
        raise HTTPException(400, "篩選尚未啟動")
    removed = r.state.unmark_manual(str(req.symbol or "").strip())
    if not removed:
        raise HTTPException(400, f"{req.symbol} 不在標記清單中 (可能已淘汰或尚未標記)")
    return {"ok": True, "removed": removed}


# ─── 個股金額覆寫 (特定股票用專屬下單金額,取代全域每檔金額) ──────────


@router.get("/symbol-budget")
def symbol_budget_list():
    """全部個股金額覆寫 (排序)。"""
    data = symbol_budget.load()
    return {"budgets": [{"symbol": s, "amount": a} for s, a in sorted(data.items())]}


class SymbolBudgetReq(BaseModel):
    symbol: str
    amount: float


@router.post("/symbol-budget")
def symbol_budget_upsert(req: SymbolBudgetReq):
    """新增或覆寫某檔專屬下單金額 (元);同步進 session,篩選清單含此檔即依此金額下。"""
    try:
        symbol_budget.upsert(req.symbol, req.amount)
    except ValueError as e:
        raise HTTPException(400, str(e))
    Runner.get().session.set_symbol_budgets(symbol_budget.load())
    return {"ok": True}


class SymbolBudgetDelReq(BaseModel):
    symbol: str


@router.post("/symbol-budget/delete")
def symbol_budget_delete(req: SymbolBudgetDelReq):
    """刪除某檔專屬金額 (之後該檔改回全域參數規則)。"""
    try:
        removed = symbol_budget.remove(req.symbol)
    except ValueError as e:
        raise HTTPException(400, str(e))
    Runner.get().session.set_symbol_budgets(symbol_budget.load())
    return {"ok": True, "removed": removed}


@router.get("/t30")
def get_t30():
    """T30 禁單名單 (全額交割 / 需預收款券) — 數量 + 檔案 meta + 排序清單。
    盤前檢視今天系統解析到哪些股票不能下單。"""
    r = Runner.get()
    syms = sorted(r.session.untradable) if r.session else []
    return {
        "count": len(syms),
        "symbols": syms,
        "meta": getattr(r, "_t30_meta", None) or {},
    }


@router.get("/avg-volume")
def get_avg_volume(symbol: Optional[str] = None):
    """月均量篩選診斷 (風控③) — 檔案健康 (是否今天產出/幾檔/門檻) + 今天實際剔除/保留數
    + 抽查個股月均量 (?symbol=2330)。盤前檢視月均量計算是否正確執行。graceful,不 raise。"""
    import avg_volume as _av
    r = Runner.get()
    path = os.environ.get("AVG_VOLUME_FILE",
                          str(Path(__file__).parent / "input" / "avg_volume.json"))
    data, meta = _av.load(path)                        # fresh 讀檔 — 免依賴 run 是否跑過
    is_today = bool(meta.get("mtime_date") == datetime.now().strftime("%Y-%m-%d"))
    threshold = r.cfg.avg_volume_min_lots if r.cfg is not None else None
    if threshold is None:
        try:
            threshold = float(os.environ.get("AVG_VOLUME_MIN_LOTS", "500"))
        except ValueError:
            threshold = None
    av = getattr(r, "_avgvol", None) or {}            # 今天實際跑的結果 (runner 保存)
    lookup = None
    if symbol:
        s = symbol.strip()
        lookup = {"symbol": s, "lots": data.get(s)}   # lots=None → 檔內查無 (被剔或非母體)
    return {
        "exists": meta["exists"],
        "count": meta["count"],
        "mtime_date": meta.get("mtime_date"),
        "stale": meta["stale"],
        "is_today": is_today,
        "threshold": threshold,
        "ran": bool(av.get("ran")),
        "reason": av.get("reason"),
        "universe_before": av.get("universe_before"),
        "kept": av.get("kept"),
        "dropped": av.get("dropped"),
        "dropped_sample": av.get("dropped_sample") or [],
        "lookup": lookup,
    }


@router.get("/avg-volume/full")
def get_avg_volume_full(authorization: Optional[str] = Header(None)):
    """全量月均量 map (Stage A5) — 給 IDC 端每日 08:05 拉取。直接讀檔 serve。
    缺檔回 {"exists": false} 不 500 (首次部署/timer 沒跑過屬正常);壞檔讓它 500 (要大聲,
    「讀不了」不能偽裝成「沒有」)。
    Auth: 環境有 AVGVOL_TOKEN 才驗 Bearer (每次 request 讀 os.environ,免重啟語意);
    沒設 = 開放 (資料是公開行情衍生)。"""
    token = os.environ.get("AVGVOL_TOKEN")
    if token and authorization != f"Bearer {token}":
        raise HTTPException(401, "unauthorized")
    path = Path(os.environ.get("AVG_VOLUME_FILE",
                               str(Path(__file__).parent / "input" / "avg_volume.json")))
    if not path.is_file():
        return {"exists": False}
    raw = json.loads(path.read_text(encoding="utf-8"))
    # 新包裝形 (有 avg_lots+date);過渡期舊平面形照 serve,date/generated_at 給 None
    if isinstance(raw, dict) and "avg_lots" in raw and "date" in raw:
        lots = raw.get("avg_lots") or {}
        date, generated_at = raw.get("date"), raw.get("generated_at")
    else:
        lots, date, generated_at = (raw if isinstance(raw, dict) else {}), None, None
    return {"exists": True, "date": date, "generated_at": generated_at,
            "count": len(lots), "avg_lots": lots}


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


class AbandonReq(BaseModel):
    symbol: str


@router.post("/trader/abandon")
def trader_abandon(req: AbandonReq):
    """取消追蹤一檔 (2026-08-12): 停止該檔一切自動化 — 撤 pending、不再進場、
    **不再自動出場**;持倉使用者自負。某檔狂下單失敗時的單檔煞車。"""
    found = Runner.get().abandon_symbol(req.symbol)
    if not found:
        raise HTTPException(400, f"{req.symbol} 不在今日追蹤/交易紀錄中")
    return {"ok": True}


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


@router.get("/trading/spending")
def trading_spending():
    """花費表 (只看實際成交): 各檔花費/超額 + 總花費/總預算超額。"""
    return Runner.get().session.spending_summary()


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


class OvernightSymbolReq(BaseModel):
    symbol: str


@router.post("/trading/overnight/add")
def trading_overnight_add(req: OvernightSymbolReq):
    """手動加入一檔隔日賣標的追蹤 (張數以券商庫存為準,盤中即時收五檔)。"""
    try:
        added = Runner.get().track_overnight(req.symbol)
    except (ValueError, RuntimeError) as e:
        raise HTTPException(400, str(e))
    return {"ok": True, "added": added}


@router.post("/trading/overnight/remove")
def trading_overnight_remove(req: OvernightSymbolReq):
    """從隔日賣清單移除一檔 (誤加可刪)。"""
    try:
        removed = Runner.get().untrack_overnight(req.symbol)
    except (ValueError, RuntimeError) as e:
        raise HTTPException(400, str(e))
    return {"ok": True, "removed": removed}


# ── 帳務台帳 (每日手動損益,獨立於交易流程) ─────────────────────────


@router.get("/pnl")
def pnl_list():
    """全部記錄 (舊→新,含累積損益) + 總計。"""
    records = pnl.load()
    total = records[-1]["cumulative"] if records else 0.0
    return {"records": records, "total": total}


class PnlUpsertReq(BaseModel):
    date: str
    pnl: float
    note: str = ""


@router.post("/pnl")
def pnl_upsert(req: PnlUpsertReq):
    """新增或覆寫某一日損益 (同日期 = 修改)。"""
    try:
        pnl.upsert(req.date, req.pnl, req.note)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"ok": True}


class PnlDeleteReq(BaseModel):
    date: str


@router.post("/pnl/delete")
def pnl_delete(req: PnlDeleteReq):
    """刪除某一日記錄。"""
    try:
        removed = pnl.remove(req.date)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"ok": True, "removed": removed}
