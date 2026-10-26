"""API endpoints — 給前端 3 個分頁讀資料."""
from __future__ import annotations

import functools
import json
import logging
import os
import socket
import subprocess
from datetime import datetime, time as _time
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

import pnl
import symbol_budget
import trading_config
from config import ConfigError, load_config
from runner import Runner

logger = logging.getLogger(__name__)

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


@router.get("/marked-snapshot")
def marked_snapshot():
    """交易節點 (ROLE=node) 08:59:50 來拉的 marked 快照 (見中心過濾架構)。"""
    return Runner.get().marked_snapshot()


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


class AutoConnectReq(BaseModel):
    is_test: bool = False


@router.post("/trading/auto_connect")
def trading_auto_connect(req: Optional[AutoConnectReq] = None):
    """central 每日 08:30 用 (2026-09-07): 讀**機上** .env 的主帳號憑證
    (FUBON_ACCOUNT_ID / PASSWORD / PFX_PATH / PFX_PASSWORD) 自己連線券商 —
    帳密不經網路,central 零機密。pfx 已在機上,不需 /trading/connect 的 base64 落檔。
    非同步: 立刻回 connecting,由 /api/trading/status 輪詢 connected/healthy/connect_error。
    mode **不**在這裡切 — central 另打 /api/trading/mode,讓「切真實」是獨立可稽核的一步。
    body {is_test: bool=false} (可省略)。"""
    r = Runner.get()
    # hub 只篩選不交易：它 .env 的 FUBON_* 是行情帳號，不能被拿去開交易 session (2026-10-13 審查)
    if os.environ.get("ROLE", "standalone").split("#", 1)[0].strip().lower() == "hub":
        raise HTTPException(400, "這台是 hub（只篩選不交易），不提供 auto_connect")
    if r.session.connecting:
        raise HTTPException(400, "連線進行中")
    # 冪等：已連線且健康 → 不再建第二個 client (舊版會把健康的 broker 換掉、交換空窗漏回報)；
    # 交易 WS 斷線、SDK 自動重連中 → 409 讓中控等，不要並行再 login 出孤兒 session
    broker = getattr(r.session, "broker", None)
    if broker is not None:
        if getattr(broker, "connected", False) and getattr(broker, "healthy", False):
            return {"status": "already_connected",
                    "account_masked": (getattr(broker, "account_no", "") or "")[:3] + "***"}
        lock = getattr(broker, "_relogin_lock", None)
        if lock is not None and getattr(lock, "locked", lambda: False)():
            raise HTTPException(409, "交易 WS 自動重連進行中，請稍後再查 /api/trading/status")
    try:
        cfg = load_config()
    except ConfigError as e:
        raise HTTPException(400, str(e))
    is_test = bool(req.is_test) if req is not None else False
    output_dir = Path(__file__).parent / "output"
    try:
        r.session.connect_async(cfg.account_id, cfg.password, cfg.pfx_path,
                                cfg.pfx_password, is_test, output_dir)
    except RuntimeError as e:          # 兩個 auto_connect 同時進來 → session 自己也擋「連線進行中」
        raise HTTPException(400, str(e))
    return {"status": "connecting", "account_masked": cfg.account_id[:3] + "***"}


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
    with trading_config.CONFIG_LOCK:
        session = Runner.get().session
        try:
            session.set_mode(req.mode)
        except (ValueError, RuntimeError) as e:
            raise HTTPException(400, str(e))
        # 2026-10-13: 落檔，重啟後套回 (armed 不存)；寫檔失敗不影響這次切換
        trading_config.persist(session)
    return {"ok": True, "mode": req.mode}


class TradingParamsReq(BaseModel):
    total_budget: Optional[float] = None
    per_symbol_budget: Optional[float] = None
    sizing_mode: Optional[str] = None       # "budget" / "fixed_lots"
    fixed_lots: Optional[int] = None


@router.post("/trading/params")
def trading_params(req: TradingParamsReq):
    with trading_config.CONFIG_LOCK:
        session = Runner.get().session
        try:
            session.set_params(req.total_budget, req.per_symbol_budget,
                               req.sizing_mode, req.fixed_lots)
        except ValueError as e:
            raise HTTPException(400, str(e))
        trading_config.persist(session)     # 2026-10-13: 跨重啟保留 (見 trading_config.py)
    return {"ok": True}


@router.post("/trading/pull_exit")
def trading_pull_exit(req: dict):
    """Strict validation before mutation; protected by the existing API middleware."""
    if "mode" not in req:
        raise HTTPException(400, "mode 必填")
    r = Runner.get()
    with trading_config.CONFIG_LOCK:
        try:
            if r.trader is not None:
                cfg = r.trader.configure_pull_exit(**req)
            else:
                cfg = r.session.set_pull_exit(**req)
        except (TypeError, ValueError) as e:
            raise HTTPException(400, str(e))
        saved = trading_config.persist(r.session)
    return {"ok": True, "pull_exit": cfg, "persisted": saved}


class TradingArmReq(BaseModel):
    armed: bool
    # True = 「開始隔日賣」: arm 但不搶今天的單 (不預掛/不盲送/不市價追);False = 明確改為正常搶單;
    # None (舊呼叫 / 遠端 {armed:true}) = 正常 arm,**但不會把已開啟的隔日賣模式悄悄升級成搶單**
    overnight_only: Optional[bool] = None


@router.post("/trading/arm")
def trading_arm(req: TradingArmReq):
    """kill switch — 開啟前 pre-flight (real mode + 連線健康 + 預算已設;overnight_only 不看預算)。"""
    s = Runner.get().session
    if (req.armed and req.overnight_only is None
            and getattr(s, "armed", False) and getattr(s, "entry_enabled", True) is False):
        # 操作者選了隔日賣模式;沒帶 overnight_only 的 {armed:true} (舊前端 / central 自動 arm) 不得改變它 —
        # 要搶單請明確送 overnight_only=false (前端「⚡ 改為搶單」)
        logger.warning("[api] /trading/arm {armed:true} 未帶 overnight_only,節點在隔日賣模式 → 維持不變 (不升級成搶單)")
        return {"ok": True, "armed": True, "overnight_only": True, "unchanged": True}
    try:
        if req.overnight_only:
            s.set_armed(req.armed, overnight_only=True)
        else:
            s.set_armed(req.armed)
    except RuntimeError as e:
        raise HTTPException(400, str(e))
    out = {"ok": True, "armed": req.armed}
    if req.armed and req.overnight_only:
        out["overnight_only"] = True
    return out


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


# ─── central 彙整狀態 (2026-09-07, teralion-central poller 每 3s 打) ────────
# 一台 5 個 endpoint → 1 個;09:00 前後 session.status() 取 _lock (與下單同鎖),少打為妙。
# 薄彙整: 全部由 Runner / session 既有方法組成,不新增 state。
# 任何子項例外 → 該子項回 None (不讓整支 500 — central 只會看到「這台掛了」,反而看不出哪裡壞)。


@functools.lru_cache(maxsize=1)
def _app_version() -> str:
    """git short sha (central 顯示部署版本用);非 git 目錄 (node 是 tarball 部署) / 無 git → "0.1"。
    process 生命週期內不變 (deploy = restart) → 只算一次,poller 每 3s 打不會每次 spawn git。"""
    try:
        out = subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                             cwd=str(Path(__file__).parent),
                             capture_output=True, text=True, timeout=3)
        sha = (out.stdout or "").strip()
        return sha if out.returncode == 0 and sha else "0.1"
    except Exception:
        return "0.1"


def _safe(label: str, fn):
    """子項彙整保護: 例外 → None + warning (不 500)。"""
    try:
        return fn()
    except Exception as e:
        logger.warning(f"[central/status] {label} 例外: {e}")
        return None


def _central_role(cfg) -> str:
    # runner 尚未跑 (08:00 前 / 重啟後) cfg=None → 直接讀 env (同 config.py 規則),
    # 不呼叫 load_config (缺憑證會 raise)。marked_snapshot 同情境回 standalone,這裡對 central 要更準。
    if cfg is not None:
        return cfg.role
    return os.environ.get("ROLE", "standalone").strip().lower()


def _central_config(cfg) -> dict:
    if cfg is not None:
        return {
            "hub_url": cfg.hub_url,
            "hub_freeze_time": cfg.hub_freeze_time,
            "pre_order_time": cfg.pre_order_time,
            "cancel_pending_time": cfg.cancel_pending_time,
            "trading_end_time": cfg.trading_end_time,
        }
    env = os.environ.get           # 預設值與 config.load_config 一致
    return {
        "hub_url": env("HUB_URL", "").strip(),
        "hub_freeze_time": env("HUB_FREEZE_TIME", "08:59:50").strip(),
        "pre_order_time": env("PRE_ORDER_TIME", "08:59:58").strip(),
        "cancel_pending_time": env("CANCEL_PENDING_TIME", "13:23:00").strip(),
        "trading_end_time": env("TRADING_END_TIME", "13:24:00").strip(),
    }


def _central_runner(r) -> dict:
    s = r.get_status()             # /api/status 同源,取子集
    return {
        "phase": s["phase"],
        "is_running": s["is_running"],
        "error": s["error"],
        "watchlist_size": s["watchlist_size"],
        "limit_up_progress": s["limit_up_progress"],
        "recorder_tick_count": s["recorder_tick_count"],
    }


def _central_marked(r) -> dict:
    snap = r.marked_snapshot()     # /api/marked-snapshot 同源,裁掉每檔明細只留代號
    syms = [x["symbol"] for x in snap.get("symbols") or []]
    return {"final": bool(snap.get("final")), "ts": snap.get("ts"),
            "count": len(syms), "symbols": syms}


def _central_spending(session) -> dict:
    s = session.spending_summary()  # /api/trading/spending 同源,去 symbols 明細
    rows = s.get("symbols") or []
    return {
        "total_budget": s["total_budget"],
        "total_spent": s["total_spent"],
        "total_over": s["total_over"],
        "budget_breached": s["budget_breached"],
        "n_over_symbols": sum(1 for x in rows if x.get("over", 0) > 0),
    }


def _central_orders(session) -> dict:
    rows = session.get_orders()     # /api/trading/orders 同源,只計數
    status_of = [x.get("status") for x in rows]
    kind_of = [x.get("kind") for x in rows]
    return {
        "total": len(rows),
        "pending": status_of.count("pending"),
        "filled": status_of.count("filled"),
        "rejected": status_of.count("rejected"),
        "cancelled": status_of.count("cancelled"),
        "pre_limit": kind_of.count("pre_limit"),
        "market_buy": kind_of.count("market_buy"),
    }


def _central_overnight(session) -> dict:
    rows = session.overnight_status()   # /api/trading/overnight 同源 (不補五檔)
    # pending_sell = 還沒賣完且未被使用者 skip 的檔數 (09:00 開盤自動賣的待辦)
    pending = sum(1 for x in rows
                  if not x.get("skip") and int(x.get("sold_lots") or 0) < int(x.get("lots") or 0))
    return {"count": len(rows), "pending_sell": pending}


def _central_ready(session, trading: dict) -> dict:
    """複製 trading_session.set_armed 的 pre-flight **求值**,不真的 arm。
    block_reason 依 set_armed 檢查順序取第一個紅燈;全綠 = ""。
    trading = session.status() 的 dict (呼叫端已取過,避免再拿一次 _lock)。"""
    p = trading.get("params") or {}
    mode_real = trading.get("mode") == "real"
    connected = bool(trading.get("connected"))
    healthy = bool(trading.get("healthy"))
    total_budget = float(p.get("total_budget") or 0)
    sizing_mode = p.get("sizing_mode")
    fixed_lots = int(p.get("fixed_lots") or 0)
    per_symbol = float(p.get("per_symbol_budget") or 0)
    per_symbol_ok = fixed_lots > 0 if sizing_mode == "fixed_lots" else per_symbol > 0
    budget_set = total_budget > 0 and per_symbol_ok
    if not mode_real:
        block = "模擬模式"
    elif not (connected and healthy):
        block = "券商未連線或不健康"
    elif total_budget <= 0:
        block = "未設總預算"
    elif not per_symbol_ok:
        block = "未設每檔上限或張數"
    else:
        block = ""
    return {
        "mode_real": mode_real,
        "connected": connected,
        "healthy": healthy,
        "budget_set": budget_set,
        "armed": bool(trading.get("armed")),
        "is_live": bool(session.is_live()),
        "block_reason": block,
    }


@router.get("/central/status")
def central_status():
    """central poller 用的單一彙整 (形狀見計畫 A3 / 跨 agent 契約)。"""
    r = Runner.get()
    cfg = r.cfg
    session = r.session
    trading = _safe("trading", lambda: session.status())
    if trading is not None:
        trading = {k: v for k, v in trading.items() if k != "pull_exit"}
    return {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "role": _safe("role", lambda: _central_role(cfg)),
        "node_name": os.environ.get("NODE_NAME") or socket.gethostname(),
        "version": _app_version(),
        "config": _safe("config", lambda: _central_config(cfg)),
        "runner": _safe("runner", lambda: _central_runner(r)),
        "marked": _safe("marked", lambda: _central_marked(r)),
        "trading": trading,
        "spending": _safe("spending", lambda: _central_spending(session)),
        "orders": _safe("orders", lambda: _central_orders(session)),
        "overnight": _safe("overnight", lambda: _central_overnight(session)),
        "t30": _safe("t30", lambda: {"count": len(session.untradable)}),
        "ready": _safe("ready", lambda: _central_ready(session, trading if trading is not None
                                                        else session.status())),
    }
