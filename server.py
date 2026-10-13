"""FastAPI server — 常駐 + APScheduler 每天 8:00 觸發 runner.

跑法:
    python server.py    # 開發 (uvicorn hot reload)
    # 或 systemd 啟動 uvicorn server:app --host 0.0.0.0 --port 8100

前端 (frontend/dist) build 後放進 static/ 目錄，會被自動 serve。
"""
from __future__ import annotations

import logging
import os
import secrets
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

import trading_config
from api import router as api_router
from notifier import get_notifier, install_logging_handler
from runner import Runner

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("server")


# ─── APScheduler 每天 8:00 觸發 ─────────────────────────────


scheduler: BackgroundScheduler | None = None


def _daily_trigger():
    """每天 8:00 觸發 — 啟動 runner."""
    r = Runner.get()
    if r.is_running():
        logger.warning("[scheduler] 8:00 觸發但 runner 已在跑，skip")
        return
    logger.info("[scheduler] 8:00 到 — 啟動 runner")
    r.start()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global scheduler
    scheduler = BackgroundScheduler(timezone="Asia/Taipei")
    scheduler.add_job(
        _daily_trigger,
        trigger=CronTrigger(hour=8, minute=0, day_of_week="mon-fri", timezone="Asia/Taipei"),
        id="daily_trigger",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("[server] APScheduler started — 每個交易日 8:00 自動觸發 runner")
    # token 中介層 fail-closed: env 沒設 → 所有遠端 POST 都 401 (central 08:30 自動連線/arm 全掛)。
    # 啟動時大聲叫一次,不擋啟動 (本機 loopback 操作仍可用)。
    if not os.environ.get("LIMITUP_API_TOKEN"):
        logger.critical("[server] ⚠ LIMITUP_API_TOKEN 未設 — 所有遠端 POST/PUT/DELETE /api/* 一律 401 "
                        "(fail-closed);只有真本機 loopback 放行。請在 .env 設定後重啟。")
    # 服務一啟動就載入隔日賣清單 → 任何時間 (含週末) 打開頁面都看得到待賣標的,
    # 不用等交易日 8:00 主流程。連線富邦後 connect_async 會再對帳庫存校正張數。
    try:
        Runner.get()._load_overnight_file()
    except Exception as e:
        logger.warning(f"[server] 啟動載入隔日賣清單失敗: {e}")
    # 2026-10-13 中控接入:
    #   (1) mode / 預算 / 張數 從 output/trading_config.json 套回 (armed 永遠不還原) —
    #       否則重啟 (部署、06:00 needrestart) 後 mode=sim、預算 0，中控 08:50 自動 arm 會被 pre-flight 擋整天
    #   (2) 把 CRITICAL / 出場 / 隔日賣 / 連線 等 log 轉到中控發 Signal (notifier.py；CENTRAL_FORWARD_URL 未設 = 不掛)
    try:
        trading_config.restore(Runner.get().session)
    except Exception as e:
        logger.warning(f"[server] 啟動套回交易設定失敗 (維持預設 sim / 預算 0): {e}")
    try:
        install_logging_handler()
    except Exception as e:
        logger.warning(f"[server] 中控通知 handler 掛載失敗 (不影響交易): {e}")
    yield
    try:
        get_notifier().flush()            # 關機前把排隊中的通知送完 (最多一封)
    except Exception:
        pass
    logger.info("[server] shutdown — stop scheduler + runner")
    if scheduler:
        scheduler.shutdown(wait=False)
    r = Runner.get()
    if r.is_running():
        r.stop()


app = FastAPI(title="Hit Limit Up Experiment", lifespan=lifespan)


# ─── Token 中介層 (2026-09-07, central 遠端 arm 的前置) ──────────
# 背景: node/hub 所有 POST 經 cloudflared 公開 tunnel 零認證可打 (實測 POST /api/trading/arm 200)。
# 規則: POST/PUT/DELETE /api/* 需 header X-Limitup-Token == env LIMITUP_API_TOKEN;
#   env 未設或不符 → 401 (fail-closed)。GET / OPTIONS 一律放行 (node→hub 拉快照、
#   monitor 腳本、前端讀取都不帶 header)。
# 真本機放行: client 是 loopback **且**沒有 CF-Connecting-IP header —
#   cloudflared 也是從 loopback 進來,但它會帶 CF-Connecting-IP;
#   scripts/connect_real_nodes.py 打 localhost 沒這個 header → 放行。
# 每 request 現讀 env (免重啟語意,同 api.py AVGVOL_TOKEN)。
# 必須在 CORS add_middleware **之前**註冊 → CORS 在外層,preflight OPTIONS 不會被擋。

_LOOPBACK_HOSTS = ("127.0.0.1", "::1")
_GUARDED_METHODS = ("POST", "PUT", "DELETE")


def _is_trusted_local(request: Request) -> bool:
    """真本機 = loopback 來源且非 cloudflared 轉送 (無 CF-Connecting-IP)。"""
    host = request.client.host if request.client else ""
    return host in _LOOPBACK_HOSTS and request.headers.get("cf-connecting-ip") is None


def _token_ok(request: Request) -> bool:
    """X-Limitup-Token 與 env 比對 (常數時間);env 未設 → 一律 False (fail-closed)。"""
    expected = os.environ.get("LIMITUP_API_TOKEN", "")
    if not expected:
        return False
    given = request.headers.get("x-limitup-token", "")
    return secrets.compare_digest(given.encode("utf-8"), expected.encode("utf-8"))


@app.middleware("http")
async def limitup_token_guard(request: Request, call_next):
    if (request.method in _GUARDED_METHODS
            and request.url.path.startswith("/api/")
            and not _is_trusted_local(request)
            and not _token_ok(request)):
        host = request.client.host if request.client else "?"
        cf_ip = request.headers.get("cf-connecting-ip") or ""
        logger.warning(f"[auth] 401 {request.method} {request.url.path} from {host}"
                       f"{' (CF ' + cf_ip + ')' if cf_ip else ''}")
        return JSONResponse(status_code=401, content={"detail": "unauthorized"})
    return await call_next(request)


# CORS (dev 前端跑 http://localhost:5173)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173",
                   "http://localhost:8100"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)


@app.get("/api")
def api_root():
    return {"name": "Hit Limit Up API", "version": "0.1"}


# 掛前端 static (build 後放 frontend/dist)
_frontend_dist = Path(__file__).parent / "frontend" / "dist"
if _frontend_dist.exists():
    app.mount("/", StaticFiles(directory=str(_frontend_dist), html=True), name="frontend")
    logger.info(f"[server] 前端 static 掛載 {_frontend_dist}")
else:
    logger.warning(f"[server] {_frontend_dist} 不存在 — 前端還沒 build，只提供 /api")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8100"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
