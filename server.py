"""FastAPI server — 常駐 + APScheduler 每天 8:00 觸發 runner.

跑法:
    python server.py    # 開發 (uvicorn hot reload)
    # 或 systemd 啟動 uvicorn server:app --host 0.0.0.0 --port 8100

前端 (frontend/dist) build 後放進 static/ 目錄，會被自動 serve。
"""
from __future__ import annotations

import logging
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from api import router as api_router
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
    yield
    logger.info("[server] shutdown — stop scheduler + runner")
    if scheduler:
        scheduler.shutdown(wait=False)
    r = Runner.get()
    if r.is_running():
        r.stop()


app = FastAPI(title="Hit Limit Up Experiment", lifespan=lifespan)

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
