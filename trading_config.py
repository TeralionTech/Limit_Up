"""交易設定落檔 (2026-10-13) — mode / 預算 / 張數 跨重啟保留。

為什麼: ``TradingSession`` 的 mode 與 params 只在記憶體；服務重啟 (部署、06:00 的 unattended-upgrades
needrestart、當機) 之後 mode 退回 sim、預算歸零 → 中控 08:50 自動 arm 會被 pre-flight 擋下
(「模擬模式不能開始交易」/「先設定總預算」)，整天不交易。以前靠部署腳本手動設回 (deploy_limitup_commit.py
``restore_trading``)；現在 API 每次改完就寫檔、啟動時讀回。

存原五個欄位與獨立 pull_exit 設定、**永遠不存 armed**。
檔案: output/trading_config.json (output/ 不進 git、搬遷時會一起搬)。壞檔 / 缺檔 → 當沒有，不影響啟動。
"""
from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path
from typing import Optional

from pull_signal import PullExitConfig

logger = logging.getLogger(__name__)

FILENAME = "trading_config.json"
FIELDS = ("mode", "sizing_mode", "fixed_lots", "total_budget", "per_symbol_budget")
# Every settings writer holds this from mutation through persistence. This is
# independent of the quote/session locks, which must be released before file I/O.
CONFIG_LOCK = threading.RLock()


def config_path(output_dir: Optional[Path] = None) -> Path:
    base = output_dir or (Path(__file__).parent / "output")
    return base / FILENAME


def snapshot(session) -> dict:
    return {
        "mode": session.mode,
        "sizing_mode": session.sizing_mode,
        "fixed_lots": int(session.fixed_lots),
        "total_budget": float(session.total_budget),
        "per_symbol_budget": float(session.per_symbol_budget),
        "pull_exit": session.pull_exit.to_dict(),
    }


def persist(session, output_dir: Optional[Path] = None) -> bool:
    """寫檔 (原子: 先寫 .tmp 再 replace)。失敗只 log、回 False。"""
    with CONFIG_LOCK:
        path = config_path(output_dir)
        try:
            data = snapshot(session)
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
            os.replace(tmp, path)
            return True
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[trading_config] 寫 {path} 失敗: {e}")
            return False


def load(output_dir: Optional[Path] = None) -> Optional[dict]:
    """讀檔；缺檔 / 壞檔 / 欄位不合法 → None。"""
    path = config_path(output_dir)
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[trading_config] {path} 不是合法 JSON，忽略: {e}")
        return None
    if not isinstance(data, dict):
        return None
    out = {}
    mode = data.get("mode")
    if mode in ("sim", "real"):
        out["mode"] = mode
    sizing = data.get("sizing_mode")
    if sizing in ("budget", "fixed_lots"):
        out["sizing_mode"] = sizing
    for key in ("fixed_lots", "total_budget", "per_symbol_budget"):
        v = data.get(key)
        if isinstance(v, (int, float)) and not isinstance(v, bool) and v >= 0:
            out[key] = int(v) if key == "fixed_lots" else float(v)
    if "pull_exit" in data:
        try:
            if not isinstance(data["pull_exit"], dict):
                raise ValueError("pull_exit 必須是 object")
            out["pull_exit"] = PullExitConfig().updated(data["pull_exit"]).to_dict()
        except (TypeError, ValueError) as e:
            logger.warning(f"[trading_config] pull_exit 無效，維持 off: {e}")
    return out or None


def restore(session, output_dir: Optional[Path] = None) -> Optional[dict]:
    """啟動時套回 session (只動 mode / params，**不碰 armed**)。回套用的欄位；沒有檔案回 None。"""
    cfg = load(output_dir)
    if not cfg:
        return None
    applied = {}
    try:
        if "mode" in cfg and cfg["mode"] != session.mode:
            session.set_mode(cfg["mode"])
            applied["mode"] = cfg["mode"]
        params = {k: cfg[k] for k in ("total_budget", "per_symbol_budget", "sizing_mode", "fixed_lots") if k in cfg}
        if params:
            session.set_params(**params)
            applied.update(params)
        if "pull_exit" in cfg:
            session.set_pull_exit(**cfg["pull_exit"])
            applied["pull_exit"] = cfg["pull_exit"]
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[trading_config] 套回設定失敗 (維持預設): {e}")
        return applied or None
    logger.warning(f"[trading_config] 啟動套回交易設定: {applied} (armed 不還原，仍需按開始交易 / 由中控排程開)")
    return applied
