"""帳務台帳 — 每日手動輸入的帳戶損益 (2026-08-13)。

獨立於交易流程的純手動記錄;任意日期可事後覆寫/刪除。
持久化: output/pnl_ledger.json ({date: {pnl, note}}),原子寫入 (temp + os.replace)
+ module 級 Lock — 重啟不丟。
"""
from __future__ import annotations

import json
import os
import re
import threading
from pathlib import Path

_LOCK = threading.Lock()
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _ledger_file() -> Path:
    return Path(__file__).parent / "output" / "pnl_ledger.json"


def _validate_date(date: str) -> str:
    date = str(date or "").strip()
    if not _DATE_RE.match(date):
        raise ValueError(f"日期格式須為 YYYY-MM-DD: {date!r}")
    return date


def _read() -> dict:
    f = _ledger_file()
    if not f.exists():
        return {}
    try:
        data = json.loads(f.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write(data: dict):
    f = _ledger_file()
    f.parent.mkdir(exist_ok=True)
    tmp = f.parent / (f.name + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, f)


def load() -> list:
    """全部記錄,依日期舊→新排序,含累積損益欄。"""
    with _LOCK:
        data = _read()
    out = []
    cumulative = 0.0
    for date in sorted(data):
        rec = data[date] or {}
        pnl = float(rec.get("pnl") or 0)
        cumulative += pnl
        out.append({"date": date, "pnl": pnl,
                    "note": str(rec.get("note") or ""),
                    "cumulative": round(cumulative, 2)})
    return out


def upsert(date: str, pnl: float, note: str = ""):
    """新增或覆寫某一日的損益 (同日期 = 修改)。"""
    date = _validate_date(date)
    with _LOCK:
        data = _read()
        data[date] = {"pnl": float(pnl), "note": str(note or "")}
        _write(data)


def remove(date: str) -> bool:
    """刪除某一日。回 True = 有刪到。"""
    date = _validate_date(date)
    with _LOCK:
        data = _read()
        if date not in data:
            return False
        del data[date]
        _write(data)
        return True
