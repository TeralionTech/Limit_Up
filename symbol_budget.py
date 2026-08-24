"""個股金額覆寫 — 使用者為特定股票指定專屬下單金額 (2026-08-24)。

當日最終篩選清單裡有這些股票時,依「專屬金額」下單(取代交易頁的全域每檔金額/張數);
其餘股票維持全域參數規則。設定跨日保留(非每日 state),使用者手動增刪。
持久化: output/symbol_budgets.json ({symbol: amount_ntd}),原子寫入 + module Lock。
"""
from __future__ import annotations

import json
import os
import re
import threading
from pathlib import Path

_LOCK = threading.Lock()
_SYM_RE = re.compile(r"^[0-9]{4,6}[A-Z]?$")   # 台股代號: 4~6 碼數字, 特別股可帶一英文


def _budget_file() -> Path:
    return Path(__file__).parent / "output" / "symbol_budgets.json"


def _validate_symbol(symbol: str) -> str:
    symbol = str(symbol or "").strip().upper()
    if not _SYM_RE.match(symbol):
        raise ValueError(f"股票代號格式不符: {symbol!r}")
    return symbol


def _validate_amount(amount) -> float:
    try:
        amt = float(amount)
    except (TypeError, ValueError):
        raise ValueError(f"金額須為數字: {amount!r}")
    if amt <= 0:
        raise ValueError(f"金額須為正數: {amt}")
    return amt


def _read() -> dict:
    f = _budget_file()
    if not f.exists():
        return {}
    try:
        data = json.loads(f.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write(data: dict):
    f = _budget_file()
    f.parent.mkdir(exist_ok=True)
    tmp = f.parent / (f.name + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, f)


def load() -> dict:
    """回 {symbol: amount_ntd} (float)。"""
    with _LOCK:
        data = _read()
    out = {}
    for k, v in data.items():
        try:
            out[str(k)] = float(v)
        except (TypeError, ValueError):
            continue
    return out


def upsert(symbol: str, amount: float):
    """新增或覆寫某檔的專屬下單金額 (元)。"""
    symbol = _validate_symbol(symbol)
    amount = _validate_amount(amount)
    with _LOCK:
        data = _read()
        data[symbol] = amount
        _write(data)


def remove(symbol: str) -> bool:
    """刪除某檔覆寫。回 True = 有刪到。"""
    symbol = _validate_symbol(symbol)
    with _LOCK:
        data = _read()
        if symbol not in data:
            return False
        del data[symbol]
        _write(data)
        return True
