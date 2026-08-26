"""月均量篩選 — 盤前讀 input/avg_volume.json (離線腳本產),剔除日均量 < 門檻 的股票。

檔案兩形都收 (Stage A5,兩半可分開部署,先到哪半 08:00 runner 都不能壞):
  新包裝形 = {"date": "YYYY-MM-DD", "generated_at": iso8601, "avg_lots": {symbol: 張}}
  舊平面形 = {symbol: avg_lots(張)}
由 scripts/compute_avg_volume.py 交易日 07:00 timer 產。
fail-open (使用者定案 2026-08-24): 檔缺/某檔無資料 → **不剔除** (寧可多做,不因資料缺停交易)。
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

_STALE_DAYS = 3      # 檔案超過幾天 = 過時 (腳本每晚跑,正常應為今/昨日)


def load(path) -> tuple:
    """讀 avg_volume.json → (avg_lots dict, meta)。
    meta = {exists, count, mtime_date, stale, date}。缺檔/壞檔不 raise (回空 + exists=False)。
    date = 包裝形檔內的交易日,舊平面形為 None (mtime 會被重佈署 touch 洗掉,
    檔內 date 才是強信號;stale 仍照 mtime 算,維持相容)。"""
    p = Path(path)
    meta = {"exists": p.is_file(), "count": 0, "mtime_date": None, "stale": True,
            "date": None}
    if not p.is_file():
        return {}, meta
    try:
        mt = datetime.fromtimestamp(p.stat().st_mtime)
        meta["mtime_date"] = mt.strftime("%Y-%m-%d")
        meta["stale"] = (datetime.now() - mt).days > _STALE_DAYS
        raw = json.loads(p.read_text(encoding="utf-8"))
        # 新包裝形 (有 avg_lots+date) 取內層 map;否則當舊平面形 {symbol: 張}
        if isinstance(raw, dict) and "avg_lots" in raw and "date" in raw:
            meta["date"] = raw.get("date")
            raw = raw.get("avg_lots")
        data = {}
        for k, v in (raw if isinstance(raw, dict) else {}).items():
            try:
                data[str(k)] = float(v)
            except (TypeError, ValueError):
                continue
        meta["count"] = len(data)
        return data, meta
    except Exception as e:
        logger.error(f"[avg_volume] 讀 {path} 失敗: {e}")
        return {}, meta


def filter_universe(universe: list, avg_lots: dict, min_lots: float) -> tuple:
    """回 (kept, dropped)。**只剔除「有資料且 < 門檻」的檔**;
    無資料的檔 (不在 avg_lots) 保留 (fail-open — 不因資料缺誤殺)。"""
    kept, dropped = [], []
    for s in universe:
        v = avg_lots.get(s)
        if v is not None and v < min_lots:
            dropped.append(s)
        else:
            kept.append(s)
    return kept, dropped
