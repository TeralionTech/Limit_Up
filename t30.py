"""T30 全額交割/禁單名單 — 每日盤前判斷「API 下不了單」的股票 (2026-08-12)。

背景: 全額交割股 API 下單必被拒,市價追「試到成功為止」會變狂送單 (實盤事故)。
檔案: T30V.TSE / T30V.OTC — 券商每日提供,100-byte 定長記錄、cp950 編碼,
由 deploy/fetch_t30.sh 於 8:30 前抓到 input/t30/ (systemd timer)。

判斷依據 (與 experiment/T30_extractor 逐 byte 交叉驗證):
    全額交割          = SETTYPE (byte 41) != '0'   (1=全額交割, 2=全額交割+分盤)
    每筆需 100% 預收  = MARK-W  (byte 42) == '2'   (第二次處置 — 實務上同樣下不了單)
兩者聯集 = untradable → 預掛/市價追一律跳過 (stopped_reason="full_cash_delivery")。
所需欄位都在 [0:84] 區段 — TSE/OTC 該區版面相同,不需分市場解析。
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

RECORD_SIZE = 100


def parse_untradable(path) -> set:
    """單一 T30 檔 → 下不了單的 stock_no 集合。格式不符 raise ValueError。"""
    data = Path(path).read_bytes()
    if not data or len(data) % RECORD_SIZE:
        raise ValueError(f"{path}: 大小 {len(data)} 不是 {RECORD_SIZE} 的整數倍,非 T30 格式")
    out = set()
    for i in range(0, len(data), RECORD_SIZE):
        rec = data[i:i + RECORD_SIZE]
        settype = rec[41:42].decode("ascii", "replace")
        mark_w = rec[42:43].decode("ascii", "replace")
        if settype != "0" or mark_w == "2":
            out.add(rec[0:6].decode("ascii", "replace").strip())
    out.discard("")
    return out


def load_untradable(t30_dir) -> tuple:
    """讀 t30_dir 下的 T30V.TSE / T30V.OTC → (untradable set, meta dict)。

    meta = {"files": {檔名: {"exists", "ok", "mtime_date", "stale"}},
            "missing_all": bool}
    缺檔/壞檔**不 raise** — 能解析多少算多少 + 旗標,caller 決定告警
    (使用者定案 2026-08-12: 舊檔照用 + CRITICAL;全缺 = 無保護 + CRITICAL)。
    """
    t30_dir = Path(t30_dir)
    today = datetime.now().strftime("%Y-%m-%d")
    untradable: set = set()
    files_meta: dict = {}
    any_ok = False
    for name in ("T30V.TSE", "T30V.OTC"):
        p = t30_dir / name
        info = {"exists": p.is_file(), "ok": False, "mtime_date": None, "stale": True}
        if info["exists"]:
            try:
                info["mtime_date"] = datetime.fromtimestamp(
                    p.stat().st_mtime).strftime("%Y-%m-%d")
                info["stale"] = info["mtime_date"] != today
                untradable |= parse_untradable(p)
                info["ok"] = True
                any_ok = True
            except Exception as e:
                logger.error(f"[t30] 解析 {name} 失敗: {e}")
        files_meta[name] = info
    return untradable, {"files": files_meta, "missing_all": not any_ok}
