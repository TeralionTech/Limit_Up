#!/usr/bin/env python3
"""compute_avg_volume.py — 離線算全市場近 N 交易日「日均成交量(張)」,寫 input/avg_volume.json。

用途: 盤前篩選要「過去一個月日均量 > 500 張」才追蹤。歷史 K 線 REST 限 60/min、逐檔無批次,
全母體 ~1900 檔要 ~35 分鐘 → **夜間 systemd timer 跑**,盤前 runner 直接讀檔 (零盤中 REST)。

⚠ 單位: 富邦 historical.candles 的 volume 是**股**,÷1000 換**張** (與五檔的「張」統一)。
      例 0050 volume 9,239,321 股 = 9,239.3 張。

正確性: 純計算核心 compute_avg_lots() 無 SDK 相依,由 tests/test_avg_volume.py 鎖死;
        SDK 抓取用 --symbols 抽驗模式對已知量的股票 (2330/0050) 人工比對。

用法:
  python scripts/compute_avg_volume.py                      # 全母體 → 寫檔
  python scripts/compute_avg_volume.py --symbols 2330,0050  # 抽驗 (印出,不寫檔)
  python scripts/compute_avg_volume.py --days 20 --out input/avg_volume.json
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("avg_volume")


# ── 純計算核心 (無 SDK;測試鎖死正確性) ──────────────────────────────
def compute_avg_lots(candles: list, days: int = 20) -> float:
    """近 days 個交易日的「日均成交量(張)」。

    candles: [{"date": "YYYY-MM-DD", "volume": <股>}, ...] (任意順序,volume 單位=股)。
    取日期最新的 days 筆 → 平均 volume(股) ÷ 1000 = 張。無資料回 0.0。
    """
    rows = [c for c in (candles or []) if c.get("date")]
    if not rows:
        return 0.0
    rows.sort(key=lambda c: str(c["date"]))
    recent = rows[-days:]                       # 最近 days 個交易日 (不足則有幾天算幾天)
    vols = [float(c.get("volume") or 0) for c in recent]
    if not vols:
        return 0.0
    avg_shares = sum(vols) / len(vols)
    return round(avg_shares / 1000.0, 1)        # 股 → 張


# ── SDK 殼 ────────────────────────────────────────────────────────
def _fetch_candles(reststock, symbol: str, frm: str, to: str) -> list:
    """單檔日 K (只取 volume 欄位)。回 [{date, volume}, ...]。失敗 raise。"""
    resp = reststock.historical.candles(**{
        "symbol": symbol, "from": frm, "to": to,
        "timeframe": "D", "fields": "volume",
    })
    return (resp or {}).get("data") or []


def run(symbols: list, days: int, calendar_days: int, reststock,
        throttle_sec: float, min_lots: float) -> dict:
    """逐檔抓 K 線算日均量(張)。回 {symbol: avg_lots}。"""
    to = datetime.now().strftime("%Y-%m-%d")
    frm = (datetime.now() - timedelta(days=calendar_days)).strftime("%Y-%m-%d")
    logger.info(f"抓 {len(symbols)} 檔日K {frm}~{to} (近 {days} 交易日均量);"
                f"節流每檔間隔 {throttle_sec:.2f}s")
    out, fail = {}, 0
    for i, sym in enumerate(symbols, 1):
        try:
            candles = _fetch_candles(reststock, sym, frm, to)
            out[sym] = compute_avg_lots(candles, days)
        except Exception as e:
            fail += 1
            if fail <= 10 or fail % 50 == 0:
                logger.warning(f"{sym} 抓取失敗 ({fail}): {e}")
        if i % 100 == 0:
            logger.info(f"進度 {i}/{len(symbols)} (失敗 {fail})")
        time.sleep(throttle_sec)
    lowvol = sum(1 for v in out.values() if v < min_lots)
    logger.info(f"完成: {len(out)} 檔成功 / {fail} 失敗;"
                f"其中 {lowvol} 檔日均量 < {min_lots} 張 (盤前會被剔除)")
    return out


def main():
    ap = argparse.ArgumentParser(description="離線算近 N 交易日日均量(張) → input/avg_volume.json")
    ap.add_argument("--symbols", default="", help="抽驗用逗號清單 (例 2330,0050);空=全母體")
    ap.add_argument("--days", type=int, default=20, help="近幾個交易日 (預設 20)")
    ap.add_argument("--calendar-days", type=int, default=40,
                    help="往回抓幾個日曆天 (要 > days 的交易日, 預設 40)")
    ap.add_argument("--max-per-min", type=int, default=55, help="歷史K線節流 (≤60/min, 預設 55)")
    ap.add_argument("--min-lots", type=float, default=500, help="低量門檻 (僅統計顯示)")
    ap.add_argument("--out", default=str(REPO / "input" / "avg_volume.json"))
    ap.add_argument("--dry-run", action="store_true", help="只印不寫檔 (--symbols 時預設 dry)")
    args = ap.parse_args()

    from config import load_config
    from filter import login_fubon
    from universe import get_universe

    cfg = load_config()
    sdk, _ = login_fubon(cfg)
    reststock = sdk.marketdata.rest_client.stock

    if args.symbols.strip():
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
        dry = True                              # 抽驗一律不覆蓋正式檔
    else:
        symbols = get_universe(sdk, cfg.universe)
        dry = args.dry_run

    throttle = 60.0 / max(1, args.max_per_min)
    result = run(symbols, args.days, args.calendar_days, reststock, throttle, args.min_lots)

    # 抽驗: 印出每檔 (人工對已知量)
    if args.symbols.strip():
        for s in symbols:
            print(f"  {s}: 日均量 {result.get(s, '—')} 張")

    if dry:
        logger.info("dry-run / 抽驗 — 不寫檔")
        return
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.parent / (out_path.name + ".tmp")
    tmp.write_text(json.dumps(result, ensure_ascii=False, indent=0), encoding="utf-8")
    os.replace(tmp, out_path)
    logger.info(f"已寫 {out_path} ({len(result)} 檔)")


if __name__ == "__main__":
    main()
