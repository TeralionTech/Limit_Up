"""Universe — 抓上市/上櫃全 symbol 清單 + 每檔漲停價。"""
from __future__ import annotations

import logging
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)


def get_universe(sdk, universe_key: str) -> List[str]:
    """回全上市+上櫃股票 symbol list，過濾 4 位數 (排權證 5-6 位)。

    universe_key: 'twse' / 'tpex' / 'twse+tpex'
    """
    stock = sdk.marketdata.rest_client.stock
    all_syms = []

    if "twse" in universe_key:
        try:
            resp = stock.intraday.tickers(
                type="EQUITY", exchange="TWSE", market="TSE", isNormal=False
            )
            twse_syms = [r["symbol"] for r in resp.get("data", [])]
            all_syms.extend(twse_syms)
            logger.info(f"[universe] TWSE 拿到 {len(twse_syms)} 檔")
        except Exception as e:
            logger.error(f"[universe] TWSE tickers 抓取失敗: {e}")

    if "tpex" in universe_key:
        try:
            resp = stock.intraday.tickers(
                type="EQUITY", exchange="TPEx", market="OTC", isNormal=False
            )
            tpex_syms = [r["symbol"] for r in resp.get("data", [])]
            all_syms.extend(tpex_syms)
            logger.info(f"[universe] TPEx 拿到 {len(tpex_syms)} 檔")
        except Exception as e:
            logger.error(f"[universe] TPEx tickers 抓取失敗: {e}")

    # 過濾: 只留 4 位數股票代碼 (排除權證 5-6 位) + 排 ETF (0開頭)
    # 註: 如果要包 ETF (e.g. 0050) 就砍掉 not s.startswith("0") 那條
    filtered = sorted(set(
        s for s in all_syms
        if len(s) == 4 and s.isdigit() and not s.startswith("0")
    ))
    logger.info(f"[universe] 過濾後 {len(filtered)} 檔 (4 位數，排 ETF/權證)")
    return filtered


def fetch_limit_ups(sdk, symbols: List[str],
                     progress_every: int = 100) -> Dict[str, float]:
    """對每檔 symbol 拿當日漲停價 (limitUpPrice)。
    開盤前一次抓齊，避免 tick 進來還要再查。
    """
    stock = sdk.marketdata.rest_client.stock
    result: Dict[str, float] = {}
    failed: List[Tuple[str, str]] = []

    for i, sym in enumerate(symbols, 1):
        try:
            resp = stock.intraday.ticker(symbol=sym)
            up = resp.get("limitUpPrice") or resp.get("limit_up")
            if up:
                result[sym] = float(up)
            else:
                failed.append((sym, "no limitUpPrice in response"))
        except Exception as e:
            failed.append((sym, str(e)[:100]))
        if i % progress_every == 0:
            logger.info(f"[universe] limit_up 抓取進度 {i}/{len(symbols)} "
                        f"(成功 {len(result)}, 失敗 {len(failed)})")

    if failed:
        logger.warning(f"[universe] {len(failed)} 檔漲停價抓不到 (sample: {failed[:5]})")
    logger.info(f"[universe] limit_up 抓齊 — 成功 {len(result)}/{len(symbols)}")
    return result
