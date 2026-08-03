"""verify.py — 快速驗證 .env 配置對，login/REST/WS 通不通。

用法:
    python verify.py

跑 4 個測試 (~30 秒)，任何一步 fail 會清楚指出問題:
    1. Login  — 主帳號 + 副帳號 (若 .env 有設) 都驗，兩者執行時都會開 socket
    2. REST  — 拿股票清單 + 對 sample 5 檔拿漲停價
    3. WS    — 訂閱 1 檔看能否連線 (盤外可能沒 tick，只驗連線)

跑完看綠燈全亮就可以正式跑 filter.py。
"""
from __future__ import annotations

import logging
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("verify")


def _ok(msg):    logger.info(f"✓ {msg}")
def _fail(msg):  logger.error(f"✗ {msg}")
def _info(msg):  logger.info(f"  {msg}")


def _login_account(FubonSDK, account_id, password, pfx_path, pfx_password, label):
    """登入單一帳號 + init_realtime。成功回 sdk instance，失敗回 None (並印原因)。

    PFX 密碼空字串要傳 3 參數 (跟 Alex fubon_adapter.py:96-99 對齊)。
    """
    sdk = FubonSDK()
    try:
        if pfx_password == "":
            accounts = sdk.login(account_id, password, pfx_path)
        else:
            accounts = sdk.login(account_id, password, pfx_path, pfx_password)
    except Exception as e:
        _fail(f"{label} login raise 例外: {e}")
        _info("常見原因: PFX 憑證密碼錯 / 帳密錯 / 憑證過期")
        return None

    # Fubon 回 Result 物件 — 用 .is_success 判斷 (Alex 也這樣)
    if not accounts:
        _fail(f"{label} login 回空 — 帳密可能錯")
        return None
    is_success = getattr(accounts, "is_success", None)
    if is_success is False:
        _fail(f"{label} login is_success=False, message={getattr(accounts, 'message', '?')}")
        return None

    data = getattr(accounts, "data", None) or []
    _ok(f"{label} Login 成功 (is_success={is_success}) — 帳戶數 {len(data)}")
    for i, a in enumerate(data[:5]):
        _info(f"{label} 帳戶 #{i+1}: {getattr(a, 'account', '?')} "
              f"分行 {getattr(a, 'branch_no', '?')}")

    try:
        sdk.init_realtime()
        _ok(f"{label} init_realtime OK")
    except Exception as e:
        _fail(f"{label} init_realtime 失敗: {e}")
        return None
    return sdk


def main():
    # ─── 1. 讀 .env ─────────────────────────────────────
    print("\n=== [1/4] 讀 .env ===")
    try:
        from config import load_config
        cfg = load_config()
        _ok(".env 4 個必填欄位都有")
        _info(f"  account_id: {cfg.account_id[:3]}*** (前 3 字)")
        _info(f"  pfx_path:   {cfg.pfx_path}")
        _info(f"  universe:   {cfg.universe}")
    except Exception as e:      # ConfigError 等 (原 SystemExit 已改為可捕捉例外)
        _fail(f"config 錯誤: {e}")
        return 1

    # ─── 2. Login (主帳號 + 副帳號若有設) ────────────────
    print("\n=== [2/4] 富邦 Login ===")
    try:
        from fubon_neo.sdk import FubonSDK
    except ImportError:
        _fail("fubon_neo 未安裝 — pip install .../fubon_neo-...whl")
        return 1

    # 主帳號 — 後面 REST/WS 測試都用這個 sdk
    sdk = _login_account(FubonSDK, cfg.account_id, cfg.password,
                         cfg.pfx_path, cfg.pfx_password, label="主帳號")
    if sdk is None:
        return 1

    # 副帳號 (選填) — 執行時 subscriber 會用它多開 5 個 socket，一定要一起驗，
    # 否則副帳號憑證錯也不會被發現，直到開盤 socket 開不起來、覆蓋率砍半。
    if cfg.account_id_2:
        sdk2 = _login_account(FubonSDK, cfg.account_id_2, cfg.password_2,
                              cfg.pfx_path_2, cfg.pfx_password_2, label="副帳號")
        if sdk2 is None:
            _fail("副帳號 login 失敗 — 開盤時副帳號 5 個 socket 會開不起來 (覆蓋率砍半)")
            return 1
    else:
        _info("(未設副帳號，只驗主帳號)")

    # ─── 3. REST — 拿股票清單 + sample 漲停價 ───────────
    print("\n=== [3/4] REST API 測試 ===")
    stock = sdk.marketdata.rest_client.stock

    try:
        t0 = time.time()
        resp = stock.intraday.tickers(
            type="EQUITY", exchange="TWSE", market="TSE", isNormal=False
        )
        twse_count = len(resp.get("data", []))
        _ok(f"TWSE tickers OK ({twse_count} 檔，{time.time()-t0:.1f}s)")
    except Exception as e:
        _fail(f"TWSE tickers 失敗: {e}")
        return 1

    try:
        t0 = time.time()
        resp = stock.intraday.tickers(
            type="EQUITY", exchange="TPEx", market="OTC", isNormal=False
        )
        tpex_count = len(resp.get("data", []))
        _ok(f"TPEx tickers OK ({tpex_count} 檔，{time.time()-t0:.1f}s)")
    except Exception as e:
        _fail(f"TPEx tickers 失敗: {e}")
        return 1

    # sample 5 檔拿漲停價
    sample_syms = ["2330", "2317", "2454", "0050", "3008"]
    print()
    _info(f"抽 5 檔試 intraday.ticker() 拿漲停價...")
    fail_count = 0
    for sym in sample_syms:
        try:
            t0 = time.time()
            resp = stock.intraday.ticker(symbol=sym)
            up = resp.get("limitUpPrice") or resp.get("limit_up")
            _info(f"  {sym}: limit_up={up} ({(time.time()-t0)*1000:.0f}ms)")
        except Exception as e:
            _info(f"  {sym}: FAIL — {e}")
            fail_count += 1
    if fail_count == 0:
        _ok("REST intraday.ticker 5/5 全通")
    elif fail_count < 5:
        _ok(f"REST intraday.ticker {5-fail_count}/5 通 (少數失敗容忍)")
    else:
        _fail("REST intraday.ticker 全失敗")
        return 1

    # ─── 4. WS — 訂閱 1 檔 (盤外可能沒 tick) ──────────
    print("\n=== [4/4] WebSocket 訂閱測試 ===")
    ws = sdk.marketdata.websocket_client.stock
    got_tick = {"count": 0}

    def _on_msg(msg):
        got_tick["count"] += 1

    try:
        ws.on("message", _on_msg)
        ws.connect()
        _ok("WS connect OK")
    except Exception as e:
        _fail(f"WS connect 失敗: {e}")
        return 1

    try:
        ws.subscribe({"channel": "books", "symbol": "2330"})
        ws.subscribe({"channel": "trades", "symbol": "2330"})
        _ok("subscribe 2330 books+trades OK")
    except Exception as e:
        _fail(f"subscribe 失敗: {e}")
        return 1

    _info("等 10 秒看有沒 tick 進來 (盤外可能都沒 tick，屬正常)...")
    time.sleep(10)
    if got_tick["count"] > 0:
        _ok(f"收到 {got_tick['count']} 筆 tick (盤中)")
    else:
        _info("  10 秒內沒 tick — 若現在盤外正常；若盤中要 debug")

    # ─── 結論 ──────────────────────────────────────────
    print("\n=" * 50)
    print("=== 全部驗證通過 ✓ ===")
    print("=" * 50)
    print(f"\n可以正式跑: python filter.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
