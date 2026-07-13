"""Config — 從 .env 讀憑證跟參數。"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass


@dataclass
class Config:
    # 富邦主帳號憑證
    account_id: str
    password: str
    pfx_path: str
    pfx_password: str
    # 監控範圍
    universe: str          # "twse" / "tpex" / "twse+tpex"
    batch_size: int        # 每批訂閱檔數 (每 socket 上限 200)
    batch_rotate_sec: int  # 多久換下一批 (多批時循環)
    socket_count: int      # 開幾個 WS 連線 (富邦上限 5)
    end_time: str          # "09:00:00" 停止篩選時間 (轉入 trader)
    limit_up_fetch_deadline: str  # "08:28" — 漲停價抓取重試截止 (盤前試撮 8:30 前留 buffer)
    limit_up_max_per_min: int  # 250 — 漲停價 REST 抓取節流上限 (富邦日內行情限 300/min，留 margin)
    final_check_start: str  # "08:59:00" — 此時起 bid 減半 final check (之前只記錄 max)
    bid_drop_ratio: float   # final check 減半閾值 (預設 0.5)
    debug: bool
    # === Trading (9:00 後 trader 用) ===
    trading_end_time: str  # "13:24:00" 收盤時間 script 結束
    target_lots: int       # 每檔目標買到幾張
    order_lots_per_sec: int  # 每秒每檔下幾張 (若已達 target 就停)
    first_trade_min_lots: int  # 首筆真實成交量 < 此則 discard
    bid_decline_minutes: int  # 連續遞減幾分鐘 = discard
    bid_decline_sample_sec: int  # 每 N 秒抓一次 bid 五檔總量 sample
    skip_trader: bool      # true → 篩選完不進 trader，改成常駐收 tick (LIVE_SUBSCRIBE)
    # 富邦副帳號 (選填 — 多開 sockets 監控更多股票)
    # dataclass 規則: 有預設值的欄位必須放最後
    account_id_2: str = ""
    password_2: str = ""
    pfx_path_2: str = ""
    pfx_password_2: str = ""


def load_config() -> Config:
    account_id = os.environ.get("FUBON_ACCOUNT_ID", "").strip()
    password = os.environ.get("FUBON_PASSWORD", "").strip()
    pfx_path = os.environ.get("FUBON_PFX_PATH", "").strip()
    pfx_password = os.environ.get("FUBON_PFX_PASSWORD", "").strip()

    missing = []
    if not account_id: missing.append("FUBON_ACCOUNT_ID")
    if not password: missing.append("FUBON_PASSWORD")
    if not pfx_path: missing.append("FUBON_PFX_PATH")
    if missing:
        raise SystemExit(f"[config] .env 缺少必填欄位: {', '.join(missing)}")

    if not Path(pfx_path).exists():
        raise SystemExit(f"[config] PFX 憑證檔案不存在: {pfx_path}")

    # 副帳號 (選填)
    account_id_2 = os.environ.get("FUBON_ACCOUNT_ID_2", "").strip()
    password_2 = os.environ.get("FUBON_PASSWORD_2", "").strip()
    pfx_path_2 = os.environ.get("FUBON_PFX_PATH_2", "").strip()
    pfx_password_2 = os.environ.get("FUBON_PFX_PASSWORD_2", "").strip()
    if account_id_2 and pfx_path_2 and not Path(pfx_path_2).exists():
        raise SystemExit(f"[config] 副帳號 PFX 不存在: {pfx_path_2}")

    return Config(
        account_id=account_id,
        password=password,
        pfx_path=pfx_path,
        pfx_password=pfx_password,
        account_id_2=account_id_2,
        password_2=password_2,
        pfx_path_2=pfx_path_2,
        pfx_password_2=pfx_password_2,
        universe=os.environ.get("UNIVERSE", "twse+tpex").strip().lower(),
        batch_size=int(os.environ.get("BATCH_SIZE", "199")),
        batch_rotate_sec=int(os.environ.get("BATCH_ROTATE_SEC", "30")),
        socket_count=int(os.environ.get("SOCKET_COUNT", "5")),
        end_time=os.environ.get("END_TIME", "09:00:00").strip(),
        limit_up_fetch_deadline=os.environ.get("LIMIT_UP_FETCH_DEADLINE", "08:28").strip(),
        limit_up_max_per_min=int(os.environ.get("LIMIT_UP_MAX_PER_MIN", "250")),
        final_check_start=os.environ.get("FINAL_CHECK_START", "08:59:00").strip(),
        bid_drop_ratio=float(os.environ.get("BID_DROP_RATIO", "0.5")),
        debug=os.environ.get("DEBUG", "false").lower() in ("1", "true", "yes"),
        # Trading
        trading_end_time=os.environ.get("TRADING_END_TIME", "13:24:00").strip(),
        target_lots=int(os.environ.get("TARGET_LOTS", "5")),
        order_lots_per_sec=int(os.environ.get("ORDER_LOTS_PER_SEC", "1")),
        first_trade_min_lots=int(os.environ.get("FIRST_TRADE_MIN_LOTS", "10")),
        bid_decline_minutes=int(os.environ.get("BID_DECLINE_MINUTES", "5")),
        bid_decline_sample_sec=int(os.environ.get("BID_DECLINE_SAMPLE_SEC", "60")),
        skip_trader=os.environ.get("SKIP_TRADER", "true").lower() in ("1", "true", "yes"),
    )
