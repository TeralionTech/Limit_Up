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


class ConfigError(RuntimeError):
    """設定錯誤 — 用可捕捉例外 (SystemExit 在 background thread 會靜默死亡,
    runner 的 phase/error_msg 都不會更新 → 早上壞掉沒人知道)。"""


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
    final_check_start: str  # (deprecated 2026-08-13) 量減半改 08:59:58 預掛前批次判,此欄不再使用
    pre_order_time: str    # "08:59:58" — 真實模式預掛漲停價限價單時點 (final check 窗口也到此為止)
    max_stock_price: float  # 500 — 只做漲停價 ≤ 此價的股票 (0 = 不限制)
    order_min_interval_sec: float  # 0.2 — 市價追單「失敗後」單檔退避 (送單速率由 ORDER_MAX_PER_SEC 管)
    order_max_per_sec: int  # 45 — 送單速率上限 (爆發式滑動窗口;富邦 50/s,留 margin)
    cancel_pending_time: str  # "13:23:00" — 撤掉所有未成交委託的時點 (與 13:24 程式結束脫鉤)
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
    # 富邦副帳號 (選填 — 多開 sockets 監控更多股票;每帳號富邦上限 5 條)
    # dataclass 規則: 有預設值的欄位必須放最後
    account_id_2: str = ""
    password_2: str = ""
    pfx_path_2: str = ""
    pfx_password_2: str = ""
    account_id_3: str = ""
    password_3: str = ""
    pfx_path_3: str = ""
    pfx_password_3: str = ""
    socket_count_2: int = 0    # 副帳號各自的 socket 數 (0/未設 → 同 SOCKET_COUNT)
    socket_count_3: int = 0


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
        raise ConfigError(f"[config] .env 缺少必填欄位: {', '.join(missing)}")

    if not Path(pfx_path).exists():
        raise ConfigError(f"[config] PFX 憑證檔案不存在: {pfx_path}")

    # 副帳號 (選填,最多兩個 — 收資料容量/冗餘用)
    account_id_2 = os.environ.get("FUBON_ACCOUNT_ID_2", "").strip()
    password_2 = os.environ.get("FUBON_PASSWORD_2", "").strip()
    pfx_path_2 = os.environ.get("FUBON_PFX_PATH_2", "").strip()
    pfx_password_2 = os.environ.get("FUBON_PFX_PASSWORD_2", "").strip()
    if account_id_2 and pfx_path_2 and not Path(pfx_path_2).exists():
        raise ConfigError(f"[config] 副帳號 PFX 不存在: {pfx_path_2}")
    account_id_3 = os.environ.get("FUBON_ACCOUNT_ID_3", "").strip()
    password_3 = os.environ.get("FUBON_PASSWORD_3", "").strip()
    pfx_path_3 = os.environ.get("FUBON_PFX_PATH_3", "").strip()
    pfx_password_3 = os.environ.get("FUBON_PFX_PASSWORD_3", "").strip()
    if account_id_3 and pfx_path_3 and not Path(pfx_path_3).exists():
        raise ConfigError(f"[config] 副帳號3 PFX 不存在: {pfx_path_3}")

    socket_count = int(os.environ.get("SOCKET_COUNT", "5"))

    return Config(
        account_id=account_id,
        password=password,
        pfx_path=pfx_path,
        pfx_password=pfx_password,
        account_id_2=account_id_2,
        password_2=password_2,
        pfx_path_2=pfx_path_2,
        pfx_password_2=pfx_password_2,
        account_id_3=account_id_3,
        password_3=password_3,
        pfx_path_3=pfx_path_3,
        pfx_password_3=pfx_password_3,
        socket_count_2=int(os.environ.get("SOCKET_COUNT_2", "0") or 0),
        socket_count_3=int(os.environ.get("SOCKET_COUNT_3", "0") or 0),
        universe=os.environ.get("UNIVERSE", "twse+tpex").strip().lower(),
        batch_size=int(os.environ.get("BATCH_SIZE", "199")),
        batch_rotate_sec=int(os.environ.get("BATCH_ROTATE_SEC", "30")),
        socket_count=socket_count,
        end_time=os.environ.get("END_TIME", "09:00:00").strip(),
        limit_up_fetch_deadline=os.environ.get("LIMIT_UP_FETCH_DEADLINE", "08:28").strip(),
        limit_up_max_per_min=int(os.environ.get("LIMIT_UP_MAX_PER_MIN", "250")),
        final_check_start=os.environ.get("FINAL_CHECK_START", "08:59:00").strip(),
        pre_order_time=os.environ.get("PRE_ORDER_TIME", "08:59:58").strip(),
        max_stock_price=float(os.environ.get("MAX_STOCK_PRICE", "500")),
        order_min_interval_sec=float(os.environ.get("ORDER_MIN_INTERVAL_SEC", "0.2")),
        order_max_per_sec=int(os.environ.get("ORDER_MAX_PER_SEC", "45")),
        cancel_pending_time=os.environ.get("CANCEL_PENDING_TIME", "13:23:00").strip(),
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
