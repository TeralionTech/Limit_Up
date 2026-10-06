"""node_client — ROLE=node 向中心過濾 Hub 拉 marked 快照 (見架構 plan)。

stdlib only (urllib + json),不加 requests 依賴。node 08:59:50 起輪詢 Hub,
重試到拿到 final=true 的快照或過死線 (08:59:57);拿不到 → 回 None → node 今日不交易 (安全)。
"""
import json
import logging
import time
import urllib.request

logger = logging.getLogger(__name__)


def pull_marked_snapshot(hub_url, deadline_ts, require_final=True,
                         retry_interval=0.5, timeout=2.0,
                         urlopen=None, time_fn=time.time, sleep_fn=time.sleep):
    """向 Hub 拉 marked 快照。輪詢到「拿到 final=true」或「過 deadline_ts」。

    hub_url 例 http://10.0.0.9:8100 → GET {hub_url}/api/marked-snapshot。
    回 snapshot dict(含 symbols/…)或 None(拿不到 → 上層當空清單、今日不交易)。
    require_final: Hub 08:59:50 凍結前回 final=false → 續等;凍結後才回 final=true。
    urlopen/time_fn/sleep_fn 可注入(測試用假時鐘/假 HTTP)。
    """
    url = hub_url.rstrip("/") + "/api/marked-snapshot"
    _open = urlopen or (lambda u, timeout: urllib.request.urlopen(u, timeout=timeout))
    last_err = None
    while time_fn() < deadline_ts:
        try:
            resp = _open(url, timeout=timeout)
            try:
                raw = resp.read()
            finally:
                close = getattr(resp, "close", None)
                if close:
                    close()
            data = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
            if not require_final or data.get("final"):
                logger.info(f"[node] 拉到 marked 快照: {len(data.get('symbols', []))} 檔 "
                            f"(final={data.get('final')}, hub role={data.get('role')})")
                return data
            # 還沒 final (Hub 未凍結) → 續等
        except Exception as e:
            last_err = e
        sleep_fn(retry_interval)
    logger.error(f"[node] 拉 marked 快照失敗到死線 (最後錯誤: {last_err}) → 空清單,今日不交易")
    return None


def poll_marked_snapshot_until(hub_url, deadline_ts, on_snapshot,
                               poll_interval=0.5, timeout=0.7,
                               urlopen=None, time_fn=time.time, sleep_fn=time.sleep,
                               stop_fn=None) -> dict:
    """拿到第一份 final 快照之後**繼續**輪詢到 deadline_ts,每份 final=true 的快照都交給 on_snapshot。

    2026-10-06 事故: Hub 的「凍結」只是旗標,名單本身還是活的 — 4716 在 08:59:54 才鎖上並被 Hub
    標記,但 node 08:59:50 拿到第一份 final 快照 (0 檔) 就不再問了,整天沒下單 (IDC 系統有下)。
    這裡讓 node 在 08:59:50 之後持續問到 08:59:57,新增的檔由 on_snapshot 補訂閱、補灌狀態。

    死線語意 (2026-10-07 審查定案): 「每次請求**含逾時**都要在死線前結束」—
      * 只在 time_fn() + timeout <= deadline_ts 時才發下一次請求 (urllib 的 timeout 是每個 socket
        操作各自計時,但 Hub 回應是單一小封包,實際上限 ≈ timeout);
      * 回應若在死線之後才回來 → **丟棄不套用** (late_drops+1) — 08:59:58 的預掛計時器已在讀清單,
        此時再 seed 只會做出「有標記、沒委託」的幽靈檔,或撞上計時器迭代中的 dict;
      * sleep 不超過剩餘時間;stop_fn() 為真 (runner 被停止) 立即返回。
    on_snapshot(data) 的例外不會中斷輪詢 (記 log 後繼續);HTTP 失敗第一次記 WARNING、之後只計數。
    回 {"polls", "snapshots", "non_final", "late_drops", "errors", "first_error", "last_error"} 供 log。"""
    url = hub_url.rstrip("/") + "/api/marked-snapshot"
    _open = urlopen or (lambda u, timeout: urllib.request.urlopen(u, timeout=timeout))
    stats = {"polls": 0, "snapshots": 0, "non_final": 0, "late_drops": 0,
             "errors": 0, "first_error": None, "last_error": None}
    while True:
        if stop_fn is not None and stop_fn():
            break
        if time_fn() + timeout > deadline_ts:       # 這次請求含逾時會超過死線 → 不發
            break
        stats["polls"] += 1
        try:
            resp = _open(url, timeout=timeout)
            try:
                raw = resp.read()
            finally:
                close = getattr(resp, "close", None)
                if close:
                    close()
            if time_fn() >= deadline_ts:            # 回應晚於死線 → 丟棄 (見 docstring)
                stats["late_drops"] += 1
                logger.warning("[node] 凍結後補收: Hub 回應晚於死線 → 丟棄這份快照,停止輪詢")
                break
            data = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
            if data.get("final"):
                stats["snapshots"] += 1
                try:
                    on_snapshot(data)
                except Exception as e:      # 補收邏輯壞掉不能拖垮主流程 — 記下來繼續
                    stats["errors"] += 1
                    stats["last_error"] = e
                    logger.exception(f"[node] 凍結後補收快照處理失敗: {e}")
            else:
                stats["non_final"] += 1
        except Exception as e:
            stats["errors"] += 1
            stats["last_error"] = e
            if stats["first_error"] is None:
                stats["first_error"] = e
                logger.warning(f"[node] 凍結後補收輪詢第一次失敗 (之後只計數): {e!r}")
        remaining = deadline_ts - time_fn()
        if remaining <= 0:
            break
        sleep_fn(min(poll_interval, remaining))
    return stats
