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
