"""notifier.py — 中控轉發：規則、節流（去重 / 每分鐘上限只管低等級 / 慢重複冷卻）、合併、payload、logging handler 不炸。"""
import json
import logging
import threading

import pytest

import notifier as N
from notifier import CentralLogHandler, CentralNotifier, should_forward


# ─── 規則 ──────────────────────────────────────────────────────

class TestShouldForward:
    @pytest.mark.parametrize("msg", [
        "[broker] ⚠ 交易 WS 斷線 (code=300) — 啟動自動重連",
        "[session] ⚠⚠ 2330 出場賣單連續失敗,部位 3 張仍在 — ...",
        "[runner] ⚠⚠ 凍結名單**排除 2 檔**行情過期的標記",
        "隨便什麼都要轉",
    ])
    def test_critical_always(self, msg):
        assert should_forward(logging.CRITICAL, msg)

    @pytest.mark.parametrize("msg,expect", [
        ("[session] ⚠ 2330 出場 — 跌停價 95.5 限價賣 2 張 (support_gone)", True),
        ("[session] ⚠ 2330 出場 — 查無跌停價,市價賣兜底 2 張 (x)", True),
        ("[session] ⚠ 2330 出場賣單庫存不足: 券商可賣 1 張 < 送出 2 張", True),
        ("[session] 隔日賣 2330 觸發賣出 (open)", True),
        ("[session] 🌙 隔日賣 2330 — 跌停價 90 限價賣 2 張", True),
        ("[session] 隔日賣 2330 中止賣出 (kill switch / 暫停 / 已移除)", True),
        ("[session] 🚨 緊急全平: 賣出 3 檔 (處置股用委買一價限價)", True),
        ("[session] ⚡ armed = True", True),
        ("[session] ⛔ 拒絕連線: 登入帳號 F12***042 ≠ 本節點", True),
        ("[session] ⚠ 2330 策略外賣出成交 1 張 (order=X)", True),
        ("[broker] ✅ 交易 WS 自動重連成功 — 觸發權威對帳補收", True),
        ("[subscriber] ⚠⚠ 行情 socket #2 被券商拒絕: 連線數達上限", True),
        ("[session] 載入隔日賣清單: 2 檔 ['2330', '2317']", False),     # 日常
        ("[session] 2330 撤管線多送 M=X 也委託成功 → 撤 (已搶到一筆;超買靠出場全量賣)", False),
        ("[runner] 時點格式錯 — 不等待", False),
    ])
    def test_warning_rules(self, msg, expect):
        assert should_forward(logging.WARNING, msg) is expect

    @pytest.mark.parametrize("msg,expect", [
        ("[runner] 主流程例外: LoginError(...)", True),
        ("[session] 連線失敗: 密碼錯誤", True),
        ("[session] 2330 出場賣單失敗 (第 1/5 次): x", True),
        ("[session] 隔日賣 2330 委託失敗 (第 1/3 次): x", True),
        ("[node] 拉 marked 快照失敗到死線 — 今日不交易", True),
        ("[runner] ROLE=node 但 HUB_URL 未設 → 今日不交易", True),
        ("[runner] pre-order-timer 例外 — 預掛/盲送可能未執行: x", True),
        ("[runner] 啟動市價盲送失敗: x", True),
        ("[runner] 13:23 撤單例外: x", True),
        ("[session] 寫成交台帳失敗: disk", False),
    ])
    def test_error_rules(self, msg, expect):
        assert should_forward(logging.ERROR, msg) is expect

    @pytest.mark.parametrize("msg,expect", [
        ("[session] broker 連線完成", True),
        ("[session] mode = real", True),
        ("[broker] 成交回報 2330 sell 2 張 @ 95.5 (order A1)", True),
        ("[broker] 成交回報 2330 buy 1 張 @ 100 (order A2)", True),
        ("[session] 非策略單成交,忽略 (僅落檔): 2330 order=X 1 張", False),
        ("[session] roll_day(2026-10-13) — 同日重啟,state 保留", False),
        ("[subscriber] 收到 100 個 tick", False),
    ])
    def test_info_rules(self, msg, expect):
        assert should_forward(logging.INFO, msg) is expect

    def test_min_level_blocks_low_rules_but_not_critical(self):
        assert should_forward(logging.INFO, "[session] broker 連線完成", min_level=logging.WARNING) is False
        assert should_forward(logging.WARNING, "[session] ⚡ armed = True", min_level=logging.WARNING) is True
        assert should_forward(logging.CRITICAL, "x", min_level=logging.ERROR) is True

    def test_higher_level_message_matching_lower_rule_is_forwarded(self):
        assert should_forward(logging.ERROR, "[session] ⚠ 2330 出場賣單庫存不足") is True
        assert should_forward(logging.INFO, "[session] ⚡ armed = True") is False
        assert should_forward(logging.INFO, "[broker] ⚠ 交易 WS 斷線") is False


# ─── 佇列 / 節流 / 合併 ─────────────────────────────────────────

class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


@pytest.fixture
def notif():
    sent = []
    clk = FakeClock()
    n = CentralNotifier(url="https://central.example", vps_id="21", label="kled",
                        send_fn=lambda level, text: sent.append((level, text)) or True, clock=clk)
    n._ensure_thread = lambda: None        # 測試裡不起背景 thread，手動 flush
    return n, sent, clk


class TestQueueAndThrottle:
    def test_unconfigured_is_noop(self):
        n = CentralNotifier(url="", send_fn=lambda *a: True)
        assert n.configured is False
        assert n.notify("critical", "x") is False

    def test_batch_merges_lines_and_takes_highest_level(self, notif):
        n, sent, _ = notif
        assert n.notify("info", "[broker] 成交回報 2330 buy 1 張")
        assert n.notify("warning", "[session] ⚡ armed = True")
        assert n.notify("critical", "[broker] ⚠ 交易 WS 斷線")
        n.flush()
        assert len(sent) == 1
        level, text = sent[0]
        assert level == "critical"
        assert text.splitlines() == ["[broker] 成交回報 2330 buy 1 張", "[session] ⚡ armed = True",
                                     "[broker] ⚠ 交易 WS 斷線"]
        assert n.stats["sent_batches"] == 1

    def test_dedup_30s_then_allowed_again(self, notif):
        n, sent, clk = notif
        assert n.notify("warning", "same") is True
        assert n.notify("warning", "same") is False
        assert n.stats["deduped"] == 1
        clk.t += 29
        assert n.notify("warning", "same") is False
        clk.t += 2
        assert n.notify("warning", "same") is True
        n.flush()
        assert len(sent) == 1 and sent[0][1] == "same\nsame"

    def test_dedup_key_includes_level(self, notif):
        n, sent, _ = notif
        assert n.notify("info", "m") is True
        assert n.notify("critical", "m") is True       # 不同等級不互相去重

    def test_dedup_ignores_digits(self, notif):
        # 「已斷線 12 分鐘」「已斷線 13 分鐘」是同一件事
        n, sent, clk = notif
        assert n.notify("critical", "[broker] ⚠⚠ 交易 WS 已斷線 12 分鐘,自動重連持續失敗") is True
        assert n.notify("critical", "[broker] ⚠⚠ 交易 WS 已斷線 13 分鐘,自動重連持續失敗") is False
        clk.t += 599
        assert n.notify("critical", "[broker] ⚠⚠ 交易 WS 已斷線 22 分鐘,自動重連持續失敗") is False   # 慢重複 10 分鐘冷卻
        clk.t += 2
        assert n.notify("critical", "[broker] ⚠⚠ 交易 WS 已斷線 22 分鐘,自動重連持續失敗") is True

    def test_symbols_are_not_merged_by_dedup(self, notif):
        # 股票代號是數字：2330 與 2317 的出場是兩則不同訊息
        n, sent, _ = notif
        assert n.notify("warning", "[session] ⚠ 2330 出場 — 跌停價 95.5 限價賣 2 張 (x)") is True
        assert n.notify("warning", "[session] ⚠ 2317 出場 — 跌停價 95.5 限價賣 2 張 (x)") is True
        assert n.notify("warning", "[session] ⚠ 2330 出場 — 跌停價 95.5 限價賣 2 張 (x)") is False

    def test_fills_are_never_deduped(self, notif):
        n, sent, _ = notif
        m = "[broker] 成交回報 2330 buy 1 張 @ 100 (order A1)"
        assert n.notify("info", m) is True
        assert n.notify("info", m) is True
        n.flush()
        assert sent[0][1].count("成交回報") == 2

    def test_rate_limit_only_for_low_levels(self, notif):
        n, sent, clk = notif
        for i in range(n.RATE_PER_MIN):
            assert n.notify("info", f"m{i}") is True
        assert n.notify("info", "overflow") is False
        assert n.notify("warning", "overflow-w") is False
        assert n.stats["dropped"] == 2
        # CRITICAL / ERROR 不受上限
        assert n.notify("critical", "[broker] ⚠ 交易 WS 斷線 (code=300)") is True
        assert n.notify("error", "[session] 連線失敗: x") is True
        n.flush()
        assert len(sent) == 1
        level, text = sent[0]
        assert level == "critical"
        lines = text.splitlines()
        # 20 行 + 「另有」 + 丟棄註記；高等級的兩則一定在裡面
        assert "[broker] ⚠ 交易 WS 斷線 (code=300)" in lines
        assert "[session] 連線失敗: x" in lines
        assert len(lines) == n.MAX_LINES_PER_MSG + 2
        assert f"另有 {n.RATE_PER_MIN + 2 - n.MAX_LINES_PER_MSG} 則" in text
        assert "另丟棄 2 則" in text
        # 被丟掉的訊息沒有記去重戳記：一分鐘後額度恢復就能送
        clk.t += 61
        assert n.notify("info", "overflow") is True

    def test_truncation_keeps_time_order_among_kept(self, notif):
        n, sent, _ = notif
        for i in range(25):
            n.notify("info", f"i{i}")
        n.notify("critical", "c-last")
        n.flush()
        lines = sent[0][1].splitlines()
        assert lines[-2] == "c-last"            # 最後一行是「另有」註記；critical 保留且維持時間序
        assert lines[0] == "i0"

    def test_flush_empty_is_noop(self, notif):
        n, sent, _ = notif
        n.flush()
        assert sent == []

    def test_send_failure_counted_not_raised(self):
        clk = FakeClock()

        def boom(level, text):
            raise RuntimeError("down")
        n = CentralNotifier(url="https://c", send_fn=boom, clock=clk)
        n._ensure_thread = lambda: None
        n.notify("critical", "x")
        n.flush()
        assert n.stats["failed"] == 1

    def test_http_payload_shape(self, monkeypatch):
        calls = []

        class R:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *a):
                pass

        def fake_urlopen(req, timeout):
            calls.append((req.full_url, json.loads(req.data.decode("utf-8")), req.get_header("Content-type"),
                          req.get_header("User-agent"), timeout))
            return R()
        monkeypatch.setattr(N.urllib.request, "urlopen", fake_urlopen)
        n = CentralNotifier(url="https://central.example/", vps_id="21", label="kled")
        assert n._http_send("warning", "hello") is True
        url, payload, ctype, ua, timeout = calls[0]
        assert url == "https://central.example/api/central/notify"
        assert payload == {"vps_id": 21, "fallback_source": "kled", "level": "warning",
                           "message": "hello", "system": "limitup"}
        assert ctype == "application/json"
        assert ua == "hit-limit-up-notifier/1.0"          # Cloudflare 擋 Python-urllib 預設 UA（實機 403 error 1010）
        assert timeout == 10

    def test_http_error_returns_false(self, monkeypatch):
        import io
        import urllib.error

        def fake_urlopen(req, timeout):
            raise urllib.error.HTTPError(req.full_url, 500, "boom", {}, io.BytesIO(b"boom"))
        monkeypatch.setattr(N.urllib.request, "urlopen", fake_urlopen)
        n = CentralNotifier(url="https://c", vps_id="", label="", node_name="node3")
        assert n._http_send("info", "x") is False

    def test_http_payload_without_vps_id_uses_label_or_node(self, monkeypatch):
        calls = []

        class R:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *a):
                pass

        def fake_urlopen(req, timeout):
            calls.append(json.loads(req.data.decode("utf-8")))
            return R()
        monkeypatch.setattr(N.urllib.request, "urlopen", fake_urlopen)
        n = CentralNotifier(url="https://c", vps_id="", label="", node_name="node3")
        assert n._http_send("info", "x") is True
        assert calls[0]["vps_id"] is None and calls[0]["fallback_source"] == "node3"


# ─── logging handler ───────────────────────────────────────────

class TestLogHandler:
    def test_forwards_matching_records_only(self, notif):
        n, sent, _ = notif
        h = CentralLogHandler(n)
        log = logging.getLogger("test.notifier.handler")
        log.setLevel(logging.INFO)
        log.addHandler(h)
        try:
            log.info("[session] broker 連線完成")
            log.info("[subscriber] 收到 100 個 tick")
            log.warning("[session] ⚡ armed = True")
            log.critical("[broker] ⚠ 交易 WS 斷線 (code=300)")
        finally:
            log.removeHandler(h)
        n.flush()
        assert sent[0][0] == "critical"
        assert sent[0][1].splitlines() == ["[session] broker 連線完成", "[session] ⚡ armed = True",
                                           "[broker] ⚠ 交易 WS 斷線 (code=300)"]

    def test_handler_ignores_its_own_logger(self, notif):
        n, sent, _ = notif
        h = CentralLogHandler(n)
        log = logging.getLogger("notifier")
        log.addHandler(h)
        try:
            log.critical("內部錯誤不可迴圈")
        finally:
            log.removeHandler(h)
        n.flush()
        assert sent == []

    def test_handler_swallows_exceptions(self):
        class Bad(CentralNotifier):
            def notify(self, level, message):
                raise RuntimeError("x")
        h = CentralLogHandler(Bad(url="https://c"))
        rec = logging.LogRecord("a", logging.CRITICAL, __file__, 1, "msg", None, None)
        h.emit(rec)        # 不 raise

    def test_install_noop_without_url(self, monkeypatch):
        monkeypatch.delenv("CENTRAL_FORWARD_URL", raising=False)
        monkeypatch.setattr(N, "_singleton", None)
        monkeypatch.setattr(N, "_handler", None)
        assert N.install_logging_handler() is None
        assert N._handler is None

    def test_install_once_with_url_and_strips_inline_comment(self, monkeypatch):
        monkeypatch.setenv("CENTRAL_FORWARD_URL", "https://central.example   # 行尾註解")
        monkeypatch.setenv("CENTRAL_VPS_ID", "7")
        monkeypatch.setenv("CENTRAL_NOTIFY_MIN_LEVEL", "WARNING")
        monkeypatch.setattr(N, "_singleton", None)
        monkeypatch.setattr(N, "_handler", None)
        h = N.install_logging_handler()
        try:
            assert h is not None and h in logging.getLogger().handlers
            assert h.notifier.url == "https://central.example"
            assert h.notifier.vps_id == "7"
            assert h.min_level == logging.WARNING
            assert N.install_logging_handler() is h           # 第二次不重複掛
            assert logging.getLogger().handlers.count(h) == 1
        finally:
            logging.getLogger().removeHandler(h)
            monkeypatch.setattr(N, "_singleton", None)
            monkeypatch.setattr(N, "_handler", None)

    def test_background_thread_flushes(self):
        done = threading.Event()
        sent = []

        def send(level, text):
            sent.append(text)
            done.set()
            return True
        n = CentralNotifier(url="https://c", send_fn=send)
        n.BATCH_SEC = 0.05
        n.notify("critical", "x")
        assert done.wait(3.0)
        assert sent == ["x"]
