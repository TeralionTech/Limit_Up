# 搶漲停策略 — 篩選 + 模擬交易

實驗性獨立 script，做多策略。

**8:25-9:00 篩選階段** — 試撮期間找出試撮價鎖漲停 + 沒被賣單打開的股票

**9:00-13:24 交易階段 (純模擬 log)** — 對篩選出的名單自動下市價買、監控條件、觸發賣出

**這是模擬，所有 order 只 log 不打富邦 API**。邏輯確認後才會整合進 teralion_WEB 走真環境。

---

## 快速開始

```bash
# 1. 一次性 setup
cd C:\Users\user\Desktop\finance\experiment\hit_limit_up
pip install -r requirements.txt
pip install "C:/Users/user/Desktop/finance/twse_day_trade_alex/fubon_neo-2.2.7-cp37-abi3-win_amd64.whl"

# 2. 填憑證
copy .env.example .env
notepad .env       # 填 FUBON_ACCOUNT_ID / PASSWORD / PFX_PATH / PFX_PASSWORD

# 3. 執行 (盤前 8:25 前開始)
python filter.py
```

script 會跑到 09:00 (或 `.env` 內 `END_TIME`) 自動結束，寫 `output/YYYY-MM-DD.json`。

---

## 演算法

### 篩選階段 (8:25 - 9:00)
1. 拿全上市+上櫃 ~2600 檔 + 每檔今日漲停價
2. WebSocket 訂閱 **books + trades** 兩個 channel (每檔佔 2 subs)
3. **Mark 條件**: bid1 == 漲停價 AND (asks 空 or ask1.size == 0)
4. **Unmark 條件** (3 種):
   - a. 出現賣單 (ask1.size > 0)
   - b. bid1 size 掉一半以上 (相對 mark 後歷史最大)
5. 09:00 前最後 5 秒 **freeze** — 不再 unmark
6. 09:00 到 → snapshot 當下 marked 名單 → 寫 JSON → 進 trading 階段

### 交易階段 (9:00 - 13:24) 純模擬 log

7. 對篩選出的 watchlist 每檔:
   - **每秒下 1 張市價買** (`.env` ORDER_LOTS_PER_SEC)
   - **達到 5 張目標就停下單** (`.env` TARGET_LOTS)
8. 收到「首筆真實成交」(trades channel):
   - 若成交價 < 漲停 → **discard + 撤單 + 賣出已成交部位**
   - 若首筆量 < 10 張 → 同上
9. 每 60 秒抽 bid1 size 樣本，**連續 5 分鐘遞減** → 標「必須丟棄股」(must_dump)
10. 必須丟棄股: 成交價 < 漲停 時 → 賣出回補
11. **bid1 size 突然掉一半** → 撤單 + 賣出 + discard
12. TRADING_END_TIME 到 (預設 13:24) → 對還有部位的 holdings 全部市價賣 + 結束

---

## 輸出檔案

執行完 script 產出 4 個檔:

| 檔案 | 內容 |
|---|---|
| `output/YYYY-MM-DD.json` | 篩選結束時 (09:00) marked 名單 + history |
| `output/YYYY-MM-DD_ticks.jsonl` | 全部 books + trades tick raw 資料 (幾百 MB) |
| `output/YYYY-MM-DD_orders.csv` | 每筆 SIM order 記錄 + API 時間差 |
| `output/YYYY-MM-DD_trading_summary.json` | 13:24 結束時各檔部位 / discard / must_dump |

### orders.csv 欄位

`order_id, ts_sent, ts_accepted, latency_ms, action, symbol, lots, price_type, extra`

- `latency_ms`: API 送出 → 被接受時間差 (模擬階段 ≈ 0，真環境用來評估富邦回應速度)
- `action`: BUY / SELL / CANCEL
- `extra`: SELL/CANCEL 的原因 (e.g. `bid_drop_half: 1000→400`, `first_trade_qty_too_small`)

### YYYY-MM-DD.json (篩選結果)


```json
{
  "snapshot_at": "2026-07-08T09:00:00",
  "end_time": "09:00:00",
  "universe": "twse+tpex",
  "stats": {
    "currently_marked": 8,
    "total_mark_events": 42,
    "total_unmark_events": 34,
    "unique_symbols_touched": 42
  },
  "marked": [
    {
      "symbol": "2330",
      "history": [
        {"event": "mark", "ts": "2026-07-08T08:35:12", "bid_price": 900.0, "limit_up": 900.0}
      ]
    }
  ]
}
```

---

## 環境變數 (`.env`)

| 變數 | 必填 | 預設 | 說明 |
|---|---|---|---|
| `FUBON_ACCOUNT_ID` | ✅ | — | 身份證字號 |
| `FUBON_PASSWORD` | ✅ | — | 富邦登入密碼 |
| `FUBON_PFX_PATH` | ✅ | — | 憑證絕對路徑 |
| `FUBON_PFX_PASSWORD` | ⚠️ | 空字串 | 憑證密碼 (若無留空) |
| `UNIVERSE` | | `twse+tpex` | `twse` / `tpex` / `twse+tpex` |
| `BATCH_SIZE` | | 200 | 每批訂閱檔數 |
| `BATCH_ROTATE_SEC` | | 30 | 多久換下一批 |
| `END_TIME` | | 09:00:00 | 何時停止標記 |
| `FREEZE_UNMARK_LAST_SEC` | | 5 | 最後 N 秒 freeze unmark |
| `DEBUG` | | false | 開更多 log |

---

## 已知限制 / 待實測

### 富邦 SDK 多 socket 架構
根據富邦官方: **單一 WS 連線 200 訂閱數上限；同帳號可同時開 5 連線**。

`subscriber.py` 實作 **multi-SDK** — 開 5 個 `FubonSDK` instance，各自 login/init_realtime → 5 個獨立 WS 連線 × 200 檔 = **1000 檔平行監控**。

母體 ~2600 檔 → 需 3 批循環 (每批 1000 檔) × 30s = **每 90s 全母體監完一次**，30 分鐘試撮期可監 **~20 圈**。

若富邦擋同帳號 5 login (實測才知)，`_try_multi_sdk()` 會 return False，自動 fallback 到**保底 loop single-socket** (單 socket 每 30s 切一批 200 檔，只能監 4-5 圈)。

配置在 `.env` 內的 `SOCKET_COUNT` (預設 5，可調小)。

### 試撮階段 books channel 是否有 tick
未實測。若富邦試撮期間**不推 books event**，script 會空跑。第一次跑要看 log 有沒 `MARK` 事件出現。

### ETF 排除
`universe.py` 目前排 `0*` 開頭 (排 ETF 如 0050)。若你想包 ETF，砍掉 `not s.startswith("0")` 那條。

### 4 位數過濾
只留 4 位數 symbol → 排除 5-6 位權證。

---

## Debug 步驟

若 output 都空：

1. 開 `DEBUG=true` 重跑，觀察 log
2. 看 log 是否有 `[subscriber] 收到 N 個 tick` — 若 tick 一直 0，可能：
   - 富邦試撮階段 books 不推
   - 訂閱參數格式不對 (`channel="books"` vs 別的)
3. 若 tick 有但 mark 一直 0 → 檢查 `limit_ups` dict 是否抓到 (log 尾聲會印 `[universe] limit_up 抓齊 — 成功 X/Y`)
4. 檢查漲停價比對 — 有些股票 (注意股/處置股) 漲停幅度不同

---

## Roadmap (確認邏輯後才做)

1. **穿進 teralion_WEB** — 變成 central 一支排程 (08:25 自動觸發) 或 Alex 內建 filter module
2. **加下單邏輯** — 篩出來的 top N 自動下單
3. **參數優化** — 漲停幾秒沒打開才標 / 賣單量多大才算打開 / 只挑成交量前 N 大等
4. **回測** — 用歷史 tick 資料驗證這套篩選在過去 N 天勝率如何

---

## 這是 experiment/，不進 git

- `.env` (含實際憑證) 在 `.gitignore`
- `output/*.json` (每日結果) 在 `.gitignore`

若你要 commit：只 commit `.py` / `README.md` / `.env.example` / `requirements.txt`。
