# 搶漲停策略 — 篩選 + 模擬交易

> 📌 **策略權威說明見 [docs/strategy.md](docs/strategy.md)**(反映目前實際程式行為;改策略先改它再改碼)。
> 以下為早期模擬版說明,保留作歷史參考。

## 中控實盤資料唯讀端點（live days v2）

供 central hub 的 **strategy-health collector** 收集搶漲停實盤資料；本功能不建立健康燈、monitor 或回測替身。
兩支端點只讀程式目錄下 `output/` 的既有檔案，不呼叫 Runner、券商、連線、arm、庫存刷新或寫檔。

| GET 路徑 | 回應 |
|---|---|
| `/api/central/live-days?date=YYYY-MM-DD` | 指定日期的 CSV 成交、orders 檔存在旗標、最後持倉快照 |
| `/api/central/live-days/dates` | `{"dates":[{"date":"2026-10-22","row_count":2,"skipped_rows":0,"truncated":false,"header_valid":true}],"truncated":false}`，日期升冪 |

**認證：**兩支 GET 都要求 `X-Limitup-Token` 等於服務程序環境變數 `LIMITUP_API_TOKEN`，沿用既有修改端點的 header／env 與 UTF-8 bytes 常數時間比對。
缺少或錯誤 header 回 401；env 未設或空字串回 503；每次請求讀取 env，loopback 也必須認證。
既有 GET 與 POST 中介層不變。部署前操作者須逐台確認服務程序已載入非空 `LIMITUP_API_TOKEN`，並與 collector 一致；不要印出 token。

`date` 必須是 ASCII `YYYY-MM-DD` 且為真實日期；缺參數、無效日期、路徑穿越回 422。
當日 `YYYY-MM-DD_fills.csv` 不存在回 **404 `{"error":"no_data"}`**，即使 orders 或持倉檔存在也一樣。
輸出目錄不存在不會建立；檔案讀取失敗回 503，拒絕 symlink 或解析後離開輸出目錄的檔案。

單日成功回應範例（其餘診斷旗標見下）：

```json
{
  "date": "2026-10-22",
  "fills": [{
    "fill_key": "<sha1>", "identity": "strong", "order_no": "A001",
    "filled_no": "F001", "symbol": "2330", "side": "buy", "lots": 2,
    "price": 900.5, "filled_at": "2026-10-22T09:00:00.123000+08:00",
    "strategy": 1, "source": "fills_csv"
  }],
  "orders_file": true,
  "positions_after": [],
  "complete": {
    "fills_file": true, "positions_snapshot": true, "all_fills": false,
    "reason": "callback CSV only; reconcile_orders fills are not written to this CSV"
  },
  "skipped_rows": 0, "truncated": false,
  "fills_truncated": false, "positions_truncated": false, "positions_skipped_rows": 0,
  "generated_at": "2026-10-22T13:40:00+08:00"
}
```

- CSV 欄位與 `trading_session.py::_append_fill_csv` 完全一致：
  `recv_time,symbol,action,price,lots,quantity,order_no,filled_no,broker_filled_time,strategy`。
  `quantity` 是股數；現有 writer 的 `lots` 來自 `quantity // 1000`，因此 API 用 `quantity / 1000` 還原，整張回 int、零股回小數。
  `action` 只接受 `buy`／`sell`，價格須正且有限；壞列跳過並計入 `skipped_rows`。
- `filled_at` 優先用 `broker_filled_time`（`HH:MM:SS[.ffffff]` 加指定日期，或完整 ISO 時間），空值才退回 `recv_time`；輸出一律 `+08:00`，日期不符或時間無效的列跳過。
  `fill_key = sha1(UTF-8(date|order_no|filled_no|broker_filled_time|lots))`，lots 使用 API 張數的標準字串（如 `2`、`0.125`）。
  `filled_no` 空值標 `identity="weak"`，可能碰撞，不保證跨系統唯一；端點不自行去重。
- **`strategy=1` 只表示 `_on_fill` 收到回報當下 `order_no` 在本程序的 `order_log` 裡**，0 表示不在；這不是券商認證的策略歸屬，程序重啟或未認領書號可能使自家成交標成 0。
  API 保留 0／1 所有列；hub collector 只選 1。**`reconcile_orders` 補收成交不經 `_append_fill_csv`，因此 CSV 不能證明當日所有成交齊全。**
- `complete.fills_file=true` 僅代表 CSV header 正確、讀取到本次檔案大小、沒有壞列或截斷；不代表帳戶／策略成交完整。
  `complete.all_fills` 固定 false，`reason` 明示補收缺口。`orders_file` 只查檔案存在，完全不解析內容。
- `positions_after` 取 `positions_history.jsonl` 檔案順序中該日期最後一筆有效 record 的 `holdings`，保留其陣列；不依 `saved_at` 排序。
  沒有同日快照回 null；空陣列表示有記錄但無持倉。最後同日 record 的 holdings 非陣列時回 null，不退回舊快照。
  `complete.positions_snapshot` 表示找到快照且反向搜尋途中無壞列；不代表券商庫存完整。
- 有界讀取：單日只解析該日一份 fills CSV，最多 **20,000 列（含壞列）、8 MiB、單行 64 KiB**；必須有換行才視為完整列，未完成的 append 不輸出。
  持倉另讀檔尾最多 **8 MiB／20,000 行**，單行 64 KiB，過舊且不在尾段的快照回 null；可找到尾段最後快照時仍標記 `positions_truncated`。
  `truncated` 是兩種截斷旗標的 OR；遇上限不宣稱整份資料完整。
- dates 最多檢視 **10,000 目錄項目、回傳 366 日期**，按日期升冪；所有 CSV **共享 20,000 列及 8 MiB 預算**，只讀這些 CSV，不讀持倉或 orders。
  `row_count` 是通過同一 parser 的有效成交列數；截斷時只是已讀部分，`header_valid=false`／未能讀取時為 null，不可當作當日成交總數。
  目錄／日期上限或任一 CSV 截斷皆回頂層 `truncated=true`；讀取失敗項目另帶 `error`。

這些端點不判斷交易日；collector 必須依共用契約執行交易日 guard，且只收 node1／2／3／4／6，永遠排除 node5／Lawyer。

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
11. **支撐隊伍消失** (一般股市價買隊伍歸零 / 處置股漲停價委買消失) → 撤委託 + 跌停價限價賣出
    (原「bid1 size 突然掉一半 → 撤單」規則已於 2026-08-19 移除)
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
