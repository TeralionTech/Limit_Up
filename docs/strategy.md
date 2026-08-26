# 交易策略 — 權威說明(以程式碼為準)

> 這份文件反映 **目前實際的程式行為**,是策略的權威來源。改策略請 **先改這裡、再改碼**,兩者要一致。
> 引用位置用「檔名::函式名」(行號會隨改動漂移,函式名較穩)。最後更新:2026-08-25。
>
> 系統:富邦 Fubon Neo SDK 為底的台股漲停鎖死監控 + 交易。平日 08:00 由 `server.py` 的 APScheduler
> (Asia/Taipei)觸發 `runner.py::_run_all_phases`,走完:登入 → 建母體 → 抓漲停價 → 篩選 →
> 預掛 → 9:00 盲送搶進 → 盤中追蹤/出場 → 收盤。

---

## 0. 一日時間軸

| 時間 | 事件 | 位置 |
|---|---|---|
| 08:00 | APScheduler 觸發,重建 timer(避免重複預掛) | `server.py::_daily_trigger` / `runner.py::start` |
| 08:00–08:28 | 登入 → 建母體(去 ETF/權證)→ **月均量 > 500 篩**(風控③)→ 節流 250/min 抓漲停價 / `canDayTrade` / `isDisposition` / 跌停價,重試到 **08:28** deadline | `runner.py::_run_all_phases` / `_filter_low_volume` / `_fetch_limit_ups_with_progress` / `_query_limit_up` |
| 08:00–08:25 | (另一 systemd timer)抓 T30 全額交割/處置名單 | `t30.py::load_untradable` / `runner.py::_load_t30_untradable` |
| 08:30 | 開始訂閱五檔(集合競價開始),book tick 進 filter | `runner.py::_run_all_phases`(SUBSCRIBE)/ `filter.py::make_on_book_handler` |
| 08:30–08:59:58 | **篩選窗口**:漲停鎖死 → mark;出賣單/委買一跌下漲停 → 永久 discard | `filter.py::_on_book` |
| 08:59:58 | **定案**:量減半剔除 → 取 20% 委託量快照 → **預掛漲停價限價單** → 掛 trade handler → 排程 9:00 盲送 | `runner.py::_start_pre_order_timer` / `state.py::final_check_all` / `trading_session.py::place_pre_orders` |
| 09:00:00 | 開盤撮合;handler **換手**(避 price=0 市價列誤判)→ **市價盲送搶進**;隔日賣開始判斷 | `runner.py::_run_all_phases`(handoff)/ `trading_session.py::start_market_chase` |
| 09:00–13:24 | 盤中追蹤 / 出場 / 隔日賣 | `trader.py` / `trading_session.py::_exit_worker` / `update_overnight_book` |
| 13:23 | 撤所有未成交委託(**留倉**) | `trading_session.py::cancel_all_pending` |
| 13:24 | 收盤,寫隔日賣清單 | `runner.py::_write_overnight_file` |

母體:TWSE + TPEx 普通股,**排除 ETF(0 開頭)、權證(5–6 碼)**,以及漲停價抓取失敗者(未訂閱 → 不可 mark)。

---

## 1. 篩選(08:30–08:59:58)

目標:找出「只有委買、買一 = 漲停、無委賣」的**漲停鎖死**股。每筆五檔 tick 進 `filter.py::_on_book`。

### 漲停鎖死判定 → mark
- **條件**:`bids` 非空、買一價 ≥ 漲停價 − 0.001、且 **asks 全空**(每一檔賣量都是 0,空陣列也算)。
- **開盤即鎖(first_tick)vs 盤中鎖上**:第一筆「真實報價」(非空 bids)就滿足鎖死 → 記為 **開盤即鎖漲停**
  (`first_tick`);之後才變漲停 = **盤中鎖上**。開盤即鎖在預掛/預算享**優先排序**。
- 位置:`filter.py::_on_book`(mark)、`state.py::mark`、`state.py::get_marked_prioritized`。

### 淘汰(unmark)→ **永久剔除 + 退訂**
已 marked 的股票,出現任一即淘汰,加入 `discarded` 黑名單(**不可再 mark**)並退訂:
1. **出現委賣單**(asks 有量)。
2. **委買一價 < 漲停價**(價跌下漲停)。⚠️ 9:00 後市價列 price=0 會誤觸此條 → 故 9:00 handler 換手(見 §4)。
3. **08:59:58 量減半**:當下委託量 < 曾經最大值的 ½(`bid_drop_ratio=0.5`),一次性批次剔除。
- 位置:`filter.py::_on_book`(unmark)、`state.py::_unmark` / `final_check_all` / `update_max_bid`。

### 上游閘門(進 mark 之前 / 之外)
- **風控③ 月均量 > 500 張**:近 20 交易日日均量 < 500 張的先從母體移除;**fail-open**(檔缺/過期 →
  CRITICAL 但照常交易,不誤殺)。單位:日K volume 是「股」÷1000 = 「張」。
  位置:`runner.py::_filter_low_volume` / `avg_volume.py::load,filter_universe` / `scripts/compute_avg_volume.py::compute_avg_lots`(離線夜間跑)。
- **T30 全額交割 / 2 次處置**:`SETTYPE≠0`(全額)或 `MARK-W=2`(每筆需 100% 預收)→ 列 untradable,
  預掛與盲送都跳過(`stopped_reason="full_cash_delivery"`)。位置:`t30.py::parse_untradable` / `runner.py::_load_t30_untradable`。
- **手動剔除**:篩選期間可 `POST /api/filter/remove` 把某檔永久移除(`state.py::unmark_manual`)。

---

## 2. 進場

### 門檻
- **只做漲停價 ≤ 500 元** 的股票(用**漲停價**判斷,≈ 現價約 455 以下)。`place_pre_orders` 內
  `limit_up > max_stock_price(500)` 則跳過。

### 下單張數(sizing 疊層)
`trading_session.py::_sized_lots`,依序:
1. **base = 風控算量**(`_calc_lots`):個股金額覆寫 / 全域每檔金額 / `fixed_lots` 三擇一,
   **無條件捨去取整數張、不足一張以一張計、且受總預算餘額硬上限**。
   - 例:個股金額 40 萬、漲停價 60 → 40萬 ÷ (60×1000) = 6.67 → **6 張**(捨去)。
2. **風控① 禁現沖減半**:`canDayTrade=False` → 張數 **÷ 2**。
3. **風控② ≤ 漲停委託量 20%**:`min(張數, ⌊08:59:58 漲停價委託量 × 0.2⌋)`;快照為 0 → 不套此條。

### 08:59:58 預掛漲停價限價單(P)
- 對 marked 清單逐檔掛漲停價限價買單排隊(集合競價),**不重試**(精準一次)。
- 下單即保留預算;T30/超價/處置/算出 0 張 → 跳過;致命拒單(全額/預收/圈存/價格穩定)→ 停該檔並釋放預算。
- 位置:`trading_session.py::place_pre_orders`、`broker.py::place_limit_buy`。

### 09:00 市價盲送搶進(混合停止)
`trading_session.py::_market_chase_worker`,每檔一 daemon thread,9:00 起每 0.1s 送一筆市價單。
- 送 **shortfall = target − 已成交**(不是整包 target → 避免預掛+市價都成交的 2×超買)。
- **停止條件(兩者任一先發生)**:
  - **(A) 首筆委託成功**(券商接受)→ 停 + **撤還沒成交的預掛 P** + **預算轉移**(釋放預掛保留、改保留市價,等額)。
  - **(B) 第一盤成交資訊來**(`first_trade_fired`)且該筆為**非致命**拒單 → 收手停送,**保留預掛 P 續守**
    (2026-08-26 改:市價這一搏沒成,漲停限價 P 留著整天還可能成交,不主動撤;13:23 才兜底撤。與 cutoff 一致)。
- **致命拒單(價格穩定 / 全額 / 預收 / 圈存)→ 停市價、**保留預掛 P 續守**、不動預算**。
  規則(2026-08-26 定案):**P 只在市價委託成功(A)時撤;其餘「市價沒成功」的停止(B / 致命 / cutoff)一律保留 P**。
  價格穩定(6144)時市價被拒,但漲停限價 P 仍是有效排隊單,留著整天還可能成交;13:23 cancel_all 才兜底撤。
- 其他中止:kill switch 關 / 該檔淘汰或出場 / 09:03 時間兜底 / 13:23 / 總曝險硬上限 / 處置股(禁市價,只留預掛)。
- 位置:`start_market_chase` / `_market_chase_worker`、`broker.py::place_market_buy`。

---

## 3. 出場(盤中)

`trader.py` 於 9:00 接手(handler 換手後)。

### 第一盤 gating
- 忽略**試撮 tick**(`isTrial`,集合競價模擬撮合)。
- 首筆真成交:委賣一出現 → 丟棄;成交量 < `first_trade_min_lots`(預設 10 張)→ 丟棄。過關 → 進入 TRACKING。
- 有部位卻被淘汰 → 市價/限價出場且**不退訂**(留著收行情到賣掉);無部位才退訂。
- 位置:`trader.py::on_book`(first books)/ `on_trade` / `_fail_first` / `trading_session.py::on_first_trade`。

### 出場觸發:支撐消失
- **一般股**:市價買隊伍(五檔 price=0 那列)**曾出現過、之後歸零** → 出場。
  (需「曾出現」的 latch,避免開盤前隊伍還沒形成就誤判。)
- **處置股**(不能下市價):看**委買一漲停價委託消失** → 出場。
- 觸發後 TRACKING → PULLED,呼叫 `exit_position`(有曝險才開)。
- 位置:`trader.py::on_book`(exit block)、`trading_session.py::has_exposure`。
- 註:舊「委買一跌下漲停」「委買量 tick 間減半 → 撤單」規則**已移除**,出場只看支撐消失。

### _exit_worker 流程
`trading_session.py::_exit_worker`(背景 thread):
1. 標 `exited=True`(防重複)。
2. 撤 pending 市價單 M + 撤孤兒預掛 P(市價盲送成功後 P 被蓋掉、只剩在 order_log)。
3. **等成交回報窗口**(固定 `_EXIT_FILL_WAIT_SEC`):撤單前已成交/在途的成交都等它落地。
   觸發條件:出場當下有 pending 單、**或**撤到 live P、**或**近期剛撤過買單(`last_buy_cancel_ts`,
   例如總曝險硬上限先撤了單)—— 避免在途成交晚到漏賣變隱形部位。
4. **跌停價限價賣** `filled_lots` 全量(所有股種;`_sell_position`)。**絕不超賣**:`filled_lots` 由
   `_on_fill` 單筆委託封頂,晚到/重複/斷線補收都冪等。
5. 賣不成 → `exited` 回退、標 `sell_failed`(前端「需人工」),下個 tick 可再觸發。
- 位置:`_exit_worker` / `_sell_position` / `exit_position` / `_cancel_orphan_pre`。

---

## 4. 隔日賣(純盤面規則)

昨天買到、未出場的持倉,隔天賣掉。**從 09:00 開盤後開始判斷**(集合競價 08:30–09:00 不動作 —— 隔日賣的
book handler 在 9:00 handler 換手後才掛上)。

- **鎖著就抱**:市價買隊伍存在(price=0 那列)**或** 委買一 ≥ 當天漲停 → 視為鎖住,續抱追蹤。
- **跌下漲停就賣**:委買一價跌下當天漲停價 → **跌停價限價賣**全部持倉(有跌停價用跌停;否則退委買一價公式)。
  ⚠️ **price=0 市價列不算「跌破」**(那是市價買支撐)。
- **賣掉不從清單移除**:記 `sold_lots`;清單靠隔早庫存對帳(`refresh_overnight_inventory`)才清。
- **今日活躍閘門**:今天還在跑的部位(未 `stopped_reason`/未 `exited`)先讓當日出場邏輯處理;出場後才交隔日賣。
- 位置:`trading_session.py::update_overnight_book` / `_overnight_sell_worker` / `refresh_overnight_inventory` /
  `get_overnight_candidates`(13:24 寫檔)。

9:00 開盤鎖著的隔日賣標的:抱著,不因試撮/成交 tick 觸發賣出(隔日賣純 book 驅動,非成交驅動)。

---

## 5. 風控 / 部位控制(IDC)

### 四道風控
1. **禁現沖減半**:`canDayTrade=False` → 部位 ÷2(`_sized_lots`)。
2. **≤ 漲停委託量 20%**:預掛 + 盲送都套(母數 = 08:59:58 漲停價委託量快照)。
3. **月均量 > 500 張**:盤前篩母體(§1,fail-open)。
4. **價格穩定拒單即停**:市價單遇「價格穩定措施」等致命拒因 → 立即停送(修 6144 狂送事故)。

### 部位金額控制
- **個股金額上限**:某標的指定最多 X 元(覆寫全域)。例:X=40 萬、漲停 60 → 40萬÷(60×1000)=6.67 → **6 張**
  (無條件捨去);**不足一張以一張計**(`max(lots,1)`),但仍受總預算硬上限。位置:`_calc_lots` / `symbol_budget.py`。
- **總下單量上限 Y 元**:當日總買進不超過 Y 元(`total_budget`)。例:每檔 40 萬、總 370 萬、10 檔 →
  前 9 檔各 40 萬(360 萬),**最後一檔只下剩餘 10 萬**的量。位置:`_calc_lots`(`remaining = total_budget − budget_used`)。
- **總曝險硬上限(最終防線)**:實際買進累計現金 `_buy_cost_actual`(每筆成交都加、含超買 race)一旦 > `total_budget`
  → `_budget_breached` **單向煞車**:停所有市價盲送 + **撤所有 pending 買單**;已成交部位不動、靠出場賣、當日不再買。
  **只擋買、不擋賣**(出場/隔日賣不受影響)。斷線補收(`reconcile_orders`)也餵此累計。
  位置:`trading_session.py::_on_fill` / `reconcile_orders` / `cancel_all_pending`。

### 預算不變式(帳務)
- `budget_used == Σ(買進已成交 × 漲停 × 1000) + Σ(st.budget_reserved)` —— 保留制,供 sizing 用;
  下單保留、成交轉消耗、撤/拒釋放;**賣出不退預算**(保守日預算)。超買 race 時會略低估 → 由上面的
  `_buy_cost_actual`(實際制、不 floor)當硬上限兜底。

---

## 6. 易踩雷點(改下單/行情程式前先看)

- **五檔 price=0 市價列**:盤中市價單顯示 price=0 且**佔第一列**,真限價檔位從第二列起;拿 `bids[0]` 判斷會誤判(6243)。
- **isTrial 試撮**:08:30–09:00 集合競價的模擬撮合 tick 帶 `isTrial`,誤當真成交會在盤前狂送市價被拒(2491)。
- **Order.price 是字串**;市價用 `None`/`"0"` + `PriceType.Market`。
- **Order.quantity 是股數**(張 × 1000);`modify_quantity` 也收股數。
- **SDK enum `str()` 帶前綴**(`"BSAction.Buy"`),比對前要剝。
- **損益方法拼字** `unrealized_gains_and_loses`(loses 非 losses)。
- **撤/改單要先拿 order object**(`get_order_results`),不能只用 order_no 字串。
- **pfx 空密碼** → `sdk.login` 只傳 3 參數。
- **速率**:富邦下單上限 50/秒;本專案送單過爆發式滑動窗口 `SendRateLimiter`(45/秒,進場/出場/撤單共用)。
