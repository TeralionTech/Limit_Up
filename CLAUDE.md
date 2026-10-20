# hit_limit_up — 漲停鎖死監控/交易系統

富邦 Fubon Neo SDK 為底的台股漲停監控 + 交易系統。8:00 自動啟動(APScheduler),
8:30–9:00 盤前試撮篩選漲停鎖死股,9:00 後追蹤/交易。

## 富邦 API 參考來源(改任何下單/行情程式前先查這兩個)

1. **官方 TradeAPI 文件完整 dump**:`../teralion_WEB/llms-full.txt`(3.7MB,**用 grep 查,別整檔讀**)。
   例:`grep -n "place_order" ../teralion_WEB/llms-full.txt`
2. **成熟 adapter 範本**(day-trade-system 實戰用):`../teralion_WEB/backend/app/core/fubon_adapter.py`
   (1529 行:login/re-login atomic swap、下單/撤改單、主動回報、帳務查詢、斷線重連全都有)

## 富邦 SDK 核心用法

```python
from fubon_neo.sdk import FubonSDK, Order
from fubon_neo.constant import TimeInForce, OrderType, PriceType, MarketType, BSAction

sdk = FubonSDK()                      # 正式環境
# 測試環境: FubonSDK(30, 2, url="wss://neoapitest.fbs.com.tw/TASP/XCPXWS")
accounts = sdk.login(id, pwd, pfx_path)            # pfx 密碼「空字串」時只傳 3 參數!
accounts = sdk.login(id, pwd, pfx_path, pfx_pwd)   # 有密碼才傳 4 參數
# 成功判斷: accounts.is_success (不是 truthy); 帳戶物件在 accounts.data[i]

order = Order(
    buy_sell=BSAction.Buy,            # BSAction.Buy / BSAction.Sell
    symbol="2330",
    price="66",                       # ⚠️ 字串! 市價用 None 或 "0"; 整數去小數 str(int(p))
    quantity=2000,                    # ⚠️ 股數! 2 張 = 2000
    market_type=MarketType.Common,    # 整股
    price_type=PriceType.Limit,       # Limit / Market
    time_in_force=TimeInForce.ROD,
    order_type=OrderType.Stock,       # Stock=現股 / DayTrade=現沖先賣 / Short=融券 / Margin=融資
)
result = sdk.stock.place_order(account, order)   # result.is_success / .message / .data(.order_no)

# 撤單/改單: 必須先拿 order object (OrderResult),不能只用 order_no 字串。來源有兩個:
#   (a) 下單當下 place_order 回傳的 result.data (阻塞式下單帶回完整資料) — 本專案撤單優先用這個,免查詢
#   (b) 全清單查詢 — ⚠ 呼叫期間整個 Python process 停擺 (見踩雷點 12),只在沒有 (a) 時才用
r = sdk.stock.get_order_results(account)          # r.data 遍歷比對 .order_no
sdk.stock.cancel_order(account, order_obj)
sdk.stock.modify_price(account, sdk.stock.make_modify_price_obj(order_obj, "67"))
sdk.stock.modify_quantity(account, sdk.stock.make_modify_quantity_obj(order_obj, 1000))  # 股數!

# 主動回報 (callback 簽名 (err, content); set_on_event 是 (code, content))
sdk.set_on_filled(h)         # 成交: content.stock_no/.filled_price/.filled_qty(股數)/.buy_sell/.order_no/.filled_no/.filled_time/.account
sdk.set_on_order(h)          # 委託回報: .order_no/.status/.filled_qty/.error_message/.function_type
sdk.set_on_order_changed(h)  # 改單回報
sdk.set_on_event(h)          # code=="300" = 交易 WS 斷線 → 要 re-login

# 帳務
sdk.accounting.inventories(account)                       # 庫存 (股數)
sdk.accounting.unrealized_gains_and_loses(account)        # ⚠️ 拼字是 loses
sdk.accounting.bank_remain(account)
sdk.stock.filled_history(account, "YYYYMMDD", "YYYYMMDD")
```

## 十三個易踩雷點

1. `Order.price` 是**字串**;市價 = `None`/`"0"` + `PriceType.Market`
2. `Order.quantity` 是**股數**(張 × 1000);`modify_quantity` 也收股數
3. `OrderType.DayTrade` = 現沖**先賣**;融券是 `Short`;現沖回補用 `Stock`+`Buy`
4. SDK enum `str()` 帶前綴(`"BSAction.Buy"`),比對前要剝前綴
5. 損益方法拼字 `unrealized_gains_and_loses`(loses 非 losses)
6. 撤/改單要有 order object(`OrderResult`)— 下單回傳的 `result.data` 或 `get_order_results` 查到的那筆;
   本專案撤單優先用前者(見 12)
7. `pfx_password` 空字串 → `sdk.login` 只能傳 3 個參數
8. 失敗有兩種:raise 例外 或 `result.is_success == False`(都要接)
9. **`get_order_results` 是後檯快照且受「帳務查詢 5/秒」流量控管**:超限回
   `Result{is_success:False, message:"Login Error, 業務系統流量控管"}`(**非例外**);剛 ack 的委託也可能
   延遲數百 ms、逐筆亂序才出現在快照。本專案所有 `get_order_results` 一律走 `broker._query_order_results`
   (5/s 閘門,最小間隔 `QUERY_MIN_INTERVAL_SEC`;鎖內排時槽、鎖外 sleep),查詢失敗 raise `OrderLookupError`
   (含富邦原文)、清單無此書號 raise `OrderNotFound` — **兩者都不是「已成交/已撤」**,絕不可靜默當成功
   (2026-09-09 node3:8 條撤單 thread 0.66 s 內 8 次查詢 → 限流被當「查無」→ 誤標 cancelled → 4 筆隱形裸單)
10. **查無 ≠ 已成交/已撤**:快照含當日**所有**委託(已撤 status 30、全成 50 都還在清單裡),所以撤單前
    「找不到」只代表快照還沒同步/查詢失敗。`order_log` 的 `cancelled` 只能由券商確認寫入
    (`cancel_by_obj` 成功 / 委託回報 ft=30 status 30|40 / 訊息「取消單已不允許取消」);
    撤不到的單留 `pending` + `cancel_state=queued|sent|unconfirmed` 交 `trading_session` 撤單佇列 worker
    重試(退避 0.3→5 s,WARNING@3、CRITICAL@6,trading_end+10 分放棄仍 CRITICAL 點名),
    13:23 收盤另做**券商權威掃單**(快照中 `user_def=="hitlimit"` 且 Buy 且 status ∈ {0,4,8,10} 且
    `after_qty>filled_qty` 者,不看本地標成什麼,一律撤)。盤後可用 `scripts/post_close_check.py` 唯讀核對
11. **撤單回報 `function_type=30`**:第一筆 `status=4` 是撤單請求的**回聲**(不是撤成),`status 30|40`
    才是券商確認撤單;`status 39`/撤單失敗回報是走 **callback 的 `err` 參數**進來的(content 多數欄位
    None),`set_on_order` handler 在 err 分支不可直接 return(舊版就是這樣漏掉所有撤單失敗)。
    撤單失敗原文分類:「成交單/部分成交單已不允許取消」= 已成交(用快照 filled_qty 補成交,不標 cancelled)、
    「取消單已不允許取消」= 早已撤(冪等標 cancelled)、其他 = 重試。新單拒單(ft 0/10,例集合競價 9049)
    的 `order_no` 可能是 None/不在 order_log → 只 log、不動任何 st
12. **`get_order_results` 呼叫期間整個 Python process 停擺**(2026-10-07 node1 隔日賣 4716 比 node4 晚
    3.7 s 送單):行情 tick 不處理、回報不處理、其他下單/撤單的回應擠到呼叫結束那一瞬間才回來;三台 node
    的 tick 空窗與該次查詢耗時吻合到 10 ms 內。同登入家族(IDC 當沖)把清單撐到 1500–2400 筆時每次凍
    1.1–1.8 s(node3 ~190 筆約 0.2–0.5 s)。`cancel_order`/`place_order` 不會凍 process,但呼叫本身是
    阻塞往返,開盤尖峰券商端排隊時可達數秒(10-07 node1:下單 2.8–6.6 s、撤單 0.3–2.7 s)。所以**熱路徑
    絕不可以拉全清單**:`broker._place` 把下單回傳的 `result.data` 記下來,撤單走 `broker.cancel_cached`
    (`cancel()` facade 與撤單 worker 的 `_run_direct_cancels` 都先走這條)直接撤、不查清單。
    - **沒物件才走快照**:缺書號反查認領 / 程序重啟或手動重新連線前下的單 / 掃單孤兒(交易 WS 自動重連
      `re_login` 是同一個 client,物件保留)。
    - **同一書號的免查詢撤單往返中**(別的 thread 送出去還沒回,< `DIRECT_CANCEL_ROUNDTRIP_MAX_SEC` 5 s)→
      `broker.CancelInFlight`:**不重送、不查清單**,佇列項延後 0.3 s 再看(不計 attempts、不記錯誤);
      往返超過 5 s(SDK 呼叫卡死)才改走快照。「直接撤 / 往返中 / 走快照」由 `broker.direct_cancel_route`
      **一個鎖區塊內決定**(分兩次問「在途嗎」「有物件嗎」,中間被別的 thread 認領就會掉進快照查詢 + 重複
      送撤單);`_try_cancel_sync` 在等到送單額度後會再看一次 row,已被別的 thread 撤成(`cancelled`)就不再
      問券商(只在免查詢撤單啟用時;停用 / `CANCEL_DIRECT_OBJ=0` 時照舊一律先問券商)。已知殘留空窗(機率極低、不比修法前差):撤成之後、session 結案之前那幾十微秒內剛好跑到的
      worker 輪仍會查一次清單。
    - **被券商拒**(`broker._on_direct_reject`):終結文案(「…已不允許取消」「已無剩餘量」)→ 丟物件;
      已成交者 `filled_qty` 未知 → 留佇列(`fbc_unverified`),**成交回報到了(row filled)就直接結案**,
      沒到才由下一輪快照核實 — **不可把下單當下的 0 當成沒成交**。「[9050]該筆委託已無剩餘量」在免查詢
      路徑上比照「成交單已不允許取消」處理(`_classify_direct_cancel_error`;沒有成交回報就由快照決定,
      券商端其實是已撤 → 結成 cancelled);快照路徑對這個文案的分類不變(一般重試)。在途文案(「檢查成交回報」= 撤單請求已
      受理、等交易所,10-07 MH014 等了 4.1 s)→ 物件留著繼續免查詢重試,從第一次被拒起算拖過時間上限才
      退回快照:證實前 `DIRECT_CANCEL_INFLIGHT_UNPROVEN_MAX_SEC`(5 s,且算進下一條的熔斷)、證實後
      `DIRECT_CANCEL_INFLIGHT_MAX_SEC`(10 s)。其他未知文案 → 見下一條。
    - **前提尚未在真實環境實測**(富邦文件 `cancel_order` 參數型別是 OrderResult、阻塞式下單回完整資料,但
      範例都用查詢結果)→ 有熔斷。「已證實」= 本連線曾有一筆免查詢撤單**真的撤成**(終結文案不算:券商
      認得這筆 ≠ 能用這個物件撤掉 live 單)。證實前從嚴:SDK 例外連續 `DIRECT_CANCEL_MAX_EXC`(2) 次,或
      放棄了 2 筆(未知文案每筆只給 1 次 / 在途文案拖過 5 s)→ 本次連線整個停用並 CRITICAL(最壞白撞 2 筆
      就回到修法前行為);停用後若有晚回來的撤成 → 自動重新啟用。證實後:例外視為傳輸層暫態(只那一筆退回
      快照,不停用),未知文案每筆給 `DIRECT_CANCEL_MAX_REJECTS`(3) 次。途中斷線 / 已換 SDK 的拒絕與例外
      不算;`re_login` 成功後停用旗標與計數重設(已證實保留),各筆「第一次被拒」的時鐘從新連線重新起算、
      斷線當下還在往返的呼叫不計(斷線期間無法重試,不可拿舊時間讓新連線一上來就熔斷)。
      `/api/trading/status` 的 `direct_cancel` = on / off / env_off,`direct_cancel_proven` = 本連線是否已證實。
    - **第三種失敗樣子:SDK 完全不回應**(不拒、不丟例外;被拒 / 例外兩條計數都看不到)。證實前:往返超過
      5 s 沒回的每一筆也算放棄一筆(`_note_direct_stuck`,2 筆 → 停用 + CRITICAL「沒有回應」)。熔斷計數
      **同一筆單在同一條連線上只計一次**(`_charge_unproven_locked`:沒回應、被未知文案拒、在途拖過上限
      共用)— 要兩筆不同的單出問題才停用,一筆慢單不會單獨把它停掉。
      證實前的同步撤單(`cancel()` facade — 出場 worker 送賣單前會走到)改由 helper thread 送、caller 最多等 5 s
      (`_cancel_cached_bounded`),逾時就入佇列改走快照,**不會卡住出場賣單**。證實後同步撤單照舊在 caller
      的 thread 上直接呼叫(一般的 SDK 卡住 — 修法前的同步撤單同樣沒有逾時)。
    - **SDK 是 Rust/PyO3**:Rust panic 進 Python 是 `pyo3_runtime.PanicException`,繼承 **BaseException**,
      `except Exception` 接不到 → 撤單 thread / 出場 worker 會無聲死掉、賣單送不出去。`broker.cancel_by_obj`
      把 SDK 呼叫丟出的 BaseException 一律轉成 RuntimeError(KeyboardInterrupt / SystemExit 照舊往外丟),
      扇出 thread 另有一層 `except BaseException` 留結果;查清單那一支(`_run_query_flight`)同樣轉成
      `OrderLookupError`。**尚未比照處理**:`place_order`(`broker._place`)與 `accounting.inventories` 仍只接
      `except Exception`。新增其他 SDK 呼叫點時比照處理。
    - 本地 row 已終結(回報先到)者不送撤單;但成交量曾被「券商權威補入」的列(`auth_filled`,重連補收 +
      晚到回報可能雙算成 filled 而券商仍有剩餘量 live)照送,讓券商回答。
    - **2026-10-14 起預設關閉**(`.env` 未設 `CANCEL_DIRECT_OBJ` = 舊路徑;要啟用設 `CANCEL_DIRECT_OBJ=1` 並重啟):
      富邦文件各語言都只寫「委託列表透過 GetOrderResults 取得」、批次刪單範例也只用查詢結果,沒有任何地方示範
      用 `place_order` 回傳物件撤單;使用者決定在真實環境驗證前不冒險。驗證方式(待使用者同意再做):非交易時間
      用 1 張跌停價限價買單(= 預約單)→ 立刻用 `place_order` 回傳物件 `cancel_order` → 快照確認 status 30;
      scratch 已有 `dc_probe.py`(quote 階段在 node6 跑過,沒下單)。
    - `.env` 設 `CANCEL_DIRECT_OBJ=0`(或不設)= 整個關掉 — 還原的是撤單**路徑**(先查清單再撤);撤單重試退避
      「從該輪結束起算」是獨立修改、任何模式都生效(見本條最後),所以關掉後的重試節奏與修法前不完全相同
      (每次重試最多晚 min(退避, 該輪耗時),換來不再零間隔連續拉全清單)。盤後看 journal:`CANCEL … (免查詢)` =
      直接撤;`order_results ok=… n=… ms=…` 每一行 = 一次凍結;另看 orders.csv 的 `latency_ms`(下單/撤單往返)。
    - 仍會拉全清單的地方:13:23 收盤券商權威掃單與 13:24 `trading_end`、盤中 09:05–13:20 每 60 s 的
      `intraday_reconcile_once`、**總曝險硬上限觸發時的 `cancel_all_pending`(先掃單再撤,落在開盤超買那
      幾秒;尚未改)**、重連補收 `reconcile_orders`、缺書號反查 `_recover_order_no`;另外隔日賣送單前的
      `get_inventories` 也是同步查詢(尚未改)。撤單重試的退避從該輪**結束**起算(`_cancel_retry_later`),
      免得一輪含一次查詢時零退避、連續拉全清單。
13. **行情 WS 會斷線 / 停擺,而且重連之後沒有初始畫面**(2026-10-12:hub 8 條 socket 08:48 一起停擺、其中
    5 條 `Connection timed out - goodbye` 之後從未重連 → 08:59:50 凍結的 14 檔全是斷線前的畫面 → 五台 node
    對 08:57:48 已打開的 6872 預掛;幸好開盤前賣單出現、撤單在 09:00 前 0.5 s 被券商確認)。
    - **盤前試撮五檔只在變動時推送、訂閱當下沒有初始快照** → 「重連成功」≠ 那條連線上的檔可以再相信。
      `Subscriber.is_stale(sym)` = 所屬連線斷線 / 停擺中,或重連後這一檔還沒收到新的 books(以「第幾段連線」
      計數,不比時間)。`runner._stale_marked`:hub 的 `marked_snapshot()` 不把過期的檔放進 `symbols`
      (另列 `stale_excluded`),08:59:50 凍結時 CRITICAL 點名;standalone 預掛 / 盲送同樣排除;node 只排除
      **自己的行情連線斷線中**的檔(`down_symbols` — node 08:59:50 才訂行情,沒畫面是常態,名單以 hub 為準)。
      node 另外記住 hub 最後一份快照的 `stale_excluded`(`_node_hub_stale`):hub 的連線若在 08:59:50 首份名單
      **之後**才被判定斷線,首份名單的檔 node 不會跟著拿掉 → 預掛前排除「hub 說過期、本機也還沒收到五檔」的檔
      (本機有五檔就照本機的);凍結後補收的檔若只是在 `stale_excluded` 裡,不做永久淘汰(`unmark_hub_dropped`)。
      殘留空窗:hub 的連線在 08:59:36 之後才停擺的話,20 s 的看門狗來不及在 node 補收結束(08:59:57)前發現。
      標記本身不動:收到新五檔由 filter 照常 mark / unmark,仍鎖住就自動回到名單(node 的凍結後補收會收進去)。
    - **斷線的來源**:fugle-marketdata 在 import 時設了 5 s 的 socket timeout,一個 frame 讀到一半卡超過 5 s
      連線就被自己砍掉;平常日單一條連線就有 3~4 s 的延遲,所以以後還會發生。閒著的連線不會因此斷
      (09-09 另有一次兩條 `Connection to remote host was lost`,當時沒人發現)。
    - **自動重連**(`subscriber.py` 檔頭 `MD_*`,比照 day-trade 生產分支的 `fubon_adapter` / `fugle_connect_patch`):
      每條 socket 各自 single-flight;先 `disconnect()` 舊的(不然 `auth_status` 還停在已認證,`connect()` 會
      立刻假成功)→ 開連線 → 重訂 books + trades、舊 subscription id 作廢;退避 2,4,8,16,32,60 s、不設次數上限。
      **不呼叫函式庫的 `connect()`**(連不上時它是不會返回的忙等,吃滿一顆 CPU;開機第一次也一樣不呼叫 —
      node 是 08:59:50 才連行情):自己起收訊 thread、sleep 輪詢認證狀態、12 s 期限(`_open_socket`)。
      舊收訊 thread 沒結束就不在同一個物件上開第二條(Linux 上要 ~10 s 才醒,最多等 12 s)。認證類失敗 /
      舊迴圈不結束 / 連續 3 次失敗 → 這條換**全新 session**(新 SDK login,每條 60 s 最多一次;換下來的 SDK
      要 logout,primary_sdk 除外)。`Maximum number of connections reached` → 等 20 s 讓券商回收名額再用新
      session 連。**不依賴 disconnect 事件**(舊版 websocket-client 重用物件時不再發;生產機版本沒 pin,
      10-13 查到 hub 1.9.0、node 1.9.2):收訊 thread 結束 = 連線結束;連線層致命錯誤一出現就先標斷線
      (關閉事件會晚約 3 s)。重連成功後 10 s 內又斷(連上就被踢)→ 接著上一輪繼續退避、累計失敗,不從 2 s 重來。
      重連 thread 不可以無聲消失:迴圈接 BaseException(換 session 會呼叫 Rust 的 login,panic 不是 Exception),
      thread 起不來 / 異常結束就放掉 single-flight,看門狗看到「斷線中卻沒人在重連」會重新啟動。
      換 session 失敗(登入失敗、換行情 token 失敗)的那個新 SDK 一定登出。
    - **看門狗**:一條 socket 安靜 `MD_IDLE_PING_SEC`(5 s)就送 ping,pong 也沒有、總共約 `MD_STALL_SEC`(20 s)
      沒有任何訊息 → 當成停擺、強制重連(SDK 內建心跳要 60~90 s)。`MD_STALL_SEC=0` 停用。
      **看不到的情況**:連線活著、pong 照回,但行情本身變慢 / 不來(10-12 存活的那 3 條就是變慢後自己恢復)。
    - **尚未在真實環境驗證**:同一組 token 能不能重連(不能就會走換 session)、ping 是否都在幾秒內回 pong、
      Linux 上舊收訊 thread 多久結束。上線後第一次發生時看 journal:`行情 socket #N 斷線/停擺` → `重連成功`
      (或 `已換全新 session`)、`凍結名單排除 N 檔`、`預掛排除 N 檔`(全是 CRITICAL);`/api/status` 的
      `tick_stats.sockets` 有每條的狀態。盤前人工檢查:`journalctl -u hit-limit-up | grep -c "行情 socket"`。
    - **淘汰的當下就停盲送**:`session.stop_entry(sym, reason)` 只設 `st.stopped_reason`、不碰券商;
      `cancel_symbol_orders(_async)` 一進來就先呼叫(以前要等同步撤單往返回來才設 → 10-12 五台在淘汰後又送
      了 26 筆市價單;09:00 之後 Trader 首盤淘汰走同一支,那時候送出去的單會被受理)。`_chase_send_one` 送出前
      再看一次(已停 / 已出場 / 硬上限 → 回 `"stopped"` 不送)。淘汰當下已經交給 SDK 的單收不回來:被受理就由
      「送出後發現已停」撤 M + P;**若在撤掉之前就成交,當天沒有自動賣** — 所以「今天下過單」的日子
      `_trade_phase` 不再因為名單空就在 09:00 提早收工(`session.has_entries_today`):照走 Trader 路徑到
      13:24,把那筆部位寫進隔日賣清單(以前提早收工 = 沒人記,隔天只剩一行「庫存有但清單沒有」的 CRITICAL)。
      ⚠ 這一項要比免查詢撤單(踩雷點 12)**先上或一起上**:舊碼撤單前的全清單查詢剛好凍住 process 0.1~0.2 s,
      反而壓低了多送的筆數;免查詢撤單拿掉那個凍結後,沒有這一項會多送更多。
    - **第一盤淘汰 vs 成交回報的競態**(2026-10-20 8089,node2/3/6):預掛單 09:00:07.085 在券商端成交,第一筆五檔
      (委賣)比成交回報早 97~300 ms 到 → `_fail_first` 讀本機 `filled_lots` 為 0 → 以前只撤單 + 退訂;撤單被拒
      「成交單已不允許取消」補進 3 張後沒人出場(晚成交保護要 `exited` 才動)→ 抱到 13:24 才進隔日賣清單。修法:
      淘汰一進來先 `session.mark_exit_on_fill`(`st.exit_on_fill` = 淘汰原因),之後**任何**買進成交落地(成交回報、
      撤單被拒依快照補成交、重連補收)都 `_exit_on_fill_due_locked` → `exit_position("fill_after_elimination…")`
      整檔賣;退訂只在「沒部位、也沒有買進委託在途(`has_live_buy_order`)」時做,有單在途就留著收行情。
      盤前 unmark(`runner._unmark_stop_and_unsub`,08:59:58 預掛後才被剔除、撤單輸給集合競價)同型,同樣先標旗標。
      出場被冷卻 / 閘門擋下的重試:trader 對淘汰檔每 5 s 問一次 `exit_pending_after_elimination`,session 撤單
      worker 迴圈每 30 s 與重連後 `retry_elimination_exits`。終結快照(status 40/50)的成交量記成 `row["auth_final"]`,
      之後同一批成交的回報不再加(部分成交 1 張補入後同筆回報晚到曾會雙算成 2 → 超賣);活單補入維持相加封頂。
      新增其他補成交的地方一律加同一個檢查。

## 台股市場規則 (影響判斷邏輯)

- **開盤集合競價 (8:30–9:00) 只收限價單**;市價單只在盤中逐筆 (9:00 後) 存在
- **盤中市價單在五檔顯示 price=0 且佔第一列**(真正限價檔位從第二列起)—
  拿 `bids[0]` 價格做判斷會誤判 (歷史教訓: 6243 誤撤事件)
- 行情 REST rate limit: 日內行情 300/min (`LIMIT_UP_MAX_PER_MIN=250` 節流);歷史 K 線 60/min
- **交易 API 速率上限**(llms-full.txt 明定):**下單 50/秒**、批次下單 10/秒、
  帳務查詢 5/秒、連線數 10。本專案送單全過 session 的 `SendRateLimiter`
  **爆發式滑動窗口**(`ORDER_MAX_PER_SEC=45`/秒,一秒最前面可全部送出,非均勻間隔;
  進場/出場/撤單共用額度)。帳務/委託查詢的 5/秒閘門**在 broker 層**(`_query_order_results`,
  所有 `get_order_results` 唯一入口;撤單 worker 對有下單回傳物件的書號免查詢直接撤,
  其餘一輪只查一次快照再扇出撤單 — 見踩雷點 12);
  `ORDER_MIN_INTERVAL_SEC=0.2` 只作市價追**失敗後**的單檔退避
- **已接受風險(2026-09-09 定案)**:市價盲送管線化下,同檔最多 `CHASE_MAX_INFLIGHT`×lots 張在飛,
  第一筆成功後其餘「多送的 M」靠撤單佇列在 ≤1.5 s 內收掉(收不掉 = 超買,由出場全量賣;絕不變隱形裸單)。
  註(2026-10-07 實測):這個 1.5 s 是自**收到**該筆下單回覆起算、券商回覆正常時的數字,不含券商回覆延遲 —
  當天 node1 得知第一筆成功已晚 2.3 s、多送的回覆 1.5 s 後才到齊,舊碼 8.4 s 後才撤完

## 本專案架構速覽

- `server.py` FastAPI + APScheduler(平日 8:00 Asia/Taipei 觸發 runner)
- `runner.py` 主流程 singleton:login → universe → 抓漲停價(節流+重試到 08:28)→ subscribe → 篩選 → trader
- `filter.py` 8:30–9:00 mark/unmark 邏輯(漲停鎖死=只有委買+買一=漲停)
- `trader.py` 9:00 後追蹤(第一盤檢查 → 盤中追蹤);監控與下單分離
- **月均量硬限制 (2026-10-14 使用者定案)**:每檔張數 ≤ floor(近 20 日均量(張) × `AVGVOL_CAP_PCT`/100),預設 1%,
  `_sized_lots` **最後一道**(疊在預算 / 個股金額覆寫「最少一張」/ 禁現沖減半 / 20% 委託量之後,預掛與盲送共用);
  月均量**未知 → 0 張、不下單**(fail-closed,`stopped_reason=avgvol_unknown`,CRITICAL 點名)。母數來源:hub/standalone 讀
  `input/avg_volume.json`(17:30 timer,1,943 檔),marked 裡沒資料的檔盤前 `_start_avg_lots_filler` 每 2 s 用 REST 日 K 補抓
  (每檔最多 2 次);hub 的 marked 快照每檔帶 `avg_lots`(None = 不知道),node 由快照累積(首份 + 凍結後補收)交 `session.set_avg_lots`。
  node 沒有 avg_volume.json,**完全靠快照** → hub 與 node 要一起部署,快照沒有任何 avg_lots 時 node CRITICAL。
  每台 node 各自套 → **艦隊總量 = node 數 × pct**(6 台 1% ≈ 6%,加 node1 帳戶上另一套程式再多)。
  **`AVGVOL_CAP_PCT=0` 是 node 端的放行開關**(hub 不論自己的值都讀檔 / 補抓 / 快照帶 avg_lots);母數三個 dict 每天
  `_reset_daily_state` 清空(昨天補抓的值不可蓋今天的檔),優先序 檔 > REST 補抓;0 / NaN / inf 都算未知;凍結後補收對已知的檔
  也會接後到的 avg_lots。
  status 頂層 `avgvol_cap_pct / avg_lots_known`(不放 `params`,中控 shape 鎖死那四個 key),每檔 `avg_lots / avgvol_cap_lots / size_cap_reason`。
- **「開始隔日賣」(2026-10-14)**:`POST /api/trading/arm {armed:true, overnight_only:true}` →
  `session.set_armed(True, overnight_only=True)`:一樣 armed(隔日賣 / 既有部位出場 / 撤單都看 `is_live()`),
  但 `entry_enabled=False` → **所有新進場路徑看 `entry_allowed()`**(08:59:58 `place_pre_orders`、08:59:59 盲送
  cadence、`_chase_send_one` 送出前)一律不送;不要求預算 pre-flight。`status().arm_mode` = off / trade /
  overnight_only。`roll_day` 重設 `entry_enabled=True`(每天重選)。盤中由搶單切成隔日賣只停「之後的送單」:
  預掛迴圈下一檔起中止;切換當下在 SDK 往返中的市價單若被受理 → 視同淘汰撤 M+P(`aborted`);已在券商端的
  買單不撤(WARNING 點名)。API 沒帶 `overnight_only` 的 `{armed:true}`(舊前端 / central)**不會**把隔日賣模式升級成
  搶單(回 `unchanged`);前端只在 status 有 `arm_mode` 時顯示按鈕,且要後端回 `overnight_only:true` 才算開成功
  (舊後端會忽略欄位、以正常模式 arm → 前端自動停止交易並報錯)。新增其他買進路徑時一律加 `entry_allowed()` 閘門。
- `broker.py` 富邦真單 client(`get_order_snapshot`/`cancel_by_obj`/`cancel` facade;`cancel_cached` +
  `direct_cancel_route` = 用下單回傳物件免查詢撤單;`OrderLookupError` vs `OrderNotFound`);`trading_session.py` 模式/連線/預算/kill switch + **撤單佇列 worker**
  (`request_cancel` → `_cancel_worker_run_once`;`cancel_all_pending` 含券商權威掃單 + 同步 drain;
  盤中 09:05–13:20 每 60 s `intraday_reconcile_once`,`INTRADAY_SWEEP_DRY_RUN` 預設 true 只 log)
- `state.py` marked/discarded/開盤即鎖 (thread-safe);`subscriber.py` 多帳號多 socket WS(斷線自動重連 + 過期判定,見踩雷點 13)
- 前端 React+Vite 在 `frontend/`(dist 不進 git,本機 build 後 scp 上 VPS)

## 部署

- **部署源 = 此 repo 的 `main` 分支**;VPS `root@45.76.222.150` 的 `/opt/hit_limit_up/repo`
- 更新:`git fetch origin main && git reset --hard origin/main && systemctl restart hit-limit-up`
- 服務:systemd `hit-limit-up`(port 8100);log:`journalctl -u hit-limit-up -f`
- `.env` 實體在 `/opt/hit_limit_up/secrets/.env`(symlink 到 app 目錄);憑證 `/opt/hit_limit_up/certs/`

## 中控接入 (2026-10-13 上線;teralion-central 同機 hub :8001)

- **token 中介層** (`server.py`):POST/PUT/DELETE `/api/*` 需 header `X-Limitup-Token` = `.env LIMITUP_API_TOKEN`
  (fail-closed:沒設 = 遠端 POST 全 401,啟動 log CRITICAL);GET 開放;真本機 loopback (無 CF-Connecting-IP) 放行。
  hub + 所有 node + 中控用同一把。前端右上「🔑 token」存 localStorage。
- **中控用的端點**:`GET /api/central/status` (單一彙整,poller 每 5 s 打;`ready.block_reason` = arm 會被哪一關擋)、
  `POST /api/trading/auto_connect` (讀**機上** .env 的 FUBON_* 自己連線,帳密不經網路)、既有 `/api/trading/mode|arm|disconnect`。
  中控每日:08:30 auto_connect → 08:50 mode real + arm → 13:45 disarm (時間在中控「自動化設定」頁改)。
- **通知** (`notifier.py`):掛在 root logger 的 handler,把 CRITICAL 全部、出場/隔日賣/緊急全平/armed/連線/成交回報
  等規則 (`should_forward`) 轉到 `CENTRAL_FORWARD_URL/api/central/notify`,中控加 `[<label>-LimitUp-<level>]` 發 Signal。
  30 s 同訊息去重、2 s 合併一封、每分鐘 60 則上限;交易路徑零改動。`.env`:CENTRAL_FORWARD_URL / CENTRAL_VPS_ID / CENTRAL_LABEL。
- **設定落檔** (`trading_config.py`):`/api/trading/mode` 與 `/params` 改完寫 `output/trading_config.json`,
  啟動套回 mode / 預算 / 張數 (**armed 永不還原**)。重啟後不再退回 sim / 預算 0 → 中控自動 arm 才過得了 pre-flight。
- 部署:`ops/limitup_deploy/deploy_limitup_commit.py --commit <sha> --hosts ... --dist frontend/dist`
  (守門:平日 07:50–13:40 拒跑、runner 在跑 / 券商已連線 / armed 的那台不動)。
