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

# 撤單/改單: 必須先拿 order object,不能只用 order_no 字串
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

## 八個易踩雷點

1. `Order.price` 是**字串**;市價 = `None`/`"0"` + `PriceType.Market`
2. `Order.quantity` 是**股數**(張 × 1000);`modify_quantity` 也收股數
3. `OrderType.DayTrade` = 現沖**先賣**;融券是 `Short`;現沖回補用 `Stock`+`Buy`
4. SDK enum `str()` 帶前綴(`"BSAction.Buy"`),比對前要剝前綴
5. 損益方法拼字 `unrealized_gains_and_loses`(loses 非 losses)
6. 撤/改單要先 `get_order_results` 找 order object
7. `pfx_password` 空字串 → `sdk.login` 只能傳 3 個參數
8. 失敗有兩種:raise 例外 或 `result.is_success == False`(都要接)

## 台股市場規則 (影響判斷邏輯)

- **開盤集合競價 (8:30–9:00) 只收限價單**;市價單只在盤中逐筆 (9:00 後) 存在
- **盤中市價單在五檔顯示 price=0 且佔第一列**(真正限價檔位從第二列起)—
  拿 `bids[0]` 價格做判斷會誤判 (歷史教訓: 6243 誤撤事件)
- 行情 REST rate limit: 日內行情 300/min (`LIMIT_UP_MAX_PER_MIN=250` 節流);歷史 K 線 60/min

## 本專案架構速覽

- `server.py` FastAPI + APScheduler(平日 8:00 Asia/Taipei 觸發 runner)
- `runner.py` 主流程 singleton:login → universe → 抓漲停價(節流+重試到 08:28)→ subscribe → 篩選 → trader
- `filter.py` 8:30–9:00 mark/unmark 邏輯(漲停鎖死=只有委買+買一=漲停)
- `trader.py` 9:00 後追蹤(第一盤檢查 → 盤中追蹤);監控與下單分離
- `broker.py` 富邦真單 client;`trading_session.py` 模式/連線/預算/kill switch
- `state.py` marked/discarded/開盤即鎖 (thread-safe);`subscriber.py` 多帳號多 socket WS
- 前端 React+Vite 在 `frontend/`(dist 不進 git,本機 build 後 scp 上 VPS)

## 部署

- **部署源 = 此 repo 的 `limit_up` 分支**;VPS `root@45.76.222.150` 的 `/opt/hit_limit_up/repo`
- 更新:`git fetch origin limit_up && git reset --hard origin/limit_up && systemctl restart hit-limit-up`
- 服務:systemd `hit-limit-up`(port 8100);log:`journalctl -u hit-limit-up -f`
- `.env` 實體在 `/opt/hit_limit_up/secrets/.env`(symlink 到 app 目錄);憑證 `/opt/hit_limit_up/certs/`
