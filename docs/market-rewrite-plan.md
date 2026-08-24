# 市價搶單重寫 + 四道風控 — 完整實作計畫 (待評估)

狀態:**計畫,未實作**。使用者評估後再動工。
基準版本:`db7ce70`(個股金額覆寫)之後。

---

## 0. 已完成 (不要重做,新東西要跟它們相容)

| 項目 | 狀態 | 位置 |
|---|---|---|
| **風控④ 價格穩定拒單即停** | ✅ 已上線 (a818a83) | `_FATAL_REJECT_KEYWORDS` 已含 `"價格穩定"` |
| **個股金額覆寫** | ✅ 已上線 (db7ce70) | `_calc_lots(limit_up, symbol)` 已依 `symbol_budgets` 覆寫 |
| 出場「一律撤+等回報+賣總量」 | ✅ 已上線 (af2fa99) | `_exit_worker` |
| 篩選階段手動剔除 / T30 檢視頁 | ✅ 已上線 (6377954) | `state.unmark_manual` / `/api/t30` |

所以本計畫只剩:**市價搶單主體重寫** + **風控① 禁現沖減半** + **風控② 20% 委託量上限** +
**風控③ 月均量>500 篩選**。(風控④ 已完成。)

---

## 1. 使用者已定決策 (recap)

1. **搶單時機**:9:00:00 起**盲送**市價單(不等首筆成交 tick),每 0.1s 一筆。
2. **停止條件**:**首筆委託成功 (券商接受) 即停**;09:05:00 為時間兜底;價格穩定/致命拒單即停(已做);
   既有 aborts(kill switch / exited / stopped_reason / 13:23 cancel_pending)。
3. **每筆張數**:送 **shortfall = target − 已成交**(扣已買),非整包 target。
   ⚠ **2026-08-25 修正**:原文誤寫「固定送 chase_lots、不扣已買、接受 2×target 超買」——
   那是我(Claude)寫進計畫、非使用者定案,且**丟掉了舊 `_first_trade_worker` 的 shortfall/撤 P/
   預算轉移三項正確行為**,造成 HIGH-1(孤兒 P)+ HIGH-2(2× 超買、budget_used 低估)。
   已復原:送 shortfall、**委託成功即撤預掛剩餘 P**、預算轉移(釋放預掛保留、改保留市價)。
4. **08:59:58 漲停價預掛限價單**:掛著排隊;**市價委託成功後撤掉還沒成交的剩餘 P**
   (致命拒單則保留 P 續守)。原文「保留(不撤)」為上述同一錯誤,已修正。
5. **20% 母數**:08:59:58 當下**漲停價的委託量**快照;預掛 + 盲送都封頂在 20%。⚠ 見待確認 (a)。
6. **禁現沖** (`canDayTrade==False`) → 部位減半。
7. **月均量 > 500 張**才追蹤;**前一晚離線**算好存檔,盤前讀;檔缺 fail-open(照常交易+警告)。
8. **月均量單位**:日K volume 是「股」,÷1000 換「張」再跟 500 張比。

### ✅ 決策已鎖定 (2026-08-24)
- (a) 20% 上限 **預掛限價單 + 盲送市價單都套**。(確認)
- (b) **9:00 盲送、不等成交 tick** 的策略風險(開低沒鎖股會被市價買進弱勢)**已知悉、接受**,照此實作。
- (c) **第一次處置分單** (Issue #1) **本批不做**,留後;本批拆單/量限制先不為它預留特殊分支
  (單純做 20%/禁現沖 的單筆上限即可)。

### 個股金額覆寫「最少一張」(2026-08-24 已補,db7ce70 之後)
`_calc_lots` 覆寫路徑:專屬金額不足一張也下 1 張(仍受總預算硬上限);本批 sizing 疊加以此為 base。

---

## 2. 市價搶單主體重寫 (時間驅動,取代 tick 驅動)

### 現行 (要改掉)
`on_first_trade`(首筆成交 tick 觸發) → `_first_trade_worker` 的 `while True`
(trading_session.py:538-653)。tick 驅動、shortfall=target−filled、無時間兜底。

### 新設計
- **觸發改時間驅動**:`_start_pre_order_timer`(runner.py:596-665)在 08:59:58 `place_pre_orders`
  之後,呼叫新的 `session.start_market_chase(marked)`。
- **`session.start_market_chase(symbols)`**(新):每檔開一 daemon thread,精準等到 **09:00:00.000**
  (骨架仿 `_start_pre_order_timer` 的 `stop_event.wait(0.01)` 細等待),再跑盲送迴圈:
  ```
  while True:
      if not is_live() / st.stopped_reason / st.exited / cancel_pending_time(13:23): break
      if st.is_disposition: break                 # 處置股禁市價 → 只留預掛 (第一次處置分單見 Issue#1)
      if datetime.now().time() >= 09:05:00: break # 時間兜底 (鎖死零成交股不會狂送到整天)
      if st.chase_lots <= 0: break                # 量限制算出 0 張 → 不送
      self._rate.acquire()                        # 45/s 共用窗口 (0.1s=10/s 遠低於)
      try:
          no = broker.place_market_buy(symbol, st.chase_lots)
          _log_order(...) ; st.order_no/kind/status 更新   # ⭐ 每筆都落帳
          break                                   # ⭐ 委託成功 → 停,不再送
      except reject:
          if _is_fatal_reject(e): st.stopped_reason=...; break   # 價格穩定/全額/預收/圈存 即停 (已做)
          stop_event.wait(0.1)                    # 暫時性拒單 → 0.1s 後重試
  ```
- **`on_first_trade` 不再 spawn 市價追**:改為只服務 trader block-1 淘汰
  (小量/委賣出現 → `passed=False` → `stopped_reason="first_check_failed"` → 盲送迴圈自動中止)。
  first_trade_fired 冪等旗標保留(擋重複淘汰觸發)。
- **`_first_trade_worker` 刪除**(其 while-True 主體移進 start_market_chase)。
- **chase_lots**:08:59:58 算好存 st(見第 4 節 sizing 組合),盲送迴圈固定用它。

### 為何「首筆委託成功即停」安全
鎖漲停股:市價買單直接排進隊伍 = 委託成功 = 送一筆就停(不會狂送);
開低沒鎖股:第一筆若成功也停(接受;風險見待確認 b);價格穩定/全額 → 即停(已做)。
09:05 只是保險絲(理論上到不了)。

---

## 3. 四道風控 — 資料源與插入點

### 風控① 禁現沖減半 (零額外 REST)
- **資料**:`intraday.ticker` 回應的 **`canDayTrade`** 欄位(跟漲停價同一包)。
  ⚠ 官方 key 是 `canDayTrade`,**不是** fubon_adapter 誤用的 `canDayBuySell`。
- **抓取**:`_query_limit_up`(runner.py:850-864)加
  `self.day_tradable[sym] = bool(resp.get("canDayTrade"))`(新 dict,仿 `dispositions`)。
- **交給 session**:新 `session.set_day_tradable(dict)`(仿 `set_dispositions`),runner 呼叫點併在
  set_dispositions 旁(runner.py:272-274 附近)。
- **套用**:sizing 時 `if not day_tradable.get(sym, True): lots //= 2`(見第 4 節)。

### 風控② 不超過漲停價委託量 20% (零額外 REST)
- **母數 = 08:59:58 當下漲停價那一檔的委託量快照**(市場掛在漲停價的買量,非我們的單)。
- **抓取**:`_start_pre_order_timer` 內、`place_pre_orders` 前,對每個 marked 標的讀
  `subscriber.get_latest_snapshot(sym)` 的 books → 取「漲停價那一檔的 size」→ `limit_up_bid_vol[sym]`(張)。
  傳進 `place_pre_orders` / 存 st(`st.limit_up_bid_vol`)。
  (⚠ **不走** 當初探查建議的「擴充 update_bid1 帶 size」—— 母數改成快照後不需要。)
- **套用**:`cap20 = floor(0.2 × limit_up_bid_vol)`(見第 4 節)。快照為 0(沒抓到 book)→ 見待確認,
  預設「cap 不生效」(等於不因 0 而擋單,靠其他上限)。

### 風控③ 月均量 > 500 張 (前一晚離線)
- **新腳本 `scripts/compute_avg_volume.py`**(離線,夜間 systemd timer):
  - 登入富邦(復用 broker/login 憑證)→ `get_universe` 取母體 → 逐檔
    `stock.historical.candles(symbol, from, to, timeframe="D", fields="volume")` 取近 ~20 交易日。
  - 均量 `sum(volume)/n / 1000` → 張;寫 `input/avg_volume.json` `{symbol: avg_lots}`。
  - 節流 **55/min**(歷史 K 線獨立 60/min 桶);仿 runner throttle pattern(`_stop_event.wait(throttle)`)。
  - 逐檔 ~1900 檔 ÷ 55/min ≈ 35 分鐘 → **夜間跑,不佔盤前**。
  - 附 `deploy/hit-limit-up-avgvol.{service,timer}`(每晚一次,例 20:00)。
- **runner 套用**:`get_universe` 後(runner.py:213 附近)`_load_avg_volume()` → 剔除 avg < 500 的檔
  (縮母體 → 省下游所有 REST)。檔缺/過期 → **CRITICAL log + 照常交易 (fail-open)**。
- **單位**:json 存「張」(腳本已 ÷1000);runner 直接跟 500 比。

### 風控④ 價格穩定拒單即停 — ✅ 已完成 (a818a83),本批不動。

---

## 4. Sizing 組合 (所有上限如何疊)

下單張數在 `place_pre_orders`(預掛)與 `start_market_chase`(盲送)共用一個算法。
建議抽成 `_sized_lots(sym, limit_up)`,鎖內呼叫,順序:

```
base = _calc_lots(limit_up, sym)          # 個股金額覆寫 or 全域 budget/fixed_lots (已做)
lots = base
if not day_tradable.get(sym, True):        # 風控① 禁現沖
    lots //= 2
cap20 = floor(0.2 * limit_up_bid_vol[sym]) # 風控② (快照>0 才生效)
if limit_up_bid_vol.get(sym, 0) > 0:
    lots = min(lots, cap20)
# (Issue#1 第一次處置: 若納入,再套 每筆≤9、拆單≤29 — 見待確認 c)
return max(0, lots)
```
- **預掛**:`st.target_lots = _sized_lots(...)`;**盲送**:`st.chase_lots = _sized_lots(...)`
  (08:59:58 一起算好存 st,盲送迴圈固定用)。兩者同值 → 超買上限 ≈ 2×該值。
- 月均量(風控③)是**篩選層**(縮 universe),不在 sizing;不影響 _sized_lots。
- 總預算餘額硬上限:`_calc_lots` 已保證(base 已受 remaining 限制)。

---

## 5. 逐檔修改點

- **[runner.py](../runner.py)**:
  - `__init__`:`self.day_tradable: Dict[str,bool] = {}`。
  - `_query_limit_up`:加 `canDayTrade` 解析。
  - Phase:`set_day_tradable` 呼叫;`get_universe` 後 `_load_avg_volume()` 篩 <500。
  - `_start_pre_order_timer`:08:59:58 讀 books 快照算 `limit_up_bid_vol` → 傳 place_pre_orders;
    place_pre_orders 後呼叫 `session.start_market_chase(marked)`。
  - `_monitor_on_trade` / `trader.on_trade`:移除「觸發市價追」(改時間驅動);保留 block-1 淘汰。
- **[trading_session.py](../trading_session.py)**:
  - 新 `set_day_tradable`;`SymbolTrade` 加 `limit_up_bid_vol` / `chase_lots`。
  - 新 `_sized_lots(sym, limit_up)`(第 4 節);place_pre_orders 改用它 + 存 chase_lots。
  - 新 `start_market_chase(symbols)`(第 2 節);刪 `_first_trade_worker`;`on_first_trade` 瘦身為 block-1 gating。
- **[trader.py](../trader.py)**:on_trade 的市價追觸發移除(block-1 first_trade_seen/淘汰保留)。
- **新 [scripts/compute_avg_volume.py](../scripts/) + [deploy/hit-limit-up-avgvol.*](../deploy/)**。
- **前端**:選配 —— ①頁 T30 區塊旁加「月均量<500 已剔除」數量、禁現沖標記(可留後)。

---

## 6. 測試

- 市價搶單:9:00 起盲送、首筆委託成功即停(只送必要筆數)、價格穩定即停(已有)、
  09:05 時間兜底(monkeypatch 時鐘)、每筆都進 order_log、處置股不盲送、chase_lots=0 不送。
  (直呼 chase 主體避免等真時鐘;monkeypatch 09:00/09:05。)
- 風控①:canDayTrade=False → target 減半。
- 風控②:cap20 = 20% 快照;預掛+盲送都受限;快照 0 → 不因 0 擋。
- Sizing 組合:覆寫/全域 × 禁現沖 × 20% 疊加順序正確、受總預算上限。
- 風控③:`scripts/compute_avg_volume` 均量換算(股→張)、<500 篩除、缺檔 fail-open;
  runner `_load_avg_volume` 縮母體。
- 既有回歸:test_full_lifecycle / test_session_money / test_budget_qty / test_trial_tick 改新觸發語意。

## 7. 驗證 / 部署
1. `python -m pytest tests -q` 全綠;前端 `npx tsc --noEmit` + build。
2. 離線腳本手動跑一次產 `input/avg_volume.json` 驗內容 + 筆數。
3. commit → push origin + gitlab;部署 = 拉碼 + scp dist + restart + 佈署 avgvol timer。
4. 上線首日 log:月均量剔除數、禁現沖減半、20% cap、盲送「首筆委託成功即停」的送單筆數。
