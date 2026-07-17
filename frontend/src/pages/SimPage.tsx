import { ReactNode, useEffect, useState } from 'react'
import { api, TraderSummary, FirstStageRow, TrackingRow, TradingStatus, SymbolTrade, OrderRow } from '../api'

export default function SimPage() {
  const [sum, setSum] = useState<TraderSummary | null>(null)
  const [minLots, setMinLots] = useState<string>('')
  const [savedLots, setSavedLots] = useState<number | null>(null)
  const [saveMsg, setSaveMsg] = useState('')

  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      try {
        const s = await api.traderSummary()
        if (!cancelled) setSum(s)
      } catch { /* ignore */ }
    }
    tick()
    const id = window.setInterval(tick, 1500)
    return () => { cancelled = true; window.clearInterval(id) }
  }, [])

  // 載入目前張數參數
  useEffect(() => {
    api.getTraderParams().then(p => {
      setSavedLots(p.first_trade_min_lots)
      setMinLots(String(p.first_trade_min_lots))
    }).catch(() => {})
  }, [])

  async function saveLots() {
    const n = parseInt(minLots, 10)
    if (!n || n < 1) { setSaveMsg('請輸入 >= 1 的整數'); return }
    try {
      await api.setTraderParams({ first_trade_min_lots: n })
      setSavedLots(n)
      setSaveMsg(`✓ 已套用 ${n} 張`)
      window.setTimeout(() => setSaveMsg(''), 3000)
    } catch (e: any) {
      setSaveMsg(`失敗: ${e.message}`)
    }
  }

  return (
    <div className="space-y-4">
      {/* 交易模式 (模擬/真實) — 連線/預算/開始交易 */}
      <TradingPanel />

      {/* 參數列 — trader 未啟動也可先調 */}
      <section className="bg-white rounded-lg shadow p-4 flex items-end gap-3 flex-wrap">
        <div>
          <label className="block text-xs text-gray-600 mb-1">
            第一盤最小成交張數 (小於此淘汰)
          </label>
          <input
            type="number" min={1} value={minLots}
            onChange={e => setMinLots(e.target.value)}
            className="border rounded px-3 py-2 w-32 font-mono"
          />
        </div>
        <button
          onClick={saveLots}
          className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded font-medium"
        >
          套用
        </button>
        {savedLots != null && (
          <span className="text-xs text-gray-500 pb-2">目前生效: {savedLots} 張</span>
        )}
        {saveMsg && <span className="text-sm pb-2 text-green-700">{saveMsg}</span>}
      </section>

      {!sum || !sum.trader_active ? (
        <div className="bg-white rounded-lg shadow p-8 text-center text-gray-500">
          <p className="text-lg">📝 模擬/真實執行頁</p>
          <p className="text-sm mt-2">
            Trader 尚未啟動 — 9:00 篩選結束且 <code className="bg-gray-100 px-1 rounded">SKIP_TRADER=false</code> 時進入
          </p>
        </div>
      ) : (
        <>
          {/* 統計 */}
          <section className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <StatCard label="Watchlist" value={sum.watchlist_total ?? 0} />
            <StatCard label="追蹤" value={sum.n_tracking ?? 0} color="green" />
            <StatCard label="撤單" value={sum.n_pulled ?? 0} color="red" />
            <StatCard label="第一盤淘汰" value={sum.n_first_failed ?? 0} color="orange" />
          </section>

          {/* 區塊 1: 第一盤檢查 */}
          <section className="bg-white rounded-lg shadow p-4">
            <h2 className="font-semibold mb-3">🔔 區塊 1 — 第一盤資料 (開盤首筆 books + 首筆成交)</h2>
            {sum.first_stage && sum.first_stage.length > 0 ? (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="text-xs text-gray-500 border-b bg-gray-50">
                    <tr>
                      <th className="text-left py-2 px-2">標的</th>
                      <th className="text-right py-2 px-2">漲停價</th>
                      <th className="text-right py-2 px-2">首筆委買一</th>
                      <th className="text-right py-2 px-2">首筆委賣一</th>
                      <th className="text-right py-2 px-2">首筆成交價</th>
                      <th className="text-right py-2 px-2">首筆成交量</th>
                      <th className="text-left py-2 px-2">結果</th>
                      <th className="text-left py-2 px-2">委託/成交</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sum.first_stage.map(row => <FirstStageTr key={row.symbol} row={row} />)}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="text-gray-400 text-sm py-4 text-center">watchlist 空</div>
            )}
          </section>

          {/* 區塊 2: 盤中追蹤 */}
          <section className="bg-white rounded-lg shadow p-4">
            <h2 className="font-semibold mb-3">📈 區塊 2 — 盤中追蹤 (第一盤通過的標的)</h2>
            {sum.tracking && sum.tracking.length > 0 ? (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="text-xs text-gray-500 border-b bg-gray-50">
                    <tr>
                      <th className="text-left py-2 px-2">標的</th>
                      <th className="text-right py-2 px-2">漲停價</th>
                      <th className="text-right py-2 px-2">委買一價</th>
                      <th className="text-right py-2 px-2">委買一量</th>
                      <th className="text-left py-2 px-2">狀態</th>
                      <th className="text-left py-2 px-2">委託/成交</th>
                      <th className="text-left py-2 px-2">警示</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sum.tracking.map(row => <TrackingTr key={row.symbol} row={row} />)}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="text-gray-400 text-sm py-4 text-center">
                尚無通過第一盤的標的
              </div>
            )}
          </section>
        </>
      )}
    </div>
  )
}


function FirstStageTr({ row }: { row: FirstStageRow }) {
  const fb = row.first_books
  const ft = row.first_trade
  const priceAtLimit = ft && Math.abs(ft.price - row.limit_up) < 0.001
  return (
    <tr className="border-b">
      <td className="py-1.5 px-2 font-mono font-semibold">
        {row.symbol}
        {row.first_tick && <span title="開盤即鎖 (8:30 首筆報價就漲停)" className="ml-1">🔒</span>}
      </td>
      <td className="py-1.5 px-2 text-right font-mono text-red-600">{row.limit_up?.toFixed(2)}</td>
      <td className="py-1.5 px-2 text-right font-mono">
        {fb ? `${fb.bid1_price?.toFixed(2)} × ${fb.bid1_size}` : <Wait />}
      </td>
      <td className="py-1.5 px-2 text-right font-mono">
        {fb ? (
          fb.ask1_size > 0
            ? <span className="text-red-600">{fb.ask1_price?.toFixed(2)} × {fb.ask1_size} ⚠</span>
            : <span className="text-gray-400">—</span>
        ) : <Wait />}
      </td>
      <td className="py-1.5 px-2 text-right font-mono">
        {ft ? (
          <span className={priceAtLimit ? 'text-red-600' : ''}>{ft.price?.toFixed(2)}</span>
        ) : <Wait />}
      </td>
      <td className="py-1.5 px-2 text-right font-mono">{ft ? `${ft.lots} 張` : <Wait />}</td>
      <td className="py-1.5 px-2 text-xs">
        {row.status === 'discarded_first' ? (
          <span className="bg-red-100 text-red-700 px-2 py-0.5 rounded" title={row.fail_reason}>
            淘汰 — {failLabel(row.fail_reason)}
          </span>
        ) : row.status === 'waiting' ? (
          <span className="bg-gray-100 text-gray-600 px-2 py-0.5 rounded">等第一盤</span>
        ) : (
          <span className="bg-green-100 text-green-700 px-2 py-0.5 rounded">✓ 通過</span>
        )}
      </td>
      <td className="py-1.5 px-2 text-xs"><TradeCell trade={row.trade} /></td>
    </tr>
  )
}


function TradeCell({ trade }: { trade?: SymbolTrade | null }) {
  if (!trade || (!trade.order_status && trade.filled_lots === 0 && !trade.stopped_reason)) {
    return <span className="text-gray-300">—</span>
  }
  const kindLabel: Record<string, string> = {
    pre_limit: '預掛限價', market_buy: '市價買', market_sell: '市價賣',
  }
  const statusLabel: Record<string, string> = {
    pending: '排隊中', cancelled: '已撤', rejected: '被拒', done: '完成',
  }
  return (
    <div className="space-y-0.5">
      {trade.filled_lots > 0 && (
        <div className="text-green-700 font-semibold">
          成交 {trade.filled_lots}/{trade.target_lots} 張
          {trade.avg_price > 0 && <span className="text-gray-500"> @ {trade.avg_price}</span>}
        </div>
      )}
      {trade.order_status === 'pending' && (
        <div className="text-blue-700">
          {kindLabel[trade.order_kind] || trade.order_kind} {statusLabel.pending}
        </div>
      )}
      {trade.exited && <div className="text-red-700 font-semibold">已出場</div>}
      {trade.order_status && trade.order_status !== 'pending' && !trade.exited && (
        <div className="text-gray-500">{statusLabel[trade.order_status] || trade.order_status}</div>
      )}
      {trade.stopped_reason && (
        <div className="text-orange-600" title={trade.stopped_reason}>停止進場</div>
      )}
    </div>
  )
}


function TrackingTr({ row }: { row: TrackingRow }) {
  const bidAtLimit = Math.abs(row.bid1_price - row.limit_up) < 0.001
  return (
    <tr className={`border-b ${row.status === 'pulled' ? 'bg-red-50' : ''}`}>
      <td className="py-1.5 px-2 font-mono font-semibold">
        {row.symbol}
        {row.first_tick && <span title="開盤即鎖 (8:30 首筆報價就漲停)" className="ml-1">🔒</span>}
      </td>
      <td className="py-1.5 px-2 text-right font-mono text-red-600">{row.limit_up?.toFixed(2)}</td>
      <td className={`py-1.5 px-2 text-right font-mono ${bidAtLimit ? '' : 'text-orange-600 font-bold'}`}>
        {row.bid1_price?.toFixed(2)}
      </td>
      <td className="py-1.5 px-2 text-right font-mono">{row.bid1_size?.toLocaleString()}</td>
      <td className="py-1.5 px-2 text-xs">
        {row.status === 'pulled' ? (
          <span className="bg-red-100 text-red-700 px-2 py-0.5 rounded" title={row.pulled_reason}>
            撤單 — {pullLabel(row.pulled_reason)}
          </span>
        ) : (
          <span className="bg-green-100 text-green-700 px-2 py-0.5 rounded">追蹤</span>
        )}
      </td>
      <td className="py-1.5 px-2 text-xs"><TradeCell trade={row.trade} /></td>
      <td className="py-1.5 px-2 text-xs">
        {row.warning
          ? <span className="bg-yellow-100 text-yellow-800 px-2 py-0.5 rounded" title={row.warning}>⚠ 量遞減</span>
          : <span className="text-gray-300">—</span>}
      </td>
    </tr>
  )
}


function Wait() {
  return <span className="text-gray-300 text-xs">等待…</span>
}


function failLabel(reason: string): string {
  if (reason.startsWith('first_books_ask_appeared')) return '委賣一出現'
  if (reason.startsWith('first_trade_qty_too_small')) return '首筆量不足'
  return reason
}


function pullLabel(reason: string): string {
  if (reason.startsWith('qty_drop_half')) return '委買量 tick 間減半'
  if (reason.startsWith('price_below_limit')) return '委買一跌下漲停'
  return reason
}


// ─── 交易模式面板 (模擬/真實) ────────────────────────────────

function TradingPanel() {
  const [ts, setTs] = useState<TradingStatus | null>(null)
  const [msg, setMsg] = useState('')
  // 連線表單
  const [acct, setAcct] = useState('')
  const [pwd, setPwd] = useState('')
  const [pfxPwd, setPfxPwd] = useState('')
  const [pfxFile, setPfxFile] = useState<File | null>(null)
  const [isTest, setIsTest] = useState(false)
  // 預算
  const [totalBudget, setTotalBudget] = useState('')
  const [perBudget, setPerBudget] = useState('')
  // 委託總表 + 右鍵選單
  const [orders, setOrders] = useState<OrderRow[]>([])
  const [ctxMenu, setCtxMenu] = useState<{ x: number; y: number; order: OrderRow } | null>(null)

  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      try {
        const s = await api.tradingStatus()
        if (!cancelled) setTs(s)
        if (s.mode === 'real') {
          const o = await api.tradingOrders()
          if (!cancelled) setOrders(o.orders)
        }
      } catch { /* ignore */ }
    }
    tick()
    const id = window.setInterval(tick, 2000)
    return () => { cancelled = true; window.clearInterval(id) }
  }, [])

  // 點任何地方關掉右鍵選單
  useEffect(() => {
    if (!ctxMenu) return
    const close = () => setCtxMenu(null)
    window.addEventListener('click', close)
    window.addEventListener('contextmenu', close, { capture: true })
    return () => {
      window.removeEventListener('click', close)
      window.removeEventListener('contextmenu', close, { capture: true })
    }
  }, [ctxMenu])

  // 帶入後端已設的預算 (只在欄位還空時)
  useEffect(() => {
    if (ts && totalBudget === '' && ts.params.total_budget > 0) {
      setTotalBudget(String(ts.params.total_budget))
    }
    if (ts && perBudget === '' && ts.params.per_symbol_budget > 0) {
      setPerBudget(String(ts.params.per_symbol_budget))
    }
  }, [ts])  // eslint-disable-line react-hooks/exhaustive-deps

  const flash = (m: string) => { setMsg(m); window.setTimeout(() => setMsg(''), 5000) }

  async function switchMode(mode: 'sim' | 'real') {
    try { await api.tradingMode(mode); flash(`✓ 已切換 ${mode === 'real' ? '真實' : '模擬'} 模式`) }
    catch (e: any) { flash(`✗ ${e.message}`) }
  }

  async function connect() {
    if (!acct || !pwd || !pfxFile) { flash('✗ 請填帳號/密碼並選擇憑證檔'); return }
    try {
      const buf = await pfxFile.arrayBuffer()
      let bin = ''
      const bytes = new Uint8Array(buf)
      for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i])
      const b64 = btoa(bin)
      await api.tradingConnect({
        account_id: acct, password: pwd, pfx_password: pfxPwd,
        is_test: isTest, pfx_b64: b64, pfx_filename: pfxFile.name,
      })
      setPwd(''); setPfxPwd('')       // 密碼即用即清
      flash('連線中… (狀態每 2 秒更新)')
    } catch (e: any) { flash(`✗ ${e.message}`) }
  }

  async function saveBudget() {
    const t = parseFloat(totalBudget); const p = parseFloat(perBudget)
    if (!(t > 0) || !(p > 0)) { flash('✗ 總預算與每檔上限都要 > 0'); return }
    try { await api.tradingParams({ total_budget: t, per_symbol_budget: p }); flash('✓ 預算已套用') }
    catch (e: any) { flash(`✗ ${e.message}`) }
  }

  async function toggleArm() {
    if (!ts) return
    if (!ts.armed && !window.confirm(
      '⚡ 確定開始「真實下單」?\n\n08:59:58 會對標記清單預掛漲停價限價單,9:00 後依策略下市價單。')) return
    try { await api.tradingArm(!ts.armed); flash(ts.armed ? '已停止交易' : '⚡ 交易已啟動') }
    catch (e: any) { flash(`✗ ${e.message}`) }
  }

  async function closeAll() {
    if (!window.confirm('🚨 緊急全平?\n撤所有未成交委託 + 市價賣出全部持倉!')) return
    try { const r = await api.tradingCloseAll(); flash(`✓ 全平完成 (賣出 ${r.sold_symbols} 檔)`) }
    catch (e: any) { flash(`✗ ${e.message}`) }
  }

  async function cancelOrder(o: OrderRow) {
    setCtxMenu(null)
    if (!window.confirm(`刪單 ${o.order_no}?\n${o.symbol} ${o.action === 'buy' ? '買' : '賣'} ${o.lots} 張`)) return
    try {
      await api.tradingCancelOrder(o.order_no)
      flash(`✓ 已刪單 ${o.order_no}`)
    } catch (e: any) { flash(`✗ ${e.message}`) }
  }

  const isReal = ts?.mode === 'real'
  return (
    <section className={`rounded-lg shadow p-4 space-y-3 ${isReal ? 'bg-red-50 border border-red-300' : 'bg-white'}`}>
      <div className="flex items-center gap-3 flex-wrap">
        <h2 className="font-semibold">💼 交易模式</h2>
        <div className="flex rounded overflow-hidden border">
          <button onClick={() => switchMode('sim')}
                  className={`px-4 py-1.5 text-sm font-medium ${!isReal ? 'bg-blue-600 text-white' : 'bg-white text-gray-600'}`}>
            模擬 (純監控)
          </button>
          <button onClick={() => switchMode('real')}
                  className={`px-4 py-1.5 text-sm font-medium ${isReal ? 'bg-red-600 text-white' : 'bg-white text-gray-600'}`}>
            真實下單
          </button>
        </div>
        {/* 連線狀態 badge */}
        {ts && (
          ts.connecting ? <Badge cls="bg-yellow-100 text-yellow-800">連線中…</Badge>
          : ts.connected && ts.healthy ? (
            <Badge cls="bg-green-100 text-green-800">
              ✓ 已連線 {ts.account_masked}{ts.is_test ? ' (測試環境)' : ''}
            </Badge>
          )
          : ts.connected ? <Badge cls="bg-red-100 text-red-800">⚠ 連線不健康 — 請重連</Badge>
          : <Badge cls="bg-gray-100 text-gray-600">未連線</Badge>
        )}
        {ts?.armed && <Badge cls="bg-red-600 text-white animate-pulse">⚡ 交易中</Badge>}
        {msg && <span className="text-sm">{msg}</span>}
      </div>

      {(ts?.connect_error || ts?.error) && (
        <div className="text-sm text-red-700 bg-red-100 rounded px-3 py-2">
          {ts.connect_error || ts.error}
        </div>
      )}

      {/* 真實模式面板 */}
      {isReal && (
        <>
          {/* 連線表單 (未連線時) */}
          {!ts?.connected && !ts?.connecting && (
            <div className="flex items-end gap-2 flex-wrap text-sm">
              <Field label="券商帳號">
                <input value={acct} onChange={e => setAcct(e.target.value)}
                       className="border rounded px-2 py-1.5 w-36 font-mono" placeholder="身分證字號" />
              </Field>
              <Field label="密碼">
                <input type="password" value={pwd} onChange={e => setPwd(e.target.value)}
                       className="border rounded px-2 py-1.5 w-32" />
              </Field>
              <Field label="憑證檔 (.pfx)">
                <input type="file" accept=".pfx,.p12"
                       onChange={e => setPfxFile(e.target.files?.[0] ?? null)}
                       className="text-xs w-52" />
              </Field>
              <Field label="憑證密碼 (無則留空)">
                <input type="password" value={pfxPwd} onChange={e => setPfxPwd(e.target.value)}
                       className="border rounded px-2 py-1.5 w-32" />
              </Field>
              <label className="flex items-center gap-1 pb-2 text-xs text-gray-600">
                <input type="checkbox" checked={isTest} onChange={e => setIsTest(e.target.checked)} />
                測試環境
              </label>
              <button onClick={connect}
                      className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded font-medium">
                連線
              </button>
            </div>
          )}

          {/* 已連線: 預算 + 開始交易 + 緊急全平 */}
          {ts?.connected && (
            <div className="flex items-end gap-2 flex-wrap text-sm">
              <Field label="總預算 (元)">
                <input type="number" min={0} value={totalBudget}
                       onChange={e => setTotalBudget(e.target.value)}
                       className="border rounded px-2 py-1.5 w-36 font-mono" placeholder="例 1000000" />
              </Field>
              <Field label="每檔上限 (元)">
                <input type="number" min={0} value={perBudget}
                       onChange={e => setPerBudget(e.target.value)}
                       className="border rounded px-2 py-1.5 w-36 font-mono" placeholder="例 200000" />
              </Field>
              <button onClick={saveBudget}
                      className="bg-blue-600 hover:bg-blue-700 text-white px-3 py-2 rounded font-medium">
                套用預算
              </button>
              <button onClick={toggleArm}
                      className={`px-5 py-2 rounded font-bold text-white ${
                        ts.armed ? 'bg-gray-600 hover:bg-gray-700' : 'bg-red-600 hover:bg-red-700'}`}>
                {ts.armed ? '■ 停止交易' : '⚡ 開始交易'}
              </button>
              <button onClick={closeAll}
                      className="px-3 py-2 rounded font-medium border border-red-400 text-red-700 hover:bg-red-100">
                🚨 緊急全平
              </button>
              <button onClick={() => api.tradingDisconnect().then(() => flash('已斷線')).catch(() => {})}
                      className="px-3 py-2 rounded text-xs text-gray-500 hover:bg-gray-100">
                斷線
              </button>
              <span className="text-xs text-gray-600 pb-2">
                已用預算 {ts.budget_used?.toLocaleString()} / {ts.params.total_budget?.toLocaleString()}
                {ts.n_positions > 0 && ` ・ 持倉 ${ts.n_positions} 檔`}
              </span>
            </div>
          )}

          {/* 委託狀態表 — 右鍵可刪 pending 單 */}
          {orders.length > 0 && (
            <div>
              <h3 className="text-sm font-semibold text-gray-700 mb-1">
                📋 委託狀態 ({orders.length})
                <span className="ml-2 font-normal text-xs text-gray-400">在委託列上按右鍵可刪單</span>
              </h3>
              <div className="overflow-x-auto max-h-64 overflow-y-auto bg-white rounded border">
                <table className="w-full text-xs">
                  <thead className="text-gray-500 border-b bg-gray-50 sticky top-0">
                    <tr>
                      <th className="text-left py-1.5 px-2">時間</th>
                      <th className="text-left py-1.5 px-2">委託書號</th>
                      <th className="text-left py-1.5 px-2">標的</th>
                      <th className="text-left py-1.5 px-2">買賣</th>
                      <th className="text-left py-1.5 px-2">種類</th>
                      <th className="text-right py-1.5 px-2">價格</th>
                      <th className="text-right py-1.5 px-2">張數</th>
                      <th className="text-right py-1.5 px-2">已成交</th>
                      <th className="text-left py-1.5 px-2">狀態</th>
                    </tr>
                  </thead>
                  <tbody>
                    {orders.map(o => (
                      <tr key={o.order_no}
                          onContextMenu={e => {
                            e.preventDefault()
                            setCtxMenu({ x: e.clientX, y: e.clientY, order: o })
                          }}
                          className={`border-b last:border-b-0 cursor-context-menu ${
                            o.status === 'pending' ? 'bg-blue-50/50' : ''}`}>
                        <td className="py-1 px-2 text-gray-500">{o.ts.slice(11)}</td>
                        <td className="py-1 px-2 font-mono">{o.order_no}</td>
                        <td className="py-1 px-2 font-mono font-semibold">{o.symbol}</td>
                        <td className={`py-1 px-2 font-medium ${
                          o.action === 'buy' ? 'text-red-600' : 'text-green-700'}`}>
                          {o.action === 'buy' ? '買' : '賣'}
                        </td>
                        <td className="py-1 px-2">{orderKindLabel(o.kind)}</td>
                        <td className="py-1 px-2 text-right font-mono">
                          {o.price > 0 ? o.price.toFixed(2) : '市價'}
                        </td>
                        <td className="py-1 px-2 text-right font-mono">{o.lots}</td>
                        <td className="py-1 px-2 text-right font-mono">{o.filled_lots}</td>
                        <td className="py-1 px-2"><OrderStatusBadge status={o.status} /></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </>
      )}

      {/* 右鍵選單 */}
      {ctxMenu && (
        <div className="fixed z-50 bg-white border rounded shadow-lg py-1 text-sm"
             style={{ left: ctxMenu.x, top: ctxMenu.y }}>
          <div className="px-3 py-1 text-xs text-gray-400 border-b">
            {ctxMenu.order.symbol} ・ {ctxMenu.order.order_no}
          </div>
          <button
            disabled={ctxMenu.order.status !== 'pending'}
            onClick={() => cancelOrder(ctxMenu.order)}
            className={`block w-full text-left px-3 py-1.5 ${
              ctxMenu.order.status === 'pending'
                ? 'text-red-600 hover:bg-red-50'
                : 'text-gray-300 cursor-not-allowed'}`}>
            🗑 刪單{ctxMenu.order.status !== 'pending' ? ` (${orderStatusText(ctxMenu.order.status)})` : ''}
          </button>
        </div>
      )}
    </section>
  )
}


function orderKindLabel(kind: string): string {
  const m: Record<string, string> = {
    pre_limit: '預掛限價', market_buy: '市價買', market_sell: '市價賣',
  }
  return m[kind] || kind
}


function orderStatusText(s: string): string {
  const m: Record<string, string> = {
    pending: '排隊中', filled: '已成交', cancelled: '已撤', rejected: '被拒',
  }
  return m[s] || s
}


function OrderStatusBadge({ status }: { status: string }) {
  const cls: Record<string, string> = {
    pending: 'bg-blue-100 text-blue-700',
    filled: 'bg-green-100 text-green-700',
    cancelled: 'bg-gray-100 text-gray-500',
    rejected: 'bg-red-100 text-red-700',
  }
  return (
    <span className={`px-1.5 py-0.5 rounded ${cls[status] || 'bg-gray-100'}`}>
      {orderStatusText(status)}
    </span>
  )
}


function Badge({ cls, children }: { cls: string; children: ReactNode }) {
  return <span className={`px-2 py-0.5 rounded text-xs font-medium ${cls}`}>{children}</span>
}


function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div>
      <label className="block text-xs text-gray-600 mb-1">{label}</label>
      {children}
    </div>
  )
}


function StatCard({ label, value, color = 'default' }: {
  label: string; value: number
  color?: 'default' | 'green' | 'orange' | 'red'
}) {
  const cls: Record<string, string> = {
    default: '',
    green: 'text-green-700',
    orange: 'text-orange-700',
    red: 'text-red-700',
  }
  return (
    <div className="bg-white rounded-lg shadow p-3">
      <div className="text-xs text-gray-500">{label}</div>
      <div className={`text-2xl font-bold mt-1 font-mono ${cls[color]}`}>{value}</div>
    </div>
  )
}
