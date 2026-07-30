import { useEffect, useState } from 'react'
import { api, OvernightRow } from '../api'

// 隔日賣標的:昨天買到、收盤未出場的持倉,今天開盤第一筆成交後以委買一價賣出
export default function OvernightPage() {
  const [rows, setRows] = useState<OvernightRow[]>([])
  const [err, setErr] = useState('')
  const [msg, setMsg] = useState('')
  const [addSym, setAddSym] = useState('')
  const [adding, setAdding] = useState(false)

  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      try {
        const r = await api.tradingOvernight()
        if (!cancelled) { setRows(r.overnight); setErr('') }
      } catch (e: any) {
        if (!cancelled) setErr(e.message)
      }
    }
    tick()
    const id = window.setInterval(tick, 1500)
    return () => { cancelled = true; window.clearInterval(id) }
  }, [])

  async function toggleSkip(r: OvernightRow) {
    const next = !r.skip
    if (next && r.sell_placed && !window.confirm(
      `${r.symbol} 已下賣單,「不要賣」會撤掉賣單。確定?`)) return
    try {
      await api.tradingOvernightSkip(r.symbol, next)
      setMsg(`✓ ${r.symbol} ${next ? '已暫停賣出' : '恢復賣出'}`)
      window.setTimeout(() => setMsg(''), 4000)
    } catch (e: any) { setMsg(`✗ ${e.message}`) }
  }

  async function addSymbol() {
    const sym = addSym.trim()
    if (!sym) return
    setAdding(true)
    try {
      const r = await api.tradingOvernightAdd(sym)
      setMsg(r.added ? `✓ 已加入追蹤 ${sym} (張數以庫存為準)` : `${sym} 已在清單中`)
      setAddSym('')
      window.setTimeout(() => setMsg(''), 4000)
    } catch (e: any) { setMsg(`✗ ${e.message}`) }
    finally { setAdding(false) }
  }

  async function removeSymbol(r: OvernightRow) {
    if (!window.confirm(`確定把 ${r.symbol} 從隔日賣清單移除?(不會影響已下的賣單)`)) return
    try {
      await api.tradingOvernightRemove(r.symbol)
      setMsg(`✓ 已移除 ${r.symbol}`)
      window.setTimeout(() => setMsg(''), 4000)
    } catch (e: any) { setMsg(`✗ ${e.message}`) }
  }

  return (
    <div className="space-y-4">
      <section className="bg-white rounded-lg shadow p-4">
        <h2 className="font-semibold mb-1">🌙 隔日賣標的 ({rows.length})</h2>
        <p className="text-xs text-gray-500 mb-3">
          昨天買到、收盤未出場的持倉。今天開盤收到第一筆成交後,以委買一價限價賣出
          (委買一委賣一價差 ≥ 5 tick 時改掛「賣一往下一檔」)。需真實模式已連線+開始交易才會實際賣。
          按「不要賣」可暫停該檔 (已下賣單會一併撤掉)。
        </p>
        <div className="flex items-center gap-2 mb-3">
          <input
            value={addSym}
            onChange={e => setAddSym(e.target.value)}
            onKeyDown={e => { if (e.key === 'Enter') addSymbol() }}
            placeholder="輸入股票代號手動追蹤 (例 2330)"
            className="border rounded px-2 py-1 text-sm font-mono w-56"
          />
          <button
            onClick={addSymbol}
            disabled={adding || !addSym.trim()}
            className="px-3 py-1 rounded text-sm font-medium bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-40">
            {adding ? '加入中…' : '追蹤'}
          </button>
          <span className="text-xs text-gray-400">張數以券商庫存為準,連線中會即時對帳並收五檔</span>
        </div>
        {err && <div className="text-red-600 text-sm mb-2">API 錯: {err}</div>}
        {msg && <div className="text-sm mb-2">{msg}</div>}
        {rows.length === 0 ? (
          <div className="text-gray-400 text-sm py-6 text-center">
            無隔日賣標的 (昨天沒有留倉,或已全部賣出)
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="text-xs text-gray-500 border-b bg-gray-50">
                <tr>
                  <th className="text-left py-2 px-2">標的</th>
                  <th className="text-right py-2 px-2">持倉(張)</th>
                  <th className="text-right py-2 px-2">成本</th>
                  <th className="text-right py-2 px-2">最新成交</th>
                  <th className="text-left py-2 px-2">賣出狀態</th>
                  <th className="text-center py-2 px-2">操作</th>
                  <th className="text-left py-2 px-2">委買五檔</th>
                  <th className="text-left py-2 px-2">委賣五檔</th>
                </tr>
              </thead>
              <tbody>
                {rows.map(r => <OvernightTr key={r.symbol} row={r}
                  onToggleSkip={() => toggleSkip(r)} onRemove={() => removeSymbol(r)} />)}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  )
}

function OvernightTr({ row, onToggleSkip, onRemove }: {
  row: OvernightRow; onToggleSkip: () => void; onRemove: () => void
}) {
  const bids = row.books?.bids ?? []
  const asks = row.books?.asks ?? []
  return (
    <tr className={`border-b align-top ${row.skip ? 'bg-gray-50' : ''}`}>
      <td className="py-2 px-2 font-mono font-semibold">
        {row.symbol}
        {!row.reconciled && (
          <span className="ml-1 text-xs text-orange-500" title={row.note}>⏳</span>
        )}
      </td>
      <td className="py-2 px-2 text-right font-mono">{row.lots}</td>
      <td className="py-2 px-2 text-right font-mono text-gray-500">
        {row.avg_cost > 0 ? row.avg_cost.toFixed(2) : '—'}
      </td>
      <td className="py-2 px-2 text-right font-mono">
        {row.last_trade?.price ? row.last_trade.price.toFixed(2) : <span className="text-gray-300">—</span>}
      </td>
      <td className="py-2 px-2 text-xs">
        {row.skip ? <span className="bg-gray-200 text-gray-600 px-2 py-0.5 rounded">已暫停</span>
                  : <SellStatus row={row} />}
      </td>
      <td className="py-2 px-2 text-center whitespace-nowrap">
        <button onClick={onToggleSkip}
                className={`px-2 py-1 rounded text-xs font-medium border ${
                  row.skip ? 'border-green-400 text-green-700 hover:bg-green-50'
                           : 'border-orange-400 text-orange-700 hover:bg-orange-50'}`}>
          {row.skip ? '恢復賣出' : '不要賣'}
        </button>
        <button onClick={onRemove}
                title="從清單移除"
                className="ml-1 px-2 py-1 rounded text-xs font-medium border border-gray-300 text-gray-500 hover:bg-gray-100">
          移除
        </button>
      </td>
      <td className="py-2 px-2"><BookSide levels={bids} color="text-red-600" /></td>
      <td className="py-2 px-2"><BookSide levels={asks} color="text-green-700" /></td>
    </tr>
  )
}

function SellStatus({ row }: { row: OvernightRow }) {
  if (!row.sell_placed) {
    return <span className="bg-gray-100 text-gray-600 px-2 py-0.5 rounded">待開盤成交</span>
  }
  const statusLabel: Record<string, string> = {
    pending: '排隊中', filled: '已賣出', cancelled: '已撤', rejected: '被拒',
  }
  const cls: Record<string, string> = {
    pending: 'bg-blue-100 text-blue-700', filled: 'bg-green-100 text-green-700',
    cancelled: 'bg-gray-100 text-gray-500', rejected: 'bg-red-100 text-red-700',
  }
  return (
    <div className="space-y-0.5">
      <div>
        <span className={`px-2 py-0.5 rounded ${cls[row.sell_status] || 'bg-blue-100 text-blue-700'}`}>
          {statusLabel[row.sell_status] || '已下賣單'}
        </span>
      </div>
      <div className="text-gray-500 font-mono">
        @ {row.sell_price.toFixed(2)}
        {row.sold_lots > 0 && <span className="text-green-700"> ・成交 {row.sold_lots}</span>}
      </div>
    </div>
  )
}

function BookSide({ levels, color }: {
  levels: Array<{ price: number; size: number }>; color: string
}) {
  // 顯示前 5 檔;price=0 是市價委託列 → 顯示「市價」(不要濾掉)
  const shown = (levels || []).filter(l => l.size > 0).slice(0, 5)
  if (shown.length === 0) return <span className="text-gray-300 text-xs">—</span>
  return (
    <div className="font-mono text-xs leading-tight">
      {shown.map((l, i) => (
        <div key={i} className="flex gap-2 justify-between">
          <span className={l.price > 0 ? color : 'text-purple-600 font-semibold'}>
            {l.price > 0 ? l.price.toFixed(2) : '市價'}
          </span>
          <span className="text-gray-400">{l.size.toLocaleString()}</span>
        </div>
      ))}
    </div>
  )
}
