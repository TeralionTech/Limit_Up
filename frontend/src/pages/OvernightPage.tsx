import { useEffect, useState } from 'react'
import { api, OvernightRow } from '../api'

// 隔日賣標的:昨天買到、收盤未出場的持倉,今天開盤第一筆成交後以委買一價賣出
export default function OvernightPage() {
  const [rows, setRows] = useState<OvernightRow[]>([])
  const [err, setErr] = useState('')
  const [msg, setMsg] = useState('')

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

  return (
    <div className="space-y-4">
      <section className="bg-white rounded-lg shadow p-4">
        <h2 className="font-semibold mb-1">🌙 隔日賣標的 ({rows.length})</h2>
        <p className="text-xs text-gray-500 mb-3">
          昨天買到、收盤未出場的持倉。今天開盤收到第一筆成交後,以委買一價限價賣出
          (委買一委賣一價差 ≥ 5 tick 時改掛「賣一往下一檔」)。需真實模式已連線+開始交易才會實際賣。
          按「不要賣」可暫停該檔 (已下賣單會一併撤掉)。
        </p>
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
                {rows.map(r => <OvernightTr key={r.symbol} row={r} onToggleSkip={() => toggleSkip(r)} />)}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  )
}

function OvernightTr({ row, onToggleSkip }: { row: OvernightRow; onToggleSkip: () => void }) {
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
      <td className="py-2 px-2 text-center">
        <button onClick={onToggleSkip}
                className={`px-2 py-1 rounded text-xs font-medium border ${
                  row.skip ? 'border-green-400 text-green-700 hover:bg-green-50'
                           : 'border-orange-400 text-orange-700 hover:bg-orange-50'}`}>
          {row.skip ? '恢復賣出' : '不要賣'}
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
  const priced = levels.filter(l => l.price > 0).slice(0, 5)
  if (priced.length === 0) return <span className="text-gray-300 text-xs">—</span>
  return (
    <div className="font-mono text-xs leading-tight">
      {priced.map((l, i) => (
        <div key={i} className="flex gap-2 justify-between">
          <span className={color}>{l.price.toFixed(2)}</span>
          <span className="text-gray-400">{l.size.toLocaleString()}</span>
        </div>
      ))}
    </div>
  )
}
