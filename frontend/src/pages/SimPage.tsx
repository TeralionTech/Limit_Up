import { useEffect, useState } from 'react'
import { api, TraderSummary, FirstStageRow, TrackingRow } from '../api'

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
          <p className="text-lg">📝 模擬執行頁</p>
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
    </tr>
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
