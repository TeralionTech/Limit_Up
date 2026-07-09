import { useEffect, useState } from 'react'
import { api, TickSnapshot, Watchlist } from '../api'

export default function QuotePage() {
  const [input, setInput] = useState('2330')
  const [symbol, setSymbol] = useState('')
  const [tick, setTick] = useState<TickSnapshot | null>(null)
  const [err, setErr] = useState('')
  const [showList, setShowList] = useState(false)

  useEffect(() => {
    if (!symbol) return
    let cancelled = false
    const fetch1 = async () => {
      try {
        const t = await api.tick(symbol)
        if (!cancelled) { setTick(t); setErr('') }
      } catch (e: any) {
        if (!cancelled) setErr(e.message)
      }
    }
    fetch1()
    const id = window.setInterval(fetch1, 1000)   // 1s polling
    return () => { cancelled = true; window.clearInterval(id) }
  }, [symbol])

  function submit(e: React.FormEvent) {
    e.preventDefault()
    setSymbol(input.trim())
  }

  function pickFromList(sym: string) {
    setInput(sym)
    setSymbol(sym)
    setShowList(false)
  }

  return (
    <div className="space-y-4">
      {/* 頂部: 目前清單按鈕 + 查詢 */}
      <section className="bg-white rounded-lg shadow p-4">
        <div className="flex items-center justify-between mb-3">
          <button
            onClick={() => setShowList(true)}
            className="bg-green-600 hover:bg-green-700 text-white px-4 py-2 rounded font-medium text-sm"
          >
            📋 目前清單
          </button>
        </div>
        <form onSubmit={submit} className="flex items-end gap-3">
          <div className="flex-1">
            <label className="block text-xs text-gray-600 mb-1">股票代號</label>
            <input
              type="text"
              value={input}
              onChange={e => setInput(e.target.value.toUpperCase())}
              placeholder="例：2330"
              className="w-full border rounded px-3 py-2 font-mono"
              autoFocus
            />
          </div>
          <button
            type="submit"
            className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded font-medium"
          >
            🔍 查詢
          </button>
        </form>
        {err && <div className="text-red-600 text-sm mt-2">{err}</div>}
      </section>

      {showList && <WatchlistModal onClose={() => setShowList(false)} onPick={pickFromList} />}

      {symbol && tick && (
        <>
          {/* 停止接收提示 — 淘汰 or 未標記 (9:00 後已退訂，資料停在最後一筆) */}
          {(tick.is_discarded || !tick.is_watched) && (
            <div className={`rounded-lg px-4 py-3 text-sm ${
              tick.is_discarded
                ? 'bg-red-50 border border-red-200 text-red-700'
                : 'bg-yellow-50 border border-yellow-200 text-yellow-800'
            }`}>
              {tick.is_discarded
                ? '⛔ 此股已被淘汰 (unmark) — 已退訂，以下顯示的是最後收到的資料'
                : 'ℹ️ 此股未在標記清單 — 若已過 9:00 訂閱已移除，以下顯示的是最後收到的資料'}
            </div>
          )}

          {/* 摘要卡 */}
          <section className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <SummaryCard label="股票" value={tick.symbol} mono />
            <SummaryCard
              label="漲停價"
              value={tick.limit_up != null ? tick.limit_up.toFixed(2) : '-'}
              mono
              color="red"
            />
            <SummaryCard
              label="階段"
              value={tick.is_pre_match ? (tick.pre_match_kind === 'pre_open' ? '盤前試撮' : '收盤試撮') : '正常競價'}
              color={tick.is_pre_match ? 'yellow' : 'green'}
            />
            <SummaryCard
              label="標記狀態"
              value={tick.is_discarded ? '✗ 已淘汰' : tick.is_watched ? '✓ Marked' : '— 未標記'}
              color={tick.is_discarded ? 'red' : tick.is_watched ? 'green' : 'gray'}
            />
          </section>

          {/* 買賣五檔 */}
          <section className="bg-white rounded-lg shadow p-4">
            <div className="flex items-center justify-between mb-3">
              <h2 className="font-semibold">📊 買賣五檔</h2>
              {tick.books?.ts && <AgeBadge ts={tick.books.ts} />}
            </div>
            {tick.books ? (
              <div className="grid grid-cols-2 gap-4">
                <BookSide label="委買 (bids)" side={tick.books.bids} color="red" />
                <BookSide label="委賣 (asks)" side={tick.books.asks} color="green" />
              </div>
            ) : (
              <div className="text-gray-400 text-sm py-6 text-center">
                此股票尚未收到任何 books tick
                <br />
                <span className="text-xs text-gray-500">
                  可能尚未開始訂閱、或此股不在監控母體內
                </span>
              </div>
            )}
          </section>
        </>
      )}
    </div>
  )
}


function WatchlistModal({ onClose, onPick }: {
  onClose: () => void
  onPick: (sym: string) => void
}) {
  const [watch, setWatch] = useState<Watchlist | null>(null)

  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      try {
        const w = await api.watchlist()
        if (!cancelled) setWatch(w)
      } catch { /* ignore */ }
    }
    tick()
    const id = window.setInterval(tick, 3000)
    return () => { cancelled = true; window.clearInterval(id) }
  }, [])

  return (
    <div
      className="fixed inset-0 bg-black bg-opacity-40 flex items-center justify-center z-50 p-4"
      onClick={onClose}
    >
      <div
        className="bg-white rounded-lg shadow-xl w-full max-w-md max-h-[70vh] flex flex-col"
        onClick={e => e.stopPropagation()}
      >
        <div className="flex items-center justify-between px-4 py-3 border-b">
          <h3 className="font-semibold">
            📋 目前標記清單 ({watch?.marked.length ?? '…'})
          </h3>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-700 text-xl leading-none">
            ✕
          </button>
        </div>
        <div className="overflow-y-auto p-4">
          {!watch ? (
            <div className="text-gray-400 text-sm text-center py-6">載入中…</div>
          ) : watch.marked.length === 0 ? (
            <div className="text-gray-400 text-sm text-center py-6">目前清單空</div>
          ) : (
            <ul className="divide-y">
              {watch.marked.map(sym => (
                <li key={sym}>
                  <button
                    onClick={() => onPick(sym)}
                    className="w-full text-left px-3 py-2 hover:bg-blue-50 font-mono text-sm flex items-center justify-between group"
                  >
                    <span>{sym}</span>
                    <span className="text-xs text-blue-500 opacity-0 group-hover:opacity-100">查詢 →</span>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>
    </div>
  )
}


function AgeBadge({ ts }: { ts: string }) {
  const [now, setNow] = useState(Date.now())
  useEffect(() => {
    const id = window.setInterval(() => setNow(Date.now()), 1000)
    return () => window.clearInterval(id)
  }, [])
  const d = new Date(ts)
  const ageMs = now - d.getTime()
  const ageSec = Math.max(0, Math.floor(ageMs / 1000))
  const hhmmss = d.toLocaleTimeString('zh-TW', { hour12: false })
  let cls = 'bg-green-100 text-green-700'
  let rel = `${ageSec} 秒前`
  if (ageSec === 0) rel = '剛剛'
  else if (ageSec > 120) { cls = 'bg-red-100 text-red-700'; if (ageSec > 3600) rel = `${Math.floor(ageSec / 60)} 分前` }
  else if (ageSec > 30) cls = 'bg-yellow-100 text-yellow-700'
  return (
    <span className={`text-xs px-2 py-0.5 rounded font-mono ${cls}`} title={d.toLocaleString('zh-TW')}>
      上次 tick: {hhmmss} ({rel})
    </span>
  )
}


function BookSide({ label, side, color }: {
  label: string
  side: Array<{ price: number; size: number }>
  color: 'red' | 'green'
}) {
  const bg = color === 'red' ? 'bg-red-50' : 'bg-green-50'
  const text = color === 'red' ? 'text-red-700' : 'text-green-700'
  return (
    <div>
      <h3 className={`text-sm font-semibold mb-1 ${text}`}>{label}</h3>
      <table className="w-full text-sm">
        <thead className="text-xs text-gray-500">
          <tr>
            <th className="text-right pr-3 py-1">價</th>
            <th className="text-right py-1">量</th>
          </tr>
        </thead>
        <tbody>
          {(side || []).map((row, i) => (
            <tr key={i} className={`${bg} bg-opacity-30`}>
              <td className="text-right pr-3 py-1 font-mono">{row.price?.toFixed(2)}</td>
              <td className="text-right py-1 font-mono">{row.size?.toLocaleString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}


function SummaryCard({ label, value, mono, color = 'default' }: {
  label: string; value: string | number; mono?: boolean
  color?: 'default' | 'red' | 'green' | 'yellow' | 'gray'
}) {
  const colorCls: Record<string, string> = {
    default: '',
    red: 'text-red-600',
    green: 'text-green-700',
    yellow: 'text-yellow-700',
    gray: 'text-gray-400',
  }
  return (
    <div className="bg-white rounded-lg shadow p-3">
      <div className="text-xs text-gray-500">{label}</div>
      <div className={`text-xl font-semibold mt-1 ${mono ? 'font-mono' : ''} ${colorCls[color]}`}>
        {value}
      </div>
    </div>
  )
}
