import { useEffect, useState } from 'react'
import { api, FilterProgress, Watchlist, T30Info } from '../api'

export default function FilterPage() {
  const [prog, setProg] = useState<FilterProgress | null>(null)
  const [watch, setWatch] = useState<Watchlist | null>(null)
  const [t30, setT30] = useState<T30Info | null>(null)
  const [msg, setMsg] = useState('')

  async function reload() {
    try {
      const [p, w, t] = await Promise.all([
        api.filterProgress(), api.watchlist(), api.t30().catch(() => null),
      ])
      setProg(p); setWatch(w); if (t) setT30(t)
    } catch {
      // 忽略單次 error
    }
  }

  useEffect(() => {
    reload()
    const id = window.setInterval(reload, 2000)
    return () => window.clearInterval(id)
  }, [])

  async function removeSymbol(sym: string) {
    if (!window.confirm(`盤前手動剔除 ${sym}？移除後永久淘汰、08:59:58 預掛也不會下這檔。`)) return
    try {
      await api.filterRemove(sym)
      setMsg(`✓ 已剔除 ${sym} (不會下單)`)
      await reload()
    } catch (e: any) {
      setMsg(`✗ ${e.message}`)
    }
    window.setTimeout(() => setMsg(''), 4000)
  }

  return (
    <div className="space-y-4">
      {/* 區塊 1: 進度條 */}
      <section className="bg-white rounded-lg shadow p-4">
        <h2 className="font-semibold mb-3">📥 抓漲停資料進度</h2>
        {prog ? <ProgressBlock prog={prog} /> : <div className="text-gray-400 text-sm">載入中...</div>}
      </section>

      {msg && <div className="text-sm bg-white rounded-lg shadow px-4 py-2">{msg}</div>}

      {/* 區塊 2: 標記清單 + 丟棄清單 */}
      <section className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="bg-white rounded-lg shadow p-4">
          <h2 className="font-semibold mb-1 text-green-700">
            ✓ 目前標記清單 ({watch?.marked.length ?? 0})
          </h2>
          <p className="text-xs text-gray-400 mb-3">點標的上的 ✗ 可盤前手動剔除 (不想搶的檔) — 剔除後不會預掛/下單</p>
          {!watch || watch.marked.length === 0 ? (
            <div className="text-gray-400 text-sm py-6 text-center">尚無標記</div>
          ) : (() => {
            const firstTickSet = new Set(watch.first_tick_marked ?? [])
            const fromStart = watch.marked.filter(s => firstTickSet.has(s))
            const rest = watch.marked.filter(s => !firstTickSet.has(s))
            return (
              <div className="space-y-4">
                {/* 塊 1: 開盤即鎖 — 8:30 第一筆報價就漲停的強勢股 */}
                <div>
                  <h3 className="text-sm font-semibold text-red-700 mb-2">
                    🔒 開盤即鎖漲停 ({fromStart.length})
                    <span className="ml-1 font-normal text-xs text-gray-400 cursor-help"
                          title="8:30 試撮第一筆真實報價就已鎖漲停 (委買一=漲停、無賣單) 的標的">ⓘ</span>
                  </h3>
                  {fromStart.length === 0 ? (
                    <div className="text-gray-400 text-xs py-2">無</div>
                  ) : (
                    <div className="flex flex-wrap gap-2">
                      {fromStart.map(sym => (
                        <MarkChip key={sym} sym={sym} lock onRemove={() => removeSymbol(sym)} />
                      ))}
                    </div>
                  )}
                </div>
                {/* 塊 2: 盤中鎖上 — 其餘標記 */}
                <div>
                  <h3 className="text-sm font-semibold text-green-700 mb-2">
                    盤中鎖上 ({rest.length})
                  </h3>
                  {rest.length === 0 ? (
                    <div className="text-gray-400 text-xs py-2">無</div>
                  ) : (
                    <div className="flex flex-wrap gap-2">
                      {rest.map(sym => (
                        <MarkChip key={sym} sym={sym} onRemove={() => removeSymbol(sym)} />
                      ))}
                    </div>
                  )}
                </div>
              </div>
            )
          })()}
        </div>

        <div className="bg-white rounded-lg shadow p-4">
          <h2 className="font-semibold mb-3 text-orange-700">
            ✗ 丟棄清單 ({watch?.discarded.length ?? 0})
          </h2>
          {!watch || watch.discarded.length === 0 ? (
            <div className="text-gray-400 text-sm py-6 text-center">尚無丟棄紀錄</div>
          ) : (
            <div className="max-h-96 overflow-y-auto">
              <table className="w-full text-sm">
                <thead className="text-xs text-gray-500 border-b">
                  <tr>
                    <th className="text-left py-1">股票</th>
                    <th className="text-left py-1">原因</th>
                    <th className="text-left py-1">時間</th>
                  </tr>
                </thead>
                <tbody>
                  {watch.discarded.map(d => (
                    <tr key={d.symbol} className="border-b last:border-b-0">
                      <td className="py-1.5 font-mono">{d.symbol}</td>
                      <td className="py-1.5 text-xs">{reasonLabel(d.reason)}</td>
                      <td className="py-1.5 text-xs text-gray-500">
                        {d.ts ? new Date(d.ts).toLocaleTimeString('zh-TW') : '-'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </section>

      {/* T30 禁單清單 (全額交割 / 需預收) */}
      <T30Section t30={t30} />

      {/* 統計小卡 */}
      {prog && (
        <section className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <StatCard label="Universe" value={prog.universe_size} />
          <StatCard label="Books tick 累計" value={prog.tick_stats.books_count ?? 0} />
          <StatCard
            label="曾標記檔數"
            value={prog.filter_stats.total_mark_events ?? 0}
            tooltip="= 目前標記 + 已淘汰。unmark 為永久淘汰，不會重複標記。"
          />
          <StatCard
            label="已淘汰檔數"
            value={prog.filter_stats.total_unmark_events ?? 0}
            tooltip={`拆分：出現賣單 ${prog.filter_stats.unmark_by_ask_appeared ?? 0} / 跌下漲停 ${prog.filter_stats.unmark_by_bid_below_limit ?? 0} / 買一量減半 ${prog.filter_stats.unmark_by_bid_dropped ?? 0}`}
          />
        </section>
      )}
    </div>
  )
}


function MarkChip({ sym, lock, onRemove }: { sym: string; lock?: boolean; onRemove: () => void }) {
  const cls = lock
    ? 'bg-red-50 border-red-400 text-red-800 font-semibold'
    : 'bg-green-50 border-green-300 text-green-800'
  return (
    <span className={`inline-flex items-center gap-1.5 border px-2.5 py-1 rounded text-sm font-mono ${cls}`}>
      {lock ? `🔒 ${sym}` : sym}
      <button onClick={onRemove} title="盤前手動剔除 (不會下單)"
              className="ml-0.5 text-gray-400 hover:text-red-600 font-bold leading-none">✗</button>
    </span>
  )
}


function T30Section({ t30 }: { t30: T30Info | null }) {
  const [open, setOpen] = useState(false)
  const [q, setQ] = useState('')
  const syms = t30?.symbols ?? []
  const hit = q.trim() ? syms.filter(s => s.includes(q.trim())) : syms
  const files = t30?.meta?.files ?? {}
  const missingAll = t30?.meta?.missing_all
  const stale = Object.entries(files).filter(([, i]) => i.ok && i.stale).map(([n]) => n)

  return (
    <section className="bg-white rounded-lg shadow p-4">
      <button onClick={() => setOpen(o => !o)} className="w-full flex items-center gap-2 text-left">
        <span className="font-semibold text-gray-700">🚫 T30 禁單清單 ({t30?.count ?? 0})</span>
        <span className="text-xs text-gray-400">全額交割 / 需預收款券 — 這些不會下單</span>
        <span className="ml-auto text-gray-400">{open ? '▲' : '▼'}</span>
      </button>

      {/* 檔案狀態警示 (永遠顯示,不需展開) */}
      {t30 && (
        <div className="mt-2 text-xs">
          {missingAll ? (
            <span className="text-red-600 font-medium">⚠ T30 檔案全缺 — 今日無禁單保護,請檢查取檔 timer</span>
          ) : stale.length > 0 ? (
            <span className="text-orange-600">⚠ 檔非今日 ({stale.join(', ')}) — 名單可能過時</span>
          ) : (
            <span className="text-gray-400">
              檔案: {Object.entries(files).map(([n, i]) =>
                `${n} ${i.ok ? (i.mtime_date ?? '') : '缺'}`).join(' / ') || '—'}
            </span>
          )}
        </div>
      )}

      {open && (
        <div className="mt-3">
          <input value={q} onChange={e => setQ(e.target.value)}
                 placeholder="搜尋代號 (例 6225)"
                 className="border rounded px-2 py-1 text-sm font-mono w-48 mb-3" />
          {hit.length === 0 ? (
            <div className="text-gray-400 text-sm py-3">{q.trim() ? `無符合 "${q.trim()}"` : '清單為空'}</div>
          ) : (
            <div className="flex flex-wrap gap-2 max-h-72 overflow-y-auto">
              {hit.map(s => (
                <span key={s} className="inline-block bg-gray-100 border border-gray-300 text-gray-700 px-2 py-0.5 rounded text-sm font-mono">
                  {s}
                </span>
              ))}
            </div>
          )}
          {q.trim() && (
            <div className="mt-2 text-xs text-gray-500">
              {hit.includes(q.trim())
                ? <span className="text-red-600">● {q.trim()} 在禁單中 (不會下單)</span>
                : <span className="text-green-700">○ {q.trim()} 不在禁單中</span>}
            </div>
          )}
        </div>
      )}
    </section>
  )
}


function ProgressBlock({ prog }: { prog: FilterProgress }) {
  const total = prog.limit_up_total || 1
  const done = prog.limit_up_done
  const pct = Math.min(100, Math.round(done / total * 100))
  return (
    <div>
      <div className="flex items-center justify-between text-sm mb-2">
        <span className="text-gray-600">
          {done} / {total} 檔 ({prog.limit_up_ok} 成功 / {prog.limit_up_fail} 失敗)
        </span>
        <span className="font-mono text-gray-700">{pct}%</span>
      </div>
      <div className="w-full bg-gray-200 rounded-full h-3 overflow-hidden">
        <div
          className="bg-blue-600 h-3 transition-all duration-500"
          style={{ width: `${pct}%` }}
        />
      </div>
      {done < total && done > 0 && (
        <div className="text-xs text-gray-400 mt-2">
          抓漲停價中... (2600 檔約 12-15 分鐘)
        </div>
      )}
    </div>
  )
}


function StatCard({ label, value, tooltip }: { label: string; value: number; tooltip?: string }) {
  return (
    <div className="bg-white rounded-lg shadow p-3" title={tooltip}>
      <div className="text-xs text-gray-500 flex items-center gap-1">
        {label}
        {tooltip && <span className="text-gray-400 cursor-help">ⓘ</span>}
      </div>
      <div className="text-2xl font-bold mt-1 font-mono">{value.toLocaleString()}</div>
    </div>
  )
}


function reasonLabel(reason: string): string {
  const m: Record<string, string> = {
    ask_appeared: '出現賣單',
    bid_below_limit: '委買一跌下漲停',
    bid_dropped_half: '買一量減半 (final check)',
    first_check_failed: '第一盤檢查淘汰',
    manual_remove: '手動剔除',
  }
  return m[reason] || reason
}
