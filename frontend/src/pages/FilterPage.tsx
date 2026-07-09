import { useEffect, useState } from 'react'
import { api, FilterProgress, Watchlist } from '../api'

export default function FilterPage() {
  const [prog, setProg] = useState<FilterProgress | null>(null)
  const [watch, setWatch] = useState<Watchlist | null>(null)

  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      try {
        const [p, w] = await Promise.all([api.filterProgress(), api.watchlist()])
        if (!cancelled) {
          setProg(p)
          setWatch(w)
        }
      } catch {
        // 忽略單次 error
      }
    }
    tick()
    const id = window.setInterval(tick, 2000)
    return () => { cancelled = true; window.clearInterval(id) }
  }, [])

  return (
    <div className="space-y-4">
      {/* 區塊 1: 進度條 */}
      <section className="bg-white rounded-lg shadow p-4">
        <h2 className="font-semibold mb-3">📥 抓漲停資料進度</h2>
        {prog ? <ProgressBlock prog={prog} /> : <div className="text-gray-400 text-sm">載入中...</div>}
      </section>

      {/* 區塊 2: 標記清單 + 丟棄清單 */}
      <section className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="bg-white rounded-lg shadow p-4">
          <h2 className="font-semibold mb-3 text-green-700">
            ✓ 目前標記清單 ({watch?.marked.length ?? 0})
          </h2>
          {!watch || watch.marked.length === 0 ? (
            <div className="text-gray-400 text-sm py-6 text-center">尚無標記</div>
          ) : (
            <div className="flex flex-wrap gap-2">
              {watch.marked.map(sym => (
                <span key={sym}
                      className="inline-block bg-green-50 border border-green-300 text-green-800 px-3 py-1 rounded text-sm font-mono">
                  {sym}
                </span>
              ))}
            </div>
          )}
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
  }
  return m[reason] || reason
}
