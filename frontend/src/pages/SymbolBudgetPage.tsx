import { useEffect, useMemo, useState } from 'react'
import { api, SymbolBudget } from '../api'

// 個股金額覆寫:為特定股票指定專屬下單金額。當日最終篩選清單含此檔 → 依專屬金額下;
// 其餘股票維持「③ 模擬/真實執行」頁的全域每檔金額/張數規則。設定跨日保留。
export default function SymbolBudgetPage() {
  const [rows, setRows] = useState<SymbolBudget[]>([])
  const [err, setErr] = useState('')
  const [msg, setMsg] = useState('')
  const [sym, setSym] = useState('')
  const [wan, setWan] = useState('')     // 以「萬」輸入,直覺
  const [saving, setSaving] = useState(false)

  async function reload() {
    try {
      const r = await api.symbolBudgetList()
      setRows(r.budgets); setErr('')
    } catch (e: any) { setErr(e.message) }
  }
  useEffect(() => { reload() }, [])

  function flash(m: string) { setMsg(m); window.setTimeout(() => setMsg(''), 4000) }

  async function save() {
    const s = sym.trim().toUpperCase()
    const w = Number(wan)
    if (!s) { flash('✗ 請輸入股票代號'); return }
    if (!wan.trim() || Number.isNaN(w) || w <= 0) { flash('✗ 金額須為正數 (萬)'); return }
    setSaving(true)
    try {
      await api.symbolBudgetUpsert({ symbol: s, amount: Math.round(w * 10000) })
      await reload()
      flash(`✓ 已存 ${s} 專屬金額 ${w} 萬`)
      setSym(''); setWan('')
    } catch (e: any) { flash(`✗ ${e.message}`) }
    finally { setSaving(false) }
  }

  async function remove(r: SymbolBudget) {
    if (!window.confirm(`刪除 ${r.symbol} 的專屬金額?之後改回全域參數規則。`)) return
    try {
      await api.symbolBudgetDelete(r.symbol)
      await reload()
      flash(`✓ 已刪除 ${r.symbol}`)
    } catch (e: any) { flash(`✗ ${e.message}`) }
  }

  function loadInto(r: SymbolBudget) {
    setSym(r.symbol); setWan(String(r.amount / 10000))
  }

  const sorted = useMemo(() => [...rows].sort((a, b) => a.symbol.localeCompare(b.symbol)), [rows])

  return (
    <div className="space-y-4">
      <section className="bg-white rounded-lg shadow p-4">
        <h2 className="font-semibold mb-1">🎯 個股金額覆寫 ({rows.length})</h2>
        <p className="text-xs text-gray-500 mb-3">
          為特定股票指定<strong>專屬下單金額</strong>。當日最終篩選清單裡有這檔時,就依此金額下單
          (依金額換算張數,仍受總預算餘額上限);<strong>其餘股票維持「③ 模擬/真實執行」頁的
          全域每檔金額/張數規則</strong>。設定跨日保留,可隨時增刪。點下方列可帶回修改。
        </p>
        <div className="flex items-center gap-2 flex-wrap mb-2">
          <input value={sym} onChange={e => setSym(e.target.value)}
                 onKeyDown={e => { if (e.key === 'Enter') save() }}
                 placeholder="股票代號 (例 2330)"
                 className="border rounded px-2 py-1 text-sm font-mono w-40" />
          <div className="flex items-center gap-1">
            <input value={wan} onChange={e => setWan(e.target.value)}
                   onKeyDown={e => { if (e.key === 'Enter') save() }}
                   placeholder="金額" inputMode="decimal"
                   className="border rounded px-2 py-1 text-sm font-mono w-28" />
            <span className="text-sm text-gray-500">萬</span>
          </div>
          <button onClick={save} disabled={saving}
                  className="px-3 py-1 rounded text-sm font-medium bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-40">
            {saving ? '儲存中…' : '儲存'}
          </button>
        </div>
        {err && <div className="text-red-600 text-sm mb-1">API 錯: {err}</div>}
        {msg && <div className="text-sm mb-1">{msg}</div>}
      </section>

      <section className="bg-white rounded-lg shadow p-4">
        <h3 className="text-sm font-semibold mb-2">已設定清單</h3>
        {sorted.length === 0 ? (
          <div className="text-gray-400 text-sm py-6 text-center">
            尚無設定 — 清單中的所有股票都用 ③ 頁全域參數
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="text-xs text-gray-500 border-b bg-gray-50">
                <tr>
                  <th className="text-left py-2 px-2">股票</th>
                  <th className="text-right py-2 px-2">專屬金額 (萬)</th>
                  <th className="text-right py-2 px-2">金額 (元)</th>
                  <th className="text-center py-2 px-2">操作</th>
                </tr>
              </thead>
              <tbody>
                {sorted.map(r => (
                  <tr key={r.symbol} onClick={() => loadInto(r)}
                      className="border-b hover:bg-blue-50 cursor-pointer" title="點一下帶回輸入欄修改">
                    <td className="py-1.5 px-2 font-mono font-semibold">{r.symbol}</td>
                    <td className="py-1.5 px-2 text-right font-mono tabular-nums">
                      {(r.amount / 10000).toLocaleString('zh-TW', { maximumFractionDigits: 2 })}
                    </td>
                    <td className="py-1.5 px-2 text-right font-mono tabular-nums text-gray-500">
                      {r.amount.toLocaleString()}
                    </td>
                    <td className="py-1.5 px-2 text-center">
                      <button onClick={e => { e.stopPropagation(); remove(r) }}
                              className="px-2 py-0.5 rounded text-xs font-medium border border-gray-300 text-gray-500 hover:bg-red-50 hover:border-red-300 hover:text-red-600">
                        刪除
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  )
}
