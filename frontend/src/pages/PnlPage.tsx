import { useEffect, useMemo, useRef, useState } from 'react'
import { api, PnlRecord } from '../api'

// 帳務台帳:每天手動輸入當日損益 (同日期再存 = 覆寫,任意日可改),
// 上方畫累積損益折線 + 每日損益正負柱 (台股慣例: 紅=賺 綠=賠)
export default function PnlPage() {
  const [records, setRecords] = useState<PnlRecord[]>([])
  const [total, setTotal] = useState(0)
  const [err, setErr] = useState('')
  const [msg, setMsg] = useState('')
  const [date, setDate] = useState(todayLocal())
  const [pnlStr, setPnlStr] = useState('')
  const [note, setNote] = useState('')
  const [saving, setSaving] = useState(false)

  async function reload() {
    try {
      const r = await api.pnlList()
      setRecords(r.records)
      setTotal(r.total)
      setErr('')
    } catch (e: any) { setErr(e.message) }
  }
  useEffect(() => { reload() }, [])

  function flash(m: string) {
    setMsg(m)
    window.setTimeout(() => setMsg(''), 4000)
  }

  async function save() {
    const v = Number(pnlStr)
    if (pnlStr.trim() === '' || Number.isNaN(v)) { flash('✗ 請輸入損益金額 (可負數)'); return }
    if (!date) { flash('✗ 請選日期'); return }
    setSaving(true)
    try {
      await api.pnlUpsert({ date, pnl: v, note: note.trim() })
      await reload()
      flash(`✓ 已存 ${date} 損益 ${fmtMoney(v)}`)
      setPnlStr(''); setNote('')
    } catch (e: any) { flash(`✗ ${e.message}`) }
    finally { setSaving(false) }
  }

  async function remove(r: PnlRecord) {
    if (!window.confirm(`確定刪除 ${r.date} 的記錄 (${fmtMoney(r.pnl)})?`)) return
    try {
      await api.pnlDelete(r.date)
      await reload()
      flash(`✓ 已刪除 ${r.date}`)
    } catch (e: any) { flash(`✗ ${e.message}`) }
  }

  // 點表格列 → 帶回輸入列修改
  function loadIntoForm(r: PnlRecord) {
    setDate(r.date)
    setPnlStr(String(r.pnl))
    setNote(r.note)
  }

  const newest = useMemo(() => [...records].reverse(), [records])

  return (
    <div className="space-y-4">
      <section className="bg-white rounded-lg shadow p-4">
        <div className="flex items-baseline gap-4 mb-1">
          <h2 className="font-semibold">📒 帳務記錄</h2>
          <span className="text-xs text-gray-500">累積總損益</span>
          <span className={`text-2xl font-bold font-mono ${signColor(total)}`}>
            {fmtMoney(total)}
          </span>
          <span className="text-xs text-gray-400">({records.length} 天)</span>
        </div>
        <p className="text-xs text-gray-500 mb-3">
          每天手動輸入該帳戶當日損益;同一天再存一次會直接覆寫 (任意日期可事後修改)。
          點下方表格任一列可帶回輸入欄修改。
        </p>
        <div className="flex items-center gap-2 flex-wrap mb-2">
          <input type="date" value={date} onChange={e => setDate(e.target.value)}
                 className="border rounded px-2 py-1 text-sm font-mono" />
          <input value={pnlStr} onChange={e => setPnlStr(e.target.value)}
                 onKeyDown={e => { if (e.key === 'Enter') save() }}
                 placeholder="當日損益 (例 12500 或 -3000)" inputMode="decimal"
                 className="border rounded px-2 py-1 text-sm font-mono w-52" />
          <input value={note} onChange={e => setNote(e.target.value)}
                 onKeyDown={e => { if (e.key === 'Enter') save() }}
                 placeholder="備註 (選填)"
                 className="border rounded px-2 py-1 text-sm w-64" />
          <button onClick={save} disabled={saving}
                  className="px-3 py-1 rounded text-sm font-medium bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-40">
            {saving ? '儲存中…' : '儲存'}
          </button>
        </div>
        {err && <div className="text-red-600 text-sm mb-1">API 錯: {err}</div>}
        {msg && <div className="text-sm mb-1">{msg}</div>}
      </section>

      <section className="bg-white rounded-lg shadow p-4">
        <h3 className="text-sm font-semibold mb-2">累積損益走勢</h3>
        {records.length === 0
          ? <div className="text-gray-400 text-sm py-10 text-center">尚無記錄 — 先在上方輸入第一天的損益</div>
          : <PnlChart records={records} />}
      </section>

      <section className="bg-white rounded-lg shadow p-4">
        <h3 className="text-sm font-semibold mb-2">每日明細 (新→舊)</h3>
        {newest.length === 0 ? (
          <div className="text-gray-400 text-sm py-4 text-center">—</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="text-xs text-gray-500 border-b bg-gray-50">
                <tr>
                  <th className="text-left py-2 px-2">日期</th>
                  <th className="text-right py-2 px-2">當日損益</th>
                  <th className="text-right py-2 px-2">累積</th>
                  <th className="text-left py-2 px-2">備註</th>
                  <th className="text-center py-2 px-2">操作</th>
                </tr>
              </thead>
              <tbody>
                {newest.map(r => (
                  <tr key={r.date} onClick={() => loadIntoForm(r)}
                      className="border-b hover:bg-blue-50 cursor-pointer" title="點一下帶回輸入欄修改">
                    <td className="py-1.5 px-2 font-mono">{r.date}</td>
                    <td className={`py-1.5 px-2 text-right font-mono tabular-nums ${signColor(r.pnl)}`}>
                      {fmtMoney(r.pnl)}
                    </td>
                    <td className={`py-1.5 px-2 text-right font-mono tabular-nums ${signColor(r.cumulative)}`}>
                      {fmtMoney(r.cumulative)}
                    </td>
                    <td className="py-1.5 px-2 text-gray-600">{r.note || <span className="text-gray-300">—</span>}</td>
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

// ─── 圖表 (inline SVG,零依賴) ──────────────────────────────
// 上面板: 累積損益折線 (藍 2px);下面板: 每日損益柱 (紅=正 綠=負,零軸上下
// 位置同時編碼正負)。共用 x 軸 (交易日等距);滑過顯示十字線 + tooltip。

const VW = 720, VH = 332
const PAD_L = 62, PAD_R = 18
const P1_TOP = 16, P1_BOT = 188      // 累積面板
const P2_TOP = 220, P2_BOT = 298    // 每日面板
const X_LABEL_Y = 318

const C_LINE = '#2a78d6'   // 累積線 (categorical slot 1)
const C_POS = '#dc2626'    // 賺 (台股紅)
const C_NEG = '#15803d'    // 賠 (台股綠)
const C_GRID = '#e5e7eb'
const C_AXIS = '#d1d5db'
const C_MUTED = '#898781'

function PnlChart({ records }: { records: PnlRecord[] }) {
  const [hover, setHover] = useState<number | null>(null)
  const svgRef = useRef<SVGSVGElement>(null)

  const n = records.length
  const step = (VW - PAD_L - PAD_R) / Math.max(n - 1, 1)
  const x = (i: number) => n === 1 ? (PAD_L + (VW - PAD_L - PAD_R) / 2) : PAD_L + i * step

  const cums = records.map(r => r.cumulative)
  const pnls = records.map(r => r.pnl)
  const s1 = scaleY(Math.min(0, ...cums), Math.max(0, ...cums), P1_TOP, P1_BOT)
  const s2 = scaleY(Math.min(0, ...pnls), Math.max(0, ...pnls), P2_TOP, P2_BOT)

  const linePath = records.map((r, i) => `${i === 0 ? 'M' : 'L'}${x(i).toFixed(1)},${s1.y(r.cumulative).toFixed(1)}`).join('')

  // x 軸標籤: 最多 6 個等距取樣
  const xTickIdx = useMemo(() => {
    const want = Math.min(6, n)
    if (want <= 1) return [0]
    const out: number[] = []
    for (let k = 0; k < want; k++) out.push(Math.round(k * (n - 1) / (want - 1)))
    return [...new Set(out)]
  }, [n])

  const barW = Math.max(2, Math.min(22, step * 0.62))

  function onMove(e: React.MouseEvent) {
    const rect = svgRef.current?.getBoundingClientRect()
    if (!rect) return
    const vx = (e.clientX - rect.left) / rect.width * VW
    const i = n === 1 ? 0 : Math.round((vx - PAD_L) / step)
    setHover(i >= 0 && i < n ? i : null)
  }

  const h = hover !== null ? records[hover] : null

  return (
    <div className="relative">
      <svg ref={svgRef} viewBox={`0 0 ${VW} ${VH}`} className="w-full select-none"
           onMouseMove={onMove} onMouseLeave={() => setHover(null)}>
        {/* 面板標題 */}
        <text x={PAD_L} y={P1_TOP - 5} fontSize="10" fill={C_MUTED}>累積損益</text>
        <text x={PAD_L} y={P2_TOP - 5} fontSize="10" fill={C_MUTED}>當日損益</text>

        {/* 上面板: 格線 + y 標籤 */}
        {s1.ticks.map(t => (
          <g key={`t1-${t}`}>
            <line x1={PAD_L} x2={VW - PAD_R} y1={s1.y(t)} y2={s1.y(t)}
                  stroke={t === 0 ? C_AXIS : C_GRID} strokeWidth="1" />
            <text x={PAD_L - 6} y={s1.y(t) + 3} fontSize="10" fill={C_MUTED}
                  textAnchor="end" style={{ fontVariantNumeric: 'tabular-nums' }}>{fmtAxis(t)}</text>
          </g>
        ))}
        {/* 下面板: 只畫零軸 + 極值標籤 */}
        {s2.ticks.map(t => (
          <g key={`t2-${t}`}>
            <line x1={PAD_L} x2={VW - PAD_R} y1={s2.y(t)} y2={s2.y(t)}
                  stroke={t === 0 ? C_AXIS : C_GRID} strokeWidth="1" />
            <text x={PAD_L - 6} y={s2.y(t) + 3} fontSize="10" fill={C_MUTED}
                  textAnchor="end" style={{ fontVariantNumeric: 'tabular-nums' }}>{fmtAxis(t)}</text>
          </g>
        ))}

        {/* 每日柱 (4px 圓角在資料端,錨在零軸) */}
        {records.map((r, i) => {
          const y0 = s2.y(0), y1 = s2.y(r.pnl)
          const top = Math.min(y0, y1), hgt = Math.max(Math.abs(y0 - y1), 1)
          return (
            <rect key={r.date} x={x(i) - barW / 2} y={top} width={barW} height={hgt}
                  rx={Math.min(3, barW / 2)} fill={r.pnl >= 0 ? C_POS : C_NEG}
                  opacity={hover === null || hover === i ? 1 : 0.45} />
          )
        })}

        {/* 累積折線 + 終點直標 */}
        {n > 1 && <path d={linePath} fill="none" stroke={C_LINE} strokeWidth="2"
                        strokeLinejoin="round" strokeLinecap="round" />}
        <circle cx={x(n - 1)} cy={s1.y(cums[n - 1])} r="3.5" fill={C_LINE} />
        <text x={Math.min(x(n - 1) + 6, VW - 4)} y={s1.y(cums[n - 1]) - 6}
              fontSize="11" fontWeight="600" fill={C_LINE}
              textAnchor={x(n - 1) > VW - 90 ? 'end' : 'start'}
              style={{ fontVariantNumeric: 'tabular-nums' }}>{fmtMoney(cums[n - 1])}</text>

        {/* x 軸日期標籤 */}
        {xTickIdx.map(i => (
          <text key={`x-${i}`} x={x(i)} y={X_LABEL_Y} fontSize="10" fill={C_MUTED}
                textAnchor="middle" style={{ fontVariantNumeric: 'tabular-nums' }}>
            {records[i].date.slice(5)}
          </text>
        ))}

        {/* 十字線 + hover 點 */}
        {hover !== null && (
          <g pointerEvents="none">
            <line x1={x(hover)} x2={x(hover)} y1={P1_TOP} y2={P2_BOT}
                  stroke="#9ca3af" strokeWidth="1" strokeDasharray="3,3" />
            <circle cx={x(hover)} cy={s1.y(cums[hover])} r="4" fill="#fff"
                    stroke={C_LINE} strokeWidth="2" />
          </g>
        )}
      </svg>

      {/* tooltip (HTML 疊在 SVG 上) */}
      {h && hover !== null && (
        <div className="absolute top-1 pointer-events-none bg-white border rounded shadow-md px-3 py-2 text-xs leading-relaxed"
             style={x(hover) / VW > 0.62
               ? { right: `${(1 - x(hover) / VW) * 100 + 2}%` }
               : { left: `${x(hover) / VW * 100 + 2}%` }}>
          <div className="font-mono font-semibold">{h.date}</div>
          <div>當日 <span className={`font-mono ${signColor(h.pnl)}`}>{fmtMoney(h.pnl)}</span></div>
          <div>累積 <span className={`font-mono ${signColor(h.cumulative)}`}>{fmtMoney(h.cumulative)}</span></div>
          {h.note && <div className="text-gray-500 max-w-48 truncate">{h.note}</div>}
        </div>
      )}
    </div>
  )
}

// y 線性刻度 + nice ticks (含 0;資料全 0 時給 ±1 避免除零)
function scaleY(min: number, max: number, top: number, bot: number) {
  if (min === 0 && max === 0) { min = -1; max = 1 }
  const span = max - min
  const pad = span * 0.08
  const lo = min < 0 ? min - pad : min
  const hi = max > 0 ? max + pad : max
  const y = (v: number) => bot - (v - lo) / (hi - lo) * (bot - top)
  const rawStep = (hi - lo) / 4
  const mag = Math.pow(10, Math.floor(Math.log10(rawStep)))
  const stepN = [1, 2, 2.5, 5, 10].map(m => m * mag).find(s => s >= rawStep) || 10 * mag
  const ticks: number[] = []
  for (let t = Math.ceil(lo / stepN) * stepN; t <= hi + 1e-9; t += stepN) ticks.push(Math.round(t * 100) / 100)
  if (!ticks.some(t => t === 0) && lo <= 0 && hi >= 0) ticks.push(0)
  return { y, ticks }
}

function fmtMoney(v: number): string {
  const s = v.toLocaleString('zh-TW', { maximumFractionDigits: 2 })
  return v > 0 ? `+${s}` : s
}

// 軸標籤縮寫: 12000 → 1.2萬 (臺灣習慣用萬)
function fmtAxis(v: number): string {
  if (v === 0) return '0'
  const a = Math.abs(v)
  if (a >= 1e8) return `${trim0(v / 1e8)}億`
  if (a >= 1e4) return `${trim0(v / 1e4)}萬`
  return trim0(v)
}
function trim0(v: number): string {
  return v.toLocaleString('zh-TW', { maximumFractionDigits: 1 })
}

function signColor(v: number): string {
  return v > 0 ? 'text-red-600' : v < 0 ? 'text-green-700' : 'text-gray-500'
}

function todayLocal(): string {
  const d = new Date()
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
}
