import { useEffect, useState } from 'react'
import { api, Status } from './api'
import FilterPage from './pages/FilterPage'
import QuotePage from './pages/QuotePage'
import SimPage from './pages/SimPage'

type Tab = 'filter' | 'quote' | 'sim'

export default function App() {
  const [tab, setTab] = useState<Tab>('filter')
  const [status, setStatus] = useState<Status | null>(null)
  const [err, setErr] = useState<string>('')

  async function reloadStatus() {
    try {
      setStatus(await api.status())
      setErr('')
    } catch (e: any) {
      setErr(e.message)
    }
  }

  useEffect(() => {
    reloadStatus()
    const id = window.setInterval(reloadStatus, 2000)
    return () => window.clearInterval(id)
  }, [])

  return (
    <div className="min-h-screen bg-gray-50 text-gray-800">
      <header className="bg-white border-b sticky top-0 z-10">
        <div className="max-w-6xl mx-auto px-4 py-3 flex items-center gap-4">
          <h1 className="text-lg font-bold">🚀 搶漲停</h1>
          <div className="ml-auto flex items-center gap-3 text-sm">
            {status && (
              <>
                <span className={`px-2 py-0.5 rounded text-xs font-medium ${phaseColor(status.phase)}`}>
                  {phaseLabel(status.phase)}
                </span>
                <span className="text-gray-500 text-xs">
                  {new Date(status.now).toLocaleTimeString('zh-TW')}
                </span>
              </>
            )}
            {err && <span className="text-red-600 text-xs">API 錯: {err}</span>}
            <button
              onClick={async () => {
                if (confirm('立刻啟動 runner (通常靠 8:00 auto trigger)？')) {
                  await api.startNow()
                  reloadStatus()
                }
              }}
              className="text-xs bg-green-600 text-white px-2 py-1 rounded hover:bg-green-700"
            >
              ▶ 手動啟動
            </button>
            <button
              onClick={async () => {
                if (confirm('停止 runner (會平倉+關 WS)？')) {
                  await api.stopNow()
                  reloadStatus()
                }
              }}
              className="text-xs bg-red-600 text-white px-2 py-1 rounded hover:bg-red-700"
            >
              ⏹ 停止
            </button>
          </div>
        </div>
        <div className="max-w-6xl mx-auto px-4 flex gap-1">
          <TabBtn active={tab === 'filter'} onClick={() => setTab('filter')}>① 篩選</TabBtn>
          <TabBtn active={tab === 'quote'} onClick={() => setTab('quote')}>② 股票資料</TabBtn>
          <TabBtn active={tab === 'sim'} onClick={() => setTab('sim')}>③ 模擬執行</TabBtn>
        </div>
      </header>

      <main className="max-w-6xl mx-auto px-4 py-4">
        {tab === 'filter' && <FilterPage />}
        {tab === 'quote' && <QuotePage />}
        {tab === 'sim' && <SimPage />}
      </main>
    </div>
  )
}

function TabBtn({ active, children, onClick }: {
  active: boolean; children: React.ReactNode; onClick: () => void
}) {
  return (
    <button
      onClick={onClick}
      className={`px-4 py-2 text-sm font-medium border-b-2 -mb-px ${
        active
          ? 'border-blue-600 text-blue-700'
          : 'border-transparent text-gray-500 hover:text-gray-800'
      }`}
    >
      {children}
    </button>
  )
}

function phaseColor(p: string): string {
  const m: Record<string, string> = {
    idle: 'bg-gray-100 text-gray-600',
    login: 'bg-blue-100 text-blue-700',
    fetch_universe: 'bg-blue-100 text-blue-700',
    fetch_limit_ups: 'bg-yellow-100 text-yellow-700',
    subscribe: 'bg-yellow-100 text-yellow-700',
    filtering: 'bg-green-100 text-green-700 animate-pulse',
    live_subscribe: 'bg-cyan-100 text-cyan-700 animate-pulse',
    trading: 'bg-purple-100 text-purple-700 animate-pulse',
    finished: 'bg-gray-100 text-gray-500',
    error: 'bg-red-100 text-red-700',
  }
  return m[p] || 'bg-gray-100 text-gray-600'
}

function phaseLabel(p: string): string {
  const m: Record<string, string> = {
    idle: '未啟動',
    login: '登入中',
    fetch_universe: '抓股票清單',
    fetch_limit_ups: '抓漲停價',
    subscribe: '訂閱 WS',
    filtering: '篩選中',
    live_subscribe: '常駐收 tick',
    trading: '交易中',
    finished: '已結束',
    error: '錯誤',
  }
  return m[p] || p
}
