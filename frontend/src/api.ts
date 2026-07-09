// API client — 打 backend /api/*

const BASE = ''   // 前端 dev 走 Vite proxy；build 後跟 backend 同 origin

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(BASE + path, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  })
  if (!r.ok) throw new Error(`HTTP ${r.status}: ${await r.text()}`)
  return r.json()
}

export const api = {
  status: () => req<Status>('/api/status'),
  startNow: () => req<{ ok: boolean }>('/api/start', { method: 'POST' }),
  stopNow: () => req<{ ok: boolean }>('/api/stop', { method: 'POST' }),
  filterProgress: () => req<FilterProgress>('/api/filter/progress'),
  watchlist: () => req<Watchlist>('/api/filter/watchlist'),
  tick: (symbol: string) => req<TickSnapshot>(`/api/tick/${symbol}`),
  traderSummary: () => req<TraderSummary>('/api/trader/summary'),
  getTraderParams: () => req<{ first_trade_min_lots: number }>('/api/trader/params'),
  setTraderParams: (p: { first_trade_min_lots: number }) =>
    req<{ ok: boolean }>('/api/trader/params', { method: 'POST', body: JSON.stringify(p) }),
}

// ─── Types ──────────────────────────────────────────────

export interface Status {
  phase: string
  is_running: boolean
  started_at?: string | null
  now: string
  error: string
  limit_up_progress: { done: number; total: number; ok: number; fail: number }
  universe_size: number
  filter_stats: Record<string, number>
  watchlist_size: number
  recorder_tick_count: number
}

export interface FilterProgress {
  phase: string
  limit_up_done: number
  limit_up_total: number
  limit_up_ok: number
  limit_up_fail: number
  universe_size: number
  recorder_tick_count: number
  tick_stats: { total_tick_count?: number; books_count?: number; trades_count?: number }
  filter_stats: {
    currently_marked?: number
    total_mark_events?: number
    total_unmark_events?: number
    unmark_by_ask_appeared?: number
    unmark_by_bid_below_limit?: number
    unmark_by_bid_dropped?: number
    unique_symbols_touched?: number
  }
}

export interface Watchlist {
  marked: string[]
  discarded: Array<{ symbol: string; reason: string; ts: string }>
}

export interface TickSnapshot {
  symbol: string
  books?: {
    bids: Array<{ price: number; size: number }>
    asks: Array<{ price: number; size: number }>
    ts: string
  } | null
  last_trade?: { price: number; size: number; ts: string } | null
  is_pre_match: boolean
  pre_match_kind?: string | null
  limit_up?: number | null
  is_watched: boolean
  is_discarded?: boolean
}

export interface TraderSummary {
  trader_active: boolean
  watchlist_total?: number
  n_tracking?: number
  n_pulled?: number
  n_first_failed?: number
  min_lots?: number
  first_stage?: FirstStageRow[]
  tracking?: TrackingRow[]
}

export interface FirstStageRow {
  symbol: string
  limit_up: number
  status: 'waiting' | 'discarded_first' | 'tracking' | 'pulled'
  first_books: {
    bid1_price: number; bid1_size: number
    ask1_price: number; ask1_size: number
    ts: string
  } | null
  first_trade: {
    price: number; qty: number; lots: number; ts: string
  } | null
  fail_reason: string
}

export interface TrackingRow {
  symbol: string
  limit_up: number
  bid1_price: number
  bid1_size: number
  ask1_price: number
  ask1_size: number
  status: 'tracking' | 'pulled'
  pulled_reason: string
  last_trade_price: number
  last_book_ts: string
}
