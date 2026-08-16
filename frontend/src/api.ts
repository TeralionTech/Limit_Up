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
  // ─── 交易 (模擬/真實) ───
  tradingStatus: () => req<TradingStatus>('/api/trading/status'),
  tradingConnect: (p: TradingConnectReq) =>
    req<{ status: string }>('/api/trading/connect', { method: 'POST', body: JSON.stringify(p) }),
  tradingDisconnect: () =>
    req<{ ok: boolean }>('/api/trading/disconnect', { method: 'POST' }),
  tradingMode: (mode: 'sim' | 'real') =>
    req<{ ok: boolean }>('/api/trading/mode', { method: 'POST', body: JSON.stringify({ mode }) }),
  tradingParams: (p: { total_budget?: number; per_symbol_budget?: number;
                        sizing_mode?: 'budget' | 'fixed_lots'; fixed_lots?: number }) =>
    req<{ ok: boolean }>('/api/trading/params', { method: 'POST', body: JSON.stringify(p) }),
  tradingArm: (armed: boolean) =>
    req<{ ok: boolean }>('/api/trading/arm', { method: 'POST', body: JSON.stringify({ armed }) }),
  tradingCloseAll: () =>
    req<{ ok: boolean; sold_symbols: number }>('/api/trading/close_all', { method: 'POST' }),
  tradingOvernight: () => req<{ overnight: OvernightRow[] }>('/api/trading/overnight'),
  tradingOvernightSkip: (symbol: string, skip: boolean) =>
    req<{ ok: boolean }>('/api/trading/overnight/skip', {
      method: 'POST', body: JSON.stringify({ symbol, skip }),
    }),
  tradingOvernightAdd: (symbol: string) =>
    req<{ ok: boolean; added: boolean }>('/api/trading/overnight/add', {
      method: 'POST', body: JSON.stringify({ symbol }),
    }),
  tradingOvernightRemove: (symbol: string) =>
    req<{ ok: boolean; removed: boolean }>('/api/trading/overnight/remove', {
      method: 'POST', body: JSON.stringify({ symbol }),
    }),
  tradingOrders: () => req<{ orders: OrderRow[] }>('/api/trading/orders'),
  tradingCancelOrder: (order_no: string) =>
    req<{ ok: boolean }>('/api/trading/cancel_order', {
      method: 'POST', body: JSON.stringify({ order_no }),
    }),
  traderAbandon: (symbol: string) =>
    req<{ ok: boolean }>('/api/trader/abandon', {
      method: 'POST', body: JSON.stringify({ symbol }),
    }),
  // ─── 帳務台帳 (每日手動損益) ───
  pnlList: () => req<{ records: PnlRecord[]; total: number }>('/api/pnl'),
  pnlUpsert: (p: { date: string; pnl: number; note?: string }) =>
    req<{ ok: boolean }>('/api/pnl', { method: 'POST', body: JSON.stringify(p) }),
  pnlDelete: (date: string) =>
    req<{ ok: boolean; removed: boolean }>('/api/pnl/delete', {
      method: 'POST', body: JSON.stringify({ date }),
    }),
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
  first_tick_marked?: string[]   // marked 中「開盤即鎖」(首筆真實報價就漲停) 子集
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
  trading?: TradingStatus | null
  first_stage?: FirstStageRow[]
  tracking?: TrackingRow[]
}

// 該檔交易 state (real mode; sim 為 null)
export interface SymbolTrade {
  target_lots: number
  order_no: string
  order_kind: string       // pre_limit / market_buy / market_sell
  order_status: string     // '' / pending / cancelled / rejected / done
  filled_lots: number
  avg_price: number
  stopped_reason: string
  exited: boolean
}

export interface FirstStageRow {
  symbol: string
  limit_up: number
  first_tick?: boolean   // 「開盤即鎖」徽章
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
  trade?: SymbolTrade | null
}

export interface TrackingRow {
  symbol: string
  limit_up: number
  first_tick?: boolean   // 「開盤即鎖」徽章
  bid1_price: number
  bid1_size: number
  ask1_price: number
  ask1_size: number
  status: 'tracking' | 'pulled' | 'abandoned'
  pulled_reason: string
  warning?: string
  last_trade_price: number
  last_book_ts: string
  trade?: SymbolTrade | null
}

// ─── 交易 (模擬/真實) ───

export interface TradingStatus {
  mode: 'sim' | 'real'
  armed: boolean
  connecting: boolean
  connect_error: string
  connected: boolean
  healthy: boolean
  account_masked: string
  is_test: boolean
  error: string
  params: {
    total_budget: number; per_symbol_budget: number
    sizing_mode?: 'budget' | 'fixed_lots'; fixed_lots?: number
  }
  budget_used: number
  n_symbols: number
  n_positions: number
}

export interface TradingConnectReq {
  account_id: string
  password: string
  pfx_password?: string
  is_test?: boolean
  pfx_b64: string
  pfx_filename?: string
}

// 委託總表 row (真實模式;右鍵可刪 pending 單)
export interface OrderRow {
  order_no: string
  symbol: string
  action: 'buy' | 'sell'
  kind: string             // pre_limit / market_buy / market_sell
  lots: number
  price: number            // 0 = 市價
  status: 'pending' | 'filled' | 'cancelled' | 'rejected'
  filled_lots: number
  ts: string
}

// 帳務台帳 row (每日手動輸入,後端已算好累積)
export interface PnlRecord {
  date: string          // YYYY-MM-DD
  pnl: number           // 當日損益 (可負)
  note: string
  cumulative: number    // 累積損益 (依日期舊→新累加)
}

// 隔日賣標的 row (昨天買到未出場的持倉,今天要賣)
export interface OvernightRow {
  symbol: string
  lots: number
  avg_cost: number
  reconciled: boolean       // 是否已對帳券商庫存
  note: string
  sell_placed: boolean
  sell_price: number
  sold_lots: number
  sell_status: string       // '' / pending / filled / cancelled / rejected
  skip: boolean             // 使用者按「不要賣」暫停
  locked_now: boolean       // 目前鎖漲停中 (市價列/買牆在) → 抱著不賣
  books?: {
    bids: Array<{ price: number; size: number }>
    asks: Array<{ price: number; size: number }>
    ts: string
  } | null
  last_trade?: { price: number; size: number; ts: string } | null
}
