'use strict';
// Paper trading — simulated Phoenix perps on a virtual $10k USDC account.
// One position per (user, market), cross OR isolated margin per position.
// State lives in users.db (SQLite/WAL) — never an in-memory blob (the
// phoenix-teams heap is capped). Equity is always derived, never stored.
//
// PRICES — freshest wins:
//  1. In-process Phoenix WS (marketStats) — sub-second marks; kills the
//     stale-price arbitrage a 5s file cadence would leave open.
//  2. data/markets-snapshot.json (5s file) as fallback when the WS is down.
//  Per-market freshness (MARKET_FRESH_MS) is enforced on every fill — a
//  symbol whose feed went quiet (e.g. delisted TON) is not tradable.
//
// REAL PRINTS — the geyser→ClickHouse warehouse feed (/perps/live-feed via
// the 9100 tunnel, injected as warehouseGet) supplies actual on-chain trades
// ~2-4s behind the chain. Resting limits fill only when a REAL trade prints
// through the limit price (price-time realism); mark-cross is the fallback
// for markets with no recent prints or when the warehouse is down.
//
// EXACT PHOENIX MODEL (from /exchange config + 264k real fills):
//  - Fees: per-market takerFee/makerFee (uniform 3.5/0.5 bps today; real
//    traders mostly pay 90% via a discount program — we charge official
//    base). Liquidations pay plain taker fee. SL/TP execute as taker.
//  - Leverage tiers WITH SIZE BANDS: every market steps to 1x above a
//    per-market size bound (~$1-3.4M notional) — tierLevFor(sym, size) is
//    the binding cap, and protocol IM/maintenance use the tier leverage for
//    the position's ACTUAL size, so oversized positions carry real risk.
//  - Margin: protocol IM = notional / tierLev(size); riskFactors:
//    maintenanceBps (50% of IM) → liquidation, cancelOrderBps (75%/70%) →
//    open orders cancelled first. User ticket leverage only sizes the
//    margin reservation (cross) or the allocated margin (isolated).
//  - Isolated margin: margin moves balance → position at open (notional/lev),
//    funding settles against it, liquidation is per-position with loss capped
//    at the allocated margin (bad debt absorbed, Phoenix-style), remainder
//    returns to balance on close. isolatedOnly markets (GOLD, SILVER, COPPER,
//    WTIOIL, ANSEM, SKR) force isolated. Cross positions share account
//    equity; cross liquidation is progressive (maxLiquidationSizeBaseLots
//    per pass). No post-open margin adjustment yet (documented gap).
//  - Funding: currentFundingRate is PERCENT per hour (verified against
//    maxFundingRatePerIntervalPercentage caps); settles once per hour
//    boundary (fundingIntervalSeconds=3600). Positive rate: longs pay.
//  - marketStatus: 'active' required for taker fills; 'postOnly' (SKHY)
//    accepts resting limits only. Closes always allowed.
const fs = require('fs');
const auth = require('./auth-shim.js');
const comp = require('./competition.js');
const db = auth.db;
let WebSocket = null;
try { WebSocket = require('ws'); } catch {}

const START_BALANCE = 10000;
const FALLBACK_TAKER_BPS = 3.5;
const FALLBACK_MAKER_BPS = 0.5;
const FALLBACK_MAINT_BPS = 5000;
const FALLBACK_CANCEL_BPS = 7500;
const SNAPSHOT_FILE = process.env.PHOENIX_SNAPSHOT_FILE || '/var/www/phoenix-showdown/data/markets-snapshot.json';
const SNAP_TTL_MS = 1_000;
const WS_URL = 'wss://perp-api.phoenix.trade/v1/ws';
const WS_FRESH_MS = 15_000;
const MARKET_FRESH_MS = 30_000;
const DELIST_MS = 3600_000;
const RESET_COOLDOWN_MS = 10 * 60_000;
const MAX_OPEN_ORDERS = 20;
const MIN_NOTIONAL = 10;
const HEAT_BALANCE = 10;          // heat-mode real bankroll (UI displays ×HEAT_SCALE)
const HEAT_MAX_LEV = 1000;        // heat/Boost cap — $10×1000x=$10k notional, fills top-of-book
const BOOST_WINDOW_MS = 2 * 60_000;   // a Boost-class fill starts a 2-min clock; then the position auto-flattens
const BOOST_ARM_LEV = 101;            // anything ABOVE the 100x baseline is Boost-class — 999x can't dodge the clock
const HEAT_MIN_NOTIONAL = 0.01;   // real dollars; scaled floor is $100 on screen
const MAX_NOTIONAL = 5_000_000;
// Stage has no orderbook: fills execute AT the index with zero slippage and
// zero fees, so there is no book to exhaust and the 5M cap models nothing.
// Size is already bounded by margin (notional <= free * leverage), which is the
// real constraint -- a $100k account at 1000x can reach $100M and no further.
// This ceiling exists only to reject nonsense payloads. Before this, Max sized
// from margin and the engine rejected it, so Max was unusable at any boost.
const STAGE_MAX_NOTIONAL = Number(process.env.PAPER_STAGE_MAX_NOTIONAL || 1_000_000_000);
const LIMIT_BAND = 0.5;
const FUNDING_INTERVAL_MS = 3600_000;
const FUNDING_MAX_INTERVALS = 24;
const FUNDING_RATE_CLAMP = 0.5;        // sanity: |percent per hour| ≤ 0.5
const FALLBACK_MAX_LEV = 10;
// Optional leverage override (paper is simulated; we are not bound by the
// venue caps). PAPER_MAX_LEV=1000 scales every market's tier table so the
// small-size cap becomes 1000x while the size-band SHAPE is preserved
// (big positions still step down proportionally). IM (1/tierLev) and
// maintenance (0.5/tierLev via maintBps) derive from the scaled tiers, so
// margin, liquidation prices and order validation stay internally
// consistent. Physics at 1000x: liq distance ~0.05% and the 3.5bps taker
// fee is 35% of margin — that is the intended arcade, not a bug.
const PAPER_MAX_LEV = Number(process.env.PAPER_MAX_LEV || 0);
const WRITES_PER_MIN = 30;
const FILLS_RETENTION_MS = 30 * 24 * 3600_000;
const LIQ_MAX_PASSES = 12;
const BOOK_FRESH_MS = 4_000;           // L2 snapshot considered live
const BOOK_AWAIT_MS = 1_500;           // max wait for a cold symbol's first book
const BOOK_IDLE_MS = 10 * 60_000;      // unsubscribe books nobody is using
const MAX_SLIP_PCT = 1;                // taker slippage tolerance (Phoenix UI default)
const PRINT_POLL_MS = 2_500;           // warehouse live-feed poll cadence
const PRINT_WINDOW_MS = 90_000;        // prints kept for limit-cross checks
const PRINT_ACTIVE_MS = 5 * 60_000;    // market "has a tape" if a print this recent
// Empirical price impact for taker fills, measured from 30d of real Phoenix
// taker market fills vs same-minute median price (warehouse forensics):
// median dev ~0 bps below $100k notional, ~+10 bps above; mean drifts +2.5bps
// in the 10k-100k band. Applied adverse to every taker-style fill.
const SLIPPAGE_TIERS = [[10_000, 0], [100_000, 2.5], [Infinity, 10]];
function slipBps(notional) {
  for (const [cap, bps] of SLIPPAGE_TIERS) if (notional <= cap) return bps;
  return SLIPPAGE_TIERS[SLIPPAGE_TIERS.length - 1][1];
}
// adverse execution price for a taker fill of this size
function takerPx(mark, orderSide, notional) {
  const s = slipBps(notional) / 1e4;
  return rpx(orderSide === 'BUY' ? mark * (1 + s) : mark * (1 - s));
}

db.exec(`
  CREATE TABLE IF NOT EXISTS paper_accounts (
    user_id      INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    epoch        INTEGER NOT NULL DEFAULT 1,
    balance      REAL    NOT NULL DEFAULT ${START_BALANCE},
    resets       INTEGER NOT NULL DEFAULT 0,
    fills_count  INTEGER NOT NULL DEFAULT 0,
    fees_paid    REAL    NOT NULL DEFAULT 0,
    funding_paid REAL    NOT NULL DEFAULT 0,
    liquidations INTEGER NOT NULL DEFAULT 0,
    created_at   INTEGER, reset_at INTEGER, updated_at INTEGER
  );
  CREATE TABLE IF NOT EXISTS paper_positions (
    user_id       INTEGER NOT NULL,
    symbol        TEXT    NOT NULL,
    epoch         INTEGER NOT NULL,
    side          TEXT    NOT NULL CHECK (side IN ('LONG','SHORT')),
    size          REAL    NOT NULL,
    entry_price   REAL    NOT NULL,
    leverage      REAL    NOT NULL,
    realized_pnl  REAL    NOT NULL DEFAULT 0,
    funding_accrued REAL  NOT NULL DEFAULT 0,
    last_funding_ms INTEGER NOT NULL,
    last_mark     REAL,
    sl_price      REAL, tp_price REAL,
    opened_at     INTEGER, updated_at INTEGER,
    PRIMARY KEY (user_id, symbol)
  );
  CREATE INDEX IF NOT EXISTS idx_ppos_symbol ON paper_positions(symbol);
  CREATE TABLE IF NOT EXISTS paper_orders (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL,
    epoch       INTEGER NOT NULL,
    symbol      TEXT NOT NULL,
    side        TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
    price       REAL NOT NULL,
    size        REAL NOT NULL,
    leverage    REAL NOT NULL,
    reduce_only INTEGER NOT NULL DEFAULT 0,
    status      TEXT NOT NULL DEFAULT 'OPEN',
    created_at  INTEGER, closed_at INTEGER
  );
  CREATE INDEX IF NOT EXISTS idx_pord_open ON paper_orders(status) WHERE status = 'OPEN';
  CREATE INDEX IF NOT EXISTS idx_pord_user ON paper_orders(user_id, status);
  CREATE TABLE IF NOT EXISTS paper_fills (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id   INTEGER NOT NULL,
    epoch     INTEGER NOT NULL,
    symbol    TEXT NOT NULL,
    side      TEXT NOT NULL,
    kind      TEXT NOT NULL,
    price     REAL NOT NULL,
    size      REAL NOT NULL,
    notional  REAL NOT NULL,
    fee       REAL NOT NULL,
    realized_pnl REAL,
    order_id  INTEGER,
    ts        INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_pfill_user ON paper_fills(user_id, epoch, id);
`);
// column migrations (CREATE IF NOT EXISTS won't extend existing tables)
for (const ddl of [
  "ALTER TABLE paper_positions ADD COLUMN margin_mode TEXT NOT NULL DEFAULT 'cross'",
  "ALTER TABLE paper_positions ADD COLUMN isolated_margin REAL NOT NULL DEFAULT 0",
  "ALTER TABLE paper_orders ADD COLUMN margin_mode TEXT NOT NULL DEFAULT 'cross'",
  'ALTER TABLE paper_orders ADD COLUMN attach_sl REAL',
  'ALTER TABLE paper_orders ADD COLUMN attach_tp REAL',
  // partial-fill watermark: last ts whose printed-through volume this resting
  // limit has already consumed (defaults to created_at at read time when null)
  'ALTER TABLE paper_orders ADD COLUMN vol_ts INTEGER',
  // heat mode: FT stage accounts — tiny real bankroll (display-scaled in the
  // UI), zero fees/funding, fractional lots. See ft-world-cup engine handoff.
  'ALTER TABLE paper_accounts ADD COLUMN heat INTEGER NOT NULL DEFAULT 0',
  'ALTER TABLE paper_accounts ADD COLUMN start_balance REAL',  // was a manual prod ALTER; fresh installs crashed without it
  // exact ledger identity across busts: balance = start + Σrealized - Σfees
  // + Σfunding + Σbad_debt. Fills record GROSS realized; the clamp goes here.
  'ALTER TABLE paper_fills ADD COLUMN bad_debt REAL NOT NULL DEFAULT 0',
  // Boost window: set when a ≥1000x fill lands on the position; the position
  // auto-flattens BOOST_WINDOW_MS later (kind EXPIRY). Never cleared by
  // adds/reduces — using Boost starts a clock the position cannot shed.
  'ALTER TABLE paper_positions ADD COLUMN boost_since INTEGER',
]) { try { db.exec(ddl); } catch {} }

const stmt = {
  acctGet:    db.prepare('SELECT * FROM paper_accounts WHERE user_id = ?'),
  acctIns:    db.prepare('INSERT OR IGNORE INTO paper_accounts (user_id, created_at, updated_at) VALUES (?, ?, ?)'),
  acctUpd:    db.prepare(`UPDATE paper_accounts SET balance = ?, fills_count = ?, fees_paid = ?, funding_paid = ?, liquidations = ?, updated_at = ? WHERE user_id = ?`),
  acctReset:  db.prepare(`UPDATE paper_accounts SET epoch = epoch + 1, balance = COALESCE(start_balance, ${START_BALANCE}), resets = resets + 1,
                fills_count = 0, fees_paid = 0, funding_paid = 0, liquidations = 0, reset_at = ?, updated_at = ? WHERE user_id = ?`),
  acctAll:    db.prepare('SELECT * FROM paper_accounts WHERE fills_count > 0'),
  posGet:     db.prepare('SELECT * FROM paper_positions WHERE user_id = ? AND symbol = ?'),
  posByUser:  db.prepare('SELECT * FROM paper_positions WHERE user_id = ?'),
  posCount:   db.prepare('SELECT COUNT(*) AS n FROM paper_positions WHERE user_id = ?'),
  posAll:     db.prepare('SELECT * FROM paper_positions'),
  posIns:     db.prepare(`INSERT INTO paper_positions (user_id, symbol, epoch, side, size, entry_price, leverage, last_funding_ms, last_mark, opened_at, updated_at, margin_mode, isolated_margin)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`),
  posUpd:     db.prepare('UPDATE paper_positions SET size = ?, entry_price = ?, leverage = ?, realized_pnl = ?, isolated_margin = ?, updated_at = ? WHERE user_id = ? AND symbol = ?'),
  posBoostStamp: db.prepare('UPDATE paper_positions SET boost_since = ? WHERE user_id = ? AND symbol = ? AND boost_since IS NULL'),
  posMark:    db.prepare('UPDATE paper_positions SET last_mark = ?, updated_at = ? WHERE user_id = ? AND symbol = ?'),
  posFunding: db.prepare('UPDATE paper_positions SET funding_accrued = ?, last_funding_ms = ?, isolated_margin = ? WHERE user_id = ? AND symbol = ?'),
  posSltp:    db.prepare('UPDATE paper_positions SET sl_price = ?, tp_price = ? WHERE user_id = ? AND symbol = ?'),
  posDel:     db.prepare('DELETE FROM paper_positions WHERE user_id = ? AND symbol = ?'),
  posDelUser: db.prepare('DELETE FROM paper_positions WHERE user_id = ?'),
  ordGet:     db.prepare('SELECT * FROM paper_orders WHERE id = ?'),
  ordOpenAll: db.prepare("SELECT * FROM paper_orders WHERE status = 'OPEN' ORDER BY id"),
  ordOpenByUser: db.prepare("SELECT * FROM paper_orders WHERE user_id = ? AND status = 'OPEN' ORDER BY id"),
  ordCountOpen:  db.prepare("SELECT COUNT(*) AS n FROM paper_orders WHERE user_id = ? AND status = 'OPEN'"),
  ordIns:     db.prepare(`INSERT INTO paper_orders (user_id, epoch, symbol, side, price, size, leverage, reduce_only, created_at, margin_mode, attach_sl, attach_tp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`),
  ordClose:   db.prepare('UPDATE paper_orders SET status = ?, closed_at = ? WHERE id = ?'),
  // partial fill of a resting limit: shrink remaining size, advance the volume
  // watermark so the same prints are never consumed twice across sweeps
  ordPartial: db.prepare('UPDATE paper_orders SET size = ?, vol_ts = ? WHERE id = ?'),
  ordCancelUser: db.prepare("UPDATE paper_orders SET status = 'CANCELLED', closed_at = ? WHERE user_id = ? AND status = 'OPEN'"),
  fillIns:    db.prepare(`INSERT INTO paper_fills (user_id, epoch, symbol, side, kind, price, size, notional, fee, realized_pnl, order_id, ts, bad_debt)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`),
  posBySymbol: db.prepare('SELECT * FROM paper_positions WHERE symbol = ?'),
  fillList:   db.prepare('SELECT * FROM paper_fills WHERE user_id = ? AND epoch = ? AND id < ? ORDER BY id DESC LIMIT ?'),
  fillListAll: db.prepare('SELECT * FROM paper_fills WHERE user_id = ? AND id < ? ORDER BY id DESC LIMIT ?'),
  fillListTrades:  db.prepare("SELECT * FROM paper_fills WHERE user_id = ? AND epoch = ? AND kind != 'FUNDING' AND id < ? ORDER BY id DESC LIMIT ?"),
  fillListFunding: db.prepare("SELECT * FROM paper_fills WHERE user_id = ? AND epoch = ? AND kind = 'FUNDING' AND id < ? ORDER BY id DESC LIMIT ?"),
  fillPrune:  db.prepare('DELETE FROM paper_fills WHERE ts < ?'),
  ordPrune:   db.prepare("DELETE FROM paper_orders WHERE status != 'OPEN' AND created_at < ?"),
  ordHistory: db.prepare('SELECT * FROM paper_orders WHERE user_id = ? AND epoch = ? ORDER BY id DESC LIMIT ?'),
};

// ── wiring from server.js ────────────────────────────────────────────────
let _apiGet = null;
let _warehouseGet = null;
let _log = (...a) => console.log('[paper]', ...a);
let _readRateOk = null;

// symbol → exact market config from /exchange (2h server cache)
const mktCfg = new Map();
async function refreshExchange() {
  if (!_apiGet) return;
  try {
    const ex = await _apiGet('/exchange', 3);
    for (const mk of (ex && ex.markets) || []) {
      if (!mk.symbol) continue;
      const dec = Number(mk.baseLotsDecimals);
      const lotFactor = Number.isFinite(dec) ? Math.pow(10, -dec) : null;
      // size-banded leverage tiers: [{maxLev, maxSizeBase}] ascending by size.
      // Every market steps to 1x above a bound (~$1-3.4M notional).
      let tiers = (Array.isArray(mk.leverageTiers) ? mk.leverageTiers : [])
        .map((t) => ({ maxLev: Number(t.maxLeverage) || 1, maxSizeBase: lotFactor ? Number(t.maxSizeBaseLots) * lotFactor : Infinity }))
        .filter((t) => t.maxLev > 0 && t.maxSizeBase > 0)
        .sort((a, b) => a.maxSizeBase - b.maxSizeBase);
      if (PAPER_MAX_LEV > 0 && tiers.length) {
        const natMax = tiers.reduce((mx, t) => Math.max(mx, t.maxLev), 0);
        if (natMax > 0 && PAPER_MAX_LEV > natMax) {
          const s = PAPER_MAX_LEV / natMax;
          tiers = tiers.map((t) => ({ ...t, maxLev: Math.max(1, Math.round(t.maxLev * s)) }));
        }
      }
      const rf = mk.riskFactors || {};
      const maxLiqLots = Number(mk.maxLiquidationSizeBaseLots);
      mktCfg.set(String(mk.symbol).toUpperCase(), {
        tiers,
        maxLev: tiers.reduce((mx, t) => Math.max(mx, t.maxLev), 0) || FALLBACK_MAX_LEV,
        lotSize: lotFactor,
        takerBps: Number(mk.takerFee) > 0 ? Number(mk.takerFee) * 1e4 : FALLBACK_TAKER_BPS,
        makerBps: Number(mk.makerFee) > 0 ? Number(mk.makerFee) * 1e4 : FALLBACK_MAKER_BPS,
        maintBps: Number(rf.maintenanceBps) > 0 ? Number(rf.maintenanceBps) : FALLBACK_MAINT_BPS,
        cancelBps: Number(rf.cancelOrderBps) > 0 ? Number(rf.cancelOrderBps) : FALLBACK_CANCEL_BPS,
        maxLiqSize: maxLiqLots > 0 && lotFactor ? maxLiqLots * lotFactor : null,
        status: mk.marketStatus || 'active',
        isolatedOnly: !!mk.isolatedOnly,
      });
    }
    _log(`exchange config loaded: ${mktCfg.size} markets`);
  } catch (e) { _log('exchange refresh fail: ' + e.message); }
}

// ── in-process Phoenix WS (primary price source) ─────────────────────────
const live = { map: new Map(), lastMsgMs: 0, started: false, ws: null };
// ── live L2 books (lazy per-symbol subscriptions; subscribe-all is ignored
// by the gateway for the orderbook channel, verified). A symbol's book is
// subscribed when an order/position first needs it and dropped after
// BOOK_IDLE_MS unused. Fills WALK these levels — the engine's own books,
// never anything client-supplied.
const books = { map: new Map(), subs: new Map() };   // symbol -> {bids, asks, mid, ts} / symbol -> lastUsedMs
// ── market tape: recent prints per symbol, newest first. The gateway honors
// subscribe-all for the trades channel (we already ingest every print for
// mark refinement), so recording it costs nothing extra. Ring-buffered and
// age-pruned; serves /api/paper/tape so the terminal can show hours of
// history instead of only what streamed in since page load.
const TAPE_MAX = 2500;                 // rows kept per symbol
const TAPE_MAX_AGE_MS = 3 * 3600_000;  // and nothing older than 3h
const tape = new Map();                // symbol -> [{ts, side, price, notional, seq}]
function tapePush(symbol, row) {
  let arr = tape.get(symbol);
  if (!arr) { arr = []; tape.set(symbol, arr); }
  arr.unshift(row);
  // prints occasionally arrive out of order across gateway batches
  if (arr.length > 1 && arr[1].seq > row.seq) arr.sort((a, b) => b.seq - a.seq);
  if (arr.length > TAPE_MAX) arr.length = TAPE_MAX;
}
setInterval(() => {
  const cut = Date.now() - TAPE_MAX_AGE_MS;
  for (const [sym, arr] of tape) {
    while (arr.length && arr[arr.length - 1].ts < cut) arr.pop();
    if (!arr.length) tape.delete(sym);
  }
}, 5 * 60_000).unref();
// ── raw pyth tick history ────────────────────────────────────────────────
// Every Hermes update (~2Hz/symbol) is recorded so the stage tick chart can
// seed real oracle-density history instead of flat 1-minute Benchmarks
// shelves. In-memory ring: ~45min × 2Hz ≈ 5.4k pts/symbol. Empty for the
// first minutes after a restart — the client splices Benchmarks behind it.
const PYTH_HIST_MAX = 9500;
const PYTH_HIST_MAX_AGE_MS = 45 * 60_000;
const PYTH_HIST_MIN_GAP_MS = 300;      // Hermes pushes ~6Hz; the chart grid is 500ms — thin, don't hoard
const pythHist = new Map();            // symbol -> [[tsMs, px], ...] ascending
const _pythHistAnchor = new Map();     // symbol -> ts of last APPENDED row (throttle anchor)
function pythHistPush(symbol, ts, px) {
  let arr = pythHist.get(symbol);
  if (!arr) { arr = []; pythHist.set(symbol, arr); }
  // throttle against the last APPEND, not the last write — measuring the gap
  // from a timestamp we keep sliding forward turns a steady sub-300ms stream
  // into a debounce that only ever commits on pauses (recorded 3-4s "gaps"
  // on a healthy ~5Hz stream)
  const anchor = _pythHistAnchor.get(symbol) || 0;
  const last = arr[arr.length - 1];
  if (last && ts - anchor < PYTH_HIST_MIN_GAP_MS) { last[0] = ts; last[1] = px; return; }   // refresh in-window, keep cadence
  _pythHistAnchor.set(symbol, ts);
  arr.push([ts, px]);
  if (arr.length > PYTH_HIST_MAX) arr.splice(0, arr.length - PYTH_HIST_MAX);
}
setInterval(() => {
  const cut = Date.now() - PYTH_HIST_MAX_AGE_MS;
  for (const arr of pythHist.values()) {
    let drop = 0;
    while (drop < arr.length && arr[drop][0] < cut) drop++;
    if (drop) arr.splice(0, drop);
  }
}, 5 * 60_000).unref();
function bookSend(obj) {
  try { if (live.ws && live.ws.readyState === 1) live.ws.send(JSON.stringify(obj)); } catch {}
}
function ensureBook(symbol) {
  const now = Date.now();
  const had = books.subs.has(symbol);
  books.subs.set(symbol, now);
  if (!had) bookSend({ type: 'subscribe', subscription: { channel: 'orderbook', symbol } });
}
function dropBook(symbol) {
  books.subs.delete(symbol);
  books.map.delete(symbol);
  bookSend({ type: 'unsubscribe', subscription: { channel: 'orderbook', symbol } });
}
function freshBook(symbol) {
  const b = books.map.get(symbol);
  return b && Date.now() - b.ts < BOOK_FRESH_MS ? b : null;
}
function awaitBook(symbol, timeoutMs = BOOK_AWAIT_MS) {
  ensureBook(symbol);
  return new Promise((resolve) => {
    const t0 = Date.now();
    const poll = () => {
      const b = freshBook(symbol);
      if (b) return resolve(b);
      if (Date.now() - t0 > timeoutMs) return resolve(null);
      setTimeout(poll, 100);
    };
    poll();
  });
}
function startWs() {
  if (!WebSocket || live.started) return;
  live.started = true;
  let backoffMs = 1000;
  const connect = () => {
    let ws;
    try { ws = new WebSocket(WS_URL); } catch (e) { return setTimeout(connect, backoffMs = Math.min(backoffMs * 2, 30_000)); }
    live.ws = ws;
    ws.on('open', () => {
      backoffMs = 1000;
      _log('price ws connected');
      try {
        ws.send(JSON.stringify({ type: 'subscribe', subscription: { channel: 'marketStats' } }));
        // subscribe-all trades: sub-second REAL prints straight from the venue
        // (verified multi-sub on one connection works); the warehouse poll
        // stays as the fallback print source
        ws.send(JSON.stringify({ type: 'subscribe', subscription: { channel: 'trades' } }));
        // re-arm any active book subscriptions across a reconnect
        for (const sym of books.subs.keys()) ws.send(JSON.stringify({ type: 'subscribe', subscription: { channel: 'orderbook', symbol: sym } }));
      } catch {}
    });
    ws.on('message', (data) => {
      let msg;
      try { msg = JSON.parse(data.toString()); } catch { return; }
      if (msg?.channel === 'orderbook' && msg.symbol && msg.orderbook) {
        if (books.subs.has(msg.symbol)) {
          books.map.set(msg.symbol, {
            bids: msg.orderbook.bids || [],
            asks: msg.orderbook.asks || [],
            mid: Number(msg.orderbook.mid),
            ts: Date.now(),
          });
        }
        return;
      }
      if (msg?.channel === 'trades' && msg.symbol && Array.isArray(msg.trades)) {
        for (const t of msg.trades) {
          const base = Number(t.baseAmount), quote = Number(t.quoteAmount);
          if (!(base > 0) || !(quote > 0)) continue;
          const ts = (Number(t.timestamp) || 0) * 1000 || Date.now();
          ingestPrint(msg.symbol, quote / base, ts, `ws:${msg.symbol}:${t.tradeSequenceNumber}`, base);
          tapePush(msg.symbol, { ts, side: t.side === 'bid' ? 'buy' : 'sell', price: quote / base, notional: quote, seq: Number(t.tradeSequenceNumber) || 0 });
        }
        return;
      }
      if (msg?.channel !== 'marketStats' || !msg.symbol) return;
      live.lastMsgMs = Date.now();
      // preserve the pyth overlay fields — this handler must never wipe them —
      // and refresh the basis EMA (venue level minus raw pyth) on each stat
      const prev = live.map.get(msg.symbol);
      let pythBasis = prev ? prev.pythBasis : null;
      if (prev && prev.pythAtMs && Date.now() - prev.pythAtMs < 3_000 && Number(prev.pythPrice) > 0) {
        const b = Number(msg.markPrice) - Number(prev.pythPrice);
        if (Number.isFinite(b)) pythBasis = pythBasis == null ? b : pythBasis * 0.9 + b * 0.1;
      }
      live.map.set(msg.symbol, {
        symbol: msg.symbol,
        markPrice: Number(msg.markPrice),
        oraclePrice: Number(msg.oraclePrice),
        currentFundingRate: Number(msg.currentFundingRate),   // PERCENT per hour
        eightHourFundingRate: Number(msg.eightHourFundingRate),
        lastUpdatedMs: Date.now(),
        ...(prev && prev.pythAtMs ? { pythPrice: prev.pythPrice, pythAtMs: prev.pythAtMs } : {}),
        ...(pythBasis != null ? { pythBasis } : {}),
        ...(prev && prev.pythPubTime ? { pythPubTime: prev.pythPubTime } : {}),
        ...(prev && prev.indexHalt ? { indexHalt: prev.indexHalt } : {}),
      });
      // per-tick risk evaluation for this symbol (throttled internally)
      try { tickEval(msg.symbol); } catch {}
    });
    ws.on('error', (e) => { _log('price ws error: ' + e.message); });
    ws.on('close', () => {
      _log(`price ws closed, reconnecting in ${backoffMs}ms`);
      live.ws = null;
      setTimeout(connect, backoffMs);
      backoffMs = Math.min(backoffMs * 2, 30_000);
    });
  };
  connect();
}

// Walk L2 levels for a taker fill of sizeBase; optional capPrice bounds how
// deep a marketable limit may take. Returns filled base + VWAP.
function walkBook(levels, sizeBase, capPrice = null, isBuy = true) {
  let remaining = sizeBase, cost = 0, filled = 0;
  for (const lvl of levels || []) {
    const p = Number(lvl[0]), s = Number(lvl[1]);
    if (!(p > 0) || !(s > 0)) continue;
    if (capPrice != null && (isBuy ? p > capPrice : p < capPrice)) break;
    const take = Math.min(remaining, s);
    cost += take * p;
    filled += take;
    remaining -= take;
    if (remaining <= 1e-12) break;
  }
  return filled > 0 ? { filledBase: filled, vwap: cost / filled } : { filledBase: 0, vwap: null };
}
// Book execution for entries (IOC semantics): walk within the slippage
// collar around mark. Returns null when no fresh book (caller falls back to
// the empirical impact model).
function bookExec(symbol, orderSide, sizeBase, mark, capPrice = null) {
  const b = freshBook(symbol);
  if (!b) return null;
  const isBuy = orderSide === 'BUY';
  const levels = isBuy ? b.asks : b.bids;
  const collar = isBuy ? mark * (1 + MAX_SLIP_PCT / 100) : mark * (1 - MAX_SLIP_PCT / 100);
  const cap = capPrice != null ? (isBuy ? Math.min(capPrice, collar) : Math.max(capPrice, collar)) : collar;
  const w = walkBook(levels, sizeBase, cap, isBuy);
  return { ...w, source: 'book' };
}
// Book execution for closes/SL/TP/liquidations: must ALWAYS fully fill —
// walked portion at book VWAP, any remainder priced at the collar bound
// (the worst price a 1%-tolerance taker would have accepted).
// stage rule: heat fills execute AT the oracle mark — the chart, the mark
// and the fill are one price, so entry uPnL starts at zero. Standard mode
// keeps real book-walk execution (venue basis and slippage are the realism).
function heatOf(userId) { const a = stmt.acctGet.get(userId); return (a && Number(a.heat)) || 0; }   // 0 std, 1 stage, 2 scaled-std
const isStage = (h) => Number(h) === 1;
const isScaled = (h) => Number(h) >= 1;
function execPxFor(userId, symbol, orderSide, sizeBase, mark) {
  if (isStage(heatOf(userId))) return { px: rpx(mark), source: 'mark' };
  return bookExecFull(symbol, orderSide, sizeBase, mark);
}
function bookExecFull(symbol, orderSide, sizeBase, mark) {
  const r = bookExec(symbol, orderSide, sizeBase, mark);
  if (!r) return { px: takerPx(mark, orderSide, sizeBase * mark), source: 'model' };
  if (r.filledBase >= sizeBase - 1e-12) return { px: rpx(r.vwap), source: 'book' };
  const isBuy = orderSide === 'BUY';
  const collarPx = isBuy ? mark * (1 + MAX_SLIP_PCT / 100) : mark * (1 - MAX_SLIP_PCT / 100);
  const rem = sizeBase - r.filledBase;
  const blended = ((r.vwap || collarPx) * r.filledBase + collarPx * rem) / sizeBase;
  return { px: rpx(blended), source: 'book+collar' };
}

// ── real on-chain prints ────────────────────────────────────────────────
// Primary: sub-second trades from the Phoenix WS (subscribe-all, above).
// Fallback/backfill: the geyser warehouse /perps/live-feed poll (~2-4s lag).
// Both feed the same per-symbol store; duplicate prints across sources are
// harmless (printThrough is a boolean cross check).
const prints = { map: new Map(), seen: new Map(), lastOkMs: 0 };   // symbol -> [{ts, price, size}]
function ingestPrint(symbol, price, ts, key, size = 0) {
  if (!(price > 0)) return;
  const now = Date.now();
  if (prints.seen.has(key)) return;
  prints.seen.set(key, now);
  prints.lastOkMs = now;
  let arr = prints.map.get(symbol);
  if (!arr) prints.map.set(symbol, arr = []);
  arr.push({ ts, price, size: Number(size) > 0 ? Number(size) : 0 });
  if (arr.length > 400) arr.splice(0, arr.length - 400);
  if (prints.seen.size > 8000) {
    // insertion order is time order: stop at the first young entry
    for (const [k, t] of prints.seen) {
      if (now - t <= 10 * 60_000) break;
      prints.seen.delete(k);
    }
  }
}
async function refreshPrints() {
  if (!_warehouseGet) return;
  try {
    const body = await _warehouseGet('/perps/live-feed?limit=100', 4000);
    if (!body) return;
    const j = JSON.parse(body);
    const now = Date.now();
    prints.lastOkMs = now;
    for (const r of j.rows || []) {
      const price = Number(r.price), ts = Number(r.ts) || now;
      if (!(price > 0) || !r.symbol) continue;
      // warehouse rows carry notional, not base size: derive base = notional/price
      const base = Number(r.notional) > 0 ? Number(r.notional) / price : 0;
      ingestPrint(r.symbol, price, ts, `wh:${r.signature || ''}:${r.symbol}:${price}:${r.notional}`, base);
    }
    const cut = now - PRINT_WINDOW_MS;
    for (const [sym, arr] of prints.map) {
      const kept = arr.filter((p) => p.ts >= cut);
      if (kept.length) prints.map.set(sym, kept); else prints.map.delete(sym);
    }
  } catch {}
}
// A real trade printed through the limit price since `sinceTs`?
function printThrough(symbol, side, price, sinceTs) {
  const arr = prints.map.get(symbol);
  if (!arr) return false;
  for (const p of arr) {
    if (p.ts < sinceTs) continue;
    if (side === 'BUY' ? p.price <= price : p.price >= price) return true;
  }
  return false;
}
// Base-size volume that printed THROUGH a resting limit's price strictly after
// `afterTs` (the order's consumption watermark). A resting limit can fill only
// as much as real counterparty volume actually traded through it — this is what
// caps a resting limit to realistic partial fills instead of granting the full
// size on a single wick print.
function printedVolumeThrough(symbol, side, price, afterTs) {
  const arr = prints.map.get(symbol);
  if (!arr) return 0;
  let v = 0;
  for (const p of arr) {
    if (p.ts <= afterTs) continue;
    if (side === 'BUY' ? p.price <= price : p.price >= price) v += p.size;
  }
  return v;
}
function tapeActive(symbol) {
  // lastOkMs is refreshed by ANY ingest (WS trades or warehouse rows): the
  // gate measures "prints are flowing from somewhere", not warehouse health
  if (Date.now() - prints.lastOkMs > 30_000) return false;
  const arr = prints.map.get(symbol);
  return !!(arr && arr.length && Date.now() - arr[arr.length - 1].ts < PRINT_ACTIVE_MS);
}

// Hermes SSE consumer: merges unrounded oracle prices into live.map and
// fires tickEval per update, so liquidation granularity == oracle granularity.
const PYTH_FEEDS = {
  BTC: 'e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43',
  ETH: 'ff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace',
  SOL: 'ef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d',
};
// ── stage index universe (phase 1 expansion, 2026-08-08) ─────────────────
// Every symbol here was LISTING-PROBED on Binance (USDT+USDC both exist) and
// Coinbase (SYM-USD), and PRICE-VALIDATED against the Phoenix mark (<10bps)
// to rule out ticker collisions. 3 components each = full guard grade.
// Symbols NOT in this set stay stage-tradable but leverage-capped (see
// STAGE_UNINDEXED_LEV_CAP): their stage mark falls back to the single-source
// venue mark, which cannot back 1000x honestly.
const INDEX_SYMBOLS = ['BTC', 'ETH', 'SOL', 'AAVE', 'ADA', 'BNB', 'CHIP', 'DOGE', 'ENA', 'FET', 'JTO', 'MEGA', 'MET', 'MORPHO', 'NEAR', 'ONDO', 'PUMP', 'RENDER', 'SUI', 'VIRTUAL', 'WLD', 'XLM', 'XPL', 'XRP'];
const PYTH_BY_ID = Object.fromEntries(Object.entries(PYTH_FEEDS).map(([sym, id]) => [id, sym]));
// ── index source selection ───────────────────────────────────────────────
// 'binance' (default): Binance spot bookTicker mids — free, ~100 upd/s on
// BTC, no deprecation risk. Pyth Core died 2026-07-31 (paid-only now; the
// legacy Hermes grace endpoint pauses 3s every ~7.5s). 'pyth' kept intact
// as an instant-rollback path (INDEX_SOURCE=pyth in the unit env).
const INDEX_SOURCE = (process.env.INDEX_SOURCE || 'binance').toLowerCase();
const BINANCE_STREAMS = Object.fromEntries(INDEX_SYMBOLS.map((s2) => [s2, s2.toLowerCase() + 'usdt']));
// USDC leg only where the book is deep. On thin alts the USDC pair keeps
// emitting size updates at a stale quote, becomes the lone outlier, and was
// the cause of nearly every divergence halt (measured 2026-08-22). Majors
// keep the third source; alts run binance-usdt + coinbase.
const BINANCE_USDC_DEEP = new Set(['BTC', 'ETH', 'SOL']);
const BINANCE_USDC_STREAMS = Object.fromEntries(INDEX_SYMBOLS.filter((s2) => BINANCE_USDC_DEEP.has(s2)).map((s2) => [s2, s2.toLowerCase() + 'usdc']));
const GUARD_PRODUCTS = Object.fromEntries(INDEX_SYMBOLS.map((s2) => [s2 + '-USD', s2]));
const STAGE_INDEXED = new Set(INDEX_SYMBOLS);
const STAGE_UNINDEXED_LEV_CAP = 100;   // single-source venue-mark fallback cannot back 1000x
// Noise-tiered stage caps (measured from phoenix.index_ticks p99 single-tick
// moves, 2026-08-08): max leverage keeps the liquidation distance at least
// ~4x the symbol's p99 tick noise. Majors (p99 < 4bps) carry 1000x; mids
// (4-5bps) 500x; small caps (5-9bps) 250x; MET printed 84bps p99 (thin book,
// often single-component) and gets 50x. Re-measure with the query in memory
// once 24h+ of ticks exist; this table is intentionally conservative.
// Recalibrated 2026-08-08 against the REAL 1000x liquidation distance (5bps:
// maintBps/1e4/L = 0.5/1000, initial 1/L=10bps, liq loss = 0.5/L = 5bps) using
// ~9k+ persisted ticks/symbol. Rule: liq distance at the cap must exceed the
// symbol's p99.9 single-tick move (a routine tick can't bust a fresh position);
// 1000x reserved for symbols whose MAX observed single tick stays under 5bps.
// SUPERSEDED 2026-08-10: that tiering was never applied (everything went to
// 500) and its premise does not hold on a larger sample -- BTC's max tick is
// 8.4bps with 22 breaches, BNB's 22.2bps with 293, not the '<5bps, 0 breaches'
// claimed. Current caps are the owner call recorded on the table below.
// 2026-08-08 (owner call): uniform 500x across the six majors — one simple
// tier, liq distance 10bps clears every major's p99.9 tick (SOL/SUI max
// single-ticks ~11-15bps remain the known rare-bust tail, ~1 event/3h,
// auditable via index_ticks). NOTE: BOOST_ARM_LEV=1000 is now unreachable —
// Boost is dormant until its future is decided (retire, or make 1000x
// boost-exclusive on BTC/BNB).
const STAGE_LEV_CAPS = {
  // Tiered by measured index noise. Liquidation distance at 1000x is 5bps, so
  // the cap is set where a routine tick cannot casually bust a fresh position.
  // Breaches/hr of that 5bps distance, 48h sample (~2M ticks):
  //   BTC 0.4 | ETH 4.4 | BNB 4.5 | XRP 10.6 | SOL 11.1        -> 1000x
  //   SUI 30 | DOGE 29 | AAVE 42 | ADA 45 | ENA 59 | NEAR 78
  //   ONDO 135 | VIRTUAL 68 | RENDER 64                        ->  500x
  //   XLM 56 | WLD 113 | CHIP 114 | XPL 115 | MEGA 131
  //   MORPHO 95 | FET 74 | JTO 154 | PUMP 194                  ->  250x
  //   MET 143, and only 1.54 index components (single-source 46%
  //   of the time) with a 23bps p99 tick                       ->   50x
  // Briefly ran everything at 1000x (2026-08-10) and reverted: below the five
  // majors it stops being a trade and becomes a countdown -- PUMP at 194/hr
  // gives a max-leverage position about 19 seconds.
  BTC: 1000, BNB: 1000,
  ETH: 1000, XRP: 1000,
  SOL: 1000, SUI: 500,
  DOGE: 500, ADA: 500, NEAR: 500, ENA: 500, ONDO: 500, VIRTUAL: 500, RENDER: 500, AAVE: 500,
  JTO: 250, MORPHO: 250, PUMP: 250, XLM: 250, WLD: 250, CHIP: 250, XPL: 250, MEGA: 250, FET: 250,
  MET: 50,
};
// ── competition aliases ──────────────────────────────────────────────────
// A synthetic ticker trading a base symbol's index under its OWN position
// row: SOL-HOT for the Hot Market segment, BTC-BOOST for the Boost window.
// Separating event exposure by ticker, rather than tagging lots inside one
// blended position, means the event trade has its own entry, size and PnL to
// put on the wall, the segment can close only what it opened, and none of the
// scoring needs lot accounting. Everything downstream (marks, config, lots,
// leverage caps, margin) resolves an alias to its base, so an alias behaves
// exactly like its underlying market and cannot drift away from it.
const ALIAS_RE = /^([A-Z0-9]+)-(HOT|BOOST)$/;
const baseOf = (sym) => { const m = ALIAS_RE.exec(sym); return m ? m[1] : sym; };
const aliasKind = (sym) => { const m = ALIAS_RE.exec(sym); return m ? m[2] : null; };
// Which aliases are tradable right now. Driven by the round clock and the
// operator, never by the client, and empty outside an event — so these
// tickers simply do not exist during ordinary paper trading.
const openAliases = new Map();   // alias -> { openedAt, roundId }
const aliasOpen = (sym) => openAliases.has(sym);
const stageLevCap = (sym) => {
  const b = baseOf(sym);
  return STAGE_INDEXED.has(b) ? (STAGE_LEV_CAPS[b] || 250) : STAGE_UNINDEXED_LEV_CAP;
};
// COMPOSITE index: equal-weight mean of Binance-USDT micro, Binance-USDC
// micro and Coinbase-USD mid. The three levels sit dollars apart and their
// steps land at different moments, so the composite breathes tick-to-tick
// the way an aggregate oracle does (a single venue's book is honest but
// visually flat between steps). Quote diversity also dilutes a stablecoin
// depeg to 1/3 before the tripwire even fires.
const GUARD_DIVERGENCE = 0.0025;       // 25 bps max pairwise disagreement (default)
// Sub-$1 assets: venue price grids are coarse relative to price — one tick on
// one venue can exceed 25bps, so the uniform band reads GRID STEPS as
// disagreement (all 4 halts on day one were MET/CHIP quantization, and every
// false halt traps open positions). Bands scale with measured noise, like the
// leverage caps do — protection stays proportional: e.g. MET at 50x has a
// 200bps liquidation distance, so a 150bps band still guards dislocation.
const GUARD_BAND = { MET: 0.015, CHIP: 0.0075, MEGA: 0.0075, XPL: 0.0075, PUMP: 0.0075, FET: 0.005, WLD: 0.005, MORPHO: 0.005, JTO: 0.005 };
const guardBandOf = (sym) => GUARD_BAND[sym] || GUARD_DIVERGENCE;
const GUARD_TRIP_MS = 2_000;           // sustained before halting
const GUARD_CLEAR_MS = 5_000;          // sustained agreement before resuming
const COMP_FRESH_MS = 5_000;           // stale components drop out of the mean
// Above this pairwise spread we stop trusting a mean and fall back to the
// median.
//
// Measured 2026-08-10: the normal spread is 8-10bps and it is STRUCTURAL, not
// disagreement -- BTC/USDT, BTC/USDC and BTC/USD are different instruments and
// the stablecoin basis sits between them. All three legs are healthy. A first
// attempt at 3bps meant the mean never engaged at all.
//
// Because the basis is structural, the median is actively worse here: it
// tracks whichever leg is in the middle and JUMPS by the basis whenever the
// ordering flips, manufacturing ticks that are not market moves. The mean has
// no such artefact.
//
// 20bps sits above normal (8-12) and below the 25bps divergence halt, so a leg
// that genuinely dislocates drops us to the median first and halts if it keeps
// going. Re-check against the spreadBps column before changing it.
const MEAN_MAX_SPREAD_BPS = Number(process.env.INDEX_MEAN_MAX_SPREAD_BPS || 20);
const _meanFallback = new Map();       // sym -> recent fallback timestamps
const _idxComps = new Map();           // sym -> { usdt:{px,ts}, usdc:{px,ts}, usd:{px,ts} }
const _halt = new Map();               // sym -> { divergeSince, okSince, halted }
// disagreement tripwire across FRESH components: (max−min)/mean over the
// threshold sustained → halt. One lone component can't disagree with itself,
// so a single-source index never halts (availability wins).
// Component prices at 2dp made every halt message self-contradicting: a 25bps
// spread on a $1.30 asset is $0.003, which rounds away entirely, so the alert
// read "components disagree 1.32 / 1.32 / 1.32". Show enough decimals for the
// disagreement to be visible, and lead with the number that actually decided
// it.
function describeSpread(prices, spreadBps, band) {
  const lo = Math.min(...prices);
  const dp = Math.min(8, Math.max(2, Math.ceil(Math.log10(1 / (lo * 0.00005)))));
  return `spread ${spreadBps.toFixed(1)}bps over ${(band * 1e4).toFixed(0)}bps band `
    + `[${prices.map((p) => p.toFixed(dp)).join(' / ')}]`;
}
const GUARD_PAGE_MS = Number(process.env.INDEX_GUARD_PAGE_MS || 60_000);
function guardCheck(sym, prices, now) {
  const st = _halt.get(sym) || { divergeSince: 0, okSince: 0, halted: false };
  if (prices.length < 2) {
    if (st.halted) { st.halted = false; _log(`index guard ${sym}: single source, halt lifted`); }
    st.divergeSince = 0; _halt.set(sym, st);
    return st.halted;
  }
  const mean = prices.reduce((a, b) => a + b, 0) / prices.length;
  const band = guardBandOf(sym);
  // With 3+ sources, ONE bad component must not freeze the market (measured
  // 2026-08-22: every halt on record was a lone outlier, usually the thin
  // Binance USDC book, while the other two agreed within 6bps). Halt on the
  // CLOSEST pair: the index is only unknowable when even the two components
  // that agree best still disagree. With 2 sources, max-min is the pair.
  const sorted = [...prices].sort((a, b) => a - b);
  let gap = sorted[sorted.length - 1] - sorted[0];
  if (sorted.length >= 3) {
    gap = Infinity;
    for (let i = 1; i < sorted.length; i++) {
      gap = Math.min(gap, sorted[i] - sorted[i - 1]);
    }
  }
  const spreadBps = (gap / mean) * 1e4;
  const diverged = spreadBps > band * 1e4;
  if (diverged) {
    st.okSince = 0;
    if (!st.divergeSince) st.divergeSince = now;
    if (!st.halted && now - st.divergeSince >= GUARD_TRIP_MS) {
      st.halted = true;
      st.haltedAt = now;
      st.paged = false;
      // Keep the numbers that DECIDED the halt: the 60s+ page fires later,
      // when components may have already re-converged (a page once read
      // "4.1bps over 25bps band" -- the trigger was 29.4bps).
      st.tripDesc = describeSpread(prices, spreadBps, band);
      _log(`INDEX HALT ${sym}: ${st.tripDesc}`);
    }
  } else {
    st.divergeSince = 0;
    if (st.halted) {
      if (!st.okSince) st.okSince = now;
      if (now - st.okSince >= GUARD_CLEAR_MS) {
        const heldMs = now - (st.haltedAt || now);
        st.halted = false; st.okSince = 0;
        _log(`index resume ${sym}: components re-agree after ${(heldMs / 1000).toFixed(1)}s`);
        // Only worth a DM if we paged for the halt in the first place.
        if (st.paged) pageResume(sym, Math.round(heldMs / 1000));
        st.paged = false;
      }
    }
  }
  // Page only on a halt that OUTLASTS the self-healing window. Measured over 3
  // days: 14 halts, every single one cleared in ~6s (the minimum the code
  // allows), so every page so far was for a blip that had already fixed itself
  // before the phone buzzed. A sustained halt freezes the market and still
  // pages. Blips stay in the log.
  if (st.halted && !st.paged && now - (st.haltedAt || now) >= GUARD_PAGE_MS) {
    st.paged = true;
    pageHalt(sym, `INDEX HALT ${sym} held ${(GUARD_PAGE_MS / 1000).toFixed(0)}s+ (market frozen): tripped at ${st.tripDesc || describeSpread(prices, spreadBps, band)}; now ${prices.length} live component(s), ${spreadBps.toFixed(1)}bps`);
  }
  _halt.set(sym, st);
  return st.halted;
}
function compUpdate(sym, key, px, now) {
  let c = _idxComps.get(sym);
  if (!c) { c = {}; _idxComps.set(sym, c); }
  c[key] = { px, ts: now };
  const fresh = Object.values(c).filter((x) => now - x.ts < COMP_FRESH_MS).map((x) => x.px);
  if (!fresh.length) return;
  // MEAN when the components agree, MEDIAN when they don't.
  //
  // A pure median is safe but STICKY: it is whichever source sits in the
  // middle, so when that is the slow one (Binance-USDC or Coinbase, vs
  // Binance-USDT at ~100 upd/s) the index inherits the slow source's update
  // rate and discards the fast one entirely. Measured 2026-08-10: BTC emitted
  // ~375 ticks/min but visited only 14-34 distinct cent-prices — the chart
  // stepped in 2-3s plateaus instead of breathing.
  //
  // A pure mean is live but lets ONE bad source drag the index by a third of
  // its error, which at 1000x (5bps liq distance) is fatal well below the
  // 25bps divergence guard. So: mean only while the spread is tight enough
  // that a third of it cannot matter; median the moment it is not.
  const srt = [...fresh].sort((x, y) => x - y);
  const median = srt.length % 2 ? srt[(srt.length - 1) / 2] : (srt[srt.length / 2 - 1] + srt[srt.length / 2]) / 2;
  let idx = median, spreadBps = 0;
  if (fresh.length >= 2) {
    const lo = srt[0], hi = srt[srt.length - 1];
    spreadBps = lo > 0 ? ((hi - lo) / lo) * 1e4 : 0;
    if (spreadBps <= MEAN_MAX_SPREAD_BPS) {
      idx = fresh.reduce((a, b) => a + b, 0) / fresh.length;
    } else {
      const arr = (_meanFallback.get(sym) || []).filter((t) => now - t < 5 * 60_000);
      arr.push(now); _meanFallback.set(sym, arr);
      if (arr.length === 50) _log(`index mean->median ${sym}: spread ${spreadBps.toFixed(2)}bps over ${MEAN_MAX_SPREAD_BPS}bps (50 ticks in 5min)`);
    }
  }
  ingestIndexTick(sym, idx, now, fresh, spreadBps);
}
// Degraded-state jump clamp: with fewer than 3 fresh components the median
// can't outvote a bad source, so a single-tick move over 50bps is HELD (tick
// dropped, logged) rather than priced. Freeze-don't-guess: a real flash move
// re-asserts itself on the next agreeing tick.
// ── ops alerts (Telegram, per-key 30min cooldown) ────────────────────────
// Pages on the states the audit found log-only: guard halts, clamp storms,
// persistence failure, and a 1000x-tier symbol riding a single component.
const _alertLast = new Map();
function tgOps(key, msg) {
  const now = Date.now();
  if (now - (_alertLast.get(key) || 0) < 30 * 60_000) return;
  _alertLast.set(key, now);
  // OPS chat ONLY. TG_CHAT_ID is the PUBLIC feed channel — an ops alert
  // landed there once (2026-08-08, deleted). Never fall back to it: with no
  // ops chat configured, alerts stay in the logs.
  const tok = process.env.TG_BOT_TOKEN, chat = process.env.OPS_ALERT_CHAT_ID;
  if (!tok || !chat) return;
  const body = JSON.stringify({ chat_id: chat, text: '[paper] ' + msg });
  const req = require('https').request({ host: 'api.telegram.org', path: `/bot${tok}/sendMessage`, method: 'POST', headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) }, timeout: 8000 }, (r) => r.resume());
  req.on('error', () => {});
  req.end(body);
}
// Storm batching: a volatility flush trips many markets within seconds and
// used to send a dozen DMs for one event. Collect pages briefly; three or
// more together become one summary (per-market detail stays in the log).
const _pendHalts = [];
let _pendHaltTimer = null;
function pageHalt(sym, msg) {
  _pendHalts.push({ msg, sym });
  if (_pendHaltTimer) return;
  _pendHaltTimer = setTimeout(() => {
    const batch = _pendHalts.splice(0);
    _pendHaltTimer = null;
    if (batch.length >= 3) {
      tgOps('halt-storm', `index storm: ${batch.length} markets frozen on component divergence (${batch.map((b) => b.sym).join(', ')}); per-market detail in logs`);
    } else {
      for (const b of batch) tgOps('halt:' + b.sym, b.msg);
    }
  }, 20_000);
}
const _pendResumes = [];
let _pendResumeTimer = null;
function pageResume(sym, heldS) {
  _pendResumes.push({ heldS, sym });
  if (_pendResumeTimer) return;
  _pendResumeTimer = setTimeout(() => {
    const batch = _pendResumes.splice(0);
    _pendResumeTimer = null;
    if (batch.length >= 3) {
      const lo = Math.min(...batch.map((b) => b.heldS));
      const hi = Math.max(...batch.map((b) => b.heldS));
      tgOps('resume-storm', `index storm over: ${batch.length} markets resumed, held ${lo}-${hi}s (${batch.map((b) => b.sym).join(', ')})`);
    } else {
      for (const b of batch) tgOps('halt-clear:' + b.sym, `INDEX RESUME ${b.sym}: components re-agree, held ${b.heldS}s`);
    }
  }, 30_000);
}
// A component stream that reconnect-loops under burst load goes stale
// between connects and shows up as "divergence" -- name the real cause.
const _bnbReconnects = [];
const _clampCount = new Map();   // sym -> [ts,...] recent clamps
const _clampAccept = new Map();  // sym -> {n, px} self-agreeing held-tick streak
const _monoSince = new Map();    // sym -> ts since single-component (1000x tier only)
const CLAMP_JUMP = 0.005;
const _lastIdx = new Map();   // sym -> last accepted px
// ClickHouse audit trail: every accepted index tick, batched via the tunnel
// to the warehouse (phoenix.index_ticks, 30d TTL). Fail-open: buffer capped,
// errors dropped — persistence must never stall pricing.
const _tickBuf = [];
let _persistFails = 0;
const _persistLast = new Map();   // own throttle: the SSE gate map only advances when clients are connected
function persistTick(sym, px, now, nComps, spreadBps) {
  if (_tickBuf.length >= 4000) return;
  _tickBuf.push({ s: sym, p: px, t: now, n: nComps, sp: Number(spreadBps) || 0 });
}
setInterval(() => {
  if (!_tickBuf.length) return;
  const rows = _tickBuf.splice(0, _tickBuf.length);
  const tok = process.env.WAREHOUSE_API_TOKEN || '';
  if (!tok) return;
  const body = JSON.stringify({ rows });
  const req = require('http').request({ host: '127.0.0.1', port: 9100, path: '/internal/index-ticks', method: 'POST', headers: { 'content-type': 'application/json', authorization: 'Bearer ' + tok, 'content-length': Buffer.byteLength(body) }, timeout: 5000 }, (res) => { res.resume(); _persistFails = 0; });
  req.on('error', () => { if (++_persistFails >= 5) tgOps('persist', `index tick persistence failing (${_persistFails} consecutive batches dropped) — audit trail has a gap`); });
  req.end(body);
}, 5_000).unref();

function ingestIndexTick(sym, px, now, comps, spreadBps) {
  const nComps = (comps || []).length;
  if (nComps < 3) {
    let prev = _lastIdx.get(sym);
    if (!(prev > 0)) {
      // post-restart cold start: no reference yet — seed from the venue mark
      // (sits ~10bps off the index, well inside the 50bps clamp) so the first
      // degraded-state tick is not accepted blind
      const m0 = live.map.get(sym) || (snapFile()?.markets || {})[sym];
      if (m0 && Number(m0.markPrice) > 0) prev = Number(m0.markPrice);
    }
    if (prev > 0 && Math.abs(px - prev) / prev > CLAMP_JUMP) {
      // Escape hatch: freeze-don't-guess deadlocks after a REAL move — the
      // reference stays at the old level and every honest tick is held
      // forever (PUMP: 2,341 held ticks at a frozen ref, 2026-08-22). A
      // genuine move re-asserts as held ticks that agree with EACH OTHER;
      // 10 in a row within 30bps = accept the new level. A glitching feed
      // scatters and never builds the streak.
      const acc = _clampAccept.get(sym);
      if (acc && Math.abs(px - acc.px) / acc.px < 0.003) {
        acc.n += 1; acc.px = px;
      } else {
        _clampAccept.set(sym, { n: 1, px });
      }
      if ((_clampAccept.get(sym)?.n || 0) >= 10) {
        _clampAccept.delete(sym);
        _log(`index step accepted ${sym}: ${prev.toFixed(6)} -> ${px.toFixed(6)} after 10 self-agreeing held ticks`);
      } else {
        _log(`index clamp ${sym}: ${prev.toFixed(6)} -> ${px.toFixed(6)} on ${nComps} component(s), tick held`);
        const arr = (_clampCount.get(sym) || []).filter((t) => now - t < 5 * 60_000); arr.push(now); _clampCount.set(sym, arr);
        if (arr.length >= 5) tgOps('clamp:' + sym, `index clamp storm ${sym}: ${arr.length} held ticks in 5min (degraded components misbehaving)`);
        return;
      }
    } else {
      _clampAccept.delete(sym);
    }
  }
  _lastIdx.set(sym, px);
  if ((STAGE_LEV_CAPS[sym] || 0) >= 1000) {
    if (nComps <= 1) {
      if (!_monoSince.has(sym)) _monoSince.set(sym, now);
      else if (now - _monoSince.get(sym) > 5 * 60_000) tgOps('mono:' + sym, `1000x symbol ${sym} riding a SINGLE index component for 5min+ (no cross-check)`);
    } else _monoSince.delete(sym);
  }
  const halted = guardCheck(sym, comps || [px], now);
  const cur = live.map.get(sym) || { symbol: sym };
  if (halted) {
    if (!cur.indexHalt) live.map.set(sym, { ...cur, indexHalt: true });
    return;                                          // frozen: no price, no eval
  }
  live.map.set(sym, { ...cur, indexHalt: false, markPrice: Number(cur.markPrice) > 0 ? cur.markPrice : px, pythPrice: px, pythAtMs: now, lastUpdatedMs: now, pythBasis: cur.pythBasis != null ? cur.pythBasis : (Number(cur.markPrice) > 0 ? null : 0) });
  live.lastMsgMs = now;
  pythHistPush(sym, now, px);
  if (now - (_persistLast.get(sym) || 0) >= SSE_MIN_GAP_MS) { _persistLast.set(sym, now); persistTick(sym, px, now, nComps, spreadBps); }
  pythSseBroadcast(sym, px, now);
  try { tickEval(sym); } catch {}
}
const micro = (b, a, bq, aq) => (bq > 0 && aq > 0 ? (b * aq + a * bq) / (aq + bq) : (b + a) / 2);
function startBinanceIndexStream() {
  const WebSocket = require('ws');
  // miniTicker (1s cadence) instead of bookTicker (every book change): the
  // t3.small cannot drain the bookTicker firehose when credit-starved — the
  // event loop lags, binance prices go stale-in-flight, and every burst
  // reads as divergence. 1s last-price is fine for an index that is clamp-
  // guarded and cross-checked against coinbase (2026-08-22, 79% steal).
  // Split cadence by depth: the deep majors are the message firehose, so
  // they ride miniTicker (1s last price; spread <1bp so last ~= mid). Thin
  // symbols keep bookTicker MIDS: their bookTicker traffic is sparse, and
  // 1s last-price bounces bid/ask across a spread wider than the 50bps
  // clamp (PUMP held 1,308 ticks in 5min on last-price, 2026-08-22).
  const MINI_CADENCE = new Set(['BTC', 'ETH', 'SOL', 'DOGE', 'XRP', 'ADA', 'BNB', 'SUI']);
  const suffixOf = (sym) => (MINI_CADENCE.has(sym) ? '@miniTicker' : '@bookTicker');
  const streams = [
    ...Object.entries(BINANCE_STREAMS).map(([k, v]) => v + suffixOf(k)),
    ...Object.entries(BINANCE_USDC_STREAMS).map(([k, v]) => v + suffixOf(k)),
  ].join('/');
  const bySym = {};
  for (const [k, v] of Object.entries(BINANCE_STREAMS)) bySym[v] = [k, 'usdt'];
  for (const [k, v] of Object.entries(BINANCE_USDC_STREAMS)) bySym[v] = [k, 'usdc'];
  let backoff = 1_000;
  const connect = () => {
    const ws = new WebSocket(`wss://stream.binance.com:9443/stream?streams=${streams}`);
    ws.on('open', () => {
      backoff = 1_000;
      _log('binance index stream connected (usdt+usdc): ' + Object.keys(BINANCE_STREAMS).join(','));
      const now = Date.now();
      _bnbReconnects.push(now);
      while (_bnbReconnects.length && now - _bnbReconnects[0] > 10 * 60_000) _bnbReconnects.shift();
      if (_bnbReconnects.length >= 3) {
        tgOps('binance-flap', `binance index stream unstable: ${_bnbReconnects.length} reconnects in 10min, its component can go stale during bursts (expect divergence halts)`);
      }
    });
    ws.on('message', (m) => {
      try {
        const d = JSON.parse(m);
        const stream = String(d.stream || '');
        const hit = bySym[stream.split('@')[0]];
        if (!hit || !d.data) return;
        let px;
        if (stream.endsWith('@miniTicker')) {
          px = Number(d.data.c);            // 1s last price, deep majors only
        } else {
          const b = Number(d.data.b), a = Number(d.data.a);
          if (!(b > 0) || !(a > 0)) return;
          px = micro(b, a, Number(d.data.B), Number(d.data.A));
        }
        if (Number.isFinite(px) && px > 0) compUpdate(hit[0], hit[1], px, Date.now());
      } catch {}
    });
    const retry = () => { try { ws.terminate(); } catch {} setTimeout(connect, backoff); backoff = Math.min(backoff * 2, 30_000); };
    ws.on('close', retry);                            // includes Binance's routine 24h disconnect
    ws.on('error', (e) => { _log('binance index ws error: ' + e.message); });
  };
  connect();
}
function startCoinbaseGuard() {
  const WebSocket = require('ws');
  let backoff = 1_000;
  const connect = () => {
    const ws = new WebSocket('wss://ws-feed.exchange.coinbase.com');
    ws.on('open', () => {
      backoff = 1_000;
      ws.send(JSON.stringify({ type: 'subscribe', product_ids: Object.keys(GUARD_PRODUCTS), channels: ['ticker'] }));
      _log('coinbase component connected');
    });
    ws.on('message', (m) => {
      try {
        const d = JSON.parse(m);
        if (d.type !== 'ticker') return;
        const sym = GUARD_PRODUCTS[d.product_id];
        const mid = (Number(d.best_bid) + Number(d.best_ask)) / 2;
        if (sym && Number.isFinite(mid) && mid > 0) compUpdate(sym, 'usd', mid, Date.now());
      } catch {}
    });
    const retry = () => { try { ws.terminate(); } catch {} setTimeout(connect, backoff); backoff = Math.min(backoff * 2, 30_000); };
    ws.on('close', retry);
    ws.on('error', (e) => { _log('coinbase component ws error: ' + e.message); });
  };
  connect();
}
// boot backfill: 45min of 1m closes so the tick-history window is full from
// the first pageview after a restart (replaces the dying Pyth Benchmarks API)
// Boot-time component validation: 90s after start, every indexed symbol's
// composite must sit within 2% of Phoenix's venue mark. A venue delisting a
// ticker and reusing it for another asset would otherwise silently poison a
// component (found once by hand during the expansion research; automated).
setTimeout(() => {
  const bad = [];
  for (const sym of INDEX_SYMBOLS) {
    const idx = _lastIdx.get(sym);
    const m = mkt(sym);
    const venue = m && Number(m.markPrice);
    if (idx > 0 && venue > 0 && Math.abs(idx - venue) / venue > 0.02) bad.push(`${sym} idx=${idx} venue=${venue}`);
  }
  if (bad.length) { _log('BOOT VALIDATION FAIL: ' + bad.join('; ')); tgOps('bootval', 'index component validation FAILED (possible ticker collision): ' + bad.join('; ')); }
  else _log('boot validation: all ' + INDEX_SYMBOLS.length + ' indexed symbols within 2% of venue marks');
}, 90_000).unref();

function backfillIndexHistory() {
  const https = require('https');
  for (const [sym, pair] of Object.entries(BINANCE_STREAMS)) {
    https.get(`https://api.binance.com/api/v3/klines?symbol=${pair.toUpperCase()}&interval=1m&limit=45`, (res) => {
      let b = '';
      res.on('data', (c) => b += c);
      res.on('end', () => {
        try {
          const rows = JSON.parse(b);
          if (!Array.isArray(rows)) return;
          const arr = pythHist.get(sym) || [];
          const firstLive = arr.length ? arr[0][0] : Infinity;
          const older = rows.map((k) => [Number(k[6]), Number(k[4])]).filter(([t, v]) => v > 0 && t < firstLive - 1000);
          if (older.length) { pythHist.set(sym, [...older, ...arr]); _log(`index history backfilled ${sym}: ${older.length} bars`); }
        } catch {}
      });
    }).on('error', () => {});
  }
}
// TWO independent SSE connections, first-arrival wins. A single connection
// pauses for 3-4s now and then (observed live) — during exactly such a pause
// a user's own browser stream kept printing and a liquidation looked ~3s
// late. Pauses are per-connection; two rarely pause together. Slot dedup via
// publish_time so the second copy of the same update is dropped, not replayed.
const PYTH_STREAM_CONNS = 2;
function startPythStream() {
  const https = require('https');
  const ids = Object.values(PYTH_FEEDS).map((id) => 'ids[]=0x' + id).join('&');
  const ingest = (pr) => {
    const sym = PYTH_BY_ID[String(pr.id || '').replace(/^0x/, '')];
    const v = pr.price ? Number(pr.price.price) * Math.pow(10, Number(pr.price.expo)) : NaN;
    if (!sym || !Number.isFinite(v) || v <= 0) return;
    const pub = Number(pr.price.publish_time) || 0;
    const now = Date.now();
    const cur = live.map.get(sym) || { symbol: sym };
    const curPub = Number(cur.pythPubTime) || 0;
    if (pub && curPub && (pub < curPub || (pub === curPub && v === cur.pythPrice))) return;   // older slot / duplicate
    live.map.set(sym, { ...cur, markPrice: Number(cur.markPrice) > 0 ? cur.markPrice : v, pythPrice: v, pythAtMs: now, pythPubTime: pub || curPub, lastUpdatedMs: now, pythBasis: cur.pythBasis != null ? cur.pythBasis : (Number(cur.markPrice) > 0 ? null : 0) });
    live.lastMsgMs = now;
    pythHistPush(sym, now, v);
    pythSseBroadcast(sym, v, now);
    try { tickEval(sym); } catch {}
  };
  const startConn = (label) => {
    let backoff = 2_000;
    let retryTimer = null;
    const retry = () => {
      if (retryTimer) return;
      retryTimer = setTimeout(() => { retryTimer = null; connect(); }, backoff);
      backoff = Math.min(backoff * 2, 60_000);
      _log(`pyth stream[${label}] reconnecting in ` + backoff + 'ms');
    };
    const connect = () => {
      const req = https.get(`https://hermes.pyth.network/v2/updates/price/stream?${ids}&parsed=true`, { headers: { accept: 'text/event-stream' } }, (res) => {
        if (res.statusCode !== 200) { res.resume(); retry(); return; }
        backoff = 2_000;
        let buf = '';
        res.on('data', (chunk) => {
          buf += chunk.toString('utf8');
          if (buf.length > 1_000_000) buf = buf.slice(-100_000);   // runaway guard
          let idx;
          while ((idx = buf.indexOf('\n')) !== -1) {
            const line = buf.slice(0, idx); buf = buf.slice(idx + 1);
            if (!line.startsWith('data:')) continue;
            try {
              const d = JSON.parse(line.slice(5));
              for (const pr of (d && d.parsed) || []) ingest(pr);
            } catch {}
          }
        });
        res.on('end', retry); res.on('error', retry);
      });
      req.on('error', retry);
    };
    // stagger so the two connections don't share a fate at connect time
    setTimeout(connect, label === 'a' ? 0 : 700);
  };
  for (let i = 0; i < PYTH_STREAM_CONNS; i++) startConn(String.fromCharCode(97 + i));
  _log(`pyth mark stream started ×${PYTH_STREAM_CONNS}: ` + Object.keys(PYTH_FEEDS).join(','));
}

function init({ apiGet, warehouseGet, log, readRateOk } = {}) {
  if (apiGet) _apiGet = apiGet;
  if (warehouseGet) _warehouseGet = warehouseGet;
  if (log) _log = (...a) => log('[paper]', ...a);
  // Hand the competition clock its controls, then pick up any round the
  // engine was running when it restarted: a mid-show reboot must resume the
  // same schedule rather than stall the wall on a phase that never ends.
  comp.wire({ openAlias, closeAlias, scoreUser, log: (m) => _log(m) });
  try { comp.resume(); } catch (e) { _log('competition resume failed: ' + e.message); }
  if (readRateOk) _readRateOk = readRateOk;
  refreshExchange().catch(() => {});
  try {
    if (INDEX_SOURCE === 'pyth') startPythStream();
    else { startBinanceIndexStream(); startCoinbaseGuard(); backfillIndexHistory(); }
    _log('index source: ' + INDEX_SOURCE);
  } catch (e) { _log('index stream failed to start: ' + e.message); }
  setInterval(() => refreshExchange().catch(() => {}), 2 * 3600_000);
  // Fast retry until the map is populated. A transient 502 on the startup
  // fetch (proxy warming up right after a service restart) used to leave the
  // engine running on FALLBACK configs (10x caps, default fees) for up to 2h,
  // silently rejecting orders that are fine under the real tiers.
  const bootRetry = setInterval(() => {
    if (mktCfg.size > 0) { clearInterval(bootRetry); return; }
    refreshExchange().catch(() => {});
  }, 30_000);
  setInterval(() => refreshPrints().catch(() => {}), PRINT_POLL_MS);
  startWs();
}

// ── local helpers ────────────────────────────────────────────────────────
function send(res, code, obj) { res.writeHead(code, { 'content-type': 'application/json' }); res.end(JSON.stringify(obj)); }
function readBody(req) {
  return new Promise((resolve, reject) => {
    let b = '';
    req.on('data', (c) => { b += c; if (b.length > 4096) { req.destroy(); reject(new Error('big')); } });
    req.on('end', () => resolve(b)); req.on('error', reject);
  });
}
async function sessionUser(req) { return await auth.validateSession(auth.parseSessionCookie(req)); }
const r6 = (x) => Math.round(x * 1e6) / 1e6;
const rpx = (x) => (!Number.isFinite(x) || Math.abs(x) >= 1 ? r6(x) : Number(x.toPrecision(9)));

const _writeRate = new Map();
function writeRateOk(userId) {
  const now = Date.now();
  let r = _writeRate.get(userId);
  if (!r || now - r.t > 60_000) { r = { t: now, n: 0 }; _writeRate.set(userId, r); }
  r.n++;
  if (_writeRate.size > 5000) for (const [k, v] of _writeRate) if (now - v.t > 120_000) _writeRate.delete(k);
  return r.n <= WRITES_PER_MIN;
}

// ── market data: WS first, snapshot file fallback ────────────────────────
let _snap = { ts: 0, data: null };
function snapFile() {
  if (Date.now() - _snap.ts < SNAP_TTL_MS) return _snap.data;
  try { _snap = { ts: Date.now(), data: JSON.parse(fs.readFileSync(SNAPSHOT_FILE, 'utf8')) }; }
  catch { _snap = { ts: Date.now(), data: _snap.data }; }
  return _snap.data;
}
function mkt(sym) {
  sym = baseOf(sym);   // an alias prices off its base symbol's index
  const lv = live.map.get(sym);
  if (lv && Date.now() - lv.lastUpdatedMs < WS_FRESH_MS) return lv;
  const s = snapFile();
  const m = s && s.markets && s.markets[sym];
  return m || lv || null;
}
function mktFresh(m) { return !!(m && Number(m.markPrice) > 0 && Date.now() - (m.lastUpdatedMs || 0) < MARKET_FRESH_MS); }
// Pyth-preferred mark: the unrounded oracle Phoenix itself prices from,
// streamed server-side (~2Hz, sub-penny). Stale/absent → venue markPrice.
const PYTH_MARK_FRESH_MS = 3_000;
// The Hermes edge serving this region pauses 2.5-4s in a ~7.5s cadence
// (verified: two boxes on different networks record IDENTICAL gap seconds —
// feed-side, not ours). Stage tolerates staleness well past that before
// falling back, and the fallback is basis-adjusted into the PYTH price world:
// a raw venue mark sits ~10bps away, which at 1000x is 2× the liquidation
// distance — flipping worlds mid-pause would print phantom wicks.
const PYTH_STAGE_FRESH_MS = 10_000;
function stageMark(m) {
  if (!m) return NaN;
  if (m.indexHalt) return NaN;   // guard tripped: stage freezes — no marks, no fills, no liquidations
  if (Number(m.pythPrice) > 0 && Date.now() - (m.pythAtMs || 0) < PYTH_STAGE_FRESH_MS) return Number(m.pythPrice);
  const basis = Number(m.pythBasis);
  return Number.isFinite(basis) ? Number(m.markPrice) - basis : Number(m.markPrice);
}
function markFor(m, heat) { return heat ? stageMark(m) : effMark(m); }
function markOfFreshFor(sym, heat) {
  const m = mkt(sym);
  return mktFresh(m) ? markFor(m, heat) : null;
}
// Basis-anchored: Phoenix's own index provides the LEVEL (its composition is
// theirs — measured ~9bps off raw Pyth BTC/USD); Pyth provides sub-penny
// high-frequency deltas on top. pythBasis = slow EMA of (venue mark − raw
// pyth), so the mark ticks at oracle granularity but stays centered on the
// venue's index. No basis yet → venue mark.
function effMark(m) {
  if (m && Number(m.pythPrice) > 0 && m.pythBasis != null && Date.now() - (m.pythAtMs || 0) < PYTH_MARK_FRESH_MS) {
    return Number(m.pythPrice) + Number(m.pythBasis);
  }
  return m ? Number(m.markPrice) : NaN;
}
function markOfFresh(sym) {
  const m = mkt(sym);
  return mktFresh(m) ? effMark(m) : null;
}
function pricesUp() {
  if (Date.now() - live.lastMsgMs < WS_FRESH_MS) return true;
  const s = snapFile();
  return !!(s && s.updatedAt && Date.now() - s.updatedAt < 60_000);
}

function cfgOf(sym) {
  const c = mktCfg.get(baseOf(sym)) || {
    tiers: [], maxLev: FALLBACK_MAX_LEV, lotSize: null, takerBps: FALLBACK_TAKER_BPS, makerBps: FALLBACK_MAKER_BPS,
    maintBps: FALLBACK_MAINT_BPS, cancelBps: FALLBACK_CANCEL_BPS, maxLiqSize: null, status: 'active', isolatedOnly: false,
  };
  // Boost twins are isolated-only. At 1000x the liquidation distance is ~5bps,
  // which routine index noise clears several times an hour on the livelier
  // majors; isolating the margin makes that cost the stake committed to the
  // Boost instead of ending the trader's round.
  return aliasKind(sym) === 'BOOST' ? { ...c, isolatedOnly: true } : c;
}
// binding max leverage for a position of this BASE size (tier bands)
function tierLevFor(sym, sizeBase) {
  const c = cfgOf(sym);
  if (!c.tiers.length) return c.maxLev;
  for (const t of c.tiers) if (sizeBase <= t.maxSizeBase + 1e-9) return t.maxLev;
  return c.tiers[c.tiers.length - 1].maxLev;
}
// maintenance / protocol-IM fractions of notional at the position's ACTUAL size
function mmfFor(sym, sizeBase) { return (cfgOf(sym).maintBps / 1e4) / tierLevFor(sym, sizeBase); }
// heat positions liquidate against their chosen leverage, not the tier cap
function mmfForPos(pos, stage) {
  if (stage && Number(pos.leverage) > 0) return (cfgOf(pos.symbol).maintBps / 1e4) / Number(pos.leverage);
  return mmfFor(pos.symbol, pos.size);
}
function imfFor(sym, sizeBase) { return 1 / tierLevFor(sym, sizeBase); }
function snapLots(sym, size, fractional = false) {
  const c = cfgOf(sym);
  if (fractional || !c.lotSize) return r6(size);   // heat mode: lot grids protect real matching engines, paper has none
  return r6(Math.floor(size / c.lotSize + 1e-9) * c.lotSize);
}

// ── account math ─────────────────────────────────────────────────────────
function ensureAccount(userId) {
  const now = Date.now();
  stmt.acctIns.run(userId, now, now);
  return stmt.acctGet.get(userId);
}
const dirOf = (side) => (side === 'LONG' ? 1 : -1);
const isIso = (p) => p.margin_mode === 'isolated';
function uPnl(pos, mark) { return pos.size * (mark - pos.entry_price) * dirOf(pos.side); }
function posMarkOf(pos) {
  const m = mkt(pos.symbol);
  const px = m && markFor(m, isStage(heatOf(pos.user_id)));
  return Number.isFinite(px) && px > 0 ? px : (Number(pos.last_mark) || pos.entry_price);
}

// Cross positions share account equity; isolated positions live on their own
// allocated margin (already moved out of balance) and are excluded from the
// cross risk numbers. equityTotal is what the leaderboard/UI shows.
function accountRisk(userId, acct, { excludeOrderId = null } = {}) {
  const positions = stmt.posByUser.all(userId);
  const orders = stmt.ordOpenByUser.all(userId).filter((o) => o.id !== excludeOrderId);
  let crossUpnl = 0, posMargin = 0, maint = 0, cancelTier = 0, isoValue = 0;
  for (const p of positions) {
    const mk = posMarkOf(p);
    if (isIso(p)) { isoValue += p.isolated_margin + uPnl(p, mk); continue; }
    crossUpnl += uPnl(p, mk);
    posMargin += (p.size * p.entry_price) / p.leverage;
    const notionalMark = p.size * mk;
    maint += notionalMark * mmfFor(p.symbol, p.size);
    cancelTier += notionalMark * imfFor(p.symbol, p.size) * (cfgOf(p.symbol).cancelBps / 1e4);
  }
  // Phoenix reserves limit-order margin per market on the WORSE side only
  // (hypothetical fill at mark × limit_order_risk_factor); mirror that per
  // symbol instead of summing both sides.
  const ordBySym = new Map();
  for (const o of orders) {
    if (o.reduce_only) continue;
    let e = ordBySym.get(o.symbol);
    if (!e) ordBySym.set(o.symbol, e = { buy: 0, sell: 0 });
    e[o.side === 'BUY' ? 'buy' : 'sell'] += (o.price * o.size) / o.leverage;
  }
  let ordMargin = 0;
  for (const e of ordBySym.values()) ordMargin += Math.max(e.buy, e.sell);
  const equityCross = acct.balance + crossUpnl;
  return {
    positions, orders, crossUpnl, maint, cancelTier, isoValue,
    equityCross, equityTotal: equityCross + isoValue,
    free: equityCross - posMargin - ordMargin,
  };
}

// per-position liquidation estimate (display only; sweep uses exact checks)
function liqEstimate(pos, positions, balance, heat = false) {
  const d = dirOf(pos.side), sz = pos.size, mmf = mmfForPos(pos, heat);
  if (isIso(pos)) {
    const im = pos.isolated_margin;
    const m = d === 1 ? (sz * pos.entry_price - im) / (sz * (1 - mmf))
                      : (sz * pos.entry_price + im) / (sz * (1 + mmf));
    return Number.isFinite(m) && m > 0 ? r6(m) : null;
  }
  let othersUpnl = 0, maintOthers = 0;
  for (const p of positions) {
    if (p.symbol === pos.symbol || isIso(p)) continue;
    const mk = posMarkOf(p);
    othersUpnl += uPnl(p, mk);
    maintOthers += p.size * mk * mmfFor(p.symbol, p.size);
  }
  let m;
  if (d === 1) m = (maintOthers - balance - othersUpnl + sz * pos.entry_price) / (sz * (1 - mmf));
  else m = (balance + othersUpnl + sz * pos.entry_price - maintOthers) / (sz * (1 + mmf));
  return Number.isFinite(m) && m > 0 ? r6(m) : null;
}

// ── core fill executor ───────────────────────────────────────────────────
// Applies one fill (open/increase/reduce/flip) to position + account rows and
// records it. Margin CHECKS are the caller's job; margin MOVEMENT for
// isolated positions happens here (balance ⇄ isolated_margin). The isolated
// guarantee holds on EVERY close path (SL/TP/DELIST/liquidation/manual): a
// negative close credit is floored at 0 — loss never exceeds the allocated
// margin, the excess is absorbed as bad debt, Phoenix-style.
function applyFill(userId, { symbol, orderSide, size, px, feeBps, kind, orderId = null, leverage = null, marginMode = 'cross', boostWindow = true }) {
  const now = Date.now();
  const acct = ensureAccount(userId);
  if (isStage(acct.heat)) feeBps = 0;   // stage mode: no fees, survivability is pure price
  const pos = stmt.posGet.get(userId, symbol);
  const notional = r6(size * px);
  const fee = r6(notional * feeBps / 1e4);
  const d = orderSide === 'BUY' ? 1 : -1;
  const mode = pos ? pos.margin_mode : marginMode;
  let realized = null;
  let badDebt = 0;
  let balance = acct.balance;

  if (!pos) {
    let isoMargin = 0;
    if (mode === 'isolated') {
      isoMargin = r6(notional / (leverage || 1));
      balance = r6(balance - isoMargin);
    }
    stmt.posIns.run(userId, symbol, acct.epoch, d === 1 ? 'LONG' : 'SHORT', size, px, leverage || 1, now, px, now, now, mode, isoMargin);
    // Boost window arms on the FILL leverage, not the blended position
    // leverage — diluting a 1000x add with a small base can't dodge the clock
    if ((leverage || 1) >= BOOST_ARM_LEV && boostWindow && isStage(acct.heat)) stmt.posBoostStamp.run(now, userId, symbol);
  } else if (dirOf(pos.side) === d) {
    const newSize = r6(pos.size + size);
    const entry = r6((pos.size * pos.entry_price + size * px) / newSize);
    const oldN = pos.size * pos.entry_price, addN = size * px;
    const addLev = leverage || pos.leverage;
    const newLev = Math.round(((oldN + addN) / (oldN / pos.leverage + addN / addLev)) * 100) / 100;
    let isoMargin = pos.isolated_margin;
    if (isIso(pos)) {
      const addMargin = r6(addN / addLev);
      isoMargin = r6(isoMargin + addMargin);
      balance = r6(balance - addMargin);
    }
    stmt.posUpd.run(newSize, entry, newLev, pos.realized_pnl, isoMargin, now, userId, symbol);
    if (addLev >= BOOST_ARM_LEV && boostWindow && isStage(acct.heat)) stmt.posBoostStamp.run(now, userId, symbol);
  } else {
    const closeSz = Math.min(size, pos.size);
    realized = r6(closeSz * (px - pos.entry_price) * dirOf(pos.side));
    if (isIso(pos)) {
      const frac = closeSz / pos.size;
      const freed = r6(pos.isolated_margin * frac);
      let credit = r6(freed + realized);
      let extraDebit = 0;
      if (credit < 0) {
        // isolated loss cap, subaccount-style: a loss beyond the freed slice
        // first drains the REMAINING isolated margin (the survivor shouldn't
        // keep a full cushion the loss should have eaten); only what exceeds
        // the whole allocated margin is absorbed as bad debt
        extraDebit = Math.min(-credit, Math.max(0, r6(pos.isolated_margin - freed)));
        badDebt = r6(Math.max(0, -credit - extraDebit));
        credit = 0;
      }
      balance = r6(balance + credit);
      if (size < pos.size - 1e-12) {
        stmt.posUpd.run(r6(pos.size - size), pos.entry_price, pos.leverage, r6(pos.realized_pnl + realized), r6(pos.isolated_margin - freed - extraDebit), now, userId, symbol);
      } else {
        stmt.posDel.run(userId, symbol);
        const rem = r6(size - pos.size);
        if (rem > 1e-9) {
          // remainder opens at the ORDER's leverage — margin debit, recorded
          // leverage and the caller's margin check all use the same number
          const remLev = leverage || pos.leverage;
          const remMargin = r6((rem * px) / remLev);
          balance = r6(balance - remMargin);
          stmt.posIns.run(userId, symbol, acct.epoch, d === 1 ? 'LONG' : 'SHORT', rem, px, remLev, now, px, now, now, 'isolated', remMargin);
        }
      }
    } else {
      balance = r6(balance + realized);
      if (size < pos.size - 1e-12) {
        stmt.posUpd.run(r6(pos.size - size), pos.entry_price, pos.leverage, r6(pos.realized_pnl + realized), 0, now, userId, symbol);
      } else {
        stmt.posDel.run(userId, symbol);
        const rem = r6(size - pos.size);
        if (rem > 1e-9) {
          stmt.posIns.run(userId, symbol, acct.epoch, d === 1 ? 'LONG' : 'SHORT', rem, px, leverage || pos.leverage, now, px, now, now, 'cross', 0);
        }
      }
    }
  }
  balance = r6(balance - fee);
  let liquidations = acct.liquidations;
  // bankruptcy floor: flat with a negative balance = busted account. On
  // LIQUIDATION-kind fills the liquidator owns the event count (else one
  // episode would count twice).
  if (balance < 0 && stmt.posCount.get(userId).n === 0) {
    badDebt = r6(badDebt - balance);
    balance = 0;
    if (kind !== 'LIQUIDATION') liquidations += 1;
  }
  stmt.acctUpd.run(balance, acct.fills_count + 1, r6(acct.fees_paid + fee), acct.funding_paid, liquidations, now, userId);
  const info = stmt.fillIns.run(userId, acct.epoch, symbol, orderSide, kind, px, size, notional, fee, realized, orderId, now, badDebt);
  return { id: Number(info.lastInsertRowid), symbol, side: orderSide, kind, price: px, size, notional, fee, realizedPnl: realized, badDebt, ts: now };
}

// ── competition scoring ──────────────────────────────────────────────────
/* One player's numbers for a round snapshot. Everything is measured against
 * the CURRENT epoch, which acctReset bumps at the start of every round, so
 * "this round" needs no timestamp arithmetic and cannot accidentally sweep in
 * a previous heat.
 *
 * accountPnl is equity against the starting balance, isolated positions
 * included, which is exactly what the trader sees. hotBonus is the realised
 * PnL on the round's -HOT ticker: the segment force-closes at its own bell,
 * so that number is final by the time anyone reads it. Adding it to an
 * accountPnl that already contains it once is what makes Hot count double,
 * losses as well as wins. */
const compRealized = db.prepare(
  "SELECT COALESCE(SUM(realized_pnl), 0) AS v FROM paper_fills WHERE user_id = ? AND epoch = ? AND realized_pnl IS NOT NULL"
);
const compHot = db.prepare(
  "SELECT COALESCE(SUM(realized_pnl), 0) AS v FROM paper_fills WHERE user_id = ? AND epoch = ? AND symbol = ? AND realized_pnl IS NOT NULL"
);
function scoreUser(userId, hotTicker) {
  const acct = ensureAccount(userId);
  const risk = accountRisk(userId, acct);
  const start = acct.start_balance || START_BALANCE;
  return {
    equity: r6(risk.equityTotal),
    accountPnl: r6(risk.equityTotal - start),
    realized: r6(compRealized.get(userId, acct.epoch).v),
    hotBonus: hotTicker ? r6(compHot.get(userId, acct.epoch, hotTicker).v) : 0,
  };
}

// ── competition HTTP surface ─────────────────────────────────────────────
/* Operator actions carry their own token on top of the nginx gate: the gate
 * only proves the request came through the site, and everyone trading on
 * /ftpaper is through the site. Fails closed — with no token configured,
 * nothing can start or abort a round. */
const COMP_TOKEN = process.env.PAPER_COMP_TOKEN || '';
const compAuthed = (req, body) =>
  !!COMP_TOKEN && (body.token === COMP_TOKEN || req.headers['x-comp-token'] === COMP_TOKEN);

/* Live wall state. Public: this is what the room sees. The drawn market is
 * withheld until the reveal actually fires, so a spectator refreshing the
 * endpoint cannot learn the Hot Market before the players do. */
function compState(req, res) {
  const p = comp.phaseNow();
  if (!p) return send(res, 200, { ok: true, live: false });
  const r = p.round;
  const drawn = !!r.draw_at;
  const players = comp.playersOf(r.id).map((pl) => {
    let s = { equity: 0, accountPnl: 0, realized: 0, hotBonus: 0 };
    try { s = scoreUser(pl.user_id, drawn ? r.hot_base + '-HOT' : null); } catch {}
    return {
      userId: pl.user_id, name: pl.display_name, seat: pl.seat,
      ...s, score: r6(s.accountPnl + s.hotBonus),
    };
  }).sort((a, b) => b.score - a.score || b.realized - a.realized || a.seat - b.seat)
    .map((x, i) => ({ ...x, rank: i + 1 }));
  return send(res, 200, {
    ok: true, live: true, now: Date.now(),
    round: { id: r.id, kind: r.kind, startedAt: r.started_at, endsAt: r.ends_at },
    phase: p.phase,
    phaseEndsAt: r.started_at + p.endsAt,
    leftMs: Math.max(0, r.ends_at - Date.now()),
    hot: drawn ? { market: r.hot_base, ticker: r.hot_base + '-HOT', open: aliasOpen(r.hot_base + '-HOT') } : null,
    candidates: JSON.parse(r.hot_candidates),
    drawCommit: r.draw_commit,
    // Report the GATE, not the clock. The order path accepts a boost only if
    // the twin is actually open, so if a boundary fired late or a market
    // failed to open, the wall must say what the engine will really do rather
    // than what the schedule intended.
    boostOpen: [...openAliases.keys()].some((k) => aliasKind(k) === 'BOOST'),
    boostMarkets: [...openAliases.keys()].filter((k) => aliasKind(k) === 'BOOST').map(baseOf),
    players,
  });
}

/* Public draw verification. Deliberately unauthenticated: the whole point of
 * commit-reveal is that anyone in the room can check it on their phone. */
function compVerify(req, res, u) {
  const id = u.searchParams.get('round') || '';
  const r = comp.__test.q.get.get(id);
  if (!r) return send(res, 404, { ok: false, error: 'no such round' });
  const v = comp.verifyDraw(r);
  return send(res, 200, {
    ok: true, round: r.id, candidates: JSON.parse(r.hot_candidates),
    commit: r.draw_commit, seed: r.draw_seed, drawn: r.hot_base, verified: v,
    howTo: 'sha256(seed + "|" + candidates.join(",")) must equal commit; sha256(seed) mod candidates.length selects the market',
  });
}

async function compAdmin(req, res) {
  let body; try { body = JSON.parse(await readBody(req) || '{}'); } catch { return send(res, 400, { ok: false, error: 'bad_json' }); }
  if (!compAuthed(req, body)) return send(res, 403, { ok: false, error: 'forbidden' });
  const a = String(body.action || '');
  try {
    if (a === 'create') {
      return send(res, 200, { ok: true, round: comp.createRound(body) });
    }
    if (a === 'start') return send(res, 200, { ok: true, round: comp.startRound(body.id) });
    if (a === 'abort') return send(res, 200, { ok: true, round: comp.abortRound(body.id) });
    if (a === 'standings') {
      return send(res, 200, { ok: true, board: comp.standings(body.id, body.checkpoint || 'final') });
    }
    if (a === 'firstFive') return send(res, 200, { ok: true, ...comp.firstFiveResult(body.id) });
    /* Fresh accounts for the next round: bump the epoch and restore the
       starting balance for everyone seated, in one pass. Doing this per
       player by hand is how someone ends up starting a round on a stale
       balance, which is the funding pre-flight lesson from Belgrade. */
    if (a === 'resetPlayers') {
      const players = comp.playersOf(body.id);
      if (!players.length) return send(res, 400, { ok: false, error: 'no players on that round' });
      const now = Date.now();
      const done = [];
      for (const p of players) {
        ensureAccount(p.user_id);
        stmt.posDelAll ? stmt.posDelAll.run(p.user_id) : db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(p.user_id);
        db.prepare('DELETE FROM paper_orders WHERE user_id = ? AND status = ?').run(p.user_id, 'OPEN');
        stmt.acctReset.run(now, now, p.user_id);
        done.push({ userId: p.user_id, name: p.display_name, epoch: stmt.acctGet.get(p.user_id).epoch });
      }
      _log(`round ${body.id}: reset ${done.length} players`);
      return send(res, 200, { ok: true, reset: done });
    }
    /* Pre-flight: is every seat actually ready? Equivalent of the funding
       check that caught a wallet nobody had topped up at Belgrade. */
    if (a === 'preflight') {
      const players = comp.playersOf(body.id);
      const rows = players.map((p) => {
        const acct = stmt.acctGet.get(p.user_id);
        const pos = stmt.posByUser.all(p.user_id).length;
        const start = acct ? (acct.start_balance || START_BALANCE) : null;
        return {
          userId: p.user_id, name: p.display_name, seat: p.seat,
          hasAccount: !!acct, epoch: acct ? acct.epoch : null,
          balance: acct ? acct.balance : null, startBalance: start,
          openPositions: pos, stage: acct ? isStage(acct.heat) : false,
          ready: !!acct && pos === 0 && isStage(acct.heat) && Math.abs(acct.balance - start) < 1e-9,
        };
      });
      return send(res, 200, { ok: true, allReady: rows.every((r) => r.ready), players: rows });
    }
    return send(res, 400, { ok: false, error: 'unknown action' });
  } catch (e) {
    return send(res, 400, { ok: false, error: e.message });
  }
}

// ── competition segment control ──────────────────────────────────────────
// Open an event ticker for trading. The alias must resolve to a real indexed
// market, so a typo cannot conjure a tradable symbol with no price behind it.
function openAlias(alias, roundId = null) {
  const kind = aliasKind(alias);
  if (!kind) throw new Error('not an alias: ' + alias);
  const base = baseOf(alias);
  if (!STAGE_INDEXED.has(base)) throw new Error('base not indexed: ' + base);
  if (!markOfFreshFor(base, true)) throw new Error('base has no fresh mark: ' + base);
  openAliases.set(alias, { openedAt: Date.now(), roundId });
  _log(`alias open ${alias} (base ${base}${roundId ? ', round ' + roundId : ''})`);
  return true;
}
// Close a segment: stop accepting orders on the ticker, then flatten every
// position on it at the index. Reuses applyFill, so these closes produce
// ordinary fills, realised PnL and audit rows like any other close — the
// segment bonus is simply what got realised, with no snapshot to reconcile.
function closeAlias(alias, { flatten = true } = {}) {
  openAliases.delete(alias);
  // Shutting the gate and flattening are DIFFERENT acts. The Hot segment ends
  // by flattening, because its bonus is what got realised. The bell ends by
  // marking: positions stay exactly where they are and the final snapshot
  // prices them, so nobody gains from closing faster at the end.
  if (!flatten) { _log(`alias close ${alias}: gate shut, positions left open`); return { alias, closed: 0, total: 0 }; }
  const ps = stmt.posBySymbol.all(alias);
  let closed = 0;
  for (const p of ps) {
    const mark = markOfFreshFor(alias, true);
    if (!(Number(mark) > 0)) { _log(`alias close ${alias}: no mark, leaving ${p.user_id} open`); continue; }
    const cSide = p.side === 'LONG' ? 'SELL' : 'BUY';
    applyFill(p.user_id, {
      symbol: alias, orderSide: cSide, size: p.size,
      px: execPxFor(p.user_id, alias, cSide, p.size, mark).px,
      feeBps: cfgOf(alias).takerBps, kind: 'SEGMENT',
    });
    closed += 1;
  }
  _log(`alias close ${alias}: flattened ${closed}/${ps.length}`);
  return { alias, closed, total: ps.length };
}

// ── shared risk evaluation: one code path for the 5s sweep AND per-tick ──
// evaluation, so cadence never changes semantics. SL/TP before liquidation
// (a protective stop wins a shared tick); strict-inequality liq boundary.
function evalPositionAtMark(p, m, now) {
  const isHeat = isStage(heatOf(p.user_id));
  const mark = markFor(m, isHeat);
  if (!Number.isFinite(mark) || mark <= 0) return;   // halted/blind symbol: touch nothing
  stmt.posMark.run(mark, now, p.user_id, p.symbol);
  settleFunding(p, mark, Number(m.currentFundingRate), now);
  const fresh = stmt.posGet.get(p.user_id, p.symbol);
  if (!fresh) return;
  // Boost window over → flatten at the mark, no questions. Runs before SL/TP:
  // an expired position has no protective claims left to exercise.
  if (fresh.boost_since && now - fresh.boost_since >= BOOST_WINDOW_MS) {
    const cSide = fresh.side === 'LONG' ? 'SELL' : 'BUY';
    applyFill(p.user_id, { symbol: p.symbol, orderSide: cSide, size: fresh.size, px: execPxFor(p.user_id, p.symbol, cSide, fresh.size, mark).px, feeBps: cfgOf(p.symbol).takerBps, kind: 'EXPIRY' });
    return;
  }
  const slHit = fresh.sl_price != null && (fresh.side === 'LONG' ? mark <= fresh.sl_price : mark >= fresh.sl_price);
  const tpHit = fresh.tp_price != null && (fresh.side === 'LONG' ? mark >= fresh.tp_price : mark <= fresh.tp_price);
  if (slHit || tpHit) {
    const cSide = fresh.side === 'LONG' ? 'SELL' : 'BUY';
    applyFill(p.user_id, { symbol: p.symbol, orderSide: cSide, size: fresh.size, px: execPxFor(p.user_id, p.symbol, cSide, fresh.size, mark).px, feeBps: cfgOf(p.symbol).takerBps, kind: slHit ? 'SL' : 'TP' });
    return;
  }
  // isolated liquidation: this position's own margin vs its maintenance
  if (isIso(fresh)) {
    const eq = fresh.isolated_margin + uPnl(fresh, mark);
    if (eq < fresh.size * mark * mmfForPos(fresh, isHeat)) liquidateIsolated(fresh);
  }
}
function evalCrossForUser(uid, now) {
  const acct = stmt.acctGet.get(uid);
  if (!acct) return;
  const risk = accountRisk(uid, acct);
  const hasCross = risk.positions.some((p) => !isIso(p));
  if (!hasCross) return;
  if (risk.equityCross < risk.maint) { liquidateCross(uid); return; }
  if (risk.orders.length && risk.equityCross < risk.cancelTier) {
    stmt.ordCancelUser.run(now, uid);
    _log(`order-cancel tier hit for user ${uid} (equity ${risk.equityCross.toFixed(2)} < ${risk.cancelTier.toFixed(2)})`);
  }
}
// Per-tick liquidation/SL/TP: runs on every fresh mark for symbols carrying
// paper risk (throttled per symbol). At high leverage the 5s sweep is too
// coarse — a wick between sweeps must still resolve. FT engine handoff §5.2.
const _lastTickEval = new Map();   // symbol -> ms
const TICK_EVAL_MIN_MS = 250;
function tickEval(symbol, { force = false } = {}) {
  if (_sweepRunning) return;
  const now = Date.now();
  const m = mkt(symbol);
  if (!mktFresh(m)) return;
  // Position lookup FIRST, and the throttle only applies when nothing is at
  // risk. The relay broadcasts every accepted price to the chart; evaluating
  // only the newest every 250ms meant a price could be drawn crossing a
  // liquidation line that the engine never saw -- at ~19 broadcasts/s that is
  // ~5 unevaluated prices per window, and raising the relay rate widened the
  // gap. A symbol carrying risk now evaluates on EVERY accepted tick, so the
  // chart cannot show a crossing the engine did not act on. The query is a
  // prepared statement on an indexed column; the expensive transaction below
  // still only runs when positions exist.
  const ps = stmt.posBySymbol.all(symbol);
  if (!ps.length) {
    if (!force && now - (_lastTickEval.get(symbol) || 0) < TICK_EVAL_MIN_MS) return;
    _lastTickEval.set(symbol, now); return;
  }
  _sweepRunning = true;
  try {
    db.transaction(() => {
      const users = new Set();
      for (const p of ps) {
        users.add(p.user_id);
        try { evalPositionAtMark(p, m, now); } catch (e) { _log(`tick ${p.user_id}/${p.symbol} error: ${e.message}`); }
      }
      for (const uid of users) {
        try { evalCrossForUser(uid, now); } catch (e) { _log(`tick user ${uid} error: ${e.message}`); }
      }
    })();
  } catch (e) { _log('tickEval error: ' + e.message);
  } finally {
    _sweepRunning = false;
    _lastTickEval.set(symbol, now);
  }
}

// funding settles once per hour boundary; isolated positions pay from their
// allocated margin, cross from the account balance
function settleFunding(pos, mark, ratePctHourly, now) {
  if (!Number.isFinite(ratePctHourly)) return 0;
  const prevIv = Math.floor(pos.last_funding_ms / FUNDING_INTERVAL_MS);
  const curIv = Math.floor(now / FUNDING_INTERVAL_MS);
  let k = curIv - prevIv;
  if (k <= 0) return 0;
  if (k > FUNDING_MAX_INTERVALS) k = FUNDING_MAX_INTERVALS;
  const rate = Math.max(-FUNDING_RATE_CLAMP, Math.min(FUNDING_RATE_CLAMP, ratePctHourly)) / 100;
  const heatAcct = stmt.acctGet.get(pos.user_id);
  const pay = (heatAcct && isStage(heatAcct.heat)) ? 0 : r6(pos.size * mark * rate * k * dirOf(pos.side));
  const newIsoMargin = isIso(pos) ? r6(pos.isolated_margin - pay) : pos.isolated_margin;
  stmt.posFunding.run(r6(pos.funding_accrued + pay), curIv * FUNDING_INTERVAL_MS, newIsoMargin, pos.user_id, pos.symbol);
  const acct = stmt.acctGet.get(pos.user_id);
  if (acct) {
    const newBal = isIso(pos) ? acct.balance : r6(acct.balance - pay);
    stmt.acctUpd.run(newBal, acct.fills_count, acct.fees_paid, r6(acct.funding_paid + pay), acct.liquidations, now, pos.user_id);
    // funding-history event (kind FUNDING, excluded from trade history in the
    // UI; realized_pnl carries the signed account impact: positive = received).
    // fills_count deliberately untouched — funding is not a trade.
    if (pay !== 0) {
      stmt.fillIns.run(pos.user_id, acct.epoch, pos.symbol, pos.side, 'FUNDING',
        mark, pos.size, r6(pos.size * mark), 0, r6(-pay), null, now, 0);
    }
  }
  return pay;
}

// cross liquidation: progressive, riskiest position first, per-pass size cap
function liquidateCross(userId) {
  const now = Date.now();
  stmt.ordCancelUser.run(now, userId);
  let closed = 0;
  for (let pass = 0; pass < LIQ_MAX_PASSES; pass++) {
    const acct = stmt.acctGet.get(userId);
    if (!acct) break;
    const risk = accountRisk(userId, acct);
    const cross = risk.positions.filter((p) => !isIso(p));
    if (!cross.length || risk.equityCross >= risk.maint) break;
    let worst = null, worstMaint = -1;
    for (const p of cross) {
      const m = p.size * posMarkOf(p) * mmfFor(p.symbol, p.size);
      if (m > worstMaint) { worstMaint = m; worst = p; }
    }
    if (!worst) break;
    const cfg = cfgOf(worst.symbol);
    const closeSz = cfg.maxLiqSize ? Math.min(worst.size, cfg.maxLiqSize) : worst.size;
    const lSide = worst.side === 'LONG' ? 'SELL' : 'BUY';
    const lMark = posMarkOf(worst);
    applyFill(userId, {
      symbol: worst.symbol, orderSide: lSide,
      size: closeSz, px: execPxFor(userId, worst.symbol, lSide, closeSz, lMark).px, feeBps: cfg.takerBps, kind: 'LIQUIDATION',
    });
    closed++;
  }
  // one liquidation event per call; the bad-debt floor fires ONLY when the
  // account is completely flat (mirrors applyFill's bankruptcy gate) — never
  // while an isolated position or an unliquidated cross remainder survives,
  // which would forgive debt an open position could later profit against
  if (closed > 0) {
    const acct = stmt.acctGet.get(userId);
    if (acct) {
      const flat = stmt.posCount.get(userId).n === 0;
      stmt.acctUpd.run(flat ? Math.max(0, acct.balance) : acct.balance, acct.fills_count, acct.fees_paid, acct.funding_paid, acct.liquidations + 1, now, userId);
    }
  }
  _log(`cross-liquidated user ${userId}: ${closed} close(s)`);
}

// isolated liquidation: full close of THIS position, loss capped at its margin
function liquidateIsolated(pos) {
  const cfg = cfgOf(pos.symbol);
  const lSide = pos.side === 'LONG' ? 'SELL' : 'BUY';
  const lMark = posMarkOf(pos);
  applyFill(pos.user_id, {
    symbol: pos.symbol, orderSide: lSide,
    size: pos.size, px: execPxFor(pos.user_id, pos.symbol, lSide, pos.size, lMark).px, feeBps: cfg.takerBps, kind: 'LIQUIDATION',
  });
  // no balance floor here: the iso loss cap already absorbed any bad debt
  // inside applyFill, and flooring a negative CROSS balance while other
  // positions are open would mint equity
  const acct = stmt.acctGet.get(pos.user_id);
  if (acct) stmt.acctUpd.run(acct.balance, acct.fills_count, acct.fees_paid, acct.funding_paid, acct.liquidations + 1, Date.now(), pos.user_id);
  _log(`iso-liquidated user ${pos.user_id} ${pos.symbol}`);
}

// apply SL/TP attached to a limit order once it fills (best-effort: skipped
// if the resulting position isn't on the expected side or trigger is invalid)
function applyAttachedTriggers(order, fillPx) {
  const pos = stmt.posGet.get(order.user_id, order.symbol);
  if (!pos) return;
  const expectSide = order.side === 'BUY' ? 'LONG' : 'SHORT';
  if (pos.side !== expectSide) return;
  let sl = Number(order.attach_sl) > 0 ? Number(order.attach_sl) : null;
  let tp = Number(order.attach_tp) > 0 ? Number(order.attach_tp) : null;
  if (sl != null && !(pos.side === 'LONG' ? sl < fillPx : sl > fillPx)) sl = null;
  if (tp != null && !(pos.side === 'LONG' ? tp > fillPx : tp < fillPx)) tp = null;
  if (sl == null && tp == null) return;
  stmt.posSltp.run(sl ?? pos.sl_price, tp ?? pos.tp_price, order.user_id, order.symbol);
}

// ── payload shaping ──────────────────────────────────────────────────────
function positionPayload(p, positions, balance, heat = false) {
  const mk = posMarkOf(p);
  const u = r6(uPnl(p, mk));
  const margin = isIso(p) ? p.isolated_margin : (p.size * p.entry_price) / p.leverage;
  return {
    symbol: p.symbol, side: p.side, size: p.size, entry: p.entry_price, mark: mk,
    leverage: p.leverage, notional: r6(p.size * mk), uPnl: u,
    uPnlPct: margin > 0 ? r6((u / margin) * 100) : 0,
    realizedPnl: p.realized_pnl, funding: p.funding_accrued,
    sl: p.sl_price, tp: p.tp_price,
    marginMode: p.margin_mode, isolatedMargin: isIso(p) ? r6(p.isolated_margin) : null,
    liqEst: liqEstimate(p, positions, balance, heat),
    maxLevAtSize: heat ? stageLevCap(p.symbol) : tierLevFor(p.symbol, p.size),
    boostExpiresAt: p.boost_since ? p.boost_since + BOOST_WINDOW_MS : null,
    openedAt: p.opened_at,
  };
}
function orderPayload(o) {
  return { id: o.id, symbol: o.symbol, side: o.side, type: 'LIMIT', price: o.price, size: o.size,
    notional: r6(o.price * o.size), leverage: o.leverage, reduceOnly: !!o.reduce_only,
    marginMode: o.margin_mode || 'cross', attachSl: o.attach_sl, attachTp: o.attach_tp, createdAt: o.created_at };
}
function fillPayload(f) {
  return { id: f.id, symbol: f.symbol, side: f.side, kind: f.kind, price: f.price, size: f.size,
    notional: f.notional, fee: f.fee, realizedPnl: f.realized_pnl, badDebt: f.bad_debt || 0, epoch: f.epoch, ts: f.ts };
}

// ── handlers ─────────────────────────────────────────────────────────────

// GET /api/paper/account
async function account(req, res) {
  const u = await sessionUser(req); if (!u) return send(res, 401, { ok: false, error: 'not_signed_in' });
  const acct = ensureAccount(u.id);
  const risk = accountRisk(u.id, acct);
  return send(res, 200, {
    ok: true,
    account: {
      balance: acct.balance, equity: r6(risk.equityTotal), free: r6(Math.max(0, risk.free)),
      uPnl: r6(risk.crossUpnl + (risk.isoValue - risk.positions.filter(isIso).reduce((s, p) => s + p.isolated_margin, 0))),
      maintenance: r6(risk.maint), isolatedValue: r6(risk.isoValue),
      epoch: acct.epoch, resets: acct.resets, fillsCount: acct.fills_count,
      feesPaid: acct.fees_paid, fundingPaid: acct.funding_paid, liquidations: acct.liquidations,
      startBalance: acct.start_balance || START_BALANCE, resetAt: acct.reset_at,
      heat: isStage(acct.heat), scaled: isScaled(acct.heat),
    },
    positions: risk.positions.map((p) => positionPayload(p, risk.positions, acct.balance, isStage(acct.heat))),
    orders: risk.orders.map(orderPayload),
    feeRates: { takerBps: FALLBACK_TAKER_BPS, makerBps: FALLBACK_MAKER_BPS },
    pricesStale: !pricesUp(),
  });
}

// POST /api/paper/order
// {symbol, side, type, size?|notionalUsd?, price?, leverage, reduceOnly?,
//  marginMode?:'cross'|'isolated', sl?, tp?}
// sl/tp on MARKET are applied to the position atomically with the fill; on
// LIMIT they ride the order and attach when it fills.
async function placeOrder(req, res) {
  const u = await sessionUser(req); if (!u) return send(res, 401, { ok: false, error: 'not_signed_in' });
  if (!writeRateOk(u.id)) return send(res, 429, { ok: false, error: 'rate_limited' });
  let body; try { body = JSON.parse(await readBody(req) || '{}'); } catch { return send(res, 400, { ok: false, error: 'bad_json' }); }

  const symbol = String(body.symbol || '').toUpperCase();
  const side = body.side === 'BUY' ? 'BUY' : body.side === 'SELL' ? 'SELL' : null;
  const type = body.type === 'LIMIT' ? 'LIMIT' : body.type === 'MARKET' ? 'MARKET' : null;
  const reduceOnly = !!body.reduceOnly;
  // Boost window opt-out: a ≥1000x fill normally starts the 2-min clock.
  // boostWindow:false arms Boost with no auto-close (free-play / practice).
  const boostWindow = body.boostWindow !== false;
  const postOnly = !!body.postOnly && type === 'LIMIT';
  if (!side || !type) return send(res, 400, { ok: false, error: 'bad_request' });

  // An event ticker exists only while its segment is open. Closed covers both
  // "the Hot Market has not been drawn yet" and "the window has ended", and
  // the check is here rather than in the client so a stale tab or a crafted
  // request cannot trade a segment that is not running.
  if (aliasKind(symbol) && !aliasOpen(symbol)) {
    return send(res, 400, { ok: false, error: 'market_closed' });
  }
  const acctH = ensureAccount(u.id);
  const mark = markOfFreshFor(symbol, isStage(acctH.heat));
  if (!mark) {
    const m0 = mkt(symbol);
    if (m0 && m0.indexHalt) return send(res, 503, { ok: false, error: 'prices_stale' });   // guard-halted, not unknown
    return send(res, pricesUp() ? 400 : 503, { ok: false, error: pricesUp() ? 'bad_symbol' : 'prices_stale' });
  }

  const cfg = cfgOf(symbol);
  if (cfg.status !== 'active' && cfg.status !== 'postOnly') return send(res, 400, { ok: false, error: 'market_not_active' });

  const pos = stmt.posGet.get(u.id, symbol);
  // margin mode: isolatedOnly markets force isolated; an existing position
  // pins the mode for anything touching that symbol
  let marginMode = body.marginMode === 'isolated' ? 'isolated' : 'cross';
  if (cfg.isolatedOnly) marginMode = 'isolated';
  if (pos) marginMode = pos.margin_mode;

  let price = null;
  if (type === 'LIMIT') {
    price = Number(body.price);
    if (!Number.isFinite(price) || price <= 0 || Math.abs(price - mark) / mark > LIMIT_BAND)
      return send(res, 400, { ok: false, error: 'bad_price' });
  }
  const refPx = type === 'LIMIT' ? price : mark;

  let size = Number(body.size);
  if (!Number.isFinite(size) || size <= 0) {
    const nUsd = Number(body.notionalUsd);
    if (Number.isFinite(nUsd) && nUsd > 0) size = nUsd / refPx; else return send(res, 400, { ok: false, error: 'bad_size' });
  }
  size = snapLots(symbol, size, isScaled(acctH.heat));
  let notional = size * refPx;   // let: the stage full-send clamp re-derives it after shrinking size
  // reduce-only shrinks risk: exempt from the min-notional floor
  const minNotional = isScaled(acctH.heat) ? HEAT_MIN_NOTIONAL : MIN_NOTIONAL;
  const maxNotional = isStage(acctH.heat) ? STAGE_MAX_NOTIONAL : MAX_NOTIONAL;
  if (!(size > 0) || (!reduceOnly && notional < minNotional) || notional > maxNotional) return send(res, 400, { ok: false, error: 'bad_size' });

  // tier-banded leverage: the cap depends on the RESULTING position size.
  // Pure reduces never consume margin, so only the top-tier cap applies.
  let resultSize = size;
  if (pos) resultSize = dirOf(pos.side) === (side === 'BUY' ? 1 : -1) ? pos.size + size : Math.max(size - pos.size, 0);
  const engineCap = isStage(acctH.heat) ? stageLevCap(symbol) : (reduceOnly ? cfg.maxLev : tierLevFor(symbol, Math.max(resultSize, size)));
  // While a competition round is running the phase owns the ceiling: ordinary
  // tickers are held at the baseline and only a -BOOST twin inside the Boost
  // window reaches the engine cap. Outside a round this returns engineCap
  // unchanged, so ordinary paper trading is untouched.
  const levCap = comp.levCapFor(symbol, engineCap, u.id);
  if (!(levCap > 0)) return send(res, 400, { ok: false, error: 'market_closed' });
  let leverage = Number(body.leverage) || 1;
  leverage = Math.min(Math.max(1, leverage), levCap);

  // SL/TP (optional): validate against the reference price now
  let sl = body.sl == null ? null : Number(body.sl);
  let tp = body.tp == null ? null : Number(body.tp);
  if (sl != null && (!Number.isFinite(sl) || sl <= 0)) return send(res, 400, { ok: false, error: 'bad_trigger' });
  if (tp != null && (!Number.isFinite(tp) || tp <= 0)) return send(res, 400, { ok: false, error: 'bad_trigger' });
  const wantLong = side === 'BUY';
  if (sl != null && !(wantLong ? sl < refPx : sl > refPx)) return send(res, 400, { ok: false, error: 'bad_trigger' });
  if (tp != null && !(wantLong ? tp > refPx : tp < refPx)) return send(res, 400, { ok: false, error: 'bad_trigger' });

  const marketable = type === 'MARKET' || (side === 'BUY' ? mark <= price : mark >= price);
  // taker fills walk the LIVE book: warm the symbol's L2 before any account
  // reads (the await may yield the event loop, so all DB state is read after)
  if (marketable) await awaitBook(symbol);
  const mark2 = markOfFreshFor(symbol, isStage(acctH.heat)) ?? mark;   // refresh after the await
  const acct = ensureAccount(u.id);
  const risk = accountRisk(u.id, acct);
  const now = Date.now();
  // Re-read the position AFTER the awaitBook yield: an SL/TP or liquidation in
  // the sweep can close it during the wait. Using the stale pre-await `pos`
  // for the reduce-only guard let a reduce-only order slip through and OPEN a
  // fresh position (applyFill re-reads null → treats it as an open), bypassing
  // the margin check. Guard on the fresh state.
  const posNow = stmt.posGet.get(u.id, symbol);

  if (reduceOnly) {
    if (!posNow || (side === 'BUY') === (posNow.side === 'LONG')) return send(res, 400, { ok: false, error: 'not_reducing' });
    if (size > posNow.size) size = posNow.size;
  } else {
    let addedNotional = notional;
    if (posNow && (side === 'BUY') !== (posNow.side === 'LONG')) addedNotional = Math.max(0, (size - posNow.size) * refPx);
    // require margin + fee + slippage cost so a max-size fill can't push the
    // account negative past its reservation (taker terms when this order
    // would execute now, maker terms when it rests). Stage fills execute AT
    // the mark with zero fees — reserving for costs that cannot occur would
    // wrongly reject boundary-exact max sends.
    const stageAcct = isStage(acctH.heat);
    const feeRate = stageAcct ? 0 : (marketable ? cfg.takerBps : cfg.makerBps) / 1e4;
    const slip = stageAcct ? 0 : (marketable ? slipBps(notional) / 1e4 : 0);
    let required = addedNotional / leverage + notional * (feeRate + slip);
    if (required > risk.free + 1e-9) {
      if (stageAcct && risk.free > 0 && addedNotional > 0) {
        // full-send tolerance: clamp to what margin affords instead of
        // rejecting over price drift or float dust. Size from the budget
        // with FLOOR — r6 rounds half-up and can round the size back past
        // the budget by ~1e-7 BTC (≈ $0.03 notional), which fails the
        // re-check and made max-sends reject on a price-dependent coin flip.
        const shrink = (risk.free * leverage) / addedNotional;
        if (shrink > 0.90) {
          size = Math.floor(((risk.free * leverage) / refPx) * 1e6) / 1e6;
          if (!(size > 0)) return send(res, 400, { ok: false, error: 'insufficient_margin' });
          notional = size * refPx;
          required = notional / leverage;
        }
      }
      if (required > risk.free + 1e-9) return send(res, 400, { ok: false, error: 'insufficient_margin' });
    }
  }

  if (marketable) {
    // post-only never takes: reject instead of crossing (Phoenix may slide or
    // reject depending on config; we reject)
    if (postOnly) return send(res, 400, { ok: false, error: 'would_cross' });
    if (cfg.status !== 'active') return send(res, 400, { ok: false, error: 'market_not_active' });

    // Execute against the LIVE L2 (IOC): walk real levels inside the slippage
    // collar (and never through a limit's own price). Partial liquidity is
    // honest: a MARKET order fills what the book offers (IOC), a marketable
    // LIMIT fills the crossable part and RESTS the remainder (cross-then-
    // rest, GTC). Model fallback only when no live book is available.
    let execSize = size, execPx, execSource, restRemainder = 0;
    if (isStage(acctH.heat)) {
      // stage rule: heat fills execute AT the oracle mark, full size — the
      // chart, the mark and the fill are one price. Non-crossable limits rest.
      const crossable = type === 'MARKET' || (side === 'BUY' ? mark2 <= price : mark2 >= price);
      if (!crossable) {
        if (stmt.ordCountOpen.get(u.id).n >= MAX_OPEN_ORDERS) return send(res, 400, { ok: false, error: 'too_many_orders' });
        const info = stmt.ordIns.run(u.id, acct.epoch, symbol, side, price, size, leverage, reduceOnly ? 1 : 0, now, marginMode, sl, tp);
        return send(res, 200, { ok: true, order: orderPayload(stmt.ordGet.get(Number(info.lastInsertRowid))) });
      }
      execPx = rpx(mark2);
      execSource = 'mark';
    } else {
    const bx = bookExec(symbol, side, size, mark2, type === 'LIMIT' ? price : null);
    if (bx) {
      if (bx.filledBase <= 0) {
        if (type === 'MARKET') return send(res, 400, { ok: false, error: 'slippage_exceeded' });
        // marketable limit with nothing crossable inside the collar: rest it all
        if (stmt.ordCountOpen.get(u.id).n >= MAX_OPEN_ORDERS) return send(res, 400, { ok: false, error: 'too_many_orders' });
        const info = stmt.ordIns.run(u.id, acct.epoch, symbol, side, price, size, leverage, reduceOnly ? 1 : 0, now, marginMode, sl, tp);
        return send(res, 200, { ok: true, order: orderPayload(stmt.ordGet.get(Number(info.lastInsertRowid))) });
      }
      execSize = snapLots(symbol, bx.filledBase, isScaled(acctH.heat)) || bx.filledBase;
      execSize = Math.min(execSize, size);
      execPx = r6(bx.vwap);
      execSource = 'book';
      if (type === 'LIMIT' && execSize < size - 1e-9) restRemainder = r6(size - execSize);
    } else {
      // no live book: empirical impact model, clamped at a limit's own price
      execPx = takerPx(mark2, side, notional);
      if (type === 'LIMIT') execPx = side === 'BUY' ? Math.min(execPx, price) : Math.max(execPx, price);
      execSource = 'model';
    }
    }

    let orderId = null;
    if (type === 'LIMIT') {
      const info = stmt.ordIns.run(u.id, acct.epoch, symbol, side, price, execSize, leverage, reduceOnly ? 1 : 0, now, marginMode, sl, tp);
      orderId = Number(info.lastInsertRowid);
      stmt.ordClose.run('FILLED', now, orderId);
    }
    const fill = applyFill(u.id, { symbol, orderSide: side, size: execSize, px: execPx, feeBps: cfg.takerBps, kind: 'MARKET', orderId, leverage, marginMode, boostWindow });
    // cross-then-rest: the uncrossed remainder of a marketable limit rests
    let restedOrder = null;
    if (restRemainder > 0 && stmt.ordCountOpen.get(u.id).n < MAX_OPEN_ORDERS) {
      const info = stmt.ordIns.run(u.id, acct.epoch, symbol, side, price, restRemainder, leverage, reduceOnly ? 1 : 0, now, marginMode, sl, tp);
      restedOrder = orderPayload(stmt.ordGet.get(Number(info.lastInsertRowid)));
    }
    // attach triggers atomically with the fill (re-check side vs actual fill
    // px); anything that no longer validates is reported back, never silent
    let triggers;
    if (sl != null || tp != null) {
      let okSl = null, okTp = null;
      const p2 = stmt.posGet.get(u.id, symbol);
      if (p2 && p2.side === (wantLong ? 'LONG' : 'SHORT')) {
        okSl = sl != null && (wantLong ? sl < execPx : sl > execPx) ? sl : null;
        okTp = tp != null && (wantLong ? tp > execPx : tp < execPx) ? tp : null;
        if (okSl != null || okTp != null) stmt.posSltp.run(okSl ?? p2.sl_price, okTp ?? p2.tp_price, u.id, symbol);
      }
      triggers = { sl: okSl, tp: okTp, droppedSl: sl != null && okSl == null, droppedTp: tp != null && okTp == null };
    }
    return send(res, 200, {
      ok: true, fill, triggers, order: restedOrder || undefined,
      requestedSize: size !== execSize ? size : undefined,
      execution: execSource,
    });
  }

  if (stmt.ordCountOpen.get(u.id).n >= MAX_OPEN_ORDERS) return send(res, 400, { ok: false, error: 'too_many_orders' });
  const info = stmt.ordIns.run(u.id, acct.epoch, symbol, side, price, size, leverage, reduceOnly ? 1 : 0, now, marginMode, sl, tp);
  return send(res, 200, { ok: true, order: orderPayload(stmt.ordGet.get(Number(info.lastInsertRowid))) });
}

// POST /api/paper/cancel {orderId}
async function cancelOrder(req, res) {
  const u = await sessionUser(req); if (!u) return send(res, 401, { ok: false, error: 'not_signed_in' });
  if (!writeRateOk(u.id)) return send(res, 429, { ok: false, error: 'rate_limited' });
  let body; try { body = JSON.parse(await readBody(req) || '{}'); } catch { return send(res, 400, { ok: false, error: 'bad_json' }); }
  const o = stmt.ordGet.get(Number(body.orderId));
  if (!o || o.user_id !== u.id || o.status !== 'OPEN') return send(res, 404, { ok: false, error: 'not_found' });
  stmt.ordClose.run('CANCELLED', Date.now(), o.id);
  return send(res, 200, { ok: true });
}

// POST /api/paper/close {symbol, size?|sizeUsd?|pct?}  (omit = full close)
async function closePosition(req, res) {
  const u = await sessionUser(req); if (!u) return send(res, 401, { ok: false, error: 'not_signed_in' });
  if (!writeRateOk(u.id)) return send(res, 429, { ok: false, error: 'rate_limited' });
  let body; try { body = JSON.parse(await readBody(req) || '{}'); } catch { return send(res, 400, { ok: false, error: 'bad_json' }); }
  const symbol = String(body.symbol || '').toUpperCase();
  if (!stmt.posGet.get(u.id, symbol)) return send(res, 404, { ok: false, error: 'no_position' });
  // warm the live book, then re-read EVERYTHING (the await yields the loop;
  // an SL could have closed the position while we waited)
  await awaitBook(symbol);
  const pos = stmt.posGet.get(u.id, symbol);
  if (!pos) return send(res, 404, { ok: false, error: 'no_position' });
  const mark = markOfFreshFor(symbol, isStage(heatOf(u.id)));
  if (!mark) return send(res, 503, { ok: false, error: 'prices_stale' });

  let size = Number(body.size);
  if (!Number.isFinite(size) || size <= 0) {
    const pct = Number(body.pct);
    const nUsd = Number(body.sizeUsd);
    if (Number.isFinite(pct) && pct > 0) size = pos.size * Math.min(pct, 100) / 100;
    else if (Number.isFinite(nUsd) && nUsd > 0) size = nUsd / mark;
    else size = pos.size;
  }
  size = Math.min(size, pos.size);
  if (size < pos.size - 1e-12) {
    size = snapLots(symbol, size, isScaled(ensureAccount(u.id).heat));
    if (!(size > 0)) return send(res, 400, { ok: false, error: 'bad_size' });
  } else {
    size = pos.size;
  }
  const closeSide = pos.side === 'LONG' ? 'SELL' : 'BUY';
  const fill = applyFill(u.id, { symbol, orderSide: closeSide, size, px: execPxFor(u.id, symbol, closeSide, size, mark).px, feeBps: cfgOf(symbol).takerBps, kind: 'MARKET' });
  return send(res, 200, { ok: true, fill });
}

// POST /api/paper/margin {symbol, amount}  — adjust ISOLATED margin
// (+amount moves balance → position, −amount frees margin back to balance;
// mirrors Phoenix TransferCollateral between cross and the child subaccount)
async function adjustMargin(req, res) {
  const u = await sessionUser(req); if (!u) return send(res, 401, { ok: false, error: 'not_signed_in' });
  if (!writeRateOk(u.id)) return send(res, 429, { ok: false, error: 'rate_limited' });
  let body; try { body = JSON.parse(await readBody(req) || '{}'); } catch { return send(res, 400, { ok: false, error: 'bad_json' }); }
  const symbol = String(body.symbol || '').toUpperCase();
  const amount = Number(body.amount);
  if (!Number.isFinite(amount) || amount === 0 || Math.abs(amount) > MAX_NOTIONAL) return send(res, 400, { ok: false, error: 'bad_size' });
  const pos = stmt.posGet.get(u.id, symbol);
  if (!pos || !isIso(pos)) return send(res, 404, { ok: false, error: 'no_position' });
  const mark = markOfFreshFor(symbol, isStage(heatOf(u.id)));
  if (!mark) return send(res, 503, { ok: false, error: 'prices_stale' });
  const acct = ensureAccount(u.id);
  const risk = accountRisk(u.id, acct);
  const amt = r6(amount);
  if (amt > 0) {
    if (amt > risk.free + 1e-9) return send(res, 400, { ok: false, error: 'insufficient_margin' });
  } else {
    // removal keeps the position at or above protocol initial margin, and can
    // never withdraw unrealized profit past the allocated margin itself
    if (-amt > pos.isolated_margin + 1e-9) return send(res, 400, { ok: false, error: 'insufficient_margin' });
    const eqAfter = pos.isolated_margin + amt + uPnl(pos, mark);
    if (eqAfter < pos.size * mark * imfFor(symbol, pos.size) - 1e-9) return send(res, 400, { ok: false, error: 'insufficient_margin' });
  }
  const now = Date.now();
  db.transaction(() => {
    stmt.posUpd.run(pos.size, pos.entry_price, pos.leverage, pos.realized_pnl, r6(pos.isolated_margin + amt), now, u.id, symbol);
    stmt.acctUpd.run(r6(acct.balance - amt), acct.fills_count, acct.fees_paid, acct.funding_paid, acct.liquidations, now, u.id);
  })();
  return send(res, 200, { ok: true, position: { symbol, isolatedMargin: r6(pos.isolated_margin + amt) } });
}

// POST /api/paper/sltp {symbol, sl?, tp?}
async function setSltp(req, res) {
  const u = await sessionUser(req); if (!u) return send(res, 401, { ok: false, error: 'not_signed_in' });
  if (!writeRateOk(u.id)) return send(res, 429, { ok: false, error: 'rate_limited' });
  let body; try { body = JSON.parse(await readBody(req) || '{}'); } catch { return send(res, 400, { ok: false, error: 'bad_json' }); }
  const symbol = String(body.symbol || '').toUpperCase();
  const pos = stmt.posGet.get(u.id, symbol);
  if (!pos) return send(res, 404, { ok: false, error: 'no_position' });
  const mark = markOfFreshFor(symbol, isStage(heatOf(u.id)));
  if (!mark) return send(res, 503, { ok: false, error: 'prices_stale' });
  const sl = body.sl == null ? null : Number(body.sl);
  const tp = body.tp == null ? null : Number(body.tp);
  if (sl != null && (!Number.isFinite(sl) || sl <= 0)) return send(res, 400, { ok: false, error: 'bad_trigger' });
  if (tp != null && (!Number.isFinite(tp) || tp <= 0)) return send(res, 400, { ok: false, error: 'bad_trigger' });
  if (pos.side === 'LONG') {
    if ((sl != null && sl >= mark) || (tp != null && tp <= mark)) return send(res, 400, { ok: false, error: 'bad_trigger' });
  } else {
    if ((sl != null && sl <= mark) || (tp != null && tp >= mark)) return send(res, 400, { ok: false, error: 'bad_trigger' });
  }
  stmt.posSltp.run(sl, tp, u.id, symbol);
  return send(res, 200, { ok: true, position: { symbol, sl, tp } });
}

// POST /api/paper/reset
async function reset(req, res) {
  const u = await sessionUser(req); if (!u) return send(res, 401, { ok: false, error: 'not_signed_in' });
  const acct = ensureAccount(u.id);
  const now = Date.now();
  // mode switch: 'heat' = FT stage config ($10 real bankroll, display-scaled,
  // no fees/funding, fractional lots); 'scaled' = venue economics on the $10
  // micro bankroll; 'standard' restores the $10k account.
  let mode = null;
  try { mode = (JSON.parse(bodyRaw || '{}').mode) || null; } catch {}
  // cooldown guards the PUBLIC leaderboard against reset-spam — it only
  // applies to standard-account resets that stay standard. Stage/scaled
  // accounts (off the board) and mode SWITCHES reset freely.
  const curMode = Number(acct.heat) === 1 ? 'heat' : Number(acct.heat) >= 1 ? 'scaled' : 'standard';
  const modeChanging = mode != null && mode !== curMode;
  if (curMode === 'standard' && !modeChanging
      && acct.reset_at && now - acct.reset_at < RESET_COOLDOWN_MS) {
    return send(res, 429, { ok: false, error: 'reset_cooldown' });
  }
  db.transaction(() => {
    if (mode === 'heat') db.prepare('UPDATE paper_accounts SET heat = 1, start_balance = ? WHERE user_id = ?').run(HEAT_BALANCE, u.id);
    else if (mode === 'scaled') db.prepare('UPDATE paper_accounts SET heat = 2, start_balance = ? WHERE user_id = ?').run(HEAT_BALANCE, u.id);
    else if (mode === 'standard') db.prepare('UPDATE paper_accounts SET heat = 0, start_balance = ? WHERE user_id = ?').run(START_BALANCE, u.id);
    stmt.posDelUser.run(u.id);
    stmt.ordCancelUser.run(now, u.id);
    stmt.acctReset.run(now, now, u.id);
  })();
  const fresh = stmt.acctGet.get(u.id);
  const sb = fresh.start_balance || START_BALANCE;
  return send(res, 200, { ok: true, account: { balance: sb, equity: sb, resets: acct.resets + 1, heat: isStage(fresh.heat), scaled: isScaled(fresh.heat) } });
}

// GET /api/paper/orders — order history (all statuses, current epoch)
async function ordersHistory(req, res, u_url) {
  const u = await sessionUser(req); if (!u) return send(res, 401, { ok: false, error: 'not_signed_in' });
  const q = u_url ? u_url.searchParams : new URL(req.url, 'http://x').searchParams;
  const limit = Math.min(Math.max(1, Number(q.get('limit')) || 50), 200);
  const acct = ensureAccount(u.id);
  const rows = stmt.ordHistory.all(u.id, acct.epoch, limit);
  return send(res, 200, { ok: true, orders: rows.map((o) => ({ ...orderPayload(o), status: o.status, closedAt: o.closed_at })) });
}

// GET /api/paper/fills?limit&before&all=1
async function fills(req, res, u_url) {
  const u = await sessionUser(req); if (!u) return send(res, 401, { ok: false, error: 'not_signed_in' });
  const q = u_url ? u_url.searchParams : new URL(req.url, 'http://x').searchParams;
  const limit = Math.min(Math.max(1, Number(q.get('limit')) || 50), 200);
  const before = Number(q.get('before')) || Number.MAX_SAFE_INTEGER;
  const acct = ensureAccount(u.id);
  // kind=trades excludes FUNDING rows (so hourly funding on long-held
  // positions can't dilute the trade-history window); kind=funding inverts
  const kind = q.get('kind');
  const rows = q.get('all') === '1'
    ? stmt.fillListAll.all(u.id, before, limit)
    : kind === 'trades' ? stmt.fillListTrades.all(u.id, acct.epoch, before, limit)
    : kind === 'funding' ? stmt.fillListFunding.all(u.id, acct.epoch, before, limit)
    : stmt.fillList.all(u.id, acct.epoch, before, limit);
  return send(res, 200, { ok: true, fills: rows.map(fillPayload) });
}

// GET /api/paper/leaderboard — public. Standard and Frontier Stage accounts
// are separate competitions with different starting balances. Stage includes
// guest identities so anyone can compete without signing in, but only after
// their first fill/open position; otherwise abandoned guest sessions at the
// untouched $100k balance would bury actual traders.
const _lbs = {
  standard: { ts: 0, rows: null, top: null },
  stage: { ts: 0, rows: null, top: null },
};
async function rebuildLb(mode = 'standard') {
  // Identity cache must be warm before the sync isGuest/name lookups below.
  await auth.warmUsers(stmt.acctAll.all().map((a) => a.user_id));
  const stage = mode === 'stage';
  const accts = stmt.acctAll.all();
  const posByUser = new Map();
  for (const p of stmt.posAll.all()) {
    if (!posByUser.has(p.user_id)) posByUser.set(p.user_id, []);
    posByUser.get(p.user_id).push(p);
  }
  const rows = [];
  for (const a of accts) {
    const ps = posByUser.get(a.user_id) || [];
    if (stage) {
      if (!a.heat) continue;
      if (!(Number(a.fills_count) > 0 || ps.length > 0)) continue;
    } else {
      if (a.heat) continue;
      try { if (auth.isGuestUser(a.user_id)) continue; } catch {}
    }
    let equity = a.balance;
    for (const p of ps) {
      const mk = posMarkOf(p);
      equity += uPnl(p, mk) + (isIso(p) ? p.isolated_margin : 0);
    }
    equity = r6(equity);
    const sb = a.start_balance || START_BALANCE;
    rows.push({ userId: a.user_id, equity, returnPct: r6(((equity - sb) / sb) * 100),
      resets: a.resets, positions: ps.length, fills: a.fills_count });
  }
  rows.sort((x, y) => y.equity - x.equity);
  const top = rows.slice(0, 100).map((r, i) => {
    let name = null, avatar = null;
    try {
      const pu = auth.publicUser(auth.getUserById(r.userId));
      if (pu) { name = pu.displayName || (pu.x && '@' + pu.x.handle) || null; avatar = pu.avatar && pu.avatar.url; }
    } catch {}
    return { rank: i + 1, displayName: name || 'Anonymous', avatarUrl: avatar || null,
      avatarSeed: `paper-${((Number(r.userId) * 2654435761) >>> 0).toString(16)}`,
      equity: r.equity, returnPct: r.returnPct, resets: r.resets, positions: r.positions, fills: r.fills };
  });
  _lbs[mode] = { ts: Date.now(), rows, top };
}
// Effective engine config per market (post-override). The frontend merges
// this over the raw /exchange meta so the ticket slider and picker badges
// show the PAPER caps, not the venue caps.
function marketTape(req, res, u_url) {
  if (_readRateOk && !_readRateOk(req.headers['x-real-ip'] || req.socket.remoteAddress)) {
    return send(res, 429, { ok: false, error: 'rate_limited' });
  }
  const symbol = String(u_url.searchParams.get('symbol') || '').toUpperCase();
  if (!symbol || !/^[A-Z0-9]{1,12}$/.test(symbol)) return send(res, 400, { ok: false, error: 'bad_symbol' });
  const limit = Math.min(500, Math.max(1, Number(u_url.searchParams.get('limit')) || 300));
  send(res, 200, { ok: true, symbol, rows: (tape.get(symbol) || []).slice(0, limit) });
}

// GET /api/paper/pyth-stream — SSE relay of the ENGINE's ingested oracle
// ticks. The terminal charts THIS feed, so what the user sees crossing a
// liquidation line is by construction the same tick the engine acted on —
// the chart can never run ahead of the engine again.
const PYTH_SSE_MAX_CLIENTS = 300;
const pythSseClients = new Set();
function pythStream(req, res) {
  if (pythSseClients.size >= PYTH_SSE_MAX_CLIENTS) return send(res, 503, { ok: false, error: 'stream_full' });
  res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', connection: 'keep-alive' });
  // snapshot so the client has a price before the first live tick
  for (const [sym, m] of live.map) {
    if (Number(m.pythPrice) > 0) res.write(`data: {"s":"${sym}","p":${m.pythPrice},"t":${m.pythAtMs}}\n\n`);
  }
  pythSseClients.add(res);
  req.on('close', () => pythSseClients.delete(res));
}
const _sseLastBySym = new Map();
// Relay gate. Binance fires ~100/s on BTC, so this is where fluidity is won or
// lost. 120ms (~8/s) was set when the chart bucketed at 500ms and anything
// faster was genuinely wasted; the chart now buckets at 125ms on indexed
// symbols, so the gate was the binding constraint. The 1000x set gets 40ms
// (~25/s) because that is where 5bps of liquidation distance lives and a
// stepped price is the difference between watching a market and watching a
// slideshow. Everything else stays at 120ms -- those feeds are naturally
// slower than the gate anyway (RENDER ticks every 6s), so tightening them
// would buy nothing and cost bandwidth on every connected client.
const SSE_MIN_GAP_MS = 120;
const SSE_FAST_GAP_MS = Number(process.env.PAPER_SSE_FAST_GAP_MS || 40);
const sseGapFor = (sym) => ((STAGE_LEV_CAPS[sym] || 0) >= 1000 ? SSE_FAST_GAP_MS : SSE_MIN_GAP_MS);
// WS mirror of the SSE relay. Buffering middleboxes (corporate proxies, some
// ISPs/VPNs, AV TLS inspection) hold chunked HTTP responses and flush every
// 10-20s — an SSE consumer behind one sees batched steps, a dead-looking
// chart. The same boxes pass WebSocket frames through immediately (observed:
// the user's direct Phoenix WS was always live while our SSE trickled).
const indexWsClients = new Set();
function attachIndexWs(httpServer) {
  const WebSocket = require('ws');
  const wss = new WebSocket.Server({ noServer: true });
  httpServer.on('upgrade', (req, socket, head) => {
    let path = '';
    try { path = new URL(req.url, 'http://x').pathname; } catch {}
    // other WS routes (perps live tape) have their own upgrade listeners —
    // only destroy paths nobody owns
    if (path !== '/api/paper/index-ws') { if (path !== '/api/perps/live-ws') socket.destroy(); return; }
    if (indexWsClients.size >= PYTH_SSE_MAX_CLIENTS) { socket.destroy(); return; }
    wss.handleUpgrade(req, socket, head, (ws) => {
      indexWsClients.add(ws);
      for (const [sym, m] of live.map) {
        if (Number(m.pythPrice) > 0) { try { ws.send(`{"s":"${sym}","p":${m.pythPrice},"t":${m.pythAtMs}}`); } catch {} }
      }
      ws.on('close', () => indexWsClients.delete(ws));
      ws.on('error', () => indexWsClients.delete(ws));
    });
  });
  setInterval(() => { for (const ws of indexWsClients) { try { ws.ping(); } catch {} } }, 25_000).unref();
}
function pythSseBroadcast(sym, px, ts) {
  if (!pythSseClients.size && !indexWsClients.size) return;
  if (ts - (_sseLastBySym.get(sym) || 0) < sseGapFor(sym)) return;
  _sseLastBySym.set(sym, ts);
  const payload = `{"s":"${sym}","p":${px},"t":${ts}}`;
  const line = `data: ${payload}\n\n`;
  for (const res of pythSseClients) {
    try { res.write(line); } catch { pythSseClients.delete(res); }
  }
  for (const ws of indexWsClients) {
    if (ws.readyState === 1 && ws.bufferedAmount < 65536) { try { ws.send(payload); } catch { indexWsClients.delete(ws); } }
  }
}
setInterval(() => {   // keep-alive comment so proxies don't reap idle streams
  for (const res of pythSseClients) { try { res.write(': ping\n\n'); } catch { pythSseClients.delete(res); } }
}, 15_000).unref();

// GET /api/paper/pyth-history?symbol= — raw oracle ticks (ascending [tsMs, px])
function pythHistory(req, res, u_url) {
  if (_readRateOk && !_readRateOk(req.headers['x-real-ip'] || req.socket.remoteAddress)) {
    return send(res, 429, { ok: false, error: 'rate_limited' });
  }
  const symbol = String(u_url.searchParams.get('symbol') || '').toUpperCase();
  if (!symbol || !/^[A-Z0-9]{1,12}$/.test(symbol)) return send(res, 400, { ok: false, error: 'bad_symbol' });
  send(res, 200, { ok: true, symbol, rows: pythHist.get(symbol) || [] });
}

// ── stage candles ────────────────────────────────────────────────────────────
// The candle chart was drawing /api/phoenix/candles (VENUE prices) while stage
// marks, fills and liquidations all run on the CEX index -- so the series and
// the live price came from different sources and stepped at the boundary
// between the last completed (venue) bar and the index-updated bar in progress.
//
// NOTE on magnitude: an earlier version of this comment claimed 39bps of
// venue-vs-index basis. That was a bad measurement -- a COMPLETED 15m close
// compared against a LIVE index price, so most of it was elapsed time. Measured
// like-for-like at one instant, Phoenix mark 64,344 vs Binance 64,336.78 is
// ~1.1bps, and the 24h change differs by 0.03pp. The seam was real; the basis
// is small. Fixing it is still right because one price world beats two.
//
// Binance klines are the closest history we can get to the composite: Binance is
// two of its three components, so klines sit ~8-10bps away (the stablecoin
// basis) rather than 39, and they carry unlimited history. Our own index_ticks
// are exact but only 53h deep, which cannot feed 4h/1d.
const _klineCache = new Map();       // "SYM|tf" -> { at, rows }
const KLINE_TTL_MS = 20_000;         // candles move slowly; protects Binance limits
const KLINE_TF = new Set(['1m', '5m', '15m', '1h', '4h', '1d']);

function fetchKlines(pair, tf, limit, endTime) {
  return new Promise((resolve) => {
    // endTime pages BACKWARDS: Binance returns the `limit` bars ending at it.
    const path = `/api/v3/klines?symbol=${pair}&interval=${tf}&limit=${limit}`
      + (endTime ? `&endTime=${endTime}` : '');
    const req = require('https').request({ host: 'api.binance.com', path, method: 'GET', timeout: 8000 }, (r) => {
      let d = '';
      r.on('data', (c) => d += c);
      r.on('end', () => { try { const j = JSON.parse(d); resolve(Array.isArray(j) ? j : null); } catch { resolve(null); } });
    });
    req.on('error', () => resolve(null));
    req.on('timeout', () => { req.destroy(); resolve(null); });
    req.end();
  });
}

async function stageCandles(req, res) {
  if (_readRateOk && !_readRateOk(req.headers['x-real-ip'] || req.socket.remoteAddress)) {
    return send(res, 429, { ok: false, error: 'rate_limited' });
  }
  const u = new URL(req.url, 'http://x');
  const symbol = String(u.searchParams.get('symbol') || '').toUpperCase();
  const tf = String(u.searchParams.get('timeframe') || u.searchParams.get('tf') || '15m');
  const limit = Math.min(1000, Math.max(10, Number(u.searchParams.get('limit')) || 300));
  // Only indexed symbols have a CEX price world; everything else legitimately
  // belongs on the venue candles and the caller should not have come here.
  if (!STAGE_INDEXED.has(baseOf(symbol)) || !KLINE_TF.has(tf)) return send(res, 400, { ok: false, error: 'unsupported' });

  // Paging back through history. Without it the chart could only ever show the
  // one window it booted with, which is what "why can't I scroll more data?"
  // was: the data exists, we just never asked for it.
  const endTime = Math.max(0, Number(u.searchParams.get('endTime')) || 0);
  const key = `${symbol}|${tf}|${limit}|${endTime}`;
  const hit = _klineCache.get(key);
  const ttl = endTime ? 6 * 3600_000 : KLINE_TTL_MS;   // closed history is immutable
  if (hit && Date.now() - hit.at < ttl) return send(res, 200, { ok: true, rows: hit.rows, source: 'binance-cache' });

  const raw = await fetchKlines(BINANCE_STREAMS[symbol].toUpperCase(), tf, limit, endTime);
  if (!raw) {
    // serve stale over nothing: a blank chart is worse than a slightly old one
    if (hit) return send(res, 200, { ok: true, rows: hit.rows, source: 'binance-stale' });
    return send(res, 502, { ok: false, error: 'klines_unavailable' });
  }
  // Same shape the venue endpoint returns, so the chart consumes it unchanged.
  // mark* mirrors OHLC: on the index there is no separate mark price.
  const rows = raw.map((k) => {
    const o = Number(k[1]), h = Number(k[2]), l = Number(k[3]), c = Number(k[4]);
    return { time: Number(k[0]), open: o, high: h, low: l, close: c,
      markOpen: o, markHigh: h, markLow: l, markClose: c, volume: Number(k[5]) };
  }).filter((r) => r.close > 0);
  _klineCache.set(key, { at: Date.now(), rows });
  if (_klineCache.size > 400) _klineCache.delete(_klineCache.keys().next().value);
  return send(res, 200, { ok: true, rows, source: 'binance' });
}

function engineConfig(req, res) {
  const markets = {};
  for (const [sym, c] of mktCfg) {
    // stageLev is the REAL ceiling in stage and differs per symbol (1000 on the
    // majors, 250 on small caps, 50 on MET). Without it the client hardcoded a
    // 1000x boost, the server clamped to the symbol's cap, and the doubled
    // margin requirement surfaced as a misleading "not enough free margin".
    markets[sym] = { maxLev: c.maxLev, stageLev: stageLevCap(sym), indexed: STAGE_INDEXED.has(sym), takerBps: c.takerBps, makerBps: c.makerBps, isolatedOnly: c.isolatedOnly, status: c.status };
  }
  send(res, 200, { ok: true, levCap: PAPER_MAX_LEV || null, markets });
}

// POST /api/paper/guest — mint a throwaway identity so the terminal is usable
// without signing in. Everything downstream (account creation, orders, risk,
// history) is unchanged; the guest simply IS a user from here on.
//
// Rate limited per IP: a guest costs a users row plus, once they trade, a live
// position that the engine now risk-evaluates on EVERY accepted tick. That is
// the real cost of this feature, not the row.
const _guestByIp = new Map();          // ip -> [timestamps]
// 5/hour locked out my own test runs within minutes, and every one of them came
// from a single IP -- which is exactly what an office, a VPN or a conference
// wifi looks like. The limit is there to bound cost, not to ration the product,
// and a guest costs one row until it trades.
const GUEST_PER_IP_HOUR = Number(process.env.PAPER_GUEST_PER_IP_HOUR || 40);
async function guestSession(req, res) {
  const ip = req.headers['x-real-ip'] || req.socket.remoteAddress || '?';
  const now = Date.now();
  const hits = (_guestByIp.get(ip) || []).filter((t) => now - t < 3600_000);
  if (hits.length >= GUEST_PER_IP_HOUR) {
    return send(res, 429, { ok: false, error: 'guest_limit', retryAfterMins: 60 });
  }
  // already signed in (guest or real): hand back the existing identity rather
  // than stacking accounts on one browser
  const bodyRaw = await readBody(req);
  const existing = await sessionUser(req);
  if (existing) return send(res, 200, { ok: true, guest: auth.isGuestUser(existing.id), reused: true });
  // Name is taken up front now rather than left blank: an account with no name
  // cannot be told apart on a leaderboard, and asking at creation is one field
  // at the only moment the answer is obviously needed.
  let name = null;
  try { name = String(JSON.parse(bodyRaw || '{}').name || '').trim().slice(0, 24); } catch {}
  if (name && !/^[\w .'-]{2,24}$/.test(name)) return send(res, 400, { ok: false, error: 'bad_name' });
  let u;
  try { u = auth.createGuestUser(); } catch (e) { return send(res, 500, { ok: false, error: 'guest_create_failed' }); }
  if (name) {
    // collisions get a numeric suffix rather than an error: a guest should
    // never be stopped at the door over a name someone else already took
    let candidate = name;
    for (let i = 2; i <= 99 && auth.isDisplayNameTaken(candidate, u.id); i++) candidate = `${name} ${i}`;
    try { auth.setDisplayName(u.id, candidate); } catch {}
  }
  // Stamp the account in the mode the caller is actually in. Without this a
  // guest on /ftpaper lands on a STANDARD account and the terminal shows
  // "stage pricing pending" -- venue prices at 100x, which is not the product
  // they were shown. Only 'heat' is honoured; anything else stays standard.
  let mode = null;
  try { mode = (JSON.parse(bodyRaw || '{}').mode) || null; } catch {}   // body already consumed above; a second read hangs the request
  try {
    const now2 = Date.now();
    stmt.acctIns.run(u.id, now2, now2);
    if (mode === 'heat') {
      db.prepare('UPDATE paper_accounts SET heat = 1, start_balance = ?, balance = ? WHERE user_id = ?')
        .run(HEAT_BALANCE, HEAT_BALANCE, u.id);
    }
  } catch (e) { _log(`guest account stamp failed: ${e.message}`); }
  const tok = auth.createSession(u.id, { ip, userAgent: req.headers['user-agent'] });
  auth.setSessionCookie(res, tok && tok.token ? tok.token : tok);
  hits.push(now); _guestByIp.set(ip, hits);
  if (_guestByIp.size > 5000) _guestByIp.clear();
  _log(`guest session created (user ${u.id})`);
  return send(res, 200, { ok: true, guest: true, reused: false });
}

async function leaderboard(req, res) {
  if (_readRateOk && !_readRateOk(req.headers['x-real-ip'] || req.socket.remoteAddress)) {
    return send(res, 429, { ok: false, error: 'rate_limited' });
  }
  let mode = 'standard';
  try { if (new URL(req.url, 'http://x').searchParams.get('mode') === 'stage') mode = 'stage'; } catch {}
  let lb = _lbs[mode];
  if (Date.now() - lb.ts > 5_000 || !lb.rows) { await rebuildLb(mode); lb = _lbs[mode]; }
  let me = null;
  const su = await sessionUser(req);
  if (su) {
    const idx = lb.rows.findIndex((r) => r.userId === su.id);
    if (idx >= 0) me = { rank: idx + 1, equity: lb.rows[idx].equity, returnPct: lb.rows[idx].returnPct };
  }
  return send(res, 200, { ok: true, mode, updatedAt: lb.ts, total: lb.rows.length, rows: lb.top, me });
}

// ── sweep ────────────────────────────────────────────────────────────────
// Limits (print-through preferred), funding, SL/TP, order-cancel tier,
// cross + isolated liquidations. One transaction per tick; per-item
// try/catch so one poisoned row can't stall the engine.
let _sweepRunning = false;
let _lastPruneMs = 0;
function sweep() {
  if (_sweepRunning) return;
  _sweepRunning = true;
  try {
    snapFile();
    if (!pricesUp()) return;
    const now = Date.now();
    db.transaction(() => {
      // 1) resting limit orders
      for (const o of stmt.ordOpenAll.all()) {
        try {
          const m = mkt(o.symbol);
          if (!mktFresh(m)) {
            const quiet = m ? now - (m.lastUpdatedMs || 0) : Infinity;
            if (quiet > DELIST_MS) stmt.ordClose.run('CANCELLED', now, o.id);
            continue;
          }
          const mark = effMark(m);
          // eager orphan cleanup: a reduce-only order whose position is gone
          // (or flipped) can never fill — cancel it now, don't let it linger
          // until price crosses it
          if (o.reduce_only) {
            const pos0 = stmt.posGet.get(o.user_id, o.symbol);
            if (!pos0 || (o.side === 'BUY') === (pos0.side === 'LONG')) { stmt.ordClose.run('CANCELLED', now, o.id); continue; }
          }
          // price-time realism: when the market has a live on-chain tape, a
          // resting limit fills only against REAL counterparty volume that
          // printed THROUGH its price, capped to that volume (partial fills
          // accumulate across sweeps via the vol_ts watermark). Mark-cross is
          // the fallback for thin markets / warehouse outages and fills whole.
          let size, tapePartial = false;
          if (tapeActive(o.symbol)) {
            const wm = o.vol_ts || o.created_at;
            const vol = printedVolumeThrough(o.symbol, o.side, o.price, wm);
            const cap = snapLots(o.symbol, vol);
            if (!(cap > 0)) continue;                 // crossed but no fillable volume yet
            size = Math.min(o.size, cap);
            tapePartial = size < o.size - 1e-9;
          } else {
            if (!(o.side === 'BUY' ? mark <= o.price : mark >= o.price)) continue;
            size = o.size;
          }
          const acct = ensureAccount(o.user_id);
          const pos = stmt.posGet.get(o.user_id, o.symbol);
          if (o.reduce_only) {
            if (!pos || (o.side === 'BUY') === (pos.side === 'LONG')) { stmt.ordClose.run('CANCELLED', now, o.id); continue; }
            size = Math.min(size, pos.size);
            if (!(size > 0)) continue;
            tapePartial = tapePartial && size < o.size - 1e-9;
          } else {
            let addedNotional = size * o.price;
            if (pos && (o.side === 'BUY') !== (pos.side === 'LONG')) addedNotional = Math.max(0, (size - pos.size) * o.price);
            // free collateral with this order's own reservation excluded;
            // require the maker fee on top of the margin
            const riskEx = accountRisk(o.user_id, acct, { excludeOrderId: o.id });
            if (addedNotional / o.leverage + size * o.price * (cfgOf(o.symbol).makerBps / 1e4) > riskEx.free + 1e-9) { stmt.ordClose.run('CANCELLED', now, o.id); continue; }
          }
          if (tapePartial) {
            // consume this slice, keep the order resting for the remainder;
            // advance the watermark so these prints aren't counted again
            stmt.ordPartial.run(r6(o.size - size), now, o.id);
          } else {
            stmt.ordClose.run('FILLED', now, o.id);
          }
          applyFill(o.user_id, { symbol: o.symbol, orderSide: o.side, size, px: o.price, feeBps: cfgOf(o.symbol).makerBps, kind: 'LIMIT', orderId: o.id, leverage: o.leverage, marginMode: o.margin_mode || 'cross' });
          applyAttachedTriggers(o, o.price);
        } catch (e) { _log(`sweep order ${o.id} error: ${e.message}`); }
      }
      // 2) positions: marks, funding, SL/TP, isolated liquidation, delist
      for (const p of stmt.posAll.all()) {
        try {
          const m = mkt(p.symbol);
          if (!mktFresh(m)) {
            const quietSince = m ? (m.lastUpdatedMs || 0) : 0;
            if ((quietSince && now - quietSince > DELIST_MS) || (!m && now - (p.updated_at || now) > DELIST_MS)) {
              if (Number(p.last_mark) > 0) {
                applyFill(p.user_id, { symbol: p.symbol, orderSide: p.side === 'LONG' ? 'SELL' : 'BUY', size: p.size, px: Number(p.last_mark), feeBps: 0, kind: 'DELIST' });
                _log(`delist-closed ${p.symbol} for user ${p.user_id}`);
              }
            }
            continue;
          }
          evalPositionAtMark(p, m, now);
        } catch (e) { _log(`sweep position ${p.user_id}/${p.symbol} error: ${e.message}`); }
      }
      // 3) cross risk tiers per user: order-cancel first, then liquidation
      const users = new Set(stmt.posAll.all().map((p) => p.user_id));
      for (const uid of users) {
        try { evalCrossForUser(uid, now); } catch (e) { _log(`sweep user ${uid} error: ${e.message}`); }
      }
      if (now - _lastPruneMs > 3600_000) {
        _lastPruneMs = now;
        try { const r = stmt.fillPrune.run(now - FILLS_RETENTION_MS); if (r.changes) _log(`pruned ${r.changes} old fills`); } catch {}
        try { const r = stmt.ordPrune.run(now - FILLS_RETENTION_MS); if (r.changes) _log(`pruned ${r.changes} closed orders`); } catch {}
      }
    })();
    // Book lifecycle (outside the tx: pure WS bookkeeping). Keep L2 streams
    // warm for every symbol carrying paper risk so SL/liq fills price off a
    // live book; drop books nobody has needed for BOOK_IDLE_MS.
    try {
      const activeSyms = new Set();
      for (const p of stmt.posAll.all()) activeSyms.add(p.symbol);
      for (const o of stmt.ordOpenAll.all()) activeSyms.add(o.symbol);
      for (const sym of activeSyms) ensureBook(sym);
      for (const [sym, lastUsed] of books.subs) {
        if (!activeSyms.has(sym) && now - lastUsed > BOOK_IDLE_MS) dropBook(sym);
      }
    } catch {}
  } catch (e) {
    _log('sweep error: ' + e.message);
  } finally {
    _sweepRunning = false;
  }
}

module.exports = { init, sweep, account, comp, compState, compVerify, compAdmin, guestSession, stageCandles, placeOrder, cancelOrder, closePosition, setSltp, adjustMargin, reset, fills, ordersHistory, leaderboard, engineConfig, marketTape, pythHistory, pythStream, attachIndexWs };
// Internal handles for the test harness / FT port test suite. Read-mostly;
// requiring the module does not start the WS or any timers (that is init's job).
module.exports.__test = { prints, live, mktCfg, stmt, db, sweep, tickEval, scoreUser, accountRisk, baseOf, aliasKind, aliasOpen, openAliases, openAlias, closeAlias, cfgOf, stageLevCap, mkt, ingestPrint, printedVolumeThrough, books, writeRate: _writeRate, guardCheck, comps: _idxComps, halt: _halt, ingestIndexTick, compUpdate };
