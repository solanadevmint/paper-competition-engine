'use strict';
// Paper trading — simulated Phoenix perps on a virtual $10k USDC account.
// One position per (user, market), cross OR isolated margin per position.
// State lives in paper.db on the engine box (SQLite/WAL; see auth-shim
// DB_FILE, default /opt/phoenix-paper/paper.db) — never an in-memory blob.
// Equity is always derived, never stored.
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
// through the limit price; each account is a counterfactual simulation of
// that tape, not a participant in one shared paper queue. Mark-cross is the
// fallback for markets with no recent prints or when the warehouse is down.
//
// PHOENIX-INFORMED SIMULATION MODEL (from /exchange config + 264k real fills):
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
//  - Funding approximation: currentFundingRate is PERCENT per hour (verified
//    against maxFundingRatePerIntervalPercentage caps); the current sample is
//    applied at each UTC hour boundary. It is not Phoenix's continuous
//    accumulator and is disclosed as hourly-current-rate-sample in the API.
//    Positive rate: longs pay.
//  - marketStatus: 'active' required for taker fills; 'postOnly' (SKHY)
//    accepts resting limits only. Closes always allowed.
const crypto = require('crypto');
const fs = require('fs');
const auth = require('./auth-shim.js');
const comp = require('./competition.js');
const bots = require('./bots.js');
const db = auth.db;
let WebSocket = null;
try { WebSocket = require('ws'); } catch {}

const START_BALANCE = 10000;
/* Public contract metadata. PAPER_BUILD_ID is injected by the server from the
   immutable deploy manifest; direct test/dev loads intentionally report null.
   Versions are numeric so clients can compare them without parsing labels. */
const PAPER_API_VERSION = 1;
const PAPER_SCHEMA_VERSION = 2;
const PAPER_BUILD_ID = String(process.env.PAPER_BUILD_ID || '').trim().slice(0, 128) || null;
const PAPER_DISPLAY_SCALE = 10_000;
const FALLBACK_TAKER_BPS = 3.5;
const FALLBACK_MAKER_BPS = 0.5;
const FALLBACK_MAINT_BPS = 5000;
const FALLBACK_CANCEL_BPS = 7500;
const SNAPSHOT_FILE = process.env.PHOENIX_SNAPSHOT_FILE || '/var/lib/phoenix-paper/markets-snapshot.json';
const MAINTENANCE_FILE = process.env.PAPER_MAINTENANCE_FILE || '/var/lib/phoenix-paper/deploy-maintenance';
/* A deployment is a transaction over code AND financial state. While the
   deploy sentinel exists, the candidate may ingest prices to prove readiness,
   but it must not change an account, order, round, funding entry or risk
   outcome that a rollback would later erase. Treat every error other than a
   definite ENOENT as maintenance: an unreadable barrier fails closed. */
function deploymentMaintenanceActive() {
  /* Missing is the ordinary state, checked for every candidate risk tick.
     Avoid allocating an ENOENT Error/stack on that hot path. This is still a
     fresh filesystem check on EVERY call: no cached lease can let a fill
     cross the instant an operator creates the deployment barrier. */
  try { return fs.statSync(MAINTENANCE_FILE, { throwIfNoEntry: false }) !== undefined; }
  catch (e) { return !(e && e.code === 'ENOENT'); }
}
const SNAP_TTL_MS = 1_000;
const WS_URL = 'wss://perp-api.phoenix.trade/v1/ws';
const WS_FRESH_MS = 15_000;
const MARKET_FRESH_MS = 30_000;
const DELIST_MS = 3600_000;
const RESET_COOLDOWN_MS = 10 * 60_000;
const MAX_OPEN_ORDERS = 20;
const MIN_NOTIONAL = 10;
/* Heat-mode bankroll in ENGINE dollars. The room sees it multiplied by the
   display scale, which is where the headline figure is set: at 10,000x this
   $10 book is the $100,000 account on every screen (owner's call,
   2026-09-04). Raising this constant instead would move the engine's real
   notionals, lot rounding and margin thresholds, which the suite pins. */
const HEAT_BALANCE = 10;
const PAPER_MODES = Object.freeze({
  standard: Object.freeze({ engineBankroll: START_BALANCE, displayScale: 1, fees: true, funding: true, fundingModel: 'hourly-current-rate-sample', execution: 'phoenix-book', liquidityModel: 'per-user-counterfactual', queueModel: 'print-through-approximation', sharedLiquidity: false, venueParity: false }),
  stage: Object.freeze({ engineBankroll: HEAT_BALANCE, displayScale: PAPER_DISPLAY_SCALE, fees: false, funding: false, fundingModel: 'none', execution: 'composite-index', liquidityModel: 'unbounded-index', queueModel: 'none', sharedLiquidity: false, venueParity: false, marginModes: Object.freeze(['isolated']) }),
  scaled: Object.freeze({ engineBankroll: HEAT_BALANCE, displayScale: PAPER_DISPLAY_SCALE, fees: true, funding: true, fundingModel: 'hourly-current-rate-sample', execution: 'phoenix-book', liquidityModel: 'per-user-counterfactual', queueModel: 'print-through-approximation', sharedLiquidity: false, venueParity: false }),
});
const HEAT_MAX_LEV = 500;         // heat/Boost cap — $10×500x=$5k notional, fills top-of-book
const BOOST_WINDOW_MS = 2 * 60_000;   // a Boost-class fill starts a 2-min clock; then the position auto-flattens
const BOOST_ARM_LEV = 101;            // anything ABOVE the 100x baseline is Boost-class — 999x can't dodge the clock
const HEAT_MIN_NOTIONAL = 0.01;   // real dollars; scaled floor is $100 on screen
const MAX_NOTIONAL = 5_000_000;
// Stage has no orderbook: fills execute AT the index with zero slippage and
// zero fees, so there is no book to exhaust and the 5M cap models nothing.
// Size is already bounded by margin (notional <= free * leverage), which is the
// real constraint -- a displayed $100k account at 500x can reach a displayed
// $50M and no further.
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
  CREATE INDEX IF NOT EXISTS idx_pord_open_symbol ON paper_orders(symbol) WHERE status = 'OPEN';
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
  CREATE TABLE IF NOT EXISTS paper_order_requests (
    user_id INTEGER NOT NULL, epoch INTEGER NOT NULL, request_id TEXT NOT NULL,
    payload_hash TEXT NOT NULL, response_json TEXT NOT NULL, created_at INTEGER NOT NULL,
    PRIMARY KEY (user_id, epoch, request_id), UNIQUE (user_id, request_id)
  );
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
  // A resting order must preserve the Boost-window choice made at placement.
  // Existing rows pre-date the flag and retain the historical default (armed).
  'ALTER TABLE paper_orders ADD COLUMN boost_window INTEGER NOT NULL DEFAULT 1',
  // A resting order whose requested protection becomes impossible at its
  // actual fill price is rejected before exposure changes. Keep that reason
  // on the order itself so the failure is visible after the sweep/log rotates.
  'ALTER TABLE paper_orders ADD COLUMN close_reason TEXT',
  // heat mode: FT stage accounts — tiny real bankroll (display-scaled in the
  // UI), zero fees/funding, fractional lots. See ft-world-cup engine handoff.
  'ALTER TABLE paper_accounts ADD COLUMN heat INTEGER NOT NULL DEFAULT 0',
  'ALTER TABLE paper_accounts ADD COLUMN start_balance REAL',  // was a manual prod ALTER; fresh installs crashed without it
  // exact ledger identity across busts: balance = start + Σrealized - Σfees
  // + Σfunding + Σbad_debt. Fills record GROSS realized; the clamp goes here.
  'ALTER TABLE paper_fills ADD COLUMN bad_debt REAL NOT NULL DEFAULT 0',
  // Durable execution provenance: enough to join a fill to the engine build,
  // accepted index event and execution model during a dispute or replay.
  'ALTER TABLE paper_fills ADD COLUMN execution_source TEXT',
  'ALTER TABLE paper_fills ADD COLUMN reference_mark REAL',
  'ALTER TABLE paper_fills ADD COLUMN engine_boot TEXT',
  'ALTER TABLE paper_fills ADD COLUMN index_seq INTEGER',
  // Bounded JSON explaining the inputs and risk transition that produced the
  // fill. This complements the indexed provenance columns without requiring
  // a live engine or mutable in-memory feed state during a later dispute.
  'ALTER TABLE paper_fills ADD COLUMN decision_context TEXT',
  // Boost window: set when a fill above the 100x base lands on the position; the position
  // auto-flattens BOOST_WINDOW_MS later (kind EXPIRY). Never cleared by
  // adds/reduces — using Boost starts a clock the position cannot shed.
  'ALTER TABLE paper_positions ADD COLUMN boost_since INTEGER',
]) { try { db.exec(ddl); } catch {} }

/* A completed v2 score proof turns the portion of the fill ledger it hashed
   into audit evidence. The account can trade again in a later epoch, but no
   row at or before the proved bell may be rewritten, removed, or inserted
   retroactively. The digest is still recomputed by /comp/verify; these
   triggers prevent an ordinary maintenance query from damaging the proof in
   the first place. */
try {
  db.exec(`CREATE TRIGGER IF NOT EXISTS paper_scored_fill_immutable_update
    BEFORE UPDATE ON paper_fills
    WHEN EXISTS (SELECT 1 FROM paper_round_score_proofs p
      WHERE p.checkpoint = 'final' AND p.user_id = OLD.user_id
        AND p.epoch = OLD.epoch AND OLD.ts <= p.as_of)
      OR EXISTS (SELECT 1 FROM paper_round_score_proofs p
      WHERE p.checkpoint = 'final' AND p.user_id = NEW.user_id
        AND p.epoch = NEW.epoch AND NEW.ts <= p.as_of)
    BEGIN SELECT RAISE(ABORT, 'settled competition fill is immutable'); END;
    CREATE TRIGGER IF NOT EXISTS paper_scored_fill_immutable_delete
    BEFORE DELETE ON paper_fills
    WHEN EXISTS (SELECT 1 FROM paper_round_score_proofs p
      WHERE p.checkpoint = 'final' AND p.user_id = OLD.user_id
        AND p.epoch = OLD.epoch AND OLD.ts <= p.as_of)
    BEGIN SELECT RAISE(ABORT, 'settled competition fill is immutable'); END;
    CREATE TRIGGER IF NOT EXISTS paper_scored_fill_no_backdated_insert
    BEFORE INSERT ON paper_fills
    WHEN EXISTS (SELECT 1 FROM paper_round_score_proofs p
      WHERE p.checkpoint = 'final' AND p.user_id = NEW.user_id
        AND p.epoch = NEW.epoch AND NEW.ts <= p.as_of)
    BEGIN SELECT RAISE(ABORT, 'cannot backdate a fill into a settled competition'); END;`);
} catch (e) {
  console.error('[paper] settled-fill immutability trigger unavailable:', e.message);
}

/* An invite is an actionable bearer only while its round is armed. Keeping
   raw tokens on completed/aborted/running rows added no operational value but
   copied those capabilities into every backup forever. Armed tokens remain
   retrievable by the desk; all expired ones are irreversibly invalidated. */
function purgeExpiredInviteTokens() {
  return db.prepare(`UPDATE paper_round_players SET invite_token = NULL
                     WHERE invite_token IS NOT NULL AND round_id IN
                       (SELECT id FROM paper_rounds WHERE status != 'armed')`).run().changes;
}
function purgeInviteCapabilities(reason) {
  try {
    const n = purgeExpiredInviteTokens();
    if (n) _log(`expired invite capabilities purged (${reason || 'lifecycle'}): ${n}`);
    return n;
  } catch (e) {
    // The lifecycle transition has already committed and cannot honestly be
    // reported as failed. Make the containment failure loud; the next
    // lifecycle action or startup retries idempotently.
    _log(`INVITE CAPABILITY PURGE FAILED (${reason || 'lifecycle'}): ${e && e.message}`);
    return -1;
  }
}

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
  ordInsWithBoost: db.prepare(`INSERT INTO paper_orders (user_id, epoch, symbol, side, price, size, leverage, reduce_only, created_at, margin_mode, attach_sl, attach_tp, boost_window)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`),
  ordClose:   db.prepare('UPDATE paper_orders SET status = ?, closed_at = ? WHERE id = ?'),
  ordReject:  db.prepare("UPDATE paper_orders SET status = 'REJECTED', closed_at = ?, close_reason = ? WHERE id = ?"),
  // partial fill of a resting limit: shrink remaining size, advance the volume
  // watermark so the same prints are never consumed twice across sweeps
  ordPartial: db.prepare('UPDATE paper_orders SET size = ?, vol_ts = ? WHERE id = ?'),
  ordCancelUser: db.prepare("UPDATE paper_orders SET status = 'CANCELLED', closed_at = ? WHERE user_id = ? AND status = 'OPEN'"),
  fillIns:    db.prepare(`INSERT INTO paper_fills (user_id, epoch, symbol, side, kind, price, size, notional, fee, realized_pnl, order_id, ts, bad_debt)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`),
  fillInsWithProvenance: db.prepare(`INSERT INTO paper_fills (user_id, epoch, symbol, side, kind, price, size, notional, fee, realized_pnl, order_id, ts, bad_debt, execution_source, reference_mark, engine_boot, index_seq)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`),
  fillInsAudited: db.prepare(`INSERT INTO paper_fills (user_id, epoch, symbol, side, kind, price, size, notional, fee, realized_pnl, order_id, ts, bad_debt, execution_source, reference_mark, engine_boot, index_seq, decision_context)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`),
  posBySymbol: db.prepare('SELECT * FROM paper_positions WHERE symbol = ?'),
  clockExposure: db.prepare(`SELECT DISTINCT p.symbol, p.leverage
    FROM paper_round_players AS r JOIN paper_positions AS p ON p.user_id = r.user_id
    WHERE r.round_id = ?`),
  /* A base tick has to reach the aliases that price off it. Alias positions
     are stored under their own symbol, so an exact match on 'BTC' never sees
     'BTC-BOOST' — whose high-leverage liquidation distance was otherwise only
     checked by the 5s sweep. Both twins are listed explicitly rather than
     pattern-matched, so the index is still used. */
  posBySymbolTree: db.prepare('SELECT * FROM paper_positions WHERE symbol IN (?, ?, ?)'),
  ordOpenBySymbol: db.prepare("SELECT * FROM paper_orders WHERE symbol = ? AND status = 'OPEN'"),
  ordCancelSymbol: db.prepare("UPDATE paper_orders SET status = 'CANCELLED', closed_at = ? WHERE symbol = ? AND status = 'OPEN'"),
  fillList:   db.prepare('SELECT * FROM paper_fills WHERE user_id = ? AND epoch = ? AND id < ? ORDER BY id DESC LIMIT ?'),
  fillListAll: db.prepare('SELECT * FROM paper_fills WHERE user_id = ? AND id < ? ORDER BY id DESC LIMIT ?'),
  fillListTrades:  db.prepare("SELECT * FROM paper_fills WHERE user_id = ? AND epoch = ? AND kind != 'FUNDING' AND id < ? ORDER BY id DESC LIMIT ?"),
  fillListFunding: db.prepare("SELECT * FROM paper_fills WHERE user_id = ? AND epoch = ? AND kind = 'FUNDING' AND id < ? ORDER BY id DESC LIMIT ?"),
  /* The wall's liquidation flash: the fills the liquidator wrote this round. */
  liqSince: db.prepare("SELECT ts, symbol, realized_pnl FROM paper_fills WHERE user_id = ? AND epoch = ? AND kind = 'LIQUIDATION' AND ts >= ? ORDER BY id DESC LIMIT 20"),
  fillPrune:  db.prepare(`DELETE FROM paper_fills WHERE ts < ?
    AND NOT EXISTS (SELECT 1 FROM paper_round_score_proofs p
      WHERE p.checkpoint = 'final' AND p.user_id = paper_fills.user_id
        AND p.epoch = paper_fills.epoch AND paper_fills.ts <= p.as_of)`),
  fillPruneProtectedCount: db.prepare(`SELECT COUNT(*) AS n FROM paper_fills f
    WHERE f.ts < ? AND EXISTS (SELECT 1 FROM paper_round_score_proofs p
      WHERE p.checkpoint = 'final' AND p.user_id = f.user_id
        AND p.epoch = f.epoch AND f.ts <= p.as_of)`),
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
    const listed = [];
    for (const mk of (ex && ex.markets) || []) {
      if (!mk.symbol) continue;
      listed.push(String(mk.symbol).toUpperCase());
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
    const off = restrictToIndexed(listed);
    if (off.length) _log(`not offered, listed by the venue but not indexed here: ${off.join(', ')}`);
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
/* The ring feeds the chart's history, so it has to be recorded at the same
   texture the chart draws live, or a reload turns the smooth minute you just
   watched into a staircase. The relay now runs at 40ms on the Lazer majors
   while this was still thinning to 300ms, 7x coarser. Matched at 50ms, with
   the depth cut to what any client actually asks for (the terminal seeds six
   minutes): about 1.2MB per symbol, 29MB across the board. */
/* Monotonic milliseconds. Every cadence, throttle, watchdog and expiry
   decision below uses this; wall time stays for event metadata and audit.
   A backward NTP step used to suppress relay frames until the wall clock
   caught up to the last stamp, and could rewrite the tail of the history
   ring behind its own predecessor. */
const _monoOrigin = process.hrtime.bigint();
const monoNow = () => Number(process.hrtime.bigint() - _monoOrigin) / 1e6;
/* Boot-scoped identity for the event stream: a client that reconnects to a
   restarted engine must not mistake a restarted sequence for continuity. */
const ENGINE_BOOT_ID = require('crypto').randomBytes(4).toString('hex');
let _evSeq = 0;          // events PUBLISHED: contiguous, so a hole means loss
let _evAccepted = 0;     // events ACCEPTED: the engine's own count, thinning included
const PYTH_HIST_MAX = 15000;
const PYTH_HIST_MAX_AGE_MS = 12 * 60_000;
const PYTH_HIST_MIN_GAP_MS = 50;
const pythHist = new Map();            // symbol -> [[tsMs, px], ...] ascending
const _pythHistAnchor = new Map();     // symbol -> ts of last APPENDED row (throttle anchor)
/* Timestamps of history rows written for an observation the engine acted on.
   Bounded with the ring it describes. */
const _pythHistMat = new Set();
function pythHistPush(symbol, ts, px, material = false) {
  let arr = pythHist.get(symbol);
  if (!arr) { arr = []; pythHist.set(symbol, arr); }
  // throttle against the last APPEND, not the last write — measuring the gap
  // from a timestamp we keep sliding forward turns a steady sub-300ms stream
  // into a debounce that only ever commits on pauses (recorded 3-4s "gaps"
  // on a healthy ~5Hz stream)
  const anchor = _pythHistAnchor.get(symbol) || 0;
  const last = arr[arr.length - 1];
  /* The ring is contractually ascending and the in-window branch used to move
     the tail's timestamp to whatever arrived, including backwards after a
     clock step. Out-of-order observations refresh the PRICE and leave the
     time alone; anything older than the tail is dropped. */
  if (last && ts < last[0]) return;
  /* ONE EXTREME PER WINDOW IS ONE TOO FEW WHEN BOTH MOVED SOMEBODY.
   *
   * The window keeps the more extreme of the prices in it, which is right for
   * ordinary quotes and wrong for the tick that actually caused a fill: a spike
   * that liquidated one trader and the reversal that liquidated another, 30ms
   * apart, left one row and the other liquidation had no price behind it in the
   * record. An observation the engine ACTED ON is never folded away, and never
   * overwritten by a later quote in its window. */
  if (last && material) {
    if (ts >= last[0]) { _pythHistAnchor.set(symbol, ts); arr.push([ts, px]); if (arr.length > PYTH_HIST_MAX) arr.splice(0, arr.length - PYTH_HIST_MAX); }
    _pythHistMat.add(symbol + ':' + ts);
    return;
  }
  if (last && ts - anchor < PYTH_HIST_MIN_GAP_MS && _pythHistMat.has(symbol + ':' + last[0])) return;
  if (last && ts - anchor < PYTH_HIST_MIN_GAP_MS) {
    /* Keep the EXTREME of the window, not the newest price in it. Two events
       inside 50ms used to leave only the second, so a wick that liquidated
       somebody could vanish from the history a reload reads back, while the
       fill it caused stayed in the ledger. Extremity is measured against the
       previous window's close. */
    const prev = arr.length > 1 ? arr[arr.length - 2][1] : last[1];
    if (Math.abs(px - prev) >= Math.abs(last[1] - prev)) last[1] = px;
    last[0] = Math.max(last[0], ts);
    return;
  }
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
  /* Bounded with the ring it annotates. */
  for (const k of _pythHistMat) {
    if (Number(k.slice(k.lastIndexOf(':') + 1)) < cut) _pythHistMat.delete(k);
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
function bestOppositePrice(book, orderSide) {
  const levels = orderSide === 'BUY' ? book && book.asks : book && book.bids;
  let best = null;
  for (const lvl of levels || []) {
    const p = Number(lvl && lvl[0]);
    if (!(p > 0)) continue;
    if (best == null || (orderSide === 'BUY' ? p < best : p > best)) best = p;
  }
  return best;
}
function bookCrossesLimit(book, orderSide, limitPrice) {
  const opposite = bestOppositePrice(book, orderSide);
  if (!(opposite > 0)) return null;
  return orderSide === 'BUY' ? opposite <= limitPrice : opposite >= limitPrice;
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
        /* The index overlay is one identity, not just a price and receipt
           timestamp. A venue stats rewrite can land between an accepted mark
           and its score fence; preserve its source deadline and accepted id
           together or the fence can renew a Lazer mark under generic TTL and
           lose the ordering proof used by fail-closed controls. */
        ...(prev ? {
          pythPrice: prev.pythPrice,
          pythAtMs: prev.pythAtMs,
          pythSrcAtMs: prev.pythSrcAtMs,
          pythPubTime: prev.pythPubTime,
          srcKey: prev.srcKey,
          acceptedSeq: prev.acceptedSeq,
          indexHalt: prev.indexHalt,
        } : {}),
        symbol: msg.symbol,
        markPrice: Number(msg.markPrice),
        oraclePrice: Number(msg.oraclePrice),
        currentFundingRate: Number(msg.currentFundingRate),   // PERCENT per hour
        eightHourFundingRate: Number(msg.eightHourFundingRate),
        lastUpdatedMs: Date.now(),
        ...(pythBasis != null ? { pythBasis } : {}),
      });
      // per-tick risk evaluation for this symbol (throttled internally)
      try { tickEval(msg.symbol); } catch {}
      /* THE 54 MARKETS THE INDEX DOES NOT COVER STILL HAVE A PRICE.
       *
       * The venue quotes all 78 markets and the engine has always been able to
       * fill an order in any of them: HYPE, the equities and the commodities
       * all execute against this mark. The relay only ever carried the 24
       * indexed symbols, so on screen those other markets had no price at all,
       * the ticket sat on "Prices Stale" and the button was dead, while the
       * same order placed through the API filled immediately. Publishing the
       * venue mark closes that gap.
       *
       * Its own source key, deliberately. A venue mark is not an index
       * observation: it has no provider timestamp, no corroboration and no
       * competition validity, compPriceReady still refuses it, and a scored
       * round can never open on one. It is a price for a screen and for a
       * public paper fill, which is exactly what it has always been. */
      try {
        /* Into the CHAIN, not around it. This used to publish straight to the
           relay for symbols the index did not cover, which was right when 54
           markets had no index at all. Now every market has one, and the venue
           mark's job is to be the leg that takes over when the index is quiet:
           a market closed for the night, a feed we lost. Going through
           compUpdate means it is clamped, risk-evaluated, recorded and
           published on the same path as everything else, and the source
           selector decides when it is actually in charge. */
        /* A market we do not offer still arrives on the venue socket, and it
           was still being indexed and relayed to every browser: 42 disabled
           symbols at roughly one frame a second each, about a sixth of the
           whole relay, for markets nobody can chart or trade. */
        const px0 = Number(msg.markPrice);
        const venueBase = baseOf(msg.symbol);
        if (px0 > 0 && STAGE_INDEXED.has(venueBase) && !DISABLED_MARKETS.has(venueBase)) compUpdate(msg.symbol, 'venue', px0, Date.now());
      } catch { /* a screen price must never be able to stall the feed */ }
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
const modeNameOf = (h) => isStage(h) ? 'stage' : isScaled(h) ? 'scaled' : 'standard';
function execPxFor(userId, symbol, orderSide, sizeBase, mark) {
  if (isStage(heatOf(userId))) return { px: pricingRoundFor(userId) ? mark : rpx(mark), source: 'composite-index' };
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
// Both feed the same per-symbol store. A one-to-one content reconciliation
// joins the WS copy to its delayed warehouse copy: source-specific ids cannot
// do that because Phoenix sequence numbers and transaction signatures live in
// different namespaces. Each paper account is evaluated counterfactually
// against the observed tape: this is NOT a claim that all simulated accounts
// shared one real queue or counterparty lot. A process-local global debit was
// tried and removed because a restart recreated the volume and could fill the
// next account from the same print. The public mode contract names this model.
const prints = { map: new Map(), seen: new Map(), lastOkMs: 0 };   // symbol -> [{ts, price, size}]
function printSource(key) {
  return String(key || '').startsWith('ws:') ? 'ws'
    : String(key || '').startsWith('wh:') ? 'warehouse' : 'other';
}
function sameEconomicPrint(a, price, ts, size) {
  if (!(a && a.size > 0 && size > 0)) return false;
  if (Math.abs(Number(a.ts) - Number(ts)) > 2_000) return false;
  const pScale = Math.max(1, Math.abs(Number(a.price)), Math.abs(Number(price)));
  const sScale = Math.max(1, Math.abs(Number(a.size)), Math.abs(Number(size)));
  return Math.abs(Number(a.price) - Number(price)) <= pScale * 1e-9
    && Math.abs(Number(a.size) - Number(size)) <= sScale * 1e-9;
}
function ingestPrint(symbol, price, ts, key, size = 0) {
  if (!(price > 0)) return;
  const now = Date.now();
  if (prints.seen.has(key)) return;
  prints.seen.set(key, now);
  prints.lastOkMs = now;
  let arr = prints.map.get(symbol);
  if (!arr) prints.map.set(symbol, arr = []);
  const nSize = Number(size) > 0 ? Number(size) : 0;
  const source = printSource(key);
  /* Reconcile only ACROSS sources and only one-to-one. If two identical lots
     genuinely trade in the same second, the first warehouse copy joins the
     first WS copy and the second joins the second; neither is collapsed. */
  if (source === 'ws' || source === 'warehouse') {
    const counterpart = source === 'ws' ? 'warehouse' : 'ws';
    const twin = arr.find((p) => p.sources && p.sources.has(counterpart)
      && !p.sources.has(source) && sameEconomicPrint(p, price, ts, nSize));
    if (twin) {
      twin.sources.add(source);
      twin.keys.add(String(key));
      return;
    }
  }
  arr.push({
    ts: Number(ts), price: Number(price), size: nSize,
    sources: new Set([source]), keys: new Set([String(key)]),
  });
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
// `afterTs` (that order's durable consumption watermark). This is deliberately
// per-order/per-account counterfactual volume, not a shared simulated queue.
function printEligibilityThrough(symbol, side, price, afterTs) {
  const arr = prints.map.get(symbol);
  if (!arr) return { volume: 0, newestTs: Number(afterTs) || 0, printCount: 0 };
  let volume = 0, newestTs = Number(afterTs) || 0, printCount = 0;
  for (const p of arr) {
    if (p.ts <= afterTs) continue;
    if (!(side === 'BUY' ? p.price <= price : p.price >= price)) continue;
    volume += Math.max(0, Number(p.size) || 0);
    newestTs = Math.max(newestTs, Number(p.ts) || 0);
    printCount++;
  }
  return { volume, newestTs, printCount };
}
function printedVolumeThrough(symbol, side, price, afterTs) {
  return printEligibilityThrough(symbol, side, price, afterTs).volume;
}
function restingPriceTime(a, b) {
  const sym = String(a.symbol).localeCompare(String(b.symbol));
  if (sym) return sym;
  const side = String(a.side).localeCompare(String(b.side));
  if (side) return side;
  const px = Number(a.price) - Number(b.price);
  if (Math.abs(px) > 1e-12) return a.side === 'BUY' ? -px : px;
  return (Number(a.created_at) - Number(b.created_at)) || (Number(a.id) - Number(b.id));
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
/* EVERY MARKET THE VENUE LISTS, ON PYTH.
 *
 * Until the Pro grant landed our token was entitled to six feeds, so eighteen
 * alts rode Binance and the other fifty four had no index at all: they were
 * priced from a single venue book with no provider timestamp, which is why
 * they could not carry the top leverage tier. All seventy eight are entitled
 * now, each one verified against the venue's own asset metadata rather than
 * matched on ticker alone. That audit mattered: MET is Meteora here and
 * MetLife in the catalog, and copper and oil are dated futures rather than
 * spot, identified by comparing the live feed against our own mark (copper
 * tracks the December contract to 0.06 percent, oil the September one to 0.19).
 *
 * The tier is the FASTEST channel each feed serves. A subscription that names
 * a feed above its tier is refused in full, so they go out as three
 * subscriptions rather than one. */
/* MARKETS WE DO NOT OFFER.
 *
 * Commodities are off by owner decision (2026-09-03). Pyth prices them as
 * DATED FUTURES rather than spot: our oil is the September 2026 WTI contract,
 * which expires on the 22nd of this month, and copper is the December one.
 * Each expiry needs a deliberate roll to the next contract, and a roll is a
 * discontinuity in the mark. Rather than carry that on a prize engine they
 * come out of the book: no feed, no relay, no index, and the order path
 * refuses to OPEN one. Closing an existing position is always allowed, because
 * turning a market off must never strand somebody in it. */
/* Equities came out on 2026-09-04, the same owner call: this is a crypto
   competition, and an equity brings a trading calendar with it. They only
   price during New York hours, so for most of the day and all weekend the
   engine would be carrying a market that either sits on a venue mark nobody
   can age or goes unpriceable, which is a freeze waiting to happen in a format
   that must not freeze. What is left is the crypto book: 37 markets, all
   quoted around the clock by the same primary. */
const DISABLED_EQUITIES = 'AAPL,AMAT,AMD,AMZN,ARM,ASML,AVGO,BABA,CBRS,COIN,CRCL,CRWD,CRWV,DELL,GOOGL,HOOD,INTC,IREN,LLY,META,MRNA,MRVL,MSFT,MSTR,MU,NBIS,NET,NFLX,NVDA,ORCL,PLTR,QCOM,SKHY,SNDK,SPCX,TSLA,TSM';
/* MET came out on 2026-09-04. It is the market that paused 26 of the 33 round
   pauses in the preceding twelve hours: its book is quiet for more than four
   seconds 19% of the time and its worst routine tick is wider than the
   liquidation distance even at 50x, the lowest cap in the book. A market that
   cannot be priced continuously and cannot carry the base tier is not one to
   run a competition alongside. */
/* This list names markets the engine COULD index and chooses not to. It is
   not the boundary of the book: restrictToIndexed below also turns off every
   venue listing outside the index, so a market can only be on offer by being
   indexed AND absent from here. PAPER_DISABLED_MARKETS replaces this list; the
   index rule still applies on top of it. */
const DISABLED_MARKETS = new Set(String(process.env.PAPER_DISABLED_MARKETS ?? `GOLD,SILVER,COPPER,WTIOIL,MET,${DISABLED_EQUITIES}`)
  .split(',').map((x) => x.trim().toUpperCase()).filter(Boolean));
const LAZER_TIER_CH = { rt: 'real_time', f50: 'fixed_rate@50ms', f200: 'fixed_rate@200ms' };
const LAZER_ALL = {
  AAPL:     [  922, 'f50'],
  AAVE:     [   29, 'f200'],
  ADA:      [   16, 'f200'],
  AMAT:     [  946, 'f200'],
  AMD:      [  949, 'f50'],
  AMZN:     [  954, 'f50'],
  ANSEM:    [ 3418, 'f200'],
  ARM:      [  968, 'f200'],
  ASML:     [  969, 'f200'],
  AVGO:     [  972, 'f200'],
  BABA:     [ 2271, 'f200'],
  BNB:      [   15, 'rt'],
  BTC:      [    1, 'rt'],
  CBRS:     [ 3246, 'f50'],
  CHIP:     [ 3202, 'f200'],
  COIN:     [ 1042, 'f50'],
  COPPER:   [ 2940, 'f50'],
  CRCL:     [ 1683, 'f50'],
  CRV:      [   82, 'f200'],
  CRWD:     [ 1054, 'f200'],
  CRWV:     [ 2737, 'f200'],
  DELL:     [ 1074, 'f200'],
  DOGE:     [   13, 'rt'],
  ENA:      [   85, 'f200'],
  ETH:      [    2, 'rt'],
  FARTCOIN: [  182, 'f50'],
  FET:      [   38, 'f200'],
  GOLD:     [  346, 'f200'],
  GOOGL:    [ 1163, 'f50'],
  HOOD:     [ 1182, 'f50'],
  HYPE:     [  110, 'rt'],
  INTC:     [ 1201, 'f50'],
  IREN:     [ 2363, 'f200'],
  JTO:      [   91, 'f200'],
  JUP:      [   92, 'f200'],
  LINK:     [   19, 'f200'],
  LIT:      [ 2921, 'f200'],
  LLY:      [ 1246, 'f200'],
  MEGA:     [ 3205, 'f200'],
  MET:      [ 2382, 'f200'],
  META:     [ 1272, 'f50'],
  MON:      [ 2396, 'f200'],
  MORPHO:   [  174, 'f200'],
  MRNA:     [ 1288, 'f200'],
  MRVL:     [ 1289, 'f200'],
  MSFT:     [ 1292, 'f50'],
  MSTR:     [ 1294, 'f200'],
  MU:       [ 1298, 'f50'],
  NBIS:     [ 2826, 'f200'],
  NEAR:     [   27, 'f200'],
  NET:      [ 2829, 'f200'],
  NFLX:     [ 1304, 'f200'],
  NVDA:     [ 1314, 'f50'],
  ONDO:     [  201, 'f200'],
  ORCL:     [ 1324, 'f50'],
  PLTR:     [ 1346, 'f50'],
  PUMP:     [ 1578, 'f200'],
  QCOM:     [ 1362, 'f200'],
  RENDER:   [   34, 'f200'],
  SILVER:   [  345, 'f200'],
  SKHY:     [ 3437, 'f200'],
  SKR:      [ 3023, 'f200'],
  SNDK:     [ 2858, 'f50'],
  SOL:      [    6, 'rt'],
  SPCX:     [ 3314, 'f50'],
  SUI:      [   11, 'f200'],
  TAO:      [   36, 'f200'],
  TRX:      [   17, 'f200'],
  TSLA:     [ 1435, 'f50'],
  TSM:      [ 1436, 'f200'],
  VIRTUAL:  [  107, 'f200'],
  VVV:      [  271, 'f200'],
  WLD:      [  104, 'f200'],
  WTIOIL:   [ 3071, 'f50'],
  XLM:      [   23, 'f200'],
  XPL:      [ 2312, 'f200'],
  XRP:      [   14, 'rt'],
  ZEC:      [   66, 'f200'],
};
/* Markets with a Binance kline history, which is a different question from
   which markets we index: the candle route backfills from Binance and there is
   no Binance USDT pair for an equity or a copper future. */
const KLINE_SYMBOLS = ['BTC', 'ETH', 'SOL', 'AAVE', 'ADA', 'BNB', 'CHIP', 'DOGE', 'ENA', 'FET', 'JTO', 'MEGA', 'MET', 'MORPHO', 'NEAR', 'ONDO', 'PUMP', 'RENDER', 'SUI', 'VIRTUAL', 'WLD', 'XLM', 'XPL', 'XRP',
  /* Added 2026-09-04 with the wider Pyth grant. Candle HISTORY is a separate
     question from the live price: Lazer streams prices and serves no bars, so
     a chart needs a source that keeps the past. These seven have a Binance
     spot pair and were falling back to Phoenix's own trades, which on a quiet
     minute produces a bar with no range at all. Every pair below was verified
     to answer klines before being listed. */
  /* LIT is NOT on this list on purpose: Binance's LITUSDT is Litentry at
     $0.74 while our LIT marks at $4.33, a different asset behind the same
     three letters. Same trap as MET, which is Meteora here and MetLife in the
     Pyth catalog. Every pair here was checked against our own mark before
     being listed, and the boot check below re-checks them on every start. */
  'CRV', 'JUP', 'LINK', 'TAO', 'TRX', 'ZEC'];
for (const sym of DISABLED_MARKETS) delete LAZER_ALL[sym];
const INDEX_SYMBOLS = Object.keys(LAZER_ALL);
const PYTH_BY_ID = Object.fromEntries(Object.entries(PYTH_FEEDS).map(([sym, id]) => [id, sym]));
// ── index source selection ───────────────────────────────────────────────
// 'binance' (default): Binance spot bookTicker mids — free, ~100 upd/s on
// BTC, no deprecation risk. Pyth Core died 2026-07-31 (paid-only now; the
// legacy Hermes grace endpoint pauses 3s every ~7.5s). 'pyth' kept intact
// as an instant-rollback path (INDEX_SOURCE=pyth in the unit env).
const INDEX_SOURCE = (process.env.INDEX_SOURCE || 'binance').toLowerCase();
let _chainStarted = false;
/* Disabled markets are filtered here too. KLINE_SYMBOLS drives two different
   things, the candle history AND the Binance index legs, so a market that was
   out of the book everywhere else was still being streamed, indexed and
   relayed to every browser through this door: MET was still arriving at 0.3
   frames a second after it had been removed from the product. */
const BINANCE_STREAMS = Object.fromEntries(KLINE_SYMBOLS.filter((s2) => !DISABLED_MARKETS.has(s2)).map((s2) => [s2, s2.toLowerCase() + 'usdt']));
// USDC leg only where the book is deep. On thin alts the USDC pair keeps
// emitting size updates at a stale quote, becomes the lone outlier, and was
// the cause of nearly every divergence halt (measured 2026-08-22). Majors
// keep the third source; alts run binance-usdt + coinbase.
const BINANCE_USDC_DEEP = new Set(['BTC', 'ETH', 'SOL']);
const BINANCE_USDC_STREAMS = Object.fromEntries(KLINE_SYMBOLS.filter((s2) => BINANCE_USDC_DEEP.has(s2)).map((s2) => [s2, s2.toLowerCase() + 'usdc']));
const GUARD_PRODUCTS = Object.fromEntries(KLINE_SYMBOLS.filter((s2) => !DISABLED_MARKETS.has(s2)).map((s2) => [s2 + '-USD', s2]));
const STAGE_INDEXED = new Set(INDEX_SYMBOLS);
/* THE BOOK IS THE INDEX. Every market on offer is one the engine indexes
   itself, around the clock, from the same primary. Anything else the venue
   lists is off until someone adds it to the index: an equity with a trading
   calendar, a token that only has a venue mark, a listing that appeared
   overnight. The previous rule was the list of names above, and it lasted
   five days: SPY, QQQ and STONK were listed on 2026-09-09, after it was
   written, and walked straight into the picker with a venue-only price.
   Called on every exchange refresh with the venue's current listing; base
   names only, event twins carry their base's standing. Returns what it
   switched off this time, for the log. */
function restrictToIndexed(symbols) {
  const off = [];
  for (const raw of symbols || []) {
    const sym = String(raw || '').toUpperCase();
    if (!sym || aliasKind(sym) || STAGE_INDEXED.has(sym) || DISABLED_MARKETS.has(sym)) continue;
    DISABLED_MARKETS.add(sym);
    off.push(sym);
  }
  return off;
}
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
// Historical calibration note (2026-08-08): the earlier free-play design used
// a uniform high tier across six majors. The current competition contract is
// the simpler rule below: ordinary markets cap at 100x and the shared Boost
// window alone may reach its round-persisted ceiling (500x today).
/* ── what a market may carry ─────────────────────────────────────────────────
 *
 * The product rule, from the owner (2026-09-04): the terminal is 100x on
 * everything, and 500x exists ONLY through the Boost, on the four markets the
 * Boost opens. This table used to encode a per-market tier of 500x, 250x and
 * 50x from a noise study, which had drifted away from the product in two
 * directions at once: the picker advertised 500x on fourteen markets nobody
 * runs a Boost on, and indexing the new markets silently moved fifty four of
 * them from 100x to the 250x default.
 *
 * One number, in one place. A market may still be pinned LOWER than the base
 * where its own noise demands it, because lowering is a safety decision and
 * MET's book genuinely cannot support 100x: its worst routine tick is wider
 * than the liquidation distance at that cap. Nothing is pinned higher. The
 * Boost cap lives on the twin ticker, which is where the Boost actually
 * trades, so a boosted position is the only way to exceed the base. */
const STAGE_BASE_LEV = Number(process.env.PAPER_STAGE_BASE_LEV || 100);
const STAGE_LEV_CAPS = {
  MET: 50,
};

// ── competition aliases ──────────────────────────────────────────────────
// Event aliases share a base symbol's index but own a distinct position row.
// Current v2 rounds use aliases only for Boost; both Hot windows trade the
// ordinary underlying so an already-held position participates without a
// close/reopen. `-HOT` remains parseable solely for historical v1 rows. Every
// downstream mark/config/risk lookup resolves an alias to its base.
const ALIAS_RE = /^([A-Z0-9]+)-(HOT|BOOST)$/;
const baseOf = (sym) => { const m = ALIAS_RE.exec(sym); return m ? m[1] : sym; };
const aliasKind = (sym) => { const m = ALIAS_RE.exec(sym); return m ? m[2] : null; };
// Which aliases are tradable right now. Driven by the round clock and the
// operator, never by the client, and empty outside an event — so these
// tickers simply do not exist during ordinary paper trading.
const openAliases = new Map();   // alias -> { openedAt, roundId }
const aliasOpen = (sym) => openAliases.has(sym);
const stageLevCap = (sym) => {
  /* The Boost twin is the ticker the Boost trades, so it is the one that may
     exceed the base. Its ceiling is still the round's armed cap and the live
     price-quality cap; this only says the engine will not stop it at 100. */
  if (aliasKind(sym) === 'BOOST') return HEAT_MAX_LEV;
  const b = baseOf(sym);
  const base = STAGE_INDEXED.has(b) ? STAGE_BASE_LEV : STAGE_UNINDEXED_LEV_CAP;
  return Math.min(base, STAGE_LEV_CAPS[b] || base);
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
  /* THE NAMED SOURCE IS THE PRICE; A BACKUP DISAGREEING IS NOT AN ERROR.
   *
   * This guard belongs to the blended index, where a divergence meant the
   * index itself was unknowable. Under one published source it contradicts the
   * product rule in both directions: a healthy primary could be frozen because
   * a backup drifted, and with three components the guard could see an
   * agreeing backup pair while the FOLLOWED source was the outlier. Divergence
   * is now telemetry, and anomalies on the followed source are caught by the
   * clamp, which is source-centric.
   *
   * It stays available behind a flag: if the engine is ever pointed back at a
   * blended index, this is the guard that index needs. */
  if (!GUARD_CROSS_VENUE) {
    if (st.halted) { st.halted = false; _log(`index guard ${sym}: halt lifted (cross-venue guard off under named-source pricing)`); }
    st.divergeSince = 0; _halt.set(sym, st);
    return false;
  }
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
/* THE INDEX IS ONE PUBLISHED SOURCE, WITH FAILOVER.
 *
 * Earlier versions blended sources (mean while they agreed, median when they
 * did not) and then gated 1000x on how well they agreed. That made the
 * headline leverage a function of two venues' micro-divergence: on a two-venue
 * feed the majors cleared the bar only 37-80% of the time, so the cap moved
 * during the decisive window and two traders clicking seconds apart could get
 * different leverage.
 *
 * A blend also asks a question nobody can answer. When two sources disagree,
 * a second source can say "we differ" but never "he is wrong", so the engine
 * was resolving a dispute it had no way to settle.
 *
 * So the rule is now the simplest one that can be published and verified: the
 * price is the FIRST source in a named chain that is currently ticking. A
 * trader can pull up that venue and check any liquidation against it. Nothing
 * is averaged, so nothing needs to agree, and 1000x is always available
 * because there is always a live source.
 *
 * Failover is fast on purpose. If the primary freezes at 100.00 while the real
 * market is at 100.30, switching gaps the mark by exactly as much as the
 * freeze cost, and at 1000x that liquidates everyone on the wrong side. The
 * gap is proportional to how long we took to notice, so we notice in about a
 * second rather than waiting out the old 5s component window. */
/* The venue's own mark is the LAST leg, not a separate world. Equities only
   publish to Pyth during New York hours while Phoenix quotes them around the
   clock, so without a tail the whole equity book would go unpriceable every
   night. It sits behind every real index source and carries no provider
   timestamp, which is exactly why the unageable rule holds it to the base
   tier. */
const INDEX_CHAIN = String(process.env.PAPER_INDEX_CHAIN || 'lazer,usdt,usd,usdc,venue').split(',').map((x) => x.trim());
/* How long the followed source may be silent before we fail over.
 *
 * Set from measurement, not taste. Sampling the live feed at 5Hz for two
 * minutes, the gap between prints on the followed source runs p50 ~0.5s and
 * p99 1.0s (BTC), 1.2s (ETH), 1.7s (SOL), 2.4s (XRP), with a worst observed
 * 2.96s. A 1.5s threshold therefore sat BELOW the normal quiet period for half
 * the majors, so they failed over on a perfectly healthy feed: XRP changed
 * source 39 times in 150s, and every switch gaps the mark by the venue
 * difference for a move that never happened.
 *
 * 4s clears the observed worst case with headroom. The cost is that a genuine
 * freeze goes unnoticed for up to 4s, and the failover gap is whatever the
 * market did in that time; that is the trade a single-source index makes, and
 * it is bounded by the jump clamp below. */
const SOURCE_STALE_MS = Number(process.env.PAPER_SOURCE_STALE_MS || 4000);
/* Staleness is PER SOURCE, because the sources behave nothing alike.
 *
 * Binance needs 4s: its own healthy quiet periods reach 2.96s, so anything
 * tighter fails over on a working feed. Lazer publishes on an exact 50ms
 * metronome (p50 and p99 both 50ms), so 250ms is five missed ticks and
 * unambiguously a fault. Using Binance's threshold for Lazer would leave us
 * pricing off a dead primary for 4 seconds when we could know in 250ms. */
/* 600, not 250. Measured Sep 2 2026 from a separate process on the box, 90s,
   three channels at once: Lazer's real_time AND fixed_rate@50ms both showed
   gaps of ~260ms about twice a minute on every symbol together (p99 73ms,
   max 264ms). A 250ms bar sat on that tail and failed the majors over to
   Binance on jitter, not on an outage; failback then needed ten clean
   seconds. 600 clears the measured tail with margin and still catches a dead
   feed well inside a second. Fail away fast, fail back slow is unchanged. */
const LAZER_STALE_MS = Number(process.env.PAPER_LAZER_STALE_MS || 600);
/* A 200ms metronome cannot be judged on a budget sized for a 50ms one: the
   cadence alone eats a third of it before the 130ms of flight time. Measured
   ages are 131ms on the fast tiers and 150ms on the 200ms tier. */
const LAZER_STALE_SLOW_MS = Number(process.env.PAPER_LAZER_SLOW_STALE_MS || 1200);
const SOURCE_STALE_OVERRIDE = {
  lazer: LAZER_STALE_MS,
  /* A venue mark's budget is the one the ORDER PATH already uses for it
     (mktFresh / MARKET_FRESH_MS). Letting it fall to the generic four seconds
     made the terminal call an equity stale while the engine would happily have
     filled it: the screen and the fill must answer the same question. */
  venue: MARKET_FRESH_MS,
};
const staleMsFor = (key) => SOURCE_STALE_OVERRIDE[key] ?? SOURCE_STALE_MS;
/* ── how often does THIS market actually quote? ──────────────────────────────
 *
 * One four second budget for every venue-priced market treated a market that
 * quotes twice a second and one that quotes every seven seconds as the same
 * thing. Measured over three hours: BTC and SOL never exceed 50ms between
 * ticks, MORPHO exceeds four seconds 2.7% of the time, and MET exceeds it
 * 18.9% of the time. MET is not broken when it is quiet for six seconds, that
 * IS MET, and calling it unpriceable froze a practice round repeatedly on
 * 2026-09-03.
 *
 * So the budget follows the market: roughly the 95th percentile of its own
 * observed gaps with headroom, never tighter than the global four seconds and
 * never looser than the cap. Lazer keeps its own 600ms, because the whole
 * point of the primary is that it does not go quiet.
 *
 * The estimate is persisted, so a restart does not re-learn from scratch and
 * re-freeze a market it already understood. A market with no history yet gets
 * the old global budget, which is the conservative direction. */
const THIN_STALE_MAX_MS = Number(process.env.PAPER_THIN_STALE_MAX_MS || 30_000);
const CADENCE_HEADROOM = Number(process.env.PAPER_CADENCE_HEADROOM || 2.5);
const CADENCE_MIN_SAMPLES = 24;
/* KEYED BY MARKET AND SOURCE, because cadence is a property of the pair.
   Keying by symbol alone let a 50ms primary and a venue mark that arrives
   every few seconds train one number, so each contaminated the other: the
   learned budget was neither source's rhythm. */
const _gapRing = new Map();      // "sym|src" -> recent inter-arrival gaps (ms)
const _gapLast = new Map();      // "sym|src" -> when we last saw an observation
const _cadence = new Map();      // "sym|src" -> { budgetMs, samples, at }
function cadenceNote(symIn, now, srcKey = 'venue') {
  const sym = `${baseOf(symIn)}|${srcKey}`;
  const prev = _gapLast.get(sym);
  _gapLast.set(sym, now);
  if (!(prev > 0)) return;
  const gap = now - prev;
  /* A gap longer than the cap is an outage, not a cadence: it would drag the
     estimate toward "this market is allowed to be silent for a minute". */
  if (gap <= 0 || gap > THIN_STALE_MAX_MS) return;
  let ring = _gapRing.get(sym);
  if (!ring) { ring = []; _gapRing.set(sym, ring); }
  ring.push(gap);
  if (ring.length > 128) ring.splice(0, ring.length - 128);
  if (ring.length < CADENCE_MIN_SAMPLES) return;
  const c = _cadence.get(sym);
  if (c && now - c.at < 30_000) return;               // recompute at most twice a minute
  const sorted = [...ring].sort((a, b) => a - b);
  const p95 = sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * 0.95))];
  const budgetMs = Math.min(THIN_STALE_MAX_MS, Math.max(SOURCE_STALE_MS, Math.ceil(p95 * CADENCE_HEADROOM)));
  _cadence.set(sym, { budgetMs, samples: ring.length, at: now });
}
/* The budget for THIS market on THIS source. */
function staleMsForSym(sym, key) {
  const base = staleMsFor(key);
  if (key === 'lazer') {
    const t = LAZER_ALL[baseOf(sym)];
    return t && t[1] === 'f200' ? LAZER_STALE_SLOW_MS : LAZER_STALE_MS;
  }
  if (SOURCE_STALE_OVERRIDE[key] !== undefined) return base;   // the primary keeps its own rule
  const c = _cadence.get(`${baseOf(sym)}|${key}`);
  return c && c.budgetMs > base ? c.budgetMs : base;
}
/* Persisted, so a restart does not spend its first minute re-learning what it
   already knew and freezing a market it already understood. */
let _cadenceStore = null;
function cadenceStore() {
  if (_cadenceStore) return _cadenceStore;
  db.exec(`CREATE TABLE IF NOT EXISTS paper_market_cadence (
    symbol TEXT PRIMARY KEY, budget_ms INTEGER NOT NULL, samples INTEGER NOT NULL, updated_at INTEGER NOT NULL)`);
  _cadenceStore = {
    all: db.prepare('SELECT symbol, budget_ms, samples FROM paper_market_cadence'),
    put: db.prepare(`INSERT INTO paper_market_cadence (symbol, budget_ms, samples, updated_at) VALUES (?, ?, ?, ?)
      ON CONFLICT(symbol) DO UPDATE SET budget_ms = excluded.budget_ms, samples = excluded.samples, updated_at = excluded.updated_at`),
    del: db.prepare('DELETE FROM paper_market_cadence WHERE symbol = ?'),
  };
  return _cadenceStore;
}
/* A CRASH LOOP MUST ANNOUNCE ITSELF.
 *
 * An unhandled rejection exits the process on purpose: an engine that decides
 * money should not keep serving from an unknown state, and systemd brings it
 * straight back. The gap is that nobody can see it. On 2026-09-04 a bad
 * property read in the candle route killed the engine nine times at six second
 * intervals, and from every client it looked like a network fault, which cost
 * hours of tunnel and proxy archaeology. Three boots inside ten minutes is not
 * a deploy, it is a loop, and it now says so on the first boot that proves it.
 */
function bootHeartbeat() {
  try {
    db.exec('CREATE TABLE IF NOT EXISTS paper_boots (at INTEGER NOT NULL)');
    const now = Date.now();
    db.prepare('INSERT INTO paper_boots (at) VALUES (?)').run(now);
    db.prepare('DELETE FROM paper_boots WHERE at < ?').run(now - 24 * 3600_000);
    const recent = db.prepare('SELECT COUNT(*) n FROM paper_boots WHERE at > ?').get(now - 10 * 60_000).n;
    if (recent >= 3) {
      _log(`ENGINE RESTART LOOP: ${recent} boots in the last 10 minutes`);
      tgOps('bootloop', `paper engine has started ${recent} times in 10 minutes: it is crash looping, not deploying`);
    }
  } catch (e) { _log('boot heartbeat failed: ' + e.message); }
}
function cadenceLoad() {
  try {
    for (const r of cadenceStore().all.all()) {
      const b = Math.min(THIN_STALE_MAX_MS, Math.max(SOURCE_STALE_MS, Number(r.budget_ms) || 0));
      /* Rows written before the key carried the source are dropped rather than
         guessed at: a budget attributed to the wrong source is worse than
         relearning one in a minute. */
      const sym = String(r.symbol);
      if (!sym.includes('|')) continue;
      /* A budget for a market we no longer trade is not just dead weight: it
         reloads into memory, re-persists itself every minute, and shows up in
         /readyz beside the live markets, so an operator reading readiness
         mid-round sees cadence for metals and equities the engine has not
         fetched since they were disabled. Drop the row rather than carry it;
         if the market is ever re-enabled it relearns its cadence in a minute,
         which is exactly what a fresh market does anyway. */
      if (!STAGE_INDEXED.has(baseOf(sym))) { try { cadenceStore().del.run(sym); } catch {} ; continue; }
      _cadence.set(sym, { budgetMs: b, samples: Number(r.samples) || 0, at: 0 });
    }
    _log(`market cadence: restored ${_cadence.size} budget(s)`);
  } catch (e) { _log('market cadence restore failed: ' + e.message); }
}
setInterval(() => {
  try {
    const t = Date.now();
    for (const [sym, c] of _cadence) cadenceStore().put.run(sym, c.budgetMs, c.samples, t);
  } catch (e) { _log('market cadence save failed: ' + e.message); }
}, 60_000).unref();
function cadenceTable() {
  const out = {};
  for (const [sym, c] of _cadence) out[sym] = { budgetMs: c.budgetMs, samples: c.samples };
  return out;
}
/* Fail away FAST, fail back SLOW.
 *
 * Symmetric thresholds flap. Measured on the live feed, XRP's primary ticks
 * just either side of the stale line, so the engine switched source 25 times
 * in 150 seconds; every one of those is a mark gap of whatever the two venues
 * happened to differ by, and at 1000x a 1bps gap is 20% of margin. Those are
 * not market moves, they are us changing our mind. So a higher-priority source
 * has to prove it is genuinely back, by ticking continuously for this long,
 * before we return to it. Dropping to a backup stays immediate, because that
 * direction is the one that keeps the market priced. */
const SOURCE_FAILBACK_MS = Number(process.env.PAPER_SOURCE_FAILBACK_MS || 10_000);
/* A SLOW FEED EARNS ITS SLOT BACK MORE SLOWLY.
 *
 * Ten seconds is the right patience for a primary that publishes every 50ms:
 * it has produced two hundred observations by then. A feed on the 200ms tier
 * for a thin token can be quiet for seconds at a time quite normally, so it
 * satisfies "ten seconds of freshness" and then goes quiet again, and the
 * chain walks back and forth between it and the venue every ten seconds all
 * day. Each of those switches changes the price world by the basis between the
 * two sources, and when that exceeds the jump clamp the market freezes itself
 * while it confirms: CHIP did that 52 times in 40 minutes on 2026-09-04. */
const SOURCE_FAILBACK_SLOW_MS = Number(process.env.PAPER_SOURCE_FAILBACK_SLOW_MS || 60_000);
const failbackMsFor = (sym, key) => (key === 'lazer' && (LAZER_ALL[baseOf(sym)] || [])[1] === 'f200'
  ? SOURCE_FAILBACK_SLOW_MS : SOURCE_FAILBACK_MS);
const _activeSrc = new Map();   // sym -> source key currently being followed

/* THE FOLLOWED SOURCE SURVIVES A RESTART.
 *
 * _activeSrc used to be memory-only. After a restart the chain re-selected
 * from whatever had reconnected first, so a service bounce could change the
 * public price definition based on packet arrival order. The old blanket
 * mid-round promotion ban then pinned that accidental choice for the rest of
 * the round. The selection is durable now, so a restart resumes the source
 * the round was actually being priced on before ordinary recovery/failback. */
let _srcStmt = null;
function srcStore() {
  if (_srcStmt) return _srcStmt;
  try {
    db.exec(`CREATE TABLE IF NOT EXISTS paper_index_source (
      symbol TEXT PRIMARY KEY, source TEXT NOT NULL, since INTEGER NOT NULL)`);
    _srcStmt = {
      put: db.prepare('INSERT INTO paper_index_source (symbol, source, since) VALUES (?, ?, ?) ON CONFLICT(symbol) DO UPDATE SET source = excluded.source, since = excluded.since'),
      all: db.prepare('SELECT symbol, source, since FROM paper_index_source'),
      one: db.prepare('SELECT source, since FROM paper_index_source WHERE symbol = ?'),
    };
  } catch { _srcStmt = { put: { run() {} }, all: { all: () => [] }, one: { get: () => null } }; }
  return _srcStmt;
}
/* FAIL CLOSED: a transition that did not commit did not happen.
 *
 * This swallowed every database error, so the engine could switch source in
 * memory, publish the new mark, and price orders and liquidations from a
 * source the durable record did not know about. A restart would then restore
 * a DIFFERENT source from the one that actually decided the round. Returns
 * false if the selection could not be made durable. */
function persistSource(sym, key, now) {
  try { srcStore().put.run(sym, key, now); return true; }
  catch (e) {
    _log(`index ${sym}: could not persist source ${key} (${e.message})`);
    tgOps('srcpersist:' + sym, `could not persist followed source for ${sym}: ${e.message}`);
    return false;
  }
}

/* Symbols whose source selection could not be made durable. Treated exactly
   like "we do not know this price": nothing may be priced off them. */
const _srcUnsafe = new Map();     // sym -> since
/* A shared, round-wide price pause, held as a SET of durable obligations.
 *
 * It was one slot holding one symbol, and it was wrong twice over. A second
 * unpriceable symbol overwrote the first, so recovering the second cleared
 * the pause while the first was still unpriceable. And the slot was
 * process-local, so a restart forgot that the field was ever frozen.
 *
 * Now: one open obligation per (symbol), persisted. The round stays paused
 * until EVERY obligation is restored. While it is held no contestant is
 * marked, liquidated, filled, sampled for drawdown, or allowed to trade. */
function pauseStore() {
  if (!_pauseStmts) {
    db.exec(`CREATE TABLE IF NOT EXISTS paper_price_pauses (
      symbol      TEXT NOT NULL,
      round_id    TEXT,
      started_at  INTEGER NOT NULL,
      restored_at INTEGER,
      reason      TEXT,
      PRIMARY KEY (symbol, started_at)
    )`);
    db.exec('CREATE INDEX IF NOT EXISTS ppp_open ON paper_price_pauses (symbol) WHERE restored_at IS NULL');
    _pauseStmts = {
      open: db.prepare('INSERT OR IGNORE INTO paper_price_pauses (symbol, round_id, started_at, reason) VALUES (?, ?, ?, ?)'),
      isOpen: db.prepare('SELECT 1 FROM paper_price_pauses WHERE symbol = ? AND round_id IS ? AND restored_at IS NULL'),
      /* Scoped to the round. An obligation belongs to the round that raised
         it: an unresolved pause on an aborted round used to reject a valid
         order in the NEXT round, on a symbol that round never named. */
      allOpen: db.prepare('SELECT symbol, round_id, started_at, reason FROM paper_price_pauses WHERE round_id IS ? AND restored_at IS NULL ORDER BY started_at'),
      restore: db.prepare('UPDATE paper_price_pauses SET restored_at = ? WHERE symbol = ? AND round_id IS ? AND restored_at IS NULL'),
      closeRound: db.prepare('UPDATE paper_price_pauses SET restored_at = ? WHERE round_id = ? AND restored_at IS NULL'),
    };
  }
  return _pauseStmts;
}
let _pauseStmts = null;
/* Built eagerly. roundPaused() has to swallow errors to avoid stalling the
   price path, and a missing table would read as "not paused" forever. */
try { pauseStore(); } catch (e) { console.error('[paper] pause table init failed:', e.message); }

/* Returns the oldest open obligation plus the full set, or null when clear.
   The TABLE is the truth, so a restart mid-pause stays paused, but this is
   asked once per position per tick, so the answer is cached and invalidated
   whenever an obligation opens or closes rather than re-queried in the hot
   path. */
let _pauseCache = { dirty: true, key: null, value: null };
let _pauseRevision = 0;
let _pauseNotifier = null;
function invalidatePause() {
  _pauseRevision++;
  _pauseCache = { dirty: true, key: null, value: null };
  // Only observe committed state after this synchronous transaction/rollback.
  try { _pauseNotifier?.wake(); } catch {}
}
/* An emergency latch for the case where the obligation could not be written
   down. It is a SET of symbols scoped to a ROUND, because several markets can
   fail before persistence returns, and because a latch that does not know
   which round it belongs to survives that round and freezes the next one. */
let _pauseLatch = null;   // { roundId, symbols: Set, since, why }
function latchFor(rid) {
  if (!_pauseLatch) return null;
  return _pauseLatch.roundId === rid ? _pauseLatch : null;
}
function roundPaused() {
  const rid = (comp.currentRound() || {}).id || null;
  if (!_pauseCache.dirty && _pauseCache.key === rid) return _pauseCache.value;
  let rows;
  try {
    rows = pauseStore().allOpen.all(rid);
  } catch (e) {
    /* A read failure is NOT "not paused". Reporting no pause here is how a
       broken table silently unfreezes a competition. */
    return { symbol: 'unknown', since: Date.now(), why: 'pause_state_unavailable: ' + e.message, symbols: ['unknown'], count: 1, degraded: true };
  }
  const all = [...rows];
  const latch = latchFor(rid);
  if (latch) {
    for (const sym of latch.symbols) all.unshift({ symbol: sym, started_at: latch.since, reason: latch.why });
  }
  const value = all.length ? {
    symbol: all[0].symbol,
    since: all[0].started_at,
    why: all[0].reason,
    symbols: [...new Set(all.map((r) => r.symbol))],
    count: all.length,
    unpersisted: !!latch,
  } : null;
  _pauseCache = { dirty: false, key: rid, value };
  return value;
}

const RESTART_PAUSE_SYMBOL = '__ENGINE_RESTART__';
const GLOBAL_FEED_PAUSE_SYMBOL = '__COMPETITION_FEED__';
const BOUNDARY_PAUSE_SYMBOL = '__BOUNDARY_PRICE__';
function pauseReasonClass(pause) {
  const why = String(pause && pause.why || '');
  const symbols = pause && pause.symbols || [];
  if (/identity service unavailable/i.test(why)) return 'identity';
  if (symbols.includes(RESTART_PAUSE_SYMBOL)) return 'restart';
  if (symbols.includes(BOUNDARY_PAUSE_SYMBOL)) return 'boundary';
  if (pause && (pause.degraded || pause.unpersisted)
      || /drawdown|sampling|risk|persist/i.test(why)) return 'risk';
  if (symbols.some((s) => s !== GLOBAL_FEED_PAUSE_SYMBOL && s !== 'unknown')) return 'exposure';
  return 'feed';
}
function publicPause(pause, r) {
  if (!pause) return null;
  const reasonClass = pauseReasonClass(pause);
  const why = {
    identity: 'contestant identity service unavailable',
    restart: 'competition recovering after engine restart',
    boundary: 'competition boundary price unavailable',
    risk: 'competition risk checks unavailable',
    exposure: 'competition position prices unavailable',
    feed: 'competition prices unavailable',
  }[reasonClass];
  return Number(r && r.format_version) >= 2
    ? { since: pause.since || null, count: Number(pause.count) || 1,
        why, reasonClass, degraded: !!pause.degraded }
    : { ...pause, reasonClass };
}

function pauseRound(symbol, what, why, startedAt = null) {
  const sym = String(symbol || 'unknown');
  const rid = (comp.currentRound() || {}).id || null;
  const liveRound = comp.currentRound();
  const requested = startedAt == null ? Number.NaN : Number(startedAt);
  const since = Number.isFinite(requested) && requested > 0
    ? Math.max(Number(liveRound && liveRound.started_at) || requested, Math.min(Date.now(), requested))
    : Date.now();
  try {
    if (pauseStore().isOpen.get(sym, rid)) {
      /* The durable obligation may have survived a crash while the round-row
         clock freeze did not. Reassert the freeze on every observation; both
         operations are idempotent. */
      let durableSince = since;
      try {
        const rows = pauseStore().allOpen.all(rid);
        if (rows.length && Number.isFinite(Number(rows[0].started_at))) {
          durableSince = Math.min(durableSince, Number(rows[0].started_at));
        }
      } catch { /* the supplied onset remains conservative */ }
      try { comp.pauseClockOpen(durableSince); }
      catch (e) { _log(`clock freeze retry failed: ${e.message}`); }
      return;
    }
    const written = pauseStore().open.run(sym, rid, since, why || what || 'unpriceable');
    // A restored row can share this onset, including one from another round.
    // INSERT OR IGNORE returning normally is not proof of a durable pause.
    if (written.changes !== 1 && !pauseStore().isOpen.get(sym, rid)) {
      throw new Error('pause obligation was not persisted');
    }
    invalidatePause();
    /* PAUSE-AND-EXTEND: freeze the round clock too, or the phase burns down
       while nobody can trade it. Idempotent, so several markets failing inside
       one outage is still a single frozen interval. */
    try { comp.pauseClockOpen(since); } catch (e) { _log(`clock freeze failed: ${e.message}`); }
  } catch (e) {
    /* The obligation could not be written. Latch it in memory so the field is
       still frozen, and block the round so an operator has to look at it: a
       pause nobody can persist will not survive the next restart, and the
       engine must not quietly resume trading on the strength of that. */
    if (_pauseLatch && _pauseLatch.roundId === rid) {
      _pauseLatch.symbols.add(sym);
      _pauseLatch.since = Math.min(Number(_pauseLatch.since) || since, since);
    } else {
      _pauseLatch = { roundId: rid, symbols: new Set([sym]), since,
        why: `pause could not be persisted (${e.message})` };
    }
    invalidatePause();
    /* The obligation row is missing, but the round clock is a separate durable
       safety edge. Freeze it at the source-known onset anyway; the in-memory
       latch and blocked state keep recovery fail-closed until an operator can
       repair persistence. */
    try { comp.pauseClockOpen(_pauseLatch.since); }
    catch (clockError) { _log(`clock freeze after pause persistence failure also failed: ${clockError.message}`); }
    _log(`PAUSE NOT PERSISTED for ${sym}: ${e.message}; latched in memory and blocking the round`);
    try { if (rid) comp.blockRound(rid, `price pause on ${sym} could not be persisted: ${e.message}`); } catch {}
  }
  _log(`ROUND PAUSED: ${sym} cannot be priced (${why}); all contestant risk is frozen`);
  // A round-scoped notifier owns redacted lifecycle messages. Symbol-keyed
  // dedupe would suppress later incidents and expose unnecessary details.
}

/* A pause is allowed to lift only in a world the public scorer can actually
   publish. Checking the named pause symbol alone is insufficient: its source
   can still be below the quality tier of a live 500x position, or some other
   held leg can have become unscoreable while the field was frozen. Use the
   canonical compact scorer rather than duplicating its exposure rules here.
   A blocked round is deliberately probeable: operator recovery has to prove
   it is scoreable before clearBlock can remove the block. */
function competitionRosterScoreability(roundId) {
  if (!roundId) return { ok: false, reason: 'no live competition roster' };
  try {
    const rank = compRankSnapshot();
    if (!rank || !rank.live || rank.roundId !== roundId) {
      return { ok: false, reason: 'live scorer is not on this round' };
    }
    if (!rank.complete) {
      const stale = Array.isArray(rank.stalePricing) ? rank.stalePricing : [];
      const failed = Array.isArray(rank.unscored) ? rank.unscored.map((x) => x.userId) : [];
      const details = [
        stale.length ? `stale ${stale.join(', ')}` : '',
        failed.length ? `unscored ${failed.join(', ')}` : '',
      ].filter(Boolean).join('; ');
      return { ok: false, reason: details || 'compact score is incomplete' };
    }
    /* The compact scorer is the production authority. Keep the existing
       non-mutating drawdown probe as a second check on a healthy round: it is
       wired to the same roster equity hook that originally raised an
       unattributed pause. A blocked round deliberately skips this call because
       sampleDrawdown rejects blocked state; compact scoring above remains the
       recovery proof that lets an operator clear that block. */
    const round = comp.currentRound();
    if (round && round.id === roundId && !round.blocked_reason) {
      const equityReady = comp.sampleDrawdown(Date.now(), { probeOnly: true });
      if (equityReady !== true) return { ok: false, reason: 'roster equity probe is incomplete' };
    }
    return { ok: true };
  } catch (e) {
    return { ok: false, reason: `score probe failed: ${e && e.message || e}` };
  }
}

/* Global feed loss is detectable even with a flat roster: once every indexed
   competition mark has expired there is no valid trading world, so warning,
   build and final-build time must stop too. The exact failed symbols remain
   private; only this bit and a conservative onset are attached to the generic
   durable pause. */
function competitionFeedStatus(now = Date.now()) {
  const states = [];
  for (const sym of STAGE_INDEXED) {
    if (DISABLED_MARKETS.has(sym)) continue;
    try {
      const current = comp.currentRound();
      const state = roundPriceAvailability(sym, 0, now, current);
      // Running held-price lookups only read current round authority. One
      // executable mark without a price deadline determines this global OR;
      // the clock still checks every exposed/Hot/opened-Boost dependency.
      // Strict and armed paths retain their full source/deadline inspection.
      if (holdsRoundPrices(current) && current.status === 'running'
          && state.ready && state.validUntil == null) {
        return { ok: true, since: null, validUntil: null };
      }
      states.push(state);
    }
    catch { /* inspect the rest */ }
  }
  const ready = states.filter((x) => x.ready);
  if (ready.length) {
    /* The platform feed is alive until the LAST currently executable indexed
       market expires. Individual roster/segment markets are checked by the
       stricter competition-clock status below. */
    const deadlines = ready.filter((x) => x.validUntil != null)
      .map((x) => Number(x.validUntil)).filter(Number.isFinite);
    return { ok: true, since: null,
      validUntil: ready.some((x) => x.validUntil == null) ? null
        : (deadlines.length ? Math.max(...deadlines) : null) };
  }
  const known = states.filter((x) => x.known && Number.isFinite(Number(x.invalidSince)));
  const round = comp.currentRound();
  const floor = Number(round && round.started_at) || now;
  /* A total outage begins when the LAST previously-live market became bad,
     hence max rather than min. Markets that never produced a valid quote do
     not move that instant forward to detection time. */
  const onset = known.length
    ? Math.max(...known.map((x) => Number(x.invalidSince))) : now;
  return { ok: false, since: Math.max(floor, Math.min(now, onset)), validUntil: null };
}

function ensureGlobalCompetitionPause(now = Date.now()) {
  const r = comp.currentRound();
  if (!r || Number(r.format_version) < 2 || r.status !== 'running') return true;
  const feed = competitionFeedStatus(now);
  if (!feed.ok) {
    pauseRound(GLOBAL_FEED_PAUSE_SYMBOL, 'competition feed',
      'no competition-valid indexed price is available', feed.since);
    return false;
  }
  return true;
}

/* Every price dependency of the active clock. This is intentionally derived
   only from already-public/current facts: held positions, an already-revealed
   Hot market, and already-open Boost markets. A sealed future draw member is
   never inspected here, so neither a pause nor its timing can become an
   oracle for the draw. */
function competitionClockStatus(now = Date.now(), { ignorePause = false } = {}) {
  const r = comp.currentRound();
  if (!r || Number(r.format_version) < 2 || r.status !== 'running') {
    return { ok: true, nextExpiry: null, invalidSince: null };
  }
  /* A complete final checkpoint has already proved the bell's canonical
     marks. After that edge the only legal work is idempotent alias cleanup,
     which must remain possible after an OOM even if feeds are still cold. */
  if (comp.terminalCleanupPending(r)) {
    return { ok: true, terminalCleanup: true, nextExpiry: null, invalidSince: null };
  }
  if (!ignorePause && roundPaused()) {
    return { ok: true, paused: true, nextExpiry: null, invalidSince: null };
  }
  const checks = [];
  const identity = auth.authHealth(now);
  if (identity.tradingAvailable === false) {
    checks.push({ ready: false, invalidSince: identity.unavailableSince ?? now, known: true,
      reason: 'contestant identity service unavailable' });
  } else if (Number.isFinite(identity.validUntil)) {
    checks.push({ ready: true, validUntil: identity.validUntil, known: true });
  }
  const feed = competitionFeedStatus(now);
  if (!feed.ok) checks.push({ ready: false, invalidSince: feed.since, known: true });
  else if (feed.validUntil != null && Number.isFinite(Number(feed.validUntil))) {
    checks.push({ ready: true, validUntil: Number(feed.validUntil), known: true });
  }

  /* Existing contestant exposure is an account-equity dependency in every
     phase, including build/warning/final-build. */
  try {
    // The clock depends on market/leverage pairs, not account metadata. One
    // indexed join replaces a wide roster read and one wide position read
    // per seat on every nested write barrier. No readiness result is cached:
    // duplicate dependencies are equivalent and each distinct pair is still
    // checked at this event's exact time, including after a position mutation.
    for (const pos of stmt.clockExposure.all(r.id)) {
      checks.push(roundPriceAvailability(baseOf(pos.symbol), Number(pos.leverage) || 0, now, r));
    }
  } catch {
    /* The roster scorer remains the fail-closed authority. A DB error here is
       not mislabelled as a price outage; its normal caller will block. */
  }

  const ph = comp.phaseNow(now);
  if (ph && ph.phase === 'hot' && ph.hotNumber) {
    const base = r[`hot${ph.hotNumber}_active_base`];
    if (base) checks.push(roundPriceAvailability(base, comp.COMP_BASE_LEV, now, r));
  }
  if (ph && ph.phase === 'boost') {
    let opened = [];
    try { opened = JSON.parse(r.boost_opened || '[]'); } catch { opened = []; }
    for (const base of opened) checks.push(roundPriceAvailability(base,
      Number(r.boost_leverage) || HEAT_MAX_LEV, now, r));
  }

  const bad = checks.filter((x) => !x.ready);
  if (bad.length) {
    const known = bad.filter((x) => Number.isFinite(Number(x.invalidSince)));
    const since = known.length
      ? Math.min(...known.map((x) => Number(x.invalidSince))) : now;
    return { ok: false,
      invalidSince: Math.max(Number(r.started_at) || since, Math.min(now, since)),
      nextExpiry: null, reason: bad.find((x) => x.reason)?.reason
        || 'competition price dependency is unavailable' };
  }
  const expiries = checks.filter((x) => x.validUntil != null)
    .map((x) => Number(x.validUntil)).filter(Number.isFinite);
  return { ok: true, invalidSince: null,
    nextExpiry: expiries.length ? Math.min(...expiries) : null };
}

function ensureCompetitionClockHealth(now = Date.now()) {
  /* A durable price obligation and the round-row clock edge are normally
     written back-to-back. If the second write faulted, a boundary timer may
     arrive before the sweep reconciler. Linearize that split here using the
     obligation's original onset; reporting merely `paused:true` while the
     active clock still advanced allowed the boundary to commit inside the
     invalid interval. */
  const durablePause = roundPaused();
  const round = comp.currentRound();
  if (durablePause && round && !round.paused_since) {
    const onset = Number(durablePause.since);
    comp.pauseClockOpen(Number.isFinite(onset) && onset > 0 ? onset : now);
  }
  const status = competitionClockStatus(now);
  if (!status.ok) {
    pauseRound(GLOBAL_FEED_PAUSE_SYMBOL, 'competition availability',
      status.reason, status.invalidSince);
    return false;
  }
  return true;
}

/* Unlike the compact-board expiry timer, this timer exists with ZERO viewers.
   It fires at the exact source deadline, before a same-instant phase boundary
   can commit, and therefore records even an outage that recovers between two
   five-second sweeps. */
let _competitionClockExpiryTimer = null;
let _armingCompetitionClock = false;
function armCompetitionClockExpiry(now = Date.now()) {
  if (_armingCompetitionClock) return;
  _armingCompetitionClock = true;
  try {
    if (_competitionClockExpiryTimer) clearTimeout(_competitionClockExpiryTimer);
    _competitionClockExpiryTimer = null;
    const status = competitionClockStatus(now);
    if (!status.ok) {
      ensureCompetitionClockHealth(now);
      return;
    }
    // null means there is no clock dependency. Number(null) is zero and
    // previously turned an idle engine into a self-rescheduling 1ms DB loop.
    if (status.paused || status.nextExpiry == null
        || !Number.isFinite(Number(status.nextExpiry))) return;
    const at = Number(status.nextExpiry);
    _competitionClockExpiryTimer = setTimeout(() => {
      _competitionClockExpiryTimer = null;
      const t = Date.now();
      try {
        // Timer insertion order must not postpone a cached, admissible source
        // transition until after this expiry check. The transition still runs
        // the normal committed-world boundary prelude before any risk commit.
        refreshExpiredSources(t);
        ensureCompetitionClockHealth(t);
      }
      finally { armCompetitionClockExpiry(t); }
    }, Math.max(1, at - Date.now()));
    _competitionClockExpiryTimer.unref?.();
  } finally {
    _armingCompetitionClock = false;
  }
}

function genericPauseRecovered(symbol, roundId, now) {
  const current = comp.currentRound();
  if (current && current.id === roundId && comp.terminalCleanupPending(current)) {
    return true;
  }
  if (symbol === RESTART_PAUSE_SYMBOL || symbol === GLOBAL_FEED_PAUSE_SYMBOL) {
    if (!competitionClockStatus(now, { ignorePause: true }).ok) return false;
    if (!competitionFeedStatus(now).ok) return false;
    try { return comp.activeSegmentReady(comp.currentRound(), now); } catch { return false; }
  }
  if (symbol === BOUNDARY_PAUSE_SYMBOL) {
    try { return comp.priceBoundaryReady(comp.currentRound(), now); } catch { return false; }
  }
  return null;
}

/* Restores only the obligations whose symbol is priceable again. One symbol
   recovering can never clear another's, and no obligation is restored until
   the complete roster passes the canonical score probe above. */
function clearPauseIfPriceable() {
  const rid = (comp.currentRound() || {}).id || null;
  let rows;
  try { rows = pauseStore().allOpen.all(rid); } catch { return; }
  /* A latched, unpersisted pause is never cleared automatically. It means the
     engine could not record what it did, which is an operator problem. */
  if (latchFor(rid)) return;
  /* Reconcile the OTHER direction too: pause rows exist but the clock never
     froze (pauseClockOpen threw after the row write, and pauseRound's isOpen
     early-return means it is never retried). Contestants are refused writes
     while the phases burn down, which is the exact injustice pause-and-extend
     exists to prevent. Idempotent, so calling it every sweep is free. */
  if (rows.length) {
    try {
      const r = comp.currentRound();
      if (r && !r.paused_since) {
        const earliest = rows.map((row) => Number(row.started_at))
          .filter(Number.isFinite).reduce((min, value) => Math.min(min, value), Date.now());
        comp.pauseClockOpen(earliest);
      }
    } catch { /* the next sweep tries again */ }
  }
  /* Do this before either restoring a row or closing an orphaned frozen clock.
     In particular, generic compPriceReady(U500) may be true on a 100x-only
     fallback while the seated 500x exposure remains unscoreable. The previous
     order briefly restarted the clock, then reopened the pause in risk later
     in the same sweep. */
  const active = comp.currentRound();
  if (rid && (rows.length || (active && active.paused_since))) {
    const scoreability = competitionRosterScoreability(rid);
    if (!scoreability.ok) return;
    /* Identity is a clock dependency even for an entirely flat roster. This
       also governs rowless recovery after a failed pause-store write/restart:
       healthy prices alone cannot resume a round whose users cannot trade. */
    if (!competitionClockStatus(Date.now(), { ignorePause: true }).ok) return;
  }
  if (!rows.length) {
    /* No open obligations, but the round clock may still be frozen: a pause
       whose row write failed (the latch case) opens the clock with no durable
       row, and after a restart the latch is gone, the rows are absent, and
       nothing else would ever close the clock. A round frozen forever with
       healthy prices is exactly the wedge this reconciles. */
    try {
      const r = comp.currentRound();
      if (r && r.paused_since) { comp.pauseClockClose(); _pauseNotifier?.wake(); }
    } catch { /* the next sweep tries again */ }
    return;
  }
  const now = Date.now();
  for (const r of rows) {
    /* With no live round there is no canonical roster that can prove an
       unattributed failure recovered. Named orphan rows retain their ordinary
       source check so restart cleanup does not wedge on absent score state. */
    const generic = genericPauseRecovered(r.symbol, rid, now);
    const recovered = generic !== null ? generic
      : /-BOOST$/.test(r.symbol)
        ? roundPriceReady(baseOf(r.symbol), HEAT_MAX_LEV, now)
        : roundPriceReady(r.symbol, 0, now);
    if (r.symbol === 'unknown' ? !rid : !recovered) continue;
    try { pauseStore().restore.run(now, r.symbol, rid); } catch { continue; }
    invalidatePause();
    _log(`pause on ${r.symbol} restored after ${now - r.started_at}ms`);
  }
  if (!pauseStore().allOpen.all(rid).length) {
    _log('round pause cleared: every symbol is priceable again');
    /* Only once EVERY symbol is priceable again does the clock restart: a
       round that can still not price one of its markets is not tradeable. */
    try { comp.pauseClockClose(); } catch (e) { _log(`clock resume failed: ${e.message}`); }
  }
}

function drawdownPauseSymbol(e) {
  const raw = typeof (e && e.symbol) === 'string' ? e.symbol.trim().toUpperCase() : '';
  return raw ? baseOf(raw) : null;
}

/* A round that is over owns no live obligations. Its rows are kept for audit
   and closed so they can never govern the next round. */
/* Releasing a latch is a RECOVERY, not a dismissal.
 *
 * It used to fire before the requested round was even validated, so a typo'd
 * round id returned 400 and still unfroze the live one, and it released while
 * the source was still unpriceable, so another contestant could trade
 * immediately. A latch may only be released once the thing that caused it has
 * actually recovered. */
function releasePauseLatch(roundId) {
  const latch = latchFor(roundId);
  if (!latch) return { released: false, reason: 'no latch for that round' };
  const stillBad = [...latch.symbols].filter((sym) => sym !== 'unknown' && !compPriceReady(sym));
  if (stillBad.length) {
    return { released: false, reason: `still unpriceable: ${stillBad.join(', ')}` };
  }
  const scoreability = competitionRosterScoreability(roundId);
  if (!scoreability.ok) {
    return { released: false, reason: `roster still unscoreable: ${scoreability.reason}` };
  }
  /* And the store has to be working again, or we would release a latch into
     the same failure that created it. Write the obligation we could not write
     before, then close it, so the incident leaves an audit trail. */
  try {
    const now = Date.now();
    for (const sym of latch.symbols) {
      pauseStore().open.run(sym, roundId, latch.since, latch.why);
      pauseStore().restore.run(now, sym, roundId);
    }
  } catch (e) {
    return { released: false, reason: `pause store still failing: ${e.message}` };
  }
  _log(`pause latch released for ${[...latch.symbols].join(', ')} on round ${roundId}`);
  _pauseLatch = null;
  invalidatePause();
  return { released: true, symbols: [...latch.symbols] };
}

/* A round that ends owns no latch either. */
function dropLatchFor(roundId) { if (latchFor(roundId)) { _pauseLatch = null; invalidatePause(); } }

function closeRoundPauses(roundId) {
  try { pauseStore().closeRound.run(Date.now(), roundId); } catch {}
  dropLatchFor(roundId);
  invalidatePause();
}

/* After a restart the persisted source is authoritative for a grace period.
 *
 * Component records are process-local and empty on boot, so whichever socket
 * reconnected first used to win the selection and, under the old blanket
 * mid-round promotion ban, got pinned there for the rest of the round. The source
 * definition must not depend on packet arrival order, so during the grace we
 * wait for the persisted source rather than accepting a substitute. */
const SOURCE_RECOVERY_MS = Number(process.env.PAPER_SOURCE_RECOVERY_MS || 15_000);
let _recoveryUntil = 0;
const _restoredSrc = new Map();   // sym -> source the round was actually priced on

function restoreSources() {
  try {
    for (const r of srcStore().all.all()) {
      _activeSrc.set(r.symbol, r.source);
      _restoredSrc.set(r.symbol, r.source);
    }
    if (_activeSrc.size) {
      _recoveryUntil = Date.now() + SOURCE_RECOVERY_MS;
      _log(`restored followed source for ${_activeSrc.size} symbol(s); honouring it for ${SOURCE_RECOVERY_MS}ms`);
    }
  } catch { /* first boot */ }
}

/** Is a competition round running? Limits optional backup-to-backup promotion. */
function roundLive() {
  try { const r = comp.currentRound(); return !!(r && r.started_at && r.status === 'running'); } catch { return false; }
}

/* How old a component's price is RIGHT NOW: what it was on arrival plus the
   monotonic time since. Wall-clock subtraction was the old answer, and it went
   negative after a backward NTP step, which read as permanently fresh. */
function compAgeMs(v, now = Date.now()) {
  if (!v) return Infinity;
  /* TWO clocks, and the older answer wins.
     The wall clock carries the observation's own age (srcAt, when the provider
     gave us one) and is what a caller simulating time is asking about. The
     monotonic clock cannot be moved by an NTP correction, so it is what
     survives a backward step: wall age would go negative there, and negative
     age reads as permanently fresh. Taking the maximum means a price is as
     stale as EITHER clock believes, which is the fail-closed direction. */
  const base = Number.isFinite(v.srcAt) && v.srcAt > 0 ? v.srcAt : v.ts;
  const wall = Math.max(0, now - base);
  const mono = Number.isFinite(v.recvMono) ? (v.age0 || 0) + Math.max(0, monoNow() - v.recvMono) : 0;
  return Math.max(wall, mono);
}
/** The source we are following for this symbol right now, and its price. */
function activeSource(sym, now = Date.now()) {
  const c = _idxComps.get(sym);
  if (!c) return null;
  /* The observation's OWN time travels with it. `ageMs` is the fail-closed
     age (the worse of the wall and monotonic readings), so `srcAt` is the
     latest instant this quote can honestly claim to have been observed. Every
     downstream deadline is measured from it: publishing an age computed from
     our callback handed a 550ms-old quote a fresh 600ms of life. */
  const at = (key) => { const v = c[key]; if (!v) return null; const ageMs = compAgeMs(v, now); return { key, px: v.px, ts: v.ts, ageMs, srcAt: now - ageMs }; };
  const fresh = (key) => { const v = c[key]; return !!v && compAgeMs(v, now) < staleMsForSym(sym, key); };

  /* Stay where we are while the followed source is still ticking, unless a
     higher-priority one has been solidly back for the failback window. */
  /* During recovery the persisted source is the only acceptable answer. If it
     has not reconnected yet the symbol is simply not priceable, which is the
     honest state — it is NOT an invitation to price from whatever arrived
     first. */
  /* Lazily recover the durable choice.
   *
   * Relying on a boot-time restoreSources() call meant any path that lost
   * in-memory state without going through boot re-selected from scratch. If
   * we have no live memory of this symbol but the database does, that record
   * is the source the round was actually priced on, and it wins until the
   * recovery grace expires. */
  /* Two shapes of the same event: we have no memory of this symbol, or we
     have a followed source whose component record has VANISHED rather than
     merely aged. Both are what a restart looks like from here, and both must
     honour the durable choice instead of re-selecting from whatever socket
     reconnected first. */
  const curKey = _activeSrc.get(sym);
  const lostCurrent = curKey && !c[curKey];
  if ((!_activeSrc.has(sym) || lostCurrent) && !_restoredSrc.has(sym)) {
    try {
      const row = srcStore().one.get(sym);
      if (row && row.source) {
        _restoredSrc.set(sym, row.source);
        _activeSrc.set(sym, row.source);
        if (!_recoveryUntil || now > _recoveryUntil) _recoveryUntil = now + SOURCE_RECOVERY_MS;
      }
    } catch { /* no durable record: ordinary selection applies */ }
  }
  const restored = _restoredSrc.get(sym);
  if (restored && now < _recoveryUntil) {
    if (fresh(restored)) { _restoredSrc.delete(sym); return at(restored); }
    return null;
  }
  if (restored && now >= _recoveryUntil) {
    _restoredSrc.delete(sym);
    _log(`index ${sym}: persisted source ${restored} did not return within the recovery grace`);
  }

  const cur = _activeSrc.get(sym);
  if (cur && fresh(cur)) {
    /* Recover the Lazer primary during a live round too. A fresh backup may
     * be insufficient to advance the round's qualified mark, so keeping it
     * selected until the bell can strand execution on an old Lazer quote.
     * Lazer still earns failback through the existing uninterrupted freshness
     * window; selection then uses the normal durable source, clamp and risk
     * path. Optional backup-to-backup promotion remains disabled mid-round. */
    const liveRound = roundLive();
    const curRank = INDEX_CHAIN.indexOf(cur);
    for (let i = 0; i < curRank; i++) {
      const key = INDEX_CHAIN[i];
      if (liveRound && key !== 'lazer') continue;
      const v = c[key];
      if (fresh(key) && v.freshSince && now - v.freshSince >= failbackMsFor(sym, key)) return at(key);
    }
    return at(cur);
  }
  for (const key of INDEX_CHAIN) {
    if (fresh(key)) return at(key);
  }
  /* NOTHING LIVE MEANS NOTHING LIVE.
   *
   * This used to fall back to the freshest stale record "rather than blanking
   * the market on a one-second lull", and let downstream age checks decide.
   * They did not: compPriceReady tested the generic 5s freshness constant, so
   * a chain where Lazer was 1s dead (250ms rule) and Binance 4.5s dead (4s
   * rule) still reported healthy and carried 1000x. The published rule and the
   * actual eligibility diverged in exactly the failure state the chain exists
   * to define. */
  return null;
}

/** The last source we saw at all, live or not. Telemetry and operator display
 *  only: it must never be used to price anything. */
function lastKnownSource(sym, now = Date.now()) {
  const c = _idxComps.get(sym);
  if (!c) return null;
  let best = null;
  for (const key of INDEX_CHAIN) {
    const v = c[key];
    if (!v) continue;
    if (!best || v.ts > best.ts) best = { key, px: v.px, ts: v.ts, ageMs: compAgeMs(v, now), stale: true };
  }
  return best;
}

/* ON FUTURE TIMESTAMPS: a row stamped ahead of the wall clock reads as
 * negative-age and therefore "fresh" until the clock catches up. This was
 * probed as an eternal-freshness hole, and deliberately NOT guarded: every
 * production caller stamps Date.now() at receipt (lazer ingest, kline mid,
 * venue mid — no remote clock reaches this function), so the only way a
 * stored ts exceeds the wall is OUR clock stepping backwards, and the very
 * next tick overwrites it with the new wall, healing in under a second at
 * feed rate. A read-side age>=0 guard was tried and rejected: it broke every
 * suite that warps time forward by design, for a window production cannot
 * sustain. If a caller is ever added that passes a REMOTE timestamp, clamp it
 * THERE, at that boundary, not here. */
/* How stale a provider observation may be at the door, per source. A quote
   that spent longer than this in a queue, a buffer or a replay is not new
   information: it is history arriving late, and pricing 500x off it is the
   failure the freshness budget exists to prevent. */
const _srcRejects = new Map();          // "sym|key|reason-code" -> last logged ms
function _srcReject(sym, key, reasonCode, detail = reasonCode) {
  noteMarketDiagnosticCounter(sym, 'ingressRejected');
  if (reasonCode === 'stale-observation') noteMarketDiagnosticCounter(sym, 'staleIngress');
  /* Keep changing measurements out of the throttle key. A delayed provider
     batch once produced a different rounded age on every observation, which
     bypassed this 60s guard and amplified the backlog into hundreds of
     synchronous journald writes. The first line still carries the exact age;
     only the suppression identity is stable. */
  const k = `${sym}|${key}|${reasonCode}`;
  const t = Date.now();
  if (t - (_srcRejects.get(k) || 0) < 60_000) return;
  _srcRejects.set(k, t);
  _log(`ingress reject ${sym} via ${key}: ${detail}`);
}
function compUpdate(sym, key, px, now, srcAt = null) {
  /* Capture the last committed verdict before this component observation can
     refresh/fail over the source chain. Boundary and segment work must not be
     certified by component state belonging to a candidate whose risk pass has
     not completed yet. */
  const readinessDetail = {};
  const committedReadyBefore = compPriceReady(sym, now, readinessDetail);
  observeMarketDiagnostic(sym, now, committedReadyBefore, null, readinessDetail);
  /* srcAt is the PROVIDER's timestamp for this observation, when it gives us
     one. Without it the engine stamped its own callback time, so a delayed or
     carried-forward quote entered the book as age zero and the source deadline
     measured how recently we were called rather than how recent the price is. */
  let c = _idxComps.get(sym);
  if (!c) { c = {}; _idxComps.set(sym, c); }
  /* The primary fails CLOSED. If a provider format change removes the publish
     time from Lazer's envelope, its observations become unageable, and an
     unageable price is exactly the thing this gate exists to refuse. The
     venues stay open: Binance's book updates and Coinbase's quotes have no
     usable per-quote stamp, and they are corroboration, not the primary. */
  if (key === 'lazer' && !(Number.isFinite(srcAt) && srcAt > 0)) {
    _srcReject(sym, key, 'primary source arrived with no publish time');
    return;
  }
  if (Number.isFinite(srcAt) && srcAt > 0) {
    if (srcAt - now > SRC_FUTURE_TOLERANCE_MS) { _srcReject(sym, key, 'source timestamp in the future'); return; }
    /* A stamp INSIDE the tolerance is accepted, and it used to become the
       ordering watermark: one observation stamped 1.9s ahead then rejected
       every honest frame behind it for longer than the primary's own 600ms
       budget, forcing an avoidable failover. Clocks disagree a little; that is
       what the tolerance is for. It is not a licence to order the future
       ahead of the present, so a stamp beyond now is clamped to now for
       ordering while its own age is still measured from the stamp. */
    if (srcAt > now) srcAt = now;
    const age = now - srcAt;
    if (age > staleMsForSym(sym, key)) {
      _srcReject(sym, key, 'stale-observation',
        `observation ${Math.round(age)}ms old, past its ${staleMsForSym(sym, key)}ms budget`);
      return;
    }
    /* Ordering, per what the provider's stamp actually MEANS.
       Lazer publishes one envelope per 50ms tick, so a repeated stamp is a
       carried-forward observation and must not price. Coinbase stamps the
       trade that triggered the quote and legitimately repeats it, and
       Binance can emit two events inside one millisecond: for those, only a
       materially OLDER stamp is evidence of replay, and equality is normal
       traffic. Rejecting equality there starved the cross-check component
       within seconds of the first deploy. */
    const prevAt = c[key] && c[key].srcAt;
    if (Number.isFinite(prevAt)) {
      const strict = key === 'lazer';
      /* Equality is normal traffic on the venues (two events in a millisecond,
         a repeated trade stamp); going BACKWARDS is not, at any size. A
         one-second rewind window let a stale quote move the mark and take a
         fresh lifetime with it. */
      if (strict ? srcAt <= prevAt : srcAt < prevAt) {
        _srcReject(sym, key, strict ? 'carried-forward or duplicate publish' : 'out-of-order source timestamp');
        return;
      }
    }
  }
  /* When did this source last come BACK from silence? That is what the
     failback rule measures, so it has to survive the tick that overwrites it. */
  const prevRec = c[key];
  /* Continuity is judged by THIS source's own deadline. Using the global 4s
     let Lazer be silent for seconds — long dead under its 250ms rule — while
     still accumulating one unbroken "recovered" streak, so it was promoted as
     though healthy and then failed straight back. */
  const wasFresh = prevRec && compAgeMs(prevRec, now) < staleMsForSym(sym, key);
  /* Learn this market's rhythm from the observations themselves. */
  cadenceNote(sym, now, key);
  /* age0 is how old the observation ALREADY was when it arrived, and recvMono
     is a monotonic stamp of that arrival. Age is the sum of the two from here
     on, so an observation cannot be handed a second lifetime by being stored,
     re-selected on failover, or looked at after a wall-clock step. */
  const known = Number.isFinite(srcAt) && srcAt > 0;
  c[key] = {
    px,
    ts: now,
    srcAt: known ? srcAt : null,
    age0: known ? Math.max(0, now - srcAt) : 0,
    ageKnown: known,
    recvMono: monoNow(),
    freshSince: wasFresh && prevRec.freshSince ? prevRec.freshSince : now,
  };
  publishFollowedSource(sym, key, now, committedReadyBefore);
}

/* Source selection is an event, not a new component observation. The expiry
   watchdog uses this exact same persistence, clamp and risk path, retaining
   the cached component's own timestamp instead of calling compUpdate with a
   fabricated receipt time. */
function publishFollowedSource(sym, key, now, committedReadyBefore, fallbackOnly = false) {
  const c = _idxComps.get(sym);
  if (!c) return false;
  const act = activeSource(sym, now);
  if (!act) return false;
  const fresh = Object.values(c).filter((x) => compAgeMs(x, now) < COMP_FRESH_MS).map((x) => x.px);
  // Corroboration's common window is not the followed source's expiry rule.
  // A cached source can remain valid under its own unchanged deadline after
  // that common window; it still contributes its one admissible observation.
  if (!fresh.length) fresh.push(act.px);
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
  /* A source switch is a real event: it is the one moment the mark can gap
     without the market having moved, so it is logged rather than silent. */
  const prev = _activeSrc.get(sym);
  const switched = prev !== act.key;
  if (fallbackOnly && !switched) return false;
  if (switched) {
    /* Commit the selection BEFORE it becomes executable. If it cannot be made
       durable the symbol is frozen rather than traded on a source the record
       does not agree with. */
    if (!persistSource(sym, act.key, now)) {
      if (!_srcUnsafe.has(sym)) _srcUnsafe.set(sym, now);
      invalidateRoundMark(sym, 'source_persistence_failed', now);
      notifyPriceabilityLost(sym, 'stale', now);
      return;
    }
    _srcUnsafe.delete(sym);
    _activeSrc.set(sym, act.key);
    if (prev) _log(`index ${sym}: following ${act.key} (was ${prev}, stale ${act.ageMs}ms)`);
  }
  /* Only the followed source moves the index — EXCEPT at the moment of a
     switch, when the new source's price must be published in the same step.
     Waiting for the new source's next tick left a window where the selector
     said Binance, readiness said healthy, and live.map still held Lazer's
     older price: orders and checkpoints priced from a source the engine no
     longer claimed to be following. */
  /* A SOURCE CHANGE IS NOT A PRICE JUMP.
   *
   * The jump clamp asks "did this source print something impossible", and it
   * compares against the last accepted price whoever produced it. On a
   * failover the two sources are simply a basis apart, which is not a bad
   * print and is not something confirmation can resolve: ten more ticks from
   * the new source agree with each other and disagree with the old anchor, so
   * the market sat frozen for the whole confirmation window every time the
   * chain moved. The anchor is re-based to the incoming source instead, and
   * the switch itself is the audit trail. */
  /* Re-base to an INDEPENDENT reference, do not simply forget.
   *
   * Dropping the anchor outright left the next tick judged against whatever
   * the cold-start path could find, and in the rare case where live.map holds
   * no venue mark that is nothing at all: a failover onto a broken backup
   * would have been accepted unchecked. The venue's own mark is a reference
   * neither source produced, so the incoming price is still measured against
   * something, and an ordinary basis of a few bps clears the gate while a
   * genuinely wrong source does not. */
  if (switched) {
    const m0 = live.map.get(sym);
    const ref = m0 && Number(m0.markPrice) > 0 ? Number(m0.markPrice) : 0;
    if (ref > 0) _lastIdx.set(sym, ref); else _lastIdx.delete(sym);
  }
  if (!switched && key !== act.key) return;
  const srt = [...fresh].sort((x, y) => x - y);
  const spreadBps = fresh.length >= 2 && srt[0] > 0
    ? ((srt[srt.length - 1] - srt[0]) / srt[0]) * 1e4
    : 0;
  /* THE SOURCE'S OWN CLOCK, NOT OUR CALLBACK'S.
   *
   * On failover we publish the selected record's PRICE, so its TIMESTAMP has
   * to travel with it. Stamping `now` reset the age of a quote that was
   * already 3.9s old and handed it a fresh 4s lifetime, which defeated the
   * source-specific expiry rule outright: live readiness had moved on while
   * strict history still honoured the dead quote. */
  const previousAccepted = live.map.get(sym)?.acceptedSeq;
  ingestIndexTick(sym, act.px, now, fresh, spreadBps, act.key,
    srcObsAt(act, now), committedReadyBefore);
  const committed = live.map.get(sym);
  return !!(committed && committed.srcKey === act.key
    && committed.acceptedSeq !== previousAccepted);
}

/* A quiet backup can still have a valid observation when the primary expires.
   Previously selection noticed that fact, but the published mark waited for
   the backup's NEXT callback. One cancellable timer per enabled base market
   closes that avoidable gap; absence of an eligible backup remains unpriced.
   Timers are disabled on plain require and are never retries of failed risk. */
const _sourceExpiryTimers = new Map();
let _sourceExpiryEnabled = false;
let _sourceExpiryBusy = false;
function clearSourceExpiryTimers() {
  for (const entry of _sourceExpiryTimers.values()) clearTimeout(entry.timer);
  _sourceExpiryTimers.clear();
}
function stopSourceExpiry() {
  _sourceExpiryEnabled = false;
  clearSourceExpiryTimers();
}
function sourceQuoteExpiry(entry, sym) {
  const receivedAt = Number(entry?.pythAtMs) || 0;
  const sourceAt = Number(entry?.pythSrcAtMs) > 0 ? Number(entry.pythSrcAtMs) : receivedAt;
  return receivedAt > 0 && entry?.srcKey
    ? Math.min(receivedAt + PYTH_STAGE_FRESH_MS,
      sourceAt + staleMsForSym(sym, entry.srcKey)) : null;
}
function armSourceExpiry(sym) {
  if (!_sourceExpiryEnabled || !STAGE_INDEXED.has(sym)) return;
  if (deploymentMaintenanceActive()) { clearSourceExpiryTimers(); return; }
  const old = _sourceExpiryTimers.get(sym);
  if (old) clearTimeout(old.timer);
  _sourceExpiryTimers.delete(sym);
  const entry = live.map.get(sym), now = Date.now();
  const at = sourceQuoteExpiry(entry, sym);
  if (!(at > now)) return;       // never spin on an already-expired mark
  const delay = Math.ceil(at - now);
  const record = { at, monoAt: monoNow() + delay, acceptedSeq: entry.acceptedSeq,
    srcKey: entry.srcKey, sourceAt: entry.pythSrcAtMs,
    component: (_idxComps.get(sym) || {})[entry.srcKey], deferred: false, timer: null };
  record.timer = setTimeout(() => {
    if (_sourceExpiryTimers.get(sym) !== record) return;
    refreshExpiredSources(Date.now());
  }, delay);
  record.timer.unref?.();
  _sourceExpiryTimers.set(sym, record);
}
function startSourceExpiry() {
  if (_sourceExpiryEnabled) return;
  _sourceExpiryEnabled = true;
  for (const sym of STAGE_INDEXED) armSourceExpiry(sym);
}
function refreshExpiredSources(now = Date.now()) {
  if (!_sourceExpiryEnabled || _sourceExpiryBusy) return 0;
  if (deploymentMaintenanceActive()) { clearSourceExpiryTimers(); return 0; }
  _sourceExpiryBusy = true;
  let committed = 0;
  try {
    const mono = monoNow();
    for (const [sym, record] of [..._sourceExpiryTimers]) {
      if (record.at > now && record.monoAt > mono) continue;
      clearTimeout(record.timer);
      _sourceExpiryTimers.delete(sym);
      const entry = live.map.get(sym);
      if (!entry || entry.acceptedSeq !== record.acceptedSeq
          || entry.srcKey !== record.srcKey || entry.pythSrcAtMs !== record.sourceAt) continue;
      try {
        const component = (_idxComps.get(sym) || {})[record.srcKey];
        const remaining = component === record.component
          ? staleMsForSym(sym, record.srcKey) - compAgeMs(component, now) : 0;
        const sourceDeadline = (Number(entry.pythSrcAtMs) || Number(entry.pythAtMs))
          + staleMsForSym(sym, entry.srcKey);
        if (!record.deferred && remaining > 0
            && sourceDeadline <= Number(entry.pythAtMs) + PYTH_STAGE_FRESH_MS) {
          // The published observation is conservatively pulled back by work
          // spent selecting it. Its deadline can therefore precede the SAME
          // component's deadline slightly. Give that component one exact
          // expiry callback; never renew the published quote or retry forever.
          const delay = Math.ceil(remaining);
          record.at = now + delay; record.monoAt = monoNow() + delay;
          record.deferred = true;
          record.timer = setTimeout(() => {
            if (_sourceExpiryTimers.get(sym) === record) refreshExpiredSources(Date.now());
          }, delay);
          record.timer.unref?.();
          _sourceExpiryTimers.set(sym, record);
          continue;
        }
        const readinessDetail = {};
        const readyBefore = compPriceReady(sym, now, readinessDetail);
        observeMarketDiagnostic(sym, now, readyBefore, null, readinessDetail);
        if (publishFollowedSource(sym, null, now, readyBefore, true)) committed++;
      } catch (e) {
        // The existing guarded path remains authoritative. A failed switch
        // waits for a real observation; it does not schedule a 1ms retry loop.
        _log(`source expiry ${sym}: transition failed (${e && e.message})`);
      }
    }
  } finally { _sourceExpiryBusy = false; }
  return committed;
}
// Degraded-state jump clamp: with fewer than 3 fresh components the median
// can't outvote a bad source, so a single-tick move over 50bps is HELD (tick
// dropped, logged) rather than priced. Freeze-don't-guess: a real flash move
// re-asserts itself on the next agreeing tick.
// ── ops alerts (Telegram, per-key 30min cooldown) ────────────────────────
// Pages on the states the audit found log-only: guard halts, clamp storms,
// persistence failure, and a 1000x-tier symbol riding a single component.
const _alertLast = new Map();
const _alertRetry = new Map();
const _alertPending = new Map();
const _opsRequests = new Set();
let _opsStopped = false;
function deliverOpsMessage(msg, signal = null) {
  const tok = process.env.TG_BOT_TOKEN, chat = process.env.OPS_ALERT_CHAT_ID;
  if (_opsStopped || !tok || !chat || signal?.aborted || _opsRequests.size >= 8) return Promise.resolve(false);
  return new Promise((resolve) => {
    const body = JSON.stringify({ chat_id: chat, text: '[paper] ' + msg });
    let finished = false, deadline, req;
    const done = (ok) => {
      if (finished) return;
      finished = true;
      clearTimeout(deadline);
      signal?.removeEventListener('abort', cancel);
      _opsRequests.delete(cancel);
      if (!ok) { try { req?.destroy(); } catch {} }
      resolve(ok === true);
    };
    const cancel = () => done(false);
    _opsRequests.add(cancel);
    try {
      req = require('https').request({ host: 'api.telegram.org', path: `/bot${tok}/sendMessage`,
        method: 'POST', headers: { 'content-type': 'application/json',
          'content-length': Buffer.byteLength(body) }, timeout: 8000 }, (res) => {
        const chunks = []; let bytes = 0;
        res.on('data', (chunk) => {
          if (finished) return;
          bytes += Buffer.byteLength(chunk);
          if (bytes > 16384) return done(false);
          chunks.push(Buffer.from(chunk));
        });
        res.on('aborted', cancel); res.on('error', cancel);
        res.on('end', () => {
          if (finished) return;
          try { done(res.statusCode === 200 && JSON.parse(Buffer.concat(chunks).toString('utf8')).ok === true); }
          catch { done(false); }
        });
      });
      deadline = setTimeout(cancel, 8000); deadline.unref?.();
      signal?.addEventListener('abort', cancel, { once: true });
      req.on('error', cancel); req.on('timeout', cancel);
      req.end(body);
    } catch { done(false); }
  });
}
function tgOps(key, msg) {
  const now = monoNow();
  if (_alertPending.has(key)) return _alertPending.get(key);
  if ((_alertLast.has(key) && now - _alertLast.get(key) < 30 * 60_000)
      || now < (_alertRetry.get(key) || 0)) return Promise.resolve(false);
  // OPS chat ONLY. TG_CHAT_ID is the PUBLIC feed channel — an ops alert
  // landed there once (2026-08-08, deleted). Never fall back to it: with no
  // ops chat configured, alerts stay in the logs.
  const pending = deliverOpsMessage(msg).then((ok) => {
    if (ok) { _alertLast.set(key, monoNow()); _alertRetry.delete(key); }
    else _alertRetry.set(key, monoNow() + 15_000);
    return ok;
  }, () => false).finally(() => _alertPending.delete(key));
  _alertPending.set(key, pending);
  return pending;
}

/* Optional boot-local messages, never a financial gate. One deferred timer,
   one request and at most16 queued events. Reminder cadence and elapsed age
   are monotonic after observing the persisted onset, independent of the
   competition clock. No symbol, raw round label or error text is transmitted. */
function createPauseNotifier({ read, send, wall = Date.now, mono = monoNow,
  schedule = setTimeout, cancelTimer = clearTimeout } = {}) {
  const allowed = new Set(['identity', 'restart', 'boundary', 'risk', 'exposure', 'feed']);
  let stopped = false, timer = null, active = null, pending = null, serial = 0;
  let queue = [];
  const stats = { sent: 0, failed: 0, dropped: 0 };
  const age = (incident) => Math.max(0, incident.age0 + mono() - incident.monoAt);
  const add = (kind, incident) => {
    if (queue.length >= 16) { queue.splice(pending ? 1 : 0, 1); stats.dropped++; }
    queue.push({ kind, incident, ageMs: age(incident), retryAt: 0 });
  };
  const arm = (delay) => {
    if (stopped || timer) return;
    timer = schedule(() => { timer = null; poll(); }, delay); timer.unref?.();
  };
  const pump = () => {
    if (stopped || pending || !queue.length || mono() < queue[0].retryAt) return;
    const event = queue[0], incident = event.incident;
    // A retry reports the age actually observed, not an invented new state
    // when the latest durable-state read is unavailable.
    const seconds = Math.floor(event.ageMs / 1000);
    const label = crypto.createHash('sha256').update(incident.roundId).digest('hex').slice(0, 12);
    const text = { onset: 'pause began', reminder: 'still paused',
      resume: 'trading resumed', ended: 'round ended while paused; not a trading resume' }[event.kind];
    const controller = new AbortController(); pending = { event, controller };
    Promise.resolve().then(() => stopped ? false : send(
      `competition ${text}; round#${label}; incident=${incident.id}; cause=${incident.reason}; observed pause age=${seconds}s`, controller.signal))
      .then((ok) => {
        if (stopped) return;
        if (ok === true) {
          stats.sent++; queue = queue.filter((item) => item !== event);
          if (event.kind === 'reminder') incident.reminderAt = mono() + 60_000;
        } else { stats.failed++; event.retryAt = mono() + 15_000; }
      }, () => { if (!stopped) { stats.failed++; event.retryAt = mono() + 15_000; } })
      .finally(() => { pending = null; if (!stopped) { pump(); arm(5000); } });
  };
  function poll() {
    if (stopped) return;
    let state;
    try { state = read(); } catch {
      // Retry already observed lifecycle facts; an unreadable current state
      // never fabricates recovery or a new "still paused" assertion.
      pump(); arm(5000); return;
    }
    if (active && (!state || state.roundId !== active.roundId)) { add('ended', active); active = null; }
    if (state?.paused && !active) {
      active = { id: ++serial, roundId: String(state.roundId), monoAt: mono(),
        age0: Math.max(0, wall() - (Number(state.since) || wall())),
        reason: allowed.has(state.reasonClass) ? state.reasonClass : 'risk', reminderAt: mono() + 60_000 };
      add('onset', active);
    } else if (active && state && !state.paused && !state.blocked) {
      add('resume', active); active = null;
    }
    queue = queue.filter((event) => event === pending?.event || event.kind !== 'reminder' || event.incident === active);
    if (active && mono() >= active.reminderAt
        && !queue.some((event) => event.kind === 'reminder' && event.incident === active)) add('reminder', active);
    pump();
    if (active || queue.length || pending) arm(5000);
  }
  return {
    wake() { if (stopped) return; if (timer) cancelTimer(timer); timer = null; arm(0); },
    stop() { stopped = true; if (timer) cancelTimer(timer); timer = null; queue = []; pending?.controller.abort(); },
    status: () => ({ ...stats, pending: !!pending, queued: queue.length, active: !!active }),
  };
}
function startPauseNotifications() {
  _opsStopped = false;
  if (_pauseNotifier || !process.env.TG_BOT_TOKEN || !process.env.OPS_ALERT_CHAT_ID) return;
  _pauseNotifier = createPauseNotifier({ send: deliverOpsMessage, read: () => {
    const round = comp.currentRound();
    if (!round || round.status !== 'running') return null;
    // Read durable rows here rather than a price-path cache populated inside
    // a transaction which may subsequently have rolled back.
    const rows = pauseStore().allOpen.all(round.id);
    const latch = latchFor(round.id);
    const pause = latch ? { since: latch.since, why: latch.why,
      symbols: [...latch.symbols], unpersisted: true }
      : rows.length ? { since: rows[0].started_at, why: rows[0].reason,
        symbols: rows.map((row) => row.symbol) } : null;
    return { roundId: round.id, paused: !!pause || !!round.paused_since,
      blocked: !!round.blocked_reason, since: pause?.since || round.paused_since,
      reasonClass: pause ? pauseReasonClass(pause) : 'risk' };
  } });
  _pauseNotifier.wake();
}
function stopOpsNotifications() {
  _opsStopped = true; _pauseNotifier?.stop(); _pauseNotifier = null;
  clearTimeout(_pendHaltTimer); clearTimeout(_pendResumeTimer);
  _pendHaltTimer = null; _pendResumeTimer = null;
  _pendHalts.length = 0; _pendResumes.length = 0;
  for (const cancel of [..._opsRequests]) cancel();
}
// Storm batching: a volatility flush trips many markets within seconds and
// used to send a dozen DMs for one event. Collect pages briefly; three or
// more together become one summary (per-market detail stays in the log).
const _pendHalts = [];
let _pendHaltTimer = null;
function pageHalt(sym, msg) {
  if (_opsStopped) return;
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
  if (_opsStopped) return;
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
/* The cross-venue divergence halt. Off by default: it is a blended-index
   guard and contradicts named-source pricing (see guardCheck). */
const GUARD_CROSS_VENUE = process.env.PAPER_GUARD_CROSS_VENUE === '1';
const _clampCount = new Map();   // sym -> [ts,...] recent clamps
/* Symbols whose price is mid-confirmation. Nothing may be priced off a symbol
   in this state: not an order, not a close, not a liquidation, not a
   checkpoint. It is a first-class "we do not currently know the price". */
const _confirming = new Map();   // sym -> { since, from, to }
/* How tightly the confirming ticks must cluster. 30bps was the pair-to-pair
   tolerance of the old rolling comparison; as a fixed band it is the total
   width the whole cluster may occupy. */
const CLAMP_CLUSTER = Number(process.env.PAPER_CLAMP_CLUSTER || 0.003);
const _clampAccept = new Map();  // sym -> {n, px} self-agreeing held-tick streak
/* A held run that keeps going ONE way. The cluster test above wants the held
   prints to sit still within 30bps of the first one, which a market moving
   fast never does: every print lands past the last, the cluster restarts,
   and the hold lasts as long as the move (29 s measured). A real move
   persists in one direction; a glitching feed scatters. So a run of the
   streak length all on the same side, each step inside the per-tick
   ceiling, is accepted at its latest print. */
const _clampRun = new Map();     // sym -> {dir, n, px, since}
const _monoSince = new Map();    // sym -> ts since single-component (1000x tier only)
const CLAMP_JUMP = 0.005;
/* ── how far THIS market can honestly move in one tick ───────────────────────
 *
 * One 50bps rule served BTC and a meme token, and it is the wrong number for
 * at least one of them. Measured over three hours of our own index ticks, the
 * 99.9th percentile single-tick move is under 2bps on the majors and 43bps on
 * CHIP, 100bps on ANSEM: those markets trip a 50bps clamp several times an
 * hour doing nothing unusual, and each trip freezes them while ten more ticks
 * confirm what was never in doubt.
 *
 * So the clamp follows the market: a multiple of its own observed tail, with
 * a floor for each base market and a ceiling so a genuinely broken feed still
 * cannot walk the mark anywhere. LIT, VVV, SKR, CHIP and ANSEM start at 75bps;
 * every other market retains the 50bps floor. The majors sit on that unchanged
 * floor, which is the point: their tail is tiny.
 *
 * The confirmation streak scales too. Ten agreeing ticks is half a second on a
 * 50ms feed and a full minute on one that quotes every six seconds, and that
 * minute is the freeze people actually notice. */
const CLAMP_JUMP_MAX = Number(process.env.PAPER_CLAMP_JUMP_MAX || 0.05);
const CLAMP_TAIL_MULT = Number(process.env.PAPER_CLAMP_TAIL_MULT || 4);
/* TAKE WHAT THE MARKET GIVES (owner's call, 2026-09-11). Measured over 48
   hours, half of the held moves on the thin markets were one-second wicks
   the feed itself published and walked back, and the other half were real
   moves; both are what the market gave. Holding them cost two seconds a
   time and produced an alert every few minutes for nothing a player could
   act on. So every market outside the four majors starts at a 2% gate: a
   print under 2% is accepted on the tick, a print over it still needs the
   agreeing streak, which is what keeps a broken feed from walking the mark.
   One gate for every indexed market, majors included (owner's call, later
   the same day): at the size it is set to, it is a feed sanity stop, not a
   market opinion, and one number is simpler to explain than two. */
/* PAPER_CLAMP_JUMP_WIDE lets the owner dial this with a restart instead of a
   release: 0.05 makes the guard bite only on prints no market produces. */
const CLAMP_JUMP_WIDE = (() => { const v = Number(process.env.PAPER_CLAMP_JUMP_WIDE); return Number.isFinite(v) && v > 0 && v <= 1 ? v : 0.02; })();
// A valid ceiling also bounds the cold floor; malformed configuration must
// never turn this fallback into NaN and silently disable the jump comparison.
const clampFloorFor = (sym) => {
  const b = baseOf(sym);
  /* The wide gate is for markets the engine indexes and offers. A symbol it
     does not know keeps the conservative floor. */
  const floor = STAGE_INDEXED.has(b) ? CLAMP_JUMP_WIDE : CLAMP_JUMP;
  return Number.isFinite(CLAMP_JUMP_MAX) && CLAMP_JUMP_MAX > 0
    ? Math.min(CLAMP_JUMP_MAX, floor) : floor;
};
const _moveRing = new Map();     // sym -> recent |move| as a fraction
const _clampOf = new Map();      // sym -> { jump, samples, at }
function moveNote(sym, px, prev, now) {
  if (!(px > 0) || !(prev > 0)) return;
  const m = Math.abs(px - prev) / prev;
  if (!(m >= 0) || m > CLAMP_JUMP_MAX) return;          // an outlier cannot widen its own gate
  let ring = _moveRing.get(sym);
  if (!ring) { ring = []; _moveRing.set(sym, ring); }
  ring.push(m);
  if (ring.length > 512) ring.splice(0, ring.length - 512);
  if (ring.length < 128) return;
  const c = _clampOf.get(sym);
  if (c && now - c.at < 60_000) return;
  const sorted = [...ring].sort((a, b) => a - b);
  const tail = sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * 0.99))];
  _clampOf.set(sym, { jump: Math.min(CLAMP_JUMP_MAX, Math.max(clampFloorFor(sym), tail * CLAMP_TAIL_MULT)), samples: ring.length, at: now });
}
const clampJumpFor = (sym) => (_clampOf.get(baseOf(sym)) || {}).jump || clampFloorFor(sym);
/* Ten ticks on a fast feed, fewer on a slow one, never below three: the point
   is a cluster that agrees, not a fixed count that costs a minute. */
function clampStreakFor(sym) {
  const c = _cadence.get(`${baseOf(sym)}|usdt`) || _cadence.get(`${baseOf(sym)}|lazer`) || null;
  const gapMs = c && c.budgetMs ? c.budgetMs / CADENCE_HEADROOM : 0;
  if (!(gapMs > 0)) return 10;
  return gapMs > 2000 ? 3 : gapMs > 500 ? 5 : 10;
}
const _lastIdx = new Map();   // sym -> last accepted px
/* Accepted-index history has a separate bounded SQLite outbox. The hot path
   only appends to RAM; immutable batches are FULL-synced once a second. This
   leaves at most one healthy timer interval unsealed on a hard process exit,
   while every sealed batch survives failed/ambiguous HTTP acknowledgments and
   restart. The financial database and its backup size budget are unchanged. */
const INDEX_BATCH_MAX_ROWS = 4000;
const INDEX_OUTBOX_MAX_BYTES = 64 * 1024 * 1024;
function createIndexOutbox(filename, { maxBytes = INDEX_OUTBOX_MAX_BYTES,
  namespace = () => crypto.randomBytes(16).toString('hex') } = {}) {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1 || maxBytes > INDEX_OUTBOX_MAX_BYTES) throw new Error('outbox_limit');
  const store = new (require('better-sqlite3'))(filename);
  try {
    store.pragma('busy_timeout = 0'); // history must never wait behind another writer
    store.pragma('page_size = 4096');
    store.pragma('max_page_count = 32768'); // 128 MiB, separate from paper.db
    store.pragma('journal_mode = WAL');
    store.pragma('synchronous = FULL');
    store.pragma('wal_autocheckpoint = 256');
    store.pragma('journal_size_limit = 8388608');
    const version = store.pragma('user_version', { simple: true });
    if (version !== 0 && version !== 1) throw new Error('outbox_version');
    store.exec(`CREATE TABLE IF NOT EXISTS index_outbox_state (
      id INTEGER PRIMARY KEY CHECK(id=1), namespace TEXT NOT NULL,
      next_seq INTEGER NOT NULL, bytes INTEGER NOT NULL DEFAULT 0,
      batches INTEGER NOT NULL DEFAULT 0, rows INTEGER NOT NULL DEFAULT 0);
      CREATE TABLE IF NOT EXISTS index_outbox_batches (
      seq INTEGER PRIMARY KEY, body TEXT NOT NULL, sha256 TEXT NOT NULL,
      bytes INTEGER NOT NULL, rows INTEGER NOT NULL, created_at INTEGER NOT NULL);
      PRAGMA user_version=1;`);
    if (!store.prepare('SELECT id FROM index_outbox_state WHERE id=1').get()) {
      const id = namespace();
      if (!/^[0-9a-f]{32}$/.test(id)) throw new Error('outbox_namespace');
      store.prepare('INSERT INTO index_outbox_state(id,namespace,next_seq) VALUES(1,?,1)').run(id);
    }
    const state = store.prepare('SELECT * FROM index_outbox_state WHERE id=1');
    const head = store.prepare('SELECT * FROM index_outbox_batches ORDER BY seq LIMIT 1');
    const oldest = store.prepare('SELECT created_at FROM index_outbox_batches ORDER BY seq LIMIT 1');
    const put = store.prepare('INSERT INTO index_outbox_batches VALUES(?,?,?,?,?,?)');
    const del = store.prepare('DELETE FROM index_outbox_batches WHERE seq=? AND sha256=?');
    let current = state.get();
    let oldestAt = oldest.get()?.created_at || null;
    const sane = () => {
      if (!current || !/^[0-9a-f]{32}$/.test(current.namespace)
          || !Number.isSafeInteger(current.next_seq) || current.next_seq < 1
          || !['bytes', 'batches', 'rows'].every(k => Number.isSafeInteger(current[k]) && current[k] >= 0)) throw new Error('outbox_state');
    };
    sane();
    const seal = store.transaction((rows, at) => {
      current = state.get(); sane();
      if (!Array.isArray(rows) || !rows.length || rows.length > INDEX_BATCH_MAX_ROWS
          || !Number.isSafeInteger(at) || at <= 0) throw new Error('outbox_batch');
      const body = JSON.stringify({ v: 2, key: `${current.namespace}:${current.next_seq}`, rows });
      const bytes = Buffer.byteLength(body);
      if (bytes > 2_000_000 || bytes + current.bytes > maxBytes) throw new Error('outbox_full');
      const sha = crypto.createHash('sha256').update(body).digest('hex');
      put.run(current.next_seq, body, sha, bytes, rows.length, at);
      store.prepare('UPDATE index_outbox_state SET next_seq=next_seq+1,bytes=bytes+?,batches=batches+1,rows=rows+? WHERE id=1').run(bytes, rows.length);
      return { seq: current.next_seq, bytes, rows: rows.length, createdAt: at };
    });
    const ack = store.transaction((seq, sha) => {
      const row = head.get();
      if (!row || row.seq !== seq || row.sha256 !== sha) throw new Error('outbox_head_changed');
      if (del.run(seq, sha).changes !== 1) throw new Error('outbox_head_changed');
      store.prepare('UPDATE index_outbox_state SET bytes=bytes-?,batches=batches-1,rows=rows-? WHERE id=1').run(row.bytes, row.rows);
    });
    return {
      seal(rows, at = Date.now()) { const result = seal(rows, at); current = state.get(); oldestAt ||= at; return result; },
      head() {
        const row = head.get();
        if (row && crypto.createHash('sha256').update(row.body).digest('hex') !== row.sha256) throw new Error('outbox_corrupt');
        return row || null;
      },
      ack(seq, sha) { ack(seq, sha); current = state.get(); oldestAt = oldest.get()?.created_at || null; },
      status() { return { queuedBytes: current.bytes, queuedBatches: current.batches,
        queuedRows: current.rows, oldestQueuedAt: oldestAt, maxQueuedBytes: maxBytes }; },
      close() { store.close(); },
    };
  } catch (error) { store.close(); throw error; }
}
const _tickBuf = [];
let _persistFails = 0;
const _persistLast = new Map();   // own throttle: the SSE gate map only advances when clients are connected
function persistTick(sym, px, now, nComps, spreadBps, srcKey = null, srcTs = null, riskOk = true, deliverySeq = 0) {
  if (_tickBuf.length >= INDEX_BATCH_MAX_ROWS) { _tickPersistence.droppedRows++; return; }
  /* The audit row used to hold price and time alone, so a fill could never be
     joined back to the observation that caused it. It now carries the accepted
     event's number, which source produced it, when that source says it
     happened, and WHETHER RISK ACCEPTED IT.
     The verdict was being passed as an eighth argument to a seven-parameter
     function, so it was silently dropped and a refused observation was stored
     as though the engine had stood behind it, under the previous accepted
     event's number. A refused observation is not an accepted event: it is
     recorded with the verdict and with no accepted number at all. */
  _tickBuf.push({
    s: sym, p: px, t: now, n: nComps, sp: Number(spreadBps) || 0,
    q: riskOk ? _evAccepted : null, src: srcKey || null,
    st: Number(srcTs) > 0 ? Number(srcTs) : null,
    ok: riskOk ? 1 : 0,
    /* The delivery id the frame went out under, so an audit row can be joined
       to the exact frame a screen was looking at. Zero means the observation
       was never published (no subscribers, or thinned). */
    dq: Number(deliverySeq) > 0 ? Number(deliverySeq) : 0,
  });
}
let _tickWrite = null;
let _tickOutbox = null;
let _tickPersistTimer = null;
let _tickPumpTimer = null;
let _tickDrain = false;
const _tickPersistence = { droppedRows: 0, lastSuccessAt: null, lastFailureAt: null,
  queueError: null, nextAttemptAt: 0, lastAckDelayMs: null, unsealedWindowMs: 1000 };
function indexOutbox() {
  if (!_tickOutbox) _tickOutbox = createIndexOutbox(db.name === ':memory:'
    ? ':memory:' : db.name + '.index-outbox.sqlite');
  return _tickOutbox;
}
function tickPersistenceStatus() {
  return { ..._tickPersistence, inFlight: !!_tickWrite, bufferedRows: _tickBuf.length,
    consecutiveFailures: _persistFails, ...(_tickOutbox?.status() || {
      queuedBytes: 0, queuedBatches: 0, queuedRows: 0, oldestQueuedAt: null, maxQueuedBytes: INDEX_OUTBOX_MAX_BYTES }) };
}
function sealPersistTicks() {
  if (!_tickBuf.length) return true;
  try {
    indexOutbox().seal(_tickBuf);
    _tickBuf.length = 0;
    _tickPersistence.queueError = null;
    return true;
  } catch (error) {
    _tickPersistence.queueError = error.message === 'outbox_full' ? 'full' : 'unavailable';
    _tickPersistence.lastFailureAt = Date.now();
    tgOps('persist-outbox', `index history durable queue ${_tickPersistence.queueError}; RAM buffer remains bounded and history can lose observations`);
    return false;
  }
}
function startTickPersistence() {
  if (_tickPersistTimer) return;
  _tickDrain = false;
  try { indexOutbox(); } catch { _tickPersistence.queueError = 'unavailable'; }
  _tickPersistTimer = setInterval(() => {
    sealPersistTicks();
    if (Date.now() >= _tickPersistence.nextAttemptAt) flushPersistTicks();
  }, 1000);
  _tickPersistTimer.unref?.();
}
function flushPersistTicks({ request = require('http').request, timeoutMs = 5000 } = {}) {
  sealPersistTicks();
  const tok = process.env.WAREHOUSE_API_TOKEN || '';
  if (_tickWrite || !tok) return false;
  let row;
  try { row = indexOutbox().head(); }
  catch { _tickPersistence.queueError = 'unavailable'; return false; }
  if (!row) return false;
  const body = row.body;
  const pending = { seq: row.seq, cancel: null };
  _tickWrite = pending;
  let req, timer, finished = false;
  const finish = (ok) => {
    if (finished) return;
    finished = true;
    clearTimeout(timer);
    if (_tickWrite === pending) _tickWrite = null;
    if (ok) {
      try { indexOutbox().ack(row.seq, row.sha256); }
      catch { ok = false; _tickPersistence.queueError = 'unavailable'; }
    }
    if (ok) {
      _persistFails = 0;
      _tickPersistence.lastSuccessAt = Date.now();
      _tickPersistence.lastAckDelayMs = Math.max(0, Date.now() - row.created_at);
      _tickPersistence.nextAttemptAt = 0;
      _tickPersistence.queueError = null;
      // An old batch can repair a bucket already cached as historical. Clear
      // only on delayed delivery, not once a second during healthy writes.
      if (_tickPersistence.lastAckDelayMs >= KLINE_PERSIST_GRACE_MS) {
        _idxCandleCache.clear(); _klineCache.clear(); _lineCache.clear();
      }
      if (!_tickDrain && _tickPersistTimer && indexOutbox().status().queuedBatches && !_tickPumpTimer) {
        _tickPumpTimer = setTimeout(() => { _tickPumpTimer = null; flushPersistTicks(); }, 200);
        _tickPumpTimer.unref?.();
      }
    } else {
      _tickPersistence.lastFailureAt = Date.now();
      _tickPersistence.nextAttemptAt = Date.now() + Math.min(30000, 1000 * 2 ** Math.min(5, _persistFails));
      if (++_persistFails >= 5) tgOps('persist', `index history delivery failing (${_persistFails} consecutive attempts); immutable batch retained for retry`);
    }
  };
  const expire = () => {
    if (finished) return;
    finish(false);
    try { req?.destroy(); } catch {}
  };
  pending.cancel = expire;
  // An absolute deadline also bounds DNS, connect and a continuously trickling
  // response. Socket timeout alone neither aborts nor limits total lifetime.
  timer = setTimeout(expire, timeoutMs);
  timer.unref?.();
  try {
    req = request({ host: '127.0.0.1', port: 9100, path: '/internal/index-ticks', method: 'POST', headers: { 'content-type': 'application/json', authorization: 'Bearer ' + tok, 'content-length': Buffer.byteLength(body) }, timeout: timeoutMs }, (res) => {
      if (finished) { res.destroy?.(); return; }
      res.on('error', expire);
      res.on('aborted', expire);
      res.on('end', () => finish(res.statusCode >= 200 && res.statusCode < 300));
      res.on('close', () => { if (!res.complete) expire(); });
      res.resume();
    });
    req.on('error', expire);
    req.on('timeout', expire);
    req.end(body);
  } catch { expire(); }
  return true;
}
function drainTickPersistence(timeoutMs = 1500) {
  _tickDrain = true;
  if (_tickPersistTimer) clearInterval(_tickPersistTimer);
  if (_tickPumpTimer) clearTimeout(_tickPumpTimer);
  _tickPersistTimer = _tickPumpTimer = null;
  sealPersistTicks();
  return new Promise((resolve) => {
    const deadline = Date.now() + Math.max(0, Math.min(1500, Number(timeoutMs) || 0));
    let attempted = !!_tickWrite;
    const finish = () => {
      _tickWrite?.cancel?.();
      // Feed callbacks may have appended while shutdown waited. Seal that
      // last tail synchronously, then return directly to the process exit.
      const sealed = sealPersistTicks();
      const status = tickPersistenceStatus();
      try { _tickOutbox?.close(); } catch {}
      _tickOutbox = null;
      resolve({ sealed, pendingBatches: status.queuedBatches, pendingRows: status.queuedRows });
    };
    const poll = () => {
      if (!_tickWrite) {
        if (attempted || Date.now() >= deadline) return finish();
        attempted = true;
        if (!flushPersistTicks({ timeoutMs: Math.max(1, deadline - Date.now()) })) return finish();
      }
      if (Date.now() >= deadline) return finish();
      setTimeout(poll, Math.min(25, Math.max(1, deadline - Date.now())));
    };
    poll();
  });
}

/* ONE observation time, and the most conservative one available. The provider
   stamp when we have it, pulled back to the fail-closed age whenever our own
   clocks say the quote is older than it claims, and never in the future. */
function srcObsAt(act, now) {
  const stamped = Number(act && act.ts) > 0 ? Number(act.ts) : now;
  const measured = Number(act && act.srcAt) > 0 ? Number(act.srcAt) : stamped;
  return Math.min(stamped, measured, now);
}
function ingestIndexTick(sym, px, now, comps, spreadBps, srcKey = null,
  srcAt = null, committedReadyHint = null) {
  const monoEntry = monoNow();
  const nComps = (comps || []).length;
  const committedReady = typeof committedReadyHint === 'boolean'
    ? committedReadyHint : compPriceReady(sym, now);
  const confirmingBefore = _confirming.has(sym)
    ? { ..._confirming.get(sym) } : null;
  const clampBefore = _clampAccept.has(sym)
    ? { ..._clampAccept.get(sym) } : null;
  const restoreCandidateGuards = () => {
    if (confirmingBefore) _confirming.set(sym, confirmingBefore);
    else _confirming.delete(sym);
    if (clampBefore) _clampAccept.set(sym, clampBefore);
    else _clampAccept.delete(sym);
  };
  /* THE CLAMP IS SOURCE-CENTRIC, NOT COUNT-CENTRIC.
   *
   * This was gated on `nComps < 3`, which belonged to the blended index: three
   * sources could outvote one another, so the clamp was only needed when they
   * could not. Under one published source that reasoning is gone — backup
   * records do not set the price, and their mere freshness must not disable
   * the only defence the followed source has. On the majors, Lazer plus two
   * Binance books routinely make nComps >= 3, so the clamp was absent exactly
   * where 1000x is used. */
  {
    let prev = _lastIdx.get(sym);
    if (!(prev > 0)) {
      // post-restart cold start: no reference yet — seed from the venue mark
      // (sits ~10bps off the index, well inside the 50bps clamp) so the first
      // degraded-state tick is not accepted blind
      const m0 = live.map.get(sym) || (snapFile()?.markets || {})[sym];
      if (m0 && Number(m0.markPrice) > 0) prev = Number(m0.markPrice);
    }
    if (prev > 0) moveNote(sym, px, prev, now);
    if (prev > 0 && Math.abs(px - prev) / prev > clampJumpFor(sym)) {
      // Escape hatch: freeze-don't-guess deadlocks after a REAL move — the
      // reference stays at the old level and every honest tick is held
      // forever (PUMP: 2,341 held ticks at a frozen ref, 2026-08-22). A
      // genuine move re-asserts as held ticks that agree with EACH OTHER;
      // 10 in a row within 30bps = accept the new level. A glitching feed
      // scatters and never builds the streak.
      /* CONFIRM AGAINST A FIXED CANDIDATE, NOT A ROLLING ANCHOR.
       *
       * This compared each held tick to the previous held tick and then moved
       * the anchor onto it, so a staircase counted as agreement: ten ticks
       * each 29bps above the last were accepted as a "30bps agreement" and
       * carried the mark 325bps away from the level that was proposed. The
       * candidate is now fixed at the first held tick and every subsequent
       * tick is measured against THAT, so the cluster cannot walk. */
      const acc = _clampAccept.get(sym);
      if (acc && Math.abs(px - acc.px) / acc.px < CLAMP_CLUSTER) {
        acc.n += 1;
        acc.lo = Math.min(acc.lo, px); acc.hi = Math.max(acc.hi, px);
        /* The whole cluster, not just each pair, has to stay inside the band. */
        if ((acc.hi - acc.lo) / acc.px >= CLAMP_CLUSTER) _clampAccept.set(sym, { n: 1, px, lo: px, hi: px, since: now });
      } else {
        _clampAccept.set(sym, { n: 1, px, lo: px, hi: px, since: now });
      }
      const dir = px > prev ? 1 : -1;
      const run = _clampRun.get(sym);
      if (run && run.dir === dir && Math.abs(px - run.px) / run.px <= CLAMP_JUMP_MAX) {
        run.n += 1; run.px = px;
      } else {
        _clampRun.set(sym, { dir, n: 1, px, since: now });
      }
      const streak = clampStreakFor(sym);
      const c0 = _clampAccept.get(sym);
      const r0 = _clampRun.get(sym);
      /* A run is accepted only inside the per-tick ceiling measured from the
         anchor: a real market can walk 3% in ten prints, a broken feed
         cannot walk the mark anywhere it likes. */
      const runOk = (r0?.n || 0) >= streak && Math.abs(px - prev) / prev <= CLAMP_JUMP_MAX;
      if ((c0?.n || 0) >= streak || runOk) {
        _clampAccept.delete(sym);
        _clampRun.delete(sym);
        _confirming.delete(sym);
        if ((c0?.n || 0) >= streak) _log(`index step accepted ${sym}: ${prev.toFixed(6)} -> ${px.toFixed(6)} after ${c0.n} ticks inside a ${((c0.hi - c0.lo) / c0.px * 1e4).toFixed(1)}bps cluster`);
        else _log(`index run accepted ${sym}: ${prev.toFixed(6)} -> ${px.toFixed(6)} after ${r0.n} ticks moving ${dir > 0 ? 'up' : 'down'} together`);
      } else {
        /* FREEZE THE MARKET, NOT JUST THE NUMBER.
         *
         * Holding the tick while leaving the symbol tradable created a
         * deterministic edge: the source had already moved, the engine kept
         * quoting the old mark, and a trader could enter at the stale price
         * and collect the whole withheld step when it confirmed. Measured at
         * 1000x that was 600% of committed margin, manufactured by the hold
         * rather than by predicting anything. While a jump is confirming the
         * symbol is not priceable at all. */
        const dSince = (_clampAccept.get(sym) || {}).since || now;
        const newlyConfirming = !_confirming.has(sym);
        _confirming.set(sym, { since: dSince, from: prev, to: px });
        disputeOpen(sym, dSince, prev, px);
        if (newlyConfirming) notifyPriceabilityLost(sym, 'stale', now);
        _log(`index clamp ${sym}: ${prev.toFixed(6)} -> ${px.toFixed(6)} via ${srcKey || 'unknown'} (${(Math.abs(px - prev) / prev * 1e4).toFixed(1)}bps against a ${(clampJumpFor(sym) * 1e4).toFixed(0)}bps gate), CONFIRMING (market frozen)`);
        const arr = (_clampCount.get(sym) || []).filter((t) => now - t < 5 * 60_000); arr.push(now); _clampCount.set(sym, arr);
        /* Paged only while a round is running: outside one, a held print on
           a thin market is weather nobody needs to act on, and it was filling
           the ops channel every few minutes. The log keeps every hold. */
        if (arr.length >= 5 && roundLive()) tgOps('clamp:' + sym, `index clamp storm ${sym}: ${arr.length} held ticks in 5min`);
        return;
      }
    } else {
      _clampAccept.delete(sym);
      _clampRun.delete(sym);
      _confirming.delete(sym);
    }
  }
  /* Snapshot the anchor before the candidate replaces it. Capturing it after
     the write, which is what the rollback used to do, restored the refused
     price as the thing the next tick's jump clamp measures against: a 51bps
     move then reads as 2bps and walks straight past confirmation. */
  const prevIdx = _lastIdx.get(sym);
  _lastIdx.set(sym, px);
  if (FAST_RELAY.has(sym)) {
    if (nComps <= 1) {
      if (!_monoSince.has(sym)) _monoSince.set(sym, now);
      else if (now - _monoSince.get(sym) > 5 * 60_000 && roundLive()) tgOps('mono:' + sym, `major ${sym} riding a SINGLE index component for 5min+ (no cross-check)`);
    } else _monoSince.delete(sym);
  }
  const halted = guardCheck(sym, comps || [px], now);
  const cur = live.map.get(sym) || { symbol: sym };
  const prevEntry = live.map.get(sym) || null;   // restored if the risk pass fails
  if (halted) {
    if (!cur.indexHalt) {
      live.map.set(sym, { ...cur, indexHalt: true });
      notifyPriceabilityLost(sym, 'stale', now);
    }
    restoreCandidateGuards();
    return;                                          // frozen: no price, no eval
  }
  /* Record with the component count observed at acceptance. A single-source
     price is still useful to the public product and the chart; it simply must
     never satisfy a strict competition lookup. */
  /* The index is published BEFORE the verdict is recorded, because the verdict
     asks whether this very price is competition-valid and that question reads
     the published index age. Recording first meant every mark was judged
     against the PREVIOUS tick's freshness. */
  /* srcKey travels WITH the published price: everything downstream judges
     freshness against the source that actually set this number. */
  live.map.set(sym, { ...cur, indexHalt: false, markPrice: Number(cur.markPrice) > 0 ? cur.markPrice : px, pythPrice: px, pythAtMs: now, pythSrcAtMs: Number(srcAt) > 0 ? Number(srcAt) : now, srcKey: srcKey || cur.srcKey || null, lastUpdatedMs: now, pythBasis: cur.pythBasis != null ? cur.pythBasis : (Number(cur.markPrice) > 0 ? null : 0) });
  /* ONE canonical verdict for this tick.
   *
   * This used to hand recordMark the FRESH component count while the live gate
   * counted fresh-plus-corroborating, so a market admitted for trading by a
   * quiet second venue stored a history row saying "one source" and the
   * checkpoint that decides the result then refused the very instant the
   * engine had accepted. Round seven's brief claimed this was fixed; it was
   * fixed only for callers that recorded a mark by hand. The production route
   * still disagreed with itself. Now the verdict is computed once, here, and
   * everything downstream reads it. */
  const prevLastMsg = live.lastMsgMs;
  live.lastMsgMs = now;
  const candidateEntry = live.map.get(sym);
  const riskProbeEntry = _riskBlocked.has(sym) ? candidateEntry : null;
  // Reserve, but do not consume, the public accepted-event identity. No
  // asynchronous work can interleave before this event commits or rolls back.
  let roundCandidate = null;
  const expiryBudget = staleMsForSym(sym, srcKey);
  const ageAtEntry = Math.max(0,
    now - (Number(srcAt) > 0 ? Number(srcAt) : now));
  const riskDeadlineMono = monoEntry + Math.max(0, expiryBudget - ageAtEntry);
  /* COMMIT, THEN PUBLISH.
   *
   * The relay used to write the frame first and evaluate risk afterwards, with
   * the evaluation's failure swallowed. A liquidation that could not be
   * processed therefore left a confident LIVE price on every screen, which is
   * the opposite of the guarantee this path exists to make: what you see
   * crossing a line is a tick the engine acted on. Now risk runs first, and a
   * failure publishes a control frame instead of a price. */
  let risked = true;
  let riskErr = null;
  let riskMaterial = false;
  let riskExpired = false;
  let committedRisk = false;
  let committedAdmission = null;
  try {
    roundCandidate = candidateRoundMark(sym, candidateEntry, now, _evAccepted + 1);
    const r = tickEval(sym, {
      at: now, candidateEntry, committedEntry: prevEntry,
      committedReady, committedConfirming: confirmingBefore,
      riskProbeEntry, riskDeadlineMono, roundCandidate,
    });
    if (r && r.ok === false) {
      risked = false; riskErr = r.error; riskExpired = r.expired === true;
    }
    else {
      riskMaterial = !!(r && r.material);
      committedRisk = r?.committed === true;
      committedAdmission = r?.admission || null;
    }
  } catch (e) { risked = false; riskErr = e; }
  if (!risked && !riskExpired) {
    _log(`tickEval FAILED for ${sym}: ${riskErr && riskErr.message}`);
    tgOps('tickeval:' + sym, `risk evaluation failed on ${sym}: ${riskErr && riskErr.message}`);
    riskFailure(sym, riskErr);
    /* Put back everything the forward path wrote. A tick whose risk pass did
       not complete is not a price this engine will stand behind, so it must
       not be readable by snapshots, readiness, the order path or settlement,
       and it must not anchor the next tick's jump clamp or make the engine
       look live. */
    /* The restored entry is the LAST COMMITTED price, and it is still inside
       its own receipt window, so readiness would happily keep quoting it while
       the source has already moved on. A symbol whose newest observation could
       not complete a risk pass is not executable until a later one does. */
    _riskBlocked.add(sym);
    if (!_riskBlockedSince.has(sym)) _riskBlockedSince.set(sym, now);
    restoreCandidateGuards();
    if (prevEntry) live.map.set(sym, prevEntry); else live.map.delete(sym);
    if (prevIdx !== undefined) _lastIdx.set(sym, prevIdx); else _lastIdx.delete(sym);
    live.lastMsgMs = prevLastMsg;
    invalidateRoundMark(sym, 'risk_failed', now);
    /* The observation still HAPPENED and the public side may already have
       filled on it in its own transaction, so it is kept as EVIDENCE: a
       durable audit row carrying the verdict. It is deliberately kept out of
       the chart's history, because that history is served back as the price
       record and a refused tick reappearing there would contradict, on a
       reload, exactly what every live client was told to ignore. */
    persistTick(sym, px, now, nComps, spreadBps, srcKey, srcAt, false);
    /* One frame per symbol per second. The failures this exists for are
       sustained, not one-shot: a database error makes every tick on every
       market fail, and an ungated frame would be a hundred a second into the
       fleet at the exact moment it is least able to take it. */
    notifyPriceabilityLost(sym, 'risk_failed', now, true);
    return;
  }
  /* DID THIS OBSERVATION SURVIVE ITS OWN RISK PASS?
   *
   * The age gate ran at ingress and nowhere else, so a quote accepted at 550ms
   * under a 600ms budget, followed by 100ms of slow risk work, was published
   * as an ordinary price frame with a ttl of zero. Every screen took it as
   * current and, until the client learned to read the ttl, offered it for
   * trading. An event that outlived its source budget while we were thinking
   * about it is not a price: the risk event transaction now rolls its public
   * and scored mutations back together, then this branch suppresses it and
   * restores the last committed mark. */
  /* MEASURED IN THE EVENT'S OWN TIME BASE, PLUS A MONOTONIC READ OF HOW LONG
     RISK TOOK. The first cut of this compared the observation against the wall
     clock, which is the same thing in production and completely different
     under a simulated clock: it refused a fixture's 60s-old event as "expired
     during risk" when the engine's own age model, the one the ingress gate
     uses, said that observation was zero milliseconds old. The question here
     is not how old the timestamp looks, it is whether THIS pass spent the
     remaining budget. */
  const ageAtCommit = Math.max(0, now - (Number(srcAt) > 0 ? Number(srcAt) : now)) + Math.max(0, monoNow() - monoEntry);
  const crossedAfterCommit = committedRisk && ageAtCommit >= expiryBudget;
  if (riskExpired || (!committedRisk && ageAtCommit >= expiryBudget)) {
    _expiredInRisk++;
    _log(`index ${sym}: observation expired DURING risk (${Math.round(ageAtCommit)}ms of a ${expiryBudget}ms budget), not published`);
    /* The browser is told this symbol is stale, and the SERVER has to agree.
       Without this the restored previous mark stayed executable to the order
       path for the rest of its own deadline, so a direct API client could
       trade on a price every screen had just been told to ignore. */
    _riskBlocked.add(sym);
    if (!_riskBlockedSince.has(sym)) _riskBlockedSince.set(sym, now);
    restoreCandidateGuards();
    if (prevEntry) live.map.set(sym, prevEntry); else live.map.delete(sym);
    if (prevIdx !== undefined) _lastIdx.set(sym, prevIdx); else _lastIdx.delete(sym);
    live.lastMsgMs = prevLastMsg;
    // The candidate expired and its whole event rolled back. This is not a
    // hard failure of the opted-in round's previously committed lineage.
    persistTick(sym, px, now, nComps, spreadBps, srcKey, srcAt, false);
    notifyPriceabilityLost(sym, 'stale', now, true, 'risk_expired');
    return;
  }
  /* Committed. Only now does the tick reach the strict mark history the result
     is settled from, the chart's history, the durable audit and the relay. */
  _riskBlocked.delete(sym);
  _riskBlockedSince.delete(sym);
  disputeClose(sym, now);
  /* A new committed mark starts a new fail-closed episode. Without clearing
     these gates, failure -> recovery -> failure inside one second suppressed
     the second control and left the browser trading while the engine refused. */
  _riskFrameAt.delete(sym);
  _staleFrameAt.delete(sym);
  const acceptedSeq = ++_evAccepted;
  /* Keep the accepted-event identity with the exact mark. A cadence-thinned
     tick has no delivery q, but a later score fence still needs an ordering
     identity so a browser can distinguish it from a queued pre-failure fence. */
  const committedEntry = live.map.get(sym);
  if (committedEntry) {
    committedEntry.acceptedSeq = acceptedSeq;
    committedEntry.acceptedBoot = ENGINE_BOOT_ID;
  }
  /* ONE canonical verdict for this tick.
   *
   * This used to hand recordMark the FRESH component count while the live gate
   * counted fresh-plus-corroborating, so a market admitted for trading by a
   * quiet second venue stored a history row saying "one source" and the
   * checkpoint that decides the result then refused the very instant the
   * engine had accepted. Now the verdict is computed once, here, and
   * everything downstream reads it. It sits AFTER the risk gate because this
   * is what settlement prices from: a refused tick that still wrote a strict
   * mark would decide a result from an observation every screen was told to
   * ignore. */
  recordMark(sym, px, now, nComps, spreadBps, srcKey, srcAt,
    crossedAfterCommit ? committedAdmission : null);
  armSourceExpiry(sym);
  /* Did this tick MOVE anybody? A cross liquidation triggered by one market
     closes positions on the others, so the flag is collected per symbol as the
     fills happen rather than counted globally around this call. */
  const material = consumeMaterialFill(sym) || riskMaterial;
  pythHistPush(sym, now, px, material);
  const mono = monoNow();
  /* srcKey, not the observation's TIMESTAMP. Passing the timestamp into the
     source slot labelled every Lazer frame with a number and, because the TTL
     is derived from the source's own budget, handed it the generic four second
     budget instead of Lazer's 600ms. Caught by the round two review. */
  // SQLite commit cannot be undone after it returned. If only that final
  // interval spent the remaining deadline, keep the admitted event's exact
  // history/aq but never announce it as a currently fresh public price.
  const pub = crossedAfterCommit ? { published: false, seq: null }
    : pythSseBroadcast(sym, px, now, srcKey, material, srcAt);
  if (crossedAfterCommit) notifyPriceabilityLost(sym, 'stale', Date.now(), true, 'source_expired');
  /* A cadence-thinned observation may be the final tick after the previous
     board flush. Schedule on every accepted tracked mark; the global
     coalescer then fences that exact newest value before scoring it. */
  scheduleCompBoard(sym, !!roundCandidate);
  /* EVERY PUBLISHED FRAME GETS ITS DURABLE ROW.
   *
   * Persistence ran on its own 120ms throttle while the majors publish every
   * 40ms, so two frames in three were shown to the fleet and to the chart with
   * nothing in the audit trail to replay them against. The relay's decision is
   * now the one that matters: what we published, we recorded, under the same
   * delivery id the frame carried. The slower throttle survives only for the
   * thin markets nobody is subscribed to, so a quiet symbol still leaves a
   * trail. */
  if (crossedAfterCommit || pub.published || material || mono - (_persistLast.get(sym) || -1e9) >= SSE_MIN_GAP_MS) {
    _persistLast.set(sym, mono);
    persistTick(sym, px, now, nComps, spreadBps, srcKey, srcAt, true, pub.seq);
  }
  /* Recovery is itself a phase event. Do not wait for the five-second sweep:
     the first accepted mark that restores the complete roster/segment can
     resume the active clock and freeze every client countdown immediately. */
  try { clearPauseIfPriceable(); } catch { /* the durable pause remains */ }
}
/* A live round must not keep trading on prices whose risk pass did not
   complete. One failure is logged and surfaced; a run of them on the same
   market inside ten seconds pauses the round, which is the same fail-closed
   posture the unpriceable-market path already takes. */
/* Symbols whose newest observation failed its risk pass. Cleared by the next
   observation that completes one. */
const _riskBlocked = new Set();
const _riskBlockedSince = new Map();
/* A prior risk failure blocks its old committed mark. The next candidate may
   prove recovery only while its scored-risk savepoint is active, and only for
   the exact live-map object installed by that ingress call. Boundary/segment
   logic runs before this token exists, so a later-refused candidate can never
   settle a checkpoint or clear a gate outage. */
let _riskProbe = null;                 // { sym, entry }, synchronous only
let _committedReadinessProbe = null;   // { sym, entry, ready }, clock prelude only
const _riskFails = new Map();
const _riskFrameAt = new Map();       // symbol -> when we last told the fleet
const _staleFrameAt = new Map();      // symbol -> when we last said "it expired on us"
/* A true -> false priceability transition has no accepted price event of its
   own, but it changes both things the live relay certifies: whether a ticket
   may execute and whether every contestant can be ranked. Put those changes
   on the relay immediately for a held competition symbol. Risk failures also
   notify public paper screens, preserving the broader fail-closed contract. */
function notifyPriceabilityLost(symbol, kind, at = Date.now(), alwaysControl = false, diagnosticReason = null) {
  const sym = baseOf(symbol);
  const reason = diagnosticReason || marketDiagnosticFailure(sym, at, kind);
  observeMarketDiagnostic(sym, at, false, false, { reason });
  noteMarketDiagnosticCounter(sym, 'lossNotifications');
  /* Invalidity is an INTERVAL in the same immutable ledger used by the jump
     clamp. Strict boundary pricing must remember a halt/risk/source failure
     even after it recovers; otherwise a delayed boundary can reach backwards
     through the outage and settle on the last pre-failure mark. */
  try { disputeOpen(sym, Number(at) || Date.now(), null, null); } catch { /* live gate still fails closed */ }
  let tracked = _compRankSymbols.has(sym);
  if (!tracked) {
    try { tracked = roundMarkets().has(sym); } catch { /* no live roster */ }
  }
  if (tracked) scheduleCompBoard(sym, true);
  if (!tracked && !alwaysControl) return false;
  const frameTimes = kind === 'risk_failed' ? _riskFrameAt : _staleFrameAt;
  if (at - (frameTimes.get(sym) || 0) < 1000) return tracked;
  frameTimes.set(sym, at);
  pythControlBroadcast({ type: kind, s: sym, t: at });
  return true;
}
let _expiredInRisk = 0;
setInterval(() => {
  if (!_expiredInRisk) return;
  _log(`index: ${_expiredInRisk} observation(s) expired during risk in the last minute`);
  if (_expiredInRisk >= 20) tgOps('expiredrisk', `${_expiredInRisk} index observations expired during risk evaluation in one minute: risk work is running long`);
  _expiredInRisk = 0;
}, 60_000).unref();
function riskFailure(sym, err) {
  const t = Date.now();
  const arr = (_riskFails.get(sym) || []).filter((x) => t - x < 10_000);
  arr.push(t);
  _riskFails.set(sym, arr);
  if (arr.length < 3) return;
  if (!(comp.currentRound() || {}).id) return;
  _riskFails.set(sym, []);
  try { pauseRound(sym, 'risk', `risk evaluation failed ${arr.length}x in 10s: ${err && err.message}`); } catch (e) { _log(`pause after risk failure failed: ${e.message}`); }
}
const micro = (b, a, bq, aq) => (bq > 0 && aq > 0 ? (b * aq + a * bq) / (aq + bq) : (b + a) / 2);
/* Pyth Lazer: the published primary.
 *
 * Chosen over Binance for the tail, not the headline. Measured from this box:
 * Binance ticks arrive 115-124ms old at p50 but its GAPS run to 3.8s, so
 * worst-case staleness is about four seconds. Lazer arrives 131ms old at p50
 * and 142ms at p99, on an exact 50ms metronome, so worst case is about 190ms.
 * That 20x cut in worst case is the number that governs 1000x, because a long
 * hole is where a real move hides and then gaps the mark on recovery.
 *
 * Only feeds this key is entitled to are listed. Lazer rejects an ENTIRE
 * subscription if one feed is unentitled, so a speculative "subscribe to
 * everything" would silently leave us with no primary at all. */
const LAZER_FEEDS = Object.fromEntries(Object.entries(LAZER_ALL).map(([k, v]) => [k, v[0]]));
/* Which symbols arrive on a 50ms metronome or faster. Everything downstream
   that used to ask "is this a 1000x market?" asks this instead: the caps were
   cut to 500 on 2026-08-31 and those tests have been silently false ever
   since. A 200ms feed is not one of these and keeps the slower relay gate. */
/* ONLY the true real_time feeds get the 40ms relay gate. Putting the 50ms
   tier on it too pushed a browser to 720 frames a second and 110KB/s for a
   book of 78 markets, when the only market anyone is scalping is the one on
   their screen. The rest still arrive 8 times a second, which is finer than a
   50ms chart grid can show and far finer than a mark or a PnL needs. */
/* Anything the provider publishes faster than the slow tier: the real time
   feeds and the 50ms ones. A 200ms feed gains nothing from a 40ms gate. */
const FAST_RELAY = new Set(Object.keys(LAZER_ALL).filter((k) => LAZER_ALL[k][1] !== 'f200'));
/* Clocks disagree by a little even when both are healthy; only a materially
   future timestamp is evidence of a broken source. */
const SRC_FUTURE_TOLERANCE_MS = 2_000;
const LAZER_TOKEN = process.env.PAPER_LAZER_TOKEN || '';
/* One endpoint was a single point of failure on the PRIMARY source: a
   provider deployment restart forced an avoidable failover to Binance, with
   the mark gap that comes with it. PAPER_LAZER_WS accepts a comma-separated
   list and every entry is connected at once; duplicate publishes are already
   rejected at ingress by envelope timestamp, so a second endpoint costs a
   socket and buys continuity. Defaults to the single documented endpoint,
   because inventing hostnames would be worse than not having them. */
/* ALL THREE DOCUMENTED ENDPOINTS, BY DEFAULT AND BY CONTRACT.
 *
 * Pyth publishes three and requires connections to all of them, because a
 * deployment takes one down at a time. The compiled default was a single
 * undocumented host, so a missing or fat-fingered environment variable
 * silently reduced the competition's primary feed to one socket while every
 * health check stayed green. The list is deduplicated, its size is a readiness
 * input, and falling under the minimum is an alert rather than a surprise. */
const LAZER_WS_DEFAULT = [
  'wss://pyth-lazer-0.dourolabs.app/v1/stream',
  'wss://pyth-lazer-1.dourolabs.app/v1/stream',
  'wss://pyth-lazer-2.dourolabs.app/v1/stream',
].join(',');
const LAZER_WS_URLS = [...new Set(String(process.env.PAPER_LAZER_WS || LAZER_WS_DEFAULT)
  .split(',').map((x) => x.trim()).filter(Boolean))];
const LAZER_MIN_ENDPOINTS = Number(process.env.PAPER_LAZER_MIN_ENDPOINTS || 2);
/* url -> when that endpoint last delivered a feed message. An endpoint that is
   configured, connected and silent is not a healthy endpoint. */
const _lazerLive = new Map();
function lazerPool(now = Date.now()) {
  const healthy = [...LAZER_WS_URLS].filter((u) => now - (_lazerLive.get(u) || 0) < 5_000);
  return { configured: LAZER_WS_URLS.length, healthy: healthy.length, minimum: LAZER_MIN_ENDPOINTS, urls: healthy };
}
const LAZER_CHANNEL = process.env.PAPER_LAZER_CHANNEL || 'real_time';
/* Confidence is a SOURCE-HEALTH signal, not a leverage dial.
 *
 * Using it to modulate the cap would recreate the moving-leverage problem the
 * show cannot have: two traders clicking seconds apart getting different
 * leverage. Instead a wildly wide band means this tick cannot be trusted, so
 * the symbol is treated as unhealthy and the chain fails over to Binance while
 * the cap stays where it was published. Measured normal is 1.00-2.64bps across
 * the entitled feeds, so 10bps is roughly 4x the worst-observed. */
const LAZER_MAX_CONF_BPS = Number(process.env.PAPER_LAZER_MAX_CONF_BPS || 10);
/* THE BAND IS PER MARKET, because 10bps means different things in different
 * order books.
 *
 * A flat band was measured against the majors, whose publishers agree to
 * within 1-3bps, and it is right for them. For a thin market it is the NORMAL
 * level: measured over three hours, CHIP crossed this line 11,061 times, LIT
 * 5,486, SKR 2,550, with a median rejected confidence of 10.8-12.6bps. Each
 * crossing drops the Lazer leg, the chain fails over to a venue, the price
 * steps between two sources, and the jump clamp holds ticks: the "index clamp
 * storm" alerts and the terminal flashing "Live prices stale, trading paused"
 * on a market whose feed was never actually broken. A market is judged against
 * its own book, exactly as its jump clamp and its cadence budget already are,
 * with a floor so the majors keep the band they were tuned for and a ceiling
 * so a genuinely disagreeing feed is still refused (ANSEM's p99 is 70bps and
 * it should be, and is, rejected there). */
const LAZER_CONF_MULT = Number(process.env.PAPER_LAZER_CONF_MULT || 1.5);
const LAZER_CONF_CEIL = Number(process.env.PAPER_LAZER_CONF_CEIL || 40);
const LAZER_CONF_MIN_SAMPLES = 128;
const LAZER_CONF_READMIT = 0.8;     // fail away fast, fail back slow
const _confRing = new Map();        // sym -> recent confidence, bps
const _confBand = new Map();        // sym -> { bps, samples, at }
function confNote(sym, bps, now) {
  if (!(bps >= 0)) return;
  let ring = _confRing.get(sym);
  if (!ring) { ring = []; _confRing.set(sym, ring); }
  ring.push(bps);
  if (ring.length > 512) ring.splice(0, ring.length - 512);
  if (ring.length < LAZER_CONF_MIN_SAMPLES) return;
  const c = _confBand.get(sym);
  if (c && now - c.at < 60_000) return;
  /* THE TAIL, not the centre, for the same reason the jump clamp uses one: a
     band set from the median still sits inside a market's ordinary spread of
     confidences, so the market keeps crossing it. Measured after the first cut
     of this fix, a median-based band halved the flapping and no more, because
     CHIP and LIT simply live above 2x their own median often enough. The p95
     with a small multiple sits above the ordinary spread and below a genuine
     disagreement, and the ceiling still refuses the real outliers. */
  const sorted = [...ring].sort((x, y) => x - y);
  const p95 = sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * 0.95))];
  const bandBps = Math.min(LAZER_CONF_CEIL, Math.max(LAZER_MAX_CONF_BPS, p95 * LAZER_CONF_MULT));
  _confBand.set(sym, { bps: bandBps, samples: ring.length, at: now });
}
const confBandFor = (sym) => (_confBand.get(sym) || {}).bps || LAZER_MAX_CONF_BPS;
const _lazerConf = new Map();       // sym -> last confidence in bps
/* Order the OBSERVATION before confidence learning or hysteresis, including
   observations whose confidence is subsequently refused. Redundant sockets
   must not train/readmit from a copy that cannot become a new component tick.
   Tolerated future timestamps retain their FIRST effective time: re-clamping
   a copy against its later callback would manufacture a new observation. */
function createLazerObservationGate(futureLimit = 4096) {
  if (!Number.isSafeInteger(futureLimit) || futureLimit < 1 || futureLimit > 4096) throw new Error('invalid future identity limit');
  const bySymbol = new Map();
  const scalar = (v) => {
    if (typeof v !== 'number' && !(typeof v === 'string' && v.trim() !== '')) return NaN;
    const n = Number(v); return Number.isFinite(n) ? n : NaN;
  };
  const same = (a, b) => a.every((v, i) => Object.is(v, b[i]));
  return {
    observe(sym, feed, envelopeAt, now, budgetMs, futureToleranceMs) {
      const feedUs = scalar(feed.feedUpdateTimestamp);
      if (feed.feedUpdateTimestamp != null && !(feedUs > 0)) return { status: 'missing_time' };
      const feedAt = feedUs > 0 ? feedUs / 1000 : null;
      const envelope = Number.isFinite(envelopeAt) && envelopeAt > 0 ? envelopeAt : null;
      const rawAt = feedAt && envelope ? Math.min(feedAt, envelope) : (feedAt || envelope);
      if (!(rawAt > 0) || !Number.isFinite(rawAt)) return { status: 'missing_time' };
      if (rawAt - now > futureToleranceMs) return { status: 'future_time' };
      if (now - rawAt > budgetMs) return { status: 'stale_time', rawAt };
      let state = bySymbol.get(sym);
      if (!state) { state = { at: -Infinity, rawAt: null, values: null,
        future: new Map(), futureOrder: [], futureHead: 0 }; bySymbol.set(sym, state); }
      if (rawAt < state.at) return { status: 'older' };
      const values = [scalar(feed.price), scalar(feed.exponent), scalar(feed.confidence)];
      const saved = state.future.get(rawAt);
      const at = saved ? saved.at : Math.min(rawAt, now);
      if (!saved && rawAt > now) {
        // Cache even a non-new future identity, so a later socket copy cannot
        // acquire a later effective time. Never evict a still-admissible one.
        if (state.future.size >= futureLimit) return { status: 'capacity' };
        state.future.set(rawAt, { at, values });
        state.futureOrder.push(rawAt);
      }
      if (at < state.at) return { status: 'older' };
      if (at === state.at) {
        return { status: rawAt === state.rawAt && !same(values, state.values) ? 'conflict' : 'duplicate' };
      }
      state.at = at; state.rawAt = rawAt; state.values = values;
      // This watermark never rewinds. Once it is strictly past a raw time,
      // every future min(rawAt, callback) is old, even after a backward clock
      // step. Wall-time expiry alone would not provide that guarantee.
      // Amortized queue cleanup, not a full4096-entry scan per fast tick. Raw
      // times may arrive out of order: retaining an eligible entry behind an
      // ineligible head is conservative; none is forgotten before it is old.
      while (state.futureHead < state.futureOrder.length
          && state.futureOrder[state.futureHead] < state.at) {
        state.future.delete(state.futureOrder[state.futureHead++]);
      }
      if (state.futureHead >= Math.min(1024, futureLimit)
          && state.futureHead * 2 >= state.futureOrder.length) {
        state.futureOrder = state.futureOrder.slice(state.futureHead); state.futureHead = 0;
      }
      return { status: 'new', at, rawAt, feedAt, priceRaw: values[0], exponent: values[1], confidenceRaw: values[2] };
    },
    snapshot(sym) {
      const s = bySymbol.get(sym);
      return { at: Number.isFinite(s?.at) ? s.at : null, rawAt: s?.rawAt ?? null,
        futureCount: s?.future.size || 0, futureQueueSize: s?.futureOrder.length || 0,
        futureLimit, symbols: bySymbol.size };
    },
  };
}
const _lazerObservations = createLazerObservationGate();
const _lazerRejects = new Map();    // sym -> why we are ignoring it

function processLazerObservation(sym, feed, envelopeAt, now) {
  const o = _lazerObservations.observe(sym, feed, envelopeAt, now,
    staleMsForSym(sym, 'lazer'), SRC_FUTURE_TOLERANCE_MS);
  if (o.status !== 'new') {
    if (['conflict', 'capacity'].includes(o.status)) {
      if (!_lazerRejects.has(sym)) {
        _lazerRejects.set(sym, now);
        _log(`lazer ${sym}: ${o.status === 'conflict' ? 'conflicting equal-time observation' : 'future observation identity capacity reached'}, not pricing off it`);
      }
    } else if (o.status === 'stale_time') {
      _srcReject(sym, 'lazer', 'stale-observation',
        `observation ${Math.round(now - o.rawAt)}ms old, past its ${staleMsForSym(sym, 'lazer')}ms budget`);
    } else if (['missing_time', 'future_time'].includes(o.status)) {
      _srcReject(sym, 'lazer', o.status, 'unusable observation time');
    } else if (o.status === 'older') {
      _srcReject(sym, 'lazer', 'carried-forward or duplicate publish');
    }
    return o.status;
  }
  // Explicit scalar presence matters: Number(null), Number('') and Number(false)
  // are zero, not evidence of a valid exponent or perfect confidence.
  const px = o.priceRaw * Math.pow(10, o.exponent);
  const confBps = (o.confidenceRaw / o.priceRaw) * 1e4;
  if (!(o.priceRaw > 0) || !Number.isFinite(o.exponent) || !(px > 0) || !Number.isFinite(px)
      || !(o.confidenceRaw >= 0) || !Number.isFinite(confBps)) {
    if (!_lazerRejects.has(sym)) {
      _lazerRejects.set(sym, now);
      _log(`lazer ${sym}: missing or malformed price, exponent or confidence, not pricing off it`);
      if (!Number.isFinite(o.exponent)) tgOps('lazerexp', `lazer messages arriving without an exponent (${sym}); prices are not being taken from them`);
    }
    return 'malformed';
  }
  _lazerConf.set(sym, confBps);
  confNote(sym, confBps, now);
  const band = confBandFor(sym);
  // Preserve the existing hysteresis: reject at the learned band, readmit
  // only at80% of it, and only on a genuinely new usable observation.
  const limit = _lazerRejects.has(sym) ? band * LAZER_CONF_READMIT : band;
  if (confBps > limit) {
    if (!_lazerRejects.has(sym)) {
      _lazerRejects.set(sym, now);
      _log(`lazer ${sym}: confidence ${confBps.toFixed(2)}bps over ${band.toFixed(1)}bps, not pricing off it`);
    }
    return 'confidence';
  }
  if (_lazerRejects.has(sym)) { _lazerRejects.delete(sym); _log(`lazer ${sym}: confidence back inside band`); }
  if (o.feedAt && envelopeAt && envelopeAt - o.feedAt > staleMsFor('lazer')) {
    tgOps('lazerfeed:' + sym, `lazer ${sym} feed timestamp is ${Math.round(envelopeAt - o.feedAt)}ms behind its envelope`);
  }
  // The ordinary component path still independently orders, persists, clamps
  // and risk-checks the observation. No accepted sequence is issued here.
  compUpdate(sym, 'lazer', px, now, o.at);
  return 'eligible';
}

let _lazerStampLogged = false;
function startLazerStream() {
  if (!LAZER_TOKEN) { _log('lazer: no PAPER_LAZER_TOKEN, staying on binance'); return; }
  if (LAZER_WS_URLS.length < LAZER_MIN_ENDPOINTS) {
    tgOps('lazerpool:config', `lazer is configured with ${LAZER_WS_URLS.length} endpoint(s), below the required ${LAZER_MIN_ENDPOINTS}: the primary feed has no redundancy`);
    _log(`lazer: WARNING only ${LAZER_WS_URLS.length} endpoint(s) configured, minimum is ${LAZER_MIN_ENDPOINTS}`);
  }
  for (const url of LAZER_WS_URLS) startLazerEndpoint(url);
  /* Configured is not connected. A credential that works on one host, a
     firewall that eats another, two endpoints mid-deployment: all of them look
     identical from the configuration and completely different from here. */
  setInterval(() => {
    const pool = lazerPool();
    if (pool.healthy < pool.minimum) {
      tgOps('lazerpool:health', `lazer pool is down to ${pool.healthy} healthy endpoint(s) of ${pool.configured}, minimum ${pool.minimum}`);
      _log(`lazer pool degraded: ${pool.healthy}/${pool.configured} healthy`);
    }
  }, 30_000).unref();
}
function startLazerEndpoint(LAZER_WS_URL) {
  const WebSocket = require('ws');
  const bySym = Object.fromEntries(Object.entries(LAZER_FEEDS).map(([k, v]) => [v, k]));
  let backoff = 1_000;
  const connect = () => {
    const ws = new WebSocket(LAZER_WS_URL, { headers: { Authorization: 'Bearer ' + LAZER_TOKEN } });
    ws.on('open', () => {
      /* ONE SUBSCRIPTION PER CHANNEL TIER.
       *
       * A request that names a feed above the channel it serves is refused in
       * full, not per feed, so a single subscription for all seventy eight
       * would leave us with no primary at all. Three go out on the same
       * socket, each carrying only the feeds that accept its channel. */
      let sid = 0;
      for (const [tier, ch] of Object.entries(LAZER_TIER_CH)) {
        const ids = Object.entries(LAZER_ALL).filter(([, v]) => v[1] === tier).map(([, v]) => v[0]);
        if (!ids.length) continue;
        sid += 1;
        ws.send(JSON.stringify({
          type: 'subscribe', subscriptionId: sid,
          priceFeedIds: ids,
          /* Per-feed freshness, not just the envelope's. The batch carries one
             timestamp for the whole slot, so a single feed that stopped updating
             inside a still-ticking stream was indistinguishable from a healthy
             one. Verified against the live stream: the property is accepted and
             every feed carries it. */
          /* EXPONENT IS REQUESTED, NOT ASSUMED.
           *
           * Lazer only sends the properties you ask for, and this asked for
           * three. `f.exponent` was therefore always undefined and the code
           * defaulted to -8, which happens to be right for every crypto feed
           * and wrong by a factor of a thousand for equities, metals and the
           * commodity futures: AAPL arrived as 0.328 against a venue mark of
           * 328.04. Caught by the boot validation on the first deploy that
           * carried non-crypto feeds. */
          properties: ['price', 'confidence', 'exponent', 'feedUpdateTimestamp'],
          chains: [], channel: LAZER_CHANNEL === 'real_time' ? ch : LAZER_CHANNEL, parsed: true,
          deliveryFormat: 'json', jsonBinaryEncoding: 'hex',
        }));
      }
    });
    ws.on('message', (buf) => {
      let m; try { m = JSON.parse(buf.toString()); } catch { return; }
      if (m.type === 'subscriptionError' || m.type === 'error') {
        _log('lazer SUBSCRIPTION REFUSED: ' + String(m.error).slice(0, 300));
        tgOps('lazer:sub', 'Pyth Lazer refused the subscription: ' + String(m.error).slice(0, 200));
        try { ws.close(); } catch {}
        return;
      }
      if (m.type === 'subscribed') { backoff = 1_000; _log(`lazer connected (${LAZER_CHANNEL}) via ${LAZER_WS_URL}: ${Object.keys(LAZER_FEEDS).join(',')}`); return; }
      const p = m.parsed; if (!p || !p.priceFeeds) return;
      const now = Date.now();
      _lazerLive.set(LAZER_WS_URL, now);
      /* The envelope carries the publish time in microseconds. Using it means
         a batch delayed in a queue is judged on when the price was made, not
         on when we happened to read it. Logged once a boot so a provider
         format change is visible rather than silently reverting us to
         callback time. */
      const srcAt = Number(p.timestampUs) > 0 ? Number(p.timestampUs) / 1000 : null;
      if (!_lazerStampLogged) {
        _lazerStampLogged = true;
        _log(srcAt ? `lazer: envelope timestamp present, ingress ages measured from it (skew ${Math.round(now - srcAt)}ms)`
                   : `lazer: NO envelope timestamp in ${Object.keys(p).join(',')}; falling back to callback time`);
      }
      for (const f of p.priceFeeds) {
        const sym = bySym[f.priceFeedId];
        if (sym) processLazerObservation(sym, f, srcAt, now);
      }
    });
    const retry = () => {
      const wait = Math.min(backoff, 30_000);
      backoff = Math.min(backoff * 2, 30_000);
      setTimeout(connect, wait).unref?.();
    };
    ws.on('close', () => { _log('lazer stream closed, reconnecting'); retry(); });
    ws.on('error', (e) => { _log('lazer stream error: ' + e.message); try { ws.close(); } catch {} });
    /* WATCHDOG. A socket that opens and then goes quiet fires neither 'close'
       nor 'error', so nothing above ever reconnected it. That is what the
       13:43 restart produced: no 'subscribed' ack was ever logged, ticks
       trickled in at 800ms gaps for the next ten minutes, and the majors sat
       on Binance while a fresh connection from the same box was clean. Two
       rules, both of which end in a deliberate close so the existing retry
       path runs: no subscription ack within 10s of open, or no message of
       any kind for 4s while open (the stream ticks every 50ms). */
    let lastMsg = Date.now();
    let acked = false;
    ws.on('message', () => { lastMsg = Date.now(); });
    ws.on('message', (buf) => { try { if (JSON.parse(buf.toString()).type === 'subscribed') acked = true; } catch {} });
    const ackTimer = setTimeout(() => {
      if (!acked && ws.readyState === ws.OPEN) { _log('lazer: no subscription ack in 10s, reconnecting'); try { ws.terminate(); } catch {} }
    }, 10_000);
    ackTimer.unref?.();
    const silence = setInterval(() => {
      if (ws.readyState !== ws.OPEN) return;
      const quiet = Date.now() - lastMsg;
      if (quiet > 4_000) { _log(`lazer: silent for ${quiet}ms, reconnecting`); try { ws.terminate(); } catch {} }
    }, 1_000);
    silence.unref?.();
    ws.once('close', () => { clearTimeout(ackTimer); clearInterval(silence); });
  };
  connect();
}

/* Terminate a socket that is OPEN but has gone quiet, so the ordinary retry
   path runs. Half-open sockets are the failure that looks healthiest. */
function watchSilence(ws, quietMs, label) {
  let last = Date.now();
  ws.on('message', () => { last = Date.now(); });
  const iv = setInterval(() => {
    if (ws.readyState !== ws.OPEN) return;
    const quiet = Date.now() - last;
    if (quiet > quietMs) { _log(`${label}: silent for ${quiet}ms, reconnecting`); try { ws.terminate(); } catch {} }
  }, Math.max(1_000, Math.floor(quietMs / 4)));
  iv.unref?.();
  ws.once('close', () => clearInterval(iv));
}
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
        /* Binance stamps every event; miniTicker carries E (event time). */
        const evAt = Number(d.data.E) > 0 ? Number(d.data.E) : null;
        if (Number.isFinite(px) && px > 0) compUpdate(hit[0], hit[1], px, Date.now(), evAt);
      } catch {}
    });
    const retry = () => { try { ws.terminate(); } catch {} setTimeout(connect, backoff); backoff = Math.min(backoff * 2, 30_000); };
    ws.on('close', retry);                            // includes Binance's routine 24h disconnect
    ws.on('error', (e) => { _log('binance index ws error: ' + e.message); });
    /* Silence watchdog, the same failure class Lazer already handles: a socket
       that stays OPEN and stops delivering fires neither close nor error, so
       nothing reconnected it and the component simply aged out. Binance sends
       at least one message a second across this many streams. */
    watchSilence(ws, 15_000, 'binance index');
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
        /* Coinbase's `time` is the TRADE that triggered the message, not the
           quote we actually read: on a thin market the best bid/ask is current
           while the last trade is minutes old. Judging the quote's age by it
           rejected healthy cross-check data (MET arrived "128s old" within a
           minute of shipping it). Arrival time is the honest stamp here. */
        if (sym && Number.isFinite(mid) && mid > 0) compUpdate(sym, 'usd', mid, Date.now());
      } catch {}
    });
    const retry = () => { try { ws.terminate(); } catch {} setTimeout(connect, backoff); backoff = Math.min(backoff * 2, 30_000); };
    ws.on('close', retry);
    ws.on('error', (e) => { _log('coinbase component ws error: ' + e.message); });
    watchSilence(ws, 30_000, 'coinbase component');   // ticker is sparser than binance
  };
  connect();
}
// boot backfill: 45min of 1m closes so the tick-history window is full from
// the first pageview after a restart (replaces the dying Pyth Benchmarks API)
// Boot-time component validation: 90s after start, every indexed symbol's
// composite must sit within 2% of Phoenix's venue mark. A venue delisting a
// ticker and reusing it for another asset would otherwise silently poison a
// component (found once by hand during the expansion research; automated).
/* THE CANDLE SOURCE GETS THE SAME CHECK AS THE INDEX.
 *
 * The index has been validated against the venue mark on every boot since
 * August, and it is what caught the equity exponent bug. The kline source had
 * no such check, so a ticker that means one asset here and another on Binance
 * would have drawn a completely unrelated chart under our symbol, with the
 * live price on top of it. LIT was exactly that: Litentry at $0.74 under our
 * $4.33 market. A pair that disagrees with our own mark by more than 2% is
 * dropped rather than drawn. */
setTimeout(() => {
  const https2 = require('https');
  for (const [sym, pair] of Object.entries(BINANCE_STREAMS)) {
    const m = live.map.get(sym);
    const venue = m && Number(m.markPrice);
    if (!(venue > 0)) continue;
    https2.get(`https://api.binance.com/api/v3/ticker/price?symbol=${pair.toUpperCase()}`, (res) => {
      let b = '';
      res.on('data', (c) => b += c);
      res.on('end', () => {
        try {
          const p = Number(JSON.parse(b).price);
          if (!(p > 0)) return;
          const off = Math.abs(p - venue) / venue;
          if (off > 0.02) {
            delete BINANCE_STREAMS[sym];
            _log(`KLINE SOURCE REJECTED ${sym}: ${pair} is ${p}, our mark is ${venue} (${(off * 100).toFixed(1)}% apart) — different asset, candles will come from the venue`);
            tgOps('kline:' + sym, `${sym} candle source dropped: Binance ${pair} is ${p} against our mark of ${venue}, a different asset behind the same ticker`);
          }
        } catch { /* a check that cannot run is not a failure */ }
      });
    }).on('error', () => {});
  }
}, 45_000).unref();
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
    recordMark(sym, v, now);
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
  startTickPersistence();
  purgeInviteCapabilities('startup');
  // Hand the competition clock its controls, then pick up any round the
  // engine was running when it restarted: a mid-show reboot must resume the
  // same schedule rather than stall the wall on a phase that never ends.
  comp.wire({
    defaultPricePolicy: () => ROUND_PRICE_POLICY,
    defaultBoostCapacityPolicy: () => 'current-equity-v1',
    defaultBackupExecutionPolicy: () => 'latest-available-500-v1',
    initializeRoundMarks,
    openAlias, closeAlias, scoreUser, scoreProofFor, hotValueOf,
    prepareSeat, seatState, markSetFor,
    indexedSymbol: (sym) => STAGE_INDEXED.has(baseOf(sym)),
    boostLeverage: () => HEAT_MAX_LEV,
    closeRoundPauses,
    segmentResidue: (alias) => ({
      positions: stmt.posBySymbol.all(alias).length,
      orders: stmt.ordOpenBySymbol.all(alias).length,
    }),
    markBefore,
    /* settleAtPrior asks these two questions before it is allowed to deviate:
       is the instant genuinely dead, and is there anything to settle. */
    markAtStrict: (sym, ts, round = comp.currentRound()) => markAt(baseOf(sym), ts, { strict: true, round }),
    aliasPositions: (alias) => stmt.posBySymbol.all(alias).length,
    ensureAccount: (u) => _ensureAccountFn(u),
    /* A practice seat needs the same two rows a person's seat needs: an
       identity stub (createRound refuses a seat with no account, and that
       check must apply to bots too) and a paper account. Nothing else about
       a bot is special; startRound's prepareSeat stamps its bankroll exactly
       as it does for a person. */
    ensureBot: (uid, name) => {
      if (!comp.isBotId(uid)) throw new Error('not a practice seat id: ' + uid);
      db.prepare('INSERT OR IGNORE INTO users (id) VALUES (?)').run(uid);
      ensureAccount(uid);
      return { userId: uid, name };
    },
    /* The driver starts and stops itself off round lifecycle events, so a
       restart mid-practice-round picks the seats back up with nothing having
       to remember they were running. */
    onPhase: (event) => {
      /* Phase transitions and pause/resume are pushed immediately. Do not let
         the short REST memo preserve the preceding phase for another poll. */
      try { _stateCache = { at: 0, body: null }; } catch { /* module startup */ }
      if (event === 'round:start' || event === 'round:aborted' || event === 'round:done' || event === 'round:end') {
        purgeInviteCapabilities(event);
        /* The next snapshot owns the exposure set. Clearing here prevents a
           finished round's symbols from keeping the scorer hot. */
        _compRankSymbols.clear();
        _compDirtySymbols.clear();
        _compPendingCauses.length = 0;
        _compCauseOverflow = false;
        if (_compExpiryTimer) { clearTimeout(_compExpiryTimer); _compExpiryTimer = null; }
      }
      /* Draw/Hot/Boost boundaries can change score semantics even when no
         market moves. Publish the new authoritative board after the clock's
         transaction has committed. */
      scheduleCompBoard(null, true);
      armCompetitionClockExpiry();
      try { bots.sync(); } catch (e) { _log('practice seats: ' + e.message); }
    },
    // a market is competition-ready only if it is indexed AND its composite
    // currently satisfies the multi-source rule
    marketReady: (sym, round = comp.currentRound()) => STAGE_INDEXED.has(baseOf(sym))
      && roundPriceReady(baseOf(sym), 0, Date.now(), round),
    /* A delayed boundary must decide fallback/readiness from the accepted
       mark history at its own active-time instant, never from a later quote. */
    marketReadyAt: (sym, at, kind, requiredLeverage, round = comp.currentRound()) => STAGE_INDEXED.has(baseOf(sym))
      && Number(markAt(baseOf(sym), Number(at), {
        strict: true, round,
        forLeverage: kind === 'BOOST'
          ? (Number(requiredLeverage) || HEAT_MAX_LEV) : comp.COMP_BASE_LEV,
      })) > 0,
    marketAvailability: (sym, kind, at, requiredLeverage, round = comp.currentRound()) => roundPriceAvailability(baseOf(sym),
      kind === 'BOOST' ? (Number(requiredLeverage) || HEAT_MAX_LEV) : comp.COMP_BASE_LEV,
      Number(at) || Date.now(), round),
    historicalAvailabilityAt: (sym, kind, at, requiredLeverage, round = comp.currentRound()) => historicalAvailabilityAt(baseOf(sym),
      kind === 'BOOST' ? (Number(requiredLeverage) || HEAT_MAX_LEV) : comp.COMP_BASE_LEV,
      Number(at) || Date.now(), round),
    marketEvidenceAt: (sym, at, kind, requiredLeverage, round = comp.currentRound()) => marketEvidenceAt(
      baseOf(sym), Number(at), kind, requiredLeverage, round),
    ensureClockHealth: (at) => ensureCompetitionClockHealth(Number(at) || Date.now()),
    pauseForSegment: (sym, kind, why, since) => pauseRound(
      kind === 'BOOST' ? baseOf(sym) + '-BOOST' : baseOf(sym),
      `${kind} segment`, why, since),
    pauseForRestart: (why, since) => pauseRound(RESTART_PAUSE_SYMBOL,
      'engine restart', why, since),
    pauseForBoundary: (since, why) => pauseRound(BOUNDARY_PAUSE_SYMBOL,
      'competition boundary', why, since),
    marketReliability: (sym) => readyRatio(baseOf(sym)),
    /* Opening a Boost ticker asks whether the market can carry the leverage
       persisted on this round (500x in the current format), against quality at
       that instant, so a brief widening lowers the cap instead of cancelling
       the segment for everyone. */
    marketReadyForBoost: (sym, requiredLeverage, round = comp.currentRound()) => STAGE_INDEXED.has(baseOf(sym))
      && roundPriceReady(baseOf(sym), Number(requiredLeverage) || HEAT_MAX_LEV, Date.now(), round),
    boostLevCap: (sym, engineCap, round = comp.currentRound()) => boostLevCap(baseOf(sym), engineCap, Date.now(), round),
    aliasOpen: (a) => competitionAliasOpen(a),
    equityOf: (uid) => {
      const a = stmt.acctGet.get(uid);
      return a ? accountRisk(uid, a).equityTotal : Number.NaN;
    },
    log: (m) => _log(m),
  });
  /* The driver is a CLIENT: it holds a session and posts orders to the same
     endpoint a person uses, so every guard applies to it as written. All it
     is given here are the reads it needs to decide, and the mint. */
  bots.attach(comp);
  bots.wire({
    log: (m) => _log(m),
    priceAcknowledgmentFor,
    sessionFor: (uid) => auth.mintBotSession(uid, comp.isBotId),
    positionsOf: (uid) => stmt.posByUser.all(uid).map((p) => ({ symbol: p.symbol, side: p.side, size: p.size })),
    /* What a bot may trade: the round's own markets, plus whichever segment
       ticker is open right now, so practice seats exercise Hot and Boost
       rather than only the base board. */
    marketsFor: (r) => {
      const base = [...new Set([...JSON.parse(r.hot_candidates || '[]'), ...(r.hot_backup ? [r.hot_backup] : [])])];
      const segs = [];
      const phase = comp.phaseNow();
      if (Number(r.format_version) >= 2) {
        if (phase && phase.phase === 'hot' && phase.hotNumber) {
          const active = r[`hot${phase.hotNumber}_active_base`] || r[`hot${phase.hotNumber}_base`];
          return active && roundPriceReady(active, 0, Date.now(), r) ? [active] : [];
        }
        if (phase && phase.phase === 'boost') {
          const required = Number(r.boost_leverage) || HEAT_MAX_LEV;
          return comp.boostMarketsOf(r).map((b) => b + '-BOOST')
            .filter((x) => aliasOpen(x) && roundPriceReady(baseOf(x), required, Date.now(), r));
        }
        return base.filter((x) => roundPriceReady(x, 0, Date.now(), r));
      }
      const hot = r.active_hot_base || r.hot_base;
      if (hot && aliasOpen(hot + '-HOT')) segs.push(hot + '-HOT');
      for (const b of comp.boostMarketsOf(r)) if (aliasOpen(b + '-BOOST')) segs.push(b + '-BOOST');
      return [...base, ...segs].filter((x) => compPriceReady(baseOf(x)));
    },
  });
  try { bots.sync(); } catch { /* nothing armed yet is the normal case */ }

  try { /* Re-arm any scheduled start the process was holding when it died. */
  try { const n = comp.resumeScheduledStarts(); if (n) _log(`re-armed ${n} scheduled start(s)`); } catch (e) { _log('schedule resume failed: ' + e.message); }
  startPauseNotifications();
  comp.resume();
  /* A restart in the middle of a round may not cross a phase boundary during
     resume. Seed the compact board and its exposure set explicitly. */
  if ((comp.currentRound() || {}).id) scheduleCompBoard(null, true);
  } catch (e) { _log('competition resume failed: ' + e.message); }
  if (readRateOk) _readRateOk = readRateOk;
  refreshExchange().catch(() => {});
  try {
    /* The legacy Hermes path predates the ingress contract: it stamps its own
       callback time, publishes before risk and swallows failures, so selecting
       it by environment variable would quietly restore every defect the round
       one review found. It is not a supported mode for a competition engine
       any more; refuse to start rather than run a second, weaker pipeline. */
    if (INDEX_SOURCE === 'pyth') {
      _log('INDEX_SOURCE=pyth is retired: the legacy Hermes ingest does not carry source time, commit ordering or risk results');
      throw new Error('INDEX_SOURCE=pyth is no longer supported; use the default binance+lazer chain');
    }
    cadenceLoad();
    bootHeartbeat();
    startBinanceIndexStream(); startCoinbaseGuard(); backfillIndexHistory();
    _chainStarted = true;
    startSourceExpiry();
    /* Lazer runs alongside, not instead: it is the chain's primary, and the
       Binance streams stay connected as the failover leg. */
    restoreSources();
    startLazerStream();
    _log('index source: ' + INDEX_SOURCE);
  } catch (e) {
    /* Swallowing this left a process that answers 200 and cannot price a
       single market. It stays a catch, because a half-started chain still has
       to be shut down cleanly, but readiness now tells the truth and an
       unsupported configuration takes the process down rather than idling in
       the rotation pretending to be a trading engine. */
    _log('index stream failed to start: ' + e.message);
    tgOps('chain', `index source chain failed to start: ${e.message}`);
    if (/no longer supported/.test(e.message || '')) {
      _log('refusing to run without a supported index source');
      setTimeout(() => process.exit(1), 250).unref?.();
    }
  }
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
function send(res, code, obj) {
  /* Account, invite and live-show JSON must never be replayed from a browser or
     intermediary cache. This is intentionally the default for the paper API;
     callers that need archival data can store the explicit response. */
  res.writeHead(code, {
    'content-type': 'application/json',
    'cache-control': 'no-store, private',
    'pragma': 'no-cache',
    'x-content-type-options': 'nosniff',
  });
  res.end(JSON.stringify(obj));
}
function readBody(req) {
  return new Promise((resolve, reject) => {
    let b = '';
    req.on('data', (c) => { b += c; if (b.length > 4096) { req.destroy(); reject(new Error('big')); } });
    req.on('end', () => resolve(b)); req.on('error', reject);
  });
}
async function sessionUser(req) { return await auth.validateSession(auth.parseSessionCookie(req)); }

/* WHO AM I IN THIS ROUND. The public state names the players of a live round
   but nothing told a seated trader, before the bell, that they were in one:
   the terminal was the ordinary terminal until the clock started. This is
   the signed-in user's own seat in the round that is armed, counting down or
   running, with the roster the wall will show, so their screen can say
   "Round 2, seat 1, starts in 4:59" and mean it. Session-authed; no token. */
async function compMe(req, res) {
  const u = await sessionUser(req);
  if (!u) return send(res, 200, { ok: true, user: null, round: null, seat: null, roster: [] });
  const rows = comp.__test.db.prepare(
    "SELECT id, status, stage, solo, speed, start_at, started_at, ends_at, kind, advance, plan_json, boost_capacity_policy, backup_execution_policy FROM paper_rounds WHERE status IN ('running', 'armed') ORDER BY CASE status WHEN 'running' THEN 0 ELSE 1 END, created_at DESC").all();
  const isBot = (id) => (comp.isBotId ? comp.isBotId(Number(id)) : Number(id) >= 9_900_000);
  for (const r of rows) {
    const players = comp.playersOf(r.id);
    const mine = players.find((p) => Number(p.user_id) === Number(u.id));
    if (!mine) continue;
    return send(res, 200, {
      ok: true,
      user: { id: u.id },
      round: {
        id: r.id, status: r.status, stage: r.stage || null, solo: !!r.solo, speed: Number(r.speed) || 1,
        startAt: r.start_at || null, startedAt: r.started_at || null, endsAt: r.ends_at || null, kind: r.kind,
        advance: r.advance || null, plan: comp.planOf(r),
        boostCapacityPolicy: comp.boostCapacityPolicyOf(r),
        ...comp.backupExecutionPolicyFields(r),
      },
      seat: {
        seat: mine.seat, name: mine.display_name || null, avatar: mine.avatar_url || null,
        /* So the terminal can show, and change, whether this player is at the
           desk. It used to live only on the invite page. */
        ready: mine.ready_at != null,
      },
      readiness: comp.readinessOf(r.id),
      roster: players.map((p) => ({ seat: p.seat, name: p.display_name || null, avatar: p.avatar_url || null, claimed: p.claimed_at != null, bot: isBot(p.user_id) })),
    });
  }
  return send(res, 200, { ok: true, user: { id: u.id }, round: null, seat: null, roster: [] });
}
/* "I am at the desk", from the terminal the player is already signed into. */
async function compMeReady(req, res) {
  const u = await sessionUser(req);
  if (!u) return send(res, 401, { ok: false, error: 'not_signed_in' });
  let body; try { body = JSON.parse(await readBody(req) || '{}'); } catch { return send(res, 400, { ok: false, error: 'bad_json' }); }
  try {
    const r = comp.setReadyByUser(u.id, !!body.ready);
    return send(res, 200, { ok: true, ...r });
  } catch (e) {
    return send(res, 400, { ok: false, error: e.message });
  }
}
/* One barrier, every write path. Returns true when the request was refused.
   Previously only placeOrder knew about the bell, so closing a position or
   adjusting margin still worked after the result was frozen. */
function barred(res, userId, { allowWhilePaused = false, symbol = null, at = null } = {}) {
  const why = comp.writeBarrier(userId, Number.isFinite(at) ? at : Date.now());
  if (why) { send(res, 409, { ok: false, error: why }); return true; }
  /* A pause that freezes automatic risk but leaves discretionary trading open
     is worse than no pause: it hands the field downside protection while they
     keep the upside. If nobody can be liquidated, nobody may trade either.
     Cancellation is the one exception, because reducing your own exposure to
     nothing can never be the unfair half of this. */
  if (!allowWhilePaused && symbol) {
    /* This market alone is unpriceable. Same fail-closed posture as a pause,
       same reason (no mark means no risk answer, so no new exposure and no
       exit at a price the engine will not stand behind), scoped to the market
       instead of the field. */
    const f = marketFrozen(symbol);
    if (f && competitionOwned(userId)
        && !(pricingRoundFor(userId) && roundPriceReady(symbol))) {
      send(res, 409, { ok: false, error: 'market_frozen', symbol: baseOf(symbol), since: f.since, why: f.why });
      return true;
    }
  }
  if (!allowWhilePaused) {
    const p = roundPaused();
    if (p && competitionOwned(userId)) {
      const r = comp.currentRound();
      send(res, 409, Number(r && r.format_version) >= 2
        ? {
            ok: false, error: 'competition_paused',
            ...publicPause(p, r), definitive: true,
          }
        : {
            ok: false, error: 'competition_paused',
            ...publicPause(p, r), definitive: true,
          });
      return true;
    }
  }
  return false;
}
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
/* Is the competition price world actually up for this symbol?
 *
 * Two separate hazards, both fatal at 1000x where the liquidation distance is
 * about 5bps:
 *   - after a cold restart the composite index has not resumed, but Phoenix
 *     venue ticks arrive immediately. The venue mark sits ~10bps from the
 *     index, which is twice the liquidation distance, so repricing a boosted
 *     position into the wrong world liquidates it on basis alone.
 *   - a single surviving component can move 8-20bps on its own and stay well
 *     inside the 50bps jump clamp.
 * Competition pricing therefore requires a fresh composite from at least two
 * agreeing components. Where it is not available the answer is "no mark",
 * which freezes risk rather than guessing. */
const COMP_MIN_COMPONENTS = Number(process.env.PAPER_COMP_MIN_COMPONENTS || 2);
/* Maximum disagreement between competition sources, in bps.
 *
 * A COUNT of fresh sources is not consensus. At 1000x the liquidation distance
 * is about 5bps, so a single source wrong by 10-15bps — comfortably inside the
 * 20bps mean band and the 25bps divergence guard — moves the mean past the
 * liquidation line while every readiness check stays green. Competition
 * pricing therefore requires the sources to AGREE, at a tolerance well inside
 * the buffer it is protecting. */
const COMP_MAX_SPREAD_BPS = Number(process.env.PAPER_COMP_MAX_SPREAD_BPS || 3);
/* How old a CORROBORATING component may be, as distinct from how old the mark
 * may be.
 *
 * Measured against the live feed, requiring every component inside the 5s mark
 * window made BTC 99% ready but ETH 88%, XRP 71% and everything thinner than
 * about 4%, with unready runs up to 77s. Almost none of that was disagreement:
 * observed spreads were 0.2-2.9bps against a 3bps gate. It was silence. A thin
 * venue simply does not print every five seconds, and a quiet source is not a
 * wrong one.
 *
 * So liveness and corroboration are separated. The mark must still be fresh:
 * at least one component inside COMP_FRESH_MS, plus the existing index-age
 * check. Corroboration may come from an older print, because a second source
 * that still AGREES with the fresh one is evidence the fresh one is not lying,
 * which is the whole thing this gate defends against. If the market genuinely
 * moved during that window the spread check fails and we go unready, which is
 * the safe direction. */
const COMP_CORROBORATE_MS = Number(process.env.PAPER_COMP_CORROBORATE_MS || 20_000);
/* Sustained readiness, not readiness at one instant.
 *
 * Readiness at the moment an operator presses start says nothing about the
 * next thirty minutes. Measured live, thin markets sit between 37% and 80%
 * ready with unready runs up to 40s, while any of them can look ready for the
 * single sample a start check takes. A market that hosts a scoring window has
 * to be reliably priceable for the length of a round, so we keep a rolling
 * record and let the round-start invariant read a RATIO rather than a
 * snapshot. */
const READY_HIST_MS = Number(process.env.PAPER_READY_HIST_MS || 10 * 60_000);
const _readyHist = new Map();   // sym -> [{t, ok}]
/* How often readiness is sampled. Used both to pace sampling and to decide
   how much time a single sample may legitimately stand for. */
const SWEEP_MS = Number(process.env.PAPER_SWEEP_MS || 5000);

/* WHO IS BLOCKING THE LOOP.
 *
 * Lazer delivers ~20 ticks a second with a worst gap of 106ms, measured from
 * a separate process on this box, yet the engine logged "stale 415ms" on a
 * five second cadence and failed the majors over to Binance. Messages that
 * arrive on time and are read late look exactly like a stalled feed from
 * inside the process, so the feed was being blamed for the engine's own
 * pauses. This names the pause: the loop's delay histogram, reported when it
 * is bad, and the sweep's stages timed so the slow one is identifiable
 * rather than inferred. */
const { monitorEventLoopDelay, performance: perfNow } = require('perf_hooks');
const _loopDelay = monitorEventLoopDelay({ resolution: 20 });
_loopDelay.enable();
setInterval(() => {
  const max = _loopDelay.max / 1e6, p99 = _loopDelay.percentile(99) / 1e6;
  if (max >= 150) _log(`event-loop stall: max ${max.toFixed(0)}ms p99 ${p99.toFixed(0)}ms over the last 5s`);
  _loopDelay.reset();
}, 5000).unref();
const _sweepMarks = [];
const sweepMark = (label) => { _sweepMarks.push([label, perfNow.now()]); };
function reportSweep(t0) {
  const total = perfNow.now() - t0;
  if (total < 120) { _sweepMarks.length = 0; return; }
  let prev = t0;
  const parts = _sweepMarks.map(([l, t]) => { const d = t - prev; prev = t; return `${l} ${d.toFixed(0)}`; });
  _log(`sweep took ${total.toFixed(0)}ms: ${parts.join(', ')}`);
  _sweepMarks.length = 0;
}

function sampleReadiness(now = Date.now()) {
  const cutoff = now - READY_HIST_MS;
  for (const sym of live.map.keys()) {
    if (aliasKind(sym)) continue;
    const ok = compPriceReady(sym, now);
    let h = _readyHist.get(sym);
    /* Do not start the record until the symbol has been ready once. The first
       seconds after a restart are unready for everything while the composite
       refills, and counting that as unreliability made a healthy market look
       like it had a ten second outage. Recording begins at the first good
       sample, so genuine outages still count and the cold start does not. */
    if (!h) {
      if (!ok) continue;
      h = []; _readyHist.set(sym, h);
    }
    h.push({ t: now, ok });
    while (h.length && h[0].t < cutoff) h.shift();
  }
}
/** Fraction of the recorded window this symbol was competition-ready, and how
 *  much of that window we actually have. A market we have only just started
 *  watching returns a short `spanMs`, which the caller must not read as
 *  reliability. */
function readyRatio(sym, now = Date.now()) {
  const h = _readyHist.get(sym);
  if (!h || !h.length) return { ratio: 0, samples: 0, spanMs: 0, longestGapMs: Infinity, unknownMs: 0 };
  /* DURATION, not sample count.
   *
   * Counting samples made the estimator blind in exactly the situation it
   * exists to catch. If the sampler stalls (event-loop lag, a slow sweep, a
   * pause) the unready seconds simply are not sampled, so a 40 second outage
   * recorded as ready / unready / [40s of nothing] / ready came back as
   * "67% ready, longest gap 1s". Availability is measured over the interval
   * each sample REPRESENTS, and an interval longer than the sampling cadence
   * is not evidence of health: it is unknown, and unknown counts against the
   * market rather than for it. */
  const MAX_REPRESENTED_MS = SWEEP_MS * 2;
  let okMs = 0, totalMs = 0, unknownMs = 0, run = 0, longest = 0;
  for (let i = 1; i < h.length; i++) {
    const dt = h[i].t - h[i - 1].t;
    if (dt <= 0) continue;
    totalMs += dt;
    /* The whole interval is credited to the state at its END, because that is
       the state we actually observed; a gap longer than the cadence means we
       do not know what happened in between, so it is treated as an outage. */
    const stalled = dt > MAX_REPRESENTED_MS;
    if (stalled) unknownMs += dt;
    if (h[i].ok && !stalled) { okMs += dt; run = 0; }
    else { run += dt; longest = Math.max(longest, run); }
  }
  return {
    ratio: totalMs > 0 ? okMs / totalMs : 0,
    samples: h.length,
    spanMs: now - h[0].t,
    longestGapMs: longest,
    unknownMs,
  };
}
/* Which VENUE each component actually comes from.
 *
 * Binance USDT and Binance USDC are two books but one exchange, one matching
 * engine, one operator and one outage. Counting them as two confirmations
 * overstates confidence: if Binance prints a bad price, both move together and
 * the "two source" test passes while nothing independent has checked the
 * number. Independence is what a second source is FOR, so it is counted
 * separately from raw component count. */
const VENUE_OF = { usdt: 'binance', usdc: 'binance', usd: 'coinbase' };
const venueOf = (k) => VENUE_OF[k] || String(k);

/** Component prices for a symbol, how far apart they are, and how many
 *  genuinely independent venues stand behind them. */
function compQuality(sym, now = Date.now()) {
  const c = _idxComps.get(sym);
  if (!c) return { n: 0, fresh: 0, venues: 0, spreadBps: Infinity, prices: [], sources: [] };
  const entries = Object.entries(c);
  const fresh = entries.filter(([, x]) => compAgeMs(x, now) < COMP_FRESH_MS).length;
  const live = entries.filter(([, x]) => compAgeMs(x, now) < COMP_CORROBORATE_MS);
  const sources = live.map(([k, x]) => ({ source: k, venue: venueOf(k), px: x.px, ageMs: compAgeMs(x, now) }));
  const venues = new Set(sources.map((x) => x.venue)).size;
  const prices = sources.map((x) => x.px);
  const maxAgeMs = sources.length ? Math.max(...sources.map((x) => x.ageMs)) : Infinity;
  if (prices.length < 2) return { n: prices.length, fresh, venues, spreadBps: Infinity, prices, sources, maxAgeMs };
  const mean = prices.reduce((a, b) => a + b, 0) / prices.length;
  const spreadBps = mean > 0 ? ((Math.max(...prices) - Math.min(...prices)) / mean) * 1e4 : Infinity;
  return { n: prices.length, fresh, venues, spreadBps, prices, sources, maxAgeMs };
}

/* Quality demanded of a price that is carrying 1000x.
 *
 * At 1000x the liquidation distance is about 5bps, so the tolerances that are
 * reasonable for ordinary exposure are not reasonable here: a 2.9bps move
 * inside a 3bps gate consumes about 29% of initial margin and can finish a
 * position already part-way to liquidation. Permitted disagreement must be a
 * small fraction of the distance it is protecting, every source must be
 * genuinely fresh rather than merely corroborating, and the confirmations must
 * come from different venues. */
const BOOST_MAX_SPREAD_BPS = Number(process.env.PAPER_BOOST_MAX_SPREAD_BPS || 1);
const BOOST_MAX_SOURCE_AGE_MS = Number(process.env.PAPER_BOOST_MAX_SOURCE_AGE_MS || 5000);
const BOOST_MIN_VENUES = Number(process.env.PAPER_BOOST_MIN_VENUES || 2);
/* Above this leverage the boost rules apply. */
const BOOST_LEV_THRESHOLD = Number(process.env.PAPER_BOOST_LEV_THRESHOLD || 200);

/* The tightest set of INDEPENDENT venues that agree, and the price they imply.
 *
 * Averaging every component and testing max-min spread means a third venue
 * makes things worse: two venues agreeing at 100 plus one at 100.02 produces a
 * mean of 100.0067 and a 2bps spread, so adding a source REDUCED 1000x
 * availability instead of making an outlier outvotable. A quorum fixes that:
 * find the largest group of distinct venues inside the 1000x band, price off
 * that group, and name what was excluded. With no majority, nothing is priced
 * at 1000x, which is the honest answer rather than a mean nobody voted for.
 */
function boostConsensus(sym, now = Date.now()) {
  const q = compQuality(sym, now);
  /* One quote per venue, the freshest, so two books on one exchange cannot
     form a "majority" between themselves. */
  const byVenue = new Map();
  for (const src of q.sources) {
    if (src.ageMs > BOOST_MAX_SOURCE_AGE_MS) continue;         // 1000x needs genuinely fresh
    const cur = byVenue.get(src.venue);
    if (!cur || src.ageMs < cur.ageMs) byVenue.set(src.venue, src);
  }
  const pts = [...byVenue.values()].sort((a, b) => a.px - b.px);
  if (pts.length < BOOST_MIN_VENUES) return { ok: false, price: NaN, cluster: [], excluded: pts.map((p) => p.venue) };
  /* Largest window of venues whose disagreement fits the band. Ties go to the
     tighter window, so a real cluster beats an accidental one. */
  let best = null;
  for (let i = 0; i < pts.length; i++) {
    for (let j = pts.length - 1; j >= i + BOOST_MIN_VENUES - 1; j--) {
      const lo = pts[i].px, hi = pts[j].px;
      const mean = (lo + hi) / 2;
      const spread = mean > 0 ? ((hi - lo) / mean) * 1e4 : Infinity;
      if (spread <= BOOST_MAX_SPREAD_BPS) {
        const size = j - i + 1;
        if (!best || size > best.size || (size === best.size && spread < best.spread)) {
          best = { i, j, size, spread, mean };
        }
        break;                                                  // widest j for this i
      }
    }
  }
  if (!best) return { ok: false, price: NaN, cluster: [], excluded: pts.map((p) => p.venue) };
  const cluster = pts.slice(best.i, best.j + 1);
  const mid = cluster.length % 2
    ? cluster[(cluster.length - 1) / 2].px
    : (cluster[cluster.length / 2 - 1].px + cluster[cluster.length / 2].px) / 2;
  return {
    ok: true,
    price: mid,                                                 // median of the agreeing venues
    spreadBps: best.spread,
    cluster: cluster.map((p) => p.venue),
    excluded: pts.filter((p) => !cluster.includes(p)).map((p) => p.venue),
  };
}

/** The strongest leverage this symbol's current price quality can carry.
 *
 * Measured over 150s of live feed, the four majors meet the 1000x bar (two
 * independent venues, <=1bps disagreement, every source inside 5s) only about
 * 53% of the time; median spread alone is 0.7-0.95bps and p90 is 1.3-1.9bps.
 * Relaxing the bar to 2bps would raise availability to ~85%, but 2bps is 40%
 * of the 5bps liquidation distance that 1000x lives inside, which is the
 * trade the price gate exists to refuse.
 *
 * So the LEVERAGE moves, not the gate: a market prices at 1000x while its
 * sources genuinely agree, and falls back to the ordinary cap when they do
 * not, instead of the segment being cancelled or a 1000x position being marked
 * on a price that cannot carry it. The cap is disclosed, so a trader is never
 * told 1000x and given something else. */
/* With a single published source there is no quality tier to fall between:
 * either we are following a live source and the full cap applies, or we have
 * no price and nothing trades. This is what makes 1000x a stable headline
 * rather than something that moves during the window. */
/* WHAT A PRICE WE CANNOT AGE IS ALLOWED TO CARRY.
 *
 * The top tier is only checkable because Lazer stamps every quote with the
 * time it was observed. Both backups are stamped on ARRIVAL, because neither
 * wire format carries a provider time, so while a market runs on one of them
 * the engine cannot tell a 20ms quote from a 2s one. Arrival freshness is not
 * source freshness, and the top tier on an unmeasurable age is the one
 * combination this engine should never authorise.
 *
 *   cap    (default) the market stays open, but not above the base tier
 *   freeze the market is not priceable at all until an ageable source is back
 *   allow  the old behaviour, for a deliberate operator override
 *
 * Deliberately scoped to the tier the risk belongs to. An earlier cut applied
 * it to every leverage question, which capped every Binance-priced market in
 * the book and broke two suites that were correct to complain: ordinary
 * trading on a venue quote is how this product has always worked, and it is
 * not what a 500x mark on an unageable quote is. */
const UNAGEABLE_POLICY = String(process.env.PAPER_UNAGEABLE_POLICY || 'cap').toLowerCase();
/* The competition's ordinary tier, mirrored from competition.js so the rule
   can be applied here without a circular import. */
const UNAGEABLE_CAP_LEV = Number(process.env.PAPER_UNAGEABLE_CAP_LEV || 100);
const _unageableLogged = new Map();
function unageable(sym, now = Date.now()) {
  if (UNAGEABLE_POLICY === 'allow') return false;
  const base = baseOf(sym);
  const act = activeSource(base, now);
  if (!act) return false;
  const rec = (_idxComps.get(base) || {})[act.key];
  if (!rec || rec.ageKnown) return false;
  /* SAY IT WHERE IT COSTS SOMETHING, AND SAY IT PROPERLY.
   *
   * This paged ops once per market as each of 78 fell back to the venue leg,
   * with a line that read "held to 100x above 100x". Most of those markets are
   * not in any round and nobody is trading them at the top tier, so the alert
   * was pure noise twice in one day. It is an alert only for a market the
   * round is actually being decided on, and only while a round is running.
   * Everything else is a log line. */
  const capped = UNAGEABLE_POLICY === 'freeze'
    ? 'not priceable until an ageable source returns'
    : `capped at ${UNAGEABLE_CAP_LEV}x`;
  const round = comp.currentRound();
  const line = comp.usesLatestBackupPricing(round, base) && UNAGEABLE_POLICY !== 'freeze'
    ? `${base} is priced by ${act.key} with provider age unverified; intrinsic qualification is ${UNAGEABLE_CAP_LEV}x, while this round explicitly permits 500x on the latest accepted backup`
    : `${base} is priced by ${act.key}, which carries no provider timestamp, so it is ${capped}`;
  if (roundMarkets().has(base) && (comp.currentRound() || {}).id) tgOps('unageable:' + base, line);
  else if (now - (_unageableLogged.get(base) || 0) > 30 * 60_000) { _unageableLogged.set(base, now); _log(line); }
  return true;
}
function qualityLeverageCap(sym, now = Date.now()) {
  return compPriceReady(sym, now) ? Infinity : 0;
}
/* A competition may explicitly keep its last qualified observation as its
   execution price. This is NOT feed freshness: raw ingestion, diagnostics,
   public paper and old rounds still use compPriceReady/recordMark unchanged.
   One lineage per base prevents base/Hot/Boost from deciding the same account
   with different prices when the public source drops to a lower tier. */
const ROUND_PRICE_POLICY = 'last-accepted-v1';
let _roundPriceCandidate = null;
// Recovery candidates are visible only to one synchronous, all-roster risk
// transaction. They are neither raw observations nor published round marks.
let _roundRecoveryBatch = null;
let _roundRecoveryPostCommit = null;
function holdsRoundPrices(round = comp.currentRound()) {
  return !!(round && typeof comp.pricePolicyOf === 'function'
    && comp.pricePolicyOf(round) === ROUND_PRICE_POLICY);
}
function pricingRoundFor(userId) {
  const round = comp.currentRound();
  return round && userId != null && comp.inRound(userId, round)
    && holdsRoundPrices(round) ? round : null;
}
function roundExecutionMark(sym, { round = comp.currentRound(), at = Date.now(),
  forLeverage = 0, candidate = true } = {}) {
  if (!holdsRoundPrices(round)) return null;
  const base = baseOf(sym), time = Number(at);
  if (!STAGE_INDEXED.has(base) || DISABLED_MARKETS.has(base)
      || !Number.isFinite(time)) return null;
  const batch = candidate && _roundRecoveryBatch;
  const token = candidate && (batch && db.inTransaction && batch.roundId === round.id
    ? batch.marks.get(base) : _roundPriceCandidate);
  const exactCandidate = !!(token && db.inTransaction && token.roundId === round.id
    && token.record.base === base && live.map.get(base) === token.entry
    && token.record.acceptedAt <= time);
  // A failed risk/source write is a system failure, not a quote-age policy.
  if (candidate && _srcUnsafe.has(base)) return null;
  const row = exactCandidate ? token.record : comp.roundMarkGet(round.id, base, time);
  if (!row || row.acceptedBoot !== ENGINE_BOOT_ID || row.base !== base
      || row.hardInvalid || row.hardFailure) return null;
  const required = Math.max(Number(forLeverage) || 0,
    Number(comp.requiredRoundMarkLeverage(round, base)) || 0);
  if (!(Number.isFinite(row.price) && row.price > 0
      && Number.isSafeInteger(row.acceptedSeq) && row.acceptedSeq > 0
      && Number.isFinite(row.acceptedAt) && row.acceptedAt > 0 && row.acceptedAt <= time
      && Number.isFinite(row.appliedAt) && row.appliedAt >= row.acceptedAt && row.appliedAt <= time
      && Number.isFinite(row.observedAt) && row.observedAt > 0
      && row.observedAt <= row.acceptedAt
      && Number.isFinite(row.originalValidUntil) && row.originalValidUntil > row.appliedAt
      && typeof row.source === 'string' && /^[a-z][a-z0-9_-]{0,31}$/.test(row.source)
      && comp.roundMarkExecutionLeverage(round, row) >= required)) return null;
  const current = live.map.get(base);
  return { ...row, held: time >= row.originalValidUntil || !current
    || current.acceptedSeq !== row.acceptedSeq || !!current.indexHalt || _confirming.has(base)
    || _riskBlocked.has(base) || (row.source === 'lazer' && _lazerRejects.has(base)) };
}
const _roundHistoricalPause = db.prepare(`SELECT started_at, ended_at FROM paper_round_clock_pauses
  WHERE round_id=? AND started_at<=? AND (ended_at IS NULL OR ended_at>?)
  ORDER BY started_at DESC LIMIT 1`);
function historicalRoundMark(sym, round, at, forLeverage = 0) {
  if (!holdsRoundPrices(round) || !Number.isFinite(at) || at > Date.now()) return null;
  const base = baseOf(sym);
  if (!/^[A-Z0-9]{1,24}$/.test(base) || _roundHistoricalPause.get(round.id, at, at)) return null;
  // Durable history is not current execution authority. A pending boundary
  // before a restart may use its genuine predecessor from that prior boot,
  // but never a candidate, a later observation, or an interval in the pause.
  const row = comp.roundMarkGet(round.id, base, at);
  const required = Math.max(Number(forLeverage) || 0, comp.requiredRoundMarkLeverage(round, base));
  if (!row || row.base !== base || row.hardInvalid !== false || row.hardFailure != null
      || typeof row.acceptedBoot !== 'string' || !/^[a-zA-Z0-9_-]{1,64}$/.test(row.acceptedBoot)
      || !Number.isSafeInteger(row.acceptedSeq) || row.acceptedSeq <= 0
      || !Number.isFinite(row.price) || row.price <= 0
      || !(Number.isFinite(row.observedAt) && row.observedAt > 0
        && Number.isFinite(row.acceptedAt) && row.acceptedAt >= row.observedAt
        && Number.isFinite(row.appliedAt) && row.appliedAt >= row.acceptedAt && row.appliedAt <= at
        && Number.isFinite(row.originalValidUntil) && row.originalValidUntil > row.appliedAt
        && comp.roundMarkExecutionLeverage(round, row) >= required)
      || typeof row.source !== 'string' || !/^[a-z][a-z0-9_-]{0,31}$/.test(row.source)) return null;
  return row;
}
function roundPriceReady(sym, leverage = 0, at = Date.now(), round = comp.currentRound()) {
  if (!holdsRoundPrices(round)) return readyForLeverage(sym, leverage, at);
  if (roundExecutionMark(sym, { round, at, forLeverage: leverage })) return true;
  // Armed readiness may prove that start can adopt a CURRENT real quote.
  // This is not execution authority: only initialization's transaction can
  // turn that strict same-boot observation into a round mark.
  if (round.status !== 'armed') return false;
  const entry = live.map.get(baseOf(sym));
  if (!entry || entry.acceptedBoot !== ENGINE_BOOT_ID) return false;
  const required = Math.max(Number(leverage) || 0,
    comp.requiredRoundMarkLeverage(round, baseOf(sym)));
  if (comp.usesLatestBackupPricing(round, baseOf(sym))) {
    const token = candidateRoundMark(baseOf(sym), entry, at, entry.acceptedSeq, round);
    return !!token && comp.roundMarkExecutionLeverage(round, token.record) >= required;
  }
  return boostLevCap(baseOf(sym), required, at) >= required;
}
function roundPriceAvailability(sym, leverage = 0, at = Date.now(), round = comp.currentRound()) {
  if (!holdsRoundPrices(round)) return marketAvailability(sym, leverage, at);
  const quote = roundExecutionMark(sym, { round, at, forLeverage: leverage });
  if (!quote && round.status === 'armed' && roundPriceReady(sym, leverage, at, round)) {
    if (comp.usesLatestBackupPricing(round, baseOf(sym))) {
      const entry = live.map.get(baseOf(sym));
      const token = entry && candidateRoundMark(baseOf(sym), entry, at, entry.acceptedSeq, round);
      return { symbol: baseOf(sym), ready: !!token, known: !!token,
        validUntil: token ? token.record.originalValidUntil : null,
        invalidSince: token ? null : at };
    }
    return marketAvailability(sym, Math.max(Number(leverage) || 0,
      comp.requiredRoundMarkLeverage(round, baseOf(sym))), at);
  }
  return { symbol: baseOf(sym), ready: !!quote, known: !!quote,
    // No PRICE deadline. The independent identity/control leases still expire.
    validUntil: null, invalidSince: quote ? null : at };
}
function invalidateRoundMark(sym, reason, at = Date.now()) {
  const round = comp.currentRound();
  if (!holdsRoundPrices(round)) return;
  try { comp.roundMarkInvalidate(round.id, baseOf(sym), reason, at); }
  catch (e) {
    // Invalidation itself must be durable before another request can trade.
    pauseRound(GLOBAL_FEED_PAUSE_SYMBOL, 'round price persistence',
      'round execution price invalidation failed', at);
    try { comp.blockRound(round.id, 'round execution price invalidation failed'); } catch {}
    return false;
  }
  _stateCache = { at: 0, body: null };
  scheduleCompBoard(baseOf(sym), true);
  return true;
}
function intrinsicCandidateAdmission(sym, entry, at) {
  let cap = 0, accepted = false;
  const previousProbe = _riskProbe;
  try {
    if (_riskBlocked.has(sym) && !previousProbe) _riskProbe = { sym, entry };
    // STRICT intrinsic acceptance only; never consult a held quote here.
    accepted = compPriceReady(sym, at);
    cap = boostLevCap(sym, HEAT_MAX_LEV, at);
  } finally { _riskProbe = previousProbe; }
  return { accepted, leverageCap: cap };
}
function candidateRoundMark(sym, entry, at, sequence, round = comp.currentRound()) {
  if (!holdsRoundPrices(round) || !entry || !Number.isSafeInteger(sequence)) return null;
  const { accepted, leverageCap: cap } = intrinsicCandidateAdmission(sym, entry, at);
  if (!accepted) return null;
  const observedAt = Number(entry.pythSrcAtMs) || Number(entry.pythAtMs);
  const record = {
    base: baseOf(sym), price: Number(entry.pythPrice), acceptedBoot: ENGINE_BOOT_ID,
    acceptedSeq: sequence, acceptedAt: Number(entry.pythAtMs), observedAt, appliedAt: at,
    source: entry.srcKey,
    originalValidUntil: Math.min(Number(entry.pythAtMs) + PYTH_STAGE_FRESH_MS,
      observedAt + staleMsForSym(sym, entry.srcKey)),
    acceptedLeverageCap: cap,
    ...(comp.usesLatestBackupPricing(round, sym) ? {
      executionLeverageCap: 500, leveragePolicy: comp.backupExecutionPolicyOf(round),
      // This records what ingestion actually knew, not a fabricated provider
      // timestamp or a claim that the source intrinsically qualifies for500x.
      sourceAgeKnown: _idxComps.get(baseOf(sym))?.[entry.srcKey]?.ageKnown === true,
    } : {}),
  };
  if (comp.roundMarkExecutionLeverage(round, record) < comp.requiredRoundMarkLeverage(round, sym)) return null;
  if (!STAGE_INDEXED.has(record.base) || DISABLED_MARKETS.has(record.base)
      || !(Number.isFinite(record.price) && record.price > 0
        && Number.isSafeInteger(record.acceptedSeq) && record.acceptedSeq > 0
        && Number.isFinite(record.observedAt) && record.observedAt > 0
        && Number.isFinite(record.acceptedAt) && record.acceptedAt >= record.observedAt
        && Number.isFinite(record.appliedAt) && record.appliedAt >= record.acceptedAt
        && record.appliedAt <= Date.now()
        && Number.isFinite(record.originalValidUntil) && record.originalValidUntil > record.appliedAt
        && Number.isFinite(record.acceptedLeverageCap))
      || typeof record.source !== 'string' || !/^[a-z][a-z0-9_-]{0,31}$/.test(record.source)) return null;
  const prior = comp.roundMarkGet(round.id, baseOf(sym), at);
  // A source handover may be intrinsically admissible to the public feed but
  // still precede this round's observation watermark. It is a non-adoption,
  // not a failed economic transaction or a reason to revoke the held mark.
  if (prior && (record.appliedAt < prior.appliedAt
      || (record.acceptedBoot === prior.acceptedBoot
        && (record.acceptedSeq <= prior.acceptedSeq
          || record.acceptedAt < prior.acceptedAt || record.observedAt < prior.observedAt)))) return null;
  return { roundId: round.id, record, entry,
    expectedIdentity: prior ? { acceptedBoot: prior.acceptedBoot, acceptedSeq: prior.acceptedSeq } : null };
}
function initializeRoundMarks(round, { at = Date.now() } = {}) {
  if (!holdsRoundPrices(round)) return;
  if (!db.inTransaction) throw new Error('round prices require the round transaction');
  for (const base of STAGE_INDEXED) {
    if (DISABLED_MARKETS.has(base)) continue;
    const prior = comp.roundMarkGet(round.id, base, at);
    const entry = live.map.get(base);
    // Seeding adopts an already committed, still intrinsically valid quote.
    // Its original observation/acceptance times and sequence never change.
    if (!entry || entry.acceptedBoot !== ENGINE_BOOT_ID) continue;
    if (prior && prior.acceptedBoot === entry.acceptedBoot
        && prior.acceptedSeq === entry.acceptedSeq) continue;
    const token = candidateRoundMark(base, entry, at, entry.acceptedSeq, round);
    if (token) comp.roundMarkCommit(round.id, token.record, token.expectedIdentity);
  }
}
function publicRoundExecution(round, revision, issuedAt = Date.now()) {
  if (!holdsRoundPrices(round) || round.status !== 'running') return null;
  const marks = [], unavailable = [];
  let healthy = false;
  let identity = null;
  try {
  for (const base of STAGE_INDEXED) {
    if (DISABLED_MARKETS.has(base)) continue;
    const row = roundExecutionMark(base, { round, at: issuedAt, candidate: false });
    if (!row) { unavailable.push(base); continue; }
    marks.push({ symbol: base, price: row.price, acceptedBoot: row.acceptedBoot,
      acceptedSeq: row.acceptedSeq, acceptedAt: row.acceptedAt,
      observedAt: row.observedAt, appliedAt: row.appliedAt, source: row.source,
      originalValidUntil: row.originalValidUntil,
      acceptedLeverageCap: row.acceptedLeverageCap, ...comp.roundMarkPolicyFields(row), held: row.held });
  }
  identity = auth.authHealth(issuedAt);
  healthy = !round.blocked_reason && !roundPaused()
    && !deploymentMaintenanceActive() && identity.tradingAvailable === true
    && Number.isFinite(identity.validUntil) && identity.validUntil > issuedAt
    && competitionClockStatus(issuedAt).ok;
  } catch {
    // A failed authority read cannot extend a previously issued control
    // lease, publish a partial roster, or escape the board timer callback.
    marks.length = 0;
    unavailable.length = 0;
    for (const base of STAGE_INDEXED) if (!DISABLED_MARKETS.has(base)) unavailable.push(base);
  }
  return { v: 1, policy: ROUND_PRICE_POLICY, roundId: round.id,
    boot: ENGINE_BOOT_ID, revision, issuedAt,
    validUntil: healthy ? Math.floor(Math.min(issuedAt + 2000, identity.validUntil)) : issuedAt,
    marks, unavailable };
}
function roundPriceAcknowledged(body, userId, symbol) {
  const round = pricingRoundFor(userId);
  if (!round) return true;
  const mark = roundExecutionMark(symbol, { round });
  // This is explicit policy/lineage acknowledgment, not an old-price fill
  // promise. Ordinary evPx requote and the current executable mark still win.
  return !!(mark && body && body.pricePolicy === ROUND_PRICE_POLICY
    && body.roundId === round.id && body.acceptedBoot === ENGINE_BOOT_ID
    && Number.isSafeInteger(body.acceptedSeq) && body.acceptedSeq > 0
    && body.acceptedSeq <= mark.acceptedSeq);
}
function requireRoundPriceAcknowledgment(res, body, userId, symbol) {
  if (roundPriceAcknowledged(body, userId, symbol)) return true;
  send(res, 409, { ok: false, error: 'round_price_policy_required', definitive: true });
  return false;
}
function priceAcknowledgmentFor(userId, symbol) {
  const round = pricingRoundFor(userId);
  const mark = round && roundExecutionMark(symbol, { round, candidate: false });
  return mark ? { pricePolicy: ROUND_PRICE_POLICY, roundId: round.id,
    acceptedBoot: mark.acceptedBoot, acceptedSeq: mark.acceptedSeq } : null;
}
/** The boost cap to advertise and enforce for a market right now. */
function boostLevCap(sym, engineCap, now = Date.now(), round = null) {
  if (holdsRoundPrices(round)) {
    const quote = roundExecutionMark(sym, { round, at: now, forLeverage: engineCap });
    return quote ? Math.min(engineCap, comp.roundMarkExecutionLeverage(round, quote)) : 0;
  }
  const cap = qualityLeverageCap(sym, now);
  if (cap === 0) return 0;
  const out = cap === Infinity ? engineCap : Math.min(engineCap, BOOST_LEV_THRESHOLD);
  if (out > UNAGEABLE_CAP_LEV && unageable(sym, now)) {
    return UNAGEABLE_POLICY === 'freeze' ? 0 : Math.min(out, UNAGEABLE_CAP_LEV);
  }
  return out;
}
/** Is this price good enough to carry the requested leverage? */
function readyForLeverage(sym, lev, now = Date.now()) {
  const want = Number(lev) || 0;
  if (!(want > BOOST_LEV_THRESHOLD)) {
    if (!compPriceReady(sym, now)) return false;
    /* The rule bites above the base tier whatever the threshold: a Hot ticker
       asking for 150x on a quote we cannot age is the same exposure. */
    return want <= UNAGEABLE_CAP_LEV || !unageable(sym, now);
  }
  return qualityLeverageCap(sym, now) === Infinity && !unageable(sym, now);
}
function compPriceReady(sym, now = Date.now(), detail = null) {
  /* AN ALIAS HAS NO PRICE OF ITS OWN. SOL-HOT *is* SOL for marks, config, lots
     and leverage, which is exactly why openAlias refuses a base that is not
     indexed. This looked the alias up in live.map directly, where aliases are
     never stored, found nothing, and reported the market unpriceable.
     Every risk path that asks about a segment ticker went through here:
     readyForLeverage, qualityLeverageCap, boostLevCap and strict markAt. So
     the moment a contestant held a position on a freshly opened Hot ticker,
     the sweep declared it unpriceable and paused the ENTIRE round.
     round6, 2026-09-01: SOL-HOT opened 04:56:08, round paused 04:56:20, and it
     stayed frozen through the whole Boost phase to the bell at 05:14 while the
     clock kept running. Contestants could not even close, because a pause
     freezes all contestant risk by design.
     Two call sites already wrapped this in baseOf(); resolving here is what
     makes every path agree instead of two of them being individually correct. */
  sym = baseOf(sym);
  const m = live.map.get(sym);
  if (detail) { detail.reason = 'ready'; detail.source = null; }
  /* During one candidate's boundary/segment prelude, answer from the verdict
     captured before its component and clamp state changed. Object identity
     (or exact prior absence) keeps this exception pinned to that one market
     and prevents current components from laundering an uncommitted tick. */
  const committedProbe = _committedReadinessProbe;
  if (committedProbe && committedProbe.sym === sym
      && (committedProbe.entry
        ? committedProbe.entry === m : !m)) {
    if (detail) detail.reason = 'transient_probe';
    return committedProbe.ready;
  }
  if (!m) { if (detail) detail.reason = 'no_observation'; return false; }
  if (m.indexHalt) { if (detail) detail.reason = 'index_halted'; return false; }
  /* A jump is being confirmed: we do not currently know this price. */
  if (_confirming.has(sym)) { if (detail) detail.reason = 'jump_confirmation'; return false; }
  /* The newest observation on this market could not complete a risk pass, so
     the price still in the map is one the engine has already stopped standing
     behind. Not executable until a later observation commits. */
  const exactRiskProbe = !!(_riskProbe && _riskProbe.sym === sym
    && _riskProbe.entry === m);
  if (_riskBlocked.has(sym) && !exactRiskProbe) { if (detail) detail.reason = 'risk_failed'; return false; }
  /* A source transition that could not be recorded durably is not a source we
     are willing to decide a result with. */
  if (_srcUnsafe.has(sym)) { if (detail) detail.reason = 'source_persistence_failed'; return false; }
  if (!(Number(m.pythPrice) > 0)) { if (detail) detail.reason = 'invalid_price'; return false; }
  if (now - (m.pythAtMs || 0) >= PYTH_STAGE_FRESH_MS) {
    if (detail) detail.reason = 'receipt_expired';
    return false;
  }
  /* One published source, so the question is no longer "do the venues agree"
     but "is the source we named still ticking". Corroboration and spread tests
     belonged to a blended index; against a single named source they would
     refuse prices that are correct by definition. */
  /* Judge the source that ACTUALLY PUBLISHED the price we would trade on,
     against that source's own deadline. Recomputing the active source here
     independently is what let source identity and executable mark disagree:
     the selector could name Binance while live.map still held Lazer's price. */
  /* Nothing live in the chain, under each source's OWN deadline, means the
     market is not priceable. This is the check that used to be a generic 5s
     window applied to a record that had already failed its own rule. */
  const act = activeSource(sym, now);
  if (detail) detail.source = act ? act.key : null;
  if (!act) { if (detail) detail.reason = 'source_unavailable'; return false; }
  /* Where the published price records which source set it, that source must
     still be the one the chain would choose: identity and executable mark can
     never refer to different sources. (A price published outside compUpdate
     carries no srcKey, and is judged on the active source alone.) */
  const srcKey = m.srcKey || null;
  if (srcKey && srcKey !== act.key) { if (detail) detail.reason = 'source_transition_pending'; return false; }
  const ready = now - (m.pythAtMs || 0) < staleMsForSym(sym, srcKey || act.key);
  if (detail && !ready) detail.reason = 'source_expired';
  return ready;
}

/* Readiness plus the interval edge that produced it. A boolean is enough to
 * refuse a trade; the active competition clock additionally needs to know
 * when refusal became true so a delayed event loop cannot burn those invalid
 * milliseconds. `validUntil` is the next instant at which the current
 * executable observation ceases to be one. */
function marketAvailability(sym, leverage = 0, now = Date.now()) {
  const base = baseOf(sym);
  const want = Number(leverage) || 0;
  const m = live.map.get(base);
  const round = comp.currentRound();
  const floor = Number(round && round.started_at) || now;
  const clamp = (t) => Math.max(floor, Math.min(now, Number(t) || now));
  const deadlines = [];
  if (m) {
    const acceptAt = Number(m.pythAtMs) || 0;
    const sourceAt = Number(m.pythSrcAtMs) > 0
      ? Number(m.pythSrcAtMs) : acceptAt;
    const srcKey = m.srcKey || null;
    if (acceptAt) deadlines.push(acceptAt + PYTH_STAGE_FRESH_MS);
    if (sourceAt) deadlines.push(sourceAt + staleMsForSym(base, srcKey));
  }
  const validUntil = deadlines.length ? Math.min(...deadlines) : now;
  const ready = want > 0
    ? readyForLeverage(base, want, now)
    : compPriceReady(base, now);
  if (ready) return { symbol: base, ready: true, known: true, validUntil };

  const onsets = [];
  if (m && m.indexHalt) {
    const halt = _halt.get(base);
    onsets.push(halt && (halt.haltedAt || halt.divergeSince));
  }
  const confirming = _confirming.get(base);
  if (confirming) onsets.push(confirming.since);
  if (_riskBlocked.has(base)) onsets.push(_riskBlockedSince.get(base));
  if (_srcUnsafe.has(base)) onsets.push(_srcUnsafe.get(base));
  if (deadlines.length && validUntil <= now) onsets.push(validUntil);

  /* A high-leverage Boost quote can be refused while ordinary readiness is
     still true (for example an unageable fallback source). Its refusal begins
     with the observation/source selection that installed that quote. */
  if (want > BOOST_LEV_THRESHOLD && m && compPriceReady(base, now)
      && !readyForLeverage(base, want, now)) {
    onsets.push(Number(m.pythAtMs) || now);
  }

  /* If the live map vanished after having accepted marks, the last immutable
     history verdict still tells us exactly when its usable interval ended. */
  if (!m) {
    const hist = _markHist.get(base) || [];
    for (let i = hist.length - 1; i >= 0; i--) {
      if (!hist[i].ok) continue;
      onsets.push(Number(hist[i].validUntil) || Number(hist[i].t));
      break;
    }
  }
  const knownOnsets = onsets.map(Number).filter(Number.isFinite);
  return {
    symbol: base, ready: false, known: !!(m || knownOnsets.length),
    invalidSince: clamp(knownOnsets.length ? Math.min(...knownOnsets) : now),
    validUntil: null,
  };
}

/* Immutable availability at a past boundary. Current live flags are not a
 * historical oracle: a halt that begins after Hot ended cannot retroactively
 * invalidate its close. markAt applies the persisted invalidity intervals and
 * the accepted observation's own expiry/leverage verdict at `at`; this helper
 * adds the interval edge needed when that past instant really was invalid. */
function historicalAvailabilityAt(sym, leverage = 0, at = Date.now(), round = null) {
  if (holdsRoundPrices(round)) {
    const row = historicalRoundMark(sym, round, at, leverage);
    const evidence = comp.roundMarkGet(round.id, baseOf(sym), at);
    const pause = _roundHistoricalPause.get(round.id, at, at);
    return { symbol: baseOf(sym), ready: !!row, known: !!evidence, validUntil: null,
      invalidSince: row ? null : (pause?.started_at ?? evidence?.hardFailure?.at ?? at) };
  }
  const base = baseOf(sym);
  const ts = Number(at);
  const want = Number(leverage) || 0;
  const px = markAt(base, ts, { strict: true, forLeverage: want });
  const hist = _markHist.get(base) || [];
  let obs = null;
  for (let i = hist.length - 1; i >= 0; i--) {
    if (Number(hist[i].t) <= ts) { obs = hist[i]; break; }
  }
  if (Number(px) > 0) {
    return { symbol: base, ready: true, known: true,
      validUntil: obs && Number.isFinite(Number(obs.validUntil)) ? Number(obs.validUntil) : ts };
  }
  const onsets = [];
  for (const d of _disputes.get(base) || []) {
    if (ts >= Number(d.from) && (d.to == null || ts < Number(d.to))) onsets.push(Number(d.from));
  }
  if (obs) {
    if (obs.ok === false) onsets.push(Number(obs.t));
    if (Number.isFinite(Number(obs.validUntil)) && Number(obs.validUntil) <= ts) {
      onsets.push(Number(obs.validUntil));
    }
  }
  const known = onsets.filter(Number.isFinite);
  return { symbol: base, ready: false, known: !!(obs || known.length),
    invalidSince: known.length ? Math.min(...known) : ts, validUntil: null };
}

/* Evidence captured once, atomically with Hot activation. It stays private in
 * the resolution table until settlement/started-abort, then lets an
 * independent verifier see that every earlier committed choice failed the
 * exact strict boundary test and the selected choice was the first that
 * passed. */
function marketEvidenceAt(sym, at = Date.now(), kind = 'HOT', requiredLeverage = null, round = null) {
  const base = baseOf(sym);
  const ts = Number(at);
  const leverage = kind === 'BOOST'
    ? (Number(requiredLeverage) || HEAT_MAX_LEV) : comp.COMP_BASE_LEV;
  if (holdsRoundPrices(round)) {
    const row = historicalRoundMark(base, round, ts, leverage);
    const record = comp.roundMarkGet(round.id, base, ts);
    const pause = _roundHistoricalPause.get(round.id, ts, ts);
    const reason = row ? null : pause ? 'round clock was paused at the boundary'
      : record?.hardFailure ? 'round execution mark has a recorded system failure'
        : 'no qualified committed round observation at the boundary';
    return {
      ready: !!row, historicalReady: !!row,
      liveReady: !!roundExecutionMark(base, { round, forLeverage: leverage }),
      checkedAt: ts, liveCheckedAt: Date.now(), boundaryMark: row?.price ?? null,
      rejectReason: reason,
      policy: { version: ROUND_PRICE_POLICY, kind, buildId: PAPER_BUILD_ID,
        engineBoot: record?.acceptedBoot || ENGINE_BOOT_ID,
        ...comp.backupExecutionPolicyFields(round),
        leverageRequired: Math.max(leverage, comp.requiredRoundMarkLeverage(round, base)) },
      clockPause: pause ? { from: pause.started_at, to: pause.ended_at } : null,
      invalidity: row ? null : { since: pause?.started_at ?? record?.hardFailure?.at ?? ts, reason },
      observation: record ? { ...record, accepted: !!row } : null,
    };
  }
  const historical = historicalAvailabilityAt(base, leverage, ts);
  const liveReady = kind === 'BOOST'
    ? readyForLeverage(base, leverage)
    : compPriceReady(base);
  const hist = _markHist.get(base) || [];
  let row = null;
  for (let i = hist.length - 1; i >= 0; i--) {
    if (Number(hist[i].t) <= ts) { row = hist[i]; break; }
  }
  const boundaryMark = row && historical.ready ? Number(row.p) : null;
  let rejectReason = null;
  if (!historical.ready) {
    if (!row) rejectReason = 'no accepted observation at or before activation';
    else if (row.ok === false) rejectReason = 'observation failed competition price policy';
    else if (Number(row.validUntil) <= ts) rejectReason = 'observation expired before activation';
    else if (kind === 'BOOST' && Number(row.leverageCap) < leverage) rejectReason = 'observation could not support Boost leverage';
    else rejectReason = 'activation instant lies in a recorded invalidity interval';
  }
  return {
    /* `ready` is the exact-boundary fallback verdict. Current liveness is
       carried separately: a market that fails after zero pauses its already
       locked activation; it does not let a later fallback steal the draw. */
    ready: !!historical.ready,
    historicalReady: !!historical.ready,
    liveReady: !!liveReady,
    checkedAt: ts,
    liveCheckedAt: Date.now(),
    boundaryMark,
    rejectReason,
    policy: {
      version: 'competition-price-v2', kind,
      buildId: PAPER_BUILD_ID,
      leverageRequired: leverage,
      maximumHistoricalAgeMs: MARK_MAX_AGE_MS,
    },
    invalidity: historical.ready ? null : {
      since: Number.isFinite(Number(historical.invalidSince))
        ? Number(historical.invalidSince) : ts,
      reason: rejectReason,
    },
    observation: row ? {
      mark: Number(row.p), observedAt: Number(row.t), validUntil: Number(row.validUntil),
      accepted: !!row.ok, source: row.src || null,
      sourceAt: Number(row.srcTs) || null,
      components: Number(row.n), spreadBps: Number(row.sp),
      leverageCap: Number(row.leverageCap) || 0,
      freshComponents: Number(row.f) || 0,
      venues: Number(row.v) || 0,
      maximumComponentAgeMs: Number(row.age) || 0,
    } : null,
  };
}

function stageMark(m, sym = null, competitor = false) {
  if (competitor && sym && holdsRoundPrices()) {
    return roundExecutionMark(sym)?.price ?? NaN;
  }
  if (!m) return NaN;
  if (m.indexHalt) return NaN;   // guard tripped: stage freezes — no marks, no fills, no liquidations
  /* Mid-confirmation the old mark is known to be wrong and the new one is not
     yet trusted. Neither may price a competitor's action. */
  if (competitor && sym && _confirming.has(sym)) return NaN;
  /* Never fall back from the composite to a raw venue mark for stage pricing.
     That fallback is what makes a cold restart able to liquidate 1000x
     exposure in a different price world. */
  /* Strictness belongs to the PLAYERS in the round, not to the clock. It was
     derived from "a round exists", so every ordinary /ftpaper trader was
     silently moved into competition pricing for half an hour whenever a show
     was on air, and could have a position frozen by a rule that has nothing
     to do with them. */
  const strict = competitor && compStrictNow();
  if (Number(m.pythPrice) > 0 && Date.now() - (m.pythAtMs || 0) < PYTH_STAGE_FRESH_MS) {
    if (sym && strict && !compPriceReady(sym)) return NaN;
    return Number(m.pythPrice);
  }
  /* The venue-mark fallback is ~10bps from the composite, twice the 1000x
     liquidation distance. Tolerable for ordinary paper trading; not while a
     result is being decided. */
  if (strict) return NaN;
  const basis = Number(m.pythBasis);
  return Number.isFinite(basis) ? Number(m.markPrice) - basis : Number(m.markPrice);
}
/* Strictness is scoped to a live competition rather than applied to all stage
   accounts. The ordinary /ftpaper product keeps the behaviour it has always
   had; the show gets the stricter world exactly while a round is running,
   which is when a wrong price changes a published result. */
let _strictOverride = null;   // tests
function compStrictNow() {
  if (_strictOverride !== null) return _strictOverride;
  try {
    const r = comp.currentRound();
    if (!r) return false;
    /* The fail-open switch is a REHEARSAL tool. A real round that cannot price
       its own result strictly has no business running, so the switch is
       ignored outside rehearsals rather than quietly disabling the guarantee
       that decides who gets paid. */
    if (process.env.PAPER_COMP_STRICT === '0') return r.kind !== 'rehearsal' ? true : false;
    return true;
  } catch { return false; }
}
function markFor(m, heat, sym = null, competitor = false) {
  return heat ? stageMark(m, sym, competitor) : effMark(m);
}
/** Is this account actually seated in the live round? */
function competingNow(userId) {
  try { const r = comp.currentRound(); return !!(r && comp.inRound(userId, r)); } catch { return false; }
}
/* A price for an ACTION, with the context that decides whether it is allowed.
 *
 * The old signature took only (symbol, heat), so it had no idea who was asking
 * or what leverage the action carried. That made the strict verdict advisory:
 * a seated trader could open a position, and more damagingly manually CLOSE
 * one and bank the move, on a price competition pricing had already refused.
 * Automated risk was frozen while the trader was not, which is a competition
 * exploit rather than a display issue.
 *
 * `userId` decides competitor scoping; `forLeverage` decides which quality bar
 * applies. Both must be passed by every competition mutation. */
/** The leverage an existing position actually carries, so a mutation on it is
 *  judged against the quality that exposure needs rather than the default. */
function levOfPosition(userId, symbol) {
  const p = stmt.posGet.get(userId, symbol);
  return p ? Number(p.leverage) || 0 : 0;
}
function markOfFreshFor(sym, heat, { userId = null, forLeverage = 0 } = {}) {
  const round = heat && pricingRoundFor(userId);
  if (round) return roundExecutionMark(sym, { round, forLeverage })?.price ?? null;
  const m = mkt(sym);
  if (!mktFresh(m)) return null;
  const competitor = userId != null && competingNow(userId);
  const px = markFor(m, heat, baseOf(sym), competitor);
  if (!Number.isFinite(px) || px <= 0) return null;
  /* Exposure-aware: a price good enough for ordinary size is not automatically
     good enough to open, close or liquidate 1000x. */
  if (competitor && !readyForLeverage(baseOf(sym), forLeverage)) return null;
  return px;
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
/* Indirected so a test can inject a failure between claiming a seat and
   creating its account, which is the only way to prove they are one act. */
let _ensureAccountFn = (u) => ensureAccount(u);
function ensureAccount(userId) {
  const now = Date.now();
  stmt.acctIns.run(userId, now, now);
  return stmt.acctGet.get(userId);
}
const dirOf = (side) => (side === 'LONG' ? 1 : -1);
const isIso = (p) => p.margin_mode === 'isolated';
function reducesPosition(pos, orderSide, size) {
  return !!pos && (orderSide === 'BUY') !== (pos.side === 'LONG')
    && Number(size) > 0 && Number(size) <= Number(pos.size) + 1e-12;
}
function uPnl(pos, mark) { return pos.size * (mark - pos.entry_price) * dirOf(pos.side); }
/* Value an isolated leg exactly as a full close at this mark would credit it.
 *
 * Fills, realised PnL, isolated margin and account balances are denominated in
 * six-decimal ledger units. Marking an open leg with the unrounded floating
 * point PnL while its mandatory bell close credits r6(PnL) can move equity by
 * one micro-unit at the close. That is economically tiny but cryptographically
 * important: the immutable final score and its post-cleanup proof must describe
 * the same account. Keep this calculation in one helper so live equity,
 * checkpoint proofs and the actual full-close path share the ledger rule. */
function isolatedSettlementValue(margin, rawPnl) {
  return Math.max(0, r6(r6(Number(margin) || 0) + r6(Number(rawPnl) || 0)));
}
/* Recent stage marks, per symbol, so a boundary can be priced at the instant
   it was SCHEDULED rather than whenever its timer callback happened to run.
   At 1000x a few hundred milliseconds of event-loop delay is worth real
   money, and "the winner is whoever the scheduler favoured" is not a result
   anyone can defend. Bounded by time and by count; pricing must never be
   slowed by bookkeeping. */
const MARK_HIST_MS = 180_000;
const MARK_HIST_MAX = 900;
/* APPEND-ONLY DISPUTE LEDGER.
 *
 * A price dispute is an INTERVAL during which we did not know the level, and
 * that fact has to outlive the dispute. It used to live only in `_confirming`,
 * so the moment a jump resolved, history forgot the interval had ever been in
 * question and a boundary inside it started answering with the pre-jump mark
 * again: valid, then null while confirming, then valid once more. Recording
 * the interval makes the answer permanent in both directions.
 *
 * Persisted, because a restart must not silently un-dispute a settled round. */
const _disputes = new Map();     // sym -> [{from, to|null}] ascending
let _disputeStore = null;
function disputeStore() {
  if (_disputeStore) return _disputeStore;
  db.exec(`CREATE TABLE IF NOT EXISTS paper_price_disputes (
    id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, since INTEGER NOT NULL,
    until INTEGER, from_px REAL, to_px REAL)`);
  db.exec('CREATE INDEX IF NOT EXISTS ix_disputes_sym ON paper_price_disputes (symbol, since)');
  _disputeStore = {
    open:  db.prepare('INSERT INTO paper_price_disputes (symbol, since, until, from_px, to_px) VALUES (?, ?, NULL, ?, ?)'),
    close: db.prepare('UPDATE paper_price_disputes SET until = ? WHERE symbol = ? AND until IS NULL'),
    all:   db.prepare('SELECT symbol, since, until FROM paper_price_disputes'),
  };
  /* MEMORY holds only intervals young enough to matter. The DB keeps every
     interval forever for audit, but a boundary can only be settled while its
     observations exist (_markHist holds 180s) plus operator-retry slack, so
     intervals older than a day cannot change any answer, PROVIDED strict
     callers keep default maxAgeMs and marks keep wall-clock stamps: the
     invariant for old instants is defended by staleness, not by this ledger.
     Do not add a strict caller with a large maxAgeMs or backdated recordMark
     without re-reading this. Loading the full
     lifetime made disputeClose (which runs on EVERY accepted tick) and every
     strict lookup walk an array that grew across campaigns without bound. */
  const memCut = Date.now() - 24 * 3600_000;
  for (const r of _disputeStore.all.all()) {
    if (r.until !== null && r.until < memCut) continue;
    if (!_disputes.has(r.symbol)) _disputes.set(r.symbol, []);
    _disputes.get(r.symbol).push({ from: r.since, to: r.until });
  }
  return _disputeStore;
}
function disputeOpen(sym, since, from, to) {
  const arr = _disputes.get(sym) || [];
  if (arr.some((d) => d.to === null)) return;          // already open, one interval
  arr.push({ from: since, to: null }); _disputes.set(sym, arr);
  try { disputeStore().open.run(sym, since, Number(from) || null, Number(to) || null); } catch (e) { _log(`dispute open not persisted for ${sym}: ${e.message}`); }
}
function disputeClose(sym, until) {
  const arr = _disputes.get(sym) || [];
  /* Fast path: no open interval means nothing to do, and this runs on every
     accepted tick. */
  if (!arr.some((d) => d.to === null)) return;
  for (const d of arr) if (d.to === null) d.to = until;
  try { disputeStore().close.run(until, sym); } catch (e) { _log(`dispute close not persisted for ${sym}: ${e.message}`); }
}
try { disputeStore(); } catch (e) { console.error('[paper] dispute ledger init failed:', e.message); }

/* Reconcile the ledger with the live dispute flag, whoever set it.
 * The clamp path calls disputeOpen/Close directly, but the flag can also be
 * set by recovery paths and by tests, and an interval that was never recorded
 * is an interval history forgets. Closing is conservative: we close at the
 * moment we NOTICE, which can only widen the disputed window, never narrow it. */
function syncDisputeLedger(sym, now = Date.now()) {
  const live = _confirming.get(sym);
  const arr = _disputes.get(sym) || [];
  const open = arr.find((d) => d.to === null);
  if (live && !open) disputeOpen(sym, Number.isFinite(live.since) ? live.since : now, live.from, live.to);
  else if (!live && open) disputeClose(sym, now);
}
/* Would this instant be settled on a mark taken BEFORE a dispute that already
 * covered it?
 *
 * Keying on the dispute's close time made the verdict depend on wall clock:
 * an interval closed before the instant it was meant to cover stopped
 * covering it, and the answer flipped back. This depends only on recorded
 * facts, the dispute's start and the observation's own timestamp, so it gives
 * the same answer forever:
 *
 *   the instant is at or after a dispute began, AND the newest observation at
 *   or before it predates that dispute
 *     -> we would be handing out the pre-dispute mark for a moment we did not
 *        know the level. Refuse, permanently.
 *
 * Once real accepted data exists inside or after the dispute, the instant is
 * settled on that instead and this stops applying, which is the recovery case. */
function instantDisputed(sym, ts, obsT) {
  const arr = _disputes.get(sym);
  if (!arr) return false;
  for (const d of arr) {
    if (ts >= d.from && (obsT === undefined || obsT < d.from)) return true;
  }
  return false;
}

/* Reconcile non-clamp live failure flags into the durable invalidity ledger.
   Production transitions call notifyPriceabilityLost immediately; this
   defensive read closes restore/manual-state gaps and makes strict lookup's
   answer depend on the failure interval, never merely on callback ordering. */
function syncCurrentInvalidity(sym, now = Date.now()) {
  const m = live.map.get(sym);
  const starts = [];
  if (m && m.indexHalt) {
    const st = _halt.get(sym);
    starts.push(st && (st.haltedAt || st.divergeSince));
  }
  if (_riskBlocked.has(sym)) starts.push(_riskBlockedSince.get(sym));
  if (_srcUnsafe.has(sym)) starts.push(_srcUnsafe.get(sym));
  const known = starts.map(Number).filter(Number.isFinite);
  if (known.length) disputeOpen(sym, Math.min(...known), null, null);
}

const _markHist = new Map();     // sym -> [{t, p, n}] ascending, n = component count
/** Fresh components behind a symbol's index at time t. */
function componentCount(sym, t = Date.now()) {
  const c = _idxComps.get(sym);
  if (!c) return 0;
  return Object.values(c).filter((x) => t - x.ts < COMP_FRESH_MS).length;
}
function recordMark(sym, px, t, comps = null, spreadBps = null, srcKeyIn = null, srcTsIn = null, admission = null) {
  if (!(Number(px) > 0)) return;
  let a = _markHist.get(sym);
  if (!a) { a = []; _markHist.set(sym, a); }
  /* Record the DECISION, not just its inputs.
   *
   * The count stored here used to be the FRESH component count, while the live
   * gate counted fresh-plus-corroborating. A market could therefore be admitted
   * for trading and then have that same instant refused by the checkpoint that
   * decides the result, blocking a round on a price the engine had already
   * accepted. Live and strict now read one recorded verdict. */
  const dq = compQuality(sym, t);
  /* Provenance travels with the price. A bare {t, p} could not tell a
     two-source observation from a lone survivor, so a one-component tick was
     admissible as a "strict" boundary mark. */
  const n = Number.isFinite(comps) ? comps : dq.n;
  /* Store HOW WELL the sources agreed, not only how many there were. */
  const sp = Number.isFinite(spreadBps) ? spreadBps : dq.spreadBps;
  /* The verdict is the LIVE decision AND this observation's own declared
     provenance. Taking only the live decision would let a caller that
     explicitly reports a lone-source tick inherit a healthy market's verdict;
     taking only the provenance is what let live and strict disagree in the
     first place. Both must hold. */
  const accepted = admission ? admission.accepted === true : compPriceReady(sym, t);
  /* WHICH source produced this observation is the audit trail now: a
     liquidation has to be explainable as "this came from <venue> at <time>,
     because the primary had been silent for <n>ms". */
  const act = activeSource(sym, t);
  /* An observation carries its own EXPIRY, not just its birthday.
   *
   * Strict lookups used to accept any stored observation inside a generic
   * 30s window, so a checkpoint six seconds after every source died still
   * settled on the dead price: the live engine knew the market was gone and
   * the settlement path did not. A verdict that was valid at t is not
   * automatically valid at t+6s, so the source's own deadline travels with
   * the observation and the boundary must fall inside it. */
  const srcKey = srcKeyIn || (act ? act.key : null);
  /* Expiry is measured from when the SOURCE spoke, not from when we happened
     to write the row down. */
  const srcTs = Number.isFinite(srcTsIn) ? srcTsIn : t;
  const leverageCap = accepted ? (admission ? admission.leverageCap : boostLevCap(sym, HEAT_MAX_LEV, t)) : 0;
  const row = { t, p: px, n, sp, ok: accepted, f: dq.fresh, v: dq.venues,
    boostOk: leverageCap >= HEAT_MAX_LEV, leverageCap,
    age: dq.maxAgeMs, src: srcKey, srcTs,
    validUntil: srcTs + (srcKey ? staleMsForSym(sym, srcKey) : COMP_FRESH_MS) };
  /* HISTORY STAYS SORTED. A blind push trusted the caller's clock, and one
     backwards-stamped tick (NTP step, a source replay) landed at the ARRAY
     TAIL: markAt scans newest-first assuming ascending time, so the misplaced
     entry shadowed the true newest observation for every later boundary until
     fresh ticks buried it — a settlement price that did not exist at its
     instant. Out-of-order arrivals are inserted where they belong; near-tail
     in practice, so this walk is O(1) amortised. */
  if (!a.length || a[a.length - 1].t <= t) a.push(row);
  else {
    let i = a.length - 1;
    while (i > 0 && a[i - 1].t > t) i--;
    a.splice(i, 0, row);
  }
  if (a.length > MARK_HIST_MAX) a.splice(0, a.length - MARK_HIST_MAX);
  const cut = t - MARK_HIST_MS;
  while (a.length && a[0].t < cut) a.shift();
  /* Only an actual committed live observation feeds diagnostics. Historical
     boundary fixtures/replays and transient risk probes are not live events. */
  const current = live.map.get(sym);
  if (current && current.pythAtMs === t && Number(current.acceptedSeq) > 0) {
    observeMarketDiagnostic(sym, t, accepted, row.boostOk,
      { reason: accepted ? 'ready' : marketDiagnosticFailure(sym, t), source: act && act.key },
      current.acceptedSeq);
  }
}
/** Last mark at or before `ts`. Falls back to the current mark when the
 *  history does not reach back that far (a fresh process, say), which is the
 *  old behaviour and is stated rather than hidden. */
/** Last recorded mark at or before `ts`, with the age of that observation.
 *  `strict` refuses to substitute a current price: a published result must be
 *  priced from a mark that actually existed at the boundary, or not at all. */
/* The newest ACCEPTED observation strictly before `ts`, with its own
 * timestamp, for the operator settle-at-prior action. Unlike markAt this does
 * not apply the dispute or staleness refusals: the whole point is that the
 * exact instant is un-priceable and the operator is deliberately, auditably
 * choosing the last price the engine ever accepted before it. */
function markBefore(sym, ts) {
  const a = _markHist.get(sym);
  if (!a) return null;
  for (let i = a.length - 1; i >= 0; i--) {
    if (a[i].t < ts && a[i].ok) return { px: a[i].p, t: a[i].t };
  }
  return null;
}

function markAt(sym, ts, { strict = false, maxAgeMs = MARK_MAX_AGE_MS, forLeverage = 0, round = null } = {}) {
  if (holdsRoundPrices(round)) {
    // Historical checkpoints never borrow the current tick's transaction token.
    return historicalRoundMark(sym, round, ts, forLeverage)?.price ?? null;
  }
  /* HISTORY IS NOT REWRITABLE.
   *
   * This asked whether the symbol is in dispute RIGHT NOW, then answered a
   * question about a PAST instant. So one boundary gave three different
   * answers depending only on when it was asked: valid, then null while a
   * later jump confirmed, then valid again on retry. A settled result could
   * change its mind, which is the oldest finding against this engine.
   *
   * The observation already carries the verdict it was accepted under (`ok`,
   * `validUntil`, `boostOk`), recorded at acceptance and never revised. A
   * dispute about a LATER price cannot retroactively unmake a price that was
   * valid when it was taken, and it does not need to: while a symbol is
   * confirming, compPriceReady is false, so every observation recorded during
   * the dispute is stored with ok:false and strict refuses it on its own
   * evidence.
   *
   * But the live check was NOT redundant, and deleting it outright reopened a
   * measured exploit: while a jump confirms, the last pre-jump observation is
   * still young and still valid, so pricing at `now` handed out the stale mark
   * and a trader could collect the entire withheld step once it confirmed
   * (600% of committed margin at 1000x). The integrity suite caught it.
   *
   * So the dispute is scoped to the interval it actually covers. A dispute
   * that began at `since` refuses instants at or after `since`, and says
   * nothing about instants before it, which are settled on their own recorded
   * evidence and can never be revised. That keeps the past immutable AND
   * keeps the stale mark unreachable. */
  /* A boundary that fell inside a recorded dispute window is refused FOREVER,
     not just while the dispute happens to be open. This is the half that makes
     the answer permanent: before, the interval was forgotten on resolution and
     the same instant started settling again. */
  if (strict) {
    syncDisputeLedger(sym);
    syncCurrentInvalidity(sym);
  }
  if (strict && _confirming.has(sym)) {
    /* Scoping by the dispute's `since` was a millisecond too permissive: the
       jump tick and the query can land in the same millisecond. What the
       dispute is really about is the NEWEST accepted observation, because that
       is the stale mark a trader would be handed. So refuse any instant that
       would be priced off it, and allow instants that already have a later
       accepted observation standing behind them, which is every genuinely
       settled boundary in the past. */
    const arr = _markHist.get(sym) || [];
    let newestOkT = null;
    for (let i = arr.length - 1; i >= 0; i--) { if (arr[i].ok) { newestOkT = arr[i].t; break; } }
    const disputeSince = Number(_confirming.get(sym).since);
    if ((!Number.isFinite(disputeSince) || ts >= disputeSince)
        && (newestOkT === null || ts >= newestOkT)) return null;
  }
  const a = _markHist.get(sym);
  if (a && a.length) {
    for (let i = a.length - 1; i >= 0; i--) {
      if (a[i].t <= ts) {
        if (strict) {
          /* The observation we are about to price from predates a dispute that
             already covered this instant: that is the stale pre-jump mark. */
          if (instantDisputed(sym, ts, a[i].t)) return null;
          if (ts - a[i].t > maxAgeMs) return null;                    // too stale to stand behind
          /* One verdict, recorded at acceptance. Re-deriving it here from the
             stored inputs is what let live and strict drift apart. */
          if ('ok' in a[i]) {
            if (!a[i].ok) return null;
            /* The boundary has to fall inside the window this observation was
               actually good for, judged by the source that produced it. */
            /* Live readiness uses `age < deadline`, so at the deadline the
               price is already refused there. Strict uses the same convention
               or the two disagree at exactly the expiry instant. */
            if (Number.isFinite(a[i].validUntil) && ts >= a[i].validUntil) return null;
            if (Number.isFinite(a[i].leverageCap)) {
              if (forLeverage > a[i].leverageCap) return null;
            } else if (forLeverage > BOOST_LEV_THRESHOLD && !a[i].boostOk) return null;
          } else {
            // observations recorded before the verdict was stored
            if ((a[i].n || 0) < COMP_MIN_COMPONENTS) return null;
            const sp = a[i].sp;
            if (!(Number.isFinite(sp) ? sp <= COMP_MAX_SPREAD_BPS : false)) return null;
          }
        }
        return a[i].p;
      }
    }
  }
  return strict ? null : markOfFreshFor(sym, true);
}
const MARK_MAX_AGE_MS = Number(process.env.PAPER_MARK_MAX_AGE_MS || 30_000);
/** The canonical mark set for every symbol a roster is exposed to, as of
 *  `ts`. One map, used for every player in a checkpoint, so two traders can
 *  never be settled at different prices for the same instant. */
function markSetAt(symbols, ts, { strict = false, levBySymbol = null, round = null } = {}) {
  const out = {};
  const missing = [];
  for (const sym of new Set(symbols)) {
    const forLeverage = levBySymbol ? (levBySymbol.get(sym) || 0) : 0;
    const px = markAt(baseOf(sym), ts, { strict, forLeverage, round });
    if (Number.isFinite(px) && px > 0) out[sym] = holdsRoundPrices(round) ? px : rpx(px);
    else missing.push(sym);
  }
  /* Every position a checkpoint prices must have a mark that existed at the
     boundary. Omitting one used to leave posMarkOf falling back to the current
     price, last_mark or the entry — so the stored mark set claimed to explain
     a result it had not produced. */
  if (strict && missing.length) {
    const e = new Error(`no boundary mark for ${missing.join(', ')} at ${new Date(ts).toISOString()}`);
    e.unpriced = true;
    if (missing.length === 1) e.symbol = missing[0];
    const unavailable = missing.map((sym) => roundPriceAvailability(sym,
      levBySymbol ? (levBySymbol.get(sym) || 0) : 0, Date.now(), round))
      .filter((x) => !x.ready && Number.isFinite(Number(x.invalidSince)));
    if (unavailable.length) e.invalidSince = Math.min(...unavailable.map((x) => Number(x.invalidSince)));
    throw e;
  }
  return out;
}

/* Every symbol the roster is exposed to, priced as of the boundary's
   scheduled instant. One set, shared by every player in the checkpoint. */
function markSetFor(userIds, ts, opts = {}) {
  /* Carry each position's LEVERAGE into settlement.
   *
   * markAt has understood `forLeverage` since round seven, but nothing passed
   * it here, so a final checkpoint could settle a 1000x Boost position on a
   * mark that was only ever valid for ordinary exposure: strict said null at
   * 1000x, the checkpoint asked without the leverage, got a price, and stored
   * it as the number that decided the round. */
  const need = new Map();   // symbol -> highest leverage carried on it
  for (const uid of userIds) {
    for (const p of stmt.posByUser.all(uid)) {
      const lev = Number(p.leverage) || 0;
      need.set(p.symbol, Math.max(need.get(p.symbol) || 0, lev));
    }
  }
  const round = opts.round || comp.currentRound();
  return markSetAt([...need.keys()], ts, { ...opts, round, levBySymbol: need });
}

/* Raised when a competition-owned position cannot be priced. Distinct from an
   ordinary error so callers can treat "we do not know" differently from "this
   blew up". */
class UnpricedPosition extends Error {
  constructor(symbol, leverage = 0) {
    super(`no competition-valid mark for ${symbol}`);
    this.symbol = symbol;
    this.unpriced = true;
    const status = marketAvailability(baseOf(symbol), leverage, Date.now());
    if (Number.isFinite(Number(status.invalidSince))) this.invalidSince = Number(status.invalidSince);
  }
}

function posMarkOf(pos, marks = null, { liveLeverage = false } = {}) {
  // ALWAYS pass the base symbol: without it stageMark skipped its own
  // readiness check, so a one-source price could still liquidate a position
  const competitor = competingNow(pos.user_id);
  /* Existing exposure obeys the same quality tier as entry/close. This is
     what freezes live score, drawdown and cross liquidation together on an
     unageable backup instead of letting the lower-tier mark move some paths. */
  if (competitor && (!marks || liveLeverage)
      && !roundPriceReady(baseOf(pos.symbol), Number(pos.leverage) || 0)) {
    throw new UnpricedPosition(pos.symbol, Number(pos.leverage) || 0);
  }
  if (marks) {
    const m = Number(marks[pos.symbol]);
    if (m > 0) return m;
    /* A mark set was supplied and this symbol is not in it. Falling through
       to a live price here is exactly how a checkpoint ends up mixing two
       instants, so refuse instead. */
    throw new Error(`no boundary mark for ${pos.symbol}`);
  }
  const m = mkt(pos.symbol);
  const px = markFor(m, isStage(heatOf(pos.user_id)), baseOf(pos.symbol), competitor);
  if (Number.isFinite(px) && px > 0) return px;
  /* FOR A COMPETITOR, THERE IS NO FALLBACK.
   *
   * Falling back to last_mark or entry meant freezing a symbol did not freeze
   * the account: a stale profitable leg still counted as cross collateral, so
   * a player could open elsewhere on money the engine could not currently
   * price, and cross liquidation could run with one leg unknown. A scored
   * account either has a competition-valid mark for every position or it has
   * no risk answer at all. */
  if (competitor) throw new UnpricedPosition(pos.symbol, Number(pos.leverage) || 0);
  return Number(pos.last_mark) || pos.entry_price;
}

// Cross positions share account equity; isolated positions live on their own
// allocated margin (already moved out of balance) and are excluded from the
// cross risk numbers. equityTotal is what the leaderboard/UI shows.
function accountRisk(userId, acct, { excludeOrderId = null, marks = null, liveLeverage = false } = {}) {
  const positions = stmt.posByUser.all(userId);
  const orders = stmt.ordOpenByUser.all(userId).filter((o) => o.id !== excludeOrderId);
  let crossUpnl = 0, posMargin = 0, maint = 0, cancelTier = 0, isoValue = 0;
  for (const p of positions) {
    const mk = posMarkOf(p, marks, { liveLeverage });
    if (isIso(p)) {
      /* An isolated leg can never be worth less than zero: every CLOSE path
         caps the loss at the allocated margin, and the bell settles by
         MARKING, not flattening. Without this floor a -BOOST leg that gapped
         past bankruptcy but had not yet been liquidated scored below its own
         floor, so two identical seats diverged on whether the liquidation
         sweep fired one tick before the bell. */
      isoValue += isolatedSettlementValue(p.isolated_margin, uPnl(p, mk));
      continue;
    }
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
    return Number.isFinite(m) && m > 0 ? rpx(m) : null;
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
  return Number.isFinite(m) && m > 0 ? rpx(m) : null;
}

// ── core fill executor ───────────────────────────────────────────────────
// Applies one fill (open/increase/reduce/flip) to position + account rows and
// records it. Margin CHECKS are the caller's job; margin MOVEMENT for
// isolated positions happens here (balance ⇄ isolated_margin). The isolated
// guarantee holds on EVERY close path (SL/TP/DELIST/liquidation/manual): a
// negative close credit is floored at 0 — loss never exceeds the allocated
// margin, the excess is absorbed as bad debt, Phoenix-style.
/* The legacy per-position Boost clock belongs to the PUBLIC paper product: a
   >100x fill there starts a 2:00 timer and auto-settles. It must never touch
   a competition player, for two reasons. The show's Boost is a shared phase
   controlled by the round clock, so a second, per-position timer would settle
   positions mid-window; and the opt-out is a client-supplied flag, which
   would let two otherwise identical competitors be treated differently based
   on what their browser sent. Inside a round the server decides, and the
   answer is always "no legacy clock". */
function armsLegacyBoost(userId, symbol, lev, boostWindow, acct) {
  if (lev < BOOST_ARM_LEV || !isStage(acct.heat)) return false;
  if (aliasKind(symbol)) return false;              // event tickers are phase-gated
  if (comp.accountLocked(userId)) return false;     // seated competitor
  return boostWindow;
}

const FILL_DECISION_CONTEXT_MAX_BYTES = 12 * 1024;
function auditNumber(v) {
  if (v == null || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? r6(n) : null;
}
function auditRiskSnapshot(userId, acct) {
  const a = acct || stmt.acctGet.get(userId);
  if (!a) return null;
  try {
    const r = accountRisk(userId, a);
    const isoMargin = r.positions.filter(isIso).reduce((s, p) => s + (Number(p.isolated_margin) || 0), 0);
    return {
      balance: auditNumber(a.balance), equity: auditNumber(r.equityTotal),
      equityCross: auditNumber(r.equityCross), free: auditNumber(r.free),
      maintenance: auditNumber(r.maint), marginUsed: auditNumber(r.equityCross - r.free),
      isolatedMargin: auditNumber(isoMargin), positions: r.positions.length,
      openOrders: r.orders.length,
    };
  } catch {
    // A fill can be closing the one leg that made the wider portfolio
    // unpriceable. Preserve the known cash state without inventing a risk
    // number from a stale mark.
    return { balance: auditNumber(a.balance), riskUnavailable: true };
  }
}
function auditBookSnapshot(symbol) {
  const b = freshBook(symbol);
  if (!b) return null;
  const clean = (levels, desc) => (levels || [])
    .map((x) => [auditNumber(x && x[0]), auditNumber(x && x[1])])
    .filter((x) => x[0] > 0 && x[1] > 0)
    .sort((x, y) => desc ? y[0] - x[0] : x[0] - y[0])
    .slice(0, 10);
  const bids = clean(b.bids, true), asks = clean(b.asks, false);
  const canonical = JSON.stringify({ bids, asks });
  return {
    observedAt: Number(b.ts) || null,
    ageMs: Number(b.ts) > 0 ? Math.max(0, Date.now() - Number(b.ts)) : null,
    bestBid: bids[0] || null, bestAsk: asks[0] || null,
    depthLevelsHashed: bids.length + asks.length,
    sha256: crypto.createHash('sha256').update(canonical).digest('hex'),
  };
}
function auditIndexSnapshot(symbol, now = Date.now()) {
  const base = baseOf(symbol);
  const c = _idxComps.get(base);
  if (!c) return null;
  const components = Object.entries(c).slice(0, 8).map(([source, v]) => ({
    source: String(source).slice(0, 24), venue: venueOf(source),
    price: auditNumber(v && v.px), observedAt: Number(v && (v.srcAt || v.ts)) || null,
    ageMs: v ? auditNumber(compAgeMs(v, now)) : null,
  }));
  let followed = null;
  const followedKey = _activeSrc.get(base);
  const followedValue = followedKey && c[followedKey];
  if (followedValue) followed = {
    source: followedKey, price: auditNumber(followedValue.px),
    observedAt: auditNumber(followedValue.srcAt || followedValue.ts),
    ageMs: auditNumber(compAgeMs(followedValue, now)),
  };
  return { base, followed, componentCount: components.length, components };
}
function sanitizeAuditValue(v, depth = 0) {
  if (v == null || typeof v === 'boolean') return v;
  if (typeof v === 'number') return auditNumber(v);
  if (typeof v === 'string') return v.slice(0, 160);
  if (depth >= 4) return '[bounded]';
  if (Array.isArray(v)) return v.slice(0, 20).map((x) => sanitizeAuditValue(x, depth + 1));
  if (typeof v !== 'object') return null;
  const out = {};
  for (const k of Object.keys(v).slice(0, 32)) out[String(k).slice(0, 64)] = sanitizeAuditValue(v[k], depth + 1);
  return out;
}
function defaultDecisionReason(kind) {
  return ({
    LIMIT: 'resting-limit-cross', MARKET: 'immediate-order', SL: 'stop-loss-trigger',
    TP: 'take-profit-trigger', EXPIRY: 'boost-window-expired',
    LIQUIDATION: 'maintenance-threshold', SEGMENT: 'segment-boundary',
    DELIST: 'market-delist-timeout',
  })[kind] || String(kind || 'fill').toLowerCase();
}
function serializeDecisionContext({ userId, symbol, kind, source, referenceMark, indexSeq, before, after, reason, context, roundExecution = null, at }) {
  const obj = {
    version: 1,
    buildId: PAPER_BUILD_ID,
    engineBoot: ENGINE_BOOT_ID,
    decidedAt: Number(at) || Date.now(),
    reason: String(reason || defaultDecisionReason(kind)).slice(0, 96),
    execution: { source, referenceMark: auditNumber(referenceMark), indexSeq },
    index: auditIndexSnapshot(symbol, Number(at) || Date.now()),
    book: auditBookSnapshot(symbol),
    risk: { before, after },
    inputs: sanitizeAuditValue(context || {}),
  };
  // This closed record comes from the engine's execution resolver, not the
  // caller extension. Its exact price/provenance must survive audit display
  // rounding, including sub-micro prices and fractional source timestamps.
  if (roundExecution) obj.inputs.roundExecution = roundExecution;
  let raw;
  try { raw = JSON.stringify(obj); } catch { raw = ''; }
  if (Buffer.byteLength(raw) <= FILL_DECISION_CONTEXT_MAX_BYTES) return raw;
  // The fixed core is the replay-critical portion. Only the caller extension
  // can be unexpectedly large; replace it explicitly rather than truncating
  // JSON into an unreadable value.
  obj.inputs = { bounded: true, ...(roundExecution ? { roundExecution } : {}) };
  raw = JSON.stringify(obj);
  return Buffer.byteLength(raw) <= FILL_DECISION_CONTEXT_MAX_BYTES ? raw : JSON.stringify({
    version: 1, buildId: PAPER_BUILD_ID, engineBoot: ENGINE_BOOT_ID,
    decidedAt: obj.decidedAt, reason: obj.reason, execution: obj.execution,
    risk: obj.risk, ...(roundExecution ? { roundExecution } : {}), bounded: true,
  });
}

/* One logical fill, one indivisible database action.
 *
 * This mutates a position, isolated margin, the account balance and counters,
 * and the fill ledger. It was a bare sequence of statements, so a failure at
 * the last step left an account with a live position and a debited balance and
 * NO audit row: the trade had happened financially and did not exist on the
 * record. Wrapped as a transaction it either all lands or none of it does.
 *
 * better-sqlite3 turns a nested transaction into a SAVEPOINT, so callers that
 * are already inside one (the sweep, a boundary settlement, the order path
 * below) compose correctly and still get all-or-nothing for the whole action.
 */
function applyFillUnsafe(userId, { symbol, orderSide, size, px, feeBps, kind, orderId = null, leverage = null, marginMode = 'cross', boostWindow = true, at = null, executionSource = null, referenceMark = null, indexSeq = null, decisionReason = null, decisionContext = null }) {
  /* `at` lets a boundary-generated fill carry the instant it belongs to
     rather than the instant it was computed. Ordinary trading passes nothing
     and keeps wall-clock behaviour. */
  const now = Number.isFinite(at) ? at : Date.now();
  if (_roundRecoveryBatch && db.inTransaction && _roundRecoveryBatch.users.has(userId)
      && aliasKind(symbol) && !recoveryAliasAllowed(symbol, userId, _roundRecoveryBatch)) {
    throw recoveryUnavailable('fill alias is not a committed open Boost');
  }
  const acct = ensureAccount(userId);
  const boostCapacity = boostCapacityCheck(userId, symbol, orderSide, size, px, {
    excludeOrderId: orderId,
    /* A segment-generated opposite fill never increases exposure. Bell
       recovery may intentionally be using its frozen checkpoint mark after
       live feeds disappeared; do not ask current accountRisk for a price the
       trusted full close neither needs nor creates. */
    trustedSettlementReduction: kind === 'SEGMENT'
      && decisionReason === 'segment-boundary-flatten',
  });
  if (!boostCapacity.ok) {
    const e = new Error(boostCapacity.error);
    e.code = boostCapacity.error;
    e.capacity = boostCapacity;
    throw e;
  }
  const riskBefore = auditRiskSnapshot(userId, acct);
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
    if (armsLegacyBoost(userId, symbol, leverage || 1, boostWindow, acct)) stmt.posBoostStamp.run(now, userId, symbol);
  } else if (dirOf(pos.side) === d) {
    const newSize = r6(pos.size + size);
    const entry = rpx((pos.size * pos.entry_price + size * px) / newSize);
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
    if (armsLegacyBoost(userId, symbol, addLev, boostWindow, acct)) stmt.posBoostStamp.run(now, userId, symbol);
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
  const src = executionSource == null ? null : String(executionSource).slice(0, 40);
  const ref = Number(referenceMark) > 0 ? Number(referenceMark) : null;
  const seq = resolveIndexSeq(indexSeq);
  const acctAfter = stmt.acctGet.get(userId);
  const riskAfter = auditRiskSnapshot(userId, acctAfter);
  const executionRound = pricingRoundFor(userId);
  const historicalSettlement = !!(executionRound && kind === 'SEGMENT'
    && decisionReason === 'segment-boundary-flatten' && Number.isFinite(at));
  const executionMark = executionRound
    ? historicalSettlement
      ? historicalRoundMark(symbol, executionRound, now, Number(leverage || pos?.leverage) || 0)
      : roundExecutionMark(symbol, { round: executionRound, at: now }) : null;
  if (historicalSettlement && (!executionMark || executionMark.price !== px || executionMark.price !== ref)) {
    throw new Error('historical segment fill does not match its committed round mark');
  }
  const audit = serializeDecisionContext({
    userId, symbol, kind, source: src, referenceMark: ref, indexSeq: seq,
    before: riskBefore, after: riskAfter, reason: decisionReason,
    context: decisionContext,
    roundExecution: executionMark ? {
      policy: ROUND_PRICE_POLICY, roundId: executionRound.id,
      base: executionMark.base, price: executionMark.price,
      acceptedBoot: executionMark.acceptedBoot, acceptedSeq: executionMark.acceptedSeq,
      acceptedAt: executionMark.acceptedAt, appliedAt: executionMark.appliedAt,
      observedAt: executionMark.observedAt, originalValidUntil: executionMark.originalValidUntil,
      source: executionMark.source, acceptedLeverageCap: executionMark.acceptedLeverageCap,
      ...comp.roundMarkPolicyFields(executionMark),
      ...comp.backupExecutionPolicyFields(executionRound),
    } : null, at: now,
  });
  const info = stmt.fillInsAudited.run(
    userId, acct.epoch, symbol, orderSide, kind, px, size, notional, fee,
    realized, orderId, now, badDebt, src, ref, ENGINE_BOOT_ID, seq, audit,
  );
  return {
    id: Number(info.lastInsertRowid), symbol, side: orderSide, kind, price: px,
    size, notional, fee, realizedPnl: realized, badDebt, ts: now,
    executionSource: src, referenceMark: ref, engineBoot: ENGINE_BOOT_ID, indexSeq: seq,
    decisionContext: JSON.parse(audit),
  };
}

function resolveIndexSeq(raw) {
  /* null and blank are absence, not event zero. Never borrow the previous
     frame for a fill caused by the current tick: the current delivery id is
     assigned only after risk completes, so stale provenance is worse than
     an honest null. Callers may supply an exact positive event id. */
  let supplied = NaN;
  if (typeof raw === 'number') supplied = raw;
  else if (typeof raw === 'string' && /^[1-9]\d*$/.test(raw.trim())) supplied = Number(raw.trim());
  if (Number.isSafeInteger(supplied) && supplied > 0) return supplied;
  return null;
}
const _applyFillTx = db.transaction(applyFillUnsafe);
/* Every fill, stop, take-profit and liquidation passes through here. The
   count is how the relay knows a tick MATTERED: an event that moved somebody's
   position is published whatever the cadence gate says, because thinning the
   exact tick that liquidated a trader is how a chart ends up unable to explain
   a result. */
const _matPending = new Map();
function queueMaterialFill(userId, fill) {
  if (!fill || !Number.isSafeInteger(Number(fill.id)) || Number(fill.id) <= 0) return;
  const base = baseOf(fill.symbol);
  let rows = _matPending.get(base);
  if (!rows) { rows = new Map(); _matPending.set(base, rows); }
  /* Replaces a ghost if SQLite reused an id after an outer rollback. */
  rows.set(Number(fill.id), {
    fillId: Number(fill.id), userId: Number(userId), symbol: String(fill.symbol),
    executionPrice: Number(fill.price), referenceMark: Number(fill.referenceMark),
    at: Number(fill.ts),
  });
  /* A market can close at most the 32-seat roster in one synchronous pass.
     Bound defensive state without changing the public cause queue. */
  while (rows.size > 64) rows.delete(rows.keys().next().value);
}
function consumeMaterialFill(base) {
  const rows = _matPending.get(base);
  _matPending.delete(base);
  if (!rows) return false;
  for (const pending of rows.values()) {
    const row = _compFillCauseRow.get(pending.fillId);
    if (row
        && Number(row.user_id) === pending.userId
        && String(row.symbol) === pending.symbol
        && Number(row.price) === pending.executionPrice
        && Number(row.reference_mark) === pending.referenceMark
        && Number(row.ts) === pending.at) return true;
  }
  return false;
}
function applyFill(userId, opts) {
  /* Mark only after the inner mutation succeeds, then verify the durable row
     before publication. The inner transaction may be a SAVEPOINT owned by a
     wider risk pass, so return alone is not proof the outer transaction kept
     the fill. */
  const r = _applyFillTx(userId, opts);
  try {
    const sym = baseOf(String(opts && opts.symbol) || '');
    queueMaterialFill(userId, r);
    /* A committed fill changes score even if the next market observation is
       cadence-thinned (and a close may remove the symbol from the exposure
       set). Force one coalesced snapshot after the transaction. */
    if (competingNow(userId)) {
      _compRankSymbols.add(sym);
      /* Keep the immutable fill basis, not merely the symbol. The live mark
         may move (or expire) before the board coalescer runs, and a fully
         closed leg no longer appears in the exposure scan. The database id
         remains internal and is revalidated at flush because this function
         can run inside a larger transaction that later rolls back. */
      const referenceMark = Number(r && r.referenceMark);
      const executionPrice = Number(r && r.price);
      const at = Number(r && r.ts);
      if (Number.isSafeInteger(Number(r && r.id)) && Number(r.id) > 0
          && referenceMark > 0 && executionPrice > 0
          && Number.isSafeInteger(at) && at > 0) {
        /* A rolled-back outer transaction may give its id to a later real
           fill. Replace that ghost now so identical retry contents cannot be
           counted twice merely because the id was reused. */
        const reused = _compPendingCauses.findIndex((x) => x.fillId === Number(r.id));
        if (reused >= 0) _compPendingCauses.splice(reused, 1);
        if (_compPendingCauses.length < COMP_CAUSE_EVENT_MAX) {
          _compPendingCauses.push({
            fillId: Number(r.id), userId: Number(userId), symbol: String(r.symbol),
            referenceMark, executionPrice, at,
          });
        } else {
          _compCauseOverflow = true;
        }
      }
      _compDirtySymbols.add(sym);
      scheduleCompBoard(sym, true);
    }
  } catch { /* standings must never block a committed fill */ }
  return r;
}
/** Run a multi-step logical action as one transaction (SAVEPOINT if nested). */
function atomically(fn) { return db.transaction(fn)(); }

// ── competition scoring ──────────────────────────────────────────────────
/* One player's numbers for a round snapshot. Everything is measured against
 * the CURRENT epoch, which acctReset bumps at the start of every round, so
 * "this round" needs no timestamp arithmetic and cannot accidentally sweep in
 * a previous heat.
 *
 * accountPnl is equity against the starting balance, isolated positions
 * included, which is exactly what the trader sees. `hotTicker` is retained
 * only for historical v1 snapshots. Current v2 scoring asks hotValueOf for
 * the ordinary asset at each exact boundary and adds one copy of that
 * interval delta in competition.js. */
/* Bounded by the boundary instant. advanceRoundClock should mean nothing
   later exists yet, but a timer-fired boundary or a clock skew must not be
   able to pull a post-boundary fill into an earlier checkpoint. Belt and
   braces, cheaply. */
const compRealized = db.prepare(
  "SELECT COALESCE(SUM(realized_pnl + COALESCE(bad_debt, 0)), 0) AS v FROM paper_fills WHERE user_id = ? AND epoch = ? AND ts <= ? AND realized_pnl IS NOT NULL"
);
const compHot = db.prepare(
  "SELECT COALESCE(SUM(realized_pnl + COALESCE(bad_debt, 0)), 0) AS v FROM paper_fills WHERE user_id = ? AND epoch = ? AND symbol = ? AND ts <= ? AND realized_pnl IS NOT NULL"
);
const compHotFamily = db.prepare(
  `SELECT COALESCE(SUM(realized_pnl + COALESCE(bad_debt, 0)), 0) AS v
   FROM paper_fills
   WHERE user_id = ? AND epoch = ? AND symbol IN (?, ?, ?) AND ts <= ?
     AND realized_pnl IS NOT NULL`
);
const compProofFills = db.prepare(`SELECT id, user_id, epoch, symbol, side, kind,
  price, size, notional, fee, realized_pnl, bad_debt, order_id, ts,
  execution_source, reference_mark, engine_boot, index_seq, decision_context
  FROM paper_fills WHERE user_id = ? AND epoch = ?
    AND (ts < ? OR (ts = ? AND id <= ?)) ORDER BY id`);
const compProofFillWatermark = db.prepare(`SELECT COALESCE(MAX(id), 0) AS id
  FROM paper_fills WHERE user_id = ? AND epoch = ? AND ts <= ?`);

function fillLedgerEvidence(userId, epoch, asOf, watermarkId = null) {
  if (watermarkId != null && !(Number.isSafeInteger(Number(watermarkId))
      && Number(watermarkId) >= 0)) {
    throw new Error('invalid competition fill watermark');
  }
  const capturedWatermark = watermarkId != null ? Number(watermarkId)
    : Number(compProofFillWatermark.get(userId, epoch, asOf).id) || 0;
  const rows = compProofFills.all(userId, epoch, asOf, asOf, capturedWatermark).map((row) => ({
    id: Number(row.id), userId: Number(row.user_id), epoch: Number(row.epoch),
    symbol: row.symbol, side: row.side, kind: row.kind,
    price: Number(row.price), size: Number(row.size), notional: Number(row.notional),
    fee: Number(row.fee), realizedPnl: row.realized_pnl == null ? null : Number(row.realized_pnl),
    badDebt: Number(row.bad_debt) || 0,
    orderId: row.order_id == null ? null : Number(row.order_id), ts: Number(row.ts),
    executionSource: row.execution_source || null,
    referenceMark: row.reference_mark == null ? null : Number(row.reference_mark),
    engineBoot: row.engine_boot || null,
    indexSeq: row.index_seq == null ? null : Number(row.index_seq),
    decisionContext: row.decision_context || null,
  }));
  const canonical = JSON.stringify(rows);
  const realizedByBase = new Map();
  let realized = 0;
  let realizedBeforeBoundarySettlement = 0;
  for (const row of rows) {
    if (row.realizedPnl == null) continue;
    const v = Number(row.realizedPnl) + Number(row.badDebt || 0);
    realized += v;
    if (!(row.kind === 'SEGMENT' && Number(row.ts) === Number(asOf))) {
      realizedBeforeBoundarySettlement += v;
    }
    const base = baseOf(row.symbol);
    realizedByBase.set(base, (realizedByBase.get(base) || 0) + v);
  }
  return {
    schema: 'paper-fill-proof-v1',
    watermarkId: capturedWatermark,
    sha256: crypto.createHash('sha256').update(canonical, 'utf8').digest('hex'),
    count: rows.length,
    firstId: rows.length ? rows[0].id : null,
    lastId: rows.length ? rows[rows.length - 1].id : null,
    realized: r6(realized),
    realizedBeforeBoundarySettlement: r6(realizedBeforeBoundarySettlement),
    realizedByBase: Object.fromEntries([...realizedByBase.entries()]
      .sort(([a], [b]) => a.localeCompare(b)).map(([base, value]) => [base, r6(value)])),
  };
}

/* Freeze the exact inputs consumed by scoreUser/hotValueOf. This is compact:
   fills remain in their append-only ledger and are represented by a canonical
   digest plus derived realised totals; open state and boundary marks are
   copied because those rows legitimately change after the checkpoint. */
function scoreProofFor(userId, epoch, startBalance, marks, asOf, explicitRound = null) {
  const acct = ensureAccount(userId);
  const ep = Number(epoch);
  const cut = Number(asOf);
  const positions = stmt.posByUser.all(userId)
    .filter((pos) => Number(pos.epoch) === ep)
    .map((pos) => {
      const mark = Number(posMarkOf(pos, marks, { liveLeverage: false }));
      const rawUpnl = Number(uPnl(pos, mark));
      const upnl = r6(rawUpnl);
      const isolated = isIso(pos);
      return {
        symbol: pos.symbol, side: pos.side,
        size: Number(pos.size), entryPrice: Number(pos.entry_price),
        leverage: Number(pos.leverage), marginMode: pos.margin_mode || 'cross',
        isolatedMargin: Number(pos.isolated_margin) || 0,
        mark, upnl, rawUpnl,
        equityContribution: isolated
          ? isolatedSettlementValue(pos.isolated_margin, rawUpnl)
          : r6(rawUpnl),
      };
    }).sort((a, b) => a.symbol.localeCompare(b.symbol));
  const equity = r6(Number(acct.balance) + positions.reduce((sum, pos) => {
    const raw = Number(pos.rawUpnl);
    return sum + (pos.marginMode === 'isolated'
      ? Number(pos.equityContribution) : raw);
  }, 0));
  const round = explicitRound != null
    ? (holdsRoundPrices(explicitRound) ? explicitRound : null) : pricingRoundFor(userId);
  const capacityRound = explicitRound || (comp.inRound(userId) ? comp.currentRound() : null);
  const boostCapacityPolicy = comp.boostCapacityPolicyOf(capacityRound);
  return {
    schema: 'competition-score-input-v1',
    buildId: PAPER_BUILD_ID,
    capturedAt: Date.now(),
    account: {
      balance: Number(acct.balance), fillsCount: Number(acct.fills_count) || 0,
      feesPaid: Number(acct.fees_paid) || 0,
      fundingPaid: Number(acct.funding_paid) || 0,
      liquidations: Number(acct.liquidations) || 0,
    },
    positions, equity,
    ...(boostCapacityPolicy === 'current-equity-v1' ? { boostCapacityPolicy } : {}),
    ...(round ? { pricePolicy: ROUND_PRICE_POLICY, engineBoot: ENGINE_BOOT_ID,
      ...comp.backupExecutionPolicyFields(round),
      roundMarkLineage: 'per-record-v1',
      roundClockAvailable: !_roundHistoricalPause.get(round.id, cut, cut),
      roundMarks: comp.roundMarkEvidenceFor(round.id,
        [...new Set(positions.map((pos) => baseOf(pos.symbol)))], cut) } : {}),
    fillLedger: fillLedgerEvidence(userId, ep, cut),
  };
}
function roundScoreEvidenceValid(state, asOf, round) {
  if (!holdsRoundPrices(round)) return true; // immutable legacy proof format
  if (!state || state.pricePolicy !== ROUND_PRICE_POLICY
      || !comp.backupExecutionPolicyMatches(state.backupExecutionPolicy, round)
      || state.roundMarkLineage !== 'per-record-v1' || state.roundClockAvailable !== true
      || typeof state.engineBoot !== 'string' || !/^[a-zA-Z0-9_-]{1,64}$/.test(state.engineBoot)
      || !Number.isFinite(asOf) || !Array.isArray(state.positions)
      || !state.roundMarks || typeof state.roundMarks !== 'object'
      || Array.isArray(state.roundMarks)) return false;
  const bases = new Map();
  for (const pos of state.positions) {
    if (!pos || typeof pos.symbol !== 'string' || !Number.isFinite(pos.mark)
        || pos.mark <= 0 || !Number.isFinite(pos.leverage) || pos.leverage <= 0) return false;
    const base = baseOf(pos.symbol);
    // Creation/admission checked the enabled universe. A later delisting is
    // not grounds to rewrite a settled proof's frozen qualification contract.
    if (!/^[A-Z0-9]{1,24}$/.test(base)) return false;
    const prior = bases.get(base);
    if (prior && prior.price !== pos.mark) return false;
    bases.set(base, { price: pos.mark, leverage: Math.max(prior?.leverage || 0,
      pos.leverage, comp.requiredRoundMarkLeverage(round, base)) });
  }
  const keys = Object.keys(state.roundMarks);
  if (keys.length !== bases.size || keys.some((base) => !bases.has(base))) return false;
  for (const [base, expected] of bases) {
    const row = state.roundMarks[base];
    if (!row || row.base !== base || row.price !== expected.price
        || typeof row.acceptedBoot !== 'string' || !/^[a-zA-Z0-9_-]{1,64}$/.test(row.acceptedBoot)
        || !Number.isSafeInteger(row.acceptedSeq) || row.acceptedSeq <= 0
        || !(Number.isFinite(row.observedAt) && row.observedAt > 0
          && Number.isFinite(row.acceptedAt) && row.acceptedAt >= row.observedAt
          && Number.isFinite(row.appliedAt) && row.appliedAt >= row.acceptedAt && row.appliedAt <= asOf
          && Number.isFinite(row.originalValidUntil) && row.originalValidUntil > row.appliedAt
          && comp.roundMarkExecutionLeverage(round, row) >= expected.leverage)
        || typeof row.source !== 'string' || !/^[a-z][a-z0-9_-]{0,31}$/.test(row.source)
        || row.hardInvalid !== false || row.hardFailure != null) return false;
  }
  return true;
}
function roundAttemptEvidenceValid(attempt, activationAt, kind, requiredLeverage, round) {
  const e = attempt?.evidence, policy = e?.policy;
  const required = Math.max(Number(requiredLeverage), comp.requiredRoundMarkLeverage(round, attempt?.asset));
  if (!policy || policy.version !== ROUND_PRICE_POLICY || policy.kind !== kind
      || !comp.backupExecutionPolicyMatches(policy.backupExecutionPolicy, round)
      || !Object.prototype.hasOwnProperty.call(policy, 'buildId')
      || typeof policy.engineBoot !== 'string' || !/^[a-zA-Z0-9_-]{1,64}$/.test(policy.engineBoot)
      || policy.leverageRequired !== required || e.checkedAt !== activationAt
      || !Object.prototype.hasOwnProperty.call(e, 'clockPause')) return false;
  const pause = e.clockPause;
  if (pause !== null && (!pause || typeof pause !== 'object' || Array.isArray(pause)
      || Object.keys(pause).length !== 2 || !Number.isFinite(pause.from) || pause.from <= 0
      || pause.from > activationAt || !(pause.to === null
        || (Number.isFinite(pause.to) && pause.to > activationAt)))) return false;
  const o = e.observation;
  const qualified = !!(o && o.base === attempt.asset && Number.isFinite(o.price) && o.price > 0
    && o.acceptedBoot === policy.engineBoot && Number.isSafeInteger(o.acceptedSeq) && o.acceptedSeq > 0
    && Number.isFinite(o.acceptedAt) && o.acceptedAt > 0
    && Number.isFinite(o.appliedAt) && o.appliedAt >= o.acceptedAt && o.appliedAt <= activationAt
    && Number.isFinite(o.observedAt) && o.observedAt > 0 && o.observedAt <= o.acceptedAt
    && Number.isFinite(o.originalValidUntil) && o.originalValidUntil > o.appliedAt
    && comp.roundMarkExecutionLeverage(round, o) >= required
    && typeof o.source === 'string' && /^[a-z][a-z0-9_-]{0,31}$/.test(o.source));
  const invalid = !!(o?.hardInvalid && o.hardFailure
    && Number.isFinite(o.hardFailure.at) && o.hardFailure.at <= activationAt
    && typeof o.hardFailure.reason === 'string' && o.hardFailure.reason.length > 0
    && o.hardFailure.reason.length <= 160);
  const ready = qualified && !invalid && pause === null;
  return !!attempt.ready === ready && !!e.ready === ready && !!e.historicalReady === ready
    && (ready ? e.boundaryMark === o.price && o.accepted === true && e.invalidity === null
      : typeof e.rejectReason === 'string' && e.rejectReason.length > 0 && e.rejectReason.length <= 200
        && e.invalidity && Number.isFinite(e.invalidity.since) && e.invalidity.since <= activationAt
        && (pause === null || e.invalidity.since === pause.from));
}
/* Per-player PnL from the BOOST window: realised, plus what the open boost
   legs are worth right now. Mirrors hotBonus deliberately, because counting
   only realised fills shows nothing during the window and then jumps at the
   bell, which is exactly the bug that made the Hot wall wrong inside its own
   window. Unlike Hot there is a boost ticker PER MARKET, so this sums across
   all of them rather than naming one. */
const compBoostRealized = db.prepare(
  "SELECT COALESCE(SUM(realized_pnl + COALESCE(bad_debt, 0)), 0) AS v FROM paper_fills WHERE user_id = ? AND epoch = ? AND symbol LIKE '%-BOOST' AND ts <= ? AND realized_pnl IS NOT NULL"
);
const posBoostOpen = db.prepare(
  "SELECT * FROM paper_positions WHERE user_id = ? AND epoch = ? AND symbol LIKE '%-BOOST'"
);
function openBoostPnl(userId, epoch, marks = null, liveLeverage = false) {
  let v = 0;
  for (const p of posBoostOpen.all(userId, epoch)) {
    const mk = posMarkOf(p, marks, { liveLeverage });
    /* Same bankruptcy floor as accountRisk: the leg's loss is capped at its
       allocated margin on every close path, so the wall must not report a
       deeper one while the position happens to still be open. */
    if (Number.isFinite(mk) && mk > 0) v += Math.max(uPnl(p, mk), -(p.isolated_margin || 0));
  }
  return v;
}
function scoreUser(userId, hotTicker, epoch = null, startBalance = null, marks = null, asOf = null, riskCapture = null) {
  const cut = Number.isFinite(asOf) ? asOf : Date.now();
  const acct = ensureAccount(userId);
  /* A supplied mark set can mean one of two things: immutable boundary marks,
     already leverage-validated by markSetFor, or the current socket fence.
     Only the latter must be rechecked against live high-tier quality. */
  const liveLeverage = !!marks && !Number.isFinite(asOf);
  const risk = accountRisk(userId, acct, { marks, liveLeverage });
  // Private caller-owned result, never serialized or spread into score/proof
  // fields. The dynamic board must not multiply an already rounded equity,
  // nor reread marks/accounts after the score's one coherent risk snapshot.
  if (riskCapture) riskCapture.equityTotal = risk.equityTotal;
  /* Measure against the balance the ROUND bound, not the account's current
     one: if anything restamps start_balance mid-round the result must not
     move under it. */
  const start = Number.isFinite(startBalance) ? startBalance : (acct.start_balance || START_BALANCE);
  /* Score the epoch the ROUND bound at its start, not whatever the account
     happens to be on now. If anything bumps the epoch mid-round, the result
     must not silently change basis underneath it. */
  const ep = Number.isFinite(epoch) ? epoch : acct.epoch;
  return {
    equity: r6(risk.equityTotal),
    accountPnl: r6(risk.equityTotal - start),
    realized: r6(compRealized.get(userId, ep, cut).v),
    /* Realised Hot PnL plus whatever the open Hot leg is worth right now.
       Counting only realised fills made the wall show 1x during the segment
       and jump to 2x the instant the ticker force-closed, so every projected
       ranking inside the window was wrong. Because the segment closes into
       realised PnL, this transitions continuously rather than stepping. */
    hotBonus: hotTicker ? r6(compHot.get(userId, ep, hotTicker, cut).v
      + openHotPnl(userId, hotTicker, marks, liveLeverage)) : 0,
    /* Reported for the stage breakdown on the wall. NOT added into score:
       boost pays through ordinary PnL at higher leverage, it is not a
       separate bonus the way the Hot segment's 2x is. */
    boostPnl: r6(compBoostRealized.get(userId, ep, cut).v
      + openBoostPnl(userId, ep, marks, liveLeverage)),
  };
}

/* Economic PnL attributable to one underlying at an instant. A Hot score is
 * the change in this cumulative quantity across its exact two-minute window:
 * realised+bad-debt plus still-open marked PnL. That delta naturally carries
 * a pre-existing position's movement without doubling gains earned before
 * Hot, and remains correct across partial closes, reopens and flips. */
function hotValueOf(userId, base, epoch = null, marks = null, asOf = null) {
  const cut = Number.isFinite(asOf) ? asOf : Date.now();
  const acct = ensureAccount(userId);
  const ep = Number.isFinite(epoch) ? epoch : acct.epoch;
  const b = baseOf(String(base || '').toUpperCase());
  let value = Number(compHotFamily.get(userId, ep, b, b + '-HOT', b + '-BOOST', cut).v) || 0;
  const liveLeverage = !!marks && !Number.isFinite(asOf);
  for (const p of stmt.posByUser.all(userId)) {
    if (baseOf(p.symbol) !== b) continue;
    const mk = posMarkOf(p, marks, { liveLeverage });
    const u = uPnl(p, mk);
    value += isIso(p) ? Math.max(u, -(p.isolated_margin || 0)) : u;
  }
  return r6(value);
}

function boostExposureOf(userId, { excludeOrderId = null } = {}) {
  let total = 0;
  for (const p of stmt.posByUser.all(userId)) {
    if (aliasKind(p.symbol) !== 'BOOST') continue;
    let mk;
    try { mk = posMarkOf(p); } catch { mk = Number(p.last_mark) || Number(p.entry_price); }
    total += Number(p.size) * Number(mk);
  }
  for (const o of stmt.ordOpenByUser.all(userId)) {
    if (o.id === excludeOrderId || aliasKind(o.symbol) !== 'BOOST' || o.reduce_only) continue;
    total += Number(o.size) * Number(o.price);
  }
  return r6(total);
}

/* Last-line invariant for every fill source, including resting orders,
 * triggers and sweep actions. The foreground handler runs the same projection
 * to return a useful 400; background fills share the sealed capacity policy. */
function projectedBoostExposure(userId, symbol, orderSide, size, px, { excludeOrderId = null } = {}) {
  let total = 0;
  const target = stmt.posGet.get(userId, symbol);
  for (const p of stmt.posByUser.all(userId)) {
    if (aliasKind(p.symbol) !== 'BOOST' || p.symbol === symbol) continue;
    const mk = posMarkOf(p);
    total += Number(p.size) * Number(mk);
  }
  const held = target ? Number(target.size) * dirOf(target.side) : 0;
  const delta = Number(size) * (orderSide === 'BUY' ? 1 : -1);
  total += Math.abs(held + delta) * Number(px);
  for (const o of stmt.ordOpenByUser.all(userId)) {
    if (o.id === excludeOrderId || aliasKind(o.symbol) !== 'BOOST' || o.reduce_only) continue;
    total += Number(o.size) * Number(o.price);
  }
  return r6(total);
}

/* A resting intention is not a fill projection. Existing positions keep
 * their current marks and every non-reducing GTC reserves its full limit
 * notional; repricing a held same-symbol position to a low BUY limit made the
 * foreground check approve an aggregate that boostExposureOf rejected the
 * instant the row was inserted. */
function projectedBoostReservation(userId, size, px,
    { excludeOrderId = null, reduceOnly = false } = {}) {
  const current = boostExposureOf(userId, { excludeOrderId });
  return reduceOnly ? current : r6(current + Number(size) * Number(px));
}

// Both admission and the board consume this policy algebra. Hot score is
// never an input; actual equity includes the remaining value of isolated legs.
function boostMaximumFor(round, actualEquity, snapshotMaximum = null) {
  const policy = comp.boostCapacityPolicyOf(round);
  const lev = Number(round && round.boost_leverage) || HEAT_MAX_LEV;
  if (!Number.isFinite(actualEquity) || !Number.isFinite(lev) || lev <= 0) {
    throw new Error('Boost equity or leverage is unavailable');
  }
  const current = Math.max(0, actualEquity) * lev;
  if (!Number.isFinite(current)) throw new Error('Boost capacity is unavailable');
  if (snapshotMaximum != null && (!Number.isFinite(snapshotMaximum) || snapshotMaximum < 0)) {
    throw new Error('Boost start reference is unavailable');
  }
  const maximum = r6(snapshotMaximum == null || policy === 'current-equity-v1'
    ? current : Math.min(snapshotMaximum, current));
  if (!Number.isFinite(maximum)) throw new Error('Boost rounded capacity is unavailable');
  return maximum; // no snapshot means projected capacity, not permission
}

function boostCapacityCheck(userId, symbol, orderSide, size, px, opts = {}) {
  if (aliasKind(symbol) !== 'BOOST' || !competingNow(userId)) return { ok: true, projected: 0, max: null };
  const round = comp.currentRound();
  comp.boostCapacityPolicyOf(round); // unknown policy cannot authorize even a new reduction intent
  if (opts.trustedSettlementReduction) {
    const held = stmt.posGet.get(userId, symbol);
    const opposite = held && ((held.side === 'LONG' && orderSide === 'SELL')
      || (held.side === 'SHORT' && orderSide === 'BUY'));
    if (opposite && Number(size) > 0 && Number(size) <= Number(held.size) + 1e-9) {
      return { ok: true, projected: null, max: null, trustedReduction: true };
    }
  }
  const budget = comp.boostBudgetOf(userId, round);
  if (!budget || budget.maxExposure == null) {
    return { ok: false, error: 'boost_bankroll_not_frozen', projected: null, max: null };
  }
  /* Legacy winners cannot compound above their minute-27 snapshot. New
     rounds may use current profits, but losses still reduce the same shared
     budget. Ordinary free-margin checks remain independent and mandatory. */
  const actualEquity = accountRisk(userId, ensureAccount(userId), opts).equityTotal;
  const liveMaximum = boostMaximumFor(round, actualEquity, Number(budget.maxExposure));
  const projected = opts.reservation
    ? projectedBoostReservation(userId, size, px, opts)
    : projectedBoostExposure(userId, symbol, orderSide, size, px, opts);
  const current = boostExposureOf(userId, opts);
  return {
    ok: projected <= liveMaximum + 1e-9 || projected <= current + 1e-9,
    error: 'boost_capacity_exceeded', projected, max: liveMaximum,
    frozenMax: Number(budget.maxExposure), actualEquity: r6(actualEquity),
    boostCapacityPolicy: budget.policy,
    remaining: r6(Math.max(0, liveMaximum - current)),
  };
}

/* CANONICAL FAMILY EXPOSURE.
 *
 * BTC, BTC-HOT and BTC-BOOST are one underlying. Holding opposite directions
 * across them is not a trade, it is an arbitrage against our own settlement
 * rules, and it broke two of them at once:
 *
 *   - -BOOST tickers are forced isolated, so their loss is CAPPED at the
 *     allocated margin while the opposing base leg pays in full. A flat hedge
 *     therefore minted equity: $10 -> $10.40 in the reviewer's probe, purely
 *     from bad debt absorbed on the capped leg.
 *   - Hot PnL counts double for SCORE only, so a flat base/-HOT hedge banked
 *     +1.25 of leaderboard score against $0 of account PnL.
 *
 * Netting the legs at settlement does not fix either one, because the legs are
 * closed by separate fills at different instants. Refusing to let the opposite
 * exposure exist is the invariant that actually holds. Same-direction sibling
 * exposure stays legal and is still bounded by ordinary margin.
 *
 * Resting orders count: a limit that would fill into an opposing sibling
 * recreates the position the moment it triggers.
 */
function familyOf(sym) { return baseOf(sym); }
/* The direction this symbol would be left holding, NOT the side of the order.
 * Keying on the raw side blocks a plain reduce: selling part of a long BTC
 * while long BTC-BOOST lowers family risk and must stay legal. Returns
 * 1 long, -1 short, 0 flat. */
function resultingDir(pos, orderSide, size) {
  const want = orderSide === 'BUY' ? 1 : -1;
  if (!pos) return want;
  const held = dirOf(pos.side);
  if (held === want) return held;              // adding
  if (size < pos.size - 1e-12) return held;    // partial reduce, still same side
  if (size > pos.size + 1e-12) return want;    // flips through flat
  return 0;                                    // exact close
}
function familyConflict(userId, symbol, resultDir, { reduceOnly = false } = {}) {
  if (reduceOnly || resultDir === 0) return null;
  const fam = familyOf(symbol);
  for (const p of stmt.posByUser.all(userId)) {
    if (p.symbol === symbol || familyOf(p.symbol) !== fam) continue;
    if (dirOf(p.side) === -resultDir) return { kind: 'position', symbol: p.symbol, side: p.side };
  }
  /* Resting siblings count only where they could OPEN opposing exposure.
   * A resting order against a sibling position of its own is a reduction, and
   * blocking those would forbid ordinary risk management inside one family. */
  for (const o of stmt.ordOpenByUser.all(userId)) {
    if (o.symbol === symbol || familyOf(o.symbol) !== fam) continue;
    if (o.reduce_only) continue;
    if ((o.side === 'BUY' ? 1 : -1) !== -resultDir) continue;
    const sib = stmt.posGet.get(userId, o.symbol);
    /* Exempt ONLY when the order cannot flip through flat. A resting SELL 5
       against a long 2 was exempted as "reducing its own leg", then its fill
       flipped the base short against a -BOOST long: the capped-loss hedge the
       whole invariant exists to refuse, rebuilt through the order book. */
    if (sib && dirOf(sib.side) !== (o.side === 'BUY' ? 1 : -1) && o.size <= sib.size + 1e-12) continue;
    return { kind: 'order', symbol: o.symbol, side: o.side };
  }
  return null;
}

/* Mark-to-market value of an open position on the Hot ticker, 0 if flat. */
function openHotPnl(userId, hotTicker, marks = null, liveLeverage = false) {
  const p = stmt.posGet.get(userId, hotTicker);
  if (!p) return 0;
  const mk = posMarkOf(p, marks, { liveLeverage });
  if (!(Number.isFinite(mk) && mk > 0)) return 0;
  const u = uPnl(p, mk);
  /* Fourth member of the bankruptcy-floor family. accountRisk, the public
     leaderboard and openBoostPnl all cap an isolated leg's loss at its
     allocated margin, because every close path does; this one fed the RAW
     depth of a gapped iso Hot leg into hotBonus, so score mixed a floored
     accountPnl with an unfloored bonus, disagreed with the wall, and froze
     the disagreement into the checkpoint. */
  return isIso(p) ? Math.max(u, -(p.isolated_margin || 0)) : u;
}

/* Operator-side account preparation, used by startRound so that resetting a
   roster is part of going live rather than a button someone must remember.
   Returns the account's NEW epoch so the round can bind its scoring basis. */
function prepareSeat(userId) {
  const now = Date.now();
  ensureAccount(userId);
  db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(userId);
  db.prepare("DELETE FROM paper_orders WHERE user_id = ? AND status = 'OPEN'").run(userId);
  // stamp the contest's account spec, do not inherit whatever mode this
  // account was in: stage bankroll, no fees, no funding, fractional lots
  db.prepare('UPDATE paper_accounts SET heat = 1, start_balance = ? WHERE user_id = ?')
    .run(HEAT_BALANCE, userId);
  stmt.acctReset.run(now, now, userId);
  return seatState(userId);
}
function seatState(userId) {
  const a = stmt.acctGet.get(userId);
  if (!a) return null;
  return { epoch: a.epoch, startBalance: a.start_balance ?? START_BALANCE, stage: isStage(a.heat) };
}

// ── competition HTTP surface ─────────────────────────────────────────────
/* Operator actions carry their own token on top of the nginx gate: the gate
 * only proves the request came through the site, and everyone trading on
 * /ftpaper is through the site. Fails closed — with no token configured,
 * nothing can start or abort a round. */
const COMP_TOKEN = process.env.PAPER_COMP_TOKEN || '';
/* Header only. A token in a JSON body ends up in request logs, proxy traces
   and shell history far more readily than one in a header, and the operator
   surface is the most valuable credential in the system. Comparison is
   constant-time so a wrong token cannot be narrowed by timing. */
function compAuthed(req) {
  const got = String(req.headers['x-comp-token'] || '');
  if (!COMP_TOKEN || !got || got.length !== COMP_TOKEN.length) return false;
  try {
    return crypto.timingSafeEqual(Buffer.from(got), Buffer.from(COMP_TOKEN));
  } catch { return false; }
}

/* Operator actions are rate limited on their own budget: the engine's
   per-user write limiter does not cover them, and an unbounded admin surface
   is a way to hammer the database during a show. */
const _opRate = { t: 0, n: 0 };
const OP_PER_MIN = Number(process.env.PAPER_COMP_OPS_PER_MIN || 60);
function opRateOk() {
  const now = Date.now();
  if (now - _opRate.t > 60_000) { _opRate.t = now; _opRate.n = 0; }
  return ++_opRate.n <= OP_PER_MIN;
}

/* Every operator action, logged. Who did what to which round and whether it
   worked, so a contested show has a record rather than a recollection. */
db.exec(`CREATE TABLE IF NOT EXISTS paper_operator_log (
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  at       INTEGER NOT NULL,
  action   TEXT    NOT NULL,
  round_id TEXT,
  ok       INTEGER NOT NULL,
  detail   TEXT,
  ip       TEXT
)`);
const _opLog = db.prepare('INSERT INTO paper_operator_log (at, action, round_id, ok, detail, ip) VALUES (?, ?, ?, ?, ?, ?)');
function logOp(req, action, roundId, ok, detail) {
  try {
    _opLog.run(Date.now(), String(action || '?'), roundId || null, ok ? 1 : 0,
      detail ? String(detail).slice(0, 400) : null,
      String(req.headers['x-real-ip'] || '').slice(0, 45) || null);
  } catch { /* logging must never break an operator action */ }
}

/* Live wall state. Public: this is what the room sees. The drawn market is
 * withheld until the reveal actually fires, so a spectator refreshing the
 * endpoint cannot learn the Hot Market before the players do. */
/* The wall polls this once a second, the room refreshes it, and anyone can
   hit it. Recomputing every player's account risk, Hot bonus, positions and
   sort order per request put the show's own event loop at the mercy of its
   audience — and event-loop delay is exactly what makes a boundary late.
   One short cache serves every caller from the same computation. */
/* How long a finished round's result stays on the desk and the wall. Long
   enough for the post-bell segment and the operator's read-out. */
const LAST_ROUND_SHOW_MS = Number(process.env.PAPER_LAST_ROUND_SHOW_MS || 10 * 60_000);
const STATE_TTL_MS = Number(process.env.PAPER_COMP_STATE_TTL_MS || 200);
let _stateCache = { at: 0, body: null };
const _stateRate = new Map();          // ip -> { t, n }
const STATE_PER_MIN = Number(process.env.PAPER_COMP_STATE_PER_MIN || 1200);
const _inviteRate = new Map();
const INVITE_PER_MIN = 60;
/* Keyed per PLAYER, not per IP. The whole point is that one contestant
   hammering ready cannot starve the others, and on show night all eight sit
   behind the venue's single NAT address: keying on the IP put them in one
   bucket, which is the starvation this was meant to prevent. */
function inviteRateOk(ip) {
  const now = Date.now();
  let r = _inviteRate.get(ip);
  if (!r || now - r.t > 60_000) { r = { t: now, n: 0 }; _inviteRate.set(ip, r); }
  if (_inviteRate.size > 5000) for (const [k, v] of _inviteRate) if (now - v.t > 120_000) _inviteRate.delete(k);
  return ++r.n <= INVITE_PER_MIN;
}

function stateRateOk(ip) {
  const now = Date.now();
  let r = _stateRate.get(ip);
  if (!r || now - r.t > 60_000) { r = { t: now, n: 0 }; _stateRate.set(ip, r); }
  if (_stateRate.size > 5000) for (const [k, v] of _stateRate) if (now - v.t > 120_000) _stateRate.delete(k);
  return ++r.n <= STATE_PER_MIN;
}

/* Which markets could host a segment right now, and how close each is to
 * failing the consensus gate. The operator picks the three candidates from
 * this, so it has to show the margin, not just a yes or no: a market sitting
 * at 2.9bps of 3 is one wick away from blocking a live scoring window.
 * Operator-only, because it exposes per-source pricing.
 */
/* Diagnostics are observations, never authority. The fixed indexed universe,
   short transition ring and finite counters bound both memory and response
   size. No round/draw/roster lookup, extra timer, persistence or logging lives
   here. Ordinary readiness/admission and all financial gates remain separate. */
const MARKET_DIAGNOSTIC_WINDOW_MS = 10 * 60_000;
const MARKET_DIAGNOSTIC_TRANSITIONS = 128;
const MARKET_DIAGNOSTIC_RECENT = 8;
const MARKET_DIAGNOSTIC_REASONS = new Set([
  'ready', 'no_observation', 'invalid_price', 'index_halted', 'jump_confirmation',
  'risk_failed', 'risk_expired', 'source_persistence_failed', 'receipt_expired',
  'source_unavailable', 'source_transition_pending', 'source_expired', 'unageable_source',
]);
const _marketDiagnostics = new Map();
function marketDiagnosticRecord(sym) {
  if (!STAGE_INDEXED.has(sym) || aliasKind(sym) || DISABLED_MARKETS.has(sym)) return null;
  let r = _marketDiagnostics.get(sym);
  if (!r) {
    r = { since: null, at: null, lastSeq: 0, base: null, boost: null,
      transitions: [], dropped: 0, counters: {
        accepted: 0, ingressRejected: 0, staleIngress: 0, lossNotifications: 0,
        ignoredPastObservations: 0,
      } };
    _marketDiagnostics.set(sym, r);
  }
  return r;
}
function noteMarketDiagnosticCounter(sym, key) {
  const r = marketDiagnosticRecord(sym);
  if (r && Object.hasOwn(r.counters, key)) {
    r.counters[key] = Math.min(Number.MAX_SAFE_INTEGER, r.counters[key] + 1);
  }
}
function marketDiagnosticFailure(sym, now, kind = null) {
  const m = live.map.get(sym);
  if (!m) return 'no_observation';
  if (m.indexHalt) return 'index_halted';
  if (_confirming.has(sym)) return 'jump_confirmation';
  if (_riskBlocked.has(sym) || kind === 'risk_failed') return 'risk_failed';
  if (_srcUnsafe.has(sym)) return 'source_persistence_failed';
  if (!(Number(m.pythPrice) > 0)) return 'invalid_price';
  if (now - (m.pythAtMs || 0) >= PYTH_STAGE_FRESH_MS) return 'receipt_expired';
  const until = sourceQuoteExpiry(m, sym);
  return Number.isFinite(until) && until <= now ? 'source_expired' : 'source_unavailable';
}
function marketDiagnosticBoostReady(sym, ready, source) {
  if (!ready) return false;
  if (HEAT_MAX_LEV <= BOOST_LEV_THRESHOLD && HEAT_MAX_LEV <= UNAGEABLE_CAP_LEV) return true;
  if (UNAGEABLE_POLICY === 'allow') return true;
  // Use the source chosen by an EXISTING canonical decision. Do not choose it
  // again: activeSource can perform lazy recovery, which telemetry must not do.
  const key = source || live.map.get(sym)?.srcKey;
  const component = key && (_idxComps.get(sym) || {})[key];
  return !component || !!component.ageKnown;
}
function observeMarketDiagnostic(sym, at, ready, boostReady = null, detail = {}, acceptedSeq = 0) {
  if (_riskProbe || _committedReadinessProbe || detail.reason === 'transient_probe') return;
  if (!Number.isFinite(at)) return;
  const r = marketDiagnosticRecord(sym);
  if (!r) return;
  if (r.at !== null && at < r.at) {
    noteMarketDiagnosticCounter(sym, 'ignoredPastObservations');
    return;
  }
  const reason = ready ? 'ready' : (MARKET_DIAGNOSTIC_REASONS.has(detail.reason)
    ? detail.reason : marketDiagnosticFailure(sym, at));
  const boost = boostReady === null ? marketDiagnosticBoostReady(sym, ready, detail.source) : !!boostReady;
  const boostReason = !ready ? reason : (boost ? 'ready' : 'unageable_source');
  const m = live.map.get(sym), deadline = sourceQuoteExpiry(m, sym);
  let changed = !r.base;
  for (const [key, nextReady, nextReason] of [['base', !!ready, reason], ['boost', boost, boostReason]]) {
    const previous = r[key];
    if (!previous) {
      r[key] = { ready: nextReady, reason: nextReason, since: at, reasonAt: at,
        closedUnavailableMs: 0, longestGapMs: 0, intervals: 0, validUntil: deadline };
      continue;
    }
    if (previous.ready !== nextReady || previous.reason !== nextReason) {
      changed = true;
      let edge = at;
      // Preserve a lease gap even when its recovery callback arrives before
      // the five-second sweep. Never rewrite time before an observed good state.
      if (previous.ready && !nextReady && ['source_expired', 'source_unavailable',
        'receipt_expired', 'source_transition_pending'].includes(nextReason)
          && Number.isFinite(previous.validUntil)) {
        edge = Math.max(r.at ?? at, Math.min(at, previous.validUntil));
      }
      if (previous.ready !== nextReady) {
        if (!previous.ready) {
          const elapsed = Math.max(0, edge - previous.since);
          previous.closedUnavailableMs += elapsed;
          previous.longestGapMs = Math.max(previous.longestGapMs, elapsed);
          if (elapsed > 0) previous.intervals++;
        }
        previous.since = edge;
      }
      previous.reasonAt = edge;
      previous.ready = nextReady;
      previous.reason = nextReason;
    }
    previous.validUntil = deadline;
  }
  if (r.since === null) r.since = at;
  r.at = at;
  if (Number.isSafeInteger(acceptedSeq) && acceptedSeq > r.lastSeq) {
    r.lastSeq = acceptedSeq;
    noteMarketDiagnosticCounter(sym, 'accepted');
  }
  if (changed) {
    r.transitions.push({ at, baseReady: r.base.ready, baseReason: r.base.reason,
      baseSince: r.base.since, boostReady: r.boost.ready, boostReason: r.boost.reason,
      boostSince: r.boost.since });
  }
  while (r.transitions.length > MARKET_DIAGNOSTIC_TRANSITIONS
      || (r.transitions.length > 1 && r.transitions[1].at < at - MARKET_DIAGNOSTIC_WINDOW_MS)) {
    r.transitions.shift();
    r.dropped++;
  }
}
function marketDiagnosticSnapshot(sym, now, ready, boostReady, detail = {}) {
  observeMarketDiagnostic(sym, now, ready, boostReady, detail);
  const r = _marketDiagnostics.get(sym);
  if (!r || !r.base) return null;
  const m = live.map.get(sym), source = m?.srcKey || detail.source || null;
  const component = source && (_idxComps.get(sym) || {})[source];
  const finite = (n) => Number.isFinite(n) ? n : null;
  const validUntil = sourceQuoteExpiry(m, sym);
  const summarize = (s) => {
    const open = s.ready ? 0 : Math.max(0, Math.max(now, r.at) - s.since);
    return { ready: s.ready, reason: s.reason, since: s.since,
      unavailableIntervals: s.intervals + (open > 0 ? 1 : 0),
      unavailableMs: s.closedUnavailableMs + open,
      longestGapMs: Math.max(s.longestGapMs, open) };
  };
  return {
    observation: { source, acceptedSeq: Number.isSafeInteger(m?.acceptedSeq) ? m.acceptedSeq : null,
      ageKnown: !!component?.ageKnown,
      sourceAgeMs: component ? finite(compAgeMs(component, now)) : null,
      receiptAgeMs: m ? finite(now - (Number(m.pythAtMs) || 0)) : null,
      validUntil: finite(validUntil),
      expiryMarginMs: Number.isFinite(validUntil) ? validUntil - now : null },
    base: { ready: !!ready, reason: ready ? 'ready' : (MARKET_DIAGNOSTIC_REASONS.has(detail.reason)
      ? detail.reason : marketDiagnosticFailure(sym, now)) },
    boost: { ready: !!boostReady, reason: !ready
      ? (MARKET_DIAGNOSTIC_REASONS.has(detail.reason) ? detail.reason : marketDiagnosticFailure(sym, now))
      : (boostReady ? 'ready' : 'unageable_source') },
    history: { scope: 'boot-local observed events', observedSince: r.since, observedAt: r.at,
      retainedSince: r.transitions[0]?.at ?? r.since,
      windowMs: MARKET_DIAGNOSTIC_WINDOW_MS, transitionLimit: MARKET_DIAGNOSTIC_TRANSITIONS,
      droppedTransitions: r.dropped, counters: { ...r.counters },
      base: summarize(r.base), boost: summarize(r.boost),
      recent: r.transitions.slice(-MARKET_DIAGNOSTIC_RECENT).map((x) => ({ ...x })) },
  };
}
function compReadiness(req, res) {
  if (!compAuthed(req)) return send(res, 403, { ok: false, error: 'forbidden' });
  const now = Date.now();
  const out = [];
  /* Iterate the INDEXED SET, not live.map. live.map is populated as feed
     messages arrive, so a symbol that has not ticked since boot is simply
     absent — and the desk, which colours an absent symbol as "not a market",
     painted a perfectly good BTC red mid-setup after any restart. The engine
     knows exactly which symbols it indexes; publish all of them and let
     `ready` carry the truth. */
  const syms = new Set([...STAGE_INDEXED, ...live.map.keys()]);
  for (const sym of syms) {
    if (aliasKind(sym)) continue;                 // segment twins, not candidates
    const q = compQuality(sym, now);
    const readinessDetail = {};
    const ready = compPriceReady(sym, now, readinessDetail);
    const readyForBoost = readyForLeverage(sym, HEAT_MAX_LEV, now);
    const followed = activeSource(sym, now);
    out.push({
      symbol: sym,
      ready,
      components: q.n,
      freshComponents: q.fresh,
      venues: q.venues,
      maxSourceAgeMs: Number.isFinite(q.maxAgeMs) ? Math.round(q.maxAgeMs) : null,
      /* Named for the cap the product actually offers, not a number it left
         behind. An operator reading "readyFor1000x" on a 500x product cannot
         tell whether the field is stale or the cap is. */
      readyForBoost,
      boostLeverage: HEAT_MAX_LEV,
      /* Which source the engine is following, and how far the others sit from
         it. The chain is the price; the rest is telemetry. */
      following: followed ? { source: followed.key, ageMs: Math.round(followed.ageMs) } : null,
      diagnostics: marketDiagnosticSnapshot(sym, now, ready, readyForBoost, readinessDetail),
      chain: INDEX_CHAIN,
      lazerConfBps: _lazerConf.has(sym) ? Number(_lazerConf.get(sym).toFixed(3)) : null,
      lazerRejected: _lazerRejects.has(sym) || false,
      leverageCap: qualityLeverageCap(sym, now),
      sustained: readyRatio(sym, now),
      /* The verdict the ARM gate will actually reach, from the gate's own
         code. `ready` above is instantaneous; this is whether the market has
         been ready long enough and steadily enough to host a scored window,
         which is the question the operator is really asking while typing
         candidates. Without it the desk went green and Arm then refused. */
      ...(() => { try { const v = comp.marketReliabilityOf(sym); return { reliable: v.reliable, enoughHistory: v.enoughHistory, needsMoreMs: v.needsMoreMs }; } catch { return {}; } })(),
      spreadBps: Number.isFinite(q.spreadBps) ? Number(q.spreadBps.toFixed(3)) : null,
      /* Component disagreement against the OLD blended-index gate. The engine
         follows one published source now and compPriceReady applies no spread
         test, so this is telemetry about the sources, NOT margin to readiness:
         markets sit at -1.9 "headroom" while perfectly ready. Kept under a
         name that says what it is, because a field called headroomBps was
         being read on the desk as a safety margin it has not measured since
         the single-source move. */
      componentSpreadBps: Number.isFinite(q.spreadBps) ? Number(q.spreadBps.toFixed(3)) : null,
      componentSpreadGateBps: COMP_MAX_SPREAD_BPS,
      prices: q.prices,
    });
  }
  out.sort((a, b) => (b.ready - a.ready) || (Number(b.reliable || false) - Number(a.reliable || false))
    || ((a.componentSpreadBps ?? 1e9) - (b.componentSpreadBps ?? 1e9)));
  send(res, 200, { ok: true, at: now, componentSpreadGateBps: COMP_MAX_SPREAD_BPS, minComponents: COMP_MIN_COMPONENTS, markets: out });
}

/** Symbols carrying live exposure that we currently cannot price strictly.
 *  These are the markets whose contribution to the board is frozen. */
/* Read the ROSTER's positions, not the scored payload.
 *
 * A seat whose account cannot be priced now fails to score, so it never
 * reached the scored list and its frozen market vanished from the report —
 * the wall would say "incomplete" without saying what was wrong, which is the
 * opposite of the point. */
function staleUnderlyings(players) {
  const out = new Set();
  const now = Date.now();
  for (const pl of players) {
    const uid = pl.userId ?? pl.user_id;
    if (uid == null) continue;
    let rows = [];
    try { rows = stmt.posByUser.all(uid); } catch { rows = []; }
    for (const pos of rows) {
      const base = baseOf(pos.symbol);
      /* Judged against the leverage the exposure carries: a high-leverage
         board can otherwise look complete on a mark only valid for 100x. */
      const lev = Number(pos.leverage) || 0;
      if (!roundPriceReady(base, lev, now)) out.add(base);
    }
  }
  return [...out];
}

function compState(req, res) {
  const ip = String((req.headers && req.headers['x-real-ip']) || 'local');
  if (!stateRateOk(ip)) return send(res, 429, { ok: false, error: 'rate_limited' });
  const nowMs = Date.now();
  if (_stateCache.body && nowMs - _stateCache.at < STATE_TTL_MS) {
    return send(res, 200, { ..._stateCache.body, cachedMs: nowMs - _stateCache.at });
  }
  const out = buildCompState();
  _stateCache = { at: nowMs, body: out };
  return send(res, 200, out);
}

/* A transport-reset baseline bypasses both the public edge microcache and
 * this module's wall cache. It pins the competition revision current during
 * the synchronous build, so the browser can require its exact successor. */
function compBaseline(req, res) {
  const ip = String((req.headers && req.headers['x-real-ip']) || 'local');
  if (!stateRateOk(ip)) return send(res, 429, { ok: false, error: 'rate_limited' });
  const out = buildCompState();
  const body = {
    ...out, engineBoot: ENGINE_BOOT_ID, compRevision: _compBoardRevision,
  };
  /* Guarantee a successor to the pinned revision. This closes the race where
     the connection's initial board was already included by the baseline and a
     quiet market would otherwise leave the client waiting indefinitely. */
  scheduleCompBoard(null, true);
  return send(res, 200, body);
}

/* The browser times this uncached request with performance.now() and adds the
 * whole RTT to the inside-request sample. That is a conservative upper clock
 * bound: network delay may expire a board early, but can never renew one. */
function engineTime(_req, res) {
  return send(res, 200, { ok: true, v: 2, boot: ENGINE_BOOT_ID, t: Date.now() });
}

/* The single definition of "how much leverage can this Boost ticker carry".
 *
 * min(engine tier, live price quality, the cap the ROUND was armed under).
 * Every surface must use it: order path, resting fills, terminal, desk, wall.
 * Three of those used to compute it three different ways. */
function boostCapFor(alias) {
  const base = baseOf(alias);
  /* The ceiling comes from the ALIAS, exactly as the order path derives it.
   *
   * This read the BASE symbol, and the two agreed only by coincidence: while
   * the indexed base cap happened to equal the armed Boost cap, min() hid the
   * difference. Flattening the base to 100x separated them, and the wall then
   * advertised 100x on a segment the engine would fill at 500x. That is the
   * same class of lie this helper exists to prevent, pointing the other way:
   * the broadcast understating the round instead of overstating it. A BOOST
   * ticker's engine ceiling is the Boost ceiling; the round's armed cap and
   * the live quality cap lower it from there, and nothing else may. */
  const engine = stageLevCap(alias) || HEAT_MAX_LEV;
  const r = comp.currentRound();
  const quality = boostLevCap(base, engine, Date.now(), r);
  const armed = r && r.boost_leverage > 0 ? r.boost_leverage : 0;
  return armed > 0 ? Math.min(quality, armed) : quality;
}

/* WHAT THE ROOM SHOULD BE LOOKING AT, resolved server-side.
 *
 * Published on every state reply, live or not, so the wall keeps exactly one
 * poller: a second fetch for the stage would have its own failure mode, and
 * two feeds that can disagree in front of an audience is the failure this
 * whole surface exists to avoid.
 *
 * The countdown is resolved here rather than on the wall because there are
 * two sources for it, and only one of them will actually ring: a scheduled
 * round start beats a hand-set clock, always. A wall counting down to a time
 * nothing happens at is worse than a wall with no clock. */
/* A seat's display name, from whichever round it sat in. Names live on the
   ROSTER row, not on the account, so a series board spanning five rounds has
   to look across them; the most recent spelling wins, which is the one the
   room has been hearing all night. */
function displayNameOf(userId) {
  try {
    const row = comp.__test.db.prepare(
      `SELECT p.display_name FROM paper_round_players p
       JOIN paper_rounds r ON r.id = p.round_id
       WHERE p.user_id = ? AND p.display_name IS NOT NULL
       ORDER BY COALESCE(r.ends_at, r.started_at, r.created_at) DESC LIMIT 1`).get(userId);
    return row ? row.display_name : null;
  } catch { return null; }
}

function showState(pending) {
  let w = null;
  try { w = comp.wallState(); } catch { return null; }
  const scheduled = pending && Number(pending.startAt) > Date.now() ? Number(pending.startAt) : null;
  const nextAt = scheduled || (Number(w.next_at) > Date.now() ? Number(w.next_at) : null);
  let series = null;
  if (w.series) {
    try {
      const b = comp.seriesBoard(w.series);
      if (b) {
        const nm = (uid) => (uid == null ? null : displayNameOf(uid));
        series = {
          label: b.series,
          practice: b.practice,
          official: b.official,
          roundsDone: b.roundsDone,
          roundsTotal: w.series_total || null,
          latest: b.latest ? { ...b.latest, winnerName: nm(b.latest.winner) } : null,
          next: b.next ? { id: b.next.id, index: b.next.index, stage: b.next.stage, status: b.next.status, advance: b.next.advance } : null,
          stages: b.stages.map((st) => ({
            id: st.id, index: st.index, stage: st.stage, status: st.status, advance: st.advance,
            practice: st.practice,
            players: st.players.map((p) => ({ ...p, name: p.seatName || nm(p.userId) })),
          })),
          /* Published, not re-derived. The wall, the desk and any recap read the
             same placing and the same prize the engine settled on, so a screen
             can never disagree with the record about who came third. */
          placings: (b.placings || []).map((x) => ({ ...x, name: x.name || nm(x.userId) })),
          prizeTable: b.prizeTable || null,
          board: b.board.map((x) => ({
            userId: x.userId, name: nm(x.userId), bot: x.bot, practice: x.practice,
            latestRank: x.latestRank, wins: x.wins, stillIn: x.stillIn, rounds: x.rounds,
          })),
        };
      }
    } catch { /* the stage still renders without the table */ }
  }
  return {
    mode: w.mode,
    message: w.message || null,
    /* The night's shape, published rather than duplicated in the client. The
       desk builds its stage buttons from this and the wall labels rounds from
       it, so there is one place the format is written down. */
    bracket: comp.BRACKET,
    nextAt,
    /* RELATIVE, because the wall is a browser whose clock is not ours. An
       absolute instant subtracted from a laptop that is forty seconds off
       shows a forty-second lie on an LED wall, and venue machines are exactly
       the machines nobody has checked the clock on. Every other countdown in
       this system is published relative for the same reason. */
    nextInMs: nextAt ? Math.max(0, nextAt - Date.now()) : null,
    /* Which clock the room is being shown, because "the operator set this by
       hand" and "a round will actually ring here" are different promises. */
    nextIsScheduled: !!scheduled,
    /* A countdown the operator is holding. The wall shows this number without
       counting it down, and the room sees the clock stopped rather than a
       clock that vanished. */
    pausedMs: w.paused_ms || null,
    nextLabel: w.next_label || null,
    series,
  };
}

/* The fast, authoritative leaderboard payload.
 *
 * /comp/state also formats clocks, wall controls, positions and recap data.
 * Rebuilding that document at price cadence was the wrong trade: contestants
 * received rank only after a one-second poll plus proxy cache, while their PnL
 * moved from the index socket. This scorer is the exact ranking slice of that
 * document and travels on the same socket. */
const _compRankSymbols = new Set();
/* Liquidations per seat this round, from the LIQUIDATION fills. The rank
   snapshot is rebuilt at price cadence, so this read is memoised for one
   second per seat: a flash that lands a second late is fine, one more SQLite
   round-trip per tick per seat is the stall class this engine already had. */
const _liqCache = new Map();
function liquidationsThisRound(pl, r) {
  const now = Date.now();
  const key = `${pl.user_id}:${pl.epoch}:${r.id}`;
  const c = _liqCache.get(key);
  if (c && now - c.at < 1000) return c.value;
  let value = { count: 0, lastAt: null, lastSymbol: null, lastPnl: null };
  try {
    const rows = stmt.liqSince.all(pl.user_id, pl.epoch, Number(r.started_at) || 0);
    if (rows.length) {
      value = { count: rows.length, lastAt: Number(rows[0].ts) || null,
        lastSymbol: String(rows[0].symbol || ''), lastPnl: Number.isFinite(Number(rows[0].realized_pnl)) ? r6(Number(rows[0].realized_pnl)) : null };
    }
  } catch { /* a wall flash must never take the snapshot down */ }
  if (_liqCache.size > 4000) _liqCache.clear();
  _liqCache.set(key, { at: now, value });
  return value;
}
function compRankSnapshot(phase = null, publishedMarks = null) {
  const p = phase || comp.phaseNow();
  if (!p || !p.round) {
    _compRankSymbols.clear();
    return { live: false, roundId: null, players: [], unscored: [], stalePricing: [], complete: true,
      ...safePhaseControl(null) };
  }
  const r = p.round;
  const drawn = !!r.draw_at;
  const activeHot = r.active_hot_base || r.hot_base;
  const symbols = new Set();
  const roster = comp.playersOf(r.id);
  /* When this snapshot is for the socket, build one mark set from prices that
     have actually crossed that socket. Accepted-but-cadence-thinned ticks may
     already be in live.map; letting them leak into score would put rank ahead
     of the PnL/chart event that explains it. REST passes no map and preserves
     its ordinary current-state semantics. */
  const roundMarks = holdsRoundPrices(r);
  const marks = (roundMarks || publishedMarks) ? {} : null;
  for (const pl of roster) {
    let held = [];
    try { held = stmt.posByUser.all(pl.user_id); } catch { /* score reports the account failure below */ }
    for (const pos of held) {
      const base = baseOf(pos.symbol);
      symbols.add(base);
      if (roundMarks || publishedMarks) {
        const px = roundMarks ? roundExecutionMark(base, { round: r,
          forLeverage: Number(pos.leverage) || 0 })?.price : Number(publishedMarks.get(base));
        if (px > 0) marks[pos.symbol] = px;
      }
    }
  }
  const rows = roster.map((pl) => {
    /* Exposure drives future refreshes. Include aliases by their base because
       BTC, BTC-HOT and BTC-BOOST all move from the BTC index frame. */
    let s = null;
    let scoreError = null;
    const scoreRisk = {};
    try {
      /* The round bound this baseline at start. Reading the mutable account
         field here made live REST/push scoring disagree with checkpoints after
         any later mode/reset drift, even though both claimed to score the same
         epoch and round. */
      const legacyHotTicker = Number(r.format_version) < 2 && drawn ? `${activeHot}-HOT` : null;
      s = scoreUser(pl.user_id, legacyHotTicker,
        pl.epoch, pl.start_balance, marks, null, scoreRisk);
      if (Number(r.format_version) >= 2) {
        s.hotBonus = r6(comp.hotBonusFor(r, pl.user_id, marks));
        /* Its split per Hot, so a player's history can say what each Hot
           added. Informational: the score above stays the single authority,
           and a split that cannot be produced is simply absent. */
        try {
          s.hotBonusByHot = Object.fromEntries(comp.hotBonusRowsFor(r, pl.user_id, marks)
            .map((x) => [String(x.number), r6(x.bonus)]));
        } catch { s.hotBonusByHot = undefined; }
      }
    } catch (e) {
      scoreError = String(e.message || e).slice(0, 120);
    }
    if (!s) {
      return {
        userId: pl.user_id, name: pl.display_name, seat: pl.seat,
        error: scoreError || 'unscoreable',
      };
    }
    const budget = comp.boostBudgetOf(pl.user_id, r);
    const liquidations = liquidationsThisRound(pl, r);
    const maxExposure = budget && budget.maxExposure != null ? budget.maxExposure : null;
    const exposure = boostExposureOf(pl.user_id);
    const dynamicBoost = comp.boostCapacityPolicyOf(r) === 'current-equity-v1';
    const boostEquity = dynamicBoost ? scoreRisk.equityTotal : Number(s.equity);
    const liveBoostMaximum = maxExposure == null ? null
      : boostMaximumFor(r, boostEquity, Number(maxExposure));
    return {
      userId: pl.user_id, name: pl.display_name, seat: pl.seat,
      ...s, ...(Number(r.format_version) >= 2 && r.score_precision === 6
        ? comp.rankingValues(s) : { score: r6(s.accountPnl + s.hotBonus) }),
      maxDrawdown: Number(pl.max_drawdown) || 0,
      /* The legacy reference remains frozen; new rounds publish their actual
         current-equity capacity. Neither field adds the Hot score bonus. */
      projectedBoostPower: maxExposure == null || dynamicBoost
        ? boostMaximumFor(r, boostEquity)
        : maxExposure,
      boostBankroll: budget ? budget.bankroll : null,
      boostMaxExposure: dynamicBoost ? liveBoostMaximum : maxExposure,
      boostRemainingExposure: liveBoostMaximum == null ? null
        : r6(Math.max(0, liveBoostMaximum - exposure)),
      /* For the wall: how many times this seat was liquidated this round and
         the last one. Published so the room sees a bust as it happens; a
         voluntary close never appears here because only the liquidator writes
         LIQUIDATION fills. */
      liquidations,
    };
  });
  _compRankSymbols.clear();
  for (const sym of symbols) _compRankSymbols.add(sym);
  const players = rows.filter((x) => !x.error)
    .sort(comp.compareStandings)
    .map((x, i) => ({ ...x, rank: i + 1 }));
  const unscored = rows.filter((x) => x.error);
  const stalePricing = staleUnderlyings(rows);
  return { live: true, roundId: r.id, players, unscored, stalePricing,
    complete: unscored.length === 0 && stalePricing.length === 0,
    ...safePhaseControl(p) };
}

function publicRoundPlan(r) {
  const p = comp.planOf(r);
  if (!p) return null;
  if (Number(r.format_version) < 2) return p;
  return {
    total: p.total,
    buildEnd: p.buildEnd,
    hot1WindowStart: p.hot1WindowStart,
    hot1WindowEnd: p.hot1WindowEnd,
    hot2WindowStart: p.hot2WindowStart,
    hot2WindowEnd: p.hot2WindowEnd,
    hotDuration: p.hotDuration,
    hotWarning: p.hotWarning,
    normalGap: p.normalGap,
    finalBuildStart: p.finalBuildStart,
    boostStart: p.boostStart,
  };
}

function revealedHots(r, phase = null) {
  if (Number(r && r.format_version) < 2) return [];
  const p = phase || comp.phaseNow();
  const out = [];
  for (const n of [1, 2]) {
    const revealedAt = Number(r[`hot${n}_revealed_at`]) || 0;
    const market = r[`hot${n}_active_base`] || r[`hot${n}_base`];
    if (!revealedAt || !market) continue;
    const active = !!(p && p.round && p.round.id === r.id && p.phase === 'hot' && p.hotNumber === n);
    /* Fire and end times are public once revealed: the fire time is the
       reveal itself, and the end is the settled close of that Hot's rows. */
    let endedAt = null;
    try {
      const closed = comp.__test.db.prepare(
        'SELECT ended_at FROM paper_round_hot_scores WHERE round_id = ? AND hot_no = ? AND ended_at IS NOT NULL LIMIT 1',
      ).get(r.id, n);
      endedAt = closed && closed.ended_at != null ? Number(closed.ended_at) : null;
    } catch { endedAt = null; }
    out.push({
      number: n, market, ticker: market, status: active ? 'active' : 'complete',
      open: active, revealedAt, endedAt: active ? null : endedAt,
    });
  }
  return out;
}

/* Settlement-only enrichment. Live serializers intentionally expose only an
   already-revealed active market; after done/started-abort the recap may also
   explain which committed primary was replaced and why. */
function settledHots(r) {
  return revealedHots(r, null).map((hot) => {
    const drawnMarket = r[`hot${hot.number}_base`] || null;
    const fallbackReason = r[`hot${hot.number}_fallback_reason`] || null;
    return {
      ...hot,
      drawnMarket,
      fellBack: !!(drawnMarket && hot.market && drawnMarket !== hot.market),
      fallbackReason,
    };
  });
}

/* The only live phase serializer. In particular, build deliberately omits
 * its private next boundary: otherwise a network poll would reveal the random
 * activation fifteen seconds before the generic warning is allowed to. */
function safePhaseControl(p, diagnostic = false) {
  if (!p || !p.round) {
    return {
      phase: 'done', hotNumber: null, leftMs: null,
      phaseLeftMs: null, phaseEndsAt: null, paused: null,
      hot: null, hots: [], boostOpen: false, boostMarkets: [], boostCaps: {},
    };
  }
  const r = p.round;
  const countdownPublic = ['hotWarning', 'hot', 'finalBuild', 'boost'].includes(p.phase);
  const hots = revealedHots(r, p);
  const hot = p.phase === 'hot'
    ? (hots.find((x) => x.number === p.hotNumber) || null)
    : null;
  const boostMarkets = p.phase === 'boost'
    ? [...openAliases.keys()].filter((k) => aliasKind(k) === 'BOOST').map(baseOf)
    : [];
  const rawPause = roundPaused();
  /* During a shared price pause no order is executable. A transient zero from
     the quality gate is therefore not a useful "live cap" and violates the
     public invariant that every advertised open Boost market has a positive
     cap. Publish the round-frozen ceiling while paused; `paused` is the
     explicit execution gate and the first recovery price refresh restores
     the ordinary live safety cap before trading resumes. */
  const boostCaps = p.phase === 'boost'
    ? Object.fromEntries([...openAliases.keys()]
        .filter((k) => aliasKind(k) === 'BOOST')
        .map((k) => [baseOf(k), rawPause
          ? Math.min(stageLevCap(k) || HEAT_MAX_LEV,
              Number(r.boost_leverage) || HEAT_MAX_LEV)
          : boostCapFor(k)]))
    : {};
  /* Never serialize the symbol attached to a v2 pause. A contestant may
     already hold a market that happens to be a still-sealed future draw
     member; echoing that symbol during build would make a generic safety
     event into an accidental draw oracle. Exact failing markets remain
     available through separate operator price diagnostics. */
  // Compact v2 pause objects are strict on already-open clients. Enriched
  // classes belong to full REST/errors; preserve their old wire shape/text.
  const paused = diagnostic ? publicPause(rawPause, r)
    : rawPause && Number(r.format_version) >= 2
      ? { since: rawPause.since || null, count: Number(rawPause.count) || 1,
          why: 'competition prices unavailable', degraded: !!rawPause.degraded }
      : rawPause;
  return {
    phase: p.phase,
    hotNumber: p.phase === 'hotWarning' || p.phase === 'hot' ? p.hotNumber : null,
    leftMs: Math.max(0, comp.planOf(r).total - (p.activeMs || 0)),
    phaseLeftMs: countdownPublic ? Math.max(0, p.endsAt - (p.activeMs || 0)) : null,
    phaseEndsAt: countdownPublic ? comp.dueInstant(r, p.endsAt) : null,
    paused,
    hot, hots,
    boostOpen: p.phase === 'boost' && boostMarkets.length > 0,
    boostMarkets, boostCaps,
  };
}

function operatorRoundView(r) {
  if (!r) return null;
  return {
    id: r.id, kind: r.kind, status: r.status,
    formatVersion: Number(r.format_version) || 1,
    startedAt: r.started_at || null, endsAt: r.ends_at || null,
    startAt: r.start_at || null, blockedReason: r.blocked_reason || null,
    boostLeverage: r.boost_leverage || null,
    boostCapacityPolicy: comp.boostCapacityPolicyOf(r),
    ...comp.backupExecutionPolicyFields(r),
    solo: !!r.solo, speed: Number(r.speed) || 1,
    series: r.series || null, stage: r.stage || null, advance: r.advance || null,
    /* Commitment is published at the bell, never while merely armed. */
    drawCommit: r.status === 'running' || r.status === 'done' ? r.draw_commit : null,
  };
}

/* The settled board is polled by every terminal and the desk. Rehashing all
   six fill checkpoints for every seat on every baseline request made a read
   consume hundreds of milliseconds on the same event loop that prices risk.
   Cache only this tiny public summary, never the independent verify endpoint.

   Invalidation is attached to SQLite mutations, not selected application
   callers: direct repairs, refunds, resets and future writers cannot forget
   it. TEMP triggers apply only to this connection; data_version detects any
   committed write from another connection. Ordinary position mark updates
   are not proof inputs (the proof owns frozen state), so they do not evict it.
   A rolled-back write may evict unnecessarily, which is the safe direction. */
const _settledExecutionCache = new Map();
const SETTLED_EXECUTION_CACHE_MAX = 4;
const SETTLED_EXECUTION_CACHE_MS = 60_000;
const _settledExecutionCacheStats = { hits: 0, misses: 0 };
let _settledExecutionDataVersion = null;
const executionDataVersion = db.prepare('PRAGMA data_version');
db.function('paper_invalidate_execution_summary', () => {
  _settledExecutionCache.clear();
  return 0;
});
for (const table of ['paper_rounds', 'paper_round_players', 'paper_round_boundaries',
  'paper_round_clock_pauses', 'paper_round_hot_scores', 'paper_round_hot_resolutions',
  'paper_round_boost_resolutions', 'paper_round_boost_freezes',
  'paper_round_score_proofs', 'paper_round_scores', 'paper_fills']) {
  for (const operation of ['INSERT', 'UPDATE', 'DELETE']) {
    db.exec(`CREATE TEMP TRIGGER IF NOT EXISTS summary_${table}_${operation}
      AFTER ${operation} ON main.${table}
      BEGIN SELECT paper_invalidate_execution_summary(); END`);
  }
}

function settledExecutionSummary(roundId, now = Date.now()) {
  try {
    const version = executionDataVersion.get().data_version;
    if (_settledExecutionDataVersion !== version) {
      _settledExecutionCache.clear();
      _settledExecutionDataVersion = version;
    }
    const cached = _settledExecutionCache.get(roundId);
    if (cached && now >= cached.at && now - cached.at < SETTLED_EXECUTION_CACHE_MS) {
      _settledExecutionCacheStats.hits++;
      return { ...cached.summary };
    }
    _settledExecutionCache.delete(roundId);
  } catch {
    // A failed revision read must never hand back an older successful proof.
    _settledExecutionCache.clear();
    return { ok: false, reason: 'execution proof revision unavailable' };
  }
  _settledExecutionCacheStats.misses++;
  const summary = computeSettledExecutionSummary(roundId);
  // Failures are cheap to report and should be retried after transient faults.
  if (summary.ok) {
    while (_settledExecutionCache.size >= SETTLED_EXECUTION_CACHE_MAX) {
      _settledExecutionCache.delete(_settledExecutionCache.keys().next().value);
    }
    _settledExecutionCache.set(roundId, { at: now, summary: Object.freeze({ ...summary }) });
  }
  return summary;
}

function computeSettledExecutionSummary(roundId) {
  let code = 500;
  let body = null;
  const sink = {
    writeHead(c) { code = c; },
    end(raw) { try { body = JSON.parse(raw); } catch { body = null; } },
  };
  try { compVerify({}, sink, new URL(`http://local/verify?round=${encodeURIComponent(roundId)}`)); }
  catch (e) { return { ok: false, reason: `verification unavailable: ${e.message}` }; }
  if (code !== 200 || !body || !body.execution) {
    return { ok: false, reason: body && body.error ? body.error : 'execution proof unavailable' };
  }
  if (body.execution.verified) return { ok: true };
  const failed = Object.entries(body.execution.checks || {})
    .filter(([, value]) => !value).map(([name]) => name);
  return { ok: false, reason: failed.length
    ? `failed checks: ${failed.join(', ')}` : 'execution proof incomplete' };
}

function buildCompState() {
  /* A public read can arrive before a delayed timer callback. Advance first so
     the returned phase and its boundary side effects are one state, never
     `hot` with no revealed market or `boost` before the bankroll froze. */
  try { comp.advanceRoundClock(Date.now()); } catch { /* state reports block */ }
  const p = comp.phaseNow();
  if (!p) {
    _compRankSymbols.clear();
    /* Not live, but possibly counting down. The wall and the desk both need
       the pending start, or the room stares at "nothing running" while a
       schedule is ticking. */
    let pending = null;
    let armed = [];
    try {
      /* An armed round remains operator truth even when it has no countdown.
         In particular, restart recovery deliberately clears start_at and blocks
         the round when the memory-only draw seed is gone. Returning only rows
         with start_at made that accepted recovery state invisible to the one
         surface that can force-abort and re-arm it. Never include invite tokens. */
      const rows = comp.__test.db.prepare(
        "SELECT id, kind, start_at, blocked_reason, boost_leverage, boost_capacity_policy, backup_execution_policy, created_at, solo, series, stage, advance, speed, format_version FROM paper_rounds WHERE status = 'armed' ORDER BY created_at, id").all();
      armed = rows.map((row) => ({
        id: row.id,
        kind: row.kind,
        /* The field, for the pre-show screen. Names and pictures only: these
           are about to be on a wall in front of an audience, and the invite
           token never leaves the desk. */
        players: (() => {
          try {
            return comp.playersOf(row.id).map((p) => ({
              seat: p.seat,
              name: p.display_name || null,
              avatar: p.avatar_url || null,
              claimed: p.claimed_at != null,
            }));
          } catch { return []; }
        })(),
        startAt: row.start_at || null,
        blockedReason: row.blocked_reason || null,
        boostLeverage: row.boost_leverage || null,
        boostCapacityPolicy: comp.boostCapacityPolicyOf(row),
        ...comp.backupExecutionPolicyFields(row),
        formatVersion: Number(row.format_version) || 1,
        solo: !!row.solo,
        speed: Number(row.speed) || 1,
        stage: row.stage || null,
        advance: row.advance || null,
      }));
      const row = rows.find((x) => x.start_at !== null && x.start_at !== undefined);
      if (row) pending = { round: row.id, startAt: row.start_at, inMs: Math.max(0, row.start_at - Date.now()) };
    } catch { /* armed state is useful to show, never worth failing state for */ }
    /* THE TEN SECONDS AFTER THE BELL. Settling flips live:false, and every
       surface went blank at the exact moment the host asks "so who won?" —
       the desk showed "Nothing running" and the wall lost the board. Publish
       the last finished round's FROZEN result for a while afterwards: the
       standings as settled, the First Five winner, whether Hot fell back to
       the backup, and any operator settle deviation, which the room must
       hear about before anyone tweets a number. Read from stored rows, so
       this is the same result the audit holds, not a recomputation. */
    let lastRound = null;
    try {
      const done = comp.__test.db.prepare(
        /* A round the operator stopped is still the last thing that happened
           to the people in it. It has no result, but their screens must say
           so rather than stay silent: aborted rounds that had started are
           published too, flagged, for the same window. */
        "SELECT * FROM paper_rounds WHERE ((status = 'done' AND ends_at IS NOT NULL) OR (status = 'aborted' AND started_at IS NOT NULL)) AND (series IS NULL OR series NOT LIKE '%~archived-%') ORDER BY CASE WHEN status = 'done' THEN ends_at ELSE updated_at END DESC LIMIT 1").get();
      const doneAt = done ? (done.status === 'done' ? done.ends_at : done.updated_at) : 0;
      /* Held from the moment it SETTLED, not from the bell: a round whose bell
         blocked and was recovered twelve minutes later still deserves its
         ten minutes on every screen. */
      const settledAt = done ? Math.max(doneAt, Number(done.updated_at) || 0) : 0;
      if (done && Date.now() - settledAt < LAST_ROUND_SHOW_MS) {
        const nameOf = (uid) => {
          const row = comp.playersOf(done.id).find((x) => String(x.user_id) === String(uid));
          return row ? row.display_name : null;
        };
        let board = comp.standings(done.id, 'final').map((b) => ({
          userId: b.user_id, name: nameOf(b.user_id), rank: b.rank,
          score: b.score, accountPnl: b.account_pnl, hotBonus: b.hot_bonus,
        }));
        if (!board.length && done.status === 'aborted') {
          board = comp.playersOf(done.id).filter((p) => p.claimed_at != null).map((p, i) => ({
            userId: p.user_id, name: p.display_name || null, rank: i + 1, score: 0, accountPnl: 0, hotBonus: 0,
          }));
        }
        /* EACH PLAYER'S OWN ROUND. The wall shows the board; a player's
           screen after the bell should show what THEY did: how many trades,
           the best and the worst, where they finished, and whether they go
           through. Computed here from the fills the audit holds, so every
           screen shows the same numbers. Engine units; the client scales. */
        const recap = {};
        const fillsQ = comp.__test.db.prepare(
          /* Realised AS THE ACCOUNT FELT IT: an isolated leg that gapped past
             its margin lost the margin, no more; bad_debt holds the clamp and
             the score was built on the clamped figure. The window ends AT the
             bell: a click a second after it fills, but it never scored. */
          "SELECT symbol, side, kind, (realized_pnl + COALESCE(bad_debt, 0)) AS realized_pnl, notional, price, ts FROM paper_fills WHERE user_id = ? AND ts >= ? AND ts <= ? AND kind != 'FUNDING' ORDER BY ts");
        for (const b of board) {
          try {
            const fl = fillsQ.all(b.userId, done.started_at || 0, doneAt);
            const closes = fl.filter((x) => x.realized_pnl != null);
            const pick = (arr, cmp) => arr.reduce((acc, x) => (acc == null || cmp(x.realized_pnl, acc.realized_pnl) ? x : acc), null);
            const best = pick(closes, (a, c) => a > c);
            const worst = pick(closes, (a, c) => a < c);
            const shape = (x) => (x ? { symbol: x.symbol, side: x.side, pnl: x.realized_pnl, notional: x.notional, at: x.ts } : null);
            /* Where the score came from, and what the bell closed for them:
               the Boost twins are settled by the engine, not the trader, so
               the recap has to say what happened to them and at what price. */
            const sum = (arr) => arr.reduce((acc, x) => acc + (Number(x.realized_pnl) || 0), 0);
            const hotPnl = Number(b.hotBonus) || 0;
            const boostPnl = sum(closes.filter((x) => /-BOOST$/.test(x.symbol)));
            const settled = closes
              .filter((x) => /-BOOST$/.test(x.symbol) && (x.kind === 'SEGMENT' || x.kind === 'EXPIRY'))
              .map((x) => ({ symbol: x.symbol, side: x.side, pnl: x.realized_pnl, notional: x.notional, price: x.price, at: x.ts, kind: x.kind }));
            recap[b.userId] = {
              trades: fl.length, closes: closes.length,
              best: shape(best), worst: shape(worst),
              rank: b.rank, of: board.length, score: b.score,
              accountPnl: b.accountPnl, hotBonus: b.hotBonus, hotPnl, boostPnl, settled,
              through: done.advance ? b.rank <= done.advance : null,
            };
          } catch { /* a recap is a courtesy */ }
        }
        /* Who goes on, by name, so no screen has to re-derive the cut. */
        const through = done.advance && done.status === 'done' ? board.filter((b) => b.rank <= done.advance).map((b) => ({ userId: b.userId, name: b.name })) : null;
        const out = done.advance && done.status === 'done' ? board.filter((b) => b.rank > done.advance).map((b) => ({ userId: b.userId, name: b.name })) : null;
        let drawReveal = null;
        try { drawReveal = done.draw_reveal_json ? JSON.parse(done.draw_reveal_json) : null; } catch {}
        const executionVerified = done.draw_reveal_json
          ? settledExecutionSummary(done.id)
          : { ok: false, reason: 'draw and execution proof are sealed' };
        lastRound = {
          id: done.id, kind: done.kind, endedAt: doneAt, startedAt: done.started_at || null,
          aborted: done.status === 'aborted', advance: done.advance || null, through, out, settledAt,
          stage: done.stage || null, solo: !!done.solo, recap,
          formatVersion: Number(done.format_version) || 1,
          boostCapacityPolicy: comp.boostCapacityPolicyOf(done),
          ...comp.backupExecutionPolicyFields(done),
          hots: settledHots(done),
          drawCommit: done.draw_reveal_json ? done.draw_commit : null,
          drawReveal,
          drawVerified: done.draw_reveal_json ? comp.verifyDraw(done) : null,
          executionVerified,
          settleDeviation: done.settle_deviation || null,
          board,
        };
      }
    } catch { /* the result is a courtesy; never fail state for it */ }
    return { ok: true, live: false, now: Date.now(), pending, armed, lastRound, wall: showState(pending) };
  }
  const r = p.round;
  const rank = compRankSnapshot(p);
  const control = safePhaseControl(p, true);
  const roster = new Map(comp.playersOf(r.id).map((pl) => [String(pl.user_id), pl]));
  const players = rank.players.map((scored) => {
    const pl = roster.get(String(scored.userId));
    /* Tell the wall which markets are being traded without publishing a
       contestant's direction, exposure, entry, mark or live PnL. Keep the
       largest holding first for the wall, but discard that private notional
       before the public state leaves the engine. */
    const positions = stmt.posByUser.all(scored.userId).map((pos) => {
      const mk = posMarkOf(pos);
      return {
        notional: r6(pos.size * mk),
        public: {
          segment: aliasKind(pos.symbol),         // HOT | BOOST | null
          symbol: pos.symbol,
        },
      };
    }).sort((a, b) => b.notional - a.notional).map((pos) => pos.public);
    return {
      ...scored, avatar: (pl && pl.avatar_url) || null, positions,
    };
  });
  /* Unscoreable seats are held out of the ranking rather than sorted as if
     their score were zero, and reported separately so the wall can say so. */
  const scored = players;
  const failed = rank.unscored.map((x) => ({
    ...x, avatar: (roster.get(String(x.userId)) || {}).avatar_url || null, positions: [],
  }));
  const issuedAt = Date.now();
  const roundExecution = publicRoundExecution(r, _compBoardRevision, issuedAt);
  return {
    ok: true, live: true, now: issuedAt,
    ...(roundExecution ? { engineBoot: ENGINE_BOOT_ID, compRevision: _compBoardRevision,
      roundExecution } : {}),
    /* Published while live too, because `technical` is the one stage that
       must be able to cover a running round, and because the desk has to be
       able to see that the wall is covered. */
    wall: showState(null),
    /* boostLeverage comes from the ROUND, not the deployed constant, so the
       wall shows the rule this round is actually being played under. */
    round: {
      id: r.id, kind: r.kind, startedAt: r.started_at, endsAt: r.ends_at,
      formatVersion: Number(r.format_version) || 1,
      pricePolicy: typeof comp.pricePolicyOf === 'function' ? comp.pricePolicyOf(r) : 'strict',
      boostLeverage: r.boost_leverage || HEAT_MAX_LEV,
      boostCapacityPolicy: comp.boostCapacityPolicyOf(r),
      ...comp.backupExecutionPolicyFields(r),
      plan: publicRoundPlan(r),
      drawCommit: r.draw_commit,
      /* A practice round says so, everywhere, unconditionally. Every surface
         that can show a round can also be pointed at a rehearsal, and a
         rehearsal that looks like a show is the one way this feature could
         do damage. */
      solo: !!r.solo, speed: Number(r.speed) || 1,
      /* Any operator settle deviation, verbatim and permanent. Published
         because a deviation nobody can see is indistinguishable from
         tampering. Null on every round where nothing was overridden. */
      settleDeviation: r.settle_deviation || null,
    },
    drawCommit: r.draw_commit,
    drawReveal: null,
    ...control,
    // Report the GATE, not the clock. The order path accepts a boost only if
    // the twin is actually open, so if a boundary fired late or a market
    // failed to open, the wall must say what the engine will really do rather
    // than what the schedule intended.
    // the wall must never look normally live while the round is stopped
    blocked: !!r.blocked_reason,
    /* The REASON, not just the fact. The desk is the surface that has to act
       on a block and it was being told only that one existed. */
    blockedReason: r.blocked_reason || null,
    /* Which markets are individually frozen right now. The round is NOT
       paused for these; they are simply not tradable until they quote again,
       and the desk and the wall have to be able to say which. */
    frozenMarkets: [...frozenMarkets().entries()]
      .filter(([sym]) => !holdsRoundPrices(r) || !roundPriceReady(sym, 0, issuedAt, r))
      .map(([sym, f]) => ({ symbol: sym, since: f.since, why: f.why })),
    /* Boost is one shared window, so a market that could not open is a
       difference between players, not a footnote. Named here so the wall and
       the strip can say which ones are live instead of quietly offering three
       of four. */
    /* Promised, opened and missing are three separate facts. Computing
       "unavailable" from the opened set could only ever return nothing. */
    boostConfigured: (() => { try { return JSON.parse(r.boost_configured || 'null') || comp.boostMarketsOf(r); } catch { return comp.boostMarketsOf(r); } })(),
    /* What each open Boost market can actually carry right now.
       ONE helper, the same one the order path uses. This computed only the
       live safety cap and ignored the round's armed cap, so a round armed at
       250x told the wall and every viewer 500x while the engine correctly
       refused anything above 250. The order engine did the safe thing and the
       broadcast lied. */
    boostCaps: control.boostCaps,
    boostUnavailable: p.phase === 'boost'
      ? (() => {
        let promised = [];
        try { promised = JSON.parse(r.boost_configured || 'null') || comp.boostMarketsOf(r); } catch { promised = comp.boostMarketsOf(r); }
        return promised.filter((m) => !aliasOpen(m + '-BOOST'));
      })()
      : [],
    players: scored,
    // present and non-empty means the board is incomplete: show it, do not rank it
    unscored: failed,
    /* Which markets the board is currently unable to price strictly. A frozen
       score is not a wrong score, but a board that looks settled while the
       chart beside it keeps moving is a wrong PICTURE, and the public feed
       does keep moving: it is allowed to show single-source ticks that
       competition pricing refuses. */
    /* The wall must be able to say PAUSED rather than looking normal while
       every contestant is frozen. */
    stalePricing: rank.stalePricing,
    /* A board is complete only when every seat scored AND every price behind
       those scores was competition-valid. */
    complete: rank.complete && !_compCauseOverflow,
    causesComplete: !_compCauseOverflow,
  };
}

/* Public draw verification. Deliberately unauthenticated: the whole point of
 * commit-reveal is that anyone in the room can check it on their phone. */
function compVerify(req, res, u) {
  const id = u.searchParams.get('round') || '';
  const r = comp.__test.q.get.get(id);
  if (!r) return send(res, 404, { ok: false, error: 'no such round' });
  if (Number(r.format_version) >= 2) {
    const publishable = r.status === 'done' || (r.status === 'aborted' && r.started_at);
    if (!publishable || !r.draw_reveal_json) {
      return send(res, 409, {
        ok: false, error: 'draw_not_revealed', round: r.id,
        commit: r.status === 'running' ? r.draw_commit : null,
      });
    }
    let reveal = null;
    try { reveal = JSON.parse(r.draw_reveal_json); } catch {}
    const canonicalJson = JSON.stringify(reveal);
    const recomputedCommit = crypto.createHash('sha256').update(canonicalJson, 'utf8').digest('hex');
    const plan = reveal && reveal.plan || {};
    const boundaryLedger = comp.__test.q.bAll.all(r.id).map((row) => ({
      offsetMs: Number(row.at), dueWallAt: row.due_wall_at == null ? null : Number(row.due_wall_at),
      status: row.status, ranAt: Number(row.ran_at),
    }));
    const boundaryAt = (offset) => boundaryLedger.find((row) => row.offsetMs === Number(offset)) || null;
    /* Clock proof is derived from its own append-only interval ledger. It
       must never infer a pause from the very boundary timestamps it is meant
       to verify: doing so makes any forged due time algebraically pass. Price
       pauses and engine-restart downtime both enter this one ledger. */
    let rawPauses = [];
    try { rawPauses = comp.__test.q.clockPauses.all(r.id); } catch { rawPauses = []; }
    let pauseLedgerValid = true;
    const mergedPauses = [];
    for (const row of rawPauses) {
      const startedAt = Number(row.started_at);
      const restoredAt = row.ended_at == null ? NaN : Number(row.ended_at);
      if (!(Number.isFinite(startedAt) && Number.isFinite(restoredAt)
          && startedAt >= Number(r.started_at) && restoredAt >= startedAt)) {
        pauseLedgerValid = false;
        continue;
      }
      const last = mergedPauses[mergedPauses.length - 1];
      if (last && startedAt <= last.restoredAt) last.restoredAt = Math.max(last.restoredAt, restoredAt);
      else mergedPauses.push({ startedAt, restoredAt, sources: [] });
      const target = mergedPauses[mergedPauses.length - 1];
      if (row.source && !target.sources.includes(String(row.source))) target.sources.push(String(row.source));
    }
    for (const row of mergedPauses) row.durationMs = row.restoredAt - row.startedAt;
    const pausedBetween = (from, to) => {
      const lo = Number(from), hi = Number(to);
      if (!(Number.isFinite(lo) && Number.isFinite(hi) && hi >= lo)) return null;
      return mergedPauses.reduce((sum, p) =>
        sum + Math.max(0, Math.min(hi, p.restoredAt) - Math.max(lo, p.startedAt)), 0);
    };
    const activeAtBoundary = (row) => {
      if (!row || !Number.isFinite(row.dueWallAt)) return null;
      const paused = pausedBetween(Number(r.started_at), row.dueWallAt);
      return Number.isFinite(paused)
        ? row.dueWallAt - Number(r.started_at) - paused : null;
    };
    const activeAtWall = (wallAt) => {
      const ts = Number(wallAt);
      if (!Number.isFinite(ts)) return null;
      const paused = pausedBetween(Number(r.started_at), ts);
      return Number.isFinite(paused) ? ts - Number(r.started_at) - paused : null;
    };
    /* Recompute the exact-boundary verdict from the captured observation.
       Merely trusting attempt.ready would let a database edit turn any later
       fallback into the alleged first valid choice. Evidence is mandatory,
       names the policy/build used, and has to contain either a valid accepted
       mark spanning the activation instant or a concrete rejection fact. */
    const attemptEvidenceValidAt = (attempt, activationAt, kind, requiredLeverage) => {
      if (!attempt || typeof attempt !== 'object' || !attempt.evidence
          || typeof attempt.evidence !== 'object') return false;
      const e = attempt.evidence;
      const policy = e.policy;
      if (holdsRoundPrices(r)) {
        return roundAttemptEvidenceValid(attempt, activationAt, kind, requiredLeverage, r);
      }
      if (!policy || policy.version !== 'competition-price-v2' || policy.kind !== kind
          || !Object.prototype.hasOwnProperty.call(policy, 'buildId')
          || Number(policy.leverageRequired) !== Number(requiredLeverage)
          || !(Number(policy.maximumHistoricalAgeMs) > 0)
          || Number(e.checkedAt) !== Number(activationAt)) return false;
      const o = e.observation;
      const observationSpans = !!(o && o.accepted === true
        && Number(o.mark) > 0
        && Number.isFinite(Number(o.observedAt))
        && Number(o.observedAt) <= Number(activationAt)
        && Number(activationAt) < Number(o.validUntil)
        && Number(activationAt) - Number(o.observedAt)
          <= Number(policy.maximumHistoricalAgeMs)
        && Number(o.leverageCap) >= Number(requiredLeverage));
      const invalidity = e.invalidity;
      const invalidAtActivation = !!(invalidity
        && Number.isFinite(Number(invalidity.since))
        && Number(invalidity.since) <= Number(activationAt)
        && String(invalidity.reason || '').length > 0);
      const boundaryIdentity = observationSpans && Number(e.boundaryMark) > 0
        && Number(e.boundaryMark) === Number(o.mark);
      const recomputedReady = boundaryIdentity && !invalidAtActivation;
      const concreteRejection = recomputedReady || (!!String(e.rejectReason || '')
        && (!o || o.accepted !== true
          || Number(o.observedAt) > Number(activationAt)
          || Number(o.validUntil) <= Number(activationAt)
          || Number(o.leverageCap) < Number(requiredLeverage)
          || invalidAtActivation));
      return concreteRejection
        && !!attempt.ready === recomputedReady
        && !!e.ready === recomputedReady
        && !!e.historicalReady === recomputedReady;
    };
    const pausedBefore = (row) => row && Number.isFinite(row.dueWallAt)
      ? pausedBetween(Number(r.started_at), row.dueWallAt) : null;
    const measuredActiveBetween = (from, to) => {
      if (!from || !to || !Number.isFinite(from.dueWallAt) || !Number.isFinite(to.dueWallAt)) return null;
      const paused = pausedBetween(from.dueWallAt, to.dueWallAt);
      return Number.isFinite(paused) ? (to.dueWallAt - from.dueWallAt) - paused : null;
    };
    const hotExecution = [1, 2].map((number) => {
      const rows = comp.__test.q.hotRows.all(r.id, number);
      const resolutionRow = comp.__test.q.hotResolution.get(r.id, number);
      let resolution = null;
      try { resolution = resolutionRow ? JSON.parse(resolutionRow.evidence_json) : null; } catch {}
      const starts = rows.map((x) => Number(x.started_at)).filter(Number.isFinite);
      const ends = rows.map((x) => Number(x.ended_at)).filter(Number.isFinite);
      const drawn = r[`hot${number}_base`] || null;
      const active = r[`hot${number}_active_base`] || null;
      const committed = reveal && reveal.draw && reveal.draw[`hot${number}`] || {};
      const startBoundary = boundaryAt(committed.activation);
      const endBoundary = boundaryAt(Number(committed.activation) + Number(plan.hotDuration));
      const priorActive = number === 2 ? (r.hot1_active_base || r.hot1_base) : null;
      const committedOrder = [committed.asset, ...(Array.isArray(committed.fallbackOrder)
        ? committed.fallbackOrder : [])].filter((x, i, a) => x && a.indexOf(x) === i);
      const expectedSkipped = priorActive && committedOrder.includes(priorActive)
        ? [{ asset: priorActive, reason: 'already_used_hot1' }] : [];
      const ordered = committedOrder.filter((x) => x !== priorActive);
      const attempts = resolution && Array.isArray(resolution.attempts) ? resolution.attempts : [];
      const selectedIndex = ordered.indexOf(active);
      const attemptEvidenceValid = selectedIndex >= 0
        && attempts.length === selectedIndex + 1
        && attempts.every((attempt, i) => attempt && attempt.asset === ordered[i]
          && !!attempt.ready === (i === selectedIndex)
          && attemptEvidenceValidAt(attempt, Number(resolutionRow.activation_at),
            'HOT', comp.COMP_BASE_LEV))
        && attempts.slice(0, -1).every((attempt) => attempt.ready === false)
        && attempts[attempts.length - 1] && attempts[attempts.length - 1].ready === true;
      const resolutionMatchesRuntime = !!(resolutionRow && resolution
        && resolutionRow.drawn === committed.asset
        && resolutionRow.drawn === drawn
        && resolutionRow.active === active
        && resolution.selected === active
        && Number(resolution.activationAt) === Number(resolutionRow.activation_at)
        && activeAtWall(resolutionRow.activation_at) === Number(committed.activation)
        && JSON.stringify(resolution.committedOrder) === JSON.stringify(committedOrder)
        && JSON.stringify(resolution.eligibleOrder) === JSON.stringify(ordered)
        && JSON.stringify(resolution.skipped) === JSON.stringify(expectedSkipped)
        && (active === drawn
          ? resolution.selectionCause === 'drawn' && !resolution.fallbackReason
          : expectedSkipped.some((x) => x.asset === drawn)
            ? resolution.selectionCause === 'already_used_hot1'
              && /already ran as Hot Market 1/.test(String(resolution.fallbackReason || ''))
            : resolution.selectionCause === 'invalid_at_activation'
              && /not competition-ready at activation/.test(String(resolution.fallbackReason || '')))
        && String(resolution.fallbackReason || '') === String(r[`hot${number}_fallback_reason`] || '')
        && rows.length > 0 && rows.every((row) => row.base === active));
      const activeDurationMs = measuredActiveBetween(startBoundary, endBoundary);
      const timestampsMatchBoundaries = !!(startBoundary && endBoundary
        && Number.isFinite(startBoundary.dueWallAt) && Number.isFinite(endBoundary.dueWallAt)
        && rows.length && rows.every((x) => Number(x.started_at) === startBoundary.dueWallAt
          && Number(x.ended_at) === endBoundary.dueWallAt));
      return {
        number, drawn, active,
        usedFallback: !!(drawn && active && drawn !== active),
        fallbackReason: r[`hot${number}_fallback_reason`] || null,
        revealedAt: r[`hot${number}_revealed_at`] || null,
        startedAt: starts.length ? Math.min(...starts) : null,
        endedAt: ends.length ? Math.max(...ends) : null,
        activeDurationMs,
        wallDurationMs: starts.length && ends.length ? Math.max(...ends) - Math.min(...starts) : null,
        clockPausedMs: startBoundary && endBoundary
          ? pausedBefore(endBoundary) - pausedBefore(startBoundary) : null,
        committedOrderIndex: active ? ordered.indexOf(active) : -1,
        resolution: resolution ? {
          activationAt: Number(resolutionRow.activation_at),
          committedOrder: resolution.committedOrder,
          eligibleOrder: resolution.eligibleOrder,
          skipped: resolution.skipped,
          attempts,
          selected: resolution.selected,
          selectionCause: resolution.selectionCause,
        } : null,
        resolutionMatchesRuntime,
        attemptEvidenceValid,
        timestampsMatchBoundaries,
        seats: rows.length,
        complete: rows.length > 0 && ends.length === rows.length,
      };
    });
    const boostStartBoundary = boundaryAt(plan.boostStart);
    const bellBoundary = boundaryAt(plan.total);
    let boostOpened = [];
    try { boostOpened = JSON.parse(r.boost_opened || '[]'); } catch { boostOpened = []; }
    const boostResolutionRow = comp.__test.q.boostResolution.get(r.id);
    let boostResolution = null;
    try { boostResolution = boostResolutionRow
      ? JSON.parse(boostResolutionRow.evidence_json) : null; } catch { boostResolution = null; }
    let boostConfigured = [];
    try { boostConfigured = JSON.parse(r.boost_configured || r.boost_markets || '[]'); }
    catch { boostConfigured = []; }
    const boostAttempts = boostResolution && Array.isArray(boostResolution.attempts)
      ? boostResolution.attempts : [];
    const boostSelected = boostResolution && Array.isArray(boostResolution.selected)
      ? boostResolution.selected : [];
    const boostEvidenceValid = !!(boostResolutionRow && boostResolution
      && Number(boostResolution.activationAt) === Number(boostResolutionRow.activation_at)
      && activeAtWall(boostResolutionRow.activation_at) === Number(plan.boostStart)
      && JSON.stringify(boostResolution.configured) === JSON.stringify(boostConfigured)
      && boostAttempts.length === boostConfigured.length
      && boostAttempts.every((attempt, i) => attempt && attempt.asset === boostConfigured[i]
        && attemptEvidenceValidAt(attempt, Number(boostResolutionRow.activation_at),
          'BOOST', Number(r.boost_leverage) || HEAT_MAX_LEV))
      && JSON.stringify(boostSelected) === JSON.stringify(boostAttempts
        .filter((attempt) => attempt.ready).map((attempt) => attempt.asset))
      && JSON.stringify(boostOpened) === JSON.stringify(boostSelected));
    const roundPlayers = comp.playersOf(r.id);
    const frozenAt = roundPlayers.map((p) => Number(p.boost_frozen_at)).filter(Number.isFinite);
    const freezeProofs = comp.__test.q.boostProofs.all(r.id);
    const freezeByUser = new Map(freezeProofs.map((row) => [String(row.user_id), row]));
    const boostLeverage = Number(r.boost_leverage) || HEAT_MAX_LEV;
    const closeEnough = (a, b) => Number.isFinite(Number(a)) && Number.isFinite(Number(b))
      && Math.abs(Number(a) - Number(b)) <= 1e-6;
    const boostFreezeProofValid = roundPlayers.length > 0
      && freezeProofs.length === roundPlayers.length
      && roundPlayers.every((player) => {
        const proof = freezeByUser.get(String(player.user_id));
        if (!proof) return false;
        const bankroll = Number(player.boost_bankroll);
        const maxExposure = Number(player.boost_max_exposure);
        let marksValid = false;
        try { const marks = JSON.parse(proof.marks_json); marksValid = !!marks && typeof marks === 'object' && !Array.isArray(marks); }
        catch { marksValid = false; }
        return marksValid
          && Number(proof.frozen_at) === Number(player.boost_frozen_at)
          && Number(proof.bankroll) === bankroll
          && Number(proof.max_exposure) === maxExposure
          && bankroll >= 0
          && maxExposure === Math.max(0, bankroll) * boostLeverage;
      });
    const scoreProofRows = comp.__test.q.scoreProofs.all(r.id);
    const expectedProofCheckpoints = ['hot1Start', 'hot1End', 'hot2Start',
      'hot2End', 'boostStart', 'final'];
    const scoreProofByKey = new Map();
    let scoreProofIdentityValid = scoreProofRows.length
      === roundPlayers.length * expectedProofCheckpoints.length;
    let scoreProofStateMathValid = true;
    let scoreProofFillDigestsValid = true;
    const parsedScoreProofs = [];
    const recomputeFrozenState = (proof) => {
      const state = proof && proof.state;
      if (!state || state.schema !== 'competition-score-input-v1'
          || !Object.prototype.hasOwnProperty.call(state, 'buildId')
          || !state.account || !Array.isArray(state.positions)
          || !Number.isFinite(Number(state.account.balance))) return null;
      if (!comp.boostCapacityPolicyMatches(state.boostCapacityPolicy, r)
          || !comp.backupExecutionPolicyMatches(state.backupExecutionPolicy, r)) return null;
      if (!roundScoreEvidenceValid(state, Number(proof.asOf), r)) return null;
      const symbols = new Set();
      let equity = Number(state.account.balance);
      for (const pos of state.positions) {
        if (!pos || typeof pos.symbol !== 'string' || symbols.has(pos.symbol)
            || !['LONG', 'SHORT'].includes(pos.side)
            || !(Number(pos.size) > 0) || !(Number(pos.entryPrice) > 0)
            || !(Number(pos.mark) > 0) || !(Number(pos.leverage) > 0)
            || !['cross', 'isolated'].includes(pos.marginMode)) return null;
        symbols.add(pos.symbol);
        const raw = Number(pos.size) * (Number(pos.mark) - Number(pos.entryPrice))
          * (pos.side === 'LONG' ? 1 : -1);
        const margin = Number(pos.isolatedMargin) || 0;
        const contribution = pos.marginMode === 'isolated'
          ? isolatedSettlementValue(margin, raw) : raw;
        if (!closeEnough(pos.rawUpnl, raw)
            || !closeEnough(pos.upnl, r6(raw))
            || !closeEnough(pos.equityContribution, r6(contribution))) return null;
        equity += contribution;
      }
      return r6(equity);
    };
    const proofAssetValue = (proof, base) => {
      const state = proof && proof.state;
      if (!state || !state.fillLedger || !Array.isArray(state.positions)) return null;
      let value = Number((state.fillLedger.realizedByBase || {})[base]) || 0;
      for (const pos of state.positions) {
        if (baseOf(pos.symbol) !== base) continue;
        const raw = Number(pos.size) * (Number(pos.mark) - Number(pos.entryPrice))
          * (pos.side === 'LONG' ? 1 : -1);
        value += pos.marginMode === 'isolated'
          ? Math.max(raw, -(Number(pos.isolatedMargin) || 0)) : raw;
      }
      return r6(value);
    };
    for (const row of scoreProofRows) {
      let proof = null;
      try { proof = JSON.parse(row.proof_json); } catch { proof = null; }
      const key = `${row.user_id}:${row.checkpoint}`;
      const wrapperValid = !!(proof && proof.version === 1
        && proof.roundId === r.id && Number(proof.userId) === Number(row.user_id)
        && comp.boostCapacityPolicyMatches(proof.boostCapacityPolicy, r)
        && comp.backupExecutionPolicyMatches(proof.backupExecutionPolicy, r)
        && proof.checkpoint === row.checkpoint
        && Number(proof.asOf) === Number(row.as_of)
        && Number(proof.epoch) === Number(row.epoch)
        && Number(proof.startBalance) === Number(row.start_balance)
        && expectedProofCheckpoints.includes(row.checkpoint)
        && crypto.createHash('sha256').update(row.proof_json, 'utf8').digest('hex')
          === row.proof_sha256 && !scoreProofByKey.has(key));
      if (!wrapperValid) scoreProofIdentityValid = false;
      const frozenEquity = proof ? recomputeFrozenState(proof) : null;
      if (!Number.isFinite(frozenEquity)
          || !closeEnough(frozenEquity, proof && proof.state && proof.state.equity)) {
        scoreProofStateMathValid = false;
      }
      let currentFillEvidence = null;
      try {
        const watermark = proof && proof.state && proof.state.fillLedger
          && Number(proof.state.fillLedger.watermarkId);
        currentFillEvidence = fillLedgerEvidence(Number(row.user_id),
          Number(row.epoch), Number(row.as_of), watermark);
      } catch { currentFillEvidence = null; }
      const fillDigestValid = !!(proof && proof.state && currentFillEvidence
        && JSON.stringify(proof.state.fillLedger) === JSON.stringify(currentFillEvidence));
      if (!fillDigestValid) scoreProofFillDigestsValid = false;
      if (wrapperValid) scoreProofByKey.set(key, proof);
      parsedScoreProofs.push({ row, proof, frozenEquity, fillDigestValid });
    }
    for (const player of roundPlayers) {
      for (const checkpoint of expectedProofCheckpoints) {
        if (!scoreProofByKey.has(`${player.user_id}:${checkpoint}`)) {
          scoreProofIdentityValid = false;
        }
      }
    }
    let hotEconomicDeltasRecomputed = true;
    for (const number of [1, 2]) {
      for (const row of comp.__test.q.hotRows.all(r.id, number)) {
        const startProof = scoreProofByKey.get(`${row.user_id}:hot${number}Start`);
        const endProof = scoreProofByKey.get(`${row.user_id}:hot${number}End`);
        const startValue = proofAssetValue(startProof, row.base);
        const endValue = proofAssetValue(endProof, row.base);
        if (!startProof || !endProof
            || Number(startProof.asOf) !== Number(row.started_at)
            || Number(endProof.asOf) !== Number(row.ended_at)
            || !startProof.economic || !endProof.economic
            || startProof.economic.kind !== 'hot' || endProof.economic.kind !== 'hot'
            || startProof.economic.edge !== 'start' || endProof.economic.edge !== 'end'
            || startProof.economic.base !== row.base || endProof.economic.base !== row.base
            || !closeEnough(startValue, row.start_value)
            || !closeEnough(endValue, row.end_value)
            || !closeEnough(Number(endValue) - Number(startValue), row.bonus)
            || !closeEnough(startProof.economic.value, row.start_value)
            || !closeEnough(endProof.economic.value, row.end_value)) {
          hotEconomicDeltasRecomputed = false;
        }
      }
    }
    const boostEquityRecomputed = roundPlayers.length > 0 && roundPlayers.every((player) => {
      const proof = scoreProofByKey.get(`${player.user_id}:boostStart`);
      const freeze = freezeByUser.get(String(player.user_id));
      const equity = proof && recomputeFrozenState(proof);
      return !!(proof && freeze && proof.economic
        && proof.economic.kind === 'boost-freeze'
        && Number(proof.asOf) === Number(freeze.frozen_at)
        && closeEnough(equity, freeze.bankroll)
        && closeEnough(proof.economic.bankroll, freeze.bankroll)
        && closeEnough(proof.economic.maxExposure, freeze.max_exposure)
        && Number(proof.economic.leverage) === boostLeverage);
    });
    const finalScores = comp.__test.q.scores.all(r.id, 'final');
    const playerByUser = new Map(roundPlayers.map((p) => [String(p.user_id), p]));
    const hotBonusByUser = new Map(roundPlayers.map((p) => [String(p.user_id), 0]));
    let hotCheckpointAlgebraValid = true;
    for (const number of [1, 2]) {
      for (const row of comp.__test.q.hotRows.all(r.id, number)) {
        if (row.ended_at == null || !closeEnough(row.bonus,
          Number(row.end_value) - Number(row.start_value))) {
          hotCheckpointAlgebraValid = false;
        }
        const key = String(row.user_id);
        hotBonusByUser.set(key, (hotBonusByUser.get(key) || 0) + Number(row.bonus || 0));
      }
    }
    const markDocuments = new Set(finalScores.map((row) => String(row.marks || '')));
    const finalScoreInputsRecomputed = roundPlayers.length > 0
      && finalScores.length === roundPlayers.length
      && finalScores.every((row) => {
        const proof = scoreProofByKey.get(`${row.user_id}:final`);
        const equity = proof && recomputeFrozenState(proof);
        const ledger = proof && proof.state && proof.state.fillLedger;
        return !!(proof && proof.economic && proof.economic.kind === 'final-score'
          && Number(proof.asOf) === Number(row.scheduled_at)
          && closeEnough(equity, row.equity)
          && closeEnough(proof.economic.equity, row.equity)
          && closeEnough(proof.economic.accountPnl, row.account_pnl)
          && closeEnough(proof.economic.realized, row.realized)
          && closeEnough(proof.economic.hotBonus, row.hot_bonus)
          && closeEnough(proof.economic.score, row.score)
          && ledger
          && closeEnough(ledger.realizedBeforeBoundarySettlement, row.realized));
      });
    const finalScoreProofValid = roundPlayers.length > 0
      && finalScores.length === roundPlayers.length
      && markDocuments.size === 1
      && hotCheckpointAlgebraValid
      && finalScoreInputsRecomputed
      && finalScores.every((row) => {
        const player = playerByUser.get(String(row.user_id));
        if (!player || !Number.isFinite(Number(row.at))
            || Number(row.scheduled_at) !== Number(bellBoundary && bellBoundary.dueWallAt)
            || !closeEnough(row.score, Number(row.account_pnl) + Number(row.hot_bonus))
            || !closeEnough(row.equity, Number(player.start_balance) + Number(row.account_pnl))
            || !closeEnough(row.hot_bonus, hotBonusByUser.get(String(row.user_id)) || 0)
            || !Number.isFinite(Number(row.realized))) return false;
        try {
          const marks = JSON.parse(row.marks);
          return !!marks && typeof marks === 'object' && !Array.isArray(marks);
        } catch { return false; }
      });
    const expectedOffsets = comp.boundariesOf(r);
    const boundaryLedgerComplete = expectedOffsets.every((offset) => {
      const row = boundaryAt(offset);
      return row && row.status === 'succeeded' && Number.isFinite(row.dueWallAt);
    });
    const boundaryClockMatchesOffsets = boundaryLedgerComplete && boundaryLedger.every((row) =>
      row.status !== 'succeeded' || activeAtBoundary(row) === row.offsetMs);
    const recordedPauseMs = mergedPauses.reduce((sum, row) => sum + row.durationMs, 0);
    const executionChecks = {
      pauseLedgerComplete: pauseLedgerValid
        && recordedPauseMs === (Number(r.paused_ms) || 0),
      boundaryLedgerComplete,
      boundaryClockMatchesOffsets,
      hotDurationsExact: hotExecution.every((h) => h.complete && h.timestampsMatchBoundaries
        && h.activeDurationMs === Number(plan.hotDuration)),
      normalGapPreserved: Number(reveal && reveal.draw && reveal.draw.hot2.activation)
        - Number(plan.hotWarning)
        - (Number(reveal && reveal.draw && reveal.draw.hot1.activation) + Number(plan.hotDuration))
        >= Number(plan.normalGap),
      runtimeMarketsDistinct: !!(hotExecution[0].active && hotExecution[1].active
        && hotExecution[0].active !== hotExecution[1].active),
      resolutionUsesCommittedOrders: hotExecution.every((h) => h.committedOrderIndex >= 0
        && h.resolutionMatchesRuntime && h.attemptEvidenceValid),
      finalBuildAtLeastFiveMinutes: Number(plan.boostStart)
        - Math.max(...hotExecution.map((h, i) => Number(reveal.draw[`hot${i + 1}`].activation)
          + Number(plan.hotDuration))) >= (reveal.kind === 'rehearsal' ? 30_000 : 5 * 60_000)
            / (Number(reveal.speed) || 1),
      boostDurationExact: measuredActiveBetween(boostStartBoundary, bellBoundary)
        === Number(plan.total) - Number(plan.boostStart),
      boostFreezeMatchesStart: !!(boostStartBoundary && Number.isFinite(boostStartBoundary.dueWallAt)
        && frozenAt.length === comp.playersOf(r.id).length
        && frozenAt.every((at) => at === boostStartBoundary.dueWallAt)),
      boostResolutionProved: boostEvidenceValid,
      /* This proves the immutable freeze record agrees with the roster and
         the published leverage algebra. It intentionally does not claim an
         independent replay of historical account equity from mutable live
         account state. */
      boostFreezeRecordConsistent: boostFreezeProofValid,
      scoreInputCheckpointsImmutable: scoreProofIdentityValid,
      scoreInputStateRecomputed: scoreProofStateMathValid,
      fillLedgerDigestsMatchCheckpoints: scoreProofFillDigestsValid,
      hotEconomicDeltasRecomputed,
      boostEquityRecomputedFromCheckpoint: boostEquityRecomputed,
      finalEquityRecomputedFromCheckpoint: finalScoreInputsRecomputed,
      finalScoresBoundToBellAndHotDeltas: finalScoreProofValid,
      totalActiveDurationExact: bellBoundary && Number.isFinite(bellBoundary.dueWallAt)
        ? activeAtBoundary(bellBoundary) === Number(plan.total)
        : false,
    };
    const executionVerified = Object.values(executionChecks).every(Boolean);
    return send(res, 200, {
      ok: true, round: r.id, commit: r.draw_commit, reveal,
      canonical: {
        encoding: 'UTF-8', algorithm: 'SHA-256', json: canonicalJson,
        recomputedCommit, matches: recomputedCommit === r.draw_commit,
      },
      /* Runtime resolution is not part of the pre-round commitment: it says
         which committed fallback actually ran when a primary was invalid.
         It is permanent post-settlement audit data, unlike the short-lived
         lastRound recap. */
      execution: {
        startedAt: Number(r.started_at),
        clock: {
          pausedMs: Number(r.paused_ms) || 0,
          ledgerPausedMs: recordedPauseMs,
          recordedPauseIntervals: mergedPauses,
          boundaryLedger,
          totalActiveDurationMs: bellBoundary && Number.isFinite(bellBoundary.dueWallAt)
            ? activeAtBoundary(bellBoundary) : null,
        },
        hots: hotExecution,
        boost: {
          capacityPolicy: comp.boostCapacityPolicyOf(r),
          snapshotRole: comp.boostCapacityPolicyOf(r) === 'current-equity-v1' ? 'audit-reference' : 'admission-ceiling',
          opened: boostOpened,
          resolution: boostResolution ? {
            activationAt: Number(boostResolutionRow.activation_at),
            configured: boostResolution.configured,
            attempts: boostAttempts,
            selected: boostSelected,
            excluded: boostResolution.excluded,
          } : null,
          startedAt: boostStartBoundary && boostStartBoundary.dueWallAt,
          endedAt: bellBoundary && bellBoundary.dueWallAt,
          activeDurationMs: measuredActiveBetween(boostStartBoundary, bellBoundary),
          frozenAt: frozenAt.length ? Math.min(...frozenAt) : null,
          freezes: freezeProofs.map((row) => ({
            userId: row.user_id,
            frozenAt: Number(row.frozen_at),
            bankroll: Number(row.bankroll),
            maxExposure: Number(row.max_exposure),
            marks: (() => { try { return JSON.parse(row.marks_json); } catch { return null; } })(),
          })),
        },
        finalScores: finalScores.map((row) => ({
          userId: row.user_id,
          at: Number(row.at), scheduledAt: Number(row.scheduled_at),
          equity: Number(row.equity), accountPnl: Number(row.account_pnl),
          hotBonus: Number(row.hot_bonus), score: Number(row.score),
        })),
        scoreProofs: parsedScoreProofs.map(({ row, proof, frozenEquity, fillDigestValid }) => ({
          userId: Number(row.user_id), checkpoint: row.checkpoint,
          asOf: Number(row.as_of), epoch: Number(row.epoch),
          sha256: row.proof_sha256, hashMatches: !!(proof
            && crypto.createHash('sha256').update(row.proof_json, 'utf8').digest('hex')
              === row.proof_sha256),
          fillDigestMatches: fillDigestValid, recomputedEquity: frozenEquity,
          evidence: proof,
        })),
        distinctActiveMarkets: !!(hotExecution[0].active && hotExecution[1].active
          && hotExecution[0].active !== hotExecution[1].active),
        checks: executionChecks,
        verified: executionVerified,
      },
      verified: comp.verifyDraw(r),
      howTo: 'SHA-256 the UTF-8 bytes of canonical.json and compare with commit; then regenerate the seeded draw and check its constraints',
    });
  }
  const v = comp.verifyDraw(r);
  return send(res, 200, {
    ok: true, round: r.id, candidates: JSON.parse(r.hot_candidates),
    commit: r.draw_commit, seed: r.draw_seed, drawn: r.hot_base, verified: v,
    howTo: 'sha256(seed + "|" + candidates.join(",")) must equal commit; sha256(seed) mod candidates.length selects the market',
  });
}

/* ── the player's side of an invite ───────────────────────────────────────
   Session-authenticated, NOT operator-token authenticated: this is the one
   competition endpoint a contestant calls themselves. The invite token is the
   only thing naming the seat, so it is read but never echoed back to anyone
   who did not already have it. */
function inviteView(st, me) {
  const { seat, round } = st;
  const mine = me != null && seat.user_id === me;
  return {
    round: round.id,
    kind: round.kind,
    status: round.status,
    seat: seat.seat + 1,
    name: seat.display_name,
    avatar: seat.avatar_url || null,
    taken: seat.user_id !== null,
    mine,
    ready: mine ? seat.ready_at !== null : null,
    readiness: comp.readinessOf(round.id),
    /* The scheduled start. The desk shows a countdown once the operator arms
       one, but this payload never carried it, so the player staring at their
       invite was told only "the round starts when the producer says go" while
       a clock was visibly running on the desk. Same fact, both ends. */
    startAt: round.start_at || null,
    /* Server-computed remainder. A countdown built from startAt minus the
       CLIENT clock is wrong by that phone's skew; differences of one clock
       are skew-immune, so clients anchor on this and interpolate. */
    startInMs: round.start_at ? Math.max(0, round.start_at - Date.now()) : null,
  };
}

async function compInvite(req, res, u) {
  /* Capabilities do not belong in request targets: nginx and upstream access
     logs record query strings, and browsers can forward them in Referer. The
     SPA reads its handoff once, removes it from history, then sends this
     non-standard header, which ordinary access logs do not record. */
  try { res.setHeader('Cache-Control', 'no-store'); res.setHeader('Referrer-Policy', 'no-referrer'); } catch {}
  const queryToken = String((u && u.searchParams.get('t')) || '');
  const rawHeader = req && req.headers && req.headers['x-invite-token'];
  const t = String(Array.isArray(rawHeader) ? rawHeader[0] : (rawHeader || ''));
  if (!t && queryToken) {
    return send(res, 400, { ok: false, error: 'invite_token_in_url_not_allowed' });
  }
  const st = comp.inviteState(t);
  if (!st) return send(res, 404, { ok: false, error: 'this invite is not valid' });
  /* Readable signed out, so a player can see WHICH seat they were sent before
     being asked to sign in. Nothing is bound until they act. */
  const me = await sessionUser(req);
  return send(res, 200, { ok: true, signedIn: !!me, ...inviteView(st, me ? me.id : null) });
}

async function compInviteAct(req, res) {
  try { res.setHeader('Cache-Control', 'no-store'); res.setHeader('Referrer-Policy', 'no-referrer'); } catch {}
  let body; try { body = JSON.parse(await readBody(req) || '{}'); } catch { return send(res, 400, { ok: false, error: 'bad_json' }); }
  const me = await sessionUser(req);
  if (!me) return send(res, 401, { ok: false, error: 'not_signed_in' });
  /* Its own bucket, per IP. Sharing the operator's single global bucket would
     let eight contestants pressing ready starve the desk that has to start
     their round, and X-Real-IP because XFF[0] is caller-controlled. */
  if (!inviteRateOk(`u:${me.id}`)) return send(res, 429, { ok: false, error: 'rate_limited' });
  const rawHeader = req && req.headers && req.headers['x-invite-token'];
  const t = String((Array.isArray(rawHeader) ? rawHeader[0] : rawHeader) || body.t || '');
  const a = String(body.action || '');
  try {
    if (a === 'claim') {
      const r = comp.claimInvite(t, me.id);
      _log(`round ${r.round.id}: seat ${r.seat.seat + 1} claimed by user ${me.id}${r.already ? ' (again)' : ''}`);
      return send(res, 200, { ok: true, ...inviteView({ seat: r.seat, round: r.round }, me.id) });
    }
    if (a === 'ready' || a === 'unready') {
      const seat = comp.setInviteReady(t, me.id, a === 'ready');
      const st = comp.inviteState(t);
      return send(res, 200, { ok: true, ...inviteView(st, me.id) });
    }
    return send(res, 400, { ok: false, error: 'unknown action' });
  } catch (e) {
    return send(res, 400, { ok: false, error: e.message });
  }
}

async function compAdmin(req, res) {
  let body; try { body = JSON.parse(await readBody(req) || '{}'); } catch { return send(res, 400, { ok: false, error: 'bad_json' }); }
  const a = String(body.action || '');
  if (!compAuthed(req)) {
    logOp(req, a, body.id, false, 'forbidden');
    return send(res, 403, { ok: false, error: 'forbidden' });
  }
  if (!opRateOk()) {
    logOp(req, a, body.id, false, 'rate_limited');
    return send(res, 429, { ok: false, error: 'rate_limited' });
  }
  /* Deploy exclusion is a durable file, checked after authentication/body
     parsing and immediately before dispatch. Read-only diagnostics stay up so
     the desk can observe the drain; every action that can change round, wall,
     schedule, account or safety state is refused without even writing an
     operator-log row (the deploy is waiting for SQLite to go idle). */
  const readOnlyActions = new Set(['log', 'standings', 'preflight', 'history']);
  const knownActions = new Set([
    ...readOnlyActions,
    'create', 'wall', 'start', 'abort', 'forceAbort', 'settleAtPrior',
    'clearBlock', 'resetNight', 'resetPlayers', 'schedule',
    'cancelSchedule', 'overrideReadiness',
  ]);
  if (knownActions.has(a) && !readOnlyActions.has(a)) {
    if (deploymentMaintenanceActive()) {
      return send(res, 503, {
        ok: false, error: 'maintenance', maintenance: true, retryable: true,
      });
    }
  }
  try {
    if (a === 'create') {
      const r = comp.createRound({ ...body, speed: body.speed ?? 1 });
      logOp(req, a, r.id, true, 'sealed two-Hot draw locked');
      return send(res, 200, { ok: true, round: operatorRoundView(r) });
    }
    /* THE STAGE. The most-pressed control of the evening, because a show is
       mostly not a live round: the wait before round one, the gap while the
       host talks, and the moment something breaks and the screen has to be
       covered. Cheap and reversible by design, so an operator under pressure
       can press it without thinking about consequences. */
    /* WHAT HAS BEEN DONE, readable. The operator log has been written since
       the first round and never read back by anything: a handover mid-evening,
       or "what did I press just before it blocked", needed a shell on the box.
       Behind the operator token, newest first, and the IP column is not
       published because the desk is the only consumer and it does not need
       it. */
    if (a === 'log') {
      const rows = db.prepare(
        'SELECT at, action, round_id, ok, detail FROM paper_operator_log ORDER BY at DESC LIMIT ?'
      ).all(Math.min(200, Math.max(1, Number(body.limit) || 40)));
      return send(res, 200, { ok: true, log: rows });
    }
    if (a === 'wall') {
      const w = comp.setWall({
        mode: body.mode,
        message: body.message,
        nextAt: body.nextAt,
        nextInMs: body.nextInMs,
        nextLabel: body.nextLabel,
        series: body.series,
        seriesTotal: body.seriesTotal,
        pausedMs: body.pausedMs,
      });
      logOp(req, a, body.mode || w.mode, true, body.message ? String(body.message).slice(0, 60) : '');
      return send(res, 200, { ok: true, wall: showState(null) });
    }
    if (a === 'start') {
      /* prepare:false skips the uniform account stamp and is a rehearsal-only
         escape hatch. It is refused unless explicitly enabled, because an
         operator reaching for it during a show would start a round on
         whatever balances happened to be there. */
      const prepare = body.prepare !== false;
      if (!prepare && process.env.PAPER_ALLOW_UNPREPARED !== '1') {
        logOp(req, a, body.id, false, 'prepare:false refused');
        return send(res, 400, { ok: false, error: 'prepare:false is not permitted here' });
      }
      const r = comp.startRound(body.id, { prepare });
      purgeInviteCapabilities('admin:start');
      logOp(req, a, r.id, true, `bell ${new Date(r.ends_at).toISOString()}`);
      return send(res, 200, { ok: true, round: operatorRoundView(r) });
    }
    if (a === 'abort') {
      // forceAbort is a separate, named, audited act for a blocked round
      const r = comp.abortRound(body.id, { force: false });
      purgeInviteCapabilities('admin:abort');
      logOp(req, a, r.id, true, null);
      return send(res, 200, { ok: true, round: operatorRoundView(r) });
    }
    if (a === 'standings') {
      /* Returned {ok:true, board:[]} for a round that never existed and for a
         checkpoint that was a typo. An empty board is only an answer when the
         round is real and that checkpoint genuinely has not settled. */
      if (!comp.__test.q.get.get(body.id)) return send(res, 404, { ok: false, error: 'no such round: ' + body.id });
      const CKPT = ['final'];
      if (body.checkpoint && !CKPT.includes(String(body.checkpoint))) {
        return send(res, 400, { ok: false, error: `unknown checkpoint: ${body.checkpoint} (expected ${CKPT.join(', ')})` });
      }
      return send(res, 200, { ok: true, board: comp.standings(body.id, body.checkpoint || 'final') });
    }
    if (a === 'history') {
      /* Nights and their settled rounds for the desk, archived ones included. */
      return send(res, 200, { ok: true, ...comp.nightHistory({ limit: body.limit }) });
    }
    if (a === 'forceAbort') {
      if (!body.reason || !String(body.reason).trim()) return send(res, 400, { ok: false, error: 'forceAbort requires a reason' });
      const target = comp.__test.q.get.get(body.id);
      if (!target) return send(res, 404, { ok: false, error: 'no such round: ' + body.id });
      /* forceAbort is the exceptional path around a durable block, not a more
         powerful spelling of Abort. The UI hid it on healthy rounds, but the
         server accepted it anyway, so a stale or buggy operator client could
         irrevocably void a live round. Enforce the state at the writer. */
      if (!target.blocked_reason) {
        logOp(req, a, body.id, false, 'target is not blocked');
        return send(res, 409, { ok: false, error: `round ${body.id} is not blocked; use ordinary abort for a running round` });
      }
      const r = comp.abortRound(body.id, { force: true });
      purgeInviteCapabilities('admin:forceAbort');
      logOp(req, a, body.id, true, String(body.reason).trim().slice(0, 200));
      return send(res, 200, { ok: true, round: operatorRoundView(r) });
    }
    if (a === 'settleAtPrior') {
      /* The audited escape hatch for a permanently un-priceable Hot close.
         Requires an explicit reason; the deviation is recorded on the round
         and PUBLISHED. See competition.settleAtPrior. */
      if (!body.reason) return send(res, 400, { ok: false, error: 'settleAtPrior requires a reason' });
      const r = comp.settleAtPrior(body.id, { reason: String(body.reason) });
      logOp(req, a, body.id, true, `prior px ${r.priorPx}, gap ${r.gapMs}ms: ${String(body.reason).slice(0, 120)}`);
      return send(res, 200, { ok: true, ...r });
    }
    if (a === 'clearBlock') {
      /* Validate the target BEFORE touching any safety state. This ran the
         latch release first, so a typo'd round id answered 400 and still
         unfroze the live round. An invalid operator request must have no
         side effects at all. */
      if (!comp.__test.q.get.get(body.id)) {
        return send(res, 404, { ok: false, error: 'no such round: ' + body.id });
      }
      /* Report a still-broken price source before asking for paperwork. This
         preserves the useful 409 diagnosis while keeping a blank-note request
         side-effect free once recovery is actually possible. */
      const latched = latchFor(body.id);
      if (latched) {
        const stillBad = [...latched.symbols].filter((sym) => sym !== 'unknown' && !compPriceReady(sym));
        if (stillBad.length) {
          const reason = `still unpriceable: ${stillBad.join(', ')}`;
          logOp(req, a, body.id, false, `latch not released: ${reason}`);
          return send(res, 409, { ok: false, error: `cannot recover yet: ${reason}` });
        }
      }
      // An invalid recovery request must have no side effects on the pause latch.
      const note = String(body.note || '').trim();
      if (!note) {
        logOp(req, a, body.id, false, 'recovery note required');
        return send(res, 400, { ok: false, error: 'clearBlock requires a recovery note' });
      }
      /* And the latch only lifts if the thing that caused it has recovered.
         Refusing here leaves the round blocked, which is the correct outcome:
         the operator can see why and try again once the source is back. */
      const rel = releasePauseLatch(body.id);
      if (rel.released === false && rel.reason !== 'no latch for that round') {
        logOp(req, a, body.id, false, `latch not released: ${rel.reason}`);
        return send(res, 409, { ok: false, error: `cannot recover yet: ${rel.reason}` });
      }
      /* A normal durable pause has no emergency latch. Give recovered sources
         one synchronous chance to close their obligations, then require the
         durable pause set itself to be empty. Clearing only blocked_reason and
         answering recovered while roundPaused() remains true is a false green:
         the desk says go, but every contestant write is still refused. */
      clearPauseIfPriceable();
      const stillPaused = roundPaused();
      if (stillPaused) {
        const why = `${stillPaused.symbols.join(', ')} still paused: ${stillPaused.why}`;
        logOp(req, a, body.id, false, why);
        return send(res, 409, { ok: false, error: `cannot recover yet: ${why}`, paused: stillPaused });
      }
      // deliberate operator recovery; the only way a blocked round resumes
      const r = comp.clearBlock(body.id, { note });
      logOp(req, a, body.id, !r.blocked_reason, r.blocked_reason || 'recovered');
      return send(res, 200, { ok: true, round: operatorRoundView(r), recovered: !r.blocked_reason, latch: rel });
    }
    if (a === 'resetNight') {
      const ser = body.series != null ? String(body.series) : (comp.wallState().series?.label ?? null);
      const r = comp.resetNight(ser);
      purgeInviteCapabilities('admin:resetNight');
      logOp(req, a, null, true, `archived ${r.archived} under ${ser || '(none)'}, discarded ${r.discarded}`);
      return send(res, 200, { ok: true, ...r });
    }
    /* Fresh accounts for the next round: bump the epoch and restore the
       starting balance for everyone seated, in one pass. Doing this per
       player by hand is how someone ends up starting a round on a stale
       balance, which is the funding pre-flight lesson from Belgrade. */
    if (a === 'resetPlayers') {
      /* Preparation belongs to startRound. Resetting mid-round bumped the
         account epoch while the roster kept the old one, so equity and the
         bound scoring basis silently disagreed. Armed rounds only. */
      const r0 = comp.__test.q.get.get(body.id);
      if (!r0) return send(res, 400, { ok: false, error: 'no such round' });
      if (r0.status !== 'armed') {
        const why = `round is ${r0.status}; preparation only happens on an armed round`;
        logOp(req, a, body.id, false, why);
        return send(res, 409, { ok: false, error: why });
      }
      const players = comp.playersOf(body.id);
      if (!players.length) return send(res, 400, { ok: false, error: 'no players on that round' });
      /* Preparing a future round must not reach into a live one: the same
         person can be seated in both, and resetting them here would restore
         the bankroll and bump the epoch under a running result. */
      for (const p of players) {
        const other = comp.otherRoundsOwning(p.user_id, body.id);
        if (other.length) {
          const why = `player ${p.user_id} is live in round ${other[0].id}`;
          logOp(req, a, body.id, false, why);
          return send(res, 409, { ok: false, error: why });
        }
      }
      const done = [];
      /* prepareSeat, not a near-copy of it. This loop used to clear positions
         and bump the epoch but never stamp the contest account spec, so a
         seat left in ordinary paper mode stayed "not in stage mode" through
         the pre-flight -- and Reset all seats, the button the desk offers as
         the fix, could not fix it. One definition of a prepared seat. */
      /* ONE transaction across the whole roster. prepareSeat performs several
         writes, so a failure on the second seat left the first fully reset and
         the second with its positions already deleted but its account mode and
         balance untouched: a roster in a state no rule describes, mid-show. */
      /* An unclaimed seat has no account to prepare, and calling prepareSeat
         on user_id NULL failed the whole action with a raw FOREIGN KEY error,
         so a roster with one empty seat could not be reset at all. Skip them
         and say how many. */
      const claimed = players.filter((p) => p.user_id !== null);
      const skipped = players.filter((p) => p.user_id === null)
        .map((p) => ({ seat: p.seat, name: p.display_name }));
      atomically(() => {
        for (const p of claimed) {
          const st = prepareSeat(p.user_id);
          done.push({ userId: p.user_id, name: p.display_name, epoch: st.epoch });
        }
      });
      _log(`round ${body.id}: reset ${done.length} claimed seat(s)`
        + (skipped.length ? `, skipped ${skipped.length} unclaimed` : ''));
      logOp(req, a, body.id, true, `${done.length} seats, ${skipped.length} unclaimed`);
      return send(res, 200, { ok: true, reset: done, skipped });
    }
    /* Pre-flight: is every seat actually ready? Equivalent of the funding
       check that caught a wallet nobody had topped up at Belgrade. */
    if (a === 'schedule') {
      /* "Start in 5 minutes." The countdown lives in the engine, so the desk
         can be closed, reloaded or moved to another machine without the room's
         start time changing. */
      const at = body.at ? Number(body.at) : Date.now() + Number(body.inMs || 0);
      const r = comp.scheduleStart(body.id, at);
      logOp(req, a, body.id, true, `starts at ${new Date(r.startAt).toISOString()}`);
      return send(res, 200, { ok: true, ...r });
    }
    if (a === 'cancelSchedule') {
      const r = comp.cancelScheduledStart(body.id);
      logOp(req, a, body.id, true, 'schedule cancelled');
      return send(res, 200, { ok: true, ...r });
    }
    if (a === 'overrideReadiness') {
      /* Deliberate, named and audited: the operator is saying "I know this box
         restarted, I can see the markets are healthy, start anyway". */
      if (!body.why) return send(res, 400, { ok: false, error: 'an override needs a reason' });
      const r = comp.overrideReadiness(body.id, body.why);
      logOp(req, a, body.id, true, `readiness overridden: ${body.why}`);
      return send(res, 200, { ok: true, ...r });
    }
    if (a === 'preflight') {
      /* Answer for a round that exists, or refuse. Without this the empty
         roster and empty market list of a NON-EXISTENT round both pass
         `[].every(...)`, so the desk was told "every seat ready" about a
         round that had never been armed -- and then start failed with the
         truthful "no such round" after the operator had already been shown a
         green light. A vacuous pass is the worst kind of pre-flight. */
      const rr = comp.__test.q.get.get(body.id);
      if (!rr) throw new Error('no such round: ' + body.id);
      const players = comp.playersOf(body.id);
      if (!players.length) throw new Error('round ' + body.id + ' has no players');
      const rows = players.map((p) => {
        const acct = stmt.acctGet.get(p.user_id);
        const pos = stmt.posByUser.all(p.user_id).length;
        const start = acct ? (acct.start_balance || START_BALANCE) : null;
        return {
          userId: p.user_id, name: p.display_name, seat: p.seat,
          /* The desk needs the invite to hand out, and the two facts the
             producer actually watches: has this person turned up, and have
             they said they are ready. */
          invite: p.invite_token || null,
          claimed: p.user_id !== null,
          playerReady: p.ready_at !== null,
          hasAccount: !!acct, epoch: acct ? acct.epoch : null,
          balance: acct ? acct.balance : null, startBalance: start,
          openPositions: pos, stage: acct ? isStage(acct.heat) : false,
          /* A seat nobody has claimed is not a failing seat, it is an empty
             one: it will simply be dropped at start. Reporting it as "not
             ready" would hold the pre-flight red for a round that is
             perfectly startable. */
          /* `ready` has always meant "this seat is PREPARED": account exists,
             flat, stage mode, untouched balance. The invite adds a second and
             completely different readiness, the player pressing a button, so
             that one is playerReady and this one keeps its name. */
          ready: p.user_id === null
            ? true
            : !!acct && pos === 0 && isStage(acct.heat) && Math.abs(acct.balance - start) < 1e-9,
        };
      });
      /* Market-data readiness belongs in preflight: starting a round whose
         markets cannot be priced strictly means blocking at the first
         checkpoint, in front of an audience. */
      /* THE SAME function start uses. Two definitions of ready is what let the
         desk go green and the next click fail. */
      const verdict = comp.marketReadiness(body.id);
      const markets = verdict.markets;
      /* The engine's verdict verbatim, including WHY, so the desk can show the
         same sentence start would have failed with. */
      const marketsReady = markets.length > 0 && verdict.ok;
      const seatsClaimed = rows.filter((r) => r.claimed).length;
      return send(res, 200, {
        ok: true,
        /* An unclaimed seat is "ready" in the sense that it will simply be
           dropped, but a roster where NOBODY has claimed a seat is not a
           startable round, and reporting it green was the same false-green
           this endpoint keeps being asked to stop producing. The desk
           separately requires two for a show. */
        allReady: rows.every((r) => r.ready) && marketsReady && seatsClaimed > 0,
        seatsReady: rows.every((r) => r.ready) && seatsClaimed > 0,
        seatsClaimed,
        readiness: comp.readinessOf(body.id),
        /* The setup shown after arming must come from the server, not from an
           editable browser draft. This is the immutable configuration that a
           later Start or schedule action will actually consume. */
        round: {
          id: rr.id,
          status: rr.status,
          kind: rr.kind,
          formatVersion: Number(rr.format_version) || 1,
          boostLeverage: rr.boost_leverage || null,
          boostCapacityPolicy: comp.boostCapacityPolicyOf(rr),
          ...comp.backupExecutionPolicyFields(rr),
          blockedReason: rr.blocked_reason || null,
          startAt: rr.start_at || null,
        },
        marketsReady,
        marketReadiness: {
          required: markets.length,
          ready: markets.filter((m) => m.priceReady && (!m.enoughHistory || m.reliable)).length,
        },
        players: rows,
        marketsError: verdict.ok ? null : 'one or more sealed draw markets are not competition-ready',
        seatsError: seatsClaimed > 0 ? null : 'nobody has claimed a seat yet',
      });
    }
    logOp(req, a, body.id, false, 'unknown action');
    return send(res, 400, { ok: false, error: 'unknown action' });
  } catch (e) {
    logOp(req, a, body.id, false, e.message);
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
  const owner = openAliases.get(alias);
  if (owner?.roundId && roundId && owner.roundId !== roundId) throw new Error('alias belongs to another round');
  const round = roundId ? comp.__test.q.get.get(roundId) : null;
  if (holdsRoundPrices(round)) {
    if (round.status !== 'running' || comp.currentRound()?.id !== round.id) throw new Error('round alias scope is not live');
    if (!roundExecutionMark(base, { round, forLeverage: kind === 'BOOST'
      ? Number(round.boost_leverage) || HEAT_MAX_LEV : comp.COMP_BASE_LEV })) throw new Error('base has no qualified round mark: ' + base);
  } else {
    if (holdsRoundPrices()) throw new Error('round alias ownership is required');
    if (!markOfFreshFor(base, true)) throw new Error('base has no fresh mark: ' + base);
  }
  openAliases.set(alias, { openedAt: Date.now(), roundId });
  _log(`alias open ${alias} (base ${base}${roundId ? ', round ' + roundId : ''})`);
  return true;
}
// Close a segment: stop accepting orders on the ticker, then flatten every
// position on it at the index. Reuses applyFill, so these closes produce
// ordinary fills, realised PnL and audit rows like any other close — the
// segment bonus is simply what got realised, with no snapshot to reconcile.
function closeAlias(alias, { flatten = true, roundId = null, settleAt = null, overrideMark = null } = {}) {
  /* Aliases are global but owned. Without this check, aborting a round that
     never opened anything could close the LIVE round's tickers and flatten
     its positions. */
  const owner = openAliases.get(alias);
  if (roundId && owner && owner.roundId && owner.roundId !== roundId) {
    _log(`alias close ${alias}: owned by round ${owner.roundId}, refusing on behalf of ${roundId}`);
    return { alias, closed: 0, total: 0, skipped: 'not_owner' };
  }
  openAliases.delete(alias);
  /* Cancel anything resting on the ticker BEFORE settling it. A limit order
     left open on a closed segment would fill later and reopen exposure the
     segment had already scored and shut. Cancellation happens even when we
     are not flattening (the bell), because the gate is shut either way. */
  const resting = stmt.ordOpenBySymbol.all(alias);
  if (resting.length) {
    stmt.ordCancelSymbol.run(Date.now(), alias);
    _log(`alias close ${alias}: cancelled ${resting.length} resting order(s)`);
  }
  // Shutting the gate and flattening are DIFFERENT acts. The Hot segment ends
  // by flattening, because its bonus is what got realised. The bell ends by
  // marking: positions stay exactly where they are and the final snapshot
  // prices them, so nobody gains from closing faster at the end.
  if (!flatten) { _log(`alias close ${alias}: gate shut, positions left open`); return { alias, closed: 0, total: 0 }; }
  /* ONE canonical mark for the whole segment, taken once. Fetching a mark per
     player meant two traders could settle the same segment at different
     prices, and a momentarily missing mark silently left someone open with a
     live bonus after the window had scored. */
  const ps = stmt.posBySymbol.all(alias);
  if (!ps.length) { _log(`alias close ${alias}: nothing open`); return { alias, closed: 0, total: 0, mark: null }; }
  /* Price the segment at the instant it was DUE, not when this ran. A late
     callback used to settle every position at the current price, so the
     segment kept accruing score after its own window had shut. */
  /* strict: a segment settlement is a scored event. Falling back to the
     current price let a late callback quietly extend the segment's economics
     past its own window, which is the failure this scheduled timestamp was
     introduced to prevent. */
  /* overrideMark is the audited operator escape hatch, never a fallback the
     engine reaches for on its own: when the exact instant has no valid
     observation and never will, the operator may settle at the last accepted
     observation BEFORE it, with the deviation disclosed. See settleAtPrior. */
  const mark = Number.isFinite(overrideMark) && overrideMark > 0
    ? overrideMark
    : (Number.isFinite(settleAt)
      ? markAt(baseOf(alias), settleAt, { strict: true,
        round: comp.__test.q.get.get(roundId) || comp.currentRound() })
      : (holdsRoundPrices() ? roundExecutionMark(alias)?.price : markOfFreshFor(alias, true)));
  if (!(Number(mark) > 0)) {
    // fail closed: the caller records the boundary as failed and blocks
    throw new Error(`cannot settle ${alias}: no mark at ${settleAt || 'now'}`);
  }
  db.transaction(() => {
    for (const p of ps) {
      const cSide = p.side === 'LONG' ? 'SELL' : 'BUY';
      const execution = execPxFor(p.user_id, alias, cSide, p.size, mark);
      applyFill(p.user_id, {
        symbol: alias, orderSide: cSide, size: p.size,
        px: execution.px,
        feeBps: cfgOf(alias).takerBps, kind: 'SEGMENT',
        executionSource: execution.source, referenceMark: mark,
        decisionReason: 'segment-boundary-flatten',
        decisionContext: { boundary: { type: aliasKind(alias), scheduledAt: settleAt, roundId, overrideMark: Number.isFinite(overrideMark) ? overrideMark : null } },
        /* Stamp the BOUNDARY instant. A recovery running after the bell used
           to stamp Date.now(), which the Hot-bonus query then excluded by its
           ts <= scheduledAt filter: the same PnL counted once in account PnL
           and zero times in the bonus, silently paying 1x on a 2x segment. */
        at: Number.isFinite(settleAt) ? settleAt : undefined,
      });
    }
    const left = stmt.posBySymbol.all(alias).length;
    if (left) throw new Error(`settle ${alias} incomplete: ${left} position(s) still open`);
  })();
  _log(`alias close ${alias}: settled ${ps.length} at ${mark}`);
  return { alias, closed: ps.length, total: ps.length, mark };
}

// ── shared risk evaluation: one code path for the 5s sweep AND per-tick ──
// evaluation, so cadence never changes semantics. SL/TP before liquidation
// (a protective stop wins a shared tick); strict-inequality liq boundary.
function pausedRiskBlocked(uid, recovery = null) {
  if (!roundPaused() || !competitionOwned(uid)) return false;
  // Only the exact guarded event may evaluate recovery. A newly opened pause
  // invalidates its token; HTTP callers and later ticks never inherit it.
  return !(recovery && db.inTransaction
    && recovery.pauseRevision === _pauseRevision
    && recovery.roundId === (comp.currentRound() || {}).id
    && (recovery === _roundRecoveryBatch
      ? recovery.users.has(uid)
      : live.map.get(recovery.base) === recovery.entry));
}
function evalPositionAtMark(p, m, now, recovery = null) {
  /* Shared pause: no contestant is marked or liquidated while any competition
     symbol is unpriceable. Public accounts are unaffected. */
  if (pausedRiskBlocked(p.user_id, recovery)) {
    if (recovery === _roundRecoveryBatch && recovery) throw recoveryUnavailable('pause changed');
    return;
  }
  /* Competition-owned state is frozen once the round is settled or blocked:
     a stop, liquidation or expiry firing after the bell would rewrite the
     account the result was computed from. */
  if (comp.writeBarrier(p.user_id, now)) {
    if (recovery === _roundRecoveryBatch && recovery) throw recoveryUnavailable('write barrier');
    return;
  }
  const isHeat = isStage(heatOf(p.user_id));
  const competitor = competingNow(p.user_id);
  /* A position is marked, and can be LIQUIDATED, only on a price good enough
     for the leverage it actually carries. Downgrading the cap for new orders
     while still liquidating existing 1000x exposure on the lower-quality
     observation was the worst version of a half-applied rule. */
  const mark = (competitor
    && !roundPriceReady(baseOf(p.symbol), Number(p.leverage) || 0))
    ? NaN
    : markFor(m, isHeat, baseOf(p.symbol), competitor);
  if (!Number.isFinite(mark) || mark <= 0) {
    if (recovery === _roundRecoveryBatch && recovery) throw recoveryUnavailable('position mark');
    return;   // halted/blind symbol: touch nothing
  }
  stmt.posMark.run(mark, now, p.user_id, p.symbol);
  settleFunding(p, mark, Number(m && m.currentFundingRate) || 0, now);
  const fresh = stmt.posGet.get(p.user_id, p.symbol);
  if (!fresh) return;
  // Boost window over → flatten at the mark, no questions. Runs before SL/TP:
  // an expired position has no protective claims left to exercise.
  if (fresh.boost_since && now - fresh.boost_since >= BOOST_WINDOW_MS) {
    const cSide = fresh.side === 'LONG' ? 'SELL' : 'BUY';
    const execution = execPxFor(p.user_id, p.symbol, cSide, fresh.size, mark);
    applyFill(p.user_id, {
      symbol: p.symbol, orderSide: cSide, size: fresh.size, px: execution.px,
      feeBps: cfgOf(p.symbol).takerBps, kind: 'EXPIRY', at: now,
      executionSource: execution.source, referenceMark: mark,
      decisionReason: 'boost-window-expired',
      decisionContext: { boundary: { type: 'boost-expiry', armedAt: fresh.boost_since, scheduledAt: fresh.boost_since + BOOST_WINDOW_MS, observedAt: now } },
    });
    return;
  }
  const slHit = fresh.sl_price != null && (fresh.side === 'LONG' ? mark <= fresh.sl_price : mark >= fresh.sl_price);
  const tpHit = fresh.tp_price != null && (fresh.side === 'LONG' ? mark >= fresh.tp_price : mark <= fresh.tp_price);
  if (slHit || tpHit) {
    const cSide = fresh.side === 'LONG' ? 'SELL' : 'BUY';
    const execution = execPxFor(p.user_id, p.symbol, cSide, fresh.size, mark);
    applyFill(p.user_id, {
      symbol: p.symbol, orderSide: cSide, size: fresh.size, px: execution.px,
      feeBps: cfgOf(p.symbol).takerBps, kind: slHit ? 'SL' : 'TP', at: now,
      executionSource: execution.source, referenceMark: mark,
      decisionReason: slHit ? 'stop-loss-threshold-crossed' : 'take-profit-threshold-crossed',
      decisionContext: { trigger: { type: slHit ? 'SL' : 'TP', threshold: slHit ? fresh.sl_price : fresh.tp_price, observedMark: mark, positionSide: fresh.side } },
    });
    return;
  }
  // isolated liquidation: this position's own margin vs its maintenance
  if (isIso(fresh)) {
    const eq = fresh.isolated_margin + uPnl(fresh, mark);
    if (eq < fresh.size * mark * mmfForPos(fresh, isHeat)) liquidateIsolated(fresh, now);
  }
}
function evalCrossForUser(uid, now, recovery = null) {
  if (pausedRiskBlocked(uid, recovery)) {
    if (recovery === _roundRecoveryBatch && recovery) throw recoveryUnavailable('pause changed');
    return;
  }
  /* The per-position pass is guarded, but this user-level pass was not, so a
     blocked or settled competitor could still be liquidated or have orders
     cancelled by an incoming tick. */
  if (comp.writeBarrier(uid, now)) {
    if (recovery === _roundRecoveryBatch && recovery) throw recoveryUnavailable('write barrier');
    return;
  }
  const acct = stmt.acctGet.get(uid);
  if (!acct) {
    if (recovery === _roundRecoveryBatch && recovery) throw new Error('recovery account missing');
    return;
  }
  const risk = accountRisk(uid, acct);
  const hasCross = risk.positions.some((p) => !isIso(p));
  if (!hasCross) return;
  if (risk.equityCross < risk.maint) { liquidateCross(uid, now); return; }
  if (risk.orders.length && risk.equityCross < risk.cancelTier) {
    stmt.ordCancelUser.run(now, uid);
    _log(`order-cancel tier hit for user ${uid} (equity ${risk.equityCross.toFixed(2)} < ${risk.cancelTier.toFixed(2)})`);
  }
}

/* A cold restart can leave several round marks on the old boot. A one-base
   tick cannot prove that the other bases recovered, so none may be adopted.
   Resolve that circular dependency with one private price world and ONE
   rollback edge for the complete roster, never one savepoint per seat. */
const _recoveryRoster = db.prepare(`SELECT p.user_id, p.seat, p.epoch,
  p.boost_bankroll, p.boost_max_exposure, p.boost_frozen_at,
  a.epoch AS account_epoch, a.heat AS account_heat
  FROM paper_round_players p LEFT JOIN paper_accounts a ON a.user_id = p.user_id
  WHERE p.round_id = ? ORDER BY p.seat, p.user_id LIMIT 33`);
const _recoveryPositions = db.prepare('SELECT * FROM paper_positions WHERE user_id = ? ORDER BY symbol LIMIT 109');
const _recoveryOrders = db.prepare("SELECT * FROM paper_orders WHERE user_id = ? AND status = 'OPEN' ORDER BY id LIMIT 21");
const _recoveryBoostBoundary = db.prepare('SELECT at, status, due_wall_at FROM paper_round_boundaries WHERE round_id=? AND at=?');
function recoveryUnavailable(reason) {
  const e = new Error('round recovery unavailable: ' + reason);
  e.roundRecoveryUnavailable = true;
  return e;
}
function recoveryRoundPin(r, at) {
  const ph = comp.phaseNow(at);
  if (!r || !ph || ph.round.id !== r.id) throw recoveryUnavailable('round changed');
  // Only public/current phase fields. Never inspect the sealed future draw.
  const boostBoundary = r.boost_opened ? _recoveryBoostBoundary.get(r.id, comp.planOf(r).boostStart) : null;
  return JSON.stringify([r.id, r.status, r.price_policy, r.format_version, comp.boostCapacityPolicyOf(r),
    comp.backupExecutionPolicyOf(r),
    r.started_at, r.ends_at, r.paused_since, r.paused_ms, r.blocked_reason,
    r.boost_opened, r.boost_leverage, r.hot1_active_base, r.hot2_active_base,
    ph.phase, ph.hotNumber || null, ph.activeMs,
    boostBoundary ? [boostBoundary.at, boostBoundary.status, boostBoundary.due_wall_at] : null]);
}
function recoveryRoster(rid) {
  const rows = _recoveryRoster.all(rid);
  if (!rows.length || rows.length > 32 || new Set(rows.map(x => x.user_id)).size !== rows.length
      || rows.some(x => !Number.isSafeInteger(x.user_id) || x.user_id <= 0
        || !Number.isSafeInteger(x.epoch) || x.epoch !== x.account_epoch
        || !isStage(x.account_heat))) throw new Error('round recovery roster invariant');
  return rows;
}
function recoveryExposure(batch) {
  const positions = [], orders = [];
  for (const seat of batch.roster) {
    const ps = _recoveryPositions.all(seat.user_id), os = _recoveryOrders.all(seat.user_id);
    if (ps.length > 108 || os.length > MAX_OPEN_ORDERS) throw new Error('round recovery exposure bound');
    for (const row of [...ps, ...os]) {
      if (row.epoch !== seat.epoch || !STAGE_INDEXED.has(baseOf(row.symbol))
          || DISABLED_MARKETS.has(baseOf(row.symbol))
          || !Number.isFinite(row.leverage) || row.leverage <= 0
          || !Number.isFinite(row.size) || row.size <= 0) throw new Error('round recovery exposure invariant');
    }
    positions.push(...ps); orders.push(...os);
  }
  return { positions, orders };
}
function recoveryAliasAllowed(symbol, uid, batch) {
  return !!(batch && batch === _roundRecoveryBatch && db.inTransaction
    && batch.users.has(uid) && batch.pauseRevision === _pauseRevision
    && comp.currentRound()?.id === batch.roundId
    && batch.phase === 'boost' && aliasKind(symbol) === 'BOOST'
    && batch.opened.has(baseOf(symbol))
    && (!openAliases.has(symbol) || openAliases.get(symbol).roundId === batch.roundId));
}
function competitionAliasOpen(symbol) {
  if (aliasOpen(symbol)) return true;
  const batch = _roundRecoveryBatch;
  // advanceRoundClock/monitorSegments is also reached from writeBarrier.
  // Its private recovery probe must see the same already-open durable gate
  // as the order/risk path, without reopening the public map before commit.
  return !!(batch && db.inTransaction && batch.pauseRevision === _pauseRevision
    && comp.currentRound()?.id === batch.roundId && batch.phase === 'boost'
    && aliasKind(symbol) === 'BOOST' && batch.opened.has(baseOf(symbol)));
}
function recoveryProbe(batch, exposure, at) {
  const dependencies = new Map(batch.dependencies);
  for (const row of [...exposure.positions, ...exposure.orders]) {
    const base = baseOf(row.symbol);
    dependencies.set(base, Math.max(dependencies.get(base) || 0, row.leverage));
    if (aliasKind(row.symbol) && !recoveryAliasAllowed(row.symbol, row.user_id, batch)) {
      throw recoveryUnavailable('alias is not a committed open Boost');
    }
  }
  // Probe every dependency before any account mutation. No unpriced catch or
  // per-seat continuation may turn partial readiness into a recovery proof.
  for (const [base, leverage] of dependencies) {
    if (!roundExecutionMark(base, { round: batch.round, at, forLeverage: leverage })) {
      throw recoveryUnavailable('price dependency');
    }
  }
  if (!competitionClockStatus(at, { ignorePause: true }).ok) throw recoveryUnavailable('clock dependency');
  for (const seat of batch.roster) {
    if (comp.writeBarrier(seat.user_id, at)) throw recoveryUnavailable('write barrier');
    const risk = accountRisk(seat.user_id, stmt.acctGet.get(seat.user_id));
    if (!Number.isFinite(risk.equityTotal)) throw recoveryUnavailable('roster equity');
  }
  if (comp.sampleDrawdown(at, { probeOnly: true }) !== true) throw recoveryUnavailable('drawdown probe');
}
function recoveryCheck(batch) {
  const at = Date.now(), r = comp.currentRound();
  if (_roundRecoveryBatch !== batch || !db.inTransaction || !r || r.id !== batch.roundId
      || r.status !== 'running' || !holdsRoundPrices(r) || r.blocked_reason
      || !r.paused_since || !roundPaused() || latchFor(r.id)
      || _pauseRevision !== batch.pauseRevision || deploymentMaintenanceActive()
      || recoveryRoundPin(r, at) !== batch.roundPin
      || JSON.stringify(recoveryRoster(r.id)) !== batch.rosterPin) throw recoveryUnavailable('authority changed');
  const identity = auth.authHealth(at);
  if (identity.tradingAvailable !== true || !Number.isFinite(identity.validUntil)
      || at >= identity.validUntil || at >= batch.identityUntil
      || monoNow() >= batch.identityDeadlineMono) throw recoveryUnavailable('identity lease');
  for (const [base, token] of batch.marks) {
    const e = live.map.get(base), r0 = token.record;
    if (e !== token.entry || e.acceptedBoot !== r0.acceptedBoot || e.acceptedSeq !== r0.acceptedSeq
        || e.pythPrice !== r0.price || e.pythAtMs !== r0.acceptedAt
        || (Number(e.pythSrcAtMs) || Number(e.pythAtMs)) !== r0.observedAt
        || e.srcKey !== r0.source || _srcUnsafe.has(base)
        || at < r0.appliedAt || at >= r0.originalValidUntil
        || monoNow() >= token.deadlineMono) throw recoveryUnavailable('original observation expired or changed');
    const admission = intrinsicCandidateAdmission(base, e, at);
    if (!admission.accepted || admission.leverageCap < r0.acceptedLeverageCap) {
      throw recoveryUnavailable('original observation quality');
    }
  }
}
function finishRoundRecovery(rid) {
  const r = comp.currentRound();
  if (!r || r.id !== rid || r.status !== 'running') {
    _roundRecoveryPostCommit = null;
    return { committed: true, resumed: false };
  }
  try {
    if (r.blocked_reason || latchFor(rid) || deploymentMaintenanceActive()) return { committed: true, resumed: false };
    if (comp.rehydrateGates(r).length) return { committed: true, resumed: false };
    clearPauseIfPriceable();
    const resumed = !roundPaused() && !comp.currentRound()?.paused_since;
    if (resumed) _roundRecoveryPostCommit = null;
    _stateCache = { at: 0, body: null };
    scheduleCompBoard(null, true);
    return { committed: true, resumed };
  } catch {
    // Financial commit is historical fact. Do not report a rollback or rerun
    // the risk event when a subsequent gate/clock publication fails.
    return { committed: true, resumed: false };
  }
}
function recoverRoundPriceBatch(at = Date.now()) {
  if (db.inTransaction || _roundRecoveryBatch || _roundPriceCandidate || _sweepRunning
      || deploymentMaintenanceActive()) return { committed: false, skipped: 'busy or maintenance' };
  const round = comp.currentRound();
  if (_roundRecoveryPostCommit) return finishRoundRecovery(_roundRecoveryPostCommit.roundId);
  if (!round || round.status !== 'running' || !holdsRoundPrices(round) || round.blocked_reason
      || !round.paused_since || !roundPaused() || latchFor(round.id)
      || comp.terminalCleanupPending(round)) return { committed: false, skipped: 'not recoverable' };
  let batch;
  try {
    const identity = auth.authHealth(at), beganMono = monoNow();
    if (identity.tradingAvailable !== true || !Number.isFinite(identity.validUntil)
        || identity.validUntil <= at) throw recoveryUnavailable('identity lease');
    const roster = recoveryRoster(round.id), ph = comp.phaseNow(at);
    const opened = JSON.parse(round.boost_opened || '[]');
    if (!Array.isArray(opened) || opened.length > 4 || new Set(opened).size !== opened.length
        || opened.some(base => !['BTC', 'ETH', 'SOL', 'XRP'].includes(base))) throw new Error('round recovery opened-set invariant');
    batch = { roundId: round.id, round, roster, users: new Set(roster.map(x => x.user_id)),
      rosterPin: JSON.stringify(roster), roundPin: recoveryRoundPin(round, at),
      pauseRevision: _pauseRevision, phase: ph.phase, opened: new Set(opened),
      marks: new Map(), dependencies: new Map(), identityUntil: identity.validUntil,
      identityDeadlineMono: beganMono + identity.validUntil - at };
    if (ph.phase === 'hot' && ph.hotNumber) {
      const base = round[`hot${ph.hotNumber}_active_base`];
      if (base) batch.dependencies.set(base, comp.COMP_BASE_LEV);
    }
    const boostBoundary = opened.length ? _recoveryBoostBoundary.get(round.id, comp.planOf(round).boostStart) : null;
    const pendingOpenedBoost = !!(opened.length && boostBoundary
      && boostBoundary.status !== 'succeeded' && Number(boostBoundary.at) <= ph.activeMs + 1e-6);
    if (ph.phase === 'boost' || pendingOpenedBoost) {
      for (const base of opened) batch.dependencies.set(base, Number(round.boost_leverage) || HEAT_MAX_LEV);
      if (roster.some(x => !Number.isFinite(x.boost_bankroll) || x.boost_bankroll < 0
          || !Number.isFinite(x.boost_max_exposure) || x.boost_max_exposure < 0
          || !Number.isFinite(x.boost_frozen_at) || x.boost_frozen_at <= 0)) throw new Error('round recovery Boost bankroll invariant');
    }
    const exposure = recoveryExposure(batch);
    for (const row of [...exposure.positions, ...exposure.orders]) {
      const base = baseOf(row.symbol);
      batch.dependencies.set(base, Math.max(batch.dependencies.get(base) || 0, row.leverage));
    }
    let feedReady = false, flatFallback = null;
    for (const base of STAGE_INDEXED) {
      if (DISABLED_MARKETS.has(base)) continue;
      // Keep a usable committed lineage. Batch recovery is not an optional
      // reprice of healthy markets, nor a new source-ingestion path.
      if (roundExecutionMark(base, { round, at, candidate: false,
        forLeverage: batch.dependencies.get(base) || 0 })) { feedReady = true; continue; }
      const entry = live.map.get(base);
      if (!entry || entry.acceptedBoot !== ENGINE_BOOT_ID || _srcUnsafe.has(base)) continue;
      const token = candidateRoundMark(base, entry, at, entry.acceptedSeq, round);
      if (!token) continue;
      token.deadlineMono = beganMono + token.record.originalValidUntil - at;
      if (batch.dependencies.has(base)) batch.marks.set(base, token);
      else if (!flatFallback || token.record.originalValidUntil > flatFallback.record.originalValidUntil) flatFallback = token;
    }
    // An unrelated optional market must never set the required batch's
    // shortest deadline. A flat cold roster needs just one trading-world
    // mark; choose the longest remaining strict lease, without renewing it.
    if (!feedReady && !batch.marks.size && flatFallback) batch.marks.set(flatFallback.record.base, flatFallback);
    if (!batch.marks.size) return { committed: false, skipped: 'no new qualified marks' };
    db.transaction(() => {
      _roundRecoveryBatch = batch;
      recoveryCheck(batch);
      recoveryProbe(batch, exposure, at);
      if (!comp.sampleDrawdown(at)) throw recoveryUnavailable('drawdown before risk');
      for (const o of exposure.orders.sort(restingPriceTime)) sweepOneOrder(o, at, batch);
      // Orders can open new positions. Read the resulting complete roster,
      // then prove every mark before evaluating any of those positions.
      const afterOrders = recoveryExposure(batch);
      recoveryProbe(batch, afterOrders, at);
      for (const p of afterOrders.positions) evalPositionAtMark(p, mkt(p.symbol), at, batch);
      for (const seat of roster) evalCrossForUser(seat.user_id, at, batch);
      recoveryProbe(batch, recoveryExposure(batch), at);
      if (!comp.sampleDrawdown(at)) throw recoveryUnavailable('drawdown after risk');
      recoveryCheck(batch);
      // The private prices become durable ONLY after all scored mutations.
      for (const token of batch.marks.values()) comp.roundMarkCommit(round.id, token.record, token.expectedIdentity);
      recoveryCheck(batch);
    })();
  } catch (e) {
    if (!e?.roundRecoveryUnavailable && !e?.unpriced) {
      try { comp.blockRound(round.id, 'round recovery transaction failed'); } catch {}
    }
    return { committed: false, skipped: e?.roundRecoveryUnavailable || e?.unpriced ? 'dependencies unavailable' : 'transaction failed' };
  } finally { _roundRecoveryBatch = null; }
  _roundRecoveryPostCommit = { roundId: round.id };
  return finishRoundRecovery(round.id);
}
// Per-tick liquidation/SL/TP: runs on every fresh mark for symbols carrying
// paper risk (throttled per symbol). At high leverage the 5s sweep is too
// coarse — a wick between sweeps must still resolve. FT engine handoff §5.2.
const _lastTickEval = new Map();   // symbol -> ms
const TICK_EVAL_MIN_MS = 250;
/* How far the market may move between the price on the trader's screen and the
   one that fills before we ask again rather than assume. Twenty five basis
   points is a fifth of the liquidation distance at 500x and several seconds of
   ordinary movement, so a healthy click never sees it. */
const ORDER_REQUOTE_BPS = Number(process.env.PAPER_ORDER_REQUOTE_BPS || 25);
/* Returns { ok } or { ok:false, error }. The caller relays the tick only when
   this says the risk pass completed. */
/* `at` is the EVENT's time, allocated once at ingress. Sampling the clock
   again in here meant a tick received a millisecond before a bell could be
   charted and audited as pre-bell while its fills, its drawdown sample and the
   round clock all landed after it: the same observation produced different
   results depending on how busy the box was. Processing time is telemetry;
   event time decides. */
function tickEval(symbol, {
  force = false, at = 0, candidateEntry = null, committedEntry = null,
  committedReady = false, committedConfirming = null,
  riskProbeEntry = null, riskDeadlineMono = null, roundCandidate = null,
} = {}) {
  /* Feed ingestion continues during a release health check, but financial
     consequences do not. This is paired with the HTTP barrier and sweep
     barrier: a failed candidate can therefore restore its pre-start database
     without deleting an acknowledged trade, funding event or liquidation. */
  if (deploymentMaintenanceActive()) return { ok: true, skipped: 'deployment maintenance' };
  let failure = null;
  let committedAdmission = null;
  let observationExpired = false;
  let deferredScoredFailure = null;
  let drawdownChanged = false;
  const now = Number(at) > 0 ? Number(at) : Date.now();
  const candidateBase = baseOf(symbol);
  const hideCandidate = !!(candidateEntry
    && live.map.get(candidateBase) === candidateEntry);
  const candidateConfirming = _confirming.has(candidateBase)
    ? _confirming.get(candidateBase) : null;
  let committedProbeToken = null;
  /* Segment/outage accounting and every due boundary must see only committed
     price state. Ingress installs the candidate early so risk can evaluate
     it, but a later failure/expiry may still restore the old mark. Temporarily
     hide that exact object through the complete clock pass; otherwise an
     unaccepted tick can clear an outage or settle a checkpoint. */
  if (hideCandidate) {
    if (committedEntry) live.map.set(candidateBase, committedEntry);
    else live.map.delete(candidateBase);
    if (committedConfirming) _confirming.set(candidateBase, committedConfirming);
    else _confirming.delete(candidateBase);
  }
  try {
    if (hideCandidate) {
      if (_committedReadinessProbe) throw new Error('committed readiness probe re-entry');
      committedProbeToken = {
        sym: candidateBase, entry: committedEntry, ready: committedReady === true,
      };
      _committedReadinessProbe = committedProbeToken;
    }
    /* A price that stops being competition-valid IS the outage, so notice it
       on the tick rather than waiting for the next sweep. The sweep still
       polls, to catch a market that has gone silent and therefore ticks not
       at all. */
    try { comp.monitorSegments(comp.currentRound(), now); }
    catch (e) {
      /* Monitoring is what closes a segment whose price world broke. Losing
         it silently was how a round could keep paying out on a market the
         engine had already stopped trusting. */
      _log('segment monitor failed: ' + (e && e.message));
      if ((comp.currentRound() || {}).id) failure = e;
    }
    /* An accepted price is an event: settle any boundary it has passed BEFORE
       using it to liquidate, stop or expire anything. The committed-world swap
       spans fireBoundary and all readiness reads it performs. */
    if (!_sweepRunning) comp.advanceRoundClock(now);
  } finally {
    if (committedProbeToken && _committedReadinessProbe === committedProbeToken) {
      _committedReadinessProbe = null;
    }
    if (hideCandidate) {
      live.map.set(candidateBase, candidateEntry);
      if (candidateConfirming) _confirming.set(candidateBase, candidateConfirming);
      else _confirming.delete(candidateBase);
    }
  }
  if (_sweepRunning) return failure ? { ok: false, error: failure } : { ok: true, busy: true };
  const m = mkt(symbol);
  /* An unpriceable market is not a failed risk pass, it is a market with no
     price to evaluate. The caller has nothing to publish either way. */
  if (!mktFresh(m) && !roundCandidate) return failure ? { ok: false, error: failure } : { ok: true, skipped: 'market not fresh' };
  // Position lookup FIRST, and the throttle only applies when nothing is at
  // risk. The relay broadcasts every accepted price to the chart; evaluating
  // only the newest every 250ms meant a price could be drawn crossing a
  // liquidation line that the engine never saw -- at ~19 broadcasts/s that is
  // ~5 unevaluated prices per window, and raising the relay rate widened the
  // gap. A symbol carrying risk now evaluates on EVERY accepted tick, so the
  // chart cannot show a crossing the engine did not act on. The query is a
  // prepared statement on an indexed column; the expensive transaction below
  // still only runs when positions exist.
  /* A failed segment monitor means this candidate cannot become an accepted
     event. Do not let public or scored risk mutations escape on a price the
     caller is about to restore and suppress. Boundary work above remains
     authoritative because it used only previously committed mark history. */
  if (failure) return { ok: false, error: failure };
  /* Drawdown is a published tie-break, so a mark touching scored exposure is
     sampled before and after its mutation rather than every five seconds. An
     unrelated market cannot move contestant equity and must not rescore the
     whole roster; a dip/recovery or liquidation fee on a held market can. */
  /* Drawdown is a tie-break input, so it must not be sampled while the
     round is paused: an account that skipped its worst dip would win a tie it
     did not earn. */
  /* Per tick, deliberately, and NOT throttled. A 50ms floor was measured as a
     large saving on the hot path, and reverted: drawdown is a tie-break input
     the owner's rules depend on, the suite pins per-tick sampling as a rule
     ("a dip between sweeps is captured"), and a dip that resolves inside one
     floor window is exactly the evidence a tie would turn on. Optimise this
     only with a measurement from a live round in hand. */
  /* Per tick, deliberately, and its failures are NOT silent. Maximum drawdown
     is a published tie-break, so a sample that could not be taken is missing
     evidence, not a cosmetic gap: it is counted, surfaced to ops, and a run of
     failures during a live round pauses rather than quietly deciding a tie on
     an incomplete record. */
  /* The sample itself runs inside the scored mutation transaction below. If a
     later liquidation/stop mutation fails, its drawdown evidence rolls back
     with it instead of surviving for a price the relay correctly refused. */
  const sampleDrawdownNow = () => {
    const sampled = comp.sampleDrawdown(now);
    drawdownChanged = drawdownChanged || !!(sampled && sampled.maxDrawdownChanged);
  };
  const drawdownFailed = (e) => {
    _log(`drawdown sample FAILED: ${e && e.message}`);
    tgOps('ddsample', `drawdown sampling failed: ${e && e.message}`);
    const round = comp.currentRound();
    if (!round) return;
    try {
      if (e && e.unpriced) pauseRound(GLOBAL_FEED_PAUSE_SYMBOL, 'drawdown',
        `tie-break sampling failed: ${e && e.message}`, e.invalidSince);
      else comp.blockRound(round.id, `drawdown sampling failed: ${e && e.message}`);
    } catch { /* the original failure is already logged */ }
  };
  /* Evaluate the base AND its event twins from this one incoming mark, so a
     boosted position is risk-checked on the same tick as the market it
     tracks rather than waiting for the sweep. */
  const ps = stmt.posBySymbolTree.all(symbol, symbol + '-HOT', symbol + '-BOOST');
  /* This is only a cheap candidate filter, never execution authority. In the
     worst ordinary resting book (8 seats x 20 limits), running every order's
     full competition barrier/family checks on every un-crossed tick consumed
     100ms+. The shared Stage price can rule those orders out before any such
     work. Actual crosses still pass every freshness, phase, margin and alias
     check inside sweepOneOrder and the scored event transaction below. The
     periodic sweep retains cleanup of closed aliases and stale intentions. */
  const stageCrossPx = stageMark(m, null, false);
  const stageOrders = [symbol, symbol + '-HOT', symbol + '-BOOST']
    .flatMap((s) => stmt.ordOpenBySymbol.all(s))
    .filter((o) => {
      if (!isStage(heatOf(o.user_id))) return false;
      const round = pricingRoundFor(o.user_id);
      // Candidate filtering is not execution authority; the real order runs
      // later inside the exact candidate transaction and all normal guards.
      const px = round ? (roundCandidate?.record.price
        ?? roundExecutionMark(symbol, { round })?.price) : stageCrossPx;
      return Number.isFinite(px) && px > 0
        && (o.side === 'BUY' ? px <= o.price : px >= o.price);
    }).sort(restingPriceTime);
  if (!ps.length && !stageOrders.length && !roundCandidate) {
    /* A skip is only a skip if nothing already failed. The monitor and the
       tie-break sample run BEFORE these returns, so an early exit here used to
       hand the caller an ok that erased a failure it had just recorded. */
    if (!force && now - (_lastTickEval.get(symbol) || 0) < TICK_EVAL_MIN_MS) {
      return failure ? { ok: false, error: failure }
        : { ok: true, skipped: 'throttled', material: false };
    }
    _lastTickEval.set(symbol, now);
    return failure ? { ok: false, error: failure }
      : { ok: true, skipped: 'no positions', material: false };
  }
  _sweepRunning = true;
  try {
    /* One accepted tick is one event for everybody it touches.
     *
     * Catching per player and carrying on meant a single failed mutation could
     * liquidate player A and leave identical player B untouched, from the same
     * price, with the round still reporting healthy. When a scored account is
     * involved the whole pass is rolled back and the round is blocked, so two
     * equivalent seats cannot be split by iteration order. */
    /* SEPARATE SAVEPOINTS, ONE EVENT COMMIT.
     *
     * Per-public-account faults remain isolated inside their savepoint. But a
     * failure that makes the caller suppress the candidate must roll back all
     * risk effects caused by it: public and competition use one price stream,
     * so neither may retain a liquidation/SL/TP from a mark absent from that
     * stream. The outer event transaction supplies that atomic commit edge. */
    const scoredPs = ps.filter((p) => competitionOwned(p.user_id));
    const publicPs = mktFresh(m) ? ps.filter((p) => !competitionOwned(p.user_id)) : [];
    const scoredOrders = stageOrders.filter((o) => competitionOwned(o.user_id));
    const publicOrders = mktFresh(m) ? stageOrders.filter((o) => !competitionOwned(o.user_id)) : [];
    const publicUids = new Set([...publicPs, ...publicOrders].map((p) => p.user_id));
    const scoredUids = new Set([...scoredPs, ...scoredOrders].map((p) => p.user_id));
    const scoredSubject = scoredPs[0] || scoredOrders[0];
    /* A pause freezes scored risk, except for the exact candidate event that
       proves EVERY current price dependency has recovered. That event is
       evaluated atomically and only resumes the clock after it commits; if
       another leg is still bad, no contestant mutation is allowed through. */
    let recoveryCandidate = false;
    if (roundPaused() && candidateEntry && live.map.get(candidateBase) === candidateEntry) {
      const priorProbe = _riskProbe;
      try {
        /* The same exact-object exception used inside the scored savepoint is
           needed for the preflight proving this candidate completes recovery.
           It is synchronous and restored before any caller can observe it. */
        if (!priorProbe && _riskBlocked.has(candidateBase)) {
          _riskProbe = { sym: candidateBase, entry: candidateEntry };
        }
        recoveryCandidate = competitionClockStatus(now, { ignorePause: true }).ok;
      } finally {
        _riskProbe = priorProbe;
      }
    }

    let recovery = recoveryCandidate ? {
      roundId: (comp.currentRound() || {}).id, pauseRevision: _pauseRevision,
      base: candidateBase, entry: candidateEntry,
    } : null;
    const priorRoundCandidate = _roundPriceCandidate;
    let admission = null;
    try {
    db.transaction(() => {
      if (roundCandidate && comp.currentRound()?.id === roundCandidate.roundId) {
        if (_roundPriceCandidate) throw new Error('round price candidate re-entry');
        _roundPriceCandidate = roundCandidate;
        // The opt-in lineage may be tested only inside this transaction. Its
        // first eligible replacement can complete a paused roster's recovery.
        if (roundPaused()) {
          recoveryCandidate = competitionClockStatus(now, { ignorePause: true }).ok;
          recovery = recoveryCandidate ? {
            roundId: roundCandidate.roundId, pauseRevision: _pauseRevision,
            base: candidateBase, entry: candidateEntry,
          } : null;
          // The read-only dependency probe above is the only permitted use
          // of an uncommitted recovery candidate. If this event cannot run
          // scored risk, no account path or durable round lineage may see
          // that price. Raw public ingestion still has its ordinary rules.
          if (!recoveryCandidate) _roundPriceCandidate = priorRoundCandidate;
        }
      }
      if (publicPs.length || publicOrders.length) {
        db.transaction(() => {
          for (const o of publicOrders) {
            try { atomically(() => sweepOneOrder(o, now)); }
            catch (e) { _log(`tick order ${o.id} error: ${e.message}`); }
          }
          const freshPublic = stmt.posBySymbolTree.all(symbol, symbol + '-HOT', symbol + '-BOOST')
            .filter((p) => publicUids.has(p.user_id));
          for (const p of freshPublic) {
            try { evalPositionAtMark(p, m, now); } catch (e) { _log(`tick ${p.user_id}/${p.symbol} error: ${e.message}`); }
          }
          for (const uid of publicUids) {
            try { evalCrossForUser(uid, now); } catch (e) { _log(`tick user ${uid} error: ${e.message}`); }
          }
        })();
      }

      if (scoredSubject && (!roundPaused() || recoveryCandidate)) {
        let scoredFailure = null;
        let probeToken = null;
        try {
        /* Keep the prior block authoritative through segment monitoring and
           boundary settlement above. Only this scored savepoint may inspect
           the exact candidate as a recovery proof; it is cleared before the
           event loop can service another request. */
        const base = baseOf(symbol);
        if (riskProbeEntry && _riskBlocked.has(base)
            && live.map.get(base) === riskProbeEntry) {
          if (_riskProbe) throw new Error('risk recovery probe re-entry');
          probeToken = { sym: base, entry: riskProbeEntry };
          _riskProbe = probeToken;
        }
        db.transaction(() => {
          if (!roundPaused() || recoveryCandidate) {
            try {
              sampleDrawdownNow();
            } catch (e) {
              scoredFailure = { userId: scoredSubject.user_id,
                what: 'drawdown sample', e };
              throw e;
            }
          }
          // Limits join the accepted-price event before risk, so a brief
          // crossing cannot vanish between sweeps. All scored fills, their
          // protections and the price verdict share this rollback boundary.
          for (const o of scoredOrders) {
            try { sweepOneOrder(o, now); }
            catch (e) {
              scoredFailure = { userId: o.user_id, what: `tick order ${o.id}`, e };
              throw e;
            }
          }
          const freshScored = stmt.posBySymbolTree.all(symbol, symbol + '-HOT', symbol + '-BOOST')
            .filter((p) => scoredUids.has(p.user_id));
          for (const p of freshScored) {
            try { evalPositionAtMark(p, m, now, recovery); }
            catch (e) {
              if (e && e.unpriced) { onSweepError(p.user_id, `tick ${p.user_id}/${p.symbol}`, e); continue; }
              scoredFailure = { userId: p.user_id, what: `tick ${p.user_id}/${p.symbol}`, e }; throw e;
            }
          }
          for (const uid of scoredUids) {
            try { evalCrossForUser(uid, now, recovery); }
            catch (e) {
              if (e && e.unpriced) { onSweepError(uid, `tick user ${uid}`, e); continue; }
              scoredFailure = { userId: uid, what: `tick user ${uid}`, e }; throw e;
            }
          }
          /* The pre-pass preserves the intratick wick. Liquidation slippage,
             fees and funding can then lower equity further; sample the final
             scored mutation in the SAME transaction so neither the financial
             change nor its tie-break evidence can survive alone. */
          if (!roundPaused() || recoveryCandidate) {
            try { sampleDrawdownNow(); }
            catch (e) {
              scoredFailure = { userId: scoredSubject.user_id,
                what: 'post-mutation drawdown sample', e };
              throw e;
            }
          }
          /* This is the commit edge for scored risk. If the source lifetime
             was spent during DD/liquidation work, throwing here rolls back
             every position, fill, funding and tie-break write made from a
             candidate that will never enter accepted history or the relay. */
          if (recovery && pausedRiskBlocked(scoredSubject.user_id, recovery)) {
            const e = new Error('competition recovery changed before commit');
            e.unpriced = true;
            scoredFailure = { userId: scoredSubject.user_id, what: 'recovery risk', e };
            throw e;
          }
          if (Number.isFinite(riskDeadlineMono)
              && monoNow() >= riskDeadlineMono) {
            const expired = new Error('observation expired during scored risk');
            expired.observationExpired = true;
            throw expired;
          }
        })();
        } catch (e) {
          if (e && e.observationExpired) {
            observationExpired = true;
            failure = e;
          } else {
            if (!scoredFailure) throw e;
            /* The competition savepoint rolled back. Record the durable
               pause/block only after the outer event has also rolled back;
               otherwise throwing to undo a public mutation would undo the
               very failure state that makes the round fail closed. */
            deferredScoredFailure = scoredFailure;
            /* A scored position that could not be evaluated is a failed risk pass,
               whatever the public side did: the caller must not publish this tick
               as a normal price. */
            failure = scoredFailure.e;
          }
        } finally {
          if (probeToken && _riskProbe === probeToken) _riskProbe = null;
        }
      }

      if (_roundPriceCandidate === roundCandidate && roundCandidate
          && comp.currentRound()?.id === roundCandidate.roundId) {
        comp.roundMarkCommit(roundCandidate.roundId, roundCandidate.record,
          roundCandidate.expectedIdentity);
      }
      if (observationExpired) throw failure;
      if (failure) throw failure;
      if (candidateEntry) admission = intrinsicCandidateAdmission(candidateBase, candidateEntry, now);
      if (Number.isFinite(riskDeadlineMono)
          && monoNow() >= riskDeadlineMono) {
        const expired = new Error('observation expired during risk event');
        expired.observationExpired = true;
        throw expired;
      }
    })();
    } finally { _roundPriceCandidate = priorRoundCandidate; }
    // Only a successfully returned OUTER transaction can claim admission.
    committedAdmission = admission;
  } catch (e) {
    /* Reported, not swallowed. The caller publishes a price only if the risk
       pass for that tick actually completed, so a failure here has to reach
       it rather than being turned into a log line and a silent success. */
    _log('tickEval error: ' + e.message);
    if (e && e.observationExpired) observationExpired = true;
    failure = e;
  } finally {
    _sweepRunning = false;
    _lastTickEval.set(symbol, now);
  }
  if (deferredScoredFailure && !observationExpired) {
    onSweepError(deferredScoredFailure.userId,
      deferredScoredFailure.what, deferredScoredFailure.e);
  }
  if (observationExpired) return { ok: false, error: failure, expired: true };
  if (failure) return { ok: false, error: failure };
  return { ok: true, material: drawdownChanged, committed: true, admission: committedAdmission };
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
function liquidateCross(userId, eventTime = null) {
  /* The pass's event time, never a fresh Date.now(). Re-reading the clock
     inside a pass let the bell fall between two identical players, freezing
     one before settlement and one after, so identical state produced
     different published scores purely by row order. */
  const now = Number.isFinite(eventTime) ? eventTime : Date.now();
  if (comp.writeBarrier(userId, now)) return;
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
    const execution = execPxFor(userId, worst.symbol, lSide, closeSz, lMark);
    applyFill(userId, {
      symbol: worst.symbol, orderSide: lSide,
      size: closeSz, px: execution.px, feeBps: cfg.takerBps, kind: 'LIQUIDATION',
      executionSource: execution.source, referenceMark: lMark,
      decisionReason: 'cross-maintenance-threshold-breach',
      decisionContext: {
        liquidation: { mode: 'cross', pass: pass + 1, equity: r6(risk.equityCross), maintenance: r6(risk.maint), closeSize: closeSz, maxPasses: LIQ_MAX_PASSES },
      },
      at: now,   // the pass's event time, so the fill belongs to the pass
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
function liquidateIsolated(pos, eventTime = null) {
  const now = Number.isFinite(eventTime) ? eventTime : Date.now();
  if (comp.writeBarrier(pos.user_id, now)) return;
  const cfg = cfgOf(pos.symbol);
  const lSide = pos.side === 'LONG' ? 'SELL' : 'BUY';
  const lMark = posMarkOf(pos);
  const execution = execPxFor(pos.user_id, pos.symbol, lSide, pos.size, lMark);
  const isoEquity = Number(pos.isolated_margin) + uPnl(pos, lMark);
  const isoMaintenance = pos.size * lMark * mmfForPos(pos, isStage(heatOf(pos.user_id)));
  applyFill(pos.user_id, {
    symbol: pos.symbol, orderSide: lSide,
    size: pos.size, px: execution.px, feeBps: cfg.takerBps, kind: 'LIQUIDATION',
    executionSource: execution.source, referenceMark: lMark,
    decisionReason: 'isolated-maintenance-threshold-breach',
    decisionContext: { liquidation: { mode: 'isolated', equity: r6(isoEquity), maintenance: r6(isoMaintenance), allocatedMargin: r6(pos.isolated_margin) } },
    at: now,
  });
  // no balance floor here: the iso loss cap already absorbed any bad debt
  // inside applyFill, and flooring a negative CROSS balance while other
  // positions are open would mint equity
  const acct = stmt.acctGet.get(pos.user_id);
  if (acct) stmt.acctUpd.run(acct.balance, acct.fills_count, acct.fees_paid, acct.funding_paid, acct.liquidations + 1, Date.now(), pos.user_id);
  _log(`iso-liquidated user ${pos.user_id} ${pos.symbol}`);
}

/* Requested protection is part of the order, not an optional afterthought.
   Validate it against the ACTUAL fill and the position that fill would leave.
   A gap may make a stop that was valid at the limit nonsensical at the index;
   filling anyway and silently dropping it creates unprotected exposure. */
function attachedTriggerProblem({ userId, symbol, orderSide, size, fillPx, sl, tp }) {
  const stop = Number(sl) > 0 ? Number(sl) : null;
  const take = Number(tp) > 0 ? Number(tp) : null;
  if (stop == null && take == null) return null;
  const wantLong = orderSide === 'BUY';
  const pos = stmt.posGet.get(userId, symbol);
  const d = wantLong ? 1 : -1;
  let afterDir = d;
  if (pos && dirOf(pos.side) !== d) {
    if (Number(size) > Number(pos.size) + 1e-12) afterDir = d;
    else afterDir = Number(size) < Number(pos.size) - 1e-12 ? dirOf(pos.side) : 0;
  }
  if (afterDir !== d) return { code: 'attached_trigger_has_no_matching_position', fillPx: auditNumber(fillPx) };
  if (stop != null && !(wantLong ? stop < fillPx : stop > fillPx)) {
    return { code: 'attached_sl_invalid_at_fill', trigger: stop, fillPx: auditNumber(fillPx) };
  }
  if (take != null && !(wantLong ? take > fillPx : take < fillPx)) {
    return { code: 'attached_tp_invalid_at_fill', trigger: take, fillPx: auditNumber(fillPx) };
  }
  return null;
}
function applyAttachedTriggers(order, fillPx) {
  const sl = Number(order.attach_sl) > 0 ? Number(order.attach_sl) : null;
  const tp = Number(order.attach_tp) > 0 ? Number(order.attach_tp) : null;
  if (sl == null && tp == null) return;
  const problem = attachedTriggerProblem({
    userId: order.user_id, symbol: order.symbol, orderSide: order.side,
    size: 0, fillPx, sl, tp,
  });
  const pos = stmt.posGet.get(order.user_id, order.symbol);
  const expectSide = order.side === 'BUY' ? 'LONG' : 'SHORT';
  // This is a defensive assertion inside the caller's transaction. Normal
  // paths prevalidate before applying the fill; if state ever disagrees, roll
  // the whole action back instead of losing the requested protection.
  if (problem || !pos || pos.side !== expectSide) {
    throw new Error(`attached trigger invariant failed: ${(problem && problem.code) || 'position_side'}`);
  }
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
    maxLevAtSize: heat
      ? comp.levCapFor(p.symbol, stageLevCap(p.symbol), p.user_id)
      : tierLevFor(p.symbol, p.size),
    boostExpiresAt: p.boost_since ? p.boost_since + BOOST_WINDOW_MS : null,
    openedAt: p.opened_at,
  };
}
function orderPayload(o) {
  return { id: o.id, symbol: o.symbol, side: o.side, type: 'LIMIT', price: o.price, size: o.size,
    notional: r6(o.price * o.size), leverage: o.leverage, reduceOnly: !!o.reduce_only,
    marginMode: o.margin_mode || 'cross', attachSl: o.attach_sl, attachTp: o.attach_tp,
    boostWindow: o.boost_window !== 0, closeReason: o.close_reason || null,
    createdAt: o.created_at };
}
function parseDecisionContext(raw) {
  if (!raw) return null;
  try { return JSON.parse(raw); } catch { return { unreadable: true }; }
}
function fillPayload(f) {
  return { id: f.id, symbol: f.symbol, side: f.side, kind: f.kind, price: f.price, size: f.size,
    notional: f.notional, fee: f.fee, realizedPnl: f.realized_pnl, badDebt: f.bad_debt || 0,
    executionSource: f.execution_source || null, referenceMark: f.reference_mark || null,
    engineBoot: f.engine_boot || null, indexSeq: f.index_seq ?? null,
    decisionContext: parseDecisionContext(f.decision_context),
    epoch: f.epoch, ts: f.ts };
}

// ── handlers ─────────────────────────────────────────────────────────────

// GET /api/paper/account
async function account(req, res) {
  const u = await sessionUser(req); if (!u) return send(res, 401, { ok: false, error: 'not_signed_in' });
  const acct = ensureAccount(u.id);
  const risk = accountRisk(u.id, acct);
  const mode = modeNameOf(acct.heat);
  return send(res, 200, {
    ok: true, apiVersion: PAPER_API_VERSION, schemaVersion: PAPER_SCHEMA_VERSION, buildId: PAPER_BUILD_ID,
    account: {
      // the caller's own id: the competition strip needs to find its row in
      // the public wall feed, which cannot identify who is asking
      userId: u.id,
      balance: acct.balance, equity: r6(risk.equityTotal), free: r6(Math.max(0, risk.free)),
      uPnl: r6(risk.crossUpnl + (risk.isoValue - risk.positions.filter(isIso).reduce((s, p) => s + p.isolated_margin, 0))),
      maintenance: r6(risk.maint), isolatedValue: r6(risk.isoValue),
      epoch: acct.epoch, resets: acct.resets, fillsCount: acct.fills_count,
      feesPaid: acct.fees_paid, fundingPaid: acct.funding_paid, liquidations: acct.liquidations,
      startBalance: acct.start_balance || START_BALANCE, resetAt: acct.reset_at,
      heat: isStage(acct.heat), scaled: isScaled(acct.heat), mode,
      displayScale: PAPER_MODES[mode].displayScale,
      execution: PAPER_MODES[mode].execution,
      fundingModel: PAPER_MODES[mode].fundingModel,
      liquidityModel: PAPER_MODES[mode].liquidityModel,
      queueModel: PAPER_MODES[mode].queueModel,
      sharedLiquidity: PAPER_MODES[mode].sharedLiquidity,
      venueParity: PAPER_MODES[mode].venueParity,
    },
    positions: risk.positions.map((p) => positionPayload(p, risk.positions, acct.balance, isStage(acct.heat))),
    orders: risk.orders.map(orderPayload),
    feeRates: { takerBps: FALLBACK_TAKER_BPS, makerBps: FALLBACK_MAKER_BPS },
    pricesStale: pricingRoundFor(u.id) ? false : !pricesUp(),
  });
}

// POST /api/paper/order
// {symbol, side, type, size?|notionalUsd?, price?, leverage, reduceOnly?,
//  marginMode?:'cross'|'isolated', sl?, tp?}
// sl/tp on MARKET are applied to the position atomically with the fill; on
// LIMIT they ride the order and attach when it fills.
const orderRequestGet = db.prepare('SELECT epoch, payload_hash, response_json FROM paper_order_requests WHERE user_id=? AND request_id=?');
const orderRequestPut = db.prepare('INSERT INTO paper_order_requests(user_id,epoch,request_id,payload_hash,response_json,created_at) VALUES (?,?,?,?,?,?)');
function orderRequestContext(userId, body, req, action = 'order') {
  const raw = body.requestId ?? req.headers['idempotency-key'];
  if (raw != null && (typeof raw !== 'string' || !/^[A-Za-z0-9._:-]{8,128}$/.test(raw))) {
    return { error: 'bad_request_id' };
  }
  if (action !== 'order' && (!raw || !Number.isSafeInteger(body.accountEpoch)
      || body.accountEpoch <= 0)) {
    return { error: 'idempotency_required', status: 409,
      message: 'Refresh the trading page before changing this position.' };
  }
  const epoch = ensureAccount(userId).epoch;
  const payload = Object.fromEntries(Object.keys(body).sort()
    .filter((key) => key !== 'requestId').map((key) => [key, body[key]]));
  // Keep existing order hashes stable across upgrades. Other action kinds
  // use a separate namespace, so one ID cannot accidentally replay an order
  // as a close or a collateral transfer. The same durable table owns them.
  const encoded = action === 'order' ? JSON.stringify(payload)
    : JSON.stringify({ action, payload });
  return { userId, epoch, requestId: raw || null,
    requestedEpoch: body.accountEpoch,
    hash: crypto.createHash('sha256').update(encoded).digest('hex') };
}
function orderRequestReplay(context) {
  const epoch = ensureAccount(context.userId).epoch;
  const row = context.requestId ? orderRequestGet.get(context.userId, context.requestId) : null;
  if (epoch !== context.epoch || (context.requestedEpoch != null && Number(context.requestedEpoch) !== epoch)
      || (row && row.epoch !== epoch)) {
    return { code: 409, body: { ok: false, error: 'idempotency_epoch_changed' } };
  }
  if (!row) return null;
  if (row.payload_hash !== context.hash) {
    return { code: 409, body: { ok: false, error: 'idempotency_conflict' } };
  }
  return { code: 200, body: { ...JSON.parse(row.response_json), idempotentReplay: true } };
}
function requestContextError(res, context) {
  return send(res, context.status || 400, { ok: false, error: context.error,
    ...(context.message ? { message: context.message } : {}) });
}
function respondToOrder(res, context, mutation) {
  const result = atomically(() => {
    const replay = orderRequestReplay(context);
    if (replay) return replay;
    const body = mutation();
    if (context.requestId) {
      body.requestId = context.requestId;
      orderRequestPut.run(context.userId, context.epoch, context.requestId,
        context.hash, JSON.stringify(body), Date.now());
    }
    return { code: 200, body };
  });
  // A lost response leaves a durable result, never a second fill on retry.
  return send(res, result.code, result.body);
}
async function placeOrder(req, res) {
  const u = await sessionUser(req); if (!u) return send(res, 401, { ok: false, error: 'not_signed_in' });
  let body; try { body = JSON.parse(await readBody(req) || '{}'); } catch { return send(res, 400, { ok: false, error: 'bad_json' }); }
  if (!body || typeof body !== 'object' || Array.isArray(body)) return send(res, 400, { ok: false, error: 'bad_request' });
  const requestContext = orderRequestContext(u.id, body, req);
  if (requestContext.error) return requestContextError(res, requestContext);
  const replay = orderRequestReplay(requestContext);
  if (replay) return send(res, replay.code, replay.body);
  if (!writeRateOk(u.id)) return send(res, 429, { ok: false, error: 'rate_limited' });

  /* This request is an event, and it must move the round clock BEFORE any
     phase or gate decision. Checking the alias gate first meant a late timer
     could reject the very order that should have opened the segment, which
     made timers authoritative again for exactly the case they were supposed
     to stop mattering for.

     But the ADVANCE and the BARRIER are one operation, not two. Calling
     advanceRoundClock here settled the round, and the barrier further down
     then asked currentRound() — which no longer returned the round it had
     just ended — so an order that fired its own bell was admitted and wrote a
     position into a frozen result. writeBarrier captures membership before it
     advances, so it is the only thing allowed to move this clock. */
  if (barred(res, u.id)) return;

  const symbol = String(body.symbol || '').toUpperCase();
  const side = body.side === 'BUY' ? 'BUY' : body.side === 'SELL' ? 'SELL' : null;
  if (!requireRoundPriceAcknowledgment(res, body, u.id, symbol)) return;
  const type = body.type === 'LIMIT' ? 'LIMIT' : body.type === 'MARKET' ? 'MARKET' : null;
  const reduceOnly = !!body.reduceOnly;
  // Legacy/free-play Boost-class fills can carry their own auto-close clock.
  // Competition v2 ignores that mechanism: its one shared Boost window is
  // fixed by the active round clock. boostWindow:false is practice-only.
  const boostWindow = body.boostWindow !== false;
  const postOnly = !!body.postOnly && type === 'LIMIT';
  if (!side || !type) return send(res, 400, { ok: false, error: 'bad_request' });

  // An event ticker exists only while its segment is open. Closed covers both
  // "the Hot Market has not been drawn yet" and "the window has ended", and
  // the check is here rather than in the client so a stale tab or a crafted
  // request cannot trade a segment that is not running.
  /* A shut segment ticker admits REDUCING orders only. Reducing exposure on
     a ticker that no longer exists is never the unfair half of anything; it
     is how a stranded position gets out. Opening on it is still refused. */
  if (aliasKind(symbol) && !aliasOpen(symbol) && !reduceOnly) {
    return send(res, 400, { ok: false, error: 'market_closed' });
  }
  /* A competitor may only trade markets the show can price at a boundary.
     An unindexed symbol has no composite history, so a checkpoint holding it
     could not be settled strictly and would block the round at the bell. */
  if (comp.accountLocked(u.id) && !STAGE_INDEXED.has(baseOf(symbol))) {
    return send(res, 400, { ok: false, error: 'market_not_in_competition' });
  }
  const acctH = ensureAccount(u.id);
  /* The requested leverage is read here purely to choose the quality bar; the
     validated value is re-derived below and re-checked on `mark2`. */
  const mark = markOfFreshFor(symbol, isStage(acctH.heat),
    { userId: u.id, forLeverage: Number(body.leverage) || 0 });
  if (!mark) {
    const m0 = mkt(symbol);
    if (m0 && m0.indexHalt) return send(res, 503, { ok: false, error: 'prices_stale', definitive: true });   // guard-halted, not unknown
    return send(res, pricesUp() ? 400 : 503, { ok: false, error: pricesUp() ? 'bad_symbol' : 'prices_stale', definitive: true });
  }

  const cfg = cfgOf(symbol);
  if (cfg.status !== 'active' && cfg.status !== 'postOnly') return send(res, 400, { ok: false, error: 'market_not_active' });

  const pos = stmt.posGet.get(u.id, symbol);
  // margin mode: isolatedOnly markets force isolated; an existing position
  // pins the mode for anything touching that symbol
  if (isStage(acctH.heat) && body.marginMode === 'cross' && (!pos || isIso(pos))) {
    return send(res, 400, { ok: false, error: 'isolated_only' });
  }
  let marginMode = isStage(acctH.heat) || body.marginMode === 'isolated' ? 'isolated' : 'cross';
  /* A SCORED ACCOUNT'S CROSS BOOK IS THE ROUND'S MARKETS, AND NOTHING ELSE.
   *
   * Under cross margin one unpriceable leg makes the whole account's equity
   * unknown, so a contestant holding a tiny position in a quiet market that
   * nobody else trades could not be risk-evaluated at all: their liquidation
   * pass was skipped while an identical rival was liquidated on the same tick.
   * Freezing that market alone, which is what the show wants, is only safe if
   * the leg cannot reach the rest of the account. So a competitor may trade
   * anything the engine prices, but outside the round's own markets it is
   * ring-fenced. Their cross collateral then contains only markets the round
   * is being decided on, which are the ones a pause is for. */
  if (comp.accountLocked(u.id) && marginMode !== 'isolated' && !roundMarkets().has(baseOf(symbol))) {
    marginMode = 'isolated';
  }
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

  /* One underlying, one direction. See familyConflict. */
  const famClash = familyConflict(u.id, symbol, resultingDir(pos, side, size), { reduceOnly });
  if (famClash) {
    return send(res, 400, {
      ok: false,
      error: 'family_direction_conflict',
      message: `${baseOf(symbol)} is one market: you already have a ${famClash.side === 'BUY' ? 'long' : 'short'} ${famClash.kind === 'order' ? 'order' : 'position'} on ${famClash.symbol}. Close it before taking the other side.`,
      conflict: famClash,
    });
  }
  let leverage = Number(body.leverage) || 1;
  leverage = Math.min(Math.max(1, leverage), levCap);

  // SL/TP (optional): validate against the reference price now
  let sl = body.sl == null ? null : Number(body.sl);
  let tp = body.tp == null ? null : Number(body.tp);
  if (sl != null && (!Number.isFinite(sl) || sl <= 0)) return send(res, 400, { ok: false, error: 'bad_trigger' });
  if (tp != null && (!Number.isFinite(tp) || tp <= 0)) return send(res, 400, { ok: false, error: 'bad_trigger' });
  if (reduceOnly && (sl != null || tp != null)) {
    return send(res, 400, { ok: false, error: 'bad_trigger', reason: 'reduce_only_cannot_attach_protection' });
  }
  const wantLong = side === 'BUY';
  if (sl != null && !(wantLong ? sl < refPx : sl > refPx)) return send(res, 400, { ok: false, error: 'bad_trigger' });
  if (tp != null && !(wantLong ? tp > refPx : tp < refPx)) return send(res, 400, { ok: false, error: 'bad_trigger' });

  let marketable = type === 'MARKET' || (side === 'BUY' ? mark <= price : mark >= price);
  // Standard LIMIT semantics come from the opposing LIVE quote, not the mark.
  // Warm every standard limit (including one that looks non-crossing at the
  // mark), otherwise a post-only buy can take an ask below the mark and a
  // harmless buy can be rejected merely because its price exceeds the mark.
  // Stage has no order book and intentionally classifies against its index.
  if (!isStage(acctH.heat)) await awaitBook(symbol);

  /* Re-check the competition gate AFTER any yield. Everything above was
     decided against a phase that may no longer be current: a Hot order can
     start while Hot is open and land after it closed, and an order can start
     before the bell and land after the result was frozen. The gate is cheap;
     a fill that reopens a settled segment is not. */
  /* Order matters here. The barrier runs FIRST because it advances the clock,
     and a boundary becoming due at this moment may close the very segment
     this order is for. Checking the gate before the barrier let a fill land
     on a ticker the barrier then closed, recreating scored exposure after its
     window had ended. Nothing below this point may yield again. */
  const actionAt = Date.now();
  const replayAfterYield = orderRequestReplay(requestContext);
  if (replayAfterYield) return send(res, replayAfterYield.code, replayAfterYield.body);
  if (barred(res, u.id, { symbol, at: actionAt })) return;
  /* Reducing an existing position is always allowed; opening a new one in a
     market we no longer offer is not. */
  if (DISABLED_MARKETS.has(baseOf(symbol)) && !body.reduceOnly) {
    return send(res, 400, { ok: false, error: 'market_disabled' });
  }
  if (aliasKind(symbol) && !aliasOpen(symbol)) {
    return send(res, 400, { ok: false, error: 'market_closed' });
  }
  const levCap2 = comp.levCapFor(symbol, engineCap, u.id);
  if (!(levCap2 > 0)) return send(res, 400, { ok: false, error: 'market_closed' });
  if (leverage > levCap2) leverage = levCap2;
  /* Refresh after the await, and FAIL if the refresh says the price is no
     longer executable. The nullish fallback restored the pre-await mark, so an
     order could execute against exactly the value the freshness recheck had
     just refused. */
  const mark2 = markOfFreshFor(symbol, isStage(acctH.heat), { userId: u.id, forLeverage: leverage });
  if (!(Number(mark2) > 0)) return send(res, 400, { ok: false, error: 'prices_stale' });
  if (type === 'LIMIT' && isStage(acctH.heat)) {
    marketable = side === 'BUY' ? mark2 <= price : mark2 >= price;
  } else if (type === 'LIMIT') {
    const crossesBook = bookCrossesLimit(freshBook(symbol), side, price);
    // A missing/empty book falls back to the mark model, preserving service
    // for markets whose L2 stream is temporarily unavailable.
    marketable = crossesBook == null
      ? (side === 'BUY' ? mark2 <= price : mark2 >= price)
      : crossesBook;
  }
  /* THE PRICE THE TRADER SAW, CHECKED AGAINST THE ONE THAT WILL FILL.
   *
   * A click carries the event the terminal was displaying. If the market has
   * moved further than the band between then and now, the order is not what
   * the trader agreed to: hand back the new price and let them decide again,
   * rather than filling silently at a number they never saw. Orders sent
   * without an expected price (older clients, scripts) are unaffected. */
  const expected = Number(body.evPx);
  if (Number.isFinite(expected) && expected > 0) {
    const driftBps = Math.abs(mark2 - expected) / expected * 1e4;
    if (driftBps > ORDER_REQUOTE_BPS) {
      return send(res, 409, { ok: false, error: 'requote', mark: rpx(mark2), expected, driftBps: Math.round(driftBps * 10) / 10 });
    }
  }
  const acct = ensureAccount(u.id);
  const risk = accountRisk(u.id, acct);
  /* The final barrier and every row it authorizes share one event instant.
     Synchronous risk work can cross a phase boundary in wall time, but it
     cannot turn a pre-boundary accepted action into a post-boundary fill. */
  const now = actionAt;
  // Re-read the position AFTER the awaitBook yield: an SL/TP or liquidation in
  // the sweep can close it during the wait. Using the stale pre-await `pos`
  // for the reduce-only guard let a reduce-only order slip through and OPEN a
  // fresh position (applyFill re-reads null → treats it as an open), bypassing
  // the margin check. Guard on the fresh state.
  const posNow = stmt.posGet.get(u.id, symbol);

  // Stage's published leverage and maintenance rules belong to isolated
  // positions. Never migrate existing cross collateral during an order:
  // preserve an old cross leg for exits, but refuse any add or flip.
  if (isStage(acct.heat) && posNow && !isIso(posNow)
      && (!reducesPosition(posNow, side, size) && !reduceOnly)) {
    return send(res, 400, { ok: false, error: 'stage_cross_reduce_only' });
  }

  if (reduceOnly) {
    if (!posNow || (side === 'BUY') === (posNow.side === 'LONG')) return send(res, 400, { ok: false, error: 'not_reducing' });
    if (size > posNow.size) size = posNow.size;
  }
  /* A resting Boost order reserves at its LIMIT, because that is exactly what
     boostExposureOf will count after insertion. Immediate Stage execution
     reserves at the index mark. Using mark2 for both let a high SELL limit be
     accepted under the frozen cap and then make remaining capacity negative
     the moment the order row appeared. */
  const boostReservationPx = type === 'LIMIT' && !marketable ? price : mark2;
  const boostReservation = type === 'LIMIT' && !marketable;
  let boostCapacity = boostCapacityCheck(u.id, symbol, side, size, boostReservationPx,
    { reservation: boostReservation, reduceOnly });
  if (!boostCapacity.ok) {
    return send(res, 400, {
      ok: false, error: boostCapacity.error,
      maximum: boostCapacity.max, projected: boostCapacity.projected,
      remaining: boostCapacity.remaining,
    });
  }
  /* What the closing slice of a flip returns, at a given execution price.
     Isolated: its margin plus realized, floored at zero because every close
     path caps the loss at the margin. Cross: the margin it reserved; its PnL
     is already in free through crossUpnl. */
  const freedOnFlip = (execSize, atPx) => {
    if (!posNow || (side === 'BUY') === (posNow.side === 'LONG')) return 0;
    const closeQty = Math.min(execSize, posNow.size);
    const realized = closeQty * (atPx - posNow.entry_price) * dirOf(posNow.side);
    return isIso(posNow)
      ? Math.max(0, (posNow.isolated_margin || 0) * (closeQty / posNow.size) + realized)
      : (closeQty * posNow.entry_price) / posNow.leverage;
  };
  if (!reduceOnly) {
    let addedNotional = notional;
    /* A FLIP PAYS FOR ITS NEW LEG WITH THE OLD LEG'S MARGIN. The order that
       reverses a position closes it and opens the remainder in one fill, and
       applyFill credits the closing slice's margin before it debits the new
       leg. This check ran on free balance as it stood BEFORE the close, so a
       max-sized isolated position, whose margin is not in free at all, could
       never be reversed: "Not enough free margin" for a trade that would
       have ended with exactly the margin it started with. Count what the
       closing slice returns, the way the fill will: an isolated slice gives
       back its margin plus realized, floored at zero because every close
       path caps the loss at the margin; a cross slice releases the margin it
       reserved, its PnL already being in free through crossUpnl. */
    if (posNow && (side === 'BUY') !== (posNow.side === 'LONG')) addedNotional = Math.max(0, (size - posNow.size) * refPx);
    const freedByClose = freedOnFlip(size, refPx);
    const freeForOrder = risk.free + freedByClose;
    // require margin + fee + slippage cost so a max-size fill can't push the
    // account negative past its reservation (taker terms when this order
    // would execute now, maker terms when it rests). Stage fills execute AT
    // the mark with zero fees — reserving for costs that cannot occur would
    // wrongly reject boundary-exact max sends.
    const stageAcct = isStage(acctH.heat);
    const feeRate = stageAcct ? 0 : (marketable ? cfg.takerBps : cfg.makerBps) / 1e4;
    const slip = stageAcct ? 0 : (marketable ? slipBps(notional) / 1e4 : 0);
    let required = addedNotional / leverage + notional * (feeRate + slip);
    if (required > freeForOrder + 1e-9) {
      const flipping = freedByClose > 0 && posNow && size > posNow.size + 1e-12;
      if (stageAcct && flipping && freeForOrder > 0) {
        /* A FLIP SHRINKS TO WHAT THE CLOSE LEAVES. At 100x with the whole
           bankroll in the position, a loss of a few dollars means the same
           size the other way is no longer affordable, and refusing the whole
           order left the trader unable to reverse at all. The closing leg is
           never touched; the new leg is the largest the returned margin plus
           free balance can carry. Stage only: fills are exact and fee-free
           there, so the size the check affords is the size that fills. */
        const newLeg = Math.floor(((freeForOrder * leverage) / refPx) * 1e6) / 1e6;
        if (!(newLeg > 0)) return send(res, 400, { ok: false, error: 'insufficient_margin' });
        size = r6(posNow.size + newLeg);
        notional = size * refPx;
        addedNotional = newLeg * refPx;
        required = addedNotional / leverage;
        boostCapacity = boostCapacityCheck(u.id, symbol, side, size, boostReservationPx,
          { reservation: boostReservation, reduceOnly });
        if (!boostCapacity.ok) {
          return send(res, 400, {
            ok: false, error: boostCapacity.error,
            maximum: boostCapacity.max, projected: boostCapacity.projected,
            remaining: boostCapacity.remaining,
          });
        }
      } else if (stageAcct && freeForOrder > 0 && addedNotional > 0) {
        // full-send tolerance: clamp to what margin affords instead of
        // rejecting over price drift or float dust. Size from the budget
        // with FLOOR — r6 rounds half-up and can round the size back past
        // the budget by ~1e-7 BTC (≈ $0.03 notional), which fails the
        // re-check and made max-sends reject on a price-dependent coin flip.
        const shrink = (freeForOrder * leverage) / addedNotional;
        if (shrink > 0.90 && freedByClose === 0) {
          /* Shrink-to-fit is for a fresh send; a flip that shrinks would no
             longer be a flip, so it is refused outright instead. */
          size = Math.floor(((freeForOrder * leverage) / refPx) * 1e6) / 1e6;
          if (!(size > 0)) return send(res, 400, { ok: false, error: 'insufficient_margin' });
          notional = size * refPx;
          required = notional / leverage;
          /* Size changed after the first reservation check. The final order,
             not the requested one, is the capacity invariant. */
          boostCapacity = boostCapacityCheck(u.id, symbol, side, size, boostReservationPx,
            { reservation: boostReservation, reduceOnly });
          if (!boostCapacity.ok) {
            return send(res, 400, {
              ok: false, error: boostCapacity.error,
              maximum: boostCapacity.max, projected: boostCapacity.projected,
              remaining: boostCapacity.remaining,
            });
          }
        }
      }
      if (required > freeForOrder + 1e-9) return send(res, 400, { ok: false, error: 'insufficient_margin' });
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
        return respondToOrder(res, requestContext, () => {
          const info = stmt.ordInsWithBoost.run(u.id, acct.epoch, symbol, side, price, size, leverage, reduceOnly ? 1 : 0, now, marginMode, sl, tp, boostWindow ? 1 : 0);
          return { ok: true, order: orderPayload(stmt.ordGet.get(Number(info.lastInsertRowid))) };
        });
      }
      execPx = rpx(mark2);
      execSource = 'composite-index';
    } else {
    const bx = bookExec(symbol, side, size, mark2, type === 'LIMIT' ? price : null);
    if (bx) {
      if (bx.filledBase <= 0) {
        if (type === 'MARKET') return send(res, 400, { ok: false, error: 'slippage_exceeded' });
        // marketable limit with nothing crossable inside the collar: rest it all
        if (stmt.ordCountOpen.get(u.id).n >= MAX_OPEN_ORDERS) return send(res, 400, { ok: false, error: 'too_many_orders' });
        return respondToOrder(res, requestContext, () => {
          const info = stmt.ordInsWithBoost.run(u.id, acct.epoch, symbol, side, price, size, leverage, reduceOnly ? 1 : 0, now, marginMode, sl, tp, boostWindow ? 1 : 0);
          return { ok: true, order: orderPayload(stmt.ordGet.get(Number(info.lastInsertRowid))) };
        });
      }
      execSize = snapLots(symbol, bx.filledBase, isScaled(acctH.heat)) || bx.filledBase;
      execSize = Math.min(execSize, size);
      execPx = rpx(bx.vwap);
      execSource = 'book';
      if (type === 'LIMIT' && execSize < size - 1e-9) restRemainder = r6(size - execSize);
    } else {
      // no live book: empirical impact model, clamped at a limit's own price
      execPx = takerPx(mark2, side, notional);
      if (type === 'LIMIT') execPx = side === 'BUY' ? Math.min(execPx, price) : Math.max(execPx, price);
      execSource = 'model';
    }
    }

    const triggerProblem = attachedTriggerProblem({
      userId: u.id, symbol, orderSide: side, size: execSize, fillPx: execPx, sl, tp,
    });
    if (triggerProblem) {
      return send(res, 400, {
        ok: false, error: 'bad_trigger', reason: triggerProblem.code,
        executionPrice: rpx(execPx), trigger: triggerProblem.trigger ?? null,
      });
    }

    /* A marketable GTC can fill part now and reserve the rest. applyFill's
       last-line check sees only the executed slice, while order insertion
       happens afterwards in the same transaction, so validate the aggregate
       action before either mutation. */
    if (restRemainder > 0 && aliasKind(symbol) === 'BOOST' && !reduceOnly) {
      const fillCapacity = boostCapacityCheck(u.id, symbol, side, execSize, execPx);
      const combinedProjected = r6(Number(fillCapacity.projected) + restRemainder * price);
      if (!fillCapacity.ok || combinedProjected > Number(fillCapacity.max) + 1e-9) {
        return send(res, 400, {
          ok: false, error: 'boost_capacity_exceeded',
          maximum: fillCapacity.max, projected: combinedProjected,
          remaining: fillCapacity.remaining,
        });
      }
    }

    let actualMarginCheck = null;
    /* The first check used the requested/reference price because execution had
       not been quoted yet. A live ask can be up to the 1% collar beyond that
       estimate, so revalidate against the actual VWAP before any row changes.
       Include the new leg's immediate mark-to-fill loss and any GTC remainder;
       otherwise an accepted fill can create negative free collateral on entry.
       Reductions remain exempt because they only remove exposure. */
    if (!reduceOnly && !isStage(acctH.heat)) {
      const sameDirection = !posNow || ((side === 'BUY') === (posNow.side === 'LONG'));
      const openingSize = sameDirection ? execSize : Math.max(0, execSize - posNow.size);
      const openingNotional = openingSize * execPx;
      const executionNotional = execSize * execPx;
      const adverseGap = execSize * Math.max(0,
        side === 'BUY' ? execPx - mark2 : mark2 - execPx);
      const remainderMargin = restRemainder > 0 ? (restRemainder * price) / leverage : 0;
      const actualRequired = openingNotional / leverage
        + executionNotional * (cfg.takerBps / 1e4)
        + adverseGap + remainderMargin;
      /* Same credit as the reservation check, at the price that will fill. */
      const actualFreed = sameDirection ? 0 : freedOnFlip(execSize, execPx);
      const actualAvailable = risk.free + actualFreed;
      actualMarginCheck = {
        available: r6(Math.max(0, actualAvailable)), required: r6(actualRequired),
        freedByClose: r6(actualFreed),
        openingNotional: r6(openingNotional), executionNotional: r6(executionNotional),
        adverseMarkGap: r6(adverseGap), restingRemainderMargin: r6(remainderMargin),
      };
      if (actualRequired > actualAvailable + 1e-9) {
        return send(res, 400, {
          ok: false, error: 'insufficient_margin',
          required: r6(actualRequired), available: r6(Math.max(0, actualAvailable)),
        });
      }
    }

    /* ONE logical action, one transaction: the order row, the fill, the
       uncrossed remainder and the attached triggers. Filling and then failing
       to rest the remainder (or to attach a stop) used to leave a trader with
       exposure they did not ask for and no protection they did ask for. */
    let fill, restedOrder = null, triggers;
    return respondToOrder(res, requestContext, () => {
      let orderId = null;
      if (type === 'LIMIT') {
        const info = stmt.ordInsWithBoost.run(u.id, acct.epoch, symbol, side, price, execSize, leverage, reduceOnly ? 1 : 0, now, marginMode, sl, tp, boostWindow ? 1 : 0);
        orderId = Number(info.lastInsertRowid);
        stmt.ordClose.run('FILLED', now, orderId);
      }
      fill = applyFill(u.id, {
        symbol, orderSide: side, size: execSize, px: execPx,
        feeBps: cfg.takerBps, kind: 'MARKET', orderId, leverage, marginMode,
        boostWindow, at: now, executionSource: execSource, referenceMark: mark2,
        decisionReason: type === 'LIMIT' ? 'marketable-limit-immediate' : 'market-order-immediate',
        decisionContext: {
          order: { type, limitPrice: price, requestedSize: size, executedSize: execSize, restingRemainder: restRemainder, postOnly },
          margin: actualMarginCheck,
          trigger: sl != null || tp != null ? { sl, tp, validatedAtFill: true, fillPrice: execPx } : null,
          liquidity: { model: PAPER_MODES[modeNameOf(acctH.heat)].liquidityModel, executedSize: execSize, requestedSize: size },
        },
      });
      // cross-then-rest: the uncrossed remainder of a marketable limit rests
      if (restRemainder > 0 && stmt.ordCountOpen.get(u.id).n < MAX_OPEN_ORDERS) {
        const info = stmt.ordInsWithBoost.run(u.id, acct.epoch, symbol, side, price, restRemainder, leverage, reduceOnly ? 1 : 0, now, marginMode, sl, tp, boostWindow ? 1 : 0);
        restedOrder = orderPayload(stmt.ordGet.get(Number(info.lastInsertRowid)));
      }
      // Attach only after the strict prevalidation above. The helper asserts
      // the post-fill state and throws inside this transaction on disagreement,
      // rolling back the order, fill and position together.
      if (sl != null || tp != null) {
        applyAttachedTriggers({ user_id: u.id, symbol, side, attach_sl: sl, attach_tp: tp }, execPx);
        triggers = { sl, tp, droppedSl: false, droppedTp: false };
      }
    return {
      ok: true, fill, triggers, order: restedOrder || undefined,
      requestedSize: size !== execSize ? size : undefined,
      execution: execSource,
      /* Which index event governed this fill, so a trader (or an audit) can
         join what was on screen to what was executed. */
      event: { boot: ENGINE_BOOT_ID, q: _symSeq.get(baseOf(symbol)) || 0, mark: rpx(mark2) },
    };
    });
  }

  if (stmt.ordCountOpen.get(u.id).n >= MAX_OPEN_ORDERS) return send(res, 400, { ok: false, error: 'too_many_orders' });
  return respondToOrder(res, requestContext, () => {
    const info = stmt.ordInsWithBoost.run(u.id, acct.epoch, symbol, side, price, size, leverage, reduceOnly ? 1 : 0, now, marginMode, sl, tp, boostWindow ? 1 : 0);
    return { ok: true, order: orderPayload(stmt.ordGet.get(Number(info.lastInsertRowid))) };
  });
}

// POST /api/paper/cancel {orderId}
async function cancelOrder(req, res) {
  const u = await sessionUser(req); if (!u) return send(res, 401, { ok: false, error: 'not_signed_in' });
  if (barred(res, u.id, { allowWhilePaused: true })) return;
  // re-checked again after the body read, immediately before any mutation
  if (!writeRateOk(u.id)) return send(res, 429, { ok: false, error: 'rate_limited' });
  let body; try { body = JSON.parse(await readBody(req) || '{}'); } catch { return send(res, 400, { ok: false, error: 'bad_json' }); }
  if (barred(res, u.id, { allowWhilePaused: true })) return;
  const o = stmt.ordGet.get(Number(body.orderId));
  if (!o || o.user_id !== u.id || o.status !== 'OPEN') return send(res, 404, { ok: false, error: 'not_found' });
  stmt.ordClose.run('CANCELLED', Date.now(), o.id);
  return send(res, 200, { ok: true });
}

// POST /api/paper/close {symbol, requestId, accountEpoch, size?|sizeUsd?|pct?}
// Omit size/pct for a full close; replay the exact body to reconcile a retry.
async function closePosition(req, res) {
  const u = await sessionUser(req); if (!u) return send(res, 401, { ok: false, error: 'not_signed_in' });
  let body; try { body = JSON.parse(await readBody(req) || '{}'); } catch { return send(res, 400, { ok: false, error: 'bad_json' }); }
  if (!body || typeof body !== 'object' || Array.isArray(body)) return send(res, 400, { ok: false, error: 'bad_request' });
  const requestContext = orderRequestContext(u.id, body, req, 'close');
  if (requestContext.error) return requestContextError(res, requestContext);
  const replay = orderRequestReplay(requestContext);
  if (replay) return send(res, replay.code, replay.body);
  if (!writeRateOk(u.id)) return send(res, 429, { ok: false, error: 'rate_limited' });
  if (barred(res, u.id)) return;
  const symbol = String(body.symbol || '').toUpperCase();
  if (!requireRoundPriceAcknowledgment(res, body, u.id, symbol)) return;
  /* Closing is refused too, for the same reason a pause refuses it: with no
     mark the engine will stand behind there is no honest exit price either. */
  if (barred(res, u.id, { symbol })) return;
  if (!stmt.posGet.get(u.id, symbol)) return send(res, 404, { ok: false, error: 'no_position' });
  /* Stage fills execute at the index and never consult the book, so the wait
     buys nothing and only widens the window in which the bell can pass under
     an in-flight close. */
  if (!isStage(heatOf(u.id))) await awaitBook(symbol);
  /* Re-check AFTER the await and immediately before mutating: the round may
     have ended or blocked while this request was parked. */
  const closeAt = Date.now();
  const replayAfterYield = orderRequestReplay(requestContext);
  if (replayAfterYield) return send(res, replayAfterYield.code, replayAfterYield.body);
  if (barred(res, u.id, { at: closeAt })) return;
  const pos = stmt.posGet.get(u.id, symbol);
  if (!pos) return send(res, 404, { ok: false, error: 'no_position' });
  const mark = markOfFreshFor(symbol, isStage(heatOf(u.id)), { userId: u.id, forLeverage: levOfPosition(u.id, symbol) });
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
  const execution = execPxFor(u.id, symbol, closeSide, size, mark);
  return respondToOrder(res, requestContext, () => {
    const fill = applyFill(u.id, {
      symbol, orderSide: closeSide, size, px: execution.px,
      feeBps: cfgOf(symbol).takerBps, kind: 'MARKET',
      at: closeAt,
      executionSource: execution.source, referenceMark: mark,
      decisionReason: 'manual-position-close',
      decisionContext: { order: { type: 'close', requestedSize: size, positionSizeBefore: pos.size } },
    });
    return { ok: true, fill };
  });
}

// POST /api/paper/margin {symbol, amount, requestId, accountEpoch}
// (+amount moves balance → position, −amount frees margin back to balance;
// mirrors Phoenix TransferCollateral between cross and the child subaccount)
async function adjustMargin(req, res) {
  const u = await sessionUser(req); if (!u) return send(res, 401, { ok: false, error: 'not_signed_in' });
  let body; try { body = JSON.parse(await readBody(req) || '{}'); } catch { return send(res, 400, { ok: false, error: 'bad_json' }); }
  if (!body || typeof body !== 'object' || Array.isArray(body)) return send(res, 400, { ok: false, error: 'bad_request' });
  const requestContext = orderRequestContext(u.id, body, req, 'margin');
  if (requestContext.error) return requestContextError(res, requestContext);
  const replay = orderRequestReplay(requestContext);
  if (replay) return send(res, replay.code, replay.body);
  if (!writeRateOk(u.id)) return send(res, 429, { ok: false, error: 'rate_limited' });
  if (barred(res, u.id)) return;
  const symbol = String(body.symbol || '').toUpperCase();
  if (!requireRoundPriceAcknowledgment(res, body, u.id, symbol)) return;
  const amount = Number(body.amount);
  if (!Number.isFinite(amount) || amount === 0 || Math.abs(amount) > MAX_NOTIONAL) return send(res, 400, { ok: false, error: 'bad_size' });
  const pos = stmt.posGet.get(u.id, symbol);
  if (!pos || !isIso(pos)) return send(res, 404, { ok: false, error: 'no_position' });
  const mark = markOfFreshFor(symbol, isStage(heatOf(u.id)), { userId: u.id, forLeverage: levOfPosition(u.id, symbol) });
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
    const positionImf = 1 / Math.max(1, Number(pos.leverage) || 1);
    const minImf = Math.max(imfFor(symbol, pos.size), positionImf);
    if (eqAfter < pos.size * mark * minImf - 1e-9) return send(res, 400, { ok: false, error: 'insufficient_margin' });
  }
  const now = Date.now();
  return respondToOrder(res, requestContext, () => {
    stmt.posUpd.run(pos.size, pos.entry_price, pos.leverage, pos.realized_pnl, r6(pos.isolated_margin + amt), now, u.id, symbol);
    stmt.acctUpd.run(r6(acct.balance - amt), acct.fills_count, acct.fees_paid, acct.funding_paid, acct.liquidations, now, u.id);
    return { ok: true, position: { symbol, isolatedMargin: r6(pos.isolated_margin + amt) } };
  });
}

// POST /api/paper/sltp {symbol, sl?, tp?}
async function setSltp(req, res) {
  const u = await sessionUser(req); if (!u) return send(res, 401, { ok: false, error: 'not_signed_in' });
  if (barred(res, u.id)) return;
  // re-checked again after the body read, immediately before any mutation
  if (!writeRateOk(u.id)) return send(res, 429, { ok: false, error: 'rate_limited' });
  let body; try { body = JSON.parse(await readBody(req) || '{}'); } catch { return send(res, 400, { ok: false, error: 'bad_json' }); }
  if (barred(res, u.id)) return;
  const symbol = String(body.symbol || '').toUpperCase();
  if (!requireRoundPriceAcknowledgment(res, body, u.id, symbol)) return;
  const pos = stmt.posGet.get(u.id, symbol);
  if (!pos) return send(res, 404, { ok: false, error: 'no_position' });
  const mark = markOfFreshFor(symbol, isStage(heatOf(u.id)), { userId: u.id, forLeverage: levOfPosition(u.id, symbol) });
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
  /* A seated competitor does not own their own account. Without this a player
     who blows up at minute 20 can restore the full bankroll and keep trading,
     and the epoch bump would move the scoring basis under a live result.
     Locked from the moment they are seated on an armed round until it ends;
     the operator resets them as part of starting the round. */
  if (comp.accountLocked(u.id)) {
    return send(res, 409, { ok: false, error: 'in_competition_round' });
  }
  // mode switch: 'heat' = FT stage config ($10 real bankroll, display-scaled,
  // no fees/funding, fractional lots); 'scaled' = venue economics on the $10
  // micro bankroll; 'standard' restores the $10k account.
  // (bodyRaw was never assigned here: mode switching via this endpoint has
  // silently never worked. Read the body before parsing it.)
  const bodyRaw = await readBody(req);
  let mode = null;
  try { mode = (JSON.parse(bodyRaw || '{}').mode) || null; } catch {}
  /* Reading a request body yields. A seat may have been armed/started, or
     another reset may have committed, while this request was waiting. Use
     the final authority and account state immediately before any mutation. */
  if (barred(res, u.id)) return;
  if (comp.accountLocked(u.id)) {
    return send(res, 409, { ok: false, error: 'in_competition_round' });
  }
  const acct = ensureAccount(u.id);
  const now = Date.now();
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
    // Reserved rehearsal drivers are implementation actors, not entrants.
    // Letting their fills onto the resettable public board makes every solo
    // run publish anonymous fake competitors and can displace real people.
    if (comp.isBotId(a.user_id)) continue;
    const ps = posByUser.get(a.user_id) || [];
    if (stage) {
      // Stage and scaled paper both use a $10 internal ledger, but they are
      // different products (index/zero-fee competition vs venue-like paper).
      // The reset contract says scaled is off-board; truthiness mixed heat=2
      // into the Stage ranking and mislabeled it as a competition account.
      if (!isStage(a.heat)) continue;
      if (!(Number(a.fills_count) > 0 || ps.length > 0)) continue;
    } else {
      if (Number(a.heat) !== 0) continue;
      try { if (auth.isGuestUser(a.user_id)) continue; } catch {}
    }
    let equity = a.balance;
    for (const p of ps) {
      const mk = posMarkOf(p);
      equity += isIso(p)
        ? Math.max(0, p.isolated_margin + uPnl(p, mk))
        : uPnl(p, mk);
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
/* A snapshot is worth sending only while the price is still inside the budget
   the ENGINE would price off. Ten seconds was its own number, so a browser
   could be handed a price the engine itself would refuse, count it as data,
   clear its transport failure state and show it as fresh. Each source keeps
   its own deadline; the snapshot borrows it. */
const snapshotAge = (m) => Date.now() - Number(m && m.pythAtMs || 0);
/* A SNAPSHOT IS THE EXECUTABLE MARK, OR IT IS NOT A PRICE.
 *
 * This measured age from the ACCEPT time and checked only the halt flag, so a
 * quote that arrived 550ms old and was accepted 100ms ago looked 100ms old and
 * was replayed to a reconnecting browser as an ordinary live price, at 650ms
 * against a 600ms budget. It also ignored every other gate the order path
 * applies: a jump under confirmation, a source we could not record, a symbol
 * whose last observation failed its risk pass. A newly connected screen was
 * therefore shown prices the engine would refuse to trade on.
 *
 * Age is now measured from the SOURCE's own time, and eligibility is the same
 * question the scored order path asks. A market with no executable mark sends
 * no price at all and waits for its next real tick. */
const snapshotWorthy = (m, sym) => !!m && !m.indexHalt && Number(m.pythPrice) > 0
  && (Date.now() - (Number(m.pythSrcAtMs) > 0 ? Number(m.pythSrcAtMs) : Number(m.pythAtMs) || 0)) < staleMsForSym(sym || m.symbol, m.srcKey)
  && snapshotAge(m) < staleMsForSym(sym || m.symbol, m.srcKey)
  && compPriceReady(sym || m.symbol);
/* Every frame says which event it is, how old the observation was when it
   left, and how long it stays executable, so a client never has to infer any
   of the three from its own clock. */
/* `eventAtMs` is when the engine accepted the observation and is what the
   chart's grid is keyed to; `srcAtMs` is when the SOURCE observed it, and is
   the only honest basis for an age and a remaining life. Deriving both from
   the accept time reissued a full source budget to a quote that had already
   spent most of it in flight. */
function priceFrame(sym, px, eventAtMs, seq, srcKey, snapshot, material = false, symSeq = 0, srcAtMs = 0, fence = false, acceptedSeq = 0) {
  const basis = Number(srcAtMs) > 0 ? Number(srcAtMs) : Number(eventAtMs || 0);
  const age = Math.max(0, Date.now() - basis);
  const ttl = Math.max(0, staleMsForSym(sym, srcKey) - age);
  /* AN ABSOLUTE DEADLINE, NOT JUST A REMAINDER.
   *
   * `a` and `ttl` are measured at send, so a browser that reconstructs them
   * from ARRIVAL hands back every millisecond the frame spent in a proxy
   * buffer or a suspended tab: a five second hold renewed a 600ms quote in
   * full. `x` is the instant this observation stops being executable, on the
   * engine's clock, which the client already tracks from `t`. It cannot be
   * renewed by being late. */
  const x = Math.round(basis + staleMsForSym(sym, srcKey));
  return `{"v":2,"boot":"${ENGINE_BOOT_ID}","s":"${sym}","p":${px},"t":${eventAtMs},"q":${seq},"sq":${symSeq},"aq":${Number.isSafeInteger(acceptedSeq) && acceptedSeq >= 0 ? acceptedSeq : 0},"a":${Math.round(age)},"ttl":${Math.round(ttl)},"x":${x}`
    + `,"src":${JSON.stringify(srcKey || null)}${snapshot ? ',"snap":1' : ''}${fence ? ',"fence":1' : ''}${material ? ',"m":1' : ''}}`;
}
/* symbol -> the identity of the event whose price is currently the mark, set
   at publication and read by reconnect snapshots. A mark that was never
   published has no delivery identity and must not borrow one. */
const _pubId = new Map();
function pubIdFor(sym, m) {
  const p = _pubId.get(sym);
  return p && p.atMs === m.pythAtMs
    && Number(p.acceptedSeq || 0) === Number(m.acceptedSeq || 0)
    ? { seq: p.seq, symSeq: p.symSeq } : { seq: 0, symSeq: 0 };
}
const _symSeq = new Map();            // symbol -> global seq of its newest published event
const _symCount = new Map();          // symbol -> how many frames it has published
const pythSseClients = new Set();
/* Competition ranks share the index transport, but have their own revision:
   price sequence gaps and board sequence gaps describe different streams. */
const _compGapConfigured = Number(process.env.PAPER_COMP_PUSH_MS);
const COMP_BOARD_GAP_MS = Number.isFinite(_compGapConfigured)
  /* Match the fast price relay cadence so a PnL-changing mark and the rank it
     causes arrive in the same perceptual frame. Four-player stage rounds do
     not need the old 100ms batch delay; the 25ms floor still bounds churn. */
  ? Math.max(25, Math.min(1000, _compGapConfigured)) : 40;
let _compBoardTimer = null;
let _compBoardRevision = 0;
let _compExpiryTimer = null;
/* One synchronous event owns the loop until the coalescer runs. The 512-event
   ceiling covers 32 seats × (three aliases + twelve cross passes), plus a
   boundary close. An overflow is explicit on the wire, never silently lost. */
const COMP_CAUSE_EVENT_MAX = 512;
/* Keep one complete websocket message below the relay's 64KiB admission
   ceiling. Competition fences are encoded once and fanned out once per
   client; 78 exposed bases must not become 78 synchronous sends per viewer. */
const COMP_WS_BUNDLE_LIMIT = 65_536;
const _compDirtySymbols = new Set();
const _compPendingCauses = [];
let _compCauseOverflow = false;
const _compFillCauseRow = db.prepare(
  'SELECT user_id, symbol, price, reference_mark, ts FROM paper_fills WHERE id = ?'
);
const _sseIpCount = new Map();        // one address must not own the fallback pool
const _sseIp = new WeakMap();        // response -> the address it was counted against
/* The mirror of dropWs: one exit that removes the client, closes the stream
   and decrements the address count exactly once. Control and keepalive writes
   used to delete the response straight from the set, so the close handler
   found it gone and never released the slot; enough failures and the venue's
   single address ran out of fallback capacity with nobody connected. */
function dropSse(res) {
  const had = pythSseClients.delete(res);
  /* A response over the backpressure ceiling must release its queued socket
     memory, not merely call end() and wait forever for a peer that stopped
     reading. Normal close paths are already closed, so destroy is harmless
     there and gives every exit the same bounded lifetime. */
  try {
    if (typeof res.destroy === 'function') res.destroy();
    else if (res.socket && typeof res.socket.destroy === 'function') res.socket.destroy();
    else res.end();
  } catch { /* already gone */ }
  if (!had) return;
  const ip = _sseIp.get(res);
  if (!ip) return;
  _sseIp.delete(res);
  const n = (_sseIpCount.get(ip) || 1) - 1;
  if (n > 0) _sseIpCount.set(ip, n); else _sseIpCount.delete(ip);
}
/* THE SAME ROOM, THE SAME ADDRESS, AND THE PATH THAT ONLY GETS USED WHEN THE
   OTHER ONE IS BLOCKED. The websocket cap is 64 precisely because contestants,
   the wall and the desk arrive from one venue NAT; SSE sat at 24, so the exact
   failure that makes SSE necessary (a proxy refusing upgrades) would have cut
   the room off after the 24th tab. */
const SSE_PER_IP = Number(process.env.PAPER_SSE_PER_IP || 64);
const _compProtocol = new WeakMap();
function requestedCompProtocol(req) {
  try { return new URL(req.url, 'http://x').searchParams.get('compProtocol') === '3' ? 3 : 2; }
  catch { return 2; }
}
function pythStream(req, res) {
  if (pythSseClients.size >= PYTH_SSE_MAX_CLIENTS) return send(res, 503, { ok: false, error: 'stream_full' });
  const ip = String((req.headers && req.headers['x-real-ip']) || (req.socket && req.socket.remoteAddress) || 'local');
  if ((_sseIpCount.get(ip) || 0) >= SSE_PER_IP) return send(res, 429, { ok: false, error: 'too_many_streams' });
  res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', connection: 'keep-alive' });
  // snapshot so the client has a price before the first live tick
  /* A halted symbol keeps its last good price in the map, and a quiet one
     keeps a price from minutes ago. Replaying either to a client that just
     connected painted it as a live tick, under a green badge, on a market the
     engine would refuse to trade. Only fresh, unhalted prices are worth
     sending; everything else arrives on its next real tick. */
  for (const [sym, m] of live.map) {
    if (!snapshotWorthy(m, sym)) continue;
    const id = pubIdFor(sym, m);
    res.write(`data: ${priceFrame(sym, m.pythPrice, m.pythAtMs, id.seq, m.srcKey, true, false, id.symSeq, m.pythSrcAtMs, false, m.acceptedSeq)}\n\n`);
  }
  res.write(`data: ${controlFrame('hello')}\n\n`);
  pythSseClients.add(res);
  _sseIpCount.set(ip, (_sseIpCount.get(ip) || 0) + 1);
  _sseIp.set(res, ip);
  _compProtocol.set(res, requestedCompProtocol(req));
  res.on('close', () => dropSse(res));
  req.on('close', () => dropSse(res));
  /* The replay above is instant; this refresh makes it authoritative at the
     moment of connection and seeds exposure after a cold engine start. */
  scheduleCompBoard(null, true);
}
/* The control channel. It is deliberately independent of market activity: a
   quiet market and a dead pipe are the same picture without it, and the
   client needs a way to know the engine is still there and which event it
   published last. */
function controlFrame(kind, extra = null) {
  /* `aq` orders fail-closed controls against cadence-thinned score fences.
     Published q cannot do that because a valid accepted mark may deliberately
     have no ordinary relay frame. */
  return JSON.stringify(Object.assign({ v: 2, type: kind, boot: ENGINE_BOOT_ID,
    q: _evSeq, aq: _evAccepted, t: Date.now() }, extra || {}));
}
function pythControlBroadcast(extra) {
  const payload = controlFrame((extra && extra.type) || 'control', extra);
  const line = `data: ${payload}\n\n`;
  for (const res of pythSseClients) {
    if (res.writableLength > 262144) { dropSse(res); continue; }   // same ceiling as the price path
    try { res.write(line); } catch { dropSse(res); }
  }
  for (const ws of indexWsClients) { if (ws.readyState === 1) { try { ws.send(payload); } catch { dropWs(ws); } } }
}

/* One compact score calculation for the whole fleet. The REST wall payload
   remains the source of phase/clock/recap structure; this frame replaces only
   the rank-bearing fields between polls. */
function flushCompBoard() {
  /* Expiry may pre-empt an ordinary coalesced refresh. Cancel that handle as
     well as forgetting it, otherwise it wakes later and publishes a duplicate
     revision from the same state. */
  if (_compBoardTimer) clearTimeout(_compBoardTimer);
  _compBoardTimer = null;
  /* No cached board is replayed. A board is meaningful only beside the mark
     fence sent to this exact audience, and doing synchronous score work with
     no audience is needless event-loop load. */
  if (!pythSseClients.size && !indexWsClients.size) {
    return;
  }
  let board;
  let captured;
  let executionRound = null;
  const queuedDirtyBatch = new Set(_compDirtySymbols);
  const dirtyBatch = new Set();
  const causeBatch = _compPendingCauses.slice();
  const causeGroups = new Map();
  try {
    /* The coalescer has its own timer and can wake exactly as a phase turns.
       Advance side effects before reading arithmetic phase, or it can publish
       `hot` without a revealed market / `boost` without frozen bankrolls for
       the next REST interval. */
    const phaseNowAt = Date.now();
    ensureGlobalCompetitionPause(phaseNowAt);
    comp.advanceRoundClock(phaseNowAt);
    const phase = comp.phaseNow();
    executionRound = phase?.round && holdsRoundPrices(phase.round) ? phase.round : null;
    /* A SAVEPOINT can have returned a fill id before an enclosing transaction
       rolled back. AUTOINCREMENT ids can then be reused. Validate the entire
       stored identity, not id existence alone, before it becomes public. */
    for (const cause of causeBatch) {
      const row = _compFillCauseRow.get(cause.fillId);
      if (!row
          || Number(row.user_id) !== cause.userId
          || String(row.symbol) !== cause.symbol
          || Number(row.price) !== cause.executionPrice
          || Number(row.reference_mark) !== cause.referenceMark
          || Number(row.ts) !== cause.at) continue;
      dirtyBatch.add(baseOf(cause.symbol));
      /* Preserve the persisted IEEE values exactly. r6 is display/account
         precision and would collapse distinct sub-dollar fills into one
         apparent cause. */
      const tuple = [cause.symbol, cause.referenceMark, cause.executionPrice, cause.at];
      const key = JSON.stringify(tuple);
      const prior = causeGroups.get(key);
      if (prior) {
        prior.tuple[4] += 1;
        prior.causes.push(cause);
      } else {
        causeGroups.set(key, { tuple: [...tuple, 1], causes: [cause] });
      }
    }
    /* Overflow should be unreachable under the documented seat/pass bounds.
       If it happens, retain the one-shot current-symbol fences and advertise
       causesComplete:false rather than hiding the exceptional loss. */
    if (_compCauseOverflow) {
      for (const sym of queuedDirtyBatch) dirtyBatch.add(sym);
    }

    const capture = () => {
      const bases = new Set(dirtyBatch);
      if (phase && phase.round && !executionRound) {
        for (const pl of comp.playersOf(phase.round.id)) {
          for (const pos of stmt.posByUser.all(pl.user_id)) bases.add(baseOf(pos.symbol));
        }
      }
      const marks = new Map();
      const entries = [];
      const now = Date.now();
      if (phase && phase.round) {
        for (const base of bases) {
          const m = live.map.get(base);
          if (!snapshotWorthy(m, base)) continue;
          const px = Number(m.pythPrice);
          const basis = Number(m.pythSrcAtMs) > 0
            ? Number(m.pythSrcAtMs) : Number(m.pythAtMs) || 0;
          const expiresAt = basis + staleMsForSym(base, m.srcKey);
          if (!(px > 0) || !(expiresAt > now)) continue;
          marks.set(base, px);
          entries.push({ base, m, px, expiresAt });
        }
      }
      return { marks, entries };
    };

    captured = capture();
    board = compRankSnapshot(phase, captured.marks);
    const crossed = () => captured.entries.some(({ base, m, expiresAt }) =>
      expiresAt <= Date.now() || !snapshotWorthy(m, base));
    /* Scoring is synchronous but not timeless. If a short-lived mark crosses
       its source deadline during roster scoring, rebuild once without it so
       REST and the pushed board answer the same freshness question. */
    if (crossed()) {
      captured = capture();
      board = compRankSnapshot(phase, captured.marks);
    }
    /* A second crossing is rare but must fail closed rather than spin out a
       complete board on an expired basis. Retain causes and retry immediately;
       the next capture will omit the expired mark and publish unscored. */
    if (crossed()) {
      _compBoardTimer = setTimeout(flushCompBoard, 1);
      _compBoardTimer.unref?.();
      return;
    }
  }
  catch (e) { _log('competition rank push failed: ' + String(e && e.message || e)); return; }
  const nextExpiry = captured.entries.reduce((x, row) => Math.min(x, row.expiresAt), Infinity);
  const pricePayloads = captured.entries.map(({ base, m, px }) => {
    const id = pubIdFor(base, m);
    /* A score fence is a committed state snapshot, not a second market event.
       `fence:1` lets a validated snapshot recover a fail-closed client and
       update its charts without pretending it advanced transport sequence. */
    return priceFrame(base, px, m.pythAtMs, id.seq, m.srcKey,
      true, false, id.symSeq, m.pythSrcAtMs, true, m.acceptedSeq);
  });
  const basisExpiresAt = Number.isFinite(nextExpiry) ? Math.floor(nextExpiry) : null;
  const groups = [...causeGroups.values()];
  /* Reserve the next id while serializing, but do not consume it until the
     final freshness check below. Otherwise expiry here creates an undelivered
     q hole and every connected client must reset on the retry. */
  const revision = _compBoardRevision + 1;
  const issuedAt = Date.now();
  const roundExecution = publicRoundExecution(executionRound, revision, issuedAt);
  const renderBundle = (groupCount) => {
    const complete = !_compCauseOverflow && groupCount === groups.length;
    const causes = groups.slice(0, groupCount).map((g) => g.tuple);
    const payload = controlFrame('comp', {
      ...board, ...(roundExecution ? { v: 3, t: issuedAt, roundExecution } : {}),
      ...(roundExecution && comp.backupExecutionPolicyOf(executionRound) ? {
        round: { id: executionRound.id, pricePolicy: ROUND_PRICE_POLICY,
          formatVersion: 2, boostLeverage: Number(executionRound.boost_leverage),
          ...comp.backupExecutionPolicyFields(executionRound) },
      } : {}),
      q: revision, x: roundExecution ? roundExecution.validUntil : basisExpiresAt,
      causes,
      causesComplete: complete,
    });
    const itemJson = [...pricePayloads, payload].join(',');
    const wsBundle = `{"v":${roundExecution ? 3 : 2},"type":"bundle","boot":"${ENGINE_BOOT_ID}","items":[${itemJson}]}`;
    // Open old tabs retain the exact v2 shape, but cannot rank this different
    // price policy. In particular no control lease is disguised as price x.
    const legacyBoard = roundExecution ? controlFrame('comp', {
      ...board, players: [], complete: false,
      unscored: [...board.players.map((p) => ({ userId: p.userId, name: p.name,
        seat: p.seat, error: 'competition client update required' })), ...board.unscored],
      q: revision, x: null, causes, causesComplete: complete,
    }) : null;
    const legacyBundle = legacyBoard
      ? `{"v":2,"type":"bundle","boot":"${ENGINE_BOOT_ID}","items":[${legacyBoard}]}` : wsBundle;
    return { payload, wsBundle, legacyBundle,
      wsBytes: Math.max(Buffer.byteLength(wsBundle), Buffer.byteLength(legacyBundle)) };
  };
  /* Select the largest deterministic cause-group prefix that fits beside the
     non-negotiable price fences and board. Omitted groups remain queued and
     `causesComplete:false` keeps every visible rank non-definitive until the
     following chunk(s) arrive in revision order. */
  let lo = 0;
  let hi = groups.length;
  let rendered = renderBundle(0);
  while (lo < hi) {
    const mid = Math.ceil((lo + hi) / 2);
    const candidate = renderBundle(mid);
    if (candidate.wsBytes < COMP_WS_BUNDLE_LIMIT) {
      lo = mid;
      rendered = candidate;
    } else {
      hi = mid - 1;
    }
  }
  if (lo === groups.length) rendered = renderBundle(lo);
  const selectedGroups = groups.slice(0, lo);
  const retainedGroups = groups.slice(lo);
  const payload = rendered.payload;
  const wsBundle = rendered.wsBundle;
  const wsBytes = rendered.wsBytes;
  /* Arm the exact instant at which the oldest mark in this board expires. A
     silent feed must turn the compact board incomplete without waiting for a
     cached REST poll or some unrelated market to move. */
  if (_compExpiryTimer) clearTimeout(_compExpiryTimer);
  _compExpiryTimer = null;
  const refreshAt = roundExecution
    ? (roundExecution.validUntil > issuedAt
      ? Math.max(issuedAt + 50, Math.min(issuedAt + 1000, roundExecution.validUntil - 250))
      : issuedAt + 1000) : nextExpiry;
  if (Number.isFinite(refreshAt)) {
    _compExpiryTimer = setTimeout(() => {
      _compExpiryTimer = null;
      flushCompBoard();
    }, Math.max(1, refreshAt - Date.now() + 1));
    _compExpiryTimer.unref?.();
  }
  /* Do not begin fan-out after the oldest scoring basis crossed while we were
     serializing a maximal roster. Causes stay queued and the immediate retry
     rebuilds without the expired mark. */
  const basisCurrent = () => roundExecution
    ? (roundExecution.validUntil === issuedAt || Date.now() < roundExecution.validUntil)
    : (!Number.isFinite(nextExpiry) || Date.now() < nextExpiry);
  if (!basisCurrent()) {
    _compBoardTimer = setTimeout(flushCompBoard, 1);
    _compBoardTimer.unref?.();
    return;
  }
  _compBoardRevision = revision;
  /* One SSE write and one WS message are each an ordered atomic fence. Encoding
     a shared WS bundle removes the legal 78-bases × 64-viewers synchronous
     send explosion and makes partial price/board delivery impossible. */
  let delivered = false;
  for (const res of pythSseClients) {
    const sseChunk = `data: ${_compProtocol.get(res) === 3 ? wsBundle : rendered.legacyBundle}\n\n`;
    const sseBytes = Buffer.byteLength(sseChunk);
    if (!basisCurrent()) { dropSse(res); _relayDrops++; continue; }
    if (res.writableLength + sseBytes > 262144) { dropSse(res); _relayDrops++; continue; }
    try { res.write(sseChunk); delivered = true; } catch { dropSse(res); }
  }
  for (const ws of indexWsClients) {
    if (ws.readyState !== 1) continue;
    if (!basisCurrent()) { _relayDrops++; dropWs(ws); continue; }
    /* Missing a competition revision is not ordinary tick thinning. Drop the
       transport so the browser resets to a REST/current-board baseline rather
       than accepting q+1 after silently skipping the only fill cause in q. */
    if (wsBytes >= COMP_WS_BUNDLE_LIMIT
        || ws.bufferedAmount + wsBytes >= COMP_WS_BUNDLE_LIMIT) {
      _relayDrops++; dropWs(ws); continue;
    }
    try {
      ws.send(_compProtocol.get(ws) === 3 ? wsBundle : rendered.legacyBundle);
      delivered = true;
    }
    catch {
      _relayDrops++;
      /* This one message is all-or-nothing at the application layer. Drop on
         any send exception so the next revision starts from a new baseline. */
      dropWs(ws);
    }
  }
  /* The candidate revision becomes history only if at least one application
     message was handed to a transport. If every recipient was rejected or
     dropped before the first write, nobody can have observed it, so reuse the
     id on the retry rather than manufacturing a global continuity hole. */
  if (!delivered) {
    _compBoardRevision = revision - 1;
    if (_compExpiryTimer) clearTimeout(_compExpiryTimer);
    _compExpiryTimer = null;
    if ((pythSseClients.size || indexWsClients.size) && !_compBoardTimer) {
      _compBoardTimer = setTimeout(flushCompBoard, 1);
      _compBoardTimer.unref?.();
    }
    return;
  }
  if (delivered) {
    /* No asynchronous producer can interleave with this synchronous flush.
       Invalid rolled-back entries and the selected valid groups retire;
       omitted groups and their closed-symbol fence obligations do not. */
    const retainedCauses = new Set(retainedGroups.flatMap((g) => g.causes));
    for (let i = causeBatch.length - 1; i >= 0; i--) {
      if (!retainedCauses.has(causeBatch[i])) _compPendingCauses.splice(i, 1);
    }
    const retainedBases = new Set(retainedGroups.flatMap((g) => g.causes.map((c) => baseOf(c.symbol))));
    for (const sym of queuedDirtyBatch) {
      if (!retainedBases.has(baseOf(sym))) _compDirtySymbols.delete(sym);
    }
    /* Overflow means an event was irretrievably absent. Stay incomplete until
       the round changes/restart provides a new delivery baseline; never turn
       one explicit loss into a silently complete next revision. */
    if (retainedGroups.length && !_compBoardTimer) {
      _compBoardTimer = setTimeout(flushCompBoard, 1);
      _compBoardTimer.unref?.();
    } else if ((causeBatch.length || queuedDirtyBatch.size) && !_compBoardTimer) {
      /* A fill/close can leave a one-shot dirty fence after the position itself
         is gone. A recovering browser may legitimately skip that fence if a
         newer ordinary price overtook it, so guarantee one clean consecutive
         revision it can publish without requiring another market move. */
      _compBoardTimer = setTimeout(flushCompBoard, COMP_BOARD_GAP_MS);
      _compBoardTimer.unref?.();
    }
  }
}
function scheduleCompBoard(symbol = null, force = false) {
  /* Clock expiry is a correctness timer, not a broadcast optimisation. Keep
     it armed even when there is no SSE/WS audience and even when this symbol
     is not currently rank-tracked. */
  try { armCompetitionClockExpiry(); } catch { /* sweep/advance is the fallback */ }
  if (!force) {
    const sym = baseOf(symbol);
    if (!sym || !_compRankSymbols.has(sym)) return false;
  }
  if (_compBoardTimer) return true;
  _compBoardTimer = setTimeout(flushCompBoard, COMP_BOARD_GAP_MS);
  _compBoardTimer.unref?.();
  return true;
}
const _sseLastBySym = new Map();
// Relay gate. Binance fires ~100/s on BTC, so this is where fluidity is won or
// lost. 120ms (~8/s) was set when the chart bucketed at 500ms and anything
// faster was genuinely wasted; the chart now buckets at 50ms, so the gate is
// the binding constraint. The Lazer symbols get 40ms (~25/s) because that is
// where 5bps of liquidation distance lives and a stepped price is the
// difference between watching a market and watching a slideshow. Everything
// else stays at 120ms -- those feeds are naturally slower than the gate anyway
// (RENDER ticks every 6s), so tightening them would buy nothing and cost
// bandwidth on every connected client.
//
// The gate used to ask for a 1000x cap. The caps came down to 500 on
// 2026-08-31 and nobody re-keyed it, so for two days EVERY symbol was held to
// 120ms: Lazer published 20 prices a second, the browser got eight, and the
// 50ms grid forward-filled two buckets in three. That is the staircase the
// finer grid was supposed to remove. Key it to the feed, not to a number that
// moved.
/* 120ms was right for a book of 24 markets. At 78 it put 470 frames and 70KB
   a second into every browser, and a client that stops reading for a moment
   during a heavy symbol switch fills its socket buffer, which the relay then
   terminates: measured as a real socket drop and a six second reconnect on
   every market change. The markets on this gate quote at 200ms or slower
   anyway, so four updates a second loses nothing a screen can show. */
/* THE GATE MUST NOT BE THE BINDING CONSTRAINT.
 *
 * 250ms was chosen when the book was 78 markets and every browser was taking
 * 110KB/s. The book is 36 now and the disabled ones no longer publish, so the
 * budget came back. It matters because 28 of those 36 sit on Pyth's 200ms
 * channel: a 250ms gate on a 200ms feed drops one update in five for no
 * reason, and on the tick chart's 50ms grid that shows as a step. At 150ms the
 * gate passes everything a 200ms feed produces and still halves what a 50ms
 * one would send. */
const SSE_MIN_GAP_MS = Number(process.env.PAPER_SSE_GAP_MS || 150);
const SSE_FAST_GAP_MS = Number(process.env.PAPER_SSE_FAST_GAP_MS || 40);
const sseGapFor = (sym) => (FAST_RELAY.has(sym) ? SSE_FAST_GAP_MS : SSE_MIN_GAP_MS);
// WS mirror of the SSE relay. Buffering middleboxes (corporate proxies, some
// ISPs/VPNs, AV TLS inspection) hold chunked HTTP responses and flush every
// 10-20s — an SSE consumer behind one sees batched steps, a dead-looking
// chart. The same boxes pass WebSocket frames through immediately (observed:
// the user's direct Phoenix WS was always live while our SSE trickled).
const indexWsClients = new Set();
const _wsIp = new WeakMap();          // socket -> the address it was counted against
const _wsAlive = new WeakMap();       // socket -> answered the last ping
/* Generous ON PURPOSE: the venue NAT puts every contestant, the wall and the
   desk on one address. Declared here rather than inside the upgrade handler so
   readiness can report the cap alongside the refusals it caused. */
const IDX_WS_PER_IP = Number(process.env.PAPER_WS_PER_IP || 64);
const _wsRefused = { gate: 0, global: 0, perIp: 0, lastIp: null };
let _wsReaped = 0;
const _wsIpCount = new Map();
/* ONE way out. Broadcast code used to delete a socket from the set directly on
   a send exception; the close handler then found it already gone and returned
   before decrementing the per-address count, so repeated send failures could
   walk the venue's single NAT address up to its cap with no live sockets. */
function dropWs(ws) {
  const had = indexWsClients.delete(ws);
  try { ws.terminate ? ws.terminate() : ws.close(); } catch { /* already gone */ }
  if (!had) return;
  const ip = _wsIp.get(ws);
  if (!ip) return;
  _wsIp.delete(ws);
  const n = (_wsIpCount.get(ip) || 1) - 1;
  if (n > 0) _wsIpCount.set(ip, n); else _wsIpCount.delete(ip);
}
function attachIndexWs(httpServer) {
  const WebSocket = require('ws');
  /* 1KB, because this socket has no message handler at all: nothing a client
     sends is ever read. The ws default is 100MB, and the receiver assembles a
     frame before discarding it, so a handful of sockets pushing junk could
     walk the engine into its systemd MemoryMax and take the wall down with
     it mid-heat. */
  const wss = new WebSocket.Server({ noServer: true, maxPayload: 1024 });
  /* Generous on purpose: on show night every contestant, the wall and the
     desk sit behind ONE venue NAT address, so a tight per-IP cap would lock
     the last arrivals out of the price feed. The real protection against a
     flood is maxPayload above; this only stops a single host from parking
     hundreds of sockets. */

  httpServer.on('upgrade', (req, socket, head) => {
    let path = '';
    try { path = new URL(req.url, 'http://x').pathname; } catch {}
    // other WS routes (perps live tape) have their own upgrade listeners —
    // only destroy paths nobody owns
    if (path !== '/api/paper/index-ws') { if (path !== '/api/perps/live-ws') socket.destroy(); return; }
    /* Same gate as every HTTP route. The upgrade path never checked it, so a
       client that could reach the port read the index feed ungated. */
    const gate = process.env.PAPER_GATE_SECRET || '';
    /* A REFUSED UPGRADE USED TO BE SILENT.
     *
     * The browser sees a socket that errors before it opens, retries every
     * 1.5s, and reports nothing but a stalled feed. From the engine there was
     * no log, no counter and nothing in health, so a client stuck in that loop
     * was indistinguishable from a network fault. Every refusal now says which
     * rule refused it, and the counts are in /readyz. */
    if ((req.headers['x-paper-gate'] || '') !== gate) { _wsRefused.gate++; socket.destroy(); return; }
    if (deploymentMaintenanceActive()) { socket.destroy(); return; }
    if (indexWsClients.size >= PYTH_SSE_MAX_CLIENTS) { _wsRefused.global++; socket.destroy(); return; }
    const ip = String(req.headers['x-real-ip'] || (req.socket && req.socket.remoteAddress) || 'local');
    if ((_wsIpCount.get(ip) || 0) >= IDX_WS_PER_IP) {
      _wsRefused.perIp++;
      _wsRefused.lastIp = ip;
      socket.destroy();
      return;
    }
    wss.handleUpgrade(req, socket, head, (ws) => {
      indexWsClients.add(ws);
      _compProtocol.set(ws, requestedCompProtocol(req));
      _wsIp.set(ws, ip);
      _wsIpCount.set(ip, (_wsIpCount.get(ip) || 0) + 1);
      for (const [sym, m] of live.map) {
        if (!snapshotWorthy(m, sym)) continue;
        const id = pubIdFor(sym, m);
        try { ws.send(priceFrame(sym, m.pythPrice, m.pythAtMs, id.seq, m.srcKey, true, false, id.symSeq, m.pythSrcAtMs, false, m.acceptedSeq)); } catch { /* dropped below */ }
      }
      try { ws.send(controlFrame('hello')); } catch { /* dropped below */ }
      /* Liveness by PONG, not by hope. The ping below was fire and forget, so
         a browser that vanished without a FIN (a laptop lid, a dropped NAT
         entry) held its slot until TCP eventually gave up, minutes later,
         while its address counted against a cap of 64. */
      _wsAlive.set(ws, true);
      ws.on('pong', () => _wsAlive.set(ws, true));
      ws.on('close', () => dropWs(ws));
      ws.on('error', () => dropWs(ws));
      scheduleCompBoard(null, true);
    });
  });
  setInterval(() => {
    for (const ws of indexWsClients) {
      if (_wsAlive.get(ws) === false) { _wsReaped++; dropWs(ws); continue; }
      _wsAlive.set(ws, false);
      try { ws.ping(); } catch { dropWs(ws); }
    }
  }, 15_000).unref();
  setInterval(() => {
    const r = _wsRefused;
    if (r.gate || r.global || r.perIp || _wsReaped) {
      _log(`index-ws: refused ${r.perIp} per-address${r.lastIp ? ' (' + r.lastIp + ')' : ''}, ${r.global} over the global cap, ${r.gate} ungated; reaped ${_wsReaped} dead in the last minute`);
      if (r.perIp) tgOps('wsperip', `index relay refused ${r.perIp} connections from ${r.lastIp} against the ${IDX_WS_PER_IP} per-address cap; that address is reconnecting in a loop`);
      r.gate = 0; r.global = 0; r.perIp = 0; _wsReaped = 0;
    }
  }, 60_000).unref();
  /* Two seconds, market or no market. A client can now tell "nothing is
     trading" from "nothing is arriving" without waiting for a price. */
  setInterval(() => { if (indexWsClients.size || pythSseClients.size) pythControlBroadcast({ type: 'hb' }); }, 2_000).unref();
}
function pythSseBroadcast(sym, px, ts, srcKey = null, material = false, srcAtMs = 0) {
  if (!pythSseClients.size && !indexWsClients.size) return { published: false, seq: 0, symSeq: 0 };
  /* Gated on MONOTONIC time: a backward wall-clock step used to suppress every
     frame until the clock caught up with the last stamp it had written.
     A MATERIAL event ignores the gate: the cadence exists to save bandwidth on
     prices that changed nothing, and the one tick that liquidated a trader is
     the last thing a chart should be missing. */
  const mono = monoNow();
  if (!material && mono - (_sseLastBySym.get(sym) || -1e9) < sseGapFor(sym)) return { published: false, seq: 0, symSeq: 0 };
  _sseLastBySym.set(sym, mono);
  /* Every published event gets a boot-scoped sequence number. The relay skips
     frames for slow clients by design, and without a sequence that loss is
     invisible: a browser could miss the exact tick that liquidated somebody,
     draw a continuous line through the hole and still call itself LIVE. */
  const seq = ++_evSeq;
  _symSeq.set(sym, seq);
  /* A PER-SYMBOL counter as well as the global one. A client watching BTC sees
     every other market's events pass between its own, so the global sequence
     jumps by dozens between two BTC frames and cannot tell that market's loss
     from ordinary interleaving. This one counts only this symbol's published
     frames, so a hole in it is a hole in what that chart was sent. */
  const symSeq = (_symCount.get(sym) || 0) + 1;
  _symCount.set(sym, symSeq);
  /* The identity of THIS event, kept with the mark it published. A reconnect
     snapshot used to pair the current price with whatever sequence was last
     published for the symbol, so a thinned tick was delivered under an older
     event's number and two different prices answered to one id. The snapshot
     now sends the id stored with the mark, or none at all. */
  const mark = live.map.get(sym) || {};
  _pubId.set(sym, { seq, symSeq, atMs: ts,
    acceptedSeq: Number(mark.acceptedSeq) || 0 });
  /* A regular live event goes first; the coalesced score fence may later send
     a snapshot of a newer cadence-thinned mark, but always immediately ahead
     of the board that uses it. */
  const payload = priceFrame(sym, px, ts, seq, srcKey || mark.srcKey,
    false, material, symSeq, srcAtMs, false, mark.acceptedSeq);
  const line = `id: ${seq}\ndata: ${payload}\n\n`;
  for (const res of pythSseClients) {
    /* The WS half has always had a ceiling; the SSE half ignored the write
       return value entirely, so a subscriber whose TCP window shut buffered
       ticks in engine memory with nothing to stop it. A client this far
       behind is not watching a live market anyway: drop it and let it
       reconnect. */
    if (res.writableLength > 262144) { dropSse(res); _relayDrops++; continue; }
    try { res.write(line); } catch { dropSse(res); }
  }
  for (const ws of indexWsClients) {
    if (ws.readyState !== 1) continue;
    /* Every payload has a contiguous sequence identity, and a material one
       may be the exact wick that liquidated a 500x position. Silently skipping
       it while keeping the socket alive makes the following frame look usable
       even though chart/fill causality is permanently broken. A slow or
       throwing socket therefore crosses an explicit reconnect/baseline edge. */
    if (ws.bufferedAmount >= 65536) { _relayDrops++; dropWs(ws); continue; }
    try { ws.send(payload); }
    catch {
      _relayDrops++;
      dropWs(ws);
    }
  }
  return { published: true, seq, symSeq };
}
/* Skipping a slow client is right, doing it silently is not: without this a
   lagging wall display and a quiet market look identical from the logs. */
let _relayDrops = 0;
setInterval(() => {
  if (!_relayDrops) return;
  _log(`index relay skipped ${_relayDrops} frames for slow clients in the last minute`);
  _relayDrops = 0;
}, 60_000).unref();
setInterval(() => {   // keep-alive comment so proxies don't reap idle streams
  for (const res of pythSseClients) {
    /* Through the same ceiling as price and control frames. A blocked stream
       that happens to be quiet would otherwise accumulate keepalives in engine
       memory until some other path noticed it. */
    if (res.writableLength > 262144) { dropSse(res); continue; }
    try { res.write(': ping\n\n'); } catch { dropSse(res); }
  }
}, 15_000).unref();

// GET /api/paper/pyth-history?symbol= — raw oracle ticks (ascending [tsMs, px])
/* The durable half of the tick seed.
 *
 * The in-memory ring is fast and dies with the process: for twelve minutes
 * after any deploy the tick chart is a staircase of one minute backfill bars
 * with live ticks pinned to the right, which is what "we don't store tick
 * history any more" looks like from the outside. We do store it, every
 * accepted observation for thirty days, so anything the ring cannot cover is
 * filled from there. Bucketed to the chart's own 50ms grid so the payload is
 * the same size it always was. */
const _lineCache = new Map();
const HISTORY_MAX_INFLIGHT = 64;
const HISTORY_CLASS_LIMITS = Object.freeze({ baseCandles: 24, indexCandles: 24, indexLine: 16 });
const HISTORY_TIMERS = Object.freeze({ setTimeout, clearTimeout });
function createHistoryCapacity(limits = HISTORY_CLASS_LIMITS, maxInflight = HISTORY_MAX_INFLIGHT) {
  const names = Object.keys(HISTORY_CLASS_LIMITS);
  if (!Number.isSafeInteger(maxInflight) || maxInflight < 1 || maxInflight > HISTORY_MAX_INFLIGHT
    || names.some((name) => !Number.isSafeInteger(limits[name]) || limits[name] < 1
      || limits[name] > HISTORY_CLASS_LIMITS[name])) throw new Error('invalid history capacity');
  const caps = Object.fromEntries(names.map((name) => [name, limits[name]]));
  const byClass = Object.fromEntries(names.map((name) => [name, 0]));
  let active = 0;
  return {
    tryAcquire(name) {
      if (!Object.hasOwn(byClass, name) || active >= maxInflight || byClass[name] >= caps[name]) return null;
      active++; byClass[name]++;
      let held = true;
      return () => {
        if (!held) return;
        held = false; active--; byClass[name]--;
      };
    },
    snapshot: () => ({ active, maxInflight, limits: { ...caps }, byClass: { ...byClass } }),
  };
}
const _historyCapacity = createHistoryCapacity();
/* Socket-idle timeouts alone do not cover DNS/connect stalls or a response
   that keeps trickling bytes. Every optional history read must release its
   socket, buffered body and cold-build slot within one absolute deadline.
   Non-borrowing reservations keep tick seeds from taking either half of the
   24 paired candle builds. This pool covers history, not live quote feeds. */
function historyText(requestClass, transport, options, maxBytes = 1024 * 1024,
  capacity = _historyCapacity, timers = HISTORY_TIMERS) {
  const release = capacity.tryAcquire(requestClass);
  if (!release) return Promise.resolve(null);
  return new Promise((resolve) => {
    let req = null, timer = null, settled = false, bytes = 0;
    const chunks = [];
    const finish = (body) => {
      if (settled) return;
      settled = true;
      if (timer !== null) timers.clearTimeout(timer);
      release();
      chunks.length = 0;
      if (body === null) { try { req?.destroy(); } catch {} }
      resolve(body);
    };
    const fail = () => finish(null);
    try {
      timer = timers.setTimeout(fail, options.timeout);
      req = transport.request(options, (res) => {
        res.on('error', fail);
        res.on('aborted', fail);
        res.on('close', () => { if (!settled) fail(); });
        if (settled) { try { req?.destroy(); } catch {} return; }
        if (res.statusCode !== 200) { fail(); return; }
        res.on('data', (chunk) => {
          if (settled) return;
          const part = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
          bytes += part.length;
          if (bytes > maxBytes) { fail(); return; }
          chunks.push(part);
        });
        res.on('end', () => {
          if (!settled) finish(Buffer.concat(chunks, bytes).toString('utf8'));
        });
      });
      req.on('error', fail);
      req.on('timeout', fail);
      if (settled) req.destroy(); else req.end();
    } catch { fail(); }
  });
}
async function indexLine(symbol, fromMs, toMs) {
  if (!process.env.WAREHOUSE_API_TOKEN || !(toMs > fromMs)) return null;
  const key = `${symbol}|${Math.floor(fromMs / 5000)}|${Math.floor(toMs / 5000)}`;
  const hit = _lineCache.get(key);
  if (hit && Date.now() - hit.at < 30_000) return hit.rows;
  const path = `/internal/index-line?symbol=${encodeURIComponent(symbol)}&fromMs=${Math.floor(fromMs)}&toMs=${Math.floor(toMs)}&bucketMs=50`;
  const body = await historyText('indexLine', require('http'),
    { host: '127.0.0.1', port: 9100, path, method: 'GET', timeout: 5000 }, 2 * 1024 * 1024);
  if (!body) return null;
  try {
    const j = JSON.parse(body);
    if (!j || !j.ok || !Array.isArray(j.rows)) return null;
    const rows = j.rows.filter((r) => Array.isArray(r) && r[0] > 0 && r[1] > 0);
    _lineCache.set(key, { at: Date.now(), rows });
    if (_lineCache.size > 120) _lineCache.delete(_lineCache.keys().next().value);
    return rows;
  } catch { return null; }
}
function requestedRoundHistory(u, symbol) {
  const policy = u.searchParams.get('pricePolicy'), id = u.searchParams.get('roundId');
  if (policy == null && id == null) return null;
  const round = comp.currentRound();
  if (policy !== ROUND_PRICE_POLICY || !round || round.id !== id
      || !holdsRoundPrices(round) || !STAGE_INDEXED.has(symbol) || DISABLED_MARKETS.has(symbol)) {
    throw new Error('round_history_scope');
  }
  return round;
}
function projectRoundHistory(result) {
  if (!result || !Array.isArray(result.breaks) || typeof result.complete !== 'boolean'
      || result.breaks.length > 4096
      || !(Number.isFinite(result.fromAvailable) && result.fromAvailable >= 0)) throw new Error('round_history_unavailable');
  const breaks = result.breaks.map((gap) => {
    if (!gap || !Number.isFinite(gap.from) || gap.from < 0
        || !(gap.to === null || (Number.isFinite(gap.to) && gap.to >= gap.from))) throw new Error('round_history_unavailable');
    return { from: gap.from, to: gap.to };
  });
  return { breaks, fromAvailable: result.fromAvailable, complete: result.complete };
}
function projectRoundTickRows(records, to) {
  if (!Array.isArray(records) || records.length > 2049) throw new Error('round_history_unavailable');
  return records.map((row) => {
    // Historical provenance is not current trading authority. Earlier boots
    // remain honest chart history; the response boot and explicit pause/hard
    // intervals separately bind the current reader and continuity gaps.
    if (!row || typeof row.acceptedBoot !== 'string'
        || !/^[a-zA-Z0-9_-]{1,64}$/.test(row.acceptedBoot)
        || !(Number.isFinite(row.acceptedAt) && row.acceptedAt > 0
          && Number.isFinite(row.appliedAt) && row.appliedAt >= row.acceptedAt
          && row.appliedAt <= to && Number.isFinite(row.price) && row.price > 0)) {
      throw new Error('round_history_unavailable');
    }
    return [row.appliedAt, row.price];
  });
}
async function pythHistory(req, res, u_url) {
  if (_readRateOk && !_readRateOk(req.headers['x-real-ip'] || req.socket.remoteAddress)) {
    return send(res, 429, { ok: false, error: 'rate_limited' });
  }
  const symbol = String(u_url.searchParams.get('symbol') || '').toUpperCase();
  if (!symbol || !/^[A-Z0-9]{1,12}$/.test(symbol)) return send(res, 400, { ok: false, error: 'bad_symbol' });
  try {
    const round = requestedRoundHistory(u_url, symbol);
    if (round) {
      const to = Date.now(), from = Math.max(Number(round.started_at) || 0,
        Math.min(to, Number(u_url.searchParams.get('sinceMs')) || 0));
      const result = comp.roundMarkHistory(round.id, symbol, { from, to, limit: 2048 });
      const metadata = projectRoundHistory(result);
      const rows = projectRoundTickRows(result.records, to);
      return send(res, 200, { ok: true, boot: ENGINE_BOOT_ID, roundId: round.id,
        pricePolicy: ROUND_PRICE_POLICY, symbol, rows, ...metadata });
    }
  } catch (e) {
    return send(res, e.message === 'round_history_scope' ? 409 : 503,
      { ok: false, error: e.message === 'round_history_scope' ? 'round_history_scope' : 'round_history_unavailable' });
  }
  /* The ring holds 45 minutes; the chart keeps ten and trims to six. Shipping
     the whole ring meant a client parsed and threw away three quarters of a
     150KB body on the first open of every market, which is the jank a phone
     feels at the start of a heat. Let the caller say how far back it cares. */
  const all = pythHist.get(symbol) || [];
  const since = Number(u_url.searchParams.get('sinceMs')) || 0;
  let rows = all;
  if (since > 0 && all.length) {
    let i = 0;
    while (i < all.length && Number(all[i][0]) < since) i++;
    rows = i > 0 ? all.slice(Math.max(0, i - 1)) : all;
  }
  /* DENSITY, NOT COVERAGE, IS THE TEST.
   *
   * After a restart the ring is not shorter than the window, it is SPARSER:
   * boot fills it with 45 one-minute bars from klines, so it spans the window
   * with fourteen points where a live feed would have thousands. That is the
   * staircase. So the question is not "does the ring reach back far enough"
   * but "does it have enough points in this window to be a tick chart", and
   * two a second is a low bar that only a genuinely quiet market misses. */
  const wantFrom = since > 0 ? since : 0;
  const nowMs = Date.now();
  if (wantFrom > 0 && nowMs - wantFrom > 5000) {
    const have = rows.filter((r) => Number(r[0]) >= wantFrom).length;
    const thin = have < Math.floor(((nowMs - wantFrom) / 1000) * 2);
    if (thin) {
      const durable = await indexLine(symbol, wantFrom, nowMs);
      if (durable && durable.length > have) {
        /* The ring wins wherever both have a point: it is the live tail and it
           preserves the material observations the chart must not lose. */
        const merged = new Map();
        for (const r of durable) merged.set(Number(r[0]), Number(r[1]));
        for (const r of rows) if (Number(r[0]) >= wantFrom) merged.set(Number(r[0]), Number(r[1]));
        rows = [...merged.entries()].sort((x, y) => x[0] - y[0]);
      }
    }
  }
  send(res, 200, { ok: true, symbol, rows });
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
const _klineInflight = new Map();    // cache key -> shared cold build
const KLINE_MAX_INFLIGHT = 24;       // bound sockets/memory under varied-key bursts
const KLINE_TTL_MS = 20_000;         // candles move slowly; protects Binance limits
const KLINE_TF = new Set(['1m', '5m', '15m', '1h', '4h', '1d']);
const KLINE_STEP_MS = { '1m': 60_000, '5m': 300_000, '15m': 900_000, '1h': 3_600_000, '4h': 14_400_000, '1d': 86_400_000 };
const KLINE_PERSIST_GRACE_MS = 10_000;

/* An explicit endTime is immutable only after its final bucket has closed AND
   the warehouse's write batch has had room to land. Treating a
   just-closed minute as immutable for six hours froze an incomplete close. */
function candleCacheTtl(tf, endTime, now = Date.now()) {
  if (!(endTime > 0)) return KLINE_TTL_MS;
  const step = KLINE_STEP_MS[tf];
  if (!(step > 0)) return KLINE_TTL_MS;
  const lastClose = Math.floor(endTime / step) * step + step;
  return now >= lastClose + KLINE_PERSIST_GRACE_MS ? 6 * 3600_000 : KLINE_TTL_MS;
}

const indexCandleRow = (time, b, volume = 0) => ({
  time: Number(time),
  open: Number(b.open), high: Number(b.high), low: Number(b.low), close: Number(b.close),
  markOpen: Number(b.open), markHigh: Number(b.high), markLow: Number(b.low), markClose: Number(b.close),
  volume: Number(volume) || 0,
});

/* Merge, rather than only patch. Phoenix's venue endpoint commonly stops at
   the last CLOSED candle, while the accepted-index warehouse already has the
   in-progress one. Only patching timestamps present in the base page omitted
   the live minute entirely on seven markets. Available index observations can
   also be served when optional Binance/Phoenix history reads fail. */
function mergeStageCandleRows(raw, pair, mine, limit) {
  let rows = Array.isArray(raw) ? (pair ? raw.map((k) => {
    const o = Number(k[1]), h = Number(k[2]), l = Number(k[3]), c = Number(k[4]);
    return { time: Number(k[0]), open: o, high: h, low: l, close: c,
      markOpen: o, markHigh: h, markLow: l, markClose: c, volume: Number(k[5]), priceSource: 'binance' };
  }) : raw.map((r) => ({ ...r, priceSource: 'venue' }))) : [];
  rows = rows.filter((r) => Number(r.time) > 0 && Number(r.close) > 0);
  const byTime = new Map(rows.map((r, i) => [Number(r.time), i]));
  const patchedTimes = new Set();
  const addedTimes = new Set();
  if (mine && mine.size) {
    for (const [time, b] of mine) {
      if (!(Number(time) > 0) || !(Number(b && b.close) > 0)) continue;
      const at = byTime.get(Number(time));
      if (at === undefined) {
        byTime.set(Number(time), rows.length);
        rows.push({ ...indexCandleRow(time, b), priceSource: 'accepted-index', observedTicks: Number(b.ticks) || null });
        addedTimes.add(Number(time));
      } else {
        const volume = rows[at].volume;
        rows[at] = { ...indexCandleRow(time, b, volume), priceSource: 'accepted-index', observedTicks: Number(b.ticks) || null };
        patchedTimes.add(Number(time));
      }
    }
  }
  rows.sort((a, b) => a.time - b.time);
  if (rows.length > limit) rows = rows.slice(-limit);
  // Provenance describes the returned page, not older buckets trimmed away.
  const patched = rows.filter((r) => patchedTimes.has(Number(r.time))).length;
  const added = rows.filter((r) => addedTimes.has(Number(r.time))).length;
  return { rows, patched, added };
}

/* The cached page is history; the live.map entry is the exact accepted mark
   that Tick, PnL, fills and the order path currently use. Overlay that tail
   at response time without mutating the cache. pythHist supplies the current
   bucket's observed range, and live.map always owns its exact close. */
function reconcileLiveCandleRows(symbol, tf, inputRows, limit, endTime = 0) {
  const step = KLINE_STEP_MS[tf];
  const mark = live.map.get(symbol);
  const px = Number(mark && mark.pythPrice);
  const eventAt = Number(mark && mark.pythAtMs);
  const rows = Array.isArray(inputRows) ? inputRows : [];
  if (!(step > 0) || !(px > 0) || !(eventAt > 0) || (endTime > 0 && eventAt > endTime)) {
    return { rows, boot: null, lastEventAt: 0, lastAcceptedSeq: 0, overlaid: false };
  }
  const bucket = Math.floor(eventAt / step) * step;
  let histOpen = px, histHigh = px, histLow = px;
  let samples = 0;
  const hist = pythHist.get(symbol) || [];
  let first = hist.length;
  while (first > 0 && Number(hist[first - 1][0]) >= bucket) first--;
  for (let i = first; i < hist.length; i++) {
    const at = Number(hist[i][0]);
    const value = Number(hist[i][1]);
    if (at > eventAt || at >= bucket + step || !(value > 0)) continue;
    if (!samples) histOpen = value;
    histHigh = samples ? Math.max(histHigh, value) : value;
    histLow = samples ? Math.min(histLow, value) : value;
    samples++;
  }
  histHigh = Math.max(histHigh, px);
  histLow = Math.min(histLow, px);
  const out = rows.slice();
  const at = out.findIndex((r) => Number(r.time) === bucket);
  if (at >= 0) {
    const old = out[at];
    const open = Number(old.open) > 0 ? Number(old.open) : histOpen;
    const high = Math.max(Number(old.high) || px, histHigh);
    const low = Math.min(Number(old.low) > 0 ? Number(old.low) : px, histLow);
    out[at] = { ...old, open, high, low, close: px,
      markOpen: open, markHigh: high, markLow: low, markClose: px,
      priceSource: old.priceSource === 'accepted-index' ? 'accepted-index' : 'mixed-live-index' };
  } else {
    out.push({ time: bucket, open: histOpen, high: histHigh, low: histLow, close: px,
      markOpen: histOpen, markHigh: histHigh, markLow: histLow, markClose: px, volume: 0,
      priceSource: 'accepted-index', observedTicks: Math.max(1, samples) });
    out.sort((a, b) => a.time - b.time);
    if (out.length > limit) out.splice(0, out.length - limit);
  }
  return {
    rows: out,
    boot: ENGINE_BOOT_ID,
    lastEventAt: eventAt,
    lastAcceptedSeq: Number(mark.acceptedSeq) || 0,
    overlaid: true,
  };
}

function sendStageCandleRows(res, symbol, tf, limit, endTime, rows, source, indexUnavailable = false) {
  const view = reconcileLiveCandleRows(symbol, tf, rows, limit, endTime);
  const indexBars = view.rows.filter(row => row.priceSource === 'accepted-index').length;
  const externalBars = view.rows.length - indexBars;
  const persistence = tickPersistenceStatus();
  return send(res, 200, {
    ok: true,
    rows: view.rows,
    source: view.overlaid ? `${source}+live-index` : source,
    history: { v: 1, complete: false,
      basis: externalBars ? (indexBars || view.overlaid ? 'mixed' : 'external') : 'accepted-observations',
      indexBars, externalBars, indexUnavailable,
      pendingDelivery: !!persistence.queueError || (persistence.queuedBatches > 0
        && (persistence.consecutiveFailures > 0 || Date.now() - persistence.oldestQueuedAt >= KLINE_PERSIST_GRACE_MS)) },
    ...(view.overlaid ? { boot: view.boot, lastEventAt: view.lastEventAt, lastAcceptedSeq: view.lastAcceptedSeq } : {}),
  });
}

async function fetchKlines(pair, tf, limit, endTime) {
  // endTime pages BACKWARDS: Binance returns the `limit` bars ending at it.
  const path = `/api/v3/klines?symbol=${pair}&interval=${tf}&limit=${limit}`
    + (endTime ? `&endTime=${endTime}` : '');
  const body = await historyText('baseCandles', require('https'),
    { host: 'api.binance.com', path, method: 'GET', timeout: 8000 });
  try { const rows = JSON.parse(body); return Array.isArray(rows) ? rows : null; }
  catch { return null; }
}

async function buildStageCandlePage(symbol, tf, limit, endTime, pair) {
  /* The base candles and our index patch are independent reads. Waiting for
     them serially made a cold timeframe click pay both latencies; request
     them together and merge after both arrive. Both reads are optional in the
     sense that an unexpected transport rejection must not poison every waiter. */
  const [raw, mine] = await Promise.all([
    (pair
      ? fetchKlines(pair.toUpperCase(), tf, limit, endTime)
      : fetchVenueCandles(symbol, tf, limit, endTime)).catch(() => null),
    indexCandles(symbol, tf, limit, endTime).catch(() => null),
  ]);
  if (!raw && !(mine && mine.size)) return null;
  /* Public history is sampled accepted observations where available, with
     explicitly identified external bars elsewhere. Neither source proves a
     complete execution audit. Volume remains external context. */
  const merged = mergeStageCandleRows(raw, pair, mine, limit);
  const rows = merged.rows;
  const patched = merged.patched + merged.added;
  const base = pair ? 'binance' : 'venue';
  const source = !raw ? 'index' : patched
    ? `index+${base} (${patched} of ${rows.length} from the engine's own ticks)`
    : base;
  return { rows, source, indexUnavailable: mine === null };
}

async function stageCandles(req, res) {
  if (_readRateOk && !_readRateOk(req.headers['x-real-ip'] || req.socket.remoteAddress)) {
    return send(res, 429, { ok: false, error: 'rate_limited' });
  }
  const u = new URL(req.url, 'http://x');
  /* An alias has no candles of its own and never will: it trades the base's
     index, so the base's history IS its history. Resolving here means the API
     answers honestly whichever name the caller uses. */
  const symbol = baseOf(String(u.searchParams.get('symbol') || '').toUpperCase());
  const tf = String(u.searchParams.get('timeframe') || u.searchParams.get('tf') || '15m');
  const limit = Math.min(1000, Math.max(10, Number(u.searchParams.get('limit')) || 300));
  // Only indexed symbols have a CEX price world; everything else legitimately
  // belongs on the venue candles and the caller should not have come here.
  /* EVERY INDEXED MARKET GETS THE SAME TREATMENT, WITH OR WITHOUT BINANCE.
   *
   * Six of the crypto book have no Binance pair at all, HYPE among them, and
   * they were falling through to the venue's own trades: on a quiet minute
   * that produces a bar with no range, which is what "empty bars" on the HYPE
   * chart were. They are the markets that needed our index bars most and were
   * the only ones not getting them. The venue candles become the BASE for
   * depth and volume, exactly as Binance is for the rest, and the same index
   * patch runs on top. */
  /* KLINE_SYMBOLS, not STAGE_INDEXED. These are different questions and
     conflating them CRASHED THE ENGINE: once all 78 markets became indexed,
     this gate started admitting equities and commodity futures, and the line
     below then read `.toUpperCase()` off an undefined Binance pair. The
     rejection was unhandled, so the process died, systemd restarted it, and
     the next browser to open a chart on one of those markets killed it again.
     Nine restarts in a row, every REST call and the relay dropped with it, and
     from the browser it looked like a feed problem. A market with no Binance
     history belongs on the venue candles, which is what the 400 tells the
     client to use. */
  if (!STAGE_INDEXED.has(baseOf(symbol)) || !KLINE_TF.has(tf)) return send(res, 400, { ok: false, error: 'unsupported' });

  // Paging back through history. Without it the chart could only ever show the
  // one window it booted with, which is what "why can't I scroll more data?"
  // was: the data exists, we just never asked for it.
  const endTime = Math.max(0, Number(u.searchParams.get('endTime')) || 0);
  try {
    const round = requestedRoundHistory(u, symbol);
    if (round) {
      const to = Math.min(Date.now(), endTime || Date.now());
      const startedAt = Number(round.started_at) || 0;
      // Paging before this round began is a known empty prefix, not a
      // reversed storage range or a provider failure. Do not borrow public
      // history or fabricate a pre-round bar. The exact start still queries
      // durable history, including its real gaps and availability verdict.
      if (Number.isFinite(endTime) && endTime > 0 && Number.isFinite(startedAt)
          && startedAt > 0 && endTime < startedAt) {
        return send(res, 200, { ok: true, boot: ENGINE_BOOT_ID, roundId: round.id,
          pricePolicy: ROUND_PRICE_POLICY, symbol, source: 'round-execution',
          rows: [], breaks: [], fromAvailable: startedAt, complete: true });
      }
      const from = Math.max(startedAt, to - KLINE_STEP_MS[tf] * limit);
      const result = comp.roundMarkCandles(round.id, symbol, { from, to, tf });
      const metadata = projectRoundHistory(result);
      if (!Array.isArray(result.rows) || result.rows.length > limit + 1) throw new Error('round_history_unavailable');
      const rows = result.rows.map((row) => {
        if (!(Number.isFinite(row.time) && row.time > 0 && row.time <= to)
            || !['open', 'high', 'low', 'close'].every((key) => Number.isFinite(row[key]) && row[key] > 0)
            || row.low > Math.min(row.open, row.close) || row.high < Math.max(row.open, row.close)) throw new Error('round_history_unavailable');
        return indexCandleRow(row.time, row);
      }).slice(-limit);
      return send(res, 200, { ok: true, boot: ENGINE_BOOT_ID, roundId: round.id,
        pricePolicy: ROUND_PRICE_POLICY, symbol, source: 'round-execution', rows, ...metadata });
    }
  } catch (e) {
    return send(res, e.message === 'round_history_scope' ? 409 : 503,
      { ok: false, error: e.message === 'round_history_scope' ? 'round_history_scope' : 'round_history_unavailable' });
  }
  const key = `${symbol}|${tf}|${limit}|${endTime}`;
  const hit = _klineCache.get(key);
  const ttl = candleCacheTtl(tf, endTime);
  if (hit && Date.now() - hit.at < ttl) {
    return sendStageCandleRows(res, symbol, tf, limit, endTime, hit.rows, `${hit.source || 'history'}-cache`, hit.indexUnavailable);
  }

  const pair = BINANCE_STREAMS[symbol];
  let cold = _klineInflight.get(key);
  if (!cold) {
    /* Per-IP rate limiting still permits many distinct endTime keys at once.
       Each cold build opens both an upstream and warehouse request, so cap
       the process-wide fan-out rather than turning a scroll burst into an OOM
       or file-descriptor cascade. An expired cached page is safe to serve. */
    if (_klineInflight.size >= KLINE_MAX_INFLIGHT) {
      if (hit) return sendStageCandleRows(res, symbol, tf, limit, endTime, hit.rows, `${hit.source || 'history'}-stale`, hit.indexUnavailable);
      return send(res, 503, { ok: false, error: 'candles_busy', retryable: true });
    }
    cold = buildStageCandlePage(symbol, tf, limit, endTime, pair)
      .then((page) => {
        /* Build, patch and cache once. Every duplicate waiter sends this same
           immutable page instead of allocating and mutating 500 more rows. */
        if (page) {
          _klineCache.set(key, { at: Date.now(), rows: page.rows, source: page.source, indexUnavailable: page.indexUnavailable });
          if (_klineCache.size > 400) _klineCache.delete(_klineCache.keys().next().value);
        }
        return page;
      })
      .finally(() => { if (_klineInflight.get(key) === cold) _klineInflight.delete(key); });
    _klineInflight.set(key, cold);
  }
  const page = await cold;
  if (!page) {
    // serve stale over nothing: a blank chart is worse than a slightly old one
    if (hit) return sendStageCandleRows(res, symbol, tf, limit, endTime, hit.rows, `${hit.source || 'history'}-stale`, true);
    return send(res, 502, { ok: false, error: 'klines_unavailable' });
  }
  return sendStageCandleRows(res, symbol, tf, limit, endTime, page.rows, page.source, page.indexUnavailable);
}
/* The venue's own candles, for the markets Binance does not list. Same shape
   the browser already receives from the /api/phoenix proxy, fetched here so
   the index patch below can run on them too. */
async function fetchVenueCandles(symbol, tf, limit, endTime) {
  const q = `symbol=${encodeURIComponent(symbol)}&timeframe=${encodeURIComponent(tf)}&limit=${Math.min(1000, limit)}`
    + (endTime ? `&endTime=${endTime}` : '');
  const body = await historyText('baseCandles', require('https'),
    { host: 'perp-api.phoenix.trade', path: `/candles?${q}`, method: 'GET', timeout: 6000 });
  try {
    const j = JSON.parse(body);
    return Array.isArray(j) ? j.map((r) => ({
      time: Number(r.time), open: Number(r.open), high: Number(r.high), low: Number(r.low), close: Number(r.close),
      markOpen: Number(r.markOpen ?? r.open), markHigh: Number(r.markHigh ?? r.high),
      markLow: Number(r.markLow ?? r.low), markClose: Number(r.markClose ?? r.close),
      volume: Number(r.volume) || 0,
    })) : null;
  } catch { return null; }
}
/* Bars rebuilt from the accepted index observations we persist. Returns a Map
   keyed by bar-open time so the caller can patch matching bars in place. A
   failure here is not a failure: the chart falls back to what it drew before. */
const _idxCandleCache = new Map();
async function indexCandles(symbol, tf, limit, endTime) {
  const tok = process.env.WAREHOUSE_API_TOKEN || '';
  if (!tok) return null;
  const key = `${symbol}|${tf}|${limit}|${endTime || 0}`;
  const hit = _idxCandleCache.get(key);
  if (hit && Date.now() - hit.at < candleCacheTtl(tf, endTime)) return hit.map;
  const path = `/internal/index-candles?symbol=${encodeURIComponent(symbol)}&tf=${encodeURIComponent(tf)}&limit=${Math.min(2000, limit)}${endTime ? `&endMs=${endTime}` : ''}`;
  const body = await historyText('indexCandles', require('http'),
    { host: '127.0.0.1', port: 9100, path, method: 'GET', timeout: 4000 });
  if (!body) return null;
  try {
    const j = JSON.parse(body);
    if (!j || !j.ok || !Array.isArray(j.rows)) return null;
    const map = new Map();
    for (const r of j.rows) {
      // One accepted observation is still evidence of a reached price. Do
      // not replace a sparse wick with a different venue's price range.
      const bar = { open: Number(r.open), high: Number(r.high), low: Number(r.low), close: Number(r.close), ticks: Number(r.ticks) };
      if (!Number.isSafeInteger(Number(r.time)) || Number(r.time) <= 0 || !(bar.ticks >= 1)
          || !['open', 'high', 'low', 'close'].every(key => Number.isFinite(bar[key]) && bar[key] > 0)
          || bar.low > Math.min(bar.open, bar.close) || bar.high < Math.max(bar.open, bar.close)) continue;
      map.set(Number(r.time), bar);
    }
    _idxCandleCache.set(key, { at: Date.now(), map });
    if (_idxCandleCache.size > 300) _idxCandleCache.delete(_idxCandleCache.keys().next().value);
    return map;
  } catch { return null; }
}

function engineConfig(req, res) {
  const markets = {};
  for (const [sym, c] of mktCfg) {
    // stageLev is the REAL Stage ceiling and differs per symbol (up to 500x in
    // the current format). Without it the client hardcoded a higher Boost,
    // the server clamped to the symbol's cap, and the doubled
    // margin requirement surfaced as a misleading "not enough free margin".
    /* A disabled market is reported as such rather than omitted: the terminal
       hides it from the picker, and anything still holding one can still read
       its economics to close out. */
    if (DISABLED_MARKETS.has(baseOf(sym))) continue;
    markets[sym] = {
      maxLev: c.maxLev, stageLev: stageLevCap(sym), indexed: STAGE_INDEXED.has(sym),
      takerBps: c.takerBps, makerBps: c.makerBps,
      maintenanceBps: c.maintBps, cancelOrderBps: c.cancelBps,
      riskTiers: c.tiers || [], isolatedOnly: c.isolatedOnly, status: c.status,
    };
  }
  /* Event tickers, while their window is open. The terminal builds its market
     list from this endpoint, so without them a player could not reach the
     segment the show is asking them to trade even though the engine accepts
     the order. They carry their base symbol's economics and are listed
     separately so the UI can present them as a segment rather than as another
     market that appeared from nowhere. */
  const segments = [];
  for (const alias of openAliases.keys()) {
    const base = baseOf(alias);
    const c = cfgOf(alias);
    markets[alias] = {
      maxLev: c.maxLev, stageLev: stageLevCap(alias), indexed: STAGE_INDEXED.has(base),
      takerBps: c.takerBps, makerBps: c.makerBps,
      maintenanceBps: c.maintBps, cancelOrderBps: c.cancelBps,
      riskTiers: c.tiers || [], isolatedOnly: c.isolatedOnly, status: c.status,
    };
    segments.push({ ticker: alias, base, kind: aliasKind(alias) });
  }
  /* One source of truth for what is on offer. The terminal builds its picker
     from this, so a market we have turned off disappears there without the
     client keeping its own copy of the list. */
  send(res, 200, {
    ok: true,
    apiVersion: PAPER_API_VERSION,
    schemaVersion: PAPER_SCHEMA_VERSION,
    buildId: PAPER_BUILD_ID,
    modes: PAPER_MODES,
    levCap: PAPER_MAX_LEV || null,
    markets, segments, disabled: [...DISABLED_MARKETS],
  });
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
  return send(res, 200, {
    ok: true, mode,
    /* This is the resettable public practice ladder. Official heat results
       live in immutable competition checkpoints/series standings instead. */
    official: false,
    boardKind: 'practice',
    policy: 'practice',
    ranking: 'current-equity',
    resetsAllowed: true,
    includesResetAccounts: true,
    officialResultsPath: '/api/paper/comp/state',
    updatedAt: lb.ts, total: lb.rows.length, rows: lb.top, me,
  });
}

// ── sweep ────────────────────────────────────────────────────────────────
// Limits (print-through preferred), funding, SL/TP, order-cancel tier,
// cross + isolated liquidations. One transaction per tick; per-item
// try/catch so one poisoned row can't stall the engine.
let _sweepRunning = false;
let _lastPruneMs = 0;
function sampleRoundDrawdown() {
  if (roundPaused()) return;
  /* THE SAME POSTURE AS THE TICK LOOP.
   *
   * The per-tick sample pauses a scored round the moment it cannot be taken,
   * because maximum drawdown is a published tie-break and a gap in it decides
   * places on an incomplete record. This copy logged the identical failure and
   * carried on, so the sweep went right on mutating accounts, settling funding
   * and liquidating, while the evidence a tie is broken with quietly stopped
   * being collected. Outside a live round it stays telemetry. */
  try { comp.sampleDrawdown(); }
  catch (e) {
    _log('drawdown sample failed: ' + e.message);
    tgOps('ddsample', `drawdown sampling failed in sweep: ${e && e.message}`);
    if ((comp.currentRound() || {}).id) {
      try {
        if (e && e.unpriced) pauseRound(GLOBAL_FEED_PAUSE_SYMBOL, 'drawdown',
          `tie-break sampling failed: ${e && e.message}`, e.invalidSince);
        else comp.blockRound(comp.currentRound().id, `drawdown sampling failed: ${e && e.message}`);
      } catch { /* logged above */ }
    }
  }
}
/* A failure while mutating a COMPETITION-owned account is not a row to skip.
 *
 * The tick and sweep loops caught per-item errors and carried on, so a
 * transient failure on one seat while an identical seat succeeded produced two
 * different outcomes from the same accepted tick, with the round still
 * reporting healthy. Public paper accounts may still fail in isolation; a
 * scored account cannot. */
/* Is this account seated in the round that is ACTUALLY RUNNING?
 *
 * This used comp.accountLocked(), which deliberately also covers users seated
 * on a future ARMED round so their accounts cannot be reset out from under
 * them. Right for reset protection, far too broad for deciding whether the
 * live result is compromised: a failure on an outsider merely rostered for a
 * later round was blocking the running one. */
function competitionOwned(userId) {
  try {
    const r = comp.currentRound();
    return !!(r && r.status === 'running' && comp.inRound(userId, r));
  } catch { return false; }
}
/* The markets the ROUND is actually decided on: the Hot candidates, the Hot
   backup, whatever Hot is now, the boost four, and the base of any alias that
   is currently open. An outage in one of these is an outage everybody shares,
   and pausing the field is the honest response. Anything else a contestant
   happens to be holding is not the competition. */
function roundMarkets() {
  const r = comp.currentRound() || {};
  const out = new Set();
  const add = (x) => { const b = baseOf(String(x || '').toUpperCase()); if (b) out.add(b); };
  const phase = r.id ? comp.phaseNow() : null;
  if (Number(r.format_version) >= 2) {
    /* Future draw members are both secret and irrelevant to current risk.
       Naming every candidate here made a quiet backup pause the whole field
       and leak its name in the pause payload before either reveal. */
    if (phase && phase.phase === 'hot' && phase.hotNumber) {
      add(r[`hot${phase.hotNumber}_active_base`] || r[`hot${phase.hotNumber}_base`]);
    }
    if (phase && phase.phase === 'boost') {
      try { for (const x of JSON.parse(r.boost_opened || '[]')) add(x); } catch {}
    }
  } else {
    try { for (const x of JSON.parse(r.hot_candidates || '[]')) add(x); } catch {}
    add(r.hot_backup); add(r.active_hot_base);
    try { for (const x of JSON.parse(r.boost_markets || '[]')) add(x); } catch {}
  }
  for (const k of openAliases.keys()) add(k);
  return out;
}
/* A market we cannot price right now, in a round that is not being decided on
   it. Nobody may open or close there, its positions are not marked and not
   liquidated, and everything else keeps trading. In memory on purpose: it is a
   statement about the present, it clears the moment the market quotes again,
   and a restart re-derives it within a tick. */
const _frozenMarkets = new Map();     // base symbol -> { since, why }
function freezeMarket(sym, why) {
  const b = baseOf(sym);
  if (!b || _frozenMarkets.has(b)) return;
  _frozenMarkets.set(b, { since: Date.now(), why: why || 'no competition-valid mark' });
  _log(`market frozen: ${b} (${why}); the round keeps running`);
}
function thawMarket(b) {
  if (!_frozenMarkets.has(b)) return;
  const f = _frozenMarkets.get(b);
  _frozenMarkets.delete(b);
  _log(`market thawed: ${b} after ${Math.round((Date.now() - f.since) / 1000)}s`);
}
function frozenMarkets() {
  /* Clears itself: a market that can be priced again is not frozen. */
  for (const b of [..._frozenMarkets.keys()]) { if (compPriceReady(b)) thawMarket(b); }
  return _frozenMarkets;
}
function marketFrozen(sym) {
  const b = baseOf(sym);
  if (!_frozenMarkets.has(b)) return null;
  if (compPriceReady(b)) { thawMarket(b); return null; }
  return _frozenMarkets.get(b);
}
/* Is the position that could not be priced ring-fenced from the rest of this
   account? Only then can the field carry on without shielding anybody. Absent
   or unreadable state answers no, because the fail-closed direction here is the
   shared pause. */
function unpricedLegIsIsolated(userId, sym) {
  if (userId == null) return false;
  try {
    const base = baseOf(sym);
    const rows = stmt.posByUser.all(userId).filter((p) => baseOf(p.symbol) === base);
    if (!rows.length) return false;
    return rows.every((p) => p.margin_mode === 'isolated');
  } catch { return false; }
}
function onSweepError(userId, what, e) {
  /* "We cannot price this right now" is not "the engine broke".
   *
   * A competition-owned account with an unpriceable leg has no risk answer,
   * which is exactly the freeze we want — no marks, no liquidations, no new
   * exposure — but it is a temporary state of the WORLD, not a fault in the
   * round. Blocking on it would stop a show for every ordinary source blip and
   * for the recovery grace after any restart. */
  if (e && e.unpriced) {
    /* A PAUSE IS SHARED, OR IT IS A SHIELD.
     *
     * Pausing only the affected account meant a contestant holding a tiny
     * unpriceable leg escaped a liquidation that hit an identical rival on
     * the same tick, and could also skip a drawdown sample. "We cannot price
     * this" is a statement about the ROUND, not about one seat, so it pauses
     * every contestant together. */
    /* ONE THIN MARKET IS NOT THE ROUND.
     *
     * This paused the whole field whatever the symbol was, so a contestant
     * who happened to be holding MET, which is quiet for more than four
     * seconds 19% of the time, froze the boost markets and the clock for
     * everybody. That happened repeatedly in the 2026-09-03 practice round.
     * The shared pause exists so nobody escapes a liquidation a rival takes
     * on the same tick, and that reasoning holds for the markets the round is
     * being decided on. It does not hold for a market nobody else is in. */
    /* CROSS MARGIN MAKES IT THE ROUND'S PROBLEM AGAIN.
     *
     * Freezing one market and carrying on was right for the market, and wrong
     * for the ACCOUNT. Under cross margin every position shares one pot, so an
     * unpriceable leg means that contestant's equity is unknown, and skipping
     * their risk pass while an identical rival is liquidated on the same tick
     * is the exact shield the shared pause exists to prevent. A tiny position
     * in a quiet market would have bought immunity for a large one in the
     * active market.
     *
     * So the unit is the RISK unit, not the market. An ISOLATED leg carries
     * its own margin and cannot move the rest of the account, so it freezes
     * alone and the field keeps trading. A CROSS leg, or anything we cannot
     * attribute, pauses everybody, which is what fairness costs. */
    const sym = drawdownPauseSymbol(e) || 'unknown';
    const liveRound = comp.currentRound();
    /* Two-Hot rounds use one active clock and one fairness domain. Any
       contestant price failure freezes that shared clock in build, warning,
       Hot, final-build and Boost alike; an isolated leg must not give only its
       owner a private risk holiday while everybody else's time keeps burning.
       Public accounts and historical v1 rounds retain the local-freeze rule. */
    if (liveRound && Number(liveRound.format_version) >= 2 && competitionOwned(userId)) {
      pauseRound(GLOBAL_FEED_PAUSE_SYMBOL, what, e.message, e.invalidSince);
      return;
    }
    const isolatedOnly = sym !== 'unknown' && unpricedLegIsIsolated(userId, sym);
    if (isolatedOnly && !roundMarkets().has(baseOf(sym))) {
      freezeMarket(sym, e.message);
      return;
    }
    pauseRound(sym, what, e.message);
    return;
  }
  _log(`${what} error: ${e.message}`);
  /* Blast radius is the RUNNING roster.
   *
   * This still asked accountLocked(), which deliberately also covers seats on
   * a future ARMED round so their accounts cannot be reset. I corrected
   * competitionOwned() last round and left this call untouched, so a failure
   * on an outsider merely rostered for a later round still blocked the live
   * one. Reset protection and result blast radius are different questions. */
  if (!competitionOwned(userId)) return;
  try {
    const r = comp.currentRound();
    if (r && !r.blocked_reason) {
      comp.blockRound(r.id, `${what} failed: ${e.message}`.slice(0, 400));
      _log(`round ${r.id} BLOCKED: a scored account could not be mutated (${what})`);
    }
  } catch (e2) { _log('failed to block the round after a scored-account error: ' + e2.message); }
}
/** Run one logical unit in its own savepoint, fail-closed for competitors. */
function competitionSafe(userId, what, fn) {
  try { atomically(fn); }
  catch (e) { onSweepError(userId, what, e); }
}

function sweepOneOrder(o, now = Date.now(), recovery = null) {
    /* Cleanup first: an order resting on a closed event ticker is
       cancelled whatever the round state. Skipping it because the round
       had settled left it stranded on a ticker that no longer exists. */
    if (aliasKind(o.symbol) && !aliasOpen(o.symbol)
        && !recoveryAliasAllowed(o.symbol, o.user_id, recovery)) {
      if (recovery && recovery === _roundRecoveryBatch) throw recoveryUnavailable('closed order alias');
      stmt.ordClose.run('CANCELLED', now, o.id);
      return;
    }
    // and a resting order must never FILL into a settled or blocked round
    if (comp.writeBarrier(o.user_id, now)) {
      if (recovery && recovery === _roundRecoveryBatch) throw recoveryUnavailable('order write barrier');
      return;
    }
    /* THE FAMILY INVARIANT HOLDS AT FILL TIME, NOT JUST AT PLACEMENT.
       Placement-time checking alone had two holes: close the base leg
       and a once-"reducing" resting order becomes an opener; or open the
       sibling AFTER the order was placed. Either way the sweep filled
       into opposite-direction family exposure, rebuilding the
       capped-loss hedge. An order that would violate the invariant NOW
       is cancelled, not filled: the world changed under it, and filling
       a stale intention is how the exploit comes back. */
    if (!o.reduce_only) {
      const posNow0 = stmt.posGet.get(o.user_id, o.symbol);
      const clash0 = familyConflict(o.user_id, o.symbol, resultingDir(posNow0, o.side, o.size), {});
      if (clash0) {
        stmt.ordClose.run('CANCELLED', now, o.id);
        _log(`order ${o.id} cancelled at fill time: would ${clash0.kind === 'position' ? 'oppose' : 'conflict with'} ${clash0.symbol} (one market, one direction)`);
        return;
      }
    }
    /* Stage orders live in the stage price world: they execute at the
       composite index, chart against it, liquidate against it and are
       scored against it. Triggering them from Phoenix venue prints let
       a limit fill at a price the trader's own screen never showed. */
    if (isStage(heatOf(o.user_id))) {
      /* A stage order lives ENTIRELY in the stage price world and is
         settled here, not by falling through to the venue path below.
         Requiring the composite to cross AND then Phoenix prints or the
         venue mark to cross made the composite necessary but not
         sufficient: a trader watched their own chart trade through the
         limit while the order sat resting because a different price
         world had not moved. */
      const sm = markOfFreshFor(o.symbol, true, { userId: o.user_id, forLeverage: o.leverage });
      if (!(Number(sm) > 0)) {
        if (recovery && recovery === _roundRecoveryBatch) throw recoveryUnavailable('order mark');
        return;                       // no index, no fill
      }
      if (!(o.side === 'BUY' ? sm <= o.price : sm >= o.price)) return;
      const acctS = ensureAccount(o.user_id);
      const posS = stmt.posGet.get(o.user_id, o.symbol);
      // A deployment does not rewrite acknowledged orders or existing cross
      // positions. Cancel legacy cross orders that would create exposure;
      // genuine reductions keep their original position's margin mode.
      if ((o.margin_mode !== 'isolated' || (posS && !isIso(posS)))
          && !o.reduce_only && !reducesPosition(posS, o.side, o.size)) {
        stmt.ordReject.run(now, 'stage_cross_reduce_only', o.id);
        return;
      }
      let sizeS = o.size;
      let marginDecisionS = null;
      if (o.reduce_only) {
        if (!posS || (o.side === 'BUY') === (posS.side === 'LONG')) { stmt.ordClose.run('CANCELLED', now, o.id); return; }
        sizeS = Math.min(sizeS, posS.size);
        if (!(sizeS > 0)) return;
      } else {
        let addN = sizeS * sm;
        if (posS && (o.side === 'BUY') !== (posS.side === 'LONG')) addN = Math.max(0, (sizeS - posS.size) * sm);
        const riskS = accountRisk(o.user_id, acctS, { excludeOrderId: o.id });
        marginDecisionS = { available: r6(riskS.free), required: r6(addN / o.leverage), openingNotional: r6(addN) };
        if (addN / o.leverage > riskS.free + 1e-9) { stmt.ordClose.run('CANCELLED', now, o.id); return; }
      }
      /* Re-check the CAP at fill time, not only at rest time.
         A Boost order placed at the round's high tier while quality
         supported it could otherwise fill at that tier minutes later, after the
         published cap had already fallen to 200x. The order named a
         leverage the show no longer offers, so it is cancelled rather
         than silently honoured. */
      /* The SYMBOL, not its base. The placement path one screen up
         computes this cap as stageLevCap(symbol), so passing the base
         here made placing and filling the same order disagree: a BOOST
         ticker's ceiling collapsed to the plain market's, and every
         resting Boost order above it was cancelled at the instant it
         should have filled, logged as exceeding a cap the show never
         published. It was masked while the base cap sat at 250x and
         became near-total when the base flattened to 100x. */
      const capNow = comp.levCapFor(o.symbol, stageLevCap(o.symbol) || o.leverage, o.user_id, now);
      if (Number(o.leverage) > capNow) {
        stmt.ordClose.run('CANCELLED', now, o.id);
        _log(`order ${o.id} cancelled: ${o.leverage}x exceeds the current cap ${capNow}x`);
        return;
      }
      const attachmentProblem = attachedTriggerProblem({
        userId: o.user_id, symbol: o.symbol, orderSide: o.side,
        size: sizeS, fillPx: sm, sl: o.attach_sl, tp: o.attach_tp,
      });
      if (attachmentProblem) {
        stmt.ordReject.run(now, attachmentProblem.code, o.id);
        _log(`order ${o.id} rejected at fill: ${attachmentProblem.code} (actual ${sm})`);
        return;
      }
      /* The crossing mark can carry more notional than the resting
         limit reserved, and losses elsewhere can lower the live
         actual-equity ceiling after placement. That is an ordinary
         stale-intention outcome: cancel it here. Letting applyFill's
         final guard throw would misclassify it as an engine fault and
         allow one resting order to block the whole round. */
      const fillCapacity = boostCapacityCheck(o.user_id, o.symbol, o.side,
        sizeS, sm, { excludeOrderId: o.id });
      if (!fillCapacity.ok) {
        stmt.ordClose.run('CANCELLED', now, o.id);
        _log(`order ${o.id} cancelled at fill: Boost capacity is now ${fillCapacity.max}`);
        return;
      }
      /* Stage has one price world. A crossed resting order fills at the
         fresh composite/index observation, exactly like an immediate
         Stage order; using the limit manufactured entry PnL and could
         instantly liquidate a position on a gap. The fill, status and
         attached protection share this order's savepoint. */
      // Release the filled order's reservation before the fill's
      // before/after risk snapshots. This status update shares the same
      // savepoint and rolls back if any subsequent invariant fails.
      stmt.ordClose.run('FILLED', now, o.id);
      applyFill(o.user_id, {
        symbol: o.symbol, orderSide: o.side, size: sizeS, px: sm,
        feeBps: 0, kind: 'LIMIT', orderId: o.id,
        leverage: o.leverage, marginMode: o.margin_mode,
        boostWindow: o.boost_window !== 0, at: now,
        executionSource: 'composite-index', referenceMark: sm,
        decisionReason: 'resting-limit-index-cross',
        decisionContext: {
          order: { limitPrice: o.price, requestedSize: o.size, executedSize: sizeS },
          margin: marginDecisionS,
          trigger: o.attach_sl != null || o.attach_tp != null
            ? { sl: o.attach_sl, tp: o.attach_tp, validatedAtFill: true, fillPrice: sm }
            : null,
          liquidity: { model: 'unbounded-index', executedSize: sizeS },
        },
      });
      applyAttachedTriggers(o, sm);
      return;                                   // never consult the venue world
    }
    const m = mkt(o.symbol);
    if (!mktFresh(m)) {
      const quiet = m ? now - (m.lastUpdatedMs || 0) : Infinity;
      if (quiet > DELIST_MS) stmt.ordClose.run('CANCELLED', now, o.id);
      return;
    }
    const mark = effMark(m);
    // eager orphan cleanup: a reduce-only order whose position is gone
    // (or flipped) can never fill — cancel it now, don't let it linger
    // until price crosses it
    if (o.reduce_only) {
      const pos0 = stmt.posGet.get(o.user_id, o.symbol);
      if (!pos0 || (o.side === 'BUY') === (pos0.side === 'LONG')) { stmt.ordClose.run('CANCELLED', now, o.id); return; }
    }
    // price-time realism: when the market has a live on-chain tape, a
    // resting limit fills only against REAL counterparty volume that
    // printed THROUGH its price, capped to that volume (partial fills
    // accumulate across sweeps via the vol_ts watermark). Mark-cross is
    // the fallback for thin markets / warehouse outages and fills whole.
    let size, tapePartial = false, tapeWm = null, tapeAdvanceTo = null;
    let tapeEligibility = null;
    if (tapeActive(o.symbol)) {
      const wm = o.vol_ts || o.created_at;
      tapeEligibility = printEligibilityThrough(o.symbol, o.side, o.price, wm);
      const cap = snapLots(o.symbol, tapeEligibility.volume);
      if (!(cap > 0)) return;                 // crossed but no fillable volume yet
      size = Math.min(o.size, cap);
      tapeWm = wm;
      tapeAdvanceTo = tapeEligibility.newestTs;
      tapePartial = size < o.size - 1e-9;
    } else {
      if (!(o.side === 'BUY' ? mark <= o.price : mark >= o.price)) return;
      size = o.size;
    }
    const acct = ensureAccount(o.user_id);
    const pos = stmt.posGet.get(o.user_id, o.symbol);
    let marginDecision = null;
    if (o.reduce_only) {
      if (!pos || (o.side === 'BUY') === (pos.side === 'LONG')) { stmt.ordClose.run('CANCELLED', now, o.id); return; }
      size = Math.min(size, pos.size);
      if (!(size > 0)) return;
      tapePartial = tapePartial && size < o.size - 1e-9;
    } else {
      let addedNotional = size * o.price;
      if (pos && (o.side === 'BUY') !== (pos.side === 'LONG')) addedNotional = Math.max(0, (size - pos.size) * o.price);
      // free collateral with this order's own reservation excluded;
      // require the maker fee on top of the margin
      const riskEx = accountRisk(o.user_id, acct, { excludeOrderId: o.id });
      const required = addedNotional / o.leverage + size * o.price * (cfgOf(o.symbol).makerBps / 1e4);
      marginDecision = { available: r6(riskEx.free), required: r6(required), openingNotional: r6(addedNotional) };
      if (required > riskEx.free + 1e-9) { stmt.ordClose.run('CANCELLED', now, o.id); return; }
    }
    const attachmentProblem = attachedTriggerProblem({
      userId: o.user_id, symbol: o.symbol, orderSide: o.side,
      size, fillPx: o.price, sl: o.attach_sl, tp: o.attach_tp,
    });
    if (attachmentProblem) {
      stmt.ordReject.run(now, attachmentProblem.code, o.id);
      _log(`order ${o.id} rejected at fill: ${attachmentProblem.code} (actual ${o.price})`);
      return;
    }
    if (tapePartial) {
      // consume this slice, keep the order resting for the remainder;
      // advance the watermark so these prints aren't counted again
      /* Advance to the newest ELIGIBLE event timestamp, not wall time.
         Warehouse events arrive seconds late; stamping Date.now() made
         every later backfill event look older than the cursor and a
         partially filled order could stop forever. */
      stmt.ordPartial.run(r6(o.size - size), tapeAdvanceTo, o.id);
    } else {
      stmt.ordClose.run('FILLED', now, o.id);
    }
    applyFill(o.user_id, {
      symbol: o.symbol, orderSide: o.side, size, px: o.price,
      feeBps: cfgOf(o.symbol).makerBps, kind: 'LIMIT', orderId: o.id,
      leverage: o.leverage, marginMode: o.margin_mode || 'cross',
      boostWindow: o.boost_window !== 0, at: now,
      executionSource: tapeWm != null ? 'venue-print' : 'mark-cross-fallback',
      referenceMark: mark,
      decisionReason: tapeWm != null ? 'resting-limit-print-through' : 'resting-limit-mark-cross-fallback',
      decisionContext: {
        order: { limitPrice: o.price, remainingBefore: o.size, executedSize: size, partial: tapePartial },
        margin: marginDecision,
        trigger: o.attach_sl != null || o.attach_tp != null
          ? { sl: o.attach_sl, tp: o.attach_tp, validatedAtFill: true, fillPrice: o.price }
          : null,
        liquidity: tapeWm != null
          ? { model: 'per-user-counterfactual', queueModel: 'print-through-approximation', watermarkBefore: tapeWm, watermarkAfter: tapeAdvanceTo, eligibleBase: tapeEligibility.volume, eligiblePrints: tapeEligibility.printCount, executedBase: size }
          : { model: 'mark-cross-fallback', executedBase: size },
      },
    });
    applyAttachedTriggers(o, o.price);
}

function sweep() {
  if (deploymentMaintenanceActive()) return;
  if (_sweepRunning) return;
  const _t0 = perfNow.now(); _sweepMarks.length = 0;
  /* Sampled here rather than on its own timer so the readiness record shares
     the sweep's cadence: the history is what the round-start invariant reads,
     and it should be paced by the same clock that prices everything else. */
  try { sampleReadiness(); } catch { /* never let bookkeeping stop the sweep */ }
  sweepMark('readiness');
  try { ensureCompetitionClockHealth(Date.now()); } catch { /* the next sweep retries the durable freeze */ }
  sweepMark('globalPause');
  // Recovery has its own all-roster commit edge. The ordinary paused sweep
  // below still performs no scored work if any dependency remains missing.
  recoverRoundPriceBatch();
  try { clearPauseIfPriceable(); } catch { /* the pause simply persists */ }
  sweepMark('clearPause');
  _sweepRunning = true;
  try {
    snapFile();
    sweepMark('snap');
    if (!pricesUp() && !holdsRoundPrices()) return;
    const now = Date.now();
    comp.advanceRoundClock(now);   // settle anything due before touching state
    sweepMark('clock');
    db.transaction(() => {
      // 1) resting limit orders
      /* Public orders keep per-order isolation; seated orders share ONE
         transaction for the whole sweep event, so two identical seats cannot
         end it in different states because of iteration order. */
      const allOrders = stmt.ordOpenAll.all();
      /* A resting contestant order filling during a shared pause is the same
         unfairness as a market order placed during one: the field's automatic
         downside is frozen while this exposure still changes. Public orders
         are untouched, they are not in the competition. */
      const seatedOrders = roundPaused()
        ? []
        : allOrders.filter((o) => competitionOwned(o.user_id));
      // Deterministic price/time evaluation within this process. This does not
      // imply a shared venue queue: each account is counterfactual by contract.
      // Scored orders are Stage/index orders and do not consult venue prints.
      const publicOrders = allOrders.filter((o) => !competitionOwned(o.user_id)).sort(restingPriceTime);
      if (seatedOrders.length) {
        let seatFail = null;
        try {
          atomically(() => {
            for (const o of seatedOrders) {
              try { sweepOneOrder(o, now); }
              catch (e) {
                if (e && e.unpriced) { onSweepError(o.user_id, `sweep order ${o.id}`, e); continue; }
                seatFail = { userId: o.user_id, what: `sweep order ${o.id}`, e }; throw e;
              }
            }
          });
        } catch (e) {
          if (!seatFail) throw e;
          onSweepError(seatFail.userId, seatFail.what, seatFail.e);
        }
      }
      for (const o of publicOrders) {
        /* ONE savepoint per order.
         *
         * The loop previously ran inside the sweep's single transaction with a
         * per-order catch. A nested applyFill savepoint could commit while the
         * order-status update that followed it failed; the catch swallowed the
         * error, the outer transaction committed the fill, and the order stayed
         * OPEN. The next sweep filled it again: a durable double-fill. */
        competitionSafe(o.user_id, `sweep order ${o.id}`, () => sweepOneOrder(o, now));
      }
      /* 2) and 3) positions and cross risk.
       *
       * ONE SWEEP EVENT, ONE OUTCOME FOR EVERY SCORED SEAT.
       *
       * These passes used to catch per participant and carry on. The round
       * blocked afterwards, but only AFTER an earlier identical player had
       * already been liquidated and a later one had not: recovery began from
       * an asymmetry produced by iteration order. Competition-owned work is
       * now one all-or-nothing unit; public paper accounts keep the isolated
       * behaviour, which is right for them. */
      const onePosition = (p) => {
        const m = mkt(p.symbol);
        if (pricingRoundFor(p.user_id)) {
          // A quiet public feed is not a delisting event for an opted-in
          // contestant. Missing/hard-invalid round marks still refuse risk.
          evalPositionAtMark(p, m, now);
          return;
        }
        if (!mktFresh(m)) {
          // a delisted-market close is still a mutation of a scored account
          if (comp.writeBarrier(p.user_id, now)) return;
          const quietSince = m ? (m.lastUpdatedMs || 0) : 0;
          if ((quietSince && now - quietSince > DELIST_MS) || (!m && now - (p.updated_at || now) > DELIST_MS)) {
            if (Number(p.last_mark) > 0) {
              applyFill(p.user_id, {
                symbol: p.symbol, orderSide: p.side === 'LONG' ? 'SELL' : 'BUY',
                size: p.size, px: Number(p.last_mark), feeBps: 0, kind: 'DELIST',
                at: now, executionSource: 'last-known-mark', referenceMark: Number(p.last_mark),
                decisionReason: 'market-delist-timeout',
                decisionContext: { boundary: { type: 'delist-timeout', quietSince, timeoutMs: DELIST_MS, observedAt: now } },
              });
              _log(`delist-closed ${p.symbol} for user ${p.user_id}`);
            }
          }
          return;
        }
        evalPositionAtMark(p, m, now);
      };
      const allPositions = stmt.posAll.all();
      const users = new Set(allPositions.map((p) => p.user_id));
      const scoredPos = allPositions.filter((p) => competitionOwned(p.user_id));
      const publicPos = allPositions.filter((p) => !competitionOwned(p.user_id));
      const scoredUsers = [...users].filter((u) => competitionOwned(u));
      const publicUsers = [...users].filter((u) => !competitionOwned(u));

      // public accounts: isolated failures, exactly as before
      for (const p of publicPos) {
        try { onePosition(p); } catch (e) { _log(`sweep position ${p.user_id}/${p.symbol} error: ${e.message}`); }
      }
      for (const uid of publicUsers) {
        try { evalCrossForUser(uid, now); } catch (e) { _log(`sweep user ${uid} error: ${e.message}`); }
      }

      // scored accounts: all of them, or none of them
      if (scoredPos.length || scoredUsers.length) {
        let scoredFail = null;
        try {
          atomically(() => {
            for (const p of scoredPos) {
              try { onePosition(p); }
              catch (e) {
                if (e && e.unpriced) { onSweepError(p.user_id, `sweep position ${p.user_id}/${p.symbol}`, e); continue; }
                scoredFail = { userId: p.user_id, what: `sweep position ${p.user_id}/${p.symbol}`, e }; throw e;
              }
            }
            for (const uid of scoredUsers) {
              try { evalCrossForUser(uid, now); }
              catch (e) {
                if (e && e.unpriced) { onSweepError(uid, `sweep user ${uid}`, e); continue; }
                scoredFail = { userId: uid, what: `sweep user ${uid}`, e }; throw e;
              }
            }
          });
        } catch (e) {
          if (!scoredFail) throw e;
          // the whole competition pass rolled back; now record why it stopped
          onSweepError(scoredFail.userId, scoredFail.what, scoredFail.e);
        }
      }
      if (now - _lastPruneMs > 3600_000) {
        _lastPruneMs = now;
        try {
          const cutoff = now - FILLS_RETENTION_MS;
          const retained = Number(stmt.fillPruneProtectedCount.get(cutoff).n) || 0;
          const result = stmt.fillPrune.run(cutoff);
          if (result.changes || retained) {
            _log(`pruned ${result.changes} old fills; retained ${retained} competition-proof fill(s)`);
          }
        } catch (e) { _log(`fill retention failed: ${e && e.message}`); }
        try { const r = stmt.ordPrune.run(now - FILLS_RETENTION_MS); if (r.changes) _log(`pruned ${r.changes} closed orders`); } catch {}
      }
    })();
    sweepMark('tx');
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
    sweepMark('books');
  } catch (e) {
    _log('sweep error: ' + e.message);
  } finally {
    _sweepRunning = false;
    reportSweep(_t0);
  }
  /* After the sweep's transaction, never inside it: drawdown sampling is
     bookkeeping for a tie-break and must never be able to stall pricing or
     liquidation. */
  const _td = perfNow.now();
  sampleRoundDrawdown();
  const _dd = perfNow.now() - _td;
  if (_dd >= 100) _log(`drawdown sample took ${_dd.toFixed(0)}ms`);
}

/* ALIVE IS NOT THE SAME QUESTION AS SAFE TO TRADE.
 *
 * /healthz answered "the process is running", which is true of an engine that
 * refused to start its index chain, is down to one price endpoint, or has not
 * seen a tick in a minute. Orchestration and the desk both need the other
 * question answered separately, and answered with a status code they can act
 * on rather than a green light with bad news in the body. */
function readiness() {
  const now = Date.now();
  const pool = lazerPool(now);
  const frozen = [...frozenMarkets().keys()];
  /* The URLs are health input, not health output. */
  const poolCounts = { configured: pool.configured, healthy: pool.healthy, minimum: pool.minimum };
  const feedAgeMs = live.lastMsgMs ? now - live.lastMsgMs : null;
  const priced = [...live.map.keys()].filter((sym) => compPriceReady(sym, now)).length;
  const reasons = [];
  if (!_chainStarted) reasons.push('index source chain did not start');
  if (LAZER_TOKEN && pool.healthy < pool.minimum) reasons.push(`lazer pool ${pool.healthy}/${pool.configured}, minimum ${pool.minimum}`);
  if (feedAgeMs === null || feedAgeMs > 10_000) reasons.push('no index observation in the last 10s');
  if (!priced) reasons.push('no market is currently priceable');
  return { ok: !reasons.length, reasons, apiVersion: PAPER_API_VERSION, schemaVersion: PAPER_SCHEMA_VERSION, buildId: PAPER_BUILD_ID, lazer: poolCounts, feedAgeMs, pricedMarkets: priced, frozenMarkets: frozen,
    sourceExpiry: { enabled: _sourceExpiryEnabled, pendingTimers: _sourceExpiryTimers.size },
    pauseNotifications: { enabled: !!_pauseNotifier,
      ...(_pauseNotifier?.status() || { sent: 0, failed: 0, dropped: 0, pending: false, queued: 0, active: false }) },
    tickPersistence: tickPersistenceStatus(),
    cadence: cadenceTable(), confBandBps: Object.fromEntries([..._confBand].map(([k, v]) => [k, Math.round(v.bps * 10) / 10])),
    /* The learned jump gate per market in bps, so an operator can see WHY
       something froze without reading logs. Majors sit on the floor. */
    clampBps: Object.fromEntries([..._clampOf.entries()].map(([k, v]) => [k, Math.round(v.jump * 1e4)])),
    /* COUNTS, NOT ADDRESSES AND NOT HOSTNAMES.
     *
     * This endpoint sits before the gate on purpose, so an orchestrator can
     * ask. That means it must not narrate the deployment: it was listing the
     * healthy provider endpoints and the top client IPs, which is operational
     * detail that a proxied route would hand to anyone. The numbers say the
     * same thing about health without saying who or where. */
    relay: { ws: indexWsClients.size, sse: pythSseClients.size, perIpCap: IDX_WS_PER_IP,
      busiestAddress: Math.max(0, ...[..._wsIpCount.values()]),
      refused: { gate: _wsRefused.gate, global: _wsRefused.global, perIp: _wsRefused.perIp } }, indexSource: INDEX_SOURCE, boot: ENGINE_BOOT_ID, up: Math.round(process.uptime()) };
}
module.exports = { init, sweep, account, readiness, deploymentMaintenanceActive, comp, compState, compBaseline, engineTime, compReadiness, compMe, compMeReady, compVerify, compAdmin, compInvite, compInviteAct, guestSession, stageCandles, placeOrder, cancelOrder, closePosition, setSltp, adjustMargin, reset, fills, ordersHistory, leaderboard, engineConfig, marketTape, pythHistory, pythStream, attachIndexWs };
module.exports.stopSourceExpiry = stopSourceExpiry;
module.exports.__testMarkets = { restrictToIndexed, DISABLED_MARKETS, STAGE_INDEXED };
module.exports.stopOpsNotifications = stopOpsNotifications;
module.exports.drainTickPersistence = drainTickPersistence;
// Internal handles for the test harness / FT port test suite. Read-mostly;
// requiring the module does not start the WS or any timers (that is init's job).
module.exports.__test = { prints, live, mktCfg, stmt, db, sweep, tickEval, recordMark, markAt, markSetAt, markSetFor, compPriceReady, compQuality, boostConsensus, activeSource, lastKnownSource, confirming: _confirming, qualityLeverageCap, boostLevCap, readyForLeverage, readyRatio, markFor, markOfFreshFor, competitionOwned, roundPaused, __onSweepError: onSweepError, frozenMarkets, marketFrozen, roundMarkets, cadenceTable, staleMsForSym, __pauseFor: (e) => pauseRound(e.symbol || 'unknown', 'test', e.message, e.startedAt),
  __clearPauses: () => { try { _pauseLatch = null; db.prepare('UPDATE paper_price_pauses SET restored_at = ? WHERE restored_at IS NULL').run(Date.now()); invalidatePause(); } catch {} },
  __pauseStmts: () => pauseStore(), closeRoundPauses, boostCapFor, __ensureAccountRef: () => _ensureAccountFn, __setEnsureAccount: (f) => { _ensureAccountFn = f; },
  __publicPause: publicPause, __safePhaseControl: safePhaseControl,
  __createPauseNotifier: createPauseNotifier, __deliverOpsMessage: deliverOpsMessage, __tgOps: tgOps,
  __opsAlertState: (key) => ({ acknowledged: _alertLast.has(key), retryScheduled: _alertRetry.has(key),
    pending: _alertPending.has(key), requests: _opsRequests.size }),
  __restorePause: (sym) => { try { pauseStore().restore.run(Date.now(), sym, (comp.currentRound() || {}).id || null); invalidatePause(); } catch {} }, __clearPauseIfPriceable: clearPauseIfPriceable, __sampleRoundDrawdown: sampleRoundDrawdown,
  __pauseAt: (sym, since, why = 'test price outage') => pauseRound(sym, 'test', why, since),
  __ensureGlobalCompetitionPause: ensureGlobalCompetitionPause,
  __competitionFeedStatus: competitionFeedStatus,
  __marketAvailability: marketAvailability,
  __historicalAvailabilityAt: historicalAvailabilityAt,
  __marketEvidenceAt: marketEvidenceAt,
  __roundExecutionMark: roundExecutionMark,
  __roundPriceAvailability: roundPriceAvailability,
  __candidateRoundMark: candidateRoundMark,
  __initializeRoundMarks: initializeRoundMarks,
  __publicRoundExecution: publicRoundExecution,
  __roundPriceAcknowledged: roundPriceAcknowledged,
  __projectRoundHistory: projectRoundHistory,
  __projectRoundTickRows: projectRoundTickRows,
  __roundScoreEvidenceValid: roundScoreEvidenceValid,
  __roundAttemptEvidenceValid: roundAttemptEvidenceValid,
  __recoverRoundPriceBatch: recoverRoundPriceBatch,
  __competitionAliasOpen: competitionAliasOpen,
  __execPxFor: execPxFor,
  compBoardGapMs: COMP_BOARD_GAP_MS,
  __competitionClockStatus: competitionClockStatus,
  __ensureCompetitionClockHealth: ensureCompetitionClockHealth,
  __armCompetitionClockExpiry: armCompetitionClockExpiry,
  __competitionClockExpiryArmed: () => !!_competitionClockExpiryTimer,
  __settledExecutionSummary: settledExecutionSummary,
  __settledExecutionCache: () => ({ ..._settledExecutionCacheStats,
    size: _settledExecutionCache.size, limit: SETTLED_EXECUTION_CACHE_MAX }),
  __executionDataVersionStatement: () => executionDataVersion,
  __sourceRejectCount: () => _srcRejects.size, resolveIndexSeq,
  __clampJumpFor: clampJumpFor, __moveNote: moveNote,
  __createLazerObservationGate: createLazerObservationGate,
  __processLazerObservation: processLazerObservation,
  __lazerObservationState: (sym) => ({ ..._lazerObservations.snapshot(sym),
    rejected: _lazerRejects.has(sym), confidence: _lazerConf.get(sym) ?? null,
    confidenceSamples: _confRing.get(sym)?.length || 0 }),
  __startSourceExpiry: startSourceExpiry, __refreshExpiredSources: refreshExpiredSources,
  __sourceExpiryTimerCount: () => _sourceExpiryTimers.size,
  __marketDiagnosticSnapshot: marketDiagnosticSnapshot,
  __marketDiagnosticObserve: observeMarketDiagnostic,
  __marketDiagnosticStats: () => ({ markets: _marketDiagnostics.size,
    retainedTransitions: [..._marketDiagnostics.values()].reduce((n, r) => n + r.transitions.length, 0) }),
  __persistTick: persistTick, __flushPersistTicks: flushPersistTicks,
  __tickPersistence: tickPersistenceStatus,
  __createIndexOutbox: createIndexOutbox, __sealPersistTicks: sealPersistTicks,
  accountRisk, __markHistory: (sym) => _markHist.get(sym), __setReadyHist: (sym, h) => _readyHist.set(sym, h), sampleReadiness, setStrict: (v) => { _strictOverride = v; }, applyFill, openAlias, closeAlias, aliasOpen: (s) => aliasOpen(s), scoreUser, scoreProofFor, fillLedgerEvidence, hotValueOf, boostExposureOf, boostCapacityCheck, boostMaximumFor, compRankSnapshot, accountRisk, liqEstimate, prepareSeat, seatState, seatState_epoch: (u) => (seatState(u) || {}).epoch, baseOf, aliasKind, aliasOpen, openAliases, openAlias, closeAlias, cfgOf, stageLevCap, mkt, ingestPrint, printedVolumeThrough, purgeExpiredInviteTokens, books, writeRate: _writeRate, guardCheck, comps: _idxComps, halt: _halt, ingestIndexTick, compUpdate,
  candleCacheTtl, mergeStageCandleRows, reconcileLiveCandleRows, historyText, createHistoryCapacity,
  historyCapacitySnapshot: () => _historyCapacity.snapshot(), fetchKlines, fetchVenueCandles, indexCandles, indexLine };
