/* Deep integration test: drives the REAL order handler end to end.
 *
 *   PAPER_DB=$(mktemp -u --suffix=.db) node test-engine-deep.js
 *
 * The other suites test pieces in isolation. This one goes through
 * paper.placeOrder itself — the same function the HTTP server routes to — so
 * it covers the parts that only break when the layers are combined: the
 * competition gate sitting in the middle of the order path, alias resolution
 * under a real fill, and whether any of it disturbed ordinary paper trading.
 *
 * Auth is stubbed at auth.validateSession (paper.js resolves the user through
 * the module object at call time), so no network and no real session.
 */
const assert = require('assert');

process.env.PHOENIX_SNAPSHOT_FILE = '/nonexistent/markets-snapshot.json';
if (!process.env.PAPER_DB || process.env.PAPER_DB.startsWith('/opt/')) {
  console.error('refusing to run: set PAPER_DB to a throwaway path first');
  process.exit(2);
}

const auth = require('./auth-shim.js');
/* These suites drive the clock by hand and never accumulate reliability
   history, which production now requires. Say so explicitly rather than
   letting the engine silently treat no-evidence as evidence. */
process.env.PAPER_ALLOW_UNPROVEN_MARKETS = '1';
const comp = require('./competition.js');
const P = require('./paper.js');
const T = P.__test;
const CT = comp.__test;

/* Fire a boundary the way the clock would: wind the round so the boundary is
   genuinely due, then fire it. Firing a segment-opening boundary while the
   phase says otherwise is not something that can happen in production, and
   the engine now refuses it rather than fabricating a segment. */
const fireAt = (id, at, processingLagMs = 200) => {
  /* A boundary opens segment gates, and opening one now requires a live
     index. The feed has to be alive AT the boundary, not only when the next
     assertion runs. */
  if (typeof refreshMarks === 'function') refreshMarks();
  CT.db.prepare('UPDATE paper_rounds SET started_at = ? WHERE id = ?')
    .run(Date.now() - at - processingLagMs, id);
  CT.fireBoundary(id, at);
};
const { ROUND_PLAN } = comp;

let pass = 0;
/* Names the in-flight test if the suite stalls. A hung suite otherwise just
   stops printing, and the last successful line is a misleading place to look. */
let _inflight = null;
const _watchdog = setInterval(() => {
  if (_inflight && Date.now() - _inflight.at > 15_000) {
    console.log(`  STALL  ${_inflight.name} (no return after 15s)`);
    process.exit(3);
  }
}, 1000);
_watchdog.unref?.();
/* Keep the index alive across the suite.
 *
 * A real engine receives ticks continuously; a fixture that seeds a price once
 * and then spends four seconds asserting does not. That used to be invisible
 * because a dead chain silently fell back to its freshest stale record. Now a
 * source past its own deadline genuinely stops pricing the market, which is
 * the point, so the fixture has to behave like a feed instead of a snapshot. */
const _fed = new Map();   // last price fed through feedMark, per symbol
function refreshMarks() {
  const now = Date.now();
  for (const [sym, m] of T.live.map) {
    if (T.aliasKind(sym)) continue;
    const px = Number(m.pythPrice);
    if (!(px > 0)) continue;
    /* WITH a provider timestamp: since the round four review the top tier
       refuses a source whose age cannot be measured. Stamping the venue
       component keeps the fixture's source chain and budgets exactly as they
       were and only makes the observation ageable. */
    T.compUpdate(sym, 'usdt', px, now, now - 50);
    T.compUpdate(sym, 'usd', px, now);
  }
}

const ok = async (name, fn) => {
  _inflight = { name, at: Date.now() };
  refreshMarks();
  try { await fn(); console.log('  ok   ' + name); pass++; }
  catch (e) { console.log('  FAIL ' + name + '\n       ' + e.message); process.exitCode = 1; }
  finally { _inflight = null; }
};
const near = (a, b, eps = 1e-6) => Math.abs(a - b) < eps;

// ── harness ──────────────────────────────────────────────────────────────
let CURRENT_USER = null;
auth.validateSession = async () => CURRENT_USER;          // stubbed, no network
const asUser = (id) => { CURRENT_USER = { id, isGuest: false }; };

function mkRes() {
  const r = { code: null, body: null, writeHead(c) { r.code = c; }, end(s) { r.body = JSON.parse(s); } };
  return r;
}
function mkReq(body) {
  const buf = Buffer.from(JSON.stringify(body || {}));
  return { headers: {}, on(ev, cb) { if (ev === 'data') cb(buf); if (ev === 'end') cb(); return this; } };
}
const order = async (body) => {
  const res = mkRes();
  await P.placeOrder(mkReq(body), res);
  // the write-rate limiter is per minute and this suite fires many orders
  T.writeRate.clear();
  return res;
};

const setMarkRaw = (sym, px) => T.live.map.set(sym, {
  markPrice: px, pythPrice: px, pythAtMs: Date.now(), pythBasis: 0,
  lastUpdatedMs: Date.now(), indexHalt: false, currentFundingRate: 0,
});

/* Fixtures must look like a HEALTHY index, not just a populated map. Stage
   pricing now requires a fresh composite from at least two agreeing
   components while a round is live, and checkpoints price strictly from
   recorded history, so a fixture that only wrote live.map was pretending to
   be a market it was not. */
function feedMark(sym, px, t = Date.now()) {
  /* Walk to the target in sub-clamp steps, the way a market actually moves.
     A fixture that jumped 100 -> 110 in one tick is a 1000bps step, which the
     engine now (correctly) treats as an unconfirmed jump and freezes the
     symbol for. Freezing on a 10% single-tick move is the desired behaviour;
     the fixture was the thing that was unrealistic. */
  /* Step from the last value THIS helper fed, not from live.map: setMarkRaw
     writes live.map directly and bypasses the clamp, so the two can be far
     apart and the walk would start from the wrong place. */
  const from = _fed.get(sym) || 0;
  if (from > 0 && Math.abs(px - from) / from > 0.004) {
    const steps = Math.ceil(Math.abs(Math.log(px / from)) / 0.004);
    for (let i = 1; i < steps; i++) {
      const mid = from * Math.pow(px / from, i / steps);
      T.compUpdate(sym, 'usdt', mid, t, t - 50);
      T.compUpdate(sym, 'usd', mid, t);
    }
  }
  T.compUpdate(sym, 'usdt', px, t, t - 50);
  T.compUpdate(sym, 'usd', px, t);
  _fed.set(sym, px);
  /* A real index ticks continuously, so any boundary instant has a mark at or
     before it. A fixture that records ONE sample at "now" has no history
     behind it, and a suite fast enough to fire a boundary within the same
     second finds nothing to price from. Seed a short trail. */
  /* Record the component count EXPLICITLY. Deriving it at record time reads
     whatever components happen to be fresh right then, so a fixture that fed
     two sources could still write single-source history and make strict
     pricing refuse it later. This helper genuinely feeds two. */
  for (let back = 20_000; back > 0; back -= 2_000) T.recordMark(sym, px, t - back, 2);
  T.recordMark(sym, px, t, 2);
}
const CFG = {
  tiers: [], maxLev: 40, lotSize: null, takerBps: 3.5, makerBps: 0.5,
  maintBps: 50, cancelBps: 0, maxLiqSize: null, status: 'active', isolatedOnly: false,
};
for (const s of ['BTC', 'SOL', 'ETH', 'BNB', 'XRP']) { setMarkRaw(s, 100); feedMark(s, 100); T.mktCfg.set(s, { ...CFG }); }

comp.wire({
  marketReady: () => true, // Explicit fixture price policy.
  openAlias: T.openAlias, closeAlias: T.closeAlias, aliasOpen: T.aliasOpen,
  scoreUser: T.scoreUser, scoreProofFor: T.scoreProofFor, hotValueOf: T.hotValueOf,
  segmentResidue: (alias) => ({
    positions: T.stmt.posBySymbol.all(alias).length,
    orders: T.stmt.ordOpenBySymbol.all(alias).length,
  }),
  // startRound now prepares the roster itself, so it needs the real hooks
  prepareSeat: T.prepareSeat, seatState: T.seatState, markSetFor: T.markSetFor,
  equityOf: (uid) => { const a = T.stmt.acctGet.get(uid); return a ? T.accountRisk(uid, a).equityTotal : NaN; },
  log: () => {},
});

const PLAYER = 9101, SPECTATOR = 9102;
function mkAccount(uid) {
  T.db.prepare('INSERT OR IGNORE INTO users (id) VALUES (?)').run(uid);
  T.stmt.acctIns.run(uid, Date.now(), Date.now());
  T.db.prepare('UPDATE paper_accounts SET heat = 1, start_balance = 10, balance = 10 WHERE user_id = ?')
    .run(uid);
}
mkAccount(PLAYER); mkAccount(SPECTATOR);
const posOf = (uid, sym) => T.stmt.posGet.get(uid, sym);
const balOf = (uid) => T.stmt.acctGet.get(uid).balance;

(async () => {
  console.log('\nordinary trading is undisturbed');
  asUser(SPECTATOR);
  await ok('a market order opens a position through the real handler', async () => {
    const r = await order({ symbol: 'BTC', side: 'BUY', type: 'MARKET', size: 0.01, leverage: 10 });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    const p = posOf(SPECTATOR, 'BTC');
    assert.ok(p, 'position should exist');
    assert.strictEqual(p.side, 'LONG');
  });
  await ok('closing at the same mark returns the balance (no fees in stage)', async () => {
    const before = 10;
    const r = await order({ symbol: 'BTC', side: 'SELL', type: 'MARKET', size: 0.01, leverage: 10 });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    assert.strictEqual(posOf(SPECTATOR, 'BTC'), undefined, 'position should be flat');
    assert.ok(near(balOf(SPECTATOR), before), `balance drifted: ${balOf(SPECTATOR)} vs ${before}`);
  });
  await ok('profit is banked exactly', async () => {
    await order({ symbol: 'BTC', side: 'BUY', type: 'MARKET', size: 0.01, leverage: 10 });
    setMarkRaw('BTC', 110); feedMark('BTC', 110);                       // +10 on 0.01 units = +0.10
    await order({ symbol: 'BTC', side: 'SELL', type: 'MARKET', size: 0.01, leverage: 10 });
    assert.ok(near(balOf(SPECTATOR), 10.1), `expected 10.1, got ${balOf(SPECTATOR)}`);
    setMarkRaw('BTC', 100); feedMark('BTC', 100);
  });
  await ok('stage leverage is available outside a competition', async () => {
    await order({ symbol: 'BTC', side: 'BUY', type: 'MARKET', size: 0.01, leverage: 1000 });
    /* The engine cap is the terminal's cap: 100x since the owner rule of
       2026-09-04, with 500x reachable only on a BOOST twin inside a round. An
       ask above it is clamped, not refused. */
    assert.strictEqual(posOf(SPECTATOR, 'BTC').leverage, 100, 'the engine cap should apply');
    await order({ symbol: 'BTC', side: 'SELL', type: 'MARKET', size: 0.01, leverage: 1000 });
  });
  await ok('event tickers do not exist outside a competition', async () => {
    const r = await order({ symbol: 'BTC-HOT', side: 'BUY', type: 'MARKET', size: 0.01, leverage: 10 });
    assert.strictEqual(r.code, 400);
    assert.strictEqual(r.body.error, 'market_closed');
  });

  console.log('\nthe competition gate, through the real order path');
  comp.createRound({
    id: 'deep', candidates: ['BTC', 'SOL', 'ETH'],
    players: [{ userId: PLAYER, displayName: 'Mia', seat: 0 }],
  });
  comp.startRound('deep');
  const warp = (ms) => CT.db.prepare('UPDATE paper_rounds SET started_at = ? WHERE id = ?')
    .run(Date.now() - ms, 'deep');

  asUser(PLAYER);
  await ok('a player is capped at the baseline on ordinary tickers', async () => {
    const r = await order({ symbol: 'BTC', side: 'BUY', type: 'MARKET', size: 0.01, leverage: 1000 });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    assert.strictEqual(posOf(PLAYER, 'BTC').leverage, comp.COMP_BASE_LEV,
      'asking for 1000x mid-round must be clamped to 100x');
    await order({ symbol: 'BTC', side: 'SELL', type: 'MARKET', size: 0.01, leverage: 100 });
  });
  asUser(SPECTATOR);
  await ok('the public keeps full leverage while the show runs', async () => {
    const r = await order({ symbol: 'BTC', side: 'BUY', type: 'MARKET', size: 0.01, leverage: 1000 });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    assert.strictEqual(posOf(SPECTATOR, 'BTC').leverage, 100,
      'a live round must not re-rule ordinary paper trading: the spectator keeps the ordinary terminal cap, which is 100x, and a round running next door neither raises nor lowers it');
    await order({ symbol: 'BTC', side: 'SELL', type: 'MARKET', size: 0.01, leverage: 1000 });
  });

  console.log('\nHot Market segment, end to end');
  const draw = CT.privateDrawOf(CT.q.get.get('deep'));
  const plan = comp.planOf(CT.q.get.get('deep'));
  const HOT = draw.hot1.asset;
  const OTHER = HOT === 'BTC' ? 'SOL' : 'BTC';
  fireAt('deep', draw.hot1.activation - plan.hotWarning);
  asUser(PLAYER);
  await ok('the retired synthetic Hot ticker is refused in v2', async () => {
    const r = await order({ symbol: HOT + '-HOT', side: 'BUY', type: 'MARKET', size: 0.01, leverage: 10 });
    assert.strictEqual(r.body.error, 'market_closed');
  });
  await ok('an already-open ordinary position carries into Hot unchanged', async () => {
    await order({ symbol: OTHER, side: 'BUY', type: 'MARKET', size: 0.01, leverage: 10 });
    const r = await order({ symbol: HOT, side: 'BUY', type: 'MARKET', size: 0.01, leverage: 10 });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    const before = posOf(PLAYER, HOT);
    fireAt('deep', draw.hot1.activation);
    assert.strictEqual(CT.q.get.get('deep').hot1_active_base, HOT);
    assert.strictEqual(posOf(PLAYER, HOT).entry_price, before.entry_price);
    assert.ok(posOf(PLAYER, OTHER), 'the unrelated base position should be untouched');
  });
  await ok('Hot uses the same ordinary book and position', async () => {
    assert.strictEqual(T.aliasOpen(HOT + '-HOT'), false);
    assert.ok(posOf(PLAYER, HOT));
  });
  await ok('Hot close preserves the ordinary leg and freezes only its economic delta bonus', async () => {
    setMarkRaw(HOT, 110); feedMark(HOT, 110);
    const liveBonus = comp.hotBonusFor(CT.q.get.get('deep'), PLAYER);
    assert.ok(liveBonus > 0, 'Hot-period gain should add one score copy');
    /* The 110 mark was accepted immediately before this boundary. Fire at
       the exact due edge so the immutable close and the live pre-close score
       intentionally consume the same observation; a synthetic 200ms
       processing lag would put the newly accepted mark after the due instant
       and make the assertion depend on how many milliseconds the fixture took. */
    fireAt('deep', draw.hot1.activation + plan.hotDuration, 0);
    assert.ok(posOf(PLAYER, HOT), 'ordinary Hot asset must remain open');
    assert.ok(posOf(PLAYER, OTHER), 'unrelated base leg must survive');
    assert.ok(near(comp.hotBonusFor(CT.q.get.get('deep'), PLAYER), liveBonus));
  });

  /* Complete the second committed ordinary-asset Hot window before Boost. */
  fireAt('deep', draw.hot2.activation - plan.hotWarning);
  fireAt('deep', draw.hot2.activation);
  fireAt('deep', draw.hot2.activation + plan.hotDuration);

  console.log('\nBoost window, end to end');
  fireAt('deep', plan.finalBuildStart);
  fireAt('deep', plan.boostStart);
  await ok('a boost twin accepts the full boost cap for a player', async () => {
    const r = await order({ symbol: 'BTC-BOOST', side: 'BUY', type: 'MARKET', size: 0.001, leverage: 1000 });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    assert.strictEqual(posOf(PLAYER, 'BTC-BOOST').leverage, 500, 'clamped to the 500x boost cap');
  });
  await ok('the boost position is ISOLATED, so a wick cannot take the round', async () => {
    assert.strictEqual(posOf(PLAYER, 'BTC-BOOST').margin_mode, 'isolated',
      'this is the whole mitigation for index-noise liquidation');
    assert.ok(posOf(PLAYER, 'BTC-BOOST').isolated_margin > 0, 'margin must actually be committed');
  });
  await ok('the base ticker stays capped even during Boost', async () => {
    await order({ symbol: 'BTC', side: 'SELL', type: 'MARKET', size: 0.01, leverage: 100 });
    const r = await order({ symbol: 'BTC', side: 'BUY', type: 'MARKET', size: 0.01, leverage: 1000 });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    assert.strictEqual(posOf(PLAYER, 'BTC').leverage, comp.COMP_BASE_LEV);
  });
  await ok('opposite exposure across a family is refused', async () => {
    /* The player is long BTC-BOOST here. Shorting the base against it is the
       hedge that minted equity: -BOOST is forced isolated, so its loss is
       capped at the allocated margin while the base leg pays in full. The
       reviewer's probe turned $10 into $10.40 this way. */
    const held = posOf(PLAYER, 'BTC');
    const flipSize = (held ? held.size : 0) + 0.02;   // big enough to flip through flat
    const r = await order({ symbol: 'BTC', side: 'SELL', type: 'MARKET', size: flipSize, leverage: 100 });
    assert.strictEqual(r.code, 400, JSON.stringify(r.body));
    assert.strictEqual(r.body.error, 'family_direction_conflict');
    assert.ok(posOf(PLAYER, 'BTC-BOOST'), 'the sibling must be untouched by a refusal');
  });
  await ok('reducing your own leg stays legal while a sibling is open', async () => {
    /* The guard keys on the RESULTING direction, not the order side. Keying on
       the side blocked a plain reduce and broke ordinary risk management. */
    const held = posOf(PLAYER, 'BTC');
    assert.ok(held && held.size > 0, 'need a base position to reduce');
    const r = await order({ symbol: 'BTC', side: 'SELL', type: 'MARKET', size: held.size / 2, leverage: 100 });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
  });
  await ok('same-direction sibling exposure is still allowed', async () => {
    const r = await order({ symbol: 'BTC-BOOST', side: 'BUY', type: 'MARKET', size: 0.001, leverage: 500 });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
  });

  asUser(SPECTATOR);
  await ok('a spectator cannot touch a boost twin even while it is open', async () => {
    const r = await order({ symbol: 'BTC-BOOST', side: 'BUY', type: 'MARKET', size: 0.001, leverage: 1000 });
    assert.strictEqual(r.code, 400);
    assert.strictEqual(r.body.error, 'market_closed');
  });

  console.log('\nthe bell');
  await ok('the bell marks everyone without closing base positions', async () => {
    /* BASE positions are marked, never closed: that is the rule this test
       guards. Segment twins (-HOT, -BOOST) are the exception since Sep 2:
       their ticker shuts at the bell and a position left on it could not be
       closed by anyone, so they are settled at the bell's own mark. */
    const base = () => T.stmt.posByUser.all(PLAYER).filter((p) => !/-(HOT|BOOST)$/.test(p.symbol)).length;
    const openBefore = base();
    assert.ok(openBefore > 0, 'the player should still be holding something');
    fireAt('deep', plan.total);
    assert.strictEqual(base(), openBefore,
      'no base position may be force-closed at the bell');
    const board = comp.standings('deep', 'final');
    assert.strictEqual(board.length, 1);
    assert.ok(Number.isFinite(board[0].score), 'a score must have been recorded');
  });
  await ok('leverage returns to normal once the round is done', async () => {
    asUser(PLAYER);
    const r = await order({ symbol: 'BNB', side: 'BUY', type: 'MARKET', size: 0.01, leverage: 1000 });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    /* "Normal" is the terminal's own cap, which is 100x since the owner rule of
       2026-09-04. Above it exists only on a BOOST twin, inside a round. */
    assert.strictEqual(posOf(PLAYER, 'BNB').leverage, 100, 'the gate lifts to the base cap, not to the Boost cap');
  });

  console.log(`\n${pass} passed${process.exitCode ? ', WITH FAILURES' : ''}\n`);
})();
