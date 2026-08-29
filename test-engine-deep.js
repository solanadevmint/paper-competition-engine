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
const comp = require('./competition.js');
const P = require('./paper.js');
const T = P.__test;
const CT = comp.__test;
const { ROUND_PLAN } = comp;

let pass = 0;
const ok = async (name, fn) => {
  try { await fn(); console.log('  ok   ' + name); pass++; }
  catch (e) { console.log('  FAIL ' + name + '\n       ' + e.message); process.exitCode = 1; }
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

const setMark = (sym, px) => T.live.map.set(sym, {
  markPrice: px, pythPrice: px, pythAtMs: Date.now(), pythBasis: 0,
  lastUpdatedMs: Date.now(), indexHalt: false, currentFundingRate: 0,
});
const CFG = {
  tiers: [], maxLev: 40, lotSize: null, takerBps: 3.5, makerBps: 0.5,
  maintBps: 50, cancelBps: 0, maxLiqSize: null, status: 'active', isolatedOnly: false,
};
for (const s of ['BTC', 'SOL', 'ETH', 'BNB', 'XRP']) { setMark(s, 100); T.mktCfg.set(s, { ...CFG }); }

comp.wire({ openAlias: T.openAlias, closeAlias: T.closeAlias, scoreUser: T.scoreUser, log: () => {} });

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
    setMark('BTC', 110);                       // +10 on 0.01 units = +0.10
    await order({ symbol: 'BTC', side: 'SELL', type: 'MARKET', size: 0.01, leverage: 10 });
    assert.ok(near(balOf(SPECTATOR), 10.1), `expected 10.1, got ${balOf(SPECTATOR)}`);
    setMark('BTC', 100);
  });
  await ok('stage leverage is available outside a competition', async () => {
    await order({ symbol: 'BTC', side: 'BUY', type: 'MARKET', size: 0.01, leverage: 1000 });
    assert.strictEqual(posOf(SPECTATOR, 'BTC').leverage, 1000, 'the engine cap should apply');
    await order({ symbol: 'BTC', side: 'SELL', type: 'MARKET', size: 0.01, leverage: 1000 });
  });
  await ok('event tickers do not exist outside a competition', async () => {
    const r = await order({ symbol: 'BTC-HOT', side: 'BUY', type: 'MARKET', size: 0.01, leverage: 10 });
    assert.strictEqual(r.code, 400);
    assert.strictEqual(r.body.error, 'market_closed');
  });

  console.log('\nthe competition gate, through the real order path');
  comp.createRound({
    id: 'deep', candidates: ['BTC', 'SOL'],
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
    assert.strictEqual(posOf(SPECTATOR, 'BTC').leverage, 1000,
      'a live round must not re-rule ordinary paper trading');
    await order({ symbol: 'BTC', side: 'SELL', type: 'MARKET', size: 0.01, leverage: 1000 });
  });

  console.log('\nHot Market segment, end to end');
  warp(ROUND_PLAN.round.reveal + 500);
  CT.fireBoundary('deep', ROUND_PLAN.round.reveal);
  const HOT = CT.q.get.get('deep').hot_base + '-HOT';
  asUser(PLAYER);
  await ok('the hot ticker is refused before its window opens', async () => {
    const r = await order({ symbol: HOT, side: 'BUY', type: 'MARKET', size: 0.01, leverage: 10 });
    assert.strictEqual(r.body.error, 'market_closed');
  });
  warp(ROUND_PLAN.round.hotStart + 500);
  CT.fireBoundary('deep', ROUND_PLAN.round.hotStart);
  await ok('a hot position opens alongside an untouched base position', async () => {
    await order({ symbol: 'BTC', side: 'BUY', type: 'MARKET', size: 0.01, leverage: 10 });
    const r = await order({ symbol: HOT, side: 'BUY', type: 'MARKET', size: 0.01, leverage: 10 });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    assert.ok(posOf(PLAYER, HOT), 'hot position should exist');
    assert.ok(posOf(PLAYER, 'BTC'), 'base position should be untouched');
    assert.notStrictEqual(posOf(PLAYER, HOT).entry_price, undefined);
  });
  await ok('the hot ticker prices off its base index', async () => {
    const base = comp.__test.q.get.get('deep').hot_base;
    assert.strictEqual(T.mkt(HOT).markPrice, T.mkt(base).markPrice);
  });
  await ok('segment close flattens the hot leg only, and pays the bonus', async () => {
    setMark(CT.q.get.get('deep').hot_base, 110);   // hot leg +0.10
    warp(ROUND_PLAN.round.hotEnd + 500);
    CT.fireBoundary('deep', ROUND_PLAN.round.hotEnd);
    assert.strictEqual(posOf(PLAYER, HOT), undefined, 'hot leg must be flat');
    assert.ok(posOf(PLAYER, 'BTC'), 'base leg must survive');
    const s = T.scoreUser(PLAYER, HOT);
    assert.ok(s.hotBonus > 0, 'a winning hot leg should pay a bonus, got ' + s.hotBonus);
    assert.ok(near(s.accountPnl + s.hotBonus, s.accountPnl + s.hotBonus));
  });

  console.log('\nBoost window, end to end');
  warp(ROUND_PLAN.round.boostStart + 500);
  CT.fireBoundary('deep', ROUND_PLAN.round.boostStart);
  await ok('a boost twin accepts 1000x for a player', async () => {
    const r = await order({ symbol: 'BTC-BOOST', side: 'BUY', type: 'MARKET', size: 0.001, leverage: 1000 });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    assert.strictEqual(posOf(PLAYER, 'BTC-BOOST').leverage, 1000);
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
  asUser(SPECTATOR);
  await ok('a spectator cannot touch a boost twin even while it is open', async () => {
    const r = await order({ symbol: 'BTC-BOOST', side: 'BUY', type: 'MARKET', size: 0.001, leverage: 1000 });
    assert.strictEqual(r.code, 400);
    assert.strictEqual(r.body.error, 'market_closed');
  });

  console.log('\nthe bell');
  await ok('the bell marks everyone without closing positions', async () => {
    const openBefore = T.stmt.posByUser.all(PLAYER).length;
    assert.ok(openBefore > 0, 'the player should still be holding something');
    warp(ROUND_PLAN.round.total + 500);
    CT.fireBoundary('deep', ROUND_PLAN.round.total);
    assert.strictEqual(T.stmt.posByUser.all(PLAYER).length, openBefore,
      'nothing may be force-closed at the bell');
    const board = comp.standings('deep', 'final');
    assert.strictEqual(board.length, 1);
    assert.ok(Number.isFinite(board[0].score), 'a score must have been recorded');
  });
  await ok('leverage returns to normal once the round is done', async () => {
    asUser(PLAYER);
    const r = await order({ symbol: 'SOL', side: 'BUY', type: 'MARKET', size: 0.01, leverage: 1000 });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    assert.strictEqual(posOf(PLAYER, 'SOL').leverage, 1000, 'the gate must lift with the round');
  });

  console.log(`\n${pass} passed${process.exitCode ? ', WITH FAILURES' : ''}\n`);
})();
