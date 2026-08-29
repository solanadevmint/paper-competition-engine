/* Live-data integration test.
 *
 *   PAPER_DB=$(mktemp -u --suffix=.db) node test-engine-live.js
 *
 * The other suites feed the engine synthetic marks at a flat price of 100.
 * That is convenient and it hides things: at 100, every price is above 1, of
 * similar magnitude, and lot sizes never bite. Real markets are not like
 * that. BTC is near 78,000, XRP near 1.4, DOGE near 0.085, and the engine
 * takes a different rounding path below 1.
 *
 * This suite reads the REAL market snapshot and the REAL exchange config, so
 * orders are sized and priced the way they will be on the day. It is
 * read-only against production: it takes prices and market parameters, and
 * writes only into a throwaway database.
 *
 * Prices move between runs, so nothing here asserts on a specific number.
 * It asserts on invariants that must hold at any price.
 */
const assert = require('assert');
const fs = require('fs');
const https = require('https');

const SNAP = '/var/www/phoenix-showdown/data/markets-snapshot.json';
process.env.PHOENIX_SNAPSHOT_FILE = SNAP;
if (!process.env.PAPER_DB || process.env.PAPER_DB.startsWith('/opt/')) {
  console.error('refusing to run: set PAPER_DB to a throwaway path first');
  process.exit(2);
}
if (!fs.existsSync(SNAP)) {
  console.error('no live snapshot at ' + SNAP + ' — run this on a box that has one');
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
const near = (a, b, eps) => Math.abs(a - b) <= eps;

// ── real exchange config ─────────────────────────────────────────────────
const apiGet = (path) => new Promise((resolve, reject) => {
  https.get('https://perp-api.phoenix.trade' + path, { timeout: 15000 }, (res) => {
    let b = ''; res.on('data', (c) => { b += c; });
    res.on('end', () => {
      if (res.statusCode !== 200) return reject(new Error('http ' + res.statusCode));
      try { resolve(JSON.parse(b)); } catch (e) { reject(e); }
    });
  }).on('error', reject).on('timeout', function () { this.destroy(new Error('timeout')); });
});

// ── auth stub ────────────────────────────────────────────────────────────
let CURRENT_USER = null;
auth.validateSession = async () => CURRENT_USER;
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
  T.writeRate.clear();
  return res;
};

const USER = 9301;
function mkAccount(uid, bal = 10) {
  T.db.prepare('INSERT OR IGNORE INTO users (id) VALUES (?)').run(uid);
  T.stmt.acctIns.run(uid, Date.now(), Date.now());
  T.db.prepare('UPDATE paper_accounts SET heat = 1, start_balance = ?, balance = ? WHERE user_id = ?')
    .run(bal, bal, uid);
}
const posOf = (uid, s) => T.stmt.posGet.get(uid, s);
const balOf = (uid) => T.stmt.acctGet.get(uid).balance;

(async () => {
  // Load the real exchange config the same way the engine does at boot.
  P.init({ apiGet: async (p) => apiGet(p), log: () => {} });
  await new Promise((r) => setTimeout(r, 3000));   // let refreshExchange land
  const snap = JSON.parse(fs.readFileSync(SNAP, 'utf8'));
  const markets = snap.markets || {};

  console.log('\nlive market data');
  await ok('the snapshot is fresh and populated', () => {
    const age = (Date.now() - (snap.updatedAt || 0)) / 1000;
    assert.ok(Object.keys(markets).length > 20, 'expected a real market list');
    assert.ok(age < 300, `snapshot is ${Math.round(age)}s old`);
  });
  await ok('real exchange config loaded for the majors', () => {
    assert.ok(T.mktCfg.size > 20, 'expected real market config, got ' + T.mktCfg.size);
    for (const s of ['BTC', 'SOL']) {
      const c = T.mktCfg.get(s);
      assert.ok(c, s + ' missing from config');
      assert.ok(c.maintBps > 0, s + ' has no maintenance margin');
    }
  });

  // pick real symbols spanning the magnitude range the engine must handle
  const priced = (s) => Number(markets[s]?.markPrice) > 0;
  const BIG = ['BTC', 'ETH'].find(priced);
  const SUB1 = ['DOGE', 'XRP', 'ADA', 'XLM'].filter(priced)
    .find((s) => Number(markets[s].markPrice) < 2);
  console.log(`         using ${BIG} @ ${markets[BIG]?.markPrice}` +
    (SUB1 ? ` and ${SUB1} @ ${markets[SUB1].markPrice}` : ' (no sub-$2 market found)'));

  mkAccount(USER);
  asUser(USER);

  console.log('\norders at real prices');
  await ok('a market order fills at a real four-figure price', async () => {
    const px = Number(markets[BIG].markPrice);
    const r = await order({ symbol: BIG, side: 'BUY', type: 'MARKET', notionalUsd: 1, leverage: 10 });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    const p = posOf(USER, BIG);
    assert.ok(p, 'position should exist');
    // entry must be within a sane band of the snapshot price, not a default
    assert.ok(near(p.entry_price, px, px * 0.05),
      `entry ${p.entry_price} is nowhere near the index ${px}`);
    assert.ok(p.size > 0 && Number.isFinite(p.size), 'size must be a real number: ' + p.size);
  });
  await ok('notional sizing survives a four-figure price', () => {
    const p = posOf(USER, BIG);
    const notional = p.size * p.entry_price;
    assert.ok(near(notional, 1, 0.25), `asked for $1 notional, got ${notional}`);
  });
  await ok('a round trip at an unchanged index is close to flat', async () => {
    const before = balOf(USER);
    const p = posOf(USER, BIG);
    const r = await order({ symbol: BIG, side: 'SELL', type: 'MARKET', size: p.size, leverage: 10 });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    assert.strictEqual(posOf(USER, BIG), undefined, 'should be flat');
    // the index moves between the two fills; tolerance is a fraction of the
    // $1 notional, which is what "no fees, no slippage in stage" should mean
    assert.ok(near(balOf(USER), before, 0.02),
      `round trip moved the balance by ${(balOf(USER) - before).toFixed(6)}`);
  });

  if (SUB1) {
    console.log('\nsub-dollar prices (a different rounding path)');
    await ok('a sub-$1 market opens and prices sanely', async () => {
      const px = Number(markets[SUB1].markPrice);
      const r = await order({ symbol: SUB1, side: 'BUY', type: 'MARKET', notionalUsd: 1, leverage: 10 });
      assert.strictEqual(r.code, 200, JSON.stringify(r.body));
      const p = posOf(USER, SUB1);
      assert.ok(near(p.entry_price, px, px * 0.05),
        `entry ${p.entry_price} vs index ${px}`);
      assert.ok(p.entry_price > 0, 'a sub-$1 price must not round to zero');
    });
    await ok('sub-$1 round trip is close to flat too', async () => {
      const before = balOf(USER);
      const p = posOf(USER, SUB1);
      await order({ symbol: SUB1, side: 'SELL', type: 'MARKET', size: p.size, leverage: 10 });
      assert.strictEqual(posOf(USER, SUB1), undefined);
      assert.ok(near(balOf(USER), before, 0.02),
        `moved ${(balOf(USER) - before).toFixed(6)}`);
    });
  }

  console.log('\ncompetition layer on live prices');
  comp.wire({ openAlias: T.openAlias, closeAlias: T.closeAlias, scoreUser: T.scoreUser, log: () => {} });
  comp.createRound({
    id: 'live', candidates: [BIG, 'SOL'],
    players: [{ userId: USER, displayName: 'Live', seat: 0 }],
  });
  comp.startRound('live');
  const warp = (ms) => CT.db.prepare('UPDATE paper_rounds SET started_at = ? WHERE id = ?')
    .run(Date.now() - ms, 'live');

  await ok('an alias tracks its base symbol at the live index', () => {
    warp(ROUND_PLAN.round.reveal + 500);
    CT.fireBoundary('live', ROUND_PLAN.round.reveal);
    const base = CT.q.get.get('live').hot_base;
    assert.strictEqual(T.mkt(base + '-HOT').markPrice, T.mkt(base).markPrice);
    assert.ok(Number(T.mkt(base + '-HOT').markPrice) > 0, 'alias must have a live price');
  });
  await ok('a hot position trades at the live index', async () => {
    warp(ROUND_PLAN.round.hotStart + 500);
    CT.fireBoundary('live', ROUND_PLAN.round.hotStart);
    const hot = CT.q.get.get('live').hot_base + '-HOT';
    const px = Number(markets[CT.q.get.get('live').hot_base].markPrice);
    const r = await order({ symbol: hot, side: 'BUY', type: 'MARKET', notionalUsd: 1, leverage: 10 });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    assert.ok(near(posOf(USER, hot).entry_price, px, px * 0.05));
  });
  await ok('1000x on a real price gives a believable liquidation distance', async () => {
    warp(ROUND_PLAN.round.boostStart + 500);
    CT.fireBoundary('live', ROUND_PLAN.round.boostStart);
    const tkr = BIG + '-BOOST';
    const r = await order({ symbol: tkr, side: 'BUY', type: 'MARKET', notionalUsd: 1, leverage: 1000 });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    const p = posOf(USER, tkr);
    assert.strictEqual(p.margin_mode, 'isolated', 'boost must isolate');
    const liq = T.liqEstimate(p, [p], balOf(USER), true);
    assert.ok(liq && liq > 0, 'a liquidation price must exist at 1000x');
    const distBps = ((p.entry_price - liq) / p.entry_price) * 1e4;
    // ~5bps is the documented distance at 1000x; allow a wide band, the point
    // is that it is single-digit bps and not something absurd
    assert.ok(distBps > 0 && distBps < 40,
      `liquidation is ${distBps.toFixed(2)}bps from entry, expected single digits`);
    console.log(`         1000x liq distance: ${distBps.toFixed(2)}bps ` +
      `(entry ${p.entry_price}, liq ${liq})`);
  });
  await ok('the bell marks live positions without closing them', async () => {
    const before = T.stmt.posByUser.all(USER).length;
    assert.ok(before > 0, 'should be holding something into the bell');
    warp(ROUND_PLAN.round.total + 500);
    CT.fireBoundary('live', ROUND_PLAN.round.total);
    assert.strictEqual(T.stmt.posByUser.all(USER).length, before);
    const board = comp.standings('live', 'final');
    assert.ok(Number.isFinite(board[0].score), 'a score must exist');
    assert.ok(Number.isFinite(board[0].equity) && board[0].equity > 0, 'equity must be real');
  });

  console.log(`\n${pass} passed${process.exitCode ? ', WITH FAILURES' : ''}\n`);
  process.exit(process.exitCode || 0);
})();
