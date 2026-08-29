/* Sandbox test for competition alias tickers (SOL-HOT / BTC-BOOST).
 *
 * Runs against a throwaway SQLite file, never the live database: point
 * PAPER_DB at a temp path BEFORE requiring the engine. Requiring paper.js
 * starts no sockets and no timers, so nothing here touches the network or
 * the running engine.
 *
 *   PAPER_DB=$(mktemp -u) node test-alias.js
 */
const assert = require('assert');

// Point the market snapshot at nothing. It defaults to the LIVE perp.so data
// directory, and a test that silently reads production marks is not a test:
// the "no fresh mark" case passed only because real SOL data was on disk.
process.env.PHOENIX_SNAPSHOT_FILE = '/nonexistent/markets-snapshot.json';

if (!process.env.PAPER_DB || process.env.PAPER_DB.startsWith('/opt/')) {
  console.error('refusing to run: set PAPER_DB to a throwaway path first');
  process.exit(2);
}

const P = require('./paper.js');
const T = P.__test;
const { baseOf, aliasKind, aliasOpen, openAlias, closeAlias, cfgOf, stageLevCap, mkt, stmt, live, mktCfg } = T;

let pass = 0;
const ok = (name, fn) => {
  try { fn(); console.log('  ok   ' + name); pass++; }
  catch (e) { console.log('  FAIL ' + name + '\n       ' + e.message); process.exitCode = 1; }
};

// ── a synthetic BTC market, fresh enough to price against ────────────────
const now = Date.now();
live.map.set('BTC', {
  markPrice: 100, pythPrice: 100, pythAtMs: now, pythBasis: 0,
  lastUpdatedMs: now, indexHalt: false, currentFundingRate: 0,
});
mktCfg.set('BTC', {
  tiers: [], maxLev: 40, lotSize: null, takerBps: 3.5, makerBps: 0.5,
  maintBps: 50, cancelBps: 0, maxLiqSize: null, status: 'active', isolatedOnly: false,
});

console.log('\nresolution');
ok('baseOf strips the suffix', () => {
  assert.strictEqual(baseOf('BTC-HOT'), 'BTC');
  assert.strictEqual(baseOf('BTC-BOOST'), 'BTC');
  assert.strictEqual(baseOf('BTC'), 'BTC');
});
ok('aliasKind identifies the segment', () => {
  assert.strictEqual(aliasKind('BTC-HOT'), 'HOT');
  assert.strictEqual(aliasKind('BTC-BOOST'), 'BOOST');
  assert.strictEqual(aliasKind('BTC'), null);
});
ok('a hyphen alone is not an alias', () => {
  // real symbols may contain characters we must not mangle
  assert.strictEqual(baseOf('BTC-PERP'), 'BTC-PERP');
  assert.strictEqual(aliasKind('BTC-PERP'), null);
});
ok('an alias prices off its base index', () => {
  assert.strictEqual(mkt('BTC-HOT').markPrice, 100);
  assert.strictEqual(mkt('BTC-BOOST').markPrice, 100);
  assert.strictEqual(mkt('BTC-HOT'), mkt('BTC'));
});
ok('an alias inherits base fees and status', () => {
  assert.strictEqual(cfgOf('BTC-HOT').takerBps, 3.5);
  assert.strictEqual(cfgOf('BTC-HOT').status, 'active');
});
ok('an alias inherits the base leverage cap', () => {
  assert.strictEqual(stageLevCap('BTC-BOOST'), stageLevCap('BTC'));
  assert.strictEqual(stageLevCap('BTC-BOOST'), 1000);
});

console.log('\nboost isolation');
ok('BOOST twins are isolated-only', () => {
  assert.strictEqual(cfgOf('BTC-BOOST').isolatedOnly, true);
});
ok('HOT twins and the base are not forced isolated', () => {
  assert.strictEqual(cfgOf('BTC-HOT').isolatedOnly, false);
  assert.strictEqual(cfgOf('BTC').isolatedOnly, false);
});

console.log('\nsegment gating');
ok('aliases are closed by default', () => {
  assert.strictEqual(aliasOpen('BTC-HOT'), false);
});
ok('open then close flips the gate', () => {
  openAlias('BTC-HOT', 'r1');
  assert.strictEqual(aliasOpen('BTC-HOT'), true);
  closeAlias('BTC-HOT');
  assert.strictEqual(aliasOpen('BTC-HOT'), false);
});
ok('opening rejects a base that is not indexed', () => {
  assert.throws(() => openAlias('NOPE-HOT'), /not indexed/);
});
ok('opening rejects a base with no fresh mark', () => {
  // SOL is indexed but has no synthetic mark in this test
  assert.throws(() => openAlias('SOL-HOT'), /no fresh mark/);
});
ok('opening rejects a non-alias', () => {
  assert.throws(() => openAlias('BTC'), /not an alias/);
});

console.log('\nsegment close flattens');
ok('closeAlias closes every position on the ticker and leaves the base alone', () => {
  const uid = 999001;   // paper_accounts.user_id is INTEGER
  // paper_accounts.user_id has a FK onto users(id): the account cannot exist
  // without the identity row the auth shim would normally have created.
  T.db.prepare('INSERT OR IGNORE INTO users (id) VALUES (?)').run(uid);
  stmt.acctIns.run(uid, now, now);
  // stage mode: fills execute at the mark, which is what the competition runs
  T.db.prepare('UPDATE paper_accounts SET heat = 1, start_balance = 10 WHERE user_id = ?').run(uid);
  const acct = stmt.acctGet.get(uid);

  const ins = (sym) => stmt.posIns.run(
    uid, sym, acct.epoch, 'LONG', 1, 100, 10, now, 100, now, now, 'cross', 0
  );
  ins('BTC-HOT');
  ins('BTC');

  openAlias('BTC-HOT', 'r1');
  const res = closeAlias('BTC-HOT');

  assert.strictEqual(res.closed, 1, 'should have flattened one position');
  assert.strictEqual(stmt.posGet.get(uid, 'BTC-HOT'), undefined, 'hot position should be gone');
  assert.ok(stmt.posGet.get(uid, 'BTC'), 'base position must survive the segment close');
  assert.strictEqual(aliasOpen('BTC-HOT'), false, 'ticker must stop accepting orders');

  const fills = T.db.prepare("SELECT * FROM paper_fills WHERE user_id = ? AND symbol = 'BTC-HOT'").all(uid);
  assert.strictEqual(fills.length, 1, 'close should produce exactly one fill');
  assert.strictEqual(fills[0].kind, 'SEGMENT', 'fill must be tagged SEGMENT for the audit trail');
  assert.strictEqual(fills[0].side, 'SELL', 'closing a long is a sell');
});

console.log(`\n${pass} passed${process.exitCode ? ', WITH FAILURES' : ''}\n`);
