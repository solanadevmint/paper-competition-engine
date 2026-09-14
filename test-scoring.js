/* Sandbox test for base account scoring and immutable final checkpoints.
 * Current two-Hot economic-delta scoring is exercised in test-two-hot.js.
 *
 *   PAPER_DB=$(mktemp -u --suffix=.db) node test-scoring.js
 */
const assert = require('assert');

process.env.PHOENIX_SNAPSHOT_FILE = '/nonexistent/markets-snapshot.json';
if (!process.env.PAPER_DB || process.env.PAPER_DB.startsWith('/opt/')) {
  console.error('refusing to run: set PAPER_DB to a throwaway path first');
  process.exit(2);
}

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
const fireAt = (id, at) => {
  CT.db.prepare('UPDATE paper_rounds SET started_at = ? WHERE id = ?')
    .run(Date.now() - at - 200, id);
  CT.fireBoundary(id, at);
};
const { ROUND_PLAN } = comp;

let pass = 0;
let fails = 0;
/* Awaits the body. A synchronous helper silently dropped promise-returning
   tests: the suite printed passes and then died on an unhandled rejection
   with a non-zero exit, so counting "ok" lines reported green on a failing
   suite. Every caller is awaited now. */
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
const ok = async (name, fn) => {
  _inflight = { name, at: Date.now() };
  try { await fn(); console.log('  ok   ' + name); pass++; }
  catch (e) { fails++; console.log('  FAIL ' + name + '\n       ' + e.message); process.exitCode = 1; }
  finally { _inflight = null; }
};
process.on('unhandledRejection', (e) => {
  console.log('  FAIL unhandled rejection\n       ' + (e && e.message ? e.message : e));
  process.exitCode = 1;
});
const near = (a, b, eps = 1e-6) => Math.abs(a - b) < eps;

const now = Date.now();
const setMarkRaw = (sym, px) => T.live.map.set(sym, {
  markPrice: px, pythPrice: px, pythAtMs: Date.now(), pythBasis: 0,
  lastUpdatedMs: Date.now(), indexHalt: false,
});

/* Fixtures must look like a HEALTHY index, not just a populated map. Stage
   pricing now requires a fresh composite from at least two agreeing
   components while a round is live, and checkpoints price strictly from
   recorded history, so a fixture that only wrote live.map was pretending to
   be a market it was not. */
function feedMark(sym, px, t = Date.now()) {
  T.compUpdate(sym, 'usdt', px, t);
  T.compUpdate(sym, 'usd', px, t);
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
for (const s of ['BTC', 'SOL', 'ETH', 'BNB', 'XRP']) { setMarkRaw(s, 100); feedMark(s, 100); }
T.mktCfg.set('BTC', { tiers: [], maxLev: 40, lotSize: null, takerBps: 3.5, makerBps: 0.5, maintBps: 50, cancelBps: 0, maxLiqSize: null, status: 'active', isolatedOnly: false });
T.mktCfg.set('SOL', { ...T.mktCfg.get('BTC') });

const logs = [];
comp.wire({
  marketReady: () => true, // Explicit fixture price policy.
  openAlias: T.openAlias, closeAlias: T.closeAlias, scoreUser: T.scoreUser,
  scoreProofFor: T.scoreProofFor,
  prepareSeat: T.prepareSeat, seatState: T.seatState, markSetFor: T.markSetFor,
  equityOf: (uid) => { const a = T.stmt.acctGet.get(uid); return a ? T.accountRisk(uid, a).equityTotal : NaN; },
  log: (m) => logs.push(m),
});
/* These rounds start with prepare:false. The suite writes fills by hand to
   set up each scoring case, and a real reset would wipe exactly the state
   under test. Preparation itself is covered in test-comp-api. */
const start = (id) => comp.startRound(id, { prepare: false });

// ── two players on a stage-mode account ──────────────────────────────────
const MIA = 5001, ALEX = 5002;
const START = 10;   // stage bankroll; the UI scales it for display
function mkPlayer(uid) {
  T.db.prepare('INSERT OR IGNORE INTO users (id) VALUES (?)').run(uid);
  T.stmt.acctIns.run(uid, now, now);
  T.db.prepare('UPDATE paper_accounts SET heat = 1, start_balance = ?, balance = ? WHERE user_id = ?').run(START, START, uid);
}
mkPlayer(MIA); mkPlayer(ALEX);
const epochOf = (uid) => T.stmt.acctGet.get(uid).epoch;
// a realised fill, written the way applyFill would
function realisedFill(uid, symbol, pnl) {
  const acct = T.stmt.acctGet.get(uid);
  T.stmt.fillIns.run(uid, acct.epoch, symbol, 'SELL', 'MARKET', 100, 1, 100, 0, pnl, null, Date.now(), 0);
  T.db.prepare('UPDATE paper_accounts SET balance = balance + ? WHERE user_id = ?').run(pnl, uid);
}

(async () => {
  console.log('\naccount PnL');
  await ok('a flat account with no trades scores zero', () => {
    const s = T.scoreUser(MIA, null);
    assert.ok(near(s.accountPnl, 0), 'expected 0, got ' + s.accountPnl);
    assert.ok(near(s.equity, START));
  });
  await ok('realised profit shows up in account PnL', () => {
    realisedFill(MIA, 'BTC', 4);
    const s = T.scoreUser(MIA, null);
    assert.ok(near(s.accountPnl, 4), 'expected 4, got ' + s.accountPnl);
    assert.ok(near(s.realized, 4));
  });
  await ok('an open position marks to the index', () => {
    // 1 unit long BTC entered at 100, mark moves to 103
    T.stmt.posIns.run(ALEX, 'BTC', epochOf(ALEX), 'LONG', 1, 100, 10, Date.now(), 100, Date.now(), Date.now(), 'cross', 0);
    setMarkRaw('BTC', 103); feedMark('BTC', 103);
    const s = T.scoreUser(ALEX, null);
    assert.ok(near(s.accountPnl, 3), 'unrealised should count: expected 3, got ' + s.accountPnl);
    assert.ok(near(s.realized, 0), 'and it is not realised');
    setMarkRaw('BTC', 100); feedMark('BTC', 100);
  });

  console.log('\nfrozen checkpoints');
  await ok('a snapshot writes one row per player and ranks them', () => {
    comp.createRound({
      id: 'r-score', candidates: ['SOL', 'BTC', 'ETH'],
      players: [{ userId: MIA, displayName: 'Mia' }, { userId: ALEX, displayName: 'Alex' }],
    });
    start('r-score');
    comp.snapshot('r-score', 'final');
    const board = comp.standings('r-score', 'final');
    assert.strictEqual(board.length, 2);
    assert.strictEqual(board[0].rank, 1);
    assert.strictEqual(board[0].display_name, 'Mia', 'Mia should lead Alex');
  });
  await ok('a frozen checkpoint does not move when the market does', () => {
    const before = comp.standings('r-score', 'final')[0].score;
    setMarkRaw('BTC', 140); feedMark('BTC', 140);            // Alex is long BTC; a published result must not change
    const after = comp.standings('r-score', 'final')[0].score;
    assert.ok(near(before, after), `checkpoint moved: ${before} -> ${after}`);
    setMarkRaw('BTC', 100); feedMark('BTC', 100);
  });
  await ok('re-running a checkpoint cannot overwrite it', () => {
    const before = comp.standings('r-score', 'final').map((r) => r.score);
    realisedFill(MIA, 'BTC', 50);   // big move after the checkpoint
    comp.snapshot('r-score', 'final');
    const after = comp.standings('r-score', 'final').map((r) => r.score);
    assert.deepStrictEqual(after, before, 'the first write must win');
  });

  console.log('\nround ownership');
  await ok('a second round cannot start while one is running', () => {
    comp.createRound({ id: 'r-clash', candidates: ['SOL', 'BTC', 'ETH'], players: [{ userId: MIA }] });
    assert.throws(() => start('r-clash'), /already running/);
    comp.abortRound('r-clash', { force: true });
    comp.abortRound('r-score', { force: true });
  });

  console.log('\nthe bell');
  await ok('the bell freezes a final checkpoint without closing positions', () => {
    comp.createRound({
      id: 'r-bell', candidates: ['SOL', 'BTC', 'ETH'],
      players: [{ userId: MIA, displayName: 'Mia' }, { userId: ALEX, displayName: 'Alex' }],
    });
    start('r-bell');
    /* Wind the clock to the bell. Firing it 30 minutes early puts the boundary
       instant far ahead of any recorded mark, and strict pricing rightly
       refuses to settle from a price that did not exist yet. */
    CT.db.prepare('UPDATE paper_rounds SET started_at = ? WHERE id = ?')
      .run(Date.now() - (ROUND_PLAN.round.total + 500), 'r-bell');
    const openBefore = T.stmt.posByUser.all(ALEX).length;
    fireAt('r-bell', ROUND_PLAN.round.total);
    assert.strictEqual(CT.q.get.get('r-bell').status, 'done');
    const board = comp.standings('r-bell', 'final');
    assert.strictEqual(board.length, 2, 'every player must be marked');
    assert.strictEqual(T.stmt.posByUser.all(ALEX).length, openBefore,
      'positions are marked where they stand, never force-closed');
  });

  console.log(`\n${pass} passed${process.exitCode ? ', WITH FAILURES' : ''}`);
  /* A suite that prints its summary before its cases have run is not a
     suite. Assert the count so a future promise-returning case cannot be
     silently dropped again. */
  if (pass + fails !== 8) {
    console.log(`  FAIL only ${pass + fails}/8 cases ran`);
    process.exitCode = 1;
  }
})();
