/* Sandbox test for the competition round clock, phase gate and Hot draw.
 *
 *   PAPER_DB=$(mktemp -u --suffix=.db) node test-competition.js
 *
 * Phase arithmetic is pure, so most of this asserts on computed boundaries
 * rather than sleeping. The scheduler is exercised by driving fireBoundary
 * directly, which is what a real timer would call.
 */
const assert = require('assert');

process.env.PHOENIX_SNAPSHOT_FILE = '/nonexistent/markets-snapshot.json';
if (!process.env.PAPER_DB || process.env.PAPER_DB.startsWith('/opt/')) {
  console.error('refusing to run: set PAPER_DB to a throwaway path first');
  process.exit(2);
}

const comp = require('./competition.js');
const P = require('./paper.js');
const T = P.__test;
const { ROUND_PLAN, phaseAt, boundariesOf, verifyDraw, levCapFor } = comp;
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
const MIN = 60_000;
const at = (kind, mins) => phaseAt(kind, mins * MIN).phase;

// a live BTC + SOL so aliases can actually open
const now = Date.now();
for (const s of ['BTC', 'SOL', 'ETH', 'BNB', 'XRP']) {
  T.live.map.set(s, { markPrice: 100, pythPrice: 100, pythAtMs: now, pythBasis: 0, lastUpdatedMs: now, indexHalt: false });
  // a healthy index: two fresh components plus recorded history, which is
  // what stage pricing and strict checkpoints now require
  T.compUpdate(s, 'usdt', 100, now); T.compUpdate(s, 'usd', 100, now);
  // a short trail so any boundary instant has a mark at or before it
  for (let back = 20_000; back >= 0; back -= 2_000) T.recordMark(s, 100, now - back, 2);
}
const logs = [];
let _ep = 1;
comp.wire({
  openAlias: T.openAlias, closeAlias: T.closeAlias, log: (m) => logs.push(m),
  // this suite exercises the clock and the draw, not account state
  prepareSeat: () => ({ epoch: (_ep += 1), startBalance: 10, stage: true }),
  seatState: () => ({ epoch: _ep, startBalance: 10, stage: true }),
});
/* startRound now refuses an empty roster, because a round with no players
   cannot be scored and should never reach a stage. Every round here gets a
   nominal seat. */
const SEAT = [{ userId: 4242, displayName: 'Test', seat: 0 }];

(async () => {
  console.log('\nphase plan, 30 minute round');
  await ok('opens in First Five and ends at the bell', () => {
    assert.strictEqual(at('round', 0), 'firstFive');
    assert.strictEqual(at('round', 4.9), 'firstFive');
    assert.strictEqual(at('round', 30), 'done');
  });
  await ok('First Five closes at 5:00 exactly', () => {
    assert.strictEqual(at('round', 5), 'open');
  });
  await ok('reveal sits before the Hot window, not on it', () => {
    assert.strictEqual(at('round', 11.7), 'open');
    assert.strictEqual(at('round', 11.8), 'reveal');
    assert.strictEqual(at('round', 12), 'hot');
  });
  await ok('Hot runs 12:00 to 16:00', () => {
    assert.strictEqual(at('round', 15.9), 'hot');
    assert.strictEqual(at('round', 16), 'open');
  });
  await ok('Boost is the last three minutes', () => {
    assert.strictEqual(at('round', 26.9), 'open');
    assert.strictEqual(at('round', 27), 'boost');
    assert.strictEqual(at('round', 29.9), 'boost');
  });

  console.log('\nphase plan, 20 minute final');
  await ok('final compresses the same five beats', () => {
    assert.strictEqual(at('final', 0), 'firstFive');
    assert.strictEqual(at('final', 4), 'open');
    assert.strictEqual(at('final', 7.8), 'reveal');
    assert.strictEqual(at('final', 8), 'hot');
    assert.strictEqual(at('final', 11), 'open');
    assert.strictEqual(at('final', 17), 'boost');
    assert.strictEqual(at('final', 20), 'done');
  });
  await ok('every boundary is inside the round and ordered', () => {
    for (const kind of ['round', 'final']) {
      const b = boundariesOf(kind);
      assert.deepStrictEqual(b, [...b].sort((x, y) => x - y), kind + ' boundaries out of order');
      assert.ok(b[b.length - 1] === ROUND_PLAN[kind].total, kind + ' must end on the bell');
    }
  });

  console.log('\nverifiable draw');
  await ok('commitment is published before the seed exists publicly', () => {
    const r = comp.createRound({ id: 't-commit', candidates: ['BTC', 'SOL', 'ETH'], players: SEAT });
    assert.ok(r.draw_commit && r.draw_commit.length === 64, 'commit should be a sha256 hex');
    assert.strictEqual(r.draw_seed, null, 'seed must not be stored before the draw');
    assert.strictEqual(r.hot_base, null);
  });
  await ok('drawing reveals a seed that reproduces the result', () => {
    comp.startRound('t-commit');
    fireAt('t-commit', ROUND_PLAN.round.reveal);
    const r = CT.q.get.get('t-commit');
    assert.ok(r.hot_base, 'a market should have been drawn');
    const v = verifyDraw(r);
    assert.strictEqual(v.ok, true, 'draw must verify: ' + v.reason);
    assert.strictEqual(v.market, r.hot_base);
  });
  await ok('a tampered result fails verification', () => {
    const r = { ...CT.q.get.get('t-commit') };
    r.hot_base = r.hot_base === 'BTC' ? 'SOL' : 'BTC';
    assert.strictEqual(verifyDraw(r).ok, false, 'swapping the winner must be detectable');
  });
  await ok('a swapped candidate list fails verification', () => {
    const r = { ...CT.q.get.get('t-commit'), hot_candidates: JSON.stringify(['BTC', 'SOL', 'DOGE']) };
    assert.strictEqual(verifyDraw(r).ok, false, 'changing the candidates must break the commitment');
  });
  await ok('the draw is uniform over the candidates', () => {
    // not a fairness proof, a smoke test that it is not pinned to one index
    const seen = new Set();
    for (let i = 0; i < 200; i++) seen.add(CT.drawIndex(String(i), 3));
    assert.deepStrictEqual([...seen].sort(), [0, 1, 2]);
  });

  console.log('\nsegment wiring');
  await ok('Hot opens and closes its ticker on the boundaries', () => {
    const r = CT.q.get.get('t-commit');
    fireAt('t-commit', ROUND_PLAN.round.hotStart);
    assert.strictEqual(T.aliasOpen(r.hot_base + '-HOT'), true, 'hot ticker should be tradable');
    fireAt('t-commit', ROUND_PLAN.round.hotEnd);
    assert.strictEqual(T.aliasOpen(r.hot_base + '-HOT'), false, 'hot ticker should close');
  });
  await ok('Boost opens a twin on every major at once', () => {
    fireAt('t-commit', ROUND_PLAN.round.boostStart);
    for (const b of comp.__test.q ? ['BTC', 'ETH', 'XRP', 'SOL'] : []) {
      assert.strictEqual(T.aliasOpen(b + '-BOOST'), true, b + '-BOOST should be open');
    }
    // BNB is excluded on purpose: single-source index, unsafe at 1000x
    assert.strictEqual(T.aliasOpen('BNB-BOOST'), false, 'BNB must not be a Boost market');
  });
  await ok('the bell closes every segment and marks the round done', () => {
    fireAt('t-commit', ROUND_PLAN.round.total);
    assert.strictEqual(CT.q.get.get('t-commit').status, 'done');
    for (const b of ['BTC', 'ETH', 'XRP', 'SOL']) {
      assert.strictEqual(T.aliasOpen(b + '-BOOST'), false, b + '-BOOST should be closed at the bell');
    }
  });

  console.log('\nleverage gate');
  await ok('outside a round the engine cap is untouched', () => {
    assert.strictEqual(levCapFor('BTC', 1000, 1), 1000);
  });
  const PLAYER = 4242, SPECTATOR = 777;
  await ok('a player on ordinary tickers is held at the baseline', () => {
    const id = 't-lev';
    comp.createRound({ id, candidates: ['BTC', 'SOL'], players: [{ userId: PLAYER, displayName: 'Mia' }] });
    comp.startRound(id);
    assert.strictEqual(levCapFor('BTC', 1000, PLAYER), comp.COMP_BASE_LEV);
    assert.strictEqual(levCapFor('SOL', 1000, PLAYER), comp.COMP_BASE_LEV);
  });
  await ok('the PUBLIC is untouched while a show is on air', () => {
    // the whole point: a live round must not silently re-rule /ftpaper for
    // everyone else on the site
    assert.strictEqual(levCapFor('BTC', 1000, SPECTATOR), 1000);
    assert.strictEqual(levCapFor('SOL', 1000, SPECTATOR), 1000);
    assert.strictEqual(levCapFor('BTC', 1000, null), 1000, 'anonymous reads too');
  });
  await ok('a spectator cannot trade show tickers at all', () => {
    const r = CT.q.get.get('t-lev');
    const inBoost = r.started_at + ROUND_PLAN.round.boostStart + 1000;
    assert.strictEqual(levCapFor('BTC-BOOST', 1000, SPECTATOR, inBoost), 0);
    assert.strictEqual(levCapFor('BTC-HOT', 1000, SPECTATOR, inBoost), 0);
  });
  await ok('a boost twin is refused outside the Boost window', () => {
    assert.strictEqual(levCapFor('BTC-BOOST', 1000, PLAYER), 0, 'must be untradable, not merely capped');
  });
  await ok('a boost twin reaches the engine cap inside the Boost window', () => {
    const r = CT.q.get.get('t-lev');
    const inBoost = r.started_at + ROUND_PLAN.round.boostStart + 1000;
    assert.strictEqual(levCapFor('BTC-BOOST', 1000, PLAYER, inBoost), 1000);
    assert.strictEqual(levCapFor('BTC', 1000, PLAYER, inBoost), comp.COMP_BASE_LEV,
      'the base ticker stays capped even during Boost');
  });
  comp.abortRound('t-lev', { force: true });

  console.log('\nrestart safety');
  await ok('a boundary fires only once', () => {
    const id = 't-once';
    comp.createRound({ id, candidates: ['BTC', 'SOL'], players: SEAT });
    comp.startRound(id);
    fireAt(id, ROUND_PLAN.round.reveal);
    const first = CT.q.get.get(id).draw_at;
    fireAt(id, ROUND_PLAN.round.reveal);
    assert.strictEqual(CT.q.get.get(id).draw_at, first, 're-firing must not redraw');
    comp.abortRound(id, { force: true });
  });
  await ok('aborting closes segments and stops the round', () => {
    const id = 't-abort';
    comp.createRound({ id, candidates: ['BTC', 'SOL'], players: SEAT });
    comp.startRound(id);
    fireAt(id, ROUND_PLAN.round.reveal);
    fireAt(id, ROUND_PLAN.round.hotStart);
    const hot = CT.q.get.get(id).hot_base + '-HOT';
    assert.strictEqual(T.aliasOpen(hot), true);
    comp.abortRound(id, { force: true });
    assert.strictEqual(T.aliasOpen(hot), false);
    assert.strictEqual(CT.q.get.get(id).status, 'aborted');
  });
  await ok('a blocked round refuses to open Hot on the backup', () => {
    const id = 't-nofall';
    comp.createRound({ id, candidates: ['BTC', 'SOL'], backup: 'ETH', players: SEAT });
    comp.startRound(id);
    CT._seeds.delete(id);
    fireAt(id, ROUND_PLAN.round.reveal);      // blocks
    fireAt(id, ROUND_PLAN.round.hotStart);    // must NOT fall through to ETH
    assert.strictEqual(T.aliasOpen('ETH-HOT'), false, 'an unproven Hot Market must not open');
    assert.strictEqual(T.aliasOpen('BTC-HOT'), false);
    comp.abortRound(id, { force: true });
  });
  await ok('drawing without a committed seed fails loudly', () => {
    const id = 't-noseed';
    comp.createRound({ id, candidates: ['BTC', 'SOL'], players: SEAT });
    comp.startRound(id);

    CT._seeds.delete(id);            // simulate a restart between arming and the draw
    fireAt(id, ROUND_PLAN.round.reveal);
    const after = CT.q.get.get(id);
    assert.strictEqual(after.hot_base, null, 'must not draw from an uncommitted seed');
    assert.match(after.blocked_reason || '', /seed missing/, 'the round must be blocked, not merely logged');
    assert.strictEqual(CT.q.bGet.get(id, ROUND_PLAN.round.reveal).status, 'failed');
    comp.abortRound(id, { force: true });
  });

  console.log(`\n${pass} passed${process.exitCode ? ', WITH FAILURES' : ''}`);
  /* A suite that prints its summary before its cases have run is not a
     suite. Assert the count so a future promise-returning case cannot be
     silently dropped again. */
  if (pass + fails !== 25) {
    console.log(`  FAIL only ${pass + fails}/25 cases ran`);
    process.exitCode = 1;
  }
})();
