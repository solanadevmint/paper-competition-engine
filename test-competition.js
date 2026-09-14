'use strict';

/* Current-format lifecycle tests. Historical First-Five/synthetic-Hot
 * behaviour is intentionally not executable here: every newly armed round is
 * the two-surprise-Hot format exercised below and in test-two-hot.js. */
const assert = require('assert');

process.env.PHOENIX_SNAPSHOT_FILE = '/nonexistent/competition-snapshot.json';
process.env.PAPER_ALLOW_UNPROVEN_MARKETS = '1';
if (!process.env.PAPER_DB || process.env.PAPER_DB.startsWith('/opt/')) {
  console.error('refusing to run: set PAPER_DB to a throwaway path first');
  process.exit(2);
}

const comp = require('./competition.js');
const P = require('./paper.js');
const T = P.__test;
const CT = comp.__test;
const MIN = 60_000;
let pass = 0;
let fail = 0;
const test = async (name, fn) => {
  try { await fn(); pass++; console.log('  ok   ' + name); }
  catch (e) { fail++; console.log('  FAIL ' + name + '\n       ' + (e.stack || e.message)); }
};

const UID = 4242;
CT.db.prepare('INSERT OR IGNORE INTO users (id) VALUES (?)').run(UID);
T.__ensureAccountRef()(UID);
function prime(sym, px = 100, at = Date.now()) {
  T.compUpdate(sym, 'usdt', px, at, at);
  T.compUpdate(sym, 'usd', px, at, at);
  T.recordMark(sym, px, at, 2, 0, 'usdt', at);
}
for (const sym of ['BTC', 'ETH', 'SOL', 'XRP']) prime(sym);

comp.wire({
  openAlias: T.openAlias,
  closeAlias: T.closeAlias,
  aliasOpen: T.aliasOpen,
  segmentResidue: (alias) => ({
    positions: T.stmt.posBySymbol.all(alias).length,
    orders: T.stmt.ordOpenBySymbol.all(alias).length,
  }),
  scoreUser: T.scoreUser,
  scoreProofFor: T.scoreProofFor,
  hotValueOf: T.hotValueOf,
  prepareSeat: T.prepareSeat,
  seatState: T.seatState,
  markSetFor: T.markSetFor,
  equityOf: (uid) => T.accountRisk(uid, T.stmt.acctGet.get(uid)).equityTotal,
  indexedSymbol: () => true,
  marketReady: () => true,
  marketReadyAt: () => true,
  marketReadyForBoost: () => true,
  marketAvailability: (_sym, _kind, at) => ({ ready: true, validUntil: Number(at) + 60_000 }),
  historicalAvailabilityAt: (_sym, _kind, at) => ({ ready: true, validUntil: Number(at) + 60_000 }),
  ensureClockHealth: () => true,
  pauseForSegment: () => {},
  pauseForBoundary: (at) => comp.pauseClockOpen(at),
  pauseForRestart: () => comp.pauseClockOpen(),
  boostLeverage: () => 500,
  boostLevCap: () => 500,
  marketReliability: () => ({ ratio: 1, samples: 100, spanMs: 600_000, longestGapMs: 0, unknownMs: 0 }),
  closeRoundPauses: () => {},
  onPhase: () => {},
  log: () => {},
});

const seat = [{ userId: UID, displayName: 'Test', seat: 0 }];
const create = (id, extra = {}) => comp.createRound({
  id, kind: extra.kind || 'round', speed: extra.speed || 1,
  candidates: ['BTC', 'ETH', 'SOL'], players: seat,
});
const abort = () => {
  const live = comp.currentRound();
  if (live) comp.abortRound(live.id, { force: true });
};

(async () => {
  console.log('\nphase plan');
  await test('rounds are the thirty-minute format and the final is the twenty-minute one', () => {
    const r = comp.ROUND_PLAN.round;
    assert.strictEqual(r.total, 30 * MIN);
    assert.strictEqual(r.buildEnd, 3 * MIN);
    assert.strictEqual(r.finalBuildStart, 22 * MIN);
    assert.strictEqual(r.boostStart, 27 * MIN);
    const f = comp.ROUND_PLAN.final;
    assert.strictEqual(f.total, 20 * MIN);
    assert.strictEqual(f.buildEnd, 2 * MIN);
    assert.strictEqual(f.finalBuildStart, 14 * MIN);
    assert.strictEqual(f.boostStart, 17 * MIN);
    // The trading beats keep their length; only the build stretches shrink.
    assert.strictEqual(f.hotDuration, r.hotDuration);
    assert.strictEqual(f.total - f.boostStart, r.total - r.boostStart);
  });

  await test('a sealed draw yields build, warning, Hot, final build and Boost phases', () => {
    const r = create('phase-shape');
    const p = comp.planOf(r);
    const d = CT.privateDrawOf(r);
    assert.strictEqual(comp.phaseAt(r, 0).phase, 'build');
    assert.strictEqual(comp.phaseAt(r, d.hot1.activation - p.hotWarning).phase, 'hotWarning');
    assert.strictEqual(comp.phaseAt(r, d.hot1.activation).phase, 'hot');
    assert.strictEqual(comp.phaseAt(r, d.hot1.activation + p.hotDuration).phase, 'build');
    assert.strictEqual(comp.phaseAt(r, p.finalBuildStart).phase, 'finalBuild');
    assert.strictEqual(comp.phaseAt(r, p.boostStart).phase, 'boost');
    assert.strictEqual(comp.phaseAt(r, p.total).phase, 'done');
    comp.abortRound(r.id, { force: true });
  });

  await test('all two-Hot boundaries are unique, ordered and end at the bell', () => {
    const r = create('boundary-shape');
    const b = comp.boundariesOf(r);
    assert.deepStrictEqual(b, [...new Set(b)].sort((a, z) => a - z));
    assert.strictEqual(b.at(-1), comp.planOf(r).total);
    assert.strictEqual(b.length >= 8, true);
    comp.abortRound(r.id, { force: true });
  });

  console.log('\ndraw and ordinary Hot execution');
  await test('arming locks a verifiable private envelope without publishing a seed', () => {
    const r = create('sealed-draw');
    assert.strictEqual(r.format_version, 2);
    assert.strictEqual(r.draw_commit.length, 64);
    assert.strictEqual(r.draw_seed, null);
    assert.strictEqual(CT.sealedDrawValid(r), true);
    assert.notStrictEqual(CT.privateDrawOf(r).hot1.asset, CT.privateDrawOf(r).hot2.asset);
    comp.abortRound(r.id, { force: true });
  });

  await test('v2 Hot opens on the ordinary asset and never creates a synthetic ticker', () => {
    const r = create('ordinary-hot');
    comp.startRound(r.id);
    CT.openHotV2(r.id, 1, Date.now());
    const active = CT.q.get.get(r.id).hot1_active_base;
    assert.ok(active);
    assert.strictEqual(T.aliasOpen(active + '-HOT'), false);
    assert.strictEqual(comp.levCapFor(active + '-HOT', 500, UID), 0);
    abort();
  });

  console.log('\nleverage and ownership');
  await test('players stay at base leverage until the persisted Boost gate opens', () => {
    const r = create('leverage-gate', { kind: 'rehearsal', speed: 30 });
    const live = comp.startRound(r.id);
    assert.strictEqual(comp.levCapFor('BTC', 500, UID), comp.COMP_BASE_LEV);
    assert.strictEqual(comp.levCapFor('BTC-BOOST', 500, UID), 0);
    const p = comp.planOf(live);
    for (const at of comp.boundariesOf(live)) {
      if (at <= p.boostStart) CT.q.bMark.run(r.id, at, 'succeeded', null, Date.now());
    }
    CT.db.prepare('UPDATE paper_rounds SET boost_opened = ?, started_at = ? WHERE id = ?')
      .run(JSON.stringify(['BTC']), Date.now() - p.boostStart - 1, r.id);
    T.openAlias('BTC-BOOST', r.id);
    assert.strictEqual(comp.levCapFor('BTC-BOOST', 500, UID), 500);
    assert.strictEqual(comp.levCapFor('BTC', 500, UID), comp.COMP_BASE_LEV);
    abort();
  });

  await test('a live round owns its seats and rejects a second start', () => {
    const r = create('owner-one');
    comp.startRound(r.id);
    const other = create('owner-two');
    assert.throws(() => comp.startRound(other.id), /already running/);
    comp.abortRound(other.id, { force: true });
    abort();
  });

  await test('active time freezes and resumes without consuming the phase', () => {
    const r = create('active-clock', { kind: 'rehearsal' });
    const started = comp.startRound(r.id);
    const t0 = started.started_at + 10_000;
    comp.pauseClockOpen(t0);
    assert.strictEqual(comp.activeElapsed(CT.q.get.get(r.id), t0 + 60_000), 10_000);
    comp.pauseClockClose(t0 + 60_000);
    assert.strictEqual(comp.activeElapsed(CT.q.get.get(r.id), t0 + 65_000), 15_000);
    abort();
  });

  await test('an unknown preassigned account is rejected before any round row exists', () => {
    assert.throws(() => comp.createRound({ id: 'ghost', candidates: ['BTC', 'ETH', 'SOL'],
      players: [{ userId: 987654321, seat: 0 }] }), /no paper account/);
    assert.strictEqual(CT.q.get.get('ghost'), undefined);
  });

  console.log(`\ncompetition lifecycle: ${pass}/${pass + fail} passed`);
  process.exitCode = fail ? 1 : 0;
})();
