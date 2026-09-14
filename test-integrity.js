/* Round-integrity regressions, from the external code review.
 *
 *   PAPER_DB=$(mktemp -u --suffix=.db) node test-integrity.js
 *
 * Each case here corresponds to a reported blocker. They are kept in their own
 * suite because they are adversarial rather than functional: none of them
 * describe a thing a well-behaved player does, and all of them change a
 * published result if they regress.
 */
const assert = require('assert');

process.env.PHOENIX_SNAPSHOT_FILE = '/nonexistent/markets-snapshot.json';
process.env.PAPER_COMP_TOKEN = process.env.PAPER_COMP_TOKEN || 'integrity-test-token';
if (!process.env.PAPER_DB || process.env.PAPER_DB.startsWith('/opt/')) {
  console.error('refusing to run: set PAPER_DB to a throwaway path first');
  process.exit(2);
}

const auth = require('./auth-shim.js');
/* Pinned before the engine loads: the production grace is 15s, sized from a
   measured cold start, which would make this suite crawl. The behaviour under
   test is the retry-then-fall-back shape, not the duration. */
process.env.PAPER_HOT_OPEN_GRACE_MS = process.env.PAPER_HOT_OPEN_GRACE_MS || '1200';
/* These suites drive the clock by hand and never accumulate reliability
   history, which production now requires. Say so explicitly rather than
   letting the engine silently treat no-evidence as evidence. */
process.env.PAPER_ALLOW_UNPROVEN_MARKETS = '1';
const comp = require('./competition.js');
const HOT_OPEN_GRACE_MS = Number(process.env.PAPER_HOT_OPEN_GRACE_MS);
const P = require('./paper.js');
const T = P.__test;
const CT = comp.__test;

/* This long adversarial file is the historical v1 regression archive. The
   current two-Hot format has its own deterministic suite; keeping these cases
   explicit v1 prevents old First Five/synthetic-ticker fixtures from silently
   masquerading as coverage of newly-created v2 rounds. Production has no
   legacy constructor: this adapter exists in this throwaway test process
   only, and downgrades the freshly-created row before it can start. */
const createCurrentRound = comp.createRound;
const LEGACY_PLANS = {
  round: { total: 30 * 60_000, firstFive: 5 * 60_000, reveal: 11.75 * 60_000,
    hotStart: 12 * 60_000, hotEnd: 16 * 60_000, boostStart: 27 * 60_000 },
  final: { total: 20 * 60_000, firstFive: 4 * 60_000, reveal: 7.75 * 60_000,
    hotStart: 8 * 60_000, hotEnd: 11 * 60_000, boostStart: 17 * 60_000 },
  rehearsal: { total: 180_000, firstFive: 30_000, reveal: 65_000,
    hotStart: 70_000, hotEnd: 100_000, boostStart: 150_000 },
};
for (const [kind, plan] of Object.entries(LEGACY_PLANS)) Object.assign(comp.ROUND_PLAN[kind], plan);
CT.db.exec('DROP TRIGGER IF EXISTS paper_round_draw_v2_immutable');
comp.createRound = (opts) => {
  const created = createCurrentRound(opts);
  const seed = created.hot_draw_secret;
  const backup = opts.backup ? String(opts.backup).trim().toUpperCase() : null;
  const candidates = opts.candidates.map((x) => String(x).trim().toUpperCase())
    .filter((x) => x && x !== backup);
  const commit = CT.commitOf(seed, candidates);
  CT.db.prepare(`UPDATE paper_rounds SET format_version = 1, plan_json = NULL,
      hot_candidates = ?, hot_backup = ?, draw_commit = ?, hot_draw_secret = NULL,
      hot_draw_json = NULL, draw_reveal_json = NULL WHERE id = ?`)
    .run(JSON.stringify(candidates), backup, commit, created.id);
  CT._seeds.set(created.id, seed);
  return CT.q.get.get(created.id);
};

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
  catch (e) { console.log('  FAIL ' + name + '\n       ' + e.message); process.exitCode = 1; }
  finally { _inflight = null; }
};

let CURRENT_USER = null;
auth.validateSession = async () => CURRENT_USER;
const asUser = (id) => { CURRENT_USER = { id, isGuest: false }; };
function mkRes() {
  const r = { code: null, body: null, writeHead(c) { r.code = c; }, end(s) { r.body = JSON.parse(s); } };
  return r;
}
const mkReqH = (body, headers) => {
  const buf = Buffer.from(JSON.stringify(body || {}));
  return { headers: headers || {}, on(ev, cb) { if (ev === 'data') cb(buf); if (ev === 'end') cb(); return this; } };
};
const mkReq = (body) => {
  const buf = Buffer.from(JSON.stringify(body || {}));
  return { headers: {}, on(ev, cb) { if (ev === 'data') cb(buf); if (ev === 'end') cb(); return this; } };
};
const callReset = async (body) => { const r = mkRes(); await P.reset(mkReq(body), r); return r; };
const order = async (uid, body) => { asUser(uid); const r = mkRes(); await P.placeOrder(mkReq(body), r); return r; };
const admin = async (body) => {
  const r = mkRes();
  await P.compAdmin(mkReqH(body, { 'x-comp-token': process.env.PAPER_COMP_TOKEN }), r);
  return r;
};

const setMarkRaw = (s, px) => T.live.map.set(s, {
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
for (const s of ['BTC', 'SOL', 'ETH', 'BNB', 'XRP']) {
  setMarkRaw(s, 100); feedMark(s, 100);
  T.mktCfg.set(s, { tiers: [], maxLev: 40, lotSize: null, takerBps: 3.5, makerBps: 0.5, maintBps: 50, cancelBps: 0, maxLiqSize: null, status: 'active', isolatedOnly: false });
}
let _equityFn = (uid) => { const a = T.stmt.acctGet.get(uid); return a ? T.accountRisk(uid, a).equityTotal : NaN; };
T.__equityRef = () => _equityFn;
T.__setEquity = (f) => { _equityFn = f; };
comp.wire({
  marketReady: () => true, // Explicit fixture price policy.
  /* The round stores the Boost cap it was armed under, so the suite has to
     say what that cap is, exactly as the engine does in production. */
  boostLeverage: () => 500,
  /* Wired because the suite exercises them: a finished round must release its
     price obligations, and claiming a seat must create its account. */
  closeRoundPauses: T.closeRoundPauses,
  ensureAccount: (u) => T.__ensureAccountRef()(u),
  openAlias: T.openAlias, closeAlias: T.closeAlias, aliasOpen: T.aliasOpen,
  scoreUser: T.scoreUser, scoreProofFor: T.scoreProofFor, hotValueOf: T.hotValueOf,
  segmentResidue: (alias) => ({
    positions: T.stmt.posBySymbol.all(alias).length,
    orders: T.stmt.ordOpenBySymbol.all(alias).length,
  }),
  prepareSeat: T.prepareSeat, seatState: T.seatState, markSetFor: T.markSetFor,
  /* Indirected so a case can inject an unpriceable seat, which is the only
     way to prove one sample covers the whole roster or none of it. */
  equityOf: (uid) => _equityFn(uid), log: () => {},
});

const A = 8001, B = 8002, OUTSIDER = 8003;
function mkAccount(uid) {
  T.db.prepare('INSERT OR IGNORE INTO users (id) VALUES (?)').run(uid);
  T.stmt.acctIns.run(uid, Date.now(), Date.now());
  T.db.prepare('UPDATE paper_accounts SET heat = 1, start_balance = 10, balance = 10 WHERE user_id = ?').run(uid);
}
[A, B, OUTSIDER].forEach(mkAccount);
const acct = (u) => T.stmt.acctGet.get(u);

(async () => {
  console.log('\nblocker 1: a seated player cannot reset their own account');
  comp.createRound({
    id: 'i1', candidates: ['BTC', 'SOL', 'ETH'],
    players: [{ userId: A, displayName: 'A', seat: 0 }, { userId: B, displayName: 'B', seat: 1 }],
  });
  await ok('locked as soon as they are seated, before the round even starts', () => {
    assert.strictEqual(comp.accountLocked(A), true, 'armed rounds own their roster too');
  });
  await ok('a player outside the round is not locked', () => {
    assert.strictEqual(comp.accountLocked(OUTSIDER), false);
  });
  await ok('the public reset endpoint refuses a seated player', async () => {
    asUser(A);
    const r = await callReset({});
    assert.strictEqual(r.code, 409, JSON.stringify(r.body));
    assert.strictEqual(r.body.error, 'in_competition_round');
  });
  await ok('losing money then resetting cannot restore the bankroll', async () => {
    T.db.prepare('UPDATE paper_accounts SET balance = 2 WHERE user_id = ?').run(A);
    asUser(A);
    await callReset({});
    assert.strictEqual(acct(A).balance, 2, 'the blown-up balance must survive the attempt');
  });
  await ok('an outsider can still reset normally', async () => {
    asUser(OUTSIDER);
    const r = await callReset({});
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
  });
  await ok('mode switching parses the body again (bodyRaw was undefined)', async () => {
    asUser(OUTSIDER);
    await callReset({ mode: 'heat' });
    assert.strictEqual(T.stmt.acctGet.get(OUTSIDER).heat, 1, 'mode switch must actually apply');
  });

  console.log('\nblocker 10: only one round may be live');
  comp.startRound('i1');
  await ok('starting a second round is refused', () => {
    comp.createRound({ id: 'i1b', candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: OUTSIDER }] });
    assert.throws(() => comp.startRound('i1b'), /already running/);
    comp.abortRound('i1b', { force: true });
  });

  console.log('\nblocker 11: the scoring epoch is bound at the start');
  await ok('starting a round resets every seat and records its epoch', () => {
    const rows = comp.playersOf('i1');
    assert.ok(rows.every((r) => Number.isFinite(r.epoch)), 'every seat needs a bound epoch');
    assert.strictEqual(acct(A).balance, 10, 'preparation restores the bankroll');
    assert.strictEqual(acct(A).epoch, rows.find((r) => r.user_id === A).epoch);
  });
  await ok('scoring follows the BOUND epoch, not the account\'s current one', () => {
    const bound = comp.playersOf('i1').find((r) => r.user_id === A).epoch;
    // a fill on the bound epoch counts
    T.stmt.fillIns.run(A, bound, 'BTC', 'SELL', 'MARKET', 100, 1, 100, 0, 3, null, Date.now(), 0);
    // one on a LATER epoch must not: that is the basis moving under a result
    T.stmt.fillIns.run(A, bound + 5, 'BTC', 'SELL', 'MARKET', 100, 1, 100, 0, 999, null, Date.now(), 0);
    const s = T.scoreUser(A, null, bound);
    assert.strictEqual(s.realized, 3, 'only the bound epoch may score, got ' + s.realized);
  });
  comp.abortRound('i1', { force: true });

  console.log('\nblocker 12: arming a round is all-or-nothing');
  await ok('a duplicate round id does not leave a stale seed behind', () => {
    comp.createRound({ id: 'i2', candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A }] });
    const before = CT._seeds.get('i2');
    assert.throws(() => comp.createRound({ id: 'i2', candidates: ['ETH', 'SOL', 'BTC'], players: [{ userId: A }] }));
    assert.strictEqual(CT._seeds.get('i2'), before, 'the committed seed must not be replaced');
  });
  await ok('a failed arm writes no partial roster', () => {
    assert.throws(() => comp.createRound({
      id: 'i3', candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A }, { userId: A }],
    }), /duplicate players/);
    assert.strictEqual(CT.q.get.get('i3'), undefined, 'no round row');
    assert.strictEqual(comp.playersOf('i3').length, 0, 'no orphan roster rows');
  });

  console.log('\ninput validation the review asked for');
  await ok('duplicate hot candidates are refused', () => {
    assert.throws(() => comp.createRound({ id: 'v1', candidates: ['BTC', 'BTC'], players: [{ userId: A }] }),
      /duplicate hot candidates/);
  });
  await ok('a rolling-client backup is normalized into the eligible pool', () => {
    const r = comp.createRound({ id: 'v2', candidates: ['BTC', 'SOL', 'ETH'], backup: 'BTC', players: [{ userId: A }] });
    assert.ok(r, 'compatibility input should arm without privileged ordering');
  });
  await ok('duplicate seats are refused', () => {
    assert.throws(() => comp.createRound({
      id: 'v3', candidates: ['BTC', 'SOL', 'ETH'],
      players: [{ userId: A, seat: 0 }, { userId: B, seat: 0 }],
    }), /duplicate seats/);
  });
  await ok('the live scorer roster is bounded to the public board contract', () => {
    assert.throws(() => comp.createRound({
      id: 'v3wide', candidates: ['BTC', 'SOL', 'ETH'],
      players: Array.from({ length: 33 }, (_, seat) => ({ displayName: `Seat ${seat + 1}`, seat })),
    }), /at most 32 seats/);
    assert.throws(() => comp.createRound({
      id: 'v3seat', candidates: ['BTC', 'SOL', 'ETH'], players: [{ displayName: 'Seat 33', seat: 32 }],
    }), /0 to 31/);
  });
  await ok('an empty roster cannot even be ARMED', () => {
    /* It used to arm successfully and only fail at start. Since there is no
       roster-amendment API, that was a round which existed solely so that it
       could later be thrown away. */
    assert.throws(() => comp.createRound({ id: 'v4', candidates: ['BTC', 'SOL', 'ETH'] }),
      /at least one seat/);
    assert.strictEqual(CT.q.get.get('v4'), undefined, 'and nothing is left behind');
    // the start-side check still stands on its own, for a roster emptied later
    comp.createRound({ id: 'v4b', candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    CT.db.prepare('DELETE FROM paper_round_players WHERE round_id = ?').run('v4b');
    assert.throws(() => comp.startRound('v4b'), /no players/);
  });

  await ok('a market with no reliability history cannot host a round in production', () => {
    /* The sustained check only applied once a minimum span had been observed,
       so a restart shortly before a show erased the evidence requirement and
       any market could start. No evidence is not evidence. */
    const prev = process.env.PAPER_ALLOW_UNPROVEN_MARKETS;
    delete process.env.PAPER_ALLOW_UNPROVEN_MARKETS;
    try {
      const v = comp.marketReadiness('v4b');
      assert.strictEqual(v.ok, false, 'a market nobody has watched must not be startable');
      assert.match(v.error, /reliability history/);
    } finally {
      if (prev !== undefined) process.env.PAPER_ALLOW_UNPROVEN_MARKETS = prev;
    }
    comp.abortRound('v4b', { force: true });
  });

  console.log('\nblocker 2: a base tick must risk-check its alias positions');
  await ok('a BTC tick evaluates BTC-BOOST, not just BTC', () => {
    const uid = A;
    T.stmt.acctIns.run(uid, Date.now(), Date.now());
    T.db.prepare('UPDATE paper_accounts SET heat = 1, start_balance = 10, balance = 10 WHERE user_id = ?').run(uid);
    T.db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(uid);
    const ep = T.seatState_epoch(uid);
    // 1000x long, isolated, tiny margin: a 5% adverse move must liquidate it
    T.stmt.posIns.run(uid, 'BTC-BOOST', ep, 'LONG', 1, 100, 1000, Date.now(), 100, Date.now(), Date.now(), 'isolated', 0.1);
    setMarkRaw('BTC', 90); feedMark('BTC', 90);
    T.tickEval('BTC', { force: true });          // a BASE tick, not an alias tick
    assert.strictEqual(T.stmt.posGet.get(uid, 'BTC-BOOST'), undefined,
      'the boosted position must be liquidated by the base tick, not left for the 5s sweep');
    setMarkRaw('BTC', 100); feedMark('BTC', 100);
  });
  await ok('the same holds for a HOT twin', () => {
    const uid = B;
    T.db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(uid);
    T.db.prepare('UPDATE paper_accounts SET heat = 1, start_balance = 10, balance = 10 WHERE user_id = ?').run(uid);
    const ep = T.seatState_epoch(uid);
    T.stmt.posIns.run(uid, 'SOL-HOT', ep, 'LONG', 1, 100, 1000, Date.now(), 100, Date.now(), Date.now(), 'isolated', 0.1);
    setMarkRaw('SOL', 90); feedMark('SOL', 90);
    T.tickEval('SOL', { force: true });
    assert.strictEqual(T.stmt.posGet.get(uid, 'SOL-HOT'), undefined);
    setMarkRaw('SOL', 100); feedMark('SOL', 100);
  });

  console.log('\nblocker 3: resting orders cannot outlive their segment');
  await ok('closing a segment cancels orders resting on it', () => {
    T.openAlias('BTC-HOT', 'i-ord');
    const ep = T.seatState_epoch(A);
    T.stmt.ordIns.run(A, ep, 'BTC-HOT', 'BUY', 50, 1, 10, 0, Date.now(), 'cross', null, null);
    assert.strictEqual(T.stmt.ordOpenBySymbol.all('BTC-HOT').length, 1, 'order should be resting');
    T.closeAlias('BTC-HOT');
    assert.strictEqual(T.stmt.ordOpenBySymbol.all('BTC-HOT').length, 0,
      'a closed segment must leave nothing resting that could fill later');
  });
  await ok('the sweep independently kills an order on a closed gate', () => {
    /* sweep() no-ops unless prices are considered up; in the sandbox nothing
       sets the WS heartbeat, so stamp it or the sweep returns before the
       guard under test is reached. */
    T.live.lastMsgMs = Date.now();
    const ep = T.seatState_epoch(A);
    // slipped in behind closeAlias: the gate is shut but the order exists
    T.stmt.ordIns.run(A, ep, 'BTC-HOT', 'BUY', 50, 1, 10, 0, Date.now(), 'cross', null, null);
    assert.strictEqual(T.aliasOpen('BTC-HOT'), false, 'gate is shut');
    T.sweep();
    assert.strictEqual(T.stmt.ordOpenBySymbol.all('BTC-HOT').length, 0,
      'the sweep must cancel it rather than fill it');
  });

  console.log('\nblocker 4: writes cannot land after their boundary');
  await ok('a trade attempted after the bell cannot enter the frozen result', async () => {
    T.db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(A);
    comp.createRound({ id: 'i4', candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    comp.startRound('i4');
    /* Walk the round through its boundaries in order, each fired when it is
       genuinely due. Jumping straight to the bell skips the Hot window, which
       the engine now refuses to fabricate, so a fixture that leaps to the end
       is testing an impossible round. */
    for (const at of comp.boundariesOf('round')) fireAt('i4', at);
    assert.strictEqual(CT.q.get.get('i4').status, 'done', 'the round should have settled');

    const board = comp.standings('i4', 'final');
    assert.strictEqual(board.length, 1, 'the bell must have settled every seat');
    const settled = board[0].account_pnl;

    // now trade after the bell: the frozen result must not move
    asUser(A);
    const res = mkRes();
    await P.placeOrder(mkReq({ symbol: 'BTC', side: 'BUY', type: 'MARKET', size: 0.01, leverage: 10 }), res);
    assert.strictEqual(comp.standings('i4', 'final')[0].account_pnl, settled,
      'a post-bell trade must not alter a published result');
  });
  await ok('a spectator is unaffected by another round settling', async () => {
    asUser(OUTSIDER);
    assert.strictEqual(comp.settledFor(OUTSIDER), false);
  });

  console.log('\nblocker 5: a checkpoint is all-or-nothing');
  await ok('one unscoreable seat fails the whole checkpoint', () => {
    comp.createRound({
      id: 'i5', candidates: ['BTC', 'SOL', 'ETH'],
      players: [{ userId: A, displayName: 'A', seat: 0 }, { userId: B, displayName: 'B', seat: 1 }],
    });
    comp.startRound('i5');
    // break the second seat BEFORE any checkpoint runs
    const realScore = T.scoreUser;
    comp.wire({ scoreUser: (uid, hot, ep, sb, mk, asOf) => {
      if (uid === B) { throw new Error('boom'); }
      return realScore(uid, hot, ep, sb, mk, asOf);
    } });
    fireAt('i5', comp.ROUND_PLAN.round.firstFive);
    assert.strictEqual(comp.standings('i5', 'firstFive').length, 0,
      'no partial leaderboard may be written');
    assert.match(CT.q.get.get('i5').blocked_reason || '', /boom/, 'and the round blocks');
    comp.wire({ scoreUser: realScore });
    comp.abortRound('i5', { force: true });   // only one round may be live
  });
  await ok('a failed bell BLOCKS the round instead of finishing it', () => {
    /* Its own round: the partial-checkpoint case above deliberately blocks at
       First Five, so this sequence needs a round that reaches its bell. */
    comp.createRound({
      id: 'i5c', candidates: ['BTC', 'SOL', 'ETH'],
      players: [{ userId: A, displayName: 'A', seat: 0 }, { userId: B, displayName: 'B', seat: 1 }],
    });
    comp.startRound('i5c');
    for (const at of comp.boundariesOf('round')) {
      if (at === comp.ROUND_PLAN.round.total) break;
      fireAt('i5c', at);
    }
    const realScore = T.scoreUser;
    comp.wire({ scoreUser: (uid, hot, ep, sb, mk, asOf) => {
      if (uid === B) { throw new Error('boom'); }
      return realScore(uid, hot, ep, sb, mk, asOf);
    } });
    fireAt('i5c', comp.ROUND_PLAN.round.total);
    const r = CT.q.get.get('i5c');
    assert.notStrictEqual(r.status, 'done', 'a round that could not be scored must not look finished');
    assert.match(r.blocked_reason || '', /boundary .* failed/);
    assert.strictEqual(CT.q.bGet.get('i5c', comp.ROUND_PLAN.round.total).status, 'failed');
    comp.wire({ scoreUser: realScore });
  });
  await ok('a blocked round refuses to advance until an operator clears it', () => {
    const before = CT.q.bGet.get('i5c', comp.ROUND_PLAN.round.total).ran_at;
    CT.fireBoundary('i5c', comp.ROUND_PLAN.round.total);
    assert.notStrictEqual(CT.q.get.get('i5c').status, 'done', 'still blocked, must not settle');
    assert.strictEqual(CT.q.bGet.get('i5c', comp.ROUND_PLAN.round.total).ran_at, before,
      'a blocked round must not even re-run the boundary');
  });
  await ok('recovery RETRIES the boundary, it does not merely unlock', () => {
    const r = comp.clearBlock('i5c', { note: 'test recovery' });
    assert.strictEqual(r.blocked_reason, null, 'the block must be cleared by a successful recovery');
    assert.strictEqual(r.status, 'done', 'and the boundary it owed must have run');
    assert.strictEqual(comp.standings('i5c', 'final').length, 2, 'scoring everyone');
    assert.strictEqual(CT.q.bGet.get('i5c', comp.ROUND_PLAN.round.total).status, 'succeeded');
  });
  await ok('a succeeded boundary is never replayed', () => {
    const before = comp.standings('i5c', 'final')[0].at;
    CT.fireBoundary('i5c', comp.ROUND_PLAN.round.total);
    assert.strictEqual(comp.standings('i5c', 'final')[0].at, before);
  });

  console.log('\nblocker 6: open Hot exposure counts 2x on the live wall');
  await ok('an OPEN hot position already shows its bonus', () => {
    T.db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(A);
    const ep = T.seatState_epoch(A);
    T.stmt.posIns.run(A, 'SOL-HOT', ep, 'LONG', 1, 100, 10, Date.now(), 100, Date.now(), Date.now(), 'cross', 0);
    setMarkRaw('SOL', 105); feedMark('SOL', 105);                       // +5 unrealised on the hot leg
    const s = T.scoreUser(A, 'SOL-HOT', ep);
    assert.ok(Math.abs(s.hotBonus - 5) < 1e-6,
      'the bonus must track the open leg, not wait for it to close: got ' + s.hotBonus);
  });
  await ok('closing the hot leg does not jump the score', () => {
    const ep = T.seatState_epoch(A);
    const before = T.scoreUser(A, 'SOL-HOT', ep);
    const beforeScore = before.accountPnl + before.hotBonus;
    // realise it at the same mark, the way the segment close does
    T.stmt.fillIns.run(A, ep, 'SOL-HOT', 'SELL', 'SEGMENT', 105, 1, 105, 0, 5, null, Date.now(), 0);
    T.db.prepare('UPDATE paper_accounts SET balance = balance + 5 WHERE user_id = ?').run(A);
    T.db.prepare("DELETE FROM paper_positions WHERE user_id = ? AND symbol = 'SOL-HOT'").run(A);
    const after = T.scoreUser(A, 'SOL-HOT', ep);
    assert.ok(Math.abs((after.accountPnl + after.hotBonus) - beforeScore) < 1e-6,
      `score stepped at close: ${beforeScore} -> ${after.accountPnl + after.hotBonus}`);
    setMarkRaw('SOL', 100); feedMark('SOL', 100);
  });

  console.log('\nblocker 7: the server owns Boost behaviour');
  await ok('a seated player cannot arm the legacy per-position clock', () => {
    comp.createRound({ id: 'i7', candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    comp.startRound('i7');
    const ep = T.seatState_epoch(A);
    T.applyFill(A, { symbol: 'BTC', orderSide: 'BUY', size: 0.01, px: 100, feeBps: 0, kind: 'MARKET', leverage: 500 });
    const pos = T.stmt.posGet.get(A, 'BTC');
    assert.ok(pos, 'position should exist');
    assert.strictEqual(pos.boost_since, null,
      'the legacy 2:00 clock must never touch a competition position');
  });
  await ok('a client flag cannot change how a competitor is treated', () => {
    T.db.prepare("DELETE FROM paper_positions WHERE user_id = ? AND symbol = 'SOL'").run(A);
    T.applyFill(A, { symbol: 'SOL', orderSide: 'BUY', size: 0.01, px: 100, feeBps: 0, kind: 'MARKET', leverage: 500, boostWindow: true });
    assert.strictEqual(T.stmt.posGet.get(A, 'SOL').boost_since, null,
      'boostWindow:true from a browser must not arm it either');
    comp.abortRound('i7', { force: true });
  });
  await ok('the public product keeps its legacy clock', () => {
    T.db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(OUTSIDER);
    T.db.prepare('UPDATE paper_accounts SET heat = 1 WHERE user_id = ?').run(OUTSIDER);
    T.applyFill(OUTSIDER, { symbol: 'BTC', orderSide: 'BUY', size: 0.01, px: 100, feeBps: 0, kind: 'MARKET', leverage: 500 });
    assert.ok(T.stmt.posGet.get(OUTSIDER, 'BTC').boost_since,
      'an ordinary paper trader above 100x still starts the 2:00 clock');
  });

  console.log('\nblocker 8: a fallback must not invalidate the draw proof');
  await ok('the drawn market survives a fallback, and both are published', async () => {
    comp.createRound({ id: 'i8', candidates: ['BTC', 'SOL', 'ETH'], backup: 'ETH', players: [{ userId: A, seat: 0 }] });
    comp.startRound('i8');
    fireAt('i8', comp.ROUND_PLAN.round.reveal);
    const drawn = CT.q.get.get('i8').hot_base;
    // make the drawn market unopenable so the backup has to take over
    T.live.map.delete(drawn);
    fireAt('i8', comp.ROUND_PLAN.round.hotStart);
    /* A fallback is now the END of a grace period, not the first thing tried:
       a market that is merely blipping gets its window back, and only a market
       that stays gone is replaced. So the backup appears after the grace, and
       the outage must be recorded while we wait. */
    assert.strictEqual(CT.q.get.get('i8').active_hot_base, null, 'nothing trades during the grace');
    assert.match(CT.q.get.get('i8').gate_outage || '', /"hot"/, 'and the wait is on the record');
    await new Promise((res) => setTimeout(res, HOT_OPEN_GRACE_MS + 600));
    const r = CT.q.get.get('i8');
    assert.strictEqual(r.hot_base, drawn, 'the DRAWN market must never be overwritten');
    assert.strictEqual(r.active_hot_base, 'ETH', 'the backup is what traded');
    assert.ok(r.fallback_reason, 'the reason must be recorded, not silent');
    const v = comp.verifyDraw(r);
    assert.strictEqual(v.ok, true, 'the proof must still verify: ' + v.reason);
    assert.strictEqual(v.fellBack, true);
    assert.strictEqual(v.traded, 'ETH');
    setMarkRaw(drawn, 100); feedMark(drawn, 100);
    comp.abortRound('i8', { force: true });
  });

  console.log('\nround two: the mutation barrier covers every path');
  await ok('closing a position after the bell is refused', async () => {
    T.db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(A);
    comp.createRound({ id: 'r2a', candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    comp.startRound('r2a');
    const ep = T.seatState_epoch(A);
    T.stmt.posIns.run(A, 'BTC', ep, 'LONG', 1, 100, 10, Date.now(), 100, Date.now(), Date.now(), 'cross', 0);
    CT.db.prepare('UPDATE paper_rounds SET started_at = ?, ends_at = ? WHERE id = ?')
      .run(Date.now() - comp.ROUND_PLAN.round.total - 1000, Date.now() - 1000, 'r2a');
    asUser(A);
    const res = mkRes();
    await P.closePosition(mkReq({ symbol: 'BTC', requestId: 'after-bell-close', accountEpoch: ep }), res);
    assert.strictEqual(res.code, 409, JSON.stringify(res.body));
    assert.ok(T.stmt.posGet.get(A, 'BTC'), 'the position must be untouched');
  });
  await ok('adjusting margin after the bell is refused', async () => {
    asUser(A);
    const res = mkRes();
    await P.adjustMargin(mkReq({ symbol: 'BTC', amount: 1,
      requestId: 'after-bell-margin', accountEpoch: T.stmt.acctGet.get(A).epoch }), res);
    assert.strictEqual(res.code, 409);
  });
  await ok('a blocked round also bars writes', async () => {
    CT.db.prepare('UPDATE paper_rounds SET started_at = ?, ends_at = ?, blocked_reason = ? WHERE id = ?')
      .run(Date.now() - 60_000, Date.now() + 60_000, 'test block', 'r2a');
    assert.strictEqual(comp.writeBarrier(A), 'round_blocked');
    CT.db.prepare('UPDATE paper_rounds SET blocked_reason = NULL WHERE id = ?').run('r2a');
  });
  await ok('a spectator is never barred by someone else\'s round', () => {
    assert.strictEqual(comp.writeBarrier(OUTSIDER), null);
  });

  console.log('\nround two: phase is authoritative for segment eligibility');
  await ok('a late hot-end leaves the ticker ineligible anyway', () => {
    T.openAlias('BTC-HOT', 'r2a');           // gate still open, as a stuck timer would leave it
    const r = CT.q.get.get('r2a');
    const inOpen = r.started_at + comp.ROUND_PLAN.round.hotEnd + 60_000;   // past Hot
    assert.strictEqual(comp.levCapFor('BTC-HOT', 1000, A, inOpen), 0,
      'the phase must refuse it even though the gate is open');
    T.closeAlias('BTC-HOT', { roundId: 'r2a' });
  });

  console.log('\nround two: operator actions cannot corrupt a live round');
  await ok('resetPlayers is refused on a running round', async () => {
    const res = mkRes();
    await P.compAdmin(mkReqH({ action: 'resetPlayers', id: 'r2a' }, { 'x-comp-token': process.env.PAPER_COMP_TOKEN }), res);
    assert.strictEqual(res.code, 409, JSON.stringify(res.body));
  });
  await ok('aborting a future armed round leaves the live round alone', () => {
    T.openAlias('BTC-BOOST', 'r2a');
    comp.createRound({ id: 'r2b', candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: B, seat: 0 }] });
    comp.abortRound('r2b', { force: true });
    assert.strictEqual(T.aliasOpen('BTC-BOOST'), true,
      'the live round\'s Boost ticker must survive an unrelated abort');
    T.closeAlias('BTC-BOOST', { roundId: 'r2a' });
  });
  await ok('a non-owner cannot close another round\'s alias', () => {
    T.openAlias('BTC-BOOST', 'r2a');
    const res = T.closeAlias('BTC-BOOST', { roundId: 'someone-else' });
    assert.strictEqual(res.skipped, 'not_owner');
    assert.strictEqual(T.aliasOpen('BTC-BOOST'), true);
    T.closeAlias('BTC-BOOST', { roundId: 'r2a' });
  });

  console.log('\nround two: restart recovery');
  await ok('a boundary left running by a dead process becomes retryable', () => {
    CT.q.bMark.run('r2a', comp.ROUND_PLAN.round.firstFive, 'running', null, Date.now() - 60_000);
    CT.db.prepare('UPDATE paper_rounds SET started_at = ?, ends_at = ? WHERE id = ?')
      .run(Date.now() - 60_000, Date.now() + 600_000, 'r2a');
    comp.resume();
    const b = CT.q.bGet.get('r2a', comp.ROUND_PLAN.round.firstFive);
    assert.notStrictEqual(b.status, 'running', 'a stale lease must not block forever');
  });
  await ok('a restart past a CLOSED segment window blocks instead of continuing', () => {
    /* MISSED means the window is gone, not merely that the boundary has not
       run. This used to wind the clock only past First Five, which is a
       settlement boundary: it prices an instant that has already passed and
       can always be retried, and resume() now lets schedule() re-fire it
       rather than killing the round. A boundary that PRICES and can no longer
       find its mark still fails closed on its own, which is where that guard
       belongs. What can never be honoured is opening a segment after its
       window has closed, so the clock is wound past the whole Hot segment:
       that show did not happen and must not be resumed as if it had. */
    CT.db.prepare('UPDATE paper_rounds SET started_at = ?, blocked_reason = NULL WHERE id = ?')
      .run(Date.now() - (comp.ROUND_PLAN.round.boostStart + 1000), 'r2a');
    CT.db.prepare('DELETE FROM paper_round_boundaries WHERE round_id = ?').run('r2a');
    comp.resume();
    assert.match(CT.q.get.get('r2a').blocked_reason || '', /missed/,
      'a show that did not happen must not be resumed as if it had');
  });
  await ok('gates are rebuilt from durable state, not from replaying boundaries', () => {
    comp.clearBlock('r2a');
    const r = CT.q.get.get('r2a');
    CT.q.setDraw.run('BTC', 'seed', Date.now(), Date.now(), 'r2a');
    CT.q.setActive.run('BTC', null, Date.now(), 'r2a');
    // sit the clock inside the Hot window, with the gate wrongly closed
    CT.db.prepare('UPDATE paper_rounds SET started_at = ? WHERE id = ?')
      .run(Date.now() - (comp.ROUND_PLAN.round.hotStart + 30_000), 'r2a');
    T.closeAlias('BTC-HOT', { roundId: 'r2a' });
    assert.strictEqual(T.aliasOpen('BTC-HOT'), false);
    comp.rehydrateGates(CT.q.get.get('r2a'));
    assert.strictEqual(T.aliasOpen('BTC-HOT'), true,
      'a restart mid-Hot must reopen the ticker its phase says is live');
    T.closeAlias('BTC-HOT', { roundId: 'r2a' });
    comp.abortRound('r2a', { force: true });
  });

  console.log('\nround two: Hot close is canonical and fail-closed');
  await ok('settlement refuses to run without a fresh mark', () => {
    T.openAlias('SOL-HOT', 'r2c');
    const ep = T.seatState_epoch(A);
    T.db.prepare("DELETE FROM paper_positions WHERE user_id = ? AND symbol = 'SOL-HOT'").run(A);
    T.stmt.posIns.run(A, 'SOL-HOT', ep, 'LONG', 1, 100, 10, Date.now(), 100, Date.now(), Date.now(), 'cross', 0);
    assert.ok(T.stmt.posGet.get(A, 'SOL-HOT'), 'fixture: the position must exist to be settled');
    const saved = T.live.map.get('SOL');
    T.live.map.delete('SOL');
    assert.throws(() => T.closeAlias('SOL-HOT', { roundId: 'r2c' }), /no mark at/,
      'a missing mark must fail the segment, not silently leave someone open');
    assert.ok(T.stmt.posGet.get(A, 'SOL-HOT'), 'and the position is still there to settle later');
    T.live.map.set('SOL', saved);
    T.closeAlias('SOL-HOT', { roundId: 'r2c' });
    assert.strictEqual(T.stmt.posGet.get(A, 'SOL-HOT'), undefined, 'settles once the mark returns');
  });

  console.log('\nround two: the final mark is the SCHEDULED one');
  await ok('a mark history answers "what was the price at time T"', () => {
    /* Own symbol, deliberately. This used BTC, whose history other tests are
       feeding constantly, and passed only because the old blind tail-push put
       the backdated mark where the newest-first scan hit it first, shadowing
       the real entries in between: the exact settlement bug the sorted insert
       fixed. On shared history the assertion is about OTHER tests' feeds. */
    const t0 = Date.now();
    T.recordMark('HISTQ', 100, t0 - 5000);
    T.recordMark('HISTQ', 110, t0 - 1000);
    assert.strictEqual(T.markAt('HISTQ', t0 - 3000), 100, 'must return the mark at or before T');
    assert.strictEqual(T.markAt('HISTQ', t0), 110);
  });
  await ok('a late bell does not let a post-bell move into the result', () => {
    T.db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(A);
    comp.createRound({ id: 'r2t', candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    comp.startRound('r2t');
    const ep = T.seatState_epoch(A);
    T.stmt.posIns.run(A, 'BTC', ep, 'LONG', 1, 100, 10, Date.now(), 100, Date.now(), Date.now(), 'cross', 0);

    const observedAt = Date.now();
    const due = observedAt - 4000;             // the bell was due 4s ago
    /* Build the boundary observation from a source that was healthy AT that
       instant. Reusing the suite-wide BTC components made this case depend on
       how quickly every earlier assertion ran: under CPU contention (or a
       deliberately longer Hot-open grace) those components were already
       stale at `due - 500`, so recordMark correctly stored ok:false and the
       fixture, not the engine, had no boundary price. feedMark lays down the
       same short accepted trail a continuously ticking source would have. */
    feedMark('BTC', 100, observedAt);           // 100 before and AT the bell
    /* setMarkRaw, not the trail-seeding helper: seeding 140 backwards would
       rewrite history across the bell instant and hide the very thing this
       test checks. The move happens strictly after the boundary. */
    setMarkRaw('BTC', 140);
    T.recordMark('BTC', 140, Date.now());

    comp.snapshot('r2t', 'final', due);
    const row = comp.standings('r2t', 'final')[0];
    // 1 unit long from 100: at the bell that is +0, at callback time +40
    assert.ok(Math.abs(row.account_pnl) < 1e-6,
      `the result used the callback price, not the bell price: pnl ${row.account_pnl}`);
    assert.strictEqual(row.scheduled_at, due, 'the scheduled instant must be recorded');
  });
  await ok('the exact marks used are stored for replay', () => {
    try {
      const row = comp.standings('r2t', 'final')[0];
      const m = JSON.parse(row.marks || '{}');
      assert.strictEqual(m.BTC, 100, 'the stored mark set must be the one the result came from');
    } finally {
      /* `ok` intentionally records a failed assertion and keeps the suite
         running. Always release this round even when the replay assertion
         fails, or later source-promotion cases inherit the live-round rules
         instead of the non-live contract they are meant to exercise. */
      comp.abortRound('r2t', { force: true });
      setMarkRaw('BTC', 100); feedMark('BTC', 100);
    }
  });

  console.log('\nround two: drawdown, auth and the audit trail');
  await ok('drawdown is measured, so the published tie-break can be applied', () => {
    T.db.prepare('DELETE FROM paper_positions WHERE user_id IN (?, ?)').run(A, B);
    comp.createRound({
      id: 'r2d', candidates: ['BTC', 'SOL', 'ETH'],
      players: [{ userId: A, displayName: 'A', seat: 0 }, { userId: B, displayName: 'B', seat: 1 }],
    });
    comp.startRound('r2d');
    /* Both finish level, but A dipped on the way and B did not. The first
       sample has to happen at the HIGH point, otherwise the peak is set at
       the bottom and no drawdown is ever recorded. */
    comp.sampleDrawdown();                                   // peak: both at 10
    T.db.prepare('UPDATE paper_accounts SET balance = 6 WHERE user_id = ?').run(A);
    comp.sampleDrawdown();                                   // A falls to 6
    T.db.prepare('UPDATE paper_accounts SET balance = 10 WHERE user_id = ?').run(A);
    comp.sampleDrawdown();                                   // and recovers
    const rows = comp.playersOf('r2d');
    const a = rows.find((x) => x.user_id === A);
    assert.ok(a.max_drawdown > 0, 'the dip must be recorded, got ' + a.max_drawdown);
    const b = rows.find((x) => x.user_id === B);
    assert.ok(!(b.max_drawdown > 0), 'a seat that never fell has no drawdown');
  });
  await ok('a tie on score is broken by the SHALLOWER drawdown', () => {
    comp.snapshot('r2d', 'final');
    const board = comp.standings('r2d', 'final');
    assert.strictEqual(board.length, 2);
    assert.ok(Math.abs(board[0].score - board[1].score) < 1e-9, 'scores should be level for this case');
    assert.strictEqual(board[0].user_id, B, 'the steadier trader should win the tie');
    assert.ok(board[0].maxDrawdown <= board[1].maxDrawdown);
    comp.abortRound('r2d', { force: true });
  });
  await ok('operator actions are written to an audit trail', () => {
    const rows = T.db.prepare("SELECT * FROM paper_operator_log ORDER BY id DESC LIMIT 20").all();
    assert.ok(rows.length, 'operator actions must leave a record');
    assert.ok(rows.some((r) => r.action === 'resetPlayers' && r.ok === 0),
      'a refused action must be recorded too, not only successful ones');
  });

  console.log('\nround three: drawdown is initialised and frozen');
  await ok('peak starts at the bankroll, so an opening loss counts', () => {
    T.db.prepare('DELETE FROM paper_positions WHERE user_id IN (?, ?)').run(A, B);
    comp.createRound({
      id: 'r3d', candidates: ['BTC', 'SOL', 'ETH'],
      players: [{ userId: A, displayName: 'A', seat: 0 }, { userId: B, displayName: 'B', seat: 1 }],
    });
    comp.startRound('r3d');
    const seeded = comp.playersOf('r3d').find((p) => p.user_id === A);
    assert.strictEqual(seeded.peak_equity, seeded.start_balance,
      'peak must begin at the bankroll, not at the first sample');
    // straight down from the open: previously this became the peak and vanished
    T.db.prepare('UPDATE paper_accounts SET balance = 6 WHERE user_id = ?').run(A);
    comp.sampleDrawdown();
    const a = comp.playersOf('r3d').find((p) => p.user_id === A);
    assert.ok(Math.abs(a.max_drawdown - 4) < 1e-6, 'expected 4, got ' + a.max_drawdown);
  });
  await ok('a published checkpoint freezes its drawdown', () => {
    comp.snapshot('r3d', 'firstFive');
    const before = comp.standings('r3d', 'firstFive').map((r) => r.maxDrawdown);
    // a deeper dip AFTER the checkpoint must not rewrite the published order
    T.db.prepare('UPDATE paper_accounts SET balance = 1 WHERE user_id = ?').run(B);
    comp.sampleDrawdown();
    const after = comp.standings('r3d', 'firstFive').map((r) => r.maxDrawdown);
    assert.deepStrictEqual(after, before, 'a frozen tie-break must not move');
    comp.abortRound('r3d', { force: true });
  });

  console.log('\nround three: crash-safety and input hygiene');
  await ok('a persisted draw survives a lost in-memory seed', () => {
    const id = 'r3draw';
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    comp.startRound(id);
    CT.db.prepare('UPDATE paper_rounds SET started_at = ? WHERE id = ?')
      .run(Date.now() - (comp.ROUND_PLAN.round.reveal + 1000), id);
    fireAt(id, comp.ROUND_PLAN.round.reveal);       // draw commits
    const drawn = CT.q.get.get(id).hot_base;
    assert.ok(drawn, 'the draw should have happened');
    // simulate a crash between the draw committing and the boundary being marked
    CT.db.prepare("DELETE FROM paper_round_boundaries WHERE round_id = ? AND at = ?")
      .run(id, comp.ROUND_PLAN.round.reveal);
    CT._seeds.delete(id);
    fireAt(id, comp.ROUND_PLAN.round.reveal);
    const r = CT.q.get.get(id);
    assert.strictEqual(r.blocked_reason, null, 'a valid persisted draw must not block');
    assert.strictEqual(r.hot_base, drawn, 'and must not be redrawn');
    comp.abortRound(id, { force: true });
  });
  await ok('a roster of 1 and "1" is rejected, not silently collapsed', () => {
    assert.throws(() => comp.createRound({
      id: 'r3n', candidates: ['BTC', 'SOL', 'ETH'],
      players: [{ userId: 1, seat: 0 }, { userId: '1', seat: 1 }],
    }), /duplicate players/);
  });
  await ok('a finished round cannot be rewritten', () => {
    const id = 'r3f';
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    comp.startRound(id);
    for (const at of comp.boundariesOf('round')) fireAt(id, at);
    assert.strictEqual(CT.q.get.get(id).status, 'done');
    assert.throws(() => comp.abortRound(id), /immutable|published result/,
      'a published result must not be re-statused');
  });
  await ok('Boost cannot report success with no market open', () => {
    const id = 'r3b';
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    comp.startRound(id);
    const saved = new Map();
    for (const b of ['BTC', 'ETH', 'BNB', 'XRP', 'SOL']) { saved.set(b, T.live.map.get(b)); T.live.map.delete(b); }
    try {
      CT.db.prepare('UPDATE paper_rounds SET started_at = ? WHERE id = ?')
        .run(Date.now() - (comp.ROUND_PLAN.round.boostStart + 1000), id);
      fireAt(id, comp.ROUND_PLAN.round.boostStart);
      assert.match(CT.q.get.get(id).blocked_reason || '', /Boost|fresh mark/,
        'a Boost phase with nothing tradable is a missing segment, not a degraded one');
    } finally {
      for (const [b, v] of saved) if (v) T.live.map.set(b, v);
      comp.abortRound(id, { force: true });
    }
  });

  console.log('\nround four: the order is the event');
  await ok('an order arriving after Hot start opens the segment itself', async () => {
    T.db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(A);
    comp.createRound({ id: 'r4a', candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    comp.startRound('r4a');
    fireAt('r4a', comp.ROUND_PLAN.round.firstFive);
    fireAt('r4a', comp.ROUND_PLAN.round.reveal);
    const hot = CT.q.get.get('r4a').hot_base + '-HOT';
    /* Wind the clock INTO the Hot window but never fire the boundary: this is
       a late timer. The order itself must advance the clock and open the
       segment, rather than being rejected by a gate the timer never opened. */
    CT.db.prepare('UPDATE paper_rounds SET started_at = ? WHERE id = ?')
      .run(Date.now() - comp.ROUND_PLAN.round.hotStart - 300, 'r4a');
    assert.strictEqual(T.aliasOpen(hot), false, 'gate is shut because the timer is late');
    asUser(A);
    const res = mkRes();
    await P.placeOrder(mkReq({ symbol: hot, side: 'BUY', type: 'MARKET', size: 0.01, leverage: 10 }), res);
    assert.strictEqual(T.aliasOpen(hot), true, 'the order must have opened Hot');
    assert.strictEqual(res.code, 200, 'and then been accepted: ' + JSON.stringify(res.body));
  });
  await ok('a Hot order straddling Hot end cannot fill', async () => {
    const hot = CT.q.get.get('r4a').hot_base + '-HOT';
    // now wind past Hot end, again without firing the timer
    CT.db.prepare('UPDATE paper_rounds SET started_at = ? WHERE id = ?')
      .run(Date.now() - comp.ROUND_PLAN.round.hotEnd - 300, 'r4a');
    /* Deterministic mark just before the wound-back hotEnd instant. The
       fixture's 2s feed cadence raced the dispute intervals earlier tests
       leave behind: when no accepted observation landed between a dispute's
       resolution and the due instant, strict markAt correctly refused to
       settle off the pre-jump mark and the round blocked, so this test
       failed on feed PHASE, about two runs in three. The engine is right to
       fail closed there; the fixture must simply provide the observation a
       live feed always would. */
    {
      const base = CT.q.get.get('r4a').hot_base;
      /* At the CURRENT level: a hardcoded 100 was itself a 900bps jump when a
         neighbouring test had walked this symbol to 110, which opened a fresh
         dispute and recorded ok:false right before the instant under test. */
      const px = Number(T.markOfFreshFor(base, true)) || 100;
      const tJust = CT.q.get.get('r4a').started_at + comp.ROUND_PLAN.round.hotEnd - 10;
      T.compUpdate(base, 'usdt', px, tJust); T.compUpdate(base, 'usd', px, tJust);
    }
    asUser(A);
    const res = mkRes();
    await P.placeOrder(mkReq({ symbol: hot, side: 'BUY', type: 'MARKET', size: 0.01, leverage: 10 }), res);
    assert.strictEqual(res.code, 400, 'must be refused: ' + JSON.stringify(res.body));
    assert.strictEqual(T.aliasOpen(hot), false, 'and Hot must be closed');
    comp.abortRound('r4a', { force: true });
  });

  console.log('\nround four: one-source prices are not competition-valid');

  console.log('\nround four: blocked accounts are untouchable');
  await ok('a blocked round survives an adverse tick without liquidating', () => {
    T.db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(A);
    comp.createRound({ id: 'r4b', candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    comp.startRound('r4b');
    const ep = T.seatState_epoch(A);
    T.stmt.posIns.run(A, 'BTC', ep, 'LONG', 1, 100, 1000, Date.now(), 100, Date.now(), Date.now(), 'cross', 0);
    CT.db.prepare('UPDATE paper_rounds SET blocked_reason = ? WHERE id = ?').run('test block', 'r4b');
    setMarkRaw('BTC', 80); feedMark('BTC', 80);
    T.tickEval('BTC', { force: true });
    assert.ok(T.stmt.posGet.get(A, 'BTC'),
      'a blocked competitor must not be liquidated by an incoming tick');
    setMarkRaw('BTC', 100); feedMark('BTC', 100);
    comp.abortRound('r4b', { force: true });
  });

  console.log('\nround four: abort cannot outrun the bell');
  await ok('a normal abort after the bell settles instead of discarding', () => {
    comp.createRound({ id: 'r4c', candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    comp.startRound('r4c');
    for (const at of comp.boundariesOf('round')) {
      if (at === comp.ROUND_PLAN.round.total) break;
      fireAt('r4c', at);
    }
    // bell is due, timer has not fired
    CT.db.prepare('UPDATE paper_rounds SET started_at = ?, ends_at = ? WHERE id = ?')
      .run(Date.now() - comp.ROUND_PLAN.round.total - 300, Date.now() - 300, 'r4c');
    assert.throws(() => comp.abortRound('r4c'), /settled while aborting|published result/,
      'the bell must settle first and the abort be refused');
    assert.strictEqual(CT.q.get.get('r4c').status, 'done');
    assert.strictEqual(comp.standings('r4c', 'final').length, 1, 'and the result exists');
  });

  console.log('\nround four: P1 hardening');
  await ok('a round cannot start on markets that are not competition-ready', () => {
    const id = 'r4r';
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    const real = T.compPriceReady;
    comp.wire({ marketReady: (sym) => sym !== 'SOL' });   // SOL unready
    assert.throws(() => comp.startRound(id), /not competition-ready/,
      'readiness must be a start invariant, not only an advisory preflight');
    comp.wire({ marketReady: () => true });
    comp.abortRound(id, { force: true });
  });
  await ok('a dip between sweeps is captured, because drawdown is per-tick', () => {
    T.db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(A);
    comp.createRound({ id: 'r4dd', candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    comp.startRound('r4dd');
    /* Drawdown work is exposure-driven: an unrelated accepted market tick
       must not scan every seat. Keep a neutral BTC leg open so this remains a
       test of the promised per-tick sampling on a market that can decide the
       account, rather than relying on the retired all-markets hot path. */
    const ep = T.stmt.acctGet.get(A).epoch;
    const openedAt = Date.now();
    T.stmt.posIns.run(A, 'BTC', ep, 'LONG', 0.001, 100, 10,
      openedAt, 100, openedAt, openedAt, 'cross', 0);
    // a dip and full recovery entirely between two 5s sweeps
    T.db.prepare('UPDATE paper_accounts SET balance = 4 WHERE user_id = ?').run(A);
    setMarkRaw('BTC', 100); feedMark('BTC', 100);
    T.tickEval('BTC', { force: true });                 // the tick samples it
    T.db.prepare('UPDATE paper_accounts SET balance = 10 WHERE user_id = ?').run(A);
    T.tickEval('BTC', { force: true });
    const me = comp.playersOf('r4dd').find((p) => p.user_id === A);
    assert.ok(me.max_drawdown >= 5.9,
      'the dip must be recorded even though no sweep ran: got ' + me.max_drawdown);
    comp.abortRound('r4dd', { force: true });
  });
  await ok('a pre-reveal restart without its seed blocks immediately', () => {
    const id = 'r4seed';
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    comp.startRound(id);
    CT._seeds.delete(id);                                // the restart lost it
    comp.resume();
    assert.match(CT.q.get.get(id).blocked_reason || '', /seed lost in restart/,
      'a round that cannot produce a verifiable draw must not keep trading');
    comp.abortRound(id, { force: true });
  });

  /* Round-five regressions. Each of these three is a defect the reviewer found
     by probing the running engine, not by reading my tests, so each one is now
     a permanent test rather than a fix I claim to have made. */


  await ok('a boundary that fires EARLY waits and re-arms, it does not execute', async () => {
    /* A live drill blocked a whole round because its Hot open timer fired 2ms
       before the due instant, so the phase still read `reveal` and the engine
       decided the window had closed. Punctuality is not a fault. */
    const id = 'r5early';
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], backup: 'XRP', players: [{ userId: A, seat: 0 }] });
    comp.startRound(id);
    fireAt(id, comp.ROUND_PLAN.round.reveal);
    // sit 40ms SHORT of the Hot boundary, as an imprecise timer does
    CT.db.prepare('UPDATE paper_rounds SET started_at = ? WHERE id = ?')
      .run(Date.now() - comp.ROUND_PLAN.round.hotStart + 40, id);
    CT.fireBoundary(id, comp.ROUND_PLAN.round.hotStart);
    const r = CT.q.get.get(id);
    /* Early must neither block NOR execute. An earlier version of this test
       asserted that an early boundary opens, which is the wrong property: it
       let a bell arrive 249ms early and freeze the result early. */
    assert.equal(r.blocked_reason, null,
      'firing early must not block the round: ' + r.blocked_reason);
    assert.equal(r.active_hot_base, null,
      'and must not execute early either, or the bell can end a round before time');
    const b = CT.q.bGet.get(id, comp.ROUND_PLAN.round.hotStart);
    assert.ok(!b || b.status !== 'succeeded', 'an unexecuted boundary is not a succeeded one');
    // it re-arms and runs at the due instant
    await new Promise((res) => setTimeout(res, 260));
    assert.ok(CT.q.get.get(id).active_hot_base,
      'the re-armed boundary must actually run once it is due');
    comp.abortRound(id, { force: true });
  });

  await ok('a boundary that arrives after its window really has gone still blocks', () => {
    const id = 'r5late';
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], backup: 'XRP', players: [{ userId: A, seat: 0 }] });
    comp.startRound(id);
    fireAt(id, comp.ROUND_PLAN.round.reveal);
    CT.db.prepare('UPDATE paper_rounds SET started_at = ? WHERE id = ?')
      .run(Date.now() - comp.ROUND_PLAN.round.hotEnd - 1000, id);   // past the window
    CT.fireBoundary(id, comp.ROUND_PLAN.round.hotStart);
    assert.match(CT.q.get.get(id).blocked_reason || '', /window has closed/,
      'loosening the early case must not lose the genuinely-missed case');
    comp.abortRound(id, { force: true });
  });

  await ok('a market that is ready right now but flaky over time cannot host a round', () => {
    const id = 'r5flaky';
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], backup: 'XRP', players: [{ userId: A, seat: 0 }] });
    /* Ready at this instant, and 40% ready across the last ten minutes with a
       38s outage in it: exactly the shape a snapshot check cannot see. */
    comp.wire({ marketReady: () => true,
      marketReliability: (sym) => (sym === 'SOL'
        ? { ratio: 0.40, samples: 120, spanMs: 10 * 60_000, longestGapMs: 38_000 }
        : { ratio: 1, samples: 120, spanMs: 10 * 60_000, longestGapMs: 0 }) });
    assert.throws(() => comp.startRound(id), /not reliably priceable/,
      'a scoring window must not be placed on a market that spends the round unpriceable');
    /* And a market we simply have not watched long enough is not condemned:
       too little history is no opinion, not a failure. */
    comp.wire({ marketReliability: (sym) => (sym === 'SOL'
      ? { ratio: 0.40, samples: 4, spanMs: 20_000, longestGapMs: 38_000 }
      : { ratio: 1, samples: 120, spanMs: 10 * 60_000, longestGapMs: 0 }) });
    comp.startRound(id);
    assert.equal(CT.q.get.get(id).status, 'running', 'a fresh restart must not be able to block every round');
    comp.wire({ marketReady: () => true, marketReliability: null });
    comp.abortRound(id, { force: true });
  });




  await ok('a brief unready blip at Hot start recovers the DRAWN market, it does not swap it', async () => {
    const id = 'r5grace';
    /* Ready at start (a start invariant), then the drawn market blips out at
       exactly the Hot boundary and comes back moments later. */
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], backup: 'XRP', players: [{ userId: A, seat: 0 }] });
    comp.startRound(id);
    fireAt(id, comp.ROUND_PLAN.round.reveal);
    const drawn = CT.q.get.get(id).hot_base;
    let blocked = true;
    comp.wire({ marketReady: (sym) => (blocked && sym === drawn ? false : true) });
    CT.db.prepare('UPDATE paper_rounds SET started_at = ? WHERE id = ?')
      .run(Date.now() - comp.ROUND_PLAN.round.hotStart - 10, id);

    const t0 = Date.now();
    CT.fireBoundary(id, comp.ROUND_PLAN.round.hotStart);
    assert.ok(Date.now() - t0 < 100,
      'the boundary must never block the event loop waiting for a price: took ' + (Date.now() - t0) + 'ms');
    assert.equal(CT.q.get.get(id).active_hot_base, null, 'nothing opens while the gate is down');
    assert.match(CT.q.get.get(id).gate_outage || '', /"hot"/, 'and the outage is recorded, not just logged');

    blocked = false;                              // the blip passes
    await new Promise((r) => setTimeout(r, 700));
    const r = CT.q.get.get(id);
    assert.equal(r.active_hot_base, drawn,
      'the drawn market must be recovered, a swap to the backup rewrites the public draw');
    assert.equal(r.fallback_reason, null, 'and it is not recorded as a fallback');
    assert.match(r.gate_outage || '', /"restoredAt":\d/, 'the outage window is closed once the gate returns');
    comp.wire({ marketReady: () => true });
    comp.abortRound(id, { force: true });
  });

  await ok('Hot that never opens inside its window blocks rather than settling as if it ran', async () => {
    const id = 'r5lost';
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], backup: 'XRP', players: [{ userId: A, seat: 0 }] });
    comp.startRound(id);
    fireAt(id, comp.ROUND_PLAN.round.reveal);             // draw first, at its due time
    comp.wire({ marketReady: () => false });              // then the gate never returns
    CT.db.prepare('UPDATE paper_rounds SET started_at = ? WHERE id = ?')
      .run(Date.now() - comp.ROUND_PLAN.round.hotStart - 10, id);
    CT.fireBoundary(id, comp.ROUND_PLAN.round.hotStart);
    assert.match(CT.q.get.get(id).gate_outage || '', /"hot"/, 'the outage is open');
    /* Jump the clock past the Hot window while the gate is still down. */
    CT.db.prepare('UPDATE paper_rounds SET started_at = ? WHERE id = ?')
      .run(Date.now() - comp.ROUND_PLAN.round.hotEnd - 10, id);
    await new Promise((r) => setTimeout(r, 700));
    assert.match(CT.q.get.get(id).blocked_reason || '', /Hot never opened/,
      'a scoring window that silently did not happen must stop the round');
    comp.wire({ marketReady: () => true });
    comp.abortRound(id, { force: true });
  });

  console.log('\nround six: the reviewer\'s findings');

  await ok('live acceptance and strict acceptance are ONE decision', () => {
    /* A quiet corroborator (fresh primary + agreeing 12s-old second venue)
       admitted an order live, then the checkpoint refused that same instant
       and blocked the round, because the mark stored the FRESH count while the
       live gate counted fresh-plus-corroborating. */
    const S = 'SUI', t = Date.now();
    T.compUpdate(S, 'usdt', 100, t);
    T.compUpdate(S, 'usd', 100, t - 12_000);
    T.live.map.set(S, { markPrice: 100, pythPrice: 100, pythAtMs: t, pythBasis: 0, lastUpdatedMs: t, indexHalt: false });
    T.recordMark(S, 100, t);
    assert.equal(T.compPriceReady(S, t), true, 'precondition: live accepts it');
    assert.equal(T.markAt(S, t, { strict: true }), 100,
      'what live accepted, the checkpoint that decides the result must accept too');
  });




  await ok('a segment that loses its price MID-window records an outage', () => {
    const id = 'r6mid';
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], backup: 'XRP', players: [{ userId: A, seat: 0 }] });
    comp.startRound(id);
    fireAt(id, comp.ROUND_PLAN.round.reveal);
    CT.db.prepare('UPDATE paper_rounds SET started_at = ? WHERE id = ?')
      .run(Date.now() - comp.ROUND_PLAN.round.hotStart - 10, id);
    CT.fireBoundary(id, comp.ROUND_PLAN.round.hotStart);
    const active = CT.q.get.get(id).active_hot_base;
    assert.ok(active, 'precondition: Hot opened');
    assert.equal(CT.q.get.get(id).gate_outage, null, 'and nothing is wrong yet');

    comp.wire({ marketReady: () => false });          // the price goes bad mid-window
    comp.advanceRoundClock(Date.now());               // what the sweep does
    assert.match(CT.q.get.get(id).gate_outage || '', /not competition-valid/,
      'an outage that starts AFTER the opening must still be recorded');

    comp.wire({ marketReady: () => true });
    comp.advanceRoundClock(Date.now());
    assert.match(CT.q.get.get(id).gate_outage || '', /"restoredAt":\d/, 'and closed when it recovers');
    comp.abortRound(id, { force: true });
  });

  await ok('Boost keeps the promised universe separate from what opened', () => {
    const id = 'r6boost';
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], backup: 'XRP', players: [{ userId: A, seat: 0 }] });
    comp.startRound(id);
    fireAt(id, comp.ROUND_PLAN.round.reveal);
    const promised = comp.boostMarketsOf(CT.q.get.get(id));
    /* One market fails; the rest are fine. The promise must survive. */
    comp.wire({ marketReadyForBoost: (s) => s !== promised[promised.length - 1] });
    CT.db.prepare('UPDATE paper_rounds SET started_at = ? WHERE id = ?')
      .run(Date.now() - comp.ROUND_PLAN.round.boostStart - 10, id);
    CT.fireBoundary(id, comp.ROUND_PLAN.round.boostStart);
    const r = CT.q.get.get(id);
    assert.deepStrictEqual(JSON.parse(r.boost_configured), promised,
      'the promised universe is frozen and never rewritten by what opened');
    const opened = JSON.parse(r.boost_opened || '[]');
    assert.ok(!opened.includes(promised[promised.length - 1]), 'the failed market did not open');
    assert.match(r.gate_outage || '', /boost:/, 'and its unavailability is on the record');
    comp.wire({ marketReadyForBoost: () => true });
    comp.abortRound(id, { force: true });
  });

  await ok('an ordinary trader is NOT moved into competition pricing by another round', () => {
    const id = 'r6leak';
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    comp.startRound(id);
    assert.equal(comp.inRound(B, CT.q.get.get(id)), false, 'precondition: B is not seated');
    const m = T.live.map.get('BTC');
    /* Under a single published source, "not competition-valid" means the whole
       CHAIN has gone quiet, not that two of three sources disagree. */
    for (const k of ['usdt', 'usdc', 'usd']) T.compUpdate('BTC', k, 100, Date.now() - 60_000);
    assert.equal(T.compPriceReady('BTC'), false, 'precondition: no live source in the chain');
    assert.ok(Number(T.markFor(m, true, 'BTC', false)) > 0, 'the public keeps a usable mark');
    assert.ok(!Number.isFinite(T.markFor(m, true, 'BTC', true)),
      'and a seated competitor still gets the strict refusal');
    comp.abortRound(id, { force: true });
    for (const k of ['usdt', 'usdc', 'usd']) T.compUpdate('BTC', k, 100, Date.now());
  });


  await ok('reliability is measured in TIME, so a stalled sampler cannot hide an outage', () => {
    /* ready, unready, forty seconds of nothing, ready. Counting samples
       reported "67% ready, longest gap 1s" for a 40s outage. */
    const S = 'ZZTEST';
    const t0 = Date.now() - 41_000;
    T.__setReadyHist(S, [
      { t: t0, ok: true },
      { t: t0 + 1000, ok: false },
      { t: t0 + 41_000, ok: true },
    ]);
    const r = T.readyRatio(S, t0 + 41_000);
    assert.ok(r.longestGapMs >= 40_000,
      'the outage must be reported at its real length, got ' + r.longestGapMs + 'ms');
    assert.ok(r.ratio < 0.1, 'and availability must not look healthy: got ' + r.ratio);
    assert.ok(r.unknownMs >= 39_000, 'the unsampled window is reported as unknown, not as uptime');
  });

  console.log('\nround seven: the reviewer\'s findings');

  await ok('the PRODUCTION ingest path stores the verdict the live gate used', () => {
    /* The regression that "proved" parity last round only did so for a mark
       recorded by hand. compUpdate passed the FRESH count to recordMark while
       the live gate counted fresh-plus-corroborating, so the real path still
       disagreed with itself and blocked rounds on prices it had accepted. */
    const S = 'XLM', now = Date.now();
    T.live.map.set(S, { markPrice: 100, pythPrice: 100, pythAtMs: now, pythBasis: 0, lastUpdatedMs: now, indexHalt: false });
    T.compUpdate(S, 'usd', 100, now - 10_000);   // quiet corroborator, independent venue
    T.compUpdate(S, 'usdt', 100, now);           // fresh primary, drives ingest
    assert.equal(T.compPriceReady(S), true, 'precondition: live accepts it');
    const strict = T.markAt(S, Date.now(), { strict: true });
    assert.ok(Number(strict) > 0,
      'the checkpoint must accept what the production ingest accepted, got ' + strict);
  });

  await ok('a seated trader cannot OPEN on a price the competition has refused', async () => {
    const id = 'r7open';
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    comp.startRound(id);
    for (const k of ['usdt', 'usdc', 'usd']) T.compUpdate('BTC', k, 100, Date.now() - 60_000);
    assert.equal(T.compPriceReady('BTC'), false, 'precondition: the chain has gone quiet');
    assert.equal(T.markOfFreshFor('BTC', true, { userId: A }), null,
      'a competitor must get no price at all, not a fallback they can trade on');
    /* Note what has changed with a single published source: "not
       competition-valid" and "no price at all" are now the SAME state, so the
       public is refused here too. The competitor-only difference that remains
       is the venue-mark fallback, which the leak test above covers: the public
       may be priced off the venue mark, a competitor never is. */
    assert.equal(T.markOfFreshFor('BTC', true, { userId: B }), null,
      'a dead chain prices nobody, competitor or not');
    comp.abortRound(id, { force: true });
    for (const k of ['usdt', 'usdc', 'usd']) T.compUpdate('BTC', k, 100, Date.now());
  });




  await ok('a second outage in the same segment is its own event, and blocks', () => {
    const id = 'r7twice';
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], backup: 'XRP', players: [{ userId: A, seat: 0 }] });
    comp.startRound(id);
    fireAt(id, comp.ROUND_PLAN.round.reveal);
    CT.db.prepare('UPDATE paper_rounds SET started_at = ? WHERE id = ?')
      .run(Date.now() - comp.ROUND_PLAN.round.hotStart - 10, id);
    CT.fireBoundary(id, comp.ROUND_PLAN.round.hotStart);

    comp.wire({ marketReady: () => false }); comp.advanceRoundClock(Date.now());
    comp.wire({ marketReady: () => true });  comp.advanceRoundClock(Date.now());
    comp.wire({ marketReady: () => false }); comp.advanceRoundClock(Date.now());
    const events = JSON.parse(CT.q.get.get(id).gate_outage || '[]');
    assert.equal(events.length, 2,
      'a restored outage must not stop the next one being recorded: got ' + events.length);
    assert.ok(!events[1].restoredAt, 'and the second is still open');
    comp.wire({ marketReady: () => true });
    comp.abortRound(id, { force: true });
  });

  console.log('\none published source, with failover');

  await ok('the index follows the primary and ignores a disagreeing secondary', () => {
    /* Nothing is averaged now, so a second venue sitting 20bps away changes
       nothing: it is not the price and was never promised to be. */
    const S = 'CHIP', t = Date.now();
    T.compUpdate(S, 'usdt', 100, t);
    T.compUpdate(S, 'usd', 100.2, t);
    const a = T.activeSource(S, t);
    assert.equal(a.key, 'usdt', 'the first live source in the chain IS the price');
    assert.equal(a.px, 100, 'a disagreeing secondary does not move it');
  });

  await ok('a silent primary fails over to the next source in the chain', () => {
    const S = 'ENA', t = Date.now();
    T.compUpdate(S, 'usdt', 100, t - 4000);      // primary went quiet
    T.compUpdate(S, 'usd', 100.05, t);
    const a = T.activeSource(S, t);
    assert.equal(a.key, 'usd', 'we follow the next live source rather than stalling');
    assert.equal(a.px, 100.05);
  });

  await ok('a sub-second lull does not move us off the primary', () => {
    const S = 'MET', t = Date.now();
    T.compUpdate(S, 'usdt', 100, t - 800);
    T.compUpdate(S, 'usd', 100.05, t);
    assert.equal(T.activeSource(S, t).key, 'usdt',
      'switching on every brief gap would gap the mark for no reason');
  });

  await ok('a recovered primary must prove itself before we switch back', () => {
    /* Measured live: symmetric thresholds made XRP change source 25 times in
       150s, and every switch gaps the mark by the venue difference. */
    const S = 'XPL', t = Date.now();
    T.compUpdate(S, 'usdt', 100, t - 4000);
    T.compUpdate(S, 'usd', 100.05, t);
    assert.equal(T.activeSource(S, t).key, 'usd', 'precondition: we failed over');
    T.compUpdate(S, 'usdt', 100, t + 1);
    assert.equal(T.activeSource(S, t + 1).key, 'usd',
      'one tick back is not proof of recovery');
    for (let k = 100; k <= 12_000; k += 400) {
      T.compUpdate(S, 'usdt', 100, t + k);
      T.compUpdate(S, 'usd', 100.05, t + k);
    }
    assert.equal(T.activeSource(S, t + 12_000).key, 'usdt',
      'after a sustained recovery we return to the published primary');
  });

  await ok('1000x is available whenever the published source is live', () => {
    const S = 'MORPHO', t = Date.now();
    T.live.map.set(S, { markPrice: 100, pythPrice: 100, pythAtMs: t, pythBasis: 0, lastUpdatedMs: t, indexHalt: false });
    /* ONE source, nothing to corroborate, and STAMPED. Corroboration is not
       required for the headline cap and never was; a measurable observation
       time is, since the round four review. The two are different questions
       and this case is about the first one. */
    T.compUpdate(S, 'usdt', 100, t, t - 50);
    assert.equal(T.readyForLeverage(S, 1000, t), true,
      'the headline cap must not depend on a second venue agreeing');
    assert.equal(T.qualityLeverageCap(S, t), Infinity);
  });

  await ok('the headline cap DOES depend on being able to age the source', () => {
    const S = 'JTO', t = Date.now();
    T.live.map.set(S, { markPrice: 100, pythPrice: 100, pythAtMs: t, pythBasis: 0, lastUpdatedMs: t, indexHalt: false });
    /* No provider timestamp: neither Binance bookTicker nor Coinbase carries
       one, so during a primary outage the engine cannot tell a 20ms quote from
       a two second one. The market keeps trading at the ordinary tier and the
       top tier is refused. */
    T.compUpdate(S, 'usdt', 100, t);
    assert.equal(T.compPriceReady(S, t), true, 'an unageable source still prices the market');
    assert.equal(T.readyForLeverage(S, 100, t), true, 'the ordinary tier keeps trading');
    assert.equal(T.readyForLeverage(S, 500, t), false, 'the top tier is refused');
  });

  await ok('an unageable backup freezes scoring, risk, and pause recovery at 500x', () => {
    const S = 'U500', U = 8110, id = 'r-u500', t = Date.now();
    mkAccount(U);
    T.mktCfg.set(S, {
      tiers: [], maxLev: 500, lotSize: null, takerBps: 0, makerBps: 0,
      maintBps: 50, cancelBps: 0, maxLiqSize: null, status: 'active', isolatedOnly: false,
    });

    /* Begin on the timestamped primary, then make it stale in the source-age
       test clock and let the real selector fail over to timestamp-less USDT.
       Establish this world before the round owns U; otherwise the desired
       500x freeze correctly refuses the failover tick itself. */
    T.compUpdate(S, 'lazer', 100, t, t);
    T.compUpdate(S, 'usdt', 99.7, t);
    T.comps.get(S).lazer.age0 = 10_000;
    T.compUpdate(S, 'usdt', 99.7, t + 1);
    assert.strictEqual(T.activeSource(S, t + 1).key, 'usdt');
    assert.strictEqual(T.comps.get(S).usdt.ageKnown, false,
      'the selected backup has receipt time but no provider observation time');

    feedMark('BTC', 100);
    feedMark('SOL', 100);
    try {
      comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'],
        players: [{ userId: U, displayName: 'High leverage', seat: 0 }] });
      comp.startRound(id);
      const epoch = T.stmt.acctGet.get(U).epoch;
      const boundaryAt = t + 2;
      T.stmt.posIns.run(U, S, epoch, 'LONG', 50, 100, 500,
        boundaryAt, 100, boundaryAt, boundaryAt, 'cross', 0);

      /* Persist the backup observation exactly as settlement would see it.
         It is an ordinary-tier mark, but it can never decide 500x exposure. */
      T.recordMark(S, 99.7, boundaryAt, 1, 0, 'usdt', boundaryAt);
      const hist = T.__markHistory(S);
      const recorded = hist[hist.length - 1];
      assert.strictEqual(recorded.src, 'usdt');
      assert.strictEqual(recorded.leverageCap, 100);
      assert.strictEqual(recorded.boostOk, false);

      const restRank = T.compRankSnapshot();
      assert.strictEqual(restRank.complete, false);
      assert.deepStrictEqual(restRank.players, []);
      assert.ok(restRank.unscored.some((p) => p.userId === U));
      assert.ok(restRank.stalePricing.includes(S));

      const suppliedRank = T.compRankSnapshot(comp.phaseNow(), new Map([[S, 99.7]]));
      assert.strictEqual(suppliedRank.complete, false,
        'a socket-supplied current mark still obeys the held position leverage');
      assert.deepStrictEqual(suppliedRank.players, []);
      assert.ok(suppliedRank.unscored.some((p) => p.userId === U));
      assert.ok(suppliedRank.stalePricing.includes(S));

      assert.strictEqual(T.markAt(S, boundaryAt, { strict: true, forLeverage: 500 }), null);
      assert.throws(() => T.markSetFor([U], boundaryAt, { strict: true }),
        /no boundary mark for U500/,
        'strict settlement must enforce the leverageCap/boostOk stored with the mark');

      /* At 99.7 this 50-unit long is far beyond cross maintenance if the mark
         is wrongly admitted. Both tick risk and the sweep must leave every
         financial row untouched and pause the shared competition instead. */
      const beforePos = T.stmt.posGet.get(U, S);
      const beforeAcct = T.stmt.acctGet.get(U);
      const fillsBefore = T.db.prepare(
        'SELECT COUNT(*) AS n FROM paper_fills WHERE user_id = ? AND symbol = ?').get(U, S).n;
      const risk = T.tickEval(S, { force: true, at: boundaryAt + 1 });
      assert.strictEqual(risk.ok, false);
      assert.ok(risk.error && risk.error.unpriced);
      assert.ok((T.roundPaused().symbols || []).includes(S));
      const pauseBeforeSweep = T.roundPaused();
      const clockBeforeSweep = CT.q.get.get(id);
      const pauseRowsBeforeSweep = T.db.prepare(
        'SELECT symbol, round_id, started_at, restored_at FROM paper_price_pauses WHERE symbol = ? AND round_id = ? ORDER BY started_at'
      ).all(S, id);
      assert.strictEqual(pauseRowsBeforeSweep.length, 1);
      assert.strictEqual(pauseRowsBeforeSweep[0].restored_at, null);
      assert.ok(clockBeforeSweep.paused_since,
        'the shared pause freezes the authoritative round clock');
      assert.deepStrictEqual(T.stmt.posGet.get(U, S), beforePos,
        'tick risk cannot mark or liquidate on the lower-tier backup');
      assert.deepStrictEqual(T.stmt.acctGet.get(U), beforeAcct);
      assert.strictEqual(T.db.prepare(
        'SELECT COUNT(*) AS n FROM paper_fills WHERE user_id = ? AND symbol = ?').get(U, S).n,
      fillsBefore);

      T.sweep();
      const pauseRowsAfterSweep = T.db.prepare(
        'SELECT symbol, round_id, started_at, restored_at FROM paper_price_pauses WHERE symbol = ? AND round_id = ? ORDER BY started_at'
      ).all(S, id);
      const clockAfterSweep = CT.q.get.get(id);
      assert.deepStrictEqual(pauseRowsAfterSweep, pauseRowsBeforeSweep,
        'sweep recovery must not restore then reopen the same still-unscoreable obligation');
      assert.deepStrictEqual(T.roundPaused(), pauseBeforeSweep,
        'the same shared pause remains authoritative throughout the sweep');
      assert.strictEqual(clockAfterSweep.paused_since, clockBeforeSweep.paused_since,
        'the clock never resumes on a mark below the held leverage tier');
      assert.strictEqual(clockAfterSweep.paused_total_ms, clockBeforeSweep.paused_total_ms,
        'no hidden active-time interval is accrued between clear and re-pause');
      assert.deepStrictEqual(T.stmt.posGet.get(U, S), beforePos,
        'the sweep obeys the same frozen risk decision');
      assert.deepStrictEqual(T.stmt.acctGet.get(U), beforeAcct);
    } finally {
      try { comp.abortRound(id, { force: true }); } catch {}
      T.db.prepare('DELETE FROM paper_positions WHERE user_id = ? AND symbol = ?').run(U, S);
      T.__clearPauses();
    }
  });

  await ok('a dead chain prices nothing and carries no leverage', () => {
    const S = 'RENDER', t = Date.now();
    T.live.map.set(S, { markPrice: 100, pythPrice: 100, pythAtMs: t - 120_000, pythBasis: 0, lastUpdatedMs: t - 120_000, indexHalt: false });
    for (const k of ['usdt', 'usd', 'usdc']) T.compUpdate(S, k, 100, t - 120_000);
    assert.equal(T.compPriceReady(S, t), false);
    assert.equal(T.qualityLeverageCap(S, t), 0);
  });

  await ok('a mark records WHICH source produced it', () => {
    const S = 'VIRTUAL', t = Date.now();
    T.live.map.set(S, { markPrice: 100, pythPrice: 100, pythAtMs: t, pythBasis: 0, lastUpdatedMs: t, indexHalt: false });
    T.compUpdate(S, 'usdt', 100, t);
    T.recordMark(S, 100, t);
    const h = T.__markHistory(S);
    assert.equal(h[h.length - 1].src, 'usdt',
      'a liquidation has to be explainable as "this price came from this venue"');
  });

  console.log('\npyth lazer as the published primary');

  /* Fresh symbols throughout: activeSource is sticky by design, so a symbol
     another case has already pinned to a source will not move, and the test
     would be measuring leftover state instead of the rule. */

  await ok('lazer is preferred over binance when it is the live primary', () => {
    const S = 'LZTESTA', t = Date.now();
    T.compUpdate(S, 'lazer', 100.02, t, t);         // primary arrives first
    T.compUpdate(S, 'usdt', 100, t);             // binance alongside
    const r = T.activeSource(S, t);
    assert.equal(r.key, 'lazer', 'the published primary holds the market');
    assert.equal(r.px, 100.02, 'and binance does not move the price');
  });

  await ok('an incumbent binance is NOT displaced the instant lazer appears', () => {
    /* Promotion is deliberately slow. A source that has just come back has
       not proved anything yet, and switching gaps the mark by the 1.4-2.1bps
       the two feeds differ by, which at 1000x is a third of margin. */
    const S = 'LZTESTE', t = Date.now();
    T.compUpdate(S, 'usdt', 100, t);
    assert.equal(T.activeSource(S, t).key, 'usdt', 'precondition: binance is incumbent');
    T.compUpdate(S, 'lazer', 100.02, t, t);
    assert.equal(T.activeSource(S, t).key, 'usdt', 'one tick is not proof of recovery');
    for (let k = 0; k <= 12_000; k += 200) {
      T.compUpdate(S, 'lazer', 100.02, t + k, t + k);
      T.compUpdate(S, 'usdt', 100, t + k);
    }
    assert.equal(T.activeSource(S, t + 12_000).key, 'lazer',
      'but a sustained lazer does take the primary slot back');
  });

  await ok('lazer is judged stale in 600ms, binance in 4s', () => {
    /* Different feeds, different normal. Binance quiet periods reach 2.96s on
       a healthy feed. Lazer publishes every 50ms but its stream has a
       measured jitter tail of ~260ms (twice a minute, every symbol at once),
       so the old 250ms bar sat on that tail and called jitter a fault. 600ms
       is a dozen missed ticks, clear of the tail, still under a second. */
    const t = Date.now();
    const S = 'LZTESTB';
    T.compUpdate(S, 'lazer', 100, t - 800, t - 800);      // 16 missed lazer ticks: dead
    T.compUpdate(S, 'usdt', 100.05, t - 2000);   // 2s: still fine for binance
    assert.equal(T.activeSource(S, t).key, 'usdt',
      'a dead lazer must not hold the market for a binance-length timeout');
    const S2 = 'LZTESTC';
    T.compUpdate(S2, 'lazer', 100, t - 400, t - 400);     // inside the measured jitter tail, still ours
    T.compUpdate(S2, 'usdt', 100.05, t);
    assert.equal(T.activeSource(S2, t).key, 'lazer',
      'and a single missed tick must not flap us off it');
  });

  await ok('a live round returns to Lazer only after its sustained recovery window', () => {
    const S = 'LZTESTD', id = 'r8prom', t0 = Date.now();
    T.compUpdate(S, 'usdt', 100, t0);            // pins the active source to binance
    assert.equal(T.activeSource(S, t0).key, 'usdt', 'precondition: we start on binance');
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    try {
      comp.startRound(id);
      for (let k = 0; k < 10_000; k += 200) {
        T.compUpdate(S, 'lazer', 100.02, t0 + k, t0 + k);
        T.compUpdate(S, 'usdt', 100, t0 + k);
        assert.equal(T.activeSource(S, t0 + k).key, 'usdt',
          'one tick or an incomplete recovery streak cannot promote Lazer');
      }
      T.compUpdate(S, 'lazer', 100.02, t0 + 10_000, t0 + 10_000);
      T.compUpdate(S, 'usdt', 100, t0 + 10_000);
      assert.equal(T.activeSource(S, t0 + 10_000).key, 'lazer',
        'the primary returns at the existing 10-second window even while its backup is fresh');
      assert.equal(T.activeSource(S, t0 + 10_000).px, 100.02);
      assert.equal(comp.currentRound().id, id, 'promotion did not require ending the round');
    } finally { comp.abortRound(id, { force: true }); }
  });

  await ok('a Lazer freshness lapse resets the live-round failback streak', () => {
    const S = 'LZTESTLAPSE', id = 'r8prom-lapse', t0 = Date.now();
    T.compUpdate(S, 'usdt', 100, t0);
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    try {
      comp.startRound(id);
      for (let k = 0; k <= 9800; k += 200) {
        T.compUpdate(S, 'lazer', 100.02, t0 + k, t0 + k);
        T.compUpdate(S, 'usdt', 100, t0 + k);
      }
      const returnedAt = t0 + 10_600; // 800ms exceeds Lazer's 600ms source budget.
      T.compUpdate(S, 'lazer', 100.02, returnedAt, returnedAt);
      T.compUpdate(S, 'usdt', 100, returnedAt);
      assert.equal(T.comps.get(S).lazer.freshSince, returnedAt);
      assert.equal(T.activeSource(S, returnedAt).key, 'usdt',
        'elapsed wall time cannot substitute for continuous source freshness');
      for (let k = 200; k < 10_000; k += 200) {
        T.compUpdate(S, 'lazer', 100.02, returnedAt + k, returnedAt + k);
        T.compUpdate(S, 'usdt', 100, returnedAt + k);
      }
      assert.equal(T.activeSource(S, returnedAt + 9800).key, 'usdt');
      T.compUpdate(S, 'lazer', 100.02, returnedAt + 10_000, returnedAt + 10_000);
      T.compUpdate(S, 'usdt', 100, returnedAt + 10_000);
      assert.equal(T.activeSource(S, returnedAt + 10_000).key, 'lazer',
        'a new complete streak restores promotion permission');
    } finally { comp.abortRound(id, { force: true }); }
  });

  await ok('a live round retains the 60-second failback window for slow-tier Lazer', () => {
    // ANSEM has the configured f200 tier and no earlier component fixture.
    const S = 'ANSEM', id = 'r8prom-slow', t0 = Date.now();
    T.compUpdate(S, 'usdt', 100, t0);
    assert.equal(T.activeSource(S, t0).key, 'usdt');
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    try {
      comp.startRound(id);
      for (let k = 0; k < 60_000; k += 500) {
        T.compUpdate(S, 'lazer', 100.02, t0 + k, t0 + k);
        T.compUpdate(S, 'usdt', 100, t0 + k);
        assert.equal(T.activeSource(S, t0 + k).key, 'usdt',
          'the fast-tier window must not shorten a slow-tier recovery');
      }
      T.compUpdate(S, 'lazer', 100.02, t0 + 60_000, t0 + 60_000);
      T.compUpdate(S, 'usdt', 100, t0 + 60_000);
      assert.equal(T.activeSource(S, t0 + 60_000).key, 'lazer');
    } finally { comp.abortRound(id, { force: true }); }
  });

  await ok('live rounds still block other optional promotions while non-live rounds allow them', () => {
    const S = 'LZTESTVENUE', id = 'r8prom-venue', t0 = Date.now();
    T.compUpdate(S, 'usd', 100, t0);
    assert.equal(T.activeSource(S, t0).key, 'usd');
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    try {
      comp.startRound(id);
      for (let k = 0; k <= 12_000; k += 200) {
        T.compUpdate(S, 'usdt', 100.02, t0 + k, t0 + k);
        T.compUpdate(S, 'usd', 100, t0 + k);
      }
      assert.equal(T.activeSource(S, t0 + 12_000).key, 'usd',
        'the live-round exception belongs only to Lazer, not every higher-priority source');
    } finally { comp.abortRound(id, { force: true }); }
    assert.equal(T.activeSource(S, t0 + 12_000).key, 'usdt',
      'the ordinary non-live chain promotion remains unchanged');
  });

  await ok('an expired incumbent still fails over live without a Lazer recovery streak', () => {
    const S = 'LZTESTEXPIRE', id = 'r8prom-expiry', t0 = Date.now();
    T.compUpdate(S, 'usdt', 100, t0);
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    try {
      comp.startRound(id);
      const at = t0 + 5000;
      T.compUpdate(S, 'lazer', 100.02, at, at);
      assert.equal(T.comps.get(S).lazer.freshSince, at);
      assert.equal(T.activeSource(S, at).key, 'lazer',
        'mandatory failover cannot wait for the optional-promotion window');
    } finally { comp.abortRound(id, { force: true }); }
  });

  console.log('\nplayer avatars on the wall');

  await ok('an avatar is stored with the ROUND, not looked up live', () => {
    const id = 'avA';
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], players: [
      { userId: A, seat: 0, displayName: 'Ada', avatarUrl: 'https://pbs.twimg.com/x.jpg' },
      { userId: B, seat: 1, displayName: 'Bo' },
    ] });
    const rows = comp.playersOf(id);
    const a = rows.find((r) => r.user_id === A);
    assert.equal(a.avatar_url, 'https://pbs.twimg.com/x.jpg',
      'a face that changes on X mid-show must not change the round record');
    assert.equal(rows.find((r) => r.user_id === B).avatar_url, null,
      'and a seat with no avatar is simply null, not an error');
    comp.abortRound(id, { force: true });
  });

  await ok('a site-relative upload path is accepted', () => {
    const id = 'avB';
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], players: [
      { userId: A, seat: 0, avatarUrl: '/data/comp-avatars/abc123.jpg' },
      { userId: B, seat: 1 },
    ] });
    assert.equal(comp.playersOf(id).find((r) => r.user_id === A).avatar_url,
      '/data/comp-avatars/abc123.jpg');
    comp.abortRound(id, { force: true });
  });

  await ok('an avatar url that could inject into the wall is refused', () => {
    /* This string is rendered into a public broadcast page, so the operator
       desk is not treated as a trusted source of markup. */
    for (const bad of ['javascript:alert(1)', 'data:image/png;base64,AAAA', '//evil.example/x.jpg', 'http://insecure/x.jpg']) {
      assert.throws(
        () => comp.createRound({ id: 'avbad', candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0, avatarUrl: bad }] }),
        /avatar url/,
        'must refuse: ' + bad,
      );
    }
    // and an over-long one, which is a different failure mode: record bloat
    assert.throws(
      () => comp.createRound({ id: 'avbad2', candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0, avatarUrl: 'https://x.com/' + 'a'.repeat(600) }] }),
      /too long/,
    );
  });

  await ok('the wall feed carries the avatar for every seat', () => {
    const id = 'avC';
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], players: [
      { userId: A, seat: 0, displayName: 'Ada', avatarUrl: 'https://pbs.twimg.com/a.jpg' },
      { userId: B, seat: 1, displayName: 'Bo' },
    ] });
    comp.startRound(id);
    const st = P.__test ? null : null;
    const rows = comp.playersOf(id);
    assert.equal(rows.length, 2);
    // the shape the wall consumes is asserted in test-comp-api against compState
    assert.ok('avatar_url' in rows[0], 'the roster row exposes the column the wall reads');
    comp.abortRound(id, { force: true });
  });

  console.log('\nround nine: source selection and the clamp');

  await ok('no source live under its OWN deadline means nothing is priceable', () => {
    /* Lazer 1s dead (250ms rule), Binance 4.5s dead (4s rule). The old code
       fell back to the freshest stale record and a generic 5s window called
       it healthy, so the published rule and actual eligibility diverged in
       exactly the failure state the chain exists to define. */
    const S = 'LZP1', now = Date.now();
    T.compUpdate(S, 'lazer', 100, now - 1000, now - 1000);
    T.compUpdate(S, 'usdt', 100, now - 4500);
    assert.equal(T.activeSource(S, now), null, 'no source is live under its own rule');
    assert.equal(T.compPriceReady(S, now), false);
    assert.equal(T.qualityLeverageCap(S, now), 0, 'and it carries no leverage');
  });

  await ok('source identity and the executable mark can never disagree', () => {
    const S = 'LZP2', now = Date.now();
    T.live.map.set(S, { markPrice: 100, pythPrice: 100, pythAtMs: now, pythBasis: 0, lastUpdatedMs: now, indexHalt: false });
    T.compUpdate(S, 'lazer', 100, now - 800, now - 800);      // dead under the 600ms rule
    /* The backup's last tick must land while lazer was still inside its bar
       (500ms old at that instant), so the switch has NOT been published yet
       and the selector and the published mark genuinely disagree at `now`.
       Landing it later would publish the switch atomically and there would
       be no disagreement left to assert on. */
    T.compUpdate(S, 'usdt', 100.20, now - 300);    // live
    const act = T.activeSource(S, now);
    assert.equal(act.key, 'usdt', 'the chain has moved to the backup');
    /* Until the switch is published nothing may trade: the selector saying
       Binance while live.map still held Lazer's price was a stale-price
       arbitrage window. */
    assert.equal(T.compPriceReady(S, now), false, 'not priceable while identity and mark disagree');
    // and the next tick from any source publishes the transition atomically
    T.compUpdate(S, 'usdt', 100.20, now);
    const m = T.live.map.get(S);
    assert.equal(m.srcKey, 'usdt');
    assert.equal(m.pythPrice, 100.20, 'the new source price is published WITH the switch');
    assert.equal(T.compPriceReady(S, now), true);
  });

  await ok('the clamp runs on the followed source however many backups are fresh', () => {
    /* Gated on `nComps < 3`, which belonged to the blended index. On the
       majors three fresh records are normal, so the only advertised wick
       defence was absent exactly where 1000x is used. */
    const S = 'LZP3', now = Date.now();
    T.live.map.set(S, { markPrice: 100, pythPrice: 100, pythAtMs: now, pythBasis: 0, lastUpdatedMs: now, indexHalt: false });
    for (const k of ['lazer', 'usdt', 'usd']) T.compUpdate(S, k, 100, now, now);
    assert.equal(T.compQuality(S, now).n, 3, 'precondition: three fresh components');
    T.compUpdate(S, 'lazer', 99.20, now + 1, now + 1);      // 80bps, over the clamp
    assert.equal(T.live.map.get(S).pythPrice, 100, 'the jump is held, not priced');
    assert.equal(T.confirming.has(S), true);
  });

  await ok('a symbol mid-confirmation is not priceable by anything', () => {
    /* Holding the number while leaving the market open let a trader enter at
       the stale mark and collect the entire withheld step when it confirmed:
       measured at 1000x that was 600% of committed margin, manufactured by
       the hold rather than by predicting anything. */
    const S = 'LZP4', now = Date.now();
    T.live.map.set(S, { markPrice: 100, pythPrice: 100, pythAtMs: now, pythBasis: 0, lastUpdatedMs: now, indexHalt: false });
    T.compUpdate(S, 'lazer', 100, now, now);
    T.compUpdate(S, 'lazer', 100.60, now + 1, now + 1);     // 60bps jump
    assert.equal(T.confirming.has(S), true, 'precondition: confirming');
    assert.equal(T.compPriceReady(S), false, 'no readiness');
    assert.equal(T.markAt(S, now + 2, { strict: true }), null, 'no checkpoint price');
    assert.ok(!Number.isFinite(T.markFor(T.live.map.get(S), true, S, true)), 'no competitor mark');
    assert.equal(T.qualityLeverageCap(S), 0, 'and no leverage');
  });

  await ok('confirmation cannot walk: the cluster is fixed, not rolling', () => {
    /* Each held tick used to become the new anchor, so ten ticks 29bps apart
       counted as a 30bps agreement and carried the mark 325bps away from the
       level that was proposed. */
    /* Since 2026-09-11 a staircase that keeps going ONE way is a market
       moving, and is accepted as a run after the streak, at its latest print,
       as long as the whole walk stays inside the per-tick ceiling. A walk
       beyond the ceiling is still refused. */
    const S = 'LZP5', now = Date.now();
    T.live.map.set(S, { markPrice: 100, pythPrice: 100, pythAtMs: now, pythBasis: 0, lastUpdatedMs: now, indexHalt: false });
    T.compUpdate(S, 'lazer', 100, now, now);
    let px = 100.60;
    for (let i = 0; i < 9; i++) { T.compUpdate(S, 'lazer', px, now + 1 + i, now + 1 + i); px *= 1.0029; }
    assert.equal(T.live.map.get(S).pythPrice, 100, 'nine steps of a staircase are still held');
    assert.equal(T.confirming.has(S), true);
    T.compUpdate(S, 'lazer', px, now + 10, now + 10);
    assert.ok(Math.abs(T.live.map.get(S).pythPrice - px) < 1e-9, 'the tenth step accepts the run at its latest print');
    assert.equal(T.confirming.has(S), false);
    /* A walk that travels past the ceiling never lands. */
    const S2 = 'LZP5W', t2 = now + 100;
    T.live.map.set(S2, { markPrice: 100, pythPrice: 100, pythAtMs: t2, pythBasis: 0, lastUpdatedMs: t2, indexHalt: false });
    T.compUpdate(S2, 'lazer', 100, t2, t2);
    let q = 100.60;
    for (let i = 0; i < 12; i++) { T.compUpdate(S2, 'lazer', q, t2 + 1 + i, t2 + 1 + i); q *= 1.006; }
    assert.equal(T.live.map.get(S2).pythPrice, 100, 'a staircase past the ceiling is not accepted as a run');
    assert.equal(T.confirming.has(S2), true);
  });

  await ok('a strict observation expires with its own source, not a generic window', () => {
    /* Accepted at t0, then every source dies. At t0+6s the live engine knew
       the market was gone; settlement did not, and used the dead price. */
    const S = 'LZP6', t0 = Date.now();
    T.live.map.set(S, { markPrice: 100, pythPrice: 100, pythAtMs: t0, pythBasis: 0, lastUpdatedMs: t0, indexHalt: false });
    T.compUpdate(S, 'lazer', 100, t0, t0);
    assert.equal(T.markAt(S, t0 + 100, { strict: true }), 100, 'valid inside the source window');
    assert.equal(T.markAt(S, t0 + 6000, { strict: true }), null,
      'and refused once that source could no longer have been live');
  });

  await ok('the followed source survives a restart', () => {
    /* Memory-only selection meant a bounce re-chose by reconnect order.
       Durable selection avoids that accidental switch and recovery delay. */
    const S = 'LZP7', now = Date.now();
    T.compUpdate(S, 'usdt', 100, now);
    const row = T.db.prepare('SELECT source FROM paper_index_source WHERE symbol = ?').get(S);
    assert.ok(row && row.source === 'usdt', 'the choice is durable, not just in memory');
  });

  await ok('a backup disagreeing does not freeze a healthy named source', () => {
    /* The cross-venue halt is a blended-index guard: under named-source
       pricing it froze a healthy primary because a backup drifted. */
    const S = 'LZP8', now = Date.now();
    T.live.map.set(S, { markPrice: 100, pythPrice: 100, pythAtMs: now, pythBasis: 0, lastUpdatedMs: now, indexHalt: false });
    T.compUpdate(S, 'lazer', 100, now, now);
    T.compUpdate(S, 'usd', 100.30, now);          // backup 30bps away
    assert.equal(T.live.map.get(S).indexHalt, false, 'the followed source is healthy');
    assert.equal(T.compPriceReady(S, now), true);
  });

  console.log('\nround ten: source time, ownership and segment sampling');

  await ok('failover publishes the source\'s own timestamp, not our callback\'s', () => {
    /* Stamping `now` on a quote that was already 3.9s old handed it a fresh
       lifetime, so strict history honoured a dead source long after live
       readiness had moved on. */
    const S = 'R10A', t0 = Date.now();
    T.live.map.set(S, { markPrice: 100, pythPrice: 100, pythAtMs: t0, pythBasis: 0, lastUpdatedMs: t0, indexHalt: false });
    T.compUpdate(S, 'usdt', 100, t0 - 3900);      // old backup quote
    T.compUpdate(S, 'usd', 100, t0);              // another source wakes the selector
    const h = T.__markHistory(S) || [];
    const last = h[h.length - 1];
    if (last && last.src === 'usdt') {
      assert.ok(last.validUntil <= (t0 - 3900) + 4000 + 5,
        'expiry must be measured from when the SOURCE spoke, not when we wrote it down');
    }
  });

  await ok('an armed future round cannot block the running one', () => {
    /* accountLocked() covers users seated on ARMED rounds so their accounts
       cannot be reset. Right for reset protection, far too broad for deciding
       the live result is compromised. */
    const live = 'r10live', future = 'r10future';
    comp.createRound({ id: live, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    comp.startRound(live);
    comp.createRound({ id: future, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: B, seat: 0 }] });
    assert.equal(comp.accountLocked(B), true, 'B is protected from reset by the armed round');
    assert.equal(T.competitionOwned(B), false,
      'but B is NOT part of the running result, so B cannot block it');
    assert.equal(T.competitionOwned(A), true, 'the actual seated player is');
    comp.abortRound(future, { force: true });
    comp.abortRound(live, { force: true });
  });

  await ok('a competitor gets no fallback mark, so a frozen leg cannot fund another trade', () => {
    /* posMarkOf fell back to last_mark or entry, so freezing a symbol did not
       freeze the account: a stale profitable leg still counted as cross
       collateral elsewhere. */
    const id = 'r10cross', S = 'R10B';
    T.live.map.set(S, { markPrice: 100, pythPrice: 100, pythAtMs: Date.now(), pythBasis: 0, lastUpdatedMs: Date.now(), indexHalt: false });
    T.mktCfg.set(S, { tiers: [], maxLev: 1000, lotSize: null, takerBps: 0, makerBps: 0, maintBps: 5000, cancelBps: 7500, maxLiqSize: null, status: 'active', isolatedOnly: false });
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    comp.startRound(id);
    const ep = T.stmt.acctGet.get(A).epoch;
    T.stmt.posIns.run(A, S, ep, 'LONG', 1, 100, 10, Date.now(), 100, Date.now(), Date.now(), 'cross', 0);
    // freeze the symbol
    T.compUpdate(S, 'usdt', 100, Date.now());
    T.compUpdate(S, 'usdt', 100.60, Date.now() + 1);
    assert.equal(T.confirming.has(S), true, 'precondition: the symbol is frozen');
    assert.throws(() => T.accountRisk(A, T.stmt.acctGet.get(A)), /no competition-valid mark/,
      'a scored account with an unpriceable leg has no risk answer at all');
    T.db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(A);
    comp.abortRound(id, { force: true });
  });

  await ok('strict and live agree at the exact expiry instant', () => {
    const S = 'R10C', t0 = Date.now();
    T.live.map.set(S, { markPrice: 100, pythPrice: 100, pythAtMs: t0, pythBasis: 0, lastUpdatedMs: t0, indexHalt: false });
    T.compUpdate(S, 'usdt', 100, t0);
    const h = T.__markHistory(S) || [];
    const last = h[h.length - 1];
    if (last && Number.isFinite(last.validUntil)) {
      assert.equal(T.markAt(S, last.validUntil, { strict: true }), null,
        'live uses `age < deadline`, so strict must reject AT the deadline too');
      assert.ok(Number(T.markAt(S, last.validUntil - 1, { strict: true })) > 0,
        'and accept the instant before it');
    }
  });

  console.log('\nround eleven: shared pause, ownership, fail-closed sampling');

  await ok('an unpriceable leg pauses the ROUND, it does not shield one seat', () => {
    /* Pausing only the affected account let a contestant holding a tiny
       unpriceable leg escape a liquidation that hit an identical rival on the
       same tick. */
    const S = 'R11A', id = 'r11shield', now = Date.now();
    T.mktCfg.set(S, { tiers: [], maxLev: 1000, lotSize: null, takerBps: 0, makerBps: 0, maintBps: 5000, cancelBps: 7500, maxLiqSize: null, status: 'active', isolatedOnly: false });
    T.live.map.set(S, { markPrice: 100, pythPrice: 100, pythAtMs: now, pythBasis: 0, lastUpdatedMs: now, indexHalt: false });
    T.compUpdate(S, 'usdt', 100, now);
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }, { userId: B, seat: 1 }] });
    comp.startRound(id);
    const ep = T.stmt.acctGet.get(A).epoch;
    T.stmt.posIns.run(A, S, ep, 'LONG', 0.001, 100, 10, now, 100, now, now, 'cross', 0);
    T.compUpdate(S, 'usdt', 100.60, now + 1);          // unconfirmable jump
    assert.equal(T.confirming.has(S), true, 'precondition: the symbol is frozen');
    assert.throws(() => T.accountRisk(A, T.stmt.acctGet.get(A)), /no competition-valid mark/);
    T.competitionOwned(A);
    // the pause is round-wide, not per seat
    try { T.accountRisk(A, T.stmt.acctGet.get(A)); } catch (e) { T.__pauseFor(e); }
    assert.ok(T.roundPaused(), 'an unpriceable contestant leg pauses everyone');
    T.db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(A);
    comp.abortRound(id, { force: true });
  });

  await ok('a market the round is NOT decided on freezes alone, it does not stop the field', () => {
    /* 2026-09-03: a contestant held MET, which is quiet for more than four
       seconds 19% of the time, and every one of those quiet spells paused the
       whole practice round and froze the boost markets and the clock. The
       shared pause exists so nobody escapes a liquidation a rival takes on the
       same tick; that reasoning covers the markets the round is decided on and
       nothing else. */
    const S = 'R11THIN', id = 'r11thin', now = Date.now();
    T.mktCfg.set(S, { tiers: [], maxLev: 20, lotSize: null, takerBps: 0, makerBps: 0, maintBps: 50, cancelBps: 0, maxLiqSize: null, status: 'active', isolatedOnly: false });
    T.live.map.set(S, { markPrice: 100, pythPrice: 100, pythAtMs: now, pythBasis: 0, lastUpdatedMs: now, indexHalt: false });
    T.compUpdate(S, 'usdt', 100, now);
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }, { userId: B, seat: 1 }] });
    comp.startRound(id);
    assert.equal(T.roundMarkets().has(S), false, 'precondition: the round is not decided on this market');
    /* ISOLATED, because that is the whole condition. A cross leg shares the
       account's pot, so an unpriceable one makes that contestant's equity
       unknown and skipping their risk pass would shield them from a
       liquidation a rival takes on the same tick. Only a ring-fenced position
       can be frozen on its own. Both directions are asserted below. */
    const ep2 = T.stmt.acctGet.get(A).epoch;
    T.stmt.posIns.run(A, S, ep2, 'LONG', 0.001, 100, 10, now, 100, now, now, 'isolated', 1);
    /* Genuinely unpriceable, not merely declared so: the freeze clears itself
       the moment the market can be priced again, which is the point of it. */
    T.compUpdate(S, 'usdt', 100.60, now + 1);          // unconfirmable jump
    assert.equal(T.compPriceReady(S), false, 'precondition: the market really cannot be priced');
    T.__onSweepError(A, 'test', Object.assign(new Error(`no competition-valid mark for ${S}`), { unpriced: true, symbol: S }));
    assert.ok(!T.roundPaused(), 'an ISOLATED leg freezes alone and the field keeps trading');
    assert.ok(T.marketFrozen(S), 'and that one market is frozen');
    T.__clearPauseIfPriceable();
    assert.ok(!T.roundPaused(), 'shared-pause recovery leaves an isolated market freeze isolated');
    assert.ok(T.marketFrozen(S), 'and does not thaw it without a valid market price');
    /* The same market, held CROSS, is the round's problem again: that account
       cannot be evaluated at all, so nobody can be. */
    T.db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(A);
    T.stmt.posIns.run(A, S, ep2, 'LONG', 0.001, 100, 10, now, 100, now, now, 'cross', 0);
    T.__onSweepError(A, 'test', Object.assign(new Error(`no competition-valid mark for ${S}`), { unpriced: true, symbol: S }));
    assert.ok(T.roundPaused(), 'a CROSS leg pauses the field, whatever the market');
    T.db.prepare('DELETE FROM paper_price_pauses').run();
    T.invalidatePause ? T.invalidatePause() : null;
    T.db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(A);
    comp.abortRound(id, { force: true });
  });

  await ok('an armed future round cannot block the running one, in onSweepError too', () => {
    /* competitionOwned was corrected last round; onSweepError kept its own
       accountLocked call, so the hole stayed open through the path that
       actually blocks rounds. */
    const live = 'r11live', future = 'r11future';
    comp.createRound({ id: live, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    comp.startRound(live);
    comp.createRound({ id: future, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: B, seat: 0 }] });
    assert.equal(comp.accountLocked(B), true, 'B is reset-protected by the armed round');
    T.__onSweepError(B, 'test path', new Error('injected'));
    assert.equal(CT.q.get.get(live).blocked_reason, null,
      'a failure on a future-round outsider must not block the live round');
    T.__onSweepError(A, 'test path', new Error('injected'));
    assert.match(CT.q.get.get(live).blocked_reason || '', /injected/,
      'but a failure on the actual seated player must');
    comp.clearBlock(live, { note: 'test' });
    comp.abortRound(future, { force: true });
    comp.abortRound(live, { force: true });
  });
  console.log('\nround thirteen: the pause is authoritative, the barrier is first');

  await ok('a shared pause bars contestant TRADING, not only automatic risk', async () => {
    /* Freezing liquidation while leaving discretionary orders open handed the
       field downside protection and kept their upside. If nobody can be
       liquidated, nobody may trade. */
    T.__clearPauses();   // an earlier case leaves its own obligation open
    const S = 'R13A', id = 'r13pause', now = Date.now();
    T.mktCfg.set(S, { tiers: [], maxLev: 500, lotSize: null, takerBps: 0, makerBps: 0, maintBps: 5000, cancelBps: 7500, maxLiqSize: null, status: 'active', isolatedOnly: false });
    T.live.map.set(S, { markPrice: 100, pythPrice: 100, pythAtMs: now, pythBasis: 0, lastUpdatedMs: now, indexHalt: false });
    T.compUpdate(S, 'usdt', 100, now);
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    comp.startRound(id);
    T.compUpdate(S, 'usdt', 100.6, now + 1);            // unconfirmable jump
    T.__pauseFor({ symbol: S, message: 'test freeze' });
    assert.ok(T.roundPaused(), 'precondition: the round is paused');
    const r = await order(A, { symbol: 'BTC', side: 'BUY', type: 'MARKET', size: 0.001, leverage: 10 });
    assert.strictEqual(r.code, 409, JSON.stringify(r.body));
    assert.strictEqual(r.body.error, 'competition_paused');
    assert.deepStrictEqual(r.body.symbols, [S], 'and it says which symbol is unpriceable');
    comp.abortRound(id, { force: true });
    T.__clearPauses();
  });

  await ok('one symbol recovering cannot clear another symbol\'s pause', () => {
    /* The pause was ONE slot: a second unpriceable symbol overwrote the first,
       so recovering the second cleared a pause the first still justified. */
    T.__clearPauses();
    T.__pauseFor({ symbol: 'R13X', message: 'x down' });
    T.__pauseFor({ symbol: 'R13Y', message: 'y down' });
    const p = T.roundPaused();
    assert.strictEqual(p.count, 2, 'both obligations are held');
    assert.deepStrictEqual(p.symbols.slice().sort(), ['R13X', 'R13Y']);
    T.__restorePause('R13Y');
    const after = T.roundPaused();
    assert.ok(after, 'the round stays paused while R13X is unresolved');
    assert.deepStrictEqual(after.symbols, ['R13X']);
    T.__restorePause('R13X');
    assert.strictEqual(T.roundPaused(), null, 'and clears only when every one is resolved');
  });

  await ok('the pause survives a restart, because it is not process state', () => {
    T.__clearPauses();
    T.__pauseFor({ symbol: 'R13Z', message: 'durable' });
    const rows = T.db.prepare('SELECT symbol, restored_at FROM paper_price_pauses WHERE restored_at IS NULL').all();
    assert.deepStrictEqual(rows.map((r) => r.symbol), ['R13Z'],
      'an obligation nobody wrote down is one a restart forgets');
    T.__clearPauses();
  });

  await ok('normalising candidates happens BEFORE counting them', () => {
    assert.throws(() => comp.createRound({ id: 'r13c1', candidates: ['   ', ''], players: [{ userId: A, seat: 0 }] }),
      /at least 3 eligible Hot markets/, 'blank candidates armed a round that died at the reveal');
    assert.throws(() => comp.createRound({ id: 'r13c2', candidates: ['BTC', '  '], players: [{ userId: A, seat: 0 }] }),
      /at least 3 eligible Hot markets/, 'one real candidate removes the draw entirely');
    assert.strictEqual(CT.q.get.get('r13c1'), undefined);
    assert.strictEqual(CT.q.get.get('r13c2'), undefined);
  });

  await ok('operator standings refuses an unknown round and an invalid checkpoint', async () => {
    let r = await admin({ action: 'standings', id: 'never-armed-at-all', checkpoint: 'final' });
    assert.strictEqual(r.code, 404, JSON.stringify(r.body));
    comp.createRound({ id: 'r13st', candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    r = await admin({ action: 'standings', id: 'r13st', checkpoint: 'typo' });
    assert.strictEqual(r.code, 400, JSON.stringify(r.body));
    assert.match(r.body.error, /unknown checkpoint/);
    comp.abortRound('r13st', { force: true });
  });
  await ok('thin reliability history refuses, says how long, and can be overridden', async () => {
    /* Refusing was right; refusing with no way out was not. A restart is what
       empties the history, so the escape hatch could not be an env var that
       needs a restart to change. */
    const prev = process.env.PAPER_ALLOW_UNPROVEN_MARKETS;
    delete process.env.PAPER_ALLOW_UNPROVEN_MARKETS;
    try {
      comp.createRound({ id: 'r13thin', candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
      let v = comp.marketReadiness('r13thin');
      assert.strictEqual(v.ok, false);
      assert.match(v.error, /more needed/, 'the operator must be told how long to wait');
      assert.ok(v.waitMs > 0, 'and it must be a number, not a shrug');

      let r = await admin({ action: 'overrideReadiness', id: 'r13thin' });
      assert.strictEqual(r.code, 400, 'an override with no reason is not an override');

      r = await admin({ action: 'overrideReadiness', id: 'r13thin', why: 'box restarted, markets healthy' });
      assert.strictEqual(r.code, 200, JSON.stringify(r.body));
      v = comp.marketReadiness('r13thin');
      assert.strictEqual(v.ok, true, 'and afterwards the round may start');
    } finally {
      if (prev !== undefined) process.env.PAPER_ALLOW_UNPROVEN_MARKETS = prev;
      comp.abortRound('r13thin', { force: true });
    }
  });

  await ok('the Boost leverage is stored ON the round, not read from deployed code', () => {
    /* Derived from whatever was deployed, so a restart or a config change
       could rewrite the rules of a round already on air. */
    comp.createRound({ id: 'r13lev', candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    assert.strictEqual(comp.boostLeverageOf('r13lev'), 500,
      'the round records the cap it was armed under');
    CT.db.prepare('UPDATE paper_rounds SET boost_leverage = 250 WHERE id = ?').run('r13lev');
    assert.strictEqual(comp.boostLeverageOf('r13lev'), 250,
      'and reads it back from the round, not from the constant');
    comp.abortRound('r13lev', { force: true });
  });
  console.log('\nround fourteen: durable rule ownership and fail-closed pauses');

  await ok('a pause that cannot be PERSISTED still freezes, and blocks the round', async () => {
    /* pauseRound() caught the write error and only logged it, roundPaused()
       read no rows, and contestant writes carried on. The log said the field
       was frozen while the engine admitted a new position. */
    T.__clearPauses();
    const id = 'r14latch';
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    comp.startRound(id);
    /* Injected at the STATEMENT, not at db.prepare: the pause statements are
       built eagerly at load, so patching prepare() afterwards changes nothing. */
    const st = T.__pauseStmts();
    const realOpen = st.open;
    st.open = { run: () => { throw new Error('injected pause write failure'); } };
    let paused;
    try {
      T.__pauseFor({ symbol: 'R14A', message: 'must freeze even if persistence fails' });
      paused = T.roundPaused();
    } finally { st.open = realOpen; }
    assert.ok(paused, 'a pause that could not be written must still be held');
    assert.strictEqual(paused.unpersisted, true, 'and must say it is not durable');
    const r = await order(A, { symbol: 'BTC', side: 'BUY', type: 'MARKET', size: 0.001, leverage: 10 });
    assert.strictEqual(r.code, 409, JSON.stringify(r.body));
    assert.match(CT.q.get.get(id).blocked_reason || '', /could not be persisted/,
      'and the round is blocked so an operator has to look at it');
    try { comp.clearBlock(id, { note: 'test' }); } catch {}
    comp.abortRound(id, { force: true });
    T.__clearPauses();
  });

  await ok('a dead round\'s pause cannot freeze the NEXT round', async () => {
    /* The obligation stored round_id and every query ignored it, so an
       unresolved pause on an aborted round rejected a valid order in a new
       round, on a symbol that round never named. */
    T.__clearPauses();
    const oldId = 'r14old', newId = 'r14new';
    comp.createRound({ id: oldId, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    comp.startRound(oldId);
    T.__pauseFor({ symbol: 'R14DEAD', message: 'never recovered' });
    assert.ok(T.roundPaused(), 'precondition: the old round is paused');
    comp.abortRound(oldId, { force: true });

    comp.createRound({ id: newId, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: B, seat: 0 }] });
    comp.startRound(newId);
    assert.strictEqual(T.roundPaused(), null, 'the new round inherits nothing');
    const r = await order(B, { symbol: 'BTC', side: 'BUY', type: 'MARKET', size: 0.001, leverage: 10 });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    const kept = T.db.prepare("SELECT round_id, restored_at FROM paper_price_pauses WHERE symbol = 'R14DEAD'").get();
    assert.strictEqual(kept.round_id, oldId, 'the old obligation is kept for audit');
    assert.ok(kept.restored_at, 'but closed with its round');
    comp.abortRound(newId, { force: true });
    T.__clearPauses();
  });

  await ok('the round\'s ARMED Boost cap is the ceiling, not the deployed constant', () => {
    /* Persisting boost_leverage meant nothing while levCapFor still read the
       engine: a redeploy rewrote the rules of a round already on air. */
    const id = 'r14cap';
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    CT.db.prepare('UPDATE paper_rounds SET boost_leverage = 250 WHERE id = ?').run(id);
    comp.startRound(id);
    const inBoost = CT.q.get.get(id).started_at + comp.ROUND_PLAN.round.boostStart + 1000;
    assert.strictEqual(comp.levCapFor('BTC-BOOST', 500, A, inBoost), 250,
      'a 500x engine must not exceed the 250x the round was armed under');
    comp.abortRound(id, { force: true });
  });

  await ok('Reset all seats skips unclaimed seats instead of failing on them', async () => {
    /* prepareSeat(null) failed the whole action with a raw FOREIGN KEY error,
       so a roster with one empty seat could not be reset at all. */
    const id = 'r14reset';
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'],
      players: [{ userId: A, seat: 0 }, { displayName: 'nobody', seat: 1 }] });
    const r = await admin({ action: 'resetPlayers', id });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    assert.strictEqual(r.body.reset.length, 1);
    assert.strictEqual(r.body.skipped.length, 1, 'and the empty seat is reported, not hidden');
    comp.abortRound(id, { force: true });
  });

  await ok('a failed Start leaves the armed roster and its invites untouched', () => {
    /* Unclaimed seats were dropped BEFORE validation, so a transient market
       failure permanently destroyed a seat and its invite while the round
       stayed armed. */
    const id = 'r14start';
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'],
      players: [{ userId: A, seat: 0 }, { displayName: 'late', seat: 1 }] });
    const before = CT.q.players.all(id).map((p) => `${p.seat}:${p.user_id}:${p.invite_token}`);
    const prev = process.env.PAPER_ALLOW_UNPROVEN_MARKETS;
    delete process.env.PAPER_ALLOW_UNPROVEN_MARKETS;
    try {
      assert.throws(() => comp.startRound(id), /reliability history|not competition-ready/);
    } finally { if (prev !== undefined) process.env.PAPER_ALLOW_UNPROVEN_MARKETS = prev; }
    const after = CT.q.players.all(id).map((p) => `${p.seat}:${p.user_id}:${p.invite_token}`);
    assert.deepStrictEqual(after, before, 'the late seat and its invite must survive');
    assert.strictEqual(CT.q.get.get(id).status, 'armed');
    comp.abortRound(id, { force: true });
  });

  await ok('claiming a seat and creating its account are one act', () => {
    /* The seat was committed before the account, so a failure between them
       left a seat owned by a user with no account, and re-opening the invite
       answered "already yours" and never repaired it. */
    const id = 'r14claim', U = 8777;
    CT.db.prepare('INSERT OR IGNORE INTO users (id) VALUES (?)').run(U);
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], players: [{ displayName: 'S', seat: 0 }] });
    const tok = CT.q.players.all(id)[0].invite_token;
    const realEnsure = T.__ensureAccountRef();
    T.__setEnsureAccount(() => { throw new Error('injected account failure'); });
    try {
      assert.throws(() => comp.claimInvite(tok, U), /injected account failure/);
    } finally { T.__setEnsureAccount(realEnsure); }
    assert.strictEqual(CT.q.players.all(id)[0].user_id, null,
      'a failed claim must leave the seat unclaimed, not owned with no account');
    const ok2 = comp.claimInvite(tok, U);
    assert.strictEqual(ok2.seat.user_id, U, 'and the retry succeeds cleanly');
    assert.ok(T.stmt.acctGet.get(U), 'with the account actually created');
    comp.abortRound(id, { force: true });
  });
  console.log('\nround fifteen: one sample, one latch, one cap');

  await ok('a drawdown sample is ALL seats or none, never a partial write', () => {
    /* Max drawdown is a published tie-break. The sampler wrote seat by seat,
       so a failure on the second seat left the first updated and the second
       stale, and a later checkpoint froze two different answers for identical
       exposure. */
    const id = 'r15dd';
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'],
      players: [{ userId: A, seat: 0 }, { userId: B, seat: 1 }] });
    comp.startRound(id);
    const before = CT.q.players.all(id).map((p) => p.max_drawdown);
    const realDd = CT.q.ddUpd;
    const realEq0 = T.__equityRef();
    let n = 0;
    /* Both seats must actually MOVE, or there is nothing to write and nothing
       to fail on. Equity below the starting bankroll gives both a drawdown. */
    T.__setEquity(() => 9.9);
    CT.q.ddUpd = { run: (...args) => { if (++n === 2) throw new Error('injected drawdown write failure'); return realDd.run(...args); } };
    try {
      assert.throws(() => comp.sampleDrawdown(Date.now()), /injected drawdown write failure/);
    } finally { CT.q.ddUpd = realDd; T.__setEquity(realEq0); }
    assert.deepStrictEqual(CT.q.players.all(id).map((p) => p.max_drawdown), before,
      'a failed sample must leave EVERY seat exactly as it was');
    assert.match(CT.q.get.get(id).blocked_reason || '', /drawdown sample could not be written/);
    comp.clearBlock(id, { note: 'test' });
    try { comp.abortRound(id, { force: true }); } catch {}
  });

  await ok('one unpriceable seat means NOBODY is sampled that instant', () => {
    /* The sampler caught a pricing error and continued, so a seat holding one
       tiny unpriceable leg kept a shallower drawdown than an identical rival
       on the same tick. */
    const id = 'r15shield';
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'],
      players: [{ userId: A, seat: 0 }, { userId: B, seat: 1 }] });
    comp.startRound(id);
    const realEq = T.__equityRef();
    T.__setEquity((uid) => { if (uid === A) throw new Error('unpriceable dust'); return 9.9; });
    /* It THROWS now, and that is the point. Returning quietly told the caller
       the sample had been taken, so the round carried on with no tie-break
       evidence for anybody and no way to know. The caller decides what a
       missing sample costs; the sampler's job is to say it is missing. */
    let threw = null;
    try { comp.sampleDrawdown(Date.now()); } catch (e) { threw = e; } finally { T.__setEquity(realEq); }
    assert.ok(threw && threw.unpriced, 'a sample that cannot be taken must be reported, not swallowed');
    const dd = CT.q.players.all(id).map((p) => Number(p.max_drawdown) || 0);
    assert.deepStrictEqual(dd, [0, 0],
      'skipping only the unpriceable seat is the asymmetry, not the fix');
    comp.abortRound(id, { force: true });
  });

  await ok('tick drawdown pauses the failed leg, not the unrelated incoming market', () => {
    const id = 'r15tick-symbol';
    T.__clearPauses();
    feedMark('ETH', 100);
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'],
      players: [{ userId: A, seat: 0 }] });
    comp.startRound(id);
    /* The tick sampler is intentionally exposure-driven. Give the incoming
       ETH tick a scored leg to evaluate; the injected pricing failure still
       names SOL, proving pause attribution without restoring the old behavior
       where every unheld market scanned the whole roster. */
    const ep = T.stmt.acctGet.get(A).epoch;
    const openedAt = Date.now();
    T.stmt.posIns.run(A, 'ETH', ep, 'LONG', 0.001, 100, 10,
      openedAt, 100, openedAt, openedAt, 'cross', 0);
    T.confirming.set('SOL', { since: Date.now(), from: 100, to: 101 });
    const realEq = T.__equityRef();
    T.__setEquity(() => {
      throw Object.assign(new Error('no competition-valid mark for SOL-BOOST'),
        { unpriced: true, symbol: ' sol-boost ' });
    });
    try {
      T.tickEval('ETH', { force: true });
      const p = T.roundPaused();
      assert.ok(p, 'the missing drawdown sample must pause the round');
      assert.deepStrictEqual(p.symbols, ['SOL'],
        'the pause must name the failed position base, not the ETH tick');
      T.__setEquity(realEq);
      T.__clearPauseIfPriceable();
      assert.deepStrictEqual(T.roundPaused().symbols, ['SOL'],
        'an unrelated recovery must not clear the actual failed market');
      T.confirming.delete('SOL');
      feedMark('SOL', 100);
      T.__clearPauseIfPriceable();
      assert.strictEqual(T.roundPaused(), null, 'the pause clears after SOL itself recovers');
    } finally {
      T.__setEquity(realEq);
      T.confirming.delete('SOL');
      comp.abortRound(id, { force: true });
      T.__clearPauses();
    }
  });

  await ok('sweep drawdown uses the sealed generic competition pause', () => {
    const id = 'r15sweep-symbol';
    T.__clearPauses();
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'],
      players: [{ userId: A, seat: 0 }] });
    comp.startRound(id);
    const realEq = T.__equityRef();
    T.__setEquity(() => {
      throw Object.assign(new Error('no competition-valid mark for SOL'),
        { unpriced: true, symbol: 'SOL' });
    });
    try {
      T.__sampleRoundDrawdown();
      assert.deepStrictEqual(T.roundPaused().symbols, ['__COMPETITION_FEED__']);
    } finally {
      T.__setEquity(realEq);
      comp.abortRound(id, { force: true });
      T.__clearPauses();
    }
  });

  await ok('an unknown drawdown pause clears only after a full roster probe succeeds', () => {
    const id = 'r15unknown-price';
    // This case tests recovery, not storage acknowledgment. Give it a unique
    // clock edge; the dedicated engine-review fixture covers a zero-row ack.
    const realNow = Date.now;
    const lastPause = T.db.prepare('SELECT MAX(started_at) t FROM paper_price_pauses').get().t;
    const floor = Math.max(realNow(), (Number(lastPause) || 0) + 1);
    Date.now = () => Math.max(realNow(), floor);
    const realEq = T.__equityRef();
    try {
      T.__clearPauses();
      comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'],
        players: [{ userId: A, seat: 0 }] });
      comp.startRound(id);
      T.__setEquity(() => NaN);
      T.__sampleRoundDrawdown();
      assert.deepStrictEqual(T.roundPaused().symbols, ['__COMPETITION_FEED__']);
      T.__clearPauseIfPriceable();
      assert.deepStrictEqual(T.roundPaused().symbols, ['__COMPETITION_FEED__'],
        'an unattributed failure must not recover merely because no symbol can be checked');
      T.__setEquity(realEq);
      T.__clearPauseIfPriceable();
      assert.strictEqual(T.roundPaused(), null,
        'a successful non-mutating whole-roster valuation proves recovery');
    } finally {
      try {
        T.__setEquity(realEq);
        if (comp.currentRound()?.id === id) comp.abortRound(id, { force: true });
        T.__clearPauses();
      } finally { Date.now = realNow; }
    }
  });

  await ok('a latch does not lift while the thing that caused it is still broken', async () => {
    const id = 'r15latch';
    T.__clearPauses();
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    comp.startRound(id);
    const st = T.__pauseStmts(); const realOpen = st.open;
    st.open = { run: () => { throw new Error('injected pause write failure'); } };
    try { T.__pauseFor({ symbol: 'R15DOWN', message: 'source lost' }); } finally { st.open = realOpen; }
    assert.ok(T.roundPaused(), 'precondition: latched and frozen');

    /* A typo'd round id must not touch the live round's safety state. */
    let r = await admin({ action: 'clearBlock', id: 'not-a-round' });
    assert.strictEqual(r.code, 404, JSON.stringify(r.body));
    assert.ok(T.roundPaused(), 'a wrong id must have NO side effects');

    /* And recovery is refused while the symbol is still unpriceable. */
    r = await admin({ action: 'clearBlock', id });
    assert.strictEqual(r.code, 409, JSON.stringify(r.body));
    assert.match(r.body.error, /still unpriceable: R15DOWN/);
    assert.ok(T.roundPaused(), 'refusing must leave the field frozen');

    T.__clearPauses();
    try { comp.clearBlock(id, { note: 'test' }); } catch {}
    comp.abortRound(id, { force: true });
  });

  await ok('a reverse pays for its new leg with the margin the close returns', async () => {
    /* One market order the other way for twice the size flips a position in
       one fill. The margin check must count what the closing slice gives
       back, or a max-sized isolated position can never be reversed. */
    const U = 8077, Sy = 'RVX1', now = Date.now();
    mkAccount(U);
    T.db.prepare('UPDATE paper_accounts SET balance = 10 WHERE user_id = ?').run(U);
    T.mktCfg.set(Sy, { tiers: [], maxLev: 100, lotSize: null, takerBps: 0, makerBps: 0, maintBps: 5000, cancelBps: 7500, maxLiqSize: null, status: 'active', isolatedOnly: false });
    setMarkRaw(Sy, 100); feedMark(Sy, 100, now);
    let r = await order(U, { symbol: Sy, side: 'BUY', type: 'MARKET', size: 1, leverage: 10, marginMode: 'isolated' });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    const before = T.stmt.posGet.get(U, Sy);
    assert.strictEqual(before.side, 'LONG');
    assert.ok(acct(U).balance < 1e-6, 'the whole bankroll is in the position: ' + acct(U).balance);
    r = await order(U, { symbol: Sy, side: 'SELL', type: 'MARKET', size: 2, leverage: 10, marginMode: 'isolated' });
    assert.strictEqual(r.code, 200, 'the flip must pass the margin check: ' + JSON.stringify(r.body));
    const after = T.stmt.posGet.get(U, Sy);
    assert.strictEqual(after.side, 'SHORT');
    assert.ok(Math.abs(after.size - 1) < 1e-9, 'same size the other way: ' + after.size);
    assert.ok(Math.abs(after.isolated_margin - 10) < 1e-6, 'same margin as before: ' + after.isolated_margin);
    /* A flip that asks for MORE than the close returns opens the largest
       new leg the margin allows, and never touches the closing leg. */
    r = await order(U, { symbol: Sy, side: 'BUY', type: 'MARKET', size: 3, leverage: 10, marginMode: 'isolated' });
    assert.strictEqual(r.code, 200, 'shrinks to fit instead of refusing: ' + JSON.stringify(r.body));
    const shrunk = T.stmt.posGet.get(U, Sy);
    assert.strictEqual(shrunk.side, 'LONG');
    assert.ok(Math.abs(shrunk.size - 1) < 1e-6, 'the new leg is what 10 of margin carries at 10x and 100: ' + shrunk.size);
    assert.ok(Math.abs(shrunk.isolated_margin - 10) < 1e-6, 'and takes all of it: ' + shrunk.isolated_margin);
    /* At a loss, the same size is out of reach and the flip lands smaller. */
    setMarkRaw(Sy, 99); feedMark(Sy, 99, Date.now());
    r = await order(U, { symbol: Sy, side: 'SELL', type: 'MARKET', size: 2, leverage: 10, marginMode: 'isolated' });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    const smaller = T.stmt.posGet.get(U, Sy);
    assert.strictEqual(smaller.side, 'SHORT');
    assert.ok(smaller.size < 1 && smaller.size > 0.85, 'the loss came out of the new leg: ' + smaller.size);
  });

  await ok('the wall never advertises a cap the order path will refuse', () => {
    /* boostCaps was computed from the live safety cap alone, so a round armed
       at 250x told the room 500x while the engine refused anything above 250. */
    const id = 'r15wall';
    /* WIRED THE WAY PRODUCTION IS, for this case only.
     *
     * paper.js wires boostLevCap so the order path applies the same
     * price-quality haircut the wall does. The suite leaves it out, and the
     * order path then falls back to the raw engine cap: the two sides were
     * being compared under rules production never runs, which is how a real
     * disagreement between them could hide here. The rest of the suite is not
     * built to satisfy that gate on every market, so it is wired for this
     * assertion and taken back out afterwards. */
    comp.wire({ boostLevCap: (sym, engineCap) => T.boostLevCap(T.baseOf(sym), engineCap) });
    comp.createRound({ id, candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: A, seat: 0 }] });
    CT.db.prepare('UPDATE paper_rounds SET boost_leverage = 250 WHERE id = ?').run(id);
    comp.startRound(id);
    CT.db.prepare('UPDATE paper_rounds SET started_at = ? WHERE id = ?')
      .run(Date.now() - comp.ROUND_PLAN.round.boostStart - 1000, id);
    T.openAlias('BTC-BOOST', id);
    const cap = T.boostCapFor('BTC-BOOST');
    /* The round was armed at 250x, so neither side may exceed it whatever the
       deployed constant says. The exact value below that depends on which
       source is live, which is the point: the two must agree on it. */
    assert.ok(cap <= 250, 'the wall never advertises above the round: ' + cap);
    /* The order path derives its ceiling from the SYMBOL, so the comparison
       hands it the same thing, not a hand-picked constant. */
    assert.strictEqual(comp.levCapFor('BTC-BOOST', T.stageLevCap('BTC-BOOST'), A), cap,
      'and it must equal what the order path enforces');
    comp.wire({ boostLevCap: null });
    T.closeAlias('BTC-BOOST');
    comp.abortRound(id, { force: true });
  });





  console.log('\nhistorical boundaries are immutable');
  await ok('one past instant gives one answer, whatever the live dispute state', () => {
    /* The oldest finding against this engine: markAt(strict) asked whether the
       symbol was in dispute RIGHT NOW and then answered about a PAST instant,
       so a boundary read valid, then null while a later jump confirmed, then
       valid again on retry. A settled result could change its mind. The
       observation's own recorded verdict decides now. */
    const S = 'HISTIMM';
    const t0 = Date.now();
    T.mktCfg.set(S, { tiers: [], maxLev: 40, lotSize: null, takerBps: 3.5, makerBps: 0.5,
      maintBps: 50, cancelBps: 0, maxLiqSize: null, status: 'active', isolatedOnly: false });
    T.compUpdate(S, 'usdt', 100, t0); T.compUpdate(S, 'usd', 100, t0);
    const at = t0 + 10;
    T.compUpdate(S, 'usdt', 100, at); T.compUpdate(S, 'usd', 100, at);
    /* The market keeps ticking after the boundary, which is what makes it a
       SETTLED past instant rather than the live edge. A dispute that starts
       later must not reach back through these. */
    for (let k = 1; k <= 3; k++) {
      T.compUpdate(S, 'usdt', 100, at + k * 5); T.compUpdate(S, 'usd', 100, at + k * 5);
    }

    const before = T.markAt(S, at, { strict: true });
    T.confirming.set(S, { since: t0 + 20 });
    const during = T.markAt(S, at, { strict: true });
    T.confirming.delete(S);
    const after = T.markAt(S, at, { strict: true });
    assert.strictEqual(before, 100, 'a clean past instant settles');
    assert.strictEqual(during, before, 'a later dispute cannot unmake it');
    assert.strictEqual(after, before, 'and the answer does not come back different');

    /* Immutable must not mean permissive: with no observation inside its
       validity window the boundary still refuses. */
    assert.strictEqual(T.markAt(S, at + 60_000, { strict: true }), null,
      'no valid evidence still means no settlement');
    assert.strictEqual(T.markAt(S, t0 - 5_000, { strict: true }), null,
      'and an instant before any observation must fail closed');
  });

  console.log(`\n${pass} passed${process.exitCode ? ', WITH FAILURES' : ''}\n`);
})();
