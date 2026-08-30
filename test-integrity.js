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
if (!process.env.PAPER_DB || process.env.PAPER_DB.startsWith('/opt/')) {
  console.error('refusing to run: set PAPER_DB to a throwaway path first');
  process.exit(2);
}

const auth = require('./auth-shim.js');
const comp = require('./competition.js');
const P = require('./paper.js');
const T = P.__test;
const CT = comp.__test;

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
  for (let back = 20_000; back > 0; back -= 2_000) T.recordMark(sym, px, t - back);
  T.recordMark(sym, px, t);
}
for (const s of ['BTC', 'SOL', 'ETH', 'BNB', 'XRP']) {
  setMarkRaw(s, 100); feedMark(s, 100);
  T.mktCfg.set(s, { tiers: [], maxLev: 40, lotSize: null, takerBps: 3.5, makerBps: 0.5, maintBps: 50, cancelBps: 0, maxLiqSize: null, status: 'active', isolatedOnly: false });
}
comp.wire({
  openAlias: T.openAlias, closeAlias: T.closeAlias, scoreUser: T.scoreUser,
  prepareSeat: T.prepareSeat, seatState: T.seatState, markSetFor: T.markSetFor,
  equityOf: (uid) => { const a = T.stmt.acctGet.get(uid); return a ? T.accountRisk(uid, a).equityTotal : NaN; }, log: () => {},
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
    id: 'i1', candidates: ['BTC', 'SOL'],
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
    comp.createRound({ id: 'i1b', candidates: ['BTC', 'SOL'], players: [{ userId: OUTSIDER }] });
    assert.throws(() => comp.startRound('i1b'), /already running/);
    comp.abortRound('i1b');
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
  comp.abortRound('i1');

  console.log('\nblocker 12: arming a round is all-or-nothing');
  await ok('a duplicate round id does not leave a stale seed behind', () => {
    comp.createRound({ id: 'i2', candidates: ['BTC', 'SOL'], players: [{ userId: A }] });
    const before = CT._seeds.get('i2');
    assert.throws(() => comp.createRound({ id: 'i2', candidates: ['ETH', 'SOL'], players: [{ userId: A }] }));
    assert.strictEqual(CT._seeds.get('i2'), before, 'the committed seed must not be replaced');
  });
  await ok('a failed arm writes no partial roster', () => {
    assert.throws(() => comp.createRound({
      id: 'i3', candidates: ['BTC', 'SOL'], players: [{ userId: A }, { userId: A }],
    }), /duplicate players/);
    assert.strictEqual(CT.q.get.get('i3'), undefined, 'no round row');
    assert.strictEqual(comp.playersOf('i3').length, 0, 'no orphan roster rows');
  });

  console.log('\ninput validation the review asked for');
  await ok('duplicate hot candidates are refused', () => {
    assert.throws(() => comp.createRound({ id: 'v1', candidates: ['BTC', 'BTC'], players: [{ userId: A }] }),
      /duplicate hot candidates/);
  });
  await ok('a backup that is also a candidate is refused', () => {
    assert.throws(() => comp.createRound({ id: 'v2', candidates: ['BTC', 'SOL'], backup: 'BTC', players: [{ userId: A }] }),
      /backup must not/);
  });
  await ok('duplicate seats are refused', () => {
    assert.throws(() => comp.createRound({
      id: 'v3', candidates: ['BTC', 'SOL'],
      players: [{ userId: A, seat: 0 }, { userId: B, seat: 0 }],
    }), /duplicate seats/);
  });
  await ok('an empty roster cannot start a round', () => {
    comp.createRound({ id: 'v4', candidates: ['BTC', 'SOL'] });
    assert.throws(() => comp.startRound('v4'), /no players/);
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
  await ok('an order is rejected once the bell has passed', async () => {
    T.db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(A);
    comp.createRound({ id: 'i4', candidates: ['BTC', 'SOL'], players: [{ userId: A, seat: 0 }] });
    comp.startRound('i4');
    // wind the round past its own bell without firing the boundary: this is
    // exactly the window an in-flight request occupies
    CT.db.prepare('UPDATE paper_rounds SET started_at = ?, ends_at = ? WHERE id = ?')
      .run(Date.now() - 40 * 60_000, Date.now() - 1000, 'i4');
    /* The clock advances on the write itself: the bell settles FIRST, and only
       then is the order considered. What matters is not the status code but
       that the frozen result cannot contain a post-bell trade. */
    asUser(A);
    const res = mkRes();
    await P.placeOrder(mkReq({ symbol: 'BTC', side: 'BUY', type: 'MARKET', size: 0.01, leverage: 10 }), res);
    const board = comp.standings('i4', 'final');
    assert.strictEqual(board.length, 1, 'the bell must have settled before the write');
    assert.ok(Math.abs(board[0].account_pnl) < 1e-9,
      'a trade attempted after the bell must not appear in the frozen result');
  });
  await ok('a spectator is unaffected by another round settling', async () => {
    asUser(OUTSIDER);
    assert.strictEqual(comp.settledFor(OUTSIDER), false);
  });

  console.log('\nblocker 5: a checkpoint is all-or-nothing');
  await ok('one unscoreable seat fails the whole checkpoint', () => {
    comp.createRound({
      id: 'i5', candidates: ['BTC', 'SOL'],
      players: [{ userId: A, displayName: 'A', seat: 0 }, { userId: B, displayName: 'B', seat: 1 }],
    });
    comp.startRound('i5');
    /* Wind the round to its bell. Recovery replays boundaries that are DUE,
       so a round whose clock says the bell has not arrived has nothing to
       retry — which is correct, and means the fixture has to be honest about
       where the clock is. */
    CT.db.prepare('UPDATE paper_rounds SET started_at = ? WHERE id = ?')
      .run(Date.now() - (comp.ROUND_PLAN.round.total + 1000), 'i5');
    // make the second seat unscoreable
    const realScore = T.scoreUser;
    comp.wire({ scoreUser: (uid, hot, ep) => {
      if (uid === B) { throw new Error('boom'); }
      return realScore(uid, hot, ep);
    } });
    assert.throws(() => comp.snapshot('i5', 'firstFive'), /boom/);
    assert.strictEqual(comp.standings('i5', 'firstFive').length, 0,
      'no partial leaderboard may be written');
    comp.wire({ scoreUser: realScore });
  });
  await ok('a failed bell BLOCKS the round instead of finishing it', () => {
    const realScore = T.scoreUser;
    comp.wire({ scoreUser: (uid, hot, ep) => {
      if (uid === B) { throw new Error('boom'); }
      return realScore(uid, hot, ep);
    } });
    CT.fireBoundary('i5', comp.ROUND_PLAN.round.total);
    const r = CT.q.get.get('i5');
    assert.notStrictEqual(r.status, 'done', 'a round that could not be scored must not look finished');
    assert.match(r.blocked_reason || '', /boundary .* failed/);
    const b = CT.q.bGet.get('i5', comp.ROUND_PLAN.round.total);
    assert.strictEqual(b.status, 'failed', 'the boundary must be recorded as failed');
    comp.wire({ scoreUser: realScore });
  });
  await ok('a blocked round refuses to advance until an operator clears it', () => {
    const before = CT.q.bGet.get('i5', comp.ROUND_PLAN.round.total).ran_at;
    CT.fireBoundary('i5', comp.ROUND_PLAN.round.total);
    assert.notStrictEqual(CT.q.get.get('i5').status, 'done', 'still blocked, must not settle');
    assert.strictEqual(CT.q.bGet.get('i5', comp.ROUND_PLAN.round.total).ran_at, before,
      'a blocked round must not even re-run the boundary');
  });
  await ok('recovery RETRIES the boundary, it does not merely unlock', () => {
    // no explicit fireBoundary here: recovery must do the work itself
    const r = comp.clearBlock('i5', { note: 'test recovery' });
    assert.strictEqual(r.blocked_reason, null, 'the block must be cleared by a successful recovery');
    assert.strictEqual(r.status, 'done', 'and the boundary it owed must have run');
    assert.strictEqual(comp.standings('i5', 'final').length, 2, 'scoring everyone');
    assert.strictEqual(CT.q.bGet.get('i5', comp.ROUND_PLAN.round.total).status, 'succeeded');
  });
  await ok('a recovery that does not fix the cause stays blocked', () => {
    const id = 'i5b';
    comp.createRound({ id, candidates: ['BTC', 'SOL'], players: [{ userId: A, seat: 0 }] });
    comp.startRound(id);
    const real = T.scoreUser;
    comp.wire({ scoreUser: () => { throw new Error('still broken'); } });
    CT.db.prepare('UPDATE paper_rounds SET started_at = ? WHERE id = ?')
      .run(Date.now() - (comp.ROUND_PLAN.round.total + 1000), id);
    comp.advanceRoundClock(Date.now());
    assert.ok(CT.q.get.get(id).blocked_reason, 'should be blocked');
    const after = comp.clearBlock(id, { note: 'premature' });
    assert.ok(after.blocked_reason, 'recovery must re-block when the cause persists');
    comp.wire({ scoreUser: real });
    comp.abortRound(id);
  });
  await ok('a succeeded boundary is never replayed', () => {
    const before = comp.standings('i5', 'final')[0].at;
    CT.fireBoundary('i5', comp.ROUND_PLAN.round.total);
    assert.strictEqual(comp.standings('i5', 'final')[0].at, before);
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
    comp.createRound({ id: 'i7', candidates: ['BTC', 'SOL'], players: [{ userId: A, seat: 0 }] });
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
    comp.abortRound('i7');
  });
  await ok('the public product keeps its legacy clock', () => {
    T.db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(OUTSIDER);
    T.db.prepare('UPDATE paper_accounts SET heat = 1 WHERE user_id = ?').run(OUTSIDER);
    T.applyFill(OUTSIDER, { symbol: 'BTC', orderSide: 'BUY', size: 0.01, px: 100, feeBps: 0, kind: 'MARKET', leverage: 500 });
    assert.ok(T.stmt.posGet.get(OUTSIDER, 'BTC').boost_since,
      'an ordinary paper trader above 100x still starts the 2:00 clock');
  });

  console.log('\nblocker 8: a fallback must not invalidate the draw proof');
  await ok('the drawn market survives a fallback, and both are published', () => {
    comp.createRound({ id: 'i8', candidates: ['BTC', 'SOL'], backup: 'ETH', players: [{ userId: A, seat: 0 }] });
    comp.startRound('i8');
    CT.fireBoundary('i8', comp.ROUND_PLAN.round.reveal);
    const drawn = CT.q.get.get('i8').hot_base;
    // make the drawn market unopenable so the backup has to take over
    T.live.map.delete(drawn);
    CT.fireBoundary('i8', comp.ROUND_PLAN.round.hotStart);
    const r = CT.q.get.get('i8');
    assert.strictEqual(r.hot_base, drawn, 'the DRAWN market must never be overwritten');
    assert.strictEqual(r.active_hot_base, 'ETH', 'the backup is what traded');
    assert.ok(r.fallback_reason, 'the reason must be recorded, not silent');
    const v = comp.verifyDraw(r);
    assert.strictEqual(v.ok, true, 'the proof must still verify: ' + v.reason);
    assert.strictEqual(v.fellBack, true);
    assert.strictEqual(v.traded, 'ETH');
    setMarkRaw(drawn, 100); feedMark(drawn, 100);
    comp.abortRound('i8');
  });

  console.log('\nround two: the mutation barrier covers every path');
  await ok('closing a position after the bell is refused', async () => {
    T.db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(A);
    comp.createRound({ id: 'r2a', candidates: ['BTC', 'SOL'], players: [{ userId: A, seat: 0 }] });
    comp.startRound('r2a');
    const ep = T.seatState_epoch(A);
    T.stmt.posIns.run(A, 'BTC', ep, 'LONG', 1, 100, 10, Date.now(), 100, Date.now(), Date.now(), 'cross', 0);
    CT.db.prepare('UPDATE paper_rounds SET ends_at = ? WHERE id = ?').run(Date.now() - 1000, 'r2a');
    asUser(A);
    const res = mkRes();
    await P.closePosition(mkReq({ symbol: 'BTC' }), res);
    assert.strictEqual(res.code, 409, JSON.stringify(res.body));
    assert.ok(T.stmt.posGet.get(A, 'BTC'), 'the position must be untouched');
  });
  await ok('adjusting margin after the bell is refused', async () => {
    asUser(A);
    const res = mkRes();
    await P.adjustMargin(mkReq({ symbol: 'BTC', amount: 1 }), res);
    assert.strictEqual(res.code, 409);
  });
  await ok('a blocked round also bars writes', async () => {
    CT.db.prepare('UPDATE paper_rounds SET ends_at = ?, blocked_reason = ? WHERE id = ?')
      .run(Date.now() + 60_000, 'test block', 'r2a');
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
    comp.createRound({ id: 'r2b', candidates: ['BTC', 'SOL'], players: [{ userId: B, seat: 0 }] });
    comp.abortRound('r2b');
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
    CT.db.prepare('UPDATE paper_rounds SET ends_at = ? WHERE id = ?').run(Date.now() + 600_000, 'r2a');
    comp.resume();
    const b = CT.q.bGet.get('r2a', comp.ROUND_PLAN.round.firstFive);
    assert.notStrictEqual(b.status, 'running', 'a stale lease must not block forever');
  });
  await ok('a restart that missed a boundary blocks instead of continuing', () => {
    // wind the clock so First Five is in the past and unrun
    CT.db.prepare('UPDATE paper_rounds SET started_at = ?, blocked_reason = NULL WHERE id = ?')
      .run(Date.now() - 10 * 60_000, 'r2a');
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
    comp.abortRound('r2a');
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
    const t0 = Date.now();
    T.recordMark('BTC', 100, t0 - 5000);
    T.recordMark('BTC', 110, t0 - 1000);
    assert.strictEqual(T.markAt('BTC', t0 - 3000), 100, 'must return the mark at or before T');
    assert.strictEqual(T.markAt('BTC', t0), 110);
  });
  await ok('a late bell does not let a post-bell move into the result', () => {
    T.db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(A);
    comp.createRound({ id: 'r2t', candidates: ['BTC', 'SOL'], players: [{ userId: A, seat: 0 }] });
    comp.startRound('r2t');
    const ep = T.seatState_epoch(A);
    T.stmt.posIns.run(A, 'BTC', ep, 'LONG', 1, 100, 10, Date.now(), 100, Date.now(), Date.now(), 'cross', 0);

    const due = Date.now() - 4000;             // the bell was due 4s ago
    T.recordMark('BTC', 100, due - 500);       // the price AT the bell
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
    const row = comp.standings('r2t', 'final')[0];
    const m = JSON.parse(row.marks || '{}');
    assert.strictEqual(m.BTC, 100, 'the stored mark set must be the one the result came from');
    comp.abortRound('r2t');
    setMarkRaw('BTC', 100); feedMark('BTC', 100);
  });

  console.log('\nround two: drawdown, auth and the audit trail');
  await ok('drawdown is measured, so the published tie-break can be applied', () => {
    T.db.prepare('DELETE FROM paper_positions WHERE user_id IN (?, ?)').run(A, B);
    comp.createRound({
      id: 'r2d', candidates: ['BTC', 'SOL'],
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
    comp.abortRound('r2d');
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
      id: 'r3d', candidates: ['BTC', 'SOL'],
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
    comp.abortRound('r3d');
  });

  console.log('\nround three: crash-safety and input hygiene');
  await ok('a persisted draw survives a lost in-memory seed', () => {
    const id = 'r3draw';
    comp.createRound({ id, candidates: ['BTC', 'SOL'], players: [{ userId: A, seat: 0 }] });
    comp.startRound(id);
    CT.db.prepare('UPDATE paper_rounds SET started_at = ? WHERE id = ?')
      .run(Date.now() - (comp.ROUND_PLAN.round.reveal + 1000), id);
    CT.fireBoundary(id, comp.ROUND_PLAN.round.reveal);       // draw commits
    const drawn = CT.q.get.get(id).hot_base;
    assert.ok(drawn, 'the draw should have happened');
    // simulate a crash between the draw committing and the boundary being marked
    CT.db.prepare("DELETE FROM paper_round_boundaries WHERE round_id = ? AND at = ?")
      .run(id, comp.ROUND_PLAN.round.reveal);
    CT._seeds.delete(id);
    CT.fireBoundary(id, comp.ROUND_PLAN.round.reveal);
    const r = CT.q.get.get(id);
    assert.strictEqual(r.blocked_reason, null, 'a valid persisted draw must not block');
    assert.strictEqual(r.hot_base, drawn, 'and must not be redrawn');
    comp.abortRound(id);
  });
  await ok('a roster of 1 and "1" is rejected, not silently collapsed', () => {
    assert.throws(() => comp.createRound({
      id: 'r3n', candidates: ['BTC', 'SOL'],
      players: [{ userId: 1, seat: 0 }, { userId: '1', seat: 1 }],
    }), /duplicate players/);
  });
  await ok('a finished round cannot be rewritten', () => {
    const id = 'r3f';
    comp.createRound({ id, candidates: ['BTC', 'SOL'], players: [{ userId: A, seat: 0 }] });
    comp.startRound(id);
    CT.db.prepare('UPDATE paper_rounds SET started_at = ? WHERE id = ?')
      .run(Date.now() - (comp.ROUND_PLAN.round.total + 1000), id);
    comp.advanceRoundClock(Date.now());
    assert.strictEqual(CT.q.get.get(id).status, 'done');
    assert.throws(() => comp.abortRound(id), /immutable/,
      'a published result must not be re-statused');
  });
  await ok('Boost cannot report success with no market open', () => {
    const id = 'r3b';
    comp.createRound({ id, candidates: ['BTC', 'SOL'], players: [{ userId: A, seat: 0 }] });
    comp.startRound(id);
    const saved = new Map();
    for (const b of ['BTC', 'ETH', 'BNB', 'XRP', 'SOL']) { saved.set(b, T.live.map.get(b)); T.live.map.delete(b); }
    CT.db.prepare('UPDATE paper_rounds SET started_at = ? WHERE id = ?')
      .run(Date.now() - (comp.ROUND_PLAN.round.boostStart + 1000), id);
    CT.fireBoundary(id, comp.ROUND_PLAN.round.boostStart);
    assert.match(CT.q.get.get(id).blocked_reason || '', /Boost market/,
      'a Boost phase with nothing tradable is a missing segment, not a degraded one');
    for (const [b, v] of saved) if (v) T.live.map.set(b, v);
    comp.abortRound(id);
  });

  console.log(`\n${pass} passed${process.exitCode ? ', WITH FAILURES' : ''}\n`);
})();
