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
const ok = async (name, fn) => {
  try { await fn(); console.log('  ok   ' + name); pass++; }
  catch (e) { console.log('  FAIL ' + name + '\n       ' + e.message); process.exitCode = 1; }
};

let CURRENT_USER = null;
auth.validateSession = async () => CURRENT_USER;
const asUser = (id) => { CURRENT_USER = { id, isGuest: false }; };
function mkRes() {
  const r = { code: null, body: null, writeHead(c) { r.code = c; }, end(s) { r.body = JSON.parse(s); } };
  return r;
}
const mkReq = (body) => {
  const buf = Buffer.from(JSON.stringify(body || {}));
  return { headers: {}, on(ev, cb) { if (ev === 'data') cb(buf); if (ev === 'end') cb(); return this; } };
};
const callReset = async (body) => { const r = mkRes(); await P.reset(mkReq(body), r); return r; };

const setMark = (s, px) => T.live.map.set(s, {
  markPrice: px, pythPrice: px, pythAtMs: Date.now(), pythBasis: 0,
  lastUpdatedMs: Date.now(), indexHalt: false,
});
for (const s of ['BTC', 'SOL', 'ETH', 'BNB', 'XRP']) {
  setMark(s, 100);
  T.mktCfg.set(s, { tiers: [], maxLev: 40, lotSize: null, takerBps: 3.5, makerBps: 0.5, maintBps: 50, cancelBps: 0, maxLiqSize: null, status: 'active', isolatedOnly: false });
}
comp.wire({
  openAlias: T.openAlias, closeAlias: T.closeAlias, scoreUser: T.scoreUser,
  resetPlayer: T.resetPlayerAccount, epochOf: T.epochOfUser, log: () => {},
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
    const ep = T.epochOfUser(uid);
    // 1000x long, isolated, tiny margin: a 5% adverse move must liquidate it
    T.stmt.posIns.run(uid, 'BTC-BOOST', ep, 'LONG', 1, 100, 1000, Date.now(), 100, Date.now(), Date.now(), 'isolated', 0.1);
    setMark('BTC', 90);
    T.tickEval('BTC', { force: true });          // a BASE tick, not an alias tick
    assert.strictEqual(T.stmt.posGet.get(uid, 'BTC-BOOST'), undefined,
      'the boosted position must be liquidated by the base tick, not left for the 5s sweep');
    setMark('BTC', 100);
  });
  await ok('the same holds for a HOT twin', () => {
    const uid = B;
    T.db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(uid);
    T.db.prepare('UPDATE paper_accounts SET heat = 1, start_balance = 10, balance = 10 WHERE user_id = ?').run(uid);
    const ep = T.epochOfUser(uid);
    T.stmt.posIns.run(uid, 'SOL-HOT', ep, 'LONG', 1, 100, 1000, Date.now(), 100, Date.now(), Date.now(), 'isolated', 0.1);
    setMark('SOL', 90);
    T.tickEval('SOL', { force: true });
    assert.strictEqual(T.stmt.posGet.get(uid, 'SOL-HOT'), undefined);
    setMark('SOL', 100);
  });

  console.log('\nblocker 3: resting orders cannot outlive their segment');
  await ok('closing a segment cancels orders resting on it', () => {
    T.openAlias('BTC-HOT', 'i-ord');
    const ep = T.epochOfUser(A);
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
    const ep = T.epochOfUser(A);
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
    assert.strictEqual(comp.settledFor(A), true, 'the round should be past its bell');
    asUser(A);
    const res = mkRes();
    await P.placeOrder(mkReq({ symbol: 'BTC', side: 'BUY', type: 'MARKET', size: 0.01, leverage: 10 }), res);
    assert.strictEqual(res.code, 409, JSON.stringify(res.body));
    assert.strictEqual(res.body.error, 'round_settled');
    comp.abortRound('i4');
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
    const good = comp.__hooks ? null : null;
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
    assert.match(r.blocked_reason || '', /final snapshot failed/);
    const b = CT.q.bGet.get('i5', comp.ROUND_PLAN.round.total);
    assert.strictEqual(b.status, 'failed', 'the boundary must be recorded as failed');
    comp.wire({ scoreUser: realScore });
  });
  await ok('a failed boundary can be retried once the cause is fixed', () => {
    CT.fireBoundary('i5', comp.ROUND_PLAN.round.total);
    const r = CT.q.get.get('i5');
    assert.strictEqual(r.status, 'done', 'the retry should settle it');
    assert.strictEqual(comp.standings('i5', 'final').length, 2, 'and score everyone');
    assert.strictEqual(CT.q.bGet.get('i5', comp.ROUND_PLAN.round.total).status, 'succeeded');
  });
  await ok('a succeeded boundary is never replayed', () => {
    const before = comp.standings('i5', 'final')[0].at;
    CT.fireBoundary('i5', comp.ROUND_PLAN.round.total);
    assert.strictEqual(comp.standings('i5', 'final')[0].at, before);
  });

  console.log('\nblocker 6: open Hot exposure counts 2x on the live wall');
  await ok('an OPEN hot position already shows its bonus', () => {
    T.db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(A);
    const ep = T.epochOfUser(A);
    T.stmt.posIns.run(A, 'SOL-HOT', ep, 'LONG', 1, 100, 10, Date.now(), 100, Date.now(), Date.now(), 'cross', 0);
    setMark('SOL', 105);                       // +5 unrealised on the hot leg
    const s = T.scoreUser(A, 'SOL-HOT', ep);
    assert.ok(Math.abs(s.hotBonus - 5) < 1e-6,
      'the bonus must track the open leg, not wait for it to close: got ' + s.hotBonus);
  });
  await ok('closing the hot leg does not jump the score', () => {
    const ep = T.epochOfUser(A);
    const before = T.scoreUser(A, 'SOL-HOT', ep);
    const beforeScore = before.accountPnl + before.hotBonus;
    // realise it at the same mark, the way the segment close does
    T.stmt.fillIns.run(A, ep, 'SOL-HOT', 'SELL', 'SEGMENT', 105, 1, 105, 0, 5, null, Date.now(), 0);
    T.db.prepare('UPDATE paper_accounts SET balance = balance + 5 WHERE user_id = ?').run(A);
    T.db.prepare("DELETE FROM paper_positions WHERE user_id = ? AND symbol = 'SOL-HOT'").run(A);
    const after = T.scoreUser(A, 'SOL-HOT', ep);
    assert.ok(Math.abs((after.accountPnl + after.hotBonus) - beforeScore) < 1e-6,
      `score stepped at close: ${beforeScore} -> ${after.accountPnl + after.hotBonus}`);
    setMark('SOL', 100);
  });

  console.log('\nblocker 7: the server owns Boost behaviour');
  await ok('a seated player cannot arm the legacy per-position clock', () => {
    comp.createRound({ id: 'i7', candidates: ['BTC', 'SOL'], players: [{ userId: A, seat: 0 }] });
    comp.startRound('i7');
    const ep = T.epochOfUser(A);
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
    setMark(drawn, 100);
    comp.abortRound('i8');
  });

  console.log(`\n${pass} passed${process.exitCode ? ', WITH FAILURES' : ''}\n`);
  process.exit(process.exitCode || 0);
})();
