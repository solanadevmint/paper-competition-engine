/* End-to-end test of the competition HTTP surface: auth, the operator
 * actions, the live wall payload and the public draw verification.
 *
 *   PAPER_DB=$(mktemp -u --suffix=.db) PAPER_COMP_TOKEN=t0ken node test-comp-api.js
 *
 * Handlers are driven directly with fake req/res objects rather than over a
 * socket, so this exercises the same code the server routes to without
 * binding a port.
 */
const assert = require('assert');

process.env.PHOENIX_SNAPSHOT_FILE = '/nonexistent/markets-snapshot.json';
process.env.PAPER_COMP_TOKEN = process.env.PAPER_COMP_TOKEN || 'test-token';
/* The wall feed is cached for ~200ms in production so an audience polling it
   cannot tax the show's event loop. A test moves faster than that, so it
   would otherwise assert against a stale body. */
process.env.PAPER_COMP_STATE_TTL_MS = '0';
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
const TOKEN = process.env.PAPER_COMP_TOKEN;

let pass = 0;
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
  catch (e) { console.log('  FAIL ' + name + '\n       ' + e.message); process.exitCode = 1; }
  finally { _inflight = null; }
};
process.on('unhandledRejection', (e) => {
  console.log('  FAIL unhandled rejection\n       ' + (e && e.message ? e.message : e));
  process.exitCode = 1;
});

// ── fake req/res ─────────────────────────────────────────────────────────
function mkRes() {
  const r = { code: null, body: null, writeHead(c) { r.code = c; }, end(s) { r.body = JSON.parse(s); } };
  return r;
}
function mkReq(body, headers = {}) {
  const chunks = [Buffer.from(JSON.stringify(body || {}))];
  return {
    headers,
    on(ev, cb) { if (ev === 'data') chunks.forEach((c) => cb(c)); if (ev === 'end') cb(); return this; },
  };
}
/* The operator token is header-only now: a token in a JSON body lands in
   request logs and proxy traces. Anything passing `token` in the body is
   expected to be refused, which is asserted below. */
const admin = async (body, headers) => {
  const res = mkRes();
  const h = { ...(headers || {}) };
  if (body && body.token && !h['x-comp-token']) { h['x-comp-token'] = body.token; }
  await P.compAdmin(mkReq(body, h), res);
  return res;
};
const adminBodyTokenOnly = async (body) => {
  const res = mkRes();
  await P.compAdmin(mkReq(body), res);   // deliberately no header
  return res;
};
const state = () => { const res = mkRes(); P.compState({}, res); return res.body; };
/* Move the round's start back so the SERVER clock really is at `ms` into the
   round. Firing a boundary by hand changes stored state but not the clock,
   and compState derives the phase from the clock, so a test that skips this
   is testing neither. */
const warpTo = (id, ms) => CT.db.prepare('UPDATE paper_rounds SET started_at = ? WHERE id = ?')
  .run(Date.now() - ms, id);
const verify = (id) => {
  const res = mkRes();
  P.compVerify({}, res, new URL('http://x/?round=' + encodeURIComponent(id)));
  return res.body;
};

// ── fixtures ─────────────────────────────────────────────────────────────
const now = Date.now();
for (const s of ['BTC', 'SOL', 'ETH', 'BNB', 'XRP']) {
  T.live.map.set(s, { markPrice: 100, pythPrice: 100, pythAtMs: now, pythBasis: 0, lastUpdatedMs: now, indexHalt: false });
  // a healthy index: two fresh components plus recorded history, which is
  // what stage pricing and strict checkpoints now require
  T.compUpdate(s, 'usdt', 100, now); T.compUpdate(s, 'usd', 100, now);
  // a short trail so any boundary instant has a mark at or before it
  for (let back = 20_000; back >= 0; back -= 2_000) T.recordMark(s, 100, now - back, 2);
}
comp.wire({
  marketReady: () => true, // Explicit fixture price policy; production has no permissive default.
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
const SEATS = [7001, 7002];
for (const uid of SEATS) {
  T.db.prepare('INSERT OR IGNORE INTO users (id) VALUES (?)').run(uid);
  T.stmt.acctIns.run(uid, now, now);
  T.db.prepare('UPDATE paper_accounts SET heat = 1, start_balance = 10, balance = 10 WHERE user_id = ?').run(uid);
}

(async () => {
  console.log('\nauth');
  await ok('no token is refused', async () => {});
  let r = await admin({ action: 'create', id: 'x', candidates: ['BTC', 'SOL', 'ETH'] });
  await ok('a request with no token is forbidden', () => {
    assert.strictEqual(r.code, 403);
    assert.strictEqual(r.body.error, 'forbidden');
  });
  r = await admin({ action: 'create', id: 'x', candidates: ['BTC', 'SOL', 'ETH'], token: 'wrong' });
  await ok('a wrong token is forbidden', () => assert.strictEqual(r.code, 403));
  r = await adminBodyTokenOnly({ action: 'create', id: 'x', candidates: ['BTC', 'SOL', 'ETH'], token: TOKEN });
  await ok('a token in the request BODY is refused', () => {
    assert.strictEqual(r.code, 403, 'body tokens leak into logs; header only');
  });
  r = await admin({ action: 'create', id: 'e2e', candidates: ['BTC', 'SOL', 'ETH'], backup: 'XRP', token: TOKEN,
                    players: SEATS.map((u, i) => ({ userId: u, displayName: 'P' + i, seat: i })) });
  await ok('the operator token is accepted', () => {
    assert.strictEqual(r.code, 200);
    assert.strictEqual(r.body.round.id, 'e2e');
    assert.strictEqual(r.body.round.status, 'armed');
  });
  await ok('the header form of the token works too', async () => {});
  r = await admin({ action: 'standings', id: 'e2e' }, { 'x-comp-token': TOKEN });
  await ok('x-comp-token is accepted', () => assert.strictEqual(r.code, 200));

  console.log('\npre-flight');
  r = await admin({ action: 'preflight', id: 'e2e', token: TOKEN });
  await ok('reports every seat ready before a round', () => {
    assert.strictEqual(r.body.allReady, true, JSON.stringify(r.body.players));
    assert.strictEqual(r.body.players.length, 2);
  });
  await ok('flags a seat that is carrying a position', async () => {
    T.stmt.posIns.run(SEATS[0], 'BTC', 1, 'LONG', 1, 100, 10, Date.now(), 100, Date.now(), Date.now(), 'cross', 0);
    // header, not body: the token is header-only now
    const r2 = await admin({ action: 'preflight', id: 'e2e', token: TOKEN });
    assert.strictEqual(r2.body.allReady, false, 'a leftover position must block the start');
  });
  r = await admin({ action: 'preflight', id: 'e2e', token: TOKEN });
  await ok('a stale seat is actually reported not ready', () => assert.strictEqual(r.body.allReady, false));

  console.log('\nreset');
  r = await admin({ action: 'resetPlayers', id: 'e2e', token: TOKEN });
  await ok('resetting clears positions and bumps the epoch for everyone', () => {
    assert.strictEqual(r.body.reset.length, 2);
    assert.strictEqual(T.stmt.posByUser.all(SEATS[0]).length, 0, 'positions must be gone');
  });
  r = await admin({ action: 'preflight', id: 'e2e', token: TOKEN });
  await ok('pre-flight goes green after a reset', () => assert.strictEqual(r.body.allReady, true));

  console.log('\nlive state');
  await ok('nothing is live before the round starts', () => assert.strictEqual(state().live, false));
  await admin({ action: 'start', id: 'e2e', token: TOKEN });
  let st = state();
  await ok('the wall sees the round, the phase and the clock', () => {
    assert.strictEqual(st.live, true);
    assert.strictEqual(st.phase, 'build');
    assert.ok(st.leftMs > 29 * 60_000, 'about 30 minutes left');
    assert.strictEqual(st.players.length, 2);
    assert.strictEqual(st.players[0].rank, 1);
  });
  await ok('the drawn market is withheld until the reveal', () => {
    assert.strictEqual(st.hot, null, 'a spectator must not learn it early');
    assert.strictEqual(st.candidates, undefined, 'the eligible pool is sealed with the draw');
    assert.ok(st.drawCommit, 'and the commitment is published up front');
  });
  await ok('boost is not open in the opening phase', () => assert.strictEqual(st.boostOpen, false));
  await ok('the wall is told what each trader is holding', () => {
    // the show has to answer "what is she trading" without narration
    for (const p of st.players ?? []) {
      assert.ok(Array.isArray(p.positions), 'positions must always be an array');
    }
  });
  await ok('held markets surface biggest first without leaking position economics', async () => {
    T.stmt.posIns.run(SEATS[0], 'BTC', T.stmt.acctGet.get(SEATS[0]).epoch,
      'SHORT', 1, 100, 25, Date.now(), 100, Date.now(), Date.now(), 'cross', 0);
    T.stmt.posIns.run(SEATS[0], 'SOL', T.stmt.acctGet.get(SEATS[0]).epoch,
      'LONG', 2, 100, 100, Date.now(), 100, Date.now(), Date.now(), 'cross', 0);
    const row = state().players.find((p) => p.userId === SEATS[0]);
    const pos = row.positions.find((x) => x.symbol === 'BTC');
    assert.ok(pos, 'the position should be visible');
    assert.strictEqual(pos.segment, null, 'a base ticker is not a segment');
    assert.deepStrictEqual(row.positions.map((x) => x.symbol), ['SOL', 'BTC'],
      'the wall still receives the largest holding first');
    assert.deepStrictEqual(Object.keys(pos).sort(), ['segment', 'symbol'],
      'public state must not reveal side, leverage, notional, prices or PnL');
    const prior = { ...T.live.map.get('BTC') };
    T.live.map.set('BTC', { ...prior, pythPrice: 101, pythAtMs: Date.now(), lastUpdatedMs: Date.now() });
    const compact = T.compRankSnapshot();
    const mine = compact.players.find((p) => p.userId === SEATS[0]);
    assert.strictEqual(compact.live, true);
    assert.strictEqual(compact.roundId, 'e2e');
    assert.strictEqual(compact.complete, true);
    assert.strictEqual(mine.rank, 2, 'the compact push uses the live mark and exact rank tie-breaks');
    assert.strictEqual(mine.score, -1, 'a one-dollar short loss reaches the compact score');
    const published = T.compRankSnapshot(null, new Map([['BTC', 102], ['SOL', 100]]));
    assert.strictEqual(published.players.find((p) => p.userId === SEATS[0]).score, -2,
      'the socket scorer binds to its published mark set, not a different mutable live mark');
    const fullMine = state().players.find((p) => p.userId === SEATS[0]);
    assert.strictEqual(fullMine.rank, mine.rank, 'REST and socket ranks cannot diverge');
    assert.strictEqual(fullMine.score, mine.score, 'REST and socket scores cannot diverge');
    T.live.map.set('BTC', prior);
    T.db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(SEATS[0]);
  });

  console.log('\ndraw and segments');
  const liveRound = CT.q.get.get('e2e');
  const draw = CT.privateDrawOf(liveRound);
  const plan = comp.planOf(liveRound);
  warpTo('e2e', draw.hot1.activation - plan.hotWarning + 200);
  fireAt('e2e', draw.hot1.activation - plan.hotWarning);
  st = state();
  await ok('the warning names its ordinal but keeps the market sealed', () => {
    assert.strictEqual(st.hot, null);
    assert.strictEqual(st.hotNumber, 1);
    assert.strictEqual(st.phase, 'hotWarning');
  });
  await ok('the draw remains sealed to public verification while live', () => {
    const v = verify('e2e');
    assert.strictEqual(v.error, 'draw_not_revealed');
    assert.ok(v.commit);
    assert.strictEqual(v.seed, undefined);
  });
  warpTo('e2e', draw.hot1.activation + 200);
  fireAt('e2e', draw.hot1.activation);
  await ok('the ordinary Hot market is revealed on its boundary', () => {
    assert.strictEqual(state().hot.open, true);
    assert.strictEqual(state().phase, 'hot');
    assert.strictEqual(state().hot.ticker, state().hot.market);
    assert.strictEqual(T.aliasOpen(state().hot.market + '-HOT'), false);
  });
  warpTo('e2e', draw.hot1.activation + plan.hotDuration + 200);
  fireAt('e2e', draw.hot1.activation + plan.hotDuration);
  await ok('Hot #1 ends without hiding its completed audit row', () => {
    const s = state();
    assert.strictEqual(s.hot, null);
    assert.strictEqual(s.hots[0].status, 'complete');
  });
  fireAt('e2e', draw.hot2.activation - plan.hotWarning);
  fireAt('e2e', draw.hot2.activation);
  fireAt('e2e', draw.hot2.activation + plan.hotDuration);
  fireAt('e2e', plan.finalBuildStart);
  warpTo('e2e', plan.boostStart + 200);
  fireAt('e2e', plan.boostStart);
  await ok('boost shows as open on the wall', () => {
    const s = state();
    assert.strictEqual(s.phase, 'boost');
    assert.strictEqual(s.boostOpen, true);
    assert.ok(s.boostMarkets.includes('BTC'), 'and names the markets: ' + JSON.stringify(s.boostMarkets));
  });
  await ok('the wall reports the GATE, not merely the clock', () => {
    // close the twins behind the wall's back: the schedule still says Boost,
    // but the engine would now refuse the order, and the wall must agree
    const opened = state().boostMarkets;
    for (const b of opened) T.closeAlias(b + '-BOOST');
    assert.strictEqual(state().phase, 'boost', 'clock still says boost');
    assert.strictEqual(state().boostOpen, false, 'but the wall must follow the gate');
    for (const b of opened) T.openAlias(b + '-BOOST', 'e2e');
    state();                         // clears durable segment-outage records
    T.__clearPauses();
    comp.pauseClockClose(Date.now());
  });

  console.log('\nbell');
  warpTo('e2e', plan.total + 200);
  fireAt('e2e', plan.total);
  await ok('the round leaves live state when it ends', () => assert.strictEqual(state().live, false));
  r = await admin({ action: 'standings', id: 'e2e', checkpoint: 'final', token: TOKEN });
  await ok('final standings are readable and ranked', () => {
    assert.strictEqual(r.body.board.length, 2);
    assert.strictEqual(r.body.board[0].rank, 1);
  });
  await ok('anyone can verify the complete draw after settlement', () => {
    const v = verify('e2e');
    assert.strictEqual(v.canonical.matches, true);
    assert.strictEqual(v.verified.ok, true);
    assert.strictEqual(v.reveal.draw.version, 2);
  });
  r = await admin({ action: 'firstFive', id: 'e2e', token: TOKEN });
  await ok('the removed First Five result is rejected', () => {
    assert.strictEqual(r.code, 400);
  });
  r = await admin({ action: 'nonsense', id: 'e2e', token: TOKEN });
  await ok('an unknown action is rejected, not ignored', () => assert.strictEqual(r.code, 400));

  console.log('\nthe wall must not look settled while its prices are frozen');
  await ok('a board priced on a refused mark is reported incomplete, and says which market', async () => {
    const id = 'w1';
    await admin({ action: 'create', id, kind: 'rehearsal', candidates: ['BTC', 'SOL', 'ETH'],
      players: [{ userId: SEATS[0], displayName: 'SEATS[0]', seat: 0 }], token: TOKEN });
    await admin({ action: 'start', id, token: TOKEN });
    const ep = T.stmt.acctGet.get(SEATS[0]).epoch;
    T.stmt.posIns.run(SEATS[0], 'BTC', ep, 'LONG', 1, 100, 10, Date.now(), 100, Date.now(), Date.now(), 'cross', 0.1);
    assert.strictEqual(state().complete, true, 'a fully priced board is complete');
    assert.deepStrictEqual(state().stalePricing, [], 'and nothing is stale');

    /* One source drops out: the public feed keeps ticking, competition pricing
       refuses, and the board must admit that rather than showing a frozen
       number as if it were current. */
    /* The engine follows ONE published source now, so a market stops being
       competition-valid when its whole chain goes quiet, not when a second
       venue drifts away from a first. */
    for (const k of ['usdt', 'usdc', 'usd']) T.compUpdate('BTC', k, 100, Date.now() - 60_000);
    assert.strictEqual(T.compPriceReady('BTC'), false, 'precondition: BTC is not competition-valid');
    const st = state();
    assert.strictEqual(st.complete, false,
      'complete:true beside a moving chart is the wrong picture, even if the score is right');
    assert.deepStrictEqual(st.stalePricing, ['BTC'], 'and the frozen market is named');
    await admin({ action: 'abort', id, force: true, reason: 'test', token: TOKEN });
    T.db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(SEATS[0]);
    for (const k of ['usdt', 'usdc', 'usd']) T.compUpdate('BTC', k, 100, Date.now());
  });
  await ok('pre-flight refuses a round that does not exist', async () => {
    /* [].every(...) is true, so an unknown round used to answer
       allReady:true with zero seats and zero markets. The desk lit green and
       the operator only found out at start. */
    const r = await admin({ action: 'preflight', id: 'never-armed-anything', token: TOKEN });
    assert.strictEqual(r.code, 400, JSON.stringify(r.body));
    assert.match(r.body.error, /no such round: never-armed-anything/);
  });
  await ok('Reset all seats leaves a seat the pre-flight calls ready', async () => {
    /* The reset used to clear positions and bump the epoch without stamping
       the contest account spec, so a seat in ordinary paper mode stayed "not
       in stage mode" and the desk offered no way out of its own red light. */
    const rid = 'resetstage';
    let r = await admin({ action: 'create', id: rid, candidates: ['BTC', 'SOL', 'ETH'],
      players: [{ userId: SEATS[0], seat: 0 }], token: TOKEN });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    // put the seat in ordinary paper mode, the state the reset must undo
    T.db.prepare('UPDATE paper_accounts SET heat = 0, start_balance = 10000, balance = 10000 WHERE user_id = ?').run(SEATS[0]);
    r = await admin({ action: 'preflight', id: rid, token: TOKEN });
    assert.strictEqual(r.body.players[0].ready, false, 'precondition: the seat is not ready');
    r = await admin({ action: 'resetPlayers', id: rid, token: TOKEN });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    r = await admin({ action: 'preflight', id: rid, token: TOKEN });
    assert.strictEqual(r.body.players[0].ready, true,
      'the fix button must actually fix what the pre-flight checks');
    await admin({ action: 'abort', id: rid, token: TOKEN });
  });
  console.log('\ninvites: the player claims the seat, the operator never types an id');

  const INV = 'invites';
  await ok('arming mints one unguessable invite per seat, and seats may be empty', async () => {
    const r = await admin({ action: 'create', id: INV, candidates: ['BTC', 'SOL', 'ETH'], token: TOKEN,
      players: [{ displayName: 'Yuki', seat: 0 }, { displayName: 'Mia', seat: 1 }] });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    const pf = await admin({ action: 'preflight', id: INV, token: TOKEN });
    const inv = pf.body.players.map((p) => p.invite);
    assert.strictEqual(inv.length, 2);
    assert.strictEqual(new Set(inv).size, 2, 'two seats must not share one invite');
    for (const x of inv) assert.match(x, /^[0-9a-f]{32}$/, 'the invite is not a guessable code');
    assert.deepStrictEqual(pf.body.players.map((p) => p.claimed), [false, false]);
    /* Each individual seat is "ready" (an empty seat is simply dropped), but a
       roster where nobody has claimed anything is not a startable round. */
    assert.deepStrictEqual(pf.body.players.map((p) => p.ready), [true, true]);
    assert.strictEqual(pf.body.allReady, false,
      'nobody has claimed a seat, so the round is not ready to start');
    assert.match(pf.body.seatsError, /nobody has claimed a seat/);
  });

  await ok('a player claims their seat and toggles ready', () => {
    const pf = CT.q.players.all(INV);
    const tok = pf[0].invite_token;
    const st = comp.claimInvite(tok, SEATS[0]);
    assert.strictEqual(st.seat.user_id, SEATS[0]);
    assert.strictEqual(comp.readinessOf(INV).claimed, 1);
    assert.strictEqual(comp.readinessOf(INV).ready, 0, 'claiming is not being ready');
    comp.setInviteReady(tok, SEATS[0], true);
    assert.strictEqual(comp.readinessOf(INV).ready, 1);
    comp.setInviteReady(tok, SEATS[0], false);
    assert.strictEqual(comp.readinessOf(INV).ready, 0, 'a player who steps away can say so');
    comp.setInviteReady(tok, SEATS[0], true);
  });

  await ok('a later claim TAKES OVER the seat, and one person still cannot hold two', () => {
    /* The link is the seat. An operator who resends it, or a player who opens
       it on a second device, must not be told to go and find the operator.
       The previous holder loses the seat and its ready flag with it. */
    const rows = CT.q.players.all(INV);
    const before = CT.q.bySeat.get(INV, 0);
    assert.strictEqual(before.user_id, SEATS[0]);
    assert.strictEqual(before.ready_at !== null, true, 'precondition: seat 1 was ready');
    const r = comp.claimInvite(rows[0].invite_token, SEATS[1]);
    assert.strictEqual(r.tookOverFrom, SEATS[0], 'the takeover names who lost the seat');
    const after = CT.q.bySeat.get(INV, 0);
    assert.strictEqual(after.user_id, SEATS[1], 'the seat moved to whoever opened the link last');
    assert.strictEqual(after.ready_at, null, 'a new holder has confirmed nothing');
    assert.strictEqual(comp.readinessOf(INV).claimed, 1, 'still one seat claimed, not two');
    /* Put it back, so the rest of this file reads as it did. */
    comp.claimInvite(rows[0].invite_token, SEATS[0]);
    comp.setInviteReady(rows[0].invite_token, SEATS[0], true);
    assert.throws(() => comp.claimInvite(rows[1].invite_token, SEATS[0]), /already hold seat 1/);
    assert.throws(() => comp.setInviteReady(rows[0].invite_token, SEATS[1], true), /not yours/);
    assert.throws(() => comp.claimInvite('0'.repeat(32), SEATS[1]), /not valid/);
  });

  await ok('a seat cannot be taken over once the round is running', () => {
    /* The roster is what the result is computed from, so the bell closes it. */
    const rows = CT.q.players.all(INV);
    CT.db.prepare("UPDATE paper_rounds SET status = 'running' WHERE id = ?").run(INV);
    assert.throws(() => comp.claimInvite(rows[0].invite_token, SEATS[1]), /seats are only claimable before it starts/);
    CT.db.prepare("UPDATE paper_rounds SET status = 'armed' WHERE id = ?").run(INV);
  });

  await ok('starting DROPS the seats nobody claimed, and never waits on ready', async () => {
    /* The whole point of the indicator: the operator starts the show, the
       roster does not. Seat 2 was never claimed and seat 1 is ready, but the
       round would start either way. */
    const before = comp.readinessOf(INV);
    assert.strictEqual(before.total, 2);
    assert.strictEqual(before.claimed, 1);
    const r = await admin({ action: 'start', id: INV, token: TOKEN });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    const after = CT.q.players.all(INV);
    assert.strictEqual(after.length, 1, 'the unclaimed seat is gone, not carried as a zero');
    assert.strictEqual(after[0].user_id, SEATS[0]);
    await admin({ action: 'abort', id: INV, token: TOKEN });
  });

  await ok('a seat cannot be claimed once the round is running', async () => {
    const rid = 'invites2';
    await admin({ action: 'create', id: rid, candidates: ['BTC', 'SOL', 'ETH'], token: TOKEN,
      players: [{ userId: SEATS[0], seat: 0 }, { displayName: 'late', seat: 1 }] });
    const late = CT.q.players.all(rid).find((p) => p.user_id === null).invite_token;
    await admin({ action: 'start', id: rid, token: TOKEN });
    assert.throws(() => comp.claimInvite(late, SEATS[1]), /not valid|is running/,
      'the roster is bound at the bell');
    await admin({ action: 'abort', id: rid, token: TOKEN });
  });




  console.log(`\n${pass} passed${process.exitCode ? ', WITH FAILURES' : ''}\n`);
})();
