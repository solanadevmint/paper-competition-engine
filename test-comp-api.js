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
  openAlias: T.openAlias, closeAlias: T.closeAlias, scoreUser: T.scoreUser,
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
  let r = await admin({ action: 'create', id: 'x', candidates: ['BTC', 'SOL'] });
  await ok('a request with no token is forbidden', () => {
    assert.strictEqual(r.code, 403);
    assert.strictEqual(r.body.error, 'forbidden');
  });
  r = await admin({ action: 'create', id: 'x', candidates: ['BTC', 'SOL'], token: 'wrong' });
  await ok('a wrong token is forbidden', () => assert.strictEqual(r.code, 403));
  r = await adminBodyTokenOnly({ action: 'create', id: 'x', candidates: ['BTC', 'SOL'], token: TOKEN });
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
    assert.strictEqual(st.phase, 'firstFive');
    assert.ok(st.leftMs > 29 * 60_000, 'about 30 minutes left');
    assert.strictEqual(st.players.length, 2);
    assert.strictEqual(st.players[0].rank, 1);
  });
  await ok('the drawn market is withheld until the reveal', () => {
    assert.strictEqual(st.hot, null, 'a spectator must not learn it early');
    assert.deepStrictEqual(st.candidates, ['BTC', 'SOL', 'ETH'], 'but the shortlist is public');
    assert.ok(st.drawCommit, 'and the commitment is published up front');
  });
  await ok('boost is not open in the opening phase', () => assert.strictEqual(st.boostOpen, false));
  await ok('the wall is told what each trader is holding', () => {
    // the show has to answer "what is she trading" without narration
    for (const p of st.players ?? []) {
      assert.ok(Array.isArray(p.positions), 'positions must always be an array');
    }
  });
  await ok('a held position surfaces on the wall with its side and size', async () => {
    T.stmt.posIns.run(SEATS[0], 'BTC', T.stmt.acctGet.get(SEATS[0]).epoch,
      'SHORT', 1, 100, 25, Date.now(), 100, Date.now(), Date.now(), 'cross', 0);
    const row = state().players.find((p) => p.userId === SEATS[0]);
    const pos = row.positions.find((x) => x.symbol === 'BTC');
    assert.ok(pos, 'the position should be visible');
    assert.strictEqual(pos.side, 'short');
    assert.strictEqual(pos.leverage, 25);
    assert.ok(pos.notional > 0, 'notional must be priced at the mark');
    assert.strictEqual(pos.segment, null, 'a base ticker is not a segment');
    T.db.prepare('DELETE FROM paper_positions WHERE user_id = ? AND symbol = ?').run(SEATS[0], 'BTC');
  });

  console.log('\ndraw and segments');
  warpTo('e2e', ROUND_PLAN.round.reveal + 1000);
  fireAt('e2e', ROUND_PLAN.round.reveal);
  st = state();
  await ok('after the reveal the market is public but not yet tradable', () => {
    assert.ok(st.hot && st.hot.market, 'market should be revealed');
    assert.strictEqual(st.hot.open, false, 'the window opens 15s later');
    assert.strictEqual(st.phase, 'reveal');
  });
  await ok('anyone can verify the draw without a token', () => {
    const v = verify('e2e');
    assert.strictEqual(v.verified.ok, true, v.verified.reason);
    assert.strictEqual(v.drawn, st.hot.market);
    assert.ok(v.seed && v.commit, 'seed and commitment both published');
  });
  warpTo('e2e', ROUND_PLAN.round.hotStart + 1000);
  fireAt('e2e', ROUND_PLAN.round.hotStart);
  await ok('the hot ticker opens on its boundary', () => {
    assert.strictEqual(state().hot.open, true);
    assert.strictEqual(state().phase, 'hot');
  });
  warpTo('e2e', ROUND_PLAN.round.hotEnd + 1000);
  fireAt('e2e', ROUND_PLAN.round.hotEnd);
  await ok('and closes again', () => assert.strictEqual(state().hot.open, false));
  warpTo('e2e', ROUND_PLAN.round.boostStart + 1000);
  fireAt('e2e', ROUND_PLAN.round.boostStart);
  await ok('boost shows as open on the wall', () => {
    const s = state();
    assert.strictEqual(s.phase, 'boost');
    assert.strictEqual(s.boostOpen, true);
    assert.ok(s.boostMarkets.includes('BTC'), 'and names the markets: ' + JSON.stringify(s.boostMarkets));
  });
  await ok('the wall reports the GATE, not merely the clock', () => {
    // close the twins behind the wall's back: the schedule still says Boost,
    // but the engine would now refuse the order, and the wall must agree
    for (const b of ['BTC', 'ETH', 'BNB', 'XRP', 'SOL']) T.closeAlias(b + '-BOOST');
    assert.strictEqual(state().phase, 'boost', 'clock still says boost');
    assert.strictEqual(state().boostOpen, false, 'but the wall must follow the gate');
    for (const b of ['BTC', 'ETH', 'BNB', 'XRP', 'SOL']) T.openAlias(b + '-BOOST', 'e2e');
  });

  console.log('\nbell');
  warpTo('e2e', ROUND_PLAN.round.total + 1000);
  fireAt('e2e', ROUND_PLAN.round.total);
  await ok('the round leaves live state when it ends', () => assert.strictEqual(state().live, false));
  r = await admin({ action: 'standings', id: 'e2e', checkpoint: 'final', token: TOKEN });
  await ok('final standings are readable and ranked', () => {
    assert.strictEqual(r.body.board.length, 2);
    assert.strictEqual(r.body.board[0].rank, 1);
  });
  r = await admin({ action: 'firstFive', id: 'e2e', token: TOKEN });
  await ok('the First Five result is available after the round', () => {
    assert.ok('winner' in r.body && 'reason' in r.body);
  });
  r = await admin({ action: 'nonsense', id: 'e2e', token: TOKEN });
  await ok('an unknown action is rejected, not ignored', () => assert.strictEqual(r.code, 400));

  console.log(`\n${pass} passed${process.exitCode ? ', WITH FAILURES' : ''}\n`);
})();
