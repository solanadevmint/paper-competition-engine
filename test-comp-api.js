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
if (!process.env.PAPER_DB || process.env.PAPER_DB.startsWith('/opt/')) {
  console.error('refusing to run: set PAPER_DB to a throwaway path first');
  process.exit(2);
}

const comp = require('./competition.js');
const P = require('./paper.js');
const T = P.__test;
const CT = comp.__test;
const { ROUND_PLAN } = comp;
const TOKEN = process.env.PAPER_COMP_TOKEN;

let pass = 0;
const ok = (name, fn) => {
  try { fn(); console.log('  ok   ' + name); pass++; }
  catch (e) { console.log('  FAIL ' + name + '\n       ' + e.message); process.exitCode = 1; }
};

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
const admin = async (body, headers) => {
  const res = mkRes();
  await P.compAdmin(mkReq(body, headers), res);
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
}
comp.wire({
  openAlias: T.openAlias, closeAlias: T.closeAlias, scoreUser: T.scoreUser,
  // startRound now prepares the roster itself, so it needs the real hooks
  resetPlayer: T.resetPlayerAccount, epochOf: T.epochOfUser,
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
  ok('no token is refused', async () => {});
  let r = await admin({ action: 'create', id: 'x', candidates: ['BTC', 'SOL'] });
  ok('a request with no token is forbidden', () => {
    assert.strictEqual(r.code, 403);
    assert.strictEqual(r.body.error, 'forbidden');
  });
  r = await admin({ action: 'create', id: 'x', candidates: ['BTC', 'SOL'], token: 'wrong' });
  ok('a wrong token is forbidden', () => assert.strictEqual(r.code, 403));
  r = await admin({ action: 'create', id: 'e2e', candidates: ['BTC', 'SOL', 'ETH'], backup: 'XRP', token: TOKEN,
                    players: SEATS.map((u, i) => ({ userId: u, displayName: 'P' + i, seat: i })) });
  ok('the operator token is accepted', () => {
    assert.strictEqual(r.code, 200);
    assert.strictEqual(r.body.round.id, 'e2e');
    assert.strictEqual(r.body.round.status, 'armed');
  });
  ok('the header form of the token works too', async () => {});
  r = await admin({ action: 'standings', id: 'e2e' }, { 'x-comp-token': TOKEN });
  ok('x-comp-token is accepted', () => assert.strictEqual(r.code, 200));

  console.log('\npre-flight');
  r = await admin({ action: 'preflight', id: 'e2e', token: TOKEN });
  ok('reports every seat ready before a round', () => {
    assert.strictEqual(r.body.allReady, true, JSON.stringify(r.body.players));
    assert.strictEqual(r.body.players.length, 2);
  });
  ok('flags a seat that is carrying a position', () => {
    T.stmt.posIns.run(SEATS[0], 'BTC', 1, 'LONG', 1, 100, 10, Date.now(), 100, Date.now(), Date.now(), 'cross', 0);
    const res = mkRes();
    return P.compAdmin(mkReq({ action: 'preflight', id: 'e2e', token: TOKEN }), res).then(() => {
      assert.strictEqual(res.body.allReady, false, 'a leftover position must block the start');
    });
  });
  r = await admin({ action: 'preflight', id: 'e2e', token: TOKEN });
  ok('a stale seat is actually reported not ready', () => assert.strictEqual(r.body.allReady, false));

  console.log('\nreset');
  r = await admin({ action: 'resetPlayers', id: 'e2e', token: TOKEN });
  ok('resetting clears positions and bumps the epoch for everyone', () => {
    assert.strictEqual(r.body.reset.length, 2);
    assert.strictEqual(T.stmt.posByUser.all(SEATS[0]).length, 0, 'positions must be gone');
  });
  r = await admin({ action: 'preflight', id: 'e2e', token: TOKEN });
  ok('pre-flight goes green after a reset', () => assert.strictEqual(r.body.allReady, true));

  console.log('\nlive state');
  ok('nothing is live before the round starts', () => assert.strictEqual(state().live, false));
  await admin({ action: 'start', id: 'e2e', token: TOKEN });
  let st = state();
  ok('the wall sees the round, the phase and the clock', () => {
    assert.strictEqual(st.live, true);
    assert.strictEqual(st.phase, 'firstFive');
    assert.ok(st.leftMs > 29 * 60_000, 'about 30 minutes left');
    assert.strictEqual(st.players.length, 2);
    assert.strictEqual(st.players[0].rank, 1);
  });
  ok('the drawn market is withheld until the reveal', () => {
    assert.strictEqual(st.hot, null, 'a spectator must not learn it early');
    assert.deepStrictEqual(st.candidates, ['BTC', 'SOL', 'ETH'], 'but the shortlist is public');
    assert.ok(st.drawCommit, 'and the commitment is published up front');
  });
  ok('boost is not open in the opening phase', () => assert.strictEqual(st.boostOpen, false));
  ok('the wall is told what each trader is holding', () => {
    // the show has to answer "what is she trading" without narration
    for (const p of st.players ?? []) {
      assert.ok(Array.isArray(p.positions), 'positions must always be an array');
    }
  });
  ok('a held position surfaces on the wall with its side and size', async () => {
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
  CT.fireBoundary('e2e', ROUND_PLAN.round.reveal);
  st = state();
  ok('after the reveal the market is public but not yet tradable', () => {
    assert.ok(st.hot && st.hot.market, 'market should be revealed');
    assert.strictEqual(st.hot.open, false, 'the window opens 15s later');
    assert.strictEqual(st.phase, 'reveal');
  });
  ok('anyone can verify the draw without a token', () => {
    const v = verify('e2e');
    assert.strictEqual(v.verified.ok, true, v.verified.reason);
    assert.strictEqual(v.drawn, st.hot.market);
    assert.ok(v.seed && v.commit, 'seed and commitment both published');
  });
  warpTo('e2e', ROUND_PLAN.round.hotStart + 1000);
  CT.fireBoundary('e2e', ROUND_PLAN.round.hotStart);
  ok('the hot ticker opens on its boundary', () => {
    assert.strictEqual(state().hot.open, true);
    assert.strictEqual(state().phase, 'hot');
  });
  warpTo('e2e', ROUND_PLAN.round.hotEnd + 1000);
  CT.fireBoundary('e2e', ROUND_PLAN.round.hotEnd);
  ok('and closes again', () => assert.strictEqual(state().hot.open, false));
  warpTo('e2e', ROUND_PLAN.round.boostStart + 1000);
  CT.fireBoundary('e2e', ROUND_PLAN.round.boostStart);
  ok('boost shows as open on the wall', () => {
    const s = state();
    assert.strictEqual(s.phase, 'boost');
    assert.strictEqual(s.boostOpen, true);
    assert.ok(s.boostMarkets.includes('BTC'), 'and names the markets: ' + JSON.stringify(s.boostMarkets));
  });
  ok('the wall reports the GATE, not merely the clock', () => {
    // close the twins behind the wall's back: the schedule still says Boost,
    // but the engine would now refuse the order, and the wall must agree
    for (const b of ['BTC', 'ETH', 'BNB', 'XRP', 'SOL']) T.closeAlias(b + '-BOOST');
    assert.strictEqual(state().phase, 'boost', 'clock still says boost');
    assert.strictEqual(state().boostOpen, false, 'but the wall must follow the gate');
    for (const b of ['BTC', 'ETH', 'BNB', 'XRP', 'SOL']) T.openAlias(b + '-BOOST', 'e2e');
  });

  console.log('\nbell');
  warpTo('e2e', ROUND_PLAN.round.total + 1000);
  CT.fireBoundary('e2e', ROUND_PLAN.round.total);
  ok('the round leaves live state when it ends', () => assert.strictEqual(state().live, false));
  r = await admin({ action: 'standings', id: 'e2e', checkpoint: 'final', token: TOKEN });
  ok('final standings are readable and ranked', () => {
    assert.strictEqual(r.body.board.length, 2);
    assert.strictEqual(r.body.board[0].rank, 1);
  });
  r = await admin({ action: 'firstFive', id: 'e2e', token: TOKEN });
  ok('the First Five result is available after the round', () => {
    assert.ok('winner' in r.body && 'reason' in r.body);
  });
  r = await admin({ action: 'nonsense', id: 'e2e', token: TOKEN });
  ok('an unknown action is rejected, not ignored', () => assert.strictEqual(r.code, 400));

  console.log(`\n${pass} passed${process.exitCode ? ', WITH FAILURES' : ''}\n`);
})();
