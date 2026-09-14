'use strict';

/* The contestant leaderboard rides the public index relay. Exercise the real
 * SSE writer and price-ingest path so a refactor cannot leave the scorer
 * correct in isolation while failing to deliver it at price cadence.
 *
 *   PAPER_DB=$(mktemp -u --suffix=.db) node test-comp-push.js
 */
const assert = require('assert');
const { EventEmitter } = require('events');
const fs = require('fs');
const Module = require('module');
const path = require('path');

process.env.PHOENIX_SNAPSHOT_FILE = '/nonexistent/markets-snapshot.json';
process.env.PAPER_ALLOW_UNPROVEN_MARKETS = '1';
/* Make the timing assertions quick while preserving the production coalescer
 * shape. The relay gap is deliberately LONGER than the board gap: one price is
 * published, later accepted prices are thinned, and the score fence must make
 * the newest mark visible before it ranks anyone on that mark. */
process.env.PAPER_COMP_PUSH_MS = '25';
process.env.PAPER_SSE_FAST_GAP_MS = '50';
process.env.PAPER_SSE_GAP_MS = '50';
process.env.PAPER_LAZER_STALE_MS = '140';
process.env.PAPER_GATE_SECRET = 'test-only-gate';
if (!process.env.PAPER_DB || process.env.PAPER_DB.startsWith('/opt/')) {
  console.error('refusing to run: set PAPER_DB to a throwaway path first');
  process.exit(2);
}

const paper = require('./paper.js');
const comp = paper.comp;
const T = paper.__test;
assert.strictEqual(T.compBoardGapMs, 25, 'rank coalescer must honor the price-cadence latency setting');
const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function waitFor(predicate, timeoutMs = 500) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() <= deadline) {
    if (predicate()) return;
    await wait(1);
  }
  throw new Error(`condition was not met within ${timeoutMs}ms`);
}

let passed = 0;
async function ok(name, fn) {
  try {
    await fn();
    console.log('  ok   ' + name);
    passed++;
  } catch (e) {
    console.error('  FAIL ' + name + '\n       ' + (e && e.stack || e));
    process.exitCode = 1;
  }
}

const now = Date.now();
for (const sym of ['BTC', 'SOL']) {
  T.live.map.set(sym, {
    symbol: sym,
    markPrice: 100,
    pythPrice: 100,
    pythAtMs: now,
    pythSrcAtMs: now,
    pythBasis: 0,
    lastUpdatedMs: now,
    indexHalt: false,
    srcKey: 'usdt',
  });
  T.compUpdate(sym, 'usdt', 100, now);
  T.compUpdate(sym, 'usd', 100, now);
  for (let back = 20_000; back >= 0; back -= 2_000) {
    T.recordMark(sym, 100, now - back, 2, 0, 'usdt', now - back);
  }
}

/* The clock and drawdown sampler use these same production hooks. In
 * particular, a price frame is not emitted until tickEval has successfully
 * sampled the live roster. */
comp.wire({
  marketReady: () => true, // Explicit fixture price policy.
  openAlias: T.openAlias,
  closeAlias: T.closeAlias,
  scoreUser: T.scoreUser,
  scoreProofFor: T.scoreProofFor,
  prepareSeat: T.prepareSeat,
  seatState: T.seatState,
  markSetFor: T.markSetFor,
  equityOf: (uid) => {
    const acct = T.stmt.acctGet.get(uid);
    return acct ? T.accountRisk(uid, acct).equityTotal : NaN;
  },
  log: () => {},
});

const USERS = [9301, 9302, 9303, 9304];
for (const uid of USERS) {
  T.db.prepare('INSERT INTO users (id) VALUES (?)').run(uid);
  T.stmt.acctIns.run(uid, now, now);
}
comp.createRound({
  id: 'push',
  candidates: ['BTC', 'SOL', 'ETH'],
  players: USERS.map((userId, seat) => ({ userId, displayName: `P${seat + 1}`, seat })),
});
comp.startRound('push');

/* Seat four begins tied for last by the final seat-number tie-break, then its
 * long moves it to first on the published BTC observation below. */
const trader = USERS[3];
const epoch = T.stmt.acctGet.get(trader).epoch;
T.stmt.posIns.run(trader, 'BTC', epoch, 'LONG', 1, 100, 25,
  now, 100, now, now, 'cross', 0);

const writes = [];
const reqHandlers = {};
const req = {
  headers: { 'x-real-ip': '127.0.0.88' },
  socket: { remoteAddress: '127.0.0.88' },
  on(event, cb) { reqHandlers[event] = cb; return this; },
};
const res = {
  writableLength: 0,
  writeHead() {},
  write(chunk) { writes.push({ at: Date.now(), chunk: String(chunk) }); return true; },
  end() {},
  on() { return this; },
};
function frames() {
  return decodeWrites(writes);
}
function decodeWrites(rows) {
  return rows.flatMap(({ at, chunk }) => chunk.split('\n')
    .filter((line) => line.startsWith('data: '))
    .map((line) => ({ at, value: JSON.parse(line.slice(6)) })))
    .flatMap(({ at, value }) => value && value.type === 'bundle'
      ? value.items.map((item) => ({ at, value: item }))
      : [{ at, value }]);
}
const priceFrames = (all, symbol) => all.filter(({ value }) => value.s === symbol && Number.isFinite(value.p));
const boardFrames = (all) => all.filter(({ value }) => value.type === 'comp');
const wsBundles = (messages) => messages.filter((value) => value.type === 'bundle');
function assertWsBundle(bundle) {
  assert.strictEqual(bundle.v, 2);
  assert.strictEqual(bundle.type, 'bundle');
  assert.match(bundle.boot, /^[0-9a-f]{8}$/);
  assert.ok(Array.isArray(bundle.items) && bundle.items.length >= 1);
  const boards = bundle.items.filter((value) => value.type === 'comp');
  assert.strictEqual(boards.length, 1, 'one atomic WS message carries exactly one board');
  assert.strictEqual(bundle.items[bundle.items.length - 1], boards[0],
    'the board is the final item in its atomic fence');
  for (const value of bundle.items) assert.strictEqual(value.boot, bundle.boot);
  for (const value of bundle.items.slice(0, -1)) {
    assert.ok(Number.isFinite(value.p));
    assert.strictEqual(value.snap, 1);
    assert.strictEqual(value.fence, 1);
  }
  assertCauseSchema(boards[0]);
  return boards[0];
}
function assertCauseSchema(board) {
  assert.ok(Array.isArray(board.causes), 'every compact board carries a causes array');
  assert.strictEqual(typeof board.causesComplete, 'boolean');
  let eventCount = 0;
  for (const cause of board.causes) {
    assert.ok(Array.isArray(cause), 'a cause is a compact tuple');
    assert.strictEqual(cause.length, 5,
      'cause tuple is [symbol, referenceMark, executionPrice, at, identicalCount]');
    assert.match(cause[0], /^[A-Z0-9][A-Z0-9-]*$/);
    assert.ok(Number.isFinite(cause[1]) && cause[1] > 0);
    assert.ok(Number.isFinite(cause[2]) && cause[2] > 0);
    assert.ok(Number.isSafeInteger(cause[3]) && cause[3] > 0);
    assert.ok(Number.isSafeInteger(cause[4]) && cause[4] > 0);
    eventCount += cause[4];
  }
  assert.ok(eventCount <= 512, 'expanded fill causes stay within the event bound');
  return eventCount;
}
function exactCause(board, expected) {
  assertCauseSchema(board);
  const tuple = [expected.symbol, expected.referenceMark, expected.executionPrice, expected.at];
  const got = board.causes.find((cause) => cause[0] === expected.symbol && cause[3] === expected.at);
  assert.deepStrictEqual(got && got.slice(0, 4), tuple);
  return got;
}
function boardWithCause(all, expected) {
  return boardFrames(all).find(({ value }) => value.causes.some((cause) =>
    cause[0] === expected.symbol && cause[1] === expected.referenceMark
      && cause[2] === expected.executionPrice && cause[3] === expected.at));
}
function cleanSuccessor(boards, causal) {
  const at = boards.indexOf(causal);
  assert.ok(at >= 0 && at + 1 < boards.length,
    'a causal revision is followed by a clean reconnect-safe revision');
  const clean = boards[at + 1];
  assert.strictEqual(clean.value.q, causal.value.q + 1);
  assert.strictEqual(clean.value.causesComplete, true);
  assert.deepStrictEqual(clean.value.causes, []);
  return clean;
}
function extraSse(ip, writableLength = 0) {
  const out = [];
  const handlers = {};
  let destroyed = 0;
  const extraReq = {
    headers: { 'x-real-ip': ip }, socket: { remoteAddress: ip },
    on(event, cb) { handlers[event] = cb; return this; },
  };
  const extraRes = {
    writableLength,
    writeHead() {},
    write(chunk) { out.push({ at: Date.now(), chunk: String(chunk) }); return true; },
    end() {},
    destroy() { destroyed++; },
    on() { return this; },
  };
  return {
    req: extraReq, res: extraRes, writes: out,
    close: () => { if (handlers.close) handlers.close(); },
    destroyed: () => destroyed,
  };
}
function callJson(handler, ip) {
  let status = 0;
  let headers = null;
  let body = '';
  handler({ headers: { 'x-real-ip': ip } }, {
    writeHead(code, nextHeaders) { status = code; headers = nextHeaders; },
    end(chunk) { body += chunk == null ? '' : String(chunk); },
  });
  return { status, headers, body: JSON.parse(body) };
}

/* Load the exact production module text in a private CommonJS wrapper and
 * expose only startWs in that wrapper. This lets the regression drive the real
 * marketStats callback with a fake socket, without adding a test-only export
 * to paper.js or starting any of its external feed clients. */
function paperWithFakeVenueSocket(FakeWebSocket) {
  const filename = require.resolve('./paper.js');
  const source = fs.readFileSync(filename, 'utf8')
    + '\nmodule.exports.__startVenueWsForTest = startWs;\n';
  const isolated = new Module(filename + ':market-stats-test', module);
  isolated.filename = filename;
  isolated.paths = Module._nodeModulePaths(path.dirname(filename));
  const nativeRequire = isolated.require.bind(isolated);
  isolated.require = (request) => request === 'ws' ? FakeWebSocket : nativeRequire(request);
  isolated._compile(source, filename);
  return isolated.exports;
}

let initialBoardRevision = 0;
let engineBoot = null;

(async () => {
  paper.pythStream(req, res);
  await wait(60);

  await ok('a new relay receives one compact board with deterministic tied ranks', () => {
    const boards = boardFrames(frames());
    assert.strictEqual(boards.length, 1, 'connection refreshes coalesce into one board');
    const board = boards[0].value;
    assert.strictEqual(board.v, 2);
    assert.strictEqual(board.type, 'comp');
    assert.match(board.boot, /^[0-9a-f]{8}$/);
    assert.ok(Number.isSafeInteger(board.q) && board.q > 0);
    initialBoardRevision = board.q;
    engineBoot = board.boot;
    assert.ok(Number.isSafeInteger(board.t) && board.t > 0);
    assert.strictEqual(board.live, true);
    assert.strictEqual(board.roundId, 'push');
    assert.strictEqual(board.complete, true);
    assert.deepStrictEqual(board.unscored, []);
    assert.deepStrictEqual(board.stalePricing, []);
    assertCauseSchema(board);
    assert.deepStrictEqual(board.causes, []);
    assert.deepStrictEqual(board.players.map((p) => p.userId), USERS,
      'score, drawdown and realized all tie, so seat is the final deterministic tie-break');
    assert.deepStrictEqual(board.players.map((p) => p.rank), [1, 2, 3, 4]);
  });

  await ok('time and reconnect-baseline handlers expose uncached engine identity', () => {
    const before = Date.now();
    const clock = callJson(paper.engineTime, '127.0.0.94');
    const after = Date.now();
    assert.strictEqual(clock.status, 200);
    assert.deepStrictEqual(Object.keys(clock.body).sort(), ['boot', 'ok', 't', 'v']);
    assert.strictEqual(clock.body.ok, true);
    assert.strictEqual(clock.body.v, 2);
    assert.strictEqual(clock.body.boot, engineBoot);
    assert.ok(clock.body.t >= before && clock.body.t <= after);

    const baseline = callJson(paper.compBaseline, '127.0.0.95');
    assert.strictEqual(baseline.status, 200);
    assert.strictEqual(baseline.body.ok, true);
    assert.strictEqual(baseline.body.live, true);
    assert.strictEqual(baseline.body.engineBoot, engineBoot);
    assert.strictEqual(baseline.body.compRevision, initialBoardRevision);
    assert.strictEqual(Object.hasOwn(baseline.body, 'cachedMs'), false,
      'the reconnect baseline bypasses the shared wall cache');
    assert.ok(Array.isArray(baseline.body.players));
    for (const response of [clock, baseline]) {
      assert.strictEqual(response.headers['content-type'], 'application/json');
      assert.strictEqual(response.headers['cache-control'], 'no-store, private');
      assert.strictEqual(response.headers.pragma, 'no-cache');
      assert.strictEqual(response.headers['x-content-type-options'], 'nosniff');
    }
  });

  await ok('compact live scoring uses the round-bound starting balance', () => {
    const bound = comp.playersOf('push').find((p) => p.user_id === trader);
    const account = T.stmt.acctGet.get(trader);
    assert.strictEqual(bound.start_balance, 10);
    try {
      T.db.prepare('UPDATE paper_accounts SET start_balance = ? WHERE user_id = ?')
        .run(1, trader);
      assert.strictEqual(T.scoreUser(trader, null, bound.epoch).accountPnl, 9,
        'precondition: the mutable account baseline would fabricate nine dollars');
      const board = T.compRankSnapshot();
      const mine = board.players.find((p) => p.userId === trader);
      assert.strictEqual(board.complete, true);
      assert.strictEqual(mine.accountPnl, 0);
      assert.strictEqual(mine.score, 0,
        'live REST/push scoring stays on the same baseline as checkpoints');
    } finally {
      T.db.prepare('UPDATE paper_accounts SET start_balance = ? WHERE user_id = ?')
        .run(account.start_balance, trader);
    }
  });

  const venueSockets = [];
  class FakeVenueWebSocket extends EventEmitter {
    constructor(url) {
      super();
      this.url = url;
      this.readyState = 1;
      this.sent = [];
      venueSockets.push(this);
    }
    send(payload) { this.sent.push(String(payload)); }
  }
  FakeVenueWebSocket.OPEN = 1;
  FakeVenueWebSocket.Server = class {};
  const overlayPaper = paperWithFakeVenueSocket(FakeVenueWebSocket);
  const overlayT = overlayPaper.__test;
  const overlaySeedAt = Date.now();
  overlayT.live.map.set('BTC', {
    symbol: 'BTC', markPrice: 100, pythBasis: 0, indexHalt: false,
  });
  overlayT.compUpdate('BTC', 'usdt', 100, overlaySeedAt, overlaySeedAt);
  overlayT.compUpdate('BTC', 'usd', 100, overlaySeedAt, overlaySeedAt);
  const overlaySse = extraSse('127.0.0.96');
  overlayPaper.pythStream(overlaySse.req, overlaySse.res);
  await wait(70);
  overlaySse.writes.length = 0;
  overlayPaper.__startVenueWsForTest();
  const venueSocket = venueSockets.find((ws) =>
    ws.url === 'wss://perp-api.phoenix.trade/v1/ws');
  assert.ok(venueSocket, 'the isolated production marketStats socket was created');

  const overlayAcceptedAt = Math.max(Date.now(), overlaySeedAt + 1);
  const overlaySourceAt = overlayAcceptedAt - 17;
  const overlayPrice = 100.002;
  overlayT.ingestIndexTick('BTC', overlayPrice, overlayAcceptedAt,
    [overlayPrice, overlayPrice], 0, 'usdt', overlaySourceAt);
  const acceptedIdentity = { ...overlayT.live.map.get('BTC') };
  assert.ok(Number.isSafeInteger(acceptedIdentity.acceptedSeq)
    && acceptedIdentity.acceptedSeq > 0);
  venueSocket.emit('message', Buffer.from(JSON.stringify({
    channel: 'marketStats', symbol: 'BTC', markPrice: 99.95,
    oraclePrice: 99.96, currentFundingRate: 0.001,
    eightHourFundingRate: 0.008,
  })));
  const rewrittenIdentity = overlayT.live.map.get('BTC');
  await wait(70);

  await ok('a marketStats rewrite preserves the accepted mark identity through its fence', () => {
    assert.strictEqual(rewrittenIdentity.pythSrcAtMs, acceptedIdentity.pythSrcAtMs);
    assert.strictEqual(rewrittenIdentity.srcKey, acceptedIdentity.srcKey);
    assert.strictEqual(rewrittenIdentity.acceptedSeq, acceptedIdentity.acceptedSeq);
    assert.strictEqual(rewrittenIdentity.pythAtMs, acceptedIdentity.pythAtMs);
    assert.strictEqual(rewrittenIdentity.pythPrice, acceptedIdentity.pythPrice);

    const all = decodeWrites(overlaySse.writes);
    const boards = boardFrames(all);
    const fence = priceFrames(all, 'BTC').find(({ value }) => value.fence === 1);
    assert.strictEqual(boards.length, 1);
    assert.ok(fence, 'the pending compact board carries an explicit BTC fence');
    assert.ok(all.indexOf(fence) < all.indexOf(boards[0]));
    assert.strictEqual(fence.value.p, overlayPrice);
    assert.strictEqual(fence.value.t, overlayAcceptedAt);
    assert.strictEqual(fence.value.src, 'usdt');
    assert.strictEqual(fence.value.aq, acceptedIdentity.acceptedSeq);
    assert.strictEqual(fence.value.x,
      overlaySourceAt + overlayT.staleMsForSym('BTC', 'usdt'));
  });
  overlaySse.close();

  writes.length = 0;
  const sentAt = Date.now();
  T.ingestIndexTick('BTC', 100.04, sentAt, [100.04, 100.04], 0, 'usdt', sentAt);
  T.ingestIndexTick('BTC', 100.041, sentAt + 1, [100.041, 100.041], 0, 'usdt', sentAt + 1);
  await wait(70);

  await ok('a thinned newer mark is fenced before the compact board that uses it', () => {
    const all = frames();
    const prices = priceFrames(all, 'BTC');
    const boards = boardFrames(all);
    assert.strictEqual(prices.length, 2, 'one live price plus one score-fence snapshot');
    assert.strictEqual(boards.length, 1);
    assert.ok(!prices[0].value.snap, 'the cadence-admitted event is live');
    assert.strictEqual(prices[1].value.snap, 1, 'the thinned mark is explicitly fenced as a snapshot');
    assert.strictEqual(prices[1].value.fence, 1, 'the score basis is distinguishable from reconnect replay');
    assert.strictEqual(prices[1].value.p, 100.041);
    assert.ok(all.indexOf(prices[1]) < all.indexOf(boards[0]), 'the explaining price must arrive first');
    const mine = boards[0].value.players.find((p) => p.userId === trader);
    assert.strictEqual(mine.rank, 1);
    assert.strictEqual(mine.score, 0.041);
    assertCauseSchema(boards[0].value);
    assert.ok(boards[0].at - prices[0].at >= 20, 'the board passed through the coalescer');
    assert.ok(boards[0].at - sentAt < 250, 'the board remains at live-PnL cadence');
  });

  writes.length = 0;
  const burstAt = Date.now();
  for (let i = 1; i <= 20; i++) {
    const px = 100.04 + i / 10_000;
    T.ingestIndexTick('BTC', px, burstAt + i, [px, px], 0, 'usdt', burstAt + i);
  }
  await wait(70);

  await ok('a burst coalesces to one fenced latest mark and one board', () => {
    const all = frames();
    const prices = priceFrames(all, 'BTC');
    const boards = boardFrames(all);
    const fences = prices.filter(({ value }) => value.fence === 1);
    assert.strictEqual(fences.length, 1, 'the whole burst owns one latest-mark fence');
    assert.ok(prices.some(({ value }) => !value.snap),
      'cadence/material admission may publish one or more live events');
    assert.strictEqual(boards.length, 1, 'one process-wide timer coalesces the burst');
    assert.strictEqual(fences[0].value.p, 100.042);
    assert.strictEqual(fences[0].value.snap, 1);
    assert.ok(all.indexOf(fences[0]) < all.indexOf(boards[0]),
      'the coalesced board follows the final price in the burst');
    const mine = boards[0].value.players.find((p) => p.userId === trader);
    assert.strictEqual(mine.score, 0.042, 'the board uses the newest published mark');
    assert.strictEqual(mine.rank, 1);
    assertCauseSchema(boards[0].value);
  });

  /* P establishes a new equity peak and is cadence-admitted. Q arrives one
     millisecond later and would ordinarily be thinned, but it deepens the
     published max-drawdown tie-break. R recovers immediately. Q therefore has
     to cross the relay as a material live event before the board is allowed to
     publish the drawdown it caused; the coalesced score basis itself is R. */
  writes.length = 0;
  const drawdownAt = Date.now();
  const peakP = 100.05;
  const dipQ = 100.045;
  const recoveredR = 100.05;
  T.ingestIndexTick('BTC', peakP, drawdownAt, [peakP, peakP], 0, 'usdt', drawdownAt);
  T.ingestIndexTick('BTC', dipQ, drawdownAt + 1, [dipQ, dipQ], 0, 'usdt', drawdownAt + 1);
  T.ingestIndexTick('BTC', recoveredR, drawdownAt + 2,
    [recoveredR, recoveredR], 0, 'usdt', drawdownAt + 2);
  await wait(70);

  await ok('a cadence-thinned drawdown dip is material before its tie-break update', () => {
    const all = frames();
    const prices = priceFrames(all, 'BTC');
    const boards = boardFrames(all);
    assert.strictEqual(prices.length, 3,
      'P and material Q are live; thinned recovery R is carried by the score fence');
    assert.strictEqual(prices[0].value.p, peakP);
    assert.ok(!prices[0].value.m && !prices[0].value.snap);
    assert.strictEqual(prices[1].value.p, dipQ);
    assert.strictEqual(prices[1].value.m, 1,
      'Q bypasses cadence because it changed the published drawdown tie-break');
    assert.ok(!prices[1].value.snap, 'Q is the actual accepted event, not reconstructed history');
    assert.strictEqual(prices[2].value.p, recoveredR);
    assert.strictEqual(prices[2].value.snap, 1);
    assert.strictEqual(prices[2].value.fence, 1);
    assert.strictEqual(boards.length, 1);
    assert.ok(all.indexOf(prices[1]) < all.indexOf(boards[0]),
      'the board cannot expose Q\'s drawdown before Q itself is visible');
    const mine = boards[0].value.players.find((p) => p.userId === trader);
    assert.ok(Math.abs(mine.maxDrawdown - (peakP - dipQ)) < 1e-9,
      `expected the ${peakP - dipQ} Q dip, got ${mine.maxDrawdown}`);
    assert.strictEqual(mine.score, 0.05,
      'current PnL uses recovered R while historical drawdown retains Q');
    assertCauseSchema(boards[0].value);
  });

  /* This is deliberately two coalescer generations, not a same-burst test.
     P publishes and its board finishes while the 50ms relay gate is still
     closed. Q then becomes the final accepted tick. Even though Q has no
     ordinary price event and no later tick wakes the engine, it owns a new
     fence and compact revision. */
  await wait(55);
  writes.length = 0;
  const priorTickAt = Date.now();
  const priorTick = 100.051;
  T.ingestIndexTick('BTC', priorTick, priorTickAt,
    [priorTick, priorTick], 0, 'usdt', priorTickAt);
  await waitFor(() => boardFrames(frames()).length === 1, 45);
  const priorCadenceBoard = boardFrames(frames())[0].value;
  assert.ok(Date.now() - priorTickAt < 50,
    'precondition: the prior board flushed before the relay gate reopened');

  writes.length = 0;
  const loneThinAt = Math.max(Date.now(), priorTickAt + 1);
  const loneThinMark = 100.052;
  T.ingestIndexTick('BTC', loneThinMark, loneThinAt,
    [loneThinMark, loneThinMark], 0, 'usdt', loneThinAt);
  await wait(70);

  await ok('a lone cadence-thinned tick after a prior flush owns a new board', () => {
    const all = frames();
    const prices = priceFrames(all, 'BTC');
    const boards = boardFrames(all);
    assert.strictEqual(prices.length, 1,
      'the thinned final tick appears only as its authoritative score fence');
    assert.strictEqual(prices[0].value.p, loneThinMark);
    assert.strictEqual(prices[0].value.snap, 1);
    assert.strictEqual(prices[0].value.fence, 1);
    assert.strictEqual(boards.length, 1, 'Q schedules without needing a later tick');
    assert.ok(all.indexOf(prices[0]) < all.indexOf(boards[0]));
    assert.ok(boards[0].value.q > priorCadenceBoard.q);
    assertCauseSchema(boards[0].value);
    const rest = T.compRankSnapshot();
    assert.strictEqual(rest.complete, true);
    assert.deepStrictEqual(boards[0].value.players, rest.players,
      'push and live REST rank the same exact final mark');
    assert.strictEqual(rest.players.find((p) => p.userId === trader).score, 0.052);
  });

  /* applyFill owns an inner SAVEPOINT, but callers such as liquidation sweeps
     may wrap several fills in one outer transaction. Its return is therefore
     not proof that the outer transaction ultimately committed. SQLite also
     reuses the rolled-back row id here, so checking only "does this id exist"
     would accidentally authenticate the ghost against the next real fill. */
  const rollbackAt = Date.now();
  T.compUpdate('RBACK', 'lazer', 99, rollbackAt, rollbackAt);
  T.compUpdate('RGOOD', 'lazer', 100, rollbackAt, rollbackAt);
  writes.length = 0;
  let rolledFill;
  assert.throws(() => T.db.transaction(() => {
    rolledFill = T.applyFill(USERS[0], {
      symbol: 'RBACK', orderSide: 'BUY', size: 1, px: 99, feeBps: 0,
      kind: 'MARKET', leverage: 25, marginMode: 'cross', at: rollbackAt,
      executionSource: 'test-index', referenceMark: 99,
    });
    throw new Error('outer rollback');
  })(), /outer rollback/);
  assert.strictEqual(T.db.prepare(
    'SELECT COUNT(*) AS n FROM paper_fills WHERE user_id = ? AND symbol = ? AND ts = ?')
    .get(USERS[0], 'RBACK', rollbackAt).n, 0);
  assert.strictEqual(T.db.prepare(
    'SELECT COUNT(*) AS n FROM paper_positions WHERE user_id = ? AND symbol = ?')
    .get(USERS[0], 'RBACK').n, 0);

  const validAt = rollbackAt + 1;
  const validOpen = T.applyFill(USERS[0], {
    symbol: 'RGOOD', orderSide: 'BUY', size: 1, px: 100, feeBps: 0,
    kind: 'MARKET', leverage: 25, marginMode: 'cross', at: validAt,
    executionSource: 'test-index', referenceMark: 100,
  });
  T.applyFill(USERS[0], {
    symbol: 'RGOOD', orderSide: 'SELL', size: 1, px: 100, feeBps: 0,
    kind: 'MARKET', leverage: 25, marginMode: 'cross', at: validAt + 1,
    executionSource: 'test-index', referenceMark: 100,
  });
  assert.strictEqual(validOpen.id, rolledFill.id,
    'the committed row deterministically reuses the rolled-back id');
  await wait(70);

  await ok('an outer transaction rollback cannot leak a ghost fill cause', () => {
    const all = frames();
    const boards = boardFrames(all);
    const causal = boardWithCause(all, {
      symbol: 'RGOOD', referenceMark: 100, executionPrice: 100, at: validAt,
    });
    assert.ok(causal);
    assertCauseSchema(causal.value);
    assert.ok(!boards.some(({ value }) => value.causes.some((cause) => cause[0] === 'RBACK')),
      'pending metadata is verified against the persisted fill contents');
    exactCause(causal.value, {
      symbol: 'RGOOD', referenceMark: 100, executionPrice: 100, at: validAt,
    });
    cleanSuccessor(boards, causal);
  });

  writes.length = 0;
  const hiddenAt = Date.now();
  const referenceQ = 100.05;
  const currentR = 100.06;
  const priorBtc = T.live.map.get('BTC');
  T.live.map.set('BTC', { ...priorBtc, pythPrice: referenceQ, pythAtMs: hiddenAt,
    pythSrcAtMs: hiddenAt, lastUpdatedMs: hiddenAt });
  T.applyFill(trader, {
    symbol: 'BTC', orderSide: 'BUY', size: 1, px: referenceQ, feeBps: 0,
    kind: 'MARKET', leverage: 25, marginMode: 'cross', at: hiddenAt,
    executionSource: 'test-index', referenceMark: referenceQ,
  });
  /* The ranking fence captures current state, not history. Move the live mark
     after the fill but before the process-wide timer: the price frame must be
     R while the accompanying cause still names the exact Q fill. */
  T.live.map.set('BTC', { ...T.live.map.get('BTC'), pythPrice: currentR,
    pythAtMs: hiddenAt + 1, pythSrcAtMs: hiddenAt + 1, lastUpdatedMs: hiddenAt + 1 });
  await wait(70);

  await ok('a Q fill keeps its exact cause while the board is fenced to newer R', () => {
    const all = frames();
    const prices = priceFrames(all, 'BTC');
    const boards = boardFrames(all);
    const causal = boardWithCause(all, {
      symbol: 'BTC', referenceMark: referenceQ, executionPrice: referenceQ, at: hiddenAt,
    });
    assert.ok(causal);
    const causalFence = prices.find((row) => all.indexOf(row) < all.indexOf(causal));
    assert.ok(causalFence, 'the forced board emits its current price basis');
    assert.strictEqual(causalFence.value.snap, 1);
    assert.strictEqual(causalFence.value.fence, 1);
    assert.strictEqual(causalFence.value.p, currentR);
    const mine = causal.value.players.find((p) => p.userId === trader);
    assert.strictEqual(mine.score, 0.07,
      'the old and new lots are both valued at current R');
    exactCause(causal.value, {
      symbol: 'BTC', referenceMark: referenceQ, executionPrice: referenceQ, at: hiddenAt,
    });
    cleanSuccessor(boards, causal);
  });

  /* A closed leg is absent from the scorer's current exposure scan, so the
     fill-dirty set has to carry its causal market into the fence. This is the
     same ordering requirement as a cross liquidation that closes another
     symbol while the triggering market is being processed. */
  const solOpenAt = Date.now();
  T.applyFill(trader, {
    symbol: 'SOL', orderSide: 'BUY', size: 1, px: 100, feeBps: 0,
    kind: 'MARKET', leverage: 25, marginMode: 'cross', at: solOpenAt,
    executionSource: 'test-index', referenceMark: 100,
  });
  await wait(70);
  writes.length = 0;
  const solHiddenAt = Date.now();
  const priorSol = T.live.map.get('SOL');
  T.live.map.set('SOL', { ...priorSol, pythPrice: 101, pythAtMs: solHiddenAt,
    pythSrcAtMs: solHiddenAt, lastUpdatedMs: solHiddenAt });
  T.applyFill(trader, {
    symbol: 'SOL', orderSide: 'SELL', size: 1, px: 101, feeBps: 0,
    kind: 'LIQUIDATION', leverage: 25, marginMode: 'cross', at: solHiddenAt,
    executionSource: 'test-index', referenceMark: 101,
  });
  await wait(70);

  await ok('a closed cross-symbol leg remains in the causal price fence', () => {
    const all = frames();
    const sol = priceFrames(all, 'SOL');
    const boards = boardFrames(all);
    assert.strictEqual(sol.length, 1);
    assert.strictEqual(sol[0].value.snap, 1);
    assert.strictEqual(sol[0].value.fence, 1);
    assert.strictEqual(sol[0].value.p, 101);
    const causal = boardWithCause(all, {
      symbol: 'SOL', referenceMark: 101, executionPrice: 101, at: solHiddenAt,
    });
    assert.ok(causal);
    assert.ok(all.indexOf(sol[0]) < all.indexOf(causal));
    const mine = causal.value.players.find((p) => p.userId === trader);
    assert.strictEqual(mine.score, 1.07);
    exactCause(causal.value, {
      symbol: 'SOL', referenceMark: 101, executionPrice: 101, at: solHiddenAt,
    });
    cleanSuccessor(boards, causal);
  });

  writes.length = 0;
  const reconnectAt = Date.now();
  const reconnectMark = 100.055;
  const reconnectBase = T.live.map.get('BTC');
  /* Simulate an accepted mark that arrived inside the ordinary relay cadence.
     A reconnect snapshot may see it even though no regular event carried it;
     the connection's first board still needs the same mark immediately ahead
     of it, with no replay from an older score basis. */
  T.live.map.set('BTC', { ...reconnectBase, pythPrice: reconnectMark,
    pythAtMs: reconnectAt, pythSrcAtMs: reconnectAt, lastUpdatedMs: reconnectAt });
  const reconnect = extraSse('127.0.0.89');
  paper.pythStream(reconnect.req, reconnect.res);

  await ok('a reconnect never receives a cached board before its price fence', () => {
    assert.strictEqual(boardFrames(decodeWrites(reconnect.writes)).length, 0,
      'the synchronous connection snapshot contains prices and hello only');
  });
  await wait(70);

  await ok('a reconnect board is fenced to the exact price snapshot it received', () => {
    const all = decodeWrites(reconnect.writes);
    const boards = boardFrames(all);
    assert.strictEqual(boards.length, 1);
    const boardAt = all.indexOf(boards[0]);
    const matching = all.findIndex(({ value }, i) => i < boardAt
      && value.s === 'BTC' && value.snap === 1 && value.fence === 1 && value.p === reconnectMark);
    assert.ok(matching >= 0, 'the reconnect received the board\'s BTC basis first');
    const mine = boards[0].value.players.find((p) => p.userId === trader);
    assert.strictEqual(mine.score, 1.06);
    assertCauseSchema(boards[0].value);
  });
  reconnect.close();

  /* An SSE client above the bundle ceiling must receive neither half of the
     fence and must release the socket memory immediately. Initial connection
     snapshots are cleared here; this assertion is about the atomic score
     bundle scheduled after the client joins. */
  const slowSse = extraSse('127.0.0.90', 262_144);
  paper.pythStream(slowSse.req, slowSse.res);
  slowSse.writes.length = 0;
  await wait(70);

  await ok('SSE backpressure drops the whole score fence and destroys the socket', () => {
    assert.strictEqual(boardFrames(decodeWrites(slowSse.writes)).length, 0);
    assert.strictEqual(slowSse.writes.length, 0, 'no partial price basis was written');
    assert.strictEqual(slowSse.destroyed(), 1, 'queued socket memory is released');
  });

  /* Drive attachIndexWs with a fake upgrade server so bufferedAmount can be
     pinned deterministically. This reaches the actual WS admission branch
     without opening a port or depending on kernel buffer timing. */
  const Ws = require('ws');
  const RealWsServer = Ws.Server;
  let upgrade = null;
  class FakeWsServer {
    handleUpgrade(_req, socket, _head, done) { done(socket.fakeWs); }
  }
  Ws.Server = FakeWsServer;
  try {
    paper.attachIndexWs({ on(event, cb) { if (event === 'upgrade') upgrade = cb; } });
  } finally {
    Ws.Server = RealWsServer;
  }
  const fakeWs = new EventEmitter();
  fakeWs.readyState = 1;
  fakeWs.bufferedAmount = 65_536;
  fakeWs.sent = [];
  fakeWs.raw = [];
  fakeWs.send = (payload) => {
    fakeWs.raw.push(String(payload));
    fakeWs.sent.push(JSON.parse(String(payload)));
  };
  fakeWs.ping = () => {};
  fakeWs.terminate = () => { fakeWs.terminated = true; };
  const rawSocket = { fakeWs, remoteAddress: '127.0.0.91', destroy() {} };
  upgrade({
    url: '/api/paper/index-ws',
    headers: { 'x-paper-gate': 'test-only-gate', 'x-real-ip': '127.0.0.91' },
    socket: rawSocket,
  }, rawSocket, Buffer.alloc(0));
  fakeWs.sent.length = 0;
  fakeWs.raw.length = 0;

  const throwWs = new EventEmitter();
  throwWs.readyState = 1;
  throwWs.bufferedAmount = 0;
  throwWs.sent = [];
  throwWs.raw = [];
  throwWs.send = (payload) => {
    throwWs.raw.push(String(payload));
    throwWs.sent.push(JSON.parse(String(payload)));
  };
  throwWs.ping = () => {};
  throwWs.terminate = () => { throwWs.terminated = true; };
  const throwSocket = { fakeWs: throwWs, remoteAddress: '127.0.0.93', destroy() {} };
  upgrade({
    url: '/api/paper/index-ws',
    headers: { 'x-paper-gate': 'test-only-gate', 'x-real-ip': '127.0.0.93' },
    socket: throwSocket,
  }, throwSocket, Buffer.alloc(0));
  throwWs.sent.length = 0;
  throwWs.raw.length = 0;
  let scoreBundleSends = 0;
  throwWs.send = (payload) => {
    const raw = String(payload);
    const value = JSON.parse(raw);
    if (value.type === 'bundle') {
      scoreBundleSends++;
      throw new Error('deterministic atomic-bundle failure');
    }
    throwWs.raw.push(raw);
    throwWs.sent.push(value);
  };

  writes.length = 0;
  const wsDropAt = Date.now();
  T.compUpdate('WDROP', 'lazer', 100, wsDropAt, wsDropAt);
  /* The source update above is an ordinary single-price message. The injected
     failure counts only the subsequent atomic competition bundle. */
  throwWs.sent.length = 0;
  throwWs.raw.length = 0;
  scoreBundleSends = 0;
  T.applyFill(USERS[0], {
    symbol: 'WDROP', orderSide: 'BUY', size: 1, px: 100, feeBps: 0,
    kind: 'MARKET', leverage: 25, marginMode: 'cross', at: wsDropAt,
    executionSource: 'test-index', referenceMark: 100,
  });
  T.applyFill(USERS[0], {
    symbol: 'WDROP', orderSide: 'SELL', size: 1, px: 100, feeBps: 0,
    kind: 'MARKET', leverage: 25, marginMode: 'cross', at: wsDropAt + 1,
    executionSource: 'test-index', referenceMark: 100,
  });
  await wait(70);

  await ok('WS backpressure drops the whole score fence and socket', () => {
    assert.strictEqual(fakeWs.sent.length, 0, 'neither price snapshots nor board fit the budget');
    assert.strictEqual(fakeWs.terminated, true,
      'a client that missed an atomic score revision must reconnect');
    const all = frames();
    const healthyBoards = boardFrames(all);
    const causal = boardWithCause(all, {
      symbol: 'WDROP', referenceMark: 100, executionPrice: 100, at: wsDropAt,
    });
    assert.ok(causal);
    exactCause(causal.value, {
      symbol: 'WDROP', referenceMark: 100, executionPrice: 100, at: wsDropAt,
    });
    cleanSuccessor(healthyBoards, causal);
  });

  await ok('one atomic WS send exception drops the stream without a partial fence', () => {
    assert.strictEqual(scoreBundleSends, 1, 'the full fence and board use one send call');
    assert.strictEqual(throwWs.sent.length, 0, 'no application-level fragment was accepted');
    assert.strictEqual(throwWs.raw.length, 0);
    assert.strictEqual(throwWs.terminated, true,
      'a failed atomic revision requires a fresh transport baseline');
  });

  /* A replacement connection starts a new observable stream lifecycle. It
     gets hello before its first compact revision and therefore cannot appear
     to have remained live while silently jumping over the dropped cause. */
  const freshWs = new EventEmitter();
  freshWs.readyState = 1;
  freshWs.bufferedAmount = 0;
  freshWs.sent = [];
  freshWs.raw = [];
  freshWs.send = (payload) => {
    freshWs.raw.push(String(payload));
    freshWs.sent.push(JSON.parse(String(payload)));
  };
  freshWs.ping = () => {};
  freshWs.terminate = () => { freshWs.terminated = true; };
  const freshSocket = { fakeWs: freshWs, remoteAddress: '127.0.0.92', destroy() {} };
  upgrade({
    url: '/api/paper/index-ws',
    headers: { 'x-paper-gate': 'test-only-gate', 'x-real-ip': '127.0.0.92' },
    socket: freshSocket,
  }, freshSocket, Buffer.alloc(0));

  await ok('a replacement WS receives hello before its first fresh score fence', () => {
    assert.ok(freshWs.sent.some((value) => value.type === 'hello'));
    assert.strictEqual(wsBundles(freshWs.sent).length, 0,
      'no cached compact revision is replayed synchronously');
  });
  await wait(70);
  await ok('a replacement WS establishes an ordered fresh board baseline', () => {
    const helloAt = freshWs.sent.findIndex((value) => value.type === 'hello');
    const bundleAt = freshWs.sent.findIndex((value) => value.type === 'bundle');
    assert.ok(helloAt >= 0 && bundleAt > helloAt);
    assert.strictEqual(wsBundles(freshWs.sent).length, 1);
    const board = assertWsBundle(freshWs.sent[bundleAt]);
    assert.ok(freshWs.sent[bundleAt].items.slice(0, -1).some((value) => value.fence === 1),
      'the replacement builds a new price fence inside its atomic baseline');
    assert.ok(!board.causes.some((cause) => cause[0] === 'WDROP'),
      'a replacement gets a fresh baseline, not a stale retired cause');
    assert.strictEqual(fakeWs.sent.length, 0, 'the dropped socket receives no later revision');
    assert.strictEqual(throwWs.sent.length, 0, 'the failed socket receives no later revision');
  });

  freshWs.sent.length = 0;
  freshWs.raw.length = 0;
  const recoveredAt = Date.now();
  T.ingestIndexTick('BTC', 100.056, recoveredAt, [100.056, 100.056], 0, 'usdt', recoveredAt);
  await wait(70);

  await ok('a recovered WS receives one atomic fence-and-board message', () => {
    const bundles = wsBundles(freshWs.sent);
    assert.strictEqual(bundles.length, 1);
    const board = assertWsBundle(bundles[0]);
    assert.ok(bundles[0].items.slice(0, -1).some((value) => value.s === 'BTC'
      && value.snap === 1 && value.fence === 1 && value.p === 100.056),
    'the score-fence snapshot precedes its board inside the same WS message');
    assert.ok(Number.isSafeInteger(board.q));
    assert.strictEqual(fakeWs.sent.length, 0, 'the dropped socket remains retired');
    assert.strictEqual(throwWs.sent.length, 0, 'the failed socket remains retired');
  });

  /* A fully closed market can disappear from the current exposure scan before
     the coalescer runs. Its immutable fill cause must survive independently of
     a now-expired live mark, then be retired after the successful delivery so
     it cannot amplify every future BTC board forever. */
  writes.length = 0;
  const closedSourceAt = Date.now() - 130;
  const closedOpenAt = Date.now();
  T.compUpdate('CEXP', 'lazer', 100, closedOpenAt, closedSourceAt);
  T.applyFill(USERS[2], {
    symbol: 'CEXP', orderSide: 'BUY', size: 1, px: 100, feeBps: 0,
    kind: 'MARKET', leverage: 25, marginMode: 'cross', at: closedOpenAt,
    executionSource: 'test-index', referenceMark: 100,
  });
  const closedAt = closedOpenAt + 1;
  T.live.map.set('CEXP', { ...T.live.map.get('CEXP'), pythPrice: 101,
    pythAtMs: closedAt, pythSrcAtMs: closedSourceAt, lastUpdatedMs: closedAt,
    srcKey: 'lazer' });
  T.applyFill(USERS[2], {
    symbol: 'CEXP', orderSide: 'SELL', size: 1, px: 101, feeBps: 0,
    kind: 'LIQUIDATION', leverage: 25, marginMode: 'cross', at: closedAt,
    executionSource: 'test-index', referenceMark: 101,
  });
  await wait(70);

  await ok('an expired closed mark retains the exact realized-fill cause', () => {
    const all = frames();
    const boards = boardFrames(all);
    const causal = boardWithCause(all, {
      symbol: 'CEXP', referenceMark: 101, executionPrice: 101, at: closedAt,
    });
    assert.ok(causal);
    assert.ok(!priceFrames(all, 'CEXP').some(({ value }) => value.fence === 1),
      'an expired observation is not revived as a current price fence');
    assert.ok(causal.value.t >= closedSourceAt + 140,
      'the compact board was actually built after the causal mark expired');
    exactCause(causal.value, {
      symbol: 'CEXP', referenceMark: 101, executionPrice: 101, at: closedAt,
    });
    const mine = causal.value.players.find((p) => p.userId === USERS[2]);
    assert.strictEqual(mine.score, 1, 'the flat leg is represented by realized PnL');
    cleanSuccessor(boards, causal);
  });

  writes.length = 0;
  const unrelatedAt = Date.now();
  T.ingestIndexTick('BTC', 100.057, unrelatedAt, [100.057, 100.057], 0, 'usdt', unrelatedAt);
  await wait(70);

  await ok('delivered closed-symbol dirty state and causes are retired', () => {
    const all = frames();
    const boards = boardFrames(all);
    assert.strictEqual(boards.length, 1);
    assert.strictEqual(priceFrames(all, 'CEXP').length, 0,
      'a later unrelated board does not resend the closed symbol');
    assertCauseSchema(boards[0].value);
    assert.ok(!boards[0].value.causes.some((cause) => cause[0] === 'CEXP'),
      'a delivered fill cause is not replayed on an unrelated score revision');
    const btcFence = priceFrames(all, 'BTC').find(({ value }) => value.snap === 1);
    assert.ok(btcFence);
    assert.strictEqual(btcFence.value.fence, 1);
  });

  /* Exercise the documented event ceiling with real committed rows and 512
     distinct causal identities. Two flat fills on each of 256 valid ticker
     names also make their price fences non-negotiable. The whole revision is
     intentionally too large for one WS message, so the engine must advance
     through explicit incomplete chunks and finish without dropping a healthy
     connection or losing an event. */
  const floodSymbols = Array.from({ length: 256 }, (_, i) =>
    `FLOOD${String(i).padStart(7, '0')}`);
  const floodPrice = 9_876_543_210.12345;
  const sourceAt = Date.now();
  for (const symbol of floodSymbols) {
    T.compUpdate(symbol, 'usdt', floodPrice, sourceAt, sourceAt);
  }
  writes.length = 0;
  freshWs.sent.length = 0;
  freshWs.raw.length = 0;

  const floodAt = Date.now();
  const expectedCauses = new Set();
  for (let i = 0; i < floodSymbols.length; i++) {
    const symbol = floodSymbols[i];
    const openAt = floodAt + i * 2;
    const closeAt = openAt + 1;
    for (const [orderSide, at] of [['BUY', openAt], ['SELL', closeAt]]) {
      T.applyFill(USERS[0], {
        symbol, orderSide, size: 1e-9, px: floodPrice, feeBps: 0,
        kind: 'MARKET', leverage: 25, marginMode: 'cross', at,
        executionSource: 'test-index', referenceMark: floodPrice,
      });
      expectedCauses.add(JSON.stringify([symbol, floodPrice, floodPrice, at]));
    }
  }
  await waitFor(() => {
    const bundles = wsBundles(freshWs.sent);
    const boards = bundles.map((bundle) => bundle.items.at(-1));
    const finalCause = boards.findIndex((board) => board && board.type === 'comp'
      && board.causesComplete === true && board.causes.length > 0);
    return finalCause >= 0 && boards[finalCause + 1]
      && boards[finalCause + 1].q === boards[finalCause].q + 1
      && boards[finalCause + 1].causesComplete === true
      && boards[finalCause + 1].causes.length === 0;
  }, 1000);

  await ok('512 distinct fill causes stream in bounded WS revision chunks', () => {
    const bundleWire = freshWs.raw.map((raw) => ({ raw, value: JSON.parse(raw) }))
      .filter(({ value }) => value.type === 'bundle');
    const bundleBoards = bundleWire.map(({ value }) => value.items.at(-1));
    const finalCauseAt = bundleBoards.findIndex((board) => board.causesComplete
      && board.causes.length > 0);
    assert.ok(finalCauseAt >= 1, 'the legal worst case spans multiple causal revisions');
    const causalWire = bundleWire.slice(0, finalCauseAt + 1);
    const cleanWire = bundleWire[finalCauseAt + 1];
    assert.ok(cleanWire, 'the causal chain is followed by a clean baseline revision');
    assert.strictEqual(freshWs.terminated, undefined,
      'a healthy client consumes every chunk without reconnecting');

    const seen = new Set();
    let previousRevision = 0;
    for (let i = 0; i < causalWire.length; i++) {
      const { raw, value } = causalWire[i];
      const bytes = Buffer.byteLength(raw);
      assert.ok(bytes < 65_536, `WS bundle ${i + 1} is ${bytes} bytes`);
      const board = assertWsBundle(value);
      assert.ok(Number.isSafeInteger(board.q) && board.q > 0);
      if (previousRevision) {
        assert.strictEqual(board.q, previousRevision + 1,
          'a healthy WS observes every chunk revision without a silent gap');
      }
      previousRevision = board.q;
      assert.strictEqual(board.causesComplete, i === causalWire.length - 1,
        'only the final revision declares the causal set complete');
      for (const cause of board.causes) {
        assert.strictEqual(cause[4], 1, 'every worst-case cause is distinct');
        const key = JSON.stringify(cause.slice(0, 4));
        assert.ok(!seen.has(key), `cause ${key} was not replayed in another revision`);
        seen.add(key);
      }
    }
    assert.ok(Buffer.byteLength(cleanWire.raw) < 65_536);
    const clean = assertWsBundle(cleanWire.value);
    assert.strictEqual(clean.q, previousRevision + 1);
    assert.strictEqual(clean.causesComplete, true);
    assert.deepStrictEqual(clean.causes, []);
    assert.strictEqual(seen.size, 512);
    assert.deepStrictEqual(seen, expectedCauses,
      'all committed events survive the bounded revision chain exactly once');
  });
  freshWs.emit('close');

  /* A price-frame loss is every bit as causal as a compact-board loss. The
     relay used to skip an over-budget WS for one ordinary/material quote but
     leave it subscribed, so its next q made a permanent hole look like a
     continuous stream. Keep readyState at OPEN after terminate in this fake:
     the only thing preventing that later delivery is synchronous removal from
     the production client set. */
  const attachRelayClient = (ip) => {
    const client = new EventEmitter();
    client.readyState = 1;
    client.bufferedAmount = 0;
    client.sent = [];
    client.send = (payload) => client.sent.push(JSON.parse(String(payload)));
    client.ping = () => {};
    client.terminate = () => { client.terminated = true; };
    const socket = { fakeWs: client, remoteAddress: ip, destroy() {} };
    upgrade({
      url: '/api/paper/index-ws',
      headers: { 'x-paper-gate': 'test-only-gate', 'x-real-ip': ip },
      socket,
    }, socket, Buffer.alloc(0));
    client.sent.length = 0;
    return client;
  };

  const ordinarySlowWs = attachRelayClient('127.0.0.94');
  ordinarySlowWs.bufferedAmount = 65_536;
  writes.length = 0;
  const ordinaryAt = Date.now();
  T.compUpdate('WSORD', 'lazer', 100, ordinaryAt, ordinaryAt);
  ordinarySlowWs.bufferedAmount = 0;
  const afterOrdinaryAt = ordinaryAt + 1;
  T.compUpdate('WSORDNEXT', 'lazer', 100, afterOrdinaryAt, afterOrdinaryAt);

  await ok('an ordinary price hitting WS backpressure terminates before any q can be skipped', () => {
    assert.strictEqual(ordinarySlowWs.terminated, true,
      'the over-budget ordinary subscriber is terminated synchronously');
    assert.deepStrictEqual(ordinarySlowWs.sent, [],
      'the retired socket receives neither the refused q nor a later q');
    const all = frames();
    assert.strictEqual(priceFrames(all, 'WSORD').length, 1,
      'the ordinary event was published to a healthy transport');
    assert.strictEqual(priceFrames(all, 'WSORDNEXT').length, 1,
      'a later q existed and was not traversed on the retired socket');
  });

  const materialUser = 9399;
  T.db.prepare('INSERT INTO users (id) VALUES (?)').run(materialUser);
  T.stmt.acctIns.run(materialUser, Date.now(), Date.now());
  const materialSeedAt = Date.now();
  T.compUpdate('WSMAT', 'lazer', 100, materialSeedAt, materialSeedAt);
  const materialSlowWs = attachRelayClient('127.0.0.95');
  T.applyFill(materialUser, {
    symbol: 'WSMAT', orderSide: 'BUY', size: 0.01, px: 100, feeBps: 0,
    kind: 'MARKET', leverage: 25, marginMode: 'cross', at: materialSeedAt,
    executionSource: 'test-index', referenceMark: 100,
  });
  materialSlowWs.bufferedAmount = 65_536;
  writes.length = 0;
  const materialAt = Math.max(Date.now(), materialSeedAt + 1);
  T.compUpdate('WSMAT', 'lazer', 100.001, materialAt, materialAt);
  materialSlowWs.bufferedAmount = 0;
  const afterMaterialAt = materialAt + 1;
  T.compUpdate('WSMATNEXT', 'lazer', 100, afterMaterialAt, afterMaterialAt);

  await ok('a material price hitting WS backpressure terminates without later q traversal', () => {
    assert.strictEqual(materialSlowWs.terminated, true,
      'the over-budget material subscriber is terminated synchronously');
    assert.deepStrictEqual(materialSlowWs.sent, [],
      'the retired socket receives neither the material q nor a later q');
    const all = frames();
    const material = priceFrames(all, 'WSMAT');
    assert.strictEqual(material.length, 1,
      'the causal event was published to a healthy transport');
    assert.strictEqual(material[0].value.m, 1,
      'the exercised frame is the material cadence-bypass path');
    assert.strictEqual(priceFrames(all, 'WSMATNEXT').length, 1,
      'a later q existed and was not traversed on the retired socket');
  });
  T.db.prepare('DELETE FROM paper_positions WHERE user_id = ? AND symbol = ?')
    .run(materialUser, 'WSMAT');

  /* A board has its own lifetime. If a held source expires in silence, the
     expiry timer must publish an incomplete board even without another tick or
     REST poll to wake the scorer. Use an isolated short-lived Lazer source so
     the test stays fast and does not disturb BTC/SOL. */
  writes.length = 0;
  const expAt = Date.now();
  T.compUpdate('EXP', 'lazer', 100, expAt, expAt);
  const expBasisAt = Number(T.live.map.get('EXP').pythSrcAtMs);
  T.applyFill(USERS[1], {
    symbol: 'EXP', orderSide: 'BUY', size: 1, px: 100, feeBps: 0,
    kind: 'MARKET', leverage: 25, marginMode: 'cross', at: expAt,
    executionSource: 'test-index', referenceMark: 100,
  });
  await wait(70);

  let expExpiryAt = 0;

  await ok('a fresh short-lived mark produces a complete fenced board', () => {
    const all = frames();
    const boards = boardFrames(all);
    const causal = boardWithCause(all, {
      symbol: 'EXP', referenceMark: 100, executionPrice: 100, at: expAt,
    });
    assert.ok(causal);
    assert.strictEqual(causal.value.complete, true);
    const expFence = priceFrames(all, 'EXP').find(({ value }) => value.snap === 1
      && value.fence === 1 && value.p === 100);
    assert.ok(expFence);
    expExpiryAt = expFence.value.x;
    assert.strictEqual(expExpiryAt,
      Math.round(expBasisAt + T.staleMsForSym('EXP', 'lazer')),
      'the fence expires from the exact age-adjusted source basis');
    exactCause(causal.value, {
      symbol: 'EXP', referenceMark: 100, executionPrice: 100, at: expAt,
    });
    cleanSuccessor(boards, causal);
  });

  writes.length = 0;
  await wait(Math.max(45, expExpiryAt - Date.now() + 45));

  await ok('silent mark expiry refreshes at its deadline without another board gap', () => {
    const all = frames();
    const boards = boardFrames(all);
    assert.strictEqual(boards.length, 1, 'the expiry owns one coalesced refresh');
    const board = boards[0].value;
    const latenessMs = board.t - expExpiryAt;
    assert.ok(latenessMs >= 0 && latenessMs <= 15,
      `expiry refresh was ${latenessMs}ms late (must not pay another coalescer gap)`);
    assert.strictEqual(board.complete, false);
    assert.ok(board.stalePricing.includes('EXP'));
    assert.ok(board.unscored.some((p) => p.userId === USERS[1]));
    assertCauseSchema(board);
    assert.ok(!board.causes.some((cause) => cause[0] === 'EXP'),
      'the already delivered opening cause is not replayed by expiry');
    assert.strictEqual(priceFrames(all, 'EXP').length, 0,
      'an expired mark is never renewed merely to explain the stale verdict');
  });
  T.db.prepare('DELETE FROM paper_positions WHERE user_id = ? AND symbol = ?')
    .run(USERS[1], 'EXP');

  writes.length = 0;
  const solAt = Date.now();
  T.ingestIndexTick('SOL', 100.01, solAt, [100.01, 100.01], 0, 'usdt', solAt);
  await wait(70);

  await ok('an unheld market emits no leaderboard work', () => {
    const all = frames();
    assert.strictEqual(priceFrames(all, 'SOL').length, 1, 'the ordinary price still publishes');
    assert.strictEqual(boardFrames(all).length, 0, 'no seated account is exposed to SOL');
  });

  /* Drawdown is an all-seat scan, so an unheld market must not pay for it.
     Once a scored position is present, however, one accepted tick samples the
     visible wick before risk mutation and the final account after its forced
     close. Both samples belong to the same database transaction as the fill. */
  const realSampleDrawdown = comp.sampleDrawdown;
  let drawdownSamples = 0;
  const sampleTransactions = [];
  comp.sampleDrawdown = (...args) => {
    drawdownSamples++;
    sampleTransactions.push(T.db.inTransaction);
    return realSampleDrawdown(...args);
  };
  const ddUser = USERS[0];
  const ddAccountBefore = T.stmt.acctGet.get(ddUser);
  try {
    writes.length = 0;
    const unheldDdAt = Date.now();
    T.compUpdate('DDNONE', 'usdt', 100, unheldDdAt, unheldDdAt);
    await wait(35);

    await ok('an accepted unheld tick performs no all-seat drawdown scan', () => {
      assert.strictEqual(drawdownSamples, 0);
      assert.strictEqual(priceFrames(frames(), 'DDNONE').length, 1,
        'the accepted market tick still reaches the chart');
      assert.strictEqual(boardFrames(frames()).length, 0,
        'an unheld market neither scans nor schedules the board');
    });

    const heldSetupAt = Math.max(Date.now(), unheldDdAt + 1);
    T.compUpdate('DDHELD', 'usdt', 100, heldSetupAt, heldSetupAt);
    T.db.prepare('UPDATE paper_accounts SET heat = 2 WHERE user_id = ?').run(ddUser);
    const ddEpoch = T.stmt.acctGet.get(ddUser).epoch;
    T.stmt.posIns.run(ddUser, 'DDHELD', ddEpoch, 'LONG', 1, 100, 25,
      heldSetupAt, 100, heldSetupAt, heldSetupAt, 'cross', 0);
    T.db.prepare('UPDATE paper_positions SET sl_price = ? WHERE user_id = ? AND symbol = ?')
      .run(99.95, ddUser, 'DDHELD');
    const ddPlayerBefore = T.db.prepare(
      'SELECT peak_equity, max_drawdown FROM paper_round_players WHERE round_id = ? AND user_id = ?')
      .get('push', ddUser);

    writes.length = 0;
    drawdownSamples = 0;
    sampleTransactions.length = 0;
    const ddTickAt = Math.max(Date.now(), heldSetupAt + 1);
    const ddWick = 99.9;
    T.compUpdate('DDHELD', 'usdt', ddWick, ddTickAt, ddTickAt);
    await wait(70);

    await ok('a held tick samples its wick and post-fill state atomically', () => {
      assert.strictEqual(drawdownSamples, 2,
        'the scored pass samples immediately before and after its mutation');
      assert.deepStrictEqual(sampleTransactions, [true, true],
        'both samples execute inside the scored fill transaction');
      assert.strictEqual(T.stmt.posGet.get(ddUser, 'DDHELD'), undefined,
        'the stop closed the position on this tick');
      const fill = T.db.prepare(
        "SELECT * FROM paper_fills WHERE user_id = ? AND symbol = ? AND kind = 'SL' ORDER BY id DESC LIMIT 1")
        .get(ddUser, 'DDHELD');
      assert.ok(fill && fill.fee > 0, 'scaled execution makes the post-wick fee mutation observable');

      const all = frames();
      const boards = boardFrames(all);
      const causal = boardWithCause(all, {
        symbol: 'DDHELD', referenceMark: ddWick, executionPrice: fill.price, at: ddTickAt,
      });
      assert.ok(causal);
      const fence = priceFrames(all, 'DDHELD').find(({ value }) => value.fence === 1);
      assert.ok(fence && fence.value.p === ddWick);
      assert.ok(all.indexOf(fence) < all.indexOf(causal));
      exactCause(causal.value, {
        symbol: 'DDHELD', referenceMark: ddWick, executionPrice: fill.price, at: ddTickAt,
      });
      cleanSuccessor(boards, causal);

      const rest = T.compRankSnapshot();
      assert.deepStrictEqual(causal.value.players, rest.players,
        'the push and REST snapshots both include the final scored mutation');
      const mine = rest.players.find((p) => p.userId === ddUser);
      const accountAfter = T.stmt.acctGet.get(ddUser);
      const wickDrawdown = ddPlayerBefore.peak_equity - (ddAccountBefore.balance - 0.1);
      assert.ok(mine.maxDrawdown > wickDrawdown,
        'the post-fill fee deepens drawdown beyond the pre-fill wick sample');
      assert.ok(Math.abs(mine.maxDrawdown - (ddPlayerBefore.peak_equity - accountAfter.balance)) < 1e-9);
      const bound = comp.playersOf('push').find((p) => p.user_id === ddUser);
      const direct = T.scoreUser(ddUser, null, ddEpoch, bound.start_balance);
      assert.strictEqual(mine.accountPnl, direct.accountPnl);
      assert.strictEqual(mine.hotBonus, direct.hotBonus);
      assert.ok(Math.abs(mine.score - (direct.accountPnl + direct.hotBonus)) < 1e-9);
    });
  } finally {
    comp.sampleDrawdown = realSampleDrawdown;
    T.db.prepare('UPDATE paper_accounts SET heat = ? WHERE user_id = ?')
      .run(ddAccountBefore.heat, ddUser);
  }

  /* A confirming jump has no accepted price frame, but for a held symbol it
     still changes ticket eligibility and ranking completeness. The transition
     must therefore publish a control synchronously and wake the score relay.
     A successful intervening tick starts a new failure episode, even when all
     three transitions happen inside the old one-second control throttle. */
  const lossSymbol = 'PLOSS';
  const lossUser = USERS[1];
  const lossSeedAt = Date.now();
  T.compUpdate(lossSymbol, 'usdt', 100, lossSeedAt, lossSeedAt);
  T.compUpdate(lossSymbol, 'usd', 100, lossSeedAt, lossSeedAt);
  T.applyFill(lossUser, {
    symbol: lossSymbol, orderSide: 'BUY', size: 0.01, px: 100, feeBps: 0,
    kind: 'MARKET', leverage: 25, marginMode: 'cross', at: lossSeedAt,
    executionSource: 'test-index', referenceMark: 100,
  });
  await wait(70);

  writes.length = 0;
  const firstLossAt = Date.now();
  T.ingestIndexTick(lossSymbol, 101, firstLossAt, [101, 101], 0, 'usdt', firstLossAt);
  const firstImmediate = frames();
  await wait(70);
  const firstLossFrames = frames();

  writes.length = 0;
  const recoveryAt = Date.now();
  T.ingestIndexTick(lossSymbol, 100.001, recoveryAt,
    [100.001, 100.001], 0, 'usdt', recoveryAt);
  await wait(70);
  const recoveryFrames = frames();
  const recoveryClearedConfirmation = !T.confirming.has(lossSymbol);

  writes.length = 0;
  const secondLossAt = Date.now();
  T.ingestIndexTick(lossSymbol, 101.002, secondLossAt,
    [101.002, 101.002], 0, 'usdt', secondLossAt);
  const secondImmediate = frames();
  await wait(70);
  const secondLossFrames = frames();
  const secondEnteredConfirmation = T.confirming.has(lossSymbol);

  await ok('tracked priceability loss publishes promptly on every failure episode', () => {
    const firstControls = firstImmediate.filter(({ value }) =>
      value.type === 'stale' && value.s === lossSymbol);
    assert.strictEqual(firstControls.length, 1,
      'the first confirming transition emits its control synchronously');
    assert.ok(firstControls[0].at - firstLossAt < 10,
      `first control took ${firstControls[0].at - firstLossAt}ms`);
    assert.strictEqual(priceFrames(firstLossFrames, lossSymbol).length, 0,
      'the refused confirming quote is never presented as a price');
    const firstBoards = boardFrames(firstLossFrames);
    assert.strictEqual(firstBoards.length, 1);
    assert.strictEqual(firstBoards[0].value.complete, false);
    assert.ok(firstBoards[0].value.stalePricing.includes(lossSymbol));
    assert.ok(firstBoards[0].value.unscored.some((p) => p.userId === lossUser));
    assert.ok(firstBoards[0].at - firstLossAt < 100,
      `first incomplete board took ${firstBoards[0].at - firstLossAt}ms`);

    const recoveredBoards = boardFrames(recoveryFrames);
    assert.strictEqual(recoveredBoards.length, 1);
    assert.ok(!recoveredBoards[0].value.stalePricing.includes(lossSymbol));
    assert.ok(recoveredBoards[0].value.players.some((p) => p.userId === lossUser),
      'a committed intervening price restores this seat to scoring');
    assert.strictEqual(recoveryClearedConfirmation, true);

    assert.ok(secondLossAt - firstLossAt < 1000,
      'the second episode deliberately lands inside the old throttle window');
    const secondControls = secondImmediate.filter(({ value }) =>
      value.type === 'stale' && value.s === lossSymbol);
    assert.strictEqual(secondControls.length, 1,
      'recovery clears the control latch for the next failure');
    assert.strictEqual(secondEnteredConfirmation, true);
    const secondBoards = boardFrames(secondLossFrames);
    assert.strictEqual(secondBoards.length, 1);
    assert.strictEqual(secondBoards[0].value.complete, false);
    assert.ok(secondBoards[0].value.stalePricing.includes(lossSymbol));
    assert.ok(secondBoards[0].value.q > recoveredBoards[0].value.q);
  });
  T.db.prepare('DELETE FROM paper_positions WHERE user_id = ? AND symbol = ?')
    .run(lossUser, lossSymbol);

  const riskSymbol = 'RLOSS';
  const riskUser = USERS[2];
  const riskSeedAt = Date.now();
  /* This regression sits after the 512-event stress pass. Refresh the one
     long-lived BTC exposure so an unrelated four-second backup deadline
     cannot turn the injected RLOSS failure into a multi-symbol pause. */
  const riskBtcMark = Number(T.live.map.get('BTC').pythPrice);
  T.compUpdate('BTC', 'usdt', riskBtcMark, riskSeedAt, riskSeedAt);
  T.compUpdate('BTC', 'usd', riskBtcMark, riskSeedAt, riskSeedAt);
  T.compUpdate(riskSymbol, 'usdt', 100, riskSeedAt, riskSeedAt);
  T.compUpdate(riskSymbol, 'usd', 100, riskSeedAt, riskSeedAt);
  T.applyFill(riskUser, {
    symbol: riskSymbol, orderSide: 'BUY', size: 0.01, px: 100, feeBps: 0,
    kind: 'MARKET', leverage: 25, marginMode: 'cross', at: riskSeedAt,
    executionSource: 'test-index', referenceMark: 100,
  });
  await wait(70);

  const realMonitorSegments = comp.monitorSegments;
  const failRiskMonitor = () => { throw new Error('deterministic monitor failure'); };
  writes.length = 0;
  const firstRiskAt = Date.now();
  comp.monitorSegments = failRiskMonitor;
  try {
    T.ingestIndexTick(riskSymbol, 100.001, firstRiskAt,
      [100.001, 100.001], 0, 'usdt', firstRiskAt);
  } finally {
    comp.monitorSegments = realMonitorSegments;
  }
  const firstRiskImmediate = frames();
  await wait(70);
  const firstRiskFrames = frames();

  writes.length = 0;
  const riskRecoveryAt = Date.now();
  T.ingestIndexTick(riskSymbol, 100.0015, riskRecoveryAt,
    [100.0015, 100.0015], 0, 'usdt', riskRecoveryAt);
  await wait(70);
  const riskRecoveryFrames = frames();

  writes.length = 0;
  const secondRiskAt = Date.now();
  comp.monitorSegments = failRiskMonitor;
  try {
    T.ingestIndexTick(riskSymbol, 100.002, secondRiskAt,
      [100.002, 100.002], 0, 'usdt', secondRiskAt);
  } finally {
    comp.monitorSegments = realMonitorSegments;
  }
  const secondRiskImmediate = frames();
  await wait(70);
  const secondRiskFrames = frames();

  await ok('tracked risk failure publishes and re-arms after a committed recovery', () => {
    const controlsFor = (all) => all.filter(({ value }) =>
      value.type === 'risk_failed' && value.s === riskSymbol);
    assert.strictEqual(controlsFor(firstRiskImmediate).length, 1);
    assert.strictEqual(priceFrames(firstRiskFrames, riskSymbol).length, 0,
      'a failed risk pass cannot publish its candidate quote');
    const firstBoard = boardFrames(firstRiskFrames);
    assert.strictEqual(firstBoard.length, 1);
    assert.strictEqual(firstBoard[0].value.complete, false);
    assert.ok(firstBoard[0].value.stalePricing.includes(riskSymbol));

    const recovered = boardFrames(riskRecoveryFrames);
    assert.strictEqual(recovered.length, 1);
    assert.ok(!recovered[0].value.stalePricing.includes(riskSymbol));
    assert.ok(recovered[0].value.players.some((p) => p.userId === riskUser));

    assert.ok(secondRiskAt - firstRiskAt < 1000);
    assert.strictEqual(controlsFor(secondRiskImmediate).length, 1,
      'the committed recovery clears the per-symbol risk control throttle');
    const secondBoard = boardFrames(secondRiskFrames);
    assert.strictEqual(secondBoard.length, 1);
    assert.strictEqual(secondBoard[0].value.complete, false);
    assert.ok(secondBoard[0].value.stalePricing.includes(riskSymbol));
    assert.ok(secondBoard[0].value.q > recovered[0].value.q);
  });
  T.db.prepare('DELETE FROM paper_positions WHERE user_id = ? AND symbol = ?')
    .run(riskUser, riskSymbol);

  /* A failed candidate leaves the previous committed mark in live.map, but
     that mark must remain unusable. Recovery gets one deliberately narrow
     exception: the exact candidate object installed by this ingest, and only
     while its scored transaction is running. A second blocked market proves
     the exception is not a global/provisional unblock. */
  const probeSymbol = 'RPROBE';
  const refusedProbeSymbol = 'RPROBEOTHER';
  const probeUser = USERS[1];
  const refusedProbeUser = USERS[2];
  const probeSeedAt = Date.now();
  for (const symbol of [probeSymbol, refusedProbeSymbol]) {
    T.compUpdate(symbol, 'usdt', 100, probeSeedAt, probeSeedAt);
    T.compUpdate(symbol, 'usd', 100, probeSeedAt, probeSeedAt);
  }
  T.applyFill(probeUser, {
    symbol: probeSymbol, orderSide: 'BUY', size: 0.01, px: 100, feeBps: 0,
    kind: 'MARKET', leverage: 25, marginMode: 'cross', at: probeSeedAt,
    executionSource: 'test-index', referenceMark: 100,
  });
  T.applyFill(refusedProbeUser, {
    symbol: refusedProbeSymbol, orderSide: 'BUY', size: 0.01, px: 100, feeBps: 0,
    kind: 'MARKET', leverage: 25, marginMode: 'cross', at: probeSeedAt + 1,
    executionSource: 'test-index', referenceMark: 100,
  });
  await wait(70);

  for (const [symbol, price] of [[probeSymbol, 100.001], [refusedProbeSymbol, 100.002]]) {
    const at = Date.now();
    comp.monitorSegments = failRiskMonitor;
    try {
      T.ingestIndexTick(symbol, price, at, [price, price], 0, 'usdt', at);
    } finally {
      comp.monitorSegments = realMonitorSegments;
    }
  }
  await wait(70);
  assert.strictEqual(T.compPriceReady(probeSymbol), false,
    'the last committed mark remains refused before an exact recovery probe');
  assert.strictEqual(T.compPriceReady(refusedProbeSymbol), false,
    'the independently failed symbol is also blocked');
  /* Keep the second block alive but remove its exposure so it cannot make the
     all-seat drawdown scan itself unavailable. */
  T.db.prepare('DELETE FROM paper_positions WHERE user_id = ? AND symbol = ?')
    .run(refusedProbeUser, refusedProbeSymbol);

  const realProbeSampleDrawdown = comp.sampleDrawdown;
  const probeObservations = [];
  comp.sampleDrawdown = (...args) => {
    probeObservations.push({
      inTransaction: T.db.inTransaction,
      exactCandidateReady: T.compPriceReady(probeSymbol),
      otherBlockedReady: T.compPriceReady(refusedProbeSymbol),
      candidatePrice: Number((T.live.map.get(probeSymbol) || {}).pythPrice),
    });
    return realProbeSampleDrawdown(...args);
  };
  writes.length = 0;
  const exactRecoveryAt = Date.now();
  const exactRecoveryPrice = 100.0015;
  try {
    T.ingestIndexTick(probeSymbol, exactRecoveryPrice, exactRecoveryAt,
      [exactRecoveryPrice, exactRecoveryPrice], 0, 'usdt', exactRecoveryAt);
  } finally {
    comp.sampleDrawdown = realProbeSampleDrawdown;
  }
  await wait(70);

  await ok('risk recovery admits only the exact candidate inside its scored transaction', () => {
    assert.strictEqual(probeObservations.length, 3,
      'the recovery sampled before/after risk plus the canonical pause-release proof');
    for (const [index, observed] of probeObservations.entries()) {
      assert.strictEqual(observed.inTransaction, index < 2,
        'only the pre/post risk samples use the transactional recovery exemption');
      assert.strictEqual(observed.exactCandidateReady, true,
        'the exact candidate is priceable during its own recovery proof');
      assert.strictEqual(observed.otherBlockedReady, false,
        'another blocked symbol does not inherit the recovery exemption');
      assert.strictEqual(observed.candidatePrice, exactRecoveryPrice);
    }
    assert.strictEqual(T.compPriceReady(probeSymbol), true,
      'a successfully committed proof clears only its own risk block');
    assert.strictEqual(T.compPriceReady(refusedProbeSymbol), false);
    const all = frames();
    assert.ok(priceFrames(all, probeSymbol).some(({ value }) =>
      value.p === exactRecoveryPrice), 'the candidate publishes only after the proof commits');
    const boards = boardFrames(all);
    assert.strictEqual(boards.length, 1);
    assert.ok(boards[0].value.players.some((p) => p.userId === probeUser));
    assert.ok(!boards[0].value.stalePricing.includes(probeSymbol));
  });
  T.db.prepare('DELETE FROM paper_positions WHERE user_id = ? AND symbol = ?')
    .run(probeUser, probeSymbol);

  /* Force the source lifetime to cross inside a liquidation write without
     relying on scheduler jitter. This is a TEMP trigger on the throwaway test
     database only; production has no sleep hook in its financial path. */
  const spinFunction = 'test_comp_expiry_spin';
  T.db.function(spinFunction, () => {
    const until = process.hrtime.bigint() + 20_000_000n;
    while (process.hrtime.bigint() < until) { /* deterministic synchronous work */ }
    return 0;
  });
  const installSpin = (name, userId) => T.db.exec(`
    CREATE TEMP TRIGGER ${name}
    BEFORE INSERT ON paper_fills
    WHEN NEW.user_id = ${Number(userId)}
    BEGIN SELECT ${spinFunction}(); END
  `);
  const removeSpin = (name) => T.db.exec(`DROP TRIGGER IF EXISTS ${name}`);
  const seedExpiryMarket = (symbol) => {
    const at = Date.now();
    T.compUpdate(symbol, 'usdt', 100, at, at);
    T.compUpdate(symbol, 'usd', 100, at, at);
  };
  const insertExpiryPosition = (userId, symbol) => {
    const at = Date.now();
    const acct = T.stmt.acctGet.get(userId);
    T.stmt.posIns.run(userId, symbol, acct.epoch, 'LONG', 1, 100, 25,
      at, 100, at, at, 'isolated', 0.01);
  };
  const seedExpiryPosition = (userId, symbol) => {
    seedExpiryMarket(symbol);
    insertExpiryPosition(userId, symbol);
  };
  const expireDuringRisk = (symbol) => {
    const at = Date.now();
    const budget = T.staleMsForSym(symbol, 'usdt');
    assert.ok(budget > 25);
    T.ingestIndexTick(symbol, 99.99, at, [99.99, 99.99], 0,
      'usdt', at - budget + 5);
  };
  const recoverExpirySymbol = (symbol) => {
    const at = Date.now();
    T.ingestIndexTick(symbol, 100, at, [100, 100], 0, 'usdt', at);
  };

  await ok('a public-only risk transaction rolls back when its trigger expires', () => {
    const publicUser = 9401;
    const symbol = 'PEXP';
    const trigger = 'test_public_expiry_delay';
    T.db.prepare('INSERT INTO users (id) VALUES (?)').run(publicUser);
    T.stmt.acctIns.run(publicUser, Date.now(), Date.now());
    seedExpiryPosition(publicUser, symbol);
    installSpin(trigger, publicUser);
    try {
      const fillsBefore = T.db.prepare(
        'SELECT COUNT(*) AS n FROM paper_fills WHERE user_id = ? AND symbol = ?'
      ).get(publicUser, symbol).n;
      expireDuringRisk(symbol);
      assert.ok(T.stmt.posGet.get(publicUser, symbol),
        'the public liquidation must not survive a trigger the relay rejects');
      const fillsAfter = T.db.prepare(
        'SELECT COUNT(*) AS n FROM paper_fills WHERE user_id = ? AND symbol = ?'
      ).get(publicUser, symbol).n;
      assert.strictEqual(fillsAfter, fillsBefore,
        'the rejected observation cannot leave a public fill behind');
      assert.strictEqual(T.live.map.get(symbol).pythPrice, 100,
        'the rejected candidate is restored to the prior committed mark');
    } finally {
      removeSpin(trigger);
      T.db.prepare('DELETE FROM paper_positions WHERE user_id = ? AND symbol = ?')
        .run(publicUser, symbol);
      recoverExpirySymbol(symbol);
    }
  });

  await ok('mixed public and scored risk roll back together on trigger expiry', () => {
    const publicUser = 9402;
    const scoredUser = USERS[2];
    const symbol = 'MEXP';
    const trigger = 'test_mixed_expiry_delay';
    T.db.prepare('INSERT INTO users (id) VALUES (?)').run(publicUser);
    T.stmt.acctIns.run(publicUser, Date.now(), Date.now());
    seedExpiryMarket(symbol);
    insertExpiryPosition(publicUser, symbol);
    insertExpiryPosition(scoredUser, symbol);
    assert.ok(T.stmt.posGet.get(publicUser, symbol),
      'precondition: seeding completed before the public position was installed');
    assert.ok(T.stmt.posGet.get(scoredUser, symbol),
      'precondition: both halves exist before the expiring candidate');
    installSpin(trigger, publicUser);
    try {
      expireDuringRisk(symbol);
      assert.ok(T.stmt.posGet.get(publicUser, symbol),
        'the public half cannot commit before the expired scored half rolls back');
      assert.ok(T.stmt.posGet.get(scoredUser, symbol),
        'the scored savepoint rolls its liquidation back at the deadline');
      assert.strictEqual(T.live.map.get(symbol).pythPrice, 100);
    } finally {
      removeSpin(trigger);
      T.db.prepare('DELETE FROM paper_positions WHERE symbol = ?').run(symbol);
      recoverExpirySymbol(symbol);
    }
  });

  /* Observe both drawdown calls and the liquidation row while they are still
     inside the scored event, then spend the candidate's final milliseconds in
     the fill trigger. The deadline rejection must roll all of those writes
     back and publish only the fail-closed state. */
  const scoredExpiryUser = USERS[1];
  const scoredExpirySymbol = 'SDEXP';
  const scoredExpiryTrigger = 'test_scored_expiry_delay';
  seedExpiryMarket(scoredExpirySymbol);
  insertExpiryPosition(scoredExpiryUser, scoredExpirySymbol);
  const scoredExpiryPositionBefore = T.stmt.posGet.get(
    scoredExpiryUser, scoredExpirySymbol);
  const scoredExpiryAccountBefore = T.stmt.acctGet.get(scoredExpiryUser);
  const scoredExpiryFillCount = T.db.prepare(
    'SELECT COUNT(*) AS n FROM paper_fills WHERE user_id = ? AND symbol = ?'
  );
  const scoredExpiryFillsBefore = scoredExpiryFillCount.get(
    scoredExpiryUser, scoredExpirySymbol).n;
  const playerDd = T.db.prepare(
    'SELECT user_id, peak_equity, max_drawdown FROM paper_round_players WHERE round_id = ? ORDER BY user_id'
  );
  const scoredExpiryOriginalDd = T.db.prepare(
    'SELECT peak_equity, max_drawdown FROM paper_round_players WHERE round_id = ? AND user_id = ?'
  ).get('push', scoredExpiryUser);
  const seededEquity = T.accountRisk(
    scoredExpiryUser, scoredExpiryAccountBefore).equityTotal;
  T.db.prepare(
    'UPDATE paper_round_players SET peak_equity = ?, max_drawdown = 0 WHERE round_id = ? AND user_id = ?'
  ).run(seededEquity + 1, 'push', scoredExpiryUser);
  const scoredExpiryDdBefore = playerDd.all('push');
  const scoredExpiryLiveBefore = T.live.map.get(scoredExpirySymbol);
  assert.ok(scoredExpiryPositionBefore && scoredExpiryLiveBefore);

  const realExpirySampleDrawdown = comp.sampleDrawdown;
  const scoredExpiryTrace = [];
  comp.sampleDrawdown = (...args) => {
    const result = realExpirySampleDrawdown(...args);
    const player = T.db.prepare(
      'SELECT peak_equity, max_drawdown FROM paper_round_players WHERE round_id = ? AND user_id = ?'
    ).get('push', scoredExpiryUser);
    scoredExpiryTrace.push({
      inTransaction: T.db.inTransaction,
      positionPresent: !!T.stmt.posGet.get(scoredExpiryUser, scoredExpirySymbol),
      fillCount: scoredExpiryFillCount.get(scoredExpiryUser, scoredExpirySymbol).n,
      maxDrawdown: Number(player.max_drawdown) || 0,
    });
    return result;
  };
  installSpin(scoredExpiryTrigger, scoredExpiryUser);
  writes.length = 0;
  try {
    expireDuringRisk(scoredExpirySymbol);
  } finally {
    comp.sampleDrawdown = realExpirySampleDrawdown;
    removeSpin(scoredExpiryTrigger);
  }
  const scoredExpiryImmediate = frames();
  await wait(70);

  await ok('a scored deadline rejection rolls back its fill and both drawdown samples', () => {
    assert.strictEqual(scoredExpiryTrace.length, 2,
      'pre-mark and post-mutation drawdown both ran before the deadline check');
    assert.deepStrictEqual(scoredExpiryTrace.map((x) => x.inTransaction), [true, true]);
    assert.strictEqual(scoredExpiryTrace[0].positionPresent, true);
    assert.strictEqual(scoredExpiryTrace[0].fillCount, scoredExpiryFillsBefore);
    assert.ok(scoredExpiryTrace[0].maxDrawdown > 0,
      'the pre-mark wick changed drawdown inside the transaction');
    assert.strictEqual(scoredExpiryTrace[1].positionPresent, false,
      'the liquidation mutation existed before the commit-edge deadline check');
    assert.strictEqual(scoredExpiryTrace[1].fillCount, scoredExpiryFillsBefore + 1,
      'the transient liquidation ledger row was visible to the post sample');

    assert.deepStrictEqual(T.stmt.posGet.get(scoredExpiryUser, scoredExpirySymbol),
      scoredExpiryPositionBefore, 'the scored position is restored exactly');
    assert.deepStrictEqual(T.stmt.acctGet.get(scoredExpiryUser),
      scoredExpiryAccountBefore, 'account balance and counters roll back with the position');
    assert.strictEqual(scoredExpiryFillCount.get(
      scoredExpiryUser, scoredExpirySymbol).n, scoredExpiryFillsBefore);
    assert.deepStrictEqual(playerDd.all('push'), scoredExpiryDdBefore,
      'neither the pre-wick nor post-liquidation tie-break write survives');
    assert.strictEqual(T.live.map.get(scoredExpirySymbol), scoredExpiryLiveBefore,
      'the exact prior accepted mark object is restored');
    assert.strictEqual(T.compPriceReady(scoredExpirySymbol), false,
      'the restored mark stays blocked after the newest observation expired');

    const all = frames();
    assert.strictEqual(priceFrames(all, scoredExpirySymbol).length, 0,
      'the expired candidate has no accepted price or score fence');
    assert.strictEqual(scoredExpiryImmediate.filter(({ value }) =>
      value.type === 'stale' && value.s === scoredExpirySymbol).length, 1,
    'source expiry emits one synchronous fail-closed control');
    const incomplete = boardFrames(all).find(({ value }) =>
      value.complete === false && value.stalePricing.includes(scoredExpirySymbol));
    assert.ok(incomplete, 'the rejected observation wakes an incomplete board');
    assert.ok(incomplete.value.unscored.some((p) => p.userId === scoredExpiryUser));
    assert.ok(!boardFrames(all).some(({ value }) => value.causes.some((cause) =>
      cause[0] === scoredExpirySymbol)),
    'the rolled-back liquidation cannot leak a causal fill tuple');
  });
  T.db.prepare(
    'UPDATE paper_round_players SET peak_equity = ?, max_drawdown = ? WHERE round_id = ? AND user_id = ?'
  ).run(scoredExpiryOriginalDd.peak_equity, scoredExpiryOriginalDd.max_drawdown,
    'push', scoredExpiryUser);
  T.db.prepare('DELETE FROM paper_positions WHERE user_id = ? AND symbol = ?')
    .run(scoredExpiryUser, scoredExpirySymbol);
  recoverExpirySymbol(scoredExpirySymbol);
  await wait(70);

  /* A recovering source is provisional until its scored risk pass commits.
     Segment monitoring runs before that commit edge, so it must continue to
     see the outage that existed at ingress. Otherwise a candidate which is
     later rejected can permanently stamp the Hot window "restored" while the
     engine restores the old stale mark. Force the round briefly into Hot and
     observe the outage again at the second drawdown sample, after per-position
     writeBarrier calls have exercised their nested clock/monitor path. */
  /* These two probes directly rewrote a live row into the retired v1
     synthetic-Hot shape. V2 correctly rejects that draw/plan mutation; the
     equivalent ordinary-asset Hot and boundary-pause cases live in
     test-two-hot.js. Keep the historical fixture readable, but unreachable. */
  if (false) {
  comp.wire({
    marketReady: (sym) => T.compPriceReady(sym),
    marketReadyForBoost: (sym) => T.compPriceReady(sym),
    aliasOpen: (alias) => T.aliasOpen(alias),
  });
  const provisionalRoundBefore = T.db.prepare(
    'SELECT * FROM paper_rounds WHERE id = ?'
  ).get('push');
  const provisionalBoundariesBefore = T.db.prepare(
    'SELECT * FROM paper_round_boundaries WHERE round_id = ? ORDER BY at'
  ).all('push');
  const provisionalBoundaryInsert = T.db.prepare(
    'INSERT INTO paper_round_boundaries (round_id, at, status, error, ran_at, due_wall_at) VALUES (?, ?, ?, ?, ?, ?)'
  );
  const provisionalPlan = {
    total: 10_000, firstFive: 100, reveal: 200,
    hotStart: 300, hotEnd: 8_000, boostStart: 9_000,
  };
  const provisionalBase = 'BTC';
  const provisionalAlias = provisionalBase + '-HOT';
  const provisionalAliasBefore = T.openAliases.get(provisionalAlias);
  const provisionalSeedAt = Date.now();
  const provisionalSource = (T.activeSource(provisionalBase, provisionalSeedAt) || {}).key || 'usdt';
  const provisionalSeedPrice = Number((T.live.map.get(provisionalBase) || {}).pythPrice) || 100;
  T.compUpdate(provisionalBase, provisionalSource, provisionalSeedPrice,
    provisionalSeedAt, provisionalSeedAt);
  await wait(70);
  T.openAlias(provisionalAlias, 'push');
  const provisionalStaleAt = Date.now()
    - T.staleMsForSym(provisionalBase, provisionalSource) - 10;
  const provisionalLive = T.live.map.get(provisionalBase);
  T.live.map.set(provisionalBase, {
    ...provisionalLive,
    pythAtMs: provisionalStaleAt,
    pythSrcAtMs: provisionalStaleAt,
    lastUpdatedMs: provisionalStaleAt,
  });
  const provisionalComponents = T.comps.get(provisionalBase) || {};
  if (provisionalComponents[provisionalSource]) {
    provisionalComponents[provisionalSource] = {
      ...provisionalComponents[provisionalSource],
      ts: provisionalStaleAt,
      srcAt: provisionalStaleAt,
    };
  }
  const provisionalStart = Date.now() - 1_000;
  const provisionalOutage = [{
    segment: 'hot', alias: provisionalAlias,
    since: provisionalStart + 500,
    reason: 'deterministic unresolved price outage',
    restoredAt: null, resolvedAt: null, resolution: null,
  }];
  T.db.prepare(`UPDATE paper_rounds
                SET started_at = ?, ends_at = ?, plan_json = ?,
                    hot_base = ?, active_hot_base = ?, gate_outage = ?,
                    blocked_reason = NULL, paused_since = NULL, paused_ms = 0
                WHERE id = ?`)
    .run(provisionalStart, provisionalStart + provisionalPlan.total,
      JSON.stringify(provisionalPlan), provisionalBase, provisionalBase,
      JSON.stringify(provisionalOutage), 'push');
  T.db.prepare('DELETE FROM paper_round_boundaries WHERE round_id = ?').run('push');
  for (const at of [provisionalPlan.firstFive, provisionalPlan.reveal,
    provisionalPlan.hotStart]) {
    provisionalBoundaryInsert.run('push', at, 'succeeded', null,
      provisionalStart + at, provisionalStart + at);
  }
  assert.strictEqual(comp.phaseNow().phase, 'hot');
  assert.strictEqual(T.compPriceReady(provisionalBase), false,
    'precondition: the old mark is stale while the Hot outage is unresolved');

  const realProvisionalSample = comp.sampleDrawdown;
  let provisionalSamples = 0;
  comp.sampleDrawdown = (...args) => {
    provisionalSamples++;
    if (provisionalSamples === 2) {
      throw new Error('deterministic provisional-candidate rejection');
    }
    return realProvisionalSample(...args);
  };
  writes.length = 0;
  const provisionalCandidateAt = Date.now();
  try {
    T.compUpdate(provisionalBase, provisionalSource,
      provisionalSeedPrice + 0.0001, provisionalCandidateAt,
      provisionalCandidateAt);
  } finally {
    comp.sampleDrawdown = realProvisionalSample;
  }
  await wait(70);

  await ok('a rejected provisional price cannot clear an active segment outage', () => {
    assert.strictEqual(provisionalSamples, 2,
      'the failure lands after the candidate has exercised scored write barriers');
    const after = JSON.parse(T.db.prepare(
      'SELECT gate_outage FROM paper_rounds WHERE id = ?'
    ).get('push').gate_outage)[0];
    assert.strictEqual(after.restoredAt, null,
      'the rejected candidate cannot durably clear the outage');
    const hotEnd = T.db.prepare(
      'SELECT status FROM paper_round_boundaries WHERE round_id = ? AND at = ?'
    ).get('push', provisionalPlan.hotEnd);
    assert.notStrictEqual(hotEnd && hotEnd.status, 'succeeded',
      'the same provisional state cannot let the Hot boundary settle');
    assert.strictEqual(T.compPriceReady(provisionalBase), false,
      'the rejected candidate restores the stale mark and remains blocked');
    assert.strictEqual(priceFrames(frames(), provisionalBase).length, 0);
  });

  /* Repeat with a fresh committed entry whose only readiness guard is an
     in-progress jump confirmation. The candidate path temporarily clears
     confirmation so it can evaluate the proposed level; a rejected proposal
     must restore both that guard and the unresolved segment obligation. */
  const confirmingSetupAt = Date.now() - 1_000;
  T.db.prepare(`UPDATE paper_rounds
                SET started_at = ?, ends_at = ?, plan_json = ?,
                    hot_base = ?, active_hot_base = ?, gate_outage = ?,
                    blocked_reason = NULL, paused_since = NULL, paused_ms = 0
                WHERE id = ?`)
    .run(confirmingSetupAt, confirmingSetupAt + provisionalPlan.total,
      JSON.stringify(provisionalPlan), provisionalBase, provisionalBase,
      JSON.stringify(provisionalOutage), 'push');
  T.db.prepare('DELETE FROM paper_round_boundaries WHERE round_id = ?').run('push');
  for (const at of [provisionalPlan.firstFive, provisionalPlan.reveal,
    provisionalPlan.hotStart]) {
    provisionalBoundaryInsert.run('push', at, 'succeeded', null,
      confirmingSetupAt + at, confirmingSetupAt + at);
  }
  const confirmingRecoveryAt = Date.now();
  T.compUpdate(provisionalBase, provisionalSource, provisionalSeedPrice,
    confirmingRecoveryAt, confirmingRecoveryAt);
  await wait(70);
  assert.strictEqual(T.compPriceReady(provisionalBase), true,
    'precondition: a committed recovery clears the earlier risk block');

  const confirmingStart = Date.now() - 1_000;
  T.db.prepare(`UPDATE paper_rounds
                SET started_at = ?, ends_at = ?, plan_json = ?,
                    hot_base = ?, active_hot_base = ?, gate_outage = ?,
                    blocked_reason = NULL, paused_since = NULL, paused_ms = 0
                WHERE id = ?`)
    .run(confirmingStart, confirmingStart + provisionalPlan.total,
      JSON.stringify(provisionalPlan), provisionalBase, provisionalBase,
      JSON.stringify(provisionalOutage), 'push');
  T.db.prepare('DELETE FROM paper_round_boundaries WHERE round_id = ?').run('push');
  for (const at of [provisionalPlan.firstFive, provisionalPlan.reveal,
    provisionalPlan.hotStart]) {
    provisionalBoundaryInsert.run('push', at, 'succeeded', null,
      confirmingStart + at, confirmingStart + at);
  }
  const confirmingGuard = {
    since: confirmingStart + 500,
    from: provisionalSeedPrice,
    to: provisionalSeedPrice * 1.01,
  };
  T.confirming.set(provisionalBase, confirmingGuard);
  assert.strictEqual(T.compPriceReady(provisionalBase), false,
    'precondition: confirmation alone makes the fresh committed mark unavailable');
  T.confirming.delete(provisionalBase);
  assert.strictEqual(T.compPriceReady(provisionalBase), true,
    'removing only confirmation exposes the same fresh committed mark');
  T.confirming.set(provisionalBase, confirmingGuard);

  const realConfirmingSample = comp.sampleDrawdown;
  let confirmingSamples = 0;
  comp.sampleDrawdown = (...args) => {
    confirmingSamples++;
    if (confirmingSamples === 2) {
      throw new Error('deterministic confirming-candidate rejection');
    }
    return realConfirmingSample(...args);
  };
  writes.length = 0;
  const confirmingCandidateAt = Date.now();
  try {
    T.compUpdate(provisionalBase, provisionalSource,
      provisionalSeedPrice + 0.0002, confirmingCandidateAt,
      confirmingCandidateAt);
  } finally {
    comp.sampleDrawdown = realConfirmingSample;
  }
  await wait(70);

  await ok('a rejected candidate restores a confirmation-only outage guard', () => {
    assert.strictEqual(confirmingSamples, 2,
      'the forced failure follows both scored drawdown samples');
    const after = JSON.parse(T.db.prepare(
      'SELECT gate_outage FROM paper_rounds WHERE id = ?'
    ).get('push').gate_outage)[0];
    assert.strictEqual(after.restoredAt, null,
      'the rejected candidate cannot durably clear the confirmation outage');
    assert.deepStrictEqual(T.confirming.get(provisionalBase), confirmingGuard,
      'the exact committed confirmation guard survives rejection');
    assert.strictEqual(T.compPriceReady(provisionalBase), false,
      'the restored guard keeps the committed mark unavailable');
    assert.strictEqual(priceFrames(frames(), provisionalBase).length, 0);
  });
  T.confirming.delete(provisionalBase);

  if (provisionalAliasBefore) {
    T.openAliases.set(provisionalAlias, provisionalAliasBefore);
  } else {
    T.closeAlias(provisionalAlias, { flatten: false, roundId: 'push' });
  }
  T.db.prepare('DELETE FROM paper_round_boundaries WHERE round_id = ?').run('push');
  for (const row of provisionalBoundariesBefore) {
    provisionalBoundaryInsert.run(row.round_id, row.at, row.status, row.error,
      row.ran_at, row.due_wall_at);
  }
  T.db.prepare(`UPDATE paper_rounds
                SET started_at = ?, ends_at = ?, plan_json = ?,
                    hot_base = ?, active_hot_base = ?, gate_outage = ?,
                    blocked_reason = ?, paused_since = ?, paused_ms = ?,
                    updated_at = ?
                WHERE id = ?`)
    .run(provisionalRoundBefore.started_at, provisionalRoundBefore.ends_at,
      provisionalRoundBefore.plan_json, provisionalRoundBefore.hot_base,
      provisionalRoundBefore.active_hot_base, provisionalRoundBefore.gate_outage,
      provisionalRoundBefore.blocked_reason, provisionalRoundBefore.paused_since,
      provisionalRoundBefore.paused_ms, provisionalRoundBefore.updated_at, 'push');
  const provisionalRecoveryAt = Date.now();
  T.compUpdate(provisionalBase, provisionalSource, provisionalSeedPrice,
    provisionalRecoveryAt, provisionalRecoveryAt);
  await wait(70);
  }

  /* A non-expiry scored failure suppresses the same candidate as an expired
     one. Public liquidation ran first, so this catches an event-level split:
     both financial halves must roll back, while the competition block is
     recorded only after that rollback and therefore survives it. This test is
     last because the durable block intentionally ends the running round. */
  await ok('a scored DD failure rolls back mixed public risk but preserves the round block', async () => {
    const publicUser = 9403;
    const scoredUser = USERS[2];
    const symbol = 'MFAIL';
    T.db.prepare('INSERT INTO users (id) VALUES (?)').run(publicUser);
    T.stmt.acctIns.run(publicUser, Date.now(), Date.now());
    seedExpiryMarket(symbol);
    insertExpiryPosition(publicUser, symbol);
    insertExpiryPosition(scoredUser, symbol);
    const publicPositionBefore = T.stmt.posGet.get(publicUser, symbol);
    const scoredPositionBefore = T.stmt.posGet.get(scoredUser, symbol);
    const publicAccountBefore = T.stmt.acctGet.get(publicUser);
    const scoredAccountBefore = T.stmt.acctGet.get(scoredUser);
    const fillCount = T.db.prepare(
      'SELECT COUNT(*) AS n FROM paper_fills WHERE user_id = ? AND symbol = ?'
    );
    const publicFillsBefore = fillCount.get(publicUser, symbol).n;
    const scoredFillsBefore = fillCount.get(scoredUser, symbol).n;
    const liveBefore = T.live.map.get(symbol);
    assert.ok(publicPositionBefore && scoredPositionBefore && liveBefore);

    const realSample = comp.sampleDrawdown;
    let samples = 0;
    let transient = null;
    comp.sampleDrawdown = (...args) => {
      samples++;
      if (samples === 2) {
        transient = {
          inTransaction: T.db.inTransaction,
          publicPositionPresent: !!T.stmt.posGet.get(publicUser, symbol),
          scoredPositionPresent: !!T.stmt.posGet.get(scoredUser, symbol),
          publicFills: fillCount.get(publicUser, symbol).n,
          scoredFills: fillCount.get(scoredUser, symbol).n,
        };
        throw new Error('deterministic scored post-drawdown failure');
      }
      return realSample(...args);
    };
    writes.length = 0;
    const failureAt = Date.now();
    try {
      T.ingestIndexTick(symbol, 99.99, failureAt,
        [99.99, 99.99], 0, 'usdt', failureAt);
    } finally {
      comp.sampleDrawdown = realSample;
    }
    const immediate = frames();
    await wait(70);

    assert.strictEqual(samples, 2);
    assert.deepStrictEqual(transient, {
      inTransaction: true,
      publicPositionPresent: false,
      scoredPositionPresent: false,
      publicFills: publicFillsBefore + 1,
      scoredFills: scoredFillsBefore + 1,
    }, 'both liquidation halves existed before the injected scored failure');
    assert.deepStrictEqual(T.stmt.posGet.get(publicUser, symbol), publicPositionBefore);
    assert.deepStrictEqual(T.stmt.posGet.get(scoredUser, symbol), scoredPositionBefore);
    assert.deepStrictEqual(T.stmt.acctGet.get(publicUser), publicAccountBefore);
    assert.deepStrictEqual(T.stmt.acctGet.get(scoredUser), scoredAccountBefore);
    assert.strictEqual(fillCount.get(publicUser, symbol).n, publicFillsBefore);
    assert.strictEqual(fillCount.get(scoredUser, symbol).n, scoredFillsBefore);
    assert.strictEqual(T.live.map.get(symbol), liveBefore);
    assert.strictEqual(priceFrames(frames(), symbol).length, 0,
      'a candidate rejected by scored risk never reaches either product');
    assert.strictEqual(immediate.filter(({ value }) =>
      value.type === 'risk_failed' && value.s === symbol).length, 1);
    const round = T.db.prepare('SELECT blocked_reason FROM paper_rounds WHERE id = ?')
      .get('push');
    assert.match(String(round.blocked_reason), /deterministic scored post-drawdown failure/,
      'the scored block is persisted after, not inside, the rolled-back event');
  });
})().finally(() => {
  if (reqHandlers.close) reqHandlers.close();
  console.log(`competition push: ${passed}/36 passed`);
});
