'use strict';
/* Regression coverage for the 2026-09-04 deep-review remediations.
 *
 *   PAPER_DB=$(mktemp -u --suffix=.db) node test-review-remediation.js
 */
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawnSync } = require('child_process');

const restartFixturePhase = process.argv[2] === '--order-restart-fixture'
  ? process.argv[3] : null;
const positionRestartFixture = process.argv[2] === '--position-action-restart-fixture'
  ? { handler: process.argv[3], phase: process.argv[4] } : null;
if (restartFixturePhase || positionRestartFixture) {
  // This worker must never obtain a price/session from a real service. The
  // reopened process intentionally has no warmed market or authentication cache.
  for (const transport of [require('http'), require('https')]) {
    transport.request = transport.get = () => { throw new Error('restart fixture attempted network access'); };
  }
}

process.env.PHOENIX_SNAPSHOT_FILE = '/nonexistent/markets-snapshot.json';
process.env.PAPER_ALLOW_UNPROVEN_MARKETS = '1';
process.env.PAPER_STAGE_BASE_LEV = '500';
process.env.PAPER_BUILD_ID = 'review-test-build';
process.env.PAPER_COMP_TOKEN = 'review-admin-token';
process.env.PAPER_MAINTENANCE_FILE = path.join(os.tmpdir(), `paper-maintenance-review-${process.pid}`);
if (!process.env.PAPER_DB || process.env.PAPER_DB.startsWith('/opt/')) {
  console.error('refusing to run: set PAPER_DB to a throwaway path first');
  process.exit(2);
}

const auth = require('./auth-shim.js');
const comp = require('./competition.js');
const P = require('./paper.js');
const T = P.__test;

let currentUser = null;
auth.validateSession = async () => currentUser;

function mkReq(body = {}, headers = {}, url = '/') {
  const buf = Buffer.from(JSON.stringify(body));
  return {
    headers, url, socket: { remoteAddress: '127.0.0.1' },
    on(ev, cb) {
      if (ev === 'data') cb(buf);
      if (ev === 'end') cb();
      return this;
    },
  };
}
function mkRes() {
  const headers = {};
  return {
    code: null, body: null, headers,
    setHeader(k, v) { headers[String(k).toLowerCase()] = v; },
    writeHead(c) { this.code = c; },
    end(s) { this.body = JSON.parse(s); },
  };
}
async function order(uid, body) {
  currentUser = { id: uid, isGuest: false };
  const res = mkRes();
  await P.placeOrder(mkReq(body), res);
  T.writeRate.clear();
  return res;
}
async function positionAction(uid, handler, body, res = mkRes()) {
  currentUser = { id: uid, isGuest: false };
  try { await P[handler](mkReq(body), res); }
  finally { T.writeRate.clear(); }
  return res;
}
function account(uid, heat, balance) {
  const now = Date.now();
  T.db.prepare('INSERT OR IGNORE INTO users(id) VALUES (?)').run(uid);
  T.stmt.acctIns.run(uid, now, now);
  T.db.prepare('UPDATE paper_accounts SET heat=?, start_balance=?, balance=? WHERE user_id=?')
    .run(heat, balance, balance, uid);
}
const CFG = {
  tiers: [], maxLev: 500, lotSize: null, takerBps: 3.5, makerBps: 0.5,
  maintBps: 5000, cancelBps: 7500, maxLiqSize: null,
  status: 'active', isolatedOnly: false,
};
T.mktCfg.set('BTC', { ...CFG });
function standardMark(px) {
  const now = Date.now();
  T.live.map.set('BTC', {
    symbol: 'BTC', markPrice: px, pythPrice: px, pythBasis: 0,
    pythAtMs: now, lastUpdatedMs: now, currentFundingRate: 0, indexHalt: false,
  });
  T.live.lastMsgMs = now;
}
function stageMark(px) {
  const now = Date.now();
  standardMark(px);
  T.compUpdate('BTC', 'usdt', px, now, now);
  T.compUpdate('BTC', 'usd', px, now, now);
  T.live.lastMsgMs = now;
}

async function orderRestartFixture(phase) {
  assert.ok(phase === 'commit' || phase === 'replay', 'unknown restart fixture phase');
  const uid = 981034;
  const body = { requestId: 'review-database-reopened', accountEpoch: 1,
    symbol: 'BTC', side: 'BUY', type: 'MARKET', size: 0.01,
    leverage: 2, marginMode: 'isolated', sl: 99, tp: 101 };
  const storedRequest = () => T.db.prepare(
    'SELECT * FROM paper_order_requests WHERE user_id=? AND request_id=?'
  ).get(uid, body.requestId);
  const snapshot = () => ({
    account: T.stmt.acctGet.get(uid),
    position: T.stmt.posGet.get(uid, 'BTC'),
    fills: T.db.prepare('SELECT id, user_id, epoch, symbol, side, price, size, fee, ts FROM paper_fills WHERE user_id=? ORDER BY id').all(uid),
    request: storedRequest(),
  });

  if (phase === 'commit') {
    account(uid, 1, 10);
    stageMark(100);
    currentUser = { id: uid, isGuest: false };
    const lost = mkRes();
    lost.end = () => { throw new Error('response lost before restart'); };
    await assert.rejects(P.placeOrder(mkReq(body), lost), /response lost before restart/);
  } else {
    assert.strictEqual(T.live.map.size, 0, 'replay must work before prices warm after restart');
    const before = snapshot();
    assert.ok(before.request, 'the acknowledged result must survive database reopen');
    const expected = JSON.parse(before.request.response_json);
    const boot = mkRes();
    P.engineTime(mkReq(), boot);
    assert.notStrictEqual(boot.body.boot, expected.event.boot, 'the replay must run in a new engine process');
    for (let attempt = 0; attempt < 2; attempt++) {
      const replay = await order(uid, body);
      assert.strictEqual(replay.code, 200, JSON.stringify(replay.body));
      assert.deepStrictEqual(replay.body, { ...expected, idempotentReplay: true });
    }
    assert.deepStrictEqual(snapshot(), before, 'replay must not change balances, protection, fills or the saved result');
  }

  const result = snapshot();
  assert.strictEqual(result.fills.length, 1);
  assert.strictEqual(result.position.size, 0.01);
  assert.strictEqual(result.position.sl_price, 99);
  assert.strictEqual(result.position.tp_price, 101);
  // Close the actual SQLite handle, not just its transaction, before allowing
  // the parent to launch the process that reopens this same private DB file.
  T.db.close();
  assert.strictEqual(T.db.open, false);
  fs.writeSync(1, 'order-restart-fixture:' + JSON.stringify(result) + '\n');
  process.exit(0);
}

let passed = 0;
async function positionActionRestartFixture({ handler, phase }) {
  assert.ok(['closePosition', 'adjustMargin'].includes(handler));
  assert.ok(['commit', 'replay'].includes(phase));
  const uid = 981119;
  const body = { symbol: 'BTC', pct: 100, amount: 1,
    requestId: 'position-action-restart-' + handler, accountEpoch: 1 };
  const snapshot = () => ({
    account: T.stmt.acctGet.get(uid), position: T.stmt.posGet.get(uid, 'BTC') || null,
    fills: T.db.prepare('SELECT * FROM paper_fills WHERE user_id=? ORDER BY id').all(uid),
    request: T.db.prepare('SELECT * FROM paper_order_requests WHERE user_id=? AND request_id=?').get(uid, body.requestId),
  });
  if (phase === 'commit') {
    account(uid, 1, 10); stageMark(100);
    T.applyFill(uid, { symbol: 'BTC', orderSide: 'BUY', size: 1, px: 100,
      feeBps: 0, kind: 'MARKET', leverage: 100, marginMode: 'isolated' });
    const lost = mkRes(); lost.end = () => { throw new Error('restart action response lost'); };
    await assert.rejects(positionAction(uid, handler, body, lost), /restart action response lost/);
  } else {
    assert.strictEqual(T.live.map.size, 0);
    const before = snapshot();
    assert.ok(before.request, 'action result must survive the original process closing SQLite');
    const replay = await positionAction(uid, handler, body);
    assert.strictEqual(replay.code, 200, JSON.stringify(replay.body));
    assert.deepStrictEqual(replay.body, { ...JSON.parse(before.request.response_json), idempotentReplay: true });
    assert.deepStrictEqual(snapshot(), before);
  }
  const result = snapshot();
  assert.strictEqual(result.fills.length, handler === 'closePosition' ? 2 : 1);
  if (handler === 'closePosition') assert.strictEqual(result.position, null);
  else assert.strictEqual(result.position.isolated_margin, 2);
  T.db.close(); assert.strictEqual(T.db.open, false);
  fs.writeSync(1, 'position-action-restart-fixture:' + JSON.stringify(result) + '\n');
  process.exit(0);
}
async function test(name, fn) {
  try {
    await fn();
    passed++;
    console.log('  ok   ' + name);
  } catch (e) {
    console.error('  FAIL ' + name + '\n       ' + e.stack);
    process.exitCode = 1;
  }
}

(async () => {
  if (restartFixturePhase) return orderRestartFixture(restartFixturePhase);
  if (positionRestartFixture) return positionActionRestartFixture(positionRestartFixture);
  const STAGE = 981001;
  account(STAGE, 1, 10);
  stageMark(100);

  await test('warehouse writes retain one immutable FIFO head through deadline, HTTP failure and aborted acknowledgments', async () => {
    const { EventEmitter } = require('events');
    const beforeToken = process.env.WAREHOUSE_API_TOKEN;
    process.env.WAREHOUSE_API_TOKEN = 'isolated-test-only';
    let callback, requests = 0, destroyed = 0;
    const bodies = [];
    const fakeRequest = (_opts, cb) => {
      callback = cb; requests++;
      const req = new EventEmitter();
      req.end = body => bodies.push(body);
      req.destroy = () => { destroyed++; req.emit('error', new Error('closed')); };
      return req;
    };
    try {
      T.__persistTick('BTC', 100, Date.now(), 1, 0);
      const before = T.__tickPersistence();
      assert.strictEqual(T.__flushPersistTicks({ request: fakeRequest, timeoutMs: 20 }), true);
      T.__persistTick('BTC', 100, Date.now(), 1, 0);
      assert.strictEqual(T.__flushPersistTicks({ request: fakeRequest, timeoutMs: 20 }), false);
      assert.strictEqual(requests, 1);
      await new Promise((resolve) => setTimeout(resolve, 40));
      const expired = T.__tickPersistence();
      assert.strictEqual(destroyed, 1);
      assert.strictEqual(expired.inFlight, false);
      assert.strictEqual(expired.consecutiveFailures, before.consecutiveFailures + 1);
      assert.strictEqual(expired.droppedRows, before.droppedRows);
      assert.ok(expired.queuedBatches >= 2, 'both batches are sealed independently of the blocked request');
      T.__flushPersistTicks({ request: fakeRequest, timeoutMs: 100 });
      const failed = new EventEmitter(); failed.statusCode = 503; failed.complete = true;
      failed.resume = () => failed.emit('end'); callback(failed);
      assert.strictEqual(T.__tickPersistence().consecutiveFailures, expired.consecutiveFailures + 1);
      T.__persistTick('BTC', 100, Date.now(), 1, 0);
      T.__flushPersistTicks({ request: fakeRequest, timeoutMs: 100 });
      const aborted = new EventEmitter(); aborted.statusCode = 200; aborted.complete = false;
      aborted.resume = () => { aborted.emit('aborted'); aborted.emit('close'); }; callback(aborted);
      assert.strictEqual(destroyed, 2, 'an aborted body must release its request exactly once');
      assert.strictEqual(T.__tickPersistence().consecutiveFailures, expired.consecutiveFailures + 2);
      T.__persistTick('BTC', 100, Date.now(), 1, 0);
      T.__flushPersistTicks({ request: fakeRequest, timeoutMs: 100 });
      const success = new EventEmitter(); success.statusCode = 204; success.complete = true;
      success.resume = () => success.emit('end'); callback(success);
      assert.strictEqual(T.__tickPersistence().consecutiveFailures, 0);
      assert.strictEqual(T.__tickPersistence().inFlight, false);
      assert.strictEqual(new Set(bodies).size, 1, 'every retry sends identical namespace, sequence and rows');
      const acknowledged = T.__tickPersistence().queuedBatches;
      T.__flushPersistTicks({ request: fakeRequest, timeoutMs: 100 });
      assert.notStrictEqual(bodies.at(-1), bodies[0], 'only a confirmed ACK advances the FIFO head');
      callback(success);
      assert.strictEqual(T.__tickPersistence().queuedBatches, acknowledged - 1);
    } finally {
      if (beforeToken == null) delete process.env.WAREHOUSE_API_TOKEN;
      else process.env.WAREHOUSE_API_TOKEN = beforeToken;
    }
  });

  await test('history outbox survives reopen, preserves legitimate identical batches and refuses non-head ACKs', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'paper-outbox-fixture-'));
    const filename = path.join(dir, 'outbox.sqlite');
    const rows = [{ s: 'BTC', p: 100, t: Date.now(), n: 1, q: 1, ok: 1 }];
    let queue = T.__createIndexOutbox(filename);
    try {
      queue.seal(rows, 1000); const first = queue.head();
      assert.strictEqual(queue.status().oldestQueuedAt, 1000);
      queue.seal(rows);
      assert.strictEqual(queue.status().queuedRows, 2);
      assert.throws(() => queue.ack(first.seq + 1, first.sha256), /head_changed/);
      queue.close(); queue = T.__createIndexOutbox(filename);
      assert.strictEqual(queue.head().body, first.body, 'restart preserves exact wire identity');
      queue.ack(first.seq, first.sha256);
      const second = queue.head();
      assert.deepStrictEqual(JSON.parse(second.body).rows, JSON.parse(first.body).rows);
      assert.notStrictEqual(JSON.parse(second.body).key, JSON.parse(first.body).key,
        'identical legitimate observations are not content-deduplicated across batches');
      queue.ack(second.seq, second.sha256);
      assert.deepStrictEqual(queue.status(), { queuedBytes: 0, queuedBatches: 0, queuedRows: 0, oldestQueuedAt: null, maxQueuedBytes: 64 * 1024 * 1024 });
    } finally { queue.close(); fs.rmSync(dir, { recursive: true, force: true }); }
  });

  await test('full history outbox refuses new rows without losing its acknowledged-order head', () => {
    const queue = T.__createIndexOutbox(':memory:', { maxBytes: 256 });
    try {
      queue.seal([{ s: 'BTC', p: 100, t: Date.now() }]);
      const head = queue.head(), before = queue.status();
      assert.throws(() => queue.seal([{ s: 'BTC', p: 100, t: Date.now(), src: 'x'.repeat(256) }]), /outbox_full/);
      assert.deepStrictEqual(queue.status(), before);
      assert.strictEqual(queue.head().body, head.body);
    } finally { queue.close(); }
  });

  await test('FULL-synced history batch survives a killed writer before any HTTP acknowledgement', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'paper-outbox-killed-'));
    const filename = path.join(dir, 'outbox.sqlite');
    const childCode = `for (const p of ['http','https']) { require(p).request = require(p).get = () => { throw Error('fixture network blocked'); }; }
      const T = require(${JSON.stringify(require.resolve('./paper.js'))}).__test;
      T.__createIndexOutbox(${JSON.stringify(filename)}).seal([{s:'BTC',p:123,t:1000}]);
      process.kill(process.pid, 'SIGKILL');`;
    try {
      const child = spawnSync(process.execPath, ['-e', childCode], {
        env: { ...process.env, PAPER_DB: path.join(dir, 'child-paper.db') }, timeout: 5000, encoding: 'utf8',
      });
      assert.strictEqual(child.signal, 'SIGKILL', child.stderr);
      const queue = T.__createIndexOutbox(filename);
      try {
        assert.strictEqual(queue.status().queuedRows, 1);
        assert.deepStrictEqual(JSON.parse(queue.head().body).rows, [{ s: 'BTC', p: 123, t: 1000 }]);
      } finally { queue.close(); }
    } finally { fs.rmSync(dir, { recursive: true, force: true }); }
  });

  await test('adding a cheap-asset position preserves entry economics and liquidation precision', () => {
    const uid = 981030, px = 0.0020004;
    account(uid, 1, 10);
    T.mktCfg.set('PUMP', { ...CFG });
    const now = Date.now();
    T.compUpdate('PUMP', 'usdt', px, now, now);
    const fill = { symbol: 'PUMP', orderSide: 'BUY', size: 100,
      px, feeBps: 0, kind: 'MARKET', leverage: 100, marginMode: 'isolated' };
    T.applyFill(uid, fill); T.applyFill(uid, fill);
    const pos = T.stmt.posGet.get(uid, 'PUMP');
    assert.strictEqual(pos.entry_price, px);
    assert.ok(Math.abs(T.accountRisk(uid, T.stmt.acctGet.get(uid)).equityTotal - 10) < 1e-6);
    const expectedLiq = (pos.size * px - pos.isolated_margin) / (pos.size * (1 - 0.5 / pos.leverage));
    assert.ok(Math.abs(T.liqEstimate(pos, [pos], 0, true) - expectedLiq) < 1e-11);
  });

  await test('one accepted Stage crossing fills a protected limit before a sweep and cannot fill twice', async () => {
    const uid = 981031;
    account(uid, 1, 10); stageMark(100);
    const placed = await order(uid, { symbol: 'BTC', side: 'BUY', type: 'LIMIT',
      price: 99.99, size: 0.01, leverage: 2, marginMode: 'isolated', sl: 99, tp: 101 });
    assert.strictEqual(placed.code, 200, JSON.stringify(placed.body));
    const id = placed.body.order.id;
    await new Promise((resolve) => setTimeout(resolve, 5));
    stageMark(99.98);
    assert.strictEqual(T.stmt.ordGet.get(id).status, 'FILLED');
    const pos = T.stmt.posGet.get(uid, 'BTC');
    assert.strictEqual(pos.entry_price, 99.98);
    assert.strictEqual(pos.sl_price, 99); assert.strictEqual(pos.tp_price, 101);
    await new Promise((resolve) => setTimeout(resolve, 5));
    stageMark(100); P.sweep();
    assert.strictEqual(T.db.prepare('SELECT COUNT(*) n FROM paper_fills WHERE order_id=?').get(id).n, 1);
  });

  await test('lost order responses replay their durable result and reject changed intent or account epoch', async () => {
    const uid = 981032;
    account(uid, 1, 10); stageMark(100);
    const body = { requestId: 'review-response-lost', symbol: 'BTC', side: 'BUY', type: 'MARKET',
      size: 0.01, leverage: 2, marginMode: 'isolated' };
    currentUser = { id: uid, isGuest: false };
    const lost = mkRes(); lost.end = () => { throw new Error('response connection lost'); };
    await assert.rejects(P.placeOrder(mkReq(body), lost), /response connection lost/);
    const replay = await order(uid, body);
    assert.strictEqual(replay.code, 200, JSON.stringify(replay.body));
    assert.strictEqual(replay.body.idempotentReplay, true);
    assert.strictEqual(T.stmt.posGet.get(uid, 'BTC').size, 0.01);
    assert.strictEqual(T.db.prepare('SELECT COUNT(*) n FROM paper_fills WHERE user_id=?').get(uid).n, 1);
    assert.strictEqual((await order(uid, { ...body, size: 0.02 })).body.error, 'idempotency_conflict');
    T.db.prepare('UPDATE paper_accounts SET epoch=epoch+1 WHERE user_id=?').run(uid);
    assert.strictEqual((await order(uid, body)).body.error, 'idempotency_epoch_changed');
    assert.strictEqual(T.stmt.posGet.get(uid, 'BTC').size, 0.01);
  });

  await test('a lost partial-close response replays once, even after the remaining position is gone', async () => {
    const uid = 981101;
    account(uid, 1, 10); stageMark(100);
    T.applyFill(uid, { symbol: 'BTC', orderSide: 'BUY', size: 1, px: 100,
      feeBps: 0, kind: 'MARKET', leverage: 100, marginMode: 'isolated' });
    const body = { symbol: 'BTC', pct: 50, requestId: 'close-response-lost', accountEpoch: 1 };
    const lost = mkRes(); lost.end = () => { throw new Error('close response lost'); };
    await assert.rejects(positionAction(uid, 'closePosition', body, lost), /close response lost/);
    assert.strictEqual(T.stmt.posGet.get(uid, 'BTC').size, 0.5);
    const replay = await positionAction(uid, 'closePosition', body);
    assert.strictEqual(replay.code, 200, JSON.stringify(replay.body));
    assert.strictEqual(replay.body.idempotentReplay, true);
    assert.strictEqual(T.stmt.posGet.get(uid, 'BTC').size, 0.5);
    assert.strictEqual((await positionAction(uid, 'closePosition', { ...body, pct: 75 })).body.error, 'idempotency_conflict');
    const full = { ...body, pct: 100, requestId: 'close-rest-of-position' };
    assert.strictEqual((await positionAction(uid, 'closePosition', full)).code, 200);
    assert.strictEqual(T.stmt.posGet.get(uid, 'BTC'), undefined);
    const afterClose = T.stmt.acctGet.get(uid);
    const cold = T.live.map.get('BTC'); T.live.map.delete('BTC');
    try {
      assert.strictEqual((await positionAction(uid, 'closePosition', full)).body.idempotentReplay, true);
      assert.strictEqual((await positionAction(uid, 'closePosition', body)).body.fill.id, replay.body.fill.id);
      assert.deepStrictEqual(T.stmt.acctGet.get(uid), afterClose);
    } finally { T.live.map.set('BTC', cold); }
    T.db.prepare('UPDATE paper_accounts SET epoch=epoch+1 WHERE user_id=?').run(uid);
    assert.strictEqual((await positionAction(uid, 'closePosition', full)).body.error, 'idempotency_epoch_changed');
    assert.strictEqual(T.db.prepare('SELECT COUNT(*) n FROM paper_fills WHERE user_id=?').get(uid).n, 3);
  });

  await test('collateral retries share an atomic result and reject reuse as a different action', async () => {
    const uid = 981102;
    account(uid, 1, 10); stageMark(100);
    T.applyFill(uid, { symbol: 'BTC', orderSide: 'BUY', size: 1, px: 100,
      feeBps: 0, kind: 'MARKET', leverage: 100, marginMode: 'isolated' });
    const body = { symbol: 'BTC', amount: 1, pct: 50, requestId: 'margin-response-lost', accountEpoch: 1 };
    const lost = mkRes(); lost.end = () => { throw new Error('margin response lost'); };
    await assert.rejects(positionAction(uid, 'adjustMargin', body, lost), /margin response lost/);
    const accountAfter = T.stmt.acctGet.get(uid), positionAfter = T.stmt.posGet.get(uid, 'BTC');
    const replay = await positionAction(uid, 'adjustMargin', body);
    assert.strictEqual(replay.body.idempotentReplay, true);
    assert.strictEqual(positionAfter.isolated_margin, 2);
    assert.deepStrictEqual(T.stmt.acctGet.get(uid), accountAfter);
    assert.deepStrictEqual(T.stmt.posGet.get(uid, 'BTC'), positionAfter);
    assert.strictEqual((await positionAction(uid, 'closePosition', body)).body.error, 'idempotency_conflict');
    assert.strictEqual((await positionAction(uid, 'adjustMargin', { ...body, amount: 2 })).body.error, 'idempotency_conflict');
    T.db.prepare('UPDATE paper_accounts SET epoch=epoch+1 WHERE user_id=?').run(uid);
    assert.strictEqual((await positionAction(uid, 'adjustMargin', body)).body.error, 'idempotency_epoch_changed');
  });

  await test('position actions from an old tab fail without mutation until it supplies an ID and epoch', async () => {
    const uid = 981103;
    account(uid, 1, 10); stageMark(100);
    T.applyFill(uid, { symbol: 'BTC', orderSide: 'BUY', size: 1, px: 100,
      feeBps: 0, kind: 'MARKET', leverage: 100, marginMode: 'isolated' });
    const before = { account: T.stmt.acctGet.get(uid), position: T.stmt.posGet.get(uid, 'BTC') };
    for (const handler of ['closePosition', 'adjustMargin']) {
      for (const extra of [{}, { requestId: 'missing-action-epoch' }, { accountEpoch: 1 },
        { requestId: 'invalid-action-epoch', accountEpoch: 0 },
        { requestId: 'invalid-action-epoch', accountEpoch: true }]) {
        const r = await positionAction(uid, handler, { symbol: 'BTC', pct: 50, amount: 1, ...extra });
        assert.strictEqual(r.code, 409); assert.strictEqual(r.body.error, 'idempotency_required');
      }
      const stale = await positionAction(uid, handler, { symbol: 'BTC', pct: 50, amount: 1,
        requestId: 'stale-action-epoch', accountEpoch: 99 });
      assert.strictEqual(stale.body.error, 'idempotency_epoch_changed');
    }
    assert.deepStrictEqual({ account: T.stmt.acctGet.get(uid), position: T.stmt.posGet.get(uid, 'BTC') }, before);
  });

  await test('concurrent identical closes across the book await execute one partial fill', async () => {
    const uid = 981104;
    account(uid, 0, 10000); standardMark(100);
    T.books.map.set('BTC', { asks: [[100, 10]], bids: [[100, 10]], ts: Date.now() });
    T.applyFill(uid, { symbol: 'BTC', orderSide: 'BUY', size: 1, px: 100,
      feeBps: 0, kind: 'MARKET', leverage: 10, marginMode: 'isolated' });
    const body = { symbol: 'BTC', pct: 50, requestId: 'simultaneous-close', accountEpoch: 1 };
    const [a, b] = await Promise.all([positionAction(uid, 'closePosition', body), positionAction(uid, 'closePosition', body)]);
    assert.strictEqual(a.code, 200); assert.strictEqual(b.code, 200);
    assert.strictEqual(a.body.fill.id, b.body.fill.id);
    assert.ok(a.body.idempotentReplay || b.body.idempotentReplay);
    assert.strictEqual(T.stmt.posGet.get(uid, 'BTC').size, 0.5);
    assert.strictEqual(T.db.prepare('SELECT COUNT(*) n FROM paper_fills WHERE user_id=?').get(uid).n, 2);
  });

  await test('an account reset during a close book await cannot close the replacement position', async () => {
    const uid = 981113;
    account(uid, 0, 10000); standardMark(100); T.books.map.delete('BTC');
    T.applyFill(uid, { symbol: 'BTC', orderSide: 'BUY', size: 1, px: 100,
      feeBps: 0, kind: 'MARKET', leverage: 10, marginMode: 'isolated' });
    const pending = positionAction(uid, 'closePosition', { symbol: 'BTC', pct: 50,
      requestId: 'close-before-account-reset', accountEpoch: 1 });
    let resetFinished;
    const reset = new Promise((resolve, reject) => {
      setTimeout(() => {
        try {
          T.db.transaction(() => {
            T.stmt.posDelUser.run(uid);
            T.stmt.acctReset.run(Date.now(), Date.now(), uid);
            T.applyFill(uid, { symbol: 'BTC', orderSide: 'SELL', size: 2, px: 100,
              feeBps: 0, kind: 'MARKET', leverage: 10, marginMode: 'isolated' });
          })();
          resetFinished = { account: T.stmt.acctGet.get(uid), position: T.stmt.posGet.get(uid, 'BTC') };
          T.books.map.set('BTC', { asks: [[100, 10]], bids: [[100, 10]], ts: Date.now() });
          resolve();
        } catch (e) { reject(e); }
      }, 10);
    });
    const [response] = await Promise.all([pending, reset]);
    assert.strictEqual(response.code, 409);
    assert.strictEqual(response.body.error, 'idempotency_epoch_changed');
    assert.deepStrictEqual({ account: T.stmt.acctGet.get(uid), position: T.stmt.posGet.get(uid, 'BTC') }, resetFinished);
    assert.strictEqual(T.db.prepare('SELECT COUNT(*) n FROM paper_order_requests WHERE user_id=?').get(uid).n, 0);
  });

  await test('close and collateral results replay after SQLite closes and a new process starts without prices', () => {
    for (const handler of ['closePosition', 'adjustMargin']) {
      const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'paper-position-action-restart-'));
      try {
        const runPhase = (phase) => {
          const run = spawnSync(process.execPath, [__filename, '--position-action-restart-fixture', handler, phase], {
            cwd: __dirname, encoding: 'utf8', timeout: 10_000, maxBuffer: 1024 * 1024,
            env: { ...process.env, PAPER_DB: path.join(scratch, 'paper.db'),
              PAPER_INTERNAL_SECRET: '', WAREHOUSE_API_TOKEN: '' },
          });
          assert.ifError(run.error);
          assert.strictEqual(run.status, 0, `${handler} ${phase}: ${run.stderr}\n${run.stdout}`);
          const prefix = 'position-action-restart-fixture:';
          const line = run.stdout.split('\n').find((entry) => entry.startsWith(prefix));
          assert.ok(line, 'worker must confirm its final state after closing SQLite');
          return JSON.parse(line.slice(prefix.length));
        };
        const committed = runPhase('commit');
        assert.deepStrictEqual(runPhase('replay'), committed);
      } finally { fs.rmSync(scratch, { recursive: true, force: true }); }
    }
  });

  await test('failure to persist a position-action result rolls back its entire financial mutation', async () => {
    for (const [offset, handler] of ['closePosition', 'adjustMargin'].entries()) {
      const uid = 981105 + offset;
      account(uid, 1, 10); stageMark(100);
      T.applyFill(uid, { symbol: 'BTC', orderSide: 'BUY', size: 1, px: 100,
        feeBps: 0, kind: 'MARKET', leverage: 100, marginMode: 'isolated' });
      const before = { account: T.stmt.acctGet.get(uid), position: T.stmt.posGet.get(uid, 'BTC'),
        fills: T.db.prepare('SELECT * FROM paper_fills WHERE user_id=?').all(uid) };
      T.db.exec(`CREATE TEMP TRIGGER fail_position_action_result BEFORE INSERT ON paper_order_requests
        BEGIN SELECT RAISE(ABORT, 'fixture action result failure'); END;`);
      try {
        await assert.rejects(positionAction(uid, handler, { symbol: 'BTC', pct: 50, amount: 1,
          requestId: 'rollback-position-action', accountEpoch: 1 }), /fixture action result failure/);
      } finally { T.db.exec('DROP TRIGGER fail_position_action_result'); }
      assert.deepStrictEqual({ account: T.stmt.acctGet.get(uid), position: T.stmt.posGet.get(uid, 'BTC'),
        fills: T.db.prepare('SELECT * FROM paper_fills WHERE user_id=?').all(uid) }, before);
      assert.strictEqual(T.db.prepare('SELECT COUNT(*) n FROM paper_order_requests WHERE user_id=?').get(uid).n, 0);
    }
  });

  await test('Stage rejects explicit cross and opens omitted margin mode as isolated at native 40x risk', async () => {
    const uid = 981107;
    account(uid, 1, 10); stageMark(100);
    const old = T.mktCfg.get('BTC'); T.mktCfg.set('BTC', { ...CFG, maxLev: 40 });
    try {
      const body = { symbol: 'BTC', side: 'BUY', type: 'MARKET', size: 10, leverage: 100 };
      assert.strictEqual((await order(uid, { ...body, marginMode: 'cross' })).body.error, 'isolated_only');
      assert.strictEqual(T.stmt.posGet.get(uid, 'BTC'), undefined);
      assert.strictEqual((await order(uid, body)).code, 200);
      assert.strictEqual(T.stmt.posGet.get(uid, 'BTC').margin_mode, 'isolated');
      assert.strictEqual(T.stmt.posGet.get(uid, 'BTC').isolated_margin, 10);
      P.sweep();
      assert.ok(T.stmt.posGet.get(uid, 'BTC'), 'an unchanged index must not immediately liquidate the Stage entry');
      const cfg = mkRes(); P.engineConfig(mkReq(), cfg);
      assert.deepStrictEqual(cfg.body.modes.stage.marginModes, ['isolated']);
    } finally { T.mktCfg.set('BTC', old); }
  });

  await test('legacy Stage cross positions allow exits but never adds or flips, without moving collateral', async () => {
    const uid = 981108;
    account(uid, 1, 10); stageMark(100);
    T.applyFill(uid, { symbol: 'BTC', orderSide: 'BUY', size: 1, px: 100,
      feeBps: 0, kind: 'MARKET', leverage: 100, marginMode: 'cross' });
    const balance = T.stmt.acctGet.get(uid).balance;
    for (const [side, size] of [['BUY', 0.1], ['SELL', 2]]) {
      const r = await order(uid, { symbol: 'BTC', side, size, type: 'MARKET', leverage: 100 });
      assert.strictEqual(r.body.error, 'stage_cross_reduce_only');
    }
    const reduction = await order(uid, { symbol: 'BTC', side: 'SELL', size: 0.5,
      type: 'MARKET', leverage: 100, marginMode: 'cross' });
    assert.strictEqual(reduction.code, 200, JSON.stringify(reduction.body));
    assert.strictEqual(T.stmt.posGet.get(uid, 'BTC').margin_mode, 'cross');
    assert.strictEqual(T.stmt.posGet.get(uid, 'BTC').size, 0.5);
    assert.strictEqual(T.stmt.acctGet.get(uid).balance, balance);
    assert.strictEqual((await positionAction(uid, 'closePosition', { symbol: 'BTC',
      requestId: 'close-legacy-cross', accountEpoch: 1 })).code, 200);
    assert.strictEqual(T.stmt.posGet.get(uid, 'BTC'), undefined);
  });

  await test('legacy Stage cross resting openers are rejected and standard/scaled cross remains available', async () => {
    const uid = 981109;
    account(uid, 1, 10); stageMark(100);
    const info = T.stmt.ordIns.run(uid, 1, 'BTC', 'BUY', 100, 1, 100, 0, Date.now(), 'cross', null, null);
    P.sweep();
    const row = T.stmt.ordGet.get(Number(info.lastInsertRowid));
    assert.strictEqual(row.status, 'REJECTED'); assert.strictEqual(row.close_reason, 'stage_cross_reduce_only');
    assert.strictEqual(T.stmt.posGet.get(uid, 'BTC'), undefined);
    for (const heat of [0, 2]) {
      const id = 981110 + heat;
      account(id, heat, 10000); standardMark(100);
      T.books.map.set('BTC', { asks: [[100, 10]], bids: [[100, 10]], ts: Date.now() });
      const r = await order(id, { symbol: 'BTC', side: 'BUY', type: 'MARKET', size: 1,
        leverage: 10, marginMode: 'cross' });
      assert.strictEqual(r.code, 200, JSON.stringify(r.body));
      assert.strictEqual(T.stmt.posGet.get(id, 'BTC').margin_mode, 'cross');
    }
  });

  await test('a lost order response replays exactly after SQLite closes and a new engine process reopens it', () => {
    const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'paper-order-restart-'));
    try {
      const runPhase = (phase) => {
        const run = spawnSync(process.execPath, [__filename, '--order-restart-fixture', phase], {
          cwd: __dirname, encoding: 'utf8', timeout: 10_000, maxBuffer: 1024 * 1024,
          env: { ...process.env, PAPER_DB: path.join(scratch, 'paper.db'),
            PAPER_INTERNAL_SECRET: '', WAREHOUSE_API_TOKEN: '' },
        });
        assert.ifError(run.error);
        assert.strictEqual(run.status, 0, `${phase} failed: ${run.stderr}\n${run.stdout}`);
        const line = run.stdout.split('\n').find((entry) => entry.startsWith('order-restart-fixture:'));
        assert.ok(line, `${phase} did not confirm its SQLite close`);
        return JSON.parse(line.slice('order-restart-fixture:'.length));
      };
      const committed = runPhase('commit');
      const reopened = runPhase('replay');
      assert.deepStrictEqual(reopened, committed, 'the restart must preserve the exact financial result');
    } finally {
      fs.rmSync(scratch, { recursive: true, force: true });
    }
  });

  await test('request result persistence and financial mutation roll back as one action', async () => {
    const uid = 981033;
    account(uid, 1, 10); stageMark(100);
    T.db.exec(`CREATE TEMP TRIGGER fail_order_result BEFORE INSERT ON paper_order_requests
      BEGIN SELECT RAISE(ABORT, 'request store unavailable'); END`);
    try {
      await assert.rejects(order(uid, { requestId: 'review-store-failure', symbol: 'BTC',
        side: 'BUY', type: 'MARKET', size: 0.01, leverage: 2, marginMode: 'isolated' }), /request store unavailable/);
    } finally { T.db.exec('DROP TRIGGER fail_order_result'); }
    assert.strictEqual(T.stmt.posGet.get(uid, 'BTC'), undefined);
    assert.strictEqual(T.stmt.acctGet.get(uid).balance, 10);
    assert.strictEqual(T.db.prepare('SELECT COUNT(*) n FROM paper_fills WHERE user_id=?').get(uid).n, 0);
  });

  await test('stale ingress rejection is throttled by stable cause, not changing age', () => {
    const realNow = Date.now;
    const realLog = console.log;
    const logs = [];
    let wall = realNow();
    const before = T.__sourceRejectCount();
    Date.now = () => wall;
    console.log = (...args) => logs.push(args.join(' '));
    try {
      T.compUpdate('REVIEWSTALE', 'lazer', 100, wall, wall - 700);
      T.compUpdate('REVIEWSTALE', 'lazer', 100, wall, wall - 701);
      assert.strictEqual(logs.length, 1, 'a changing measured age must not bypass the throttle');
      assert.match(logs[0], /observation 700ms old/, 'the emitted line keeps its exact diagnostic');

      wall += 60_001;
      T.compUpdate('REVIEWSTALE', 'lazer', 100, wall, wall - 702);
      T.compUpdate('REVIEWSTALEOTHER', 'lazer', 100, wall, wall - 703);
      T.compUpdate('REVIEWSTALE', 'lazer', 100, wall, wall + 3_000);
    } finally {
      Date.now = realNow;
      console.log = realLog;
    }
    assert.strictEqual(logs.length, 4,
      'the same cause logs after 60s, while another symbol and cause stay independent');
    assert.strictEqual(T.__sourceRejectCount() - before, 3,
      'changing ages must not grow the throttle map');
  });

  await test('fills keep unavailable event provenance null instead of zero or a prior frame', () => {
    const uid = 981020;
    account(uid, 1, 10);
    stageMark(100);
    const fill = T.applyFill(uid, {
      symbol: 'BTC', orderSide: 'BUY', size: 0.01, px: 100,
      feeBps: 0, kind: 'MARKET', leverage: 2, marginMode: 'isolated',
      executionSource: 'composite-index', referenceMark: 100,
    });
    assert.strictEqual(fill.indexSeq, null);
    const row = T.db.prepare('SELECT index_seq, decision_context FROM paper_fills WHERE id=?').get(fill.id);
    assert.strictEqual(row.index_seq, null);
    assert.strictEqual(JSON.parse(row.decision_context).execution.indexSeq, null);
    assert.strictEqual(T.resolveIndexSeq(null), null);
    assert.strictEqual(T.resolveIndexSeq(undefined), null);
    assert.strictEqual(T.resolveIndexSeq(''), null);
    assert.strictEqual(T.resolveIndexSeq(0), null);
    assert.strictEqual(T.resolveIndexSeq(true), null);
    assert.strictEqual(T.resolveIndexSeq([77]), null);
    assert.strictEqual(T.resolveIndexSeq('77'), 77);
  });

  await test('Stage resting limit persists Boost choice and attached protection', async () => {
    const r = await order(STAGE, {
      symbol: 'BTC', side: 'BUY', type: 'LIMIT', price: 99.99,
      size: 0.01, leverage: 200, marginMode: 'isolated',
      sl: 99, tp: 101, boostWindow: false,
    });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    assert.ok(r.body.order, 'order should rest');
    assert.strictEqual(r.body.order.boostWindow, false);
    const raw = T.stmt.ordGet.get(r.body.order.id);
    assert.strictEqual(raw.boost_window, 0);

    stageMark(99.98);
    P.sweep();
    const filled = T.stmt.ordGet.get(raw.id);
    const pos = T.stmt.posGet.get(STAGE, 'BTC');
    assert.strictEqual(filled.status, 'FILLED');
    assert.ok(pos, 'position missing');
    assert.strictEqual(pos.entry_price, 99.98, 'Stage fill must use fresh index, not 99.99 limit');
    assert.strictEqual(pos.sl_price, 99);
    assert.strictEqual(pos.tp_price, 101);
    assert.strictEqual(pos.boost_since, null, 'boostWindow:false must survive the resting order');
    const fill = T.db.prepare('SELECT * FROM paper_fills WHERE order_id=?').get(raw.id);
    assert.strictEqual(fill.execution_source, 'composite-index');
    assert.strictEqual(fill.reference_mark, 99.98);
    assert.ok(fill.engine_boot);
    const dc = JSON.parse(fill.decision_context);
    assert.strictEqual(dc.reason, 'resting-limit-index-cross');
    assert.strictEqual(dc.execution.source, 'composite-index');
    assert.ok(dc.index.componentCount >= 2);
    assert.ok(dc.risk.before && dc.risk.after);
  });

  const GAP = 981009;
  account(GAP, 1, 10);
  await test('Stage gap rejects a resting order before silently dropping its SL', async () => {
    stageMark(100.01);
    const r = await order(GAP, {
      symbol: 'BTC', side: 'BUY', type: 'LIMIT', price: 100,
      size: 0.01, leverage: 100, marginMode: 'isolated', sl: 99.99,
    });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    assert.ok(r.body.order);
    stageMark(99.98);
    P.sweep();
    const raw = T.stmt.ordGet.get(r.body.order.id);
    assert.strictEqual(raw.status, 'REJECTED');
    assert.strictEqual(raw.close_reason, 'attached_sl_invalid_at_fill');
    assert.strictEqual(T.stmt.posGet.get(GAP, 'BTC'), undefined);
    assert.strictEqual(T.db.prepare('SELECT COUNT(*) n FROM paper_fills WHERE order_id=?').get(raw.id).n, 0);
  });

  const AUTO = 981010;
  account(AUTO, 1, 10);
  await test('automated SL fill durably records its threshold reason and risk transition', async () => {
    T.applyFill(AUTO, {
      symbol: 'BTC', orderSide: 'BUY', size: 0.01, px: 100,
      feeBps: 0, kind: 'MARKET', leverage: 2, marginMode: 'isolated',
      executionSource: 'composite-index', referenceMark: 100,
      decisionReason: 'test-position-open',
    });
    T.stmt.posSltp.run(99.95, null, AUTO, 'BTC');
    stageMark(99.94);
    P.sweep();
    assert.strictEqual(T.stmt.posGet.get(AUTO, 'BTC'), undefined);
    const f = T.db.prepare("SELECT * FROM paper_fills WHERE user_id=? AND kind='SL' ORDER BY id DESC LIMIT 1").get(AUTO);
    assert.ok(f, 'SL fill missing');
    const dc = JSON.parse(f.decision_context);
    assert.strictEqual(dc.reason, 'stop-loss-threshold-crossed');
    assert.strictEqual(dc.inputs.trigger.type, 'SL');
    assert.strictEqual(dc.inputs.trigger.threshold, 99.95);
    assert.strictEqual(dc.inputs.trigger.observedMark, 99.94);
    assert.ok(dc.risk.before && dc.risk.after);
  });

  const POST = 981002;
  account(POST, 0, 10_000);
  standardMark(100);
  await test('post-only crossing is classified against best ask, not mark', async () => {
    T.books.map.set('BTC', { asks: [[99, 10]], bids: [[98.5, 10]], ts: Date.now() });
    const r = await order(POST, {
      symbol: 'BTC', side: 'BUY', type: 'LIMIT', price: 99.5,
      size: 1, leverage: 2, postOnly: true,
    });
    assert.strictEqual(r.code, 400, JSON.stringify(r.body));
    assert.strictEqual(r.body.error, 'would_cross');
  });
  await test('post-only non-crossing quote rests even when it is above mark', async () => {
    T.books.map.set('BTC', { asks: [[101, 10]], bids: [[99, 10]], ts: Date.now() });
    const r = await order(POST, {
      symbol: 'BTC', side: 'BUY', type: 'LIMIT', price: 100.5,
      size: 1, leverage: 2, postOnly: true,
    });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    assert.ok(r.body.order, 'non-crossing post-only order should rest');
    T.stmt.ordClose.run('CANCELLED', Date.now(), r.body.order.id);
  });

  const MARGIN = 981003;
  account(MARGIN, 0, 10_000);
  await test('actual live-book VWAP is revalidated against initial margin', async () => {
    standardMark(100);
    T.books.map.set('BTC', { asks: [[100.9, 200]], bids: [[99, 200]], ts: Date.now() });
    const r = await order(MARGIN, {
      symbol: 'BTC', side: 'BUY', type: 'MARKET', notionalUsd: 9_990, leverage: 1,
    });
    assert.strictEqual(r.code, 400, JSON.stringify(r.body));
    assert.strictEqual(r.body.error, 'insufficient_margin');
    assert.strictEqual(T.stmt.posGet.get(MARGIN, 'BTC'), undefined);
    assert.strictEqual(T.db.prepare('SELECT COUNT(*) n FROM paper_fills WHERE user_id=?').get(MARGIN).n, 0);
  });
  await test('an affordable book fill records durable provenance', async () => {
    const r = await order(MARGIN, {
      symbol: 'BTC', side: 'BUY', type: 'MARKET', size: 1, leverage: 2,
    });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    assert.strictEqual(r.body.fill.executionSource, 'book');
    const f = T.db.prepare('SELECT * FROM paper_fills WHERE id=?').get(r.body.fill.id);
    assert.strictEqual(f.execution_source, 'book');
    assert.strictEqual(f.reference_mark, 100);
    assert.strictEqual(f.engine_boot, r.body.fill.engineBoot);
    const dc = JSON.parse(f.decision_context);
    assert.strictEqual(dc.reason, 'market-order-immediate');
    assert.strictEqual(dc.execution.source, 'book');
    assert.strictEqual(dc.book.bestAsk[0], 100.9);
    assert.ok(dc.book.sha256.length === 64);
    assert.ok(dc.risk.before && dc.risk.after);
    assert.ok(Buffer.byteLength(f.decision_context) <= 12 * 1024);
  });

  const FLIP = 981012;
  account(FLIP, 0, 1_000);
  await test('a public flip is funded by the margin its close returns, at the fill price', async () => {
    /* Isolated long that takes nearly the whole bankroll, then one SELL for
       twice the size. The reservation check and the post-walk check both had
       to count the closing slice's margin, or the flip was refused with a
       balance that would end exactly where it started. */
    standardMark(100);
    T.books.map.set('BTC', { asks: [[100.1, 500]], bids: [[99.9, 500]], ts: Date.now() });
    let r = await order(FLIP, { symbol: 'BTC', side: 'BUY', type: 'MARKET', size: 90, leverage: 10, marginMode: 'isolated' });
    assert.strictEqual(r.code, 200, JSON.stringify(r.body));
    const before = T.stmt.posGet.get(FLIP, 'BTC');
    assert.strictEqual(before.side, 'LONG');
    const freeBefore = T.stmt.acctGet.get(FLIP).balance;
    assert.ok(freeBefore < before.isolated_margin, 'free alone cannot fund the new leg: ' + freeBefore);
    r = await order(FLIP, { symbol: 'BTC', side: 'SELL', type: 'MARKET', size: 180, leverage: 10, marginMode: 'isolated' });
    assert.strictEqual(r.code, 200, 'the flip must pass both margin checks: ' + JSON.stringify(r.body));
    const after = T.stmt.posGet.get(FLIP, 'BTC');
    assert.strictEqual(after.side, 'SHORT');
    assert.ok(Math.abs(after.size - 90) < 1e-9, 'same size the other way: ' + after.size);
    assert.ok(after.isolated_margin > 0.95 * before.isolated_margin, 'the new leg carries about the same margin: ' + after.isolated_margin);
    r = await order(FLIP, { symbol: 'BTC', side: 'BUY', type: 'MARKET', size: 300, leverage: 10, marginMode: 'isolated' });
    assert.strictEqual(r.code, 400, 'a flip that needs more than the close returns is still refused: ' + JSON.stringify(r.body));
    assert.strictEqual(r.body.error, 'insufficient_margin');
    assert.strictEqual(T.stmt.posGet.get(FLIP, 'BTC').side, 'SHORT');
  });
  const FIFO1 = 981004, FIFO2 = 981005, FIFO3 = 981006, FIFO4 = 981007, FIFO5 = 981008, FIFO6 = 981011;
  account(FIFO1, 0, 10_000); account(FIFO2, 0, 10_000);
  account(FIFO3, 0, 10_000); account(FIFO4, 0, 10_000); account(FIFO5, 0, 10_000); account(FIFO6, 0, 10_000);
  await test('cross-source copies deduplicate under the declared per-user counterfactual model', async () => {
    standardMark(100);
    T.prints.map.clear(); T.prints.seen.clear(); T.prints.lastOkMs = 0;
    const created = Date.now() - 10;
    const a1 = T.stmt.acctGet.get(FIFO1), a2 = T.stmt.acctGet.get(FIFO2);
    const o1 = Number(T.stmt.ordIns.run(FIFO1, a1.epoch, 'BTC', 'BUY', 100, 1, 2, 0, created, 'cross', null, null).lastInsertRowid);
    const o2 = Number(T.stmt.ordIns.run(FIFO2, a2.epoch, 'BTC', 'BUY', 100, 1, 2, 0, created, 'cross', null, null).lastInsertRowid);
    const tradeAt = Date.now();
    T.ingestPrint('BTC', 99, tradeAt, 'ws:BTC:777', 1);
    T.ingestPrint('BTC', 99, tradeAt, 'wh:signature-redacted:BTC:99:99', 1);
    assert.strictEqual(T.printedVolumeThrough('BTC', 'BUY', 100, created), 1,
      'WS + warehouse copies must count once');
    P.sweep();
    assert.strictEqual(T.stmt.ordGet.get(o1).status, 'FILLED', 'oldest order gets the print');
    assert.strictEqual(T.stmt.ordGet.get(o2).status, 'FILLED', 'each account is an explicit counterfactual tape simulation');
    assert.strictEqual(T.printedVolumeThrough('BTC', 'BUY', 100, created), 1,
      'there is no restart-unsafe process-global remaining-volume claim');
    const fills = T.db.prepare('SELECT decision_context FROM paper_fills WHERE order_id IN (?,?) ORDER BY id').all(o1, o2);
    assert.strictEqual(fills.length, 2);
    for (const f of fills) {
      const dc = JSON.parse(f.decision_context);
      assert.strictEqual(dc.inputs.liquidity.model, 'per-user-counterfactual');
      assert.strictEqual(dc.inputs.liquidity.eligibleBase, 1);
    }
  });
  await test('counterfactual print-through remains deterministic across resting prices', async () => {
    T.prints.map.clear(); T.prints.seen.clear(); T.prints.lastOkMs = 0;
    const oldAt = Date.now() - 20;
    const newerAt = Date.now() - 10;
    const a3 = T.stmt.acctGet.get(FIFO3), a4 = T.stmt.acctGet.get(FIFO4);
    const worseOld = Number(T.stmt.ordIns.run(FIFO3, a3.epoch, 'BTC', 'BUY', 99, 1, 2, 0, oldAt, 'cross', null, null).lastInsertRowid);
    const betterNew = Number(T.stmt.ordIns.run(FIFO4, a4.epoch, 'BTC', 'BUY', 100, 1, 2, 0, newerAt, 'cross', null, null).lastInsertRowid);
    T.ingestPrint('BTC', 98, Date.now(), 'ws:BTC:778', 1);
    P.sweep();
    assert.strictEqual(T.stmt.ordGet.get(betterNew).status, 'FILLED', 'better-priced bid gets the lot first');
    assert.strictEqual(T.stmt.ordGet.get(worseOld).status, 'FILLED', 'the other account independently sees the eligible tape');
  });
  await test('partial-fill watermark advances by event time, so delayed backfill keeps progressing', async () => {
    T.prints.map.clear(); T.prints.seen.clear(); T.prints.lastOkMs = 0;
    const firstAt = Date.now() - 5_000;
    const a6 = T.stmt.acctGet.get(FIFO6);
    const oid = Number(T.stmt.ordIns.run(FIFO6, a6.epoch, 'BTC', 'BUY', 100, 2, 2, 0, firstAt - 100, 'cross', null, null).lastInsertRowid);
    T.ingestPrint('BTC', 99, firstAt, 'wh:delayed-one:BTC:99:99', 1);
    P.sweep();
    let raw = T.stmt.ordGet.get(oid);
    assert.strictEqual(raw.status, 'OPEN');
    assert.strictEqual(raw.size, 1);
    assert.strictEqual(raw.vol_ts, firstAt, 'cursor is the consumed event, not later processing wall time');

    T.ingestPrint('BTC', 99, firstAt + 100, 'wh:delayed-two:BTC:99:99', 1);
    P.sweep();
    raw = T.stmt.ordGet.get(oid);
    assert.strictEqual(raw.status, 'FILLED', 'a later event remains eligible even though both arrived behind wall time');
  });
  await test('a rolled-back counterfactual fill remains eligible for retry', async () => {
    T.prints.map.clear(); T.prints.seen.clear(); T.prints.lastOkMs = 0;
    const created = Date.now() - 10;
    const a5 = T.stmt.acctGet.get(FIFO5);
    const oid = Number(T.stmt.ordIns.run(FIFO5, a5.epoch, 'BTC', 'BUY', 100, 1, 2, 0, created, 'cross', null, null).lastInsertRowid);
    T.ingestPrint('BTC', 99, Date.now(), 'ws:BTC:779', 1);
    T.db.exec(`CREATE TRIGGER fail_review_fill BEFORE INSERT ON paper_fills
               WHEN NEW.user_id=${FIFO5} BEGIN SELECT RAISE(ABORT,'injected'); END;`);
    P.sweep();
    assert.strictEqual(T.stmt.ordGet.get(oid).status, 'OPEN');
    assert.strictEqual(T.printedVolumeThrough('BTC', 'BUY', 100, created), 1,
      'a failed database savepoint must not advance the order watermark');
    T.db.exec('DROP TRIGGER fail_review_fill');
    P.sweep();
    assert.strictEqual(T.stmt.ordGet.get(oid).status, 'FILLED');
  });

  await test('config/account disclose versioned mode economics and units', async () => {
    const cfgRes = mkRes();
    P.engineConfig(mkReq(), cfgRes);
    assert.strictEqual(cfgRes.body.apiVersion, 1);
    assert.strictEqual(cfgRes.body.schemaVersion, 2);
    assert.strictEqual(cfgRes.body.buildId, 'review-test-build');
    assert.strictEqual(cfgRes.body.modes.stage.displayScale, 10_000);
    assert.strictEqual(cfgRes.body.modes.standard.venueParity, false);
    assert.strictEqual(cfgRes.body.modes.standard.liquidityModel, 'per-user-counterfactual');
    assert.strictEqual(cfgRes.body.modes.standard.sharedLiquidity, false);
    assert.strictEqual(cfgRes.body.markets.BTC.maintenanceBps, 5000);
    currentUser = { id: STAGE, isGuest: false };
    const acctRes = mkRes();
    await P.account(mkReq(), acctRes);
    assert.strictEqual(acctRes.body.account.mode, 'stage');
    assert.strictEqual(acctRes.body.account.displayScale, 10_000);
  });

  await test('maintenance sentinel refuses admin mutations without touching SQLite', async () => {
    const before = T.db.prepare('SELECT COUNT(*) n FROM paper_operator_log').get().n;
    fs.writeFileSync(process.env.PAPER_MAINTENANCE_FILE, 'review test\n', { mode: 0o600 });
    try {
      const blocked = mkRes();
      await P.compAdmin(mkReq({ action: 'wall', mode: 'holding' }, {
        'x-comp-token': process.env.PAPER_COMP_TOKEN,
      }), blocked);
      assert.strictEqual(blocked.code, 503, JSON.stringify(blocked.body));
      assert.strictEqual(blocked.body.error, 'maintenance');
      assert.strictEqual(blocked.body.retryable, true);
      assert.strictEqual(T.db.prepare('SELECT COUNT(*) n FROM paper_operator_log').get().n, before,
        'maintenance refusal must not prolong the database drain');

      const readable = mkRes();
      await P.compAdmin(mkReq({ action: 'log', limit: 1 }, {
        'x-comp-token': process.env.PAPER_COMP_TOKEN,
      }), readable);
      assert.strictEqual(readable.code, 200, JSON.stringify(readable.body));
    } finally {
      try { fs.unlinkSync(process.env.PAPER_MAINTENANCE_FILE); } catch {}
    }
  });

  await test('invite header works and a successful start purges its raw capability immediately', async () => {
    const roundId = 'invite-transport-review';
    comp.createRound({
      id: roundId, candidates: ['BTC', 'SOL', 'ETH'], backup: 'ETH',
      players: [{ userId: STAGE, displayName: 'Seat One', seat: 0 }],
    });
    const token = T.db.prepare('SELECT invite_token FROM paper_round_players WHERE round_id=?').get(roundId).invite_token;
    currentUser = null;
    const queryRes = mkRes();
    await P.compInvite(mkReq({}, {}, '/api/paper/comp/invite?t=' + token), queryRes,
      new URL('http://x/api/paper/comp/invite?t=' + token));
    assert.strictEqual(queryRes.code, 400);
    assert.strictEqual(queryRes.body.error, 'invite_token_in_url_not_allowed');

    const headerRes = mkRes();
    await P.compInvite(mkReq({}, { 'x-invite-token': token }), headerRes,
      new URL('http://x/api/paper/comp/invite'));
    assert.strictEqual(headerRes.code, 200, JSON.stringify(headerRes.body));
    assert.strictEqual(headerRes.headers['cache-control'], 'no-store');
    assert.strictEqual(headerRes.headers['referrer-policy'], 'no-referrer');

    comp.wire({
      marketReady: () => true, // Explicit fixture price policy.
      openAlias: T.openAlias, closeAlias: T.closeAlias, scoreUser: T.scoreUser,
      scoreProofFor: T.scoreProofFor,
      prepareSeat: T.prepareSeat, seatState: T.seatState, markSetFor: T.markSetFor,
      log: () => {},
    });
    const startRes = mkRes();
    await P.compAdmin(mkReq({ action: 'start', id: roundId }, {
      'x-comp-token': process.env.PAPER_COMP_TOKEN,
    }), startRes);
    assert.strictEqual(startRes.code, 200, JSON.stringify(startRes.body));
    assert.strictEqual(startRes.body.round.status, 'running');
    assert.strictEqual(T.db.prepare('SELECT invite_token FROM paper_round_players WHERE round_id=?').get(roundId).invite_token, null);

    const abortRes = mkRes();
    await P.compAdmin(mkReq({ action: 'abort', id: roundId }, {
      'x-comp-token': process.env.PAPER_COMP_TOKEN,
    }), abortRes);
    assert.strictEqual(abortRes.code, 200, JSON.stringify(abortRes.body));
  });

  await test('practice leaderboard explicitly disclaims official ranking', async () => {
    const bot = comp.botIdForSeat(3);
    account(bot, 1, 10);
    T.db.prepare('UPDATE paper_accounts SET fills_count=1 WHERE user_id=?').run(bot);
    const scaled = 981012;
    account(scaled, 2, 10);
    T.db.prepare('UPDATE paper_accounts SET fills_count=1 WHERE user_id=?').run(scaled);
    const expectedHumans = T.stmt.acctAll.all()
      .filter((a) => Number(a.heat) === 1 && !comp.isBotId(a.user_id)).length;
    currentUser = null;
    const res = mkRes();
    await P.leaderboard(mkReq({}, {}, '/api/paper/leaderboard?mode=stage'), res);
    assert.strictEqual(res.body.official, false);
    assert.strictEqual(res.body.boardKind, 'practice');
    assert.strictEqual(res.body.resetsAllowed, true);
    assert.strictEqual(res.body.officialResultsPath, '/api/paper/comp/state');
    assert.strictEqual(res.body.total, expectedHumans,
      'reserved rehearsal drivers and scaled-paper accounts must not enter the Stage ranking');
  });

  await test('shutdown seals its final RAM tail without waiting for unavailable history delivery', async () => {
    T.__persistTick('BTC', 123, Date.now(), 1, 0);
    assert.ok(T.__tickPersistence().bufferedRows > 0);
    const started = Date.now();
    const result = await P.drainTickPersistence(0);
    assert.strictEqual(result.sealed, true);
    assert.ok(result.pendingRows > 0);
    assert.strictEqual(T.__tickPersistence().bufferedRows, 0);
    assert.ok(Date.now() - started < 1000, 'zero-send drain should only commit its bounded local tail');
  });

  console.log(`\n${passed} review-remediation checks passed`);
  process.exit(process.exitCode || 0);
})().catch((e) => {
  console.error(e.stack || e);
  process.exit(1);
});
