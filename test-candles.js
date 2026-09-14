'use strict';
/* Cold candle reads are deliberately parallel, but they must remain bounded
 * and exact-key duplicate requests must share the whole built page. No live
 * service, database or upstream is touched here. */
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { EventEmitter } = require('events');
const http = require('http');
const https = require('https');

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'phoenix-paper-candles-'));
process.env.PAPER_DB = path.join(tmp, 'paper.db');
process.env.PHOENIX_SNAPSHOT_FILE = path.join(tmp, 'missing-snapshot.json');
process.env.PAPER_ALLOW_UNPROVEN_MARKETS = '1';
process.env.WAREHOUSE_API_TOKEN = 'test-only-warehouse';

const paper = require('./paper.js');
const realHttpsRequest = https.request;
const realHttpRequest = http.request;
const pending = [];
let baseCalls = 0;
let warehouseCalls = 0;
const barAt = Math.floor(Date.now() / 60_000) * 60_000;

function deferredRequest(kind, callback) {
  if (kind === 'base') baseCalls++;
  else warehouseCalls++;
  const req = new EventEmitter();
  req.destroy = () => {};
  req.end = () => pending.push(() => {
    const res = new EventEmitter();
    res.statusCode = 200;
    callback(res);
    const body = kind === 'base'
      ? JSON.stringify([[barAt, '100', '102', '99', '101', '4']])
      : JSON.stringify({ ok: true, rows: [{ time: barAt, open: 100, high: 103, low: 98, close: 101.5, ticks: 4 }] });
    queueMicrotask(() => { res.emit('data', Buffer.from(body)); res.emit('end'); });
  });
  return req;
}

function request(url) {
  return new Promise((resolve) => {
    const req = { url, headers: {}, socket: { remoteAddress: '127.0.0.1' } };
    const res = {
      code: null,
      headers: {},
      setHeader(name, value) { this.headers[String(name).toLowerCase()] = value; },
      writeHead(code) { this.code = code; },
      end(body) { resolve({ code: this.code, body: JSON.parse(body) }); },
    };
    void paper.stageCandles(req, res);
  });
}

async function releaseAll() {
  while (pending.length) pending.shift()();
  await new Promise((resolve) => setImmediate(resolve));
}

async function withRoundCandleFixture(run) {
  const comp = paper.comp;
  const previous = { currentRound: comp.currentRound, roundMarkCandles: comp.roundMarkCandles };
  const round = { id: 'round-candle-prefix', status: 'running',
    started_at: barAt - 60_000 + 2524, price_policy: 'last-accepted-v1' };
  const calls = [];
  comp.currentRound = () => round;
  comp.roundMarkCandles = (id, symbol, options) => {
    calls.push({ id, symbol, ...options });
    return { rows: [], breaks: [], fromAvailable: round.started_at, complete: true };
  };
  const url = (endTime, symbol = 'BTC', timeframe = '1m') =>
    `/api/paper/candles?symbol=${symbol}&timeframe=${timeframe}&limit=10`
      + (endTime == null ? '' : `&endTime=${endTime}`)
      + `&roundId=${round.id}&pricePolicy=last-accepted-v1`;
  try { await run({ comp, round, calls, url }); }
  finally { comp.currentRound = previous.currentRound; comp.roundMarkCandles = previous.roundMarkCandles; }
}

function historyFixture(maxInflight = 5) {
  const capacity = paper.__test.createHistoryCapacity({ baseCandles: 2, indexCandles: 2, indexLine: 1 }, maxInflight);
  const deadlines = new Map(), requests = [];
  let scheduled = 0;
  const timers = {
    setTimeout(fn) { const id = ++scheduled; deadlines.set(id, fn); return id; },
    clearTimeout(id) { deadlines.delete(id); },
  };
  const transport = { request(_options, callback) {
    const req = new EventEmitter(), res = new EventEmitter();
    const entry = { req, res, destroyed: 0 };
    res.statusCode = 200;
    req.destroy = () => { entry.destroyed++; };
    req.end = () => callback(res);
    requests.push(entry);
    return req;
  } };
  return {
    capacity, deadlines, requests, timers,
    scheduled: () => scheduled,
    read: (name, via = transport) => paper.__test.historyText(name, via, { timeout: 50 }, 16, capacity, timers),
  };
}

let passed = 0;
let total = 0;
async function test(name, fn) {
  total++;
  try {
    await fn();
    passed++;
    console.log('  ok   ' + name);
  } catch (e) {
    console.error('  FAIL ' + name + '\n       ' + (e && e.stack || e));
    process.exitCode = 1;
  }
}

(async () => {
  https.request = (_options, callback) => deferredRequest('base', callback);
  http.request = (_options, callback) => deferredRequest('warehouse', callback);

  await test('round candles return an exact scoped empty prefix before inception', async () => {
    await withRoundCandleFixture(async ({ round, calls, url }) => {
      const before = { baseCalls, warehouseCalls, pending: pending.length };
      let boot;
      for (const [symbol, timeframe, end] of [
        ['BTC', '1m', round.started_at - 1],
        ['BTC-BOOST', '15m', Math.floor(round.started_at / 900_000) * 900_000],
        ['BTC', '1d', Math.floor(round.started_at / 86_400_000) * 86_400_000],
      ]) {
        const result = await request(url(end, symbol, timeframe));
        assert.strictEqual(result.code, 200);
        assert.ok(typeof result.body.boot === 'string' && result.body.boot.length > 0);
        boot ??= result.body.boot;
        assert.deepStrictEqual(result.body, { ok: true, boot, roundId: round.id,
          pricePolicy: 'last-accepted-v1', symbol: 'BTC', source: 'round-execution',
          rows: [], breaks: [], fromAvailable: round.started_at, complete: true });
      }
      assert.deepStrictEqual(calls, [], 'pre-round pages do not ask storage for a reversed range');
      assert.deepStrictEqual({ baseCalls, warehouseCalls, pending: pending.length }, before,
        'no public or venue history is substituted into the round prefix');
    });
  });

  await test('the exact round start and current candle window still query durable history', async () => {
    await withRoundCandleFixture(async ({ round, calls, url }) => {
      const exact = await request(url(round.started_at));
      assert.strictEqual(exact.code, 200);
      assert.deepStrictEqual(calls[0], { id: round.id, symbol: 'BTC',
        from: round.started_at, to: round.started_at, tf: '1m' });
      const current = await request(url(null));
      assert.strictEqual(current.code, 200);
      assert.strictEqual(calls.length, 2);
      assert.strictEqual(calls[1].from, round.started_at);
      assert.ok(calls[1].to >= round.started_at);
      assert.deepStrictEqual(current.body.rows, [], 'no initial mark is fabricated');
    });
  });

  await test('pre-round paging cannot bypass scope or supported-market validation', async () => {
    await withRoundCandleFixture(async ({ round, calls, url }) => {
      const end = round.started_at - 1;
      for (const bad of [url(end).replace(round.id, 'other-round'),
        url(end).replace('last-accepted-v1', 'strict')]) {
        const result = await request(bad);
        assert.strictEqual(result.code, 409);
        assert.strictEqual(result.body.error, 'round_history_scope');
      }
      for (const bad of [url(end, 'NOTAMARKET'), url(end, 'BTC', '2m')]) {
        const result = await request(bad);
        assert.strictEqual(result.code, 400);
        assert.strictEqual(result.body.error, 'unsupported');
      }
      assert.deepStrictEqual(calls, []);
    });
  });

  await test('actual round candle storage and projection failures remain unavailable', async () => {
    await withRoundCandleFixture(async ({ comp, round, url }) => {
      let reads = 0;
      comp.roundMarkCandles = () => { reads++; throw new Error('ordinary history unavailable fixture'); };
      for (const end of [round.started_at, null, 'not-a-number']) {
        const result = await request(url(end));
        assert.strictEqual(result.code, 503);
        assert.strictEqual(result.body.error, 'round_history_unavailable');
      }
      assert.strictEqual(reads, 3, 'invalid endTime keeps its existing latest-window handling');
      const originalNow = Date.now;
      Date.now = () => round.started_at - 10;
      try {
        for (const end of [round.started_at, round.started_at + 1000]) {
          const result = await request(url(end));
          assert.strictEqual(result.code, 503,
            'a regressed callback clock cannot turn an at/after-start request into a pre-round page');
          assert.strictEqual(result.body.error, 'round_history_unavailable');
        }
        assert.strictEqual(reads, 5, 'only the explicit requested end identifies the empty prefix');
      } finally { Date.now = originalNow; }
      comp.roundMarkCandles = () => ({ rows: [], breaks: [], fromAvailable: NaN, complete: true });
      const invalid = await request(url(round.started_at));
      assert.strictEqual(invalid.code, 503);
      assert.strictEqual(invalid.body.error, 'round_history_unavailable');
    });
  });

  await test('index-only buckets are appended and survive a base-history failure', async () => {
    const mine = new Map([
      [barAt - 60_000, { open: 98, high: 101, low: 97, close: 100 }],
      [barAt, { open: 100, high: 104, low: 99, close: 103 }],
    ]);
    const venue = [{ time: barAt - 60_000, open: 97, high: 100, low: 96, close: 99, volume: 7 }];
    const merged = paper.__test.mergeStageCandleRows(venue, null, mine, 10);
    assert.deepStrictEqual(merged.rows.map((r) => r.time), [barAt - 60_000, barAt]);
    assert.strictEqual(merged.rows[0].close, 100, 'matching venue history is replaced by the index');
    assert.strictEqual(merged.rows[1].close, 103, 'the live index bucket is not dropped merely because venue history stops');

    const indexOnly = paper.__test.mergeStageCandleRows(null, 'btcusdt', mine, 10);
    assert.deepStrictEqual(indexOnly.rows.map((r) => r.close), [100, 103], 'warehouse history is sufficient when the optional base fails');
  });

  await test('trimmed buckets do not inflate returned index provenance', () => {
    const mine = new Map(Array.from({ length: 11 }, (_, i) => [barAt - i * 60_000,
      { open: 100, high: 101, low: 99, close: 100 }]));
    const merged = paper.__test.mergeStageCandleRows([], null, mine, 10);
    assert.strictEqual(merged.rows.length, 10);
    assert.strictEqual(merged.patched + merged.added, 10);
  });

  await test('one accepted observation preserves its reached price and public provenance stays honest on fallback', async () => {
    const oldHttp = http.request, oldHttps = https.request;
    const sparseAt = barAt - 600_000;
    const response = body => (_options, callback) => {
      const req = new EventEmitter(); req.destroy = () => {};
      req.end = () => queueMicrotask(() => {
        const res = new EventEmitter(); res.statusCode = 200; callback(res);
        res.emit('data', Buffer.from(JSON.stringify(body))); res.emit('end');
      });
      return req;
    };
    try {
      http.request = response({ ok: true, rows: [{ time: sparseAt, open: 80, high: 80, low: 80, close: 80, ticks: 1 }] });
      https.request = response([[sparseAt, '100', '102', '99', '101', '4']]);
      const sparse = await request(`/api/paper/candles?symbol=SOL&timeframe=1m&limit=10&endTime=${sparseAt + 60_000}`);
      assert.strictEqual(sparse.code, 200);
      assert.strictEqual(sparse.body.rows[0].low, 80, 'external 99 low cannot erase the accepted 80 observation');
      assert.strictEqual(sparse.body.rows[0].observedTicks, 1);
      assert.strictEqual(sparse.body.history.basis, 'accepted-observations');
      assert.strictEqual(sparse.body.history.complete, false, 'sampled public history is never a complete execution audit');
      assert.strictEqual(sparse.body.history.indexBars, 1);
      http.request = response({ ok: false });
      const fallback = await request(`/api/paper/candles?symbol=SOL&timeframe=5m&limit=10&endTime=${sparseAt + 60_000}`);
      assert.strictEqual(fallback.body.history.basis, 'external');
      assert.strictEqual(fallback.body.history.indexUnavailable, true);
      assert.strictEqual(fallback.body.rows[0].priceSource, 'binance');
      const cached = await request(`/api/paper/candles?symbol=SOL&timeframe=5m&limit=10&endTime=${sparseAt + 60_000}`);
      assert.deepStrictEqual(cached.body.history, fallback.body.history, 'cache retains source availability');
    } finally { http.request = oldHttp; https.request = oldHttps; }
  });

  for (const mode of ['silent-connect', 'partial-body', 'trickling-body']) {
    await test(`history absolute deadline releases ${mode}`, async () => {
      let destroyed = 0, trickle = null;
      const transport = { request(_options, callback) {
        const req = new EventEmitter();
        req.destroy = () => { destroyed++; clearInterval(trickle); };
        req.end = () => {
          if (mode === 'silent-connect') return;
          queueMicrotask(() => {
            const res = new EventEmitter(); res.statusCode = 200;
            callback(res); res.emit('data', Buffer.from('partial'));
            if (mode === 'trickling-body') trickle = setInterval(() => res.emit('data', Buffer.from('.')), 2);
          });
        };
        return req;
      } };
      try {
        assert.strictEqual(await paper.__test.historyText('baseCandles', transport, { timeout: 20 }), null);
        assert.strictEqual(destroyed, 1);
        assert.strictEqual(paper.__test.historyCapacitySnapshot().active, 0);
      } finally { clearInterval(trickle); }
    });
  }

  for (const mode of ['aborted', 'response-error', 'premature-close', 'http-error', 'oversize', 'multibyte-oversize', 'request-error']) {
    await test(`history handles ${mode} once and closes its socket`, async () => {
      let destroyed = 0;
      const transport = { request(_options, callback) {
        const req = new EventEmitter(); req.destroy = () => { destroyed++; };
        req.end = () => queueMicrotask(() => {
          if (mode === 'request-error') return req.emit('error', new Error('fixture'));
          const res = new EventEmitter(); res.statusCode = mode === 'http-error' ? 503 : 200;
          callback(res);
          if (mode === 'aborted') res.emit('aborted');
          else if (mode === 'response-error') res.emit('error', new Error('fixture'));
          else if (mode === 'premature-close') res.emit('close');
          else if (mode === 'oversize') res.emit('data', Buffer.alloc(5));
          else if (mode === 'multibyte-oversize') res.emit('data', '€€');
          // Real response teardown may produce every one of these events.
          res.emit('aborted'); res.emit('error', new Error('late fixture')); res.emit('close');
          res.emit('end'); req.emit('error', new Error('late request'));
        });
        return req;
      } };
      assert.strictEqual(await paper.__test.historyText('baseCandles', transport, { timeout: 1000 }, 4), null);
      assert.strictEqual(destroyed, 1);
      assert.strictEqual(paper.__test.historyCapacitySnapshot().active, 0);
    });
  }

  await test('history success accepts a bounded UTF-8 body and ignores normal close', async () => {
    let destroyed = 0;
    const transport = { request(_options, callback) {
      const req = new EventEmitter(); req.destroy = () => { destroyed++; };
      req.end = () => queueMicrotask(() => {
        const res = new EventEmitter(); res.statusCode = 200; callback(res);
        const body = Buffer.from('€');
        res.emit('data', body.subarray(0, 1)); res.emit('data', body.subarray(1));
        res.emit('end'); res.emit('close');
      });
      return req;
    } };
    assert.strictEqual(await paper.__test.historyText('baseCandles', transport, { timeout: 1000 }, 3), '€');
    assert.strictEqual(destroyed, 0);
    assert.strictEqual(paper.__test.historyCapacitySnapshot().active, 0);
  });

  await test('production history reservations keep the existing64 total and24 paired-build capacity', () => {
    const snapshot = paper.__test.historyCapacitySnapshot();
    assert.strictEqual(snapshot.maxInflight, 64);
    assert.deepStrictEqual(snapshot.limits, { baseCandles: 24, indexCandles: 24, indexLine: 16 });
    assert.strictEqual(Object.values(snapshot.limits).reduce((sum, n) => sum + n, 0), 64);
    snapshot.limits.baseCandles = 100;
    snapshot.byClass.baseCandles = 100;
    assert.strictEqual(paper.__test.historyCapacitySnapshot().limits.baseCandles, 24);
    assert.strictEqual(paper.__test.historyCapacitySnapshot().active, 0);
  });

  await test('tiny history classes cannot borrow reservations or allocate on refusal', async () => {
    const f = historyFixture();
    const reads = [f.read('indexLine')];
    assert.strictEqual(await f.read('indexLine'), null);
    assert.strictEqual(f.requests.length, 1, 'tick history cannot borrow unused candle slots');
    assert.strictEqual(f.scheduled(), 1, 'refused calls create no deadline');
    reads.push(f.read('baseCandles'), f.read('indexCandles'), f.read('baseCandles'), f.read('indexCandles'));
    assert.deepStrictEqual(f.capacity.snapshot().byClass, { baseCandles: 2, indexCandles: 2, indexLine: 1 });
    assert.strictEqual(f.capacity.snapshot().active, 5);
    for (const name of ['baseCandles', 'indexCandles', 'indexLine', 'unknown', 'toString']) {
      assert.strictEqual(await f.read(name), null);
    }
    assert.strictEqual(f.requests.length, 5);
    assert.strictEqual(f.scheduled(), 5);
    for (const { req } of f.requests) req.emit('error', new Error('fixture'));
    assert.ok((await Promise.all(reads)).every((body) => body === null));
    assert.strictEqual(f.capacity.snapshot().active, 0);
    assert.strictEqual(f.deadlines.size, 0);
  });

  await test('global history ceiling remains independent of class ceilings and releases once', async () => {
    const f = historyFixture(4);
    const reads = [f.read('baseCandles'), f.read('baseCandles'), f.read('indexCandles'), f.read('indexCandles')];
    assert.strictEqual(await f.read('indexLine'), null, 'free class capacity cannot exceed the global ceiling');
    assert.strictEqual(f.scheduled(), 4);
    f.requests[0].req.emit('error', new Error('fixture'));
    assert.strictEqual(await reads[0], null);
    reads.push(f.read('indexLine'));
    assert.strictEqual(f.capacity.snapshot().active, 4);
    for (const { req } of f.requests) req.emit('error', new Error('late fixture'));
    await Promise.all(reads);
    assert.deepStrictEqual(f.capacity.snapshot().byClass, { baseCandles: 0, indexCandles: 0, indexLine: 0 });
    assert.strictEqual(f.capacity.snapshot().active, 0);
    assert.strictEqual(f.deadlines.size, 0);
  });

  for (const mode of ['success', 'request-error', 'aborted', 'deadline', 'constructor-error']) {
    await test(`tiny history reservation releases once after ${mode} and late events`, async () => {
      const f = historyFixture();
      const read = f.read('indexLine', mode === 'constructor-error'
        ? { request() { throw new Error('fixture constructor'); } } : undefined);
      const entry = f.requests[0];
      if (mode === 'success') { entry.res.emit('data', Buffer.from('ok')); entry.res.emit('end'); }
      else if (mode === 'request-error') entry.req.emit('error', new Error('fixture'));
      else if (mode === 'aborted') entry.res.emit('aborted');
      else if (mode === 'deadline') [...f.deadlines.values()][0]();
      assert.strictEqual(await read, mode === 'success' ? 'ok' : null);
      assert.strictEqual(f.capacity.snapshot().active, 0);
      assert.strictEqual(f.deadlines.size, 0);
      const next = f.read('indexLine');
      if (entry) {
        entry.res.emit('aborted'); entry.res.emit('error', new Error('late fixture'));
        entry.res.emit('close'); entry.res.emit('end'); entry.req.emit('error', new Error('late request'));
        assert.strictEqual(entry.destroyed, mode === 'success' ? 0 : 1);
      }
      assert.strictEqual(f.capacity.snapshot().active, 1, 'late completion cannot release the next owner');
      assert.strictEqual(await f.read('indexLine'), null);
      f.requests.at(-1).req.emit('error', new Error('cleanup'));
      assert.strictEqual(await next, null);
      assert.strictEqual(f.capacity.snapshot().active, 0);
      assert.strictEqual(f.deadlines.size, 0);
    });
  }

  await test('all four history paths settle an interrupted response', async () => {
    const interrupt = (_options, callback) => {
      const req = new EventEmitter(); req.destroy = () => {};
      req.end = () => queueMicrotask(() => {
        const res = new EventEmitter(); res.statusCode = 200; callback(res);
        res.emit('data', Buffer.from('{')); res.emit('aborted');
      });
      return req;
    };
    https.request = http.request = interrupt;
    try {
      const paths = [
        paper.__test.fetchKlines('BTCUSDT', '1m', 10, 0),
        paper.__test.fetchVenueCandles('ANSEM', '1m', 10, 0),
        paper.__test.indexCandles('BTC', '1m', 10, 0),
        paper.__test.indexLine('BTC', barAt - 60_000, barAt),
      ];
      assert.deepStrictEqual(paper.__test.historyCapacitySnapshot().byClass,
        { baseCandles: 2, indexCandles: 1, indexLine: 1 }, 'all four internal callers select the intended reservation');
      assert.deepStrictEqual(await Promise.all(paths), [null, null, null, null]);
      assert.strictEqual(paper.__test.historyCapacitySnapshot().active, 0);
      const reads = Array.from({ length: 24 }, (_, i) => request(`/api/paper/candles?symbol=ETH&timeframe=1m&limit=10&endTime=${barAt - (i + 1) * 60_000}`));
      assert.ok((await Promise.all(reads)).every((r) => r.code === 502));
      const after = await request('/api/paper/candles?symbol=ETH&timeframe=5m&limit=10');
      assert.strictEqual(after.code, 502, 'failed builds release slots; not candles_busy');
    } finally {
      https.request = (_options, callback) => deferredRequest('base', callback);
      http.request = (_options, callback) => deferredRequest('warehouse', callback);
    }
  });

  await test('cached rows are overlaid with the exact accepted live close without mutation', async () => {
    const original = paper.__test.live.map.get('BTC');
    const cached = [{ time: barAt, open: 100, high: 102, low: 99, close: 101,
      markOpen: 100, markHigh: 102, markLow: 99, markClose: 101, volume: 4 }];
    paper.__test.live.map.set('BTC', { pythPrice: 103.125, pythAtMs: barAt + 30_000, acceptedSeq: 77 });
    try {
      const view = paper.__test.reconcileLiveCandleRows('BTC', '1m', cached, 10);
      assert.strictEqual(view.rows.at(-1).close, 103.125);
      assert.strictEqual(view.rows.at(-1).markClose, 103.125);
      assert.strictEqual(view.rows.at(-1).high, 103.125);
      assert.strictEqual(view.lastAcceptedSeq, 77);
      assert.strictEqual(cached[0].close, 101, 'the shared cache remains immutable');

      const previousOnly = [{ ...cached[0], time: barAt - 60_000 }];
      const appended = paper.__test.reconcileLiveCandleRows('BTC', '1m', previousOnly, 10);
      assert.deepStrictEqual(appended.rows.map((r) => r.time), [barAt - 60_000, barAt]);
      assert.strictEqual(appended.rows.at(-1).close, 103.125);
    } finally {
      if (original) paper.__test.live.map.set('BTC', original); else paper.__test.live.map.delete('BTC');
    }
  });

  await test('a just-closed explicit page waits for persistence before becoming immutable', async () => {
    const end = barAt + 59_999;
    assert.strictEqual(paper.__test.candleCacheTtl('1m', end, barAt + 69_999), 20_000);
    assert.strictEqual(paper.__test.candleCacheTtl('1m', end, barAt + 70_000), 6 * 3600_000);
  });

  await test('identical cold reads share upstreams and the completed page', async () => {
    const reads = Array.from({ length: 12 }, () => request('/api/paper/candles?symbol=BTC&timeframe=15m&limit=500'));
    await new Promise((resolve) => setImmediate(resolve));
    assert.strictEqual(baseCalls, 1);
    assert.strictEqual(warehouseCalls, 1);
    await releaseAll();
    const answers = await Promise.all(reads);
    assert.ok(answers.every((r) => r.code === 200));
    assert.ok(answers.every((r) => JSON.stringify(r.body.rows) === JSON.stringify(answers[0].body.rows)));
    assert.strictEqual(answers[0].body.rows[0].high, 103, 'the shared page includes the warehouse patch');
  });

  await test('a paired cold candle build completes while an unrelated tick seed remains outstanding', async () => {
    const oldHttp = http.request;
    let lineReq;
    http.request = (options, callback) => {
      if (!options.path.startsWith('/internal/index-line?')) return oldHttp(options, callback);
      lineReq = new EventEmitter(); lineReq.destroy = () => {}; lineReq.end = () => {};
      return lineReq;
    };
    const line = paper.__test.indexLine('ETH', barAt - 120_000, barAt);
    try {
      const cold = request('/api/paper/candles?symbol=BTC&timeframe=1h&limit=10');
      assert.deepStrictEqual(paper.__test.historyCapacitySnapshot().byClass,
        { baseCandles: 1, indexCandles: 1, indexLine: 1 });
      await releaseAll();
      assert.strictEqual((await cold).code, 200);
      assert.deepStrictEqual(paper.__test.historyCapacitySnapshot().byClass,
        { baseCandles: 0, indexCandles: 0, indexLine: 1 });
    } finally {
      http.request = oldHttp;
      lineReq.emit('error', new Error('cleanup'));
      await line;
    }
    assert.strictEqual(paper.__test.historyCapacitySnapshot().active, 0);
  });

  await test('distinct-key fan-out is capped and releases its slots', async () => {
    const beforeBase = baseCalls;
    const beforeWarehouse = warehouseCalls;
    const reads = Array.from({ length: 24 }, (_, i) => request(`/api/paper/candles?symbol=BTC&timeframe=1m&limit=500&endTime=${barAt - (i + 1) * 60_000}`));
    await new Promise((resolve) => setImmediate(resolve));
    assert.strictEqual(baseCalls - beforeBase, 24);
    assert.strictEqual(warehouseCalls - beforeWarehouse, 24);
    const refused = await request(`/api/paper/candles?symbol=BTC&timeframe=1m&limit=500&endTime=${barAt - 30 * 60_000}`);
    assert.strictEqual(refused.code, 503);
    assert.strictEqual(refused.body.error, 'candles_busy');
    assert.strictEqual(refused.body.retryable, true);
    assert.strictEqual(baseCalls - beforeBase, 24, 'the refused key must not open another socket');
    await releaseAll();
    await Promise.all(reads);

    const afterCleanup = request(`/api/paper/candles?symbol=BTC&timeframe=1m&limit=500&endTime=${barAt - 31 * 60_000}`);
    await new Promise((resolve) => setImmediate(resolve));
    assert.strictEqual(baseCalls - beforeBase, 25, 'a completed build releases its global slot');
    await releaseAll();
    assert.strictEqual((await afterCleanup).code, 200);
  });
})().finally(() => {
  https.request = realHttpsRequest;
  http.request = realHttpRequest;
  try { fs.rmSync(tmp, { recursive: true, force: true }); } catch {}
  console.log(`candle integrity/concurrency: ${passed}/${total} passed`);
});
