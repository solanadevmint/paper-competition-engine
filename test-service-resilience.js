'use strict';
/* Service-edge regression tests: snapshot cache validation/provenance and the
   remote-identity outage posture. No live service or production database is
   touched. */
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { EventEmitter } = require('events');
const https = require('https');
const { performance } = require('perf_hooks');

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'phoenix-paper-resilience-'));
process.env.PAPER_DB = path.join(tmp, 'paper.db');
process.env.PHOENIX_SNAPSHOT_FILE = path.join(tmp, 'markets-snapshot.json');
process.env.PAPER_MAINTENANCE_FILE = path.join(tmp, 'deploy-maintenance');
process.env.PAPER_GATE_SECRET = 'test-only-gate';
process.env.PAPER_INTERNAL_SECRET = 'test-only-internal';
delete process.env.PAPER_BUILD_ID;

const service = require('./server.js');
const auth = require('./auth-shim.js');
const paper = require('./paper.js');
const S = service.__test;
let pass = 0;

async function ok(name, fn) {
  try {
    await fn();
    console.log('  ok   ' + name);
    pass++;
  } catch (e) {
    console.error('  FAIL ' + name + '\n       ' + (e && e.stack || e));
    process.exitCode = 1;
  }
}

function fixture(now, count = 12, rowAgeMs = 500) {
  const markets = {};
  for (let i = 0; i < count; i++) {
    const symbol = `T${i}`;
    markets[symbol] = { symbol, markPrice: 100 + i, lastUpdatedMs: now - rowAgeMs };
  }
  return JSON.stringify({ updatedAt: now - 250, source: 'test', markets });
}

function throwsCode(fn, code) {
  assert.throws(fn, (e) => e && e.code === code, `expected ${code}`);
}

function localRequest(url, method = 'GET', headers = {}) {
  return new Promise((resolve) => {
    const req = { url, method, headers, socket: { remoteAddress: '127.0.0.1' } };
    const res = {
      code: null,
      headers: {},
      setHeader(name, value) { this.headers[String(name).toLowerCase()] = value; },
      writeHead(code, headers) {
        this.code = code;
        for (const [name, value] of Object.entries(headers || {})) this.setHeader(name, value);
      },
      end(body) { resolve({ code: this.code, headers: this.headers, body: body == null ? null : JSON.parse(body) }); },
    };
    service.server.emit('request', req, res);
  });
}

function fakeHttps() {
  let answer = { statusCode: 200, body: '{}' };
  let calls = 0;
  let destroyed = 0;
  const requests = [];
  const request = (options, callback) => {
    calls++;
    requests.push(options);
    const response = answer;
    const req = new EventEmitter();
    req.write = () => {};
    req.destroy = (err) => {
      if (!req.destroyed) destroyed++;
      req.destroyed = true;
      if (err) queueMicrotask(() => req.emit('error', err));
    };
    options.signal?.addEventListener('abort', () => {
      queueMicrotask(() => req.emit('error', Object.assign(new Error('aborted'), { name: 'AbortError' })));
    }, { once: true });
    const deliver = () => {
      if (response.error) return req.emit('error', Object.assign(new Error(response.error),
        response.errorCode ? { code: response.errorCode } : {}));
      const res = new EventEmitter();
      res.statusCode = response.statusCode;
      callback(res);
      queueMicrotask(() => {
        if (response.chunks) { for (const chunk of response.chunks) res.emit('data', chunk); }
        else if (response.body != null) res.emit('data', Buffer.from(response.body));
        if (response.aborted) return res.emit('aborted');
        if (response.hang) return;
        res.emit('end');
      });
    };
    req.end = () => {
      if (response.socket) queueMicrotask(() => {
        req.reusedSocket = response.reusedSocket === true;
        req.emit('socket', response.socket);
        for (const [delay, event] of response.transportEvents || []) {
          setTimeout(() => response.socket.emit(event), delay);
        }
      });
      return response.delayMs > 0 ? setTimeout(deliver, response.delayMs) : queueMicrotask(deliver);
    };
    return req;
  };
  return {
    request,
    set(value) { answer = value; },
    calls() { return calls; },
    destroyed() { return destroyed; },
    requests,
  };
}

/* Advance only this fixture's request deadlines/retry timers. No real HTTP,
   wall-clock sleep, production service or globally installed timer is used. */
function fakeTimeouts(startAt) {
  const original = { setTimeout, clearTimeout, now: Date.now,
    performanceNow: Object.getOwnPropertyDescriptor(performance, 'now') };
  const pending = new Set();
  let clock = startAt;
  global.setTimeout = (callback, delay = 0) => {
    const timer = { callback, at: clock + Math.max(0, delay), unref() { return this; } };
    pending.add(timer);
    return timer;
  };
  global.clearTimeout = (timer) => {
    if (!pending.delete(timer)) original.clearTimeout(timer);
  };
  Date.now = () => clock;
  Object.defineProperty(performance, 'now', { configurable: true, value: () => clock });
  const flush = () => new Promise((resolve) => setImmediate(resolve));
  return {
    flush,
    now: () => clock,
    delays: () => [...pending].map((timer) => timer.at - clock).sort((a, b) => a - b),
    async advance(ms) {
      const until = clock + ms;
      await flush();
      for (;;) {
        const timer = [...pending].filter((item) => item.at <= until).sort((a, b) => a.at - b.at)[0];
        if (!timer) break;
        pending.delete(timer);
        clock = timer.at;
        timer.callback();
        await flush();
      }
      clock = until;
      await flush();
    },
    restore() {
      global.setTimeout = original.setTimeout;
      global.clearTimeout = original.clearTimeout;
      Date.now = original.now;
      if (original.performanceNow) Object.defineProperty(performance, 'now', original.performanceNow);
      else delete performance.now;
      pending.clear();
    },
  };
}

async function withProbeFixture(run) {
  auth.stopHealthProbe();
  const original = { request: https.request, warn: console.warn, info: console.info };
  const timers = fakeTimeouts(1_800_000_000_000);
  const fake = fakeHttps();
  const logs = [];
  https.request = fake.request;
  console.warn = console.info = (...parts) => logs.push(parts.join(' '));
  try { await run({ timers, fake, logs }); }
  finally {
    auth.stopHealthProbe();
    await timers.flush();
    https.request = original.request;
    console.warn = original.warn;
    console.info = original.info;
    timers.restore();
  }
}

(async () => {
  const now = Date.now();
  await ok('origin keepalive outlasts the nginx upstream pool', () => {
    assert.strictEqual(service.server.keepAliveTimeout, 65_000);
    assert.ok(service.server.headersTimeout > service.server.keepAliveTimeout,
      'headers must remain open beyond the keepalive deadline');
  });
  await ok('upstream reads have a whole-response deadline and cancel a hanging body', async () => {
    let destroyed = 0;
    const transport = { request(_url, _options, callback) {
      const req = new EventEmitter();
      req.destroy = () => { destroyed++; };
      req.end = () => queueMicrotask(() => {
        const res = new EventEmitter(); res.statusCode = 200;
        callback(res); res.emit('data', Buffer.from('partial'));
      });
      return req;
    } };
    await assert.rejects(S.remoteText(transport, 'http://fixture', {}, 20, 100), /deadline/);
    assert.strictEqual(destroyed, 1);
  });
  await ok('upstream reads reject oversized, aborted and HTTP-error responses', async () => {
    for (const mode of ['oversized', 'aborted', 'http']) {
      let destroyed = 0;
      const transport = { request(_url, _options, callback) {
        const req = new EventEmitter(); req.destroy = () => { destroyed++; };
        req.end = () => queueMicrotask(() => {
          const res = new EventEmitter(); res.statusCode = mode === 'http' ? 503 : 200;
          callback(res);
          if (mode === 'oversized') res.emit('data', Buffer.from('too much'));
          if (mode === 'aborted') res.emit('aborted');
          res.emit('end');
        });
        return req;
      } };
      await assert.rejects(S.remoteText(transport, 'http://fixture', {}, 100, 4));
      assert.strictEqual(destroyed, 1, mode);
    }
  });
  console.log('\nsnapshot validation');
  await ok('the production default points at the JSON endpoint', () => {
    assert.strictEqual(S.SNAPSHOT_SRC, 'https://perp.so/markets-snapshot.json');
  });
  await ok('readiness fails closed while no valid fallback exists', async () => {
    const realReadiness = paper.readiness;
    paper.readiness = () => ({ ok: true, reasons: [], boot: 'test-boot' });
    try {
      const r = await localRequest('/readyz');
      assert.strictEqual(r.code, 503);
      assert.strictEqual(r.body.ok, false);
      assert.strictEqual(r.body.fallback.ok, false);
      assert.strictEqual(r.body.build, S.BUILD_ID);
      assert.strictEqual(r.body.boot, 'test-boot');
      assert.ok(r.body.reasons.includes('snapshot fallback unavailable'));
      assert.strictEqual(r.body.auth.configured, true);
      assert.strictEqual(r.body.auth.expiredCacheFallback, false);
    } finally {
      paper.readiness = realReadiness;
    }
  });
  await ok('HEAD inherits GET status and headers without an entity body', async () => {
    const r = await localRequest('/healthz', 'HEAD');
    assert.strictEqual(r.code, 200);
    assert.strictEqual(r.headers['content-type'], 'application/json');
    assert.strictEqual(r.body, null);
  });
  await ok('finite APIs set a uniform private response baseline for GET and HEAD', async () => {
    const gate = { 'x-paper-gate': process.env.PAPER_GATE_SECRET };
    for (const method of ['GET', 'HEAD']) {
      const r = await localRequest('/api/paper/config', method, gate);
      assert.strictEqual(r.code, 200);
      assert.match(String(r.headers['cache-control']), /(?:^|,\s*)no-store(?:$|,)/);
      assert.strictEqual(r.headers['referrer-policy'], 'no-referrer');
      assert.strictEqual(r.headers['x-content-type-options'], 'nosniff');
      if (method === 'HEAD') assert.strictEqual(r.body, null);
    }
  });
  await ok('HEAD cannot be normalized into a long-lived SSE subscription', async () => {
    const r = await localRequest('/api/paper/pyth-stream', 'HEAD', { 'x-paper-gate': process.env.PAPER_GATE_SECRET });
    assert.strictEqual(r.code, 404);
    assert.strictEqual(r.body, null);
  });
  await ok('deployment maintenance exposes only certification reads and quiesces risk', async () => {
    const gate = { 'x-paper-gate': process.env.PAPER_GATE_SECRET };
    fs.writeFileSync(process.env.PAPER_MAINTENANCE_FILE, '');
    try {
      assert.strictEqual((await localRequest('/healthz', 'GET')).body.maintenance, true);
      assert.strictEqual((await localRequest('/api/paper/config', 'GET', gate)).code, 200);
      assert.strictEqual((await localRequest('/api/paper/comp/state', 'GET', gate)).code, 200);
      for (const [url, method] of [
        ['/api/paper/account', 'GET'],
        ['/api/paper/account', 'HEAD'],
        ['/api/paper/order', 'POST'],
        ['/api/paper/pyth-stream', 'GET'],
      ]) {
        const r = await localRequest(url, method, gate);
        assert.strictEqual(r.code, 503, `${method} ${url}`);
        if (method !== 'HEAD') assert.strictEqual(r.body.error, 'maintenance');
      }
      assert.deepStrictEqual(paper.__test.tickEval('BTC', { force: true }), {
        ok: true, skipped: 'deployment maintenance',
      });
      assert.strictEqual(paper.sweep(), undefined);
    } finally {
      fs.unlinkSync(process.env.PAPER_MAINTENANCE_FILE);
    }
  });
  await ok('a fresh, populated snapshot passes schema validation', () => {
    const checked = S.validateSnapshotDocument(fixture(now), now);
    assert.strictEqual(checked.totalMarkets, 12);
    assert.strictEqual(checked.freshMarkets, 12);
  });
  await ok('a 200 SPA document cannot pass as a snapshot', () => {
    assert.strictEqual(S.jsonContentType('text/html; charset=utf-8'), false);
    throwsCode(() => S.validateSnapshotDocument('<!doctype html><title>app</title>', now), 'invalid_json');
  });
  await ok('stale and future-dated snapshots are rejected', () => {
    const stale = JSON.parse(fixture(now)); stale.updatedAt = now - 60_001;
    throwsCode(() => S.validateSnapshotDocument(JSON.stringify(stale), now), 'stale_snapshot');
    const future = JSON.parse(fixture(now)); future.updatedAt = now + 30_001;
    throwsCode(() => S.validateSnapshotDocument(JSON.stringify(future), now), 'future_timestamp');
  });
  await ok('thin or stale market sets are rejected', () => {
    throwsCode(() => S.validateSnapshotDocument(fixture(now, 9), now), 'too_few_markets');
    throwsCode(() => S.validateSnapshotDocument(fixture(now, 12, 60_001), now), 'too_few_fresh_markets');
  });
  await ok('only a checked snapshot atomically replaces the cache', () => {
    const first = S.validateSnapshotDocument(fixture(now), now);
    assert.strictEqual(S.installSnapshot(first, now), true);
    const before = fs.readFileSync(process.env.PHOENIX_SNAPSHOT_FILE, 'utf8');
    throwsCode(() => S.validateSnapshotDocument('<html>wrong route</html>', now), 'invalid_json');
    assert.strictEqual(fs.readFileSync(process.env.PHOENIX_SNAPSHOT_FILE, 'utf8'), before);
  });
  await ok('an HTML 200 from the configured source preserves last-known-good', async () => {
    const before = fs.readFileSync(process.env.PHOENIX_SNAPSHOT_FILE, 'utf8');
    const realGet = https.get;
    https.get = (url, options, callback) => {
      const req = new EventEmitter();
      req.destroy = () => {};
      queueMicrotask(() => {
        const res = new EventEmitter();
        res.statusCode = 200;
        res.headers = { 'content-type': 'text/html; charset=utf-8' };
        res.destroy = () => {};
        callback(res);
        queueMicrotask(() => {
          res.emit('data', Buffer.from('<!doctype html><title>paper app</title>'));
          res.emit('end');
        });
      });
      return req;
    };
    try {
      S.syncSnapshot();
      await new Promise((resolve) => setImmediate(resolve));
      await new Promise((resolve) => setImmediate(resolve));
    } finally {
      https.get = realGet;
    }
    assert.strictEqual(fs.readFileSync(process.env.PHOENIX_SNAPSHOT_FILE, 'utf8'), before);
    assert.strictEqual(S.snapshotHealth(now).lastError, 'content_type');
  });
  await ok('fallback health exposes build, age and eventual staleness', () => {
    assert.match(S.BUILD_ID, /^[0-9a-f]{16}$/);
    assert.strictEqual(process.env.PAPER_BUILD_ID, S.BUILD_ID);
    const healthy = S.snapshotHealth(now + 1000);
    assert.strictEqual(healthy.ok, true);
    assert.strictEqual(healthy.freshMarkets, 12);
    assert.strictEqual(S.snapshotHealth(now + 60_001).ok, false);
  });

  console.log('\nauthentication outage posture');
  const realRequest = https.request;
  const realNow = Date.now;
  const fake = fakeHttps();
  https.request = fake.request;
  let clock = now;
  Date.now = () => clock;
  try {
    await ok('a verified session is cached for the bounded fresh TTL', async () => {
      fake.set({ statusCode: 200, body: JSON.stringify({ ok: true, user: { id: 741 }, pub: { id: 741 }, isGuest: false }) });
      assert.deepStrictEqual(await auth.validateSession('session-a'), { id: 741 });
      clock += 29_000;
      assert.deepStrictEqual(await auth.validateSession('session-a'), { id: 741 });
      assert.strictEqual(fake.calls(), 1);
      assert.strictEqual(auth.authHealth(clock).upstream, 'ok');
    });
    await ok('an expired positive fails closed if identity cannot be revalidated', async () => {
      clock += 1_001;
      fake.set({ error: 'identity upstream unavailable' });
      assert.strictEqual(await auth.validateSession('session-a'), null);
      assert.strictEqual(fake.calls(), 2);
      assert.strictEqual(auth.authHealth(clock).upstream, 'unavailable');
      assert.strictEqual(auth.authHealth(clock).expiredCacheFallback, false);
    });
    await ok('local bot sessions remain available without remote identity', async () => {
      const token = auth.mintBotSession(9_900_123, (id) => id >= 9_900_000);
      assert.deepStrictEqual(await auth.validateSession(token), { id: 9_900_123 });
      auth.dropBotSessions();
    });
    await ok('simultaneous validations share one upstream request', async () => {
      fake.set({ statusCode: 200, body: JSON.stringify({ ok: true, user: { id: 742 } }) });
      const before = fake.calls();
      const users = await Promise.all(Array.from({ length: 8 }, () => auth.validateSession('session-coalesced')));
      assert.strictEqual(fake.calls() - before, 1);
      for (const user of users) assert.deepStrictEqual(user, { id: 742 });
    });
    await ok('identity body hangs and aborts cannot leave validations in flight', async () => {
      fake.set({ statusCode: 200, body: '{}', hang: true });
      await assert.rejects(auth.__test.mainJson('GET', '/fixture', null, 20), /timeout/);
      fake.set({ statusCode: 200, body: '{}', aborted: true });
      await assert.rejects(auth.__test.mainJson('GET', '/fixture', null, 20), /aborted/);
    });
    await ok('identity responses preserve names split across UTF-8 network chunks', async () => {
      const body = Buffer.from(JSON.stringify({ ok: true, name: 'Zoë 🚀' }));
      const at = body.indexOf(Buffer.from('🚀')) + 1;
      fake.set({ statusCode: 200, chunks: [body.subarray(0, at), body.subarray(at)] });
      assert.deepStrictEqual(await auth.__test.mainJson('GET', '/fixture', null, 20),
        { ok: true, name: 'Zoë 🚀' });
    });
    await ok('identity health is independently leased, and invalid cookies do not pause trading', async () => {
      fake.set({ statusCode: 200, body: JSON.stringify({ ok: true, user: null }) });
      const starting = auth.startHealthProbe();
      assert.strictEqual(auth.authHealth(clock).tradingAvailable, false);
      await starting;
      assert.strictEqual(auth.authHealth(clock).tradingAvailable, true);
      assert.strictEqual(auth.authHealth(clock).validUntil, clock + 6000);
      assert.strictEqual(await auth.validateSession('invalid-cookie-fixture'), null);
      assert.strictEqual(auth.authHealth(clock).tradingAvailable, true);
      assert.strictEqual(auth.authHealth(clock + 6000).tradingAvailable, false);
      assert.strictEqual(auth.authHealth(clock + 6000).unavailableSince, clock + 6000);
    });
    await ok('a confirmed identity outage makes readiness fail until the probe recovers', async () => {
      const realReadiness = paper.readiness;
      paper.readiness = () => ({ ok: true, reasons: [], boot: 'test-boot' });
      try {
        fake.set({ statusCode: 403, body: JSON.stringify({ ok: false, error: 'forbidden' }) });
        await auth.__test.probeIdentity();
        assert.strictEqual(auth.authHealth(clock).tradingAvailable, false);
        assert.strictEqual(auth.authHealth(clock).unavailableSince, clock);
        const r = await localRequest('/readyz');
        assert.strictEqual(r.code, 503);
        assert.ok(r.body.reasons.includes('identity service unavailable'));
        fake.set({ statusCode: 200, body: JSON.stringify({ ok: true, user: null }) });
        await auth.__test.probeIdentity();
        assert.strictEqual(auth.authHealth(clock).tradingAvailable, true);
        assert.strictEqual(auth.authHealth(clock).unavailableSince, null);
      } finally { paper.readiness = realReadiness; }
    });
  } finally {
    auth.stopHealthProbe();
    https.request = realRequest;
    Date.now = realNow;
  }

  console.log('\nidentity probe recovery and sanitized diagnostics');
  const healthyProbe = { statusCode: 200, body: JSON.stringify({ ok: true, user: null }) };
  await ok('healthy probes retain the 2s cadence, 1.5s deadline and 6s lease', () => withProbeFixture(async ({ timers, fake, logs }) => {
    fake.set({ ...healthyProbe, delayMs: 17 });
    const starting = auth.startHealthProbe();
    assert.strictEqual(auth.authHealth().tradingAvailable, false);
    assert.strictEqual(auth.startHealthProbe(), starting, 'probe startup is single-flight');
    assert.strictEqual(auth.__test.probeIdentity(), starting);
    await timers.advance(17);
    assert.strictEqual(await starting, true);
    const health = auth.authHealth();
    assert.strictEqual(health.tradingAvailable, true);
    assert.strictEqual(health.validUntil, timers.now() + 6000);
    assert.strictEqual(health.probeLastOutcome, 'ok');
    assert.strictEqual(health.probeLastDurationMs, 17);
    assert.strictEqual(health.probeLastSuccessDurationMs, 17);
    assert.strictEqual(health.probeLastFailureAgeMs, null);
    assert.strictEqual(health.probeFailures, 0);
    assert.strictEqual(health.probeRetryMs, 2000);
    assert.deepStrictEqual(timers.delays(), [2000]);
    assert.strictEqual(fake.requests[0].timeout, 1500);
    assert.strictEqual(fake.requests[0].path, '/api/internal/paper-auth');
    await timers.advance(1999);
    assert.strictEqual(fake.calls(), 1);
    await timers.advance(1);
    assert.strictEqual(fake.calls(), 2);
    await timers.advance(17);
    assert.strictEqual(auth.authHealth().probeFailures, 0);
    assert.strictEqual(logs.length, 0, 'ordinary healthy probes do not spam logs');
  }));
  await ok('a transient probe failure retains only its existing lease and retries after 250ms', () => withProbeFixture(async ({ timers, fake }) => {
    fake.set(healthyProbe);
    await auth.startHealthProbe();
    const priorLease = auth.authHealth().validUntil;
    fake.set({ error: 'fixture transient network failure', errorCode: 'ECONNRESET' });
    assert.strictEqual(await auth.__test.probeIdentity(), false);
    const health = auth.authHealth();
    assert.strictEqual(health.tradingAvailable, true);
    assert.strictEqual(health.validUntil, priorLease, 'failure does not change the success lease');
    assert.ok(priorLease > timers.now());
    assert.strictEqual(health.unavailableSince, null);
    assert.strictEqual(health.probeLastOutcome, 'network_error');
    assert.strictEqual(health.probeFailures, 1);
    assert.strictEqual(health.probeConsecutiveFailures, 1);
    assert.strictEqual(health.probeRetryMs, 250);
    assert.deepStrictEqual(timers.delays(), [250]);
    await timers.advance(249);
    assert.strictEqual(fake.calls(), 2);
    assert.strictEqual(auth.authHealth().probeLastFailureAgeMs, 249);
    fake.set(healthyProbe);
    await timers.advance(1);
    assert.strictEqual(fake.calls(), 3);
    assert.strictEqual(auth.authHealth().tradingAvailable, true);
    assert.strictEqual(auth.authHealth().probeFailures, 1);
    assert.strictEqual(auth.authHealth().probeConsecutiveFailures, 0);
    assert.strictEqual(auth.authHealth().probeRetryMs, 2000);
    assert.strictEqual(auth.authHealth().validUntil, timers.now() + 6000,
      'only the successful retry starts a new six-second lease');
  }));
  await ok('identity diagnostics separate connection stages and return private immutable snapshots', () => withProbeFixture(async ({ timers, fake }) => {
    const socket = new EventEmitter();
    socket.remoteAddress = 'fixture-private-address';
    fake.set({ ...healthyProbe, socket, delayMs: 20,
      transportEvents: [[3, 'lookup'], [7, 'connect'], [11, 'secureConnect']] });
    const probe = auth.startHealthProbe();
    await timers.advance(20);
    assert.strictEqual(await probe, true);
    const expected = { socketMs: 0, dnsMs: 3, tcpMs: 7, tlsMs: 11,
      headersMs: 20, bodyMs: 20, reusedSocket: false, totalMs: 20 };
    assert.deepStrictEqual(auth.authHealth().probeLastTiming, expected);
    const exposed = auth.authHealth().probeLastTiming;
    exposed.tlsMs = 99999;
    assert.deepStrictEqual(auth.authHealth().probeLastTiming, expected);
    for (const name of ['lookup', 'connect', 'secureConnect']) assert.strictEqual(socket.listenerCount(name), 0);
    assert.ok(!JSON.stringify(auth.authHealth()).includes(socket.remoteAddress));
  }));
  await ok('reused identity sockets do not accumulate connection listeners or imply a new TLS handshake', () => withProbeFixture(async ({ fake }) => {
    const socket = new EventEmitter();
    fake.set({ ...healthyProbe, socket, reusedSocket: true });
    for (let i = 0; i < 20; i++) {
      assert.strictEqual(await (i ? auth.__test.probeIdentity() : auth.startHealthProbe()), true);
      const timing = auth.authHealth().probeLastTiming;
      assert.strictEqual(timing.reusedSocket, true);
      assert.strictEqual(timing.tlsMs, null);
      assert.strictEqual(timing.dnsMs, null);
      assert.strictEqual(timing.tcpMs, null);
      for (const name of ['lookup', 'connect', 'secureConnect']) assert.strictEqual(socket.listenerCount(name), 0);
    }
  }));
  await ok('an identity timeout preserves the last completed transport stage and cleans listeners', () => withProbeFixture(async ({ timers, fake, logs }) => {
    const socket = new EventEmitter();
    fake.set({ ...healthyProbe, socket, delayMs: 3000,
      transportEvents: [[2, 'lookup'], [5, 'connect'], [9, 'secureConnect']] });
    const probe = auth.startHealthProbe();
    await timers.advance(1500);
    assert.strictEqual(await probe, false);
    const health = auth.authHealth();
    assert.strictEqual(health.tradingAvailable, false);
    assert.deepStrictEqual(health.probeLastTiming, { socketMs: 0, dnsMs: 2,
      tcpMs: 5, tlsMs: 9, headersMs: null, bodyMs: null,
      reusedSocket: false, totalMs: 1500 });
    assert.strictEqual(health.probeRetryMs, 250);
    assert.match(logs.at(-1), /"headersMs":null/);
    for (const name of ['lookup', 'connect', 'secureConnect']) assert.strictEqual(socket.listenerCount(name), 0);
  }));
  await ok('consecutive failures back off 250/500/1000/2000ms and success resets recovery', () => withProbeFixture(async ({ timers, fake, logs }) => {
    fake.set({ error: 'fixture down', errorCode: 'ECONNREFUSED' });
    assert.strictEqual(await auth.startHealthProbe(), false);
    for (const [index, retryMs] of [250, 500, 1000, 2000, 2000].entries()) {
      const health = auth.authHealth();
      assert.strictEqual(health.tradingAvailable, false);
      assert.strictEqual(health.probeConsecutiveFailures, index + 1);
      assert.strictEqual(health.probeRetryMs, retryMs);
      assert.deepStrictEqual(timers.delays(), [retryMs]);
      const calls = fake.calls();
      await timers.advance(retryMs - 1);
      assert.strictEqual(fake.calls(), calls);
      await timers.advance(1);
      assert.strictEqual(fake.calls(), calls + 1);
    }
    const failureCount = auth.authHealth().probeFailures;
    const unavailableSince = auth.authHealth().unavailableSince;
    fake.set({ ...healthyProbe, delayMs: 23 });
    await timers.advance(2000);
    assert.strictEqual(auth.authHealth().tradingAvailable, false, 'an in-flight retry is not a success');
    assert.strictEqual(auth.authHealth().unavailableSince, unavailableSince);
    await timers.advance(23);
    const restored = auth.authHealth();
    assert.strictEqual(restored.tradingAvailable, true);
    assert.strictEqual(restored.probeLastSuccessDurationMs, 23);
    assert.strictEqual(restored.probeLastFailureAgeMs, 2023);
    assert.strictEqual(restored.probeFailures, failureCount);
    assert.strictEqual(restored.probeConsecutiveFailures, 0);
    assert.strictEqual(restored.probeRetryMs, 2000);
    assert.match(logs.at(-1), /"outcome":"ok"/);
    fake.set({ error: 'fixture second outage', errorCode: 'ECONNRESET' });
    await auth.__test.probeIdentity();
    assert.strictEqual(auth.authHealth().probeConsecutiveFailures, 1);
    assert.strictEqual(auth.authHealth().probeRetryMs, 250);
  }));
  await ok('a hanging identity body times out at 1500ms without extending or revoking its valid lease', () => withProbeFixture(async ({ timers, fake }) => {
    fake.set(healthyProbe);
    await auth.startHealthProbe();
    const priorLease = auth.authHealth().validUntil;
    fake.set({ statusCode: 200, body: '{"ok":', hang: true });
    const probe = auth.__test.probeIdentity();
    await timers.advance(1499);
    assert.strictEqual(auth.authHealth().tradingAvailable, true, 'previous success is still leased while pending');
    assert.strictEqual(auth.__test.probeIdentity(), probe);
    await timers.advance(1);
    assert.strictEqual(await probe, false);
    const failed = auth.authHealth();
    assert.strictEqual(failed.tradingAvailable, true);
    assert.strictEqual(failed.unavailableSince, null);
    assert.strictEqual(failed.validUntil, priorLease);
    assert.strictEqual(failed.probeLastOutcome, 'timeout');
    assert.strictEqual(failed.probeLastDurationMs, 1500);
    assert.strictEqual(failed.probeRetryMs, 250);
    assert.strictEqual(fake.destroyed(), 1);
    fake.set(healthyProbe);
    await timers.advance(250);
    assert.strictEqual(auth.authHealth().tradingAvailable, true);
    assert.strictEqual(auth.authHealth().probeFailures, 1);
  }));
  await ok('sustained transport failure expires at the original deadline and only verified recovery renews it', () => withProbeFixture(async ({ timers, fake }) => {
    fake.set(healthyProbe);
    await auth.startHealthProbe();
    const originalDeadline = auth.authHealth().validUntil;
    fake.set({ error: 'fixture sustained transport failure', errorCode: 'ECONNRESET' });
    await auth.__test.probeIdentity();
    await timers.advance(5999);
    assert.strictEqual(auth.authHealth().tradingAvailable, true);
    assert.strictEqual(auth.authHealth().validUntil, originalDeadline);
    assert.strictEqual(auth.authHealth().unavailableSince, null);
    await timers.advance(1);
    assert.strictEqual(auth.authHealth().tradingAvailable, false);
    assert.strictEqual(auth.authHealth().unavailableSince, originalDeadline);
    await timers.advance(1000);
    await auth.__test.probeIdentity();
    assert.strictEqual(auth.authHealth().validUntil, originalDeadline);
    assert.strictEqual(auth.authHealth().unavailableSince, originalDeadline,
      'a later failed callback cannot move the outage onset');
    fake.set({ statusCode: 503, body: '{"ok":false,"error":"identity_unavailable"}' });
    await auth.__test.probeIdentity();
    assert.strictEqual(auth.authHealth().unavailableSince, originalDeadline,
      'even a definitive denial after expiry preserves the earlier onset');
    fake.set({ ...healthyProbe, delayMs: 23 });
    const recovery = auth.__test.probeIdentity();
    await timers.advance(22);
    assert.strictEqual(auth.authHealth().tradingAvailable, false);
    assert.strictEqual(auth.authHealth().validUntil, originalDeadline);
    await timers.advance(1);
    assert.strictEqual(await recovery, true);
    assert.strictEqual(auth.authHealth().unavailableSince, null);
    assert.strictEqual(auth.authHealth().validUntil, timers.now() + 6000);
  }));
  await ok('an initial transient failure never creates unverified service authority', () => withProbeFixture(async ({ timers, fake }) => {
    const startedAt = timers.now();
    fake.set({ error: 'fixture unavailable at startup', errorCode: 'ECONNREFUSED' });
    assert.strictEqual(await auth.startHealthProbe(), false);
    await timers.advance(6000);
    const health = auth.authHealth();
    assert.strictEqual(health.tradingAvailable, false);
    assert.strictEqual(health.validUntil, null);
    assert.strictEqual(health.probeVerifiedAgeMs, null);
    assert.strictEqual(health.unavailableSince, startedAt);
    fake.set(healthyProbe);
    assert.strictEqual(await auth.__test.probeIdentity(), true);
    assert.strictEqual(auth.authHealth().validUntil, timers.now() + 6000);
  }));
  await ok('only enumerated transport codes and an aborted 200 body retain a verified lease', () => withProbeFixture(async ({ timers, fake }) => {
    fake.set(healthyProbe);
    await auth.startHealthProbe();
    const originalDeadline = auth.authHealth().validUntil;
    for (const errorCode of ['ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE',
      'EAI_AGAIN', 'ENETUNREACH', 'EHOSTUNREACH']) {
      fake.set({ error: 'fixture transport error', errorCode });
      assert.strictEqual(await auth.__test.probeIdentity(), false);
      assert.strictEqual(auth.authHealth().tradingAvailable, true, errorCode);
      assert.strictEqual(auth.authHealth().validUntil, originalDeadline, errorCode);
      assert.strictEqual(auth.authHealth().unavailableSince, null, errorCode);
      assert.strictEqual(auth.authHealth().probeLastOutcome, 'network_error');
    }
    fake.set({ statusCode: 200, body: '{"ok":', aborted: true });
    assert.strictEqual(await auth.__test.probeIdentity(), false);
    assert.strictEqual(auth.authHealth().probeLastOutcome, 'response_aborted');
    assert.strictEqual(auth.authHealth().tradingAvailable, true);
    assert.strictEqual(auth.authHealth().validUntil, originalDeadline);
    assert.strictEqual(auth.authHealth().probeVerifiedAgeMs, 0);
    assert.ok(originalDeadline > timers.now());
  }));
  await ok('every non-200 response revokes at its headers even when its body hangs or aborts', async () => {
    for (const answer of [
      { statusCode: 401, body: '{}' }, { statusCode: 403, hang: true },
      { statusCode: 429, body: '{}' }, { statusCode: 500, body: '{}' },
      { statusCode: 502, body: '{}' }, { statusCode: 503, body: '{}' },
      { statusCode: 503, body: '{"ok":false,"error":"identity_unavailable"}' },
      { statusCode: 503, hang: true }, { statusCode: 503, aborted: true },
      { statusCode: 504, body: '{}' }, { statusCode: 302, hang: true },
    ]) await withProbeFixture(async ({ timers, fake }) => {
      fake.set(healthyProbe);
      await auth.startHealthProbe();
      const originalDeadline = auth.authHealth().validUntil;
      await timers.advance(100);
      const rejectedAt = timers.now();
      fake.set(answer);
      assert.strictEqual(await auth.__test.probeIdentity(), false);
      assert.strictEqual(timers.now(), rejectedAt, 'no body deadline is needed to classify HTTP denial');
      assert.strictEqual(auth.authHealth().tradingAvailable, false, String(answer.statusCode));
      assert.strictEqual(auth.authHealth().unavailableSince, rejectedAt);
      assert.strictEqual(auth.authHealth().validUntil, originalDeadline);
      assert.strictEqual(auth.authHealth().probeLastOutcome, 'http_error');
      assert.deepStrictEqual(timers.delays(), [250], 'denied response deadline is cancelled');
    });
  });
  await ok('invalid responses and TLS/configuration/unknown errors revoke and stay latched through soft failures', async () => {
    for (const answer of [
      { statusCode: 200, body: 'not-json' },
      { statusCode: 200, body: '{"ok":false}' },
      { statusCode: 200, body: '{"ok":true,"user":{"id":1}}' },
      { statusCode: 200, body: 'x'.repeat(1024 * 1024 + 1) },
      { error: 'fixture expired certificate', errorCode: 'CERT_HAS_EXPIRED' },
      { error: 'fixture mismatched certificate', errorCode: 'ERR_TLS_CERT_ALTNAME_INVALID' },
      { error: 'fixture local configuration', errorCode: 'ERR_INVALID_URL' },
      { error: 'fixture unclassified error' },
    ]) await withProbeFixture(async ({ timers, fake }) => {
      fake.set(healthyProbe);
      await auth.startHealthProbe();
      const originalDeadline = auth.authHealth().validUntil;
      await timers.advance(100);
      const rejectedAt = timers.now();
      fake.set(answer);
      assert.strictEqual(await auth.__test.probeIdentity(), false);
      assert.strictEqual(auth.authHealth().tradingAvailable, false);
      assert.strictEqual(auth.authHealth().unavailableSince, rejectedAt);
      assert.strictEqual(auth.authHealth().validUntil, originalDeadline);
      fake.set({ error: 'fixture later network failure', errorCode: 'ECONNRESET' });
      await timers.advance(250);
      assert.strictEqual(auth.authHealth().probeLastOutcome, 'network_error');
      assert.strictEqual(auth.authHealth().tradingAvailable, false);
      assert.strictEqual(auth.authHealth().unavailableSince, rejectedAt,
        'a subsequent soft failure cannot undo a definitive rejection');
      assert.strictEqual(auth.authHealth().validUntil, originalDeadline);
      fake.set(healthyProbe);
      assert.strictEqual(await auth.__test.probeIdentity(), true);
      assert.strictEqual(auth.authHealth().tradingAvailable, true);
      assert.strictEqual(auth.authHealth().unavailableSince, null);
    });
  });
  await ok('service-lease grace does not extend session cache authority or bypass a revoked session', () => withProbeFixture(async ({ timers, fake }) => {
    fake.set(healthyProbe);
    await auth.startHealthProbe();
    const token = 'fixture-session-independent-of-health-lease';
    const user = { id: 746 };
    fake.set({ statusCode: 200, body: JSON.stringify({ ok: true, user }) });
    assert.deepStrictEqual(await auth.validateSession(token), user);
    fake.set(healthyProbe);
    await timers.advance(29_999);
    const originalDeadline = auth.authHealth().validUntil;
    fake.set({ error: 'fixture ordinary transport failure', errorCode: 'ECONNRESET' });
    await auth.__test.probeIdentity();
    assert.strictEqual(auth.authHealth().tradingAvailable, true);
    const callsBeforeCached = fake.calls();
    assert.deepStrictEqual(await auth.validateSession(token), user);
    assert.strictEqual(fake.calls(), callsBeforeCached);
    await timers.advance(1);
    assert.strictEqual(await auth.validateSession(token), null,
      'an expired positive still fails closed during service-lease grace');
    assert.strictEqual(auth.authHealth().tradingAvailable, true);
    assert.strictEqual(auth.authHealth().validUntil, originalDeadline);
    fake.set(healthyProbe);
    assert.strictEqual(await auth.validateSession(token), null, 'upstream revocation remains authoritative');
    const callsBeforeNegative = fake.calls();
    fake.set({ statusCode: 200, body: JSON.stringify({ ok: true, user }) });
    assert.strictEqual(await auth.validateSession(token), null, 'the existing negative cache is retained');
    assert.strictEqual(fake.calls(), callsBeforeNegative);
    assert.strictEqual(auth.authHealth().positiveCacheTtlMs, 30_000);
    assert.strictEqual(auth.authHealth().negativeCacheTtlMs, 5_000);
    assert.strictEqual(auth.authHealth().expiredCacheFallback, false);
  }));
  await ok('probe diagnostics expose only closed categories and numeric timings, never upstream text', () => withProbeFixture(async ({ fake, logs }) => {
    const privateMarker = 'fixture-private-credential-or-location';
    const cases = [
      [{ error: privateMarker }, 'network_error'],
      [{ statusCode: 503, body: privateMarker }, 'http_error'],
      [{ statusCode: 200, body: privateMarker }, 'invalid_json'],
      [{ statusCode: 200, body: JSON.stringify({ ok: false, error: privateMarker }) }, 'invalid_response'],
      [{ statusCode: 200, body: '{}', aborted: true }, 'response_aborted'],
      [{ statusCode: 200, body: privateMarker.repeat(40_000) }, 'response_too_large'],
    ];
    for (const [answer, outcome] of cases) {
      fake.set(answer);
      const result = auth.authHealth().probeEnabled ? auth.__test.probeIdentity() : auth.startHealthProbe();
      assert.strictEqual(await result, false);
      const health = auth.authHealth();
      assert.strictEqual(health.probeLastOutcome, outcome);
      assert.ok(Number.isFinite(health.probeLastDurationMs) && health.probeLastDurationMs >= 0);
      assert.strictEqual(JSON.stringify(health).includes(privateMarker), false);
    }
    assert.strictEqual(logs.length, cases.length);
    for (const line of logs) {
      assert.strictEqual(line.includes(privateMarker), false);
      const value = JSON.parse(line.slice('auth:identity-probe '.length));
      assert.deepStrictEqual(Object.keys(value).sort(), ['consecutiveFailures', 'durationMs', 'outcome', 'retryMs', 'timing']);
      assert.deepStrictEqual(Object.keys(value.timing).sort(), ['bodyMs', 'dnsMs', 'headersMs',
        'reusedSocket', 'socketMs', 'tcpMs', 'tlsMs', 'totalMs']);
      for (const [key, part] of Object.entries(value.timing)) {
        if (key === 'reusedSocket') assert.strictEqual(typeof part, 'boolean');
        else assert.ok(part === null || (Number.isFinite(part) && part >= 0));
      }
      assert.strictEqual(typeof value.outcome, 'string');
      assert.ok(Number.isFinite(value.durationMs));
    }
  }));
  await ok('stopping cancels pending work; an old generation cannot alter or reschedule its replacement', () => withProbeFixture(async ({ timers, fake, logs }) => {
    fake.set({ ...healthyProbe, hang: true });
    const oldProbe = auth.startHealthProbe();
    await timers.flush();
    auth.stopHealthProbe();
    fake.set({ ...healthyProbe, delayMs: 40 });
    const replacement = auth.startHealthProbe();
    assert.strictEqual(await oldProbe, false);
    assert.strictEqual(auth.__test.probeIdentity(), replacement, 'old finally cannot clear new pending work');
    assert.strictEqual(auth.authHealth().probeLastOutcome, null);
    assert.strictEqual(auth.authHealth().probeFailures, 0);
    assert.strictEqual(auth.authHealth().tradingAvailable, false);
    await timers.advance(40);
    assert.strictEqual(await replacement, true);
    assert.strictEqual(auth.authHealth().probeLastSuccessDurationMs, 40);
    assert.strictEqual(auth.authHealth().probeFailures, 0);
    assert.deepStrictEqual(timers.delays(), [2000]);
    assert.strictEqual(logs.length, 0, 'cancelled generations never report an outage/recovery');
    const calls = fake.calls();
    auth.stopHealthProbe();
    assert.deepStrictEqual(timers.delays(), []);
    await timers.advance(10_000);
    assert.strictEqual(fake.calls(), calls);
    assert.strictEqual(auth.authHealth().probeEnabled, false);
    assert.strictEqual(auth.authHealth().probeRetryMs, null);
  }));

  await ok('ordinary gate authority and session cookies retain their contract', () => {
    assert.strictEqual(S.gateMatches('test-only-gate'), true);
    assert.strictEqual(S.gateMatches(undefined), false);
    assert.strictEqual(S.gateMatches(''), false);
    assert.strictEqual(auth.parseSessionCookie({ headers: {} }), null);
    assert.strictEqual(auth.parseSessionCookie({ headers: { cookie: 'other=1; phoenix_session=session%2Dfixture' } }), 'session-fixture');
  });

  await ok('pause alerts cover onset, monotonic age reminders, recovery and later incidents without symbol dedupe', async () => {
    const timers = fakeTimeouts(1_800_000_000_000), messages = [];
    let wall = timers.now();
    let state = { roundId: 'private-BTC-round', paused: true, since: wall - 5000, reasonClass: 'identity' };
    const notifier = paper.__test.__createPauseNotifier({ read: () => state,
      send: async (message) => { messages.push(message); return true; }, wall: () => wall, mono: timers.now });
    try {
      notifier.wake(); assert.strictEqual(messages.length, 0, 'no pre-commit synchronous send');
      await timers.advance(0);
      assert.match(messages[0], /pause began.*incident=1.*cause=identity.*age=5s/);
      wall -= 3_600_000;
      await timers.advance(60_000);
      assert.strictEqual(messages.length, 2);
      assert.match(messages[1], /still paused.*age=65s/);
      state = { ...state, paused: false }; notifier.wake(); await timers.advance(0);
      assert.match(messages[2], /trading resumed.*incident=1.*age=65s/);
      state = { ...state, paused: true, since: wall }; notifier.wake(); await timers.advance(0);
      assert.match(messages[3], /pause began.*incident=2/);
      state = { ...state, roundId: 'other-private-SOL-round' }; notifier.wake(); await timers.advance(0);
      assert.match(messages[4], /round ended while paused/);
      assert.match(messages[5], /pause began.*incident=3/);
      assert.ok(messages.every((message) => !/private|BTC|SOL/.test(message)));
      assert.strictEqual(notifier.status().sent, 6);
    } finally { notifier.stop(); await timers.flush(); assert.deepStrictEqual(timers.delays(), []); timers.restore(); }
  });
  await ok('failed pause delivery retries while state is unknown and never invents recovery', async () => {
    const timers = fakeTimeouts(1_800_000_000_000), messages = [];
    let unreadable = false, failNext = false;
    let state = { roundId: 'fixture-round', paused: true, since: timers.now(), reasonClass: 'feed' };
    const notifier = paper.__test.__createPauseNotifier({ read: () => {
      if (unreadable) throw new Error('fixture read unavailable'); return state;
    }, send: async (message) => { messages.push(message); return messages.length > 1 && !failNext; }, mono: timers.now });
    try {
      notifier.wake(); await timers.advance(0);
      assert.strictEqual(notifier.status().sent, 0);
      assert.strictEqual(notifier.status().failed, 1);
      unreadable = true; state = { ...state, paused: false };
      await timers.advance(14_999); assert.strictEqual(messages.length, 1);
      await timers.advance(1); assert.strictEqual(messages.length, 2);
      assert.strictEqual(notifier.status().sent, 1);
      await timers.advance(60_000);
      assert.ok(messages.every((message) => !message.includes('trading resumed')));
      unreadable = false; state.blocked = true;
      notifier.wake(); await timers.advance(0);
      assert.ok(messages.every((message) => !message.includes('trading resumed')));
      state.blocked = false; notifier.wake(); await timers.advance(0);
      assert.ok(messages.at(-1).includes('trading resumed'));
      state = { ...state, paused: true, since: timers.now() };
      notifier.wake(); await timers.advance(0);
      failNext = true; await timers.advance(60_000);
      const observedReminder = messages.at(-1);
      assert.match(observedReminder, /still paused.*observed pause age=60s/);
      unreadable = true; failNext = false; await timers.advance(15_000);
      assert.strictEqual(messages.at(-1), observedReminder,
        'retrying a known reminder during an unreadable state cannot invent a newer pause age');
    } finally { notifier.stop(); await timers.flush(); timers.restore(); }
  });
  await ok('pause queue stays bounded and stop cancels timer plus the sole pending delivery', async () => {
    const timers = fakeTimeouts(1_800_000_000_000);
    let state = { roundId: 'fixture-queue', paused: true, since: timers.now(), reasonClass: 'risk' };
    let calls = 0, cancelled = 0;
    const notifier = paper.__test.__createPauseNotifier({ read: () => state, mono: timers.now,
      send: (_message, signal) => new Promise((resolve) => {
        calls++; signal.addEventListener('abort', () => { cancelled++; resolve(false); }, { once: true });
      }) });
    try {
      notifier.wake(); await timers.advance(0);
      for (let i = 0; i < 40; i++) {
        state = { ...state, paused: !state.paused }; notifier.wake(); await timers.advance(0);
      }
      assert.strictEqual(calls, 1);
      assert.ok(notifier.status().queued <= 16);
      assert.ok(notifier.status().dropped > 0);
      notifier.stop(); await timers.flush();
      assert.strictEqual(cancelled, 1);
      assert.strictEqual(notifier.status().pending, false);
      assert.deepStrictEqual(timers.delays(), []);
      await timers.advance(120_000); assert.strictEqual(calls, 1);
    } finally { notifier.stop(); await timers.flush(); timers.restore(); }
  });
  await ok('ops sender requires a bounded positive acknowledgement and does not spend success cooldown on failure', () => withProbeFixture(async ({ timers, fake }) => {
    const old = { token: process.env.TG_BOT_TOKEN, chat: process.env.OPS_ALERT_CHAT_ID };
    process.env.TG_BOT_TOKEN = 'fixture-not-a-token'; process.env.OPS_ALERT_CHAT_ID = 'fixture-private-chat';
    try {
      for (const answer of [
        { statusCode: 200, body: '{"ok":false}' }, { statusCode: 503, body: '{"ok":true}' },
        { statusCode: 200, body: 'not-json' }, { statusCode: 200, aborted: true },
        { statusCode: 200, body: 'x'.repeat(16385) },
      ]) {
        fake.set(answer); assert.strictEqual(await paper.__test.__deliverOpsMessage('fixture'), false);
      }
      fake.set({ statusCode: 200, hang: true });
      const timed = paper.__test.__deliverOpsMessage('fixture deadline');
      await timers.advance(7999);
      assert.strictEqual(paper.__test.__opsAlertState('none').requests, 1);
      await timers.advance(1); assert.strictEqual(await timed, false);
      assert.strictEqual(paper.__test.__opsAlertState('none').requests, 0);
      fake.set({ statusCode: 200, body: '{"ok":false}' });
      assert.strictEqual(await paper.__test.__tgOps('fixture-failed-ack', 'fixture'), false);
      assert.strictEqual(paper.__test.__opsAlertState('fixture-failed-ack').acknowledged, false);
      fake.set({ statusCode: 200, body: '{"ok":true}' });
      assert.strictEqual(await paper.__test.__tgOps('fixture-valid-ack', 'fixture'), true);
      assert.strictEqual(paper.__test.__opsAlertState('fixture-valid-ack').acknowledged, true);
      const calls = fake.calls();
      assert.strictEqual(await paper.__test.__tgOps('fixture-valid-ack', 'fixture'), false);
      assert.strictEqual(fake.calls(), calls);
      delete process.env.OPS_ALERT_CHAT_ID;
      assert.strictEqual(await paper.__test.__deliverOpsMessage('no fallback chat'), false);
      assert.strictEqual(fake.calls(), calls);
    } finally {
      for (const [key, value] of [['TG_BOT_TOKEN', old.token], ['OPS_ALERT_CHAT_ID', old.chat]]) {
        if (value === undefined) delete process.env[key]; else process.env[key] = value;
      }
    }
  }));

  await ok('graceful drain flips state even before a listener is active', async () => {
    assert.strictEqual(S.isDraining(), false);
    assert.strictEqual(await S.drain('test', false), true);
    assert.strictEqual(S.isDraining(), true);
    const realReadiness = paper.readiness;
    paper.readiness = () => ({ ok: true, reasons: [], boot: 'test-boot' });
    try {
      const r = await localRequest('/readyz');
      assert.strictEqual(r.code, 503);
      assert.strictEqual(r.body.draining, true);
      assert.ok(r.body.reasons.includes('service is draining'));
    } finally {
      paper.readiness = realReadiness;
    }
  });

  try { auth.db.close(); } catch {}
  fs.rmSync(tmp, { recursive: true, force: true });
  console.log(`\n${pass} service resilience assertions passed`);
  if (process.exitCode) process.exit(process.exitCode);
})().catch((e) => {
  console.error(e && e.stack || e);
  process.exit(1);
});
