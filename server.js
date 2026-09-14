'use strict';
// phoenix-paper — the paper trading engine as an isolated service.
//
// This box owns simulation state (paper.db) and the price/print feeds; it
// does NOT own identity. Sessions and guest creation stay with phoenix-teams
// on perp.so (auth-shim.js resolves cookies remotely), which is why nginx
// keeps /api/paper/guest pointed at the main box and everything else under
// /api/paper/ pointed here.
//
// Inputs this service needs to function:
//  - Phoenix WS marks: paper.js subscribes on its own (primary price path).
//  - markets-snapshot.json fallback: fetched from perp.so every 5s into
//    PHOENIX_SNAPSHOT_FILE (atomic rename), same shape paper.js expects.
//  - Real prints + index-tick persistence: monad warehouse through a local
//    ssh -L 9100 tunnel, same as the main box (WAREHOUSE_API_TOKEN).
//  - /exchange config: direct Phoenix REST with a long TTL (one call site).
const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

/* Every response that operators use to decide whether this process is the
   intended release carries the same build id. Derive it before paper.js is
   loaded so that module (and anything it loads) can use PAPER_BUILD_ID too.
   An immutable deployment may stamp its own id; an ordinary source checkout
   gets a deterministic digest of all executable service-local source. */
function sourceBuildId() {
  const stamped = String(process.env.PAPER_BUILD_ID || '').trim();
  if (/^[A-Za-z0-9._-]{1,64}$/.test(stamped)) return stamped;
  const h = crypto.createHash('sha256');
  for (const name of ['server.js', 'paper.js', 'competition.js', 'auth-shim.js', 'bots.js', 'package.json', 'package-lock.json']) {
    h.update(name); h.update('\0'); h.update(fs.readFileSync(path.join(__dirname, name))); h.update('\0');
  }
  return h.digest('hex').slice(0, 16);
}
const BUILD_ID = sourceBuildId();
process.env.PAPER_BUILD_ID = BUILD_ID;
/* systemd's StateDirectory owns durable mutable state. Set both paths before
   loading paper.js/auth-shim.js so every module opens the same files even
   when an EnvironmentFile does not repeat these defaults. */
const STATE_DIR = process.env.PAPER_STATE_DIR || '/var/lib/phoenix-paper';
if (!process.env.PAPER_DB) process.env.PAPER_DB = path.join(STATE_DIR, 'paper.db');
if (!process.env.PHOENIX_SNAPSHOT_FILE) {
  process.env.PHOENIX_SNAPSHOT_FILE = path.join(STATE_DIR, 'markets-snapshot.json');
}
const paper = require('./paper.js');
const auth = require('./auth-shim.js');

/* A throw inside a timer callback (a boundary firing into a full disk, say)
   had no handler: the process died with the reason only on stderr, and
   systemd crash-looped it through boot migrations. Log the cause durably,
   then exit and let systemd restart into the fail-closed resume path. */
process.on('uncaughtException', (e) => {
  try { console.error('[paper] FATAL uncaught:', e && e.stack || e); } catch {}
  process.exit(1);
});
process.on('unhandledRejection', (e) => {
  try { console.error('[paper] FATAL unhandled rejection:', e && e.stack || e); } catch {}
  process.exit(1);
});

const PORT = Number(process.env.PAPER_PORT || 9200);
const WAREHOUSE_TOKEN = process.env.WAREHOUSE_API_TOKEN || '';
const GATE = process.env.PAPER_GATE_SECRET || '';
const gateBytes = Buffer.from(GATE);
function gateMatches(value) {
  if (!gateBytes.length || typeof value !== 'string') return false;
  const candidate = Buffer.from(value);
  return candidate.length === gateBytes.length && crypto.timingSafeEqual(candidate, gateBytes);
}
/* Fail CLOSED. An empty gate used to disable the check entirely, so one
   missing line in the env file silently opened every route to whoever could
   reach the port. A competition engine with no gate must not serve. */
const SNAPSHOT_FILE = process.env.PHOENIX_SNAPSHOT_FILE;
const SNAPSHOT_SRC = process.env.PAPER_SNAPSHOT_SRC || 'https://perp.so/markets-snapshot.json';
const API_BASE = 'https://perp-api.phoenix.trade';

function log(...a) { console.log(new Date().toISOString(), ...a); }

/* Bound the entire response, not just socket inactivity. A peer that trickles
   bytes or disconnects after headers must release the request and its buffer. */
function remoteText(transport, url, options, timeoutMs, maxBytes) {
  return new Promise((resolve, reject) => {
    let finished = false, deadline, req;
    const done = (err, value) => {
      if (finished) return;
      finished = true;
      clearTimeout(deadline);
      if (err) { reject(err); req?.destroy(err); }
      else resolve(value);
    };
    req = transport.request(url, { ...options, timeout: timeoutMs }, (res) => {
      const chunks = [];
      let bytes = 0;
      res.on('data', (chunk) => {
        if (finished) return;
        bytes += Buffer.byteLength(chunk);
        if (bytes > maxBytes) return done(new Error('upstream response too large'));
        chunks.push(Buffer.from(chunk));
      });
      res.on('aborted', () => done(new Error('upstream response aborted')));
      res.on('error', (err) => done(err));
      res.on('end', () => {
        if (finished) return;
        if (res.statusCode !== 200) return done(new Error(`upstream HTTP ${res.statusCode}`));
        done(null, Buffer.concat(chunks, bytes).toString('utf8'));
      });
    });
    deadline = setTimeout(() => done(new Error('upstream deadline exceeded')), timeoutMs);
    req.on('timeout', () => done(new Error('upstream timeout')));
    req.on('error', (err) => done(err));
    req.end();
  });
}

// ── Phoenix REST with a small TTL cache (the engine only asks for /exchange,
// which is near-static config) ──────────────────────────────────────────────
const apiCache = new Map();
const apiPending = new Map();
function apiGet(pathname) {
  const hit = apiCache.get(pathname);
  if (hit && Date.now() < hit.expires) return Promise.resolve(hit.data);
  if (apiPending.has(pathname)) return apiPending.get(pathname);
  const pending = remoteText(https, API_BASE + pathname, { method: 'GET' }, 10_000, 2 * 1024 * 1024)
    .then((body) => {
      const data = JSON.parse(body);
      apiCache.set(pathname, { data, expires: Date.now() + 2 * 3600 * 1000 });
      return data;
    }).finally(() => apiPending.delete(pathname));
  apiPending.set(pathname, pending);
  return pending;
}

// ── Warehouse getter: byte-for-byte the main box semantics (raw string body,
// null on any failure, per-endpoint breaker so a dead tunnel costs 10s not
// a timeout per call) ───────────────────────────────────────────────────────
const whBreakers = new Map();
const whPending = new Map();
function warehouseGet(pathname, timeoutMs) {
  if (!WAREHOUSE_TOKEN) return Promise.resolve(null);
  if (whPending.has(pathname)) return whPending.get(pathname);
  const key = String(pathname).split('?')[0].split('/').filter(Boolean).slice(0, 2).join('/');
  let br = whBreakers.get(key);
  if (!br) { br = { fails: 0, skipUntil: 0 }; whBreakers.set(key, br); }
  if (Date.now() < br.skipUntil) return Promise.resolve(null);
  const pending = remoteText(http, 'http://127.0.0.1:9100' + pathname,
    { method: 'GET', headers: { authorization: 'Bearer ' + WAREHOUSE_TOKEN } },
    timeoutMs || 3000, 8 * 1024 * 1024)
    .then((body) => { br.fails = 0; return body; }, () => {
      if (++br.fails >= 3) { br.skipUntil = Date.now() + 10_000; br.fails = 0; }
      return null;
    }).finally(() => whPending.delete(pathname));
  whPending.set(pathname, pending);
  return pending;
}

// ── Per-IP read limiter, same policy as the main box ────────────────────────
const READ_RATE_MAX = Number(process.env.PAPER_READ_RATE_MAX || 300);
const readRate = new Map();
function readRateOk(ip) {
  if (ip === '127.0.0.1' || ip === '::1') return true;
  const now = Date.now();
  const e = readRate.get(ip) || { count: 0, windowStart: now };
  if (now - e.windowStart > 60_000) { e.count = 0; e.windowStart = now; }
  e.count += 1;
  readRate.set(ip, e);
  return e.count <= READ_RATE_MAX;
}
setInterval(() => {
  const now = Date.now();
  for (const [k, v] of readRate) if (now - v.windowStart > 120_000) readRate.delete(k);
}, 300_000).unref();

// ── Snapshot fallback sync: perp.so → local file, validated + atomic ────────
const envPosInt = (name, fallback) => {
  const n = Number(process.env[name]);
  return Number.isSafeInteger(n) && n > 0 ? n : fallback;
};
const SNAPSHOT_MAX_AGE_MS = envPosInt('PAPER_SNAPSHOT_MAX_AGE_MS', 60_000);
const SNAPSHOT_FUTURE_SKEW_MS = envPosInt('PAPER_SNAPSHOT_FUTURE_SKEW_MS', 30_000);
const SNAPSHOT_MIN_FRESH_MARKETS = envPosInt('PAPER_SNAPSHOT_MIN_FRESH_MARKETS', 10);
const SNAPSHOT_MAX_BYTES = envPosInt('PAPER_SNAPSHOT_MAX_BYTES', 2 * 1024 * 1024);
fs.mkdirSync(path.dirname(SNAPSHOT_FILE), { recursive: true });

let _snapFails = 0;
let _snapInFlight = false;
let _snapshotState = {
  lastSuccessAt: null,
  snapshotUpdatedAt: null,
  freshMarkets: 0,
  totalMarkets: 0,
  lastError: 'not_loaded',
};

function snapshotValidationError(code) {
  const e = new Error(code);
  e.code = code;
  return e;
}

/* A 200 response is not evidence of a price snapshot. In particular, nginx's
   SPA fallback also returns 200 and used to be atomically installed here as
   HTML. Validate the document and its price-time semantics before it is ever
   allowed to replace the last-known-good file. */
function validateSnapshotDocument(body, now = Date.now()) {
  if (typeof body !== 'string' || Buffer.byteLength(body) > SNAPSHOT_MAX_BYTES) {
    throw snapshotValidationError('too_large');
  }
  let data;
  try { data = JSON.parse(body); } catch { throw snapshotValidationError('invalid_json'); }
  if (!data || typeof data !== 'object' || Array.isArray(data)
      || typeof data.updatedAt !== 'number' || !Number.isFinite(data.updatedAt)
      || !data.markets || typeof data.markets !== 'object' || Array.isArray(data.markets)) {
    throw snapshotValidationError('invalid_schema');
  }
  const sourceAgeMs = now - data.updatedAt;
  if (sourceAgeMs < -SNAPSHOT_FUTURE_SKEW_MS) throw snapshotValidationError('future_timestamp');
  if (sourceAgeMs > SNAPSHOT_MAX_AGE_MS) throw snapshotValidationError('stale_snapshot');

  const rows = Object.entries(data.markets);
  if (rows.length < SNAPSHOT_MIN_FRESH_MARKETS) throw snapshotValidationError('too_few_markets');
  let freshMarkets = 0;
  for (const [symbol, market] of rows) {
    if (!/^[A-Z0-9_.-]{1,32}$/.test(symbol) || !market || typeof market !== 'object'
        || Array.isArray(market) || market.symbol !== symbol) {
      throw snapshotValidationError('invalid_market_schema');
    }
    const markPrice = market.markPrice;
    const updatedAt = market.lastUpdatedMs;
    if (typeof markPrice === 'number' && Number.isFinite(markPrice) && markPrice > 0
        && typeof updatedAt === 'number' && Number.isFinite(updatedAt)
        && now - updatedAt <= SNAPSHOT_MAX_AGE_MS
        && now - updatedAt >= -SNAPSHOT_FUTURE_SKEW_MS) freshMarkets++;
  }
  if (freshMarkets < SNAPSHOT_MIN_FRESH_MARKETS) {
    throw snapshotValidationError('too_few_fresh_markets');
  }
  return { data, updatedAt: data.updatedAt, freshMarkets, totalMarkets: rows.length };
}

function jsonContentType(value) {
  return /^(application\/json|application\/[a-z0-9!#$&^_.+-]+\+json)(?:\s*;|$)/i.test(String(value || '').trim());
}

function noteSnapshotFailure(code, detail) {
  _snapFails++;
  _snapshotState.lastError = code;
  if (_snapFails === 5 || _snapFails % 60 === 0) {
    log(`snapshot sync failing (${code}${detail ? `: ${detail}` : ''})`);
  }
}

function installSnapshot(checked, now = Date.now()) {
  const tmp = `${SNAPSHOT_FILE}.tmp.${process.pid}`;
  try {
    /* Re-serialize the parsed value: only the document that passed validation,
       not unchecked wire bytes, reaches the engine. rename(2) keeps readers
       on either the old complete file or the new complete file. */
    fs.writeFileSync(tmp, JSON.stringify(checked.data), { encoding: 'utf8', mode: 0o644 });
    fs.renameSync(tmp, SNAPSHOT_FILE);
    _snapFails = 0;
    _snapshotState = {
      lastSuccessAt: now,
      snapshotUpdatedAt: checked.updatedAt,
      freshMarkets: checked.freshMarkets,
      totalMarkets: checked.totalMarkets,
      lastError: null,
    };
    return true;
  } catch (e) {
    try { fs.unlinkSync(tmp); } catch {}
    noteSnapshotFailure('write_failed', e.message);
    return false;
  }
}

function snapshotHealth(now = Date.now()) {
  const sourceAgeMs = _snapshotState.snapshotUpdatedAt == null
    ? null : Math.max(0, Math.round(now - _snapshotState.snapshotUpdatedAt));
  const lastSuccessAgeMs = _snapshotState.lastSuccessAt == null
    ? null : Math.max(0, Math.round(now - _snapshotState.lastSuccessAt));
  const ok = sourceAgeMs != null && sourceAgeMs <= SNAPSHOT_MAX_AGE_MS
    && _snapshotState.freshMarkets >= SNAPSHOT_MIN_FRESH_MARKETS;
  return {
    ok,
    sourceAgeMs,
    lastSuccessAgeMs,
    freshMarkets: _snapshotState.freshMarkets,
    totalMarkets: _snapshotState.totalMarkets,
    consecutiveFailures: _snapFails,
    lastError: _snapshotState.lastError,
  };
}

/* Seed health from a valid cache after a service restart. A stale or malformed
   file is left in place until a valid replacement arrives, but it is never
   represented as healthy. */
function inspectExistingSnapshot(now = Date.now()) {
  try {
    const stat = fs.statSync(SNAPSHOT_FILE);
    if (stat.size > SNAPSHOT_MAX_BYTES) throw snapshotValidationError('too_large');
    const checked = validateSnapshotDocument(fs.readFileSync(SNAPSHOT_FILE, 'utf8'), now);
    _snapshotState = {
      lastSuccessAt: stat.mtimeMs,
      snapshotUpdatedAt: checked.updatedAt,
      freshMarkets: checked.freshMarkets,
      totalMarkets: checked.totalMarkets,
      lastError: null,
    };
  } catch (e) {
    _snapshotState.lastError = e && e.code && e.code !== 'ENOENT' ? e.code : 'not_loaded';
  }
}
inspectExistingSnapshot();

function syncSnapshot() {
  if (_snapInFlight) return;
  _snapInFlight = true;
  let settled = false;
  let timedOut = false;
  const fail = (code, detail) => {
    if (settled) return;
    settled = true;
    _snapInFlight = false;
    noteSnapshotFailure(code, detail);
  };
  let req;
  try {
    req = https.get(SNAPSHOT_SRC, {
      timeout: 4000,
      headers: { accept: 'application/json', 'user-agent': 'phoenix-paper-snapshot/1' },
    }, (res) => {
      let body = '';
      let bytes = 0;
      res.on('data', (chunk) => {
        if (settled) return;
        bytes += chunk.length;
        if (bytes > SNAPSHOT_MAX_BYTES) {
          fail('too_large');
          res.destroy();
          return;
        }
        body += chunk;
      });
      res.on('aborted', () => fail('aborted'));
      res.on('error', (e) => fail('network', e.message));
      res.on('end', () => {
        if (settled) return;
        if (res.statusCode !== 200) return fail('http_status', String(res.statusCode || 0));
        if (!jsonContentType(res.headers['content-type'])) return fail('content_type');
        let checked;
        try { checked = validateSnapshotDocument(body); }
        catch (e) { return fail(e.code || 'invalid_snapshot'); }
        settled = true;
        _snapInFlight = false;
        installSnapshot(checked);
      });
    });
  } catch (e) {
    fail('bad_source', e.message);
    return;
  }
  req.on('timeout', () => {
    timedOut = true;
    fail('timeout');
    req.destroy();
  });
  req.on('error', (e) => fail(timedOut ? 'timeout' : 'network', e.message));
}

// ── HTTP surface ────────────────────────────────────────────────────────────
function send(res, code, obj) { res.writeHead(code, { 'content-type': 'application/json' }); res.end(JSON.stringify(obj)); }

let _draining = false;
const _connections = new Set();
const server = http.createServer(async (req, res) => {
  /* HEAD must have the same status and headers as GET but no entity body.
     Monitoring, caches and CDNs routinely probe this way; treating it as an
     unknown method made healthy public API resources answer 404. Normalize
     once here so every current and future GET route inherits correct HTTP
     semantics without duplicating method branches in paper.js. */
  if (req.method === 'HEAD') {
    /* An SSE GET intentionally never ends. Do not turn a finite HEAD probe
       into a live event-stream subscription; it receives the ordinary
       method-not-found status below, still without an entity body. */
    let headPath = '';
    try { headPath = new URL(req.url, 'http://x').pathname; } catch {}
    if (headPath !== '/api/paper/pyth-stream') req.method = 'GET';
    const end = res.end.bind(res);
    res.end = (...args) => {
      const callback = args.find((arg) => typeof arg === 'function');
      return callback ? end(callback) : end();
    };
  }
  let u;
  try { u = new URL(req.url, 'http://x'); } catch { return send(res, 400, { ok: false }); }
  /* Every finite JSON API response, including auth failures and 404s, gets
     the same privacy/cache baseline. Pre-setting preserves the content type
     and any route-specific headers paper.js supplies in writeHead(). SSE has
     its own streaming cache contract and is deliberately left untouched. */
  if (u.pathname.startsWith('/api/paper/') && u.pathname !== '/api/paper/pyth-stream') {
    res.setHeader('cache-control', 'no-store');
    res.setHeader('referrer-policy', 'no-referrer');
    res.setHeader('x-content-type-options', 'nosniff');
  }
  try {
    if (u.pathname === '/healthz') {
      let boot = null;
      try { boot = paper.readiness().boot || null; } catch {}
      return send(res, 200, {
        ok: true,
        up: process.uptime(),
        build: BUILD_ID,
        boot,
        maintenance: paper.deploymentMaintenanceActive(),
        draining: _draining,
        fallback: snapshotHealth(),
        auth: auth.authHealth(),
      });
    }
    /* Readiness, not liveness: 200 only when this engine can actually price a
       market, on a primary pool that meets its minimum, having heard from the
       feed recently. Ungated on purpose, like /healthz, and it carries no
       secret: an orchestrator has to be able to ask. */
    if (u.pathname === '/readyz') {
      const r = paper.readiness();
      const fallback = snapshotHealth();
      const identity = auth.authHealth();
      const reasons = Array.isArray(r.reasons) ? [...r.reasons] : [];
      if (!fallback.ok) reasons.push('snapshot fallback unavailable');
      if (!identity.configured || identity.tradingAvailable === false) reasons.push('identity service unavailable');
      if (_draining) reasons.push('service is draining');
      const ready = !!r.ok && fallback.ok && identity.configured
        && identity.tradingAvailable !== false && !_draining;
      return send(res, ready ? 200 : 503, {
        ...r,
        ok: ready,
        reasons,
        build: BUILD_ID,
        maintenance: paper.deploymentMaintenanceActive(),
        fallback,
        auth: identity,
        draining: _draining,
      });
    }
    if (_draining) return send(res, 503, { ok: false, error: 'service_draining' });
    // nginx stamps the gate header; direct internet hits (should the port ever
    // be exposed) carry nothing and stop here.
    if (!gateMatches(req.headers['x-paper-gate'])) return send(res, 403, { ok: false, error: 'forbidden' });

    /* During an immutable release transaction, only the two finite public
       documents needed to certify the candidate are served. Account GET is
       intentionally not called "read-only": it may create/settle account
       state. Blocking every other paper route also prevents an acknowledged
       write from being erased if candidate verification rolls the DB back. */
    if (paper.deploymentMaintenanceActive()) {
      const deployProbe = req.method === 'GET' && (
        u.pathname === '/api/paper/config' || u.pathname === '/api/paper/time'
        || u.pathname === '/api/paper/comp/state'
        || u.pathname === '/api/paper/comp/baseline'
      );
      if (!deployProbe) return send(res, 503, {
        ok: false, error: 'maintenance', maintenance: true, retryable: true,
      });
    }

    if (req.method === 'GET'  && u.pathname === '/api/paper/account')      return await paper.account(req, res);
    if (req.method === 'POST' && u.pathname === '/api/paper/order')        return await paper.placeOrder(req, res);
    if (req.method === 'POST' && u.pathname === '/api/paper/cancel')       return await paper.cancelOrder(req, res);
    if (req.method === 'POST' && u.pathname === '/api/paper/close')        return await paper.closePosition(req, res);
    if (req.method === 'POST' && u.pathname === '/api/paper/sltp')         return await paper.setSltp(req, res);
    if (req.method === 'POST' && u.pathname === '/api/paper/margin')       return await paper.adjustMargin(req, res);
    if (req.method === 'POST' && u.pathname === '/api/paper/reset')        return await paper.reset(req, res);
    if (req.method === 'GET'  && u.pathname === '/api/paper/fills')        return await paper.fills(req, res, u);
    if (req.method === 'GET'  && u.pathname === '/api/paper/orders')       return await paper.ordersHistory(req, res, u);
    if (req.method === 'GET'  && u.pathname === '/api/paper/leaderboard')  return await paper.leaderboard(req, res);
    if (req.method === 'GET'  && u.pathname === '/api/paper/config')       return paper.engineConfig(req, res);
    if (req.method === 'GET'  && u.pathname === '/api/paper/time')         return paper.engineTime(req, res);
    /* AWAITED, like every other async handler.
     *
     * Without it a rejection inside stageCandles escapes this handler's
     * try/catch entirely and becomes an unhandled rejection, which takes the
     * process down. That is not hypothetical: on 2026-09-04 a bad property
     * read in this exact function crash-looped the engine nine times at six
     * second intervals, and it presented as a network fault to every client
     * because every request and the relay died with it. The specific bug was
     * fixed; this is the reason it was fatal rather than a 500. */
    if (req.method === 'GET'  && u.pathname === '/api/paper/candles')      return await paper.stageCandles(req, res);
    if (req.method === 'GET'  && u.pathname === '/api/paper/tape')         return paper.marketTape(req, res, u);
    if (req.method === 'GET'  && u.pathname === '/api/paper/pyth-history') return await paper.pythHistory(req, res, u);
    if (req.method === 'GET'  && u.pathname === '/api/paper/pyth-stream')  return paper.pythStream(req, res);
    // competition: state and draw-verification are public (the wall and the
    // room read them); every mutation goes through compAdmin's own token.
    if (req.method === 'GET'  && u.pathname === '/api/paper/comp/state')    return paper.compState(req, res);
    if (req.method === 'GET'  && u.pathname === '/api/paper/comp/baseline') return paper.compBaseline(req, res);
    if (req.method === 'GET'  && u.pathname === '/api/paper/comp/verify')   return paper.compVerify(req, res, u);
    if (req.method === 'GET'  && u.pathname === '/api/paper/comp/readiness') return paper.compReadiness(req, res);
    if (req.method === 'GET'  && u.pathname === '/api/paper/comp/me')       return await paper.compMe(req, res);
    if (req.method === 'POST' && u.pathname === '/api/paper/comp/me/ready')  return await paper.compMeReady(req, res);
    /* The player's side. Session-authenticated, not operator-token gated. */
    if (req.method === 'GET'  && u.pathname === '/api/paper/comp/invite')   return await paper.compInvite(req, res, u);
    if (req.method === 'POST' && u.pathname === '/api/paper/comp/invite')   return await paper.compInviteAct(req, res);
    if (req.method === 'POST' && u.pathname === '/api/paper/comp/admin')    return await paper.compAdmin(req, res);
    // /api/paper/guest lives on phoenix-teams (it writes users.db + cookie).
    return send(res, 404, { ok: false, error: 'not_found' });
  } catch (e) {
    log(`handler error ${u.pathname}: ${e.message}`);
    try { send(res, 500, { ok: false, error: 'internal' }); } catch { /* headers gone */ }
  }
});
/* nginx retains idle upstream connections for 60s. Node's 5s default closed
   them first, so nginx occasionally reused a dead socket and surfaced a 502
   before its backup tunnel could recover. Keep the origin alive slightly
   longer than the proxy pool, with the header deadline longer again. */
const UPSTREAM_KEEPALIVE_MS = envPosInt('PAPER_KEEPALIVE_TIMEOUT_MS', 65_000);
server.keepAliveTimeout = UPSTREAM_KEEPALIVE_MS;
server.headersTimeout = Math.max(
  envPosInt('PAPER_HEADERS_TIMEOUT_MS', 66_000),
  UPSTREAM_KEEPALIVE_MS + 1_000,
);
server.on('connection', (socket) => {
  _connections.add(socket);
  socket.on('close', () => _connections.delete(socket));
});

const DRAIN_TIMEOUT_MS = envPosInt('PAPER_DRAIN_TIMEOUT_MS', 10_000);
let _started = false;
let _snapshotTimer = null;
let _sweepTimer = null;
let _firstSweepTimer = null;
let _drainPromise = null;

/* systemd stop is a handoff, not a crash. Flip readiness first so nginx and
   operators stop choosing this process, stop accepting new connections, and
   give in-flight requests a bounded window to finish. Long-lived SSE/WS
   sockets are included in _connections and are terminated at the deadline. */
function drain(signal = 'shutdown', exitProcess = false) {
  if (_drainPromise) {
    if (exitProcess) {
      for (const socket of _connections) socket.destroy();
      process.exit(1);
    }
    return _drainPromise;
  }
  _draining = true;
  auth.stopHealthProbe();
  paper.stopSourceExpiry();
  paper.stopOpsNotifications();
  if (_snapshotTimer) clearInterval(_snapshotTimer);
  if (_sweepTimer) clearInterval(_sweepTimer);
  if (_firstSweepTimer) clearTimeout(_firstSweepTimer);
  log(`draining (${signal}); ${_connections.size} connection(s)`);
  _drainPromise = new Promise((resolve) => {
    let finished = false;
    const finish = async (clean) => {
      if (finished) return;
      finished = true;
      clearTimeout(forceTimer);
      // A graceful HTTP handoff must also seal the final history tail. The
      // separate outbox retains every unacknowledged batch after the bounded
      // final send; provider/network failure cannot hold shutdown indefinitely.
      try { await paper.drainTickPersistence(1500); }
      catch { log('index history shutdown seal unavailable'); }
      resolve(clean);
      if (exitProcess) process.exit(clean ? 0 : 1);
    };
    const forceTimer = setTimeout(() => {
      for (const socket of _connections) socket.destroy();
      finish(false);
    }, DRAIN_TIMEOUT_MS);
    try {
      server.close(() => finish(true));
      if (typeof server.closeIdleConnections === 'function') server.closeIdleConnections();
    } catch (e) {
      if (e && e.code === 'ERR_SERVER_NOT_RUNNING') finish(true);
      else finish(false);
    }
  });
  return _drainPromise;
}

function start() {
  if (_started) return server;
  if (!GATE) throw new Error('PAPER_GATE_SECRET is not set; refusing to start');
  if (!auth.authHealth().configured) throw new Error('PAPER_INTERNAL_SECRET is not set; refusing to start');
  _started = true;
  auth.startHealthProbe();
  syncSnapshot();
  _snapshotTimer = setInterval(syncSnapshot, 5_000);
  paper.init({ apiGet, warehouseGet, log, readRateOk });
  _sweepTimer = setInterval(() => { try { paper.sweep(); } catch (e) { log(`paper-sweep error: ${e.message}`); } }, 5_000);
  _firstSweepTimer = setTimeout(() => { try { paper.sweep(); } catch (e) { log(`paper-sweep error: ${e.message}`); } }, 15_000);
  paper.attachIndexWs(server);
  process.once('SIGTERM', () => { drain('SIGTERM', true); });
  process.once('SIGINT', () => { drain('SIGINT', true); });

  /* Loopback ONLY. The default listen() binds 0.0.0.0, which put the engine on
     every interface and left isolation to the cloud firewall plus a gate header
     that fails open when its env var is missing. The ssh tunnel from the web box
     connects to localhost here, so nothing legitimate needs a wider bind. */
  const host = process.env.PAPER_BIND || '127.0.0.1';
  server.listen(PORT, host, () => log(`phoenix-paper listening on ${host}:${PORT} build ${BUILD_ID}`));
  return server;
}

if (require.main === module) {
  try { start(); }
  catch (e) { console.error(`[paper] ${e.message}`); process.exit(1); }
}

module.exports = {
  start,
  server,
  __test: {
    gateMatches,
    BUILD_ID,
    SNAPSHOT_SRC,
    validateSnapshotDocument,
    jsonContentType,
    installSnapshot,
    inspectExistingSnapshot,
    snapshotHealth,
    syncSnapshot,
    remoteText,
    drain,
    isDraining: () => _draining,
  },
};
