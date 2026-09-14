'use strict';
// The slice of auth.js that paper.js consumes, re-backed for the isolated
// paper box. paper_* tables live in a LOCAL SQLite (paper.js creates them on
// this handle); identity stays with phoenix-teams on perp.so and is resolved
// over HTTPS through two secret-gated internal endpoints, with short caches
// so the hot path (order placement) almost never leaves the box.
//
// Guest creation intentionally has no implementation here: nginx keeps
// routing /api/paper/guest to phoenix-teams, which owns users.db and the
// session cookie. The stubs throw so a routing mistake is loud, not silent.
const Database = require('better-sqlite3');
const https = require('https');
const { performance } = require('perf_hooks');

const DB_FILE = process.env.PAPER_DB || '/var/lib/phoenix-paper/paper.db';
const MAIN_BASE = process.env.PAPER_MAIN_BASE || 'https://perp.so';
const SECRET = process.env.PAPER_INTERNAL_SECRET || '';
const SESSION_COOKIE_NAME = 'phoenix_session';

const db = new Database(DB_FILE);
db.pragma('journal_mode = WAL');
db.pragma('journal_size_limit = 67108864');   // 64MB cap: a runaway WAL on a small shared disk fails loudly, not by filling it
db.pragma('busy_timeout = 5000');
db.pragma('foreign_keys = ON');
db.pragma('synchronous = NORMAL');
// paper_* DDL carries REFERENCES users(id); identity lives on the main box,
// so a local id-only stub satisfies the schema. Rows are added as sessions
// resolve. NOTE the prod cascade (guest prune deletes users → paper rows) does
// not reach this box — stale zero-fill accounts are swept by ownSweep below.
db.exec('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY)');
const ensureUserStmt = db.prepare('INSERT OR IGNORE INTO users (id) VALUES (?)');

// token → { user, at }; userId → { pub, isGuest, at }
const sessCache = new Map();
const userCache = new Map();
const SESS_TTL_MS = 30_000;
const SESS_NEG_TTL_MS = 5_000;
const USER_TTL_MS = 300_000;
const AUTH_PROBE_INTERVAL_MS = 2_000;
const AUTH_PROBE_LEASE_MS = 6_000;
let _lastRemoteAuthOkAt = null;
let _lastRemoteAuthErrorAt = null;
const pendingSessions = new Map();
const identityProbe = {
  enabled: false, generation: 0, verifiedAt: null, unavailableSince: null,
  pending: null, timer: null, controller: null,
  failures: 0, consecutiveFailures: 0, lastFailureAt: null,
  lastOutcome: null, lastDurationMs: null, lastSuccessDurationMs: null,
  retryMs: null, lastTiming: null,
};

function mainFailure(outcome, message) {
  return Object.assign(new Error(message), { identityOutcome: outcome });
}

/* Only this closed vocabulary may enter readiness or logs. In particular,
   upstream exception text can contain hosts, paths or credential material. */
function identityFailureOutcome(error) {
  const known = new Set(['timeout', 'http_error', 'invalid_json',
    'response_too_large', 'response_aborted', 'invalid_response']);
  if (known.has(error?.identityOutcome)) return error.identityOutcome;
  return error?.name === 'AbortError' ? 'aborted' : 'network_error';
}

/* This changes only the health probe's existing service lease, never session
   validation. Every HTTP/invalid response and unknown/configuration/TLS error
   remains definitive, including a callback's HTTP 503 database failure. */
function transientProbeFailure(error) {
  if (error?.identityOutcome === 'timeout' || error?.identityOutcome === 'response_aborted') return true;
  if (error?.identityOutcome) return false;
  return ['ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE', 'EAI_AGAIN',
    'ENETUNREACH', 'EHOSTUNREACH'].includes(error?.code);
}

function mainJson(method, path, bodyObj, timeoutMs = 5000, signal, onTiming = null) {
  return new Promise((resolve, reject) => {
    const base = new URL(MAIN_BASE);
    const body = bodyObj ? JSON.stringify(bodyObj) : null;
    const began = performance.now();
    const timing = { socketMs: null, dnsMs: null, tcpMs: null, tlsMs: null,
      headersMs: null, bodyMs: null, reusedSocket: false };
    const elapsed = () => Math.max(0, Math.round(performance.now() - began));
    const timingListeners = [];
    const onceTiming = (emitter, event, callback) => {
      if (!onTiming || !emitter?.once) return;
      emitter.once(event, callback);
      timingListeners.push([emitter, event, callback]);
    };
    let settled = false;
    let deadline;
    let req;
    const finish = (err, value) => {
      if (settled) return;
      settled = true;
      clearTimeout(deadline);
      // A reused TLS socket may outlive thousands of probes. Never retain
      // listeners for connection events that already happened on that socket.
      for (const [emitter, event, callback] of timingListeners) {
        emitter.removeListener?.(event, callback);
      }
      if (onTiming) {
        // Closed numeric/boolean fields only: no address, path, session,
        // upstream error text, response body or socket object escapes here.
        try { onTiming({ ...timing, totalMs: elapsed() }); } catch {}
      }
      if (err) { req?.destroy(); reject(err); } else resolve(value);
    };
    req = https.request({
      host: base.hostname,
      port: base.port || undefined,
      path,
      method,
      timeout: timeoutMs,
      ...(signal ? { signal } : {}),
      headers: {
        'content-type': 'application/json',
        'x-paper-secret': SECRET,
        ...(body ? { 'content-length': Buffer.byteLength(body) } : {}),
      },
    }, (res) => {
      timing.headersMs = elapsed();
      const chunks = [];
      let bytes = 0;
      res.on('data', (c) => {
        if (settled) return;
        bytes += Buffer.byteLength(c);
        if (bytes > 1024 * 1024) {
          finish(mainFailure('response_too_large', 'main response too large')); return;
        }
        chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c));
      });
      res.on('aborted', () => finish(mainFailure('response_aborted', 'main response aborted')));
      res.on('error', (err) => finish(err));
      // A denied/misrouted request is already definitive at its headers;
      // a hanging error body must not disguise it as a transient timeout.
      if (res.statusCode !== 200) {
        finish(mainFailure('http_error', `main ${res.statusCode}`)); return;
      }
      res.on('end', () => {
        if (settled) return;
        timing.bodyMs = elapsed();
        if (res.statusCode !== 200) return finish(mainFailure('http_error', `main ${res.statusCode}`));
        try { finish(null, JSON.parse(Buffer.concat(chunks).toString('utf8'))); }
        catch { finish(mainFailure('invalid_json', 'main invalid JSON')); }
      });
    });
    const timedOut = () => {
      const err = mainFailure('timeout', 'main timeout');
      finish(err);
    };
    deadline = setTimeout(timedOut, timeoutMs);
    onceTiming(req, 'socket', (socket) => {
      timing.socketMs = elapsed();
      timing.reusedSocket = req.reusedSocket === true;
      if (timing.reusedSocket) return;
      onceTiming(socket, 'lookup', () => { timing.dnsMs = elapsed(); });
      onceTiming(socket, 'connect', () => { timing.tcpMs = elapsed(); });
      onceTiming(socket, 'secureConnect', () => { timing.tlsMs = elapsed(); });
    });
    req.on('timeout', timedOut);
    req.on('error', (err) => finish(err));
    if (body) req.write(body);
    req.end();
  });
}

/* A bad/expired contestant cookie is not a service outage. Probe the protected
   identity callback independently, without any person's cookie or side effect.
   Only this short-lived service lease controls the shared competition clock. */
function probeIdentity() {
  if (!identityProbe.enabled) return Promise.resolve(true);
  if (identityProbe.pending) return identityProbe.pending;
  clearTimeout(identityProbe.timer);
  identityProbe.timer = null;
  const generation = identityProbe.generation;
  const controller = new AbortController();
  identityProbe.controller = controller;
  identityProbe.retryMs = null;
  const startedMono = performance.now();
  const durationMs = () => Math.max(0, Math.round(performance.now() - startedMono));
  let probeTiming = null;
  identityProbe.pending = (async () => {
    try {
      // Real session tokens are 64 hex characters. This non-empty sentinel can
      // never name a session, but exercises the identity DB lookup (null would
      // return before touching SQLite and miss a broken session store).
      const r = await mainJson('POST', '/api/internal/paper-auth',
        { token: 'paper-health-probe:not-a-session' }, 1500, controller.signal,
        (timing) => { probeTiming = timing; });
      if (!r || r.ok !== true || r.user !== null) throw mainFailure('invalid_response', 'invalid identity probe');
      if (generation !== identityProbe.generation) return false;
      const recovering = identityProbe.consecutiveFailures > 0;
      identityProbe.verifiedAt = Date.now();
      identityProbe.unavailableSince = null;
      identityProbe.lastOutcome = 'ok';
      identityProbe.lastDurationMs = durationMs();
      identityProbe.lastTiming = probeTiming;
      identityProbe.lastSuccessDurationMs = identityProbe.lastDurationMs;
      identityProbe.consecutiveFailures = 0;
      if (recovering) console.info('auth:identity-probe ' + JSON.stringify({
        outcome: 'ok', durationMs: identityProbe.lastDurationMs,
        timing: identityProbe.lastTiming,
        consecutiveFailures: 0, retryMs: AUTH_PROBE_INTERVAL_MS,
      }));
      return true;
    } catch (error) {
      if (generation === identityProbe.generation) {
        const now = Date.now();
        if (!transientProbeFailure(error) && identityProbe.unavailableSince == null) {
          // Never move an already expired lease's outage onset to this later
          // callback. Only a successful probe may reset verifiedAt or a denial.
          const expires = identityProbe.verifiedAt == null ? now
            : identityProbe.verifiedAt + AUTH_PROBE_LEASE_MS;
          identityProbe.unavailableSince = Math.min(now, expires);
        }
        identityProbe.lastFailureAt = now;
        identityProbe.failures++;
        identityProbe.consecutiveFailures++;
        identityProbe.lastOutcome = identityFailureOutcome(error);
        identityProbe.lastDurationMs = durationMs();
        identityProbe.lastTiming = probeTiming;
        console.warn('auth:identity-probe ' + JSON.stringify({
          outcome: identityProbe.lastOutcome, durationMs: identityProbe.lastDurationMs,
          timing: identityProbe.lastTiming,
          consecutiveFailures: identityProbe.consecutiveFailures,
          retryMs: Math.min(AUTH_PROBE_INTERVAL_MS,
            250 * 2 ** Math.min(3, identityProbe.consecutiveFailures - 1)),
        }));
      }
      return false;
    } finally {
      if (generation === identityProbe.generation) {
        identityProbe.pending = null;
        identityProbe.controller = null;
        if (identityProbe.enabled) {
          /* Transient failure preserves only the remaining verified lease;
             definitive failures revoke it immediately. Neither extends it.
             Prompt single-flight retries retain their bounded backoff. */
          identityProbe.retryMs = identityProbe.consecutiveFailures > 0
            ? Math.min(AUTH_PROBE_INTERVAL_MS,
              250 * 2 ** Math.min(3, identityProbe.consecutiveFailures - 1))
            : AUTH_PROBE_INTERVAL_MS;
          identityProbe.timer = setTimeout(() => {
            identityProbe.timer = null;
            probeIdentity();
          }, identityProbe.retryMs);
          identityProbe.timer.unref();
        }
      }
    }
  })();
  return identityProbe.pending;
}

function startHealthProbe() {
  if (identityProbe.enabled) return identityProbe.pending || Promise.resolve(authHealth().tradingAvailable);
  identityProbe.enabled = true;
  identityProbe.generation++;
  identityProbe.verifiedAt = null;
  identityProbe.unavailableSince = Date.now();
  identityProbe.failures = 0;
  identityProbe.consecutiveFailures = 0;
  identityProbe.lastFailureAt = null;
  identityProbe.lastOutcome = null;
  identityProbe.lastDurationMs = null;
  identityProbe.lastSuccessDurationMs = null;
  identityProbe.lastTiming = null;
  identityProbe.retryMs = null;
  return probeIdentity();
}

function stopHealthProbe() {
  identityProbe.enabled = false;
  identityProbe.generation++;
  clearTimeout(identityProbe.timer);
  identityProbe.timer = null;
  identityProbe.controller?.abort();
  identityProbe.controller = null;
  identityProbe.pending = null;
  identityProbe.retryMs = null;
}

function cacheUser(id, pub, isGuest) {
  userCache.set(Number(id), { pub: pub || null, isGuest: !!isGuest, at: Date.now() });
}

// Sweep both caches so long-gone sessions don't accumulate.
setInterval(() => {
  const now = Date.now();
  for (const [k, v] of sessCache) if (now - v.at > 10 * SESS_TTL_MS) sessCache.delete(k);
  for (const [k, v] of userCache) if (now - v.at > 4 * USER_TTL_MS) userCache.delete(k);
}, 600_000).unref();

/* PRACTICE SEATS.
 *
 * Identity lives on the main box, so a bot account cannot have a real
 * session, and the driver must still be an ordinary CLIENT of the order
 * endpoint or it would need a second copy of every guard. This is the
 * narrowest thing that makes that possible: an in-memory map from a random
 * 32-byte token to a bot user id.
 *
 * It cannot be reached from outside the process. Tokens are generated with
 * crypto.randomBytes, never persisted, never logged, and never leave the
 * driver. Only ids inside the reserved bot band can be minted, so this can
 * never mint a session for a person. And the map is dropped on restart, which
 * is correct: a practice round that outlives its process re-mints. */
const _botSessions = new Map();          // token -> userId
function mintBotSession(userId, inBand) {
  if (!inBand(userId)) throw new Error('refusing to mint a session for a non-bot id');
  for (const [t, u] of _botSessions) if (u === userId) return t;
  const tok = require('crypto').randomBytes(32).toString('hex');
  _botSessions.set(tok, Number(userId));
  try { ensureUserStmt.run(Number(userId)); } catch { /* stub row only */ }
  return tok;
}
function dropBotSessions() { _botSessions.clear(); }

async function validateSession(token) {
  if (!token || typeof token !== 'string') return null;
  /* Checked before the network call, so a practice round keeps running while
     the main box is unreachable, which is exactly when a rehearsal is most
     likely to be happening. */
  if (_botSessions.has(token)) return { id: _botSessions.get(token) };
  const hit = sessCache.get(token);
  const now = Date.now();
  if (hit && now - hit.at < (hit.user ? SESS_TTL_MS : SESS_NEG_TTL_MS)) return hit.user;
  if (pendingSessions.has(token)) return pendingSessions.get(token);
  const pending = (async () => {
  try {
    const r = await mainJson('POST', '/api/internal/paper-auth', { token });
    _lastRemoteAuthOkAt = Date.now();
    const user = r && r.ok && r.user ? r.user : null;
    sessCache.set(token, { user, at: now });
    if (user) {
      cacheUser(user.id, r.pub, r.isGuest);
      try { ensureUserStmt.run(user.id); } catch { /* stub row only */ }
    }
    return user;
  } catch {
    _lastRemoteAuthErrorAt = Date.now();
    /* Authentication is authority, not availability. Once the 30-second
       positive cache has expired, a main-box outage must not turn that old
       answer into an unbounded session: a revoked or expired cookie could
       otherwise keep mutating accounts until the cache sweeper happened to
       delete it. Fresh positives above still absorb ordinary network blips;
       expired entries fail closed and are retried on the next request. */
    return null;
  }
  })();
  pendingSessions.set(token, pending);
  try { return await pending; }
  finally { if (pendingSessions.get(token) === pending) pendingSessions.delete(token); }
}

/* Safe for an ungated readiness response: policy and ages only, never session
   counts, user ids, cookies, upstream locations or secret material. */
function authHealth(now = Date.now()) {
  const age = (at) => at == null ? null : Math.max(0, Math.round(now - at));
  const validUntil = identityProbe.enabled && identityProbe.verifiedAt != null
    ? identityProbe.verifiedAt + AUTH_PROBE_LEASE_MS : null;
  const tradingAvailable = !identityProbe.enabled || (identityProbe.unavailableSince == null
    && validUntil != null && now < validUntil);
  const unavailableSince = tradingAvailable ? null
    : identityProbe.unavailableSince ?? validUntil ?? now;
  const upstream = identityProbe.enabled ? (tradingAvailable ? 'ok' : 'unavailable') : _lastRemoteAuthErrorAt != null
    && (_lastRemoteAuthOkAt == null || _lastRemoteAuthErrorAt > _lastRemoteAuthOkAt)
    ? 'unavailable'
    : _lastRemoteAuthOkAt != null ? 'ok' : 'unknown';
  return {
    configured: !!SECRET,
    upstream,
    lastVerifiedAgeMs: age(_lastRemoteAuthOkAt),
    lastFailureAgeMs: age(_lastRemoteAuthErrorAt),
    positiveCacheTtlMs: SESS_TTL_MS,
    negativeCacheTtlMs: SESS_NEG_TTL_MS,
    expiredCacheFallback: false,
    tradingAvailable,
    unavailableSince,
    validUntil,
    probeEnabled: identityProbe.enabled,
    probeVerifiedAgeMs: age(identityProbe.verifiedAt),
    probeLastOutcome: identityProbe.lastOutcome,
    probeLastDurationMs: identityProbe.lastDurationMs,
    probeLastTiming: identityProbe.lastTiming ? { ...identityProbe.lastTiming } : null,
    probeLastSuccessDurationMs: identityProbe.lastSuccessDurationMs,
    probeLastFailureAgeMs: age(identityProbe.lastFailureAt),
    probeFailures: identityProbe.failures,
    probeConsecutiveFailures: identityProbe.consecutiveFailures,
    probeRetryMs: identityProbe.retryMs,
  };
}

/** Prefetch public identity for a set of user ids (leaderboard names). */
async function warmUsers(ids) {
  const now = Date.now();
  const missing = [...new Set(ids.map(Number))].filter((id) => {
    const c = userCache.get(id);
    return !c || now - c.at > USER_TTL_MS;
  });
  if (missing.length === 0) return;
  try {
    // Chunked so the querystring stays sane on big rosters.
    for (let i = 0; i < missing.length; i += 200) {
      const chunk = missing.slice(i, i + 200);
      const r = await mainJson('GET', `/api/internal/paper-users?ids=${chunk.join(',')}`, null, 8000);
      if (r && r.ok && r.users) {
        for (const id of chunk) {
          const u = r.users[id];
          cacheUser(id, u ? u.pub : null, u ? u.isGuest : false);
        }
      }
    }
  } catch { /* stale names beat a dead leaderboard */ }
}

function parseSessionCookie(req) {
  const raw = req.headers.cookie || '';
  for (const part of raw.split(';')) {
    const [k, ...v] = part.trim().split('=');
    if (k === SESSION_COOKIE_NAME) {
      try { return decodeURIComponent(v.join('=')); }
      catch { return null; }
    }
  }
  return null;
}

// Sync lookups served from the cache warmUsers/validateSession filled.
function isGuestUser(userId) {
  const c = userCache.get(Number(userId));
  return c ? c.isGuest : false;
}
function getUserById(id) {
  const c = userCache.get(Number(id));
  return c && c.pub ? c.pub : null;
}
// publicUser shaping already happened on the main box; pass through.
function publicUser(u) { return u; }

function neverHere(name) {
  return () => { throw new Error(`auth-shim: ${name} must not run on the paper box (guest flow lives on phoenix-teams)`); };
}

module.exports = {
  db,
  validateSession,
  parseSessionCookie,
  isGuestUser,
  getUserById,
  publicUser,
  warmUsers,
  authHealth,
  startHealthProbe, stopHealthProbe,
  __test: { mainJson, probeIdentity },
  createGuestUser: neverHere('createGuestUser'),
  createSession: neverHere('createSession'),
  mintBotSession, dropBotSessions,
  setSessionCookie: neverHere('setSessionCookie'),
  setDisplayName: neverHere('setDisplayName'),
  isDisplayNameTaken: neverHere('isDisplayNameTaken'),
};
