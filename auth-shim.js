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

const DB_FILE = process.env.PAPER_DB || '/opt/phoenix-paper/paper.db';
const MAIN_BASE = process.env.PAPER_MAIN_BASE || 'https://perp.so';
const SECRET = process.env.PAPER_INTERNAL_SECRET || '';
const SESSION_COOKIE_NAME = 'phoenix_session';

const db = new Database(DB_FILE);
db.pragma('journal_mode = WAL');
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

function mainJson(method, path, bodyObj, timeoutMs = 5000) {
  return new Promise((resolve, reject) => {
    const base = new URL(MAIN_BASE);
    const body = bodyObj ? JSON.stringify(bodyObj) : null;
    const req = https.request({
      host: base.hostname,
      path,
      method,
      timeout: timeoutMs,
      headers: {
        'content-type': 'application/json',
        'x-paper-secret': SECRET,
        ...(body ? { 'content-length': Buffer.byteLength(body) } : {}),
      },
    }, (res) => {
      let out = '';
      res.on('data', (c) => { out += c; });
      res.on('end', () => {
        if (res.statusCode !== 200) return reject(new Error(`main ${res.statusCode}`));
        try { resolve(JSON.parse(out)); } catch (e) { reject(e); }
      });
    });
    req.on('timeout', () => req.destroy(new Error('main timeout')));
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
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

async function validateSession(token) {
  if (!token || typeof token !== 'string') return null;
  const hit = sessCache.get(token);
  const now = Date.now();
  if (hit && now - hit.at < (hit.user ? SESS_TTL_MS : SESS_NEG_TTL_MS)) return hit.user;
  try {
    const r = await mainJson('POST', '/api/internal/paper-auth', { token });
    const user = r && r.ok && r.user ? r.user : null;
    sessCache.set(token, { user, at: now });
    if (user) {
      cacheUser(user.id, r.pub, r.isGuest);
      try { ensureUserStmt.run(user.id); } catch { /* stub row only */ }
    }
    return user;
  } catch {
    // Main-box blip: a stale positive beats logging every trader out.
    return hit ? hit.user : null;
  }
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
    if (k === SESSION_COOKIE_NAME) return decodeURIComponent(v.join('='));
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
  createGuestUser: neverHere('createGuestUser'),
  createSession: neverHere('createSession'),
  setSessionCookie: neverHere('setSessionCookie'),
  setDisplayName: neverHere('setDisplayName'),
  isDisplayNameTaken: neverHere('isDisplayNameTaken'),
};
