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
const paper = require('./paper.js');

const PORT = Number(process.env.PAPER_PORT || 9200);
const WAREHOUSE_TOKEN = process.env.WAREHOUSE_API_TOKEN || '';
const GATE = process.env.PAPER_GATE_SECRET || '';
const SNAPSHOT_FILE = process.env.PHOENIX_SNAPSHOT_FILE || '/opt/phoenix-paper/data/markets-snapshot.json';
const SNAPSHOT_SRC = process.env.PAPER_SNAPSHOT_SRC || 'https://perp.so/data/markets-snapshot.json';
const API_BASE = 'https://perp-api.phoenix.trade';

function log(...a) { console.log(new Date().toISOString(), ...a); }

// ── Phoenix REST with a small TTL cache (the engine only asks for /exchange,
// which is near-static config) ──────────────────────────────────────────────
const apiCache = new Map();
function apiGet(pathname) {
  const hit = apiCache.get(pathname);
  if (hit && Date.now() < hit.expires) return Promise.resolve(hit.data);
  return new Promise((resolve, reject) => {
    const req = https.get(API_BASE + pathname, { timeout: 10_000 }, (res) => {
      let body = '';
      res.on('data', (c) => { body += c; });
      res.on('end', () => {
        if (res.statusCode !== 200) return reject(new Error(`phoenix ${res.statusCode} ${pathname}`));
        try {
          const data = JSON.parse(body);
          apiCache.set(pathname, { data, expires: Date.now() + 2 * 3600 * 1000 });
          resolve(data);
        } catch (e) { reject(e); }
      });
    });
    req.on('timeout', () => req.destroy(new Error('phoenix timeout')));
    req.on('error', reject);
  });
}

// ── Warehouse getter: byte-for-byte the main box semantics (raw string body,
// null on any failure, per-endpoint breaker so a dead tunnel costs 10s not
// a timeout per call) ───────────────────────────────────────────────────────
const whBreakers = new Map();
function warehouseGet(pathname, timeoutMs) {
  if (!WAREHOUSE_TOKEN) return Promise.resolve(null);
  const key = String(pathname).split('?')[0].split('/').filter(Boolean).slice(0, 2).join('/');
  let br = whBreakers.get(key);
  if (!br) { br = { fails: 0, skipUntil: 0 }; whBreakers.set(key, br); }
  if (Date.now() < br.skipUntil) return Promise.resolve(null);
  return new Promise((resolve) => {
    const done = (val) => {
      if (val == null) { if (++br.fails >= 3) { br.skipUntil = Date.now() + 10_000; br.fails = 0; } }
      else { br.fails = 0; }
      resolve(val);
    };
    const r = http.get('http://127.0.0.1:9100' + pathname, { timeout: timeoutMs || 3000, headers: { authorization: 'Bearer ' + WAREHOUSE_TOKEN } }, (resp) => {
      let b = ''; resp.on('data', (c) => (b += c)); resp.on('end', () => done(resp.statusCode === 200 ? b : null));
    });
    r.on('error', () => done(null));
    r.on('timeout', () => { r.destroy(); done(null); });
  });
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

// ── Snapshot fallback sync: perp.so → local file, atomic ────────────────────
fs.mkdirSync(path.dirname(SNAPSHOT_FILE), { recursive: true });
let _snapFails = 0;
function syncSnapshot() {
  const req = https.get(SNAPSHOT_SRC, { timeout: 4000 }, (res) => {
    let body = '';
    res.on('data', (c) => { body += c; });
    res.on('end', () => {
      if (res.statusCode !== 200 || body.length < 50) { if (++_snapFails === 5) log(`snapshot sync failing (http ${res.statusCode})`); return; }
      _snapFails = 0;
      try {
        fs.writeFileSync(SNAPSHOT_FILE + '.tmp', body);
        fs.renameSync(SNAPSHOT_FILE + '.tmp', SNAPSHOT_FILE);
      } catch (e) { log(`snapshot write failed: ${e.message}`); }
    });
  });
  req.on('timeout', () => req.destroy());
  req.on('error', () => { if (++_snapFails === 5) log('snapshot sync failing (network)'); });
}
syncSnapshot();
setInterval(syncSnapshot, 5_000);

// ── HTTP surface ────────────────────────────────────────────────────────────
function send(res, code, obj) { res.writeHead(code, { 'content-type': 'application/json' }); res.end(JSON.stringify(obj)); }

const server = http.createServer(async (req, res) => {
  let u;
  try { u = new URL(req.url, 'http://x'); } catch { return send(res, 400, { ok: false }); }
  try {
    if (u.pathname === '/healthz') return send(res, 200, { ok: true, up: process.uptime() });
    // nginx stamps the gate header; direct internet hits (should the port ever
    // be exposed) carry nothing and stop here.
    if (GATE && (req.headers['x-paper-gate'] || '') !== GATE) return send(res, 403, { ok: false, error: 'forbidden' });

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
    if (req.method === 'GET'  && u.pathname === '/api/paper/candles')      return paper.stageCandles(req, res);
    if (req.method === 'GET'  && u.pathname === '/api/paper/tape')         return paper.marketTape(req, res, u);
    if (req.method === 'GET'  && u.pathname === '/api/paper/pyth-history') return paper.pythHistory(req, res, u);
    if (req.method === 'GET'  && u.pathname === '/api/paper/pyth-stream')  return paper.pythStream(req, res);
    // competition: state and draw-verification are public (the wall and the
    // room read them); every mutation goes through compAdmin's own token.
    if (req.method === 'GET'  && u.pathname === '/api/paper/comp/state')    return paper.compState(req, res);
    if (req.method === 'GET'  && u.pathname === '/api/paper/comp/verify')   return paper.compVerify(req, res, u);
    if (req.method === 'POST' && u.pathname === '/api/paper/comp/admin')    return await paper.compAdmin(req, res);
    // /api/paper/guest lives on phoenix-teams (it writes users.db + cookie).
    return send(res, 404, { ok: false, error: 'not_found' });
  } catch (e) {
    log(`handler error ${u.pathname}: ${e.message}`);
    try { send(res, 500, { ok: false, error: 'internal' }); } catch { /* headers gone */ }
  }
});

paper.init({ apiGet, warehouseGet, log, readRateOk });
setInterval(() => { try { paper.sweep(); } catch (e) { log(`paper-sweep error: ${e.message}`); } }, 5_000);
setTimeout(() => { try { paper.sweep(); } catch (e) { log(`paper-sweep error: ${e.message}`); } }, 15_000);
paper.attachIndexWs(server);

server.listen(PORT, () => log(`phoenix-paper listening on :${PORT}`));
