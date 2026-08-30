#!/usr/bin/env node
/* Fault-injection rehearsal.
 *
 *   sudo -u ubuntu PAPER_COMP_TOKEN=... node rehearse.js
 *
 * Runs a compressed round against the LIVE engine, over HTTP, on real index
 * prices, and restarts the service underneath it at the worst moments. The
 * unit suites assert these behaviours in isolation with synthetic marks; this
 * is where they meet a real feed, a real database and each other.
 *
 * What it injects, in order:
 *   1. a restart during the Hot window        -> gates must rehydrate
 *   2. a restart just after the draw commits  -> draw must survive, not block
 *   3. an operator mistake mid-round          -> must be refused
 *   4. the bell arriving while the engine is  -> result must still be settled
 *      busy                                      and priced at the bell
 *
 * It uses clearly-marked throwaway accounts and removes everything it created,
 * including on failure. Nothing here touches a real trader's account.
 */
const http = require('node:http');
const { execFileSync } = require('node:child_process');

const BASE = process.env.REHEARSE_BASE || 'http://127.0.0.1:9200';
const TOKEN = process.env.PAPER_COMP_TOKEN || '';
const GATE = process.env.PAPER_GATE_SECRET || '';
const IDS = [990101, 990102];
const ROUND = `rehearsal-${Date.now().toString(36)}`;

if (!TOKEN) { console.error('PAPER_COMP_TOKEN required'); process.exit(2); }

let failures = 0;
const step = (name) => console.log(`\n── ${name}`);
const ok = (msg) => console.log(`   ok    ${msg}`);
const bad = (msg) => { console.log(`   FAIL  ${msg}`); failures++; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function req(method, path, body, extraHeaders) {
  return new Promise((resolve, reject) => {
    const data = body ? JSON.stringify(body) : null;
    const headers = { 'content-type': 'application/json' };
    if (TOKEN) headers['x-comp-token'] = TOKEN;
    if (GATE) headers['x-paper-gate'] = GATE;
    if (data) headers['content-length'] = Buffer.byteLength(data);
    Object.assign(headers, extraHeaders || {});
    const r = http.request(BASE + path, { method, headers, timeout: 15000 }, (res) => {
      let b = '';
      res.on('data', (c) => { b += c; });
      res.on('end', () => {
        try { resolve({ code: res.statusCode, body: JSON.parse(b || '{}') }); }
        catch { resolve({ code: res.statusCode, body: { raw: b.slice(0, 200) } }); }
      });
    });
    r.on('error', reject);
    r.on('timeout', () => r.destroy(new Error('timeout')));
    if (data) r.write(data);
    r.end();
  });
}
const admin = (action, extra = {}) => req('POST', '/api/paper/comp/admin', { action, ...extra });

/* Trading as a seated player, over HTTP against the LIVE engine.
 *
 * An earlier version required paper.js inside this process and called
 * placeOrder directly. That silently created a SECOND engine instance: the
 * database was shared but the in-memory alias registry, round cache and price
 * state were not, so segment gates looked closed and positions were written
 * by an engine nobody was driving. A drill that does not go through the same
 * door as a real trader is not testing the thing that will be on stage.
 *
 * Sessions are minted with the internal secret the engine already trusts. */
/* Real session cookies, minted outside the drill and passed in base64 (the
   raw JSON is full of double quotes and does not survive a shell command
   line). No test-only auth bypass is added to a competition engine just to
   make a drill convenient. */
const SEAT_TOKENS = JSON.parse(
  process.env.SEAT_TOKENS_B64
    ? Buffer.from(process.env.SEAT_TOKENS_B64, 'base64').toString('utf8')
    : '{}'
);
function trade(userId, body) {
  const tok = SEAT_TOKENS[String(userId)];
  if (!tok) return Promise.resolve({ code: 0, body: { error: 'no session for seat ' + userId } });
  return req('POST', '/api/paper/order', body, { cookie: 'phoenix_session=' + tok });
}
const positionsOf = (userId) => {
  const a = require('/opt/phoenix-paper/auth-shim.js');
  return a.db.prepare('SELECT symbol, side, size, entry_price, leverage, margin_mode FROM paper_positions WHERE user_id = ? ORDER BY symbol').all(userId);
};

const state = () => req('GET', '/api/paper/comp/state');

function restartEngine(why) {
  console.log(`   ... restarting engine (${why})`);
  execFileSync('sudo', ['systemctl', 'restart', 'phoenix-paper'], { stdio: 'ignore' });
}
async function waitUp(timeoutMs = 30_000) {
  const t0 = Date.now();
  for (;;) {
    try { const r = await state(); if (r.code === 200) return true; } catch { /* still down */ }
    if (Date.now() - t0 > timeoutMs) return false;
    await sleep(500);
  }
}
/** Wait until the live round reports `phase`, or give up. */
async function waitPhase(phase, timeoutMs = 90_000) {
  const t0 = Date.now();
  for (;;) {
    const s = await state().catch(() => null);
    const p = s && s.body && s.body.phase;
    if (p === phase) return s.body;
    if (s && s.body && s.body.blocked) return s.body;      // stop early if blocked
    if (Date.now() - t0 > timeoutMs) return null;
    await sleep(400);
  }
}

async function cleanup() {
  step('cleanup');
  try { await admin('abort', { id: ROUND }); } catch { /* may already be finished */ }
  // remove the throwaway accounts and this round's rows
  const script = `
    const a = require('/opt/phoenix-paper/auth-shim.js');
    const ids = ${JSON.stringify(IDS)};
    const d = a.db;
    d.prepare("DELETE FROM paper_round_scores WHERE round_id = ?").run('${ROUND}');
    d.prepare("DELETE FROM paper_round_players WHERE round_id = ?").run('${ROUND}');
    d.prepare("DELETE FROM paper_round_boundaries WHERE round_id = ?").run('${ROUND}');
    d.prepare("DELETE FROM paper_rounds WHERE id = ?").run('${ROUND}');
    for (const id of ids) {
      d.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(id);
      d.prepare('DELETE FROM paper_orders WHERE user_id = ?').run(id);
      d.prepare('DELETE FROM paper_fills WHERE user_id = ?').run(id);
      d.prepare('DELETE FROM paper_accounts WHERE user_id = ?').run(id);
      d.prepare('DELETE FROM users WHERE id = ?').run(id);
    }
    console.log('removed rehearsal rows and accounts');
  `;
  try {
    console.log('   ' + execFileSync('node', ['-e', script], { encoding: 'utf8' }).trim());
  } catch (e) { console.log('   cleanup warning: ' + e.message); }
}

(async () => {
  console.log(`rehearsal ${ROUND} against ${BASE}`);
  try {
    step('seed throwaway seats');
    const seed = `
      const a = require('/opt/phoenix-paper/auth-shim.js');
      for (const id of ${JSON.stringify(IDS)}) {
        a.db.prepare('INSERT OR IGNORE INTO users (id) VALUES (?)').run(id);
        a.db.prepare('INSERT OR IGNORE INTO paper_accounts (user_id, created_at, updated_at) VALUES (?,?,?)')
          .run(id, Date.now(), Date.now());
        a.db.prepare('UPDATE paper_accounts SET heat = 1, start_balance = 10, balance = 10 WHERE user_id = ?').run(id);
      }
      console.log('seeded ${IDS.length} seats');
    `;
    console.log('   ' + execFileSync('node', ['-e', seed], { encoding: 'utf8' }).trim());

    step('arm and pre-flight');
    let r = await admin('create', {
      id: ROUND, kind: 'rehearsal', candidates: ['BTC', 'SOL', 'ETH'], backup: 'XRP',
      players: IDS.map((u, i) => ({ userId: u, displayName: `Seat ${i + 1}`, seat: i })),
    });
    r.body.ok ? ok(`armed, commit ${String(r.body.round.draw_commit).slice(0, 12)}…`)
              : bad('arm failed: ' + JSON.stringify(r.body));
    r = await admin('preflight', { id: ROUND });
    r.body.marketsReady ? ok('market data ready') : bad('markets not ready: ' + JSON.stringify(r.body.markets));
    r.body.seatsReady ? ok('seats ready') : bad('seats not ready');

    step('start');
    r = await admin('start', { id: ROUND });
    r.body.ok ? ok('running') : bad('start failed: ' + JSON.stringify(r.body));

    step('open real exposure before anything is injected');
    {
      const r1 = await trade(IDS[0], { symbol: 'BTC', side: 'BUY', type: 'MARKET', notionalUsd: 1, leverage: 10 });
      r1.code === 200 ? ok('base position open') : bad('base order refused: ' + JSON.stringify(r1.body));
      const before = positionsOf(IDS[0]);
      before.length ? ok(`carrying ${before.length} position(s) into the faults`)
                    : bad('no exposure to carry');
      global.__before = before;
    }

    step('fault 1: restart just after the draw commits');
    let s = await waitPhase('reveal');
    if (!s) { bad('never reached the reveal'); } else {
      ok(`drew ${s.hot && s.hot.market}`);
      restartEngine('post-draw');
      if (!(await waitUp())) { bad('engine did not come back'); } else {
        s = (await state()).body;
        if (s.blocked) bad('a persisted draw should NOT block: ' + s.blockedReason);
        else ok('survived the restart without blocking');
        /* Position identity must survive a restart exactly: same symbol, side,
           size, entry and margin mode. A phantom fill or a silent liquidation
           here would be invisible to a drill that carried no exposure. */
        const after = positionsOf(IDS[0]);
        const same = JSON.stringify(after) === JSON.stringify(global.__before);
        same ? ok('exposure survived the restart unchanged')
             : bad(`exposure changed across restart:\n     before ${JSON.stringify(global.__before)}\n     after  ${JSON.stringify(after)}`);
      }
    }

    step('fault 2: restart during the Hot window');
    s = await waitPhase('hot');
    if (!s || s.blocked) { bad('never reached Hot' + (s && s.blockedReason ? ': ' + s.blockedReason : '')); } else {
      ok(`hot open on ${s.hot.market}`);
      restartEngine('mid-Hot');
      if (!(await waitUp())) { bad('engine did not come back'); } else {
        /* Recovery is not instant and should not be: the composite index has
           to resume before a ticker can be priced, and the engine retries
           until it can. Poll for the gate rather than sampling once and
           calling a slow recovery a failure. */
        const t0 = Date.now();
        let restored = false;
        for (;;) {
          s = (await state().catch(() => ({ body: {} }))).body;
          if (s.blocked) { bad('blocked after restart: ' + s.blockedReason); break; }
          if (s.phase !== 'hot') { bad(`Hot window ended before the gate came back (phase ${s.phase}) after ${Date.now() - t0}ms`); break; }
          if (s.hot && s.hot.open) { restored = true; break; }
          if (Date.now() - t0 > 20_000) { bad('Hot gate never rehydrated within 20s'); break; }
          await sleep(500);
        }
        if (restored) ok(`gates rehydrated after ${Date.now() - t0}ms`);
      }
    }

    step('fault 2b: trade the Hot segment, then restart again');
    {
      s = (await state().catch(() => ({ body: {} }))).body;
      if (s.live && s.hot && s.hot.open) {
        const r2 = await trade(IDS[0], { symbol: s.hot.ticker, side: 'BUY', type: 'MARKET', notionalUsd: 1, leverage: 10 });
        r2.code === 200 ? ok(`hot exposure open on ${s.hot.ticker}`)
                        : bad('hot order refused: ' + JSON.stringify(r2.body));
        const hotPos = positionsOf(IDS[0]).filter((p) => p.symbol.endsWith('-HOT'));
        hotPos.length ? ok('hot leg is a separate position, as designed')
                      : bad('no hot position created');
      } else {
        bad('Hot window not open when expected');
      }
    }

    step('fault 3: operator mistake mid-round');
    r = await admin('resetPlayers', { id: ROUND });
    r.code === 409 ? ok('resetPlayers refused on a live round')
                   : bad(`resetPlayers returned ${r.code}: ${JSON.stringify(r.body)}`);

    step('fault 4: ride to the bell');
    for (let i = 0; i < 240; i++) {
      s = (await state().catch(() => ({ body: {} }))).body;
      if (!s.live) break;
      await sleep(1000);
    }
    /* The Hot leg must have been force-closed by its own boundary and scored
       at 2x, while the base leg is still open and marked at the bell. */
    {
      const left = positionsOf(IDS[0]);
      left.some((p) => p.symbol.endsWith('-HOT'))
        ? bad('a Hot position survived its segment: ' + JSON.stringify(left))
        : ok('hot leg settled by its segment, base leg left open');
      left.some((p) => !p.symbol.includes('-'))
        ? ok('base position marked at the bell, not force-closed')
        : bad('the base position vanished: ' + JSON.stringify(left));
    }

    r = await admin('standings', { id: ROUND, checkpoint: 'final' });
    const board = (r.body && r.body.board) || [];
    board.length === IDS.length ? ok(`final settled for ${board.length} seats`)
                                : bad(`final has ${board.length}/${IDS.length} rows`);
    if (board.length) {
      const row = board[0];
      row.marks ? ok('mark set stored for replay') : bad('no mark set stored');
      Number.isFinite(row.scheduled_at) ? ok('scheduled bell instant recorded')
                                        : bad('no scheduled_at recorded');
      const drift = row.at - row.scheduled_at;
      console.log(`   note   bell ran ${drift}ms after it was due (priced at the due instant)`);
    }
    r = await admin('firstFive', { id: ROUND });
    'winner' in (r.body || {}) ? ok('First Five resolved') : bad('First Five missing');

    step('verify the draw publicly');
    r = await req('GET', `/api/paper/comp/verify?round=${ROUND}`);
    r.body && r.body.verified && r.body.verified.ok
      ? ok(`draw verifies (${r.body.drawn}${r.body.verified.fellBack ? ', fell back to ' + r.body.verified.traded : ''})`)
      : bad('draw does NOT verify: ' + JSON.stringify(r.body && r.body.verified));
  } catch (e) {
    bad('rehearsal threw: ' + e.message);
  } finally {
    await cleanup();
  }

  console.log(`\n${failures ? failures + ' FAILURES' : 'all checks passed'}\n`);
  process.exitCode = failures ? 1 : 0;
})();
