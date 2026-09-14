#!/usr/bin/env node
throw new Error('Retired legacy fault drill: do not run this against a service or database. Use npm test for isolated fixtures; any production rehearsal requires separate authorization and DEPLOY.md.');
// Archival body retained below for provenance; unreachable by design.
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
 *   1. a restart during the sealed warning    -> active time and secrecy survive
 *   2. a restart during Hot Market #1         -> the revealed ordinary market resumes
 *   3. an operator mistake mid-round          -> must be refused
 *   4. real Hot #2, Final Build and 500x      -> frozen bankroll and result proof agree
 *      Boost traffic through the bell
 *
 * It uses clearly-marked throwaway accounts. Live exposure and orders are
 * cleaned even on failure; the settled/aborted round, fills and proof rows are
 * retained as immutable audit evidence. Nothing touches a real trader account.
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

async function abortRehearsal(reason) {
  let ordinary = null;
  try { ordinary = await admin('abort', { id: ROUND }); } catch { /* inspect below */ }
  if (ordinary && ordinary.code >= 200 && ordinary.code < 300 && ordinary.body?.ok) return;
  await sleep(400);                       // let the state micro-cache turn over
  const response = await state();
  if (response.code < 200 || response.code >= 300 || !response.body?.ok) {
    throw new Error(`cleanup state failed (${response.code})`);
  }
  const observed = response.body;
  const live = observed.live && observed.round?.id === ROUND ? observed : null;
  const armed = (observed.armed || []).find((row) => row.id === ROUND) || null;
  if (!live && !armed) return;
  if (!(live?.blocked || live?.blockedReason || armed?.blockedReason)) {
    throw new Error(`ordinary cleanup abort refused while ${ROUND} remains unblocked`);
  }
  const forced = await admin('forceAbort', {
    id: ROUND,
    reason: `automated fault-rehearsal cleanup: ${String(reason || 'rehearsal failure').slice(0, 150)}`,
  });
  if (forced.code < 200 || forced.code >= 300 || !forced.body?.ok) {
    throw new Error(`forceAbort failed (${forced.code}): ${forced.body?.error || 'unknown error'}`);
  }
}

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
async function waitHot(number, timeoutMs = 120_000) {
  const t0 = Date.now();
  for (;;) {
    const s = await state().catch(() => null);
    const body = s && s.body;
    if (body && body.phase === 'hot' && Number(body.hotNumber) === Number(number)
        && body.hot && Number(body.hot.number) === Number(number)) return body;
    if (body && body.blocked) return body;
    if (Date.now() - t0 > timeoutMs) return null;
    await sleep(400);
  }
}

async function cleanup() {
  step('cleanup');
  try { await abortRehearsal('fault-injection rehearsal finished with failures'); }
  catch (e) { bad('round cleanup failed: ' + e.message); }
  // Remove executable residue, but preserve the round/fills/proofs. A current
  // v2 result is intentionally immutable and deleting it would defeat the
  // rehearsal's commit/reveal and execution audit.
  const script = `
    const a = require('/opt/phoenix-paper/auth-shim.js');
    const ids = ${JSON.stringify(IDS)};
    const d = a.db;
    for (const id of ids) {
      d.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(id);
      d.prepare("UPDATE paper_orders SET status='CANCELLED', closed_at=? WHERE user_id=? AND status='OPEN'").run(Date.now(), id);
    }
    console.log('removed executable residue; preserved round ${ROUND} and immutable audit evidence');
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
      id: ROUND, kind: 'rehearsal', candidates: ['BTC', 'SOL', 'ETH', 'XRP'],
      players: IDS.map((u, i) => ({ userId: u, displayName: `Seat ${i + 1}`, seat: i })),
    });
    if (!r.body.ok) bad('arm failed: ' + JSON.stringify(r.body));
    else if (r.body.round && r.body.round.drawCommit != null) {
      bad('armed response leaked sealed draw material');
    } else ok('armed with one eligible-market pool; draw remains sealed');
    r = await admin('preflight', { id: ROUND });
    /* A drill restarts the engine on purpose, which empties the reliability
       history production now demands. That is exactly the case the override
       exists for, so the drill takes it explicitly and audibly rather than
       being silently exempt. */
    if (!r.body.marketsReady && /reliability history/.test(r.body.marketsError || '')) {
      await admin('overrideReadiness', { id: ROUND, why: 'rehearsal on a deliberately restarted engine' });
      console.log('   note   readiness overridden: this drill restarts the engine by design');
      r = await admin('preflight', { id: ROUND });
    }
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

    step('fault 1: restart during the sealed Hot #1 warning');
    let s = await waitPhase('hotWarning');
    if (!s || Number(s.hotNumber) !== 1) { bad('never reached Hot #1 warning'); } else if (s.hot || (s.hots || []).length) {
      bad('warning revealed an asset before activation');
    } else {
      ok('generic warning is live and the asset is still sealed');
      restartEngine('Hot #1 warning');
      if (!(await waitUp())) { bad('engine did not come back'); } else {
        s = (await state()).body;
        if (s.blocked) bad('warning restart blocked: ' + s.blockedReason);
        else if (s.phase === 'hotWarning' && (s.hot || (s.hots || []).length)) {
          bad('restart leaked the sealed Hot #1 market');
        } else ok(`survived warning restart in ${s.phase} without leaking or blocking`);
        /* Position identity must survive a restart exactly: same symbol, side,
           size, entry and margin mode. A phantom fill or a silent liquidation
           here would be invisible to a drill that carried no exposure. */
        const after = positionsOf(IDS[0]);
        const same = JSON.stringify(after) === JSON.stringify(global.__before);
        same ? ok('exposure survived the restart unchanged')
             : bad(`exposure changed across restart:\n     before ${JSON.stringify(global.__before)}\n     after  ${JSON.stringify(after)}`);
      }
    }

    step('fault 2: restart during Hot Market #1');
    s = await waitHot(1);
    let firstHotMarket = null;
    if (!s || s.blocked) { bad('never reached Hot #1' + (s && s.blockedReason ? ': ' + s.blockedReason : '')); } else {
      firstHotMarket = s.hot.market;
      ok(`Hot #1 active on ordinary market ${firstHotMarket}`);
      restartEngine('mid-Hot #1');
      if (!(await waitUp())) { bad('engine did not come back'); } else {
        /* Recovery waits for the same revealed market's composite price. V2
           Hot has no synthetic gate/ticker to recreate. */
        const t0 = Date.now();
        let restored = false;
        for (;;) {
          s = (await state().catch(() => ({ body: {} }))).body;
          if (s.blocked) { bad('blocked after restart: ' + s.blockedReason); break; }
          if (s.phase !== 'hot' || Number(s.hotNumber) !== 1) { bad(`Hot #1 ended before recovery (phase ${s.phase}) after ${Date.now() - t0}ms`); break; }
          if (s.hot && s.hot.market === firstHotMarket && !s.paused) { restored = true; break; }
          if (Date.now() - t0 > 25_000) { bad('Hot #1 did not resume within 25s'); break; }
          await sleep(500);
        }
        if (restored) ok(`same Hot #1 market resumed after ${Date.now() - t0}ms`);
      }
    }

    step('trade the ordinary market during Hot #1');
    {
      s = (await state().catch(() => ({ body: {} }))).body;
      if (s.live && s.phase === 'hot' && Number(s.hotNumber) === 1 && s.hot) {
        /* A restart empties the engine's source memory, so for the recovery
           grace nothing is competition-valid and the SHARED PAUSE is held: no
           liquidations, no drawdown, and — correctly — no trading either.
           Wait for it to lift, which is what a show would do, rather than
           asserting a trade the rules currently forbid. */
        const tp = Date.now();
        while (Date.now() - tp < 25_000) {
          const cur = (await state().catch(() => ({ body: {} }))).body;
          if (!cur.paused) break;
          await sleep(500);
        }
        const stillPaused = ((await state().catch(() => ({ body: {} }))).body || {}).paused;
        if (stillPaused) {
          bad(`still paused after 25s: ${JSON.stringify(stillPaused.symbols)}`);
        } else {
          ok(`shared pause lifted ${Date.now() - tp}ms after the restart`);
        }
        const r2 = await trade(IDS[0], { symbol: s.hot.market, side: 'BUY', type: 'MARKET', notionalUsd: 1, leverage: 10 });
        r2.code === 200 ? ok(`Hot #1 exposure traded on ${s.hot.market}`)
                        : bad('hot order refused: ' + JSON.stringify(r2.body));
        const held = positionsOf(IDS[0]);
        held.some((p) => p.symbol === s.hot.market)
          ? ok('Hot movement accrues on the ordinary position')
          : bad('ordinary Hot position is missing');
        held.some((p) => p.symbol.endsWith('-HOT'))
          ? bad('v2 created a retired synthetic -HOT position')
          : ok('no synthetic -HOT ticker was created');
      } else {
        bad('Hot #1 window not open when expected');
      }
    }

    step('fault 3: operator mistake mid-round');
    r = await admin('resetPlayers', { id: ROUND });
    r.code === 409 ? ok('resetPlayers refused on a live round')
                   : bad(`resetPlayers returned ${r.code}: ${JSON.stringify(r.body)}`);

    step('observe and trade Hot Market #2');
    s = await waitHot(2);
    if (!s || s.blocked) {
      bad('never reached Hot #2' + (s && s.blockedReason ? ': ' + s.blockedReason : ''));
    } else {
      const secondHotMarket = s.hot.market;
      secondHotMarket && secondHotMarket !== firstHotMarket
        ? ok(`Hot #2 is distinct: ${secondHotMarket}`)
        : bad(`Hot markets were not distinct (${firstHotMarket}, ${secondHotMarket})`);
      const r2 = await trade(IDS[1], { symbol: secondHotMarket, side: 'SELL',
        type: 'MARKET', notionalUsd: 1, leverage: 10 });
      r2.code === 200 ? ok(`Hot #2 exposure traded on ${secondHotMarket}`)
                      : bad('Hot #2 order refused: ' + JSON.stringify(r2.body));
    }

    step('Final Build publishes projected 500x capacity');
    s = await waitPhase('finalBuild', 120_000);
    if (!s || s.blocked) bad('never reached Final Build');
    else {
      const projected = (s.players || []).filter((p) => Number.isFinite(Number(p.projectedBoostPower)));
      projected.length === IDS.length
        ? ok(`projected Boost power published for ${projected.length} seats`)
        : bad(`projected Boost power missing for ${IDS.length - projected.length} seat(s)`);
    }

    step('fault 4: trade 500x Boost under the round capacity policy and ride to the bell');
    s = await waitPhase('boost', 90_000);
    if (!s || s.blocked || !s.boostOpen || !(s.boostMarkets || []).length) {
      bad('Boost never opened: ' + JSON.stringify(s && { phase: s.phase, blocked: s.blocked, markets: s.boostMarkets }));
    } else {
      const boostLeverage = Number(s.round && s.round.boostLeverage) || 500;
      const player = (s.players || []).find((p) => Number(p.userId) === IDS[0]);
      const policy = s.round?.boostCapacityPolicy ?? 'frozen-start-v1';
      const bankroll = policy === 'current-equity-v1' ? player?.equity : player?.boostBankroll;
      const tolerance = policy === 'current-equity-v1'
        ? 0.0000005 * boostLeverage + 0.0000005 : 1e-6;
      if (['current-equity-v1', 'frozen-start-v1'].includes(policy)
          && Number.isFinite(player?.boostBankroll) && Number.isFinite(bankroll)
          && Number.isFinite(player?.boostMaxExposure)
          && Math.abs(player.boostMaxExposure - Math.max(0, bankroll) * boostLeverage)
            <= tolerance + Number.EPSILON * Math.abs(player.boostMaxExposure)) {
        ok(`${policy}: max exposure ${player.boostMaxExposure}; Boost-start equity ${player.boostBankroll}`);
      } else bad(`Boost capacity does not match the round's ${boostLeverage}x policy`);
      const boostSymbol = s.boostMarkets[0] + '-BOOST';
      const br = await trade(IDS[0], { symbol: boostSymbol, side: 'BUY', type: 'MARKET',
        notionalUsd: 1, leverage: boostLeverage });
      br.code === 200 ? ok(`${boostLeverage}x Boost order accepted on ${boostSymbol}`)
                      : bad('Boost order refused: ' + JSON.stringify(br.body));
    }
    for (let i = 0; i < 240; i++) {
      s = (await state().catch(() => ({ body: {} }))).body;
      if (!s.live) break;
      await sleep(1000);
    }
    /* Hot uses the ordinary position and does not force-close it. Only event
       Boost aliases are flattened at their shared bell mark. */
    {
      const left = positionsOf(IDS[0]);
      left.some((p) => p.symbol.endsWith('-HOT'))
        ? bad('a retired synthetic Hot position exists: ' + JSON.stringify(left))
        : ok('both Hot windows used ordinary positions only');
      left.some((p) => p.symbol.endsWith('-BOOST'))
        ? bad('a Boost position survived the bell: ' + JSON.stringify(left))
        : ok('Boost aliases settled at the shared bell');
      left.some((p) => !p.symbol.includes('-'))
        ? ok('base position marked at the bell, not force-closed')
        : bad('the base position vanished: ' + JSON.stringify(left));
    }
    if (s && s.lastRound && Array.isArray(s.lastRound.hots)
        && s.lastRound.hots.length === 2) ok('recap contains both Hot Markets');
    else bad('recap does not contain two Hot Markets');

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
    step('verify the draw and execution publicly');
    r = await req('GET', `/api/paper/comp/verify?round=${ROUND}`);
    r.body && r.body.verified && r.body.verified.ok && r.body.canonical && r.body.canonical.matches
      ? ok('committed two-Hot draw verifies')
      : bad('draw does NOT verify: ' + JSON.stringify(r.body && r.body.verified));
    r.body && r.body.execution && r.body.execution.verified
      ? ok('active-time, Hot score, Boost-start reference and final score execution proof verifies')
      : bad('execution does NOT verify: ' + JSON.stringify(r.body && r.body.execution && r.body.execution.checks));
  } catch (e) {
    bad('rehearsal threw: ' + e.message);
  } finally {
    await cleanup();
  }

  console.log(`\n${failures ? failures + ' FAILURES' : 'all checks passed'}\n`);
  process.exitCode = failures ? 1 : 0;
})();
