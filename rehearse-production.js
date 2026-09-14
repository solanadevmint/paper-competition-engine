#!/usr/bin/env node
'use strict';

/* Safe production end-to-end rehearsal.
 *
 * Runs one three-minute, clearly-labelled solo round through the real HTTP
 * operator API and lets only reserved engine bot accounts trade.  It never
 * creates/deletes a human identity, edits SQLite directly, or erases its
 * audit record.  Run on the paper box with PAPER_COMP_TOKEN and
 * PAPER_GATE_SECRET in the environment.
 *
 *   node rehearse-production.js
 *   REHEARSAL_ALLOW_READINESS_OVERRIDE=1 node rehearse-production.js
 */
const http = require('node:http');

const BASE = process.env.REHEARSE_BASE || 'http://127.0.0.1:9200';
const TOKEN = String(process.env.PAPER_COMP_TOKEN || '');
const GATE = String(process.env.PAPER_GATE_SECRET || '');
const ALLOW_OVERRIDE = process.env.REHEARSAL_ALLOW_READINESS_OVERRIDE === '1';
const ROUND = `ops-rehearsal-${new Date().toISOString().replace(/[-:TZ.]/g, '').slice(0, 14)}`;

if (!TOKEN || !GATE) {
  console.error('PAPER_COMP_TOKEN and PAPER_GATE_SECRET are required');
  process.exit(2);
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
function request(method, pathname, body) {
  return new Promise((resolve, reject) => {
    const data = body == null ? null : JSON.stringify(body);
    const req = http.request(BASE + pathname, {
      method,
      timeout: 10_000,
      headers: {
        'x-paper-gate': GATE,
        'x-comp-token': TOKEN,
        ...(data ? { 'content-type': 'application/json', 'content-length': Buffer.byteLength(data) } : {}),
      },
    }, (res) => {
      let raw = '';
      res.on('data', (chunk) => { raw += chunk; });
      res.on('end', () => {
        let parsed;
        try { parsed = JSON.parse(raw || '{}'); }
        catch { parsed = { ok: false, error: 'invalid JSON response' }; }
        resolve({ status: res.statusCode || 0, body: parsed });
      });
    });
    req.on('timeout', () => req.destroy(new Error('request timed out')));
    req.on('error', reject);
    if (data) req.write(data);
    req.end();
  });
}
const state = () => request('GET', '/api/paper/comp/state');
const admin = (action, fields = {}) => request('POST', '/api/paper/comp/admin', { action, ...fields });
function requireOk(reply, label) {
  if (reply.status < 200 || reply.status >= 300 || reply.body?.ok !== true) {
    throw new Error(`${label} failed (${reply.status}): ${reply.body?.error || 'unknown error'}`);
  }
  return reply.body;
}

/* Ordinary Abort is deliberately powerless against a blocked round. Cleanup
 * therefore tries the normal audited path first, then confirms from public
 * state that THIS rehearsal still owns a blocked running/armed slot before
 * using the separately named forceAbort action with an explicit reason. A
 * `{force:true}` field on `abort` is ignored by the server and used to leave
 * failed rehearsals live indefinitely. */
async function abortRehearsal(reason) {
  let ordinary = null;
  try { ordinary = await admin('abort', { id: ROUND }); } catch { /* inspect below */ }
  if (ordinary && ordinary.status >= 200 && ordinary.status < 300 && ordinary.body?.ok) return;

  /* Let the state micro-cache turn over after the failed mutation. */
  await sleep(300);
  const observed = requireOk(await state(), 'cleanup state');
  const live = observed.live && observed.round?.id === ROUND ? observed : null;
  const armed = (observed.armed || []).find((row) => row.id === ROUND) || null;
  const stillPresent = !!(live || armed);
  const blocked = !!(live?.blocked || live?.blockedReason || armed?.blockedReason);
  if (!stillPresent) return;              // it settled/aborted while we inspected
  if (!blocked) {
    throw new Error(`cleanup abort refused while rehearsal ${ROUND} remains unblocked`);
  }
  requireOk(await admin('forceAbort', {
    id: ROUND,
    reason: `automated production-rehearsal cleanup: ${String(reason || 'rehearsal failure').slice(0, 140)}`,
  }), 'force-abort blocked rehearsal');
}

(async () => {
  const initial = requireOk(await state(), 'initial state');
  if (initial.live !== false || (initial.armed || []).length || initial.pending) {
    throw new Error('rehearsal refused: competition is live, armed, or scheduled');
  }

  console.log(`round=${ROUND}`);
  const created = requireOk(await admin('create', {
    id: ROUND,
    kind: 'rehearsal',
    solo: true,
    seats: 4,
    stage: 'ops-rehearsal',
    /* One eligible universe only. The engine privately shuffles two
       primaries and the complete backup order before Start. */
    candidates: ['BTC', 'SOL', 'ETH', 'XRP'],
    players: [{ seat: 0, displayName: 'Post-deploy observer' }],
    botRoster: [
      { displayName: 'Rehearsal Alpha' },
      { displayName: 'Rehearsal Bravo' },
      { displayName: 'Rehearsal Charlie' },
    ],
  }), 'create');
  if (created.round?.draw_commit || created.round?.drawCommit) {
    throw new Error('armed response leaked the draw commitment before Start');
  }
  console.log('sealed two-Hot draw armed');

  /* Practice bots are persistent reserved accounts and may carry positions or
     a non-Stage mode from an earlier rehearsal. Production preflight quite
     correctly rejects that state. Prepare the claimed bot seats through the
     same atomic operator action used by the real desk; the named observer is
     unclaimed and is deliberately skipped. */
  requireOk(await admin('resetPlayers', { id: ROUND }), 'prepare rehearsal seats');
  let preflight = requireOk(await admin('preflight', { id: ROUND }), 'preflight');
  if (!preflight.allReady) {
    if (!ALLOW_OVERRIDE || !preflight.marketsError || !/history|more needed/i.test(preflight.marketsError)) {
      throw new Error(`preflight refused: ${preflight.marketsError || preflight.seatsError || 'not ready'}`);
    }
    requireOk(await admin('overrideReadiness', {
      id: ROUND,
      why: 'post-deploy production rehearsal before the reliability window filled',
    }), 'readiness override');
    preflight = requireOk(await admin('preflight', { id: ROUND }), 'preflight after override');
  }
  if (!preflight.allReady) throw new Error(`preflight remained red: ${preflight.marketsError || 'unknown'}`);

  requireOk(await admin('start', { id: ROUND }), 'start');

  /* /comp/state is deliberately micro-cached for the wall. The idle document
     read before Create can therefore survive for a fraction of a second after
     Start. Do not mistake that stale-but-valid idle response for an instantly
     finished round (the safety catch would then abort the round we just
     started). First require positive observation of this exact live round. */
  let last;
  const liveProofDeadline = Date.now() + 10_000;
  while (Date.now() < liveProofDeadline) {
    last = requireOk(await state(), 'live-start proof');
    if (last.live && last.round?.id === ROUND) break;
    if (last.live && last.round?.id !== ROUND) {
      throw new Error(`unexpected live round ${last.round?.id || '(unknown)'}`);
    }
    await sleep(250);
  }
  if (!last?.live || last.round?.id !== ROUND) throw new Error('started round was never published live');
  const boostCapacityPolicy = last.round?.boostCapacityPolicy ?? 'frozen-start-v1';
  if (!['frozen-start-v1', 'current-equity-v1'].includes(boostCapacityPolicy)) {
    throw new Error('unknown Boost capacity policy');
  }

  const phases = [];
  let lastPhase = null;
  let maxOpenPositions = 0;
  const warningNumbers = new Set();
  const hotNumbers = new Set();
  const hotMarkets = new Set();
  let sawFinalBuild = false;
  let sawBoostOpen = false;
  let sawProjectedBoost = false;
  let sawFrozenBoost = false;
  let sawBoostCapacity = false;
  const deadline = Date.now() + 5 * 60_000;
  while (Date.now() < deadline) {
    last = requireOk(await state(), 'state poll');
    if (!last.live) break;
    if ((last.round?.boostCapacityPolicy ?? 'frozen-start-v1') !== boostCapacityPolicy) {
      throw new Error('Boost capacity policy changed during the round');
    }
    if (last.blocked) throw new Error(`round blocked: ${last.blockedReason || 'unspecified'}`);
    if (last.phase !== lastPhase) {
      lastPhase = last.phase;
      phases.push(lastPhase);
      console.log(`phase=${lastPhase}`);
    }
    const players = Array.isArray(last.players) ? last.players : [];
    maxOpenPositions = Math.max(maxOpenPositions,
      players.reduce((n, player) => n + (Array.isArray(player.positions) ? player.positions.length : 0), 0));
    if (last.phase === 'hotWarning' && Number(last.hotNumber)) {
      const ordinal = Number(last.hotNumber);
      warningNumbers.add(ordinal);
      if (last.hot != null || (last.hots || []).some((h) => Number(h.number) >= ordinal)) {
        throw new Error(`Hot #${ordinal} asset leaked during its warning`);
      }
    }
    if (last.phase === 'hot' && last.hot?.open && Number(last.hot.number || last.hotNumber)) {
      const ordinal = Number(last.hot.number || last.hotNumber);
      const market = String(last.hot.market || '');
      if (!market || /-(HOT|BOOST)$/.test(market)) {
        throw new Error(`Hot #${ordinal} did not use an ordinary market`);
      }
      hotNumbers.add(ordinal);
      hotMarkets.add(market);
    }
    if (last.phase === 'finalBuild') {
      sawFinalBuild = true;
      sawProjectedBoost ||= players.length > 0 && players.every((player) =>
        Number.isFinite(Number(player.projectedBoostPower)));
    }
    if (last.phase === 'boost' && last.boostOpen) {
      sawBoostOpen = true;
      const leverage = Number(last.round?.boostLeverage || 500);
      const validCapacity = Number.isFinite(leverage) && leverage > 0
        && players.length > 0 && players.every((player) => {
          const bankroll = boostCapacityPolicy === 'current-equity-v1'
            ? player.equity : player.boostBankroll;
          // Public equity has six decimals; the engine sizes dynamic Boost
          // from the same risk capture before that presentation rounding.
          const tolerance = boostCapacityPolicy === 'current-equity-v1'
            ? 0.0000005 * leverage + 0.0000005 : 1e-6;
          return Number.isFinite(player.boostBankroll) && Number.isFinite(bankroll)
            && Number.isFinite(player.boostMaxExposure)
            && Math.abs(player.boostMaxExposure - Math.max(0, bankroll) * leverage)
              <= tolerance + Number.EPSILON * Math.abs(player.boostMaxExposure);
        });
      if (!validCapacity) throw new Error('Boost capacity disagrees with its sealed policy');
      sawBoostCapacity = true;
      sawFrozenBoost ||= boostCapacityPolicy === 'frozen-start-v1';
    }
    await sleep(500);
  }

  if (!last || last.live) throw new Error('round did not reach the bell within five minutes');
  if (last.lastRound?.id !== ROUND || last.lastRound?.aborted) throw new Error('finished rehearsal was not published');
  if (!phases.includes('build') || !phases.includes('hotWarning')
      || !phases.includes('hot') || !phases.includes('finalBuild')
      || !phases.includes('boost')) {
    throw new Error(`missing phase(s): saw ${phases.join(',')}`);
  }
  if (warningNumbers.size !== 2 || hotNumbers.size !== 2 || hotMarkets.size !== 2
      || !sawFinalBuild || !sawProjectedBoost || !sawBoostOpen || !sawBoostCapacity) {
    throw new Error(`incomplete format: warnings=${[...warningNumbers]}, hots=${[...hotNumbers]}, markets=${[...hotMarkets]}, finalBuild=${sawFinalBuild}, projected=${sawProjectedBoost}, boost=${sawBoostOpen}, capacity=${sawBoostCapacity}`);
  }
  if (maxOpenPositions <= 0) throw new Error('practice bots never opened a position');
  const board = last.lastRound?.board || [];
  if (board.length !== 3) throw new Error(`final board has ${board.length}/3 bot seats`);
  if (!Array.isArray(last.lastRound?.hots) || last.lastRound.hots.length !== 2) {
    throw new Error('settled recap does not contain both Hot Markets');
  }
  const verification = requireOk(await request('GET', `/api/paper/comp/verify?round=${encodeURIComponent(ROUND)}`), 'draw verification');
  if (!verification.verified?.ok) throw new Error('commit-reveal verification failed');
  if (!verification.execution?.verified) {
    throw new Error(`execution verification failed: ${JSON.stringify(verification.execution?.checks || {})}`);
  }

  console.log(JSON.stringify({
    ok: true,
    round: ROUND,
    phases,
    hotWarnings: warningNumbers.size,
    hotMarkets: hotNumbers.size,
    distinctHotMarkets: hotMarkets.size,
    sawFinalBuild,
    sawProjectedBoost,
    sawBoostOpen,
    sawFrozenBoost,
    sawBoostCapacity,
    boostCapacityPolicy,
    maxOpenPositions,
    finalSeats: board.length,
    drawVerified: true,
    executionVerified: true,
    practice: last.lastRound?.solo === true,
  }));
})().catch(async (error) => {
  console.error(`FAIL ${error.message}`);
  // Preserve evidence, but do not leave an armed/running rehearsal behind.
  try { await abortRehearsal(error.message); }
  catch (cleanupError) { console.error(`CLEANUP FAIL ${cleanupError.message}`); }
  process.exit(1);
});
