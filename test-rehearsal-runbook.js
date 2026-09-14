'use strict';

/* Regression coverage for the production rehearsal wrapper itself. The mock
 * deliberately gives bots dirty preflight state until resetPlayers is called,
 * then returns one cached idle state after Start. Those are the two conditions
 * that safely aborted the first production attempts on 2026-09-04. */
const assert = require('node:assert/strict');
const http = require('node:http');
const path = require('node:path');
const { spawn } = require('node:child_process');

const makePhases = () => [
  { phase: 'build' },
  { phase: 'hotWarning', hotNumber: 1 },
  { phase: 'hot', hotNumber: 1, hot: { number: 1, market: 'BTC', open: true } },
  { phase: 'build' },
  { phase: 'hotWarning', hotNumber: 2 },
  { phase: 'hot', hotNumber: 2, hot: { number: 2, market: 'SOL', open: true } },
  { phase: 'finalBuild' },
  { phase: 'boost' },
];
let phases = makePhases();
let roundId = null;
let started = false;
let stateAfterStart = 0;
let fixtureMode = 'success';
let forceAbortReason = null;
const actions = [];
const capacityPolicy = () => fixtureMode === 'unknown-policy' ? 'future-policy'
  : fixtureMode.startsWith('dynamic') ? 'current-equity-v1' : undefined;

function json(res, status, body) {
  const data = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(data) });
  res.end(data);
}

const server = http.createServer((req, res) => {
  assert.equal(req.headers['x-paper-gate'], 'gate-fixture');
  assert.equal(req.headers['x-comp-token'], 'comp-fixture');
  const url = new URL(req.url, 'http://fixture');

  if (req.method === 'GET' && url.pathname === '/api/paper/comp/state') {
    if (!started || stateAfterStart++ === 0) {
      return json(res, 200, { ok: true, live: false, armed: [], pending: null });
    }
    if (fixtureMode === 'blocked') {
      return json(res, 200, {
        ok: true, live: true,
        round: { id: roundId, boostLeverage: 500, boostCapacityPolicy: capacityPolicy() },
        phase: 'build', blocked: true, blockedReason: 'fixture price fault',
        hot: null, hots: [], boostOpen: false, players: [],
      });
    }
    const frame = phases.shift();
    if (frame) {
      const players = [{ positions: [{ symbol: 'BTC' }] }, { positions: [] }, { positions: [] }];
      if (frame.phase === 'finalBuild') {
        for (const player of players) player.projectedBoostPower = 5000;
      }
      if (frame.phase === 'boost') {
        for (const player of players) {
          player.boostBankroll = 10;
          player.equity = 12;
          player.hotBonus = 90; // leaderboard points never fund Boost.
          player.boostMaxExposure = fixtureMode === 'dynamic' ? 6000 : 5000;
        }
      }
      return json(res, 200, {
        ok: true,
        live: true,
        round: { id: roundId, boostLeverage: 500, boostCapacityPolicy: capacityPolicy() },
        ...frame,
        blocked: false,
        hot: frame.hot || null,
        boostOpen: frame.phase === 'boost',
        players,
      });
    }
    return json(res, 200, {
      ok: true,
      live: false,
      lastRound: {
        id: roundId,
        aborted: false,
        board: [{}, {}, {}],
        hots: [
          { number: 1, market: 'BTC', status: 'completed' },
          { number: 2, market: 'SOL', status: 'completed' },
        ],
        solo: true,
      },
    });
  }

  if (req.method === 'GET' && url.pathname === '/api/paper/comp/verify') {
    assert.equal(url.searchParams.get('round'), roundId);
    return json(res, 200, { ok: true, verified: { ok: true }, execution: { verified: true, checks: {} } });
  }

  if (req.method !== 'POST' || url.pathname !== '/api/paper/comp/admin') {
    return json(res, 404, { ok: false });
  }
  let raw = '';
  req.on('data', (chunk) => { raw += chunk; });
  req.on('end', () => {
    const body = JSON.parse(raw || '{}');
    actions.push(body.action);
    if (body.action === 'create') {
      roundId = body.id;
      return json(res, 200, { ok: true, round: { id: roundId } });
    }
    if (body.action === 'resetPlayers') {
      return json(res, 200, { ok: true, reset: [{ userId: 9900001 }], skipped: [{ seat: 0 }] });
    }
    if (body.action === 'preflight') {
      assert.ok(actions.indexOf('resetPlayers') < actions.indexOf('preflight'));
      return json(res, 200, { ok: true, allReady: true, marketsReady: true, seatsReady: true });
    }
    if (body.action === 'start') {
      started = true;
      return json(res, 200, { ok: true });
    }
    if (body.action === 'abort') {
      if (fixtureMode === 'blocked' && started) {
        return json(res, 400, { ok: false, error: 'round is blocked; use forceAbort with a reason' });
      }
      started = false;
      return json(res, 200, { ok: true });
    }
    if (body.action === 'forceAbort') {
      assert.equal(fixtureMode, 'blocked');
      assert.equal(body.id, roundId);
      assert.ok(typeof body.reason === 'string' && body.reason.includes('production-rehearsal cleanup'));
      forceAbortReason = body.reason;
      started = false;
      return json(res, 200, { ok: true });
    }
    return json(res, 400, { ok: false, error: 'unexpected action' });
  });
});

async function runWrapper(port) {
  const child = spawn(process.execPath, [path.join(__dirname, 'rehearse-production.js')], {
    env: {
      ...process.env,
      PAPER_GATE_SECRET: 'gate-fixture',
      PAPER_COMP_TOKEN: 'comp-fixture',
      REHEARSE_BASE: `http://127.0.0.1:${port}`,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '';
  let stderr = '';
  child.stdout.on('data', (chunk) => { stdout += chunk; });
  child.stderr.on('data', (chunk) => { stderr += chunk; });
  const code = await new Promise((resolve, reject) => {
    child.once('error', reject);
    child.once('close', resolve);
  });
  return { code, stdout, stderr };
}

(async () => {
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const { port } = server.address();
  let { code, stdout, stderr } = await runWrapper(port);
  assert.equal(code, 0, stderr || stdout);
  assert.deepEqual(actions.slice(0, 4), ['create', 'resetPlayers', 'preflight', 'start']);
  assert.ok(!actions.includes('abort'), 'cached idle state must not trigger the safety abort');
  assert.match(stdout, /"ok":true/);
  assert.match(stdout, /"drawVerified":true/);
  assert.match(stdout, /"executionVerified":true/);
  assert.match(stdout, /"distinctHotMarkets":2/);
  assert.match(stdout, /"sawProjectedBoost":true/);
  assert.match(stdout, /"sawFrozenBoost":true/);
  assert.match(stdout, /"sawBoostCapacity":true/);
  assert.match(stdout, /"boostCapacityPolicy":"frozen-start-v1"/);

  for (const mode of ['dynamic', 'dynamic-wrong-cap', 'unknown-policy']) {
    fixtureMode = mode;
    phases = makePhases();
    roundId = null;
    started = false;
    stateAfterStart = 0;
    actions.length = 0;
    ({ code, stdout, stderr } = await runWrapper(port));
    if (mode === 'dynamic') {
      assert.equal(code, 0, stderr || stdout);
      assert.match(stdout, /"sawBoostCapacity":true/);
      assert.match(stdout, /"sawFrozenBoost":false/);
      assert.match(stdout, /"boostCapacityPolicy":"current-equity-v1"/);
      assert.ok(!actions.includes('abort'));
    } else {
      assert.equal(code, 1, `${mode} must refuse certification`);
      assert.match(stderr, mode === 'unknown-policy'
        ? /unknown Boost capacity policy/ : /Boost capacity disagrees with its sealed policy/);
      assert.ok(actions.includes('abort'), 'failed certification must clean up its rehearsal');
      assert.equal(started, false);
      assert.doesNotMatch(stderr, /CLEANUP FAIL/);
    }
  }

  /* A blocked round refuses ordinary Abort. The wrapper must observe that it
     still owns the blocked slot and issue the separately audited forceAbort
     with a reason, rather than assuming an ignored `force:true` field worked. */
  fixtureMode = 'blocked';
  phases = makePhases();
  roundId = null;
  started = false;
  stateAfterStart = 0;
  forceAbortReason = null;
  actions.length = 0;
  ({ code, stdout, stderr } = await runWrapper(port));
  assert.equal(code, 1, 'the injected blocked rehearsal must report failure');
  assert.deepEqual(actions.slice(0, 4), ['create', 'resetPlayers', 'preflight', 'start']);
  assert.ok(actions.includes('abort'), 'cleanup must try ordinary Abort first');
  assert.ok(actions.includes('forceAbort'), 'blocked cleanup must use the named forceAbort action');
  assert.ok(actions.indexOf('abort') < actions.indexOf('forceAbort'));
  assert.ok(forceAbortReason && forceAbortReason.includes('fixture price fault'));
  assert.equal(started, false, 'forceAbort must release the live competition slot');
  assert.doesNotMatch(stderr, /CLEANUP FAIL/);
  console.log('rehearsal runbook regressions: 6/6 passed');
})().finally(() => server.close()).catch((error) => {
  console.error(error.stack || error.message);
  process.exitCode = 1;
});
