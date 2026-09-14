'use strict';

/* Deterministic contract tests for the current competition format. This file
 * intentionally does not preserve First Five behaviour: every newly-created
 * round is v2 and has two ordinary-asset Hot windows plus one sealed-policy
 * Boost. */
const assert = require('assert');
const crypto = require('crypto');
const { Readable } = require('stream');

process.env.PHOENIX_SNAPSHOT_FILE = '/nonexistent/two-hot-snapshot.json';
process.env.PAPER_ALLOW_UNPROVEN_MARKETS = '1';
process.env.PAPER_COMP_TOKEN = process.env.PAPER_COMP_TOKEN || 'deterministic-test-token';
if (!process.env.PAPER_DB || process.env.PAPER_DB.startsWith('/opt/')) {
  console.error('refusing to run: set PAPER_DB to a throwaway path first');
  process.exit(2);
}

const comp = require('./competition.js');
const P = require('./paper.js');
const T = P.__test;
const CT = comp.__test;
const MIN = 60_000;
let passed = 0;
let failed = 0;
let executionRoundId = null;
let protectedAuditFillId = null;
const test = async (name, fn) => {
  try { await fn(); passed++; console.log('  ok   ' + name); }
  catch (e) { failed++; console.log('  FAIL ' + name + '\n       ' + (e.stack || e.message)); }
};

const USERS = [8101, 8102, 8103, 8104, 8199];
const userIns = CT.db.prepare('INSERT OR IGNORE INTO users (id) VALUES (?)');
for (const id of USERS) { userIns.run(id); T.__ensureAccountRef()(id); }

/* Seed enough real indexed prices for the paper serializer, Boost aliases and
 * flat-roster clock-health checks. Candidate readiness itself is injected so
 * secrecy tests can use unmistakable names that cannot occur incidentally. */
function prime(sym, px = 100, at = Date.now()) {
  T.compUpdate(sym, 'usdt', px, at, at);
  T.compUpdate(sym, 'usd', px, at, at);
  T.recordMark(sym, px, at, 2, 0, 'usdt', at);
}
function primeMajors(at = Date.now()) {
  for (const s of ['BTC', 'ETH', 'SOL', 'XRP']) prime(s, 100, at);
}
primeMajors();

let readyFn = () => true;
let healthFn = () => true;
const events = [];
function wire() {
  comp.wire({
    openAlias: T.openAlias,
    closeAlias: T.closeAlias,
    aliasOpen: T.aliasOpen,
    scoreUser: T.scoreUser,
    scoreProofFor: T.scoreProofFor,
    hotValueOf: T.hotValueOf,
    segmentResidue: (alias) => ({
      positions: T.stmt.posBySymbol.all(alias).length,
      orders: T.stmt.ordOpenBySymbol.all(alias).length,
    }),
    prepareSeat: T.prepareSeat,
    seatState: T.seatState,
    markSetFor: T.markSetFor,
    equityOf: (uid) => T.accountRisk(uid, T.stmt.acctGet.get(uid)).equityTotal,
    indexedSymbol: () => true,
    marketReady: (sym) => !!readyFn(String(sym), 'HOT'),
    marketReadyAt: (sym, at, kind, requiredLeverage) =>
      !!readyFn(String(sym), kind, at, requiredLeverage),
    marketReadyForBoost: (sym, requiredLeverage) =>
      !!readyFn(String(sym), 'BOOST', null, requiredLeverage),
    marketEvidenceAt: (sym, at, kind, requiredLeverage) => {
      const checkedAt = Number(at);
      const leverage = kind === 'BOOST' ? (Number(requiredLeverage) || 500) : comp.COMP_BASE_LEV;
      const ready = !!readyFn(String(sym), kind, checkedAt, leverage);
      return {
        ready, historicalReady: ready,
        liveReady: !!readyFn(String(sym), kind, null, leverage),
        checkedAt, liveCheckedAt: Date.now(), boundaryMark: ready ? 100 : null,
        rejectReason: ready ? null : 'test observation unavailable at activation',
        policy: {
          version: 'competition-price-v2', kind, buildId: null,
          leverageRequired: leverage,
          maximumHistoricalAgeMs: 30_000,
        },
        invalidity: ready ? null : {
          since: checkedAt, reason: 'test observation unavailable at activation',
        },
        observation: ready ? {
          mark: 100, observedAt: checkedAt - 1, validUntil: checkedAt + 60_000,
          accepted: true, source: 'deterministic-test', sourceAt: checkedAt - 1,
          components: 2, spreadBps: 0, leverageCap: Math.max(500, leverage),
          freshComponents: 2, venues: 2, maximumComponentAgeMs: 1,
        } : null,
      };
    },
    marketAvailability: (sym, kind, at, requiredLeverage) => ({
      ready: !!readyFn(String(sym), kind, at, requiredLeverage),
      invalidSince: Number(at) || Date.now(),
      validUntil: (Number(at) || Date.now()) + 60_000,
    }),
    historicalAvailabilityAt: (sym, kind, at, requiredLeverage) => ({
      ready: !!readyFn(String(sym), kind, Number(at), requiredLeverage),
      invalidSince: Number(at) || Date.now(),
      validUntil: (Number(at) || Date.now()) + 60_000,
    }),
    marketReliability: () => ({ ratio: 1, samples: 1000, spanMs: 600_000, longestGapMs: 0, unknownMs: 0 }),
    boostLeverage: () => 500,
    boostLevCap: () => 500,
    ensureClockHealth: (at) => healthFn(Number(at) || Date.now()),
    pauseForBoundary: (since) => comp.pauseClockOpen(Number(since) || Date.now()),
    pauseForRestart: (_why, since) => comp.pauseClockOpen(Number(since) || Date.now()),
    pauseForSegment: (_sym, _kind, _why, since) => comp.pauseClockOpen(Number(since) || Date.now()),
    closeRoundPauses: () => {},
    onPhase: (event, payload) => events.push({ event, payload }),
    log: () => {},
  });
}
wire();

const roster = (uid = USERS[0]) => [{ userId: uid, displayName: 'Player', seat: 0 }];
const abortLive = () => {
  const r = comp.currentRound();
  if (r) {
    try { comp.abortRound(r.id, { force: true }); } catch {}
  }
  T.__clearPauses();
};
function create(id, opts = {}) {
  // Historical-policy fixtures declare their contract before its immutable
  // seal; all other ordinary rounds exercise the production new default.
  comp.wire({ defaultBoostCapacityPolicy: () => opts.boostCapacityPolicy || 'current-equity-v1',
    // Existing fixtures intentionally retain the deployed pre-backup policy.
    defaultBackupExecutionPolicy: () => opts.backupExecutionPolicy || null });
  try { return comp.createRound({
    id, kind: opts.kind || 'round', speed: opts.speed || 1,
    candidates: opts.candidates || ['ALPHA', 'BETA', 'GAMMA'],
    players: opts.players || roster(opts.uid || USERS[0]),
  }); } finally { comp.wire({ defaultBoostCapacityPolicy: () => 'current-equity-v1',
    defaultBackupExecutionPolicy: () => 'latest-available-500-v1' }); }
}
function start(id) {
  primeMajors();
  return comp.startRound(id);
}
async function withDynamicBoostFixture(id, run) {
  const uid = USERS[3];
  const r = create(id, { candidates: ['BTC', 'ETH', 'SOL'], uid });
  start(r.id);
  const originalEntry = T.live.map.get('BTC');
  const originalPrices = { pythPrice: originalEntry.pythPrice, markPrice: originalEntry.markPrice };
  const hotBonusFor = comp.hotBonusFor;
  try {
    comp.freezeBoostBankroll(r.id, Date.now());
    for (const base of ['BTC', 'SOL']) T.openAlias(base + '-BOOST', r.id);
    const reference = CT.q.boostProofs.all(r.id);
    assert.strictEqual(reference.length, 1);
    assert.strictEqual(reference[0].max_exposure, 5000);
    const mark = (price) => {
      assert.strictEqual(T.live.map.get('BTC'), originalEntry);
      originalEntry.pythPrice = price; originalEntry.markPrice = price;
    };
    const fill = (symbol, size, orderSide = 'BUY') => T.applyFill(uid, {
      symbol, orderSide, size, px: symbol === 'BTC-BOOST' ? originalEntry.pythPrice : 100,
      feeBps: 0, kind: 'MARKET', leverage: 500, marginMode: 'isolated',
      executionSource: 'test', referenceMark: symbol === 'BTC-BOOST' ? originalEntry.pythPrice : 100,
    });
    await run({ r, uid, mark, fill, reference });
    assert.deepStrictEqual(CT.q.boostProofs.all(r.id), reference, 'current-equity updates never rewrite the start reference');
  } finally {
    comp.hotBonusFor = hotBonusFor;
    assert.strictEqual(T.live.map.get('BTC'), originalEntry);
    Object.assign(originalEntry, originalPrices);
    try {
      // This fixture starts flat and may add its own ordinary same-direction
      // cross leg for precision coverage. It must not escape into later tests.
      const basePosition = T.stmt.posGet.get(uid, 'BTC');
      if (basePosition) {
        assert.strictEqual(basePosition.side, 'LONG');
        T.applyFill(uid, { symbol: 'BTC', orderSide: 'SELL', size: basePosition.size,
          px: originalEntry.pythPrice, feeBps: 0, kind: 'MARKET', leverage: basePosition.leverage,
          marginMode: 'cross', executionSource: 'test', referenceMark: originalEntry.pythPrice });
      }
      assert.strictEqual(T.stmt.posGet.get(uid, 'BTC'), undefined);
      for (const alias of ['BTC-BOOST', 'SOL-BOOST']) {
        T.closeAlias(alias, { flatten: true, roundId: r.id });
        assert.strictEqual(T.aliasOpen(alias), false);
        assert.deepStrictEqual(T.stmt.posBySymbol.all(alias), []);
        assert.deepStrictEqual(T.stmt.ordOpenBySymbol.all(alias), []);
      }
    } finally { abortLive(); }
  }
}
function setActiveOffset(id, offset, at = Date.now()) {
  CT.db.prepare('UPDATE paper_rounds SET started_at = ?, ends_at = ? WHERE id = ?')
    .run(at - offset, at - offset + comp.planOf(CT.q.get.get(id)).total, id);
}

function response() {
  return new Promise((resolve) => {
    const res = {
      writeHead(code, headers) { this.code = code; this.headers = headers; },
      end(raw) { resolve({ code: this.code, headers: this.headers, body: JSON.parse(raw) }); },
    };
    res.promise = null;
    resolve.res = res;
  });
}
async function invoke(handler, { body = null, url = '/', headers = {} } = {}) {
  let finish;
  const done = new Promise((resolve) => { finish = resolve; });
  const req = Readable.from(body == null ? [] : [JSON.stringify(body)]);
  req.url = url;
  req.headers = { 'x-real-ip': '127.0.0.77', ...headers };
  req.socket = { remoteAddress: '127.0.0.77' };
  const res = {
    writeHead(code, hs) { this.code = code; this.headers = hs; },
    end(raw) { finish({ code: this.code, headers: this.headers, body: JSON.parse(raw) }); },
  };
  const out = handler.length >= 3
    ? handler(req, res, new URL(url, 'http://test'))
    : handler(req, res);
  if (out && typeof out.then === 'function') await out;
  return done;
}

/* Storage-only policy fixtures. These never start a round, call init, open a
 * network listener, or borrow mutable production data. The existing suite's
 * throwaway DB and a synchronous clock keep every timestamp deterministic. */
const LAST_ACCEPTED_POLICY = 'last-accepted-v1';
function withRoundMarkFixture(id, run, opts = {}) {
  const previousNow = Date.now;
  const clock = { now: 1_800_000_000_000 };
  let round = null;
  Date.now = () => clock.now;
  comp.wire({ defaultPricePolicy: () => LAST_ACCEPTED_POLICY,
    initializeRoundMarks: () => {} });
  try {
    round = create(id, { candidates: ['BTC', 'ETH', 'SOL'], ...opts });
    let boot = null;
    P.engineTime({}, { writeHead() {}, end(raw) { boot = JSON.parse(raw).boot; } });
    assert.ok(typeof boot === 'string' && boot.length > 0);
    const record = (sequence, at, patch = {}) => ({
      base: 'BTC', price: 100 + sequence / 1000, acceptedBoot: boot,
      acceptedSeq: sequence, acceptedAt: at, observedAt: at - 5, appliedAt: at,
      source: 'lazer', originalValidUntil: at + 600, acceptedLeverageCap: 500,
      ...patch,
    });
    const identity = (row) => row
      ? { acceptedBoot: row.acceptedBoot, acceptedSeq: row.acceptedSeq } : null;
    const commit = (row, expected) => CT.db.transaction(() =>
      comp.roundMarkCommit(round.id, row, expected))();
    return run({ round, clock, record, identity, commit });
  } finally {
    // Armed fixtures own no aliases/accounts. Mark them terminal without
    // invoking unrelated clock/lifecycle integration in this storage slice.
    try {
      if (round) CT.q.setStatus.run('aborted', clock.now, round.id);
    } finally {
      comp.wire({ defaultPricePolicy: () => 'strict', initializeRoundMarks: null });
      Date.now = previousNow;
    }
  }
}

(async () => {
  console.log('\ntwo-Hot draw and phase contract');
  await test('the narrow write-barrier read observes a block or settlement committed by its clock pass', () => {
    for(const state of ['blocked','settled']) {
      const r=create('barrier-state-'+state,{uid:USERS[0],candidates:['BTC','ETH','SOL']});
      start(r.id);
      const priorHealth=healthFn;
      let applied=false;
      healthFn=()=>{
        if(!applied) {
          applied=true;
          if(state==='blocked') CT.q.setBlocked.run('fixture clock block',Date.now(),r.id);
          else CT.q.setStatus.run('done',Date.now(),r.id);
        }
        return true;
      };
      try {
        CT.db.transaction(()=>{
          assert.strictEqual(comp.writeBarrier(USERS[0]),state==='blocked'?'round_blocked':'round_settled');
          assert.strictEqual(CT.q.writeState.get(r.id).status,state==='blocked'?'running':'done');
        })();
        assert.strictEqual(applied,true);
      } finally {
        healthFn=priorHealth;
        if(state==='settled') CT.q.setStatus.run('running',Date.now(),r.id);
        abortLive();
      }
    }
  });

  await test('terminal checkpoint counts stay fresh and retain joined-roster semantics', () => {
    const players = [USERS[0], USERS[1]].map((userId, seat) => ({userId, seat, displayName:'Count '+seat}));
    const r = create('checkpoint-count-query', {players, candidates:['BTC','ETH','SOL']});
    start(r.id);
    assert.strictEqual(comp.terminalCleanupPending({...comp.currentRound(), id:'no-roster'}), false);
    const isComplete = () => comp.terminalCleanupPending(comp.currentRound());
    const score = (uid, checkpoint='final') => CT.q.scoreIns.run(r.id, uid, checkpoint,
      Date.now(), 10, 0, 0, 0, 0);
    assert.strictEqual(isComplete(), false);
    score(USERS[0], 'other');
    assert.strictEqual(isComplete(), false, 'another checkpoint is not the final checkpoint');
    score(USERS[0]);
    score(USERS[4]);
    assert.strictEqual(isComplete(), false, 'a score for somebody outside the roster cannot complete it');
    assert.throws(() => CT.db.transaction(() => {
      score(USERS[1]);
      assert.strictEqual(isComplete(), true, 'same-transaction final score must be visible');
      throw new Error('fixture rollback');
    })(), /fixture rollback/);
    assert.strictEqual(isComplete(), false, 'rolled-back completion must not survive');
    score(USERS[1]);
    assert.strictEqual(isComplete(), true);
    CT.db.prepare('DELETE FROM paper_round_scores WHERE round_id = ? AND user_id = ?')
      .run(r.id, USERS[1]);
    assert.strictEqual(isComplete(), false, 'a deleted checkpoint must be noticed immediately');
    abortLive();
  });

  await test('joined active-clock exposure preserves leverage, aliases and immediate position changes', () => {
    const players = USERS.slice(0,3).map((userId,seat) => ({userId,seat,displayName:'Exposure '+seat}));
    const r = create('clock-exposure-query', {players,candidates:['BTC','ETH','SOL']});
    start(r.id);
    const fill = (uid, symbol, leverage) => T.applyFill(uid, {symbol,orderSide:'BUY',
      size:0.1,px:100,feeBps:0,kind:'MARKET',leverage,marginMode:'isolated',
      executionSource:'test',referenceMark:100});
    fill(USERS[0],'BTC',100); fill(USERS[1],'BTC',100);
    fill(USERS[4],'SOL',100);
    assert.deepStrictEqual(T.stmt.clockExposure.all(r.id), [{symbol:'BTC',leverage:100}],
      'equal dependencies collapse, and an outsider does not join the clock');
    CT.db.prepare('UPDATE paper_positions SET leverage = 200 WHERE user_id = ? AND symbol = ?')
      .run(USERS[1], 'BTC');
    CT.db.prepare(`UPDATE paper_round_players SET boost_bankroll = 10,
      boost_max_exposure = 5000, boost_frozen_at = ? WHERE round_id = ? AND user_id = ?`)
      .run(Date.now(),r.id,USERS[2]);
    T.openAlias('BTC-BOOST',r.id); fill(USERS[2],'BTC-BOOST',500);
    const deps = () => T.stmt.clockExposure.all(r.id).map((p) => p.symbol+':'+p.leverage).sort();
    assert.deepStrictEqual(deps(), ['BTC-BOOST:500','BTC:100','BTC:200']);
    const originalPositions = T.stmt.posByUser.all;
    const originalPlayers = CT.q.players.all;
    let wideReads = 0;
    T.stmt.posByUser.all = (...args) => { wideReads++; return originalPositions.apply(T.stmt.posByUser,args); };
    CT.q.players.all = (...args) => { wideReads++; return originalPlayers.apply(CT.q.players,args); };
    try {
      assert.strictEqual(T.__competitionClockStatus().ok, true);
      assert.strictEqual(wideReads,0,'clock health must not deserialize full roster or per-seat portfolios');
    } finally { T.stmt.posByUser.all=originalPositions; CT.q.players.all=originalPlayers; }
    const sol = T.live.map.get('SOL'); T.live.map.delete('SOL');
    try { assert.strictEqual(T.__competitionClockStatus().ok, true, 'outsider-only stale position must not pause the field'); }
    finally { T.live.map.set('SOL',sol); }
    const btc = T.live.map.get('BTC'); T.live.map.delete('BTC');
    try { assert.strictEqual(T.__competitionClockStatus().ok, false, 'held underlying remains a required fresh dependency'); }
    finally { T.live.map.set('BTC',btc); }
    CT.db.prepare('DELETE FROM paper_positions WHERE user_id = ? AND symbol = ?')
      .run(USERS[2],'BTC-BOOST');
    assert.deepStrictEqual(deps(), ['BTC:100','BTC:200'], 'closed exposure disappears without a TTL');
    // This fixture opened a gate directly, outside the durable Boost plan.
    // It owns that gate's cleanup; aborting the round must not be expected to
    // infer an opening that was never recorded in boost_opened.
    T.closeAlias('BTC-BOOST', { flatten: false, roundId: r.id });
    assert.strictEqual(T.aliasOpen('BTC-BOOST'), false);
    assert.strictEqual(T.openAliases.has('BTC-BOOST'), false);
    abortLive();
    CT.db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(USERS[4]);
  });

  await test('the maintenance probe avoids missing-file exceptions without caching or failing open', () => {
    const fs = require('fs');
    const originalStat = fs.statSync;
    let calls = 0;
    let outcome = undefined;
    fs.statSync = (_path, options) => {
      calls++;
      assert.deepStrictEqual(options, { throwIfNoEntry: false });
      if (outcome instanceof Error) throw outcome;
      return outcome;
    };
    try {
      assert.strictEqual(P.deploymentMaintenanceActive(), false, 'ENOENT is represented by undefined');
      outcome = {};
      assert.strictEqual(P.deploymentMaintenanceActive(), true, 'a newly present barrier applies immediately');
      outcome = undefined;
      assert.strictEqual(P.deploymentMaintenanceActive(), false, 'removal is read again, never cached');
      for (const code of ['ENOTDIR', 'EACCES', 'EIO']) {
        outcome = Object.assign(new Error(code), { code });
        assert.strictEqual(P.deploymentMaintenanceActive(), true, code + ' must fail closed');
      }
      outcome = Object.assign(new Error('ENOENT'), { code: 'ENOENT' });
      assert.strictEqual(P.deploymentMaintenanceActive(), false, 'legacy ENOENT semantics are preserved');
      assert.strictEqual(calls, 7, 'every invocation must reach the filesystem');
    } finally { fs.statSync = originalStat; }
  });
  await test('an idle clock with no expiry never schedules a 1ms timer loop', () => {
    assert.strictEqual(comp.currentRound(), null);
    const original = global.setTimeout;
    let scheduled = 0;
    global.setTimeout = (...args) => { scheduled++; return original(...args); };
    try { T.__armCompetitionClockExpiry(); }
    finally { global.setTimeout = original; }
    assert.strictEqual(scheduled, 0);
    assert.strictEqual(T.__competitionClockExpiryArmed(), false);
  });
  await test('a reset rechecks round ownership after the request body yields', async () => {
    const auth = require('./auth-shim.js');
    const originalSession = auth.validateSession;
    const uid = USERS[4];
    const before = T.stmt.acctGet.get(uid);
    let armed = null;
    const req = {
      headers: {},
      on(event, callback) {
        if (event === 'data') callback(Buffer.from('{"mode":"heat"}'));
        if (event === 'end') {
          armed = create('reset-body-ownership', { uid, candidates: ['BTC', 'ETH', 'SOL'] });
          callback();
        }
        return this;
      },
    };
    const res = {
      writeHead(code) { this.code = code; },
      end(raw) { this.body = JSON.parse(raw); },
    };
    auth.validateSession = async () => ({ id: uid });
    try {
      await P.reset(req, res);
      assert.strictEqual(res.code, 409);
      assert.strictEqual(res.body.error, 'in_competition_round');
      assert.deepStrictEqual(T.stmt.acctGet.get(uid), before,
        'the newly owned account must not change mode, balance or epoch');
    } finally {
      auth.validateSession = originalSession;
      if (armed) comp.abortRound(armed.id, { force: true });
    }
  });
  await test('the final is a 20-minute plan with the same beats and full-length Hot and Boost', () => {
    const r = comp.ROUND_PLAN.round, f = comp.ROUND_PLAN.final;
    assert.deepStrictEqual(Object.keys(f).sort(), Object.keys(r).sort());
    assert.strictEqual(r.total, 30 * 60_000);
    assert.strictEqual(f.total, 20 * 60_000);
    assert.strictEqual(f.hotDuration, r.hotDuration);
    assert.strictEqual(f.total - f.boostStart, r.total - r.boostStart, 'Boost keeps its three minutes');
    // Beats stay ordered and Hot #2 can never run into the Final Build.
    assert.ok(f.buildEnd < f.hot1WindowStart && f.hot1WindowStart < f.hot1WindowEnd);
    assert.ok(f.hot1WindowEnd + f.hotDuration + f.normalGap + f.hotWarning <= f.hot2WindowEnd);
    assert.ok(f.hot2WindowStart < f.hot2WindowEnd && f.hot2WindowEnd + f.hotDuration <= f.finalBuildStart);
    assert.ok(f.finalBuildStart < f.boostStart && f.boostStart < f.total);
    const p = comp.ROUND_PLAN.round;
    assert.strictEqual(p.buildEnd, 3 * MIN);
    assert.strictEqual(p.finalBuildStart, 22 * MIN);
    assert.strictEqual(p.boostStart, 27 * MIN);
    assert.strictEqual(p.total, 30 * MIN);
    assert.strictEqual(p.hotDuration, 2 * MIN);
    assert.strictEqual(p.hotWarning, 15_000);
  });

  await test('thousands of seeded draws satisfy both windows, gap, finish and distinctness', () => {
    const p = comp.ROUND_PLAN.round;
    for (let i = 0; i < 4000; i++) {
      const d = CT.makeHotDraw({ candidates: ['A', 'B', 'C', 'D'], seed: `seed-${i}`, plan: p });
      assert.ok(d.hot1.activation >= p.hot1WindowStart && d.hot1.activation <= p.hot1WindowEnd);
      assert.ok(d.hot2.activation >= p.hot2WindowStart && d.hot2.activation <= p.hot2WindowEnd);
      assert.notStrictEqual(d.hot1.asset, d.hot2.asset);
      assert.ok(d.hot2.activation - p.hotWarning >= d.hot1.activation + p.hotDuration + p.normalGap);
      assert.ok(d.hot2.activation + p.hotDuration <= p.finalBuildStart);
      assert.strictEqual(new Set([...d.backupOrder, d.hot1.asset, d.hot2.asset]).size, 4);
      assert.deepStrictEqual(d.hot1.fallbackOrder.slice(0, d.backupOrder.length), d.backupOrder);
      assert.deepStrictEqual(d.hot2.fallbackOrder.slice(0, d.backupOrder.length), d.backupOrder);
      assert.strictEqual(d.hot1.fallbackOrder.at(-1), d.hot2.asset);
      assert.strictEqual(d.hot2.fallbackOrder.at(-1), d.hot1.asset);
    }
  });

  await test('Hot #2 ending exactly at 22:00 coalesces close and Final Build', () => {
    const p = comp.ROUND_PLAN.round;
    let draw = null;
    for (let i = 0; i < 2_000_000 && !draw; i++) {
      const d = CT.makeHotDraw({ candidates: ['A', 'B', 'C'], seed: `edge-${i}`, plan: p });
      if (d.hot2.activation === p.hot2WindowEnd) draw = d;
    }
    assert.ok(draw, 'expected to find a deterministic max-end seed');
    const row = { format_version: 2, kind: 'round', plan_json: JSON.stringify(p), hot_draw_json: JSON.stringify(draw) };
    const edge = CT.v2BoundaryAt(row, p.finalBuildStart);
    assert.deepStrictEqual(edge, { kind: 'hotEnd', hotNumber: 2, startsFinalBuild: true });
  });

  await test('v2 rejects aliases and requires a real committed backup', () => {
    assert.throws(() => comp.createRound({ id: 'bad-two', candidates: ['BTC', 'SOL'], players: roster() }), /at least 3/);
    assert.throws(() => comp.createRound({ id: 'bad-alias', candidates: ['BTC-HOT', 'SOL', 'ETH'], players: roster() }), /ordinary assets/);
    assert.throws(() => comp.createRound({ id: 'bad-alias2', candidates: ['BTC-BOOST', 'SOL', 'ETH'], players: roster() }), /ordinary assets/);
  });

  await test('official round/final cannot be accelerated; rehearsal remains explicit', () => {
    assert.throws(() => comp.createRound({ id: 'bad-fast-round', kind: 'round', speed: 30,
      candidates: ['BTC', 'SOL', 'ETH'], players: roster() }), /official round.*real-time speed/i);
    assert.throws(() => comp.createRound({ id: 'bad-fast-final', kind: 'final', speed: 5,
      candidates: ['BTC', 'SOL', 'ETH'], players: roster() }), /official round and final.*real-time speed/i);
    const practice = create('explicit-fast-rehearsal', { kind: 'rehearsal', speed: 30 });
    assert.strictEqual(practice.speed, 30);
    comp.abortRound(practice.id, { force: true });
  });

  console.log('\nsealed public/admin contract');
  let secretRound;
  await test('armed/live build and preflight never serialize private draw material', async () => {
    secretRound = create('sealed-public', { candidates: ['ALPHA', 'BETA', 'GAMMA'] });
    const draw = CT.privateDrawOf(secretRound);
    const armed = await invoke(P.compBaseline);
    assert.strictEqual(armed.code, 200);
    assert.ok(!JSON.stringify(armed.body).includes(secretRound.hot_draw_secret));
    assert.ok(!JSON.stringify(armed.body).includes(draw.hot1.asset));
    assert.strictEqual(armed.body.armed.find((x) => x.id === secretRound.id).drawCommit, undefined);

    start(secretRound.id);
    const live = await invoke(P.compBaseline);
    assert.strictEqual(live.body.phase, 'build');
    assert.strictEqual(live.body.phaseEndsAt, null);
    assert.strictEqual(live.body.phaseLeftMs, null);
    assert.strictEqual(live.body.round.drawCommit, secretRound.draw_commit);
    const raw = JSON.stringify(live.body);
    assert.ok(!raw.includes(secretRound.hot_draw_secret));
    for (const asset of ['ALPHA', 'BETA', 'GAMMA']) assert.ok(!raw.includes(asset));

    const preflight = await invoke(P.compAdmin, {
      body: { action: 'preflight', id: secretRound.id },
      headers: { 'x-comp-token': process.env.PAPER_COMP_TOKEN || 'deterministic-test-token' },
    });
    assert.strictEqual(preflight.code, 200);
    const adminRaw = JSON.stringify(preflight.body);
    assert.ok(!adminRaw.includes(secretRound.hot_draw_secret));
    for (const asset of ['ALPHA', 'BETA', 'GAMMA']) assert.ok(!adminRaw.includes(asset));

    const verify = await invoke(P.compVerify, { url: `/verify?round=${secretRound.id}` });
    assert.strictEqual(verify.code, 409);
    assert.strictEqual(verify.body.commit, secretRound.draw_commit);
    assert.strictEqual(verify.body.reveal, undefined);
  });

  await test('generic warning exposes its ordinal but no asset or next secret', async () => {
    const r = CT.q.get.get(secretRound.id);
    const d = CT.privateDrawOf(r);
    const warning = d.hot1.activation - comp.planOf(r).hotWarning;
    CT.q.bMark.run(r.id, warning, 'succeeded', null, Date.now());
    setActiveOffset(r.id, warning + 500);
    const state = await invoke(P.compBaseline);
    assert.strictEqual(state.body.phase, 'hotWarning');
    assert.strictEqual(state.body.hotNumber, 1);
    assert.strictEqual(state.body.hot, null);
    assert.deepStrictEqual(state.body.hots, []);
    const raw = JSON.stringify(state.body);
    for (const asset of ['ALPHA', 'BETA', 'GAMMA']) assert.ok(!raw.includes(asset));
    abortLive();
  });

  await test('the committed envelope is immutable and verifies only after settlement reveal', () => {
    const r = create('commit-reveal');
    assert.throws(() => CT.db.prepare('UPDATE paper_rounds SET hot_candidates = ? WHERE id = ?')
      .run(JSON.stringify(['X', 'Y', 'Z']), r.id), /immutable/);
    const draw = CT.privateDrawOf(r);
    const envelope = CT.revealEnvelope({
      id: r.id, kind: r.kind, speed: r.speed,
      candidates: JSON.parse(r.hot_candidates), seed: r.hot_draw_secret,
      plan: comp.planOf(r), draw,
      boostCapacityPolicy: comp.boostCapacityPolicyOf(r),
    });
    CT.db.prepare("UPDATE paper_rounds SET status = 'done', draw_reveal_json = ?, ends_at = ?, updated_at = ? WHERE id = ?")
      .run(JSON.stringify(envelope), Date.now(), Date.now(), r.id);
    const done = CT.q.get.get(r.id);
    assert.deepStrictEqual(comp.verifyDraw(done), { ok: true, draw });
    const altered = JSON.parse(JSON.stringify(envelope));
    altered.draw.hot1.activation++;
    assert.notStrictEqual(CT.v2Commit({
      id: altered.roundId, kind: altered.kind, speed: altered.speed,
      candidates: altered.candidates, seed: altered.seed, plan: altered.plan, draw: altered.draw,
    }), r.draw_commit);
  });

  await test('preflight requires the promised Boost markets at the 500x price tier', () => {
    readyFn = (sym, kind) => kind !== 'BOOST' || sym !== 'XRP';
    const r = create('boost-tier-preflight', { candidates: ['BTC', 'SOL', 'ETH'] });
    const verdict = comp.marketReadiness(r.id);
    assert.strictEqual(verdict.ok, false);
    const xrp = verdict.markets.find((m) => m.symbol === 'XRP');
    assert.ok(xrp && xrp.hotPriceReady && !xrp.boostPriceReady && !xrp.priceReady);
    assert.throws(() => comp.startRound(r.id), /not competition-ready/);
    comp.abortRound(r.id, { force: true });
    readyFn = () => true;
  });

  await test('started abort reveals its locked draw; armed abort stays sealed and non-live state has server time', async () => {
    const armed = create('sealed-armed-abort');
    comp.abortRound(armed.id, { force: true });
    const sealed = await invoke(P.compVerify, { url: `/verify?round=${armed.id}` });
    assert.strictEqual(sealed.code, 409);
    assert.strictEqual(sealed.body.commit, null);

    const started = create('revealed-started-abort');
    start(started.id);
    comp.abortRound(started.id, { force: true });
    const revealed = await invoke(P.compVerify, { url: `/verify?round=${started.id}` });
    assert.strictEqual(revealed.code, 200);
    assert.strictEqual(revealed.body.canonical.matches, true);
    assert.strictEqual(revealed.body.execution.verified, false);
    const state = await invoke(P.compBaseline);
    assert.strictEqual(state.body.live, false);
    assert.ok(Number.isFinite(state.body.now));
    assert.strictEqual(state.body.lastRound.aborted, true);
    assert.strictEqual(state.body.lastRound.drawVerified.ok, true);
    assert.strictEqual(state.body.lastRound.executionVerified.ok, false);
  });

  await test('post-settlement verification proves measured active windows and boundary ledger', async () => {
    readyFn = () => true;
    healthFn = () => true;
    const armed = create('execution-proof', { kind: 'rehearsal', speed: 30 });
    const running = start(armed.id);
    executionRoundId = running.id;
    for (const at of comp.boundariesOf(running)) {
      CT.fireBoundary(running.id, at, running.started_at + at);
      const row = CT.q.bGet.get(running.id, at);
      assert.ok(row && row.status === 'succeeded', `boundary ${at} did not commit: ${row?.error || 'no recorded error'}`);
    }
    assert.strictEqual(CT.q.get.get(running.id).status, 'done');
    assert.strictEqual(CT.q.bGet.get(running.id, comp.planOf(running).total).status, 'succeeded',
      'done status and bell boundary must share one durable edge');
    const verify = await invoke(P.compVerify, { url: `/verify?round=${running.id}` });
    assert.strictEqual(verify.code, 200);
    assert.strictEqual(verify.body.canonical.matches, true);
    assert.strictEqual(verify.body.execution.verified, true,
      JSON.stringify(verify.body.execution.checks));
    assert.strictEqual(verify.body.reveal.boostCapacityPolicy, 'current-equity-v1');
    assert.strictEqual(verify.body.execution.boost.capacityPolicy, 'current-equity-v1');
    assert.strictEqual(verify.body.execution.boost.snapshotRole, 'audit-reference');
    for (const row of CT.q.scoreProofs.all(running.id)) {
      const proof = JSON.parse(row.proof_json);
      assert.strictEqual(proof.boostCapacityPolicy, 'current-equity-v1');
      assert.strictEqual(proof.state.boostCapacityPolicy, 'current-equity-v1');
    }
    assert.strictEqual(verify.body.execution.hots.length, 2);
    for (const hot of verify.body.execution.hots) {
      assert.strictEqual(hot.activeDurationMs, comp.planOf(running).hotDuration);
      assert.strictEqual(hot.timestampsMatchBoundaries, true);
    }
    assert.strictEqual(verify.body.execution.boost.activeDurationMs,
      comp.planOf(running).total - comp.planOf(running).boostStart);
    assert.strictEqual(verify.body.execution.clock.totalActiveDurationMs,
      comp.planOf(running).total);
    for (const check of ['scoreInputCheckpointsImmutable', 'scoreInputStateRecomputed',
      'fillLedgerDigestsMatchCheckpoints', 'hotEconomicDeltasRecomputed',
      'boostEquityRecomputedFromCheckpoint', 'finalEquityRecomputedFromCheckpoint']) {
      assert.strictEqual(verify.body.execution.checks[check], true, check);
    }
    assert.strictEqual(verify.body.execution.scoreProofs.length,
      CT.q.players.all(running.id).length * 6);
  });

  await test('settled summary reuses proof work but never hides writes or caller mutation', async () => {
    const id = executionRoundId;
    const at = Date.now();
    const first = T.__settledExecutionSummary(id, at);
    assert.strictEqual(first.ok, true);
    const cached = T.__settledExecutionCache();
    first.ok = false;
    assert.deepStrictEqual(T.__settledExecutionSummary(id, at + 1), { ok: true });
    assert.strictEqual(T.__settledExecutionCache().misses, cached.misses);
    assert.strictEqual(T.__settledExecutionCache().hits, cached.hits + 1);
    CT.db.prepare('UPDATE paper_accounts SET updated_at = updated_at WHERE user_id = ?').run(USERS[0]);
    assert.strictEqual(T.__settledExecutionSummary(id, at + 2).ok, true);
    assert.strictEqual(T.__settledExecutionCache().misses, cached.misses,
      'ordinary live account/mark writes are not immutable proof inputs');
    const plan = comp.planOf(CT.q.get.get(id));
    const bell = CT.q.bGet.get(id, plan.total);
    const moveBell = CT.db.prepare('UPDATE paper_round_boundaries SET due_wall_at = ? WHERE round_id = ? AND at = ?');
    try {
      moveBell.run(bell.due_wall_at + 90, id, plan.total);
      assert.strictEqual(T.__settledExecutionSummary(id).ok, false);
      assert.strictEqual(T.__settledExecutionCache().size, 0, 'negative proof is not cached');
    } finally { moveBell.run(bell.due_wall_at, id, plan.total); }
    assert.strictEqual(T.__settledExecutionSummary(id).ok, true);

    const Database = require('better-sqlite3');
    const other = new Database(process.env.PAPER_DB);
    try {
      const update = other.prepare('UPDATE paper_round_boundaries SET due_wall_at = ? WHERE round_id = ? AND at = ?');
      update.run(bell.due_wall_at + 90, id, plan.total);
      const verified = await invoke(P.compVerify, { url: `/verify?round=${id}` });
      assert.strictEqual(verified.body.execution.verified, false,
        'the explicit verifier never reads the summary cache');
      assert.strictEqual(T.__settledExecutionSummary(id).ok, false,
        'another connection must invalidate through data_version');
      update.run(bell.due_wall_at, id, plan.total);
      assert.strictEqual(T.__settledExecutionSummary(id).ok, true);
    } finally {
      other.prepare('UPDATE paper_round_boundaries SET due_wall_at = ? WHERE round_id = ? AND at = ?')
        .run(bell.due_wall_at, id, plan.total);
      other.close();
    }

    const revision = T.__executionDataVersionStatement();
    const originalGet = revision.get;
    revision.get = () => { throw new Error('deterministic revision read fault'); };
    try {
      assert.strictEqual(T.__settledExecutionSummary(id).ok, false);
      assert.strictEqual(T.__settledExecutionCache().size, 0,
        'revision failure may not preserve an earlier green proof');
    } finally { revision.get = originalGet; }
    assert.strictEqual(T.__settledExecutionSummary(id, at + 10).ok, true);
    const beforeExpiry = T.__settledExecutionCache().misses;
    assert.strictEqual(T.__settledExecutionSummary(id, at + 60_010).ok, true);
    assert.strictEqual(T.__settledExecutionCache().misses, beforeExpiry + 1);
    assert.ok(T.__settledExecutionCache().size <= T.__settledExecutionCache().limit);
  });

  await test('execution proof fails closed on boundary, pause, resolution or Boost-cap tampering', async () => {
    const r = CT.q.get.get(executionRoundId);
    const plan = comp.planOf(r);
    const verify = async () => invoke(P.compVerify, { url: `/verify?round=${r.id}` });
    assert.throws(() => CT.db.prepare(`UPDATE paper_round_scores
      SET score = score + 1, account_pnl = account_pnl + 1
      WHERE round_id = ? AND checkpoint = 'final'`).run(r.id), /immutable/);
    assert.throws(() => CT.db.prepare(`UPDATE paper_round_hot_scores
      SET bonus = bonus + 1 WHERE round_id = ?`).run(r.id), /immutable/);
    assert.throws(() => CT.db.prepare(`UPDATE paper_round_score_proofs
      SET proof_sha256 = 'forged' WHERE round_id = ?`).run(r.id), /immutable/);
    const sealedPlayer = CT.q.players.all(r.id)[0];
    const sealedBell = CT.q.bGet.get(r.id, plan.total);
    assert.throws(() => CT.db.prepare(`INSERT INTO paper_fills
      (user_id, epoch, symbol, side, kind, price, size, notional, fee,
       realized_pnl, order_id, ts, bad_debt)
      VALUES (?, ?, 'BTC', 'BUY', 'MARKET', 100, 0.01, 1, 0, NULL, NULL, ?, 0)`)
      .run(sealedPlayer.user_id, sealedPlayer.epoch, sealedBell.due_wall_at),
    /cannot backdate/);
    const bell = CT.q.bGet.get(r.id, plan.total);
    CT.db.prepare('UPDATE paper_round_boundaries SET due_wall_at = due_wall_at + 90 WHERE round_id = ? AND at = ?')
      .run(r.id, plan.total);
    assert.strictEqual((await verify()).body.execution.verified, false);
    CT.db.prepare('UPDATE paper_round_boundaries SET due_wall_at = ? WHERE round_id = ? AND at = ?')
      .run(bell.due_wall_at, r.id, plan.total);

    CT.db.prepare('UPDATE paper_rounds SET paused_ms = paused_ms + 12345 WHERE id = ?').run(r.id);
    assert.strictEqual((await verify()).body.execution.checks.pauseLedgerComplete, false);
    CT.db.prepare('UPDATE paper_rounds SET paused_ms = paused_ms - 12345 WHERE id = ?').run(r.id);

    const player = CT.q.players.all(r.id)[0];
    CT.db.prepare('UPDATE paper_round_players SET boost_max_exposure = boost_max_exposure + 12345 WHERE round_id = ? AND user_id = ?')
      .run(r.id, player.user_id);
    assert.strictEqual((await verify()).body.execution.checks.boostFreezeRecordConsistent, false);
    CT.db.prepare('UPDATE paper_round_players SET boost_max_exposure = boost_max_exposure - 12345 WHERE round_id = ? AND user_id = ?')
      .run(r.id, player.user_id);

    const original = r.hot1_active_base;
    const draw = CT.privateDrawOf(r);
    const forged = [draw.hot1.asset, ...draw.hot1.fallbackOrder].find((x) => x !== original);
    CT.db.prepare('UPDATE paper_rounds SET hot1_active_base = ? WHERE id = ?').run(forged, r.id);
    assert.strictEqual((await verify()).body.execution.checks.resolutionUsesCommittedOrders, false);
    CT.db.prepare('UPDATE paper_rounds SET hot1_active_base = ? WHERE id = ?').run(original, r.id);

    assert.strictEqual((await verify()).body.execution.verified, true,
      'restoring mutable audit rows must restore the independently checked proof');
  });

  console.log('\nHot economic delta and deterministic fallback');
  await test('a same-millisecond post-boundary fill is excluded by the checkpoint watermark', () => {
    const uid = USERS[4];
    const seat = T.prepareSeat(uid);
    const at = Date.now();
    prime('BTC', 100, at);
    const proof = T.scoreProofFor(uid, seat.epoch, seat.startBalance, { BTC: 100 }, at);
    T.applyFill(uid, { symbol: 'BTC', orderSide: 'BUY', size: 0.01, px: 100,
      feeBps: 0, kind: 'MARKET', leverage: 10, marginMode: 'cross', at,
      executionSource: 'test', referenceMark: 100 });
    assert.deepStrictEqual(T.fillLedgerEvidence(uid, seat.epoch, at,
      proof.fillLedger.watermarkId), proof.fillLedger);
    assert.strictEqual(T.fillLedgerEvidence(uid, seat.epoch, at).count,
      proof.fillLedger.count + 1);
  });
  await test('economic delta handles pre-held gains, partial close, reopen, flip and symmetric loss', () => {
    const uid = USERS[4];
    T.prepareSeat(uid);
    prime('BTC', 100);
    const originalEntry = T.live.map.get('BTC');
    assert.ok(originalEntry);
    const originalPrices = { pythPrice: originalEntry.pythPrice, markPrice: originalEntry.markPrice };
    try {
      T.applyFill(uid, { symbol: 'BTC', orderSide: 'BUY', size: 0.1, px: 100,
        feeBps: 0, kind: 'MARKET', leverage: 10, marginMode: 'cross', executionSource: 'test', referenceMark: 100 });
      T.live.map.get('BTC').pythPrice = 140;
      T.live.map.get('BTC').markPrice = 140;
      const baseline = T.hotValueOf(uid, 'BTC');
      assert.strictEqual(baseline, 4);

      T.applyFill(uid, { symbol: 'BTC', orderSide: 'SELL', size: 0.04, px: 150,
        feeBps: 0, kind: 'MARKET', leverage: 10, marginMode: 'cross', executionSource: 'test', referenceMark: 150 });
      T.live.map.get('BTC').pythPrice = 150;
      T.live.map.get('BTC').markPrice = 150;
      assert.strictEqual(T.hotValueOf(uid, 'BTC') - baseline, 1);

      T.applyFill(uid, { symbol: 'BTC', orderSide: 'BUY', size: 0.04, px: 125,
        feeBps: 0, kind: 'MARKET', leverage: 10, marginMode: 'cross', executionSource: 'test', referenceMark: 125 });
      T.live.map.get('BTC').pythPrice = 125;
      T.live.map.get('BTC').markPrice = 125;
      assert.strictEqual(T.hotValueOf(uid, 'BTC') - baseline, -0.5);

      T.applyFill(uid, { symbol: 'BTC', orderSide: 'SELL', size: 0.15, px: 120,
        feeBps: 0, kind: 'MARKET', leverage: 10, marginMode: 'cross', executionSource: 'test', referenceMark: 120 });
      T.live.map.get('BTC').pythPrice = 110;
      T.live.map.get('BTC').markPrice = 110;
      assert.strictEqual(T.hotValueOf(uid, 'BTC') - baseline, -0.5);

      /* A one-dollar economic loss contributes one extra negative score copy,
         exactly as a one-dollar gain contributes one positive copy. */
      T.db.prepare('DELETE FROM paper_positions WHERE user_id = ?').run(uid);
      T.db.prepare('DELETE FROM paper_fills WHERE user_id = ?').run(uid);
      T.applyFill(uid, { symbol: 'BTC', orderSide: 'BUY', size: 0.1, px: 100,
        feeBps: 0, kind: 'MARKET', leverage: 10, marginMode: 'cross', executionSource: 'test', referenceMark: 100 });
      T.live.map.get('BTC').pythPrice = 140; T.live.map.get('BTC').markPrice = 140;
      const b2 = T.hotValueOf(uid, 'BTC');
      T.live.map.get('BTC').pythPrice = 130; T.live.map.get('BTC').markPrice = 130;
      assert.strictEqual(T.hotValueOf(uid, 'BTC') - b2, -1);
    } finally {
      assert.strictEqual(T.live.map.get('BTC'), originalEntry,
        'economic fixture must retain its original price entry');
      originalEntry.pythPrice = originalPrices.pythPrice;
      originalEntry.markPrice = originalPrices.markPrice;
    }
  });

  await test('three-market fallback may use the recovered unused primary for Hot #2', () => {
    readyFn = () => true;
    const r = create('fallback-three', { candidates: ['A', 'B', 'C'], uid: USERS[1] });
    start(r.id);
    const live = CT.q.get.get(r.id);
    const d = CT.privateDrawOf(live);
    const primary1 = d.hot1.asset;
    const primary2 = d.hot2.asset;
    const backup = d.backupOrder[0];
    const allowed = new Set([backup]);
    readyFn = (sym) => allowed.has(sym);
    CT.openHotV2(r.id, 1, Date.now());
    let row = CT.q.get.get(r.id);
    assert.strictEqual(row.hot1_active_base, backup);
    CT.closeHotV2(r.id, 1, Date.now());
    allowed.clear(); allowed.add(primary1);
    CT.openHotV2(r.id, 2, Date.now());
    row = CT.q.get.get(r.id);
    assert.strictEqual(row.hot2_active_base, primary1);
    assert.notStrictEqual(row.hot2_active_base, row.hot1_active_base);
    assert.notStrictEqual(primary2, primary1);
    abortLive(); readyFn = () => true;
  });

  await test('Hot #2 records an already-used primary honestly instead of calling it unready', () => {
    readyFn = () => true;
    const r = create('hot2-distinct-reason', { candidates: ['A', 'B', 'C'] });
    start(r.id);
    const draw = CT.privateDrawOf(CT.q.get.get(r.id));
    readyFn = (sym) => sym === draw.hot2.asset;
    CT.openHotV2(r.id, 1, Date.now());
    assert.strictEqual(CT.q.get.get(r.id).hot1_active_base, draw.hot2.asset);
    CT.closeHotV2(r.id, 1, Date.now());
    readyFn = (sym) => sym === draw.hot1.asset;
    CT.openHotV2(r.id, 2, Date.now());
    const after = CT.q.get.get(r.id);
    assert.strictEqual(after.hot2_active_base, draw.hot1.asset);
    assert.match(after.hot2_fallback_reason, /already ran as Hot Market 1/);
    const resolution = JSON.parse(CT.q.hotResolution.get(r.id, 2).evidence_json);
    assert.deepStrictEqual(resolution.skipped,
      [{ asset: draw.hot2.asset, reason: 'already_used_hot1' }]);
    assert.strictEqual(resolution.selectionCause, 'already_used_hot1');
    abortLive(); readyFn = () => true;
  });

  await test('a due-valid Hot primary remains locked when it fails before a delayed callback', () => {
    readyFn = () => true;
    const r = create('hot-due-lock', { kind: 'rehearsal', speed: 30 });
    start(r.id);
    const live = CT.q.get.get(r.id);
    const draw = CT.privateDrawOf(live);
    const due = Date.now();
    const primary = draw.hot1.asset;
    readyFn = (sym, kind, at) => kind !== 'HOT' || at !== undefined || sym !== primary;
    assert.throws(() => CT.openHotV2(r.id, 1, due), /awaiting.*live price/);
    const locked = CT.q.hotResolution.get(r.id, 1);
    assert.ok(locked);
    assert.strictEqual(locked.active, primary);
    assert.notStrictEqual(locked.active, draw.hot1.fallbackOrder[0]);

    const p = comp.planOf(live);
    CT.q.bMark.run(r.id, draw.hot1.activation - p.hotWarning, 'succeeded', null, Date.now());
    CT.q.bMark.run(r.id, draw.hot1.activation, 'retryable',
      'competition price unavailable at boundary', Date.now());
    setActiveOffset(r.id, draw.hot1.activation + 1);
    assert.strictEqual(comp.activeSegmentReady(CT.q.get.get(r.id)), false,
      'a healthy alternate may not release the selected-primary pause');
    readyFn = () => true;
    CT.openHotV2(r.id, 1, Date.now());
    assert.strictEqual(CT.q.get.get(r.id).hot1_active_base, primary);
    abortLive();
  });

  await test('Boost locks the exact-due eligible set and waits rather than shrinking it', () => {
    readyFn = () => true;
    const r = create('boost-due-lock', { kind: 'rehearsal', speed: 30 });
    start(r.id);
    const downNow = new Set(['BTC', 'XRP']);
    readyFn = (sym, kind, at) => kind !== 'BOOST' || at != null || !downNow.has(sym);
    const due = Date.now();
    assert.throws(() => CT.openBoost(r.id, due), /awaiting.*live price/);
    const row = CT.q.boostResolution.get(r.id);
    assert.ok(row);
    const locked = JSON.parse(row.evidence_json);
    assert.deepStrictEqual(locked.selected, ['BTC', 'ETH', 'XRP', 'SOL']);
    assert.strictEqual(CT.q.players.all(r.id)[0].boost_frozen_at, null);
    assert.deepStrictEqual(JSON.parse(CT.q.get.get(r.id).boost_opened || '[]'), []);

    readyFn = () => true;
    CT.openBoost(r.id, Date.now());
    assert.deepStrictEqual(JSON.parse(CT.q.get.get(r.id).boost_opened), locked.selected);
    abortLive();
  });

  await test('Boost readiness and evidence use the leverage persisted on the round', () => {
    const r = create('boost-persisted-tier', { kind: 'rehearsal', speed: 30 });
    CT.db.prepare('UPDATE paper_rounds SET boost_leverage = 250 WHERE id = ?').run(r.id);
    start(r.id);
    readyFn = (_sym, kind, _at, requiredLeverage) =>
      kind !== 'BOOST' || Number(requiredLeverage) === 250;
    const due = Date.now();
    CT.openBoost(r.id, due);
    const opened = JSON.parse(CT.q.get.get(r.id).boost_opened || '[]');
    assert.deepStrictEqual(opened, comp.boostMarketsOf(CT.q.get.get(r.id)));
    const resolution = JSON.parse(CT.q.boostResolution.get(r.id).evidence_json);
    assert.ok(resolution.attempts.every((attempt) =>
      Number(attempt.evidence.policy.leverageRequired) === 250));
    assert.ok(CT.q.players.all(r.id).every((player) =>
      Number(player.boost_max_exposure) === Number(player.boost_bankroll) * 250));
    readyFn = () => true;
    abortLive();
  });

  console.log('\nactive clock, restart and public phase linearization');
  await test('boundary window checks use the authoritative event timestamp, not processing wall time', () => {
    readyFn = () => true;
    healthFn = () => true;
    const r = create('delayed-event-time', { kind: 'rehearsal', speed: 30 });
    start(r.id);
    const live = CT.q.get.get(r.id);
    const draw = CT.privateDrawOf(live);
    const p = comp.planOf(live);
    const activation = draw.hot1.activation;
    const startedAt = Date.now() - activation - p.hotDuration - 1000;
    CT.db.prepare('UPDATE paper_rounds SET started_at = ?, ends_at = ? WHERE id = ?')
      .run(startedAt, startedAt + p.total, r.id);
    CT.q.bMark.run(r.id, activation - p.hotWarning, 'succeeded', null,
      startedAt + activation - p.hotWarning);
    CT.fireBoundary(r.id, activation, startedAt + activation);
    assert.strictEqual(CT.q.bGet.get(r.id, activation).status, 'succeeded');
    assert.ok(CT.q.get.get(r.id).hot1_active_base);
    abortLive();
  });

  await test('a coalesced price expiry prevents boundary success and preserves the full warning', () => {
    const r = create('boundary-pause', { kind: 'rehearsal', speed: 30 });
    start(r.id);
    const live = CT.q.get.get(r.id);
    const warning = CT.privateDrawOf(live).hot1.activation - comp.planOf(live).hotWarning;
    // One observation defines both onset and elapsed time. Crossing a real
    // millisecond between these reads must not move onset before the warning.
    const detectedAt = Date.now(), dueWall = detectedAt - 700;
    setActiveOffset(r.id, warning + 700, detectedAt);
    let healthy = false;
    healthFn = () => {
      if (!healthy) { comp.pauseClockOpen(dueWall); return false; }
      return true;
    };
    events.length = 0;
    CT.fireBoundary(r.id, warning, Date.now());
    assert.ok(CT.q.get.get(r.id).paused_since);
    assert.ok(!CT.q.bGet.get(r.id, warning), 'warning cannot commit inside invalid time');
    assert.strictEqual(comp.phaseNow().phase, 'build');
    healthy = true;
    const resumeAt = Date.now();
    comp.pauseClockClose(resumeAt);
    CT.fireBoundary(r.id, warning, resumeAt);
    assert.strictEqual(CT.q.bGet.get(r.id, warning).status, 'succeeded');
    assert.strictEqual(comp.phaseNow(resumeAt).phase, 'hotWarning');
    assert.ok(comp.phaseNow(resumeAt).phaseLeftMs === undefined);
    assert.ok(comp.planOf(live).hotWarning > 0);
    abortLive(); healthFn = () => true;
  });

  await test('sub-millisecond outage edges cannot fractionalise checkpoints or the active clock', async () => {
    /* Production rehearsal ops-rehearsal-20260904232252 received a source
       validity edge ending in .039ms. SQLite retained that fraction in its
       INTEGER-affinity clock columns, so Hot close tried to create a score
       proof with a fractional asOf and correctly failed identity validation.
       The clock protocol is integer milliseconds; exercise both rounding
       directions here and then settle every boundary through the real proof
       path. */
    readyFn = () => true;
    healthFn = () => true;
    const r = create('fractional-outage-clock', { kind: 'rehearsal', speed: 30 });
    const running = start(r.id);
    comp.pauseClockOpen(running.started_at + 123.039);
    comp.pauseClockClose(running.started_at + 1563.961);

    let clock = CT.q.get.get(r.id);
    assert.ok(Number.isSafeInteger(Number(clock.started_at)));
    assert.ok(Number.isSafeInteger(Number(clock.ends_at)));
    assert.ok(Number.isSafeInteger(Number(clock.paused_ms)));
    assert.strictEqual(clock.paused_since, null);
    const pauses = CT.q.clockPauses.all(r.id);
    assert.strictEqual(Number(pauses[0].started_at), running.started_at + 123,
      'the invalid interval must start at the containing whole millisecond');
    assert.strictEqual(Number(pauses[0].ended_at), running.started_at + 1564,
      'recovery must begin after the containing invalid millisecond');
    assert.strictEqual(Number(clock.paused_ms), 1441);
    for (const pause of pauses) {
      assert.ok(Number.isSafeInteger(Number(pause.started_at)));
      assert.ok(Number.isSafeInteger(Number(pause.ended_at)));
    }

    for (const at of comp.boundariesOf(clock)) {
      const dueAt = comp.dueInstant(CT.q.get.get(r.id), at);
      assert.ok(Number.isSafeInteger(dueAt));
      CT.fireBoundary(r.id, at, dueAt);
      assert.strictEqual(CT.q.bGet.get(r.id, at).status, 'succeeded',
        `integer boundary ${at} did not settle`);
    }
    clock = CT.q.get.get(r.id);
    assert.strictEqual(clock.status, 'done');
    assert.ok(CT.q.bAll.all(r.id).every((row) => Number.isSafeInteger(Number(row.due_wall_at))));
    assert.ok(CT.q.scoreProofs.all(r.id).every((row) => Number.isSafeInteger(Number(row.as_of))));
    const verify = await invoke(P.compVerify, { url: `/verify?round=${r.id}` });
    assert.strictEqual(verify.code, 200);
    assert.strictEqual(verify.body.execution.verified, true,
      JSON.stringify(verify.body.execution.checks));
  });

  await test('a first boundary stamp during an open pause excludes that still-open interval', () => {
    const r = create('open-pause-boundary-stamp', { kind: 'rehearsal', speed: 30 });
    start(r.id);
    const warning = CT.privateDrawOf(CT.q.get.get(r.id)).hot1.activation
      - comp.planOf(CT.q.get.get(r.id)).hotWarning;
    setActiveOffset(r.id, warning + 500);
    const beforePause = CT.q.get.get(r.id);
    const expectedDue = beforePause.started_at + warning + Number(beforePause.paused_ms || 0);
    const pauseAt = beforePause.started_at + warning + 500.789;
    comp.pauseClockOpen(pauseAt);
    CT.fireBoundary(r.id, warning, pauseAt + 5000.321);
    const boundary = CT.q.bGet.get(r.id, warning);
    assert.strictEqual(boundary.status, 'succeeded');
    assert.strictEqual(Number(boundary.due_wall_at), expectedDue,
      'the growing outage must not move a boundary already reached before it opened');
    abortLive();
  });

  await test('effective phase stays warning at failed activation and Boost at failed bell', () => {
    const r = create('effective-phase', { kind: 'rehearsal', speed: 30 });
    start(r.id);
    const live = CT.q.get.get(r.id);
    const d = CT.privateDrawOf(live);
    const p = comp.planOf(live);
    CT.q.bMark.run(r.id, d.hot1.activation - p.hotWarning, 'succeeded', null, Date.now());
    CT.q.bMark.run(r.id, d.hot1.activation, 'retryable', 'competition price unavailable at boundary', Date.now());
    setActiveOffset(r.id, d.hot1.activation);
    assert.strictEqual(comp.phaseNow().phase, 'hotWarning');
    for (const at of comp.boundariesOf(live)) CT.q.bMark.run(r.id, at, 'succeeded', null, Date.now());
    CT.q.bMark.run(r.id, p.total, 'retryable', 'competition price unavailable at boundary', Date.now());
    setActiveOffset(r.id, p.total);
    assert.strictEqual(comp.phaseNow().phase, 'boost');
    abortLive();
  });

  await test('an unpriced opening becomes a retryable active-time pause, never a hard block', () => {
    const r = create('unpriced-boundary', { kind: 'rehearsal', speed: 30 });
    start(r.id);
    const d = CT.privateDrawOf(CT.q.get.get(r.id));
    const p = comp.planOf(r);
    const warning = d.hot1.activation - p.hotWarning;
    CT.q.bMark.run(r.id, warning, 'succeeded', null, Date.now());
    setActiveOffset(r.id, d.hot1.activation + 1);
    readyFn = () => false;
    CT.fireBoundary(r.id, d.hot1.activation, Date.now());
    const after = CT.q.get.get(r.id);
    assert.ok(after.paused_since, 'boundary price failure must freeze the active clock');
    assert.strictEqual(after.blocked_reason, null);
    assert.strictEqual(CT.q.bGet.get(r.id, d.hot1.activation).status, 'retryable');
    readyFn = () => true;
    comp.pauseClockClose(Date.now());
    abortLive();
  });

  await test('a null outage onset freezes at detection, not at the round start', () => {
    const r = create('null-outage-onset', { kind: 'rehearsal', speed: 30 });
    start(r.id);
    setActiveOffset(r.id, 1000);
    const detectedAt = Date.now();
    T.__pauseFor({ symbol: 'NULLONSET', message: 'deterministic missing onset' });
    const after = CT.q.get.get(r.id);
    assert.ok(after.paused_since >= detectedAt,
      `null onset rewound pause to ${after.paused_since}, before ${detectedAt}`);
    assert.ok(comp.activeElapsed(after, after.paused_since) >= 900,
      'null onset must not rewind the active clock to zero');
    T.__clearPauses();
    comp.pauseClockClose(Date.now());
    abortLive();
  });

  await test('identity outage stays paused through price recovery, including a rowless clock', () => {
    const auth = require('./auth-shim.js');
    const originalHealth = auth.authHealth;
    const r = create('identity-outage-recovery', { candidates: ['BTC', 'ETH', 'SOL'] });
    start(r.id);
    const onset = Date.now();
    auth.authHealth = () => ({ tradingAvailable: false, unavailableSince: onset, validUntil: null });
    try {
      assert.strictEqual(T.__ensureCompetitionClockHealth(onset), false);
      const pause = T.roundPaused();
      assert.match(pause.why, /contestant identity service unavailable/);
      assert.strictEqual(Number(CT.q.get.get(r.id).paused_since), onset);
      primeMajors();
      T.__clearPauseIfPriceable();
      assert.ok(T.roundPaused(), 'healthy prices must not clear the identity obligation');
      assert.strictEqual(Number(CT.q.get.get(r.id).paused_since), onset);

      T.__clearPauses();
      T.__clearPauseIfPriceable();
      assert.strictEqual(Number(CT.q.get.get(r.id).paused_since), onset,
        'rowless clock recovery must also wait for identity availability');

      auth.authHealth = () => ({ tradingAvailable: true, unavailableSince: null,
        validUntil: Date.now() + 6000 });
      T.__clearPauseIfPriceable();
      assert.strictEqual(CT.q.get.get(r.id).paused_since, null);
      assert.strictEqual(T.__competitionClockStatus().ok, true);
    } finally {
      auth.authHealth = originalHealth;
      abortLive();
    }
  });

  await test('pause reconciliation preserves the durable outage onset after a clock-write fault', () => {
    const r = create('pause-write-fault', { kind: 'rehearsal', speed: 30 });
    start(r.id);
    const live = CT.q.get.get(r.id);
    const draw = CT.privateDrawOf(live);
    const warning = draw.hot1.activation - comp.planOf(live).hotWarning;
    setActiveOffset(r.id, warning);
    /* Stay inside the (seeded and highly compressed) running round. The
       production clamp correctly refuses to backdate before START. */
    const onset = Date.now() - 250;
    const originalPauseClockOpen = comp.pauseClockOpen;
    const originalHealthFn = healthFn;
    comp.pauseClockOpen = () => { throw new Error('injected clock write fault'); };
    try {
      T.__pauseFor({ symbol: 'FAULTMARK', message: 'deterministic fault', startedAt: onset });
    } finally {
      comp.pauseClockOpen = originalPauseClockOpen;
    }
    assert.strictEqual(CT.q.get.get(r.id).paused_since, null);
    /* Exercise the direct timer path before the normal sweep reconciler. The
       health hook must backdate the clock first and leave the warning due. */
    healthFn = (at) => T.__ensureCompetitionClockHealth(at);
    try {
      CT.fireBoundary(r.id, warning, Date.now());
    } finally {
      healthFn = originalHealthFn;
    }
    const frozen = CT.q.get.get(r.id);
    assert.strictEqual(Number(frozen.paused_since), onset,
      'reconciliation must use the persisted row onset, not detection time');
    const boundary = CT.q.bGet.get(r.id, warning);
    assert.ok(!boundary || boundary.status !== 'succeeded',
      'a boundary may not commit between the durable pause row and clock edge');
    T.__clearPauses();
    comp.pauseClockClose(Date.now());
    abortLive();
  });

  await test('a pause-store failure still freezes and latches the exact source onset', () => {
    const r = create('pause-store-fault', { kind: 'rehearsal', speed: 30 });
    start(r.id);
    setActiveOffset(r.id, 1000);
    const onset = Date.now() - 250;
    const store = T.__pauseStmts();
    const originalOpen = store.open.run;
    store.open.run = () => { throw new Error('injected pause-store fault'); };
    try {
      T.__pauseAt('FAULTSTORE', onset, 'deterministic store fault');
    } finally {
      store.open.run = originalOpen;
    }
    const frozen = CT.q.get.get(r.id);
    assert.strictEqual(Number(frozen.paused_since), onset,
      'the clock must preserve the source-known onset even without a pause row');
    assert.ok(frozen.blocked_reason && frozen.blocked_reason.includes('could not be persisted'));
    const latched = T.roundPaused();
    assert.ok(latched && latched.unpersisted);
    assert.strictEqual(Number(latched.since), onset);
    abortLive();
  });

  await test('restart banking never rewinds behind Hot/Boost and freezes every active phase', () => {
    for (const target of ['hotWarning', 'hot', 'finalBuild', 'boost']) {
      const id = `restart-${target}`;
      const r = create(id, { kind: 'rehearsal', speed: 30 });
      start(id);
      const live = CT.q.get.get(id);
      const d = CT.privateDrawOf(live);
      const p = comp.planOf(live);
      const offset = target === 'hotWarning' ? d.hot1.activation - Math.floor(p.hotWarning / 2)
        : target === 'hot' ? d.hot1.activation + 100
          : target === 'finalBuild' ? p.finalBuildStart + 100
            : p.boostStart + 100;
      for (const at of comp.boundariesOf(live)) {
        if (at <= offset) CT.q.bMark.run(id, at, 'succeeded', null, Date.now());
      }
      if (target === 'hot') {
        CT.db.prepare('UPDATE paper_rounds SET hot1_active_base = ?, hot1_revealed_at = ? WHERE id = ?')
          .run(d.hot1.asset, Date.now(), id);
      }
      if (target === 'boost') {
        CT.db.prepare('UPDATE paper_rounds SET boost_opened = ? WHERE id = ?')
          .run(JSON.stringify(['BTC']), id);
        if (!T.aliasOpen('BTC-BOOST')) T.openAlias('BTC-BOOST', id);
      }
      setActiveOffset(id, offset);
      const beforePhase = comp.phaseNow().phase;
      const now = Date.now();
      CT.db.prepare('UPDATE paper_rounds SET clock_owner_id = ?, clock_heartbeat_at = ?, paused_since = NULL WHERE id = ?')
        .run('dead-process', now - 900, id);
      const before = comp.activeElapsed(CT.q.get.get(id), now);
      CT.claimClockAfterRestart(CT.q.get.get(id), now);
      const claimed = CT.q.get.get(id);
      assert.ok(claimed.paused_since, `${target} restart did not freeze`);
      const pauseRows = CT.q.clockPauses.all(id);
      const banked = [...pauseRows].reverse().find((row) => row.source === 'restart');
      const recovery = pauseRows.find((row) => row.ended_at == null);
      assert.ok(banked && recovery, `${target} restart/recovery ledger is incomplete`);
      assert.strictEqual(Number(banked.ended_at), Number(recovery.started_at),
        `${target} restart handoff consumed active milliseconds`);
      assert.strictEqual(comp.phaseNow(now).phase, beforePhase, `${target} phase changed`);
      const furthest = CT.q.bAll.all(id).filter((b) => b.status === 'succeeded')
        .reduce((m, b) => Math.max(m, Number(b.at)), 0);
      assert.ok(comp.activeElapsed(claimed, now) >= furthest, `${target} rewound behind ${furthest}`);
      assert.ok(comp.activeElapsed(claimed, now) <= before);
      comp.pauseClockClose(now + 10);
      abortLive();
    }
  });

  await test('restart after the bell blocks if even one opening segment was missed', () => {
    readyFn = () => true;
    const r = create('restart-missed-boost', { kind: 'rehearsal', speed: 30 });
    start(r.id);
    const live = CT.q.get.get(r.id);
    const p = comp.planOf(live);
    for (const at of comp.boundariesOf(live)) {
      if (at < p.boostStart) CT.q.bMark.run(r.id, at, 'succeeded', null, Date.now());
    }
    setActiveOffset(r.id, p.total + 1000);
    comp.resume();
    const after = CT.q.get.get(r.id);
    assert.match(after.blocked_reason || '', /restart missed 1 boundary/);
    assert.strictEqual(CT.q.scores.all(r.id, 'final').length, 0);
    abortLive();
  });

  await test('a persisted Hot opening is revalidated and never redrawn after a crash', () => {
    readyFn = () => true;
    const r = create('restart-hot-partial', { kind: 'rehearsal', speed: 30 });
    start(r.id);
    const d = CT.privateDrawOf(CT.q.get.get(r.id));
    const p = comp.planOf(r);
    CT.openHotV2(r.id, 1, Date.now());
    const active = CT.q.get.get(r.id).hot1_active_base;
    CT.q.bMark.run(r.id, d.hot1.activation - p.hotWarning, 'succeeded', null, Date.now());
    CT.q.bMark.run(r.id, d.hot1.activation, 'retryable', null, Date.now());
    setActiveOffset(r.id, d.hot1.activation + 1);

    readyFn = (sym) => sym !== active;
    assert.strictEqual(comp.activeSegmentReady(CT.q.get.get(r.id)), false,
      'restart pause must retain the already-revealed market obligation');
    assert.throws(() => CT.openHotV2(r.id, 1, Date.now()), /revealed market is not priceable/);
    assert.strictEqual(CT.q.get.get(r.id).hot1_active_base, active, 'retry may not redraw');

    readyFn = () => true;
    assert.strictEqual(comp.activeSegmentReady(CT.q.get.get(r.id)), true);
    CT.openHotV2(r.id, 1, Date.now());
    assert.strictEqual(CT.q.get.get(r.id).hot1_active_base, active);
    abortLive();
  });

  await test('a persisted Boost opening rehydrates exact aliases and preserves its freeze on retry', () => {
    const selected = new Set(['BTC', 'SOL']);
    readyFn = () => true;
    const r = create('restart-boost-partial', { kind: 'rehearsal', speed: 30 });
    start(r.id);
    const p = comp.planOf(r);
    for (const at of comp.boundariesOf(CT.q.get.get(r.id))) {
      if (at < p.boostStart) CT.q.bMark.run(r.id, at, 'succeeded', null, Date.now());
    }
    CT.q.bMark.run(r.id, p.boostStart, 'retryable', null, Date.now());
    readyFn = (sym, kind) => kind !== 'BOOST' || selected.has(sym);
    CT.openBoost(r.id, Date.now());
    const frozenAt = CT.q.players.all(r.id)[0].boost_frozen_at;
    T.closeAlias('BTC-BOOST', { flatten: false, roundId: r.id });
    T.closeAlias('SOL-BOOST', { flatten: false, roundId: r.id });
    setActiveOffset(r.id, p.boostStart + 1);
    assert.strictEqual(comp.phaseNow().phase, 'finalBuild');
    readyFn = () => true;
    comp.rehydrateGates(CT.q.get.get(r.id));
    assert.strictEqual(T.aliasOpen('BTC-BOOST'), true);
    assert.strictEqual(T.aliasOpen('SOL-BOOST'), true);
    assert.strictEqual(comp.activeSegmentReady(CT.q.get.get(r.id)), true);
    CT.openBoost(r.id, Date.now());
    const after = CT.q.get.get(r.id);
    assert.deepStrictEqual(JSON.parse(after.boost_opened), ['BTC', 'SOL']);
    assert.strictEqual(CT.q.players.all(r.id)[0].boost_frozen_at, frozenAt,
      'idempotent retry must not move the bankroll snapshot');
    abortLive();
  });

  await test('Hot close and bell use a valid due instant despite a later live outage', () => {
    readyFn = () => true;
    const hotRound = create('historical-hot-close', { kind: 'rehearsal', speed: 30 });
    start(hotRound.id);
    const openedAt = Date.now() - 20;
    CT.openHotV2(hotRound.id, 1, openedAt);
    const closeAt = Date.now() - 10;
    /* Current readiness is false; immutable history at and before closeAt is
       true. A delayed settlement must use the latter. */
    readyFn = (_sym, _kind, at) => Number.isFinite(Number(at)) && Number(at) <= closeAt;
    CT.closeHotV2(hotRound.id, 1, closeAt);
    assert.ok(CT.q.hotRows.all(hotRound.id, 1).every((row) => row.ended_at === closeAt));
    abortLive();

    readyFn = () => true;
    const bellRound = create('historical-bell', { kind: 'rehearsal', speed: 30 });
    start(bellRound.id);
    const p = comp.planOf(bellRound);
    CT.db.prepare('UPDATE paper_rounds SET boost_opened = ? WHERE id = ?')
      .run(JSON.stringify(['BTC']), bellRound.id);
    setActiveOffset(bellRound.id, p.total + 100);
    const dueAt = CT.q.get.get(bellRound.id).started_at + p.total;
    readyFn = (_sym, _kind, at) => Number.isFinite(Number(at)) && Number(at) <= dueAt;
    CT.fireBoundary(bellRound.id, p.total, Date.now());
    assert.strictEqual(CT.q.get.get(bellRound.id).status, 'done');
    assert.strictEqual(CT.q.bGet.get(bellRound.id, p.total).status, 'succeeded');
    readyFn = () => true;
  });

  await test('bell cleanup failure is retryable without inventing a closed-gate outage', () => {
    readyFn = () => true;
    const r = create('bell-cleanup-retry', { kind: 'rehearsal', speed: 30 });
    start(r.id);
    const p = comp.planOf(r);
    for (const at of comp.boundariesOf(CT.q.get.get(r.id))) {
      if (at < p.total) CT.q.bMark.run(r.id, at, 'succeeded', null, Date.now());
    }
    CT.db.prepare('UPDATE paper_rounds SET boost_opened = ? WHERE id = ?')
      .run(JSON.stringify(['BTC', 'SOL']), r.id);
    T.openAlias('BTC-BOOST', r.id);
    T.openAlias('SOL-BOOST', r.id);
    setActiveOffset(r.id, p.total + 10);
    let failSol = true;
    comp.wire({ closeAlias: (alias, opts) => {
      if (alias === 'SOL-BOOST' && failSol) {
        failSol = false;
        throw new Error('deterministic close failure');
      }
      return T.closeAlias(alias, opts);
    } });
    CT.fireBoundary(r.id, p.total, Date.now());
    let after = CT.q.get.get(r.id);
    assert.strictEqual(after.status, 'running');
    assert.ok(after.blocked_reason);
    assert.strictEqual(CT.q.scores.all(r.id, 'final').length, 1,
      'the immutable checkpoint proves the initial health pass completed');
    assert.strictEqual(T.aliasOpen('BTC-BOOST'), false);
    assert.strictEqual(T.aliasOpen('SOL-BOOST'), true);

    comp.wire({ closeAlias: T.closeAlias });
    after = comp.clearBlock(r.id, { note: 'retry deterministic terminal cleanup' });
    assert.strictEqual(after.status, 'done');
    assert.strictEqual(CT.q.bGet.get(r.id, p.total).status, 'succeeded');
    assert.strictEqual(T.aliasOpen('SOL-BOOST'), false);
    assert.deepStrictEqual(JSON.parse(CT.q.get.get(r.id).gate_outage || '[]'), [],
      'retry must not reinterpret an already-closed alias as a pre-bell outage');
  });

  await test('bell recovery flattens a Boost leg from frozen final marks with the live feed gone', () => {
    readyFn = () => true;
    const uid = USERS[3];
    const r = create('bell-cold-feed-cleanup', { kind: 'rehearsal', speed: 30, uid });
    start(r.id);
    const due = Date.now();
    CT.db.prepare(`UPDATE paper_round_players SET boost_bankroll = 10,
      boost_max_exposure = 5000, boost_frozen_at = ?
      WHERE round_id = ? AND user_id = ?`).run(due, r.id, uid);
    CT.db.prepare('UPDATE paper_rounds SET boost_opened = ? WHERE id = ?')
      .run(JSON.stringify(['BTC']), r.id);
    T.openAlias('BTC-BOOST', r.id);
    T.applyFill(uid, { symbol: 'BTC-BOOST', orderSide: 'BUY', size: 0.01, px: 100,
      feeBps: 0, kind: 'MARKET', leverage: 500, marginMode: 'isolated',
      executionSource: 'test', referenceMark: 100 });
    comp.snapshot(r.id, 'final', due);
    const plan = comp.planOf(CT.q.get.get(r.id));
    CT.db.prepare('UPDATE paper_rounds SET started_at = ?, ends_at = ? WHERE id = ?')
      .run(due - plan.total, due, r.id);
    const saved = T.live.map.get('BTC');
    T.live.map.delete('BTC');
    try {
      CT.fireBoundary(r.id, plan.total, due);
      assert.strictEqual(CT.q.get.get(r.id).status, 'done');
      assert.strictEqual(T.stmt.posGet.get(uid, 'BTC-BOOST'), undefined);
      protectedAuditFillId = T.db.prepare(`SELECT MAX(id) AS id FROM paper_fills
        WHERE user_id = ? AND epoch = ?`).get(uid, CT.q.players.all(r.id)[0].epoch).id;
      assert.ok(protectedAuditFillId);
    } finally {
      if (saved) T.live.map.set('BTC', saved);
      prime('BTC');
    }
  });

  await test('isolated bell settlement preserves checkpoint equity at ledger precision', () => {
    /* Production rehearsal 20260904230452 found the adversarial rounding
       combination: two isolated legs whose raw sub-micro PnL rounded on the
       mandatory Boost close. Marking raw PnL but settling ledger-rounded PnL
       moved the post-close proof by exactly one micro-dollar. */
    const uid = USERS[4];
    const seat = T.prepareSeat(uid);
    T.applyFill(uid, { symbol: 'BTC', orderSide: 'BUY', size: 0.00005, px: 79645.78,
      feeBps: 0, kind: 'MARKET', leverage: 17, marginMode: 'isolated',
      executionSource: 'test', referenceMark: 79645.78 });
    T.openAlias('ETH-BOOST', 'isolated-ledger-precision');
    T.applyFill(uid, { symbol: 'ETH-BOOST', orderSide: 'SELL', size: 0.003014, px: 2451.02,
      feeBps: 0, kind: 'MARKET', leverage: 500, marginMode: 'isolated',
      executionSource: 'test', referenceMark: 2451.02 });
    const at = Date.now();
    const marks = { BTC: 79660.808502, 'ETH-BOOST': 2451.15 };
    const checkpoint = T.scoreUser(uid, null, seat.epoch, seat.startBalance, marks, at);
    T.closeAlias('ETH-BOOST', {
      roundId: 'isolated-ledger-precision', settleAt: at, overrideMark: 2451.15,
    });
    const proof = T.scoreProofFor(uid, seat.epoch, seat.startBalance, marks, at);
    assert.strictEqual(proof.equity, checkpoint.equity,
      'mandatory same-mark cleanup must not move the checkpoint by one ledger unit');
    T.prepareSeat(uid);
  });

  await test('historical availability is not contaminated by a later halt interval', () => {
    const at = Date.now();
    prime('XRP', 100, at);
    const live = T.live.map.get('XRP');
    const since = at + 10;
    live.indexHalt = true;
    T.halt.set('XRP', { halted: true, haltedAt: since, divergeSince: since });
    const before = T.__historicalAvailabilityAt('XRP', 100, at);
    const during = T.__historicalAvailabilityAt('XRP', 100, since + 1);
    assert.strictEqual(before.ready, true, 'later halt must not rewrite a prior instant');
    assert.strictEqual(during.ready, false, 'the halt interval itself remains invalid');
    live.indexHalt = false;
    T.halt.delete('XRP');
    prime('XRP');
  });

  await test('market expiry reports the quote deadline, not detection time', () => {
    readyFn = () => true;
    const r = create('expiry-deadline', { candidates: ['ALPHA', 'BETA', 'GAMMA'] });
    start(r.id);
    setActiveOffset(r.id, 10_000);
    const at = Date.now() - 4500;
    /* A deliberately expired accepted live record. The source components do
       not need to exist: availability must still preserve the record's own
       exact expiry instead of replacing it with detection time. */
    T.live.map.set('EXPIRYTEST', {
      pythPrice: 100, pythAtMs: at, pythSrcAtMs: at,
      srcKey: 'usdt', indexHalt: false,
    });
    const s = T.__marketAvailability('EXPIRYTEST', 100, Date.now());
    assert.strictEqual(s.ready, false);
    assert.ok(s.invalidSince < Date.now() - 300,
      `invalidSince ${s.invalidSince} was not backdated`);
    assert.ok(s.invalidSince >= at);
    T.live.map.delete('EXPIRYTEST');
    abortLive();
  });

  console.log('\nsealed Boost capacity policies');
  await test('new Boost capacity policy seals before creation and legacy absence never inherits a new default', () => {
    const current = create('boost-policy-new', { candidates: ['BTC', 'ETH', 'SOL'] });
    const old = create('boost-policy-old', { candidates: ['BTC', 'ETH', 'SOL'], boostCapacityPolicy: 'frozen-start-v1' });
    const inputFor = (r) => ({ id: r.id, kind: r.kind, speed: r.speed,
      candidates: JSON.parse(r.hot_candidates), seed: r.hot_draw_secret,
      plan: comp.planOf(r), draw: CT.privateDrawOf(r) });
    try {
      assert.strictEqual(current.boost_capacity_policy, 'current-equity-v1');
      assert.strictEqual(old.boost_capacity_policy, null);
      assert.strictEqual(comp.boostCapacityPolicyOf(old), 'frozen-start-v1');
      assert.strictEqual(CT.v2Commit(inputFor(old)), old.draw_commit, 'legacy commitment bytes remain unchanged');
      assert.notStrictEqual(CT.v2Commit(inputFor(current)), current.draw_commit);
      assert.strictEqual(CT.v2Commit({ ...inputFor(current), boostCapacityPolicy: 'current-equity-v1' }), current.draw_commit);
      for (const row of [current, old]) {
        const next = row === current ? 'frozen-start-v1' : 'current-equity-v1';
        assert.throws(() => CT.db.prepare('UPDATE paper_rounds SET boost_capacity_policy=? WHERE id=?')
          .run(next, row.id), /Boost capacity policy is immutable/);
        assert.strictEqual(CT.sealedDrawValid(CT.q.get.get(row.id)), true);
      }
      comp.wire({ defaultBoostCapacityPolicy: () => 'frozen-start-v1' });
      assert.strictEqual(comp.boostCapacityPolicyOf(CT.q.get.get(current.id)), 'current-equity-v1');
      comp.wire({ defaultBoostCapacityPolicy: () => 'current-equity-v1' });
      assert.strictEqual(comp.boostCapacityPolicyOf(CT.q.get.get(old.id)), 'frozen-start-v1');
      for (const value of ['', 'future-policy', false, 1]) {
        const invalid = { ...current, boost_capacity_policy: value };
        assert.throws(() => comp.boostCapacityPolicyOf(invalid), /unsupported/);
        assert.strictEqual(CT.sealedDrawValid(invalid), false, 'recovery cannot reinterpret an unknown policy');
        assert.strictEqual(comp.boostCapacityPolicyMatches(value, current), false);
        const id = 'unsupported-boost-policy-' + String(value);
        comp.wire({ defaultBoostCapacityPolicy: () => value });
        assert.throws(() => comp.createRound({ id, candidates: ['BTC', 'ETH', 'SOL'], players: roster() }), /unsupported/);
        assert.strictEqual(CT.q.get.get(id), undefined, 'unknown policy fails before any round is created');
      }
      assert.strictEqual(comp.boostCapacityPolicyMatches(undefined, old), true);
      assert.strictEqual(comp.boostCapacityPolicyMatches(undefined, current), false);
    } finally {
      comp.wire({ defaultBoostCapacityPolicy: () => 'current-equity-v1' });
      comp.abortRound(current.id, { force: true }); comp.abortRound(old.id, { force: true });
    }
  });

  await test('Boost maximum uses sealed leverage and finite actual equity, never a new default', () => {
    const dynamic = { boost_capacity_policy: 'current-equity-v1', boost_leverage: 250 };
    const legacy = { boost_capacity_policy: null, boost_leverage: 250 };
    assert.strictEqual(T.boostMaximumFor(dynamic, 12, 2500), 3000);
    assert.strictEqual(T.boostMaximumFor(legacy, 12, 2500), 2500);
    assert.strictEqual(T.boostMaximumFor(dynamic, 8, 2500), 2000);
    assert.strictEqual(T.boostMaximumFor(legacy, 8, 2500), 2000);
    assert.strictEqual(T.boostMaximumFor(dynamic, -1, 2500), 0);
    for (const bad of [NaN, Infinity, -Infinity]) assert.throws(() => T.boostMaximumFor(dynamic, bad, 2500), /unavailable/);
    assert.throws(() => T.boostMaximumFor(dynamic, 1e300, 2500), /rounded capacity is unavailable/);
    assert.throws(() => T.boostMaximumFor({ ...dynamic, boost_capacity_policy: 'unknown' }, 10, 2500), /unsupported/);
  });

  await test('current-equity Boost profits expand capacity while start proof stays immutable', async () => {
    await withDynamicBoostFixture('boost-dynamic-profit', async ({ r, uid, mark, fill }) => {
      fill('BTC-BOOST', 40);
      mark(100.1);
      const capacity = T.boostCapacityCheck(uid, 'SOL-BOOST', 'BUY', 10, 100);
      assert.strictEqual(capacity.max, 7000);
      assert.strictEqual(capacity.frozenMax, 5000);
      assert.strictEqual(capacity.projected, 5004);
      assert.strictEqual(capacity.ok, true);
      fill('SOL-BOOST', 10); // actual last-line fill exceeds the legacy cap, with sufficient free margin
      assert.strictEqual(T.boostExposureOf(uid), 5004);
      assert.strictEqual(T.accountRisk(uid, T.stmt.acctGet.get(uid)).free, 0);
      assert.strictEqual(comp.boostBudgetOf(uid, CT.q.get.get(r.id)).maxExposure, 5000);
      const row = T.compRankSnapshot().players.find((p) => p.userId === uid);
      assert.strictEqual(row.equity, 14);
      assert.strictEqual(row.boostMaxExposure, 7000);
      assert.strictEqual(row.projectedBoostPower, 7000);
      assert.strictEqual(row.boostRemainingExposure, 1996);
      // Isolated value is deliberately rounded to ledger precision before it
      // reaches accountRisk. A tiny move of only Boost legs cannot establish
      // the raw-equity precondition. Release margin and add an ordinary cross
      // leg, whose marked PnL is genuinely unrounded in that same risk call.
      fill('SOL-BOOST', 10, 'SELL');
      assert.strictEqual(T.stmt.posGet.get(uid, 'SOL-BOOST'), undefined);
      assert.strictEqual(T.roundMarkets().has('BTC'), true);
      assert.strictEqual(!!T.cfgOf('BTC').isolatedOnly, false);
      assert.ok(T.accountRisk(uid, T.stmt.acctGet.get(uid)).free >= 0.5 * 100.1 / 100);
      T.applyFill(uid, { symbol: 'BTC', orderSide: 'BUY', size: 0.5, px: 100.1,
        feeBps: 0, kind: 'MARKET', leverage: 100, marginMode: 'cross',
        executionSource: 'test', referenceMark: 100.1 });
      assert.strictEqual(T.stmt.posGet.get(uid, 'BTC').margin_mode, 'cross');
      assert.strictEqual(T.stmt.posGet.get(uid, 'BTC-BOOST').margin_mode, 'isolated');
      assert.ok(T.accountRisk(uid, T.stmt.acctGet.get(uid)).free > 0);
      mark(100.100000713);
      const rawEquity = T.accountRisk(uid, T.stmt.acctGet.get(uid)).equityTotal;
      const fractional = T.boostCapacityCheck(uid, 'SOL-BOOST', 'BUY', 0.01, 100);
      const preciseRow = T.compRankSnapshot().players.find((p) => p.userId === uid);
      assert.notStrictEqual(rawEquity, preciseRow.equity, 'precondition: equity has sub-ledger precision');
      assert.strictEqual(preciseRow.boostMaxExposure, fractional.max);
      assert.strictEqual(preciseRow.projectedBoostPower, fractional.max);
      assert.notStrictEqual(preciseRow.boostMaxExposure, Math.round(preciseRow.equity * 500 * 1e6) / 1e6,
        'capacity must not multiply an already rounded public equity');
      assert.strictEqual(Object.hasOwn(preciseRow, 'equityTotal'), false, 'private risk capture is not a new public field');
      const state = await invoke(P.compBaseline);
      assert.strictEqual(state.body.round.boostCapacityPolicy, 'current-equity-v1');
    });
  });

  await test('current-equity Boost losses shrink capacity but retain reduction and fill-time checks', async () => {
    await withDynamicBoostFixture('boost-dynamic-loss', ({ uid, mark, fill }) => {
      fill('BTC-BOOST', 40);
      mark(100.1);
      const prior = T.boostCapacityCheck(uid, 'SOL-BOOST', 'BUY', 10, 100);
      assert.strictEqual(prior.ok, true);
      mark(99.9);
      const lower = T.boostCapacityCheck(uid, 'SOL-BOOST', 'BUY', 10, 100);
      assert.strictEqual(lower.max, 3000);
      assert.strictEqual(lower.remaining, 0);
      assert.strictEqual(lower.ok, false);
      const before = T.stmt.posByUser.all(uid);
      assert.throws(() => fill('SOL-BOOST', 10), (e) => e.code === 'boost_capacity_exceeded');
      assert.deepStrictEqual(T.stmt.posByUser.all(uid), before);
      assert.strictEqual(T.boostCapacityCheck(uid, 'BTC-BOOST', 'SELL', 1, 99.9).ok, true);
      fill('BTC-BOOST', 1, 'SELL');
      assert.strictEqual(T.stmt.posGet.get(uid, 'BTC-BOOST').size, 39);
    });
  });

  await test('dynamic Boost positions and non-reducing orders reserve one shared budget', async () => {
    await withDynamicBoostFixture('boost-dynamic-reservation', ({ uid, mark, fill }) => {
      fill('BTC-BOOST', 40); mark(100.1);
      const epoch = T.stmt.acctGet.get(uid).epoch;
      const orderId = Number(T.stmt.ordInsWithBoost.run(uid, epoch, 'SOL-BOOST', 'BUY',
        100, 10, 500, 0, Date.now(), 'isolated', null, null, 1).lastInsertRowid);
      const blocked = T.boostCapacityCheck(uid, 'BTC-BOOST', 'BUY', 20, 100,
        { reservation: true, reduceOnly: false });
      assert.strictEqual(blocked.projected, 7004);
      assert.strictEqual(blocked.max, 7000);
      assert.strictEqual(blocked.ok, false);
      const replacement = T.boostCapacityCheck(uid, 'SOL-BOOST', 'BUY', 10, 100,
        { reservation: true, excludeOrderId: orderId });
      assert.strictEqual(replacement.projected, 5004, 'replacement excludes its own old reservation exactly once');
      assert.strictEqual(replacement.ok, true);
      const reduce = T.boostCapacityCheck(uid, 'BTC-BOOST', 'SELL', 1, 100,
        { reservation: true, reduceOnly: true });
      assert.strictEqual(reduce.projected, 5004, 'reduce-only intention adds no new collateral reservation');
    });
  });

  await test('Hot score bonus cannot enlarge dynamic Boost equity or published buying power', async () => {
    await withDynamicBoostFixture('boost-dynamic-hot-score', ({ uid, mark, fill }) => {
      fill('BTC-BOOST', 40); mark(100.1);
      comp.hotBonusFor = () => 123;
      const row = T.compRankSnapshot().players.find((p) => p.userId === uid);
      assert.strictEqual(row.hotBonus, 123);
      assert.strictEqual(row.accountPnl, 4);
      assert.strictEqual(row.score, 127);
      assert.strictEqual(row.equity, 14);
      assert.strictEqual(row.projectedBoostPower, 7000);
      assert.strictEqual(row.boostMaxExposure, 7000);
      assert.strictEqual(T.boostCapacityCheck(uid, 'SOL-BOOST', 'BUY', 30, 100).max, 7000);
    });
  });

  await test('legacy absent-policy rounds still settle and verify their original proof format', async () => {
    const armed = create('boost-legacy-proof', { kind: 'rehearsal', speed: 30,
      boostCapacityPolicy: 'frozen-start-v1' });
    try {
      const running = start(armed.id);
      for (const at of comp.boundariesOf(running)) {
        CT.fireBoundary(running.id, at, running.started_at + at);
        const row = CT.q.bGet.get(running.id, at);
        assert.ok(row && row.status === 'succeeded', `legacy boundary ${at}: ${row?.error}`);
      }
      const verify = await invoke(P.compVerify, { url: `/verify?round=${running.id}` });
      assert.strictEqual(verify.body.execution.verified, true, JSON.stringify(verify.body.execution.checks));
      assert.strictEqual(verify.body.reveal.boostCapacityPolicy, undefined);
      assert.strictEqual(verify.body.execution.boost.capacityPolicy, 'frozen-start-v1');
      assert.strictEqual(verify.body.execution.boost.snapshotRole, 'admission-ceiling');
      for (const row of CT.q.scoreProofs.all(running.id)) {
        const proof = JSON.parse(row.proof_json);
        assert.strictEqual(proof.boostCapacityPolicy, undefined);
        assert.strictEqual(proof.state.boostCapacityPolicy, undefined);
      }
      assert.strictEqual(CT.q.get.get(running.id).boost_capacity_policy, null);
    } finally { abortLive(); }
  });

  await test('resting reservations use current marks and reject a high SELL limit aggregate', () => {
    const r = create('boost-capacity', { candidates: ['BTC', 'ETH', 'SOL'], uid: USERS[2], boostCapacityPolicy: 'frozen-start-v1' });
    start(r.id);
    const uid = USERS[2];
    CT.db.prepare(`UPDATE paper_round_players
      SET boost_bankroll = 10, boost_max_exposure = 5000, boost_frozen_at = ?
      WHERE round_id = ? AND user_id = ?`).run(Date.now(), r.id, uid);
    try {
      T.openAlias('BTC-BOOST', r.id);
      T.openAlias('SOL-BOOST', r.id);
      prime('BTC', 100); prime('SOL', 100);
      T.applyFill(uid, { symbol: 'BTC-BOOST', orderSide: 'BUY', size: 40, px: 100,
        feeBps: 0, kind: 'MARKET', leverage: 500, marginMode: 'isolated', executionSource: 'test', referenceMark: 100 });

      const oldFillProjection = T.boostCapacityCheck(uid, 'BTC-BOOST', 'BUY', 30, 50);
      const reservation = T.boostCapacityCheck(uid, 'BTC-BOOST', 'BUY', 30, 50,
        { reservation: true, reduceOnly: false });
      assert.strictEqual(oldFillProjection.ok, true, 'precondition: fill projection reprices the target');
      assert.strictEqual(reservation.ok, false);
      assert.strictEqual(reservation.projected, 5500);

      const highSell = T.boostCapacityCheck(uid, 'SOL-BOOST', 'SELL', 9, 120,
        { reservation: true, reduceOnly: false });
      assert.strictEqual(highSell.ok, false);
      assert.strictEqual(highSell.projected, 5080);
    } finally {
      try {
        for (const alias of ['BTC-BOOST', 'SOL-BOOST']) {
          T.closeAlias(alias, { flatten: true, roundId: r.id });
          assert.strictEqual(T.aliasOpen(alias), false);
          assert.strictEqual(T.openAliases.has(alias), false);
          assert.deepStrictEqual(T.stmt.posBySymbol.all(alias), []);
          assert.deepStrictEqual(T.stmt.ordOpenBySymbol.all(alias), []);
        }
      } finally { abortLive(); }
    }
  });

  await test('profits cannot compound frozen capacity and losses lower the live ceiling', () => {
    const r = create('boost-loss', { candidates: ['BTC', 'ETH', 'SOL'], uid: USERS[3], boostCapacityPolicy: 'frozen-start-v1' });
    start(r.id);
    const uid = USERS[3];
    CT.db.prepare(`UPDATE paper_round_players
      SET boost_bankroll = 10, boost_max_exposure = 5000, boost_frozen_at = ?
      WHERE round_id = ? AND user_id = ?`).run(Date.now(), r.id, uid);
    let originalEntry, originalPrices;
    try {
      T.openAlias('BTC-BOOST', r.id); T.openAlias('SOL-BOOST', r.id);
      prime('BTC', 100); prime('SOL', 100);
      originalEntry = T.live.map.get('BTC');
      assert.ok(originalEntry);
      originalPrices = { pythPrice: originalEntry.pythPrice, markPrice: originalEntry.markPrice };
      T.applyFill(uid, { symbol: 'BTC-BOOST', orderSide: 'BUY', size: 25, px: 100,
        feeBps: 0, kind: 'MARKET', leverage: 500, marginMode: 'isolated', executionSource: 'test', referenceMark: 100 });
      T.live.map.get('BTC').pythPrice = 100.1; T.live.map.get('BTC').markPrice = 100.1;
      const win = T.boostCapacityCheck(uid, 'SOL-BOOST', 'BUY', 26, 100);
      assert.strictEqual(win.max, 5000);
      assert.strictEqual(win.ok, false, 'winning PnL must not lift frozen maximum');

      T.live.map.get('BTC').pythPrice = 99.9; T.live.map.get('BTC').markPrice = 99.9;
      const loss = T.boostCapacityCheck(uid, 'SOL-BOOST', 'BUY', 13, 100);
      assert.ok(loss.max < 5000, `loss did not lower max: ${loss.max}`);
      assert.strictEqual(loss.ok, false, 'new exposure must obey remaining actual equity');
    } finally {
      try {
        if (originalEntry) {
          assert.strictEqual(T.live.map.get('BTC'), originalEntry,
            'Boost loss fixture must retain its original price entry');
          originalEntry.pythPrice = originalPrices.pythPrice;
          originalEntry.markPrice = originalPrices.markPrice;
        }
      } finally {
        try {
          for (const alias of ['BTC-BOOST', 'SOL-BOOST']) {
            T.closeAlias(alias, { flatten: true, roundId: r.id });
            assert.strictEqual(T.aliasOpen(alias), false);
            assert.strictEqual(T.openAliases.has(alias), false);
            assert.deepStrictEqual(T.stmt.posBySymbol.all(alias), []);
            assert.deepStrictEqual(T.stmt.ordOpenBySymbol.all(alias), []);
          }
        } finally { abortLive(); }
      }
    }
  });

  await test('a failed scored limit tick rolls back every seat and preserves its orders for recovery', async () => {
    abortLive();
    const r = create('scored-limit-tick', { candidates: ['BTC', 'ETH', 'SOL'], players: [
      { userId: USERS[0], displayName: 'One', seat: 0 },
      { userId: USERS[1], displayName: 'Two', seat: 1 },
    ] });
    start(r.id);
    const ids = [USERS[0], USERS[1]].map((uid) => Number(T.stmt.ordInsWithBoost.run(
      uid, T.stmt.acctGet.get(uid).epoch, 'BTC', 'BUY', 99.99, 0.01, 2, 0,
      Date.now(), 'isolated', 99, 101, 1).lastInsertRowid));
    T.db.exec(`CREATE TEMP TRIGGER fail_scored_limit BEFORE UPDATE ON paper_orders
      WHEN OLD.user_id = ${USERS[1]} AND NEW.status = 'FILLED'
      BEGIN SELECT RAISE(ABORT, 'scored limit write unavailable'); END`);
    try {
      await new Promise((resolve) => setTimeout(resolve, 5));
      prime('BTC', 99.98);
      for (const id of ids) {
        assert.strictEqual(T.stmt.ordGet.get(id).status, 'OPEN');
        assert.strictEqual(T.db.prepare('SELECT COUNT(*) n FROM paper_fills WHERE order_id=?').get(id).n, 0);
      }
      assert.strictEqual(T.stmt.posGet.get(USERS[0], 'BTC'), undefined);
      assert.strictEqual(T.stmt.posGet.get(USERS[1], 'BTC'), undefined);
      assert.match(comp.currentRound().blocked_reason, /scored limit write unavailable/);
    } finally {
      T.db.exec('DROP TRIGGER fail_scored_limit');
      abortLive();
    }
  });

  await test('a full resting book skips per-order barriers until an accepted price crosses', () => {
    const players = Array.from({ length: 8 }, (_,i) => ({ userId: 8250 + i,
      displayName: 'Resting fixture ' + i, seat: i }));
    for (const p of players) { userIns.run(p.userId); T.__ensureAccountRef()(p.userId); }
    const r = create('bounded-resting-tick', { players, candidates: ['BTC', 'ETH', 'SOL'] });
    start(r.id);
    for (const p of players) {
      const acct = T.stmt.acctGet.get(p.userId);
      for (let i = 0; i < 20; i++) T.stmt.ordInsWithBoost.run(p.userId, acct.epoch,
        'BTC', i % 2 ? 'SELL' : 'BUY', i % 2 ? 110 : 90, 0.001, 2, 0,
        Date.now(), 'cross', null, null, 0);
    }
    const barrier = comp.writeBarrier;
    let orderBarriers = 0;
    const fixtureUsers = new Set(players.map((p) => p.userId));
    comp.writeBarrier = (...args) => {
      if (fixtureUsers.has(args[0])) orderBarriers++;
      return barrier(...args);
    };
    try {
      assert.strictEqual(T.tickEval('BTC', { force: true }).ok, true);
      assert.strictEqual(orderBarriers, 0,
        '160 noncrossing orders must not run 160 full per-order round barriers');
      assert.strictEqual(T.stmt.ordOpenBySymbol.all('BTC').filter((o) => fixtureUsers.has(o.user_id)).length, 160);
      assert.strictEqual(T.stmt.posBySymbol.all('BTC').filter((p) => fixtureUsers.has(p.user_id)).length, 0);
    } finally {
      comp.writeBarrier = barrier;
      abortLive();
    }
  });

  await test('fill retention preserves proved rows without blocking unrelated pruning', () => {
    assert.ok(protectedAuditFillId);
    const uid = USERS[4];
    const acct = T.stmt.acctGet.get(uid);
    const inserted = T.stmt.fillIns.run(uid, acct.epoch, 'ETH', 'BUY', 'MARKET',
      100, 0.01, 1, 0, null, null, Date.now(), 0);
    const unprotectedId = Number(inserted.lastInsertRowid);
    const result = T.stmt.fillPrune.run(Date.now() + 1000);
    assert.ok(result.changes > 0);
    assert.ok(T.db.prepare('SELECT 1 FROM paper_fills WHERE id = ?').get(protectedAuditFillId));
    assert.strictEqual(T.db.prepare('SELECT 1 FROM paper_fills WHERE id = ?').get(unprotectedId), undefined);
  });

  await test('last-accepted policy is immutable while legacy commitment bytes stay unchanged', () => {
    comp.wire({ defaultPricePolicy: () => 'strict', initializeRoundMarks: null });
    const old = create('policy-legacy-commit', { candidates: ['BTC', 'ETH', 'SOL'], boostCapacityPolicy: 'frozen-start-v1' });
    const inputFor = (r) => ({ id: r.id, kind: r.kind, speed: Number(r.speed) || 1,
      candidates: JSON.parse(r.hot_candidates), seed: r.hot_draw_secret,
      plan: comp.planOf(r), draw: CT.privateDrawOf(r) });
    try {
      const input = inputFor(old);
      const legacy = { version: 2, roundId: input.id, kind: input.kind,
        speed: input.speed, plan: { ...input.plan }, candidates: input.candidates.slice(),
        seed: input.seed, draw: input.draw };
      const bytes = JSON.stringify(legacy);
      assert.strictEqual(comp.pricePolicyOf(old), 'strict');
      assert.strictEqual(old.price_policy, null);
      assert.strictEqual(JSON.stringify(CT.revealEnvelope(input)), bytes);
      assert.strictEqual(JSON.stringify(CT.revealEnvelope({ ...input, pricePolicy: 'strict' })), bytes);
      assert.strictEqual(old.draw_commit, crypto.createHash('sha256').update(bytes).digest('hex'));
      assert.throws(() => CT.db.prepare('UPDATE paper_rounds SET price_policy=? WHERE id=?')
        .run(LAST_ACCEPTED_POLICY, old.id), /price policy is immutable/);
      assert.strictEqual(CT.q.get.get(old.id).draw_commit, old.draw_commit);
      withRoundMarkFixture('policy-new-commit', ({ round }) => {
        const envelope = CT.revealEnvelope({ ...inputFor(round), pricePolicy: LAST_ACCEPTED_POLICY,
          boostCapacityPolicy: comp.boostCapacityPolicyOf(round) });
        assert.strictEqual(comp.pricePolicyOf(round), LAST_ACCEPTED_POLICY);
        assert.strictEqual(envelope.pricePolicy, LAST_ACCEPTED_POLICY);
        assert.strictEqual(round.draw_commit,
          crypto.createHash('sha256').update(JSON.stringify(envelope)).digest('hex'));
        assert.notStrictEqual(CT.v2Commit(inputFor(round)), round.draw_commit,
          'omitting the armed policy changes this new commitment');
        assert.strictEqual(CT.sealedDrawValid(round), true);
        assert.throws(() => CT.db.prepare('UPDATE paper_rounds SET price_policy=NULL WHERE id=?')
          .run(round.id), /price policy is immutable/);
        comp.wire({ defaultPricePolicy: () => 'strict' });
        assert.strictEqual(comp.pricePolicyOf(CT.q.get.get(round.id)), LAST_ACCEPTED_POLICY,
          'a later default does not rewrite a prepared round');
      });
    } finally {
      CT.q.setStatus.run('aborted', Date.now(), old.id);
      comp.wire({ defaultPricePolicy: () => 'strict', initializeRoundMarks: null });
    }
  });

  await test('round-mark CAS and outer rollback preserve all economic storage together', () => {
    withRoundMarkFixture('policy-mark-cas', ({ round, clock, record, identity, commit }) => {
      const first = record(1, clock.now);
      assert.throws(() => comp.roundMarkCommit(round.id, first, null), /economic transaction/);
      commit(first, null);
      const storedBefore = CT.db.prepare('SELECT * FROM paper_round_marks WHERE round_id=?').get(round.id);
      const ticksBefore = CT.db.prepare('SELECT * FROM paper_round_mark_ticks WHERE round_id=?').all(round.id);
      const minutesBefore = CT.db.prepare('SELECT * FROM paper_round_mark_minutes WHERE round_id=?').all(round.id);
      clock.now += 10;
      const second = record(2, clock.now, { price: 123.456789123 });
      assert.throws(() => commit(second, null), /lineage changed/);
      assert.throws(() => commit(second, { acceptedBoot: first.acceptedBoot, acceptedSeq: 99 }), /lineage changed/);
      assert.throws(() => CT.db.transaction(() => {
        comp.roundMarkCommit(round.id, second, identity(first));
        assert.strictEqual(comp.roundMarkGet(round.id, 'BTC', clock.now).price, second.price);
        throw new Error('fixture transaction cancelled');
      })(), /fixture transaction cancelled/);
      assert.deepStrictEqual(CT.db.prepare('SELECT * FROM paper_round_marks WHERE round_id=?').get(round.id), storedBefore);
      assert.deepStrictEqual(CT.db.prepare('SELECT * FROM paper_round_mark_ticks WHERE round_id=?').all(round.id), ticksBefore);
      assert.deepStrictEqual(CT.db.prepare('SELECT * FROM paper_round_mark_minutes WHERE round_id=?').all(round.id), minutesBefore);
      commit(second, identity(first));
      assert.strictEqual(comp.roundMarkGet(round.id, 'BTC', clock.now).price, second.price);
      assert.throws(() => commit(second, identity(second)), /not monotonic/);
    });
  });

  await test('a held mark retains exact original lineage after age expiry and never invents an initial mark', () => {
    withRoundMarkFixture('policy-mark-age', ({ round, clock, record, commit }) => {
      assert.strictEqual(comp.roundMarkGet(round.id, 'BTC', clock.now), null);
      assert.strictEqual(T.__roundExecutionMark('BTC', { round, at: clock.now, candidate: false }), null);
      const first = record(1, clock.now, { price: 0.0000123456789123 });
      commit(first, null);
      clock.now += 90_000;
      const stored = comp.roundMarkGet(round.id, 'BTC', clock.now);
      assert.deepStrictEqual(stored, { ...first, hardInvalid: false, hardFailure: null });
      const held = T.__roundExecutionMark('BTC', { round, at: clock.now, forLeverage: 500, candidate: false });
      assert.ok(held);
      assert.strictEqual(held.held, true);
      for (const key of Object.keys(first)) assert.strictEqual(held[key], first[key], key);
      assert.strictEqual(T.__roundExecutionMark('BTC', { round, at: clock.now, forLeverage: 501, candidate: false }), null);
      assert.strictEqual(comp.roundMarkGet(round.id, 'ETH', clock.now), null);
    });
  });

  await test('round system-failure intervals are half-open and recover only with a new committed lineage', () => {
    withRoundMarkFixture('policy-mark-break', ({ round, clock, record, identity, commit }) => {
      const first = record(1, clock.now);
      commit(first, null);
      const failedAt = clock.now += 100;
      comp.roundMarkInvalidate(round.id, 'BTC', 'fixture storage unavailable', failedAt);
      assert.strictEqual(comp.roundMarkGet(round.id, 'BTC', failedAt - 1).hardInvalid, false);
      assert.strictEqual(comp.roundMarkGet(round.id, 'BTC', failedAt).hardInvalid, true);
      assert.strictEqual(T.__roundExecutionMark('BTC', { round, at: failedAt, candidate: false }), null);
      clock.now += 1000;
      const recovered = record(2, clock.now, { price: 102 });
      assert.throws(() => CT.db.transaction(() => {
        comp.roundMarkCommit(round.id, recovered, identity(first));
        throw new Error('fixture recovery cancelled');
      })(), /fixture recovery cancelled/);
      assert.strictEqual(comp.roundMarkGet(round.id, 'BTC', clock.now).hardInvalid, true);
      commit(recovered, identity(first));
      assert.strictEqual(comp.roundMarkGet(round.id, 'BTC', clock.now - 1).hardInvalid, true);
      assert.deepStrictEqual(comp.roundMarkGet(round.id, 'BTC', clock.now),
        { ...recovered, hardInvalid: false, hardFailure: null });
      const history = comp.roundMarkHistory(round.id, 'BTC', { from: first.appliedAt, to: clock.now });
      assert.deepStrictEqual(history.breaks, [{ from: failedAt, to: recovered.appliedAt }]);
    });
  });

  await test('past round prices select adoption time exactly without borrowing a later mark', () => {
    withRoundMarkFixture('policy-mark-past', ({ round, clock, record, identity, commit }) => {
      const first = record(1, clock.now);
      commit(first, null);
      clock.now += 20;
      const second = record(2, clock.now - 5, { appliedAt: clock.now, price: 110 });
      commit(second, identity(first));
      const third = record(3, clock.now, { price: 120 });
      commit(third, identity(second));
      assert.strictEqual(comp.roundMarkGet(round.id, 'BTC', first.appliedAt - 1), null);
      assert.strictEqual(comp.roundMarkGet(round.id, 'BTC', first.appliedAt).price, first.price);
      assert.strictEqual(comp.roundMarkGet(round.id, 'BTC', second.acceptedAt).price, first.price);
      assert.strictEqual(comp.roundMarkGet(round.id, 'BTC', second.appliedAt - 1).price, first.price);
      assert.strictEqual(comp.roundMarkGet(round.id, 'BTC', third.appliedAt).price, third.price,
        'same-millisecond updates resolve by durable revision order');
      assert.strictEqual(comp.roundMarkGet(round.id, 'BTC', third.appliedAt + 5000).price, third.price);
    });
  });

  await test('recent round ticks remain bounded while old pinned boundaries do not fabricate chart continuity', () => {
    withRoundMarkFixture('policy-mark-ring', ({ round, clock, record, identity }) => {
      const origin = clock.now;
      const pin = origin + 5;
      CT.q.bMark.run(round.id, 1000, 'pending', null, origin);
      CT.q.bDue.run(pin, round.id, 1000);
      let previous = null;
      // One small deterministic storage sequence, not a timing/load benchmark.
      CT.db.transaction(() => {
        for (let seq = 1; seq <= 2048 + 128; seq++) {
          clock.now = origin + seq;
          const row = record(seq, clock.now);
          comp.roundMarkCommit(round.id, row, identity(previous));
          previous = row;
        }
      })();
      const retained = CT.db.prepare('SELECT COUNT(*) n FROM paper_round_mark_ticks WHERE round_id=? AND base=?')
        .get(round.id, 'BTC').n;
      assert.strictEqual(retained, 2049, '2048 contiguous ticks and one exact pending-boundary anchor');
      assert.strictEqual(comp.roundMarkGet(round.id, 'BTC', pin).acceptedSeq, 5);
      assert.strictEqual(comp.roundMarkGet(round.id, 'BTC', pin + 1), null,
        'the old anchor cannot prove the pruned interval after its exact boundary');
      const history = comp.roundMarkHistory(round.id, 'BTC', { from: pin, to: clock.now });
      assert.strictEqual(history.complete, false);
      assert.strictEqual(history.records.length, 2048);
      assert.strictEqual(history.records[0].acceptedSeq, 129);
      assert.strictEqual(history.fromAvailable, origin + 129);
      assert.strictEqual(history.records.at(-1).acceptedSeq, 2176);
      const short = comp.roundMarkHistory(round.id, 'BTC', { from: pin, to: clock.now, limit: 32 });
      assert.strictEqual(short.complete, false);
      assert.ok(short.records.length <= 33);
      assert.strictEqual(comp.roundMarkGet(round.id, 'BTC', clock.now + 1_000_000).acceptedSeq, 2176);
    });
  });

  await test('round candle history holds quiet prices but leaves explicit system-pause gaps', () => {
    withRoundMarkFixture('policy-mark-candles', ({ round, clock, record, identity, commit }) => {
      const origin = clock.now;
      clock.now = origin + 10;
      const first = record(1, clock.now, { price: 100 });
      commit(first, null);
      const gapStart = clock.now = origin + 2 * MIN + 1000;
      comp.roundMarkInvalidate(round.id, 'BTC', 'fixture system pause', clock.now);
      clock.now = origin + 3 * MIN + 1000;
      const second = record(2, clock.now, { price: 110 });
      commit(second, identity(first));
      clock.now = origin + 5 * MIN - 1;
      const history = comp.roundMarkCandles(round.id, 'BTC', { from: first.appliedAt, to: clock.now, tf: '1m' });
      assert.deepStrictEqual(history.breaks, [{ from: gapStart, to: second.appliedAt }]);
      assert.deepStrictEqual(history.rows.map((row) => row.time), [origin, origin + MIN, origin + 4 * MIN]);
      assert.deepStrictEqual(history.rows[1], { time: origin + MIN, open: 100, high: 100, low: 100, close: 100 });
      assert.deepStrictEqual(history.rows[2], { time: origin + 4 * MIN, open: 110, high: 110, low: 110, close: 110 });
      assert.strictEqual(history.fromAvailable, first.appliedAt);
      const unknown = comp.roundMarkCandles(round.id, 'ETH', { from: origin, to: clock.now, tf: '1m' });
      assert.deepStrictEqual(unknown.rows, []);
      assert.strictEqual(unknown.complete, false);
    });
  });

  await test('a partial historical round minute is omitted rather than filled from an older close', () => {
    withRoundMarkFixture('policy-mark-partial-minute', ({ round, clock, record, identity, commit }) => {
      const origin = clock.now;
      clock.now += 10;
      const first = record(1, clock.now, { price: 100 });
      commit(first, null);
      clock.now = origin + MIN + 100;
      const middle = record(2, clock.now, { price: 101 });
      commit(middle, identity(first));
      clock.now = origin + MIN + 500;
      const later = record(3, clock.now, { price: 102 });
      commit(later, identity(middle));
      const cutoff = origin + MIN + 300;
      assert.strictEqual(comp.roundMarkGet(round.id, 'BTC', cutoff).price, 101);
      const partial = comp.roundMarkCandles(round.id, 'BTC', { from: first.appliedAt, to: cutoff, tf: '1m' });
      assert.strictEqual(partial.complete, false);
      assert.deepStrictEqual(partial.rows.map((row) => row.time), [origin]);
      const wider = comp.roundMarkCandles(round.id, 'BTC', { from: first.appliedAt, to: cutoff, tf: '5m' });
      assert.strictEqual(wider.complete, false);
      assert.deepStrictEqual(wider.rows, [], 'the containing timeframe bucket is also incomplete');
      const current = comp.roundMarkCandles(round.id, 'BTC', { from: first.appliedAt, to: clock.now, tf: '1m' });
      assert.strictEqual(current.complete, true);
      assert.strictEqual(current.rows.at(-1).close, 102);
    });
  });

  await test('ordinary held-price accounting crosses both Hot windows, frozen 500x Boost and the final bell', () => {
    abortLive();
    const previousNow = Date.now;
    let now = previousNow() + 60_000;
    let round = null;
    const majors = ['BTC', 'ETH', 'SOL', 'XRP'];
    const hotBases = ['BTC', 'ETH', 'SOL'];
    const uid = USERS[0];
    Date.now = () => now;
    const availability = (sym, kind, at, lev, r) => T.__roundPriceAvailability(
      sym, kind === 'BOOST' ? Number(lev) || 500 : 100, at, r);
    try {
      // Age any prior ordinary fixture components, then accept the preferred
      // ageable source normally. No source state or accepted identity is forged.
      for (const sym of majors) {
        T.compUpdate(sym, 'lazer', 100, now, now);
        assert.strictEqual(T.live.map.get(sym).srcKey, 'lazer');
        assert.strictEqual(T.compPriceReady(sym, now), true);
      }
      comp.wire({
        defaultPricePolicy: () => LAST_ACCEPTED_POLICY,
        initializeRoundMarks: T.__initializeRoundMarks,
        marketReady: (sym, r) => availability(sym, 'HOT', now, 100, r).ready,
        marketReadyForBoost: (sym, lev, r) => availability(sym, 'BOOST', now, lev, r).ready,
        marketReadyAt: (sym, at, kind, lev, r) => T.markAt(sym, at,
          { strict: true, forLeverage: kind === 'BOOST' ? lev : 100, round: r }) > 0,
        marketAvailability: availability,
        historicalAvailabilityAt: T.__historicalAvailabilityAt
          ? (sym, kind, at, lev, r) => T.__historicalAvailabilityAt(sym,
            kind === 'BOOST' ? Number(lev) || 500 : 100, at, r) : null,
        marketEvidenceAt: T.__marketEvidenceAt,
        boostLevCap: (sym, cap, r) => T.boostLevCap(sym, cap, now, r),
        // No identity service is started in a storage/accounting fixture.
        // Source/mark readiness is real; identity outage gates have separate
        // isolated auth/clock tests rather than a fabricated live callback.
        ensureClockHealth: () => true,
      });
      round = create('policy-complete-economic-round', { kind: 'rehearsal', candidates: hotBases });
      comp.startRound(round.id, { at: now });
      round = CT.q.get.get(round.id);
      const plan = comp.planOf(round), draw = CT.privateDrawOf(round);
      const original = Object.fromEntries(majors.map((sym) => [sym, comp.roundMarkGet(round.id, sym, now)]));
      for (const sym of majors) assert.ok(original[sym], `missing initial ${sym}`);
      for (const symbol of hotBases) {
        const px = original[symbol].price;
        const fill = T.applyFill(uid, { symbol, orderSide: 'BUY', size: 0.1,
          px, feeBps: 0, kind: 'MARKET', leverage: 100, marginMode: 'isolated',
          at: now, executionSource: 'composite-index', referenceMark: px });
        assert.strictEqual(fill.price, px);
        assert.strictEqual(fill.decisionContext.inputs.roundExecution.acceptedSeq, original[symbol].acceptedSeq);
      }
      let expectedPnl = 0;
      let boostFrozen = null;
      for (const offset of comp.boundariesOf(round)) {
        now = round.started_at + offset;
        if (offset > 10_000) {
          for (const sym of majors) assert.strictEqual(T.compPriceReady(sym, now), false,
            `${sym}: this phase is deliberately beyond raw source lifetime`);
        }
        for (const sym of majors) assert.strictEqual(availability(sym, 'BOOST', now, 500,
          CT.q.get.get(round.id)).ready, true, `${sym}: held authority must remain available`);
        CT.fireBoundary(round.id, offset, now);
        const boundary = CT.q.bGet.get(round.id, offset);
        assert.ok(boundary && boundary.status === 'succeeded',
          `boundary ${offset}: ${boundary && boundary.error}`);
        const currentRound = CT.q.get.get(round.id);
        assert.strictEqual(currentRound.paused_since, null);
        assert.strictEqual(Number(currentRound.paused_ms), 0);
        assert.strictEqual(currentRound.blocked_reason, null);
        const hotNo = offset === draw.hot1.activation ? 1 : offset === draw.hot2.activation ? 2 : 0;
        if (hotNo) {
          const sym = currentRound[`hot${hotNo}_active_base`];
          const before = comp.roundMarkGet(round.id, sym, now);
          now += 1000;
          const nextPrice = 100.01;
          T.compUpdate(sym, 'lazer', nextPrice, now, now);
          const after = comp.roundMarkGet(round.id, sym, now);
          assert.ok(after.acceptedSeq > before.acceptedSeq);
          assert.strictEqual(after.price, nextPrice);
          expectedPnl += 0.1 * (nextPrice - before.price);
          assert.strictEqual(T.compRankSnapshot().complete, true);
        }
        if (offset === plan.boostStart) {
          boostFrozen = comp.boostBudgetOf(uid, currentRound);
          assert.ok(boostFrozen);
          assert.ok(Math.abs(boostFrozen.bankroll - (10 + expectedPnl)) < 1e-6);
          assert.strictEqual(boostFrozen.maxExposure, boostFrozen.bankroll * 500);
          for (const sym of majors) assert.strictEqual(T.aliasOpen(sym + '-BOOST'), true);
          const held = comp.roundMarkGet(round.id, 'BTC', now);
          const fill = T.applyFill(uid, { symbol: 'BTC-BOOST', orderSide: 'BUY', size: 0.01,
            px: held.price, feeBps: 0, kind: 'MARKET', leverage: 500, marginMode: 'isolated',
            at: now, executionSource: 'composite-index', referenceMark: held.price });
          assert.strictEqual(fill.price, held.price);
          assert.strictEqual(fill.decisionContext.inputs.roundExecution.acceptedSeq, held.acceptedSeq);
        }
      }
      const done = CT.q.get.get(round.id);
      assert.strictEqual(done.status, 'done');
      assert.strictEqual(comp.verifyDraw(done).ok, true);
      assert.ok(boostFrozen);
      const final = CT.q.scores.all(round.id, 'final').find((row) => row.user_id === uid);
      assert.ok(final);
      assert.ok(Math.abs(final.account_pnl - expectedPnl) < 1e-6);
      assert.ok(Math.abs(final.hot_bonus - expectedPnl) < 1e-6);
      assert.ok(Math.abs(final.score - 2 * expectedPnl) < 1e-6);
      assert.strictEqual(T.stmt.posGet.get(uid, 'BTC-BOOST'), undefined);
      for (const sym of majors) assert.strictEqual(T.aliasOpen(sym + '-BOOST'), false);
      const proofRows = CT.q.scoreProofs.all(round.id);
      assert.strictEqual(proofRows.length, 6);
      for (const proof of proofRows) {
        const state = JSON.parse(proof.proof_json).state;
        assert.strictEqual(state.pricePolicy, LAST_ACCEPTED_POLICY);
        for (const [sym, row] of Object.entries(state.roundMarks || {})) {
          assert.strictEqual(row.price, T.markAt(sym, JSON.parse(proof.proof_json).asOf,
            { strict: true, round: done }));
        }
      }
      let checked = null;
      P.compVerify({}, { writeHead(code) { this.code = code; },
        end(raw) { checked = { code: this.code, body: JSON.parse(raw) }; } },
      new URL(`/verify?round=${round.id}`, 'http://fixture'));
      assert.strictEqual(checked.code, 200);
      assert.strictEqual(checked.body.execution.verified, true,
        JSON.stringify(checked.body.execution.checks));
    } finally {
      try {
        if (round && ['armed', 'running'].includes(CT.q.get.get(round.id).status)) {
          comp.abortRound(round.id, { force: true });
        }
      } finally {
        comp.wire({ defaultPricePolicy: () => 'strict', initializeRoundMarks: null });
        wire();
        Date.now = previousNow;
      }
    }
  });

  await test('committed Hot and Boost recover identity pauses without making pause history executable', () => {
    abortLive();
    const auth = require('./auth-shim.js');
    const previousNow = Date.now, previousHealth = auth.authHealth;
    const majors = ['BTC', 'ETH', 'SOL', 'XRP'];
    let now = Math.max(previousNow(), ...majors.map((sym) =>
      Number(T.live.map.get(sym)?.pythAtMs) || 0)) + 60_000;
    let round = null, identityAvailable = true, identitySince = null;
    Date.now = () => now;
    auth.authHealth = () => ({ tradingAvailable: identityAvailable,
      unavailableSince: identityAvailable ? null : identitySince,
      validUntil: identityAvailable ? now + 6000 : null });
    const availability = (sym, kind, at, lev, r) => T.__roundPriceAvailability(
      sym, kind === 'BOOST' ? Number(lev) || 500 : 100, at, r);
    try {
      for (const target of ['hot', 'boost']) {
        now += 60_000;
        identityAvailable = true;
        for (const sym of majors) {
          T.compUpdate(sym, 'lazer', 100, now, now);
          assert.strictEqual(T.live.map.get(sym).srcKey, 'lazer');
        }
        comp.wire({
          defaultPricePolicy: () => LAST_ACCEPTED_POLICY,
          initializeRoundMarks: T.__initializeRoundMarks,
          marketReady: (sym, r) => availability(sym, 'HOT', now, 100, r).ready,
          marketReadyForBoost: (sym, lev, r) => availability(sym, 'BOOST', now, lev, r).ready,
          marketReadyAt: (sym, at, kind, lev, r) => T.markAt(sym, at,
            { strict: true, forLeverage: kind === 'BOOST' ? lev : 100, round: r }) > 0,
          marketAvailability: availability,
          historicalAvailabilityAt: (sym, kind, at, lev, r) => T.__historicalAvailabilityAt(
            sym, kind === 'BOOST' ? Number(lev) || 500 : 100, at, r),
          marketEvidenceAt: T.__marketEvidenceAt,
          boostLevCap: (sym, cap, r) => T.boostLevCap(sym, cap, now, r),
          // The fixture wires the real clock/mark checks, not a live callback.
          ensureClockHealth: T.__ensureCompetitionClockHealth,
        });
        round = create(`policy-identity-recovery-${target}`, {
          kind: 'rehearsal', candidates: ['BTC', 'ETH', 'SOL'], uid: USERS[2],
        });
        comp.startRound(round.id, { at: now });
        round = CT.q.get.get(round.id);
        assert.deepStrictEqual(T.stmt.posByUser.all(USERS[2]), [], 'this recovery fixture starts flat');
        const draw = CT.privateDrawOf(round), plan = comp.planOf(round);
        const edge = target === 'hot' ? draw.hot1.activation : plan.boostStart;
        for (const offset of comp.boundariesOf(round).filter((at) => at <= edge)) {
          now = round.started_at + offset;
          CT.fireBoundary(round.id, offset, now);
          assert.strictEqual(CT.q.bGet.get(round.id, offset).status, 'succeeded');
        }
        now += 1000;
        assert.strictEqual(comp.phaseNow(now).phase, target);
        const active = CT.q.get.get(round.id);
        const base = target === 'hot' ? active.hot1_active_base : 'BTC';
        const leverage = target === 'hot' ? 100 : 500;
        const historical = (at) => T.markAt(base, at, { strict: true, forLeverage: leverage,
          round: CT.q.get.get(round.id) });
        const held = comp.roundMarkGet(round.id, base, now);
        assert.ok(held && historical(now) > 0);
        assert.strictEqual(T.compRankSnapshot().complete, true);
        if (target === 'boost') {
          for (const sym of majors) assert.strictEqual(T.aliasOpen(sym + '-BOOST'), true);
        }

        identityAvailable = false; identitySince = now;
        assert.strictEqual(T.__ensureCompetitionClockHealth(now), false);
        assert.strictEqual(CT.q.get.get(round.id).paused_since, identitySince);
        now += 100;
        const insidePause = now;
        assert.strictEqual(historical(insidePause), null, 'an open pause has no historical execution authority');
        assert.strictEqual(comp.activeSegmentReady(CT.q.get.get(round.id), now), true,
          'the committed segment has healthy current marks despite the historical pause');
        T.__clearPauseIfPriceable();
        assert.strictEqual(CT.q.get.get(round.id).paused_since, identitySince,
          'current marks alone cannot bypass unavailable identity');
        identityAvailable = true;
        now += 100;
        T.__clearPauseIfPriceable();
        assert.strictEqual(T.roundPaused(), null);
        assert.strictEqual(CT.q.get.get(round.id).paused_since, null);
        assert.strictEqual(CT.q.get.get(round.id).paused_ms, 200);
        assert.strictEqual(comp.phaseNow(now).phase, target);
        assert.strictEqual(historical(insidePause), null, 'recovery does not rewrite the closed pause interval');
        assert.strictEqual(historical(now), held.price, 'the recovery edge is usable again');
        assert.strictEqual(comp.roundMarkGet(round.id, base, now).acceptedSeq, held.acceptedSeq,
          'closing identity pause neither replaces nor renews the accepted mark');

        // A separate ordinary price obligation still blocks recovery after
        // identity returns. The flat roster keeps this a readiness fixture,
        // not a synthetic liquidation or accounting replay.
        now += 100;
        identityAvailable = false; identitySince = now;
        assert.strictEqual(T.__ensureCompetitionClockHealth(now), false);
        identityAvailable = true;
        assert.strictEqual(comp.roundMarkInvalidate(round.id, base, 'fixture mark unavailable', now), true);
        assert.strictEqual(T.__roundExecutionMark(base, { round: CT.q.get.get(round.id),
          at: now, forLeverage: leverage }), null);
        assert.strictEqual(comp.activeSegmentReady(CT.q.get.get(round.id), now), false);
        T.__clearPauseIfPriceable();
        assert.strictEqual(CT.q.get.get(round.id).paused_since, identitySince,
          'a hard-invalid current mark keeps the original pause');
        abortLive();
        assert.strictEqual(comp.currentRound(), null);
        round = null;
      }
    } finally {
      try { abortLive(); }
      finally {
        auth.authHealth = previousHealth;
        comp.wire({ defaultPricePolicy: () => 'strict', initializeRoundMarks: null });
        wire();
        Date.now = previousNow;
      }
    }
  });

  await test('live Lazer failback replaces a held round mark and advances base and 500x Boost accounting', () => {
    abortLive();
    const auth = require('./auth-shim.js');
    const previousNow = Date.now, previousHealth = auth.authHealth;
    const majors = ['BTC', 'ETH', 'SOL', 'XRP'], base = 'SOL', uid = USERS[1];
    // Age every prior fixture component before using the real source selector.
    let now = Math.max(previousNow(), ...majors.map((sym) =>
      Number(T.live.map.get(sym)?.pythAtMs) || 0)) + 60_000;
    let round = null;
    Date.now = () => now;
    // No identity/network service is started. Quote qualification, mark
    // adoption, risk, ledger fills and scoring below use the real paper hooks.
    auth.authHealth = () => ({ tradingAvailable: true, unavailableSince: null,
      validUntil: now + 6000 });
    const availability = (sym, kind, at, lev, r) => T.__roundPriceAvailability(
      sym, kind === 'BOOST' ? Number(lev) || 500 : 100, at, r);
    try {
      for (const sym of majors) {
        T.compUpdate(sym, 'lazer', 100, now, now);
        assert.strictEqual(T.live.map.get(sym).srcKey, 'lazer');
        assert.strictEqual(T.compPriceReady(sym, now), true);
      }
      comp.wire({
        defaultPricePolicy: () => LAST_ACCEPTED_POLICY,
        initializeRoundMarks: T.__initializeRoundMarks,
        marketReady: (sym, r) => availability(sym, 'HOT', now, 100, r).ready,
        marketReadyForBoost: (sym, lev, r) => availability(sym, 'BOOST', now, lev, r).ready,
        marketReadyAt: (sym, at, kind, lev, r) => T.markAt(sym, at,
          { strict: true, forLeverage: kind === 'BOOST' ? lev : 100, round: r }) > 0,
        marketAvailability: availability,
        historicalAvailabilityAt: (sym, kind, at, lev, r) => T.__historicalAvailabilityAt(
          sym, kind === 'BOOST' ? Number(lev) || 500 : 100, at, r),
        marketEvidenceAt: T.__marketEvidenceAt,
        boostLevCap: (sym, cap, r) => T.boostLevCap(sym, cap, now, r),
        ensureClockHealth: T.__ensureCompetitionClockHealth,
      });
      round = create('policy-live-lazer-failback', {
        kind: 'rehearsal', candidates: ['BTC', 'ETH', 'SOL'], uid,
      });
      comp.startRound(round.id, { at: now });
      round = CT.q.get.get(round.id);
      for (const offset of comp.boundariesOf(round).filter((at) => at <= comp.planOf(round).boostStart)) {
        now = round.started_at + offset;
        CT.fireBoundary(round.id, offset, now);
        const boundary = CT.q.bGet.get(round.id, offset);
        assert.strictEqual(boundary.status, 'succeeded', boundary.error);
      }
      assert.strictEqual(comp.phaseNow(now).phase, 'boost');
      assert.strictEqual(T.aliasOpen(base + '-BOOST'), true);
      now += 1;
      T.compUpdate(base, 'lazer', 100, now, now);
      const before = comp.roundMarkGet(round.id, base, now);
      assert.ok(before && before.acceptedLeverageCap >= 500);
      const fill = (symbol, side, price) => T.applyFill(uid, {
        symbol, orderSide: side, size: 0.1, px: price, feeBps: 0,
        kind: 'MARKET', leverage: symbol.endsWith('-BOOST') ? 500 : 100,
        marginMode: 'isolated', at: now, executionSource: 'composite-index', referenceMark: price,
      });
      for (const symbol of [base, base + '-BOOST']) {
        const opened = fill(symbol, 'BUY', before.price);
        assert.strictEqual(opened.decisionContext.inputs.roundExecution.acceptedSeq, before.acceptedSeq);
      }
      const score = () => T.scoreUser(uid, null, T.stmt.acctGet.get(uid).epoch, 10);
      assert.strictEqual(score().accountPnl, 0);

      now += 800;
      T.compUpdate(base, 'usd', 100.005, now); // Deliberately no provider timestamp.
      assert.strictEqual(T.activeSource(base, now).key, 'usd');
      assert.strictEqual(T.comps.get(base).usd.ageKnown, false);
      assert.strictEqual(T.live.map.get(base).srcKey, 'usd');
      assert.strictEqual(T.live.map.get(base).pythPrice, 100.005);
      assert.strictEqual(T.boostLevCap(base, 500, now), 100,
        'the actual admitted fallback cannot qualify this round\'s 500x lineage');
      assert.deepStrictEqual(comp.roundMarkGet(round.id, base, now), before);
      assert.strictEqual(T.__roundExecutionMark(base, { round, at: now }).held, true);
      assert.strictEqual(score().accountPnl, 0, 'public fallback prices do not change round PnL');

      now += 200;
      const returnedAt = now;
      for (let elapsed = 0; elapsed < 10_000; elapsed += 200) {
        now = returnedAt + elapsed;
        T.compUpdate(base, 'lazer', 100.01, now, now);
        T.compUpdate(base, 'usd', 100.005, now);
        assert.strictEqual(T.activeSource(base, now).key, 'usd');
        assert.strictEqual(comp.roundMarkGet(round.id, base, now).acceptedSeq, before.acceptedSeq);
      }
      now = returnedAt + 10_000;
      T.compUpdate(base, 'lazer', 100.01, now, now);
      T.compUpdate(base, 'usd', 100.005, now);
      const returned = comp.roundMarkGet(round.id, base, now);
      assert.strictEqual(T.activeSource(base, now).key, 'lazer');
      assert.strictEqual(T.live.map.get(base).srcKey, 'lazer');
      assert.strictEqual(returned.source, 'lazer');
      assert.strictEqual(returned.price, 100.01);
      assert.ok(returned.acceptedSeq > before.acceptedSeq);
      assert.strictEqual(returned.acceptedSeq, T.live.map.get(base).acceptedSeq);
      assert.strictEqual(returned.acceptedAt, now);
      assert.ok(returned.observedAt > returnedAt && returned.observedAt <= now);
      assert.ok(returned.originalValidUntil > now && returned.originalValidUntil <= now + 600);
      assert.strictEqual(T.__roundExecutionMark(base, { round, at: now }).held, false);
      assert.strictEqual(score().accountPnl, 0.002);
      assert.strictEqual(score().boostPnl, 0.001);

      now += 200;
      T.compUpdate(base, 'lazer', 100.02, now, now);
      const latest = comp.roundMarkGet(round.id, base, now);
      assert.ok(latest.acceptedSeq > returned.acceptedSeq, 'subsequent primary observations continue advancing');
      const marks = T.markSetFor([uid], now, { strict: true, round: CT.q.get.get(round.id) });
      assert.strictEqual(marks[base], latest.price);
      assert.strictEqual(marks[base + '-BOOST'], latest.price);
      assert.strictEqual(score().accountPnl, 0.004);
      assert.strictEqual(score().boostPnl, 0.002);
      assert.ok(Math.abs(T.hotValueOf(uid, base, T.stmt.acctGet.get(uid).epoch, marks, now) - 0.004) < 1e-9,
        'the Hot-family valuation uses the same returned mark, not the public fallback');
      for (const symbol of [base + '-BOOST', base]) {
        const closed = fill(symbol, 'SELL', latest.price);
        const provenance = closed.decisionContext.inputs.roundExecution;
        assert.strictEqual(closed.price, 100.02);
        assert.strictEqual(closed.referenceMark, latest.price);
        assert.strictEqual(closed.realizedPnl, 0.002);
        assert.strictEqual(provenance.source, 'lazer');
        assert.strictEqual(provenance.acceptedSeq, latest.acceptedSeq);
        assert.strictEqual(provenance.observedAt, latest.observedAt);
      }
      assert.strictEqual(score().accountPnl, 0.004, 'closing at the returned mark preserves the marked PnL');
      assert.deepStrictEqual(T.stmt.posByUser.all(uid), []);
      assert.strictEqual(T.compRankSnapshot().complete, true);
      assert.strictEqual(CT.q.get.get(round.id).paused_since, null);
      assert.strictEqual(comp.phaseNow(now).phase, 'boost', 'the round did not have to end for failback');
    } finally {
      try {
        if (round && comp.currentRound()?.id === round.id) {
          // Clear only this fixture's ordinary base leg; abort owns its
          // durable Boost aliases and their normal flattening cleanup.
          const position = T.stmt.posGet.get(uid, base);
          if (position) {
            const mark = T.__roundExecutionMark(base, { round: CT.q.get.get(round.id), at: now });
            assert.ok(mark, 'fixture cleanup requires the real round mark');
            T.applyFill(uid, { symbol: base, orderSide: 'SELL', size: position.size,
              px: mark.price, feeBps: 0, kind: 'MARKET', leverage: position.leverage,
              marginMode: 'isolated', at: now, referenceMark: mark.price });
          }
          comp.abortRound(round.id, { force: true });
        }
      } finally {
        T.__clearPauses();
        auth.authHealth = previousHealth;
        comp.wire({ defaultPricePolicy: () => 'strict', initializeRoundMarks: null });
        wire();
        Date.now = previousNow;
      }
    }
  });

  await test('new competition defaults seal latest-backup execution without retrofitting legacy rounds', async () => {
    abortLive();
    comp.wire({ defaultPricePolicy: () => LAST_ACCEPTED_POLICY, initializeRoundMarks: () => {} });
    let round;
    const auth = require('./auth-shim.js'), previousSession = auth.validateSession;
    try {
      round = comp.createRound({ id: 'latest-backup-default', candidates: ['BTC', 'ETH', 'SOL'], players: roster() });
      assert.strictEqual(comp.backupExecutionPolicyOf(round), 'latest-available-500-v1');
      assert.strictEqual(CT.sealedDrawValid(round), true);
      auth.validateSession = async () => ({ id: USERS[0] });
      const mine = await invoke(P.compMe, { url: '/api/paper/comp/me',
        headers: { cookie: 'phoenix_session=synthetic-policy-fixture' } });
      assert.strictEqual(mine.body.round.backupExecutionPolicy, 'latest-available-500-v1');
      const baseline = await invoke(P.compBaseline, { url: '/api/paper/comp/baseline' });
      assert.strictEqual(baseline.body.armed.find(r => r.id === round.id).backupExecutionPolicy,
        'latest-available-500-v1');
      assert.throws(() => CT.db.prepare('UPDATE paper_rounds SET backup_execution_policy=NULL WHERE id=?')
        .run(round.id), /backup execution policy is immutable/);
      CT.db.prepare('UPDATE paper_rounds SET backup_execution_policy=backup_execution_policy WHERE id=?').run(round.id);
      assert.strictEqual(CT.q.get.get(round.id).draw_commit, round.draw_commit);
      assert.strictEqual(CT.sealedDrawValid({ ...round, backup_execution_policy: null }), false);
      const prior = CT.q.get.get('policy-live-lazer-failback');
      assert.strictEqual(prior.backup_execution_policy, null);
      assert.deepStrictEqual(comp.backupExecutionPolicyFields(prior), {});
    } finally {
      auth.validateSession = previousSession;
      if (round) CT.q.setStatus.run('aborted', Date.now(), round.id);
      comp.wire({ defaultPricePolicy: () => 'strict', initializeRoundMarks: null });
    }
  });

  await test('dual-cap major records commit truthful metadata and reject partial or foreign policy groups', () => {
    withRoundMarkFixture('latest-backup-records', ({ round, clock, record, commit }) => {
      const row = record(1, clock.now, { source: 'usd', acceptedLeverageCap: 100,
        executionLeverageCap: 500, leveragePolicy: 'latest-available-500-v1', sourceAgeKnown: false });
      for (const key of ['executionLeverageCap', 'leveragePolicy', 'sourceAgeKnown']) {
        const malformed = { ...row }; delete malformed[key];
        assert.strictEqual(comp.roundMarkExecutionLeverage(round, malformed), 0);
        assert.throws(() => commit(malformed, null), /invalid qualified round mark/);
      }
      for (const patch of [{ executionLeverageCap: 1000 }, { leveragePolicy: 'future-v2' },
        { sourceAgeKnown: 'false' }, { acceptedLeverageCap: 99 }, { acceptedLeverageCap: Infinity }]) {
        assert.strictEqual(comp.roundMarkExecutionLeverage(round, { ...row, ...patch }), 0);
      }
      commit(row, null);
      const saved = comp.roundMarkGet(round.id, 'BTC', clock.now);
      for (const key of ['acceptedLeverageCap', 'executionLeverageCap', 'leveragePolicy', 'sourceAgeKnown']) {
        assert.strictEqual(saved[key], row[key]);
        assert.strictEqual(comp.roundMarkHistory(round.id, 'BTC',
          { from: clock.now, to: clock.now + 1 }).records[0][key], row[key]);
      }
    }, { backupExecutionPolicy: 'latest-available-500-v1' });
  });

  await test('latest-backup execution never broadens legacy, strict, non-major or other-leverage scope', () => {
    const enabled = { format_version: 2, price_policy: LAST_ACCEPTED_POLICY, boost_leverage: 500,
      backup_execution_policy: 'latest-available-500-v1' };
    const raw = { base: 'ETH', acceptedLeverageCap: 100 };
    const group = { ...raw, executionLeverageCap: 500, leveragePolicy: 'latest-available-500-v1', sourceAgeKnown: false };
    for (const base of ['BTC', 'ETH', 'SOL', 'XRP']) for (const suffix of ['', '-HOT', '-BOOST']) {
      assert.strictEqual(comp.usesLatestBackupPricing(enabled, base + suffix), true);
      assert.strictEqual(comp.roundMarkExecutionLeverage(enabled, { ...group, base }), 500);
    }
    for (const round of [null, { ...enabled, backup_execution_policy: null },
      { ...enabled, format_version: 1 }, { ...enabled, price_policy: 'strict' },
      { ...enabled, boost_leverage: 100 }, { ...enabled, boost_leverage: 400 },
      { ...enabled, boost_leverage: 1000 }]) {
      assert.strictEqual(comp.usesLatestBackupPricing(round, 'ETH'), false);
      assert.strictEqual(comp.roundMarkExecutionLeverage(round, raw), 100);
      assert.strictEqual(comp.roundMarkExecutionLeverage(round, group), 0);
    }
    assert.strictEqual(comp.roundMarkExecutionLeverage(enabled, { ...raw, base: 'DOGE' }), 100);
    assert.strictEqual(comp.roundMarkExecutionLeverage(enabled, { ...group, base: 'DOGE' }), 0);
    assert.strictEqual(comp.backupExecutionPolicyMatches('unknown', enabled), false);
  });

  await test('score and Hot evidence bind the sealed backup policy and complete dual-cap group', () => {
    withRoundMarkFixture('latest-backup-proof-policy', ({ round, clock, record, commit }) => {
      const clone = value => JSON.parse(JSON.stringify(value));
      commit(record(1, clock.now, { source: 'usd', acceptedLeverageCap: 100,
        executionLeverageCap: 500, leveragePolicy: 'latest-available-500-v1', sourceAgeKnown: false }), null);
      const row = comp.roundMarkGet(round.id, 'BTC', clock.now);
      const state = { pricePolicy: LAST_ACCEPTED_POLICY, backupExecutionPolicy: 'latest-available-500-v1',
        roundMarkLineage: 'per-record-v1', roundClockAvailable: true, engineBoot: row.acceptedBoot,
        positions: [{ symbol: 'BTC-BOOST', mark: row.price, leverage: 500 }], roundMarks: { BTC: row } };
      const validScore = value => T.__roundScoreEvidenceValid(value, clock.now, round);
      const evidence = T.__marketEvidenceAt('BTC', clock.now, 'HOT', 100, round);
      const attempt = { asset: 'BTC', ready: true, evidence };
      const validHot = value => T.__roundAttemptEvidenceValid(value, clock.now, 'HOT', 100, round);
      assert.strictEqual(validScore(state), true);
      assert.strictEqual(validHot(attempt), true);
      for (const value of [null, 'future-v2']) {
        const score = clone(state), hot = clone(attempt);
        score.backupExecutionPolicy = value; hot.evidence.policy.backupExecutionPolicy = value;
        assert.strictEqual(validScore(score), false);
        assert.strictEqual(validHot(hot), false);
      }
      const withoutScore = clone(state), withoutHot = clone(attempt);
      delete withoutScore.backupExecutionPolicy; delete withoutHot.evidence.policy.backupExecutionPolicy;
      assert.strictEqual(validScore(withoutScore), false);
      assert.strictEqual(validHot(withoutHot), false);
      for (const key of ['executionLeverageCap', 'leveragePolicy', 'sourceAgeKnown']) {
        const score = clone(state), hot = clone(attempt);
        delete score.roundMarks.BTC[key]; delete hot.evidence.observation[key];
        assert.strictEqual(validScore(score), false);
        assert.strictEqual(validHot(hot), false);
      }
      // A legacy proof remains valid on its original intrinsic qualification,
      // and cannot inherit the override by adding or stripping metadata.
      const legacyRound = { ...round, backup_execution_policy: null };
      const legacyState = clone(state), legacyAttempt = clone(attempt);
      delete legacyState.backupExecutionPolicy; delete legacyAttempt.evidence.policy.backupExecutionPolicy;
      for (const key of ['executionLeverageCap', 'leveragePolicy', 'sourceAgeKnown']) {
        delete legacyState.roundMarks.BTC[key]; delete legacyAttempt.evidence.observation[key];
      }
      assert.strictEqual(T.__roundScoreEvidenceValid(legacyState, clock.now, legacyRound), false);
      assert.strictEqual(T.__roundAttemptEvidenceValid(legacyAttempt, clock.now, 'HOT', 100, legacyRound), false);
      legacyState.roundMarks.BTC.acceptedLeverageCap = 500;
      legacyAttempt.evidence.observation.acceptedLeverageCap = 500;
      assert.strictEqual(T.__roundScoreEvidenceValid(legacyState, clock.now, legacyRound), true);
      assert.strictEqual(T.__roundAttemptEvidenceValid(legacyAttempt, clock.now, 'HOT', 100, legacyRound), true);
      legacyState.backupExecutionPolicy = 'latest-available-500-v1';
      legacyAttempt.evidence.policy.backupExecutionPolicy = 'latest-available-500-v1';
      assert.strictEqual(T.__roundScoreEvidenceValid(legacyState, clock.now, legacyRound), false);
      assert.strictEqual(T.__roundAttemptEvidenceValid(legacyAttempt, clock.now, 'HOT', 100, legacyRound), false);
    }, { backupExecutionPolicy: 'latest-available-500-v1' });
  });

  await test('round mark decoding preserves current SQL bytes, rollback and output isolation', () => {
    withRoundMarkFixture('round-mark-decode-isolation', ({ round, clock, record, commit }) => {
      const first = record(1, clock.now);
      commit(first, null);
      const read = () => comp.roundMarkGet(round.id, 'BTC', clock.now);
      const raw = CT.db.prepare('SELECT record_json FROM paper_round_marks WHERE round_id=? AND base=?');
      const update = CT.db.prepare('UPDATE paper_round_marks SET record_json=? WHERE round_id=? AND base=?');
      const original = raw.get(round.id, 'BTC').record_json;
      const output = read(); output.price = 999; output.source = 'caller-only';
      assert.deepStrictEqual(read(), { ...first, hardInvalid: false, hardFailure: null });
      assert.throws(() => CT.db.transaction(() => {
        const next = { ...first, price: 101, note: 'changed without a revision increment' };
        update.run(JSON.stringify(next), round.id, 'BTC');
        assert.strictEqual(read().price, 101, 'same-revision SQL changes are immediately visible');
        assert.strictEqual(read().note, next.note);
        throw new Error('decode rollback fixture');
      })(), /decode rollback fixture/);
      assert.strictEqual(raw.get(round.id, 'BTC').record_json, original);
      assert.strictEqual(read().price, first.price, 'rollback immediately selects the old bytes');
      for (const extra of [{ nested: { value: 'original' }, list: [{ value: 'original' }] },
        { note: 'x'.repeat(5000), nested: { value: 'original' } }]) {
        update.run(JSON.stringify({ ...first, ...extra }), round.id, 'BTC');
        const value = read(); value.nested.value = 'caller-only';
        if (value.list) value.list[0].value = 'caller-only';
        assert.strictEqual(read().nested.value, 'original');
        if (value.list) assert.strictEqual(read().list[0].value, 'original');
      }
      update.run(original, round.id, 'BTC');
      const failedAt = clock.now += 10;
      comp.roundMarkInvalidate(round.id, 'BTC', 'fresh SQL failure', failedAt);
      const failed = read(); assert.strictEqual(failed.hardInvalid, true);
      failed.hardFailure.reason = 'caller-only';
      assert.strictEqual(read().hardFailure.reason, 'fresh SQL failure');
      assert.strictEqual(comp.roundMarkGet(round.id, 'BTC', failedAt - 1).hardInvalid, false);
      assert.throws(() => CT.db.transaction(() => {
        CT.db.prepare('DELETE FROM paper_round_mark_breaks WHERE round_id=?').run(round.id);
        assert.strictEqual(read().hardInvalid, false, 'failure SQL is never memoized');
        throw new Error('failure rollback fixture');
      })(), /failure rollback fixture/);
      assert.strictEqual(read().hardInvalid, true);
    });
  });

  await test('held global feed short-circuit preserves exposed dependencies and total outage detection', () => {
    const auth = require('./auth-shim.js'), previousHealth = auth.authHealth;
    try {
      withRoundMarkFixture('held-feed-short-circuit', ({ round, clock, record, identity, commit }) => {
        auth.authHealth = () => ({ tradingAvailable: true, unavailableSince: null, validUntil: clock.now + 6000 });
        const fields = { source: 'usd', acceptedLeverageCap: 100, executionLeverageCap: 500,
          leveragePolicy: 'latest-available-500-v1', sourceAgeKnown: false };
        const btc = record(1, clock.now, fields), eth = record(2, clock.now, { ...fields, base: 'ETH' });
        commit(btc, null); commit(eth, null);
        comp.startRound(round.id, { at: clock.now });
        const uid = USERS[0], epoch = T.stmt.acctGet.get(uid).epoch;
        T.stmt.posIns.run(uid, 'ETH', epoch, 'LONG', 0.1, eth.price, 100,
          clock.now, eth.price, clock.now, clock.now, 'isolated', 1);
        clock.now += 90_000;
        commit(record(3, clock.now, { ...fields, price: btc.price }), identity(btc));
        const originalGet = comp.roundMarkGet, reads = [];
        comp.roundMarkGet = (...args) => { reads.push(args[1]); return originalGet(...args); };
        try {
          assert.deepStrictEqual(T.__competitionFeedStatus(clock.now), { ok: true, since: null, validUntil: null });
          assert.strictEqual(reads.at(-1), 'BTC');
          assert.strictEqual(reads.includes('ETH'), false,
            'the scan stops at the first non-expiring ready mark without querying later markets');
        } finally { comp.roundMarkGet = originalGet; }
        assert.strictEqual(T.__competitionClockStatus(clock.now, { ignorePause: true }).ok, true,
          'age expiry alone retains the already-sealed held-price policy');
        comp.roundMarkInvalidate(round.id, 'ETH', 'exposure risk unavailable', clock.now);
        assert.strictEqual(T.__competitionFeedStatus(clock.now).ok, true, 'unrelated BTC still supplies the global feed');
        assert.strictEqual(T.__competitionClockStatus(clock.now, { ignorePause: true }).ok, false,
          'held ETH exposure remains a separately checked clock dependency');
        T.stmt.posDel.run(uid, 'ETH');
        assert.strictEqual(T.__competitionClockStatus(clock.now, { ignorePause: true }).ok, true,
          'deleting exposure is visible immediately without an authority TTL');
        comp.roundMarkInvalidate(round.id, 'BTC', 'all accepted marks unavailable', clock.now);
        assert.deepStrictEqual(T.__competitionFeedStatus(clock.now), { ok: false, since: clock.now, validUntil: null });
        assert.strictEqual(T.__competitionClockStatus(clock.now, { ignorePause: true }).ok, false,
          'a flat roster still detects complete loss of executable round authority');
      }, { backupExecutionPolicy: 'latest-available-500-v1' });
    } finally { auth.authHealth = previousHealth; }
  });

  console.log(`\ntwo-Hot format: ${passed}/${passed + failed} passed`);
  process.exitCode = failed ? 1 : 0;
})();
