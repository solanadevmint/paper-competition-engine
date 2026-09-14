'use strict';

// Each case opens its own disposable SQLite authority. No feed, HTTP request,
// live session or production state is involved in this source-transition test.
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawnSync } = require('child_process');

const cases = [
  'cached backup preserves its observation and lease',
  'older admissible backup is not confused with corroboration expiry',
  'no eligible backup remains unpriced without expiry retries',
  'source updates replace timers and shutdown cancels them',
  'maintenance cancels pending source transitions',
  'source persistence failure cannot publish a fallback',
  'cached fallback still passes the jump guard',
  'cached fallback still rolls back on risk failure',
  'a real Hot round scores a cached fallback once with clock expiry first',
  'a real Hot round scores a cached fallback once with source expiry first',
  'conservative published expiry defers once to the same component deadline',
  'a Hot close sharing the source deadline keeps canonical economic score',
  'reversed Hot-close/source timer order keeps the same score and pause ledger',
  'confidence observes unique ordered quotes before learning or readmission',
  'equal-time conflicting quotes cannot train or clear rejection',
  'future-stamped redundant observations retain their first effective time',
  'price exponent and confidence scalars fail closed without quote renewal',
  'future identity capacity is bounded and nonfuture recovery remains available',
  'timestamp rejection preserves stale telemetry and the fast600ms deadline',
  'explicit round carry keeps one unchanged lineage after raw expiry',
  'round carry cannot use another boot or a hard-invalid lineage',
  'candidate preparation never grants or renews round execution authority',
  'round initialization adopts existing timestamps inside its transaction only',
  'round mutation acknowledgment is scoped but does not promise an old fill price',
  'round history preserves explicit unknown prefixes and hard gaps',
  'real round adoption and risk changes commit or roll back together',
  'a deadline crossed after commit preserves one accepted historical event',
  'a deadline crossed before commit preserves the previous held lineage',
  'frozen score evidence binds exact round marks without changing legacy proofs',
  'a paused non-recovering event cannot adopt a skipped-risk round mark',
  'two-base two-seat cold recovery commits one complete risk event',
  'a later seat risk failure rolls back every recovery mark and account',
  'batch recovery keeps the original monotonic source deadline at commit',
  'batch recovery rechecks identity after its final mark writes',
  'durable open Boost recovers private gates then retries only postcommit work',
  'old-boot history remains historical and respects hard and clock gaps',
  'a 100-only accepted fallback cannot recover a 500-qualified round lineage',
  'a late mark CAS failure rolls back the whole recovery batch',
  'a reached pending Boost opening recovers all durable dependencies without early trading',
  'every indexed market accepts a 60bps move while an unknown symbol retains the 50bps gate',
  'selected volatile assets still confirm larger moves despite fresh backups',
  'selected floors survive warmup and bound tightening without speeding up learning',
  'adaptive jump learning retains its ceiling and excludes larger outliers',
  'a lower configured jump ceiling also caps the selected cold and learned floors',
  'a malformed jump ceiling cannot disable cold or warmed jump confirmation',
];

if (!process.env.PAPER_SOURCE_EXPIRY_CASE) {
  const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'paper-source-expiry-'));
  let passed = 0;
  try {
    for (const [index, name] of cases.entries()) {
      const run = spawnSync(process.execPath, [__filename], {
        encoding: 'utf8', timeout: 10_000,
        env: { ...process.env, PAPER_DB: path.join(scratch, `case-${index}.db`),
          PAPER_SOURCE_EXPIRY_CASE: String(index + 1), PAPER_COMP_TOKEN: 'source-test-only' },
      });
      if (run.status !== 0 || run.error) {
        console.error(`  FAIL ${name}\n${run.stdout || ''}${run.stderr || ''}`);
        if (run.error) console.error(run.error.message);
        process.exitCode = 1;
      } else { passed++; console.log(`  ok   ${name}`); }
    }
  } finally { fs.rmSync(scratch, { recursive: true, force: true }); }
  console.log(`\n${passed} source-expiry assertions passed`);
} else {
  process.env.PHOENIX_SNAPSHOT_FILE = '/nonexistent/source-expiry-snapshot.json';
  process.env.PAPER_MAINTENANCE_FILE = process.env.PAPER_DB + '.maintenance';
  delete process.env.TG_BOT_TOKEN;
  delete process.env.OPS_ALERT_CHAT_ID;
  // New clamp cases own their policy inputs; each runs in a fresh child, so
  // neither inherited operator overrides nor this ceiling case leak between tests.
  if (Number(process.env.PAPER_SOURCE_EXPIRY_CASE) >= 40) {
    const clampCase = Number(process.env.PAPER_SOURCE_EXPIRY_CASE);
    process.env.PAPER_CLAMP_JUMP_MAX = clampCase === 45 ? 'invalid-ceiling'
      : clampCase === 44 ? '0.006' : '0.05';
    process.env.PAPER_CLAMP_TAIL_MULT = '4';
    process.env.PAPER_CLAMP_CLUSTER = '0.003';
  }
  const P = require('./paper.js'), T = P.__test;
  const original = { now: Date.now, setTimeout, clearTimeout };
  const base = Date.now();
  let now = base;
  const pending = new Set();
  const which = Number(process.env.PAPER_SOURCE_EXPIRY_CASE);
  const easedClampAssets = ['LIT', 'VVV', 'SKR', 'CHIP', 'ANSEM', 'LINK', 'NEAR', 'VIRTUAL', 'BTC'];
  const baselineClampAssets = ['ZZUNKNOWN'];
  Date.now = () => now;
  global.setTimeout = (callback, delay) => {
    const timer = { callback, at: now + Math.ceil(Math.max(0, Number(delay) || 0)), unref() { return this; } };
    pending.add(timer); return timer;
  };
  global.clearTimeout = (timer) => {
    if (!pending.delete(timer)) original.clearTimeout(timer);
  };
  function advance(offset) {
    const until = base + offset;
    let calls = 0;
    for (;;) {
      const available = [...pending].filter(t => t.at <= until);
      // Exercise both orderings of callbacks that share an exact deadline.
      if (which === 10 || which === 13) available.reverse();
      const timer = available.sort((a, b) => a.at - b.at)[0];
      if (!timer) break;
      assert.ok(++calls < 200, 'expiry timer must not create a tight retry loop');
      pending.delete(timer); now = timer.at; timer.callback();
    }
    now = until;
  }
  const quote = () => T.live.map.get('ANSEM');
  const current = () => T.compPriceReady('ANSEM', now);
  function seed(backupOffset = 200, backupPrice = .235) {
    T.__startSourceExpiry();
    T.compUpdate('ANSEM', 'lazer', .235, now, now);
    // A component update can arrive without moving the followed primary.
    T.compUpdate('ANSEM', 'venue', backupPrice, base + backupOffset);
    return { component: { ...T.comps.get('ANSEM').venue }, seq: quote().acceptedSeq };
  }
  const feed = (at, fields = {}) => ({ price: '10000', exponent: '-2', confidence: '1',
    feedUpdateTimestamp: String(at * 1000), ...fields });
  const observe = (at, fields = {}, sym = 'ANSEM', envelope = at) => T.__processLazerObservation(sym, feed(at, fields), envelope, now);
  const observation = () => T.__lazerObservationState('ANSEM');
  try {
    if (which === 1 || which === 2) {
      const seeded = seed(which === 2 ? -6000 : 200);
      advance(1199); assert.strictEqual(quote().srcKey, 'lazer');
      advance(1200);
      assert.strictEqual(quote().srcKey, 'venue');
      assert.strictEqual(current(), true);
      assert.strictEqual(T.readyForLeverage('ANSEM', 500, now), false,
        'an unageable venue backup never inherits the primary Boost tier');
      assert.deepStrictEqual(T.comps.get('ANSEM').venue, seeded.component,
        'transition cannot refresh or invent a component observation');
      assert.ok(Math.abs(quote().pythSrcAtMs - seeded.component.ts) < .1);
      assert.strictEqual(quote().acceptedSeq, seeded.seq + 1, 'exactly one ordinary source-transition risk event');
      assert.strictEqual(T.db.prepare('SELECT source FROM paper_index_source WHERE symbol = ?').get('ANSEM').source, 'venue');
      assert.strictEqual(T.__refreshExpiredSources(now), 0);
      assert.strictEqual(quote().acceptedSeq, seeded.seq + 1, 'rechecking cannot apply the event twice');
      advance(11200);
      assert.strictEqual(current(), false, 'a cached quote is not renewed when its published receipt guard expires');
      assert.strictEqual(T.__sourceExpiryTimerCount(), 0);
    } else if (which === 3) {
      seed(-29900);
      advance(1200); assert.strictEqual(quote().srcKey, 'lazer');
      assert.strictEqual(current(), false);
      assert.strictEqual(T.__sourceExpiryTimerCount(), 0);
      advance(60000); assert.strictEqual(T.__sourceExpiryTimerCount(), 0);
    } else if (which === 4) {
      seed(); advance(500);
      T.compUpdate('ANSEM', 'lazer', .235, now, now);
      assert.strictEqual(T.__sourceExpiryTimerCount(), 1);
      advance(1200); assert.strictEqual(quote().srcKey, 'lazer');
      advance(1699); assert.strictEqual(quote().srcKey, 'lazer');
      advance(1700); assert.strictEqual(quote().srcKey, 'venue');
      P.stopSourceExpiry(); assert.strictEqual(T.__sourceExpiryTimerCount(), 0);
      T.compUpdate('ANSEM', 'venue', .235, now + 1);
      assert.strictEqual(T.__sourceExpiryTimerCount(), 0);
    } else if (which === 5) {
      seed(); fs.writeFileSync(process.env.PAPER_MAINTENANCE_FILE, 'test-only');
      advance(1200); assert.strictEqual(quote().srcKey, 'lazer');
      assert.strictEqual(T.__sourceExpiryTimerCount(), 0);
      fs.unlinkSync(process.env.PAPER_MAINTENANCE_FILE);
      T.compUpdate('ANSEM', 'lazer', .235, now, now);
      assert.strictEqual(T.__sourceExpiryTimerCount(), 1);
    } else if (which === 6) {
      const seeded = seed();
      T.db.exec("CREATE TEMP TRIGGER reject_source BEFORE UPDATE ON paper_index_source WHEN NEW.source='venue' BEGIN SELECT RAISE(ABORT,'isolated source failure'); END");
      advance(1200);
      assert.strictEqual(quote().srcKey, 'lazer');
      assert.strictEqual(quote().acceptedSeq, seeded.seq);
      assert.strictEqual(current(), false);
      assert.strictEqual(T.__sourceExpiryTimerCount(), 0);
      assert.strictEqual(T.db.prepare('SELECT source FROM paper_index_source WHERE symbol = ?').get('ANSEM').source, 'lazer');
    } else if (which === 7) {
      const seeded = seed(200, .3);
      advance(1200);
      assert.strictEqual(quote().acceptedSeq, seeded.seq);
      assert.strictEqual(current(), false);
      assert.strictEqual(T.confirming.has('ANSEM'), true);
      assert.strictEqual(T.__sourceExpiryTimerCount(), 0);
    } else if (which === 8) {
      const seeded = seed();
      const all = T.stmt.posBySymbolTree.all;
      T.stmt.posBySymbolTree.all = () => { throw new Error('isolated risk failure'); };
      try { advance(1200); } finally { T.stmt.posBySymbolTree.all = all; }
      assert.strictEqual(quote().acceptedSeq, seeded.seq);
      assert.strictEqual(quote().srcKey, 'lazer');
      assert.strictEqual(current(), false);
      assert.strictEqual(T.__sourceExpiryTimerCount(), 0);
    } else if ([9, 10, 12, 13].includes(which)) {
      const vm = require('vm');
      const source = fs.readFileSync(path.join(__dirname, 'test-two-hot.js'), 'utf8');
      const end = source.indexOf('(async () => {');
      assert.ok(end > 0);
      // Reuse the existing deterministic draw/seat fixture, then open every
      // crossed Hot boundary normally. No succeeded/Hot state is fabricated.
      const H = vm.compileFunction(source.slice(0, end) + '\nreturn {comp,CT,create,start,setActiveOffset,abortLive};', ['require'])(require);
      const r = H.create('source-expiry-hot', { candidates: ['ANSEM', 'BTC', 'ETH'] });
      H.start(r.id);
      const rr = H.CT.q.get.get(r.id), draw = H.CT.privateDrawOf(rr);
      const closing = which === 12 || which === 13;
      const closeOffset = draw.hot1.activation + H.comp.planOf(rr).hotDuration;
      const target = closing ? closeOffset - T.staleMsForSym(draw.hot1.asset, 'lazer')
        : draw.hot1.activation + 1000;
      H.setActiveOffset(r.id, target);
      const shifted = H.CT.q.get.get(r.id);
      for (const offset of H.comp.boundariesOf(shifted).filter(at => at <= target)) {
        const due = shifted.started_at + offset;
        for (const sym of ['ANSEM', 'BTC', 'ETH', 'SOL', 'XRP']) T.recordMark(sym, 100, due, 2, 0, 'lazer', due);
        H.CT.fireBoundary(r.id, offset, due);
        assert.strictEqual(H.CT.q.bGet.get(r.id, offset).status, 'succeeded');
      }
      assert.strictEqual(H.comp.phaseNow().phase, 'hot');
      const sym = H.CT.q.get.get(r.id).hot1_active_base;
      // Make the followed primary explicit before enabling real health hooks.
      for (const component of Object.values(T.comps.get(sym) || {})) {
        component.srcAt = base - 60000; component.ts = base - 60000; component.age0 = 60000;
      }
      T.compUpdate(sym, 'lazer', 100, now, now);
      H.comp.wire({ ensureClockHealth: T.__ensureCompetitionClockHealth,
        marketAvailability: (s, kind, at, lev) => T.__marketAvailability(s, kind === 'BOOST' ? lev || 500 : 100, at) });
      const uid = H.comp.playersOf(r.id)[0].user_id;
      T.applyFill(uid, { symbol: sym, orderSide: 'BUY', size: 1, px: 100,
        feeBps: 0, kind: 'MARKET', leverage: 100, marginMode: 'isolated',
        executionSource: 'test', referenceMark: 100 });
      if (closing) H.CT.schedule(r.id);
      T.__startSourceExpiry();
      advance(200);
      T.compUpdate(sym, 'venue', 100.01, now);
      const component = { ...T.comps.get(sym).venue };
      const priorSeq = T.live.map.get(sym).acceptedSeq;
      const primaryExpiry = T.live.map.get(sym).pythSrcAtMs + T.staleMsForSym(sym, 'lazer');
      const countFills = () => T.db.prepare('SELECT COUNT(*) n FROM paper_fills WHERE user_id = ?').get(uid).n;
      const fillsBefore = countFills();
      const endsBefore = H.CT.q.get.get(r.id).ends_at;
      T.__armCompetitionClockExpiry(now);
      advance(T.staleMsForSym(sym, 'lazer'));
      const after = T.live.map.get(sym), round = H.CT.q.get.get(r.id);
      assert.strictEqual(after.srcKey, 'venue');
      assert.strictEqual(after.acceptedSeq, priorSeq + 1);
      assert.deepStrictEqual(T.comps.get(sym).venue, component);
      assert.strictEqual(round.paused_since, null, 'a valid fallback must recover the round');
      const banked = Math.max(0, Math.ceil(now) - Math.floor(primaryExpiry));
      assert.strictEqual(Number(round.paused_ms), banked,
        'the ledger conservatively banks only the actual published-quote gap');
      assert.strictEqual(round.ends_at, endsBefore + banked,
        'no valid competition time is consumed by source-transition ordering');
      assert.strictEqual(countFills(), fillsBefore);
      const board = T.compRankSnapshot();
      assert.strictEqual(board.complete, true);
      assert.ok(Math.abs(board.players[0].accountPnl - .01) < 1e-8);
      assert.ok(Math.abs(board.players[0].hotBonus - .01) < 1e-8);
      assert.ok(Math.abs(board.players[0].score - .02) < 1e-8);
      assert.strictEqual(T.__refreshExpiredSources(now), 0);
      assert.deepStrictEqual(T.compRankSnapshot().players, board.players,
        'a repeated expiry check cannot double Hot economic PnL');
      if (closing) {
        advance(T.staleMsForSym(sym, 'lazer') + banked);
        assert.strictEqual(H.CT.q.bGet.get(r.id, closeOffset).status, 'succeeded',
          'the actual Hot-close boundary commits after the conservative pause is banked');
        assert.strictEqual(H.comp.phaseNow().phase, 'build');
        const finalHotBoard = T.compRankSnapshot();
        assert.strictEqual(finalHotBoard.complete, true);
        assert.ok(Math.abs(finalHotBoard.players[0].accountPnl - .01) < 1e-8);
        assert.ok(Math.abs(finalHotBoard.players[0].hotBonus - .01) < 1e-8);
        assert.ok(Math.abs(finalHotBoard.players[0].score - .02) < 1e-8);
        assert.strictEqual(countFills(), fillsBefore);
      }
      H.abortLive();
    } else if (which === 11) {
      seed(); P.stopSourceExpiry();
      quote().pythSrcAtMs = base - 2;
      T.__startSourceExpiry();
      advance(1198);
      assert.strictEqual(quote().srcKey, 'lazer');
      assert.strictEqual(quote().pythSrcAtMs, base - 2, 'deferral must not renew the old mark');
      assert.strictEqual(T.__sourceExpiryTimerCount(), 1);
      advance(1200);
      assert.strictEqual(quote().srcKey, 'venue');
      assert.strictEqual(current(), true);
      assert.ok(T.__sourceExpiryTimerCount() <= 36);
    } else if (which === 14) {
      assert.strictEqual(observe(now), 'eligible');
      const accepted = { ...quote() }, component = { ...T.comps.get('ANSEM').lazer };
      advance(10); assert.strictEqual(observe(base), 'duplicate');
      assert.strictEqual(observation().confidenceSamples, 1);
      assert.deepStrictEqual(quote(), accepted);
      advance(100); assert.strictEqual(observe(now, { confidence: '100' }), 'confidence');
      assert.strictEqual(observation().rejected, true);
      assert.strictEqual(observation().confidenceSamples, 2);
      advance(150); assert.strictEqual(observe(base + 50), 'older');
      assert.strictEqual(observe(base + 100, { confidence: '100' }), 'duplicate');
      assert.strictEqual(observation().rejected, true);
      assert.strictEqual(observation().confidenceSamples, 2);
      assert.deepStrictEqual(T.comps.get('ANSEM').lazer, component);
      assert.deepStrictEqual(quote(), accepted);
      advance(200); assert.strictEqual(observe(now), 'eligible');
      assert.strictEqual(observation().rejected, false);
      assert.strictEqual(observation().confidenceSamples, 3);
      assert.strictEqual(quote().acceptedSeq, accepted.acceptedSeq + 1);
    } else if (which === 15) {
      observe(now); const accepted = { ...quote() };
      advance(10); assert.strictEqual(observe(base, { confidence: '2' }), 'conflict');
      assert.strictEqual(observation().rejected, true);
      assert.strictEqual(observation().confidenceSamples, 1);
      assert.deepStrictEqual(quote(), accepted);
      assert.strictEqual(observe(base), 'duplicate');
      assert.strictEqual(observation().rejected, true, 'an identical old copy cannot readmit after conflict');
      advance(50); assert.strictEqual(observe(now), 'eligible');
      assert.strictEqual(observation().rejected, false);
      assert.strictEqual(quote().acceptedSeq, accepted.acceptedSeq + 1);
    } else if (which === 16) {
      const a = base + 1500, b = base + 1600;
      assert.strictEqual(observe(a), 'eligible');
      const accepted = { ...quote() };
      assert.strictEqual(T.comps.get('ANSEM').lazer.srcAt, base);
      assert.strictEqual(observe(b, { confidence: '2' }), 'duplicate',
        'different future raw identities in the same callback are nonnew, not conflicting');
      assert.strictEqual(observation().futureCount, 2);
      advance(100);
      assert.strictEqual(observe(a), 'duplicate');
      assert.strictEqual(observe(b, { confidence: '2' }), 'duplicate');
      assert.deepStrictEqual(quote(), accepted, 'later copies cannot reclamp the same future timestamp');
      advance(200); assert.strictEqual(observe(now), 'eligible');
      const newer = { ...quote() }, learned = observation().confidenceSamples;
      advance(300); assert.strictEqual(observe(a, { confidence: '100' }), 'older');
      assert.strictEqual(observation().rejected, false);
      assert.strictEqual(observation().confidenceSamples, learned);
      assert.deepStrictEqual(quote(), newer);
      advance(1500); assert.strictEqual(observe(b, { confidence: '2' }), 'older');
      assert.strictEqual(current(), false, 'an old future identity cannot renew its expired accepted receipt');
      assert.deepStrictEqual(quote(), newer);
    } else if (which === 17) {
      observe(now); const accepted = { ...quote() };
      let offset = 0;
      const malformed = [];
      for (const key of ['price', 'exponent', 'confidence']) {
        for (const value of [null, '', false]) malformed.push({ [key]: value });
      }
      malformed.push({ exponent: 309 }, { price: 1e-308, exponent: 308, confidence: 1e308 });
      for (const fields of malformed) {
        advance(offset += 10);
        assert.strictEqual(observe(now, fields), 'malformed');
        assert.strictEqual(observation().rejected, true);
        assert.strictEqual(observation().confidenceSamples, 1);
        assert.deepStrictEqual(quote(), accepted);
      }
      advance(offset += 10);
      assert.strictEqual(observe(now, { price: 10000, exponent: -2, confidence: 1 }), 'eligible');
      assert.strictEqual(observation().rejected, false);
      assert.strictEqual(quote().acceptedSeq, accepted.acceptedSeq + 1);
      assert.strictEqual(quote().pythPrice, 100);
    } else if (which === 18) {
      const gate = T.__createLazerObservationGate(2);
      const send = (raw, callback, fields = {}, sym = 'ANSEM') => gate.observe(sym, feed(raw, fields), raw, callback, 1200, 2000).status;
      // Out-of-order raw insertion is intentional. Queue cleanup may retain a
      // safely old tail behind a future head but must never forget a live ID.
      assert.strictEqual(send(base + 200, base), 'new');
      assert.strictEqual(send(base + 100, base), 'duplicate');
      assert.strictEqual(send(base + 300, base + 1), 'capacity');
      assert.strictEqual(gate.snapshot('ANSEM').futureCount, 2);
      assert.strictEqual(send(base + 150, base + 150), 'new', 'nonfuture recovery bypasses future capacity');
      assert.strictEqual(gate.snapshot('ANSEM').futureCount, 2);
      assert.strictEqual(send(base + 201, base + 201), 'new');
      assert.strictEqual(gate.snapshot('ANSEM').futureCount, 0);
      assert.strictEqual(send(base + 300, base + 202), 'new');
      assert.strictEqual(send(base + 100, base + 1), 'older', 'a backward wall clock cannot rewind the watermark');
      assert.strictEqual(gate.snapshot('ANSEM').at, base + 202);
      assert.strictEqual(send(base + 50, base, {}, 'SKR'), 'new', 'the bound is per feed, not a global outage');
      assert.ok(gate.snapshot('ANSEM').futureQueueSize <= 4);
      assert.strictEqual(T.__createLazerObservationGate().snapshot('BTC').futureLimit, 4096);
    } else if (which === 19) {
      assert.strictEqual(observe(base, {}, 'BTC'), 'eligible');
      const accepted = { ...T.live.map.get('BTC') };
      const before = T.__marketDiagnosticSnapshot('BTC', now, true, true).history.counters.staleIngress;
      advance(100);
      assert.strictEqual(observe(base - 601, {}, 'BTC'), 'stale_time');
      assert.strictEqual(observe(base + 2101, {}, 'BTC'), 'future_time');
      assert.strictEqual(observe(base, { feedUpdateTimestamp: null }, 'BTC', null), 'missing_time');
      for (const badTime of [true, false, '', 'not-a-time', 0, -1, Infinity]) {
        assert.strictEqual(observe(now, { feedUpdateTimestamp: badTime }, 'BTC'), 'missing_time',
          'a present malformed per-feed time cannot borrow a valid envelope timestamp');
      }
      const after = T.__marketDiagnosticSnapshot('BTC', now, true, true).history.counters.staleIngress;
      assert.strictEqual(after, before + 1, 'the stable stale-observation telemetry code is retained');
      assert.strictEqual(T.__lazerObservationState('BTC').confidenceSamples, 1);
      advance(599); assert.strictEqual(T.compPriceReady('BTC', now), true);
      advance(600); assert.strictEqual(T.compPriceReady('BTC', now), false);
      assert.deepStrictEqual(T.live.map.get('BTC'), accepted);
    } else if (which >= 20 && which <= 25) {
      // The ordinary source processor supplies a genuinely accepted same-boot
      // record. A tiny in-memory round store isolates resolver semantics; the
      // competition suite separately exercises the durable journal/CAS.
      assert.strictEqual(observe(now, {}, 'BTC'), 'eligible');
      const accepted = { ...T.live.map.get('BTC') };
      const round = { id: 'continuity-fixture', status: 'running', format_version: 2,
        started_at: base, price_policy: 'last-accepted-v1' };
      let record = null, commits = 0;
      const copy = () => record && { ...record };
      P.comp.currentRound = () => round;
      P.comp.inRound = (uid, r) => uid === 7 && r === round;
      P.comp.pricePolicyOf = (r) => r?.price_policy || 'strict';
      P.comp.requiredRoundMarkLeverage = (_r, sym) => sym === 'BTC' ? 500 : 100;
      P.comp.roundMarkGet = (_rid, sym, at) => sym === 'BTC' && record
        && record.appliedAt <= at ? { ...copy(), hardInvalid: !!record.hardInvalid,
          hardFailure: record.hardFailure ?? null } : null;
      P.comp.roundMarkCommit = (_rid, next, previous) => {
        assert.strictEqual(T.db.inTransaction, true);
        assert.deepStrictEqual(previous, record
          ? { acceptedBoot: record.acceptedBoot, acceptedSeq: record.acceptedSeq } : null);
        record = { ...next }; commits++;
      };
      const prepared = T.__candidateRoundMark('BTC', T.live.map.get('BTC'), now, accepted.acceptedSeq, round);
      assert.ok(prepared && prepared.record.acceptedLeverageCap >= 500);
      if (which === 25) {
        const history = { complete: false, fromAvailable: now,
          breaks: [{ from: now + 1, to: now + 2 }, { from: now + 3, to: null }] };
        assert.deepStrictEqual(T.__projectRoundHistory(history), history);
        assert.throws(() => T.__projectRoundHistory({ ...history, complete: null }), /unavailable/);
        assert.throws(() => T.__projectRoundHistory({ ...history,
          breaks: [{ from: now + 2, to: now + 1 }] }), /unavailable/);
        assert.throws(() => T.__projectRoundHistory({ ...history, fromAvailable: NaN }), /unavailable/);
        const previousBoot = { ...prepared.record, acceptedBoot: 'previous-fixture-boot' };
        assert.deepStrictEqual(T.__projectRoundTickRows([previousBoot, prepared.record], now),
          [[previousBoot.appliedAt, previousBoot.price], [prepared.record.appliedAt, prepared.record.price]],
          'past-boot chart history does not grant that boot current execution authority');
        assert.throws(() => T.__projectRoundTickRows([{ ...previousBoot, acceptedBoot: '' }], now), /unavailable/);
      } else if (which === 24) {
        record = { ...prepared.record, acceptedSeq: accepted.acceptedSeq + 3 };
        const body = { pricePolicy: 'last-accepted-v1', roundId: round.id,
          acceptedBoot: record.acceptedBoot, acceptedSeq: accepted.acceptedSeq };
        assert.strictEqual(T.__roundPriceAcknowledged(body, 7, 'BTC-BOOST'), true);
        assert.strictEqual(T.__roundPriceAcknowledged({}, 7, 'BTC'), false);
        assert.strictEqual(T.__roundPriceAcknowledged({ ...body, acceptedBoot: 'old' }, 7, 'BTC'), false);
        assert.strictEqual(T.__roundPriceAcknowledged({ ...body, acceptedSeq: record.acceptedSeq + 1 }, 7, 'BTC'), false);
        assert.strictEqual(T.__roundPriceAcknowledged({ ...body, roundId: 'another' }, 7, 'BTC'), false);
        assert.strictEqual(T.__roundPriceAcknowledged({}, 8, 'BTC'), true, 'ordinary paper remains unchanged');
      } else if (which === 23) {
        assert.throws(() => T.__initializeRoundMarks(round, { at: now }), /transaction/);
        T.db.transaction(() => T.__initializeRoundMarks(round, { at: now }))();
        assert.strictEqual(commits, 1);
        assert.strictEqual(record.acceptedAt, accepted.pythAtMs);
        assert.strictEqual(record.observedAt, accepted.pythSrcAtMs);
        assert.strictEqual(record.appliedAt, now);
        assert.strictEqual(record.acceptedSeq, accepted.acceptedSeq);
        T.db.transaction(() => T.__initializeRoundMarks(round, { at: now }))();
        assert.strictEqual(commits, 1, 'repeated initialization never rewrites the lineage');
      } else {
        record = { ...prepared.record };
        const frozen = copy();
        if (which === 20) {
          advance(10000);
          assert.strictEqual(T.compPriceReady('BTC', now), false);
          for (const sym of ['BTC', 'BTC-HOT', 'BTC-BOOST']) {
            assert.strictEqual(T.__roundExecutionMark(sym, { round, at: now }).price, 100);
            assert.strictEqual(T.markOfFreshFor(sym, true, { userId: 7, forLeverage: 500 }), 100);
          }
          assert.strictEqual(T.markAt('BTC', now, { strict: true }), null);
          assert.strictEqual(T.markAt('BTC', now, { strict: true, round }), 100);
          assert.strictEqual(T.__roundPriceAvailability('BTC', 500, now, round).validUntil, null);
          assert.strictEqual(T.__roundExecutionMark('BTC', { round, at: now }).held, true);
          assert.deepStrictEqual(record, frozen);
          assert.strictEqual(T.live.map.get('BTC').acceptedSeq, accepted.acceptedSeq);
          const getRound = P.comp.__test.q.get.get;
          P.comp.__test.q.get.get = (id) => id === round.id ? round : null;
          try {
            assert.strictEqual(T.openAlias('BTC-HOT', round.id), true);
            assert.strictEqual(T.openAlias('BTC-BOOST', round.id), true);
            assert.throws(() => T.openAlias('BTC-HOT', 'foreign-round'), /another round/);
          } finally { P.comp.__test.q.get.get = getRound; }
          record.price = .123456789123;
          assert.strictEqual(T.markSetAt(['BTC'], now, { strict: true, round }).BTC, record.price,
            'boundary marks preserve the exact new-policy execution price');
        } else if (which === 21) {
          T.confirming.set('BTC', { since: now });
          assert.strictEqual(T.__roundExecutionMark('BTC', { round }).price, 100,
            'a rejected incoming candidate does not erase the held accepted price');
          record.hardInvalid = true; record.hardFailure = { reason: 'risk_failed', at: now };
          assert.strictEqual(T.__roundExecutionMark('BTC', { round }), null);
          record = { ...frozen, acceptedBoot: 'another-boot' };
          assert.strictEqual(T.__roundExecutionMark('BTC', { round }), null);
          record = { ...frozen, acceptedLeverageCap: 100 };
          assert.strictEqual(T.__roundExecutionMark('BTC', { round }), null);
          record = { ...frozen }; round.price_policy = 'strict';
          assert.strictEqual(T.__roundExecutionMark('BTC', { round }), null,
            'a historical strict round never inherits carry authority');
          round.price_policy = 'last-accepted-v1';
          P.comp.roundMarkGet = () => { throw new Error('ordinary unavailable authority fixture'); };
          const unavailable = T.__publicRoundExecution(round, 3, now);
          assert.strictEqual(unavailable.validUntil, now);
          assert.deepStrictEqual(unavailable.marks, []);
          assert.ok(unavailable.unavailable.includes('BTC'));
        } else {
          const candidate = T.__candidateRoundMark('BTC', T.live.map.get('BTC'), now, accepted.acceptedSeq + 1, round);
          assert.ok(candidate);
          assert.strictEqual(T.__roundExecutionMark('BTC', { round }).acceptedSeq, accepted.acceptedSeq);
          assert.deepStrictEqual(record, frozen);
          assert.strictEqual(commits, 0, 'preparation is neither admission nor durable publication');
          const entry = T.live.map.get('BTC');
          const sourceAt = entry.pythSrcAtMs;
          entry.pythSrcAtMs = now + 1;
          assert.strictEqual(T.__candidateRoundMark('BTC', entry, now, accepted.acceptedSeq + 1, round), null,
            'a non-adoptable future source time cannot become a transaction failure');
          entry.pythSrcAtMs = sourceAt;
          advance(601);
          assert.strictEqual(T.__candidateRoundMark('BTC', T.live.map.get('BTC'), now, accepted.acceptedSeq + 1, round), null);
          assert.deepStrictEqual(record, frozen, 'an expired candidate cannot renew the held price');
        }
      }
    } else if ((which >= 26 && which <= 28) || which === 30) {
      // Reuse only the ordinary draw/seat setup, not the suite runner. These
      // cases execute actual paper risk and durable round-mark transactions;
      // no listener, source subscription, real delay or live DB is involved.
      for (const sym of ['BTC', 'ETH', 'SOL', 'XRP']) observe(now, {}, sym);
      const cheap = { price: '123456789123', exponent: '-18', confidence: '1' };
      observe(now, cheap, 'ANSEM');
      const vm = require('vm');
      const fixture = fs.readFileSync(path.join(__dirname, 'test-two-hot.js'), 'utf8');
      const end = fixture.indexOf('(async () => {');
      assert.ok(end > 0);
      const H = vm.compileFunction(fixture.slice(0, end)
        + '\nreturn {comp,CT,create,start,abortLive};', ['require'])(require);
      H.comp.wire({ defaultPricePolicy: () => 'last-accepted-v1',
        initializeRoundMarks: T.__initializeRoundMarks });
      const r = H.create('continuity-commit-' + which, { candidates: ['BTC', 'ETH', 'ANSEM'] });
      H.start(r.id);
      const uid = H.comp.playersOf(r.id)[0].user_id;
      const held = () => H.comp.roundMarkGet(r.id, 'BTC', now);
      const initial = held();
      assert.ok(initial && initial.acceptedLeverageCap >= 500);
      T.applyFill(uid, { symbol: 'BTC', orderSide: 'BUY', size: 1, px: initial.price,
        feeBps: 0, kind: 'MARKET', leverage: 100, marginMode: 'isolated',
        executionSource: 'composite-index', referenceMark: initial.price });
      if (which === 30) {
        const auth = require('./auth-shim.js'), health = auth.authHealth;
        const markRun = T.stmt.posMark.run;
        const position = T.stmt.posGet.get(uid, 'BTC');
        let riskMarks = 0;
        auth.authHealth = () => ({ tradingAvailable: false,
          unavailableSince: now, validUntil: null });
        T.stmt.posMark.run = (...args) => { riskMarks++; return markRun.apply(T.stmt.posMark, args); };
        try {
          T.__pauseAt('BTC', now, 'ordinary identity-unavailable fixture');
          now += 50;
          observe(now, { price: '10001' }, 'BTC');
          assert.strictEqual(T.live.map.get('BTC').pythPrice, 100.01,
            'the independent raw public feed keeps its normal acceptance path');
          assert.ok(T.live.map.get('BTC').acceptedSeq > initial.acceptedSeq);
          assert.strictEqual(held().price, initial.price);
          assert.strictEqual(held().acceptedSeq, initial.acceptedSeq);
          assert.strictEqual(held().hardInvalid, false);
          assert.deepStrictEqual(T.stmt.posGet.get(uid, 'BTC'), position);
          assert.strictEqual(riskMarks, 0);
          const authority = T.__publicRoundExecution(H.comp.currentRound(), 10, now);
          assert.strictEqual(authority.validUntil, now);
        } finally { auth.authHealth = health; T.stmt.posMark.run = markRun; }
      } else if (which === 26) {
        const exact = H.comp.roundMarkGet(r.id, 'ANSEM', now);
        assert.ok(exact.price > 0 && exact.price < .000001);
        const open = T.applyFill(uid, { symbol: 'ANSEM', orderSide: 'BUY', size: 1_000_000,
          px: exact.price, feeBps: 0, kind: 'MARKET', leverage: 100, marginMode: 'isolated',
          executionSource: 'composite-index', referenceMark: exact.price });
        assert.strictEqual(open.price, exact.price);
        assert.strictEqual(open.decisionContext.inputs.roundExecution.price, exact.price,
          'exact closed provenance must not be rounded to zero by generic audit formatting');
        const execution = T.__execPxFor(uid, 'ANSEM', 'SELL', 1_000_000, exact.price);
        const close = T.applyFill(uid, { symbol: 'ANSEM', orderSide: 'SELL', size: 1_000_000,
          px: execution.px, feeBps: 0, kind: 'MARKET', executionSource: execution.source,
          referenceMark: exact.price });
        assert.strictEqual(close.price, exact.price);
        assert.strictEqual(close.decisionContext.inputs.roundExecution.price, exact.price);
        assert.strictEqual(close.realizedPnl, 0);
        now += 50;
        observe(now, { price: '10001' }, 'BTC');
        const adopted = held();
        assert.strictEqual(adopted.price, 100.01);
        assert.strictEqual(adopted.acceptedSeq, T.live.map.get('BTC').acceptedSeq);
        assert.ok(adopted.acceptedSeq > initial.acceptedSeq);
        const position = T.stmt.posGet.get(uid, 'BTC');
        assert.strictEqual(position.last_mark, adopted.price);
        const count = T.stmt.acctGet.get(uid).fills_count;
        const run = T.stmt.posMark.run;
        T.stmt.posMark.run = () => { throw new Error('ordinary rollback fixture'); };
        try { now += 50; observe(now, { price: '10002' }, 'BTC'); }
        finally { T.stmt.posMark.run = run; }
        assert.strictEqual(held().price, adopted.price);
        assert.strictEqual(held().acceptedSeq, adopted.acceptedSeq);
        assert.strictEqual(held().hardInvalid, true, 'a true failed risk write revokes authority');
        assert.strictEqual(T.live.map.get('BTC').acceptedSeq, adopted.acceptedSeq);
        assert.deepStrictEqual(T.stmt.posGet.get(uid, 'BTC'), position);
        assert.strictEqual(T.stmt.acctGet.get(uid).fills_count, count);
      } else {
        const bigint = process.hrtime.bigint, transaction = T.db.transaction;
        const commit = H.comp.roundMarkCommit;
        let offset = 0n, pendingCross = false, crossed = false;
        process.hrtime.bigint = () => bigint() + offset;
        H.comp.roundMarkCommit = (...args) => {
          const result = commit(...args);
          if (args[1].base === 'BTC') {
            if (which === 28) { offset += 700_000_000n; crossed = true; }
            else pendingCross = true;
          }
          return result;
        };
        T.db.transaction = function (...args) {
          const run = transaction.apply(this, args);
          return function (...values) {
            const outer = !T.db.inTransaction;
            const result = run(...values);
            if (outer && pendingCross) {
              pendingCross = false; crossed = true;
              offset += 700_000_000n; now += 700;
            }
            return result;
          };
        };
        const beforePosition = T.stmt.posGet.get(uid, 'BTC');
        const at = now + 50;
        try { now = at; observe(at, { price: '10001' }, 'BTC'); }
        finally {
          process.hrtime.bigint = bigint;
          T.db.transaction = transaction;
          H.comp.roundMarkCommit = commit;
        }
        assert.strictEqual(crossed, true, 'the fixture must cross the intended transaction edge');
        if (which === 27) {
          assert.strictEqual(held().price, 100.01);
          assert.strictEqual(held().acceptedAt, at);
          assert.strictEqual(held().acceptedSeq, T.live.map.get('BTC').acceptedSeq);
          assert.ok(held().acceptedSeq > initial.acceptedSeq);
          const history = T.__markHistory('BTC').find((row) => row.t === at);
          assert.ok(history && history.ok === true);
          assert.strictEqual(history.validUntil, held().originalValidUntil,
            'history keeps the exact originally admitted conservative source expiry');
          assert.ok(history.validUntil > held().acceptedAt && history.validUntil <= at + 600,
            'fractional monotonic source age may shorten, but never renew, the 600ms lifetime');
          assert.strictEqual(T.compPriceReady('BTC', now), false);
          assert.strictEqual(T.stmt.posGet.get(uid, 'BTC').last_mark, 100.01);
          assert.strictEqual(T.__roundExecutionMark('BTC', { round: H.comp.currentRound() }).price, 100.01);
        } else {
          assert.strictEqual(held().price, initial.price);
          assert.strictEqual(held().acceptedSeq, initial.acceptedSeq);
          assert.strictEqual(held().hardInvalid, false, 'mere age expiry never hard-invalidates the prior held mark');
          assert.strictEqual(T.live.map.get('BTC').acceptedSeq, initial.acceptedSeq);
          assert.deepStrictEqual(T.stmt.posGet.get(uid, 'BTC'), beforePosition);
          assert.strictEqual((T.__markHistory('BTC') || []).some((row) => row.t === at && row.ok), false);
        }
      }
      H.abortLive();
    } else if (which === 29) {
      observe(now, {}, 'BTC');
      const round = { id: 'proof-policy', price_policy: 'last-accepted-v1' };
      const raw = T.live.map.get('BTC');
      const mark = { base: 'BTC', price: raw.pythPrice, acceptedBoot: raw.acceptedBoot,
        acceptedSeq: raw.acceptedSeq, acceptedAt: raw.pythAtMs, observedAt: raw.pythSrcAtMs,
        appliedAt: now, source: raw.srcKey, originalValidUntil: now + 600,
        acceptedLeverageCap: 500, hardInvalid: false, hardFailure: null };
      const state = { pricePolicy: 'last-accepted-v1', engineBoot: raw.acceptedBoot,
        roundMarkLineage: 'per-record-v1', roundClockAvailable: true,
        positions: [{ symbol: 'BTC', mark: mark.price, leverage: 100 },
          { symbol: 'BTC-BOOST', mark: mark.price, leverage: 500 }], roundMarks: { BTC: mark } };
      assert.strictEqual(T.__roundScoreEvidenceValid(state, now + 10000, round), true,
        'an old but validly adopted mark remains valid score evidence');
      for (const patch of [{ price: mark.price + .000000001 }, { acceptedSeq: 0 },
        { acceptedBoot: '' }, { acceptedLeverageCap: 100 }, { hardInvalid: true },
        { observedAt: now + 1 }, { originalValidUntil: now }, { source: '' }]) {
        assert.strictEqual(T.__roundScoreEvidenceValid({ ...state, roundMarks: { BTC: { ...mark, ...patch } } }, now, round), false);
      }
      assert.strictEqual(T.__roundScoreEvidenceValid({ ...state, roundMarks: {} }, now, round), false);
      assert.strictEqual(T.__roundScoreEvidenceValid({ ...state, roundMarks: { BTC: mark, ETH: mark } }, now, round), false);
      assert.strictEqual(T.__roundScoreEvidenceValid({ ...state, pricePolicy: 'strict' }, now, round), false);
      assert.strictEqual(T.__roundScoreEvidenceValid({ ...state, roundClockAvailable: false }, now, round), false);
      assert.strictEqual(T.__roundScoreEvidenceValid({ ...state, roundMarkLineage: null }, now, round), false);
      assert.strictEqual(T.__roundScoreEvidenceValid({}, now, { price_policy: 'strict' }), true);
    } else if (which >= 31 && which <= 39) {
      for (const sym of ['BTC', 'ETH', 'SOL', 'XRP', 'ANSEM']) observe(now, {}, sym);
      const vm = require('vm');
      const fixture = fs.readFileSync(path.join(__dirname, 'test-two-hot.js'), 'utf8');
      const end = fixture.indexOf('(async () => {');
      assert.ok(end > 0);
      const H = vm.compileFunction(fixture.slice(0, end)
        + '\nreturn {comp,CT,create,start,abortLive,setActiveOffset};', ['require'])(require);
      // Real paper hooks, including monitorSegments' private gate read. These
      // fixtures do not replace the write barrier, phase or account risk.
      H.comp.wire({ defaultPricePolicy: () => 'last-accepted-v1',
        initializeRoundMarks: T.__initializeRoundMarks,
        aliasOpen: T.__competitionAliasOpen,
        marketReady: (sym, round) => T.__roundPriceAvailability(sym, 100, now, round).ready,
        marketAvailability: (sym, _kind, at, lev, round) => T.__roundPriceAvailability(sym, lev, at, round),
        marketReadyForBoost: (sym, lev, round) => T.__roundPriceAvailability(sym, lev, now, round).ready,
        boostLevCap: (sym, cap, round) => T.boostLevCap(sym, cap, now, round),
        ensureClockHealth: T.__ensureCompetitionClockHealth,
        pauseForSegment: (sym, kind, why, since) => T.__pauseAt(kind === 'BOOST' ? sym + '-BOOST' : sym, since || now, why),
      });
      const r = H.create('continuity-batch-' + which, { candidates: ['BTC', 'ETH', 'ANSEM'],
        players: [{ userId: 8101, displayName: 'A', seat: 0 }, { userId: 8102, displayName: 'B', seat: 1 }] });
      H.start(r.id);
      const users = [8101, 8102], bases = ['BTC', 'ETH'];
      const current = () => H.comp.currentRound();
      const get = (base, at = now) => H.comp.roundMarkGet(r.id, base, at);
      for (const uid of which === 39 ? [] : users) for (const base of bases) {
        T.applyFill(uid, { symbol: base, orderSide: 'BUY', size: .1, px: get(base).price,
          feeBps: 0, kind: 'MARKET', leverage: 100, marginMode: 'cross',
          executionSource: 'composite-index', referenceMark: get(base).price });
      }
      if (which === 35 || which === 39) {
        const plan = H.comp.planOf(current());
        for (const offset of H.comp.boundariesOf(current())) {
          if (offset <= plan.boostStart) H.CT.q.bMark.run(r.id, offset, 'succeeded', null, now);
        }
        H.CT.db.prepare('UPDATE paper_rounds SET boost_opened=? WHERE id=?').run(JSON.stringify(bases), r.id);
        H.CT.db.prepare(`UPDATE paper_round_players SET boost_bankroll=100000,
          boost_max_exposure=50000000,boost_frozen_at=? WHERE round_id=?`).run(now, r.id);
        H.setActiveOffset(r.id, plan.boostStart + 100);
        if (which === 39) H.CT.q.bMark.run(r.id, plan.boostStart, 'retryable',
          'competition price unavailable at boundary', now);
        assert.strictEqual(H.comp.phaseNow(now).phase, which === 39 ? 'finalBuild' : 'boost');
        if (which === 35) for (const base of bases) T.openAlias(base + '-BOOST', r.id);
      }
      const orders = [];
      for (const uid of which === 39 ? [] : users) {
        const symbol = which === 35 ? 'BTC-BOOST' : 'BTC';
        const epoch = T.stmt.acctGet.get(uid).epoch;
        // Retain the legacy cross portfolio so recovery still proves a
        // coupled two-market risk event. Stage now permits only reductions
        // of those old cross legs; new Boost exposure remains isolated.
        const o = T.stmt.ordInsWithBoost.run(uid, epoch, symbol,
          which === 35 ? 'BUY' : 'SELL', which === 35 ? 101 : 99, .01,
          which === 35 ? 500 : 100, which === 35 ? 0 : 1, now,
          which === 35 ? 'isolated' : 'cross', null, null, 0);
        orders.push(Number(o.lastInsertRowid));
      }
      // Construct ordinary retained prior-boot records through the storage
      // API. No live restart, production DB or provider is involved.
      T.db.transaction(() => {
        for (const base of [...bases, 'ANSEM']) {
          const prior = get(base);
          H.comp.roundMarkCommit(r.id, { ...prior, acceptedBoot: 'fixture-prior-boot' },
            { acceptedBoot: prior.acceptedBoot, acceptedSeq: prior.acceptedSeq });
        }
      })();
      if (which === 36) {
        const prior = get('BTC'), at = now;
        assert.strictEqual(T.__roundExecutionMark('BTC', { round: current() }), null,
          'a durable old boot is never live authority');
        assert.strictEqual(T.markAt('BTC', at, { round: current(), forLeverage: 500 }), prior.price);
        assert.strictEqual(T.__historicalAvailabilityAt('BTC', 500, at, current()).ready, true);
        const evidence = T.__marketEvidenceAt('BTC', at, 'BOOST', 500, current());
        assert.strictEqual(evidence.historicalReady, true);
        assert.strictEqual(evidence.liveReady, false);
        assert.strictEqual(evidence.policy.engineBoot, 'fixture-prior-boot');
        assert.strictEqual(T.__roundAttemptEvidenceValid({ asset: 'BTC', ready: true, evidence }, at, 'BOOST', 500, current()), true);
        const marks = T.markSetFor(users, at, { round: current() });
        const proof = T.scoreProofFor(users[0], T.stmt.acctGet.get(users[0]).epoch, 100000, marks, at, current());
        assert.strictEqual(proof.roundMarkLineage, 'per-record-v1');
        assert.notStrictEqual(proof.engineBoot, prior.acceptedBoot);
        assert.strictEqual(T.__roundScoreEvidenceValid(proof, at, current()), true);
        // The boundary occurred before this callback. Its fill keeps `at`,
        // while postcommit publication correctly checks current availability
        // and may open a current global pause on these retained old-boot rows.
        now += 10;
        const callbackAt = now;
        const historicalClose = T.applyFill(users[0], { symbol: 'BTC', orderSide: 'SELL', size: .1,
          px: prior.price, feeBps: 0, kind: 'SEGMENT', at, referenceMark: prior.price,
          executionSource: 'boundary-mark', decisionReason: 'segment-boundary-flatten' });
        assert.strictEqual(historicalClose.decisionContext.inputs.roundExecution.acceptedBoot, prior.acceptedBoot);
        assert.strictEqual(historicalClose.decisionContext.inputs.roundExecution.price, prior.price);
        assert.strictEqual(historicalClose.ts, at);
        const currentPause = H.CT.q.clockPauses.all(r.id).find(row => row.ended_at == null);
        assert.ok(currentPause, 'postcommit availability must record the current clock pause');
        assert.strictEqual(currentPause.started_at, callbackAt);
        assert.ok(currentPause.started_at > at, 'current outage must not rewrite the earlier boundary');
        const beforeWrongClose = T.stmt.acctGet.get(users[1]);
        assert.throws(() => T.applyFill(users[1], { symbol: 'BTC', orderSide: 'SELL', size: .1,
          px: prior.price + 1, feeBps: 0, kind: 'SEGMENT', at, referenceMark: prior.price,
          decisionReason: 'segment-boundary-flatten' }), /historical segment fill/);
        assert.deepStrictEqual(T.stmt.acctGet.get(users[1]), beforeWrongClose);
        now += 10; T.__pauseAt('BTC', now, 'ordinary historical clock gap');
        assert.strictEqual(T.markAt('BTC', now, { round: current() }), null);
        assert.strictEqual(T.__historicalAvailabilityAt('BTC', 500, now, current()).invalidSince, currentPause.started_at);
        const pausedEvidence = T.__marketEvidenceAt('BTC', now, 'BOOST', 500, current());
        assert.strictEqual(pausedEvidence.historicalReady, false);
        assert.deepStrictEqual(pausedEvidence.clockPause, { from: currentPause.started_at, to: null });
        assert.strictEqual(T.__roundAttemptEvidenceValid({ asset: 'BTC', ready: false, evidence: pausedEvidence }, now, 'BOOST', 500, current()), true);
        assert.strictEqual(T.__roundAttemptEvidenceValid({ asset: 'BTC', ready: false,
          evidence: { ...pausedEvidence, clockPause: null } }, now, 'BOOST', 500, current()), false,
          'a qualified record cannot justify a pause rejection without its captured interval');
        assert.strictEqual(T.markAt('BTC', at, { round: current() }), prior.price,
          'a later pause never rewrites an earlier valid predecessor');
        H.comp.roundMarkInvalidate(r.id, 'ETH', 'ordinary fixture hard gap', now);
        T.__clearPauses(); H.comp.pauseClockClose(now);
        assert.strictEqual(T.markAt('ETH', now, { round: current() }), null);
        assert.strictEqual(T.markAt('ETH', at, { round: current() }), get('ETH', at).price);
      } else {
        const auth = require('./auth-shim.js'), health = auth.authHealth;
        let identityAvailable = false;
        auth.authHealth = () => ({ tradingAvailable: identityAvailable,
          validUntil: identityAvailable ? now + 5000 : null, unavailableSince: now });
        const oldPositions = users.map(uid => T.stmt.posByUser.all(uid));
        const oldAccounts = users.map(uid => T.stmt.acctGet.get(uid));
        const oldMarks = bases.map(base => get(base));
        const optional = get('ANSEM');
        const commit = H.comp.roundMarkCommit, markRun = T.stmt.posMark.run;
        const rehydrate = H.comp.rehydrateGates, bigint = process.hrtime.bigint;
        let offset = 0n, commits = 0, riskMarks = 0, failGateOnce = which === 35 || which === 39;
        try {
          T.__pauseAt('BTC', now, 'ordinary cold recovery fixture');
          for (const base of bases) {
            T.live.map.delete(base);
            // Case37 tests an ordinary aged-source downgrade, not a missing
            // component restart. Other cases retain the cold-recovery setup.
            if (which === 37 && base === 'ETH') {
              const followed = T.comps.get(base).lazer;
              assert.ok(followed, 'keep the actually followed source for an age-only transition');
              // Exclude the harness's other fresh backup components so the
              // ordinary source chain selects the intended venue fallback.
              T.comps.set(base, { lazer: followed });
            } else T.comps.delete(base);
          }
          if (which === 35) for (const base of bases) T.openAliases.delete(base + '-BOOST');
          now += 50;
          observe(now, { price: '10001' }, 'BTC');
          identityAvailable = true;
          const partial = T.__recoverRoundPriceBatch(now);
          assert.strictEqual(partial.committed, false, 'one base cannot recover a two-base roster');
          assert.deepStrictEqual(bases.map(base => get(base)), oldMarks);
          assert.deepStrictEqual(users.map(uid => T.stmt.posByUser.all(uid)), oldPositions);
          identityAvailable = false;
          now += which === 37 ? 700 : 50;
          if (which === 37) {
            observe(now, { price: '10001' }, 'BTC');
            T.compUpdate('ETH', 'venue', 100, now);
            const venue = T.live.map.get('ETH');
            assert.ok(venue && venue.acceptedSeq > 0, 'the fallback must actually be accepted');
            assert.strictEqual(venue.srcKey, 'venue');
            assert.strictEqual(T.compPriceReady('ETH', now), true);
            assert.strictEqual(T.boostLevCap('ETH', 500, now), 100);
            assert.strictEqual(T.readyForLeverage('ETH', 500, now), false);
          }
          else observe(now, { price: '10001' }, 'ETH');
          identityAvailable = true;
          const raw = bases.map(base => ({ ...T.live.map.get(base) }));
          const appliedAt = now;
          const beforePause = T.roundPaused();
          T.stmt.posMark.run = (...args) => {
            riskMarks++;
            if (which === 32 && args[2] === users[1]) throw new Error('ordinary later-seat rollback');
            return markRun.apply(T.stmt.posMark, args);
          };
          process.hrtime.bigint = () => bigint() + offset;
          H.comp.roundMarkCommit = (...args) => {
            if (++commits === 2 && which === 38) throw new Error('ordinary late CAS refusal');
            const out = commit(...args);
            if (commits === 2 && which === 33) offset += 1_000_000_000n;
            if (commits === 2 && which === 34) identityAvailable = false;
            return out;
          };
          H.comp.rehydrateGates = (...args) => {
            if (which === 39) {
              assert.strictEqual(H.comp.phaseNow(now).phase, 'finalBuild');
              assert.strictEqual(T.__competitionAliasOpen('BTC-BOOST'), false,
                'pending opening is a price dependency, never private early trading permission');
            }
            if (failGateOnce) { failGateOnce = false; throw new Error('ordinary postcommit gate retry'); }
            return rehydrate(...args);
          };
          const result = T.__recoverRoundPriceBatch(now);
          if ([32, 33, 34, 37, 38].includes(which)) {
            assert.strictEqual(result.committed, false);
            assert.deepStrictEqual(bases.map(base => get(base)), oldMarks);
            assert.deepStrictEqual(users.map(uid => T.stmt.posByUser.all(uid)), oldPositions);
            assert.deepStrictEqual(users.map(uid => T.stmt.acctGet.get(uid)), oldAccounts);
            for (const id of orders) assert.strictEqual(T.stmt.ordGet.get(id).status, 'OPEN');
            assert.ok(T.roundPaused());
            if (which === 37) {
              assert.ok(T.live.map.get('ETH'));
              assert.strictEqual(T.readyForLeverage('ETH', 500, now), false);
              assert.strictEqual(riskMarks, 0, 'probe before every financial write');
            }
          } else {
            assert.strictEqual(result.committed, true);
            if (which === 39) assert.strictEqual(riskMarks, 0, 'the pending-opening fixture has a flat roster');
            else assert.ok(riskMarks >= 4, 'both positions of both seats were evaluated');
            for (let i = 0; i < bases.length; i++) {
              const row = get(bases[i]);
              assert.strictEqual(row.acceptedBoot, raw[i].acceptedBoot);
              assert.strictEqual(row.acceptedSeq, raw[i].acceptedSeq);
              assert.strictEqual(row.acceptedAt, raw[i].pythAtMs);
              assert.strictEqual(row.observedAt, raw[i].pythSrcAtMs);
              assert.strictEqual(row.appliedAt, appliedAt);
              assert.ok(row.originalValidUntil > appliedAt && row.originalValidUntil <= raw[i].pythAtMs + 600);
              assert.strictEqual(T.live.map.get(bases[i]).acceptedSeq, raw[i].acceptedSeq,
                'adoption never creates a new raw sequence or receipt');
            }
            assert.deepStrictEqual(get('ANSEM'), optional, 'unrelated optional marks do not join the batch deadline');
            for (const id of orders) assert.strictEqual(T.stmt.ordGet.get(id).status, 'FILLED');
            const counts = users.map(uid => T.stmt.acctGet.get(uid).fills_count);
            const marked = riskMarks;
            if (which === 39) {
              assert.strictEqual(result.resumed, false);
              assert.strictEqual(commits, 2, 'both durable opened bases, not one flat fallback, share recovery');
              assert.strictEqual(T.aliasOpen('BTC-BOOST'), false);
              assert.strictEqual(H.comp.levCapFor('BTC-BOOST', 500, users[0], now), 0);
            } else if (which === 35) {
              assert.strictEqual(result.resumed, false);
              assert.strictEqual(T.aliasOpen('BTC-BOOST'), false);
              assert.deepStrictEqual(T.roundPaused(), beforePause,
                'private monitorSegments gate checks never open a fresh pause obligation');
              assert.strictEqual(T.__recoverRoundPriceBatch(now).resumed, true);
              assert.strictEqual(T.aliasOpen('BTC-BOOST'), true);
            } else assert.strictEqual(result.resumed, true);
            if (which !== 39) T.__recoverRoundPriceBatch(now);
            assert.strictEqual(riskMarks, marked, 'postcommit gate retry never replays risk');
            assert.deepStrictEqual(users.map(uid => T.stmt.acctGet.get(uid).fills_count), counts);
          }
        } finally {
          auth.authHealth = health; H.comp.roundMarkCommit = commit;
          T.stmt.posMark.run = markRun; H.comp.rehydrateGates = rehydrate;
          process.hrtime.bigint = bigint;
        }
      }
      H.abortLive();
    } else if (which === 40) {
      for (const sym of [...easedClampAssets, ...baselineClampAssets]) {
        const accepted = easedClampAssets.includes(sym);
        const floor = accepted ? 0.02 : 0.005;
        assert.strictEqual(T.__clampJumpFor(sym), floor);
        assert.strictEqual(T.__clampJumpFor(sym + '-BOOST'), floor,
          'the Boost alias resolves the same base floor');
        assert.strictEqual(T.__clampJumpFor(sym + '-HOT'), floor,
          'historical Hot aliases resolve the same base floor');
        T.compUpdate(sym, 'lazer', 100, now, now);
        const seeded = { ...T.live.map.get(sym) };
        now += 1;
        T.compUpdate(sym, 'lazer', 100.6, now, now);
        assert.strictEqual(T.live.map.get(sym).pythPrice, accepted ? 100.6 : 100);
        assert.strictEqual(T.confirming.has(sym), !accepted);
        if (accepted) {
          assert.ok(T.live.map.get(sym).acceptedSeq > seeded.acceptedSeq,
            'the scoped move completes the ordinary risk and acceptance path');
          assert.strictEqual(T.compPriceReady(sym, now), true);
        } else {
          assert.strictEqual(T.live.map.get(sym).acceptedSeq, seeded.acceptedSeq);
          assert.strictEqual(T.compPriceReady(sym, now), false);
        }
      }
      for (const sym of ['ETH', 'SOL', 'XRP', 'FARTCOIN']) {
        assert.strictEqual(T.__clampJumpFor(sym), 0.02, 'every indexed market, majors included, starts wide');
      }
    } else if (which === 41) {
      for (const sym of easedClampAssets) {
        for (const key of ['lazer', 'usdt', 'usd']) T.compUpdate(sym, key, 100, now, now);
        assert.strictEqual(T.compQuality(sym, now).n, 3);
        const seeded = { ...T.live.map.get(sym) };
        for (let tick = 1; tick <= 9; tick++) {
          now += 1;
          T.compUpdate(sym, 'lazer', 102.5, now, now);
          assert.strictEqual(T.live.map.get(sym).pythPrice, 100,
            'a move above the wide floor still waits for confirmation');
          assert.strictEqual(T.live.map.get(sym).acceptedSeq, seeded.acceptedSeq);
          assert.strictEqual(T.confirming.has(sym), true);
          assert.strictEqual(T.compPriceReady(sym, now), false,
            'fresh backups never bypass the followed-source clamp');
        }
        now += 1;
        T.compUpdate(sym, 'lazer', 102.5, now, now);
        assert.strictEqual(T.live.map.get(sym).pythPrice, 102.5);
        assert.strictEqual(T.confirming.has(sym), false);
        assert.ok(T.live.map.get(sym).acceptedSeq > seeded.acceptedSeq);
      }
      /* A market that keeps moving one way never clusters within 30bps, and
         used to stay held for as long as it ran. A run of the streak length
         in one direction is accepted at its latest print. */
      for (const sym of easedClampAssets) {
        const base = T.live.map.get(sym).pythPrice;
        const before = { ...T.live.map.get(sym) };
        let px = base;
        for (let tick = 1; tick <= 9; tick++) {
          now += 1; px = px * (tick === 1 ? 1.025 : 1.002);  // a 2.5% jump, then +20bps a tick: the cluster breaks by the third print, the walk stays inside the 5% ceiling
          T.compUpdate(sym, 'lazer', px, now, now);
          assert.strictEqual(T.live.map.get(sym).pythPrice, base, 'a running market is still held until the streak');
          assert.strictEqual(T.confirming.has(sym), true);
        }
        now += 1; px = px * 1.002;
        T.compUpdate(sym, 'lazer', px, now, now);
        assert.strictEqual(T.live.map.get(sym).pythPrice, px, 'ten prints moving the same way are accepted at the latest one');
        assert.strictEqual(T.confirming.has(sym), false);
        assert.ok(T.live.map.get(sym).acceptedSeq > before.acceptedSeq);
        /* A scattering feed does not qualify: alternating sides restart the run. */
        const b2 = T.live.map.get(sym).pythPrice; const s2 = { ...T.live.map.get(sym) };
        for (let tick = 1; tick <= 12; tick++) {
          now += 1;
          T.compUpdate(sym, 'lazer', b2 * (tick % 2 ? 1.03 : 0.97), now, now);
          assert.strictEqual(T.live.map.get(sym).pythPrice, b2, 'prints on alternating sides never build a run');
        }
        assert.strictEqual(T.live.map.get(sym).acceptedSeq, s2.acceptedSeq);
        now += 1; T.compUpdate(sym, 'lazer', b2, now, now);   // back inside the gate: hold clears
        assert.strictEqual(T.confirming.has(sym), false);
      }
    } else if (which === 42) {
      for (const sym of [...easedClampAssets, ...baselineClampAssets]) {
        const floor = easedClampAssets.includes(sym) ? 0.02 : 0.005;
        // Learning is clock-driven in memory; no feed or financial write is needed.
        for (let i = 0; i < 127; i++) T.__moveNote(sym, 100.8, 100, now);
        assert.strictEqual(T.__clampJumpFor(sym), floor, '127 samples do not learn a new gate');
        T.__moveNote(sym, 100.8, 100, now);
        const widened = T.__clampJumpFor(sym);
        assert.ok(Math.abs(widened - 0.032) < 1e-12, 'the existing tail multiplier still widens the gate');
        assert.strictEqual(T.__clampJumpFor(sym + '-BOOST'), widened);
        for (let i = 0; i < 512; i++) T.__moveNote(sym, 100, 100, now + 59_999);
        assert.strictEqual(T.__clampJumpFor(sym), widened, 'new samples do not bypass the one-minute interval');
        T.__moveNote(sym, 100, 100, now + 60_000);
        assert.strictEqual(T.__clampJumpFor(sym), floor, 'calm prices tighten to the per-base floor');
        assert.strictEqual(P.readiness().clampBps[sym], Math.round(floor * 1e4));
      }
    } else if (which === 43) {
      for (let i = 0; i < 128; i++) T.__moveNote('VVV', 102, 100, now);
      assert.strictEqual(T.__clampJumpFor('VVV'), 0.05,
        'the adaptive tail cannot raise the gate beyond its existing ceiling');
      for (let i = 0; i < 128; i++) T.__moveNote('SKR', 106, 100, now);
      assert.strictEqual(T.__clampJumpFor('SKR'), 0.02,
        'observations beyond the ceiling never train their own gate');
      assert.strictEqual(P.readiness().clampBps.SKR, undefined,
        'excluded outliers do not satisfy the minimum sample count');
    } else if (which === 44) {
      for (const sym of easedClampAssets) {
        assert.strictEqual(T.__clampJumpFor(sym), 0.006, 'a configured ceiling bounds the cold floor');
        assert.strictEqual(T.__clampJumpFor(sym + '-BOOST'), 0.006);
        T.compUpdate(sym, 'lazer', 100, now, now);
        now += 1;
        T.compUpdate(sym, 'lazer', 100.65, now, now);
        assert.strictEqual(T.live.map.get(sym).pythPrice, 100);
        assert.strictEqual(T.confirming.has(sym), true);
        for (let i = 0; i < 128; i++) T.__moveNote(sym, 100, 100, now);
        assert.strictEqual(T.__clampJumpFor(sym), 0.006, 'the learned floor respects the same ceiling');
      }
      assert.strictEqual(T.__clampJumpFor('ZZUNKNOWN'), 0.005);
    } else if (which === 45) {
      for (const sym of [...easedClampAssets, ...baselineClampAssets]) {
        const eased = easedClampAssets.includes(sym);
        const floor = eased ? 0.02 : 0.005;
        const proposed = eased ? 102.5 : 100.6;
        assert.strictEqual(T.__clampJumpFor(sym), floor,
          'malformed configuration retains a finite positive cold fallback');
        T.compUpdate(sym, 'lazer', 100, now, now);
        const seeded = { ...T.live.map.get(sym) };
        now += 1;
        T.compUpdate(sym, 'lazer', proposed, now, now);
        assert.strictEqual(T.live.map.get(sym).pythPrice, 100);
        assert.strictEqual(T.confirming.has(sym), true);
        for (let i = 0; i < 128; i++) T.__moveNote(sym, 100, 100, now);
        assert.strictEqual(T.__clampJumpFor(sym), floor,
          'an unusable learned gate still falls back to the per-base floor');
        now += 1;
        T.compUpdate(sym, 'lazer', proposed, now, now);
        assert.strictEqual(T.live.map.get(sym).pythPrice, 100);
        assert.strictEqual(T.live.map.get(sym).acceptedSeq, seeded.acceptedSeq);
        assert.strictEqual(T.confirming.has(sym), true);
        assert.strictEqual(T.compPriceReady(sym, now), false);
      }
    }
  } finally {
    P.stopSourceExpiry(); T.db.close();
    Date.now = original.now;
    global.setTimeout = original.setTimeout;
    global.clearTimeout = original.clearTimeout;
  }
  process.exit(0);
}
