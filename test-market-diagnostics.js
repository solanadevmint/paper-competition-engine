'use strict';
const assert = require('node:assert');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const cases = [
  'canonical readiness reasons preserve boolean verdicts',
  'sub-sweep expiry gap remains visible after recovery',
  'base-ready fallback is separately unavailable for Boost',
  'duplicate and stale ingress have bounded diagnostic counters',
  'risk failure is observed only after the candidate rolls back',
  'source persistence failure has an exact diagnostic reason',
  'history and symbols are bounded and returned data is copy-safe',
  'historical observations cannot rewrite current diagnostic history',
  'diagnostic reads do not select sources or mutate financial state',
  'operator diagnostics cover public markets without revealing a sealed draw',
];
if (!process.env.PAPER_DIAGNOSTIC_CASE) {
  const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'paper-diagnostic-test-'));
  let passed = 0;
  try {
    for (let i = 0; i < cases.length; i++) {
      const run = spawnSync(process.execPath, [__filename], { encoding: 'utf8', timeout: 15000,
        env: { ...process.env, PAPER_DIAGNOSTIC_CASE: String(i + 1),
          PAPER_DB: path.join(scratch, 'case-' + i + '.db'), PAPER_COMP_TOKEN: 'diagnostic-test-only' } });
      if (run.status || run.error) {
        process.exitCode = 1;
        console.error('  FAIL ' + cases[i] + '\n' + (run.stdout || '') + (run.stderr || ''));
      } else { passed++; console.log('  ok   ' + cases[i]); }
    }
  } finally { fs.rmSync(scratch, { recursive: true, force: true }); }
  console.log('\n' + passed + ' diagnostic assertions passed');
} else {
  process.env.PAPER_MAINTENANCE_FILE = process.env.PAPER_DB + '.maintenance';
  process.env.PHOENIX_SNAPSHOT_FILE = process.env.PAPER_DB + '.snapshot';
  delete process.env.TG_BOT_TOKEN;
  delete process.env.OPS_ALERT_CHAT_ID;
  for (const transport of [require('node:http'), require('node:https')]) {
    transport.get = transport.request = () => { throw new Error('isolated diagnostic tests refuse network'); };
  }
  const P = require('./paper.js'), T = P.__test;
  const originalNow = Date.now;
  const base = Date.now();
  let now = base;
  Date.now = () => now;
  const which = Number(process.env.PAPER_DIAGNOSTIC_CASE);
  function seed(sym = 'BTC', px = 100) { T.compUpdate(sym, 'lazer', px, now, now); }
  function verdict(sym = 'BTC') {
    const detail = {};
    const ready = T.compPriceReady(sym, now, detail);
    assert.strictEqual(ready, T.compPriceReady(sym, now), 'detail must not alter the trading verdict');
    return { ready, boost: T.readyForLeverage(sym, 500, now), detail };
  }
  function snapshot(sym = 'BTC') {
    const v = verdict(sym);
    return T.__marketDiagnosticSnapshot(sym, now, v.ready, v.boost, v.detail);
  }
  function reason(expected) { assert.strictEqual(verdict().detail.reason, expected); }
  function endpoint() {
    let code, body;
    P.compReadiness({ headers: { 'x-comp-token': process.env.PAPER_COMP_TOKEN } }, {
      writeHead(c) { code = c; }, end(text) { body = JSON.parse(text); },
    });
    assert.strictEqual(code, 200); return body;
  }
  try {
    if (which === 1) {
      reason('no_observation'); seed(); reason('ready');
      const m = T.live.map.get('BTC');
      m.indexHalt = true; reason('index_halted'); m.indexHalt = false;
      T.confirming.set('BTC', { since: now }); reason('jump_confirmation'); T.confirming.delete('BTC');
      m.pythPrice = 0; reason('invalid_price'); m.pythPrice = 100;
      now = base + 601; reason('source_unavailable');
      const c = T.comps.get('BTC').lazer;
      c.ts = now; c.srcAt = now; c.age0 = 0; c.recvMono = performance.now();
      reason('source_expired');
      m.pythAtMs = now;
      T.comps.get('BTC').venue = { px: 100, ts: now, srcAt: null, age0: 0, ageKnown: false, recvMono: performance.now(), freshSince: now };
      c.ts = base - 60000; c.srcAt = base - 60000; c.age0 = 60000;
      reason('source_transition_pending');
      now = base + 11000; reason('receipt_expired');
      const d = snapshot();
      assert.ok(d.observation.expiryMarginMs < 0);
      assert.ok(Number.isFinite(d.observation.validUntil));
    } else if (which === 2) {
      seed();
      now = base + 800; seed();
      const d = snapshot();
      assert.strictEqual(d.base.ready, true);
      assert.strictEqual(d.history.base.unavailableIntervals, 1);
      assert.ok(d.history.base.unavailableMs >= 199 && d.history.base.unavailableMs < 220);
      assert.ok(d.history.recent.some(x => !x.baseReady && x.baseSince < x.at));
      assert.strictEqual(d.history.counters.accepted, 2);
      assert.ok(d.observation.expiryMarginMs > 570 && d.observation.expiryMarginMs <= 600);
    } else if (which === 3) {
      seed('ANSEM', .235);
      now = base + 1300;
      T.compUpdate('ANSEM', 'venue', .235, now);
      const d = snapshot('ANSEM');
      assert.strictEqual(d.base.ready, true);
      assert.strictEqual(d.boost.ready, false);
      assert.strictEqual(d.boost.reason, 'unageable_source');
      assert.strictEqual(d.observation.ageKnown, false);
      now += 100;
      const later = snapshot('ANSEM');
      assert.ok(later.history.boost.unavailableMs >= 100);
      assert.ok(later.history.boost.unavailableMs > later.history.base.unavailableMs);
    } else if (which === 4) {
      seed(); T.compUpdate('BTC', 'lazer', 100, now, now);
      now += 800; T.compUpdate('BTC', 'lazer', 100, now, base);
      const d = snapshot();
      assert.strictEqual(d.history.counters.ingressRejected, 2);
      assert.strictEqual(d.history.counters.staleIngress, 1);
      assert.strictEqual(d.history.counters.accepted, 1);
    } else if (which === 5) {
      seed(); const seq = T.live.map.get('BTC').acceptedSeq;
      now += 10;
      const query = T.stmt.posBySymbolTree.all;
      T.stmt.posBySymbolTree.all = () => { throw new Error('fixture-only risk read failure'); };
      try { seed(); } finally { T.stmt.posBySymbolTree.all = query; }
      const d = snapshot();
      assert.strictEqual(d.base.reason, 'risk_failed');
      assert.strictEqual(d.base.ready, false);
      assert.strictEqual(T.live.map.get('BTC').acceptedSeq, seq);
      assert.strictEqual(d.history.counters.accepted, 1);
      assert.ok(d.history.counters.lossNotifications > 0);
    } else if (which === 6) {
      seed();
      T.db.exec("CREATE TEMP TRIGGER diagnostic_source_failure BEFORE UPDATE ON paper_index_source WHEN NEW.source='venue' BEGIN SELECT RAISE(ABORT,'fixture-only source failure'); END");
      now += 800; T.compUpdate('BTC', 'venue', 100, now);
      assert.strictEqual(snapshot().base.reason, 'source_persistence_failed');
    } else if (which === 7) {
      seed();
      for (let i = 1; i <= 300; i++) {
        now = base + i;
        T.__marketDiagnosticObserve('BTC', now, i % 2 === 0, i % 2 === 0,
          { reason: i % 2 ? 'risk_failed' : 'ready' });
        T.__marketDiagnosticObserve('UNKNOWN_' + i, now, false, false, { reason: 'no_observation' });
        T.__marketDiagnosticObserve('BTC-BOOST', now, false, false, { reason: 'no_observation' });
      }
      let d = snapshot();
      assert.strictEqual(T.__marketDiagnosticStats().markets, 1);
      assert.ok(T.__marketDiagnosticStats().retainedTransitions <= 128);
      assert.ok(d.history.droppedTransitions > 170);
      assert.ok(d.history.recent.length <= 8);
      d.history.counters.accepted = 999;
      d.history.recent[0].baseReason = 'caller mutation';
      d.history.base.unavailableMs = -1;
      d = snapshot();
      assert.strictEqual(d.history.counters.accepted, 1);
      assert.ok(!JSON.stringify(d).includes('caller mutation'));
      assert.ok(d.history.base.unavailableMs >= 0);
      now = base + 10 * 60_000 + 501;
      T.__marketDiagnosticObserve('BTC', now, true, true, { reason: 'ready' });
      assert.strictEqual(T.__marketDiagnosticStats().retainedTransitions, 1);
    } else if (which === 8) {
      seed(); const before = snapshot();
      T.__marketDiagnosticObserve('BTC', base - 100, false, false, { reason: 'risk_failed' });
      const after = snapshot();
      assert.strictEqual(after.history.base.ready, true);
      assert.strictEqual(after.history.observedSince, before.history.observedSince);
      assert.strictEqual(after.history.counters.ignoredPastObservations, 1);
      assert.strictEqual(after.history.base.unavailableMs, 0);
    } else if (which === 9) {
      seed();
      const source = T.db.prepare('SELECT * FROM paper_index_source').all();
      const before = JSON.stringify({ live: [...T.live.map], components: [...T.comps] });
      const changes = T.db.prepare('SELECT total_changes() n').get().n;
      for (let i = 0; i < 100; i++) T.__marketDiagnosticSnapshot('BTC', now, true, true, { reason: 'ready', source: 'lazer' });
      assert.strictEqual(T.db.prepare('SELECT total_changes() n').get().n, changes);
      assert.deepStrictEqual(T.db.prepare('SELECT * FROM paper_index_source').all(), source);
      assert.strictEqual(JSON.stringify({ live: [...T.live.map], components: [...T.comps] }), before);
      assert.strictEqual(T.__sourceExpiryTimerCount(), 0, 'diagnostics must not create timers');
      assert.strictEqual(snapshot().history.counters.accepted, 1);
    } else if (which === 10) {
      const vm = require('node:vm');
      const source = fs.readFileSync(path.join(__dirname, 'test-two-hot.js'), 'utf8');
      const end = source.indexOf('(async () => {');
      assert.ok(end > 0);
      const H = vm.compileFunction(source.slice(0, end) + '\nreturn {comp,CT,create};', ['require'])(require);
      const round = H.create('diagnostic-sealed-round', { candidates: ['SECRET_A', 'SECRET_B', 'SECRET_C'] });
      const secret = H.CT.q.get.get(round.id);
      const result = endpoint();
      assert.strictEqual(result.markets.length, 36);
      assert.ok(result.markets.every(m => m.diagnostics && m.diagnostics.base.ready === m.ready
        && m.diagnostics.boost.ready === m.readyForBoost));
      assert.strictEqual(T.__marketDiagnosticStats().markets, 36);
      const serialized = JSON.stringify(result);
      for (const forbidden of [round.id, 'SECRET_A', 'SECRET_B', 'SECRET_C', secret.hot_draw_secret, secret.hot_draw_json]) {
        if (forbidden) assert.ok(!serialized.includes(forbidden), 'no sealed material in all-public-universe diagnostics');
      }
    }
  } finally {
    Date.now = originalNow;
    P.stopSourceExpiry();
    require('./auth-shim.js').db.close();
  }
  process.exit(0);
}
