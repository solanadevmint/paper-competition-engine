'use strict';

// Small deterministic regression fixtures. This process always owns a new DB
// and cannot open a feed or make an HTTP request, even if an env is inherited.
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const vm = require('vm');
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'paper-review-fixture-'));
process.env.PAPER_DB = path.join(scratch, 'fixture.db');
process.env.PAPER_MAINTENANCE_FILE = path.join(scratch, 'maintenance');
process.env.PAPER_COMP_TOKEN = 'review-fixture-only';
for (const key of ['TG_BOT_TOKEN', 'OPS_ALERT_CHAT_ID', 'WAREHOUSE_API_TOKEN', 'PYTH_LAZER_TOKEN']) delete process.env[key];
for (const transport of [require('http'), require('https')]) {
  transport.request = transport.get = () => { throw new Error('fixture has no network'); };
}
const comp = require('./competition');
const P = require('./paper');
const T = P.__test, CT = comp.__test;
let passed = 0, failed = 0;
const test = (name, run) => {
  try { run(); passed++; console.log('  ok   ' + name); }
  catch (e) { failed++; console.error('  FAIL ' + name + '\n' + (e.stack || e)); }
};
try {
  T.db.prepare('INSERT INTO users(id) VALUES(8101)').run();
  T.__ensureAccountRef()(8101);
  const unwired = comp.createRound({ id: 'unwired-review', candidates: ['BTC', 'SOL', 'ETH'], players: [{ userId: 8101 }] });
  test('a venue listing outside the index is not offered', () => {
    const M = P.__testMarkets;
    const before = new Set(M.DISABLED_MARKETS);
    assert.ok(M.STAGE_INDEXED.has('BTC') && !M.DISABLED_MARKETS.has('BTC'));
    const off = M.restrictToIndexed(['BTC', 'spy', 'PONS', 'BTC-BOOST', '', null, 'SPY', 'GOLD']);
    assert.deepStrictEqual(off, ['SPY', 'PONS'], 'only new, unindexed base listings are switched off');
    assert.ok(M.DISABLED_MARKETS.has('SPY') && M.DISABLED_MARKETS.has('PONS'));
    assert.ok(!M.DISABLED_MARKETS.has('BTC') && !M.DISABLED_MARKETS.has('BTC-BOOST'));
    assert.deepStrictEqual(M.restrictToIndexed(['SPY', 'PONS']), [], 'a second refresh reports nothing new');
    for (const sym of before) assert.ok(M.DISABLED_MARKETS.has(sym), 'the named list is kept: ' + sym);
    assert.ok(M.STAGE_INDEXED.has('BTC') && !M.STAGE_INDEXED.has('SPY'), 'the index itself is untouched');
    // The config the terminal builds its picker from reports it off and does
    // not list it, while an indexed market is still on offer.
    const btc = T.mktCfg.get('BTC') || { tiers: [], maxLev: 10, lotSize: null, takerBps: 3.5, makerBps: 0.5, maintBps: 5000, cancelBps: 7000, maxLiqSize: null, status: 'active', isolatedOnly: false };
    T.mktCfg.set('BTC', btc);
    T.mktCfg.set('SPY', { ...btc });
    let body = null;
    P.engineConfig({}, { setHeader() {}, writeHead() {}, end(b) { body = JSON.parse(String(b)); } });
    assert.ok(body && body.ok === true);
    assert.ok(body.disabled.includes('SPY') && body.disabled.includes('PONS'));
    assert.ok(!('SPY' in body.markets) && ('BTC' in body.markets));
    T.mktCfg.delete('SPY');
  });

  test('missing money and mark hooks cannot publish a zero checkpoint', () => {
    assert.throws(() => comp.snapshot(unwired.id, 'final'), /not wired: markSetFor/);
    comp.wire({ markSetFor: () => ({}) });
    assert.throws(() => comp.snapshot(unwired.id, 'final'), /not wired: scoreUser/);
    assert.strictEqual(CT.q.scores.all(unwired.id, 'final').length, 0);
    assert.throws(() => comp.marketReadiness(unwired.id), /not wired: marketReady/);
  });

  // Reuse the current two-Hot fixture's explicit market/financial hooks. This
  // loads only declarations before its test runner, not another test suite.
  const source = fs.readFileSync(path.join(__dirname, 'test-two-hot.js'), 'utf8');
  const end = source.indexOf('(async () => {');
  assert.ok(end > 0);
  const H = vm.compileFunction(source.slice(0, end)
    + '\nreturn {comp,CT,create,start,abortLive,wire,primeMajors};', ['require'])(require);
  const players = [{ userId: 8101, seat: 0 }, { userId: 8102, seat: 1 }];
  const create = (id, more = {}) => comp.createRound({ id,
    candidates: ['BTC', 'SOL', 'ETH'], players, ...more });

  test('new checkpoints use shared ledger precision without reranking historical rows', () => {
    const r = create('precision-review');
    CT.q.ddUpd.run(10, 1, r.id, 8101);
    CT.q.ddUpd.run(10, 0, r.id, 8102);
    comp.wire({ markSetFor: () => ({}), scoreUser: (uid) => ({
      equity: 10.3, accountPnl: uid === 8101 ? .1 + .2 : .3,
      hotBonus: 0, realized: uid === 8101 ? .1 + .2 : .3,
    }) });
    try {
      comp.snapshot(r.id, 'final');
      const board = comp.standings(r.id, 'final');
      assert.deepStrictEqual(board.map(x => x.score), [.3, .3]);
      assert.strictEqual(board[0].user_id, 8102, 'equal ledger scores use the existing drawdown tie-break');
      const stored = JSON.stringify(CT.q.scores.all(r.id, 'final'));
      comp.snapshot(r.id, 'final');
      assert.strictEqual(JSON.stringify(CT.q.scores.all(r.id, 'final')), stored);
      const at = Date.now();
      CT.q.ins.run('historical-precision-review', 'round', '[]', null, 'historical-fixture', at, at);
      for (const p of players) {
        CT.q.playerIns.run('historical-precision-review', p.userId, null, p.seat, null);
        const score = p.userId === 8101 ? .1 + .2 : .3;
        CT.q.scoreInsStrict.run('historical-precision-review', p.userId, 'final', at,
          10.3, score, 0, 0, score, '{}', at, p.userId === 8101 ? 1 : 0);
      }
      assert.strictEqual(CT.q.get.get('historical-precision-review').score_precision, null);
      assert.strictEqual(comp.standings('historical-precision-review', 'final')[0].user_id, 8101,
        'stored historical scalar order is not rounded on read');
    } finally { H.wire(); }
  });

  comp.wire({ ensureBot: (uid) => {
    T.db.prepare('INSERT OR IGNORE INTO users(id) VALUES(?)').run(uid);
    T.__ensureAccountRef()(uid);
  } });
  test('practice is isolated by default and still has a marked practice wall', () => {
    comp.setWall({ series: 'review-official-show' });
    const r = create('practice-review', { solo: true, seats: 3 });
    assert.strictEqual(r.series, 'practice');
    const board = comp.seriesBoard('practice');
    assert.strictEqual(board.practice, true);
    assert.strictEqual(board.official, false);
    assert.ok(board.stages.some(s => s.players.some(p => p.bot === true)));
    assert.deepStrictEqual(board.placings, []);
    assert.strictEqual(comp.seriesBoard('review-official-show').stages.length, 0);
    assert.throws(() => create('bad-series-review', { solo: true, series: 'review-official-show' }), /reserved practice series/);
  });
  test('reserved bots cannot enter a non-solo roster', () => {
    assert.throws(() => create('bot-official-review', {
      players: [{ userId: comp.botIdForSeat(2), seat: 0 }],
    }), /bot seats require a solo practice round/);
  });
  test('an operational rehearsal never inherits the official series', () => {
    const r = create('ops-review', { kind: 'rehearsal', stage: 'ops-rehearsal' });
    assert.strictEqual(r.series, 'practice');
    assert.strictEqual(comp.seriesBoard('review-official-show').roundIds.includes(r.id), false);
  });
  test('an official final cannot arm with the wrong number of seats', () => {
    assert.throws(() => create('bad-final-review', { kind: 'final',
      series: 'review-official-show', players: players.slice(0, 1) }), /exactly two seats/);
    const r = create('valid-final-review', { kind: 'final', series: 'review-official-show' });
    assert.strictEqual(comp.playersOf(r.id).length, 2);
    const unclaimed = create('unclaimed-final-review', { kind: 'final', series: 'review-official-show',
      players: [players[0], { displayName: 'Reserved finalist', seat: 1 }] });
    assert.throws(() => comp.startRound(unclaimed.id), /exactly two claimed players/);
    assert.strictEqual(comp.playersOf(unclaimed.id).length, 2, 'failed start keeps the reserved seat');
  });

  test('published stop placings follow the stored semi/final tie order and prize table', () => {
    const series = 'review-official-placings';
    const seedResult = (id, stage, kind, rows, solo = 0) => {
      const at = Date.now();
      CT.q.ins.run(id, kind, '[]', null, 'historical-fixture', at, at);
      T.db.prepare("UPDATE paper_rounds SET status='done',series=?,stage=?,solo=?,ends_at=? WHERE id=?")
        .run(series, stage, solo, at, id);
      rows.forEach((row, seat) => {
        T.db.prepare('INSERT OR IGNORE INTO users(id) VALUES(?)').run(row.uid);
        CT.q.playerIns.run(id, row.uid, `Seat ${seat}`, seat, null);
        CT.q.scoreInsStrict.run(id, row.uid, 'final', at,
          100 + row.score, row.score, 0, 0, row.score, '{}', at, row.dd || 0);
      });
    };
    seedResult('placing-semi-review', 'Semi-finals', 'round', [
      { uid: 8201, score: 30 }, { uid: 8202, score: 20 },
      { uid: 8203, score: 10, dd: 2 }, { uid: 8204, score: 10, dd: 1 }]);
    seedResult('placing-final-review', 'The Final', 'final', [
      { uid: 8201, score: 5, dd: 2 }, { uid: 8202, score: 5, dd: 1 }]);
    const rows = comp.stopPlacings(series);
    assert.deepStrictEqual(rows.map(x => x.userId), [8202, 8201, 8204, 8203]);
    assert.deepStrictEqual(rows.map(x => x.prize), [1, 2, 3, 4].map(p => comp.STOP_PRIZE[p]));
    seedResult('placing-practice-review', 'The Final', 'final', [
      { uid: comp.botIdForSeat(1), score: 1e6 }, { uid: 8101, score: 1e5 }], 1);
    assert.deepStrictEqual(comp.stopPlacings(series), rows, 'legacy practice does not replace the official final');
    assert.strictEqual(comp.seriesBoard(series).roundIds.includes('placing-practice-review'), false);
  });

  test('normal pause writes and idempotent repeats retain a durable obligation', () => {
    const r = H.create('pause-write-review', { candidates: ['BTC', 'SOL', 'ETH'] });
    H.start(r.id);
    try {
      T.__pauseFor({ symbol: 'BTC', message: 'fixture unavailable' });
      T.__pauseFor({ symbol: 'BTC', message: 'fixture unavailable' });
      assert.strictEqual(T.__pauseStmts().allOpen.all(r.id).length, 1);
      assert.strictEqual(T.roundPaused().unpersisted, false);
      const phase = comp.phaseNow();
      const compact = T.__safePhaseControl(phase).paused;
      assert.deepStrictEqual(Object.keys(compact).sort(), ['count', 'degraded', 'since', 'why']);
      assert.strictEqual(compact.why, 'competition prices unavailable');
      const full = T.__safePhaseControl(phase, true).paused;
      assert.strictEqual(full.reasonClass, 'exposure');
      assert.strictEqual(full.since, compact.since);
      assert.strictEqual(JSON.stringify(full).includes('BTC'), false);
    } finally { H.abortLive(); }
  });
  test('an unsuccessful pause-store acknowledgment uses the existing emergency latch', () => {
    const r = H.create('pause-ack-review', { candidates: ['BTC', 'SOL', 'ETH'] });
    H.start(r.id);
    const statement = T.__pauseStmts().open, original = statement.run;
    statement.run = () => ({ changes: 0 });
    try {
      T.__pauseFor({ symbol: 'BTC', message: 'fixture storage unavailable' });
      assert.strictEqual(T.roundPaused().unpersisted, true);
      assert.ok(comp.currentRound().paused_since);
      assert.match(comp.currentRound().blocked_reason, /pause.*could not be persisted/);
    } finally { statement.run = original; H.abortLive(); }
  });
  test('the committed recovery observation evaluates an existing protective stop', () => {
    const r = H.create('recovery-stop-review', { candidates: ['BTC', 'SOL', 'ETH'] });
    H.start(r.id);
    try {
      T.applyFill(8101, { symbol: 'BTC', orderSide: 'BUY', size: .1, px: 100,
        feeBps: 0, kind: 'MARKET', leverage: 10, marginMode: 'isolated',
        executionSource: 'fixture', referenceMark: 100 });
      T.db.prepare('UPDATE paper_positions SET sl_price=? WHERE user_id=? AND symbol=?').run(99.99, 8101, 'BTC');
      T.__pauseFor({ symbol: 'BTC', message: 'fixture recovery' });
      const before = T.live.map.get('BTC').acceptedSeq;
      const at = Date.now();
      T.compUpdate('BTC', 'usdt', 99.98, at, at);
      assert.ok(T.live.map.get('BTC').acceptedSeq > before);
      assert.strictEqual(T.stmt.posGet.get(8101, 'BTC'), undefined);
      assert.strictEqual(T.db.prepare("SELECT COUNT(*) n FROM paper_fills WHERE user_id=8101 AND kind='SL'").get().n, 1);
      assert.strictEqual(T.roundPaused(), null);
    } finally { H.abortLive(); }
  });
  test('all closed pause classes redact source symbols and internal details', () => {
    for (const [reasonClass, symbols, why] of [
      ['identity', ['__COMPETITION_FEED__'], 'contestant identity service unavailable'],
      ['restart', ['__ENGINE_RESTART__'], 'private restart detail'],
      ['boundary', ['__BOUNDARY_PRICE__'], 'private boundary detail'],
      ['risk', ['BTC'], 'private risk detail'],
      ['exposure', ['BTC'], 'private price detail'],
      ['feed', ['__COMPETITION_FEED__'], 'private feed detail'],
    ]) {
      const result = T.__publicPause({ since: 123, count: 1, symbols, why }, { format_version: 2 });
      assert.strictEqual(result.reasonClass, reasonClass);
      assert.strictEqual(result.since, 123);
      assert.strictEqual(/BTC|private|__/.test(JSON.stringify(result)), false);
    }
  });
  const boostFacts = (id) => ({
    round: CT.q.get.get(id), players: CT.q.players.all(id),
    freezes: CT.q.boostProofs.all(id), resolution: CT.q.boostResolution.get(id),
    scores: CT.q.scoreProofs.all(id),
    accounts: players.map(p => T.stmt.acctGet.get(p.userId)),
  });
  const seedLegacyFreeze = (id, seatCount = 2, proofCount = seatCount) => {
    const at = Date.now() - 10;
    players.forEach((p, i) => {
      if (i < seatCount) CT.q.boostFreeze.run(120, 60000, at, id, p.userId);
      if (i < proofCount) CT.q.boostProofIns.run(id, p.userId, at, 120, 60000, '{}');
    });
    return at;
  };
  test('legacy partial Boost states fail before writes, gate changes or new equity reads', () => {
    const cases = [
      { name: 'freeze-proof-no-open', seats: 2, proofs: 2 },
      { name: 'freeze-resolution-no-open', seats: 2, proofs: 2, resolution: true },
      { name: 'freeze-no-proof-no-open', seats: 2, proofs: 0 },
      { name: 'orphan-proof-no-open', seats: 0, proofs: 1 },
      { name: 'partial-seats-open', seats: 1, proofs: 1, opened: ['BTC', 'SOL'] },
      { name: 'missing-bankroll-open', seats: 2, proofs: 2, opened: ['BTC', 'SOL'], missingBankroll: true },
      { name: 'open-without-freeze', seats: 0, proofs: 0, opened: ['BTC', 'SOL'] },
    ];
    for (const c of cases) {
      const r = create('legacy-boost-' + c.name);
      H.start(r.id);
      try {
        const at = seedLegacyFreeze(r.id, c.seats, c.proofs);
        if (c.opened) CT.q.setBoostOpened.run(JSON.stringify(c.opened), Date.now(), r.id);
        if (c.missingBankroll) CT.db.prepare(`UPDATE paper_round_players
          SET boost_bankroll = NULL WHERE round_id = ? AND user_id = ?`).run(r.id, players[0].userId);
        if (c.resolution) CT.q.boostResolutionIns.run(r.id, at, JSON.stringify({
          version: 1, activationAt: at, configured: ['BTC', 'ETH', 'XRP', 'SOL'],
          attempts: [], selected: ['BTC', 'SOL'], excluded: ['ETH', 'XRP'],
        }), at);
        const before = boostFacts(r.id);
        let hookCalls = 0;
        const unexpected = () => { hookCalls++; throw new Error('partial opening must not reach hooks'); };
        comp.wire({ marketEvidenceAt: unexpected, markSetFor: unexpected,
          scoreUser: unexpected, openAlias: unexpected });
        for (let retry = 0; retry < 2; retry++) {
          assert.throws(() => CT.openBoost(r.id, Date.now() + 1000), e =>
            e.code === 'boost_recovery_inconsistent'
              && /existing bankroll and proofs preserved; review required/.test(e.message));
          assert.deepStrictEqual(boostFacts(r.id), before, c.name + ': no durable fact may change');
        }
        assert.strictEqual(hookCalls, 0, 'diagnosis precedes price selection, gates and equity');
        assert.strictEqual(['BTC', 'ETH', 'SOL', 'XRP'].some(s => T.aliasOpen(s + '-BOOST')), false);
      } finally { H.wire(); H.abortLive(); }
    }
  });
  test('unreadable legacy Boost opening metadata is diagnosed without rewriting it', () => {
    for (const raw of ['{', '{}', 'null']) {
      const r = create('legacy-boost-unreadable-' + raw.length + '-' + raw.charCodeAt(0));
      H.start(r.id);
      try {
        CT.q.setBoostOpened.run(raw, Date.now(), r.id);
        const before = boostFacts(r.id);
        assert.throws(() => CT.openBoost(r.id), e => e.code === 'boost_recovery_inconsistent');
        assert.deepStrictEqual(boostFacts(r.id), before);
      } finally { H.abortLive(); }
    }
  });
  test('an inconsistent legacy Boost boundary records a block instead of retrying a new bankroll', () => {
    const r = create('legacy-boost-boundary-review');
    H.start(r.id);
    try {
      seedLegacyFreeze(r.id);
      const p = comp.planOf(CT.q.get.get(r.id));
      const now = Date.now();
      CT.db.prepare('UPDATE paper_rounds SET started_at = ?, ends_at = ? WHERE id = ?')
        .run(now - p.boostStart, now - p.boostStart + p.total, r.id);
      for (const at of comp.boundariesOf(CT.q.get.get(r.id))) {
        if (at < p.boostStart) CT.q.bMark.run(r.id, at, 'succeeded', null, now);
      }
      const before = boostFacts(r.id);
      CT.fireBoundary(r.id, p.boostStart, now);
      const boundary = CT.q.bGet.get(r.id, p.boostStart);
      assert.strictEqual(boundary.status, 'failed');
      assert.match(boundary.error, /Boost recovery inconsistent: freeze exists without a durable opened set/);
      assert.match(CT.q.get.get(r.id).blocked_reason, /Boost recovery inconsistent/);
      assert.strictEqual(comp.writeBarrier(players[0].userId, now), 'round_blocked');
      const after = boostFacts(r.id);
      assert.deepStrictEqual(after.players, before.players);
      assert.deepStrictEqual(after.freezes, before.freezes);
      assert.deepStrictEqual(after.scores, before.scores);
      assert.deepStrictEqual(after.accounts, before.accounts);
      assert.strictEqual(after.resolution, undefined);
      assert.strictEqual(after.round.boost_opened, before.round.boost_opened);
      assert.strictEqual(['BTC', 'ETH', 'SOL', 'XRP'].some(s => T.aliasOpen(s + '-BOOST')), false);
    } finally { H.abortLive(); }
  });
  test('a fully committed Boost opening still reuses its exact frozen bankroll and proofs', () => {
    const r = create('committed-boost-review');
    H.start(r.id);
    try {
      CT.openBoost(r.id, Date.now());
      const original = boostFacts(r.id);
      const opened = JSON.parse(original.round.boost_opened);
      assert.strictEqual(original.freezes.length, players.length);
      assert.strictEqual(original.scores.filter(x => x.checkpoint === 'boostStart').length, players.length);
      for (const base of opened) T.closeAlias(base + '-BOOST', { flatten: false, roundId: r.id });
      CT.db.prepare('UPDATE paper_accounts SET balance = balance + 123 WHERE user_id = ?')
        .run(players[0].userId);
      const before = boostFacts(r.id);
      comp.wire({ markSetFor: () => { throw new Error('committed freeze must not read new equity'); },
        scoreUser: () => { throw new Error('committed freeze must not rescore'); } });
      assert.deepStrictEqual(CT.openBoost(r.id, Date.now() + 1000), opened);
      assert.deepStrictEqual(boostFacts(r.id), before, 'reopening does not move boundary, bankroll or proofs');
      assert.ok(opened.every(base => T.aliasOpen(base + '-BOOST')));
    } finally { H.wire(); H.abortLive(); }
  });
  test('aggregate fixture runner continues independent suites but retains failure exit', () => {
    const called = [], exits = [];
    const runner = fs.readFileSync(path.join(__dirname, 'test-all.js'), 'utf8');
    const stubs = {
      fs: { mkdtempSync: () => '/fixture-only', rmSync: () => {} },
      os: { tmpdir: () => '/fixture-only' }, path,
      child_process: { spawnSync: (_exe, [file]) => { called.push(file); return { status: called.length === 1 ? 1 : 0 }; } },
    };
    vm.runInNewContext(runner, { require: (id) => {
      assert.ok(Object.hasOwn(stubs, id)); return stubs[id];
    }, __dirname: '/fixture-only', console: { log() {}, error() {} },
    process: { env: {}, execPath: 'fixture-node', stdout: { write() {} }, exit: (n) => { exits.push(n); } } });
    assert.ok(called.length >= 16);
    assert.strictEqual(called.at(-1), 'test-rehearsal-runbook.js');
    assert.deepStrictEqual(exits, [1]);
  });
  test('retired tools stop before their archived imports or side effects', () => {
    for (const file of ['rehearse.js', 'test-engine-live.js', 'migrate-paper-tables.js']) {
      const body = fs.readFileSync(path.join(__dirname, file), 'utf8');
      const firstCode = body.replace(/^#![^\n]*\n/, '').replace(/^'use strict';\s*/, '');
      assert.ok(firstCode.startsWith("throw new Error('Retired legacy"), file);
      assert.ok(firstCode.indexOf('throw new Error') < firstCode.indexOf('require('), file);
    }
  });
} finally {
  for (const timers of CT._timers.values()) for (const timer of timers) clearTimeout(timer);
  P.stopSourceExpiry();
  T.db.close();
  fs.rmSync(scratch, { recursive: true, force: true });
}
console.log(`\n${passed}/${passed + failed} engine-review checks passed`);
process.exitCode = failed ? 1 : 0;
