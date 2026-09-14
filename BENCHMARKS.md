# Isolated resting-limit tick benchmark

Run from `paper-engine/` with the engine's dependencies installed. On the
shared development host, prefix `node` with
`NODE_PATH` pointing at an installed `node_modules` if necessary.

This creates its own temporary SQLite database and reuses only the deterministic
fixture setup from `test-two-hot.js`. It does not start feeds, authenticate
against a service, use production state, or place any real order. Network calls
are explicitly disabled. Do not adapt it to use a live database.

It measures 50 synchronous `tickEval` passes with eight flat scored seats,
then 50 with twenty noncrossing Stage BUY limits per seat on BTC. It measures
the risk path, not upstream ingress, transport latency, or simultaneous fills.
There is deliberately no timing assertion: host load and hardware vary. The
deterministic suite separately asserts that this resting book skips per-order
barriers while retaining all orders, and covers crossing/failure rollback.

```sh
node - <<'NODE'
const fs = require('fs'), path = require('path'), vm = require('vm');
for (const transport of [require('http'), require('https')]) {
  transport.request = transport.get = () => { throw new Error('benchmark network access refused'); };
}
const scratch = fs.mkdtempSync('/tmp/paper-tick-bench-');
process.env.PAPER_DB = path.join(scratch, 'bench.db');
process.env.PAPER_COMP_TOKEN = 'benchmark-only';
const source = fs.readFileSync('test-two-hot.js', 'utf8');
const boundary = source.indexOf('(async () => {');
if (boundary < 0) throw new Error('fixture setup boundary changed; review benchmark first');
try {
  vm.runInThisContext(source.slice(0, boundary) + `
    const players = Array.from({length:8}, (_,i) => ({
      userId:8250+i, displayName:'Fixture '+i, seat:i
    }));
    for (const p of players) { userIns.run(p.userId); T.__ensureAccountRef()(p.userId); }
    const r = create('bounded-tick-benchmark', {players, candidates:['BTC','ETH','SOL']});
    start(r.id);
    const sample = () => {
      const times = [];
      for (let i=0; i<50; i++) {
        const began = performance.now();
        const result = T.tickEval('BTC', {force:true});
        if (!result.ok) throw result.error || new Error('tick refused');
        times.push(performance.now()-began);
      }
      times.sort((a,b) => a-b);
      return {samples:times.length, p50Ms:+times[25].toFixed(3),
        p95Ms:+times[47].toFixed(3), maxMs:+times[49].toFixed(3)};
    };
    const empty = sample();
    for (const p of players) {
      const a = T.stmt.acctGet.get(p.userId);
      for (let i=0; i<20; i++) T.stmt.ordInsWithBoost.run(p.userId, a.epoch,
        'BTC', 'BUY', 90, 0.001, 2, 0, Date.now(), 'cross', null, null, 0);
    }
    primeMajors();
    const with160Limits = sample();
    assert.strictEqual(T.stmt.ordOpenBySymbol.all('BTC').length, 160);
    console.log(JSON.stringify({empty, with160Limits}, null, 2));
    abortLive();
    T.__armCompetitionClockExpiry();
  `, {filename:'isolated-tick-benchmark.js'});
} finally {
  require('./auth-shim.js').db.close();
  fs.rmSync(scratch, {recursive:true, force:true});
}
process.exit(0);
NODE
```

Observed on the development host, 2026-09-05, Node 20.20.2:

| Fixture | p50 | p95 | Maximum |
| --- | ---: | ---: | ---: |
| 160 limits, before candidate prefilter | 105.595 ms | 300.771 ms | 375.283 ms |
| 160 limits, after candidate prefilter | 1.337 ms | 1.902 ms | 2.480 ms |
| No resting orders, after change | 0.210 ms | 0.391 ms | 0.750 ms |

These are isolated measurements, not a production capacity guarantee. A book
of orders that actually crosses still performs full transactional execution;
that materially different workload is not measured here.

## Maintenance-sentinel probe

The live CPU sample on 2026-09-05 attributed 3.22 seconds out of a 65-second
capture to `statSync`, predominantly the maintenance probe inside `tickEval`.
The check runs once at each risk-evaluation entry, including each accepted
index candidate and venue-stat risk callback; it also guards sweeps, operator
mutations and WebSocket upgrades. Its missing-file path used to construct and
catch an `ENOENT` exception every time.

This read-only microbenchmark compares the two forms on an absent temporary
pathname. It neither creates a file nor reads a production sentinel:

```sh
node - <<'NODE'
const fs = require('fs'), {performance} = require('perf_hooks');
const target = '/tmp/paper-maintenance-absent-benchmark-' + process.pid;
if (fs.existsSync(target)) throw new Error('benchmark path unexpectedly exists');
for (const [label, probe] of [
  ['throwing', () => {
    try { fs.statSync(target); return true; }
    catch (e) { return e.code !== 'ENOENT'; }
  }],
  ['nonthrowing', () => {
    try { return fs.statSync(target, {throwIfNoEntry:false}) !== undefined; }
    catch (e) { return e.code !== 'ENOENT'; }
  }],
]) {
  let active = 0;
  const began = performance.now();
  for (let i=0; i<20000; i++) active += probe() ? 1 : 0;
  console.log(JSON.stringify({label, calls:20000,
    ms:+(performance.now()-began).toFixed(2), active}));
}
NODE
```

Observed on the same development host: 218.63 ms before versus 31.84 ms after
for 20,000 calls; both returned inactive every time. The optimized production
check still reaches the filesystem on every invocation. It adds no cache or
TTL, and errors other than `ENOENT` remain fail-closed.

## Loaded active-round ingress and publication

`bench-active-round.js` is a bounded diagnostic, not a runtime module or a
timing-based CI test. It creates and removes its own disposable database,
refuses HTTP/HTTPS access, opens no listener, and optionally uses an in-process
CPU sampler without an inspector port. It reuses the deterministic round
fixture but wires the **real** clock-health, account-risk, drawdown and
publication paths. No production database, credential or running round is used.

```sh
# 36 markets, 4 seats: three BTC positions and one SOL position.
# Eight fast markets at 20 Hz; 28 other markets at 5 Hz; 3 fake WS + 1 fake SSE.
BENCH_STREAM=1 BENCH_SECONDS=12 node bench-active-round.js

# The same stream plus the observed stuck-client baseline request rate.
BENCH_STREAM=1 BENCH_BASELINE_HZ=13 BENCH_SECONDS=12 node bench-active-round.js

# Real committed Hot/Boost boundaries, with the same recovery-read pressure.
BENCH_STREAM=1 BENCH_PHASE=hot BENCH_BASELINE_HZ=13 node bench-active-round.js
BENCH_STREAM=1 BENCH_PHASE=boost BENCH_BASELINE_HZ=13 node bench-active-round.js

# Broader portfolio pressure: 8 seats, each holding BTC/ETH/SOL/XRP.
BENCH_STREAM=1 BENCH_SEATS=8 BENCH_LEGS=4 node bench-active-round.js

# Reproduce the previous wide SQL query shapes in the disposable fixture only.
BENCH_STREAM=1 BENCH_LEGACY_QUERIES=1 node bench-active-round.js

# Synchronous loaded-risk timing, statement counts and optional local CPU sample.
BENCH_PROFILE=1 BENCH_FRAMES=80 node bench-active-round.js
```

Use the same `NODE_PATH` prefix described above on the shared development host.
Stream mode runs for 10–30 seconds and caps baseline reads at 20/s. Scheduled
provider timestamps are retained: event-loop backlog consumes the unchanged
600/1200 ms source budgets. Increasing, tiny price excursions exercise live
drawdown writes without closing positions. The script reports offered versus
accepted observations, complete/incomplete boards, loop delay, scheduling lag,
CPU, batch work and baseline response failures. A reported pause or rejected
observation is not suppressed or treated as a successful sample.

Observed 2026-09-05 during the loaded-round follow-up:

- Replacing repeated wide checkpoint/roster/portfolio reads with fresh count
  and joined-dependency queries reduced the 4-seat profiled held-tick p50 from
  6.45 to 2.31 ms and p95 from 28.39 to 5.26 ms. Roster reads per BTC tick fell
  from 24 to 2; full per-seat portfolio reads fell from 39 to 11. Every held
  tick still runs risk and both drawdown samples.
- A 12-second stream with 4 seats accepted all 3,600 observations and published
  236 complete boards, with no paused sample. CPU averaged 30.77%, and loop
  delay reached 57.15 ms (p99 30.33 ms). Reproducing the prior wide query shape
  used 42.79% CPU and reached 92.41 ms loop delay on the same workload. These
  first comparison runs used fixed-amplitude tiny price changes.
- With progressively wider tiny excursions and 13 baseline requests/s, all
  3,600 observations were accepted, all 151 baseline calls succeeded, and no
  board was incomplete or sample paused. CPU averaged 33.83%; maximum loop
  delay was 85.85 ms; the slowest baseline response took 20.43 ms. This final
  query set also narrows the post-clock write-barrier read to status/block
  fields; same-transaction changes remain immediately visible.
- The 8-seat, 32-position run accepted all 3,600 observations with no pause or
  incomplete board, but averaged 79.60% CPU and reached 308 ms scheduling lag.
  That is limited spare capacity, **not** a full-capacity certification.
- Hot mode committed the warning/open boundaries using synthetic historical
  marks and held the actually revealed asset in one seat. It accepted all
  3,600 observations and served 152 baselines, with zero paused samples or
  incomplete boards: 36.85% average CPU, 58.59 ms maximum loop delay.
- Boost mode committed both Hot windows and Boost opening through the real
  boundary handlers, froze each $10 bankroll/$5,000 capacity, then opened four
  500x alias positions. It accepted all 3,600 observations and served 137
  baselines, with zero paused samples or incomplete boards: 57.47% average
  CPU, 101.25 ms maximum loop delay. The nominal 13/s baseline timer drifts
  under load; the actual count is reported instead of claiming a fixed rate.

These are short development-host fixtures. They do not reproduce real
provider/network jitter, JSON envelope parsing and redundant endpoint traffic,
historical production database size, host contention, simultaneous liquidation
or order fills, real-time Hot/Boost boundary transitions, settlement, or a
full-length rehearsal. Phase-mode boundaries are committed before timing starts.
They establish the measured hot-path savings, not a guarantee that a live
competition can never pause. The deterministic suite separately checks exact
boundary ownership, freshness, scoring, rollback and settlement semantics.

The source-expiry follow-up on 2026-09-05 reran 12-second four-seat streams
with all 36 watchdog timers active and `BENCH_BASELINE_HZ=13`. Build, Hot and
Boost each accepted all 3,600 offered observations, with no paused sample,
incomplete board or failed baseline. Actual baseline counts were 154, 151 and
148; CPU averaged 32.76%, 37.59% and 48.31%; maximum event-loop delay was
51.25, 79.43 and 59.47 ms respectively. Maximum scheduling lag was 3, 28 and
17 ms.
This rerun exercises the new timer overhead but retains all limitations above.
