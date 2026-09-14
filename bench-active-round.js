'use strict';

// Isolated diagnostic only: a fresh database, no sockets or live state. This
// deliberately uses real clock-health and portfolio-risk hooks, unlike the
// draw-selection stubs in the deterministic fixture's ordinary tests.
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const inspector = require('inspector');
const { EventEmitter } = require('events');
const { monitorEventLoopDelay } = require('perf_hooks');
for (const transport of [require('http'), require('https')]) {
  transport.request = transport.get = () => { throw new Error('fixture network access refused'); };
}
const scratch = fs.mkdtempSync('/tmp/paper-active-benchmark-');
process.env.PAPER_DB = path.join(scratch, 'fixture.db');
// The production default is deliberately fail-closed on EACCES. A fixture
// running as an isolated user must not accidentally see production maintenance.
process.env.PAPER_MAINTENANCE_FILE = path.join(scratch, 'maintenance');
process.env.PAPER_COMP_TOKEN = 'isolated-benchmark-only';
process.env.PAPER_GATE_SECRET = 'isolated-benchmark-gate';
const seats = Number(process.env.BENCH_SEATS) || 4;
const legs = Number(process.env.BENCH_LEGS) || 1;
const frames = Number(process.env.BENCH_FRAMES) || 100;
const phase = process.env.BENCH_PHASE || 'build';
const wsClients = Number(process.env.BENCH_WS_CLIENTS || 3);
const sseClients = Number(process.env.BENCH_SSE_CLIENTS || 1);
if (!Number.isInteger(wsClients) || wsClients < 1 || wsClients > 32
    || !Number.isInteger(sseClients) || sseClients < 1 || sseClients > 16) {
  throw new Error('bounded audience only: 1..32 WS and 1..16 SSE clients');
}
if (![4, 8].includes(seats) || ![1, 4].includes(legs) || frames < 1 || frames > 500) {
  throw new Error('bounded fixtures only: 4/8 seats, 1/4 legs, 1..500 frames');
}
if (!['build','hot','boost'].includes(phase)) throw new Error('phase must be build, hot or boost');
const source = fs.readFileSync(path.join(__dirname, 'test-two-hot.js'), 'utf8');
const boundary = source.indexOf('(async () => {');
if (boundary < 0) throw new Error('fixture setup boundary changed; review before running');
const engine = fs.readFileSync(path.join(__dirname, 'paper.js'), 'utf8');
const allMatch = engine.match(/const LAZER_ALL = (\{[\s\S]*?\n\});/);
if (!allMatch) throw new Error('market declaration changed; review before running');
const feedMap = vm.runInNewContext('(' + allMatch[1] + ')');
const all = Object.keys(feedMap);
const P = require('./paper.js');
if (P.deploymentMaintenanceActive()) throw new Error('fixture maintenance must be independently inactive');
let config;
P.engineConfig({}, { writeHead() {}, end(s) { config = JSON.parse(s); } });
const symbols = all.filter((s) => !config.disabled.includes(s));
const session = process.env.BENCH_PROFILE === '1' ? new inspector.Session() : null;
const post = (method, params = {}) => new Promise((resolve, reject) =>
  session.post(method, params, (err, result) => err ? reject(err) : resolve(result)));
const summarizeProfile = (profile) => {
  const nodes = new Map(profile.nodes.map((n) => [n.id, n]));
  const self = new Map();
  for (let i = 0; i < profile.samples.length; i++) {
    const n = nodes.get(profile.samples[i]);
    const label = `${n.callFrame.functionName || '(anonymous)'} ${path.basename(n.callFrame.url || '')}:${n.callFrame.lineNumber + 1}`;
    self.set(label, (self.get(label) || 0) + profile.timeDeltas[i] / 1000);
  }
  return [...self].map(([name, ms]) => ({ name, ms: +ms.toFixed(3) }))
    .sort((a, b) => b.ms - a.ms).slice(0, 25);
};
globalThis.activeBenchmark = {
  symbols, seats, legs, frames, phase,
  stream: process.env.BENCH_STREAM === '1',
  legacyQueries: process.env.BENCH_LEGACY_QUERIES === '1',
  baselineHz: Math.min(20, Math.max(0, Number(process.env.BENCH_BASELINE_HZ) || 0)),
  streamSeconds: Math.min(30, Math.max(10, Number(process.env.BENCH_SECONDS) || 12)),
  fast: symbols.filter((s) => feedMap[s][1] !== 'f200'),
  makeAudience: () => {
    const Ws = require('ws'), RealServer = Ws.Server;
    let upgrade;
    Ws.Server = class { handleUpgrade(_req, socket, _head, done) { done(socket.fakeWs); } };
    try { P.attachIndexWs({ on(event, fn) { if (event === 'upgrade') upgrade = fn; } }); }
    finally { Ws.Server = RealServer; }
    const counts = { bundles:0, incompleteBoards:0, priceFrames:0, sseWrites:0 };
    const clients = [];
    for (let i=0; i<wsClients; i++) {
      const ws = new EventEmitter();
      ws.readyState=1; ws.bufferedAmount=0;
      ws.send = (raw) => {
        if (i !== 0) return;
        const frame = JSON.parse(String(raw));
        if (frame.type === 'bundle') {
          counts.bundles++;
          for (const item of frame.items) if (item.type === 'comp' && item.complete === false) counts.incompleteBoards++;
        } else if (Number.isFinite(frame.p)) counts.priceFrames++;
      };
      ws.ping = () => ws.emit('pong');
      ws.terminate = () => ws.emit('close');
      // All devices share the venue NAT; separate IPs would miss per-IP limits.
      const socket = { fakeWs:ws, remoteAddress:'127.0.0.120', destroy() { throw new Error('fixture WS admission refused'); } };
      upgrade({ url:'/api/paper/index-ws', headers:{'x-paper-gate':process.env.PAPER_GATE_SECRET,
        'x-real-ip':socket.remoteAddress}, socket },socket,Buffer.alloc(0));
      clients.push(ws);
    }
    const requests = [];
    for (let i=0; i<sseClients; i++) {
      const req = new EventEmitter(); req.headers={'x-real-ip':'127.0.0.120'};
      req.socket={remoteAddress:'127.0.0.120'};
      P.pythStream(req,{writableLength:0,writeHead(code){if(code!==200)throw new Error('fixture SSE admission refused');},
        write(){counts.sseWrites++;return true;},end(){},on(){return this;}});
      requests.push(req);
    }
    return {counts, wsClients, sseClients,
      close(){for(const ws of clients)ws.emit('close');for(const req of requests)req.emit('close');}};
  },
  loopMonitor: () => { const monitor=monitorEventLoopDelay({resolution:10});monitor.enable();return monitor; },
  profileStart: async () => {
    if (!session) return;
    session.connect();
    await post('Profiler.enable');
    await post('Profiler.setSamplingInterval', { interval: 1000 });
    await post('Profiler.start');
  },
  profileStop: async () => {
    if (!session) return null;
    const result = await post('Profiler.stop');
    session.disconnect();
    return summarizeProfile(result.profile);
  },
  cleanup: () => {
    P.stopSourceExpiry();
    try { require('./auth-shim.js').db.close(); }
    finally { fs.rmSync(scratch, { recursive: true, force: true }); }
  },
};

vm.compileFunction(source.slice(0, boundary) + `
(async () => {
  const B = globalThis.activeBenchmark;
  try {
    for (const s of B.symbols) prime(s, 100);
    if (B.stream) for (const s of B.symbols) {
      // Select the primary before the round starts. Deleting the durable
      // source's component would instead simulate restart recovery, which is
      // intentionally not this normal-flow workload.
      for (const component of Object.values(T.comps.get(s))) {
        component.srcAt=Date.now()-60000; component.ts=Date.now()-60000; component.age0=60000;
      }
      T.compUpdate(s,'lazer',100,Date.now(),Date.now());
    }
    const players = Array.from({length:B.seats}, (_,i) => ({
      userId:8400+i, displayName:'Fixture '+i, seat:i
    }));
    for (const p of players) { userIns.run(p.userId); T.__ensureAccountRef()(p.userId); }
    const r = create('active-loaded-benchmark', {players, candidates:['BTC','ETH','SOL']});
    start(r.id);
    comp.wire({
      ensureClockHealth: T.__ensureCompetitionClockHealth,
      marketAvailability: (s, kind, at, lev) => T.__marketAvailability(s,
        kind === 'BOOST' ? (lev || 500) : 100, at),
    });
    if(B.legacyQueries) {
      // Reproduce the prior query shape inside this disposable fixture only.
      // Production code and all price/transaction decisions remain untouched.
      CT.q.checkpointCounts.get=(id)=>({players:CT.q.players.all(id).length,
        scores:CT.q.scores.all(id,'final').length});
      T.stmt.clockExposure.all=(id)=>comp.playersOf(id).flatMap(p=>T.stmt.posByUser.all(p.user_id));
    }
    if(B.phase!=='build') {
      // Only the disposable fixture's clock origin is shifted. Every crossed
      // boundary receives a synthetic accepted historical mark and executes
      // fireBoundary normally: no phase or succeeded row is fabricated. Seats
      // are still flat while Hot proofs and the Boost bankroll are frozen.
      const rr=CT.q.get.get(r.id),draw=CT.privateDrawOf(rr),plan=comp.planOf(rr);
      const target=(B.phase==='hot'?draw.hot1.activation:plan.boostStart)+1000;
      setActiveOffset(r.id,target);
      const shifted=CT.q.get.get(r.id);
      for(const offset of comp.boundariesOf(shifted).filter(at=>at<=target)) {
        const due=shifted.started_at+offset;
        for(const s of B.symbols)T.recordMark(s,100,due,2,0,'lazer',due);
        CT.fireBoundary(r.id,offset,due);
        const boundary=CT.q.bGet.get(r.id,offset);
        if(!boundary||boundary.status!=='succeeded')throw Error('fixture phase boundary did not commit');
      }
      assert.strictEqual(comp.phaseNow().phase,B.phase);
      if(B.phase==='hot')assert.ok(CT.q.hotRows.all(r.id,1).length===B.seats);
      if(B.phase==='boost')for(const player of comp.playersOf(r.id)) {
        assert.strictEqual(player.boost_bankroll,10);
        assert.strictEqual(player.boost_max_exposure,5000);
        assert.ok(player.boost_frozen_at>0);
      }
    }
    for (const [i,p] of players.entries()) {
      const hotBase=B.phase==='hot'?CT.q.get.get(r.id).hot1_active_base:null;
      const held = B.legs === 4 ? ['BTC','ETH','SOL','XRP'] : [i % 4 < 3 ? 'BTC' : (hotBase||'SOL')];
      for (const s of held) T.applyFill(p.userId, {symbol:s+(B.phase==='boost'?'-BOOST':''), orderSide:'BUY',
        size:1, px:100, feeBps:0, kind:'MARKET', leverage:B.phase==='boost'?500:100,
        marginMode:'isolated', executionSource:'fixture', referenceMark:100});
    }
    if (B.stream) {
      // This provider schedule is the actual 8 fast/28 slow market split.
      // Timestamps are allocated by the scheduled provider slot, so any
      // processing backlog consumes the ordinary production freshness budget.
      for(const s of B.symbols) T.compUpdate(s,'lazer',100,Date.now(),Date.now());
      T.__startSourceExpiry();
      assert.strictEqual(T.__sourceExpiryTimerCount(), B.symbols.length, 'all source watchdogs must be active');
      const audience=B.makeAudience(), lag=B.loopMonitor();
      const cpuStart=process.cpuUsage(), began=performance.now(), origin=Date.now();
      let offered=0,accepted=0,pausedSamples=0,maxSlotLag=0,baselineCalls=0,baselineFailures=0;
      const batchTimes=[];
      const baselineTimes=[];
      const baselineTimer=B.baselineHz ? setInterval(()=>{
        const began=performance.now();
        P.compBaseline({headers:{'x-real-ip':'127.0.0.124'}},{writeHead(code){if(code!==200)baselineFailures++;},
          end(raw){const body=JSON.parse(raw);if(!body.ok||!body.live)baselineFailures++;}});
        baselineCalls++;baselineTimes.push(performance.now()-began);
      },1000/B.baselineHz) : null;
      await B.profileStart();
      for(let i=1;i<=B.streamSeconds*20;i++) {
        const scheduledAt=origin+i*50;
        await new Promise(resolve=>setTimeout(resolve,Math.max(0,scheduledAt-Date.now())));
        const at=Date.now(),batchBegan=performance.now();
        maxSlotLag=Math.max(maxSlotLag,at-scheduledAt);
        const updating=i%4===0?B.symbols:B.fast;
        for(const s of updating) {
          const before=T.live.map.get(s).acceptedSeq;
          // Successively wider tiny excursions change the drawdown record,
          // exercising its real writes without crossing SL/TP/liquidation.
          T.compUpdate(s,'lazer',100+(i%2?i:-i)*0.000001,at,scheduledAt);
          offered++;
          if(T.live.map.get(s).acceptedSeq!==before)accepted++;
        }
        batchTimes.push(performance.now()-batchBegan);
        if(T.roundPaused())pausedSamples++;
      }
      const profile=await B.profileStop(),elapsedMs=performance.now()-began;
      if(baselineTimer)clearInterval(baselineTimer);
      assert.strictEqual(T.__sourceExpiryTimerCount(), B.symbols.length, 'watchdogs stayed active');
      assert.strictEqual(accepted, offered, 'every offered observation was accepted');
      assert.strictEqual(pausedSamples, 0, 'no sampled fixture pause');
      assert.strictEqual(baselineFailures, 0, 'all baselines succeeded');
      assert.ok(audience.counts.bundles > 0, 'WebSocket competition bundles were delivered');
      assert.strictEqual(audience.counts.incompleteBoards, 0, 'all observed boards were complete');
      const cpu=process.cpuUsage(cpuStart);lag.disable();audience.close();
      batchTimes.sort((a,b)=>a-b);
      console.log(JSON.stringify({mode:'scheduled-accepted-ingress',symbols:B.symbols.length,
        phase:B.phase,legacyQueries:B.legacyQueries,
        fastSymbols:B.fast.length,seats:B.seats,positions:B.seats*B.legs,elapsedMs,
        offered,accepted,pausedSamples,cpuMs:(cpu.user+cpu.system)/1000,
        sourceExpiryTimers:T.__sourceExpiryTimerCount(),
        averageCpuPct:(cpu.user+cpu.system)/10/elapsedMs,maxSlotLagMs:maxSlotLag,
        eventLoop:{maxMs:lag.max/1e6,p99Ms:lag.percentile(99)/1e6},
        batchP50Ms:batchTimes[Math.floor(batchTimes.length*.5)],
        batchP95Ms:batchTimes[Math.floor(batchTimes.length*.95)],
        batchMaxMs:batchTimes[batchTimes.length-1],baselineHz:B.baselineHz,baselineCalls,baselineFailures,
        baselineMaxMs:baselineTimes.length?Math.max(...baselineTimes):null,
        audience:{...audience.counts,wsClients:audience.wsClients,sseClients:audience.sseClients},profile},null,2));
      abortLive();T.__armCompetitionClockExpiry();return;
    }
    const stats = new Map();
    let measured = false;
    for (const [group,table] of [['paper',T.stmt],['comp',CT.q]]) {
      for (const [name,st] of Object.entries(table)) {
        for (const method of ['all','get','run']) {
          if (typeof st[method] !== 'function') continue;
          const original = st[method].bind(st);
          st[method] = (...args) => {
            if (!measured) return original(...args);
            const began = performance.now();
            try { return original(...args); }
            finally {
              const key = group+'.'+name+'.'+method;
              const row = stats.get(key) || {key, count:0, ms:0};
              row.count++; row.ms += performance.now()-began; stats.set(key,row);
            }
          };
        }
      }
    }
    const times = [], batches = [];
    await B.profileStart();
    for (let i=0; i<B.frames; i++) {
      const batchAt = performance.now();
      for (const s of B.symbols) prime(s,100);
      batches.push(performance.now()-batchAt);
      measured = true;
      const began = performance.now();
      const result = T.tickEval('BTC', {force:true});
      times.push(performance.now()-began);
      measured = false;
      if (!result.ok || T.roundPaused()) throw result.error || new Error('fixture paused');
      await new Promise((resolve) => setImmediate(resolve));
    }
    const profile = await B.profileStop();
    const distribution = (xs) => {
      xs.sort((a,b) => a-b);
      return {p50Ms:xs[Math.floor(xs.length*.5)], p95Ms:xs[Math.min(xs.length-1,Math.floor(xs.length*.95))], maxMs:xs[xs.length-1]};
    };
    console.log(JSON.stringify({symbols:B.symbols.length, seats:B.seats,
      positions:B.seats*B.legs, frames:B.frames, singleHeldTick:distribution(times),
      fullPrimingBatch:distribution(batches),
      dbTop:[...stats.values()].sort((a,b) => b.ms-a.ms).slice(0,20), profile},null,2));
    abortLive(); T.__armCompetitionClockExpiry();
  } finally { B.cleanup(); }
})().catch((e) => { console.error(e.stack || e); process.exitCode=1; })
  .finally(() => process.exit(process.exitCode || 0));
`, ['require', '__dirname'], {filename:path.join(__dirname, 'isolated-active-round-benchmark.js')})(require, __dirname);
