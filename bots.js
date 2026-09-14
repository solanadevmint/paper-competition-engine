/* PRACTICE SEATS.
 *
 * One person cannot rehearse an eight-seat format alone: the wall is seven
 * flat rows, neither surprise Hot Market has a contest, and nothing about the show's shape is
 * visible until eight people are in a room. This drives the other seats.
 *
 * The design rule that matters: a bot is a CLIENT, not a privileged path. It
 * places orders over the same HTTP endpoint a person does, with a real
 * session, so every guard the engine has — the family invariant, the
 * competition leverage ceiling, segment gates, price pauses, rate limits,
 * min notional — applies to a bot exactly as written, with no second
 * implementation to keep in step. A bot order that gets refused is simply a
 * bot that does not trade this tick, which is the correct outcome and needs
 * no special handling.
 *
 * Two hard gates, both asserted at the only place an order is issued:
 *   1. the round must be marked `solo`;
 *   2. the account must be inside the reserved bot id band.
 * Neither is inferred from the other. A real round cannot be driven, and a
 * real person cannot be driven, whatever else is wrong.
 */
'use strict';

const http = require('node:http');

/* Resolved per call, not at load. Read once at module scope it froze
   whatever the environment happened to be when the file was first required,
   which is the wrong value in every harness that starts a server and then
   points the driver at it. */
const port = () => Number(hooks.port || process.env.PAPER_PORT || 9200);
/* How often the field moves. Fast enough that the wall is alive, slow enough
   that it reads as trading rather than noise, and well inside the per-user
   write budget. */
const TICK_MS = Number(process.env.PAPER_BOT_TICK_MS || 6000);
/* Chance a flat bot opens on a given tick, and that a positioned one closes.
   Asymmetric on purpose: bots should mostly be IN the market, because an
   empty board teaches the operator nothing about what the show looks like. */
const P_OPEN = 0.55;
const P_CLOSE = 0.22;

let comp = null;
let hooks = { log: () => {}, sessionFor: null, positionsOf: null, marketsFor: null, port: null };
let timer = null;
let running = null;          // round id currently being driven

function wire(h) { hooks = { ...hooks, ...h }; }
function attach(c) { comp = c; }

const pick = (a) => a[Math.floor(Math.random() * a.length)];
const between = (lo, hi) => lo + Math.random() * (hi - lo);

/** One order, as a client. Resolves to the parsed reply whatever the status:
 *  a refusal is information, not an error. */
function order(token, body) {
  return new Promise((resolve) => {
    const data = JSON.stringify(body);
    const req = http.request({
      host: '127.0.0.1', port: port(), path: '/api/paper/order', method: 'POST',
      headers: {
        'content-type': 'application/json',
        'content-length': Buffer.byteLength(data),
        /* The bot is a signed-in user and nothing more. */
        cookie: `phoenix_session=${token}`,
        /* The server refuses anything that did not come through the site's
           gate. Real traffic gets this header from nginx; the driver lives on
           the box and has to add it itself. The probe called the engine
           function directly and never met the gate, which is how every bot
           order was "forbidden" on the first real round. */
        'x-paper-gate': process.env.PAPER_GATE_SECRET || '',
      },
      timeout: 8000,
    }, (res) => {
      let buf = '';
      res.on('data', (c) => { buf += c; });
      res.on('end', () => { try { resolve(JSON.parse(buf)); } catch { resolve({ ok: false, error: 'bad_reply' }); } });
    });
    req.on('error', (e) => resolve({ ok: false, error: e.message }));
    req.on('timeout', () => { req.destroy(); resolve({ ok: false, error: 'timeout' }); });
    req.end(data);
  });
}

/** What this bot should do this tick, given what it already holds. */
function decide(positions, markets, boostLeverage = 500) {
  /* During Hot/Boost `markets` is deliberately narrowed to the revealed
     ordinary Hot asset or opened Boost aliases. An unrelated build position
     must not make the bot sit out the mechanic we are rehearsing. */
  const inCurrentMarket = positions.filter((p) => markets.includes(p.symbol));
  if (inCurrentMarket.length) {
    /* Close the oldest leg sometimes, so positions turn over and the board
       shows realised as well as open PnL. */
    if (Math.random() < P_CLOSE) {
      const p = inCurrentMarket[0];
      return { kind: 'close', symbol: p.symbol, side: p.side === 'LONG' ? 'SELL' : 'BUY', size: p.size };
    }
    return null;
  }
  if (Math.random() > P_OPEN) return null;
  if (!markets.length) return null;
  const symbol = pick(markets);
  return {
    kind: 'open',
    symbol,
    side: Math.random() < 0.5 ? 'BUY' : 'SELL',
    /* Modest and varied. Big enough to move a score, small enough that a bot
       does not liquidate itself out of the round in the first two minutes. */
    notionalUsd: between(3, 12),
    /* Actually exercise the headline Boost order path in practice. The
       notional stays modest and the server remains the authority for both
       the frozen exposure budget and the live safe cap. */
    leverage: /-BOOST$/.test(symbol)
      ? Math.max(1, Number(boostLeverage) || 500) : Math.round(between(2, 20)),
  };
}

async function tickOnce() {
  if (!comp) return;
  let r = null;
  try { r = comp.currentRound(); } catch { return; }
  if (!r || r.status !== 'running') return;
  /* GATE 1. Only a round that was ARMED as practice is ever driven. Read
     fresh from the round row every tick, never cached, so a flag that somehow
     changed takes effect immediately rather than at the next restart. */
  if (!r.solo) return;
  if (r.blocked_reason) return;                       // a blocked round is stopped, for bots too

  let seats = [];
  try { seats = comp.playersOf(r.id) || []; } catch { return; }
  const markets = hooks.marketsFor ? hooks.marketsFor(r) : [];

  for (const seat of seats) {
    const uid = seat.user_id;
    /* GATE 2. Independent of gate 1: the id must be in the reserved band.
       A person seated in a practice round trades for themselves. */
    if (!comp.isBotId(uid)) continue;
    let token = null;
    try { token = hooks.sessionFor(uid); } catch { continue; }
    if (!token) continue;
    let positions = [];
    try { positions = hooks.positionsOf(uid) || []; } catch { positions = []; }
    const move = decide(positions, markets, Number(r.boost_leverage) || 500);
    if (!move) continue;
    const body = move.kind === 'close'
      ? { symbol: move.symbol, side: move.side, type: 'MARKET', size: move.size, reduceOnly: true }
      : { symbol: move.symbol, side: move.side, type: 'MARKET', notionalUsd: move.notionalUsd, leverage: move.leverage };
    if (comp.pricePolicyOf && comp.pricePolicyOf(r) === 'last-accepted-v1') {
      const acknowledged = hooks.priceAcknowledgmentFor?.(uid, move.symbol);
      if (!acknowledged) continue;
      Object.assign(body, acknowledged);
    }
    /* Awaited one at a time rather than fired in parallel: eight concurrent
       orders against one SQLite writer is a burst the show never produces,
       and a rehearsal that stresses a path the real thing never takes is
       measuring the wrong system. */
    const reply = await order(token, body);
    if (!reply.ok && reply.error && reply.error !== 'rate_limited') {
      hooks.log(`bot seat ${seat.seat + 1} ${move.kind} ${move.symbol} refused: ${reply.error}`);
    }
  }
}

let inFlight = false;
async function tick() {
  if (inFlight) return;                 // a slow tick must not stack on itself
  inFlight = true;
  try { await tickOnce(); } catch (e) { hooks.log('bot tick failed: ' + e.message); }
  finally { inFlight = false; }
}

/** Idempotent. Called on every phase event, so a restart mid-round picks the
 *  bots back up without anything having to remember they were running. */
function sync() {
  if (!comp) return;
  let r = null;
  try { r = comp.currentRound(); } catch { r = null; }
  const want = r && r.status === 'running' && r.solo ? r.id : null;
  if (want === running) return;
  if (timer) { clearInterval(timer); timer = null; }
  running = want;
  if (!want) { hooks.log('practice seats idle'); return; }
  /* A sped-up round gets proportionally busier bots, floored so four seats
     cannot hammer the order path. */
  const every = Math.max(750, Math.round(TICK_MS / (Number(r.speed) || 1)));
  timer = setInterval(() => { void tick(); }, every);
  if (timer.unref) timer.unref();
  hooks.log(`practice seats driving round ${want} every ${every}ms`);
}

function stop() {
  if (timer) clearInterval(timer);
  timer = null; running = null;
}

module.exports = { wire, attach, sync, stop, __test: { decide, tickOnce } };
