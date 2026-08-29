/* Competition round clock, phase gate and verifiable Hot Market draw.
 *
 * The show runs off ONE server clock. Every phase boundary fires from a timer
 * anchored to the round's start timestamp, never from a client poll and never
 * from a sweep that happens to run nearby: at 1000x a late boundary is worth
 * real money, and "the wall said Boost was open" has to mean the engine
 * agreed. Boundaries are recomputed from the database on boot, so an engine
 * restart mid-round resumes the same schedule instead of losing it.
 *
 * This module owns no market data and no account math. paper.js wires the
 * alias controls and a logger in at startup; everything here is either pure
 * (phase arithmetic, the draw) or a row in paper_rounds.
 */
const crypto = require('crypto');
const auth = require('./auth-shim.js');

const db = auth.db;
const MIN = 60_000;

/* Phase plan per round type. Times are milliseconds from the start bell.
 * The reveal sits 15s before the Hot window opens so the draw is a beat of
 * its own and the multiplier is not live during the scramble to react to it:
 * everyone enters the segment having seen the same market for the same
 * length of time. Boost is the last 3 minutes in both formats. */
const ROUND_PLAN = {
  round: { total: 30 * MIN, firstFive: 5 * MIN, reveal: 11.75 * MIN, hotStart: 12 * MIN, hotEnd: 16 * MIN, boostStart: 27 * MIN },
  final: { total: 20 * MIN, firstFive: 4 * MIN, reveal: 7.75 * MIN, hotStart: 8 * MIN, hotEnd: 11 * MIN, boostStart: 17 * MIN },
};

/* Leverage ceiling on ordinary tickers while a round is running. Boost is the
 * only way past it, and only on a -BOOST twin during the Boost phase. */
const COMP_BASE_LEV = 100;

db.exec(`
  CREATE TABLE IF NOT EXISTS paper_rounds (
    id             TEXT PRIMARY KEY,
    kind           TEXT    NOT NULL CHECK (kind IN ('round','final')),
    status         TEXT    NOT NULL CHECK (status IN ('armed','running','done','aborted')),
    started_at     INTEGER,
    ends_at        INTEGER,
    hot_candidates TEXT    NOT NULL,
    hot_backup     TEXT,
    hot_base       TEXT,
    draw_commit    TEXT    NOT NULL,
    draw_seed      TEXT,
    draw_at        INTEGER,
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS paper_round_players (
    round_id     TEXT    NOT NULL,
    user_id      INTEGER NOT NULL,
    display_name TEXT,
    seat         INTEGER NOT NULL,
    PRIMARY KEY (round_id, user_id)
  );
  /* Frozen standings. One row per player per checkpoint, written once from a
     single shared price snapshot so every account is marked against the same
     instant. Nothing recomputes these afterwards: a published result must
     still read the same tomorrow. */
  CREATE TABLE IF NOT EXISTS paper_round_scores (
    round_id    TEXT    NOT NULL,
    user_id     INTEGER NOT NULL,
    checkpoint  TEXT    NOT NULL CHECK (checkpoint IN ('firstFive','final')),
    at          INTEGER NOT NULL,
    equity      REAL    NOT NULL,
    account_pnl REAL    NOT NULL,
    realized    REAL    NOT NULL,
    hot_bonus   REAL    NOT NULL,
    score       REAL    NOT NULL,
    PRIMARY KEY (round_id, user_id, checkpoint)
  );
`);

const q = {
  ins: db.prepare(`INSERT INTO paper_rounds (id, kind, status, hot_candidates, hot_backup, draw_commit, created_at, updated_at)
                   VALUES (?, ?, 'armed', ?, ?, ?, ?, ?)`),
  start: db.prepare('UPDATE paper_rounds SET status = ?, started_at = ?, ends_at = ?, updated_at = ? WHERE id = ?'),
  setStatus: db.prepare('UPDATE paper_rounds SET status = ?, updated_at = ? WHERE id = ?'),
  setDraw: db.prepare('UPDATE paper_rounds SET hot_base = ?, draw_seed = ?, draw_at = ?, updated_at = ? WHERE id = ?'),
  get: db.prepare('SELECT * FROM paper_rounds WHERE id = ?'),
  running: db.prepare("SELECT * FROM paper_rounds WHERE status = 'running' ORDER BY started_at DESC LIMIT 1"),
  playerIns: db.prepare('INSERT OR REPLACE INTO paper_round_players (round_id, user_id, display_name, seat) VALUES (?, ?, ?, ?)'),
  players: db.prepare('SELECT * FROM paper_round_players WHERE round_id = ? ORDER BY seat'),
  scoreIns: db.prepare(`INSERT OR IGNORE INTO paper_round_scores
                        (round_id, user_id, checkpoint, at, equity, account_pnl, realized, hot_bonus, score)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`),
  scores: db.prepare(`SELECT s.*, p.display_name, p.seat FROM paper_round_scores s
                      JOIN paper_round_players p ON p.round_id = s.round_id AND p.user_id = s.user_id
                      WHERE s.round_id = ? AND s.checkpoint = ?`),
};

/* Injected by paper.js so this module never reaches into engine internals. */
let hooks = {
  openAlias: () => {}, closeAlias: () => {}, onPhase: () => {}, log: () => {},
  // paper.js supplies this: it is the only thing here that touches account math
  scoreUser: () => ({ equity: 0, accountPnl: 0, realized: 0, hotBonus: 0 }),
};
function wire(h) { hooks = { ...hooks, ...h }; }

// ── phase arithmetic (pure) ──────────────────────────────────────────────
/** Phase at `elapsed` ms after the start bell. `endsAt` is ms-from-start of
 *  the next boundary, so callers can schedule without re-deriving the plan. */
function phaseAt(kind, elapsed) {
  const p = ROUND_PLAN[kind];
  if (!p) throw new Error('unknown round kind: ' + kind);
  if (elapsed < 0) return { phase: 'pre', endsAt: 0 };
  if (elapsed >= p.total) return { phase: 'done', endsAt: p.total };
  if (elapsed < p.firstFive) return { phase: 'firstFive', endsAt: p.firstFive };
  if (elapsed < p.reveal) return { phase: 'open', endsAt: p.reveal };
  if (elapsed < p.hotStart) return { phase: 'reveal', endsAt: p.hotStart };
  if (elapsed < p.hotEnd) return { phase: 'hot', endsAt: p.hotEnd };
  if (elapsed < p.boostStart) return { phase: 'open', endsAt: p.boostStart };
  return { phase: 'boost', endsAt: p.total };
}
const boundariesOf = (kind) => {
  const p = ROUND_PLAN[kind];
  return [p.firstFive, p.reveal, p.hotStart, p.hotEnd, p.boostStart, p.total];
};

// ── verifiable Hot Market draw ───────────────────────────────────────────
/* Commit-reveal. At round creation we publish sha256(seed + candidates)
 * without the seed; at the reveal we publish the seed. Anyone can then
 * recompute both the commitment and the winning index, which is what makes
 * the draw checkable rather than something production merely asserts. The
 * commitment fixes the candidate list too, so the three markets cannot be
 * swapped after the fact either. */
const commitOf = (seed, candidates) =>
  crypto.createHash('sha256').update(seed + '|' + candidates.join(',')).digest('hex');
const drawIndex = (seed, n) =>
  Number(BigInt('0x' + crypto.createHash('sha256').update(seed).digest('hex')) % BigInt(n));

/** Recompute a finished draw from published values. Nothing secret needed. */
function verifyDraw({ draw_commit, draw_seed, hot_candidates, hot_base }) {
  const cands = JSON.parse(hot_candidates);
  if (!draw_seed) return { ok: false, reason: 'not drawn yet' };
  if (commitOf(draw_seed, cands) !== draw_commit) return { ok: false, reason: 'commitment does not match seed' };
  const expected = cands[drawIndex(draw_seed, cands.length)];
  if (expected !== hot_base) return { ok: false, reason: `seed selects ${expected}, round recorded ${hot_base}` };
  return { ok: true, market: expected };
}

// ── round lifecycle ──────────────────────────────────────────────────────
function createRound({ id, kind = 'round', candidates, backup = null, players = [] }) {
  if (!id) throw new Error('round id required');
  if (!ROUND_PLAN[kind]) throw new Error('unknown round kind: ' + kind);
  if (!Array.isArray(candidates) || candidates.length < 2) throw new Error('need at least 2 hot candidates');
  const now = Date.now();
  const seed = crypto.randomBytes(32).toString('hex');
  _seeds.set(id, seed);
  q.ins.run(id, kind, JSON.stringify(candidates), backup, commitOf(seed, candidates), now, now);
  players.forEach((p, i) => q.playerIns.run(id, p.userId, p.displayName || null, p.seat ?? i));
  hooks.log(`round ${id} armed (${kind}, ${candidates.join('/')}, commit ${commitOf(seed, candidates).slice(0, 12)}…)`);
  return q.get.get(id);
}

/* The seed is held in memory until the reveal so it never sits in the same
 * row as its own commitment before the draw. An engine restart between
 * arming and revealing loses it, which fails loudly at draw time rather than
 * silently drawing from a seed nobody committed to. */
const _seeds = new Map();

function startRound(id, at = Date.now()) {
  const r = q.get.get(id);
  if (!r) throw new Error('no such round: ' + id);
  if (r.status !== 'armed') throw new Error(`round ${id} is ${r.status}, not armed`);
  const ends = at + ROUND_PLAN[r.kind].total;
  q.start.run('running', at, ends, Date.now(), id);
  schedule(id);
  hooks.log(`round ${id} started, bell at ${new Date(ends).toISOString()}`);
  return q.get.get(id);
}

function abortRound(id) {
  const r = q.get.get(id);
  if (!r) throw new Error('no such round: ' + id);
  clearTimers(id);
  closeAllAliases(r);
  q.setStatus.run('aborted', Date.now(), id);
  hooks.log(`round ${id} aborted`);
  return q.get.get(id);
}

const currentRound = () => q.running.get() || null;
const playersOf = (id) => q.players.all(id);

// ── scheduling ───────────────────────────────────────────────────────────
const _timers = new Map();   // roundId -> [Timeout]
function clearTimers(id) {
  for (const t of _timers.get(id) || []) clearTimeout(t);
  _timers.delete(id);
}

/** Arm one timer per remaining boundary. Boundaries already in the past are
 *  fired immediately and in order, so a restart mid-round catches up rather
 *  than skipping the segments it slept through. */
function schedule(id) {
  const r = q.get.get(id);
  if (!r || r.status !== 'running') return;
  clearTimers(id);
  const ts = [];
  for (const at of boundariesOf(r.kind)) {
    const delay = r.started_at + at - Date.now();
    if (delay <= 0) { fireBoundary(id, at); continue; }
    ts.push(setTimeout(() => fireBoundary(id, at), delay));
  }
  _timers.set(id, ts);
}

const _fired = new Set();   // `${id}@${at}` — boundaries are once-only
function fireBoundary(id, at) {
  const key = `${id}@${at}`;
  if (_fired.has(key)) return;
  _fired.add(key);
  const r = q.get.get(id);
  if (!r || r.status !== 'running') return;
  const p = ROUND_PLAN[r.kind];
  try {
    if (at === p.firstFive) { snapshot(id, 'firstFive'); }
    else if (at === p.reveal) { drawHotMarket(id); }
    else if (at === p.hotStart) { openHot(id); }
    else if (at === p.hotEnd) { closeHot(id); }
    else if (at === p.boostStart) { openBoost(id); }
    else if (at === p.total) { bell(id); }
  } catch (e) {
    hooks.log(`round ${id} boundary ${at} FAILED: ${e.message}`);
  }
}

// ── boundary actions ─────────────────────────────────────────────────────
function drawHotMarket(id) {
  const r = q.get.get(id);
  const seed = _seeds.get(id);
  if (!seed) throw new Error(`round ${id}: seed missing, cannot draw (engine restarted after arming?)`);
  const cands = JSON.parse(r.hot_candidates);
  const market = cands[drawIndex(seed, cands.length)];
  q.setDraw.run(market, seed, Date.now(), Date.now(), id);
  _seeds.delete(id);
  hooks.log(`round ${id} HOT MARKET DRAW -> ${market} (seed ${seed.slice(0, 12)}…, verifiable)`);
  hooks.onPhase('hot:draw', q.get.get(id));
}

/* If the drawn market cannot be opened (feed trouble at exactly the wrong
 * moment) fall through to the backup production picked before the round,
 * rather than losing the segment entirely. */
function openHot(id) {
  const r = q.get.get(id);
  const tryOpen = (base) => { hooks.openAlias(base + '-HOT', id); return base; };
  let used;
  try { used = tryOpen(r.hot_base); }
  catch (e) {
    if (!r.hot_backup) throw e;
    hooks.log(`round ${id} hot open failed on ${r.hot_base} (${e.message}), using backup ${r.hot_backup}`);
    used = tryOpen(r.hot_backup);
    q.setDraw.run(used, r.draw_seed, r.draw_at, Date.now(), id);
  }
  hooks.onPhase('hot:open', q.get.get(id));
}

function closeHot(id) {
  const r = q.get.get(id);
  if (r.hot_base) hooks.closeAlias(r.hot_base + '-HOT');
  hooks.onPhase('hot:close', r);
}

/* Boost opens a twin on every major at once: the window is shared, so there
 * is no per-player activation to track and nothing to consume. */
let BOOST_MARKETS = ['BTC', 'ETH', 'BNB', 'XRP', 'SOL'];
const setBoostMarkets = (list) => { BOOST_MARKETS = list.slice(); };
function openBoost(id) {
  const opened = [];
  for (const base of BOOST_MARKETS) {
    try { hooks.openAlias(base + '-BOOST', id); opened.push(base); }
    catch (e) { hooks.log(`round ${id} boost skip ${base}: ${e.message}`); }
  }
  hooks.log(`round ${id} BOOST OPEN: ${opened.join(', ')}`);
  hooks.onPhase('boost:open', q.get.get(id));
}

/* opts pass straight through to closeAlias: an abort cleans up by flattening,
   the bell shuts the gates and leaves every position to be marked. */
function closeAllAliases(r, opts = {}) {
  if (r.hot_base) { try { hooks.closeAlias(r.hot_base + '-HOT', opts); } catch {} }
  if (r.hot_backup) { try { hooks.closeAlias(r.hot_backup + '-HOT', opts); } catch {} }
  for (const base of BOOST_MARKETS) { try { hooks.closeAlias(base + '-BOOST', opts); } catch {} }
}

/* The bell. Stop the segments, mark the round done, and hand off to whatever
 * settles scores. Positions are deliberately NOT force-closed: they are
 * marked where they stand, so nobody gains from clicking faster at the end. */
function bell(id) {
  const r = q.get.get(id);
  // Order matters twice over. Snapshot FIRST, so every account is marked
  // where it actually stands from one pass. Then shut the segment gates
  // WITHOUT flattening: a bell that closed positions would turn the final
  // marks into realised exits, and would hand an edge to whoever closed
  // fastest — the exact thing this format promises it does not do.
  try { snapshot(id, 'final'); } catch (e) { hooks.log(`round ${id} final snapshot FAILED: ${e.message}`); }
  closeAllAliases(r, { flatten: false });
  q.setStatus.run('done', Date.now(), id);
  clearTimers(id);
  hooks.log(`round ${id} BELL`);
  hooks.onPhase('round:end', q.get.get(id));
}

// ── scoring ──────────────────────────────────────────────────────────────
/* Two numbers, deliberately kept apart.
 *
 *   account PnL  — what the paper account is actually worth against its
 *                  starting balance. This drives equity, margin and
 *                  liquidation, and it is the only thing the engine knows.
 *   competition  — account PnL + the Hot Market bonus. This drives the
 *     score       leaderboard and nothing else. A bonus never adds collateral
 *                  and never saves anyone from liquidation.
 *
 * The Hot bonus is the realised PnL on the round's -HOT ticker. Because the
 * segment force-closes at its own bell, that PnL is fully realised by the
 * time anyone reads it, so the bonus needs no snapshot of its own and cannot
 * drift afterwards. Account PnL already contains it once; adding it again is
 * what makes Hot exposure count double, losses included.
 */
function snapshot(roundId, checkpoint) {
  const r = q.get.get(roundId);
  if (!r) throw new Error('no such round: ' + roundId);
  const at = Date.now();
  const hotTicker = r.hot_base ? r.hot_base + '-HOT' : null;
  const rows = [];
  for (const p of q.players.all(roundId)) {
    let s;
    try {
      s = hooks.scoreUser(p.user_id, hotTicker);
    } catch (e) {
      hooks.log(`round ${roundId} score FAILED for ${p.user_id}: ${e.message}`);
      continue;
    }
    const score = s.accountPnl + s.hotBonus;
    q.scoreIns.run(roundId, p.user_id, checkpoint, at, s.equity, s.accountPnl, s.realized, s.hotBonus, score);
    rows.push({ ...p, ...s, score, at });
  }
  hooks.log(`round ${roundId} ${checkpoint} snapshot: ${rows.length} players`);
  hooks.onPhase(checkpoint + ':scored', r);
  return rows;
}

/* Ranking order: score, then realised PnL, then seat. Deterministic, so the
 * operator never has to make a call on stage.
 * NOTE: the format's first tie-break is lowest maximum drawdown, which the
 * engine does not track per round. Until it does, realised PnL is the first
 * live tie-break and this is a known gap, not an oversight. */
function standings(roundId, checkpoint) {
  return q.scores.all(roundId, checkpoint)
    .sort((a, b) => b.score - a.score || b.realized - a.realized || a.seat - b.seat)
    .map((r, i) => ({ ...r, rank: i + 1 }));
}

/** First Five result. Heats require a positive score, so an unclaimed prize
 *  rolls forward; the final always pays, because an anticlimax on the main
 *  stage is worse than rewarding the least-bad round. */
function firstFiveResult(roundId) {
  const r = q.get.get(roundId);
  if (!r) throw new Error('no such round: ' + roundId);
  const board = standings(roundId, 'firstFive');
  if (!board.length) return { winner: null, reason: 'no scores recorded', board };
  const top = board[0];
  if (r.kind === 'final') return { winner: top, reason: 'final always pays', board };
  if (top.score > 0) return { winner: top, reason: 'positive score', board };
  return { winner: null, reason: 'nobody positive, prize rolls over', board };
}

// ── gates consumed by the order path ─────────────────────────────────────
/** Current phase of the running round, or null when no round is live. */
function phaseNow(now = Date.now()) {
  const r = currentRound();
  if (!r || !r.started_at) return null;
  return { round: r, ...phaseAt(r.kind, now - r.started_at) };
}

/** Is this user a player in the running round? The competition must never
 *  change the rules for the public: /ftpaper keeps its own leverage while a
 *  show is on air, and only the eight people on stage are gated. */
const _memo = { roundId: null, ids: new Set() };
function inRound(userId, r = currentRound()) {
  if (!r || userId == null) return false;
  if (_memo.roundId !== r.id) {
    _memo.roundId = r.id;
    _memo.ids = new Set(q.players.all(r.id).map((p) => p.user_id));
  }
  return _memo.ids.has(Number(userId));
}

/** Leverage ceiling for a symbol right now, given the engine's own cap.
 *  Outside a round, or for anyone not playing in it, nothing changes. For a
 *  player: ordinary tickers are held at COMP_BASE_LEV and only a -BOOST twin
 *  during the Boost phase reaches the engine cap, which is what makes Boost
 *  a phase rather than a setting. */
function levCapFor(symbol, engineCap, userId = null, now = Date.now()) {
  const twin = /-(HOT|BOOST)$/.test(symbol);
  const p = phaseNow(now);
  const playing = !!p && inRound(userId, p.round);
  // Event tickers belong to the show whoever asks: a spectator must not be
  // able to trade a segment ticker just by not being on the roster.
  if (twin && !playing) return 0;
  if (!playing) return engineCap;
  if (/-BOOST$/.test(symbol)) return p.phase === 'boost' ? engineCap : 0;
  return Math.min(engineCap, COMP_BASE_LEV);
}

/** Re-arm timers for a round left running by a restart. */
function resume() {
  const r = currentRound();
  if (!r) return null;
  if (Date.now() >= r.ends_at) { fireBoundary(r.id, ROUND_PLAN[r.kind].total); return r; }
  hooks.log(`resuming round ${r.id}, ${Math.round((r.ends_at - Date.now()) / 1000)}s left`);
  schedule(r.id);
  return r;
}

module.exports = {
  ROUND_PLAN, COMP_BASE_LEV, wire, phaseAt, boundariesOf, inRound,
  createRound, startRound, abortRound, currentRound, playersOf,
  phaseNow, levCapFor, resume, verifyDraw, setBoostMarkets,
  snapshot, standings, firstFiveResult,
  __test: { db, q, commitOf, drawIndex, _seeds, _fired, _timers, fireBoundary, schedule },
};
