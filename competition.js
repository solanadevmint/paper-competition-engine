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
    /* The account epoch this player is being scored on, bound when the round
       starts. Scoring must never read "whatever epoch the account is on now":
       if anything bumps it mid-round the scoring basis would move under the
       result. */
    epoch        INTEGER,
    PRIMARY KEY (round_id, user_id)
  );
  /* Frozen standings. One row per player per checkpoint, written once from a
     single shared price snapshot so every account is marked against the same
     instant. Nothing recomputes these afterwards: a published result must
     still read the same tomorrow. */
  /* Durable boundary log. _fired is process-local, so a restart would replay
     side effects that already happened; and a boundary that threw used to be
     marked done anyway. Status here is the truth: only 'succeeded' suppresses
     a replay, and a 'failed' boundary is visible to the operator instead of
     living in a log line. */
  CREATE TABLE IF NOT EXISTS paper_round_boundaries (
    round_id  TEXT    NOT NULL,
    at        INTEGER NOT NULL,
    status    TEXT    NOT NULL,
    error     TEXT,
    ran_at    INTEGER NOT NULL,
    PRIMARY KEY (round_id, at)
  );
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

/* Added after the table existed in the wild. */
for (const alter of [
  'ALTER TABLE paper_round_players ADD COLUMN epoch INTEGER',
  /* Set when settlement could not complete. The round deliberately does NOT
     become 'done': a result that could not be computed must not look
     finished, and the operator has to clear it. */
  'ALTER TABLE paper_rounds ADD COLUMN blocked_reason TEXT',
  /* hot_base is the DRAWN market and is never overwritten: it is what the
     commitment proves. active_hot_base is what actually traded, which differs
     only when the drawn market could not open and the pre-declared backup
     took over. Overwriting the draw made verifyDraw report a mismatch against
     the round's own record. */
  'ALTER TABLE paper_rounds ADD COLUMN active_hot_base TEXT',
  'ALTER TABLE paper_rounds ADD COLUMN boost_markets TEXT',
  'ALTER TABLE paper_round_players ADD COLUMN start_balance REAL',
  /* The exact prices a checkpoint was computed from, so a disputed stage
     result can be replayed rather than argued about. */
  'ALTER TABLE paper_round_scores ADD COLUMN marks TEXT',
  'ALTER TABLE paper_round_scores ADD COLUMN scheduled_at INTEGER',
  /* Running peak equity and the deepest fall from it, sampled through the
     round. The published tie-break is lowest maximum drawdown, and until now
     nothing measured it, so the rule could not actually be applied. */
  'ALTER TABLE paper_round_players ADD COLUMN peak_equity REAL',
  'ALTER TABLE paper_round_players ADD COLUMN max_drawdown REAL',
  'ALTER TABLE paper_rounds ADD COLUMN fallback_reason TEXT',
]) {
  try { db.exec(alter); } catch { /* already applied */ }
}

const q = {
  ins: db.prepare(`INSERT INTO paper_rounds (id, kind, status, hot_candidates, hot_backup, draw_commit, created_at, updated_at)
                   VALUES (?, ?, 'armed', ?, ?, ?, ?, ?)`),
  start: db.prepare('UPDATE paper_rounds SET status = ?, started_at = ?, ends_at = ?, updated_at = ? WHERE id = ?'),
  setStatus: db.prepare('UPDATE paper_rounds SET status = ?, updated_at = ? WHERE id = ?'),
  setDraw: db.prepare('UPDATE paper_rounds SET hot_base = ?, draw_seed = ?, draw_at = ?, updated_at = ? WHERE id = ?'),
  setActive: db.prepare('UPDATE paper_rounds SET active_hot_base = ?, fallback_reason = ?, updated_at = ? WHERE id = ?'),
  get: db.prepare('SELECT * FROM paper_rounds WHERE id = ?'),
  running: db.prepare("SELECT * FROM paper_rounds WHERE status = 'running' ORDER BY started_at DESC LIMIT 1"),
  playerIns: db.prepare('INSERT OR REPLACE INTO paper_round_players (round_id, user_id, display_name, seat) VALUES (?, ?, ?, ?)'),
  players: db.prepare('SELECT * FROM paper_round_players WHERE round_id = ? ORDER BY seat'),
  bindEpoch: db.prepare('UPDATE paper_round_players SET epoch = ? WHERE round_id = ? AND user_id = ?'),
  ddUpd: db.prepare('UPDATE paper_round_players SET peak_equity = ?, max_drawdown = ? WHERE round_id = ? AND user_id = ?'),
  /* Any round that owns its players' accounts: armed means the operator has
     seated them and may reset them at any moment, running means the result is
     being decided. In both states the account belongs to the show. */
  liveForUser: db.prepare(`SELECT r.id FROM paper_round_players p
                           JOIN paper_rounds r ON r.id = p.round_id
                           WHERE p.user_id = ? AND r.status IN ('armed','running') LIMIT 1`),
  anyRunning: db.prepare("SELECT id FROM paper_rounds WHERE status = 'running' LIMIT 1"),
  bMark: db.prepare('INSERT OR REPLACE INTO paper_round_boundaries (round_id, at, status, error, ran_at) VALUES (?, ?, ?, ?, ?)'),
  bGet: db.prepare('SELECT * FROM paper_round_boundaries WHERE round_id = ? AND at = ?'),
  bAll: db.prepare('SELECT * FROM paper_round_boundaries WHERE round_id = ? ORDER BY at'),
  setBlocked: db.prepare('UPDATE paper_rounds SET blocked_reason = ?, updated_at = ? WHERE id = ?'),
  scoreInsStrict: db.prepare(`INSERT INTO paper_round_scores
                        (round_id, user_id, checkpoint, at, equity, account_pnl, realized, hot_bonus, score, marks, scheduled_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`),
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
  // paper.js supplies these: the only things here that touch account state
  scoreUser: () => ({ equity: 0, accountPnl: 0, realized: 0, hotBonus: 0 }),
  // must return { epoch, startBalance, stage } after stamping a uniform account
  prepareSeat: () => null,
  seatState: () => null,
  equityOf: () => Number.NaN,
  // canonical mark set for a roster's exposure, as of a timestamp
  markSetFor: () => ({}),
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
function verifyDraw({ draw_commit, draw_seed, hot_candidates, hot_base, active_hot_base, fallback_reason }) {
  const cands = JSON.parse(hot_candidates);
  if (!draw_seed) return { ok: false, reason: 'not drawn yet' };
  if (commitOf(draw_seed, cands) !== draw_commit) return { ok: false, reason: 'commitment does not match seed' };
  const expected = cands[drawIndex(draw_seed, cands.length)];
  if (expected !== hot_base) return { ok: false, reason: `seed selects ${expected}, round recorded ${hot_base}` };
  /* A fallback does not invalidate the draw; it is a separate, disclosed
     fact. The proof is about what the seed selected, and that still holds. */
  const fellBack = !!active_hot_base && active_hot_base !== hot_base;
  return {
    ok: true, market: expected,
    traded: active_hot_base || hot_base,
    ...(fellBack ? { fellBack: true, fallbackReason: fallback_reason || 'unspecified' } : {}),
  };
}

// ── round lifecycle ──────────────────────────────────────────────────────
function createRound({ id, kind = 'round', candidates, backup = null, players = [] }) {
  if (!id) throw new Error('round id required');
  if (!ROUND_PLAN[kind]) throw new Error('unknown round kind: ' + kind);
  if (!Array.isArray(candidates) || candidates.length < 2) throw new Error('need at least 2 hot candidates');
  /* Validate the whole show constraint before anything is written. A round
     that half-exists is worse than one that failed to arm. */
  const uniq = new Set(candidates);
  if (uniq.size !== candidates.length) throw new Error('duplicate hot candidates');
  if (backup && uniq.has(backup)) throw new Error('backup must not also be a candidate');
  const seats = players.map((p, i) => p.seat ?? i);
  if (new Set(seats).size !== seats.length) throw new Error('duplicate seats');
  const uids = players.map((p) => p.userId);
  if (uids.some((u) => !Number.isFinite(Number(u)))) throw new Error('every player needs a numeric userId');
  if (new Set(uids).size !== uids.length) throw new Error('duplicate players');

  const now = Date.now();
  const seed = crypto.randomBytes(32).toString('hex');
  const commit = commitOf(seed, candidates);
  /* One transaction: the row, its commitment and its roster arrive together
     or not at all. The seed enters memory only after the commit succeeds, so
     a failed insert can never leave a secret that no stored commitment
     corresponds to. */
  const write = db.transaction(() => {
    q.ins.run(id, kind, JSON.stringify(candidates), backup, commit, now, now);
    db.prepare('UPDATE paper_rounds SET boost_markets = ? WHERE id = ?')
      .run(JSON.stringify(BOOST_MARKETS), id);
    players.forEach((p, i) => q.playerIns.run(id, p.userId, p.displayName || null, p.seat ?? i));
  });
  write();
  _seeds.set(id, seed);
  hooks.log(`round ${id} armed (${kind}, ${candidates.join('/')}, commit ${commit.slice(0, 12)}…)`);
  return q.get.get(id);
}

/* The seed is held in memory until the reveal so it never sits in the same
 * row as its own commitment before the draw. An engine restart between
 * arming and revealing loses it, which fails loudly at draw time rather than
 * silently drawing from a seed nobody committed to. */
const _seeds = new Map();

/** Start a round.
 *
 *  This is the only place a round becomes live, so it is where the invariants
 *  are enforced rather than hoped for. Two rounds running at once would give
 *  the wall one clock, the gate another, and leave the older round's players
 *  silently unrostered; and a round that starts on stale accounts scores a
 *  basis nobody agreed to. Both are refused here.
 *
 *  `prepare` (default on) resets every seated player first, so preflight and
 *  reset are not two optional buttons an operator can forget under pressure.
 */
function startRound(id, { at = Date.now(), prepare = true } = {}) {
  const r = q.get.get(id);
  if (!r) throw new Error('no such round: ' + id);
  if (r.status !== 'armed') throw new Error(`round ${id} is ${r.status}, not armed`);
  const other = q.anyRunning.get();
  if (other) throw new Error(`round ${other.id} is already running; abort it first`);
  /* The seed lives in memory only. If it is gone the round is already
     guaranteed to block at its reveal, so refusing here turns a live-stage
     incident into a pre-show "re-arm the round". */
  if (!_seeds.has(id)) {
    throw new Error(`round ${id}: committed draw seed unavailable; re-arm the round`);
  }
  if (r.blocked_reason) throw new Error(`round ${id} is blocked: ${r.blocked_reason}`);
  const players = q.players.all(id);
  if (!players.length) throw new Error('cannot start a round with no players');

  /* Reset, bind the epoch and go live in ONE transaction. If any seat cannot
     be prepared the round does not start at all, rather than starting with
     one player on last round's balance. */
  const ends = at + ROUND_PLAN[r.kind].total;
  const go = db.transaction(() => {
    for (const p of players) {
      /* Preparation stamps a UNIFORM account: same stage mode, same bankroll,
         no positions, no resting orders, fresh epoch. Resetting "in whatever
         mode the account happened to be in" let a standard account start on
         $10,000 with different fees, leverage and lot rules than a stage
         account on $10, which is not the same contest. */
      const prep = prepare ? hooks.prepareSeat(p.user_id) : hooks.seatState(p.user_id);
      if (!prep || !Number.isFinite(prep.epoch) || !Number.isFinite(prep.startBalance)) {
        throw new Error(`could not prepare seat ${p.user_id}`);
      }
      if (!prep.stage) throw new Error(`seat ${p.user_id} is not in stage mode`);
      q.bindEpoch.run(prep.epoch, id, p.user_id);
      db.prepare('UPDATE paper_round_players SET start_balance = ? WHERE round_id = ? AND user_id = ?')
        .run(prep.startBalance, id, p.user_id);
    }
    q.start.run('running', at, ends, Date.now(), id);
  });
  go();
  _memo.roundId = null;   // roster membership cache must not outlive the change
  schedule(id);
  hooks.log(`round ${id} started (${players.length} players prepared), bell at ${new Date(ends).toISOString()}`);
  return q.get.get(id);
}

function abortRound(id) {
  const r = q.get.get(id);
  if (!r) throw new Error('no such round: ' + id);
  clearTimers(id);
  /* An armed round has never opened a ticker. Closing "its" aliases used to
     reach into whatever round actually owned them, so aborting a future round
     could flatten the live round's Boost positions. */
  if (r.status === 'running') closeAllAliases(r);
  q.setStatus.run('aborted', Date.now(), id);
  hooks.log(`round ${id} aborted`);
  return q.get.get(id);
}

const currentRound = () => q.running.get() || null;

/** Operator recovery from a blocked round. Blocking halts every boundary and
 *  every player write, so it needs a deliberate, audited way out: this is it.
 *  Clearing the reason is the ONLY way the clock restarts, and the failed
 *  boundary is reset to retryable in the same transaction so the round
 *  resumes from where it stopped rather than skipping what it missed. */
function clearBlock(id, { note = '' } = {}) {
  const r = q.get.get(id);
  if (!r) throw new Error('no such round: ' + id);
  if (!r.blocked_reason) return r;
  db.transaction(() => {
    db.prepare("UPDATE paper_round_boundaries SET status = 'retryable' WHERE round_id = ? AND status = 'failed'").run(id);
    q.setBlocked.run(null, Date.now(), id);
  })();
  hooks.log(`round ${id} UNBLOCKED by operator${note ? ': ' + note : ''} (was: ${r.blocked_reason})`);
  hooks.onPhase('round:unblocked', q.get.get(id));
  return q.get.get(id);
}

/** Is this account owned by a show right now? True from the moment a player
 *  is seated on an armed round until that round ends. The public reset
 *  endpoint refuses while this holds: a player who could reset mid-round
 *  could lose the bankroll, restore it and carry on. */
/** True when this user's round has stopped accepting writes: the bell has
 *  passed (or is passing) but the round has not been cleared. Used by the
 *  order path to reject anything that arrives after the result was frozen. */
function settledFor(userId, now = Date.now()) {
  const r = currentRound();
  if (!r || !r.started_at || !inRound(userId, r)) return false;
  return now >= r.ends_at;
}

/** The single answer to "may this user change state right now".
 *
 *  Every write path consults this, foreground and background alike. Previously
 *  only placeOrder knew about the bell, so a player could close a position or
 *  a resting order could fill after the result was frozen. Returns a reason
 *  string when writes are barred, or null when they are allowed. */
function writeBarrier(userId, now = Date.now()) {
  const r = currentRound();
  if (!r || !r.started_at || !inRound(userId, r)) return null;
  if (r.blocked_reason) return 'round_blocked';
  if (now >= r.ends_at) return 'round_settled';
  return null;
}

const accountLocked = (userId) =>
  userId != null && !!q.liveForUser.get(Number(userId));
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

/* A boundary runs at most once SUCCESSFULLY. Marking it fired before the
   action ran meant a transient failure was permanent within the process and
   invisible after a restart; now the durable status decides, so a failed
   boundary can be retried and a succeeded one is never replayed. */
function fireBoundary(id, at) {
  const prev = q.bGet.get(id, at);
  if (prev && prev.status === 'succeeded') return;
  if (prev && prev.status === 'running') return;      // re-entrancy guard
  const r = q.get.get(id);
  if (!r || r.status !== 'running') return;
  /* A blocked round is stopped, not merely annotated. Previously the reason
     was recorded and every later boundary still fired, so a round that could
     not prove its draw went on to open Boost and mark itself done. */
  if (r.blocked_reason) {
    hooks.log(`round ${id} boundary ${at} skipped: blocked (${r.blocked_reason})`);
    return;
  }
  const p = ROUND_PLAN[r.kind];
  q.bMark.run(id, at, 'running', null, Date.now());
  try {
    const dueAt = r.started_at + at;      // when this boundary was scheduled
    if (at === p.firstFive) { snapshot(id, 'firstFive', dueAt); }
    else if (at === p.reveal) { drawHotMarket(id); }
    else if (at === p.hotStart) { openHot(id); }
    else if (at === p.hotEnd) { closeHot(id); }
    else if (at === p.boostStart) { openBoost(id); }
    else if (at === p.total) { bell(id, dueAt); }
    q.bMark.run(id, at, 'succeeded', null, Date.now());
  } catch (e) {
    q.bMark.run(id, at, 'failed', String(e.message).slice(0, 500), Date.now());
    hooks.log(`round ${id} boundary ${at} FAILED: ${e.message}`);
    hooks.onPhase('boundary:failed', { ...r, boundary: at, error: e.message });
  }
}

// ── boundary actions ─────────────────────────────────────────────────────
function drawHotMarket(id) {
  const r = q.get.get(id);
  const seed = _seeds.get(id);
  if (!seed) {
    /* The seed lives in memory only, so a restart between arming and the
       reveal loses it. There is then no verifiable draw available, and a
       round whose headline mechanic cannot be proven must not quietly
       continue on a backup market. Block it and make the operator decide. */
    const why = `seed missing, cannot draw (engine restarted after arming?)`;
    q.setBlocked.run(why, Date.now(), id);
    hooks.onPhase('round:blocked', q.get.get(id));
    throw new Error(`round ${id}: ${why}`);
  }
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
  /* No draw, no segment. Falling through to the backup here would open a Hot
     Market that no published commitment selected, which is worse than having
     no Hot Market at all. */
  if (!r.hot_base) throw new Error(`round ${id}: no drawn market, refusing to open Hot`);
  if (r.blocked_reason) throw new Error(`round ${id} is blocked: ${r.blocked_reason}`);
  const tryOpen = (base) => { hooks.openAlias(base + '-HOT', id); return base; };
  let used, why = null;
  try { used = tryOpen(r.hot_base); }
  catch (e) {
    if (!r.hot_backup) throw e;
    why = `${r.hot_base} could not open: ${e.message}`;
    hooks.log(`round ${id} hot open failed on ${r.hot_base}, using backup ${r.hot_backup} (${e.message})`);
    used = tryOpen(r.hot_backup);
  }
  // record what TRADED; the drawn result stays untouched so the proof holds
  q.setActive.run(used, why, Date.now(), id);
  hooks.onPhase('hot:open', q.get.get(id));
}

function closeHot(id) {
  const r = q.get.get(id);
  const active = r.active_hot_base || r.hot_base;
  /* Not wrapped in try/catch on purpose: if the segment cannot be settled
     from one canonical mark, the boundary must fail and the round block
     rather than record a success that left someone holding scored exposure. */
  if (active) hooks.closeAlias(active + '-HOT', { roundId: id });
  hooks.onPhase('hot:close', r);
}

/* Boost opens a twin on every major at once: the window is shared, so there
 * is no per-player activation to track and nothing to consume. */
let BOOST_MARKETS = ['BTC', 'ETH', 'BNB', 'XRP', 'SOL'];
const setBoostMarkets = (list) => { BOOST_MARKETS = list.slice(); };
function openBoost(id) {
  const opened = [];
  for (const base of boostMarketsOf(q.get.get(id))) {
    try { hooks.openAlias(base + '-BOOST', id); opened.push(base); }
    catch (e) { hooks.log(`round ${id} boost skip ${base}: ${e.message}`); }
  }
  hooks.log(`round ${id} BOOST OPEN: ${opened.join(', ')}`);
  hooks.onPhase('boost:open', q.get.get(id));
}

/* opts pass straight through to closeAlias: an abort cleans up by flattening,
   the bell shuts the gates and leaves every position to be marked. */
function closeAllAliases(r, opts = {}) {
  const own = { ...opts, roundId: r.id };   // only close what THIS round opened
  if (r.hot_base) { try { hooks.closeAlias(r.hot_base + '-HOT', own); } catch {} }
  if (r.active_hot_base) { try { hooks.closeAlias(r.active_hot_base + '-HOT', own); } catch {} }
  if (r.hot_backup) { try { hooks.closeAlias(r.hot_backup + '-HOT', own); } catch {} }
  for (const base of boostMarketsOf(r)) { try { hooks.closeAlias(base + '-BOOST', own); } catch {} }
}

/* The Boost market set is persisted on the round at arm time, so a config
   change or a restart cannot alter which tickers a live round owns. */
const boostMarketsOf = (r) => {
  try { const v = JSON.parse((r && r.boost_markets) || 'null'); if (Array.isArray(v) && v.length) return v; }
  catch { /* fall through */ }
  return BOOST_MARKETS;
};

/* The bell. Stop the segments, mark the round done, and hand off to whatever
 * settles scores. Positions are deliberately NOT force-closed: they are
 * marked where they stand, so nobody gains from clicking faster at the end. */
function bell(id, dueAt = null) {
  const r = q.get.get(id);
  // Order matters twice over. Snapshot FIRST, so every account is marked
  // where it actually stands from one pass. Then shut the segment gates
  // WITHOUT flattening: a bell that closed positions would turn the final
  // marks into realised exits, and would hand an edge to whoever closed
  // fastest — the exact thing this format promises it does not do.
  /* If the final snapshot cannot be taken, the round does NOT finish. A round
     marked done with an empty or partial leaderboard is the worst possible
     outcome on a stage: it looks settled and is not. It stays running with a
     blocked reason for the operator to resolve, and the failure propagates so
     the boundary is recorded as failed. */
  try {
    snapshot(id, 'final', dueAt);
  } catch (e) {
    q.setBlocked.run(`final snapshot failed: ${e.message}`, Date.now(), id);
    hooks.log(`round ${id} BELL BLOCKED: ${e.message}`);
    hooks.onPhase('round:blocked', q.get.get(id));
    throw e;
  }
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
/** Take a checkpoint.
 *
 *  `scheduledAt` is when the boundary was DUE, not when this code happened to
 *  run. Every account is priced from one canonical mark set captured as of
 *  that instant, so a late timer callback cannot let a post-bell price move
 *  into the result, and two players can never be settled at different prices.
 *  The mark set is stored with the row. */
function snapshot(roundId, checkpoint, scheduledAt = null) {
  const r = q.get.get(roundId);
  if (!r) throw new Error('no such round: ' + roundId);
  const at = Date.now();
  const asOf = Number.isFinite(scheduledAt) ? scheduledAt : at;
  const hotTicker = (r.active_hot_base || r.hot_base) ? (r.active_hot_base || r.hot_base) + '-HOT' : null;
  const players = q.players.all(roundId);
  const marks = hooks.markSetFor(players.map((p) => p.user_id), asOf);
  const marksJson = JSON.stringify(marks);
  if (at - asOf > 1000) {
    hooks.log(`round ${roundId} ${checkpoint}: boundary ran ${at - asOf}ms late, priced as of the scheduled time`);
  }

  /* Score EVERY player first, then write. A checkpoint containing some of the
     field is worse than none: it publishes a leaderboard that silently omits
     whoever the engine happened to trip over. If any seat cannot be scored
     the whole checkpoint throws and the boundary is recorded as failed. */
  const rows = players.map((p) => {
    const s = hooks.scoreUser(p.user_id, hotTicker, p.epoch, p.start_balance, marks);
    const row = { ...p, ...s, score: s.accountPnl + s.hotBonus, at };
    /* Validate EVERY stored scalar, not just the two the score is built from.
       A null equity used to pass this check and then be dropped by the NOT
       NULL constraint under INSERT OR IGNORE, so the "atomic" checkpoint
       committed with a player silently missing. */
    for (const f of ['equity', 'accountPnl', 'realized', 'hotBonus', 'score']) {
      if (!Number.isFinite(row[f])) {
        throw new Error(`unscoreable seat ${p.user_id} (${p.display_name || 'unnamed'}): ${f}`);
      }
    }
    return row;
  });

  /* Idempotency is explicit rather than delegated to INSERT OR IGNORE:
     a COMPLETE checkpoint is returned as-is, an INCOMPLETE one is a fault
     worth blocking on, and otherwise we insert plainly so any constraint
     violation rolls the whole thing back. */
  const existing = q.scores.all(roundId, checkpoint);
  if (existing.length === rows.length && rows.length > 0) {
    hooks.log(`round ${roundId} ${checkpoint} already complete, leaving it`);
    return existing;
  }
  if (existing.length) {
    throw new Error(`round ${roundId} ${checkpoint} is partial (${existing.length}/${rows.length}); refusing to patch it`);
  }
  db.transaction(() => {
    for (const x of rows) {
      q.scoreInsStrict.run(roundId, x.user_id, checkpoint, at, x.equity, x.accountPnl, x.realized, x.hotBonus, x.score, marksJson, asOf);
    }
    const n = q.scores.all(roundId, checkpoint).length;
    if (n !== rows.length) throw new Error(`checkpoint wrote ${n}/${rows.length} rows`);
  })();
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
  const dd = new Map(q.players.all(roundId).map((p) => [p.user_id, Number(p.max_drawdown) || 0]));
  return q.scores.all(roundId, checkpoint)
    .map((r) => ({ ...r, maxDrawdown: dd.get(r.user_id) ?? 0 }))
    /* Published order: score, then LOWEST maximum drawdown, then higher
       realised PnL, then seat. Deterministic, so nobody has to make a call on
       stage. */
    .sort((a, b) =>
      b.score - a.score ||
      a.maxDrawdown - b.maxDrawdown ||
      b.realized - a.realized ||
      a.seat - b.seat)
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

/** Sample every seat's equity and keep the deepest fall from its own peak.
 *  Called from the engine's sweep, so the resolution is the sweep cadence:
 *  enough to separate two traders on a tie-break, and cheap because it only
 *  ever touches the roster of the one running round. */
function sampleDrawdown(now = Date.now()) {
  const r = currentRound();
  if (!r || !r.started_at || r.blocked_reason || now >= r.ends_at) return;
  for (const p of q.players.all(r.id)) {
    let eq;
    try { eq = hooks.equityOf(p.user_id); } catch { continue; }
    if (!Number.isFinite(eq)) continue;
    const peak = Math.max(Number(p.peak_equity) || eq, eq);
    const dd = Math.max(Number(p.max_drawdown) || 0, peak - eq);
    if (peak !== p.peak_equity || dd !== p.max_drawdown) {
      q.ddUpd.run(peak, dd, r.id, p.user_id);
    }
  }
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
  if (p.round.blocked_reason) return 0;              // a blocked round trades nothing
  /* PHASE is the eligibility rule; the in-memory gate is only the execution
     mechanism. Hot previously relied on the gate alone, so a late or failed
     hot-end timer left the ticker tradable straight through the Boost phase. */
  if (/-BOOST$/.test(symbol)) return p.phase === 'boost' ? engineCap : 0;
  if (/-HOT$/.test(symbol)) return p.phase === 'hot' ? Math.min(engineCap, COMP_BASE_LEV) : 0;
  return Math.min(engineCap, COMP_BASE_LEV);
}

/** Re-arm timers for a round left running by a restart. */
function resume() {
  const r = currentRound();
  if (!r) return null;

  /* A boundary left 'running' by a killed process would otherwise be skipped
     forever, because the re-entrancy guard cannot tell a live execution from
     a dead one. Anything still running at boot belongs to a previous process
     and is demoted to retryable. */
  const stale = db.prepare("UPDATE paper_round_boundaries SET status = 'retryable' WHERE round_id = ? AND status = 'running'").run(r.id);
  if (stale.changes) hooks.log(`round ${r.id}: ${stale.changes} boundary(ies) left running by a dead process, marked retryable`);

  if (r.blocked_reason) {
    hooks.log(`round ${r.id} resumed BLOCKED: ${r.blocked_reason}`);
    return r;
  }

  /* If the process was away across a boundary that has not run, the round
     cannot simply continue: the missed segment never happened, and jumping to
     the bell would manufacture a completed round out of a show that did not
     take place. Block it and let an operator decide. */
  const elapsed = Date.now() - r.started_at;
  const missed = boundariesOf(r.kind).filter((at) => {
    if (at > elapsed) return false;
    const b = q.bGet.get(r.id, at);
    return !b || b.status !== 'succeeded';
  });
  const past = Date.now() >= r.ends_at;
  if (missed.length > (past ? 1 : 0)) {
    const why = `restart missed ${missed.length} boundary(ies): ${missed.join(', ')}`;
    q.setBlocked.run(why, Date.now(), r.id);
    hooks.log(`round ${r.id} BLOCKED on resume: ${why}`);
    hooks.onPhase('round:blocked', q.get.get(r.id));
    return q.get.get(r.id);
  }

  /* Segment gates are in-memory, so a restart mid-Hot leaves the phase saying
     Hot while the ticker is shut. Rebuild them from durable state rather than
     relying on replaying a boundary that is already recorded as succeeded. */
  rehydrateGates(r);

  if (past) { fireBoundary(r.id, ROUND_PLAN[r.kind].total); return q.get.get(r.id); }
  hooks.log(`resuming round ${r.id}, ${Math.round((r.ends_at - Date.now()) / 1000)}s left`);
  schedule(r.id);
  return r;
}

/** Reopen exactly the tickers the current phase says should be open, and
 *  close anything else this round owns. Called on boot. */
function rehydrateGates(r) {
  const ph = phaseAt(r.kind, Date.now() - r.started_at).phase;
  const hot = r.active_hot_base || r.hot_base;
  const want = new Set();
  if (ph === 'hot' && hot) want.add(hot + '-HOT');
  if (ph === 'boost') for (const b of boostMarketsOf(r)) want.add(b + '-BOOST');
  for (const a of want) {
    try { hooks.openAlias(a, r.id); } catch (e) { hooks.log(`rehydrate ${a} failed: ${e.message}`); }
  }
  const all = [hot && hot + '-HOT', r.hot_base && r.hot_base + '-HOT',
    ...boostMarketsOf(r).map((b) => b + '-BOOST')].filter(Boolean);
  for (const a of all) {
    if (!want.has(a)) { try { hooks.closeAlias(a, { flatten: false, roundId: r.id }); } catch {} }
  }
  hooks.log(`round ${r.id} gates rehydrated for phase ${ph}: ${[...want].join(', ') || 'none'}`);
}

module.exports = {
  ROUND_PLAN, COMP_BASE_LEV, wire, phaseAt, boundariesOf, inRound,
  createRound, startRound, abortRound, currentRound, playersOf, accountLocked, clearBlock, sampleDrawdown,
  phaseNow, levCapFor, resume, rehydrateGates, verifyDraw, setBoostMarkets, settledFor, writeBarrier,
  snapshot, standings, firstFiveResult,
  __test: { db, q, commitOf, drawIndex, _seeds, _timers, fireBoundary, schedule },
};
