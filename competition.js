/* Competition round clock, phase gate and verifiable Hot Market draw.
 *
 * The show runs off ONE server clock. Every phase boundary fires from a timer
 * anchored to the round's start timestamp, never from a client poll and never
 * from a sweep that happens to run nearby: at 500x a late boundary is worth
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
/* Identifies this engine process in the durable active-clock lease. A new
 * process can distinguish a real restart from an ordinary resume() call in
 * the same process and bank only genuine engine downtime. */
const CLOCK_OWNER_ID = crypto.randomBytes(16).toString('hex');
const CLOCK_HEARTBEAT_MS = Math.max(250,
  Number(process.env.PAPER_COMP_CLOCK_HEARTBEAT_MS) || 1000);
/* A seat must name an account that actually exists. paper_accounts.user_id
   references users(id), so an unknown id survives arming and only fails at
   START, as a bare "FOREIGN KEY constraint failed" with the clock about to
   run and a room watching. Check it while there is still time to retype. */
/* Any write to paper_rounds invalidates currentRound's per-turn memo (see
   below). Done at prepare time on the shared handle rather than at each of
   the thirty-odd call sites, so a writer added later cannot forget to. */
{
  const rawPrepare = db.prepare.bind(db);
  db.prepare = function preparePatched(sql) {
    const st = rawPrepare(sql);
    if (/^\s*(update|insert|delete|replace)\b[\s\S]*\bpaper_rounds\b/i.test(String(sql))) {
      const run = st.run.bind(st);
      st.run = (...a) => { const r = run(...a); bustCurrentRound(); return r; };
    }
    return st;
  };
}
const userExists = db.prepare('SELECT 1 FROM users WHERE id = ?');
const MIN = 60_000;
// Money and score use the same internal ledger unit. The tie-break order is
// unchanged; new rounds stamp the precision so old checkpoints stay intact.
const ledgerPrecision = (value) => Math.round(value * 1e6) / 1e6;
function rankingValues(s) {
  const accountPnl = ledgerPrecision(s.accountPnl);
  const hotBonus = ledgerPrecision(s.hotBonus);
  return { accountPnl, hotBonus, realized: ledgerPrecision(s.realized),
    score: ledgerPrecision(accountPnl + hotBonus) };
}
const compareStandings = (a, b) => b.score - a.score
  || a.maxDrawdown - b.maxDrawdown || b.realized - a.realized || a.seat - b.seat;

/* Competition time is persisted and proved in whole Unix milliseconds.
 * External price sources can describe a validity edge with sub-millisecond
 * precision, but SQLite's dynamic typing will happily retain that fraction
 * even in an INTEGER column. One such edge made paused_ms, ends_at and every
 * later boundary fractional, which in turn made an otherwise valid score
 * proof fail its integer identity check. Canonicalise at the clock boundary:
 * general events use the nearest millisecond, while a price-pause interval is
 * rounded outwards so no invalid fraction is counted as tradeable time. Every
 * durable timestamp and duration stays in the one unit the protocol promises. */
function competitionMs(value, fallback = Date.now()) {
  const candidate = Number(value);
  const rounded = Math.round(Number.isFinite(candidate) ? candidate : Number(fallback));
  if (!Number.isSafeInteger(rounded) || rounded < 0) {
    throw new Error('invalid competition millisecond timestamp');
  }
  return rounded;
}
function competitionPauseStartMs(value) {
  const floored = Math.floor(Number(value));
  if (!Number.isSafeInteger(floored) || floored < 0) {
    throw new Error('invalid competition pause start');
  }
  return floored;
}
function competitionPauseEndMs(value) {
  const ceiled = Math.ceil(Number(value));
  if (!Number.isSafeInteger(ceiled) || ceiled < 0) {
    throw new Error('invalid competition pause end');
  }
  return ceiled;
}
function competitionDurationMs(value) {
  const rounded = Math.round(Number(value) || 0);
  if (!Number.isSafeInteger(rounded) || rounded < 0) {
    throw new Error('invalid competition millisecond duration');
  }
  return rounded;
}

/* Phase plan per round type. Times are ACTIVE milliseconds from the start
 * bell: wall time spent in a price outage is subtracted everywhere below.
 *
 * Hot activation instants are deliberately NOT part of this public plan.
 * They are sampled and committed before the bell, stored in the private draw
 * envelope, and disclosed only when their warning/activation is due.
 * Ordinary rounds run thirty minutes; the Final runs twenty. */
const ROUND_PLAN = {
  round: {
    total: 30 * MIN, buildEnd: 3 * MIN,
    hot1WindowStart: 3.25 * MIN, hot1WindowEnd: 10 * MIN,
    hot2WindowStart: 13 * MIN, hot2WindowEnd: 20 * MIN,
    hotDuration: 2 * MIN, hotWarning: 15_000, normalGap: MIN,
    finalBuildStart: 22 * MIN, boostStart: 27 * MIN,
  },
  /* The Final is a twenty-minute show with the same five beats. The two
     trading beats keep their full length (Hot Markets two minutes, Boost
     three minutes); the build stretches between them are what shrink. Hot #2
     can end no later than the Final Build starts (12 + 2 = 14), and the
     latest Hot #1 (6:00) still leaves the full gap before the earliest
     Hot #2 (8:30). */
  final: {
    total: 20 * MIN, buildEnd: 2 * MIN,
    hot1WindowStart: 2.25 * MIN, hot1WindowEnd: 6 * MIN,
    hot2WindowStart: 8.5 * MIN, hot2WindowEnd: 12 * MIN,
    hotDuration: 2 * MIN, hotWarning: 15_000, normalGap: MIN,
    finalBuildStart: 14 * MIN, boostStart: 17 * MIN,
  },
  /* A compressed round with the same five beats in the same order, for
     rehearsal and fault injection. A restart-during-Hot drill is not worth
     doing if each attempt costs thirty minutes, and a drill nobody repeats is
     one nobody trusts. Never use this for a real show: the phases are far too
     short to trade. */
  rehearsal: {
    total: 180_000, buildEnd: 18_000,
    hot1WindowStart: 19_500, hot1WindowEnd: 60_000,
    hot2WindowStart: 78_000, hot2WindowEnd: 120_000,
    hotDuration: 12_000, hotWarning: 1_500, normalGap: 6_000,
    finalBuildStart: 132_000, boostStart: 162_000,
  },
};

/* Leverage ceiling on ordinary tickers while a round is running. Boost is the
 * only way past it, and only on a -BOOST twin during the Boost phase. */
const COMP_BASE_LEV = 100;

/* A round can run its clock faster than the format: a 30 minute heat at 10x
   is a three minute heat with every phase in proportion. For practice and
   rehearsal, so the operator can walk the whole shape of a round in the time
   it takes to make coffee. The plan is scaled ONCE at arm time and stamped on
   the round, so nothing downstream knows or cares; it reads the round's own
   plan. The allowed speeds are a short list on purpose. */
const SPEEDS = [1, 2, 5, 10, 30];
function scaledPlan(kind, speed = 1) {
  const p = ROUND_PLAN[kind];
  if (!p) return null;
  const f = Number(speed) || 1;
  const o = {};
  for (const k of Object.keys(p)) o[k] = Math.round(p[k] / f);
  return o;
}
/** The plan THIS round runs on. Falls back to the format's plan for rows
 *  armed before plans were stamped. Accepts a kind string for old callers. */
function planOf(r) {
  if (typeof r === 'string') return ROUND_PLAN[r];
  if (r && r.plan_json) { try { return JSON.parse(r.plan_json); } catch { /* fall through */ } }
  return ROUND_PLAN[r && r.kind];
}
/* THE STOP PRIZE, an owner rule. Engineering does not get to invent this.
 *
 * Per stop: the champion takes the title and $5,000, the runner-up $2,000,
 * third $1,000, fourth $500. The two who go out at the semi-final are ranked
 * against each other by their SEMI-FINAL PnL, which is the only thing that
 * separates them: they lost at the same stage and there is no third place
 * playoff.
 *
 * That last clause is why this lives in the engine rather than on a graphic.
 * At Belgrade the wall kept only the top two of each stage and overwrote the
 * scores when the next round started, so third and fourth were unrecoverable
 * afterwards and $1,500 of the pot had nothing behind it. Here the full board
 * of every round is already durable in paper_round_scores, so the placing is
 * derived from the record rather than remembered. */
/* Per stop: champion, runner-up, and the higher-placed semifinal loser. The
   lower semifinal loser is fourth and unpaid. Revised 2026-09-08, published on
   the engine page since then; this is the first release that pays it. */
const STOP_PRIZE = { 1: 2500, 2: 1500, 3: 1000, 4: 0 };

/* PRACTICE SEATS.
 *
 * Reserved id band, far above any real user id, so a bot and a person can
 * never be the same account by accident and every bot guard can be a range
 * test rather than a lookup. Nothing outside a round marked `solo` will ever
 * drive one, and nothing will ever drive an id outside this band: both halves
 * of that sentence are asserted at the only place orders are issued. */
const BOT_ID_BASE = Number(process.env.PAPER_BOT_ID_BASE || 9_900_000);
const BOT_SEATS = 8;
/* Public board frames and every current show surface are bounded to this. An
   operator payload must not be able to turn synchronous per-tick scoring into
   unbounded roster work or create a round no client can represent. */
const MAX_ROSTER_SEATS = 32;
const isBotId = (id) => Number.isSafeInteger(Number(id)) && Number(id) >= BOT_ID_BASE && Number(id) < BOT_ID_BASE + 1000;
/* Any night whose name starts with "practice" is practice. The old rule
   wanted exactly "practice" or "practice:" and treated "practiceba" as a show
   night, which put official prize placings on the desk for a rehearsal. */
const isPracticeSeries = (series) => /^(?:practice|ops-rehearsal)/i.test(String(series || '').trim());
const isPracticeRound = (r) => !!(r && (r.solo || r.kind === 'rehearsal' || isPracticeSeries(r.series)));
const botIdForSeat = (seat) => BOT_ID_BASE + Number(seat);
/* How long a drawn Hot Market gets to become priceable before the backup is
   used. Long enough to ride out an oracle pause, short enough that the
   segment still starts on time. */
/* How long to keep trying the drawn market before taking the backup.
 *
 * Measured, not guessed: a cold engine answers HTTP in about 0.5s but takes
 * about 4.4s before all four majors are competition-ready, because the
 * composite has to refill from the venue streams. A 3s grace was therefore
 * shorter than a restart, so a restart landing on the Hot boundary could not
 * open Hot at all and blew the round. 15s clears that with margin and is still
 * a small fraction of a 4 minute window. The retry is additionally bounded by
 * the window itself, so this can never eat a segment. */
const HOT_OPEN_GRACE_MS = Number(process.env.PAPER_HOT_OPEN_GRACE_MS || 15_000);
/* Reliability thresholds for a market that will host a scoring window. Set
   from live measurement: the four majors run 98-100% ready with worst-case
   gaps of about a second, while everything thinner sits between 37% and 80%
   with gaps up to 40s. 95% with a 5s worst gap admits the former and excludes
   the latter, and the gap ceiling is deliberately just above the open grace,
   so a market can only qualify if its outages are ones the grace can absorb. */
const MIN_READY_RATIO = Number(process.env.PAPER_MIN_READY_RATIO || 0.95);
const MAX_READY_GAP_MS = Number(process.env.PAPER_MAX_READY_GAP_MS || 5000);
/* Below this much observed history we have no opinion and say so, rather than
   failing a round because the engine restarted two minutes ago. */
const MIN_RELIABILITY_SPAN_MS = Number(process.env.PAPER_MIN_RELIABILITY_SPAN_MS || 5 * 60_000);
/* The published floor for a Boost segment. Below this the window is not the
   one that was announced, so the boundary fails and an operator rules on it
   rather than the show quietly running a reduced game. */
const BOOST_MIN_OPEN = Number(process.env.PAPER_BOOST_MIN_OPEN || 2);

db.exec(`
  CREATE TABLE IF NOT EXISTS paper_rounds (
    id             TEXT PRIMARY KEY,
    kind           TEXT    NOT NULL CHECK (kind IN ('round','final','rehearsal')),
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
  /* WHAT THE ROOM IS LOOKING AT.
   *
   * A broadcast is mostly NOT a live round: there is the wait before round
   * one, the gap between rounds while the host talks, the moment something
   * breaks and the screen has to be covered, and the result at the end. Those
   * are show states, not engine states, and without somewhere to put them the
   * wall's only two moods were "a round is running" and "nothing is running",
   * which is what an audience sees for most of the evening.
   *
   * One row, on the server, because a wall driven from a browser tab loses
   * its stage on every refresh, and the operator is not going to notice the
   * screen behind them reverted while they were talking. */
  CREATE TABLE IF NOT EXISTS paper_wall (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    mode       TEXT    NOT NULL DEFAULT 'auto',
    message    TEXT,
    /* Set only when the operator armed a countdown by hand. A scheduled round
       start wins over it, so the clock on the wall and the clock that will
       actually ring are never two different numbers. */
    next_at    INTEGER,
    next_label TEXT,
    series     TEXT,
    /* How many rounds the night has, so the wall can say "3 of 5" rather than
       counting what has happened and implying the show is over. */
    series_total INTEGER,
    updated_at INTEGER NOT NULL
  );
  INSERT OR IGNORE INTO paper_wall (id, mode, updated_at) VALUES (1, 'auto', 0);

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
/* A CHECK constraint cannot be altered in place, so a database created before
   the rehearsal kind existed would reject it. Rebuild that one table, in a
   transaction, only when the old constraint is actually present.

   This runs BEFORE the ADD COLUMN pass, and copies the intersection of the two
   tables' real columns rather than a hand-written list. Both matter: the list
   used to be hardcoded and ran after the ALTERs, so it silently DROPPED
   gate_outage on exactly the production upgrade path, and every future column
   would have hit the same trap. Rebuilding first means the ALTERs below always
   get the last word. */
try {
  const ddl = db.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name='paper_rounds'").get();
  if (ddl && ddl.sql && !ddl.sql.includes("'rehearsal'")) {
    db.transaction(() => {
      db.exec('ALTER TABLE paper_rounds RENAME TO paper_rounds_old');
      db.exec(`CREATE TABLE paper_rounds (
        id TEXT PRIMARY KEY,
        kind TEXT NOT NULL CHECK (kind IN ('round','final','rehearsal')),
        status TEXT NOT NULL CHECK (status IN ('armed','running','done','aborted')),
        started_at INTEGER, ends_at INTEGER,
        hot_candidates TEXT NOT NULL, hot_backup TEXT, hot_base TEXT,
        draw_commit TEXT NOT NULL, draw_seed TEXT, draw_at INTEGER,
        created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
        blocked_reason TEXT, active_hot_base TEXT, boost_markets TEXT, fallback_reason TEXT)`);
      const oldCols = db.prepare('PRAGMA table_info(paper_rounds_old)').all().map((x) => x.name);
      const newCols = db.prepare('PRAGMA table_info(paper_rounds)').all().map((x) => x.name);
      const shared = newCols.filter((x) => oldCols.includes(x));
      db.exec(`INSERT INTO paper_rounds (${shared.join(',')}) SELECT ${shared.join(',')} FROM paper_rounds_old`);
      /* Anything the old table had that the new shape does not is a column
         added by a later migration; it is re-added by the ALTER pass below,
         so carry its data across rather than losing it. */
      for (const extra of oldCols.filter((x) => !newCols.includes(x))) {
        try {
          db.exec(`ALTER TABLE paper_rounds ADD COLUMN ${extra} TEXT`);
          db.exec(`UPDATE paper_rounds SET ${extra} = (SELECT o.${extra} FROM paper_rounds_old o WHERE o.id = paper_rounds.id)`);
        } catch { /* not carryable, and the ALTER pass will re-create it empty */ }
      }
      db.exec('DROP TABLE paper_rounds_old');
    })();
  }
} catch (e) { /* a fresh database already has the current shape */ }

/* Seats exist BEFORE anyone occupies them. The operator names a seat, the
   engine mints an invite for it, and the player who opens that invite binds
   their account to it. That inverts the old assumption: the table was keyed on
   (round_id, user_id), so a seat could not exist without already knowing who
   sat in it, which is exactly what forced the operator to type user ids they
   had no way of knowing. Rekeyed on (round_id, seat), with user_id nullable
   and unique-per-round when set. */
try {
  const ddl = db.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name='paper_round_players'").get();
  if (ddl && ddl.sql && !ddl.sql.includes('PRIMARY KEY (round_id, seat)')) {
    db.transaction(() => {
      db.exec('ALTER TABLE paper_round_players RENAME TO paper_round_players_old');
      db.exec(`CREATE TABLE paper_round_players (
        round_id     TEXT    NOT NULL,
        user_id      INTEGER,
        display_name TEXT,
        seat         INTEGER NOT NULL,
        epoch        INTEGER,
        PRIMARY KEY (round_id, seat)
      )`);
      const oldCols = db.prepare('PRAGMA table_info(paper_round_players_old)').all().map((x) => x.name);
      const newCols = db.prepare('PRAGMA table_info(paper_round_players)').all().map((x) => x.name);
      const shared = newCols.filter((x) => oldCols.includes(x));
      db.exec(`INSERT INTO paper_round_players (${shared.join(',')}) SELECT ${shared.join(',')} FROM paper_round_players_old`);
      for (const extra of oldCols.filter((x) => !newCols.includes(x))) {
        try {
          db.exec(`ALTER TABLE paper_round_players ADD COLUMN ${extra} ${extra === 'avatar_url' ? 'TEXT' : 'REAL'}`);
          db.exec(`UPDATE paper_round_players SET ${extra} = (SELECT o.${extra} FROM paper_round_players_old o
                     WHERE o.round_id = paper_round_players.round_id AND o.seat = paper_round_players.seat)`);
        } catch { /* re-created empty by the ALTER pass below */ }
      }
      db.exec('DROP TABLE paper_round_players_old');
    })();
  }
} catch (e) { /* a fresh database already has the current shape */ }

/* Two running rounds is unreachable through the API; this makes it
   unreachable through DB surgery too. OUTSIDE the alter loop on purpose: that
   loop swallows every error as "already applied", so on a dirty legacy DB the
   guard silently failed to exist on every boot and nothing ever said so. A
   dirty DB still boots (an engine that refuses to start is worse than a
   missing belt), but it now shouts on every start until someone fixes it. */
try {
  db.exec("CREATE UNIQUE INDEX IF NOT EXISTS ix_one_running ON paper_rounds(status) WHERE status = 'running'");
} catch (e) {
  console.error('[competition] ix_one_running could not be created, the one-running-round guard is ABSENT (two running rows in the DB?):', e.message);
}

for (const alter of [
  /* Format v2 is the two-surprise-Hot format. The complete draw is persisted
     before the bell for restart safety, but is never serialized while the
     round is live. Existing rows stay v1 so their historical proof remains
     interpretable under the rules they actually ran. */
  'ALTER TABLE paper_rounds ADD COLUMN format_version INTEGER NOT NULL DEFAULT 1',
  // NULL preserves the calculation contract of already armed/historical rows.
  'ALTER TABLE paper_rounds ADD COLUMN score_precision INTEGER',
  // NULL is the historical strict-price contract. Never migrate old rounds.
  'ALTER TABLE paper_rounds ADD COLUMN price_policy TEXT',
  // NULL retains the original minute-27 ceiling for every existing round.
  'ALTER TABLE paper_rounds ADD COLUMN boost_capacity_policy TEXT',
  // NULL preserves every existing round's original source-qualification rule.
  'ALTER TABLE paper_rounds ADD COLUMN backup_execution_policy TEXT',
  'ALTER TABLE paper_rounds ADD COLUMN hot_draw_secret TEXT',
  'ALTER TABLE paper_rounds ADD COLUMN hot_draw_json TEXT',
  'ALTER TABLE paper_rounds ADD COLUMN draw_reveal_json TEXT',
  'ALTER TABLE paper_rounds ADD COLUMN hot1_base TEXT',
  'ALTER TABLE paper_rounds ADD COLUMN hot1_active_base TEXT',
  'ALTER TABLE paper_rounds ADD COLUMN hot1_fallback_reason TEXT',
  'ALTER TABLE paper_rounds ADD COLUMN hot1_revealed_at INTEGER',
  'ALTER TABLE paper_rounds ADD COLUMN hot2_base TEXT',
  'ALTER TABLE paper_rounds ADD COLUMN hot2_active_base TEXT',
  'ALTER TABLE paper_rounds ADD COLUMN hot2_fallback_reason TEXT',
  'ALTER TABLE paper_rounds ADD COLUMN hot2_revealed_at INTEGER',
  /* Boost-start collateral is snapshotted once for audit. Legacy rounds also
     use this as their ceiling; current-equity rounds retain the same proof
     without restricting subsequent profits to this reference amount. */
  'ALTER TABLE paper_round_players ADD COLUMN boost_bankroll REAL',
  'ALTER TABLE paper_round_players ADD COLUMN boost_max_exposure REAL',
  'ALTER TABLE paper_round_players ADD COLUMN boost_frozen_at INTEGER',
  /* PAUSE-AND-EXTEND. The round clock measures ACTIVE time, not wall time.
     When price is unavailable the phase clock stops, so every phase gets its
     full tradeable duration; a 178s outage inside a 180s Boost window used to
     settle as a normal completed result, handing someone a title decided by
     two seconds of tradeable market. paused_ms is the total already banked,
     paused_since is the open interval (null when running). */
  /* The wall instant the active clock FIRST hit this boundary. A retried
     boundary (blocked, recovered later) used to recompute its settle instant
     with pause time banked AFTER the true instant, pricing a scored segment
     minutes late. Captured once, reused on every retry. */
  'ALTER TABLE paper_round_boundaries ADD COLUMN due_wall_at INTEGER',
  'ALTER TABLE paper_rounds ADD COLUMN paused_ms INTEGER NOT NULL DEFAULT 0',
  'ALTER TABLE paper_rounds ADD COLUMN paused_since INTEGER',
  /* A dead engine is not valid trading time. The current process refreshes a
     small durable lease while live; its successor banks the gap since the
     last refresh as paused time before it examines or replays boundaries. */
  'ALTER TABLE paper_rounds ADD COLUMN clock_heartbeat_at INTEGER',
  'ALTER TABLE paper_rounds ADD COLUMN clock_owner_id TEXT',
  'ALTER TABLE paper_round_players ADD COLUMN epoch INTEGER',
  /* The invite. Unguessable, one per seat, minted when the round is armed.
     Whoever opens it and is signed in takes that seat. */
  'ALTER TABLE paper_round_players ADD COLUMN invite_token TEXT',
  'ALTER TABLE paper_round_players ADD COLUMN claimed_at INTEGER',
  /* Readiness is an INDICATOR, never a gate: the operator can start a round
     with players who have not pressed it. It exists so the desk can tell the
     difference between "nobody has turned up" and "everyone is at their
     desk", which is the thing a producer actually needs to see. */
  'ALTER TABLE paper_round_players ADD COLUMN ready_at INTEGER',
  /* The Boost leverage this round was ARMED under. It was derived from
     whatever code happened to be deployed, so a restart or a config change
     could alter the rules of a round already on air. A round's rules belong to
     the round. */
  'ALTER TABLE paper_rounds ADD COLUMN boost_leverage INTEGER',
  /* The PHASE PLAN this round was armed under. Boundaries are identified by
     their offset (PK round_id+at), and schedule/resume re-derive offsets from
     the ROUND_PLAN constants: a deploy that edits those constants mid-round
     makes a restarted round replay boundaries at instants nobody traded to.
     Same bug class boost_leverage was persisted for. */
  'ALTER TABLE paper_rounds ADD COLUMN plan_json TEXT',
  /* Public record of any operator settle deviation (settleAtPrior). One round
     may carry several lines; published on the wall and in verify, because a
     deviation nobody can see is indistinguishable from tampering. */
  'ALTER TABLE paper_rounds ADD COLUMN settle_deviation TEXT',

  /* A scheduled start. The countdown belongs to the ENGINE, not to whichever
     browser tab pressed the button: an operator closing the desk, or a laptop
     sleeping, must not be able to cancel a start eight people are waiting
     for. Persisted, so a restart re-arms the same instant. */
  'ALTER TABLE paper_rounds ADD COLUMN start_at INTEGER',
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
  /* A countdown the operator has HELD. The wall shows this number frozen; the
     clock resumes from it rather than from wherever the wall clock would have
     drifted to. */
  'ALTER TABLE paper_wall ADD COLUMN paused_ms INTEGER',
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
  /* Frozen with the checkpoint. Reading the roster's live value meant a
     published First Five order could change later in the round, because a
     dip after the checkpoint rewrote the tie-break behind it. */
  'ALTER TABLE paper_round_scores ADD COLUMN max_drawdown REAL',
  'ALTER TABLE paper_rounds ADD COLUMN fallback_reason TEXT',
  /* Segment availability. A gate that never came back used to vanish from the
     record the moment the phase moved on, so a round could silently lose the
     tail of Hot or Boost and still settle normally. */
  'ALTER TABLE paper_rounds ADD COLUMN gate_outage TEXT',
  /* Boost: what was PROMISED and what actually OPENED are different facts and
     need different columns. One column served both, and openBoost overwrote it
     with the opened subset, so a market that was configured and then failed
     simply vanished from the record and the wall computed "unavailable"
     against an already-reduced set. */
  'ALTER TABLE paper_rounds ADD COLUMN boost_configured TEXT',
  'ALTER TABLE paper_rounds ADD COLUMN boost_opened TEXT',
  /* A practice round: every seat except the operator's is driven by the
     engine, so one person can rehearse the whole format alone. Recorded on
     the ROUND, because every guard that must never fire in a real show reads
     it, and a flag held anywhere else could drift from the round it governs. */
  'ALTER TABLE paper_rounds ADD COLUMN solo INTEGER DEFAULT 0',
  /* Which show this round belongs to. A broadcast is five rounds with one
     running standing, and without this every round is an island: there is no
     honest way to answer "who is winning the night" from a table of rounds
     that do not know they are related. */
  'ALTER TABLE paper_rounds ADD COLUMN series TEXT',
  /* WHERE THIS ROUND SITS IN THE BRACKET.
     `stage` is what the round is called on air ("Heat 1", "Semi-final").
     `advance` is how many of its players go through to the next one, which is
     the only number that turns a list of results into a bracket: without it
     nothing can say who is still in. Null on a standalone round, which then
     behaves exactly as it did before. */
  'ALTER TABLE paper_rounds ADD COLUMN stage TEXT',
  'ALTER TABLE paper_rounds ADD COLUMN advance INTEGER',
  'ALTER TABLE paper_rounds ADD COLUMN speed REAL DEFAULT 1',
  /* Player avatar for the broadcast wall. Resolved by the operator desk (from
     the player's linked account, or an explicit upload) and stored WITH THE
     ROUND rather than looked up live: a face that changes on X mid-show should
     not change the wall, and the round record should still explain itself
     months later. */
  'ALTER TABLE paper_round_players ADD COLUMN avatar_url TEXT',
]) {
  try { db.exec(alter); } catch { /* already applied */ }
}

db.exec(`
  CREATE TABLE IF NOT EXISTS paper_round_hot_scores (
    round_id    TEXT    NOT NULL,
    hot_no      INTEGER NOT NULL CHECK (hot_no IN (1, 2)),
    user_id     INTEGER NOT NULL,
    base        TEXT    NOT NULL,
    started_at  INTEGER NOT NULL,
    start_value REAL    NOT NULL,
    ended_at    INTEGER,
    end_value   REAL,
    bonus       REAL,
    PRIMARY KEY (round_id, hot_no, user_id)
  );
  CREATE TABLE IF NOT EXISTS paper_round_hot_resolutions (
    round_id       TEXT    NOT NULL,
    hot_no         INTEGER NOT NULL CHECK (hot_no IN (1, 2)),
    activation_at  INTEGER NOT NULL,
    drawn          TEXT    NOT NULL,
    active         TEXT    NOT NULL,
    evidence_json  TEXT    NOT NULL,
    created_at     INTEGER NOT NULL,
    PRIMARY KEY (round_id, hot_no)
  );
  CREATE TABLE IF NOT EXISTS paper_round_clock_pauses (
    id          INTEGER PRIMARY KEY,
    round_id    TEXT    NOT NULL,
    started_at  INTEGER NOT NULL,
    ended_at    INTEGER,
    source      TEXT    NOT NULL
  );
  CREATE UNIQUE INDEX IF NOT EXISTS prcp_one_open
    ON paper_round_clock_pauses(round_id) WHERE ended_at IS NULL;
  CREATE TABLE IF NOT EXISTS paper_round_boost_freezes (
    round_id     TEXT    NOT NULL,
    user_id      INTEGER NOT NULL,
    frozen_at    INTEGER NOT NULL,
    bankroll     REAL    NOT NULL,
    max_exposure REAL    NOT NULL,
    marks_json   TEXT    NOT NULL,
    PRIMARY KEY (round_id, user_id)
  );
  CREATE TABLE IF NOT EXISTS paper_round_boost_resolutions (
    round_id        TEXT PRIMARY KEY,
    activation_at   INTEGER NOT NULL,
    evidence_json   TEXT    NOT NULL,
    created_at      INTEGER NOT NULL
  );
  /* Immutable score inputs, captured beside each economic boundary. These
     are deliberately separate from the result rows: verification can
     recompute the published equity/Hot delta from the frozen account and
     position state, and can detect any later edit to the fill ledger through
     its canonical digest, instead of merely checking that asserted output
     columns add up. */
  CREATE TABLE IF NOT EXISTS paper_round_score_proofs (
    round_id       TEXT    NOT NULL,
    user_id        INTEGER NOT NULL,
    checkpoint     TEXT    NOT NULL,
    as_of          INTEGER NOT NULL,
    epoch          INTEGER NOT NULL,
    start_balance  REAL    NOT NULL,
    proof_json     TEXT    NOT NULL,
    proof_sha256   TEXT    NOT NULL,
    created_at     INTEGER NOT NULL,
    PRIMARY KEY (round_id, user_id, checkpoint)
  );
  /* Round marks are economic state, not a fresh-source cache. The latest
     record and bounded recent tick ring commit with the risk transaction.
     Minute bars and immutable boundary proofs outlive the recent tick ring. */
  CREATE TABLE IF NOT EXISTS paper_round_marks (
    round_id TEXT NOT NULL, base TEXT NOT NULL, revision INTEGER NOT NULL,
    first_applied_at REAL NOT NULL, applied_at REAL NOT NULL, record_json TEXT NOT NULL,
    PRIMARY KEY (round_id, base)
  );
  CREATE TABLE IF NOT EXISTS paper_round_mark_ticks (
    round_id TEXT NOT NULL, base TEXT NOT NULL, revision INTEGER NOT NULL,
    applied_at REAL NOT NULL, record_json TEXT NOT NULL,
    PRIMARY KEY (round_id, base, revision)
  );
  CREATE INDEX IF NOT EXISTS prmt_time
    ON paper_round_mark_ticks(round_id, base, applied_at, revision);
  CREATE TABLE IF NOT EXISTS paper_round_mark_breaks (
    id INTEGER PRIMARY KEY, round_id TEXT NOT NULL, base TEXT NOT NULL,
    started_at REAL NOT NULL, ended_at REAL, reason TEXT NOT NULL
  );
  CREATE UNIQUE INDEX IF NOT EXISTS prmb_open
    ON paper_round_mark_breaks(round_id, base) WHERE ended_at IS NULL;
  CREATE INDEX IF NOT EXISTS prmb_time
    ON paper_round_mark_breaks(round_id, base, started_at);
  CREATE TABLE IF NOT EXISTS paper_round_mark_minutes (
    round_id TEXT NOT NULL, base TEXT NOT NULL, time INTEGER NOT NULL,
    first_at REAL NOT NULL, last_at REAL NOT NULL,
    open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
    PRIMARY KEY (round_id, base, time)
  );
`);

db.exec(`CREATE TRIGGER IF NOT EXISTS paper_round_price_policy_immutable
  BEFORE UPDATE OF price_policy ON paper_rounds
  WHEN OLD.format_version >= 2 AND NEW.price_policy IS NOT OLD.price_policy
  BEGIN SELECT RAISE(ABORT, 'round price policy is immutable'); END`);

db.exec(`CREATE TRIGGER IF NOT EXISTS paper_round_boost_capacity_policy_immutable
  BEFORE UPDATE OF boost_capacity_policy ON paper_rounds
  WHEN OLD.format_version >= 2 AND NEW.boost_capacity_policy IS NOT OLD.boost_capacity_policy
  BEGIN SELECT RAISE(ABORT, 'round Boost capacity policy is immutable'); END`);

db.exec(`CREATE TRIGGER IF NOT EXISTS paper_round_backup_execution_policy_immutable
  BEFORE UPDATE OF backup_execution_policy ON paper_rounds
  WHEN OLD.format_version >= 2 AND NEW.backup_execution_policy IS NOT OLD.backup_execution_policy
  BEGIN SELECT RAISE(ABORT, 'round backup execution policy is immutable'); END`);

// Precision is a round contract too. A new row is stamped before its v2 draw
// is committed; neither an operator nor an accidental update may change it.
db.exec(`CREATE TRIGGER IF NOT EXISTS paper_round_score_precision_immutable
  BEFORE UPDATE OF score_precision ON paper_rounds
  WHEN OLD.format_version >= 2 AND NEW.score_precision IS NOT OLD.score_precision
  BEGIN SELECT RAISE(ABORT, 'round score precision is immutable'); END`);

/* Once a v2 draw exists, no SQL path may rewrite the material the published
   commitment binds. The engine only reveals it later in separate columns.
   A database owner can always replace binaries/triggers, but an ordinary
   operator action or accidental UPDATE cannot redraw a live competition. */
try {
  db.exec(`CREATE TRIGGER IF NOT EXISTS paper_round_draw_v2_immutable
    BEFORE UPDATE OF hot_draw_secret, hot_draw_json, draw_commit, hot_candidates,
                     hot_backup, plan_json, format_version ON paper_rounds
    WHEN OLD.format_version >= 2
      AND (NEW.hot_draw_secret IS NOT OLD.hot_draw_secret
        OR NEW.hot_draw_json IS NOT OLD.hot_draw_json
        OR NEW.draw_commit IS NOT OLD.draw_commit
        OR NEW.hot_candidates IS NOT OLD.hot_candidates
        OR NEW.hot_backup IS NOT OLD.hot_backup
        OR NEW.plan_json IS NOT OLD.plan_json
        OR NEW.format_version IS NOT OLD.format_version)
    BEGIN SELECT RAISE(ABORT, 'v2 Hot draw is immutable'); END`);
} catch (e) {
  console.error('[competition] draw immutability trigger unavailable:', e.message);
}
try {
  db.exec(`CREATE TRIGGER IF NOT EXISTS paper_hot_resolution_immutable_update
    BEFORE UPDATE ON paper_round_hot_resolutions
    BEGIN SELECT RAISE(ABORT, 'Hot activation resolution is immutable'); END;
    CREATE TRIGGER IF NOT EXISTS paper_hot_resolution_immutable_delete
    BEFORE DELETE ON paper_round_hot_resolutions
    BEGIN SELECT RAISE(ABORT, 'Hot activation resolution is immutable'); END;`);
} catch (e) {
  console.error('[competition] Hot resolution immutability trigger unavailable:', e.message);
}
try {
  db.exec(`CREATE TRIGGER IF NOT EXISTS paper_clock_pause_immutable_delete
    BEFORE DELETE ON paper_round_clock_pauses
    BEGIN SELECT RAISE(ABORT, 'competition clock pause history is immutable'); END;
    CREATE TRIGGER IF NOT EXISTS paper_clock_pause_closed_immutable
    BEFORE UPDATE ON paper_round_clock_pauses
    WHEN OLD.ended_at IS NOT NULL
      OR NEW.round_id IS NOT OLD.round_id
      OR NEW.source IS NOT OLD.source
      OR NEW.started_at > OLD.started_at
    BEGIN SELECT RAISE(ABORT, 'closed competition clock pause is immutable'); END;`);
} catch (e) {
  console.error('[competition] clock-pause immutability trigger unavailable:', e.message);
}
try {
  db.exec(`CREATE TRIGGER IF NOT EXISTS paper_boost_freeze_proof_immutable_update
    BEFORE UPDATE ON paper_round_boost_freezes
    BEGIN SELECT RAISE(ABORT, 'Boost freeze proof is immutable'); END;
    CREATE TRIGGER IF NOT EXISTS paper_boost_freeze_proof_immutable_delete
    BEFORE DELETE ON paper_round_boost_freezes
    BEGIN SELECT RAISE(ABORT, 'Boost freeze proof is immutable'); END;`);
} catch (e) {
  console.error('[competition] Boost-freeze proof immutability trigger unavailable:', e.message);
}
try {
  db.exec(`CREATE TRIGGER IF NOT EXISTS paper_boost_resolution_immutable_update
    BEFORE UPDATE ON paper_round_boost_resolutions
    BEGIN SELECT RAISE(ABORT, 'Boost activation resolution is immutable'); END;
    CREATE TRIGGER IF NOT EXISTS paper_boost_resolution_immutable_delete
    BEFORE DELETE ON paper_round_boost_resolutions
    BEGIN SELECT RAISE(ABORT, 'Boost activation resolution is immutable'); END;`);
} catch (e) {
  console.error('[competition] Boost-resolution immutability trigger unavailable:', e.message);
}
try {
  db.exec(`CREATE TRIGGER IF NOT EXISTS paper_score_proof_immutable_update
    BEFORE UPDATE ON paper_round_score_proofs
    BEGIN SELECT RAISE(ABORT, 'competition score proof is immutable'); END;
    CREATE TRIGGER IF NOT EXISTS paper_score_proof_immutable_delete
    BEFORE DELETE ON paper_round_score_proofs
    BEGIN SELECT RAISE(ABORT, 'competition score proof is immutable'); END;`);
} catch (e) {
  console.error('[competition] score-proof immutability trigger unavailable:', e.message);
}
try {
  db.exec(`CREATE TRIGGER IF NOT EXISTS paper_v2_final_score_immutable_update
    BEFORE UPDATE ON paper_round_scores
    WHEN OLD.checkpoint = 'final' AND EXISTS (
      SELECT 1 FROM paper_rounds r WHERE r.id = OLD.round_id
        AND r.format_version >= 2 AND r.status IN ('done','aborted'))
    BEGIN SELECT RAISE(ABORT, 'settled v2 final score is immutable'); END;
    CREATE TRIGGER IF NOT EXISTS paper_v2_final_score_immutable_delete
    BEFORE DELETE ON paper_round_scores
    WHEN OLD.checkpoint = 'final' AND EXISTS (
      SELECT 1 FROM paper_rounds r WHERE r.id = OLD.round_id
        AND r.format_version >= 2 AND r.status IN ('done','aborted'))
    BEGIN SELECT RAISE(ABORT, 'settled v2 final score is immutable'); END;
    CREATE TRIGGER IF NOT EXISTS paper_v2_hot_score_immutable_update
    BEFORE UPDATE ON paper_round_hot_scores
    WHEN EXISTS (SELECT 1 FROM paper_rounds r WHERE r.id = OLD.round_id
      AND r.format_version >= 2 AND r.status IN ('done','aborted'))
    BEGIN SELECT RAISE(ABORT, 'settled v2 Hot score is immutable'); END;
    CREATE TRIGGER IF NOT EXISTS paper_v2_hot_score_immutable_delete
    BEFORE DELETE ON paper_round_hot_scores
    WHEN EXISTS (SELECT 1 FROM paper_rounds r WHERE r.id = OLD.round_id
      AND r.format_version >= 2 AND r.status IN ('done','aborted'))
    BEGIN SELECT RAISE(ABORT, 'settled v2 Hot score is immutable'); END;`);
} catch (e) {
  console.error('[competition] settled-score immutability trigger unavailable:', e.message);
}

/* The old primary key gave "one seat per person per round" for free. Keyed on
   the seat now, so state it: a partial index, because unclaimed seats are all
   NULL and must not collide with each other. */
for (const idx of [
  'CREATE UNIQUE INDEX IF NOT EXISTS prp_round_user ON paper_round_players (round_id, user_id) WHERE user_id IS NOT NULL',
  'CREATE UNIQUE INDEX IF NOT EXISTS prp_invite ON paper_round_players (invite_token) WHERE invite_token IS NOT NULL',
]) {
  try { db.exec(idx); } catch { /* already applied */ }
}

const q = {
  ins: db.prepare(`INSERT INTO paper_rounds (id, kind, status, hot_candidates, hot_backup, draw_commit, created_at, updated_at)
                   VALUES (?, ?, 'armed', ?, ?, ?, ?, ?)`),
  start: db.prepare('UPDATE paper_rounds SET status = ?, started_at = ?, ends_at = ?, start_at = NULL, updated_at = ? WHERE id = ?'),
  setStatus: db.prepare('UPDATE paper_rounds SET status = ?, start_at = NULL, updated_at = ? WHERE id = ?'),
  setStartAt: db.prepare('UPDATE paper_rounds SET start_at = ?, updated_at = ? WHERE id = ?'),
  setDraw: db.prepare('UPDATE paper_rounds SET hot_base = ?, draw_seed = ?, draw_at = ?, updated_at = ? WHERE id = ?'),
  setActive: db.prepare('UPDATE paper_rounds SET active_hot_base = ?, fallback_reason = ?, updated_at = ? WHERE id = ?'),
  hotRows: db.prepare('SELECT * FROM paper_round_hot_scores WHERE round_id = ? AND hot_no = ? ORDER BY user_id'),
  hotRowsForUser: db.prepare('SELECT * FROM paper_round_hot_scores WHERE round_id = ? AND user_id = ? ORDER BY hot_no'),
  hotResolution: db.prepare('SELECT * FROM paper_round_hot_resolutions WHERE round_id = ? AND hot_no = ?'),
  hotResolutions: db.prepare('SELECT * FROM paper_round_hot_resolutions WHERE round_id = ? ORDER BY hot_no'),
  hotResolutionIns: db.prepare(`INSERT INTO paper_round_hot_resolutions
    (round_id, hot_no, activation_at, drawn, active, evidence_json, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)`),
  hotStartIns: db.prepare(`INSERT INTO paper_round_hot_scores
    (round_id, hot_no, user_id, base, started_at, start_value)
    VALUES (?, ?, ?, ?, ?, ?)`),
  hotEndUpd: db.prepare(`UPDATE paper_round_hot_scores
    SET ended_at = ?, end_value = ?, bonus = ?
    WHERE round_id = ? AND hot_no = ? AND user_id = ? AND ended_at IS NULL`),
  get: db.prepare('SELECT * FROM paper_rounds WHERE id = ?'),
  writeState: db.prepare('SELECT status, blocked_reason FROM paper_rounds WHERE id = ?'),
  running: db.prepare("SELECT * FROM paper_rounds WHERE status = 'running' ORDER BY started_at DESC LIMIT 1"),
  setPausedSince: db.prepare('UPDATE paper_rounds SET paused_since = ?, updated_at = ? WHERE id = ?'),
  setPausedMs:    db.prepare('UPDATE paper_rounds SET paused_ms = ?, paused_since = NULL, updated_at = ? WHERE id = ?'),
  setEndsAt:      db.prepare('UPDATE paper_rounds SET ends_at = ?, updated_at = ? WHERE id = ?'),
  heartbeat:      db.prepare('UPDATE paper_rounds SET clock_heartbeat_at = ?, clock_owner_id = ? WHERE id = ?'),
  claimClock:     db.prepare(`UPDATE paper_rounds
    SET paused_ms = COALESCE(paused_ms, 0) + ?, ends_at = ends_at + ?,
        clock_heartbeat_at = ?, clock_owner_id = ?, updated_at = ?
    WHERE id = ?`),
  clockPauseOpen: db.prepare(`INSERT OR IGNORE INTO paper_round_clock_pauses
    (round_id, started_at, ended_at, source) VALUES (?, ?, NULL, ?)`),
  clockPauseClosed: db.prepare(`INSERT INTO paper_round_clock_pauses
    (round_id, started_at, ended_at, source) VALUES (?, ?, ?, ?)`),
  clockPauseBackdate: db.prepare(`UPDATE paper_round_clock_pauses
    SET started_at = MIN(started_at, ?) WHERE round_id = ? AND ended_at IS NULL`),
  clockPauseClose: db.prepare(`UPDATE paper_round_clock_pauses
    SET ended_at = ? WHERE round_id = ? AND ended_at IS NULL`),
  clockPauses: db.prepare(`SELECT started_at, ended_at, source
    FROM paper_round_clock_pauses WHERE round_id = ? ORDER BY started_at, id`),
  clockLastClosed: db.prepare(`SELECT MAX(ended_at) AS ended_at
    FROM paper_round_clock_pauses WHERE round_id = ? AND ended_at IS NOT NULL`),
  playerIns: db.prepare('INSERT INTO paper_round_players (round_id, user_id, display_name, seat, invite_token) VALUES (?, ?, ?, ?, ?)'),
  bySeat: db.prepare('SELECT * FROM paper_round_players WHERE round_id = ? AND seat = ?'),
  byInvite: db.prepare('SELECT * FROM paper_round_players WHERE invite_token = ?'),
  claimSeat: db.prepare(`UPDATE paper_round_players SET user_id = ?, claimed_at = ?
                         WHERE round_id = ? AND seat = ? AND user_id IS NULL`),
  /* A takeover: the seat changes hands and the new holder has not confirmed
     anything yet, so the ready flag goes with the old one. */
  retakeSeat: db.prepare(`UPDATE paper_round_players SET user_id = ?, claimed_at = ?, ready_at = NULL
                          WHERE round_id = ? AND seat = ?`),
  setReady: db.prepare('UPDATE paper_round_players SET ready_at = ? WHERE round_id = ? AND seat = ?'),
  seatOfUser: db.prepare('SELECT * FROM paper_round_players WHERE round_id = ? AND user_id = ?'),
  dropSeat: db.prepare('DELETE FROM paper_round_players WHERE round_id = ? AND seat = ?'),
  players: db.prepare('SELECT * FROM paper_round_players WHERE round_id = ? ORDER BY seat'),
  bindEpoch: db.prepare('UPDATE paper_round_players SET epoch = ? WHERE round_id = ? AND user_id = ?'),
  playerAvatar: db.prepare('UPDATE paper_round_players SET avatar_url = ? WHERE round_id = ? AND user_id = ?'),
  ddUpd: db.prepare('UPDATE paper_round_players SET peak_equity = ?, max_drawdown = ? WHERE round_id = ? AND user_id = ?'),
  boostFreeze: db.prepare(`UPDATE paper_round_players
    SET boost_bankroll = ?, boost_max_exposure = ?, boost_frozen_at = ?
    WHERE round_id = ? AND user_id = ? AND boost_frozen_at IS NULL`),
  boostProofIns: db.prepare(`INSERT INTO paper_round_boost_freezes
    (round_id, user_id, frozen_at, bankroll, max_exposure, marks_json)
    VALUES (?, ?, ?, ?, ?, ?)`),
  boostProofs: db.prepare(`SELECT * FROM paper_round_boost_freezes
    WHERE round_id = ? ORDER BY user_id`),
  boostResolution: db.prepare(`SELECT * FROM paper_round_boost_resolutions WHERE round_id = ?`),
  boostResolutionIns: db.prepare(`INSERT INTO paper_round_boost_resolutions
    (round_id, activation_at, evidence_json, created_at) VALUES (?, ?, ?, ?)`),
  /* Any round that owns its players' accounts: armed means the operator has
     seated them and may reset them at any moment, running means the result is
     being decided. In both states the account belongs to the show. */
  liveForUser: db.prepare(`SELECT r.id FROM paper_round_players p
                           JOIN paper_rounds r ON r.id = p.round_id
                           WHERE p.user_id = ? AND r.status IN ('armed','running') LIMIT 1`),
  anyRunning: db.prepare("SELECT id FROM paper_rounds WHERE status = 'running' LIMIT 1"),
  bMark: db.prepare('INSERT INTO paper_round_boundaries (round_id, at, status, error, ran_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT(round_id, at) DO UPDATE SET status = excluded.status, error = excluded.error, ran_at = excluded.ran_at'),
  bDue: db.prepare('UPDATE paper_round_boundaries SET due_wall_at = ? WHERE round_id = ? AND at = ? AND due_wall_at IS NULL'),
  bClearDue: db.prepare('UPDATE paper_round_boundaries SET due_wall_at = NULL WHERE round_id = ? AND at = ?'),
  bGet: db.prepare('SELECT * FROM paper_round_boundaries WHERE round_id = ? AND at = ?'),
  bAll: db.prepare('SELECT * FROM paper_round_boundaries WHERE round_id = ? ORDER BY at'),
  setBlocked: db.prepare('UPDATE paper_rounds SET blocked_reason = ?, updated_at = ? WHERE id = ?'),
  setBoostOpened: db.prepare('UPDATE paper_rounds SET boost_opened = ?, updated_at = ? WHERE id = ?'),
  scoreInsStrict: db.prepare(`INSERT INTO paper_round_scores
                        (round_id, user_id, checkpoint, at, equity, account_pnl, realized, hot_bonus, score, marks, scheduled_at, max_drawdown)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`),
  scoreIns: db.prepare(`INSERT OR IGNORE INTO paper_round_scores
                        (round_id, user_id, checkpoint, at, equity, account_pnl, realized, hot_bonus, score)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`),
  scores: db.prepare(`SELECT s.*, p.display_name, p.seat FROM paper_round_scores s
                      JOIN paper_round_players p ON p.round_id = s.round_id AND p.user_id = s.user_id
                      WHERE s.round_id = ? AND s.checkpoint = ?`),
  checkpointCounts: db.prepare(`SELECT
    (SELECT COUNT(*) FROM paper_round_players WHERE round_id = ?) AS players,
    (SELECT COUNT(*) FROM paper_round_scores s
      JOIN paper_round_players p ON p.round_id = s.round_id AND p.user_id = s.user_id
      WHERE s.round_id = ? AND s.checkpoint = 'final') AS scores`),
  scoreProofIns: db.prepare(`INSERT INTO paper_round_score_proofs
    (round_id, user_id, checkpoint, as_of, epoch, start_balance,
     proof_json, proof_sha256, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`),
  scoreProofs: db.prepare(`SELECT * FROM paper_round_score_proofs
    WHERE round_id = ? ORDER BY checkpoint, user_id`),
  scoreProofAt: db.prepare(`SELECT * FROM paper_round_score_proofs
    WHERE round_id = ? AND user_id = ? AND checkpoint = ?`),
};

/* Injected by paper.js so this module never reaches into engine internals. */
/* UNWIRED IS AN ERROR, NOT A QUIET SUCCESS. These defaults were silent
   no-ops, and a caller that skipped init()/wire() ran whole rounds whose
   boundaries all read "succeeded" while settling nothing: a probe lost an
   afternoon to it, and a future entry point would corrupt a live result the
   same way. The hooks that decide money or gates now throw until wired;
   cosmetic ones stay quiet. */
const notWired = (name) => () => { throw new Error(`competition engine not wired: ${name} (call paper.init or comp.wire first)`); };
let hooks = {
  defaultPricePolicy: () => 'strict',
  // Only createRound consults this default. Persisted absent policies never
  // inherit it on start, replay, recovery or settlement.
  defaultBoostCapacityPolicy: () => 'current-equity-v1',
  defaultBackupExecutionPolicy: () => 'latest-available-500-v1',
  initializeRoundMarks: null,
  openAlias: notWired('openAlias'), closeAlias: notWired('closeAlias'), onPhase: () => {}, log: () => {},
  // paper.js supplies these: the only things here that touch account state
  scoreUser: notWired('scoreUser'),
  scoreProofFor: null,
  // must return { epoch, startBalance, stage } after stamping a uniform account
  prepareSeat: () => null,
  seatState: () => null,
  equityOf: () => Number.NaN,
  hotValueOf: notWired('hotValueOf'),
  segmentResidue: notWired('segmentResidue'),
  marketReady: notWired('marketReady'),
  marketReadyAt: null,
  marketAvailability: null,
  historicalAvailabilityAt: null,
  marketEvidenceAt: null,
  /* paper.js owns the source deadlines. Called before any active-clock
     advance so an expiry and a phase boundary at the same millisecond are
     ordered price-pause first, boundary second. */
  ensureClockHealth: null,
  pauseForSegment: () => {},
  pauseForRestart: null,
  pauseForBoundary: null,
  // canonical mark set for a roster's exposure, as of a timestamp
  markSetFor: notWired('markSetFor'),
};
function wire(h) { hooks = { ...hooks, ...h }; }

const ROUND_PRICE_POLICY = 'last-accepted-v1';
const CURRENT_EQUITY_BOOST_POLICY = 'current-equity-v1';
const FROZEN_BOOST_POLICY = 'frozen-start-v1';
const LATEST_BACKUP_POLICY = 'latest-available-500-v1';
function backupExecutionPolicyOf(round) {
  const value = round && round.backup_execution_policy;
  if (value == null) return null;
  if (value === LATEST_BACKUP_POLICY) return value;
  throw new Error('unsupported round backup execution policy');
}
function backupExecutionPolicyMatches(value, round) {
  try {
    return backupExecutionPolicyOf({ backup_execution_policy: value }) === backupExecutionPolicyOf(round);
  } catch { return false; }
}
function backupExecutionPolicyFields(round) {
  const policy = backupExecutionPolicyOf(round);
  return policy ? { backupExecutionPolicy: policy } : {};
}
function usesLatestBackupPricing(round, base) {
  return backupExecutionPolicyOf(round) === LATEST_BACKUP_POLICY
    && Number(round.format_version) >= 2 && pricePolicyOf(round) === ROUND_PRICE_POLICY
    && Number(round.boost_leverage) === 500
    && ['BTC', 'ETH', 'SOL', 'XRP'].includes(String(base).replace(/-(?:HOT|BOOST)$/, ''));
}
function roundMarkPolicyFields(row) {
  return Object.prototype.hasOwnProperty.call(row || {}, 'executionLeverageCap')
    ? { executionLeverageCap: row.executionLeverageCap, leveragePolicy: row.leveragePolicy,
      sourceAgeKnown: row.sourceAgeKnown } : {};
}
function roundMarkExecutionLeverage(round, row) {
  if (!row || !Number.isFinite(row.acceptedLeverageCap)) return 0;
  const keys = ['executionLeverageCap', 'leveragePolicy', 'sourceAgeKnown'];
  const present = keys.filter(key => Object.prototype.hasOwnProperty.call(row, key));
  let enabled;
  try { enabled = usesLatestBackupPricing(round, row.base); } catch { return 0; }
  if (!enabled) return present.length ? 0 : row.acceptedLeverageCap;
  // Intrinsic source qualification stays truthful. The separate, sealed rule
  // permits execution at500 only for these four competition families.
  return present.length === keys.length && row.executionLeverageCap === 500
    && row.leveragePolicy === LATEST_BACKUP_POLICY && typeof row.sourceAgeKnown === 'boolean'
    && row.acceptedLeverageCap >= COMP_BASE_LEV && row.acceptedLeverageCap <= 1000 ? 500 : 0;
}
function boostCapacityPolicyOf(round) {
  const value = round && round.boost_capacity_policy;
  if (value == null || value === FROZEN_BOOST_POLICY) return FROZEN_BOOST_POLICY;
  if (value === CURRENT_EQUITY_BOOST_POLICY) return value;
  throw new Error('unsupported round Boost capacity policy');
}
function boostCapacityPolicyMatches(value, round) {
  try {
    return boostCapacityPolicyOf({ boost_capacity_policy: value }) === boostCapacityPolicyOf(round);
  } catch { return false; }
}
const ROUND_MARK_TICK_LIMIT = 2048;
function pricePolicyOf(round) {
  return round && round.price_policy === ROUND_PRICE_POLICY ? ROUND_PRICE_POLICY : 'strict';
}
function requiredRoundMarkLeverage(round, base) {
  return ['BTC', 'ETH', 'SOL', 'XRP'].includes(String(base).replace(/-(?:HOT|BOOST)$/, ''))
    ? (Number(round && round.boost_leverage) || 500) : COMP_BASE_LEV;
}
const rmq = {
  latest: db.prepare('SELECT * FROM paper_round_marks WHERE round_id = ? AND base = ?'),
  past: db.prepare(`SELECT * FROM paper_round_mark_ticks WHERE round_id = ? AND base = ?
    AND applied_at <= ? ORDER BY applied_at DESC, revision DESC LIMIT 1`),
  failure: db.prepare(`SELECT started_at, reason FROM paper_round_mark_breaks
    WHERE round_id = ? AND base = ? AND started_at <= ?
      AND (ended_at IS NULL OR ended_at > ?) ORDER BY started_at DESC LIMIT 1`),
  put: db.prepare(`INSERT INTO paper_round_marks
    (round_id, base, revision, first_applied_at, applied_at, record_json) VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(round_id, base) DO UPDATE SET revision=excluded.revision,
      applied_at=excluded.applied_at, record_json=excluded.record_json`),
  tick: db.prepare(`INSERT INTO paper_round_mark_ticks
    (round_id, base, revision, applied_at, record_json) VALUES (?, ?, ?, ?, ?)`),
  minute: db.prepare(`INSERT INTO paper_round_mark_minutes
    (round_id, base, time, first_at, last_at, open, high, low, close)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(round_id, base, time) DO UPDATE SET last_at=excluded.last_at,
      high=MAX(high,excluded.high), low=MIN(low,excluded.low), close=excluded.close`),
  // A pending exact boundary retains its predecessor even beyond the ring.
  prune: db.prepare(`DELETE FROM paper_round_mark_ticks AS t
    WHERE round_id = ? AND base = ? AND revision <= ?
      AND NOT EXISTS (SELECT 1 FROM paper_round_boundaries AS b
        WHERE b.round_id=t.round_id AND b.status!='succeeded' AND b.due_wall_at IS NOT NULL
          AND t.revision=(SELECT p.revision FROM paper_round_mark_ticks AS p
            WHERE p.round_id=t.round_id AND p.base=t.base AND p.applied_at<=b.due_wall_at
            ORDER BY p.applied_at DESC,p.revision DESC LIMIT 1))`),
};
const ROUND_MARK_DECODE_LIMIT = 256;
const _roundMarkDecoded = new Map();
function decodeRoundMarkRecord(raw) {
  const cached = _roundMarkDecoded.get(raw);
  if (cached) return cached;
  const record = JSON.parse(raw);
  // Only parsing is reused. The exact SQL-selected bytes are the key, and
  // every authority, history and failure query still executes on every read.
  // A shallow copy is independent only for flat primitive records; unusual
  // nested/array or large records keep their previous fresh-decode behavior.
  if (typeof raw === 'string' && raw.length <= 4096 && record
      && typeof record === 'object' && !Array.isArray(record)
      && Object.values(record).every(value => value === null || typeof value !== 'object')) {
    if (_roundMarkDecoded.size >= ROUND_MARK_DECODE_LIMIT) {
      _roundMarkDecoded.delete(_roundMarkDecoded.keys().next().value);
    }
    Object.freeze(record);
    _roundMarkDecoded.set(raw, record);
  }
  return record;
}
function roundMarkGet(roundId, base, at = Date.now()) {
  if (!Number.isFinite(at)) return null;
  const latest = rmq.latest.get(roundId, base);
  if (!latest) return null;
  const row = latest.applied_at <= at ? latest : rmq.past.get(roundId, base, at);
  if (!row) return null;
  const cut = Math.floor(latest.revision / 128) * 128 - ROUND_MARK_TICK_LIMIT;
  if (row !== latest && row.revision <= cut) {
    // Old pinned points prove only their pending boundary, not the unknown
    // tick interval between that anchor and the retained contiguous ring.
    const pinned = db.prepare(`SELECT 1 FROM paper_round_boundaries
      WHERE round_id=? AND status!='succeeded' AND due_wall_at=? LIMIT 1`).get(roundId, at);
    if (!pinned) return null;
  }
  const record = decodeRoundMarkRecord(row.record_json);
  const failure = rmq.failure.get(roundId, base, at, at);
  return { ...record, hardInvalid: !!failure,
    hardFailure: failure ? { reason: failure.reason, at: failure.started_at } : null };
}
function roundMarkCommit(roundId, input, expectedIdentity) {
  if (!db.inTransaction) throw new Error('round mark requires an economic transaction');
  const round = q.get.get(roundId);
  if (!round || !['armed', 'running'].includes(round.status)
      || pricePolicyOf(round) !== ROUND_PRICE_POLICY) throw new Error('round mark scope is not live');
  const { base, price, acceptedBoot, acceptedSeq, acceptedAt, observedAt,
    source, originalValidUntil, acceptedLeverageCap, appliedAt } = input || {};
  if (!(typeof base === 'string' && /^[A-Z0-9]{1,24}$/.test(base)
      && (!hooks.indexedSymbol || hooks.indexedSymbol(base))
      && Number.isFinite(price) && price > 0
      && typeof acceptedBoot === 'string' && /^[a-zA-Z0-9_-]{1,64}$/.test(acceptedBoot)
      && Number.isSafeInteger(acceptedSeq) && acceptedSeq > 0
      && Number.isFinite(observedAt) && observedAt > 0
      && Number.isFinite(acceptedAt) && acceptedAt >= observedAt
      && Number.isFinite(appliedAt) && appliedAt >= acceptedAt && appliedAt <= Date.now()
      && Number.isFinite(originalValidUntil) && originalValidUntil > appliedAt
      && typeof source === 'string' && source.length > 0 && source.length <= 64
      && Number.isFinite(acceptedLeverageCap)
      && roundMarkExecutionLeverage(round, input) >= requiredRoundMarkLeverage(round, base))) {
    throw new Error('invalid qualified round mark');
  }
  const existing = rmq.latest.get(roundId, base);
  const prior = existing ? JSON.parse(existing.record_json) : null;
  if (prior ? !expectedIdentity || expectedIdentity.acceptedBoot !== prior.acceptedBoot
      || expectedIdentity.acceptedSeq !== prior.acceptedSeq : expectedIdentity !== null) {
    throw new Error('round mark lineage changed before commit');
  }
  if (prior && (appliedAt < prior.appliedAt
      || (acceptedBoot === prior.acceptedBoot && (acceptedSeq <= prior.acceptedSeq
        || acceptedAt < prior.acceptedAt || observedAt < prior.observedAt)))) {
    throw new Error('round mark lineage is not monotonic');
  }
  const record = { base, price, acceptedBoot, acceptedSeq, acceptedAt, observedAt,
    source, originalValidUntil, acceptedLeverageCap, appliedAt, ...roundMarkPolicyFields(input) };
  const raw = JSON.stringify(record), revision = (existing?.revision || 0) + 1;
  rmq.put.run(roundId, base, revision, existing?.first_applied_at ?? appliedAt, appliedAt, raw);
  rmq.tick.run(roundId, base, revision, appliedAt, raw);
  const minute = Math.floor(appliedAt / MIN) * MIN;
  rmq.minute.run(roundId, base, minute, appliedAt, appliedAt, price, price, price, price);
  db.prepare(`UPDATE paper_round_mark_breaks SET ended_at = ?
    WHERE round_id = ? AND base = ? AND ended_at IS NULL AND started_at <= ?`)
    .run(appliedAt, roundId, base, appliedAt);
  if (revision % 128 === 0) rmq.prune.run(roundId, base, revision - ROUND_MARK_TICK_LIMIT);
  return record;
}
function roundMarkInvalidate(roundId, base, reason, at = Date.now()) {
  const round = q.get.get(roundId);
  if (!round || !['armed', 'running'].includes(round.status)
      || pricePolicyOf(round) !== ROUND_PRICE_POLICY) return false;
  if (!(typeof base === 'string' && /^[A-Z0-9]{1,24}$/.test(base)
      && typeof reason === 'string' && reason.length > 0 && reason.length <= 160
      && Number.isFinite(at) && at > 0 && at <= Date.now())) throw new Error('invalid round mark failure');
  // An earlier recorded system failure may widen an open interval, never
  // silently erase one or mutate its accepted price's identity.
  db.prepare(`INSERT INTO paper_round_mark_breaks(round_id,base,started_at,reason)
    VALUES (?,?,?,?) ON CONFLICT(round_id,base) WHERE ended_at IS NULL
    DO UPDATE SET started_at=MIN(started_at,excluded.started_at)`)
    .run(roundId, base, at, reason);
  return true;
}
function roundMarksFor(roundId, at = Date.now()) {
  const rows = db.prepare('SELECT base FROM paper_round_marks WHERE round_id = ? ORDER BY base').all(roundId);
  return Object.fromEntries(rows.map(({ base }) => [base, roundMarkGet(roundId, base, at)]));
}
function roundMarkEvidenceFor(roundId, bases, at = Date.now()) {
  return Object.fromEntries([...new Set(bases)].sort().map((base) => [base, roundMarkGet(roundId, base, at)]));
}
function roundMarkBreaks(roundId, base, from, to) {
  const rows = db.prepare(`SELECT started_at AS 'from', ended_at AS 'to'
    FROM paper_round_mark_breaks WHERE round_id=? AND base=? AND started_at<=?
      AND (ended_at IS NULL OR ended_at>=?)
    UNION ALL SELECT started_at AS 'from', ended_at AS 'to'
    FROM paper_round_clock_pauses WHERE round_id=? AND started_at<=?
      AND (ended_at IS NULL OR ended_at>=?) ORDER BY 1`)
    .all(roundId, base, to, from, roundId, to, from);
  const out = [];
  for (const row of rows.sort((a, b) => a.from - b.from)) {
    const last = out[out.length - 1];
    if (last && (last.to == null || row.from <= last.to)) {
      last.to = last.to == null || row.to == null ? null : Math.max(last.to, row.to);
    } else out.push({ from: row.from, to: row.to });
  }
  return out;
}
function roundMarkHistory(roundId, base, { from = 0, to = Date.now(), limit = ROUND_MARK_TICK_LIMIT } = {}) {
  if (!(Number.isFinite(from) && from >= 0 && Number.isFinite(to) && to >= from
      && Number.isSafeInteger(limit) && limit > 0 && limit <= ROUND_MARK_TICK_LIMIT)) throw new Error('invalid round history range');
  const latest = rmq.latest.get(roundId, base);
  const rows = db.prepare(`SELECT * FROM paper_round_mark_ticks WHERE round_id=? AND base=?
    AND applied_at>=? AND applied_at<=? ORDER BY applied_at DESC,revision DESC LIMIT ?`)
    .all(roundId, base, from, to, limit).reverse();
  const edge = rows.length === limit ? rows[0].applied_at : from;
  const predecessor = rmq.past.get(roundId, base, edge);
  if (predecessor && (!rows.length || predecessor.revision < rows[0].revision)) rows.unshift(predecessor);
  // A held latest mark remains a valid predecessor after archived tick
  // retention; it never supplies an earlier unknown portion of the round.
  if (!rows.length && latest && latest.applied_at <= to) rows.push(latest);
  // A pinned old boundary predecessor is not contiguous chart history.
  let tailStart = 0;
  for (let i = 1; i < rows.length; i++) {
    if (rows[i].revision !== rows[i - 1].revision + 1) tailStart = i;
  }
  if (tailStart) rows.splice(0, tailStart);
  const first = rows[0];
  const complete = !!first && first.applied_at <= from && rows.length <= limit + 1;
  return { records: rows.map((row) => JSON.parse(row.record_json)),
    breaks: roundMarkBreaks(roundId, base, from, to),
    fromAvailable: first ? Math.max(from, first.applied_at) : to, complete };
}
function roundMarkCandles(roundId, base, { from = 0, to = Date.now(), tf = '1m' } = {}) {
  const step = { '1m': MIN, '5m': 5 * MIN, '15m': 15 * MIN,
    '1h': 60 * MIN, '4h': 240 * MIN, '1d': 1440 * MIN }[tf];
  if (!(step && Number.isFinite(from) && from >= 0 && Number.isFinite(to) && to >= from)) throw new Error('invalid round candle range');
  // HTTP callers and this storage layer both cap work. No request scans an
  // unbounded series or materialises per-tick history to build candles.
  const floor = Math.max(Math.floor(from / step) * step, Math.floor(to / step) * step - 999 * step);
  const round = q.get.get(roundId);
  const end = Math.min(to, ['done', 'aborted'].includes(round?.status) ? round.updated_at : to);
  const rangeStart = Math.max(floor, Math.floor((round?.created_at || to) / MIN) * MIN);
  if ((end - rangeStart) / MIN > 10080) throw new Error('round candle span exceeds seven days');
  const bars = db.prepare(`SELECT * FROM paper_round_mark_minutes WHERE round_id=? AND base=?
    AND time>=? AND time<=? ORDER BY time LIMIT 10081`).all(roundId, base, rangeStart, end);
  // An OHLC row updated after a historical cutoff cannot answer that partial
  // minute. Do not silently substitute the previous minute's held close.
  const partialBuckets = new Set(bars.filter((bar) => bar.last_at > to)
    .map((bar) => Math.floor(bar.time / step) * step));
  const previous = db.prepare(`SELECT * FROM paper_round_mark_minutes WHERE round_id=? AND base=?
    AND time<? ORDER BY time DESC LIMIT 1`).get(roundId, base, floor);
  const breaks = roundMarkBreaks(roundId, base, floor, to);
  const byTime = new Map(bars.map((row) => [row.time, row]));
  const buckets = new Map();
  let held = previous ? previous.close : null;
  const firstKnown = previous ? floor : (bars[0]?.first_at ?? to);
  // Only the actual round span can have marks. 1000 daily display buckets
  // must not expand into 1.44 million empty minute iterations.
  const start = Math.max(floor, Math.floor((round?.created_at || firstKnown) / MIN) * MIN);
  for (let time = start; time <= end; time += MIN) {
    const bar = byTime.get(time);
    // A partial minute touching a real outage cannot be represented as a
    // continuous OHLC bar. Exclude it and leave the explicit gap in metadata.
    const key = Math.floor(time / step) * step;
    const broken = breaks.some((gap) => gap.from < key + step && (gap.to == null || gap.to > key));
    const open = bar ? (held ?? bar.open) : held;
    if (bar) held = bar.close;
    if (broken || partialBuckets.has(key) || open == null) continue;
    const next = { time: key, open, high: bar ? Math.max(open, bar.high) : open,
      low: bar ? Math.min(open, bar.low) : open, close: held };
    const existing = buckets.get(key);
    if (existing) { existing.high = Math.max(existing.high, next.high);
      existing.low = Math.min(existing.low, next.low); existing.close = next.close; }
    else buckets.set(key, next);
  }
  return { rows: [...buckets.values()], breaks, fromAvailable: Math.max(floor, firstKnown),
    complete: !partialBuckets.size && floor <= from && firstKnown <= from };
}

/* Lifecycle callbacks are an internal integration boundary, not permission to
 * hand consumers the private persisted draw. V1 callbacks retain their
 * historical row shape; every v2 callback carries only public round facts. */
function phasePayload(r, extra = null) {
  if (!r) return extra || null;
  if (Number(r.format_version) < 2) return extra ? { ...r, ...extra } : r;
  return {
    id: r.id, kind: r.kind, status: r.status, formatVersion: 2, pricePolicy: pricePolicyOf(r),
    boostCapacityPolicy: boostCapacityPolicyOf(r),
    ...backupExecutionPolicyFields(r),
    startedAt: r.started_at || null, endsAt: r.ends_at || null,
    blockedReason: r.blocked_reason || null,
    drawCommit: r.status === 'running' || r.status === 'done' ? r.draw_commit : null,
    ...(extra || {}),
  };
}

// ── verifiable two-Hot draw ──────────────────────────────────────────────
/* V1 helpers remain exported solely so historical rows and their published
 * proofs keep verifying. New rounds use the v2 envelope below. */
const commitOf = (seed, candidates) =>
  crypto.createHash('sha256').update(seed + '|' + candidates.join(',')).digest('hex');
const drawIndex = (seed, n) =>
  Number(BigInt('0x' + crypto.createHash('sha256').update(seed).digest('hex')) % BigInt(n));

const sha256 = (s) => crypto.createHash('sha256').update(String(s)).digest('hex');
const drawInt = (seed, label, lo, hi) => {
  const a = Math.ceil(Number(lo)), b = Math.floor(Number(hi));
  if (!(Number.isSafeInteger(a) && Number.isSafeInteger(b) && b >= a)) {
    throw new Error(`invalid draw range ${label}: ${lo}..${hi}`);
  }
  const span = BigInt(b - a + 1);
  return a + Number(BigInt('0x' + sha256(`${seed}|${label}`)) % span);
};
function shuffled(seed, values, label) {
  const out = values.slice();
  for (let i = out.length - 1; i > 0; i--) {
    const j = drawInt(seed, `${label}:${i}`, 0, i);
    [out[i], out[j]] = [out[j], out[i]];
  }
  return out;
}
function revealEnvelope({ id, kind, speed, candidates, seed, plan, draw, pricePolicy, boostCapacityPolicy, backupExecutionPolicy }) {
  const capacityPolicy = boostCapacityPolicyOf({ boost_capacity_policy: boostCapacityPolicy });
  return {
    version: 2, roundId: id, kind, speed: Number(speed) || 1,
    plan: { ...plan }, candidates: candidates.slice(), seed, draw,
    // Optional only for the new contract: old commitments stay byte-exact.
    ...(pricePolicy === ROUND_PRICE_POLICY ? { pricePolicy } : {}),
    ...(capacityPolicy === CURRENT_EQUITY_BOOST_POLICY ? { boostCapacityPolicy: capacityPolicy } : {}),
    ...backupExecutionPolicyFields({ backup_execution_policy: backupExecutionPolicy }),
  };
}
function makeHotDraw({ candidates, seed, plan }) {
  const order = shuffled(seed, candidates, 'asset-order');
  const hot1At = drawInt(seed, 'hot-1-at', plan.hot1WindowStart, plan.hot1WindowEnd);
  /* Warning #2 starts only after a complete normal-trading minute following
     Hot #1. Expressing the constraint against activation includes its 15s
     hidden warning and remains correct under accelerated rehearsal plans. */
  const hot2Min = Math.max(plan.hot2WindowStart,
    hot1At + plan.hotDuration + plan.normalGap + plan.hotWarning);
  const hot2At = drawInt(seed, 'hot-2-at', hot2Min, plan.hot2WindowEnd);
  const chosen = order.slice(0, 2);
  const backupOrder = shuffled(seed, order.slice(2), 'backup-order');
  return {
    version: 2,
    /* Commit the complete per-window resolution order, not just the two
       primaries. This makes fallback independently auditable without asking
       a verifier to know which version of the selection algorithm ran. The
       other primary is deliberately last: ordinary backup markets are tried
       first, while an unused primary remains a deterministic liveness option
       (notably with a three-market pool). */
    hot1: { activation: hot1At, asset: chosen[0], fallbackOrder: [...backupOrder, chosen[1]] },
    hot2: { activation: hot2At, asset: chosen[1], fallbackOrder: [...backupOrder, chosen[0]] },
    backupOrder,
  };
}
function privateDrawOf(r) {
  if (!r || Number(r.format_version) < 2 || !r.hot_draw_json) return null;
  try {
    const d = JSON.parse(r.hot_draw_json);
    return d && d.version === 2 ? d : null;
  } catch { return null; }
}
function v2Commit(input) {
  return sha256(JSON.stringify(revealEnvelope(input)));
}
function sealedDrawValid(r) {
  if (!r || Number(r.format_version) < 2 || !r.hot_draw_secret) return false;
  if (!boostCapacityPolicyMatches(r.boost_capacity_policy, r)) return false;
  if (!backupExecutionPolicyMatches(r.backup_execution_policy, r)) return false;
  if (backupExecutionPolicyOf(r) && !usesLatestBackupPricing(r, 'BTC')) return false;
  let candidates;
  try { candidates = JSON.parse(r.hot_candidates || '[]'); } catch { return false; }
  const input = { id: r.id, kind: r.kind, speed: Number(r.speed) || 1,
    candidates, seed: r.hot_draw_secret, plan: planOf(r), draw: privateDrawOf(r), pricePolicy: pricePolicyOf(r),
    boostCapacityPolicy: boostCapacityPolicyOf(r), ...backupExecutionPolicyFields(r) };
  if (v2Commit(input) !== r.draw_commit) return false;
  const expected = makeHotDraw(input);
  return JSON.stringify(expected) === JSON.stringify(input.draw);
}

/** Recompute a FINISHED draw from its public reveal. Live rows intentionally
 *  return unrevealed even inside the server so callers cannot accidentally
 *  turn this helper into an oracle. */
function verifyDraw(r) {
  if (Number(r && r.format_version) >= 2) {
    const publishable = !!(r && (r.status === 'done'
      || (r.status === 'aborted' && r.started_at)));
    if (!publishable || !r.draw_reveal_json) {
      return { ok: false, reason: 'draw is sealed until settlement' };
    }
    let reveal;
    try { reveal = JSON.parse(r.draw_reveal_json); }
    catch { return { ok: false, reason: 'draw reveal is unreadable' }; }
    if (!reveal || !reveal.draw || !reveal.plan) return { ok: false, reason: 'draw reveal is incomplete' };
    const input = { id: reveal.roundId, kind: reveal.kind, speed: reveal.speed,
      candidates: reveal.candidates, seed: reveal.seed, plan: reveal.plan, draw: reveal.draw,
      pricePolicy: reveal.pricePolicy, boostCapacityPolicy: reveal.boostCapacityPolicy,
      backupExecutionPolicy: reveal.backupExecutionPolicy };
    if (!boostCapacityPolicyMatches(reveal.boostCapacityPolicy, r)) {
      return { ok: false, reason: 'revealed Boost capacity policy differs from the armed round' };
    }
    if ((reveal.pricePolicy || 'strict') !== pricePolicyOf(r)) {
      return { ok: false, reason: 'revealed price policy differs from the armed round' };
    }
    if (!backupExecutionPolicyMatches(reveal.backupExecutionPolicy, r)) {
      return { ok: false, reason: 'revealed backup execution policy differs from the armed round' };
    }
    if (v2Commit(input) !== r.draw_commit) return { ok: false, reason: 'commitment does not match reveal' };
    const expected = makeHotDraw(input);
    if (JSON.stringify(expected) !== JSON.stringify(reveal.draw)) {
      return { ok: false, reason: 'revealed draw does not follow the committed seed' };
    }
    return { ok: true, draw: expected };
  }
  const cands = JSON.parse(r.hot_candidates);
  if (!r.draw_seed) return { ok: false, reason: 'not drawn yet' };
  if (commitOf(r.draw_seed, cands) !== r.draw_commit) return { ok: false, reason: 'commitment does not match seed' };
  const expected = cands[drawIndex(r.draw_seed, cands.length)];
  if (expected !== r.hot_base) return { ok: false, reason: `seed selects ${expected}, round recorded ${r.hot_base}` };
  const fellBack = !!r.active_hot_base && r.active_hot_base !== r.hot_base;
  return {
    ok: true, market: expected, traded: r.active_hot_base || r.hot_base,
    ...(fellBack ? { fellBack: true, fallbackReason: r.fallback_reason || 'unspecified' } : {}),
  };
}

// ── phase arithmetic (pure) ──────────────────────────────────────────────
/** Phase at `elapsed` active ms. For v2, `endsAt` is the next private
 * boundary and MUST NOT be serialized while phase=build. */
function phaseAt(r, elapsed) {
  const p = planOf(r);
  if (!p) throw new Error('unknown round kind: ' + (typeof r === 'string' ? r : r && r.kind));
  if (elapsed < 0) return { phase: 'pre', endsAt: 0, hotNumber: null };
  if (elapsed >= p.total) return { phase: 'done', endsAt: p.total, hotNumber: null };
  const d = privateDrawOf(r);
  if (d) {
    for (const n of [1, 2]) {
      const h = d[`hot${n}`];
      const warningAt = h.activation - p.hotWarning;
      const endAt = h.activation + p.hotDuration;
      if (elapsed < warningAt) return { phase: 'build', endsAt: warningAt, hotNumber: null };
      if (elapsed < h.activation) return { phase: 'hotWarning', endsAt: h.activation, hotNumber: n };
      if (elapsed < endAt) return { phase: 'hot', endsAt: endAt, hotNumber: n };
    }
    if (elapsed < p.finalBuildStart) return { phase: 'build', endsAt: p.finalBuildStart, hotNumber: null };
    if (elapsed < p.boostStart) return { phase: 'finalBuild', endsAt: p.boostStart, hotNumber: null };
    return { phase: 'boost', endsAt: p.total, hotNumber: null };
  }
  /* Historical v1 rows keep their original arithmetic. */
  if (elapsed < p.firstFive) return { phase: 'firstFive', endsAt: p.firstFive, hotNumber: null };
  if (elapsed < p.reveal) return { phase: 'open', endsAt: p.reveal, hotNumber: null };
  if (elapsed < p.hotStart) return { phase: 'reveal', endsAt: p.hotStart, hotNumber: 1 };
  if (elapsed < p.hotEnd) return { phase: 'hot', endsAt: p.hotEnd, hotNumber: 1 };
  if (elapsed < p.boostStart) return { phase: 'open', endsAt: p.boostStart, hotNumber: null };
  return { phase: 'boost', endsAt: p.total, hotNumber: null };
}

/* Arithmetic can reach a boundary before its side effect commits. That gap is
 * normally only a few microtasks, but a price pause deliberately holds it for
 * as long as recovery takes. Public/runtime phase must describe the state the
 * engine has actually opened: never `hot` before the asset was revealed,
 * never `boost` before bankrolls/gates froze, and never `done` before the
 * final snapshot committed. */
function phaseForState(r, elapsed) {
  const raw = phaseAt(r, elapsed);
  if (!r || Number(r.format_version) < 2 || r.status !== 'running') return raw;
  for (const at of boundariesOf(r)) {
    if (at > elapsed) break;
    const row = q.bGet.get(r.id, at);
    if (row && row.status === 'succeeded') continue;
    const b = v2BoundaryAt(r, at);
    if (!b) continue;
    if (b.kind === 'warning') return { phase: 'build', endsAt: at, hotNumber: null };
    if (b.kind === 'hotStart') return { phase: 'hotWarning', endsAt: at, hotNumber: b.hotNumber };
    if (b.kind === 'hotEnd') return { phase: 'hot', endsAt: at, hotNumber: b.hotNumber };
    if (b.kind === 'finalBuild') return { phase: 'build', endsAt: at, hotNumber: null };
    if (b.kind === 'boostStart') return { phase: 'finalBuild', endsAt: at, hotNumber: null };
    if (b.kind === 'bell') return { phase: 'boost', endsAt: at, hotNumber: null };
  }
  return raw;
}
const boundariesOf = (r) => {
  const p = planOf(r);
  const d = privateDrawOf(r);
  if (!d) return [p.firstFive, p.reveal, p.hotStart, p.hotEnd, p.boostStart, p.total];
  return [...new Set([
    d.hot1.activation - p.hotWarning, d.hot1.activation,
    d.hot1.activation + p.hotDuration,
    d.hot2.activation - p.hotWarning, d.hot2.activation,
    d.hot2.activation + p.hotDuration,
    p.finalBuildStart, p.boostStart, p.total,
  ])].sort((a, b) => a - b);
};

// ── round lifecycle ──────────────────────────────────────────────────────
/* An avatar URL goes straight onto a public broadcast wall, so it is checked
 * rather than trusted. Only https, or a site-relative path, is accepted:
 * `javascript:` and `data:` are refused outright, the first because it is an
 * injection and the second because a megabyte of base64 per seat has no
 * business in the round record. */
function cleanAvatarUrl(v) {
  if (v == null || v === '') return null;
  const s = String(v).trim();
  if (s.length > 500) throw new Error('avatar url too long');
  /* A protocol-relative URL is refused rather than quietly dropped: it reads
     like a local path but resolves to a third-party host, and an operator who
     pasted one should be told, not handed a blank seat. */
  if (s.startsWith('//')) throw new Error('avatar url must be https or site-relative, got protocol-relative');
  if (s.startsWith('/')) return s;
  let u;
  try { u = new URL(s); } catch { throw new Error('avatar url is not a url: ' + s.slice(0, 40)); }
  if (u.protocol !== 'https:') throw new Error('avatar url must be https or site-relative');
  return u.toString();
}

function createRound({ id, kind = 'round', candidates, backup = null, players = [], solo = false, series = null, stage = null, advance = null, botRoster = [], speed = 1, seats: seatCount = null }) {
  if (!id) throw new Error('round id required');
  if (!ROUND_PLAN[kind]) throw new Error('unknown round kind: ' + kind);
  if (!SPEEDS.includes(Number(speed))) throw new Error('speed must be one of ' + SPEEDS.join(', '));
  if ((kind === 'round' || kind === 'final') && !solo && Number(speed) !== 1) {
    throw new Error('official round and final formats must run at real-time speed; use rehearsal for acceleration');
  }
  const requestedSeries = series != null ? String(series).trim().slice(0, 60) : '';
  const practice = !!solo || kind === 'rehearsal';
  if (practice && requestedSeries && !isPracticeSeries(requestedSeries)) {
    throw new Error('practice rounds require a reserved practice series');
  }
  // Do not let a script omitting series inherit the currently broadcast show.
  const roundSeries = practice ? (requestedSeries || 'practice')
    : (requestedSeries || wallState().series || null);
  if (!Array.isArray(candidates)) throw new Error('need at least 3 eligible Hot markets');
  /* Validate the whole show constraint before anything is written. A round
     that half-exists is worse than one that failed to arm. */
  /* Normalise BEFORE validating. new Set([1, "1"]) sees two values while the
     integer column sees one, so a roster of 1 and "1" passed the duplicate
     check and then collapsed into a single seat. */
  candidates = candidates.map((c) => String(c).trim().toUpperCase()).filter(Boolean);
  backup = backup ? String(backup).trim().toUpperCase() : null;
  /* Counted BEFORE this normalization, so ['  ', ''] armed a round storing []
     which then died at the reveal with "Division by zero", and ['BTC', '  ']
     armed with a single candidate, quietly removing the random draw the whole
     format rests on. Cardinality is only meaningful after normalization. */
  if (new Set(candidates).size !== candidates.length) throw new Error('duplicate hot candidates');
  /* `backup` is accepted only as a rolling-client compatibility input. It is
     merged into the same eligible pool before the seed shuffles everything;
     it has no privileged tail position in a v2 draw. */
  if (backup && !candidates.includes(backup)) candidates.push(backup);
  backup = null;
  if (candidates.length < 3) throw new Error('need at least 3 eligible Hot markets (two primaries and a committed backup)');
  /* V2 Hot runs on the ordinary underlying. Accepting an event alias here
     would reveal e.g. BTC-HOT as if it were a base, then score the BTC family
     while the terminal tried to trade a retired synthetic book. Do this
     before indexedSymbol: that hook intentionally resolves aliases to their
     base for normal order validation, so it cannot distinguish this input. */
  const aliases = candidates.filter((c) => /-(?:HOT|BOOST)$/.test(c));
  if (aliases.length) throw new Error(`eligible Hot markets must be ordinary assets, not event tickers: ${aliases.join(', ')}`);
  /* Only when the engine has told us what is indexed. Unwired (tests, and any
     caller that arms before init) the check must be absent, not universally
     failing: a guard that rejects everything when it lacks information is not
     a safer guard, it is a broken one. */
  if (hooks.indexedSymbol) {
    const badSym = [...candidates, ...(backup ? [backup] : [])].filter((c) => !hooks.indexedSymbol(c));
    if (badSym.length) throw new Error(`not a tradable indexed market: ${badSym.join(', ')}`);
  }
  players = players.map((p, i) => ({
    /* null, not 0: an unclaimed seat is a seat waiting for an invite to be
       opened, and it must be distinguishable from a seat belonging to user 0. */
    userId: p.userId == null || String(p.userId).trim() === '' ? null : Number(p.userId),
    displayName: p.displayName == null ? null : String(p.displayName).slice(0, 40),
    avatarUrl: cleanAvatarUrl(p.avatarUrl),
    seat: Number(p.seat ?? i),
  }));

  /* PRACTICE ROUND. Every seat the operator did not name is given to the
     engine, and its account is created here rather than by an invite nobody
     will ever open. Filled BEFORE validation so bots go through exactly the
     same duplicate-seat, duplicate-id and account-exists checks a person
     does; a roster the guards did not see is a roster that can surprise the
     show. */
  solo = !!solo;
  if (solo) {
    const named = new Set(players.filter((p) => p.userId != null || (p.displayName || '').trim()).map((p) => p.seat));
    if (!named.size) throw new Error('a practice round still needs YOUR seat named');
    const kept = players.filter((p) => named.has(p.seat));
    /* A practice seat can wear a real face. Rehearsing against "Bot 2" tells
       the operator nothing about how the wall reads with eight real names
       and pictures on it; rehearsing against the Belgrade field does. The
       roster is used in order for each seat the engine fills, and anything
       beyond it falls back to the plain label. Cosmetic only: the account
       behind the seat is still a bot in the reserved band. */
    const wear = (Array.isArray(botRoster) ? botRoster : []).map((b) => ({
      displayName: b && b.displayName != null ? String(b.displayName).slice(0, 40) : null,
      avatarUrl: b ? cleanAvatarUrl(b.avatarUrl) : null,
    }));
    /* Fill to the STAGE's size, not to eight. A heat is four seats; a
       practice heat with eight would rehearse a round the night never plays. */
    const stg0 = stage != null ? String(stage).trim() : '';
    /* The desk's own seat count first: a practice round is filed outside the
       bracket on purpose, and without this it rehearsed an eight-seat heat
       the night never plays. Then the stage's size, then the old default. */
    const asked = Number(seatCount);
    const fillTo = (Number.isInteger(asked) && asked >= 1 && asked <= BOT_SEATS ? asked : 0)
      || (BRACKET.find((b) => b.stage === stg0) || {}).seats || BOT_SEATS;
    const bots = [];
    for (let seat = 0; seat < fillTo; seat++) {
      if (named.has(seat)) continue;
      const uid = botIdForSeat(seat);
      if (!hooks.ensureBot) throw new Error('this engine cannot create practice seats');
      const face = wear.shift() || {};
      const displayName = face.displayName || `Bot ${seat + 1}`;
      hooks.ensureBot(uid, displayName);
      bots.push({ userId: uid, displayName, avatarUrl: face.avatarUrl || null, seat });
    }
    players = [...kept, ...bots].sort((a, b) => a.seat - b.seat);
  }

  /* An empty roster armed successfully and could then never start, and there
     is no roster-amendment API, so it was a round that existed only to be
     thrown away. */
  if (!players.length) throw new Error('a round needs at least one seat');
  if (players.length > MAX_ROSTER_SEATS) throw new Error(`a round supports at most ${MAX_ROSTER_SEATS} seats`);
  if (!practice && roundSeries && !isPracticeSeries(roundSeries)
      && (kind === 'final' || String(stage || '').trim() === 'The Final')
      && players.length !== 2) {
    throw new Error('an official final requires exactly two seats');
  }
  /* Named BEFORE the insert. A duplicate id surfaced as the raw
     "UNIQUE constraint failed: paper_rounds.id", which tells an operator
     nothing about what to do next. The insert still enforces it; this just
     says it in a language a person can act on. */
  const clash = q.get.get(id);
  if (clash) {
    throw new Error(`a round called "${id}" already exists (${clash.status}); pick another id`);
  }
  const uniq = new Set(candidates);
  const seats = players.map((p) => p.seat);
  if (seats.some((x) => !Number.isInteger(x) || x < 0 || x >= MAX_ROSTER_SEATS)) {
    throw new Error(`seats must be integers from 0 to ${MAX_ROSTER_SEATS - 1}`);
  }
  if (new Set(seats).size !== seats.length) throw new Error('duplicate seats');
  /* Only seats the operator PRE-ASSIGNED are checked here. An empty seat is
     not an error, it is the normal case: the invite decides who fills it. */
  const uids = players.map((p) => p.userId).filter((u) => u !== null);
  if (uids.some((u) => !Number.isSafeInteger(u) || u <= 0)) throw new Error('a pre-assigned seat needs a positive integer userId');
  if (new Set(uids).size !== uids.length) throw new Error('duplicate players');
  if (!solo && uids.some(isBotId)) throw new Error('bot seats require a solo practice round');
  const unknown = uids.filter((u) => !userExists.get(u));
  if (unknown.length) {
    throw new Error(`no paper account for user id ${unknown.join(', ')}`
      + ' (that person has never opened the trading page, or the id is a typo)');
  }

  const now = Date.now();
  const seed = crypto.randomBytes(32).toString('hex');
  const stampedPlan = scaledPlan(kind, speed);
  const draw = makeHotDraw({ candidates, seed, plan: stampedPlan });
  const pricePolicy = hooks.defaultPricePolicy();
  if (!['strict', ROUND_PRICE_POLICY].includes(pricePolicy)) throw new Error('unsupported round price policy');
  const boostCapacityPolicy = boostCapacityPolicyOf({ boost_capacity_policy: hooks.defaultBoostCapacityPolicy() });
  const configuredBackupPolicy = backupExecutionPolicyOf({ backup_execution_policy: hooks.defaultBackupExecutionPolicy() });
  const boostLeverage = hooks.boostLeverage ? hooks.boostLeverage() : null;
  const backupExecutionPolicy = pricePolicy === ROUND_PRICE_POLICY && Number(boostLeverage) === 500
    ? configuredBackupPolicy : null;
  const commitInput = { id, kind, speed: Number(speed), candidates, seed, plan: stampedPlan, draw,
    pricePolicy, boostCapacityPolicy, backupExecutionPolicy };
  const commit = v2Commit(commitInput);
  /* One transaction: the row, its commitment and its roster arrive together
     or not at all. The seed enters memory only after the commit succeeds, so
     a failed insert can never leave a secret that no stored commitment
     corresponds to. */
  const write = db.transaction(() => {
    q.ins.run(id, kind, JSON.stringify(candidates), backup, commit, now, now);
    db.prepare('UPDATE paper_rounds SET score_precision = 6 WHERE id = ?').run(id);
    db.prepare('UPDATE paper_rounds SET price_policy = ? WHERE id = ?')
      .run(pricePolicy === ROUND_PRICE_POLICY ? pricePolicy : null, id);
    db.prepare('UPDATE paper_rounds SET boost_capacity_policy = ? WHERE id = ?')
      .run(boostCapacityPolicy === CURRENT_EQUITY_BOOST_POLICY ? boostCapacityPolicy : null, id);
    db.prepare('UPDATE paper_rounds SET backup_execution_policy = ? WHERE id = ?')
      .run(backupExecutionPolicy, id);
    /* Stamp the plan the round is armed under, so a mid-round deploy that
       edits ROUND_PLAN is DETECTABLE rather than silently replayed. */
    db.prepare('UPDATE paper_rounds SET plan_json = ?, speed = ? WHERE id = ?').run(JSON.stringify(stampedPlan), Number(speed), id);
    db.prepare('UPDATE paper_rounds SET boost_leverage = ? WHERE id = ?')
      .run(boostLeverage, id);
    db.prepare('UPDATE paper_rounds SET boost_markets = ? WHERE id = ?')
      .run(JSON.stringify(BOOST_MARKETS), id);
    if (solo) db.prepare('UPDATE paper_rounds SET solo = 1 WHERE id = ?').run(id);
    /* Which show this round belongs to. Defaulted from whatever the wall is
       currently running, so an operator arming round three of five does not
       have to remember to type the series name a third time. */
    const ser = roundSeries;
    if (ser) db.prepare('UPDATE paper_rounds SET series = ? WHERE id = ?').run(ser, id);
    const stg = stage != null && String(stage).trim() ? String(stage).trim().slice(0, 40) : null;
    /* Bounded by the roster: a round cannot advance more players than sit in
       it, and "everybody goes through" is not a bracket stage. */
    const adv = Number.isFinite(Number(advance)) && Number(advance) > 0
      ? Math.min(Math.max(1, Math.round(Number(advance))), Math.max(1, players.length - 1))
      : null;
    if (stg || adv) {
      db.prepare('UPDATE paper_rounds SET stage = ?, advance = ? WHERE id = ?').run(stg, adv, id);
    }
    players.forEach((p) => {
      /* 16 bytes: the invite is the only thing standing between a stranger and
         a seat on the broadcast, so it is not a guessable short code. */
      q.playerIns.run(id, p.userId, p.displayName, p.seat, crypto.randomBytes(16).toString('hex'));
      /* A bot cannot open its invite, and an unclaimed seat is dropped at the
         bell, so a practice round would have started with one player in it. */
      if (isBotId(p.userId)) {
        db.prepare('UPDATE paper_round_players SET claimed_at = ?, ready_at = ? WHERE round_id = ? AND seat = ?')
          .run(now, now, id, p.seat);
      }
      if (p.avatarUrl) {
        db.prepare('UPDATE paper_round_players SET avatar_url = ? WHERE round_id = ? AND seat = ?')
          .run(p.avatarUrl, id, p.seat);
      }
    });
    /* Written last so the immutability trigger protects every committed input
       from this point forward. The private seed/draw never enter an HTTP or
       operator-log serializer before settlement. */
    db.prepare(`UPDATE paper_rounds
      SET hot_draw_secret = ?, hot_draw_json = ?, format_version = 2
      WHERE id = ?`).run(seed, JSON.stringify(draw), id);
    if (pricePolicy === ROUND_PRICE_POLICY) {
      if (typeof hooks.initializeRoundMarks !== 'function') throw new Error('round mark initializer is not wired');
      hooks.initializeRoundMarks(q.get.get(id), { at: Date.now() });
      // Recent ticks are an explicitly bounded display cache, not the audit
      // ledger. Only this newly introduced cache for closed rounds is aged
      // out; fills, minute bars, latest marks and all proofs remain durable.
      db.prepare(`DELETE FROM paper_round_mark_ticks WHERE round_id IN
        (SELECT id FROM paper_rounds WHERE status IN ('done','aborted'))`).run();
    }
  });
  write();
  hooks.log(`round ${id} armed (${kind}, sealed two-Hot draw)`);
  return q.get.get(id);
}

/* V1-only memory seeds. V2 persists its sealed draw before the bell so a
 * restart cannot force a redraw or destroy a live competition. */
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
/* The single definition of "can these markets host this round".
 *
 * Preflight used to check current readiness only, while start ALSO checked
 * sustained reliability, so the desk could go green and the very next click
 * fail with "markets not reliably priceable". Two definitions of ready is the
 * operator-facing failure class this whole round is trying to remove, so
 * there is now one function and both callers use it. */
/* Rounds an operator has deliberately allowed to start on thin history. A
   named, audited act with a reason attached, not an env var that needs a
   restart — and a restart is exactly what empties the history in the first
   place, so the env var was unusable in the situation it existed for. */
const _readinessOverride = new Map();   // roundId -> { at, why }
function overrideReadiness(id, why) {
  if (!q.get.get(id)) throw new Error('no such round: ' + id);
  _readinessOverride.set(id, { at: Date.now(), why: String(why || '').slice(0, 200) });
  hooks.log(`round ${id}: readiness overridden by the operator (${why})`);
  return { id, why };
}

/* The reliability verdict for ONE market, by the same rule marketReadiness
   applies at Arm. Exported because the desk was asking a different question
   from the one that gates it: the market chips coloured on instantaneous
   readiness, so a market that had been ticking for two minutes, or that was
   ready 86% of the time, sat green while the operator typed it into the
   candidate box and then failed at Arm with "markets not reliably priceable".
   One rule, one answer, wherever it is asked. */
function marketReliabilityOf(sym) {
  const rel = hooks.marketReliability ? hooks.marketReliability(sym) : null;
  const enoughHistory = !!rel && rel.spanMs >= MIN_RELIABILITY_SPAN_MS;
  return {
    reliability: rel,
    enoughHistory,
    reliable: enoughHistory && rel.ratio >= MIN_READY_RATIO && !(rel.longestGapMs > MAX_READY_GAP_MS),
    /* How long until a thin market could qualify, so "not enough history" is
       a wait rather than a mystery. */
    needsMoreMs: enoughHistory ? 0 : Math.max(0, MIN_RELIABILITY_SPAN_MS - ((rel && rel.spanMs) || 0)),
    thresholds: { minRatio: MIN_READY_RATIO, maxGapMs: MAX_READY_GAP_MS, minSpanMs: MIN_RELIABILITY_SPAN_MS },
  };
}

/* ── the show's own state ───────────────────────────────────────────────
 *
 * `mode` is what the room should be looking at:
 *   auto       the engine decides: the live round, or the idle card
 *   preshow    before round one, the field and the format
 *   interval   between rounds, the running standing and a countdown
 *   technical  something broke, cover the screen
 *   final      the night's result, held until the operator moves on
 *
 * `technical` is the one mode that overrides a LIVE round, because that is
 * the entire point of the button: the thing you press when what is on the
 * wall must stop being on the wall. Every other mode yields to a live round,
 * so a stage left set by mistake can never hide one. */
const WALL_MODES = new Set(['auto', 'preshow', 'interval', 'technical', 'final']);

/** FLUSH THE NIGHT. Every finished round filed under the series is re-tagged
 *  out of it, every armed round is discarded, the wall goes back to the idle
 *  card with no line and no clock. The rows stay in the database under an
 *  archive tag, so nothing is destroyed; the bracket simply starts clean.
 *  Refused while a round is live: abort that first, on purpose. */
function resetNight(series) {
  if (currentRound()) throw new Error('a round is live; abort it before resetting the night');
  const armed = db.prepare("SELECT id FROM paper_rounds WHERE status = 'armed'").all();
  for (const a of armed) abortRound(a.id, { force: true });
  const ser = series ? String(series).trim() : '';
  const stamp = new Date().toISOString().slice(0, 16).replace(/[-:T]/g, '');
  const tag = `${ser || 'night'}~archived-${stamp}`;
  const archived = ser
    ? db.prepare("UPDATE paper_rounds SET series = ?, updated_at = ? WHERE series = ? AND status IN ('done', 'aborted')").run(tag, Date.now(), ser).changes
    : 0;
  setWall({ mode: 'auto', message: null, nextInMs: 0 });
  hooks.log(`night reset: ${archived} rounds under "${ser || '(none)'}" archived as ${tag}, ${armed.length} armed discarded`);
  return { archived, discarded: armed.length, tag };
}

function wallState() {
  const w = db.prepare('SELECT * FROM paper_wall WHERE id = 1').get()
    || { mode: 'auto', message: null, next_at: null, next_label: null, series: null, series_total: null };
  return w;
}

function setWall({ mode, message, nextAt, nextInMs, nextLabel, series, seriesTotal, pausedMs } = {}) {
  const w = wallState();
  /* THE CALLER'S CLOCK IS NOT OURS.
     The desk used to send an absolute instant derived from `Date.now()` in a
     venue browser, and we stored it verbatim: a laptop three minutes fast put
     three minutes of lie on the LED wall, and a venue machine is exactly the
     machine whose clock nobody has checked. A DURATION is unambiguous, so
     when one is offered it wins and the instant is stamped here. */
  if (nextInMs !== undefined) {
    const ms = Number(nextInMs);
    nextAt = Number.isFinite(ms) && ms > 0 ? Date.now() + Math.min(ms, 12 * 3600_000) : 0;
  }
  /* HOLDING the clock and RUNNING it are the same clock in two states, so one
     of them is always null. Pausing stores what was left and stops the wall's
     countdown; resuming passes the remainder back as a duration. */
  let paused = pausedMs === undefined ? w.paused_ms : (Number(pausedMs) > 0 ? Math.min(Math.round(Number(pausedMs)), 12 * 3600_000) : null);
  if (paused != null && (nextAt || nextInMs)) paused = pausedMs === undefined ? null : paused;
  if (paused != null) nextAt = 0;
  if (mode !== undefined && !WALL_MODES.has(String(mode))) {
    throw new Error(`unknown wall mode: ${mode} (${[...WALL_MODES].join(', ')})`);
  }
  const next = {
    mode: mode === undefined ? w.mode : String(mode),
    message: message === undefined ? w.message : (String(message || '').slice(0, 240) || null),
    /* A countdown in the past is not a countdown, and a wall showing -0:14 in
       front of a room is worse than showing nothing. Cleared rather than
       stored. */
    nextAt: nextAt === undefined ? w.next_at : (Number(nextAt) > Date.now() ? Number(nextAt) : null),
    nextLabel: nextLabel === undefined ? w.next_label : (String(nextLabel || '').slice(0, 60) || null),
    series: series === undefined ? w.series : (String(series || '').slice(0, 60) || null),
    seriesTotal: seriesTotal === undefined ? w.series_total : (Number(seriesTotal) > 0 ? Math.min(50, Math.round(Number(seriesTotal))) : null),
    pausedMs: paused,
  };
  db.prepare(`UPDATE paper_wall SET mode = ?, message = ?, next_at = ?, next_label = ?,
                series = ?, series_total = ?, paused_ms = ?, updated_at = ? WHERE id = 1`)
    .run(next.mode, next.message, next.nextAt, next.nextLabel, next.series, next.seriesTotal, next.pausedMs, Date.now());
  hooks.log(`wall set to ${next.mode}${next.message ? ` ("${next.message}")` : ''}`);
  return wallState();
}

/* THE NIGHT'S SHAPE, named once.
 *
 * Eight players, two heats of four, the four who came through into a
 * semi-final, and a one-on-one final. The same bracket the Belgrade arena
 * runs, so the two products describe the show with the same words rather
 * than each inventing their own.
 *
 * `kind` picks the phase plan: the heats and the semi are full rounds, the
 * final is the short one. `advance` is how many go through, and it is what
 * turns four results into a bracket. */
const BRACKET = [
  { key: 'heat1', stage: 'Round 1', kind: 'round', seats: 4, advance: 2, note: 'four traders, top two advance' },
  { key: 'heat2', stage: 'Round 2', kind: 'round', seats: 4, advance: 2, note: 'four traders, top two advance' },
  { key: 'semi', stage: 'Semi-finals', kind: 'round', seats: 4, advance: 2, note: 'the four who came through, top two advance' },
  { key: 'final', stage: 'The Final', kind: 'final', seats: 2, advance: 1, note: 'one against one' },
];

/* THE BRACKET.
 *
 * Each round is fresh: every player starts flat at the same bankroll and
 * nothing carries over, so there is no running total and no points table.
 * What DOES carry is who is still in, and that is the whole shape of the
 * night: eight players, two heats of four, four survivors into a semi-final,
 * two into a one-on-one final.
 *
 * A round therefore has two facts beyond its result: what it is called, and
 * how many of its players go through. `advance` is the one that turns a list
 * of results into a bracket. A round with no `advance` is a standalone round
 * and reads exactly as it always did.
 */
/** Final placings for a whole stop, with the prize each one pays.
 *
 *  1st and 2nd come from the final. 3rd and 4th are the two the semi-final put
 *  out, ordered by their semi-final PnL. Returns [] until the final is done,
 *  because a placing that can still change is not a placing.
 */
/* Every night the desk has run, newest first, archived nights included. A
   round row is what the round wrote when it settled: the board is the frozen
   standings, never a recomputation, so the history can only repeat what the
   result said. Practice nights carry no prize placings. Read-only. */
function nightHistory({ limit = 30 } = {}) {
  const max = Math.min(100, Math.max(1, Number(limit) || 30));
  const rows = db.prepare(`SELECT * FROM paper_rounds
    WHERE status IN ('done', 'aborted') AND started_at IS NOT NULL
    ORDER BY COALESCE(ends_at, updated_at, started_at) DESC LIMIT 2000`).all();
  const nights = new Map();
  for (const r of rows) {
    const tag = r.series || '';
    const name = tag.replace(/~archived-\d+$/, '') || '(no series)';
    const archivedAt = (tag.match(/~archived-(\d{12})$/) || [])[1] || null;
    if (!nights.has(tag)) {
      nights.set(tag, { series: tag, name, archived: !!archivedAt, archivedAt, practice: isPracticeSeries(name), rounds: [] });
    }
    const night = nights.get(tag);
    if (night.rounds.length >= 40) continue;
    let board = [];
    if (r.status === 'done') { try { board = standings(r.id, 'final'); } catch { board = []; } }
    let roster = [];
    try { roster = q.players.all(r.id); } catch { roster = []; }
    const seatOf = new Map(roster.map((p) => [p.user_id, p.seat]));
    const nameOf = new Map(roster.map((p) => [p.user_id, p.display_name || null]));
    night.rounds.push({
      id: r.id, kind: r.kind, stage: r.stage || null, status: r.status,
      startedAt: r.started_at || null, endedAt: r.ends_at || null, settledAt: r.updated_at || null,
      advance: r.advance || null, solo: !!r.solo, practice: isPracticeRound(r),
      players: roster.map((p) => ({ userId: p.user_id, seat: p.seat, name: p.display_name || null })),
      board: board.map((b) => ({
        rank: b.rank, userId: b.user_id, seat: seatOf.get(b.user_id) ?? null, name: nameOf.get(b.user_id) || null,
        score: b.score, maxDrawdown: b.maxDrawdown,
        through: r.advance && r.status === 'done' ? b.rank <= r.advance : null,
      })),
    });
  }
  const out = [...nights.values()].slice(0, max);
  for (const n of out) {
    n.rounds.reverse();
    const finalR = [...n.rounds].reverse().find((x) => x.status === 'done' && (x.kind === 'final' || x.stage === 'The Final'));
    n.winner = finalR && finalR.board[0] ? { userId: finalR.board[0].userId, name: finalR.board[0].name, score: finalR.board[0].score } : null;
    let placings = [];
    if (!n.practice) { try { placings = stopPlacings(n.series); } catch { placings = []; } }
    n.placings = placings;
  }
  return { nights: out, total: nights.size };
}

function visibleSeriesRounds(series, doneOnly = false) {
  return db.prepare(`SELECT r.* FROM paper_rounds r
    WHERE r.series = ? AND r.status IN ('armed', 'running', 'done')
      AND (? = 0 OR r.status = 'done')
      AND (? = 1 OR (COALESCE(r.solo, 0) = 0 AND r.kind != 'rehearsal'
        AND NOT EXISTS (SELECT 1 FROM paper_round_players p WHERE p.round_id = r.id
          AND p.user_id >= ? AND p.user_id < ?)))
    ORDER BY COALESCE(r.ends_at, r.started_at, r.start_at, r.created_at)`)
    .all(String(series), Number(doneOnly), Number(isPracticeSeries(series)), BOT_ID_BASE, BOT_ID_BASE + 1000);
}
function stopPlacings(series) {
  if (!series) return [];
  // Practice keeps its round winners on the wall, never official prize claims.
  if (isPracticeSeries(series)) return [];
  const rounds = visibleSeriesRounds(series, true);
  if (!rounds.length) return [];
  const finalR = [...rounds].reverse().find((r) => r.kind === 'final' || r.stage === 'The Final');
  if (!finalR) return [];
  const semiR = [...rounds].reverse().find((r) => r.stage === 'Semi-finals');
  const nameOn = (roundId, uid) => {
    try { return (q.players.all(roundId).find((p) => p.user_id === uid) || {}).display_name || null; }
    catch { return null; }
  };
  const out = [];
  let finalBoard = [];
  try { finalBoard = standings(finalR.id, 'final'); } catch { finalBoard = []; }
  // The published stop rule has two finalists. Do not silently turn a
  // semifinal loser into runner-up or discard an extra finalist.
  if (finalBoard.length !== 2) return [];
  for (const row of finalBoard.slice(0, 2)) {
    out.push({ place: out.length + 1, userId: row.user_id, name: nameOn(finalR.id, row.user_id),
      score: row.score, from: 'The Final', roundId: finalR.id });
  }
  /* The semi's OWN board decides third from fourth, and the two who advanced
     are skipped rather than assumed to be its top two: if the final's roster
     ever differs from the semi's winners, the record is what counts. */
  if (semiR) {
    let semiBoard = [];
    try { semiBoard = standings(semiR.id, 'final'); } catch { semiBoard = []; }
    const advanced = new Set(out.map((x) => x.userId));
    for (const row of semiBoard) {
      if (advanced.has(row.user_id)) continue;
      if (out.length >= 4) break;
      out.push({ place: out.length + 1, userId: row.user_id, name: nameOn(semiR.id, row.user_id),
        score: row.score, from: 'Semi-finals', roundId: semiR.id });
    }
  }
  return out.map((x) => ({ ...x, prize: STOP_PRIZE[x.place] ?? 0 }));
}

function seriesBoard(series) {
  if (!series) return null;
  /* Armed and running rounds count. A bracket screen that only knows about
     finished rounds cannot show the round about to be played, which is the
     one the room is waiting for. */
  const rounds = visibleSeriesRounds(series);

  const seen = new Map();          // userId -> { userId, rounds: [], wins, out }
  const stages = rounds.map((r, i) => {
    let board = [];
    if (r.status === 'done') { try { board = standings(r.id, 'final'); } catch { board = []; } }
    let roster = [];
    try { roster = q.players.all(r.id); } catch { roster = []; }
    const byUser = new Map(board.map((b) => [b.user_id, b]));
    /* Frozen into the round when it was armed, so a player changing their
       picture between the heat and the semi cannot change the wall. */
    const faceOf = new Map(roster.map((p) => [p.user_id, p.avatar_url || null]));
    const nameOnRound = new Map(roster.map((p) => [p.user_id, p.display_name || null]));
    const advance = r.advance || null;
    /* A round that has not been played is a ROSTER, and a seat on it may not
       have been claimed yet: the player has a name on the desk and no user id
       until they open their invite. Keyed on the seat in that case, because
       the wall's job here is to say who is about to play, and it has known
       that since the operator typed it. */
    const rows = board.length
      ? board.map((b) => ({ uid: b.user_id, seat: null }))
      : roster.map((p) => ({ uid: p.user_id, seat: p.seat, name: p.display_name, avatar: p.avatar_url }));
    const players = rows.map((row) => {
      const uid = row.uid;
      const res = uid == null ? null : (byUser.get(uid) || null);
      const rank = res ? res.rank : null;
      return {
        /* Negative for an unclaimed seat, so a key is always unique and a
           caller can never mistake it for a real account. */
        userId: uid != null ? uid : -(1000 * (i + 1) + row.seat),
        seat: row.seat,
        claimed: uid != null,
        bot: uid != null && isBotId(uid),
        practice: isPracticeRound(r),
        rank,
        avatar: (uid != null ? faceOf.get(uid) : row.avatar) || null,
        seatName: (uid != null ? nameOnRound.get(uid) : row.name) || null,
        score: res ? res.score : null,
        /* Through, out, or not yet decided. Null while the round is unplayed
           so nothing on the wall claims a result that does not exist. */
        through: rank == null ? null : (advance ? rank <= advance : rank === 1),
      };
    });
    for (const p of players) {
      /* An unclaimed seat is not a competitor yet, so it does not get a row in
         the who-is-still-in board; it exists only inside its own stage. */
      if (!p.claimed) continue;
      if (!seen.has(p.userId)) seen.set(p.userId, { userId: p.userId, rounds: [], wins: 0 });
      const e = seen.get(p.userId);
      e.rounds.push({ id: r.id, index: i + 1, stage: r.stage || null, rank: p.rank, score: p.score, through: p.through });
      if (p.rank === 1) e.wins += 1;
    }
    return {
      id: r.id,
      index: i + 1,
      stage: r.stage || null,
      practice: isPracticeRound(r),
      status: r.status,
      advance,
      players,
      winner: board[0] ? board[0].user_id : null,
    };
  });

  const done = stages.filter((x) => x.status === 'done');
  const latest = done.length ? done[done.length - 1] : null;
  /* Still in: everybody who went through their most recent decided round, or
     who has a round ahead of them that has not been played yet. */
  const live = new Set();
  for (const e of seen.values()) {
    const played = e.rounds.filter((x) => x.through !== null);
    const pending = e.rounds.some((x) => x.through === null);
    if (pending || (played.length && played[played.length - 1].through)) live.add(e.userId);
  }
  const board = [...seen.values()].map((e) => ({
    ...e,
    bot: isBotId(e.userId),
    practice: isPracticeSeries(series),
    stillIn: live.has(e.userId),
    latestRank: latest ? (e.rounds.find((x) => x.id === latest.id)?.rank ?? null) : null,
  })).sort((a, b) =>
    Number(b.stillIn) - Number(a.stillIn)
    || (a.latestRank ?? 99) - (b.latestRank ?? 99)
    || b.rounds.length - a.rounds.length
    || a.userId - b.userId
  );
  return {
    series: String(series),
    practice: isPracticeSeries(series),
    official: !isPracticeSeries(series),
    roundsDone: done.length,
    roundIds: rounds.map((r) => r.id),
    latest: latest ? { id: latest.id, index: latest.index, stage: latest.stage, winner: latest.winner } : null,
    /* The round the room is waiting for, if one is armed or running. */
    next: (stages.find((x) => x.status !== 'done') || null),
    stages,
    board,
    /* Empty until the final is decided. */
    placings: stopPlacings(series),
    prizeTable: { ...STOP_PRIZE },
  };
}

function marketReadiness(id) {
  const r = q.get.get(id);
  if (!r) return { ok: false, error: 'no such round: ' + id, markets: [] };
  const hotNeed = [...new Set([
    ...JSON.parse(r.hot_candidates || '[]'),
    ...(r.hot_backup ? [r.hot_backup] : []),
  ])];
  const boostNeed = [...new Set(boostMarketsOf(r))];
  const need = [...new Set([...hotNeed, ...boostNeed])];
  const markets = need.map((sym) => {
    /* No history is NOT a pass, and that rule lives in one place: a restart
       shortly before a show erases the evidence, and a second copy of the
       test is how the desk and the gate came to disagree. */
    const v = marketReliabilityOf(sym);
    const needsHot = hotNeed.includes(sym);
    const needsBoost = boostNeed.includes(sym);
    const hotPriceReady = !needsHot || !!hooks.marketReady(sym, r);
    const boostPriceReady = !needsBoost || !!(hooks.marketReadyForBoost
      ? hooks.marketReadyForBoost(sym, Number(r.boost_leverage) || 500, r)
      : hooks.marketReady(sym, r));
    return {
      symbol: sym,
      indexed: !hooks.indexedSymbol || hooks.indexedSymbol(sym),
      roles: { hot: needsHot, boost: needsBoost },
      hotPriceReady,
      boostPriceReady,
      /* Compatibility aggregate. Start/preflight now means every role this
         market was promised for, including the actual frozen 500x Boost
         tier—not merely that an ordinary 100x mark exists. */
      priceReady: hotPriceReady && boostPriceReady,
      reliability: v.reliability,
      enoughHistory: v.enoughHistory,
      reliable: v.reliable,
    };
  });
  const unready = markets.filter((m) => !m.priceReady).map((m) => m.symbol);
  if (unready.length) {
    const error = Number(r.format_version) >= 2
      ? `${unready.length} sealed draw market(s) are not competition-ready`
      : `markets not competition-ready: ${unready.join(', ')}`;
    return { ok: false, error, markets, need };
  }
  const flaky = markets.filter((m) => m.enoughHistory && !m.reliable);
  if (flaky.length) {
    return {
      ok: false, markets, need,
      error: Number(r.format_version) >= 2
        ? `${flaky.length} sealed draw market(s) are not reliably priceable`
        : 'markets not reliably priceable: ' + flaky
        .map((m) => `${m.symbol} ${(m.reliability.ratio * 100).toFixed(0)}% ready, longest gap ${Math.round(m.reliability.longestGapMs / 1000)}s`)
        .join('; '),
    };
  }
  const thin = markets.filter((m) => !m.enoughHistory);
  if (thin.length && !allowUnproven() && !_readinessOverride.has(id)) {
    /* Say how long, not just no. After a restart the engine is watching
       healthy markets and simply has not been watching for long, and an
       operator staring at "not enough history" three minutes before a show
       needs to know whether to wait ninety seconds or fix something. */
    const waitMs = Math.max(...thin.map((m) => MIN_RELIABILITY_SPAN_MS - ((m.reliability && m.reliability.spanMs) || 0)));
    return {
      ok: false, markets, need, thin: thin.map((m) => m.symbol), waitMs,
      error: Number(r.format_version) >= 2
        ? `${thin.length} sealed draw market(s) need ${Math.ceil(waitMs / 1000)}s more reliability history, or override it deliberately`
        : `only ${Math.round((thin[0].reliability?.spanMs || 0) / 1000)}s of reliability history for `
          + `${thin.map((m) => m.symbol).join(', ')}; ${Math.ceil(waitMs / 1000)}s more needed, `
          + 'or override it deliberately',
    };
  }
  return { ok: true, markets, need };
}

/* Rehearsals and test harnesses legitimately have no history. This is an
   explicit, named escape hatch rather than the silent pass it was, and it is
   read at CALL time so a rehearsal can enable it without a restart. In
   production it is unset, and a market the engine has not watched long enough
   cannot host a round. */
const allowUnproven = () => process.env.PAPER_ALLOW_UNPROVEN_MARKETS === '1';

/* ── scheduled starts ─────────────────────────────────────────────────────
   "Start in 5 minutes" is how a show actually begins: the operator commits,
   the room sees a countdown, and everyone gets to their desk. */
const _startTimers = new Map();

function scheduleStart(id, atMs) {
  const r = q.get.get(id);
  if (!r) throw new Error('no such round: ' + id);
  if (r.status !== 'armed') throw new Error(`round ${id} is ${r.status}, not armed`);
  const at = Number(atMs);
  if (!Number.isFinite(at)) throw new Error('a scheduled start needs a timestamp');
  const inMs = at - Date.now();
  /* Start-now is a separate, explicit operator action. Treating a typo'd or
     stale timestamp as an immediate bell is dangerous under show pressure. */
  if (inMs < 1_000) throw new Error('a scheduled start must be at least one second in the future; use Start now for an immediate bell');
  if (inMs > 60 * 60_000) throw new Error('a start more than an hour out is almost certainly a mistake');
  /* The public state and the desk expose one pending countdown. Therefore the
     engine must own at most one, rather than accepting hidden timers that later
     collide with the live round and block themselves. Rescheduling the same
     round remains allowed. */
  const pending = db.prepare("SELECT id, start_at FROM paper_rounds WHERE status = 'armed' AND start_at IS NOT NULL AND id != ? ORDER BY start_at LIMIT 1").get(id);
  if (pending) throw new Error(`round ${pending.id} is already scheduled; cancel it before scheduling ${id}`);
  /* A scheduled bell is a promise to the room, so reject every condition
     already known to make Start now fail. The fire-time path still re-checks
     these because accounts and markets can change during the countdown. */
  const running = q.anyRunning.get();
  if (running) throw new Error(`round ${running.id} is already running; abort it first`);
  if (Number(r.format_version) >= 2 ? !sealedDrawValid(r) : !_seeds.has(id)) {
    throw new Error(`round ${id}: committed draw unavailable or invalid; re-arm the round`);
  }
  if (r.blocked_reason) throw new Error(`round ${id} is blocked: ${r.blocked_reason}`);
  const players = q.players.all(id).filter((p) => p.user_id !== null);
  if (!players.length) throw new Error('cannot schedule a round with no claimed players');
  const missing = players.map((p) => p.user_id).filter((u) => !userExists.get(u));
  if (missing.length) throw new Error(`no paper account for user id ${missing.join(', ')}`);
  if (!r.solo && players.some((p) => isBotId(p.user_id))) {
    throw new Error('bot seats require a solo practice round');
  }
  if (!isPracticeRound(r) && r.series
      && (r.kind === 'final' || r.stage === 'The Final') && players.length !== 2) {
    throw new Error('an official final requires exactly two claimed players');
  }
  const verdict = marketReadiness(id);
  if (!verdict.ok) throw new Error(verdict.error);
  q.setStartAt.run(at, Date.now(), id);
  armStartTimer(id, at);
  hooks.log(`round ${id} scheduled to start in ${Math.round(inMs / 1000)}s`);
  return { id, startAt: at, inMs };
}

function cancelScheduledStart(id) {
  const r = q.get.get(id);
  if (!r) throw new Error('no such round: ' + id);
  if (r.status !== 'armed') throw new Error(`round ${id} is ${r.status}, not armed`);
  if (!r.start_at) throw new Error(`round ${id} has no scheduled start`);
  const t = _startTimers.get(id);
  if (t) { clearTimeout(t); _startTimers.delete(id); }
  q.setStartAt.run(null, Date.now(), id);
  hooks.log(`round ${id}: scheduled start cancelled`);
  return { id, startAt: null };
}

/* How long a scheduled start may be held waiting for a temporary gate before
   it is treated as a real failure. Long enough to cover an engine restart's
   five minute reliability window with room to spare, short enough that a
   round cannot sit held through a whole segment without anyone noticing. */
const START_HOLD_MAX_MS = Number(process.env.PAPER_START_HOLD_MAX_MS || 8 * 60_000);
const _startHeldSince = new Map();
function armStartTimer(id, at) {
  const prev = _startTimers.get(id);
  if (prev) clearTimeout(prev);
  const fire = () => {
    _startTimers.delete(id);
    const r = q.get.get(id);
    /* Re-read rather than trusting the closure: the round may have been
       aborted, started by hand, or rescheduled while we waited. */
    if (!r || r.status !== 'armed' || !r.start_at) return;
    if (Date.now() < r.start_at - 250) { armStartTimer(id, r.start_at); return; }
    try {
      /* Only this timer may consume a published countdown. A second desk tab
         calling Start now must not move the bell forward after the room has
         already been shown a scheduled time. Pass the exact promise we just
         re-read so startRound can distinguish the timer from a manual call. */
      startRound(id, { scheduledStartAt: Number(r.start_at) });
      _startHeldSince.delete(id);
      hooks.log(`round ${id} started on its schedule`);
    } catch (e) {
      /* A KNOWN WAIT IS NOT A FAILURE.
       *
       * Blocking on any error destroyed armed rounds for a reason that fixes
       * itself: an engine restart resets the five minute market reliability
       * window, and a countdown that fired inside it took the roster, the
       * claimed seats and the committed draw down with it (twice on
       * 2026-09-03, both times 59s and 248s short). When readiness says how
       * long it needs, hold the round, keep the countdown, and start when the
       * wait is over. Anything else, and any hold that outlasts the operator's
       * patience, still blocks loudly. */
      const wait = Number(e && e.retryInMs) > 0 ? Number(e.retryInMs) : 0;
      const held = _startHeldSince.get(id) || 0;
      const heldFor = held ? Date.now() - held : 0;
      if (wait > 0 && heldFor + wait <= START_HOLD_MAX_MS) {
        if (!held) _startHeldSince.set(id, Date.now());
        const at = Date.now() + wait + 500;
        try { q.setStartAt.run(at, Date.now(), id); } catch {}
        hooks.log(`round ${id}: start held ${Math.ceil(wait / 1000)}s, ${e.message}`);
        armStartTimer(id, at);
        return;
      }
      hooks.log(`round ${id}: SCHEDULED START FAILED: ${e.message}`);
      _startHeldSince.delete(id);
      try { q.setStartAt.run(null, Date.now(), id); } catch {}
      try { blockRound(id, `scheduled start failed: ${e.message}`); } catch {}
    }
  };
  const delay = Math.max(0, at - Date.now());
  /* setTimeout saturates past ~24.8 days; nothing here is that far out, but
     cap the hop anyway so a bad number cannot fire immediately. */
  _startTimers.set(id, setTimeout(fire, Math.min(delay, 2_000_000_000)));
}

/* Reconcile every ARMED round after boot. The draw seed is deliberately
   memory-only, so a fresh process cannot prove the commitment for either a
   scheduled or an unscheduled armed round. Fail all such rounds closed and
   expose one operator recovery path: abort the old commitment and re-arm a
   fresh round. A same-process call still re-arms a schedule whose seed exists. */
function resumeScheduledStarts() {
  const rows = db.prepare("SELECT * FROM paper_rounds WHERE status = 'armed' ORDER BY created_at, id").all();
  let resumed = 0;
  for (const r of rows) {
    if (Number(r.format_version) < 2 && !_seeds.has(r.id)) {
      /* AN ARMED ROUND IS RE-SEEDED, NOT KILLED. Before the bell nobody has
         traded and nothing has been drawn, so a fresh seed and a fresh
         published commitment are exactly as honest as the first ones. The
         old rule blocked every armed round on every deploy and sent the
         operator back to a blank form; a live round is different and still
         fails closed, because a mid-round re-draw would be a different game. */
      let candidates = [];
      try { candidates = JSON.parse(r.hot_candidates || '[]'); } catch { candidates = []; }
      const seed = crypto.randomBytes(32).toString('hex');
      const commit = commitOf(seed, candidates);
      db.prepare('UPDATE paper_rounds SET draw_commit = ?, updated_at = ? WHERE id = ?').run(commit, Date.now(), r.id);
      _seeds.set(r.id, seed);
      if (r.blocked_reason && /draw seed unavailable/i.test(r.blocked_reason)) q.setBlocked.run(null, Date.now(), r.id);
      hooks.log(`round ${r.id}: re-committed the draw after an engine restart (armed, nobody had traded); new commitment ${commit.slice(0, 16)}`);
    }
    if (r.start_at !== null && r.start_at !== undefined) {
      armStartTimer(r.id, r.start_at);
      hooks.log(`round ${r.id}: scheduled start re-armed for ${new Date(r.start_at).toISOString()}`);
      resumed += 1;
    }
  }
  return resumed;
}

function startRound(id, { at = Date.now(), prepare = true, scheduledStartAt = null } = {}) {
  at = competitionMs(at);
  const r = q.get.get(id);
  if (!r) throw new Error('no such round: ' + id);
  if (r.status !== 'armed') throw new Error(`round ${id} is ${r.status}, not armed`);
  /* A scheduled start is a published promise to the room. Start now used to
     ignore start_at, so a stale second tab could ring the bell immediately.
     The internal timer proves which exact promise it is consuming; every
     ordinary/manual call must cancel the countdown first. */
  if (r.start_at !== null && r.start_at !== undefined) {
    const promised = Number(r.start_at);
    if (!Number.isFinite(Number(scheduledStartAt)) || Number(scheduledStartAt) !== promised) {
      throw new Error(`round ${id} is scheduled for ${new Date(promised).toISOString()}; cancel the countdown before using Start now`);
    }
    if (Date.now() < promised - 250) {
      throw new Error(`round ${id} scheduled start is not due yet`);
    }
  } else if (scheduledStartAt !== null && scheduledStartAt !== undefined) {
    throw new Error(`round ${id} no longer has that scheduled start`);
  }
  const other = q.anyRunning.get();
  if (other) throw new Error(`round ${other.id} is already running; abort it first`);
  /* The seed lives in memory only. If it is gone the round is already
     guaranteed to block at its reveal, so refusing here turns a live-stage
     incident into a pre-show "re-arm the round". */
  if (Number(r.format_version) >= 2 ? !sealedDrawValid(r) : !_seeds.has(id)) {
    throw new Error(`round ${id}: committed draw unavailable or invalid; re-arm the round`);
  }
  if (r.blocked_reason) throw new Error(`round ${id} is blocked: ${r.blocked_reason}`);
  /* A seat nobody claimed cannot trade and cannot score. Drop it rather than
     carrying a permanent zero onto the wall, and say which seats went, because
     "we started without Yuki" is a thing the producer must know they did.

     Readiness is deliberately NOT consulted: a player who never pressed ready
     still plays. The operator starts the show, not the roster. */
  const all = q.players.all(id);
  const empty = all.filter((p) => p.user_id === null);
  /* Computed here, DROPPED later. Dropping before validation meant a
     transient market-readiness failure permanently destroyed a seat and its
     invite while leaving the round armed: the operator retried Start and the
     player who had not opened their link yet no longer had a seat to open. */
  const players = all.filter((p) => p.user_id !== null);
  /* One claimed seat is enough for the ENGINE: a round is scoreable with a
     single player and rehearsals rely on it. How many seats a SHOW needs is a
     production question, and the desk asks for two. */
  if (!players.length) throw new Error('cannot start a round with no players');
  if (empty.length) {
    hooks.log(`round ${id}: will drop ${empty.length} unclaimed seat(s) at start: `
      + empty.map((p) => p.display_name || `#${p.seat + 1}`).join(', '));
  }
  /* Re-checked here and not only at arm: an account can be removed in the
     window between arming and the bell, and the failure mode without this is
     an opaque constraint error at the worst possible moment. */
  const missing = players.map((p) => p.user_id).filter((u) => !userExists.get(u));
  if (missing.length) throw new Error(`no paper account for user id ${missing.join(', ')}`);
  /* Market readiness is checked HERE, not only in the preflight endpoint.
     Preflight is a preview an operator can skip; a round that starts on
     markets it cannot price strictly will simply block at its first
     checkpoint, in front of an audience. */
  if (!r.solo && players.some((p) => isBotId(p.user_id))) {
    throw new Error('bot seats require a solo practice round');
  }
  if (!isPracticeRound(r) && r.series
      && (r.kind === 'final' || r.stage === 'The Final') && players.length !== 2) {
    throw new Error('an official final requires exactly two claimed players');
  }
  const need = [...new Set([
    ...JSON.parse(r.hot_candidates || '[]'),
    ...(r.hot_backup ? [r.hot_backup] : []),
    ...boostMarketsOf(r),
  ])];
  const verdict = marketReadiness(id);
  if (!verdict.ok) {
    const e = new Error(verdict.error);
    /* A readiness verdict that says how long to wait is a DELAY, not a
       verdict. The scheduled-start timer needs to tell those apart, so the
       wait travels with the error instead of being flattened into a string. */
    if (Number(verdict.waitMs) > 0) e.retryInMs = Number(verdict.waitMs);
    throw e;
  }

  /* Reset, bind the epoch and go live in ONE transaction. If any seat cannot
     be prepared the round does not start at all, rather than starting with
     one player on last round's balance. */
  const ends = at + planOf(r).total;
  const go = db.transaction(() => {
    /* Roster finalisation is part of going live, not a prelude to it. If
       anything below fails the whole transaction rolls back and the armed
       round is byte-for-byte what it was. */
    for (const p of empty) q.dropSeat.run(id, p.seat);
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
      /* Peak starts at the bankroll, not at whatever the first sample sees.
         Initialising lazily meant an opening loss became the peak and the
         drawdown it caused vanished. */
      db.prepare(`UPDATE paper_round_players
                  SET start_balance = ?, peak_equity = ?, max_drawdown = 0
                  WHERE round_id = ? AND user_id = ?`)
        .run(prep.startBalance, prep.startBalance, id, p.user_id);
    }
    const writtenAt = Date.now();
    if (pricePolicyOf(r) === ROUND_PRICE_POLICY) {
      if (typeof hooks.initializeRoundMarks !== 'function') throw new Error('round mark initializer is not wired');
      hooks.initializeRoundMarks(q.get.get(id), { at: writtenAt });
    }
    q.start.run('running', at, ends, writtenAt, id);
    q.heartbeat.run(writtenAt, CLOCK_OWNER_ID, id);
  });
  go();
  _memo.roundId = null;   // roster membership cache must not outlive the change
  schedule(id);
  armClockHeartbeat();
  hooks.log(`round ${id} started (${players.length} players prepared), bell at ${new Date(ends).toISOString()}`);
  /* A stage left set by hand must not survive the bell. An operator who
     talked over the interval screen and then started round three would have
     had the interval standings sitting on the wall for the whole round.
     `technical` is exempt: it is the deliberate cover, and only the person
     who pressed it takes it down. */
  try {
    const w = wallState();
    if (w.mode !== 'auto' && w.mode !== 'technical') setWall({ mode: 'auto', nextAt: null });
  } catch { /* the round has started either way */ }
  try { hooks.onPhase('round:start', phasePayload(q.get.get(id))); } catch { /* the round has started either way */ }
  return q.get.get(id);
}

/* A finished round owns no open clock interval. The bell and abort closed the
   pause ROWS (audit) but never the clock, so a pause opening milliseconds
   before the end left paused_since set on a done row forever: durable state
   asserting "frozen" about a round that is over. Bank honestly, then clear. */
function closeClockInterval(id, now = Date.now()) {
  const r = q.get.get(id);
  if (!r || !r.paused_since) return;
  now = competitionPauseEndMs(now);
  const pausedSince = competitionPauseStartMs(r.paused_since);
  const add = Math.max(0, now - pausedSince);
  try {
    db.transaction(() => {
      q.setPausedMs.run(competitionDurationMs(r.paused_ms) + add, now, id);
      q.clockPauseClose.run(now, id);
    })();
  } catch { /* terminal path; best effort */ }
}

function abortRound(id, { force = false } = {}) {
  /* Settle anything already due BEFORE aborting. A late timer or a busy event
     loop otherwise leaves a window where an ordinary abort discards a result
     the round had already earned. */
  try { advanceRoundClock(Date.now()); } catch { /* fall through to the checks */ }
  let r = q.get.get(id);
  if (!r) throw new Error('no such round: ' + id);
  if (r.status === 'done' && !force) {
    throw new Error(`round ${id} settled while aborting; refusing to discard a published result`);
  }
  if (r.blocked_reason && !force) {
    throw new Error(`round ${id} is blocked (${r.blocked_reason}); use forceAbort with a reason`);
  }
  /* done and aborted are final. Rewriting a completed round's status would
     mutate a published result's provenance after the fact. */
  if (r.status === 'done' || r.status === 'aborted') {
    throw new Error(`round ${id} is ${r.status}; finished rounds are immutable`);
  }
  clearTimers(id);
  /* An armed round has never opened a ticker. Closing "its" aliases used to
     reach into whatever round actually owned them, so aborting a future round
     could flatten the live round's Boost positions. */
  if (r.status === 'running' && Number(r.format_version) >= 2) {
    let aliases = [];
    try { aliases = JSON.parse(r.boost_opened || '[]').map((b) => b + '-BOOST'); }
    catch { aliases = []; }
    try {
      for (const alias of [...new Set(aliases)]) {
        const result = hooks.closeAlias(alias, { flatten: true, roundId: r.id });
        if (!result || result.skipped) throw new Error(`could not close ${alias}`);
        const residue = hooks.segmentResidue(alias);
        if (!residue || Number(residue.positions) !== 0 || Number(residue.orders) !== 0) {
          throw new Error(`${alias} retained positions or orders`);
        }
      }
    } catch (e) {
      const why = `abort cleanup pending: ${e.message}`.slice(0, 400);
      q.setBlocked.run(why, Date.now(), id);
      hooks.log(`round ${id} abort deferred: ${why}`);
      hooks.onPhase('round:blocked', phasePayload(q.get.get(id)));
      throw new Error(why);
    }
  } else if (r.status === 'running') closeAllAliases(r);
  if (r.status === 'running') stopClockHeartbeat();
  try { const t = _startTimers.get(id); if (t) { clearTimeout(t); _startTimers.delete(id); } } catch {}
  const abortedAt = Date.now();
  db.transaction(() => {
    /* Publishing the already-committed draw on every started abort prevents
       an operator from selectively hiding an inconvenient draw after seeing
       positions or the first Hot reveal. Armed-never-started rounds remain
       sealed because no competition took place. */
    if (r.status === 'running' && Number(r.format_version) >= 2) revealV2Draw(id);
    q.setStatus.run('aborted', abortedAt, id);
  })();
  /* A finished round owns no live price obligations. Kept for audit, closed
     so they can never freeze the NEXT round. */
  closeClockInterval(id);
  try { hooks.closeRoundPauses && hooks.closeRoundPauses(id); } catch {}
  hooks.log(`round ${id} aborted`);
  try { hooks.onPhase('round:aborted', phasePayload(q.get.get(id))); } catch { /* the abort stands either way */ }
  return q.get.get(id);
}

/* ONE QUERY PER TURN, NOT PER CALL.
 *
 * currentRound() was a bare SELECT, and thirteen sites in the engine call it,
 * several from the per-tick position evaluation: every Lazer message, every
 * symbol, every position, a synchronous SQLite round-trip to learn what has
 * not changed since the last one. A 15s CPU profile of the live engine put
 * it at 14% of wall time with another 9% in the .get() beneath it, and the
 * event loop it was starving read Lazer's on-time messages late, called the
 * feed stale, and failed the majors over to Binance every few minutes. The
 * feed was fine; this was the stall.
 *
 * Memoised for exactly one synchronous turn. The cache is dropped on the
 * next microtask, so nothing asynchronous can ever see a stale row, and it
 * is dropped IMMEDIATELY by any statement that writes paper_rounds, so a
 * settle or a block inside the same burst is seen by the very next call.
 * Correct by construction rather than by TTL: a TTL cache that outlived the
 * bell by even 200ms would have let an order into a frozen result, which is
 * the exploit class the write barrier exists to stop. */
let _cur;   // undefined = nothing cached this turn
const bustCurrentRound = () => { _cur = undefined; };
const currentRound = () => {
  if (_cur !== undefined) return _cur;
  _cur = q.running.get() || null;
  queueMicrotask(bustCurrentRound);
  return _cur;
};

/** Operator recovery from a blocked round. Blocking halts every boundary and
 *  every player write, so it needs a deliberate, audited way out: this is it.
 *  Clearing the reason is the ONLY way the clock restarts, and the failed
 *  boundary is reset to retryable in the same transaction so the round
 *  resumes from where it stopped rather than skipping what it missed. */
/* Stop a round from outside the boundary machinery.
 *
 * Used when a scored account cannot be mutated: the engine has lost the
 * ability to treat two identical seats identically, so the round must stop
 * rather than continue and let iteration order decide a result. */
function blockRound(id, reason) {
  const r = q.get.get(id);
  if (!r || r.blocked_reason) return r;
  q.setBlocked.run(String(reason).slice(0, 400), Date.now(), id);
  const after = q.get.get(id);
  hooks.log(`round ${id} BLOCKED: ${reason}`);
  try { hooks.onPhase('round:blocked', phasePayload(after)); } catch { /* the block still stands */ }
  return after;
}

/* THE AUDITED ESCAPE HATCH for a permanently un-priceable settle instant.
 *
 * The fail-closed rule stands: a scored boundary settles at a valid mark at
 * its exact instant, or the round blocks. But when that instant fell inside a
 * price dispute, no valid mark exists at it and never will; clearBlock retries
 * the identical instant forever and the only other exit was forceAbort,
 * discarding the whole show. The owner chose a third path (2026-09-01): the
 * operator may settle at the LAST ACCEPTED OBSERVATION BEFORE the instant,
 * typically seconds earlier, with the deviation recorded on the round and
 * PUBLISHED. Requirements: the round must be blocked on that boundary, a
 * reason must be given, and the deviation text carries the price, the gap and
 * the reason, permanently. */
function settleAtPrior(id, { reason = '' } = {}) {
  if (!String(reason).trim()) throw new Error('settleAtPrior requires a reason');
  const r = q.get.get(id);
  if (!r) throw new Error('no such round: ' + id);
  if (Number(r.format_version) >= 2) {
    throw new Error('settleAtPrior is a legacy synthetic-Hot recovery and cannot modify a two-Hot round');
  }
  if (!r.blocked_reason) throw new Error('the round is not blocked; nothing to override');
  const p = planOf(r);
  /* Only the Hot close settles a scored window from one canonical instant
     today; the bell marks rather than flattens and checkpoints refuse-and-
     retry harmlessly. Scope the override to what it is FOR. */
  const at = p.hotEnd;
  const b = q.bGet.get(id, at);
  if (!b || b.status === 'succeeded') throw new Error('the Hot close is not the blocked boundary');
  const base = r.active_hot_base || r.hot_base;
  if (!base) throw new Error('no Hot market was ever active');
  const dueAt = (b.due_wall_at) || (r.started_at + at + (r.paused_ms || 0));

  /* THE INSTANT MUST ACTUALLY BE DEAD.
     Without this the override was a general-purpose re-settle button: a round
     blocked for an UNRELATED reason (a drawdown write failure, say) could be
     "rescued" at a stale price even though the true instant was perfectly
     priceable, turning a $30 winner into $10. If a valid mark exists, there is
     no dead end and clearBlock is the correct, non-deviating recovery. */
  if (hooks.markAtStrict) {
    const real = hooks.markAtStrict(base, dueAt, r);
    if (Number.isFinite(real) && real > 0) {
      throw new Error(`the settle instant IS priceable (${real}); use clearBlock, which settles at the true price without a deviation`);
    }
  }
  /* Something must actually be settled. Reporting success and publishing a
     deviation for a segment with no open positions is a false audit record. */
  const openPositions = hooks.aliasPositions ? hooks.aliasPositions(base + '-HOT') : 1;
  if (!openPositions) {
    throw new Error('no open positions on the Hot ticker; nothing to settle, retry with clearBlock');
  }

  const prior = hooks.markBefore ? hooks.markBefore(base, dueAt) : null;
  if (!prior) throw new Error('no accepted observation exists before the settle instant');
  /* And it must be a price from INSIDE the window being settled. With thin
     history the "last accepted observation" could predate the round itself:
     one probe settled a 30-second Hot window at a price from 160 seconds
     before the round opened. A segment is settled at its own prices or not
     at all. */
  /* The window's own opening instant, read from the boundary that stamped it
     rather than recomputed. `paused_ms` is the round's TOTAL pause, so adding
     it here charged the window's open with every second frozen AFTER it
     opened, and a mid-Hot outage is the canonical reason this override
     exists. A 60s pause banked mid-window pushed the recomputed wall past
     observations that were genuinely inside it, and the desk was told to
     abort a rescuable round. `dueAt` above already reads the stamp for
     exactly this reason; so does this.
     The fallback drops paused_ms rather than adding it: without the stamp we
     only have bounds, and the un-inflated one is the bound that can only
     ever accept a slightly early price, never falsely reject an in-window
     one. It still sits far above the fault the guard exists to catch, which
     was a price from before the ROUND. */
  const bOpen = q.bGet.get(id, p.hotStart);
  const hotOpenWall = (bOpen && bOpen.due_wall_at) || (r.started_at + p.hotStart);
  if (prior.t < hotOpenWall) {
    throw new Error(`the last accepted price (${new Date(prior.t).toISOString()}) predates the Hot window itself; there is no in-window price to settle at, abort and re-arm`);
  }
  const gapMs = dueAt - prior.t;
  hooks.closeAlias(base + '-HOT', { roundId: id, settleAt: dueAt, overrideMark: prior.px });
  /* The outage that blocked the close is discharged BY this override: leaving
     it open would re-block the next boundary for an incident the operator has
     already ruled on. */
  try { clearOutage(id, 'hot'); } catch { /* record-keeping only */ }
  q.bMark.run(id, at, 'succeeded', `OPERATOR OVERRIDE: settled at prior observation`, Date.now());
  const line = `Hot close settled by operator at the last accepted price BEFORE its instant: ${prior.px} from ${gapMs}ms earlier (no valid price existed at the instant itself). Reason: ${String(reason).slice(0, 200)}`;
  const existing = r.settle_deviation ? r.settle_deviation + '\n' : '';
  db.prepare('UPDATE paper_rounds SET settle_deviation = ?, updated_at = ? WHERE id = ?')
    .run(existing + line, Date.now(), id);
  /* Only discharge the block if the block was ABOUT this close. An override
     that silently cleared an unrelated blocked_reason resumed a round whose
     actual fault nobody had looked at. */
  /* Matched on boundary IDENTITY, not on reason prose. The prose test was
     defeated by the data the reason quotes: an unrelated fault on a holder of
     `ETH-HOT` ("drawdown sample write failed: ... for ETH-HOT holder")
     contains "hot", so the override silently discharged a block nobody had
     triaged, which is the precise failure this check exists to prevent.
     A boundary failure is worded by fireBoundary and nothing else, so the
     prefix is a fact about which boundary blocked the round rather than a
     guess about what the words mean. */
  const wasHotBlock = String(r.blocked_reason || '').startsWith(`boundary ${at} failed:`);
  if (wasHotBlock) q.setBlocked.run(null, Date.now(), id);
  else hooks.log(`round ${id} settled the Hot close, but the round stays BLOCKED on its own cause: ${r.blocked_reason}`);
  hooks.log(`round ${id} ${line}`);
  hooks.onPhase('hot:close', q.get.get(id));
  /* the rest of the round resumes from here */
  schedule(id);
  advanceRoundClock(Date.now());
  return { round: q.get.get(id), deviation: line, priorPx: prior.px, gapMs };
}

function clearBlock(id, { note = '' } = {}) {
  const r = q.get.get(id);
  if (!r) throw new Error('no such round: ' + id);
  if (!r.blocked_reason) return r;
  /* The plan-drift block is TERMINAL, and saying so in the reason string was
     not enforcement: clearBlock lifted any reason, and one operator note
     re-armed every boundary at the NEW plan's offsets, which is exactly the
     replay the block exists to prevent. Checked against the DATA, not the
     string, so the refusal cannot rot when the wording changes. */
  if (r.plan_json && r.plan_json !== JSON.stringify(scaledPlan(r.kind, r.speed || 1))) {
    throw new Error('this round was armed under a different phase plan; recovery would replay boundaries at the wrong instants. Abort and re-arm.');
  }

  /* Recovery RETRIES, it does not merely unlock. Clearing the flag and hoping
     a later timer finished the job left rounds "unblocked but inert": the
     failed boundary never ran, so the checkpoint it owed never existed while
     the round looked healthy again.
     The block is lifted first so the retry can run, and re-applied by
     fireBoundary itself if the cause has not actually been fixed. */
  const was = r.blocked_reason;
  db.transaction(() => {
    /* 'pending' too, not only 'failed'. A hot-open that deferred to its
       250ms retry chain is marked pending; if the round then blocks for an
       UNRELATED reason, the chain exits silently and fireBoundary skips
       pending forever, so after recovery the segment could never open until
       a full process restart. Recovery must make every unresolved boundary
       retryable, whatever state the block interrupted it in. */
    db.prepare("UPDATE paper_round_boundaries SET status = 'retryable' WHERE round_id = ? AND status IN ('failed', 'pending')").run(id);
    q.setBlocked.run(null, Date.now(), id);
  })();
  hooks.log(`round ${id} recovery attempt${note ? ' (' + note + ')' : ''}, was: ${was}`);

  // replay every boundary that is due and unresolved, oldest first
  advanceRoundClock(Date.now());

  const after = q.get.get(id);
  if (after.blocked_reason) {
    hooks.log(`round ${id} recovery FAILED, still blocked: ${after.blocked_reason}`);
    hooks.onPhase('round:blocked', phasePayload(after));
    return after;
  }
  if (after.status === 'running') {
    /* The return value matters: previously it was discarded, so recovery
       reported success with a required segment ticker still shut. */
    const missing = rehydrateGates(after);
    if (missing.length) {
      const why = `required segment ticker(s) still unavailable: ${missing.join(', ')}`;
      q.setBlocked.run(why, Date.now(), id);
      hooks.log(`round ${id} recovery INCOMPLETE: ${why}`);
      hooks.onPhase('round:blocked', phasePayload(q.get.get(id)));
      return q.get.get(id);
    }
    schedule(id);                 // re-arm only the boundaries still ahead
  }
  hooks.log(`round ${id} RECOVERED by operator${note ? ': ' + note : ''}`);
  hooks.onPhase('round:unblocked', phasePayload(after));
  return after;
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
  return roundOverNow(r, now);
}

/** The single answer to "may this user change state right now".
 *
 *  Every write path consults this, foreground and background alike. Previously
 *  only placeOrder knew about the bell, so a player could close a position or
 *  a resting order could fill after the result was frozen. Returns a reason
 *  string when writes are barred, or null when they are allowed. */
function writeBarrier(userId, now = Date.now()) {
  /* Capture membership BEFORE advancing. Advancing may settle the round, and
     currentRound() only returns rows that are still running — so the request
     that fired the bell used to find no round and be admitted, which is
     exactly the mutation the bell was supposed to stop. */
  const before = currentRound();
  const owned = !!(before && before.started_at && inRound(userId, before));
  advanceRoundClock(now);
  if (owned) {
    if (roundOverNow(before, now)) return 'round_settled';
    // The barrier needs only these mutable state bits. Avoid repeatedly
    // deserializing sealed draw/plan/proof metadata for every held position.
    // This remains a fresh same-transaction read after clock advancement.
    const after = q.writeState.get(before.id);
    if (after && after.blocked_reason) return 'round_blocked';
    if (after && after.status !== 'running') return 'round_settled';
  }
  const r = currentRound();
  if (!r || !r.started_at || !inRound(userId, r)) return null;
  if (r.blocked_reason) return 'round_blocked';
  if (roundOverNow(r, now)) return 'round_settled';
  return null;
}

const accountLocked = (userId) =>
  userId != null && !!q.liveForUser.get(Number(userId));

/** Rounds, other than `exceptId`, that currently own this account. Used to
 *  stop a FUTURE armed round from preparing a player who is live in another
 *  one: resetting them there restored the bankroll and bumped the epoch out
 *  from under a running result. */
function otherRoundsOwning(userId, exceptId) {
  return db.prepare(`SELECT r.id, r.status FROM paper_round_players p
                     JOIN paper_rounds r ON r.id = p.round_id
                     WHERE p.user_id = ? AND r.id != ? AND r.status IN ('running')`)
    .all(Number(userId), exceptId);
}
const playersOf = (id) => q.players.all(id);

/* ── invites ──────────────────────────────────────────────────────────────
   The operator names seats; the engine mints one invite per seat; the player
   who opens an invite while signed in binds their account to that seat and
   can then say they are ready.

   Readiness is an indicator and nothing else. It is never consulted by
   startRound, never gates a checkpoint, and never appears in scoring. The
   only thing it changes is what the desk can truthfully show the producer. */

function inviteState(token) {
  const p = token ? q.byInvite.get(String(token)) : null;
  if (!p) return null;
  const r = q.get.get(p.round_id);
  if (!r) return null;
  return { seat: p, round: r };
}

/* Bind a signed-in account to the seat this invite names.

   Deliberately first-come: the invite IS the identity claim, because before
   this the operator had to know a user id that no part of the product ever
   showed them. Re-opening your own invite is idempotent, someone else's is
   refused, and one person cannot hold two seats in the same round. */
function claimInvite(token, userId) {
  const st = inviteState(token);
  if (!st) throw new Error('this invite is not valid');
  const { seat, round } = st;
  if (seat.user_id === userId) {
    /* Idempotent repair. A seat claimed before the transaction above existed
       may have no account behind it, and answering "already yours" without
       checking is what made that state permanent. */
    try { hooks.ensureAccount?.(userId); } catch (e) {
      throw new Error(`seat is yours but its account could not be created: ${e.message}`);
    }
    return { round, seat: q.bySeat.get(round.id, seat.seat), already: true };
  }
  if (round.status !== 'armed') throw new Error(`round ${round.id} is ${round.status}; seats are only claimable before it starts`);
  const held = q.seatOfUser.get(round.id, userId);
  if (held) throw new Error(`you already hold seat ${held.seat + 1} in this round`);
  /* THE LINK IS THE SEAT.
   *
   * A seat already claimed by somebody else used to be a dead end: the
   * operator resent the link and the new player was told to go and ask the
   * operator. Whoever holds the link holds the seat, so a later claim takes it
   * over and the previous holder's ready state goes with it. Only before the
   * bell: once the round is running the roster is what the result is computed
   * from, and the status check above already refuses that. */
  const takenFrom = seat.user_id;
  /* One transaction. The seat used to be committed before the account was
     created, so a failure in between left a seat owned by a user with no
     paper account, and re-opening the invite answered {already:true} and never
     retried, stranding that player permanently. */
  let changes = 0;
  db.transaction(() => {
    const res = takenFrom == null
      ? q.claimSeat.run(userId, Date.now(), round.id, seat.seat)
      : q.retakeSeat.run(userId, Date.now(), round.id, seat.seat);
    changes = res.changes;
    /* Zero rows means the seat moved between the read and the write. The
       UNIQUE index is what actually prevents one player holding two seats;
       this reports it. */
    if (!changes) throw new Error('that seat was just taken');
    hooks.ensureAccount?.(userId);
  })();
  if (takenFrom != null) hooks.log?.(`round ${round.id}: seat ${seat.seat + 1} taken over by user ${userId} from user ${takenFrom}`);
  return { round, seat: q.bySeat.get(round.id, seat.seat), already: false, tookOverFrom: takenFrom };
}

/* Toggleable on purpose: a player who steps away should be able to say so, or
   the indicator stops meaning anything by the third round of the day. */
function setInviteReady(token, userId, ready) {
  const st = inviteState(token);
  if (!st) throw new Error('this invite is not valid');
  const { seat, round } = st;
  if (seat.user_id === null) throw new Error('take the seat first');
  if (seat.user_id !== userId) throw new Error('that seat is not yours');
  /* Readiness is a pre-show attendance signal. Once the bell has rung the
     stored roster is an audit record, not a live preference that an old
     invite may rewrite after the result. */
  if (round.status !== 'armed') {
    throw new Error(`round ${round.id} is ${round.status}; readiness is only editable before it starts`);
  }
  q.setReady.run(ready ? Date.now() : null, round.id, seat.seat);
  return q.bySeat.get(round.id, seat.seat);
}

/* The leverage a round was armed under, never the deployed constant. */
const boostLeverageOf = (id) => {
  const r = q.get.get(id);
  return r && r.boost_leverage ? r.boost_leverage : null;
};

/* Readiness without the invite token. The token is how a link claims a seat;
   once you hold one, the terminal you are already signed into is the honest
   place to say you are at the desk, and it should not have to carry a
   capability around to do it. */
function setReadyByUser(userId, ready) {
  const r = db.prepare(
    `SELECT r.* FROM paper_rounds r JOIN paper_round_players p ON p.round_id = r.id
     WHERE p.user_id = ? AND r.status = 'armed' ORDER BY r.created_at DESC LIMIT 1`).get(userId);
  if (!r) throw new Error('you do not hold a seat in an armed round');
  const seat = db.prepare('SELECT * FROM paper_round_players WHERE round_id = ? AND user_id = ?').get(r.id, userId);
  if (!seat) throw new Error('you do not hold a seat in this round');
  q.setReady.run(ready ? Date.now() : null, r.id, seat.seat);
  hooks.log(`round ${r.id}: seat ${seat.seat + 1} is ${ready ? 'ready' : 'away'}`);
  return { round: r.id, seat: seat.seat, ready: !!ready, readiness: readinessOf(r.id) };
}

const readinessOf = (id) => {
  const all = q.players.all(id);
  return {
    claimed: all.filter((p) => p.user_id !== null).length,
    ready: all.filter((p) => p.ready_at !== null).length,
    total: all.length,
  };
};

// ── scheduling ───────────────────────────────────────────────────────────
const _timers = new Map();   // roundId -> [Timeout]
let _clockHeartbeatTimer = null;
function armClockHeartbeat() {
  if (_clockHeartbeatTimer) return;
  const tick = () => {
    _clockHeartbeatTimer = null;
    const r = currentRound();
    if (!r || r.status !== 'running') return;
    if (Number(r.format_version) >= 2 && !r.paused_since) {
      try { q.heartbeat.run(Date.now(), CLOCK_OWNER_ID, r.id); }
      catch (e) {
        hooks.log(`round ${r.id}: active-clock heartbeat failed (${e.message})`);
        try { blockRound(r.id, `active-clock heartbeat could not be persisted: ${e.message}`); } catch {}
      }
    }
    _clockHeartbeatTimer = setTimeout(tick, CLOCK_HEARTBEAT_MS);
    _clockHeartbeatTimer.unref?.();
  };
  _clockHeartbeatTimer = setTimeout(tick, CLOCK_HEARTBEAT_MS);
  _clockHeartbeatTimer.unref?.();
}
function stopClockHeartbeat() {
  if (_clockHeartbeatTimer) clearTimeout(_clockHeartbeatTimer);
  _clockHeartbeatTimer = null;
}

/** Claim a v2 active clock after a process change and turn the period in which
 * no engine could accept/price trades into paused time before phase checks.
 * The heartbeat cadence makes this conservative by at most one cadence: it
 * can grant a little extra valid trading, never consume outage time. */
function durableBoundarySideEffect(r, row) {
  if (!r || !row || Number(r.format_version) < 2) return false;
  const b = v2BoundaryAt(r, Number(row.at));
  if (!b) return false;
  const players = q.players.all(r.id);
  if (b.kind === 'hotStart') {
    const rows = q.hotRows.all(r.id, b.hotNumber);
    return !!q.hotResolution.get(r.id, b.hotNumber)
      && players.length > 0 && rows.length === players.length;
  }
  if (b.kind === 'hotEnd') {
    const rows = q.hotRows.all(r.id, b.hotNumber);
    return players.length > 0 && rows.length === players.length
      && rows.every((x) => x.ended_at != null);
  }
  if (b.kind === 'boostStart') {
    let opened = [];
    try { opened = JSON.parse(r.boost_opened || '[]'); } catch { opened = []; }
    return opened.length > 0 && players.length > 0
      && players.every((p) => p.boost_frozen_at != null);
  }
  /* Bell is deliberately not promoted here: a final snapshot proves pricing,
     but exact alias cleanup and the terminal status transaction still owe
     work. Its durable due stamp is nevertheless retained for that retry. */
  if (b.kind === 'bell') return finalCheckpointComplete(r);
  return false;
}

function reconcileRestartBoundaries(r) {
  for (const row of q.bAll.all(r.id)) {
    if (row.status === 'succeeded' || row.due_wall_at == null) continue;
    const retryOwned = ['running', 'pending', 'retryable'].includes(row.status);
    if (!retryOwned) continue;
    if (durableBoundarySideEffect(r, row)) {
      const b = v2BoundaryAt(r, Number(row.at));
      if (b && b.kind !== 'bell') {
        q.bMark.run(r.id, Number(row.at), 'succeeded', null, Date.now());
      }
      /* For bell cleanup, keep the exact checkpoint instant even though the
         terminal edge is not complete yet. */
    } else {
      /* A killed process persisted the proposed wall instant but none of the
         action it was meant to price. Restart downtime shifts that active
         boundary; retaining the stale stamp either requires vanished
         in-memory history or double-counts the recovery pause. */
      q.bClearDue.run(r.id, Number(row.at));
    }
  }
}

function claimClockAfterRestart(r, now = Date.now()) {
  if (!r || Number(r.format_version) < 2 || r.status !== 'running') return r;
  if (r.clock_owner_id === CLOCK_OWNER_ID) return r;
  now = competitionMs(now);
  reconcileRestartBoundaries(r);
  r = q.get.get(r.id);
  /* An already-open durable price pause accounts for the whole interval from
     paused_since, including the restart. Merely transfer the lease. */
  if (r.paused_since) {
    const pausedSince = competitionPauseStartMs(r.paused_since);
    db.transaction(() => {
      /* A pre-fix fractional open edge may be inherited across this restart.
         Moving it backwards to the containing whole millisecond is both
         conservative and consistent with pauseClockOpen's audit semantics. */
      if (pausedSince !== Number(r.paused_since)) {
        q.setPausedSince.run(pausedSince, now, r.id);
        q.clockPauseBackdate.run(pausedSince, r.id);
      }
      q.clockPauseOpen.run(r.id, pausedSince, 'runtime');
      q.heartbeat.run(now, CLOCK_OWNER_ID, r.id);
    })();
  } else {
    const beat = competitionMs(r.clock_heartbeat_at, now);
    const observedGap = beat > 0 ? Math.max(0, now - beat) : 0;
    /* A committed boundary is stronger evidence of liveness than an older
       periodic heartbeat. Banking the whole heartbeat gap can rewind active
       time behind a Hot/Boost boundary that already revealed/froze/opened;
       succeeded boundaries are intentionally never replayed, so that would
       lengthen Hot or permanently close Boost on rehydrate. Cap the bank so
       the clock never moves behind the furthest durable successful edge. */
    const reached = q.bAll.all(r.id)
      .filter((b) => ['running', 'pending', 'retryable', 'succeeded'].includes(b.status))
      .reduce((m, b) => Math.max(m, Number(b.at) || 0), 0);
    const before = activeElapsed(r, now);
    const downtime = Math.min(observedGap, Math.max(0, before - reached));
    db.transaction(() => {
      q.claimClock.run(downtime, downtime, now, CLOCK_OWNER_ID, now, r.id);
      if (downtime > 0) q.clockPauseClosed.run(r.id, now - downtime, now, 'restart');
    })();
    if (downtime) {
      hooks.log(`round ${r.id}: active clock restored after engine restart; banked ${downtime}ms unavailable time`
        + (downtime < observedGap ? ` (capped at durable boundary ${reached}ms)` : ''));
    }
  }
  /* Booting is not recovery. Keep the active clock continuously frozen from
     the last heartbeat until the canonical roster and any active segment are
     priceable again. paper.js owns the durable generic obligation and clears
     it only after that proof. */
  if (typeof hooks.pauseForRestart === 'function') {
    hooks.pauseForRestart('engine restarted; validating competition prices and gates', now);
  } else {
    pauseClockOpen(now);
    blockRound(r.id, 'engine restart recovery pause is not wired');
  }
  return q.get.get(r.id);
}

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
  for (const at of boundariesOf(r)) {
    /* Remaining ACTIVE time, not wall time. A round that has been frozen for
       four minutes still owes every boundary its full tradeable gap. */
    const delay = at - activeElapsed(r);
    if (delay <= 0) { fireBoundary(id, at); continue; }
    const t = setTimeout(() => fireBoundary(id, at), delay);
    /* Do not hold the process open on our account. The engine is a long-lived
       server that stays alive regardless, and the clock is advanced by events
       anyway; an unreffed timer means a stray round can never keep a script
       (or a test run) alive for the length of a round. */
    t.unref?.();
    ts.push(t);
  }
  _timers.set(id, ts);
}

/* Which boundaries can still be honestly executed, and which are windows
   that have simply been missed.
 *
 * Replaying everything merely "due" let recovery open a Hot segment after its
 * window had closed, shut it immediately, and record BOTH boundaries as
 * succeeded. The durable record then claimed a four-minute segment that never
 * took place. Opening a window is only meaningful while the window is open;
 * settling and scoring can always be retried, because they price a moment
 * that has already passed.
 */
function boundaryPolicy(r, at) {
  const p = planOf(r);
  const d = privateDrawOf(r);
  if (d) {
    for (const n of [1, 2]) {
      const h = d[`hot${n}`];
      if (at === h.activation) return { window: `hot${n}`, label: `Hot ${n} open`, endsAt: h.activation + p.hotDuration };
    }
    if (at === p.boostStart) return { window: 'boost', label: 'Boost open', endsAt: p.total };
    return { window: null, label: 'boundary' };
  }
  if (at === p.hotStart) return { window: 'hot', label: 'Hot open' };
  if (at === p.boostStart) return { window: 'boost', label: 'Boost open' };
  return { window: null, label: 'settlement' };   // firstFive, reveal, hotEnd, bell
}
function v2BoundaryAt(r, at) {
  const d = privateDrawOf(r), p = planOf(r);
  if (!d) return null;
  for (const n of [1, 2]) {
    const h = d[`hot${n}`];
    if (at === h.activation - p.hotWarning) return { kind: 'warning', hotNumber: n };
    if (at === h.activation) return { kind: 'hotStart', hotNumber: n };
    if (at === h.activation + p.hotDuration) {
      return { kind: 'hotEnd', hotNumber: n, startsFinalBuild: at === p.finalBuildStart };
    }
  }
  if (at === p.finalBuildStart) return { kind: 'finalBuild', hotNumber: null };
  if (at === p.boostStart) return { kind: 'boostStart', hotNumber: null };
  if (at === p.total) return { kind: 'bell', hotNumber: null };
  return null;
}
/* Is the phase this boundary opens still live? */
function windowStillOpen(r, at, now = Date.now()) {
  const pol = boundaryPolicy(r, at);
  if (!pol.window) return true;
  /* Ask whether the window has ENDED, not whether we are exactly inside it.
   *
   * This previously compared the phase name at `now` against the boundary's
   * window, which meant a timer that fired a millisecond EARLY was judged to
   * have missed. That is not hypothetical: in the drill the Hot open boundary
   * fired 2ms before its due instant, phaseAt still said `reveal`, and the
   * engine blocked the entire round for being too punctual. Being early is
   * not a missed window, and the segment is priced at its scheduled instant
   * regardless, so the only real failure is arriving after the window is
   * gone. */
  const p = planOf(r);
  const windowEnds = pol.endsAt || (pol.window === 'hot' ? p.hotEnd : p.total);
  return activeElapsed(r, now) < windowEnds;
}

/* A boundary runs at most once SUCCESSFULLY. Marking it fired before the
   action ran meant a transient failure was permanent within the process and
   invisible after a restart; now the durable status decides, so a failed
   boundary can be retried and a succeeded one is never replayed. */
function fireBoundary(id, at, now = Date.now()) {
  now = competitionMs(now);
  const prev = q.bGet.get(id, at);
  if (prev && prev.status === 'succeeded') return;
  if (prev && prev.status === 'running') return;      // re-entrancy guard
  if (prev && prev.status === 'pending') return;      // an async open owns it
  let r = q.get.get(id);
  if (!r || r.status !== 'running') return;
  /* Boundary timers call this function directly, bypassing
     advanceRoundClock(). Check source deadlines here as well. If the price
     interval ends at the boundary, the pause owns that instant and the
     boundary remains uncommitted until recovery; a boundary strictly before
     a later outage may still settle from its immutable historical mark. */
  if (Number(r.format_version) >= 2 && typeof hooks.ensureClockHealth === 'function') {
    try { hooks.ensureClockHealth(now); }
    catch (e) {
      q.setBlocked.run(`competition clock health check failed: ${e.message}`.slice(0, 400), Date.now(), id);
      hooks.onPhase('round:blocked', phasePayload(q.get.get(id)));
      return;
    }
    r = q.get.get(id);
    if (!r || r.status !== 'running') return;
    if (r.paused_since && activeElapsed(r, Number(r.paused_since)) <= Number(at)) return;
  }
  /* EARLY IS NOT DUE.
   *
   * Relaxing the window check so that a hair-early timer was not treated as a
   * MISSED window left the opposite hole: the callback then went on to EXECUTE
   * the boundary early. A timer arriving 249ms before the bell froze the final
   * result 249ms early. Timers are not precise and never will be, so the
   * boundary itself has to hold the line: early means wait and re-arm, due
   * means execute, past an opening window means block. */
  /* Measured in ACTIVE time. A frozen round does not advance toward its
     boundaries, so while paused this re-arms on the remaining active gap and
     the pause release re-schedules from the new clock. */
  const activeNow = activeElapsed(r, now);
  /* Exclude an interval still open. If this first fire happens while frozen,
     the active clock reached the boundary before that pause began; charging
     the open interval would move an exact scored instant into invalid time. */
  const due = bankedDueInstant(r, at);
  if (r.paused_since && prev && prev.status === 'retryable'
      && prev.error === 'competition price unavailable at boundary') {
    return;   // pauseClockClose reschedules the shifted boundary
  }
  if (activeNow < at) {
    /* While the clock is FROZEN, do not re-arm: activeNow is constant, so a
       boundary frozen N ms short re-armed every N ms, and every sweep tick
       appended more duplicates, thousands of timers over a long outage.
       pauseClockClose reschedules everything the moment the clock resumes. */
    if (r.paused_since) return;
    const t = setTimeout(() => fireBoundary(id, at), Math.max(1, at - activeNow));
    t.unref?.();
    const ts = _timers.get(id) || [];
    ts.push(t); _timers.set(id, ts);
    return;
  }
  /* A blocked round is stopped, not merely annotated. Previously the reason
     was recorded and every later boundary still fired, so a round that could
     not prove its draw went on to open Boost and mark itself done. */
  if (r.blocked_reason) {
    /* STAMP BEFORE SKIPPING. A boundary that went due while the round was
       blocked for an unrelated reason was skipped without recording its
       instant; a pause banked during the block then inflated the stamp the
       eventual first fire computed, settling a scored window late. The 'due'
       status is inert to the guards above (only succeeded/running/pending
       short-circuit) and counts as missed on resume, which it genuinely is. */
    if (!prev) q.bMark.run(id, at, 'due', null, Date.now());
    if (!prev || !prev.due_wall_at) {
      q.bDue.run(Math.min(now, due), id, at);
    }
    hooks.log(`round ${id} boundary ${at} skipped: blocked (${r.blocked_reason})`);
    return;
  }
  const p = planOf(r);
  /* Refuse to "open" a segment whose window has gone. Better a recorded gap
     than a fabricated segment. */
  if (!windowStillOpen(r, at, now)) {
    const pol = boundaryPolicy(r, at);
    const why = `${pol.label} boundary ${at} missed: its window has closed`;
    q.bMark.run(id, at, 'missed', why, Date.now());
    q.setBlocked.run(why, Date.now(), id);
    hooks.log(`round ${id} BLOCKED: ${why}`);
    hooks.onPhase('round:blocked', phasePayload(q.get.get(id)));
    return;
  }
  q.bMark.run(id, at, 'running', null, Date.now());
  let dueAt = (prev && prev.due_wall_at)
    ? competitionMs(prev.due_wall_at)
    : Math.min(now, due);
  try {
    /* The settle instant is captured at FIRST fire and reused on retries.
       Recomputing it after a block+pause+recovery inflated it by every pause
       banked since the true instant. bMark's INSERT OR REPLACE wipes columns,
       so bDue re-stamps after every mark, first-write-wins via the IS NULL
       guard reading through prev. */
    /* The stamp must be the wall instant the active clock REACHED this
       boundary, and if we are firing during an open pause, the boundary went
       due BEFORE that pause began (a frozen clock cannot advance to it), so
       the open interval must not be counted. dueInstant includes it, and a
       first fire mid-pause stamped the instant inflated by the whole outage,
       permanently, settling a scored segment at a price from minutes later.
       Capped at now because a due instant cannot be in the future. */
    q.bDue.run(dueAt, id, at);
    let deferred = false;
    const v2b = v2BoundaryAt(r, at);
    if (v2b) {
      if (v2b.kind === 'warning') hotWarning(id, v2b.hotNumber);
      else if (v2b.kind === 'hotStart') openHotV2(id, v2b.hotNumber, dueAt);
      else if (v2b.kind === 'hotEnd') {
        closeHotV2(id, v2b.hotNumber, dueAt);
        if (v2b.startsFinalBuild) hooks.onPhase('finalBuild:start', { id, formatVersion: 2 });
      }
      else if (v2b.kind === 'finalBuild') hooks.onPhase('finalBuild:start', { id, formatVersion: 2 });
      else if (v2b.kind === 'boostStart') openBoost(id, dueAt);
      else if (v2b.kind === 'bell') bell(id, dueAt);
    } else if (at === p.firstFive) { snapshot(id, 'firstFive', dueAt); }
    else if (at === p.reveal) { drawHotMarket(id); }
    else if (at === p.hotStart) { deferred = openHot(id) === DEFERRED; }
    else if (at === p.hotEnd) { closeHot(id, dueAt); }
    else if (at === p.boostStart) { openBoost(id, dueAt); }
    else if (at === p.total) { bell(id, dueAt); }
    /* A deferred opening has not happened yet, so it must not be recorded as
       having happened. Its own retry owns the final transition. */
    const finalStatus = deferred ? 'pending' : 'succeeded';
    const finalError = deferred ? 'waiting for a competition-valid price' : null;
    /* Boundary status and the liveness lease advance together. A crash after
       Hot/Boost side effects but before the periodic heartbeat must not make
       restart accounting rewind behind the side effect. */
    db.transaction(() => {
      q.bMark.run(id, at, finalStatus, finalError, Date.now());
      if (Number(r.format_version) >= 2 && !deferred) {
        q.heartbeat.run(Date.now(), CLOCK_OWNER_ID, id);
      }
    })();
    /* Opening callbacks run before this durable status edge, so their price
       timer sees the prior effective phase. Re-notify after commit: paper can
       now arm the Hot/Boost dependency's own exact expiry and compact clients
       receive a state whose side effects and phase agree. */
    if (Number(r.format_version) >= 2 && !deferred) {
      hooks.onPhase('boundary:committed', { id, formatVersion: 2, offsetMs: at });
    }
  } catch (e) {
    if (Number(r.format_version) >= 2 && e && e.unpriced) {
      /* Price loss is a pause, not a verdict on the round. Clear the first
         wall stamp so the retry is settled at the shifted active-time
         boundary after recovery; retaining the pre-outage wall instant would
         price the Hot/Boost edge inside a period explicitly removed from the
         competition clock. The persisted reason is deliberately generic:
         at Hot activation the exception may name a still-sealed asset. */
      q.bMark.run(id, at, 'retryable', 'competition price unavailable at boundary', Date.now());
      q.bClearDue.run(id, at);
      if (typeof hooks.pauseForBoundary === 'function') {
        try {
          hooks.pauseForBoundary(dueAt, 'competition price unavailable at boundary');
          hooks.log(`round ${id} boundary ${at} waiting for competition prices; active clock frozen`);
          return;
        } catch (pauseErr) {
          e = new Error(`boundary price pause could not be persisted: ${pauseErr.message}`);
        }
      } else {
        e = new Error('boundary price pause is not wired');
      }
    }
    q.bMark.run(id, at, 'failed', String(e.message).slice(0, 500), Date.now());
    /* Every boundary in this format is REQUIRED: First Five, the draw, Hot
       open and close, Boost open and the bell all decide or enable part of
       the result. A failure that merely logged let the show carry on past a
       segment that never happened. */
    q.setBlocked.run(`boundary ${at} failed: ${e.message}`.slice(0, 400), Date.now(), id);
    hooks.log(`round ${id} BLOCKED, boundary ${at} FAILED: ${e.message}`);
    hooks.onPhase('round:blocked', phasePayload(q.get.get(id)));
  }
}

// ── boundary actions ─────────────────────────────────────────────────────
/* The warning deliberately carries no round row: that row contains the
 * private draw. A future onPhase consumer cannot leak what it never receives. */
function hotWarning(id, hotNumber) {
  hooks.log(`round ${id}: Hot Market ${hotNumber} warning started (asset sealed)`);
  hooks.onPhase('hot:warning', { id, formatVersion: 2, hotNumber });
}

function hotColumns(n) {
  if (n !== 1 && n !== 2) throw new Error('Hot number must be 1 or 2');
  return {
    drawn: `hot${n}_base`, active: `hot${n}_active_base`,
    fallback: `hot${n}_fallback_reason`, revealed: `hot${n}_revealed_at`,
  };
}

function scoreProofRecord(r, player, checkpoint, marks, asOf, economic = null) {
  if (Number(r && r.format_version) < 2) return null;
  const boostCapacityPolicy = boostCapacityPolicyOf(r);
  if (typeof hooks.scoreProofFor !== 'function') {
    throw new Error('competition engine not wired: scoreProofFor');
  }
  const state = hooks.scoreProofFor(player.user_id, player.epoch,
    player.start_balance, marks, asOf, r);
  if (!state || typeof state !== 'object' || Array.isArray(state)) {
    throw new Error(`score proof unavailable for seat ${player.user_id}`);
  }
  const proof = {
    version: 1,
    roundId: r.id,
    ...(boostCapacityPolicy === CURRENT_EQUITY_BOOST_POLICY ? { boostCapacityPolicy } : {}),
    ...backupExecutionPolicyFields(r),
    userId: Number(player.user_id),
    checkpoint,
    asOf: Number(asOf),
    epoch: Number(player.epoch),
    startBalance: Number(player.start_balance),
    economic,
    state,
  };
  if (!(Number.isSafeInteger(proof.asOf) && proof.asOf > 0
      && Number.isSafeInteger(proof.epoch) && proof.epoch > 0
      && Number.isFinite(proof.startBalance))) {
    throw new Error(`invalid score proof identity for seat ${player.user_id}`);
  }
  const raw = JSON.stringify(proof);
  return {
    proof, raw,
    sha256: crypto.createHash('sha256').update(raw, 'utf8').digest('hex'),
  };
}

function insertScoreProof(r, player, checkpoint, marks, asOf, economic = null) {
  const record = scoreProofRecord(r, player, checkpoint, marks, asOf, economic);
  if (!record) return;
  q.scoreProofIns.run(r.id, player.user_id, checkpoint, asOf,
    player.epoch, player.start_balance, record.raw, record.sha256, Date.now());
}

/* Choose only from committed material. Backup order is tried first; the other
 * primary is a deterministic last resort because it, too, was locked in the
 * reveal envelope. Only the asset that actually ran for Hot #1 is forbidden
 * for Hot #2. This matters with a three-market pool: if C replaced A for Hot
 * #1 and B is later down, recovered A is a valid, fair Hot #2 fallback. */
function priceUnavailable(message, symbol = null) {
  const e = new Error(message);
  e.unpriced = true;
  if (symbol) e.symbol = symbol;
  return e;
}

function requiredLeverage(r, kind) {
  return kind === 'BOOST' ? (Number(r && r.boost_leverage) || 500) : COMP_BASE_LEV;
}

function historicalReadyAt(base, at, kind = 'HOT', r = null) {
  const leverage = requiredLeverage(r, kind);
  if (typeof hooks.marketReadyAt === 'function' && Number.isFinite(Number(at))) {
    return !!hooks.marketReadyAt(base, Number(at), kind, leverage, r);
  }
  return kind === 'BOOST' && hooks.marketReadyForBoost
    ? !!hooks.marketReadyForBoost(base, leverage, r)
    : !!hooks.marketReady(base, r);
}

function readyAt(base, at, kind = 'HOT', r = null) {
  /* Opening/retry needs both facts: the scheduled instant was priceable and
     contestants can trade the market now. Settlement differs: Hot close and
     bell may be processed late from a historically valid due mark even if a
     separate outage began afterwards. */
  const liveReady = kind === 'BOOST' && hooks.marketReadyForBoost
    ? !!hooks.marketReadyForBoost(base, requiredLeverage(r, kind), r)
    : !!hooks.marketReady(base, r);
  return liveReady && historicalReadyAt(base, at, kind, r);
}

function hotChoices(r, hotNumber) {
  const d = privateDrawOf(r);
  if (!d) throw new Error(`round ${r.id}: sealed draw unavailable`);
  const hotDraw = d[`hot${hotNumber}`];
  const drawn = hotDraw.asset;
  const prior = hotNumber === 2 ? (r.hot1_active_base || r.hot1_base) : null;
  const forbidden = new Set([prior].filter(Boolean));
  const fallbackOrder = Array.isArray(hotDraw.fallbackOrder) ? hotDraw.fallbackOrder : [];
  const committedOrder = [drawn, ...fallbackOrder].filter((x, i, a) =>
    x && a.indexOf(x) === i);
  const skipped = committedOrder.filter((asset) => forbidden.has(asset))
    .map((asset) => ({ asset, reason: 'already_used_hot1' }));
  return { drawn, committedOrder, skipped,
    candidates: committedOrder.filter((asset) => !forbidden.has(asset)) };
}

function chooseHotAsset(r, hotNumber, dueAt = Date.now()) {
  const { drawn, committedOrder, skipped, candidates } = hotChoices(r, hotNumber);
  const attempts = [];
  for (const candidate of candidates) {
    let evidence = null;
    if (typeof hooks.marketEvidenceAt === 'function') {
      try { evidence = hooks.marketEvidenceAt(candidate, Number(dueAt), 'HOT', COMP_BASE_LEV, r); }
      catch (e) { evidence = { ready: false, error: String(e && e.message || e).slice(0, 160) }; }
    }
    /* Fallback is decided only by the immutable exact-activation evidence.
       A timer callback may be delayed until after a market subsequently
       fails; that must pause the already-selected primary, not redraw the
       event onto a backup that was never entitled to replace it at zero. */
    const isReady = evidence && typeof evidence.historicalReady === 'boolean'
      ? evidence.historicalReady : historicalReadyAt(candidate, dueAt, 'HOT', r);
    attempts.push({ asset: candidate, ready: !!isReady, ...(evidence ? { evidence } : {}) });
    if (isReady) {
      const selectionCause = candidate === drawn ? 'drawn'
        : skipped.some((x) => x.asset === drawn) ? 'already_used_hot1'
          : 'invalid_at_activation';
      const fallbackReason = candidate === drawn ? null
        : selectionCause === 'already_used_hot1'
          ? `${drawn} already ran as Hot Market 1; used the next committed distinct market`
          : `${drawn} was not competition-ready at activation; used the next committed backup`;
      return {
        drawn, active: candidate,
        reason: fallbackReason,
        resolution: {
          version: 1, hotNumber, activationAt: Number(dueAt),
          committedOrder, eligibleOrder: candidates.slice(), skipped,
          attempts, selected: candidate, selectionCause, fallbackReason,
        },
      };
    }
  }
  throw priceUnavailable(`Hot ${hotNumber} cannot activate: committed market order is not priceable`);
}

/** Cumulative selected-family PnL delta is the Hot bonus invariant. At the
 * opening mark we freeze realised+bad-debt plus current marked PnL; at close
 * we freeze the same cumulative quantity. Their difference counts once more
 * on the leaderboard and nowhere in account equity. */
function openHotV2(id, hotNumber, dueAt) {
  const r = q.get.get(id);
  if (!r || Number(r.format_version) < 2) throw new Error(`round ${id} is not a two-Hot round`);
  const existing = q.hotRows.all(id, hotNumber);
  const players = q.players.all(id);
  if (existing.length) {
    if (existing.length !== players.length) throw new Error(`Hot ${hotNumber} baseline is partial (${existing.length}/${players.length})`);
    /* A process may die after the baseline/reveal transaction commits but
       before the boundary row is marked succeeded. The retry must not turn
       that durable partial success into an unavailable live Hot merely
       because the baseline is already complete. Revalidate the already
       revealed asset (never redraw it) before completing the boundary. */
    const activeBase = existing[0] && existing[0].base;
    if (!activeBase || existing.some((row) => row.base !== activeBase)
        || !readyAt(activeBase, Date.now(), 'HOT', r)) {
      throw priceUnavailable(`Hot ${hotNumber} cannot resume: revealed market is not priceable`, activeBase);
    }
    return existing;
  }
  const asOf = Number(dueAt) || Date.now();
  let picked;
  const recordedResolution = q.hotResolution.get(id, hotNumber);
  if (recordedResolution) {
    let resolution;
    try { resolution = JSON.parse(recordedResolution.evidence_json); }
    catch { throw new Error(`Hot ${hotNumber} activation evidence is unreadable`); }
    if (!resolution || resolution.selected !== recordedResolution.active) {
      throw new Error(`Hot ${hotNumber} activation evidence is inconsistent`);
    }
    picked = {
      drawn: recordedResolution.drawn,
      active: recordedResolution.active,
      reason: resolution.fallbackReason || null,
      resolution,
    };
  } else {
    picked = chooseHotAsset(r, hotNumber, asOf);
    /* Lock the first exact-activation-ready choice before consulting current
       liveness. If it goes unavailable after zero, retries must preserve this
       winner rather than walking farther down the fallback order. */
    q.hotResolutionIns.run(id, hotNumber, asOf, picked.drawn, picked.active,
      JSON.stringify(picked.resolution), Date.now());
  }
  if (!readyAt(picked.active, Date.now(), 'HOT', r)) {
    throw priceUnavailable(`Hot ${hotNumber} selected market is awaiting a competition-valid live price`, picked.active);
  }
  const marks = hooks.markSetFor(players.map((p) => p.user_id), asOf, { strict: true, round: r });
  const values = players.map((p) => ({ p,
    value: Number(hooks.hotValueOf(p.user_id, picked.active, p.epoch, marks, asOf)) }));
  for (const x of values) {
    if (!Number.isFinite(x.value)) throw priceUnavailable(`Hot ${hotNumber} baseline is not priceable`);
  }
  const c = hotColumns(hotNumber);
  db.transaction(() => {
    for (const x of values) {
      q.hotStartIns.run(id, hotNumber, x.p.user_id, picked.active, asOf, x.value);
      insertScoreProof(r, x.p, `hot${hotNumber}Start`, marks, asOf,
        { kind: 'hot', number: hotNumber, edge: 'start', base: picked.active, value: x.value });
    }
    db.prepare(`UPDATE paper_rounds SET ${c.drawn} = ?, ${c.active} = ?, ${c.fallback} = ?,
                  ${c.revealed} = ?, hot_base = ?, active_hot_base = ?, fallback_reason = ?,
                  draw_at = ?, updated_at = ? WHERE id = ?`)
      .run(picked.drawn, picked.active, picked.reason, asOf,
        picked.drawn, picked.active, picked.reason, asOf, Date.now(), id);
  })();
  hooks.log(`round ${id}: Hot Market ${hotNumber} activated on ${picked.active}`
    + (picked.reason ? ' using committed fallback' : ''));
  hooks.onPhase('hot:open', { id, formatVersion: 2, hotNumber, market: picked.active });
  return q.hotRows.all(id, hotNumber);
}

function closeHotV2(id, hotNumber, dueAt) {
  sampleSegmentsNow(id, (Number(dueAt) || Date.now()) - 1);
  const r = q.get.get(id);
  const openOutages = unresolvedOutages(r).filter((o) => o.segment === `hot${hotNumber}`);
  if (openOutages.length) {
    throw priceUnavailable(`Hot ${hotNumber} ended while its price was unavailable`);
  }
  const rows = q.hotRows.all(id, hotNumber);
  const players = q.players.all(id);
  if (rows.length !== players.length) throw new Error(`Hot ${hotNumber} baseline is incomplete (${rows.length}/${players.length})`);
  if (rows.every((x) => x.ended_at != null)) return rows;
  if (rows.some((x) => x.ended_at != null)) throw new Error(`Hot ${hotNumber} close is partial`);
  const asOf = Number(dueAt) || Date.now();
  const activeBase = rows[0] && rows[0].base;
  if (!activeBase || !historicalReadyAt(activeBase, asOf, 'HOT', r)) {
    throw priceUnavailable(`Hot ${hotNumber} close cannot be priced`, activeBase);
  }
  const marks = hooks.markSetFor(players.map((p) => p.user_id), asOf, { strict: true, round: r });
  const end = rows.map((row) => {
    const p = players.find((x) => Number(x.user_id) === Number(row.user_id));
    const value = Number(hooks.hotValueOf(row.user_id, row.base, p && p.epoch, marks, asOf));
    if (!Number.isFinite(value)) throw priceUnavailable(`Hot ${hotNumber} close is not priceable`, row.base);
    return { row, p, value, bonus: value - Number(row.start_value) };
  });
  db.transaction(() => {
    for (const x of end) {
      const wrote = q.hotEndUpd.run(asOf, x.value, x.bonus, id, hotNumber, x.row.user_id);
      if (wrote.changes !== 1) throw new Error(`Hot ${hotNumber} close raced for seat ${x.row.user_id}`);
      insertScoreProof(r, x.p, `hot${hotNumber}End`, marks, asOf,
        { kind: 'hot', number: hotNumber, edge: 'end', base: x.row.base, value: x.value });
    }
  })();
  hooks.log(`round ${id}: Hot Market ${hotNumber} complete`);
  hooks.onPhase('hot:close', { id, formatVersion: 2, hotNumber });
  return q.hotRows.all(id, hotNumber);
}

/* One seat's Hot rows, each with its own extra: the frozen bonus once the
   Hot has closed, the live value minus the start value while it is open.
   The aggregate the score uses is the sum; the split is what a player
   reading their history needs. */
function hotBonusRowsFor(r, userId, marks = null, asOf = null) {
  if (!r || Number(r.format_version) < 2) return [];
  const player = q.players.all(r.id).find((p) => Number(p.user_id) === Number(userId));
  if (!player) return [];
  return q.hotRowsForUser.all(r.id, userId).map((row) => {
    let bonus;
    if (row.ended_at != null) bonus = Number(row.bonus) || 0;
    else {
      const value = Number(hooks.hotValueOf(userId, row.base, player.epoch, marks,
        Number.isFinite(asOf) ? asOf : Date.now()));
      if (!Number.isFinite(value)) throw new Error(`Hot ${row.hot_no} live score unavailable for seat ${userId}`);
      bonus = value - Number(row.start_value);
    }
    return { number: Number(row.hot_no), base: row.base, startedAt: Number(row.started_at) || null,
      endedAt: row.ended_at != null ? Number(row.ended_at) : null, bonus };
  });
}

function hotBonusFor(r, userId, marks = null, asOf = null) {
  let bonus = 0;
  for (const row of hotBonusRowsFor(r, userId, marks, asOf)) bonus += row.bonus;
  return bonus;
}

function drawHotMarket(id) {
  const r = q.get.get(id);
  /* If the draw already committed but the process died before the boundary
     was marked, the persisted result is valid and verifiable. Treat it as
     done rather than blocking for a seed we no longer hold in memory. */
  if (r.draw_seed && r.hot_base && verifyDraw(r).ok) {
    hooks.log(`round ${id} draw already persisted and verifies (${r.hot_base})`);
    return;
  }
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
  /* The reveal exists so the drawn market has a beat to be checked before its
     window opens. If it is not competition-ready here, say so now rather than
     discovering it when the segment is already due. */
  if (!hooks.marketReady(market, r)) {
    hooks.log(`round ${id} WARNING: drawn market ${market} is not competition-ready at the reveal`);
    hooks.onPhase('hot:unready', { ...q.get.get(id), market });
  }
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
  let used, why = null;
  try { used = openGate(r.hot_base, id); }
  catch (e) {
    /* Retry asynchronously, never by spinning. An earlier version retried in a
       synchronous busy-loop, which froze the single thread that delivers the
       price updates it was waiting for: the recovery it hoped to observe could
       not happen, and the engine stalled at the exact moment Hot begins.
       The segment is briefly without a gate while we retry, which is honest
       and recorded, and a market that is merely blipping keeps its window. */
    if (!r.hot_backup) throw e;
    /* The retry, not this call, decides how the boundary ends. Marking it
       succeeded here made durable state claim Hot had opened while nothing was
       open, so a crash inside the grace period would skip the boundary on
       restart and a later failure could not correct the record. */
    noteOutage(id, 'hot', r.hot_base + '-HOT', e.message);
    /* Never retry past the window we are trying to open. Whatever the grace,
       a segment that has run out of time is a lost segment and must be
       reported as one rather than opened moments before it closes. */
    /* Grace and window both in ACTIVE time. On wall time, banked pause made
       wall-elapsed exceed wall-hotEnd while the active window had minutes
       left: one blip on the drawn market then declared "unavailable for the
       whole window" and blocked the round, or forced a premature fallback to
       the backup. A pause during the retry now extends the retry, which is
       what pause-and-extend means. */
    const activeDeadline = Math.min(activeElapsed(r) + HOT_OPEN_GRACE_MS, planOf(r).hotEnd);
    const deadline = activeDeadline;   // consumed as an ACTIVE offset by the retry
    hooks.log(`round ${id} ${r.hot_base} not ready at Hot start (${e.message}); retrying for ${Math.max(0, deadline - activeElapsed(r))}ms of active time`);
    scheduleHotOpen(id, r.hot_base, r.hot_backup, deadline);
    return DEFERRED;   // the retry, not the boundary, records what opened
  }
  // record what TRADED; the drawn result stays untouched so the proof holds
  q.setActive.run(used, why, Date.now(), id);
  hooks.onPhase('hot:open', q.get.get(id));
}

/* SAMPLE BEFORE DECIDING, DO NOT WAIT TO BE TOLD.
 *
 * Segment health was only observed when the round clock advanced — from a
 * tick, a request, or the 5s sweep. A quiet tail outage in the last seconds of
 * Hot or Boost could therefore never become a durable event before the
 * boundary fired, so the bell settled a normal result over a segment that had
 * been unpriceable at the moment it mattered. The deciding boundaries now take
 * their own reading first. */
function sampleSegmentsNow(id, atMs = null) {
  const r = q.get.get(id);
  if (!r) return;
  /* Sample the segment that was running UP TO this boundary, not the phase we
     are already in. At the bell the clock has left Boost, so sampling "now"
     watched nothing and a Boost that died in its final seconds was invisible
     to the very check meant to catch it. */
  const at = Number.isFinite(atMs) ? atMs : Date.now();
  /* FAIL CLOSED.
   *
   * This swallowed every error "so sampling can never throw here". But the
   * health verdict IS part of settlement correctness: if the market was
   * unhealthy at the deciding instant and the outage write failed, the round
   * published a normal result over a segment it had just failed to record as
   * broken. A boundary that cannot establish its own health has not
   * established it, and must not settle. */
  monitorSegments(r, at, { historical: true });
}

/** Every segment obligation that is still open. A round may not settle over
 *  one: the window it belongs to did not happen as published. */
function assertObligationsResolved(r, label) {
  const open = unresolvedOutages(r);
  if (open.length) {
    const first = open[0];
    if (Number(r && r.format_version) >= 2) {
      throw priceUnavailable(`${label} with unresolved competition pricing`);
    }
    throw new Error(`${label} with ${open.length} unresolved segment outage(s), including ${first.alias} since ${new Date(first.since).toISOString()}`);
  }
}

function closeHot(id, dueAt = null) {
  sampleSegmentsNow(id, (Number(dueAt) || Date.now()) - 1);
  const r = q.get.get(id);
  /* If Hot's gate was never restored, the segment did not happen as published.
     Better a blocked round an operator must rule on than a result that
     silently omitted a scoring window. */
  const open = unresolvedOutages(r).filter((o) => o.segment === 'hot');
  if (open.length) {
    throw new Error(`Hot ended with its gate still unavailable since ${new Date(open[0].since).toISOString()}`);
  }
  const active = r.active_hot_base || r.hot_base;
  /* Not wrapped in try/catch on purpose: if the segment cannot be settled
     from one canonical mark, the boundary must fail and the round block
     rather than record a success that left someone holding scored exposure. */
  /* Settle at the SCHEDULED mark. A late callback used to settle every Hot
     position at the current price, so the segment kept accruing score after
     its own window had closed. */
  if (active) hooks.closeAlias(active + '-HOT', { roundId: id, settleAt: dueAt });
  hooks.onPhase('hot:close', r);
}

/* Returned by an opening that has not completed yet. The boundary stays
   `pending` in durable state until the retry succeeds or fails, so a crash
   mid-grace cannot leave a record claiming the segment opened. */
const DEFERRED = Symbol('deferred');

/** Open a segment ticker only if its price is competition-valid. Readiness is
 *  the gate, not merely whether the alias registers: a market whose sources
 *  disagree must not host a 2x scoring window just because its ticker opens. */
function openGate(base, id, kind = 'HOT') {
  const r = q.get.get(id);
  /* Boost asks the round's persisted high-leverage tier (500x in the current
     format), so it applies a stricter price-quality test:
     independent venues, tighter agreement, genuinely fresh sources. A market
     good enough to host Hot is not automatically good enough to host Boost. */
  const ready = kind === 'BOOST' && hooks.marketReadyForBoost
    ? hooks.marketReadyForBoost(base, requiredLeverage(r, kind), r)
    : (!hooks.marketReady || hooks.marketReady(base, r));
  if (!ready) {
    throw priceUnavailable(`${kind} market is not competition-ready`, base);
  }
  hooks.openAlias(base + '-' + kind, id);
  return base;
}

/** Retry the drawn market without blocking, then fall back once the grace
 *  period is spent. Whichever opens is recorded as the active market. */
function scheduleHotOpen(id, drawn, backup, deadline) {
  const t = setTimeout(() => {
    const r = q.get.get(id);
    if (!r || r.status !== 'running' || r.blocked_reason) return;
    if (phaseAt(r, activeElapsed(r)).phase !== 'hot') {
      /* The window closed while we were still trying, judged on the ACTIVE
         clock so a pause extends the attempt rather than expiring it. A lost
         segment is recorded and blocked rather than pretended. */
      const why = `Hot never opened: ${drawn} unavailable for the whole window`;
      q.bMark.run(id, planOf(r).hotStart, 'failed', why, Date.now());
      q.setBlocked.run(why, Date.now(), id);
      hooks.log(`round ${id} BLOCKED: ${why}`);
      hooks.onPhase('round:blocked', q.get.get(id));
      return;
    }
    try {
      openGate(drawn, id);
      q.setActive.run(drawn, null, Date.now(), id);
      q.bMark.run(id, planOf(r).hotStart, 'succeeded', null, Date.now());
      clearOutage(id);
      hooks.log(`round ${id} Hot opened on the DRAWN market ${drawn} after a brief unready window`);
      hooks.onPhase('hot:open', q.get.get(id));
      return;
    } catch (e) {
      if (activeElapsed(r) < deadline) { scheduleHotOpen(id, drawn, backup, deadline); return; }
      try {
        openGate(backup, id);
        q.setActive.run(backup, `${drawn} could not open within ${HOT_OPEN_GRACE_MS}ms: ${e.message}`, Date.now(), id);
        q.bMark.run(id, planOf(r).hotStart, 'succeeded', null, Date.now());
        /* P0-5: the drawn market's obligation is DISCHARGED by the declared
           replacement, not left open. Leaving it unresolved made every
           fallback round block at Hot end, so the backup path could never
           actually finish a round. The audit still records that it happened. */
        resolveOutage(id, 'hot', 'replaced_by_fallback', backup + '-HOT');
        hooks.log(`round ${id} Hot fell back to ${backup} after the grace period`);
        hooks.onPhase('hot:open', q.get.get(id));
      } catch (e2) {
        q.bMark.run(id, planOf(r).hotStart, 'failed', e2.message, Date.now());
        const why = `neither ${drawn} nor ${backup} could open Hot: ${e2.message}`;
        q.setBlocked.run(why, Date.now(), id);
        hooks.log(`round ${id} BLOCKED: ${why}`);
        hooks.onPhase('round:blocked', q.get.get(id));
      }
    }
  }, 250);
  t.unref?.();
}

/* Segment availability accounting. A required gate that is missing for part of
   its window is a fact about the round, not a transient log line. */
/* Outages are EVENTS, not one slot per segment.
 *
 * A single object keyed by segment could only ever hold one record, and
 * noteOutage created it only when the key was empty, so once a Hot outage had
 * been restored (or discharged by a fallback) a SECOND outage in the same
 * segment was silently ignored and the round settled as if the window had been
 * clean. A list also lets several Boost markets be down at once and keeps the
 * order they happened in. */
function readOutages(r) {
  let v = [];
  try { v = JSON.parse((r && r.gate_outage) || '[]'); } catch { v = []; }
  if (Array.isArray(v)) return v;
  // migrate the old one-object-per-segment shape in place
  return Object.entries(v).map(([segment, o]) => ({ segment, ...o }));
}
function writeOutages(id, list) {
  db.prepare('UPDATE paper_rounds SET gate_outage = ?, updated_at = ? WHERE id = ?')
    .run(JSON.stringify(list), Date.now(), id);
}
function noteOutage(id, segment, alias, reason, startedAt = null) {
  const r = q.get.get(id);
  const list = readOutages(r);
  // only one OPEN event per segment at a time; a new one starts once it closes
  if (list.some((o) => o.segment === segment && !o.restoredAt && !o.resolvedAt)) return;
  const requested = startedAt == null ? Number.NaN : Number(startedAt);
  const since = Number.isFinite(requested) && requested > 0
    ? Math.min(Date.now(), requested) : Date.now();
  list.push({ segment, alias, since, reason, restoredAt: null, resolvedAt: null, resolution: null });
  writeOutages(id, list);
}
/** Discharge an obligation by a declared replacement rather than pretending
 *  the gate came back. The distinction matters in an audit: "restored" and
 *  "we ran the backup instead" are different facts about the round. */
function resolveOutage(id, segment, resolution, replacement = null) {
  const list = readOutages(q.get.get(id));
  for (const o of list) {
    if (o.segment.startsWith(segment) && !o.restoredAt && !o.resolvedAt) {
      o.resolution = resolution; o.replacement = replacement; o.resolvedAt = Date.now();
    }
  }
  writeOutages(id, list);
}

function clearOutage(id, segment = null) {
  const list = readOutages(q.get.get(id));
  for (const o of list) {
    if ((!segment || o.segment === segment) && !o.restoredAt && !o.resolvedAt) o.restoredAt = Date.now();
  }
  writeOutages(id, list);
}
/** Any segment whose gate was never restored. */
function unresolvedOutages(r) {
  return readOutages(r).filter((o) => !o.restoredAt && !o.resolvedAt);
}

/* Boost opens a twin on every major at once: the window is shared, so there
 * is no per-player activation to track and nothing to consume. */
/* BNB is deliberately absent. Measured on the live feed 2026-08-30: its index
   has ONE component (binance-usdt). Coinbase does not list it and it is
   outside the binance-USDC deep set, so it is structurally single-source and
   cannot meet the two-component rule Boost requires. A single wrong 8-20bps
   tick sits well inside the 50bps jump clamp and is more than the ~5bps
   liquidation distance at 500x, so a lone source could liquidate a boosted
   position on noise nobody could audit. Re-add it only if a second source
   appears. */
let BOOST_MARKETS = ['BTC', 'ETH', 'XRP', 'SOL'];
const setBoostMarkets = (list) => { BOOST_MARKETS = list.slice(); };
function freezeBoostBankroll(id, dueAt = null) {
  const r = q.get.get(id);
  boostCapacityPolicyOf(r);
  const players = q.players.all(id);
  const frozen = players.filter((p) => p.boost_frozen_at != null);
  if (frozen.length === players.length) return frozen;
  if (frozen.length) throw new Error(`Boost bankroll snapshot is partial (${frozen.length}/${players.length})`);
  const asOf = Number(dueAt) || Date.now();
  const marks = hooks.markSetFor(players.map((p) => p.user_id), asOf, { strict: true, round: r });
  const marksJson = JSON.stringify(marks);
  const lev = Number(r.boost_leverage) > 0 ? Number(r.boost_leverage) : 500;
  const rows = players.map((p) => {
    const s = hooks.scoreUser(p.user_id, null, p.epoch, p.start_balance, marks, asOf);
    const bankroll = Math.max(0, Number(s && s.equity));
    if (!Number.isFinite(bankroll)) throw priceUnavailable('Boost bankroll is not priceable');
    return { p, bankroll, max: bankroll * lev };
  });
  db.transaction(() => {
    for (const x of rows) {
      const wrote = q.boostFreeze.run(x.bankroll, x.max, asOf, id, x.p.user_id);
      if (wrote.changes !== 1) throw new Error(`Boost bankroll raced for seat ${x.p.user_id}`);
      q.boostProofIns.run(id, x.p.user_id, asOf, x.bankroll, x.max, marksJson);
      insertScoreProof(r, x.p, 'boostStart', marks, asOf,
        { kind: 'boost-freeze', bankroll: x.bankroll, maxExposure: x.max, leverage: lev });
    }
  })();
  hooks.log(`round ${id}: Boost bankroll frozen for ${rows.length} players`);
  return q.players.all(id);
}

function boostBudgetOf(userId, r = currentRound()) {
  if (!r || !inRound(userId, r)) return null;
  const p = q.players.all(r.id).find((x) => Number(x.user_id) === Number(userId));
  if (!p) return null;
  return {
    policy: boostCapacityPolicyOf(r),
    bankroll: p.boost_bankroll == null ? null : Number(p.boost_bankroll),
    maxExposure: p.boost_max_exposure == null ? null : Number(p.boost_max_exposure),
    frozenAt: p.boost_frozen_at == null ? null : Number(p.boost_frozen_at),
  };
}

function openBoost(id, dueAt = null) {
  const r0 = q.get.get(id);
  boostCapacityPolicyOf(r0);
  const players = q.players.all(id);
  const hasFreeze = players.some((p) => p.boost_frozen_at != null
    || p.boost_bankroll != null || p.boost_max_exposure != null);
  const alreadyFrozen = players.length > 0 && players.every((p) =>
    p.boost_frozen_at != null && p.boost_bankroll != null && p.boost_max_exposure != null);
  const freezeProofs = q.boostProofs.all(id);
  const inconsistent = (why) => {
    const e = new Error(`Boost recovery inconsistent: ${why}; existing bankroll and proofs preserved; review required`);
    e.code = 'boost_recovery_inconsistent';
    return e;
  };
  let persistedOpened = [];
  try { persistedOpened = JSON.parse(r0.boost_opened || '[]'); }
  catch { throw inconsistent('opened set is unreadable'); }
  if (!Array.isArray(persistedOpened)) throw inconsistent('opened set is invalid');
  /* A pre-transactional opening may have frozen equity (and even immutable
     proofs) without recording an opened set. That does not prove which
     boundary actually became tradeable. Never clear the old freeze or take
     a new snapshot from later equity. Diagnose before selecting markets,
     writing resolution evidence, opening memory gates or calling money
     hooks; fireBoundary records the ordinary failed/blocked obligation. */
  if ((hasFreeze || freezeProofs.length) && !persistedOpened.length) {
    throw inconsistent('freeze exists without a durable opened set');
  }
  if ((hasFreeze || freezeProofs.length || persistedOpened.length) && !alreadyFrozen) {
    throw inconsistent('opened set and seat freezes are incomplete');
  }
  const configured = boostMarketsOf(r0);
  const minimum = Math.min(BOOST_MIN_OPEN, configured.length);
  const asOf = Number(dueAt) || Date.now();
  let resolutionRow = q.boostResolution.get(id);
  let resolution = null;
  if (resolutionRow) {
    try { resolution = JSON.parse(resolutionRow.evidence_json); }
    catch { throw new Error('Boost activation evidence is unreadable'); }
    if (!resolution || !Array.isArray(resolution.configured)
        || !Array.isArray(resolution.attempts) || !Array.isArray(resolution.selected)
        || JSON.stringify(resolution.configured) !== JSON.stringify(configured)
        || Number(resolution.activationAt) !== Number(resolutionRow.activation_at)) {
      throw new Error('Boost activation evidence is inconsistent');
    }
  } else {
    const attempts = configured.map((base) => {
      let evidence = null;
      if (typeof hooks.marketEvidenceAt === 'function') {
        try { evidence = hooks.marketEvidenceAt(base, asOf, 'BOOST',
          requiredLeverage(r0, 'BOOST'), r0); }
        catch (e) { evidence = { ready: false, historicalReady: false,
          checkedAt: asOf, rejectReason: String(e && e.message || e).slice(0, 160) }; }
      }
      const ready = evidence && typeof evidence.historicalReady === 'boolean'
        ? evidence.historicalReady : historicalReadyAt(base, asOf, 'BOOST', r0);
      return { asset: base, ready: !!ready, ...(evidence ? { evidence } : {}) };
    });
    const selected = attempts.filter((x) => x.ready).map((x) => x.asset);
    if (selected.length < minimum) {
      /* Do not lock an undersized set. The active clock pauses at this edge;
         after recovery the shifted activation receives a fresh exact-time
         observation. Once a valid set is large enough it is immutable. */
      throw priceUnavailable(`Boost cannot open its required market set (${selected.length}/${minimum} ready)`);
    }
    resolution = {
      version: 1, activationAt: asOf, configured: configured.slice(), attempts,
      selected, excluded: configured.filter((base) => !selected.includes(base)),
    };
    q.boostResolutionIns.run(id, asOf, JSON.stringify(resolution), Date.now());
    resolutionRow = q.boostResolution.get(id);
  }
  const selected = [...new Set(resolution.selected.map((base) =>
    String(base || '').trim().toUpperCase()).filter(Boolean))];
  if (selected.length !== resolution.selected.length
      || selected.some((base) => !configured.includes(base))
      || selected.length < minimum) {
    throw new Error('persisted Boost selection is invalid');
  }
  /* The durable opening transaction may commit immediately before a process
     dies, leaving the boundary row retryable. That is a completed Boost
     selection/freeze, not an invitation to redraw the set or move the
     bankroll snapshot. Reopen exactly that committed set, after proving it is
     live again, and let the boundary retry finish idempotently. */
  if (alreadyFrozen && persistedOpened.length) {
    const opened = [...new Set(persistedOpened
      .map((base) => String(base || '').trim().toUpperCase()).filter(Boolean))];
    if (opened.length !== persistedOpened.length
        || opened.some((base) => !configured.includes(base))
        || opened.length < minimum
        || JSON.stringify(opened) !== JSON.stringify(selected)) {
      throw new Error('persisted Boost opening is invalid');
    }
    const reopened = [];
    try {
      for (const base of opened) {
        if (!readyAt(base, Date.now(), 'BOOST', r0)) {
          throw priceUnavailable('persisted Boost market is not competition-ready', base);
        }
        openGate(base, id, 'BOOST');
        reopened.push(base);
      }
    } catch (e) {
      for (const base of reopened) {
        try { hooks.closeAlias(base + '-BOOST', { flatten: false, roundId: id }); } catch {}
      }
      throw e;
    }
    /* If the crash happened before outage bookkeeping, reconstruct only the
       missing facts; never append a duplicate resolved episode on replay. */
    const failed = configured.filter((base) => !opened.includes(base));
    const recorded = readOutages(q.get.get(id));
    for (const base of failed) {
      const segment = 'boost:' + base;
      if (!recorded.some((o) => o.segment === segment)) {
        noteOutage(id, segment, base + '-BOOST', 'not competition-ready at Boost activation');
      }
      resolveOutage(id, segment, 'excluded_under_minimum', null);
    }
    hooks.log(`round ${id} BOOST OPEN restored from durable set: ${opened.join(', ')}`);
    hooks.onPhase('boost:open', phasePayload(q.get.get(id)));
    return opened;
  }
  /* The PROMISED universe is frozen here, before anything opens, and is never
     rewritten. It used to share one column with the opened set, so a market
     that was promised and then failed simply vanished: the wall computed
     "unavailable" against the already-reduced set and could not show, or
     later prove, that it had been offered at all. */
  /* The offered set was locked from exact-boundary history above. Current
     liveness may pause that same set, but must never shrink or redraw it just
     because this callback ran late. */
  const eligible = selected;
  for (const base of eligible) {
    if (!readyAt(base, Date.now(), 'BOOST', r0)) {
      throw priceUnavailable('selected Boost market is awaiting a competition-valid live price', base);
    }
  }
  const opened = [];
  const failed = configured.filter((base) => !eligible.includes(base));
  try {
    for (const base of eligible) {
      openGate(base, id, 'BOOST');
      opened.push(base);
    }
  } catch (e) {
    for (const base of opened) { try { hooks.closeAlias(base + '-BOOST', { flatten: false, roundId: id }); } catch {} }
    if (e && e.unpriced) throw e;
    throw e;
  }
  /* Memory gates open synchronously, then every durable fact that makes that
     opening real lands in one transaction: frozen bankroll, promised set and
     actual opened set. On any write failure the transaction rolls back and
     the catch below shuts the in-memory gates, leaving a clean retry. */
  try {
    db.transaction(() => {
      freezeBoostBankroll(id, asOf);
      if (!r0.boost_configured) {
        db.prepare('UPDATE paper_rounds SET boost_configured = ?, updated_at = ? WHERE id = ?')
          .run(JSON.stringify(configured), Date.now(), id);
      }
      q.setBoostOpened.run(JSON.stringify(opened), Date.now(), id);
    })();
  } catch (e) {
    for (const base of opened) {
      try { hooks.closeAlias(base + '-BOOST', { flatten: false, roundId: id }); } catch {}
    }
    throw e;
  }
  for (const base of failed) {
    /* A promised market that could not open is a durable fact about the
       round, not a log line, because it is a difference between players in
       one shared window. */
    noteOutage(id, 'boost:' + base, base + '-BOOST', 'not competition-ready at Boost activation');
    hooks.log(`round ${id} boost market unavailable at activation`);
  }
  /* A predeclared minimum, never a silent improvisation. Opening one market
     out of four is not the segment that was published. */
  if (opened.length && opened.length < Math.min(BOOST_MIN_OPEN, configured.length)) {
    /* Shut what we opened before failing. A blocked round with a live
       high-leverage
       ticker is worse than no Boost at all: the segment is void but the door
       is open. */
    for (const b of opened) { try { hooks.closeAlias(b + '-BOOST', id); } catch { /* already shut */ } }
    throw new Error(`Boost needs at least ${BOOST_MIN_OPEN} of ${configured.length} markets, only ${opened.join(', ')} opened`);
  }
  if (!opened.length) {
    // a Boost phase with no tradable market is not a degraded show, it is a
    // missing one: fail the boundary so the round blocks rather than
    // pretending the segment happened
    throw new Error('no Boost market could be opened');
  }
  /* ONE POLICY, NOT TWO.
   *
   * A reduced set was allowed to open under the published minimum, and every
   * market that did not open left an unresolved obligation, which the bell
   * then refused to settle over. So the segment was simultaneously "open under
   * the minimum rule" and "unsettleable under the outage rule", and the round
   * could never finish. If the minimum is met, the excluded markets are
   * DELIBERATELY unavailable under the published rule, and their obligations
   * are discharged as such rather than left open to poison the bell. */
  for (const base of failed) {
    resolveOutage(id, 'boost:' + base, 'excluded_under_minimum', null);
  }
  // The actual opened set was persisted atomically with the bankroll above.
  hooks.log(`round ${id} BOOST OPEN: ${opened.join(', ')}`
    + (failed.length ? ` (UNAVAILABLE: ${failed.join(', ')})` : ''));
  hooks.onPhase('boost:open', phasePayload(q.get.get(id)));
}

/* opts pass straight through to closeAlias: an abort cleans up by flattening,
   the bell shuts the gates and leaves every position to be marked. */
function closeAllAliases(r, opts = {}) {
  const own = { ...opts, roundId: r.id };   // only close what THIS round opened
  if (Number(r.format_version) < 2) {
    if (r.hot_base) { try { hooks.closeAlias(r.hot_base + '-HOT', own); } catch {} }
    if (r.active_hot_base) { try { hooks.closeAlias(r.active_hot_base + '-HOT', own); } catch {} }
    if (r.hot_backup) { try { hooks.closeAlias(r.hot_backup + '-HOT', own); } catch {} }
  }
  for (const base of boostMarketsOf(r)) { try { hooks.closeAlias(base + '-BOOST', own); } catch {} }
}

/* The Boost market set is persisted on the round at arm time, so a config
   change or a restart cannot alter which tickers a live round owns. */
const boostMarketsOf = (r) => {
  try { const v = JSON.parse((r && r.boost_markets) || 'null'); if (Array.isArray(v) && v.length) return v; }
  catch { /* fall through */ }
  return BOOST_MARKETS;
};

function revealV2Draw(id) {
  const r = q.get.get(id);
  if (!r || Number(r.format_version) < 2) return null;
  if (r.draw_reveal_json) return JSON.parse(r.draw_reveal_json);
  if (!sealedDrawValid(r)) throw new Error('sealed Hot draw failed verification at settlement');
  const candidates = JSON.parse(r.hot_candidates || '[]');
  const envelope = revealEnvelope({
    id: r.id, kind: r.kind, speed: Number(r.speed) || 1,
    candidates, seed: r.hot_draw_secret, plan: planOf(r), draw: privateDrawOf(r), pricePolicy: pricePolicyOf(r),
    boostCapacityPolicy: boostCapacityPolicyOf(r),
    ...backupExecutionPolicyFields(r),
  });
  db.prepare(`UPDATE paper_rounds SET draw_reveal_json = ?, draw_seed = ?, updated_at = ?
              WHERE id = ? AND draw_reveal_json IS NULL`)
    .run(JSON.stringify(envelope), r.hot_draw_secret, Date.now(), id);
  return envelope;
}

function finalizeFinalScoreProofs(id, dueAt) {
  const r = q.get.get(id);
  if (!r || Number(r.format_version) < 2) return [];
  const players = q.players.all(id);
  const scores = q.scores.all(id, 'final');
  if (!players.length || scores.length !== players.length) {
    throw new Error(`final score proof cannot seal an incomplete checkpoint (${scores.length}/${players.length})`);
  }
  const existing = q.scoreProofs.all(id).filter((row) => row.checkpoint === 'final');
  if (existing.length === players.length) return existing;
  if (existing.length) throw new Error(`final score proof is partial (${existing.length}/${players.length})`);
  let marks = null;
  try { marks = JSON.parse(scores[0].marks || 'null'); } catch { marks = null; }
  if (!marks || typeof marks !== 'object' || Array.isArray(marks)
      || scores.some((row) => row.marks !== scores[0].marks)) {
    throw new Error('final score proof has no common canonical mark set');
  }
  const asOf = Number(dueAt) || Number(scores[0].scheduled_at);
  const records = players.map((player) => {
    const score = scores.find((row) => Number(row.user_id) === Number(player.user_id));
    const record = scoreProofRecord(r, player, 'final', marks, asOf, {
      kind: 'final-score', equity: Number(score.equity),
      accountPnl: Number(score.account_pnl), realized: Number(score.realized),
      hotBonus: Number(score.hot_bonus), score: Number(score.score),
    });
    const frozenEquity = Number(record && record.proof && record.proof.state
      && record.proof.state.equity);
    if (!Number.isFinite(frozenEquity)
        || Math.abs(frozenEquity - Number(score.equity)) > 1e-6) {
      throw new Error(`final score proof equity diverged for seat ${player.user_id}`);
    }
    return { player, record };
  });
  db.transaction(() => {
    for (const { player, record } of records) {
      q.scoreProofIns.run(id, player.user_id, 'final', asOf,
        player.epoch, player.start_balance, record.raw, record.sha256, Date.now());
    }
  })();
  return q.scoreProofs.all(id).filter((row) => row.checkpoint === 'final');
}

/* The bell. Stop the segments, mark the round done, and hand off to whatever
 * settles scores. Positions are deliberately NOT force-closed: they are
 * marked where they stand, so nobody gains from clicking faster at the end. */
function bell(id, dueAt = null) {
  const r = q.get.get(id);
  const players = r ? q.players.all(id) : [];
  const finalRows = r ? q.scores.all(id, 'final') : [];
  const cleanupRetry = !!(r && finalRows.length === players.length && players.length > 0);
  /* Once the immutable final checkpoint exists, the initial health sample,
     obligation check and snapshot have already succeeded. A cleanup retry may
     find an alias deliberately closed by the first attempt; resampling that
     post-boundary gate as though it were the pre-bell segment invents an
     outage and makes the retry impossible. */
  if (!cleanupRetry) sampleSegmentsNow(id, (Number(dueAt) || Date.now()) - 1);
  if (!cleanupRetry && r && Number(r.format_version) >= 2) {
    let opened = [];
    try { opened = JSON.parse(r.boost_opened || '[]'); } catch { opened = []; }
    const asOf = Number(dueAt) || Date.now();
    if (opened.some((base) => !historicalReadyAt(base, asOf, 'BOOST', r))) {
      throw priceUnavailable('Boost close cannot be priced');
    }
  }
  /* A result may not be published over a segment that was not available. The
     bell is the last place this can be caught, so it is checked here as well
     as at Hot end. */
  if (!cleanupRetry) assertObligationsResolved(r, 'the round reached its bell');
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
    if (!cleanupRetry) snapshot(id, 'final', dueAt);
  } catch (e) {
    /* V2 price loss is handled by fireBoundary's active-time pause/retry.
       Pre-blocking here made that retry unreachable even after recovery. */
    if (Number(r.format_version) >= 2 && e && e.unpriced) throw e;
    q.setBlocked.run(`final snapshot failed: ${e.message}`, Date.now(), id);
    hooks.log(`round ${id} BELL BLOCKED: ${e.message}`);
    hooks.onPhase('round:blocked', phasePayload(q.get.get(id)));
    throw e;
  }
  /* SEGMENT TWINS ARE SETTLED AT THE BELL, base positions are not.
     The rule above is right about players: nobody may gain by closing fastest,
     and base positions (BTC, SOL) stay open and marked, tradable afterwards
     like any paper position. But a -HOT or -BOOST position on a ticker whose
     gate has just shut is a different object: the order path refuses
     market_closed on it, so it sat orphaned at 500x until liquidation or the
     next round wiped the seat, and a player watching it bleed after the
     result was frozen was told nothing stops. Settling every twin at the
     snapshot's OWN instant and mark hands nobody an edge: the exit price is
     the price that already scored them, so the leaderboard is unchanged,
     and the account is clean when the round is. Same mechanism the Hot
     window has always closed with. */
  /* Bell cleanup is a required result mutation, not best-effort housekeeping.
     Close exactly the event books that actually opened and propagate any
     ownership/mark/DB/fill failure so the boundary stays retryable. */
  let terminalAliases = [];
  let terminalMarks = {};
  if (Number(r.format_version) >= 2) {
    try { terminalAliases = JSON.parse(r.boost_opened || '[]').map((b) => b + '-BOOST'); }
    catch { throw new Error('Boost opened set is unreadable at the bell'); }
    /* The final checkpoint is the durable price authority for terminal
       cleanup. A process can die after that checkpoint and before every
       alias is flattened; its in-memory mark history is then gone, but the
       exact marks that already decided the result remain in every final row.
       Reuse those values rather than requiring a new quote or silently
       changing the settlement instant on recovery. */
    if (cleanupRetry) {
      try { terminalMarks = JSON.parse(finalRows[0].marks || '{}'); }
      catch { throw new Error('final checkpoint marks are unreadable during bell recovery'); }
    }
  } else {
    terminalAliases = [r.hot_base && r.hot_base + '-HOT',
      r.active_hot_base && r.active_hot_base + '-HOT',
      r.hot_backup && r.hot_backup + '-HOT',
      ...boostMarketsOf(r).map((b) => b + '-BOOST')].filter(Boolean);
  }
  for (const alias of [...new Set(terminalAliases)]) {
    const checkpointMark = Number(terminalMarks[alias] ?? terminalMarks[alias.replace(/-BOOST$/, '')]);
    const result = hooks.closeAlias(alias, {
      flatten: true, settleAt: dueAt, roundId: r.id,
      overrideMark: cleanupRetry && checkpointMark > 0 ? checkpointMark : null,
    });
    if (!result || result.skipped) {
      throw new Error(`bell could not close ${alias}${result && result.skipped ? ` (${result.skipped})` : ''}`);
    }
    const residue = hooks.segmentResidue(alias);
    if (!residue || Number(residue.positions) !== 0 || Number(residue.orders) !== 0) {
      throw new Error(`bell left ${alias} residue (${Number(residue && residue.positions) || 0} positions, ${Number(residue && residue.orders) || 0} orders)`);
    }
  }
  /* Seal the score inputs only after terminal Boost closes have landed. The
     close uses the already-scored bell mark, so equity is unchanged, while
     the proof's fill digest now includes every settlement fill. Sealing it
     before cleanup would make the append-only guard reject those exact
     boundary fills and wedge the round forever. */
  finalizeFinalScoreProofs(id, dueAt);
  try { const t = _startTimers.get(id); if (t) { clearTimeout(t); _startTimers.delete(id); } } catch {}
  /* A killed process must never leave `status=done` with the bell boundary
     still `running`: resume only owns running rounds, so that record would be
     permanently unauditable. Reveal, done status and terminal boundary are
     one durable edge. fireBoundary's final mark is then an idempotent repeat. */
  const terminalAt = Date.now();
  db.transaction(() => {
    revealV2Draw(id);
    q.setStatus.run('done', terminalAt, id);
    q.bMark.run(id, planOf(r).total, 'succeeded', null, terminalAt);
    if (Number(r.format_version) >= 2) q.heartbeat.run(terminalAt, CLOCK_OWNER_ID, id);
  })();
  stopClockHeartbeat();
  try { hooks.onPhase('round:done', phasePayload(q.get.get(id))); } catch { /* the result stands either way */ }
  /* A finished round owns no live price obligations. Kept for audit, closed
     so they can never freeze the NEXT round. */
  closeClockInterval(id);
  try { hooks.closeRoundPauses && hooks.closeRoundPauses(id); } catch {}
  clearTimers(id);
  hooks.log(`round ${id} BELL`);
  hooks.onPhase('round:end', phasePayload(q.get.get(id)));
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
 * In v2 the bonus is one extra copy of the selected ordinary asset's economic
 * PnL delta between immutable Hot-start and Hot-end checkpoints. That keeps
 * pre-Hot gains at 1x and handles already-open, partially closed, reopened or
 * flipped positions symmetrically. Historical v1 rows used realised PnL on a
 * force-closed `-HOT` alias; that compatibility path remains isolated below.
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
  /* A complete checkpoint is immutable and is returned as-is. Recomputing
     current state first meant a replay could throw on today's data even
     though the stored result was already final. */
  const already = q.scores.all(roundId, checkpoint);
  if (already.length && already.length === q.players.all(roundId).length) {
    hooks.log(`round ${roundId} ${checkpoint} already complete, returning stored rows`);
    return already;
  }
  const at = Date.now();
  const asOf = Number.isFinite(scheduledAt) ? scheduledAt : at;
  const hotTicker = Number(r.format_version) >= 2 ? null
    : ((r.active_hot_base || r.hot_base) ? (r.active_hot_base || r.hot_base) + '-HOT' : null);
  const players = q.players.all(roundId);
  /* strict: a checkpoint must be priced from marks that existed at the
     boundary. If any are missing the snapshot throws, the boundary is
     recorded failed and the round blocks, rather than publishing a number
     nobody can reproduce. */
  const marks = hooks.markSetFor(players.map((p) => p.user_id), asOf, { strict: true, round: r });
  const marksJson = JSON.stringify(marks);
  if (at - asOf > 1000) {
    hooks.log(`round ${roundId} ${checkpoint}: boundary ran ${at - asOf}ms late, priced as of the scheduled time`);
  }

  /* Score EVERY player first, then write. A checkpoint containing some of the
     field is worse than none: it publishes a leaderboard that silently omits
     whoever the engine happened to trip over. If any seat cannot be scored
     the whole checkpoint throws and the boundary is recorded as failed. */
  const rows = players.map((p) => {
    const s = hooks.scoreUser(p.user_id, hotTicker, p.epoch, p.start_balance, marks, asOf);
    if (Number(r.format_version) >= 2) s.hotBonus = hotBonusFor(r, p.user_id, marks, asOf);
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
    if (Number(r.format_version) >= 2 && r.score_precision === 6) {
      Object.assign(row, rankingValues(row));
    }
    row.maxDrawdown = Number(p.max_drawdown) || 0;
    /* Bell cleanup produces deterministic SEGMENT fills at this same mark.
       Seal the final proof after those closes so its fill digest covers the
       complete round. Non-terminal checkpoints can be sealed immediately. */
    row.scoreProof = checkpoint === 'final' ? null
      : scoreProofRecord(r, p, checkpoint, marks, asOf, {
          kind: 'checkpoint-score', equity: row.equity,
          accountPnl: row.accountPnl, realized: row.realized,
          hotBonus: row.hotBonus, score: row.score,
        });
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
      q.scoreInsStrict.run(roundId, x.user_id, checkpoint, at, x.equity, x.accountPnl, x.realized, x.hotBonus, x.score, marksJson, asOf, x.maxDrawdown);
      if (x.scoreProof) {
        q.scoreProofIns.run(roundId, x.user_id, checkpoint, asOf,
          x.epoch, x.start_balance, x.scoreProof.raw, x.scoreProof.sha256, Date.now());
      }
    }
    const n = q.scores.all(roundId, checkpoint).length;
    if (n !== rows.length) throw new Error(`checkpoint wrote ${n}/${rows.length} rows`);
  })();
  hooks.log(`round ${roundId} ${checkpoint} snapshot: ${rows.length} players`);
  hooks.onPhase(checkpoint + ':scored', phasePayload(r, { checkpoint }));
  return rows;
}

/* Ranking order: score, lowest frozen maximum drawdown, realised PnL, seat.
 * Stored historical values are not rounded or rewritten on read. */
function standings(roundId, checkpoint) {
  return q.scores.all(roundId, checkpoint)
    /* The FROZEN drawdown, not the roster's current one: a published order
       must not change because someone dipped after the checkpoint. */
    .map((r) => ({ ...r, maxDrawdown: Number(r.max_drawdown) || 0 }))
    /* Published order: score, then LOWEST maximum drawdown, then higher
       realised PnL, then seat. Deterministic, so nobody has to make a call on
       stage. */
    .sort(compareStandings)
    .map((r, i) => ({ ...r, rank: i + 1 }));
}

/** First Five result: whoever is FIRST ON PnL at the checkpoint, red or
 *  green. The prize never rolls over; a negative leader still led. Equal top
 *  scores share it evenly, and only the raw score decides a tie here (not the
 *  drawdown or realised tie-breaks the published order uses), because the
 *  owner's rule is "same PnL, split", not "closest call, judged". */
function firstFiveResult(roundId) {
  const r = q.get.get(roundId);
  if (!r) throw new Error('no such round: ' + roundId);
  if (Number(r.format_version) >= 2) throw new Error('First Five does not exist in the two-Hot format');
  const board = standings(roundId, 'firstFive');
  /* Historical v1 prize schedule; never offered by a v2 round. */
  const prize = ({ round: 250, final: 750, rehearsal: 250 })[r.kind] ?? 0;
  if (!board.length) return { winner: null, winners: [], share: 0, prize, reason: 'no scores recorded', board };
  /* Equal to the cent the room sees, not to the float: two scores that both
     print as +$1,383.41 are a tie on the wall and must be a tie here. */
  const cents = (v) => Math.round(Number(v) * 1e6);
  const winners = board.filter((b) => cents(b.score) === cents(board[0].score));
  const share = winners.length ? Math.round((prize / winners.length) * 100) / 100 : 0;
  const reason = winners.length > 1
    ? `tied first on PnL, split ${winners.length} ways`
    : (board[0].score < 0 ? 'first on PnL, in the red' : 'first on PnL');
  return { winner: winners[0], winners, share, prize, reason, board };
}

// ── gates consumed by the order path ─────────────────────────────────────
/** Current phase of the running round, or null when no round is live. */
/* "Is this round over" is an ACTIVE-time question. `ends_at` is a wall-clock
   estimate stamped at start; after any pause the bell is later than that, and
   judging settlement by it cut every contestant off for the tail of Boost
   while the active clock said the decisive segment was still open. */
function roundOverNow(r, now = Date.now()) {
  const p = planOf(r);
  return p ? activeElapsed(r, now) >= p.total : now >= r.ends_at;
}
/* Wall time this round has spent frozen, including an interval still open. */
function pausedSoFar(r, now = Date.now()) {
  const banked = competitionDurationMs(r.paused_ms);
  if (!r.paused_since) return banked;
  return banked + Math.max(0, competitionMs(now) - competitionMs(r.paused_since));
}
/* THE ROUND CLOCK. Time the round was actually tradeable, which is what every
   phase boundary is measured in. Wall elapsed minus frozen time. */
function activeElapsed(r, now = Date.now()) {
  if (!r || !r.started_at) return 0;
  const wallNow = competitionMs(now);
  return Math.max(0, (wallNow - competitionMs(r.started_at)) - pausedSoFar(r, wallNow));
}
/* The wall instant at which active elapsed reaches `at`. Only meaningful while
   the round is not currently frozen: an open pause pushes it later every ms,
   which is exactly the point, so callers re-derive it when a pause clears. */
function bankedDueInstant(r, at) {
  return competitionMs(competitionMs(r.started_at)
    + competitionDurationMs(at) + competitionDurationMs(r.paused_ms));
}
function dueInstant(r, at, now = Date.now()) {
  const openPauseMs = r.paused_since
    ? Math.max(0, competitionMs(now) - competitionMs(r.paused_since)) : 0;
  return competitionMs(bankedDueInstant(r, at) + openPauseMs);
}
/* Called by the engine when the shared price pause opens and closes. The
   clock is competition state, but only the engine knows whether a market can
   be priced, so the two are wired rather than merged. Opening is idempotent:
   several markets can fail inside one freeze and it is still one interval. */
function pauseClockOpen(now = Date.now()) {
  const r = currentRound();
  if (!r || r.status !== 'running') return null;
  /* Paused milliseconds are accumulated interval-by-interval on the round.
     Never backdate a new/open interval into an interval already banked by a
     restart claim or earlier price pause, or their overlap is subtracted
     twice and active time rewinds. */
  const lastClosed = q.clockLastClosed.get(r.id);
  const closedFloor = Number(lastClosed && lastClosed.ended_at);
  now = competitionMs(Math.max(competitionMs(r.started_at),
    Number.isFinite(closedFloor) ? competitionMs(closedFloor) : 0,
    competitionPauseStartMs(now)));
  if (r.paused_since) {
    /* A later observer may know a more exact source deadline than the first
       generic failure path. Move the open edge backwards, never forwards, so
       detection latency cannot survive merely because another obligation won
       the race to persist the pause. */
    if (Number(now) < Number(r.paused_since)) {
      db.transaction(() => {
        q.setPausedSince.run(Number(now), Date.now(), r.id);
        q.clockPauseBackdate.run(Number(now), r.id);
        if (Number(r.format_version) >= 2) q.heartbeat.run(Date.now(), CLOCK_OWNER_ID, r.id);
      })();
      hooks.log(`round ${r.id} clock pause backdated by ${Number(r.paused_since) - Number(now)}ms to the price-validity edge`);
      hooks.onPhase('clock:paused', { id: r.id, formatVersion: Number(r.format_version) || 1 });
    }
    return r.id;
  }
  db.transaction(() => {
    q.setPausedSince.run(now, now, r.id);
    q.clockPauseOpen.run(r.id, now, 'runtime');
    if (Number(r.format_version) >= 2) q.heartbeat.run(now, CLOCK_OWNER_ID, r.id);
  })();
  hooks.log(`round ${r.id} clock FROZEN at ${activeElapsed(r, now)}ms active`);
  hooks.onPhase('clock:paused', { id: r.id, formatVersion: Number(r.format_version) || 1 });
  return r.id;
}
function pauseClockClose(now = Date.now()) {
  const r = currentRound();
  if (!r || !r.paused_since) return null;
  now = Math.max(competitionPauseEndMs(now), competitionPauseStartMs(r.paused_since));
  const add = Math.max(0, now - competitionPauseStartMs(r.paused_since));
  db.transaction(() => {
    q.setPausedMs.run(competitionDurationMs(r.paused_ms) + add, now, r.id);
    q.clockPauseClose.run(now, r.id);
    if (Number(r.format_version) >= 2) q.heartbeat.run(now, CLOCK_OWNER_ID, r.id);
  /* ends_at is a WALL estimate of the bell; the bell just moved later by the
     outage, so every reader of the raw column (the desk, the wall, the state
     payload) stays truthful. Settlement itself never trusts it: see
     roundOverNow. */
    try { q.setEndsAt.run(competitionMs(r.ends_at) + add, now, r.id); } catch { /* estimate only */ }
  })();
  hooks.log(`round ${r.id} clock RESUMED after ${add}ms frozen; every phase keeps its full tradeable duration`);
  /* The boundaries all moved later by `add`, so their timers are wrong. */
  schedule(r.id);
  hooks.onPhase('clock:resumed', { id: r.id, formatVersion: Number(r.format_version) || 1 });
  return { round: r.id, frozenMs: add };
}

function phaseNow(now = Date.now()) {
  const r = currentRound();
  if (!r || !r.started_at) return null;
  const elapsed = activeElapsed(r, now);
  return { round: r, ...phaseForState(r, elapsed), activeMs: elapsed, pausedMs: pausedSoFar(r, now) };
}

/** Sample every seat's equity and keep the deepest fall from its own peak.
 *  Called from the engine's sweep, so the resolution is the sweep cadence:
 *  enough to separate two traders on a tie-break, and cheap because it only
 *  ever touches the roster of the one running round. */
/* ONE sample is ONE competition event, across the whole roster.
 *
 * This used to loop seat by seat, catch a pricing error and `continue`, and
 * write each seat immediately. Maximum drawdown is a PUBLISHED TIE-BREAK, so
 * that produced two ways for identical seats to end up ranked differently:
 *
 *   - a database failure on the second seat left the first seat's tie-break
 *     updated and the second's stale, with the round still healthy;
 *   - a seat holding one tiny unpriceable leg threw, was skipped, and kept a
 *     shallower drawdown than an identical rival on the same tick.
 *
 * So: price everyone first, and if anybody cannot be priced, sample NOBODY and
 * let the shared pause handle it. Then write the whole roster in one
 * transaction, and if that fails, block the round rather than publishing a
 * partial sample.
 */
function sampleDrawdown(now = Date.now(), options = null) {
  const r = currentRound();
  if (!r || !r.started_at || r.blocked_reason || roundOverNow(r, now)) return false;
  const players = q.players.all(r.id);
  if (!players.length) return false;

  const rows = [];
  for (const p of players) {
    let eq;
    try {
      eq = hooks.equityOf(p.user_id);
    } catch (e) {
      /* One unpriceable contestant means this instant has no honest sample for
         ANYONE. Skipping only them is exactly the asymmetry we are removing.
         THROWN, not returned: the caller decides whether a missing tie-break
         sample pauses the round, and a quiet return told it the sample had
         been taken. Maximum drawdown is published; missing evidence must not
         be indistinguishable from evidence. */
      const err = new Error(`drawdown sample unavailable: ${p.user_id} cannot be priced (${e.message})`);
      err.unpriced = true;
      err.symbol = e.symbol || null;
      if (Number.isFinite(Number(e.invalidSince))) err.invalidSince = Number(e.invalidSince);
      throw err;
    }
    if (!Number.isFinite(eq)) {
      const err = new Error(`drawdown sample unavailable: ${p.user_id} has no finite equity`);
      err.unpriced = true;
      throw err;
    }
    const peak = Math.max(Number(p.peak_equity) || eq, eq);
    const dd = Math.max(Number(p.max_drawdown) || 0, peak - eq);
    if (peak !== p.peak_equity || dd !== p.max_drawdown) rows.push({ p, peak, dd });
  }
  /* Recovery from an unattributed price pause needs to answer whether the
     whole roster is priceable again, but sampling marks while the round clock
     is frozen would contaminate the published drawdown tie-break. */
  if (options && options.probeOnly === true) return true;
  if (!rows.length) return true;
  const maxDrawdownChanged = rows.some(({ p, dd }) => dd !== p.max_drawdown);

  try {
    db.transaction(() => {
      for (const x of rows) q.ddUpd.run(x.peak, x.dd, r.id, x.p.user_id);
    })();
  } catch (e) {
    /* A half-written sample decides ties by iteration order. Block instead. */
    try { blockRound(r.id, `drawdown sample could not be written: ${e.message}`); } catch {}
    throw e;
  }
  /* The caller uses this one bit to make the exact price observation visible
     even when normal relay cadence would thin it. Peak-only changes affect no
     published tie-break yet; a max-drawdown change does. */
  return { ok: true, maxDrawdownChanged };
}

/* ── the round clock ──────────────────────────────────────────────────────
 *
 * The correctness argument for the whole competition rests here.
 *
 * Timers alone could only guarantee that a boundary fires EVENTUALLY. A
 * checkpoint taken by a late callback then priced the past but read the
 * present: a fill, stop, or liquidation landing after the boundary entered a
 * result that claimed to be from before it.
 *
 * Every mutation now advances this clock first. If a boundary is due, it
 * settles BEFORE the mutation is applied. That makes "current state at the
 * moment the boundary fires" identical to "state as of the boundary", which
 * is what lets snapshot() read live rows and still be correct. Timers remain
 * as the liveness mechanism for a quiet market; they are no longer the
 * authority.
 */
let _advancing = false;
/* Availability is a property of the WHOLE window, not of its opening moment.
 *
 * Outage recording used to begin only when the initial open failed, so a gate
 * that opened cleanly and then lost its price thirty seconds into Hot left no
 * record at all: the segment ended, the boundary succeeded, and the round
 * settled as though every second of it had been priceable. A restart that lost
 * the alias near the tail was invisible the same way, because once the phase
 * moved on nothing wanted that gate any more.
 *
 * So each open segment is checked on every clock advance, which is every
 * mutation plus the sweep, and any loss is opened as an obligation the moment
 * it happens rather than the moment someone asks. */
function monitorSegments(r, now, { historical = false } = {}) {
  if (!r || r.status !== 'running' || r.blocked_reason) return;
  /* A complete immutable final checkpoint means price health has already
     passed at the bell. Only deterministic alias cleanup remains. Watching a
     gate that an earlier cleanup attempt deliberately closed fabricates a
     pre-bell outage and can deadlock recovery forever. */
  if (terminalCleanupPending(r)) return;
  /* ACTIVE time: on wall time this stopped watching Hot before the active
     window ended (a tail-of-Hot outage left no record) and watched Boost
     before it opened, logging phantom outages against gates legitimately shut. */
  const phase = phaseForState(r, activeElapsed(r, now));
  const ph = phase.phase;
  const watch = [];
  if (ph === 'hot' && Number(r.format_version) >= 2) {
    const n = phase.hotNumber;
    const base = r[`hot${n}_active_base`] || r[`hot${n}_base`];
    if (base) watch.push({ seg: `hot${n}`, base, kind: 'HOT', gateRequired: false });
  } else if (ph === 'hot' && r.active_hot_base) {
    watch.push({ seg: 'hot', base: r.active_hot_base, kind: 'HOT', gateRequired: true });
  }
  if (ph === 'boost') {
    let opened = [];
    try { opened = JSON.parse(r.boost_opened || r.boost_markets || '[]'); } catch { opened = []; }
    for (const b of opened) watch.push({ seg: 'boost:' + b, base: b, kind: 'BOOST' });
  }
  for (const w of watch) {
    const alias = w.base + '-' + w.kind;
    /* Two different ways a segment stops being real: the gate shut (a restart
       that could not rehydrate it), or the price stopped being good enough to
       decide a result on. Both are outages. */
    const gateOpen = w.gateRequired === false ? true : (hooks.aliasOpen ? hooks.aliasOpen(alias) : true);
    const availability = historical && typeof hooks.historicalAvailabilityAt === 'function'
      ? hooks.historicalAvailabilityAt(w.base, w.kind, now,
          requiredLeverage(r, w.kind), r)
      : typeof hooks.marketAvailability === 'function'
        ? hooks.marketAvailability(w.base, w.kind, now,
            requiredLeverage(r, w.kind), r)
      : null;
    const priced = availability
      ? !!availability.ready
      : w.kind === 'BOOST' && hooks.marketReadyForBoost
        ? hooks.marketReadyForBoost(w.base, requiredLeverage(r, w.kind), r)
        : (!hooks.marketReady || hooks.marketReady(w.base, r));
    const healthy = gateOpen && priced;
    const open = unresolvedOutages(r).some((o) => o.segment === w.seg);
    if (!healthy) {
      if (!open) {
        noteOutage(r.id, w.seg, alias,
          !gateOpen ? 'segment gate is not open' : 'price is not competition-valid',
          availability && availability.invalidSince != null
            && Number.isFinite(Number(availability.invalidSince))
            ? Number(availability.invalidSince) : null);
        hooks.log(`round ${r.id} ${w.seg} OUTAGE: ${!gateOpen ? 'gate shut' : 'price not competition-valid'}`);
      }
      /* Retry the clock freeze even when the durable segment-outage record
         already exists. A restart can rehydrate that record before paper's
         shared pause hook runs; keying the hook to !open then let the active
         timer burn forever while the gate was visibly unavailable. */
      hooks.pauseForSegment(w.base, w.kind,
        !gateOpen ? 'segment gate is not open' : 'price is not competition-valid',
        availability && Number.isFinite(Number(availability.invalidSince))
          ? Number(availability.invalidSince) : null);
    } else if (healthy && open) {
      clearOutage(r.id, w.seg);
      hooks.log(`round ${r.id} ${w.seg} outage cleared`);
    }
  }
}

function finalCheckpointComplete(r) {
  if (!r) return false;
  // This predicate runs inside every clock/barrier pass. Deserializing the
  // entire roster and immutable score proofs to count them dominated loaded
  // ticks. Keep the exact joined-score and nonempty-roster semantics, reading
  // fresh counts on each call so a same-transaction checkpoint is visible.
  const counts = q.checkpointCounts.get(r.id, r.id);
  return counts.players > 0 && counts.scores === counts.players;
}

/** True only in the crash/failure slice after the canonical final snapshot
 * exists but before bell cleanup and terminal status committed. */
function terminalCleanupPending(r = currentRound()) {
  return !!(r && Number(r.format_version) >= 2 && r.status === 'running'
    && finalCheckpointComplete(r));
}

/** Read-only recovery predicates used by paper's durable generic pauses.
 * They intentionally return only a bit: a build/warning response must never
 * acquire the name of a sealed draw member through a recovery diagnostic. */
function activeSegmentReady(r = currentRound(), now = Date.now()) {
  if (!r || Number(r.format_version) < 2 || r.status !== 'running') return true;
  if (terminalCleanupPending(r)) return true;
  /* A crashed opening can have committed its segment state while its boundary
     row is still retryable. phaseForState intentionally remains on the prior
     public phase until that row succeeds, so inspect the unresolved opening
     directly or a restart pause could clear without validating the already
     revealed Hot / already-opened Boost. */
  const elapsed = activeElapsed(r, now);
  const unresolvedOpen = q.bAll.all(r.id).find((row) => {
    if (Number(row.at) > elapsed + 1e-6 || row.status === 'succeeded') return false;
    const b = v2BoundaryAt(r, Number(row.at));
    return b && (b.kind === 'hotStart' || b.kind === 'boostStart');
  });
  if (unresolvedOpen) {
    const b = v2BoundaryAt(r, Number(unresolvedOpen.at));
    if (b.kind === 'hotStart') {
      const rows = q.hotRows.all(r.id, b.hotNumber);
      const base = r[`hot${b.hotNumber}_active_base`] || (rows[0] && rows[0].base);
      if (base) return readyAt(base, now, 'HOT', r);
      /* Selection is locked before current liveness is consulted. A pause
         must therefore wait for that exact selected market; asking the draw
         again can observe a different healthy backup, briefly resume the
         clock, then re-pause when openHotV2 correctly preserves the locked
         winner. */
      const locked = q.hotResolution.get(r.id, b.hotNumber);
      if (locked) return readyAt(locked.active, now, 'HOT', r);
      try { chooseHotAsset(r, b.hotNumber, now); return true; }
      catch (e) { return !(e && e.unpriced); }
    }
    let opened = [];
    try { opened = JSON.parse(r.boost_opened || '[]'); } catch { opened = []; }
    if (opened.length) {
      return opened.every((base) =>
        (!hooks.aliasOpen || hooks.aliasOpen(base + '-BOOST')) && readyAt(base, now, 'BOOST', r));
    }
    const lockedBoost = q.boostResolution.get(r.id);
    if (lockedBoost) {
      try {
        const resolution = JSON.parse(lockedBoost.evidence_json);
        return Array.isArray(resolution.selected) && resolution.selected.length > 0
          && resolution.selected.every((base) => readyAt(base, now, 'BOOST', r));
      } catch { return false; }
    }
    const configured = boostMarketsOf(r);
    return configured.filter((base) => historicalReadyAt(base, now, 'BOOST', r)).length
      >= Math.min(BOOST_MIN_OPEN, configured.length);
  }
  const ph = phaseForState(r, activeElapsed(r, now));
  /* A committed segment's recovery asks whether its current mark is usable,
     not whether trading was allowed inside the still-open pause. Historical
     last-accepted marks correctly refuse that interval; requiring one at
     `now` here made the pause depend on its own prior closure. Real boundary
     and unresolved-opening checks above keep their exact historical rules. */
  const activeReady = (base, kind) => pricePolicyOf(r) === ROUND_PRICE_POLICY
    ? kind === 'BOOST' && hooks.marketReadyForBoost
      ? !!hooks.marketReadyForBoost(base, requiredLeverage(r, kind), r)
      : !!hooks.marketReady(base, r)
    : readyAt(base, now, kind, r);
  if (ph.phase === 'hot') {
    const base = r[`hot${ph.hotNumber}_active_base`] || r[`hot${ph.hotNumber}_base`];
    /* No base means the Hot-open boundary itself has not committed yet. Its
       retry predicate below decides whether the clock may resume. */
    return base ? activeReady(base, 'HOT') : priceBoundaryReady(r, now);
  }
  if (ph.phase === 'boost') {
    let opened = [];
    try { opened = JSON.parse(r.boost_opened || '[]'); } catch { opened = []; }
    if (!opened.length) return priceBoundaryReady(r, now);
    return opened.every((base) =>
      (!hooks.aliasOpen || hooks.aliasOpen(base + '-BOOST')) && activeReady(base, 'BOOST'));
  }
  return true;
}

function priceBoundaryReady(r = currentRound(), now = Date.now()) {
  if (!r || Number(r.format_version) < 2 || r.status !== 'running') return true;
  if (terminalCleanupPending(r)) return true;
  const elapsed = activeElapsed(r, now);
  const retry = q.bAll.all(r.id).find((b) =>
    b.status === 'retryable' && b.error === 'competition price unavailable at boundary'
      && Number(b.at) <= elapsed + 1e-6);
  if (!retry) return true;
  const b = v2BoundaryAt(r, Number(retry.at));
  if (!b) return true;
  if (b.kind === 'hotStart') {
    const locked = q.hotResolution.get(r.id, b.hotNumber);
    if (locked) return readyAt(locked.active, now, 'HOT', r);
    try { chooseHotAsset(r, b.hotNumber, now); return true; } catch (e) { return !(e && e.unpriced); }
  }
  if (b.kind === 'hotEnd') {
    const rows = q.hotRows.all(r.id, b.hotNumber);
    return !!(rows[0] && readyAt(rows[0].base, now, 'HOT', r));
  }
  if (b.kind === 'boostStart') {
    const locked = q.boostResolution.get(r.id);
    if (locked) {
      try {
        const resolution = JSON.parse(locked.evidence_json);
        return Array.isArray(resolution.selected) && resolution.selected.length > 0
          && resolution.selected.every((base) => readyAt(base, now, 'BOOST', r));
      } catch { return false; }
    }
    const configured = boostMarketsOf(r);
    return configured.filter((base) => historicalReadyAt(base, now, 'BOOST', r)).length
      >= Math.min(BOOST_MIN_OPEN, configured.length);
  }
  if (b.kind === 'bell') {
    let opened = [];
    try { opened = JSON.parse(r.boost_opened || '[]'); } catch { opened = []; }
    return opened.every((base) => readyAt(base, now, 'BOOST', r));
  }
  return true;
}

function advanceRoundClock(eventTime = Date.now()) {
  eventTime = competitionMs(eventTime);
  // Re-entrancy: firing a boundary settles positions, which applies fills,
  // which would advance the clock again. The outermost call owns the pass.
  if (_advancing) return;
  let r = currentRound();
  if (!r || !r.started_at) return;
  /* A feed deadline is itself a competition event. Paper arms an independent
     timer for it, but every mutation/boundary also checks here so timer
     ordering and event-loop delay cannot let a warning, Hot open, Boost open
     or bell cross an invalid interval. The hook is deliberately only a bit;
     no private draw material crosses this boundary. */
  if (Number(r.format_version) >= 2 && typeof hooks.ensureClockHealth === 'function') {
    try { hooks.ensureClockHealth(eventTime); }
    catch (e) {
      hooks.log(`round ${r.id}: competition clock health check failed (${e.message})`);
      try { blockRound(r.id, `competition clock health check failed: ${e.message}`); } catch {}
      return;
    }
    r = currentRound();
    if (!r || !r.started_at) return;
  }
  if (r.blocked_reason) {
    /* A blocked round fires nothing, but a boundary going DUE during the
       block still happened at an instant, and that instant must be recorded
       NOW: a pause banked later in the same block inflated the stamp the
       eventual post-recovery fire computed, settling a scored window late.
       The 'due' status is inert to fireBoundary's guards and honestly counts
       as missed on resume. */
    try {
      const activeNow = activeElapsed(r, eventTime);
      for (const at of boundariesOf(r)) {
        if (at > activeNow) break;
        const b = q.bGet.get(r.id, at);
        if (b && b.due_wall_at) continue;
        if (!b) q.bMark.run(r.id, at, 'due', null, Date.now());
        q.bDue.run(Math.min(eventTime, bankedDueInstant(r, at)), r.id, at);
      }
    } catch { /* stamping must never break the clock */ }
    return;
  }
  /* Before the early return. Most advances have no boundary due, and those are
     exactly the moments a mid-segment outage happens in. */
  try { monitorSegments(r, eventTime); } catch { /* never let monitoring stop the clock */ }
  const elapsed = activeElapsed(r, eventTime);
  const due = boundariesOf(r).filter((at) => at <= elapsed);
  if (!due.length) return;
  _advancing = true;
  try {
    for (const at of due) {          // ascending: boundariesOf is ordered
      const b = q.bGet.get(r.id, at);
      if (b && b.status === 'succeeded') continue;
      fireBoundary(r.id, at, eventTime);
      if (q.get.get(r.id).blocked_reason) break;   // stop at the first block
    }
  } finally {
    _advancing = false;
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
  if (/-BOOST$/.test(symbol)) {
    if (p.phase !== 'boost') return 0;
    /* The boosted cap is whatever this market's price quality can actually
       carry right now. Handing out 500x on a price whose sources disagree by
       more than half the liquidation distance is how a wick, rather than a
       trade, decides a round. */
    const safety = hooks.boostLevCap ? hooks.boostLevCap(symbol, engineCap, p.round) : engineCap;
    /* And never above the cap the ROUND was armed under. Persisting
       boost_leverage was pointless while the gate still read the deployed
       constant: a redeploy, a config change or a restart onto different code
       silently rewrote the rules of a round already on air, while the row went
       on claiming otherwise. The round's own rule is the ceiling; the live
       safety cap can only lower it. */
    const armed = p.round.boost_leverage;
    return armed > 0 ? Math.min(safety, armed) : safety;
  }
  if (/-HOT$/.test(symbol)) {
    if (Number(p.round.format_version) >= 2) return 0;
    return p.phase === 'hot' ? Math.min(engineCap, COMP_BASE_LEV) : 0;
  }
  return Math.min(engineCap, COMP_BASE_LEV);
}

/** Re-arm timers for a round left running by a restart. */
function resume() {
  let r = currentRound();
  if (!r) return null;

  /* Claim and repair the active clock BEFORE any elapsed-time or missed-
     boundary decision. Time spent with no engine process was not tradeable. */
  r = claimClockAfterRestart(r, Date.now());
  armClockHeartbeat();

  /* A boundary left 'running' by a killed process would otherwise be skipped
     forever, because the re-entrancy guard cannot tell a live execution from
     a dead one. Anything still running at boot belongs to a previous process
     and is demoted to retryable. */
  const stale = db.prepare("UPDATE paper_round_boundaries SET status = 'retryable' WHERE round_id = ? AND status IN ('running', 'pending')").run(r.id);
  if (stale.changes) hooks.log(`round ${r.id}: ${stale.changes} boundary(ies) left running by a dead process, marked retryable`);

  if (r.blocked_reason) {
    hooks.log(`round ${r.id} resumed BLOCKED: ${r.blocked_reason}`);
    return r;
  }

  /* A round that has not drawn yet and whose committed seed died with the
     previous process can never produce a verifiable Hot Market. Block now
     rather than letting people trade for minutes into a reveal that cannot
     happen. */
  if (Number(r.format_version) >= 2 ? !sealedDrawValid(r) : (!r.hot_base && !_seeds.has(r.id))) {
    const why = 'committed draw seed lost in restart; no verifiable draw is possible';
    q.setBlocked.run(why, Date.now(), r.id);
    hooks.log(`round ${r.id} BLOCKED on resume: ${why}`);
    hooks.onPhase('round:blocked', phasePayload(q.get.get(r.id)));
    return q.get.get(r.id);
  }

  /* If the process was away across a boundary that has not run, the round
     cannot simply continue: the missed segment never happened, and jumping to
     the bell would manufacture a completed round out of a show that did not
     take place. Block it and let an operator decide. */
  /* ACTIVE elapsed, not wall: after a pause, wall runs ahead, and a clean
     restart counted boundaries not yet due as missed and blocked a healthy
     round on every restart. */
  const elapsed = activeElapsed(r);
  /* MISSED means the window is GONE, not merely that the boundary has not run.
   *
   * This counted every due-but-unfired boundary as missed, which is a cruder
   * rule than the one fireBoundary itself applies. Settlement boundaries
   * (firstFive, reveal, hotEnd, the bell) have no window at all: they price an
   * instant that has already passed, so they can always be retried, and
   * schedule() below fires anything past due immediately. The recovery was
   * already built and correct; this check blocked the round one line before it
   * could run. The cost was a restart of a few seconds that happened to span a
   * boundary instant killing a live round outright and needing an operator to
   * notice, which is the worst possible failure during a show. Only a window
   * that has genuinely closed (a Hot or Boost open whose segment is over)
   * cannot be honoured, and that still blocks, because opening a segment after
   * its window would manufacture a show that did not take place. */
  const missed = boundariesOf(r).filter((at) => {
    if (at > elapsed) return false;
    const b = q.bGet.get(r.id, at);
    if (b && b.status === 'succeeded') return false;
    return !windowStillOpen(r, at);
  });
  const past = roundOverNow(r);
  /* Bell is a settlement boundary and is never included in `missed`; there
     is therefore no terminal-boundary allowance to subtract here. One missed
     Hot/Boost opening means one whole promised segment did not occur. */
  if (missed.length > 0) {
    const why = `restart missed ${missed.length} boundary(ies): ${missed.join(', ')}`;
    q.setBlocked.run(why, Date.now(), r.id);
    hooks.log(`round ${r.id} BLOCKED on resume: ${why}`);
    hooks.onPhase('round:blocked', phasePayload(q.get.get(r.id)));
    return q.get.get(r.id);
  }

  /* Segment gates are in-memory, so a restart mid-Hot leaves the phase saying
     Hot while the ticker is shut. Rebuild them from durable state rather than
     relying on replaying a boundary that is already recorded as succeeded. */
  /* Gates cannot always be restored on the first attempt: straight after a
     restart the composite index has not resumed, so opening a ticker fails on
     "no fresh mark". Giving up there left the phase saying Hot while the
     ticker stayed shut, which is exactly what the rehearsal caught. Retry
     while the round is still in that phase, and block if it never comes back. */
  /* A deploy that changed this round's phase plan mid-flight must not replay
     boundaries at the NEW offsets: every checkpoint and settle instant is
     keyed by offset, so the replay would settle scored windows at instants
     nobody traded to. Block with a reason clearBlock refuses to lift; the
     only honest recovery is abort and re-arm under the new plan. */
  if (r.plan_json && r.plan_json !== JSON.stringify(scaledPlan(r.kind, r.speed || 1))) {
    const why = 'PHASE PLAN CHANGED by a deploy mid-round; abort and re-arm, do not attempt recovery';
    q.setBlocked.run(why, Date.now(), r.id);
    hooks.log(`round ${r.id} BLOCKED on resume: ${why}`);
    hooks.onPhase('round:blocked', phasePayload(q.get.get(r.id)));
    return q.get.get(r.id);
  }
  rehydrateWithRetry(r.id);

  if (past) { fireBoundary(r.id, planOf(r).total); return q.get.get(r.id); }
  hooks.log(`resuming round ${r.id}, ${Math.round((r.ends_at - Date.now()) / 1000)}s left`);
  schedule(r.id);
  return r;
}

const REHYDRATE_TIMEOUT_MS = Number(process.env.PAPER_REHYDRATE_TIMEOUT_MS || 60_000);
/** Keep trying to restore this round's gates until they match the phase, the
 *  phase moves on, or we give up and block. Prices lag a restart by seconds;
 *  a segment must not be silently missing for the rest of its window. */
function rehydrateWithRetry(id, startedAt = Date.now()) {
  const r = q.get.get(id);
  if (!r || r.status !== 'running' || r.blocked_reason) return;
  const missing = rehydrateGates(r);
  if (!missing.length) return;   // rehydrateGates has already cleared the obligation
  /* A frozen clock is not a failing rehydrate. An outage that would merely
     stay paused with no restart became a BLOCKED round if a restart landed
     inside it and it outlasted the 60s budget, forcing a clearBlock nobody
     should have needed. While paused, keep waiting: the pause is the system
     working. The timeout budget resumes when the clock does. */
  if (r.paused_since) {
    const t = setTimeout(() => rehydrateWithRetry(id, startedAt + 1000), 1000);
    t.unref?.();
    return;
  }
  if (Date.now() - startedAt > REHYDRATE_TIMEOUT_MS) {
    const why = `could not restore segment ticker(s) ${missing.join(', ')} after a restart`;
    q.setBlocked.run(why, Date.now(), id);
    hooks.log(`round ${id} BLOCKED: ${why}`);
    hooks.onPhase('round:blocked', phasePayload(q.get.get(id)));
    return;
  }
  const t = setTimeout(() => rehydrateWithRetry(id, startedAt), 1000);
  t.unref?.();
}

/** Reopen exactly the tickers the current phase says should be open, and
 *  close anything else this round owns. Returns the tickers it could NOT
 *  open, so the caller can retry or block rather than assume success. */
function rehydrateGates(r) {
  /* ACTIVE time: rehydrating on wall phase after a banked pause closed the
     live -HOT gate mid-active-Hot and opened -BOOST twins early. */
  const elapsed = activeElapsed(r);
  const ph = phaseForState(r, elapsed).phase;
  const hot = r.active_hot_base || r.hot_base;
  const want = new Set();
  if (Number(r.format_version) < 2 && ph === 'hot' && hot) want.add(hot + '-HOT');
  /* A crash can commit boost_opened before the boundary status/heartbeat.
     Effective phase correctly remains finalBuild, but those exact durable
     aliases still need rehydrating so the restart pause can prove recovery
     and finish the idempotent boundary retry. */
  const boostBoundary = Number(r.format_version) >= 2
    ? q.bGet.get(r.id, planOf(r).boostStart) : null;
  const unresolvedBoostOpen = !!(boostBoundary
    && boostBoundary.status !== 'succeeded'
    && Number(boostBoundary.at) <= elapsed + 1e-6
    && r.boost_opened);
  if (!terminalCleanupPending(r) && (ph === 'boost' || unresolvedBoostOpen)) {
    let opened = [];
    if (Number(r.format_version) >= 2) {
      try { opened = JSON.parse(r.boost_opened || '[]'); } catch { opened = []; }
    } else opened = boostMarketsOf(r);
    for (const b of opened) want.add(b + '-BOOST');
  }
  const missing = [];
  for (const a of want) {
    /* SAME DOOR AS THE INITIAL OPEN.
     *
     * This called hooks.openAlias directly, which only asks whether a generic
     * stage mark exists. It has no competition context, so recovery could
     * reopen a Hot or Boost ticker while the base symbol was confirming and
     * compPriceReady was false: a gate "restored" over a market nobody could
     * price, with the wall and the outage logic both reporting it healthy.
     * One function decides both first open and restart recovery. */
    const kind = /-BOOST$/.test(a) ? 'BOOST' : 'HOT';
    try { openGate(a.replace(/-(HOT|BOOST)$/, ''), r.id, kind); }
    catch (e) { missing.push(a); hooks.log(`rehydrate ${a} not yet: ${e.message}`); }
  }
  const all = [Number(r.format_version) < 2 && hot && hot + '-HOT',
    Number(r.format_version) < 2 && r.hot_base && r.hot_base + '-HOT',
    ...boostMarketsOf(r).map((b) => b + '-BOOST')].filter(Boolean);
  for (const a of all) {
    if (!want.has(a)) { try { hooks.closeAlias(a, { flatten: false, roundId: r.id }); } catch {} }
  }
  /* Record the obligation HERE, in the gate rebuild itself, so it does not
     depend on which caller happened to run. resume() calls this directly and
     only schedules the retry a second later; a bell landing in that second
     used to settle a healthy-looking result over a segment that had no gate. */
  for (const a of missing) {
    const seg = /-BOOST$/.test(a) ? 'boost:' + a.replace(/-BOOST$/, '') : 'hot';
    noteOutage(r.id, seg, a, 'could not be restored after a restart');
    hooks.pauseForSegment(a.replace(/-(HOT|BOOST)$/, ''), /-BOOST$/.test(a) ? 'BOOST' : 'HOT',
      'segment gate could not be restored after restart');
  }
  for (const a of want) {
    if (!missing.includes(a)) {
      const seg = /-BOOST$/.test(a) ? 'boost:' + a.replace(/-BOOST$/, '') : 'hot';
      clearOutage(r.id, seg);
    }
  }
  hooks.log(`round ${r.id} gates for phase ${ph}: ${[...want].filter((a) => !missing.includes(a)).join(', ') || 'none'}` +
    (missing.length ? ` (pending: ${missing.join(', ')})` : ''));
  return missing;
}

module.exports = {
  boostCapacityPolicyOf, boostCapacityPolicyMatches,
  backupExecutionPolicyOf, backupExecutionPolicyMatches, backupExecutionPolicyFields,
  usesLatestBackupPricing, roundMarkExecutionLeverage, roundMarkPolicyFields,
  pricePolicyOf, requiredRoundMarkLeverage, roundMarkGet, roundMarkCommit, roundMarkInvalidate,
  roundMarksFor, roundMarkEvidenceFor, roundMarkHistory, roundMarkCandles,
  pauseClockOpen, pauseClockClose, activeElapsed, pausedSoFar, dueInstant, settleAtPrior,
  boostMarketsOf,
  ROUND_PLAN, COMP_BASE_LEV, wire, phaseAt, boundariesOf, inRound,
  createRound, startRound, abortRound, currentRound, playersOf, accountLocked, clearBlock, blockRound, sampleDrawdown, otherRoundsOwning,
  marketReadiness, overrideReadiness, marketReliabilityOf,
  wallState, setWall, seriesBoard, stopPlacings, STOP_PRIZE, WALL_MODES, BRACKET, planOf, scaledPlan, SPEEDS, resetNight,
  nightHistory,
  isBotId, botIdForSeat, BOT_ID_BASE, isPracticeRound, isPracticeSeries,
  scheduleStart, cancelScheduledStart, resumeScheduledStarts,
  inviteState, claimInvite, setInviteReady, setReadyByUser, readinessOf,
  boostLeverageOf,
  phaseNow, levCapFor, resume, rehydrateGates, verifyDraw, advanceRoundClock, setBoostMarkets, settledFor, writeBarrier,
  hotBonusFor, hotBonusRowsFor, boostBudgetOf, freezeBoostBankroll,
  monitorSegments, activeSegmentReady, priceBoundaryReady, terminalCleanupPending,
  snapshot, standings, firstFiveResult,
  rankingValues, compareStandings,
  __test: { db, q, commitOf, drawIndex, drawInt, makeHotDraw, revealEnvelope, v2Commit,
    privateDrawOf, sealedDrawValid, v2BoundaryAt, phaseForState, claimClockAfterRestart,
    CLOCK_OWNER_ID, _seeds, _timers, fireBoundary, schedule, resumeScheduledStarts,
    openHotV2, closeHotV2, openBoost },
};
