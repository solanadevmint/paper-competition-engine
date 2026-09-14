'use strict';
/* Schema upgrades, from every shape that has ever been in production.
 *
 * A fresh database always looks right, which is exactly why this suite exists:
 * the round-six review found that the paper_rounds rebuild (needed because a
 * CHECK constraint cannot be altered in place) hardcoded its column list and
 * ran AFTER the ADD COLUMN pass, so upgrading a real pre-rehearsal database
 * silently DROPPED gate_outage. Nothing in the test suite noticed, because
 * nothing tested an upgrade.
 *
 * Every historical shape gets a row of real data, so we assert that the
 * migration preserves CONTENT and not merely column names.
 */
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { execFileSync } = require('child_process');

let pass = 0;
const ok = (name, fn) => {
  try { fn(); console.log('  ok   ' + name); pass++; }
  catch (e) { console.log('  FAIL ' + name + '\n       ' + e.message); process.exitCode = 1; }
};

/* The historical shapes, oldest first. Each is the CREATE that was live at
   that time, so "upgrade from v1" means "run today's engine against this". */
const SHAPES = {
  'v1 pre-rehearsal, pre-fallback': `
    CREATE TABLE paper_rounds (
      id TEXT PRIMARY KEY,
      kind TEXT NOT NULL CHECK (kind IN ('round','final')),
      status TEXT NOT NULL CHECK (status IN ('armed','running','done','aborted')),
      started_at INTEGER, ends_at INTEGER,
      hot_candidates TEXT NOT NULL, hot_backup TEXT, hot_base TEXT,
      draw_commit TEXT NOT NULL, draw_seed TEXT, draw_at INTEGER,
      created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
      blocked_reason TEXT, active_hot_base TEXT, boost_markets TEXT)`,
  'v2 pre-rehearsal, with fallback_reason': `
    CREATE TABLE paper_rounds (
      id TEXT PRIMARY KEY,
      kind TEXT NOT NULL CHECK (kind IN ('round','final')),
      status TEXT NOT NULL CHECK (status IN ('armed','running','done','aborted')),
      started_at INTEGER, ends_at INTEGER,
      hot_candidates TEXT NOT NULL, hot_backup TEXT, hot_base TEXT,
      draw_commit TEXT NOT NULL, draw_seed TEXT, draw_at INTEGER,
      created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
      blocked_reason TEXT, active_hot_base TEXT, boost_markets TEXT, fallback_reason TEXT)`,
  /* The shape that actually broke: pre-rehearsal CHECK, but gate_outage had
     already been added by the round-five ALTER pass. The rebuild then dropped
     it. This is the exact production upgrade path. */
  'v3 pre-rehearsal, with gate_outage already added': `
    CREATE TABLE paper_rounds (
      id TEXT PRIMARY KEY,
      kind TEXT NOT NULL CHECK (kind IN ('round','final')),
      status TEXT NOT NULL CHECK (status IN ('armed','running','done','aborted')),
      started_at INTEGER, ends_at INTEGER,
      hot_candidates TEXT NOT NULL, hot_backup TEXT, hot_base TEXT,
      draw_commit TEXT NOT NULL, draw_seed TEXT, draw_at INTEGER,
      created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
      blocked_reason TEXT, active_hot_base TEXT, boost_markets TEXT,
      fallback_reason TEXT, gate_outage TEXT)`,
};

/* Columns today's engine must be able to rely on after ANY upgrade. */
const REQUIRED = [
  'id', 'kind', 'status', 'started_at', 'ends_at', 'hot_candidates', 'hot_backup',
  'hot_base', 'draw_commit', 'draw_seed', 'draw_at', 'created_at', 'updated_at',
  'blocked_reason', 'active_hot_base', 'boost_markets', 'fallback_reason',
  'gate_outage', 'boost_configured', 'boost_opened', 'backup_execution_policy',
];
/* Columns the ROSTER table must have after any upgrade. */
const REQUIRED_PLAYER = ['round_id', 'user_id', 'display_name', 'seat', 'epoch', 'avatar_url',
  'invite_token', 'claimed_at', 'ready_at'];

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'papermig-'));

function upgradeFrom(shapeSql, seed = {}) {
  const dbPath = path.join(tmp, 'm' + Math.abs(shapeSql.length) + '-' + REQUIRED.length + '-' + Object.keys(seed).length + '.db');
  try { fs.unlinkSync(dbPath); } catch { /* first run */ }
  const Database = require('better-sqlite3');
  const db = new Database(dbPath);
  db.exec(shapeSql);
  const cols = db.prepare('PRAGMA table_info(paper_rounds)').all().map((c) => c.name);
  const row = {
    id: 'legacy-1', kind: 'round', status: 'done', started_at: 1000, ends_at: 2000,
    hot_candidates: '["BTC","SOL"]', hot_backup: 'XRP', hot_base: 'SOL',
    draw_commit: 'abc', draw_seed: 'seed', draw_at: 1500,
    created_at: 900, updated_at: 2100, blocked_reason: null,
    active_hot_base: 'SOL', boost_markets: '["BTC","ETH"]', fallback_reason: null,
    ...seed,
  };
  const use = cols.filter((c) => c in row);
  db.prepare(`INSERT INTO paper_rounds (${use.join(',')}) VALUES (${use.map(() => '?').join(',')})`)
    .run(...use.map((c) => row[c]));
  db.close();

  /* Load the engine in a child process against this database, which is what a
     real deploy does: the migration runs at require time. */
  execFileSync(process.execPath, ['-e', "require('./competition.js');"],
    { cwd: __dirname, env: { ...process.env, PAPER_DB: dbPath }, stdio: 'pipe' });

  const after = new Database(dbPath, { readonly: true });
  const outCols = after.prepare('PRAGMA table_info(paper_rounds)').all().map((c) => c.name);
  const outRow = after.prepare('SELECT * FROM paper_rounds WHERE id = ?').get('legacy-1');
  const ddl = after.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name='paper_rounds'").get();
  after.close();
  return { cols: outCols, row: outRow, ddl: ddl && ddl.sql, path: dbPath };
}

(async () => {
  console.log('\nupgrading from every shape that has been in production');

  for (const [label, sql] of Object.entries(SHAPES)) {
    ok(`${label}: every required column survives`, () => {
      const { cols } = upgradeFrom(sql);
      const missing = REQUIRED.filter((c) => !cols.includes(c));
      assert.deepStrictEqual(missing, [], 'missing after upgrade: ' + missing.join(', '));
    });

    ok(`${label}: the rehearsal kind becomes usable`, () => {
      const { ddl } = upgradeFrom(sql);
      assert.ok(ddl.includes("'rehearsal'"), 'the CHECK constraint was not rebuilt');
    });

    ok(`${label}: existing round data is preserved exactly`, () => {
      const { row } = upgradeFrom(sql);
      assert.ok(row, 'the legacy round disappeared entirely');
      assert.strictEqual(row.id, 'legacy-1');
      assert.strictEqual(row.hot_base, 'SOL', 'the drawn market must survive an upgrade');
      assert.strictEqual(row.draw_commit, 'abc', 'the published commitment must survive');
      assert.strictEqual(row.boost_markets, '["BTC","ETH"]');
      assert.strictEqual(row.status, 'done');
      assert.strictEqual(row.backup_execution_policy, null, 'migration never opts an existing row into new pricing');
    });
  }

  /* The specific regression: content in a column the rebuild did not know
     about must not be thrown away. */
  ok('gate_outage CONTENT survives the CHECK-constraint rebuild', () => {
    const outage = '{"hot":{"alias":"SOL-HOT","since":1,"reason":"x","restoredAt":null}}';
    const { cols, row } = upgradeFrom(SHAPES['v3 pre-rehearsal, with gate_outage already added'],
      { gate_outage: outage });
    assert.ok(cols.includes('gate_outage'), 'the column was dropped by the rebuild');
    assert.strictEqual(row.gate_outage, outage,
      'the column survived but its contents did not, which loses the availability record');
  });

  ok('the roster table gains avatar_url on every historical shape', () => {
    for (const [label, sql] of Object.entries(SHAPES)) {
      const dbPath = upgradeFrom(sql).path;
      const Database = require('better-sqlite3');
      const db = new Database(dbPath, { readonly: true });
      const cols = db.prepare('PRAGMA table_info(paper_round_players)').all().map((c) => c.name);
      db.close();
      const missing = REQUIRED_PLAYER.filter((c) => !cols.includes(c));
      assert.deepStrictEqual(missing, [], label + ' missing: ' + missing.join(', '));
    }
  });

  ok('upgrading twice is a no-op, not a second rebuild', () => {
    const a = upgradeFrom(SHAPES['v1 pre-rehearsal, pre-fallback']);
    assert.ok(a.cols.includes('gate_outage'));
    // running the engine again against an already-current database
    const b = upgradeFrom(SHAPES['v1 pre-rehearsal, pre-fallback']);
    assert.deepStrictEqual(b.cols, a.cols, 'a second migration changed the shape');
    assert.strictEqual(b.row.id, 'legacy-1');
  });
  /* The roster table was rekeyed from (round_id, user_id) to (round_id, seat)
     so a seat can exist before anyone occupies it. That is a full table
     rebuild on a live database, and the last rebuild in this file silently
     dropped a column, so it gets the same scrutiny. */
  ok('the roster rekey preserves every seat, exactly', () => {
    const Database = require('better-sqlite3');
    const dbPath = path.join(tmp, 'roster-rekey.db');
    try { fs.unlinkSync(dbPath); } catch { /* first run */ }
    const db = new Database(dbPath);
    db.exec(`CREATE TABLE paper_round_players (
      round_id TEXT NOT NULL, user_id INTEGER NOT NULL, display_name TEXT,
      seat INTEGER NOT NULL, epoch INTEGER, start_balance REAL,
      peak_equity REAL, max_drawdown REAL, avatar_url TEXT,
      PRIMARY KEY (round_id, user_id))`);
    const rows = [
      ['r1', 41, 'Yuki', 0, 7, 10, 12.5, 1.25, 'https://x/y.png'],
      ['r1', 42, 'Mia', 1, 7, 10, 11, 0.5, null],
      ['r2', 41, 'Yuki', 0, 9, 10, 10, 0, null],
    ];
    for (const r of rows) {
      db.prepare(`INSERT INTO paper_round_players
        (round_id,user_id,display_name,seat,epoch,start_balance,peak_equity,max_drawdown,avatar_url)
        VALUES (?,?,?,?,?,?,?,?,?)`).run(...r);
    }
    const before = db.prepare('SELECT * FROM paper_round_players ORDER BY round_id, seat').all();
    db.close();

    execFileSync(process.execPath, ['-e', "require('./competition.js');"],
      { cwd: __dirname, env: { ...process.env, PAPER_DB: dbPath }, stdio: 'pipe' });

    const after = new Database(dbPath, { readonly: true });
    const cols = after.prepare('PRAGMA table_info(paper_round_players)').all().map((c) => c.name);
    const ddl = after.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name='paper_round_players'").get().sql;
    const out = after.prepare('SELECT * FROM paper_round_players ORDER BY round_id, seat').all();
    after.close();

    assert.ok(ddl.includes('PRIMARY KEY (round_id, seat)'), 'the table was not rekeyed');
    assert.deepStrictEqual(REQUIRED_PLAYER.filter((c) => !cols.includes(c)), [],
      'a required roster column did not survive');
    assert.strictEqual(out.length, before.length, 'a seat was lost in the rebuild');
    for (let i2 = 0; i2 < before.length; i2 += 1) {
      for (const c of ['round_id', 'user_id', 'display_name', 'seat', 'epoch',
        'start_balance', 'peak_equity', 'max_drawdown', 'avatar_url']) {
        assert.strictEqual(out[i2][c], before[i2][c],
          `${c} changed on seat ${i2}: ${before[i2][c]} -> ${out[i2][c]}`);
      }
      assert.strictEqual(out[i2].invite_token, null, 'existing seats get no retro-fitted invite');
    }
  });

  ok('a seat can exist with no player, and two empty seats do not collide', () => {
    const Database = require('better-sqlite3');
    const dbPath = path.join(tmp, 'empty-seats.db');
    try { fs.unlinkSync(dbPath); } catch { /* first run */ }
    execFileSync(process.execPath, ['-e', "require('./competition.js');"],
      { cwd: __dirname, env: { ...process.env, PAPER_DB: dbPath }, stdio: 'pipe' });
    const db = new Database(dbPath);
    const ins = db.prepare('INSERT INTO paper_round_players (round_id,user_id,display_name,seat,invite_token) VALUES (?,?,?,?,?)');
    ins.run('r', null, 'A', 0, 'tok-a');
    ins.run('r', null, 'B', 1, 'tok-b');
    assert.strictEqual(db.prepare('SELECT COUNT(*) c FROM paper_round_players').get().c, 2,
      'NULL user_id must not collide across seats');
    assert.throws(() => ins.run('r', 5, 'C', 2, 'tok-a'), /UNIQUE/, 'an invite must be unique');
    ins.run('r', 5, 'C', 2, 'tok-c');
    assert.throws(() => ins.run('r', 5, 'D', 3, 'tok-d'), /UNIQUE/,
      'one person cannot hold two seats in a round');
    db.close();
  });


  ok('armed, running and settled v2 rows retain absent backup policy across repeated migration', () => {
    const Database = require('better-sqlite3');
    const shape = SHAPES['v3 pre-rehearsal, with gate_outage already added']
      .replace('fallback_reason TEXT, gate_outage TEXT)',
        'fallback_reason TEXT, gate_outage TEXT, format_version INTEGER, price_policy TEXT, draw_reveal_json TEXT)');
    for (const status of ['armed', 'running', 'done']) {
      const result = upgradeFrom(shape, { status, format_version: 2,
        price_policy: 'last-accepted-v1', draw_reveal_json: '{"immutable":"legacy-reveal"}' });
      assert.strictEqual(result.row.backup_execution_policy, null);
      const before = JSON.stringify(result.row);
      execFileSync(process.execPath, ['-e', "require('./competition.js');"],
        { cwd: __dirname, env: { ...process.env, PAPER_DB: result.path }, stdio: 'pipe' });
      const db = new Database(result.path);
      try {
        assert.strictEqual(JSON.stringify(db.prepare('SELECT * FROM paper_rounds WHERE id=?').get('legacy-1')), before);
        assert.throws(() => db.prepare('UPDATE paper_rounds SET backup_execution_policy=? WHERE id=?')
          .run('latest-available-500-v1', 'legacy-1'), /backup execution policy is immutable/);
      } finally { db.close(); }
    }
  });

  console.log(`\n${pass} passed${process.exitCode ? ', WITH FAILURES' : ''}\n`);
})();
