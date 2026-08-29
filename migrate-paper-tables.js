'use strict';
// One-shot: copy paper_* tables from the phoenix-teams users.db into the
// isolated paper.db. Run ON THE PERP.SO BOX during the cutover freeze, then
// scp the produced file to the paper box:
//   node migrate-paper-tables.js /opt/phoenix-teams/users.db ./paper.db
// Idempotent: re-running replaces the paper_* rows wholesale.
const Database = require('better-sqlite3');

const [src, dst] = process.argv.slice(2);
if (!src || !dst) { console.error('usage: node migrate-paper-tables.js <users.db> <paper.db>'); process.exit(1); }

const out = new Database(dst);
out.pragma('journal_mode = WAL');
out.exec(`ATTACH DATABASE '${src.replace(/'/g, "''")}' AS legacy`);

const tables = out.prepare(
  "SELECT name FROM legacy.sqlite_master WHERE type='table' AND name LIKE 'paper_%'"
).all().map((r) => r.name);
if (tables.length === 0) { console.error('no paper_* tables found in source'); process.exit(1); }

out.exec('BEGIN');
// FK stub: paper_* DDL references users(id); seed every id the data mentions.
out.exec('CREATE TABLE IF NOT EXISTS main.users (id INTEGER PRIMARY KEY)');
for (const t of tables) {
  const ddl = out.prepare("SELECT sql FROM legacy.sqlite_master WHERE name = ?").get(t).sql;
  out.exec(`DROP TABLE IF EXISTS main.${t}`);
  out.exec(ddl);
  out.exec(`INSERT OR IGNORE INTO main.users (id) SELECT DISTINCT user_id FROM legacy.${t}`);
  out.exec(`INSERT INTO main.${t} SELECT * FROM legacy.${t}`);
  const n = out.prepare(`SELECT count(*) c FROM main.${t}`).get().c;
  console.log(`${t}: ${n} rows`);
}
// Indexes (paper.js recreates its own on boot, but carry any custom ones).
for (const r of out.prepare(
  "SELECT sql FROM legacy.sqlite_master WHERE type='index' AND tbl_name LIKE 'paper_%' AND sql IS NOT NULL"
).all()) {
  try { out.exec(r.sql); } catch { /* duplicate name — paper.js will manage */ }
}
out.exec('COMMIT');
out.exec('DETACH DATABASE legacy');
console.log('done');
