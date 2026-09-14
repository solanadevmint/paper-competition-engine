'use strict';

/* The one deterministic test entry point used by CI and deploys.
 *
 * Each suite gets its own database. Sharing one database made parallel or
 * repeated runs order-dependent and produced failures that looked like engine
 * regressions. The network/live suite is deliberately separate: it requires a
 * warmed reliability window and tests external availability, not a release's
 * deterministic correctness. */
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawnSync } = require('child_process');

const suites = [
  'test-service-resilience.js',
  'test-candles.js',
  'test-source-expiry.js',
  'test-market-diagnostics.js',
  'test-engine-review.js',
  'test-comp-push.js',
  'test-two-hot.js',
  'test-alias.js',
  'test-competition.js',
  'test-scoring.js',
  'test-engine-deep.js',
  'test-comp-api.js',
  'test-integrity.js',
  'test-migration.js',
  'test-review-remediation.js',
  'test-rehearsal-runbook.js',
];

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'phoenix-paper-test-'));
let failed = false;

try {
  for (const suite of suites) {
    const dbName = suite.replace(/\.js$/, '.db');
    process.stdout.write(`\n=== ${suite} ===\n`);
    const run = spawnSync(process.execPath, [suite], {
      cwd: __dirname,
      stdio: 'inherit',
      env: {
        ...process.env,
        PAPER_DB: path.join(scratch, dbName),
        // Tests must never inherit or probe production coordination state.
        // An isolated service user cannot traverse the real 0700 state path,
        // and the engine correctly treats that EACCES as active maintenance.
        PAPER_MAINTENANCE_FILE: path.join(scratch, `${dbName}.maintenance`),
        PAPER_COMP_TOKEN: 'deterministic-test-token',
      },
    });
    if (run.error) {
      console.error(`${suite}: ${run.error.message}`);
      failed = true;
      continue;
    }
    if (run.status !== 0) {
      console.error(`${suite}: exited ${run.status}`);
      failed = true;
    }
  }
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

if (failed) process.exit(1);
console.log(`\nAll ${suites.length} deterministic suites passed.`);
