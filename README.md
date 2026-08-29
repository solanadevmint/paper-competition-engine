# Paper trading engine + live competition layer

A perps paper-trading engine, and a competition layer built on top of it for a
live trading show. Published for review.

**This is a review snapshot, not a deployable artifact.** The deployment
runbook, infrastructure details and the production database are deliberately
not in this repo.

---

## What to review

The competition layer is new and unshipped. The engine underneath it has been
running in production for weeks. Reviewers should weight accordingly: bugs in
`competition.js` and the parts of `paper.js` marked below are the interesting
target; the rest is context.

New in this snapshot:

| File | What it is |
|---|---|
| `competition.js` | Round clock, phase gate, verifiable market draw, scoring. Entirely new. |
| `paper.js` | Engine. New: alias tickers, the order-path gate, `scoreUser`, the competition HTTP handlers. Everything else predates this work. |
| `server.js` | HTTP routing. Three new routes. |
| `test-*.js` | 129 assertions, all passing. |
| `test-integrity.js` | **New.** One case per blocker from the first review round. Adversarial rather than functional: nothing here describes what a well-behaved player does, and all of it changes a published result if it regresses. |

---

## The format being modelled

Three beats inside a 30-minute round (20 for the final):

1. **First Five** — highest net PnL at the 5:00 checkpoint wins a cash prize.
   Positions stay open, the round continues.
2. **Hot Market** — one market, drawn live from three candidates, is worth
   **2x** for a four-minute window. Losses count double too.
3. **Boost** — the last three minutes, leverage ceiling lifts from 100x to
   1000x for every player at once.

Two numbers are kept deliberately apart:

```
account PnL       drives equity, margin, liquidation. What the engine knows.
competition score = account PnL + Hot bonus. Drives the leaderboard only.
```

A bonus never adds collateral and never prevents a liquidation.

---

## Design decisions worth scrutinising

**Event exposure is a separate ticker, not a tagged lot.** `SOL-HOT` and
`BTC-BOOST` are synthetic tickers that resolve to their base symbol's index
(`baseOf()` in `paper.js`). The engine blends entry price and leverage when
adding to a position, so lot-level tagging would have required rebuilding
position accounting. A separate ticker gives event exposure its own entry,
size and PnL for free.

*Known consequence:* a trader can hold `SOL-HOT` long and `SOL` short
simultaneously. An earlier version of this README called that "not
exploitable", which was wrong and was corrected in review. See **Open product
decision** below: there is no free money, but it is a valid low-risk
score-only strategy and it needs an owner's call.

**Boost twins are forced isolated-margin** (`cfgOf()`). At 1000x the
liquidation distance is ~5bps. Measured index-noise breach rates over a
two-minute window: BTC under 1%, ETH/BNB around 7%, XRP/SOL around 15%.
Isolating the margin means a noise liquidation costs the stake committed to
the Boost rather than ending the player's round.

**The draw is commit-reveal.** `sha256(seed + "|" + candidates)` is published
when the round is armed; the seed is published at the reveal. Anyone can
recompute the winner. The commitment covers the candidate list too, so the
shortlist cannot be swapped after the fact either. The seed is held in memory
until the reveal, and a restart in between now blocks the round rather than
falling through to the backup. See `verifyDraw()`.

*Scope of the proof:* commit-reveal shows the result was not changed after the
commitment. It does not show the seed was not chosen before it. If the public
claim needs to be "nobody could influence it", combine the commitment with
future external entropy (a block hash at an announced height); otherwise
describe it narrowly.

**Phases fire from a server timer anchored to the round start**, never from a
client poll. Boundaries already past are replayed in order on boot, and each
fires at most once, so an engine restart mid-round resumes rather than stalls.

**The bell marks, it does not close.** `bell()` snapshots every account first,
then shuts the segment gates *without* flattening. An earlier version closed
positions first, which turned final marks into realised exits and would have
rewarded whoever closed fastest. The test for this is
`test-engine-deep.js` → "the bell marks everyone without closing positions".

---

## Fixed since the first review

All ten reported ship blockers are closed, each with a regression test in
`test-integrity.js`:

1. **Public reset during a round.** Accounts are locked from the moment a
   player is seated on an armed round until it ends (`accountLocked`); the
   endpoint returns 409. Also fixed the `bodyRaw` reference bug found in the
   same function, which meant mode switching there had silently never worked.
2. **Alias positions skipped by base ticks.** `tickEval` now evaluates the
   base and both twins from the same incoming mark, so a 1000x position is
   risk-checked on the tick rather than the 5s sweep.
3. **Resting segment orders.** `closeAlias` cancels everything resting before
   settling, and the sweep independently cancels any order on a closed gate.
4. **In-flight orders crossing a boundary.** The gate, leverage ceiling and
   round state are re-checked after the await; `awaitBook` is skipped in stage
   mode entirely; an order arriving after the bell gets 409 `round_settled`.
5. **Partial checkpoints.** Every seat is scored before anything is written,
   then written in one transaction. A failed final snapshot sets
   `blocked_reason` and leaves the round unsettled rather than marking it done.
   Boundary status is durable (`paper_round_boundaries`), so a failed boundary
   can be retried and a succeeded one is never replayed.
6. **Live Hot scoring.** The bonus now includes open Hot exposure marked to
   market, so the wall shows 2x during the segment instead of jumping at
   force-close. Tested for continuity across the close, not just correctness.
7. **Boost semantics.** The legacy per-position 2:00 clock can no longer touch
   a competition player, on any ticker, and a client-supplied `boostWindow`
   flag cannot change how a competitor is treated. The public product keeps
   its clock (also tested).
8. **Fallback vs the draw proof.** `hot_base` is immutable and is what the
   commitment proves; `active_hot_base` and `fallback_reason` record what
   actually traded and why. `verifyDraw` still verifies after a fallback and
   reports `fellBack`/`traded`.
9. **Restart before reveal.** A missing seed now blocks the round and
   `openHot` refuses to open anything undrawn. This was worse than reported:
   with a backup configured, the old code opened an unproven Hot Market with
   the 2x multiplier live on it.
10. **Concurrent rounds.** Enforced at `startRound`. Arming is one
    transaction and the seed enters memory only after commit.

`startRound` now also resets every seat and binds the scoring epoch to the
roster row, so preflight and reset are no longer optional buttons and scoring
cannot change basis mid-round.

## Known gaps, still open

- **Maximum drawdown is not tracked per round.** The format's first tie-break
  is lowest drawdown; ranking falls through to realised PnL, then seat.
- **A flip clears the legacy per-position boost clock** in `applyFill`. Public
  paper product only; the competition no longer uses that path at all.
- **Hot close settles player by player**, not from one canonical mark, and a
  missing mark leaves that player open.
- **The wall publishes zeroes if scoring throws** rather than showing an error
  state.
- **The operator token is accepted in the body as well as a header**, and
  comparison is not constant-time. No operator audit table yet.
- **Checkpoints store scalar outputs, not the mark set used**, so a disputed
  result cannot be independently replayed tick by tick.
- **The backup market is not inside the commitment.** It is declared at arm
  time and published with a reason, but not hashed.
- No rate limiting on the competition endpoints beyond the existing per-user
  write limiter.
- The engine runs a $10 bankroll displayed as $100,000 (a 10,000x display
  scale). Order sizes execute at 1/10,000th of displayed size so fills stay at
  top of book. Easy to misread when auditing figures.

## Open product decision, not a bug

**Cross-ticker hedging.** The previous claim that it is "not exploitable" was
overstated and the reviewer was right. Holding `SOL-HOT` long against `SOL`
short nets account PnL toward zero while leaving the bonus directional, which
is a valid low-risk score-only strategy: no guaranteed profit, but it weakens
the intended "wins and losses both count double" effect and reduces
liquidation risk. This needs an owner decision (allow it deliberately, or net
the bonus against base exposure), not more analysis.

---

## Running the tests

No network, no fixtures to fetch. Every suite runs against a throwaway SQLite
file and refuses to run against a path under `/opt/`.

```sh
npm install
for t in test-alias test-competition test-scoring test-comp-api test-engine-deep test-integrity; do
  PAPER_DB=$(mktemp -u --suffix=.db) node $t.js
done
```

| Suite | Covers |
|---|---|
| `test-alias` | Alias resolution, isolated-margin forcing, segment gating, segment close |
| `test-competition` | Phase arithmetic for both formats, commit-reveal draw and tamper detection, boundary scheduling, restart safety |
| `test-scoring` | 2x in both directions, bonus/equity separation, frozen checkpoints, prize rules |
| `test-comp-api` | HTTP surface, auth, pre-flight, live wall payload, public draw verification |
| `test-engine-deep` | Drives the real order handler end to end: the gate, both segments, and that ordinary trading is undisturbed |
| `test-integrity` | The ten blockers from review round one, as adversarial regressions |

`test-engine-deep.js` stubs `auth.validateSession` so it can drive
`placeOrder` without a session or a network call.

One test worth reading first, because it is the property most likely to be
broken by a future change: *"the public keeps full leverage while the show
runs"*. An early version of the gate applied to every user, which would have
silently dropped every ordinary paper trader from 1000x to 100x the moment a
round started.

---

## Architecture notes

`competition.js` owns no market data and no account math. `paper.js` injects
`openAlias`, `closeAlias` and `scoreUser` at startup, so the competition layer
is either pure functions or rows in three tables:

```
paper_rounds          one row per round, including the draw commitment
paper_round_players   the roster, and what the wall calls each seat
paper_round_scores    frozen standings, write-once per checkpoint
```

`INSERT OR IGNORE` on the scores table is deliberate: a published checkpoint
must never be recomputed, so re-running a snapshot cannot overwrite it.

Endpoints:

```
GET  /api/paper/comp/state    live wall feed (public)
GET  /api/paper/comp/verify   draw proof (public, unauthenticated by design)
POST /api/paper/comp/admin    operator actions (own token, fails closed)
```

The operator token is required on top of the reverse-proxy gate, because the
gate only proves a request arrived through the site and every ordinary trader
does too.
