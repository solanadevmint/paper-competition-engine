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
| `test-*.js` | 94 assertions, all passing. |

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
simultaneously. We concluded this is not exploitable — hedging halves your
upside rather than creating free money, since the base leg cancels the account
PnL that the bonus would otherwise double. **Worth a second opinion.**

**Boost twins are forced isolated-margin** (`cfgOf()`). At 1000x the
liquidation distance is ~5bps. Measured index-noise breach rates over a
two-minute window: BTC under 1%, ETH/BNB around 7%, XRP/SOL around 15%.
Isolating the margin means a noise liquidation costs the stake committed to
the Boost rather than ending the player's round.

**The draw is commit-reveal.** `sha256(seed + "|" + candidates)` is published
when the round is armed; the seed is published at the reveal. Anyone can
recompute the winner. The commitment covers the candidate list too, so the
shortlist cannot be swapped after the fact either. The seed is held in memory
until the reveal and a restart in between fails loudly rather than drawing
from an uncommitted seed. See `verifyDraw()`.

**Phases fire from a server timer anchored to the round start**, never from a
client poll. Boundaries already past are replayed in order on boot, and each
fires at most once, so an engine restart mid-round resumes rather than stalls.

**The bell marks, it does not close.** `bell()` snapshots every account first,
then shuts the segment gates *without* flattening. An earlier version closed
positions first, which turned final marks into realised exits and would have
rewarded whoever closed fastest. The test for this is
`test-engine-deep.js` → "the bell marks everyone without closing positions".

---

## Known gaps

- **Maximum drawdown is not tracked per round.** The format's first tie-break
  is lowest drawdown; ranking currently falls through to realised PnL, then
  seat. Flagged in `standings()`.
- **A flip clears the legacy per-position boost clock.** In `applyFill`, the
  flip branches insert a fresh position row without stamping `boost_since`, so
  flipping escapes the old 2:00 auto-settle. This affects the public paper
  product, not the competition (which gates on phase instead). Unfixed.
- **No rate limiting on the competition endpoints** beyond the engine's
  existing per-user write limiter.
- The engine runs a $10 bankroll displayed as $100,000 (a 10,000x display
  scale). Order sizes execute at 1/10,000th of displayed size so fills stay at
  top of book. Easy to misread when auditing figures.

---

## Running the tests

No network, no fixtures to fetch. Every suite runs against a throwaway SQLite
file and refuses to run against a path under `/opt/`.

```sh
npm install
for t in test-alias test-competition test-scoring test-comp-api test-engine-deep; do
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
