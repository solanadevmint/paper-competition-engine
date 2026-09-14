# Paper competition engine

A perps paper-trading engine with a live competition layer on top of it. It is
the engine behind the Frontier Traders World Cup stops (Seoul, Singapore, the
London final) and behind the paper terminal at [perp.so/ftpaper](https://perp.so/ftpaper).

This repository is the production source, published as deployed. The five
runtime files are byte for byte the build that is live, `8f63a262d0844c84`
(hashes below). What is not here: the deployment runbook, host inventory,
secrets, and the production database.

For the show format as the room sees it, with every wall screen, read the
[engine page](https://paper.perp.so/engine).

## The game

Every round is thirty minutes; the final is twenty. Everyone starts every
round with $100,000 at up to 100x. Real prices, real liquidations, no fees,
no slippage, no real money. Three rules make it a game:

1. **Hot Markets.** Twice a round one asset scores double for two minutes.
   The first fires between 3:15 and 10:00, the second between 13:00 and 20:00,
   always a different asset, with fifteen seconds of warning that does not
   name it. Only the score doubles: gains and losses on the hot asset count
   twice in the score and nowhere else, so the bonus cannot liquidate anyone.
2. **500x Boost.** The last three minutes open at 500x for everyone at once,
   on BTC, ETH, SOL and XRP. Buying power is equity times 500 and moves with
   equity; there is no separate Boost bankroll. Boost tickers are isolated
   margin, so a bad tick costs the stake on that trade, not the round.
3. **The draw is sealed.** One seed decides both Hot assets, both instants and
   a fallback. The hash of the seed is published at the bell and the seed
   after the round, so anyone can check the draw.

Score is account PnL plus the Hot bonus. Ties break on smaller maximum
drawdown, then higher realised PnL, then lower seat number. Eight traders play
two heats of four, a semifinal and a one-against-one final; per stop the
champion takes $2,500, the runner-up $1,500, third $1,000.

The round clock is active time. If the engine cannot price the field it holds
the clock for everyone, and the round still gets its full length.

## What is in the repository

| File | What it is |
|---|---|
| `paper.js` | The engine: accounts, orders, fills at the mark, funding, liquidation, the price index and its failover, candles and tape, the HTTP handlers. |
| `competition.js` | The competition layer: round clock, phase gate, the sealed draw, Hot scoring, Boost window, per-seat proofs at every boundary, placings and prizes. |
| `server.js` | HTTP routing, the gate, the snapshot fallback, SSE and websocket fan-out. |
| `auth-shim.js` | Identity. The engine stores no users; it resolves the site session through two secret-gated endpoints with short caches. |
| `bots.js` | Practice accounts that can fill empty seats so a full round can be rehearsed solo. |
| `migrate-paper-tables.js` | Schema migration for an existing database. |
| `rehearse.js`, `rehearse-production.js` | Scripted rehearsals of a full night against a running engine. |
| `bench-active-round.js`, `BENCHMARKS.md` | The active-round benchmark and its recorded numbers. |
| `test-*.js`, `test-all.js` | The test suite, see below. |
| `phoenix-paper.service` | An example systemd unit. |

Everything is plain Node with two dependencies, `better-sqlite3` and `ws`.
The engine runs on one thread: the same thread prices, fills, scores and
answers requests, so a fill is never decided on a price another thread has
already replaced.

## Prices, fills and risk

Prices come from one source at a time: Pyth Lazer, and if that fails Binance,
then Coinbase. The engine does not blend them; whichever source it is following
is the price, so any liquidation can be checked against that venue. A price
has to be fresh enough for the leverage in play, and 500x needs a fresher
price than 100x. If the engine does not trust a price it holds the clock
instead of trading on it. A clamp gate refuses single ticks that jump too far
from the accepted index.

Orders fill at the mark. One position per user and market, cross or isolated
margin per position. Maintenance margin is half of the initial margin at the
position's leverage, so at 500x the liquidation distance is about five basis
points. Equity is always derived, never stored.

## Verification

Every round boundary writes a per-seat proof: the marks, the positions, the
fills, hashed. A result can be recomputed later and settled scores cannot be
edited. `/api/paper/comp/verify` serves the proofs and the draw reveal.

## Running it

```
npm ci
cp .env.example .env      # then edit
set -a; . ./.env; set +a
node server.js
```

The service binds loopback and expects a reverse proxy in front of it that adds
the gate header; it refuses to serve without a gate secret. State is a single
SQLite file in WAL mode, created on first run. The identity callback assumes a
site that owns the session cookie; without one, practice accounts from
`bots.js` are the way to fill seats.

## Tests

```
npm run check     # syntax of the five runtime files
npm test          # 16 deterministic suites
npm run test:live # the one suite that needs a running engine and the network
```

The deterministic suites run with no network and no production data. On the
files in this repository they pass 495 checks across 16 suites: service
resilience, candles, source expiry, market diagnostics, engine review,
competition push, the two-Hot format, alias tickers, competition lifecycle,
scoring, engine deep checks, the competition API, integrity, migration, review
remediation and the rehearsal runbook. `test-integrity.js` is adversarial
rather than functional: nothing in it describes what a well-behaved player
does, and all of it changes a published result if it regresses.

## API

All routes sit under `/api/paper/` behind the gate.

| Route | Purpose |
|---|---|
| `GET account`, `POST order`, `POST cancel`, `POST close`, `POST sltp`, `POST margin`, `POST reset` | The terminal. |
| `GET fills`, `GET orders`, `GET leaderboard`, `GET config`, `GET time` | History, standings, engine config and clock. |
| `GET candles`, `GET tape`, `GET pyth-history`, `GET pyth-stream`, `index-ws` | Price history and streams. |
| `GET comp/state`, `GET comp/baseline`, `GET comp/verify`, `GET comp/readiness` | The public competition state, the round baseline, the proofs, feed readiness. |
| `GET comp/me`, `POST comp/me/ready`, `GET|POST comp/invite` | A seat's own view, ready-up and invite links. |
| `POST comp/admin` | The operator desk, behind `PAPER_COMP_TOKEN`. It arms rounds, fills seats and drives the wall; it cannot edit a score, a fill or a placement. |

## Provenance

SHA-256 of the runtime files, matching the live build `8f63a262d0844c84`:

```
paper.js        f4f0e57ad9ce5c81f753c0a97ece27db4f9f9febdb35d979b582e73589f77da0
competition.js  36cb4258ead0c24478692c2b7dee4931f1e9881c120c0f8fe226ae4f8acaf2c2
server.js       06aa3c8884c441205eada58bd2d27a236919bceca1bf951dda234926cf00e836
auth-shim.js    4707d6a7467977dd8f2598275a109c2a45b82a77f51193d80fa7213c4d168abc
bots.js         6cb27cbe4822bb274e91462135f9e6cb9f56f1fcef0359bde8b942cd2b616432
```

## Limits

This is a simulation. Fills are at the mark with no queue, no slippage and no
fees, which is more generous than any venue. The index follows one upstream
source at a time and holds the clock when none is trustworthy, so a round can
pause. Liquidity is not modelled beyond the freshness rule. The engine has run
live competition nights, but no code qualification replaces a rehearsal on the
venue's own network.

## License

MIT.
