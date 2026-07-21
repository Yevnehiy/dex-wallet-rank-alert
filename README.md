# DEX Whale-Alert Bot

A production system that detects large wallet activity on decentralized exchanges in real time, filters it for genuine intent, and issues directional alerts to Telegram — with every alert logged and backtested for signal quality.

It watches Binance-Futures-listed tokens for momentum, inspects the underlying DEX order flow to find wallets accumulating or distributing ahead of the crowd, confirms candidates against price and market-cap conditions, and tracks every signal to resolution across a 42-cell stop-loss / take-profit grid.

---

## Live alerts

<p align="center">
  <img src="screenshots/telegram-alerts-1.png" width="45%" alt="Telegram alerts — chain badges, direction, signal type, entry timeframe">
  <img src="screenshots/telegram-alerts-2.png" width="45%" alt="Telegram alerts — jump and activity signals across chains">
</p>

Each alert encodes four things at a glance: the **chain** (coloured badge — 🟦 Base, 🔷 Ethereum, 🟨 BSC, 🟪 Solana), the **direction** (🟢 buy / 🔴 sell), the **detection type** (🚀 jump / 📈 step-up / 🔔 activity), and **how the token entered watch** (⚡ 5-minute / 🕒 30-minute / 🗓 24-hour momentum board). Alerts also reconcile the executing wallet against the trade receipt to show the *true token holder* when it differs from the operator address, and report the wallet's remaining balance in USD.

---

## How it works — a two-stage funnel across a split runtime

Detection runs on a dedicated VPS (its egress is not rate-limited by the data providers), while confirmation, alerting and backtest bookkeeping run on Cloudflare's edge with a KV store as shared state. The scanner also relays prices into KV, so the edge never has to call a rate-limited provider directly.

```
 VPS scanner (every 5 min)                          Cloudflare Worker (every 1 min)
 ─────────────────────────                          ──────────────────────────────
 1. Binance Futures boards   ─┐
    5m / 30m / 24h movers     │
 2. Map movers → DEX pools    │   candidates            4. Direction gate + confirm
    (CEX-only skipped,        ├─────────────► KV ──────►   (price / market-cap check)
     next-ranked backfills)   │   + prices              5. Telegram alert
 3. Wallet order-flow         │                         6. Backtest: price trail →
    detection (60-min window)─┘                            42-cell SL/TP grid
```

### Signal taxonomy

Two axes — how a token qualifies, and what a wallet did inside it:

| Detection type | Trigger |
| --- | --- |
| **Jump** | A wallet bursts into the top-4 by volume, having been outside the top-10 an hour earlier. |
| **Step-up** | A wallet in ranks 5–10 climbs at least two positions in an hour. |
| **Activity** | A top-5 wallet trading heavily: ≥3 trades over $10k, >10 trades, or a single position over $50k. |

Each fires for both buy and sell, off any of the three momentum boards.

### Quality filters

- **Minimum size** — a wallet needs ≥ $5,000 of window volume and ≥ $200 per trade.
- **Anti-wash** — a wallet that both bought and sold the same token in the window (opposite-side volume ≥ 25% of the signal side) is discarded, removing volume-painting bots.
- **Direction gate** — a candidate can only confirm while price agrees with the signal; a counter-move beyond 2% invalidates it outright.

Confirmation itself requires the wallet's volume to exceed 0.1% of live market cap, or price to move ≥ 3% in the signal's direction.

### Backtest methodology

Every confirmed alert opens a tracked position. Its price is sampled into a trail until it reaches ±15% or 48 hours; at close, that trail is replayed against a grid of stop-loss (2–10%) and take-profit (2–15%) levels, so any risk-reward configuration can be evaluated from the same data with no re-collection and no look-ahead.

- **Expectancy (R)** — average P/L per signal in units of risk; one R equals the stop distance.
- **Win rate** — share of signals reaching take-profit before stop-loss.

---

## Results (1,199 resolved signals)

The edge is concentrated **off-BSC**. Split by chain, three of four are clearly positive; BSC — 92% of all signals — sits at breakeven and dilutes the aggregate.

| Network | Signals | Best risk-reward | Win rate | Expectancy |
| --- | ---: | --- | ---: | ---: |
| Solana | 52 | 2% / 15% (1:7.5) | 36.4% | **+2.09 R** |
| Base | 16 | 3% / 5% (1:1.7) | 71.4% | **+0.90 R** |
| Ethereum | 27 | 2% / 15% (1:7.5) | 18.5% | **+0.57 R** |
| BSC | 1,104 | 2% / 10% (1:5) | 16.0% | −0.04 R |

Buys beat sells across every type; the 5-minute board underperforms (late entry); the payoff shape is tight-stop / wide-take, consistent with momentum tokens. Smaller per-chain samples are directional rather than final.

`GET /analytics?all=1` on the Worker returns the full breakdown live as JSON.

📄 Full write-up in [`reports/`](reports/) — technical report and one-page summary (PDF).

---

## Tech stack

- **Cloudflare Workers** — confirmation, alerting, backtest, cron scheduling
- **Cloudflare KV** — shared state (candidates, price relay, signal log, aggregates)
- **Node.js** (VPS, systemd) — the detection scanner
- **GeckoTerminal / CoinGecko** — DEX trades, prices, market caps
- **Binance Futures API** — momentum leaderboards
- **Telegram Bot API** — delivery

No framework, no build step, zero runtime dependencies.

## Repository layout

```
worker/
  index.js        Cloudflare Worker: confirmation, alerts, backtest, /analytics
  wrangler.toml   Worker config (KV binding, cron)
scripts/
  scanner.mjs         VPS detection scanner (boards → wallet detection → KV)
  update-token-list.js Pool discovery seeder
  .env.example        Required environment variables
```

## Setup

```bash
# 1. Worker
cd worker
npm i -g wrangler
wrangler secret put TELEGRAM_BOT_TOKEN
wrangler secret put TELEGRAM_CHAT_ID
wrangler deploy

# 2. Scanner (on a VPS)
cd scripts
cp .env.example .env   # fill in Cloudflare + CoinGecko values
node scanner.mjs       # run under systemd with Restart=always for 24/7
```

---

*Personal project. The backtest measures signal quality (directional correctness of alerts), not realised trading P&L.*
