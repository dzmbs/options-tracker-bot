# options-flow-bot

Telegram bot for crypto options market insights and big-trade alerts across
Deribit, OKX, Binance, Bybit, and Derive.

## Status

v0 — big-trade alerts, end to end. Streams every option trade and block/RFQ
trade from all five venues, normalizes USD premium/notional, and pushes
Telegram alerts when prints cross user-defined thresholds.

## Run

```bash
pnpm install
cp .env.example .env           # put your @BotFather token in TELEGRAM_BOT_TOKEN
pnpm bot                       # the Telegram bot (or: pnpm dev for watch mode)
pnpm firehose                  # console trade tape only, no Telegram needed
```

## Bot commands

```
/alert BTC 50k       alert on every BTC trade ≥ $50K — prints and blocks, one feed
/alert ALL 250k      every streamed underlying
/alerts              your alerts
/remove BTC          stop alerting
/tape BTC 25k        recent big trades (in DMs, just type "btc")
/status              per-venue feed health
```

One alert kind, intentionally: users think "show me all trades over $50k",
not prints-vs-blocks — that split stays under the hood. Thresholds match on
**USD premium** (dollars actually paid); blocks missing premium fall back to
notional. Alerts always label premium and notional separately. Amounts accept
`100000`, `100k`, `1.5m`. Startup seed/backfill trades never alert (freshness
filter + uid dedup); block-flagged tape prints are excluded from the print
path so a block never fires twice.

### Running from a geo-blocked region (e.g. US)

Bybit/Binance REST endpoints geo-block some regions (their WS streams still
work). Spot prices fail over automatically; for full REST coverage during
local dev, route REST through a proxy with a non-blocked egress:

```bash
HTTPS_PROXY=http://user:pass@your-proxy:port pnpm firehose
```

Only `fetch` calls are proxied — WebSocket streams connect directly. Hosted
in a non-blocked region (EU/Asia), no proxy is needed.

## Architecture

```
src/
  venues/
    shared/endpoints.ts   venue WS/REST URLs
    types.ts              VenueId + option primitives
    trades/               TradeRuntime — live option trade streams (WS, all 5 venues)
    block-trades/         BlockTradeRuntime — block/RFQ trades (Deribit+Bybit WS, OKX+Binance+Derive poll)
    spot/                 SpotRuntime — reference spot prices, multi-source failover
    trade-amounts.ts      USD premium/notional computation, instrument parsing, trade UIDs
  alerts/engine.ts        threshold matching, seed suppression, uid dedup
  bot/                    grammY commands, alert formatting, rate-limited sender
  store.ts                SQLite (node:sqlite) alert subscriptions
  utils/                  logger (pino), reconnect backoff, proxy, event-loop lag monitor
  index.ts                bot entry — feeds → engine → Telegram
  firehose.ts             console trade tape (no Telegram)
```

Connection reliability is built into the runtimes: per-venue keepalives,
exponential backoff with jitter, a staleness watchdog that force-reconnects
half-open sockets, and rate-limit cooldowns.

## Venue gotchas baked in

- **Deribit/OKX are inverse** — premiums are coin-denominated; USD conversion
  uses the venue index price at trade time, falling back to spot.
- **OKX contract multipliers**: 0.01 BTC / 0.1 ETH per contract.
- **Deribit IV arrives as a percentage** (49.8 = 49.8%); normalized to a fraction.
- **Binance has no public bulk trade-history endpoint** — live stream only.
- **Bybit needs JSON pings** (`{"op":"ping"}`), not WS ping frames.
- **Venue REST geo-blocks vary** — spot prices fail over Bybit → OKX → Deribit.

## AI Use Disclaimer

This codebase has been built with significant AI assistance. A combination of
hand-written code, Codex, and Claude Code was used to create this repository.
