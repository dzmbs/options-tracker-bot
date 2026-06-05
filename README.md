<p align="center">
  <img src="./assets/banner.png" width="420" />
</p>

# options-flow-bot

Telegram bot for crypto options flow across Deribit, OKX, Binance, Bybit, and Derive.

## Run

```bash
pnpm install
cp .env.example .env   # add TELEGRAM_BOT_TOKEN from @BotFather
pnpm bot               # start bot (pnpm dev for watch mode)
pnpm firehose          # trade tape without Telegram
```

## Commands

```
/alert BTC 500k        alert on BTC trades ≥ $500K notional
/alert ALL 1m          all underlyings
/alert                 list your alerts
/alert_remove BTC      remove alert
/oi BTC 25DEC26        puts vs calls OI by strike
/oi BTC                list available expiries
/help                  command reference
```

Amounts: `50k`, `1.5m`, `500000` all work.

## Proxy (geo-blocked regions)

```bash
HTTPS_PROXY=http://user:pass@proxy:port pnpm bot
```

## Layout

```
src/
  venues/trades/       live option trade streams (WS, all 5 venues)
  venues/block-trades/ block/RFQ trades
  venues/spot/         spot price feeds
  venues/trade-amounts.ts  USD normalization, instrument parsing
  alerts/engine.ts     threshold matching, dedup
  oi/                  OI chart (venue REST → PNG via @napi-rs/canvas)
  bot/                 grammY commands, formatting
  store.ts             SQLite subscriptions
```

## AI Use Disclaimer

Built with a combination of hand-written code, Codex, and Claude Code.
