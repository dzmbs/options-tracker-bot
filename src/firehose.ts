import { logger } from './utils/logger.js';
import { configureProxyFromEnv } from './utils/proxy.js';
import { BlockTradeRuntime } from './venues/block-trades/block-trade-runtime.js';
import { SpotRuntime } from './venues/spot/index.js';
import { computeBlockTradeAmounts, computeLiveTradeAmounts } from './venues/trade-amounts.js';
import { TradeRuntime } from './venues/trades/trade-runtime.js';

const UNDERLYINGS = (process.env['UNDERLYINGS'] ?? 'BTC,ETH,SOL')
  .split(',')
  .map((u) => u.trim().toUpperCase())
  .filter(Boolean);

const log = logger.child({ component: 'firehose' });

const spot = new SpotRuntime();
const trades = new TradeRuntime();
const blocks = new BlockTradeRuntime();

function referencePrice(underlying: string, indexPrice: number | null): number | null {
  // Prefer the venue-provided index price at trade time; fall back to spot.
  if (indexPrice != null && indexPrice > 0) return indexPrice;
  return spot.getSnapshot(underlying)?.lastPrice ?? null;
}

function fmtUsd(value: number | null): string {
  if (value == null) return '—';
  if (value >= 1_000_000) return `$${(value / 1_000_000).toFixed(2)}M`;
  if (value >= 1_000) return `$${(value / 1_000).toFixed(1)}K`;
  return `$${value.toFixed(2)}`;
}

trades.subscribe((trade) => {
  const ref = referencePrice(trade.underlying, trade.indexPrice);
  const amounts = computeLiveTradeAmounts(trade, ref);
  log.info(
    {
      venue: trade.venue,
      instrument: trade.instrument,
      side: trade.side,
      contracts: amounts.contracts,
      premiumUsd: amounts.premiumUsd,
      notionalUsd: amounts.notionalUsd,
      iv: trade.iv,
      isBlock: trade.isBlock,
    },
    `${trade.venue} ${trade.side.toUpperCase()} ${amounts.contracts} ${trade.instrument} premium=${fmtUsd(amounts.premiumUsd)} notional=${fmtUsd(amounts.notionalUsd)}${trade.isBlock ? ' [BLOCK]' : ''}`,
  );
});

blocks.subscribe((trade) => {
  // Some venues (OKX) emit blocks without USD amounts — recompute from legs
  // using the index price at trade time, falling back to spot.
  const ref = referencePrice(trade.underlying, trade.indexPrice);
  const amounts = computeBlockTradeAmounts(trade, ref);
  const notionalUsd = amounts.notionalUsd;
  log.info(
    {
      venue: trade.venue,
      underlying: trade.underlying,
      strategy: trade.strategy,
      legs: trade.legs.map((leg) => `${leg.direction} ${leg.size}x ${leg.instrument}`),
      totalSize: trade.totalSize,
      premiumUsd: amounts.premiumUsd,
      notionalUsd,
    },
    `BLOCK ${trade.venue} ${trade.underlying} ${trade.strategy ?? 'SINGLE'} ${trade.legs.length} leg(s) premium=${fmtUsd(amounts.premiumUsd)} notional=${fmtUsd(notionalUsd)}`,
  );
});

async function main(): Promise<void> {
  if (configureProxyFromEnv()) {
    log.info('REST proxy enabled from HTTPS_PROXY/HTTP_PROXY');
  }
  log.info({ underlyings: UNDERLYINGS }, 'starting firehose');
  await spot.start(UNDERLYINGS.map((u) => `${u}USDT`));
  await Promise.all([trades.start(UNDERLYINGS), blocks.start()]);
  log.info('firehose running');
}

function shutdown(): void {
  log.info('shutting down');
  trades.dispose();
  blocks.dispose();
  spot.dispose();
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

main().catch((err) => {
  log.error({ err: String(err) }, 'firehose failed to start');
  process.exit(1);
});
