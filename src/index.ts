import { AlertEngine } from './alerts/engine.js';
import { createBot, registerCommandMenu, Sender } from './bot/bot.js';
import { formatBlockAlert, formatTradeAlert } from './bot/format.js';
import { renderOiChart } from './oi/oi-chart.js';
import { fetchExpiriesWithVenues, fetchOi } from './oi/oi-client.js';
import { Store } from './store.js';
import { logger } from './utils/logger.js';
import { configureProxyFromEnv } from './utils/proxy.js';
import { BlockTradeRuntime } from './venues/block-trades/block-trade-runtime.js';
import { SpotRuntime } from './venues/spot/index.js';
import { computeBlockTradeAmounts, computeLiveTradeAmounts } from './venues/trade-amounts.js';
import { TradeRuntime } from './venues/trades/trade-runtime.js';

try {
  process.loadEnvFile();
} catch {
  // no .env file — env vars come from the environment
}

const log = logger.child({ component: 'main' });

const token = process.env['TELEGRAM_BOT_TOKEN'];
if (!token) {
  log.error(
    'TELEGRAM_BOT_TOKEN is not set — create a bot with @BotFather and put the token in .env',
  );
  process.exit(1);
}

const UNDERLYINGS = (process.env['UNDERLYINGS'] ?? 'BTC,ETH,SOL')
  .split(',')
  .map((u) => u.trim().toUpperCase())
  .filter(Boolean);

const store = new Store();
const engine = new AlertEngine(store);
const spot = new SpotRuntime();
const trades = new TradeRuntime();
const blocks = new BlockTradeRuntime();

function referencePrice(underlying: string, indexPrice: number | null): number | null {
  if (indexPrice != null && indexPrice > 0) return indexPrice;
  return spot.getSnapshot(underlying)?.lastPrice ?? null;
}

const bot = createBot(token, {
  store,
  underlyings: UNDERLYINGS,
  oi: async (underlying, expiry, venue) => {
    const spotPrice = spot.getSnapshot(underlying)?.lastPrice ?? null;
    const data = await fetchOi(underlying, expiry, spotPrice, venue);
    return renderOiChart(data);
  },
  listExpiries: (underlying) => fetchExpiriesWithVenues(underlying),
});

const sender = new Sender(bot.api);

trades.subscribe((trade) => {
  const amounts = computeLiveTradeAmounts(
    trade,
    referencePrice(trade.underlying, trade.indexPrice),
  );
  const targets = engine.evaluateTrade(trade, amounts);
  if (targets == null) return;
  const text = formatTradeAlert(trade, amounts);
  for (const { chatId, threadId } of targets) sender.enqueue(chatId, threadId, text);
});

blocks.subscribe((trade) => {
  const ref = referencePrice(trade.underlying, trade.indexPrice);
  const amounts = computeBlockTradeAmounts(trade, ref);
  const fallbackPremiumUsd = trade.rawAmountUsd > 0 ? trade.rawAmountUsd : null;
  const effectivePremiumUsd = amounts.premiumUsd ?? fallbackPremiumUsd;
  const displayNotionalUsd = amounts.notionalUsd;
  if (effectivePremiumUsd == null && displayNotionalUsd == null) {
    log.warn(
      { venue: trade.venue, underlying: trade.underlying, tradeId: trade.tradeId },
      'block trade has no USD reference — alert evaluation skipped',
    );
  }
  const targets = engine.evaluateBlock(trade, effectivePremiumUsd, displayNotionalUsd);
  if (targets == null) return;
  const text = formatBlockAlert(trade, effectivePremiumUsd, displayNotionalUsd);
  for (const { chatId, threadId } of targets) sender.enqueue(chatId, threadId, text);
});

async function main(): Promise<void> {
  if (configureProxyFromEnv()) {
    log.info('REST proxy enabled from HTTPS_PROXY/HTTP_PROXY');
  }

  const me = await bot.api.getMe();
  log.info({ username: me.username, underlyings: UNDERLYINGS }, 'bot authenticated');
  await registerCommandMenu(bot);

  await spot.start(UNDERLYINGS.map((u) => `${u}USDT`));
  await Promise.all([trades.start(UNDERLYINGS), blocks.start()]);
  log.info('feeds running — starting long polling');

  await bot.start({
    onStart: () => log.info('bot is live'),
  });
}

function shutdown(): void {
  log.info('shutting down');
  void bot.stop();
  trades.dispose();
  blocks.dispose();
  spot.dispose();
  engine.dispose();
  store.close();
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

process.on('unhandledRejection', (reason) => {
  log.error({ reason: String(reason) }, 'unhandled promise rejection');
});

process.on('uncaughtException', (err) => {
  log.error({ err: String(err) }, 'uncaught exception — process will exit');
  process.exit(1);
});

main().catch((err) => {
  log.error({ err: String(err) }, 'failed to start');
  process.exit(1);
});
