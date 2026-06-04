import type { Subscription } from '../store.js';
import type { BlockTradeEvent, BlockTradeLeg } from '../venues/block-trades/types.js';
import { parseTradeInstrument, type TradeAmounts } from '../venues/trade-amounts.js';
import type { TradeEvent } from '../venues/trades/types.js';

const MAX_LEGS_SHOWN = 6;

const VENUE_NAMES: Record<string, string> = {
  deribit: 'Deribit',
  okx: 'OKX',
  binance: 'Binance',
  bybit: 'Bybit',
  derive: 'Derive',
};

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

export function escapeHtml(text: string): string {
  return text.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
}

export function fmtUsd(value: number | null): string {
  if (value == null) return '—';
  const abs = Math.abs(value);
  if (abs >= 1_000_000) return `$${(value / 1_000_000).toFixed(2)}M`;
  if (abs >= 1_000) return `$${(value / 1_000).toFixed(1)}K`;
  return `$${value.toFixed(2)}`;
}

function fmtPrice(value: number): string {
  if (value >= 1_000) return `$${value.toLocaleString('en-US', { maximumFractionDigits: 0 })}`;
  if (value >= 1) return `$${value.toFixed(2)}`;
  return `$${value.toFixed(4)}`;
}

function fmtSize(value: number): string {
  return value.toLocaleString('en-US', { maximumFractionDigits: 2 });
}

function venueName(venue: string): string {
  return VENUE_NAMES[venue] ?? venue;
}

function fmtExpiry(iso: string): string {
  const [year, month, day] = iso.split('-');
  if (!year || !month || !day) return iso;
  const monthName = MONTHS[Number(month) - 1] ?? month;
  return `${Number(day)} ${monthName} ${year.slice(2)}`;
}

export function humanInstrument(instrument: string, underlying: string): string {
  const parsed = parseTradeInstrument(instrument);
  if (parsed.expiry == null || parsed.strike == null || parsed.optionType == null) {
    return instrument;
  }
  const right = parsed.optionType === 'call' ? 'Call' : 'Put';
  return `${underlying} ${fmtExpiry(parsed.expiry)} ${fmtSize(parsed.strike)} ${right}`;
}

function sideWord(side: 'buy' | 'sell'): string {
  return side === 'buy' ? '🟢 BOUGHT' : '🔴 SOLD';
}

function legPriceUsd(leg: BlockTradeLeg, indexPrice: number | null): number | null {
  if (leg.priceIsUsd) return leg.price;
  return indexPrice != null && indexPrice > 0 ? leg.price * indexPrice : null;
}

function prettyStrategy(strategy: string | null, legCount: number): string {
  if (strategy == null) return legCount > 1 ? 'Multi-leg Block' : 'Block';
  const words = strategy
    .toLowerCase()
    .split(/[_\s]+/)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');
  return words === 'Custom' ? 'Multi-leg Block' : `${words} Block`;
}

export function formatTradeAlert(trade: TradeEvent, amounts: TradeAmounts): string {
  const contract = humanInstrument(trade.instrument, trade.underlying);
  const head = `${sideWord(trade.side)} ${fmtSize(amounts.contracts)}× <code>${escapeHtml(contract)}</code>`;

  const stats = [
    `Premium <b>${fmtUsd(amounts.premiumUsd)}</b>`,
    `Notional ${fmtUsd(amounts.notionalUsd)}`,
  ];
  if (trade.iv != null) stats.push(`IV ${(trade.iv * 100).toFixed(1)}%`);
  if (amounts.referencePriceUsd != null) stats.push(`Spot ${fmtPrice(amounts.referencePriceUsd)}`);

  return `${head}\n${stats.join(' · ')}\n<i>${venueName(trade.venue)}</i>`;
}

export function formatBlockAlert(
  trade: BlockTradeEvent,
  premiumUsd: number | null,
  notionalUsd: number | null,
): string {
  const lines = [
    `📦 <b>${escapeHtml(trade.underlying)} ${escapeHtml(prettyStrategy(trade.strategy, trade.legs.length))}</b>`,
    `Premium <b>${fmtUsd(premiumUsd)}</b> · Notional ${fmtUsd(notionalUsd)} · <i>${venueName(trade.venue)}</i>`,
  ];

  for (const leg of trade.legs.slice(0, MAX_LEGS_SHOWN)) {
    const contract = humanInstrument(leg.instrument, trade.underlying);
    const price = legPriceUsd(leg, trade.indexPrice);
    lines.push(
      `${sideWord(leg.direction)} ${fmtSize(leg.size)}× <code>${escapeHtml(contract)}</code>${
        price != null ? ` @ ${fmtPrice(price)}` : ''
      }`,
    );
  }
  if (trade.legs.length > MAX_LEGS_SHOWN) {
    lines.push(`… ${trade.legs.length - MAX_LEGS_SHOWN} more leg(s)`);
  }
  return lines.join('\n');
}

export function formatAlertList(subs: Subscription[]): string {
  if (subs.length === 0)
    return 'No alerts set.\nUse /alert &lt;underlying&gt; &lt;premium&gt; to add one.';
  const lines = ['<b>Your alerts</b>'];
  for (const sub of subs) {
    const scope = sub.underlying === '*' ? 'ALL' : sub.underlying;
    lines.push(`🔔 <b>${escapeHtml(scope)}</b> — trades ≥ <b>${fmtUsd(sub.minUsd)}</b> notional`);
  }
  return lines.join('\n');
}
