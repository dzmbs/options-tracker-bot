import { type Api, Bot, type CommandContext, type Context, InputFile } from 'grammy';

import { ANY_UNDERLYING, type Store } from '../store.js';
import { feedLogger } from '../utils/logger.js';
import { parseHumanExpiry } from '../venues/trade-amounts.js';
import { escapeHtml, fmtUsd, formatAlertList } from './format.js';

const log = feedLogger('bot');

const MIN_THRESHOLD_USD = 100;
const MAX_THRESHOLD_USD = 100_000_000;
const SEND_GAP_MS = 100;
const MAX_QUEUE_LENGTH = 500;
const MAX_ALERT_AGE_MS = 5 * 60 * 1000;

const EXPIRY_CMD_MONTHS = [
  'JAN',
  'FEB',
  'MAR',
  'APR',
  'MAY',
  'JUN',
  'JUL',
  'AUG',
  'SEP',
  'OCT',
  'NOV',
  'DEC',
];

export interface ExpiryRow {
  expiry: string;
  venues: string[];
}

export interface BotDeps {
  store: Store;
  underlyings: string[];
  oi: (underlying: string, expiry: string, venue?: string) => Promise<Buffer>;
  listExpiries: (underlying: string) => Promise<ExpiryRow[]>;
}

export class Sender {
  private readonly queue: Array<{ chatId: number; text: string; enqueuedAt: number }> = [];
  private draining = false;

  constructor(private readonly api: Api) {}

  enqueue(chatId: number, text: string): void {
    if (this.queue.length >= MAX_QUEUE_LENGTH) {
      this.queue.shift();
      log.warn({ queued: this.queue.length }, 'send queue full — dropping oldest alert');
    }
    this.queue.push({ chatId, text, enqueuedAt: Date.now() });
    void this.drain();
  }

  private async drain(): Promise<void> {
    if (this.draining) return;
    this.draining = true;
    let droppedStale = 0;
    try {
      while (this.queue.length > 0) {
        const message = this.queue.shift();
        if (!message) break;
        if (Date.now() - message.enqueuedAt > MAX_ALERT_AGE_MS) {
          droppedStale += 1;
          continue;
        }
        try {
          await this.api.sendMessage(message.chatId, message.text, {
            parse_mode: 'HTML',
            link_preview_options: { is_disabled: true },
          });
        } catch (err: unknown) {
          log.warn({ chatId: message.chatId, err: String(err) }, 'alert send failed');
        }
        await new Promise((resolve) => setTimeout(resolve, SEND_GAP_MS));
      }
    } finally {
      this.draining = false;
      if (droppedStale > 0) {
        log.warn({ droppedStale }, 'dropped stale alerts older than send window');
      }
    }
  }
}

/** Parses "100000", "100k", "1.5m", "$250K". Returns null on garbage. */
export function parseUsd(raw: string): number | null {
  const match = raw.trim().match(/^\$?(\d+(?:\.\d+)?)([km])?$/i);
  if (!match) return null;
  const base = Number(match[1]);
  if (!Number.isFinite(base)) return null;
  const suffix = match[2]?.toLowerCase();
  return base * (suffix === 'm' ? 1_000_000 : suffix === 'k' ? 1_000 : 1);
}

/** '2026-12-25' → '25DEC26' — used to hint users what to type for /oi. */
function toExpiryCmd(iso: string): string {
  const [year, month, day] = iso.split('-');
  if (!year || !month || !day) return iso;
  const mon = EXPIRY_CMD_MONTHS[Number(month) - 1] ?? month;
  return `${Number(day)}${mon}${year.slice(2)}`;
}

function helpText(underlyings: string[]): string {
  const list = underlyings.join(', ');
  const example = underlyings[0] ?? 'BTC';
  return [
    '<b>Options Flow Alerts</b>',
    'Every options trade across Deribit, OKX, Binance, Bybit and Derive in one feed.',
    '',
    `/alert ${example} 50k — alert on ${example} trades ≥ $50K notional`,
    '/alert ALL 250k — all underlyings',
    '/alert — your alerts',
    `/alert_remove ${example} — remove alert`,
    '',
    `/oi ${example} 25DEC26 — puts vs calls OI by strike`,
    `/oi ${example} — list available expiries`,
    '',
    `Underlyings: ${escapeHtml(list)}`,
    'Amounts: 100000 = 100k = 0.1m',
  ].join('\n');
}

export function createBot(token: string, deps: BotDeps): Bot {
  const bot = new Bot(token);
  const validUnderlyings = new Set(deps.underlyings.map((u) => u.toUpperCase()));

  function parseTarget(raw: string): string | null {
    const upper = raw.trim().toUpperCase();
    if (upper === 'ALL' || upper === ANY_UNDERLYING) return ANY_UNDERLYING;
    if (validUnderlyings.has(upper)) return upper;
    return null;
  }

  function badUnderlying(): string {
    return `Unknown underlying. Available: ${escapeHtml(deps.underlyings.join(', '))} or ALL`;
  }

  bot.command(['start', 'help'], (ctx) =>
    ctx.reply(helpText(deps.underlyings), { parse_mode: 'HTML' }),
  );

  bot.command('alert', (ctx: CommandContext<Context>) => {
    const match = ctx.match.trim();
    if (!match) {
      return ctx.reply(formatAlertList(deps.store.listByChat(ctx.chat.id)), { parse_mode: 'HTML' });
    }
    const [rawUnderlying, rawUsd, ...rest] = match.split(/\s+/);
    if (rawUnderlying?.toLowerCase() === 'remove') {
      if (!rawUsd || rest.length > 0) {
        return ctx.reply(
          'Usage: /alert remove <underlying>\nExamples: /alert remove BTC, /alert remove ALL',
        );
      }
      const target = parseTarget(rawUsd);
      if (target == null) return ctx.reply(badUnderlying());
      const removed = deps.store.remove(ctx.chat.id, target);
      return ctx.reply(removed ? '✅ Removed.' : 'No alert set for that underlying.');
    }
    if (!rawUnderlying || !rawUsd || rest.length > 0) {
      return ctx.reply(
        'Usage: /alert <underlying> <notional>\nExamples: /alert BTC 50k, /alert ALL 250k\nMinimum: $100 notional. Amounts: 50k = 50000, 1.5m = 1500000',
      );
    }
    const underlying = parseTarget(rawUnderlying);
    if (underlying == null) return ctx.reply(badUnderlying());
    const usd = parseUsd(rawUsd);
    if (usd == null || usd < MIN_THRESHOLD_USD) {
      return ctx.reply(
        `Minimum notional: ${fmtUsd(MIN_THRESHOLD_USD)}. Amounts: 1k = 1000, 1.5m = 1500000`,
      );
    }
    if (usd > MAX_THRESHOLD_USD) {
      return ctx.reply(`Maximum notional threshold: ${fmtUsd(MAX_THRESHOLD_USD)}.`);
    }
    deps.store.upsert(ctx.chat.id, underlying, usd);
    const scope = underlying === ANY_UNDERLYING ? 'all underlyings' : underlying;
    return ctx.reply(
      `🔔 Alerting on <b>${scope}</b> options trades ≥ <b>${fmtUsd(usd)}</b> notional.`,
      { parse_mode: 'HTML' },
    );
  });

  bot.command('alerts', (ctx) =>
    ctx.reply(formatAlertList(deps.store.listByChat(ctx.chat.id)), { parse_mode: 'HTML' }),
  );

  bot.command('alert_remove', (ctx: CommandContext<Context>) => {
    const raw = ctx.match.trim();
    if (!raw)
      return ctx.reply(
        'Usage: /alert_remove <underlying>\nExamples: /alert_remove BTC, /alert_remove ALL',
      );
    const target = parseTarget(raw);
    if (target == null) return ctx.reply(badUnderlying());
    const removed = deps.store.remove(ctx.chat.id, target);
    return ctx.reply(removed ? '✅ Removed.' : 'No alert set for that underlying.');
  });

  bot.command('remove', (ctx: CommandContext<Context>) => {
    const raw = ctx.match.trim();
    if (!raw)
      return ctx.reply(
        'Usage: /alert_remove <underlying>\nExamples: /alert_remove BTC, /alert_remove ALL',
      );
    const target = parseTarget(raw);
    if (target == null) return ctx.reply(badUnderlying());
    const removed = deps.store.remove(ctx.chat.id, target);
    return ctx.reply(removed ? '✅ Removed.' : 'No alert set for that underlying.');
  });

  bot.command('oi', async (ctx: CommandContext<Context>) => {
    const [rawUnderlying, rawExpiry, rawVenue] = ctx.match.trim().split(/\s+/);

    if (!rawUnderlying) {
      return ctx.reply(
        'Usage: /oi <underlying> <expiry> [venue]\nExamples: /oi BTC 25DEC26, /oi ETH 27JUN26, /oi BTC 25DEC26 deribit\nVenues: deribit, okx, bybit, binance, derive (omit for all)\nRun /oi BTC to list available expiries.',
      );
    }

    const underlying = parseTarget(rawUnderlying);
    if (underlying == null || underlying === ANY_UNDERLYING) {
      return ctx.reply(badUnderlying());
    }

    if (!rawExpiry) {
      try {
        const rows = await deps.listExpiries(underlying);
        if (rows.length === 0) {
          return ctx.reply(`No expiries found for ${underlying}.`);
        }
        const lines = rows.map((r) => {
          const cmd = toExpiryCmd(r.expiry);
          const venueStr = r.venues.map((v) => v.charAt(0).toUpperCase() + v.slice(1)).join(' · ');
          return `<code>${escapeHtml(cmd)}</code>  ${escapeHtml(venueStr)}`;
        });
        const first = rows[0];
        const hint = first != null ? toExpiryCmd(first.expiry) : '';
        return ctx.reply(
          `<b>${escapeHtml(underlying)} expiries</b>\n\n${lines.join('\n')}\n\n/oi ${escapeHtml(underlying)} ${escapeHtml(hint)}`,
          { parse_mode: 'HTML' },
        );
      } catch (err: unknown) {
        log.warn({ err: String(err) }, 'listExpiries failed');
        return ctx.reply('Could not fetch expiries.');
      }
    }

    let expiry: string | null = null;
    if (/^\d{4}-\d{2}-\d{2}$/.test(rawExpiry)) {
      expiry = rawExpiry;
    } else {
      expiry = parseHumanExpiry(rawExpiry.toUpperCase());
    }
    if (expiry == null) {
      return ctx.reply('Bad expiry — use 25DEC26 or 2026-12-25.');
    }

    const venue = rawVenue?.toLowerCase();

    await ctx.replyWithChatAction('upload_photo');

    try {
      const img = await deps.oi(underlying, expiry, venue);
      return ctx.replyWithPhoto(new InputFile(img, 'oi.png'));
    } catch (err: unknown) {
      log.warn({ err: String(err) }, 'OI chart failed');
      return ctx.reply(`Chart failed — ${err instanceof Error ? err.message : String(err)}`);
    }
  });

  bot.catch((err) => {
    log.warn({ err: String(err.error) }, 'bot handler error');
  });

  return bot;
}

export async function registerCommandMenu(bot: Bot): Promise<void> {
  await bot.api.setMyCommands([
    { command: 'alert', description: 'Set notional alert: /alert <underlying> <notional>' },
    { command: 'alert_remove', description: 'Remove alert: /alert_remove <underlying>' },
    { command: 'oi', description: 'OI by strike: /oi <underlying> <expiry> [venue]' },
    { command: 'help', description: 'How to use this bot' },
  ]);
}
