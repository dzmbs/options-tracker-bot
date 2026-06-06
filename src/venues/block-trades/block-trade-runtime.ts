import WebSocket from 'ws';
import { z } from 'zod';

import { feedLogger } from '../../utils/logger.js';
import { backoffDelay, flapBackoffDelay } from '../../utils/reconnect.js';
import {
  BINANCE_BLOCK_TRADES,
  BINANCE_REST_BASE_URL,
  BYBIT_RFQ_WS_URL,
  DERIBIT_GET_BLOCK_RFQ_TRADES,
  DERIBIT_REST_BASE_URL,
  DERIBIT_WS_URL,
  DERIVE_GET_TRADE_HISTORY,
  DERIVE_REST_BASE_URL,
  OKX_REST_BASE_URL,
  OKX_RFQ_PUBLIC_TRADES,
} from '../shared/endpoints.js';
import type { VenueId } from '../types.js';
import { createBlockVenueState, mergeBlockVenueState } from './health.js';
import { BLOCK_TRADE_RUNTIME_BUFFER_SIZE, insertBlockTrades } from './retention.js';
import type {
  BlockTradeEvent,
  BlockTradeRuntimeHealth,
  BlockVenuePoller,
  BlockVenueState,
  BlockVenueStream,
  BlockVenueStreamHandlers,
} from './types.js';

const log = feedLogger('block-trade-runtime');
const RATE_LIMIT_COOLDOWN_MS = 90 * 1000;
const DERIBIT_BLOCK_KEEPALIVE_MS = 25_000;
// Liveness watchdog for the WS streams: keepalives produce inbound frames
// every 20-25s, so a 5-minute silence is an unambiguous half-open socket
// (close never fires on those — without this, blocks stop silently).
const STALENESS_THRESHOLD_MS = 5 * 60 * 1000;
const WATCHDOG_INTERVAL_MS = 30 * 1000;
// Sessions dying younger than this never reset the flap streak — prevents
// tight reconnect loops when the venue accepts then drops connections.
const STABLE_SESSION_FLOOR_MS = 30 * 1000;

function isRateLimitSignal(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return /(^|\D)429(\D|$)|over_limit|rate\s*limit|too many requests/i.test(message);
}

const DERIBIT_SEED_COUNT = 250;
// Deribit docs say max 1000 but the API enforces [10, 50].
const DERIBIT_SEED_PAGE_SIZE = 50;
const DERIBIT_SEED_RETRIES = 5;
const DERIVE_INITIAL_LOOKBACK_MS = 7 * 24 * 60 * 60 * 1000;
const DERIVE_POLL_OVERLAP_MS = 60_000;
const DERIVE_PAGE_SIZE = 1_000;
const MAX_DERIVE_PAGES_PER_POLL = 25;

/**
 * Aggregates block/RFQ trades across all venues.
 * Deribit + Bybit via WebSocket (real-time), OKX + Binance + Derive via REST polling.
 */
// After this many consecutive failures the poller backs off.
const POLL_BACKOFF_THRESHOLD = 3;
// Max skip intervals: 2^min(streak-threshold, 5) — 2, 4, 8, 16, 32 intervals skipped.
const POLL_MAX_BACKOFF_STEPS = 5;

export class BlockTradeRuntime {
  private buffer: BlockTradeEvent[] = [];
  private seenTradeTimestamps = new Map<string, number>();
  private streams: BlockVenueStream[] = [];
  private pollTimers: ReturnType<typeof setInterval>[] = [];
  private listeners = new Set<(trade: BlockTradeEvent) => void>();
  private venueState = new Map<VenueId, BlockVenueState>();
  private readonly pollerInFlight = new Set<VenueId>();
  private readonly pollFailureStreak = new Map<VenueId, number>();
  private readonly pollSkipCounter = new Map<VenueId, number>();

  async start(): Promise<void> {
    const wsStreams = [deribitBlockStream(), bybitBlockStream()];
    const pollers = [okxBlockPoller(), binanceBlockPoller(), deriveBlockPoller()];

    for (const stream of wsStreams) {
      this.venueState.set(stream.venue, createBlockVenueState('ws', null));
      stream.connect({
        onTrades: (trades) => {
          this.recordPollResult(stream.venue, trades.length, null);
          this.pushTrades(trades);
        },
        onConnected: () => {
          this.updateVenueState(stream.venue, { connected: true, lastStatusAt: Date.now() });
        },
        onDisconnected: () => {
          this.updateVenueState(stream.venue, { connected: false, lastStatusAt: Date.now() });
        },
        onError: () => {
          this.updateVenueState(stream.venue, {
            errors: (this.venueState.get(stream.venue)?.errors ?? 0) + 1,
            lastStatusAt: Date.now(),
          });
        },
        onReconnect: () => {
          this.updateVenueState(stream.venue, {
            reconnects: (this.venueState.get(stream.venue)?.reconnects ?? 0) + 1,
            lastStatusAt: Date.now(),
          });
        },
      });
      this.streams.push(stream);
    }

    for (const poller of pollers) {
      this.venueState.set(poller.venue, createBlockVenueState('poll', poller.limit ?? null));
      void this.runPoller(poller, true);
      const timer = setInterval(() => {
        void this.runPoller(poller, false);
      }, poller.intervalMs);
      this.pollTimers.push(timer);
    }

    log.info('block flow service started');
  }

  getTrades(underlying?: string): BlockTradeEvent[] {
    if (!underlying) return this.buffer;
    const upper = underlying.toUpperCase();
    return this.buffer.filter((t) => t.underlying === upper);
  }

  subscribe(listener: (trade: BlockTradeEvent) => void): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  getHealth(): BlockTradeRuntimeHealth[] {
    return Array.from(this.venueState.entries()).map(([venue, state]) => ({
      venue,
      transport: state.transport,
      connected: state.connected,
      lastSuccessAt: state.lastSuccessAt,
      lastTradeAt: state.lastTradeAt,
      lastStatusAt: state.lastStatusAt,
      lastPollCount: state.lastPollCount,
      pollLimit: state.pollLimit,
      hitLimitCount: state.hitLimitCount,
      reconnects: state.reconnects,
      errors: state.errors,
      bufferedTrades: this.buffer.filter((trade) => trade.venue === venue).length,
    }));
  }

  private async runPoller(poller: BlockVenuePoller, initial: boolean): Promise<void> {
    if (!initial && this.pollerInFlight.has(poller.venue)) {
      log.debug({ venue: poller.venue }, 'block poll skipped — previous poll still in flight');
      return;
    }

    if (!initial) {
      const streak = this.pollFailureStreak.get(poller.venue) ?? 0;
      if (streak >= POLL_BACKOFF_THRESHOLD) {
        const steps = Math.min(streak - POLL_BACKOFF_THRESHOLD, POLL_MAX_BACKOFF_STEPS);
        const skipEvery = 2 ** (steps + 1); // 4, 8, 16, 32, 64 intervals
        const counter = this.pollSkipCounter.get(poller.venue) ?? 0;
        if (counter < skipEvery - 1) {
          this.pollSkipCounter.set(poller.venue, counter + 1);
          return;
        }
        this.pollSkipCounter.set(poller.venue, 0);
      }
    }

    this.pollerInFlight.add(poller.venue);
    try {
      const trades = await poller.poll();
      const prevStreak = this.pollFailureStreak.get(poller.venue) ?? 0;
      if (prevStreak > 0) {
        log.info({ venue: poller.venue }, 'block trade poll recovered');
        this.pollFailureStreak.set(poller.venue, 0);
        this.pollSkipCounter.set(poller.venue, 0);
      }
      this.recordPollResult(poller.venue, trades.length, poller.limit ?? null);
      this.pushTrades(trades);
    } catch (err: unknown) {
      const streak = (this.pollFailureStreak.get(poller.venue) ?? 0) + 1;
      this.pollFailureStreak.set(poller.venue, streak);
      this.updateVenueState(poller.venue, {
        errors: (this.venueState.get(poller.venue)?.errors ?? 0) + 1,
        lastStatusAt: Date.now(),
      });
      // Only log at warn for the first few failures; after that back off to info to reduce noise.
      const level = streak <= POLL_BACKOFF_THRESHOLD ? 'warn' : 'info';
      log[level](
        { venue: poller.venue, err: String(err), streak },
        initial ? 'initial block trade poll failed' : 'block trade poll failed',
      );
    } finally {
      this.pollerInFlight.delete(poller.venue);
    }
  }

  private updateVenueState(venue: VenueId, patch: Partial<BlockVenueState>): void {
    const current = this.venueState.get(venue);
    if (!current) return;
    this.venueState.set(venue, mergeBlockVenueState(current, patch));
  }

  private recordPollResult(venue: VenueId, tradeCount: number, limit: number | null): void {
    const now = Date.now();
    const state = this.venueState.get(venue);

    this.updateVenueState(venue, {
      connected: true,
      lastSuccessAt: now,
      lastStatusAt: now,
      lastPollCount: tradeCount,
      pollLimit: limit,
      hitLimitCount:
        limit != null && tradeCount >= limit
          ? (state?.hitLimitCount ?? 0) + 1
          : (state?.hitLimitCount ?? 0),
    });

    if (limit != null && tradeCount >= limit) {
      log.warn({ venue, count: tradeCount, limit }, 'block trade poll hit limit');
    }
  }

  private pushTrades(trades: BlockTradeEvent[]): void {
    const { inserted, latestByVenue } = insertBlockTrades(
      this.buffer,
      this.seenTradeTimestamps,
      trades,
      BLOCK_TRADE_RUNTIME_BUFFER_SIZE,
    );

    for (const [venue, timestamp] of latestByVenue) {
      this.updateVenueState(venue, { lastTradeAt: timestamp });
    }

    for (const trade of inserted) {
      for (const listener of this.listeners) {
        try {
          listener(trade);
        } catch (error: unknown) {
          log.warn(
            { err: String(error), venue: trade.venue, tradeId: trade.tradeId },
            'block trade listener failed',
          );
        }
      }
    }
  }

  dispose(): void {
    for (const s of this.streams) s.dispose();
    this.streams = [];
    for (const t of this.pollTimers) clearInterval(t);
    this.pollTimers = [];
  }
}

function extractUnderlying(instrument: string): string {
  return (instrument.split('-')[0] ?? instrument).replace(/_.*$/, '').toUpperCase();
}

function isOptionInstrument(instrument: string): boolean {
  const parts = instrument.split('-');
  // Bybit appended a currency suffix in 2026: BTC-28MAR26-60000-C-USDT.
  // Find C/P from the end so both old and new formats work.
  let typeIdx = -1;
  for (let i = parts.length - 1; i >= 0; i--) {
    if (parts[i] === 'C' || parts[i] === 'P') {
      typeIdx = i;
      break;
    }
  }
  if (typeIdx < 0) return false;
  const strike = parts[typeIdx - 1];
  const expiry = parts[typeIdx - 2];
  const hasStrike = strike != null && /^[0-9]+(?:\.[0-9]+)?$/.test(strike);
  const hasExpiry =
    expiry != null && (/^\d{1,2}[A-Z]{3}\d{2}$/.test(expiry) || /^\d{6,8}$/.test(expiry));
  return hasStrike && hasExpiry;
}

function areOptionLegs(instruments: string[]): boolean {
  return (
    instruments.length > 0 && instruments.every((instrument) => isOptionInstrument(instrument))
  );
}

async function fetchJson(url: string): Promise<unknown> {
  const response = await fetch(url, { signal: AbortSignal.timeout(10_000) });
  if (!response.ok) throw new Error(`HTTP ${response.status} from ${url}`);
  return response.json() as Promise<unknown>;
}

export const DeribitBlockRfqSchema = z.object({
  id: z.number(),
  timestamp: z.number(),
  amount: z.number(),
  direction: z.enum(['buy', 'sell']),
  mark_price: z.number().optional(),
  combo_id: z.string().nullable().optional(),
  index_prices: z.record(z.string(), z.number()).optional(),
  legs: z.array(
    z.object({
      price: z.number(),
      direction: z.enum(['buy', 'sell']),
      instrument_name: z.string(),
      ratio: z.number(),
    }),
  ),
});

const DeribitBlockRfqResponseSchema = z.object({
  result: z
    .object({
      block_rfqs: z.array(DeribitBlockRfqSchema),
      continuation: z.string().nullable().optional(),
    })
    .optional(),
});

function mapDeribitBlockTrade(
  trade: z.infer<typeof DeribitBlockRfqSchema>,
): BlockTradeEvent | null {
  const instruments = trade.legs.map((leg) => leg.instrument_name);
  if (!areOptionLegs(instruments)) return null;

  const underlying = extractUnderlying(trade.legs[0]?.instrument_name ?? 'BTC');
  const indexPriceEntries = Object.entries(trade.index_prices ?? {});
  const indexPrice = indexPriceEntries[0]?.[1] ?? null;

  return {
    venue: 'deribit',
    tradeId: String(trade.id),
    timestamp: trade.timestamp,
    underlying,
    direction: trade.direction,
    strategy: deriveStrategy(trade.combo_id, trade.legs.length),
    legs: trade.legs.map((leg) => ({
      instrument: leg.instrument_name,
      direction: leg.direction,
      price: leg.price,
      priceIsUsd: false,
      size: trade.amount,
      ratio: leg.ratio,
    })),
    totalSize: trade.amount,
    rawAmountUsd: 0,
    indexPrice,
  };
}

async function fetchDeribitSeedTrades(): Promise<BlockTradeEvent[]> {
  const trades: BlockTradeEvent[] = [];
  const seen = new Set<string>();
  let continuation: string | undefined;

  while (trades.length < DERIBIT_SEED_COUNT) {
    const params = new URLSearchParams({ currency: 'any', count: String(DERIBIT_SEED_PAGE_SIZE) });
    if (continuation) params.set('continuation', continuation);

    const json = await fetchJson(
      `${DERIBIT_REST_BASE_URL}${DERIBIT_GET_BLOCK_RFQ_TRADES}?${params}`,
    );
    const parsed = DeribitBlockRfqResponseSchema.safeParse(json);
    if (!parsed.success) break;

    const page = parsed.data.result?.block_rfqs ?? [];
    if (page.length === 0) break;

    for (const item of page) {
      const tradeId = String(item.id);
      if (seen.has(tradeId)) continue;
      seen.add(tradeId);
      const trade = mapDeribitBlockTrade(item);
      if (!trade) continue;
      trades.push(trade);
      if (trades.length >= DERIBIT_SEED_COUNT) break;
    }

    continuation = parsed.data.result?.continuation ?? undefined;
    if (!continuation) break;
  }

  return trades;
}

function deribitBlockStream(): BlockVenueStream {
  let ws: WebSocket | null = null;
  let shouldReconnect = true;
  let handlers: BlockVenueStreamHandlers | null = null;
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  let keepaliveTimer: ReturnType<typeof setInterval> | null = null;
  let watchdogTimer: ReturnType<typeof setInterval> | null = null;
  let rateLimitUntil = 0;
  let shortSessionStreak = 0;
  let lastMessageAt = 0;

  function connect(attempt = 0): void {
    if (!shouldReconnect || ws != null || reconnectTimer != null) return;
    const socket = new WebSocket(DERIBIT_WS_URL);
    ws = socket;
    let didOpen = false;
    let openedAt = 0;

    const detachSocket = (): void => {
      socket.removeAllListeners();
    };

    socket.on('open', () => {
      if (ws !== socket) return;

      didOpen = true;
      openedAt = Date.now();
      lastMessageAt = Date.now();
      rateLimitUntil = 0;
      handlers?.onConnected();
      log.info({ venue: 'deribit' }, 'block trade WS connected');
      socket.send(
        JSON.stringify({
          jsonrpc: '2.0',
          id: 1,
          method: 'public/subscribe',
          params: { channels: ['block_rfq.trades.any'] },
        }),
      );
      keepaliveTimer = setInterval(() => {
        if (socket.readyState === WebSocket.OPEN) {
          socket.send(JSON.stringify({ jsonrpc: '2.0', id: 0, method: 'public/test', params: {} }));
        }
      }, DERIBIT_BLOCK_KEEPALIVE_MS);
    });

    socket.on('message', (raw: WebSocket.RawData) => {
      if (ws !== socket) return;

      lastMessageAt = Date.now();
      try {
        const msg = JSON.parse(raw.toString()) as Record<string, unknown>;
        if (msg['method'] !== 'subscription') return;
        const params = msg['params'] as Record<string, unknown> | undefined;
        const data = params?.['data'];
        if (!data || typeof data !== 'object') return;
        // Deribit pushes single block_rfq objects, not arrays
        const items = Array.isArray(data) ? data : [data];
        const trades: BlockTradeEvent[] = [];
        for (const item of items) {
          const parsed = DeribitBlockRfqSchema.safeParse(item);
          if (!parsed.success) continue;
          const trade = mapDeribitBlockTrade(parsed.data);
          if (!trade) continue;
          trades.push(trade);
        }
        if (trades.length > 0) handlers?.onTrades(trades);
      } catch (err: unknown) {
        log.debug({ err: String(err) }, 'malformed WS frame');
      }
    });

    socket.on('close', (code: number, reason: Buffer) => {
      if (ws !== socket) return;

      ws = null;
      detachSocket();
      const reasonStr = reason.length > 0 ? reason.toString() : undefined;
      const uptimeMs = openedAt > 0 ? Date.now() - openedAt : undefined;
      log.warn(
        { venue: 'deribit', closeCode: code, closeReason: reasonStr, uptimeMs },
        'block trade WS closed',
      );
      handlers?.onDisconnected();
      if (keepaliveTimer) {
        clearInterval(keepaliveTimer);
        keepaliveTimer = null;
      }
      if (shouldReconnect) {
        if (didOpen) {
          shortSessionStreak =
            uptimeMs != null && uptimeMs >= STABLE_SESSION_FLOOR_MS ? 0 : shortSessionStreak + 1;
        }
        handlers?.onReconnect();
        const delay = Math.max(
          backoffDelay(didOpen ? 0 : attempt + 1),
          flapBackoffDelay(shortSessionStreak),
          rateLimitUntil - Date.now(),
        );
        if (reconnectTimer != null) return;
        reconnectTimer = setTimeout(() => {
          reconnectTimer = null;
          connect(didOpen ? 0 : attempt + 1);
        }, delay);
      }
    });

    socket.on('error', (err) => {
      if (ws !== socket) return;
      if (isRateLimitSignal(err)) {
        rateLimitUntil = Math.max(rateLimitUntil, Date.now() + RATE_LIMIT_COOLDOWN_MS);
      }
      handlers?.onError();
      log.warn({ venue: 'deribit', err: err.message }, 'block trade WS error');
    });
  }

  async function seed(): Promise<BlockTradeEvent[]> {
    for (let attempt = 0; attempt < DERIBIT_SEED_RETRIES; attempt++) {
      try {
        return await fetchDeribitSeedTrades();
      } catch (err: unknown) {
        log.warn(
          {
            venue: 'deribit',
            err: String(err),
            attempt: attempt + 1,
            retries: DERIBIT_SEED_RETRIES,
          },
          'block trade seed failed',
        );
        if (attempt < DERIBIT_SEED_RETRIES - 1) {
          await new Promise((r) => setTimeout(r, backoffDelay(attempt)));
        }
      }
    }
    return [];
  }

  function startWatchdog(): void {
    if (watchdogTimer != null) return;
    watchdogTimer = setInterval(() => {
      const socket = ws;
      if (!shouldReconnect || socket == null || lastMessageAt === 0) return;
      const staleMs = Date.now() - lastMessageAt;
      if (staleMs < STALENESS_THRESHOLD_MS) return;
      log.error({ venue: 'deribit', staleMs }, 'block trade stream stale, forcing reconnect');
      lastMessageAt = 0; // one terminate per stall — resets when the new socket opens
      socket.terminate();
    }, WATCHDOG_INTERVAL_MS);
  }

  return {
    venue: 'deribit',
    connect(streamHandlers) {
      handlers = streamHandlers;
      connect();
      startWatchdog();
      void seed()
        .then((trades) => {
          if (trades.length > 0) {
            streamHandlers.onTrades(trades);
          }
        })
        .catch((err: unknown) => {
          log.warn({ venue: 'deribit', err: String(err) }, 'block trade seed dispatch failed');
        });
    },
    dispose() {
      shouldReconnect = false;
      if (reconnectTimer) clearTimeout(reconnectTimer);
      reconnectTimer = null;
      if (keepaliveTimer) clearInterval(keepaliveTimer);
      keepaliveTimer = null;
      if (watchdogTimer) clearInterval(watchdogTimer);
      watchdogTimer = null;
      const socket = ws;
      ws = null;
      socket?.removeAllListeners();
      socket?.close();
    },
  };
}

// Deribit combo_id encodes strategy: BTC-STRD-27MAR26-70000, BTC-CS-..., etc.
function deriveStrategy(comboId: string | null | undefined, legCount: number): string | null {
  if (!comboId) return legCount > 1 ? 'CUSTOM' : null;
  const parts = comboId.split('-');
  const code = parts[1]?.toUpperCase();
  const STRATEGY_MAP: Record<string, string> = {
    STRD: 'STRADDLE',
    STRG: 'STRANGLE',
    CS: 'CALL_SPREAD',
    PS: 'PUT_SPREAD',
    CF: 'CALL_BUTTERFLY',
    PF: 'PUT_BUTTERFLY',
    IC: 'IRON_CONDOR',
    IB: 'IRON_BUTTERFLY',
    CR: 'CALL_RATIO',
    PR: 'PUT_RATIO',
    CCS: 'CALL_CALENDAR_SPREAD',
    PCS: 'PUT_CALENDAR_SPREAD',
    CD: 'CALL_DIAGONAL',
    PD: 'PUT_DIAGONAL',
    FSR: 'FUTURE_SPREAD',
    COMBO: 'COMBO',
  };
  return STRATEGY_MAP[code ?? ''] ?? (legCount > 1 ? 'CUSTOM' : null);
}

export const BybitBlockTradeSchema = z.object({
  rfqId: z.string(),
  strategyType: z.string().optional(),
  createdAt: z.string(),
  updatedAt: z.string().optional(),
  legs: z
    .array(
      z.object({
        category: z.string().optional(),
        symbol: z.string(),
        side: z.string(),
        price: z.string(),
        qty: z.string(),
        markPrice: z.string().optional(),
      }),
    )
    .min(1),
});

function mapBybitBlockTrade(trade: z.infer<typeof BybitBlockTradeSchema>): BlockTradeEvent | null {
  const optionLegs = trade.legs.filter(
    (leg) => leg.category?.toLowerCase() === 'option' && isOptionInstrument(leg.symbol),
  );
  const firstLeg = optionLegs[0];
  if (!firstLeg || optionLegs.length !== trade.legs.length) return null;

  const underlying = extractUnderlying(firstLeg.symbol);
  const totalSize = optionLegs.reduce((sum, leg) => sum + Number(leg.qty), 0);

  const direction = firstLeg.side.toLowerCase();
  if (direction !== 'buy' && direction !== 'sell') return null;

  return {
    venue: 'bybit',
    tradeId: trade.rfqId,
    timestamp: Number(trade.updatedAt ?? trade.createdAt),
    underlying,
    direction,
    strategy: trade.strategyType?.trim()
      ? trade.strategyType.toUpperCase()
      : optionLegs.length > 1
        ? 'CUSTOM'
        : null,
    legs: optionLegs.flatMap((leg) => {
      const legDirection = leg.side.toLowerCase();
      if (legDirection !== 'buy' && legDirection !== 'sell') return [];
      return [
        {
          instrument: leg.symbol,
          direction: legDirection,
          price: Number(leg.price),
          priceIsUsd: true,
          size: Number(leg.qty),
          ratio: 1,
        },
      ];
    }),
    totalSize,
    // markPrice per leg is the option's mark price at execution time (USDT ≈ USD).
    // Fall back to execution price if markPrice is absent.
    rawAmountUsd: optionLegs.reduce(
      (sum, leg) => sum + Number(leg.markPrice ?? leg.price) * Number(leg.qty),
      0,
    ),
    indexPrice: null,
  };
}

function bybitBlockStream(): BlockVenueStream {
  let ws: WebSocket | null = null;
  let shouldReconnect = true;
  let handlers: BlockVenueStreamHandlers | null = null;
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  let keepaliveTimer: ReturnType<typeof setInterval> | null = null;
  let watchdogTimer: ReturnType<typeof setInterval> | null = null;
  let rateLimitUntil = 0;
  let shortSessionStreak = 0;
  let lastMessageAt = 0;

  function connect(attempt = 0): void {
    if (!shouldReconnect || ws != null || reconnectTimer != null) return;
    const socket = new WebSocket(BYBIT_RFQ_WS_URL);
    ws = socket;
    let didOpen = false;
    let openedAt = 0;

    const detachSocket = (): void => {
      socket.removeAllListeners();
    };

    socket.on('open', () => {
      if (ws !== socket) return;

      didOpen = true;
      openedAt = Date.now();
      lastMessageAt = Date.now();
      rateLimitUntil = 0;
      handlers?.onConnected();
      log.info({ venue: 'bybit' }, 'block trade WS connected');
      socket.send(JSON.stringify({ op: 'subscribe', args: ['rfq.open.public.trades'] }));
      keepaliveTimer = setInterval(() => {
        if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify({ op: 'ping' }));
      }, 20_000);
    });

    socket.on('message', (raw: WebSocket.RawData) => {
      if (ws !== socket) return;

      lastMessageAt = Date.now();
      try {
        const msg = JSON.parse(raw.toString()) as Record<string, unknown>;
        const data = msg['data'];
        if (!data || !Array.isArray(data)) return;

        const trades: BlockTradeEvent[] = [];
        for (const item of data) {
          const parsed = BybitBlockTradeSchema.safeParse(item);
          if (!parsed.success) continue;
          const trade = mapBybitBlockTrade(parsed.data);
          if (!trade) continue;
          trades.push(trade);
        }
        if (trades.length > 0) handlers?.onTrades(trades);
      } catch (err: unknown) {
        log.debug({ err: String(err) }, 'malformed WS frame');
      }
    });

    socket.on('close', (code: number, reason: Buffer) => {
      if (ws !== socket) return;

      ws = null;
      detachSocket();
      const reasonStr = reason.length > 0 ? reason.toString() : undefined;
      const uptimeMs = openedAt > 0 ? Date.now() - openedAt : undefined;
      log.warn(
        { venue: 'bybit', closeCode: code, closeReason: reasonStr, uptimeMs },
        'block trade WS closed',
      );
      handlers?.onDisconnected();
      if (keepaliveTimer) {
        clearInterval(keepaliveTimer);
        keepaliveTimer = null;
      }
      if (shouldReconnect) {
        if (didOpen) {
          shortSessionStreak =
            uptimeMs != null && uptimeMs >= STABLE_SESSION_FLOOR_MS ? 0 : shortSessionStreak + 1;
        }
        handlers?.onReconnect();
        const delay = Math.max(
          backoffDelay(didOpen ? 0 : attempt + 1),
          flapBackoffDelay(shortSessionStreak),
          rateLimitUntil - Date.now(),
        );
        if (reconnectTimer != null) return;
        reconnectTimer = setTimeout(() => {
          reconnectTimer = null;
          connect(didOpen ? 0 : attempt + 1);
        }, delay);
      }
    });

    socket.on('error', (err) => {
      if (ws !== socket) return;
      if (isRateLimitSignal(err)) {
        rateLimitUntil = Math.max(rateLimitUntil, Date.now() + RATE_LIMIT_COOLDOWN_MS);
      }
      handlers?.onError();
      log.warn({ venue: 'bybit', err: err.message }, 'block trade WS error');
    });
  }

  function startWatchdog(): void {
    if (watchdogTimer != null) return;
    watchdogTimer = setInterval(() => {
      const socket = ws;
      if (!shouldReconnect || socket == null || lastMessageAt === 0) return;
      const staleMs = Date.now() - lastMessageAt;
      if (staleMs < STALENESS_THRESHOLD_MS) return;
      log.error({ venue: 'bybit', staleMs }, 'block trade stream stale, forcing reconnect');
      lastMessageAt = 0; // one terminate per stall — resets when the new socket opens
      socket.terminate();
    }, WATCHDOG_INTERVAL_MS);
  }

  return {
    venue: 'bybit',
    connect(streamHandlers) {
      handlers = streamHandlers;
      connect();
      startWatchdog();
    },
    dispose() {
      shouldReconnect = false;
      if (reconnectTimer) clearTimeout(reconnectTimer);
      reconnectTimer = null;
      if (keepaliveTimer) clearInterval(keepaliveTimer);
      keepaliveTimer = null;
      if (watchdogTimer) clearInterval(watchdogTimer);
      watchdogTimer = null;
      const socket = ws;
      ws = null;
      socket?.removeAllListeners();
      socket?.close();
    },
  };
}

export const OkxBlockTradeSchema = z.object({
  blockTdId: z.string(),
  cTime: z.string(),
  strategy: z.string().optional(),
  legs: z.array(
    z.object({
      instId: z.string(),
      side: z.string(),
      sz: z.string(),
      px: z.string(),
    }),
  ),
});

function okxBlockPoller(): BlockVenuePoller {
  // Tracks the highest (newest) blockTdId seen so far — used as the pagination
  // stop condition. On first run it's 0n, meaning we accept all results.
  let lastMaxId = 0n;
  const PAGE_LIMIT = 100;
  // Safety cap: at 100 trades/page, 5 pages = 500 trades per poll cycle.
  // If OKX generates >500 block trades in 90s we have bigger problems.
  const MAX_PAGES = 5;

  return {
    venue: 'okx',
    // OKX rate limit: 120 req/min. At 10s we use 6 req/min (5%). Polling
    // frequently keeps batch sizes tiny — far cheaper than 90s+pagination.
    intervalMs: 10_000,
    // No limit: pagination is handled internally, so the global hit-limit
    // warning is suppressed — we log internally when we paginate instead.
    async poll() {
      const trades: BlockTradeEvent[] = [];
      let endId: string | null = null;
      let newMaxId: bigint | null = null;

      for (let page = 0; page < MAX_PAGES; page++) {
        const qs = endId ? `limit=${PAGE_LIMIT}&endId=${endId}` : `limit=${PAGE_LIMIT}`;
        // No internal catch — failures propagate to runPoller so venue health
        // reflects them rather than silently reading as "successful empty poll".
        const res = await fetch(`${OKX_REST_BASE_URL}${OKX_RFQ_PUBLIC_TRADES}?${qs}`, {
          signal: AbortSignal.timeout(10_000),
        });
        if (!res.ok) throw new Error(`OKX block trade HTTP ${res.status}`);
        const json: unknown = await res.json();
        const items =
          typeof json === 'object' && json !== null && 'data' in json
            ? (json as Record<string, unknown>)['data']
            : undefined;
        if (!Array.isArray(items) || items.length === 0) break;

        let hitPrev = false;
        let oldestIdInPage: string | null = null;

        for (const item of items) {
          const parsed = OkxBlockTradeSchema.safeParse(item);
          if (!parsed.success) continue;
          const d = parsed.data;

          // Stop if we've reached a trade we already processed last cycle.
          if (BigInt(d.blockTdId) <= lastMaxId) {
            hitPrev = true;
            break;
          }

          // Items arrive newest-first; first item on first page = newest ever seen.
          if (newMaxId === null) newMaxId = BigInt(d.blockTdId);
          // Track the oldest ID on this page for the next page cursor.
          oldestIdInPage = d.blockTdId;

          // Skip non-option trades (spot, futures, swaps)
          const hasOptionLeg = d.legs.some((l) => /-[CP]$/.test(l.instId));
          if (!hasOptionLeg) continue;

          const underlying = extractUnderlying(d.legs[0]?.instId ?? 'BTC');
          const totalSize = d.legs.reduce((sum, l) => sum + Number(l.sz), 0);
          const firstSide = d.legs[0]?.side.toLowerCase();
          const direction: 'buy' | 'sell' =
            firstSide === 'buy' || firstSide === 'sell' ? firstSide : 'buy';
          trades.push({
            venue: 'okx',
            tradeId: d.blockTdId,
            timestamp: Number(d.cTime),
            underlying,
            direction,
            strategy:
              d.strategy && d.strategy !== '' ? d.strategy : d.legs.length > 1 ? 'CUSTOM' : null,
            legs: d.legs.flatMap((l) => {
              const legDir = l.side.toLowerCase();
              if (legDir !== 'buy' && legDir !== 'sell') return [];
              return [
                {
                  instrument: l.instId,
                  direction: legDir,
                  price: Number(l.px),
                  priceIsUsd: false,
                  size: Number(l.sz),
                  ratio: 1,
                },
              ];
            }),
            totalSize,
            rawAmountUsd: 0,
            indexPrice: null,
          });
        }

        endId = oldestIdInPage;
        if (hitPrev || items.length < PAGE_LIMIT) break;
        log.info({ venue: 'okx', page: page + 1 }, 'okx block page full — paginating');
      }

      // Advance the cursor so next poll only fetches genuinely new trades.
      if (newMaxId !== null) lastMaxId = newMaxId;

      if (trades.length > 0)
        log.info({ venue: 'okx', count: trades.length }, 'polled block trades');
      return trades;
    },
  };
}

export const BinanceBlockTradeSchema = z.object({
  id: z.number(),
  symbol: z.string(),
  price: z.string(),
  qty: z.string(),
  side: z.number(),
  time: z.number(),
});

function binanceBlockPoller(): BlockVenuePoller {
  // Binance options API (eapi.binance.com) is geo-blocked from the US. After
  // the first confirmed geo-block we skip further network calls and return []
  // so runPoller stops logging errors every 120s for a known limitation.
  let geoBlocked = false;
  let lastMaxId = 0n;

  return {
    venue: 'binance',
    intervalMs: 30_000,
    limit: 500,
    async poll() {
      if (geoBlocked) return [];

      const res = await fetch(`${BINANCE_REST_BASE_URL}${BINANCE_BLOCK_TRADES}?limit=500`, {
        signal: AbortSignal.timeout(10_000),
      });
      const body = (await res.json()) as unknown;
      if (!Array.isArray(body)) {
        const msg = (body as { msg?: string } | null)?.msg ?? '';
        // 403/451 or a "restricted location" message → permanent geo-block.
        if (res.status === 403 || res.status === 451 || /restricted.location/i.test(msg)) {
          geoBlocked = true;
          log.warn(
            { venue: 'binance' },
            'Binance options API geo-restricted (US/restricted region) — disabling Binance block trades',
          );
          return [];
        }
        throw new Error(msg || `unexpected response shape (HTTP ${res.status})`);
      }

      const trades: BlockTradeEvent[] = [];
      let newMaxId: bigint | null = null;

      for (const item of body) {
        const parsed = BinanceBlockTradeSchema.safeParse(item);
        if (!parsed.success) continue;
        const d = parsed.data;
        const idBig = BigInt(d.id);
        if (idBig <= lastMaxId) break;

        if (newMaxId === null) newMaxId = idBig;

        const underlying = extractUnderlying(d.symbol);
        const price = Number(d.price);
        const size = Math.abs(Number(d.qty));

        trades.push({
          venue: 'binance',
          tradeId: String(d.id),
          timestamp: d.time,
          underlying,
          direction: d.side === 1 ? 'buy' : 'sell',
          strategy: null,
          legs: [
            {
              instrument: d.symbol,
              direction: d.side === 1 ? 'buy' : 'sell',
              price,
              priceIsUsd: true,
              size,
              ratio: 1,
            },
          ],
          totalSize: size,
          rawAmountUsd: price * size,
          indexPrice: null,
        });
      }

      if (newMaxId !== null) lastMaxId = newMaxId;

      if (trades.length > 0)
        log.info({ venue: 'binance', count: trades.length }, 'polled block trades');
      return trades;
    },
  };
}

const DeriveTradeSchema = z.object({
  trade_id: z.string(),
  instrument_name: z.string(),
  direction: z.enum(['buy', 'sell']),
  trade_price: z.string(),
  trade_amount: z.string(),
  index_price: z.string().optional(),
  rfq_id: z.string().nullable().optional(),
  timestamp: z.number(),
});

const DeriveTradeHistoryResponseSchema = z.object({
  result: z
    .object({
      trades: z.array(DeriveTradeSchema),
      pagination: z
        .object({
          count: z.number(),
          num_pages: z.number(),
        })
        .optional(),
    })
    .optional(),
});

function mapDeriveBlockTrade(trade: z.infer<typeof DeriveTradeSchema>): BlockTradeEvent {
  const price = Number(trade.trade_price);
  const size = Number(trade.trade_amount);
  const indexPrice = trade.index_price ? Number(trade.index_price) : null;

  return {
    venue: 'derive',
    tradeId: trade.trade_id,
    timestamp: trade.timestamp,
    underlying: extractUnderlying(trade.instrument_name),
    direction: trade.direction,
    strategy: null,
    legs: [
      {
        instrument: trade.instrument_name,
        direction: trade.direction,
        price,
        priceIsUsd: true,
        size,
        ratio: 1,
      },
    ],
    totalSize: size,
    rawAmountUsd: price * size,
    indexPrice,
  };
}

async function fetchDeriveTradeHistory(
  fromTimestamp: number,
  toTimestamp: number,
): Promise<BlockTradeEvent[]> {
  const trades: BlockTradeEvent[] = [];
  const seen = new Set<string>();
  let page = 1;
  let totalPages = 1;

  while (page <= totalPages && page <= MAX_DERIVE_PAGES_PER_POLL) {
    const params = new URLSearchParams({
      instrument_type: 'option',
      page: String(page),
      page_size: String(DERIVE_PAGE_SIZE),
      from_timestamp: String(fromTimestamp),
      to_timestamp: String(toTimestamp),
    });

    const json = await fetchJson(`${DERIVE_REST_BASE_URL}${DERIVE_GET_TRADE_HISTORY}?${params}`);
    const parsed = DeriveTradeHistoryResponseSchema.safeParse(json);
    if (!parsed.success) break;

    const result = parsed.data.result;
    const pageTrades = result?.trades ?? [];
    totalPages = result?.pagination?.num_pages ?? page;
    if (pageTrades.length === 0) break;

    for (const item of pageTrades) {
      if (!item.rfq_id) continue;
      if (seen.has(item.trade_id)) continue;
      seen.add(item.trade_id);
      trades.push(mapDeriveBlockTrade(item));
    }

    page += 1;
  }

  return trades;
}

function deriveBlockPoller(): BlockVenuePoller {
  let nextFromTimestamp: number | null = null;

  return {
    venue: 'derive',
    // Derive has no rate-limit docs published; 15s is conservative but still
    // means a 15s batch is tiny vs the old 90s window.
    intervalMs: 15_000,
    async poll() {
      // No internal catch — failures must propagate to runPoller so venue
      // health reflects them instead of reading as "successful empty poll".
      const now = Date.now();
      const fromTimestamp = nextFromTimestamp ?? now - DERIVE_INITIAL_LOOKBACK_MS;
      const trades = await fetchDeriveTradeHistory(fromTimestamp, now);
      const newestTimestamp = trades.reduce<number | null>((latest, trade) => {
        if (latest == null || trade.timestamp > latest) return trade.timestamp;
        return latest;
      }, null);

      if (newestTimestamp != null) {
        nextFromTimestamp = Math.max(newestTimestamp - DERIVE_POLL_OVERLAP_MS, 0);
      } else {
        nextFromTimestamp = Math.max(now - DERIVE_POLL_OVERLAP_MS, fromTimestamp);
      }

      if (trades.length > 0) {
        log.info(
          { venue: 'derive', count: trades.length, fromTimestamp, toTimestamp: now },
          'polled block trades',
        );
      }
      return trades;
    },
  };
}
