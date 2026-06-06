import WebSocket from 'ws';
import { z } from 'zod';

import { startEventLoopLagMonitor } from '../../utils/event-loop-lag.js';
import { feedLogger } from '../../utils/logger.js';
import { backoffDelay, flapBackoffDelay } from '../../utils/reconnect.js';
import {
  BINANCE_OPTIONS_WS_URL,
  BYBIT_RECENT_TRADE,
  BYBIT_REST_BASE_URL,
  BYBIT_WS_URL,
  DERIBIT_WS_URL,
  DERIVE_WS_URL,
  OKX_INSTRUMENT_FAMILY_TRADES,
  OKX_REST_BASE_URL,
  OKX_WS_URL,
} from '../shared/endpoints.js';
import type { VenueId } from '../types.js';
import { createTradeStreamState, mergeTradeStreamState } from './health.js';
import { filterTradesByMinNotional, pushTradeEvents } from './retention.js';
import type { TradeEvent, TradeRuntimeHealth, TradeStreamState, VenueStream } from './types.js';

const log = feedLogger('trade-runtime');

// Watchdog fires level:50 and force-closes a connection whose lastMessageAt
// hasn't advanced within this window. With keepalives sending inbound pongs
// every 20-180s per venue, a 5-minute gap is unambiguous zombie state.
const STALENESS_THRESHOLD_MS = 5 * 60 * 1000;
const WATCHDOG_INTERVAL_MS = 30 * 1000;
const WATCHDOG_TERMINATE_JITTER_MS = 5_000;
const RATE_LIMIT_COOLDOWN_MS = 90 * 1000;
const IDLE_UNDERLYING_TTL_MS = 30 * 1000;
const SEED_PUSH_BATCH_SIZE = 250;
// Sessions that die younger than this never reset the flap streak — a venue
// that accepts the socket then drops it seconds later would otherwise reset
// `attempt` to 0 on every cycle and reconnect in a tight ~500ms loop forever.
const STABLE_SESSION_FLOOR_MS = 30 * 1000;

function yieldToEventLoop(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

function isRateLimitSignal(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return /(^|\D)429(\D|$)|over_limit|rate\s*limit|too many requests/i.test(message);
}

function sharedConnectionKey(venue: VenueId): string {
  return `${venue}:shared`;
}

/**
 * Subscribes to bulk option trade streams across all five venues.
 * Maintains a ring buffer of the last N trades per underlying.
 */
export class TradeRuntime {
  private buffers = new Map<string, TradeEvent[]>();
  private connections = new Map<string, WebSocket>();
  private keepaliveTimers = new Map<string, ReturnType<typeof setInterval>>();
  private reconnectTimers = new Map<string, ReturnType<typeof setTimeout>>();
  private watchdogTerminateTimers = new Map<string, ReturnType<typeof setTimeout>>();
  private rateLimitCooldownUntil = new Map<string, number>();
  private shortSessionStreaks = new Map<string, number>();
  private reseedTimers = new Map<string, ReturnType<typeof setInterval>>();
  private subscribedUnderlyingsByConnection = new Map<string, Set<string>>();
  private activeUnderlyings = new Set<string>();
  private alwaysOnUnderlyings = new Set<string>();
  private underlyingLeaseCounts = new Map<string, number>();
  private idleUnderlyingTimers = new Map<string, ReturnType<typeof setTimeout>>();
  private ensureUnderlyingPromises = new Map<string, Promise<void>>();
  private seedPromises = new Map<string, Promise<void>>();
  private listeners = new Set<(trade: TradeEvent) => void>();
  private streamState = new Map<string, TradeStreamState>();
  private shouldReconnect = true;
  private watchdogTimer: ReturnType<typeof setInterval> | null = null;
  private stopEventLoopMonitor: (() => void) | null = null;
  private started = false;

  async start(underlyings: string[] = ['BTC', 'ETH']): Promise<void> {
    this.ensureRuntimeStarted();

    const normalizedUnderlyings = [
      ...new Set(underlyings.map((underlying) => normalizeTradeUnderlying(underlying))),
    ];
    for (const underlying of normalizedUnderlyings) this.alwaysOnUnderlyings.add(underlying);
    await Promise.all(normalizedUnderlyings.map((underlying) => this.ensureUnderlying(underlying)));
  }

  async acquire(underlying: string): Promise<() => void> {
    const normalizedUnderlying = normalizeTradeUnderlying(underlying);
    this.ensureRuntimeStarted();
    this.clearIdleUnderlyingTimer(normalizedUnderlying);
    this.underlyingLeaseCounts.set(
      normalizedUnderlying,
      (this.underlyingLeaseCounts.get(normalizedUnderlying) ?? 0) + 1,
    );
    await this.ensureUnderlying(normalizedUnderlying);

    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.releaseUnderlying(normalizedUnderlying);
    };
  }

  private ensureRuntimeStarted(): void {
    if (this.started) return;
    this.started = true;
    this.watchdogTimer = setInterval(() => this.checkStreamLiveness(), WATCHDOG_INTERVAL_MS);
    this.stopEventLoopMonitor = startEventLoopLagMonitor();
  }

  private async ensureUnderlying(underlying: string): Promise<void> {
    const normalizedUnderlying = normalizeTradeUnderlying(underlying);
    const inFlight = this.ensureUnderlyingPromises.get(normalizedUnderlying);
    if (inFlight != null) {
      await inFlight;
      return;
    }

    const ensurePromise = (async () => {
      if (this.activeUnderlyings.has(normalizedUnderlying)) return;

      this.activeUnderlyings.add(normalizedUnderlying);
      this.buffers.set(normalizedUnderlying, this.buffers.get(normalizedUnderlying) ?? []);

      for (const stream of VENUE_STREAMS) {
        if (!this.supportsUnderlying(stream, normalizedUnderlying)) continue;
        this.streamState.set(
          this.streamKey(stream.venue, normalizedUnderlying),
          this.streamState.get(this.streamKey(stream.venue, normalizedUnderlying)) ??
            createTradeStreamState(),
        );

        const connectionKey = this.connectionKey(stream, normalizedUnderlying);
        const alreadyRegistered =
          this.subscribedUnderlyingsByConnection.get(connectionKey)?.has(normalizedUnderlying) ??
          false;

        this.registerConnectionUnderlying(stream, normalizedUnderlying);

        const ws = this.connections.get(connectionKey);
        if (ws != null && ws.readyState === WebSocket.OPEN && alreadyRegistered) continue;

        if (ws != null && ws.readyState === WebSocket.CONNECTING) continue;

        if (ws != null && ws.readyState === WebSocket.OPEN && stream.subscribe != null) {
          stream.subscribe(ws, [normalizedUnderlying]);
          continue;
        }

        if (ws != null) {
          this.restartConnection(stream, normalizedUnderlying);
          continue;
        }

        this.connectStream(stream, normalizedUnderlying);
      }

      this.startReseedTimersForUnderlying(normalizedUnderlying);
      this.seedUnderlying(normalizedUnderlying);
    })();

    this.ensureUnderlyingPromises.set(normalizedUnderlying, ensurePromise);
    try {
      await ensurePromise;
    } finally {
      if (this.ensureUnderlyingPromises.get(normalizedUnderlying) === ensurePromise) {
        this.ensureUnderlyingPromises.delete(normalizedUnderlying);
      }
    }
  }

  private seedUnderlying(underlying: string): void {
    if (this.seedPromises.has(underlying)) return;
    const seedPromise = this.seedFromRest(underlying).finally(() => {
      if (this.seedPromises.get(underlying) === seedPromise) {
        this.seedPromises.delete(underlying);
      }
    });
    this.seedPromises.set(underlying, seedPromise);
    void seedPromise;
  }

  private startReseedTimersForUnderlying(underlying: string): void {
    for (const stream of VENUE_STREAMS) {
      if (stream.seed == null || stream.reseedIntervalMs == null) continue;
      if (!this.supportsUnderlying(stream, underlying)) continue;

      const key = this.streamKey(stream.venue, underlying);
      if (this.reseedTimers.has(key)) continue;

      const timer = setInterval(() => {
        void this.runVenueReseed(stream, underlying);
      }, stream.reseedIntervalMs);
      this.reseedTimers.set(key, timer);
    }
  }

  private stopReseedTimersForUnderlying(underlying: string): void {
    for (const stream of VENUE_STREAMS) {
      const key = this.streamKey(stream.venue, underlying);
      const timer = this.reseedTimers.get(key);
      if (timer == null) continue;
      clearInterval(timer);
      this.reseedTimers.delete(key);
    }
  }

  private releaseUnderlying(underlying: string): void {
    const current = this.underlyingLeaseCounts.get(underlying) ?? 0;
    if (current <= 1) this.underlyingLeaseCounts.delete(underlying);
    else this.underlyingLeaseCounts.set(underlying, current - 1);

    if (this.alwaysOnUnderlyings.has(underlying)) return;
    if ((this.underlyingLeaseCounts.get(underlying) ?? 0) > 0) return;
    this.scheduleIdleUnderlyingRelease(underlying);
  }

  private scheduleIdleUnderlyingRelease(underlying: string): void {
    this.clearIdleUnderlyingTimer(underlying);
    const timer = setTimeout(() => {
      this.idleUnderlyingTimers.delete(underlying);
      void this.deactivateUnderlying(underlying);
    }, IDLE_UNDERLYING_TTL_MS);
    this.idleUnderlyingTimers.set(underlying, timer);
  }

  private clearIdleUnderlyingTimer(underlying: string): void {
    const timer = this.idleUnderlyingTimers.get(underlying);
    if (timer == null) return;
    clearTimeout(timer);
    this.idleUnderlyingTimers.delete(underlying);
  }

  private async deactivateUnderlying(underlying: string): Promise<void> {
    if (this.alwaysOnUnderlyings.has(underlying)) return;
    if ((this.underlyingLeaseCounts.get(underlying) ?? 0) > 0) return;
    if (!this.activeUnderlyings.delete(underlying)) return;

    this.stopReseedTimersForUnderlying(underlying);
    this.buffers.delete(underlying);
    this.streamState.forEach((_, key) => {
      if (key.endsWith(`:${underlying}`)) this.streamState.delete(key);
    });

    for (const stream of VENUE_STREAMS) {
      if (!this.supportsUnderlying(stream, underlying)) continue;
      const connectionKey = this.connectionKey(stream, underlying);
      const subscribed = this.subscribedUnderlyingsByConnection.get(connectionKey);
      if (subscribed == null || !subscribed.delete(underlying)) continue;

      if (subscribed.size === 0) {
        this.subscribedUnderlyingsByConnection.delete(connectionKey);
        this.closeConnection(connectionKey);
        continue;
      }

      this.subscribedUnderlyingsByConnection.set(connectionKey, subscribed);
      const firstSubscribed = [...subscribed][0];
      if (firstSubscribed != null) this.restartConnection(stream, firstSubscribed);
    }
  }

  private async runVenueReseed(stream: VenueStream, underlying: string): Promise<void> {
    if (stream.seed == null) return;
    try {
      const trades = await stream.seed(underlying);
      if (trades.length === 0) return;
      this.updateStreamState(stream.venue, underlying, {
        seedTrades: trades.length,
        lastStatusAt: Date.now(),
      });
      await this.pushTradesBatched(underlying, trades);
      log.info({ venue: stream.venue, underlying, count: trades.length }, 'venue reseed completed');
    } catch (err: unknown) {
      log.warn({ venue: stream.venue, underlying, err: String(err) }, 'venue reseed failed');
    }
  }

  // Detects half-open TCP connections that report `connected:true` locally but
  // haven't received any inbound frames recently. Forces a terminate so the
  // existing close→reconnect path rebuilds the subscription from scratch.
  // Emits level:50 — the first error-level signal this runtime produces for
  // this failure mode, so any log aggregator will pick it up.
  private checkStreamLiveness(): void {
    if (!this.shouldReconnect) return;
    const now = Date.now();

    for (const [connectionKey, ws] of this.connections) {
      const subscribedUnderlyings = this.subscribedUnderlyingsByConnection.get(connectionKey);
      if (subscribedUnderlyings == null || subscribedUnderlyings.size === 0) continue;

      const colonIdx = connectionKey.indexOf(':');
      if (colonIdx < 0) continue;
      const venue = connectionKey.slice(0, colonIdx) as VenueId;

      let worstStaleMs = 0;
      let worstUnderlying: string | null = null;
      for (const underlying of subscribedUnderlyings) {
        const state = this.streamState.get(this.streamKey(venue, underlying));
        if (state == null || !state.connected || state.lastMessageAt == null) continue;
        const staleMs = now - state.lastMessageAt;
        if (staleMs > worstStaleMs) {
          worstStaleMs = staleMs;
          worstUnderlying = underlying;
        }
      }

      if (worstStaleMs >= STALENESS_THRESHOLD_MS && worstUnderlying != null) {
        if (this.watchdogTerminateTimers.has(connectionKey)) continue;
        const delayMs = Math.floor(Math.random() * WATCHDOG_TERMINATE_JITTER_MS);
        log.error(
          {
            venue,
            underlying: worstUnderlying,
            connectionKey,
            staleMs: worstStaleMs,
            thresholdMs: STALENESS_THRESHOLD_MS,
            delayMs,
          },
          'trade stream stale, forcing reconnect',
        );
        const timer = setTimeout(() => {
          this.watchdogTerminateTimers.delete(connectionKey);
          if (!this.shouldReconnect) return;
          if (this.connections.get(connectionKey) !== ws) return;
          ws.terminate();
        }, delayMs);
        this.watchdogTerminateTimers.set(connectionKey, timer);
        continue;
      }

      const pendingTimer = this.watchdogTerminateTimers.get(connectionKey);
      if (pendingTimer != null) {
        clearTimeout(pendingTimer);
        this.watchdogTerminateTimers.delete(connectionKey);
      }
    }
  }

  private async seedFromRest(underlying: string): Promise<void> {
    const seedStreams = VENUE_STREAMS.filter(
      (stream) => stream.seed != null && this.supportsUnderlying(stream, underlying),
    );
    const results = await Promise.allSettled(
      seedStreams.map((stream) => (stream.seed as NonNullable<typeof stream.seed>)(underlying)),
    );

    let total = 0;
    for (const [index, result] of results.entries()) {
      const stream = seedStreams[index];
      if (!stream) continue;

      if (result.status === 'rejected') {
        log.warn(
          { venue: stream.venue, underlying, err: String(result.reason) },
          'trade seed failed',
        );
        continue;
      }

      const count = result.value.length;
      this.updateStreamState(stream.venue, underlying, {
        seedTrades: count,
        lastStatusAt: Date.now(),
      });
      log.info({ venue: stream.venue, underlying, count }, 'trade seed completed');

      if (count === 0) continue;
      await this.pushTradesBatched(underlying, result.value);
      total += count;
    }

    if (total > 0) log.info({ underlying, count: total }, 'seeded trades total');
  }

  getTrades(underlying: string, minNotional = 0): TradeEvent[] {
    const buffer = this.buffers.get(normalizeTradeUnderlying(underlying)) ?? [];
    return filterTradesByMinNotional(buffer, minNotional);
  }

  subscribe(listener: (trade: TradeEvent) => void): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  getHealth(): TradeRuntimeHealth[] {
    return VENUE_STREAMS.flatMap((stream) =>
      Array.from(this.buffers.keys()).map((underlying) => {
        const currentState = this.streamState.get(this.streamKey(stream.venue, underlying));
        return {
          venue: stream.venue,
          underlying,
          connected: currentState?.connected ?? false,
          lastMessageAt: currentState?.lastMessageAt ?? null,
          lastTradeAt: currentState?.lastTradeAt ?? null,
          lastStatusAt: currentState?.lastStatusAt ?? null,
          reconnects: currentState?.reconnects ?? 0,
          errors: currentState?.errors ?? 0,
          seedTrades: currentState?.seedTrades ?? 0,
          bufferedTrades:
            this.buffers.get(underlying)?.filter((trade) => trade.venue === stream.venue).length ??
            0,
        } satisfies TradeRuntimeHealth;
      }),
    );
  }

  private connectStream(stream: VenueStream, underlying: string, attempt = 0): void {
    if (!this.shouldReconnect) return;

    const key = this.connectionKey(stream, underlying);
    if (this.connections.has(key) || this.reconnectTimers.has(key)) return;

    const url = typeof stream.url === 'function' ? stream.url() : stream.url;
    const ws = new WebSocket(url);
    let didOpen = false;
    let openedAt = 0;

    const stopKeepalive = (): void => {
      const timer = this.keepaliveTimers.get(key);
      if (timer == null) return;
      clearInterval(timer);
      this.keepaliveTimers.delete(key);
    };

    ws.on('open', () => {
      if (this.connections.get(key) !== ws) return;

      didOpen = true;
      openedAt = Date.now();
      const activeUnderlyings = this.getConnectionUnderlyings(stream, underlying);
      const watchdogTimer = this.watchdogTerminateTimers.get(key);
      if (watchdogTimer != null) {
        clearTimeout(watchdogTimer);
        this.watchdogTerminateTimers.delete(key);
      }
      this.rateLimitCooldownUntil.delete(key);
      this.updateStreamStates(stream.venue, activeUnderlyings, {
        connected: true,
        lastStatusAt: Date.now(),
      });
      log.info(
        { venue: stream.venue, underlying: activeUnderlyings.join(',') },
        'trade stream connected',
      );
      stream.connect(ws, activeUnderlyings);

      if (stream.startKeepalive) {
        const timer = stream.startKeepalive(ws);
        this.keepaliveTimers.set(key, timer);
      }
    });

    ws.on('pong', () => {
      if (this.connections.get(key) !== ws) return;
      const activeUnderlyings = this.getConnectionUnderlyings(stream, underlying);
      this.updateStreamStates(stream.venue, activeUnderlyings, { lastMessageAt: Date.now() });
    });

    ws.on('message', (raw: WebSocket.RawData) => {
      if (this.connections.get(key) !== ws) return;

      try {
        const msg: unknown = JSON.parse(raw.toString());
        const now = Date.now();
        const activeUnderlyings = this.getConnectionUnderlyings(stream, underlying);
        this.updateStreamStates(stream.venue, activeUnderlyings, { lastMessageAt: now });

        const trades = stream.parse(msg, activeUnderlyings);
        if (trades.length === 0) return;

        const tradesByUnderlying = new Map<string, TradeEvent[]>();
        for (const trade of trades) {
          const tradeUnderlying = normalizeTradeUnderlying(trade.underlying);
          const bucket = tradesByUnderlying.get(tradeUnderlying);
          if (bucket) bucket.push({ ...trade, underlying: tradeUnderlying });
          else tradesByUnderlying.set(tradeUnderlying, [{ ...trade, underlying: tradeUnderlying }]);
        }

        for (const [tradeUnderlying, underlyingTrades] of tradesByUnderlying) {
          this.updateStreamState(stream.venue, tradeUnderlying, {
            lastTradeAt: Math.max(...underlyingTrades.map((trade) => trade.timestamp)),
          });
          this.pushTrades(tradeUnderlying, underlyingTrades);
        }
      } catch {
        // Ignore malformed upstream frames.
      }
    });

    ws.on('close', (code: number, reason: Buffer) => {
      if (this.connections.get(key) !== ws) return;

      const watchdogTimer = this.watchdogTerminateTimers.get(key);
      if (watchdogTimer != null) {
        clearTimeout(watchdogTimer);
        this.watchdogTerminateTimers.delete(key);
      }
      const reasonStr = reason.length > 0 ? reason.toString() : undefined;
      const uptimeMs = openedAt > 0 ? Date.now() - openedAt : undefined;
      const activeUnderlyings = this.getConnectionUnderlyings(stream, underlying);
      log.warn(
        {
          venue: stream.venue,
          underlying: activeUnderlyings.join(','),
          closeCode: code,
          closeReason: reasonStr,
          uptimeMs,
        },
        'trade stream closed',
      );
      this.connections.delete(key);
      this.updateStreamStates(stream.venue, activeUnderlyings, {
        connected: false,
        lastStatusAt: Date.now(),
      });
      stopKeepalive();
      ws.removeAllListeners();

      if (this.shouldReconnect) {
        const nextAttempt = didOpen ? 0 : attempt + 1;
        if (didOpen) {
          const wasStable = uptimeMs != null && uptimeMs >= STABLE_SESSION_FLOOR_MS;
          const streak = wasStable ? 0 : (this.shortSessionStreaks.get(key) ?? 0) + 1;
          this.shortSessionStreaks.set(key, streak);
        }
        for (const subscribedUnderlying of activeUnderlyings) {
          this.updateStreamState(stream.venue, subscribedUnderlying, {
            reconnects:
              (this.streamState.get(this.streamKey(stream.venue, subscribedUnderlying))
                ?.reconnects ?? 0) + 1,
          });
        }
        const flapStreak = this.shortSessionStreaks.get(key) ?? 0;
        const flapDelay = flapBackoffDelay(flapStreak);
        if (flapDelay > 0) {
          log.warn(
            { venue: stream.venue, connectionKey: key, flapStreak, flapDelay },
            'trade stream flapping, widening reconnect gap',
          );
        }
        const delay = Math.max(
          backoffDelay(nextAttempt),
          flapDelay,
          this.remainingRateLimitCooldownMs(key),
        );
        if (this.reconnectTimers.has(key)) return;
        const timer = setTimeout(() => {
          this.reconnectTimers.delete(key);
          this.connectStream(stream, activeUnderlyings[0] ?? underlying, nextAttempt);
        }, delay);
        this.reconnectTimers.set(key, timer);
      }
    });

    ws.on('error', (err) => {
      if (this.connections.get(key) !== ws) return;

      this.noteRateLimit(key, err);
      const activeUnderlyings = this.getConnectionUnderlyings(stream, underlying);

      for (const subscribedUnderlying of activeUnderlyings) {
        this.updateStreamState(stream.venue, subscribedUnderlying, {
          errors:
            (this.streamState.get(this.streamKey(stream.venue, subscribedUnderlying))?.errors ??
              0) + 1,
          lastStatusAt: Date.now(),
        });
      }
      log.warn(
        { venue: stream.venue, underlying: activeUnderlyings.join(','), err: err.message },
        'trade stream error',
      );
    });

    this.connections.set(key, ws);
  }

  private streamKey(venue: VenueId, underlying: string): string {
    return `${venue}:${underlying}`;
  }

  private remainingRateLimitCooldownMs(key: string): number {
    return Math.max(0, (this.rateLimitCooldownUntil.get(key) ?? 0) - Date.now());
  }

  private noteRateLimit(key: string, error: unknown): void {
    if (!isRateLimitSignal(error)) return;

    const until = Date.now() + RATE_LIMIT_COOLDOWN_MS;
    const currentUntil = this.rateLimitCooldownUntil.get(key) ?? 0;
    if (until > currentUntil) {
      this.rateLimitCooldownUntil.set(key, until);
    }
    log.warn(
      { connectionKey: key, retryAt: until },
      'trade stream rate limited, delaying reconnect',
    );
  }

  private connectionKey(stream: VenueStream, underlying: string): string {
    return stream.connectionKey?.([underlying]) ?? this.streamKey(stream.venue, underlying);
  }

  private supportsUnderlying(stream: VenueStream, underlying: string): boolean {
    if (stream.venue === 'deribit' && getDeribitTradeCurrency(underlying) == null) {
      return false;
    }
    return true;
  }

  private registerConnectionUnderlying(stream: VenueStream, underlying: string): void {
    const key = this.connectionKey(stream, underlying);
    const subscribedUnderlyings =
      this.subscribedUnderlyingsByConnection.get(key) ?? new Set<string>();
    subscribedUnderlyings.add(underlying);
    this.subscribedUnderlyingsByConnection.set(key, subscribedUnderlyings);
  }

  private getConnectionUnderlyings(stream: VenueStream, underlying: string): string[] {
    const key = this.connectionKey(stream, underlying);
    const subscribedUnderlyings = this.subscribedUnderlyingsByConnection.get(key);
    return subscribedUnderlyings ? [...subscribedUnderlyings] : [underlying];
  }

  private restartConnection(stream: VenueStream, underlying: string): void {
    const key = this.connectionKey(stream, underlying);
    this.closeConnection(key);
    this.connectStream(stream, underlying);
  }

  private closeConnection(key: string): void {
    const reconnectTimer = this.reconnectTimers.get(key);
    if (reconnectTimer != null) {
      clearTimeout(reconnectTimer);
      this.reconnectTimers.delete(key);
    }

    const watchdogTimer = this.watchdogTerminateTimers.get(key);
    if (watchdogTimer != null) {
      clearTimeout(watchdogTimer);
      this.watchdogTerminateTimers.delete(key);
    }

    const keepaliveTimer = this.keepaliveTimers.get(key);
    if (keepaliveTimer != null) {
      clearInterval(keepaliveTimer);
      this.keepaliveTimers.delete(key);
    }

    this.shortSessionStreaks.delete(key);

    const ws = this.connections.get(key);
    if (ws == null) return;
    this.connections.delete(key);
    ws.removeAllListeners();
    ws.close();
  }

  private updateStreamState(
    venue: VenueId,
    underlying: string,
    patch: Partial<TradeStreamState>,
  ): void {
    const key = this.streamKey(venue, underlying);
    const current = this.streamState.get(key);
    if (!current) return;
    this.streamState.set(key, mergeTradeStreamState(current, patch));
  }

  private updateStreamStates(
    venue: VenueId,
    underlyings: string[],
    patch: Partial<TradeStreamState>,
  ): void {
    for (const underlying of underlyings) {
      this.updateStreamState(venue, underlying, patch);
    }
  }

  private pushTrades(underlying: string, trades: TradeEvent[]): void {
    const buffer = this.buffers.get(underlying);
    if (!buffer) return;
    pushTradeEvents(buffer, trades);
    for (const trade of trades) {
      for (const listener of this.listeners) {
        try {
          listener(trade);
        } catch (error: unknown) {
          log.warn({ err: String(error), venue: trade.venue, underlying }, 'trade listener failed');
        }
      }
    }
  }

  private async pushTradesBatched(underlying: string, trades: TradeEvent[]): Promise<void> {
    for (let offset = 0; offset < trades.length; offset += SEED_PUSH_BATCH_SIZE) {
      this.pushTrades(underlying, trades.slice(offset, offset + SEED_PUSH_BATCH_SIZE));
      if (offset + SEED_PUSH_BATCH_SIZE < trades.length) {
        await yieldToEventLoop();
      }
    }
  }

  dispose(): void {
    this.shouldReconnect = false;
    if (this.watchdogTimer != null) {
      clearInterval(this.watchdogTimer);
      this.watchdogTimer = null;
    }
    if (this.stopEventLoopMonitor != null) {
      this.stopEventLoopMonitor();
      this.stopEventLoopMonitor = null;
    }
    for (const timer of this.reconnectTimers.values()) clearTimeout(timer);
    this.reconnectTimers.clear();
    for (const timer of this.watchdogTerminateTimers.values()) clearTimeout(timer);
    this.watchdogTerminateTimers.clear();
    for (const timer of this.keepaliveTimers.values()) clearInterval(timer);
    this.keepaliveTimers.clear();
    for (const timer of this.reseedTimers.values()) clearInterval(timer);
    this.reseedTimers.clear();
    for (const timer of this.idleUnderlyingTimers.values()) clearTimeout(timer);
    this.idleUnderlyingTimers.clear();
    for (const ws of this.connections.values()) {
      ws.removeAllListeners();
      ws.close();
    }
    this.connections.clear();
    this.shortSessionStreaks.clear();
    this.subscribedUnderlyingsByConnection.clear();
    this.activeUnderlyings.clear();
    this.alwaysOnUnderlyings.clear();
    this.underlyingLeaseCounts.clear();
    this.ensureUnderlyingPromises.clear();
    this.seedPromises.clear();
    clearTradeCaches();
  }
}

const numStr = z.union([z.string(), z.number()]).transform(Number).refine(Number.isFinite);
const optNum = z
  .union([z.string(), z.number(), z.null()])
  .optional()
  .transform((v) => {
    if (v == null) return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  });
const sideStr = z
  .string()
  .transform((s) => s.toLowerCase())
  .pipe(z.enum(['buy', 'sell']));

const DeribitTradeSchema = z.object({
  instrument_name: z.string(),
  direction: z.enum(['buy', 'sell']),
  price: z.number(),
  amount: z.number(),
  iv: z.number().optional(),
  mark_price: z.number().optional(),
  index_price: z.number().optional(),
  block_trade_id: z.string().optional(),
  trade_id: z
    .union([z.string(), z.number()])
    .optional()
    .transform((value) => (value != null ? String(value) : null)),
  trade_seq: z
    .union([z.string(), z.number()])
    .optional()
    .transform((value) => (value != null ? String(value) : null)),
  timestamp: z.number(),
});

const OkxTradeSchema = z.object({
  instId: z.string(),
  side: sideStr,
  px: numStr,
  sz: numStr,
  fillVol: numStr.optional(),
  tradeId: z
    .union([z.string(), z.number()])
    .optional()
    .transform((value) => (value != null ? String(value) : null)),
  ts: numStr,
});

const BybitTradeSchema = z.object({
  s: z.string(),
  S: sideStr,
  p: numStr,
  v: numStr,
  iv: numStr.optional(),
  mP: optNum,
  iP: optNum,
  i: z.string().optional(),
  BT: z.boolean().optional(),
  T: z.number(),
});

const BinanceTradeSchema = z.object({
  e: z.literal('trade'),
  s: z.string(),
  S: sideStr,
  p: numStr,
  q: numStr,
  t: z
    .union([z.string(), z.number()])
    .optional()
    .transform((value) => (value != null ? String(value) : null)),
  X: z.string().optional(),
  T: z.number(),
});

const DeriveTradeSchema = z.object({
  instrument_name: z.string(),
  direction: sideStr,
  trade_id: z
    .union([z.string(), z.number()])
    .optional()
    .transform((value) => (value != null ? String(value) : null)),
  trade_price: numStr,
  trade_amount: numStr,
  mark_price: optNum,
  index_price: optNum,
  rfq_id: z.string().nullable().optional(),
  timestamp: z.number(),
});

// OKX REST trade-history response format — differs from the WS stream schema.
const OkxRestTradeSchema = z.object({
  instId: z.string(),
  side: sideStr,
  px: numStr,
  sz: numStr,
  tradeId: z
    .union([z.string(), z.number()])
    .optional()
    .transform((value) => (value != null ? String(value) : null)),
  ts: numStr,
});

// Bybit REST recent-trade response format — differs from the WS stream schema.
const BybitRestTradeSchema = z.object({
  symbol: z.string(),
  side: sideStr,
  execId: z.string().optional(),
  price: numStr,
  size: numStr,
  iv: numStr.optional(),
  mP: optNum,
  iP: optNum,
  isBlockTrade: z.boolean().optional(),
  time: numStr,
});

const DERIBIT_INVERSE_OPTION_CURRENCIES = new Set(['BTC', 'ETH']);
const DERIBIT_USDC_OPTION_BASES = new Set(['AVAX', 'SOL', 'TRX', 'XRP']);

export function clearTradeCaches(): void {
  deribitSeedCache.clear();
}

export function normalizeTradeUnderlying(underlying: string): string {
  return underlying.toUpperCase().split('_')[0] ?? underlying.toUpperCase();
}

export function getDeribitTradeCurrency(underlying: string): string | null {
  const normalizedUnderlying = normalizeTradeUnderlying(underlying);
  if (DERIBIT_INVERSE_OPTION_CURRENCIES.has(normalizedUnderlying)) return normalizedUnderlying;
  if (DERIBIT_USDC_OPTION_BASES.has(normalizedUnderlying)) return 'USDC';
  return null;
}

export function getDeribitUnderlyingFromInstrument(instrument: string): string | null {
  const instrumentFamily = instrument.split('-')[0];
  if (!instrumentFamily) return null;
  return normalizeTradeUnderlying(instrumentFamily);
}

function isDeribitTradeForUnderlying(instrument: string, underlying: string): boolean {
  return getDeribitUnderlyingFromInstrument(instrument) === normalizeTradeUnderlying(underlying);
}

function deribitTradeToEvent(
  raw: z.infer<typeof DeribitTradeSchema>,
  underlying: string,
): TradeEvent {
  return {
    venue: 'deribit',
    tradeId: raw.trade_id ?? raw.trade_seq,
    instrument: raw.instrument_name,
    underlying,
    side: raw.direction,
    price: raw.price,
    size: raw.amount,
    // Deribit sends IV as percentage (49.80 = 49.80%)
    iv: raw.iv != null ? raw.iv / 100 : null,
    markPrice: raw.mark_price ?? null,
    indexPrice: raw.index_price ?? null,
    isBlock: raw.block_trade_id != null,
    timestamp: raw.timestamp,
  };
}

const deribitSeedCache = new Map<string, Promise<TradeEvent[]>>();

function fetchDeribitTradesByCurrency(currency: string): Promise<TradeEvent[]> {
  const existing = deribitSeedCache.get(currency);
  if (existing) return existing;

  const promise = deribitRpcSeed(currency);
  deribitSeedCache.set(currency, promise);
  // The cache exists to coalesce concurrent seeds (BTC + ETH share USDC) —
  // don't let it memoize a timeout/empty result for the process lifetime.
  promise.then(
    (trades) => {
      if (trades.length === 0) deribitSeedCache.delete(currency);
    },
    () => deribitSeedCache.delete(currency),
  );
  return promise;
}

function deribitRpcSeed(currency: string): Promise<TradeEvent[]> {
  return new Promise<TradeEvent[]>((resolve) => {
    const ws = new WebSocket(DERIBIT_WS_URL);
    const timeout = setTimeout(() => {
      ws.removeAllListeners();
      ws.close();
      resolve([]);
    }, 10_000);

    const finish = (trades: TradeEvent[]): void => {
      clearTimeout(timeout);
      ws.removeAllListeners();
      ws.close();
      resolve(trades);
    };

    ws.on('open', () => {
      ws.send(
        JSON.stringify({
          jsonrpc: '2.0',
          id: 1,
          method: 'public/get_last_trades_by_currency',
          params: { currency, kind: 'option', count: 50 },
        }),
      );
    });

    ws.on('message', (raw: WebSocket.RawData) => {
      let msg: Record<string, unknown>;
      try {
        msg = JSON.parse(raw.toString()) as Record<string, unknown>;
      } catch {
        return; // ignore malformed frames — a throw here would crash the process
      }
      if (msg['id'] !== 1) return;

      const trades = (msg['result'] as Record<string, unknown> | undefined)?.['trades'];
      if (!Array.isArray(trades)) {
        finish([]);
        return;
      }

      finish(
        trades.flatMap((t) => {
          const p = DeribitTradeSchema.safeParse(t);
          if (!p.success) return [];
          return [
            deribitTradeToEvent(
              p.data,
              getDeribitUnderlyingFromInstrument(p.data.instrument_name) ?? currency,
            ),
          ];
        }),
      );
    });

    ws.on('error', () => finish([]));
  });
}

let bybitSeedGeoBlocked = false;

export const VENUE_STREAMS: VenueStream[] = [
  {
    venue: 'deribit',
    url: DERIBIT_WS_URL,
    connectionKey(underlyings) {
      const underlying = underlyings[0] ?? 'BTC';
      const tradeCurrency = getDeribitTradeCurrency(underlying);
      return `deribit:${tradeCurrency ?? normalizeTradeUnderlying(underlying)}`;
    },
    // Deribit closes idle sockets silently when the app-level heartbeat isn't
    // configured on this connection. `public/test` is a free no-op RPC; we
    // send it every 25s so the server keeps pushing trades and, critically,
    // we see an inbound response that refreshes `lastMessageAt` for the watchdog.
    startKeepalive(ws) {
      return setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ jsonrpc: '2.0', id: 0, method: 'public/test', params: {} }));
        }
      }, 25_000);
    },
    connect(ws, underlyings) {
      const tradeCurrency = getDeribitTradeCurrency(underlyings[0] ?? '');
      if (!tradeCurrency) return;

      ws.send(
        JSON.stringify({
          jsonrpc: '2.0',
          id: 1,
          method: 'public/subscribe',
          params: { channels: [`trades.option.${tradeCurrency}.100ms`] },
        }),
      );
    },
    subscribe() {},
    parse(msg, underlyings) {
      const m = msg as Record<string, unknown>;
      if (m['method'] !== 'subscription') return [];
      const params = m['params'] as Record<string, unknown> | undefined;
      const data = params?.['data'];
      if (!Array.isArray(data)) return [];

      const trades: TradeEvent[] = [];
      for (const item of data) {
        const parsed = DeribitTradeSchema.safeParse(item);
        if (!parsed.success) continue;
        const matchedUnderlying = underlyings.find((underlying) =>
          isDeribitTradeForUnderlying(parsed.data.instrument_name, underlying),
        );
        if (matchedUnderlying == null) continue;
        trades.push(
          deribitTradeToEvent(
            parsed.data,
            getDeribitUnderlyingFromInstrument(parsed.data.instrument_name) ??
              normalizeTradeUnderlying(matchedUnderlying),
          ),
        );
      }
      return trades;
    },
    async seed(underlying) {
      const tradeCurrency = getDeribitTradeCurrency(underlying);
      if (!tradeCurrency) return [];

      const allTrades = await fetchDeribitTradesByCurrency(tradeCurrency);
      return allTrades.filter((t) => isDeribitTradeForUnderlying(t.instrument, underlying));
    },
  },
  {
    venue: 'okx',
    url: OKX_WS_URL,
    connectionKey() {
      return sharedConnectionKey('okx');
    },
    // OKX drops idle connections — must send "ping" text every 25s
    startKeepalive(ws) {
      return setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) ws.send('ping');
      }, 25_000);
    },
    connect(ws, underlyings) {
      ws.send(
        JSON.stringify({
          op: 'subscribe',
          args: underlyings.map((underlying) => ({
            channel: 'option-trades',
            instType: 'OPTION',
            instFamily: `${underlying}-USD`,
          })),
        }),
      );
    },
    subscribe(ws, underlyings) {
      ws.send(
        JSON.stringify({
          op: 'subscribe',
          args: underlyings.map((underlying) => ({
            channel: 'option-trades',
            instType: 'OPTION',
            instFamily: `${underlying}-USD`,
          })),
        }),
      );
    },
    parse(msg, underlyings) {
      const m = msg as Record<string, unknown>;
      if (!m['data'] || !Array.isArray(m['data'])) return [];
      const trades: TradeEvent[] = [];
      for (const item of m['data'] as unknown[]) {
        const parsed = OkxTradeSchema.safeParse(item);
        if (!parsed.success) continue;
        const tradeUnderlying = normalizeTradeUnderlying(parsed.data.instId.split('-')[0] ?? '');
        if (!underlyings.includes(tradeUnderlying)) continue;
        trades.push({
          venue: 'okx',
          tradeId: parsed.data.tradeId,
          instrument: parsed.data.instId,
          underlying: tradeUnderlying,
          side: parsed.data.side,
          price: parsed.data.px,
          size: parsed.data.sz,
          iv: parsed.data.fillVol ?? null,
          markPrice: null,
          indexPrice: null,
          isBlock: false,
          timestamp: parsed.data.ts,
        });
      }
      return trades;
    },
    async seed(underlying) {
      try {
        const res = await fetch(
          `${OKX_REST_BASE_URL}${OKX_INSTRUMENT_FAMILY_TRADES}?instFamily=${underlying}-USD`,
          { signal: AbortSignal.timeout(10_000) },
        );
        const data = (await res.json()) as Record<string, unknown>;
        const items = data['data'] as Array<Record<string, unknown>> | undefined;
        if (!items) return [];
        const trades: TradeEvent[] = [];
        for (const group of items) {
          const infos = group['tradeInfo'];
          if (!Array.isArray(infos)) continue;
          for (const raw of infos) {
            const p = OkxRestTradeSchema.safeParse(raw);
            if (!p.success) continue;
            trades.push({
              venue: 'okx',
              tradeId: p.data.tradeId,
              instrument: p.data.instId,
              underlying,
              side: p.data.side,
              price: p.data.px,
              size: p.data.sz,
              iv: null,
              markPrice: null,
              indexPrice: null,
              isBlock: false,
              timestamp: p.data.ts,
            });
          }
        }
        return trades;
      } catch {
        return [];
      }
    },
  },
  {
    venue: 'bybit',
    url: BYBIT_WS_URL,
    connectionKey() {
      return sharedConnectionKey('bybit');
    },
    // Bybit requires JSON ping every 20s — not WS-level ping frames
    startKeepalive(ws) {
      return setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ op: 'ping' }));
      }, 20_000);
    },
    connect(ws, underlyings) {
      ws.send(
        JSON.stringify({
          op: 'subscribe',
          args: underlyings.map((underlying) => `publicTrade.${underlying}`),
        }),
      );
    },
    subscribe(ws, underlyings) {
      ws.send(
        JSON.stringify({
          op: 'subscribe',
          args: underlyings.map((underlying) => `publicTrade.${underlying}`),
        }),
      );
    },
    parse(msg, underlyings) {
      const m = msg as Record<string, unknown>;
      if (!m['data'] || !Array.isArray(m['data'])) return [];
      const trades: TradeEvent[] = [];
      for (const item of m['data'] as unknown[]) {
        const parsed = BybitTradeSchema.safeParse(item);
        if (!parsed.success) continue;
        const tradeUnderlying = normalizeTradeUnderlying(parsed.data.s.split('-')[0] ?? '');
        if (!underlyings.includes(tradeUnderlying)) continue;
        trades.push({
          venue: 'bybit',
          tradeId: parsed.data.i ?? null,
          instrument: parsed.data.s,
          underlying: tradeUnderlying,
          side: parsed.data.S,
          price: parsed.data.p,
          size: parsed.data.v,
          iv: parsed.data.iv ?? null,
          markPrice: parsed.data.mP,
          indexPrice: parsed.data.iP,
          isBlock: parsed.data.BT === true,
          timestamp: parsed.data.T,
        });
      }
      return trades;
    },
    async seed(underlying) {
      try {
        const res = await fetch(
          `${BYBIT_REST_BASE_URL}${BYBIT_RECENT_TRADE}?category=option&baseCoin=${underlying}&limit=50`,
          { signal: AbortSignal.timeout(10_000) },
        );
        if (res.status === 403) {
          if (!bybitSeedGeoBlocked) {
            bybitSeedGeoBlocked = true;
            log.info(
              { venue: 'bybit' },
              'Bybit REST API geo-blocked (CloudFront 403) — live WS stream still active, skipping seed',
            );
          }
          return [];
        }
        if (!res.ok) throw new Error(`recent-trade HTTP ${res.status}`);
        const data = (await res.json()) as Record<string, unknown>;
        const result = data['result'] as Record<string, unknown> | undefined;
        const list = result?.['list'] as Array<Record<string, unknown>> | undefined;
        if (!list) return [];
        const trades: TradeEvent[] = [];
        for (const item of list) {
          const p = BybitRestTradeSchema.safeParse(item);
          if (!p.success) continue;
          trades.push({
            venue: 'bybit',
            tradeId: p.data.execId ?? null,
            instrument: p.data.symbol,
            underlying,
            side: p.data.side,
            price: p.data.price,
            size: p.data.size,
            iv: p.data.iv ?? null,
            markPrice: p.data.mP,
            indexPrice: p.data.iP,
            isBlock: p.data.isBlockTrade === true,
            timestamp: p.data.time,
          });
        }
        return trades;
      } catch (err: unknown) {
        // Bybit REST geo-blocks some regions (CloudFront 403) while its WS
        // stream still works — rethrow so the seed failure is logged as
        // 'trade seed failed' instead of silently reporting zero history.
        throw new Error(`bybit recent-trade seed failed: ${String(err)}`, { cause: err });
      }
    },
  },
  {
    venue: 'binance',
    url: BINANCE_OPTIONS_WS_URL,
    connectionKey() {
      return sharedConnectionKey('binance');
    },
    // No seed: Binance's only public trade history endpoint (GET /eapi/v1/trades)
    // requires a specific symbol — there is no bulk "all trades for underlying"
    // equivalent without auth. Users see no history until a live trade arrives.
    //
    // Binance server pings every 5 min and disconnects on missed pong after 15 min
    // (per websocket-market-streams.md). Sending our own ping every 3 min keeps
    // NAT/proxy entries alive and forces an inbound pong that refreshes lastMessageAt.
    startKeepalive(ws) {
      return setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) ws.ping();
      }, 180_000);
    },
    connect(ws, underlyings) {
      ws.send(
        JSON.stringify({
          method: 'SUBSCRIBE',
          params: underlyings.map((underlying) => `${underlying.toLowerCase()}usdt@optionTrade`),
          id: 1,
        }),
      );
    },
    subscribe(ws, underlyings) {
      ws.send(
        JSON.stringify({
          method: 'SUBSCRIBE',
          params: underlyings.map((underlying) => `${underlying.toLowerCase()}usdt@optionTrade`),
          id: 1,
        }),
      );
    },
    parse(msg, underlyings) {
      const m = msg as Record<string, unknown>;
      const data = (m['data'] as Record<string, unknown> | undefined) ?? m;

      const parsed = BinanceTradeSchema.safeParse(data);
      if (!parsed.success) return [];

      const tradeUnderlying = normalizeTradeUnderlying(parsed.data.s.split('-')[0] ?? '');
      if (!underlyings.includes(tradeUnderlying)) return [];

      return [
        {
          venue: 'binance' as VenueId,
          tradeId: parsed.data.t,
          instrument: parsed.data.s,
          underlying: tradeUnderlying,
          side: parsed.data.S,
          price: parsed.data.p,
          size: parsed.data.q,
          iv: null,
          markPrice: null,
          indexPrice: null,
          isBlock: parsed.data.X === 'BLOCK',
          timestamp: parsed.data.T,
        },
      ];
    },
  },
  {
    venue: 'derive',
    url: DERIVE_WS_URL,
    connectionKey() {
      return sharedConnectionKey('derive');
    },
    // Derive has no app-level heartbeat — rely on WS ping/pong.
    // 20s cadence matches OKX/Bybit and keeps the socket warm against half-open TCP.
    startKeepalive(ws) {
      return setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) ws.ping();
      }, 20_000);
    },
    connect(ws, underlyings) {
      ws.send(
        JSON.stringify({
          jsonrpc: '2.0',
          id: 1,
          method: 'subscribe',
          params: { channels: underlyings.map((underlying) => `trades.option.${underlying}`) },
        }),
      );
    },
    subscribe(ws, underlyings) {
      ws.send(
        JSON.stringify({
          jsonrpc: '2.0',
          id: 1,
          method: 'subscribe',
          params: { channels: underlyings.map((underlying) => `trades.option.${underlying}`) },
        }),
      );
    },
    parse(msg, underlyings) {
      const m = msg as Record<string, unknown>;
      if (m['method'] !== 'subscription') return [];
      const params = m['params'] as Record<string, unknown> | undefined;
      const data = params?.['data'];
      if (!Array.isArray(data)) return [];

      const trades: TradeEvent[] = [];
      for (const item of data) {
        const parsed = DeriveTradeSchema.safeParse(item);
        if (!parsed.success) continue;
        const tradeUnderlying = normalizeTradeUnderlying(
          parsed.data.instrument_name.split('-')[0] ?? '',
        );
        if (!underlyings.includes(tradeUnderlying)) continue;
        trades.push({
          venue: 'derive',
          tradeId: parsed.data.trade_id,
          instrument: parsed.data.instrument_name,
          underlying: tradeUnderlying,
          side: parsed.data.direction,
          price: parsed.data.trade_price,
          size: parsed.data.trade_amount,
          iv: null,
          markPrice: parsed.data.mark_price,
          indexPrice: parsed.data.index_price,
          isBlock: parsed.data.rfq_id != null && parsed.data.rfq_id !== '',
          timestamp: parsed.data.timestamp,
        });
      }
      return trades;
    },
    async seed(underlying) {
      const ws = new WebSocket(DERIVE_WS_URL);
      return new Promise<TradeEvent[]>((resolve) => {
        let settled = false;
        const timeout = setTimeout(() => finish([]), 10_000);
        const finish = (trades: TradeEvent[]) => {
          if (settled) return;
          settled = true;
          clearTimeout(timeout);
          ws.removeAllListeners();
          ws.close();
          resolve(trades);
        };

        ws.on('open', () => {
          // Bound the window explicitly — Derive sorts trade history oldest-first,
          // so "fetch the last page" tricks return years-old trades. A recent
          // from_timestamp guarantees the seed is fresh regardless of sort order.
          const now = Date.now();
          ws.send(
            JSON.stringify({
              jsonrpc: '2.0',
              id: 1,
              method: 'public/get_trade_history',
              params: {
                currency: underlying,
                instrument_type: 'option',
                // Derive sorts oldest-first. We request page 1 of the last 2h only — this
                // fits in 100 items for all but the most active underlyings. The live WS
                // stream covers new trades from this point forward.
                from_timestamp: now - 2 * 60 * 60 * 1000,
                to_timestamp: now,
                page: 1,
                page_size: 100,
              },
            }),
          );
        });

        ws.on('message', (raw) => {
          let msg: Record<string, unknown>;
          try {
            msg = JSON.parse(raw.toString()) as Record<string, unknown>;
          } catch {
            return; // ignore malformed frames — a throw here would crash the process
          }
          if (msg['id'] !== 1) return;

          const result = msg['result'] as Record<string, unknown> | undefined;
          const items = result?.['trades'];
          if (!Array.isArray(items)) {
            finish([]);
            return;
          }

          const trades: TradeEvent[] = [];
          for (const item of items) {
            const parsed = DeriveTradeSchema.safeParse(item);
            if (!parsed.success) continue;
            trades.push({
              venue: 'derive',
              tradeId: parsed.data.trade_id,
              instrument: parsed.data.instrument_name,
              underlying,
              side: parsed.data.direction,
              price: parsed.data.trade_price,
              size: parsed.data.trade_amount,
              iv: null,
              markPrice: parsed.data.mark_price,
              indexPrice: parsed.data.index_price,
              isBlock: parsed.data.rfq_id != null && parsed.data.rfq_id !== '',
              timestamp: parsed.data.timestamp,
            });
          }

          finish(trades);
        });

        ws.on('error', () => finish([]));
      });
    },
  },
];
