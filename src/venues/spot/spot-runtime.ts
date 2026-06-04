import { z } from 'zod';

import { feedLogger } from '../../utils/logger.js';
import {
  BYBIT_REST_BASE_URL,
  BYBIT_TICKERS,
  DERIBIT_REST_BASE_URL,
  OKX_REST_BASE_URL,
} from '../shared/endpoints.js';

const log = feedLogger('spot-runtime');
const DEFAULT_POLL_INTERVAL_MS = 60_000;
const FETCH_TIMEOUT_MS = 10_000;
// A snapshot older than this many poll intervals is treated as missing —
// inverse-venue USD premiums computed from hours-old spot are silently wrong,
// which is worse than no number at all.
const MAX_SNAPSHOT_AGE_POLLS = 5;
// After this many consecutive all-source poll failures, escalate to error
// and flip health.connected so /status shows the degradation.
const FULL_FAILURE_ESCALATION_THRESHOLD = 3;

export interface SpotSnapshot {
  symbol: string;
  source: string;
  lastPrice: number;
  prevPrice24h: number | null;
  change24hPct: number | null;
  high24h: number | null;
  low24h: number | null;
  updatedAt: number;
}

export interface SpotRuntimeHealth {
  connected: boolean;
  symbols: string[];
  lastSuccessAt: number | null;
  lastStatusAt: number | null;
  errors: number;
}

export interface SpotRuntimeSnapshotEvent {
  type: 'snapshot';
  snapshot: SpotSnapshot;
}

export type SpotRuntimeEvent = SpotRuntimeSnapshotEvent;

export interface SpotRuntimeListener {
  onEvent(event: SpotRuntimeEvent): void;
}

export interface SpotRuntimeOptions {
  pollIntervalMs?: number;
  fetchImpl?: typeof fetch;
  log?: {
    warn: (obj: object, msg: string) => void;
  };
}

const BybitSpotTickerSchema = z.object({
  retCode: z.number(),
  result: z.object({
    list: z.array(
      z.object({
        lastPrice: z.string(),
        prevPrice24h: z.string(),
        price24hPcnt: z.string(),
        highPrice24h: z.string(),
        lowPrice24h: z.string(),
      }),
    ),
  }),
});

const OkxSpotTickerSchema = z.object({
  code: z.string(),
  data: z.array(
    z.object({
      last: z.string(),
      open24h: z.string(),
      high24h: z.string(),
      low24h: z.string(),
    }),
  ),
});

const DeribitIndexSchema = z.object({
  result: z.object({
    index_price: z.number(),
  }),
});

interface SpotSource {
  name: string;
  fetch(symbol: string, fetchImpl: typeof fetch): Promise<SpotSnapshot | null>;
}

function baseOf(symbol: string): string {
  return symbol.replace(/USDT$/, '');
}

// Sources are tried in order per symbol each poll. Venue REST APIs geo-block
// inconsistently (e.g. Bybit/Binance behind CloudFront country blocks while
// their WS feeds still work), so a single-source spot poller is a deployment
// footgun — failover keeps reference prices flowing anywhere.
const SOURCES: SpotSource[] = [
  {
    name: 'bybit',
    async fetch(symbol, fetchImpl) {
      const response = await fetchImpl(
        `${BYBIT_REST_BASE_URL}${BYBIT_TICKERS}?category=spot&symbol=${symbol}`,
        { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) },
      );
      if (!response.ok) return null;
      const parsed = BybitSpotTickerSchema.safeParse(await response.json());
      if (!parsed.success || parsed.data.retCode !== 0) return null;
      const item = parsed.data.result.list[0];
      if (item == null) return null;
      return {
        symbol,
        source: 'bybit',
        lastPrice: Number(item.lastPrice),
        prevPrice24h: Number(item.prevPrice24h),
        change24hPct: Number(item.price24hPcnt),
        high24h: Number(item.highPrice24h),
        low24h: Number(item.lowPrice24h),
        updatedAt: Date.now(),
      };
    },
  },
  {
    name: 'okx',
    async fetch(symbol, fetchImpl) {
      const instId = `${baseOf(symbol)}-USDT`;
      const response = await fetchImpl(
        `${OKX_REST_BASE_URL}/api/v5/market/ticker?instId=${instId}`,
        {
          signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
        },
      );
      if (!response.ok) return null;
      const parsed = OkxSpotTickerSchema.safeParse(await response.json());
      if (!parsed.success || parsed.data.code !== '0') return null;
      const item = parsed.data.data[0];
      if (item == null) return null;
      const last = Number(item.last);
      const open = Number(item.open24h);
      return {
        symbol,
        source: 'okx',
        lastPrice: last,
        prevPrice24h: open,
        change24hPct: open > 0 ? (last - open) / open : null,
        high24h: Number(item.high24h),
        low24h: Number(item.low24h),
        updatedAt: Date.now(),
      };
    },
  },
  {
    name: 'deribit',
    async fetch(symbol, fetchImpl) {
      const indexName = `${baseOf(symbol).toLowerCase()}_usd`;
      const response = await fetchImpl(
        `${DERIBIT_REST_BASE_URL}/api/v2/public/get_index_price?index_name=${indexName}`,
        { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) },
      );
      if (!response.ok) return null;
      const parsed = DeribitIndexSchema.safeParse(await response.json());
      if (!parsed.success) return null;
      return {
        symbol,
        source: 'deribit',
        lastPrice: parsed.data.result.index_price,
        prevPrice24h: null,
        change24hPct: null,
        high24h: null,
        low24h: null,
        updatedAt: Date.now(),
      };
    },
  },
];

export class SpotRuntime {
  private readonly snapshots = new Map<string, SpotSnapshot>();
  private readonly listeners = new Set<SpotRuntimeListener>();
  private readonly fetchImpl: typeof fetch;
  private readonly pollIntervalMs: number;
  private readonly runtimeLog: { warn: (obj: object, msg: string) => void };
  // Remember the last working source per symbol so steady-state polls hit one
  // endpoint instead of walking through geo-blocked ones every cycle.
  private readonly preferredSource = new Map<string, string>();
  private pollTimer: ReturnType<typeof setInterval> | null = null;
  private symbols: string[] = [];
  private started = false;
  private polling = false;
  private consecutiveFullFailures = 0;
  private health: SpotRuntimeHealth = {
    connected: false,
    symbols: [],
    lastSuccessAt: null,
    lastStatusAt: null,
    errors: 0,
  };

  constructor(options: SpotRuntimeOptions = {}) {
    this.fetchImpl = options.fetchImpl ?? fetch;
    this.pollIntervalMs = options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
    this.runtimeLog = options.log ?? log;
  }

  async start(symbols: string[] = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']): Promise<void> {
    this.symbols = [...symbols];
    this.health.symbols = [...symbols];

    await this.poll();

    if (this.started) return;
    this.started = true;
    this.pollTimer = setInterval(() => {
      void this.poll();
    }, this.pollIntervalMs);
  }

  subscribe(listener: SpotRuntimeListener): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  getSnapshot(baseOrSymbol: string): SpotSnapshot | null {
    const symbol = baseOrSymbol.endsWith('USDT') ? baseOrSymbol : `${baseOrSymbol}USDT`;
    const snapshot = this.snapshots.get(symbol);
    if (snapshot == null) return null;
    const maxAgeMs = this.pollIntervalMs * MAX_SNAPSHOT_AGE_POLLS;
    if (Date.now() - snapshot.updatedAt > maxAgeMs) return null;
    return snapshot;
  }

  getAllSnapshots(): SpotSnapshot[] {
    return [...this.snapshots.values()];
  }

  getHealth(): SpotRuntimeHealth {
    return {
      ...this.health,
      symbols: [...this.health.symbols],
    };
  }

  dispose(): void {
    if (this.pollTimer != null) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
    }
    this.started = false;
  }

  private orderedSources(symbol: string): SpotSource[] {
    const preferred = this.preferredSource.get(symbol);
    if (preferred == null) return SOURCES;
    return [...SOURCES].sort((a, b) => (a.name === preferred ? -1 : b.name === preferred ? 1 : 0));
  }

  private async poll(): Promise<void> {
    if (this.polling) return;
    this.polling = true;
    try {
      let sawSuccess = false;

      for (const symbol of this.symbols) {
        let snapshot: SpotSnapshot | null = null;

        for (const source of this.orderedSources(symbol)) {
          try {
            snapshot = await source.fetch(symbol, this.fetchImpl);
          } catch {
            snapshot = null;
          }
          if (snapshot != null) {
            this.preferredSource.set(symbol, source.name);
            break;
          }
        }

        if (snapshot == null) {
          this.health.errors += 1;
          this.health.lastStatusAt = Date.now();
          this.runtimeLog.warn({ symbol }, 'spot fetch failed on all sources');
          continue;
        }

        sawSuccess = true;
        this.snapshots.set(symbol, snapshot);
        this.broadcast({ type: 'snapshot', snapshot });
      }

      if (sawSuccess) {
        this.consecutiveFullFailures = 0;
        this.health.connected = true;
        this.health.lastSuccessAt = Date.now();
        this.health.lastStatusAt = Date.now();
      } else if (this.symbols.length > 0) {
        this.consecutiveFullFailures += 1;
        if (this.consecutiveFullFailures >= FULL_FAILURE_ESCALATION_THRESHOLD) {
          this.health.connected = false;
          log.error(
            { consecutiveFailures: this.consecutiveFullFailures, symbols: this.symbols },
            'spot prices unavailable from all sources — inverse-venue USD amounts degraded',
          );
        }
      }
    } finally {
      this.polling = false;
    }
  }

  private broadcast(event: SpotRuntimeEvent): void {
    for (const listener of this.listeners) {
      try {
        listener.onEvent(event);
      } catch (err: unknown) {
        this.runtimeLog.warn({ err: String(err) }, 'spot listener failed');
      }
    }
  }
}
