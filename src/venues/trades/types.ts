import type WebSocket from 'ws';
import type { VenueId } from '../types.js';

export interface TradeEvent {
  venue: VenueId;
  tradeId: string | null;
  instrument: string;
  underlying: string;
  side: 'buy' | 'sell';
  price: number;
  size: number;
  iv: number | null;
  markPrice: number | null;
  indexPrice: number | null;
  isBlock: boolean;
  timestamp: number;
}

export interface TradeRuntimeHealth {
  venue: VenueId;
  underlying: string;
  connected: boolean;
  lastMessageAt: number | null;
  lastTradeAt: number | null;
  lastStatusAt: number | null;
  reconnects: number;
  errors: number;
  seedTrades: number;
  bufferedTrades: number;
}

export interface TradeStreamState {
  connected: boolean;
  lastMessageAt: number | null;
  lastTradeAt: number | null;
  lastStatusAt: number | null;
  reconnects: number;
  errors: number;
  seedTrades: number;
}

export interface VenueStream {
  venue: VenueId;
  url: string | (() => string);
  connectionKey?: (underlyings: string[]) => string;
  connect: (ws: WebSocket, underlyings: string[]) => void;
  subscribe?: (ws: WebSocket, underlyings: string[]) => void;
  parse: (msg: unknown, underlyings: string[]) => TradeEvent[];
  seed?: (underlying: string) => Promise<TradeEvent[]>;
  // Set on venues whose WS stream alone produces sparse history. The runtime
  // invokes `seed()` on this interval after startup; tradeId-based dedup in
  // pushTradeEvents prevents duplicates across reseeds.
  reseedIntervalMs?: number;
  startKeepalive?: (ws: WebSocket) => ReturnType<typeof setInterval>;
}
