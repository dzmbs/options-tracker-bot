declare const __brand: unique symbol;
type Brand<T, B extends string> = T & { readonly [__brand]: B };

export type UnixMs = Brand<number, 'UnixMs'>;

export const VENUE_IDS = ['deribit', 'okx', 'binance', 'bybit', 'derive'] as const;

export type VenueId = (typeof VENUE_IDS)[number];

export type OptionRight = 'call' | 'put';

export type DataSource = 'rest' | 'ws' | 'poll';
