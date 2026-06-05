import { z } from 'zod';

import {
  BINANCE_REST_BASE_URL,
  BYBIT_REST_BASE_URL,
  BYBIT_TICKERS,
  DERIBIT_REST_BASE_URL,
  DERIVE_REST_BASE_URL,
  OKX_OPEN_INTEREST,
  OKX_REST_BASE_URL,
} from '../venues/shared/endpoints.js';
import { parseHumanExpiry } from '../venues/trade-amounts.js';

const TIMEOUT_MS = 12_000;

export type VenueId = 'deribit' | 'okx' | 'bybit' | 'binance' | 'derive';
export const ALL_VENUES: readonly VenueId[] = ['deribit', 'okx', 'bybit', 'binance', 'derive'];

export interface OiRecord {
  venue: VenueId;
  strike: number;
  expiry: string;
  optionType: 'call' | 'put';
  oiUsd: number;
}

async function fetchJson(url: string): Promise<unknown> {
  const res = await fetch(url, { signal: AbortSignal.timeout(TIMEOUT_MS) });
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${url}`);
  return res.json() as Promise<unknown>;
}

function parseNum(s: string | number | null | undefined): number {
  if (s == null) return 0;
  const n = Number(s);
  return Number.isFinite(n) && n > 0 ? n : 0;
}

function parseYYMMDD(s: string): string | null {
  if (s.length !== 6) return null;
  return `20${s.slice(0, 2)}-${s.slice(2, 4)}-${s.slice(4, 6)}`;
}

const DeribitSummaryItemSchema = z.object({
  instrument_name: z.string(),
  open_interest: z.number().nullable().optional(),
});

const DeribitBookSummarySchema = z.object({
  result: z.array(DeribitSummaryItemSchema),
});

const DeribitIndexPriceSchema = z.object({
  result: z.object({ index_price: z.number() }),
});

function parseDeribitInstrument(name: string): {
  strike: number;
  expiry: string;
  optionType: 'call' | 'put';
} | null {
  // "BTC-25DEC26-60000-C"  or  "ETH-USDC-25DEC26-60000-C"
  const parts = name.split('-');
  const type = parts[parts.length - 1];
  if (type !== 'C' && type !== 'P') return null;
  const strikeStr = parts[parts.length - 2];
  const expiryRaw = parts[parts.length - 3];
  const strike = Number(strikeStr);
  if (!Number.isFinite(strike) || strike <= 0) return null;
  const expiry = parseHumanExpiry(expiryRaw?.toUpperCase() ?? '');
  if (!expiry) return null;
  return { strike, expiry, optionType: type === 'C' ? 'call' : 'put' };
}

export async function fetchDeribitSpotPrice(underlying: string): Promise<number | null> {
  try {
    const indexName = `${underlying.toLowerCase()}_usd`;
    const url = `${DERIBIT_REST_BASE_URL}/api/v2/public/get_index_price?index_name=${encodeURIComponent(indexName)}`;
    const raw = await fetchJson(url);
    return DeribitIndexPriceSchema.parse(raw).result.index_price;
  } catch {
    return null;
  }
}

export async function fetchDeribitOi(underlying: string, spotPrice: number): Promise<OiRecord[]> {
  const url = `${DERIBIT_REST_BASE_URL}/api/v2/public/get_book_summary_by_currency?currency=${encodeURIComponent(underlying)}&kind=option`;
  const raw = await fetchJson(url);
  const parsed = DeribitBookSummarySchema.parse(raw);

  const records: OiRecord[] = [];
  for (const item of parsed.result) {
    const oi = item.open_interest;
    if (!oi || oi <= 0) continue;
    const inst = parseDeribitInstrument(item.instrument_name);
    if (!inst) continue;
    records.push({
      venue: 'deribit',
      strike: inst.strike,
      expiry: inst.expiry,
      optionType: inst.optionType,
      oiUsd: oi * spotPrice,
    });
  }
  return records;
}

export async function listDeribitExpiries(underlying: string): Promise<string[]> {
  const url = `${DERIBIT_REST_BASE_URL}/api/v2/public/get_book_summary_by_currency?currency=${encodeURIComponent(underlying)}&kind=option`;
  const raw = await fetchJson(url);
  const parsed = DeribitBookSummarySchema.parse(raw);

  const seen = new Set<string>();
  for (const item of parsed.result) {
    const inst = parseDeribitInstrument(item.instrument_name);
    if (inst) seen.add(inst.expiry);
  }
  return [...seen].sort();
}

const OkxOiItemSchema = z.object({
  instId: z.string(),
  oi: z.string().optional(),
  oiCcy: z.string().optional(),
  oiUsd: z.string().optional(),
});

const OkxOiResponseSchema = z.object({
  code: z.string(),
  data: z.array(OkxOiItemSchema),
});

function parseOkxInstId(instId: string): {
  strike: number;
  expiry: string;
  optionType: 'call' | 'put';
} | null {
  // "BTC-USD-261225-60000-C"  or  "BTC-USDT-261225-60000-C"
  const parts = instId.split('-');
  if (parts.length < 5) return null;
  const type = parts[parts.length - 1];
  if (type !== 'C' && type !== 'P') return null;
  const strikeStr = parts[parts.length - 2];
  const dateStr = parts[parts.length - 3];
  const strike = Number(strikeStr);
  if (!Number.isFinite(strike) || strike <= 0) return null;
  const expiry = parseYYMMDD(dateStr ?? '');
  if (!expiry) return null;
  return { strike, expiry, optionType: type === 'C' ? 'call' : 'put' };
}

async function fetchOkxOiFamily(instFamily: string, spotPrice: number): Promise<OiRecord[]> {
  const url = `${OKX_REST_BASE_URL}${OKX_OPEN_INTEREST}?instType=OPTION&instFamily=${encodeURIComponent(instFamily)}`;
  const raw = await fetchJson(url);
  const parsed = OkxOiResponseSchema.parse(raw);
  if (parsed.code !== '0') return [];

  const records: OiRecord[] = [];
  for (const item of parsed.data) {
    const inst = parseOkxInstId(item.instId);
    if (!inst) continue;
    const oiUsd = parseNum(item.oiUsd) || parseNum(item.oiCcy) * spotPrice;
    if (oiUsd <= 0) continue;
    records.push({
      venue: 'okx',
      strike: inst.strike,
      expiry: inst.expiry,
      optionType: inst.optionType,
      oiUsd,
    });
  }
  return records;
}

export async function fetchOkxOi(underlying: string, spotPrice: number): Promise<OiRecord[]> {
  const families = [`${underlying}-USD`, `${underlying}-USDT`];
  const results = await Promise.allSettled(families.map((f) => fetchOkxOiFamily(f, spotPrice)));
  return results.flatMap((r) => (r.status === 'fulfilled' ? r.value : []));
}

export async function listOkxExpiries(underlying: string): Promise<string[]> {
  const url = `${OKX_REST_BASE_URL}/api/v5/public/instruments?instType=OPTION&instFamily=${encodeURIComponent(`${underlying}-USD`)}`;
  const raw = await fetchJson(url);
  const parsed = OkxOiResponseSchema.safeParse(raw);
  // instruments response uses same shape: { code, data: [{ instId }] }
  if (!parsed.success || parsed.data.code !== '0') return [];
  const seen = new Set<string>();
  for (const item of parsed.data.data) {
    const inst = parseOkxInstId(item.instId);
    if (inst) seen.add(inst.expiry);
  }
  return [...seen].sort();
}

const BybitTickerItemSchema = z
  .object({
    symbol: z.string(),
    openInterest: z.string().optional(),
    openInterestValue: z.string().optional(),
  })
  .passthrough();

const BybitTickersResponseSchema = z.object({
  retCode: z.number(),
  result: z.object({ list: z.array(BybitTickerItemSchema) }),
});

export async function fetchBybitOi(underlying: string, spotPrice: number): Promise<OiRecord[]> {
  const url = `${BYBIT_REST_BASE_URL}${BYBIT_TICKERS}?category=option&baseCoin=${encodeURIComponent(underlying)}`;
  const raw = await fetchJson(url);
  const parsed = BybitTickersResponseSchema.parse(raw);
  if (parsed.retCode !== 0) return [];

  const records: OiRecord[] = [];
  for (const item of parsed.result.list) {
    // Bybit appends "-USDT" to option symbols: "BTC-25DEC26-60000-C-USDT"
    const inst = parseDeribitInstrument(item.symbol.replace(/-(?:USDT|USD)$/, ''));
    if (!inst) continue;
    // openInterestValue is notional USD; openInterest is in contracts (contractSize=1)
    const oiUsd = parseNum(item.openInterestValue) || parseNum(item.openInterest) * spotPrice;
    if (oiUsd <= 0) continue;
    records.push({
      venue: 'bybit',
      strike: inst.strike,
      expiry: inst.expiry,
      optionType: inst.optionType,
      oiUsd,
    });
  }
  return records;
}

export async function listBybitExpiries(underlying: string): Promise<string[]> {
  const url = `${BYBIT_REST_BASE_URL}${BYBIT_TICKERS}?category=option&baseCoin=${encodeURIComponent(underlying)}`;
  const raw = await fetchJson(url);
  const parsed = BybitTickersResponseSchema.parse(raw);
  if (parsed.retCode !== 0) return [];
  const seen = new Set<string>();
  for (const item of parsed.result.list) {
    const inst = parseDeribitInstrument(item.symbol.replace(/-(?:USDT|USD)$/, ''));
    if (inst) seen.add(inst.expiry);
  }
  return [...seen].sort();
}

const BinanceExchangeInfoSymbolSchema = z
  .object({ expiryDate: z.number().optional() })
  .passthrough();

const BinanceExchangeInfoSchema = z.object({
  optionSymbols: z.array(BinanceExchangeInfoSymbolSchema).optional(),
  symbols: z.array(BinanceExchangeInfoSymbolSchema).optional(),
});

export async function listBinanceExpiries(_underlying: string): Promise<string[]> {
  const url = `${BINANCE_REST_BASE_URL}/eapi/v1/exchangeInfo`;
  const raw = await fetchJson(url);
  const parsed = BinanceExchangeInfoSchema.parse(raw);
  const symbols = parsed.optionSymbols ?? parsed.symbols ?? [];

  const seen = new Set<string>();
  for (const sym of symbols) {
    if (!sym.expiryDate) continue;
    // expiryDate is a Unix timestamp in ms; filter by underlying via symbol name if needed
    const expiry = new Date(sym.expiryDate).toISOString().slice(0, 10);
    seen.add(expiry);
  }
  return [...seen].sort();
}

const BinanceOiItemSchema = z.object({
  symbol: z.string(),
  sumOpenInterest: z.string(),
  sumOpenInterestUsd: z.string().optional(),
});

const BinanceOiResponseSchema = z.array(BinanceOiItemSchema);

function parseBinanceSymbol(symbol: string): {
  strike: number;
  expiry: string;
  optionType: 'call' | 'put';
} | null {
  // "BTC-261225-60000-C"
  const parts = symbol.split('-');
  if (parts.length < 4) return null;
  const type = parts[parts.length - 1];
  if (type !== 'C' && type !== 'P') return null;
  const strikeStr = parts[parts.length - 2];
  const dateStr = parts[parts.length - 3];
  const strike = Number(strikeStr);
  if (!Number.isFinite(strike) || strike <= 0) return null;
  const expiry = parseYYMMDD(dateStr ?? '');
  if (!expiry) return null;
  return { strike, expiry, optionType: type === 'C' ? 'call' : 'put' };
}

const DeriveTickerStatsSchema = z.object({ oi: z.string().nullable().optional() }).passthrough();

const DeriveTickerSchema = z
  .object({ stats: DeriveTickerStatsSchema.nullable().optional() })
  .passthrough();

const DeriveTickersResponseSchema = z.object({
  result: z.object({ tickers: z.record(z.string(), DeriveTickerSchema) }),
});

const DeriveInstrumentSchema = z
  .object({
    instrument_name: z.string(),
    is_active: z.boolean().optional(),
    option_details: z
      .object({ expiry: z.number(), strike: z.string(), option_type: z.string() })
      .passthrough()
      .nullable()
      .optional(),
  })
  .passthrough();

const DeriveInstrumentsResponseSchema = z.object({
  result: z.array(DeriveInstrumentSchema),
});

function parseDeriveInstrumentName(name: string): {
  strike: number;
  expiry: string;
  optionType: 'call' | 'put';
} | null {
  // "BTC-20260607-60000-C"
  const parts = name.split('-');
  if (parts.length < 4) return null;
  const type = parts[3];
  if (type !== 'C' && type !== 'P') return null;
  const dateStr = parts[1]; // YYYYMMDD
  const strikeStr = parts[2];
  if (!dateStr || dateStr.length !== 8) return null;
  const strike = Number(strikeStr);
  if (!Number.isFinite(strike) || strike <= 0) return null;
  const expiry = `${dateStr.slice(0, 4)}-${dateStr.slice(4, 6)}-${dateStr.slice(6, 8)}`;
  return { strike, expiry, optionType: type === 'C' ? 'call' : 'put' };
}

export async function fetchDeriveOi(
  underlying: string,
  expiry: string,
  spotPrice: number,
): Promise<OiRecord[]> {
  // Derive needs YYYYMMDD — ISO "2026-06-07" → "20260607"
  const expiryDate = expiry.replace(/-/g, '');
  const res = await fetch(`${DERIVE_REST_BASE_URL}/public/get_tickers`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      instrument_type: 'option',
      currency: underlying,
      expiry_date: expiryDate,
    }),
    signal: AbortSignal.timeout(TIMEOUT_MS),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}: Derive get_tickers`);
  const raw = await (res.json() as Promise<unknown>);
  const parsed = DeriveTickersResponseSchema.parse(raw);

  const records: OiRecord[] = [];
  for (const [name, ticker] of Object.entries(parsed.result.tickers)) {
    const oi = parseNum(ticker.stats?.oi);
    if (oi <= 0) continue;
    const inst = parseDeriveInstrumentName(name);
    if (!inst || inst.expiry !== expiry) continue;
    records.push({
      venue: 'derive',
      strike: inst.strike,
      expiry: inst.expiry,
      optionType: inst.optionType,
      oiUsd: oi * spotPrice,
    });
  }
  return records;
}

export async function listDeriveExpiries(underlying: string): Promise<string[]> {
  const url = `${DERIVE_REST_BASE_URL}/public/get_instruments?currency=${encodeURIComponent(underlying)}&instrument_type=option&expired=false`;
  const res = await fetch(url, { signal: AbortSignal.timeout(TIMEOUT_MS) });
  if (!res.ok) throw new Error(`HTTP ${res.status}: Derive get_instruments`);
  const raw = await (res.json() as Promise<unknown>);
  const parsed = DeriveInstrumentsResponseSchema.parse(raw);

  const seen = new Set<string>();
  for (const inst of parsed.result) {
    if (!inst.option_details?.expiry) continue;
    const dt = new Date(inst.option_details.expiry * 1000);
    const expiry = dt.toISOString().slice(0, 10);
    seen.add(expiry);
  }
  return [...seen].sort();
}

export async function fetchBinanceOi(
  underlying: string,
  expiry: string,
  spotPrice: number,
): Promise<OiRecord[]> {
  // Binance requires YYMMDD: "2026-12-25" → "261225"
  const yymmdd = `${expiry.slice(2, 4)}${expiry.slice(5, 7)}${expiry.slice(8, 10)}`;
  const url = `${BINANCE_REST_BASE_URL}/eapi/v1/openInterest?underlyingAsset=${encodeURIComponent(underlying)}&expiration=${yymmdd}`;
  const raw = await fetchJson(url);
  const parsed = BinanceOiResponseSchema.parse(raw);

  const records: OiRecord[] = [];
  for (const item of parsed) {
    const inst = parseBinanceSymbol(item.symbol);
    if (!inst || inst.expiry !== expiry) continue;
    const oiUsd = parseNum(item.sumOpenInterestUsd) || parseNum(item.sumOpenInterest) * spotPrice;
    if (oiUsd <= 0) continue;
    records.push({
      venue: 'binance',
      strike: inst.strike,
      expiry: inst.expiry,
      optionType: inst.optionType,
      oiUsd,
    });
  }
  return records;
}
