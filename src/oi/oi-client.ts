import {
  ALL_VENUES,
  fetchBinanceOi,
  fetchBybitOi,
  fetchDeribitOi,
  fetchDeribitSpotPrice,
  fetchDeriveOi,
  fetchOkxOi,
  listBinanceExpiries,
  listBybitExpiries,
  listDeribitExpiries,
  listDeriveExpiries,
  listOkxExpiries,
  type OiRecord,
  type VenueId,
} from './venue-oi.js';

export interface OiStrike {
  strike: number;
  callOiUsd: number;
  putOiUsd: number;
}

export interface OiData {
  underlying: string;
  expiry: string;
  spotPrice: number | null;
  venueLabel: string;
  strikes: OiStrike[];
  totalCallOiUsd: number;
  totalPutOiUsd: number;
}

export async function fetchOi(
  underlying: string,
  expiry: string,
  spotPrice: number | null,
  venueFilter?: string,
): Promise<OiData> {
  const venues: VenueId[] = venueFilter
    ? ALL_VENUES.filter((v) => v === venueFilter.toLowerCase())
    : [...ALL_VENUES];

  if (venues.length === 0)
    throw new Error(`Unknown venue "${venueFilter}". Valid: ${ALL_VENUES.join(', ')}`);

  const spot = spotPrice ?? (await fetchDeribitSpotPrice(underlying));
  if (spot == null) throw new Error(`Cannot resolve spot price for ${underlying}`);

  const [deribit, okx, bybit, binance, derive] = await Promise.allSettled([
    venues.includes('deribit') ? fetchDeribitOi(underlying, spot) : Promise.resolve([]),
    venues.includes('okx') ? fetchOkxOi(underlying, spot) : Promise.resolve([]),
    venues.includes('bybit') ? fetchBybitOi(underlying, spot) : Promise.resolve([]),
    venues.includes('binance') ? fetchBinanceOi(underlying, expiry, spot) : Promise.resolve([]),
    venues.includes('derive') ? fetchDeriveOi(underlying, expiry, spot) : Promise.resolve([]),
  ]);

  const venueResults: Array<{ id: VenueId; records: OiRecord[] }> = [
    { id: 'deribit', records: deribit.status === 'fulfilled' ? deribit.value : [] },
    { id: 'okx', records: okx.status === 'fulfilled' ? okx.value : [] },
    { id: 'bybit', records: bybit.status === 'fulfilled' ? bybit.value : [] },
    { id: 'binance', records: binance.status === 'fulfilled' ? binance.value : [] },
    { id: 'derive', records: derive.status === 'fulfilled' ? derive.value : [] },
  ];

  const strikeMap = new Map<number, { call: number; put: number }>();
  const contributers = new Set<VenueId>();

  for (const { id, records } of venueResults) {
    for (const record of records) {
      if (record.expiry !== expiry) continue;
      contributers.add(id);
      const bucket = strikeMap.get(record.strike) ?? { call: 0, put: 0 };
      if (record.optionType === 'call') bucket.call += record.oiUsd;
      else bucket.put += record.oiUsd;
      strikeMap.set(record.strike, bucket);
    }
  }

  const strikes: OiStrike[] = [...strikeMap.entries()]
    .map(([strike, { call: callOiUsd, put: putOiUsd }]) => ({ strike, callOiUsd, putOiUsd }))
    .filter((s) => s.callOiUsd > 0 || s.putOiUsd > 0)
    .sort((a, b) => a.strike - b.strike);

  const totalCallOiUsd = strikes.reduce((acc, s) => acc + s.callOiUsd, 0);
  const totalPutOiUsd = strikes.reduce((acc, s) => acc + s.putOiUsd, 0);

  const venueLabel =
    contributers.size > 0
      ? [...contributers].map((v) => v.charAt(0).toUpperCase() + v.slice(1)).join(' · ')
      : venues.map((v) => v.charAt(0).toUpperCase() + v.slice(1)).join(' · ');

  return {
    underlying,
    expiry,
    spotPrice: spot,
    venueLabel,
    strikes,
    totalCallOiUsd,
    totalPutOiUsd,
  };
}

export interface ExpiryInfo {
  expiry: string;
  venues: VenueId[];
}

export async function fetchExpiriesWithVenues(underlying: string): Promise<ExpiryInfo[]> {
  const [deribit, okx, bybit, binance, derive] = await Promise.allSettled([
    listDeribitExpiries(underlying),
    listOkxExpiries(underlying),
    listBybitExpiries(underlying),
    listBinanceExpiries(underlying),
    listDeriveExpiries(underlying),
  ]);

  const venueExpiries: Array<{ id: VenueId; expiries: string[] }> = [
    { id: 'deribit', expiries: deribit.status === 'fulfilled' ? deribit.value : [] },
    { id: 'okx', expiries: okx.status === 'fulfilled' ? okx.value : [] },
    { id: 'bybit', expiries: bybit.status === 'fulfilled' ? bybit.value : [] },
    { id: 'binance', expiries: binance.status === 'fulfilled' ? binance.value : [] },
    { id: 'derive', expiries: derive.status === 'fulfilled' ? derive.value : [] },
  ];

  const expiryVenueMap = new Map<string, VenueId[]>();
  for (const { id, expiries } of venueExpiries) {
    for (const e of expiries) {
      const existing = expiryVenueMap.get(e) ?? [];
      existing.push(id);
      expiryVenueMap.set(e, existing);
    }
  }

  return [...expiryVenueMap.entries()]
    .map(([expiry, venues]) => ({ expiry, venues }))
    .sort((a, b) => a.expiry.localeCompare(b.expiry));
}
