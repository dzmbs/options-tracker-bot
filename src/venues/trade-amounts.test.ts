import { describe, expect, it } from 'vitest';
import type { BlockTradeEvent } from './block-trades/types.js';
import {
  computeBlockTradeAmounts,
  computeLiveTradeAmounts,
  getVenueContractMultiplier,
  isInversePremiumVenue,
  parseTradeInstrument,
} from './trade-amounts.js';
import type { TradeEvent } from './trades/types.js';

describe('parseTradeInstrument', () => {
  it('parses Deribit/Bybit format BTC-27MAY20-9000-C', () => {
    const r = parseTradeInstrument('BTC-27MAY20-9000-C');
    expect(r.expiry).toBe('2020-05-27');
    expect(r.strike).toBe(9000);
    expect(r.optionType).toBe('call');
  });

  it('parses OKX format BTC-USD-230224-18000-C', () => {
    const r = parseTradeInstrument('BTC-USD-230224-18000-C');
    expect(r.expiry).toBe('2023-02-24');
    expect(r.strike).toBe(18000);
    expect(r.optionType).toBe('call');
  });

  it('parses Binance format BTC-251123-126000-C', () => {
    const r = parseTradeInstrument('BTC-251123-126000-C');
    expect(r.expiry).toBe('2025-11-23');
    expect(r.strike).toBe(126000);
    expect(r.optionType).toBe('call');
  });

  it('parses put option ETH-27JUN26-2000-P', () => {
    const r = parseTradeInstrument('ETH-27JUN26-2000-P');
    expect(r.optionType).toBe('put');
    expect(r.strike).toBe(2000);
    expect(r.expiry).toBe('2026-06-27');
  });

  it('returns nulls for non-option instrument BTC-PERPETUAL', () => {
    const r = parseTradeInstrument('BTC-PERPETUAL');
    expect(r.expiry).toBeNull();
    expect(r.strike).toBeNull();
    expect(r.optionType).toBeNull();
  });
});

describe('getVenueContractMultiplier', () => {
  it('returns 0.01 for OKX BTC', () => {
    expect(getVenueContractMultiplier('okx', 'BTC')).toBe(0.01);
  });
  it('returns 0.1 for OKX ETH', () => {
    expect(getVenueContractMultiplier('okx', 'ETH')).toBe(0.1);
  });
  it('returns 1 for OKX SOL', () => {
    expect(getVenueContractMultiplier('okx', 'SOL')).toBe(1);
  });
  it('returns 1 for deribit BTC', () => {
    expect(getVenueContractMultiplier('deribit', 'BTC')).toBe(1);
  });
  it('returns 1 for bybit ETH', () => {
    expect(getVenueContractMultiplier('bybit', 'ETH')).toBe(1);
  });
});

describe('isInversePremiumVenue', () => {
  it('returns true for deribit and okx', () => {
    expect(isInversePremiumVenue('deribit')).toBe(true);
    expect(isInversePremiumVenue('okx')).toBe(true);
  });
  it('returns false for bybit, binance, derive', () => {
    expect(isInversePremiumVenue('bybit')).toBe(false);
    expect(isInversePremiumVenue('binance')).toBe(false);
    expect(isInversePremiumVenue('derive')).toBe(false);
  });
});

function makeTrade(overrides: Partial<TradeEvent>): TradeEvent {
  return {
    venue: 'deribit',
    tradeId: '1',
    instrument: 'BTC-27MAY20-9000-C',
    underlying: 'BTC',
    side: 'sell',
    price: 0.0075,
    size: 3,
    iv: null,
    markPrice: null,
    indexPrice: null,
    isBlock: false,
    timestamp: 1590484589306,
    ...overrides,
  };
}

describe('computeLiveTradeAmounts', () => {
  it('deribit BTC: coin-denominated premium requires reference price', () => {
    // price=0.0075 BTC, amount=3 contracts, index=8956.17
    const amounts = computeLiveTradeAmounts(makeTrade({}), 8956.17);
    expect(amounts.contracts).toBe(3);
    // premiumUsd = 0.0075 × 3 × 8956.17 ≈ 201.6
    expect(amounts.premiumUsd).toBeCloseTo(0.0075 * 3 * 8956.17, 0);
    expect(amounts.notionalUsd).toBeCloseTo(3 * 8956.17, 0);
    expect(amounts.referencePriceUsd).toBe(8956.17);
  });

  it('deribit: null premiumUsd and notionalUsd when no reference price', () => {
    const amounts = computeLiveTradeAmounts(makeTrade({}), null);
    expect(amounts.premiumUsd).toBeNull();
    expect(amounts.notionalUsd).toBeNull();
  });

  it('okx BTC: 0.01 contract multiplier', () => {
    // OKX: px=0.045 BTC, sz=2, index=16537.2
    const trade = makeTrade({ venue: 'okx', price: 0.045, size: 2, underlying: 'BTC' });
    const amounts = computeLiveTradeAmounts(trade, 16537.2);
    // contracts = 2 × 0.01 = 0.02
    expect(amounts.contracts).toBeCloseTo(0.02);
    // premiumUsd = 0.045 × 0.02 × 16537.2 ≈ 14.88
    expect(amounts.premiumUsd).toBeCloseTo(0.045 * 0.02 * 16537.2, 1);
  });

  it('okx ETH: 0.1 contract multiplier', () => {
    const trade = makeTrade({ venue: 'okx', price: 0.05, size: 5, underlying: 'ETH' });
    const amounts = computeLiveTradeAmounts(trade, 3000);
    expect(amounts.contracts).toBeCloseTo(0.5);
    expect(amounts.premiumUsd).toBeCloseTo(0.05 * 0.5 * 3000, 1);
  });

  it('bybit: USD-denominated price, no coin conversion', () => {
    const trade = makeTrade({ venue: 'bybit', price: 350.5, size: 2, underlying: 'BTC' });
    const amounts = computeLiveTradeAmounts(trade, 65000);
    expect(amounts.contracts).toBe(2);
    expect(amounts.premiumUsd).toBeCloseTo(350.5 * 2, 2);
  });

  it('binance: USD-denominated, size in contracts', () => {
    const trade = makeTrade({ venue: 'binance', price: 1300, size: 0.1, underlying: 'BTC' });
    const amounts = computeLiveTradeAmounts(trade, 126000);
    expect(amounts.contracts).toBe(0.1);
    expect(amounts.premiumUsd).toBeCloseTo(1300 * 0.1, 2);
  });

  it('derive: USD-denominated (USDC)', () => {
    const trade = makeTrade({ venue: 'derive', price: 125.5, size: 10.5, underlying: 'BTC' });
    const amounts = computeLiveTradeAmounts(trade, 45000);
    expect(amounts.contracts).toBe(10.5);
    expect(amounts.premiumUsd).toBeCloseTo(125.5 * 10.5, 2);
  });
});

function makeBlockTrade(overrides: Partial<BlockTradeEvent>): BlockTradeEvent {
  return {
    venue: 'okx',
    tradeId: 'test-id',
    timestamp: 1780679169564,
    underlying: 'BTC',
    direction: 'buy',
    strategy: null,
    legs: [],
    totalSize: 1,
    rawAmountUsd: 0,
    indexPrice: null,
    ...overrides,
  };
}

describe('computeBlockTradeAmounts', () => {
  it('coin-denominated leg (priceIsUsd: false) is multiplied by reference', () => {
    // OKX BTC option: px=0.005 BTC, sz=2, multiplier=0.01, ref=16537.2
    const trade = makeBlockTrade({
      venue: 'okx',
      totalSize: 2,
      legs: [
        {
          instrument: 'BTC-USD-230224-18000-C',
          direction: 'buy',
          price: 0.005,
          priceIsUsd: false,
          size: 2,
          ratio: 1,
        },
      ],
    });
    const amounts = computeBlockTradeAmounts(trade, 16537.2);
    // contracts = 2 × 0.01 = 0.02
    // premiumUsd = 0.005 × 2 × 1 × 0.01 × 16537.2
    expect(amounts.premiumUsd).toBeCloseTo(0.005 * 2 * 1 * 0.01 * 16537.2, 2);
    expect(amounts.contracts).toBeCloseTo(0.02);
  });

  it('USD-denominated leg (priceIsUsd: true) is used directly', () => {
    const trade = makeBlockTrade({
      venue: 'bybit',
      totalSize: 1,
      legs: [
        {
          instrument: 'BTC-241227-60000-C',
          direction: 'buy',
          price: 580.5,
          priceIsUsd: true,
          size: 1,
          ratio: 1,
        },
      ],
    });
    const amounts = computeBlockTradeAmounts(trade, 60000);
    expect(amounts.premiumUsd).toBeCloseTo(580.5, 2);
  });

  it('USD leg: null reference price does not affect premiumUsd', () => {
    const trade = makeBlockTrade({
      venue: 'binance',
      totalSize: 0.1,
      legs: [
        {
          instrument: 'BTC-251123-126000-C',
          direction: 'sell',
          price: 1300,
          priceIsUsd: true,
          size: 0.1,
          ratio: 1,
        },
      ],
    });
    const amounts = computeBlockTradeAmounts(trade, null);
    expect(amounts.premiumUsd).toBeCloseTo(1300 * 0.1, 2);
    // notionalUsd requires reference price
    expect(amounts.notionalUsd).toBeNull();
  });

  it('coin leg: null premiumUsd when no reference price', () => {
    const trade = makeBlockTrade({
      venue: 'deribit',
      totalSize: 3,
      legs: [
        {
          instrument: 'BTC-27MAY20-9000-C',
          direction: 'sell',
          price: 0.0075,
          priceIsUsd: false,
          size: 3,
          ratio: 1,
        },
      ],
    });
    const amounts = computeBlockTradeAmounts(trade, null);
    expect(amounts.premiumUsd).toBeNull();
    expect(amounts.notionalUsd).toBeNull();
  });

  it('multi-leg: premiumUsd is sum of all legs', () => {
    const trade = makeBlockTrade({
      venue: 'bybit',
      totalSize: 2,
      legs: [
        {
          instrument: 'BTC-241227-60000-C',
          direction: 'buy',
          price: 500,
          priceIsUsd: true,
          size: 2,
          ratio: 1,
        },
        {
          instrument: 'BTC-241227-70000-C',
          direction: 'sell',
          price: 200,
          priceIsUsd: true,
          size: 2,
          ratio: 1,
        },
      ],
    });
    const amounts = computeBlockTradeAmounts(trade, 60000);
    // premiumUsd = (500 × 2 × 1) + (200 × 2 × 1) = 1000 + 400 = 1400
    expect(amounts.premiumUsd).toBeCloseTo(1400, 2);
  });
});
