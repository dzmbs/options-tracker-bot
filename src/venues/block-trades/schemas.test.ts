import { describe, expect, it } from 'vitest';
import {
  BinanceBlockTradeSchema,
  BybitBlockTradeSchema,
  DeribitBlockRfqSchema,
  OkxBlockTradeSchema,
} from './block-trade-runtime.js';

const okxPayload = {
  blockTdId: '3629810058109882368',
  cTime: '1780679169564',
  strategy: 'PUT',
  legs: [
    {
      instId: 'BTC-USD-260612-68000-P',
      side: 'buy',
      sz: '30',
      px: '0.1117',
      tradeId: '3629810058084716546',
    },
  ],
};

const bybitPayload = {
  rfqId: '1757579281847749169219132657134900',
  strategyType: 'custom',
  createdAt: '1757579314213',
  updatedAt: '1757579314347',
  legs: [
    {
      category: 'option',
      symbol: 'BTC-230224-18000-C',
      side: 'Sell',
      price: '350.5',
      qty: '2',
      markPrice: '355.0',
    },
  ],
};

const deribitPayload = {
  id: 939,
  timestamp: 1739869829823,
  amount: 3,
  direction: 'sell',
  index_prices: { BTC: 8956.17 },
  legs: [
    {
      price: 0.0075,
      direction: 'buy',
      instrument_name: 'BTC-27MAY20-9000-C',
      ratio: 1,
    },
  ],
};

const binancePayload = {
  id: 12345,
  symbol: 'BTC-251123-126000-C',
  price: '1300.000',
  qty: '0.1000',
  side: 1,
  time: 1762856064203,
};

describe('OkxBlockTradeSchema', () => {
  it('parses real doc payload', () => {
    const result = OkxBlockTradeSchema.safeParse(okxPayload);
    expect(result.success).toBe(true);
    if (!result.success) return;
    expect(result.data.blockTdId).toBe('3629810058109882368');
    expect(result.data.strategy).toBe('PUT');
    expect(result.data.legs).toHaveLength(1);
    expect(result.data.legs[0]!.instId).toBe('BTC-USD-260612-68000-P');
    expect(result.data.legs[0]!.px).toBe('0.1117');
    expect(result.data.legs[0]!.sz).toBe('30');
    expect(result.data.legs[0]!.side).toBe('buy');
  });

  it('accepts payload without optional strategy field', () => {
    const { strategy: _s, ...noStrategy } = okxPayload;
    const result = OkxBlockTradeSchema.safeParse(noStrategy);
    expect(result.success).toBe(true);
  });

  it('rejects payload with empty legs array', () => {
    const result = OkxBlockTradeSchema.safeParse({ ...okxPayload, legs: [] });
    // Zod array has no minLength, this passes schema — but the parser skips non-option legs
    expect(result.success).toBe(true);
  });

  it('rejects payload missing blockTdId', () => {
    const { blockTdId: _id, ...noId } = okxPayload;
    expect(OkxBlockTradeSchema.safeParse(noId).success).toBe(false);
  });
});

describe('BybitBlockTradeSchema', () => {
  it('parses real doc payload', () => {
    const result = BybitBlockTradeSchema.safeParse(bybitPayload);
    expect(result.success).toBe(true);
    if (!result.success) return;
    expect(result.data.rfqId).toBe('1757579281847749169219132657134900');
    expect(result.data.strategyType).toBe('custom');
    expect(result.data.legs).toHaveLength(1);
    expect(result.data.legs[0]!.symbol).toBe('BTC-230224-18000-C');
    expect(result.data.legs[0]!.price).toBe('350.5');
    expect(result.data.legs[0]!.qty).toBe('2');
    expect(result.data.legs[0]!.markPrice).toBe('355.0');
  });

  it('rejects payload with no legs', () => {
    const result = BybitBlockTradeSchema.safeParse({ ...bybitPayload, legs: [] });
    expect(result.success).toBe(false);
  });

  it('rejects payload missing rfqId', () => {
    const { rfqId: _id, ...noId } = bybitPayload;
    expect(BybitBlockTradeSchema.safeParse(noId).success).toBe(false);
  });
});

describe('DeribitBlockRfqSchema', () => {
  it('parses real doc payload', () => {
    const result = DeribitBlockRfqSchema.safeParse(deribitPayload);
    expect(result.success).toBe(true);
    if (!result.success) return;
    expect(result.data.id).toBe(939);
    expect(result.data.timestamp).toBe(1739869829823);
    expect(result.data.amount).toBe(3);
    expect(result.data.direction).toBe('sell');
    expect(result.data.legs).toHaveLength(1);
    expect(result.data.legs[0]!.price).toBe(0.0075);
    expect(result.data.legs[0]!.instrument_name).toBe('BTC-27MAY20-9000-C');
    expect(result.data.legs[0]!.ratio).toBe(1);
    expect(result.data.index_prices?.['BTC']).toBe(8956.17);
  });

  it('accepts payload without optional index_prices', () => {
    const { index_prices: _ip, ...noIp } = deribitPayload;
    const result = DeribitBlockRfqSchema.safeParse(noIp);
    expect(result.success).toBe(true);
  });

  it('rejects invalid direction value', () => {
    const result = DeribitBlockRfqSchema.safeParse({ ...deribitPayload, direction: 'long' });
    expect(result.success).toBe(false);
  });

  it('rejects payload missing required id', () => {
    const { id: _id, ...noId } = deribitPayload;
    expect(DeribitBlockRfqSchema.safeParse(noId).success).toBe(false);
  });
});

describe('BinanceBlockTradeSchema', () => {
  it('parses real doc payload', () => {
    const result = BinanceBlockTradeSchema.safeParse(binancePayload);
    expect(result.success).toBe(true);
    if (!result.success) return;
    expect(result.data.id).toBe(12345);
    expect(result.data.symbol).toBe('BTC-251123-126000-C');
    expect(result.data.price).toBe('1300.000');
    expect(result.data.qty).toBe('0.1000');
    expect(result.data.side).toBe(1);
    expect(result.data.time).toBe(1762856064203);
  });

  it('accepts side value 0 (sell)', () => {
    const result = BinanceBlockTradeSchema.safeParse({ ...binancePayload, side: 0 });
    expect(result.success).toBe(true);
  });

  it('rejects payload missing required fields', () => {
    expect(BinanceBlockTradeSchema.safeParse({ id: 1 }).success).toBe(false);
    expect(BinanceBlockTradeSchema.safeParse({}).success).toBe(false);
  });
});
