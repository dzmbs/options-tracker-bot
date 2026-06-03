import { describe, expect, it } from 'vitest';
import { VENUE_STREAMS } from './trade-runtime.js';

const deribit = VENUE_STREAMS.find((s) => s.venue === 'deribit')!;
const okx = VENUE_STREAMS.find((s) => s.venue === 'okx')!;
const bybit = VENUE_STREAMS.find((s) => s.venue === 'bybit')!;
const binance = VENUE_STREAMS.find((s) => s.venue === 'binance')!;
const derive = VENUE_STREAMS.find((s) => s.venue === 'derive')!;

const deribitMsg = {
  method: 'subscription',
  params: {
    channel: 'trades.option.BTC.100ms',
    data: [
      {
        trade_seq: 2,
        trade_id: '48079289',
        timestamp: 1590484589306,
        price: 0.0075,
        mark_price: 0.01062686,
        iv: 47.58,
        instrument_name: 'BTC-27MAY20-9000-C',
        index_price: 8956.17,
        direction: 'sell',
        amount: 3,
      },
    ],
  },
};

const okxMsg = {
  arg: { channel: 'option-trades', instType: 'OPTION', instFamily: 'BTC-USD' },
  data: [
    {
      fillVol: '0.5066007836914062',
      fwdPx: '16469.69928595038',
      idxPx: '16537.2',
      instFamily: 'BTC-USD',
      instId: 'BTC-USD-230224-18000-C',
      markPx: '0.04690107010619562',
      optType: 'C',
      px: '0.045',
      side: 'sell',
      sz: '2',
      tradeId: '38',
      ts: '1672286551080',
    },
  ],
};

const bybitMsg = {
  topic: 'publicTrade.BTC',
  type: 'snapshot',
  ts: 1672304486868,
  data: [
    {
      T: 1672304486865,
      s: 'BTC-230224-18000-C',
      S: 'Buy',
      v: '2',
      p: '350.50',
      i: 'trade-id-abc',
      BT: false,
      mP: '355.0',
      iP: '16537.2',
      iv: '0.506',
    },
  ],
};

const binanceMsg = {
  data: {
    e: 'trade',
    E: 1762856064204,
    T: 1762856064203,
    s: 'BTC-251123-126000-C',
    t: '4',
    p: '1300.000',
    q: '0.1000',
    X: 'MARKET',
    S: 'BUY',
    m: false,
  },
};

const deriveMsg = {
  method: 'subscription',
  params: {
    channel: 'trades.option.BTC',
    data: [
      {
        instrument_name: 'BTC-27JUN24-50000-C',
        direction: 'buy',
        trade_id: 'trade-12345',
        trade_price: '125.50',
        trade_amount: '10.5',
        mark_price: '125.0',
        index_price: '45000.5',
        timestamp: 1693526400000,
        rfq_id: null,
      },
    ],
  },
};

describe('Deribit trade parser', () => {
  it('parses a sell trade from docs fixture', () => {
    const trades = deribit.parse(deribitMsg, ['BTC']);
    expect(trades).toHaveLength(1);
    const t = trades[0]!;
    expect(t.venue).toBe('deribit');
    expect(t.side).toBe('sell');
    expect(t.price).toBe(0.0075);
    expect(t.size).toBe(3);
    expect(t.instrument).toBe('BTC-27MAY20-9000-C');
    expect(t.underlying).toBe('BTC');
    // Deribit sends IV as percentage — stored as fraction
    expect(t.iv).toBeCloseTo(47.58 / 100, 4);
    expect(t.indexPrice).toBe(8956.17);
    expect(t.tradeId).toBe('48079289');
    expect(t.isBlock).toBe(false);
    expect(t.timestamp).toBe(1590484589306);
  });

  it('filters out trades for non-subscribed underlying', () => {
    const trades = deribit.parse(deribitMsg, ['ETH']);
    expect(trades).toHaveLength(0);
  });

  it('returns empty array for non-subscription messages', () => {
    expect(deribit.parse({ method: 'heartbeat' }, ['BTC'])).toHaveLength(0);
    expect(deribit.parse({ method: 'public/test', result: {} }, ['BTC'])).toHaveLength(0);
    expect(deribit.parse({}, ['BTC'])).toHaveLength(0);
  });

  it('returns empty array when params.data is not an array', () => {
    const msg = { method: 'subscription', params: { data: 'not-an-array' } };
    expect(deribit.parse(msg, ['BTC'])).toHaveLength(0);
  });
});

describe('OKX trade parser', () => {
  it('parses a sell trade from docs fixture', () => {
    const trades = okx.parse(okxMsg, ['BTC']);
    expect(trades).toHaveLength(1);
    const t = trades[0]!;
    expect(t.venue).toBe('okx');
    expect(t.side).toBe('sell');
    expect(t.price).toBeCloseTo(0.045);
    expect(t.size).toBe(2);
    expect(t.instrument).toBe('BTC-USD-230224-18000-C');
    expect(t.underlying).toBe('BTC');
    // fillVol is stored as-is (already a decimal fraction from OKX)
    expect(t.iv).toBeCloseTo(0.5066, 3);
    expect(t.tradeId).toBe('38');
    expect(t.isBlock).toBe(false);
    expect(t.timestamp).toBeCloseTo(1672286551080);
  });

  it('filters out trades for non-subscribed underlying', () => {
    const trades = okx.parse(okxMsg, ['ETH']);
    expect(trades).toHaveLength(0);
  });

  it('returns empty array when data is missing', () => {
    expect(okx.parse({}, ['BTC'])).toHaveLength(0);
    expect(okx.parse({ data: 'not-array' }, ['BTC'])).toHaveLength(0);
  });
});

describe('Bybit trade parser', () => {
  it('parses a buy trade from docs fixture', () => {
    const trades = bybit.parse(bybitMsg, ['BTC']);
    expect(trades).toHaveLength(1);
    const t = trades[0]!;
    expect(t.venue).toBe('bybit');
    expect(t.side).toBe('buy');
    expect(t.price).toBeCloseTo(350.5);
    expect(t.size).toBe(2);
    expect(t.instrument).toBe('BTC-230224-18000-C');
    expect(t.underlying).toBe('BTC');
    // Bybit iv field is a decimal fraction stored as-is
    expect(t.iv).toBeCloseTo(0.506, 2);
    expect(t.isBlock).toBe(false);
    expect(t.indexPrice).toBeCloseTo(16537.2);
    expect(t.markPrice).toBeCloseTo(355.0);
    expect(t.tradeId).toBe('trade-id-abc');
    expect(t.timestamp).toBe(1672304486865);
  });

  it('filters out trades for non-subscribed underlying', () => {
    const trades = bybit.parse(bybitMsg, ['ETH']);
    expect(trades).toHaveLength(0);
  });

  it('marks block trades correctly', () => {
    const blockMsg = {
      ...bybitMsg,
      data: [{ ...bybitMsg.data[0], BT: true }],
    };
    const trades = bybit.parse(blockMsg, ['BTC']);
    expect(trades[0]?.isBlock).toBe(true);
  });
});

describe('Binance trade parser', () => {
  it('parses a buy trade from docs fixture (wrapped in data)', () => {
    const trades = binance.parse(binanceMsg, ['BTC']);
    expect(trades).toHaveLength(1);
    const t = trades[0]!;
    expect(t.venue).toBe('binance');
    expect(t.side).toBe('buy');
    expect(t.price).toBeCloseTo(1300.0);
    expect(t.size).toBeCloseTo(0.1);
    expect(t.instrument).toBe('BTC-251123-126000-C');
    expect(t.underlying).toBe('BTC');
    expect(t.isBlock).toBe(false);
    expect(t.timestamp).toBe(1762856064203);
  });

  it('also accepts unwrapped trade object directly', () => {
    const trades = binance.parse(binanceMsg.data, ['BTC']);
    expect(trades).toHaveLength(1);
    expect(trades[0]?.price).toBeCloseTo(1300.0);
  });

  it('filters out trades for non-subscribed underlying', () => {
    const trades = binance.parse(binanceMsg, ['ETH']);
    expect(trades).toHaveLength(0);
  });

  it('returns empty for non-trade events', () => {
    const msg = { data: { e: 'kline', s: 'BTC-251123-126000-C' } };
    expect(binance.parse(msg, ['BTC'])).toHaveLength(0);
  });
});

describe('Derive trade parser', () => {
  it('parses a buy trade from docs fixture', () => {
    const trades = derive.parse(deriveMsg, ['BTC']);
    expect(trades).toHaveLength(1);
    const t = trades[0]!;
    expect(t.venue).toBe('derive');
    expect(t.side).toBe('buy');
    expect(t.price).toBeCloseTo(125.5);
    expect(t.size).toBeCloseTo(10.5);
    expect(t.instrument).toBe('BTC-27JUN24-50000-C');
    expect(t.underlying).toBe('BTC');
    expect(t.tradeId).toBe('trade-12345');
    expect(t.timestamp).toBe(1693526400000);
    expect(t.isBlock).toBe(false);
  });

  it('marks as block when rfq_id is set', () => {
    const withRfq = {
      ...deriveMsg,
      params: {
        ...deriveMsg.params,
        data: [{ ...deriveMsg.params.data[0], rfq_id: 'rfq-abc-123' }],
      },
    };
    const trades = derive.parse(withRfq, ['BTC']);
    expect(trades[0]?.isBlock).toBe(true);
  });

  it('filters out trades for non-subscribed underlying', () => {
    const trades = derive.parse(deriveMsg, ['ETH']);
    expect(trades).toHaveLength(0);
  });

  it('returns empty for non-subscription messages', () => {
    expect(derive.parse({ id: 1, result: {} }, ['BTC'])).toHaveLength(0);
    expect(derive.parse({ method: 'heartbeat' }, ['BTC'])).toHaveLength(0);
  });
});
