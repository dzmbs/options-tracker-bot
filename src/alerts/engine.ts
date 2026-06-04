import type { Store } from '../store.js';
import type { BlockTradeEvent } from '../venues/block-trades/types.js';
import {
  buildBlockTradeUid,
  buildLiveTradeUid,
  type TradeAmounts,
} from '../venues/trade-amounts.js';
import type { TradeEvent } from '../venues/trades/types.js';

// Runtimes replay historical trades on startup (REST seeds, Derive's 7-day
// block lookback). Only trades printed after process start should alert;
// the skew window tolerates venue clock drift and in-flight WS frames.
const FRESHNESS_SKEW_MS = 60_000;
const SEEN_UID_TTL_MS = 60 * 60 * 1000;
const SEEN_PRUNE_INTERVAL_MS = 10 * 60 * 1000;

/**
 * One user-facing alert kind: "trades ≥ $X". Thresholds match on USD notional
 * (underlying exposure controlled); falls back to premium when notional is
 * unavailable so no trade slips through silently.
 */
export class AlertEngine {
  private readonly startedAt = Date.now();
  private readonly seenUids = new Map<string, number>();
  private readonly pruneTimer: ReturnType<typeof setInterval>;

  constructor(private readonly store: Store) {
    this.pruneTimer = setInterval(() => this.pruneSeen(), SEEN_PRUNE_INTERVAL_MS);
    this.pruneTimer.unref();
  }

  /**
   * Chat ids to alert for a live print, or null if it shouldn't fire.
   * Block-flagged prints are excluded — the block path covers the same trade
   * with leg/strategy detail, and skipping here prevents double alerts.
   */
  evaluateTrade(trade: TradeEvent, amounts: TradeAmounts): number[] | null {
    if (trade.isBlock) return null;
    const usd = amounts.notionalUsd ?? amounts.premiumUsd;
    if (usd == null) return null;
    if (!this.isFresh(trade.timestamp)) return null;
    if (!this.markSeen(buildLiveTradeUid(trade))) return null;

    return this.chatIds(trade.underlying, usd);
  }

  /** Chat ids to alert for a block/RFQ trade. */
  evaluateBlock(
    trade: BlockTradeEvent,
    premiumUsd: number | null,
    notionalUsd: number | null,
  ): number[] | null {
    const usd = notionalUsd ?? premiumUsd;
    if (usd == null) return null;
    if (!this.isFresh(trade.timestamp)) return null;
    if (!this.markSeen(buildBlockTradeUid(trade))) return null;

    return this.chatIds(trade.underlying, usd);
  }

  dispose(): void {
    clearInterval(this.pruneTimer);
  }

  private chatIds(underlying: string, usd: number): number[] | null {
    const subs = this.store.matching(underlying, usd);
    if (subs.length === 0) return null;
    return [...new Set(subs.map((sub) => sub.chatId))];
  }

  private isFresh(timestamp: number): boolean {
    return timestamp >= this.startedAt - FRESHNESS_SKEW_MS;
  }

  /** Returns false when the uid was already alerted on. */
  private markSeen(uid: string): boolean {
    if (this.seenUids.has(uid)) return false;
    this.seenUids.set(uid, Date.now());
    return true;
  }

  private pruneSeen(): void {
    const cutoff = Date.now() - SEEN_UID_TTL_MS;
    for (const [uid, seenAt] of this.seenUids) {
      if (seenAt < cutoff) this.seenUids.delete(uid);
    }
  }
}
