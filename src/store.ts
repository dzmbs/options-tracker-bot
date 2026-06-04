import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

export const ANY_UNDERLYING = '*';

export interface Subscription {
  id: number;
  chatId: number;
  underlying: string; // uppercase base, or '*' for all
  minUsd: number;
  createdAt: number;
}

interface SubscriptionRow {
  id: number;
  chat_id: number;
  underlying: string;
  min_usd: number;
  created_at: number;
}

function toSubscription(row: SubscriptionRow): Subscription {
  return {
    id: row.id,
    chatId: row.chat_id,
    underlying: row.underlying,
    minUsd: row.min_usd,
    createdAt: row.created_at,
  };
}

export class Store {
  private readonly db: DatabaseSync;

  constructor(path: string = process.env['DB_PATH'] ?? 'data/bot.db') {
    if (path !== ':memory:') mkdirSync(dirname(path), { recursive: true });
    this.db = new DatabaseSync(path);
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER NOT NULL,
        underlying TEXT NOT NULL,
        min_usd REAL NOT NULL,
        created_at INTEGER NOT NULL,
        UNIQUE (chat_id, underlying)
      );
      CREATE INDEX IF NOT EXISTS idx_alerts_match ON alerts (underlying, min_usd);
    `);
  }

  /** One threshold per (chat, underlying) — re-adding overwrites. */
  upsert(chatId: number, underlying: string, minUsd: number): void {
    this.db
      .prepare(
        `INSERT INTO alerts (chat_id, underlying, min_usd, created_at)
         VALUES (?, ?, ?, ?)
         ON CONFLICT (chat_id, underlying) DO UPDATE SET min_usd = excluded.min_usd`,
      )
      .run(chatId, underlying, minUsd, Date.now());
  }

  remove(chatId: number, underlying: string): boolean {
    const result = this.db
      .prepare('DELETE FROM alerts WHERE chat_id = ? AND underlying = ?')
      .run(chatId, underlying);
    return result.changes > 0;
  }

  listByChat(chatId: number): Subscription[] {
    const rows = this.db
      .prepare('SELECT * FROM alerts WHERE chat_id = ? ORDER BY underlying')
      .all(chatId) as unknown as SubscriptionRow[];
    return rows.map(toSubscription);
  }

  /** Alerts triggered by a trade of `usd` size on `underlying`. */
  matching(underlying: string, usd: number): Subscription[] {
    const rows = this.db
      .prepare(
        `SELECT * FROM alerts
         WHERE (underlying = ? OR underlying = '*') AND min_usd <= ?`,
      )
      .all(underlying, usd) as unknown as SubscriptionRow[];
    return rows.map(toSubscription);
  }

  close(): void {
    this.db.close();
  }
}
