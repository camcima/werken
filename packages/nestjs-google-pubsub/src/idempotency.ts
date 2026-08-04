import type { CloudEventContext } from "./types.js";

/**
 * Identifies one delivery of one event to one consumer.
 *
 * `consumer` is part of the key on purpose: two services consuming the same event must each
 * process it once, not once between them. `source` is part of it because `id` is only unique
 * within a source, per CloudEvents 1.0.
 */
export interface IdempotencyKey {
  readonly consumer: string;
  readonly source: string;
  readonly id: string;
}

/**
 * Duplicate suppression port.
 *
 * The event context is passed to every call so an implementation can resolve per-message state —
 * a connection, a tenant, or a transaction — rather than being fixed at construction.
 */
export interface IdempotencyStore {
  /**
   * Records the key. True if newly recorded; false if it was already present.
   *
   * `MessagePipeline` calls this only *after* a handler has succeeded, and `has()` is what decides
   * whether the handler runs at all. So `false` here does not suppress anything — the side effect
   * has already happened. It means another replica recorded the same event between this pipeline's
   * `has()` and this call, which is a real cross-replica duplicate, and the pipeline logs it at WARN
   * rather than letting it pass unseen.
   *
   * Implement it atomically anyway (`ON CONFLICT`, `SET NX`): the return value is the only place
   * that race is observable, and a transactional Mode B store would rely on the atomicity directly.
   */
  tryRecord(key: IdempotencyKey, ttlMs: number, ctx: CloudEventContext): Promise<boolean>;
  has(key: IdempotencyKey, ctx: CloudEventContext): Promise<boolean>;
}

/**
 * Stable string form of a key, for stores that need a single-column identity.
 *
 * JSON rather than a delimiter: `ce-source` is a URI-reference, `consumer` is caller-supplied and
 * `ce-id` is producer-controlled, so any separator one of them could contain would let two
 * different keys flatten to one string — and the collision presents as an event silently dropped as
 * a duplicate, not as an error. JSON escapes the field boundaries instead of hoping they never
 * appear, which no unescaped separator can promise.
 */
export function idempotencyKeyToString(key: IdempotencyKey): string {
  return JSON.stringify([key.consumer, key.source, key.id]);
}

// ---------------------------------------------------------------------------
// SQL store
// ---------------------------------------------------------------------------

/**
 * The single database operation this library needs, so it can talk to Postgres through whatever
 * driver or ORM the consumer already uses without depending on any of them.
 *
 * `rowCount` must be **the number of rows the statement returned**.
 *
 * Both statements this store issues end in `RETURNING`, so "rows returned" is all an adapter ever
 * has to report — no distinguishing reads from writes, which is where drivers disagree most. Most
 * can simply return `rows.length`.
 *
 * Get this wrong and the failure is silent, not loud, in both directions:
 *  - always-0 makes `has` report every message as new, disabling de-duplication entirely, and makes
 *    `tryRecord` report every write as already present, so each message logs a spurious duplicate;
 *  - always-positive makes `has` report every message as already processed, so every message is
 *    acked without the handler ever running. That is the one that loses work.
 *
 * See the README adapter examples, which are verified against each driver's actual types.
 */
export interface SqlExecutor {
  execute(sql: string, params: readonly unknown[]): Promise<{ rowCount: number }>;
}

export const DEFAULT_IDEMPOTENCY_TABLE = "werken_processed_events";

/** Postgres identifiers cannot be bound as parameters, so the table name is validated instead. */
const SAFE_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

export interface SqlIdempotencyStoreOptions {
  /**
   * Resolves the executor for a given message. Deliberately per-message rather than fixed at
   * construction:
   *
   *  - **Mode A (today).** Return a pool-backed executor. The dedup insert runs in its own
   *    transaction, separate from whatever the handler does. This is best-effort de-duplication:
   *    a crash between the handler's write and the insert still yields a duplicate.
   *  - **Mode B (later).** Return an executor scoped to the transaction the handler is about to
   *    use, so the dedup insert commits atomically with the business write — a transactional
   *    inbox. Same port, no redesign needed.
   */
  executor: (ctx: CloudEventContext) => SqlExecutor | Promise<SqlExecutor>;
  /** Defaults to `werken_processed_events`. Must be a plain SQL identifier. */
  table?: string;
}

/**
 * Postgres-backed idempotency store with no driver dependency.
 *
 * Postgres-only by design: it relies on `ON CONFLICT DO NOTHING` and `$n` placeholders. Supporting
 * other dialects would mean generating SQL per dialect, which is a bigger job than this port is
 * worth — a non-Postgres consumer should implement `IdempotencyStore` directly instead.
 *
 * DDL is not run by this library. Copy `docs/idempotency-schema.sql` into your own migrations.
 */
export function createSqlIdempotencyStore(options: SqlIdempotencyStoreOptions): IdempotencyStore {
  const table = assertIdentifier(options.table ?? DEFAULT_IDEMPOTENCY_TABLE);

  return {
    async tryRecord(key, ttlMs, ctx) {
      const executor = await options.executor(ctx);
      // A single atomic statement, so two concurrent consumers of the same event cannot both win a
      // read-then-write race.
      //
      // DO UPDATE ... WHERE expires_at <= now(), not DO NOTHING: the row outlives its TTL until
      // something prunes it, so DO NOTHING would keep matching an expired row and report every
      // later delivery as a duplicate — the key would never actually free up. The guarded update
      // refreshes an expired marker (returning a row, so "newly recorded") and leaves a live one
      // untouched (returning none, so "already present").
      const result = await executor.execute(
        `INSERT INTO ${table} (consumer, source, event_id, expires_at)
         VALUES ($1, $2, $3, now() + ($4::bigint * interval '1 millisecond'))
         ON CONFLICT (consumer, source, event_id) DO UPDATE
           SET expires_at = EXCLUDED.expires_at, processed_at = now()
           WHERE ${table}.expires_at <= now()
         RETURNING 1`,
        [key.consumer, key.source, key.id, String(ttlMs)],
      );
      return result.rowCount > 0;
    },

    async has(key, ctx) {
      const executor = await options.executor(ctx);
      const result = await executor.execute(
        `SELECT 1 FROM ${table}
         WHERE consumer = $1 AND source = $2 AND event_id = $3 AND expires_at > now()`,
        [key.consumer, key.source, key.id],
      );
      return result.rowCount > 0;
    },
  };
}

/**
 * SQL to delete expired markers. Run it from your own scheduled job — deliberately not a timer the
 * library starts, because a library quietly issuing DELETEs against a consumer's database is a
 * surprise nobody wants.
 */
export function pruneExpiredSql(table: string = DEFAULT_IDEMPOTENCY_TABLE): string {
  return `DELETE FROM ${assertIdentifier(table)} WHERE expires_at < now()`;
}

function assertIdentifier(name: string): string {
  if (!SAFE_IDENTIFIER.test(name)) {
    throw new Error(`werken: table name ${JSON.stringify(name)} is not a valid SQL identifier`);
  }
  return name;
}

// ---------------------------------------------------------------------------
// Built-in stores
// ---------------------------------------------------------------------------

export interface InMemoryIdempotencyStoreOptions {
  now?: () => number;
}

/**
 * Process-local store. Useful for tests and single-instance consumers; it cannot de-duplicate
 * across replicas, so it is not a production answer for a scaled-out worker pool.
 */
export class InMemoryIdempotencyStore implements IdempotencyStore {
  private readonly seen = new Map<string, number>();
  private readonly now: () => number;

  constructor(options: InMemoryIdempotencyStoreOptions = {}) {
    this.now = options.now ?? Date.now;
  }

  async tryRecord(key: IdempotencyKey, ttlMs: number): Promise<boolean> {
    const id = idempotencyKeyToString(key);
    if (this.isLive(id)) return false;
    this.seen.set(id, this.now() + ttlMs);
    return true;
  }

  async has(key: IdempotencyKey): Promise<boolean> {
    return this.isLive(idempotencyKeyToString(key));
  }

  /** Drops expired entries. Call periodically if the process is long-lived. */
  prune(): number {
    const now = this.now();
    let removed = 0;
    for (const [id, expiresAt] of this.seen) {
      if (expiresAt <= now) {
        this.seen.delete(id);
        removed++;
      }
    }
    return removed;
  }

  private isLive(id: string): boolean {
    const expiresAt = this.seen.get(id);
    if (expiresAt === undefined) return false;
    if (expiresAt <= this.now()) {
      this.seen.delete(id);
      return false;
    }
    return true;
  }
}

/**
 * The default when no store is configured: every message is treated as new.
 *
 * It warns loudly at construction rather than silently doing nothing, because the failure mode —
 * duplicate side effects under redelivery — is invisible until it has already happened.
 */
export class NoopIdempotencyStore implements IdempotencyStore {
  constructor(logger: Pick<Console, "warn"> = console) {
    logger.warn(
      "werken: no idempotency store configured — duplicate deliveries WILL be reprocessed. " +
        "Pub/Sub is at-least-once, so configure a store before running in production.",
    );
  }

  async tryRecord(): Promise<boolean> {
    return true;
  }

  async has(): Promise<boolean> {
    return false;
  }
}
