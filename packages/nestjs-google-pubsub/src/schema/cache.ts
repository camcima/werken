export interface SchemaRevisionCacheOptions<T> {
  /** Loads a value for a revision key. Called at most once per key per TTL window. */
  fetch: (key: string) => Promise<T>;
  /** LRU bound. Default 200. */
  maxEntries?: number;
  /** Entry lifetime, so schema corrections get picked up. Default 1 hour. */
  ttlMs?: number;
  now?: () => number;
}

export interface SchemaCacheStats {
  hits: number;
  misses: number;
}

interface Entry<T> {
  value: T;
  expiresAt: number;
}

export const DEFAULT_MAX_CACHED_REVISIONS = 200;
export const DEFAULT_SCHEMA_CACHE_TTL_MS = 3_600_000;

/**
 * Bounded, single-flight cache keyed by schema **revision id**.
 *
 * Keying by revision rather than schema name is the whole point: producers move between revisions
 * independently, so a name-keyed cache would decode a new revision's bytes against a stale writer
 * schema — silent corruption rather than a loud failure.
 */
export class SchemaRevisionCache<T> {
  private readonly entries = new Map<string, Entry<T>>();
  private readonly inFlight = new Map<string, Promise<T>>();
  private readonly maxEntries: number;
  private readonly ttlMs: number;
  private readonly now: () => number;
  private hits = 0;
  private misses = 0;

  constructor(private readonly options: SchemaRevisionCacheOptions<T>) {
    this.maxEntries = options.maxEntries ?? DEFAULT_MAX_CACHED_REVISIONS;
    this.ttlMs = options.ttlMs ?? DEFAULT_SCHEMA_CACHE_TTL_MS;
    this.now = options.now ?? Date.now;
  }

  get stats(): SchemaCacheStats {
    return { hits: this.hits, misses: this.misses };
  }

  async get(key: string): Promise<T> {
    const entry = this.entries.get(key);
    if (entry !== undefined && entry.expiresAt > this.now()) {
      // Re-insert to mark most-recently-used; Map preserves insertion order.
      this.entries.delete(key);
      this.entries.set(key, entry);
      this.hits++;
      return entry.value;
    }
    if (entry !== undefined) this.entries.delete(key);

    // Single-flight: concurrent misses on one revision share a single fetch, so a cold start under
    // load makes one Schema Service call rather than one per in-flight message.
    const existing = this.inFlight.get(key);
    if (existing !== undefined) {
      this.hits++;
      return existing;
    }

    this.misses++;
    const pending = this.options
      .fetch(key)
      .then((value) => {
        this.store(key, value);
        return value;
      })
      .finally(() => {
        // Failures are deliberately not cached — a transient Schema Service error must not pin a
        // revision into a permanently broken state.
        this.inFlight.delete(key);
      });

    this.inFlight.set(key, pending);
    return pending;
  }

  private store(key: string, value: T): void {
    this.entries.set(key, { value, expiresAt: this.now() + this.ttlMs });
    while (this.entries.size > this.maxEntries) {
      const oldest = this.entries.keys().next();
      if (oldest.done === true) break;
      this.entries.delete(oldest.value);
    }
  }
}
