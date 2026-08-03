import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { Pool } from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, test } from "vitest";
import { createSqlIdempotencyStore, pruneExpiredSql } from "@werken/nestjs-google-pubsub";
import type { CloudEventContext, IdempotencyKey, SqlExecutor } from "@werken/nestjs-google-pubsub";
import { skipUnlessAvailable } from "./requires.js";

const DATABASE_URL = process.env.DATABASE_URL;
const SCHEMA_SQL = fileURLToPath(new URL("../../../../docs/idempotency-schema.sql", import.meta.url));

/**
 * The SQL store against real Postgres.
 *
 * The unit tests can only check the SQL text and the boolean mapping — they assert against a fake
 * whose rowCount I choose, so they cannot tell me whether `ON CONFLICT DO NOTHING RETURNING`
 * actually yields zero rows on conflict, whether the bigint/interval expression is valid, or
 * whether the DDL we ship even applies. Those are contracts with Postgres, so they are tested
 * against Postgres.
 */
describe.skipIf(skipUnlessAvailable("DATABASE_URL", DATABASE_URL))("SQL idempotency store against Postgres", () => {
  const pool = new Pool({ connectionString: DATABASE_URL });
  const ctx = {} as CloudEventContext;

  const executor: SqlExecutor = {
    execute: async (sql, params) => {
      const result = await pool.query(sql, params as unknown[]);
      return { rowCount: result.rowCount ?? 0 };
    },
  };
  const store = createSqlIdempotencyStore({ executor: () => executor });

  const key = (overrides: Partial<IdempotencyKey> = {}): IdempotencyKey => ({
    consumer: "consumer-a",
    source: "https://example.test/service",
    id: "01931b7c-3f2a-7000-8000-000000000001",
    ...overrides,
  });

  beforeAll(async () => {
    // Applies the file we actually ship, so a broken migration fails here rather than in a
    // consumer's pipeline.
    await pool.query(readFileSync(SCHEMA_SQL, "utf8"));
  });

  beforeEach(async () => {
    await pool.query("TRUNCATE werken_processed_events");
  });

  afterAll(async () => {
    await pool.end();
  });

  test("the shipped DDL creates the documented table", async () => {
    const columns = await pool.query<{ column_name: string }>(
      `SELECT column_name FROM information_schema.columns
       WHERE table_name = 'werken_processed_events' ORDER BY column_name`,
    );

    expect(columns.rows.map((r) => r.column_name)).toEqual([
      "consumer",
      "event_id",
      "expires_at",
      "processed_at",
      "source",
    ]);
  });

  test("the DDL is idempotent, as a migration pipeline will re-run it", async () => {
    await expect(pool.query(readFileSync(SCHEMA_SQL, "utf8"))).resolves.toBeDefined();
  });

  test("records a key the first time and refuses it the second", async () => {
    expect(await store.tryRecord(key(), 60_000, ctx)).toBe(true);
    // The real question a fake cannot answer: does ON CONFLICT DO NOTHING RETURNING give 0 rows?
    expect(await store.tryRecord(key(), 60_000, ctx)).toBe(false);
  });

  test("has() reflects what was actually written", async () => {
    expect(await store.has(key(), ctx)).toBe(false);
    await store.tryRecord(key(), 60_000, ctx);
    expect(await store.has(key(), ctx)).toBe(true);
  });

  test("scopes by consumer, so two services each process the event once", async () => {
    expect(await store.tryRecord(key({ consumer: "consumer-a" }), 60_000, ctx)).toBe(true);
    expect(await store.tryRecord(key({ consumer: "consumer-b" }), 60_000, ctx)).toBe(true);
    expect(await store.has(key({ consumer: "consumer-b" }), ctx)).toBe(true);
  });

  test("scopes by source, since ids are only unique within a source", async () => {
    await store.tryRecord(key({ source: "https://a.test" }), 60_000, ctx);

    expect(await store.has(key({ source: "https://b.test" }), ctx)).toBe(false);
  });

  // Exercises `now() + ($4::bigint * interval '1 millisecond')` for real — the cast and the
  // interval multiplication are both places a hand-written expression can be silently wrong.
  test("expires a marker once its TTL has passed", async () => {
    await store.tryRecord(key(), 50, ctx);
    expect(await store.has(key(), ctx)).toBe(true);

    await new Promise((r) => setTimeout(r, 120));

    expect(await store.has(key(), ctx)).toBe(false);
    // Expired means re-recordable, not permanently blocked by the primary key.
    expect(await store.tryRecord(key(), 60_000, ctx)).toBe(true);
  });

  test("stores a TTL far enough out for the documented 7-day default", async () => {
    await store.tryRecord(key(), 7 * 24 * 60 * 60 * 1000, ctx);
    const row = await pool.query<{ days: number }>(
      "SELECT EXTRACT(day FROM expires_at - now()) AS days FROM werken_processed_events",
    );

    expect(Number(row.rows[0].days)).toBe(6); // 6.99… days
  });

  test("a concurrent burst records exactly one winner", async () => {
    // The reason tryRecord is a single statement rather than read-then-write.
    const results = await Promise.all(Array.from({ length: 20 }, () => store.tryRecord(key(), 60_000, ctx)));

    expect(results.filter(Boolean)).toHaveLength(1);
  });

  test("pruneExpiredSql deletes only expired rows", async () => {
    await store.tryRecord(key({ id: "expires-soon" }), 50, ctx);
    await store.tryRecord(key({ id: "expires-later" }), 60_000, ctx);
    await new Promise((r) => setTimeout(r, 120));

    const deleted = await pool.query(pruneExpiredSql());

    expect(deleted.rowCount).toBe(1);
    expect(await store.has(key({ id: "expires-later" }), ctx)).toBe(true);
  });

  test("treats a hostile event id as data, not SQL", async () => {
    const hostile = key({ id: "'; DROP TABLE werken_processed_events; --" });

    expect(await store.tryRecord(hostile, 60_000, ctx)).toBe(true);
    expect(await store.has(hostile, ctx)).toBe(true);
    // The table is still there.
    await expect(pool.query("SELECT 1 FROM werken_processed_events")).resolves.toBeDefined();
  });
});
