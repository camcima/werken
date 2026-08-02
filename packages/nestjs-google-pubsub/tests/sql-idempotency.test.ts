import { describe, expect, test, vi } from "vitest";
import { createSqlIdempotencyStore } from "@werken/nestjs-google-pubsub";
import type { CloudEventContext, IdempotencyKey, SqlExecutor } from "@werken/nestjs-google-pubsub";

const key: IdempotencyKey = {
  consumer: "reconciliation",
  source: "https://example.test/service",
  id: "01931b7c-3f2a-7000-8000-000000000001",
};

const ctx = { id: key.id, source: key.source, type: "com.example.thing.v1" } as CloudEventContext;

function fakeExecutor(rowCounts: number[]) {
  const calls: Array<{ sql: string; params: readonly unknown[] }> = [];
  let i = 0;
  const executor: SqlExecutor = {
    execute: vi.fn(async (sql: string, params: readonly unknown[]) => {
      calls.push({ sql, params });
      return { rowCount: rowCounts[i++] ?? 0 };
    }),
  };
  return { executor, calls };
}

describe("tryRecord", () => {
  test("is true when the insert affected a row", async () => {
    const { executor } = fakeExecutor([1]);
    const store = createSqlIdempotencyStore({ executor: () => executor });

    expect(await store.tryRecord(key, 60_000, ctx)).toBe(true);
  });

  test("is false when ON CONFLICT DO NOTHING affected no rows", async () => {
    const { executor } = fakeExecutor([0]);
    const store = createSqlIdempotencyStore({ executor: () => executor });

    expect(await store.tryRecord(key, 60_000, ctx)).toBe(false);
  });

  // Only the shape is asserted here. Whether Postgres actually returns zero rows on conflict, and
  // whether an expired marker can be re-recorded, are contracts with the database and are covered
  // by tests/integration/sql-idempotency.integration.test.ts.
  test("records with a single conflict-guarded upsert", async () => {
    const { executor, calls } = fakeExecutor([1]);
    await createSqlIdempotencyStore({ executor: () => executor }).tryRecord(key, 60_000, ctx);

    expect(calls[0].sql).toMatch(/insert into/i);
    expect(calls[0].sql).toMatch(/on conflict/i);
  });

  // Both statements RETURNING means an adapter only ever reports "rows returned", which is the one
  // thing every driver agrees on.
  test("ends in RETURNING so adapters never have to distinguish reads from writes", async () => {
    const { executor, calls } = fakeExecutor([1]);
    const store = createSqlIdempotencyStore({ executor: () => executor });

    await store.tryRecord(key, 60_000, ctx);
    await store.has(key, ctx);

    expect(calls[0].sql).toMatch(/returning/i);
    expect(calls[1].sql).toMatch(/select 1/i);
  });

  test("passes every value as a $n parameter, never inlined", async () => {
    const { executor, calls } = fakeExecutor([1]);
    const hostile = { ...key, id: "'; drop table werken_processed_events; --" };
    await createSqlIdempotencyStore({ executor: () => executor }).tryRecord(hostile, 60_000, ctx);

    expect(calls[0].sql).not.toContain("drop table");
    expect(calls[0].params).toContain(hostile.id);
    expect(calls[0].sql).toMatch(/\$1/);
  });
});

describe("has", () => {
  test("is false when no live row matches", async () => {
    const { executor } = fakeExecutor([0]);

    expect(await createSqlIdempotencyStore({ executor: () => executor }).has(key, ctx)).toBe(false);
  });

  test("is true when a live row matches", async () => {
    const { executor } = fakeExecutor([1]);

    expect(await createSqlIdempotencyStore({ executor: () => executor }).has(key, ctx)).toBe(true);
  });

  test("ignores rows whose expiry has passed", async () => {
    const { executor, calls } = fakeExecutor([0]);
    await createSqlIdempotencyStore({ executor: () => executor }).has(key, ctx);

    expect(calls[0].sql).toMatch(/expires_at\s*>\s*now\(\)/i);
  });
});

describe("per-message executor", () => {
  // Resolved per message so a consumer can later hand back a transaction-scoped executor bound to
  // the handler's own transaction (Mode B) without changing this port.
  test("resolves the executor for every call, not once at construction", async () => {
    const { executor } = fakeExecutor([1, 1, 1]);
    const resolve = vi.fn(() => executor);
    const store = createSqlIdempotencyStore({ executor: resolve });

    await store.has(key, ctx);
    await store.tryRecord(key, 60_000, ctx);

    expect(resolve).toHaveBeenCalledTimes(2);
  });

  test("passes the event context to the resolver", async () => {
    const { executor } = fakeExecutor([1]);
    const resolve = vi.fn(() => executor);
    await createSqlIdempotencyStore({ executor: resolve }).has(key, ctx);

    expect(resolve).toHaveBeenCalledWith(ctx);
  });

  test("awaits an async resolver", async () => {
    const { executor } = fakeExecutor([1]);
    const store = createSqlIdempotencyStore({ executor: async () => executor });

    expect(await store.tryRecord(key, 60_000, ctx)).toBe(true);
  });
});

describe("table name", () => {
  test("defaults to werken_processed_events", async () => {
    const { executor, calls } = fakeExecutor([0]);
    await createSqlIdempotencyStore({ executor: () => executor }).has(key, ctx);

    expect(calls[0].sql).toContain("werken_processed_events");
  });

  test("honours a custom table", async () => {
    const { executor, calls } = fakeExecutor([0]);
    await createSqlIdempotencyStore({ executor: () => executor, table: "inbox_events" }).has(key, ctx);

    expect(calls[0].sql).toContain("inbox_events");
  });

  test("rejects a table name that is not a plain identifier", () => {
    // The one value that cannot be a $n parameter, so it is validated instead.
    expect(() =>
      createSqlIdempotencyStore({ executor: () => fakeExecutor([]).executor, table: "a; drop table b" }),
    ).toThrow(/identifier/i);
  });
});

describe("transport configuration", () => {
  test("rejects supplying both store and executor rather than silently picking one", async () => {
    const { WerkenPubSubTransport } = await import("@werken/nestjs-google-pubsub");
    const { executor } = fakeExecutor([1]);

    expect(
      () =>
        new WerkenPubSubTransport({
          projectId: "p",
          subscription: "s",
          idempotency: {
            consumer: "c",
            executor: () => executor,
            store: { tryRecord: async () => true, has: async () => false },
          },
        }),
    ).toThrow(/mutually exclusive/i);
  });
});
