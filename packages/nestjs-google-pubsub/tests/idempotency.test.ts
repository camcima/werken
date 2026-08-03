import { describe, expect, test, vi } from "vitest";
import { InMemoryIdempotencyStore, NoopIdempotencyStore, idempotencyKeyToString } from "@werken/nestjs-google-pubsub";
import type { IdempotencyKey } from "@werken/nestjs-google-pubsub";

const key = (overrides: Partial<IdempotencyKey> = {}): IdempotencyKey => ({
  consumer: "baggage-reconciliation",
  source: "https://example.test/service",
  id: "01931b7c-3f2a-7000-8000-000000000001",
  ...overrides,
});

describe("InMemoryIdempotencyStore", () => {
  test("records a key the first time", async () => {
    const store = new InMemoryIdempotencyStore();

    expect(await store.tryRecord(key(), 60_000)).toBe(true);
  });

  test("refuses to record the same key twice", async () => {
    const store = new InMemoryIdempotencyStore();
    await store.tryRecord(key(), 60_000);

    expect(await store.tryRecord(key(), 60_000)).toBe(false);
  });

  test("has() reports whether a key was recorded", async () => {
    const store = new InMemoryIdempotencyStore();

    expect(await store.has(key())).toBe(false);
    await store.tryRecord(key(), 60_000);
    expect(await store.has(key())).toBe(true);
  });

  // §5.4: two services consuming the same event must each process it once, not once between them.
  test("scopes keys per consumer", async () => {
    const store = new InMemoryIdempotencyStore();
    await store.tryRecord(key({ consumer: "service-a" }), 60_000);

    expect(await store.has(key({ consumer: "service-b" }))).toBe(false);
    expect(await store.tryRecord(key({ consumer: "service-b" }), 60_000)).toBe(true);
  });

  test("scopes keys per source, since ids are only unique within a source", async () => {
    const store = new InMemoryIdempotencyStore();
    await store.tryRecord(key({ source: "https://a.test" }), 60_000);

    expect(await store.has(key({ source: "https://b.test" }))).toBe(false);
  });

  test("forgets a key once its TTL has passed", async () => {
    let clock = 1000;
    const store = new InMemoryIdempotencyStore({ now: () => clock });
    await store.tryRecord(key(), 500);

    clock += 499;
    expect(await store.has(key())).toBe(true);

    clock += 2;
    expect(await store.has(key())).toBe(false);
    expect(await store.tryRecord(key(), 500)).toBe(true);
  });
});

describe("NoopIdempotencyStore", () => {
  // §5.4: the default must not silently no-op — an operator who forgets to configure a store
  // should find out at startup, not from duplicate side effects in production.
  test("warns once at construction that de-duplication is off", () => {
    const warn = vi.fn();
    new NoopIdempotencyStore({ warn });

    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn.mock.calls[0][0]).toMatch(/idempotenc/i);
  });

  test("always reports a key as new, so every message is processed", async () => {
    const store = new NoopIdempotencyStore({ warn: () => {} });

    expect(await store.tryRecord(key(), 60_000)).toBe(true);
    expect(await store.tryRecord(key(), 60_000)).toBe(true);
    expect(await store.has(key())).toBe(false);
  });
});

describe("prune", () => {
  test("drops only expired entries and reports the count", async () => {
    let clock = 1000;
    const store = new InMemoryIdempotencyStore({ now: () => clock });
    await store.tryRecord(key({ id: "short" }), 100);
    await store.tryRecord(key({ id: "long" }), 10_000);

    clock += 200;

    expect(store.prune()).toBe(1);
    expect(await store.has(key({ id: "short" }))).toBe(false);
    expect(await store.has(key({ id: "long" }))).toBe(true);
  });

  test("reports zero when nothing has expired", async () => {
    const store = new InMemoryIdempotencyStore();
    await store.tryRecord(key(), 60_000);

    expect(store.prune()).toBe(0);
  });
});

describe("key identity", () => {
  // The string form is what single-column stores use as their key — the README's Redis and Mongo
  // adapters included — so two different keys flattening to one string would silently drop the
  // second event as an already-processed duplicate.
  test("keeps keys distinct when a field boundary shifts between them", () => {
    const a = idempotencyKeyToString({ consumer: "orders svc", source: "https://x.test", id: "1" });
    const b = idempotencyKeyToString({ consumer: "orders", source: "svc https://x.test", id: "1" });

    expect(a).not.toBe(b);
  });

  test("is stable for the same key", () => {
    expect(idempotencyKeyToString(key())).toBe(idempotencyKeyToString(key()));
  });
});
