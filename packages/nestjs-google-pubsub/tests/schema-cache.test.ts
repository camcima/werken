import { describe, expect, test, vi } from "vitest";
import { SchemaRevisionCache } from "@werken/nestjs-google-pubsub";

const REV_A = "projects/p/schemas/thing@aaaa1111";
const REV_B = "projects/p/schemas/thing@bbbb2222";

describe("caching", () => {
  test("fetches once and serves the cached value thereafter", async () => {
    const fetch = vi.fn(async (key: string) => `compiled:${key}`);
    const cache = new SchemaRevisionCache<string>({ fetch });

    expect(await cache.get(REV_A)).toBe(`compiled:${REV_A}`);
    expect(await cache.get(REV_A)).toBe(`compiled:${REV_A}`);

    expect(fetch).toHaveBeenCalledTimes(1);
  });

  // §5.3: producers move between revisions independently, so the revision id is the only safe key.
  test("keys by revision, so two revisions of one schema do not collide", async () => {
    const fetch = vi.fn(async (key: string) => `compiled:${key}`);
    const cache = new SchemaRevisionCache<string>({ fetch });

    expect(await cache.get(REV_A)).toBe(`compiled:${REV_A}`);
    expect(await cache.get(REV_B)).toBe(`compiled:${REV_B}`);

    expect(fetch).toHaveBeenCalledTimes(2);
  });
});

describe("single-flight", () => {
  // Without this, a cold start under load fires one Schema Service call per in-flight message.
  test("concurrent misses on one revision make a single fetch", async () => {
    let resolveFetch: ((v: string) => void) | undefined;
    const fetch = vi.fn(
      () =>
        new Promise<string>((resolve) => {
          resolveFetch = resolve;
        }),
    );
    const cache = new SchemaRevisionCache<string>({ fetch });

    const all = Promise.all([cache.get(REV_A), cache.get(REV_A), cache.get(REV_A)]);
    resolveFetch!("compiled");

    expect(await all).toEqual(["compiled", "compiled", "compiled"]);
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  test("a failed fetch is not cached, so the next call retries", async () => {
    const fetch = vi
      .fn<(key: string) => Promise<string>>()
      .mockRejectedValueOnce(new Error("transient"))
      .mockResolvedValueOnce("compiled");
    const cache = new SchemaRevisionCache<string>({ fetch });

    await expect(cache.get(REV_A)).rejects.toThrow("transient");
    expect(await cache.get(REV_A)).toBe("compiled");
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  test("concurrent waiters all see a failure rather than hanging", async () => {
    const fetch = vi.fn(async () => {
      throw new Error("boom");
    });
    const cache = new SchemaRevisionCache<string>({ fetch });

    const results = await Promise.allSettled([cache.get(REV_A), cache.get(REV_A)]);

    expect(results.every((r) => r.status === "rejected")).toBe(true);
    expect(fetch).toHaveBeenCalledTimes(1);
  });
});

describe("bounds", () => {
  test("evicts least-recently-used entries past maxEntries", async () => {
    const fetch = vi.fn(async (key: string) => `compiled:${key}`);
    const cache = new SchemaRevisionCache<string>({ fetch, maxEntries: 2 });

    await cache.get("r1");
    await cache.get("r2");
    await cache.get("r1"); // r1 is now the most recently used
    await cache.get("r3"); // evicts r2

    expect(fetch).toHaveBeenCalledTimes(3);
    await cache.get("r1");
    expect(fetch).toHaveBeenCalledTimes(3); // still cached
    await cache.get("r2");
    expect(fetch).toHaveBeenCalledTimes(4); // was evicted
  });

  test("expires entries past the TTL so corrections get picked up", async () => {
    let clock = 1000;
    const fetch = vi.fn(async (key: string) => `compiled:${key}`);
    const cache = new SchemaRevisionCache<string>({ fetch, ttlMs: 500, now: () => clock });

    await cache.get(REV_A);
    clock += 499;
    await cache.get(REV_A);
    expect(fetch).toHaveBeenCalledTimes(1);

    clock += 2;
    await cache.get(REV_A);
    expect(fetch).toHaveBeenCalledTimes(2);
  });
});

describe("observability", () => {
  test("reports hits and misses so cache hit rate can be asserted", async () => {
    const cache = new SchemaRevisionCache<string>({ fetch: async (k) => k });

    await cache.get(REV_A);
    await cache.get(REV_A);
    await cache.get(REV_B);

    expect(cache.stats).toEqual({ hits: 1, misses: 2 });
  });
});
