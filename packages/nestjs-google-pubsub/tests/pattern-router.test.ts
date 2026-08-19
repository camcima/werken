import { describe, expect, test } from "vitest";
import { AmbiguousPatternError, InvalidPatternError } from "@werken/nestjs-google-pubsub";
import { PatternRouter } from "@werken/nestjs-google-pubsub/internal";

const handler = (name: string) => ({ name }) as unknown as () => void;

function router(patterns: Record<string, unknown>) {
  return new PatternRouter(Object.entries(patterns) as Array<[string, () => void]>);
}

const nameOf = (route: unknown) => (route as { handler: { name?: string } } | null)?.handler?.name;

/** The matched pattern is what telemetry labels on, so resolve() has to report it. */
const patternOf = (route: unknown) => (route as { pattern?: string } | null)?.pattern;

describe("exact patterns", () => {
  test("routes an exact match", () => {
    const r = router({ "com.example.thing.v1": handler("exact") });

    expect(nameOf(r.resolve("com.example.thing.v1"))).toBe("exact");
  });

  test("returns null when nothing matches", () => {
    const r = router({ "com.example.thing.v1": handler("exact") });

    expect(r.resolve("com.example.other.v1")).toBeNull();
  });
});

describe("suffix wildcards", () => {
  test("matches one trailing segment", () => {
    const r = router({ "com.example.*": handler("wild") });

    expect(nameOf(r.resolve("com.example.thing"))).toBe("wild");
  });

  test("matches several trailing segments", () => {
    const r = router({ "com.example.*": handler("wild") });

    expect(nameOf(r.resolve("com.example.thing.happened.v1"))).toBe("wild");
  });

  // "one or more trailing segments" — the prefix alone is not a match.
  test("does not match the prefix itself", () => {
    const r = router({ "com.example.*": handler("wild") });

    expect(r.resolve("com.example")).toBeNull();
  });

  test("does not match a different prefix", () => {
    const r = router({ "com.example.*": handler("wild") });

    expect(r.resolve("com.other.thing")).toBeNull();
  });

  // Segment-aware: the wildcard must not behave like a bare string prefix.
  test("does not treat a partial segment as a prefix match", () => {
    const r = router({ "com.example.*": handler("wild") });

    expect(r.resolve("com.exampleother.thing")).toBeNull();
  });
});

describe("catch-all", () => {
  test("matches anything", () => {
    const r = router({ "*": handler("all") });

    expect(nameOf(r.resolve("literally.anything.at.all"))).toBe("all");
  });

  test("matches a single-segment type", () => {
    const r = router({ "*": handler("all") });

    expect(nameOf(r.resolve("bare"))).toBe("all");
  });
});

describe("precedence", () => {
  test("exact beats wildcard", () => {
    const r = router({
      "com.example.thing.v1": handler("exact"),
      "com.example.*": handler("wild"),
      "*": handler("all"),
    });

    expect(nameOf(r.resolve("com.example.thing.v1"))).toBe("exact");
  });

  test("the longest literal prefix wins among wildcards", () => {
    const r = router({
      "com.*": handler("short"),
      "com.example.*": handler("long"),
      "*": handler("all"),
    });

    expect(nameOf(r.resolve("com.example.thing.v1"))).toBe("long");
  });

  test("catch-all is the last resort", () => {
    const r = router({ "com.example.*": handler("wild"), "*": handler("all") });

    expect(nameOf(r.resolve("org.other.thing"))).toBe("all");
  });

  test("precedence does not depend on registration order", () => {
    const forwards = router({ "com.*": handler("short"), "com.example.*": handler("long") });
    const backwards = router({ "com.example.*": handler("long"), "com.*": handler("short") });

    expect(nameOf(forwards.resolve("com.example.x"))).toBe("long");
    expect(nameOf(backwards.resolve("com.example.x"))).toBe("long");
  });
});

describe("ambiguity", () => {
  // Nest chains duplicate event handlers via .next instead of overwriting, so a second
  // @EventPattern for the same type registers but never runs. Silent handler loss found in
  // production is far worse than a startup failure.
  test("rejects two handlers registered for the same pattern", () => {
    const chained = Object.assign(handler("first"), { next: handler("second") });

    expect(() => new PatternRouter([["com.example.thing.v1", chained as never]])).toThrow(AmbiguousPatternError);
  });

  test("names the pattern that is doubly registered", () => {
    const chained = Object.assign(handler("first"), { next: handler("second") });

    expect(() => new PatternRouter([["com.example.thing.v1", chained as never]])).toThrow(/com\.example\.thing\.v1/);
  });
});

describe("pattern validation", () => {
  // A mid-pattern wildcard would silently never match, which is the worst possible outcome.
  test.each(["com.*.thing", "*.example.thing", "com.*.*"])("rejects the unsupported pattern %s", (pattern) => {
    expect(() => router({ [pattern]: handler("x") })).toThrow(InvalidPatternError);
  });

  test("rejects a wildcard inside a segment", () => {
    expect(() => router({ "com.exam*ple.*": handler("x") })).toThrow(InvalidPatternError);
  });

  test("explains what is supported", () => {
    expect(() => router({ "com.*.thing": handler("x") })).toThrow(/trailing|suffix|final/i);
  });

  test("accepts the three supported shapes", () => {
    expect(() =>
      router({ "com.example.thing.v1": handler("a"), "com.example.*": handler("b"), "*": handler("c") }),
    ).not.toThrow();
  });
});

describe("the matched pattern", () => {
  test("reports the registered pattern, not the type that matched it", () => {
    const r = router({ "com.example.*": handler("wild") });

    expect(patternOf(r.resolve("com.example.thing.happened.v1"))).toBe("com.example.*");
  });

  test("reports the type itself for an exact registration", () => {
    const r = router({ "com.example.thing.v1": handler("exact") });

    expect(patternOf(r.resolve("com.example.thing.v1"))).toBe("com.example.thing.v1");
  });

  test("reports the catch-all as its own pattern", () => {
    const r = router({ "*": handler("all") });

    expect(patternOf(r.resolve("literally.anything"))).toBe("*");
  });
});

describe("lookup caching", () => {
  // Build the resolved map once; patterns must not be re-scanned for every message.
  test("resolves a repeated type without rescanning", () => {
    const r = router({ "com.example.*": handler("wild") });
    const first = r.resolve("com.example.thing");

    expect(r.resolve("com.example.thing")).toBe(first);
    expect(r.stats.scans).toBe(1);
  });

  test("caches misses too, so unhandled traffic is cheap", () => {
    const r = router({ "com.example.*": handler("wild") });
    r.resolve("org.other");
    r.resolve("org.other");

    expect(r.stats.scans).toBe(1);
  });

  // ce-type is producer-controlled. A subscription carrying a wide spread of types this consumer
  // ignores — or a producer emitting unique ones — must not grow the miss cache without bound.
  test("bounds the cache rather than growing once per distinct type", () => {
    const r = new PatternRouter([["com.example.*", handler("wild")]] as Array<[string, () => void]>, {
      maxCachedTypes: 8,
    });

    for (let i = 0; i < 500; i++) r.resolve(`org.other.thing.${i}`);

    expect(r.stats.cached).toBeLessThanOrEqual(8);
  });

  test("still routes correctly once entries have been evicted", () => {
    const r = new PatternRouter(
      [
        ["com.example.thing.v1", handler("exact")],
        ["com.example.*", handler("wild")],
      ] as Array<[string, () => void]>,
      { maxCachedTypes: 2 },
    );

    for (let i = 0; i < 50; i++) r.resolve(`org.other.thing.${i}`);

    expect(nameOf(r.resolve("com.example.thing.v1"))).toBe("exact");
    expect(nameOf(r.resolve("com.example.something.else"))).toBe("wild");
    expect(r.resolve("org.unmatched")).toBeNull();
  });
});

/**
 * Nest represents two @EventPattern handlers for one pattern as a linked list, which
 * assertNotChained already catches. A caller constructing PatternRouter directly gets no such
 * marker: duplicate exact entries silently overwrote one another, and duplicate wildcards silently
 * kept the first. Either way a handler registered in good faith never runs.
 */
describe("duplicate entries passed directly", () => {
  const entries = (pattern: string) =>
    [
      [pattern, handler("first")],
      [pattern, handler("second")],
    ] as Array<[string, () => void]>;

  test("rejects two exact entries for the same pattern", () => {
    expect(() => new PatternRouter(entries("com.example.thing.v1"))).toThrow(AmbiguousPatternError);
  });

  test("rejects two wildcard entries for the same pattern", () => {
    expect(() => new PatternRouter(entries("com.example.*"))).toThrow(AmbiguousPatternError);
  });

  test("rejects two catch-all entries", () => {
    expect(() => new PatternRouter(entries("*"))).toThrow(AmbiguousPatternError);
  });

  test("names the duplicated pattern", () => {
    expect(() => new PatternRouter(entries("com.example.thing.v1"))).toThrow(/com\.example\.thing\.v1/);
  });

  test("still allows distinct patterns that overlap", () => {
    expect(() => router({ "com.example.thing.v1": handler("exact"), "com.example.*": handler("wild") })).not.toThrow();
  });
});

/**
 * Nest registers `@MessagePattern` and `@EventPattern` handlers into the same map, distinguishing
 * them only by the `isEventHandler` flag it stamps on the callback. This transport carries events:
 * a request/response handler has no reply path here, so its return value would be computed and
 * silently dropped. Refusing at startup is the same trade the duplicate-pattern check makes.
 */
describe("request/response handlers", () => {
  const messageHandler = (name: string) =>
    Object.assign(handler(name), { isEventHandler: false }) as unknown as () => void;

  test("rejects a @MessagePattern handler", () => {
    expect(() => new PatternRouter([["com.example.thing.v1", messageHandler("rpc")]])).toThrow(InvalidPatternError);
  });

  test("names the offending pattern and the decorator to change", () => {
    expect(() => new PatternRouter([["com.example.thing.v1", messageHandler("rpc")]])).toThrow(
      /com\.example\.thing\.v1[\s\S]*EventPattern/,
    );
  });

  test("accepts a handler Nest marked as an event handler", () => {
    const eventHandler = Object.assign(handler("event"), { isEventHandler: true }) as unknown as () => void;

    expect(() => new PatternRouter([["com.example.thing.v1", eventHandler]])).not.toThrow();
  });

  // A router built directly — by this repo's own tests, or by anything not going through Nest —
  // carries no flag at all, and must keep working.
  test("accepts an unflagged handler", () => {
    expect(() => new PatternRouter([["com.example.thing.v1", handler("plain")]])).not.toThrow();
  });
});
