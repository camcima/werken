import { describe, expect, test } from "vitest";
import { ResourcePrefixError } from "@werken/nestjs-google-pubsub";
import { applyResourcePrefix, assertResourcePrefixSafe } from "@werken/nestjs-google-pubsub/internal";

/**
 * Rules verified empirically against the Pub/Sub emulator, which enforces the documented
 * constraints: 3-255 characters, must start with a letter, must not start with lowercase "goog",
 * and only [A-Za-z0-9] plus - . _ ~ + % are allowed.
 */
describe("applyResourcePrefix", () => {
  test("is a no-op when no prefix is set — the production path", () => {
    expect(applyResourcePrefix("orders-consumer", undefined)).toBe("orders-consumer");
  });

  test("is a no-op for an empty prefix", () => {
    expect(applyResourcePrefix("orders-consumer", "")).toBe("orders-consumer");
  });

  test("prefixes a short name", () => {
    expect(applyResourcePrefix("orders-consumer", "alice")).toBe("alice-orders-consumer");
  });

  test("prefixes only the last segment of a fully-qualified path", () => {
    // Both a short name and a fully-qualified path are valid, so scoping must not mangle the project path.
    expect(applyResourcePrefix("projects/p/subscriptions/orders", "alice")).toBe(
      "projects/p/subscriptions/alice-orders",
    );
  });

  test("does not double the separator when the prefix already ends with one", () => {
    expect(applyResourcePrefix("orders", "alice-")).toBe("alice-orders");
  });
});

describe("name validation", () => {
  test("accepts the characters Pub/Sub actually allows", () => {
    expect(() => applyResourcePrefix("a-b.c_d~e+f", "x")).not.toThrow();
  });

  test.each([
    ["a slash", "al/ice"],
    ["a space", "al ice"],
    ["an exclamation mark", "al!ice"],
    ["a colon", "al:ice"],
  ])("rejects %s in the prefix", (_label, prefix) => {
    expect(() => applyResourcePrefix("orders", prefix)).toThrow(ResourcePrefixError);
  });

  test("rejects a prefix that would make the name start with a digit", () => {
    expect(() => applyResourcePrefix("orders", "1alice")).toThrow(ResourcePrefixError);
  });

  test("rejects a prefix that would make the name start with goog", () => {
    expect(() => applyResourcePrefix("orders", "goog")).toThrow(ResourcePrefixError);
  });

  test("allows GOOG, which Pub/Sub accepts — the restriction is case-sensitive", () => {
    expect(applyResourcePrefix("orders", "GOOG")).toBe("GOOG-orders");
  });

  test("rejects a resolved name longer than 255 characters", () => {
    expect(() => applyResourcePrefix("a".repeat(250), "b".repeat(10))).toThrow(ResourcePrefixError);
  });

  test("accepts a resolved name of exactly 255 characters", () => {
    const resolved = applyResourcePrefix("a".repeat(249), "b".repeat(5));
    expect(resolved).toHaveLength(255);
  });

  // Reachable only when the base name is empty — a misconfigured subscription — since any
  // non-empty prefix contributes at least two characters of its own.
  test("rejects a scoped name shorter than 3 characters", () => {
    expect(() => applyResourcePrefix("", "a")).toThrow(ResourcePrefixError);
  });

  // Without a prefix the name is the caller's own and already accepted by Pub/Sub; validating it
  // would break existing consumers over a name we did not choose.
  test("does not validate the name when there is no prefix to apply", () => {
    expect(applyResourcePrefix("s", undefined)).toBe("s");
    expect(applyResourcePrefix("1-starts-with-digit", "")).toBe("1-starts-with-digit");
  });

  // Failing at startup is only useful if the operator can see what was actually built.
  test("names the resolved resource in the error, not just the prefix", () => {
    expect(() => applyResourcePrefix("orders", "al ice")).toThrow(/al ice-orders/);
  });
});

describe("production guard", () => {
  test("allows no prefix in production", () => {
    expect(() => assertResourcePrefixSafe(undefined, false, "production")).not.toThrow();
  });

  test("allows a prefix outside production", () => {
    expect(() => assertResourcePrefixSafe("alice", false, "development")).not.toThrow();
  });

  // A scoped consumer in production listens to a subscription nobody publishes to — it looks
  // healthy and receives nothing.
  test("rejects a prefix in production", () => {
    expect(() => assertResourcePrefixSafe("alice", false, "production")).toThrow(ResourcePrefixError);
  });

  test("explains the risk rather than just refusing", () => {
    expect(() => assertResourcePrefixSafe("alice", false, "production")).toThrow(/production/i);
  });

  test("honours the explicit escape hatch", () => {
    expect(() => assertResourcePrefixSafe("alice", true, "production")).not.toThrow();
  });
});
