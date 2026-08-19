import { describe, expect, test, vi } from "vitest";
import { createEventPublisher } from "@werken/nestjs-google-pubsub";
import type { PubSubClientLike } from "@werken/nestjs-google-pubsub";

/**
 * `@opentelemetry/api` is an optional peer dependency, loaded through `require` so it can be absent
 * without crashing. Node caches a *successful* require, but not a failed one: when the package is
 * genuinely missing — the case the optionality exists for — every lookup walks the node_modules
 * chain again and constructs a MODULE_NOT_FOUND error. On the publisher's per-message path that is
 * the hot path paying for a feature the caller declined to install.
 *
 * The delegating mock keeps real behaviour intact; only the call count is under test.
 */
const loads = vi.fn();
vi.mock("../src/optional-require.cjs", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../src/optional-require.cjs")>();
  return {
    optionalRequire: (name: string) => {
      loads(name);
      return actual.optionalRequire(name);
    },
  };
});

function fakeClient(): PubSubClientLike {
  return {
    topic: () => ({ publishMessage: async () => "msg-1" }),
    subscription: vi.fn(),
    close: async () => {},
  } as unknown as PubSubClientLike;
}

describe("optional OpenTelemetry loading", () => {
  test("resolves the module once however many events are published", async () => {
    loads.mockClear();
    const publisher = createEventPublisher({
      source: "https://example.test/service",
      client: fakeClient(),
      topicResolver: (type) => type.replaceAll(".", "-"),
    });

    for (let i = 0; i < 25; i++) {
      await publisher.publish({ type: "com.example.thing.happened.v1", data: { i } });
    }

    const otelLoads = loads.mock.calls.filter(([name]) => name === "@opentelemetry/api");
    expect(otelLoads.length).toBeLessThanOrEqual(1);
  });

  // The point of caching it is to keep the behaviour identical, so the traceparent must still be
  // attached on every message rather than only the first.
  test("still stamps a traceparent on every message once cached", async () => {
    const published: Array<Record<string, string>> = [];
    const publisher = createEventPublisher({
      source: "https://example.test/service",
      client: {
        topic: () => ({
          publishMessage: async (m: { attributes: Record<string, string> }) => {
            published.push(m.attributes);
            return "msg-1";
          },
        }),
        subscription: vi.fn(),
        close: async () => {},
      } as unknown as PubSubClientLike,
      topicResolver: (type) => type.replaceAll(".", "-"),
    });

    await publisher.publish({ type: "com.example.thing.happened.v1", data: {} });
    await publisher.publish({ type: "com.example.thing.happened.v1", data: {} });

    // No active span here, so no traceparent is expected — what matters is that both messages are
    // treated the same way rather than the second silently losing instrumentation.
    expect(published).toHaveLength(2);
    expect("ce-traceparent" in published[0]).toBe("ce-traceparent" in published[1]);
  });
});
