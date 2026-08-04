import { EventEmitter } from "node:events";
import { afterEach, describe, expect, test, vi } from "vitest";
import { WerkenPubSubTransport } from "@werken/nestjs-google-pubsub";

class FakeSubscription extends EventEmitter {
  close = vi.fn(async () => {});
}

function fakeClient(exists = true) {
  const subscription = new FakeSubscription();
  const names: string[] = [];
  const topicNames: string[] = [];
  return {
    subscription,
    names,
    topicNames,
    client: {
      subscription: vi.fn((name: string) => {
        names.push(name);
        return Object.assign(subscription, { exists: vi.fn(async () => [exists] as [boolean]) });
      }),
      topic: vi.fn((name: string) => {
        topicNames.push(name);
        return { publishMessage: vi.fn(async () => undefined) };
      }),
      close: vi.fn(async () => {}),
    },
  };
}

const listen = (transport: WerkenPubSubTransport) =>
  new Promise<unknown>((resolve) => transport.listen((error?: unknown) => resolve(error)));

const originalEnv = process.env.NODE_ENV;
afterEach(() => {
  process.env.NODE_ENV = originalEnv;
});

describe("subscription scoping", () => {
  test("subscribes to the unscoped name when no prefix is set", async () => {
    const { client, names } = fakeClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "orders-consumer",
      createClient: () => client as never,
    });

    await listen(transport);

    expect(names[0]).toBe("orders-consumer");
  });

  test("subscribes to the scoped name when a prefix is set", async () => {
    const { client, names } = fakeClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "orders-consumer",
      resourcePrefix: "alice",
      createClient: () => client as never,
    });

    await listen(transport);

    expect(names[0]).toBe("alice-orders-consumer");
  });

  // A scoped consumer reading from an unscoped dead-letter topic would write into the shared one.
  test("scopes the dead-letter topic too", async () => {
    const { client, topicNames, subscription } = fakeClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "orders-consumer",
      deadLetterTopic: "orders-dead-letters",
      resourcePrefix: "alice",
      createClient: () => client as never,
    });

    await listen(transport);
    subscription.emit("message", {
      id: "m1",
      attributes: { "ce-specversion": "1.0" },
      data: Buffer.from("{}"),
      publishTime: new Date(),
      ack: vi.fn(),
      nack: vi.fn(),
    });
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setImmediate(r));

    expect(topicNames[0]).toBe("alice-orders-dead-letters");
  });

  test("warns at startup that names are being rewritten", async () => {
    const { client } = fakeClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "orders-consumer",
      resourcePrefix: "alice",
      createClient: () => client as never,
    });
    const warnings: string[] = [];
    vi.spyOn(transport["logger"], "warn").mockImplementation((m: unknown) => void warnings.push(String(m)));

    await listen(transport);

    expect(warnings.join("\n")).toMatch(/resourcePrefix/);
    expect(warnings.join("\n")).toMatch(/alice-orders-consumer/);
  });
});

// The prefix is resolved during startup, but the pipeline was built from the raw option, so a
// handler and the dead-letter provenance both named a subscription that had not delivered anything.
describe("the resolved name is what the pipeline reports", () => {
  const scoped = (client: unknown, extra: Record<string, unknown> = {}) =>
    new WerkenPubSubTransport({
      projectId: "p",
      subscription: "orders-consumer",
      resourcePrefix: "alice",
      createClient: () => client as never,
      ...extra,
    } as never);

  const emit = async (subscription: EventEmitter, attributes: Record<string, string>) => {
    subscription.emit("message", {
      id: "m1",
      attributes,
      data: Buffer.from("{}"),
      publishTime: new Date(),
      ack: vi.fn(),
      nack: vi.fn(),
    });
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setImmediate(r));
  };

  const VALID = {
    "ce-specversion": "1.0",
    "ce-id": "01931b7c-3f2a-7000-8000-000000000001",
    "ce-source": "https://example.test/service",
    "ce-type": "com.example.thing.happened.v1",
  };

  test("gives the handler the subscription it actually read from", async () => {
    const { client, subscription } = fakeClient();
    const transport = scoped(client);
    let seen: string | undefined;
    transport.addHandler(
      VALID["ce-type"],
      ((_data: unknown, ctx: { subscription: string }) => {
        seen = ctx.subscription;
      }) as never,
      true,
    );

    await listen(transport);
    await emit(subscription, VALID);

    expect(seen).toBe("alice-orders-consumer");
  });

  test("names the scoped subscription in dead-letter provenance", async () => {
    const published: Array<Record<string, string>> = [];
    const { client, subscription } = fakeClient();
    client.topic = vi.fn(() => ({
      publishMessage: vi.fn(async (m: { attributes: Record<string, string> }) => {
        published.push(m.attributes);
        return undefined;
      }),
    })) as never;
    const transport = scoped(client, { deadLetterTopic: "orders-dead-letters" });

    await listen(transport);
    // No ce-id, so this fails envelope validation and is dead-lettered.
    await emit(subscription, { "ce-specversion": "1.0" });

    expect(published[0]?.["werken-dl-source-subscription"]).toBe("alice-orders-consumer");
  });
});

describe("startup failures", () => {
  test("fails when the scoped subscription does not exist, naming it exactly", async () => {
    // The library never provisions resources, so a missing scoped subscription must be a loud
    // startup failure rather than a consumer that sits there receiving nothing.
    const { client } = fakeClient(false);
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "orders-consumer",
      resourcePrefix: "alice",
      createClient: () => client as never,
    });

    const error = await listen(transport);

    expect(String(error)).toMatch(/alice-orders-consumer/);
    expect(String(error)).toMatch(/does not exist/i);
  });

  // The client and subscription are created before the existence check, so a failure past that
  // point used to leave the SDK's gRPC channels, retry timers and credentials-backed client alive.
  // Repeated bootstrap attempts — a crash loop, a test suite — then stack them up.
  test("closes what it created when the scoped subscription is missing", async () => {
    const { client, subscription } = fakeClient(false);
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "orders-consumer",
      resourcePrefix: "alice",
      createClient: () => client as never,
    });

    expect(String(await listen(transport))).toMatch(/does not exist/i);

    expect(subscription.close).toHaveBeenCalledTimes(1);
    expect(client.close).toHaveBeenCalledTimes(1);
  });

  test("closes the client when the existence check itself throws", async () => {
    const { client, subscription } = fakeClient();
    client.subscription = vi.fn(() =>
      Object.assign(subscription, {
        exists: vi.fn(async () => {
          throw new Error("permission denied");
        }),
      }),
    ) as never;
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "orders-consumer",
      resourcePrefix: "alice",
      createClient: () => client as never,
    });

    expect(String(await listen(transport))).toMatch(/permission denied/);

    expect(subscription.close).toHaveBeenCalledTimes(1);
    expect(client.close).toHaveBeenCalledTimes(1);
  });

  test("fails on an invalid prefix rather than at first publish", async () => {
    const { client } = fakeClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "orders-consumer",
      resourcePrefix: "not valid",
      createClient: () => client as never,
    });

    expect(String(await listen(transport))).toMatch(/not valid-orders-consumer/);
  });

  test("refuses to scope in production", async () => {
    process.env.NODE_ENV = "production";
    const { client } = fakeClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "orders-consumer",
      resourcePrefix: "alice",
      createClient: () => client as never,
    });

    expect(String(await listen(transport))).toMatch(/production/i);
  });

  test("allows scoping in production behind the explicit escape hatch", async () => {
    process.env.NODE_ENV = "production";
    const { client, names } = fakeClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "orders-consumer",
      resourcePrefix: "alice",
      allowUnsafeResourcePrefix: true,
      createClient: () => client as never,
    });

    expect(await listen(transport)).toBeUndefined();
    expect(names[0]).toBe("alice-orders-consumer");
  });

  test("does not check existence when no prefix is set, so production start is unchanged", async () => {
    process.env.NODE_ENV = "production";
    const { client } = fakeClient(false);
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "orders-consumer",
      createClient: () => client as never,
    });

    expect(await listen(transport)).toBeUndefined();
  });
});

/**
 * Negative, zero, NaN and non-integer values used to be accepted and only surfaced later as
 * surprising SDK, timer or SQL behaviour — a flow-control limit of NaN, an ack deadline of -1. The
 * option path is named exactly, because "invalid configuration" tells you nothing when a transport
 * has a dozen numbers in it.
 */
describe("numeric option validation", () => {
  const listenTo = async (options: Record<string, unknown>) => {
    const { client } = fakeClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "s",
      createClient: () => client as never,
      ...options,
    } as never);
    return String(await listen(transport));
  };

  test.each([
    ["flowControl.maxOutstandingMessages", { flowControl: { maxOutstandingMessages: 0 } }],
    ["flowControl.maxOutstandingBytes", { flowControl: { maxOutstandingBytes: -1 } }],
    ["streaming.maxStreams", { streaming: { maxStreams: 0 } }],
    ["ackDeadline.initialMs", { ackDeadline: { initialMs: Number.NaN } }],
    ["ackDeadline.maxExtensionMs", { ackDeadline: { maxExtensionMs: -5 } }],
    ["shutdownDrainTimeoutMs", { shutdownDrainTimeoutMs: -1 }],
    ["idempotency.ttlMs", { idempotency: { ttlMs: 0 } }],
    ["schemaRegistry.cacheTtlMs", { schemaRegistry: { readerTypeFor: (): undefined => undefined, cacheTtlMs: -1 } }],
    [
      "schemaRegistry.maxCachedRevisions",
      { schemaRegistry: { readerTypeFor: (): undefined => undefined, maxCachedRevisions: 0 } },
    ],
  ])("rejects an invalid %s, naming it", async (path, options) => {
    expect(await listenTo(options)).toContain(path);
  });

  test("rejects a non-integer count rather than letting the SDK round it", async () => {
    expect(await listenTo({ flowControl: { maxOutstandingMessages: 2.5 } })).toContain("maxOutstandingMessages");
  });

  test("accepts the defaults", async () => {
    const { client } = fakeClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "s",
      createClient: () => client as never,
    });

    expect(await listen(transport)).toBeUndefined();
  });
});
