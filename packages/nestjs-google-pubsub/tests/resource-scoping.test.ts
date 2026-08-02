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
