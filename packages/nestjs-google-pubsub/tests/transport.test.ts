import { EventEmitter } from "node:events";
import { describe, expect, test, vi } from "vitest";
import { WerkenPubSubTransport } from "@werken/nestjs-google-pubsub";
import type { IncomingMessage } from "@werken/nestjs-google-pubsub";

/** listen() completes asynchronously — the callback is how Nest learns the transport is ready. */
const listenReady = (transport: WerkenPubSubTransport) =>
  new Promise<void>((resolve, reject) => transport.listen((error?: unknown) => (error ? reject(error) : resolve())));

const TYPE = "com.example.thing.happened.v1";

/** Stands in for a Pub/Sub Subscription: an EventEmitter with open/close semantics. */
class FakeSubscription extends EventEmitter {
  closed = false;
  close = vi.fn(async () => {
    this.closed = true;
  });
}

function fakeClient() {
  const subscription = new FakeSubscription();
  return {
    subscription,
    client: {
      subscription: vi.fn(() => subscription),
      close: vi.fn(async () => {}),
    },
  };
}

function incoming(overrides: Partial<IncomingMessage> = {}): IncomingMessage {
  return {
    id: "pubsub-message-1",
    attributes: {
      "ce-specversion": "1.0",
      "ce-id": "01931b7c-3f2a-7000-8000-000000000001",
      "ce-source": "https://example.test/service",
      "ce-type": TYPE,
    },
    data: Buffer.from(JSON.stringify({ hello: "world" })),
    publishTime: new Date("2026-08-02T15:00:00.000Z"),
    deliveryAttempt: 0,
    orderingKey: "",
    ack: vi.fn(),
    nack: vi.fn(),
    ...overrides,
  };
}

function transportWith(client: unknown) {
  return new WerkenPubSubTransport({
    projectId: "p",
    subscription: "s",
    createClient: () => client as never,
  });
}

/** Waits for the transport's async message handling to settle. */
const settle = () => new Promise((r) => setImmediate(r));

describe("lifecycle", () => {
  test("listen opens the configured subscription and signals readiness", async () => {
    const { client } = fakeClient();
    const transport = transportWith(client);
    const ready = vi.fn();

    await new Promise<void>((resolve) =>
      transport.listen((...args: unknown[]) => {
        ready(...args);
        resolve();
      }),
    );

    expect(client.subscription).toHaveBeenCalledWith("s", expect.anything());
    expect(ready).toHaveBeenCalledTimes(1);
  });

  test("close stops the subscription and the client", async () => {
    const { subscription, client } = fakeClient();
    const transport = transportWith(client);

    await listenReady(transport);
    await transport.close();

    expect(subscription.close).toHaveBeenCalled();
    expect(client.close).toHaveBeenCalled();
  });

  test("unwrap returns the underlying subscription", async () => {
    const { subscription, client } = fakeClient();
    const transport = transportWith(client);

    await listenReady(transport);

    expect(transport.unwrap()).toBe(subscription);
  });

  test("unwrap throws before listen rather than returning undefined", () => {
    const { client } = fakeClient();

    expect(() => transportWith(client).unwrap()).toThrow(/not listening/);
  });

  // Fail loudly at startup. Nest surfaces this through the listen callback, so an
  // error here must reach it rather than escaping as an unhandled throw.
  test("reports a client construction failure through the listen callback", async () => {
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "s",
      createClient: () => {
        throw new Error("bad credentials");
      },
    });

    const error = await new Promise<unknown>((resolve) => transport.listen((e?: unknown) => resolve(e)));

    expect(error).toBeInstanceOf(Error);
    expect(String(error)).toMatch(/bad credentials/);
  });

  test("isHealthy reflects whether the subscription is open", async () => {
    const { client } = fakeClient();
    const transport = transportWith(client);

    expect(transport.isHealthy()).toBe(false);
    await listenReady(transport);
    expect(transport.isHealthy()).toBe(true);

    await transport.close();
    expect(transport.isHealthy()).toBe(false);
  });
});

describe("message handling", () => {
  test("routes a message to the handler registered for its ce-type and acks", async () => {
    const { subscription, client } = fakeClient();
    const transport = transportWith(client);
    const handler = vi.fn();
    transport.addHandler(TYPE, handler as never, true);

    await listenReady(transport);

    const message = incoming();
    subscription.emit("message", message);
    await settle();

    expect(handler).toHaveBeenCalledTimes(1);
    expect(message.ack).toHaveBeenCalledTimes(1);
    expect(message.nack).not.toHaveBeenCalled();
  });

  test("nacks when the handler throws", async () => {
    const { subscription, client } = fakeClient();
    const transport = transportWith(client);
    transport.addHandler(
      TYPE,
      (() => {
        throw new Error("transient");
      }) as never,
      true,
    );

    await listenReady(transport);

    const message = incoming();
    subscription.emit("message", message);
    await settle();

    expect(message.nack).toHaveBeenCalledTimes(1);
    expect(message.ack).not.toHaveBeenCalled();
  });

  test("acks a message no handler matches", async () => {
    const { subscription, client } = fakeClient();
    const transport = transportWith(client);

    await listenReady(transport);

    const message = incoming();
    subscription.emit("message", message);
    await settle();

    expect(message.ack).toHaveBeenCalledTimes(1);
  });
});

describe("on()", () => {
  test("forwards subscription errors to a registered listener", async () => {
    const { subscription, client } = fakeClient();
    const transport = transportWith(client);
    const onError = vi.fn();
    transport.on("error", onError);

    await listenReady(transport);

    const boom = new Error("stream broke");
    subscription.emit("error", boom);
    await settle();

    expect(onError).toHaveBeenCalledWith(boom);
  });
});

describe("idempotency wiring", () => {
  // The SQL store is well covered on its own; what is easy to get wrong is the transport option
  // that builds it, since a store that is never consulted looks exactly like one that finds no
  // duplicates.
  test("builds the SQL store from an executor and consults it per message", async () => {
    const statements: string[] = [];
    const executor = {
      execute: async (sql: string) => {
        statements.push(sql);
        return { rowCount: sql.startsWith("SELECT") ? 0 : 1 };
      },
    };
    const { subscription, client } = fakeClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "s",
      idempotency: { consumer: "orders", executor: () => executor },
      createClient: () => client as never,
    });

    const handler = vi.fn();
    transport.addHandler(TYPE, handler as never, true);
    await listenReady(transport);

    subscription.emit("message", incoming());
    await new Promise((r) => setImmediate(r));

    expect(handler).toHaveBeenCalledTimes(1);
    expect(statements.some((s) => s.startsWith("SELECT"))).toBe(true);
    expect(statements.some((s) => s.startsWith("INSERT"))).toBe(true);

    await transport.close();
  });

  test("refuses a store and an executor together", () => {
    const { client } = fakeClient();

    expect(
      () =>
        new WerkenPubSubTransport({
          projectId: "p",
          subscription: "s",
          idempotency: {
            consumer: "orders",
            executor: () => ({ execute: async () => ({ rowCount: 0 }) }),
            store: { has: async () => false, tryRecord: async () => true },
          },
          createClient: () => client as never,
        }),
    ).toThrow(/mutually exclusive/);
  });
});
