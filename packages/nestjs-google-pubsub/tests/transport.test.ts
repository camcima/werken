import { EventEmitter } from "node:events";
import { describe, expect, test, vi } from "vitest";
import { WerkenPubSubTransport } from "@werken/nestjs-google-pubsub";
import type { IncomingMessage } from "@werken/nestjs-google-pubsub";

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

    transport.listen(ready);
    await settle();

    expect(client.subscription).toHaveBeenCalledWith("s", expect.anything());
    expect(ready).toHaveBeenCalledTimes(1);
  });

  test("close stops the subscription and the client", async () => {
    const { subscription, client } = fakeClient();
    const transport = transportWith(client);

    transport.listen(() => {});
    await settle();
    await transport.close();

    expect(subscription.close).toHaveBeenCalled();
    expect(client.close).toHaveBeenCalled();
  });

  test("unwrap returns the underlying subscription", async () => {
    const { subscription, client } = fakeClient();
    const transport = transportWith(client);

    transport.listen(() => {});
    await settle();

    expect(transport.unwrap()).toBe(subscription);
  });

  test("unwrap throws before listen rather than returning undefined", () => {
    const { client } = fakeClient();

    expect(() => transportWith(client).unwrap()).toThrow(/not listening/);
  });

  // §Appendix B: fail loudly at startup. Nest surfaces this through the listen callback, so an
  // error here must reach it rather than escaping as an unhandled throw.
  test("reports a client construction failure through the listen callback", () => {
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "s",
      createClient: () => {
        throw new Error("bad credentials");
      },
    });
    const callback = vi.fn();

    transport.listen(callback);

    expect(callback).toHaveBeenCalledTimes(1);
    expect(callback.mock.calls[0][0]).toBeInstanceOf(Error);
  });

  test("isHealthy reflects whether the subscription is open", async () => {
    const { client } = fakeClient();
    const transport = transportWith(client);

    expect(transport.isHealthy()).toBe(false);
    transport.listen(() => {});
    await settle();
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

    transport.listen(() => {});
    await settle();

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

    transport.listen(() => {});
    await settle();

    const message = incoming();
    subscription.emit("message", message);
    await settle();

    expect(message.nack).toHaveBeenCalledTimes(1);
    expect(message.ack).not.toHaveBeenCalled();
  });

  test("acks a message no handler matches", async () => {
    const { subscription, client } = fakeClient();
    const transport = transportWith(client);

    transport.listen(() => {});
    await settle();

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

    transport.listen(() => {});
    await settle();

    const boom = new Error("stream broke");
    subscription.emit("error", boom);
    await settle();

    expect(onError).toHaveBeenCalledWith(boom);
  });
});
