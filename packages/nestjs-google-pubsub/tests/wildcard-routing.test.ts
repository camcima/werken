import { EventEmitter } from "node:events";
import { describe, expect, test, vi } from "vitest";
import { WerkenPubSubTransport } from "@werken/nestjs-google-pubsub";
import type { IncomingMessage } from "@werken/nestjs-google-pubsub";

class FakeSubscription extends EventEmitter {
  close = vi.fn(async () => {});
}

function fakeClient() {
  const subscription = new FakeSubscription();
  return {
    subscription,
    client: {
      subscription: vi.fn(() => subscription),
      topic: vi.fn(() => ({ publishMessage: vi.fn(async () => undefined) })),
      close: vi.fn(async () => {}),
    },
  };
}

function incoming(type: string): IncomingMessage {
  return {
    id: "m1",
    attributes: {
      "ce-specversion": "1.0",
      "ce-id": "01931b7c-3f2a-7000-8000-000000000001",
      "ce-source": "https://example.test/service",
      "ce-type": type,
    },
    data: Buffer.from("{}"),
    publishTime: new Date("2026-08-03T10:00:00.000Z"),
    deliveryAttempt: 1,
    orderingKey: "",
    ack: vi.fn(),
    nack: vi.fn(),
  };
}

const listenReady = (transport: WerkenPubSubTransport) =>
  new Promise<void>((resolve, reject) => transport.listen((error?: unknown) => (error ? reject(error) : resolve())));

const settle = () => new Promise((r) => setImmediate(r));

async function deliver(patterns: Record<string, string>, type: string) {
  const { subscription, client } = fakeClient();
  const calls: string[] = [];
  const transport = new WerkenPubSubTransport({
    projectId: "p",
    subscription: "s",
    createClient: () => client as never,
  });
  for (const [pattern, name] of Object.entries(patterns)) {
    transport.addHandler(pattern, (() => void calls.push(name)) as never, true);
  }
  await listenReady(transport);

  const message = incoming(type);
  subscription.emit("message", message);
  await settle();
  await settle();

  return { calls, message, transport };
}

describe("routing through the transport", () => {
  test("delivers to a suffix wildcard handler", async () => {
    const { calls } = await deliver({ "com.example.*": "wild" }, "com.example.thing.v1");

    expect(calls).toEqual(["wild"]);
  });

  test("prefers an exact handler over a wildcard", async () => {
    const { calls } = await deliver(
      { "com.example.thing.v1": "exact", "com.example.*": "wild" },
      "com.example.thing.v1",
    );

    expect(calls).toEqual(["exact"]);
  });

  test("prefers the longest literal prefix among wildcards", async () => {
    const { calls } = await deliver({ "com.*": "short", "com.example.*": "long" }, "com.example.thing.v1");

    expect(calls).toEqual(["long"]);
  });

  test("falls back to the catch-all", async () => {
    const { calls } = await deliver({ "com.example.*": "wild", "*": "all" }, "org.other.thing");

    expect(calls).toEqual(["all"]);
  });

  // §4.5: exactly one handler runs per message.
  test("runs exactly one handler even when several patterns match", async () => {
    const { calls } = await deliver(
      { "com.example.thing.v1": "exact", "com.example.*": "wild", "*": "all" },
      "com.example.thing.v1",
    );

    expect(calls).toHaveLength(1);
  });

  test("acks an unmatched type when no catch-all is registered", async () => {
    const { calls, message } = await deliver({ "com.example.*": "wild" }, "org.other.thing");

    expect(calls).toEqual([]);
    expect(message.ack).toHaveBeenCalledTimes(1);
  });
});

describe("bootstrap failures", () => {
  // Ambiguous routing discovered in production is far worse than a startup failure.
  test("fails to start when one pattern has two handlers", async () => {
    const { client } = fakeClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "s",
      createClient: () => client as never,
    });
    transport.addHandler("com.example.thing.v1", (() => {}) as never, true);
    transport.addHandler("com.example.thing.v1", (() => {}) as never, true);

    await expect(listenReady(transport)).rejects.toThrow(/com\.example\.thing\.v1/);
  });

  test("fails to start on an unsupported wildcard position", async () => {
    const { client } = fakeClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "s",
      createClient: () => client as never,
    });
    transport.addHandler("com.*.thing", (() => {}) as never, true);

    await expect(listenReady(transport)).rejects.toThrow(/not supported/i);
  });
});
