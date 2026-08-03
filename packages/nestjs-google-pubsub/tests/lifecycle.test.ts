import { EventEmitter } from "node:events";
import { describe, expect, test, vi } from "vitest";
import { WerkenPubSubTransport, toSubscriberOptions } from "@werken/nestjs-google-pubsub";
import type { IncomingMessage } from "@werken/nestjs-google-pubsub";

/** listen() completes asynchronously — the callback is how Nest learns the transport is ready. */
const listenReady = (transport: WerkenPubSubTransport) =>
  new Promise<void>((resolve, reject) => transport.listen((error?: unknown) => (error ? reject(error) : resolve())));

const TYPE = "com.example.thing.happened.v1";

class FakeSubscription extends EventEmitter {
  closed = false;
  close = vi.fn(async () => {
    this.closed = true;
  });
}

function fakeClient() {
  const subscription = new FakeSubscription();
  const subscriptionOptions: unknown[] = [];
  return {
    subscription,
    subscriptionOptions,
    client: {
      subscription: vi.fn((_name: string, options?: unknown) => {
        subscriptionOptions.push(options);
        return subscription;
      }),
      topic: vi.fn(() => ({ publishMessage: vi.fn(async () => undefined) })),
      close: vi.fn(async () => {}),
    },
  };
}

function incoming(id = "m1"): IncomingMessage {
  return {
    id,
    attributes: {
      "ce-specversion": "1.0",
      "ce-id": `01931b7c-3f2a-7000-8000-00000000000${id.slice(-1)}`,
      "ce-source": "https://example.test/service",
      "ce-type": TYPE,
    },
    data: Buffer.from(JSON.stringify({ hello: "world" })),
    publishTime: new Date("2026-08-02T15:00:00.000Z"),
    deliveryAttempt: 0,
    orderingKey: "",
    ack: vi.fn(),
    nack: vi.fn(),
  };
}

const settle = () => new Promise((r) => setImmediate(r));

// §4.1 names these after the broker-neutral Pub/Sub concepts, but the Node SDK's own
// FlowControlOptions uses maxMessages/maxBytes. Passing our names straight through means the SDK
// silently ignores them and applies its own defaults instead — flow control that looks configured
// and is not.
describe("toSubscriberOptions", () => {
  test("maps our flow-control names onto the SDK's", () => {
    const options = toSubscriberOptions({
      projectId: "p",
      subscription: "s",
      flowControl: { maxOutstandingMessages: 7, maxOutstandingBytes: 1234, allowExcessMessages: true },
    });

    expect(options.flowControl).toEqual({ maxMessages: 7, maxBytes: 1234, allowExcessMessages: true });
  });

  test("applies the documented defaults", () => {
    const options = toSubscriberOptions({ projectId: "p", subscription: "s" });

    expect(options.flowControl).toEqual({
      maxMessages: 50,
      maxBytes: 20 * 1024 * 1024,
      allowExcessMessages: false,
    });
  });

  test("never emits the SDK-unknown names", () => {
    const options = toSubscriberOptions({ projectId: "p", subscription: "s" });

    expect(options.flowControl).not.toHaveProperty("maxOutstandingMessages");
    expect(options.flowControl).not.toHaveProperty("maxOutstandingBytes");
  });

  test("defaults maxStreams to 1, since raising it interacts with ordering", () => {
    expect(toSubscriberOptions({ projectId: "p", subscription: "s" }).streamingOptions).toEqual({ maxStreams: 1 });
  });

  test("carries ack deadlines through as plain milliseconds", () => {
    const options = toSubscriberOptions({
      projectId: "p",
      subscription: "s",
      ackDeadline: { initialMs: 45_000, maxExtensionMs: 300_000 },
    });

    expect(options.minAckDeadlineMs).toBe(45_000);
    expect(options.maxExtensionTimeMs).toBe(300_000);
  });

  test("applies the documented ack deadline defaults", () => {
    const options = toSubscriberOptions({ projectId: "p", subscription: "s" });

    expect(options.minAckDeadlineMs).toBe(60_000);
    expect(options.maxExtensionTimeMs).toBe(600_000);
  });

  // Regression: a hand-rolled duration duck-type passed unit tests but crashed the real SDK inside
  // subscription.close() with "first.total is not a function". Only the SDK's Duration will do.
  test("hands the SDK real Duration instances, not look-alikes", async () => {
    const { Duration } = await import("@google-cloud/pubsub");
    const { subscription, client, subscriptionOptions } = fakeClient();
    void subscription;
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "s",
      ackDeadline: { initialMs: 45_000, maxExtensionMs: 300_000 },
      createClient: () => client as never,
    });

    await listenReady(transport);

    const passed = subscriptionOptions[0] as { minAckDeadline: unknown; maxExtensionTime: unknown };
    expect(passed.minAckDeadline).toBeInstanceOf(Duration);
    expect(passed.maxExtensionTime).toBeInstanceOf(Duration);
    expect((passed.minAckDeadline as InstanceType<typeof Duration>).total("millisecond")).toBe(45_000);
  });
});

describe("drain on shutdown", () => {
  test("waits for an in-flight handler before closing", async () => {
    const { subscription, client } = fakeClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "s",
      createClient: () => client as never,
    });

    let finished = false;
    let release: (() => void) | undefined;
    transport.addHandler(
      TYPE,
      (async () => {
        await new Promise<void>((r) => (release = r));
        finished = true;
      }) as never,
      true,
    );

    await listenReady(transport);

    const message = incoming();
    subscription.emit("message", message);
    await settle();

    const closing = transport.close();
    await settle();

    expect(finished).toBe(false);
    expect(subscription.close).not.toHaveBeenCalled();

    release!();
    await closing;

    expect(finished).toBe(true);
    expect(message.ack).toHaveBeenCalledTimes(1);
    expect(subscription.close).toHaveBeenCalled();
  });

  test("nacks work still in flight when the drain times out", async () => {
    const { subscription, client } = fakeClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "s",
      shutdownDrainTimeoutMs: 20,
      createClient: () => client as never,
    });

    transport.addHandler(TYPE, (() => new Promise(() => {})) as never, true);
    await listenReady(transport);

    const message = incoming();
    subscription.emit("message", message);
    await settle();

    await transport.close();

    // Fast redelivery beats letting the ack deadline lapse silently.
    expect(message.nack).toHaveBeenCalledTimes(1);
    expect(message.ack).not.toHaveBeenCalled();
  });

  test("never acks work that did not complete", async () => {
    const { subscription, client } = fakeClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "s",
      shutdownDrainTimeoutMs: 20,
      createClient: () => client as never,
    });

    transport.addHandler(TYPE, (() => new Promise(() => {})) as never, true);
    await listenReady(transport);

    const messages = [incoming("m1"), incoming("m2"), incoming("m3")];
    for (const m of messages) subscription.emit("message", m);
    await settle();

    await transport.close();

    for (const m of messages) {
      expect(m.ack).not.toHaveBeenCalled();
      expect(m.nack).toHaveBeenCalledTimes(1);
    }
  });

  test("stops handling messages that arrive after close begins", async () => {
    const { subscription, client } = fakeClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "s",
      createClient: () => client as never,
    });
    const handler = vi.fn();
    transport.addHandler(TYPE, handler as never, true);

    await listenReady(transport);
    await transport.close();

    subscription.emit("message", incoming());
    await settle();

    expect(handler).not.toHaveBeenCalled();
  });

  test("reports a drain summary", async () => {
    const { subscription, client } = fakeClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "s",
      shutdownDrainTimeoutMs: 20,
      createClient: () => client as never,
    });
    const logs: string[] = [];
    vi.spyOn(transport["logger"], "log").mockImplementation((m: unknown) => void logs.push(String(m)));

    transport.addHandler(TYPE, (() => new Promise(() => {})) as never, true);
    await listenReady(transport);
    subscription.emit("message", incoming());
    await settle();

    await transport.close();

    expect(logs.join("\n")).toMatch(/drain/i);
    expect(logs.join("\n")).toMatch(/nacked=1/);
  });

  test("is safe to call twice", async () => {
    const { client } = fakeClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "s",
      createClient: () => client as never,
    });

    await listenReady(transport);

    await transport.close();
    await expect(transport.close()).resolves.toBeUndefined();
  });

  test("does not settle a message the drain already nacked", async () => {
    const { subscription, client } = fakeClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "s",
      shutdownDrainTimeoutMs: 20,
      createClient: () => client as never,
    });

    let release: (() => void) | undefined;
    transport.addHandler(
      TYPE,
      (async () => {
        await new Promise<void>((r) => (release = r));
      }) as never,
      true,
    );
    await listenReady(transport);

    const message = incoming();
    subscription.emit("message", message);
    await settle();

    await transport.close();
    expect(message.nack).toHaveBeenCalledTimes(1);

    // The handler finishes after the drain gave up on it. The message was already nacked and is on
    // its way back, so acking now would race the redelivery — and settling a message twice on a
    // closed subscription throws, which out of a fire-and-forget listener kills the process.
    release!();
    await settle();

    expect(message.ack).not.toHaveBeenCalled();
    expect(message.nack).toHaveBeenCalledTimes(1);
  });

  test("logs rather than crashing when settling a message throws", async () => {
    const { subscription, client } = fakeClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "s",
      createClient: () => client as never,
    });
    const errors: string[] = [];
    vi.spyOn(transport["logger"], "error").mockImplementation((m: unknown) => void errors.push(String(m)));

    transport.addHandler(TYPE, (() => {}) as never, true);
    await listenReady(transport);

    const message = incoming();
    message.ack = vi.fn(() => {
      throw new Error("subscriber already closed");
    });
    subscription.emit("message", message);
    await settle();

    expect(errors.join("\n")).toMatch(/ack/i);
  });

  test("reports unhealthy once closed", async () => {
    const { client } = fakeClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "s",
      createClient: () => client as never,
    });

    await listenReady(transport);
    expect(transport.isHealthy()).toBe(true);

    await transport.close();
    expect(transport.isHealthy()).toBe(false);
  });
});

describe("stream errors", () => {
  test("keeps its error listener attached until the subscription is closed", async () => {
    const { subscription, client } = fakeClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "s",
      createClient: () => client as never,
    });
    const seen: unknown[] = [];
    transport.on("error", (error) => void seen.push(error));

    await listenReady(transport);

    let releaseClose: (() => void) | undefined;
    subscription.close = vi.fn(() => new Promise<void>((r) => (releaseClose = r)));

    const closing = transport.close();
    await settle();

    // An EventEmitter throws when it emits 'error' with nothing listening, so dropping the listener
    // before the stream is closed turns a broker hiccup during shutdown into a crash.
    expect(() => subscription.emit("error", new Error("stream broke"))).not.toThrow();

    releaseClose!();
    await closing;

    expect(seen).toHaveLength(1);
  });

  test("logs subscription errors even when nothing registered a listener", async () => {
    const { subscription, client } = fakeClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "s",
      createClient: () => client as never,
    });
    const errors: string[] = [];
    vi.spyOn(transport["logger"], "error").mockImplementation((m: unknown) => void errors.push(String(m)));

    await listenReady(transport);
    subscription.emit("error", new Error("stream broke"));

    expect(errors.join("\n")).toMatch(/stream broke/);
  });
});
