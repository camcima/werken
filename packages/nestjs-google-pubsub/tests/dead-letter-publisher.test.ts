import { EventEmitter } from "node:events";
import { describe, expect, test, vi } from "vitest";
import {
  DEAD_LETTER_ATTRIBUTES,
  PubSubDeadLetterPublisher,
  TerminalEventError,
  WerkenPubSubTransport,
} from "@werken/nestjs-google-pubsub";
import type { IncomingMessage } from "@werken/nestjs-google-pubsub";

/** listen() completes asynchronously — the callback is how Nest learns the transport is ready. */
const listenReady = (transport: WerkenPubSubTransport) =>
  new Promise<void>((resolve, reject) => transport.listen((error?: unknown) => (error ? reject(error) : resolve())));

const TYPE = "com.example.thing.happened.v1";

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

function fakeTopic() {
  const published: Array<{ data: Buffer; attributes: Record<string, string> }> = [];
  return {
    published,
    topic: {
      publishMessage: vi.fn(async (m: { data: Buffer; attributes: Record<string, string> }) => void published.push(m)),
    },
  };
}

describe("PubSubDeadLetterPublisher", () => {
  test("publishes the original body to the configured topic", async () => {
    const { topic, published } = fakeTopic();
    const client = { topic: vi.fn(() => topic), subscription: vi.fn(), close: vi.fn() };
    const publisher = new PubSubDeadLetterPublisher(client as never, "dead-letters");

    const message = incoming();
    await publisher.publish({
      message,
      reason: "unknown reference",
      stage: "handler",
      subscription: "projects/p/subscriptions/s",
      timestamp: new Date("2026-08-02T16:00:00.000Z"),
    });

    expect(client.topic).toHaveBeenCalledWith("dead-letters");
    expect(published).toHaveLength(1);
    expect(published[0].data.toString()).toBe(JSON.stringify({ hello: "world" }));
  });

  test("preserves the original attributes and adds provenance", async () => {
    const { topic, published } = fakeTopic();
    const client = { topic: vi.fn(() => topic), subscription: vi.fn(), close: vi.fn() };
    const publisher = new PubSubDeadLetterPublisher(client as never, "dead-letters");

    await publisher.publish({
      message: incoming(),
      reason: "unknown reference",
      stage: "handler",
      subscription: "projects/p/subscriptions/s",
      timestamp: new Date("2026-08-02T16:00:00.000Z"),
    });

    const attributes = published[0].attributes;
    expect(attributes["ce-type"]).toBe(TYPE);
    expect(attributes[DEAD_LETTER_ATTRIBUTES.reason]).toBe("unknown reference");
    expect(attributes[DEAD_LETTER_ATTRIBUTES.stage]).toBe("handler");
    expect(attributes[DEAD_LETTER_ATTRIBUTES.sourceSubscription]).toBe("projects/p/subscriptions/s");
    expect(attributes[DEAD_LETTER_ATTRIBUTES.timestamp]).toBe("2026-08-02T16:00:00.000Z");
  });
});

describe("transport integration with dead-lettering", () => {
  class FakeSubscription extends EventEmitter {
    close = vi.fn(async () => {});
  }

  function harnessClient() {
    const subscription = new FakeSubscription();
    const { topic, published } = fakeTopic();
    return {
      subscription,
      published,
      client: {
        subscription: vi.fn(() => subscription),
        topic: vi.fn(() => topic),
        close: vi.fn(async () => {}),
      },
    };
  }

  const settle = () => new Promise((r) => setImmediate(r));

  test("acks the original message once it has been dead-lettered", async () => {
    const { subscription, client, published } = harnessClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "s",
      deadLetterTopic: "dead-letters",
      createClient: () => client as never,
    });
    transport.addHandler(
      TYPE,
      (() => {
        throw new TerminalEventError("terminal");
      }) as never,
      true,
    );

    await listenReady(transport);

    const message = incoming();
    subscription.emit("message", message);
    await settle();
    await settle();

    expect(published).toHaveLength(1);
    // Original is acked only after the dead-letter publish succeeds.
    expect(message.ack).toHaveBeenCalledTimes(1);
    expect(message.nack).not.toHaveBeenCalled();
  });

  test("dead-letters an invalid envelope by default when a topic is configured", async () => {
    const { subscription, client, published } = harnessClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "s",
      deadLetterTopic: "dead-letters",
      createClient: () => client as never,
    });

    await listenReady(transport);

    const message = incoming({ attributes: { "ce-specversion": "1.0" } });
    subscription.emit("message", message);
    await settle();
    await settle();

    expect(published).toHaveLength(1);
    expect(published[0].attributes[DEAD_LETTER_ATTRIBUTES.stage]).toBe("envelope");
    expect(message.ack).toHaveBeenCalledTimes(1);
  });

  test("nacks an invalid envelope when no dead-letter topic is configured", async () => {
    const { subscription, client, published } = harnessClient();
    const transport = new WerkenPubSubTransport({
      projectId: "p",
      subscription: "s",
      createClient: () => client as never,
    });

    await listenReady(transport);

    const message = incoming({ attributes: { "ce-specversion": "1.0" } });
    subscription.emit("message", message);
    await settle();
    await settle();

    expect(published).toHaveLength(0);
    expect(message.nack).toHaveBeenCalledTimes(1);
    expect(message.ack).not.toHaveBeenCalled();
  });
});

/**
 * TerminalEventError accepts structured detail and the pipeline carries it into DeadLetterRequest,
 * but the production publisher dropped it — so the diagnostic context a handler deliberately
 * attached survived only for a custom publisher, never for the real one.
 */
describe("structured detail and ordering provenance", () => {
  const publish = async (request: Partial<Parameters<PubSubDeadLetterPublisher["publish"]>[0]> = {}) => {
    const { published, topic } = fakeTopic();
    const publisher = new PubSubDeadLetterPublisher({ topic: () => topic } as never, "dead-letters");
    await publisher.publish({
      message: incoming(),
      reason: "unknown shipment",
      stage: "handler",
      subscription: "orders-consumer",
      timestamp: new Date("2026-08-03T10:00:00.000Z"),
      ...request,
    } as never);
    return published[0];
  };

  test("serialises structured detail as a JSON provenance attribute", async () => {
    const sent = await publish({ detail: { shipmentId: "known-1", attempts: 3 } });

    expect(JSON.parse(sent.attributes[DEAD_LETTER_ATTRIBUTES.detail])).toEqual({
      shipmentId: "known-1",
      attempts: 3,
    });
  });

  test("adds no detail attribute when the handler attached none", async () => {
    const sent = await publish();

    expect(sent.attributes).not.toHaveProperty(DEAD_LETTER_ATTRIBUTES.detail);
  });

  // Pub/Sub caps an attribute value at 1024 bytes. Truncating JSON mid-string yields something
  // nothing can parse, so oversized detail is replaced by a marker naming its real size — the
  // operator learns detail existed and why it is not here.
  test("replaces oversized detail with a marker rather than unparseable JSON", async () => {
    const sent = await publish({ detail: { blob: "x".repeat(4000) } });

    const marker = JSON.parse(sent.attributes[DEAD_LETTER_ATTRIBUTES.detail]) as Record<string, unknown>;
    expect(marker.truncated).toBe(true);
    expect(marker.bytes).toBeGreaterThan(1024);
  });

  test("never fails the publish over unserialisable detail", async () => {
    const circular: Record<string, unknown> = {};
    circular.self = circular;

    const sent = await publish({ detail: circular });

    expect(JSON.parse(sent.attributes[DEAD_LETTER_ATTRIBUTES.detail])).toMatchObject({ unserialisable: true });
  });

  // Redrive tooling cannot restore per-entity ordering without knowing the original key. Carried as
  // provenance, not as a live ordering key: republishing with one would need the dead-letter topic
  // built for ordering, and would serialise dead-letter publishes per key.
  test("preserves the original ordering key as provenance", async () => {
    const sent = await publish({ message: incoming({ orderingKey: "shipment-42" }) });

    expect(sent.attributes[DEAD_LETTER_ATTRIBUTES.orderingKey]).toBe("shipment-42");
  });

  test("omits the ordering key attribute when the original had none", async () => {
    const sent = await publish();

    expect(sent.attributes).not.toHaveProperty(DEAD_LETTER_ATTRIBUTES.orderingKey);
  });
});

/**
 * Pub/Sub's per-message limits apply to the dead-letter publish like any other, and this is the one
 * publish that must not fail: the pipeline maps a failed dead-letter publish to a nack, so an
 * oversized provenance attribute would redeliver the poison message into the same failure forever.
 * The emulator enforces none of these, so only a unit test can hold the line.
 */
describe("Pub/Sub attribute limits", () => {
  const publishWith = async (request: Partial<Parameters<PubSubDeadLetterPublisher["publish"]>[0]>) => {
    const { published, topic } = fakeTopic();
    const publisher = new PubSubDeadLetterPublisher({ topic: () => topic } as never, "dead-letters");
    await publisher.publish({
      message: incoming(),
      reason: "unknown shipment",
      stage: "handler",
      subscription: "orders-consumer",
      timestamp: new Date("2026-08-03T10:00:00.000Z"),
      ...request,
    } as never);
    return published[0];
  };

  // An envelope validation error interpolates the offending attribute value, which can itself be a
  // full 1024 bytes, and a TerminalEventError reason is whatever the application passed.
  test("caps an oversized reason at the 1024-byte attribute limit", async () => {
    const sent = await publishWith({ reason: `unknown shipment ${"x".repeat(4000)}` });

    const reason = sent.attributes[DEAD_LETTER_ATTRIBUTES.reason];
    expect(Buffer.byteLength(reason, "utf8")).toBeLessThanOrEqual(1024);
    // The head survives, so the operator still reads what went wrong.
    expect(reason.startsWith("unknown shipment xxx")).toBe(true);
    expect(reason).toMatch(/truncated/);
  });

  test("leaves a reason that already fits untouched", async () => {
    const sent = await publishWith({ reason: "unknown shipment known-1" });

    expect(sent.attributes[DEAD_LETTER_ATTRIBUTES.reason]).toBe("unknown shipment known-1");
  });

  // Truncating mid-sequence yields U+FFFD, and mid-surrogate-pair a lone surrogate — neither is
  // something an operator should have to see in a diagnostic.
  test("truncates on a character boundary, not a byte boundary", async () => {
    const sent = await publishWith({ reason: "🚚".repeat(1000) });

    const reason = sent.attributes[DEAD_LETTER_ATTRIBUTES.reason];
    expect(Buffer.byteLength(reason, "utf8")).toBeLessThanOrEqual(1024);
    expect(reason).not.toMatch(/�/);
    expect(reason.match(/\p{Surrogate}/u)).toBeNull();
  });

  // Pub/Sub allows at most 100 attributes. The original's are forwarded verbatim, so a message that
  // was itself at the limit would push the publish over it once provenance is added.
  test("stays within the 100-attribute limit when the original is already at it", async () => {
    const crowded = Object.fromEntries(Array.from({ length: 100 }, (_, i) => [`attr-${i}`, String(i)]));
    const sent = await publishWith({
      message: incoming({ attributes: { ...crowded, "ce-type": TYPE, "ce-id": "id-1" } }),
    });

    expect(Object.keys(sent.attributes).length).toBeLessThanOrEqual(100);
  });

  // Provenance is why the message is on the topic at all, and the envelope is what redrive needs to
  // republish it. Anything else is the first to go, and the drop is reported rather than silent.
  test("keeps provenance and the envelope when it has to drop attributes", async () => {
    const crowded = Object.fromEntries(Array.from({ length: 100 }, (_, i) => [`attr-${i}`, String(i)]));
    const sent = await publishWith({
      message: incoming({ attributes: { ...crowded, "ce-type": TYPE, "ce-id": "id-1" } }),
      detail: { shipmentId: "known-1" },
    });

    expect(sent.attributes[DEAD_LETTER_ATTRIBUTES.reason]).toBe("unknown shipment");
    expect(sent.attributes[DEAD_LETTER_ATTRIBUTES.stage]).toBe("handler");
    expect(sent.attributes[DEAD_LETTER_ATTRIBUTES.detail]).toBeDefined();
    expect(sent.attributes["ce-type"]).toBe(TYPE);
    expect(sent.attributes["ce-id"]).toBe("id-1");
    expect(Number(sent.attributes[DEAD_LETTER_ATTRIBUTES.droppedAttributes])).toBeGreaterThan(0);
  });
});
