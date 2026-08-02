import { EventEmitter } from "node:events";
import { describe, expect, test, vi } from "vitest";
import {
  DEAD_LETTER_ATTRIBUTES,
  PubSubDeadLetterPublisher,
  TerminalEventError,
  WerkenPubSubTransport,
} from "@werken/nestjs-google-pubsub";
import type { IncomingMessage } from "@werken/nestjs-google-pubsub";

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

    transport.listen(() => {});
    await settle();

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

    transport.listen(() => {});
    await settle();

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

    transport.listen(() => {});
    await settle();

    const message = incoming({ attributes: { "ce-specversion": "1.0" } });
    subscription.emit("message", message);
    await settle();
    await settle();

    expect(published).toHaveLength(0);
    expect(message.nack).toHaveBeenCalledTimes(1);
    expect(message.ack).not.toHaveBeenCalled();
  });
});
