import { describe, expect, test, vi } from "vitest";
import { InMemoryIdempotencyStore, MessagePipeline, TerminalEventError } from "@werken/nestjs-google-pubsub";
import type { CloudEventContext, IdempotencyStore, IncomingMessage } from "@werken/nestjs-google-pubsub";

const TYPE = "com.example.thing.happened.v1";
const CONSUMER = "baggage-reconciliation";

function message(overrides: Partial<IncomingMessage> = {}): IncomingMessage {
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
    ack: () => {},
    nack: () => {},
    ...overrides,
  };
}

function pipelineWith(
  handlers: Record<string, (data: unknown, ctx: CloudEventContext) => unknown>,
  extra: Partial<ConstructorParameters<typeof MessagePipeline>[0]> = {},
) {
  return new MessagePipeline({
    subscription: "projects/p/subscriptions/s",
    resolveHandler: (pattern) => handlers[pattern] ?? null,
    consumer: CONSUMER,
    ...extra,
  });
}

describe("duplicate suppression", () => {
  test("processes a message the first time", async () => {
    const handler = vi.fn();
    const outcome = await pipelineWith(
      { [TYPE]: handler },
      { idempotencyStore: new InMemoryIdempotencyStore() },
    ).handle(message());

    expect(handler).toHaveBeenCalledTimes(1);
    expect(outcome).toBe("ack");
  });

  test("acks a redelivery without invoking the handler again", async () => {
    const handler = vi.fn();
    const store = new InMemoryIdempotencyStore();
    const pipeline = pipelineWith({ [TYPE]: handler }, { idempotencyStore: store });

    await pipeline.handle(message());
    const outcome = await pipeline.handle(message());

    expect(handler).toHaveBeenCalledTimes(1);
    expect(outcome).toBe("ack");
  });

  test("treats a different event id as a new message", async () => {
    const handler = vi.fn();
    const store = new InMemoryIdempotencyStore();
    const pipeline = pipelineWith({ [TYPE]: handler }, { idempotencyStore: store });

    await pipeline.handle(message());
    await pipeline.handle(
      message({ attributes: { ...message().attributes, "ce-id": "01931b7c-3f2a-7000-8000-000000000002" } }),
    );

    expect(handler).toHaveBeenCalledTimes(2);
  });
});

describe("§5.2 ordering", () => {
  // A duplicate must not pay the decode cost, and a message already processed successfully must
  // still be acked even if its schema has since become unreadable.
  test("checks idempotency before decoding", async () => {
    const store = new InMemoryIdempotencyStore();
    await store.tryRecord(
      { consumer: CONSUMER, source: "https://example.test/service", id: "01931b7c-3f2a-7000-8000-000000000001" },
      60_000,
    );

    const decode = vi.fn();
    const outcome = await pipelineWith({ [TYPE]: () => {} }, { idempotencyStore: store, codec: { decode } }).handle(
      message(),
    );

    expect(decode).not.toHaveBeenCalled();
    expect(outcome).toBe("ack");
  });

  test("acks an already-processed message whose body is now undecodable", async () => {
    const store = new InMemoryIdempotencyStore();
    await store.tryRecord(
      { consumer: CONSUMER, source: "https://example.test/service", id: "01931b7c-3f2a-7000-8000-000000000001" },
      60_000,
    );

    const outcome = await pipelineWith({ [TYPE]: () => {} }, { idempotencyStore: store }).handle(
      message({ data: Buffer.from("{not json") }),
    );

    expect(outcome).toBe("ack");
  });

  // Recording before the handler risks silently dropping a message that then failed.
  test("does not record when the handler throws", async () => {
    const store = new InMemoryIdempotencyStore();
    const handler = vi.fn(() => {
      throw new Error("transient");
    });
    const pipeline = pipelineWith({ [TYPE]: handler }, { idempotencyStore: store });

    expect(await pipeline.handle(message())).toBe("nack");
    expect(await pipeline.handle(message())).toBe("nack");
    expect(handler).toHaveBeenCalledTimes(2);
  });

  test("records a dead-lettered message so it is not reprocessed on redelivery", async () => {
    const store = new InMemoryIdempotencyStore();
    const handler = vi.fn(() => {
      throw new TerminalEventError("will never resolve");
    });
    const pipeline = pipelineWith(
      { [TYPE]: handler },
      { idempotencyStore: store, deadLetterPublisher: { publish: async () => {} } },
    );

    expect(await pipeline.handle(message())).toBe("dead-letter");
    expect(await pipeline.handle(message())).toBe("ack");
    expect(handler).toHaveBeenCalledTimes(1);
  });
});

describe("store failures", () => {
  test("nacks rather than risking a duplicate when the store cannot be read", async () => {
    const store: IdempotencyStore = {
      has: async () => {
        throw new Error("database down");
      },
      tryRecord: async () => true,
    };
    const handler = vi.fn();

    const outcome = await pipelineWith({ [TYPE]: handler }, { idempotencyStore: store }).handle(message());

    expect(outcome).toBe("nack");
    expect(handler).not.toHaveBeenCalled();
  });

  test("still acks when recording fails after a successful handler", async () => {
    // The side effect already happened. Nacking would guarantee a duplicate; acking risks one only
    // if the store write was genuinely lost.
    const store: IdempotencyStore = {
      has: async () => false,
      tryRecord: async () => {
        throw new Error("database down");
      },
    };
    const handler = vi.fn();

    const outcome = await pipelineWith({ [TYPE]: handler }, { idempotencyStore: store }).handle(message());

    expect(handler).toHaveBeenCalledTimes(1);
    expect(outcome).toBe("ack");
  });
});

describe("concurrent duplicates", () => {
  // Pub/Sub can deliver the same message twice concurrently. has() then tryRecord() leaves a
  // window where both pass the check, so the pipeline collapses in-process duplicates itself.
  test("runs the handler once for two concurrent deliveries of one message", async () => {
    const store = new InMemoryIdempotencyStore();
    let running = 0;
    let maxConcurrent = 0;
    const handler = vi.fn(async () => {
      running++;
      maxConcurrent = Math.max(maxConcurrent, running);
      await new Promise((r) => setTimeout(r, 20));
      running--;
    });
    const pipeline = pipelineWith({ [TYPE]: handler }, { idempotencyStore: store });

    const outcomes = await Promise.all([pipeline.handle(message()), pipeline.handle(message())]);

    expect(handler).toHaveBeenCalledTimes(1);
    expect(maxConcurrent).toBe(1);
    expect(outcomes).toEqual(["ack", "ack"]);
  });
});
