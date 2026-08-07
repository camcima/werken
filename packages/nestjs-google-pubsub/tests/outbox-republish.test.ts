import "reflect-metadata";
import { Controller, Injectable, Module } from "@nestjs/common";
import { EventPattern, Payload } from "@nestjs/microservices";
import { afterEach, describe, expect, test, vi } from "vitest";
import { createEventPublisher } from "@werken/nestjs-google-pubsub";
import { createWerkenTestHarness } from "@werken/nestjs-google-pubsub/testing";
import type { PubSubClientLike } from "@werken/nestjs-google-pubsub";
import type { WerkenTestHarness } from "@werken/nestjs-google-pubsub/testing";

const TYPE = "com.example.order.placed.v1";
const SOURCE = "https://example.test/orders";

@Injectable()
class OrderProjection {
  readonly applied: unknown[] = [];
  apply(data: unknown) {
    this.applied.push(data);
  }
}

@Controller()
class OrderConsumer {
  constructor(private readonly projection: OrderProjection) {}

  @EventPattern(TYPE)
  onOrderPlaced(@Payload() data: unknown) {
    this.projection.apply(data);
  }
}

@Module({ controllers: [OrderConsumer], providers: [OrderProjection] })
class WorkerModule {}

interface Captured {
  attributes: Record<string, string>;
  data: Buffer;
}

/** Captures what the publisher put on the wire, so it can be replayed into the consumer harness. */
function capturingPublisher() {
  const sent: Captured[] = [];
  const client = {
    topic: vi.fn(() => ({
      publishMessage: vi.fn(async (m: Captured) => {
        sent.push(m);
        return `msg-${sent.length}`;
      }),
    })),
    subscription: vi.fn(),
    close: vi.fn(async () => {}),
  } as unknown as PubSubClientLike;

  return {
    sent,
    publisher: createEventPublisher({
      source: SOURCE,
      client,
      topicResolver: (type: string) => type.replaceAll(".", "-"),
    }),
  };
}

let harness: WerkenTestHarness;
afterEach(async () => {
  await harness?.close();
});

/**
 * The transactional outbox publishes first and marks second, so a relay that dies in between
 * republishes the row. That is only harmless because the row carries the ce-id: republished under a
 * fresh one, the consumer's idempotency store has nothing to match and one state change becomes two
 * events, with no error anywhere. This is that failure, end to end.
 */
describe("republishing an outbox row", () => {
  test("keeps the same ce-id and reaches the handler exactly once", async () => {
    const { publisher, sent } = capturingPublisher();
    // The relay reads this off the outbox row; it was generated in the ingest transaction.
    const request = {
      type: TYPE,
      data: { orderId: "abc" },
      subject: "abc",
      id: "01927f9a-0000-7000-8000-00000000abcd",
    };

    await publisher.publish(request);
    await publisher.publish(request); // the relay crashed before marking the row, so it goes again

    expect(sent).toHaveLength(2);
    expect(sent[0].attributes["ce-id"]).toBe("01927f9a-0000-7000-8000-00000000abcd");
    expect(sent[1].attributes["ce-id"]).toBe(sent[0].attributes["ce-id"]);

    harness = await createWerkenTestHarness({ module: WorkerModule });
    for (const message of sent) {
      await harness.emitRaw(message.attributes, message.data);
    }

    expect(harness.get<OrderProjection>(OrderProjection).applied).toEqual([{ orderId: "abc" }]);
    expect(harness.acked).toHaveLength(2);
    expect(harness.nacked).toHaveLength(0);
  });

  // The control: without a stable id this is what the relay produced before, and it is why the bug
  // was invisible — two distinct events, both acked, no error raised anywhere.
  test("without a supplied id the republish is a second event the store cannot suppress", async () => {
    const { publisher, sent } = capturingPublisher();
    const request = { type: TYPE, data: { orderId: "abc" }, subject: "abc" };

    await publisher.publish(request);
    await publisher.publish(request);

    expect(sent[0].attributes["ce-id"]).not.toBe(sent[1].attributes["ce-id"]);

    harness = await createWerkenTestHarness({ module: WorkerModule });
    for (const message of sent) {
      await harness.emitRaw(message.attributes, message.data);
    }

    expect(harness.get<OrderProjection>(OrderProjection).applied).toHaveLength(2);
  });
});
