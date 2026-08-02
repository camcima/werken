import { PubSub } from "@google-cloud/pubsub";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { toPubSubAttributes } from "@werken/cloudevents";
import { WerkenPubSubTransport } from "@werken/nestjs-google-pubsub";
import type { CloudEventContext, PubSubClientLike } from "@werken/nestjs-google-pubsub";

const EMULATOR = process.env.PUBSUB_EMULATOR_HOST;
const PROJECT = process.env.PUBSUB_PROJECT_ID ?? "werken-it";
const TYPE = "com.example.thing.happened.v1";

/**
 * Exercises the real @google-cloud/pubsub client against the emulator — no GCP project,
 * credentials or spend. Skipped when the emulator is not running so `pnpm test` stays green.
 */
describe.skipIf(!EMULATOR)("transport against the Pub/Sub emulator", () => {
  const suffix = Date.now();
  const topicId = `werken-it-topic-${suffix}`;
  const subscriptionId = `werken-it-sub-${suffix}`;
  const pubsub = new PubSub({ projectId: PROJECT });
  let transport: WerkenPubSubTransport;

  beforeAll(async () => {
    await pubsub.createTopic(topicId);
    await pubsub.topic(topicId).createSubscription(subscriptionId);
  });

  afterAll(async () => {
    await transport?.close();
    await pubsub
      .subscription(subscriptionId)
      .delete()
      .catch(() => {});
    await pubsub
      .topic(topicId)
      .delete()
      .catch(() => {});
    await pubsub.close();
  });

  test("delivers a published CloudEvent to the matching handler and acks it", async () => {
    const received: Array<{ data: unknown; ctx: CloudEventContext }> = [];

    transport = new WerkenPubSubTransport({
      projectId: PROJECT,
      subscription: subscriptionId,
      createClient: () => new PubSub({ projectId: PROJECT }) as unknown as PubSubClientLike,
    });

    transport.addHandler(
      TYPE,
      ((data: unknown, ctx: CloudEventContext) => {
        received.push({ data, ctx });
      }) as never,
      true,
    );

    await new Promise<void>((resolve, reject) => {
      transport.listen((error?: unknown) => (error ? reject(error) : resolve()));
    });

    expect(transport.isHealthy()).toBe(true);

    const attributes = toPubSubAttributes({
      specversion: "1.0",
      id: "01931b7c-3f2a-7000-8000-00000000beef",
      source: "https://example.test/service",
      type: TYPE,
      subject: "thing-42",
      time: new Date("2026-08-02T14:23:10.029Z"),
      datacontenttype: "application/json",
      extensions: { tenantid: "acme" },
    });

    await pubsub.topic(topicId).publishMessage({
      data: Buffer.from(JSON.stringify({ hello: "world" })),
      attributes,
    });

    await waitFor(() => received.length > 0, 20_000);

    expect(received).toHaveLength(1);
    expect(received[0].data).toEqual({ hello: "world" });
    expect(received[0].ctx.type).toBe(TYPE);
    expect(received[0].ctx.subject).toBe("thing-42");
    expect(received[0].ctx.time).toEqual(new Date("2026-08-02T14:23:10.029Z"));
    expect(received[0].ctx.extensions).toEqual({ tenantid: "acme" });
    // No dead-letter policy on this subscription, so Pub/Sub reports 0 — normalised to 1.
    expect(received[0].ctx.deliveryAttempt).toBe(1);
    expect(received[0].ctx.orderingKey).toBeUndefined();
  }, 60_000);

  // Every real consumer omits createClient and lets the transport build its own PubSub via Nest's
  // loadPackage. Injecting a client in the test above would leave that path unexercised.
  test("builds its own Pub/Sub client when createClient is not supplied", async () => {
    const received: unknown[] = [];
    const ownClientSubId = `${subscriptionId}-own`;
    await pubsub.topic(topicId).createSubscription(ownClientSubId);

    const own = new WerkenPubSubTransport({ projectId: PROJECT, subscription: ownClientSubId });
    own.addHandler(TYPE, ((data: unknown) => void received.push(data)) as never, true);

    try {
      await new Promise<void>((resolve, reject) => {
        own.listen((error?: unknown) => (error ? reject(error) : resolve()));
      });

      await pubsub.topic(topicId).publishMessage({
        data: Buffer.from(JSON.stringify({ via: "default-factory" })),
        attributes: toPubSubAttributes({
          specversion: "1.0",
          id: "01931b7c-3f2a-7000-8000-00000000cafe",
          source: "https://example.test/service",
          type: TYPE,
          datacontenttype: "application/json",
          extensions: {},
        }),
      });

      await waitFor(() => received.length > 0, 20_000);
      expect(received[0]).toEqual({ via: "default-factory" });
    } finally {
      await own.close();
      await pubsub
        .subscription(ownClientSubId)
        .delete()
        .catch(() => {});
    }
  }, 60_000);
});

async function waitFor(predicate: () => boolean, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`timed out after ${timeoutMs}ms waiting for condition`);
}
