import { Encodings, PubSub, SchemaTypes } from "@google-cloud/pubsub";
import avro from "avsc";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { createEventPublisher, WerkenPubSubTransport } from "@werken/nestjs-google-pubsub";
import type { CloudEventContext, PubSubClientLike } from "@werken/nestjs-google-pubsub";
import { skipUnlessAvailable } from "./requires.js";

const EMULATOR = process.env.PUBSUB_EMULATOR_HOST;
const PROJECT = process.env.PUBSUB_PROJECT_ID ?? "werken-it";
const TYPE = "com.example.published.v1";

const SCHEMA = {
  type: "record",
  name: "Published",
  fields: [
    { name: "id", type: "string" },
    { name: "station", type: ["null", "string"], default: null },
  ],
};
const AVRO_TYPE = avro.Type.forSchema(SCHEMA as avro.Schema);

/**
 * A real publish/consume round trip against the emulator.
 *
 * The unit tests assert against a fake topic, so they only confirm the publisher called it with
 * what I intended. Whether Pub/Sub accepts the encoding, preserves the attributes, and whether the
 * transport can read back what the publisher wrote, are contracts with the broker.
 */
describe.skipIf(skipUnlessAvailable("PUBSUB_EMULATOR_HOST", EMULATOR))("publisher against the emulator", () => {
  const suffix = Date.now();
  const topicId = `werken-pub-topic-${suffix}`;
  const subscriptionId = `werken-pub-sub-${suffix}`;
  const schemaId = `werken-pub-schema-${suffix}`;
  const schemaTopicId = `werken-pub-schema-topic-${suffix}`;
  const pubsub = new PubSub({ projectId: PROJECT });
  let transport: WerkenPubSubTransport | undefined;

  beforeAll(async () => {
    await pubsub.createTopic(topicId);
    await pubsub.topic(topicId).createSubscription(subscriptionId);
    await pubsub.createSchema(schemaId, SchemaTypes.Avro, JSON.stringify(SCHEMA));
    await pubsub.createTopic({
      name: schemaTopicId,
      schemaSettings: { schema: `projects/${PROJECT}/schemas/${schemaId}`, encoding: Encodings.Json },
    });
  });

  afterAll(async () => {
    await transport?.close();
    await pubsub
      .subscription(subscriptionId)
      .delete()
      .catch(() => {});
    for (const t of [topicId, schemaTopicId]) {
      await pubsub
        .topic(t)
        .delete()
        .catch(() => {});
    }
    await pubsub
      .schema(schemaId)
      .delete()
      .catch(() => {});
    await pubsub.close();
  });

  function publisher(overrides: Record<string, unknown> = {}) {
    return createEventPublisher({
      source: "https://example.test/publisher",
      client: pubsub as unknown as PubSubClientLike,
      topicResolver: () => topicId,
      ...overrides,
    });
  }

  test("a published event round-trips through the transport", async () => {
    const received: Array<{ data: unknown; ctx: CloudEventContext }> = [];

    transport = new WerkenPubSubTransport({
      projectId: PROJECT,
      subscription: subscriptionId,
      createClient: () => new PubSub({ projectId: PROJECT }) as unknown as PubSubClientLike,
    });
    transport.addHandler(
      TYPE,
      ((data: unknown, ctx: CloudEventContext) => void received.push({ data, ctx })) as never,
      true,
    );
    await new Promise<void>((resolve, reject) => {
      transport!.listen((error?: unknown) => (error ? reject(error) : resolve()));
    });

    const messageId = await publisher().publish({
      type: TYPE,
      data: { id: "e1", station: "SCL" },
      subject: "thing-42",
      extensions: { tenantid: "acme" },
    });

    expect(messageId).toBeTruthy();
    await waitFor(() => received.length > 0, 20_000);

    // Everything the publisher wrote is what the consumer sees.
    expect(received[0].data).toEqual({ id: "e1", station: "SCL" });
    expect(received[0].ctx.type).toBe(TYPE);
    expect(received[0].ctx.subject).toBe("thing-42");
    expect(received[0].ctx.source).toBe("https://example.test/publisher");
    expect(received[0].ctx.extensions).toMatchObject({ tenantid: "acme" });
    // v7 id, and both timestamps present.
    expect(received[0].ctx.id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    expect(received[0].ctx.ingestionTime).toBeInstanceOf(Date);
  }, 90_000);

  // SPIKE-1: Pub/Sub rejects plain JSON on a schema'd topic. This is the test that would fail if
  // anyone replaced the encoder with JSON.stringify.
  test("an Avro-encoded payload is accepted by a schema-attached topic", async () => {
    const encoded = publisher({
      topicResolver: () => schemaTopicId,
      encode: (_type: string, data: unknown) => Buffer.from(AVRO_TYPE.toString(data)),
    });

    await expect(encoded.publish({ type: TYPE, data: { id: "e1", station: "SCL" } })).resolves.toBeTruthy();
  }, 60_000);

  test("plain JSON is rejected by that same topic, which is why the encoder exists", async () => {
    const plain = publisher({ topicResolver: () => schemaTopicId });

    await expect(plain.publish({ type: TYPE, data: { id: "e1", station: "SCL" } })).rejects.toThrow();
  }, 60_000);

  test("publishBatch returns one id per request", async () => {
    const ids = await publisher().publishBatch([
      { type: TYPE, data: { id: "b1", station: null } },
      { type: TYPE, data: { id: "b2", station: null } },
    ]);

    expect(ids).toHaveLength(2);
    expect(new Set(ids).size).toBe(2);
  }, 60_000);
});

async function waitFor(predicate: () => boolean, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`timed out after ${timeoutMs}ms`);
}
