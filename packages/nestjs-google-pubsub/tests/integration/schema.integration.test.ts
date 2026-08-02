import { Encodings, PubSub, SchemaTypes } from "@google-cloud/pubsub";
import avro from "avsc";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { toPubSubAttributes } from "@werken/cloudevents";
import { WerkenPubSubTransport } from "@werken/nestjs-google-pubsub";
import type { PubSubClientLike } from "@werken/nestjs-google-pubsub";

const EMULATOR = process.env.PUBSUB_EMULATOR_HOST;
const PROJECT = process.env.PUBSUB_PROJECT_ID ?? "werken-it";
const TYPE = "com.example.thing.happened.v1";

const SCHEMA = {
  type: "record",
  name: "Thing",
  fields: [
    { name: "id", type: "string" },
    { name: "station", type: ["null", "string"], default: null },
  ],
};
const READER = avro.Type.forSchema(SCHEMA as avro.Schema);

/**
 * End-to-end schema resolution against the emulator: a real schema, a real schema-attached topic,
 * real googclient_* attributes, and a real Schema Service fetch by revision.
 */
describe.skipIf(!EMULATOR)("schema resolution against the emulator", () => {
  const suffix = Date.now();
  const schemaId = `werken-it-schema-${suffix}`;
  const topicId = `werken-it-schema-topic-${suffix}`;
  const subscriptionId = `werken-it-schema-sub-${suffix}`;
  const pubsub = new PubSub({ projectId: PROJECT });
  let transport: WerkenPubSubTransport;

  beforeAll(async () => {
    await pubsub.createSchema(schemaId, SchemaTypes.Avro, JSON.stringify(SCHEMA));
    await pubsub.createTopic({
      name: topicId,
      schemaSettings: {
        schema: `projects/${PROJECT}/schemas/${schemaId}`,
        encoding: Encodings.Json,
      },
    });
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
    await pubsub
      .schema(schemaId)
      .delete()
      .catch(() => {});
    await pubsub.close();
  });

  test("resolves the writer schema by revision and decodes Avro JSON", async () => {
    const received: unknown[] = [];

    transport = new WerkenPubSubTransport({
      projectId: PROJECT,
      subscription: subscriptionId,
      schemaRegistry: { readerTypeFor: () => READER },
      createClient: () => new PubSub({ projectId: PROJECT }) as unknown as PubSubClientLike,
    });
    transport.addHandler(TYPE, ((data: unknown) => void received.push(data)) as never, true);

    await new Promise<void>((resolve, reject) => {
      transport.listen((error?: unknown) => (error ? reject(error) : resolve()));
    });

    // SPIKE-1: the wire format is standard Avro JSON, so the nullable union is {"string":"SCL"}.
    // Publishing plain JSON here would be rejected by Pub/Sub outright.
    await pubsub.topic(topicId).publishMessage({
      data: Buffer.from(JSON.stringify({ id: "e1", station: { string: "SCL" } })),
      attributes: toPubSubAttributes({
        specversion: "1.0",
        id: "01931b7c-3f2a-7000-8000-0000000schema",
        source: "https://example.test/service",
        type: TYPE,
        datacontenttype: "application/json",
        extensions: {},
      }),
    });

    await waitFor(() => received.length > 0, 20_000);

    // The handler sees a normal object with the union already unwrapped.
    expect(received[0]).toEqual({ id: "e1", station: "SCL" });
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
