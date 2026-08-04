import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { PubSub } from "@google-cloud/pubsub";
import avro from "avsc";
import { PartialPublishError, createEventPublisher } from "@werken/nestjs-google-pubsub";
import type { EventPublisher, PubSubClientLike, PublishRequest } from "@werken/nestjs-google-pubsub";

export interface ShipmentEvent {
  readonly shipmentId: string;
  readonly carrier: string | null;
}

const DEFINITION = readFileSync(
  fileURLToPath(new URL("../../advanced-consumer/schema/shipment-events.avsc", import.meta.url)),
  "utf8",
);
const WRITER = avro.Type.forSchema(JSON.parse(DEFINITION) as avro.Schema);

const TOPICS: Record<string, string> = {
  "com.example.shipment.ready.v1": "shipment-events",
  "com.example.shipment.cancelled.v1": "shipment-events",
};

export function buildPublisher(client: PubSubClientLike): EventPublisher {
  return createEventPublisher({
    source: "https://example.com/shipping",
    client,
    topicResolver: (type) => TOPICS[type],

    /**
     * Avro JSON, which is what Pub/Sub's `JSON` encoding means — a nullable union is
     * {"string":"dhl"} and not "dhl", and plain JSON is rejected outright by a schema-attached
     * topic. Returning bare bytes declares `application/json`, which is correct here; return
     * `{ data, datacontenttype }` instead when the bytes are genuinely something else.
     */
    encode: (_type, data) => Buffer.from(WRITER.toString(data)),

    // Off unless asked for. With it on, `subject` becomes the ordering key, so two events for one
    // shipment stay in order — and the Topic is built with messageOrdering, which the SDK requires.
    ordering: true,
  });
}

export function shipmentEvents(): Array<PublishRequest<ShipmentEvent>> {
  return [
    { type: "com.example.shipment.ready.v1", data: { shipmentId: "s-1", carrier: "dhl" }, subject: "s-1" },
    { type: "com.example.shipment.ready.v1", data: { shipmentId: "s-2", carrier: "ups" }, subject: "s-2" },
    { type: "com.example.shipment.cancelled.v1", data: { shipmentId: "s-1", carrier: null }, subject: "s-1" },
  ];
}

async function main() {
  const client = new PubSub({ projectId: process.env.GCP_PROJECT_ID! }) as unknown as PubSubClientLike;
  try {
    const ids = await buildPublisher(client).publishBatch(shipmentEvents());
    console.log(`published ${ids.length} events: ${ids.join(", ")}`);
  } catch (error) {
    // Pub/Sub has no multi-message transaction, so a partly-failed batch leaves the successes
    // published and impossible to unsend. Retrying the whole batch would duplicate them.
    if (error instanceof PartialPublishError) {
      console.error(`published ${error.published.length}, failed ${error.failures.length} — retry only the failures`);
      for (const failure of error.failures)
        console.error(`  [${failure.index}] ${failure.type}: ${String(failure.cause)}`);
    }
    throw error;
  } finally {
    await client.close();
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) void main();
