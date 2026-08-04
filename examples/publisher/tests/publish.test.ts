import { describe, expect, test, vi } from "vitest";
import type { PubSubClientLike } from "@werken/nestjs-google-pubsub";
import { buildPublisher, shipmentEvents } from "../src/main.js";

function fakeClient() {
  const published: Array<{ topic: string; data: Buffer; attributes: Record<string, string>; orderingKey?: string }> =
    [];
  const client = {
    topic: vi.fn((topic: string) => ({
      publishMessage: vi.fn(async (m: { data: Buffer; attributes: Record<string, string>; orderingKey?: string }) => {
        published.push({ topic, ...m });
        return `msg-${published.length}`;
      }),
    })),
    subscription: vi.fn(),
    close: vi.fn(async () => {}),
  };
  return { published, client: client as unknown as PubSubClientLike };
}

describe("publisher example", () => {
  test("stamps a CloudEvents envelope on every event", async () => {
    const { published, client } = fakeClient();

    await buildPublisher(client).publishBatch(shipmentEvents());

    expect(published.length).toBeGreaterThan(0);
    for (const message of published) {
      expect(message.attributes["ce-specversion"]).toBe("1.0");
      expect(message.attributes["ce-type"]).toMatch(/^com\.example\.shipment\./);
      expect(message.attributes["ce-id"]).toBeTruthy();
    }
  });

  // Ordering is off unless asked for. With it on, `subject` becomes the key, which is what keeps
  // two events for one shipment in order.
  test("derives the ordering key from subject", async () => {
    const { published, client } = fakeClient();

    await buildPublisher(client).publishBatch(shipmentEvents());

    expect(published[0].orderingKey).toBe(published[0].attributes["ce-subject"]);
  });

  // Pub/Sub's JSON encoding is Avro JSON: a nullable union is {"string":"dhl"}, not "dhl". Plain
  // JSON is rejected outright by a schema-attached topic.
  test("encodes the body as Avro JSON, not plain JSON", async () => {
    const { published, client } = fakeClient();

    await buildPublisher(client).publishBatch(shipmentEvents());

    const body = JSON.parse(published[0].data.toString("utf8")) as { carrier?: unknown };
    expect(body.carrier).toEqual({ string: "dhl" });
  });
});
