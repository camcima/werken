import { describe, expect, test } from "vitest";
import { eventLogFields, withEventFields } from "@werken/nestjs-google-pubsub/internal";
import type { IncomingMessage } from "@werken/nestjs-google-pubsub";

function message(overrides: Partial<IncomingMessage> = {}): IncomingMessage {
  return {
    id: "pubsub-message-1",
    attributes: {
      "ce-specversion": "1.0",
      "ce-id": "01931b7c-3f2a-7000-8000-000000000001",
      "ce-source": "https://example.test/service",
      "ce-type": "com.example.thing.happened.v1",
      "ce-subject": "thing-42",
    },
    data: Buffer.from("{}"),
    publishTime: new Date("2026-08-02T15:00:00.000Z"),
    deliveryAttempt: 0,
    orderingKey: "",
    ack: () => {},
    nack: () => {},
    ...overrides,
  };
}

describe("eventLogFields", () => {
  test("carries every field a pipeline log line needs", () => {
    expect(eventLogFields(message())).toEqual({
      ceId: "01931b7c-3f2a-7000-8000-000000000001",
      ceType: "com.example.thing.happened.v1",
      ceSource: "https://example.test/service",
      ceSubject: "thing-42",
      deliveryAttempt: 1,
      messageId: "pubsub-message-1",
    });
  });

  // Same normalisation as CloudEventContext, so a log line and a handler never disagree about
  // which attempt this is.
  test("normalises Pub/Sub's 0 delivery attempt to 1", () => {
    expect(eventLogFields(message({ deliveryAttempt: 0 })).deliveryAttempt).toBe(1);
  });

  test("passes a real redelivery count through", () => {
    expect(eventLogFields(message({ deliveryAttempt: 5 })).deliveryAttempt).toBe(5);
  });

  test("survives a message with no CloudEvents attributes at all", () => {
    const fields = eventLogFields(message({ attributes: {} }));

    expect(fields.ceId).toBeUndefined();
    expect(fields.messageId).toBe("pubsub-message-1");
  });
});

describe("withEventFields", () => {
  test("appends the fields as parseable JSON", () => {
    const line = withEventFields("werken: something happened", message());
    const json = JSON.parse(line.slice(line.indexOf("{")));

    expect(line).toMatch(/^werken: something happened /);
    expect(json.ceType).toBe("com.example.thing.happened.v1");
  });

  test("omits absent fields rather than emitting nulls", () => {
    const attributes = { ...message().attributes };
    delete (attributes as Record<string, string>)["ce-subject"];
    const line = withEventFields("werken: no subject", message({ attributes }));

    expect(JSON.parse(line.slice(line.indexOf("{")))).not.toHaveProperty("ceSubject");
  });
});
