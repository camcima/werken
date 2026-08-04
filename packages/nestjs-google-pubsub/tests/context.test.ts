import { describe, expect, test } from "vitest";
import { parseEnvelope } from "@werken/cloudevents";
import { buildContext } from "@werken/nestjs-google-pubsub/internal";
import type { IncomingMessage } from "@werken/nestjs-google-pubsub";

const attributes = {
  "ce-specversion": "1.0",
  "ce-id": "01931b7c-3f2a-7000-8000-000000000001",
  "ce-source": "https://example.test/service",
  "ce-type": "com.example.thing.happened.v1",
};

function message(overrides: Partial<IncomingMessage> = {}): IncomingMessage {
  return {
    id: "pubsub-message-1",
    attributes,
    data: Buffer.from("{}"),
    publishTime: new Date("2026-08-02T15:00:00.000Z"),
    deliveryAttempt: 0,
    orderingKey: "",
    ack: () => {},
    nack: () => {},
    ...overrides,
  };
}

const subscription = "projects/p/subscriptions/s";

describe("buildContext", () => {
  test("carries the CloudEvents attributes through", () => {
    const ctx = buildContext(parseEnvelope(attributes), message(), subscription);

    expect(ctx.id).toBe("01931b7c-3f2a-7000-8000-000000000001");
    expect(ctx.source).toBe("https://example.test/service");
    expect(ctx.type).toBe("com.example.thing.happened.v1");
    expect(ctx.datacontenttype).toBe("application/json");
  });

  test("carries the Pub/Sub delivery metadata through", () => {
    const ctx = buildContext(parseEnvelope(attributes), message(), subscription);

    expect(ctx.messageId).toBe("pubsub-message-1");
    expect(ctx.subscription).toBe(subscription);
    expect(ctx.publishTime).toEqual(new Date("2026-08-02T15:00:00.000Z"));
  });

  describe("time", () => {
    test("uses ce-time when the producer sent one", () => {
      const envelope = parseEnvelope({ ...attributes, "ce-time": "2026-08-02T14:23:10.029Z" });
      const ctx = buildContext(envelope, message(), subscription);

      expect(ctx.time).toEqual(new Date("2026-08-02T14:23:10.029Z"));
    });

    test("falls back to publish time when ce-time is absent", () => {
      const ctx = buildContext(parseEnvelope(attributes), message(), subscription);

      expect(ctx.time).toEqual(new Date("2026-08-02T15:00:00.000Z"));
    });
  });

  describe("deliveryAttempt", () => {
    // Pub/Sub reports 0 on a subscription with no dead-letter policy (confirmed in SPIKE-0). A
    // handler branching on `deliveryAttempt === 1` for "first attempt" would otherwise be wrong on
    // every message on such a subscription.
    test("normalises 0 to 1", () => {
      expect(
        buildContext(parseEnvelope(attributes), message({ deliveryAttempt: 0 }), subscription).deliveryAttempt,
      ).toBe(1);
    });

    test("normalises undefined to 1", () => {
      const m = message();
      delete (m as { deliveryAttempt?: number }).deliveryAttempt;

      expect(buildContext(parseEnvelope(attributes), m, subscription).deliveryAttempt).toBe(1);
    });

    test("passes a real redelivery count through", () => {
      expect(
        buildContext(parseEnvelope(attributes), message({ deliveryAttempt: 4 }), subscription).deliveryAttempt,
      ).toBe(4);
    });
  });

  describe("orderingKey", () => {
    // Pub/Sub reports "" when unset, and the context types it optional, so a falsy-but-present key would be
    // a trap for `if (ctx.orderingKey)` style checks.
    test("maps an empty ordering key to undefined", () => {
      expect(
        buildContext(parseEnvelope(attributes), message({ orderingKey: "" }), subscription).orderingKey,
      ).toBeUndefined();
    });

    test("passes a real ordering key through", () => {
      expect(buildContext(parseEnvelope(attributes), message({ orderingKey: "bag-1" }), subscription).orderingKey).toBe(
        "bag-1",
      );
    });
  });

  test("exposes the raw message as an escape hatch", () => {
    const m = message();
    expect(buildContext(parseEnvelope(attributes), m, subscription).raw).toBe(m);
  });

  test("exposes unknown ce-* attributes as extensions", () => {
    const envelope = parseEnvelope({ ...attributes, "ce-tenantid": "acme" });

    expect(buildContext(envelope, message(), subscription).extensions).toEqual({ tenantid: "acme" });
  });
});
