import { describe, expect, test } from "vitest";
import { parseEnvelope, toPubSubAttributes } from "@werken/cloudevents";
import type { CloudEventEnvelope } from "@werken/cloudevents";

const base: CloudEventEnvelope = {
  specversion: "1.0",
  id: "01931b7c-3f2a-7000-8000-000000000001",
  source: "https://example.test/service",
  type: "com.example.thing.happened.v1",
  datacontenttype: "application/json",
  extensions: {},
};

describe("toPubSubAttributes", () => {
  test("writes the required attributes", () => {
    expect(toPubSubAttributes(base)).toEqual({
      "ce-specversion": "1.0",
      "ce-id": "01931b7c-3f2a-7000-8000-000000000001",
      "ce-source": "https://example.test/service",
      "ce-type": "com.example.thing.happened.v1",
      "ce-datacontenttype": "application/json",
    });
  });

  test("omits absent optional attributes rather than writing empty strings", () => {
    const attributes = toPubSubAttributes(base);

    expect(attributes).not.toHaveProperty("ce-subject");
    expect(attributes).not.toHaveProperty("ce-time");
    expect(attributes).not.toHaveProperty("ce-dataschema");
    expect(attributes).not.toHaveProperty("ce-traceparent");
  });

  test("writes timestamps as RFC 3339 UTC", () => {
    const attributes = toPubSubAttributes({
      ...base,
      time: new Date("2026-08-02T14:23:10.029Z"),
      ingestiontime: new Date("2026-08-02T15:00:00.000Z"),
    });

    expect(attributes["ce-time"]).toBe("2026-08-02T14:23:10.029Z");
    expect(attributes["ce-ingestiontime"]).toBe("2026-08-02T15:00:00.000Z");
  });

  test("writes extensions back with the ce- prefix restored", () => {
    const attributes = toPubSubAttributes({ ...base, extensions: { tenantid: "acme", partitionkey: "7" } });

    expect(attributes["ce-tenantid"]).toBe("acme");
    expect(attributes["ce-partitionkey"]).toBe("7");
  });

  test("does not let an extension overwrite a known attribute", () => {
    const attributes = toPubSubAttributes({ ...base, extensions: { type: "spoofed", id: "spoofed" } });

    expect(attributes["ce-type"]).toBe("com.example.thing.happened.v1");
    expect(attributes["ce-id"]).toBe("01931b7c-3f2a-7000-8000-000000000001");
  });
});

describe("round trip", () => {
  test("parse(toAttributes(envelope)) preserves every field", () => {
    const envelope: CloudEventEnvelope = {
      ...base,
      subject: "0045123456",
      time: new Date("2026-08-02T14:23:10.029Z"),
      dataschema: "https://schemas.example.test/thing/v1",
      traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
      tracestate: "vendor=value",
      ingestiontime: new Date("2026-08-02T15:00:00.000Z"),
      extensions: { tenantid: "acme" },
    };

    expect(parseEnvelope(toPubSubAttributes(envelope))).toEqual(envelope);
  });
});
