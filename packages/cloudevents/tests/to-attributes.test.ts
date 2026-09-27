import { describe, expect, test } from "vitest";
import { EnvelopeValidationError, parseEnvelope, toPubSubAttributes } from "@werken/cloudevents";
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

  test.each(["time", "ingestiontime"] as const)("rejects an invalid %s Date with a validation error", (field) => {
    // `new Date(NaN)` is still a Date, so the type admits it; toISOString() would throw a bare
    // RangeError that callers branching on EnvelopeValidationError would not recognise.
    expect(() => toPubSubAttributes({ ...base, [field]: new Date(Number.NaN) })).toThrow(
      expect.objectContaining({ code: "invalid-attribute", attribute: `ce-${field}` }),
    );
  });

  test("types specversion as the only version it can write", () => {
    // parseEnvelope rejects anything but 1.0, so the type should not admit an envelope that would
    // serialise cleanly and then fail on the consumer's side of the wire.
    // @ts-expect-error -- "0.3" is not a CloudEvents version this package speaks
    const envelope: CloudEventEnvelope = { ...base, specversion: "0.3" };

    expect(envelope.specversion).toBe("0.3");
  });

  test("writes extensions back with the ce- prefix restored", () => {
    const attributes = toPubSubAttributes({ ...base, extensions: { tenantid: "acme", partitionkey: "7" } });

    expect(attributes["ce-tenantid"]).toBe("acme");
    expect(attributes["ce-partitionkey"]).toBe("7");
  });

  // Every name the envelope has a field for. An extension under one of these either loses to the
  // field (required ones, always written) or silently stands in for it (optional ones, written only
  // when present) — so an extension could declare a ce-time or ce-dataschema the envelope does not.
  const reserved = [
    "specversion",
    "id",
    "source",
    "type",
    "subject",
    "time",
    "datacontenttype",
    "dataschema",
    "traceparent",
    "tracestate",
    "ingestiontime",
  ];

  test.each(reserved)("rejects an extension named %s when the envelope field is absent", (name) => {
    expect(() => toPubSubAttributes({ ...base, extensions: { [name]: "x" } })).toThrow(
      expect.objectContaining({ code: "invalid-attribute", attribute: `ce-${name}` }),
    );
  });

  test.each(reserved)("rejects an extension named %s when the envelope field is present", (name) => {
    const full: CloudEventEnvelope = {
      ...base,
      subject: "s",
      time: new Date("2026-08-02T14:23:10.029Z"),
      dataschema: "https://schemas.example.test/thing/v1",
      traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
      tracestate: "vendor=value",
      ingestiontime: new Date("2026-08-02T15:00:00.000Z"),
    };

    expect(() => toPubSubAttributes({ ...full, extensions: { [name]: "x" } })).toThrow(EnvelopeValidationError);
  });

  test("rejects an empty extension name, which would serialise as a bare ce- attribute", () => {
    expect(() => toPubSubAttributes({ ...base, extensions: { "": "x" } })).toThrow(
      expect.objectContaining({ code: "invalid-attribute", attribute: "ce-" }),
    );
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
