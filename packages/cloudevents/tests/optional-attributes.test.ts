import { describe, expect, test } from "vitest";
import { EnvelopeValidationError, parseEnvelope } from "@werken/cloudevents";

const required = {
  "ce-specversion": "1.0",
  "ce-id": "01931b7c-3f2a-7000-8000-000000000001",
  "ce-source": "https://example.test/service",
  "ce-type": "com.example.thing.happened.v1",
};

describe("optional attributes", () => {
  test("are undefined when absent", () => {
    const envelope = parseEnvelope(required);

    expect(envelope.subject).toBeUndefined();
    expect(envelope.time).toBeUndefined();
    expect(envelope.dataschema).toBeUndefined();
    expect(envelope.traceparent).toBeUndefined();
    expect(envelope.tracestate).toBeUndefined();
    expect(envelope.ingestiontime).toBeUndefined();
  });

  test("are read when present", () => {
    const envelope = parseEnvelope({
      ...required,
      "ce-subject": "0045123456",
      "ce-dataschema": "https://schemas.example.test/thing/v1",
      "ce-traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
      "ce-tracestate": "vendor=value",
    });

    expect(envelope.subject).toBe("0045123456");
    expect(envelope.dataschema).toBe("https://schemas.example.test/thing/v1");
    expect(envelope.traceparent).toBe("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
    expect(envelope.tracestate).toBe("vendor=value");
  });
});

describe("datacontenttype", () => {
  test("defaults to application/json when absent", () => {
    expect(parseEnvelope(required).datacontenttype).toBe("application/json");
  });

  test("is read when present", () => {
    const envelope = parseEnvelope({ ...required, "ce-datacontenttype": "application/avro" });
    expect(envelope.datacontenttype).toBe("application/avro");
  });
});

describe("timestamps", () => {
  test("parses ce-time as an occurrence Date", () => {
    const envelope = parseEnvelope({ ...required, "ce-time": "2026-08-02T14:23:10.029Z" });
    expect(envelope.time).toEqual(new Date("2026-08-02T14:23:10.029Z"));
  });

  test("parses ce-ingestiontime as a Date", () => {
    const envelope = parseEnvelope({ ...required, "ce-ingestiontime": "2026-08-02T15:00:00.000Z" });
    expect(envelope.ingestiontime).toEqual(new Date("2026-08-02T15:00:00.000Z"));
  });

  test.each(["ce-time", "ce-ingestiontime"])("rejects an unparseable %s", (key) => {
    expect(() => parseEnvelope({ ...required, [key]: "not-a-timestamp" })).toThrow(
      expect.objectContaining({ code: "invalid-attribute", attribute: key }),
    );
  });

  test("rejects a ce-time that parses but is not RFC 3339", () => {
    // Date.parse accepts this; RFC 3339 does not. Accepting it would silently admit ambiguous
    // local-time values into a field whose whole purpose is a globally comparable instant.
    expect(() => parseEnvelope({ ...required, "ce-time": "August 2, 2026" })).toThrow(EnvelopeValidationError);
  });
});

describe("extensions", () => {
  test("preserves unknown ce-* attributes with the prefix stripped", () => {
    const envelope = parseEnvelope({ ...required, "ce-tenantid": "acme", "ce-partitionkey": "7" });

    expect(envelope.extensions).toEqual({ tenantid: "acme", partitionkey: "7" });
  });

  test("is empty when there are no unknown attributes", () => {
    expect(parseEnvelope(required).extensions).toEqual({});
  });

  test("does not absorb known attributes", () => {
    const envelope = parseEnvelope({ ...required, "ce-subject": "s", "ce-time": "2026-08-02T14:23:10Z" });

    expect(envelope.extensions).toEqual({});
  });

  test("ignores non-CloudEvents attributes such as Pub/Sub's own", () => {
    const envelope = parseEnvelope({
      ...required,
      googclient_schemaname: "projects/p/schemas/s",
      googclient_schemarevisionid: "ecd96f6b",
    });

    expect(envelope.extensions).toEqual({});
  });
});
