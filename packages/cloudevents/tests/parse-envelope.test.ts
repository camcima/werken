import { describe, expect, test } from "vitest";
import { parseEnvelope } from "@werken/cloudevents";

const required = {
  "ce-specversion": "1.0",
  "ce-id": "01931b7c-3f2a-7000-8000-000000000001",
  "ce-source": "https://example.test/service",
  "ce-type": "com.example.thing.happened.v1",
};

describe("parseEnvelope", () => {
  test("reads the four required attributes", () => {
    const envelope = parseEnvelope(required);

    expect(envelope.specversion).toBe("1.0");
    expect(envelope.id).toBe("01931b7c-3f2a-7000-8000-000000000001");
    expect(envelope.source).toBe("https://example.test/service");
    expect(envelope.type).toBe("com.example.thing.happened.v1");
  });
});
