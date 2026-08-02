import { describe, expect, test } from "vitest";
import { EnvelopeValidationError, parseEnvelope } from "@werken/cloudevents";

const required = {
  "ce-specversion": "1.0",
  "ce-id": "01931b7c-3f2a-7000-8000-000000000001",
  "ce-source": "https://example.test/service",
  "ce-type": "com.example.thing.happened.v1",
};

describe("required attributes", () => {
  test.each(["ce-specversion", "ce-id", "ce-source", "ce-type"])("rejects a missing %s", (missing) => {
    const attributes = { ...required };
    delete (attributes as Record<string, string>)[missing];

    expect(() => parseEnvelope(attributes)).toThrow(EnvelopeValidationError);
  });

  test.each(["ce-id", "ce-source", "ce-type"])("rejects an empty %s", (empty) => {
    expect(() => parseEnvelope({ ...required, [empty]: "" })).toThrow(EnvelopeValidationError);
  });

  test("names the offending attribute on the error", () => {
    const attributes = { ...required };
    delete (attributes as Record<string, string>)["ce-source"];

    expect(() => parseEnvelope(attributes)).toThrow(
      expect.objectContaining({ code: "missing-attribute", attribute: "ce-source" }),
    );
  });
});

describe("specversion", () => {
  test("rejects a specversion other than 1.0", () => {
    expect(() => parseEnvelope({ ...required, "ce-specversion": "0.3" })).toThrow(
      expect.objectContaining({ code: "unsupported-specversion", attribute: "ce-specversion" }),
    );
  });

  test("error message carries the rejected value so operators can see it", () => {
    expect(() => parseEnvelope({ ...required, "ce-specversion": "0.3" })).toThrow(/0\.3/);
  });
});
