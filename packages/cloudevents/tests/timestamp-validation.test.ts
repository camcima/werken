import { describe, expect, test } from "vitest";
import { parseEnvelope } from "@werken/cloudevents";

const required = {
  "ce-specversion": "1.0",
  "ce-id": "01931b7c-3f2a-7000-8000-000000000001",
  "ce-source": "https://example.test/service",
  "ce-type": "com.example.thing.happened.v1",
};

describe("calendar validity", () => {
  // JS Date silently rolls impossible dates forward: new Date("2026-02-30T00:00:00Z") yields
  // 2026-03-02. Accepting that would put a date the producer never sent into ce-time, and every
  // lateness figure derived from it would be quietly wrong.
  test.each([
    ["2026-02-30T00:00:00Z", "30 February"],
    ["2027-02-29T00:00:00Z", "29 February in a non-leap year"],
    ["2026-04-31T00:00:00Z", "31 April"],
  ])("rejects %s (%s)", (value) => {
    expect(() => parseEnvelope({ ...required, "ce-time": value })).toThrow(
      expect.objectContaining({ code: "invalid-attribute", attribute: "ce-time" }),
    );
  });

  test.each(["2026-13-01T00:00:00Z", "2026-00-01T00:00:00Z"])("rejects out-of-range month %s", (value) => {
    expect(() => parseEnvelope({ ...required, "ce-time": value })).toThrow(
      expect.objectContaining({ code: "invalid-attribute" }),
    );
  });

  // Calendar validation covers the date half; these reach the Date-parse guard instead, since the
  // regex admits any two digits for the time components.
  test.each(["2026-08-02T25:00:00Z", "2026-08-02T12:61:00Z"])("rejects out-of-range time %s", (value) => {
    expect(() => parseEnvelope({ ...required, "ce-time": value })).toThrow(
      expect.objectContaining({ code: "invalid-attribute", attribute: "ce-time" }),
    );
  });

  test("accepts 29 February in a leap year", () => {
    const envelope = parseEnvelope({ ...required, "ce-time": "2028-02-29T12:00:00Z" });
    expect(envelope.time).toEqual(new Date("2028-02-29T12:00:00Z"));
  });

  test("accepts a non-UTC offset without shifting the instant", () => {
    const envelope = parseEnvelope({ ...required, "ce-time": "2026-08-02T12:00:00-04:00" });
    expect(envelope.time).toEqual(new Date("2026-08-02T16:00:00Z"));
  });
});
