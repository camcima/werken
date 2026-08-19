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

/**
 * The Date-parse guard catches minute 60 and second 60, but not hour 24: JS accepts it and rolls
 * to the following midnight, so "2026-08-02T24:00:00Z" arrives as the 3rd. That is the same silent
 * day-rolling the calendar check above exists to refuse, and RFC 3339 forbids hour 24 outright.
 */
describe("time-of-day range", () => {
  test.each([
    ["2026-08-02T24:00:00Z", "hour 24 rolls to the next day"],
    ["2026-08-02T24:00:00.000Z", "with a fraction"],
    ["2026-08-02T24:00:00+00:00", "with an explicit offset"],
  ])("rejects %s (%s)", (value) => {
    expect(() => parseEnvelope({ ...required, "ce-time": value })).toThrow(
      expect.objectContaining({ code: "invalid-attribute", attribute: "ce-time" }),
    );
  });

  test("still accepts the last representable instant of a day", () => {
    const envelope = parseEnvelope({ ...required, "ce-time": "2026-08-02T23:59:59.999Z" });

    expect(envelope.time?.toISOString()).toBe("2026-08-02T23:59:59.999Z");
  });

  test("applies the same range check to ce-ingestiontime", () => {
    expect(() => parseEnvelope({ ...required, "ce-ingestiontime": "2026-08-02T24:00:00Z" })).toThrow(
      expect.objectContaining({ code: "invalid-attribute", attribute: "ce-ingestiontime" }),
    );
  });
});
