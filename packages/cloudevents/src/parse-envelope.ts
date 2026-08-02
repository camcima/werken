import { EnvelopeValidationError } from "./errors.js";
import type { CloudEventEnvelope, PubSubAttributes } from "./types.js";

const SPEC_VERSION = "1.0";
const DEFAULT_DATACONTENTTYPE = "application/json";
const CE_PREFIX = "ce-";

const REQUIRED = ["ce-specversion", "ce-id", "ce-source", "ce-type"] as const;

/**
 * Attributes this package lifts into named envelope fields. Everything else prefixed `ce-` is an
 * extension and is preserved verbatim on `extensions`.
 */
const KNOWN = new Set<string>([
  ...REQUIRED,
  "ce-subject",
  "ce-time",
  "ce-datacontenttype",
  "ce-dataschema",
  "ce-traceparent",
  "ce-tracestate",
  "ce-ingestiontime",
]);

/**
 * RFC 3339 date-time. Deliberately stricter than `Date.parse`, which accepts things like
 * "August 2, 2026" and locale-dependent forms — admitting those into a field whose purpose is a
 * globally comparable instant is how lateness maths silently goes wrong.
 */
const RFC_3339 = /^(\d{4})-(\d{2})-(\d{2})[Tt]\d{2}:\d{2}:\d{2}(\.\d+)?([Zz]|[+-]\d{2}:\d{2})$/;

/** Days in `month` (1-based) of `year`, accounting for leap years. */
function daysInMonth(year: number, month: number): number {
  return new Date(Date.UTC(year, month, 0)).getUTCDate();
}

function requireAttribute(attributes: PubSubAttributes, key: string): string {
  const value = attributes[key];
  if (value === undefined || value === "") {
    throw new EnvelopeValidationError("missing-attribute", key, `required CloudEvents attribute ${key} is missing`);
  }
  return value;
}

function optionalTimestamp(attributes: PubSubAttributes, key: string): Date | undefined {
  const raw = attributes[key];
  if (raw === undefined || raw === "") return undefined;

  const match = RFC_3339.exec(raw);
  if (match === null) {
    throw new EnvelopeValidationError(
      "invalid-attribute",
      key,
      `${key} must be an RFC 3339 timestamp, got ${JSON.stringify(raw)}`,
    );
  }

  // Date silently rolls impossible calendar dates forward ("2026-02-30" becomes 2026-03-02), which
  // would put an instant the producer never sent into the envelope. Reject rather than roll.
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (month < 1 || month > 12 || day < 1 || day > daysInMonth(year, month)) {
    throw new EnvelopeValidationError(
      "invalid-attribute",
      key,
      `${key} is not a real calendar date: ${JSON.stringify(raw)}`,
    );
  }

  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) {
    throw new EnvelopeValidationError(
      "invalid-attribute",
      key,
      `${key} is not a valid timestamp: ${JSON.stringify(raw)}`,
    );
  }
  return parsed;
}

function extensionsFrom(attributes: PubSubAttributes): Record<string, string> {
  const extensions: Record<string, string> = {};
  for (const [key, value] of Object.entries(attributes)) {
    if (key.startsWith(CE_PREFIX) && !KNOWN.has(key)) {
      extensions[key.slice(CE_PREFIX.length)] = value;
    }
  }
  return extensions;
}

export function parseEnvelope(attributes: PubSubAttributes): CloudEventEnvelope {
  for (const key of REQUIRED) {
    requireAttribute(attributes, key);
  }

  const specversion = attributes["ce-specversion"];
  if (specversion !== SPEC_VERSION) {
    throw new EnvelopeValidationError(
      "unsupported-specversion",
      "ce-specversion",
      `unsupported CloudEvents specversion ${JSON.stringify(specversion)}, expected ${SPEC_VERSION}`,
    );
  }

  return {
    specversion,
    id: attributes["ce-id"],
    source: attributes["ce-source"],
    type: attributes["ce-type"],
    subject: attributes["ce-subject"] || undefined,
    time: optionalTimestamp(attributes, "ce-time"),
    datacontenttype: attributes["ce-datacontenttype"] || DEFAULT_DATACONTENTTYPE,
    dataschema: attributes["ce-dataschema"] || undefined,
    traceparent: attributes["ce-traceparent"] || undefined,
    tracestate: attributes["ce-tracestate"] || undefined,
    ingestiontime: optionalTimestamp(attributes, "ce-ingestiontime"),
    extensions: extensionsFrom(attributes),
  };
}
