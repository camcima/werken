import { CE_PREFIX, ENVELOPE_ATTRIBUTES } from "./attributes.js";
import { EnvelopeValidationError } from "./errors.js";
import type { CloudEventEnvelope } from "./types.js";

/**
 * Bind an envelope to Pub/Sub message attributes (binary content mode).
 *
 * Throws `EnvelopeValidationError` for an extension named after an envelope field. Overwriting it
 * is not enough: optional fields are written only when present, so an extension named `time` or
 * `dataschema` would stand in for an absent field and declare metadata the envelope does not.
 * `parseEnvelope` can never produce such an envelope, so a collision is always a caller mistake.
 */
export function toPubSubAttributes(envelope: CloudEventEnvelope): Record<string, string> {
  const attributes: Record<string, string> = {};

  for (const [name, value] of Object.entries(envelope.extensions)) {
    const key = `${CE_PREFIX}${name}`;
    if (name === "" || ENVELOPE_ATTRIBUTES.has(name)) {
      throw new EnvelopeValidationError(
        "invalid-attribute",
        key,
        name === ""
          ? "extension name must not be empty"
          : `extension ${JSON.stringify(name)} collides with the envelope's own ${key} attribute`,
      );
    }
    attributes[key] = value;
  }

  attributes["ce-specversion"] = envelope.specversion;
  attributes["ce-id"] = envelope.id;
  attributes["ce-source"] = envelope.source;
  attributes["ce-type"] = envelope.type;
  attributes["ce-datacontenttype"] = envelope.datacontenttype;

  if (envelope.subject !== undefined) attributes["ce-subject"] = envelope.subject;
  if (envelope.time !== undefined) attributes["ce-time"] = timestamp(envelope.time, "ce-time");
  if (envelope.dataschema !== undefined) attributes["ce-dataschema"] = envelope.dataschema;
  if (envelope.traceparent !== undefined) attributes["ce-traceparent"] = envelope.traceparent;
  if (envelope.tracestate !== undefined) attributes["ce-tracestate"] = envelope.tracestate;
  if (envelope.ingestiontime !== undefined) {
    attributes["ce-ingestiontime"] = timestamp(envelope.ingestiontime, "ce-ingestiontime");
  }

  return attributes;
}

/**
 * An invalid Date is still a Date, so the type cannot keep one out, and `toISOString()` would throw a
 * bare RangeError. Raised as the same error type the parser uses, so a caller has one thing to catch.
 */
function timestamp(value: Date, key: string): string {
  if (Number.isNaN(value.getTime())) {
    throw new EnvelopeValidationError("invalid-attribute", key, `${key} is an invalid Date`);
  }
  return value.toISOString();
}
