import type { CloudEventEnvelope } from "./types.js";

const CE_PREFIX = "ce-";

/**
 * Bind an envelope to Pub/Sub message attributes (binary content mode).
 *
 * Extensions are written first so a malicious or careless extension key cannot shadow a known
 * CloudEvents attribute — `ce-type` must always be the routing key the envelope declares.
 */
export function toPubSubAttributes(envelope: CloudEventEnvelope): Record<string, string> {
  const attributes: Record<string, string> = {};

  for (const [name, value] of Object.entries(envelope.extensions)) {
    attributes[`${CE_PREFIX}${name}`] = value;
  }

  attributes["ce-specversion"] = envelope.specversion;
  attributes["ce-id"] = envelope.id;
  attributes["ce-source"] = envelope.source;
  attributes["ce-type"] = envelope.type;
  attributes["ce-datacontenttype"] = envelope.datacontenttype;

  if (envelope.subject !== undefined) attributes["ce-subject"] = envelope.subject;
  if (envelope.time !== undefined) attributes["ce-time"] = envelope.time.toISOString();
  if (envelope.dataschema !== undefined) attributes["ce-dataschema"] = envelope.dataschema;
  if (envelope.traceparent !== undefined) attributes["ce-traceparent"] = envelope.traceparent;
  if (envelope.tracestate !== undefined) attributes["ce-tracestate"] = envelope.tracestate;
  if (envelope.ingestiontime !== undefined) attributes["ce-ingestiontime"] = envelope.ingestiontime.toISOString();

  return attributes;
}
