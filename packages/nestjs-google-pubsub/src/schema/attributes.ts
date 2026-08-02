import type { SchemaMessageMeta } from "./avro-codec.js";

/**
 * Attribute names Pub/Sub sets on a schema-encoded message. Confirmed empirically and against the
 * client library source in docs/spikes/SPIKE-0-pubsub-schema-attrs.md.
 *
 * SPIKE-0 recommends calling `Schema.metadataFromMessage()` rather than hardcoding these. That
 * would make @google-cloud/pubsub a hard runtime import of the decode path, which is otherwise
 * pure and unit-testable without the SDK. This function is the single place that knows the names —
 * if the SDK ever changes them, only this file moves.
 */
export const SCHEMA_ATTRIBUTES = {
  name: "googclient_schemaname",
  revision: "googclient_schemarevisionid",
  encoding: "googclient_schemaencoding",
} as const;

export function schemaMetaFromAttributes(attributes: Readonly<Record<string, string>>): SchemaMessageMeta {
  return {
    name: attributes[SCHEMA_ATTRIBUTES.name],
    revision: attributes[SCHEMA_ATTRIBUTES.revision],
    encoding: attributes[SCHEMA_ATTRIBUTES.encoding],
  };
}
