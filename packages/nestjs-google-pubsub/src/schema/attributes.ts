import type { SchemaMessageMeta } from "./avro-codec.js";

/**
 * Attribute names Pub/Sub sets on a schema-encoded message. Confirmed empirically against the
 * emulator and against the client library's own source.
 *
 * The SDK offers `Schema.metadataFromMessage()` for exactly this, and hardcoding the names is a
 * deliberate trade: calling it would make @google-cloud/pubsub a hard runtime import of the decode
 * path, which is otherwise pure and unit-testable without the SDK installed. This file is the
 * single place that knows the names, so if the SDK ever changes them, only this file moves.
 */
const SCHEMA_ATTRIBUTES = {
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
