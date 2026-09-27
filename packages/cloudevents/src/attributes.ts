/** Prefix every CloudEvents attribute carries as a Pub/Sub message attribute (binary content mode). */
export const CE_PREFIX = "ce-";

/**
 * Attribute names this package lifts into named envelope fields, without the prefix. Shared by the
 * parser and the serializer so the set of fields the parser reads and the set of names an extension
 * may not use cannot drift apart.
 */
export const ENVELOPE_ATTRIBUTES: ReadonlySet<string> = new Set([
  "specversion",
  "id",
  "source",
  "type",
  "subject",
  "time",
  "datacontenttype",
  "dataschema",
  "traceparent",
  "tracestate",
  "ingestiontime",
]);
