import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import avro from "avsc";

/**
 * The READER schema — the shape this consumer was compiled against. Never the writer schema from
 * the registry: resolution is what lets a producer add a field without breaking us, and reading
 * with the writer's own type would silently adopt whatever it changed.
 */
const DEFINITION = readFileSync(fileURLToPath(new URL("../../schema/shipment-events.avsc", import.meta.url)), "utf8");

const SHIPMENT_EVENT = avro.Type.forSchema(JSON.parse(DEFINITION) as avro.Schema);

/**
 * Pub/Sub passes the fully-qualified schema name (`projects/p/schemas/name`). This service reads one
 * stream, so any schema on its topic resolves to the one reader type; returning undefined would
 * make the codec fail closed, which is what we want for anything unexpected.
 */
export function readerTypeFor(schemaName: string): avro.Type | undefined {
  // The leading slash matters: a bare "shipment-events" suffix would also claim an unrelated
  // `legacy-shipment-events` schema and decode it with the wrong reader type.
  return schemaName.endsWith("/shipment-events") ? SHIPMENT_EVENT : undefined;
}
