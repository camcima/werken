import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { Encodings, PubSub, SchemaTypes } from "@google-cloud/pubsub";
import pg from "pg";

const PROJECT = process.env.GCP_PROJECT_ID;
const SCHEMA_ID = "shipment-events";
const TOPIC = "shipment-events";
const SUBSCRIPTION = "shipment-projection";
const DEAD_LETTER = "shipment-events-dead-letters";

const definition = readFileSync(fileURLToPath(new URL("../schema/shipment-events.avsc", import.meta.url)), "utf8");
const pubsub = new PubSub({ projectId: PROJECT });
const ignoreExists = (error) => {
  if (error.code !== 6) throw error;
};

await pubsub.createSchema(SCHEMA_ID, SchemaTypes.Avro, definition).catch(ignoreExists);
await pubsub
  .createTopic({
    name: TOPIC,
    schemaSettings: { schema: `projects/${PROJECT}/schemas/${SCHEMA_ID}`, encoding: Encodings.Json },
  })
  .catch(ignoreExists);
await pubsub.createTopic(DEAD_LETTER).catch(ignoreExists);

// Ordering has to be enabled on the *subscription*, not just at publish time. The publisher sets
// `ordering: true`, which makes `subject` the ordering key, but a subscription without this flag
// still fans the two events for one shipment out concurrently — so a cancellation can be projected
// before the ready that preceded it, and the ordering the example advertises is silently inert.
await pubsub.topic(TOPIC).createSubscription(SUBSCRIPTION, { enableMessageOrdering: true }).catch(ignoreExists);

// Checked rather than assumed, because `enableMessageOrdering` is create-only: Pub/Sub (and the
// emulator) reject an update to it as immutable, so a subscription left over from before this flag
// was set here would keep delivering out of order with nothing to show for it.
const [metadata] = await pubsub.subscription(SUBSCRIPTION).getMetadata();
if (metadata.enableMessageOrdering !== true) {
  throw new Error(
    `werken example: subscription ${SUBSCRIPTION} already exists without message ordering, and it ` +
      "cannot be added to an existing subscription. Delete it and re-run this script — for the " +
      "emulator, `docker compose down && docker compose up -d` is the quickest way.",
  );
}

await pubsub.close();

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
// `werken_processed_events` mirrors docs/idempotency-schema.sql, which is the canonical copy — the
// library never runs DDL. It never issues DELETEs either, so pruning is the consumer's job: nothing
// here or in the worker removes expired markers, and the table grows forever without a scheduled
//
//   DELETE FROM werken_processed_events WHERE expires_at < now();
//
// `pruneExpiredSql(table?)` is exported for exactly that statement. The expires_at index below
// serves both the read-side expiry filter and that delete.
await pool.query(`
  CREATE TABLE IF NOT EXISTS werken_processed_events (
    consumer     text        NOT NULL,
    source       text        NOT NULL,
    event_id     text        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    expires_at   timestamptz NOT NULL,
    PRIMARY KEY (consumer, source, event_id)
  );
  CREATE INDEX IF NOT EXISTS werken_processed_events_expires_at_idx
    ON werken_processed_events (expires_at);
  CREATE TABLE IF NOT EXISTS shipment_projection (
    shipment_id text PRIMARY KEY,
    status      text NOT NULL,
    carrier     text,
    updated_at  timestamptz NOT NULL
  );
`);
await pool.end();

console.log(`provisioned topic=${TOPIC} subscription=${SUBSCRIPTION} dead-letter=${DEAD_LETTER} + Postgres tables`);
