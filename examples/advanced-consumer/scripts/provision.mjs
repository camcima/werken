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
await pubsub.topic(TOPIC).createSubscription(SUBSCRIPTION).catch(ignoreExists);
await pubsub.close();

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
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
