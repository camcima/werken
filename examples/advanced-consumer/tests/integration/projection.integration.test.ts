import { execFile, spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { PubSub } from "@google-cloud/pubsub";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { skipUnlessAvailable } from "@werken/test-support";

const run = promisify(execFile);
const EMULATOR = process.env.PUBSUB_EMULATOR_HOST;
const DATABASE_URL = process.env.DATABASE_URL;
const PROJECT = process.env.PUBSUB_PROJECT_ID ?? "werken-dev";
const CONSUMER = fileURLToPath(new URL("../../dist/main.worker.js", import.meta.url));
const PUBLISHER = fileURLToPath(new URL("../../../publisher/dist/main.js", import.meta.url));
const PROVISION = fileURLToPath(new URL("../../scripts/provision.mjs", import.meta.url));

const TOPIC = "shipment-events";
/** Matches `idempotency.consumer` in the example's `main.worker.ts`. */
const CONSUMER_NAME = "shipment-projection";
const MARKERS_SQL = "SELECT count(*)::int AS n FROM werken_processed_events WHERE consumer = $1";

/**
 * The advanced example's whole point is that its features are real: a schema-attached topic, a
 * Postgres idempotency store and a projection. None of that can be asserted without both backends,
 * so this is the only place it is covered.
 */
describe.skipIf(
  skipUnlessAvailable("PUBSUB_EMULATOR_HOST", EMULATOR) ||
    skipUnlessAvailable("DATABASE_URL", DATABASE_URL) ||
    skipUnlessAvailable("a built advanced-consumer (run `pnpm run build`)", existsSync(CONSUMER)) ||
    skipUnlessAvailable("a built publisher (run `pnpm run build`)", existsSync(PUBLISHER)),
)("advanced consumer projects published events", () => {
  const env = { ...process.env, GCP_PROJECT_ID: PROJECT };
  const pool = new pg.Pool({ connectionString: DATABASE_URL });
  const pubsub = new PubSub({ projectId: PROJECT });

  /**
   * Its own subscription per run, rather than the provisioned `shipment-projection`.
   *
   * The worker is killed the moment the projection is complete, which is before the SDK has
   * flushed the acks for the messages it just handled, so those come back as a backlog. Every run
   * starts by clearing the dedup markers, so the next run's worker cheerfully reprocesses them —
   * and because the payloads are identical, every assertion here still passes while measuring the
   * *previous* run's events. Draining a shared subscription for 25 runs turned up 75 stranded
   * messages, which is how that was found. A subscription deleted in afterAll cannot leak.
   */
  const subscriptionId = `shipment-projection-it-${Date.now()}`;

  beforeAll(async () => {
    // Creates the schema, the schema-attached topic, the dead-letter topic and both tables — and
    // fails loudly if any of it is wrong, so a broken provisioning script is caught here.
    await run(process.execPath, [PROVISION], { env });
    // Ordering mirrors what provision.mjs gives the real subscription.
    await pubsub.topic(TOPIC).createSubscription(subscriptionId, { enableMessageOrdering: true });

    await pool.query("DELETE FROM shipment_projection");
    // Scoped to this consumer, not a bare DELETE: `werken_processed_events` is the library's
    // default table, so the package's own SQL-store suite writes into the same rows from another
    // file, and vitest runs the two in parallel. Wiping the whole table would fail *its* tests.
    await pool.query("DELETE FROM werken_processed_events WHERE consumer = $1", [CONSUMER_NAME]);
  }, 60_000);

  afterAll(async () => {
    await pubsub
      .subscription(subscriptionId)
      .delete()
      .catch(() => {});
    await pubsub.close();
    await pool.end();
  });

  test("decodes Avro, projects rows, and records one dedup marker per event", async () => {
    const worker = spawn(process.execPath, [CONSUMER], {
      env: {
        ...env,
        PUBSUB_SUBSCRIPTION: subscriptionId,
        PUBSUB_DEAD_LETTER_TOPIC: "shipment-events-dead-letters",
        DATABASE_URL,
      },
      stdio: ["ignore", "pipe", "pipe"],
    });

    try {
      await run(process.execPath, [PUBLISHER], { env });

      // Waits on the dedup markers rather than on the row count, because two rows appear as soon
      // as any two of the three events land — including the state where s-1 is still `ready` and
      // its cancellation is in flight. A marker is written only after its handler succeeded, so
      // three of them mean all three events have been projected and nothing is outstanding.
      const deadline = Date.now() + 60_000;
      let processed = 0;
      while (Date.now() < deadline) {
        processed = (await pool.query<{ n: number }>(MARKERS_SQL, [CONSUMER_NAME])).rows[0].n;
        if (processed >= 3) break;
        await new Promise((r) => setTimeout(r, 250));
      }

      // One marker per published event proves the SQL store was really written and not bypassed —
      // the publisher sends exactly three, and this subscription carries nothing else.
      expect(processed).toBe(3);

      const rows = (
        await pool.query<{ shipment_id: string; status: string }>(
          "SELECT shipment_id, status FROM shipment_projection ORDER BY shipment_id",
        )
      ).rows;

      expect(rows.map((r) => r.shipment_id)).toEqual(["s-1", "s-2"]);
      // Only one event ever touched s-2, so this is the wildcard route's output and nothing else's.
      expect(rows.find((r) => r.shipment_id === "s-2")?.status).toBe("ready");

      // Deliberately NOT asserted: that s-1 settles on `cancelled`. It does on real Pub/Sub, where
      // an ordering key admits one unacked message per key at a time, so the ready is fully
      // processed before the cancellation is delivered. The emulator does not implement that
      // back-pressure — it hands every message for a key to the subscriber at once — so the two
      // writes to the s-1 row race and whichever reaches Postgres last wins. Asserting the settled
      // value here fails about one run in ten for a reason that exists only in the emulator. That
      // the exact route beats the wildcard is covered in ../shipment-events.consumer.test.ts,
      // through the same DI and the same pipeline.
      expect(["ready", "cancelled"]).toContain(rows.find((r) => r.shipment_id === "s-1")?.status);
    } finally {
      worker.kill("SIGTERM");
    }
  }, 120_000);
});
