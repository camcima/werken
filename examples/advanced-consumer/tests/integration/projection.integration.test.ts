import { execFile, spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { PubSub } from "@google-cloud/pubsub";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { DEAD_LETTER_ATTRIBUTES } from "@werken/nestjs-google-pubsub";
import { skipUnlessAvailable } from "@werken/test-support";
import { resetPubSubFixtures, tidyPubSubFixtures } from "@werken/test-support/pubsub";

const run = promisify(execFile);
const EMULATOR = process.env.PUBSUB_EMULATOR_HOST;
const DATABASE_URL = process.env.DATABASE_URL;
const PROJECT = process.env.PUBSUB_PROJECT_ID ?? "werken-dev";
const CONSUMER = fileURLToPath(new URL("../../dist/main.worker.js", import.meta.url));
const PUBLISHER = fileURLToPath(new URL("../../../publisher/dist/main.js", import.meta.url));
const PROVISION = fileURLToPath(new URL("../../scripts/provision.mjs", import.meta.url));

const TOPIC = "shipment-events";
const DEAD_LETTER_TOPIC = "shipment-events-dead-letters";
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
   * Its own subscription, rather than the provisioned `shipment-projection`.
   *
   * The worker is killed the moment the projection is complete, which is before the SDK has
   * flushed the acks for the messages it just handled, so those come back as a backlog. Every run
   * starts by clearing the dedup markers, so the next run's worker cheerfully reprocesses them —
   * and because the payloads are identical, every assertion here still passes while measuring the
   * *previous* run's events. Draining a shared subscription for 25 runs turned up 75 stranded
   * messages, which is how that was found.
   *
   * The name is fixed rather than suffixed per run, and beforeAll deletes it before recreating it.
   * That is what discards the backlog, and unlike deleting it in afterAll it also holds when the
   * previous run never reached afterAll at all.
   */
  const subscriptionId = "shipment-projection-it";

  /**
   * Where anything the worker gives up on lands. Created here rather than in provision.mjs, and
   * before the worker starts, because Pub/Sub fans a message out only to the subscriptions that
   * already exist when it is published — a subscription attached afterwards sees nothing and would
   * make "no dead letters" true by construction. Recreated for the same reason as the one above:
   * a previous run's dead letter still sitting here would fail this run's `toEqual([])`.
   */
  const deadLetterSubId = "shipment-dead-letters-it";

  // Only the subscriptions. The topics and the schema belong to provision.mjs, which is idempotent
  // and is run below — deleting them here would be this file reaching into another file's fixtures.
  const fixtures = { subscriptions: [subscriptionId, deadLetterSubId] };

  beforeAll(async () => {
    // Creates the schema, the schema-attached topic, the dead-letter topic and both tables — and
    // fails loudly if any of it is wrong, so a broken provisioning script is caught here.
    await run(process.execPath, [PROVISION], { env });
    // Reset before create, not create-and-hope: `enableMessageOrdering` is fixed at creation, so a
    // leftover subscription from before that flag was set here could not be corrected in place.
    await resetPubSubFixtures(pubsub, fixtures);
    // Ordering mirrors what provision.mjs gives the real subscription.
    await pubsub.topic(TOPIC).createSubscription(subscriptionId, { enableMessageOrdering: true });
    await pubsub.topic(DEAD_LETTER_TOPIC).createSubscription(deadLetterSubId);

    // The Postgres fixtures were already reset here rather than after the run, and the Pub/Sub ones
    // above now follow the same rule — one discipline for the whole file.
    await pool.query("DELETE FROM shipment_projection");
    // Scoped to this consumer, not a bare DELETE: `werken_processed_events` is the library's
    // default table, so the package's own SQL-store suite writes into the same rows from another
    // file, and vitest runs the two in parallel. Wiping the whole table would fail *its* tests.
    await pool.query("DELETE FROM werken_processed_events WHERE consumer = $1", [CONSUMER_NAME]);
  }, 60_000);

  afterAll(async () => {
    await tidyPubSubFixtures(pubsub, fixtures);
    await pubsub.close();
    await pool.end();
  });

  test("decodes Avro, projects rows, and records one dedup marker per event", async () => {
    const deadLettered: string[] = [];
    let subscriberError: unknown;
    const deadLetters = pubsub.subscription(deadLetterSubId);
    deadLetters.on("message", (m) => {
      deadLettered.push(
        `${m.attributes["ce-type"]} ${m.attributes["ce-subject"]}: ` +
          `${m.attributes[DEAD_LETTER_ATTRIBUTES.stage]}/${m.attributes[DEAD_LETTER_ATTRIBUTES.reason]}`,
      );
      m.ack();
    });
    // Without this, a stream error arrives as an unhandled 'error' event and takes the whole run
    // down with a stack trace that names neither this test nor the subscription.
    deadLetters.on("error", (e) => (subscriberError = e));

    const worker = spawn(process.execPath, [CONSUMER], {
      env: {
        ...env,
        PUBSUB_SUBSCRIPTION: subscriptionId,
        PUBSUB_DEAD_LETTER_TOPIC: DEAD_LETTER_TOPIC,
        DATABASE_URL,
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    // Drained, not just piped: an unread pipe blocks the child once ~64 KB has accumulated in it,
    // and the transcript is the only diagnostic there is when the worker fails to reach three
    // markers.
    let output = "";
    worker.stdout.on("data", (chunk: Buffer) => (output += chunk.toString()));
    worker.stderr.on("data", (chunk: Buffer) => (output += chunk.toString()));

    try {
      await run(process.execPath, [PUBLISHER], { env });

      // Waits on the dedup markers rather than on the row count, because two rows appear as soon
      // as any two of the three events land — including the state where s-1 is still `ready` and
      // its cancellation is in flight. A marker is written once an event is retired, so three of
      // them mean nothing is still outstanding and the projection has stopped moving.
      //
      // Retired is not the same as handled: pipeline.ts also records a marker for an event its
      // handler dead-lettered, so that a redelivery cannot publish a second copy. The dead-letter
      // assertion below is what tells the two apart.
      const deadline = Date.now() + 60_000;
      let processed = 0;
      while (Date.now() < deadline) {
        // A worker that dies on startup — a bad DATABASE_URL, a subscription that is not there —
        // would otherwise sit here for the full 60s and then report `expected 0 to be 3` with
        // nothing to debug from.
        if (worker.exitCode !== null) throw new Error(`worker exited early (${worker.exitCode}):\n${output}`);
        processed = (await pool.query<{ n: number }>(MARKERS_SQL, [CONSUMER_NAME])).rows[0].n;
        if (processed >= 3) break;
        await new Promise((r) => setTimeout(r, 250));
      }

      // One marker per published event proves the SQL store was really written and not bypassed —
      // the publisher sends exactly three, and this subscription carries nothing else.
      expect(processed, `worker output:\n${output}`).toBe(3);

      // Nothing was given up on. A handler that throws TerminalEventError still retires its event
      // and still leaves three markers, two projection rows and s-2 on `ready` — so every other
      // assertion here passes while a third of the stream went to the dead-letter topic. The
      // dead-letter publish completes before the marker that retires the message is written, and
      // this subscription predates the worker, so anything dead-lettered is already in its backlog
      // by the time the count reached three; the second is for the emulator's delivery hop only.
      const quietUntil = Date.now() + 1_000;
      while (Date.now() < quietUntil && deadLettered.length === 0) {
        await new Promise((r) => setTimeout(r, 50));
      }
      expect(deadLettered).toEqual([]);
      expect(subscriberError).toBeUndefined();

      const rows = (
        await pool.query<{ shipment_id: string; status: string; carrier: string | null }>(
          "SELECT shipment_id, status, carrier FROM shipment_projection ORDER BY shipment_id",
        )
      ).rows;

      expect(rows.map((r) => r.shipment_id)).toEqual(["s-1", "s-2"]);
      const s2 = rows.find((r) => r.shipment_id === "s-2");
      // Only one event ever touched s-2, so this is the wildcard route's output and nothing else's.
      expect(s2?.status).toBe("ready");

      // The only assertion here that Avro decoding actually happened. `carrier` is a nullable
      // union, so the publisher puts it on the wire as Avro JSON — {"carrier":{"string":"ups"}},
      // not {"carrier":"ups"} — and unwrapping it to a bare string is schema resolution's doing.
      // A plain JSON.parse hands the handler the wrapper object, which node-postgres stringifies
      // into the column as {"string":"ups"}. shipment_id and status are byte-identical under both
      // paths, so without this line, deleting `schemaRegistry` from main.worker.ts leaves the
      // whole suite green. s-2 rather than s-1 because exactly one event ever touches it, which
      // makes the value deterministic on an emulator that does not enforce ordering-key
      // back-pressure.
      expect(s2?.carrier).toBe("ups");

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
      // Awaited, not fire-and-forget: afterAll deletes the subscription this worker is still
      // draining from, and an un-awaited kill leaks the process into later runs. SIGKILL after the
      // drain budget so a wedged worker cannot hang the suite either.
      worker.kill("SIGTERM");
      await new Promise<void>((resolve) => {
        const forceKill = setTimeout(() => worker.kill("SIGKILL"), 35_000);
        worker.on("exit", () => {
          clearTimeout(forceKill);
          resolve();
        });
      });
      await deadLetters.close();
    }
  }, 120_000);
});
