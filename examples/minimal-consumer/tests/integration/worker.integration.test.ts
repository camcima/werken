import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { PubSub } from "@google-cloud/pubsub";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { toPubSubAttributes } from "@werken/cloudevents";
import { skipUnlessAvailable } from "@werken/test-support";

const EMULATOR = process.env.PUBSUB_EMULATOR_HOST;
const PROJECT = process.env.PUBSUB_PROJECT_ID ?? "werken-it";
const TYPE = "com.example.shipment.ready.v1";
const WORKER = fileURLToPath(new URL("../../dist/main.worker.js", import.meta.url));

/**
 * The README calls examples/minimal-consumer a complete, runnable service, and its documented path
 * is `pnpm build` then `node dist/main.worker.js`. Nothing else exercises that: the unit run
 * imports the example's *source*, so a config that type-checks but emits nothing — or emits
 * nothing runnable — passes every other gate and fails the first time a reader follows the README.
 *
 * The missing-dist case is deliberately routed through skipUnlessAvailable rather than a plain
 * assertion, so CI (WERKEN_REQUIRE_INTEGRATION=1) fails loudly while a local `pnpm test` without a
 * build still works.
 */
describe.skipIf(
  skipUnlessAvailable("PUBSUB_EMULATOR_HOST", EMULATOR) ||
    skipUnlessAvailable("a built examples/minimal-consumer (run `pnpm run build`)", existsSync(WORKER)),
)("the worked example builds and runs", () => {
  const suffix = Date.now();
  const topicId = `werken-example-topic-${suffix}`;
  const subscriptionId = `werken-example-sub-${suffix}`;
  const pubsub = new PubSub({ projectId: PROJECT });

  beforeAll(async () => {
    await pubsub.createTopic(topicId);
    await pubsub.topic(topicId).createSubscription(subscriptionId, { ackDeadlineSeconds: 60 });
  });

  afterAll(async () => {
    await pubsub
      .subscription(subscriptionId)
      .delete()
      .catch(() => {});
    await pubsub
      .topic(topicId)
      .delete()
      .catch(() => {});
    await pubsub.close();
  });

  test("starts from dist and dispatches a shipment the handler recognises", async () => {
    const child = spawn(process.execPath, [WORKER], {
      env: {
        ...process.env,
        GCP_PROJECT_ID: PROJECT,
        PUBSUB_SUBSCRIPTION: subscriptionId,
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let output = "";
    child.stdout.on("data", (chunk: Buffer) => (output += chunk.toString()));
    child.stderr.on("data", (chunk: Buffer) => (output += chunk.toString()));

    try {
      // Published straight away: the subscription retains it until the worker attaches, so this
      // needs no readiness handshake with the child.
      await pubsub.topic(topicId).publishMessage({
        data: Buffer.from(JSON.stringify({ shipmentId: "known-1", carrier: "dhl" })),
        attributes: toPubSubAttributes({
          specversion: "1.0",
          id: `example-${suffix}`,
          source: "https://example.test/orders",
          type: TYPE,
          datacontenttype: "application/json",
          extensions: {},
        }),
      });

      const deadline = Date.now() + 60_000;
      while (Date.now() < deadline && !output.includes("dispatched known-1 via dhl")) {
        if (child.exitCode !== null) throw new Error(`worker exited early (${child.exitCode}):\n${output}`);
        await new Promise((r) => setTimeout(r, 100));
      }

      expect(output).toContain("dispatched known-1 via dhl");
    } finally {
      child.kill("SIGTERM");
    }
  }, 90_000);
});
