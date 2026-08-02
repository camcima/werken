import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { PubSub } from "@google-cloud/pubsub";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { toPubSubAttributes } from "@werken/cloudevents";

const EMULATOR = process.env.PUBSUB_EMULATOR_HOST;
const PROJECT = process.env.PUBSUB_PROJECT_ID ?? "werken-it";
const TYPE = "com.example.slow.v1";
const WORKER = fileURLToPath(new URL("./sigterm-worker.mjs", import.meta.url));

/**
 * Acceptance criterion 5: SIGTERM mid-flight drains cleanly — zero message loss, zero acks of
 * unprocessed work. This runs a real child process against the emulator and signals it for real,
 * because the failure it guards against is the single most likely source of production duplicates
 * and the least likely thing to be caught by accident.
 */
describe.skipIf(!EMULATOR)("SIGTERM mid-flight", () => {
  const suffix = Date.now();
  const topicId = `werken-sigterm-topic-${suffix}`;
  const subscriptionId = `werken-sigterm-sub-${suffix}`;
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

  async function publish(id: string) {
    await pubsub.topic(topicId).publishMessage({
      data: Buffer.from(JSON.stringify({ id })),
      attributes: toPubSubAttributes({
        specversion: "1.0",
        id,
        source: "https://example.test/service",
        type: TYPE,
        datacontenttype: "application/json",
        extensions: {},
      }),
    });
  }

  function runWorker(env: Record<string, string>) {
    const events: Array<Record<string, unknown>> = [];
    const child = spawn(process.execPath, [WORKER], {
      env: { ...process.env, WERKEN_SUBSCRIPTION: subscriptionId, PUBSUB_PROJECT_ID: PROJECT, ...env },
      stdio: ["ignore", "pipe", "pipe"],
    });
    child.stdout.on("data", (chunk: Buffer) => {
      for (const line of chunk.toString().split("\n").filter(Boolean)) {
        try {
          events.push(JSON.parse(line));
        } catch {
          /* ignore non-JSON diagnostics */
        }
      }
    });
    const exited = new Promise<number>((resolve) => child.on("exit", (code) => resolve(code ?? -1)));
    const seen = (event: string) => events.some((e) => e.event === event);
    return { child, events, exited, seen };
  }

  const waitFor = async (predicate: () => boolean, timeoutMs: number, what: string) => {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (predicate()) return;
      await new Promise((r) => setTimeout(r, 50));
    }
    throw new Error(`timed out waiting for ${what}`);
  };

  test("lets an in-flight handler finish, then exits cleanly", async () => {
    const worker = runWorker({ WERKEN_HANDLER_MS: "2000", WERKEN_DRAIN_MS: "15000" });
    await waitFor(() => worker.seen("listening"), 30_000, "worker to start listening");

    await publish("sigterm-completes");
    await waitFor(() => worker.seen("handler:start"), 30_000, "handler to start");

    // Signal while the handler is definitely still running.
    worker.child.kill("SIGTERM");
    await waitFor(() => worker.seen("sigterm"), 10_000, "SIGTERM to be received");

    expect(await worker.exited).toBe(0);
    // The drain waited rather than cutting the handler off.
    expect(worker.seen("handler:finish")).toBe(true);
    expect(worker.seen("drained")).toBe(true);

    const order = worker.events.map((e) => e.event);
    expect(order.indexOf("handler:finish")).toBeLessThan(order.indexOf("drained"));
  }, 90_000);

  test("redelivers work it could not finish, rather than acking it", async () => {
    // Handler outlives the drain budget, so the message must come back — not be lost.
    const worker = runWorker({ WERKEN_HANDLER_MS: "60000", WERKEN_DRAIN_MS: "500" });
    await waitFor(() => worker.seen("listening"), 30_000, "worker to start listening");

    await publish("sigterm-redelivers");
    await waitFor(() => worker.seen("handler:start"), 30_000, "handler to start");

    worker.child.kill("SIGTERM");
    expect(await worker.exited).toBe(0);
    expect(worker.seen("handler:finish")).toBe(false);

    // Zero message loss: a fresh subscriber receives it again.
    const redelivered = await new Promise<boolean>((resolve) => {
      const sub = pubsub.subscription(subscriptionId);
      const timer = setTimeout(() => {
        void sub.close();
        resolve(false);
      }, 30_000);
      sub.on("message", (m) => {
        clearTimeout(timer);
        m.ack();
        void sub.close();
        resolve(true);
      });
      sub.on("error", () => {
        clearTimeout(timer);
        resolve(false);
      });
    });

    expect(redelivered).toBe(true);
  }, 120_000);
});
