import { PubSub } from "@google-cloud/pubsub";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { toPubSubAttributes } from "@werken/cloudevents";
import { DEAD_LETTER_ATTRIBUTES, TerminalEventError, WerkenPubSubTransport } from "@werken/nestjs-google-pubsub";
import type { PubSubClientLike } from "@werken/nestjs-google-pubsub";
import { skipUnlessAvailable } from "@werken/test-support";
import { resetPubSubFixtures, tidyPubSubFixtures } from "@werken/test-support/pubsub";

const EMULATOR = process.env.PUBSUB_EMULATOR_HOST;
const PROJECT = process.env.PUBSUB_PROJECT_ID ?? "werken-it";
const TYPE = "com.example.terminal.v1";

/**
 * Dead-lettering against a real broker.
 *
 * The unit tests assert against a fake topic whose publishMessage I wrote, so they can only confirm
 * I called it with what I intended. Whether Pub/Sub actually preserves the original attributes
 * alongside the provenance ones, and whether the original is really acked rather than redelivered,
 * are contracts with the broker.
 */
describe.skipIf(skipUnlessAvailable("PUBSUB_EMULATOR_HOST", EMULATOR))("dead-lettering against the emulator", () => {
  // Fixed, and specific to this file. See @werken/test-support/pubsub for why these are not
  // suffixed per run and why beforeAll deletes before it creates.
  const topicId = "werken-dl-src";
  const subscriptionId = "werken-dl-sub";
  const deadLetterTopicId = "werken-dl-dest";
  const deadLetterSubId = "werken-dl-dest-sub";
  const fixtures = {
    subscriptions: [subscriptionId, deadLetterSubId],
    topics: [topicId, deadLetterTopicId],
  };
  const pubsub = new PubSub({ projectId: PROJECT });
  let transport: WerkenPubSubTransport;

  beforeAll(async () => {
    // Recreating the dead-letter subscription is what keeps the assertion honest: a leftover one
    // still holds the previous run's dead letters, whose attributes are identical, so the test
    // would pass on a message this run never produced.
    await resetPubSubFixtures(pubsub, fixtures);
    await pubsub.createTopic(topicId);
    await pubsub.topic(topicId).createSubscription(subscriptionId);
    await pubsub.createTopic(deadLetterTopicId);
    await pubsub.topic(deadLetterTopicId).createSubscription(deadLetterSubId);
  });

  afterAll(async () => {
    await transport?.close();
    await tidyPubSubFixtures(pubsub, fixtures);
    await pubsub.close();
  });

  test("publishes a terminal message with provenance and acks the original", async () => {
    transport = new WerkenPubSubTransport({
      projectId: PROJECT,
      subscription: subscriptionId,
      deadLetterTopic: deadLetterTopicId,
      createClient: () => new PubSub({ projectId: PROJECT }) as unknown as PubSubClientLike,
    });
    transport.addHandler(
      TYPE,
      (() => {
        throw new TerminalEventError("references an entity that will never exist");
      }) as never,
      true,
    );

    await new Promise<void>((resolve, reject) => {
      transport.listen((error?: unknown) => (error ? reject(error) : resolve()));
    });

    const received = new Promise<{ attributes: Record<string, string>; data: string }>((resolve, reject) => {
      const sub = pubsub.subscription(deadLetterSubId);
      const timer = setTimeout(() => reject(new Error("no dead-lettered message arrived")), 25_000);
      sub.on("message", (m) => {
        clearTimeout(timer);
        m.ack();
        void sub.close();
        resolve({ attributes: { ...m.attributes }, data: m.data.toString() });
      });
      sub.on("error", (e) => {
        clearTimeout(timer);
        reject(e);
      });
    });

    await pubsub.topic(topicId).publishMessage({
      data: Buffer.from(JSON.stringify({ ref: "missing" })),
      attributes: toPubSubAttributes({
        specversion: "1.0",
        id: "01931b7c-3f2a-7000-8000-0000000000dl",
        source: "https://example.test/service",
        type: TYPE,
        subject: "entity-1",
        datacontenttype: "application/json",
        extensions: { tenantid: "acme" },
      }),
    });

    const dead = await received;

    // Original body and every original attribute survive the round trip.
    expect(dead.data).toBe(JSON.stringify({ ref: "missing" }));
    expect(dead.attributes["ce-type"]).toBe(TYPE);
    expect(dead.attributes["ce-subject"]).toBe("entity-1");
    expect(dead.attributes["ce-tenantid"]).toBe("acme");

    // Plus provenance.
    expect(dead.attributes[DEAD_LETTER_ATTRIBUTES.reason]).toContain("never exist");
    expect(dead.attributes[DEAD_LETTER_ATTRIBUTES.stage]).toBe("handler");
    expect(dead.attributes[DEAD_LETTER_ATTRIBUTES.sourceSubscription]).toBe(subscriptionId);
    expect(Date.parse(dead.attributes[DEAD_LETTER_ATTRIBUTES.timestamp])).not.toBeNaN();
  }, 60_000);
});
