import { context, propagation, trace } from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { PartialPublishError, createEventPublisher } from "@werken/nestjs-google-pubsub";
import type { PubSubClientLike } from "@werken/nestjs-google-pubsub";

const TYPE = "com.example.thing.happened.v1";
const SOURCE = "https://example.test/service";

interface Published {
  topic: string;
  topicOptions?: unknown;
  data: Buffer;
  attributes: Record<string, string>;
  orderingKey?: string;
}

function fakeClient() {
  const published: Published[] = [];
  /** One entry per `client.topic()` call, so Topic reuse can be asserted. */
  const topicCalls: string[] = [];
  /** One entry per `resumePublishing` call, so ordering-key recovery can be asserted. */
  const resumed: Array<{ topic: string; orderingKey: string }> = [];
  let nextId = 0;
  const client = {
    topic: vi.fn((topic: string, topicOptions?: unknown) => {
      topicCalls.push(topic);
      return {
        publishMessage: vi.fn(async (m: { data: Buffer; attributes: Record<string, string>; orderingKey?: string }) => {
          published.push({ topic, topicOptions, ...m });
          return `msg-${++nextId}`;
        }),
        resumePublishing: vi.fn((orderingKey: string) => {
          resumed.push({ topic, orderingKey });
        }),
      };
    }),
    subscription: vi.fn(),
    close: vi.fn(async () => {}),
  };
  return { published, topicCalls, resumed, client: client as unknown as PubSubClientLike };
}

function publisherWith(overrides: Partial<Parameters<typeof createEventPublisher>[0]> = {}) {
  const { published, topicCalls, resumed, client } = fakeClient();
  const publisher = createEventPublisher({
    source: SOURCE,
    client,
    topicResolver: (type: string) => type.replaceAll(".", "-"),
    ...overrides,
  });
  return { publisher, published, topicCalls, resumed };
}

/**
 * A client whose publishes reject, so ordering-key recovery can be asserted. `shouldFail` is given
 * the 1-based attempt number, letting a test fail some publishes in a batch and not others.
 */
function failingClient(shouldFail: (attempt: number) => boolean = () => true) {
  const resumed: Array<{ topic: string; orderingKey: string }> = [];
  let attempt = 0;
  const client = {
    topic: vi.fn((topic: string) => ({
      publishMessage: vi.fn(async () => {
        attempt++;
        if (shouldFail(attempt)) throw new Error(`publish ${attempt} failed`);
        return `msg-${attempt}`;
      }),
      resumePublishing: vi.fn((orderingKey: string) => {
        resumed.push({ topic, orderingKey });
      }),
    })),
    subscription: vi.fn(),
    close: vi.fn(async () => {}),
  };
  return { resumed, client: client as unknown as PubSubClientLike };
}

const originalEnv = process.env.NODE_ENV;
afterEach(() => {
  process.env.NODE_ENV = originalEnv;
});

describe("envelope construction", () => {
  test("returns the published message id", async () => {
    const { publisher } = publisherWith();

    expect(await publisher.publish({ type: TYPE, data: { hello: "world" } })).toBe("msg-1");
  });

  test("generates a time-ordered v7 event id", async () => {
    const { publisher, published } = publisherWith();
    await publisher.publish({ type: TYPE, data: {} });

    const id = published[0].attributes["ce-id"];
    expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  });

  // The reason for v7 over v4: ce-id becomes the idempotency table's primary key, so ids arriving
  // in time order keep inserts at the right-hand edge of the index.
  test("generates ids that sort in publication order even within a millisecond", async () => {
    const { publisher, published } = publisherWith();
    await publisher.publishBatch(Array.from({ length: 200 }, () => ({ type: TYPE, data: {} })));

    const ids = published.map((p) => p.attributes["ce-id"]);
    expect([...ids].sort()).toEqual(ids);
  });

  // The outbox pattern mints the id inside the ingest transaction and stores it on the row, so a
  // relay that crashes between publishing and marking republishes under the same id and consumer
  // de-duplication collapses the two. Generated here instead, a republish is a second event.
  test("lets the caller supply the event id", async () => {
    const { publisher, published } = publisherWith();
    await publisher.publish({ type: TYPE, data: {}, id: "01927f9a-0000-7000-8000-000000000001" });

    expect(published[0].attributes["ce-id"]).toBe("01927f9a-0000-7000-8000-000000000001");
  });

  // Falling back to uuidv7() would reinstate the exact duplicate this option exists to prevent, and
  // do it silently: toPubSubAttributes writes ce-id verbatim, so an empty one is not caught until
  // the consumer rejects the envelope as missing-attribute, blaming the wrong side of the wire.
  test.each([
    ["empty", ""],
    ["whitespace-only", "   "],
  ])("rejects an %s id rather than generating one behind the caller's back", async (_label, id) => {
    const { publisher } = publisherWith();

    await expect(publisher.publish({ type: TYPE, data: {}, id })).rejects.toThrow(/PublishRequest\.id/);
  });

  test("sets the configured source, and lets a request override it", async () => {
    const { publisher, published } = publisherWith();
    await publisher.publish({ type: TYPE, data: {} });
    await publisher.publish({ type: TYPE, data: {}, source: "https://other.test/svc" });

    expect(published[0].attributes["ce-source"]).toBe(SOURCE);
    expect(published[1].attributes["ce-source"]).toBe("https://other.test/svc");
  });

  test("defaults ce-time to now and honours an explicit occurrence time", async () => {
    const now = new Date("2026-08-03T10:00:00.000Z");
    const { publisher, published } = publisherWith({ now: () => now });

    await publisher.publish({ type: TYPE, data: {} });
    await publisher.publish({ type: TYPE, data: {}, time: new Date("2026-08-01T09:00:00.000Z") });

    expect(published[0].attributes["ce-time"]).toBe("2026-08-03T10:00:00.000Z");
    expect(published[1].attributes["ce-time"]).toBe("2026-08-01T09:00:00.000Z");
  });

  // ce-time is when it happened; ingestiontime is when the platform learned of it. Reasoning about
  // lateness needs both, so the publisher stamps the second one itself unless told otherwise.
  test("defaults ingestiontime to publish time", async () => {
    const now = new Date("2026-08-03T10:00:00.000Z");
    const { publisher, published } = publisherWith({ now: () => now });

    await publisher.publish({ type: TYPE, data: {}, time: new Date("2026-08-01T09:00:00.000Z") });

    expect(published[0].attributes["ce-ingestiontime"]).toBe("2026-08-03T10:00:00.000Z");
  });

  // A relay publishes what its ingest transaction committed earlier. Pinning ingestiontime to
  // publish time would fold the queueing delay away, leaving ingest lag and relay lag
  // indistinguishable in a per-stage lead-time SLI — and arbitrarily wrong when the relay backs up.
  test("lets the caller supply ingestiontime, for events published by a relay", async () => {
    const now = new Date("2026-08-03T10:00:00.000Z");
    const { publisher, published } = publisherWith({ now: () => now });

    await publisher.publish({
      type: TYPE,
      data: {},
      time: new Date("2026-08-01T09:00:00.000Z"),
      ingestiontime: new Date("2026-08-01T09:00:02.000Z"),
    });

    expect(published[0].attributes["ce-ingestiontime"]).toBe("2026-08-01T09:00:02.000Z");
    // The occurrence time is untouched — the two answer different questions.
    expect(published[0].attributes["ce-time"]).toBe("2026-08-01T09:00:00.000Z");
  });

  test("carries subject, dataschema and extensions through", async () => {
    const { publisher, published } = publisherWith();
    await publisher.publish({
      type: TYPE,
      data: {},
      subject: "thing-42",
      dataschema: "https://schemas.example.test/thing/v1",
      extensions: { tenantid: "acme" },
    });

    expect(published[0].attributes["ce-subject"]).toBe("thing-42");
    expect(published[0].attributes["ce-dataschema"]).toBe("https://schemas.example.test/thing/v1");
    expect(published[0].attributes["ce-tenantid"]).toBe("acme");
  });

  test("sets specversion 1.0", async () => {
    const { publisher, published } = publisherWith();
    await publisher.publish({ type: TYPE, data: {} });

    expect(published[0].attributes["ce-specversion"]).toBe("1.0");
  });
});

describe("topic resolution", () => {
  test("resolves the topic from the event type", async () => {
    const { publisher, published } = publisherWith();
    await publisher.publish({ type: TYPE, data: {} });

    expect(published[0].topic).toBe("com-example-thing-happened-v1");
  });

  test("lets an explicit topic override the resolver", async () => {
    const { publisher, published } = publisherWith();
    await publisher.publish({ type: TYPE, data: {} }, { topic: "explicit-topic" });

    expect(published[0].topic).toBe("explicit-topic");
  });

  test("fails clearly when no topic can be resolved for a type", async () => {
    const { publisher } = publisherWith({ topicResolver: () => undefined });

    await expect(publisher.publish({ type: TYPE, data: {} })).rejects.toThrow(new RegExp(TYPE));
  });

  test("applies the resource prefix to the resolved topic", async () => {
    const { publisher, published } = publisherWith({ resourcePrefix: "alice" });
    await publisher.publish({ type: TYPE, data: {} });

    expect(published[0].topic).toBe("alice-com-example-thing-happened-v1");
  });

  // A scoped consumer reading from an unscoped topic is worse than no scoping, so the publisher
  // enforces the same production guard as the transport.
  test("refuses to scope in production", () => {
    process.env.NODE_ENV = "production";

    expect(() => publisherWith({ resourcePrefix: "alice" })).toThrow(/production/i);
  });

  test("allows scoping in production behind the explicit escape hatch", () => {
    process.env.NODE_ENV = "production";

    expect(() => publisherWith({ resourcePrefix: "alice", allowUnsafeResourcePrefix: true })).not.toThrow();
  });
});

describe("ordering", () => {
  test("does not set an ordering key when ordering is off", async () => {
    const { publisher, published } = publisherWith();
    await publisher.publish({ type: TYPE, data: {}, subject: "thing-42" });

    expect(published[0].orderingKey).toBeUndefined();
  });

  test("derives the ordering key from subject when ordering is on", async () => {
    const { publisher, published } = publisherWith({ ordering: true });
    await publisher.publish({ type: TYPE, data: {}, subject: "thing-42" });

    expect(published[0].orderingKey).toBe("thing-42");
  });

  test("lets an explicit ordering key win", async () => {
    const { publisher, published } = publisherWith({ ordering: true });
    await publisher.publish({ type: TYPE, data: {}, subject: "thing-42", orderingKey: "custom" });

    expect(published[0].orderingKey).toBe("custom");
  });

  // The SDK ignores orderingKey unless the Topic itself was constructed with messageOrdering.
  test("constructs the topic with messageOrdering enabled", async () => {
    const { publisher, published } = publisherWith({ ordering: true });
    await publisher.publish({ type: TYPE, data: {}, subject: "thing-42" });

    expect(published[0].topicOptions).toMatchObject({ messageOrdering: true });
  });
});

describe("ordering-key recovery", () => {
  const RESOLVED_TOPIC = "com-example-thing-happened-v1";

  // The SDK suspends an ordering key the moment a keyed publish fails, and then rejects every later
  // message on that key. Without resuming, one failure silences the key for the publisher's whole
  // lifetime — permanent silence traded for a recoverable ordering hazard.
  test("resumes the ordering key when a keyed publish fails, and still raises the original error", async () => {
    const { resumed, client } = failingClient();
    const { publisher } = publisherWith({ client, ordering: true });

    await expect(publisher.publish({ type: TYPE, data: {}, subject: "thing-42" })).rejects.toThrow("publish 1 failed");

    expect(resumed).toEqual([{ topic: RESOLVED_TOPIC, orderingKey: "thing-42" }]);
  });

  test("does not resume when the request carried no ordering key", async () => {
    const { resumed, client } = failingClient();
    const { publisher } = publisherWith({ client, ordering: true });

    await expect(publisher.publish({ type: TYPE, data: {} })).rejects.toThrow("publish 1 failed");

    expect(resumed).toEqual([]);
  });

  test("resumes each distinct key once per batch, not once per failure", async () => {
    const { resumed, client } = failingClient();
    const { publisher } = publisherWith({ client, ordering: true });

    await publisher
      .publishBatch([
        { type: TYPE, data: {}, subject: "thing-42" },
        { type: TYPE, data: {}, subject: "thing-42" },
        { type: TYPE, data: {}, subject: "other" },
      ])
      .catch(() => {});

    expect(resumed).toEqual([
      { topic: RESOLVED_TOPIC, orderingKey: "thing-42" },
      { topic: RESOLVED_TOPIC, orderingKey: "other" },
    ]);
  });

  // Resuming from the failing publish's own error path would lift the suspension while its
  // batch-mates are still in flight on that key, letting a later message in the same batch go out
  // ahead of the one that failed — the inversion `ordering` was turned on to prevent.
  test("does not resume until every publish in the batch has settled", async () => {
    const order: string[] = [];
    let release: (() => void) | undefined;
    const gate = new Promise<void>((r) => (release = r));
    let attempt = 0;
    const client = {
      topic: () => ({
        publishMessage: async () => {
          attempt++;
          if (attempt === 1) throw new Error("first failed");
          await gate;
          order.push("slow publish settled");
          return "msg-2";
        },
        resumePublishing: (orderingKey: string) => {
          order.push(`resumed ${orderingKey}`);
        },
      }),
      subscription: vi.fn(),
      close: vi.fn(async () => {}),
    } as unknown as PubSubClientLike;
    const { publisher } = publisherWith({ client, ordering: true });

    const batch = publisher
      .publishBatch([
        { type: TYPE, data: {}, subject: "k" },
        { type: TYPE, data: {}, subject: "k" },
      ])
      .catch(() => {});
    await new Promise((r) => setImmediate(r));

    // The first publish has already rejected, but the second is still queued on the same key.
    expect(order).toEqual([]);

    release!();
    await batch;
    expect(order).toEqual(["slow publish settled", "resumed k"]);
  });

  // resumePublishing is optional on TopicLike so an existing custom client still satisfies the type.
  // Reaching for it unconditionally would turn a publish failure into a TypeError and lose the cause.
  test("survives a Topic that has no resumePublishing", async () => {
    const client = {
      topic: () => ({
        publishMessage: async () => {
          throw new Error("boom");
        },
      }),
      subscription: vi.fn(),
      close: vi.fn(async () => {}),
    } as unknown as PubSubClientLike;
    const { publisher } = publisherWith({ client, ordering: true });

    await expect(publisher.publish({ type: TYPE, data: {}, subject: "thing-42" })).rejects.toThrow("boom");
  });
});

describe("batch", () => {
  test("publishes every request and returns their ids in order", async () => {
    const { publisher, published } = publisherWith();

    const ids = await publisher.publishBatch([
      { type: TYPE, data: { n: 1 } },
      { type: TYPE, data: { n: 2 } },
    ]);

    expect(ids).toEqual(["msg-1", "msg-2"]);
    expect(published).toHaveLength(2);
  });

  test("gives each message its own id", async () => {
    const { publisher, published } = publisherWith();
    await publisher.publishBatch([
      { type: TYPE, data: {} },
      { type: TYPE, data: {} },
    ]);

    expect(published[0].attributes["ce-id"]).not.toBe(published[1].attributes["ce-id"]);
  });

  test("issues every publish before awaiting any, so the SDK can batch them", async () => {
    const calls: string[] = [];
    let release: (() => void) | undefined;
    const gate = new Promise<void>((r) => (release = r));
    const { publisher } = publisherWith({
      client: {
        topic: () => ({
          publishMessage: async (m: { attributes: Record<string, string> }) => {
            calls.push(m.attributes["ce-type"]);
            await gate;
            return "msg";
          },
        }),
        subscription: vi.fn(),
        close: vi.fn(async () => {}),
      } as unknown as PubSubClientLike,
    });

    const batch = publisher.publishBatch([
      { type: "com.example.a.v1", data: {} },
      { type: "com.example.b.v1", data: {} },
    ]);
    await new Promise((r) => setImmediate(r));

    // Awaiting each publish in turn would leave the second message unsent until the first resolved,
    // so a batch could only ever hold one message. Call order is what preserves ordering-key order.
    expect(calls).toEqual(["com.example.a.v1", "com.example.b.v1"]);

    release!();
    await batch;
  });

  // Pub/Sub has no multi-message transaction: a batch that fails part-way leaves the successful
  // messages published and unsendable. A bare throw tells the caller nothing about which, so
  // retrying the batch would duplicate them.
  test("reports what was published and what failed when one request fails", async () => {
    let call = 0;
    const { publisher } = publisherWith({
      client: {
        topic: () => ({
          publishMessage: async () => {
            call++;
            if (call === 2) throw new Error("topic not found");
            return `msg-${call}`;
          },
        }),
        subscription: vi.fn(),
        close: vi.fn(async () => {}),
      } as unknown as PubSubClientLike,
    });

    const error = await publisher
      .publishBatch([
        { type: "com.example.a.v1", data: {} },
        { type: "com.example.b.v1", data: {} },
        { type: "com.example.c.v1", data: {} },
      ])
      .catch((e: unknown) => e);

    expect(error).toBeInstanceOf(PartialPublishError);
    const partial = error as PartialPublishError;
    expect(partial.published.map((p) => p.index)).toEqual([0, 2]);
    expect(partial.failures.map((f) => f.index)).toEqual([1]);
    expect(partial.failures[0].type).toBe("com.example.b.v1");
    expect((partial.failures[0].cause as Error).message).toBe("topic not found");
  });

  test("still returns plain ids when every request succeeds", async () => {
    const { publisher } = publisherWith();

    await expect(
      publisher.publishBatch([
        { type: TYPE, data: {} },
        { type: TYPE, data: {} },
      ]),
    ).resolves.toEqual(["msg-1", "msg-2"]);
  });
});

describe("topic reuse", () => {
  // Every client.topic() call returns a Topic with its own publisher and batch queue, so building
  // one per message means the SDK never actually batches and each publish pays full overhead.
  test("builds one Topic per destination and reuses it", async () => {
    const { publisher, topicCalls } = publisherWith();

    await publisher.publish({ type: TYPE, data: {} });
    await publisher.publish({ type: TYPE, data: {} });
    await publisher.publish({ type: TYPE, data: {} });

    expect(topicCalls).toEqual(["com-example-thing-happened-v1"]);
  });

  test("keeps a separate Topic per distinct destination", async () => {
    const { publisher, topicCalls } = publisherWith();

    await publisher.publish({ type: TYPE, data: {} });
    await publisher.publish({ type: "com.example.other.v1", data: {} });
    await publisher.publish({ type: TYPE, data: {} });

    expect(topicCalls).toEqual(["com-example-thing-happened-v1", "com-example-other-v1"]);
  });
});

describe("trace propagation", () => {
  const exporter = new InMemorySpanExporter();

  beforeEach(() => {
    context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
    propagation.setGlobalPropagator(new W3CTraceContextPropagator());
    trace.setGlobalTracerProvider(new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] }));
    exporter.reset();
  });

  afterEach(() => {
    trace.disable();
    propagation.disable();
    context.disable();
  });

  // Lifting the ambient context is what lets a consumer's span join the producer's trace.
  test("stamps ce-traceparent from the active span", async () => {
    const { publisher, published } = publisherWith();

    await trace.getTracer("test").startActiveSpan("publishing", async (span) => {
      await publisher.publish({ type: TYPE, data: {} });
      span.end();
    });

    const traceparent = published[0].attributes["ce-traceparent"];
    expect(traceparent).toMatch(/^00-[0-9a-f]{32}-[0-9a-f]{16}-\d{2}$/);
    expect(traceparent).toContain(exporter.getFinishedSpans()[0].spanContext().traceId);
  });

  test("omits ce-traceparent when there is no active span", async () => {
    const { publisher, published } = publisherWith();
    await publisher.publish({ type: TYPE, data: {} });

    expect(published[0].attributes["ce-traceparent"]).toBeUndefined();
  });
});

describe("payload encoding", () => {
  test("sends plain JSON when no encoder is configured", async () => {
    const { publisher, published } = publisherWith();
    await publisher.publish({ type: TYPE, data: { hello: "world" } });

    expect(JSON.parse(published[0].data.toString())).toEqual({ hello: "world" });
    expect(published[0].attributes["ce-datacontenttype"]).toBe("application/json");
  });

  // SPIKE-1: Pub/Sub rejects plain JSON on a schema'd topic, so encoding must go through the codec
  // rather than JSON.stringify.
  test("uses the configured encoder for the body", async () => {
    const encode = vi.fn(() => Buffer.from('{"id":"e1","station":{"string":"SCL"}}'));
    const { publisher, published } = publisherWith({ encode });

    await publisher.publish({ type: TYPE, data: { id: "e1", station: "SCL" } });

    expect(encode).toHaveBeenCalledWith(TYPE, { id: "e1", station: "SCL" });
    expect(published[0].data.toString()).toBe('{"id":"e1","station":{"string":"SCL"}}');
  });

  // An encoder returning bare bytes cannot say what they are, so every payload was announced as
  // application/json — protobuf, CBOR, binary Avro and compressed bodies all mislabelled, which
  // sends a standards-aware consumer to the wrong decoder.
  test("lets the encoder declare the media type it actually produced", async () => {
    const { publisher, published } = publisherWith({
      encode: () => ({ data: Buffer.from([0x08, 0x96, 0x01]), datacontenttype: "application/protobuf" }),
    });

    await publisher.publish({ type: TYPE, data: { id: "e1" } });

    expect(published[0].data).toEqual(Buffer.from([0x08, 0x96, 0x01]));
    expect(published[0].attributes["ce-datacontenttype"]).toBe("application/protobuf");
  });

  test("keeps declaring JSON for an encoder that returns bare bytes", async () => {
    const { publisher, published } = publisherWith({ encode: () => Buffer.from('{"avro":"json"}') });

    await publisher.publish({ type: TYPE, data: { id: "e1" } });

    expect(published[0].attributes["ce-datacontenttype"]).toBe("application/json");
  });

  test("rejects a media type that is not one, rather than emitting an invalid envelope", async () => {
    const { publisher } = publisherWith({
      encode: () => ({ data: Buffer.from("x"), datacontenttype: "not a media type" }),
    });

    await expect(publisher.publish({ type: TYPE, data: {} })).rejects.toThrow(/datacontenttype/);
  });
});
