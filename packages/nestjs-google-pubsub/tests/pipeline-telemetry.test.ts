import { context, propagation, trace } from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { afterEach, beforeEach, describe, expect, test } from "vitest";
import { InMemoryIdempotencyStore, TerminalEventError } from "@werken/nestjs-google-pubsub";
import { MessagePipeline, createTelemetry } from "@werken/nestjs-google-pubsub/internal";
import type { IncomingMessage } from "@werken/nestjs-google-pubsub";
import type { Telemetry } from "@werken/nestjs-google-pubsub/internal";

const TYPE = "com.example.thing.happened.v1";
const SUBSCRIPTION = "projects/p/subscriptions/s";

function message(overrides: Partial<IncomingMessage> = {}): IncomingMessage {
  return {
    id: "pubsub-message-1",
    attributes: {
      "ce-specversion": "1.0",
      "ce-id": "01931b7c-3f2a-7000-8000-000000000001",
      "ce-source": "https://example.test/service",
      "ce-type": TYPE,
    },
    data: Buffer.from(JSON.stringify({ hello: "world" })),
    publishTime: new Date("2026-08-02T15:00:00.000Z"),
    deliveryAttempt: 0,
    orderingKey: "",
    ack: () => {},
    nack: () => {},
    ...overrides,
  };
}

/** Records what the pipeline asked of telemetry, so the wiring itself can be asserted. */
function recordingTelemetry() {
  const outcomes: Array<{ type: string; outcome: string }> = [];
  const messageSpans: string[] = [];
  const durations: Array<{ type: string; ms: number }> = [];
  const telemetry: Telemetry = {
    withMessageSpan: (input, work) => {
      messageSpans.push(input.envelope.id);
      return work();
    },
    withChildSpan: (_name, work) => work(),
    recordReceived: () => {},
    recordOutcome: (type, outcome) => {
      outcomes.push({ type, outcome });
    },
    recordHandlerDuration: (type, ms) => {
      durations.push({ type, ms });
    },
    recordDecodeFailure: () => {},
    recordSchemaCache: () => {},
    addInFlight: () => {},
    recordLateness: () => {},
  };
  return { telemetry, outcomes, messageSpans, durations };
}

describe("outcome metric wiring", () => {
  test("records ack when the handler succeeds", async () => {
    const { telemetry, outcomes } = recordingTelemetry();
    const pipeline = new MessagePipeline({
      subscription: SUBSCRIPTION,
      resolveRoute: () => ({ handler: async () => {}, pattern: TYPE }),
      telemetry,
    });

    await pipeline.handle(message());

    expect(outcomes).toEqual([{ type: TYPE, outcome: "ack" }]);
  });

  test("records nack when the handler throws", async () => {
    const { telemetry, outcomes } = recordingTelemetry();
    const pipeline = new MessagePipeline({
      subscription: SUBSCRIPTION,
      resolveRoute: () => ({
        pattern: TYPE,
        handler: () => {
          throw new Error("transient");
        },
      }),
      telemetry,
    });

    await pipeline.handle(message());

    expect(outcomes).toEqual([{ type: TYPE, outcome: "nack" }]);
  });

  test("records dead-letter for a terminal failure", async () => {
    const { telemetry, outcomes } = recordingTelemetry();
    const pipeline = new MessagePipeline({
      subscription: SUBSCRIPTION,
      resolveRoute: () => ({
        pattern: TYPE,
        handler: () => {
          throw new TerminalEventError("will never resolve");
        },
      }),
      deadLetterPublisher: { publish: async () => {} },
      telemetry,
    });

    await pipeline.handle(message());

    expect(outcomes).toEqual([{ type: TYPE, outcome: "dead-letter" }]);
  });

  // Labelled by a bounded sentinel, never the raw ce-type: an unmatched type is by definition one
  // this consumer did not register, so labelling on it lets a stray producer mint a new series per
  // event type it invents.
  test("records a message no handler matches under the unmatched sentinel", async () => {
    const { telemetry, outcomes } = recordingTelemetry();
    const pipeline = new MessagePipeline({
      subscription: SUBSCRIPTION,
      resolveRoute: () => null,
      telemetry,
    });

    await pipeline.handle(message());

    expect(outcomes).toEqual([{ type: "<unmatched>", outcome: "ack" }]);
  });

  test("records skipped_duplicate, not a second ack, for a duplicate delivery", async () => {
    const { telemetry, outcomes } = recordingTelemetry();
    const pipeline = new MessagePipeline({
      subscription: SUBSCRIPTION,
      resolveRoute: () => ({ handler: async () => {}, pattern: TYPE }),
      idempotencyStore: new InMemoryIdempotencyStore(),
      consumer: "orders",
      telemetry,
    });

    await pipeline.handle(message());
    const duplicateOutcome = await pipeline.handle(message());

    expect(duplicateOutcome).toBe("ack");
    expect(outcomes).toEqual([
      { type: TYPE, outcome: "ack" },
      { type: TYPE, outcome: "skipped_duplicate" },
    ]);
  });

  test("records an invalid envelope under the invalid sentinel", async () => {
    const { telemetry, outcomes } = recordingTelemetry();
    const pipeline = new MessagePipeline({
      subscription: SUBSCRIPTION,
      resolveRoute: () => ({ handler: async () => {}, pattern: TYPE }),
      deadLetterPublisher: { publish: async () => {} },
      telemetry,
    });

    await pipeline.handle(message({ attributes: { "ce-specversion": "1.0" } }));

    // Previously skipped entirely, which made a producer emitting garbage indistinguishable from
    // one emitting nothing — the case most worth spotting.
    expect(outcomes).toEqual([{ type: "<invalid>", outcome: "dead-letter" }]);
  });
});

describe("handler duration metric", () => {
  // A terminal failure is often the slowest thing a handler does — the timeout that finally gave
  // up — so leaving it out of the histogram hides exactly the tail worth seeing.
  test("records handler duration for a terminal failure, not only for success and nack", async () => {
    const { telemetry, durations } = recordingTelemetry();
    const pipeline = new MessagePipeline({
      subscription: SUBSCRIPTION,
      resolveRoute: () => ({
        pattern: TYPE,
        handler: () => {
          throw new TerminalEventError("will never resolve");
        },
      }),
      deadLetterPublisher: { publish: async () => {} },
      telemetry,
    });

    await pipeline.handle(message());

    expect(durations.map((d) => d.type)).toEqual([TYPE]);
  });
});

describe("message span wiring", () => {
  // Every message with a valid envelope gets a span, including one no handler matches: that case is
  // contract drift, and seeing it join the producer's trace is how it gets noticed at all.
  test("opens a message span for an unmatched type as well as a handled one", async () => {
    const { telemetry, messageSpans } = recordingTelemetry();
    const handlers: Record<string, () => void> = { [TYPE]: () => {} };
    const pipeline = new MessagePipeline({
      subscription: SUBSCRIPTION,
      resolveRoute: (type) => (handlers[type] === undefined ? null : { handler: handlers[type], pattern: type }),
      telemetry,
    });

    await pipeline.handle(message());
    await pipeline.handle(
      message({
        id: "pubsub-message-2",
        attributes: {
          ...message().attributes,
          "ce-id": "01931b7c-3f2a-7000-8000-000000000002",
          "ce-type": "com.example.unrelated.v1",
        },
      }),
    );

    expect(messageSpans).toEqual(["01931b7c-3f2a-7000-8000-000000000001", "01931b7c-3f2a-7000-8000-000000000002"]);
  });

  // An invalid envelope has no trustworthy traceparent to parent a span on, so it stays
  // metrics-only. The rule is: a span for every valid envelope, a metric for every message.
  test("opens no span for an invalid envelope", async () => {
    const { telemetry, messageSpans } = recordingTelemetry();
    const pipeline = new MessagePipeline({
      subscription: SUBSCRIPTION,
      resolveRoute: () => ({ handler: async () => {}, pattern: TYPE }),
      deadLetterPublisher: { publish: async () => {} },
      telemetry,
    });

    await pipeline.handle(message({ attributes: { "ce-specversion": "1.0" } }));

    expect(messageSpans).toEqual([]);
  });
});

/**
 * Asserted against the real OpenTelemetry SDK with in-memory exporters, as telemetry.test.ts does:
 * a fake would encode the assumption that the pipeline calls the facade, which is the very thing
 * under test here.
 */
describe("trace continuity through the pipeline", () => {
  const spanExporter = new InMemorySpanExporter();

  beforeEach(() => {
    context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
    spanExporter.reset();
    trace.setGlobalTracerProvider(new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(spanExporter)] }));
    propagation.setGlobalPropagator(new W3CTraceContextPropagator());
  });

  afterEach(() => {
    trace.disable();
    propagation.disable();
    context.disable();
  });

  test("wraps decode and handler in one CONSUMER span continuing ce-traceparent", async () => {
    const pipeline = new MessagePipeline({
      subscription: SUBSCRIPTION,
      resolveRoute: () => ({ handler: async () => {}, pattern: TYPE }),
      telemetry: createTelemetry({ serviceName: "werken" }),
    });

    const traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    await pipeline.handle(message({ attributes: { ...message().attributes, "ce-traceparent": traceparent } }));

    const spans = spanExporter.getFinishedSpans();
    const processSpan = spans.find((s) => s.name === `${SUBSCRIPTION} process`);
    expect(processSpan).toBeDefined();
    expect(processSpan?.kind).toBe(4); // SpanKind.CONSUMER
    // Same trace id as the producer means the two halves join up in the trace view.
    expect(processSpan?.spanContext().traceId).toBe("4bf92f3577b34da6a3ce929d0e0e4736");

    for (const name of ["werken.decode", "werken.handler"]) {
      const child = spans.find((s) => s.name === name);
      expect(child?.parentSpanContext?.spanId).toBe(processSpan?.spanContext().spanId);
    }
  });
});
