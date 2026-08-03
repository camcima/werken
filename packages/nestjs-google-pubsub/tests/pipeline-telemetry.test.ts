import { context, propagation, trace } from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { afterEach, beforeEach, describe, expect, test } from "vitest";
import {
  InMemoryIdempotencyStore,
  MessagePipeline,
  TerminalEventError,
  createTelemetry,
} from "@werken/nestjs-google-pubsub";
import type { IncomingMessage, Telemetry } from "@werken/nestjs-google-pubsub";

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
      resolveHandler: () => async () => {},
      telemetry,
    });

    await pipeline.handle(message());

    expect(outcomes).toEqual([{ type: TYPE, outcome: "ack" }]);
  });

  test("records nack when the handler throws", async () => {
    const { telemetry, outcomes } = recordingTelemetry();
    const pipeline = new MessagePipeline({
      subscription: SUBSCRIPTION,
      resolveHandler: () => () => {
        throw new Error("transient");
      },
      telemetry,
    });

    await pipeline.handle(message());

    expect(outcomes).toEqual([{ type: TYPE, outcome: "nack" }]);
  });

  test("records dead-letter for a terminal failure", async () => {
    const { telemetry, outcomes } = recordingTelemetry();
    const pipeline = new MessagePipeline({
      subscription: SUBSCRIPTION,
      resolveHandler: () => () => {
        throw new TerminalEventError("will never resolve");
      },
      deadLetterPublisher: { publish: async () => {} },
      telemetry,
    });

    await pipeline.handle(message());

    expect(outcomes).toEqual([{ type: TYPE, outcome: "dead-letter" }]);
  });

  test("records the policy outcome for a message no handler matches", async () => {
    const { telemetry, outcomes } = recordingTelemetry();
    const pipeline = new MessagePipeline({
      subscription: SUBSCRIPTION,
      resolveHandler: () => null,
      telemetry,
    });

    await pipeline.handle(message());

    expect(outcomes).toEqual([{ type: TYPE, outcome: "ack" }]);
  });

  test("records skipped_duplicate, not a second ack, for a duplicate delivery", async () => {
    const { telemetry, outcomes } = recordingTelemetry();
    const pipeline = new MessagePipeline({
      subscription: SUBSCRIPTION,
      resolveHandler: () => async () => {},
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

  test("records nothing for an invalid envelope, whose type cannot be trusted", async () => {
    const { telemetry, outcomes } = recordingTelemetry();
    const pipeline = new MessagePipeline({
      subscription: SUBSCRIPTION,
      resolveHandler: () => async () => {},
      deadLetterPublisher: { publish: async () => {} },
      telemetry,
    });

    await pipeline.handle(message({ attributes: { "ce-specversion": "1.0" } }));

    expect(outcomes).toEqual([]);
  });
});

describe("handler duration metric", () => {
  // A terminal failure is often the slowest thing a handler does — the timeout that finally gave
  // up — so leaving it out of the histogram hides exactly the tail worth seeing.
  test("records handler duration for a terminal failure, not only for success and nack", async () => {
    const { telemetry, durations } = recordingTelemetry();
    const pipeline = new MessagePipeline({
      subscription: SUBSCRIPTION,
      resolveHandler: () => () => {
        throw new TerminalEventError("will never resolve");
      },
      deadLetterPublisher: { publish: async () => {} },
      telemetry,
    });

    await pipeline.handle(message());

    expect(durations.map((d) => d.type)).toEqual([TYPE]);
  });
});

describe("message span wiring", () => {
  // The span opens once a handler is known — unhandled types are not this consumer's work.
  test("opens the message span only when a handler resolves", async () => {
    const { telemetry, messageSpans } = recordingTelemetry();
    const handlers: Record<string, () => void> = { [TYPE]: () => {} };
    const pipeline = new MessagePipeline({
      subscription: SUBSCRIPTION,
      resolveHandler: (pattern) => handlers[pattern] ?? null,
      telemetry,
    });

    await pipeline.handle(message());
    await pipeline.handle(message({ attributes: { ...message().attributes, "ce-type": "com.example.unrelated.v1" } }));

    expect(messageSpans).toEqual(["01931b7c-3f2a-7000-8000-000000000001"]);
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
      resolveHandler: () => async () => {},
      telemetry: createTelemetry({ serviceName: "werken" }),
    });

    const traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    await pipeline.handle(message({ attributes: { ...message().attributes, "ce-traceparent": traceparent } }));

    const spans = spanExporter.getFinishedSpans();
    const processSpan = spans.find((s) => s.name === `${TYPE} process`);
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
