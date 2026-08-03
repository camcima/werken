import { SpanStatusCode, context, propagation, trace } from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from "@opentelemetry/sdk-metrics";
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { afterEach, beforeEach, describe, expect, test } from "vitest";
import { createTelemetry } from "@werken/nestjs-google-pubsub";

/**
 * Asserted against the real OpenTelemetry SDK with in-memory exporters, not a hand-rolled fake.
 * A fake would encode my assumption about the API and pass exactly when that assumption is wrong.
 */
const spanExporter = new InMemorySpanExporter();
let tracerProvider: BasicTracerProvider;
let meterProvider: MeterProvider;
let reader: PeriodicExportingMetricReader;
// Created per test: meterProvider.shutdown() in afterEach also shuts down the exporter, and a
// shut-down InMemoryMetricExporter silently drops every later export.
let metricExporter: InMemoryMetricExporter;

beforeEach(() => {
  // Without a ContextManager, context.active() always returns ROOT_CONTEXT and active-span
  // propagation silently does nothing — child spans come out unparented. Real consumers get this
  // from the OTel Node SDK; tests must register it explicitly.
  context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
  spanExporter.reset();
  metricExporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
  tracerProvider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(spanExporter)] });
  trace.setGlobalTracerProvider(tracerProvider);
  propagation.setGlobalPropagator(new W3CTraceContextPropagator());
  reader = new PeriodicExportingMetricReader({ exporter: metricExporter, exportIntervalMillis: 60_000 });
  meterProvider = new MeterProvider({ readers: [reader] });
  return undefined;
});

afterEach(async () => {
  trace.disable();
  propagation.disable();
  context.disable();
  await meterProvider.shutdown();
});

/** The registered pattern that matched — what metrics and the span attribute label on. */
const ROUTE = "com.example.thing.*";

const envelope = {
  id: "01931b7c-3f2a-7000-8000-000000000001",
  type: "com.example.thing.happened.v1",
  source: "https://example.test/service",
  subject: "thing-42",
};

function telemetry() {
  return createTelemetry({ enabled: true, serviceName: "orders", meterProvider });
}

async function collect() {
  await reader.forceFlush();
  const all = metricExporter.getMetrics().flatMap((m) => m.scopeMetrics.flatMap((s) => s.metrics));
  return (name: string) => all.find((m) => m.descriptor.name === name);
}

describe("consumer span", () => {
  // Named for the subscription, not ce-type: a span name is a low-cardinality aggregation key and
  // ce-type is producer-controlled. The type is still on the span as an attribute.
  test("records one CONSUMER span named after the subscription", async () => {
    const t = telemetry();
    await t.withMessageSpan({ envelope, subscription: "orders-sub", messageId: "m1", route: ROUTE }, async () => "ack");

    const spans = spanExporter.getFinishedSpans();
    expect(spans).toHaveLength(1);
    expect(spans[0].name).toBe("orders-sub process");
    expect(spans[0].kind).toBe(4); // SpanKind.CONSUMER
  });

  test("carries the messaging and cloudevents attributes", async () => {
    const t = telemetry();
    await t.withMessageSpan({ envelope, subscription: "orders-sub", messageId: "m1", route: ROUTE }, async () => "ack");

    const attributes = spanExporter.getFinishedSpans()[0].attributes;
    expect(attributes["messaging.system"]).toBe("gcp_pubsub");
    expect(attributes["messaging.operation"]).toBe("process");
    expect(attributes["messaging.destination.name"]).toBe("orders-sub");
    expect(attributes["messaging.message.id"]).toBe("m1");
    expect(attributes["cloudevents.event_id"]).toBe(envelope.id);
    expect(attributes["cloudevents.event_type"]).toBe(envelope.type);
    expect(attributes["cloudevents.event_source"]).toBe(envelope.source);
    expect(attributes["cloudevents.event_subject"]).toBe(envelope.subject);
  });

  test("continues the producer's trace from ce-traceparent", async () => {
    const t = telemetry();
    const traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    await t.withMessageSpan(
      { envelope: { ...envelope, traceparent }, subscription: "orders-sub", messageId: "m1", route: ROUTE },
      async () => "ack",
    );

    // Same trace id as the producer means the two halves join up in the trace view.
    expect(spanExporter.getFinishedSpans()[0].spanContext().traceId).toBe("4bf92f3577b34da6a3ce929d0e0e4736");
  });

  // tracestate carries vendor sampling state alongside traceparent; dropping it silently degrades
  // sampling decisions for everything downstream of this consumer.
  test("carries the producer's tracestate alongside traceparent", async () => {
    const t = telemetry();
    const traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    await t.withMessageSpan(
      {
        envelope: { ...envelope, traceparent, tracestate: "vendor=t61rcWkgMzE" },
        subscription: "orders-sub",
        messageId: "m1",
        route: ROUTE,
      },
      async () => "ack",
    );

    expect(spanExporter.getFinishedSpans()[0].spanContext().traceState?.get("vendor")).toBe("t61rcWkgMzE");
  });

  test("starts a fresh trace when the producer sent no traceparent", async () => {
    const t = telemetry();
    await t.withMessageSpan({ envelope, subscription: "orders-sub", messageId: "m1", route: ROUTE }, async () => "ack");

    expect(spanExporter.getFinishedSpans()[0].spanContext().traceId).toMatch(/^[0-9a-f]{32}$/);
  });

  test("marks the span as errored when the handler throws", async () => {
    const t = telemetry();
    await expect(
      t.withMessageSpan({ envelope, subscription: "orders-sub", messageId: "m1", route: ROUTE }, async () => {
        throw new Error("handler blew up");
      }),
    ).rejects.toThrow("handler blew up");

    const span = spanExporter.getFinishedSpans()[0];
    expect(span.status.code).toBe(SpanStatusCode.ERROR);
    expect(span.events.some((e) => e.name === "exception")).toBe(true);
  });

  test("records the outcome on the span", async () => {
    const t = telemetry();
    await t.withMessageSpan(
      { envelope, subscription: "orders-sub", messageId: "m1", route: ROUTE },
      async () => "dead-letter",
    );

    expect(spanExporter.getFinishedSpans()[0].attributes["werken.outcome"]).toBe("dead-letter");
  });

  test("nests child spans under the message span", async () => {
    const t = telemetry();
    await t.withMessageSpan({ envelope, subscription: "orders-sub", messageId: "m1", route: ROUTE }, async () => {
      await t.withChildSpan("werken.decode", async () => undefined);
      return "ack";
    });

    const spans = spanExporter.getFinishedSpans();
    const child = spans.find((s) => s.name === "werken.decode");
    const parent = spans.find((s) => s.name.endsWith("process"));
    expect(child?.parentSpanContext?.spanId).toBe(parent?.spanContext().spanId);
  });
});

describe("metrics", () => {
  test("counts received messages by route and subscription", async () => {
    const t = telemetry();
    t.recordReceived(ROUTE, "orders-sub");
    t.recordReceived(ROUTE, "orders-sub");

    const metric = (await collect())("werken.messages.received");
    expect(metric?.dataPoints[0].value).toBe(2);
    expect(metric?.dataPoints[0].attributes).toMatchObject({ route: ROUTE, subscription: "orders-sub" });
  });

  test("counts outcomes separately", async () => {
    const t = telemetry();
    t.recordOutcome(envelope.type, "ack");
    t.recordOutcome(envelope.type, "nack");
    t.recordOutcome(envelope.type, "nack");

    const metric = (await collect())("werken.messages.outcome");
    const nack = metric?.dataPoints.find((d) => d.attributes.outcome === "nack");
    expect(nack?.value).toBe(2);
  });

  test("records handler duration as a histogram", async () => {
    const t = telemetry();
    t.recordHandlerDuration(envelope.type, 12.5);

    const metric = (await collect())("werken.handler.duration");
    expect(metric?.dataPoints[0].value).toMatchObject({ count: 1 });
  });

  test("tracks in-flight messages up and down", async () => {
    const t = telemetry();
    t.addInFlight("orders-sub", 1);
    t.addInFlight("orders-sub", 1);
    t.addInFlight("orders-sub", -1);

    const metric = (await collect())("werken.messages.inflight");
    expect(metric?.dataPoints[0].value).toBe(1);
  });

  // Lateness is an operational signal in its own right for events that arrive long after they
  // happened, not just a debugging aid.
  test("records lateness in seconds from ce-time", async () => {
    const t = telemetry();
    const now = new Date("2026-08-02T15:00:30.000Z");
    t.recordLateness(envelope.type, new Date("2026-08-02T15:00:00.000Z"), now);

    const metric = (await collect())("werken.event.lateness");
    expect(metric?.dataPoints[0].value).toMatchObject({ sum: 30 });
  });

  test("counts decode failures with a reason", async () => {
    const t = telemetry();
    t.recordDecodeFailure(envelope.type, "schema-unavailable");

    const metric = (await collect())("werken.decode.failures");
    expect(metric?.dataPoints[0].attributes).toMatchObject({ reason: "schema-unavailable" });
  });

  test("counts schema cache hits and misses", async () => {
    const t = telemetry();
    t.recordSchemaCache("hit");
    t.recordSchemaCache("miss");

    const metric = (await collect())("werken.schema.cache");
    expect(metric?.dataPoints.map((d) => d.attributes.result).sort()).toEqual(["hit", "miss"]);
  });
});

describe("when telemetry is disabled", () => {
  test("records nothing and still runs the work", async () => {
    const t = createTelemetry({ enabled: false, serviceName: "orders", meterProvider });

    const result = await t.withMessageSpan(
      { envelope, subscription: "s", messageId: "m1", route: ROUTE },
      async () => "ack",
    );

    expect(result).toBe("ack");
    expect(spanExporter.getFinishedSpans()).toHaveLength(0);
  });

  test("still propagates a handler failure", async () => {
    const t = createTelemetry({ enabled: false, serviceName: "orders", meterProvider });

    await expect(
      t.withMessageSpan({ envelope, subscription: "s", messageId: "m1", route: ROUTE }, async () => {
        throw new Error("boom");
      }),
    ).rejects.toThrow("boom");
  });
});

describe("active context", () => {
  test("makes the message span current, so handler work joins the trace", async () => {
    const t = telemetry();
    let seenTraceId: string | undefined;

    await t.withMessageSpan({ envelope, subscription: "s", messageId: "m1", route: ROUTE }, async () => {
      seenTraceId = trace.getSpan(context.active())?.spanContext().traceId;
      return "ack";
    });

    expect(seenTraceId).toBe(spanExporter.getFinishedSpans()[0].spanContext().traceId);
  });
});
