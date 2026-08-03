// Runs against the BUILT package under plain Node ESM — no vitest module runner in between.
// Guards the class of bug where a `require(...)` survives into the ESM output, throws
// ReferenceError at runtime, and a try/catch downgrades telemetry to a silent no-op.
import { context, propagation, trace } from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { createTelemetry } from "../../dist/telemetry.js";
import { createEventPublisher } from "../../dist/publisher.js";

context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
const exporter = new InMemorySpanExporter();
trace.setGlobalTracerProvider(new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] }));
propagation.setGlobalPropagator(new W3CTraceContextPropagator());

const telemetry = createTelemetry({ serviceName: "smoke" });
await telemetry.withChildSpan("werken.smoke", async () => {});

let published;
const publisher = createEventPublisher({
  source: "https://smoke.test/service",
  client: {
    topic: () => ({
      publishMessage: async (message) => {
        published = message;
        return "smoke-id";
      },
    }),
    subscription: () => {
      throw new Error("unused");
    },
    close: () => {},
  },
  topicResolver: () => "smoke-topic",
});

await trace.getTracer("smoke").startActiveSpan("produce", async (span) => {
  await publisher.publish({ type: "com.example.smoke.v1", data: { ok: true } });
  span.end();
});

process.stdout.write(
  JSON.stringify({
    telemetryLoaded: exporter.getFinishedSpans().some((s) => s.name === "werken.smoke"),
    traceparent: published?.attributes["ce-traceparent"] ?? null,
  }),
);
