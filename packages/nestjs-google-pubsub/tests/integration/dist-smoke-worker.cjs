// The CommonJS twin of dist-smoke-worker.mjs: proves the CJS build reaches OpenTelemetry too, so
// a fix for the ESM loader cannot quietly regress the build where `require` was already fine.
const { context, propagation, trace } = require("@opentelemetry/api");
const { AsyncLocalStorageContextManager } = require("@opentelemetry/context-async-hooks");
const { W3CTraceContextPropagator } = require("@opentelemetry/core");
const { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } = require("@opentelemetry/sdk-trace-base");
const { createTelemetry } = require("../../dist/cjs/telemetry.js");
const { createEventPublisher } = require("../../dist/cjs/publisher.js");

async function main() {
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
}

void main();
