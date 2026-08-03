import type { Meter, MeterProvider, Span, Tracer } from "@opentelemetry/api";
import type { Outcome } from "./pipeline.js";

/** The envelope fields telemetry needs, so this module does not depend on the full type. */
export interface TelemetryEnvelope {
  readonly id: string;
  readonly type: string;
  readonly source: string;
  readonly subject?: string;
  readonly traceparent?: string;
  readonly tracestate?: string;
}

export interface MessageSpanInput {
  readonly envelope: TelemetryEnvelope;
  readonly subscription: string;
  readonly messageId: string;
}

export interface TelemetryOptions {
  /** Default true. When false, every call is a no-op with no OpenTelemetry involvement. */
  enabled?: boolean;
  serviceName: string;
  /** Injectable for tests; defaults to the globally registered provider. */
  meterProvider?: MeterProvider;
}

export type SchemaCacheResult = "hit" | "miss";

export interface Telemetry {
  withMessageSpan<T>(input: MessageSpanInput, work: () => Promise<T>): Promise<T>;
  withChildSpan<T>(name: string, work: () => Promise<T>): Promise<T>;
  recordReceived(type: string, subscription: string): void;
  recordOutcome(type: string, outcome: Outcome | "skipped_duplicate"): void;
  recordHandlerDuration(type: string, milliseconds: number): void;
  recordDecodeFailure(type: string, reason: string): void;
  recordSchemaCache(result: SchemaCacheResult): void;
  addInFlight(subscription: string, delta: number): void;
  recordLateness(type: string, occurredAt: Date, now?: Date): void;
}

const NOOP: Telemetry = {
  withMessageSpan: (_input, work) => work(),
  withChildSpan: (_name, work) => work(),
  recordReceived: () => {},
  recordOutcome: () => {},
  recordHandlerDuration: () => {},
  recordDecodeFailure: () => {},
  recordSchemaCache: () => {},
  addInFlight: () => {},
  recordLateness: () => {},
};

/**
 * Builds the telemetry facade.
 *
 * `@opentelemetry/api` is an optional peer dependency, so it is loaded lazily and everything
 * degrades to a no-op when it is absent — a consumer that does not want tracing should not have to
 * install it, and must not crash for skipping it.
 */
export function createTelemetry(options: TelemetryOptions): Telemetry {
  if (options.enabled === false) return NOOP;

  const otel = loadOtel();
  if (otel === undefined) return NOOP;

  const { api, SpanKind, SpanStatusCode } = otel;
  const tracer: Tracer = api.trace.getTracer(options.serviceName);
  const meter: Meter = (options.meterProvider ?? api.metrics).getMeter(options.serviceName);

  const received = meter.createCounter("werken.messages.received");
  const outcomes = meter.createCounter("werken.messages.outcome");
  const handlerDuration = meter.createHistogram("werken.handler.duration", { unit: "ms" });
  const decodeFailures = meter.createCounter("werken.decode.failures");
  const schemaCache = meter.createCounter("werken.schema.cache");
  const inFlight = meter.createUpDownCounter("werken.messages.inflight");
  const lateness = meter.createHistogram("werken.event.lateness", { unit: "s" });

  return {
    async withMessageSpan(input, work) {
      // Continues the producer's trace when it sent one; W3C extraction is what joins the two
      // halves of an event's journey in the trace view.
      const carrier: Record<string, string> = {};
      if (input.envelope.traceparent !== undefined) carrier["traceparent"] = input.envelope.traceparent;
      if (input.envelope.tracestate !== undefined) carrier["tracestate"] = input.envelope.tracestate;
      const parent = api.propagation.extract(api.context.active(), carrier);

      return await tracer.startActiveSpan(
        `${input.envelope.type} process`,
        {
          kind: SpanKind.CONSUMER,
          attributes: {
            "messaging.system": "gcp_pubsub",
            "messaging.operation": "process",
            "messaging.destination.name": input.subscription,
            "messaging.message.id": input.messageId,
            "cloudevents.event_id": input.envelope.id,
            "cloudevents.event_type": input.envelope.type,
            "cloudevents.event_source": input.envelope.source,
            ...(input.envelope.subject === undefined ? {} : { "cloudevents.event_subject": input.envelope.subject }),
          },
        },
        parent,
        async (span: Span) => {
          try {
            const result = await work();
            if (typeof result === "string") span.setAttribute("werken.outcome", result);
            return result;
          } catch (error) {
            span.recordException(error as Error);
            span.setStatus({ code: SpanStatusCode.ERROR, message: asMessage(error) });
            throw error;
          } finally {
            span.end();
          }
        },
      );
    },

    async withChildSpan(name, work) {
      return await tracer.startActiveSpan(name, async (span: Span) => {
        try {
          return await work();
        } catch (error) {
          span.recordException(error as Error);
          span.setStatus({ code: SpanStatusCode.ERROR, message: asMessage(error) });
          throw error;
        } finally {
          span.end();
        }
      });
    },

    recordReceived: (type, subscription) => received.add(1, { type, subscription }),
    recordOutcome: (type, outcome) => outcomes.add(1, { type, outcome }),
    recordHandlerDuration: (type, milliseconds) => handlerDuration.record(milliseconds, { type }),
    recordDecodeFailure: (type, reason) => decodeFailures.add(1, { type, reason }),
    recordSchemaCache: (result) => schemaCache.add(1, { result }),
    addInFlight: (subscription, delta) => inFlight.add(delta, { subscription }),
    recordLateness: (type, occurredAt, now = new Date()) =>
      lateness.record((now.getTime() - occurredAt.getTime()) / 1000, { type }),
  };
}

interface LoadedOtel {
  api: typeof import("@opentelemetry/api");
  SpanKind: typeof import("@opentelemetry/api").SpanKind;
  SpanStatusCode: typeof import("@opentelemetry/api").SpanStatusCode;
}

let cached: LoadedOtel | null | undefined;

function loadOtel(): LoadedOtel | undefined {
  if (cached !== undefined) return cached ?? undefined;
  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports -- optional peer dependency, resolved at runtime
    const api = require("@opentelemetry/api") as typeof import("@opentelemetry/api");
    cached = { api, SpanKind: api.SpanKind, SpanStatusCode: api.SpanStatusCode };
  } catch {
    cached = null;
  }
  return cached ?? undefined;
}

function asMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
