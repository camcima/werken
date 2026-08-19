import type { Meter, MeterProvider, Span, Tracer } from "@opentelemetry/api";
import { loadOtelApi } from "./otel.js";
import type { Outcome } from "./pipeline.js";

/** The envelope fields telemetry needs, so this module does not depend on the full type. */
interface TelemetryEnvelope {
  readonly id: string;
  readonly type: string;
  readonly source: string;
  readonly subject?: string;
  readonly traceparent?: string;
  readonly tracestate?: string;
}

interface MessageSpanInput {
  readonly envelope: TelemetryEnvelope;
  readonly subscription: string;
  readonly messageId: string;
  /** The registered pattern that matched, or a bounded sentinel when none did. */
  readonly route: string;
}

export interface TelemetryOptions {
  /** Default true. When false, every call is a no-op with no OpenTelemetry involvement. */
  enabled?: boolean;
  serviceName: string;
  /** Injectable for tests; defaults to the globally registered provider. */
  meterProvider?: MeterProvider;
}

type SchemaCacheResult = "hit" | "miss";

export interface Telemetry {
  withMessageSpan<T>(input: MessageSpanInput, work: () => Promise<T>): Promise<T>;
  withChildSpan<T>(name: string, work: () => Promise<T>): Promise<T>;
  /**
   * Every metric below is labelled by `route` — the registered pattern, or a bounded sentinel for
   * a message that matched none. Never the raw `ce-type`: that is producer-controlled, and a
   * wildcard route matches an open-ended set of them, so labelling on it lets one misbehaving
   * producer mint unbounded metric series.
   */
  recordReceived(route: string, subscription: string): void;
  recordOutcome(route: string, outcome: Outcome | "skipped_duplicate"): void;
  recordHandlerDuration(route: string, milliseconds: number): void;
  recordDecodeFailure(route: string, reason: string): void;
  recordSchemaCache(result: SchemaCacheResult): void;
  addInFlight(subscription: string, delta: number): void;
  recordLateness(route: string, occurredAt: Date, now?: Date): void;
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

      // Named for the subscription, not the event type: a span name is a low-cardinality
      // aggregation key, and ce-type is producer-controlled. The type is still on the span, as an
      // attribute, where high cardinality is expected and cheap.
      return await tracer.startActiveSpan(
        `${input.subscription} process`,
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
            "werken.route": input.route,
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

    recordReceived: (route, subscription) => received.add(1, { route, subscription }),
    recordOutcome: (route, outcome) => outcomes.add(1, { route, outcome }),
    recordHandlerDuration: (route, milliseconds) => handlerDuration.record(milliseconds, { route }),
    recordDecodeFailure: (route, reason) => decodeFailures.add(1, { route, reason }),
    recordSchemaCache: (result) => schemaCache.add(1, { result }),
    addInFlight: (subscription, delta) => inFlight.add(delta, { subscription }),
    recordLateness: (route, occurredAt, now = new Date()) =>
      lateness.record((now.getTime() - occurredAt.getTime()) / 1000, { route }),
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
  const api = loadOtelApi();
  cached = api === undefined ? null : { api, SpanKind: api.SpanKind, SpanStatusCode: api.SpanStatusCode };
  return cached ?? undefined;
}

function asMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
