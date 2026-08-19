import { parseEnvelope } from "@werken/cloudevents";
import type { CloudEventEnvelope } from "@werken/cloudevents";
import { buildContext } from "./context.js";
import { asTerminalFailure } from "./dead-letter.js";
import type { DeadLetterPublisher, DeadLetterStage } from "./dead-letter.js";
import { idempotencyKeyToString } from "./idempotency.js";
import type { IdempotencyKey, IdempotencyStore } from "./idempotency.js";
import { schemaMetaFromAttributes } from "./schema/attributes.js";
import type { Telemetry } from "./telemetry.js";
import { withEventFields } from "./logging.js";
import { WriterSchemaUnavailableError } from "./schema/avro-codec.js";
import type { AvroCodec } from "./schema/avro-codec.js";
import type { CloudEventContext, IncomingMessage } from "./types.js";

/** What the transport should do with a message once the pipeline has run. */
export type Outcome = "ack" | "nack" | "dead-letter";

/** Policy for a message the pipeline refuses to process. */
export type RejectionPolicy = "dead-letter" | "nack" | "ack";

export type EventHandler = (data: unknown, ctx: CloudEventContext) => unknown;

/** A handler plus the registered pattern that selected it. */
export interface ResolvedRoute {
  readonly handler: EventHandler;
  /**
   * The pattern this consumer registered, not the `ce-type` that matched it. Bounded by
   * registration, which is why telemetry labels on it: `ce-type` is producer-controlled, and a
   * wildcard route matches an open-ended set of them.
   */
  readonly pattern: string;
}

/**
 * Telemetry labels for messages that never reached a route. Bounded stand-ins, so a malformed or
 * unrecognised producer cannot mint an unbounded number of metric series.
 */
const UNMATCHED_ROUTE = "<unmatched>";
const INVALID_ROUTE = "<invalid>";

export interface ValidationOptions {
  /** Default 'dead-letter'. */
  onInvalidEnvelope?: RejectionPolicy;
  /**
   * A body this consumer cannot read: not valid JSON, not decodable against its schema, or a
   * reader type it does not have. Default 'dead-letter' — the bytes will not become readable on
   * redelivery, so retrying only burns the budget.
   */
  onDecodeFailure?: RejectionPolicy;
  /**
   * The writer schema could not be *fetched* — a Schema Service outage, a revision that has not
   * propagated, a client without schema support. Default 'nack'.
   *
   * Deliberately separate from `onDecodeFailure`, and deliberately not defaulted to it: nothing is
   * known to be wrong with the message, so the next delivery will very likely decode it. Dead-
   * lettering here turns a blip into a manual redrive of every in-flight message, which
   * is the same reasoning that makes an unreadable idempotency store nack rather than reprocess.
   *
   * Only reachable with `schemaRegistry.strict` left on: with it off the codec has already fallen
   * back to plain JSON for exactly this failure and decode never fails.
   */
  onSchemaUnavailable?: RejectionPolicy;
  /** Default false. */
  requireDataschema?: boolean;
}

export interface MessagePipelineOptions {
  readonly subscription: string;
  /**
   * Resolves the route for a `ce-type`, or null when this consumer has none. The transport supplies
   * a `PatternRouter`, which prefers an exact pattern over a wildcard one.
   */
  readonly resolveRoute: (type: string) => ResolvedRoute | null;
  readonly deadLetterPublisher?: DeadLetterPublisher;
  /** Avro schema resolution and decode. Without it, bodies are parsed as plain JSON. */
  readonly codec?: Pick<AvroCodec, "decode">;
  /** Duplicate suppression. Defaults to a no-op store that warns loudly at startup. */
  readonly idempotencyStore?: IdempotencyStore;
  /** Identifies this consumer in the idempotency key. Required for a shared store to be correct. */
  readonly consumer?: string;
  /** How long a processed marker is retained. Default 7 days. */
  readonly idempotencyTtlMs?: number;
  readonly validation?: ValidationOptions;
  /** Default 'ack' — a subscription legitimately carries types this consumer ignores. */
  readonly onUnhandledPattern?: RejectionPolicy;
  readonly now?: () => Date;
  readonly telemetry?: Telemetry;
  readonly logger?: Pick<Console, "warn" | "error"> & Partial<Pick<Console, "debug">>;
}

/**
 * How long a processed-event marker is retained: 7 days.
 *
 * The bound that matters is how long a duplicate can plausibly arrive after the original — a
 * subscription's own retention plus any replay window — so this is deliberately far longer than a
 * redelivery cycle.
 */
const DEFAULT_IDEMPOTENCY_TTL_MS = 7 * 24 * 60 * 60 * 1000;

export class MessagePipeline {
  /**
   * Keys currently being handled in this process. Pub/Sub can deliver the same message twice
   * concurrently, and has()-then-tryRecord() leaves a window where both deliveries pass the check.
   * This collapses that window in-process; across replicas the store is still the only guard, which
   * is why handlers must remain idempotent regardless.
   */
  private readonly inProcess = new Map<string, Promise<Outcome>>();

  constructor(private readonly options: MessagePipelineOptions) {}

  async handle(message: IncomingMessage): Promise<Outcome> {
    const now = this.options.now ?? (() => new Date());

    let envelope: CloudEventEnvelope;
    try {
      envelope = parseEnvelope(message.attributes);
      if (this.options.validation?.requireDataschema === true && envelope.dataschema === undefined) {
        throw new Error("ce-dataschema is required but absent");
      }
    } catch (error) {
      // Rejected ahead of coalescing, not inside run(). The de-duplication key is built from ce-id
      // and ce-source, so a message whose envelope has not been validated has no identity worth
      // keying on: two malformed messages both carrying an empty ce-id would share one outcome, and
      // the transport would ack both while only the first was ever copied to the dead-letter topic.
      const outcome = await this.reject(message, {
        policy: this.options.validation?.onInvalidEnvelope ?? "dead-letter",
        stage: "envelope",
        reason: asMessage(error),
        now,
      });
      // Counted under a sentinel rather than skipped. These are the messages that matter most for
      // spotting contract drift, and leaving them out of the metrics made a producer emitting
      // garbage look identical to one emitting nothing. No span: there is no envelope to trust,
      // so nothing to parent it on.
      const telemetry = this.options.telemetry;
      telemetry?.recordReceived(INVALID_ROUTE, this.options.subscription);
      telemetry?.recordOutcome(INVALID_ROUTE, outcome);
      return outcome;
    }

    if (this.options.idempotencyStore === undefined) return this.run(envelope, message, now);

    const dedupeKey = idempotencyKeyToString(this.keyFor(envelope.source, envelope.id));
    const running = this.inProcess.get(dedupeKey);
    if (running !== undefined) return running;

    const pending = this.run(envelope, message, now).finally(() => this.inProcess.delete(dedupeKey));
    this.inProcess.set(dedupeKey, pending);
    return pending;
  }

  private keyFor(source: string, id: string): IdempotencyKey {
    return { consumer: this.options.consumer ?? "werken", source, id };
  }

  private async run(envelope: CloudEventEnvelope, message: IncomingMessage, now: () => Date): Promise<Outcome> {
    const telemetry = this.options.telemetry;

    const route = this.options.resolveRoute(envelope.type);
    // Labelled by the registered pattern, never the raw ce-type: the type is producer-controlled,
    // so using it would let one misbehaving producer mint unbounded metric series.
    const label = route?.pattern ?? UNMATCHED_ROUTE;

    telemetry?.recordReceived(label, this.options.subscription);
    // Lateness is an operational signal in its own right, not just a debugging aid: for events that
    // routinely arrive long after they happened, its distribution is the thing worth alerting on.
    if (envelope.time !== undefined) telemetry?.recordLateness(label, envelope.time, now());

    // The span covers every message with a valid envelope, including one no handler matches —
    // that case is contract drift, and seeing it join the producer's trace is how it gets noticed.
    // It spans the idempotency check, decode, the handler and the idempotency record, parented on
    // ce-traceparent so the werken.decode/werken.handler children join the producer's trace.
    const work = (): Promise<Outcome | "skipped_duplicate"> =>
      route === null
        ? this.reject(message, {
            policy: this.options.onUnhandledPattern ?? "ack",
            stage: "unhandled",
            reason: `no handler registered for ${envelope.type}`,
            now,
          })
        : this.dispatch(route.handler, label, envelope, message, now);

    const result =
      telemetry === undefined
        ? await work()
        : await telemetry.withMessageSpan(
            { envelope, subscription: this.options.subscription, messageId: message.id, route: label },
            work,
          );
    telemetry?.recordOutcome(label, result);
    return result === "skipped_duplicate" ? "ack" : result;
  }

  private async dispatch(
    handler: EventHandler,
    label: string,
    envelope: CloudEventEnvelope,
    message: IncomingMessage,
    now: () => Date,
  ): Promise<Outcome | "skipped_duplicate"> {
    const telemetry = this.options.telemetry;
    const store = this.options.idempotencyStore;
    const key = this.keyFor(envelope.source, envelope.id);
    // Built before the check so the store can resolve per-message state (a connection, a tenant, a
    // transaction) from the same context the handler will see.
    const ctx = buildContext(envelope, message, this.options.subscription);
    if (store !== undefined) {
      try {
        // Before decode: a duplicate should not pay the decode cost, and a message already
        // processed must still be acked even if its schema has since become unreadable.
        if (await store.has(key, ctx)) {
          this.options.logger?.debug?.(withEventFields("werken: skipping duplicate", message));
          // The caller records this as skipped_duplicate rather than a second ack, and maps it
          // back to an ack for the transport.
          return "skipped_duplicate";
        }
      } catch (error) {
        // Treating an unreadable store as "not seen" would reprocess every message during an
        // outage. Redelivering is the safer failure.
        this.options.logger?.error(
          withEventFields(`werken: idempotency check failed: ${asMessage(error)} — nacking`, message),
        );
        return "nack";
      }
    }

    let data: unknown;
    try {
      data = await (telemetry === undefined
        ? this.decode(message)
        : telemetry.withChildSpan("werken.decode", () => this.decode(message)));
    } catch (error) {
      telemetry?.recordDecodeFailure(label, decodeReason(error));
      // Two different failures wear the same stage. An unfetchable writer schema says nothing about
      // the bytes, so it gets its own policy and defaults to redelivery; everything else here is a
      // body this consumer will never read, whatever it does next.
      const unavailable = error instanceof WriterSchemaUnavailableError;
      return this.reject(message, {
        policy: unavailable
          ? (this.options.validation?.onSchemaUnavailable ?? "nack")
          : (this.options.validation?.onDecodeFailure ?? "dead-letter"),
        stage: "decode",
        reason: asMessage(error),
        now,
      });
    }

    const startedAt = Date.now();
    try {
      await (telemetry === undefined
        ? settle(await handler(data, ctx))
        : telemetry.withChildSpan("werken.handler", async () => settle(await handler(data, ctx))));
      telemetry?.recordHandlerDuration(label, Date.now() - startedAt);
      await this.record(key, ctx);
      return "ack";
    } catch (error) {
      // Recorded here rather than per-branch so a terminal failure counts too — those are often the
      // slowest thing a handler does, and the tail is the reason to keep a histogram at all. Taken
      // before the dead-letter publish below, so it measures the handler and not the publish.
      telemetry?.recordHandlerDuration(label, Date.now() - startedAt);

      const terminal = asTerminalFailure(error);
      if (terminal !== null) {
        const outcome = await this.reject(message, {
          policy: "dead-letter",
          stage: "handler",
          reason: terminal.reason,
          detail: terminal.detail,
          now,
        });
        // A dead-lettered message is finished with; recording stops a redelivery re-running the
        // handler and publishing a second copy.
        if (outcome === "dead-letter") await this.record(key, ctx);
        return outcome;
      }
      this.options.logger?.error(withEventFields(`werken: handler failed: ${asMessage(error)}`, message));
      return "nack";
    }
  }

  /** Avro when a codec is configured and the topic carries a schema, plain JSON otherwise. */
  private async decode(message: IncomingMessage): Promise<unknown> {
    return this.options.codec === undefined
      ? plainJson(message)
      : await this.options.codec.decode(message.data, schemaMetaFromAttributes(message.attributes));
  }

  /**
   * Recorded only after the handler succeeded and before the ack. Recording earlier risks silently
   * dropping a message that then failed; recording after the ack risks a crash in between. Neither
   * is eliminable — this is at-least-once — which is why handlers must still be idempotent.
   */
  private async record(key: IdempotencyKey, ctx: CloudEventContext): Promise<void> {
    const store = this.options.idempotencyStore;
    if (store === undefined) return;
    try {
      const recorded = await store.tryRecord(key, this.options.idempotencyTtlMs ?? DEFAULT_IDEMPOTENCY_TTL_MS, ctx);
      if (!recorded) {
        // has() said new, tryRecord() says already present: another replica recorded this event
        // between the two calls, so the handler has just run a second time and produced a duplicate
        // side effect. Nothing here can undo it — the point is that it stops being invisible, since
        // this is precisely the duplicate the store exists to prevent.
        this.options.logger?.warn(
          `werken: ${key.id} was already recorded by another consumer — the handler ran on a duplicate`,
        );
      }
    } catch (error) {
      // The side effect already happened. Nacking now would guarantee a duplicate; acking risks
      // one only if the write was genuinely lost.
      this.options.logger?.error(`werken: failed to record ${key.id}: ${asMessage(error)} — acking anyway`);
    }
  }

  private async reject(
    message: IncomingMessage,
    rejection: {
      policy: RejectionPolicy;
      stage: DeadLetterStage;
      reason: string;
      detail?: Record<string, unknown>;
      now: () => Date;
    },
  ): Promise<Outcome> {
    const { policy, stage, reason, detail, now } = rejection;

    if (policy === "ack") return "ack";
    if (policy === "nack") {
      this.options.logger?.warn(withEventFields(`werken: nacking at ${stage}: ${reason}`, message));
      return "nack";
    }

    const publisher = this.options.deadLetterPublisher;
    if (publisher === undefined) {
      // Failing loudly beats silently dropping: without a dead-letter topic the only safe outcome
      // is redelivery.
      this.options.logger?.error(
        withEventFields(
          `werken: terminal at ${stage} (${reason}) but no dead-letter topic is configured — nacking`,
          message,
        ),
      );
      return "nack";
    }

    try {
      await publisher.publish({
        message,
        reason,
        stage,
        subscription: this.options.subscription,
        timestamp: now(),
        detail,
      });
      return "dead-letter";
    } catch (error) {
      // Losing a message is worse than redelivering it.
      this.options.logger?.error(
        withEventFields(`werken: dead-letter publish failed: ${asMessage(error)} — nacking`, message),
      );
      return "nack";
    }
  }
}

function decodeReason(error: unknown): string {
  return error instanceof Error ? error.name : "unknown";
}

function plainJson(message: IncomingMessage): unknown {
  return message.data.length === 0 ? undefined : JSON.parse(message.data.toString("utf8"));
}

function asMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

interface ObservableLike {
  subscribe(observer: { next?: (v: unknown) => void; error: (e: unknown) => void; complete: () => void }): unknown;
}

function isObservableLike(value: unknown): value is ObservableLike {
  return typeof (value as ObservableLike | null)?.subscribe === "function";
}

/**
 * Nest wraps controller handlers so they return an Observable, not a plain value. Awaiting that
 * Observable is a no-op — it is not thenable — so a handler that throws would resolve as success
 * and the message would be ACKED and lost. Subscribe and wait for completion instead.
 *
 * Detected structurally so rxjs stays a transitive concern of @nestjs/microservices rather than a
 * runtime dependency of this package.
 */
async function settle(result: unknown): Promise<void> {
  if (!isObservableLike(result)) return;

  await new Promise<void>((resolve, reject) => {
    result.subscribe({
      error: (error: unknown) => reject(error),
      complete: () => resolve(),
    });
  });
}
