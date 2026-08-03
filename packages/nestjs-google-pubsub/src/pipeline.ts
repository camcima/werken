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
import type { AvroCodec } from "./schema/avro-codec.js";
import type { CloudEventContext, IncomingMessage } from "./types.js";

/** What the transport should do with a message once the pipeline has run. */
export type Outcome = "ack" | "nack" | "dead-letter";

/** Policy for a message the pipeline refuses to process. */
export type RejectionPolicy = "dead-letter" | "nack" | "ack";

export type EventHandler = (data: unknown, ctx: CloudEventContext) => unknown;

export interface ValidationOptions {
  /** Default 'dead-letter'. */
  onInvalidEnvelope?: RejectionPolicy;
  /** Default 'dead-letter'. */
  onDecodeFailure?: RejectionPolicy;
  /** Default false. */
  requireDataschema?: boolean;
}

export interface MessagePipelineOptions {
  readonly subscription: string;
  /** Exact-match handler lookup. Wildcard precedence arrives in M10. */
  readonly resolveHandler: (pattern: string) => EventHandler | null;
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
 * Ordered stages a message passes through (§5.2). M4 covers envelope, routing, handler invocation
 * and outcomes; schema decode, idempotency and telemetry slot in later.
 */
export const DEFAULT_IDEMPOTENCY_TTL_MS = 7 * 24 * 60 * 60 * 1000;

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
    const id = message.attributes["ce-id"];
    const source = message.attributes["ce-source"];
    if (this.options.idempotencyStore === undefined || id === undefined || source === undefined) {
      return this.run(message);
    }

    const dedupeKey = idempotencyKeyToString(this.keyFor(source, id));
    const running = this.inProcess.get(dedupeKey);
    if (running !== undefined) return running;

    const pending = this.run(message).finally(() => this.inProcess.delete(dedupeKey));
    this.inProcess.set(dedupeKey, pending);
    return pending;
  }

  private keyFor(source: string, id: string): IdempotencyKey {
    return { consumer: this.options.consumer ?? "werken", source, id };
  }

  private async run(message: IncomingMessage): Promise<Outcome> {
    const now = this.options.now ?? (() => new Date());
    const telemetry = this.options.telemetry;

    let envelope;
    try {
      envelope = parseEnvelope(message.attributes);
      if (this.options.validation?.requireDataschema === true && envelope.dataschema === undefined) {
        throw new Error("ce-dataschema is required but absent");
      }
    } catch (error) {
      return this.reject(message, {
        policy: this.options.validation?.onInvalidEnvelope ?? "dead-letter",
        stage: "envelope",
        reason: asMessage(error),
        now,
      });
    }

    telemetry?.recordReceived(envelope.type, this.options.subscription);
    // Lateness is an operational signal in its own right, not just a debugging aid: for events that
    // routinely arrive long after they happened, its distribution is the thing worth alerting on.
    if (envelope.time !== undefined) telemetry?.recordLateness(envelope.type, envelope.time, now());

    const handler = this.options.resolveHandler(envelope.type);
    if (handler === null) {
      const outcome = await this.reject(message, {
        policy: this.options.onUnhandledPattern ?? "ack",
        stage: "unhandled",
        reason: `no handler registered for ${envelope.type}`,
        now,
      });
      telemetry?.recordOutcome(envelope.type, outcome);
      return outcome;
    }

    // The message span opens once a handler is known (§5.2), covering the idempotency check,
    // decode, the handler and the idempotency record — parented on the producer's ce-traceparent
    // so the werken.decode/werken.handler child spans join the producer's trace.
    const work = () => this.dispatch(handler, envelope, message, now);
    const result =
      telemetry === undefined
        ? await work()
        : await telemetry.withMessageSpan(
            { envelope, subscription: this.options.subscription, messageId: message.id },
            work,
          );
    telemetry?.recordOutcome(envelope.type, result);
    return result === "skipped_duplicate" ? "ack" : result;
  }

  private async dispatch(
    handler: EventHandler,
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
      telemetry?.recordDecodeFailure(envelope.type, decodeReason(error));
      return this.reject(message, {
        policy: this.options.validation?.onDecodeFailure ?? "dead-letter",
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
      telemetry?.recordHandlerDuration(envelope.type, Date.now() - startedAt);
      await this.record(key, ctx);
      return "ack";
    } catch (error) {
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
      telemetry?.recordHandlerDuration(envelope.type, Date.now() - startedAt);
      this.options.logger?.error(withEventFields(`werken: handler failed: ${asMessage(error)}`, message));
      return "nack";
    }
  }

  /**
   * Recorded only after the handler succeeded and before the ack. Recording earlier risks silently
   * dropping a message that then failed; recording after the ack risks a crash in between. Neither
   * is eliminable — this is at-least-once — which is why handlers must still be idempotent.
   */
  private async decode(message: IncomingMessage): Promise<unknown> {
    return this.options.codec === undefined
      ? plainJson(message)
      : await this.options.codec.decode(message.data, schemaMetaFromAttributes(message.attributes));
  }

  private async record(key: IdempotencyKey, ctx: CloudEventContext): Promise<void> {
    const store = this.options.idempotencyStore;
    if (store === undefined) return;
    try {
      await store.tryRecord(key, this.options.idempotencyTtlMs ?? DEFAULT_IDEMPOTENCY_TTL_MS, ctx);
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
