import { RpcException } from "@nestjs/microservices";
import type { PubSubClientLike, TopicLike } from "./options.js";
import type { IncomingMessage } from "./types.js";

/**
 * Raised by a handler for a message that is schema-valid but will never be processable by *this*
 * consumer — a referential integrity failure, or a business rule that will never become true.
 *
 * Such a message must not burn its retry budget: behind an ordering key it blocks every subsequent
 * event for that entity until it clears.
 */
export class TerminalEventError extends RpcException {
  readonly reason: string;
  readonly detail?: Record<string, unknown>;

  constructor(reason: string, detail?: Record<string, unknown>) {
    // Extending RpcException is load-bearing, not cosmetic. Nest's RpcExceptionsHandler replaces
    // ANY other thrown error with `{ status: 'error', message: 'Internal server error' }` before
    // the transport ever sees it, which would silently turn every terminal failure into a plain
    // nack and an infinite retry loop. An RpcException's payload is passed through untouched.
    super({ [TERMINAL_MARKER]: true, reason, detail } satisfies TerminalPayload);
    this.name = "TerminalEventError";
    this.reason = reason;
    this.detail = detail;
  }
}

/** Marker so a terminal failure is recognisable after Nest has unwrapped it to a plain payload. */
const TERMINAL_MARKER = "__werkenTerminalEvent";

interface TerminalPayload {
  readonly [TERMINAL_MARKER]: true;
  readonly reason: string;
  readonly detail?: Record<string, unknown>;
}

/**
 * Recognises a terminal failure in either form:
 *  - the `TerminalEventError` itself, when a handler is invoked directly
 *  - the unwrapped payload, when the throw passed through Nest's exception filter
 */
export function asTerminalFailure(error: unknown): { reason: string; detail?: Record<string, unknown> } | null {
  if (error instanceof TerminalEventError) {
    return { reason: error.reason, detail: error.detail };
  }
  const payload = error as Partial<TerminalPayload> | null;
  if (typeof payload === "object" && payload !== null && payload[TERMINAL_MARKER] === true) {
    return { reason: payload.reason ?? "terminal event", detail: payload.detail };
  }
  return null;
}

/** Which pipeline stage rejected the message. */
export type DeadLetterStage = "envelope" | "decode" | "handler" | "unhandled";

export interface DeadLetterRequest {
  readonly message: IncomingMessage;
  readonly reason: string;
  readonly stage: DeadLetterStage;
  readonly subscription: string;
  readonly timestamp: Date;
  readonly detail?: Record<string, unknown>;
}

/**
 * Publishes to an explicitly configured dead-letter topic.
 *
 * Deliberately not the subscription's own dead-letter policy: exhausting that means the consumer
 * slowly grinds through every retry first, which is far too slow for a message we already know is
 * terminal.
 */
export interface DeadLetterPublisher {
  publish(request: DeadLetterRequest): Promise<void>;
}

/** Provenance attributes added alongside the original message's own. */
export const DEAD_LETTER_ATTRIBUTES = {
  reason: "werken-dl-reason",
  sourceSubscription: "werken-dl-source-subscription",
  timestamp: "werken-dl-timestamp",
  stage: "werken-dl-stage",
  /** JSON of the structured `detail` a TerminalEventError carried, when there was any. */
  detail: "werken-dl-detail",
  /** The original ordering key, so redrive tooling can restore per-entity order. */
  orderingKey: "werken-dl-ordering-key",
  /**
   * How many of the original's own attributes had to be dropped to fit Pub/Sub's 100-attribute
   * limit. Absent when none were — which is every ordinary message.
   */
  droppedAttributes: "werken-dl-dropped-attributes",
} as const;

/** Publishes terminal messages to an explicitly configured Pub/Sub topic. */
export class PubSubDeadLetterPublisher implements DeadLetterPublisher {
  /**
   * Resolved once, for the same reason `createEventPublisher` keeps one Topic per destination:
   * every `client.topic()` call returns a Topic with its own publisher and batch queue, so one per
   * message means the SDK's batching never engages and each publish pays full overhead. Dead-letter
   * volume is normally low, and the moment it is not is a poison-message storm — which is when this
   * path should not also be at its slowest.
   *
   * Lazy rather than built in the constructor: the transport constructs this during startup, and
   * resolving a topic is the client's work to do when it is actually needed.
   */
  private resolved?: TopicLike;

  constructor(
    private readonly client: PubSubClientLike,
    private readonly topic: string,
  ) {}

  async publish(request: DeadLetterRequest): Promise<void> {
    this.resolved ??= this.client.topic(this.topic);
    await this.resolved.publishMessage({
      data: request.message.data,
      attributes: deadLetterAttributes(request),
    });
  }
}

/** Builds the attribute set for a dead-lettered message: the original plus provenance. */
function deadLetterAttributes(request: DeadLetterRequest): Record<string, string> {
  const orderingKey = request.message.orderingKey;
  const provenance: Record<string, string> = {
    // Truncated because it is the one provenance value with no bound of its own: an envelope error
    // interpolates the offending attribute (itself up to the full 1024 bytes) and a
    // TerminalEventError reason is whatever the application passed.
    [DEAD_LETTER_ATTRIBUTES.reason]: truncateAttribute(request.reason),
    [DEAD_LETTER_ATTRIBUTES.sourceSubscription]: request.subscription,
    [DEAD_LETTER_ATTRIBUTES.timestamp]: request.timestamp.toISOString(),
    [DEAD_LETTER_ATTRIBUTES.stage]: request.stage,
    ...(request.detail === undefined ? {} : { [DEAD_LETTER_ATTRIBUTES.detail]: serialiseDetail(request.detail) }),
    // Provenance only. Republishing under a real ordering key would need the dead-letter topic
    // built with messageOrdering, and would serialise dead-letter publishes per key — a slow path
    // made slower exactly when things are already going wrong.
    ...(orderingKey === undefined || orderingKey === "" ? {} : { [DEAD_LETTER_ATTRIBUTES.orderingKey]: orderingKey }),
  };

  return withinAttributeLimit(request.message.attributes, provenance);
}

/**
 * Pub/Sub accepts at most 100 attributes per message, and the original's are forwarded verbatim —
 * so a message already at the limit would push this publish past it once provenance is added. The
 * publish failing is the one outcome that must not happen here: the pipeline answers it with a
 * nack, which redelivers the poison message into the same failure indefinitely.
 *
 * Provenance is kept whole, because it is the reason the message is on this topic at all. The
 * envelope goes next, because redrive republishes from it. A producer's own annotations are what
 * gives, and the count that went is reported rather than left to be inferred from a gap.
 */
const MAX_ATTRIBUTES = 100;
const CE_PREFIX = "ce-";

function withinAttributeLimit(
  original: Readonly<Record<string, string>>,
  provenance: Record<string, string>,
): Record<string, string> {
  // Keys provenance overwrites cost nothing extra, so they are not counted twice — which is the
  // case when a message that was already dead-lettered once passes through again.
  const carried = Object.keys(original).filter((key) => !(key in provenance));
  if (Object.keys(provenance).length + carried.length <= MAX_ATTRIBUTES) {
    return { ...original, ...provenance };
  }

  const ordered = [
    ...carried.filter((key) => key.startsWith(CE_PREFIX)),
    ...carried.filter((key) => !key.startsWith(CE_PREFIX)),
  ];
  // One slot reserved for the marker below, which only exists on this path.
  const budget = Math.max(MAX_ATTRIBUTES - Object.keys(provenance).length - 1, 0);
  const kept = ordered.slice(0, budget);

  const attributes: Record<string, string> = {};
  for (const key of kept) attributes[key] = original[key];
  return {
    ...attributes,
    ...provenance,
    [DEAD_LETTER_ATTRIBUTES.droppedAttributes]: String(ordered.length - kept.length),
  };
}

/** Marks a value as cut rather than letting it read as though the text simply ended there. */
const TRUNCATION_MARKER = "…[truncated]";

function truncateAttribute(value: string): string {
  if (Buffer.byteLength(value, "utf8") <= MAX_ATTRIBUTE_BYTES) return value;

  const budget = MAX_ATTRIBUTE_BYTES - Buffer.byteLength(TRUNCATION_MARKER, "utf8");
  let bytes = 0;
  let head = "";
  // Accumulated per code point rather than sliced per byte: slicing bytes can cut a multi-byte
  // sequence into a U+FFFD replacement character, or a surrogate pair into a lone surrogate.
  for (const character of value) {
    const size = Buffer.byteLength(character, "utf8");
    if (bytes + size > budget) break;
    bytes += size;
    head += character;
  }
  return `${head}${TRUNCATION_MARKER}`;
}

/**
 * Pub/Sub caps an attribute value at 1024 bytes. The emulator does not enforce that — verified —
 * so it cannot be relied on to surface an oversized value during testing.
 *
 * Truncating JSON mid-string yields something nothing can parse, so oversized or unserialisable
 * detail becomes a marker naming what happened instead. This never throws: losing the message is
 * far worse than losing the diagnostic context attached to it.
 */
const MAX_ATTRIBUTE_BYTES = 1024;

function serialiseDetail(detail: Record<string, unknown>): string {
  let json: string | undefined;
  try {
    json = JSON.stringify(detail);
  } catch (error) {
    return JSON.stringify({
      unserialisable: true,
      reason: error instanceof Error ? error.message : String(error),
    });
  }

  if (json === undefined) return JSON.stringify({ unserialisable: true, reason: "not representable as JSON" });

  const bytes = Buffer.byteLength(json, "utf8");
  return bytes <= MAX_ATTRIBUTE_BYTES ? json : JSON.stringify({ truncated: true, bytes });
}
