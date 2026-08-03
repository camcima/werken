import { RpcException } from "@nestjs/microservices";
import type { PubSubClientLike } from "./options.js";
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
} as const;

/** Publishes terminal messages to an explicitly configured Pub/Sub topic. */
export class PubSubDeadLetterPublisher implements DeadLetterPublisher {
  constructor(
    private readonly client: PubSubClientLike,
    private readonly topic: string,
  ) {}

  async publish(request: DeadLetterRequest): Promise<void> {
    await this.client.topic(this.topic).publishMessage({
      data: request.message.data,
      attributes: deadLetterAttributes(request),
    });
  }
}

/** Builds the attribute set for a dead-lettered message: the original plus provenance. */
function deadLetterAttributes(request: DeadLetterRequest): Record<string, string> {
  const orderingKey = request.message.orderingKey;
  return {
    ...request.message.attributes,
    [DEAD_LETTER_ATTRIBUTES.reason]: request.reason,
    [DEAD_LETTER_ATTRIBUTES.sourceSubscription]: request.subscription,
    [DEAD_LETTER_ATTRIBUTES.timestamp]: request.timestamp.toISOString(),
    [DEAD_LETTER_ATTRIBUTES.stage]: request.stage,
    ...(request.detail === undefined ? {} : { [DEAD_LETTER_ATTRIBUTES.detail]: serialiseDetail(request.detail) }),
    // Provenance only. Republishing under a real ordering key would need the dead-letter topic
    // built with messageOrdering, and would serialise dead-letter publishes per key — a slow path
    // made slower exactly when things are already going wrong.
    ...(orderingKey === undefined || orderingKey === "" ? {} : { [DEAD_LETTER_ATTRIBUTES.orderingKey]: orderingKey }),
  };
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
