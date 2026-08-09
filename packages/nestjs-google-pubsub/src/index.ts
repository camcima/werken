/**
 * The supported public API.
 *
 * Deliberately narrow: everything here is documented in the README and covered by semver, and the
 * engine behind it — the pipeline, router, codec, schema cache, telemetry facade and resource-name
 * helpers — is not exported at all. A published surface that nothing documents is one nobody can
 * change safely, and at 0.x this is the cheap moment to draw the line.
 *
 * Those internals live in `internal.ts`, which is absent from the package's `exports` map and so
 * cannot be resolved from an installed copy. If you need something that is not here, open an issue
 * rather than reaching past this file — that way it gets documented and kept working.
 */

// ---------------------------------------------------------------------------
// Consuming events
// ---------------------------------------------------------------------------

export { WerkenPubSubTransport } from "./transport.js";
export type { WerkenTransportEvents, WerkenTransportStatus } from "./transport.js";
export type { CloudEventContext, IncomingMessage } from "./types.js";
export type { EventHandler, Outcome, RejectionPolicy, ValidationOptions } from "./pipeline.js";
export type {
  FlowControlOptions,
  SchemaRegistryOptions,
  WerkenTransportOptions,
  PubSubClientLike,
  SchemaLike,
  SubscriptionLike,
  TopicLike,
} from "./options.js";

// ---------------------------------------------------------------------------
// Publishing events
// ---------------------------------------------------------------------------

export { createEventPublisher, OrderingKeyBlockedError, PartialPublishError } from "./publisher.js";
export type {
  EncodedPayload,
  EventPublisher,
  EventPublisherOptions,
  PublishFailure,
  PublishOptions,
  PublishRequest,
  PublishSuccess,
} from "./publisher.js";

// ---------------------------------------------------------------------------
// Dead-lettering
// ---------------------------------------------------------------------------

export { DEAD_LETTER_ATTRIBUTES, PubSubDeadLetterPublisher, TerminalEventError } from "./dead-letter.js";
export type { DeadLetterPublisher, DeadLetterRequest, DeadLetterStage } from "./dead-letter.js";

// ---------------------------------------------------------------------------
// Idempotency
// ---------------------------------------------------------------------------

export {
  createSqlIdempotencyStore,
  idempotencyKeyToString,
  pruneExpiredSql,
  DEFAULT_IDEMPOTENCY_TABLE,
  InMemoryIdempotencyStore,
  NoopIdempotencyStore,
} from "./idempotency.js";
export type {
  IdempotencyKey,
  IdempotencyStore,
  InMemoryIdempotencyStoreOptions,
  SqlExecutor,
  SqlIdempotencyStoreOptions,
} from "./idempotency.js";

// ---------------------------------------------------------------------------
// Errors worth catching by type
// ---------------------------------------------------------------------------

export { SchemaDecodeError } from "./schema/avro-codec.js";
export { ResourcePrefixError } from "./resource-name.js";
export { AmbiguousPatternError, InvalidPatternError } from "./pattern-router.js";
