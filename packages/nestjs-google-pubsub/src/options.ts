import type avro from "avsc";
import type { IdempotencyStore, SqlIdempotencyStoreOptions } from "./idempotency.js";
import type { RejectionPolicy, ValidationOptions } from "./pipeline.js";

/**
 * The subset of `@google-cloud/pubsub`'s `Subscription` this transport drives. Declared
 * structurally so the transport can be unit-tested without a broker.
 */
export interface SubscriptionLike {
  /** Present on a real Subscription; used only for the scoped-startup existence check. */
  exists?(): Promise<[boolean]>;
  // Signature kept wide enough for both `@google-cloud/pubsub`'s Subscription and a plain
  // EventEmitter, so the transport can be driven in-memory by the test harness.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- must match EventEmitter's own `on`, which uses any[]
  on(event: string, listener: (...args: any[]) => void): unknown;
  /** Called with "message" to stop taking work while the error listener stays attached. */
  removeAllListeners(event?: string): unknown;
  /**
   * False once the SDK has given up on the stream. Optional because it is a real `Subscription`
   * getter that a hand-rolled test double need not provide; absent is treated as open.
   */
  readonly isOpen?: boolean;
  close(): Promise<void> | void;
}

export interface SchemaRegistryOptions {
  /**
   * Compiled reader type for a schema name, from the types this consumer imports — never from the
   * registry. Returning undefined means the consumer cannot read that schema at all.
   */
  readerTypeFor: (schemaName: string) => avro.Type | undefined;
  /**
   * Fail closed if a writer schema cannot be *fetched* — a Schema Service outage, a client without
   * schema support, a revision that has not propagated. Default true.
   *
   * `false` trades correctness for availability on that one failure only, decoding the body as
   * plain JSON instead. It does not loosen anything else: a missing reader type, a definition that
   * is not valid Avro, a writer the reader cannot resolve, and incoherent schema metadata all stay
   * fatal, because there the schema is known and the message still cannot be read correctly.
   */
  strict?: boolean;
  /** Default 1 hour. */
  cacheTtlMs?: number;
  /** LRU bound. Default 200. */
  maxCachedRevisions?: number;
}

/** The subset of `@google-cloud/pubsub`'s `Topic` used for dead-letter publishing. */
export interface TopicLike {
  publishMessage(message: { data: Buffer; attributes: Record<string, string>; orderingKey?: string }): Promise<unknown>;
}

/** The subset of `@google-cloud/pubsub`'s `Schema` used to fetch a writer schema by revision. */
export interface SchemaLike {
  get(): Promise<{ definition?: string | null }>;
}

/** The subset of `@google-cloud/pubsub`'s `PubSub` client this library uses. */
export interface PubSubClientLike {
  subscription(name: string, options?: unknown): SubscriptionLike;
  topic(name: string, options?: unknown): TopicLike;
  /** Accepts a revision-qualified name (`name@revisionId`), verified against the emulator. */
  schema?(name: string): SchemaLike;
  close(): Promise<void> | void;
}

export interface FlowControlOptions {
  maxOutstandingMessages?: number;
  maxOutstandingBytes?: number;
  allowExcessMessages?: boolean;
}

export interface WerkenTransportOptions {
  /** GCP project containing the subscription. */
  projectId: string;

  /** Subscription short name or fully-qualified path. */
  subscription: string;

  flowControl?: FlowControlOptions;

  streaming?: {
    /**
     * Defaults to 1. More streams mean more concurrent delivery, but Pub/Sub still serialises
     * delivery per ordering key, so raising this does not speed up a single hot key.
     */
    maxStreams?: number;
  };

  ackDeadline?: {
    /** Initial ack deadline. Default 60s. */
    initialMs?: number;
    /**
     * Cap on automatic lease extension. Default 10 minutes. The SDK stops extending at this point
     * and the message's deadline simply lapses, so a handler that runs longer gets redelivered.
     */
    maxExtensionMs?: number;
  };

  /** Max wall-clock to drain in-flight handlers on shutdown. Default 30s. */
  shutdownDrainTimeoutMs?: number;

  /**
   * Prefix applied to the resolved subscription name, the dead-letter topic, and (via the
   * publisher) resolved topic names.
   *
   * Exists because Pub/Sub delivers each message to exactly one subscriber: developers sharing a
   * dev project and a subscription silently steal each other's messages, which presents as flaky
   * delivery. Scoping gives each developer their own resources in the same project.
   *
   * Development only. Undefined or empty is a no-op and is the production path. The library does
   * not create the scoped resources — provision them yourself.
   */
  resourcePrefix?: string;

  /** Permits `resourcePrefix` while NODE_ENV=production. Almost certainly a mistake. */
  allowUnsafeResourcePrefix?: boolean;

  /**
   * OpenTelemetry tracing and metrics. `@opentelemetry/api` is an optional peer dependency: if it
   * is not installed, every call degrades to a no-op rather than crashing.
   *
   * Spans only propagate if a ContextManager is registered — the OTel Node SDK does this for you.
   * Without one, `context.active()` always returns root and child spans come out unparented.
   */
  telemetry?: {
    /** Default true. When false, no OpenTelemetry calls are made at all. */
    enabled?: boolean;
    /** Tracer and meter name. Defaults to "werken". */
    serviceName?: string;
  };

  /** Regional endpoint override. */
  apiEndpoint?: string;

  /**
   * Topic to publish terminal messages to. Deliberately explicit rather than relying on the
   * subscription's own dead-letter policy, which only triggers after every retry is exhausted.
   * Without it, terminal messages nack rather than being silently dropped.
   */
  deadLetterTopic?: string;

  /** Avro schema resolution. Without it, message bodies are parsed as plain JSON. */
  schemaRegistry?: SchemaRegistryOptions;

  /**
   * Duplicate suppression. Omitting it installs a no-op store that warns loudly at startup rather
   * than silently reprocessing duplicates.
   */
  idempotency?: {
    /** Identifies this consumer in the key, so two services each process an event once. */
    consumer: string;
    /**
     * Postgres executor, resolved per message. Supplying this builds the built-in SQL store.
     * Mutually exclusive with `store`.
     */
    executor?: SqlIdempotencyStoreOptions["executor"];
    /** Table for the SQL store. Default `werken_processed_events`. */
    table?: string;
    /** A store of your own — for Redis, MongoDB, or anything not Postgres. */
    store?: IdempotencyStore;
    /** How long a processed marker is retained. Default 7 days. */
    ttlMs?: number;
  };

  validation?: ValidationOptions;

  /** What to do with a message no handler matches. Default 'ack'. */
  onUnhandledPattern?: RejectionPolicy;

  /**
   * Escape hatch for tests and for callers that need to own client construction. Defaults to
   * building a real `PubSub` from `@google-cloud/pubsub`, which stays a peer dependency.
   */
  createClient?: (options: WerkenTransportOptions) => PubSubClientLike;
}

export const DEFAULT_FLOW_CONTROL: Required<FlowControlOptions> = {
  maxOutstandingMessages: 50,
  maxOutstandingBytes: 20 * 1024 * 1024,
  allowExcessMessages: false,
};

export const DEFAULT_MAX_STREAMS = 1;
export const DEFAULT_ACK_DEADLINE_MS = 60_000;
export const DEFAULT_MAX_EXTENSION_MS = 600_000;
export const DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MS = 30_000;

/**
 * The subscriber options this transport hands to `PubSub#subscription()`, with ack deadlines still
 * as plain milliseconds.
 *
 * They are NOT converted to `Duration` here. The SDK calls `.total()` and `Duration.compare()` on
 * these values internally, so a hand-rolled duck-type crashes it during `subscription.close()` —
 * only the SDK's own `Duration` will do. The transport converts, because that is where the SDK is
 * loaded; keeping this function free of the import is what lets it be unit-tested without one.
 */
export interface SubscriberOptionsLike {
  flowControl: { maxMessages: number; maxBytes: number; allowExcessMessages: boolean };
  streamingOptions: { maxStreams: number };
  minAckDeadlineMs: number;
  maxExtensionTimeMs: number;
}

/**
 * Translates Werken's broker-neutral option names into the Node SDK's.
 *
 * This mapping is not cosmetic. Werken names these after the Pub/Sub concepts themselves
 * (`maxOutstandingMessages`, as the Python and Java clients do), but the Node client's own
 * `FlowControlOptions` uses
 * `maxMessages`/`maxBytes`. Passing our names straight through means the SDK silently ignores them
 * and applies its own far larger defaults — flow control that looks configured and is not.
 *
 * Ack deadlines are `Duration` objects in the SDK, not raw milliseconds, for the same reason.
 */
export function toSubscriberOptions(options: WerkenTransportOptions): SubscriberOptionsLike {
  const flow = { ...DEFAULT_FLOW_CONTROL, ...options.flowControl };
  return {
    flowControl: {
      maxMessages: flow.maxOutstandingMessages,
      maxBytes: flow.maxOutstandingBytes,
      allowExcessMessages: flow.allowExcessMessages,
    },
    streamingOptions: { maxStreams: options.streaming?.maxStreams ?? DEFAULT_MAX_STREAMS },
    minAckDeadlineMs: options.ackDeadline?.initialMs ?? DEFAULT_ACK_DEADLINE_MS,
    maxExtensionTimeMs: options.ackDeadline?.maxExtensionMs ?? DEFAULT_MAX_EXTENSION_MS,
  };
}
