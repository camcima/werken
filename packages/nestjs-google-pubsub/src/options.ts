import type avro from "avsc";
import type { IdempotencyStore, SqlIdempotencyStoreOptions } from "./idempotency.js";
import type { RejectionPolicy, ValidationOptions } from "./pipeline.js";

/**
 * The subset of `@google-cloud/pubsub`'s `Subscription` this transport drives. Declared
 * structurally so the transport can be unit-tested without a broker.
 */
export interface SubscriptionLike {
  // Signature kept wide enough for both `@google-cloud/pubsub`'s Subscription and a plain
  // EventEmitter, so the transport can be driven in-memory by the test harness.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- must match EventEmitter's own `on`, which uses any[]
  on(event: string, listener: (...args: any[]) => void): unknown;
  removeAllListeners(): unknown;
  close(): Promise<void> | void;
}

export interface SchemaRegistryOptions {
  /**
   * Compiled reader type for a schema name, from the types this consumer imports — never from the
   * registry (§5.3). Returning undefined means the consumer cannot read that schema at all.
   */
  readerTypeFor: (schemaName: string) => avro.Type | undefined;
  /** Fail closed if a writer schema cannot be fetched. Default true. */
  strict?: boolean;
  /** Default 1 hour. */
  cacheTtlMs?: number;
  /** LRU bound. Default 200. */
  maxCachedRevisions?: number;
}

/** The subset of `@google-cloud/pubsub`'s `Topic` used for dead-letter publishing. */
export interface TopicLike {
  publishMessage(message: { data: Buffer; attributes: Record<string, string> }): Promise<unknown>;
}

/** The subset of `@google-cloud/pubsub`'s `PubSub` client this transport uses. */
/** The subset of `@google-cloud/pubsub`'s `Schema` used to fetch a writer schema by revision. */
export interface SchemaLike {
  get(): Promise<{ definition?: string | null }>;
}

export interface PubSubClientLike {
  subscription(name: string, options?: unknown): SubscriptionLike;
  topic(name: string): TopicLike;
  /** Accepts a revision-qualified name (`name@revisionId`) — confirmed in SPIKE-0. */
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
    /** Defaults to 1. Raising it interacts with ordering — see docs/ordering.md. */
    maxStreams?: number;
  };

  /** Regional endpoint override. */
  apiEndpoint?: string;

  /**
   * Topic to publish terminal messages to (§4.4). Deliberately explicit rather than relying on the
   * subscription's own dead-letter policy, which only triggers after every retry is exhausted.
   * Without it, terminal messages nack rather than being silently dropped.
   */
  deadLetterTopic?: string;

  /** Avro schema resolution. Without it, message bodies are parsed as plain JSON. */
  schemaRegistry?: SchemaRegistryOptions;

  /**
   * Duplicate suppression. Omitting it installs a no-op store that warns loudly at startup rather
   * than silently reprocessing duplicates (§5.4).
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
