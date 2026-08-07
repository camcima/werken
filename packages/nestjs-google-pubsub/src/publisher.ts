import { uuidv7 } from "uuidv7";
import { toPubSubAttributes } from "@werken/cloudevents";
import { optionalRequire } from "./optional-require.cjs";
import type { PubSubClientLike, TopicLike } from "./options.js";
import { applyResourcePrefix, assertResourcePrefixSafe } from "./resource-name.js";

export interface PublishRequest<T> {
  type: string;
  data: T;
  /**
   * Overrides the generated event id. Supply one when the id has to survive a republish: a
   * transactional outbox mints it inside the ingest transaction and stores it on the row, so a
   * relay that crashes between publishing and marking republishes under the same id and consumer
   * de-duplication collapses the two into one. Left to generate, that republish is a second event.
   *
   * CloudEvents 1.0 requires this to be unique per `source`. Nothing here enforces it, and nothing
   * could: a reused id is indistinguishable from a redelivery to every consumer downstream.
   *
   * Sent verbatim, whitespace and all — only the emptiness check trims. Consumers de-duplicate on
   * the literal value, so a republish of the same stored id still collapses into one event.
   */
  id?: string;
  subject?: string;
  /** Overrides the configured source. Rarely needed. */
  source?: string;
  /** Overrides the key derived from `subject`. */
  orderingKey?: string;
  /** Explicit occurrence time. Defaults to now. */
  time?: Date;
  /**
   * When the platform learned of the event, if that is not now. A relay publishes what its ingest
   * transaction committed earlier, so defaulting this to publish time folds the queueing delay away
   * and leaves ingest lag and relay lag indistinguishable in a per-stage lead-time SLI.
   */
  ingestiontime?: Date;
  dataschema?: string;
  extensions?: Record<string, string>;
}

export interface PublishOptions {
  /** Bypasses the topic resolver. */
  topic?: string;
}

export interface EventPublisher {
  publish<T>(request: PublishRequest<T>, options?: PublishOptions): Promise<string>;
  /**
   * Publishes every request, in order. Throws {@link PartialPublishError} if any failed — the
   * successes are already on the topic by then, so the error names them rather than leaving the
   * caller to guess.
   */
  publishBatch<T>(requests: Array<PublishRequest<T>>, options?: PublishOptions): Promise<string[]>;
}

/** A request that made it, identified by its position in the batch. */
export interface PublishSuccess {
  readonly index: number;
  readonly messageId: string;
}

/** A request that did not, identified by its position in the batch. */
export interface PublishFailure {
  readonly index: number;
  readonly type: string;
  readonly cause: unknown;
}

/**
 * Raised when part of a batch published and part did not.
 *
 * Pub/Sub has no multi-message transaction, so a partly-failed batch leaves the successful messages
 * published and impossible to unsend. A bare throw would tell the caller nothing about which ones,
 * making the only safe recovery — retry just the failures — impossible: retrying the whole batch
 * would duplicate everything that already went out.
 */
export class PartialPublishError extends Error {
  constructor(
    readonly published: readonly PublishSuccess[],
    readonly failures: readonly PublishFailure[],
  ) {
    super(
      `werken: ${failures.length} of ${published.length + failures.length} events failed to publish. ` +
        `${published.length} were published and cannot be unsent — retry only the failures.`,
      { cause: failures[0]?.cause },
    );
    this.name = "PartialPublishError";
  }
}

/**
 * What an `encode` function hands back: the bytes, and optionally the media type they are in.
 *
 * Bare bytes mean `application/json`, which keeps the built-in `JSON.stringify` path and existing
 * encoders working unchanged.
 */
export type EncodedPayload = Buffer | { readonly data: Buffer; readonly datacontenttype: string };

/** Where a request is going and under which key — needed both to publish it and to recover it. */
interface Destination {
  readonly topicName: string;
  readonly orderingKey?: string;
}

export interface EventPublisherOptions {
  /** This service's `ce-source`. */
  source: string;
  client: PubSubClientLike;
  /** Maps an event type to a topic name. Returning undefined fails the publish. */
  topicResolver: (type: string) => string | undefined;
  /**
   * Encodes the payload. Without one, bodies are plain JSON — which Pub/Sub rejects on a topic
   * with a schema attached, since Pub/Sub's JSON encoding is Avro JSON rather than plain JSON.
   *
   * Return bare bytes and the event declares `application/json`. Return `{ data, datacontenttype }`
   * to say what the bytes actually are, which is what protobuf, CBOR, binary Avro or a compressed
   * body needs — otherwise a standards-aware consumer picks its decoder from a lie.
   */
  encode?: (type: string, data: unknown) => EncodedPayload;
  /** Derives the ordering key from `subject` and enables ordering on the topic. */
  ordering?: boolean;
  /** Development-only resource scoping — see `WerkenTransportOptions.resourcePrefix`. */
  resourcePrefix?: string;
  allowUnsafeResourcePrefix?: boolean;
  now?: () => Date;
}

/**
 * Publishes CloudEvents to Pub/Sub.
 *
 * Deliberately not built on Nest's `ClientProxy`: its request/response semantics are a poor fit for
 * fire-and-forget events, and its default serializer wraps the payload in a Nest-shaped envelope —
 * putting a framework detail on a contract consumed company-wide.
 */
export function createEventPublisher(options: EventPublisherOptions): EventPublisher {
  assertResourcePrefixSafe(options.resourcePrefix, options.allowUnsafeResourcePrefix === true, process.env.NODE_ENV);

  const now = options.now ?? (() => new Date());

  // One Topic per destination, kept for the publisher's lifetime. Each `client.topic()` call
  // returns a Topic with its own publisher and batch queue, so building one per message means the
  // SDK's batching never engages and every publish pays the full per-message overhead.
  const topics = new Map<string, TopicLike>();
  function topicFor(name: string): TopicLike {
    let topic = topics.get(name);
    if (topic === undefined) {
      // The SDK ignores orderingKey unless the Topic itself was constructed with messageOrdering.
      topic = options.client.topic(name, options.ordering === true ? { messageOrdering: true } : undefined);
      topics.set(name, topic);
    }
    return topic;
  }

  function destinationFor<T>(request: PublishRequest<T>, publishOptions?: PublishOptions): Destination {
    const resolved = publishOptions?.topic ?? options.topicResolver(request.type);
    if (resolved === undefined || resolved === "") {
      throw new Error(
        `werken: no topic resolved for event type ${JSON.stringify(request.type)}. ` +
          "Add it to the topic resolver, or pass an explicit topic.",
      );
    }
    const key = options.ordering === true ? (request.orderingKey ?? request.subject) : request.orderingKey;
    return {
      topicName: applyResourcePrefix(resolved, options.resourcePrefix),
      orderingKey: key === undefined || key === "" ? undefined : key,
    };
  }

  /**
   * A failed keyed publish leaves the SDK rejecting every later message on that key, so a publisher
   * that never resumes goes permanently deaf on it. Optional on TopicLike, so a custom client that
   * predates this keeps the old behaviour rather than failing to compile.
   */
  function resumeOrdering(destination: Destination): void {
    if (destination.orderingKey === undefined) return;
    try {
      topicFor(destination.topicName).resumePublishing?.(destination.orderingKey);
    } catch {
      // Swallowed: recovery must never replace the outcome it is recovering from. `client.topic()`
      // and `resumePublishing` are both caller-supplied, so a throw from either would surface in
      // place of the publish error and take `PartialPublishError.published` with it — leaving an
      // outbox relay to roll back and republish everything that already went out.
    }
  }

  function resumeFailed(attempted: ReadonlyArray<Destination | undefined>, failures: readonly PublishFailure[]): void {
    // JSON rather than a delimiter, for the same reason `idempotencyKeyToString` uses it: a topic
    // name and an ordering key are both caller-controlled, so any separator either could contain
    // would flatten two distinct destinations to one string and skip a resume that was needed.
    const seen = new Set<string>();
    for (const failure of failures) {
      // What the publish was attempted on, never a destination recomputed from the request. An
      // empty id, a throwing `encode` and a datacontenttype that is not one all fail with a
      // resolvable destination but nothing queued under its key, and resuming an untouched key is
      // not a no-op: the SDK's resume drains a queue whose only batch has already been dispatched,
      // deleting it, so the next publish on that key builds a fresh queue racing an outstanding
      // RPC. It also stops a `topicResolver` that reads mutable config from naming a different
      // topic on the way back, resuming a key nothing failed on and skipping the one that did.
      const destination = attempted[failure.index];
      if (destination?.orderingKey === undefined) continue;

      const seenKey = JSON.stringify([destination.topicName, destination.orderingKey]);
      if (seen.has(seenKey)) continue;
      seen.add(seenKey);
      resumeOrdering(destination);
    }
  }

  async function publishOne<T>(
    request: PublishRequest<T>,
    publishOptions: PublishOptions | undefined,
    // Called with the destination this publish is about to be attempted on, and only then, so both
    // callers recover from what was reached rather than from what can be recomputed afterwards.
    recordAttempt: (destination: Destination) => void,
  ): Promise<string> {
    // Rejected rather than defaulted. A silent fallback to uuidv7() would hand a fresh id to a
    // caller who believes they pinned a stable one — worse off than a caller who never tried, and
    // invisible until two events show up downstream for one state change.
    if (request.id !== undefined && request.id.trim() === "") {
      throw new Error(
        "werken: PublishRequest.id was supplied but is empty. Omit it to generate one, or pass a non-empty id.",
      );
    }

    const destination = destinationFor(request, publishOptions);

    const at = now();

    // Encoded before the attributes are built, because the encoder is what decides the media type
    // those attributes have to declare.
    const { data, datacontenttype } = normaliseEncoded(
      options.encode?.(request.type, request.data) ?? Buffer.from(JSON.stringify(request.data)),
    );

    const attributes = toPubSubAttributes({
      specversion: "1.0",
      id: request.id ?? uuidv7(),
      source: request.source ?? options.source,
      type: request.type,
      subject: request.subject,
      // ce-time is when it happened; ingestiontime is when the platform learned of it. Late and
      // out-of-order arrivals need both to be reasoned about.
      time: request.time ?? at,
      ingestiontime: request.ingestiontime ?? at,
      datacontenttype,
      dataschema: request.dataschema,
      traceparent: currentTraceparent(),
      extensions: request.extensions ?? {},
    });

    const topic = topicFor(destination.topicName);
    // Recorded with the Topic in hand and nothing left between here and the publish: from this
    // point on the SDK can have queued the message under the key, which is the only condition
    // under which resuming that key is a recovery rather than a hazard.
    recordAttempt(destination);

    const messageId = await topic.publishMessage({
      data,
      attributes,
      ...(destination.orderingKey === undefined ? {} : { orderingKey: destination.orderingKey }),
    });
    return String(messageId);
  }

  return {
    // Wrapped rather than passed by reference, so publishOne's internal attempt sink stays off the
    // public signature.
    async publish<T>(request: PublishRequest<T>, publishOptions?: PublishOptions) {
      let attempted: Destination | undefined;
      try {
        return await publishOne(request, publishOptions, (destination) => {
          attempted = destination;
        });
      } catch (error) {
        // Unset means the failure landed before `publishMessage`, so no key was ever suspended.
        if (attempted !== undefined) resumeOrdering(attempted);
        throw error;
      }
    },
    async publishBatch(requests, publishOptions) {
      const attempted = new Array<Destination | undefined>(requests.length);
      // Every publish is issued before any is awaited. Awaiting each in turn would flush a batch of
      // one — the SDK batches what is queued together — while the calls still happen in request
      // order, which is what preserves ordering per ordering key.
      const settled = await Promise.allSettled(
        requests.map((request, index) =>
          publishOne(request, publishOptions, (destination) => {
            attempted[index] = destination;
          }),
        ),
      );

      const failures = settled.flatMap<PublishFailure>((result, index) =>
        result.status === "rejected" ? [{ index, type: requests[index].type, cause: result.reason }] : [],
      );
      if (failures.length > 0) {
        // Resumed only now, with every publish settled. The whole batch is queued on a key before
        // the first failure is observable, so lifting the suspension any earlier could release a
        // later message in this same batch ahead of the one that failed.
        resumeFailed(attempted, failures);

        const published = settled.flatMap<PublishSuccess>((result, index) =>
          result.status === "fulfilled" ? [{ index, messageId: result.value }] : [],
        );
        throw new PartialPublishError(published, failures);
      }

      return settled.map((result) => (result as PromiseFulfilledResult<string>).value);
    },
  };
}

/**
 * Lifts the W3C traceparent from the ambient OpenTelemetry context, so a consumer's span joins the
 * producer's trace. Absent OTel, or outside a span, this contributes nothing.
 */
function currentTraceparent(): string | undefined {
  const api = optionalRequire("@opentelemetry/api") as typeof import("@opentelemetry/api") | undefined;
  if (api === undefined) return undefined;
  try {
    const carrier: Record<string, string> = {};
    api.propagation.inject(api.context.active(), carrier);
    return carrier["traceparent"];
  } catch {
    return undefined;
  }
}

const DEFAULT_CONTENT_TYPE = "application/json";

/**
 * Deliberately shallow: `type/subtype` with optional parameters, enough to catch a mistake before
 * it reaches the wire as an invalid CloudEvent, without reimplementing RFC 2045 parameter parsing.
 */
const MEDIA_TYPE = /^[A-Za-z0-9!#$%&'*+.^_`|~-]+\/[A-Za-z0-9!#$%&'*+.^_`|~-]+\s*(;.*)?$/;

function normaliseEncoded(encoded: EncodedPayload): { data: Buffer; datacontenttype: string } {
  if (Buffer.isBuffer(encoded)) return { data: encoded, datacontenttype: DEFAULT_CONTENT_TYPE };

  if (!MEDIA_TYPE.test(encoded.datacontenttype)) {
    throw new Error(
      `werken: encode returned datacontenttype ${JSON.stringify(encoded.datacontenttype)}, ` +
        'which is not a media type. Expected something like "application/protobuf".',
    );
  }
  return { data: encoded.data, datacontenttype: encoded.datacontenttype };
}
