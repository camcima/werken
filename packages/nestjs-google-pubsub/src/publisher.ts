import { uuidv7 } from "uuidv7";
import { toPubSubAttributes } from "@werken/cloudevents";
import { optionalRequire } from "./optional-require.cjs";
import type { PubSubClientLike, TopicLike } from "./options.js";
import { applyResourcePrefix, assertResourcePrefixSafe } from "./resource-name.js";

export interface PublishRequest<T> {
  type: string;
  data: T;
  subject?: string;
  /** Overrides the configured source. Rarely needed. */
  source?: string;
  /** Overrides the key derived from `subject`. */
  orderingKey?: string;
  /** Explicit occurrence time. Defaults to now. */
  time?: Date;
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
    topicFor(destination.topicName).resumePublishing?.(destination.orderingKey);
  }

  function resumeFailed<T>(
    requests: Array<PublishRequest<T>>,
    failures: readonly PublishFailure[],
    publishOptions?: PublishOptions,
  ): void {
    // JSON rather than a delimiter, for the same reason `idempotencyKeyToString` uses it: a topic
    // name and an ordering key are both caller-controlled, so any separator either could contain
    // would flatten two distinct destinations to one string and skip a resume that was needed.
    const seen = new Set<string>();
    for (const failure of failures) {
      let destination: Destination;
      try {
        destination = destinationFor(requests[failure.index], publishOptions);
      } catch {
        // Resolving the destination is itself one of the ways a request fails. If there is no
        // destination, nothing was ever queued under a key, so nothing is suspended.
        continue;
      }
      if (destination.orderingKey === undefined) continue;

      const seenKey = JSON.stringify([destination.topicName, destination.orderingKey]);
      if (seen.has(seenKey)) continue;
      seen.add(seenKey);
      resumeOrdering(destination);
    }
  }

  async function publishOne<T>(
    request: PublishRequest<T>,
    publishOptions: PublishOptions | undefined,
    // False from publishBatch, which resumes once per key after the whole batch has settled.
    resumeOnFailure: boolean,
  ): Promise<string> {
    const destination = destinationFor(request, publishOptions);

    const at = now();

    // Encoded before the attributes are built, because the encoder is what decides the media type
    // those attributes have to declare.
    const { data, datacontenttype } = normaliseEncoded(
      options.encode?.(request.type, request.data) ?? Buffer.from(JSON.stringify(request.data)),
    );

    const attributes = toPubSubAttributes({
      specversion: "1.0",
      id: uuidv7(),
      source: request.source ?? options.source,
      type: request.type,
      subject: request.subject,
      // ce-time is when it happened; ingestiontime is when the platform learned of it. Late and
      // out-of-order arrivals need both to be reasoned about.
      time: request.time ?? at,
      ingestiontime: at,
      datacontenttype,
      dataschema: request.dataschema,
      traceparent: currentTraceparent(),
      extensions: request.extensions ?? {},
    });

    try {
      const messageId = await topicFor(destination.topicName).publishMessage({
        data,
        attributes,
        ...(destination.orderingKey === undefined ? {} : { orderingKey: destination.orderingKey }),
      });
      return String(messageId);
    } catch (error) {
      if (resumeOnFailure) resumeOrdering(destination);
      throw error;
    }
  }

  return {
    // Wrapped rather than passed by reference, so publishOne's internal resume flag stays off the
    // public signature.
    publish: <T>(request: PublishRequest<T>, publishOptions?: PublishOptions) =>
      publishOne(request, publishOptions, true),
    async publishBatch(requests, publishOptions) {
      // Every publish is issued before any is awaited. Awaiting each in turn would flush a batch of
      // one — the SDK batches what is queued together — while the calls still happen in request
      // order, which is what preserves ordering per ordering key.
      const settled = await Promise.allSettled(requests.map((request) => publishOne(request, publishOptions, false)));

      const failures = settled.flatMap<PublishFailure>((result, index) =>
        result.status === "rejected" ? [{ index, type: requests[index].type, cause: result.reason }] : [],
      );
      if (failures.length > 0) {
        // Resumed only now, with every publish settled. The whole batch is queued on a key before
        // the first failure is observable, so lifting the suspension any earlier could release a
        // later message in this same batch ahead of the one that failed.
        resumeFailed(requests, failures, publishOptions);

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
