import { uuidv7 } from "uuidv7";
import { toPubSubAttributes } from "@werken/cloudevents";
import type { PubSubClientLike } from "./options.js";
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
  publishBatch<T>(requests: Array<PublishRequest<T>>, options?: PublishOptions): Promise<string[]>;
}

export interface EventPublisherOptions {
  /** This service's `ce-source`. */
  source: string;
  client: PubSubClientLike;
  /** Maps an event type to a topic name. Returning undefined fails the publish. */
  topicResolver: (type: string) => string | undefined;
  /**
   * Encodes the payload. Without one, bodies are plain JSON — which Pub/Sub rejects on a topic
   * with a schema attached, since its JSON encoding is Avro JSON (see SPIKE-1).
   */
  encode?: (type: string, data: unknown) => Buffer;
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

  async function publishOne<T>(request: PublishRequest<T>, publishOptions?: PublishOptions): Promise<string> {
    const resolved = publishOptions?.topic ?? options.topicResolver(request.type);
    if (resolved === undefined || resolved === "") {
      throw new Error(
        `werken: no topic resolved for event type ${JSON.stringify(request.type)}. ` +
          "Add it to the topic resolver, or pass an explicit topic.",
      );
    }
    const topicName = applyResourcePrefix(resolved, options.resourcePrefix);

    const at = now();
    const orderingKey = options.ordering === true ? (request.orderingKey ?? request.subject) : request.orderingKey;

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
      datacontenttype: "application/json",
      dataschema: request.dataschema,
      traceparent: currentTraceparent(),
      extensions: request.extensions ?? {},
    });

    const data = options.encode?.(request.type, request.data) ?? Buffer.from(JSON.stringify(request.data));

    // The SDK ignores orderingKey unless the Topic itself was constructed with messageOrdering.
    const topic = options.client.topic(topicName, options.ordering === true ? { messageOrdering: true } : undefined);

    const messageId = await topic.publishMessage({
      data,
      attributes,
      ...(orderingKey === undefined || orderingKey === "" ? {} : { orderingKey }),
    });
    return String(messageId);
  }

  return {
    publish: publishOne,
    async publishBatch(requests, publishOptions) {
      const ids: string[] = [];
      for (const request of requests) {
        ids.push(await publishOne(request, publishOptions));
      }
      return ids;
    },
  };
}

/**
 * Lifts the W3C traceparent from the ambient OpenTelemetry context, so a consumer's span joins the
 * producer's trace. Absent OTel, or outside a span, this contributes nothing.
 */
function currentTraceparent(): string | undefined {
  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports -- optional peer dependency
    const api = require("@opentelemetry/api") as typeof import("@opentelemetry/api");
    const carrier: Record<string, string> = {};
    api.propagation.inject(api.context.active(), carrier);
    return carrier["traceparent"];
  } catch {
    return undefined;
  }
}
