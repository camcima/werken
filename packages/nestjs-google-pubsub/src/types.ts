/**
 * The subset of `@google-cloud/pubsub`'s `Message` this transport relies on.
 *
 * Declared structurally rather than imported so the pipeline can be unit-tested without
 * constructing a real `Message` (which needs a live `Subscriber`), and so the SDK stays a peer
 * dependency rather than a hard one.
 */
export interface IncomingMessage {
  readonly id: string;
  readonly attributes: Readonly<Record<string, string>>;
  readonly data: Buffer;
  readonly publishTime: Date;
  /** Pub/Sub reports 0 when the subscription has no dead-letter policy. */
  readonly deliveryAttempt?: number;
  /** Pub/Sub reports "" when unset. */
  readonly orderingKey?: string;
  ack(): void;
  nack(): void;
}

/**
 * What a handler receives alongside its payload. Everything a consumer should need about the
 * delivery, so `raw` stays unnecessary.
 */
export interface CloudEventContext {
  readonly id: string;
  readonly source: string;
  readonly type: string;
  readonly subject?: string;
  /** Occurrence time — `ce-time` when the producer sent one, otherwise the Pub/Sub publish time. */
  readonly time: Date;
  readonly ingestionTime?: Date;
  readonly dataschema?: string;
  readonly datacontenttype: string;
  readonly traceparent?: string;
  readonly extensions: Readonly<Record<string, string>>;

  /** Delivery attempt, 1-based and normalised — Pub/Sub's own 0 becomes 1. */
  readonly deliveryAttempt: number;
  readonly orderingKey?: string;
  readonly publishTime: Date;
  readonly messageId: string;
  readonly subscription: string;

  /** Escape hatch. Using it couples the handler to Pub/Sub — prefer extending this context. */
  readonly raw: IncomingMessage;
}
