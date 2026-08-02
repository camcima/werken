/** Pub/Sub message attributes, as delivered. */
export type PubSubAttributes = Readonly<Record<string, string>>;

/**
 * A CloudEvents 1.0 envelope, bound from Pub/Sub message attributes (binary content mode).
 * Carries no payload — the domain payload is the message body.
 */
export interface CloudEventEnvelope {
  readonly specversion: string;
  readonly id: string;
  readonly source: string;
  readonly type: string;
  readonly subject?: string;
  /**
   * Occurrence time. Undefined when the producer omitted `ce-time`; the transport substitutes the
   * Pub/Sub publish time, which this package deliberately does not know about.
   */
  readonly time?: Date;
  readonly datacontenttype: string;
  readonly dataschema?: string;
  readonly traceparent?: string;
  readonly tracestate?: string;
  /** Extension attribute: when the platform learned about the event, as opposed to when it happened. */
  readonly ingestiontime?: Date;
  /** Unknown `ce-*` attributes, prefix stripped. Never dropped. */
  readonly extensions: Readonly<Record<string, string>>;
}
