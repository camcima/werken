import type { CloudEventEnvelope } from "@werken/cloudevents";
import type { CloudEventContext, IncomingMessage } from "./types.js";

export function buildContext(
  envelope: CloudEventEnvelope,
  message: IncomingMessage,
  subscription: string,
): CloudEventContext {
  return {
    id: envelope.id,
    source: envelope.source,
    type: envelope.type,
    subject: envelope.subject,
    // ce-time is the occurrence time, and falls back to the publish time when the producer omits it.
    time: envelope.time ?? message.publishTime,
    ingestionTime: envelope.ingestiontime,
    dataschema: envelope.dataschema,
    datacontenttype: envelope.datacontenttype,
    traceparent: envelope.traceparent,
    extensions: envelope.extensions,

    deliveryAttempt: message.deliveryAttempt ? message.deliveryAttempt : 1,
    orderingKey: message.orderingKey || undefined,
    publishTime: message.publishTime,
    messageId: message.id,
    subscription,

    raw: message,
  };
}
