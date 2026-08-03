import type { IncomingMessage } from "./types.js";

/**
 * Fields §5.5 requires on every pipeline log line.
 *
 * Emitted as JSON appended to the message rather than through a logger interface of our own: Cloud
 * Logging and most aggregators parse embedded JSON into queryable fields, and it keeps the library
 * from dictating which logger a consumer uses.
 */
export interface EventLogFields {
  ceId?: string;
  ceType?: string;
  ceSource?: string;
  ceSubject?: string;
  deliveryAttempt: number;
  messageId: string;
}

export function eventLogFields(message: IncomingMessage): EventLogFields {
  return {
    ceId: message.attributes["ce-id"],
    ceType: message.attributes["ce-type"],
    ceSource: message.attributes["ce-source"],
    ceSubject: message.attributes["ce-subject"],
    // Normalised the same way as CloudEventContext: Pub/Sub reports 0 without a dead-letter policy.
    deliveryAttempt: message.deliveryAttempt ? message.deliveryAttempt : 1,
    messageId: message.id,
  };
}

/** Appends the event fields to a log message as JSON, dropping any that are absent. */
export function withEventFields(text: string, message: IncomingMessage): string {
  const fields = eventLogFields(message);
  const present = Object.fromEntries(Object.entries(fields).filter(([, v]) => v !== undefined));
  return `${text} ${JSON.stringify(present)}`;
}
