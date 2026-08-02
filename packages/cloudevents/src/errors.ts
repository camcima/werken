/**
 * Machine-readable reason an envelope was rejected. Kept precise so callers can route on the
 * cause rather than parsing messages — §4.4 outcome policies branch on this.
 */
export type EnvelopeErrorCode = "missing-attribute" | "unsupported-specversion" | "invalid-attribute";

/** Raised when Pub/Sub attributes do not form a valid CloudEvents 1.0 envelope. */
export class EnvelopeValidationError extends Error {
  constructor(
    readonly code: EnvelopeErrorCode,
    readonly attribute: string,
    message: string,
  ) {
    super(message);
    this.name = "EnvelopeValidationError";
  }
}
