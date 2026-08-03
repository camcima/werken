/**
 * Internals, for this repository's own tests.
 *
 * **Not a public entry point and not covered by semver.** It is deliberately absent from the
 * package's `exports` map, so Node refuses to resolve it from an installed copy — importing
 * `@werken/nestjs-google-pubsub/internal` in a consuming project fails. Inside this repo,
 * `vitest.shared.ts` aliases it to source so tests can drive the machinery directly while the
 * published surface in `index.ts` stays small enough to document and support.
 *
 * If something here turns out to be genuinely useful to a consumer, promote it to `index.ts`
 * deliberately, with documentation — rather than leaving the whole engine exposed by default.
 */

export { MessagePipeline, DEFAULT_IDEMPOTENCY_TTL_MS, INVALID_ROUTE, UNMATCHED_ROUTE } from "./pipeline.js";
export type { MessagePipelineOptions, ResolvedRoute } from "./pipeline.js";

export { PatternRouter, DEFAULT_MAX_CACHED_TYPES } from "./pattern-router.js";
export type { PatternRouterOptions } from "./pattern-router.js";

export { SchemaRevisionCache, DEFAULT_MAX_CACHED_REVISIONS, DEFAULT_SCHEMA_CACHE_TTL_MS } from "./schema/cache.js";
export type { SchemaCacheStats, SchemaRevisionCacheOptions } from "./schema/cache.js";

export { AvroCodec } from "./schema/avro-codec.js";
export type { AvroCodecOptions, SchemaMessageMeta } from "./schema/avro-codec.js";
export { SCHEMA_ATTRIBUTES, schemaMetaFromAttributes } from "./schema/attributes.js";

export { createTelemetry } from "./telemetry.js";
export type {
  MessageSpanInput,
  SchemaCacheResult,
  Telemetry,
  TelemetryEnvelope,
  TelemetryOptions,
} from "./telemetry.js";

export { buildContext } from "./context.js";
export { applyResourcePrefix, assertResourcePrefixSafe } from "./resource-name.js";
export { deadLetterAttributes } from "./dead-letter.js";
export { eventLogFields, withEventFields } from "./logging.js";
export type { EventLogFields } from "./logging.js";

export {
  DEFAULT_ACK_DEADLINE_MS,
  DEFAULT_FLOW_CONTROL,
  DEFAULT_MAX_EXTENSION_MS,
  DEFAULT_MAX_STREAMS,
  DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MS,
  assertValidOptions,
  toSubscriberOptions,
} from "./options.js";
export type { SubscriberOptionsLike } from "./options.js";
