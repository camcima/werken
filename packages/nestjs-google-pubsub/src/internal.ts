/**
 * Internals, for this repository's own tests.
 *
 * **Not a public entry point and not covered by semver.** It is deliberately absent from the
 * package's `exports` map, so Node refuses to resolve it from an installed copy — importing
 * `@werken/nestjs-google-pubsub/internal` in a consuming project fails. Inside this repo,
 * `vitest.shared.ts` aliases it to source so tests can drive the machinery directly while the
 * published surface in `index.ts` stays small enough to document and support.
 *
 * Kept to exactly what the suites import, and not registered as a Knip entry point, so a re-export
 * that loses its last test is reported as dead rather than sitting here looking load-bearing. If
 * something here turns out to be genuinely useful to a consumer, promote it to `index.ts`
 * deliberately, with documentation — rather than leaving the whole engine exposed by default.
 */

export { MessagePipeline } from "./pipeline.js";
export { PatternRouter } from "./pattern-router.js";
export { SchemaRevisionCache } from "./schema/cache.js";
export { AvroCodec } from "./schema/avro-codec.js";
export { createTelemetry } from "./telemetry.js";
export type { Telemetry } from "./telemetry.js";
export { buildContext } from "./context.js";
export { applyResourcePrefix, assertResourcePrefixSafe } from "./resource-name.js";
export { eventLogFields, withEventFields } from "./logging.js";
export { toSubscriberOptions } from "./options.js";
