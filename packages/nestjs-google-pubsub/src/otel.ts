import { optionalRequire } from "./optional-require.cjs";

type OtelApi = typeof import("@opentelemetry/api");

/**
 * Resolved once per process, then remembered — including the answer "not installed".
 *
 * Node caches a successful `require`, but not a failed one. So on the path this optionality exists
 * for — the consumer who did not install `@opentelemetry/api` — every lookup walks the
 * node_modules chain again and builds a MODULE_NOT_FOUND error. Repeated per published message,
 * that is the hot path paying for a feature the caller declined.
 *
 * Caching the namespace object is safe: the tracer, meter and propagator are all read off it at
 * call time, so a provider registered later is still picked up.
 */
let cached: OtelApi | null | undefined;

export function loadOtelApi(): OtelApi | undefined {
  cached ??= (optionalRequire("@opentelemetry/api") as OtelApi | undefined) ?? null;
  return cached ?? undefined;
}
