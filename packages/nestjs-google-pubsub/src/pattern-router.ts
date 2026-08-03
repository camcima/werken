import type { EventHandler, ResolvedRoute } from "./pipeline.js";

export class InvalidPatternError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InvalidPatternError";
  }
}

export class AmbiguousPatternError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AmbiguousPatternError";
  }
}

const WILDCARD = "*";
const SEPARATOR = ".";

export const DEFAULT_MAX_CACHED_TYPES = 1024;

export interface PatternRouterOptions {
  /**
   * Upper bound on cached resolutions. `ce-type` is producer-controlled, so the cache must not
   * grow once per distinct type seen. Default 1024.
   */
  maxCachedTypes?: number;
}

interface WildcardRoute {
  /** Literal segments before the wildcard. Empty for the catch-all. */
  prefix: string[];
  handler: EventHandler;
  pattern: string;
}

/**
 * Routes a `ce-type` to exactly one handler.
 *
 * Supports three shapes: an exact type, a suffix wildcard (`com.example.*`, matching one or more
 * trailing segments), and the catch-all (`*`). Precedence is exact first, then the wildcard with
 * the longest literal prefix, then the catch-all — independent of registration order.
 */
export class PatternRouter {
  private readonly exact = new Map<string, EventHandler>();
  private readonly wildcards: WildcardRoute[] = [];
  /** Resolved lookups, so patterns are scanned once per type rather than once per message. */
  private readonly resolved = new Map<string, ResolvedRoute | null>();
  private readonly maxCachedTypes: number;
  private scans = 0;

  constructor(entries: Iterable<[string, EventHandler]>, options: PatternRouterOptions = {}) {
    this.maxCachedTypes = options.maxCachedTypes ?? DEFAULT_MAX_CACHED_TYPES;

    const seen = new Set<string>();
    for (const [pattern, handler] of entries) {
      assertNotChained(pattern, handler);
      assertSupported(pattern);
      // Nest signals a duplicate by chaining handlers, which assertNotChained catches. A caller
      // building this router directly gets no such marker, and without this an exact duplicate
      // would overwrite the first entry while a duplicate wildcard would keep it — either way a
      // handler registered in good faith never runs.
      if (seen.has(pattern)) {
        throw new AmbiguousPatternError(
          `werken: pattern ${JSON.stringify(pattern)} is registered more than once. ` +
            "Exactly one handler runs per message, so the others would silently never execute.",
        );
      }
      seen.add(pattern);

      if (pattern === WILDCARD) {
        this.wildcards.push({ prefix: [], handler, pattern });
      } else if (pattern.endsWith(`${SEPARATOR}${WILDCARD}`)) {
        this.wildcards.push({
          prefix: pattern.slice(0, -2).split(SEPARATOR),
          handler,
          pattern,
        });
      } else {
        this.exact.set(pattern, handler);
      }
    }

    // Longest literal prefix first, so the first match found is the most specific one.
    this.wildcards.sort((a, b) => b.prefix.length - a.prefix.length);
  }

  get stats(): { scans: number; cached: number } {
    return { scans: this.scans, cached: this.resolved.size };
  }

  resolve(type: string): ResolvedRoute | null {
    // With no wildcard registered there is nothing to scan: hits and misses alike are already a
    // single Map lookup, so a cache would only add a second one and a Map that grows forever.
    if (this.wildcards.length === 0) return exactRoute(this.exact.get(type), type);

    const cached = this.resolved.get(type);
    if (cached !== undefined) {
      // Re-insert to mark most-recently-used; Map preserves insertion order.
      this.resolved.delete(type);
      this.resolved.set(type, cached);
      return cached;
    }

    this.scans++;
    const route = this.scan(type);
    // Misses are cached too: a subscription legitimately carries types this consumer ignores, and
    // rescanning for each of them is pure waste.
    this.resolved.set(type, route);
    while (this.resolved.size > this.maxCachedTypes) {
      const oldest = this.resolved.keys().next();
      if (oldest.done === true) break;
      this.resolved.delete(oldest.value);
    }
    return route;
  }

  private scan(type: string): ResolvedRoute | null {
    const exact = exactRoute(this.exact.get(type), type);
    if (exact !== null) return exact;

    const segments = type.split(SEPARATOR);
    for (const route of this.wildcards) {
      // "one or more trailing segments": the prefix alone is not a match.
      if (segments.length <= route.prefix.length) continue;
      if (route.prefix.every((segment, index) => segments[index] === segment)) {
        return { handler: route.handler, pattern: route.pattern };
      }
    }
    return null;
  }
}

/** An exact hit's registered pattern is the type itself. */
function exactRoute(handler: EventHandler | undefined, type: string): ResolvedRoute | null {
  return handler === undefined ? null : { handler, pattern: type };
}

function assertSupported(pattern: string): void {
  if (pattern === WILDCARD) return;

  const segments = pattern.split(SEPARATOR);
  const wildcardCount = segments.filter((s) => s === WILDCARD).length;
  const embedded = segments.some((s) => s !== WILDCARD && s.includes(WILDCARD));

  if (embedded) {
    throw new InvalidPatternError(
      `werken: pattern ${JSON.stringify(pattern)} places "*" inside a segment. ` +
        'Only a whole trailing segment may be a wildcard, as in "com.example.*".',
    );
  }
  if (wildcardCount === 0) return;
  if (wildcardCount > 1 || segments[segments.length - 1] !== WILDCARD) {
    // Failing here is the point: a mid-pattern wildcard would simply never match, and a handler
    // that never runs is far harder to notice than a startup error.
    throw new InvalidPatternError(
      `werken: pattern ${JSON.stringify(pattern)} is not supported. ` +
        '"*" is only allowed as the final segment ("com.example.*") or on its own ("*").',
    );
  }
}

/**
 * Nest chains duplicate event handlers through `.next` rather than replacing them, so a second
 * `@EventPattern` for the same type registers successfully and then never runs. Exactly one handler
 * per message is the contract, so the ambiguity is refused at startup rather than at runtime.
 */
function assertNotChained(pattern: string, handler: EventHandler): void {
  if ((handler as { next?: unknown }).next === undefined) return;

  throw new AmbiguousPatternError(
    `werken: pattern ${JSON.stringify(pattern)} has more than one handler registered. ` +
      "Exactly one handler runs per message, so the others would silently never execute. " +
      "Remove the duplicate @EventPattern, or give each handler a distinct type.",
  );
}
