/**
 * Resource scoping for shared development projects.
 *
 * Pub/Sub delivers each message to exactly one subscriber of a subscription. When several
 * developers point at the same dev project and the same subscription, they silently consume each
 * other's messages — it presents as flaky delivery and costs hours to diagnose. Prefixing gives
 * each developer their own subscription and topic names against the same project.
 *
 * This is a development affordance, not a production feature. Nothing here provisions resources.
 */

export class ResourcePrefixError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ResourcePrefixError";
  }
}

/**
 * Pub/Sub resource id rules, verified empirically against the emulator (which enforces the
 * documented constraints): 3-255 characters, must start with a letter, and beyond the first
 * character only letters, digits, `-`, `.`, `_`, `~`, `+` and `%` are accepted.
 */
const VALID_RESOURCE_ID = /^[A-Za-z][A-Za-z0-9\-._~+%]{2,254}$/;

/** Reserved by Google, and case-sensitive — `GOOG` is accepted, `goog` is not. */
const RESERVED_PREFIX = "goog";

const MIN_LENGTH = 3;
const MAX_LENGTH = 255;

/**
 * Applies a scoping prefix to a resource name, validating the result.
 *
 * An absent or empty prefix is a no-op, which must remain the production path. Fully-qualified
 * paths keep their project segment: only the resource id is prefixed.
 */
export function applyResourcePrefix(name: string, prefix: string | undefined): string {
  // A no-op must be a true no-op: the name is the caller's, already accepted by Pub/Sub, and not
  // ours to police. We validate only when we have rewritten it and are therefore responsible for
  // the result.
  if (prefix === undefined || prefix === "") return name;

  const separatorIndex = name.lastIndexOf("/");
  const path = separatorIndex === -1 ? "" : name.slice(0, separatorIndex + 1);
  const id = separatorIndex === -1 ? name : name.slice(separatorIndex + 1);

  const normalised = prefix.endsWith("-") ? prefix : `${prefix}-`;
  const scopedId = `${normalised}${id}`;

  assertValidResourceId(scopedId, `${path}${scopedId}`);
  return `${path}${scopedId}`;
}

/**
 * Refuses to scope resources in production.
 *
 * A scoped consumer in production subscribes to a name nobody publishes to: it starts, reports
 * healthy, and receives nothing forever. That is a far worse failure than refusing to boot, so it
 * takes an explicit opt-out to allow.
 */
export function assertResourcePrefixSafe(
  prefix: string | undefined,
  allowUnsafe: boolean,
  nodeEnv: string | undefined,
): void {
  if (prefix === undefined || prefix === "") return;
  if (allowUnsafe) return;
  if (nodeEnv !== "production") return;

  throw new ResourcePrefixError(
    `werken: resourcePrefix ${JSON.stringify(prefix)} is set while NODE_ENV=production. ` +
      "Scoped resources exist only in shared development projects, so a production consumer would " +
      "subscribe to a name nothing publishes to and silently receive no messages. " +
      "Set allowUnsafeResourcePrefix: true if this is genuinely intended.",
  );
}

function assertValidResourceId(id: string, resolvedName: string): void {
  if (id.length < MIN_LENGTH || id.length > MAX_LENGTH) {
    throw new ResourcePrefixError(
      `werken: resolved resource name ${JSON.stringify(resolvedName)} is ${id.length} characters; ` +
        `Pub/Sub requires between ${MIN_LENGTH} and ${MAX_LENGTH}.`,
    );
  }
  if (id.startsWith(RESERVED_PREFIX)) {
    throw new ResourcePrefixError(
      `werken: resolved resource name ${JSON.stringify(resolvedName)} starts with "${RESERVED_PREFIX}", ` +
        "which Pub/Sub reserves.",
    );
  }
  if (!VALID_RESOURCE_ID.test(id)) {
    throw new ResourcePrefixError(
      `werken: resolved resource name ${JSON.stringify(resolvedName)} is not a valid Pub/Sub resource id. ` +
        "It must start with a letter and contain only letters, digits, or - . _ ~ + %",
    );
  }
}
