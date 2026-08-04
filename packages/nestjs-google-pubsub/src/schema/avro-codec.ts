import avro from "avsc";
import { SchemaRevisionCache } from "./cache.js";

/** Schema metadata Pub/Sub attaches to a message. See `SCHEMA_ATTRIBUTES` for the wire names. */
export interface SchemaMessageMeta {
  readonly name?: string;
  readonly revision?: string;
  readonly encoding?: string;
}

export class SchemaDecodeError extends Error {
  constructor(
    message: string,
    readonly cause?: unknown,
  ) {
    super(message);
    this.name = "SchemaDecodeError";
  }
}

export interface AvroCodecOptions {
  /** Fetches a writer schema definition by revision-qualified name (`name@revisionId`). */
  fetchWriterSchema: (revisionQualifiedName: string) => Promise<string>;
  /**
   * The compiled reader type for a schema, from the types the consumer imports — never from the
   * registry. Returning undefined means this consumer cannot read the schema at all.
   */
  readerTypeFor: (schemaName: string) => avro.Type | undefined;
  /**
   * Fail closed when the writer schema cannot be *fetched*. Default true. `false` falls back to
   * plain JSON for that failure alone — every other resolution failure stays fatal. See
   * `SchemaRegistryOptions.strict`.
   */
  strict?: boolean;
  maxCachedRevisions?: number;
  cacheTtlMs?: number;
  now?: () => number;
  logger?: Pick<Console, "debug" | "warn">;
  /** Called once per schema lookup, so the cache hit rate can be reported as a metric. */
  onCacheResult?: (result: "hit" | "miss") => void;
}

/** The writer and reader types plus the resolver that reads writer bytes into the reader's shape. */
interface ResolvedSchema {
  writerType: avro.Type;
  readerType: avro.Type;
  resolver: avro.Resolver;
}

export class AvroCodec {
  private readonly cache: SchemaRevisionCache<ResolvedSchema>;
  private readonly strict: boolean;

  constructor(private readonly options: AvroCodecOptions) {
    this.strict = options.strict ?? true;
    this.cache = new SchemaRevisionCache<ResolvedSchema>({
      fetch: (revisionQualifiedName) => this.resolve(revisionQualifiedName),
      maxEntries: options.maxCachedRevisions,
      ttlMs: options.cacheTtlMs,
      now: options.now,
      onResult: options.onCacheResult,
    });
  }

  async decode(body: Buffer, meta: SchemaMessageMeta): Promise<unknown> {
    // No schema attached to the topic is a distinct case from an unknown revision: the first means
    // an unschematised topic, the second a producer running ahead of this consumer.
    const schema = classifySchemaMeta(meta);
    if (schema === undefined) {
      return parsePlainJson(body);
    }

    const key = `${schema.name}@${schema.revision}`;
    let resolved: ResolvedSchema;
    try {
      resolved = await this.cache.get(key);
    } catch (error) {
      // `strict` governs exactly one failure: the writer schema could not be fetched, which is an
      // availability problem. A missing reader type, a definition that is not valid Avro, and a
      // writer the reader cannot resolve are all cases where the schema is known and this consumer
      // still cannot read the message correctly — falling back there would hand the handler raw
      // Avro-JSON shapes, or silently mis-read any field the writer changed. Those stay fatal
      // whatever `strict` says, because they are correctness failures, not availability ones.
      if (this.strict || !(error instanceof WriterSchemaUnavailableError)) {
        // Name the cause: "could not resolve" alone does not distinguish a client without schema
        // support from a schema with no definition from a Schema Service outage, and those need
        // different responses.
        throw error instanceof SchemaDecodeError
          ? error
          : new SchemaDecodeError(`could not resolve writer schema ${key}: ${asMessage(error)}`, error);
      }
      this.options.logger?.warn(`werken: falling back to plain JSON for ${key}: ${asMessage(error)}`);
      return parsePlainJson(body);
    }

    try {
      if (schema.encoding === "JSON") {
        // Pub/Sub's JSON encoding is standard Avro JSON, where a nullable union is
        // {"string":"v"} and not "v". fromString applies those rules; JSON.parse would not.
        // Round-tripping through the writer's binary form is how the resolver gets applied.
        const writerValue = resolved.writerType.fromString(body.toString("utf8"));
        return readThroughResolver(resolved, resolved.writerType.toBuffer(writerValue));
      }
      return readThroughResolver(resolved, body);
    } catch (error) {
      throw new SchemaDecodeError(`could not decode message against ${key}`, error);
    }
  }

  private async resolve(revisionQualifiedName: string): Promise<ResolvedSchema> {
    const schemaName = revisionQualifiedName.split("@")[0];
    const readerType = this.options.readerTypeFor(schemaName);
    if (readerType === undefined) {
      throw new SchemaDecodeError(`no reader type registered for schema ${schemaName}`);
    }

    // An unknown revision is the normal steady state while a producer rolls out ahead of its
    // consumers, so this is debug rather than warn.
    this.options.logger?.debug(`werken: resolving writer schema ${revisionQualifiedName}`);

    let definition: string;
    try {
      definition = await this.options.fetchWriterSchema(revisionQualifiedName);
    } catch (error) {
      // Marked, so decode() can tell this apart from everything else that can go wrong here. It is
      // the only failure `strict: false` is allowed to fall back on.
      throw new WriterSchemaUnavailableError(revisionQualifiedName, error);
    }

    const writerType = avro.Type.forSchema(JSON.parse(definition) as avro.Schema);
    return { writerType, readerType, resolver: readerType.createResolver(writerType) };
  }
}

/**
 * The writer schema could not be fetched: a Schema Service outage, a client without schema support,
 * or a revision that has not propagated yet. An availability problem rather than evidence that the
 * message cannot be read, which is why it is the one case `strict: false` may fall back on.
 */
class WriterSchemaUnavailableError extends SchemaDecodeError {
  constructor(revisionQualifiedName: string, cause: unknown) {
    super(`could not fetch writer schema ${revisionQualifiedName}: ${asMessage(cause)}`, cause);
    this.name = "WriterSchemaUnavailableError";
  }
}

/**
 * Sorts the `googclient_*` attributes into "no schema attached" or a complete, usable set.
 *
 * Verified against the emulator: Pub/Sub sets the schema name, revision and encoding together for
 * both JSON and BINARY topics, or sets none of them. A publisher cannot forge them either, since
 * Pub/Sub reserves attribute keys beginning with `goog`. So a partial set does not mean an
 * unschematised topic — it means metadata that cannot be trusted, and picking a decoder on a guess
 * is how a message gets read against the wrong shape.
 */
function classifySchemaMeta(
  meta: SchemaMessageMeta,
): { name: string; revision: string; encoding: Encoding } | undefined {
  const { name, revision, encoding } = meta;
  const set = [name, revision, encoding].filter((value) => value !== undefined && value !== "");
  if (set.length === 0) return undefined;

  if (isBlank(name) || isBlank(revision) || isBlank(encoding)) {
    throw new SchemaDecodeError(
      `incomplete schema metadata (name=${JSON.stringify(name)}, revision=${JSON.stringify(revision)}, ` +
        `encoding=${JSON.stringify(encoding)}): Pub/Sub sets all three together, so a partial set ` +
        "cannot be decoded safely",
    );
  }

  const normalised = encoding.toUpperCase();
  if (normalised !== "JSON" && normalised !== "BINARY") {
    // Previously anything that was not JSON took the binary branch, so a typo or a future encoding
    // would be decoded as Avro binary rather than reported.
    throw new SchemaDecodeError(`unsupported schema encoding ${JSON.stringify(encoding)}: expected JSON or BINARY`);
  }
  return { name, revision, encoding: normalised };
}

type Encoding = "JSON" | "BINARY";

function isBlank(value: string | undefined): value is undefined {
  return value === undefined || value === "";
}

function readThroughResolver(resolved: ResolvedSchema, buffer: Buffer): unknown {
  // Decoding goes through the READER type with the resolver — calling this on the writer type
  // throws "invalid resolver", because the resolver belongs to the reader.
  return resolved.readerType.fromBuffer(buffer, resolved.resolver, true);
}

function parsePlainJson(body: Buffer): unknown {
  if (body.length === 0) return undefined;
  try {
    return JSON.parse(body.toString("utf8"));
  } catch (error) {
    throw new SchemaDecodeError("message body is not valid JSON", error);
  }
}

function asMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
