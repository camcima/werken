import avro from "avsc";
import { SchemaRevisionCache } from "./cache.js";
import type { SchemaCacheStats } from "./cache.js";

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
  /** Fail closed when the writer schema cannot be fetched. Default true. */
  strict?: boolean;
  maxCachedRevisions?: number;
  cacheTtlMs?: number;
  now?: () => number;
  logger?: Pick<Console, "debug" | "warn">;
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
    });
  }

  get cacheStats(): SchemaCacheStats {
    return this.cache.stats;
  }

  async decode(body: Buffer, meta: SchemaMessageMeta): Promise<unknown> {
    // No schema attached to the topic is a distinct case from an unknown revision: the first means
    // an unschematised topic, the second a producer running ahead of this consumer.
    if (meta.name === undefined || meta.revision === undefined) {
      return parsePlainJson(body);
    }

    const key = `${meta.name}@${meta.revision}`;
    let resolved: ResolvedSchema;
    try {
      resolved = await this.cache.get(key);
    } catch (error) {
      if (this.strict) {
        // Decoding against the reader schema alone would silently mis-read any field the writer
        // changed. Fail loudly instead.
        throw new SchemaDecodeError(`could not resolve writer schema ${key}`, error);
      }
      this.options.logger?.warn(`werken: falling back to plain JSON for ${key}: ${asMessage(error)}`);
      return parsePlainJson(body);
    }

    try {
      if (isJsonEncoding(meta.encoding)) {
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

    const definition = await this.options.fetchWriterSchema(revisionQualifiedName);
    const writerType = avro.Type.forSchema(JSON.parse(definition) as avro.Schema);
    return { writerType, readerType, resolver: readerType.createResolver(writerType) };
  }
}

function readThroughResolver(resolved: ResolvedSchema, buffer: Buffer): unknown {
  // Decoding goes through the READER type with the resolver — calling this on the writer type
  // throws "invalid resolver", because the resolver belongs to the reader.
  return resolved.readerType.fromBuffer(buffer, resolved.resolver, true);
}

function isJsonEncoding(encoding: string | undefined): boolean {
  return encoding?.toUpperCase() === "JSON";
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
