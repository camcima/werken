import avro from "avsc";
import { describe, expect, test, vi } from "vitest";
import { AvroCodec, SchemaDecodeError } from "@werken/nestjs-google-pubsub";

const WRITER_V1 = {
  type: "record",
  name: "Thing",
  fields: [
    { name: "id", type: "string" },
    { name: "station", type: ["null", "string"], default: null },
  ],
};

/** v2 adds a field the reader does not know about — the normal producer-ahead case. */
const WRITER_V2 = {
  type: "record",
  name: "Thing",
  fields: [
    { name: "id", type: "string" },
    { name: "station", type: ["null", "string"], default: null },
    { name: "addedByProducer", type: ["null", "string"], default: null },
  ],
};

const READER = avro.Type.forSchema(WRITER_V1 as avro.Schema);

const NAME = "projects/p/schemas/thing";
const REV1 = "aaaa1111";
const REV2 = "bbbb2222";

function codecWith(overrides: Partial<ConstructorParameters<typeof AvroCodec>[0]> = {}) {
  return new AvroCodec({
    fetchWriterSchema: async (key: string) => JSON.stringify(key.endsWith(REV2) ? WRITER_V2 : WRITER_V1),
    readerTypeFor: () => READER,
    ...overrides,
  });
}

const meta = (encoding: "JSON" | "BINARY", revision = REV1) => ({ name: NAME, revision, encoding });

describe("BINARY encoding", () => {
  test("decodes a message written with the same revision", async () => {
    const writer = avro.Type.forSchema(WRITER_V1 as avro.Schema);
    const body = writer.toBuffer({ id: "e1", station: "SCL" });

    const decoded = await codecWith().decode(body, meta("BINARY"));

    expect(decoded).toEqual({ id: "e1", station: "SCL" });
  });

  test("resolves a newer writer revision down to the reader schema", async () => {
    const writer = avro.Type.forSchema(WRITER_V2 as avro.Schema);
    const body = writer.toBuffer({ id: "e1", station: "SCL", addedByProducer: "ignored" });

    const decoded = await codecWith().decode(body, meta("BINARY", REV2));

    // The reader does not know addedByProducer, so resolution drops it rather than failing.
    expect(decoded).toEqual({ id: "e1", station: "SCL" });
  });
});

describe("JSON encoding", () => {
  // SPIKE-1: Pub/Sub's JSON encoding is standard Avro JSON, where a nullable union is
  // {"string":"SCL"} and NOT "SCL". Parsing it as plain JSON silently mangles every union field.
  test("decodes standard Avro JSON, not plain JSON", async () => {
    const body = Buffer.from(JSON.stringify({ id: "e1", station: { string: "SCL" } }));

    const decoded = await codecWith().decode(body, meta("JSON"));

    expect(decoded).toEqual({ id: "e1", station: "SCL" });
  });

  test("rejects plain JSON that is not valid Avro JSON", async () => {
    const body = Buffer.from(JSON.stringify({ id: "e1", station: "SCL" }));

    await expect(codecWith().decode(body, meta("JSON"))).rejects.toThrow(SchemaDecodeError);
  });

  test("resolves a newer writer revision", async () => {
    const body = Buffer.from(JSON.stringify({ id: "e1", station: { string: "SCL" }, addedByProducer: null }));

    const decoded = await codecWith().decode(body, meta("JSON", REV2));

    expect(decoded).toEqual({ id: "e1", station: "SCL" });
  });
});

describe("unschematised messages", () => {
  test("falls back to plain JSON when the topic has no schema attached", async () => {
    const body = Buffer.from(JSON.stringify({ anything: "goes" }));

    const decoded = await codecWith().decode(body, { name: undefined, revision: undefined, encoding: undefined });

    expect(decoded).toEqual({ anything: "goes" });
  });
});

describe("strict mode", () => {
  test("fails closed when the writer schema cannot be fetched", async () => {
    const codec = codecWith({
      fetchWriterSchema: async () => {
        throw new Error("schema service unavailable");
      },
    });
    const body = avro.Type.forSchema(WRITER_V1 as avro.Schema).toBuffer({ id: "e1", station: null });

    // Guessing here would decode against the reader schema alone and silently mis-read any field
    // the writer changed.
    await expect(codec.decode(body, meta("BINARY"))).rejects.toThrow(SchemaDecodeError);
  });

  test("throws when no reader type is registered for the schema", async () => {
    const codec = codecWith({ readerTypeFor: () => undefined });
    const body = avro.Type.forSchema(WRITER_V1 as avro.Schema).toBuffer({ id: "e1", station: null });

    await expect(codec.decode(body, meta("BINARY"))).rejects.toThrow(SchemaDecodeError);
  });
});

/**
 * Verified against the emulator: Pub/Sub sets googclient_schemaname, googclient_schemarevisionid
 * and googclient_schemaencoding together, for both JSON and BINARY topics, or sets none of them.
 * A partial set therefore did not come from Pub/Sub — and since a publisher cannot forge attributes
 * beginning with "goog", it means something is wrong rather than that the topic is unschematised.
 * Guessing which half to trust is how a message gets read against the wrong shape.
 */
describe("incoherent schema metadata", () => {
  const body = () => Buffer.from(JSON.stringify({ id: "e1" }));

  test("refuses a schema name with no revision", async () => {
    await expect(codecWith().decode(body(), { name: NAME, encoding: "JSON" })).rejects.toThrow(SchemaDecodeError);
  });

  test("refuses a revision with no schema name", async () => {
    await expect(codecWith().decode(body(), { revision: REV1, encoding: "JSON" })).rejects.toThrow(SchemaDecodeError);
  });

  test("refuses a schema with no encoding", async () => {
    await expect(codecWith().decode(body(), { name: NAME, revision: REV1 })).rejects.toThrow(SchemaDecodeError);
  });

  test("refuses an encoding on its own", async () => {
    await expect(codecWith().decode(body(), { encoding: "JSON" })).rejects.toThrow(SchemaDecodeError);
  });

  // Anything that is not JSON took the binary branch, so a typo or a future encoding would be
  // decoded as Avro binary rather than reported.
  test("refuses an encoding that is neither JSON nor BINARY", async () => {
    await expect(
      codecWith().decode(body(), { name: NAME, revision: REV1, encoding: "PROTOCOL_BUFFER" }),
    ).rejects.toThrow(SchemaDecodeError);
  });

  test("still refuses incoherent metadata when strict is off", async () => {
    const codec = codecWith({ strict: false, logger: { debug: vi.fn(), warn: vi.fn() } });

    await expect(codec.decode(body(), { name: NAME, encoding: "JSON" })).rejects.toThrow(SchemaDecodeError);
  });
});

describe("caching", () => {
  test("fetches a revision once across many messages", async () => {
    const fetchWriterSchema = vi.fn(async () => JSON.stringify(WRITER_V1));
    const codec = codecWith({ fetchWriterSchema });
    const body = avro.Type.forSchema(WRITER_V1 as avro.Schema).toBuffer({ id: "e1", station: null });

    await codec.decode(body, meta("BINARY"));
    await codec.decode(body, meta("BINARY"));
    await codec.decode(body, meta("BINARY"));

    expect(fetchWriterSchema).toHaveBeenCalledTimes(1);
  });

  test("fetches each revision separately", async () => {
    const fetchWriterSchema = vi.fn(async (key: string) => JSON.stringify(key.endsWith(REV2) ? WRITER_V2 : WRITER_V1));
    const codec = codecWith({ fetchWriterSchema });

    await codec.decode(
      avro.Type.forSchema(WRITER_V1 as avro.Schema).toBuffer({ id: "a", station: null }),
      meta("BINARY"),
    );
    await codec.decode(
      avro.Type.forSchema(WRITER_V2 as avro.Schema).toBuffer({ id: "b", station: null, addedByProducer: null }),
      meta("BINARY", REV2),
    );

    expect(fetchWriterSchema).toHaveBeenCalledTimes(2);
  });

  test("asks the Schema Service for the revision-qualified name", async () => {
    const fetchWriterSchema = vi.fn(async () => JSON.stringify(WRITER_V1));
    const codec = codecWith({ fetchWriterSchema });

    await codec.decode(
      avro.Type.forSchema(WRITER_V1 as avro.Schema).toBuffer({ id: "e1", station: null }),
      meta("BINARY"),
    );

    // SPIKE-0 confirmed pubsub.schema() accepts `name@revisionId`.
    expect(fetchWriterSchema).toHaveBeenCalledWith(`${NAME}@${REV1}`);
  });
});

describe("cache result reporting", () => {
  test("forwards cache hits and misses so the schema cache metric can be fed", async () => {
    const results: string[] = [];
    const codec = codecWith({ onCacheResult: (result: string) => results.push(result) });
    const body = avro.Type.forSchema(WRITER_V1 as avro.Schema).toBuffer({ id: "e1", station: null });

    await codec.decode(body, meta("BINARY"));
    await codec.decode(body, meta("BINARY"));

    expect(results).toEqual(["miss", "hit"]);
  });

  test("reports nothing for a message carrying no schema at all", async () => {
    const results: string[] = [];
    const codec = codecWith({ onCacheResult: (result: string) => results.push(result) });

    await codec.decode(Buffer.from(JSON.stringify({ plain: true })), {});

    expect(results).toEqual([]);
  });
});

/**
 * `strict: false` trades safety for availability: rather than refusing to decode when the writer
 * schema is unreachable, it falls back to reading the body as plain JSON. Worth testing precisely
 * because it is the opt-out from the guarantee the rest of this codec exists to provide.
 */
describe("non-strict fallback", () => {
  test("falls back to plain JSON when the writer schema cannot be fetched", async () => {
    const warn = vi.fn();
    const codec = codecWith({
      strict: false,
      logger: { debug: vi.fn(), warn },
      fetchWriterSchema: async () => {
        throw new Error("schema service unavailable");
      },
    });

    const decoded = await codec.decode(Buffer.from(JSON.stringify({ id: "e1" })), meta("JSON"));

    expect(decoded).toEqual({ id: "e1" });
    expect(warn).toHaveBeenCalledWith(expect.stringMatching(/falling back to plain JSON/));
  });

  // The fallback is not a universal safety net: a body written in Avro's binary encoding is not
  // JSON, so an unreachable schema is still fatal for a BINARY topic.
  test("still fails on a binary body, which plain JSON cannot represent", async () => {
    const codec = codecWith({
      strict: false,
      logger: { debug: vi.fn(), warn: vi.fn() },
      fetchWriterSchema: async () => {
        throw new Error("schema service unavailable");
      },
    });
    const body = avro.Type.forSchema(WRITER_V1 as avro.Schema).toBuffer({ id: "e1", station: null });

    await expect(codec.decode(body, meta("BINARY"))).rejects.toThrow(SchemaDecodeError);
  });

  // `strict` is documented as controlling what happens when the writer schema cannot be *fetched*.
  // Everything below is a different failure: the schema resolved fine and this consumer still
  // cannot read it correctly. Falling back would hand the handler Avro-JSON shapes, or silently
  // mis-read fields the writer changed — a correctness failure, not an availability trade.
  test("still fails on a missing reader type, which no fallback can substitute for", async () => {
    const codec = codecWith({
      strict: false,
      logger: { debug: vi.fn(), warn: vi.fn() },
      readerTypeFor: () => undefined,
    });

    await expect(codec.decode(Buffer.from(JSON.stringify({ id: "e1" })), meta("JSON"))).rejects.toThrow(
      SchemaDecodeError,
    );
  });

  test("still fails when the writer schema definition is not valid Avro", async () => {
    const codec = codecWith({
      strict: false,
      logger: { debug: vi.fn(), warn: vi.fn() },
      fetchWriterSchema: async () => JSON.stringify({ type: "record", name: "Broken", fields: "not-a-list" }),
    });

    await expect(codec.decode(Buffer.from(JSON.stringify({ id: "e1" })), meta("JSON"))).rejects.toThrow(
      SchemaDecodeError,
    );
  });

  test("still fails when the writer cannot be resolved into the reader", async () => {
    const codec = codecWith({
      strict: false,
      logger: { debug: vi.fn(), warn: vi.fn() },
      // A different record entirely: no resolution path exists from this writer to the reader.
      fetchWriterSchema: async () =>
        JSON.stringify({ type: "record", name: "Unrelated", fields: [{ name: "x", type: "int" }] }),
    });

    await expect(codec.decode(Buffer.from(JSON.stringify({ id: "e1" })), meta("JSON"))).rejects.toThrow(
      SchemaDecodeError,
    );
  });

  test("names the underlying cause, so an operator can tell outage from misconfiguration", async () => {
    const codec = codecWith({
      fetchWriterSchema: async () => {
        throw new Error("schema service unavailable");
      },
    });
    const body = avro.Type.forSchema(WRITER_V1 as avro.Schema).toBuffer({ id: "e1", station: null });

    await expect(codec.decode(body, meta("BINARY"))).rejects.toThrow(/schema service unavailable/);
  });
});

describe("unschematised bodies", () => {
  test("treats an empty body as an undefined payload rather than a parse failure", async () => {
    expect(await codecWith().decode(Buffer.alloc(0), {})).toBeUndefined();
  });

  test("reports a body that is not valid JSON", async () => {
    await expect(codecWith().decode(Buffer.from("{not json"), {})).rejects.toThrow(SchemaDecodeError);
  });
});
