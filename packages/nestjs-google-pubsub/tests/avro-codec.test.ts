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
