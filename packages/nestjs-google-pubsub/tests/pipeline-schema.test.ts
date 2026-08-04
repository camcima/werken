import avro from "avsc";
import { describe, expect, test, vi } from "vitest";
import { AvroCodec, MessagePipeline } from "@werken/nestjs-google-pubsub/internal";
import type { CloudEventContext, DeadLetterRequest, IncomingMessage } from "@werken/nestjs-google-pubsub";

const TYPE = "com.example.thing.happened.v1";
const NAME = "projects/p/schemas/thing";
const REV = "aaaa1111";

const SCHEMA = {
  type: "record",
  name: "Thing",
  fields: [
    { name: "id", type: "string" },
    { name: "station", type: ["null", "string"], default: null },
  ],
};
const TYPE_ = avro.Type.forSchema(SCHEMA as avro.Schema);

function message(body: Buffer, schemaAttrs = true): IncomingMessage {
  return {
    id: "pubsub-message-1",
    attributes: {
      "ce-specversion": "1.0",
      "ce-id": "01931b7c-3f2a-7000-8000-000000000001",
      "ce-source": "https://example.test/service",
      "ce-type": TYPE,
      ...(schemaAttrs
        ? {
            googclient_schemaname: NAME,
            googclient_schemarevisionid: REV,
            googclient_schemaencoding: "BINARY",
          }
        : {}),
    },
    data: body,
    publishTime: new Date("2026-08-02T15:00:00.000Z"),
    deliveryAttempt: 0,
    orderingKey: "",
    ack: () => {},
    nack: () => {},
  };
}

function codec() {
  return new AvroCodec({
    fetchWriterSchema: async () => JSON.stringify(SCHEMA),
    readerTypeFor: () => TYPE_,
  });
}

function pipelineWith(
  handlers: Record<string, (data: unknown, ctx: CloudEventContext) => unknown>,
  extra: Partial<ConstructorParameters<typeof MessagePipeline>[0]> = {},
) {
  return new MessagePipeline({
    subscription: "projects/p/subscriptions/s",
    resolveRoute: (type) => (handlers[type] === undefined ? null : { handler: handlers[type], pattern: type }),
    ...extra,
  });
}

describe("schema-aware decoding", () => {
  test("hands the handler the decoded record, not raw bytes", async () => {
    let seen: unknown;
    const outcome = await pipelineWith({ [TYPE]: (data) => void (seen = data) }, { codec: codec() }).handle(
      message(TYPE_.toBuffer({ id: "e1", station: "SCL" })),
    );

    expect(outcome).toBe("ack");
    expect(seen).toEqual({ id: "e1", station: "SCL" });
  });

  test("still parses plain JSON when the topic has no schema attached", async () => {
    let seen: unknown;
    const outcome = await pipelineWith({ [TYPE]: (data) => void (seen = data) }, { codec: codec() }).handle(
      message(Buffer.from(JSON.stringify({ plain: true })), false),
    );

    expect(outcome).toBe("ack");
    expect(seen).toEqual({ plain: true });
  });

  test("routes a decode failure through onDecodeFailure with the decode stage", async () => {
    const published: DeadLetterRequest[] = [];
    const outcome = await pipelineWith(
      { [TYPE]: () => {} },
      {
        codec: codec(),
        deadLetterPublisher: { publish: vi.fn(async (r: DeadLetterRequest) => void published.push(r)) },
      },
    ).handle(message(Buffer.from("not avro at all")));

    expect(outcome).toBe("dead-letter");
    expect(published[0].stage).toBe("decode");
  });

  test("nacks a decode failure when the writer schema cannot be fetched in strict mode", async () => {
    const failing = new AvroCodec({
      fetchWriterSchema: async () => {
        throw new Error("schema service down");
      },
      readerTypeFor: () => TYPE_,
    });

    // Fail-closed: a transient Schema Service outage must not silently dead-letter good messages,
    // so this is nacked for redelivery rather than treated as a permanent decode failure.
    const outcome = await pipelineWith(
      { [TYPE]: () => {} },
      { codec: failing, validation: { onDecodeFailure: "nack" } },
    ).handle(message(TYPE_.toBuffer({ id: "e1", station: null })));

    expect(outcome).toBe("nack");
  });

  test("does not invoke the handler when decoding fails", async () => {
    const handler = vi.fn();
    await pipelineWith({ [TYPE]: handler }, { codec: codec(), validation: { onDecodeFailure: "ack" } }).handle(
      message(Buffer.from("not avro at all")),
    );

    expect(handler).not.toHaveBeenCalled();
  });
});
