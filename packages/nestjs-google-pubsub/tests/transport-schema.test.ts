import { EventEmitter } from "node:events";
import { metrics } from "@opentelemetry/api";
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from "@opentelemetry/sdk-metrics";
import avro from "avsc";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { WerkenPubSubTransport } from "@werken/nestjs-google-pubsub";
import type { IncomingMessage } from "@werken/nestjs-google-pubsub";

const TYPE = "com.example.thing.happened.v1";
const SCHEMA_NAME = "projects/p/schemas/thing";
const REVISION = "aaaa1111";
const WRITER = {
  type: "record",
  name: "Thing",
  fields: [{ name: "id", type: "string" }],
};
const READER = avro.Type.forSchema(WRITER as avro.Schema);

const listenReady = (transport: WerkenPubSubTransport) =>
  new Promise<void>((resolve, reject) => transport.listen((error?: unknown) => (error ? reject(error) : resolve())));

const settle = () => new Promise((resolve) => setImmediate(resolve));

class FakeSubscription extends EventEmitter {
  close = vi.fn(async () => {});
}

/** A client whose `schema()` behaves however the test needs it to. */
function fakeClient(schema?: unknown) {
  const subscription = new FakeSubscription();
  return {
    subscription,
    client: {
      subscription: vi.fn(() => subscription),
      close: vi.fn(async () => {}),
      ...(schema === undefined ? {} : { schema }),
    },
  };
}

function schemaMessage(id: string): IncomingMessage {
  return {
    id,
    attributes: {
      "ce-specversion": "1.0",
      "ce-id": id,
      "ce-source": "https://example.test/service",
      "ce-type": TYPE,
      googclient_schemaname: SCHEMA_NAME,
      googclient_schemarevisionid: REVISION,
      googclient_schemaencoding: "BINARY",
    },
    data: READER.toBuffer({ id }),
    publishTime: new Date("2026-08-03T10:00:00.000Z"),
    deliveryAttempt: 1,
    orderingKey: "",
    ack: vi.fn(),
    nack: vi.fn(),
  };
}

function transportWith(client: unknown, overrides: Record<string, unknown> = {}) {
  return new WerkenPubSubTransport({
    projectId: "p",
    subscription: "s",
    schemaRegistry: { readerTypeFor: () => READER },
    createClient: () => client as never,
    ...overrides,
  });
}

/**
 * Asserted against the real OpenTelemetry SDK through a global MeterProvider, because the bug this
 * guards against is precisely a missing call between the codec and the telemetry facade — a fake
 * telemetry object would assume the very wiring under test.
 */
describe("schema cache metric", () => {
  let exporter: InMemoryMetricExporter;
  let reader: PeriodicExportingMetricReader;
  let meterProvider: MeterProvider;

  beforeEach(() => {
    exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
    reader = new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 60_000 });
    meterProvider = new MeterProvider({ readers: [reader] });
    metrics.setGlobalMeterProvider(meterProvider);
  });

  afterEach(async () => {
    metrics.disable();
    await meterProvider.shutdown();
  });

  async function schemaCachePoints() {
    await reader.forceFlush();
    const all = exporter.getMetrics().flatMap((m) => m.scopeMetrics.flatMap((s) => s.metrics));
    return all.find((m) => m.descriptor.name === "werken.schema.cache")?.dataPoints ?? [];
  }

  test("counts a miss then a hit as messages are decoded", async () => {
    const { subscription, client } = fakeClient(
      vi.fn(() => ({ get: async () => ({ definition: JSON.stringify(WRITER) }) })),
    );
    const transport = transportWith(client);
    transport.addHandler(TYPE, (() => {}) as never, true);
    await listenReady(transport);

    subscription.emit("message", schemaMessage("e1"));
    await settle();
    subscription.emit("message", schemaMessage("e2"));
    await settle();

    const byResult = Object.fromEntries((await schemaCachePoints()).map((p) => [p.attributes.result, p.value]));
    expect(byResult).toEqual({ miss: 1, hit: 1 });

    await transport.close();
  });
});

describe("schema fetch failures", () => {
  // Logged at WARN rather than ERROR: an unfetchable writer schema takes the
  // `validation.onSchemaUnavailable` path, which defaults to nack, and a policy doing what it was
  // configured to do is not a library failure. The cause is still named either way.
  test("nacks and names the cause when the client cannot fetch schemas", async () => {
    // A client built without schema support still receives schema-encoded messages; decoding them
    // against the reader alone would silently mis-read anything the writer changed.
    const { subscription, client } = fakeClient();
    const transport = transportWith(client);
    const errors: string[] = [];
    vi.spyOn(transport["logger"], "warn").mockImplementation((m: unknown) => void errors.push(String(m)));

    transport.addHandler(TYPE, (() => {}) as never, true);
    await listenReady(transport);

    const message = schemaMessage("e1");
    subscription.emit("message", message);
    await settle();

    expect(message.nack).toHaveBeenCalledTimes(1);
    expect(errors.join("\n")).toMatch(/does not expose schema/);

    await transport.close();
  });

  test("nacks and names the cause when the schema has no definition", async () => {
    const { subscription, client } = fakeClient(vi.fn(() => ({ get: async () => ({ definition: null }) })));
    const transport = transportWith(client);
    const errors: string[] = [];
    vi.spyOn(transport["logger"], "warn").mockImplementation((m: unknown) => void errors.push(String(m)));

    transport.addHandler(TYPE, (() => {}) as never, true);
    await listenReady(transport);

    const message = schemaMessage("e1");
    subscription.emit("message", message);
    await settle();

    expect(message.nack).toHaveBeenCalledTimes(1);
    expect(errors.join("\n")).toMatch(/has no definition/);

    await transport.close();
  });
});
