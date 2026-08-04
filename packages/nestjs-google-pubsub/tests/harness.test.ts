import "reflect-metadata";
import { Controller, Injectable, Module } from "@nestjs/common";
import { Ctx, EventPattern, Payload } from "@nestjs/microservices";
import { afterEach, describe, expect, test } from "vitest";
import { createWerkenTestHarness } from "@werken/nestjs-google-pubsub/testing";
import { TerminalEventError } from "@werken/nestjs-google-pubsub";
import type { WerkenTestHarness } from "@werken/nestjs-google-pubsub/testing";
import type { CloudEventContext } from "@werken/nestjs-google-pubsub";

const TYPE = "com.example.thing.happened.v1";

/** Stands in for a port the consumer depends on, so overrides have something to replace. */
@Injectable()
class ThingStore {
  readonly saved: Array<{ data: unknown; ctx: CloudEventContext }> = [];
  save(data: unknown, ctx: CloudEventContext) {
    this.saved.push({ data, ctx });
  }
}

@Controller()
class ThingConsumer {
  constructor(private readonly store: ThingStore) {}

  @EventPattern(TYPE)
  onThingHappened(@Payload() data: unknown, @Ctx() ctx: CloudEventContext) {
    if ((data as { explode?: boolean }).explode) throw new Error("transient");
    if ((data as { terminal?: boolean }).terminal) throw new TerminalEventError("will never resolve");
    this.store.save(data, ctx);
  }
}

@Module({ controllers: [ThingConsumer], providers: [ThingStore] })
class WorkerModule {}

let harness: WerkenTestHarness;
afterEach(async () => {
  await harness?.close();
});

describe("createWerkenTestHarness", () => {
  test("routes an emitted event to the matching handler and acks it", async () => {
    harness = await createWerkenTestHarness({ module: WorkerModule });

    await harness.emit(TYPE, { hello: "world" });

    expect(harness.acked).toHaveLength(1);
    expect(harness.nacked).toHaveLength(0);
    expect(harness.deadLettered).toHaveLength(0);
  });

  test("gives the handler the payload and a populated context", async () => {
    harness = await createWerkenTestHarness({ module: WorkerModule });

    await harness.emit(TYPE, { hello: "world" }, { subject: "thing-42", deliveryAttempt: 3 });

    const store = harness.get<ThingStore>(ThingStore);
    expect(store.saved).toHaveLength(1);
    expect(store.saved[0].data).toEqual({ hello: "world" });
    expect(store.saved[0].ctx.type).toBe(TYPE);
    expect(store.saved[0].ctx.subject).toBe("thing-42");
    expect(store.saved[0].ctx.deliveryAttempt).toBe(3);
  });

  test("records a nack when the handler throws", async () => {
    harness = await createWerkenTestHarness({ module: WorkerModule });

    await harness.emit(TYPE, { explode: true });

    expect(harness.nacked).toHaveLength(1);
    expect(harness.acked).toHaveLength(0);
  });

  test("acks an event no handler matches", async () => {
    harness = await createWerkenTestHarness({ module: WorkerModule });

    await harness.emit("com.example.unrelated.v1", {});

    expect(harness.acked).toHaveLength(1);
  });

  test("applies provider overrides so ports can be faked", async () => {
    const fake = new ThingStore();
    harness = await createWerkenTestHarness({
      module: WorkerModule,
      overrides: [{ provide: ThingStore, useValue: fake }],
    });

    await harness.emit(TYPE, { hello: "world" });

    expect(fake.saved).toHaveLength(1);
    expect(harness.get<ThingStore>(ThingStore)).toBe(fake);
  });

  // The harness dedupes by ce-id via its default in-memory store, exactly as production would —
  // so generated ids must be unique per emit, or every event after the first is silently skipped.
  test("gives each default-id emit a fresh ce-id so none are dropped as duplicates", async () => {
    harness = await createWerkenTestHarness({ module: WorkerModule });

    await harness.emit(TYPE, { n: 1 });
    await harness.emit(TYPE, { n: 2 });

    const store = harness.get<ThingStore>(ThingStore);
    expect(store.saved).toHaveLength(2);
    expect(store.saved[0].ctx.id).not.toBe(store.saved[1].ctx.id);
  });

  // Once drained, the transport has stopped listening: the emitted message reaches nothing and the
  // promise waiting for its ack or nack never settles. A hung test says nothing about what broke.
  test("fails loudly when emitting after drain rather than hanging", async () => {
    harness = await createWerkenTestHarness({ module: WorkerModule });
    await harness.drain();

    await expect(harness.emit(TYPE, { n: 1 })).rejects.toThrow(/drain/i);
  });

  test("needs no network, credentials or emulator", async () => {
    delete process.env.PUBSUB_EMULATOR_HOST;
    harness = await createWerkenTestHarness({ module: WorkerModule });

    await harness.emit(TYPE, { hello: "world" });

    expect(harness.acked).toHaveLength(1);
  });
});

describe("emitRaw", () => {
  test("drives envelope-level tests with hand-built attributes", async () => {
    harness = await createWerkenTestHarness({ module: WorkerModule });

    await harness.emitRaw(
      {
        "ce-specversion": "1.0",
        "ce-id": "raw-1",
        "ce-source": "https://example.test/service",
        "ce-type": TYPE,
      },
      JSON.stringify({ hello: "raw" }),
    );

    expect(harness.acked).toHaveLength(1);
    expect(harness.get<ThingStore>(ThingStore).saved[0].data).toEqual({ hello: "raw" });
  });

  // validation.onInvalidEnvelope defaults to 'dead-letter'. The original is acked only
  // because a copy is safely on the dead-letter topic.
  test("dead-letters a malformed envelope by default", async () => {
    harness = await createWerkenTestHarness({ module: WorkerModule });

    await harness.emitRaw({ "ce-specversion": "1.0" }, "{}");

    expect(harness.deadLettered).toHaveLength(1);
    expect(harness.deadLettered[0].stage).toBe("envelope");
    expect(harness.deadLettered[0].reason).toMatch(/ce-id/);
    expect(harness.nacked).toHaveLength(0);
  });

  test("nacks a malformed envelope when the policy says so", async () => {
    harness = await createWerkenTestHarness({
      module: WorkerModule,
      validation: { onInvalidEnvelope: "nack" },
    });

    await harness.emitRaw({ "ce-specversion": "1.0" }, "{}");

    expect(harness.nacked).toHaveLength(1);
    expect(harness.deadLettered).toHaveLength(0);
  });

  test("dead-letters a terminal handler failure with the handler stage", async () => {
    harness = await createWerkenTestHarness({ module: WorkerModule });

    await harness.emit(TYPE, { terminal: true });

    expect(harness.deadLettered).toHaveLength(1);
    expect(harness.deadLettered[0].stage).toBe("handler");
    expect(harness.deadLettered[0].reason).toContain("will never resolve");
    expect(harness.nacked).toHaveLength(0);
  });
});

describe("deterministic clock", () => {
  test("stamps ce-time from the injected clock", async () => {
    const now = new Date("2026-08-02T12:00:00.000Z");
    harness = await createWerkenTestHarness({ module: WorkerModule, now: () => now });

    await harness.emit(TYPE, {});

    expect(harness.get<ThingStore>(ThingStore).saved[0].ctx.time).toEqual(now);
  });

  test("an explicit time on the emit wins over the clock", async () => {
    harness = await createWerkenTestHarness({
      module: WorkerModule,
      now: () => new Date("2026-08-02T12:00:00.000Z"),
    });

    await harness.emit(TYPE, {}, { time: new Date("2026-08-01T09:30:00.000Z") });

    expect(harness.get<ThingStore>(ThingStore).saved[0].ctx.time).toEqual(new Date("2026-08-01T09:30:00.000Z"));
  });
});

/**
 * `Promise.all` over several emits is the natural way to test concurrent and duplicate delivery in
 * a message-processing harness, so the harness has to survive it. It used to hold one global slot
 * for the message being processed, which credited every dead-letter to whichever emit happened to
 * run last.
 */
describe("concurrent emits", () => {
  test("attributes each dead-letter to the message it came from", async () => {
    const harness = await createWerkenTestHarness({ module: WorkerModule });

    try {
      await Promise.all([
        harness.emitRaw({ "ce-specversion": "1.0" }, JSON.stringify({ first: true })),
        harness.emitRaw({ "ce-specversion": "1.0" }, JSON.stringify({ second: true })),
      ]);

      expect(harness.deadLettered).toHaveLength(2);
      expect(harness.deadLettered.map((r) => r.body).sort()).toEqual([
        JSON.stringify({ first: true }),
        JSON.stringify({ second: true }),
      ]);
    } finally {
      await harness.close();
    }
  });

  test("keeps both records for two indistinguishable messages", async () => {
    const harness = await createWerkenTestHarness({ module: WorkerModule });

    try {
      const same = () => harness.emitRaw({ "ce-specversion": "1.0" }, JSON.stringify({ same: true }));
      await Promise.all([same(), same()]);

      expect(harness.deadLettered).toHaveLength(2);
    } finally {
      await harness.close();
    }
  });
});

describe("after the harness stops", () => {
  // drain() already refused; close() left emit() hanging forever instead, which tells a reader
  // nothing about what they did wrong.
  test("close() refuses further emits rather than hanging", async () => {
    const harness = await createWerkenTestHarness({ module: WorkerModule });
    await harness.close();

    await expect(harness.emit(TYPE, { hello: "world" })).rejects.toThrow(/closed/);
  });
});

describe("the injected clock", () => {
  test("expires an idempotency marker once time passes the TTL", async () => {
    let clock = new Date("2026-08-03T10:00:00.000Z");
    const harness = await createWerkenTestHarness({
      module: WorkerModule,
      now: () => clock,
      idempotency: { ttlMs: 60_000, consumer: "clock-test" },
    });

    try {
      await harness.emit(TYPE, { hello: "world" }, { id: "same-event" });
      await harness.emit(TYPE, { hello: "world" }, { id: "same-event" });
      expect(harness.get<ThingStore>(ThingStore).saved).toHaveLength(1);

      clock = new Date("2026-08-03T10:02:00.000Z");
      await harness.emit(TYPE, { hello: "world" }, { id: "same-event" });

      expect(harness.get<ThingStore>(ThingStore).saved).toHaveLength(2);
    } finally {
      await harness.close();
    }
  });
});
