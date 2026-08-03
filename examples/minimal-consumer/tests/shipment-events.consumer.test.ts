import "reflect-metadata";
import { afterEach, describe, expect, test } from "vitest";
import { createWerkenTestHarness } from "@werken/nestjs-google-pubsub/testing";
import type { WerkenTestHarness } from "@werken/nestjs-google-pubsub/testing";
import { WorkerModule } from "../src/worker.module.js";
import { DispatchShipment, ShipmentLookup } from "../src/domain/ports.js";

const TYPE = "com.example.shipment.ready.v1";

class FakeShipments extends ShipmentLookup {
  async find(id: string) {
    return id === "known-1" ? { id, destination: "warehouse-3" } : undefined;
  }
}

class RecordingDispatch extends DispatchShipment {
  readonly dispatched: Array<{ shipment: { id: string }; carrier: string; occurredAt: Date }> = [];
  async execute(command: { shipment: { id: string }; carrier: string; occurredAt: Date }) {
    this.dispatched.push(command);
  }
}

/**
 * §9 acceptance criterion 8: handler tests run green with no network, no credentials and no
 * emulator. Nothing in this file touches GCP.
 */
describe("ShipmentEventsConsumer", () => {
  let harness: WerkenTestHarness;
  let dispatch: RecordingDispatch;

  const start = async () => {
    dispatch = new RecordingDispatch();
    harness = await createWerkenTestHarness({
      module: WorkerModule,
      overrides: [
        { provide: ShipmentLookup, useValue: new FakeShipments() },
        { provide: DispatchShipment, useValue: dispatch },
      ],
    });
  };

  afterEach(async () => {
    await harness?.close();
  });

  test("dispatches a shipment it can find", async () => {
    await start();

    await harness.emit(TYPE, { shipmentId: "known-1", carrier: "acme-freight" }, { subject: "known-1" });

    expect(dispatch.dispatched).toHaveLength(1);
    expect(dispatch.dispatched[0].carrier).toBe("acme-freight");
    expect(harness.acked).toHaveLength(1);
  });

  test("gives the handler the occurrence time from the envelope", async () => {
    await start();
    const occurredAt = new Date("2026-08-01T09:30:00.000Z");

    await harness.emit(TYPE, { shipmentId: "known-1", carrier: "acme-freight" }, { time: occurredAt });

    expect(dispatch.dispatched[0].occurredAt).toEqual(occurredAt);
  });

  // A shipment that will never exist must not burn its retry budget.
  test("dead-letters an unknown shipment instead of retrying forever", async () => {
    await start();

    await harness.emit(TYPE, { shipmentId: "missing-9", carrier: "acme-freight" });

    expect(dispatch.dispatched).toHaveLength(0);
    expect(harness.deadLettered).toHaveLength(1);
    expect(harness.deadLettered[0].stage).toBe("handler");
    expect(harness.deadLettered[0].reason).toContain("missing-9");
    expect(harness.nacked).toHaveLength(0);
  });

  test("ignores event types this consumer does not handle", async () => {
    await start();

    await harness.emit("com.example.unrelated.v1", {});

    expect(dispatch.dispatched).toHaveLength(0);
    expect(harness.acked).toHaveLength(1);
  });
});
