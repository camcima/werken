import "reflect-metadata";
import { afterEach, describe, expect, test } from "vitest";
import { createWerkenTestHarness } from "@werken/nestjs-google-pubsub/testing";
import type { WerkenTestHarness } from "@werken/nestjs-google-pubsub/testing";
import { WorkerModule } from "../src/worker.module.js";
import { ShipmentProjection } from "../src/domain/ports.js";
import type { ProjectionChange } from "../src/domain/ports.js";

class RecordingProjection extends ShipmentProjection {
  readonly applied: ProjectionChange[] = [];
  async apply(change: ProjectionChange) {
    this.applied.push(change);
  }
}

/**
 * The harness runs the real module through real Nest DI and the real pipeline, with only the broker
 * in memory. It is `schemas: "passthrough"`, so payloads here are plain JSON — Avro decoding is
 * exercised against the emulator in tests/integration instead.
 */
describe("shipment events consumer", () => {
  let harness: WerkenTestHarness;
  let projection: RecordingProjection;

  afterEach(async () => {
    await harness?.close();
  });

  const start = async () => {
    projection = new RecordingProjection();
    harness = await createWerkenTestHarness({
      module: WorkerModule,
      overrides: [{ provide: ShipmentProjection, useValue: projection }],
    });
  };

  test("projects a ready event matched by the wildcard route", async () => {
    await start();

    await harness.emit("com.example.shipment.ready.v1", { shipmentId: "s-1", carrier: "dhl" }, { subject: "s-1" });

    expect(projection.applied).toEqual([
      { shipmentId: "s-1", status: "ready", carrier: "dhl", occurredAt: expect.any(Date) },
    ]);
    expect(harness.acked).toHaveLength(1);
  });

  // The exact pattern must win over com.example.shipment.* — otherwise a cancellation would be
  // projected as a ready, which is silent data corruption rather than a loud failure.
  test("routes a cancellation to the exact handler, not the wildcard", async () => {
    await start();

    await harness.emit("com.example.shipment.cancelled.v1", { shipmentId: "s-2" }, { subject: "s-2" });

    expect(projection.applied).toEqual([
      { shipmentId: "s-2", status: "cancelled", carrier: undefined, occurredAt: expect.any(Date) },
    ]);
  });

  test("dead-letters a shipment id the payload never carried", async () => {
    await start();

    await harness.emit("com.example.shipment.ready.v1", { carrier: "dhl" }, { subject: "none" });

    expect(projection.applied).toHaveLength(0);
    expect(harness.deadLettered).toHaveLength(1);
    expect(harness.deadLettered[0].reason).toMatch(/shipmentId/);
  });
});
