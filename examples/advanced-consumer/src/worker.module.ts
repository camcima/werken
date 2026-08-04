import { Module } from "@nestjs/common";
import { ShipmentEventsConsumer } from "./adapters/inbound/shipment-events.consumer.js";
import { ShipmentProjection } from "./domain/ports.js";

/**
 * Stand-in until Task 4 lands `PgShipmentProjection`. Kept here — rather than left unbound — so
 * this task's module is independently testable: a missing provider would fail DI resolution before
 * the harness ever reached the handler under test.
 */
class UnconfiguredProjection extends ShipmentProjection {
  async apply() {
    throw new Error("werken example: no projection configured");
  }
}

@Module({
  controllers: [ShipmentEventsConsumer],
  providers: [{ provide: ShipmentProjection, useClass: UnconfiguredProjection }],
})
export class WorkerModule {}
