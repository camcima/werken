import { Module } from "@nestjs/common";
import { ShipmentEventsConsumer } from "./adapters/inbound/shipment-events.consumer.js";
import { DispatchShipment, ShipmentLookup } from "./domain/ports.js";

/** Stand-ins so the example runs on its own; a real service binds these to real adapters. */
class InMemoryShipmentLookup extends ShipmentLookup {
  async find(id: string) {
    return id.startsWith("known-") ? { id, destination: "warehouse-3" } : undefined;
  }
}

class LoggingDispatchShipment extends DispatchShipment {
  async execute(command: { shipment: { id: string }; carrier: string }) {
    console.log(`dispatched ${command.shipment.id} via ${command.carrier}`);
  }
}

@Module({
  controllers: [ShipmentEventsConsumer],
  providers: [
    { provide: ShipmentLookup, useClass: InMemoryShipmentLookup },
    { provide: DispatchShipment, useClass: LoggingDispatchShipment },
  ],
})
export class WorkerModule {}
