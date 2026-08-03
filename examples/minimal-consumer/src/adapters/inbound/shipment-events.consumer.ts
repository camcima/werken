import { Controller } from "@nestjs/common";
import { Ctx, EventPattern, Payload } from "@nestjs/microservices";
import { TerminalEventError } from "@werken/nestjs-google-pubsub";
import type { CloudEventContext } from "@werken/nestjs-google-pubsub";
import { DispatchShipment, ShipmentLookup } from "../../domain/ports.js";

interface ShipmentReadyV1 {
  readonly shipmentId: string;
  readonly carrier: string;
}

/**
 * The whole consumer. Business logic only — §1.3's target is under 20 lines, and the handler below
 * is 12.
 *
 * Everything absent from this file is the point: no ack/nack, no retry policy, no schema
 * resolution, no de-duplication, no trace propagation, no shutdown handling.
 */
@Controller()
export class ShipmentEventsConsumer {
  constructor(
    private readonly dispatch: DispatchShipment,
    private readonly shipments: ShipmentLookup,
  ) {}

  @EventPattern("com.example.shipment.ready.v1")
  async onShipmentReady(@Payload() data: ShipmentReadyV1, @Ctx() ctx: CloudEventContext) {
    const shipment = await this.shipments.find(data.shipmentId);
    if (shipment === undefined) {
      // Terminal, not transient: this shipment will never appear, so retrying only burns the
      // budget and — behind an ordering key — blocks every later event for the same entity.
      throw new TerminalEventError(`unknown shipment ${data.shipmentId}`, { shipmentId: data.shipmentId });
    }

    await this.dispatch.execute({
      shipment,
      carrier: data.carrier,
      occurredAt: ctx.time,
    });
  }
}
