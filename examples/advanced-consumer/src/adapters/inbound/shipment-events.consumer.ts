import { Controller } from "@nestjs/common";
import { Ctx, EventPattern, Payload } from "@nestjs/microservices";
import { TerminalEventError } from "@werken/nestjs-google-pubsub";
import type { CloudEventContext } from "@werken/nestjs-google-pubsub";
import { ShipmentProjection } from "../../domain/ports.js";

interface ShipmentEventV1 {
  readonly shipmentId?: string;
  readonly carrier?: string | null;
}

@Controller()
export class ShipmentEventsConsumer {
  constructor(private readonly projection: ShipmentProjection) {}

  /**
   * Everything on the shipment stream except cancellation. The exact pattern below outranks this
   * one, which is what lets a new shipment event type land here without a code change.
   */
  @EventPattern("com.example.shipment.*")
  async onShipmentEvent(@Payload() data: ShipmentEventV1, @Ctx() ctx: CloudEventContext) {
    await this.projection.apply({
      shipmentId: requireShipmentId(data),
      status: "ready",
      carrier: data.carrier ?? undefined,
      occurredAt: ctx.time,
    });
  }

  @EventPattern("com.example.shipment.cancelled.v1")
  async onShipmentCancelled(@Payload() data: ShipmentEventV1, @Ctx() ctx: CloudEventContext) {
    await this.projection.apply({
      shipmentId: requireShipmentId(data),
      status: "cancelled",
      // Deliberately erases the carrier (the projection writes it as NULL): a cancelled shipment
      // has no carrier handling it, and leaving the old one there reads as though it still does.
      carrier: undefined,
      occurredAt: ctx.time,
    });
  }
}

/**
 * Terminal, not transient: a payload with no shipment id will never acquire one on redelivery, so
 * retrying only burns the budget. The detail rides along into `werken-dl-detail`.
 */
function requireShipmentId(data: ShipmentEventV1): string {
  if (data.shipmentId === undefined || data.shipmentId === "") {
    throw new TerminalEventError("payload carries no shipmentId", { received: Object.keys(data) });
  }
  return data.shipmentId;
}
