/**
 * The domain side of the example.
 *
 * Note what is absent: nothing here imports Werken, Nest or `@google-cloud/pubsub`. That is the
 * point of §1.3 — the domain layer needs no Pub/Sub or CloudEvents knowledge whatsoever.
 */

export interface Shipment {
  readonly id: string;
  readonly destination: string;
}

/** A port the consumer depends on. Substituted with a fake in tests. */
export abstract class ShipmentLookup {
  abstract find(id: string): Promise<Shipment | undefined>;
}

export interface DispatchCommand {
  readonly shipment: Shipment;
  readonly carrier: string;
  readonly occurredAt: Date;
}

export abstract class DispatchShipment {
  abstract execute(command: DispatchCommand): Promise<void>;
}
