/**
 * The domain side. Nothing here imports Werken, Nest or `@google-cloud/pubsub` — the handler's
 * dependencies are expressed as ports so the test can substitute them without a broker.
 */

export interface ProjectionChange {
  readonly shipmentId: string;
  readonly status: "ready" | "cancelled";
  readonly carrier?: string;
  readonly occurredAt: Date;
}

/** The read model this service maintains. Backed by Postgres in production, a fake in tests. */
export abstract class ShipmentProjection {
  abstract apply(change: ProjectionChange): Promise<void>;
}
