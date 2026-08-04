import { Inject, Injectable } from "@nestjs/common";
import type { Pool } from "pg";
import { ShipmentProjection } from "../../domain/ports.js";
import type { ProjectionChange } from "../../domain/ports.js";

/** DI token for the shared pool, so the projection and the idempotency store use one connection set. */
export const PG_POOL = Symbol("PG_POOL");

@Injectable()
export class PgShipmentProjection extends ShipmentProjection {
  constructor(@Inject(PG_POOL) private readonly pool: Pool) {
    super();
  }

  async apply(change: ProjectionChange): Promise<void> {
    // Latest-event-time wins, not latest-arrival. Two guards doing two different jobs: the
    // idempotency store prevents re-applying *this* event; the `updated_at` guard is what prevents
    // an older one. The store keys on {consumer, source, ce-id}, so it says nothing about a stale
    // event arriving after a fresher one, and past `idempotency.ttlMs` even the same id can return.
    await this.pool.query(
      `INSERT INTO shipment_projection (shipment_id, status, carrier, updated_at)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (shipment_id) DO UPDATE
         SET status = EXCLUDED.status, carrier = EXCLUDED.carrier, updated_at = EXCLUDED.updated_at
         WHERE shipment_projection.updated_at <= EXCLUDED.updated_at`,
      [change.shipmentId, change.status, change.carrier ?? null, change.occurredAt],
    );
  }
}
