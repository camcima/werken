import { Module } from "@nestjs/common";
import type { DynamicModule } from "@nestjs/common";
import { Pool } from "pg";
import { ShipmentEventsConsumer } from "./adapters/inbound/shipment-events.consumer.js";
import { ShipmentProjection } from "./domain/ports.js";
import { PG_POOL, PgShipmentProjection } from "./adapters/outbound/pg-projection.js";

@Module({
  controllers: [ShipmentEventsConsumer],
  providers: [{ provide: ShipmentProjection, useClass: PgShipmentProjection }],
})
export class WorkerModule {
  /**
   * Registers the shared Postgres pool.
   *
   * Deliberately not a static provider on the bare module: Nest's instance loader eagerly
   * constructs every singleton provider a module declares — override or no override — so a
   * `PG_POOL` factory declared directly here would run, and throw on a missing `DATABASE_URL`,
   * even under the test harness, which overrides `ShipmentProjection` precisely to avoid touching
   * Postgres at all. Routing the pool through `forRoot` keeps the bare `WorkerModule` import (what
   * the harness and Task 3's handler test use) free of it, while production wiring supplies —
   * or shares — one real `Pool`.
   */
  static forRoot(pool?: Pool): DynamicModule {
    return {
      module: WorkerModule,
      providers: [
        {
          provide: PG_POOL,
          useFactory: () => pool ?? new Pool({ connectionString: requireDatabaseUrl() }),
        },
      ],
      exports: [PG_POOL],
    };
  }
}

/**
 * Fail loudly rather than degrading. A `process.env.X!` only silences the compiler: at runtime the
 * value is still `undefined`, so the consumer starts, subscribes to the literal string "undefined",
 * reports healthy and receives nothing — the exact silent-misconfiguration failure this example
 * exists to argue against.
 */
export function requireEnv(name: string, hint: string): string {
  const value = process.env[name];
  if (value === undefined || value === "") {
    throw new Error(`werken example: ${name} is not set. ${hint}`);
  }
  return value;
}

export function requireDatabaseUrl(): string {
  return requireEnv(
    "DATABASE_URL",
    "Start Postgres with `docker compose up -d` and use " +
      "postgresql://postgres:postgres@localhost:55432/werken_test",
  );
}
