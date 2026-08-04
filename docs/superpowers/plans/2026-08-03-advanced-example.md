# Advanced Example Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `examples/advanced-consumer` (Avro + Postgres idempotency + wildcard routing) and `examples/publisher`, so every advertised feature has a runnable reference.

**Architecture:** Two new private workspaces alongside `examples/minimal-consumer`, following its ports-and-adapters layout. The consumer maintains a Postgres projection, which is what makes idempotency non-optional; the publisher is a plain program that emits the events it reads. They share one Avro schema and one provisioning script.

**Tech Stack:** TypeScript (ES2022, Node16 modules), NestJS 11 microservices, `@google-cloud/pubsub` 5, `avsc`, `pg`, Vitest, pnpm workspaces.

**Design spec:** `docs/superpowers/specs/2026-08-03-advanced-example-design.md`

## Global Constraints

- Every example workspace sets `"private": true` and `"type": "module"`.
- Examples must never be published. `scripts/publish.sh` restricts publication with `--filter "./packages/*"`.
- Examples live outside `packages/`; nothing in an example may be imported by library source.
- Each example has `tsconfig.json` (emits, `rootDir: src`, `include: ["src"]`, references the library) and `tsconfig.test.json` (`noEmit`, `composite: false`, `rootDir: "."`, `include: ["src","tests"]`), matching `examples/minimal-consumer`.
- `tsc --build` is incremental against `*.tsbuildinfo`. After deleting `dist`, run `pnpm run clean` before `pnpm run build`.
- Postgres for local runs: `postgresql://postgres:postgres@localhost:55432/werken_test` (port 55432, not 5432).
- Pub/Sub emulator: `PUBSUB_EMULATOR_HOST=localhost:8085`, project `werken-dev`.
- Gates that must pass before any commit: `pnpm run lint`, `pnpm run format:check`, `pnpm run lint:dead-code`, `pnpm run typecheck`, `pnpm test`.
- Commit messages follow Conventional Commits (commitlint runs on `commit-msg`).

---

### Task 1: Restrict publication to real packages

Standalone hardening, valuable whether or not the examples land. Today a new example missing `private: true` would be published to npm by `pnpm -r publish`.

**Files:**

- Modify: `scripts/publish.sh`
- Modify: `CONTRIBUTING.md` (Releasing section)

**Interfaces:**

- Consumes: nothing.
- Produces: nothing importable. Later tasks rely on this making a forgotten `private` flag harmless.

- [ ] **Step 1: Prove the current behaviour is unsafe**

```bash
mkdir -p examples/__probe && echo '{"name":"@werken/example-probe","version":"0.0.0"}' > examples/__probe/package.json
pnpm -r exec node -e "const p=require('./package.json');if(!p.private)console.log('WOULD PUBLISH:',p.name)" 2>/dev/null | grep WOULD
```

Expected: three lines, including `WOULD PUBLISH: @werken/example-probe`.

- [ ] **Step 2: Prove the allowlist excludes it**

```bash
pnpm --filter "./packages/*" -r exec node -e "console.log('SELECTED:',require('./package.json').name)" 2>/dev/null | grep SELECTED
```

Expected: exactly two lines — `@werken/cloudevents` and `@werken/nestjs-google-pubsub`. No probe.

- [ ] **Step 3: Remove the probe**

```bash
rm -rf examples/__probe
```

- [ ] **Step 4: Apply the allowlist**

In `scripts/publish.sh`, replace both `exec pnpm -r publish ...` lines so each carries the filter, and update the header comment:

```sh
# Publishes every public workspace package.
#
# The --filter allowlist is load-bearing: `pnpm -r publish` publishes every workspace that is not
# marked private, so an example that forgets `"private": true` would be published to npm as a real
# package — irreversibly. Restricting to ./packages/* makes that impossible by construction rather
# than by every example remembering to opt out.

if [ -n "$NPM_OTP" ]; then
  exec pnpm -r --filter "./packages/*" publish --no-git-checks --otp "$NPM_OTP"
fi

exec pnpm -r --filter "./packages/*" publish --no-git-checks
```

- [ ] **Step 5: Note it in CONTRIBUTING**

In the `## Releasing` section, after the three-command block, add:

```markdown
Publication is restricted to `./packages/*`. Examples are private workspaces, but the filter is
what actually prevents one being published if that flag is ever forgotten.
```

- [ ] **Step 6: Verify and commit**

```bash
pnpm run format:check && pnpm run lint
git add scripts/publish.sh CONTRIBUTING.md
git commit -m "fix(release): restrict publication to real packages

pnpm -r publish publishes every workspace that is not marked private, so an
example missing that flag would be published to npm as a real package. The
--filter allowlist makes it impossible by construction."
```

---

### Task 2: Let integration tests live with the example they test

`INTEGRATION_GLOB` only reaches `packages/*`, which is why the existing example test was parked inside the library's test folder. Three examples makes that untenable.

**Files:**

- Create: `tests/support/requires.ts`
- Delete: `packages/nestjs-google-pubsub/tests/integration/requires.ts`
- Modify: `vitest.shared.ts`
- Modify: `tsconfig.typecheck.json`
- Modify: `knip.ts`
- Move: `packages/nestjs-google-pubsub/tests/integration/example-worker.integration.test.ts` → `examples/minimal-consumer/tests/integration/worker.integration.test.ts`
- Modify: every file importing `./requires.js` under `packages/nestjs-google-pubsub/tests/integration/`

**Interfaces:**

- Produces: `skipUnlessAvailable(dependency: string, available: unknown): boolean`, importable as `@werken/test-support`.

- [ ] **Step 1: Move the skip helper to a shared module**

Create `tests/support/requires.ts` with the exact current contents of `packages/nestjs-google-pubsub/tests/integration/requires.ts`, then delete the original:

```bash
mkdir -p tests/support
git mv packages/nestjs-google-pubsub/tests/integration/requires.ts tests/support/requires.ts
```

- [ ] **Step 2: Alias it, the way `/internal` is aliased**

In `vitest.shared.ts`, add to `shared.resolve.alias`:

```ts
      {
        // Shared by both packages' and examples' integration suites. Aliased rather than imported
        // by relative path so a test does not have to know how deep it sits.
        find: /^@werken\/test-support$/,
        replacement: fromHere("./tests/support/requires.ts"),
      },
```

In `tsconfig.typecheck.json`, add to `compilerOptions.paths`:

```json
      "@werken/test-support": ["tests/support/requires.ts"]
```

and add `"tests/support"` to the `include` array.

- [ ] **Step 3: Widen the integration glob**

In `vitest.shared.ts`:

```ts
/** Files owned by the integration run. Excluded from the unit run so neither reports skips. */
export const INTEGRATION_GLOB = "{packages,examples}/*/tests/integration/**/*.test.ts";
```

- [ ] **Step 4: Repoint every importer**

Replace `from "./requires.js"` with `from "@werken/test-support"` in all four integration tests under `packages/nestjs-google-pubsub/tests/integration/`:

```bash
rg -l 'from "./requires.js"' packages/nestjs-google-pubsub/tests/integration/ \
  | xargs sed -i 's|from "./requires.js"|from "@werken/test-support"|'
```

- [ ] **Step 5: Move the example's test to the example**

```bash
mkdir -p examples/minimal-consumer/tests/integration
git mv packages/nestjs-google-pubsub/tests/integration/example-worker.integration.test.ts \
       examples/minimal-consumer/tests/integration/worker.integration.test.ts
```

Then fix its `WORKER` constant, which is now two directories shallower:

```ts
const WORKER = fileURLToPath(new URL("../../dist/main.worker.js", import.meta.url));
```

- [ ] **Step 6: Add the root workspace to knip**

In `knip.ts`, extend the root workspace so the shared helper is seen:

```ts
    ".": {
      entry: ["tests/support/requires.ts"],
      project: ["*.ts", "tests/**/*.ts"],
    },
```

- [ ] **Step 7: Verify nothing regressed**

```bash
pnpm run build
pnpm test                 # expect 28 files / 395 tests
pnpm run typecheck        # expect 0 errors
pnpm run lint:dead-code   # expect exit 0
docker compose up -d && pnpm run test:integration   # expect 8 files / 24 tests
```

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "test: let integration tests live with the example they test

INTEGRATION_GLOB only reached packages/*, so the minimal-consumer integration
test was parked inside the library's test folder. Widen the glob, move the test
to the example, and share the skip helper as @werken/test-support."
```

---

### Task 3: `advanced-consumer` — domain, routing and handler

No infrastructure. The handler and its routing are pure logic, so the harness covers them.

**Files:**

- Create: `examples/advanced-consumer/package.json`
- Create: `examples/advanced-consumer/tsconfig.json`
- Create: `examples/advanced-consumer/tsconfig.test.json`
- Create: `examples/advanced-consumer/src/domain/ports.ts`
- Create: `examples/advanced-consumer/src/adapters/inbound/shipment-events.consumer.ts`
- Create: `examples/advanced-consumer/src/worker.module.ts`
- Test: `examples/advanced-consumer/tests/shipment-events.consumer.test.ts`

**Interfaces:**

- Produces: `ShipmentProjection` (abstract, `apply(change: ProjectionChange): Promise<void>`), `ProjectionChange { shipmentId: string; status: "ready" | "cancelled"; carrier?: string; occurredAt: Date }`, `WorkerModule`.
- Consumes: `TerminalEventError`, `CloudEventContext` from `@werken/nestjs-google-pubsub`.

- [ ] **Step 1: Scaffold the workspace**

`examples/advanced-consumer/package.json`:

```json
{
  "name": "@werken/example-advanced-consumer",
  "version": "0.2.0",
  "private": true,
  "type": "module",
  "description": "Worked example: a Werken consumer with Avro schemas, Postgres idempotency and wildcard routing.",
  "scripts": {
    "build": "tsc --build",
    "start": "node dist/main.worker.js",
    "provision": "sh scripts/provision.sh",
    "test": "vitest run"
  },
  "dependencies": {
    "@nestjs/common": "^11.0.0",
    "@nestjs/core": "^11.0.0",
    "@nestjs/microservices": "^11.0.0",
    "@google-cloud/pubsub": "^5.0.0",
    "@werken/nestjs-google-pubsub": "workspace:*",
    "avsc": "^5.7.9",
    "pg": "^8.22.0",
    "reflect-metadata": "^0.2.2",
    "rxjs": "^7.8.2"
  }
}
```

`examples/advanced-consumer/tsconfig.json`:

```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": {
    "outDir": "dist",
    "rootDir": "src",
    "resolveJsonModule": true
  },
  "include": ["src"],
  "references": [
    {
      "path": "../../packages/nestjs-google-pubsub"
    }
  ]
}
```

`examples/advanced-consumer/tsconfig.test.json`:

```json
{
  "extends": "./tsconfig.json",
  "compilerOptions": {
    "composite": false,
    "noEmit": true,
    "rootDir": "."
  },
  "include": ["src", "tests"],
  "references": []
}
```

- [ ] **Step 2: Write the domain ports**

`examples/advanced-consumer/src/domain/ports.ts`:

```ts
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
```

- [ ] **Step 3: Write the failing handler test**

`examples/advanced-consumer/tests/shipment-events.consumer.test.ts`:

```ts
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
```

- [ ] **Step 4: Run it and watch it fail**

```bash
pnpm exec vitest run examples/advanced-consumer/tests/
```

Expected: FAIL — cannot resolve `../src/worker.module.js`.

- [ ] **Step 5: Write the consumer**

`examples/advanced-consumer/src/adapters/inbound/shipment-events.consumer.ts`:

```ts
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
```

`examples/advanced-consumer/src/worker.module.ts`:

```ts
import { Module } from "@nestjs/common";
import { ShipmentEventsConsumer } from "./adapters/inbound/shipment-events.consumer.js";
import { ShipmentProjection } from "./domain/ports.js";
import { PgShipmentProjection } from "./adapters/outbound/pg-projection.js";

@Module({
  controllers: [ShipmentEventsConsumer],
  providers: [{ provide: ShipmentProjection, useClass: PgShipmentProjection }],
})
export class WorkerModule {}
```

Note: `PgShipmentProjection` arrives in Task 4. Until then, temporarily provide an inline stub so this task stays independently testable:

```ts
class UnconfiguredProjection extends ShipmentProjection {
  async apply() {
    throw new Error("werken example: no projection configured");
  }
}
```

and register `{ provide: ShipmentProjection, useClass: UnconfiguredProjection }`. Task 4 replaces it.

- [ ] **Step 6: Run the tests**

```bash
pnpm exec vitest run examples/advanced-consumer/tests/
```

Expected: 3 passed.

- [ ] **Step 7: Commit**

```bash
git add examples/advanced-consumer
git commit -m "feat(example): add the advanced consumer's domain and routing

Wildcard route for the shipment stream with an exact cancellation route that
outranks it, and a TerminalEventError carrying structured detail."
```

---

### Task 4: `advanced-consumer` — Postgres adapters

The `SqlExecutor` contract is where the README says precision is load-bearing: `rowCount` must be rows _returned_, and `pg` types it `number | null`, so the `?? 0` decides whether de-duplication works at all.

**Files:**

- Create: `examples/advanced-consumer/src/adapters/outbound/pg-executor.ts`
- Create: `examples/advanced-consumer/src/adapters/outbound/pg-projection.ts`
- Modify: `examples/advanced-consumer/src/worker.module.ts` (drop the stub)
- Test: `examples/advanced-consumer/tests/pg-executor.test.ts`

**Interfaces:**

- Produces: `createPgExecutor(pool: Pool): SqlExecutor`, `class PgShipmentProjection extends ShipmentProjection` (constructor takes a `Pool`).
- Consumes: `ShipmentProjection`, `ProjectionChange` from Task 3.

- [ ] **Step 1: Write the failing executor test**

`examples/advanced-consumer/tests/pg-executor.test.ts`:

```ts
import { describe, expect, test, vi } from "vitest";
import { createPgExecutor } from "../src/adapters/outbound/pg-executor.js";

/** `pg` types QueryResult.rowCount as `number | null`. Null must read as zero, not NaN. */
describe("pg executor", () => {
  test("reports the row count the driver returned", async () => {
    const pool = { query: vi.fn(async () => ({ rowCount: 1 })) };
    const executor = createPgExecutor(pool as never);

    expect(await executor.execute("SELECT 1", [])).toEqual({ rowCount: 1 });
  });

  test("reads a null row count as zero", async () => {
    const pool = { query: vi.fn(async () => ({ rowCount: null })) };
    const executor = createPgExecutor(pool as never);

    expect(await executor.execute("SELECT 1", [])).toEqual({ rowCount: 0 });
  });

  test("passes sql and params straight through", async () => {
    const pool = { query: vi.fn(async () => ({ rowCount: 0 })) };
    const executor = createPgExecutor(pool as never);

    await executor.execute("SELECT $1", ["x"]);

    expect(pool.query).toHaveBeenCalledWith("SELECT $1", ["x"]);
  });
});
```

- [ ] **Step 2: Run it and watch it fail**

```bash
pnpm exec vitest run examples/advanced-consumer/tests/pg-executor.test.ts
```

Expected: FAIL — cannot resolve `../src/adapters/outbound/pg-executor.js`.

- [ ] **Step 3: Write the adapters**

`examples/advanced-consumer/src/adapters/outbound/pg-executor.ts`:

```ts
import type { Pool } from "pg";
import type { SqlExecutor } from "@werken/nestjs-google-pubsub";

/**
 * Mode A: a pool-backed executor. The dedup insert runs in its own transaction, separate from the
 * projection write, so a crash between the two still yields a duplicate — which is why the handler
 * stays idempotent regardless.
 *
 * `QueryResult.rowCount` is typed `number | null`, so the `?? 0` is load-bearing: the store reads
 * "rows returned" to decide whether a marker was newly written, and NaN would break both
 * directions silently.
 */
export function createPgExecutor(pool: Pool): SqlExecutor {
  return {
    execute: async (sql, params) => {
      const result = await pool.query(sql, params as unknown[]);
      return { rowCount: result.rowCount ?? 0 };
    },
  };
}
```

`examples/advanced-consumer/src/adapters/outbound/pg-projection.ts`:

```ts
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
    // Last-write-wins on the projection row. The idempotency store is what stops a redelivery
    // re-applying an older event on top of a newer one.
    await this.pool.query(
      `INSERT INTO shipment_projection (shipment_id, status, carrier, updated_at)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (shipment_id) DO UPDATE
         SET status = EXCLUDED.status, carrier = EXCLUDED.carrier, updated_at = EXCLUDED.updated_at`,
      [change.shipmentId, change.status, change.carrier ?? null, change.occurredAt],
    );
  }
}
```

- [ ] **Step 4: Wire the module to the real projection**

Replace the `UnconfiguredProjection` stub in `examples/advanced-consumer/src/worker.module.ts`:

```ts
import { Module } from "@nestjs/common";
import { Pool } from "pg";
import { ShipmentEventsConsumer } from "./adapters/inbound/shipment-events.consumer.js";
import { ShipmentProjection } from "./domain/ports.js";
import { PG_POOL, PgShipmentProjection } from "./adapters/outbound/pg-projection.js";

@Module({
  controllers: [ShipmentEventsConsumer],
  providers: [
    {
      provide: PG_POOL,
      useFactory: () => new Pool({ connectionString: requireDatabaseUrl() }),
    },
    { provide: ShipmentProjection, useClass: PgShipmentProjection },
  ],
  exports: [PG_POOL],
})
export class WorkerModule {}

/**
 * Fail loudly rather than degrading: a consumer that starts without a database looks healthy and
 * silently drops every projection write.
 */
function requireDatabaseUrl(): string {
  const url = process.env.DATABASE_URL;
  if (url === undefined || url === "") {
    throw new Error(
      "werken example: DATABASE_URL is not set. Start Postgres with `docker compose up -d` and use " +
        "postgresql://postgres:postgres@localhost:55432/werken_test",
    );
  }
  return url;
}
```

The harness test overrides `ShipmentProjection`, so the pool factory is never invoked there.

- [ ] **Step 5: Run the tests**

```bash
pnpm exec vitest run examples/advanced-consumer/tests/
```

Expected: 6 passed (3 handler + 3 executor).

- [ ] **Step 6: Commit**

```bash
git add examples/advanced-consumer
git commit -m "feat(example): add the advanced consumer's Postgres adapters

A Mode A SqlExecutor over a pg Pool, shared with the projection, and a
DATABASE_URL check that fails startup rather than degrading."
```

---

### Task 5: `advanced-consumer` — transport wiring, schema and provisioning

**Files:**

- Create: `examples/advanced-consumer/schema/shipment-events.avsc`
- Create: `examples/advanced-consumer/src/schema/reader-types.ts`
- Create: `examples/advanced-consumer/src/main.worker.ts`
- Create: `examples/advanced-consumer/scripts/provision.sh`
- Create: `examples/advanced-consumer/README.md`

**Interfaces:**

- Produces: `readerTypeFor(schemaName: string): avro.Type | undefined`, a runnable `dist/main.worker.js`.
- Consumes: `WorkerModule`, `PG_POOL` from Tasks 3–4.

- [ ] **Step 1: Write the Avro schema**

`examples/advanced-consumer/schema/shipment-events.avsc`. One schema per topic — Pub/Sub attaches exactly one — so `ce-type` is what distinguishes ready from cancelled, not the payload shape:

```json
{
  "type": "record",
  "name": "ShipmentEvent",
  "namespace": "com.example.shipment",
  "fields": [
    { "name": "shipmentId", "type": "string" },
    { "name": "carrier", "type": ["null", "string"], "default": null }
  ]
}
```

- [ ] **Step 2: Compile the reader type**

`examples/advanced-consumer/src/schema/reader-types.ts`:

```ts
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import avro from "avsc";

/**
 * The READER schema — the shape this consumer was compiled against. Never the writer schema from
 * the registry: resolution is what lets a producer add a field without breaking us, and reading
 * with the writer's own type would silently adopt whatever it changed.
 */
const DEFINITION = readFileSync(fileURLToPath(new URL("../../schema/shipment-events.avsc", import.meta.url)), "utf8");

const SHIPMENT_EVENT = avro.Type.forSchema(JSON.parse(DEFINITION) as avro.Schema);

/**
 * Pub/Sub passes the fully-qualified schema name (`projects/p/schemas/name`). This service reads one
 * stream, so any schema on its topic resolves to the one reader type; returning undefined would
 * make the codec fail closed, which is what we want for anything unexpected.
 */
export function readerTypeFor(schemaName: string): avro.Type | undefined {
  return schemaName.endsWith("shipment-events") ? SHIPMENT_EVENT : undefined;
}
```

- [ ] **Step 3: Write the worker entry point**

`examples/advanced-consumer/src/main.worker.ts`:

```ts
import "reflect-metadata";
import { NestFactory } from "@nestjs/core";
import { Pool } from "pg";
import { WerkenPubSubTransport } from "@werken/nestjs-google-pubsub";
import { WorkerModule } from "./worker.module.js";
import { PG_POOL } from "./adapters/outbound/pg-projection.js";
import { createPgExecutor } from "./adapters/outbound/pg-executor.js";
import { readerTypeFor } from "./schema/reader-types.js";

async function bootstrap() {
  const app = await NestFactory.createApplicationContext(WorkerModule, { bufferLogs: true });
  const pool = app.get<Pool>(PG_POOL);
  await app.close();

  const transport = new WerkenPubSubTransport({
    projectId: process.env.GCP_PROJECT_ID!,
    subscription: process.env.PUBSUB_SUBSCRIPTION!,
    deadLetterTopic: process.env.PUBSUB_DEAD_LETTER_TOPIC,

    // Resolves the writer schema by revision and decodes into the reader type above. Strict by
    // default: a schema that cannot be read fails the message rather than guessing.
    schemaRegistry: { readerTypeFor },

    // The projection and the dedup marker share one pool, so both are Mode A against one database.
    idempotency: {
      consumer: "shipment-projection",
      executor: () => createPgExecutor(pool),
    },

    validation: { onInvalidEnvelope: "dead-letter", onDecodeFailure: "dead-letter" },
    onUnhandledPattern: "ack",

    flowControl: { maxOutstandingMessages: 100, maxOutstandingBytes: 50 * 1024 * 1024 },
    ackDeadline: { initialMs: 60_000, maxExtensionMs: 600_000 },
    shutdownDrainTimeoutMs: 30_000,

    telemetry: { serviceName: "shipment-projection" },
  });

  const worker = await NestFactory.createMicroservice(WorkerModule, { strategy: transport, bufferLogs: true });
  worker.enableShutdownHooks();
  await worker.listen();
}

void bootstrap();
```

- [ ] **Step 4: Write the provisioning script**

`examples/advanced-consumer/scripts/provision.sh`. Provisions for **both** examples:

```sh
#!/bin/sh
# Provisions everything the advanced consumer and the publisher need, against the local emulator
# and Postgres from docker-compose.yml.
#
# Werken never provisions Pub/Sub resources itself — that belongs in Terraform or your platform
# catalogue, in dev as much as in production. This script exists so the example is runnable, not
# because the library will do it for you.
set -e

: "${PUBSUB_EMULATOR_HOST:=localhost:8085}"
: "${GCP_PROJECT_ID:=werken-dev}"
: "${DATABASE_URL:=postgresql://postgres:postgres@localhost:55432/werken_test}"
export PUBSUB_EMULATOR_HOST GCP_PROJECT_ID DATABASE_URL

node "$(dirname "$0")/provision.mjs"
```

`examples/advanced-consumer/scripts/provision.mjs`:

```js
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { Encodings, PubSub, SchemaTypes } from "@google-cloud/pubsub";
import pg from "pg";

const PROJECT = process.env.GCP_PROJECT_ID;
const SCHEMA_ID = "shipment-events";
const TOPIC = "shipment-events";
const SUBSCRIPTION = "shipment-projection";
const DEAD_LETTER = "shipment-events-dead-letters";

const definition = readFileSync(fileURLToPath(new URL("../schema/shipment-events.avsc", import.meta.url)), "utf8");
const pubsub = new PubSub({ projectId: PROJECT });
const ignoreExists = (error) => {
  if (error.code !== 6) throw error;
};

await pubsub.createSchema(SCHEMA_ID, SchemaTypes.Avro, definition).catch(ignoreExists);
await pubsub
  .createTopic({
    name: TOPIC,
    schemaSettings: { schema: `projects/${PROJECT}/schemas/${SCHEMA_ID}`, encoding: Encodings.Json },
  })
  .catch(ignoreExists);
await pubsub.createTopic(DEAD_LETTER).catch(ignoreExists);
await pubsub.topic(TOPIC).createSubscription(SUBSCRIPTION).catch(ignoreExists);
await pubsub.close();

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
await pool.query(`
  CREATE TABLE IF NOT EXISTS werken_processed_events (
    consumer     text        NOT NULL,
    source       text        NOT NULL,
    event_id     text        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    expires_at   timestamptz NOT NULL,
    PRIMARY KEY (consumer, source, event_id)
  );
  CREATE INDEX IF NOT EXISTS werken_processed_events_expires_at_idx
    ON werken_processed_events (expires_at);
  CREATE TABLE IF NOT EXISTS shipment_projection (
    shipment_id text PRIMARY KEY,
    status      text NOT NULL,
    carrier     text,
    updated_at  timestamptz NOT NULL
  );
`);
await pool.end();

console.log(`provisioned topic=${TOPIC} subscription=${SUBSCRIPTION} dead-letter=${DEAD_LETTER} + Postgres tables`);
```

- [ ] **Step 5: Write the example README**

`examples/advanced-consumer/README.md`:

````markdown
# Advanced consumer

A read-model builder: consumes shipment events and maintains a Postgres projection. Where
[`../minimal-consumer`](../minimal-consumer) shows the smallest possible handler, this one shows the
features a production consumer actually needs.

| Feature                | Why this service needs it                                        |
| ---------------------- | ---------------------------------------------------------------- |
| Avro schema resolution | It has a producer contract, and must survive a field being added |
| Idempotency (Postgres) | It writes to a database; a redelivery would double-apply         |
| Wildcard routing       | It reads a stream, with one exact route outranking the wildcard  |
| Dead-lettering         | A payload with no shipment id will never become processable      |

## Running it

```bash
docker compose up -d                    # from the repo root: emulator + Postgres
pnpm run build                          # from the repo root
pnpm --filter @werken/example-advanced-consumer provision

GCP_PROJECT_ID=werken-dev \
PUBSUB_EMULATOR_HOST=localhost:8085 \
PUBSUB_SUBSCRIPTION=shipment-projection \
PUBSUB_DEAD_LETTER_TOPIC=shipment-events-dead-letters \
DATABASE_URL=postgresql://postgres:postgres@localhost:55432/werken_test \
pnpm --filter @werken/example-advanced-consumer start
```

Then publish some events with [`../publisher`](../publisher) and watch `shipment_projection` fill.

It fails at startup rather than degrading if `DATABASE_URL` is unset or the subscription is missing.
````

- [ ] **Step 6: Build and verify it starts**

```bash
pnpm run clean && pnpm run build
ls examples/advanced-consumer/dist/main.worker.js
docker compose up -d
pnpm --filter @werken/example-advanced-consumer provision
```

Expected: the file exists, and provisioning prints its summary line.

- [ ] **Step 7: Commit**

```bash
git add examples/advanced-consumer
git commit -m "feat(example): wire the advanced consumer's transport and provisioning

Avro reader types, a Mode A executor sharing the projection's pool, validation
policies, and a provisioning script for both examples."
```

---

### Task 6: `publisher` example

**Files:**

- Create: `examples/publisher/package.json`
- Create: `examples/publisher/tsconfig.json`
- Create: `examples/publisher/tsconfig.test.json`
- Create: `examples/publisher/src/main.ts`
- Create: `examples/publisher/README.md`
- Test: `examples/publisher/tests/publish.test.ts`

**Interfaces:**

- Produces: `buildPublisher(client: PubSubClientLike): EventPublisher`, `shipmentEvents(): PublishRequest<ShipmentEvent>[]`.
- Consumes: the Avro schema at `../advanced-consumer/schema/shipment-events.avsc`.

- [ ] **Step 1: Scaffold**

`examples/publisher/package.json`:

```json
{
  "name": "@werken/example-publisher",
  "version": "0.2.0",
  "private": true,
  "type": "module",
  "description": "Worked example: publishing CloudEvents to a schema-attached Pub/Sub topic.",
  "scripts": {
    "build": "tsc --build",
    "start": "node dist/main.js",
    "test": "vitest run"
  },
  "dependencies": {
    "@google-cloud/pubsub": "^5.0.0",
    "@werken/nestjs-google-pubsub": "workspace:*",
    "avsc": "^5.7.9"
  }
}
```

`examples/publisher/tsconfig.json`:

```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": {
    "outDir": "dist",
    "rootDir": "src"
  },
  "include": ["src"],
  "references": [
    {
      "path": "../../packages/nestjs-google-pubsub"
    }
  ]
}
```

`examples/publisher/tsconfig.test.json`:

```json
{
  "extends": "./tsconfig.json",
  "compilerOptions": {
    "composite": false,
    "noEmit": true,
    "rootDir": "."
  },
  "include": ["src", "tests"],
  "references": []
}
```

- [ ] **Step 2: Write the failing test**

`examples/publisher/tests/publish.test.ts`:

```ts
import { describe, expect, test, vi } from "vitest";
import type { PubSubClientLike } from "@werken/nestjs-google-pubsub";
import { buildPublisher, shipmentEvents } from "../src/main.js";

function fakeClient() {
  const published: Array<{ topic: string; data: Buffer; attributes: Record<string, string>; orderingKey?: string }> =
    [];
  const client = {
    topic: vi.fn((topic: string) => ({
      publishMessage: vi.fn(async (m: { data: Buffer; attributes: Record<string, string>; orderingKey?: string }) => {
        published.push({ topic, ...m });
        return `msg-${published.length}`;
      }),
    })),
    subscription: vi.fn(),
    close: vi.fn(async () => {}),
  };
  return { published, client: client as unknown as PubSubClientLike };
}

describe("publisher example", () => {
  test("stamps a CloudEvents envelope on every event", async () => {
    const { published, client } = fakeClient();

    await buildPublisher(client).publishBatch(shipmentEvents());

    expect(published.length).toBeGreaterThan(0);
    for (const message of published) {
      expect(message.attributes["ce-specversion"]).toBe("1.0");
      expect(message.attributes["ce-type"]).toMatch(/^com\.example\.shipment\./);
      expect(message.attributes["ce-id"]).toBeTruthy();
    }
  });

  // Ordering is off unless asked for. With it on, `subject` becomes the key, which is what keeps
  // two events for one shipment in order.
  test("derives the ordering key from subject", async () => {
    const { published, client } = fakeClient();

    await buildPublisher(client).publishBatch(shipmentEvents());

    expect(published[0].orderingKey).toBe(published[0].attributes["ce-subject"]);
  });

  // Pub/Sub's JSON encoding is Avro JSON: a nullable union is {"string":"dhl"}, not "dhl". Plain
  // JSON is rejected outright by a schema-attached topic.
  test("encodes the body as Avro JSON, not plain JSON", async () => {
    const { published, client } = fakeClient();

    await buildPublisher(client).publishBatch(shipmentEvents());

    const body = JSON.parse(published[0].data.toString("utf8")) as { carrier?: unknown };
    expect(body.carrier).toEqual({ string: "dhl" });
  });
});
```

- [ ] **Step 3: Run it and watch it fail**

```bash
pnpm exec vitest run examples/publisher/tests/
```

Expected: FAIL — cannot resolve `../src/main.js`.

- [ ] **Step 4: Write the publisher**

`examples/publisher/src/main.ts`:

```ts
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { PubSub } from "@google-cloud/pubsub";
import avro from "avsc";
import { PartialPublishError, createEventPublisher } from "@werken/nestjs-google-pubsub";
import type { EventPublisher, PubSubClientLike, PublishRequest } from "@werken/nestjs-google-pubsub";

export interface ShipmentEvent {
  readonly shipmentId: string;
  readonly carrier: string | null;
}

const DEFINITION = readFileSync(
  fileURLToPath(new URL("../../advanced-consumer/schema/shipment-events.avsc", import.meta.url)),
  "utf8",
);
const WRITER = avro.Type.forSchema(JSON.parse(DEFINITION) as avro.Schema);

const TOPICS: Record<string, string> = {
  "com.example.shipment.ready.v1": "shipment-events",
  "com.example.shipment.cancelled.v1": "shipment-events",
};

export function buildPublisher(client: PubSubClientLike): EventPublisher {
  return createEventPublisher({
    source: "https://example.com/shipping",
    client,
    topicResolver: (type) => TOPICS[type],

    /**
     * Avro JSON, which is what Pub/Sub's `JSON` encoding means — a nullable union is
     * {"string":"dhl"} and not "dhl", and plain JSON is rejected outright by a schema-attached
     * topic. Returning bare bytes declares `application/json`, which is correct here; return
     * `{ data, datacontenttype }` instead when the bytes are genuinely something else.
     */
    encode: (_type, data) => Buffer.from(WRITER.toString(data)),

    // Off unless asked for. With it on, `subject` becomes the ordering key, so two events for one
    // shipment stay in order — and the Topic is built with messageOrdering, which the SDK requires.
    ordering: true,
  });
}

export function shipmentEvents(): Array<PublishRequest<ShipmentEvent>> {
  return [
    { type: "com.example.shipment.ready.v1", data: { shipmentId: "s-1", carrier: "dhl" }, subject: "s-1" },
    { type: "com.example.shipment.ready.v1", data: { shipmentId: "s-2", carrier: "ups" }, subject: "s-2" },
    { type: "com.example.shipment.cancelled.v1", data: { shipmentId: "s-1", carrier: null }, subject: "s-1" },
  ];
}

async function main() {
  const client = new PubSub({ projectId: process.env.GCP_PROJECT_ID! }) as unknown as PubSubClientLike;
  try {
    const ids = await buildPublisher(client).publishBatch(shipmentEvents());
    console.log(`published ${ids.length} events: ${ids.join(", ")}`);
  } catch (error) {
    // Pub/Sub has no multi-message transaction, so a partly-failed batch leaves the successes
    // published and impossible to unsend. Retrying the whole batch would duplicate them.
    if (error instanceof PartialPublishError) {
      console.error(`published ${error.published.length}, failed ${error.failures.length} — retry only the failures`);
      for (const failure of error.failures)
        console.error(`  [${failure.index}] ${failure.type}: ${String(failure.cause)}`);
    }
    throw error;
  } finally {
    await client.close();
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) void main();
```

- [ ] **Step 5: Write the README**

`examples/publisher/README.md`:

````markdown
# Publisher

Emits the shipment events [`../advanced-consumer`](../advanced-consumer) reads. Run that first, then
this, and watch `shipment_projection` fill.

Covers `createEventPublisher`, a `topicResolver`, an Avro-JSON `encode`, `ordering: true` deriving
keys from `subject`, `publishBatch`, and recovering from `PartialPublishError`.

```bash
pnpm --filter @werken/example-advanced-consumer provision   # once
GCP_PROJECT_ID=werken-dev PUBSUB_EMULATOR_HOST=localhost:8085 \
  pnpm --filter @werken/example-publisher start
```

Note the encoder returns bare bytes, which declares `application/json` — correct, because Pub/Sub's
`JSON` schema encoding _is_ Avro JSON. Return `{ data, datacontenttype }` when the payload is
genuinely another format, such as protobuf.
````

- [ ] **Step 6: Run the tests**

```bash
pnpm exec vitest run examples/publisher/tests/
```

Expected: 3 passed.

- [ ] **Step 7: Commit**

```bash
git add examples/publisher
git commit -m "feat(example): add a publisher example

Covers createEventPublisher, Avro-JSON encode, ordering keys from subject,
publishBatch and PartialPublishError — none of which had a runnable reference."
```

---

### Task 7: End-to-end integration test

**Files:**

- Create: `examples/advanced-consumer/tests/integration/projection.integration.test.ts`

**Interfaces:**

- Consumes: `skipUnlessAvailable` from `@werken/test-support` (Task 2), the built `dist` of both examples, the provisioning script.

- [ ] **Step 1: Write the test**

```ts
import { execFile } from "node:child_process";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { skipUnlessAvailable } from "@werken/test-support";

const run = promisify(execFile);
const EMULATOR = process.env.PUBSUB_EMULATOR_HOST;
const DATABASE_URL = process.env.DATABASE_URL;
const CONSUMER = fileURLToPath(new URL("../../dist/main.worker.js", import.meta.url));
const PUBLISHER = fileURLToPath(new URL("../../../publisher/dist/main.js", import.meta.url));
const PROVISION = fileURLToPath(new URL("../../scripts/provision.mjs", import.meta.url));

/**
 * The advanced example's whole point is that its features are real: a schema-attached topic, a
 * Postgres idempotency store and a projection. None of that can be asserted without both backends,
 * so this is the only place it is covered.
 */
describe.skipIf(
  skipUnlessAvailable("PUBSUB_EMULATOR_HOST", EMULATOR) ||
    skipUnlessAvailable("DATABASE_URL", DATABASE_URL) ||
    skipUnlessAvailable("a built advanced-consumer (run `pnpm run build`)", existsSync(CONSUMER)) ||
    skipUnlessAvailable("a built publisher (run `pnpm run build`)", existsSync(PUBLISHER)),
)("advanced consumer projects published events", () => {
  const env = { ...process.env, GCP_PROJECT_ID: process.env.PUBSUB_PROJECT_ID ?? "werken-dev" };
  const pool = new pg.Pool({ connectionString: DATABASE_URL });

  beforeAll(async () => {
    await run(process.execPath, [PROVISION], { env });
    await pool.query("DELETE FROM shipment_projection");
    await pool.query("DELETE FROM werken_processed_events");
  }, 60_000);

  afterAll(async () => {
    await pool.end();
  });

  test("decodes Avro, projects rows, and suppresses a redelivery", async () => {
    const { spawn } = await import("node:child_process");
    const worker = spawn(process.execPath, [CONSUMER], {
      env: {
        ...env,
        PUBSUB_SUBSCRIPTION: "shipment-projection",
        PUBSUB_DEAD_LETTER_TOPIC: "shipment-events-dead-letters",
        DATABASE_URL,
      },
      stdio: ["ignore", "pipe", "pipe"],
    });

    try {
      await run(process.execPath, [PUBLISHER], { env });

      const deadline = Date.now() + 60_000;
      let rows: Array<{ shipment_id: string; status: string }> = [];
      while (Date.now() < deadline) {
        rows = (await pool.query("SELECT shipment_id, status FROM shipment_projection ORDER BY shipment_id")).rows;
        if (rows.length >= 2) break;
        await new Promise((r) => setTimeout(r, 250));
      }

      expect(rows.map((r) => r.shipment_id)).toEqual(["s-1", "s-2"]);
      // s-1 was published ready then cancelled, and ordering keeps them in that order.
      expect(rows.find((r) => r.shipment_id === "s-1")?.status).toBe("cancelled");

      // The dedup markers prove the SQL store was actually written, not bypassed.
      const markers = await pool.query("SELECT count(*)::int AS n FROM werken_processed_events");
      expect(markers.rows[0].n).toBe(3);
    } finally {
      worker.kill("SIGTERM");
    }
  }, 120_000);
});
```

- [ ] **Step 2: Run it**

```bash
docker compose up -d
pnpm run build
pnpm run test:integration
```

Expected: 10 files passed. If the projection assertion fails on ordering, check that the publisher sets `ordering: true` — without it `subject` is not used as a key.

- [ ] **Step 3: Commit**

```bash
git add examples/advanced-consumer/tests
git commit -m "test(example): cover the advanced consumer end to end

Provisions a schema-attached topic and Postgres, runs the publisher and the
consumer for real, and asserts both the projection and the dedup markers."
```

---

### Task 8: Wire the new examples into the repository gates

**Files:**

- Modify: `tsconfig.json`
- Modify: `knip.ts`
- Modify: `README.md`
- Modify: `CONTRIBUTING.md`

- [ ] **Step 1: Add project references**

In `tsconfig.json`, add after the existing example reference:

```json
    {
      "path": "examples/advanced-consumer"
    },
    {
      "path": "examples/publisher"
    }
```

- [ ] **Step 2: Add knip workspaces**

In `knip.ts`, alongside `examples/minimal-consumer`:

```ts
    "examples/advanced-consumer": {
      // Spawned by the integration test rather than imported.
      entry: ["src/main.worker.ts", "scripts/provision.mjs"],
      project: ["src/**/*.ts", "tests/**/*.ts"],
    },

    "examples/publisher": {
      entry: ["src/main.ts"],
      project: ["src/**/*.ts", "tests/**/*.ts"],
    },
```

- [ ] **Step 3: Point the README at both examples**

Replace the line `A complete, runnable service is in [`examples/minimal-consumer`](examples/minimal-consumer).` with:

```markdown
Two runnable examples:

- **[`examples/minimal-consumer`](examples/minimal-consumer)** — the smallest possible consumer. A
  12-line handler with business logic only.
- **[`examples/advanced-consumer`](examples/advanced-consumer)** — what a production consumer needs:
  Avro schema resolution, Postgres idempotency, wildcard routing and dead-lettering. Paired with
  **[`examples/publisher`](examples/publisher)** for the writing side.
```

Also update the Documentation table's "Worked example" row to list both.

- [ ] **Step 4: Note the extra backend in CONTRIBUTING**

In the commands table, add:

```markdown
| `pnpm run test:integration` | the integration suite, including both examples | `docker compose up -d` |
```

- [ ] **Step 5: Run every gate**

```bash
pnpm run clean && pnpm run build
pnpm test
pnpm run typecheck
pnpm run lint && pnpm run format:check
pnpm run lint:neutrality && pnpm run lint:no-deep-imports
pnpm run lint:dead-code
pnpm run test:integration
```

Expected: all pass. `lint:dead-code` is the one most likely to complain — if it reports the new examples' entry points as unused files, the `knip.ts` entries in Step 2 are wrong.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "docs: wire the new examples into the build, gates and README"
```

---

## Self-Review

**Spec coverage.** Every spec section maps to a task: shape → 3, 5, 6; distribution safety → 1; provisioning → 5; verification → 3, 4, 6, 7; repository changes → 2, 8. Non-goals are respected — no rename, no media-type topic, no snippet checks.

**Placeholders.** None. Every code step carries real content; the one forward reference (`PgShipmentProjection` in Task 3) ships an explicit stub so the task stands alone, and Task 4 Step 4 replaces it.

**Type consistency.** `ShipmentProjection.apply(change: ProjectionChange)` is used identically in Tasks 3, 4 and the harness test. `createPgExecutor(pool: Pool): SqlExecutor` matches its call in Task 5. `PG_POOL` is declared in `pg-projection.ts` and imported from there in both `worker.module.ts` and `main.worker.ts`. `readerTypeFor` matches `SchemaRegistryOptions.readerTypeFor`. `buildPublisher`/`shipmentEvents` match between Task 6's test and implementation.

**Known risk, called out rather than hidden.** Task 5's `main.worker.ts` builds a throwaway application context purely to obtain the pool, then closes it. If that proves awkward in practice, construct the `Pool` directly in `main.worker.ts` and pass it into `WorkerModule.forRoot(pool)` — a dynamic module. The plan takes the simpler route first; switch if the context dance causes trouble.
