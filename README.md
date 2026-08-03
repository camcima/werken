<div align="center">

<picture>
  <img alt="werken" src="assets/logo.svg" width="340">
</picture>

<br>

[![CI](https://github.com/camcima/werken/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/camcima/werken/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/camcima/werken/graph/badge.svg)](https://codecov.io/gh/camcima/werken)
[![npm version](https://img.shields.io/npm/v/@werken/nestjs-google-pubsub)](https://www.npmjs.com/package/@werken/nestjs-google-pubsub)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.9%2B-blue.svg)](https://www.typescriptlang.org/)
[![Node.js](https://img.shields.io/badge/Node.js-22%20%7C%2024-green.svg)](https://nodejs.org/)

</div>

A NestJS transport for Google Cloud Pub/Sub that speaks [CloudEvents 1.0](https://cloudevents.io/).
Write an event consumer as an ordinary Nest controller and get schema resolution, envelope
validation, idempotency, dead-lettering, distributed tracing and clean shutdown without writing any
of them yourself.

> [!IMPORTANT]
> **Your producers must emit CloudEvents.** Werken routes on the `ce-type` message attribute, so a
> Pub/Sub message that arrives without a CloudEvents 1.0 envelope cannot be handled at all — it is
> dead-lettered rather than delivered. In practice that means four Pub/Sub attributes; your payload
> itself is untouched and can be anything. See [Event requirements](#event-requirements).

_Werken_ is Mapudungun for **messenger** — the emissary who carries word between communities.

```ts
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
      throw new TerminalEventError(`unknown shipment ${data.shipmentId}`);
    }
    await this.dispatch.execute({ shipment, carrier: data.carrier, occurredAt: ctx.time });
  }
}
```

Returning acks. Throwing nacks. `TerminalEventError` dead-letters. Everything else — decoding,
de-duplication, tracing, draining — happens around the handler.

## Features

- **CloudEvents 1.0 on the wire** — binary content mode, so any CloudEvents consumer in any language
  can read your events. No framework envelope wrapped around the payload.
- **Outcome by return value** — `return` to ack, `throw` to nack and retry, `TerminalEventError` to
  dead-letter immediately without burning the retry budget.
- **Explicit dead-lettering** — publishes to a topic you configure, preserving the original body and
  attributes and adding provenance (reason, stage, source subscription, timestamp, structured
  detail and the original ordering key). Not the
  subscription's retry policy, which only triggers after every retry is exhausted.
- **Avro schema resolution** — fetches the _writer_ schema by revision from the Pub/Sub Schema
  Service and resolves it against your compiled _reader_ type. Cached by revision id, single-flight,
  LRU- and TTL-bounded, fails closed.
- **Idempotency** — a pluggable store with a driver-free Postgres implementation, plus documented
  adapters for pg, Kysely, Drizzle, Prisma, TypeORM, Redis and MongoDB.
- **Wildcard routing** — exact, suffix wildcard and catch-all patterns with deterministic precedence.
  Ambiguous or unsupported patterns fail at startup, never silently.
- **OpenTelemetry** — one `CONSUMER` span per message continuing the producer's trace, child spans
  for decode and handler, and seven metrics including event lateness. Optional peer dependency;
  degrades to a no-op when absent.
- **Graceful drain** — on `SIGTERM`, stops taking work, waits out in-flight handlers, then nacks the
  remainder so they are redelivered promptly rather than waiting for the ack deadline to lapse.
- **Testable without GCP** — an in-memory harness with real Nest DI. No broker, no credentials, no
  emulator.
- **Dual ESM + CJS** — every entry point importable from both.

## Packages

| Package                                                                             | Description                                                                                                                  |
| ----------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| [`@werken/cloudevents`](packages/cloudevents)                                       | CloudEvents 1.0 envelope types, validation and Pub/Sub attribute binding. Zero runtime dependencies, no GCP or Nest imports. |
| [`@werken/nestjs-google-pubsub`](packages/nestjs-google-pubsub)                     | The transport, publisher, Avro codec and idempotency.                                                                        |
| [`@werken/nestjs-google-pubsub/testing`](packages/nestjs-google-pubsub/src/testing) | In-memory test harness. A subpath rather than a package, so it can never version-skew from the transport it wraps.           |

## Event requirements

Werken consumes **CloudEvents 1.0 in binary content mode**: the envelope travels in Pub/Sub message
attributes and the body is your payload, untouched. Every message must carry four attributes:

| Attribute        | Notes                                                                 |
| ---------------- | --------------------------------------------------------------------- |
| `ce-specversion` | Must be exactly `1.0`. Any other value, including `0.3`, is rejected. |
| `ce-id`          | Unique within the source. Half of the idempotency key.                |
| `ce-source`      | The producing system, as a URI-reference. The other half of that key. |
| `ce-type`        | The routing key — what `@EventPattern` matches on.                    |

Everything else is optional and degrades sensibly: `ce-datacontenttype` defaults to
`application/json`, `ce-time` falls back to the Pub/Sub publish time, and any unrecognised `ce-*`
attribute is preserved verbatim on `ctx.extensions` rather than dropped.

**Your payload is not constrained.** Werken parses the body as JSON, or decodes it as Avro when you
configure `schemaRegistry` — and with your own `encode`, it can be any format you like, declared
honestly: return `{ data, datacontenttype }` and the event says what the bytes actually are.
Producers do not restructure their events to adopt this; they set four attributes alongside them.

### If a message arrives without an envelope

It is never delivered to a handler. `validation.onInvalidEnvelope` decides its fate — `dead-letter`
(the default), `nack`, or `ack` to drop it — and that is the whole range of outcomes. There is no
hook to substitute a different envelope format: parsing runs before routing, decode and everything
else.

This is deliberate rather than incidental. Routing has no key without `ce-type`, and duplicate
suppression has nothing to deduplicate by without `ce-source` and `ce-id`.

### Producers you do not control

In increasing order of cost:

- **Have them set the four attributes.** Much the cheapest option — no payload change, and no need
  to take a dependency on this library to do it.
- **Put a transform in front**, consuming from their topic and republishing with the envelope
  attached.
- **Use [`@werken/cloudevents`](packages/cloudevents) directly** and drive the Pub/Sub SDK yourself.
  It is standalone, with zero dependencies and no GCP or Nest imports.

If your events are not CloudEvents and are not going to become CloudEvents, then Werken is the wrong
tool and there is a better one:
[`nestjs-google-pubsub-microservice`](https://github.com/p-fedyukovich/nestjs-google-pubsub-microservice)
is a community Nest transport for Pub/Sub, MIT-licensed, that takes no position on the wire format.
What you give up is what the envelope pays for — routing on `ce-type`, idempotency keyed on source
and id, and a trace that continues from the producer.

## Installation

```bash
pnpm add @werken/nestjs-google-pubsub @google-cloud/pubsub
# optional, for tracing and metrics
pnpm add @opentelemetry/api
```

## Quick start

```ts
// main.worker.ts — Cloud Run worker pools have no HTTP endpoint,
// so this is a microservice, not an HTTP app.
const app = await NestFactory.createMicroservice(WorkerModule, {
  strategy: new WerkenPubSubTransport({
    projectId: process.env.GCP_PROJECT_ID!,
    subscription: process.env.PUBSUB_SUBSCRIPTION!,
    deadLetterTopic: process.env.PUBSUB_DEAD_LETTER_TOPIC,
    idempotency: { consumer: "shipment-dispatch" },
    telemetry: { serviceName: "shipment-dispatch" },
  }),
  bufferLogs: true,
});

// Without this, Nest never calls the transport's close(), so scale-down kills
// in-flight handlers and every interrupted message is reprocessed.
app.enableShutdownHooks();

await app.listen();
```

A complete, runnable service is in [`examples/minimal-consumer`](examples/minimal-consumer).

## How it differs from Nest's own abstraction

Werken is a `CustomTransportStrategy`, so it lives inside Nest's microservices abstraction rather
than replacing it — you keep controllers, DI, `@EventPattern`, guards, interceptors and pipes. What
changes is the wire format and the delivery contract.

|                    | Nest default                          | Werken                      |
| ------------------ | ------------------------------------- | --------------------------- |
| Wire format        | `{ pattern, data }` envelope          | CloudEvents 1.0 binary mode |
| Routing key        | inside the payload                    | `ce-type` attribute         |
| Readable by        | Nest only                             | any CloudEvents consumer    |
| Handler throws     | logged, message acked                 | nacked and redelivered      |
| Error identity     | replaced with `Internal server error` | preserved                   |
| Duplicate patterns | chained, only the first runs          | fails at startup            |

Two of those rows are behaviours of the default abstraction that will silently lose messages in a
naive transport, and both took real work to defeat:

- **Handlers resolve to Observables, and awaiting one is a no-op** — so a transport written the
  obvious way acks a throwing handler and loses the message.
- **`RpcExceptionsHandler` replaces any non-`RpcException` error** with
  `{ status: 'error', message: 'Internal server error' }` before the transport sees it. That is why
  `TerminalEventError` extends `RpcException`.

**→ [The full comparison, with the source that proves each claim](docs/vs-nestjs-microservices.md)**

## Message pipeline

Every message passes through these stages. Any stage can terminate it with an outcome.

```
receive
  → parse envelope        invalid → validation.onInvalidEnvelope (default dead-letter)
  → resolve handler       none    → onUnhandledPattern (default ack)
  → open telemetry span   parented on ce-traceparent
  → idempotency check     already processed → ack, count as skipped duplicate
  → decode payload        failure → validation.onDecodeFailure (default dead-letter)
  → invoke handler        lease extension running
  → record idempotency
  → outcome               ack | nack | dead-letter
```

Two orderings in there are deliberate and worth knowing:

- **The idempotency check precedes decode.** A duplicate should not pay the decode cost, and a
  message already processed successfully must still be acked even if its schema has since become
  unreadable.
- **Idempotency is recorded after the handler succeeds and before the ack.** Recording earlier risks
  silently swallowing a message whose handler then failed; recording after the ack risks a crash in
  between. Neither is eliminable — this is at-least-once — which is exactly why handlers must remain
  idempotent regardless.

### What a dead-lettered message carries

The original body and attributes, untouched, plus provenance:

| Attribute                       | Value                                                       |
| ------------------------------- | ----------------------------------------------------------- |
| `werken-dl-reason`              | Why it was terminal                                         |
| `werken-dl-stage`               | `envelope`, `decode`, `handler` or `unhandled`              |
| `werken-dl-source-subscription` | The subscription it was read from, prefix already resolved  |
| `werken-dl-timestamp`           | When it was dead-lettered, ISO 8601                         |
| `werken-dl-detail`              | JSON of the `detail` a `TerminalEventError` carried, if any |
| `werken-dl-ordering-key`        | The original ordering key, if it had one                    |

`throw new TerminalEventError("unknown shipment", { shipmentId })` puts `{"shipmentId":"..."}` in
`werken-dl-detail`, so the context you attached is there when you come to triage it.

Two limits worth knowing. Pub/Sub caps an attribute at 1024 bytes, so detail larger than that is
replaced by `{"truncated":true,"bytes":N}` — truncating JSON mid-string would leave something
nothing can parse. Detail that cannot be serialised at all becomes `{"unserialisable":true,...}`.
Neither ever fails the publish: losing the message is worse than losing its diagnostics.

The ordering key is carried as **provenance, not as a live ordering key**. Republishing under a real
one would require the dead-letter topic to be built with `messageOrdering` and would serialise
dead-letter publishes per key — a slow path made slower exactly when things are already wrong.
Redrive tooling reads the attribute and restores order itself.

## Documentation

| Guide                                                            | What it covers                                          |
| ---------------------------------------------------------------- | ------------------------------------------------------- |
| [Migrating an existing consumer](docs/migration.md)              | What to delete, and the one behaviour change that bites |
| [Werken vs. Nest microservices](docs/vs-nestjs-microservices.md) | Every difference, with the source that proves it        |
| [Idempotency schema](docs/idempotency-schema.sql)                | DDL for your own migration pipeline                     |
| [Worked example](examples/minimal-consumer)                      | A 12-line handler with tests that need no GCP           |

---

## Idempotency

Pub/Sub is **at-least-once**. Even with exactly-once _delivery_ enabled, exactly-once _processing_
is not something a broker can give you, so Werken records which events it has already handled.

> **Handlers must remain idempotent regardless.** The store narrows the window; it does not close
> it. See [what the store does and does not guarantee](#what-the-store-does-and-does-not-guarantee).

### The port

Everything plugs in through one interface:

```ts
interface IdempotencyStore {
  tryRecord(key: IdempotencyKey, ttlMs: number, ctx: CloudEventContext): Promise<boolean>;
  has(key: IdempotencyKey, ctx: CloudEventContext): Promise<boolean>;
}
```

For Postgres you do not implement that. Use the built-in SQL store, which has **no database driver
dependency** — it needs exactly one operation:

```ts
interface SqlExecutor {
  execute(sql: string, params: readonly unknown[]): Promise<{ rowCount: number }>;
}
```

`rowCount` is **the number of rows the statement returned**. Both statements the store issues end in
`RETURNING`, so that is the only thing an adapter ever has to report — for most drivers,
`rows.length`.

> **This is the one thing to get right.** The failure is silent, not loud, and it goes both ways:
>
> - An adapter that always returns `0` makes `has` report every message as new, so **de-duplication
>   is off** — every duplicate is reprocessed. `tryRecord` then reports every write as already
>   present, so each message also logs a spurious duplicate warning.
> - An adapter that always returns a **positive** count is the dangerous one: `has` reports every
>   message as already processed, so every message is acked **without the handler ever running**.
>
> The snippets below were checked against each driver's actual type definitions, not written from
> memory.

### Schema

Werken never runs DDL. Copy [`docs/idempotency-schema.sql`](docs/idempotency-schema.sql) into your
own migration pipeline. Pruning expired rows is also yours to schedule — `pruneExpiredSql(table?)`
returns the statement.

### Wiring it up

```ts
new WerkenPubSubTransport({
  projectId: process.env.GCP_PROJECT_ID!,
  subscription: process.env.PUBSUB_SUBSCRIPTION!,
  idempotency: {
    consumer: "order-projections", // identifies THIS consumer in the key
    executor: () => myExecutor, // resolved per message — see below
    table: "werken_processed_events", // optional
  },
});
```

`consumer` is part of the key on purpose: two services consuming the same event must each process
it once, not once between them.

The executor is resolved **per message**, which is what lets you move from Mode A to Mode B later
without changing anything else.

---

## SQL adapters

Five lines each. Every snippet is complete — imports and construction included.

### node-postgres (`pg`)

`QueryResult.rowCount` is typed `number | null`, so the `?? 0` is load-bearing.

```ts
import { Pool } from "pg";
import type { SqlExecutor } from "@werken/nestjs-google-pubsub";

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const executor: SqlExecutor = {
  execute: async (sql, params) => {
    const result = await pool.query(sql, params as unknown[]);
    return { rowCount: result.rowCount ?? 0 };
  },
};
```

### Kysely

**Do not use `numAffectedRows` here.** Kysely's own typings say it is "defined for insert, update,
delete and merge queries" — it is `undefined` for the `SELECT` that `has()` issues, so
`Number(numAffectedRows ?? 0)` would silently disable de-duplication. `rows.length` is correct for
both statements, because both `RETURN`.

```ts
import { CompiledQuery, Kysely, PostgresDialect } from "kysely";
import type { SqlExecutor } from "@werken/nestjs-google-pubsub";

const db = new Kysely<never>({ dialect: new PostgresDialect({ pool }) });

const executor: SqlExecutor = {
  execute: async (sql, params) => {
    const result = await db.executeQuery(CompiledQuery.raw(sql, params as unknown[]));
    return { rowCount: result.rows.length };
  },
};
```

### Drizzle

**`sql.raw()` takes a string and nothing else** — it cannot bind `$1`, so building the statement
with it would force string interpolation. Go through `db.$client`, which is the underlying driver,
and note the two drivers return genuinely different shapes.

With **node-postgres** (`$client` is a `pg` `Pool`):

```ts
import { drizzle } from "drizzle-orm/node-postgres";
import type { SqlExecutor } from "@werken/nestjs-google-pubsub";

const db = drizzle(process.env.DATABASE_URL!);

const executor: SqlExecutor = {
  execute: async (sql, params) => {
    const result = await db.$client.query(sql, params as unknown[]);
    return { rowCount: result.rowCount ?? 0 };
  },
};
```

With **postgres-js** (`$client` is a `postgres` instance; the result is a `RowList`, which is an
array that also carries `.count`):

```ts
import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import type { SqlExecutor } from "@werken/nestjs-google-pubsub";

const db = drizzle(postgres(process.env.DATABASE_URL!));

const executor: SqlExecutor = {
  execute: async (sql, params) => {
    const rows = await db.$client.unsafe(sql, params as never[]);
    return { rowCount: rows.count };
  },
};
```

> `.count` and `.length` agree here only because both statements `RETURN` rows. For an `INSERT`
> without `RETURNING`, postgres-js gives `.length === 0` but a correct `.count` — which is exactly
> how this goes subtly wrong if the SQL is ever changed.

### Prisma

**Use `$queryRawUnsafe`, not `$executeRawUnsafe`.** `$executeRawUnsafe` returns an affected-row
count but cannot run the `SELECT` that `has()` issues. Since both statements `RETURN` rows,
`$queryRawUnsafe` handles both.

```ts
import { PrismaClient } from "@prisma/client";
import type { SqlExecutor } from "@werken/nestjs-google-pubsub";

const prisma = new PrismaClient();

const executor: SqlExecutor = {
  execute: async (sql, params) => {
    const rows = await prisma.$queryRawUnsafe<unknown[]>(sql, ...params);
    return { rowCount: rows.length };
  },
};
```

### TypeORM

`dataSource.query()` returns **raw rows, not a count** — there is no `affected` property on it. With
`RETURNING` on both statements, `rows.length` is the answer. (`QueryRunner.query(sql, params, true)`
returns a structured result with `affected`, but you do not need it here.)

```ts
import { DataSource } from "typeorm";
import type { SqlExecutor } from "@werken/nestjs-google-pubsub";

const dataSource = await new DataSource({ type: "postgres", url: process.env.DATABASE_URL }).initialize();

const executor: SqlExecutor = {
  execute: async (sql, params) => {
    const rows = await dataSource.query<unknown[]>(sql, params as unknown[]);
    return { rowCount: rows.length };
  },
};
```

---

## Non-SQL stores

Redis and MongoDB **do not use `SqlExecutor`**. There is no SQL to execute, so implement
`IdempotencyStore` directly — both methods, `tryRecord` and `has`.

### Redis — `SET NX` with a TTL

`SET key value NX PX ttl` returns `"OK"` when it set the key and `null` when it already existed,
which is precisely `tryRecord`'s contract. The TTL is on the key itself, so expiry needs no pruning
job.

```ts
import { createClient } from "redis";
import { idempotencyKeyToString } from "@werken/nestjs-google-pubsub";
import type { IdempotencyStore } from "@werken/nestjs-google-pubsub";

const redis = createClient({ url: process.env.REDIS_URL });
await redis.connect();

const store: IdempotencyStore = {
  tryRecord: async (key, ttlMs) =>
    (await redis.set(`werken:${idempotencyKeyToString(key)}`, "1", { NX: true, PX: ttlMs })) === "OK",
  has: async (key) => (await redis.exists(`werken:${idempotencyKeyToString(key)}`)) === 1,
};
```

Redis can never support Mode B: there is no shared transaction with your business write.

### MongoDB — unique index and duplicate-key errors

Insert into a collection with a unique index on the key and treat error code `11000` (duplicate key)
as "already processed". A TTL index on `expiresAt` handles expiry, so again no pruning job.

```ts
// One-off, in your migrations:
//   db.werken_processed_events.createIndex({ key: 1 }, { unique: true })
//   db.werken_processed_events.createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 })
import { MongoClient } from "mongodb";
import { idempotencyKeyToString } from "@werken/nestjs-google-pubsub";
import type { IdempotencyStore } from "@werken/nestjs-google-pubsub";

const collection = (await new MongoClient(process.env.MONGO_URL!).connect()).db().collection("werken_processed_events");

const store: IdempotencyStore = {
  tryRecord: async (key, ttlMs) => {
    try {
      await collection.insertOne({
        key: idempotencyKeyToString(key),
        expiresAt: new Date(Date.now() + ttlMs),
      });
      return true;
    } catch (error) {
      if ((error as { code?: number }).code === 11000) return false; // duplicate key
      throw error;
    }
  },
  has: async (key) => (await collection.countDocuments({ key: idempotencyKeyToString(key) }, { limit: 1 })) > 0,
};
```

Unlike Redis, MongoDB _can_ reach Mode B through a session/transaction.

Pass either one as `idempotency: { consumer, store }`. `store` and `executor` are mutually
exclusive — supplying both throws at startup rather than silently picking one.

---

## What the store does and does not guarantee

### Mode A — best-effort de-duplication (what you get today)

Return a pool-backed executor. The dedup insert runs in **its own transaction**, separate from
whatever your handler wrote.

**Mode A does not eliminate duplicate processing.** The pipeline records the marker after your
handler succeeds and before the ack. If the process dies in that window, the side effect has
happened but the marker has not — and the redelivery runs your handler again.

Recording earlier would be worse: a marker written before the handler would silently swallow a
message whose handler then failed. Neither ordering is safe, because at-least-once delivery does not
admit a safe one.

### Mode B — transactional inbox (later)

Return an executor scoped to the transaction your handler is about to use, so the dedup insert
commits atomically with the business write. Same `SqlExecutor`, same config shape — only what your
`executor` callback hands back changes.

> Mode B is not wired up yet. The port is shaped for it; the pipeline still calls `tryRecord` after
> the handler returns, which means the handler's transaction must still be open at that point for
> the two writes to be atomic. That sequencing is unresolved.

### Therefore

**Handlers must be idempotent regardless of which mode you use, and regardless of whether
exactly-once delivery is enabled on the subscription.** Exactly-once delivery is a guarantee about
_delivery_, not about _processing_. Werken narrows the duplicate window; your handler closes it.

---

## Routing

`@EventPattern` matches on the CloudEvents `ce-type`, in three shapes:

| Pattern                       | Matches                                                                                      |
| ----------------------------- | -------------------------------------------------------------------------------------------- |
| `com.example.order.placed.v1` | exactly that type                                                                            |
| `com.example.*`               | one or more trailing segments — `com.example.a` and `com.example.a.b`, but not `com.example` |
| `*`                           | anything                                                                                     |

**Exactly one handler runs per message.** Exact beats wildcard; among wildcards the longest literal
prefix wins; the catch-all is last. Precedence does not depend on registration order.

Two failures are deliberately made loud at startup rather than left to production:

- **Two handlers for one pattern.** Nest chains duplicate event handlers, so the second would
  register successfully and then never run. Werken refuses to start instead.
- **A wildcard anywhere but the final segment.** `com.*.thing` is rejected, because a pattern that
  silently never matches is much harder to notice than a boot error.

---

## Schemas

Werken resolves the **writer** schema by revision from the Pub/Sub Schema Service and decodes it
into the compiled **reader** type your service imports:

```ts
schemaRegistry: {
  readerTypeFor: (schemaName) => readerTypes[schemaName],
  strict: true, // default — fail closed rather than guess
}
```

Behaviour that matters:

- **Cached by revision id, never by schema name.** Producers move between revisions independently, so
  a name-keyed cache would decode new bytes against a stale writer schema — silent corruption rather
  than a loud failure.
- **Single-flight.** Concurrent misses on one revision make a single Schema Service call, not one per
  in-flight message. Failures are not cached, so a transient outage cannot pin a revision.
- **Bounded.** LRU plus TTL, so corrections get picked up.
- **An unknown revision is normal**, not an error — a producer rolling out ahead of its consumers is
  the expected steady state. Logged at debug.
- **`strict` covers one failure, not all of them.** It decides what happens when the writer schema
  cannot be _fetched_ — a Schema Service outage, a client without schema support, a revision that has
  not propagated. `strict: false` decodes the body as plain JSON in that case, trading correctness
  for availability. It does **not** loosen anything else: a missing reader type, a definition that is
  not valid Avro, and a writer the reader cannot resolve stay fatal whatever `strict` says, because
  there the schema is known and the message still cannot be read correctly.
- **Schema metadata is all-or-nothing.** Pub/Sub sets the schema name, revision and encoding
  together or sets none of them, so a partial set is rejected rather than treated as an
  unschematised topic, and an encoding that is neither `JSON` nor `BINARY` is rejected rather than
  guessed at.

> ⚠️ **Pub/Sub's `JSON` encoding is Avro JSON, not plain JSON.** A nullable union is
> `{"string":"SCL"}`, not `"SCL"`, and plain JSON is _rejected_ outright by a schema-attached topic.
> Prefer non-null fields with defaults over nullable unions: it keeps payloads readable and evolves
> more cleanly. This was verified empirically — the integration tests publish both forms.

---

## Sharing a development project

Pub/Sub delivers each message to **exactly one** subscriber of a subscription. When several
developers point at the same dev project and the same subscription, they consume each other's
messages. Nothing errors — it presents as flaky, intermittent delivery, and it reliably costs
someone an afternoon before they work out what is happening.

`resourcePrefix` gives each developer their own resource names inside the shared project:

```ts
new WerkenPubSubTransport({
  projectId: process.env.GCP_PROJECT_ID!,
  subscription: "orders-consumer",
  deadLetterTopic: "orders-dead-letters",
  // Recommended: the developer's username, from the environment.
  resourcePrefix: process.env.WERKEN_RESOURCE_PREFIX,
});
```

With `WERKEN_RESOURCE_PREFIX=alice` that consumer subscribes to `alice-orders-consumer` and
dead-letters to `alice-orders-dead-letters`.

The prefix is applied to whatever each side names — a consumer's `subscription` and
`deadLetterTopic`, a publisher's resolved topic. Those are different resources, so scoping one side
without deciding what the other does is how you get a consumer that looks configured and receives
nothing. Pick one of the two topologies below; mixing them does not work.

### This is for shared development projects only

- **Unset or empty is a no-op**, and that is the production path. With no prefix, names are passed
  through untouched and not validated — they are yours, not ours to police.
- **It refuses to run in production.** Setting `resourcePrefix` while `NODE_ENV=production` fails at
  startup, because a scoped production consumer subscribes to a name nothing publishes to and
  silently receives nothing forever. `allowUnsafeResourcePrefix: true` overrides this if you really
  mean it.
- **It logs at WARN on startup**, naming the resolved resources. Silent name rewriting is exactly
  what costs an hour to diagnose when someone forgets the env var is set in their shell.
- **The resolved name is what gets reported**, not the one you configured. `ctx.subscription`, the
  `subscription` label on metrics, and the `werken-dl-source-subscription` provenance attribute all
  read `alice-orders-consumer`, so diagnostics name the resource that actually delivered the message.
- **Invalid prefixes fail at startup**, not at first publish, with the full resolved name in the
  error. Pub/Sub names must be 3-255 characters, start with a letter, avoid a leading `goog`, and
  use only letters, digits, `-`, `.`, `_`, `~`, `+` or `%`.

### Topology 1: shared topic, per-developer subscriptions

Prefix the **consumer only**, and leave `resourcePrefix` unset on the publisher so it keeps writing
to the shared `orders`. Every developer's subscription hangs off that one topic, which is what lets
each of them receive their own copy of the same events — including whatever a shared producer, or a
teammate, publishes.

```bash
PREFIX="$USER"
gcloud pubsub topics create "$PREFIX-orders-dead-letters" --project "$GCP_PROJECT_ID"
gcloud pubsub subscriptions create "$PREFIX-orders-consumer" \
  --topic orders --project "$GCP_PROJECT_ID"
```

This is the usual choice: it solves the problem this section opened with — several developers
stealing messages from one another's subscription — while keeping one shared stream of events.

### Topology 2: fully isolated publisher and consumer

Set the same prefix on **both** sides, and attach each scoped subscription to the **matching scoped
topic**. A developer then sees only the events they published themselves, which is what you want for
a test that must not be perturbed by anyone else's traffic.

```bash
PREFIX="$USER"
gcloud pubsub topics create "$PREFIX-orders" "$PREFIX-orders-dead-letters" --project "$GCP_PROJECT_ID"
gcloud pubsub subscriptions create "$PREFIX-orders-consumer" \
  --topic "$PREFIX-orders" --project "$GCP_PROJECT_ID"
```

> **The failure mode both topologies exist to avoid** is a scoped subscription attached to a topic
> nothing publishes to: a prefixed publisher writing to `alice-orders` while `alice-orders-consumer`
> reads the shared `orders`, or the reverse. Neither errors. The consumer starts, reports healthy,
> and receives nothing.

### You must create the scoped resources yourself

**Werken never provisions topics or subscriptions** — that belongs in Terraform or your platform
catalogue, in dev as much as in production. If the scoped subscription does not exist, startup fails
naming the exact resource that is missing rather than sitting there healthy and idle.

---

## Testing

The harness runs your real module through real Nest DI and the real pipeline. Only the broker is
in-memory: no network, no credentials, no emulator.

```ts
const harness = await createWerkenTestHarness({
  module: WorkerModule,
  overrides: [{ provide: ShipmentLookup, useValue: fakeShipments }],
});

await harness.emit("com.example.shipment.ready.v1", payload, { subject: "known-1" });

expect(harness.acked).toHaveLength(1);
expect(harness.deadLettered).toHaveLength(0);
```

Also available: `emitRaw(attributes, body)` for envelope-level tests, `drain()` to simulate
shutdown, deterministic clock injection via `now`, and `get(token)` to reach into the module.

For anything that talks to a real dependency, use an integration test — the Pub/Sub emulator
supports schemas, so schema resolution, dead-lettering and publishing can all be exercised end to
end without a GCP project.

---

## Publishing

```ts
const publisher = createEventPublisher({
  source: "https://example.com/orders",
  client: new PubSub({ projectId }),
  topicResolver: (type) => topicMap[type],
  encode: (type, data) => Buffer.from(readerTypes[type].toString(data)), // Avro JSON
});
```

`encode` returning bare bytes declares `application/json`, which is right for the Avro-JSON case
above. For anything else, say so — otherwise a standards-aware consumer picks its decoder from a
lie:

```ts
encode: ((type, data) => ({
  data: protobufFor(type).encode(data).finish(),
  datacontenttype: "application/protobuf",
}),
  await publisher.publish({
    type: "com.example.order.placed.v1",
    data: { orderId: "abc" },
    subject: "abc",
  }));
```

The publisher generates a time-ordered UUIDv7 `ce-id`, stamps `ce-time` and `ingestiontime`
separately, lifts `traceparent` from the ambient OpenTelemetry context, and resolves the destination
topic from the event type. One `Topic` is built per destination and reused, so the SDK's own
batching actually engages.

### Ordering

Ordering is **off** unless you ask for it. With `ordering: true` the publisher derives each message's
ordering key from `subject` (an explicit `orderingKey` on the request still wins), and builds its
`Topic` with `messageOrdering` — which the SDK requires, or it ignores the key entirely:

```ts
const publisher = createEventPublisher({
  source: "https://example.com/orders",
  client: new PubSub({ projectId }),
  topicResolver: (type) => topicMap[type],
  ordering: true, // without this, `subject` is not used as an ordering key
});
```

Without `ordering: true`, `subject` is not used as an ordering key. An explicit `orderingKey` on the
request is still handed to the SDK, but the `Topic` was not built with `messageOrdering`, so nothing
orders on it — and the publish succeeds all the same, with no error to notice. Ordering also needs
message ordering enabled on the **subscription**, which is a broker-side setting Werken does not
manage.

### Batches

`publishBatch` issues every publish before awaiting any — in request order, which is what preserves
ordering per ordering key — and returns the message ids:

```ts
const ids = await publisher.publishBatch([
  { type: "com.example.order.placed.v1", data: { orderId: "abc" }, subject: "abc" },
  { type: "com.example.order.placed.v1", data: { orderId: "def" }, subject: "def" },
]);
```

Pub/Sub has no multi-message transaction, so a batch can fail part-way with the earlier messages
already published and impossible to unsend. That throws `PartialPublishError`, which names both
sides so you can retry **only** the failures — retrying the whole batch would duplicate everything
that already went out:

```ts
try {
  await publisher.publishBatch(requests);
} catch (error) {
  if (error instanceof PartialPublishError) {
    error.published; // [{ index, messageId }] — already sent, do not resend
    error.failures; // [{ index, type, cause }] — safe to retry
  }
  throw error;
}
```

## Observability

One `CONSUMER` span named `{subscription} process`, continuing the producer's trace from
`ce-traceparent`, with child spans for decode and handler. The event type is on the span as the
`cloudevents.event_type` attribute rather than in its name, because a span name is a
low-cardinality aggregation key and `ce-type` is producer-controlled.

The coverage rule is simple: **a span for every message with a valid envelope, a metric for every
message.**

- **A message no handler matches** gets both. That is contract drift, and a span joining the
  producer's trace is how it gets noticed rather than silently acked.
- **An invalid envelope** gets metrics but no span: there is no envelope to trust, so nothing to
  parent a span on. It also shows up in the logs and, by default, in the dead-letter topic.

| Metric                     | Type            | Labels                  |
| -------------------------- | --------------- | ----------------------- |
| `werken.messages.received` | counter         | `route`, `subscription` |
| `werken.messages.outcome`  | counter         | `route`, `outcome`      |
| `werken.handler.duration`  | histogram       | `route`                 |
| `werken.decode.failures`   | counter         | `route`, `reason`       |
| `werken.schema.cache`      | counter         | `result`                |
| `werken.messages.inflight` | up-down counter | `subscription`          |
| `werken.event.lateness`    | histogram       | `route`                 |

### Why `route` and not `ce-type`

`route` is the **pattern you registered** — `com.example.order.*`, or the exact type for an exact
registration. `ce-type` is chosen by the producer, and a wildcard route matches an open-ended set of
them, so labelling on it lets one misbehaving or dynamic producer mint unbounded metric series and
drive up your observability bill. Two bounded sentinels cover the rest:

| `route`       | Meaning                                           |
| ------------- | ------------------------------------------------- |
| `<unmatched>` | Valid envelope, but no registered pattern matched |
| `<invalid>`   | The envelope failed validation                    |

If you need per-type breakdown, take it from the span attribute, where high cardinality is expected
and priced accordingly.

`werken.event.lateness` (`now - ce-time`, in seconds) is deliberately included: for events that
routinely arrive long after they happened, the distribution of lateness is an operational signal in
its own right, not just a debugging aid.

Every pipeline log line carries `ce-id`, `ce-type`, `ce-source`, `ce-subject`, `deliveryAttempt` and
`messageId` as embedded JSON, which Cloud Logging parses into queryable fields.

> Spans only propagate if a `ContextManager` is registered — the OpenTelemetry Node SDK does this for
> you. Without one, `context.active()` always returns root and child spans come out unparented.

## Public API

Everything below is exported from `@werken/nestjs-google-pubsub`, documented here, and covered by
semver. The test harness is at `@werken/nestjs-google-pubsub/testing`.

| Consuming                                                          | Publishing                         | Dead-lettering                         | Idempotency                                                              |
| ------------------------------------------------------------------ | ---------------------------------- | -------------------------------------- | ------------------------------------------------------------------------ |
| `WerkenPubSubTransport`                                            | `createEventPublisher`             | `TerminalEventError`                   | `createSqlIdempotencyStore`                                              |
| `WerkenTransportOptions`                                           | `EventPublisherOptions`            | `PubSubDeadLetterPublisher`            | `InMemoryIdempotencyStore`, `NoopIdempotencyStore`                       |
| `CloudEventContext`, `IncomingMessage`                             | `PublishRequest`, `PublishOptions` | `DeadLetterPublisher`                  | `IdempotencyStore`, `IdempotencyKey`                                     |
| `EventHandler`, `Outcome`, `RejectionPolicy`                       | `EncodedPayload`                   | `DeadLetterRequest`, `DeadLetterStage` | `SqlExecutor`, `SqlIdempotencyStoreOptions`                              |
| `ValidationOptions`, `FlowControlOptions`, `SchemaRegistryOptions` | `PartialPublishError`              | `DEAD_LETTER_ATTRIBUTES`               | `idempotencyKeyToString`, `pruneExpiredSql`, `DEFAULT_IDEMPOTENCY_TABLE` |

Plus the errors worth catching by type — `SchemaDecodeError`, `ResourcePrefixError`,
`InvalidPatternError`, `AmbiguousPatternError` — and the structural SDK types you need only if you
supply your own `createClient` or dead-letter publisher: `PubSubClientLike`, `SubscriptionLike`,
`TopicLike`, `SchemaLike`.

**What is deliberately not exported.** The engine — the message pipeline, pattern router, Avro
codec, schema revision cache, telemetry facade, context builder and resource-name helpers. They are
in `src/internal.ts`, which is absent from the package's `exports` map, so Node refuses to resolve
`@werken/nestjs-google-pubsub/internal` from an installed copy (`ERR_PACKAGE_PATH_NOT_EXPORTED`) —
this repo's own tests reach it through a build-time alias.

That is a deliberate 0.x decision. A published surface nothing documents is one nobody can change
safely, and it is cheaper to widen later than to narrow after people depend on it. If you need
something that is not here, open an issue and it gets exported with documentation and a test rather
than by accident.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Note that pnpm 11 does not run lifecycle scripts by default,
so git hooks need `pnpm run hooks:install` explicitly.

## Licence

MIT
