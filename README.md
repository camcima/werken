# Werken

NestJS transport for Google Cloud Pub/Sub speaking CloudEvents 1.0. Write an event consumer as an
ordinary Nest controller and get schema resolution, envelope validation, idempotency, dead-lettering
and clean shutdown without writing any of them yourself.

| Package                                | What it is                                                                                |
| -------------------------------------- | ----------------------------------------------------------------------------------------- |
| `@werken/cloudevents`                  | CloudEvents 1.0 envelope types, validation, Pub/Sub attribute binding. Zero dependencies. |
| `@werken/nestjs-google-pubsub`         | The transport, publisher, schema codec and idempotency.                                   |
| `@werken/nestjs-google-pubsub/testing` | In-memory harness. No broker, no credentials, no emulator.                                |

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

> **This is the one thing to get right.** The failure is silent, not loud. An adapter that always
> returns `0` makes `tryRecord` report every message as already-processed and **drop all of them**,
> and makes `has` report every message as new, disabling de-duplication. The snippets below were
> checked against each driver's actual type definitions, not written from memory.

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

### MongoDB — unique index and duplicate-key errors

Insert into a collection with a unique index on the key and treat error code `11000`
(duplicate key) as "already processed". A TTL index on `expiresAt` handles expiry, so again no
pruning job.

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
> the two writes to be atomic. That sequencing is unresolved — see the open questions in the spec.

### Therefore

**Handlers must be idempotent regardless of which mode you use, and regardless of whether
exactly-once delivery is enabled on the subscription.** Exactly-once delivery is a guarantee about
_delivery_, not about _processing_. Werken narrows the duplicate window; your handler closes it.

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
dead-letters to `alice-orders-dead-letters`. Set the same value on the publisher so both directions
are scoped together — **a scoped consumer reading from an unscoped topic is worse than no scoping**,
because it looks configured and receives nothing.

### This is for shared development projects only

- **Unset or empty is a no-op**, and that is the production path. With no prefix, names are passed
  through untouched and not validated — they are yours, not ours to police.
- **It refuses to run in production.** Setting `resourcePrefix` while `NODE_ENV=production` fails at
  startup, because a scoped production consumer subscribes to a name nothing publishes to and
  silently receives nothing forever. `allowUnsafeResourcePrefix: true` overrides this if you really
  mean it.
- **It logs at WARN on startup**, naming the resolved resources. Silent name rewriting is exactly
  what costs an hour to diagnose when someone forgets the env var is set in their shell.
- **Invalid prefixes fail at startup**, not at first publish, with the full resolved name in the
  error. Pub/Sub names must be 3-255 characters, start with a letter, avoid a leading `goog`, and
  use only letters, digits, `-`, `.`, `_`, `~`, `+` or `%`.

### You must create the scoped resources yourself

**Werken never provisions topics or subscriptions** — that belongs in Terraform or your platform
catalogue, in dev as much as in production. If the scoped subscription does not exist, startup fails
naming the exact resource that is missing rather than sitting there healthy and idle:

```bash
PREFIX="$USER"
gcloud pubsub topics create "$PREFIX-orders-dead-letters" --project "$GCP_PROJECT_ID"
gcloud pubsub subscriptions create "$PREFIX-orders-consumer" \
  --topic orders --project "$GCP_PROJECT_ID"
```

Note the subscription attaches to the **shared** `orders` topic, which is what lets every developer
receive their own copy of the same published events.

---

## Licence

MIT
