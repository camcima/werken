# Migrating an existing Pub/Sub consumer to Werken

For services already consuming Pub/Sub directly, or through a community Nest transport.

The end state is the one in [`examples/minimal-consumer`](../examples/minimal-consumer): the
handler holds business logic and nothing else.

---

## 1. What you delete

Most of a hand-rolled consumer is machinery Werken already provides. Delete rather than port:

| You currently have                                            | Werken                                    |
| ------------------------------------------------------------- | ----------------------------------------- |
| `subscription.on("message", …)` plus `message.ack()`/`nack()` | Return to ack, throw to nack              |
| A `try/catch` deciding retry vs give-up                       | Throw `TerminalEventError` to dead-letter |
| Manual JSON parse / Avro decode                               | `schemaRegistry`, resolved by revision    |
| A "have I seen this id" table lookup                          | `idempotency`                             |
| `traceparent` extraction and span creation                    | Automatic, from `ce-traceparent`          |
| `process.on("SIGTERM", …)` drain logic                        | `app.enableShutdownHooks()`               |
| Manual ack-deadline extension                                 | `ackDeadline.maxExtensionMs`              |

If a step below asks you to keep hand-rolled machinery, that is a bug in this guide — raise it.

## 2. Bootstrap

Worker pools have no HTTP endpoint, so this is a microservice, not an HTTP app:

```ts
const app = await NestFactory.createMicroservice(WorkerModule, {
  strategy: new WerkenPubSubTransport({
    projectId: process.env.GCP_PROJECT_ID!,
    subscription: process.env.PUBSUB_SUBSCRIPTION!,
    deadLetterTopic: process.env.PUBSUB_DEAD_LETTER_TOPIC,
    idempotency: { consumer: "your-service-name" },
  }),
  bufferLogs: true,
});

app.enableShutdownHooks(); // ← do not skip; see §5
await app.listen();
```

## 3. Handlers

Replace your message loop with `@EventPattern` on the **`ce-type`**, not the topic or subscription:

```ts
@Controller()
export class OrdersConsumer {
  @EventPattern("com.example.order.placed.v1")
  async onOrderPlaced(@Payload() data: OrderPlacedV1, @Ctx() ctx: CloudEventContext) { … }
}
```

Wildcards are supported — `com.example.*` matches one or more trailing segments, and `*` catches
everything. **Exactly one handler runs per message**: exact beats wildcard, and among wildcards the
longest literal prefix wins. Registering two handlers for one pattern fails at startup rather than
silently running only one of them.

`*` is only valid as the final segment or on its own. `com.*.thing` fails at startup, because a
pattern that silently never matches is far harder to notice than a boot error.

## 4. Outcomes — the one behaviour change that matters

| Your handler                | Werken                                    |
| --------------------------- | ----------------------------------------- |
| returns                     | ack                                       |
| throws anything             | nack, redelivered with backoff            |
| throws `TerminalEventError` | dead-lettered immediately, original acked |

**Audit your existing `catch` blocks before switching.** A hand-rolled consumer that swallows an
error and acks becomes, under Werken, a consumer that nacks and retries forever. Anything that
will never succeed must throw `TerminalEventError`, not a plain `Error`:

```ts
if (order === undefined) {
  throw new TerminalEventError(`unknown order ${data.orderId}`);
}
```

This matters most behind an ordering key, where one stuck message blocks every later event for the
same entity.

## 5. Shutdown

`app.enableShutdownHooks()` is not optional. Without it Nest never calls the transport's `close()`,
so a scale-down kills in-flight handlers mid-write and every interrupted message is reprocessed
from scratch. With it, Werken stops taking new work, waits out the in-flight handlers up to
`shutdownDrainTimeoutMs`, and nacks anything still running so it is redelivered promptly rather
than waiting for the ack deadline to lapse.

## 6. Idempotency

Pub/Sub is at-least-once, and exactly-once _delivery_ is not exactly-once _processing_. Configure a
store — see the adapters in the [README](../README.md) — and keep your handlers idempotent anyway.

If you already have a "processed events" table, point the executor at it via a custom `table` rather
than migrating data; the schema is in [`idempotency-schema.sql`](idempotency-schema.sql).

## 7. Schemas

Werken resolves the **writer** schema by revision from the Pub/Sub Schema Service and decodes it
into your compiled **reader** type. Supply the reader type; do not fetch schemas yourself:

```ts
schemaRegistry: {
  readerTypeFor: (name) => readerTypes[name];
}
```

⚠️ **Pub/Sub's `JSON` encoding is Avro JSON, not plain JSON.** A nullable union is
`{"string":"SCL"}`, not `"SCL"`. If you are hand-rolling `JSON.stringify` on the publish side today,
it is being rejected or will be as soon as a schema is attached. Prefer non-null fields with
defaults over nullable unions — it keeps payloads readable and evolves more cleanly.

## 8. Tests

Delete your emulator-based handler tests. The harness needs no broker, credentials or emulator:

```ts
const harness = await createWerkenTestHarness({
  module: WorkerModule,
  overrides: [{ provide: OrderLookup, useValue: fakeOrders }],
});

await harness.emit("com.example.order.placed.v1", payload, { subject: "order-1" });

expect(harness.acked).toHaveLength(1);
expect(harness.deadLettered).toHaveLength(0);
```

Keep integration tests for anything that talks to a real dependency — the emulator supports schemas,
so schema resolution can be tested end to end without a GCP project.

## 9. Checklist

- [ ] `createMicroservice`, not `createApplication`
- [ ] `app.enableShutdownHooks()`
- [ ] `@EventPattern` keyed on `ce-type`
- [ ] Every "will never succeed" path throws `TerminalEventError`
- [ ] `catch` blocks that used to swallow-and-ack reviewed
- [ ] `idempotency.consumer` set, and a store configured
- [ ] `deadLetterTopic` configured and provisioned
- [ ] Publishers encode Avro JSON, not plain JSON
- [ ] Handler tests moved to the harness
