# @werken/nestjs-google-pubsub

A NestJS custom transport for Google Cloud Pub/Sub that speaks CloudEvents 1.0. Write an event
consumer as an ordinary Nest controller.

Part of [Werken](https://github.com/camcima/werken) — **see the root README for the full
documentation**: configuration reference, idempotency store adapters, schema handling, resource
prefixing, observability and the migration guide.

```bash
npm install @werken/nestjs-google-pubsub @google-cloud/pubsub
```

Peer dependencies: `@nestjs/common`, `@nestjs/microservices`, `@google-cloud/pubsub`.
`@opentelemetry/api` and `@nestjs/testing` are optional — without them, telemetry degrades to a
no-op and the test harness is simply unavailable.

## Quick start

A handler is a controller method. Routing is on `ce-type`, not on the topic or subscription:

```ts
@Controller()
export class OrderConsumer {
  constructor(private readonly orders: OrderService) {}

  @EventPattern("com.example.order.placed.v1")
  async onOrderPlaced(@Payload() data: OrderPlaced, @Ctx() ctx: CloudEventContext) {
    await this.orders.place(data, ctx.id);
  }
}
```

Return to ack, throw to nack and be retried, or throw `TerminalEventError` to dead-letter
immediately — for a message that is well-formed but will never be processable, so it does not burn
its retry budget or block an ordering key.

```ts
const app = await NestFactory.createMicroservice(WorkerModule, {
  strategy: new WerkenPubSubTransport({
    projectId: process.env.GCP_PROJECT_ID!,
    subscription: process.env.PUBSUB_SUBSCRIPTION!,
    deadLetterTopic: process.env.PUBSUB_DEAD_LETTER_TOPIC,
    idempotency: { consumer: "order-processing" },
  }),
});

// Do not skip: without it Nest never calls close(), so scale-down kills in-flight handlers.
app.enableShutdownHooks();
await app.listen();
```

## What it handles for you

Envelope parsing and validation, routing on `ce-type` with wildcard patterns, Avro decode against
the writer's schema revision, duplicate suppression, dead-lettering, graceful drain on SIGTERM, and
OpenTelemetry spans continuing the producer's trace.

## Testing

`@werken/nestjs-google-pubsub/testing` runs your real Nest module against an in-memory broker — no
emulator, credentials or network:

```ts
const harness = await createWerkenTestHarness({ module: WorkerModule });
await harness.emit("com.example.order.placed.v1", { orderId: "abc" });
expect(harness.acked).toHaveLength(1);
```

## License

MIT
