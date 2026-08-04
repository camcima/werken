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
