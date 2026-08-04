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
