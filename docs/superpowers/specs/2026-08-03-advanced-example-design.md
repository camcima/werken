# Advanced example — design

- **Date:** 2026-08-03
- **Status:** approved, not yet implemented
- **Baseline:** v0.2.0 (`438af91`)

## Problem

`examples/minimal-consumer` is the only runnable artefact in the repository, and it exercises 2 of
the library's 16 public exports. Measured against the README's own feature list, Avro schema
resolution, idempotency, wildcard routing and the publisher have no runnable reference at all.

Two of those gaps are worse than mere absence:

- **Idempotency ships switched off.** The example sets `idempotency: { consumer }` with the
  `executor` commented out, so `resolveIdempotencyStore` falls through to `NoopIdempotencyStore`.
  The block reads as configured while de-duplication is disabled. Confirmed by running it: the
  worker logs `no idempotency store configured — duplicate deliveries WILL be reprocessed`. This is
  the feature the README is most emphatic about — an entire section on at-least-once, Mode A versus
  Mode B, and adapter tables for seven drivers.
- **The publisher has no example whatsoever**, despite being a public export with ~80 lines of
  README and a breaking change in 0.2.0 (`encode` may now return `{ data, datacontenttype }`).

## Decisions

Recorded with the reasoning, because each closed off a plausible alternative.

### The advanced example does not publish

An earlier proposal had one service consume an event, write to Postgres, and emit a follow-up
event. Rejected: a consumer example that publishes in order to claim coverage is the demo-reel
failure this design is trying to avoid. The publisher gets its own example instead, where ordering,
batching and `encode` can be shown deliberately rather than incidentally.

### Real backends, not in-memory substitutes

The advanced consumer requires the Pub/Sub emulator _and_ Postgres. Rejected alternatives: plain
JSON with a real Postgres (leaves Avro unreferenced, and Avro's behaviour is the most surprising
thing in the library — Pub/Sub's "JSON" encoding is Avro JSON, where a nullable union is
`{"string":"SCL"}` and plain JSON is rejected outright), and a zero-infrastructure version using
`InMemoryIdempotencyStore` (repeats the existing example's sin of shipping a store nobody should
run in production).

Both backends are already in `docker-compose.yml`, and the repository's established posture is to
test against real dependencies rather than fakes.

**Constraint:** the example must fail loudly and legibly when Postgres or the schema is absent —
matching the transport's own missing-subscription startup error — rather than degrading.

### The media-type feature stays undemonstrated

`encode` returning `{ data, datacontenttype }` does not fit. A schema-attached Avro topic _is_
`application/json` on the wire, so the honest encoder returns bare bytes and the feature never
appears. Covering it would need a second, unschematised topic publishing protobuf purely to
demonstrate a header — a topic that exists only for a demo. Left to the README, which already has a
correct, copy-pasteable snippet, plus a comment in the publisher example explaining why Avro JSON
declares `application/json`.

### `minimal-consumer` keeps its name

The basic/advanced framing lives in documentation. The path is linked from the README, the
migration guide, and the 0.2.0 release notes already published to npm.

## Shape

```
examples/minimal-consumer/    unchanged — the basic example
examples/advanced-consumer/   new
examples/publisher/           new
```

### `advanced-consumer` — a read-model builder

Consumes shipment events and maintains a Postgres projection table. The domain is chosen because it
_motivates_ each feature rather than displaying it:

| Feature                    | Why this service needs it                                                     |
| -------------------------- | ----------------------------------------------------------------------------- |
| Idempotency (SQL, Mode A)  | It writes to a database; a redelivery would double-apply                      |
| Wildcard routing           | It cares about several event types; one exact route outranks the wildcard     |
| Avro schema resolution     | It has a real producer contract, and must survive the producer adding a field |
| `TerminalEventError` + DLQ | A shipment that will never exist must not burn its retry budget               |

The projection and the dedup marker share one Postgres pool, which is what makes Mode A's
per-message `executor` honest rather than decorative.

```
examples/advanced-consumer/
  package.json                          private: true
  tsconfig.json, tsconfig.test.json
  schema/shipment-events.avsc           the contract, shared with the publisher
  scripts/provision.sh                  shared provisioning — see below
  src/main.worker.ts
  src/worker.module.ts
  src/domain/ports.ts                   ShipmentProjection port
  src/schema/reader-types.ts            compiled avsc reader types
  src/adapters/outbound/pg-executor.ts  SqlExecutor over a pg Pool — the Mode A adapter
  src/adapters/outbound/pg-projection.ts
  src/adapters/inbound/shipment-events.consumer.ts
  tests/shipment-events.consumer.test.ts
```

`scripts/provision.sh` provisions for **both** examples — the Avro schema, the schema-attached
topic, the subscription, the dead-letter topic, and the Postgres DDL. It lives here rather than in
a shared directory because the consumer is the side that cannot start without any of it; the
publisher only needs the topic. Both examples' READMEs point at the one script.

Transport configuration exercises `schemaRegistry` (strict, the default), `idempotency.executor`,
wildcard plus exact routing, `validation.onInvalidEnvelope` and `onDecodeFailure`,
`onUnhandledPattern`, `deadLetterTopic`, and tuned `ackDeadline` / `flowControl` /
`shutdownDrainTimeoutMs`.

### `publisher` — the writer side

A plain program, not a Nest application. Emits the events `advanced-consumer` reads, so a reader can
run both and watch rows appear.

Exercises `createEventPublisher`, `topicResolver`, `encode` producing Avro JSON, `ordering: true`
deriving keys from `subject`, `publishBatch`, and a `PartialPublishError` catch.

Pairing the two demonstrates the Avro contract from both ends — writer schema on the publish side,
compiled reader type on the consume side.

## Keeping examples out of the distribution

Two distinct risks.

**Example code leaking into the library tarball.** Already impossible: examples live outside
`packages/`, and the package ships a `files: ["dist", ...]` allowlist. Verified against the
published 0.2.0 tarball, which contains only `LICENSE`, `README.md`, `dist` and `package.json`.
Unchanged by this work.

**Examples published as their own npm packages.** The live risk. Today the only guard is
`private: true`, which nothing enforces — a new example missing that line would be published by
`pnpm -r publish`. Demonstrated with a throwaway workspace: the current blocklist selects it, a
`--filter "./packages/*"` allowlist does not.

- Both new examples set `private: true`, keeping the convention.
- `scripts/publish.sh` switches to `pnpm -r --filter "./packages/*" publish`, restricting
  publication to real packages by construction.

Deliberately **no** third guard asserting every `examples/*` is private. Once the allowlist is in
place a forgotten flag is harmless, so the check would police a convention whose failure mode no
longer exists.

The publish.sh change is worth making regardless of whether the examples land: it is a latent
footgun on a command that cannot be undone.

## Verification

- **Unit, `advanced-consumer`**: a harness test, no infrastructure, runs in `pnpm test`. The
  harness is `schemas: "passthrough"`, so it covers handler logic on plain JSON only — Avro is
  never exercised here, by design.
- **Unit, `publisher`**: _not_ a harness test. `createWerkenTestHarness` drives a Nest module, and
  the publisher is a plain program with no module to give it. It gets a plain unit test against a
  fake `PubSubClientLike`, asserting the attributes and ordering key it emits — the same approach
  `packages/nestjs-google-pubsub/tests/publisher.test.ts` already uses.
- **Integration**: one test that provisions, builds both examples, runs the publisher, runs the
  consumer, and asserts the projected rows in Postgres. CI's integration job already provides both
  backends.

## Repository changes outside the examples

- `vitest.shared.ts`: widen `INTEGRATION_GLOB` from `packages/*/tests/integration/**` to
  `{packages,examples}/*/tests/integration/**`, and move `example-worker.integration.test.ts` out of
  the library's test folder, where it was parked only because the glob did not reach examples.
- `tsconfig.json`: project references for both new examples.
- `knip.ts`: workspace entries for both, with `src/main.worker.ts` / `src/main.ts` as entry points.
- `scripts/publish.sh`: the `--filter` allowlist above.
- `README.md`: point at the basic and advanced examples, and say what each one argues.

## Non-goals

- Renaming `minimal-consumer`.
- Demonstrating `EncodedPayload`'s media type (see Decisions).
- Executable documentation checks for README snippets. A real gap — a broken snippet reached `main`
  in this release cycle and Prettier reformatted it into something that looked deliberate — but it
  is separate work.
- Mode B (transactional inbox). Still unresolved in the library; nothing to demonstrate.
