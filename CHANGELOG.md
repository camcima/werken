# Changelog

## [0.3.0](https://github.com/camcima/werken/compare/v0.2.0...v0.3.0) (2026-08-04)

### Features

* **example:** add a publisher example ([f4e1b9a](https://github.com/camcima/werken/commit/f4e1b9a4994ad33665d8fe5edd06a010afdd8d5b))
* **example:** add the advanced consumer's domain and routing ([f9ca337](https://github.com/camcima/werken/commit/f9ca337b7c62f9ebf78d0094f88fda1a5c6fa73c))
* **example:** add the advanced consumer's Postgres adapters ([c97d906](https://github.com/camcima/werken/commit/c97d906d0494b46d3d5c133b08279bf73ffaace3))
* **example:** wire the advanced consumer's transport and provisioning ([519de6a](https://github.com/camcima/werken/commit/519de6ae6b4a5314e2fbfedb52115a3fdf8586c0))

### Bug Fixes

* **example:** enable message ordering on the shipment subscription ([e9afee7](https://github.com/camcima/werken/commit/e9afee748542dca7cbf039924d19ab661fe3d987))
* **example:** fail loudly on every missing env var, and await the worker ([b36948b](https://github.com/camcima/werken/commit/b36948b55f6ee5909095953a9304d30667f8d9c0)), references [#2](https://github.com/camcima/werken/issues/2)
* **example:** guard the projection against out-of-order events ([ab3024f](https://github.com/camcima/werken/commit/ab3024f6fdab482419803f5f9213420cae34e6e1))
* **release:** restrict publication to real packages ([e8cb214](https://github.com/camcima/werken/commit/e8cb214b90baa21b978754abfc22589e6bcade75))

## [0.2.0](https://github.com/camcima/werken/compare/v0.1.0...v0.2.0) (2026-08-04)

### ⚠ BREAKING CHANGES

* **pubsub:** idempotencyKeyToString now emits JSON rather than fields
  joined by U+001F. Only stores keyed on the flattened form are affected — the
  documented Redis and MongoDB adapters — and the effect is that existing markers
  no longer match, so a redelivery in flight during the upgrade may be
  reprocessed. The SQL store keys on separate columns and is unaffected.

  - **Key encoding.** The separator was not escaped, so a ce-id or consumer
    containing it could flatten two different keys to one string, and the symptom
    is an event silently dropped as a duplicate rather than an error anyone sees.
    JSON escapes the boundaries instead of hoping they never occur.
  - **Duplicate routes.** PatternRouter caught Nest's chained-handler
    representation but not two entries passed directly: an exact duplicate
    overwrote the first, a duplicate wildcard kept it, and either way a handler
    registered in good faith never ran. Now rejected, naming the pattern.
  - **Option validation.** Negative, zero, NaN and non-integer values were
    accepted and surfaced later as surprising SDK, timer or SQL behaviour — a NaN
    flow-control limit the SDK silently replaces with its own default, a zero TTL
    that expires an idempotency marker the instant it is written. Validated once
    at startup, each message naming its exact option path.
  - **CI.** Top-level `permissions: contents: read`, and third-party actions
    pinned to commit SHAs with the version kept as a comment. A tag can be moved
    to point at new code; a SHA cannot.
* **pubsub:** metric labels are renamed from `type` to `route` and carry
  the registered pattern rather than the raw ce-type; the consumer span is named
  `{subscription} process` rather than `{ce-type} process`; and
  MessagePipelineOptions.resolveHandler becomes resolveRoute, returning
  `{ handler, pattern }`.

  ce-type is producer-controlled and a wildcard route matches an open-ended set
  of them, so labelling five instruments and the span name on it let one
  misbehaving or dynamic producer mint unbounded metric series and span
  operation names. The registered pattern is bounded by what this consumer
  registered, so PatternRouter.resolve now reports which pattern matched and the
  pipeline labels on that. Messages that reached no route are labelled
  `<unmatched>`, and invalid envelopes `<invalid>`. The event type is still on
  the span, as an attribute, where high cardinality is expected and priced.

  Coverage had two holes, both in the messages most useful for spotting contract
* **pubsub:** the barrel no longer exports MessagePipeline, PatternRouter,
  AvroCodec, SchemaRevisionCache, createTelemetry, buildContext, the
  resource-name helpers, toSubscriberOptions, the logging helpers, the schema
  attribute helpers, or the DEFAULT_* tuning constants.

  The barrel exported the entire engine while the README documented a handful of
  entry points, so most of the published surface was supported by accident:
  impossible to change safely and impossible to write a reference for. Narrowing
  it is only cheap while nobody depends on it, which is now.

  What remains is what the README documents — the transport, the publisher,
  dead-lettering, idempotency, the option and context types, the errors worth
  catching by type, and the structural SDK types needed to supply your own client
  or dead-letter publisher. A Public API section lists all of it.

  The internals move to src/internal.ts, which is deliberately absent from the
  package's `exports` map: Node refuses to resolve
  @werken/nestjs-google-pubsub/internal from an installed copy, verified —
  ERR_PACKAGE_PATH_NOT_EXPORTED. So the boundary is enforced by the resolver
  rather than by convention. This repo's tests reach it through a vitest alias
  and a tsconfig path, neither of which ships.

### Features

* **pubsub:** let an encoder declare the media type it produced ([3442134](https://github.com/camcima/werken/commit/3442134bf787c86482797eeb900f7f25b2e2f53b))

### Bug Fixes

* address Copilot review on the publisher docs and harness key ([0329784](https://github.com/camcima/werken/commit/03297844a7de2b7c5a69f073476497b2ce1ee48d))
* **example:** make the worked example build and start ([3d17cff](https://github.com/camcima/werken/commit/3d17cff63260425e3ed4ef652144061d9cff5ad8))
* **pubsub:** act on tryRecord's result instead of discarding it ([ff7f944](https://github.com/camcima/werken/commit/ff7f944bea27da7c1fb7ae3a205902a20b819e32))
* **pubsub:** bound telemetry cardinality and cover every message ([db4895b](https://github.com/camcima/werken/commit/db4895b9387220e963588e91be6868b830e81143))
* **pubsub:** close the remaining hardening gaps ([4a9ea18](https://github.com/camcima/werken/commit/4a9ea18d16128a03a6bb93938557ee37d92a927d))
* **pubsub:** close what startup created when startup fails ([d9045ad](https://github.com/camcima/werken/commit/d9045ad372d215a7a012528d34ea733fbd609e47))
* **pubsub:** keep terminal detail and the ordering key when dead-lettering ([3ed228e](https://github.com/camcima/werken/commit/3ed228e5b58add601b68205b1b65b6a110ed6ab2))
* **pubsub:** narrow strict mode to the failure it documents ([1be45f4](https://github.com/camcima/werken/commit/1be45f4ca43bbd3b1461bd4d80c5e7acfbfcf854))
* **pubsub:** report the subscription a message actually came from ([ef88ead](https://github.com/camcima/werken/commit/ef88eade64edc01074ed2cd69b3b881d6e4fe4e7))
* **pubsub:** validate the envelope before in-process de-duplication ([3981157](https://github.com/camcima/werken/commit/398115722b69cfd79b2f15c40ff283ad1f50f6a4))
* **testing:** make the harness safe under concurrent emits ([5abd6fc](https://github.com/camcima/werken/commit/5abd6fc68f491fe82e707a671ee8fc35b005d51f))

### Code Refactoring

* **pubsub:** narrow the public API to what is documented ([761c8e1](https://github.com/camcima/werken/commit/761c8e1e0fbf108813e8f12e99b1a950db1675c6))

## 0.1.0 (2026-08-03)

### Features

* **cloudevents:** add CloudEvents 1.0 envelope binding and validation ([3e23cc0](https://github.com/camcima/werken/commit/3e23cc04203cbb66984b971feb520645058a95fa))
* **pubsub:** add EventPublisher ([9eff651](https://github.com/camcima/werken/commit/9eff651cba2429ee3c2bc6fe44f57dba2800ce4e))
* **pubsub:** add lifecycle, flow control and drain on shutdown ([c14d7be](https://github.com/camcima/werken/commit/c14d7be2defdda3dce23deecc5d41f00251731d2))
* **pubsub:** add Nest transport with schema, dead-lettering and idempotency ([565ce86](https://github.com/camcima/werken/commit/565ce86cd146e03a5551330a5d7a5babceb21da4))
* **pubsub:** add resource prefixing for shared dev projects ([18fd217](https://github.com/camcima/werken/commit/18fd2171f2778c9e776cd5631fce60741b7cec78))
* **pubsub:** add tracing, metrics and structured logging ([b4d4b03](https://github.com/camcima/werken/commit/b4d4b0339ba8930dfe9f80e5a63308cb5bb099b9))
* **pubsub:** add wildcard routing, worked example and migration guide ([761a21c](https://github.com/camcima/werken/commit/761a21ce84a8f6d41ba0382403a99d5a56f9b6fc))

### Bug Fixes

* **docs:** centre the logo by hugging its viewBox to the artwork ([9c21672](https://github.com/camcima/werken/commit/9c2167203d6552921432b9755ede5092fdeb737c))
* **pubsub:** close out low-severity findings and documentation parity ([87c8778](https://github.com/camcima/werken/commit/87c87780b8f39258b8f3549a4ceb51fef76d2d46))
* **pubsub:** emit the schema cache metric and close untested paths ([1d92f29](https://github.com/camcima/werken/commit/1d92f298ac5c3c25d446ca1f338e1859a0dafe1e))
* **pubsub:** harden shutdown, bound the router cache and report partial batch failures ([3013fb7](https://github.com/camcima/werken/commit/3013fb74e82129439b545dc0db0a80f3fa61c432))
* **pubsub:** wire up telemetry and stop the harness dropping events ([57e5ece](https://github.com/camcima/werken/commit/57e5eceb1d39c3928b7e038c7fde4caec07293cb))
