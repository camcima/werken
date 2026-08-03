# Code review — werken

Date: 2026-08-03. Scope: full source of `@werken/cloudevents` and `@werken/nestjs-google-pubsub`,
the test harness, the example, build/CI tooling, and all documentation (README, docs/, package
metadata). Reviewed at commit `e0d620a`.

> **Status: everything below is fixed.** All findings — high, medium and low severity, plus the
> documentation-parity gaps — were addressed on 2026-08-03. Every behavioural fix has a regression
> test that was watched failing first; see `tests/integration/dist-smoke.integration.test.ts`,
> `tests/pipeline-telemetry.test.ts`, and the new cases in `tests/harness.test.ts`,
> `tests/lifecycle.test.ts`, `tests/pattern-router.test.ts`, `tests/publisher.test.ts` and
> `tests/idempotency.test.ts`. The sections below describe the code as originally reviewed and are
> kept as the rationale for the changes.
>
> Three fixes changed behaviour worth knowing about. `publishBatch` now issues all publishes before
> awaiting (still in request order) and throws `PartialPublishError` on partial failure.
> `PatternRouter` no longer caches when no wildcard is registered, since exact lookups are already
> O(1). And `idempotencyKeyToString` now joins on `\u001f` rather than a space, which changes the
> stored key for single-column stores — relevant only to adapters built against the pre-fix format.
>
> One item from §Design observations is deliberately **not** done: `idempotency.consumer` still
> falls back to `"werken"` when a store is configured without one, so two services sharing a store
> and both defaulting would dedupe against each other. Making that a warning or a startup failure
> is a behaviour change worth deciding on separately.

## Follow-up: coverage audit (2026-08-03)

A coverage pass afterwards found one more instance of the finding #2 pattern — a documented metric
that was implemented, unit-tested, and never called. `recordSchemaCache` had no call site anywhere
in the library, so `werken.schema.cache` was always empty despite appearing in the README's metrics
table. `SchemaRevisionCache` now reports each lookup through an `onResult` callback, which
`AvroCodec` forwards and the transport feeds to telemetry. A callback rather than polling
`cacheStats`, because a counter sampled after the fact cannot attribute individual lookups.

All nine `Telemetry` methods have since been audited for call sites; this was the last orphan.

The same pass also found that a schema resolution failure logged only `could not resolve writer
schema <name>`, with the cause dropped — so a client without schema support, a schema with no
definition, and a Schema Service outage were indistinguishable in the logs. The cause is now folded
into the message.

Coverage went from 95.7% to 98.2% of statements and 91.6% to 94.8% of branches, closing the
untested paths for the `strict: false` fallback, both schema-fetch error branches, the
nack-throws-during-drain path, the codec's plain-JSON handling, `tracestate` propagation, and the
`idempotency.executor` wiring.

`AvroCodec#cacheStats` was removed rather than kept: once the cache reports each lookup through
`onResult`, a cumulative getter nothing reads is redundant surface. `SchemaRevisionCache#stats`
remains, since that is where the counters live.

CI now uploads coverage from **both** suites — the `unit` flag from the `ci` job and the
`integration` flag from the job running against the emulator and a real Postgres — so the project
total is their union. Previously only the unit half was uploaded, which meant code reachable only
against a real broker or database counted as uncovered, and the reported number could not
distinguish "tested in the integration job" from "tested nowhere". `codecov.notify.after_n_builds`
holds the status until both uploads land, so it is never graded on the unit half alone.

What remains uncovered is deliberate: no-op stubs, the `@opentelemetry/api`-absent branches, and two
defensive `if (iterator.done) break` guards in LRU eviction that cannot be reached because the loop
only runs when the map is over capacity.

One line is worth calling out as genuinely unreachable rather than merely untested:
`transport.ts`'s `if (this.draining) return` in `handleMessage`. `close()` removes the message
listener synchronously before its first `await`, so nothing can arrive afterwards. It is kept as
defence in case listener-removal stops being a hard guarantee, but no honest test reaches it — the
existing "stops handling messages that arrive after close begins" test passes because the listener
is gone, not because of this guard.

## Verdict

This is an unusually well-built library. The ports-and-adapters structure is genuinely clean
(structural `*Like` types keep `@google-cloud/pubsub` a peer dependency; the SQL store needs no
driver), failure policy is consistently "loud over silent", the two Nest traps that lose messages
(Observable-wrapped handlers, `RpcExceptionsHandler` swallowing error identity) were found and
defeated with tests, and the comments explain _why_ rather than _what_. The findings below do not
change that assessment — but three of them are real bugs, and two of those undermine features the
README explicitly advertises.

---

## High severity

### 1. Telemetry and trace propagation are silently dead in the ESM build

`telemetry.ts:157` and `publisher.ts:122` load `@opentelemetry/api` with a bare `require(...)`.
Both packages are `"type": "module"`, and the ESM build (`dist/index.js`, the `import` condition —
also the `main`) preserves the call verbatim (`dist/telemetry.js:104`, `dist/publisher.js:66`).
In Node ESM scope `require` is undefined, so the call throws `ReferenceError`, the surrounding
`try/catch` swallows it, and:

- `createTelemetry()` always returns the no-op — no spans, no metrics, even with
  `@opentelemetry/api` installed and `telemetry.enabled` true;
- `currentTraceparent()` always returns `undefined` — published events never carry `ce-traceparent`.

The CJS build works, which is why NestJS apps compiled to CJS won't notice — but the package
advertises "Dual ESM + CJS — every entry point importable both" and the ESM entry is the primary
one. The transport already solves this correctly: `loadDuration()`/`createDefaultClient()` go
through Nest's `loadPackage` helper, which lives in CJS scope where `require` works
(`transport.ts:263-281`). Route the OpenTelemetry loading the same way, or restructure to a
dynamic `import()`.

Nothing catches this because tests run through vitest's module runner, not the built dist. A smoke
test that imports `dist/index.js` under plain `node` and asserts `createTelemetry` is not the no-op
would have caught it, and would keep catching this whole class of bug.

### 2. The per-message CONSUMER span and the outcome metric are not wired into the pipeline

`Telemetry.withMessageSpan` (`telemetry.ts:80-118`) — the span that continues the producer's trace
from `ce-traceparent`, `SpanKind.CONSUMER`, all the messaging attributes — is implemented, unit
tested, and **never called** outside `telemetry.test.ts`. Consequences:

- No consumer span is ever created; the `werken.decode` and `werken.handler` child spans
  (`pipeline.ts:148-150, 163-165`) come out parented to whatever ambient context exists — usually
  root, disconnected from the producer's trace.
- `recordOutcome` is only ever called for `"skipped_duplicate"` (`pipeline.ts:133`). The
  `werken.messages.outcome` metric never counts `ack`, `nack` or `dead-letter`.

`WerkenPubSubTransport.processTraced` (`transport.ts:306-315`) is the natural seam — the name
suggests that was the intent: wrap `this.pipeline.handle(message)` in `withMessageSpan` (the
envelope is parsed inside the pipeline, so either hoist a cheap attribute read, or move the span
into `MessagePipeline.run` right after `parseEnvelope`) and call `recordOutcome` with the returned
outcome.

This is also a documentation-parity failure — see §Docs below: the README's Observability section,
the pipeline diagram ("open telemetry span parented on ce-traceparent"), and migration.md's
"traceparent extraction and span creation — Automatic" all describe behaviour that does not
currently happen.

### 3. Test harness: every default-ID event after the first is silently swallowed as a duplicate

`harness.ts:176` generates the event ID as `` emitOptions.id ?? `harness-${sequence + 1}` `` — but
never increments `sequence`. The `++sequence` fallback in `push` (`harness.ts:139`) is unreachable
from `emit`, because `emit` always sets `ce-id`. So every `emit()` without an explicit `id` gets
`ce-id: "harness-1"` from the same default source.

The harness installs an `InMemoryIdempotencyStore` by default (`harness.ts:125`), so the second
default-ID `emit()` in a test is skipped as a duplicate: it lands in `harness.acked`, and the
handler never runs. For a harness whose selling point is faithfulness, "emit A, emit B, assert both
handled" failing mysteriously on B is exactly the wrong kind of surprise. The existing harness
tests only ever emit once per harness instance, which is why this went unnoticed — add a
two-events test alongside the fix.

---

## Medium severity

### 4. Late ack after drain can crash the process via unhandled rejection

During `close()`, handlers still running at the timeout get their messages `nack()`ed
(`transport.ts:214-223`) and the subscription is closed — but the handler promise keeps running.
When it eventually finishes, `processTraced` calls `message.ack()`/`nack()` a second time
(`transport.ts:310-314`). If the SDK throws on settling an already-nacked message on a closed
subscription, the exception propagates through `handleMessage` (which has `try/finally` but no
`catch`, `transport.ts:290-294`) into the `void this.handleMessage(message)` fire-and-forget at
`transport.ts:180` — an unhandled rejection, which is fatal in Node by default. Guard the settle
calls, or have `handleMessage` catch and log.

### 5. `close()` removes the `error` listener before closing the stream

`this.subscription?.removeAllListeners()` (`transport.ts:201`) also removes the `"error"`
listener. A real `Subscription` is an `EventEmitter`; an `error` emitted in the window before
`subscription.close()` completes has no listener and throws, crashing the process during shutdown.
Remove only the `"message"` listener (or keep an error listener attached until after `close()`).
Related: `emit("error")` with no user-registered listener (`transport.ts:317-321`) drops broker
errors silently — consider logging them by default.

### 6. `PatternRouter.resolved` cache is unbounded

`pattern-router.ts:38` caches every distinct `ce-type` seen, including misses, forever. `ce-type`
is producer-controlled input, so a misbehaving (or hostile) producer publishing unique types grows
this map without bound. The schema cache next door is LRU+TTL bounded (`schema/cache.ts`); give
this cache the same treatment, or only cache when wildcard routes exist (the exact-map lookup is
already O(1) — the cache only amortises wildcard scans).

### 7. Publisher constructs a new `Topic` per publish, defeating SDK batching

`publisher.ts:93` calls `client.topic(topicName, ...)` on every publish. Each call returns a fresh
`Topic` with its own publisher and batching state, so the SDK's message batching never engages and
per-message overhead is paid every time. Cache the `Topic` per resolved
`(topicName, messageOrdering)` pair. Same applies to `PubSubDeadLetterPublisher.publish`
(`dead-letter.ts:92`), though DLQ volume rarely matters.

### 8. `publishBatch` semantics: sequential, and partial failure is unreported

`publisher.ts:105-111` awaits each publish in series. Sequential may be a deliberate choice for
ordering, but when `ordering` is off it costs a full round-trip per event; and if request _n_
throws, the caller has no way to know requests `0..n-1` were already published. Either document
"sequential, throws on first failure, prior messages are out" or return per-item results. Nothing
in the README describes `publishBatch` at all.

---

## Low severity

- **Terminal failures skip the handler-duration metric.** `recordHandlerDuration` runs on success
  (`pipeline.ts:166`) and on non-terminal failure (`pipeline.ts:184`) but not on the terminal path
  (`pipeline.ts:170-183`). The slowest failures vanish from the histogram.
- **`idempotencyKeyToString` is ambiguous under spaces** (`idempotency.ts:29-31`): space-joined
  `consumer/source/id` means `("a b","c")` and `("a","b c")` collide. `ce-source` is a
  URI-reference so it's unlikely in practice; a non-printable separator (`\u001f`, the ASCII unit separator) closes it
  cheaply. The README's Redis/Mongo examples use this string as the stored key, so changing it
  later is a breaking change — better now than after 1.0.
- **Root `package.json` carries runtime `dependencies`** (`avsc`, `uuidv7`) duplicating the
  package's own. The root is private; they belong only in
  `packages/nestjs-google-pubsub/package.json`. Looks like an accidental `pnpm add` at the root.
- **Harness: `emit()` after `drain()` hangs forever.** `drain()` removes the subscription
  listeners, so the next `push` emits into the void and its `settled` promise never resolves. A
  guard that throws "harness is drained" would turn a hung test into a clear failure.
- **Harness `pending` is a single shared slot** (`harness.ts:93,144`): concurrent `emit()`s (not
  awaited) would misattribute dead-letters. Fine for a sequential harness — say so in the JSDoc.
- **`isHealthy()` ignores stream errors** (`transport.ts:258-260`): a subscription in a permanent
  error state still reports healthy as long as `subscription` is set. Consider flipping on
  fatal `error` events.

---

## Design observations

Things that are right and worth keeping:

- The **decode-after-dedupe ordering** and **record-after-handler-before-ack** ordering are both
  correct for at-least-once, and the comments explaining the irreducible windows are exemplary.
- The **SQL upsert with `DO UPDATE ... WHERE expires_at <= now()`** is correct against the
  expired-marker trap that `DO NOTHING` would create; verified against the DDL in
  `docs/idempotency-schema.sql` (columns and PK match the statements).
- The **revision-keyed, single-flight, failure-uncached schema cache** is exactly the right shape.
- **`TerminalEventError extends RpcException`** and the structural Observable `settle()` are the
  two places a naive transport loses messages; both are handled and covered by tests.
- The `Duration` handling (SDK's own class or nothing, `transport.ts:151-163`) shows the spike
  work paid off.

Suggestions:

- `unwrap()` returns the `Subscription` (`transport.ts:247-252`). Nest's convention for
  `unwrap()` on other transports is the underlying _client_; returning the subscription is
  defensible (it's the thing the transport drives) but worth an explicit JSDoc note that this
  deviates, since the consumer might expect the `PubSub` instance.
- `resolveIdempotencyStore` rejects `store` + `executor` together at construction — good — but
  `idempotency.consumer` is typed required while the whole `idempotency` block is optional, and
  the pipeline silently falls back to `consumer: "werken"` (`pipeline.ts:85`). A shared store with
  two consumers that both defaulted to `"werken"` dedupes _across_ services — the exact failure
  the key design guards against. Consider warning (or failing) when a real store is configured
  without an explicit consumer.
- `DEFAULT_IDEMPOTENCY_TTL_MS` is used by the pipeline but not exported from the package barrel,
  while its sibling defaults all are (`index.ts:14-21`).

---

## Documentation quality and parity

The writing itself is excellent — the README's failure-mode framing ("this one thing get right",
the `rowCount` contract, the resource-prefix rationale) is better than most mature libraries.
Parity is where it slips:

1. **Observability is oversold** (consequence of finding #2). README's Observability section
   ("One CONSUMER span per message … continuing producer's trace"), the feature bullet, the
   pipeline diagram stage "open telemetry span parented on ce-traceparent", and migration.md's
   "traceparent extraction and span creation — Automatic, from `ce-traceparent`" all describe
   unwired behaviour. `werken.messages.outcome` is documented as a metric but only counts
   duplicates. Fix the code (preferred) — the docs are the right spec.
2. **Source comments point at files the repo deliberately excludes.** `.gitignore` excludes
   `docs/spikes/` and `docs/spec-amendments.md` as "working notes, not shipped artefacts", yet
   shipped source directs readers there: `transport.ts:36`
   (`docs/spikes/nest-11-transport-typings.md`), `schema/attributes.ts:5` and `options.ts:48`
   (SPIKE-0), `avro-codec.ts` and `publisher.ts:38` (SPIKE-1). Anyone cloning the repo finds
   dangling references. Either ship the spikes (they're good — they're the evidence for the
   library's most surprising claims) or inline the one-paragraph conclusion at each call site.
3. **`§x.y` references are unresolvable.** Dozens of comments cite spec sections
   (`§4.4`, `§5.6`, …) and `docs/spec-amendments.md` opens with "The implementation spec lives
   outside this repo". For an open-source repository that's a private citation system. Ship the
   spec, or replace each `§` with the requirement stated in place.
4. **Stale milestone references.** `pipeline.ts:32` — "Exact-match handler lookup. Wildcard
   precedence arrives in M10" (wildcards shipped); `pipeline.ts:51-55` — the "Ordered stages
   (§5.2). M4 covers…" JSDoc is attached to `DEFAULT_IDEMPOTENCY_TTL_MS`, which it does not
   describe; `harness.ts:45-49` — "`strict` (M5) will decode against local .avsc files" promises
   a harness mode that doesn't exist though schema support shipped in the transport.
5. **Misattached JSDoc.** `pipeline.ts:190-194` — the (excellent) comment about when idempotency
   is recorded sits on `decode()`; it describes `record()`.
6. **No per-package READMEs.** `packages/cloudevents` and `packages/nestjs-google-pubsub` have
   none, and `files` only ships `dist` — the npm pages for both packages will be blank, and the
   root README's package table links into directories with no landing page. Even a short README
   per package (what it is, link to the root) fixes both.
7. **`publishBatch` is undocumented** (see finding #8).

## Testing

The unit/integration split is disciplined and the integration matrix (emulator with schemas, real
Postgres, SIGTERM against a real process) is well beyond typical. Gaps, in priority order:

- Nothing exercises the **built artefacts under plain Node** — the gap that hides finding #1. One
  ESM and one CJS smoke test over `dist/` would close it.
- No test drives the **pipeline with telemetry attached** and asserts spans/metrics come out the
  other side — the gap that hides finding #2 (telemetry is only tested in isolation).
- No harness test emits **two events**, — the gap that hides finding #3.
- `transport.close()` during/after handler completion (the double-settle path, finding #4) is
  untested; the sigterm integration test covers the happy drain.
