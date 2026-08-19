# Implementation review — 2026-08-18

> **Status.** Everything in the priority table below except row 5 was fixed on
> `fix/code-review-2026-08-18`; see the table for the per-item outcome. Line references are to the
> tree as reviewed (v0.4.1), so they point at the code _before_ those fixes.

Scope: the full source of `@werken/cloudevents` and `@werken/nestjs-google-pubsub` (~3,400 lines),
their packaging and build pipeline, the test strategy, CI, and the examples. Reviewed at v0.4.1 on
`main` (clean tree). Findings are ranked by severity within each section; file references are
`path:line` against the current tree.

## Overall assessment

This is an unusually well-engineered library. The failure-mode analysis is the kind most teams
never write down: the Observable-swallows-errors trap in Nest is defeated and documented
(`pipeline.ts` `settle()`), the `RpcExceptionsHandler` error-replacement trap is defeated by making
`TerminalEventError` extend `RpcException`, the ordering-key suspension semantics of the Pub/Sub
SDK are handled with real care in `publisher.ts` (including the subtle pre-queue vs. broker-observed
failure distinction), and the idempotency check/record ordering is correctly reasoned about and
honestly documented as at-least-once. Fail-fast validation is applied consistently at startup, the
public API is deliberately narrow with internals reachable only by the repo's own tests, and the
dual ESM/CJS build is verified by smoke tests against the built artifacts — the failure class most
dual-build packages ship blind on.

The findings below are correspondingly narrow. Two are real production defects worth fixing before
this is relied on at scale (§1.1, §1.2); the rest are hardening, consistency, and design-symmetry
improvements.

---

## 1. Bugs and correctness risks

### 1.1 A transient Schema Service outage dead-letters messages by default — HIGH

`AvroCodec` carefully distinguishes _availability_ failures (the writer schema could not be
fetched — modelled as `WriterSchemaUnavailableError`, `avro-codec.ts:140`) from _correctness_
failures (missing reader type, unresolvable writer, invalid definition). But that distinction dies
at the pipeline boundary: every decode failure funnels into one policy,
`validation.onDecodeFailure ?? "dead-letter"` (`pipeline.ts:217`).

Consequence with default configuration and `strict: true` (also the default): if the Pub/Sub
Schema Service is unreachable for long enough to exhaust the SDK's internal retries, every
in-flight message is **dead-lettered** — a terminal outcome requiring manual redrive — for a
failure that would have healed itself on redelivery. The library gets the equivalent case right for
idempotency: an unreadable idempotency store nacks, precisely because "treating an unreadable store
as 'not seen' would reprocess every message during an outage" (`pipeline.ts` `dispatch`). The same
philosophy — infrastructure unavailability is transient, so redeliver — should apply to schema
fetches.

Compounding it, `WriterSchemaUnavailableError` is `class`-private (not exported from `index.ts` or
even `internal.ts`), so a consumer cannot detect or policy-route this case at all. Their only lever
is `onDecodeFailure: "nack"`, which then retries genuinely-undecodable poison messages forever.

**Recommendation:** in `MessagePipeline.dispatch`, branch on the unavailability case and nack
(retry) instead of applying `onDecodeFailure`; optionally expose it as its own policy
(`onSchemaUnavailable`, default `"nack"`). Export the error type either way.

### 1.2 Dead-letter provenance attributes are not bounded, so the DLQ publish itself can fail — MEDIUM-HIGH

`serialiseDetail` (`dead-letter.ts:128–145`) correctly enforces Pub/Sub's 1,024-byte attribute
value cap for `werken-dl-detail` — and its comment even records that the emulator does not enforce
the cap, so tests cannot catch violations. But `werken-dl-reason` (`dead-letter.ts:108`) has no
such bound, and reason strings embed uncontrolled content:

- Envelope validation errors interpolate the raw attribute value:
  `` `ce-time must be an RFC 3339 timestamp, got ${JSON.stringify(raw)}` ``
  (`parse-envelope.ts`). An attribute value can itself be up to 1,024 bytes, so the reason exceeds
  the cap with room to spare.
- `TerminalEventError.reason` is application-controlled and frequently wraps upstream error text.
- Decode failure reasons carry `asMessage(error)` from arbitrary causes.

Consequence: against real Pub/Sub (not the emulator), the dead-letter publish is rejected, the
pipeline maps that to nack (`pipeline.ts` `reject`), and the message redelivers into the same
failure — an indefinite retry loop for **exactly the poison messages the dead-letter path exists to
drain**. A subscription-level retry policy eventually catches it if one is configured; without one
it loops until retention expires.

**Recommendation:** byte-truncate `werken-dl-reason` (UTF-8-aware, as `serialiseDetail` does) to
fit the cap; consider also guarding the 100-attributes-per-message limit, since the original
message's attributes are forwarded verbatim plus up to six `werken-dl-*` additions.

### 1.3 The fire-and-forget message path has no terminal catch — MEDIUM

`listen()` wires `void this.handleMessage(message)` (`transport.ts:215`), and
`handleMessage` → `processMessage` → `processTraced` catches errors from ack/nack and from the
handler — but **not** from everything else on the path. `pipeline.handle()` can reject via code the
library does not control: a user-registered `MeterProvider` whose counter `add()` throws, a tracer
that throws in `startActiveSpan`, or a logger injected via Nest that throws. Any such rejection
propagates out of the `void`-ed promise as an unhandled rejection, which by default terminates the
Node process — the exact failure `processTraced`'s own comment says the library must prevent
("an unhandled rejection out of the fire-and-forget message listener in listen() would take the
whole process down"). The message is also never settled.

**Recommendation:** wrap the body of `handleMessage` in a final `try/catch` that logs and nacks.
It costs three lines and converts a crash-the-worker failure into a redelivery.

### 1.4 `close()` leaks the client when `subscription.close()` throws, and double-close lies — LOW

In `close()` step 4 (`transport.ts:292`), `await this.subscription?.close()` and
`await this.client?.close()` run sequentially inside one `try`: if the subscription close throws,
the client — with its gRPC channels and credential refresh timers — is never closed (the `finally`
clears the references but closes nothing). `closePartialStartup` gets this right by wrapping each
close separately; `close()` should do the same.

Separately, a second concurrent `close()` returns immediately (`transport.ts:258`,
`if (this.draining) return`) while the first drain is still in flight, so a caller awaiting the
second call believes shutdown is complete when it is not. Storing and returning the in-progress
drain promise fixes it.

### 1.5 The in-flight metric is labelled with the unresolved subscription name — LOW

`processMessage` records `werken.messages.inflight` against `this.options.subscription`
(`transport.ts:374/378`) — the raw, unprefixed name — while `werken.messages.received`,
`werken.messages.outcome`, span attributes, and `CloudEventContext.subscription` all use the
prefix-resolved name that `listen()` passes into the pipeline. With `resourcePrefix` active (a
development scenario, but the library invests heavily in making that scenario non-confusing) the
in-flight series is labelled with a subscription that no other metric names.

### 1.6 `@MessagePattern` handlers are silently treated as event handlers — LOW

`listen()` feeds `this.getHandlers()` straight into `PatternRouter` (`transport.ts:154`). Nest's
`MessageHandler` carries `isEventHandler?: boolean` (verified in `@nestjs/microservices` 11.1.28),
and a `@MessagePattern` (request/response) handler registers in the same map. Werken routes it like
an event handler and discards its return value, silently. For a library whose signature move is
refusing ambiguous registrations at startup (`assertNotChained`, `AmbiguousPatternError`), a
`@MessagePattern` in a werken consumer deserves the same treatment: a startup error explaining
that this transport carries events only.

### 1.7 `parseEnvelope` accepts `T24:00:00Z` — NIT

The RFC 3339 pedantry in `parse-envelope.ts:30–70` is genuinely good — impossible calendar dates
are rejected rather than rolled — but the time-of-day is only vetted by `new Date(raw)` producing
`NaN`, and V8 accepts hour 24 (verified: `"2026-01-01T24:00:00Z"` parses and **rolls to the next
day**). RFC 3339 forbids hour 24, and silent day-rolling is precisely the failure class the
calendar-date check exists to prevent. A range check on hour/minute/second (the capture groups are
already there) closes it. The unhandled flip side: RFC 3339 _permits_ the leap second `:60`, which
`Date` rejects — acceptable, but worth a comment if it stays.

---

## 2. Design observations

### 2.1 The publish and consume sides disagree about payload formats

The publisher's `encode` hook is deliberately general: return `{ data, datacontenttype }` and the
event may be protobuf, CBOR, compressed — the README advertises this. The consumer, however, can
only decode two formats: Avro via `schemaRegistry`, or plain JSON (`MessagePipeline.decode`). There
is no public decode hook, and `ctx.datacontenttype` is never consulted before `JSON.parse`. So an
event published by werken's own publisher with a custom encoder **cannot be consumed by werken** —
it dead-letters at decode. The asymmetry is real: either the consumer grows a `decode` counterpart
(dispatching on `datacontenttype`, which the envelope already carries for exactly this purpose), or
the publisher docs should state plainly that non-JSON/non-Avro encodings produce events only
non-werken consumers can read.

### 2.2 The publisher validates `id` but not `type` or `source`

`prepareOne` rejects an empty caller-supplied `id` with a well-argued error, yet an empty `type` or
`source` sails through and produces `ce-type: ""` / `ce-source: ""` on the wire — an envelope every
werken consumer (and any compliant CloudEvents consumer) rejects as invalid, discovered only at the
far end of the pipe as dead-letters. The library's own philosophy — fail at the point of the
mistake, with the option path named — argues for the same guard on all three.

### 2.3 `currentTraceparent()` pays a failed module resolution on every publish

`publisher.ts:444` calls `optionalRequire("@opentelemetry/api")` per prepared message. When OTel
**is** installed, Node's require cache makes this cheap. When it is **not** — the exact case the
optionality exists for — Node does not cache failed resolutions, so every single publish walks the
`node_modules` chain and constructs a `MODULE_NOT_FOUND` error, on the hot path. `telemetry.ts`
already solved this with a module-level `cached` slot (`loadOtel()`); the publisher should do the
same.

### 2.4 The dead-letter publisher rebuilds its `Topic` per publish

`PubSubDeadLetterPublisher.publish` calls `this.client.topic(this.topic)` on every call
(`dead-letter.ts:96`). `publisher.ts` documents at length why this is wasteful — each `topic()`
call returns a fresh `Topic` with its own batcher, so batching never engages. Dead-letter volume is
normally low, so this is defensible, but during a poison-message storm (the one time the path is
hot) every publish pays full overhead. Caching one `Topic` in the constructor is a two-line fix and
makes the codebase agree with itself.

Related edge: a message consumed _from_ a DLQ subscription and dead-lettered again has its original
`werken-dl-*` attributes overwritten by the second failure's provenance. If redrive pipelines are
expected (the attributes exist to enable them), first-failure provenance may be worth preserving
under a `werken-dl-original-*` prefix or an attempt counter.

### 2.5 Subscription existence is only verified when a resource prefix is active

`startSubscribing` checks `subscription.exists()` only on the scoped-development path. The
justification there — "a consumer that sits there healthy and receives nothing" is worse than
failing startup — applies equally to a typo'd production subscription name: the SDK surfaces
NOT_FOUND as an `error` event, the transport logs and stays `connected`, and `isHealthy()` reports
true until the SDK eventually gives up the stream. There may be a deliberate reason (the check
costs `pubsub.subscriptions.get`, a permission consumers may not hold) — if so it deserves a
comment; if not, running the existence check whenever the client supports it would close the same
trap on the path where it matters most.

### 2.6 No producer-side telemetry

The consumer half is instrumented to an unusually high standard (seven metrics, spans continuing
the producer's trace). The publisher contributes a `traceparent` and nothing else: no PRODUCER
span, no publish counters or latency, no partial-batch metric. An outbox relay — the exact consumer
this publisher is designed for — will want publish failure rates and latencies from somewhere; parity
here is the obvious next telemetry investment.

### 2.7 Minor asymmetries worth a look

- **Clock injection**: `MessagePipeline` takes `now?: () => Date` for timestamps but measures
  handler duration with bare `Date.now()`; harness docs then have to explain why lateness cannot be
  tested through the harness clock. Injecting one clock consistently would simplify both.
- **Lateness under clock skew**: `recordLateness` can record negative seconds when the producer's
  clock runs ahead. Worth either clamping or documenting, since alerting on the distribution is the
  stated purpose.
- **`emitRaw` drops `EmitOptions`** (`harness.ts:241`): a raw-attribute emit cannot set
  `deliveryAttempt` or `orderingKey`, so redelivery-dependent behaviour cannot be tested for
  malformed-envelope cases — the very cases `emitRaw` exists to exercise.
- **`InMemoryIdempotencyStore` growth is unbounded** unless the consumer calls `prune()`; it is
  documented, but a max-entries bound (as the router and schema cache both have) would fit the
  house style of bounded-by-construction.

---

## 3. Documentation accuracy nits

For a codebase whose comments are this load-bearing, comment drift is worth flagging as a defect
class of its own:

1. **Misplaced JSDoc** — the "Recorded only after the handler succeeded and before the ack" comment
   (`pipeline.ts:258`) sits on `decode()`, but describes `record()`. Anyone skimming decode's
   contract reads the wrong contract.
2. **`SqlExecutor` claim** — "Both statements this store issues end in `RETURNING`"
   (`idempotency.ts:62`) is false for `has()`, which is a plain `SELECT`. The _contract_ ("rowCount
   = rows returned") still holds for a SELECT, so nothing breaks, but the stated justification does
   not match the code it justifies.

---

## 4. What is notably good (keep doing this)

- **Rationale-dense comments that explain _why not_, not just _why_** — the `resumeFailed`
  attempted-vs-recomputed destination reasoning, the `serialiseDetail` truncation rationale, and
  the `blockedEverywhere`/`blockedOnTopic` split are reference-quality.
- **Fail-fast everywhere it is cheap**: numeric option validation with named paths, ambiguous
  pattern rejection at startup, resource-prefix production guard requiring explicit opt-out,
  mutually-exclusive idempotency options rejected at construction.
- **The public/internal API split** — `internal.ts` absent from `exports`, aliased only for tests,
  deliberately _not_ a Knip entry so dead re-exports still get reported. This is the right way to
  keep a 0.x surface small without strangling the test suite.
- **Packaging discipline**: dual builds with a `{"type":"commonjs"}` marker, the `.cjs` escape
  hatch for `optionalRequire` with its rationale, `typesVersions` for node10 resolution of the
  testing subpath, and dist smoke tests that would catch all of it regressing.
- **Test strategy**: unit and integration suites with disjoint ownership of files, and
  `WERKEN_REQUIRE_INTEGRATION=1` turning silently-skipped integration tests into failures — this
  closes the most common CI lie in emulator-based suites.
- **Supply-chain hygiene**: actions pinned by SHA, least-privilege workflow permissions,
  osv-scanner, gitleaks, dependency overrides for advisories, and a non-blocking Nest 12 canary.
- **The in-process duplicate-delivery coalescing** (`MessagePipeline.inProcess`) with a clear
  statement of what it does and does not guarantee across replicas.

---

## 5. Recommended priorities

| #   | Item                                                                                    | Severity    | Outcome                                                      |
| --- | --------------------------------------------------------------------------------------- | ----------- | ------------------------------------------------------------ |
| 1   | Nack (not dead-letter) on writer-schema _unavailability_; export the error type (§1.1)  | High        | Fixed — new `validation.onSchemaUnavailable`, default `nack` |
| 2   | Byte-cap `werken-dl-reason`; consider the 100-attribute limit (§1.2)                    | Medium-high | Fixed — both, plus `werken-dl-dropped-attributes`            |
| 3   | Terminal catch around `handleMessage` (§1.3)                                            | Medium      | Fixed — logs and nacks, each recovery step guarded           |
| 4   | Per-close error isolation + idempotent awaited `close()` (§1.4)                         | Low         | Fixed — both                                                 |
| 5   | Decide the consumer-side story for non-JSON/non-Avro payloads (§2.1)                    | Design      | **Deferred** — needs an API decision, see below              |
| 6   | Publisher: validate `type`/`source`, cache the OTel module (§2.2, §2.3)                 | Low         | Fixed — cache now shared with `telemetry.ts`                 |
| 7   | Reject `@MessagePattern` handlers at startup; resolved-name inflight label (§1.6, §1.5) | Low         | Fixed — both                                                 |
| 8   | Doc drift and RFC 3339 hour-24 nits (§3, §1.7)                                          | Nit         | Fixed — both                                                 |

Also fixed alongside these: the dead-letter publisher now reuses one `Topic` (§2.4), and the test
harness's `emitRaw` accepts the same delivery options as `emit` (§2.7).

### Deliberately not done

- **§2.1, a consumer-side `decode` hook.** The asymmetry is real, but closing it means adding public
  API to a library that has been deliberate about keeping its surface small, and the shape is a
  genuine choice: a `decode` counterpart to `encode`, or dispatch on `ctx.datacontenttype`, or a
  documented statement that custom encodings are for non-werken consumers. That is the author's call
  to make, not a fix to slip into a defect PR.
- **§2.5, the subscription existence check in production.** Plausibly deliberate — it costs
  `pubsub.subscriptions.get`, a permission a consumer may not hold — so the right first step is
  confirming the intent, not changing the behaviour.
- **§2.6, producer-side telemetry.** A feature, not a defect.
- **§2.7, bounding `InMemoryIdempotencyStore`.** An LRU bound there would silently evict a live
  marker and reprocess an event, which is worse than the documented unbounded growth of a store
  whose stated scope is tests and single-instance consumers.

Nothing here challenges the architecture. The pipeline/transport/router/codec decomposition is
sound, the seams (structural SDK types, `IdempotencyStore`, `DeadLetterPublisher`, `SqlExecutor`)
are the right ones, and the two production-grade defects are both local fixes inside existing
abstractions.
