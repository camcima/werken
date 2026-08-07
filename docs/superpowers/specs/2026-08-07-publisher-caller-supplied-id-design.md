# Caller-supplied `ce-id`, `ingestiontime`, and ordering-key recovery — design

- **Date:** 2026-08-07
- **Status:** implemented in [#4](https://github.com/camcima/werken/pull/4)
- **Baseline:** v0.3.0 (`b47ae48`)

## Problem

Werken cannot serve a producer using the transactional outbox pattern, and it fails at that
silently. A second, unrelated defect in the same file makes an ordering key unusable for the
lifetime of a publisher after one failed publish.

### A relayed event cannot keep its identity

The outbox pattern writes a service's state change and an outbox row in one transaction, then has a
separate relay claim unpublished rows, publish each, and mark them published. Pub/Sub and Postgres
cannot commit together, so the order is deliberately publish-first, mark-second: that guarantees
duplicates and forbids loss, which is the correct direction. Duplicates are made harmless by
generating the CloudEvents `id` inside the original transaction and storing it on the row, so a
republish after a relay crash carries _the same_ `ce-id` and consumer de-duplication collapses the
two into one.

`PublishRequest` has no `id`. `publishOne` hardcodes `id: uuidv7()`. Consumer-side dedup keys on
exactly that field — `IdempotencyKey` is `{ consumer, source, id }`, and `MessagePipeline` derives
it via `keyFor(envelope.source, envelope.id)` (`src/pipeline.ts:134`). A relay that crashes between
publishing and marking therefore republishes under a fresh `uuidv7`, Werken's own idempotency store
does not suppress it, and consumers see two distinct events for one state change with no error
anywhere. This is the precise failure the outbox pattern exists to prevent.

It is also internally inconsistent: the library ships a Postgres idempotency store whose docstring
describes a "Mode B, transactional inbox" direction (`src/idempotency.ts:91`), which presumes
outbox-shaped producers — but the publisher cannot be one.

### `ingestiontime` is pinned to publish time

`publishOne` sets `ingestiontime` to `now()` unconditionally. `CloudEventEnvelope.ingestiontime` is
documented as when the platform learned of the event, as opposed to when it happened. For a relayed
event that is wrong: the platform learned of it when the ingest transaction committed, strictly
before the relay publishes, and arbitrarily earlier when the relay stalls. Pinning it to publish
time folds the acquire-to-commit interval away, which is exactly what a per-stage lead-time SLI
needs to decompose.

### A failed keyed publish silences that key permanently

Google's documentation is explicit: on a non-retryable error the client library stops publishing
_all_ subsequent messages with that ordering key, and "you must resume publishing ordering keys
[when] failures occur" via `resumePublishing(orderingKey)` in Node. Werken never calls it, and
`TopicLike` (`src/options.ts:49`) does not even declare it. One failed publish on a key therefore
takes that key out of service until the process restarts, and every later publish on it rejects.
Leaving the key suspended is not the safer default: it trades a recoverable ordering hazard for
permanent silence.

## Decisions

Recorded with the reasoning, because each closed off a plausible alternative.

### `resumePublishing` is optional on `TopicLike`

`TopicLike` is a public export and callers pass their own implementations through `createClient` and
their own fakes in tests. A required method would break every one of them, and the constraint on this
work is additive-only. Optional costs one `?.` at the call site and means an older client keeps
today's behaviour — the key stays suspended — rather than failing to compile.

The interface's docstring currently says "used for dead-letter publishing", which is already stale
because the publisher uses it too. Corrected in the same edit.

### The resume happens after a batch settles, not inside `publishOne`'s catch

`publishBatch` issues every publish before awaiting any, so a whole batch is queued on a key before
any failure is observable. Resuming from the failing message's own catch handler would lift the
suspension while its batch-mates are still notionally in flight on that key, and a later message
could then go out ahead of the one that failed — the exact inversion `ordering` was enabled to
prevent.

Rejected alternative: resume in `publishOne`'s catch, which is roughly four lines and covers both
entry points identically. It is correct against today's SDK, whose `OrderedQueue` rejects every
queued message synchronously when the first fails, so the batch-mates are already dead by the time
the catch runs. That is an undocumented internal with no guarantee behind it, and the failure mode
if it changes is a silently overtaking message. The repository already writes defensively against
this class of assumption — see the `inFlight` queue comment at `src/testing/harness.ts:116`, which
guards a collision that "currently resolves correctly anyway".

`publish()` publishes exactly one message, so its own catch _is_ after-settle; only `publishBatch`
needs the deferred path.

### An empty `id` is rejected, not defaulted

Falling back to `uuidv7()` when `id` is present-but-empty would recreate the exact bug this change
fixes, invisibly. A caller who believes they pinned a stable id and did not is worse off than one
who never tried.

There is no internal error to reword: `toPubSubAttributes` performs no validation and writes
`attributes["ce-id"] = envelope.id` verbatim (`packages/cloudevents/src/to-attributes.ts:19`), so an
empty id ships as `ce-id: ""` and is first caught at the _consumer_, as an
`EnvelopeValidationError("missing-attribute")` that blames the wrong side of the wire. The publisher
is the only place the check can name the caller's mistake.

### Validation stops at empty; no length cap

Rejected alternative: also reject an id longer than Pub/Sub's 1024-byte per-attribute-value limit.
The bug being fixed is _silent_ divergence, and an oversize id is not silent — it fails at publish.
`subject`, `dataschema` and every `extensions` value are equally attribute-bound and equally
unchecked today, so capping only `id` would be an inconsistency without a matching payoff. The
ceiling is documented instead.

CloudEvents 1.0 requires `id` to be unique per `source`. Uniqueness is not enforced — a reused id is
indistinguishable from a redelivery to every consumer downstream — and the property's own docstring
says so.

### No change to `@werken/cloudevents`

`CloudEventEnvelope` already declares `readonly id: string` and `readonly ingestiontime?: Date`. The
gap was only that the publisher exposed no way to set them.

### The CHANGELOG is not hand-edited

`CHANGELOG.md` is generated by `@release-it/conventional-changelog` from conventional commits
(`.release-it.json`), and `CONTRIBUTING.md:48` documents `pnpm run release` as the thing that writes
it. `.prettierignore` excludes it outright, with the comment "Generated by
@release-it/conventional-changelog; reformatting it churns every release". The long explanatory
entries under 0.2.0 are commit bodies, not prose typed into the file. This work therefore carries
its changelog in commit messages written to that standard; a hand-added `Unreleased` section would
be duplicated by the generator at bump time.

## Shape

Four commits on one branch, ordered so each is true when it lands — in particular the documentation
commit lands only after the API it describes exists.

### 1. `fix(pubsub): resume publishing on an ordering key after a failed publish`

**`src/options.ts`** — `TopicLike` gains `resumePublishing?(orderingKey: string): void`, with a
docstring saying what its absence costs. Stale "used for dead-letter publishing" line corrected.

**`src/publisher.ts`**:

- Topic-name and ordering-key derivation, currently inline in `publishOne`, moves to a
  closure-scoped `destinationFor(request, publishOptions)` returning `{ topicName, orderingKey }`.
  The recovery path needs both and only `publishOne` could compute them.
- `publishOne` gains an internal third parameter: a callback it invokes with the destination it is
  about to publish on, immediately before `publishMessage`. The public `publish` wraps it so the
  sink never reaches the exported signature, and resumes that recorded destination from its own
  catch. Rejected alternative: duplicate the publish call inside `publishBatch`.
- `publishBatch` collects one destination per index, then after `allSettled` resumes each distinct
  `(topicName, orderingKey)` among the rejected indices, once each. Distinctness is keyed on
  `JSON.stringify([topicName, orderingKey])` — same reasoning as `idempotencyKeyToString` and the
  harness `correlationKey`, both of which already refuse a delimiter because either component could
  contain it.
- Recorded rather than recomputed from the failed request. Several failure modes — an unresolvable
  topic, an empty `id`, a throwing `encode`, a datacontenttype that is not one — reject before
  `publishMessage` and so queue nothing under a key, and the SDK's resume is not a no-op on a key
  it never touched: it drains a queue whose only batch is already in flight, deleting it, so the
  next publish on that key races an outstanding RPC. Recording also removes any dependence on
  `topicResolver` being deterministic.
- `resumeOrdering` swallows whatever `client.topic()` or `resumePublishing` throws. Both are
  caller-supplied, and recovery that propagates replaces the outcome it is recovering from — the
  caller loses `PartialPublishError.published` and an outbox relay rethrows, rolls back and
  republishes everything that already went out.
- Resume is attempted whenever a non-empty ordering key was attached, `ordering` on or off. The SDK
  selects its `OrderedQueue` on the message's ordering key alone, not on `messageOrdering`, so a key
  passed explicitly with `ordering: false` still suspends on failure and still has to be resumed.

### 2. `feat(pubsub): let callers supply the CloudEvents id`

`PublishRequest` gains `id?: string`; `publishOne` uses `request.id ?? uuidv7()`. A guard before the
envelope is built throws when `id` is supplied but `trim()`s to empty, naming `PublishRequest.id`
and saying that omitting it generates one.

### 3. `feat(pubsub): let callers supply ingestiontime`

`PublishRequest` gains `ingestiontime?: Date`; `publishOne` uses `request.ingestiontime ?? at`. The
existing test `"always stamps ingestiontime at publish time"` is renamed to
`"defaults ingestiontime to publish time"`, because "always" stops being true.

### 4. `docs: document the outbox relay, ordering recovery and extension sizing`

Root `README.md` only; the package README defers to it.

- **`### Ordering`** — the suspension caveat, loudly. A non-retryable failure on a key stops all
  subsequent publishes on that key; Werken now resumes it; and therefore **retry the failures before
  publishing anything further on that key**, because a resumed key will carry a later message past
  the one that failed. An outbox relay gets this right for free by claiming in id order; an ad-hoc
  caller will not.
- **`### Batches`** — resume happens after the batch settles, and what that means for retry order.
- **New `### Transactional outbox`**, placed after `### Batches` because it builds directly on
  `publishBatch` and `PartialPublishError`. Worked example: generate the `ce-id` in the ingest
  transaction, claim rows with `FOR UPDATE SKIP LOCKED`, `publishBatch`, catch `PartialPublishError`,
  and mark published by mapping `error.published[].index` back to rows — the index-based recovery
  that is the whole reason the error type exists and is currently undocumented in terms of a real
  use case. Notes that publish-first/mark-second is deliberate, and passes
  `ingestiontime: row.createdAt`.
- **New `### Extension attributes`** — verified against Google's published quotas: 100 attributes
  per message, keys at most 256 bytes, values at most 1024 bytes, none raisable by quota request,
  and separate from the 10 MB data limit. Werken's own envelope spends up to 11 of the 100, and the
  `ce-` prefix costs 3 bytes of every key budget. When structured metadata exceeds that, move it
  into the payload — and state the cost: a JSON string in one attribute cannot be matched by a
  Pub/Sub subscription filter, which is the capability binary content mode exists to provide.
- The "publisher generates a time-ordered UUIDv7 `ce-id`" paragraph updated to say both `id` and
  `ingestiontime` are now overridable.
- A cross-reference from **`### Mode B`** to the outbox section as its producer-side dual, without
  implying Mode B is unblocked. It is not.
- `## Public API` table is unchanged: `PublishRequest`, `PublishOptions` and `TopicLike` are already
  listed, and nothing new is exported. Both new fields land on an already-documented type.

## Verification

New `packages/nestjs-google-pubsub/tests/outbox-republish.test.ts` — the regression, end to end.
Publish the same request with the same explicit `id` twice through a fake-client publisher, assert
both messages carry an identical `ce-id`, then feed both captured attribute sets and bodies into a
test harness via `emitRaw` and assert the handler ran exactly once and both messages were acked.

Publisher output into harness input is what makes this end to end. `harness.emit` already accepts an
explicit id via `EmitOptions.id` (`src/testing/harness.ts:27`), so no harness change is needed — but
using it would only test the harness, not the publisher's new id path feeding the consumer's dedup
path.

Added to `tests/publisher.test.ts`:

- An explicit `id` round-trips to `ce-id`.
- An empty `id`, and a whitespace-only `id`, each throw naming `PublishRequest.id`.
- An omitted `id` still produces a valid uuidv7 (existing test, kept).
- `ingestiontime` round-trips when supplied, and defaults to publish time when not.
- `resumePublishing` is called with the key when a keyed publish rejects, and the original error
  still surfaces to the caller.
- It is not called when the request carries no ordering key.
- After a batch with several failures on one key, it is called once for that key, not once per
  failure.
- A `TopicLike` without `resumePublishing` does not crash the failure path.

Repository scripts, read from `package.json` rather than assumed, all run before reporting done:
`lint`, `lint:no-deep-imports`, `lint:dead-code`, `format:check`, `typecheck`, `test`, `build`.

## Non-goals

- No new dependencies.
- No breaking changes; every existing caller behaves identically. 0.x, so this is a minor bump.
- No change to the transport, or to the idempotency store's schema or semantics. Mode B stays
  unwired.
- No enforcement of CloudEvents `id` uniqueness per `source`.
- No length or count validation of `subject`, `dataschema` or `extensions` against Pub/Sub's
  attribute quotas — documented, not enforced.
- No refactoring beyond the `destinationFor` extraction that Task 0's recovery path requires.

Other gaps noticed while reading the repository are reported at the end of the work rather than
fixed here.
