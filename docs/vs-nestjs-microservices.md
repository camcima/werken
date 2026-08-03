# Werken vs. Nest's microservices abstraction

Werken is a `CustomTransportStrategy`, so it lives _inside_ Nest's microservices abstraction rather
than replacing it. You still get controllers, dependency injection, `@EventPattern`, `@Payload()`,
`@Ctx()`, guards, interceptors and pipes.

What differs is everything about the wire format and the delivery contract. This document explains
each difference, and — where the divergence exists because Nest's default behaviour is actively
dangerous for an event consumer — shows the behaviour that forced it.

---

## Summary

|                    | Nest default (`ClientProxy` + built-in transports) | Werken                                                         |
| ------------------ | -------------------------------------------------- | -------------------------------------------------------------- |
| Wire format        | `{ pattern, data }` — a Nest-shaped envelope       | CloudEvents 1.0 binary content mode                            |
| Routing key        | `pattern` field **inside** the payload             | `ce-type` **attribute**, payload untouched                     |
| Interoperability   | Nest producers and consumers only                  | Any CloudEvents producer or consumer                           |
| Pattern matching   | Exact string match                                 | Exact, suffix wildcard, catch-all, with precedence             |
| Duplicate patterns | Chained; only the first runs                       | Fails at startup                                               |
| Handler outcome    | Errors logged, message acked                       | return = ack, throw = nack, `TerminalEventError` = dead-letter |
| Error fidelity     | Replaced with `Internal server error`              | Preserved, including structured detail                         |
| Redelivery control | Per-transport, mostly absent                       | Explicit nack with backoff                                     |
| Dead-lettering     | Subscription retry policy, if any                  | Explicit topic with provenance attributes                      |
| Schema handling    | None                                               | Avro resolution by writer revision                             |
| De-duplication     | None                                               | Pluggable idempotency store                                    |
| Tracing            | None                                               | W3C context continued from `ce-traceparent`                    |
| Shutdown           | `close()`, no in-flight tracking                   | Bounded drain, nack the remainder                              |

---

## 1. The wire format

This is the difference everything else follows from.

`ClientProxy.emit(pattern, data)` dispatches `{ pattern, data }` — verified in
`client-proxy.js`, which calls `this.dispatchEvent({ pattern, data })`, and in the `ReadPacket`
interface, which is literally `{ pattern: any; data: T }`.

So a message published by Nest looks like this on the wire:

```json
{ "pattern": "order.placed", "data": { "orderId": "abc" } }
```

The routing key is **inside the payload**. Consequences:

- **Only Nest can read it.** A Java consumer, a Python job, a BigQuery subscription or a Dataflow
  pipeline all have to know about Nest's envelope shape to find the payload.
- **The framework is on the contract.** §1.5 requires this library to be handed to another team and
  reused unmodified. A payload shape named after the framework that happened to produce it is
  precisely the kind of coupling that makes a handover painful.
- **There is no metadata channel.** Trace context, occurrence time, schema identity and event id
  have nowhere to live except inside the business payload.

Werken uses CloudEvents 1.0 **binary content mode**: envelope attributes go in the Pub/Sub message
_attributes_, and the body is the payload and nothing else.

```
attributes:
  ce-specversion: "1.0"
  ce-id:          "01931b7c-3f2a-7000-8000-000000000001"
  ce-source:      "https://example.com/orders"
  ce-type:        "com.example.order.placed.v1"
  ce-time:        "2026-08-03T10:00:00.000Z"
  ce-traceparent: "00-4bf9…-00f0…-01"
body:
  {"orderId":"abc"}
```

Any CloudEvents-aware consumer reads it. The body stays exactly what the producer meant to send,
which is also what makes Avro schema validation on the topic possible at all.

## 2. Routing and pattern matching

Nest's `Server.getHandlerByPattern` is an exact `Map` lookup on the normalized pattern string. There
is no wildcard support, and no notion of one pattern being more specific than another.

Werken overrides handler resolution with a router supporting three shapes:

| Pattern                       | Matches                       |
| ----------------------------- | ----------------------------- |
| `com.example.order.placed.v1` | exactly that type             |
| `com.example.*`               | one or more trailing segments |
| `*`                           | anything                      |

Exactly one handler runs per message: exact beats wildcard, longest literal prefix wins among
wildcards, catch-all last — independent of registration order. The resolved map is built once at
`listen()`, so patterns are never rescanned per message.

### Duplicate patterns: a silent failure Nest permits

`Server.addHandler` treats event handlers specially:

```js
if (this.messageHandlers.has(normalizedPattern) && isEventHandler) {
  const headRef = this.messageHandlers.get(normalizedPattern);
  const getTail = (handler) => (handler?.next ? getTail(handler.next) : handler);
  getTail(headRef).next = callback; // ← chained, not replaced
}
```

Two controllers declaring the same `@EventPattern` therefore both register, and the second is
reachable only by walking `.next`. A transport that resolves a handler and invokes it — as Werken
does, because §4.5 requires exactly one handler per message — runs the first and silently ignores
the second.

Werken refuses to start instead, naming the pattern. A handler that never runs is far harder to
notice than a boot error.

## 3. Handler outcomes — the dangerous default

For an event handler, Nest's generic abstraction has no ack/nack concept at all. Its contract is
"call the handler"; what happens to the broker message afterwards is each transport's business, and
most built-in transports simply acknowledge on receipt.

Werken makes the outcome the handler's return value:

```ts
return; // ack
throw new Error("transient"); // nack → redelivered with backoff
throw new TerminalEventError(reason); // dead-lettered immediately, original acked
```

Getting that to work correctly required working around two behaviours of the abstraction that are
actively hostile to it. Both are documented here because they are invisible until they cost you a
production incident, and because anyone writing their own Nest transport will hit them.

### 3a. Handlers return Observables, and awaiting one is a no-op

Nest wraps controller methods so the registered handler returns an `Observable`, not the method's
own return value. An `Observable` is not a thenable, so:

```ts
await handler(data, ctx); // resolves immediately, ALWAYS
```

A transport written the obvious way therefore treats every message as successful the instant the
handler is invoked — **a throwing handler gets its message acked and lost**, and async work is never
waited for.

Werken subscribes and waits for completion instead. The detection is structural, so `rxjs` stays a
transitive concern of `@nestjs/microservices` rather than a runtime dependency here.

### 3b. `RpcExceptionsHandler` destroys your error

Any thrown error that is not an `RpcException` is replaced, before the transport ever sees it, with:

```json
{ "status": "error", "message": "Internal server error" }
```

The original class, message and any structured detail are gone. For Werken this was fatal:
`TerminalEventError` — the entire mechanism for "this will never succeed, dead-letter it now" —
would arrive as an anonymous object, silently degrading every terminal failure into a plain nack and
an infinite retry loop. Behind an ordering key that stalls every subsequent event for the entity.

`TerminalEventError` therefore **extends `RpcException`**, because an `RpcException`'s payload is
passed through untouched. This is load-bearing, not stylistic; the class carries a comment saying so
because it looks removable.

The practical lesson for consumers: if you need an error's identity to survive to the transport,
it must be an `RpcException`.

## 4. Publishing

Werken does not use `ClientProxy`.

- Its `send()`/`emit()` split is request/response-shaped, which is a poor fit for fire-and-forget
  domain events.
- Its serializer wraps the payload, per §1.
- It has nowhere to put an event id, occurrence time, ingestion time, trace context or ordering key.

`EventPublisher` is purpose-built instead: it generates a time-ordered UUIDv7 `ce-id` (which becomes
the idempotency table's primary key, so ordering keeps index inserts local), stamps `ce-time` and
`ingestiontime` separately, lifts `traceparent` from the ambient OpenTelemetry context, derives the
ordering key from `subject` when `ordering: true` is set, encodes the payload, and resolves the
destination topic from the event type.

## 5. What Nest gives you that Werken keeps

Werken is a transport strategy, not a fork. Unchanged:

- Controllers, providers, modules and full dependency injection
- `@EventPattern`, `@Payload()`, `@Ctx()`
- Guards, interceptors, pipes and exception filters
- `app.enableShutdownHooks()` driving the drain
- `Server`'s `status` observable, which the transport pushes `connected`/`disconnected` onto

`@Ctx()` yields a `CloudEventContext` — the envelope plus delivery metadata — rather than a
transport-specific context object.

`isHealthy()` is Werken's own addition, for Cloud Run worker pools that have no HTTP endpoint to
probe. It reads the subscription's `isOpen` directly rather than the `status` observable, so it goes
false as soon as the SDK gives up on the stream — the observable only moves on an explicit start or
close. Prefer the observable when you want to react to changes rather than poll.

## 6. When the default abstraction is the better choice

Werken is not a general-purpose improvement. Prefer plain Nest microservices when:

- **Both ends are Nest and always will be.** The envelope costs nothing if nothing else reads it.
- **You want request/response.** Werken is one-way by design; `ClientProxy.send()` has no equivalent.
- **You are not on Pub/Sub.** Werken is deliberately Pub/Sub-specific (§1.4). The handler-facing
  contract is broker-neutral, but there is no Kafka adapter and building one is a non-goal until a
  second broker is actually required.
- **Events are internal and low-stakes.** Schema resolution, idempotency and dead-lettering are real
  configuration surface. If duplicates and lost messages are genuinely acceptable, this is overhead.

Werken earns its keep when events cross team or language boundaries, when duplicate processing has a
cost, and when someone will eventually need to answer "what happened to this event?" from a trace.

---

## Appendix: verifying these claims yourself

Everything above was checked against the installed source rather than recalled, and the notable
findings have regression tests. The relevant code:

| Claim                                                 | Where                                                                              |
| ----------------------------------------------------- | ---------------------------------------------------------------------------------- |
| Payload is wrapped as `{ pattern, data }`             | `@nestjs/microservices/client/client-proxy.js`, `interfaces/packet.interface.d.ts` |
| Duplicate event handlers are chained                  | `@nestjs/microservices/server/server.js`, `addHandler`                             |
| Handlers resolve to Observables                       | `tests/pipeline.test.ts`, "Observable-returning handlers"                          |
| Non-`RpcException` errors are replaced                | `tests/harness.test.ts`, terminal-failure cases                                    |
| `CustomTransportStrategy` needs only `listen`/`close` | `interfaces/custom-transport-strategy.interface.d.ts`                              |
