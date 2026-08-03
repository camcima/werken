# @werken/cloudevents

CloudEvents 1.0 envelope types, validation, and Pub/Sub attribute binding. Zero runtime
dependencies.

Part of [Werken](https://github.com/camcima/werken). Useful on its own if you speak CloudEvents
over Pub/Sub without NestJS — if you are writing a Nest consumer, use
[`@werken/nestjs-google-pubsub`](../nestjs-google-pubsub), which depends on this package and binds
envelopes for you.

```bash
npm install @werken/cloudevents
```

## What it does

CloudEvents in **binary content mode**: the envelope travels in Pub/Sub message attributes as
`ce-*`, and the message body is the domain payload, untouched. An envelope here therefore carries
no data — only the metadata around it.

```ts
import { parseEnvelope, toPubSubAttributes } from "@werken/cloudevents";

// Inbound: attributes -> envelope. Throws EnvelopeValidationError if they do not form a valid one.
const envelope = parseEnvelope(message.attributes);
envelope.type; // "com.example.order.placed.v1"
envelope.time; // Date | undefined — undefined when the producer omitted ce-time

// Outbound: envelope -> attributes.
const attributes = toPubSubAttributes(envelope);
```

Validation is deliberately strict where being lax goes wrong quietly:

- `ce-specversion` must be exactly `1.0`; anything else is rejected rather than best-guessed.
- Timestamps must be RFC 3339 with a real calendar date. `Date.parse` accepts locale-dependent
  forms like `"August 2, 2026"` and rolls `"2026-02-30"` over to March — admitting either into a
  field whose purpose is a globally comparable instant is how lateness arithmetic silently breaks.
- Unknown `ce-*` attributes are preserved verbatim on `extensions`, never dropped.

Failures throw `EnvelopeValidationError`, which carries a machine-readable `code`
(`missing-attribute`, `unsupported-specversion`, `invalid-attribute`) and the offending
`attribute`, so callers can branch on the cause instead of parsing messages.

## License

MIT
