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
- Unknown `ce-*` attributes are preserved verbatim on `extensions`, never dropped. A bare `ce-`
  names nothing and is skipped.
- `toPubSubAttributes` refuses an extension named after an envelope field, such as `time` or
  `dataschema`, and an empty extension name. Written out, it would stand in for the field whenever
  the field is absent, declaring metadata the envelope does not. It also refuses an invalid `Date`.

Failures throw `EnvelopeValidationError`, which carries a machine-readable `code`
(`missing-attribute`, `unsupported-specversion`, `invalid-attribute`) and the offending
`attribute`, so callers can branch on the cause instead of parsing messages. Both functions throw
it.

### Content type

Werken writes `ce-datacontenttype`. Other producers do not all agree: the CloudEvents Go SDK writes
a plain `Content-Type` attribute, and Google's Pub/Sub binding draft says to read `content-type`.
`parseEnvelope` therefore reads `ce-datacontenttype`, then `content-type`, then `Content-Type`,
and only when all three are absent assumes `application/json`. That default is a Werken policy,
not a CloudEvents rule: a producer sending non-JSON bytes has to say so.

### What it does not check

- **Attribute value syntax.** `ce-source`, `ce-dataschema` and `ce-datacontenttype` are taken as
  given, not checked as a URI-reference, an absolute URI or a media type. Rejecting a slightly-off
  value would dead-letter an event that is otherwise usable. Extension names are not checked
  against the CloudEvents naming rules either.
- **Sub-millisecond precision.** Timestamps become `Date`, which holds milliseconds.
  `…T12:00:00.123456789Z` is read as `…T12:00:00.123Z` and written back that way.
- **Leap seconds.** A `ce-time` with second `60` is rejected, since `Date` cannot represent it.

### Envelopes are not frozen

`readonly` on `CloudEventEnvelope` is a compile-time contract only. The objects are ordinary and
mutable at runtime, and `time` and `ingestiontime` are `Date` instances, which `setTime()` can
change. Treat an envelope as a data-transfer object, not as proof that it is still valid.

## License

MIT
