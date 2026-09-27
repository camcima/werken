# CloudEvents library architecture review

Date: 2026-09-26  
Package: `@werken/cloudevents` 0.5.0  
Reviewed revision: `8365655`, plus the working tree

## Assessment

The package has a sound, deliberately small architecture: two conversion functions, a metadata model, a structured error type, and no runtime dependencies. Keeping payload decoding and Pub/Sub client operations outside this package is the right boundary.

The main weakness is the contract around those conversions. The library can misidentify the content type of events from other implementations, and its serializer can produce envelopes that its own parser rejects. I would address these issues before relying on it as a general interoperability boundary. Its current tests establish the Werken happy path, but do not establish compatibility with independently produced events.

### Findings at a glance

| ID  | Priority | Finding                                                          |
| --- | -------- | ---------------------------------------------------------------- |
| F1  | High     | Content-type mapping loses metadata from other Pub/Sub producers |
| F2  | Medium   | Extensions can populate reserved optional fields                 |
| F3  | Medium   | Inbound validation accepts malformed attribute values and names  |
| F4  | Medium   | Outbound serialization has no corresponding validation boundary  |

High means a common integration can silently receive incorrect metadata. Medium means a concrete input can violate the advertised envelope contract. All four findings were reproduced against JavaScript compiled from the tracked TypeScript source.

## Findings

### F1. Content-type mapping loses metadata from other Pub/Sub producers

**Locations:** [parse-envelope.ts](../packages/cloudevents/src/parse-envelope.ts), lines 5 and 128; [to-attributes.ts](../packages/cloudevents/src/to-attributes.ts), line 22; [types.ts](../packages/cloudevents/src/types.ts), line 19.

The parser reads only `ce-datacontenttype` and substitutes `application/json` when it is absent. The serializer writes only `ce-datacontenttype`.

Google's published Pub/Sub binding says to read `content-type` in binary mode. That document is a working draft and is internally inconsistent: its examples use `ce-datacontenttype`. This is a compatibility issue that needs an explicit policy, rather than a claim that the draft has one unambiguous spelling. See [the binding, section 3.1.1 and examples](https://github.com/googleapis/google-cloudevents/blob/main/docs/spec/pubsub.md#311-content-type).

There is also a concrete implementation mismatch: the official Go SDK writes `Content-Type` for this field. Its reader recognizes that spelling as well as prefixed attributes. See the SDK's [writer](https://github.com/cloudevents/sdk-go/blob/main/protocol/pubsub/v2/write_pubsub_message.go) and [reader](https://github.com/cloudevents/sdk-go/blob/main/protocol/pubsub/v2/message.go), inspected on the review date.

**Reproduction:** With otherwise valid required attributes, both `"content-type": "application/protobuf"` and `"Content-Type": "application/protobuf"` produce an envelope whose `datacontenttype` is `"application/json"`.

**Impact:** A consumer selecting a decoder from this field can select JSON for protobuf bytes. Forwarding the parsed envelope also replaces the original media type with JSON. This is observable without a broker and affects independently produced non-JSON events.

**Recommendation:** Accept the documented and deployed spellings through a small explicit mapping. Reject contradictory values or define a documented precedence. Select and document the outbound mapping, with a migration policy for existing Werken consumers. Add fixtures based on another SDK's wire output.

Also preserve an absent content type in the general envelope model, or make JSON defaulting an explicit Werken policy. The JSON event format's implied JSON content type does not establish a universal default for arbitrary binary payloads. See [CloudEvents 1.0.2, datacontenttype](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md#datacontenttype).

### F2. Extensions can populate reserved optional fields

**Location:** [to-attributes.ts](../packages/cloudevents/src/to-attributes.ts), lines 14–29.

Writing extensions first protects required attributes because those are always overwritten. Optional attributes are written only when present, so reserved extension names survive whenever their corresponding envelope fields are absent.

```ts
const attributes = toPubSubAttributes({
  ...base,
  extensions: {
    time: "not-a-timestamp",
    dataschema: "https://untrusted.example/schema",
    traceparent: "injected",
  },
});
// ce-time, ce-dataschema, and ce-traceparent are emitted.
// parseEnvelope(attributes) throws for ce-time.
```

Here `base` is a valid envelope without those optional fields. The same problem affects `subject`, `tracestate`, and `ingestiontime` when absent.

**Impact:** The serialized event can declare metadata that differs from the named envelope fields. An extension bag can change downstream schema or tracing metadata, or make an otherwise valid event unparseable. This requires caller-supplied extensions; it is not evidence of a remote compromise.

**Recommendation:** Reject reserved names in `extensions`, regardless of whether the corresponding field is present. Share the reserved-name definition between parser and serializer. Add a table-driven test covering every reserved name with its envelope field both present and absent. The existing collision test covers only `type` and `id`.

### F3. Inbound validation accepts malformed attribute values and names

**Location:** [parse-envelope.ts](../packages/cloudevents/src/parse-envelope.ts), lines 37–42, 97–104, and 121–133.

The parser checks required-field presence, the version, and timestamps. Other fields pass through without syntax validation. These inputs were accepted independently:

| Attribute            | Reproduced input   | Result                                 |
| -------------------- | ------------------ | -------------------------------------- |
| `ce-source`          | `not a uri`        | Accepted as source                     |
| `ce-dataschema`      | `relative/schema`  | Accepted as schema URI                 |
| `ce-datacontenttype` | `not a media type` | Accepted as content type               |
| `ce-Tenant-ID`       | `acme`             | Preserved as extension `Tenant-ID`     |
| `ce-`                | `empty`            | Preserved with an empty extension name |

CloudEvents defines `source` as a nonempty URI-reference, `dataschema` as an absolute URI, media-type syntax for `datacontenttype`, and lowercase ASCII letters/digits for attribute names. See [the core specification](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md#context-attributes).

**Impact:** Successful parsing does not establish a valid CloudEvents envelope. Invalid metadata can reach routing, schema selection, and forwarding, with failures deferred to another system. The Nest pipeline treats successful parsing as its envelope-validation boundary in [pipeline.ts](../packages/nestjs-google-pubsub/src/pipeline.ts), lines 114–119.

**Recommendation:** Add shared attribute validators and return `EnvelopeValidationError("invalid-attribute", ...)` for malformed values. Preserve valid relative sources such as `/orders`; requiring every source to be an absolute URL would introduce another compatibility problem. Distinguish an omitted optional value from a malformed supplied value. If permissive ingestion is intentional, expose and document that policy explicitly.

The promise that unknown attributes are never dropped also has an edge case: `ce-__proto__` disappears because `extensionsFrom` assigns into `{}`. This name is already invalid under the naming rules. Rejecting it is sufficient in strict mode; a permissive preservation mode should use a dictionary without a prototype or safe own-property creation. The string-valued reproduction showed data loss, not prototype pollution.

### F4. Outbound serialization has no corresponding validation boundary

**Locations:** [to-attributes.ts](../packages/cloudevents/src/to-attributes.ts), lines 11–29; [types.ts](../packages/cloudevents/src/types.ts), lines 8–26.

Ordinary values allowed by `CloudEventEnvelope` can produce attributes that `parseEnvelope` rejects:

```ts
toPubSubAttributes({ ...base, specversion: "0.3" }); // succeeds
toPubSubAttributes({ ...base, id: "" }); // succeeds
toPubSubAttributes({ ...base, time: new Date(NaN) }); // throws RangeError
```

The first two cases fail only on a later parse. The third bypasses the library's structured validation error model. No TypeScript escape hatch is needed for these examples: `specversion` is `string`, and an invalid date is still a `Date`.

**Impact:** Standalone callers can publish events that their own consumers reject. Error handling also depends on which conversion path encounters the problem. The Nest publisher performs some of its own checks, but that does not establish the standalone package's contract.

**Recommendation:** Validate at the public serialization boundary using the same field rules as parsing. Narrow `specversion` to the literal `"1.0"`, check date validity and representable output range, and use structured validation errors consistently. If serialization must remain unchecked, document its preconditions and provide a validated constructor; the present interface does not enforce those preconditions.

## Architectural observations

### Preserve the existing package boundary

The functions are synchronous and small, the error type is machine-readable, and the library has no dependency on Nest, Google clients, or payload codecs. These are useful properties. The fixes fit within the current package; they do not require a plugin framework, a new package hierarchy, or support for every CloudEvents transport.

Keep wire-key mapping and shared attribute validation separate internally. A single attribute definition table can prevent the parser's known-field set, serializer, reserved-name checks, and tests from drifting apart.

### Make timestamp limitations explicit

The calendar and hour-24 checks prevent real JavaScript date-normalization errors. The remaining limitations should be documented and tested:

| Reproduced behavior                                                                                 | Consequence                                                         | Suggested action                                                                                                |
| --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `2026-08-02T12:00:00.123456789Z` becomes `2026-08-02T12:00:00.123Z` after parsing and serialization | Submillisecond precision is lost                                    | Preserve the source timestamp if lossless forwarding is a requirement; otherwise document millisecond precision |
| `1990-12-31T23:59:60Z` is rejected                                                                  | Valid leap-second timestamps are outside the supported profile      | Document the limitation or adopt a representation that can retain them                                          |
| `0000-02-29T00:00:00Z` is rejected even though JavaScript parses it                                 | `Date.UTC(0, ...)` makes the calendar helper calculate against 1900 | Use direct Gregorian leap-year arithmetic                                                                       |

RFC 3339 permits fractional seconds and gives the leap-second example above; its Gregorian leap-year rule also explains the year-zero edge case. See [RFC 3339, sections 5.6–5.8 and appendix C](https://www.rfc-editor.org/rfc/rfc3339.html).

These are lower-priority than F1–F4 for normal application events. Treat timestamp precision as an API decision before expanding the library's role into event relaying or archival reconstruction.

### Clarify what readonly means

`readonly time?: Date` does not stop `envelope.time?.setTime(...)`. The envelope and its extensions are also mutable at runtime from JavaScript. This is acceptable for a data-transfer object, but it should not be treated as an immutable validation result. If stronger invariants become necessary, timestamps represented as strings or numbers are easier to keep immutable than `Date` instances.

### Verify the installed package boundary

The export map provides ESM and CommonJS entry points. The root build compiles both and creates the CommonJS package marker, while the package-local `build` script compiles only ESM. Document which build command produces a publishable artifact, or make the local command complete.

CI builds the repository and unit tests resolve this package to source through aliases. Add a packed-artifact smoke test that installs the tarball in a temporary consumer and checks ESM import, CommonJS require, and TypeScript declarations. The isolated compiled-entry checks performed for this review do not establish that the published tarball has all required files.

## Validation performed

- Read all five source files and all five TypeScript test files, package documentation/configuration, build scripts, CI configuration, and relevant Nest publisher/consumer integration points.
- Ran `pnpm exec vitest run packages/cloudevents/tests`: **44 tests passed across 5 files** on Node **22.22.2**.
- Repeated those tests in a temporary copy containing only TypeScript source and tests: **44 tests passed**. This excluded the pre-existing untracked JavaScript files from resolution.
- Compiled the package using both its ESM and CommonJS TypeScript configurations into `/tmp/werken-cloudevents-review`, with incremental compilation and declaration output disabled: **both passed**. Loaded both compiled entry points successfully.
- Ran 14 focused behavior assertions against the isolated ESM output, then an additional assertion for the Go SDK's `Content-Type` spelling. They confirmed the reported behavior; these were defect reproductions, not assertions that the behavior is correct.
- Compared protocol assumptions with the versioned CloudEvents 1.0.2 specification, RFC 3339, Google's Pub/Sub binding draft, and the official Go SDK source.

No broker integration, actual cross-language SDK execution, benchmark, packed-artifact installation, or full monorepo test run was performed. Runtime compatibility evidence here is limited to Node 22. Library source and existing tests were left unchanged; the pre-existing untracked JavaScript files were preserved.

## Recommended sequence

1. **Resolve the wire contract:** support deployed content-type spellings, define conflict handling, and plan compatibility for existing Werken attributes.
2. **Close the extension collision gap:** reject reserved names consistently and expand the collision matrix.
3. **Unify validation:** apply shared field rules on both ingress and egress, with consistent error codes and explicit handling of missing values.
4. **Strengthen compatibility evidence:** add external wire fixtures and packed-package consumer checks. Round trips through the same implementation cannot reveal a shared mapping mistake.
5. **Document the supported profile:** binary mode, timestamp precision, leap seconds, defaulting, and runtime mutability. Make stricter validation and default changes visible in migration notes because existing producers may depend on current behavior.
