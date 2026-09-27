// node10 resolution ignores exports maps; "types" and typesVersions have to carry it.
import { parseEnvelope, type CloudEventEnvelope } from "@werken/cloudevents";
import { createEventPublisher } from "@werken/nestjs-google-pubsub";
import { createWerkenTestHarness } from "@werken/nestjs-google-pubsub/testing";

export const envelope: CloudEventEnvelope = parseEnvelope({});
export const publisher: typeof createEventPublisher = createEventPublisher;
export const harness: typeof createWerkenTestHarness = createWerkenTestHarness;
