// A .cts file is CommonJS under Node16 resolution, so these imports resolve through the "require"
// condition of each exports map, exactly as require() would.
import { parseEnvelope, type CloudEventEnvelope } from "@werken/cloudevents";
import { createEventPublisher } from "@werken/nestjs-google-pubsub";
import { createWerkenTestHarness } from "@werken/nestjs-google-pubsub/testing";

export const envelope: CloudEventEnvelope = parseEnvelope({});
export const publisher: typeof createEventPublisher = createEventPublisher;
export const harness: typeof createWerkenTestHarness = createWerkenTestHarness;
