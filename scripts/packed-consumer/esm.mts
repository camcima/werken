// Resolved through the "import" condition of each exports map.
import { parseEnvelope, type CloudEventEnvelope } from "@werken/cloudevents";
import { createEventPublisher, type EventPublisher } from "@werken/nestjs-google-pubsub";
import { createWerkenTestHarness, type WerkenTestHarness } from "@werken/nestjs-google-pubsub/testing";

export const envelope: CloudEventEnvelope = parseEnvelope({});
export const publisher: typeof createEventPublisher = createEventPublisher;
export type Publisher = EventPublisher;
export const harness: typeof createWerkenTestHarness = createWerkenTestHarness;
export type Harness = WerkenTestHarness;
