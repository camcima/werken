import { EventEmitter } from "node:events";
import { Test } from "@nestjs/testing";
import type { TestingModuleBuilder } from "@nestjs/testing";
import type { INestMicroservice } from "@nestjs/common";
import { toPubSubAttributes } from "@werken/cloudevents";
import { WerkenPubSubTransport } from "../transport.js";
import { InMemoryIdempotencyStore } from "../idempotency.js";
import type { WerkenTransportOptions } from "../options.js";
import type { PubSubClientLike, SubscriptionLike } from "../options.js";
import type { DeadLetterStage } from "../dead-letter.js";
import type { IncomingMessage } from "../types.js";

/** A message the harness pushed through the transport, with the outcome it reached. */
export interface HarnessRecord {
  readonly type?: string;
  readonly id: string;
  readonly attributes: Readonly<Record<string, string>>;
  readonly body: string;
}

export interface DeadLetteredRecord extends HarnessRecord {
  readonly reason: string;
  readonly stage: DeadLetterStage;
}

export interface EmitOptions {
  readonly id?: string;
  readonly source?: string;
  readonly subject?: string;
  readonly time?: Date;
  readonly dataschema?: string;
  readonly datacontenttype?: string;
  readonly deliveryAttempt?: number;
  readonly orderingKey?: string;
  readonly extensions?: Record<string, string>;
}

export interface WerkenTestHarnessOptions {
  /** The Nest module under test — normally the worker module. */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- matches Nest's own module typing
  readonly module: any;
  /** Provider overrides, so ports can be replaced with fakes. */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- provider tokens are untyped in Nest
  readonly overrides?: Array<{ provide: any; useValue: any }>;
  /**
   * Schema handling. `passthrough` sends payloads as plain JSON with no schema resolution.
   * `strict` (M5) will decode against local .avsc files.
   */
  readonly schemas?: "passthrough";
  /** Default `ce-source` for emitted events. */
  readonly source?: string;
  /** Deterministic clock, for lateness and TTL assertions. */
  readonly now?: () => Date;
  /** Validation policies, so rejection behaviour can be asserted. */
  readonly validation?: WerkenTransportOptions["validation"];
  /** What to do with an event no handler matches. Default 'ack'. */
  readonly onUnhandledPattern?: WerkenTransportOptions["onUnhandledPattern"];
  /** Duplicate suppression. Defaults to a fresh in-memory store per harness. */
  readonly idempotency?: WerkenTransportOptions["idempotency"];
}

export interface WerkenTestHarness {
  emit(type: string, data: unknown, options?: EmitOptions): Promise<void>;
  emitRaw(attributes: Record<string, string>, body: Buffer | string): Promise<void>;
  readonly acked: readonly HarnessRecord[];
  readonly nacked: readonly HarnessRecord[];
  /** Messages the pipeline sent to the dead-letter topic, with the reason and stage. */
  readonly deadLettered: readonly DeadLetteredRecord[];
  /** Resolve a provider from the testing module — useful for asserting on fakes. */
  get<T>(token: unknown): T;
  /** Simulate shutdown drain (§5.6). */
  drain(): Promise<void>;
  close(): Promise<void>;
}

/** In-memory stand-in for a Pub/Sub subscription. No broker, no network, no credentials. */
class InMemorySubscription extends EventEmitter implements SubscriptionLike {
  close(): void {
    this.removeAllListeners();
  }
}

const DEFAULT_SOURCE = "https://harness.test/consumer";

export async function createWerkenTestHarness(options: WerkenTestHarnessOptions): Promise<WerkenTestHarness> {
  const acked: HarnessRecord[] = [];
  const nacked: HarnessRecord[] = [];
  const deadLettered: DeadLetteredRecord[] = [];
  const now = options.now ?? (() => new Date());
  const source = options.source ?? DEFAULT_SOURCE;

  const subscription = new InMemorySubscription();
  let pending: HarnessRecord | undefined;
  const client: PubSubClientLike = {
    subscription: () => subscription,
    topic: () => ({
      publishMessage: async (published) => {
        if (pending !== undefined) {
          deadLettered.push({
            ...pending,
            reason: published.attributes["werken-dl-reason"],
            stage: published.attributes["werken-dl-stage"] as DeadLetterStage,
          });
        }
        return undefined;
      },
    }),
    close: () => {},
  };

  let builder: TestingModuleBuilder = Test.createTestingModule({ imports: [options.module] });
  for (const override of options.overrides ?? []) {
    builder = builder.overrideProvider(override.provide).useValue(override.useValue);
  }
  const moduleRef = await builder.compile();

  const transport = new WerkenPubSubTransport({
    projectId: "werken-harness",
    subscription: "werken-harness-subscription",
    // A topic name is always configured so dead-lettering is exercised by default; assertions read
    // harness.deadLettered rather than a real broker.
    deadLetterTopic: "werken-harness-dead-letters",
    validation: options.validation,
    onUnhandledPattern: options.onUnhandledPattern,
    idempotency: options.idempotency ?? { store: new InMemoryIdempotencyStore(), consumer: "werken-harness" },
    createClient: () => client,
  });

  // Going through a real microservice is what makes the harness faithful: Nest discovers
  // @EventPattern handlers and resolves @Payload/@Ctx exactly as it would in production.
  const app: INestMicroservice = moduleRef.createNestMicroservice({ strategy: transport });
  await app.listen();

  let sequence = 0;

  async function push(attributes: Record<string, string>, body: Buffer, extra: EmitOptions): Promise<void> {
    const record: HarnessRecord = {
      type: attributes["ce-type"],
      id: attributes["ce-id"] ?? `harness-${++sequence}`,
      attributes,
      body: body.toString("utf8"),
    };

    pending = record;
    const settled = new Promise<void>((resolve) => {
      const message: IncomingMessage = {
        id: record.id,
        attributes,
        data: body,
        publishTime: now(),
        deliveryAttempt: extra.deliveryAttempt ?? 1,
        orderingKey: extra.orderingKey ?? "",
        ack: () => {
          acked.push(record);
          resolve();
        },
        nack: () => {
          nacked.push(record);
          resolve();
        },
      };
      subscription.emit("message", message);
    });

    await settled;
  }

  return {
    acked,
    nacked,
    deadLettered,

    async emit(type, data, emitOptions = {}) {
      const attributes = toPubSubAttributes({
        specversion: "1.0",
        id: emitOptions.id ?? `harness-${++sequence}`,
        source: emitOptions.source ?? source,
        type,
        subject: emitOptions.subject,
        time: emitOptions.time ?? now(),
        datacontenttype: emitOptions.datacontenttype ?? "application/json",
        dataschema: emitOptions.dataschema,
        extensions: emitOptions.extensions ?? {},
      });
      await push(attributes, Buffer.from(JSON.stringify(data ?? null)), emitOptions);
    },

    async emitRaw(attributes, body) {
      await push({ ...attributes }, Buffer.isBuffer(body) ? body : Buffer.from(body), {});
    },

    get<T>(token: unknown): T {
      return moduleRef.get<T>(token as never, { strict: false });
    },

    async drain() {
      await transport.close();
    },

    async close() {
      await app.close();
    },
  };
}
