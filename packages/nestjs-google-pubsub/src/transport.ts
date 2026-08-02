import { Server } from "@nestjs/microservices";
import type { CustomTransportStrategy } from "@nestjs/microservices";
import { MessagePipeline } from "./pipeline.js";
import type { EventHandler } from "./pipeline.js";
import { PubSubDeadLetterPublisher } from "./dead-letter.js";
import type { DeadLetterPublisher } from "./dead-letter.js";
import { AvroCodec } from "./schema/avro-codec.js";
import { NoopIdempotencyStore, createSqlIdempotencyStore } from "./idempotency.js";
import {
  DEFAULT_FLOW_CONTROL,
  DEFAULT_MAX_STREAMS,
  type PubSubClientLike,
  type SubscriptionLike,
  type WerkenTransportOptions,
} from "./options.js";
import type { IdempotencyStore } from "./idempotency.js";
import type { IncomingMessage } from "./types.js";

export type WerkenTransportStatus = "connected" | "disconnected";

// eslint-disable-next-line @typescript-eslint/no-unsafe-function-type -- shape required by Server's EventsMap constraint
export interface WerkenTransportEvents extends Record<string, Function> {
  error: (error: unknown) => void;
  close: () => void;
}

/**
 * Nest custom transport for Google Cloud Pub/Sub.
 *
 * `on()` and `unwrap()` are abstract on Nest's `Server` base class — note they are NOT part of
 * `CustomTransportStrategy`, which requires only `listen()`/`close()`. Verified against
 * @nestjs/microservices 11.1.28; see docs/spikes/nest-11-transport-typings.md.
 */
export class WerkenPubSubTransport
  extends Server<WerkenTransportEvents, WerkenTransportStatus>
  implements CustomTransportStrategy
{
  private client?: PubSubClientLike;
  private subscription?: SubscriptionLike;
  // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type -- Server<EventsMap> constrains to Record<string, Function>, so this is the type Nest hands us
  private readonly listeners = new Map<keyof WerkenTransportEvents, Function[]>();
  private pipeline: MessagePipeline;
  private readonly idempotencyStore: IdempotencyStore;

  constructor(private readonly options: WerkenTransportOptions) {
    super();
    // Constructed eagerly so the "no idempotency store" warning fires at startup, not on the first
    // message — an operator should learn about it before traffic arrives.
    this.idempotencyStore = resolveIdempotencyStore(options, (m: string) => this.logger.warn(m));
    // Built without a dead-letter publisher until listen() has a client to give it; terminal
    // messages nack rather than being dropped in the meantime.
    this.pipeline = this.buildPipeline(undefined);
  }

  private buildCodec(client: PubSubClientLike): AvroCodec | undefined {
    const registry = this.options.schemaRegistry;
    if (registry === undefined) return undefined;

    return new AvroCodec({
      fetchWriterSchema: async (revisionQualifiedName) => {
        if (client.schema === undefined) {
          throw new Error("Pub/Sub client does not expose schema()");
        }
        const schema = await client.schema(revisionQualifiedName).get();
        if (schema.definition === undefined || schema.definition === null) {
          throw new Error(`schema ${revisionQualifiedName} has no definition`);
        }
        return schema.definition;
      },
      readerTypeFor: registry.readerTypeFor,
      strict: registry.strict,
      cacheTtlMs: registry.cacheTtlMs,
      maxCachedRevisions: registry.maxCachedRevisions,
      logger: { debug: (m) => this.logger.debug?.(m), warn: (m) => this.logger.warn(m) },
    });
  }

  private buildPipeline(deadLetterPublisher: DeadLetterPublisher | undefined, codec?: AvroCodec): MessagePipeline {
    return new MessagePipeline({
      subscription: this.options.subscription,
      resolveHandler: (pattern) => this.getHandlerByPattern(pattern) as EventHandler | null,
      deadLetterPublisher,
      codec,
      idempotencyStore: this.idempotencyStore,
      consumer: this.options.idempotency?.consumer,
      idempotencyTtlMs: this.options.idempotency?.ttlMs,
      validation: this.options.validation,
      onUnhandledPattern: this.options.onUnhandledPattern,
      logger: { warn: (m) => this.logger.warn(m), error: (m) => this.logger.error(m) },
    });
  }

  listen(callback: (...optionalParams: unknown[]) => void): void {
    try {
      this.client = this.options.createClient?.(this.options) ?? this.createDefaultClient();

      this.pipeline = this.buildPipeline(
        this.options.deadLetterTopic === undefined
          ? undefined
          : new PubSubDeadLetterPublisher(this.client, this.options.deadLetterTopic),
        this.buildCodec(this.client),
      );

      const flowControl = { ...DEFAULT_FLOW_CONTROL, ...this.options.flowControl };
      this.subscription = this.client.subscription(this.options.subscription, {
        flowControl,
        streamingOptions: { maxStreams: this.options.streaming?.maxStreams ?? DEFAULT_MAX_STREAMS },
      });

      this.subscription.on("message", (message: IncomingMessage) => {
        void this.handleMessage(message);
      });

      this.subscription.on("error", (error: unknown) => {
        this.emit("error", error);
      });

      this._status$.next("connected");
      callback();
    } catch (error) {
      callback(error);
    }
  }

  async close(): Promise<void> {
    try {
      this.subscription?.removeAllListeners();
      await this.subscription?.close();
      await this.client?.close();
    } finally {
      this.subscription = undefined;
      this.client = undefined;
      this._status$.next("disconnected");
      this.emit("close");
    }
  }

  on<K extends keyof WerkenTransportEvents, F extends WerkenTransportEvents[K]>(event: K, callback: F): void {
    const existing = this.listeners.get(event) ?? [];
    existing.push(callback);
    this.listeners.set(event, existing);
  }

  unwrap<T>(): T {
    if (this.subscription === undefined) {
      throw new Error("werken: transport is not listening — call listen() before unwrap()");
    }
    return this.subscription as T;
  }

  /**
   * Readiness for Cloud Run worker pools, which have no HTTP endpoint (§7.1). Prefer the `status`
   * observable Nest's Server already exposes when you want to react to changes.
   */
  isHealthy(): boolean {
    return this.subscription !== undefined;
  }

  private createDefaultClient(): PubSubClientLike {
    // Nest's own helper, so @google-cloud/pubsub stays a peer dependency and resolution works the
    // same way in the ESM and CJS builds.
    const { PubSub } = this.loadPackage<{ new (o: unknown): PubSubClientLike }>(
      "@google-cloud/pubsub",
      WerkenPubSubTransport.name,
    ) as unknown as { PubSub: new (o: unknown) => PubSubClientLike };

    return new PubSub({ projectId: this.options.projectId, apiEndpoint: this.options.apiEndpoint });
  }

  private async handleMessage(message: IncomingMessage): Promise<void> {
    const outcome = await this.pipeline.handle(message);
    // 'dead-letter' means the copy is already safely on the dead-letter topic, so the original can
    // be acked. A failed dead-letter publish comes back as 'nack' instead.
    if (outcome === "ack" || outcome === "dead-letter") {
      message.ack();
    } else {
      message.nack();
    }
  }

  private emit<K extends keyof WerkenTransportEvents>(event: K, ...args: unknown[]): void {
    for (const listener of this.listeners.get(event) ?? []) {
      listener(...args);
    }
  }
}

function resolveIdempotencyStore(options: WerkenTransportOptions, warn: (message: string) => void): IdempotencyStore {
  const config = options.idempotency;
  if (config?.store !== undefined && config.executor !== undefined) {
    throw new Error("werken: idempotency.store and idempotency.executor are mutually exclusive — supply one");
  }
  if (config?.store !== undefined) return config.store;
  if (config?.executor !== undefined) {
    return createSqlIdempotencyStore({ executor: config.executor, table: config.table });
  }
  return new NoopIdempotencyStore({ warn });
}
