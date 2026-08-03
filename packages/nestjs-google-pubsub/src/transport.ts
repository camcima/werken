import { Server } from "@nestjs/microservices";
import type { CustomTransportStrategy } from "@nestjs/microservices";
import { MessagePipeline } from "./pipeline.js";
import type { EventHandler } from "./pipeline.js";
import { PubSubDeadLetterPublisher } from "./dead-letter.js";
import type { DeadLetterPublisher } from "./dead-letter.js";
import { AvroCodec } from "./schema/avro-codec.js";
import { NoopIdempotencyStore, createSqlIdempotencyStore } from "./idempotency.js";
import {
  DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MS,
  toSubscriberOptions,
  type PubSubClientLike,
  type SubscriptionLike,
  type WerkenTransportOptions,
} from "./options.js";
import type { IdempotencyStore } from "./idempotency.js";
import { applyResourcePrefix, assertResourcePrefixSafe } from "./resource-name.js";
import { PatternRouter } from "./pattern-router.js";
import { createTelemetry } from "./telemetry.js";
import type { Telemetry } from "./telemetry.js";
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
 * `CustomTransportStrategy`, which requires only `listen()`/`close()`. Both must therefore be
 * implemented here even though nothing in the transport contract asks for them. Verified against
 * @nestjs/microservices 11.1.28.
 */
export class WerkenPubSubTransport
  extends Server<WerkenTransportEvents, WerkenTransportStatus>
  implements CustomTransportStrategy
{
  private client?: PubSubClientLike;
  private subscription?: SubscriptionLike;
  /** In-flight handlers, so shutdown can wait for them rather than cutting them off. */
  private readonly inFlight = new Map<IncomingMessage, Promise<void>>();
  private draining = false;
  // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type -- Server<EventsMap> constrains to Record<string, Function>, so this is the type Nest hands us
  private readonly listeners = new Map<keyof WerkenTransportEvents, Function[]>();
  private pipeline: MessagePipeline;
  private readonly idempotencyStore: IdempotencyStore;
  private readonly telemetry: Telemetry;
  /** Built once at listen(), so patterns are not re-scanned for every message. */
  private router?: PatternRouter;

  constructor(private readonly options: WerkenTransportOptions) {
    super();
    // Constructed eagerly so the "no idempotency store" warning fires at startup, not on the first
    // message — an operator should learn about it before traffic arrives.
    this.idempotencyStore = resolveIdempotencyStore(options, (m: string) => this.logger.warn(m));
    this.telemetry = createTelemetry({
      enabled: options.telemetry?.enabled,
      serviceName: options.telemetry?.serviceName ?? "werken",
    });
    // Built without a dead-letter publisher until listen() has a client to give it; terminal
    // messages nack rather than being dropped in the meantime. The raw subscription name is
    // provisional: resolving the prefix here would move an invalid-prefix error from startup to
    // construction, and listen() replaces this pipeline with one naming the resolved subscription
    // before any message can arrive.
    this.pipeline = this.buildPipeline(options.subscription, undefined);
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
      onCacheResult: (result) => this.telemetry.recordSchemaCache(result),
      logger: { debug: (m) => this.logger.debug?.(m), warn: (m) => this.logger.warn(m) },
    });
  }

  private buildPipeline(
    subscription: string,
    deadLetterPublisher: DeadLetterPublisher | undefined,
    codec?: AvroCodec,
  ): MessagePipeline {
    return new MessagePipeline({
      subscription,
      resolveRoute: (type) => this.router?.resolve(type) ?? null,
      deadLetterPublisher,
      codec,
      idempotencyStore: this.idempotencyStore,
      consumer: this.options.idempotency?.consumer,
      idempotencyTtlMs: this.options.idempotency?.ttlMs,
      telemetry: this.telemetry,
      validation: this.options.validation,
      onUnhandledPattern: this.options.onUnhandledPattern,
      logger: { warn: (m) => this.logger.warn(m), error: (m) => this.logger.error(m) },
    });
  }

  listen(callback: (...optionalParams: unknown[]) => void): void {
    void this.start().then(
      () => callback(),
      (error: unknown) => callback(error),
    );
  }

  private async start(): Promise<void> {
    const prefix = this.options.resourcePrefix;
    assertResourcePrefixSafe(prefix, this.options.allowUnsafeResourcePrefix === true, process.env.NODE_ENV);

    // Resolved before anything connects, so an invalid prefix fails here rather than at first
    // publish, with the full name the operator actually needs to see.
    const subscriptionName = applyResourcePrefix(this.options.subscription, prefix);
    const deadLetterTopic =
      this.options.deadLetterTopic === undefined
        ? undefined
        : applyResourcePrefix(this.options.deadLetterTopic, prefix);

    if (prefix !== undefined && prefix !== "") {
      // Silent name rewriting is exactly the thing that costs an hour to diagnose when someone
      // forgets an env var is set.
      this.logger.warn(
        `werken: resourcePrefix ${JSON.stringify(prefix)} is active — subscribing to ` +
          `${JSON.stringify(subscriptionName)}` +
          (deadLetterTopic === undefined ? "" : ` and dead-lettering to ${JSON.stringify(deadLetterTopic)}`) +
          ". This is a development-only affordance; unset it in production.",
      );
    }

    // Built before anything connects, so an ambiguous or unsupported pattern fails startup rather
    // than leaving a handler that silently never runs.
    this.router = new PatternRouter(this.getHandlers() as unknown as Iterable<[string, EventHandler]>);

    this.client = this.options.createClient?.(this.options) ?? this.createDefaultClient();

    // Everything past the client's creation runs guarded: a failure here has already allocated SDK
    // resources, and leaving them behind means a crash loop or a test suite stacks up gRPC
    // channels, retry timers and credentials-backed clients.
    try {
      await this.startSubscribing(subscriptionName, deadLetterTopic, prefix);
    } catch (error) {
      await this.closePartialStartup();
      throw error;
    }
  }

  private async startSubscribing(
    subscriptionName: string,
    deadLetterTopic: string | undefined,
    prefix: string | undefined,
  ): Promise<void> {
    if (this.client === undefined) throw new Error("werken: startup lost its client");

    // The resolved name, not options.subscription: it is what CloudEventContext.subscription,
    // telemetry labels and dead-letter provenance report, so all three name the resource that
    // actually delivered the message.
    this.pipeline = this.buildPipeline(
      subscriptionName,
      deadLetterTopic === undefined ? undefined : new PubSubDeadLetterPublisher(this.client, deadLetterTopic),
      this.buildCodec(this.client),
    );

    this.draining = false;
    const { flowControl, streamingOptions, minAckDeadlineMs, maxExtensionTimeMs } = toSubscriberOptions(this.options);
    const Duration = this.loadDuration();
    this.subscription = this.client.subscription(subscriptionName, {
      flowControl,
      streamingOptions,
      // Must be the SDK's own Duration: it calls .total() and Duration.compare() on these, so a
      // structurally-similar object throws "first.total is not a function" on close().
      ...(Duration === undefined
        ? {}
        : {
            minAckDeadline: Duration.from({ millis: minAckDeadlineMs }),
            maxExtensionTime: Duration.from({ millis: maxExtensionTimeMs }),
          }),
    });

    // The library never provisions resources. A scoped subscription that was not created is a
    // startup failure, not a consumer that sits there healthy and receives nothing.
    if (prefix !== undefined && prefix !== "" && this.subscription.exists !== undefined) {
      const [exists] = await this.subscription.exists();
      if (!exists) {
        throw new Error(
          `werken: subscription ${JSON.stringify(subscriptionName)} does not exist in project ` +
            `${JSON.stringify(this.options.projectId)}. Scoped resources are not created by this library — ` +
            "create it, or unset resourcePrefix.",
        );
      }
    }

    this.subscription.on("message", (message: IncomingMessage) => {
      void this.handleMessage(message);
    });

    this.subscription.on("error", (error: unknown) => {
      // Logged as well as emitted. Registering an `on('error')` listener is optional, and a broker
      // error that disappears silently is exactly what gets diagnosed hours too late.
      this.logger.error(`werken: subscription error: ${asMessage(error)}`);
      this.emit("error", error);
    });

    this._status$.next("connected");
  }

  /**
   * Drain sequence. Nest calls this on shutdown, which needs
   * `app.enableShutdownHooks()` in the consumer's bootstrap — without it, scale-down kills
   * in-flight work and produces duplicates on every event.
   */
  /**
   * Closes whatever startup managed to create before it failed, and clears the instance state so a
   * later close() cannot double-close it.
   *
   * Close failures are swallowed deliberately: the startup error is the one the operator needs, and
   * replacing it with "close failed" while unwinding would bury the actual cause.
   */
  private async closePartialStartup(): Promise<void> {
    const { subscription, client } = this;
    this.subscription = undefined;
    this.client = undefined;

    try {
      await subscription?.close();
    } catch {
      /* startup already failed; that error is the one worth reporting */
    }
    try {
      await client?.close();
    } catch {
      /* as above */
    }
  }

  async close(): Promise<void> {
    if (this.draining) return;
    this.draining = true;
    const startedAt = Date.now();

    // 1. Stop taking new work. Only the message listener comes off, so nothing new is leased while
    //    draining. The error listener stays until the stream is actually closed: an EventEmitter
    //    throws when it emits 'error' with nothing listening, which would turn a broker hiccup
    //    mid-shutdown into a crash.
    this.subscription?.removeAllListeners("message");

    // 2. Await in-flight handlers, bounded.
    const timeoutMs = this.options.shutdownDrainTimeoutMs ?? DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MS;
    const outstanding = [...this.inFlight.values()];
    const completed = outstanding.length;
    let timedOut = false;
    if (outstanding.length > 0) {
      timedOut = !(await raceWithTimeout(Promise.allSettled(outstanding), timeoutMs));
    }

    // 3. Nack anything still running at the timeout. Fast redelivery beats letting the ack deadline
    //    lapse silently, and nacking is the only outcome that cannot ack unprocessed work.
    let nacked = 0;
    for (const message of this.inFlight.keys()) {
      try {
        message.nack();
        nacked++;
      } catch (error) {
        this.logger.error(`werken: failed to nack ${message.id} during drain: ${asMessage(error)}`);
      }
    }
    this.inFlight.clear();

    // 4. Close the subscription and client.
    try {
      await this.subscription?.close();
      await this.client?.close();
    } finally {
      this.subscription?.removeAllListeners();
      this.subscription = undefined;
      this.client = undefined;
      this._status$.next("disconnected");
      this.logger.log(
        `werken: drain complete — completed=${completed - nacked} nacked=${nacked} ` +
          `durationMs=${Date.now() - startedAt}${timedOut ? " (timed out)" : ""}`,
      );
      this.emit("close");
    }
  }

  on<K extends keyof WerkenTransportEvents, F extends WerkenTransportEvents[K]>(event: K, callback: F): void {
    const existing = this.listeners.get(event) ?? [];
    existing.push(callback);
    this.listeners.set(event, existing);
  }

  /**
   * Returns the underlying `Subscription` — not the `PubSub` client, which is what `unwrap()`
   * yields on most other Nest transports. The subscription is the thing this transport drives, and
   * the client is reachable from it; the deviation is called out because it is easy to assume
   * otherwise.
   */
  unwrap<T>(): T {
    if (this.subscription === undefined) {
      throw new Error("werken: transport is not listening — call listen() before unwrap()");
    }
    return this.subscription as T;
  }

  /**
   * Readiness for Cloud Run worker pools, which have no HTTP endpoint. Prefer the `status`
   * observable Nest's Server already exposes when you want to react to changes.
   */
  isHealthy(): boolean {
    // `isOpen === false` means the SDK has given up on the stream. Reporting healthy then leaves a
    // worker sitting in the pool receiving nothing, which is precisely what a health check is for.
    // Deliberately not latched on 'error' events: most are transient and the SDK reconnects, so
    // failing on those would pull healthy workers out of rotation.
    return this.subscription !== undefined && this.subscription.isOpen !== false;
  }

  /** The SDK's Duration class, or undefined if the peer dependency is not installed. */
  private loadDuration(): { from(like: { millis: number }): unknown } | undefined {
    try {
      const pkg = this.loadPackage<Record<string, unknown>>("@google-cloud/pubsub", WerkenPubSubTransport.name);
      return pkg["Duration"] as { from(like: { millis: number }): unknown } | undefined;
    } catch {
      return undefined;
    }
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
    // Anything arriving after the drain began is left unacked so Pub/Sub redelivers it to a live
    // instance rather than this one abandoning it.
    if (this.draining) return;

    const tracked = this.processMessage(message);
    this.inFlight.set(message, tracked);
    try {
      await tracked;
    } finally {
      this.inFlight.delete(message);
    }
  }

  private async processMessage(message: IncomingMessage): Promise<void> {
    this.telemetry.addInFlight(this.options.subscription, 1);
    try {
      await this.processTraced(message);
    } finally {
      this.telemetry.addInFlight(this.options.subscription, -1);
    }
  }

  private async processTraced(message: IncomingMessage): Promise<void> {
    const outcome = await this.pipeline.handle(message);

    // The drain nacks whatever is still running when it times out and then forgets it. A handler
    // that finishes after that point must not settle its message a second time: the redelivery is
    // already under way, so acking here would race it.
    if (!this.inFlight.has(message)) {
      this.logger.warn(`werken: dropping late ${outcome} for ${message.id} — the drain already nacked it`);
      return;
    }

    // 'dead-letter' means the copy is already safely on the dead-letter topic, so the original can
    // be acked. A failed dead-letter publish comes back as 'nack' instead.
    const acking = outcome === "ack" || outcome === "dead-letter";
    try {
      if (acking) {
        message.ack();
      } else {
        message.nack();
      }
    } catch (error) {
      // Settling throws if the subscriber closed underneath us. Redelivery already covers the
      // message, whereas an unhandled rejection out of the fire-and-forget message listener in
      // listen() would take the whole process down.
      this.logger.error(`werken: failed to ${acking ? "ack" : "nack"} ${message.id}: ${asMessage(error)}`);
    }
  }

  private emit<K extends keyof WerkenTransportEvents>(event: K, ...args: unknown[]): void {
    for (const listener of this.listeners.get(event) ?? []) {
      listener(...args);
    }
  }
}

async function raceWithTimeout(work: Promise<unknown>, timeoutMs: number): Promise<boolean> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<false>((resolve) => {
    timer = setTimeout(() => resolve(false), timeoutMs);
  });
  try {
    return (await Promise.race([work.then(() => true), timeout])) !== false;
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

function asMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
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
