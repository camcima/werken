import "reflect-metadata";
import { NestFactory } from "@nestjs/core";
import { Pool } from "pg";
import { WerkenPubSubTransport } from "@werken/nestjs-google-pubsub";
import { WorkerModule, requireDatabaseUrl } from "./worker.module.js";
import { createPgExecutor } from "./adapters/outbound/pg-executor.js";
import { readerTypeFor } from "./schema/reader-types.js";

/**
 * Cloud Run worker pools have no HTTP endpoint and no port to bind, so this is a microservice with
 * a custom transport rather than an HTTP app (§7.1).
 */
async function bootstrap() {
  // Constructed here, not fetched out of a booted context: Nest eagerly instantiates every
  // singleton a module declares, so a pool provider that reads DATABASE_URL would also run under
  // the tests, which never touch Postgres. Owning it at the entry point is also what lets the same
  // instance reach both consumers below — the projection through DI, the dedup marker through the
  // executor — so they are genuinely Mode A against one database rather than two pools that happen
  // to share a URL.
  const pool = new Pool({ connectionString: requireDatabaseUrl() });

  const transport = new WerkenPubSubTransport({
    projectId: process.env.GCP_PROJECT_ID!,
    subscription: process.env.PUBSUB_SUBSCRIPTION!,
    deadLetterTopic: process.env.PUBSUB_DEAD_LETTER_TOPIC,

    // Resolves the writer schema by revision and decodes into the reader type above. Strict by
    // default: a schema that cannot be read fails the message rather than guessing.
    schemaRegistry: { readerTypeFor },

    // The projection and the dedup marker share one pool, so both are Mode A against one database.
    idempotency: {
      consumer: "shipment-projection",
      executor: () => createPgExecutor(pool),
    },

    validation: { onInvalidEnvelope: "dead-letter", onDecodeFailure: "dead-letter" },
    onUnhandledPattern: "ack",

    flowControl: { maxOutstandingMessages: 100, maxOutstandingBytes: 50 * 1024 * 1024 },
    ackDeadline: { initialMs: 60_000, maxExtensionMs: 600_000 },
    shutdownDrainTimeoutMs: 30_000,

    telemetry: { serviceName: "shipment-projection" },
  });

  const worker = await NestFactory.createMicroservice(WorkerModule.forRoot(pool), {
    strategy: transport,
    bufferLogs: true,
  });

  // Without this, Nest never calls the transport's close(), so scale-down kills in-flight handlers
  // and every interrupted message is reprocessed from scratch (§5.6).
  worker.enableShutdownHooks();

  await worker.listen();
}

void bootstrap();
