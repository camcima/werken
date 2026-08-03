import "reflect-metadata";
import { NestFactory } from "@nestjs/core";
import { WerkenPubSubTransport } from "@werken/nestjs-google-pubsub";
import { WorkerModule } from "./worker.module.js";

/**
 * Cloud Run worker pools have no HTTP endpoint and no port to bind, so this is a microservice with
 * a custom transport rather than an HTTP app (§7.1).
 */
async function bootstrap() {
  const transport = new WerkenPubSubTransport({
    projectId: process.env.GCP_PROJECT_ID!,
    subscription: process.env.PUBSUB_SUBSCRIPTION!,
    deadLetterTopic: process.env.PUBSUB_DEAD_LETTER_TOPIC,

    flowControl: { maxOutstandingMessages: 50, maxOutstandingBytes: 20 * 1024 * 1024 },

    idempotency: {
      consumer: "shipment-dispatch",
      // Mode A: a pool-backed executor. See the README for the adapter for your driver.
      // executor: () => myExecutor,
    },

    // Development only. Unset in production, where it fails startup unless explicitly overridden.
    resourcePrefix: process.env.WERKEN_RESOURCE_PREFIX,

    telemetry: { serviceName: "shipment-dispatch" },
  });

  const app = await NestFactory.createMicroservice(WorkerModule, { strategy: transport, bufferLogs: true });

  // Without this, Nest never calls the transport's close(), so scale-down kills in-flight handlers
  // and every interrupted message is reprocessed from scratch (§5.6).
  app.enableShutdownHooks();

  await app.listen();
}

void bootstrap();
