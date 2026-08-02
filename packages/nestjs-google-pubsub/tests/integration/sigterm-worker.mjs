/**
 * Child process for the SIGTERM drain test.
 *
 * Boots a real Nest microservice on the real transport against the Pub/Sub emulator, starts a
 * handler that takes a while, and reports what happened when SIGTERM arrives mid-flight.
 *
 * Prints one JSON line per event to stdout so the parent can assert on the sequence.
 */
import { PubSub } from "@google-cloud/pubsub";
import { WerkenPubSubTransport } from "@werken/nestjs-google-pubsub";

const say = (event, detail = {}) => console.log(JSON.stringify({ event, ...detail }));

const subscription = process.env.WERKEN_SUBSCRIPTION;
const project = process.env.PUBSUB_PROJECT_ID;
const handlerMs = Number(process.env.WERKEN_HANDLER_MS ?? "3000");

const transport = new WerkenPubSubTransport({
  projectId: project,
  subscription,
  shutdownDrainTimeoutMs: Number(process.env.WERKEN_DRAIN_MS ?? "10000"),
  createClient: () => new PubSub({ projectId: project }),
});

transport.addHandler(
  "com.example.slow.v1",
  async (data) => {
    say("handler:start", { data });
    await new Promise((r) => setTimeout(r, handlerMs));
    say("handler:finish", { data });
  },
  true,
);

await new Promise((resolve, reject) => {
  transport.listen((error) => (error ? reject(error) : resolve()));
});
say("listening");

process.on("SIGTERM", () => {
  say("sigterm");
  transport
    .close()
    .then(() => {
      say("drained");
      process.exit(0);
    })
    .catch((error) => {
      say("drain:error", { message: String(error) });
      process.exit(1);
    });
});
