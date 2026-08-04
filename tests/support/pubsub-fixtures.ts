/**
 * Fixture lifecycle for the integration suites that talk to the Pub/Sub emulator.
 *
 * Each suite owns a small, fixed set of resources named after the file that owns them, and resets
 * them in `beforeAll` instead of trusting `afterAll` to have tidied up after the previous run.
 * `afterAll` does not run when the process dies without unwinding — Ctrl-C, a CI step timeout, an
 * OOM kill, `docker compose down` mid-suite — so whatever it was going to delete simply stays on
 * the emulator, and nothing else ever removes it. Cleaning before, not only after, is what makes a
 * run's correctness independent of how the previous run happened to exit.
 *
 * Fixed names are what make cleaning before possible at all. The alternative — a `Date.now()`
 * suffix — cannot be cleaned up in advance, because there is nothing to name: you would have to
 * sweep the project by prefix and hope nothing else matched. Names are kept distinct per file
 * rather than per run, since vitest runs the files in parallel and two suites sharing one
 * subscription would steal each other's messages.
 */

/** gRPC NOT_FOUND. Deleting a fixture that is not there is the ordinary case, not a failure. */
const NOT_FOUND = 5;

/** The resources one suite owns. */
export interface PubSubFixtures {
  subscriptions?: readonly string[];
  topics?: readonly string[];
  schemas?: readonly string[];
}

/**
 * Only the handles deleting a fixture needs, declared structurally rather than imported as the
 * SDK's `PubSub`. A real client satisfies it, and the root workspace gains no dependency on
 * @google-cloud/pubsub for four method signatures — the same reasoning as `PubSubClientLike`.
 */
export interface PubSubFixtureClient {
  subscription(name: string): { delete(): Promise<unknown> };
  topic(name: string): { delete(): Promise<unknown> };
  schema(name: string): { delete(): Promise<unknown> };
}

/**
 * Deletes the suite's resources before `beforeAll` creates them.
 *
 * Anything left over from an interrupted run goes, along with its backlog — so a stale message
 * cannot be mistaken for one this run published, and `createTopic`/`createSubscription` cannot fail
 * with ALREADY_EXISTS. It also re-creates settings that Pub/Sub refuses to change after the fact,
 * `enableMessageOrdering` and `schemaSettings` among them.
 *
 * Only NOT_FOUND is tolerated. A delete that fails for any other reason means the emulator is not
 * in the state the suite is about to assume, and that has to surface here rather than as a baffling
 * assertion failure several minutes later.
 */
export async function resetPubSubFixtures(pubsub: PubSubFixtureClient, fixtures: PubSubFixtures): Promise<void> {
  await deleteFixtures(pubsub, fixtures, true);
}

/**
 * Best-effort tidy-up for `afterAll`, so an ordinary run leaves the emulator as it found it.
 *
 * It swallows every error and keeps going, because the next run resets these same names anyway:
 * failing the suite over a cleanup race would report a cosmetic problem as a broken test, and
 * stopping at the first failure would strand the resources behind it.
 */
export async function tidyPubSubFixtures(pubsub: PubSubFixtureClient, fixtures: PubSubFixtures): Promise<void> {
  await deleteFixtures(pubsub, fixtures, false);
}

/**
 * Subscriptions first, then topics, then schemas: each depends on the one after it, and Pub/Sub
 * refuses to drop a schema that a live topic still references.
 */
async function deleteFixtures(pubsub: PubSubFixtureClient, fixtures: PubSubFixtures, strict: boolean): Promise<void> {
  const deletions = [
    ...(fixtures.subscriptions ?? []).map((id) => () => pubsub.subscription(id).delete()),
    ...(fixtures.topics ?? []).map((id) => () => pubsub.topic(id).delete()),
    ...(fixtures.schemas ?? []).map((id) => () => pubsub.schema(id).delete()),
  ];

  for (const remove of deletions) {
    try {
      await remove();
    } catch (error) {
      if (strict && !isNotFound(error)) throw error;
    }
  }
}

function isNotFound(error: unknown): boolean {
  return typeof error === "object" && error !== null && (error as { code?: number }).code === NOT_FOUND;
}
