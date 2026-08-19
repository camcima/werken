import { defineConfig } from "vitest/config";
import { INTEGRATION_GLOB, shared } from "./vitest.shared.js";

/**
 * The unit run. Integration tests are excluded rather than left to skip themselves: a run that
 * reports "6 skipped" is indistinguishable from one where those tests silently stopped working,
 * and that ambiguity is the whole problem. They are owned by vitest.integration.config.ts, which
 * refuses to skip them.
 */
export default defineConfig({
  resolve: shared.resolve,
  oxc: shared.oxc,
  test: {
    globals: true,
    include: ["packages/*/tests/**/*.test.ts", "examples/*/tests/**/*.test.ts"],
    exclude: ["**/node_modules/**", "**/dist/**", INTEGRATION_GLOB],
    /**
     * Well above what any unit test needs, because the 5s default is not a budget for the test —
     * it is a budget for the test plus whatever it loads on the way.
     *
     * The first test in a file that calls `listen()` pays a lazy `require` of
     * `@google-cloud/pubsub`, which is gRPC and protobufs and costs ~800ms on an idle machine.
     * Multiply that across parallel workers on a loaded box — CI, or the pre-push hook running
     * this alongside tsc, knip and semgrep — and a test that normally takes 20ms times out. It
     * presents as a different test each run, always as a timeout and never as an assertion, which
     * is the signature of contention rather than a defect.
     *
     * A genuine hang is not what this hides: the handlers that deliberately never settle are
     * bounded by `shutdownDrainTimeoutMs`, not by this.
     */
    testTimeout: 15_000,
    coverage: shared.coverage,
  },
});
