import { defineConfig } from "vitest/config";
import { INTEGRATION_GLOB, shared } from "./vitest.shared.js";

/**
 * The integration run: the emulator, Postgres and built-dist tests. Driven by
 * `pnpm run test:integration`, which also sets WERKEN_REQUIRE_INTEGRATION=1 so a missing backend
 * fails rather than skips.
 */
export default defineConfig({
  resolve: shared.resolve,
  oxc: shared.oxc,
  test: {
    globals: true,
    include: [INTEGRATION_GLOB],
    // The SIGTERM test spawns a real worker against the emulator and needs room beyond the 5s default.
    testTimeout: 30_000,
    hookTimeout: 30_000,
    coverage: shared.coverage,
  },
});
