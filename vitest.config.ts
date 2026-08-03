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
    coverage: shared.coverage,
  },
});
