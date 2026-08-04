import { fileURLToPath } from "node:url";

const fromHere = (rel: string) => fileURLToPath(new URL(rel, import.meta.url));

/**
 * Settings both the unit and integration runs need. Kept in one place so the two configs cannot
 * drift on module resolution or decorator handling.
 */
export const shared = {
  resolve: {
    // Make `@werken/*` imports inside tests resolve to source, not dist. Without this, tests go
    // through the package barrel and are counted as 0% by v8, because the file path coverage sees
    // does not match the package's compiled file path.
    alias: [
      { find: /^@werken\/cloudevents$/, replacement: fromHere("./packages/cloudevents/src/index.ts") },
      {
        find: /^@werken\/nestjs-google-pubsub\/testing$/,
        replacement: fromHere("./packages/nestjs-google-pubsub/src/testing/index.ts"),
      },
      {
        // Not a published entry point: absent from the package's `exports`, so Node refuses to
        // resolve it from an installed copy. Tests drive the internals through it while the
        // published surface stays small enough to document and support.
        find: /^@werken\/nestjs-google-pubsub\/internal$/,
        replacement: fromHere("./packages/nestjs-google-pubsub/src/internal.ts"),
      },
      {
        find: /^@werken\/nestjs-google-pubsub$/,
        replacement: fromHere("./packages/nestjs-google-pubsub/src/index.ts"),
      },
      {
        // Shared by both packages' and examples' integration suites. Aliased rather than imported
        // by relative path so a test does not have to know how deep it sits.
        find: /^@werken\/test-support$/,
        replacement: fromHere("./tests/support/requires.ts"),
      },
    ],
  },
  oxc: {
    decorator: {
      legacy: true,
      emitDecoratorMetadata: true,
    },
  },
  coverage: {
    provider: "v8" as const,
    include: ["packages/*/src/**/*.ts"],
    exclude: ["packages/*/src/index.ts", "packages/*/src/**/types.ts"],
    reporter: ["text", "text-summary", "html", "json"],
    reportsDirectory: "coverage",
  },
};

/** Files owned by the integration run. Excluded from the unit run so neither reports skips. */
export const INTEGRATION_GLOB = "{packages,examples}/*/tests/integration/**/*.test.ts";
