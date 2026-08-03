import { defineConfig } from "vitest/config";
import { fileURLToPath } from "node:url";

const fromHere = (rel: string) => fileURLToPath(new URL(rel, import.meta.url));

export default defineConfig({
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
        find: /^@werken\/nestjs-google-pubsub$/,
        replacement: fromHere("./packages/nestjs-google-pubsub/src/index.ts"),
      },
    ],
  },
  oxc: {
    decorator: {
      legacy: true,
      emitDecoratorMetadata: true,
    },
  },
  test: {
    globals: true,
    include: ["packages/*/tests/**/*.test.ts", "examples/*/tests/**/*.test.ts"],
    coverage: {
      provider: "v8",
      include: ["packages/*/src/**/*.ts"],
      exclude: ["packages/*/src/index.ts", "packages/*/src/**/types.ts"],
      reporter: ["text", "text-summary", "html", "json"],
      reportsDirectory: "coverage",
    },
  },
});
