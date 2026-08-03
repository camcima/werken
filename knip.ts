import type { KnipConfig } from "knip";

/**
 * Dead-code detection: unused files, exports and dependencies.
 *
 * Run by `pnpm run lint:dead-code` and by the pre-push hook. It earns its place here because this
 * repo just narrowed its public API — once the barrel stops re-exporting the whole engine, an
 * internal symbol that loses its last caller becomes genuinely unreachable rather than
 * "maybe someone imports it", and nothing else in the toolchain notices. `tsc` does not report an
 * unused export, and ESLint only sees one file at a time.
 */
const config: KnipConfig = {
  workspaces: {
    ".": {
      project: ["*.ts"],
    },

    "packages/cloudevents": {
      project: ["src/**/*.ts", "tests/**/*.ts"],
    },

    "packages/nestjs-google-pubsub": {
      entry: [
        // Spawned as child processes by the integration suite rather than imported, so nothing in
        // the module graph points at them and Knip would otherwise call them unused files.
        "tests/integration/dist-smoke-worker.mjs",
        "tests/integration/dist-smoke-worker.cjs",
        "tests/integration/sigterm-worker.mjs",
      ],
      project: ["src/**/*.ts", "tests/**/*.ts"],
      /**
       * `src/internal.ts` is deliberately absent from the package's `exports` map, so Node cannot
       * resolve it from an installed copy — only this repo's tests reach it, through a vitest
       * alias. Teaching Knip the same alias is what makes the report truthful: without it the file
       * looks unused and every symbol behind it looks dead, which would bury any real finding in
       * noise.
       *
       * Deliberately NOT listed as an entry point. An entry's exports are exempt from the
       * unused-export report, and the whole reason this file exists is to keep the internals
       * reviewable — a re-export here that no test uses is exactly the dead code worth hearing
       * about.
       */
      paths: {
        "@werken/nestjs-google-pubsub/internal": ["src/internal.ts"],
      },
      /**
       * Optional on purpose. It is loaded lazily through `optionalRequire`, and every telemetry
       * call degrades to a no-op when it is absent — a consumer who does not want tracing should
       * not have to install it. Knip reports a referenced optional peer as an issue by default,
       * which here describes the design rather than a defect.
       */
      ignoreDependencies: ["@opentelemetry/api"],
    },

    "examples/minimal-consumer": {
      entry: ["src/main.worker.ts"],
      project: ["src/**/*.ts", "tests/**/*.ts"],
    },
  },

  /**
   * Declared at the root so every workspace resolves one copy under pnpm, but imported from inside
   * `packages/` and `examples/` — which reads to Knip as an unused root dependency. They are the
   * peer dependencies the suites exercise; removing any one breaks the build immediately.
   */
  ignoreDependencies: [
    "@google-cloud/pubsub",
    "@nestjs/common",
    "@nestjs/core",
    "@nestjs/microservices",
    "@nestjs/testing",
    "rxjs",
  ],

  // Optional by design: the pre-commit hook runs it only `if command -v gitleaks`.
  ignoreBinaries: ["gitleaks"],
};

export default config;
