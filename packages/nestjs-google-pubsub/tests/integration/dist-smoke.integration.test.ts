import { execFile } from "node:child_process";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { describe, expect, test } from "vitest";
import { skipUnlessAvailable } from "./requires.js";

const run = promisify(execFile);
const DIST = fileURLToPath(new URL("../../dist/index.js", import.meta.url));

interface SmokeResult {
  telemetryLoaded: boolean;
  traceparent: string | null;
}

async function smoke(worker: string): Promise<SmokeResult> {
  const { stdout } = await run(process.execPath, [fileURLToPath(new URL(worker, import.meta.url))]);
  return JSON.parse(stdout) as SmokeResult;
}

/**
 * The unit suite runs source through vitest's module runner, where CJS/ESM differences blur — a
 * bare `require` in the ESM build only fails under plain Node. These spawn real Node against the
 * built dist, once per module system, and assert the optional OpenTelemetry peer actually loads:
 * spans are recorded and published events carry ce-traceparent. Requires `pnpm run build` first.
 */
describe.skipIf(skipUnlessAvailable("a built dist/ (run `pnpm run build`)", existsSync(DIST)))(
  "built package loads optional OpenTelemetry",
  () => {
    test("ESM build records spans and stamps ce-traceparent", async () => {
      const result = await smoke("./dist-smoke-worker.mjs");

      expect(result.telemetryLoaded).toBe(true);
      expect(result.traceparent).toMatch(/^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/);
    });

    test("CJS build records spans and stamps ce-traceparent", async () => {
      const result = await smoke("./dist-smoke-worker.cjs");

      expect(result.telemetryLoaded).toBe(true);
      expect(result.traceparent).toMatch(/^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/);
    });
  },
);
