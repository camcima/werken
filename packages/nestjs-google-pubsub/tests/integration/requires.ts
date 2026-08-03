/**
 * Skip policy for the integration suite.
 *
 * These tests need a Pub/Sub emulator, a Postgres, or a built `dist/`, so they skip when those are
 * absent — a bare `pnpm test` has to work without Docker. That silence is exactly what makes the
 * arrangement dangerous in CI: a renamed environment variable, or an emulator that failed to come
 * up, would skip every one of them and still report success, and it would look identical to a
 * healthy run because "some tests skipped" is already normal in the unit job.
 *
 * `WERKEN_REQUIRE_INTEGRATION=1` turns that skip into a hard failure. CI sets it, so the integration
 * job can only pass by actually running.
 */
const REQUIRED = process.env.WERKEN_REQUIRE_INTEGRATION === "1";

/**
 * Returns whether to skip, for `describe.skipIf`. Throws instead when integration is required.
 *
 * @param dependency what is missing, named as the developer would set it
 * @param available whether it is present
 */
export function skipUnlessAvailable(dependency: string, available: unknown): boolean {
  const present = Boolean(available);
  if (present) return false;

  if (REQUIRED) {
    throw new Error(
      `werken: ${dependency} is unavailable, but WERKEN_REQUIRE_INTEGRATION=1 says the integration ` +
        "suite must run. Refusing to skip: a silently skipped integration suite passes CI while " +
        "testing nothing. Start the dependencies with `docker compose up -d` (see CONTRIBUTING.md).",
    );
  }
  return true;
}
