/**
 * Loads an optional peer dependency, or returns undefined when it is not installed.
 *
 * Deliberately a plain-JavaScript `.cjs` file: it stays CommonJS in BOTH builds, so `require` is a
 * real function here even when the rest of the package runs as ESM. A bare `require` in an ESM
 * module throws ReferenceError at runtime, and a try/catch around it silently disables the
 * feature — telemetry that no-ops with @opentelemetry/api installed — which is the bug this file
 * exists to prevent. Covered by tests/integration/dist-smoke.integration.test.ts against both
 * built outputs.
 *
 * @param {string} name
 * @returns {unknown | undefined}
 */
function optionalRequire(name) {
  try {
    return require(name);
  } catch {
    return undefined;
  }
}

exports.optionalRequire = optionalRequire;
