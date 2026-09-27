// Loads every published entry point both ways and checks the two module formats agree.
import assert from "node:assert/strict";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

// The Nest package depends on @werken/cloudevents. If npm fetched that from the registry instead of
// using the tarball installed alongside it, this would be testing the last published release.
const fromPubsub = createRequire(require.resolve("@werken/nestjs-google-pubsub"));
assert.equal(
  fromPubsub.resolve("@werken/cloudevents"),
  require.resolve("@werken/cloudevents"),
  "@werken/nestjs-google-pubsub resolved a different copy of @werken/cloudevents than the packed one",
);

const entryPoints = {
  "@werken/cloudevents": "parseEnvelope",
  "@werken/nestjs-google-pubsub": "createEventPublisher",
  "@werken/nestjs-google-pubsub/testing": "createWerkenTestHarness",
};

for (const [specifier, expected] of Object.entries(entryPoints)) {
  const esm = await import(specifier);
  const cjs = require(specifier);

  assert.equal(typeof esm[expected], "function", `${specifier}: ESM is missing ${expected}`);
  assert.equal(typeof cjs[expected], "function", `${specifier}: CommonJS is missing ${expected}`);
  // "default" appears only on the ESM namespace of a CommonJS-interop module; ignore it.
  const names = (m) =>
    Object.keys(m)
      .filter((k) => k !== "default")
      .sort();
  assert.deepEqual(names(cjs), names(esm), `${specifier}: ESM and CommonJS export different names`);
  console.log(`check-packed: ${specifier} loads as ESM and CommonJS`);
}
