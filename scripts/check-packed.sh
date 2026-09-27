#!/bin/sh
# Installs the packed tarballs into a throwaway consumer and loads every published entry point the
# way a user would: ESM import, CommonJS require, and TypeScript declarations under both Node16
# resolution modes and the older node10 one.
#
# Nothing else here would notice a broken package. The test suites alias @werken/* to source, and
# the build only proves dist/ compiles. A file missing from `files`, a wrong path in `exports`, or a
# declaration that does not resolve once installed would all pass CI and fail for the first user.
#
# Run after `pnpm run build`. Needs network access for the peer dependencies npm installs.
set -e

root=$(pwd)
for pkg in packages/*/; do
  if [ ! -d "$pkg/dist/cjs" ]; then
    echo "check-packed: $pkg has no dist/cjs; run 'pnpm run build' first" >&2
    exit 1
  fi
done

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/tarballs"

# pnpm, not npm, so workspace:^ dependency ranges are rewritten to real versions as on publish.
for pkg in packages/*/; do
  (cd "$pkg" && pnpm pack --pack-destination "$work/tarballs" >/dev/null)
done

cp -R "$root/scripts/packed-consumer" "$work/consumer"
cd "$work/consumer"

# npm rather than pnpm: a flat node_modules is what most consumers have, and it keeps this
# directory out of the workspace. Required peers are installed automatically. The optional
# @nestjs/testing is named so the ./testing entry point can load.
npm install --no-audit --no-fund --no-package-lock --loglevel=error \
  "$work"/tarballs/*.tgz "@nestjs/testing@^11" >/dev/null

node runtime.mjs
"$root/node_modules/.bin/tsc" -p tsconfig.node16.json
"$root/node_modules/.bin/tsc" -p tsconfig.node10.json
echo "check-packed: OK"
