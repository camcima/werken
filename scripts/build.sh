#!/bin/sh
# Dual ESM + CJS build (§2.3), one tsc project-references pass each.
#
# The guard exists because `tsc --build` fails with TS18002 on a root config whose `files` and
# `references` are both empty — the state this repo is in until the first package lands at M1.
# Once packages/*/tsconfig.json exist and are referenced from tsconfig.json, the guard is inert.
set -e

if [ ! -d packages ] || [ -z "$(ls -A packages 2>/dev/null)" ]; then
  echo "build: no packages yet, nothing to compile"
  exit 0
fi

tsc --build
tsc --build tsconfig.cjs.json
sh scripts/add-cjs-package-json.sh
