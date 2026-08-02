#!/bin/sh
# Acceptance criterion 11 — no import from a Nest path containing /dist/ or /internal/.
#
# Nest 12's ESM migration calls out deep imports as a breaking hazard, and a transport strategy is
# exactly the kind of code that tends to reach inside. Where a type is not publicly exported,
# re-declare a minimal structural type locally instead (§2.3).
set -e

[ -d packages ] || exit 0

matches=$(grep -rnE "from ['\"][^'\"]*@nestjs/[^'\"]*/(dist|internal)/" \
  --include="*.ts" --include="*.mts" --include="*.cts" \
  packages/*/src/ 2>/dev/null || true)

if [ -n "$matches" ]; then
  echo "ERROR: deep import into Nest internals (acceptance criterion 11)." >&2
  echo "$matches" >&2
  echo "" >&2
  echo "Import from a public entry point (@nestjs/common, @nestjs/core, @nestjs/microservices)." >&2
  echo "If the type is not publicly exported, re-declare a minimal structural type locally." >&2
  exit 1
fi

echo "no-deep-imports: OK"
