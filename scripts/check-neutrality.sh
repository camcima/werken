#!/bin/sh
# Acceptance criterion 13 — zero occurrences of employer- or domain-specific nouns in library
# source. This library is meant to be handed over to whichever team ultimately owns it, and to be
# reusable without modification (§1.5). Domain knowledge belongs in the downstream config package.
#
# Scope is packages/*/src only. Docs, tests and examples may reference a domain.
set -e

[ -d packages ] || exit 0

# Proper nouns: unambiguous, so a plain substring match is safe.
PROPER_NOUNS="latam|emantto|ex3|lafken|ramtun|condor|crema"

# Domain nouns that also exist in ordinary technical English. Matched at a word or camelCase
# boundary, then filtered through the allowlist below, so `inFlight` (concurrency) does not trip on
# `flight` (aviation). This is a heuristic: it favours letting a rare real term through over
# blocking contributors on false positives every day.
DOMAIN_NOUNS="flight|aircraft|baggage|bagtag|airport|airline|passenger|boarding"

# Known-good technical terms that contain a domain noun as a substring.
ALLOWLIST="inflight|in-flight|in_flight|single-flight|singleflight|single_flight"

fail=0

proper=$(grep -rniE "$PROPER_NOUNS" \
  --include="*.ts" --include="*.mts" --include="*.cts" --include="*.json" \
  packages/*/src/ 2>/dev/null || true)

# Pass 1 (case-insensitive): the noun starts a word — `flight`, `flightRef`, `"flight"`.
domain_word=$(grep -rniE "(^|[^a-zA-Z])($DOMAIN_NOUNS)" \
  --include="*.ts" --include="*.mts" --include="*.cts" --include="*.json" \
  packages/*/src/ 2>/dev/null | grep -viE "$ALLOWLIST" || true)

# Pass 2 (case-SENSITIVE): the noun starts a camelCase segment — `getFlight`, `bookedAircraft`.
# Must be case-sensitive, or every `inFlight` matches too; the allowlist then rescues the genuine
# technical compounds.
DOMAIN_NOUNS_CAP=$(printf '%s' "$DOMAIN_NOUNS" | sed -E 's/(^|\|)([a-z])/\1\U\2/g')
domain_camel=$(grep -rnE "[a-z]($DOMAIN_NOUNS_CAP)" \
  --include="*.ts" --include="*.mts" --include="*.cts" --include="*.json" \
  packages/*/src/ 2>/dev/null | grep -viE "$ALLOWLIST" || true)

domain=$(printf '%s\n%s' "$domain_word" "$domain_camel" | grep -v '^$' || true)

for hits in "$proper" "$domain"; do
  if [ -n "$hits" ]; then
    echo "$hits" >&2
    fail=1
  fi
done

if [ "$fail" -eq 1 ]; then
  echo "" >&2
  echo "ERROR: domain-specific term in library source (acceptance criterion 13)." >&2
  echo "This library must contain no employer or domain references. If a change seems to require" >&2
  echo "the library to understand a domain concept, the design is wrong — surface it as a question." >&2
  echo "If this is a false positive on ordinary technical English, extend ALLOWLIST in this script." >&2
  exit 1
fi

echo "neutrality: OK"
