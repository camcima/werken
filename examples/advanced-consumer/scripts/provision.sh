#!/bin/sh
# Provisions everything the advanced consumer and the publisher need, against the local emulator
# and Postgres from docker-compose.yml.
#
# Werken never provisions Pub/Sub resources itself — that belongs in Terraform or your platform
# catalogue, in dev as much as in production. This script exists so the example is runnable, not
# because the library will do it for you.
set -e

: "${PUBSUB_EMULATOR_HOST:=localhost:8085}"
: "${GCP_PROJECT_ID:=werken-dev}"
: "${DATABASE_URL:=postgresql://postgres:postgres@localhost:55432/werken_test}"
export PUBSUB_EMULATOR_HOST GCP_PROJECT_ID DATABASE_URL

node "$(dirname "$0")/provision.mjs"
