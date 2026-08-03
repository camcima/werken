#!/bin/sh
# Publishes every public workspace package.
#
# Split out of .release-it.publish.json rather than inlined there because release-it interpolates
# ${...} in hook strings as its own template variables, so shell parameter expansion cannot be used
# in the hook itself.
#
# npm challenges for a one-time password on publish — always for a package being created for the
# first time, and on every publish for accounts set to "auth and writes". release-it runs hooks
# non-interactively under --ci, so pnpm cannot prompt and fails with ERR_PNPM_OTP_NON_INTERACTIVE.
# Pass the code through NPM_OTP:
#
#   NPM_OTP=123456 pnpm exec release-it --no-increment --ci --config .release-it.publish.json
#
# Codes expire in about 30 seconds, so generate one immediately before running.
set -e

if [ -n "$NPM_OTP" ]; then
  exec pnpm -r publish --no-git-checks --otp "$NPM_OTP"
fi

exec pnpm -r publish --no-git-checks
