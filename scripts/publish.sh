#!/bin/sh
# Publishes every public workspace package.
#
# Split out of .release-it.publish.json rather than inlined there because release-it interpolates
# ${...} in hook strings as its own template variables, so shell parameter expansion cannot be used
# in the hook itself.
#
# Run it through `pnpm run release:publish`, which is deliberately NOT --ci: release-it prompts
# before tagging, pushing and creating the GitHub release, and hooks inherit an interactive
# terminal so pnpm can prompt for a one-time password if the registry asks for one.
#
# Under --ci that prompt is impossible and the publish dies with ERR_PNPM_OTP_NON_INTERACTIVE,
# after the tag has already been created. For automation, pass the code in instead:
#
#   NPM_OTP=123456 pnpm exec release-it --no-increment --ci --config .release-it.publish.json
#
# Codes expire in about 30 seconds, so generate one immediately before running.
set -e

if [ -n "$NPM_OTP" ]; then
  exec pnpm -r publish --no-git-checks --otp "$NPM_OTP"
fi

exec pnpm -r publish --no-git-checks
