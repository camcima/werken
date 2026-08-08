# Releasing

How maintainers cut a new `@werken/*` release to npm. Both packages
(`@werken/cloudevents` and `@werken/nestjs-google-pubsub`) are versioned and published **in
lockstep** — they always share the same version number.

Publishing uses **your local npm credentials** (`~/.npmrc`). There is intentionally **no CI publish
workflow**, so a release is never a side effect of merging.

## Tooling

Both phases use [release-it](https://github.com/release-it/release-it) +
[`@release-it/conventional-changelog`](https://github.com/release-it/conventional-changelog):

- **Phase 1** uses [`.release-it.json`](./.release-it.json) as a **prepare-only** config: it bumps
  the root and both packages, generates the `CHANGELOG.md` section, and commits — but does **not**
  tag, push, or publish.
- **Phase 2** uses [`.release-it.publish.json`](./.release-it.publish.json) with `--no-increment`
  (no bump): it tags `v<version>`, **pushes the tag itself** (`git.push`), creates a **GitHub
  release** named `v<version>` (`github.release`), and publishes through
  [`scripts/publish.sh`](./scripts/publish.sh). release-it's own npm plugin is disabled
  (`npm.publish: false`) so the publish goes through that script's `--filter "./packages/*"`
  allowlist, which is what stops an example that forgot `"private": true` from reaching npm.
  The CHANGELOG holds the notes; the GitHub release body is generated from it.

The internal dependency needs no re-pinning. `@werken/nestjs-google-pubsub` depends on
`@werken/cloudevents` as `workspace:^`, which pnpm rewrites to `^<version>` at publish time.

## Prerequisites

- **You can publish to the `werken` scope** — `npm org ls werken` should list you. A scope is not
  created implicitly by publishing, so the organisation has to exist first.
- You are logged in: `npm whoami` prints your username. If it prints `E401`, run `npm login`.
- The GitHub CLI is authenticated (`gh auth status`).
- `main` is green and you are up to date: `git checkout main && git pull`.

> **`main` is not branch-protected in this repo**, so Phase 1 can commit and push straight to
> `main`. If a ruleset is added later, run Phase 1 on a `chore/release-*` branch and merge it via
> PR before Phase 2 — the configs need no change for that.

## Choosing the version

Versions follow [SemVer](https://semver.org/), derived from the
[Conventional Commits](https://www.conventionalcommits.org/) since the last release:

| Commits since last release           | Bump      |
| ------------------------------------ | --------- |
| `fix:`                               | **patch** |
| `feat:`                              | **minor** |
| any `!` or `BREAKING CHANGE:` footer | **major** |

release-it computes the recommended bump from the last git tag, so pass the version explicitly
whenever the history and the registry might disagree — and always for the first release of a line,
where there is nothing to compute from.

> **Merge content PRs with a merge commit or rebase, not squash.** Squashing collapses the
> individual `feat:`/`fix:`/`!` commits into one, so conventional-changelog can no longer detect the
> bump or list the changes. (The Phase 1 release commit itself is a single commit — squash it
> freely if it goes through a PR.)

## Phase 1 — prepare the version bump

Run from an up-to-date `main`. Set `VERSION` to the release you are cutting — deliberately left
empty, so pasting this unedited does nothing rather than cutting whatever number the docs happened
to carry.

The bump is gated on `VERSION` rather than merely checked. `${VERSION:?...}` on its own line would
not do: it aborts a script, but an **interactive** shell only prints the message and carries on to
the next line — which is the paste this guard exists for.

```bash
VERSION= # ← set this first, e.g. VERSION=0.4.0

# Prepare-only: bumps the root and both packages, writes the CHANGELOG section, and commits
# "chore: release v$VERSION". Nothing is tagged, pushed, or published.
if [ -z "$VERSION" ]; then
  echo "VERSION is empty — set it to the release you are cutting, e.g. VERSION=0.4.0" >&2
else
  pnpm exec release-it "$VERSION" --ci
fi
```

Verify, then push:

```bash
pnpm run build && pnpm test
git push origin main
```

## Phase 2 — tag & publish

Phase 2 takes no version argument. `--no-increment` means "publish what Phase 1 already committed",
so there is nothing here to keep in sync with the version — and nothing to get wrong.

The simplest path prompts for the one-time password interactively, which avoids racing its ~30
second expiry:

```bash
git checkout main
git pull
pnpm install --frozen-lockfile
pnpm run build && pnpm test          # dist/ must exist — `files` ships only dist

# Prompts before tagging, pushing, creating the GitHub release, and publishing.
pnpm run release:publish
```

For automation, pass the code in and skip the prompts. Generate it immediately before running:

```bash
NPM_OTP=123456 pnpm exec release-it --no-increment --ci --config .release-it.publish.json
```

Either way release-it pushes the tag and creates the GitHub release — there is no separate
`git push origin v<version>` step.

Verify:

Read the version back out of the repo rather than retyping it. Phase 1 committed it, so this cannot
disagree with what was just released — and a checklist that silently checks the wrong version, or
an empty one, is worse than no checklist.

```bash
VERSION=$(node -p "require('./package.json').version")

for p in cloudevents nestjs-google-pubsub; do printf "@werken/%s: " "$p"; npm view "@werken/$p" version; done
git ls-remote --tags origin "v$VERSION"   # the tag reached the remote
gh release view "v$VERSION" --json name,isDraft

# What actually shipped, from the registry rather than the local build — a stale or missing dist
# publishes silently and cannot be undone.
npm pack "@werken/nestjs-google-pubsub@$VERSION" --silent | xargs tar -tzf | head
```

## Footguns

- **Build before Phase 2.** Both packages ship `files: ["dist"]` and nothing regenerates it during
  publish. Publishing without a build produces packages containing only `package.json` and the
  README, and that cannot be undone — see below.
- **`release-it --dry-run` still executes the lifecycle hooks.** `before:bump` runs `npm version`,
  which **mutates** every package.json. It is not side-effect-free — run it on a throwaway branch
  you can discard, not on `main`.
- **Don't run `pnpm run release -- … --ci`.** pnpm forwards the literal `--`, which yargs treats as
  "end of options", so `--ci` is parsed as a positional and release-it stays **interactive** (then
  hangs in a non-TTY shell). Use `pnpm exec release-it <version> --ci`.
- **npm will ask for a one-time password, and `--ci` cannot prompt for it.** A package being
  created for the first time always triggers the challenge, and accounts set to "auth and writes"
  get it on every publish. release-it runs its hooks non-interactively, so pnpm fails with
  `ERR_PNPM_OTP_NON_INTERACTIVE` **after the tag has already been created**. Pass the code through
  `NPM_OTP`, and generate it immediately before running — codes expire in about 30 seconds.
- **Publishing is irreversible.** npm does not allow re-publishing a version+name, and unpublishing
  public packages is heavily restricted. If a Phase 2 publish fails partway, re-running is safe:
  `pnpm -r publish` **skips versions already on the registry** and publishes only the missing ones.
- **`git push` runs a semgrep pre-push hook (~60s)** — don't kill it early. Tag-only pushes are
  fast.

## Troubleshooting

**npm has a version that `main` never tagged.** This happens if a release published but the tag was
never pushed. Do **not** re-publish that version: move to the next version number, document the
orphaned one, and release forward.

**`pnpm -r publish` fails with `ENEEDAUTH`.** Your `~/.npmrc` token has expired. Run `npm login` and
re-run Phase 2 — already-published packages are skipped.
