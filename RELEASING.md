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
  (no bump): it tags `v<version>` and runs `pnpm -r publish`. You then push the tag. No GitHub
  release is created — the tag is the source of truth and the CHANGELOG holds the notes.

The internal dependency needs no re-pinning. `@werken/nestjs-google-pubsub` depends on
`@werken/cloudevents` as `workspace:^`, which pnpm rewrites to `^<version>` at publish time.

## Prerequisites

- **The `werken` npm organisation exists and you can publish to it.** Both names are currently
  unclaimed, so the scope has to be created before the first release — a scope is not created
  implicitly by publishing.
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

Both packages are still at `0.0.0` and nothing has been published, so the first release is a
judgement call rather than a computed bump: `0.1.0` signals that the API may still move, `1.0.0`
commits to SemVer guarantees from day one. Pass it explicitly either way.

> **Merge content PRs with a merge commit or rebase, not squash.** Squashing collapses the
> individual `feat:`/`fix:`/`!` commits into one, so conventional-changelog can no longer detect the
> bump or list the changes. (The Phase 1 release commit itself is a single commit — squash it
> freely if it goes through a PR.)

## Phase 1 — prepare the version bump

Run from an up-to-date `main`. Replace `0.1.0` with your target version.

```bash
VERSION=0.1.0

# Prepare-only: bumps the root and both packages, writes the CHANGELOG section, and commits
# "chore: release v$VERSION". Nothing is tagged, pushed, or published.
pnpm exec release-it $VERSION --ci
```

Verify, then push:

```bash
pnpm run build && pnpm test
git push origin main
```

## Phase 2 — tag & publish

```bash
VERSION=0.1.0
git checkout main
git pull
pnpm install --frozen-lockfile
pnpm run build && pnpm test          # dist/ must exist — `files` ships only dist

# Tag v$VERSION locally and publish both packages with your local npm credentials.
# --no-increment means "don't bump" — the version is already committed from Phase 1.
pnpm exec release-it --no-increment --ci --config .release-it.publish.json

# Push the tag to record the release. No workflow runs on the tag.
git push origin v$VERSION
```

Verify:

```bash
for p in cloudevents nestjs-google-pubsub; do printf "@werken/%s: " "$p"; npm view "@werken/$p" version; done
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
