# Contributing

## Setup

```bash
pnpm install
pnpm run hooks:install   # required — see below
```

Node **22 or 24**. `engines` declares `>=22` and CI runs both majors as a matrix — for unit and
integration alike, since the SIGTERM drain test spawns a child process and is the most
Node-version-sensitive code here. `.nvmrc` pins 24 for local development; either is supported.

### Why `hooks:install` is a separate step

pnpm 11 does not run lifecycle scripts by default (`ignore-scripts` is on), so neither this repo's
`prepare` script nor lefthook's own postinstall will install the git hooks for you. Without them the
pre-commit and pre-push gates silently do not run.

CI runs every gate regardless, so a missed hook costs you a red build rather than a bad merge — but
run the command anyway so you find problems locally.

## Quality gates

Both lefthook and CI run these. `pnpm run <name>`:

| Gate                     | What it enforces                                                          |
| ------------------------ | ------------------------------------------------------------------------- |
| `build`                  | Dual ESM + CJS compile via project references                             |
| `lint`                   | ESLint, including a `no-restricted-imports` rule for Nest internals       |
| `format:check`           | Prettier                                                                  |
| `typecheck`              | Vitest typecheck pass                                                     |
| `test` / `test:coverage` | Vitest; coverage target 80% project-wide                                  |
| `lint:no-deep-imports`   | Acceptance criterion 11 — no `@nestjs/*/dist/*` or `/internal/*` imports  |
| `lint:neutrality`        | Acceptance criterion 13 — no employer or domain nouns in `packages/*/src` |

The last two exist because this library is meant to be handed to another team and reused unmodified.
If a change seems to require the library to understand a domain concept, the design is wrong —
raise it rather than adding the term to the denylist.

## Tests

Test-driven: write the failing test first.

The suite is split in two, and each half runs in its own CI job:

| Command                     | Covers                | Needs                  |
| --------------------------- | --------------------- | ---------------------- |
| `pnpm test`                 | unit tests            | nothing                |
| `pnpm run test:integration` | the integration suite | `docker compose up -d` |

Neither command reports skipped tests, and that is deliberate. The two suites are separated by
config — `vitest.config.ts` excludes `tests/integration/**`, and `vitest.integration.config.ts`
owns it — rather than by runtime `skipIf` alone. A run that reports "6 skipped" is
indistinguishable from one where those tests silently stopped working, and that ambiguity is the
problem worth removing. On top of that, `pnpm run test:integration` sets
`WERKEN_REQUIRE_INTEGRATION=1`, which turns a missing backend into a failure instead of a skip. CI
sets the same flag, so the integration job cannot go green without actually running.

Integration tests use the Pub/Sub **emulator**, which supports schemas, so no GCP project or
credentials are needed:

```bash
docker compose up -d           # Pub/Sub emulator + Postgres, images matching CI
pnpm run build                 # the SIGTERM and dist-smoke tests run against dist/
pnpm run test:integration
docker compose down
```

The compose Postgres publishes on **55432**, not 5432, because a developer machine very often
already runs one there. That collision is silent — the container simply fails to publish and the
tests then talk to your database instead.

## Commits

Conventional Commits, enforced by commitlint on `commit-msg`.
