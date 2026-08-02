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

Test-driven: write the failing test first. Integration tests run against the Pub/Sub **emulator**,
which supports schemas, so no GCP project or credentials are needed:

```bash
docker run -d --name pubsub-emu -p 8085:8085 \
  gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \
  gcloud beta emulators pubsub start --project=werken-ci --host-port=0.0.0.0:8085

PUBSUB_EMULATOR_HOST=localhost:8085 pnpm vitest run packages/*/tests/integration
```

## Commits

Conventional Commits, enforced by commitlint on `commit-msg`.
