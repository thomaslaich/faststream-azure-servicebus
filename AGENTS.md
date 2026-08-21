# AGENTS.md

Guidance for AI coding agents working in **faststream-azure-servicebus**.
This file is the short, operational version of the repository context.

## What This Repo Is

`faststream-azure-servicebus` is an Azure Service Bus broker implementation for
[FastStream](https://github.com/ag2ai/faststream). FastStream does not ship a
Service Bus broker, so this package supplies the broker, publisher, subscriber,
parser, and message settlement pieces needed to use Service Bus queues and topic
subscriptions with FastStream.

The package intentionally depends on FastStream private internals under
`faststream._internal`. Treat FastStream upgrades as compatibility-sensitive and
run the full test suite after touching broker, subscriber, parser, or response
code.

## Common Commands

This repo is driven by [`just`](https://github.com/casey/just); run `just` to
list recipes. Key ones:

| Command | What it does |
| --- | --- |
| `just fmt` | Format the repository with treefmt. |
| `just lint` | Run Ruff linting. |
| `just typecheck` | Run `ty check`. |
| `just check` | Local static analysis: treefmt, lint, typecheck. |
| `just ci` | CI static analysis: treefmt check, GitHub-formatted lint, typecheck. |
| `just up` / `just down` | Start / stop the Service Bus emulator stack. |
| `just test` | Run the connected pytest suite with xdist. |
| `just test-cov` | Run tests with a local coverage report. |
| `just test-ci` | CI test recipe with coverage data but no terminal report. |
| `just build` | Build the Python distribution with `uv build`. |

Use the `just` recipes in automation instead of spelling out the underlying
`uv`, `pytest`, `ruff`, or `ty` commands in workflow files.

The contributor toolchain is managed by devenv. Run `direnv allow` or enter it
manually with `devenv shell` before invoking the recipes.

## Repository Layout

- `src/faststream_azure_servicebus/` - package source.
- `broker/` - FastStream broker usecase and registration API.
- `subscriber/` - queue/topic-subscription receivers and consume loops.
- `publisher/` - Service Bus producer and reply publisher adapter.
- `configs/` - client connection state and broker config.
- `schemas/` - destination models for queues and topic subscriptions.
- `tests/` - connected tests against the Service Bus emulator or namespace.
- `tests/infra/servicebus-config.json` - fixed emulator entity pool.
- `.github/workflows/` - PR, main-branch, and nightly FastStream-main checks.

## Conventions And Gotchas

- Tests are connected tests. Start the emulator with `just up` before running
  `just test` locally, unless `SERVICEBUS_CONNECTION_STRING` points at a real
  namespace.
- The emulator cannot create entities at runtime. Tests lease names from the
  fixed pool in `tests/infra/servicebus-config.json`; regenerate it with
  `just gen-config` after changing queue/topic counts.
- The emulator caps a namespace at 10 concurrent connections, so test recipes
  use `-n 2` rather than `-n auto`.
- Do not vendor FastStream or add a git submodule for it. The nightly workflow
  installs `faststream` from upstream `main` to catch private-API drift.
- Keep message settlement semantics explicit: `ack()` completes,
  `nack()` abandons, and `reject()` dead-letters in peek-lock mode.
- `AckPolicy.ACK_FIRST` maps to Service Bus `RECEIVE_AND_DELETE`, so a handler
  failure after receive cannot redeliver the message.

## CI And Release Shape

Workflow names describe when and why they run:

- `PR Validation` runs for pull requests.
- `CI` runs for pushes to `main`.
- `Nightly (faststream@main)` checks compatibility against FastStream upstream.

The release workflow follows the standard Python trusted-publishing shape:
release-triggered, tag-aware checkout, pinned build tooling, `uv build`, smoke
tests from the built wheel and sdist, then `uv publish` to PyPI.

## Commit / PR Conventions

Branch off `main`. Prefer Conventional Commits (`feat:`, `fix:`, `chore:`,
`docs:`, `refactor:`) and keep changes narrowly scoped.
