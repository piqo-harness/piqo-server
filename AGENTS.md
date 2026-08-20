# Repository Guidelines

## Project Status and Source of Truth

`piqo-server` is an experimental, headless Rust agent harness. The implemented
foundation provides a versioned HTTP/SSE API, durable SQLite-backed sessions and
semantic event logs, OpenAI-compatible Chat Completions and Responses transports,
layered verbatim request-body merging, project-based session grouping, provider
configuration reload, and a loopback-only macOS sidecar.

Text-only conversations are the primary usable path. Tool calls are recorded and
move a run to `requires_action`; tool execution, permission resolution,
orchestration, context compaction, and remote/TLS access are not wired into the
server yet. Do not describe those planned capabilities as implemented.

Consult these documents before changing their respective surfaces:

- `ARCHITECTURE.md` — design constraints, dependency direction, and domain
  principles.
- `README.md` — current behaviour, configuration, CLI use, and sidecar packaging.
- `docs/CLIENT_PROTOCOL.md` — normative client, HTTP, SSE, and sidecar contract.
- `docs/AGENT_HARNESS_ROADMAP.md` — ordered milestone index, dependencies,
  project-wide readiness/completion criteria, and planning-agent instructions.
- `docs/milestones/` — scope, design decisions, implementation slices, required
  tests, acceptance criteria, and exclusions for each planned milestone.

Before planning or implementing milestone work, read
`docs/AGENT_HARNESS_ROADMAP.md` and then the complete document for every
milestone touched by the change. Inspect the current code before relying on a
milestone's status, update the roadmap tracker when its state changes, and use
the roadmap's required handoff format in the tracking issue or pull request.
Milestone documents are planning contracts, not evidence that a capability is
implemented.

When code changes an externally visible behaviour, update the relevant contract
and README material in the same change.

## Workspace Layout and Boundaries

The Cargo workspace uses Rust 2021 and requires Rust 1.88 or newer.

- `piqo-core/` — pure domain types: session projection/state machine,
  permissions, semantic events, and event-log validation. It must remain free
  of IO, async runtimes, and Tokio.
- `piqo-provider/` — outbound provider HTTP/SSE transport and lossless JSON
  request-body merging; depends only on `piqo-core`.
- `piqo-tools/` — tool-runtime and MCP configuration boundary; depends only on
  `piqo-core`. Its execution integration is not yet active in the server.
- `piqo-server/` — Axum API, auth, SSE fanout/replay, SQLite storage,
  configuration, runtime/sidecar lifecycle, migrations, and run supervision.
- `piqo-cli/` — `serve`, `attach`, `run`, and project-management commands; it
  depends on `piqo-server`.
- `piqo-server/migrations/` — ordered, append-only SQLite migrations.
- `piqo-server/tests/` — HTTP/OpenAPI/config and sidecar integration tests.
- `docs/CLIENT_PROTOCOL.md` — API v1 and lifecycle protocol.
- `docs/AGENT_HARNESS_ROADMAP.md` and `docs/milestones/` — implementation
  roadmap and milestone-specific planning contracts.
- `scripts/package-macos-arm64.sh` — macOS arm64 release archive build.

Keep IO at the crate edges and preserve the dependency direction above. Put pure
state-machine, permission, and event-log behaviour in `piqo-core`; do not move
server concerns into it for convenience. Introduce traits only when a genuine
second implementation or runtime heterogeneity requires them; prefer concrete
types and generics otherwise.

## Domain and API Invariants

- The event log is append-only and per-session event IDs are monotonically
  increasing. Derived projections are caches and must stay replayable from the
  log.
- Clients rely on durable history plus live SSE, including `Last-Event-ID`
  reconnection semantics. Preserve ordering and replay/live handoff behaviour.
- Provider request bodies are deliberately `serde_json::Value`. Merge layers
  verbatim with last-writer-wins semantics, then fill only missing harness
  fields. Do not normalize, rename, whitelist, or strongly type provider-owned
  JSON.
- Treat API v1 schemas, error envelopes, OpenAPI output, and the sidecar's
  newline-delimited stdout protocol as public contracts. Make compatibility
  decisions explicitly and cover them with integration tests.
- Configuration reload must atomically replace a valid snapshot and leave the
  active configuration intact when validation fails.

## Build, Test, and Development Commands

Run commands from the repository root. CI uses the locked variants, so use them
before submitting whenever `Cargo.lock` should not change:

```sh
cargo fmt --all -- --check
cargo check --locked --workspace
cargo test --locked --workspace
cargo clippy --locked --workspace --all-targets -- -D warnings
```

Useful focused checks:

```sh
cargo test -p piqo-core
cargo test -p piqo-server --test api_v1
cargo test -p piqo-server --test sidecar
cargo run -p piqo-cli -- serve --config ./piqo.toml --database sqlite://piqo.db
```

Use `cargo fmt --all` to format Rust changes. The sidecar archive is built with
`./scripts/package-macos-arm64.sh`; it requires a macOS arm64-capable toolchain
and produces output under `dist/` by default.

## Implementation Style

Follow `rustfmt` and standard Rust naming: `snake_case` for modules, functions,
and variables; `UpperCamelCase` for types and traits; `SCREAMING_SNAKE_CASE` for
constants. Use `thiserror` for library/domain errors and add contextual
`anyhow` errors at CLI boundaries. Prefer `?` and explicit error propagation to
panics in production paths.

Use `tracing` for operational diagnostics; never log bearer tokens, provider
API keys, raw credentials, or sensitive request/event content. Keep async work
in Tokio/Axum-facing crates, avoid holding locks across `.await`, and make
cancellation/shutdown effects explicit in supervisor code.

## Storage and Migrations

Migrations are deployed state. Never edit, rename, reorder, or delete an
existing file in `piqo-server/migrations/`. Add a new numerically ordered SQL
migration for schema changes, preserving SQLite foreign-key and integrity
guarantees. Update storage code and tests together, including replay/projection
integrity where applicable.

## Testing Expectations

Keep pure unit tests adjacent to the domain logic. In particular, cover session
transitions, projection replay, permission decisions, provider-body merge
behaviour, and malformed upstream provider output without mocks where possible.

Add `piqo-server/tests/` coverage for changes to routes, error envelopes,
OpenAPI, configuration reload, SSE/resume behaviour, auth, or sidecar lifecycle.
Run the focused test for the changed surface, then the full workspace suite for
cross-crate or release-facing changes.

## Security and Configuration

Treat this project as a remote-shell boundary even while tool execution is
incomplete. Keep normal server bindings loopback-only by default; do not add a
public binding, remote auth mode, or TLS behaviour without an explicit security
design.

The embedded `piqo-server` sidecar binds `127.0.0.1:0`, issues a fresh
per-process bearer token, requires it on every HTTP/SSE request, uses a private
`~/.config/piqo/` profile, and has a versioned stdout lifecycle protocol. The
`piqo-cli serve` path is intentionally unauthenticated development mode; do not
present it as safe for remote use.

Keep provider credentials in environment variables such as `api_key_env`; never
commit `piqo.toml` files containing secrets, databases, event logs, request
dumps, or generated `dist/` artifacts. Every future native or MCP tool
invocation must pass through the permission evaluator before execution.

## Git and Pull Requests

The current history uses Conventional Commit-style subjects, usually with a
scope, for example `feat(config): Add runtime configuration reload`. Follow
that convention with short, imperative subjects, and keep unrelated changes in
separate commits.

Pull requests should summarize the design impact, link relevant issues, state
the validation run, and explicitly call out API/protocol, storage/migration,
permission, configuration, or security changes. CI validates formatting,
`--locked` workspace checks and tests, and Clippy with warnings denied for pull
requests targeting `main`.
