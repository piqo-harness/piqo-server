# Repository Guidelines

## Project Structure & Module Organization

This repository contains the initial Rust implementation. `ARCHITECTURE.md` is the source of truth for the design and the workspace currently contains these crates:

- `piqo-core/` — pure session state, permissions, and event-log domain logic.
- `piqo-provider/` — outbound LLM HTTP/SSE transport and verbatim JSON body merging.
- `piqo-tools/` — native tools and MCP client integration.
- `piqo-server/` — axum HTTP/SSE API and session supervision.
- `piqo-cli/` — `serve`, `attach`, and one-shot command-line entry points.

Keep IO at the crate edges. Preserve the dependency direction described in `ARCHITECTURE.md`, and do not strongly type provider request bodies that must pass through unchanged.

## Build, Test, and Development Commands

Use the standard workflow from the repository root:

```sh
cargo fmt --all -- --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Run the CLI locally with `cargo run -p piqo-cli -- serve`. Add project-specific commands here when they become authoritative.

## Coding Style & Naming Conventions

Use Rust 2021 conventions and let `rustfmt` define formatting (four-space indentation, no manual alignment). Use `snake_case` for modules, functions, and variables; `UpperCamelCase` for types and traits; and `SCREAMING_SNAKE_CASE` for constants. Prefer straightforward generics and concrete types; introduce trait objects only where runtime heterogeneity is required.

## Testing Guidelines

Keep domain tests close to the pure logic, especially exhaustive permission-evaluator and state-machine cases. Add integration tests for HTTP/SSE behavior, event-log replay, and request-body preservation. Security-sensitive tests should cover command parsing and remote binding/authentication. Run `cargo test --workspace` before submitting changes.

## Commit & Pull Request Guidelines

Only an initial commit exists, so no repository-specific commit format is established. Use short, imperative subjects such as `Add event log replay`, and keep unrelated changes separate. Pull requests should explain the design impact, link relevant issues, include tests or a reason none apply, and call out API, permission, storage, or security changes explicitly.

## Security & Configuration

Treat the server as a remote shell: require authentication, use TLS for non-local access, avoid binding to `0.0.0.0` by default, and route every tool invocation through the permission evaluator. Never commit credentials, model endpoints containing secrets, or session/event-log data.
