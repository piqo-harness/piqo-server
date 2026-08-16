# Cargo and Workspaces

## `Cargo.toml` basics

```toml
[package]
name = "my_crate"
version = "0.1.0"
edition = "2024"

[dependencies]
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }

[dev-dependencies]
criterion = "0.5"

[[bin]]
name = "my_cli"
path = "src/bin/my_cli.rs"
```

- `edition = "2024"` opts into the current edition's syntax/defaults (editions are opt-in per crate and don't break interop between crates on different editions).
- Pin dependency versions with a minimum-compatible range (`"1"` means `>=1.0.0, <2.0.0`) unless there's a specific reason to pin exactly.
- Only enable the `features` a crate actually uses — unused features increase compile time and binary size, and can pull in unwanted transitive dependencies.

## Common feature-flag patterns

```toml
[features]
default = ["std"]
std = []
serde = ["dep:serde"] # optional dependency, only compiled when this feature is enabled
```

```toml
[dependencies]
serde = { version = "1", optional = true }
```

Use `cargo tree -e features` (or `cargo tree --duplicates`) to inspect the actual dependency/feature graph when a build pulls in more than expected.

## Workspaces

A workspace groups multiple crates that share one `Cargo.lock` and target directory — use it once a project has genuinely separate publishable/deployable units (e.g., a core library plus a CLI plus a proc-macro crate):

```toml
# Cargo.toml at the workspace root — no [package] section here
[workspace]
members = ["core", "cli", "macros"]
resolver = "2"

[workspace.dependencies]
serde = { version = "1", features = ["derive"] }
```

Each member crate then references the shared dependency version:

```toml
# core/Cargo.toml
[dependencies]
serde = { workspace = true }
```

`resolver = "2"` (the default for edition 2021+) resolves features independently per target (avoiding features unnecessarily leaking between, e.g., a crate's normal build and its test build) — do not downgrade to resolver `"1"` without a specific reason.

## Useful Cargo commands

```bash
cargo check              # type-check without producing a binary (fastest feedback loop)
cargo build --release    # optimized build
cargo run --bin my_cli
cargo test
cargo doc --open         # build and open rendered documentation
cargo tree               # print the dependency graph
cargo update -p serde    # update one dependency within its Cargo.toml-allowed range
cargo add serde --features derive   # add a dependency from the command line
```

Prefer `cargo check` over `cargo build` while iterating — it skips code generation and is significantly faster for catching type errors.

## `.cargo/config.toml`

Project- or user-level Cargo configuration (build target defaults, registry mirrors, linker settings):

```toml
# .cargo/config.toml
[build]
target = "x86_64-unknown-linux-gnu"

[target.x86_64-unknown-linux-gnu]
linker = "clang"
```

## Stop conditions for this file

- `Cargo.toml` declares only the dependencies and features the crate actually uses.
- A workspace was introduced only once there are genuinely multiple separately-versioned/published crates, not as a default for every new project.
- `cargo check`/`cargo build` succeed with the intended feature flags before moving on to writing tests.
