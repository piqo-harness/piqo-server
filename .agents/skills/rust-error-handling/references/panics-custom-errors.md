# Panics vs. Result, and Custom Error Types

## When to panic vs. return `Result`

Panic (`panic!`, `.unwrap()`, `.expect()`, array out-of-bounds indexing, integer overflow in debug builds) is for **unrecoverable programmer errors** — situations that indicate a bug, not an expected failure mode:

```rust
// OK to panic: this invariant is guaranteed by the code just above it
let first = v.first().expect("v is guaranteed non-empty by the caller's precondition");
```

Return `Result` for **anything that can fail because of the outside world**: user input, file/network I/O, parsing, configuration, another service's response.

```rust
// NOT ok to panic: user-controlled input
fn parse_port(input: &str) -> Result<u16, std::num::ParseIntError> {
    input.parse()
}
```

Prefer `.expect("message explaining why this can't fail")` over bare `.unwrap()` even in the rare cases where panicking is acceptable — it documents the assumed invariant for the next reader.

## Implementing `std::error::Error` by hand

```rust
use std::fmt;

#[derive(Debug)]
enum ConfigError {
    MissingField(String),
    InvalidValue { field: String, value: String },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::MissingField(field) => write!(f, "missing field: {field}"),
            ConfigError::InvalidValue { field, value } => {
                write!(f, "invalid value '{value}' for field '{field}'")
            }
        }
    }
}

impl std::error::Error for ConfigError {}
```

This is verbose and easy to get wrong (forgetting `Display`, not chaining `source()`) — prefer `thiserror` for anything beyond a trivial one-variant error.

## `thiserror`: for library crates

`thiserror` derives `Display` and `std::error::Error` from attributes, and lets `#[from]` generate the `From` impl `?` needs:

```rust
use thiserror::Error;

#[derive(Error, Debug)]
enum ConfigError {
    #[error("missing field: {0}")]
    MissingField(String),

    #[error("invalid value '{value}' for field '{field}'")]
    InvalidValue { field: String, value: String },

    #[error("failed to read config file")]
    Io(#[from] std::io::Error), // enables `?` to convert io::Error automatically

    #[error("failed to parse config")]
    Parse(#[source] toml::de::Error), // #[source] chains the cause without auto-`From`
}
```

Use `thiserror` in library/reusable code so callers get a concrete, matchable error enum instead of an opaque error type.

## `anyhow`: for application/binary code

`anyhow::Error` is a type-erased, boxed error good enough when the top-level caller (typically `main`, or a CLI/service entry point) only needs to log/display the error, not match on its kind:

```rust
use anyhow::{Context, Result};

fn load_config(path: &str) -> Result<Config> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config at {path}"))?;
    let config: Config = toml::from_str(&raw)
        .context("failed to parse config as TOML")?;
    Ok(config)
}
```

`.context()`/`.with_context()` attach a human-readable message while preserving the original error as the source chain (visible via `{:#}`/`{:?}` formatting or `anyhow::Error::chain()`).

## Choosing between them

| | `thiserror` | `anyhow` |
|---|---|---|
| Best for | library crates whose callers need to match on error kind | application/binary top-level error handling |
| Error type | a concrete enum you define | `anyhow::Error`, effectively `Box<dyn Error>` with context |
| Caller can match variants | yes | no (by design) |

A common pattern: internal library modules use `thiserror` enums; the binary's `main` (or a top-level handler) uses `anyhow::Result` and lets `?` convert any `thiserror` error into `anyhow::Error` automatically (blanket `impl From<E: std::error::Error> for anyhow::Error` is provided by the crate).

## Stop conditions for this file

- No panic-prone call (`.unwrap()`, `.expect()`, unchecked indexing) remains on a path reachable from external input.
- A library crate's public API returns a concrete, matchable error type (typically `thiserror`-derived), not `anyhow::Error` or `Box<dyn Error>`.
- Every custom error variant carries enough context (via `#[error("...")]` message or `.context()`) to diagnose the failure without a debugger.
