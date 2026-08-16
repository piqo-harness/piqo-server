---
name: rust-error-handling
description: >-
  Use when handling Result/Option in Rust, propagating errors with the ?
  operator, deciding between panic! and returning an error, or designing
  custom error types with thiserror/anyhow.
---

Handle Rust errors idiomatically — `Result`/`Option`, the `?` operator, and custom error types — without panicking on recoverable failures.

## Use this skill when

- Writing functions that return `Result<T, E>` or `Option<T>`, and propagating failures with `?`.
- Deciding whether a failure should `panic!`/`unwrap`/`expect` or be returned as an `Err`.
- Designing a custom error enum, implementing `std::error::Error`, or choosing `thiserror` vs `anyhow` vs a hand-written error type.
- Converting between error types at a function boundary (`From<OtherError> for MyError`, `.map_err(...)`).
- Handling errors from async code, iterators (`Result` inside `?` in a loop), or `main` returning `Result`.

## Do not use this skill when

- The task is basic `Option` pattern matching with no propagation/custom error design — that's covered briefly in rust-fundamentals; use this skill once `?` or custom error types are involved.
- The concurrency/async structure itself (spawning, channels, `Send` bounds) is the focus, not the error type flowing through it — use rust-concurrency, then return here for the error type design.
- The error needs to cross an FFI boundary (C error codes, `errno`) — use rust-interop for the boundary conversion, this skill for the Rust-side error type.

## Instructions

Follow these steps in order. Do the minimum needed; stop when errors are handled correctly and propagate with an appropriate type.

1. Identify the task type and open EXACTLY ONE reference file from the list below.
2. Default to `Result<T, E>` for recoverable failures (parsing, I/O, network, validation) and `Option<T>` only for "value may legitimately be absent, no error information needed."
3. Never use `.unwrap()`/`.expect()`/`panic!` for input that can come from outside the program's control (user input, network, files, env vars) — propagate with `?` instead. Reserve panics for programmer errors / violated invariants that indicate a bug (e.g., an index computed to always be in range).
4. Propagate with `?` rather than manual `match { Ok(v) => v, Err(e) => return Err(e) }`.
5. For a library crate (reusable by others), define a specific error enum (typically with `thiserror`) so callers can match on failure kinds. For an application binary's top-level error handling, `anyhow::Result`/`anyhow::Error` is usually sufficient and less boilerplate.
6. When a function's `?` needs to convert between error types, implement `From<SourceError> for MyError` (so `?` converts automatically) rather than `.map_err(...)` at every call site.
7. Re-read the changed code once and confirm there is no `.unwrap()`/`.expect()` left on a fallible operation whose failure the caller should see.
8. Stop here.

Anti-loop rules:
- ONE reference file per task.
- Do not add `.unwrap()` to silence a type error without first checking whether returning `Result`/propagating with `?` is the correct fix.
- Do not wrap every error in `anyhow::Error` inside a library crate meant to be consumed by other code — that erases the caller's ability to match on error kind; use `thiserror` there instead.
- Stop as soon as the function's error path compiles and returns/propagates the right type.

## Reference files

- `references/result-option-question-mark.md` — open for `Result`/`Option` basics, the `?` operator, and combinators (`map`, `and_then`, `unwrap_or`, ...).
- `references/panics-custom-errors.md` — open for panic vs. `Result` decisions, `std::error::Error`, and designing custom error types with `thiserror`/`anyhow`.
