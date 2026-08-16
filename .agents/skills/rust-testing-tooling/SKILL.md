---
name: rust-testing-tooling
description: >-
  Use when setting up a Cargo project or workspace, writing unit/integration/
  doc tests, running or fixing Clippy lints and rustfmt formatting, or
  benchmarking Rust code.
---

Set up and use Rust's tooling — Cargo, testing, Clippy/rustfmt — correctly for a project or workspace.

## Use this skill when

- Configuring `Cargo.toml`, dependency features/versions, or a multi-crate workspace.
- Writing unit tests (`#[test]` in the same file), integration tests (`tests/` directory), or doc tests (examples in `///` comments).
- Running/fixing `cargo clippy` lints or `cargo fmt` formatting issues.
- Benchmarking code (`cargo bench`, `criterion`) or measuring test coverage.
- Deciding test organization: what goes in `#[cfg(test)] mod tests` vs. a top-level `tests/` integration test.

## Do not use this skill when

- The question is about the language feature being tested (ownership, traits, async, error types) rather than the test/tooling setup itself — use the skill matching that feature, then return here for how to structure the test.
- Writing a proc-macro crate's own `Cargo.toml`/testing setup specifically — the proc-macro mechanics are in rust-macros-metaprogramming; this skill covers ordinary crate/workspace testing.

## Instructions

Follow these steps in order. Do the minimum needed; stop when the test/tooling setup works and passes.

1. Identify the task type and open EXACTLY ONE reference file from the list below.
2. For a new project, default to a single crate (`cargo new`) unless there are genuinely multiple deployable/publishable units (a library plus a CLI, multiple related libraries) — only then introduce a Cargo workspace.
3. Put fast, focused unit tests next to the code they test (`#[cfg(test)] mod tests` at the bottom of the file); reserve `tests/` integration tests for exercising the crate's public API as an external consumer would.
4. Write a doc test (a ` ```rust ` block inside a `///` doc comment) for public API examples that should double as both documentation and a compiled/run correctness check — do not duplicate the same example as both a doc test and a unit test.
5. Run `cargo clippy --all-targets --all-features` and `cargo fmt --check` before considering a change complete; fix the underlying pattern Clippy flags rather than adding `#[allow(...)]` unless the lint is a genuine false positive for this specific case.
6. For performance-sensitive code, use `criterion` for statistically sound benchmarks rather than a manual `Instant::now()` timing loop, which is noisy and easy to get wrong (dead-code elimination, warm-up effects).
7. Re-read the test output once and confirm every new test actually exercises the intended behavior (would fail if the implementation were wrong) — a test that passes unconditionally is worse than no test.
8. Stop here.

Anti-loop rules:
- ONE reference file per task.
- Do not add `#[allow(clippy::...)]` to silence a lint without first trying the fix Clippy's message describes.
- Do not keep adding tests for the same code path; stop once the behavior described in the task is covered by a test that would fail without the fix.
- Stop as soon as `cargo test`, `cargo clippy`, and `cargo fmt --check` all pass for the change at hand.

## Reference files

- `references/cargo-workspaces.md` — open for `Cargo.toml` structure, dependency features, and multi-crate workspaces.
- `references/testing-unit-integration-doc.md` — open for `#[test]` unit tests, `tests/` integration tests, doc tests, and test organization (`#[cfg(test)]`, fixtures, `assert!`/`assert_eq!`).
- `references/clippy-rustfmt-lints.md` — open for running/fixing Clippy lints, configuring rustfmt, and benchmarking with `criterion`.
