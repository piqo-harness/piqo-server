# Unit, Integration, and Doc Tests

## Unit tests

Live in the same file as the code, inside a `#[cfg(test)] mod tests` block — compiled only when running tests, and can access private items:

```rust
fn add(a: i32, b: i32) -> i32 {
    a + b
}

#[cfg(test)]
mod tests {
    use super::*; // bring the parent module's items into scope

    #[test]
    fn adds_two_positive_numbers() {
        assert_eq!(add(2, 3), 5);
    }

    #[test]
    #[should_panic(expected = "divide by zero")]
    fn panics_on_zero_division() {
        let _ = 1 / (0 - 0);
        panic!("divide by zero");
    }

    #[test]
    fn parses_ok() -> Result<(), std::num::ParseIntError> {
        let n: i32 = "42".parse()?; // tests can return Result and use `?`
        assert_eq!(n, 42);
        Ok(())
    }
}
```

Assertion macros: `assert!(cond)`, `assert_eq!(a, b)`, `assert_ne!(a, b)` — all accept an optional format-string message as trailing arguments (`assert_eq!(a, b, "context: {ctx}")`).

## Integration tests

Files under `tests/` at the crate root are compiled as separate crates that only see the library's `pub` API — exactly what an external consumer would see:

```
my_crate/
├── src/
│   └── lib.rs
└── tests/
    └── api_test.rs
```

```rust
// tests/api_test.rs
use my_crate::add;

#[test]
fn public_api_adds_correctly() {
    assert_eq!(add(2, 3), 5);
}
```

Share test helpers across multiple integration test files via `tests/common/mod.rs` (a module, not itself a test file — no `#[test]` functions in it):

```
tests/
├── common/
│   └── mod.rs
├── api_test.rs
└── error_test.rs
```

```rust
// tests/api_test.rs
mod common;

#[test]
fn uses_shared_setup() {
    let fixture = common::setup();
    assert!(fixture.is_ready());
}
```

## Doc tests

Code blocks inside `///` doc comments are compiled and run as tests by default:

```rust
/// Adds two numbers together.
///
/// ```
/// assert_eq!(my_crate::add(2, 3), 5);
/// ```
pub fn add(a: i32, b: i32) -> i32 {
    a + b
}
```

Use ` ```rust,no_run ` for an example that should compile but not execute (e.g., it opens a real network connection), and ` ```rust,ignore ` only when the snippet is illustrative pseudocode that can't compile as-is (avoid this — an ignored doc test gives no correctness guarantee at all).

## Choosing where a test belongs

| Test type | Use for |
|---|---|
| Unit test (`#[cfg(test)] mod tests`) | Fast checks of private functions/internal logic, edge cases, error paths. |
| Integration test (`tests/`) | End-to-end behavior of the crate's public API, exactly as an external caller would use it. |
| Doc test (`///` example) | An example that's valuable as both documentation and a compiled/run correctness check — keep these focused and few; don't use them for exhaustive edge-case coverage. |

## Running tests

```bash
cargo test                       # everything: unit, integration, doc tests
cargo test parses_ok             # filter by substring match on test name
cargo test -- --nocapture        # show println! output even for passing tests
cargo test --doc                 # doc tests only
cargo test -p my_crate           # a specific crate in a workspace
```

## Test organization tips

- Name tests for the behavior, not the implementation (`rejects_negative_age`, not `test_1`).
- One logical assertion focus per test; several `assert_eq!` calls checking the same behavior from different angles are fine, but avoid one test silently covering several unrelated behaviors.
- Prefer `Result<(), E>`-returning tests with `?` over `.unwrap()` chains when the test itself performs fallible setup — it keeps failure output attributable to the actual failing step.

## Stop conditions for this file

- Every new/changed public behavior has a test that would fail if the behavior regressed (not just a test that exercises the happy path with no meaningful assertion).
- Tests are placed at the right level (unit vs. integration vs. doc) per the table above, not all crammed into one file for convenience.
- `cargo test` passes, including doc tests, before considering the task complete.
