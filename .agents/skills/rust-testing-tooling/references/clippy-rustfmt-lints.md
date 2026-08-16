# Clippy, rustfmt, and Benchmarking

## Running Clippy

```bash
cargo clippy                          # default lint pass
cargo clippy --all-targets --all-features   # include tests, examples, benches, every feature combo
cargo clippy --fix                    # auto-apply machine-applicable suggestions
```

Clippy lints are grouped by category; the defaults (`clippy::correctness`, `clippy::style`, `clippy::complexity`, `clippy::perf`) are enabled automatically. Opt into stricter groups deliberately, not by default, since they produce more false positives:

```bash
cargo clippy -- -W clippy::pedantic -W clippy::nursery
```

## Fixing vs. allowing a lint

Fix the pattern Clippy flags rather than suppressing it, unless the lint is a genuine false positive for the specific case:

```rust
// Clippy: clippy::needless_return
fn bad() -> i32 {
    return 5; // flagged: unnecessary `return` on the last expression
}
fn good() -> i32 {
    5
}
```

When a lint truly doesn't apply, scope the `#[allow(...)]` as narrowly as possible (the specific item, not the whole module/crate) and add a comment explaining why:

```rust
#[allow(clippy::too_many_arguments)] // this FFI shim mirrors a fixed C function signature
extern "C" fn c_callback(a: i32, b: i32, c: i32, d: i32, e: i32, f: i32, g: i32, h: i32) {}
```

## Common high-value Clippy lints

| Lint | What it catches |
|---|---|
| `clippy::clone_on_copy` | `.clone()` on a `Copy` type — unnecessary. |
| `clippy::needless_collect` | Collecting into a `Vec` immediately consumed by another iterator adapter. |
| `clippy::redundant_clone` | A clone whose result is never mutated independently from the original. |
| `clippy::unwrap_used` (restriction group) | Any `.unwrap()` call — useful to enable in application code that should handle all errors explicitly. |
| `clippy::mutex_atomic` | A `Mutex` guarding a value that could be a lock-free atomic instead. |

## rustfmt

```bash
cargo fmt              # reformat the whole crate/workspace
cargo fmt --check       # exit non-zero if formatting would change anything (for CI)
```

Configure project-wide style in `rustfmt.toml` at the crate/workspace root — most defaults should stay untouched; only override settings the team has explicitly agreed on:

```toml
# rustfmt.toml
max_width = 100
imports_granularity = "Module"
```

Run `cargo fmt` before every commit rather than manually matching rustfmt's style by hand — it's deterministic and avoids unrelated formatting churn in diffs.

## Benchmarking with `criterion`

Manual `Instant::now()` timing loops are noisy (JIT/cache warm-up, dead-code elimination removing the "unused" computed value) — use `criterion` for statistically meaningful benchmarks:

```toml
# Cargo.toml
[dev-dependencies]
criterion = { version = "0.5", features = ["html_reports"] }

[[bench]]
name = "my_benchmark"
harness = false
```

```rust
// benches/my_benchmark.rs
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn fib(n: u64) -> u64 {
    if n < 2 { n } else { fib(n - 1) + fib(n - 2) }
}

fn bench_fib(c: &mut Criterion) {
    c.bench_function("fib 20", |b| b.iter(|| fib(black_box(20))));
}

criterion_group!(benches, bench_fib);
criterion_main!(benches);
```

```bash
cargo bench
```

`black_box` prevents the compiler from optimizing away a computation whose result is never observably used — always wrap the benchmarked input (and, where relevant, output) in it.

## Stop conditions for this file

- `cargo clippy --all-targets --all-features` reports no unaddressed warnings, and every remaining `#[allow(...)]` has a comment justifying it.
- `cargo fmt --check` passes (no formatting diff).
- Any performance claim about the change is backed by a `criterion` benchmark, not an ad hoc timing print.
