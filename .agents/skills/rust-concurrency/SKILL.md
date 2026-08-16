---
name: rust-concurrency
description: >-
  Use when writing or fixing Rust threads, Mutex/Arc/RwLock shared state,
  mpsc channels, async/await code, Future implementations, async runtimes
  (Tokio), async closures, or resolving Send/Sync compiler errors.
---

Write and fix Rust concurrent and async code — threads, shared-state synchronization, channels, and async/await — that compiles cleanly with correct `Send`/`Sync` bounds.

## Use this skill when

- Spawning OS threads with `std::thread::spawn` and joining them, or sharing state with `Arc<Mutex<T>>`/`Arc<RwLock<T>>`.
- Sending data between threads or tasks with `std::sync::mpsc` or an async channel (e.g., `tokio::sync::mpsc`).
- Writing `async fn`, `.await`, `async` blocks, or async closures (`AsyncFn`, stable since Rust 1.85).
- Implementing a custom `Future`, or choosing between structured concurrency helpers (`tokio::join!`, `futures::join_all`) and spawned tasks.
- Fixing a `Send`/`Sync` compiler error, a deadlock, or a data race in threaded or async code.
- Deciding between blocking (`std::thread`) and async (`tokio`/async runtime) concurrency for a given workload.

## Do not use this skill when

- The code has no threads, channels, or `async`/`.await` at all — use rust-fundamentals or rust-types-generics.
- The question is about ownership/borrowing/lifetimes with no concurrency involved — use rust-memory-safety.
- Handling a `Result`/`Option` returned from async code, unrelated to the concurrency itself — use rust-error-handling for the error-handling shape, come back here for the async/threading structure.
- Defining a trait that merely happens to have generic bounds, with no `async fn` involved — use rust-types-generics.

## Instructions

Follow these steps in order. Do the minimum needed; stop when the requested concurrent/async behavior is correct and compiles.

1. Identify the task type and open EXACTLY ONE reference file from the list below.
2. Decide blocking vs. async first: CPU-bound work or a small, fixed number of OS-level parallel tasks → `std::thread`; I/O-bound work, many concurrent operations, or an existing async codebase → `async`/await with a runtime (Tokio is the de facto default; note it in `Cargo.toml` if not already a dependency).
3. For shared mutable state across threads, wrap it in `Arc<Mutex<T>>` (or `Arc<RwLock<T>>` for many-readers/few-writers); never share a raw reference across a `thread::spawn` boundary — the compiler will reject non-`'static`, non-`Send` captures.
4. Prefer message passing (channels) over shared-state locking when tasks can be decoupled into producer/consumer stages — it avoids lock contention and most deadlock shapes entirely.
5. When the compiler reports a `Send`/`Sync` error, find the actual non-`Send`/non-`Sync` type being captured or held across an `.await` point (e.g., a `Rc`, a `RefCell` guard, a non-thread-safe client) and replace it with a thread-safe equivalent (`Arc`, `Mutex`, a `Send` client) — do not suppress the error.
6. Hold a `Mutex`/`RwLock` guard for the shortest scope possible; never hold one across an `.await` point (it can deadlock the runtime) — drop it before awaiting, or restructure to clone the needed data out first.
7. Re-read the changed code once and confirm no lock is held across `.await`, and that spawned threads/tasks are joined/awaited (not silently detached) unless fire-and-forget is intentional.
8. Stop here.

Anti-loop rules:
- ONE reference file per task.
- Do not wrap a value in `unsafe impl Send`/`unsafe impl Sync` to silence a compiler error without first trying the structural fix (thread-safe wrapper type, message passing, cloning data out).
- Do not hold a `Mutex` guard across an `.await` — restructure instead of adding a retry/timeout loop around a deadlock.
- Stop as soon as the requested concurrent/async feature compiles and runs without deadlocking.

## Reference files

- `references/threads-sync-primitives.md` — open for `std::thread`, `Arc`/`Mutex`/`RwLock`, and `mpsc` channels.
- `references/async-await-futures.md` — open for `async fn`/`.await`, async closures, custom `Future` implementations, and `Send`/`Sync` bounds on async code.
- `references/channels-tokio-async-runtime.md` — open for Tokio-specific APIs: `#[tokio::main]`, `tokio::spawn`, `tokio::sync::{mpsc, Mutex}`, `join!`/`select!`, and structured task cancellation.
