---
name: rust-memory-safety
description: >-
  Use when writing or fixing Rust lifetime annotations, smart pointers
  (Box/Rc/Arc/RefCell/Cell), interior mutability, reference-counted cycles,
  or unsafe Rust (raw pointers, unsafe blocks, unsafe trait impls, FFI
  memory ownership).
---

Write and fix Rust code involving lifetimes, smart pointers, interior mutability, and `unsafe` — while preserving memory safety.

## Use this skill when

- Annotating or fixing lifetime parameters (`'a`) on structs, functions, or trait impls, or resolving a "borrowed value does not live long enough" error.
- Choosing between `Box<T>`, `Rc<T>`/`Arc<T>`, `RefCell<T>`/`Cell<T>`, `Weak<T>`, and combinations like `Rc<RefCell<T>>`.
- Adding interior mutability to a type that otherwise needs to be immutable at the type level.
- Breaking a reference-counting cycle (`Rc`/`Arc` cycle) with `Weak`.
- Writing `unsafe` blocks/functions, raw pointer dereferencing, `unsafe impl Send/Sync`, or reasoning about `unsafe` invariants at an FFI boundary.

## Do not use this skill when

- The task is basic ownership/move/borrow semantics with no explicit lifetime parameter, smart pointer, or `unsafe` involved — use rust-fundamentals.
- The shared state is across OS threads (`Arc<Mutex<T>>` for thread-safety, `Send`/`Sync` bounds) — use rust-concurrency; come back here only for the underlying smart-pointer/lifetime mechanics.
- The `unsafe` code is specifically for calling into C or exposing a C ABI — use rust-interop for the FFI-specific conventions (`#[repr(C)]`, `extern "C"`), this skill for the general `unsafe` reasoning.

## Instructions

Follow these steps in order. Do the minimum needed; stop when the code compiles and the invariants are actually upheld (not merely silenced).

1. Identify the task type and open EXACTLY ONE reference file from the list below.
2. When the compiler asks for a lifetime annotation, first check whether restructuring (returning owned data, narrowing a scope) removes the need entirely before adding `'a` everywhere the compiler suggests.
3. Choose the least powerful smart pointer that solves the problem: `Box<T>` for simple heap allocation/ownership, `Rc<T>` for shared ownership within a single thread, `Arc<T>` only when sharing across threads, `RefCell<T>`/`Cell<T>` only when a `&self` method genuinely needs to mutate through a shared reference.
4. If two owners need to reference each other (parent/child, doubly-linked structures), use `Weak<T>` for the back-reference to avoid a reference-count cycle that leaks memory.
5. Avoid `unsafe` entirely unless the task specifically requires it (FFI, raw performance-critical code, implementing a lower-level abstraction). When `unsafe` is required, keep the `unsafe` block as small as possible and write a comment directly above it stating the invariant that makes it sound.
6. Never use `unsafe` to silence a borrow-checker or lifetime error you don't fully understand — that converts a compile-time error into a runtime memory-safety bug.
7. Re-read the changed code once and confirm every `unsafe` block has a documented safety invariant, and that lifetimes/smart pointers are no more complex than the data's actual ownership shape requires.
8. Stop here.

Anti-loop rules:
- ONE reference file per task.
- Do not reach for `Rc<RefCell<T>>` as a default fix for a borrow-checker error before checking whether restructuring ownership (splitting fields, passing indices, returning owned data) solves it without runtime-checked borrowing.
- Do not add `unsafe` to make a lifetime/borrow error disappear; fix the underlying ownership shape or accept the lifetime annotation instead.
- Stop as soon as the code compiles, upholds its stated invariants, and (for `unsafe` code) every safety comment is accurate.

## Reference files

- `references/ownership-lifetimes-deep-dive.md` — open for explicit lifetime parameters (`'a`), lifetime elision rules, and structs holding references.
- `references/smart-pointers-interior-mutability.md` — open for `Box`/`Rc`/`Arc`/`RefCell`/`Cell`/`Weak` and breaking reference-count cycles.
- `references/unsafe-rust.md` — open for `unsafe` blocks/functions, raw pointers, `unsafe impl`, and the invariants required to keep them sound.
