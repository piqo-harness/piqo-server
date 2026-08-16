---
name: rust-interop
description: >-
  Use when writing Rust FFI to/from C (extern "C", #[repr(C)], bindgen/
  cbindgen), compiling Rust to WebAssembly with wasm-bindgen, or binding
  Rust to another language (e.g. Python via PyO3).
---

Write and fix Rust code that crosses a language boundary — C FFI, WebAssembly, or bindings to another language runtime.

## Use this skill when

- Declaring `extern "C"` functions/blocks, `#[repr(C)]` types, or calling into/being called from a C library.
- Generating bindings with `bindgen` (C headers → Rust) or `cbindgen` (Rust → a C header for consumers).
- Compiling Rust to WebAssembly and exposing/consuming JS values with `wasm-bindgen`.
- Writing Python bindings for a Rust crate with `PyO3` (or a similar binding crate for another host language).
- Managing ownership/lifetime of memory that crosses the FFI boundary (who allocates, who frees).

## Do not use this skill when

- The `unsafe` code has nothing to do with an external language/ABI boundary — use rust-memory-safety for general `unsafe` reasoning.
- The concurrency primitives happen to be used near an FFI call but the question is about the concurrency itself — use rust-concurrency, then return here for the boundary-crossing specifics.
- The macro question is about writing a `macro_rules!`/proc-macro from scratch, not about using `bindgen`/`cbindgen`/`wasm-bindgen`'s generated code — use rust-macros-metaprogramming.

## Instructions

Follow these steps in order. Do the minimum needed; stop when the boundary compiles, links, and round-trips a value correctly.

1. Identify the task type and open EXACTLY ONE reference file from the list below.
2. Decide the target boundary first: C ABI → `extern "C"`/`#[repr(C)]` (+ `bindgen`/`cbindgen`); browser/JS/WASM → `wasm-bindgen`; another language runtime (Python, etc.) → the language-specific binding crate (`PyO3` for Python). Each has different ownership and marshaling conventions — don't mix C-FFI patterns into a `wasm-bindgen` crate or vice versa.
3. For every type crossing the boundary, use `#[repr(C)]` (or the binding crate's equivalent, e.g. `#[wasm_bindgen]`) — never rely on Rust's default (unspecified) layout for cross-language structs.
4. Decide, and document, exactly who owns and frees any heap memory that crosses the boundary (Rust-allocated-Rust-freed via an explicit `free_*` function, vs. caller-allocated-caller-freed) — this is the single most common source of FFI memory bugs.
5. Prefer generating bindings (`bindgen`, `cbindgen`, `wasm-bindgen`'s macro expansion) over hand-writing `extern "C"` declarations for a large existing API surface; hand-write only small, stable boundaries.
6. Wrap every raw FFI call in a safe Rust function that validates inputs (non-null, valid UTF-8 for strings, expected length) before crossing back into `unsafe`, following the same minimal-`unsafe`-surface principle as rust-memory-safety.
7. Re-read the boundary code once and confirm every allocation has exactly one clearly-owned deallocation path, and that panics cannot unwind across the FFI boundary (wrap `extern "C" fn` bodies in `std::panic::catch_unwind` if the called Rust code might panic).
8. Stop here.

Anti-loop rules:
- ONE reference file per task.
- Do not guess a struct's C layout; add `#[repr(C)]` (or the binding crate's macro) rather than relying on default Rust layout, and don't iterate blindly on segfaults — check the layout/ownership assumption first.
- Do not let a panic unwind across an `extern "C"` boundary (undefined behavior) — catch it explicitly instead of adding ad hoc `catch_unwind` only after a crash is observed.
- Stop as soon as a round-trip call across the boundary (call out, or be called into, and get the expected value back) works correctly.

## Reference files

- `references/c-ffi-interop.md` — open for `extern "C"`, `#[repr(C)]`, `bindgen`/`cbindgen`, and memory-ownership conventions at a C boundary.
- `references/wasm-bindgen-wasm.md` — open for compiling to WebAssembly and exposing/consuming values with `wasm-bindgen`.
- `references/python-other-language-bindings.md` — open for Python bindings with `PyO3` and the general shape of binding Rust into another managed-language runtime.
