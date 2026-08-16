---
name: rust-types-generics
description: >-
  Use when defining or fixing Rust structs, enums, impl blocks, traits and
  trait bounds, generics with where clauses, trait objects (dyn/impl Trait),
  operator overloading, or writing closures and iterator adapter chains.
---

Design and fix Rust (edition 2024) types, traits, and generic code — structs, enums, trait bounds, and closures/iterators.

## Use this skill when

- Defining `struct`/`enum` types, `impl` blocks (inherent methods, associated functions/constants), and derive macros (`#[derive(Debug, Clone, PartialEq, ...)]`).
- Defining or implementing traits, default methods, supertraits, or associated types/constants.
- Writing generic functions/types with bounds (`T: Trait`, `where` clauses), or choosing between `impl Trait`, `dyn Trait`, and monomorphized generics.
- Overloading operators via `std::ops` traits (`Add`, `Index`, etc.) or implementing `From`/`Into`/`TryFrom`/`Default`.
- Writing closures (`Fn`/`FnMut`/`FnOnce`) or building iterator chains (`map`/`filter`/`fold`/custom `Iterator` impls).

## Do not use this skill when

- The task is basic syntax, ownership basics, or built-in collections — use rust-fundamentals.
- The trait/generic is specifically for async code (`async fn` in traits, `Send`/`Sync` bounds for concurrency) — use rust-concurrency, then return here only for the non-async trait shape.
- The question is about lifetimes on structs/references, smart pointers, or `unsafe` — use rust-memory-safety.
- Writing declarative or procedural macros — use rust-macros-metaprogramming.
- The type needs to cross an FFI boundary (`#[repr(C)]`, `extern "C"`) — use rust-interop, then come back here for the Rust-side trait design.

## Instructions

Follow these steps in order. Do the minimum needed; stop when the requested type/trait compiles and is used correctly.

1. Identify the task type and open EXACTLY ONE reference file from the list below.
2. Choose `struct` vs `enum` by shape: a fixed set of named fields is a struct; a closed set of mutually exclusive variants (optionally carrying data) is an enum. Do not simulate an enum with multiple `Option` fields on a struct.
3. Default to generics with trait bounds (`fn f<T: Trait>(x: T)`) for compile-time dispatch; use `dyn Trait` only when you need a heterogeneous collection or runtime polymorphism, and confirm the trait is object-safe first.
4. Prefer `impl Trait` in argument/return position over a bare generic parameter when there's exactly one bound and no need to name the type elsewhere.
5. Derive standard traits (`Debug`, `Clone`, `PartialEq`, `Eq`, `Hash`, `Default`) instead of hand-writing them unless custom behavior is required.
6. When writing an iterator chain, prefer a single chained expression over a manual loop with intermediate `Vec`s, unless the manual loop is clearer for the specific transform.
7. Re-read the changed code once and confirm all trait bounds are satisfied and no unnecessary `dyn`/boxing was introduced.
8. Stop here.

Anti-loop rules:
- ONE reference file per task.
- Do not add `Box<dyn Trait>` to fix a compile error before checking whether a generic bound or `impl Trait` resolves it without heap allocation.
- Do not implement a trait by hand when `#[derive(...)]` covers the same behavior.
- Stop as soon as the type/trait/generic compiles and passes its intended use site.

## Reference files

- `references/structs-enums.md` — open when defining structs, enums, `impl` blocks, or deriving standard traits.
- `references/traits-generics.md` — open when defining/implementing traits, generic bounds, `where` clauses, `impl Trait` vs `dyn Trait`, or operator/conversion traits (`From`/`Into`).
- `references/closures-iterators.md` — open when writing closures (`Fn`/`FnMut`/`FnOnce`) or iterator adapter chains, including custom `Iterator` implementations.
