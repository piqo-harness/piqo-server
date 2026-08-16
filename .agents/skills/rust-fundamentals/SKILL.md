---
name: rust-fundamentals
description: >-
  Use when writing basic Rust syntax, variable bindings and mutability,
  ownership/borrowing at the introductory level, control flow, match/pattern
  matching, or working with core collections (Vec, HashMap, HashSet),
  strings, slices, and tuples.
---

Write correct, idiomatic Rust (edition 2024) for core syntax, control flow, and standard collections.

## Use this skill when

- Declaring variables/constants (`let`, `let mut`, `const`), understanding shadowing, or basic type inference.
- Writing a first-pass ownership/borrowing model: move semantics, `&`/`&mut` references, the borrow checker's basic rules.
- Writing `if`/`if let`/`while let`/`loop`/`for` control flow or `match` with pattern matching (guards, bindings, `|`, ranges, `@`).
- Using `Vec<T>`, `HashMap<K, V>`, `HashSet<T>`, slices (`&[T]`), and iterating over them.
- Working with `String` vs `&str`, string slicing, UTF-8 concerns, and formatting (`format!`, `println!`).
- Using tuples, arrays, and basic destructuring.

## Do not use this skill when

- The question is really about lifetimes, `Rc`/`RefCell`, smart pointers, or `unsafe` — use rust-memory-safety.
- Defining your own structs/enums/traits/generics — use rust-types-generics.
- Handling errors with `Result`/`Option`/`?` beyond the basic `Option` pattern match — use rust-error-handling.
- Writing threads, async/await, or channels — use rust-concurrency.
- Writing macros — use rust-macros-metaprogramming.

## Instructions

Follow these steps in order. Do the minimum needed; stop when the requested code compiles and behaves correctly.

1. Identify the task type and open EXACTLY ONE reference file from the list below.
2. Default to immutable bindings (`let`) and add `mut` only where reassignment or in-place mutation is required.
3. When passing data to a function, prefer borrowing (`&T`/`&mut T`) over moving/cloning unless ownership must transfer; only reach for `.clone()` after confirming a borrow does not satisfy the borrow checker.
4. Prefer `match` or `if let` over chains of `if`/`else` when branching on an enum or `Option`/`Result` shape.
5. Choose the narrowest collection/string type that fits: `&str` for a borrowed view, `String` only when ownership or growth is needed; `&[T]` over `Vec<T>` in function parameters unless the callee needs to own or resize the data.
6. Re-read the changed code once and confirm it compiles with `cargo check` semantics in mind (no missing `mut`, no moved-value reuse).
7. Stop here.

Anti-loop rules:
- ONE reference file per task.
- Do not add `.clone()` repeatedly to "fix" borrow-checker errors without first checking whether restructuring the borrow (shorter scope, splitting a struct, reordering) removes the conflict.
- Do not rewrite working code to avoid the borrow checker with `unsafe`; that belongs to rust-memory-safety and is out of scope for basic fundamentals code.
- Stop as soon as the requested behavior is correct.

## Reference files

- `references/ownership-borrowing-basics.md` — open for move semantics, `&`/`&mut` references, and the introductory borrow-checker rules.
- `references/control-flow-pattern-matching.md` — open for `if`/`loop`/`for`, `match`, `if let`/`while let`, and destructuring.
- `references/collections-strings.md` — open for `Vec`, `HashMap`, `HashSet`, slices, `String`/`&str`, and formatting.
