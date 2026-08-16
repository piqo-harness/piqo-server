---
name: rust-macros-metaprogramming
description: >-
  Use when writing or fixing Rust declarative macros (macro_rules!),
  procedural macros (function-like, derive, or attribute macros), or
  build.rs build scripts that generate code.
---

Write and fix Rust macros — declarative (`macro_rules!`) and procedural (derive/attribute/function-like) — and `build.rs` code generation.

## Use this skill when

- Writing or debugging a `macro_rules!` declarative macro, including its matcher patterns (`$x:expr`, repetition `$(...)*`) and hygiene.
- Writing a procedural macro crate: a `#[derive(MyTrait)]` derive macro, an attribute macro (`#[my_attribute]`), or a function-like macro (`my_macro!(...)`).
- Using `syn`/`quote`/`proc-macro2` to parse and generate token streams inside a proc-macro crate.
- Writing a `build.rs` script that generates Rust source, links native libraries, or sets `cargo:` instructions.

## Do not use this skill when

- The task just calls an existing macro from the standard library or a crate (`vec!`, `println!`, `#[derive(Debug)]`, `#[tokio::main]`) without writing new macro code — use the skill matching what the macro produces (rust-fundamentals, rust-types-generics, rust-concurrency).
- Generic code that could be done with a normal generic function/trait instead of a macro — prefer that; macros are for cases generics genuinely cannot express (varying argument counts, syntax-level transformation, deriving trait impls across arbitrary types).
- The code crosses an FFI boundary — use rust-interop for the binding generation itself (`bindgen`/`cbindgen` are tools, not something you hand-write macros for).

## Instructions

Follow these steps in order. Do the minimum needed; stop when the macro expands correctly and the generated code compiles.

1. Identify the task type and open EXACTLY ONE reference file from the list below.
2. Before writing a macro, confirm a plain function, generic function, or trait cannot solve the problem — macros add real complexity (harder to read, debug, and IDE-support) and should only be reached for when syntax-level flexibility is genuinely required.
3. For repeated boilerplate within one crate, prefer `macro_rules!` (no extra crate, no proc-macro compilation step). Reach for a procedural macro crate only when you need a custom derive, an attribute macro, or syntax `macro_rules!` cannot express.
4. When writing a proc macro, keep the proc-macro crate (`proc-macro = true` in `Cargo.toml`) limited to token-stream parsing/generation; put any complex logic in a regular library crate the proc macro depends on, so it stays testable outside the macro-expansion context.
5. Expand and inspect the macro's output (`cargo expand`, or a minimal test) before assuming it's correct — macro bugs often only surface at a specific call-site shape.
6. Re-read the generated code once (via `cargo expand` or by reasoning through the substitution) and confirm it doesn't unintentionally capture/shadow identifiers from the call site (hygiene) unless that's explicitly intended.
7. Stop here.

Anti-loop rules:
- ONE reference file per task.
- Do not reach for a macro to solve something a generic function/trait bound already solves — that's rust-types-generics territory, not this skill.
- Do not keep tweaking `macro_rules!` matcher patterns blindly against compiler errors; write out the expected expansion for the failing input first, then adjust the pattern to produce it.
- Stop as soon as the macro expands to the intended code for every call-site shape the task requires (not just the first one tried).

## Reference files

- `references/declarative-macros.md` — open for `macro_rules!` matchers, repetition, and hygiene.
- `references/procedural-derive-macros.md` — open for proc-macro crates, `syn`/`quote`, derive/attribute/function-like macros, and `build.rs` code generation.
