# Declarative Macros (macro_rules!)

## Basic structure

A `macro_rules!` macro matches token-tree patterns and expands to code, similar to `match` but operating on syntax instead of values:

```rust
macro_rules! square {
    ($x:expr) => {
        $x * $x
    };
}

let n = square!(5); // expands to 5 * 5
```

## Fragment specifiers

Each `$name:fragment` captures a specific syntactic category:

| Specifier | Captures |
|---|---|
| `expr` | An expression |
| `ident` | An identifier or keyword |
| `ty` | A type |
| `pat` | A pattern |
| `stmt` | A statement |
| `block` | A `{ ... }` block |
| `item` | An item (fn, struct, impl, ...) |
| `literal` | A literal (`1`, `"s"`, `true`) |
| `tt` | A single token tree (most flexible, least structured) |

```rust
macro_rules! make_getter {
    ($field:ident, $ty:ty) => {
        pub fn $field(&self) -> &$ty {
            &self.$field
        }
    };
}
```

## Repetition

`$(...)*`, `$(...)+`, and `$(...)?` repeat a sub-pattern zero-or-more, one-or-more, or zero-or-one times, with an optional separator token:

```rust
macro_rules! my_vec {
    ( $( $x:expr ),* $(,)? ) => {
        {
            let mut v = Vec::new();
            $( v.push($x); )*
            v
        }
    };
}

let v = my_vec![1, 2, 3,]; // trailing comma allowed by $(,)?
```

## Multiple match arms

```rust
macro_rules! min {
    ($x:expr) => { $x };
    ($x:expr, $($rest:expr),+) => {
        {
            let rest_min = min!($($rest),+);
            if $x < rest_min { $x } else { rest_min }
        }
    };
}

let m = min!(3, 1, 4, 1, 5); // recursive expansion across the arms
```

Order arms from most specific to most general — `macro_rules!` tries arms top to bottom and uses the first that matches.

## Hygiene

`macro_rules!` macros are (mostly) hygienic: identifiers introduced inside the macro body don't accidentally capture or clash with identifiers at the call site, and vice versa, unless a fragment is passed in explicitly:

```rust
macro_rules! using_temp {
    ($x:expr) => {
        {
            let temp = $x; // this `temp` cannot collide with a caller's `temp`
            temp * 2
        }
    };
}

let temp = 5;
let result = using_temp!(temp); // works fine; the macro's `temp` is a distinct binding
```

Hygiene does not extend across separate macro invocations or into items the macro generates at module scope in every edge case — if a macro must define something the caller needs to refer to by a fixed name, pass the name in explicitly as an `ident` fragment rather than relying on the macro to "leak" one.

## Exporting macros

```rust
#[macro_export] // makes the macro usable outside this crate at the crate root
macro_rules! my_macro {
    () => { println!("hello from my_macro!"); };
}
```

## When `macro_rules!` isn't enough

Reach for a procedural macro (see `references/procedural-derive-macros.md`) when you need: a custom `#[derive(...)]`, an attribute macro that inspects/rewrites an existing item, or logic too complex to express as token-pattern matching (e.g., parsing a mini-language, walking a full AST).

## Stop conditions for this file

- The macro's expansion was checked (via `cargo expand` or manual substitution) for every call-site shape the task requires, not just the first example.
- Match arms are ordered most-specific-first, and repetition separators/trailing-comma handling match how the macro is actually called.
- No identifier is assumed to "leak" from the macro into the caller's scope without being passed in explicitly.
