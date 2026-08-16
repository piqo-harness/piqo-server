# Ownership and Lifetimes in Depth

## Why lifetimes exist

A reference (`&T`/`&mut T`) must never outlive the data it points to. Lifetime parameters (`'a`) let the compiler verify this across function boundaries, where it can't always infer the relationship on its own.

```rust
// Without an explicit lifetime, the compiler cannot tell which input
// the returned reference borrows from.
fn longest<'a>(x: &'a str, y: &'a str) -> &'a str {
    if x.len() > y.len() { x } else { y }
}
```

`'a` here means: "the returned reference is valid for at least as long as both `x` and `y` are valid." It does not change how long anything actually lives — it only describes a constraint the compiler checks.

## Lifetime elision rules

The compiler infers lifetimes automatically in common patterns, so explicit annotations are needed less often than beginners expect:

1. Each elided input reference gets its own lifetime parameter.
2. If there's exactly one input lifetime, it's assigned to all elided output lifetimes.
3. If one parameter is `&self`/`&mut self`, its lifetime is assigned to all elided output lifetimes.

```rust
fn first_word(s: &str) -> &str { // desugars to fn first_word<'a>(s: &'a str) -> &'a str
    s.split_whitespace().next().unwrap_or("")
}
```

Only write an explicit lifetime when the elision rules don't produce the relationship you need (e.g., a function taking two references where only one relates to the output).

## Lifetimes on structs

A struct holding a reference must declare the lifetime, and no instance can outlive the data it borrows:

```rust
struct Excerpt<'a> {
    part: &'a str,
}

impl<'a> Excerpt<'a> {
    fn announce(&self, msg: &str) -> &str {
        println!("Attention: {msg}");
        self.part
    }
}

fn main() {
    let novel = String::from("Call me Ishmael. Some years ago...");
    let first_sentence = novel.split('.').next().unwrap();
    let excerpt = Excerpt { part: first_sentence };
    println!("{}", excerpt.part);
}
```

Prefer an owned field (`String` instead of `&str`, `Vec<T>` instead of `&[T]`) when the struct's data doesn't need to strictly borrow from somewhere else — it avoids threading lifetime parameters through every type that uses the struct. Reach for a borrowed field only when avoiding the clone genuinely matters (hot path, large data, or the struct is deliberately a short-lived "view").

## `'static`

`'static` means the reference is valid for the entire program (string literals, or data explicitly leaked/promoted). It is not a magic fix for a lifetime error — a `'static` bound often means "this data must be owned" rather than "just add `'static`."

```rust
let s: &'static str = "a string literal, valid for the whole program";
```

Avoid reaching for `'static` bounds on generic parameters just to make a compile error go away unless the value genuinely needs to outlive any specific caller's scope (e.g., data moved into a spawned thread or stored in a long-lived registry).

## Common lifetime-error fixes, in order of preference

1. Return owned data (`String`/`Vec<T>`/`.to_owned()`/`.clone()`) instead of a reference, when the caller doesn't specifically need zero-copy access.
2. Narrow the scope of the borrow so it doesn't overlap the conflicting use (often via non-lexical lifetimes, or restructuring statement order).
3. Add the explicit lifetime parameter the compiler is asking for, only after confirming options 1–2 don't fit the actual requirement.
4. As a last resort for genuinely self-referential needs, consider whether the data structure should instead use indices/handles into a shared arena rather than direct references (self-referential structs are not directly expressible safely in Rust without a crate like `ouroboros` or unsafe code — prefer redesigning the data model first).

## Stop conditions for this file

- The code compiles without lifetime errors, and every explicit `'a` reflects a real "this reference must not outlive that data" relationship.
- Owned fields were preferred over borrowed ones unless the borrow was specifically required.
- No `'static` bound was added purely to silence an error without confirming the value truly lives for the whole program.
