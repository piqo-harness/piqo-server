# Traits and Generics

## Defining and implementing a trait

```rust
trait Summary {
    fn author(&self) -> String;

    // Default method — implementors may override it
    fn summarize(&self) -> String {
        format!("(Read more from {}...)", self.author())
    }
}

struct Article { author: String, title: String }

impl Summary for Article {
    fn author(&self) -> String {
        self.author.clone()
    }
    // uses the default summarize()
}
```

## Generic functions and bounds

```rust
fn largest<T: PartialOrd + Copy>(items: &[T]) -> T {
    let mut largest = items[0];
    for &item in items {
        if item > largest {
            largest = item;
        }
    }
    largest
}
```

Use a `where` clause when bounds get long or involve multiple type parameters:

```rust
fn process<T, U>(a: T, b: U) -> String
where
    T: std::fmt::Debug,
    U: std::fmt::Display + Clone,
{
    format!("{a:?} and {b}")
}
```

## `impl Trait` vs generics vs `dyn Trait`

- `fn f<T: Trait>(x: T)` — generic, monomorphized at compile time (one specialized copy per concrete type used). Fastest; increases binary size.
- `fn f(x: impl Trait)` — sugar for the generic form above; use for a single, unnamed bound in argument or return position.
- `fn f() -> impl Trait` — return an opaque concrete type without naming it (common for returning closures or iterator chains).
- `fn f(x: &dyn Trait)` / `Box<dyn Trait>` — dynamic dispatch via a vtable; needed for heterogeneous collections (`Vec<Box<dyn Trait>>`) or when the concrete type can't be known at compile time. Requires the trait to be **object-safe** (no generic methods, no `Self` returned by value in a method other than via `Box<Self>`, etc.).

```rust
fn make_adder(x: i32) -> impl Fn(i32) -> i32 {
    move |y| x + y
}

let shapes: Vec<Box<dyn Summary>> = vec![Box::new(article)];
```

Default to generics/`impl Trait`; reach for `dyn Trait` only when you specifically need runtime polymorphism or a mixed-type collection.

## Associated types vs generic parameters

Use an associated type when a trait has exactly one "output" type per implementor (e.g., `Iterator::Item`); use a generic parameter when a type can implement the trait multiple times for different type arguments (e.g., `From<T>`).

```rust
trait Container {
    type Item;
    fn get(&self, i: usize) -> Option<&Self::Item>;
}
```

## Conversion traits: `From`/`Into`/`TryFrom`

Implement `From` (not `Into` directly — `Into` is auto-derived from `From`) for infallible conversions, and `TryFrom` for conversions that can fail:

```rust
struct Celsius(f64);
struct Fahrenheit(f64);

impl From<Celsius> for Fahrenheit {
    fn from(c: Celsius) -> Self {
        Fahrenheit(c.0 * 9.0 / 5.0 + 32.0)
    }
}

let f: Fahrenheit = Celsius(100.0).into();

impl TryFrom<i64> for Celsius {
    type Error = &'static str;
    fn try_from(v: i64) -> Result<Self, Self::Error> {
        if v < -273 { Err("below absolute zero") } else { Ok(Celsius(v as f64)) }
    }
}
```

## Operator overloading

```rust
use std::ops::Add;

#[derive(Clone, Copy)]
struct Vec2 { x: f64, y: f64 }

impl Add for Vec2 {
    type Output = Vec2;
    fn add(self, other: Vec2) -> Vec2 {
        Vec2 { x: self.x + other.x, y: self.y + other.y }
    }
}
```

## `async fn` in traits (edition 2024, non-object-safe)

Native `async fn` in traits works for static dispatch (generics/`impl Trait`) as of Rust 1.85+, but the resulting trait is not object-safe — `dyn Trait` with an async method does not compile. Use the `async-trait` crate only when you specifically need `Box<dyn Trait>` with async methods; see rust-concurrency for the async-specific details.

## Stop conditions for this file

- The chosen dispatch mechanism (generic/`impl Trait` vs `dyn Trait`) matches the actual need (compile-time vs. runtime polymorphism).
- Any `dyn Trait` usage was checked for object-safety before committing to it.
- `From`/`TryFrom` are implemented instead of ad hoc `to_x()`/`from_x()` free functions where a conversion trait fits.
