# Structs, Enums, and impl Blocks

## Structs

```rust
#[derive(Debug, Clone, PartialEq)]
struct User {
    name: String,
    age: u32,
}

impl User {
    // Associated function (no `self`) — a "constructor"
    fn new(name: impl Into<String>, age: u32) -> Self {
        Self { name: name.into(), age }
    }

    // Method borrowing self
    fn is_adult(&self) -> bool {
        self.age >= 18
    }

    // Method mutably borrowing self
    fn birthday(&mut self) {
        self.age += 1;
    }
}

let mut u = User::new("Ada", 17);
u.birthday();
assert!(u.is_adult());
```

Tuple structs and unit structs:

```rust
struct Point(f64, f64);      // tuple struct
struct Marker;                // unit struct, useful as a zero-sized type tag

let p = Point(1.0, 2.0);
println!("{}", p.0);
```

Use `#[derive(Default)]` for a struct whose fields all implement `Default`, and `..Default::default()` for partial construction:

```rust
#[derive(Debug, Default)]
struct Config {
    verbose: bool,
    retries: u32,
}

let cfg = Config { verbose: true, ..Default::default() };
```

## Enums

```rust
#[derive(Debug, Clone, PartialEq)]
enum Shape {
    Circle { radius: f64 },
    Rectangle { width: f64, height: f64 },
    Triangle(f64, f64, f64), // tuple-style variant
}

impl Shape {
    fn area(&self) -> f64 {
        match self {
            Shape::Circle { radius } => std::f64::consts::PI * radius * radius,
            Shape::Rectangle { width, height } => width * height,
            Shape::Triangle(a, b, c) => {
                let s = (a + b + c) / 2.0;
                (s * (s - a) * (s - b) * (s - c)).sqrt()
            }
        }
    }
}
```

Use an enum instead of several `Option<T>`/boolean fields whenever the fields are mutually exclusive — it makes invalid states unrepresentable and forces exhaustive handling everywhere the value is used.

## Deriving common traits

| Derive | When to use |
|---|---|
| `Debug` | Almost always — enables `{:?}` formatting and easier debugging/tests. |
| `Clone` | The type should support explicit duplication (`.clone()`). |
| `Copy` | Only for small, stack-only data (requires `Clone` too); do not derive on types owning heap data (`String`, `Vec`, etc.). |
| `PartialEq`/`Eq` | The type needs `==`/`!=`; `Eq` requires every field to be `Eq` (no floats). |
| `Hash` | The type will be used as a `HashMap`/`HashSet` key; requires consistent `PartialEq`. |
| `Default` | The type has a sensible "empty"/zero value. |
| `PartialOrd`/`Ord` | The type needs `<`/`>`/`sort()`. |

Only hand-write a trait impl instead of deriving when the derived behavior would be wrong (e.g., comparing only a subset of fields for `PartialEq`, or ordering by a computed key).

## Newtype pattern

Wrap a primitive/foreign type to add meaning and prevent mixing up values of the same underlying type:

```rust
struct UserId(u64);
struct ProductId(u64);

fn find_user(id: UserId) { /* ... */ }
// find_user(ProductId(1)) // compile error — different types, even though both wrap u64
```

## Stop conditions for this file

- The struct/enum shape makes invalid states unrepresentable (no parallel `Option` fields that should be one enum).
- Standard traits are derived, not hand-written, unless custom semantics are required.
- `impl` methods borrow (`&self`/`&mut self`) rather than consuming (`self`) unless the method is meant to transform ownership (e.g., a builder's terminal `build(self)`).
