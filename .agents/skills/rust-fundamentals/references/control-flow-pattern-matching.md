# Control Flow and Pattern Matching

## `if`, `loop`, `while`, `for`

```rust
let n = 7;
if n % 2 == 0 {
    println!("even");
} else {
    println!("odd");
}

let mut count = 0;
let result = loop {
    count += 1;
    if count == 10 {
        break count * 2; // loop can return a value via break
    }
};

while count < 20 {
    count += 1;
}

for x in 0..5 {          // exclusive range
    print!("{x} ");
}
for x in 0..=5 {         // inclusive range
    print!("{x} ");
}
for item in &["a", "b", "c"] {
    print!("{item} ");
}
```

Label loops to break/continue an outer loop from a nested one:

```rust
'outer: for x in 0..5 {
    for y in 0..5 {
        if x + y == 6 {
            break 'outer;
        }
    }
}
```

## `match`

`match` must be exhaustive — cover every variant or add a `_` catch-all.

```rust
enum Direction { North, South, East, West }

fn describe(d: Direction) -> &'static str {
    match d {
        Direction::North => "up",
        Direction::South => "down",
        Direction::East | Direction::West => "sideways",
    }
}

fn classify(n: i32) -> &'static str {
    match n {
        n if n < 0 => "negative",
        0 => "zero",
        1..=9 => "single digit",
        _ => "large",
    }
}
```

Bind part of a matched value with `@`:

```rust
match 5 {
    n @ 1..=5 => println!("got {n} in range"),
    _ => println!("out of range"),
}
```

## `if let` / `while let`

Use `if let` when you only care about one matching pattern (no need for full `match` exhaustiveness):

```rust
let maybe_value: Option<i32> = Some(3);
if let Some(v) = maybe_value {
    println!("got {v}");
} else {
    println!("nothing");
}

let mut stack = vec![1, 2, 3];
while let Some(top) = stack.pop() {
    println!("{top}");
}
```

Edition 2024 lets you chain conditions with `let` inside `if`/`while` (let-chains remain unstable as a *general* boolean-mixed feature outside match ergonomics in older editions — check the edition guide for the current stabilization status before relying on `if let ... && ...`).

## Destructuring

```rust
struct Point { x: i32, y: i32 }

let p = Point { x: 1, y: 2 };
let Point { x, y } = p;

let (a, b, c) = (1, 2, 3);

let [first, .., last] = [1, 2, 3, 4, 5];

// Ignoring parts of a value
let (_, second) = (1, 2);
```

Destructure enum variants directly in `match` arms, including nested and struct-like variants:

```rust
enum Shape {
    Circle { radius: f64 },
    Rectangle { width: f64, height: f64 },
}

fn area(s: &Shape) -> f64 {
    match s {
        Shape::Circle { radius } => std::f64::consts::PI * radius * radius,
        Shape::Rectangle { width, height } => width * height,
    }
}
```

## Stop conditions for this file

- Every `match` is exhaustive (compiles with no "non-exhaustive patterns" error) or has an explicit `_` arm.
- `if let`/`while let` is used instead of a full `match` only when a single pattern is relevant.
- Loops that need to return a value use `break value` rather than an external mutable variable, where that reads more clearly.
