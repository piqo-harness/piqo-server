# Closures and Iterators

## Closures and the `Fn`/`FnMut`/`FnOnce` traits

- `Fn` — can be called multiple times, borrows captured variables immutably.
- `FnMut` — can be called multiple times, borrows captured variables mutably.
- `FnOnce` — can be called at most once, may consume/move captured variables.

Every closure implements at least `FnOnce`; whether it also implements `FnMut`/`Fn` depends on what it does with its captures. Use `move` to force capture by value (ownership) instead of by reference — required when the closure outlives the current scope (e.g., returned from a function, spawned onto a thread).

```rust
fn apply<F: Fn(i32) -> i32>(f: F, x: i32) -> i32 {
    f(x)
}

let double = |x| x * 2;
assert_eq!(apply(double, 5), 10);

fn make_counter() -> impl FnMut() -> i32 {
    let mut count = 0;
    move || {
        count += 1;
        count
    }
}

let mut counter = make_counter();
assert_eq!(counter(), 1);
assert_eq!(counter(), 2);
```

Accept the least restrictive trait your function actually needs (`FnOnce` > `FnMut` > `Fn` in generality) so callers have maximum flexibility.

## Iterator adapters

Iterators are lazy — nothing runs until a consuming call (`.collect()`, `.sum()`, `for`, etc.):

```rust
let v = vec![1, 2, 3, 4, 5];

let squares_of_evens: Vec<i32> = v.iter()
    .filter(|&&x| x % 2 == 0)
    .map(|&x| x * x)
    .collect();

let total: i32 = v.iter().sum();
let max = v.iter().max();
let first_over_3 = v.iter().find(|&&x| x > 3);

let (evens, odds): (Vec<i32>, Vec<i32>) = v.into_iter().partition(|x| x % 2 == 0);
```

`fold` for a custom accumulation:

```rust
let product = v.iter().fold(1, |acc, x| acc * x);
```

`enumerate`, `zip`, `chain`:

```rust
for (i, x) in v.iter().enumerate() {
    println!("{i}: {x}");
}

let a = vec![1, 2, 3];
let b = vec!["a", "b", "c"];
let zipped: Vec<(i32, &str)> = a.into_iter().zip(b).collect();
```

Prefer a single chained iterator expression over collecting into intermediate `Vec`s between each step — it avoids extra allocations and usually reads more clearly for straightforward transforms. Fall back to an explicit `for` loop when the chain would need multiple `.collect()` round-trips or awkward tuple bookkeeping to stay readable.

## `iter()` vs `into_iter()` vs `iter_mut()`

- `.iter()` — yields `&T` (borrowed).
- `.iter_mut()` — yields `&mut T` (mutably borrowed).
- `.into_iter()` — yields `T` (owned); consumes the collection.

```rust
let v = vec![1, 2, 3];
for x in &v { /* &i32 */ }
for x in v.iter() { /* &i32, same as above */ }
for x in v.into_iter() { /* i32, owned; v is consumed */ }
```

## Implementing a custom `Iterator`

```rust
struct Fibonacci { a: u64, b: u64 }

impl Iterator for Fibonacci {
    type Item = u64;
    fn next(&mut self) -> Option<u64> {
        let next = self.a;
        self.a = self.b;
        self.b = next + self.b;
        Some(next)
    }
}

let fibs: Vec<u64> = Fibonacci { a: 0, b: 1 }.take(10).collect();
```

Implementing `Iterator` automatically gives access to every adapter method (`map`, `filter`, `take`, `zip`, ...) via the blanket `IntoIterator`/`Iterator` provided methods — do not hand-roll `map`/`filter` on a custom type when implementing `Iterator` covers it for free.

## Stop conditions for this file

- Closures use the least restrictive `Fn*` bound the call site actually requires.
- `move` is added only when the closure must outlive the current stack frame (returned, spawned, stored).
- Iterator chains avoid unnecessary intermediate `.collect()` calls between adapter steps.
