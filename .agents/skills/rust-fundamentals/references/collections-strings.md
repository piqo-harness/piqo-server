# Collections and Strings

## `Vec<T>`

```rust
let mut v: Vec<i32> = Vec::new();
v.push(1);
v.push(2);
let v2 = vec![1, 2, 3]; // macro form

for x in &v2 {          // borrow each element
    print!("{x} ");
}
for x in &mut v {        // mutate in place
    *x += 1;
}

let doubled: Vec<i32> = v2.iter().map(|x| x * 2).collect();
let evens: Vec<&i32> = v2.iter().filter(|x| **x % 2 == 0).collect();

// Indexing panics out of bounds; use .get() for a checked Option<&T>
let maybe_first = v2.get(0); // Option<&i32>
```

Prefer `&[T]` (a slice) as a function parameter over `&Vec<T>` — it also accepts arrays and other slices:

```rust
fn sum(values: &[i32]) -> i32 {
    values.iter().sum()
}
```

## `HashMap<K, V>`

```rust
use std::collections::HashMap;

let mut scores: HashMap<String, i32> = HashMap::new();
scores.insert(String::from("blue"), 10);
scores.entry(String::from("blue")).or_insert(0); // no-op, key already exists
*scores.entry(String::from("red")).or_insert(0) += 1; // insert 0 then increment

match scores.get("blue") {
    Some(v) => println!("blue = {v}"),
    None => println!("no entry"),
}

for (key, value) in &scores {
    println!("{key}: {value}");
}
```

Keys must implement `Eq` + `Hash`. Use `HashSet<T>` for a set with the same constraints when you only need membership, not a mapping.

```rust
use std::collections::HashSet;

let mut seen: HashSet<i32> = HashSet::new();
seen.insert(1);
if !seen.insert(1) {
    println!("1 was already present");
}
```

Use `BTreeMap`/`BTreeSet` instead of the `HashMap`/`HashSet` family when iteration order must be sorted by key.

## `String` vs `&str`

- `&str` is a borrowed, immutable view into UTF-8 text (a string slice) — use it for function parameters and anywhere you don't need ownership.
- `String` is an owned, growable UTF-8 buffer — use it when you need to build, mutate, or own the text.

```rust
fn greet(name: &str) -> String {
    format!("Hello, {name}!")
}

fn main() {
    let owned = String::from("world");
    println!("{}", greet(&owned)); // &String derefs to &str
    println!("{}", greet("literal")); // string literals are already &str
}
```

Because Rust strings are UTF-8, do not index a `String`/`&str` by byte position directly (`s[0]` does not compile); use `.chars()`, `.bytes()`, or `.get(range)` which returns `Option<&str>` and validates char boundaries:

```rust
let s = "héllo";
for c in s.chars() {
    print!("{c}");
}
let slice: Option<&str> = s.get(0..1); // safe, checked
```

## Formatting

```rust
let name = "Ada";
let age = 36;
println!("{name} is {age}");           // captured identifiers (edition 2021+)
println!("{} is {}", name, age);       // positional
let msg = format!("{name} is {age}");  // build a String instead of printing
eprintln!("error: {msg}");             // stderr
```

## Tuples and arrays

```rust
let pair: (i32, &str) = (1, "one");
println!("{} {}", pair.0, pair.1);

let arr: [i32; 3] = [1, 2, 3];
let zeros = [0; 5]; // [0, 0, 0, 0, 0]
```

## Stop conditions for this file

- The chosen collection type matches the actual access pattern (ordered vs. unordered, unique vs. duplicate keys allowed).
- Function signatures borrow (`&str`, `&[T]`) rather than owning, unless ownership transfer is genuinely required.
- No byte-index string slicing that could panic on a non-char-boundary; `.chars()`/`.get()` used instead where the input isn't guaranteed ASCII.
