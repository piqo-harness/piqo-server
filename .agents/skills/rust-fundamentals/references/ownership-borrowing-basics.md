# Ownership and Borrowing Basics

## The three rules

1. Each value has exactly one owner at a time.
2. When the owner goes out of scope, the value is dropped.
3. Ownership can move, or the value can be borrowed (immutably or mutably), but not both a mutable borrow and any other borrow at the same time.

```rust
fn main() {
    let s1 = String::from("hello");
    let s2 = s1; // s1 is moved into s2; s1 is no longer valid
    // println!("{s1}"); // error: value borrowed after move
    println!("{s2}");
}
```

Types that implement `Copy` (integers, floats, `bool`, `char`, and tuples/arrays of `Copy` types) are copied instead of moved:

```rust
let x = 5;
let y = x; // copy, not move
println!("{x} {y}"); // both valid
```

## Borrowing with references

Borrow instead of moving when the callee only needs to read or temporarily mutate the data:

```rust
fn len(s: &String) -> usize {
    s.len()
}

fn push_world(s: &mut String) {
    s.push_str(" world");
}

fn main() {
    let mut s = String::from("hello");
    println!("{}", len(&s));
    push_world(&mut s);
    println!("{s}");
}
```

Rules enforced by the borrow checker:
- Any number of immutable references (`&T`) OR exactly one mutable reference (`&mut T`) — never both at once, in the same scope.
- A reference must never outlive the value it points to (no dangling references).

```rust
fn main() {
    let mut v = vec![1, 2, 3];
    let first = &v[0];
    // v.push(4); // error: cannot borrow `v` as mutable because it is also borrowed as immutable
    println!("{first}");
    v.push(4); // fine once `first`'s borrow has ended (non-lexical lifetimes)
}
```

## Non-lexical lifetimes (NLL)

A borrow's scope ends at its last use, not at the end of the enclosing block — this is why the `v.push(4)` above compiles once `first` is no longer read afterward.

## Moving into and out of functions

```rust
fn takes_ownership(s: String) -> String {
    println!("{s}");
    s // return ownership back to the caller
}

fn main() {
    let s = String::from("hello");
    let s = takes_ownership(s); // reassign since ownership was moved and returned
    println!("{s}");
}
```

Prefer borrowing (`&String` or, better, `&str`) over taking and returning ownership when the function does not need to consume the value.

## Common borrow-checker fixes, in order of preference

1. Shorten the borrow's scope (introduce a block `{ }` or reorder statements) so it ends before the conflicting use.
2. Restructure the data (e.g., split a struct field so two different fields are borrowed instead of the whole struct).
3. Use an index or a re-borrow instead of holding a long-lived reference.
4. Only as a last resort, `.clone()` the value — and note in the code why a borrow could not be used.

## Stop conditions for this file

- The code compiles without moved-value or borrow-conflict errors.
- Mutability (`mut`) is present only where needed.
- No unnecessary `.clone()` was added without first trying to restructure the borrow.
