# Smart Pointers and Interior Mutability

## `Box<T>`: simple heap allocation

Use `Box<T>` to put a value on the heap with single ownership — needed for recursive types (unknown size at compile time) or to avoid moving a large value around the stack.

```rust
enum List {
    Cons(i32, Box<List>),
    Nil,
}

use List::{Cons, Nil};
let list = Cons(1, Box::new(Cons(2, Box::new(Nil))));
```

`Box<T>` has no runtime cost beyond the allocation itself — it derefs transparently to `T`.

## `Rc<T>` / `Arc<T>`: shared ownership

`Rc<T>` (single-threaded) and `Arc<T>` (thread-safe, atomic refcount) allow multiple owners of the same heap data. Cloning increments the refcount rather than copying data; the data drops when the last `Rc`/`Arc` drops.

```rust
use std::rc::Rc;

let a = Rc::new(String::from("shared"));
let b = Rc::clone(&a); // increments refcount, same allocation
println!("count = {}", Rc::strong_count(&a)); // 2
```

Use `Arc<T>` instead of `Rc<T>` the moment the data crosses a thread boundary — see rust-concurrency for `Arc<Mutex<T>>` patterns.

## `RefCell<T>` / `Cell<T>`: interior mutability

`RefCell<T>` moves borrow-checking from compile time to runtime — it lets you mutate data through a shared (`&`) reference, panicking if the runtime borrow rules (one mutable XOR many immutable) are violated.

```rust
use std::cell::RefCell;

struct Counter {
    count: RefCell<i32>,
}

let counter = Counter { count: RefCell::new(0) };
*counter.count.borrow_mut() += 1; // panics if another borrow is active
println!("{}", counter.count.borrow());
```

`Cell<T>` is a lighter-weight alternative for `Copy` types — no borrow tracking, just `get()`/`set()`:

```rust
use std::cell::Cell;

let hits = Cell::new(0);
hits.set(hits.get() + 1);
```

Only reach for `RefCell`/`Cell` when a `&self` method genuinely must mutate shared state (e.g., a cache, a lazily-computed field, or the classic `Rc<RefCell<T>>` shared-mutable-graph pattern) — prefer `&mut self` and normal borrowing whenever the call site can provide unique access instead.

## `Rc<RefCell<T>>`: the common shared-mutable pattern

```rust
use std::rc::Rc;
use std::cell::RefCell;

#[derive(Debug)]
struct Node {
    value: i32,
    children: Vec<Rc<RefCell<Node>>>,
}

let leaf = Rc::new(RefCell::new(Node { value: 3, children: vec![] }));
let branch = Rc::new(RefCell::new(Node { value: 5, children: vec![Rc::clone(&leaf)] }));
leaf.borrow_mut().value = 10;
```

## Breaking cycles with `Weak<T>`

A parent holding `Rc` children and children holding `Rc` back to their parent creates a reference cycle — the refcount never reaches zero and the memory leaks. Use `Weak<T>` (via `Rc::downgrade`) for back-references; `Weak` does not keep the value alive and must be `.upgrade()`d (returns `Option<Rc<T>>`) before use.

```rust
use std::rc::{Rc, Weak};
use std::cell::RefCell;

struct Node {
    value: i32,
    parent: RefCell<Weak<Node>>,
    children: RefCell<Vec<Rc<Node>>>,
}

let leaf = Rc::new(Node { value: 3, parent: RefCell::new(Weak::new()), children: RefCell::new(vec![]) });
let branch = Rc::new(Node { value: 5, parent: RefCell::new(Weak::new()), children: RefCell::new(vec![Rc::clone(&leaf)]) });
*leaf.parent.borrow_mut() = Rc::downgrade(&branch);

if let Some(parent) = leaf.parent.borrow().upgrade() {
    println!("parent value: {}", parent.value);
}
```

Rule of thumb: the "owning" direction (parent → child) uses `Rc`; the "non-owning" direction (child → parent) uses `Weak`.

## Choosing among smart pointers

| Need | Use |
|---|---|
| Single owner, heap allocation | `Box<T>` |
| Multiple owners, single thread | `Rc<T>` |
| Multiple owners, across threads | `Arc<T>` |
| Mutate through a shared reference (single thread) | `RefCell<T>` (or `Cell<T>` for `Copy` types) |
| Mutate through a shared reference, across threads | `Mutex<T>`/`RwLock<T>` (see rust-concurrency) |
| Non-owning back-reference to avoid a cycle | `Weak<T>` |

## Stop conditions for this file

- The smart pointer chosen is the least powerful one that satisfies the actual ownership/mutation/thread-sharing need.
- Any bidirectional reference structure uses `Weak` on the non-owning side to avoid a leaking cycle.
- `RefCell`'s runtime borrow panics were considered (no overlapping `borrow()`/`borrow_mut()` in the same scope) before shipping the code.
