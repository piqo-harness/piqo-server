# Threads and Synchronization Primitives

## Spawning and joining threads

```rust
use std::thread;

fn main() {
    let handle = thread::spawn(|| {
        for i in 1..5 {
            println!("spawned: {i}");
        }
    });

    for i in 1..3 {
        println!("main: {i}");
    }

    handle.join().unwrap(); // block until the spawned thread finishes
}
```

Closures passed to `thread::spawn` must be `'static` (own all their captures) and `Send`. Use `move` to force ownership of captured variables:

```rust
let data = vec![1, 2, 3];
let handle = thread::spawn(move || {
    println!("{data:?}");
});
handle.join().unwrap();
```

## Sharing state with `Arc<Mutex<T>>`

`Rc<T>` is not thread-safe (its reference count isn't atomic) — use `Arc<T>` (atomically reference-counted) across threads. Wrap the shared data in a `Mutex` (or `RwLock`) so only one thread mutates it at a time.

```rust
use std::sync::{Arc, Mutex};
use std::thread;

fn main() {
    let counter = Arc::new(Mutex::new(0));
    let mut handles = vec![];

    for _ in 0..10 {
        let counter = Arc::clone(&counter);
        handles.push(thread::spawn(move || {
            let mut num = counter.lock().unwrap();
            *num += 1;
        })); // lock guard drops here, at end of closure
    }

    for handle in handles {
        handle.join().unwrap();
    }

    println!("Result: {}", *counter.lock().unwrap());
}
```

`Mutex::lock()` returns a `Result` because it can be poisoned if a thread panicked while holding the lock; `.unwrap()` is the common (if blunt) default. Keep the guard's scope as short as possible — assign it to a block or drop it explicitly before doing unrelated work.

Use `RwLock<T>` instead of `Mutex<T>` when reads vastly outnumber writes — it allows multiple simultaneous readers OR one writer:

```rust
use std::sync::RwLock;

let lock = RwLock::new(5);
{
    let r1 = lock.read().unwrap();
    let r2 = lock.read().unwrap(); // multiple readers OK
    println!("{} {}", *r1, *r2);
}
{
    let mut w = lock.write().unwrap();
    *w += 1;
}
```

## Channels (`std::sync::mpsc`)

Multiple-producer, single-consumer queue for message passing between threads:

```rust
use std::sync::mpsc;
use std::thread;

fn main() {
    let (tx, rx) = mpsc::channel();

    for id in 0..3 {
        let tx = tx.clone();
        thread::spawn(move || {
            tx.send(format!("message from {id}")).unwrap();
        });
    }
    drop(tx); // drop the original sender so rx knows when all senders are gone

    for received in rx { // iterates until all senders are dropped
        println!("{received}");
    }
}
```

Prefer channels over shared-state locking when the code naturally decomposes into producer/consumer stages — it sidesteps most lock-ordering deadlocks entirely.

## `Send` and `Sync`

- `Send`: safe to transfer ownership of a value to another thread. Almost all types are `Send`; notable exceptions include `Rc<T>` and raw pointers.
- `Sync`: safe to share `&T` across threads (equivalent to `&T: Send`). `Cell`/`RefCell` are not `Sync` (no synchronization); `Mutex<T>`/`RwLock<T>` are `Sync` when `T: Send`.

These are auto-derived marker traits — the compiler infers them from a type's fields. If a compile error names a missing `Send`/`Sync` bound, find and replace the actual non-thread-safe field (commonly `Rc`, `RefCell`, or a raw pointer) rather than force-implementing the marker trait with `unsafe impl`.

## Stop conditions for this file

- Shared mutable state across threads uses `Arc<Mutex<T>>`/`Arc<RwLock<T>>`, never a bare `Rc`/`RefCell`.
- Every `Mutex`/`RwLock` guard's scope is as short as possible and never held across a blocking call that could deadlock another thread.
- Every spawned thread is either joined or its fire-and-forget nature is intentional and documented.
