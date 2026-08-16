# Tokio: Async Runtime, Channels, and Task Management

Tokio is the de facto standard async runtime for Rust. Add it with the features you need, e.g. `cargo add tokio --features full` for prototyping, or a narrower feature list (`rt-multi-thread`, `macros`, `sync`, `net`, `time`) for production builds.

## Entry point

```rust
#[tokio::main]
async fn main() {
    let result = do_work().await;
    println!("{result:?}");
}
```

`#[tokio::main]` defaults to the multi-threaded runtime. For a single-threaded runtime (lower overhead, fine for I/O-bound work with no CPU parallelism needed):

```rust
#[tokio::main(flavor = "current_thread")]
async fn main() { /* ... */ }
```

## Spawning tasks

```rust
async fn compute(n: u32) -> u32 {
    n * n
}

#[tokio::main]
async fn main() {
    let handle = tokio::spawn(compute(21));
    let result = handle.await.unwrap(); // JoinHandle<T> — await then unwrap the JoinError
    println!("{result}");
}
```

`tokio::spawn` requires the future to be `'static` and `Send` (on the multi-threaded runtime) — the same rule as `std::thread::spawn`, but for tasks instead of OS threads. Tasks are much cheaper than threads; spawning thousands is normal.

## `join!` vs `select!`

```rust
use tokio::time::{sleep, Duration};

async fn with_timeout() {
    tokio::select! {
        result = compute(21) => println!("computed: {result}"),
        _ = sleep(Duration::from_secs(5)) => println!("timed out"),
    }
}

async fn run_concurrently() {
    let (a, b) = tokio::join!(compute(1), compute(2));
    println!("{a} {b}");
}
```

- `join!` waits for **all** branches to complete and returns all results — use for a fixed, known set of independent operations that are all needed.
- `select!` returns as soon as the **first** branch completes and cancels the rest — use for timeouts, cancellation, or racing multiple sources of the same event.

For a dynamic number of futures, use `futures::future::join_all` (all must complete) or a `JoinSet` (Tokio's task-tracking collection, supports cancellation and result draining as tasks finish):

```rust
use tokio::task::JoinSet;

let mut set = JoinSet::new();
for i in 0..5 {
    set.spawn(compute(i));
}
while let Some(res) = set.join_next().await {
    println!("{:?}", res.unwrap());
}
```

## Async channels

```rust
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    let (tx, mut rx) = mpsc::channel::<u32>(32); // bounded channel, capacity 32

    tokio::spawn(async move {
        for i in 0..5 {
            tx.send(i).await.unwrap();
        }
    });

    while let Some(v) = rx.recv().await {
        println!("{v}");
    }
}
```

Use `tokio::sync::oneshot` for a single value passed once (e.g., a request/response pattern), and `tokio::sync::broadcast` when multiple consumers each need every message.

## Async mutex

Only use `tokio::sync::Mutex` (instead of `std::sync::Mutex`) when the lock must genuinely be held across an `.await` point; otherwise prefer the std version — it's cheaper and avoids accidentally serializing the whole runtime on lock contention.

```rust
use std::sync::Arc;
use tokio::sync::Mutex;

let shared = Arc::new(Mutex::new(0));
let shared2 = Arc::clone(&shared);
tokio::spawn(async move {
    let mut guard = shared2.lock().await;
    *guard += 1;
});
```

## Cancellation

Dropping a `JoinHandle` does not stop the task — Tokio tasks run to completion unless explicitly aborted. Use `handle.abort()` or a `CancellationToken` (from `tokio-util`) for cooperative cancellation:

```rust
let handle = tokio::spawn(async { /* long work */ });
handle.abort(); // requests cancellation at the next await point
```

## Stop conditions for this file

- The runtime flavor (`current_thread` vs multi-threaded) matches the actual parallelism need.
- `select!` is used only when racing/timeout semantics are intended; `join!`/`JoinSet` used when all results are needed.
- Any task holding a `tokio::sync::Mutex` across `.await` genuinely needs to (data must stay locked while awaiting), not just out of convenience.
