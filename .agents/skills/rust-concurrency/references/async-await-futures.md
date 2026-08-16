# Async/Await and Futures

## `async fn` and `.await`

An `async fn` returns a value implementing `Future<Output = T>` instead of running immediately — nothing happens until the future is `.await`ed (or otherwise driven) on a runtime.

```rust
async fn fetch_len(url: &str) -> Result<usize, reqwest::Error> {
    let body = reqwest::get(url).await?.text().await?;
    Ok(body.len())
}
```

An `async fn`/`async` block compiles to a state machine implementing `Future`; it needs an executor (a runtime, e.g. Tokio) to actually run — see `references/channels-tokio-async-runtime.md`.

## Async closures (stable, Rust 1.85+)

Native `AsyncFn`/`AsyncFnMut`/`AsyncFnOnce` closures let you write `async || { ... }` directly, instead of a sync closure returning a boxed future:

```rust
async fn run<F, Fut>(f: F) where F: Fn() -> Fut, Fut: std::future::Future<Output = ()> {
    f().await;
}

let greet = async || {
    println!("hello from an async closure");
};
run(greet).await;
```

Before async closures stabilized, the common workaround was a sync closure returning a boxed future (`Fn() -> Pin<Box<dyn Future<Output = ()>>>`) — prefer the native `async ||` form in new code.

## `async fn` in traits

Native `async fn` in traits works for **static dispatch** (generics, `impl Trait`) since Rust 1.85 — no external crate needed:

```rust
trait Fetcher {
    async fn fetch(&self, id: u32) -> String;
}

struct HttpFetcher;
impl Fetcher for HttpFetcher {
    async fn fetch(&self, id: u32) -> String {
        format!("data for {id}")
    }
}

async fn use_fetcher(f: &impl Fetcher) -> String {
    f.fetch(1).await
}
```

This does **not** make the trait object-safe: `dyn Fetcher` fails to compile because async methods desugar to `-> impl Future`, which can't appear in a vtable. Only reach for the `async-trait` crate (which boxes the future) when you specifically need `Box<dyn Fetcher>`:

```rust
#[async_trait::async_trait]
trait Fetcher {
    async fn fetch(&self, id: u32) -> String;
}
// now `Box<dyn Fetcher>` compiles, at the cost of one heap allocation per call
```

## Structured concurrency: joining multiple futures

```rust
async fn run_both() {
    let (a, b) = tokio::join!(fetch_len("https://a"), fetch_len("https://b"));
    println!("{a:?} {b:?}");
}
```

Use `tokio::join!`/`futures::future::join_all` for a fixed or dynamic set of futures that must all complete; use `tokio::select!` when you need the *first* of several futures to complete (e.g., a request racing a timeout) — see the Tokio reference file for both.

## `Send`/`Sync` bounds and `.await` points

A future generated from `async fn` is `Send` only if every value held *across* an `.await` point is itself `Send`. This is the most common source of `the future cannot be sent between threads safely` errors on multi-threaded runtimes:

```rust
// BAD: `Rc` is not Send, and it's alive across the .await point
async fn bad() {
    let data = std::rc::Rc::new(5);
    some_async_call().await;
    println!("{data}");
}

// GOOD: drop or scope the non-Send value before the .await
async fn good() {
    {
        let data = std::rc::Rc::new(5);
        println!("{data}");
    } // dropped here, before the .await
    some_async_call().await;
}
```

The same applies to lock guards: never hold a `std::sync::MutexGuard` across `.await` — use `tokio::sync::Mutex` if you truly need to hold a lock across an await point, or (preferably) copy the needed data out and drop the guard first.

## Implementing `Future` manually (rare)

Only needed for low-level integration (e.g., bridging a callback API). Most code should compose existing `async fn`s instead:

```rust
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

struct Ready<T>(Option<T>);

impl<T: Unpin> Future for Ready<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<T> {
        Poll::Ready(self.0.take().expect("polled after completion"))
    }
}
```

## Stop conditions for this file

- No `std::sync::MutexGuard` (or `Rc`/`RefCell`) is held across an `.await` point.
- `dyn Trait` with async methods only appears alongside `#[async_trait::async_trait]`, and only when genuinely needed.
- The future compiles under the target runtime's `Send` requirements (multi-threaded Tokio requires `Send` futures for `tokio::spawn`).
