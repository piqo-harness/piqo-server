# WebAssembly with wasm-bindgen

## Project setup

```toml
# Cargo.toml
[lib]
crate-type = ["cdylib", "rlib"]

[dependencies]
wasm-bindgen = "0.2"
```

Build with `wasm-pack` (wraps `cargo build --target wasm32-unknown-unknown` and generates the JS glue code):

```bash
wasm-pack build --target web
```

## Exposing Rust functions to JavaScript

```rust
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn add(a: i32, b: i32) -> i32 {
    a + b
}

#[wasm_bindgen]
pub fn greet(name: &str) -> String {
    format!("Hello, {name}!")
}
```

```js
import init, { add, greet } from "./pkg/my_crate.js";

await init();
console.log(add(2, 3));      // 5
console.log(greet("Ada"));   // "Hello, Ada!"
```

`#[wasm_bindgen]` handles the JS ↔ Rust marshaling (numbers, strings, and — for structs — reference-counted JS-visible objects) so you don't hand-write the C-ABI-style boundary conventions used in `c-ffi-interop.md`.

## Exposing a struct

```rust
#[wasm_bindgen]
pub struct Counter {
    value: i32,
}

#[wasm_bindgen]
impl Counter {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Counter {
        Counter { value: 0 }
    }

    pub fn increment(&mut self) -> i32 {
        self.value += 1;
        self.value
    }
}
```

```js
import { Counter } from "./pkg/my_crate.js";
const c = new Counter();
console.log(c.increment()); // 1
```

## Calling JavaScript from Rust

```rust
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);

    #[wasm_bindgen(js_namespace = Math)]
    fn random() -> f64;
}

#[wasm_bindgen]
pub fn log_random() {
    log(&format!("random: {}", random()));
}
```

## Async and `Promise` interop

Use `wasm-bindgen-futures` to bridge Rust futures and JS `Promise`s in both directions:

```toml
[dependencies]
wasm-bindgen-futures = "0.4"
```

```rust
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::window;

#[wasm_bindgen]
pub async fn fetch_text(url: String) -> Result<String, JsValue> {
    let resp = JsFuture::from(window().unwrap().fetch_with_str(&url)).await?;
    // ... convert the JS Response into text, also via JsFuture
    Ok(String::new())
}
```

An `async fn` exported with `#[wasm_bindgen]` compiles to a JS function returning a `Promise` automatically.

## Passing complex data: `serde-wasm-bindgen`

For structured data beyond primitives/strings/simple structs, serialize through `serde` rather than hand-mapping every field:

```toml
[dependencies]
serde = { version = "1", features = ["derive"] }
serde-wasm-bindgen = "0.6"
```

```rust
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Point { x: f64, y: f64 }

#[wasm_bindgen]
pub fn distance(a: JsValue, b: JsValue) -> Result<f64, JsValue> {
    let a: Point = serde_wasm_bindgen::from_value(a)?;
    let b: Point = serde_wasm_bindgen::from_value(b)?;
    Ok(((a.x - b.x).powi(2) + (a.y - b.y).powi(2)).sqrt())
}
```

## Panics in WASM

A Rust panic in WASM traps the whole module (it does not neatly propagate as a JS exception by default) — set the panic hook once at startup so panics at least produce a readable console message during development:

```toml
[dependencies]
console_error_panic_hook = "0.1"
```

```rust
#[wasm_bindgen(start)]
pub fn main() {
    console_error_panic_hook::set_once();
}
```

## Stop conditions for this file

- Every exported item is either a primitive/string, a `#[wasm_bindgen]`-annotated struct/impl, or serialized via `serde-wasm-bindgen` — no ad hoc manual byte marshaling.
- `console_error_panic_hook` (or an equivalent) is installed so panics are diagnosable rather than silently trapping the module.
- Async exported functions were verified to actually resolve/reject the resulting JS `Promise` as expected from the JS side, not just compile.
