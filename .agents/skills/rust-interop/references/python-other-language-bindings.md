# Python (PyO3) and Other Language Bindings

## Project setup for a Python extension module

```toml
# Cargo.toml
[lib]
name = "my_module"
crate-type = ["cdylib"]

[dependencies]
pyo3 = { version = "0.22", features = ["extension-module"] }
```

Build with `maturin` (the standard PyO3 build/packaging tool):

```bash
pip install maturin
maturin develop     # builds and installs into the active virtualenv for local testing
maturin build --release
```

## Exposing functions and classes to Python

```rust
use pyo3::prelude::*;

#[pyfunction]
fn add(a: i64, b: i64) -> i64 {
    a + b
}

#[pyclass]
struct Counter {
    value: i64,
}

#[pymethods]
impl Counter {
    #[new]
    fn new() -> Self {
        Counter { value: 0 }
    }

    fn increment(&mut self) -> i64 {
        self.value += 1;
        self.value
    }
}

#[pymodule]
fn my_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(add, m)?)?;
    m.add_class::<Counter>()?;
    Ok(())
}
```

```python
import my_module
print(my_module.add(2, 3))          # 5
c = my_module.Counter()
print(c.increment())                 # 1
```

## Error handling: Rust `Result` → Python exceptions

`PyResult<T>` is `Result<T, PyErr>` — return it from a `#[pyfunction]`/`#[pymethods]` method and PyO3 raises the corresponding Python exception automatically:

```rust
use pyo3::exceptions::PyValueError;

#[pyfunction]
fn parse_positive(s: &str) -> PyResult<i64> {
    let n: i64 = s.parse().map_err(|_| PyValueError::new_err("not a valid integer"))?;
    if n <= 0 {
        return Err(PyValueError::new_err("must be positive"));
    }
    Ok(n)
}
```

Implement `From<MyError> for PyErr` for a custom error type so `?` converts automatically, mirroring the `From`-based conversion pattern from rust-error-handling.

## The GIL and releasing it for long-running Rust work

PyO3 holds Python's Global Interpreter Lock (GIL) by default for the duration of a call from Python into Rust. Release it explicitly around CPU-bound Rust work so other Python threads can run concurrently:

```rust
use pyo3::prelude::*;

#[pyfunction]
fn expensive_computation(py: Python<'_>, n: u64) -> u64 {
    py.allow_threads(|| {
        // No Python objects may be touched inside this closure — the GIL is released.
        (0..n).sum()
    })
}
```

Never touch a `Python<'_>` token or any `Py<T>`/`PyObject` from inside `allow_threads`'s closure — doing so is a soundness violation PyO3 cannot always catch at compile time.

## Converting between Rust and Python collections

```rust
use pyo3::types::PyList;

#[pyfunction]
fn sum_list(list: &Bound<'_, PyList>) -> PyResult<i64> {
    let mut total = 0;
    for item in list.iter() {
        total += item.extract::<i64>()?;
    }
    Ok(total)
}
```

For structured data, derive `serde::Serialize`/`Deserialize` and use `pythonize`/`depythonize` (or PyO3's `FromPyObject`/`IntoPy` derives) instead of manually walking `PyDict`/`PyList` for anything beyond a couple of fields.

## General shape for other language bindings

The same pattern recurs across binding crates for other host languages (Ruby via `magnus`, Node.js via `napi-rs`, Java/JVM via `jni`): a proc-macro/attribute-based crate marshals values across the boundary, the host runtime's equivalent of the GIL/thread model must be respected, and errors convert into the host language's native exception type rather than being handled as raw `Result` on the other side. Reach for the ecosystem-standard binding crate for the target language rather than hand-writing a raw C-ABI FFI shim (see `c-ffi-interop.md`) unless no such crate exists.

## Stop conditions for this file

- Every exposed function's `Result`/`PyResult` error path raises an appropriate Python exception type, not a generic one, when the caller would need to distinguish failure kinds.
- Any CPU-bound work releases the GIL via `py.allow_threads`, and nothing inside that closure touches a Python object.
- Structured data crossing the boundary goes through `#[pyclass]`/serde-based conversion rather than manual `PyDict`/`PyList` field-by-field extraction, unless the structure is trivial.
