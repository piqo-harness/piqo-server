# C FFI Interop

## Calling a C function from Rust

```rust
#[link(name = "m")] // link against libm
extern "C" {
    fn sqrt(x: f64) -> f64;
}

fn main() {
    let result = unsafe { sqrt(4.0) };
    println!("{result}");
}
```

Every call into an `extern "C"` function is `unsafe` — the compiler cannot verify the foreign function's contract (argument validity, thread-safety, whether it can panic/longjmp).

## Exposing a Rust function to C

```rust
#[no_mangle] // keep the symbol name unmangled so C can link against it by name
pub extern "C" fn add(a: i32, b: i32) -> i32 {
    a + b
}
```

`#[unsafe(no_mangle)]` is the edition-2024 spelling once the "unsafe attributes" lint is enforced — check your edition's requirement; both compile identically on current stable, but new code should prefer explicitly marking it `unsafe` where the edition requires it.

## `#[repr(C)]` for cross-language struct layout

Rust's default struct layout is unspecified and may be reordered by the compiler — always add `#[repr(C)]` to any type shared across an FFI boundary so its field layout matches what C expects:

```rust
#[repr(C)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

#[no_mangle]
pub extern "C" fn point_distance(a: Point, b: Point) -> f64 {
    ((a.x - b.x).powi(2) + (a.y - b.y).powi(2)).sqrt()
}
```

## Strings across the boundary

C strings are null-terminated byte sequences; Rust `String`/`&str` are not. Use `std::ffi::CString`/`CStr` to convert:

```rust
use std::ffi::{CStr, CString};
use std::os::raw::c_char;

#[no_mangle]
pub extern "C" fn greet(name: *const c_char) -> *mut c_char {
    let name = unsafe {
        // SAFETY: caller guarantees `name` is a valid, null-terminated C string.
        CStr::from_ptr(name).to_string_lossy().into_owned()
    };
    let greeting = format!("Hello, {name}!");
    CString::new(greeting).unwrap().into_raw() // caller must free this with `free_string` below
}

#[no_mangle]
pub extern "C" fn free_string(s: *mut c_char) {
    if s.is_null() {
        return;
    }
    // SAFETY: `s` must have come from `CString::into_raw` above and not been freed already.
    unsafe { drop(CString::from_raw(s)) };
}
```

Document and enforce a single, unambiguous ownership rule for every pointer that crosses the boundary: if Rust allocates it, Rust must provide (and the caller must call) the matching `free_*` function — never let the C side call `free()` directly on Rust-allocated memory (different allocators).

## Generating bindings instead of hand-writing them

- `bindgen`: generates Rust `extern "C"` declarations from a C header — use when consuming an existing C library.

```rust
// build.rs
fn main() {
    let bindings = bindgen::Builder::default()
        .header("wrapper.h")
        .generate()
        .expect("failed to generate bindings");
    let out_path = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
    bindings.write_to_file(out_path.join("bindings.rs")).unwrap();
}
```

- `cbindgen`: generates a C header from Rust `#[no_mangle] extern "C"` items — use when publishing a Rust library for C consumers.

```rust
// build.rs
fn main() {
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    cbindgen::generate(crate_dir)
        .expect("failed to generate C bindings")
        .write_to_file("include/mylib.h");
}
```

Prefer generated bindings over hand-written `extern "C"` blocks for any non-trivial existing API — hand-transcribing signatures is a common source of subtle ABI mismatches (wrong integer width, missing `#[repr(C)]`).

## Panics must not cross the FFI boundary

Unwinding a panic across an `extern "C"` function is undefined behavior. Wrap the body in `catch_unwind` if the Rust code might panic:

```rust
use std::panic::catch_unwind;

#[no_mangle]
pub extern "C" fn safe_divide(a: i32, b: i32) -> i32 {
    catch_unwind(|| a / b).unwrap_or(-1) // returns -1 instead of unwinding on a panic (e.g. divide by zero)
}
```

## Stop conditions for this file

- Every type crossing the boundary is `#[repr(C)]` (or a plain primitive/pointer with a well-defined C representation).
- Every heap allocation crossing the boundary has one documented, matching deallocation function, and the C side never calls its own `free()` on Rust-allocated memory.
- No `extern "C" fn` can unwind a Rust panic across the boundary unguarded.
