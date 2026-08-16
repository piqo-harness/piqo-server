# Unsafe Rust

## What `unsafe` actually enables

An `unsafe` block does not disable the borrow checker or turn off all checks — it unlocks exactly five additional capabilities, and the programmer takes on the responsibility the compiler would otherwise enforce:

1. Dereferencing a raw pointer (`*const T` / `*mut T`).
2. Calling an `unsafe fn` or `unsafe` method (including FFI functions).
3. Accessing or mutating a mutable `static` variable.
4. Implementing an `unsafe trait` (e.g., `unsafe impl Send for MyType {}`).
5. Accessing fields of a `union`.

Everything else (moves, borrows, generics, most of the standard library) works exactly the same inside `unsafe` blocks.

## Raw pointers

```rust
let mut num = 5;
let r1 = &num as *const i32;
let r2 = &mut num as *mut i32;

unsafe {
    println!("r1 = {}", *r1);
    *r2 += 1;
}
```

Raw pointers may be null, dangling, or unaligned, and creating one is always safe — only *dereferencing* it requires `unsafe`. Never dereference a raw pointer without first confirming it is non-null, aligned, and points to a live, correctly-typed value.

## Writing a sound `unsafe fn`

```rust
/// # Safety
/// `ptr` must be non-null, properly aligned, and point to a valid, initialized `i32`
/// that is not concurrently mutated for the duration of this call.
unsafe fn read_raw(ptr: *const i32) -> i32 {
    unsafe { *ptr }
}
```

Document the exact preconditions in a `# Safety` doc comment section — this is the contract every caller must uphold; the compiler cannot check it for you.

## Minimizing the unsafe surface

Wrap `unsafe` operations in a small, safe function whose signature makes misuse hard, and put the invariant-justifying comment immediately above the `unsafe` block, not just in a doc comment far away:

```rust
fn get_or_zero(slice: &[i32], index: usize) -> i32 {
    if index >= slice.len() {
        return 0;
    }
    // SAFETY: index < slice.len() was just checked above.
    unsafe { *slice.get_unchecked(index) }
}
```

Prefer safe standard-library alternatives (`slice.get(index)`, `split_at_mut`, `Cell`/`RefCell`) whenever they solve the problem — reach for raw pointers/`unsafe` only when a safe abstraction genuinely doesn't exist or profiling shows the safe path is the bottleneck.

## `unsafe impl Send`/`Sync`

Only implement these by hand when you have manually verified the type upholds the trait's contract (no interior aliasing hazards across threads) — this is a common source of undefined behavior when done to silence a compiler error rather than after genuine analysis:

```rust
struct RawBuffer(*mut u8, usize);

// SAFETY: RawBuffer owns its buffer exclusively and never aliases it;
// no two threads ever hold overlapping raw pointers into the same buffer.
unsafe impl Send for RawBuffer {}
```

## `unsafe` and undefined behavior

Undefined behavior in Rust includes: dereferencing a dangling/null/misaligned/unowned pointer, creating two `&mut` references to the same location (including through raw pointers converted back to references), reading uninitialized memory as an initialized type, and data races. `unsafe` code that avoids compiler errors but still triggers UB is a bug — the type system's usual guarantees no longer hold once inside the block, so reason about each invariant explicitly rather than relying on "it compiled."

## Working with `MaybeUninit`

Prefer `std::mem::MaybeUninit<T>` over reading uninitialized memory directly when building up a value incrementally (e.g., filling an array without a default value first):

```rust
use std::mem::MaybeUninit;

let mut arr: [MaybeUninit<i32>; 4] = [const { MaybeUninit::uninit() }; 4];
for (i, slot) in arr.iter_mut().enumerate() {
    slot.write(i as i32);
}
// SAFETY: every element was written above.
let arr: [i32; 4] = unsafe { std::mem::transmute(arr) };
```

## Stop conditions for this file

- Every `unsafe` block is preceded by a `// SAFETY:` comment stating exactly why the operation is sound at that call site.
- No safe standard-library alternative was skipped in favor of raw pointers/`unsafe` without a concrete reason (FFI, verified perf need, or a genuinely missing safe abstraction).
- `unsafe impl Send`/`Sync` was added only after manually verifying the type's actual thread-safety, not to silence a compiler error.
