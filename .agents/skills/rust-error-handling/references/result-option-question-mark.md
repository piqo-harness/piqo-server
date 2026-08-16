# Result, Option, and the ? Operator

## `Option<T>`: value may be absent

```rust
fn find(v: &[i32], target: i32) -> Option<usize> {
    v.iter().position(|&x| x == target)
}

match find(&[1, 2, 3], 2) {
    Some(i) => println!("found at {i}"),
    None => println!("not found"),
}
```

Common combinators:

```rust
let maybe: Option<i32> = Some(4);
let doubled = maybe.map(|x| x * 2);          // Option<i32>
let value = maybe.unwrap_or(0);               // i32, default if None
let value = maybe.unwrap_or_else(|| compute()); // lazy default
let chained = maybe.and_then(|x| if x > 0 { Some(x) } else { None });
let as_result: Result<i32, &str> = maybe.ok_or("was none");
```

## `Result<T, E>`: value or an error

```rust
fn parse_port(s: &str) -> Result<u16, std::num::ParseIntError> {
    s.parse::<u16>()
}

match parse_port("8080") {
    Ok(port) => println!("port: {port}"),
    Err(e) => println!("invalid port: {e}"),
}
```

Combinators mirror `Option`'s:

```rust
let result: Result<i32, String> = Ok(4);
let doubled = result.map(|x| x * 2);
let value = result.unwrap_or(0);
let mapped_err = result.map_err(|e| format!("wrapped: {e}"));
```

## The `?` operator

`?` unwraps `Ok`/`Some` or returns early with `Err`/`None` (converting the error type via `From`, for `Result`):

```rust
use std::num::ParseIntError;

fn parse_and_double(s: &str) -> Result<i32, ParseIntError> {
    let n: i32 = s.parse()?; // returns Err early if parse fails
    Ok(n * 2)
}
```

`?` also works on `Option` inside a function returning `Option`:

```rust
fn first_char_upper(s: &str) -> Option<char> {
    let c = s.chars().next()?;
    Some(c.to_ascii_uppercase())
}
```

`?` cannot mix `Result` and `Option` in the same function — convert explicitly with `.ok_or(...)` (`Option` → `Result`) or `.ok()` (`Result` → `Option`, discarding the error) when you need to cross between them.

## Automatic error conversion with `?`

`?` calls `From::from` on the error before returning, so implement `From<SourceError> for MyError` once and every `?` in that function converts automatically:

```rust
#[derive(Debug)]
enum AppError {
    Parse(std::num::ParseIntError),
    Io(std::io::Error),
}

impl From<std::num::ParseIntError> for AppError {
    fn from(e: std::num::ParseIntError) -> Self { AppError::Parse(e) }
}
impl From<std::io::Error> for AppError {
    fn from(e: std::io::Error) -> Self { AppError::Io(e) }
}

fn read_and_parse(path: &str) -> Result<i32, AppError> {
    let contents = std::fs::read_to_string(path)?; // io::Error -> AppError via From
    let n: i32 = contents.trim().parse()?;         // ParseIntError -> AppError via From
    Ok(n)
}
```

## `main` returning `Result`

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let contents = std::fs::read_to_string("config.toml")?;
    println!("{contents}");
    Ok(())
}
```

Returning `Result` from `main` lets `?` propagate all the way out; on `Err`, the program exits with status 1 and prints the error via `Debug`.

## `?` inside iterators / loops collecting Results

```rust
fn parse_all(strs: &[&str]) -> Result<Vec<i32>, std::num::ParseIntError> {
    strs.iter().map(|s| s.parse::<i32>()).collect() // Vec<Result<T,E>> -> Result<Vec<T>,E>
}
```

`.collect::<Result<Vec<_>, _>>()` short-circuits on the first `Err`, mirroring `?` semantics across a whole iterator.

## Stop conditions for this file

- Every fallible call in the function either handles its error explicitly or propagates with `?`.
- No mixing of `Result` and `Option` via `?` without an explicit `.ok_or(...)`/`.ok()` conversion.
- `From` impls exist for every error type that needs to flow through `?` into the function's declared error type.
