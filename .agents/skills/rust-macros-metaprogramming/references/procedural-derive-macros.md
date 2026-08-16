# Procedural Macros and build.rs

## Proc-macro crate setup

Procedural macros live in their own crate with `proc-macro = true`:

```toml
# Cargo.toml
[lib]
proc-macro = true

[dependencies]
syn = { version = "2", features = ["full"] }
quote = "1"
proc-macro2 = "1"
```

- `syn` parses a `TokenStream` into a typed syntax tree.
- `quote` generates a `TokenStream` from a template with `#variable` interpolation.
- `proc-macro2` is the token-stream type both libraries use so proc macros can be unit-tested outside the compiler's actual macro-expansion context.

## Derive macro

```rust
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(Describe)]
pub fn derive_describe(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let name = &ast.ident;

    let expanded = quote! {
        impl Describe for #name {
            fn describe() -> &'static str {
                stringify!(#name)
            }
        }
    };

    expanded.into()
}
```

Usage from another crate:

```rust
#[derive(Describe)]
struct User { name: String }

assert_eq!(User::describe(), "User");
```

## Attribute macro

Attribute macros receive both the attribute's arguments and the item they're attached to, and return a replacement for that item:

```rust
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

#[proc_macro_attribute]
pub fn log_call(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let name = &input.sig.ident;
    let block = &input.block;
    let sig = &input.sig;
    let vis = &input.vis;

    quote! {
        #vis #sig {
            println!("calling {}", stringify!(#name));
            #block
        }
    }
    .into()
}
```

```rust
#[log_call]
fn greet() {
    println!("hello");
}
```

## Function-like macro

```rust
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, LitStr};

#[proc_macro]
pub fn sql(input: TokenStream) -> TokenStream {
    let query = parse_macro_input!(input as LitStr).value();
    // Validate/transform `query` at compile time here.
    quote! { #query }.into()
}
```

## Structuring a proc-macro crate for testability

Keep the `#[proc_macro_derive]`/`#[proc_macro_attribute]` functions as thin wrappers that only do `TokenStream ↔ proc_macro2::TokenStream` conversion, and put the actual `syn`/`quote` logic in plain functions operating on `proc_macro2::TokenStream`/`syn` types — those can be unit-tested directly without going through the compiler's macro-expansion pipeline:

```rust
fn expand_describe(ast: syn::DeriveInput) -> proc_macro2::TokenStream {
    let name = &ast.ident;
    quote::quote! {
        impl Describe for #name {
            fn describe() -> &'static str { stringify!(#name) }
        }
    }
}

#[proc_macro_derive(Describe)]
pub fn derive_describe(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast = syn::parse_macro_input!(input as syn::DeriveInput);
    expand_describe(ast).into()
}

#[test]
fn expands_correctly() {
    let ast: syn::DeriveInput = syn::parse_quote! { struct User; };
    let tokens = expand_describe(ast).to_string();
    assert!(tokens.contains("impl Describe for User"));
}
```

## `build.rs`

A `build.rs` at the crate root runs before compilation and can generate source files, link native libraries, or set compile-time configuration via `cargo:` directives printed to stdout:

```rust
// build.rs
fn main() {
    println!("cargo:rerun-if-changed=src/schema.proto");
    println!("cargo:rustc-link-lib=static=mylib");

    let out_dir = std::env::var("OUT_DIR").unwrap();
    let dest = std::path::Path::new(&out_dir).join("generated.rs");
    std::fs::write(dest, "pub const GENERATED: bool = true;").unwrap();
}
```

Include the generated file from your crate with `include!(concat!(env!("OUT_DIR"), "/generated.rs"));`. Always emit `cargo:rerun-if-changed=<path>` for every input the script reads, or Cargo will re-run the script on every build unnecessarily (or, worse, fail to re-run it when the real input changes if you emit an incomplete/incorrect path).

## Stop conditions for this file

- The proc-macro crate's actual logic lives in testable functions over `proc_macro2`/`syn` types, not directly in the `#[proc_macro_derive]`/`#[proc_macro_attribute]` entry points.
- The macro's expansion was verified against a real call site (`cargo expand` or a unit test on the expansion function), not just "it compiles."
- Any `build.rs` script declares `cargo:rerun-if-changed` for every file it reads, so Cargo's rebuild detection stays correct.
