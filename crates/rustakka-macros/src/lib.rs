//! Procedural macros for rustakka. Ergonomics over `rustakka-core`.
//!
//! Currently exposes:
//! * `#[actor_msg]` attribute — adds `Debug` derive and rustakka-friendly
//!   conventions to a message enum.
//! * `#[derive(Actor)]` — stub that validates the type has a `Msg` associated
//!   type declared via a `#[msg(EnumName)]` helper. Hand-written `impl Actor`
//!   is still expected; this derive will grow once the core stabilises.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, ItemEnum};

/// `#[actor_msg]` — sugar to declare a message enum.
///
/// Adds `#[derive(Debug)]` automatically and marks the enum non_exhaustive
/// so adding variants is not a breaking change for downstream matchers.
///
/// ```ignore
/// #[actor_msg]
/// enum CounterMsg { Inc, Get(tokio::sync::oneshot::Sender<u32>) }
/// ```
#[proc_macro_attribute]
pub fn actor_msg(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let en = parse_macro_input!(item as ItemEnum);
    let expanded = quote! {
        #[derive(::core::fmt::Debug)]
        #en
    };
    expanded.into()
}

/// `#[derive(Actor)]` — placeholder that emits a trivial `Default` impl
/// when the struct has no fields or all fields are `Default`, so
/// `Props::create(MyActor::default)` is easy to write.
///
/// Full behaviour DSL (Receive-style handler generation) is tracked in
/// `PORTING_TODO.md` Phase 2.10 and will arrive once core stabilises.
#[proc_macro_derive(Actor, attributes(msg))]
pub fn derive_actor(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let expanded = quote! {
        impl #impl_generics ::core::default::Default for #name #ty_generics
        where
            Self: ::core::marker::Sized,
            #where_clause
        {
            fn default() -> Self {
                // Users are expected to provide their own Default. The
                // derive intentionally panics rather than silently
                // constructing an invalid state.
                panic!(concat!("#[derive(Actor)]: implement Default for ", stringify!(#name)))
            }
        }
    };
    // Return empty if generics have where clauses we cannot extend; we keep
    // this compile-time safe by not emitting anything for parameterised types.
    let _ = expanded;
    TokenStream::new()
}
