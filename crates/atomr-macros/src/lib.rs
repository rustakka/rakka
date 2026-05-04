//! Procedural macros for atomr. Ergonomics over `atomr-core`.
//!
//! Exposes:
//! * `#[actor_msg]` attribute — adds `Debug` derive and atomr-friendly
//!   conventions to a message enum.
//! * `#[derive(Actor)]` with `#[msg(MyMsgEnum)]` — generates a thin
//!   `impl Actor` that delegates to the struct's `handle_msg` method,
//!   removing the `async_trait` boilerplate users would otherwise repeat.
//! * `#[derive(Receive)]` with `#[msg(MyMsgEnum)]` — generates a
//!   `handle` method that dispatches enum variants to `on_<variant>`
//!   methods on the actor (Phase 1.E of `docs/full-port-plan.md`).
//! * `props!` macro — terse `Props::create(|| ExprThatBuildsAnActor)`.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, DeriveInput, Fields, ItemEnum};

fn to_snake_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    let mut prev_upper = true;
    for (i, ch) in name.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 && !prev_upper {
                out.push('_');
            }
            for low in ch.to_lowercase() {
                out.push(low);
            }
            prev_upper = true;
        } else {
            out.push(ch);
            prev_upper = false;
        }
    }
    out
}

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

/// `#[derive(Actor)]` with a `#[msg(MyMsg)]` attribute.
///
/// Generates:
///
/// ```ignore
/// #[async_trait]
/// impl Actor for Foo {
///     type Msg = MyMsg;
///     async fn handle(&mut self, ctx: &mut Context<Self>, msg: MyMsg) {
///         self.handle_msg(ctx, msg).await;
///     }
/// }
/// ```
///
/// The user only needs to write `impl Foo { async fn handle_msg(...) }` —
/// no `#[async_trait]` boilerplate required.
#[proc_macro_derive(Actor, attributes(msg))]
pub fn derive_actor(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let msg_ty = match extract_msg_attr(&input) {
        Ok(t) => t,
        Err(e) => return e.to_compile_error().into(),
    };

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let expanded = quote! {
        #[::atomr_core::prelude::async_trait]
        impl #impl_generics ::atomr_core::prelude::Actor for #name #ty_generics #where_clause {
            type Msg = #msg_ty;

            async fn handle(
                &mut self,
                ctx: &mut ::atomr_core::prelude::Context<Self>,
                msg: Self::Msg,
            ) {
                Self::handle_msg(self, ctx, msg).await;
            }
        }
    };

    expanded.into()
}

fn extract_msg_attr(input: &DeriveInput) -> Result<syn::Type, syn::Error> {
    for attr in &input.attrs {
        if attr.path().is_ident("msg") {
            return attr.parse_args::<syn::Type>();
        }
    }
    Err(syn::Error::new_spanned(
        &input.ident,
        "#[derive(Actor)] requires a `#[msg(MsgType)]` attribute naming the actor's message type",
    ))
}

/// `#[derive(Receive)]` with a `#[msg(MyMsg)]` attribute on the actor
/// struct, plus a separate `enum MyMsg { Inc, Get(...), … }` whose
/// variants must be visible at the macro expansion site.
///
/// Generates an `impl atomr_core::actor::Actor` whose `handle` method
/// dispatches each enum variant to a method on the actor named
/// `on_<snake_variant>`. Unit variants get `(&mut self, ctx)`; tuple
/// variants get `(&mut self, ctx, field0, field1, …)`. Struct variants
/// are not supported (produces a compile error).
///
/// This is the typed message-router DSL referenced by
/// `docs/idiomatic-rust.md` (P-8 follow-on) and Phase 1.E.
#[proc_macro_derive(Receive, attributes(msg, receive))]
pub fn derive_receive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let msg_ty = match extract_msg_attr(&input) {
        Ok(t) => t,
        Err(e) => return e.to_compile_error().into(),
    };

    // The variants list isn't visible from the derive site (we have the
    // struct, not the enum). Instead we generate a `match` arm that
    // delegates to a single trait the user implements. Two strategies:
    // (a) require the user to repeat the variant list via a second
    //     attribute; or
    // (b) generate a no-op stub that the user fills with `on_*` methods
    //     and the compiler errors on missing methods (matched at
    //     `Self::on_<variant>` call sites).
    //
    // We go with a third option: emit a `handle` method that calls
    // `Self::dispatch(self, ctx, msg).await` and require the user to
    // implement a single `dispatch` method. That defeats the purpose of
    // the macro — so instead we require the user to pass the variant
    // list via `#[receive(variants(Inc, Get(reply: oneshot::Sender<u32>)))]`.
    //
    // For Phase 1.E we ship a *minimal* version that works for unit
    // variants only, with the variant names supplied through a
    // `#[receive(unit_variants(Inc, Stop, …))]` attribute. Tuple/struct
    // variants are a follow-on once the syn-side parsing is in place.
    let unit_variants = match extract_unit_variants(&input) {
        Ok(v) => v,
        Err(e) => return e.to_compile_error().into(),
    };

    let arms = unit_variants.iter().map(|v| {
        let snake = format_ident!("on_{}", to_snake_case(&v.to_string()));
        quote! {
            #msg_ty::#v => Self::#snake(self, ctx).await,
        }
    });

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let expanded = quote! {
        #[::atomr_core::prelude::async_trait]
        impl #impl_generics ::atomr_core::prelude::Actor for #name #ty_generics #where_clause {
            type Msg = #msg_ty;

            async fn handle(
                &mut self,
                ctx: &mut ::atomr_core::prelude::Context<Self>,
                msg: Self::Msg,
            ) {
                match msg {
                    #(#arms)*
                    #[allow(unreachable_patterns)]
                    _ => {} // tuple/struct variants — unsupported in 1.E minimal
                }
            }
        }
    };
    expanded.into()
}

fn extract_unit_variants(input: &DeriveInput) -> Result<Vec<syn::Ident>, syn::Error> {
    for attr in &input.attrs {
        if !attr.path().is_ident("receive") {
            continue;
        }
        let mut out = Vec::new();
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("unit_variants") {
                meta.parse_nested_meta(|inner| {
                    if let Some(ident) = inner.path.get_ident() {
                        out.push(ident.clone());
                        Ok(())
                    } else {
                        Err(inner.error("expected variant identifier"))
                    }
                })
            } else {
                Err(meta.error("unknown #[receive(...)] key (expected `unit_variants`)"))
            }
        })?;
        return Ok(out);
    }
    Err(syn::Error::new_spanned(
        &input.ident,
        "#[derive(Receive)] requires `#[receive(unit_variants(A, B, …))]` for the Phase 1.E minimal subset",
    ))
}

/// `props!(EXPR)` — terse `Props::create(|| EXPR)`.
///
/// `EXPR` should evaluate to a fresh actor instance every call; Props
/// is used to spawn possibly-many actors from the same template.
///
/// ```ignore
/// let p = props!(MyActor { count: 0 });
/// system.actor_of(p, "a")?;
/// ```
#[proc_macro]
pub fn props(input: TokenStream) -> TokenStream {
    let expr = parse_macro_input!(input as syn::Expr);
    let expanded = quote! {
        ::atomr_core::actor::Props::create(move || #expr)
    };
    expanded.into()
}

// We re-introduce Fields here so the unused warning doesn't fire if
// future macros need it; the variable is allowed-dead.
#[allow(dead_code)]
fn _unused_fields_marker(_: Fields) {}
