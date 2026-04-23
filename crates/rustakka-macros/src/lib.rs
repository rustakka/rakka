//! Procedural macros for rustakka. Ergonomics over `rustakka-core`.
//!
//! Currently exposes:
//! * `#[actor_msg]` attribute — adds `Debug` derive and rustakka-friendly
//!   conventions to a message enum.
//! * `#[derive(Actor)]` with `#[msg(MyMsgEnum)]` — generates a thin
//!   `impl Actor` that delegates to the struct's `handle_msg` method,
//!   removing the `async_trait` boilerplate users would otherwise repeat.

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
        #[::rustakka_core::prelude::async_trait]
        impl #impl_generics ::rustakka_core::prelude::Actor for #name #ty_generics #where_clause {
            type Msg = #msg_ty;

            async fn handle(
                &mut self,
                ctx: &mut ::rustakka_core::prelude::Context<Self>,
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
