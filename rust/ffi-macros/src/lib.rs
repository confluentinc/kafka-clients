// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The `#[ffi_guard]` attribute: keeps a Rust panic from unwinding out of an
//! `extern "C"` entry point of the `confluent_kafka` C API.
//!
//! A panic that reaches an `extern "C"` frame aborts the whole process, so the
//! C or Python host dies instead of seeing an error. `#[ffi_guard]` rewrites
//!
//! ```text
//! #[ffi_guard]
//! #[unsafe(no_mangle)]
//! pub unsafe extern "C" fn kafka_x_X_new(config: *const C, out_new: *mut *mut kafka_x_X_t) -> *mut kafka_common_Error_t {
//!     BODY
//! }
//! ```
//!
//! into three functions:
//!
//! ```text
//! #[unsafe(no_mangle)]
//! pub unsafe extern "C" fn kafka_x_X_name(self_: *const kafka_x_X_t) -> *const c_char {
//!     crate::ffi::common::ffi_guard_or("kafka_x_X_name", ON_PANIC, move || __ffi_body_kafka_x_X_name(self_))
//! }
//!
//! fn __ffi_body_kafka_x_X_name(self_: *const kafka_x_X_t) -> *const c_char {
//!     BODY
//! }
//!
//! #[doc(hidden)]
//! #[unsafe(no_mangle)]
//! pub unsafe extern "C" fn kafka_x_X_name_err(
//!     self_: *const kafka_x_X_t,
//!     out_err: *mut *mut kafka_common_Error_t,
//! ) -> *const c_char {
//!     crate::ffi::common::ffi_guard_or(
//!         "kafka_x_X_name_err",
//!         move |err| { store_error(out_err, err); ON_PANIC_VALUE },
//!         move || __ffi_body_kafka_x_X_name(self_),
//!     )
//! }
//! ```
//!
//! `ffi_guard_or` runs the body under `std::panic::catch_unwind`. On a panic it
//! turns the payload into a `crate::common::Error` and hands it to the
//! `ON_PANIC` closure, whose value the entry point returns. The body is moved,
//! unchanged, into a private Rust-ABI function both exports call, so `return`
//! inside it still returns the entry point's value. That function is safe, as
//! the closure the body used to run in was a safe context: every unsafe
//! operation in it keeps its own `unsafe` block and `// SAFETY:` comment. Every
//! parameter of a C entry point is a pointer, a number, a by-value `repr(C)`
//! struct of those or a function pointer, so the `move` closure copies them.
//!
//! # Error channels
//!
//! An entry point has one of three, read from its signature and arguments:
//!
//! | entry point | on a panic | twin |
//! |---|---|---|
//! | fallible: its last parameter is `out_err: *mut *mut kafka_common_Error_t` | the panic error is stored in `*out_err` and the entry point returns `crate::ffi::common::FailValue::fail()`, exactly as on any other failure | none |
//! | `#[ffi_guard(on_panic = ...)]` (a `_cb`) | `on_panic` reports it through the completion | none |
//! | any other | the derived value below, or `fallback` | `<name>_err`, the same parameters plus a trailing `out_err` |
//!
//! `confluent_kafka_bindings.h`, which `build.rs` derives from the C header by
//! the same rule, declares the fallible and `_cb` entry points under their own
//! names and every other one only as its twin, so a language binding sees
//! every failure, a caught panic included, as an error.
//!
//! The twin is `unsafe`. It returns what the entry point returns; on a panic it
//! stores the panic error in `*out_err` and returns the entry point's on-panic
//! value, except that an error handle (an error factory's) is null there, the
//! panic being in `*out_err`. A twin of an entry point without `#[no_mangle]`
//! (a guarded function in a unit test) is a plain Rust function that the tests
//! call to reach the twin's panic path.
//!
//! A caught panic is an error of code `kafka_common_Error_LOCAL_ILLEGAL_STATE`
//! whose message begins `Rust panic caught at the FFI boundary in <function>`.
//! `fallback` and `on_panic` are compile errors on a fallible entry point: its
//! panic is a failure like any other.
//!
//! # The on-panic value
//!
//! Unless overridden, `ON_PANIC` of an entry point that is not fallible returns
//! a value derived from the return type
//! (decision D2 of `design/current/appsec-7665-4521-ffi-panic-guard.md`):
//!
//! | return type | on panic |
//! |---|---|
//! | `()` | nothing to return; the panic is only logged |
//! | `i8` | `0`, which an `int8_t` boolean reads as false. An `int8_t` that carries a Java `byte` code whose 0 is a valid constant (`IsolationLevel.READ_UNCOMMITTED`, `AlterConfigOp.OpType.SET`, `ElectionType.PREFERRED`, an `UNKNOWN`) declares `fallback = -1`, a value outside its domain |
//! | `bool` | `false` |
//! | `i16`, `i32`, `i64` | `-1`, or `0` when the function name ends in `_count` or `_size`, since C feeds a count or size to `malloc` and to a `for` bound. A size that already returns -1 for a null key or value (`serialized_key_size`) still returns `0` on a panic. An accessor mapping Java's empty `Optional`, or `null`, to -1 whose Java domain is that or a value >= 0 (a leader epoch) declares `fallback = -2` |
//! | `f64` | `f64::NAN` |
//! | `kafka_common_Error_e` | `kafka_common_Error_UNKNOWN_SERVER_ERROR` |
//! | `kafka_Bytes_t` | `kafka_Bytes_t::NULL`, Java's `null` array |
//! | `*mut kafka_common_Error_t` | a boxed error, because null means success |
//! | `*const kafka_common_Error_t` | a boxed error, leaked on purpose: the caller never frees a borrowed pointer, and null would read as "no error" |
//! | any other `*mut T` / `*const T` | null |
//! | any other C enum, `<Enum>_e` | a compile error naming the function: no enumerator means "failed", so the entry point names one with `fallback` |
//! | anything else | a compile error naming the function |
//!
//! # Overrides
//!
//! - `#[ffi_guard(fallback = <expr>)]` replaces the derived value.
//! - `#[ffi_guard(on_panic = |err| <expr>)]` takes full control. The closure
//!   receives the `crate::common::Error` and evaluates to the return type. A
//!   `_cb` entry point uses it to report the panic the way it reports every
//!   other outcome: it queues its completion, carrying the error, on the
//!   client's callback queue, exactly once, and `__execute_callbacks` runs it
//!   like any other completion.
//!
//! The two are mutually exclusive.
//!
//! # Hardcoded paths
//!
//! The expansion names `crate::ffi::common::ffi_guard_or`,
//! `crate::ffi::common::box_error`, `crate::ffi::common::kafka_common_Error_t`,
//! `crate::ffi::common::kafka_common_Error_e`,
//! `crate::ffi::util::kafka_Bytes_t` and `crate::common::Error`. That is
//! deliberate: the attribute exists only for the `confluent_kafka` crate's own
//! `ffi` module, where those items live, and a procedural macro has no other
//! way to name the crate that invokes it short of an extra argument at every
//! one of its call sites.
//!
//! # Placement
//!
//! Doc comments, then `#[ffi_guard...]`, then `#[unsafe(no_mangle)]`, so the
//! attribute sees and re-emits `no_mangle`, on the entry point and on its twin.
//! cbindgen reads the unexpanded source and ignores this attribute, so it sees
//! neither the body function nor the twin: `confluent_kafka.h` declares the
//! entry points alone, and `build.rs` writes the twins into
//! `confluent_kafka_bindings.h` itself. A unit test in `src/ffi/mod.rs` requires
//! every `#[unsafe(no_mangle)]` in `src/ffi/` to be preceded by this attribute.
//!
//! # Example
//!
//! The scaffold modules stand in for the items the expansion names:
//!
//! ```
//! # mod common { pub struct Error; }
//! # mod ffi {
//! #     pub mod common {
//! #         pub struct kafka_common_Error_t;
//! #         pub fn box_error(_: crate::common::Error) -> *mut kafka_common_Error_t {
//! #             Box::into_raw(Box::new(kafka_common_Error_t))
//! #         }
//! #         pub unsafe fn store_error(out: *mut *mut kafka_common_Error_t, e: crate::common::Error) {
//! #             unsafe { out.write(box_error(e)) }
//! #         }
//! #         pub fn ffi_guard_or<R>(
//! #             _: &'static str,
//! #             _: impl FnOnce(crate::common::Error) -> R,
//! #             body: impl FnOnce() -> R,
//! #         ) -> R {
//! #             body()
//! #         }
//! #     }
//! # }
//! use ffi_macros::ffi_guard;
//!
//! #[ffi_guard]
//! extern "C" fn answer() -> i32 {
//!     42
//! }
//! # fn main() {
//! assert_eq!(answer(), 42);
//! // The twin returns the value and leaves `out_err` alone.
//! let mut err = std::ptr::null_mut();
//! // SAFETY: `err` is valid for one write and null.
//! assert_eq!(unsafe { answer_err(&mut err) }, 42);
//! assert!(err.is_null());
//! # }
//! ```
//!
//! The same scaffold with a return type the table does not cover is rejected:
//!
//! ```compile_fail
//! # mod common { pub struct Error; }
//! # mod ffi {
//! #     pub mod common {
//! #         pub struct kafka_common_Error_t;
//! #         pub fn box_error(_: crate::common::Error) -> *mut kafka_common_Error_t {
//! #             Box::into_raw(Box::new(kafka_common_Error_t))
//! #         }
//! #         pub unsafe fn store_error(out: *mut *mut kafka_common_Error_t, e: crate::common::Error) {
//! #             unsafe { out.write(box_error(e)) }
//! #         }
//! #         pub fn ffi_guard_or<R>(
//! #             _: &'static str,
//! #             _: impl FnOnce(crate::common::Error) -> R,
//! #             body: impl FnOnce() -> R,
//! #         ) -> R {
//! #             body()
//! #         }
//! #     }
//! # }
//! use ffi_macros::ffi_guard;
//!
//! #[ffi_guard]
//! extern "C" fn answer() -> u8 {
//!     42
//! }
//! # fn main() {}
//! ```
//!
//! So is a C enum without a `fallback`:
//!
//! ```compile_fail
//! # mod common { pub struct Error; }
//! # mod ffi {
//! #     pub mod common {
//! #         pub struct kafka_common_Error_t;
//! #         pub fn box_error(_: crate::common::Error) -> *mut kafka_common_Error_t {
//! #             Box::into_raw(Box::new(kafka_common_Error_t))
//! #         }
//! #         pub unsafe fn store_error(out: *mut *mut kafka_common_Error_t, e: crate::common::Error) {
//! #             unsafe { out.write(box_error(e)) }
//! #         }
//! #         pub fn ffi_guard_or<R>(
//! #             _: &'static str,
//! #             _: impl FnOnce(crate::common::Error) -> R,
//! #             body: impl FnOnce() -> R,
//! #         ) -> R {
//! #             body()
//! #         }
//! #     }
//! # }
//! use ffi_macros::ffi_guard;
//!
//! #[derive(Clone, Copy)]
//! #[repr(C)]
//! pub enum kafka_x_Kind_e {
//!     kafka_x_Kind_A,
//! }
//!
//! #[ffi_guard]
//! extern "C" fn kafka_x_X__enum() -> kafka_x_Kind_e {
//!     kafka_x_Kind_e::kafka_x_Kind_A
//! }
//! # fn main() {}
//! ```

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{ToTokens, format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{AttrStyle, Expr, ExprClosure, FnArg, Ident, ItemFn, Pat, ReturnType, Signature, Token, Type};

/// Guards an `extern "C"` entry point against a Rust panic unwinding into its
/// caller; see the [crate documentation](crate) for the expansion, the derived
/// on-panic value and the `fallback` / `on_panic` overrides.
#[proc_macro_attribute]
pub fn ffi_guard(attr: TokenStream, item: TokenStream) -> TokenStream {
    let item = TokenStream2::from(item);
    match expand(attr.into(), item.clone()) {
        Ok(tokens) => tokens.into(),
        Err(error) => {
            // Keep the function itself, so one mistake yields one error rather
            // than a cascade of "cannot find function" at every caller.
            let mut tokens = error.to_compile_error();
            tokens.extend(item);
            tokens.into()
        },
    }
}

/// The arguments `#[ffi_guard(...)]` accepts.
#[derive(Default)]
struct GuardArgs {
    /// `fallback = <expr>`: the value to return on a panic.
    fallback: Option<Expr>,
    /// `on_panic = |err| <expr>`: the whole on-panic path.
    on_panic: Option<ExprClosure>,
}

impl Parse for GuardArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut args = GuardArgs::default();
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            if key == "fallback" {
                if args.fallback.is_some() {
                    return Err(syn::Error::new(key.span(), "duplicate `fallback` argument"));
                }
                args.fallback = Some(input.parse()?);
            } else if key == "on_panic" {
                if args.on_panic.is_some() {
                    return Err(syn::Error::new(key.span(), "duplicate `on_panic` argument"));
                }
                args.on_panic = Some(input.parse()?);
            } else {
                return Err(syn::Error::new(
                    key.span(),
                    format!(
                        "unknown `ffi_guard` argument `{key}`: expected `fallback = <expr>` or `on_panic = |err| <expr>`"
                    ),
                ));
            }
            if input.is_empty() {
                break;
            }
            input.parse::<Token![,]>()?;
        }
        if let (Some(_), Some(on_panic)) = (&args.fallback, &args.on_panic) {
            return Err(syn::Error::new_spanned(
                on_panic,
                "`fallback` and `on_panic` are mutually exclusive: `on_panic` already decides the return value",
            ));
        }
        Ok(args)
    }
}

/// The expansion of `#[ffi_guard(attr)] item`, or the error to report instead.
fn expand(attr: TokenStream2, item: TokenStream2) -> syn::Result<TokenStream2> {
    let args: GuardArgs = syn::parse2(attr)?;
    let function: ItemFn = syn::parse2(item)?;
    let name = function.sig.ident.to_string();
    let kind = if is_fallible(&function.sig) {
        if let Some(closure) = &args.on_panic {
            return Err(syn::Error::new_spanned(
                closure,
                format!("`{name}` reports a panic through `out_err`, so it takes no `on_panic`"),
            ));
        }
        if let Some(fallback) = &args.fallback {
            return Err(syn::Error::new_spanned(
                fallback,
                format!(
                    "`{name}` returns its failure value on a panic, as on any other failure, so it takes no \
                     `fallback`"
                ),
            ));
        }
        Kind::Fallible
    } else if args.on_panic.is_some() {
        Kind::OnPanic
    } else {
        Kind::Plain
    };
    let on_panic = match (&kind, args.on_panic) {
        (_, Some(closure)) => closure.into_token_stream(),
        (Kind::Fallible, None) => fallible_on_panic(&function.sig),
        (_, None) => generated_on_panic(&function.sig, args.fallback.clone())?,
    };
    // syn files a body's inner attributes (`#![...]`) under the function's
    // attributes; they have to go back inside the body.
    let (inner_attrs, outer_attrs): (Vec<_>, Vec<_>) = function
        .attrs
        .iter()
        .partition(|attr| matches!(attr.style, AttrStyle::Inner(_)));
    // A `#[cfg]` that removes the entry point removes its body and twin too.
    let cfg_attrs: Vec<_> = outer_attrs
        .iter()
        .filter(|attr| attr.path().is_ident("cfg") || attr.path().is_ident("cfg_attr"))
        .collect();
    let no_mangle = outer_attrs.iter().find(|attr| is_no_mangle(attr));

    let body_name = format_ident!("__ffi_body_{}", name);
    let vis = &function.vis;
    let params = forwarded_params(&function.sig)?;
    let call = quote!(#body_name(#(#params),*));

    // The body, written once and shared by the entry point and its twin. It is
    // a safe Rust-ABI function, as the closure it used to be was a safe
    // context: every unsafe operation in it keeps its own `unsafe` block.
    let mut body_sig = function.sig.clone();
    body_sig.ident = body_name.clone();
    body_sig.abi = None;
    body_sig.unsafety = None;
    // rustc exempts a `#[no_mangle]` function from `non_snake_case`, not the
    // body it no longer carries.
    let body_case = (!is_snake_case(&name)).then(|| quote!(#[expect(non_snake_case)]));
    // clippy exempts an `extern "C"` function from `too_many_arguments` (its
    // arity is the C API's), not its Rust-ABI body. 7 is clippy's default
    // `too-many-arguments-threshold`, which `clippy.toml` keeps.
    let body_arity = (params.len() > 7).then(|| quote!(#[expect(clippy::too_many_arguments)]));
    let stmts = &function.block.stmts;

    // The entry point and the twin take the parameters by value only, so a
    // `mut` binding stays on the body, which is where it is used.
    let mut sig = function.sig.clone();
    strip_bindings(&mut sig);

    let twin = match kind {
        Kind::Plain => Some(twin(&function.sig, &sig, args.fallback, &call, &cfg_attrs, no_mangle, vis)?),
        // A fallible entry point already reports everything through `out_err`,
        // and an `on_panic` one (a `_cb`) through its completion: both are
        // declared under their own name in `confluent_kafka_bindings.h`.
        Kind::Fallible | Kind::OnPanic => None,
    };

    Ok(quote! {
        #(#outer_attrs)*
        #vis #sig {
            crate::ffi::common::ffi_guard_or(#name, #on_panic, move || #call)
        }

        #(#cfg_attrs)*
        #body_case
        #body_arity
        #body_sig {
            #(#inner_attrs)*
            #(#stmts)*
        }

        #twin
    })
}

/// Which error channel an entry point has.
enum Kind {
    /// Its last parameter is `out_err: *mut *mut kafka_common_Error_t`: a panic
    /// is stored there, as every other failure is, and it has no twin.
    Fallible,
    /// `on_panic` reports a panic through the entry point's own channel (a
    /// `_cb` completion), so it has no twin.
    OnPanic,
    /// No error channel: its `_err` twin adds `out_err` for the panic.
    Plain,
}

/// Whether the last parameter of `sig` is `out_err: *mut *mut kafka_common_Error_t`.
fn is_fallible(sig: &Signature) -> bool {
    let Some(FnArg::Typed(last)) = sig.inputs.last() else {
        return false;
    };
    let named = matches!(last.pat.as_ref(), Pat::Ident(pat) if pat.ident == "out_err");
    let typed = match last.ty.as_ref() {
        Type::Ptr(outer) if outer.mutability.is_some() => {
            matches!(outer.elem.as_ref(), Type::Ptr(inner) if inner.mutability.is_some() && is_error_handle(&inner.elem))
        },
        _ => false,
    };
    named && typed
}

/// The on-panic closure of a fallible entry point: the panic goes to
/// `*out_err` and the entry point returns its failure value
/// (`crate::ffi::common::FailValue`), as on any other failure.
///
/// The generated `unsafe` block writes through `out_err`. It is sound because
/// the entry point's `# Safety` makes the caller pass an `out_err` valid for
/// one write; clippy does not check `// SAFETY:` comments in macro output,
/// hence this note here.
fn fallible_on_panic(sig: &Signature) -> TokenStream2 {
    let output = match &sig.output {
        ReturnType::Default => TokenStream2::new(),
        ReturnType::Type(arrow, ty) => quote!(#arrow #ty),
    };
    quote! {
        move |__ffi_guard_error: crate::common::Error| #output {
            unsafe { crate::ffi::common::fail_with(out_err, __ffi_guard_error) }
        }
    }
}

/// `<name>_err`, the twin of an entry point without an error channel: the same
/// parameters plus `out_err`, where a caught panic is stored.
///
/// The generated `unsafe` block writes through `out_err`. It is sound because
/// the twin is an `unsafe fn` whose `# Safety` section (`twin_doc`) makes the
/// caller pass an `out_err` valid for one write.
fn twin(
    original: &Signature,
    sig: &Signature,
    fallback: Option<Expr>,
    call: &TokenStream2,
    cfg_attrs: &[&&syn::Attribute],
    no_mangle: Option<&&syn::Attribute>,
    vis: &syn::Visibility,
) -> syn::Result<TokenStream2> {
    let name = original.ident.to_string();
    let twin_name = format_ident!("{}_err", name);
    let twin_str = twin_name.to_string();
    let value = twin_panic_value(original, fallback)?;
    let output = match &original.output {
        ReturnType::Default => TokenStream2::new(),
        ReturnType::Type(arrow, ty) => quote!(#arrow #ty),
    };
    let mut twin_sig = sig.clone();
    twin_sig.ident = twin_name.clone();
    twin_sig
        .inputs
        .push(syn::parse_quote!(out_err: *mut *mut crate::ffi::common::kafka_common_Error_t));
    twin_sig.unsafety = Some(Default::default());
    let twin_doc = twin_doc(&name);
    // A twin without `#[no_mangle]` (a guarded function in a unit test) is
    // reachable only from Rust; the anonymous constant keeps `dead_code` quiet
    // when no test calls it.
    let keep_alive = no_mangle
        .is_none()
        .then(|| quote!(#(#cfg_attrs)* const _: () = { let _ = #twin_name; };));
    Ok(quote! {
        #(#cfg_attrs)*
        #(#[doc = #twin_doc])*
        #[doc(hidden)]
        #no_mangle
        #vis #twin_sig {
            crate::ffi::common::ffi_guard_or(
                #twin_str,
                move |__ffi_guard_error: crate::common::Error| #output {
                    unsafe { crate::ffi::common::store_error(out_err, __ffi_guard_error) };
                    #value
                },
                move || #call
            )
        }

        #keep_alive
    })
}

/// What a twin returns beside the panic it stored: the entry point's own
/// on-panic value (`fallback`, or the derived one), except that an error
/// handle is null, the panic being in `*out_err`.
fn twin_panic_value(sig: &Signature, fallback: Option<Expr>) -> syn::Result<TokenStream2> {
    if let Some(expr) = fallback {
        return Ok(expr.into_token_stream());
    }
    if let ReturnType::Type(_, ty) = &sig.output
        && let Type::Ptr(ptr) = ty.as_ref()
        && is_error_handle(&ptr.elem)
    {
        return Ok(if ptr.mutability.is_some() {
            quote!(::core::ptr::null_mut())
        } else {
            quote!(::core::ptr::null())
        });
    }
    let unused = Ident::new("__ffi_guard_error", Span::call_site());
    Ok(derived_fallback(sig, &unused)?.0)
}

/// The `#[doc]` lines of `<name>_err`.
fn twin_doc(name: &str) -> Vec<String> {
    vec![
        format!(" The `_err` twin of [`{name}`], declared in `confluent_kafka_bindings.h`."),
        String::new(),
        " It returns what the entry point returns. On a caught panic it also stores the panic error in \
         `*out_err`, which it otherwise leaves untouched."
            .to_owned(),
        String::new(),
        " # Safety".to_owned(),
        String::new(),
        format!(
            " The caller upholds what [`{name}`] requires, and `out_err` is valid for one write and points \
             at null."
        ),
    ]
}

/// rustc's `non_snake_case` test: after trimming leading and trailing `_`, no
/// uppercase letter and no `__`. Mirrored exactly, so the `#[expect]` on the
/// body is always fulfilled.
fn is_snake_case(ident: &str) -> bool {
    let mut allow_underscore = true;
    ident.trim_matches('_').chars().all(|c| match c {
        '_' if !allow_underscore => false,
        '_' => {
            allow_underscore = false;
            true
        },
        c if !c.is_uppercase() => {
            allow_underscore = true;
            true
        },
        _ => false,
    })
}

/// Whether `attr` is `#[no_mangle]` or `#[unsafe(no_mangle)]`.
fn is_no_mangle(attr: &syn::Attribute) -> bool {
    match &attr.meta {
        syn::Meta::Path(path) => path.is_ident("no_mangle"),
        syn::Meta::List(list) => list.path.is_ident("unsafe") && list.tokens.to_string() == "no_mangle",
        syn::Meta::NameValue(_) => false,
    }
}

/// The parameter names, which the entry point and the twin pass to the body.
fn forwarded_params(sig: &Signature) -> syn::Result<Vec<Ident>> {
    sig.inputs
        .iter()
        .map(|input| match input {
            FnArg::Typed(typed) => match typed.pat.as_ref() {
                Pat::Ident(pat) if pat.subpat.is_none() => Ok(pat.ident.clone()),
                pat => Err(syn::Error::new_spanned(
                    pat,
                    format!(
                        "`#[ffi_guard]` forwards the parameters of `{}` by name: give this one a plain name",
                        sig.ident
                    ),
                )),
            },
            FnArg::Receiver(receiver) => Err(syn::Error::new_spanned(receiver, "`#[ffi_guard]` takes no `self`")),
        })
        .collect()
}

/// Drops `mut` and `ref` from `sig`'s parameter bindings.
fn strip_bindings(sig: &mut Signature) {
    for input in &mut sig.inputs {
        if let FnArg::Typed(typed) = input
            && let Pat::Ident(pat) = typed.pat.as_mut()
        {
            pat.mutability = None;
            pat.by_ref = None;
        }
    }
}

/// The on-panic closure derived from the signature (D2), with `fallback`
/// replacing the derived value when given.
fn generated_on_panic(sig: &Signature, fallback: Option<Expr>) -> syn::Result<TokenStream2> {
    let error = Ident::new("__ffi_guard_error", Span::call_site());
    let (value, value_uses_error) = match fallback {
        Some(expr) => (expr.into_token_stream(), false),
        None => derived_fallback(sig, &error)?,
    };
    let param = if value_uses_error { quote!(#error) } else { quote!(_) };
    let output = match &sig.output {
        ReturnType::Default => TokenStream2::new(),
        ReturnType::Type(arrow, ty) => quote!(#arrow #ty),
    };
    Ok(quote! {
        move |#param: crate::common::Error| #output { #value }
    })
}

/// The D2 value for `sig`'s return type, and whether it consumes `error`.
fn derived_fallback(sig: &Signature, error: &Ident) -> syn::Result<(TokenStream2, bool)> {
    let ty = match &sig.output {
        ReturnType::Default => return Ok((TokenStream2::new(), false)),
        ReturnType::Type(_, ty) => ty.as_ref(),
    };
    let name = sig.ident.to_string();
    let derived = match ty {
        Type::Tuple(tuple) if tuple.elems.is_empty() => Some((TokenStream2::new(), false)),
        // Null would tell the caller "no error", so an error-returning function
        // reports the panic as a real handle.
        Type::Ptr(ptr) if is_error_handle(&ptr.elem) => Some(if ptr.mutability.is_some() {
            (quote!(crate::ffi::common::box_error(#error)), true)
        } else {
            (quote!(crate::ffi::common::box_error(#error) as #ty), true)
        }),
        Type::Ptr(ptr) if ptr.mutability.is_some() => Some((quote!(::core::ptr::null_mut()), false)),
        Type::Ptr(_) => Some((quote!(::core::ptr::null()), false)),
        _ => path_ident(ty).and_then(|ident| {
            let value = if ident == "i8" {
                quote!(0)
            } else if ident == "bool" {
                quote!(false)
            } else if ident == "i16" || ident == "i32" || ident == "i64" {
                if name.ends_with("_count") || name.ends_with("_size") {
                    quote!(0)
                } else {
                    quote!(-1)
                }
            } else if ident == "f64" {
                quote!(::core::primitive::f64::NAN)
            } else if ident == "kafka_common_Error_e" {
                quote!(crate::ffi::common::kafka_common_Error_e::kafka_common_Error_UNKNOWN_SERVER_ERROR)
            } else if ident == "kafka_Bytes_t" {
                quote!(crate::ffi::util::kafka_Bytes_t::NULL)
            } else {
                return None;
            };
            Some((value, false))
        }),
    };
    derived.ok_or_else(|| {
        let ty_text = ty.to_token_stream();
        let message = if path_ident(ty).is_some_and(|ident| ident.to_string().ends_with("_e")) {
            format!(
                "`#[ffi_guard]` cannot derive a panic fallback for `{name}`, which returns the C enum `{ty_text}`: \
                 no enumerator means \"failed\", so name the one to return with \
                 `#[ffi_guard(fallback = <expr>)]`, or use `#[ffi_guard(on_panic = |err| <expr>)]`"
            )
        } else {
            format!(
                "`#[ffi_guard]` cannot derive a panic fallback for `{name}`, which returns `{ty_text}`: \
                 add `#[ffi_guard(fallback = <expr>)]` or `#[ffi_guard(on_panic = |err| <expr>)]`"
            )
        };
        syn::Error::new_spanned(ty, message)
    })
}

/// The last segment of a plain path type: `i32`, or `kafka_common_Error_t` in
/// `crate::ffi::common::kafka_common_Error_t`.
fn path_ident(ty: &Type) -> Option<&Ident> {
    match ty {
        Type::Path(path) if path.qself.is_none() => path
            .path
            .segments
            .last()
            .filter(|segment| segment.arguments.is_none())
            .map(|segment| &segment.ident),
        Type::Paren(paren) => path_ident(&paren.elem),
        Type::Group(group) => path_ident(&group.elem),
        _ => None,
    }
}

/// Whether `ty` names the opaque `kafka_common_Error_t` handle.
fn is_error_handle(ty: &Type) -> bool {
    path_ident(ty).is_some_and(|ident| ident == "kafka_common_Error_t")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The expansion of `#[ffi_guard(attr)] item` as a string, or the error message.
    fn expand_str(attr: TokenStream2, item: TokenStream2) -> Result<String, String> {
        expand(attr, item)
            .map(|tokens| tokens.to_string())
            .map_err(|error| error.to_string())
    }

    /// The generated on-panic closure of a guarded `item`, as a string.
    fn on_panic_of(item: TokenStream2) -> String {
        let function: ItemFn = syn::parse2(item).unwrap();
        generated_on_panic(&function.sig, None).unwrap().to_string()
    }

    /// The `#[doc]` lines of a twin, as tokens.
    fn doc_of(name: &str) -> TokenStream2 {
        let lines = twin_doc(name);
        quote!(#(#[doc = #lines])*)
    }

    #[test]
    fn test_expansion_wraps_body_and_keeps_attributes_in_order() {
        let expanded = expand_str(
            TokenStream2::new(),
            quote! {
                /// Docs.
                #[unsafe(no_mangle)]
                pub unsafe extern "C" fn kafka_x_get(handle: *const u8) -> i32 { 7 }
            },
        )
        .unwrap();
        let doc = doc_of("kafka_x_get");
        let expected = quote! {
            #[doc = r" Docs."]
            #[unsafe(no_mangle)]
            pub unsafe extern "C" fn kafka_x_get(handle: *const u8) -> i32 {
                crate::ffi::common::ffi_guard_or(
                    "kafka_x_get",
                    move |_: crate::common::Error| -> i32 { -1 },
                    move || __ffi_body_kafka_x_get(handle)
                )
            }

            fn __ffi_body_kafka_x_get(handle: *const u8) -> i32 { 7 }

            #doc
            #[doc(hidden)]
            #[unsafe(no_mangle)]
            pub unsafe extern "C" fn kafka_x_get_err(
                handle: *const u8,
                out_err: *mut *mut crate::ffi::common::kafka_common_Error_t
            ) -> i32 {
                crate::ffi::common::ffi_guard_or(
                    "kafka_x_get_err",
                    move |__ffi_guard_error: crate::common::Error| -> i32 {
                        unsafe { crate::ffi::common::store_error(out_err, __ffi_guard_error) };
                        -1
                    },
                    move || __ffi_body_kafka_x_get(handle)
                )
            }
        };
        assert_eq!(expanded, expected.to_string());
    }

    #[test]
    fn test_inner_attributes_move_into_the_new_body() {
        let expanded = expand_str(
            TokenStream2::new(),
            quote! {
                extern "C" fn kafka_x_run() {
                    #![allow(unused_variables)]
                    let x = 1;
                }
            },
        )
        .unwrap();
        let doc = doc_of("kafka_x_run");
        // Without `#[no_mangle]` the twin is a Rust function only, kept alive
        // for `dead_code` by the anonymous constant.
        let expected = quote! {
            extern "C" fn kafka_x_run() {
                crate::ffi::common::ffi_guard_or(
                    "kafka_x_run",
                    move |_: crate::common::Error| {},
                    move || __ffi_body_kafka_x_run()
                )
            }

            fn __ffi_body_kafka_x_run() {
                #![allow(unused_variables)]
                let x = 1;
            }

            #doc
            #[doc(hidden)]
            unsafe extern "C" fn kafka_x_run_err(out_err: *mut *mut crate::ffi::common::kafka_common_Error_t) {
                crate::ffi::common::ffi_guard_or(
                    "kafka_x_run_err",
                    move |__ffi_guard_error: crate::common::Error| {
                        unsafe { crate::ffi::common::store_error(out_err, __ffi_guard_error) };
                    },
                    move || __ffi_body_kafka_x_run()
                )
            }

            const _: () = { let _ = kafka_x_run_err; };
        };
        assert_eq!(expanded, expected.to_string());
    }

    #[test]
    fn test_fallible_entry_point_stores_the_panic_and_has_no_twin() {
        let expanded = expand_str(
            TokenStream2::new(),
            quote! {
                #[unsafe(no_mangle)]
                pub unsafe extern "C" fn kafka_x_X_new(
                    config: *const u8,
                    out_err: *mut *mut kafka_common_Error_t,
                ) -> *mut kafka_x_X_t { body() }
            },
        )
        .unwrap();
        let expected = quote! {
            #[unsafe(no_mangle)]
            pub unsafe extern "C" fn kafka_x_X_new(
                config: *const u8,
                out_err: *mut *mut kafka_common_Error_t,
            ) -> *mut kafka_x_X_t {
                crate::ffi::common::ffi_guard_or(
                    "kafka_x_X_new",
                    move |__ffi_guard_error: crate::common::Error| -> *mut kafka_x_X_t {
                        unsafe { crate::ffi::common::fail_with(out_err, __ffi_guard_error) }
                    },
                    move || __ffi_body_kafka_x_X_new(config, out_err)
                )
            }

            #[expect(non_snake_case)]
            fn __ffi_body_kafka_x_X_new(
                config: *const u8,
                out_err: *mut *mut kafka_common_Error_t,
            ) -> *mut kafka_x_X_t { body() }
        };
        assert_eq!(expanded, expected.to_string());
        // A fallible `void` entry point has the same shape.
        let void = expand_str(
            TokenStream2::new(),
            quote!(
                extern "C" fn kafka_x_close(out_err: *mut *mut crate::ffi::common::kafka_common_Error_t) {}
            ),
        )
        .unwrap();
        assert!(void.contains("move | __ffi_guard_error : crate :: common :: Error | { unsafe { crate :: ffi :: common :: fail_with (out_err , __ffi_guard_error) } }"), "{void}");
        assert!(!void.contains("kafka_x_close_err"), "{void}");
    }

    #[test]
    fn test_only_a_mutable_error_slot_named_out_err_is_fallible() {
        for item in [
            quote!(
                extern "C" fn f(out_error: *mut *mut kafka_common_Error_t) -> i32 {}
            ),
            quote!(
                extern "C" fn f(out_err: *mut *const kafka_common_Error_t) -> i32 {}
            ),
            quote!(
                extern "C" fn f(out_err: *mut *mut kafka_x_t) -> i32 {}
            ),
            quote!(
                extern "C" fn f(out_err: *mut *mut kafka_common_Error_t, n: i32) -> i32 {}
            ),
        ] {
            let expanded = expand_str(TokenStream2::new(), item.clone()).unwrap();
            assert!(expanded.contains("fn f_err"), "for {item}: {expanded}");
        }
    }

    #[test]
    fn test_fallible_entry_point_takes_no_override() {
        let item = quote!(
            extern "C" fn kafka_x_get(out_err: *mut *mut kafka_common_Error_t) -> i32 {}
        );
        assert_eq!(
            expand_str(quote!(fallback = 0), item.clone()).unwrap_err(),
            "`kafka_x_get` returns its failure value on a panic, as on any other failure, so it takes no `fallback`"
        );
        assert_eq!(
            expand_str(quote!(on_panic = |err| 0), item).unwrap_err(),
            "`kafka_x_get` reports a panic through `out_err`, so it takes no `on_panic`"
        );
    }

    #[test]
    fn test_error_factory_twin_returns_null_beside_the_panic() {
        let expanded = expand_str(
            TokenStream2::new(),
            quote! {
                #[unsafe(no_mangle)]
                pub extern "C" fn kafka_common_Error_wakeup() -> *mut kafka_common_Error_t { body() }
            },
        )
        .unwrap();
        // The entry point reports a panic as a real handle: null would read as
        // "no error". Its twin has `out_err` for that, so it returns null.
        assert!(
            expanded.contains(
                &quote!(move |__ffi_guard_error: crate::common::Error| -> *mut kafka_common_Error_t {
                    crate::ffi::common::box_error(__ffi_guard_error)
                })
                .to_string()
            ),
            "{expanded}"
        );
        assert!(
            expanded.contains(
                &quote!(move |__ffi_guard_error: crate::common::Error| -> *mut kafka_common_Error_t {
                    unsafe { crate::ffi::common::store_error(out_err, __ffi_guard_error) };
                    ::core::ptr::null_mut()
                })
                .to_string()
            ),
            "{expanded}"
        );
    }

    #[test]
    fn test_cfg_attributes_follow_the_body_and_the_twin() {
        let expanded = expand_str(
            TokenStream2::new(),
            quote! {
                #[cfg(test)]
                #[unsafe(no_mangle)]
                extern "C" fn f() {}
            },
        )
        .unwrap();
        // The entry point, its body and its twin each carry it.
        assert_eq!(expanded.matches("# [cfg (test)]").count(), 3, "{expanded}");
        // `no_mangle` is the entry point's and the twin's only.
        assert_eq!(expanded.matches("no_mangle").count(), 2, "{expanded}");
    }

    #[test]
    fn test_parameter_bindings_stay_on_the_body() {
        let expanded = expand_str(
            TokenStream2::new(),
            quote! {
                extern "C" fn f(mut n: i32, r#type: i32) {}
            },
        )
        .unwrap();
        assert!(expanded.contains("extern \"C\" fn f (n : i32 , r#type : i32)"), "{expanded}");
        assert!(expanded.contains("fn __ffi_body_f (mut n : i32 , r#type : i32)"), "{expanded}");
        assert!(
            expanded.contains(
                "unsafe extern \"C\" fn f_err (n : i32 , r#type : i32 , out_err : * mut * mut crate :: ffi :: common :: \
                 kafka_common_Error_t)"
            ),
            "{expanded}"
        );
        assert!(expanded.contains("__ffi_body_f (n , r#type)"), "{expanded}");
    }

    #[test]
    fn test_body_lints_mirror_the_exemptions_of_the_entry_point() {
        // rustc's `non_snake_case` and clippy's `too_many_arguments` exempt the
        // `#[no_mangle] extern "C"` entry point, not its Rust-ABI body.
        let eight = quote!(
            extern "C" fn f(a: i8, b: i8, c: i8, d: i8, e: i8, f: i8, g: i8, h: i8) {}
        );
        let seven = quote!(
            extern "C" fn f(a: i8, b: i8, c: i8, d: i8, e: i8, f: i8, g: i8) {}
        );
        let lint = "# [expect (clippy :: too_many_arguments)]";
        assert_eq!(expand_str(TokenStream2::new(), eight).unwrap().matches(lint).count(), 1);
        assert_eq!(expand_str(TokenStream2::new(), seven).unwrap().matches(lint).count(), 0);
        let lint = "# [expect (non_snake_case)]";
        for (name, expected) in [
            ("kafka_x_X_new", 1),
            ("kafka_x_y_new", 0),
            ("kafka_x_Y__enum", 1),
            ("kafka_x_y__enum", 1),
            ("__kafka_x_", 0),
        ] {
            let ident = Ident::new(name, Span::call_site());
            let expanded = expand_str(TokenStream2::new(), quote!(extern "C" fn #ident() {})).unwrap();
            assert_eq!(expanded.matches(lint).count(), expected, "for {name}");
        }
    }

    #[test]
    fn test_parameters_need_a_plain_name() {
        assert_eq!(
            expand_str(
                TokenStream2::new(),
                quote!(
                    extern "C" fn kafka_x_pair((a, b): (i32, i32)) {}
                )
            )
            .unwrap_err(),
            "`#[ffi_guard]` forwards the parameters of `kafka_x_pair` by name: give this one a plain name"
        );
    }

    #[test]
    fn test_derived_values_follow_the_return_type() {
        let cases = [
            (
                quote!(
                    extern "C" fn f() {}
                ),
                quote!(move |_: crate::common::Error| {}),
            ),
            (
                quote!(
                    extern "C" fn f() -> () {}
                ),
                quote!(move |_: crate::common::Error| -> () {}),
            ),
            (
                quote!(
                    extern "C" fn f() -> i8 {}
                ),
                quote!(move |_: crate::common::Error| -> i8 { 0 }),
            ),
            (
                quote!(
                    extern "C" fn f() -> bool {}
                ),
                quote!(move |_: crate::common::Error| -> bool { false }),
            ),
            (
                quote!(
                    extern "C" fn f() -> i16 {}
                ),
                quote!(move |_: crate::common::Error| -> i16 { -1 }),
            ),
            (
                quote!(
                    extern "C" fn f() -> i32 {}
                ),
                quote!(move |_: crate::common::Error| -> i32 { -1 }),
            ),
            (
                quote!(
                    extern "C" fn f() -> i64 {}
                ),
                quote!(move |_: crate::common::Error| -> i64 { -1 }),
            ),
            (
                quote!(
                    extern "C" fn f() -> f64 {}
                ),
                quote!(move |_: crate::common::Error| -> f64 { ::core::primitive::f64::NAN }),
            ),
            (
                quote!(
                    extern "C" fn f() -> kafka_common_Error_e {}
                ),
                quote!(move |_: crate::common::Error| -> kafka_common_Error_e {
                    crate::ffi::common::kafka_common_Error_e::kafka_common_Error_UNKNOWN_SERVER_ERROR
                }),
            ),
            (
                quote!(
                    extern "C" fn f() -> crate::ffi::common::kafka_common_Error_e {}
                ),
                quote!(move |_: crate::common::Error| -> crate::ffi::common::kafka_common_Error_e {
                    crate::ffi::common::kafka_common_Error_e::kafka_common_Error_UNKNOWN_SERVER_ERROR
                }),
            ),
            (
                quote!(
                    extern "C" fn f() -> kafka_Bytes_t {}
                ),
                quote!(move |_: crate::common::Error| -> kafka_Bytes_t { crate::ffi::util::kafka_Bytes_t::NULL }),
            ),
            (
                quote!(
                    extern "C" fn f() -> *mut kafka_common_Error_t {}
                ),
                quote!(move |__ffi_guard_error: crate::common::Error| -> *mut kafka_common_Error_t {
                    crate::ffi::common::box_error(__ffi_guard_error)
                }),
            ),
            (
                quote!(
                    extern "C" fn f() -> *const crate::ffi::common::kafka_common_Error_t {}
                ),
                quote!(
                    move |__ffi_guard_error: crate::common::Error| -> *const crate::ffi::common::kafka_common_Error_t {
                        crate::ffi::common::box_error(__ffi_guard_error)
                            as *const crate::ffi::common::kafka_common_Error_t
                    }
                ),
            ),
            (
                quote!(
                    extern "C" fn f() -> *mut kafka_x_t {}
                ),
                quote!(move |_: crate::common::Error| -> *mut kafka_x_t { ::core::ptr::null_mut() }),
            ),
            (
                quote!(
                    extern "C" fn f() -> *const c_char {}
                ),
                quote!(move |_: crate::common::Error| -> *const c_char { ::core::ptr::null() }),
            ),
        ];
        for (item, expected) in cases {
            assert_eq!(on_panic_of(item.clone()), expected.to_string(), "for {item}");
        }
    }

    #[test]
    fn test_count_and_size_suffixes_select_zero_for_every_integer_width() {
        for suffix in ["count", "size"] {
            let name = Ident::new(&format!("kafka_x_topics_{suffix}"), Span::call_site());
            let misplaced = Ident::new(&format!("kafka_x_{suffix}_get"), Span::call_site());
            for ty in [quote!(i16), quote!(i32), quote!(i64)] {
                assert_eq!(
                    on_panic_of(quote!(extern "C" fn #name() -> #ty {})),
                    quote!(move |_: crate::common::Error| -> #ty { 0 }).to_string(),
                    "for {name} -> {ty}"
                );
                // Only a suffix counts: the word elsewhere in the name does not.
                assert_eq!(
                    on_panic_of(quote!(extern "C" fn #misplaced() -> #ty {})),
                    quote!(move |_: crate::common::Error| -> #ty { -1 }).to_string(),
                    "for {misplaced} -> {ty}"
                );
            }
        }
        // `kafka_List_size`, the shape the suffix exists for.
        assert_eq!(
            on_panic_of(quote!(
                extern "C" fn kafka_List_size(list: *const kafka_List_t) -> i32 {}
            )),
            quote!(move |_: crate::common::Error| -> i32 { 0 }).to_string()
        );
    }

    /// The derived default only: a Java `byte` code whose 0 is valid declares
    /// `fallback = -1` (`test_explicit_i8_fallback_overrides_the_zero_default`).
    #[test]
    fn test_i8_is_zero_whatever_the_name() {
        for name in ["kafka_x_X_is_closed", "kafka_x_X_topics_count", "kafka_x_X_byte"] {
            let name = Ident::new(name, Span::call_site());
            assert_eq!(
                on_panic_of(quote!(extern "C" fn #name() -> i8 {})),
                quote!(move |_: crate::common::Error| -> i8 { 0 }).to_string(),
                "for {name}"
            );
        }
    }

    #[test]
    fn test_explicit_i8_fallback_overrides_the_zero_default() {
        // An `int8_t` carrying a Java `byte` code whose 0 is a valid constant
        // (`IsolationLevel.READ_UNCOMMITTED`) declares a value outside its domain.
        let expanded = expand_str(
            quote!(fallback = -1),
            quote!(
                extern "C" fn kafka_common_IsolationLevel_id(self_: *const X) -> i8 {
                    1
                }
            ),
        )
        .unwrap();
        let expected = quote! {
            extern "C" fn kafka_common_IsolationLevel_id(self_: *const X) -> i8 {
                crate::ffi::common::ffi_guard_or(
                    "kafka_common_IsolationLevel_id",
                    move |_: crate::common::Error| -> i8 { -1 },
                    move || __ffi_body_kafka_common_IsolationLevel_id(self_)
                )
            }
        };
        assert!(expanded.starts_with(&expected.to_string()), "{expanded}");
    }

    #[test]
    fn test_out_parameters_are_never_written() {
        // An `out_<name>` slot, an `out_error`-named one included, is left alone:
        // the error is the return value.
        assert_eq!(
            on_panic_of(quote! {
                extern "C" fn kafka_x_X_new(
                    config: *const u8,
                    out_new: *mut *mut kafka_x_X_t,
                ) -> *mut kafka_common_Error_t {}
            }),
            quote! {
                move |__ffi_guard_error: crate::common::Error| -> *mut kafka_common_Error_t {
                    crate::ffi::common::box_error(__ffi_guard_error)
                }
            }
            .to_string()
        );
        assert_eq!(
            on_panic_of(quote!(
                extern "C" fn f(out_error: *mut i32) -> i32 {}
            )),
            quote!(move |_: crate::common::Error| -> i32 { -1 }).to_string()
        );
    }

    #[test]
    fn test_fallback_replaces_the_value() {
        let expanded = expand_str(
            quote!(fallback = 42),
            quote!(
                extern "C" fn f(out_value: *mut u8) -> u8 {
                    1
                }
            ),
        )
        .unwrap();
        let doc = doc_of("f");
        // The twin stores the panic and returns `fallback` too.
        let expected = quote! {
            extern "C" fn f(out_value: *mut u8) -> u8 {
                crate::ffi::common::ffi_guard_or(
                    "f",
                    move |_: crate::common::Error| -> u8 { 42 },
                    move || __ffi_body_f(out_value)
                )
            }

            fn __ffi_body_f(out_value: *mut u8) -> u8 { 1 }

            #doc
            #[doc(hidden)]
            unsafe extern "C" fn f_err(
                out_value: *mut u8,
                out_err: *mut *mut crate::ffi::common::kafka_common_Error_t
            ) -> u8 {
                crate::ffi::common::ffi_guard_or(
                    "f_err",
                    move |__ffi_guard_error: crate::common::Error| -> u8 {
                        unsafe { crate::ffi::common::store_error(out_err, __ffi_guard_error) };
                        42
                    },
                    move || __ffi_body_f(out_value)
                )
            }

            const _: () = { let _ = f_err; };
        };
        assert_eq!(expanded, expected.to_string());
    }

    #[test]
    fn test_on_panic_closure_is_emitted_verbatim_and_has_no_twin() {
        let expanded = expand_str(
            quote!(on_panic = |err| deliver(cb, opaque, Err(err))),
            quote!(
                extern "C" fn f(self_: *const X, cb: Cb, opaque: *mut c_void) {}
            ),
        )
        .unwrap();
        // The completion is the error channel, panic included.
        let expected = quote! {
            extern "C" fn f(self_: *const X, cb: Cb, opaque: *mut c_void) {
                crate::ffi::common::ffi_guard_or(
                    "f",
                    |err| deliver(cb, opaque, Err(err)),
                    move || __ffi_body_f(self_, cb, opaque)
                )
            }

            fn __ffi_body_f(self_: *const X, cb: Cb, opaque: *mut c_void) {}
        };
        assert_eq!(expanded, expected.to_string());
    }

    #[test]
    fn test_is_snake_case_mirrors_rustc() {
        for name in ["f", "kafka_x_y", "_kafka_", "__kafka__", "a1_b2"] {
            assert!(is_snake_case(name), "{name}");
        }
        for name in ["kafka_x_Y", "kafka_x__enum", "A", "a__b_"] {
            assert!(!is_snake_case(name), "{name}");
        }
    }

    #[test]
    fn test_unsupported_return_type_names_the_function() {
        assert_eq!(
            expand_str(
                TokenStream2::new(),
                quote!(
                    extern "C" fn kafka_x_size() -> usize {
                        0
                    }
                )
            )
            .unwrap_err(),
            "`#[ffi_guard]` cannot derive a panic fallback for `kafka_x_size`, which returns `usize`: \
             add `#[ffi_guard(fallback = <expr>)]` or `#[ffi_guard(on_panic = |err| <expr>)]`"
        );
        // A fallback makes any return type acceptable.
        assert!(
            expand_str(
                quote!(fallback = 0),
                quote!(
                    extern "C" fn kafka_x_size() -> usize {
                        0
                    }
                )
            )
            .is_ok()
        );
    }

    #[test]
    fn test_c_enum_requires_an_explicit_fallback() {
        let item = quote!(
            extern "C" fn kafka_common_IsolationLevel__enum(
                self_: *const kafka_common_IsolationLevel_t,
            ) -> kafka_common_IsolationLevel_e {
                kafka_common_IsolationLevel_e::kafka_common_IsolationLevel_READ_COMMITTED
            }
        );
        assert_eq!(
            expand_str(TokenStream2::new(), item.clone()).unwrap_err(),
            "`#[ffi_guard]` cannot derive a panic fallback for `kafka_common_IsolationLevel__enum`, which returns \
             the C enum `kafka_common_IsolationLevel_e`: no enumerator means \"failed\", so name the one to return \
             with `#[ffi_guard(fallback = <expr>)]`, or use `#[ffi_guard(on_panic = |err| <expr>)]`"
        );
        // A path to the enum is recognised the same way.
        assert!(
            expand_str(
                TokenStream2::new(),
                quote!(
                    extern "C" fn f() -> crate::ffi::common::isolation_level::kafka_common_IsolationLevel_e {}
                )
            )
            .unwrap_err()
            .contains("which returns the C enum")
        );
        // Naming the enumerator is accepted.
        assert!(
            expand_str(
                quote!(fallback = kafka_common_IsolationLevel_e::kafka_common_IsolationLevel_READ_UNCOMMITTED),
                item
            )
            .is_ok()
        );
    }

    #[test]
    fn test_argument_errors() {
        let item = quote!(
            extern "C" fn f() -> i32 {
                0
            }
        );
        assert_eq!(
            expand_str(quote!(fallback = 1, on_panic = |err| 2), item.clone()).unwrap_err(),
            "`fallback` and `on_panic` are mutually exclusive: `on_panic` already decides the return value"
        );
        assert_eq!(
            expand_str(quote!(fallback = 1, fallback = 2), item.clone()).unwrap_err(),
            "duplicate `fallback` argument"
        );
        assert_eq!(
            expand_str(quote!(on_panic = |err| 1, on_panic = |err| 2), item.clone()).unwrap_err(),
            "duplicate `on_panic` argument"
        );
        assert_eq!(
            expand_str(quote!(default = 1), item.clone()).unwrap_err(),
            "unknown `ffi_guard` argument `default`: expected `fallback = <expr>` or `on_panic = |err| <expr>`"
        );
        // A trailing comma is accepted.
        assert!(expand_str(quote!(fallback = 1,), item).is_ok());
    }
}
