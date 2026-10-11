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
//! into
//!
//! ```text
//! #[unsafe(no_mangle)]
//! pub unsafe extern "C" fn kafka_x_X_new(config: *const C, out_new: *mut *mut kafka_x_X_t) -> *mut kafka_common_Error_t {
//!     crate::ffi::common::ffi_guard_or("kafka_x_X_new", ON_PANIC, move || { BODY })
//! }
//! ```
//!
//! `ffi_guard_or` runs the body under `std::panic::catch_unwind`. On a panic it
//! turns the payload into a `crate::common::Error` and hands it to the
//! `ON_PANIC` closure, whose value the entry point returns. The body itself is
//! not touched: every parameter of a C entry point is a pointer, a number, a
//! by-value `repr(C)` struct of those or a function pointer, so the `move`
//! closure copies them and `return` inside the body still returns the entry
//! point's value.
//!
//! The error is reported through the return value only. A fallible entry point
//! returns `kafka_common_Error_t *` (null on success), so a caught panic is that
//! returned error: code `kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE`, message
//! beginning `Rust panic caught at the FFI boundary in <function>`. Its
//! `out_<name>` parameters are left unwritten, the body having stopped before
//! it delivered a value; the C caller reads them only when the call returned
//! null.
//!
//! # The on-panic value
//!
//! Unless overridden, `ON_PANIC` returns a value derived from the return type
//! (decision D2 of `design/current/appsec-7665-4521-ffi-panic-guard.md`):
//!
//! | return type | on panic |
//! |---|---|
//! | `()` | nothing to return; the panic is only logged |
//! | `i8` | `0`: an `int8_t` boolean reads it as false |
//! | `bool` | `false` |
//! | `i16`, `i32`, `i64` | `-1`, or `0` when the function name ends in `_count` or `_size` (a count or size is never negative: C feeds it to `malloc` and to a `for` bound) |
//! | `f64` | `f64::NAN` |
//! | `kafka_common_ErrorCode_e` | `kafka_common_ErrorCode_UNKNOWN_SERVER_ERROR` |
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
//! `crate::ffi::common::box_error`, `crate::ffi::common::kafka_common_ErrorCode_e`,
//! `crate::ffi::util::kafka_Bytes_t` and `crate::common::Error`. That is
//! deliberate: the attribute exists only for the `confluent_kafka` crate's own
//! `ffi` module, where those items live, and a procedural macro has no other
//! way to name the crate that invokes it short of an extra argument at every
//! one of its call sites.
//!
//! # Placement
//!
//! Doc comments, then `#[ffi_guard...]`, then `#[unsafe(no_mangle)]`, so the
//! attribute sees and re-emits `no_mangle`. cbindgen reads the unexpanded
//! source and ignores this attribute, which is why the generated C header does
//! not change; a unit test in `src/ffi/mod.rs` requires every
//! `#[unsafe(no_mangle)]` in `src/ffi/` to be preceded by it.
//!
//! # Example
//!
//! The scaffold modules stand in for the items the expansion names:
//!
//! ```
//! # mod common { pub struct Error; }
//! # mod ffi {
//! #     pub mod common {
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
//! # }
//! ```
//!
//! The same scaffold with a return type the table does not cover is rejected:
//!
//! ```compile_fail
//! # mod common { pub struct Error; }
//! # mod ffi {
//! #     pub mod common {
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
use quote::{ToTokens, quote};
use syn::parse::{Parse, ParseStream};
use syn::{AttrStyle, Expr, ExprClosure, Ident, ItemFn, ReturnType, Signature, Token, Type};

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
    let on_panic = match args.on_panic {
        Some(closure) => closure.into_token_stream(),
        None => generated_on_panic(&function.sig, args.fallback)?,
    };
    // syn files a body's inner attributes (`#![...]`) under the function's
    // attributes; they have to go back inside the new body.
    let (inner_attrs, outer_attrs): (Vec<_>, Vec<_>) = function
        .attrs
        .iter()
        .partition(|attr| matches!(attr.style, AttrStyle::Inner(_)));
    let name = function.sig.ident.to_string();
    let vis = &function.vis;
    let sig = &function.sig;
    let block = &function.block;
    Ok(quote! {
        #(#outer_attrs)*
        #vis #sig {
            #(#inner_attrs)*
            crate::ffi::common::ffi_guard_or(#name, #on_panic, move || #block)
        }
    })
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
            } else if ident == "kafka_common_ErrorCode_e" {
                quote!(crate::ffi::common::kafka_common_ErrorCode_e::kafka_common_ErrorCode_UNKNOWN_SERVER_ERROR)
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
        let expected = quote! {
            #[doc = r" Docs."]
            #[unsafe(no_mangle)]
            pub unsafe extern "C" fn kafka_x_get(handle: *const u8) -> i32 {
                crate::ffi::common::ffi_guard_or(
                    "kafka_x_get",
                    move |_: crate::common::Error| -> i32 { -1 },
                    move || { 7 }
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
        let expected = quote! {
            extern "C" fn kafka_x_run() {
                #![allow(unused_variables)]
                crate::ffi::common::ffi_guard_or(
                    "kafka_x_run",
                    move |_: crate::common::Error| {},
                    move || { let x = 1; }
                )
            }
        };
        assert_eq!(expanded, expected.to_string());
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
                    extern "C" fn f() -> kafka_common_ErrorCode_e {}
                ),
                quote!(move |_: crate::common::Error| -> kafka_common_ErrorCode_e {
                    crate::ffi::common::kafka_common_ErrorCode_e::kafka_common_ErrorCode_UNKNOWN_SERVER_ERROR
                }),
            ),
            (
                quote!(
                    extern "C" fn f() -> crate::ffi::common::kafka_common_ErrorCode_e {}
                ),
                quote!(move |_: crate::common::Error| -> crate::ffi::common::kafka_common_ErrorCode_e {
                    crate::ffi::common::kafka_common_ErrorCode_e::kafka_common_ErrorCode_UNKNOWN_SERVER_ERROR
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
        let expected = quote! {
            extern "C" fn f(out_value: *mut u8) -> u8 {
                crate::ffi::common::ffi_guard_or(
                    "f",
                    move |_: crate::common::Error| -> u8 { 42 },
                    move || { 1 }
                )
            }
        };
        assert_eq!(expanded, expected.to_string());
    }

    #[test]
    fn test_on_panic_closure_is_emitted_verbatim() {
        let expanded = expand_str(
            quote!(on_panic = |err| deliver(cb, opaque, Err(err))),
            quote!(
                extern "C" fn f(self_: *const X, cb: Cb, opaque: *mut c_void) {}
            ),
        )
        .unwrap();
        let expected = quote! {
            extern "C" fn f(self_: *const X, cb: Cb, opaque: *mut c_void) {
                crate::ffi::common::ffi_guard_or(
                    "f",
                    |err| deliver(cb, opaque, Err(err)),
                    move || {}
                )
            }
        };
        assert_eq!(expanded, expected.to_string());
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
