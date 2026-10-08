// Copyright 2025 Confluent Inc.
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

//! `check-ffi-translation`: every public Rust struct, enum, trait and method
//! has a correctly named and shaped C counterpart, and every C export maps
//! back to a Rust item (CLAUDE.md §4).
//!
//! The rule derives, from the crate's public surface alone, the complete set
//! of C symbols CLAUDE.md §4 asks for — names and signatures — and compares it
//! with the `#[no_mangle] extern "C"` functions and the `kafka_*` types the
//! `ffi` module declares. Both sides are `syn` items: the FFI spells its C
//! types as Rust items carrying their C names (`kafka_common_Error_t`,
//! `*const c_char`, `i32`), so a signature derived from a Rust method can be
//! compared token for token with the one the FFI declares.
//!
//! One finding kind per defect, each keyed `<kind> <symbol>`:
//!
//!   - `missing`: an expected C symbol the FFI does not export;
//!   - `unexpected`: an export no public Rust item translates to;
//!   - `shape`: a symbol whose signature differs from the derived one;
//!   - `scalar`: a signature using `bool`, `usize`, `isize`, an unsigned
//!     integer or `f32` (§4: signed fixed-width integers, `int8_t` for
//!     booleans, `uint8_t` only inside `kafka_Bytes_t`);
//!   - `unmapped`: a Rust type in a public signature the mapping table of
//!     [`Mapper::map_type`] does not cover, so the C shape cannot be derived
//!     (the symbol is still expected to exist);
//!   - `prefix`: two public Rust types deriving the same C prefix;
//!   - `baseline`: a stale or duplicated line of the burn-down baseline.
//!
//! **Baseline.** The current FFI predates §4, so the first run reports most
//! of it. [`FFI_BASELINE`] lists the finding keys the refactor still has to
//! burn down, one per line, or `prefix:kafka_admin_` to waive every symbol
//! under a prefix. A line matching no finding is stale and is itself a
//! finding, so the baseline can only shrink. `cargo xtask ffi-baseline`
//! regenerates it; `cargo xtask lint-custom check-ffi-translation
//! --no-baseline` shows everything.
//!
//! **What is expected, per Rust item** (CLAUDE.md §4 and the decisions of
//! `.claude/plans`'s C FFI audit):
//!
//!   - a public struct, or an enum whose variants all carry data (`Error`,
//!     `TopicCollection`): the opaque handle `<prefix>_t` when some method
//!     takes `self` or some static returns the type, else no handle (nothing
//!     constructs a `KafkaConsumer` from C: its constructor returns the
//!     `Consumer` interface); `<prefix>_destroy` for every handle;
//!   - every `pub fn` of its inherent impls: `<prefix>_<name>`, `self` first
//!     as `*const <prefix>_t` for `&self` and `*mut` for `&mut self` or
//!     `self` (a unit enum is the exception: its instances are borrowed
//!     singletons, so `self` and `Self` are `*const` in every position and
//!     nothing it returns has a `_destroy`), the parameters mapped
//!     positionally (a struct by value as `*const`, copied out of the
//!     caller's handle; `Error` by value as `*mut`, consumed), a `Result<T, Error>`
//!     returned as `*mut kafka_common_Error_t` with `T` through a trailing
//!     `out_*` slot, a fluent `self -> Self` setter returning nothing;
//!   - `<prefix>_to_string(const <prefix>_t *)` returning an owned string
//!     when the type implements `Display` (Java's `toString()`);
//!   - an `async fn` (or one returning `impl Future`, `Pin<Box<dyn Future>>`,
//!     `BoxFuture`): the blocking form above plus `<fn>_cb(.., cb, opaque)`
//!     and the typedef `<fn>_cb_t = fn(value, error, opaque)`, value and
//!     error slots present only when the method yields them;
//!   - an enum with a handle: `<prefix>_e` (a `#[repr(C)]` enum listing
//!     one enumerator `<prefix>_<VARIANT>` per variant, the key in constant
//!     case and no `_e_` infix, spelled out in full in Rust because cbindgen's
//!     `[enum] prefix_with_name` is off), `<prefix>__enum(const <prefix>_t *)`, one
//!     `<prefix>_<variant>(void)` returning the borrowed singleton per unit
//!     variant and one `<prefix>_<variant>(<fields>..)` returning an owned
//!     handle per data variant (`MetricValue::Double(f64)`); the Java static
//!     factory, when there is one (`OffsetSpec::for_timestamp`), is an
//!     ordinary method beside it. `Error` is the exception: its constructors
//!     are the `kafka_common_Error_<class>` factories and C classifies it
//!     with the predicates, never by variant;
//!   - a public trait some public method accepts or returns, or a public
//!     struct with a handle implements: the interface handle `<prefix>_t` and
//!     one invoker per method, the public supertraits' methods included
//!     (`MeasurableStat: Stat + Measurable` has `record` and `measure`, as
//!     Java inherits interface methods), each shaped like an inherent method
//!     (a trait nothing builds, such as `ClusterResourceListener`, expects
//!     nothing); a trait some public method accepts (`Box<dyn T>`,
//!     `Arc<dyn T>`, `&dyn T`, `impl T`, a bounded type parameter) or a
//!     public enum's variant carries (`MetricValueProvider::Gauge(Box<dyn
//!     Gauge>)`) also `<prefix>_new(void *self, <prefix>_<m>_fn_t ..)`
//!     with one `_fn_t` per method; the typedef is declared
//!     `Option<unsafe extern "C" fn(..)>` when the Rust method has a default
//!     body (a Java default method: a nullable pointer in C, `NULL` meaning
//!     the default), a bare `unsafe extern "C" fn(..)` otherwise, and the
//!     `_new` parameter is always the bare typedef name (cbindgen does not
//!     see through `Option<alias>` in a signature); an async method's
//!     `_fn_t` returns nothing and takes a trailing `int64_t callback_id`;
//!   - a client trait ([`CLIENT_TRAITS`]): `_execute_callbacks`,
//!     `_set_callbacks_notify` with its `_callbacks_notify_fn_t`, and
//!     `__set_callback_result` when it accepts an interface with an async
//!     method (the double underscore: derived by convention, no Java
//!     counterpart);
//!   - `impl Trait for Struct`, both with a handle:
//!     `<struct prefix>__as_<Trait>` returning the borrowed interface view,
//!     `*mut` on both sides when a trait method takes `&mut self`; a blanket
//!     `impl<T: A + B> Trait for T` counts for every struct implementing `A`
//!     and `B`;
//!   - an `Error` variant's payload struct with a handle:
//!     `kafka_common_Error_<payload>(const kafka_common_Error_t *)` returning
//!     the payload view.
//!
//! Prefixes come from the Java marker: `kafka_<module>_<Class>` with nested
//! classes joined by `_` (§4 rule 7), `kafka_common_<RustName>` for an
//! exception payload whatever its package, and the package module of the
//! defining Rust module for a `rust-only` type.
//!
//! Deliberate exceptions, each from CLAUDE.md §4: `KafkaError` has no C type
//! and a method mentioning it no counterpart ("never crosses the boundary on
//! its own"); `Dyn<Trait>` maps onto `Trait`; the package-less `rust-only`
//! helpers (`kafka_List_t`, `kafka_Map_t`, `kafka_Bytes_t`,
//! [`C_ONLY_FREE_FUNCTIONS`]) stand for JDK types and are accepted with any
//! functions under their prefix; the numeric error code ([`C_ONLY_ERROR_CODE`])
//! is the C caller's discriminator and has no Rust item behind it; a trait
//! method with `where Self: Sized`
//! cannot be called through the interface handle and expects nothing;
//! associated consts expect nothing (cbindgen exports no constants); a
//! struct generic over a closure (`ClosureGauge<F>`, a Java lambda's
//! stand-in) expects nothing, since C implements the trait's interface
//! instead; a Java functional interface the Rust API takes as a closure
//! ([`C_ONLY_CLOSURE_INTERFACES`]) has no Rust item, so its §4 rule 3
//! interface — `_t`, `_new`, `_destroy`, one invoker and one `_fn_t` per
//! method — is accepted as C-only, while the closure-taking Rust method stays
//! `unmapped`.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use quote::ToTokens;
use syn::ext::IdentExt;

use super::{
    compact, ffi_class, has_tag, is_cfg_test, is_no_mangle, java_markers, type_ident, variant_payload, Context, Crate,
    ModPath, Rule, RUST_ONLY,
};
use crate::java::{self, JavaIndex};

/// The burn-down baseline, relative to `rust/`.
pub(super) const FFI_BASELINE: &str = "xtask/ffi-baseline.txt";

/// The traits whose implementations are clients, which pump the queued
/// callbacks (CLAUDE.md §4 rule 5).
const CLIENT_TRAITS: &[&str] = &["Producer", "Consumer", "Admin"];

/// Public Rust types that deliberately have no C counterpart: `KafkaError`
/// is the embedded `KafkaException` base, which never crosses the boundary on
/// its own (CLAUDE.md §4).
const NO_C_TYPE: &[&str] = &["KafkaError"];

/// Package-less free helpers the FFI may export besides the `kafka_<Type>_t`
/// containers: the deallocator of an owned string (CLAUDE.md §4 rule 6).
const C_ONLY_FREE_FUNCTIONS: &[&str] = &["kafka_string_destroy"];

/// Java functional interfaces that the Rust API takes as closures, with their
/// methods. C has no closures, so each is translated as a CLAUDE.md §4 rule 3
/// interface (`<prefix>_t`, `_new`, `_destroy`, `<prefix>_<method>` and
/// `<prefix>_<method>_fn_t`) that no Rust item stands behind: the closure
/// parameter of the Rust method is `unmapped` by design, and these are the C
/// items it takes instead.
const C_ONLY_CLOSURE_INTERFACES: &[(&str, &[&str])] = &[
    ("kafka_common_KafkaFuture_BaseFunction", &["apply"]),
    ("kafka_producer_Callback", &["on_completion"]),
];

/// C items with no Rust item behind them that CLAUDE.md §4 presupposes: a C
/// caller classifies an error "beyond its numeric code" through the
/// predicates, so the numeric code itself — `kafka_common_Error_code` and the
/// `kafka_common_ErrorCode_e` enum naming every class — is part of the
/// contract. Rust needs neither: it matches the `Error` variant.
const C_ONLY_ERROR_CODE: &[&str] = &["kafka_common_ErrorCode_e", "kafka_common_Error_code"];

/// The handle over `common::Error`.
const ERROR_HANDLE: &str = "kafka_common_Error_t";

/// The cbindgen configuration, relative to the crate root `lint-custom` runs in.
pub(super) const CBINDGEN_CONFIG: &str = "cbindgen.toml";

/// The scalars no `extern "C"` signature may use.
const BANNED_SCALARS: &[&str] = &["bool", "usize", "isize", "u8", "u16", "u32", "u64", "f32"];

/// The module of the crate's flat `Error` enum.
const ERROR_MODULE: [&str; 2] = ["common", "error"];

// ---------------------------------------------------------------------------
// Findings and baseline
// ---------------------------------------------------------------------------

/// One defect; `key` is what the baseline lists.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Finding {
    kind: &'static str,
    /// The C symbol — expected or found — the finding is about.
    symbol: String,
    /// The file to fix: the Rust file implying an expected symbol, the FFI
    /// file declaring a found one.
    file: String,
    detail: String,
}

impl Finding {
    pub(super) fn key(&self) -> String {
        format!("{} {}", self.kind, self.symbol)
    }

    fn message(&self) -> String {
        format!("{}: {} `{}`: {}", self.file, self.kind, self.symbol, self.detail)
    }
}

/// The keys [`FFI_BASELINE`] waives.
struct Baseline {
    file: String,
    keys: BTreeSet<String>,
    /// The `prefix:` lines: every symbol starting with one is waived.
    prefixes: Vec<String>,
    /// Lines listed twice.
    duplicates: Vec<String>,
}

impl Baseline {
    fn load(path: &Path) -> Self {
        let mut keys = BTreeSet::new();
        let mut prefixes = Vec::new();
        let mut duplicates = Vec::new();
        let text = fs::read_to_string(path).unwrap_or_default();
        for line in text.lines().map(str::trim).filter(|l| !l.is_empty() && !l.starts_with('#')) {
            if let Some(prefix) = line.strip_prefix("prefix:") {
                if prefixes.iter().any(|p| p == prefix) {
                    duplicates.push(line.to_string());
                } else {
                    prefixes.push(prefix.to_string());
                }
            } else if !keys.insert(line.to_string()) {
                duplicates.push(line.to_string());
            }
        }
        Baseline { file: path.display().to_string(), keys, prefixes, duplicates }
    }

    /// Drops the waived findings and adds one per stale or duplicated line.
    fn apply(&self, findings: Vec<Finding>) -> Vec<Finding> {
        let mut used_keys = BTreeSet::new();
        let mut used_prefixes = BTreeSet::new();
        let mut kept = Vec::new();
        for f in findings {
            let key = f.key();
            if self.keys.contains(&key) {
                used_keys.insert(key);
                continue;
            }
            if let Some(p) = self.prefixes.iter().find(|p| f.symbol.starts_with(p.as_str())) {
                used_prefixes.insert(p.clone());
                continue;
            }
            kept.push(f);
        }
        for key in self.keys.difference(&used_keys) {
            kept.push(Finding {
                kind: "baseline",
                symbol: key.clone(),
                file: self.file.clone(),
                detail: "stale entry: no finding matches it any more; delete the line".to_string(),
            });
        }
        for prefix in self.prefixes.iter().filter(|p| !used_prefixes.contains(*p)) {
            kept.push(Finding {
                kind: "baseline",
                symbol: format!("prefix:{prefix}"),
                file: self.file.clone(),
                detail: "stale entry: no finding starts with it any more; delete the line".to_string(),
            });
        }
        for line in &self.duplicates {
            kept.push(Finding {
                kind: "baseline",
                symbol: line.clone(),
                file: self.file.clone(),
                detail: "listed twice; delete one".to_string(),
            });
        }
        kept
    }
}

// ---------------------------------------------------------------------------
// The C types a Rust type maps to
// ---------------------------------------------------------------------------

/// An FFI type, spelled the way the `ffi` module declares it.
#[derive(Clone, Debug, PartialEq, Eq)]
enum CType {
    /// `i8`, `i16`, `i32`, `i64`, `f64`.
    Scalar(&'static str),
    /// `*const c_char` (borrowed or input) / `*mut c_char` (owned output).
    Str { owned: bool },
    /// `kafka_Bytes_t` by value.
    Bytes,
    /// `*const c_void` / `*mut c_void`: a generic argument.
    Void { mutable: bool },
    /// `*const <prefix>_t` / `*mut <prefix>_t`.
    Handle { prefix: String, mutable: bool },
    /// `*const kafka_List_t` / `*mut kafka_List_t`.
    List { mutable: bool },
    /// `*const kafka_Map_t` / `*mut kafka_Map_t`.
    Map { mutable: bool },
    /// A function-pointer typedef. Nullability is part of the typedef's
    /// declaration (`Option<unsafe extern "C" fn(..)>`), not of the places
    /// that use it, so it renders as the bare name either way.
    FnPtr { name: String, nullable: bool },
}

impl CType {
    fn render(&self) -> String {
        let ptr = |mutable: bool| if mutable { "*mut" } else { "*const" };
        match self {
            CType::Scalar(s) => (*s).to_string(),
            CType::Str { owned } => format!("{} c_char", ptr(*owned)),
            CType::Bytes => "kafka_Bytes_t".to_string(),
            CType::Void { mutable } => format!("{} c_void", ptr(*mutable)),
            CType::Handle { prefix, mutable } => format!("{} {prefix}_t", ptr(*mutable)),
            CType::List { mutable } => format!("{} kafka_List_t", ptr(*mutable)),
            CType::Map { mutable } => format!("{} kafka_Map_t", ptr(*mutable)),
            CType::FnPtr { name, .. } => name.clone(),
        }
    }

    /// The `out_` slot delivering a value of this type.
    fn out_slot(&self) -> String {
        format!("*mut {}", self.render())
    }

    fn error() -> String {
        format!("*mut {ERROR_HANDLE}")
    }
}

/// Whether a Rust type is passed in or returned.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Dir {
    In,
    Out,
}

/// How a public Rust type reaches C.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TypeClass {
    /// Owned instances: `*mut` when owned or `&mut`, `*const` when borrowed.
    Struct,
    /// Borrowed singletons only: always `*const`.
    UnitEnum,
    /// An interface handle.
    Trait,
}

/// Maps the Rust types of one signature, knowing the public types, the
/// aliases, and the type parameters in scope.
struct Mapper<'a> {
    /// Public type name → (prefix, class), several when the name is ambiguous.
    by_name: &'a BTreeMap<String, Vec<(String, TypeClass)>>,
    /// The public `pub type` aliases, by name: their type parameters and the
    /// aliased type.
    aliases: &'a BTreeMap<String, (BTreeSet<String>, syn::Type)>,
    /// The error payload types the error macros define (`message_only_error!`,
    /// `kafka_error_type!`), which syn does not see as structs: they make a
    /// `Result<T, LocalIllegalStateError>` fallible but, carrying no field
    /// beyond the message, have no C type of their own (CLAUDE.md §4).
    error_types: &'a BTreeSet<String>,
    /// The owner's prefix, for `Self`.
    owner: &'a str,
    /// The owner's Rust name, for a fluent setter returning it by name.
    owner_name: &'a str,
    /// The type parameters in scope, each with its bounds.
    generics: BTreeMap<String, Vec<syn::TypeParamBound>>,
}

impl Mapper<'_> {
    /// The C type of Rust type `ty` in direction `dir`, or why it has none.
    fn map_type(&self, ty: &syn::Type, dir: Dir) -> Result<CType, String> {
        self.map(ty, dir, true, false)
    }

    /// `owned`: the value itself is passed, not a reference; `mut_ref`: behind `&mut`.
    fn map(&self, ty: &syn::Type, dir: Dir, owned: bool, mut_ref: bool) -> Result<CType, String> {
        match ty {
            syn::Type::Reference(r) => self.map(&r.elem, dir, false, r.mutability.is_some()),
            syn::Type::Paren(p) => self.map(&p.elem, dir, owned, mut_ref),
            syn::Type::Group(g) => self.map(&g.elem, dir, owned, mut_ref),
            syn::Type::Slice(s) => {
                if is_ident(&s.elem, "u8") {
                    Ok(CType::Bytes)
                } else {
                    Ok(CType::List { mutable: dir == Dir::Out })
                }
            },
            syn::Type::ImplTrait(it) => self.map_bounds(&it.bounds, dir, owned, mut_ref),
            syn::Type::TraitObject(to) => self.map_bounds(&to.bounds, dir, owned, mut_ref),
            syn::Type::Path(p) => self.map_path(p, ty, dir, owned, mut_ref),
            other => Err(format!("`{}`", compact(other))),
        }
    }

    fn map_path(
        &self,
        p: &syn::TypePath,
        ty: &syn::Type,
        dir: Dir,
        owned: bool,
        mut_ref: bool,
    ) -> Result<CType, String> {
        let Some(last) = p.path.segments.last() else {
            return Err(format!("`{}`", compact(ty)));
        };
        let name = last.ident.to_string();
        let args = type_args(&last.arguments);
        let unmapped = || Err(format!("`{}`", compact(ty)));

        if p.path.segments.len() == 1 && args.is_empty() {
            if let Some(bounds) = self.generics.get(&name) {
                if bounds.is_empty() {
                    return Ok(CType::Void { mutable: dir == Dir::Out });
                }
                return match self.map_bounds_slice(bounds, dir, owned, mut_ref) {
                    Ok(c) => Ok(c),
                    // A parameter bounded by marker traits only (`Send`, `Debug`)
                    // is still a generic argument.
                    Err(_) if bounds.iter().all(is_marker_bound) => Ok(CType::Void { mutable: dir == Dir::Out }),
                    // Otherwise the bounds say what could not be mapped
                    // (`closure ...`), which beats the parameter's name.
                    Err(e) => Err(e),
                };
            }
            if name == "Self" {
                // A unit enum's instances are borrowed singletons (D5), so
                // `Self` returned from `parse` / `for_id` is `*const` like any
                // other mention of the type; a struct's `Self` is owned.
                let singleton = self.by_name.get(self.owner_name).is_some_and(|classes| {
                    classes
                        .iter()
                        .any(|(prefix, class)| prefix == self.owner && *class == TypeClass::UnitEnum)
                });
                return Ok(CType::Handle { prefix: self.owner.to_string(), mutable: !singleton });
            }
            if let Some(scalar) = scalar_of(&name) {
                return Ok(CType::Scalar(scalar));
            }
        }
        if p.path.segments.len() == 1 {
            if let Some((params, aliased)) = self.aliases.get(&name) {
                // The alias's own parameters are generic arguments of its target
                // (`PollTask<K, V> = Box<dyn FnOnce(&mut MockConsumer<K, V>)>`).
                let mut generics = self.generics.clone();
                generics.extend(params.iter().map(|p| (p.clone(), Vec::new())));
                let inner = Mapper {
                    by_name: self.by_name,
                    aliases: self.aliases,
                    error_types: self.error_types,
                    owner: self.owner,
                    owner_name: self.owner_name,
                    generics,
                };
                return inner.map(aliased, dir, owned, mut_ref);
            }
        }

        match name.as_str() {
            "String" | "str" => return Ok(CType::Str { owned: owned && dir == Dir::Out }),
            "Duration" => return Ok(CType::Scalar("i64")),
            "Bytes" | "BytesMut" => return Ok(CType::Bytes),
            "Vec" | "VecDeque" | "LinkedList" | "HashSet" | "BTreeSet" | "IndexSet" => {
                return match args.first() {
                    Some(elem) if is_ident(elem, "u8") => Ok(CType::Bytes),
                    Some(_) => Ok(CType::List { mutable: dir == Dir::Out }),
                    None => unmapped(),
                };
            },
            // A borrowed iterator over a collection's elements, as a list.
            "Iter" | "IterMut" | "IntoIter" | "Keys" | "Values" => return Ok(CType::List { mutable: dir == Dir::Out }),
            "HashMap" | "BTreeMap" | "IndexMap" => return Ok(CType::Map { mutable: dir == Dir::Out }),
            "Option" => {
                return match args.first() {
                    // NULL, or `-1` for a scalar, stands for `None`.
                    Some(inner) => self.map(inner, dir, owned, mut_ref),
                    None => unmapped(),
                };
            },
            "Box" | "Arc" | "Rc" | "Cow" => {
                return match args.first() {
                    Some(inner) => self.map(inner, dir, true, mut_ref),
                    None => unmapped(),
                };
            },
            _ => {},
        }

        match self.by_name.get(&name).map(Vec::as_slice) {
            Some([(prefix, class)]) => Ok(match class {
                TypeClass::UnitEnum => CType::Handle { prefix: prefix.clone(), mutable: false },
                // An implementation handed to Rust (`Box<dyn Serializer>`) is
                // the client's from then on: `*mut`.
                TypeClass::Trait => CType::Handle { prefix: prefix.clone(), mutable: owned || mut_ref },
                // A struct passed by value is copied out of the caller's handle,
                // which stays the caller's to destroy: `*const`. Only `Error`
                // moves — an error handle handed to Rust is consumed — as does
                // every struct returned by value, which the caller then owns.
                TypeClass::Struct => CType::Handle {
                    prefix: prefix.clone(),
                    mutable: mut_ref || (owned && (dir == Dir::Out || name == "Error")),
                },
            }),
            Some(several) => Err(format!(
                "`{}` names {} public types ({})",
                compact(ty),
                several.len(),
                several.iter().map(|(p, _)| p.as_str()).collect::<Vec<_>>().join(", ")
            )),
            _ => unmapped(),
        }
    }

    fn map_bounds(
        &self,
        bounds: &syn::punctuated::Punctuated<syn::TypeParamBound, syn::Token![+]>,
        dir: Dir,
        owned: bool,
        mut_ref: bool,
    ) -> Result<CType, String> {
        let bounds: Vec<syn::TypeParamBound> = bounds.iter().cloned().collect();
        self.map_bounds_slice(&bounds, dir, owned, mut_ref)
    }

    /// The C type of `impl Bound`, `dyn Bound` or a parameter bounded by `Bound`.
    fn map_bounds_slice(
        &self,
        bounds: &[syn::TypeParamBound],
        dir: Dir,
        owned: bool,
        mut_ref: bool,
    ) -> Result<CType, String> {
        let shown = || bounds.iter().map(compact).collect::<Vec<_>>().join(" + ").replace(" (", "(");
        for bound in bounds {
            let syn::TypeParamBound::Trait(tb) = bound else {
                continue;
            };
            let Some(last) = tb.path.segments.last() else { continue };
            let name = last.ident.to_string();
            let args = type_args(&last.arguments);
            match name.as_str() {
                "Fn" | "FnMut" | "FnOnce" => return Err(format!("closure `{}`", shown())),
                "Into" | "AsRef" | "Borrow" => {
                    // `impl Into<String>`, `impl Into<Arc<str>>`, `impl AsRef<[u8]>`: a
                    // string or a byte buffer the callee copies.
                    if let Some(arg) = args.first() {
                        match self.map(arg, dir, false, false) {
                            Ok(CType::Str { .. }) => return Ok(CType::Str { owned: false }),
                            Ok(CType::Bytes) => return Ok(CType::Bytes),
                            _ => {},
                        }
                    }
                    return Err(format!("`impl {}`", shown()));
                },
                // `impl Display` / `impl ToString`: a value the callee only
                // formats (`Error::config_name_value(name, value)`), so C
                // passes the text.
                "Display" | "ToString" => return Ok(CType::Str { owned: false }),
                "IntoIterator" | "Iterator" | "ExactSizeIterator" | "DoubleEndedIterator" => {
                    let item = assoc_type(&last.arguments, "Item");
                    return match item {
                        Some(item) if is_ident(&item, "u8") => Ok(CType::Bytes),
                        Some(_) => Ok(CType::List { mutable: dir == Dir::Out }),
                        None => Err(format!("`impl {}`", shown())),
                    };
                },
                _ => {},
            }
            if let Some([(prefix, TypeClass::Trait)]) = self.by_name.get(&name).map(Vec::as_slice) {
                return Ok(CType::Handle { prefix: prefix.clone(), mutable: owned || mut_ref });
            }
        }
        Err(format!("`{}`", shown()))
    }
}

/// Whether `bound` is a marker or utility trait that says nothing about the
/// value's shape.
fn is_marker_bound(bound: &syn::TypeParamBound) -> bool {
    match bound {
        syn::TypeParamBound::Trait(tb) => tb.path.segments.last().is_some_and(|s| {
            matches!(
                s.ident.to_string().as_str(),
                "Send"
                    | "Sync"
                    | "Sized"
                    | "Debug"
                    | "Clone"
                    | "Copy"
                    | "Unpin"
                    | "Hash"
                    | "Eq"
                    | "PartialEq"
                    | "Ord"
                    | "PartialOrd"
                    | "Default"
            )
        }),
        syn::TypeParamBound::Lifetime(_) => true,
        _ => false,
    }
}

/// The C scalar for a Rust scalar: the signed fixed-width integers and `f64`
/// as themselves, `bool` as `i8`, unsigned and pointer-sized integers as the
/// signed type of the same role (CLAUDE.md §4 rule 4).
fn scalar_of(name: &str) -> Option<&'static str> {
    Some(match name {
        "i8" | "u8" | "bool" => "i8",
        "i16" | "u16" => "i16",
        "i32" | "u32" | "usize" => "i32",
        "i64" | "u64" | "isize" => "i64",
        "f32" | "f64" => "f64",
        _ => return None,
    })
}

/// The type arguments of a path segment: `Vec<T>` → `[T]`.
fn type_args(arguments: &syn::PathArguments) -> Vec<syn::Type> {
    let syn::PathArguments::AngleBracketed(args) = arguments else {
        return Vec::new();
    };
    args.args
        .iter()
        .filter_map(|a| match a {
            syn::GenericArgument::Type(t) => Some(t.clone()),
            _ => None,
        })
        .collect()
}

/// The `Name = T` binding of a path segment: `Iterator<Item = T>` → `T`.
fn assoc_type(arguments: &syn::PathArguments, name: &str) -> Option<syn::Type> {
    let syn::PathArguments::AngleBracketed(args) = arguments else {
        return None;
    };
    args.args.iter().find_map(|a| match a {
        syn::GenericArgument::AssocType(at) if at.ident == name => Some(at.ty.clone()),
        _ => None,
    })
}

/// Whether `ty` is the bare path `name`.
fn is_ident(ty: &syn::Type, name: &str) -> bool {
    matches!(ty, syn::Type::Path(p) if p.qself.is_none() && p.path.is_ident(name))
}

/// Whether `ty` is `()`.
fn is_unit(ty: &syn::Type) -> bool {
    matches!(ty, syn::Type::Tuple(t) if t.elems.is_empty())
}

// ---------------------------------------------------------------------------
// Signatures of expected C items
// ---------------------------------------------------------------------------

/// One parameter of an expected C function: how it is shown, the prefix its
/// name must carry if any (`out_`), and its type.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Param {
    name: String,
    name_prefix: Option<&'static str>,
    ty: String,
}

impl Param {
    fn new(name: &str, ty: String) -> Self {
        Param { name: name.to_string(), name_prefix: None, ty }
    }

    fn out(name: &str, ty: String) -> Self {
        Param { name: format!("out_{name}"), name_prefix: Some("out_"), ty }
    }
}

/// An expected C function or function-pointer signature.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
struct Sig {
    params: Vec<Param>,
    ret: Option<String>,
}

impl Sig {
    fn render(&self) -> String {
        let params: Vec<String> = self.params.iter().map(|p| format!("{}: {}", p.name, p.ty)).collect();
        match &self.ret {
            Some(ret) => format!("fn({}) -> {ret}", params.join(", ")),
            None => format!("fn({})", params.join(", ")),
        }
    }
}

/// What an expected C symbol must be.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Shape {
    /// An opaque handle struct.
    Opaque,
    /// A `#[repr(C)]` enum with exactly these variants.
    CEnum(Vec<String>),
    Fn(Sig),
    /// A `pub type .. = unsafe extern "C" fn(..)` typedef, declared
    /// `Option<unsafe extern "C" fn(..)>` when `nullable`.
    FnPtr {
        sig: Sig,
        nullable: bool,
    },
    /// Must exist; its shape could not be derived (an unmapped Rust type).
    Unknown,
}

struct Expected {
    shape: Shape,
    /// The Rust file whose item implies the symbol.
    file: String,
}

/// The expected C surface, keyed by symbol.
#[derive(Default)]
struct Expectations {
    items: BTreeMap<String, Expected>,
    findings: Vec<Finding>,
}

impl Expectations {
    fn add(&mut self, name: String, shape: Shape, file: &str) {
        self.items.entry(name).or_insert(Expected { shape, file: file.to_string() });
    }
}

// ---------------------------------------------------------------------------
// The Rust surface
// ---------------------------------------------------------------------------

/// What kind of public type, with what the kind needs.
enum Kind {
    Struct,
    Enum {
        unit: Vec<String>,
        data: Vec<DataVariant>,
    },
    Trait {
        /// Whether some method takes `&mut self`.
        mutable: bool,
    },
}

/// An enum variant carrying data: what its constructor takes.
struct DataVariant {
    name: String,
    /// The fields' names (positional ones are `value`, `value_1`, ..) and types.
    fields: Vec<(String, syn::Type)>,
}

impl DataVariant {
    fn new(v: &syn::Variant) -> Self {
        let fields = match &v.fields {
            syn::Fields::Named(n) => n
                .named
                .iter()
                .map(|f| (f.ident.as_ref().map(ToString::to_string).unwrap_or_default(), f.ty.clone()))
                .collect(),
            syn::Fields::Unnamed(u) => u
                .unnamed
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    (
                        if i == 0 {
                            "value".to_string()
                        } else {
                            format!("value_{i}")
                        },
                        f.ty.clone(),
                    )
                })
                .collect(),
            syn::Fields::Unit => Vec::new(),
        };
        DataVariant { name: v.ident.to_string(), fields }
    }
}

/// A public method: an inherent `pub fn`, or a trait's method.
struct RustMethod {
    name: String,
    sig: syn::Signature,
    /// A trait method with a body: Java's default method, a nullable pointer in C.
    has_default: bool,
}

/// A public Rust type outside the FFI.
struct RustType {
    name: String,
    prefix: String,
    file: String,
    kind: Kind,
    /// Its type parameters.
    generics: BTreeSet<String>,
    methods: Vec<RustMethod>,
}

impl RustType {
    fn class(&self) -> TypeClass {
        match &self.kind {
            Kind::Trait { .. } => TypeClass::Trait,
            Kind::Enum { unit, data } if !unit.is_empty() && data.is_empty() => TypeClass::UnitEnum,
            _ => TypeClass::Struct,
        }
    }

    /// Whether C needs an instance handle: an interface, an enum with
    /// singletons, or a struct some method is called on or some static builds.
    fn needs_handle(&self) -> bool {
        match &self.kind {
            Kind::Trait { .. } => true,
            Kind::Enum { unit, .. } if !unit.is_empty() => true,
            _ => self.methods.iter().any(|m| {
                m.sig.receiver().is_some()
                    || matches!(&m.sig.output, syn::ReturnType::Type(_, ty) if mentions(ty, "Self") || mentions(ty, &self.name))
            }),
        }
    }
}

/// Whether `ty`'s tokens contain identifier `name`.
fn mentions(ty: &syn::Type, name: &str) -> bool {
    ty.to_token_stream()
        .to_string()
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .any(|tok| tok == name)
}

/// The receiver of a method.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Receiver {
    Static,
    Ref,
    /// `&mut self`, or `self` by value (`Box<Self>` included).
    Mut,
}

fn receiver(sig: &syn::Signature) -> Receiver {
    match sig.receiver() {
        None => Receiver::Static,
        Some(r) if r.reference.is_some() && r.mutability.is_none() => Receiver::Ref,
        Some(_) => Receiver::Mut,
    }
}

/// The output of an async method, `None` for a sync one: `Some(None)` when
/// it yields `()`.
fn async_output(sig: &syn::Signature) -> Option<Option<syn::Type>> {
    let ret = match &sig.output {
        syn::ReturnType::Default => None,
        syn::ReturnType::Type(_, ty) => Some((**ty).clone()),
    };
    if sig.asyncness.is_some() {
        return Some(ret.filter(|t| !is_unit(t)));
    }
    let ty = ret?;
    future_output(&ty).map(|out| out.filter(|t| !is_unit(t)))
}

/// `X` of `impl Future<Output = X>`, `Pin<Box<dyn Future<Output = X>>>`,
/// `BoxFuture<'_, X>` or any other `*Future<.., X>` but `KafkaFuture`, which is
/// a value type; `None` when `ty` is not a future.
fn future_output(ty: &syn::Type) -> Option<Option<syn::Type>> {
    let bounds_output = |bounds: &syn::punctuated::Punctuated<syn::TypeParamBound, syn::Token![+]>| {
        bounds.iter().find_map(|b| match b {
            syn::TypeParamBound::Trait(tb) => {
                let last = tb.path.segments.last()?;
                (last.ident == "Future").then(|| assoc_type(&last.arguments, "Output"))
            },
            _ => None,
        })
    };
    match ty {
        syn::Type::ImplTrait(it) => bounds_output(&it.bounds),
        syn::Type::TraitObject(to) => bounds_output(&to.bounds),
        syn::Type::Path(p) => {
            let last = p.path.segments.last()?;
            let name = last.ident.to_string();
            let args = type_args(&last.arguments);
            match name.as_str() {
                "Pin" | "Box" => args.first().and_then(future_output),
                "KafkaFuture" => None,
                n if n.ends_with("Future") => Some(args.last().cloned()),
                _ => None,
            }
        },
        _ => None,
    }
}

/// How a method's result reaches C.
enum Output {
    /// Nothing returned.
    Unit,
    /// A value returned directly.
    Value(syn::Type),
    /// `Result<T, Error>`: the error returned, `T` (if any) through an `out_` slot.
    Fallible(Option<syn::Type>),
    /// A fluent setter consuming and returning `Self`: in C it mutates in place.
    Fluent,
}

/// Classifies `ty`, the (sync or awaited) output of a method with receiver `recv`.
fn classify_output(ty: Option<&syn::Type>, recv: Receiver, mapper: &Mapper<'_>) -> Result<Output, String> {
    let Some(ty) = ty else {
        return Ok(Output::Unit);
    };
    if is_unit(ty) {
        return Ok(Output::Unit);
    }
    if let Some(ok) = mapper.result_ok(ty)? {
        return Ok(Output::Fallible(ok));
    }
    if recv == Receiver::Mut && sig_receiver_is_owned(ty, mapper.owner_name) {
        return Ok(Output::Fluent);
    }
    Ok(Output::Value(ty.clone()))
}

impl Mapper<'_> {
    /// `Some(T)` for a `Result<T, E>` whose `E` is the crate's `Error` or one
    /// of its payload types (`LocalIllegalArgumentError`, converted into
    /// `Error` at the boundary) — `Some(None)` when `T` is `()` —, `None` for
    /// any other type, and an error for a `Result` with another error type.
    fn result_ok(&self, ty: &syn::Type) -> Result<Option<Option<syn::Type>>, String> {
        let syn::Type::Path(p) = ty else { return Ok(None) };
        let Some(last) = p.path.segments.last() else {
            return Ok(None);
        };
        if last.ident != "Result" {
            return Ok(None);
        }
        let is_error = |err: &syn::Type| {
            type_ident(err).is_some_and(|e| {
                e == "Error"
                    || (e.ends_with("Error") && (self.by_name.contains_key(&e) || self.error_types.contains(&e)))
            })
        };
        let args = type_args(&last.arguments);
        let ok = match args.as_slice() {
            [ok] => ok,
            [ok, err] if is_error(err) => ok,
            _ => return Err(format!("`{}` is not a `Result<T, Error>`", compact(ty))),
        };
        Ok(Some((!is_unit(ok)).then(|| ok.clone())))
    }
}

/// Whether `sig` is async, and how its (awaited) result reaches C.
fn method_output(sig: &syn::Signature, mapper: &Mapper<'_>) -> (bool, Result<Output, String>) {
    let recv = receiver(sig);
    match async_output(sig) {
        Some(awaited) => (true, classify_output(awaited.as_ref(), recv, mapper)),
        None => {
            let ty = match &sig.output {
                syn::ReturnType::Type(_, t) => Some(&**t),
                syn::ReturnType::Default => None,
            };
            (false, classify_output(ty, recv, mapper))
        },
    }
}

/// The C parameters Rust parameter `name: ty` becomes: one, or two for a
/// `Result<T, Error>` passed in (`KafkaFuture::completed_future`): the value
/// and the error, the error slot in the input direction (CLAUDE.md §4).
fn map_param(mapper: &Mapper<'_>, name: &str, ty: &syn::Type) -> Result<Vec<Param>, String> {
    if let Some(ok) = mapper.result_ok(ty)? {
        let mut out = Vec::new();
        if let Some(ok) = ok {
            out.push(Param::new(name, mapper.map_type(&ok, Dir::In)?.render()));
        }
        out.push(Param::new(&format!("{name}_error"), CType::error()));
        return Ok(out);
    }
    Ok(vec![Param::new(name, mapper.map_type(ty, Dir::In)?.render())])
}

/// The name of a typed parameter, without a `mut` binding.
fn param_name(pt: &syn::PatType) -> String {
    compact(&pt.pat).trim_start_matches("mut ").to_string()
}

/// Whether `ty` is `Self` or the owner type itself (a fluent setter's output).
fn sig_receiver_is_owned(ty: &syn::Type, owner: &str) -> bool {
    match ty {
        syn::Type::Path(p) => p.path.segments.last().is_some_and(|s| s.ident == "Self" || s.ident == owner),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// The rule
// ---------------------------------------------------------------------------

/// See the module documentation.
pub(super) struct FfiTranslation {
    java: Rc<JavaIndex>,
    /// Every Rust module a Java package maps to, and each of its ancestors
    /// (the prefix of a `rust-only` type is its package module).
    package_modules: BTreeSet<Vec<String>>,
    baseline: Option<Baseline>,
    cbindgen_config: Option<PathBuf>,
}

impl FfiTranslation {
    pub(super) fn new(ctx: &Context) -> Self {
        let mut package_modules = BTreeSet::new();
        for class in &ctx.java.classes {
            for len in 0..=class.module.len() {
                package_modules.insert(class.module[..len].to_vec());
            }
        }
        FfiTranslation {
            java: Rc::clone(&ctx.java),
            package_modules,
            baseline: ctx.ffi_baseline.as_deref().map(Baseline::load),
            cbindgen_config: ctx.cbindgen_config.clone(),
        }
    }

    /// The findings before the baseline, and how many symbols were compared.
    pub(super) fn findings(&self, krate: &Crate) -> (Vec<Finding>, usize) {
        let surface = self.rust_surface(krate);
        let expectations = self.expectations(&surface);
        let actual = CSurface::load(krate);
        let mut findings = expectations.findings;
        let checked = expectations.items.len() + actual.fns.len() + actual.types.len();
        compare(&expectations.items, &actual, &mut findings);
        scalar_ban(&actual, &mut findings);
        enumerator_names(&actual, &mut findings);
        if let Some(config) = &self.cbindgen_config {
            cbindgen_enum_prefixing(config, &mut findings);
        }
        findings.sort();
        findings.dedup();
        (findings, checked)
    }

    /// The C prefix of public type `name` defined in `module` with `attrs`.
    fn prefix_of(&self, attrs: &[syn::Attribute], module: &ModPath, name: &str) -> String {
        for marker in java_markers(attrs).iter().filter(|m| !m.contains('#')) {
            let Some(class) = self.java.resolve(marker) else {
                continue;
            };
            if class.name().ends_with("Exception") {
                // An error payload hangs off `kafka_common_Error_t` under its
                // Rust name, whatever the exception's package (CLAUDE.md §4).
                return format!("kafka_common_{}", class.rust_name());
            }
            if class.path.first().is_some_and(|outer| outer.ends_with("Exception")) {
                // A class nested in an exception (`RecordDeserializationException
                // .DeserializationExceptionOrigin`) hangs off the payload's
                // prefix, each segment under its Rust name (§4 rule 7).
                return format!("kafka_common_{}", class.rust_path().join("_"));
            }
            return format!("kafka_{}_{}", class.module.join("_"), class.path.join("_"));
        }
        // A Rust-only type: the package module it is defined under, less the
        // `internals` the re-export lifts it out of.
        let mut pkg: Vec<String> = Vec::new();
        for seg in module {
            pkg.push(seg.clone());
            if !self.package_modules.contains(&pkg) {
                pkg.pop();
                break;
            }
        }
        pkg.retain(|s| !s.contains("internal"));
        if pkg.is_empty() {
            format!("kafka_{name}")
        } else {
            format!("kafka_{}_{name}", pkg.join("_"))
        }
    }

    /// The public types of `krate` outside the FFI, with their methods, plus
    /// the aliases and the trait impls.
    fn rust_surface(&self, krate: &Crate) -> Surface {
        let is_ffi = |module: &[String]| module.first().is_some_and(|m| m == "ffi");
        let public = krate.public_types();
        let trait_items: BTreeMap<String, &syn::ItemTrait> = public
            .iter()
            .filter_map(|(path, name)| match krate.type_item(path, name) {
                syn::Item::Trait(t) => Some((name.clone(), t)),
                _ => None,
            })
            .collect();
        let trait_names: BTreeSet<String> = trait_items.keys().cloned().collect();

        let mut surface = Surface::default();
        for (module, name) in &public {
            if is_ffi(module) || NO_C_TYPE.contains(&name.as_str()) {
                continue;
            }
            let item = krate.type_item(module, name);
            let m = &krate.modules[module];
            let file = m.file.display().to_string();
            let (attrs, kind, generics, methods) = match item {
                syn::Item::Struct(s) => {
                    // A struct generic over a closure (`ClosureGauge<F>`)
                    // stands in for a Java lambda: C implements the trait's
                    // interface instead, so the struct has no C type.
                    if has_closure_bound(&s.generics) {
                        continue;
                    }
                    (
                        &s.attrs,
                        Kind::Struct,
                        type_params(&s.generics),
                        inherent_methods(&m.items, name),
                    )
                },
                syn::Item::Enum(e) => {
                    let (unit, data): (Vec<_>, Vec<_>) =
                        e.variants.iter().partition(|v| matches!(v.fields, syn::Fields::Unit));
                    (
                        &e.attrs,
                        Kind::Enum {
                            unit: unit.iter().map(|v| v.ident.to_string()).collect(),
                            data: data.into_iter().map(DataVariant::new).collect(),
                        },
                        type_params(&e.generics),
                        inherent_methods(&m.items, name),
                    )
                },
                syn::Item::Trait(t) => {
                    // `DynProducer` is `Producer`'s dyn-compatible companion
                    // (check-dyn-compatible): C sees one `Producer_t`.
                    if let Some(base) = name.strip_prefix("Dyn") {
                        if trait_names.contains(base) {
                            continue;
                        }
                    }
                    let methods = trait_methods(t, &trait_items, &mut BTreeSet::new());
                    let mutable = methods.iter().any(|m| receiver(&m.sig) == Receiver::Mut);
                    (&t.attrs, Kind::Trait { mutable }, type_params(&t.generics), methods)
                },
                _ => continue,
            };
            if is_hidden(attrs) {
                continue;
            }
            let prefix = self.prefix_of(attrs, module, name);
            surface
                .types
                .push(RustType { name: name.clone(), prefix, file, kind, generics, methods });
        }

        // The public type aliases, re-exported ones included
        // (`mod callback; pub use callback::Callback;`).
        let alias_names = krate.public_names(|item| match item {
            syn::Item::Type(t) if matches!(t.vis, syn::Visibility::Public(_)) => Some(t.ident.to_string()),
            _ => None,
        });
        for (module, name) in alias_names.into_iter().filter(|(module, _)| !is_ffi(module)) {
            let alias = krate.modules[&module].items.iter().find_map(|item| match item {
                syn::Item::Type(t) if t.ident == name => Some(t),
                _ => None,
            });
            if let Some(t) = alias {
                surface.aliases.insert(name, (type_params(&t.generics), (*t.ty).clone()));
            }
        }

        // Blanket impls, trait → the bounds a struct must implement.
        let mut blanket: Vec<(String, BTreeSet<String>)> = Vec::new();
        for (module, m) in krate.modules.iter().filter(|(path, _)| !is_ffi(path)) {
            for item in &m.items {
                match item {
                    // An alias is transparent, so a private one may name the type
                    // of a public signature (`type ConsumerGroupOffsets = HashMap<..>`
                    // behind `MockProducer::uncommitted_offsets`): the public
                    // aliases above take precedence over these.
                    syn::Item::Type(t) => {
                        surface
                            .aliases
                            .entry(t.ident.to_string())
                            .or_insert_with(|| (type_params(&t.generics), (*t.ty).clone()));
                    },
                    syn::Item::Macro(mac) => {
                        if let Some(def) = super::error_macro_type_def(mac) {
                            surface.error_types.insert(def.name);
                        }
                    },
                    syn::Item::Impl(i) => {
                        let Some((_, trait_path, _)) = &i.trait_ else { continue };
                        let Some(trait_name) = trait_path.segments.last().map(|s| s.ident.to_string()) else {
                            continue;
                        };
                        let Some(self_name) = type_ident(&i.self_ty) else {
                            continue;
                        };
                        if type_params(&i.generics).contains(&self_name) {
                            // `impl<T: Stat + Measurable> MeasurableStat for T`:
                            // every public struct implementing the bounds
                            // implements the trait.
                            if trait_names.contains(&trait_name) {
                                let bounds: BTreeSet<String> = bound_trait_names(&i.generics, &self_name)
                                    .into_iter()
                                    .filter(|b| trait_names.contains(b))
                                    .collect();
                                if !bounds.is_empty() {
                                    blanket.push((trait_name, bounds));
                                }
                            }
                            continue;
                        }
                        if !public.contains(&(module.clone(), self_name.clone())) {
                            continue;
                        }
                        if trait_name == "Display" {
                            surface.display.insert(self_name);
                        } else if trait_names.contains(&trait_name) {
                            surface.impls.insert((self_name, trait_name));
                        }
                    },
                    _ => {},
                }
            }
        }

        // A blanket impl applies to every struct implementing its bounds —
        // bounds another blanket impl may in turn satisfy.
        let mut changed = true;
        while changed {
            changed = false;
            let structs: BTreeSet<String> = surface.impls.iter().map(|(s, _)| s.clone()).collect();
            for (trait_name, bounds) in &blanket {
                for s in &structs {
                    if bounds.iter().all(|b| surface.impls.contains(&(s.clone(), b.clone())))
                        && surface.impls.insert((s.clone(), trait_name.clone()))
                    {
                        changed = true;
                    }
                }
            }
        }

        // The payload type of every `Error` variant.
        let error_module: ModPath = ERROR_MODULE.iter().map(ToString::to_string).collect();
        if let Some(m) = krate.modules.get(&error_module) {
            for item in &m.items {
                let syn::Item::Enum(e) = item else { continue };
                if e.ident != "Error" {
                    continue;
                }
                surface.error_payloads = e.variants.iter().filter_map(variant_payload).collect();
            }
        }
        surface
    }

    /// Every C symbol the Rust surface requires.
    fn expectations(&self, surface: &Surface) -> Expectations {
        let mut out = Expectations::default();

        // Prefix → type, reporting collisions.
        let mut by_prefix: BTreeMap<&str, &RustType> = BTreeMap::new();
        for ty in &surface.types {
            if let Some(other) = by_prefix.insert(&ty.prefix, ty) {
                out.findings.push(Finding {
                    kind: "prefix",
                    symbol: ty.prefix.clone(),
                    file: ty.file.clone(),
                    detail: format!("derived from both `{}` and `{}` ({})", other.name, ty.name, other.file),
                });
            }
        }
        let by_name: BTreeMap<String, Vec<(String, TypeClass)>> =
            surface.types.iter().fold(BTreeMap::new(), |mut acc, t| {
                acc.entry(t.name.clone()).or_default().push((t.prefix.clone(), t.class()));
                acc
            });

        // A type some public signature passes or returns needs a handle even
        // without methods of its own: `Error`, which every fallible method
        // returns, or a value type a getter hands out.
        let error_prefix = by_name.get("Error").and_then(|v| match v.as_slice() {
            [(prefix, _)] => Some(prefix.clone()),
            _ => None,
        });
        let mut referenced: BTreeSet<String> = BTreeSet::new();
        for ty in &surface.types {
            for m in &ty.methods {
                if mentions_any(&m.sig, NO_C_TYPE) {
                    continue;
                }
                let mapper = Mapper {
                    by_name: &by_name,
                    aliases: &surface.aliases,
                    error_types: &surface.error_types,
                    owner: &ty.prefix,
                    owner_name: &ty.name,
                    generics: generics_in_scope(&ty.generics, &m.sig),
                };
                let mut mapped: Vec<CType> = Vec::new();
                let mut fallible = false;
                for arg in &m.sig.inputs {
                    let syn::FnArg::Typed(pt) = arg else { continue };
                    match mapper.result_ok(&pt.ty) {
                        Ok(Some(ok)) => {
                            fallible = true;
                            mapped.extend(ok.and_then(|t| mapper.map_type(&t, Dir::In).ok()));
                        },
                        Ok(None) => mapped.extend(mapper.map_type(&pt.ty, Dir::In).ok()),
                        Err(_) => {},
                    }
                }
                match method_output(&m.sig, &mapper).1 {
                    Ok(Output::Fallible(value)) => {
                        fallible = true;
                        mapped.extend(value.and_then(|t| mapper.map_type(&t, Dir::Out).ok()));
                    },
                    Ok(Output::Value(t)) => mapped.extend(mapper.map_type(&t, Dir::Out).ok()),
                    _ => {},
                }
                referenced.extend(mapped.into_iter().filter_map(|c| match c {
                    CType::Handle { prefix, .. } => Some(prefix),
                    _ => None,
                }));
                if fallible {
                    referenced.extend(error_prefix.clone());
                }
            }
        }
        let traits: BTreeMap<&str, &RustType> = surface
            .types
            .iter()
            .filter(|t| matches!(t.kind, Kind::Trait { .. }))
            .map(|t| (t.name.as_str(), t))
            .collect();

        // The traits some public method accepts an implementation of.
        let mut accepted: BTreeSet<String> = BTreeSet::new();
        // Client trait → the traits its methods or constructors accept.
        let mut client_accepts: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        let client_of_struct: BTreeMap<&str, &str> = surface
            .impls
            .iter()
            .filter(|(_, t)| CLIENT_TRAITS.contains(&t.as_str()))
            .map(|(s, t)| (s.as_str(), t.as_str()))
            .collect();
        for ty in &surface.types {
            for m in &ty.methods {
                let found = param_traits(&m.sig, &traits);
                accepted.extend(found.iter().cloned());
                let client = if CLIENT_TRAITS.contains(&ty.name.as_str()) {
                    Some(ty.name.clone())
                } else if receiver(&m.sig) == Receiver::Static {
                    // A constructor of a client, or a factory returning one.
                    client_of_struct.get(ty.name.as_str()).map(|c| (*c).to_string()).or_else(|| {
                        CLIENT_TRAITS
                            .iter()
                            .find(|c| matches!(&m.sig.output, syn::ReturnType::Type(_, t) if mentions(t, c)))
                            .map(|c| (*c).to_string())
                    })
                } else {
                    None
                };
                if let Some(client) = client {
                    client_accepts.entry(client).or_default().extend(found);
                }
            }
        }

        // A trait a public enum's variant carries
        // (`MetricValueProvider::Gauge(Box<dyn Gauge>)`): the variant's
        // constructor accepts an implementation.
        for ty in &surface.types {
            let Kind::Enum { data, .. } = &ty.kind else { continue };
            for (_, fty) in data.iter().flat_map(|v| &v.fields) {
                let mut names = BTreeSet::new();
                collect_bound_traits(fty, &BTreeMap::new(), &mut names);
                accepted.extend(names.into_iter().filter(|n| traits.contains_key(n.as_str())));
            }
        }

        // The traits some public struct with a handle implements: its
        // `__as_<Trait>` view builds an instance.
        let implemented: BTreeSet<&str> = surface
            .impls
            .iter()
            .filter(|(s, _)| {
                surface.types.iter().any(|t| {
                    &t.name == s
                        && !matches!(t.kind, Kind::Trait { .. })
                        && (t.needs_handle() || referenced.contains(&t.prefix))
                })
            })
            .map(|(_, t)| t.as_str())
            .collect();
        let handles: BTreeSet<&str> = surface
            .types
            .iter()
            .filter(|t| match &t.kind {
                // An interface no public method accepts or returns and no
                // public struct implements has no instance C could hold
                // (`ClusterResourceListener`): neither handle nor invokers.
                // A client trait is what C holds, whatever builds it.
                Kind::Trait { .. } => {
                    CLIENT_TRAITS.contains(&t.name.as_str())
                        || accepted.contains(&t.name)
                        || referenced.contains(&t.prefix)
                        || implemented.contains(t.name.as_str())
                },
                _ => t.needs_handle() || referenced.contains(&t.prefix),
            })
            .map(|t| t.prefix.as_str())
            .collect();

        for ty in &surface.types {
            let has_handle = handles.contains(ty.prefix.as_str());
            let prefix = &ty.prefix;
            let file = ty.file.as_str();
            let mapper_for = |sig: &syn::Signature| Mapper {
                by_name: &by_name,
                aliases: &surface.aliases,
                error_types: &surface.error_types,
                owner: prefix,
                owner_name: &ty.name,
                generics: generics_in_scope(&ty.generics, sig),
            };

            if matches!(ty.kind, Kind::Trait { .. }) && !has_handle {
                continue;
            }
            if has_handle {
                out.add(format!("{prefix}_t"), Shape::Opaque, file);
            }
            // Java's `toString()`: `Display` on the Rust side, an owned string
            // in C.
            if has_handle && surface.display.contains(&ty.name) && !matches!(ty.kind, Kind::Trait { .. }) {
                out.add(
                    format!("{prefix}_to_string"),
                    Shape::Fn(Sig {
                        params: vec![Param::new("self", format!("*const {prefix}_t"))],
                        ret: Some(CType::Str { owned: true }.render()),
                    }),
                    file,
                );
            }
            let owned_handle = match &ty.kind {
                Kind::Trait { .. } => {
                    accepted.contains(&ty.name)
                        || surface
                            .types
                            .iter()
                            .flat_map(|t| &t.methods)
                            .any(|m| returns_owned_trait(&m.sig, &ty.name))
                },
                Kind::Enum { unit, data } if !unit.is_empty() => !data.is_empty(),
                _ => has_handle,
            };
            if owned_handle {
                out.add(
                    format!("{prefix}_destroy"),
                    Shape::Fn(Sig { params: vec![Param::new("self", format!("*mut {prefix}_t"))], ret: None }),
                    file,
                );
            }

            if let Kind::Enum { unit, data } = &ty.kind {
                // Not the error enum: C builds it through the
                // `kafka_common_Error_<class>` factories and classifies it
                // with the predicates (CLAUDE.md §12.4), never by variant.
                let is_error = error_prefix.as_deref() == Some(prefix.as_str());
                if has_handle && !is_error {
                    let variants: Vec<String> = unit
                        .iter()
                        .chain(data.iter().map(|v| &v.name))
                        .map(|v| enumerator(prefix, v))
                        .collect();
                    out.add(format!("{prefix}_e"), Shape::CEnum(variants), file);
                    out.add(
                        format!("{prefix}__enum"),
                        Shape::Fn(Sig {
                            params: vec![Param::new("self", format!("*const {prefix}_t"))],
                            ret: Some(format!("{prefix}_e")),
                        }),
                        file,
                    );
                }
                for v in unit {
                    out.add(
                        format!("{prefix}_{}", java::snake_case(v)),
                        Shape::Fn(Sig { params: Vec::new(), ret: Some(format!("*const {prefix}_t")) }),
                        file,
                    );
                }
                // A variant carrying data is built from its fields and owned
                // (§4 rule 2); the Java static factory, when there is one, is
                // an ordinary method beside it.
                if has_handle && !is_error {
                    for v in data {
                        let mapper = Mapper {
                            by_name: &by_name,
                            aliases: &surface.aliases,
                            error_types: &surface.error_types,
                            owner: prefix,
                            owner_name: &ty.name,
                            generics: ty.generics.iter().map(|g| (g.clone(), Vec::new())).collect(),
                        };
                        let name = format!("{prefix}_{}", java::snake_case(&v.name));
                        let mut params = Vec::new();
                        let mut unmapped = Vec::new();
                        for (fname, fty) in &v.fields {
                            match map_param(&mapper, fname, fty) {
                                Ok(c) => params.extend(c),
                                Err(why) => unmapped.push(format!("field `{fname}`: {why}")),
                            }
                        }
                        if unmapped.is_empty() {
                            out.add(name, Shape::Fn(Sig { params, ret: Some(format!("*mut {prefix}_t")) }), file);
                        } else {
                            out.findings.push(Finding {
                                kind: "unmapped",
                                symbol: name.clone(),
                                file: file.to_string(),
                                detail: format!("the C shape cannot be derived: {}", unmapped.join("; ")),
                            });
                            out.add(name, Shape::Unknown, file);
                        }
                    }
                }
            }

            // `&mut self`, or `self` by value, is `*mut` — except on a unit
            // enum, whose by-value `self` (`TimestampType::id(self)`) only reads
            // the borrowed singleton.
            let singleton = ty.class() == TypeClass::UnitEnum;
            let self_mutable = |recv: Receiver| recv == Receiver::Mut && !singleton;
            for m in &ty.methods {
                if mentions_any(&m.sig, NO_C_TYPE) {
                    continue;
                }
                let mapper = mapper_for(&m.sig);
                let recv = receiver(&m.sig);
                let self_param = (recv != Receiver::Static).then(|| {
                    Param::new(
                        "self",
                        CType::Handle { prefix: prefix.clone(), mutable: self_mutable(recv) }.render(),
                    )
                });
                expect_method(&mut out, &mapper, prefix, m, self_param, file);
            }

            if let Kind::Trait { .. } = &ty.kind {
                if accepted.contains(&ty.name) {
                    expect_interface(&mut out, &mapper_for, prefix, &ty.methods, file);
                }
                if CLIENT_TRAITS.contains(&ty.name.as_str()) {
                    let self_ty = format!("*const {prefix}_t");
                    out.add(
                        format!("{prefix}_execute_callbacks"),
                        Shape::Fn(Sig {
                            params: vec![Param::new("self", self_ty.clone())],
                            ret: Some("i32".to_string()),
                        }),
                        file,
                    );
                    let notify = format!("{prefix}_callbacks_notify_fn_t");
                    out.add(
                        notify.clone(),
                        Shape::FnPtr {
                            sig: Sig { params: vec![Param::new("opaque", "*mut c_void".to_string())], ret: None },
                            nullable: false,
                        },
                        file,
                    );
                    out.add(
                        format!("{prefix}_set_callbacks_notify"),
                        Shape::Fn(Sig {
                            params: vec![
                                Param::new("self", self_ty.clone()),
                                Param::new("notify", notify),
                                Param::new("opaque", "*mut c_void".to_string()),
                            ],
                            ret: None,
                        }),
                        file,
                    );
                    let async_interface = client_accepts
                        .get(&ty.name)
                        .into_iter()
                        .flatten()
                        .filter_map(|t| traits.get(t.as_str()))
                        .any(|t| t.methods.iter().any(|m| async_output(&m.sig).is_some()));
                    if async_interface {
                        out.add(
                            format!("{prefix}__set_callback_result"),
                            Shape::Fn(Sig {
                                params: vec![
                                    Param::new("self", self_ty),
                                    Param::new("callback_id", "i64".to_string()),
                                    Param::new("result", "*mut c_void".to_string()),
                                ],
                                ret: None,
                            }),
                            file,
                        );
                    }
                }
            }
        }

        // `__as_<Trait>` views.
        for (struct_name, trait_name) in &surface.impls {
            let Some(s) = surface
                .types
                .iter()
                .find(|t| &t.name == struct_name && !matches!(t.kind, Kind::Trait { .. }))
            else {
                continue;
            };
            let Some(t) = traits.get(trait_name.as_str()) else {
                continue;
            };
            if !handles.contains(s.prefix.as_str()) || !handles.contains(t.prefix.as_str()) {
                continue;
            }
            let Kind::Trait { mutable } = t.kind else { continue };
            out.add(
                format!("{}__as_{trait_name}", s.prefix),
                Shape::Fn(Sig {
                    params: vec![Param::new(
                        "self",
                        CType::Handle { prefix: s.prefix.clone(), mutable }.render(),
                    )],
                    ret: Some(CType::Handle { prefix: t.prefix.clone(), mutable }.render()),
                }),
                &s.file,
            );
        }

        // The payload view of every `Error` variant whose payload has a handle.
        for payload in &surface.error_payloads {
            let Some(p) = surface.types.iter().find(|t| &t.name == payload) else {
                continue;
            };
            if !handles.contains(p.prefix.as_str()) || payload == "Error" {
                continue;
            }
            // `kafka_common_Error_<class>` without the `Error` suffix
            // (`kafka_common_Error_resource_not_found`, CLAUDE.md §4) — unless
            // the `Error` constructor of that class already owns the name
            // (`kafka_common_Error_topic_authorization` builds one), in which
            // case the view keeps the suffix.
            let base = payload.strip_suffix("Error").filter(|b| !b.is_empty()).unwrap_or(payload);
            let mut view = format!("kafka_common_Error_{}", java::snake_case(base));
            if out.items.contains_key(&view) {
                view = format!("kafka_common_Error_{}", java::snake_case(payload));
            }
            out.add(
                view,
                Shape::Fn(Sig {
                    params: vec![Param::new("error", format!("*const {ERROR_HANDLE}"))],
                    ret: Some(format!("*const {}_t", p.prefix)),
                }),
                &p.file,
            );
        }
        out
    }
}

/// The public Rust surface the C one must mirror.
#[derive(Default)]
struct Surface {
    types: Vec<RustType>,
    /// The type aliases: their type parameters and target.
    aliases: BTreeMap<String, (BTreeSet<String>, syn::Type)>,
    /// The error payload types defined by the error macros.
    error_types: BTreeSet<String>,
    /// `(struct, trait)` of every `impl Trait for Struct` between public types.
    impls: BTreeSet<(String, String)>,
    /// The public types implementing `Display` (Java's `toString`).
    display: BTreeSet<String>,
    /// The payload types of `Error`'s variants.
    error_payloads: BTreeSet<String>,
}

/// The names of `generics`' type parameters.
fn type_params(generics: &syn::Generics) -> BTreeSet<String> {
    generics
        .params
        .iter()
        .filter_map(|p| match p {
            syn::GenericParam::Type(t) => Some(t.ident.to_string()),
            _ => None,
        })
        .collect()
}

/// The type parameters in scope for `sig` of a type with `owner_generics`:
/// the owner's (unbounded, plain generic arguments) and the method's own,
/// each with its bounds from the parameter list and the `where` clause.
fn generics_in_scope(
    owner_generics: &BTreeSet<String>,
    sig: &syn::Signature,
) -> BTreeMap<String, Vec<syn::TypeParamBound>> {
    let mut out: BTreeMap<String, Vec<syn::TypeParamBound>> =
        owner_generics.iter().map(|g| (g.clone(), Vec::new())).collect();
    for p in &sig.generics.params {
        if let syn::GenericParam::Type(t) = p {
            out.insert(t.ident.to_string(), t.bounds.iter().cloned().collect());
        }
    }
    if let Some(w) = &sig.generics.where_clause {
        for pred in &w.predicates {
            let syn::WherePredicate::Type(pt) = pred else { continue };
            let syn::Type::Path(p) = &pt.bounded_ty else { continue };
            let Some(ident) = p.path.get_ident() else { continue };
            if let Some(bounds) = out.get_mut(&ident.to_string()) {
                bounds.extend(pt.bounds.iter().cloned());
            }
        }
    }
    out
}

/// The trait names among `bounds`.
fn trait_bound_names<'a>(bounds: impl IntoIterator<Item = &'a syn::TypeParamBound>) -> Vec<String> {
    bounds
        .into_iter()
        .filter_map(|b| match b {
            syn::TypeParamBound::Trait(tb) => tb.path.segments.last().map(|s| s.ident.to_string()),
            _ => None,
        })
        .collect()
}

/// The traits type parameter `param` of `generics` is bounded by, from its
/// declaration and the `where` clause.
fn bound_trait_names(generics: &syn::Generics, param: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for p in &generics.params {
        if let syn::GenericParam::Type(t) = p {
            if t.ident == param {
                out.extend(trait_bound_names(&t.bounds));
            }
        }
    }
    if let Some(w) = &generics.where_clause {
        for pred in &w.predicates {
            if let syn::WherePredicate::Type(pt) = pred {
                if is_ident(&pt.bounded_ty, param) {
                    out.extend(trait_bound_names(&pt.bounds));
                }
            }
        }
    }
    out
}

/// Whether some type parameter of `generics` is a closure (`F: Fn(..)`).
fn has_closure_bound(generics: &syn::Generics) -> bool {
    type_params(generics).iter().any(|p| {
        bound_trait_names(generics, p)
            .iter()
            .any(|b| matches!(b.as_str(), "Fn" | "FnMut" | "FnOnce"))
    })
}

/// The methods of trait `t`, those of its public supertraits first in
/// declaration order, each name once: Java inherits interface methods, so
/// the C interface of `MeasurableStat: Stat + Measurable` lists `record`
/// and `measure`.
fn trait_methods(
    t: &syn::ItemTrait,
    traits: &BTreeMap<String, &syn::ItemTrait>,
    seen: &mut BTreeSet<String>,
) -> Vec<RustMethod> {
    let mut out = Vec::new();
    for name in trait_bound_names(&t.supertraits) {
        if let Some(sup) = traits.get(&name) {
            out.extend(trait_methods(sup, traits, seen));
        }
    }
    for it in &t.items {
        let syn::TraitItem::Fn(f) = it else { continue };
        if requires_sized(&f.sig.generics) || is_hidden(&f.attrs) || !seen.insert(f.sig.ident.unraw().to_string()) {
            continue;
        }
        out.push(RustMethod {
            name: f.sig.ident.unraw().to_string(),
            sig: f.sig.clone(),
            has_default: f.default.is_some(),
        });
    }
    out
}

/// Whether `generics` has a `where Self: Sized` bound.
fn requires_sized(generics: &syn::Generics) -> bool {
    super::requires_sized(generics)
}

fn is_hidden(attrs: &[syn::Attribute]) -> bool {
    attrs
        .iter()
        .any(|a| a.path().is_ident("doc") && a.meta.to_token_stream().to_string().contains("hidden"))
}

/// Whether `sig` names any of `names` in a parameter or the return type.
fn mentions_any(sig: &syn::Signature, names: &[&str]) -> bool {
    let types = sig.inputs.iter().filter_map(|a| match a {
        syn::FnArg::Typed(pt) => Some((*pt.ty).clone()),
        syn::FnArg::Receiver(_) => None,
    });
    let ret = match &sig.output {
        syn::ReturnType::Type(_, t) => Some((**t).clone()),
        syn::ReturnType::Default => None,
    };
    types.chain(ret).any(|t| names.iter().any(|n| mentions(&t, n)))
}

/// The `pub fn`s of the inherent impls of `name` among `items`, test and
/// hidden ones excluded.
fn inherent_methods(items: &[syn::Item], name: &str) -> Vec<RustMethod> {
    let mut out = Vec::new();
    for item in items {
        let syn::Item::Impl(i) = item else { continue };
        if i.trait_.is_some() || type_ident(&i.self_ty).as_deref() != Some(name) {
            continue;
        }
        for it in &i.items {
            let syn::ImplItem::Fn(f) = it else { continue };
            if !matches!(f.vis, syn::Visibility::Public(_)) || is_cfg_test(&f.attrs) || is_hidden(&f.attrs) {
                continue;
            }
            out.push(RustMethod { name: f.sig.ident.unraw().to_string(), sig: f.sig.clone(), has_default: false });
        }
    }
    out
}

/// The public traits `sig`'s parameters accept an implementation of:
/// `Box<dyn T>`, `Arc<dyn T>`, `&dyn T`, `impl T`, or a type parameter
/// bounded by `T`.
fn param_traits(sig: &syn::Signature, traits: &BTreeMap<&str, &RustType>) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let generics = generics_in_scope(&BTreeSet::new(), sig);
    let mut visit = |ty: &syn::Type| {
        let mut names = BTreeSet::new();
        collect_bound_traits(ty, &generics, &mut names);
        out.extend(names.into_iter().filter(|n| traits.contains_key(n.as_str())));
    };
    for arg in &sig.inputs {
        if let syn::FnArg::Typed(pt) = arg {
            visit(&pt.ty);
        }
    }
    out
}

/// The trait names `ty` mentions as `dyn T`, `impl T`, or through a bounded
/// type parameter.
fn collect_bound_traits(
    ty: &syn::Type,
    generics: &BTreeMap<String, Vec<syn::TypeParamBound>>,
    out: &mut BTreeSet<String>,
) {
    let bounds_of = |bounds: &mut dyn Iterator<Item = &syn::TypeParamBound>, out: &mut BTreeSet<String>| {
        for b in bounds {
            if let syn::TypeParamBound::Trait(tb) = b {
                if let Some(last) = tb.path.segments.last() {
                    out.insert(last.ident.to_string());
                }
            }
        }
    };
    match ty {
        syn::Type::Reference(r) => collect_bound_traits(&r.elem, generics, out),
        syn::Type::Paren(p) => collect_bound_traits(&p.elem, generics, out),
        syn::Type::ImplTrait(it) => bounds_of(&mut it.bounds.iter(), out),
        syn::Type::TraitObject(to) => bounds_of(&mut to.bounds.iter(), out),
        syn::Type::Path(p) => {
            if let Some(ident) = p.path.get_ident() {
                if let Some(bounds) = generics.get(&ident.to_string()) {
                    bounds_of(&mut bounds.iter(), out);
                }
            }
            for seg in &p.path.segments {
                for arg in type_args(&seg.arguments) {
                    collect_bound_traits(&arg, generics, out);
                }
            }
        },
        _ => {},
    }
}

/// Whether `sig` returns an owned implementation of trait `name`
/// (`Box<dyn T>`, `Arc<dyn T>`, possibly inside a `Result`).
fn returns_owned_trait(sig: &syn::Signature, name: &str) -> bool {
    let syn::ReturnType::Type(_, ty) = &sig.output else {
        return false;
    };
    let text = ty.to_token_stream().to_string().replace(' ', "");
    text.contains(&format!("Box<dyn{name}")) || text.contains(&format!("Arc<dyn{name}"))
}

/// Adds the expected C functions of method `m` of the type with `prefix`:
/// the blocking form, and for an async method its `_cb` twin and `_cb_t`.
fn expect_method(
    out: &mut Expectations,
    mapper: &Mapper<'_>,
    prefix: &str,
    m: &RustMethod,
    self_param: Option<Param>,
    file: &str,
) {
    let name = format!("{prefix}_{}", m.name);
    let (is_async, output) = method_output(&m.sig, mapper);

    let mut params: Vec<Param> = self_param.into_iter().collect();
    let mut unmapped: Vec<String> = Vec::new();
    for arg in &m.sig.inputs {
        let syn::FnArg::Typed(pt) = arg else { continue };
        let pname = param_name(pt);
        match map_param(mapper, &pname, &pt.ty) {
            Ok(c) => params.extend(c),
            Err(why) => unmapped.push(format!("parameter `{pname}`: {why}")),
        }
    }
    let output = match output {
        Ok(o) => Some(o),
        Err(why) => {
            unmapped.push(format!("return type: {why}"));
            None
        },
    };
    // The value the method yields, mapped as an output.
    let value: Option<Result<CType, String>> = match &output {
        Some(Output::Value(t)) | Some(Output::Fallible(Some(t))) => Some(mapper.map_type(t, Dir::Out)),
        _ => None,
    };
    if let Some(Err(why)) = &value {
        unmapped.push(format!("return type: {why}"));
    }

    if !unmapped.is_empty() {
        out.findings.push(Finding {
            kind: "unmapped",
            symbol: name.clone(),
            file: file.to_string(),
            detail: format!("the C shape cannot be derived: {}", unmapped.join("; ")),
        });
        out.add(name.clone(), Shape::Unknown, file);
        if is_async {
            out.add(format!("{name}_cb"), Shape::Unknown, file);
            out.add(format!("{name}_cb_t"), Shape::Unknown, file);
        }
        return;
    }
    let value = value.and_then(Result::ok);
    let output = output.unwrap_or(Output::Unit);

    let mut blocking = Sig { params: params.clone(), ret: None };
    match (&output, &value) {
        (Output::Fallible(_), v) => {
            if let Some(v) = v {
                blocking.params.push(Param::out(&m.name, v.out_slot()));
            }
            blocking.ret = Some(CType::error());
        },
        (Output::Value(_), Some(v)) => blocking.ret = Some(v.render()),
        _ => {},
    }
    out.add(name.clone(), Shape::Fn(blocking), file);

    if is_async {
        let cb_t = format!("{name}_cb_t");
        let mut cb_params = params;
        cb_params.push(Param::new("cb", cb_t.clone()));
        cb_params.push(Param::new("opaque", "*mut c_void".to_string()));
        out.add(format!("{name}_cb"), Shape::Fn(Sig { params: cb_params, ret: None }), file);
        let mut typedef = Sig::default();
        if let Some(v) = &value {
            typedef.params.push(Param::new("value", v.render()));
        }
        if matches!(output, Output::Fallible(_)) {
            typedef.params.push(Param::new("error", CType::error()));
        }
        typedef.params.push(Param::new("opaque", "*mut c_void".to_string()));
        out.add(cb_t, Shape::FnPtr { sig: typedef, nullable: false }, file);
    }
}

/// Adds the C interface of an accepted trait: `_new` and one `_fn_t` per
/// method (CLAUDE.md §4 rule 3).
fn expect_interface<'a>(
    out: &mut Expectations,
    mapper_for: &dyn Fn(&syn::Signature) -> Mapper<'a>,
    prefix: &str,
    methods: &[RustMethod],
    file: &str,
) {
    let mut new_params = vec![Param::new("self", "*mut c_void".to_string())];
    for m in methods {
        let fn_t = format!("{prefix}_{}_fn_t", m.name);
        new_params.push(Param::new(
            &m.name,
            CType::FnPtr { name: fn_t.clone(), nullable: m.has_default }.render(),
        ));

        let mapper = mapper_for(&m.sig);
        let (is_async, output) = method_output(&m.sig, &mapper);
        let mut sig = Sig { params: vec![Param::new("self", "*mut c_void".to_string())], ret: None };
        let mut failed = false;
        for arg in &m.sig.inputs {
            let syn::FnArg::Typed(pt) = arg else { continue };
            match map_param(&mapper, &param_name(pt), &pt.ty) {
                Ok(c) => sig.params.extend(c),
                Err(_) => failed = true,
            }
        }
        let value = match &output {
            Ok(Output::Value(t)) | Ok(Output::Fallible(Some(t))) => match mapper.map_type(t, Dir::Out) {
                Ok(c) => Some(c),
                Err(_) => {
                    failed = true;
                    None
                },
            },
            Ok(_) => None,
            Err(_) => {
                failed = true;
                None
            },
        };
        if failed {
            // Reported once, on the invoker, by `expect_method`.
            out.add(fn_t, Shape::Unknown, file);
            continue;
        }
        if is_async {
            // The result is reported through `__set_callback_result`.
            sig.params.push(Param::new("callback_id", "i64".to_string()));
        } else {
            match (&output, &value) {
                (Ok(Output::Fallible(_)), v) => {
                    if let Some(v) = v {
                        sig.params.push(Param::out(&m.name, v.out_slot()));
                    }
                    sig.ret = Some(CType::error());
                },
                (Ok(Output::Value(_)), Some(v)) => sig.ret = Some(v.render()),
                _ => {},
            }
        }
        out.add(fn_t, Shape::FnPtr { sig, nullable: m.has_default }, file);
    }
    out.add(
        format!("{prefix}_new"),
        Shape::Fn(Sig { params: new_params, ret: Some(format!("*mut {prefix}_t")) }),
        file,
    );
}

// ---------------------------------------------------------------------------
// The C surface
// ---------------------------------------------------------------------------

/// The `ffi` module's exports: its `#[no_mangle]` functions and its `pub`
/// `kafka_*` types.
#[derive(Default)]
struct CSurface {
    fns: BTreeMap<String, CFn>,
    types: BTreeMap<String, CItem>,
}

struct CFn {
    file: String,
    sig: syn::Signature,
    rust_only: bool,
}

struct CItem {
    file: String,
    item: syn::Item,
    rust_only: bool,
}

impl CSurface {
    fn load(krate: &Crate) -> Self {
        let mut out = CSurface::default();
        for (path, module) in &krate.modules {
            if path.first().is_none_or(|m| m != "ffi") {
                continue;
            }
            let file = module.file.display().to_string();
            for item in &module.items {
                match item {
                    syn::Item::Fn(f) if is_no_mangle(&f.attrs) => {
                        out.fns.insert(
                            f.sig.ident.unraw().to_string(),
                            CFn { file: file.clone(), sig: f.sig.clone(), rust_only: has_tag(&f.attrs, RUST_ONLY) },
                        );
                    },
                    syn::Item::Struct(syn::ItemStruct { vis, ident, attrs, .. })
                    | syn::Item::Enum(syn::ItemEnum { vis, ident, attrs, .. })
                    | syn::Item::Type(syn::ItemType { vis, ident, attrs, .. })
                    | syn::Item::Union(syn::ItemUnion { vis, ident, attrs, .. })
                        if matches!(vis, syn::Visibility::Public(_)) && ident.to_string().starts_with("kafka_") =>
                    {
                        out.types.insert(
                            ident.to_string(),
                            CItem { file: file.clone(), item: item.clone(), rust_only: has_tag(attrs, RUST_ONLY) },
                        );
                    },
                    _ => {},
                }
            }
        }
        out
    }

    /// The prefixes of the package-less `rust-only` C types (`kafka_List_t`),
    /// which stand for JDK types and own their functions (CLAUDE.md §4 rule 6).
    fn c_only_prefixes(&self) -> BTreeSet<String> {
        self.types
            .iter()
            .filter(|(_, item)| item.rust_only)
            .filter_map(|(name, _)| match ffi_class(name) {
                Some((pkg, class)) if pkg.is_empty() => Some(format!("kafka_{class}")),
                _ => None,
            })
            .collect()
    }
}

/// `fn(name: ty, ..) -> ret` for an FFI function's signature.
fn render_sig(sig: &syn::Signature) -> String {
    let params: Vec<String> = sig
        .inputs
        .iter()
        .map(|arg| match arg {
            syn::FnArg::Typed(pt) => format!("{}: {}", compact(&pt.pat), render_type(&pt.ty)),
            syn::FnArg::Receiver(r) => compact(r),
        })
        .collect();
    match render_return(&sig.output) {
        Some(ret) => format!("fn({}) -> {ret}", params.join(", ")),
        None => format!("fn({})", params.join(", ")),
    }
}

/// `fn(ty, ..) -> ret` for a function-pointer typedef.
fn render_bare_fn(f: &syn::TypeBareFn) -> String {
    let params: Vec<String> = f
        .inputs
        .iter()
        .map(|arg| match &arg.name {
            Some((name, _)) => format!("{name}: {}", render_type(&arg.ty)),
            None => render_type(&arg.ty),
        })
        .collect();
    match render_return(&f.output) {
        Some(ret) => format!("fn({}) -> {ret}", params.join(", ")),
        None => format!("fn({})", params.join(", ")),
    }
}

fn render_return(output: &syn::ReturnType) -> Option<String> {
    match output {
        syn::ReturnType::Default => None,
        syn::ReturnType::Type(_, ty) if is_unit(ty) => None,
        syn::ReturnType::Type(_, ty) => Some(render_type(ty)),
    }
}

/// An FFI type as the expected shapes spell it: the last path segment with
/// its arguments, pointers as `*const X` / `*mut X`.
fn render_type(ty: &syn::Type) -> String {
    match ty {
        syn::Type::Ptr(p) => {
            format!(
                "{} {}",
                if p.mutability.is_some() { "*mut" } else { "*const" },
                render_type(&p.elem)
            )
        },
        syn::Type::Path(p) => {
            let Some(last) = p.path.segments.last() else {
                return compact(ty);
            };
            let args = type_args(&last.arguments);
            if args.is_empty() {
                last.ident.to_string()
            } else {
                format!(
                    "{}<{}>",
                    last.ident,
                    args.iter().map(render_type).collect::<Vec<_>>().join(", ")
                )
            }
        },
        syn::Type::Paren(p) => render_type(&p.elem),
        syn::Type::Group(g) => render_type(&g.elem),
        syn::Type::BareFn(f) => render_bare_fn(f),
        other => compact(other),
    }
}

/// The function-pointer type a typedef declares, `unsafe extern "C" fn(..)`
/// or `Option<unsafe extern "C" fn(..)>`, and whether it is the latter: the
/// nullable form, which cbindgen emits as a plain C function pointer while an
/// `Option<alias>` in a signature it copies into the header verbatim.
fn fn_ptr_decl(ty: &syn::Type) -> Option<(&syn::TypeBareFn, bool)> {
    match ty {
        syn::Type::BareFn(f) => Some((f, false)),
        syn::Type::Path(p) => {
            let last = p.path.segments.last()?;
            if last.ident != "Option" {
                return None;
            }
            let syn::PathArguments::AngleBracketed(args) = &last.arguments else {
                return None;
            };
            match args.args.first()? {
                syn::GenericArgument::Type(syn::Type::BareFn(f)) => Some((f, true)),
                _ => None,
            }
        },
        _ => None,
    }
}

/// How a function-pointer typedef must be declared.
fn render_fn_ptr_decl(name: &str, sig: &Sig, nullable: bool) -> String {
    if nullable {
        format!("type {name} = Option<unsafe extern \"C\" {}>", sig.render())
    } else {
        format!("type {name} = unsafe extern \"C\" {}", sig.render())
    }
}

/// Compares the expected surface with the declared one.
fn compare(expected: &BTreeMap<String, Expected>, actual: &CSurface, findings: &mut Vec<Finding>) {
    let c_only = actual.c_only_prefixes();
    let closure_interfaces: BTreeSet<String> = C_ONLY_CLOSURE_INTERFACES
        .iter()
        .flat_map(|(prefix, methods)| {
            let mut items = vec![
                format!("{prefix}_t"),
                format!("{prefix}_new"),
                format!("{prefix}_destroy"),
            ];
            for m in methods.iter() {
                items.push(format!("{prefix}_{m}"));
                items.push(format!("{prefix}_{m}_fn_t"));
            }
            items
        })
        .collect();
    let is_c_only = |name: &str| {
        c_only.iter().any(|p| name == p.as_str() || name.starts_with(&format!("{p}_")))
            || C_ONLY_FREE_FUNCTIONS.contains(&name)
            || C_ONLY_ERROR_CODE.contains(&name)
            || closure_interfaces.contains(name)
    };

    for (name, exp) in expected {
        let found = match &exp.shape {
            Shape::Fn(sig) => match actual.fns.get(name) {
                None => None,
                Some(f) => {
                    let got = render_sig(&f.sig);
                    let want = sig.render();
                    let names_ok =
                        sig.params
                            .iter()
                            .zip(f.sig.inputs.iter())
                            .all(|(p, arg)| match (p.name_prefix, arg) {
                                (Some(prefix), syn::FnArg::Typed(pt)) => compact(&pt.pat).starts_with(prefix),
                                _ => true,
                            });
                    let types_ok = f.sig.inputs.len() == sig.params.len()
                        && sig.params.iter().zip(f.sig.inputs.iter()).all(|(p, arg)| match arg {
                            syn::FnArg::Typed(pt) => render_type(&pt.ty) == p.ty,
                            syn::FnArg::Receiver(_) => false,
                        })
                        && render_return(&f.sig.output) == sig.ret;
                    if !(types_ok && names_ok) {
                        findings.push(Finding {
                            kind: "shape",
                            symbol: name.clone(),
                            file: f.file.clone(),
                            detail: format!("expected `{want}`, found `{got}`"),
                        });
                    }
                    Some(())
                },
            },
            Shape::Unknown => actual.fns.get(name).map(|_| ()).or_else(|| actual.types.get(name).map(|_| ())),
            Shape::Opaque => match actual.types.get(name) {
                None => None,
                Some(t) => {
                    if !matches!(t.item, syn::Item::Struct(_)) {
                        findings.push(Finding {
                            kind: "shape",
                            symbol: name.clone(),
                            file: t.file.clone(),
                            detail: "expected an opaque `#[repr(C)] struct`".to_string(),
                        });
                    }
                    Some(())
                },
            },
            Shape::CEnum(variants) => match actual.types.get(name) {
                None => None,
                Some(t) => {
                    match &t.item {
                        syn::Item::Enum(e) => {
                            let got: Vec<String> = e.variants.iter().map(|v| v.ident.unraw().to_string()).collect();
                            let repr_c = e.attrs.iter().any(|a| {
                                a.path().is_ident("repr") && a.meta.to_token_stream().to_string().contains('C')
                            });
                            if !repr_c || got != *variants {
                                findings.push(Finding {
                                    kind: "shape",
                                    symbol: name.clone(),
                                    file: t.file.clone(),
                                    detail: format!(
                                        "expected a `#[repr(C)]` enum with variants [{}], found [{}]",
                                        variants.join(", "),
                                        got.join(", ")
                                    ),
                                });
                            }
                        },
                        _ => findings.push(Finding {
                            kind: "shape",
                            symbol: name.clone(),
                            file: t.file.clone(),
                            detail: format!("expected a `#[repr(C)]` enum with variants [{}]", variants.join(", ")),
                        }),
                    }
                    Some(())
                },
            },
            Shape::FnPtr { sig, nullable } => match actual.types.get(name) {
                None => None,
                Some(t) => {
                    let want = render_fn_ptr_decl(name, sig, *nullable);
                    match &t.item {
                        syn::Item::Type(td) => {
                            let ok = fn_ptr_decl(&td.ty).is_some_and(|(f, declared_nullable)| {
                                declared_nullable == *nullable
                                    && f.inputs.len() == sig.params.len()
                                    && f.inputs.iter().zip(sig.params.iter()).all(|(a, p)| render_type(&a.ty) == p.ty)
                                    && render_return(&f.output) == sig.ret
                            });
                            if !ok {
                                findings.push(Finding {
                                    kind: "shape",
                                    symbol: name.clone(),
                                    file: t.file.clone(),
                                    detail: format!("expected `{want}`, found `{}`", render_type(&td.ty)),
                                });
                            }
                        },
                        _ => findings.push(Finding {
                            kind: "shape",
                            symbol: name.clone(),
                            file: t.file.clone(),
                            detail: format!("expected `{want}`"),
                        }),
                    }
                    Some(())
                },
            },
        };
        if found.is_none() {
            findings.push(Finding {
                kind: "missing",
                symbol: name.clone(),
                file: exp.file.clone(),
                detail: match &exp.shape {
                    Shape::Fn(sig) => format!("expected `{}`", sig.render()),
                    Shape::FnPtr { sig, nullable } => {
                        format!("expected `{}`", render_fn_ptr_decl(name, sig, *nullable))
                    },
                    Shape::Opaque => "expected an opaque `#[repr(C)] struct`".to_string(),
                    Shape::CEnum(v) => format!("expected a `#[repr(C)]` enum with variants [{}]", v.join(", ")),
                    Shape::Unknown => "expected to exist".to_string(),
                },
            });
        }
    }

    for (name, f) in &actual.fns {
        if expected.contains_key(name)
            || is_c_only(name)
            || (f.rust_only && C_ONLY_FREE_FUNCTIONS.contains(&name.as_str()))
        {
            continue;
        }
        findings.push(Finding {
            kind: "unexpected",
            symbol: name.clone(),
            file: f.file.clone(),
            detail: "no public Rust item translates to it".to_string(),
        });
    }
    for (name, t) in &actual.types {
        if expected.contains_key(name) || is_c_only(name) {
            continue;
        }
        findings.push(Finding {
            kind: "unexpected",
            symbol: name.clone(),
            file: t.file.clone(),
            detail: "no public Rust item translates to it".to_string(),
        });
    }
}

/// The C enumerator of Rust variant `variant` in the enum with prefix
/// `prefix` (CLAUDE.md §4 "Enums"): the prefix, then the key the per-value
/// function `<prefix>_<variant>` uses, in constant case —
/// `IsolationLevel::ReadCommitted` is `kafka_common_IsolationLevel_READ_COMMITTED`
/// beside `kafka_common_IsolationLevel_read_committed()`.
fn enumerator(prefix: &str, variant: &str) -> String {
    format!("{prefix}_{}", java::snake_case(variant).to_uppercase())
}

/// Whether `key` is in constant case: upper-case letters and digits in
/// `_`-separated words, none empty.
fn is_constant_case(key: &str) -> bool {
    !key.is_empty()
        && key
            .split('_')
            .all(|word| !word.is_empty() && word.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()))
}

/// Reports every enumerator of a C enum `<prefix>_e` not spelled
/// `<prefix>_<VALUE>` with the key in constant case (CLAUDE.md §4 "Enums") —
/// including the enums with no Rust counterpart (`kafka_common_ErrorCode_e`),
/// which the per-type comparison does not see — and a per-enum
/// `cbindgen:prefix-with-name=true` annotation, which would make cbindgen
/// prepend the `_e` export name to every enumerator.
fn enumerator_names(actual: &CSurface, findings: &mut Vec<Finding>) {
    for (name, t) in &actual.types {
        let (Some(prefix), syn::Item::Enum(e)) = (name.strip_suffix("_e"), &t.item) else {
            continue;
        };
        let docs = e
            .attrs
            .iter()
            .filter(|a| a.path().is_ident("doc"))
            .map(|a| a.meta.to_token_stream().to_string().replace(' ', ""))
            .collect::<String>();
        if docs.contains("cbindgen:prefix-with-name=true") {
            findings.push(Finding {
                kind: "enumerator",
                symbol: name.clone(),
                file: t.file.clone(),
                detail: "`cbindgen:prefix-with-name=true` would prefix every enumerator with the `_e` export \
                         name; spell the enumerators `<prefix>_<VALUE>` in full instead"
                    .to_string(),
            });
        }
        for v in &e.variants {
            let ident = v.ident.unraw().to_string();
            let ok = ident
                .strip_prefix(prefix)
                .and_then(|r| r.strip_prefix('_'))
                .is_some_and(is_constant_case);
            if !ok {
                findings.push(Finding {
                    kind: "enumerator",
                    symbol: ident,
                    file: t.file.clone(),
                    detail: format!(
                        "expected `{prefix}_<VALUE>`: the enum's prefix without `_e`, then the key in constant case"
                    ),
                });
            }
        }
    }
}

/// Reports `[enum] prefix_with_name = true` in the cbindgen configuration:
/// cbindgen would then prepend each enum's `_e` export name to the enumerators
/// the FFI spells out in full, and the header would no longer carry the names
/// [`enumerator_names`] checks (CLAUDE.md §4 "Enums"). A missing file or key
/// is cbindgen's default, `false`.
fn cbindgen_enum_prefixing(config: &Path, findings: &mut Vec<Finding>) {
    let Ok(text) = fs::read_to_string(config) else {
        return;
    };
    let mut in_enum = false;
    for line in text.lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.starts_with('[') {
            in_enum = line == "[enum]";
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        if in_enum && key.trim() == "prefix_with_name" && value.trim() == "true" {
            findings.push(Finding {
                kind: "config",
                symbol: "prefix_with_name".to_string(),
                file: config.display().to_string(),
                detail: "`[enum] prefix_with_name = true` puts the `_e` export name into every C enumerator; \
                         set it to `false` (CLAUDE.md §4 \"Enums\")"
                    .to_string(),
            });
        }
    }
}

/// Reports every `extern "C"` function and function-pointer typedef using a
/// banned scalar (CLAUDE.md §4 rule 4).
fn scalar_ban(actual: &CSurface, findings: &mut Vec<Finding>) {
    for (name, f) in &actual.fns {
        let mut banned = Banned::default();
        syn::visit::Visit::visit_signature(&mut banned, &f.sig);
        banned.report(name, &f.file, findings);
    }
    for (name, t) in &actual.types {
        let syn::Item::Type(td) = &t.item else { continue };
        let mut banned = Banned::default();
        syn::visit::Visit::visit_type(&mut banned, &td.ty);
        banned.report(name, &t.file, findings);
    }
}

/// The banned scalars a visited signature uses, as written.
#[derive(Default)]
struct Banned(BTreeSet<String>);

impl Banned {
    fn report(self, symbol: &str, file: &str, findings: &mut Vec<Finding>) {
        if self.0.is_empty() {
            return;
        }
        let list: Vec<String> = self.0.iter().map(|s| format!("`{s}`")).collect();
        findings.push(Finding {
            kind: "scalar",
            symbol: symbol.to_string(),
            file: file.to_string(),
            detail: format!(
                "uses {}; use the signed fixed-width integer of the same role, `i8` for a boolean",
                list.join(", ")
            ),
        });
    }
}

impl<'ast> syn::visit::Visit<'ast> for Banned {
    fn visit_type_path(&mut self, p: &'ast syn::TypePath) {
        if let Some(ident) = p.path.get_ident() {
            let name = ident.to_string();
            if BANNED_SCALARS.contains(&name.as_str()) {
                self.0.insert(name);
            }
        }
        syn::visit::visit_type_path(self, p);
    }
}

impl Rule for FfiTranslation {
    fn name(&self) -> &'static str {
        "check-ffi-translation"
    }

    fn skip_reason(&self) -> Option<String> {
        self.java.is_empty().then(|| {
            format!(
                "no Java sources at `{}` (run `git submodule update --init kafka`)",
                java::JAVA_MAIN_ROOT
            )
        })
    }

    fn check(&self, krate: &Crate, findings: &mut Vec<String>) -> usize {
        let (mut raw, checked) = self.findings(krate);
        if let Some(baseline) = &self.baseline {
            raw = baseline.apply(raw);
            raw.sort();
        }
        findings.extend(raw.iter().map(Finding::message));
        checked
    }

    fn hint(&self) -> &'static str {
        "   Every public Rust struct, enum, trait and method has its C counterpart
   (CLAUDE.md §4): `<prefix>_t` and `<prefix>_destroy` for a type with
   instances, `<prefix>_<method>(self, params.., out_<value>)` returning
   `*mut kafka_common_Error_t` for a `Result`, a `_cb` twin and `_cb_t` typedef
   for an async method, `_e` (enumerators `<prefix>_<VALUE>` in constant case,
   no `_e_`) / `__enum` / one `(void)` function per unit variant for an enum,
   `_new` and `_fn_t`s for a trait C may implement,
   `__as_<Trait>` for a struct implementing a trait, no `bool`, `usize`,
   unsigned integer or `f32` in a signature. A finding the refactor has not
   reached yet is listed in xtask/ffi-baseline.txt (`cargo xtask ffi-baseline`
   rewrites it); a line matching nothing any more must be deleted."
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::java::JavaClass;

    fn class(package: &str, path: &[&str]) -> JavaClass {
        JavaClass {
            module: package.split('.').filter(|s| *s != "clients").map(str::to_string).collect(),
            package: package.to_string(),
            path: path.iter().map(|s| s.to_string()).collect(),
            methods: BTreeSet::new(),
            overloads: Vec::new(),
            fields: BTreeMap::new(),
            field_visibility: BTreeMap::new(),
            deprecated: false,
            is_test: false,
        }
    }

    fn index() -> JavaIndex {
        JavaIndex {
            classes: vec![
                class("clients.producer", &["KafkaProducer"]),
                class("clients.producer", &["Partitioner"]),
                class("clients.producer", &["Callback"]),
                class("clients.producer", &["ProducerRecord"]),
                class("clients.producer", &["Producer"]),
                class("clients.consumer", &["KafkaConsumer"]),
                class("clients.consumer.internals", &["Fetcher"]),
                class("clients.admin", &["OffsetSpec"]),
                class("clients.admin", &["CreateTopicsResult"]),
                class("clients.admin", &["CreateTopicsResult", "TopicMetadataAndConfig"]),
                class("common", &["TopicPartition"]),
                class("common", &["Cluster"]),
                class("common.record", &["TimestampType"]),
                class("common.errors", &["TopicAuthorizationException"]),
                class("common.errors", &["ResourceNotFoundException"]),
                class(
                    "common.errors",
                    &["RecordDeserializationException", "DeserializationExceptionOrigin"],
                ),
                class("common.serialization", &["Serializer"]),
                class("common.serialization", &["StringSerializer"]),
                class("common.metrics", &["Stat"]),
                class("common.metrics", &["Measurable"]),
                class("common.metrics", &["MeasurableStat"]),
                class("common.metrics", &["Gauge"]),
                class("common.metrics", &["MetricValueProvider"]),
                class("common.metrics", &["Metrics"]),
                class("common.metrics.stats", &["Avg"]),
            ],
        }
    }

    fn fixture_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("xtask-ffi-translation-{name}-{}", std::process::id()))
    }

    /// Writes a crate made of `rust` plus `pub mod ffi { ffi }`, runs the rule
    /// without a baseline, and returns the finding keys with their details.
    fn run(name: &str, rust: &str, ffi: &str) -> Vec<(String, String)> {
        let dir = fixture_dir(name);
        fs::create_dir_all(&dir).unwrap();
        let lib = format!("{rust}\npub mod ffi {{\n use std::ffi::{{c_char, c_void}};\n{ffi}\n}}\n");
        fs::write(dir.join("lib.rs"), lib).unwrap();
        let krate = Crate::load(&dir.join("lib.rs")).unwrap();
        let ctx = Context { java: Rc::new(index()), ffi_baseline: None, cbindgen_config: None };
        let rule = FfiTranslation::new(&ctx);
        let (findings, _) = rule.findings(&krate);
        fs::remove_dir_all(&dir).unwrap();
        findings.into_iter().map(|f| (f.key(), f.detail)).collect()
    }

    fn keys(findings: &[(String, String)]) -> Vec<&str> {
        findings.iter().map(|(k, _)| k.as_str()).collect()
    }

    fn detail<'a>(findings: &'a [(String, String)], key: &str) -> &'a str {
        findings
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, d)| d.as_str())
            .unwrap_or_else(|| panic!("no finding `{key}` among {:?}", keys(findings)))
    }

    const TOPIC_PARTITION: &str = r#"
        pub mod common {
            #[doc(alias = "org.apache.kafka.common.TopicPartition")]
            pub struct TopicPartition;
            impl TopicPartition {
                pub fn new(topic: &str, partition: i32) -> Self { TopicPartition }
                pub fn topic(&self) -> &str { "" }
                pub fn partition(&self) -> i32 { 0 }
            }
        }
    "#;

    const TOPIC_PARTITION_FFI: &str = r#"
        #[repr(C)] pub struct kafka_common_TopicPartition_t { _p: [u8; 0] }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_TopicPartition_new(topic: *const c_char, partition: i32) -> *mut kafka_common_TopicPartition_t { std::ptr::null_mut() }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_TopicPartition_topic(this: *const kafka_common_TopicPartition_t) -> *const c_char { std::ptr::null() }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_TopicPartition_partition(this: *const kafka_common_TopicPartition_t) -> i32 { 0 }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_TopicPartition_destroy(this: *mut kafka_common_TopicPartition_t) {}
    "#;

    #[test]
    fn test_complete_struct_passes() {
        let findings = run("complete", TOPIC_PARTITION, TOPIC_PARTITION_FFI);
        assert!(findings.is_empty(), "{findings:?}");
    }

    #[test]
    fn test_missing_handle_and_destroy() {
        let ffi = r#"
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_TopicPartition_new(topic: *const c_char, partition: i32) -> *mut kafka_common_TopicPartition_t { std::ptr::null_mut() }
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_TopicPartition_topic(this: *const kafka_common_TopicPartition_t) -> *const c_char { std::ptr::null() }
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_TopicPartition_partition(this: *const kafka_common_TopicPartition_t) -> i32 { 0 }
            #[repr(C)] struct kafka_common_TopicPartition_t { _p: [u8; 0] }
        "#;
        let findings = run("missing-handle", TOPIC_PARTITION, ffi);
        assert_eq!(
            keys(&findings),
            [
                "missing kafka_common_TopicPartition_destroy",
                "missing kafka_common_TopicPartition_t"
            ]
        );
        assert_eq!(
            detail(&findings, "missing kafka_common_TopicPartition_destroy"),
            "expected `fn(self: *mut kafka_common_TopicPartition_t)`"
        );
    }

    #[test]
    fn test_wrong_receiver_constness_is_a_shape_finding() {
        let ffi = TOPIC_PARTITION_FFI.replace(
            "kafka_common_TopicPartition_topic(this: *const",
            "kafka_common_TopicPartition_topic(this: *mut",
        );
        let findings = run("receiver", TOPIC_PARTITION, &ffi);
        assert_eq!(keys(&findings), ["shape kafka_common_TopicPartition_topic"]);
        assert_eq!(
            detail(&findings, "shape kafka_common_TopicPartition_topic"),
            "expected `fn(self: *const kafka_common_TopicPartition_t) -> *const c_char`, \
             found `fn(this: *mut kafka_common_TopicPartition_t) -> *const c_char`"
        );
    }

    #[test]
    fn test_no_handle_for_a_factory_returning_an_interface() {
        let rust = r#"
            pub mod producer {
                #[doc(alias = "org.apache.kafka.clients.producer.Producer")]
                pub trait Producer { fn flush(&self) -> Result<(), crate::common::Error>; }
                #[doc(alias = "org.apache.kafka.clients.producer.KafkaProducer")]
                pub struct KafkaProducer;
                impl KafkaProducer {
                    pub fn new(bootstrap: &str) -> Result<Box<dyn Producer>, crate::common::Error> { todo!() }
                }
            }
            pub mod common { pub mod error { pub enum Error { Timeout(TimeoutError) } pub struct TimeoutError; } pub use error::Error; }
        "#;
        let ffi = r#"
            #[doc(alias = "rust-only")]
            #[repr(C)] pub struct kafka_common_Error_t { _p: [u8; 0] }
            #[repr(C)] pub struct kafka_producer_Producer_t { _p: [u8; 0] }
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_producer_Producer_flush(this: *const kafka_producer_Producer_t) -> *mut kafka_common_Error_t { std::ptr::null_mut() }
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_producer_Producer_destroy(this: *mut kafka_producer_Producer_t) {}
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_producer_Producer_execute_callbacks(this: *const kafka_producer_Producer_t) -> i32 { 0 }
            pub type kafka_producer_Producer_callbacks_notify_fn_t = unsafe extern "C" fn(opaque: *mut c_void);
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_producer_Producer_set_callbacks_notify(this: *const kafka_producer_Producer_t, notify: kafka_producer_Producer_callbacks_notify_fn_t, opaque: *mut c_void) {}
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_producer_KafkaProducer_new(bootstrap: *const c_char, out_producer: *mut *mut kafka_producer_Producer_t) -> *mut kafka_common_Error_t { std::ptr::null_mut() }
        "#;
        let findings = run("factory", rust, ffi);
        // `Error` is a public enum whose variants all carry data: a class with no
        // methods here, so only its handle is implied (it is returned).
        assert_eq!(keys(&findings), ["missing kafka_common_Error_destroy"], "{findings:?}");
    }

    #[test]
    fn test_fallible_method_needs_error_return_and_out_slot() {
        let rust = r#"
            pub mod common {
                pub mod error { pub enum Error { Timeout(TimeoutError) } pub struct TimeoutError; }
                pub use error::Error;
                #[doc(alias = "org.apache.kafka.common.Cluster")]
                pub struct Cluster;
                impl Cluster {
                    pub fn node_count(&self) -> Result<i32, Error> { Ok(0) }
                }
            }
        "#;
        let ffi = r#"
            #[doc(alias = "rust-only")]
            #[repr(C)] pub struct kafka_common_Error_t { _p: [u8; 0] }
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_Error_destroy(this: *mut kafka_common_Error_t) {}
            #[repr(C)] pub struct kafka_common_Cluster_t { _p: [u8; 0] }
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_Cluster_destroy(this: *mut kafka_common_Cluster_t) {}
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_Cluster_node_count(this: *const kafka_common_Cluster_t, out_error: *mut *mut kafka_common_Error_t) -> i32 { 0 }
        "#;
        let findings = run("fallible", rust, ffi);
        assert_eq!(keys(&findings), ["shape kafka_common_Cluster_node_count"]);
        assert_eq!(
            detail(&findings, "shape kafka_common_Cluster_node_count"),
            "expected `fn(self: *const kafka_common_Cluster_t, out_node_count: *mut i32) -> *mut kafka_common_Error_t`, \
             found `fn(this: *const kafka_common_Cluster_t, out_error: *mut *mut kafka_common_Error_t) -> i32`"
        );
    }

    #[test]
    fn test_out_slot_must_be_named_out() {
        let rust = r#"
            pub mod common {
                pub mod error { pub enum Error { Timeout(TimeoutError) } pub struct TimeoutError; }
                pub use error::Error;
                #[doc(alias = "org.apache.kafka.common.Cluster")]
                pub struct Cluster;
                impl Cluster { pub fn node_count(&self) -> Result<i32, Error> { Ok(0) } }
            }
        "#;
        let ffi = r#"
            #[doc(alias = "rust-only")]
            #[repr(C)] pub struct kafka_common_Error_t { _p: [u8; 0] }
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_Error_destroy(this: *mut kafka_common_Error_t) {}
            #[repr(C)] pub struct kafka_common_Cluster_t { _p: [u8; 0] }
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_Cluster_destroy(this: *mut kafka_common_Cluster_t) {}
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_Cluster_node_count(this: *const kafka_common_Cluster_t, count: *mut i32) -> *mut kafka_common_Error_t { std::ptr::null_mut() }
        "#;
        let findings = run("out-name", rust, ffi);
        assert_eq!(keys(&findings), ["shape kafka_common_Cluster_node_count"]);
    }

    const ASYNC_RUST: &str = r#"
        pub mod common {
            pub mod error { pub enum Error { Timeout(TimeoutError) } pub struct TimeoutError; }
            pub use error::Error;
            #[doc(alias = "org.apache.kafka.common.Cluster")]
            pub struct Cluster;
            impl Cluster {
                pub async fn refresh(&self, timeout: std::time::Duration) -> Result<i64, Error> { Ok(0) }
                pub fn id(&self) -> i32 { 0 }
            }
        }
    "#;

    const ASYNC_FFI: &str = r#"
        #[doc(alias = "rust-only")]
        #[repr(C)] pub struct kafka_common_Error_t { _p: [u8; 0] }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_Error_destroy(this: *mut kafka_common_Error_t) {}
        #[repr(C)] pub struct kafka_common_Cluster_t { _p: [u8; 0] }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_Cluster_destroy(this: *mut kafka_common_Cluster_t) {}
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_Cluster_id(this: *const kafka_common_Cluster_t) -> i32 { 0 }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_Cluster_refresh(this: *const kafka_common_Cluster_t, timeout: i64, out_refresh: *mut i64) -> *mut kafka_common_Error_t { std::ptr::null_mut() }
        pub type kafka_common_Cluster_refresh_cb_t = unsafe extern "C" fn(value: i64, error: *mut kafka_common_Error_t, opaque: *mut c_void);
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_Cluster_refresh_cb(this: *const kafka_common_Cluster_t, timeout: i64, cb: kafka_common_Cluster_refresh_cb_t, opaque: *mut c_void) {}
    "#;

    #[test]
    fn test_async_method_has_blocking_form_cb_twin_and_typedef() {
        let findings = run("async-ok", ASYNC_RUST, ASYNC_FFI);
        assert!(findings.is_empty(), "{findings:?}");
    }

    #[test]
    fn test_missing_cb_typedef_and_cb_on_a_sync_method() {
        let ffi = ASYNC_FFI
            .replace("pub type kafka_common_Cluster_refresh_cb_t", "type _unused")
            + "#[unsafe(no_mangle)] pub unsafe extern \"C\" fn kafka_common_Cluster_id_cb(this: *const kafka_common_Cluster_t, cb: *mut c_void, opaque: *mut c_void) {}";
        let findings = run("async-bad", ASYNC_RUST, &ffi);
        assert_eq!(
            keys(&findings),
            [
                "missing kafka_common_Cluster_refresh_cb_t",
                "unexpected kafka_common_Cluster_id_cb"
            ],
            "{findings:?}"
        );
        assert_eq!(
            detail(&findings, "missing kafka_common_Cluster_refresh_cb_t"),
            "expected `type kafka_common_Cluster_refresh_cb_t = unsafe extern \"C\" \
             fn(value: i64, error: *mut kafka_common_Error_t, opaque: *mut c_void)`"
        );
    }

    #[test]
    fn test_impl_future_return_is_async() {
        let rust = r#"
            pub mod common {
                pub mod error { pub enum Error { Timeout(TimeoutError) } pub struct TimeoutError; }
                pub use error::Error;
                #[doc(alias = "org.apache.kafka.common.Cluster")]
                pub struct Cluster;
                impl Cluster {
                    pub fn refresh(&self) -> impl std::future::Future<Output = Result<(), Error>> + Send { async { Ok(()) } }
                }
            }
        "#;
        let findings = run("impl-future", rust, "");
        assert!(
            keys(&findings).contains(&"missing kafka_common_Cluster_refresh_cb"),
            "{findings:?}"
        );
        assert_eq!(
            detail(&findings, "missing kafka_common_Cluster_refresh_cb_t"),
            "expected `type kafka_common_Cluster_refresh_cb_t = unsafe extern \"C\" \
             fn(error: *mut kafka_common_Error_t, opaque: *mut c_void)`"
        );
        assert_eq!(
            detail(&findings, "missing kafka_common_Cluster_refresh"),
            "expected `fn(self: *const kafka_common_Cluster_t) -> *mut kafka_common_Error_t`"
        );
    }

    const ENUM_RUST: &str = r#"
        pub mod admin {
            #[doc(alias = "org.apache.kafka.clients.admin.OffsetSpec")]
            pub enum OffsetSpec { Earliest, Latest, MaxTimestamp }
        }
    "#;

    const ENUM_FFI: &str = r#"
        #[repr(C)] pub struct kafka_admin_OffsetSpec_t { _p: [u8; 0] }
        #[repr(C)] pub enum kafka_admin_OffsetSpec_e { kafka_admin_OffsetSpec_EARLIEST, kafka_admin_OffsetSpec_LATEST, kafka_admin_OffsetSpec_MAX_TIMESTAMP }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_admin_OffsetSpec__enum(this: *const kafka_admin_OffsetSpec_t) -> kafka_admin_OffsetSpec_e { kafka_admin_OffsetSpec_e::kafka_admin_OffsetSpec_EARLIEST }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_admin_OffsetSpec_earliest() -> *const kafka_admin_OffsetSpec_t { std::ptr::null() }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_admin_OffsetSpec_latest() -> *const kafka_admin_OffsetSpec_t { std::ptr::null() }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_admin_OffsetSpec_max_timestamp() -> *const kafka_admin_OffsetSpec_t { std::ptr::null() }
    "#;

    #[test]
    fn test_unit_enum_protocol_passes_without_destroy() {
        let findings = run("enum-ok", ENUM_RUST, ENUM_FFI);
        assert!(findings.is_empty(), "{findings:?}");
    }

    #[test]
    fn test_enum_without_enum_accessor_or_variant_fn() {
        let ffi = ENUM_FFI
            .lines()
            .filter(|l| !l.contains("__enum") && !l.contains("_latest()"))
            .collect::<Vec<_>>()
            .join("\n");
        let findings = run("enum-bad", ENUM_RUST, &ffi);
        assert_eq!(
            keys(&findings),
            [
                "missing kafka_admin_OffsetSpec__enum",
                "missing kafka_admin_OffsetSpec_latest"
            ]
        );
        assert_eq!(
            detail(&findings, "missing kafka_admin_OffsetSpec__enum"),
            "expected `fn(self: *const kafka_admin_OffsetSpec_t) -> kafka_admin_OffsetSpec_e`"
        );
    }

    #[test]
    fn test_c_enum_lists_every_variant() {
        let ffi = ENUM_FFI.replace(", kafka_admin_OffsetSpec_MAX_TIMESTAMP }", " }");
        let findings = run("enum-variants", ENUM_RUST, &ffi);
        assert_eq!(keys(&findings), ["shape kafka_admin_OffsetSpec_e"]);
        assert_eq!(
            detail(&findings, "shape kafka_admin_OffsetSpec_e"),
            "expected a `#[repr(C)]` enum with variants [kafka_admin_OffsetSpec_EARLIEST, kafka_admin_OffsetSpec_LATEST, \
             kafka_admin_OffsetSpec_MAX_TIMESTAMP], found [kafka_admin_OffsetSpec_EARLIEST, kafka_admin_OffsetSpec_LATEST]"
        );
    }

    const TRAIT_RUST: &str = r#"
        pub mod common {
            pub mod error { pub enum Error { Timeout(TimeoutError) } pub struct TimeoutError; }
            pub use error::Error;
            #[doc(alias = "org.apache.kafka.common.Cluster")]
            pub struct Cluster;
            impl Cluster { pub fn id(&self) -> i32 { 0 } }
        }
        pub mod producer {
            use crate::common::{Cluster, Error};
            #[doc(alias = "org.apache.kafka.clients.producer.Partitioner")]
            pub trait Partitioner<K, V>: Send + Sync {
                fn configure(&mut self, configs: &std::collections::HashMap<String, String>) {}
                fn partition(&self, topic: &str, key: Option<&K>, key_bytes: Option<&[u8]>, cluster: &Cluster) -> i32;
            }
            #[doc(alias = "org.apache.kafka.clients.producer.KafkaProducer")]
            pub struct KafkaProducer<K, V>(K, V);
            impl<K, V> KafkaProducer<K, V> {
                pub fn new(partitioner: Box<dyn Partitioner<K, V>>) -> Result<Self, Error> { todo!() }
            }
        }
    "#;

    const TRAIT_FFI: &str = r#"
        #[doc(alias = "rust-only")]
        #[repr(C)] pub struct kafka_common_Error_t { _p: [u8; 0] }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_Error_destroy(this: *mut kafka_common_Error_t) {}
        #[repr(C)] pub struct kafka_common_Cluster_t { _p: [u8; 0] }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_Cluster_destroy(this: *mut kafka_common_Cluster_t) {}
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_Cluster_id(this: *const kafka_common_Cluster_t) -> i32 { 0 }
        #[doc(alias = "rust-only")]
        #[repr(C)] pub struct kafka_Bytes_t { data: *const u8, len: i32 }
        #[doc(alias = "rust-only")]
        #[repr(C)] pub struct kafka_Map_t { _p: [u8; 0] }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_Map_new() -> *mut kafka_Map_t { std::ptr::null_mut() }
        #[repr(C)] pub struct kafka_producer_Partitioner_t { _p: [u8; 0] }
        pub type kafka_producer_Partitioner_configure_fn_t = Option<unsafe extern "C" fn(this: *mut c_void, configs: *const kafka_Map_t)>;
        pub type kafka_producer_Partitioner_partition_fn_t = unsafe extern "C" fn(this: *mut c_void, topic: *const c_char, key: *const c_void, key_bytes: kafka_Bytes_t, cluster: *const kafka_common_Cluster_t) -> i32;
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_producer_Partitioner_new(this: *mut c_void, configure: kafka_producer_Partitioner_configure_fn_t, partition: kafka_producer_Partitioner_partition_fn_t) -> *mut kafka_producer_Partitioner_t { std::ptr::null_mut() }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_producer_Partitioner_destroy(this: *mut kafka_producer_Partitioner_t) {}
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_producer_Partitioner_configure(this: *mut kafka_producer_Partitioner_t, configs: *const kafka_Map_t) {}
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_producer_Partitioner_partition(this: *const kafka_producer_Partitioner_t, topic: *const c_char, key: *const c_void, key_bytes: kafka_Bytes_t, cluster: *const kafka_common_Cluster_t) -> i32 { 0 }
        #[repr(C)] pub struct kafka_producer_KafkaProducer_t { _p: [u8; 0] }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_producer_KafkaProducer_destroy(this: *mut kafka_producer_KafkaProducer_t) {}
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_producer_KafkaProducer_new(partitioner: *mut kafka_producer_Partitioner_t, out_new: *mut *mut kafka_producer_KafkaProducer_t) -> *mut kafka_common_Error_t { std::ptr::null_mut() }
    "#;

    #[test]
    fn test_accepted_trait_interface_passes() {
        let findings = run("trait-ok", TRAIT_RUST, TRAIT_FFI);
        assert!(findings.is_empty(), "{findings:?}");
    }

    #[test]
    fn test_trait_without_invoker_or_new() {
        let ffi = TRAIT_FFI
            .lines()
            .filter(|l| {
                !l.contains("fn kafka_producer_Partitioner_partition(")
                    && !l.contains("fn kafka_producer_Partitioner_new(")
            })
            .collect::<Vec<_>>()
            .join("\n");
        let findings = run("trait-bad", TRAIT_RUST, &ffi);
        assert_eq!(
            keys(&findings),
            [
                "missing kafka_producer_Partitioner_new",
                "missing kafka_producer_Partitioner_partition"
            ]
        );
        assert_eq!(
            detail(&findings, "missing kafka_producer_Partitioner_new"),
            "expected `fn(self: *mut c_void, configure: kafka_producer_Partitioner_configure_fn_t, \
             partition: kafka_producer_Partitioner_partition_fn_t) -> *mut kafka_producer_Partitioner_t`"
        );
    }

    #[test]
    fn test_trait_nothing_builds_expects_no_handle() {
        let rust = r#"
            pub mod common {
                #[doc(alias = "org.apache.kafka.common.ClusterResource")]
                pub struct ClusterResource;
                impl ClusterResource { pub fn new() -> Self { ClusterResource } }
                #[doc(alias = "org.apache.kafka.common.ClusterResourceListener")]
                pub trait ClusterResourceListener: Send {
                    fn on_update(&self, cluster_resource: &ClusterResource);
                }
            }
        "#;
        let ffi = r#"
            #[repr(C)] pub struct kafka_common_ClusterResource_t { _p: [u8; 0] }
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_ClusterResource_new() -> *mut kafka_common_ClusterResource_t { std::ptr::null_mut() }
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_ClusterResource_destroy(this: *mut kafka_common_ClusterResource_t) {}
        "#;
        assert!(run("trait-unbuilt", rust, ffi).is_empty());
        // Once a public method accepts it, the whole interface is expected.
        let accepted = rust.replace(
            "impl ClusterResource { pub fn new() -> Self { ClusterResource } }",
            "impl ClusterResource { pub fn new() -> Self { ClusterResource } \
             pub fn watch(&self, listener: Box<dyn ClusterResourceListener>) {} }",
        );
        let findings = run("trait-accepted", &accepted, ffi);
        let found = keys(&findings);
        for key in [
            "missing kafka_common_ClusterResourceListener_t",
            "missing kafka_common_ClusterResourceListener_new",
            "missing kafka_common_ClusterResourceListener_on_update",
        ] {
            assert!(found.contains(&key), "{key} not in {found:?}");
        }
    }

    #[test]
    fn test_display_expects_to_string() {
        let rust = r#"
            pub mod common {
                use std::fmt;
                #[doc(alias = "org.apache.kafka.common.Uuid")]
                pub struct Uuid;
                impl Uuid { pub fn random_uuid() -> Self { Uuid } }
                impl fmt::Display for Uuid {
                    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { Ok(()) }
                }
            }
        "#;
        let ffi = r#"
            #[repr(C)] pub struct kafka_common_Uuid_t { _p: [u8; 0] }
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_Uuid_random_uuid() -> *mut kafka_common_Uuid_t { std::ptr::null_mut() }
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_Uuid_destroy(this: *mut kafka_common_Uuid_t) {}
        "#;
        let findings = run("display", rust, ffi);
        assert_eq!(keys(&findings), ["missing kafka_common_Uuid_to_string"]);
        assert_eq!(
            detail(&findings, "missing kafka_common_Uuid_to_string"),
            "expected `fn(self: *const kafka_common_Uuid_t) -> *mut c_char`"
        );
    }

    #[test]
    fn test_async_trait_method_fn_t_takes_callback_id() {
        let rust = r#"
            pub mod common {
                pub mod error { pub enum Error { Timeout(TimeoutError) } pub struct TimeoutError; }
                pub use error::Error;
                #[doc(alias = "org.apache.kafka.common.TopicPartition")]
                pub struct TopicPartition;
                impl TopicPartition { pub fn partition(&self) -> i32 { 0 } }
            }
            pub mod consumer {
                use crate::common::{Error, TopicPartition};
                #[doc(alias = "rust-only")]
                pub trait ConsumerRebalanceListener: Send + Sync {
                    async fn on_partitions_revoked(&self, partitions: &[TopicPartition]) -> Result<(), Error>;
                }
                #[doc(alias = "rust-only")]
                pub trait Consumer: Send {
                    async fn subscribe(&mut self, topics: Vec<String>, listener: std::sync::Arc<dyn ConsumerRebalanceListener>) -> Result<(), Error>;
                }
            }
        "#;
        let findings = run("async-trait", rust, "");
        assert_eq!(
            detail(
                &findings,
                "missing kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_fn_t"
            ),
            "expected `type kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_fn_t = unsafe extern \"C\" \
             fn(self: *mut c_void, partitions: *const kafka_List_t, callback_id: i64)`"
        );
        assert_eq!(
            detail(&findings, "missing kafka_consumer_Consumer__set_callback_result"),
            "expected `fn(self: *const kafka_consumer_Consumer_t, callback_id: i64, result: *mut c_void)`"
        );
        assert_eq!(
            detail(&findings, "missing kafka_consumer_Consumer_subscribe"),
            "expected `fn(self: *mut kafka_consumer_Consumer_t, topics: *const kafka_List_t, \
             listener: *mut kafka_consumer_ConsumerRebalanceListener_t) -> *mut kafka_common_Error_t`"
        );
        // The listener is never returned owned, so the interface's destroy comes
        // from `_new` alone; the client has no `_new` and no constructor returns
        // it here, so it has no destroy.
        assert!(keys(&findings).contains(&"missing kafka_consumer_ConsumerRebalanceListener_destroy"));
        assert!(
            !keys(&findings).contains(&"missing kafka_consumer_Consumer_destroy"),
            "{findings:?}"
        );
    }

    const AS_RUST: &str = r#"
        pub mod common {
            pub mod error { pub enum Error { Timeout(TimeoutError) } pub struct TimeoutError; }
            pub use error::Error;
            pub mod serialization {
                #[doc(alias = "org.apache.kafka.common.serialization.Serializer")]
                pub trait Serializer<T: ?Sized> {
                    fn serialize(&self, topic: &str, data: Option<&T>) -> Result<Option<Vec<u8>>, crate::common::Error>;
                }
                #[doc(alias = "org.apache.kafka.common.serialization.StringSerializer")]
                pub struct StringSerializer;
                impl StringSerializer { pub fn new() -> Self { StringSerializer } }
                impl Serializer<str> for StringSerializer {
                    fn serialize(&self, topic: &str, data: Option<&str>) -> Result<Option<Vec<u8>>, crate::common::Error> { Ok(None) }
                }
                impl Serializer<String> for StringSerializer {
                    fn serialize(&self, topic: &str, data: Option<&String>) -> Result<Option<Vec<u8>>, crate::common::Error> { Ok(None) }
                }
            }
        }
    "#;

    const AS_FFI: &str = r#"
        #[doc(alias = "rust-only")]
        #[repr(C)] pub struct kafka_common_Error_t { _p: [u8; 0] }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_Error_destroy(this: *mut kafka_common_Error_t) {}
        #[doc(alias = "rust-only")]
        #[repr(C)] pub struct kafka_Bytes_t { data: *const u8, len: i32 }
        #[repr(C)] pub struct kafka_common_serialization_Serializer_t { _p: [u8; 0] }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_serialization_Serializer_serialize(this: *const kafka_common_serialization_Serializer_t, topic: *const c_char, data: *const c_void, out_serialize: *mut kafka_Bytes_t) -> *mut kafka_common_Error_t { std::ptr::null_mut() }
        #[repr(C)] pub struct kafka_common_serialization_StringSerializer_t { _p: [u8; 0] }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_serialization_StringSerializer_new() -> *mut kafka_common_serialization_StringSerializer_t { std::ptr::null_mut() }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_serialization_StringSerializer_destroy(this: *mut kafka_common_serialization_StringSerializer_t) {}
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_serialization_StringSerializer__as_Serializer(this: *const kafka_common_serialization_StringSerializer_t) -> *const kafka_common_serialization_Serializer_t { std::ptr::null() }
    "#;

    #[test]
    fn test_as_view_once_per_struct_trait_pair() {
        let findings = run("as-ok", AS_RUST, AS_FFI);
        assert!(findings.is_empty(), "{findings:?}");
    }

    #[test]
    fn test_missing_as_view_and_as_view_without_impl() {
        let ffi = AS_FFI.replace("StringSerializer__as_Serializer", "StringSerializer__as_Deserializer");
        let findings = run("as-bad", AS_RUST, &ffi);
        assert_eq!(
            keys(&findings),
            [
                "missing kafka_common_serialization_StringSerializer__as_Serializer",
                "unexpected kafka_common_serialization_StringSerializer__as_Deserializer",
            ]
        );
        assert_eq!(
            detail(&findings, "missing kafka_common_serialization_StringSerializer__as_Serializer"),
            "expected `fn(self: *const kafka_common_serialization_StringSerializer_t) \
             -> *const kafka_common_serialization_Serializer_t`"
        );
    }

    /// Supertraits, a blanket impl, a trait carried by an enum variant, data
    /// variants and a closure adapter: the metrics module in miniature.
    const METRICS_RUST: &str = r#"
        pub mod common {
            pub mod metrics {
                #[doc(alias = "org.apache.kafka.common.metrics.Stat")]
                pub trait Stat: Send + Sync { fn record(&self, value: f64, time_ms: i64); }
                #[doc(alias = "org.apache.kafka.common.metrics.Measurable")]
                pub trait Measurable: Send + Sync { fn measure(&self, now: i64) -> f64; }
                #[doc(alias = "org.apache.kafka.common.metrics.MeasurableStat")]
                pub trait MeasurableStat: Stat + Measurable {}
                impl<T: Stat + Measurable> MeasurableStat for T {}
                #[doc(alias = "org.apache.kafka.common.metrics.Gauge")]
                pub trait Gauge: Send + Sync { fn value(&self, now: i64) -> f64; }
                #[doc(alias = "rust-only")]
                pub struct ClosureGauge<F>(F) where F: Fn(i64) -> f64 + Send + Sync;
                impl<F> ClosureGauge<F> where F: Fn(i64) -> f64 + Send + Sync { pub fn new(f: F) -> Self { ClosureGauge(f) } }
                impl<F> Gauge for ClosureGauge<F> where F: Fn(i64) -> f64 + Send + Sync { fn value(&self, now: i64) -> f64 { (self.0)(now) } }
                #[doc(alias = "org.apache.kafka.common.metrics.MetricValueProvider")]
                pub enum MetricValueProvider { Measurable(Box<dyn Measurable>), Gauge(Box<dyn Gauge>) }
                #[doc(alias = "org.apache.kafka.common.metrics.Metrics")]
                pub struct Metrics;
                impl Metrics {
                    pub fn new() -> Self { Metrics }
                    pub fn add(&self, stat: Box<dyn MeasurableStat>) {}
                    pub fn add_provider(&self, provider: MetricValueProvider) {}
                }
                pub mod stats {
                    #[doc(alias = "org.apache.kafka.common.metrics.stats.Avg")]
                    pub struct Avg;
                    impl Avg { pub fn new() -> Self { Avg } }
                    impl super::Stat for Avg { fn record(&self, value: f64, time_ms: i64) {} }
                    impl super::Measurable for Avg { fn measure(&self, now: i64) -> f64 { 0.0 } }
                }
            }
        }
    "#;

    const METRICS_FFI: &str = r#"
        #[repr(C)] pub struct kafka_common_metrics_Stat_t { _p: [u8; 0] }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_metrics_Stat_record(this: *const kafka_common_metrics_Stat_t, value: f64, time_ms: i64) {}
        #[repr(C)] pub struct kafka_common_metrics_Measurable_t { _p: [u8; 0] }
        pub type kafka_common_metrics_Measurable_measure_fn_t = unsafe extern "C" fn(this: *mut c_void, now: i64) -> f64;
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_metrics_Measurable_new(this: *mut c_void, measure: kafka_common_metrics_Measurable_measure_fn_t) -> *mut kafka_common_metrics_Measurable_t { std::ptr::null_mut() }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_metrics_Measurable_measure(this: *const kafka_common_metrics_Measurable_t, now: i64) -> f64 { 0.0 }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_metrics_Measurable_destroy(this: *mut kafka_common_metrics_Measurable_t) {}
        #[repr(C)] pub struct kafka_common_metrics_MeasurableStat_t { _p: [u8; 0] }
        pub type kafka_common_metrics_MeasurableStat_record_fn_t = unsafe extern "C" fn(this: *mut c_void, value: f64, time_ms: i64);
        pub type kafka_common_metrics_MeasurableStat_measure_fn_t = unsafe extern "C" fn(this: *mut c_void, now: i64) -> f64;
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_metrics_MeasurableStat_new(this: *mut c_void, record: kafka_common_metrics_MeasurableStat_record_fn_t, measure: kafka_common_metrics_MeasurableStat_measure_fn_t) -> *mut kafka_common_metrics_MeasurableStat_t { std::ptr::null_mut() }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_metrics_MeasurableStat_record(this: *const kafka_common_metrics_MeasurableStat_t, value: f64, time_ms: i64) {}
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_metrics_MeasurableStat_measure(this: *const kafka_common_metrics_MeasurableStat_t, now: i64) -> f64 { 0.0 }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_metrics_MeasurableStat_destroy(this: *mut kafka_common_metrics_MeasurableStat_t) {}
        #[repr(C)] pub struct kafka_common_metrics_Gauge_t { _p: [u8; 0] }
        pub type kafka_common_metrics_Gauge_value_fn_t = unsafe extern "C" fn(this: *mut c_void, now: i64) -> f64;
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_metrics_Gauge_new(this: *mut c_void, value: kafka_common_metrics_Gauge_value_fn_t) -> *mut kafka_common_metrics_Gauge_t { std::ptr::null_mut() }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_metrics_Gauge_value(this: *const kafka_common_metrics_Gauge_t, now: i64) -> f64 { 0.0 }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_metrics_Gauge_destroy(this: *mut kafka_common_metrics_Gauge_t) {}
        #[repr(C)] pub struct kafka_common_metrics_MetricValueProvider_t { _p: [u8; 0] }
        #[repr(C)] pub enum kafka_common_metrics_MetricValueProvider_e { kafka_common_metrics_MetricValueProvider_MEASURABLE, kafka_common_metrics_MetricValueProvider_GAUGE }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_metrics_MetricValueProvider__enum(this: *const kafka_common_metrics_MetricValueProvider_t) -> kafka_common_metrics_MetricValueProvider_e { kafka_common_metrics_MetricValueProvider_e::kafka_common_metrics_MetricValueProvider_GAUGE }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_metrics_MetricValueProvider_measurable(value: *mut kafka_common_metrics_Measurable_t) -> *mut kafka_common_metrics_MetricValueProvider_t { std::ptr::null_mut() }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_metrics_MetricValueProvider_gauge(value: *mut kafka_common_metrics_Gauge_t) -> *mut kafka_common_metrics_MetricValueProvider_t { std::ptr::null_mut() }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_metrics_MetricValueProvider_destroy(this: *mut kafka_common_metrics_MetricValueProvider_t) {}
        #[repr(C)] pub struct kafka_common_metrics_Metrics_t { _p: [u8; 0] }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_metrics_Metrics_new() -> *mut kafka_common_metrics_Metrics_t { std::ptr::null_mut() }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_metrics_Metrics_add(this: *const kafka_common_metrics_Metrics_t, stat: *mut kafka_common_metrics_MeasurableStat_t) {}
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_metrics_Metrics_add_provider(this: *const kafka_common_metrics_Metrics_t, provider: *const kafka_common_metrics_MetricValueProvider_t) {}
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_metrics_Metrics_destroy(this: *mut kafka_common_metrics_Metrics_t) {}
        #[repr(C)] pub struct kafka_common_metrics_stats_Avg_t { _p: [u8; 0] }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_metrics_stats_Avg_new() -> *mut kafka_common_metrics_stats_Avg_t { std::ptr::null_mut() }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_metrics_stats_Avg__as_Stat(this: *const kafka_common_metrics_stats_Avg_t) -> *const kafka_common_metrics_Stat_t { std::ptr::null() }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_metrics_stats_Avg__as_Measurable(this: *const kafka_common_metrics_stats_Avg_t) -> *const kafka_common_metrics_Measurable_t { std::ptr::null() }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_metrics_stats_Avg__as_MeasurableStat(this: *const kafka_common_metrics_stats_Avg_t) -> *const kafka_common_metrics_MeasurableStat_t { std::ptr::null() }
        #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_metrics_stats_Avg_destroy(this: *mut kafka_common_metrics_stats_Avg_t) {}
    "#;

    #[test]
    fn test_metrics_shapes_pass() {
        let findings = run("metrics-ok", METRICS_RUST, METRICS_FFI);
        assert!(findings.is_empty(), "{findings:?}");
    }

    #[test]
    fn test_closure_adapter_struct_expects_nothing() {
        let ffi = format!(
            "{METRICS_FFI}
            #[repr(C)] pub struct kafka_common_metrics_ClosureGauge_t {{ _p: [u8; 0] }}
            #[unsafe(no_mangle)] pub unsafe extern \"C\" fn kafka_common_metrics_ClosureGauge_new(f: *const c_void) -> *mut kafka_common_metrics_ClosureGauge_t {{ std::ptr::null_mut() }}"
        );
        let findings = run("metrics-closure", METRICS_RUST, &ffi);
        assert_eq!(
            keys(&findings),
            [
                "unexpected kafka_common_metrics_ClosureGauge_new",
                "unexpected kafka_common_metrics_ClosureGauge_t",
            ]
        );
    }

    #[test]
    fn test_closure_interface_is_accepted_as_c_only() {
        // `then_apply` takes a closure (Java's `KafkaFuture.BaseFunction`): the
        // method is `unmapped`, and the rule 3 interface C gets instead has no
        // Rust item, so it is admitted — but only its rule 3 items.
        let rust = r#"
            pub mod common {
                #[doc(alias = "org.apache.kafka.common.KafkaFuture")]
                pub struct KafkaFuture<T>(T);
                impl<T> KafkaFuture<T> {
                    pub fn then_apply<R, F: Fn(T) -> R>(&self, function: F) -> KafkaFuture<R> { unimplemented!() }
                }
            }
        "#;
        let ffi = r#"
            #[repr(C)] pub struct kafka_common_KafkaFuture_t { _p: [u8; 0] }
            #[repr(C)] pub struct kafka_common_KafkaFuture_BaseFunction_t { _p: [u8; 0] }
            pub type kafka_common_KafkaFuture_BaseFunction_apply_fn_t = unsafe extern "C" fn(self_: *mut c_void, a: *mut c_void, out_apply: *mut *mut c_void) -> *mut kafka_common_Error_t;
            #[repr(C)] pub struct kafka_common_Error_t { _p: [u8; 0] }
            #[unsafe(no_mangle)] pub extern "C" fn kafka_common_KafkaFuture_BaseFunction_new(self_: *mut c_void, apply: kafka_common_KafkaFuture_BaseFunction_apply_fn_t) -> *mut kafka_common_KafkaFuture_BaseFunction_t { std::ptr::null_mut() }
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_KafkaFuture_BaseFunction_apply(self_: *const kafka_common_KafkaFuture_BaseFunction_t, a: *mut c_void, out_apply: *mut *mut c_void) -> *mut kafka_common_Error_t { std::ptr::null_mut() }
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_KafkaFuture_BaseFunction_destroy(self_: *mut kafka_common_KafkaFuture_BaseFunction_t) {}
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_KafkaFuture_BaseFunction_stray(self_: *mut kafka_common_KafkaFuture_BaseFunction_t) {}
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_KafkaFuture_then_apply(self_: *const kafka_common_KafkaFuture_t, function: *const kafka_common_KafkaFuture_BaseFunction_t) -> *mut kafka_common_KafkaFuture_t { std::ptr::null_mut() }
        "#;
        let findings = run("closure-interface", rust, ffi);
        let keys: Vec<&str> = keys(&findings)
            .into_iter()
            .filter(|k| k.contains("BaseFunction") || k.contains("then_apply"))
            .collect();
        assert_eq!(
            keys,
            [
                "unexpected kafka_common_KafkaFuture_BaseFunction_stray",
                "unmapped kafka_common_KafkaFuture_then_apply"
            ]
        );
    }

    #[test]
    fn test_producer_callback_interface_is_accepted_as_c_only() {
        // `Producer::send_with_callback` takes Java's `Callback` as a closure:
        // the method is `unmapped`, and `kafka_producer_Callback` is the C-only
        // interface standing in for it — admitted with its rule 3 items only.
        let rust = r#"
            pub mod producer {
                #[doc(alias = "org.apache.kafka.clients.producer.Producer")]
                pub trait Producer<K, V> {
                    fn send_with_callback(&self, record: K, callback: Option<Box<dyn FnOnce(Option<&V>) + Send + Sync>>) -> i32;
                }
            }
        "#;
        let ffi = r#"
            #[repr(C)] pub struct kafka_producer_Producer_t { _p: [u8; 0] }
            #[repr(C)] pub struct kafka_producer_Callback_t { _p: [u8; 0] }
            #[repr(C)] pub struct kafka_common_Error_t { _p: [u8; 0] }
            pub type kafka_producer_Callback_on_completion_fn_t = unsafe extern "C" fn(self_: *mut c_void, metadata: *const c_void, error: *const kafka_common_Error_t);
            #[unsafe(no_mangle)] pub extern "C" fn kafka_producer_Callback_new(self_: *mut c_void, on_completion: kafka_producer_Callback_on_completion_fn_t) -> *mut kafka_producer_Callback_t { std::ptr::null_mut() }
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_producer_Callback_on_completion(self_: *const kafka_producer_Callback_t, metadata: *const c_void, error: *const kafka_common_Error_t) {}
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_producer_Callback_destroy(self_: *mut kafka_producer_Callback_t) {}
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_producer_Callback_stray(self_: *mut kafka_producer_Callback_t) {}
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_producer_Producer_send_with_callback(self_: *const kafka_producer_Producer_t, record: *const c_void, callback: *const kafka_producer_Callback_t) -> i32 { 0 }
        "#;
        let findings = run("callback-interface", rust, ffi);
        let keys: Vec<&str> = keys(&findings)
            .into_iter()
            .filter(|k| k.contains("Callback") || k.contains("send_with_callback"))
            .collect();
        assert_eq!(
            keys,
            [
                "unexpected kafka_producer_Callback_stray",
                "unmapped kafka_producer_Producer_send_with_callback"
            ]
        );
    }

    #[test]
    fn test_interface_lists_supertrait_methods() {
        let ffi = METRICS_FFI
            .lines()
            .filter(|l| {
                !l.contains("fn kafka_common_metrics_MeasurableStat_new(")
                    && !l.contains("fn kafka_common_metrics_MeasurableStat_record(")
            })
            .collect::<Vec<_>>()
            .join("\n");
        let findings = run("metrics-supertraits", METRICS_RUST, &ffi);
        assert_eq!(
            keys(&findings),
            [
                "missing kafka_common_metrics_MeasurableStat_new",
                "missing kafka_common_metrics_MeasurableStat_record",
            ]
        );
        assert_eq!(
            detail(&findings, "missing kafka_common_metrics_MeasurableStat_new"),
            "expected `fn(self: *mut c_void, record: kafka_common_metrics_MeasurableStat_record_fn_t, \
             measure: kafka_common_metrics_MeasurableStat_measure_fn_t) -> *mut kafka_common_metrics_MeasurableStat_t`"
        );
        assert_eq!(
            detail(&findings, "missing kafka_common_metrics_MeasurableStat_record"),
            "expected `fn(self: *const kafka_common_metrics_MeasurableStat_t, value: f64, time_ms: i64)`"
        );
    }

    #[test]
    fn test_enum_payload_trait_is_accepted_and_data_variants_have_constructors() {
        let ffi = METRICS_FFI
            .lines()
            .filter(|l| {
                !l.contains("fn kafka_common_metrics_Gauge_new(")
                    && !l.contains("fn kafka_common_metrics_MetricValueProvider_gauge(")
                    && !l.contains("fn kafka_common_metrics_MetricValueProvider__enum(")
            })
            .collect::<Vec<_>>()
            .join("\n");
        let findings = run("metrics-payload", METRICS_RUST, &ffi);
        assert_eq!(
            keys(&findings),
            [
                "missing kafka_common_metrics_Gauge_new",
                "missing kafka_common_metrics_MetricValueProvider__enum",
                "missing kafka_common_metrics_MetricValueProvider_gauge",
            ]
        );
        assert_eq!(
            detail(&findings, "missing kafka_common_metrics_MetricValueProvider_gauge"),
            "expected `fn(value: *mut kafka_common_metrics_Gauge_t) -> *mut kafka_common_metrics_MetricValueProvider_t`"
        );
        assert_eq!(
            detail(&findings, "missing kafka_common_metrics_MetricValueProvider__enum"),
            "expected `fn(self: *const kafka_common_metrics_MetricValueProvider_t) -> kafka_common_metrics_MetricValueProvider_e`"
        );
    }

    #[test]
    fn test_blanket_impl_yields_as_view() {
        let ffi = METRICS_FFI
            .lines()
            .filter(|l| !l.contains("fn kafka_common_metrics_stats_Avg__as_MeasurableStat("))
            .collect::<Vec<_>>()
            .join("\n");
        let findings = run("metrics-blanket", METRICS_RUST, &ffi);
        assert_eq!(keys(&findings), ["missing kafka_common_metrics_stats_Avg__as_MeasurableStat"]);
        assert_eq!(
            detail(&findings, "missing kafka_common_metrics_stats_Avg__as_MeasurableStat"),
            "expected `fn(self: *const kafka_common_metrics_stats_Avg_t) -> *const kafka_common_metrics_MeasurableStat_t`"
        );
    }

    #[test]
    fn test_unknown_suffix_and_untranslated_type_are_unexpected() {
        let ffi = format!(
            "{TOPIC_PARTITION_FFI}
            #[unsafe(no_mangle)] pub unsafe extern \"C\" fn kafka_common_TopicPartition_hash(this: *const kafka_common_TopicPartition_t) -> i32 {{ 0 }}
            #[repr(C)] pub struct kafka_common_Node_t {{ _p: [u8; 0] }}
            #[unsafe(no_mangle)] pub unsafe extern \"C\" fn kafka_consumer_string_destroy(s: *mut c_char) {{}}
            #[doc(alias = \"rust-only\")]
            #[unsafe(no_mangle)] pub unsafe extern \"C\" fn kafka_string_destroy(s: *mut c_char) {{}}
            #[doc(alias = \"rust-only\")]
            #[repr(C)] pub struct kafka_List_t {{ _p: [u8; 0] }}
            #[unsafe(no_mangle)] pub unsafe extern \"C\" fn kafka_List_size(this: *const kafka_List_t) -> i32 {{ 0 }}"
        );
        let findings = run("unexpected", TOPIC_PARTITION, &ffi);
        assert_eq!(
            keys(&findings),
            [
                "unexpected kafka_common_Node_t",
                "unexpected kafka_common_TopicPartition_hash",
                "unexpected kafka_consumer_string_destroy",
            ]
        );
    }

    #[test]
    fn test_banned_scalars_in_signatures_and_typedefs() {
        let ffi = format!(
            "{TOPIC_PARTITION_FFI}
            #[unsafe(no_mangle)] pub unsafe extern \"C\" fn kafka_common_TopicPartition_is_null(this: *const kafka_common_TopicPartition_t, data: *const u8, len: usize) -> bool {{ false }}
            pub type kafka_common_TopicPartition_done_cb_t = unsafe extern \"C\" fn(ok: bool, opaque: *mut c_void);"
        );
        let findings = run("scalars", TOPIC_PARTITION, &ffi);
        assert_eq!(
            detail(&findings, "scalar kafka_common_TopicPartition_is_null"),
            "uses `bool`, `u8`, `usize`; use the signed fixed-width integer of the same role, `i8` for a boolean"
        );
        assert!(keys(&findings).contains(&"scalar kafka_common_TopicPartition_done_cb_t"));
        // A `u8` inside `kafka_Bytes_t`'s definition is a struct field, never a
        // signature, so it is not scanned.
        let bytes = format!("{TOPIC_PARTITION_FFI}\n#[doc(alias = \"rust-only\")] #[repr(C)] pub struct kafka_Bytes_t {{ data: *const u8, len: i32 }}");
        assert!(run("bytes-struct", TOPIC_PARTITION, &bytes).is_empty());
    }

    #[test]
    fn test_baseline_waives_keys_and_prefixes_and_reports_stale_lines() {
        let baseline = Baseline {
            file: "ffi-baseline.txt".to_string(),
            keys: ["missing kafka_a_B_t".to_string(), "missing kafka_a_B_gone".to_string()].into(),
            prefixes: vec!["kafka_admin_".to_string(), "kafka_nothing_".to_string()],
            duplicates: vec!["missing kafka_a_B_t".to_string()],
        };
        let finding = |kind, symbol: &str| Finding {
            kind,
            symbol: symbol.to_string(),
            file: "f".to_string(),
            detail: String::new(),
        };
        let kept = baseline.apply(vec![
            finding("missing", "kafka_a_B_t"),
            finding("missing", "kafka_admin_Foo_bar"),
            finding("shape", "kafka_a_B_t"),
        ]);
        let keys: Vec<String> = kept.iter().map(Finding::key).collect();
        assert_eq!(
            keys,
            [
                "shape kafka_a_B_t",
                "baseline missing kafka_a_B_gone",
                "baseline prefix:kafka_nothing_",
                "baseline missing kafka_a_B_t",
            ]
        );
    }

    // --- one test per mapping family -------------------------------------

    /// Runs `Mapper::map_type` on `ty` with `TopicPartition` (a struct),
    /// `Serializer` (a trait) and `OffsetSpec` (a unit enum) known.
    fn map(ty: &str, dir: Dir) -> Result<String, String> {
        let by_name: BTreeMap<String, Vec<(String, TypeClass)>> = [
            ("TopicPartition", "kafka_common_TopicPartition", TypeClass::Struct),
            ("Serializer", "kafka_common_serialization_Serializer", TypeClass::Trait),
            ("OffsetSpec", "kafka_admin_OffsetSpec", TypeClass::UnitEnum),
            ("Error", "kafka_common_Error", TypeClass::Struct),
        ]
        .into_iter()
        .map(|(n, p, c)| (n.to_string(), vec![(p.to_string(), c)]))
        .collect();
        let aliases: BTreeMap<String, (BTreeSet<String>, syn::Type)> = [
            ("Callback", BTreeSet::new(), "Box<dyn FnOnce(Option<&TopicPartition>) + Send>"),
            ("PollTask", ["T".to_string()].into(), "Box<dyn Serializer<T>>"),
            ("Topic", BTreeSet::new(), "Arc<str>"),
        ]
        .into_iter()
        .map(|(n, g, t)| (n.to_string(), (g, syn::parse_str(t).unwrap())))
        .collect();
        let error_types = BTreeSet::new();
        let mapper = Mapper {
            by_name: &by_name,
            aliases: &aliases,
            error_types: &error_types,
            owner: "kafka_x_Owner",
            owner_name: "Owner",
            generics: [("K".to_string(), Vec::new())].into(),
        };
        mapper.map_type(&syn::parse_str(ty).unwrap(), dir).map(|c| c.render())
    }

    #[test]
    fn test_mapping_strings() {
        assert_eq!(map("&str", Dir::In).unwrap(), "*const c_char");
        assert_eq!(map("String", Dir::In).unwrap(), "*const c_char");
        assert_eq!(map("Option<&str>", Dir::In).unwrap(), "*const c_char");
        assert_eq!(map("impl Into<String>", Dir::In).unwrap(), "*const c_char");
        assert_eq!(map("impl Into<Arc<str>>", Dir::In).unwrap(), "*const c_char");
        assert_eq!(map("impl AsRef<[u8]>", Dir::In).unwrap(), "kafka_Bytes_t");
        assert_eq!(map("impl std::fmt::Display", Dir::In).unwrap(), "*const c_char");
        assert_eq!(map("impl ToString", Dir::In).unwrap(), "*const c_char");
        assert_eq!(map("&str", Dir::Out).unwrap(), "*const c_char");
        assert_eq!(map("String", Dir::Out).unwrap(), "*mut c_char");
        assert_eq!(map("Arc<str>", Dir::Out).unwrap(), "*mut c_char");
        assert_eq!(map("Option<String>", Dir::Out).unwrap(), "*mut c_char");
    }

    #[test]
    fn test_mapping_collections() {
        assert_eq!(map("Vec<String>", Dir::In).unwrap(), "*const kafka_List_t");
        assert_eq!(map("&[TopicPartition]", Dir::In).unwrap(), "*const kafka_List_t");
        assert_eq!(map("HashSet<String>", Dir::Out).unwrap(), "*mut kafka_List_t");
        assert_eq!(map("impl IntoIterator<Item = String>", Dir::In).unwrap(), "*const kafka_List_t");
        assert_eq!(map("&HashMap<String, String>", Dir::In).unwrap(), "*const kafka_Map_t");
        assert_eq!(map("HashMap<TopicPartition, i64>", Dir::Out).unwrap(), "*mut kafka_Map_t");
        assert_eq!(map("IndexMap<String, i64>", Dir::Out).unwrap(), "*mut kafka_Map_t");
        assert_eq!(map("IndexSet<String>", Dir::In).unwrap(), "*const kafka_List_t");
        assert_eq!(
            map("std::slice::Iter<'_, TopicPartition>", Dir::Out).unwrap(),
            "*mut kafka_List_t"
        );
    }

    #[test]
    fn test_mapping_follows_public_aliases() {
        // A plain alias, a generic one whose parameters become generic
        // arguments, and one over a closure, which stays unmapped.
        assert_eq!(map("Topic", Dir::In).unwrap(), "*const c_char");
        assert_eq!(
            map("PollTask<K>", Dir::In).unwrap(),
            "*mut kafka_common_serialization_Serializer_t"
        );
        assert_eq!(
            map("Callback", Dir::In).unwrap_err(),
            "closure `FnOnce(Option<&TopicPartition>) + Send`"
        );
    }

    #[test]
    fn test_result_with_an_error_payload_type_is_fallible() {
        // One payload is a plain struct, the other comes out of the error
        // macro, which syn sees as a macro invocation rather than a struct.
        let rust = r#"
            pub mod common {
                pub mod error {
                    pub enum Error {
                        LocalIllegalArgument(LocalIllegalArgumentError),
                        LocalIllegalState(LocalIllegalStateError),
                    }
                    pub struct LocalIllegalArgumentError;
                    message_only_error! {
                        #[doc(alias = "rust-only")]
                        LocalIllegalStateError
                    }
                }
                pub use error::{Error, LocalIllegalArgumentError, LocalIllegalStateError};
                type Offsets = HashMap<String, i64>;
                #[doc(alias = "org.apache.kafka.common.Cluster")]
                pub struct Cluster;
                impl Cluster {
                    pub fn with_id(id: &str) -> Result<Self, LocalIllegalArgumentError> { Ok(Cluster) }
                    pub fn set_id(self, id: &str) -> Cluster { self }
                    pub fn check(&self) -> Result<(), LocalIllegalStateError> { Ok(()) }
                    pub fn offsets(&self) -> Offsets { Offsets::new() }
                }
            }
        "#;
        let findings = run("payload-error", rust, "");
        assert_eq!(
            detail(&findings, "missing kafka_common_Cluster_with_id"),
            "expected `fn(id: *const c_char, out_with_id: *mut *mut kafka_common_Cluster_t) -> *mut kafka_common_Error_t`"
        );
        // A fluent setter returning the owner by name mutates the handle in place.
        assert_eq!(
            detail(&findings, "missing kafka_common_Cluster_set_id"),
            "expected `fn(self: *mut kafka_common_Cluster_t, id: *const c_char)`"
        );
        assert_eq!(
            detail(&findings, "missing kafka_common_Cluster_check"),
            "expected `fn(self: *const kafka_common_Cluster_t) -> *mut kafka_common_Error_t`"
        );
        // A private alias is transparent: the signature means its target.
        assert_eq!(
            detail(&findings, "missing kafka_common_Cluster_offsets"),
            "expected `fn(self: *const kafka_common_Cluster_t) -> *mut kafka_Map_t`"
        );
        // The macro's message-only type has no C type of its own.
        assert!(
            !findings.iter().any(|(key, _)| key.contains("LocalIllegalStateError")),
            "{findings:#?}"
        );
    }

    #[test]
    fn test_mapping_bytes_by_value() {
        assert_eq!(map("&[u8]", Dir::In).unwrap(), "kafka_Bytes_t");
        assert_eq!(map("Vec<u8>", Dir::Out).unwrap(), "kafka_Bytes_t");
        assert_eq!(map("Option<Vec<u8>>", Dir::Out).unwrap(), "kafka_Bytes_t");
        assert_eq!(map("&bytes::Bytes", Dir::In).unwrap(), "kafka_Bytes_t");
    }

    #[test]
    fn test_mapping_generics_to_void() {
        assert_eq!(map("K", Dir::In).unwrap(), "*const c_void");
        assert_eq!(map("Option<&K>", Dir::In).unwrap(), "*const c_void");
        assert_eq!(map("K", Dir::Out).unwrap(), "*mut c_void");
    }

    #[test]
    fn test_mapping_options_and_scalars() {
        assert_eq!(map("Option<i64>", Dir::Out).unwrap(), "i64");
        assert_eq!(map("Option<bool>", Dir::In).unwrap(), "i8");
        assert_eq!(map("bool", Dir::Out).unwrap(), "i8");
        assert_eq!(map("usize", Dir::Out).unwrap(), "i32");
        assert_eq!(map("Duration", Dir::In).unwrap(), "i64");
        assert_eq!(map("Option<std::time::Duration>", Dir::In).unwrap(), "i64");
        assert_eq!(
            map("Option<&TopicPartition>", Dir::Out).unwrap(),
            "*const kafka_common_TopicPartition_t"
        );
    }

    #[test]
    fn test_mapping_handles_and_interfaces() {
        assert_eq!(map("&TopicPartition", Dir::In).unwrap(), "*const kafka_common_TopicPartition_t");
        assert_eq!(
            map("&mut TopicPartition", Dir::In).unwrap(),
            "*mut kafka_common_TopicPartition_t"
        );
        // By value in: copied out of the caller's handle. By value out: owned.
        assert_eq!(map("TopicPartition", Dir::In).unwrap(), "*const kafka_common_TopicPartition_t");
        assert_eq!(
            map("Option<TopicPartition>", Dir::In).unwrap(),
            "*const kafka_common_TopicPartition_t"
        );
        assert_eq!(map("TopicPartition", Dir::Out).unwrap(), "*mut kafka_common_TopicPartition_t");
        assert_eq!(map("OffsetSpec", Dir::Out).unwrap(), "*const kafka_admin_OffsetSpec_t");
        assert_eq!(
            map("Box<dyn Serializer<K>>", Dir::In).unwrap(),
            "*mut kafka_common_serialization_Serializer_t"
        );
        assert_eq!(
            map("Arc<dyn Serializer<K>>", Dir::Out).unwrap(),
            "*mut kafka_common_serialization_Serializer_t"
        );
        assert_eq!(
            map("&dyn Serializer<K>", Dir::In).unwrap(),
            "*const kafka_common_serialization_Serializer_t"
        );
        assert_eq!(
            map("impl Serializer<K>", Dir::In).unwrap(),
            "*mut kafka_common_serialization_Serializer_t"
        );
        assert_eq!(map("Error", Dir::In).unwrap(), "*mut kafka_common_Error_t");
        assert_eq!(map("Option<&Error>", Dir::In).unwrap(), "*const kafka_common_Error_t");
        assert_eq!(map("Self", Dir::Out).unwrap(), "*mut kafka_x_Owner_t");
    }

    #[test]
    fn test_unit_enum_self_and_receiver_are_borrowed_singletons() {
        let rust = r#"
            pub mod common {
                pub mod error { pub enum Error { Timeout(TimeoutError) } pub struct TimeoutError; }
                pub use error::Error;
                pub mod record {
                    use crate::common::Error;
                    #[doc(alias = "org.apache.kafka.common.record.TimestampType")]
                    pub enum TimestampType { CreateTime, LogAppendTime }
                    impl TimestampType {
                        pub fn id(self) -> i32 { 0 }
                        pub fn for_name(name: &str) -> Result<Self, Error> { Ok(Self::CreateTime) }
                        pub fn parse(name: &str) -> Self { Self::CreateTime }
                    }
                }
            }
        "#;
        let ffi = r#"
            #[repr(C)] pub struct kafka_common_record_TimestampType_t { _p: [u8; 0] }
            #[repr(C)] pub enum kafka_common_record_TimestampType_e { kafka_common_record_TimestampType_CREATE_TIME, kafka_common_record_TimestampType_LOG_APPEND_TIME }
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_record_TimestampType__enum(this: *const kafka_common_record_TimestampType_t) -> kafka_common_record_TimestampType_e { kafka_common_record_TimestampType_e::kafka_common_record_TimestampType_CREATE_TIME }
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_record_TimestampType_create_time() -> *const kafka_common_record_TimestampType_t { std::ptr::null() }
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_record_TimestampType_log_append_time() -> *const kafka_common_record_TimestampType_t { std::ptr::null() }
        "#;
        let findings = run("unit-enum-self", rust, ffi);
        assert_eq!(
            detail(&findings, "missing kafka_common_record_TimestampType_id"),
            "expected `fn(self: *const kafka_common_record_TimestampType_t) -> i32`"
        );
        assert_eq!(
            detail(&findings, "missing kafka_common_record_TimestampType_parse"),
            "expected `fn(name: *const c_char) -> *const kafka_common_record_TimestampType_t`"
        );
        assert_eq!(
            detail(&findings, "missing kafka_common_record_TimestampType_for_name"),
            "expected `fn(name: *const c_char, out_for_name: *mut *const kafka_common_record_TimestampType_t) -> *mut kafka_common_Error_t`"
        );
        // No `_destroy`: nothing a unit enum returns is owned.
        assert!(
            !findings
                .iter()
                .any(|(key, _)| key == "missing kafka_common_record_TimestampType_destroy"),
            "{findings:#?}"
        );
    }

    #[test]
    fn test_unmapped_types_are_findings() {
        assert_eq!(map("(i32, i64)", Dir::Out).unwrap_err(), "`(i32, i64)`");
        assert_eq!(map("impl Fn(i32) -> i64", Dir::In).unwrap_err(), "closure `Fn(i32) -> i64`");
        assert_eq!(map("[u8; 16]", Dir::In).unwrap_err(), "`[u8; 16]`");
        assert_eq!(map("Instant", Dir::In).unwrap_err(), "`Instant`");
        let rust = r#"
            pub mod common {
                #[doc(alias = "org.apache.kafka.common.Cluster")]
                pub struct Cluster;
                impl Cluster { pub fn parts(&self) -> (i32, i32) { (0, 0) } }
            }
        "#;
        let findings = run("unmapped", rust, "");
        assert_eq!(
            detail(&findings, "unmapped kafka_common_Cluster_parts"),
            "the C shape cannot be derived: return type: `(i32, i32)`"
        );
        assert_eq!(detail(&findings, "missing kafka_common_Cluster_parts"), "expected to exist");
    }

    #[test]
    fn test_prefixes_from_markers_nested_errors_and_rust_only() {
        let ctx = Context { java: Rc::new(index()), ffi_baseline: None, cbindgen_config: None };
        let rule = FfiTranslation::new(&ctx);
        let attrs = |alias: &str| -> Vec<syn::Attribute> {
            let item: syn::ItemStruct = syn::parse_str(&format!("#[doc(alias = \"{alias}\")] struct S;")).unwrap();
            item.attrs
        };
        let module = |path: &str| -> ModPath { path.split("::").map(str::to_string).collect() };
        assert_eq!(
            rule.prefix_of(
                &attrs("org.apache.kafka.clients.admin.CreateTopicsResult$TopicMetadataAndConfig"),
                &module("admin"),
                "TopicMetadataAndConfig"
            ),
            "kafka_admin_CreateTopicsResult_TopicMetadataAndConfig"
        );
        assert_eq!(
            rule.prefix_of(
                &attrs("org.apache.kafka.common.errors.TopicAuthorizationException"),
                &module("common::errors"),
                "TopicAuthorizationError"
            ),
            "kafka_common_TopicAuthorizationError"
        );
        assert_eq!(
            rule.prefix_of(
                &attrs("org.apache.kafka.common.errors.RecordDeserializationException$DeserializationExceptionOrigin"),
                &module("common::errors"),
                "DeserializationErrorOrigin"
            ),
            "kafka_common_RecordDeserializationError_DeserializationErrorOrigin"
        );
        assert_eq!(
            rule.prefix_of(
                &attrs("rust-only"),
                &module("consumer::internals::async_kafka_consumer"),
                "ConsumerHandle"
            ),
            "kafka_consumer_ConsumerHandle"
        );
        assert_eq!(
            rule.prefix_of(&attrs("rust-only"), &module("common::serialization::foo"), "Foo"),
            "kafka_common_serialization_Foo"
        );
    }

    #[test]
    fn test_payload_view_keeps_the_error_suffix_when_the_constructor_owns_the_name() {
        let rust = r#"
            pub mod common {
                pub mod error {
                    pub enum Error {
                        TopicAuthorization(TopicAuthorizationError),
                        ResourceNotFound(ResourceNotFoundError),
                    }
                    impl Error {
                        pub fn topic_authorization(topics: HashSet<String>) -> Self { todo!() }
                    }
                    #[doc(alias = "org.apache.kafka.common.errors.TopicAuthorizationException")]
                    pub struct TopicAuthorizationError;
                    impl TopicAuthorizationError {
                        pub fn unauthorized_topics(&self) -> &HashSet<String> { todo!() }
                    }
                    #[doc(alias = "org.apache.kafka.common.errors.ResourceNotFoundException")]
                    pub struct ResourceNotFoundError;
                    impl ResourceNotFoundError {
                        pub fn resource(&self) -> &str { todo!() }
                    }
                }
                pub use error::{Error, ResourceNotFoundError, TopicAuthorizationError};
            }
        "#;
        let findings = run("view-collision", rust, "");
        assert_eq!(
            detail(&findings, "missing kafka_common_Error_topic_authorization"),
            "expected `fn(topics: *const kafka_List_t) -> *mut kafka_common_Error_t`"
        );
        assert_eq!(
            detail(&findings, "missing kafka_common_Error_topic_authorization_error"),
            "expected `fn(error: *const kafka_common_Error_t) -> *const kafka_common_TopicAuthorizationError_t`"
        );
        // No constructor of that name: the view drops the suffix.
        assert_eq!(
            detail(&findings, "missing kafka_common_Error_resource_not_found"),
            "expected `fn(error: *const kafka_common_Error_t) -> *const kafka_common_ResourceNotFoundError_t`"
        );
        assert!(
            !findings
                .iter()
                .any(|(k, _)| k == "missing kafka_common_Error_resource_not_found_error"),
            "{findings:#?}"
        );
    }

    #[test]
    fn test_the_numeric_error_code_is_accepted_without_a_rust_item() {
        let ffi = r#"
            #[repr(C)] pub struct kafka_common_Error_t { _private: [u8; 0] }
            #[repr(C)] pub enum kafka_common_ErrorCode_e { kafka_common_ErrorCode_NONE = 0 }
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_Error_code(error: *const kafka_common_Error_t) -> kafka_common_ErrorCode_e { kafka_common_ErrorCode_e::kafka_common_ErrorCode_NONE }
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_Error_codes(error: *const kafka_common_Error_t) -> i32 { 0 }
        "#;
        let findings = run("error-code", "", ffi);
        assert_eq!(
            keys(&findings),
            ["unexpected kafka_common_Error_codes", "unexpected kafka_common_Error_t"]
        );
    }

    /// A Rust keyword used as a method name (`ConfigResource::r#type`) is
    /// compared without its `r#` prefix, as cbindgen renders it; a variant
    /// named after a keyword (`PatternType::Match`) needs no raw identifier,
    /// since its enumerator carries the enum's prefix.
    #[test]
    fn test_raw_identifiers_are_compared_without_their_prefix() {
        let rust = r#"
            pub mod common {
                pub mod record {
                    #[doc(alias = "org.apache.kafka.common.record.TimestampType")]
                    pub enum TimestampType { Match, Literal }
                    impl TimestampType {
                        pub fn r#type(self) -> i32 { 0 }
                    }
                }
            }
        "#;
        let ffi = r#"
            #[repr(C)] pub struct kafka_common_record_TimestampType_t { _p: [u8; 0] }
            #[repr(C)] pub enum kafka_common_record_TimestampType_e { kafka_common_record_TimestampType_MATCH, kafka_common_record_TimestampType_LITERAL }
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_record_TimestampType__enum(this: *const kafka_common_record_TimestampType_t) -> kafka_common_record_TimestampType_e { kafka_common_record_TimestampType_e::kafka_common_record_TimestampType_MATCH }
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_record_TimestampType_match() -> *const kafka_common_record_TimestampType_t { std::ptr::null() }
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_record_TimestampType_literal() -> *const kafka_common_record_TimestampType_t { std::ptr::null() }
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_record_TimestampType_type(this: *const kafka_common_record_TimestampType_t) -> i32 { 0 }
        "#;
        let findings = run("raw-identifiers", rust, ffi);
        assert!(findings.is_empty(), "{findings:#?}");
    }

    /// The C enumerators of an enum with a Rust counterpart are
    /// `<prefix>_<VALUE>`: the bare snake-cased value and the `_e_` spelling
    /// cbindgen's `prefix_with_name` produced (`..._e_earliest`) are both
    /// reported, by the per-type comparison and by the enumerator check.
    #[test]
    fn test_enumerators_must_be_prefixed_constant_case() {
        for (variants, bad) in [
            ("earliest, latest, max_timestamp", ["earliest", "latest", "max_timestamp"]),
            (
                "kafka_admin_OffsetSpec_e_earliest, kafka_admin_OffsetSpec_e_latest, kafka_admin_OffsetSpec_e_max_timestamp",
                [
                    "kafka_admin_OffsetSpec_e_earliest",
                    "kafka_admin_OffsetSpec_e_latest",
                    "kafka_admin_OffsetSpec_e_max_timestamp",
                ],
            ),
        ] {
            let ffi = ENUM_FFI
                .replace(
                    "kafka_admin_OffsetSpec_EARLIEST, kafka_admin_OffsetSpec_LATEST, kafka_admin_OffsetSpec_MAX_TIMESTAMP",
                    variants,
                )
                .replace(
                    "kafka_admin_OffsetSpec_e::kafka_admin_OffsetSpec_EARLIEST",
                    &format!("kafka_admin_OffsetSpec_e::{}", bad[0]),
                );
            let findings = run("enumerator-case", ENUM_RUST, &ffi);
            let mut expected = vec!["shape kafka_admin_OffsetSpec_e".to_string()];
            expected.extend(bad.iter().map(|b| format!("enumerator {b}")));
            expected.sort();
            assert_eq!(keys(&findings), expected.iter().map(String::as_str).collect::<Vec<_>>(), "{variants}");
            assert_eq!(
                detail(&findings, &format!("enumerator {}", bad[0])),
                "expected `kafka_admin_OffsetSpec_<VALUE>`: the enum's prefix without `_e`, then the key in constant case"
            );
        }
    }

    /// A C-only enum (`kafka_common_ErrorCode_e`, no Rust counterpart, so no
    /// per-type comparison) is still held to the enumerator rule, and a
    /// `cbindgen:prefix-with-name=true` annotation on any `_e` enum is
    /// reported.
    #[test]
    fn test_c_only_enum_enumerators_and_prefix_annotation() {
        let ffi = r#"
            #[repr(C)] pub struct kafka_common_Error_t { _private: [u8; 0] }
            /// cbindgen:prefix-with-name=true
            #[repr(C)] pub enum kafka_common_ErrorCode_e { NONE = 0, kafka_common_ErrorCode_e_UNKNOWN = -1, kafka_common_ErrorCode_offset_out_of_range = 1, kafka_common_ErrorCode_CORRUPT_MESSAGE = 2 }
            #[unsafe(no_mangle)] pub unsafe extern "C" fn kafka_common_Error_code(error: *const kafka_common_Error_t) -> kafka_common_ErrorCode_e { kafka_common_ErrorCode_e::NONE }
        "#;
        let findings = run("c-only-enumerators", "", ffi);
        assert_eq!(
            keys(&findings),
            [
                "enumerator NONE",
                "enumerator kafka_common_ErrorCode_e",
                "enumerator kafka_common_ErrorCode_e_UNKNOWN",
                "enumerator kafka_common_ErrorCode_offset_out_of_range",
                "unexpected kafka_common_Error_t",
            ]
        );
        assert_eq!(
            detail(&findings, "enumerator kafka_common_ErrorCode_e"),
            "`cbindgen:prefix-with-name=true` would prefix every enumerator with the `_e` export name; \
             spell the enumerators `<prefix>_<VALUE>` in full instead"
        );
    }

    /// `[enum] prefix_with_name = true` in the cbindgen configuration is
    /// reported; `false`, a commented-out `true`, the same key in another
    /// section, and a missing file are not.
    #[test]
    fn test_cbindgen_enum_prefixing_is_reported() {
        let dir = std::env::temp_dir().join(format!("xtask-ffi-cbindgen-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let config = dir.join("cbindgen.toml");
        let check = |text: &str| {
            fs::write(&config, text).unwrap();
            let mut findings = Vec::new();
            cbindgen_enum_prefixing(&config, &mut findings);
            findings
                .into_iter()
                .map(|f| format!("{} {}", f.kind, f.symbol))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            check("[enum]\nrename_variants = \"None\"\nprefix_with_name = true\n"),
            ["config prefix_with_name"]
        );
        assert!(check("[enum]\nprefix_with_name = false\n").is_empty());
        assert!(check("[enum]\n# prefix_with_name = true\n").is_empty());
        assert!(check("[struct]\nprefix_with_name = true\n[enum]\n").is_empty());
        fs::remove_dir_all(&dir).unwrap();
        let mut findings = Vec::new();
        cbindgen_enum_prefixing(&config, &mut findings);
        assert!(findings.is_empty());
    }
}
