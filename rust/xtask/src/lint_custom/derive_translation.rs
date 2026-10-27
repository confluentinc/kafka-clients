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

//! The two rules of CLAUDE.md §3 "Super and auto traits".
//!
//! `check-derive-translation`: every public struct and enum outside `ffi`
//! carries the standard traits its Java class implies, and no other:
//!
//!   - `Debug`: always;
//!   - `PartialEq + Eq`, `Hash`: iff Java overrides `equals(Object)` /
//!     `hashCode()`, in the class or a superclass of the client tree
//!     (`AbstractConfig`), a Java enum or record counting as overriding both;
//!   - `PartialOrd + Ord`: iff Java implements `Comparable` or is an enum;
//!   - `Display`: iff Java overrides `toString()` (likewise inherited), is an
//!     enum or a record, or is a `Throwable`;
//!   - `Copy`: never on a struct but those of [`COPY_STRUCTS`], never on an enum
//!     with a data-carrying variant;
//!   - `Clone`: never on a client (an implementor of a
//!     [`CLIENT_TRAITS`] trait) or a [`CONFIG_HOLDERS`] type.
//!
//! A trait counts when it is derived, delegated through ambassador's
//! `#[delegate(..)]`, or implemented by an `impl Trait for Type` anywhere in the
//! crate outside `#[cfg(test)]`. A type tagged `rust-only`, or with no class
//! marker, has no Java class to follow and gets only the Java-independent
//! checks (`Debug`, `Copy`, `Clone`); a marker the Java index cannot resolve is
//! `check-java-name`'s and the type is not checked. The error types
//! `kafka_error_type!` / `message_only_error!` define are not items the rule
//! sees: the macros give each `Clone, Debug` and a `Display`, and `Throwable`
//! has no `equals`, so they comply by construction.
//!
//! `check-thread-safety-probe`: [`API_PROMISES`] asserts the auto traits of
//! every public struct and enum outside `ffi`, through `is_send!` /
//! `is_sync!`. The rule checks presence only: instantiating a generic type,
//! choosing a lifetime and stating why a consumer is `!Sync` are a person's
//! choices, so the finding says which line to add instead of generating it.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use syn::punctuated::Punctuated;
use syn::visit::Visit;

use super::ffi_translation::CLIENT_TRAITS;
use super::{has_tag, is_cfg_test, java_markers, type_ident, Context, Crate, DynForwarding, ModPath, Rule, RUST_ONLY};
use crate::java::{self, JavaIndex};

/// The structs that may be `Copy`, with the reason.
const COPY_STRUCTS: &[(&str, &str)] = &[(
    "Uuid",
    "a 128-bit value by definition, as std's `Ipv6Addr`; the message generator returns `Uuid` fields by value",
)];

/// The configuration holders, which are never `Clone`: one is built per
/// client from its own properties, as Java builds one per client.
const CONFIG_HOLDERS: &[&str] = &[
    "ProducerConfig",
    "ConsumerConfig",
    "AdminClientConfig",
    "SslConfigs",
    "SaslConfigs",
];

/// The file asserting the auto traits of the public types, relative to `rust/`.
pub(super) const API_PROMISES: &str = "src/api_promises.rs";

/// The standard traits a type carries, by its simple name: every
/// `impl Trait for Type` of the crate, keyed by the type's name, with the
/// module the impl is written in.
struct TraitImpls {
    by_type: BTreeMap<String, Vec<(ModPath, String)>>,
    /// How many structs and enums of each name the crate defines, of any
    /// visibility: an impl of a unique name is that type's wherever it is
    /// written; one of a shared name (`Builder`) only counts in the defining
    /// module.
    definitions: BTreeMap<String, usize>,
}

impl TraitImpls {
    fn new(krate: &Crate) -> Self {
        let mut by_type: BTreeMap<String, Vec<(ModPath, String)>> = BTreeMap::new();
        let mut definitions: BTreeMap<String, usize> = BTreeMap::new();
        for (path, module) in &krate.modules {
            for item in &module.items {
                match item {
                    syn::Item::Struct(s) => *definitions.entry(s.ident.to_string()).or_default() += 1,
                    syn::Item::Enum(e) => *definitions.entry(e.ident.to_string()).or_default() += 1,
                    syn::Item::Impl(imp) => {
                        let Some((bang, trait_path, _)) = &imp.trait_ else {
                            continue;
                        };
                        if bang.is_some() || is_cfg_test(&imp.attrs) || DynForwarding::is_blanket(imp) {
                            continue;
                        }
                        let (Some(ty), Some(tr)) = (type_ident(&imp.self_ty), trait_path.segments.last()) else {
                            continue;
                        };
                        by_type.entry(ty).or_default().push((path.clone(), tr.ident.to_string()));
                    },
                    _ => {},
                }
            }
        }
        TraitImpls { by_type, definitions }
    }

    /// The traits implemented for type `name` defined in `module`.
    fn of(&self, module: &[String], name: &str) -> BTreeSet<String> {
        let unique = self.definitions.get(name).copied().unwrap_or_default() <= 1;
        self.by_type
            .get(name)
            .into_iter()
            .flatten()
            .filter(|(m, _)| unique || m == module)
            .map(|(_, t)| t.clone())
            .collect()
    }
}

/// The last segments of the paths `attrs`' `#[derive(..)]`s name.
fn derives(attrs: &[syn::Attribute]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for attr in attrs.iter().filter(|a| a.path().is_ident("derive")) {
        if let Ok(paths) = attr.parse_args_with(Punctuated::<syn::Path, syn::Token![,]>::parse_terminated) {
            out.extend(paths.iter().filter_map(|p| p.segments.last()).map(|s| s.ident.to_string()));
        }
    }
    out
}

/// The traits `attrs`' ambassador `#[delegate(Trait, ..)]`s forward.
fn delegated(attrs: &[syn::Attribute]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for attr in attrs.iter().filter(|a| a.path().is_ident("delegate")) {
        if let Ok(metas) = attr.parse_args_with(Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated) {
            if let Some(syn::Meta::Path(p)) = metas.first() {
                out.extend(p.segments.last().map(|s| s.ident.to_string()));
            }
        }
    }
    out
}

/// The first pair of `variants`, in Rust declaration order, whose Java
/// constants `constants` declares the other way round. A variant and a
/// constant match when their names agree with underscores dropped and case
/// folded (`ScramSha256`, `SCRAM_SHA_256`); a variant with no constant is not
/// compared.
fn out_of_java_order<'v>(variants: &'v [String], constants: &[String]) -> Option<(&'v str, &'v str)> {
    let fold = |s: &str| s.replace('_', "").to_ascii_lowercase();
    let folded: Vec<String> = constants.iter().map(|c| fold(c)).collect();
    let mut previous: Option<(&str, usize)> = None;
    for variant in variants {
        let Some(at) = folded.iter().position(|c| *c == fold(variant)) else {
            continue;
        };
        if let Some((earlier, before)) = previous {
            if at < before {
                return Some((earlier, variant));
            }
        }
        previous = Some((variant, at));
    }
    None
}

/// A public struct or enum outside `ffi`, with what the rules read of it.
struct PublicType<'a> {
    module: &'a ModPath,
    name: String,
    file: &'a Path,
    attrs: &'a [syn::Attribute],
    is_struct: bool,
    /// For an enum: whether a variant carries data.
    carries_data: bool,
    /// For an enum: its variant names, in declaration order.
    variants: Vec<String>,
}

/// Every public struct and enum outside `ffi`, in (module, name) order.
fn public_structs_and_enums(krate: &Crate) -> Vec<PublicType<'_>> {
    let mut out = Vec::new();
    for (module, name) in krate.public_types() {
        if module.first().is_some_and(|m| m == "ffi") {
            continue;
        }
        let (module, m) = krate
            .modules
            .get_key_value(&module)
            .expect("public_types only returns loaded modules");
        let (attrs, is_struct, carries_data, variants) = match krate.type_item(module, &name) {
            syn::Item::Struct(s) => (&s.attrs[..], true, false, Vec::new()),
            syn::Item::Enum(e) => (
                &e.attrs[..],
                false,
                e.variants.iter().any(|v| !matches!(v.fields, syn::Fields::Unit)),
                e.variants.iter().map(|v| v.ident.to_string()).collect(),
            ),
            _ => continue,
        };
        out.push(PublicType { module, name, file: &m.file, attrs, is_struct, carries_data, variants });
    }
    out
}

pub(super) struct DeriveTranslation {
    java: Rc<JavaIndex>,
}

impl DeriveTranslation {
    pub(super) fn new(ctx: &Context) -> Self {
        DeriveTranslation { java: Rc::clone(&ctx.java) }
    }

    /// The Java class the type translates: its first class marker the index
    /// resolves. `Err` for a marker it cannot resolve, which leaves the type
    /// unchecked; `Ok(None)` for a type with no class marker.
    fn class(&self, attrs: &[syn::Attribute]) -> Result<Option<(String, &java::JavaClass)>, ()> {
        let markers: Vec<String> = java_markers(attrs).into_iter().filter(|m| !m.contains('#')).collect();
        if markers.is_empty() {
            return Ok(None);
        }
        markers
            .into_iter()
            .find_map(|m| self.java.resolve(&m).map(|c| (m, c)))
            .map(Some)
            .ok_or(())
    }

    /// The Java-dependent findings for `ty`, carrying `traits`, against `class`
    /// (named by `marker`).
    fn check_java(
        &self,
        ty: &PublicType<'_>,
        traits: &BTreeSet<String>,
        marker: &str,
        class: &java::JavaClass,
        findings: &mut Vec<String>,
    ) {
        let file = ty.file.display();
        let name = ty.name.as_str();
        let enum_or_record = matches!(class.kind, java::JavaKind::Enum | java::JavaKind::Record);
        let what = match class.kind {
            java::JavaKind::Enum => "a Java enum",
            java::JavaKind::Record => "a Java record",
            _ => "",
        };
        let overriding = |method: &str, params: &[&str]| -> Option<String> {
            if enum_or_record {
                return Some(format!("`{marker}` is {what}"));
            }
            self.java.declaring(class, |c| c.declares(method, params)).map(|d| {
                if d.marker() == class.marker() {
                    format!("`{marker}` overrides `{method}`")
                } else {
                    format!("`{marker}` inherits `{method}` from `{}`", d.marker())
                }
            })
        };

        // A pair of traits required or forbidden together.
        let mut pair = |pair: &[&str], why: Option<String>, missing_fix: &str, extra: &str| {
            let present: Vec<&str> = pair.iter().copied().filter(|t| traits.contains(*t)).collect();
            match why {
                Some(why) if present.len() < pair.len() => {
                    let lacking: Vec<&str> = pair.iter().copied().filter(|t| !present.contains(t)).collect();
                    findings.push(format!(
                        "{file}: `{name}` lacks `{}` but {why}; {missing_fix}",
                        lacking.join(", ")
                    ));
                },
                None if !present.is_empty() => {
                    findings.push(format!(
                        "{file}: `{name}` has `{}` but {extra}; remove {}",
                        present.join(", "),
                        if present.len() == 1 { "it" } else { "them" }
                    ));
                },
                _ => {},
            }
        };

        pair(
            &["PartialEq", "Eq"],
            overriding("equals", &["Object"]),
            "derive them, or hand-write them over the fields Java compares (`to_bits()` for an `f64`)",
            &format!("`{marker}` does not override `equals`"),
        );
        pair(
            &["Hash"],
            overriding("hashCode", &[]),
            "derive it, or hash exactly the fields `PartialEq` compares",
            &format!("`{marker}` does not override `hashCode`"),
        );
        let comparable = if class.kind == java::JavaKind::Enum {
            Some(format!("`{marker}` is a Java enum, ordered by declaration"))
        } else {
            self.java
                .is_comparable(class)
                .then(|| format!("`{marker}` implements `Comparable`"))
        };
        pair(
            &["PartialOrd", "Ord"],
            comparable,
            "derive them (variants in Java declaration order), or hand-write them as Java's `compareTo`",
            &format!("`{marker}` is neither `Comparable` nor an enum"),
        );
        let to_string = if self.java.is_throwable(class) {
            Some(format!("`{marker}` is a `Throwable`"))
        } else {
            overriding("toString", &[])
        };
        pair(
            &["Display"],
            to_string,
            "implement it with Java's `toString()` format and add the `<prefix>_to_string` C export",
            &format!("`{marker}` does not override `toString()`"),
        );

        // A derived `Ord` orders by variant declaration, so it is Java's
        // `Enum.compareTo` only while the variants keep the constants' order.
        if class.kind == java::JavaKind::Enum && derives(ty.attrs).contains("Ord") {
            if let Some((earlier, later)) = out_of_java_order(&ty.variants, &class.enum_constants) {
                findings.push(format!(
                    "{file}: `{name}` derives `Ord` but declares `{earlier}` before `{later}`, which `{marker}` \
                     declares the other way round; reorder the variants as Java's constants, since a derived \
                     `Ord` must order them as `Enum.compareTo` does"
                ));
            }
        }
    }
}

impl Rule for DeriveTranslation {
    fn name(&self) -> &'static str {
        "check-derive-translation"
    }

    fn check(&self, krate: &Crate, findings: &mut Vec<String>) -> usize {
        let impls = TraitImpls::new(krate);
        let mut clients: BTreeSet<String> = BTreeSet::new();
        for (ty, list) in &impls.by_type {
            if list.iter().any(|(_, t)| CLIENT_TRAITS.contains(&t.as_str())) {
                clients.insert(ty.clone());
            }
        }

        let mut checked = 0usize;
        for ty in public_structs_and_enums(krate) {
            let class = if has_tag(ty.attrs, RUST_ONLY) {
                Ok(None)
            } else {
                self.class(ty.attrs)
            };
            let Ok(class) = class else { continue };
            checked += 1;
            let mut traits = derives(ty.attrs);
            traits.extend(delegated(ty.attrs));
            traits.extend(impls.of(ty.module, &ty.name));
            let file = ty.file.display();
            let name = ty.name.as_str();

            if !traits.contains("Debug") {
                findings.push(format!(
                    "{file}: `{name}` has no `Debug`; derive it, or hand-write one printing its identifiers"
                ));
            }
            if traits.contains("Copy") {
                if ty.is_struct && !COPY_STRUCTS.iter().any(|(n, _)| *n == name) {
                    findings.push(format!(
                        "{file}: `{name}` is `Copy`; a public struct is never `Copy`, remove it"
                    ));
                } else if ty.carries_data {
                    findings.push(format!(
                        "{file}: `{name}` is `Copy` but has a data-carrying variant; remove `Copy`"
                    ));
                }
            }
            if traits.contains("Clone") {
                if clients.contains(name) {
                    findings.push(format!("{file}: `{name}` is a client and is `Clone`; remove `Clone`"));
                } else if CONFIG_HOLDERS.contains(&name) {
                    findings.push(format!(
                        "{file}: `{name}` is a configuration holder and is `Clone`; remove `Clone`"
                    ));
                }
            }
            if let Some((marker, class)) = class {
                self.check_java(&ty, &traits, &marker, class, findings);
            }
        }
        checked
    }

    fn hint(&self) -> &'static str {
        "   CLAUDE.md §3 \"Super and auto traits\": `Debug` everywhere; `PartialEq + Eq`, `Hash`, `Display` iff
   Java overrides `equals` / `hashCode` / `toString` (a superclass of the client tree counts, an enum
   or record counts, a `Throwable` always has `Display`); `PartialOrd + Ord` iff `Comparable` or an
   enum; `Copy` never on a struct but `Uuid`; `Clone` never on a client or a `*Config` holder. A new
   `Display` also needs its C `_to_string` export (`check-ffi-translation`)."
    }

    fn skip_reason(&self) -> Option<String> {
        self.java.is_empty().then(|| {
            format!(
                "no Java sources at `{}` (run `git submodule update --init kafka`)",
                java::JAVA_MAIN_ROOT
            )
        })
    }
}

/// The `is_send!` / `is_sync!` invocations of a file: for each simple type
/// name, the distinct paths it is written with.
#[derive(Default)]
struct Probes {
    send: BTreeMap<String, BTreeSet<String>>,
    sync: BTreeMap<String, BTreeSet<String>>,
}

impl Probes {
    fn record(&mut self, macro_name: &str, ty: &syn::Type) {
        let Some((name, path)) = probed(ty) else { return };
        let map = if macro_name == "is_send" {
            &mut self.send
        } else {
            &mut self.sync
        };
        map.entry(name).or_default().insert(path);
    }

    fn names(&self) -> BTreeSet<&String> {
        self.send.keys().chain(self.sync.keys()).collect()
    }
}

impl<'ast> Visit<'ast> for Probes {
    fn visit_macro(&mut self, mac: &'ast syn::Macro) {
        let Some(last) = mac.path.segments.last() else { return };
        let macro_name = last.ident.to_string();
        if macro_name == "is_send" || macro_name == "is_sync" {
            if let Ok(ty) = mac.parse_body::<syn::Type>() {
                self.record(&macro_name, &ty);
            }
            return;
        }
        // `assert!(is_send!(T) && is_sync!(T), ..)`: the probes sit in the
        // tokens of another macro, which `syn` does not parse on its own.
        if let Ok(exprs) = mac.parse_body_with(Punctuated::<syn::Expr, syn::Token![,]>::parse_terminated) {
            for e in &exprs {
                Visit::visit_expr(self, e);
            }
        }
    }
}

/// The simple name and written path of the type a probe names, looking
/// through `Box` / `Arc` / `Rc`, references and `dyn` (its first bound).
fn probed(ty: &syn::Type) -> Option<(String, String)> {
    match ty {
        syn::Type::Path(p) => {
            let last = p.path.segments.last()?;
            if matches!(last.ident.to_string().as_str(), "Box" | "Arc" | "Rc") {
                if let syn::PathArguments::AngleBracketed(args) = &last.arguments {
                    if let Some(syn::GenericArgument::Type(inner)) = args.args.first() {
                        return probed(inner);
                    }
                }
            }
            let path = p
                .path
                .segments
                .iter()
                .map(|s| s.ident.to_string())
                .collect::<Vec<_>>()
                .join("::");
            Some((last.ident.to_string(), path))
        },
        syn::Type::TraitObject(t) => t.bounds.iter().find_map(|b| match b {
            syn::TypeParamBound::Trait(tb) => {
                let last = tb.path.segments.last()?;
                let path = tb
                    .path
                    .segments
                    .iter()
                    .map(|s| s.ident.to_string())
                    .collect::<Vec<_>>()
                    .join("::");
                Some((last.ident.to_string(), path))
            },
            _ => None,
        }),
        syn::Type::Reference(r) => probed(&r.elem),
        syn::Type::Paren(p) => probed(&p.elem),
        syn::Type::Group(g) => probed(&g.elem),
        _ => None,
    }
}

pub(super) struct ThreadSafetyProbe {
    /// [`API_PROMISES`], or a fixture's.
    file: PathBuf,
}

impl ThreadSafetyProbe {
    pub(super) fn new() -> Self {
        ThreadSafetyProbe { file: PathBuf::from(API_PROMISES) }
    }
}

impl Rule for ThreadSafetyProbe {
    fn name(&self) -> &'static str {
        "check-thread-safety-probe"
    }

    fn check(&self, krate: &Crate, findings: &mut Vec<String>) -> usize {
        let probe_file = self.file.display();
        let source = match fs::read_to_string(&self.file) {
            Ok(s) => s,
            Err(e) => {
                findings.push(format!(
                    "{probe_file}: cannot read it ({e}); it asserts the auto traits of every public type"
                ));
                return 0;
            },
        };
        let parsed = match syn::parse_file(&source) {
            Ok(f) => f,
            Err(e) => {
                findings.push(format!("{probe_file}: cannot parse it ({e})"));
                return 0;
            },
        };
        let mut probes = Probes::default();
        probes.visit_file(&parsed);

        let types = public_structs_and_enums(krate);
        let mut by_name: BTreeMap<&str, Vec<&PublicType<'_>>> = BTreeMap::new();
        for ty in &types {
            by_name.entry(ty.name.as_str()).or_default().push(ty);
        }
        for (name, defs) in &by_name {
            let wanted = defs.len();
            let sends = probes.send.get(*name).map_or(0, BTreeSet::len);
            let syncs = probes.sync.get(*name).map_or(0, BTreeSet::len);
            if sends >= wanted && syncs >= wanted {
                continue;
            }
            let missing = match (sends >= wanted, syncs >= wanted) {
                (false, false) => "`is_send!` and `is_sync!`",
                (false, true) => "`is_send!`",
                _ => "`is_sync!`",
            };
            if wanted == 1 {
                findings.push(format!(
                    "{probe_file}: `{name}` ({}) has no {missing} assertion; add `assert!(is_send!({name}) && \
                     is_sync!({name}));`, or for a consumer `!is_sync!` with the reason",
                    defs[0].file.display()
                ));
            } else {
                let files: Vec<String> = defs.iter().map(|d| d.file.display().to_string()).collect();
                findings.push(format!(
                    "{probe_file}: `{name}` is defined {wanted} times ({}) but {missing} names it by fewer distinct \
                     paths; probe each definition by its own path",
                    files.join(", ")
                ));
            }
        }

        let traits: BTreeSet<String> = krate
            .public_types()
            .into_iter()
            .filter(|(m, _)| m.first().is_none_or(|f| f != "ffi"))
            .map(|(_, n)| n)
            .collect();
        for name in probes.names() {
            if !traits.contains(name.as_str()) {
                findings.push(format!(
                    "{probe_file}: `{name}` is probed but is no public type of the crate; remove its assertion"
                ));
            }
        }
        types.len()
    }

    fn hint(&self) -> &'static str {
        "   CLAUDE.md §3 \"Super and auto traits\": every public type is `Send + Sync` but the consumers, which
   are `Send + !Sync`. `src/api_promises.rs` pins that with one assertion per public struct and enum,
   so a field that silently drops `Sync` fails a test instead of a downstream build."
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    /// A main class of `package` (below `org.apache.kafka`) declaring the
    /// instance methods `methods` (name, parameter types) and the static ones
    /// `statics`.
    fn class(
        package: &str,
        path: &[&str],
        kind: java::JavaKind,
        supertypes: &[&str],
        methods: &[(&str, &[&str])],
        statics: &[(&str, &[&str])],
    ) -> java::JavaClass {
        let overload = |(name, params): &(&str, &[&str]), is_static: bool| java::Overload {
            name: name.to_string(),
            params: params.iter().map(|p| p.to_string()).collect(),
            deprecated: false,
            visibility: java::Visibility::Public,
            is_static,
            is_default: false,
            param_names: params.iter().map(|_| "o".to_string()).collect(),
            returns_class: false,
            javadoc: None,
        };
        java::JavaClass {
            module: package.split('.').filter(|s| *s != "clients").map(str::to_string).collect(),
            package: package.to_string(),
            path: path.iter().map(|s| s.to_string()).collect(),
            methods: methods.iter().chain(statics).map(|(n, _)| n.to_string()).collect(),
            overloads: methods
                .iter()
                .map(|m| overload(m, false))
                .chain(statics.iter().map(|m| overload(m, true)))
                .collect(),
            fields: BTreeMap::new(),
            field_visibility: BTreeMap::new(),
            enum_constants: Vec::new(),
            deprecated: false,
            is_test: false,
            kind,
            visibility: java::Visibility::Public,
            is_abstract: false,
            functional_interface: false,
            supertypes: supertypes.iter().map(|s| s.to_string()).collect(),
            javadoc: None,
            field_docs: BTreeMap::new(),
        }
    }

    /// `equals(Object)`, `hashCode()` and `toString()`.
    const VALUE: &[(&str, &[&str])] = &[("equals", &["Object"]), ("hashCode", &[]), ("toString", &[])];

    /// Writes `files` (name, content) into a fresh directory and loads its
    /// `lib.rs`.
    fn fixture(name: &str, files: &[(&str, &str)]) -> (PathBuf, Crate) {
        let dir = std::env::temp_dir().join(format!("xtask-{name}-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        for (file, content) in files {
            fs::write(dir.join(file), content).unwrap();
        }
        let krate = Crate::load(&dir.join("lib.rs")).unwrap();
        (dir, krate)
    }

    const DERIVE_LIB: &str = r#"
        pub trait Producer {}

        #[doc(alias = "org.apache.kafka.common.TopicPartition")]
        #[derive(Clone, Debug, PartialEq, Eq, Hash)]
        pub struct TopicPartition;
        impl std::fmt::Display for TopicPartition {}

        #[doc(alias = "org.apache.kafka.clients.admin.NewTopic")]
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub struct NewTopic;

        #[doc(alias = "org.apache.kafka.common.record.TimestampType")]
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub enum TimestampType { NoTimestampType, CreateTime }
        impl std::fmt::Display for TimestampType {}

        // `Unknown` has no Java constant and is not compared.
        #[doc(alias = "org.apache.kafka.common.IsolationLevel")]
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum IsolationLevel { Unknown, ReadCommitted, ReadUncommitted }
        impl std::fmt::Display for IsolationLevel {}

        #[doc(alias = "org.apache.kafka.common.GroupType")]
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum GroupType { Unknown, Consumer, Classic }
        impl std::fmt::Display for GroupType {}

        #[doc(alias = "org.apache.kafka.common.errors.TimeoutException")]
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub struct TimeoutError;
        impl Display for TimeoutError {}

        #[doc(alias = "org.apache.kafka.clients.admin.CreateTopicsOptions")]
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub struct CreateTopicsOptions;

        #[doc(alias = "org.apache.kafka.clients.producer.ProducerConfig")]
        #[derive(Clone, Debug)]
        pub struct ProducerConfig;
        impl PartialEq for ProducerConfig {}
        impl Eq for ProducerConfig {}
        impl std::hash::Hash for ProducerConfig {}

        #[doc(alias = "org.apache.kafka.clients.producer.KafkaProducer")]
        #[derive(Clone)]
        pub struct KafkaProducer<K, V>(K, V);
        impl<K, V> Producer<K, V> for KafkaProducer<K, V> {}
        // A blanket impl is no type's.
        impl<T: Producer> std::fmt::Debug for T {}

        #[doc(alias = "org.apache.kafka.common.metrics.stats.Avg")]
        #[derive(Debug)]
        pub struct Avg;

        #[doc(alias = "org.apache.kafka.common.Uuid")]
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub struct Uuid;
        impl std::fmt::Display for Uuid {}

        #[doc(alias = "org.apache.kafka.clients.admin.DeletedRecords")]
        #[derive(Clone, Copy, Debug)]
        pub struct DeletedRecords;

        #[doc(alias = "org.apache.kafka.clients.admin.OffsetSpec")]
        #[derive(Clone, Copy, Debug)]
        pub enum OffsetSpec { Earliest, Timestamp(DeletedRecords) }

        #[doc(alias = "org.apache.kafka.common.Helper")]
        #[derive(Debug, PartialEq, Eq)]
        pub struct Helper;

        // Rust-only: the Java-dependent checks do not apply.
        #[doc(alias = "rust-only")]
        #[derive(PartialEq)]
        pub struct ConsumerHandle;

        // A marker the index cannot resolve is `check-java-name`'s.
        #[doc(alias = "org.apache.kafka.common.Missing")]
        pub struct Missing;

        pub mod ffi {
            pub struct kafka_common_Uuid_t;
        }
    "#;

    #[test]
    fn test_derives_follow_the_java_class() {
        use java::JavaKind::{Class, Enum};
        let (dir, krate) = fixture("derive-translation", &[("lib.rs", DERIVE_LIB)]);
        let java_enum = |package: &str, name: &str, constants: &[&str]| java::JavaClass {
            enum_constants: constants.iter().map(|c| c.to_string()).collect(),
            ..class(package, &[name], Enum, &[], &[], &[])
        };
        let rule = DeriveTranslation {
            java: Rc::new(JavaIndex {
                classes: vec![
                    class("common", &["TopicPartition"], Class, &[], VALUE, &[]),
                    class("clients.admin", &["NewTopic"], Class, &[], VALUE, &[]),
                    class("common.record", &["TimestampType"], Enum, &[], &[], &[]),
                    java_enum("common", "IsolationLevel", &["READ_UNCOMMITTED", "READ_COMMITTED"]),
                    java_enum("common", "GroupType", &["UNKNOWN", "CONSUMER", "CLASSIC"]),
                    class("common.errors", &["TimeoutException"], Class, &["RetriableException"], &[], &[]),
                    class("common.errors", &["RetriableException"], Class, &["ApiException"], &[], &[]),
                    class("common.errors", &["ApiException"], Class, &["KafkaException"], &[], &[]),
                    class("common", &["KafkaException"], Class, &["RuntimeException"], &[], &[]),
                    class("clients.admin", &["CreateTopicsOptions"], Class, &["AbstractOptions"], &[], &[]),
                    class("clients.admin", &["AbstractOptions"], Class, &[], &[], &[]),
                    class("clients.producer", &["ProducerConfig"], Class, &["AbstractConfig"], &[], &[]),
                    class("common.config", &["AbstractConfig"], Class, &[], &VALUE[..2], &[]),
                    class("clients.producer", &["KafkaProducer"], Class, &["Producer"], &[], &[]),
                    class(
                        "clients.producer",
                        &["Producer"],
                        java::JavaKind::Interface,
                        &["Closeable"],
                        &[],
                        &[],
                    ),
                    class("common.metrics.stats", &["Avg"], Class, &["SampledStat"], &[], &[]),
                    class(
                        "common.metrics.stats",
                        &["SampledStat"],
                        Class,
                        &["MeasurableStat"],
                        &VALUE[2..],
                        &[],
                    ),
                    class("common", &["Uuid"], Class, &["Comparable"], VALUE, &[]),
                    class("clients.admin", &["DeletedRecords"], Class, &[], &[], &[]),
                    class("clients.admin", &["OffsetSpec"], Class, &[], &[], &[]),
                    // A static helper named `equals` overrides nothing.
                    class("common", &["Helper"], Class, &[], &[], &[("equals", &["Object"])]),
                ],
            }),
        };
        let mut findings = Vec::new();
        let checked = rule.check(&krate, &mut findings);
        fs::remove_dir_all(&dir).unwrap();
        let file = dir.join("lib.rs").display().to_string();
        let display_fix = "implement it with Java's `toString()` format and add the `<prefix>_to_string` C export";
        let ord_fix = "derive them (variants in Java declaration order), or hand-write them as Java's `compareTo`";
        assert_eq!(
            findings,
            [
                format!(
                    "{file}: `Avg` lacks `Display` but `org.apache.kafka.common.metrics.stats.Avg` inherits `toString` \
                     from `org.apache.kafka.common.metrics.stats.SampledStat`; {display_fix}"
                ),
                format!(
                    "{file}: `ConsumerHandle` has no `Debug`; derive it, or hand-write one printing its identifiers"
                ),
                format!(
                    "{file}: `CreateTopicsOptions` has `PartialEq, Eq` but \
                     `org.apache.kafka.clients.admin.CreateTopicsOptions` does not override `equals`; remove them"
                ),
                format!("{file}: `DeletedRecords` is `Copy`; a public struct is never `Copy`, remove it"),
                format!(
                    "{file}: `Helper` has `PartialEq, Eq` but `org.apache.kafka.common.Helper` does not override \
                     `equals`; remove them"
                ),
                format!(
                    "{file}: `IsolationLevel` derives `Ord` but declares `ReadCommitted` before `ReadUncommitted`, \
                     which `org.apache.kafka.common.IsolationLevel` declares the other way round; reorder the \
                     variants as Java's constants, since a derived `Ord` must order them as `Enum.compareTo` does"
                ),
                format!(
                    "{file}: `KafkaProducer` has no `Debug`; derive it, or hand-write one printing its identifiers"
                ),
                format!("{file}: `KafkaProducer` is a client and is `Clone`; remove `Clone`"),
                format!(
                    "{file}: `NewTopic` lacks `Hash` but `org.apache.kafka.clients.admin.NewTopic` overrides \
                     `hashCode`; derive it, or hash exactly the fields `PartialEq` compares"
                ),
                format!(
                    "{file}: `NewTopic` lacks `Display` but `org.apache.kafka.clients.admin.NewTopic` overrides \
                     `toString`; {display_fix}"
                ),
                format!("{file}: `OffsetSpec` is `Copy` but has a data-carrying variant; remove `Copy`"),
                format!("{file}: `ProducerConfig` is a configuration holder and is `Clone`; remove `Clone`"),
                format!(
                    "{file}: `TimeoutError` has `PartialEq, Eq` but `org.apache.kafka.common.errors.TimeoutException` \
                     does not override `equals`; remove them"
                ),
                format!(
                    "{file}: `TimestampType` lacks `PartialOrd, Ord` but \
                     `org.apache.kafka.common.record.TimestampType` is a Java enum, ordered by declaration; {ord_fix}"
                ),
                format!(
                    "{file}: `Uuid` lacks `PartialOrd, Ord` but `org.apache.kafka.common.Uuid` implements \
                     `Comparable`; {ord_fix}"
                ),
            ]
        );
        // Every public struct and enum outside `ffi` but `Missing`.
        assert_eq!(checked, 15);
    }

    const PROBE_LIB: &str = r#"
        pub trait T {}
        pub struct A;
        pub struct B;
        pub enum E { V }
        pub mod x { pub struct Dup; }
        pub mod y { pub struct Dup; }
        pub mod ffi { pub struct kafka_t; }
    "#;

    const PROBE_FILE: &str = r#"
        #[test]
        fn probes() {
            assert!(is_send!(A) && is_sync!(A));
            assert!(is_send!(crate::x::Dup) && is_sync!(crate::x::Dup));
            assert!(is_send!(Box<dyn T>), "a trait object");
            assert!(!is_sync!(E), "E is not Sync");
            assert!(is_send!(Stale) && is_sync!(Stale));
        }
    "#;

    #[test]
    fn test_every_public_type_has_its_probe() {
        let (dir, krate) = fixture("thread-safety-probe", &[("lib.rs", PROBE_LIB), ("api_promises.rs", PROBE_FILE)]);
        let rule = ThreadSafetyProbe { file: dir.join("api_promises.rs") };
        let mut findings = Vec::new();
        let checked = rule.check(&krate, &mut findings);
        fs::remove_dir_all(&dir).unwrap();
        let lib = dir.join("lib.rs").display().to_string();
        let probes = dir.join("api_promises.rs").display().to_string();
        let add = |n: &str| {
            format!("add `assert!(is_send!({n}) && is_sync!({n}));`, or for a consumer `!is_sync!` with the reason")
        };
        assert_eq!(
            findings,
            [
                format!("{probes}: `B` ({lib}) has no `is_send!` and `is_sync!` assertion; {}", add("B")),
                format!(
                    "{probes}: `Dup` is defined 2 times ({lib}, {lib}) but `is_send!` and `is_sync!` names it by \
                     fewer distinct paths; probe each definition by its own path"
                ),
                format!("{probes}: `E` ({lib}) has no `is_send!` assertion; {}", add("E")),
                format!("{probes}: `Stale` is probed but is no public type of the crate; remove its assertion"),
            ]
        );
        // `A`, `B`, `E` and the two `Dup`s.
        assert_eq!(checked, 5);
    }

    #[test]
    fn test_a_missing_probe_file_is_a_finding() {
        let (dir, krate) = fixture("thread-safety-probe-missing", &[("lib.rs", PROBE_LIB)]);
        let rule = ThreadSafetyProbe { file: dir.join("api_promises.rs") };
        let mut findings = Vec::new();
        rule.check(&krate, &mut findings);
        fs::remove_dir_all(&dir).unwrap();
        assert_eq!(findings.len(), 1);
        assert!(findings[0].contains("cannot read it"), "{findings:?}");
    }
}
