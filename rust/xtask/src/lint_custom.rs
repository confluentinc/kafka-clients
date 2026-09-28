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

//! `cargo xtask lint-custom`: source-level rules clippy cannot express.
//!
//! Every `.rs` file of the crate is parsed once, from `src/lib.rs` down through
//! its `mod` declarations, into a [`Crate`] index. Each [`Rule`] then runs over
//! that index. A rule reports findings rather than failing fast, so one run
//! lists every violation of every rule.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use quote::ToTokens;

use crate::java::{self, rust_method_bases, same_name, squash, JavaIndex};

/// A module path from the crate root, e.g. `["producer", "producer_record"]`.
type ModPath = Vec<String>;

/// One source-level rule run by `lint-custom`.
trait Rule {
    /// The rule's name, as printed in the report.
    fn name(&self) -> &'static str;

    /// Checks the crate, pushing one line per violation onto `findings`, and
    /// returns how many items it inspected.
    fn check(&self, krate: &Crate, findings: &mut Vec<String>) -> usize;

    /// How to fix a violation, printed once after the rule's findings.
    fn hint(&self) -> &'static str;

    /// Why the rule cannot run in this checkout, if it cannot; it is then
    /// reported as skipped instead of being run.
    fn skip_reason(&self) -> Option<String> {
        None
    }
}

/// The rules `lint-custom` runs, in order.
fn rules() -> Vec<Box<dyn Rule>> {
    vec![
        Box::new(NoDataCarryingEnumVariants),
        Box::new(NoPublicField),
        Box::new(JavaName::new()),
        Box::new(NoDeprecatedTranslation::new()),
        Box::new(PublicAudience::new()),
        Box::new(DynCompatible),
    ]
}

/// Runs every rule and fails if any of them reports a finding.
pub fn lint_custom() -> anyhow::Result<()> {
    println!("🔍 Running custom lint rules...");

    let krate = Crate::load(Path::new("src/lib.rs"))?;
    let mut failed = 0usize;

    // A file we cannot parse is reported rather than silently skipped: a silent
    // skip is how a rule like this rots.
    if !krate.parse_errors.is_empty() {
        eprintln!();
        for e in &krate.parse_errors {
            eprintln!("  {e}");
        }
        eprintln!("\n❌ {} file(s) could not be parsed.", krate.parse_errors.len());
        failed += krate.parse_errors.len();
    }

    for rule in rules() {
        if let Some(reason) = rule.skip_reason() {
            println!("⏭️  {}: skipped, {reason}", rule.name());
            continue;
        }
        let mut findings = Vec::new();
        let checked = rule.check(&krate, &mut findings);
        if findings.is_empty() {
            println!("✅ {}: {checked} item(s) checked", rule.name());
        } else {
            eprintln!();
            for f in &findings {
                eprintln!("  {f}");
            }
            eprintln!("\n❌ {}: {} finding(s).", rule.name(), findings.len());
            eprintln!("{}", rule.hint());
            failed += findings.len();
        }
    }

    if failed > 0 {
        anyhow::bail!("{failed} custom lint finding(s)");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Crate index
// ---------------------------------------------------------------------------

/// A parsed module: a file, or an inline `mod x { .. }` inside one.
struct Module {
    /// The file the module's items live in.
    file: PathBuf,
    /// Whether the module is declared `pub` in its parent (the root counts as `pub`).
    is_pub: bool,
    /// Whether the module sits under a `pub` inline-module chain within its file
    /// (the file itself counts as `pub`). This is the reachability notion
    /// [`NoDataCarryingEnumVariants`] has always used.
    file_public_path: bool,
    /// The module's items, excluding `#[cfg(test)]` modules.
    items: Vec<syn::Item>,
}

/// Every non-test module of the crate, keyed by path.
struct Crate {
    modules: BTreeMap<ModPath, Module>,
    parse_errors: Vec<String>,
}

impl Crate {
    fn load(root: &Path) -> anyhow::Result<Self> {
        let mut krate = Crate { modules: BTreeMap::new(), parse_errors: Vec::new() };
        krate.load_file(root, Vec::new(), true, root.parent().unwrap_or(Path::new("")).to_path_buf())?;
        Ok(krate)
    }

    /// Parses `file` as module `path`; its `mod x;` children resolve under `child_dir`.
    fn load_file(&mut self, file: &Path, path: ModPath, is_pub: bool, child_dir: PathBuf) -> anyhow::Result<()> {
        let source = fs::read_to_string(file)?;
        match syn::parse_file(&source) {
            Ok(parsed) => self.add_module(file, path, is_pub, true, parsed.items, &child_dir),
            Err(e) => {
                self.parse_errors.push(format!("{}: failed to parse: {e}", file.display()));
                Ok(())
            },
        }
    }

    fn add_module(
        &mut self,
        file: &Path,
        path: ModPath,
        is_pub: bool,
        file_public_path: bool,
        items: Vec<syn::Item>,
        child_dir: &Path,
    ) -> anyhow::Result<()> {
        let mut kept = Vec::with_capacity(items.len());
        for item in items {
            let syn::Item::Mod(m) = &item else {
                kept.push(item);
                continue;
            };
            // `#[cfg(test)]` modules never ship to downstream crates.
            if is_cfg_test(&m.attrs) {
                continue;
            }
            let name = m.ident.to_string();
            let child_pub = matches!(m.vis, syn::Visibility::Public(_));
            let mut child_path = path.clone();
            child_path.push(name.clone());
            match &m.content {
                Some((_, inner)) => self.add_module(
                    file,
                    child_path,
                    child_pub,
                    file_public_path && child_pub,
                    inner.clone(),
                    &child_dir.join(&name),
                )?,
                None => {
                    let flat = child_dir.join(format!("{name}.rs"));
                    let nested = child_dir.join(&name).join("mod.rs");
                    let child_file = if flat.exists() { flat } else { nested };
                    if !child_file.exists() {
                        self.parse_errors.push(format!("{}: `mod {name};` has no file", file.display()));
                        continue;
                    }
                    self.load_file(&child_file, child_path, child_pub, child_dir.join(&name))?;
                },
            }
            kept.push(item);
        }
        self.modules
            .insert(path, Module { file: file.to_path_buf(), is_pub, file_public_path, items: kept });
        Ok(())
    }

    /// Whether every module from the root down to `path` is declared `pub`.
    fn is_reachable(&self, path: &[String]) -> bool {
        (0..=path.len()).all(|n| self.modules.get(&path[..n]).is_some_and(|m| m.is_pub))
    }

    /// Resolves the leading segments of a `use` path, relative to `from`, to a
    /// module of this crate. `None` means the path leaves the crate (std, a
    /// dependency) or names something that is not a module.
    fn resolve_module(&self, from: &[String], segments: &[String]) -> Option<ModPath> {
        let mut current: ModPath = from.to_vec();
        for (i, seg) in segments.iter().enumerate() {
            match seg.as_str() {
                "crate" if i == 0 => current.clear(),
                "self" if i == 0 => {},
                "super" => {
                    current.pop()?;
                },
                _ => {
                    current.push(seg.clone());
                    if !self.modules.contains_key(&current) {
                        return None;
                    }
                },
            }
        }
        Some(current)
    }

    /// The `pub struct`s, `pub enum`s and `pub trait`s another crate can name, as (defining
    /// module, name): defined `pub` in a reachable module, or re-exported by a
    /// `pub use` chain from one, e.g.
    /// `mod producer_record; pub use producer_record::ProducerRecord;`.
    fn public_types(&self) -> BTreeSet<(ModPath, String)> {
        self.public_names(|item| pub_type_ident(item).map(ToString::to_string))
    }

    /// The names another crate can reach, as (defining module, name), where
    /// `def` gives the name an item defines `pub` (`None` for an item that
    /// defines none): defined in a reachable module, or re-exported by a
    /// `pub use` chain from one.
    fn public_names(&self, def: impl Fn(&syn::Item) -> Option<String>) -> BTreeSet<(ModPath, String)> {
        // The names each module exposes (`pub` items and `pub use`s), each
        // resolved to the (module, name) that may define it.
        let mut public: BTreeSet<(ModPath, String)> = BTreeSet::new();
        let mut work: Vec<(ModPath, String)> = Vec::new();
        let mut glob_work: Vec<ModPath> = Vec::new();

        for (path, module) in &self.modules {
            if !self.is_reachable(path) {
                continue;
            }
            for name in module.items.iter().filter_map(&def) {
                work.push((path.clone(), name));
            }
            for (target, name) in self.pub_uses(path) {
                match name {
                    Some(n) => work.push((target, n)),
                    None => glob_work.push(target),
                }
            }
        }

        let mut seen_globs = BTreeSet::new();
        loop {
            if let Some(module) = glob_work.pop() {
                if !seen_globs.insert(module.clone()) {
                    continue;
                }
                if let Some(m) = self.modules.get(&module) {
                    for name in m.items.iter().filter_map(&def) {
                        work.push((module.clone(), name));
                    }
                }
                for (target, name) in self.pub_uses(&module) {
                    match name {
                        Some(n) => work.push((target, n)),
                        None => glob_work.push(target),
                    }
                }
                continue;
            }
            let Some((module, name)) = work.pop() else { break };
            if !public.insert((module.clone(), name.clone())) {
                continue;
            }
            // A name that is itself a `use` in its module (of any visibility) is
            // followed to where it is defined.
            for (target, used) in self.uses(&module, false) {
                if used.as_deref() == Some(name.as_str()) {
                    work.push((target, name.clone()));
                }
            }
        }

        public
            .into_iter()
            .filter(|(module, name)| {
                self.modules
                    .get(module)
                    .is_some_and(|m| m.items.iter().filter_map(&def).any(|n| n == *name))
            })
            .collect()
    }

    /// The item defining public type `name` in `module`, found by [`Crate::public_types`].
    fn type_item(&self, module: &[String], name: &str) -> &syn::Item {
        self.modules[module]
            .items
            .iter()
            .find(|item| pub_type_ident(item).is_some_and(|ident| ident == name))
            .expect("public_types only returns defined types")
    }

    /// The `pub use` imports of `module`, as (target module, name); `None` for a glob.
    fn pub_uses(&self, module: &[String]) -> Vec<(ModPath, Option<String>)> {
        self.uses(module, true)
    }

    /// The `use` imports of `module` that land in this crate, as
    /// (target module, name) with the name under which it is imported (after `as`);
    /// `None` for a glob. With `only_pub`, restricted to `pub use`.
    fn uses(&self, module: &[String], only_pub: bool) -> Vec<(ModPath, Option<String>)> {
        let Some(m) = self.modules.get(module) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for item in &m.items {
            let syn::Item::Use(u) = item else { continue };
            if only_pub && !matches!(u.vis, syn::Visibility::Public(_)) {
                continue;
            }
            let mut leaves = Vec::new();
            flatten_use(&u.tree, &mut Vec::new(), &mut leaves);
            for (segments, leaf) in leaves {
                let Some(target) = self.resolve_module(module, &segments) else {
                    continue;
                };
                match leaf {
                    UseLeaf::Glob => out.push((target, None)),
                    UseLeaf::Name(name) => out.push((target, Some(name))),
                }
            }
        }
        out
    }
}

/// The name of a `pub struct`, `pub enum` or `pub trait` item; `None` for any other item.
fn pub_type_ident(item: &syn::Item) -> Option<&syn::Ident> {
    match item {
        syn::Item::Struct(s) if matches!(s.vis, syn::Visibility::Public(_)) => Some(&s.ident),
        syn::Item::Enum(e) if matches!(e.vis, syn::Visibility::Public(_)) => Some(&e.ident),
        syn::Item::Trait(t) if matches!(t.vis, syn::Visibility::Public(_)) => Some(&t.ident),
        _ => None,
    }
}

enum UseLeaf {
    Glob,
    /// The imported item's own name — for `Name as Alias`, `Name`, since that is
    /// the name it is defined under.
    Name(String),
}

/// Flattens a `use` tree into (path to the parent module, leaf) pairs.
fn flatten_use(tree: &syn::UseTree, prefix: &mut Vec<String>, out: &mut Vec<(Vec<String>, UseLeaf)>) {
    match tree {
        syn::UseTree::Path(p) => {
            prefix.push(p.ident.to_string());
            flatten_use(&p.tree, prefix, out);
            prefix.pop();
        },
        syn::UseTree::Name(n) => out.push((prefix.clone(), UseLeaf::Name(n.ident.to_string()))),
        syn::UseTree::Rename(r) => out.push((prefix.clone(), UseLeaf::Name(r.ident.to_string()))),
        syn::UseTree::Glob(_) => out.push((prefix.clone(), UseLeaf::Glob)),
        syn::UseTree::Group(g) => {
            for t in &g.items {
                flatten_use(t, prefix, out);
            }
        },
    }
}

fn is_cfg_test(attrs: &[syn::Attribute]) -> bool {
    attrs
        .iter()
        .any(|a| a.path().is_ident("cfg") && a.meta.to_token_stream().to_string().contains("test"))
}

// ---------------------------------------------------------------------------
// Rule: check-no-data-carrying-enum-variants
// ---------------------------------------------------------------------------

/// Checks that no variant of a public enum holds data inline.
///
/// Clippy's `exhaustive_enums` (denied in `Cargo.toml`) covers the *enum*, so a
/// downstream `match` can never be exhaustive. It says nothing about a *variant*
/// that carries fields inline: another crate can construct it and destructure it
/// field-for-field, so adding the field a new Kafka version introduces is a
/// breaking change.
///
/// The fix is structural rather than an attribute. A variant may carry data only
/// as a single wrapped type, which then carries its own forward compatibility —
/// `#[non_exhaustive]` on the wrapped struct, or private fields. This also
/// matches the Java source: every data-carrying enum here translates a Java
/// class hierarchy (a Java enum cannot give its constants per-constant shapes),
/// so one named type per variant is the faithful shape.
///
/// Marking the *variant* `#[non_exhaustive]` is deliberately not the rule: on a
/// tuple variant that attribute makes the variant wholly private to other crates
/// (E0603) — not merely unconstructable but *unmatchable*, so
/// `match err { Error::Api(e) => .. }` stops compiling downstream.
///
/// Scope: every `pub enum` not under a non-`pub` inline module of its file.
struct NoDataCarryingEnumVariants;

impl Rule for NoDataCarryingEnumVariants {
    fn name(&self) -> &'static str {
        "check-no-data-carrying-enum-variants"
    }

    fn check(&self, krate: &Crate, findings: &mut Vec<String>) -> usize {
        let mut checked = 0usize;
        for module in krate.modules.values() {
            if !module.file_public_path {
                continue;
            }
            for item in &module.items {
                let syn::Item::Enum(e) = item else { continue };
                if !matches!(e.vis, syn::Visibility::Public(_)) {
                    continue;
                }
                for v in &e.variants {
                    checked += 1;
                    let shape = match &v.fields {
                        // Carries nothing.
                        syn::Fields::Unit => continue,
                        // A newtype delegates forward compatibility to the type
                        // it wraps; two or more fields have nothing to delegate
                        // to and can be destructured positionally downstream.
                        syn::Fields::Unnamed(f) if f.unnamed.len() == 1 => continue,
                        syn::Fields::Unnamed(f) => format!("{} unnamed fields", f.unnamed.len()),
                        syn::Fields::Named(_) => "inline named fields".to_string(),
                    };
                    findings.push(format!(
                        "{}: `{}::{}` holds data inline ({shape})",
                        module.file.display(),
                        e.ident,
                        v.ident
                    ));
                }
            }
        }
        checked
    }

    fn hint(&self) -> &'static str {
        "   Move the fields into their own struct and wrap it, e.g.:
       pub struct GzipCompression { level: i32 }
       Gzip(GzipCompression),   // instead of Gzip { level: i32 }
   The struct then carries forward compatibility (private fields, or
   #[non_exhaustive]); do NOT mark the variant itself."
    }
}

// ---------------------------------------------------------------------------
// Rule: check-no-public-field
// ---------------------------------------------------------------------------

/// Checks that no public struct has a `pub` field (CLAUDE.md §3, forward
/// compatibility).
///
/// A `pub` field lets another crate read, assign and destructure it, so the
/// field's type and existence become part of the API: changing its
/// representation, or replacing it with a computed getter as a new Java version
/// does, is a breaking change. `#[non_exhaustive]` does not help here — it
/// forbids struct expressions downstream, but not field access. Java exposes
/// fields through getters, setters or builders; the Rust struct does the same,
/// keeping its fields private or `pub(crate)`.
///
/// Scope: a struct another crate can name — declared `pub` in a module whose
/// every ancestor is `pub`, or re-exported by a `pub use` chain from such a module.
/// Named and tuple fields are both checked.
struct NoPublicField;

impl Rule for NoPublicField {
    fn name(&self) -> &'static str {
        "check-no-public-field"
    }

    fn check(&self, krate: &Crate, findings: &mut Vec<String>) -> usize {
        let mut checked = 0usize;
        for (path, name) in krate.public_types() {
            let syn::Item::Struct(s) = krate.type_item(&path, &name) else {
                continue;
            };
            checked += 1;
            for (i, field) in s.fields.iter().enumerate() {
                if !matches!(field.vis, syn::Visibility::Public(_)) {
                    continue;
                }
                let field_name = field.ident.as_ref().map_or_else(|| i.to_string(), ToString::to_string);
                findings.push(format!(
                    "{}: `{}::{field_name}` is a `pub` field of a public struct",
                    krate.modules[&path].file.display(),
                    s.ident
                ));
            }
        }
        checked
    }

    fn hint(&self) -> &'static str {
        "   Make the field private or `pub(crate)`, and expose it the way Java does:
   a getter `field()`, a setter `set_field(..)`, or a builder."
    }
}

// ---------------------------------------------------------------------------
// Rule: check-java-name
// ---------------------------------------------------------------------------

/// Checks that every item translated from Java carries the Java name, as
/// adapted by the translation rules (CLAUDE.md §2, §3, §4).
///
/// An item declares what it translates with a marker (see [`crate::java`]):
/// `#[doc(alias = "org.apache.kafka.<package>.<Class>[$Nested][#method]")]`.
/// The marker is also the Java name rustdoc search finds the item under.
/// Mentioning a Java class in a doc comment claims nothing, so helpers that
/// cite the class they serve are not held to its name.
///
/// For every marker:
///   - **class** (`..Class`, `..Outer$Nested`): the class exists in the Java
///     sources, and the item is a type named as the class — `Exception` →
///     `Error` with the package prefix outside `common`, acronyms in Rust
///     casing (`SSLFactory` → `SslFactory`), a nested class under its bare
///     name.
///   - **method** (`..Class#method`): the class declares the method, and the
///     function's name derives from it — snake_case, `Exception` → `Error`,
///     `throw` → `return`, an optional dropped `get`, `_with_<params>` for an
///     overload, `new` / `with_<params>` for a constructor.
///
/// The C FFI (`src/ffi`) is excluded for now.
///
/// And for every file named after a Java class of its package
/// (`producer/producer_record.rs` ↔ `clients.producer.ProducerRecord`,
/// CLAUDE.md §2: one class per file), the file holds that class's marked type,
/// or — when a rule keeps the outer class out of scope — its marked nested ones.
///
/// Packages (CLAUDE.md §3: two Java classes of one name must never collide in
/// one Rust package). A Java package maps to one Rust module, and three checks
/// together guarantee a class only ever shares a module with its own package:
///   - **location**: a marked item under `src/` lives in its class's package
///     module. A file's module is its directory, less the folders that are no
///     Java package — grouping folders such as `admin/options/`.
///   - **mapping**: no two Java packages map to one module, and no two classes
///     of a package map to one Rust name.
///   - **re-exports**: a module's `pub use` lifts only from its own class files
///     and grouping folders — never from another package (`crate::`, `super::`,
///     a sub-package), whose class could collide with one of this package's.
///
/// Scope: `src/` (without `src/bin`) and `tests/`, test modules included; the
/// package checks cover `src/` only. Runs only when the `kafka` submodule is
/// checked out.
struct JavaName {
    index: JavaIndex,
    /// Every Rust module a Java package maps to, and each of its ancestors.
    package_modules: BTreeSet<Vec<String>>,
    /// [`PUBLIC_AUDIENCE_ALLOWLIST`]: a re-export of an allow-listed item from
    /// another package is allowed (`common::header` lifts
    /// `internals::{RecordHeader, RecordHeaders}`).
    allow: AllowList,
}

impl JavaName {
    fn new() -> Self {
        let index = JavaIndex::load();
        let mut package_modules = BTreeSet::new();
        for class in &index.classes {
            for len in 0..=class.module.len() {
                package_modules.insert(class.module[..len].to_vec());
            }
        }
        let (allow, _) = AllowList::load(Path::new(PUBLIC_AUDIENCE_ALLOWLIST));
        JavaName { index, package_modules, allow }
    }

    /// The Rust module of `src/` file `path`: its directory, less the grouping
    /// folders that are no Java package. `None` outside `src/`.
    fn file_module(&self, path: &Path) -> Option<Vec<String>> {
        let rel = path.strip_prefix("src").ok()?;
        let mut dirs: Vec<String> = rel.iter().map(|s| s.to_string_lossy().into_owned()).collect();
        dirs.pop();
        let mut module = Vec::new();
        for dir in dirs {
            module.push(dir);
            if !self.package_modules.contains(&module) {
                module.pop();
            }
        }
        Some(module)
    }
}

/// An item that may carry a marker.
struct Marked {
    kind: ItemKind,
    name: String,
    attrs: Vec<syn::Attribute>,
}

#[derive(Clone, Copy, PartialEq)]
enum ItemKind {
    Type,
    Fn,
}

/// Every type and function of `items`, recursively: module-level functions,
/// inherent-impl and trait methods, and the error types of the error macros.
fn marked_items(items: &[syn::Item], out: &mut Vec<Marked>) {
    let push = |kind, ident: &syn::Ident, attrs: &[syn::Attribute], out: &mut Vec<Marked>| {
        out.push(Marked { kind, name: ident.to_string(), attrs: attrs.to_vec() })
    };
    for item in items {
        match item {
            syn::Item::Struct(s) => push(ItemKind::Type, &s.ident, &s.attrs, out),
            syn::Item::Enum(e) => push(ItemKind::Type, &e.ident, &e.attrs, out),
            syn::Item::Union(u) => push(ItemKind::Type, &u.ident, &u.attrs, out),
            syn::Item::Type(t) => push(ItemKind::Type, &t.ident, &t.attrs, out),
            syn::Item::Trait(t) => {
                push(ItemKind::Type, &t.ident, &t.attrs, out);
                for it in &t.items {
                    if let syn::TraitItem::Fn(f) = it {
                        push(ItemKind::Fn, &f.sig.ident, &f.attrs, out);
                    }
                }
            },
            syn::Item::Fn(f) => push(ItemKind::Fn, &f.sig.ident, &f.attrs, out),
            syn::Item::Impl(i) if i.trait_.is_none() => {
                for it in &i.items {
                    if let syn::ImplItem::Fn(f) = it {
                        push(ItemKind::Fn, &f.sig.ident, &f.attrs, out);
                    }
                }
            },
            syn::Item::Mod(m) => {
                if let Some((_, inner)) = &m.content {
                    marked_items(inner, out);
                }
            },
            syn::Item::Macro(m) => {
                if let Some(def) = error_macro_type_def(m) {
                    out.push(Marked { kind: ItemKind::Type, name: def.name, attrs: def.attrs });
                }
            },
            _ => {},
        }
    }
}

/// A type defined by an error macro: its name and the attributes written
/// inside the macro.
struct TypeDef {
    name: String,
    attrs: Vec<syn::Attribute>,
}

/// The type a `kafka_error_class!` / `message_only_error!` invocation defines:
/// the leading `#[..]* Name` of its body.
fn error_macro_type_def(m: &syn::ItemMacro) -> Option<TypeDef> {
    let is_error_macro = m
        .mac
        .path
        .segments
        .last()
        .is_some_and(|s| s.ident == "kafka_error_class" || s.ident == "message_only_error");
    if !is_error_macro {
        return None;
    }
    m.mac
        .parse_body_with(|input: syn::parse::ParseStream| {
            let attrs = input.call(syn::Attribute::parse_outer)?;
            let ident: syn::Ident = input.parse()?;
            // The rest of the body (the `extends` list, ..) is not needed.
            input.step(|cursor| {
                let mut rest = *cursor;
                while let Some((_, next)) = rest.token_tree() {
                    rest = next;
                }
                Ok(((), rest))
            })?;
            Ok(TypeDef { name: ident.to_string(), attrs })
        })
        .ok()
}

/// The markers among `attrs`' `#[doc(alias = "..")]`s.
fn java_markers(attrs: &[syn::Attribute]) -> Vec<String> {
    let mut out = Vec::new();
    for attr in attrs.iter().filter(|a| a.path().is_ident("doc")) {
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("alias") {
                let lit: syn::LitStr = meta.value()?.parse()?;
                if lit.value().starts_with(java::MARKER_PREFIX) {
                    out.push(lit.value());
                }
            } else if meta.input.peek(syn::Token![=]) {
                // Skip the value of any other `doc(key = value)`.
                let _: syn::Expr = meta.value()?.parse()?;
            }
            Ok(())
        });
    }
    out
}

/// `module` as a Rust path, `crate` for the root.
fn module_path(module: &[String]) -> String {
    if module.is_empty() {
        "crate".to_string()
    } else {
        format!("crate::{}", module.join("::"))
    }
}

/// Each path a `use` tree imports, skipping a leading `self`:
/// `use a::{b, c as d}` → `a::b`, `a::c`. Globs are skipped.
fn use_paths(tree: &syn::UseTree, prefix: &mut Vec<String>, out: &mut Vec<Vec<String>>) {
    match tree {
        syn::UseTree::Path(p) if p.ident == "self" && prefix.is_empty() => use_paths(&p.tree, prefix, out),
        syn::UseTree::Path(p) => {
            prefix.push(p.ident.to_string());
            use_paths(&p.tree, prefix, out);
            prefix.pop();
        },
        syn::UseTree::Name(n) if n.ident == "self" => out.push(prefix.clone()),
        syn::UseTree::Name(n) => out.push([prefix.as_slice(), &[n.ident.to_string()]].concat()),
        syn::UseTree::Rename(r) => out.push([prefix.as_slice(), &[r.ident.to_string()]].concat()),
        syn::UseTree::Glob(_) => {},
        syn::UseTree::Group(g) => g.items.iter().for_each(|t| use_paths(t, prefix, out)),
    }
}

/// Every `.rs` file under `dir`, sorted.
fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else { return };
    let mut entries: Vec<_> = entries.filter_map(Result::ok).collect();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|x| x == "rs") {
            out.push(path);
        }
    }
}

impl JavaName {
    /// Checks one marker on `item`, pushing a finding if its name does not follow,
    /// or if the item is not in its class's package module (`module`, the item's
    /// module; `None` outside `src/`).
    fn check_marker(
        &self,
        file: &str,
        module: Option<&[String]>,
        item: &Marked,
        marker: &str,
        findings: &mut Vec<String>,
    ) {
        let Some(class) = self.index.resolve(marker) else {
            findings.push(format!(
                "{file}: `{}` is marked `{marker}`, which is not a Java class",
                item.name
            ));
            return;
        };
        let origin = format!("`{}`", marker.trim_start_matches(java::MARKER_PREFIX));
        if let Some(module) = module.filter(|m| **m != class.module) {
            findings.push(format!(
                "{file}: `{}` translates {origin}, so it belongs in module `{}`, not `{}`",
                item.name,
                module_path(&class.module),
                module_path(module)
            ));
        }
        match (JavaIndex::marker_method(marker), item.kind) {
            (None, ItemKind::Type) => {
                let expected = class.rust_name();
                if !same_name(&item.name, &expected) {
                    findings.push(format!(
                        "{file}: `{}` translates {origin} and must be named `{expected}`",
                        item.name
                    ));
                }
            },
            (Some(method), ItemKind::Fn) => {
                if let Some(signature) = JavaIndex::marker_member(marker).filter(|m| m.contains('(')) {
                    if class.deprecation(Some(signature)) == java::Deprecation::Unknown {
                        findings.push(format!(
                            "{file}: `{}` is marked {origin}, but `{}` declares no overload `{signature}`",
                            item.name,
                            class.name()
                        ));
                        return;
                    }
                }
                if !class.methods.contains(method) {
                    findings.push(format!(
                        "{file}: `{}` is marked {origin}, but `{}` declares no `{method}`",
                        item.name,
                        class.name()
                    ));
                    return;
                }
                if !class.is_rust_name_of(method, &item.name) {
                    let expected = if method == class.name() {
                        "`new` or `with_<params>`".to_string()
                    } else {
                        rust_method_bases(method)
                            .iter()
                            .map(|b| format!("`{b}`"))
                            .collect::<Vec<_>>()
                            .join(" or ")
                    };
                    findings.push(format!(
                        "{file}: `{}` translates {origin} and must be named {expected} (plus `_with_<params>` for an overload)",
                        item.name
                    ));
                }
            },
            (None, ItemKind::Fn) => {
                findings.push(format!(
                    "{file}: function `{}` is marked with class {origin}; name the method",
                    item.name
                ));
            },
            (Some(_), ItemKind::Type) => {
                findings.push(format!("{file}: type `{}` is marked with method {origin}", item.name));
            },
        }
    }

    /// The file check: a file named after a Java class of its package holds that
    /// class's marked type (or its marked nested ones).
    fn check_file(&self, path: &Path, items: &[Marked], findings: &mut Vec<String>) -> usize {
        let Ok(rel) = path.strip_prefix("src") else { return 0 };
        let mut package: Vec<String> = rel.iter().map(|s| s.to_string_lossy().into_owned()).collect();
        let Some(stem) = package.pop().map(|f| f.trim_end_matches(".rs").to_string()) else {
            return 0;
        };
        if matches!(stem.as_str(), "mod" | "lib" | "main") {
            return 0;
        }
        let mut checked = 0;
        for class in self
            .index
            .classes
            .iter()
            .filter(|c| !c.is_test && c.path.len() == 1 && c.module == package)
        {
            if squash(&stem) == squash(&class.rust_name()) {
                checked += 1;
                let top = class.marker();
                let nested = format!("{top}$");
                let marked = |m: &Marked| java_markers(&m.attrs).iter().any(|k| *k == top || k.starts_with(&nested));
                if !items.iter().any(|m| m.kind == ItemKind::Type && marked(m)) {
                    let types: Vec<String> = items
                        .iter()
                        .filter(|m| m.kind == ItemKind::Type)
                        .map(|m| format!("`{}`", m.name))
                        .collect();
                    findings.push(format!(
                        "{}: named after `{}` but holds no type marked as it (found {})",
                        path.display(),
                        top.trim_start_matches(java::MARKER_PREFIX),
                        if types.is_empty() {
                            "none".to_string()
                        } else {
                            types.join(", ")
                        }
                    ));
                }
            } else if class.name().ends_with("Exception")
                && squash(&stem) == squash(&class.name().replace("Exception", "Error"))
            {
                checked += 1;
                findings.push(format!(
                    "{}: translates `{}.{}`, which must be named `{}` in a file of that name",
                    path.display(),
                    class.package,
                    class.name(),
                    class.rust_name()
                ));
            }
        }
        checked
    }

    /// The mapping check: distinct Java packages map to distinct modules, and
    /// distinct classes of a package to distinct Rust names.
    fn check_mapping(&self, findings: &mut Vec<String>) -> usize {
        let mut packages: BTreeMap<&[String], BTreeSet<&str>> = BTreeMap::new();
        let mut names: BTreeMap<(&[String], String), BTreeSet<String>> = BTreeMap::new();
        for class in &self.index.classes {
            packages.entry(&class.module).or_default().insert(&class.package);
            if !class.is_test && class.path.len() == 1 {
                names
                    .entry((&class.module, squash(&class.rust_name())))
                    .or_default()
                    .insert(format!("{}.{}", class.package, class.name()));
            }
        }
        for (module, pkgs) in packages.iter().filter(|(_, p)| p.len() > 1) {
            findings.push(format!(
                "Java packages {} all map to module `{}`",
                pkgs.iter().map(|p| format!("`{p}`")).collect::<Vec<_>>().join(", "),
                module_path(module)
            ));
        }
        for ((module, _), classes) in names.iter().filter(|(_, c)| c.len() > 1) {
            findings.push(format!(
                "Java classes {} share one Rust name in module `{}`",
                classes.iter().map(|c| format!("`{c}`")).collect::<Vec<_>>().join(", "),
                module_path(module)
            ));
        }
        packages.len() + names.len()
    }

    /// The re-export check: each `pub use` of `items`, in module `module`, lifts
    /// from a class file or grouping folder of that same module — never from
    /// another package. Macros the file defines may be re-exported too, and so
    /// may an item of [`PUBLIC_AUDIENCE_ALLOWLIST`] (by its lifted Rust path).
    fn check_reexports(&self, file: &str, module: &[String], items: &[syn::Item], findings: &mut Vec<String>) -> usize {
        let mut children = BTreeSet::new();
        let mut macros = BTreeSet::new();
        for item in items {
            match item {
                syn::Item::Mod(m) => {
                    children.insert(m.ident.to_string());
                },
                syn::Item::Macro(m) => {
                    if let Some(ident) = &m.ident {
                        macros.insert(ident.to_string());
                    }
                },
                _ => {},
            }
        }
        let mut checked = 0;
        for item in items {
            let syn::Item::Use(u) = item else { continue };
            if matches!(u.vis, syn::Visibility::Inherited) {
                continue;
            }
            let mut paths = Vec::new();
            use_paths(&u.tree, &mut Vec::new(), &mut paths);
            let into = module_path(module);
            for path in paths {
                checked += 1;
                let name = &path[0];
                let lifted = path.join("::");
                let mut child = module.to_vec();
                child.push(name.clone());
                let foreign = if macros.contains(name) || self.allow.allows_quietly(&[&format!("{into}::{lifted}")]) {
                    None
                } else if !children.contains(name) {
                    Some(format!("`{name}`, outside this module"))
                } else if self.package_modules.contains(&child) {
                    Some(format!("package `{}`", module_path(&child)))
                } else {
                    None
                };
                if let Some(from) = foreign {
                    findings.push(format!(
                        "{file}: `pub use {lifted}` re-exports from {from} into `{into}`; import it from its own package instead"
                    ));
                }
            }
        }
        checked
    }
}

impl Rule for JavaName {
    fn name(&self) -> &'static str {
        "check-java-name"
    }

    fn skip_reason(&self) -> Option<String> {
        self.index.is_empty().then(|| {
            format!(
                "no Java sources at `{}` (run `git submodule update --init kafka`)",
                java::JAVA_MAIN_ROOT
            )
        })
    }

    fn check(&self, _krate: &Crate, findings: &mut Vec<String>) -> usize {
        let mut files = Vec::new();
        rust_files(Path::new("src"), &mut files);
        rust_files(Path::new("tests"), &mut files);
        let mut checked = 0usize;
        // The C FFI (`src/ffi`) is excluded for now: it carries no markers and
        // its CLAUDE.md §4 naming is not checked.
        for path in files.iter().filter(|p| !p.starts_with("src/bin") && !p.starts_with("src/ffi")) {
            let file = path.display().to_string();
            let parsed = match fs::read_to_string(path).map(|s| syn::parse_file(&s)) {
                Ok(Ok(parsed)) => parsed,
                Ok(Err(e)) => {
                    findings.push(format!("{file}: failed to parse: {e}"));
                    continue;
                },
                Err(e) => {
                    findings.push(format!("{file}: failed to read: {e}"));
                    continue;
                },
            };
            let module = self.file_module(path);
            let mut items = Vec::new();
            marked_items(&parsed.items, &mut items);
            for item in &items {
                for marker in java_markers(&item.attrs) {
                    checked += 1;
                    self.check_marker(&file, module.as_deref(), item, &marker, findings);
                }
            }
            checked += self.check_file(path, &items, findings);
            if let Some(module) = &module {
                checked += self.check_reexports(&file, module, &parsed.items, findings);
            }
        }
        checked + self.check_mapping(findings)
    }

    fn hint(&self) -> &'static str {
        "   Name the item as Java does, adapted only by the translation rules
   (CLAUDE.md §2: Exception -> Error with the package prefix outside `common`,
   camelCase -> snake_case, `_with_<params>` for overloads, `new`/`with_..` for
   constructors, a nested class under its bare name). Fix the marker instead if
   it names the wrong Java class or method; a Rust-only helper carries none."
    }
}

// ---------------------------------------------------------------------------
// Rule: check-no-deprecated-translation
// ---------------------------------------------------------------------------

/// Where the items already shipped in a released major version are listed,
/// each with the version: Java deprecating one of them later does not remove
/// it before the next major (CLAUDE.md §3, forward compatibility).
const DEPRECATED_BASELINE: &str = "xtask/deprecated-baseline.txt";

/// Checks that no public item translates API Java marks `@Deprecated`
/// (CLAUDE.md §3: deprecated API is not translated from the first major version
/// on).
///
/// An item is deprecated when its marker (see [`crate::java`]) names a
/// `@Deprecated` class, overload or field, or a member of a `@Deprecated`
/// class, in the `kafka` working tree or in [`java::DEPRECATED_LIST`] (the
/// union over [`java::DEPRECATION_REFS`], which a shallow clone cannot read).
/// A method name some of whose overloads are deprecated must be marked with the
/// overload it translates, `#name(T1,T2)`, so a translation of a live overload
/// is not taken for a deprecated one.
///
/// Items Java deprecated after they shipped in a released major version are
/// listed in [`DEPRECATED_BASELINE`]; they stay until the next major but must
/// carry `#[deprecated]`.
///
/// Scope: public items only — a type another crate can name, its `pub`
/// inherent methods and associated consts, a public trait's methods, `pub` free
/// functions and consts of a reachable module. A `pub(crate)` translation is
/// not API: Java's own internals use deprecated constructors
/// (`ConsumerGroupMetadata`'s, by `AsyncKafkaConsumer`), and so may the crate.
/// Classes of an `internals` package are `pub(crate)` by rule and skipped.
struct NoDeprecatedTranslation {
    index: JavaIndex,
    /// The markers of [`DEPRECATED_BASELINE`].
    baseline: BTreeSet<String>,
    /// Why the checked-in list is out of date with the refs, if it is.
    stale_list: Option<String>,
}

impl NoDeprecatedTranslation {
    fn new() -> Self {
        let mut index = JavaIndex::load();
        let list = fs::read_to_string(java::DEPRECATED_LIST).unwrap_or_default();
        index.merge_deprecation_list(&list);
        let baseline = fs::read_to_string(DEPRECATED_BASELINE)
            .unwrap_or_default()
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .filter_map(|l| l.split_whitespace().next().map(str::to_string))
            .collect();
        NoDeprecatedTranslation { stale_list: Self::stale_list(&list), index, baseline }
    }

    /// Compares the checked-in list with the refs, when the refs are present.
    fn stale_list(list: &str) -> Option<String> {
        let mut expected = BTreeSet::new();
        for reference in java::DEPRECATION_REFS {
            expected.extend(java::deprecated_items(&java::load_ref(reference)?));
        }
        let listed: BTreeSet<String> = list
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(str::to_string)
            .collect();
        (listed != expected).then(|| {
            format!(
                "{} is out of date with Kafka {} (run `cargo xtask java-deprecated`)",
                java::DEPRECATED_LIST,
                java::DEPRECATION_REFS.join(" + ")
            )
        })
    }
}

/// A public item that may carry a marker.
struct PublicItem {
    name: String,
    attrs: Vec<syn::Attribute>,
}

/// The public items of `krate` (see [`NoDeprecatedTranslation`] for the scope).
fn public_items(krate: &Crate) -> Vec<(&Path, PublicItem)> {
    let types = krate.public_types();
    let is_pub = |vis: &syn::Visibility| matches!(vis, syn::Visibility::Public(_));
    let mut out = Vec::new();
    for (path, module) in &krate.modules {
        let file = module.file.as_path();
        let reachable = krate.is_reachable(path);
        let is_public_type = |name: &str| types.contains(&(path.clone(), name.to_string()));
        for item in &module.items {
            let mut push = |name: &syn::Ident, attrs: &[syn::Attribute]| {
                out.push((file, PublicItem { name: name.to_string(), attrs: attrs.to_vec() }))
            };
            match item {
                syn::Item::Struct(s) if is_public_type(&s.ident.to_string()) => push(&s.ident, &s.attrs),
                syn::Item::Enum(e) if is_public_type(&e.ident.to_string()) => push(&e.ident, &e.attrs),
                syn::Item::Trait(t) if is_public_type(&t.ident.to_string()) => {
                    push(&t.ident, &t.attrs);
                    for it in &t.items {
                        match it {
                            syn::TraitItem::Fn(f) => push(&f.sig.ident, &f.attrs),
                            syn::TraitItem::Const(c) => push(&c.ident, &c.attrs),
                            _ => {},
                        }
                    }
                },
                syn::Item::Type(t) if reachable && is_pub(&t.vis) => push(&t.ident, &t.attrs),
                syn::Item::Fn(f) if reachable && is_pub(&f.vis) => push(&f.sig.ident, &f.attrs),
                syn::Item::Const(c) if reachable && is_pub(&c.vis) => push(&c.ident, &c.attrs),
                syn::Item::Impl(i) if i.trait_.is_none() => {
                    let syn::Type::Path(ty) = &*i.self_ty else { continue };
                    let Some(ty) = ty.path.segments.last() else { continue };
                    if !is_public_type(&ty.ident.to_string()) {
                        continue;
                    }
                    for it in &i.items {
                        match it {
                            syn::ImplItem::Fn(f) if is_pub(&f.vis) => push(&f.sig.ident, &f.attrs),
                            syn::ImplItem::Const(c) if is_pub(&c.vis) => push(&c.ident, &c.attrs),
                            _ => {},
                        }
                    }
                },
                syn::Item::Macro(m) => {
                    // The error types are public: every error is a variant of
                    // the public `Error` (CLAUDE.md §12).
                    if let Some(def) = error_macro_type_def(m) {
                        out.push((file, PublicItem { name: def.name, attrs: def.attrs }));
                    }
                },
                _ => {},
            }
        }
    }
    out
}

impl Rule for NoDeprecatedTranslation {
    fn name(&self) -> &'static str {
        "check-no-deprecated-translation"
    }

    fn skip_reason(&self) -> Option<String> {
        self.index.is_empty().then(|| {
            format!(
                "no Java sources at `{}` (run `git submodule update --init kafka`)",
                java::JAVA_MAIN_ROOT
            )
        })
    }

    fn check(&self, krate: &Crate, findings: &mut Vec<String>) -> usize {
        if let Some(stale) = &self.stale_list {
            findings.push(stale.clone());
        }
        let mut checked = 0usize;
        for (file, item) in public_items(krate) {
            for marker in java_markers(&item.attrs) {
                if marker.split('#').next().is_some_and(|c| c.split('.').any(|p| p == "internals")) {
                    continue;
                }
                checked += 1;
                let origin = marker.trim_start_matches(java::MARKER_PREFIX);
                let file = file.display();
                match self.index.deprecation(&marker) {
                    java::Deprecation::Yes if self.baseline.contains(&marker) => {
                        if !item.attrs.iter().any(|a| a.path().is_ident("deprecated")) {
                            findings.push(format!(
                                "{file}: `{}` translates `{origin}`, deprecated after it shipped; mark it `#[deprecated]`",
                                item.name
                            ));
                        }
                    },
                    java::Deprecation::Yes => findings.push(format!(
                        "{file}: `{}` translates `{origin}`, which Java deprecates",
                        item.name
                    )),
                    java::Deprecation::Ambiguous(live) => findings.push(format!(
                        "{file}: `{}` is marked `{origin}`, some of whose overloads Java deprecates; mark the overload it translates: {}",
                        item.name,
                        live.iter().map(|s| format!("`#{s}`")).collect::<Vec<_>>().join(", ")
                    )),
                    java::Deprecation::No | java::Deprecation::Unknown => {},
                }
            }
        }
        checked
    }

    fn hint(&self) -> &'static str {
        "   Remove the item and move its callers to the replacement Java names in the
   @deprecated javadoc (CLAUDE.md §3); a `pub(crate)` item may stay if the
   crate needs it internally, as Java's internals do. An item Java deprecated
   after it shipped in a released major is listed in
   xtask/deprecated-baseline.txt and marked `#[deprecated]` instead."
    }
}

// ---------------------------------------------------------------------------
// Rule: check-public-audience
// ---------------------------------------------------------------------------

/// The types annotated `@InterfaceAudience.Public` at [`java::AUDIENCE_REF`],
/// one fully-qualified top-level name per line.
const PUBLIC_AUDIENCE_LIST: &str = "../design/current/interface-audience-public-4.4.txt";

/// The public items [`PublicAudience`] lets through although no Public Java
/// class backs them, one per line: `<key>  # <reason>`.
const PUBLIC_AUDIENCE_ALLOWLIST: &str = "xtask/public-audience-allowlist.txt";

/// Where the C header's `[export] include` list lives.
const CBINDGEN_TOML: &str = "cbindgen.toml";

/// One line of [`PUBLIC_AUDIENCE_ALLOWLIST`].
struct AllowEntry {
    /// A Java class (`org.apache.kafka.clients.admin.MockAdminClient`), a Rust
    /// path (`crate::common::Error`) or a C symbol prefix
    /// (`kafka_consumer_ConsumerHandle`); `*` matches any run of characters.
    key: String,
    line: usize,
    /// Whether the entry let through an item that would otherwise be a finding.
    used: std::cell::Cell<bool>,
}

/// The entries of [`PUBLIC_AUDIENCE_ALLOWLIST`].
#[derive(Default)]
struct AllowList {
    entries: Vec<AllowEntry>,
}

impl AllowList {
    /// Reads the list at `path` (empty if there is none), with a finding for
    /// each malformed line.
    fn load(path: &Path) -> (Self, Vec<String>) {
        Self::parse(&path.display().to_string(), &fs::read_to_string(path).unwrap_or_default())
    }

    /// Parses `text`: a line is `<key>  # <reason>`, and the reason is
    /// required; blank lines and lines starting with `#` are comments.
    fn parse(file: &str, text: &str) -> (Self, Vec<String>) {
        let mut list = AllowList::default();
        let mut errors = Vec::new();
        for (i, raw) in text.lines().enumerate() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (key, reason) = line.split_once('#').map_or((line, ""), |(k, r)| (k.trim(), r.trim()));
            if key.is_empty() || key.contains(char::is_whitespace) || reason.is_empty() {
                errors.push(format!("{file}:{}: expected `<key>  # <reason>`, found `{line}`", i + 1));
                continue;
            }
            list.entries
                .push(AllowEntry { key: key.to_string(), line: i + 1, used: Default::default() });
        }
        (list, errors)
    }

    /// Whether an entry matches one of `keys`; the entry is then recorded as used.
    fn allows(&self, keys: &[&str]) -> bool {
        let hit = self.entries.iter().find(|e| keys.iter().any(|k| glob_match(&e.key, k)));
        hit.inspect(|e| e.used.set(true)).is_some()
    }

    /// [`AllowList::allows`], without recording the entry as used.
    fn allows_quietly(&self, keys: &[&str]) -> bool {
        self.entries.iter().any(|e| keys.iter().any(|k| glob_match(&e.key, k)))
    }
}

/// Whether `text` matches `pattern`, in which `*` matches any run of
/// characters (`::` included) and every other character matches itself.
fn glob_match(pattern: &str, text: &str) -> bool {
    let Some((head, rest)) = pattern.split_once('*') else {
        return pattern == text;
    };
    let Some(mut text) = text.strip_prefix(head) else {
        return false;
    };
    let parts: Vec<&str> = rest.split('*').collect();
    let (last, middle) = parts.split_last().expect("split yields at least one part");
    for part in middle {
        match text.find(part) {
            Some(at) => text = &text[at + part.len()..],
            None => return false,
        }
    }
    text.len() >= last.len() && text.ends_with(last)
}

/// Checks that the crate makes public — in Rust and in C — only what Java's
/// public API holds (CLAUDE.md §2, §4). A public item translating a Java class
/// must pass all three rules, which are conjunctive:
///   1. its package contains no `internal` segment;
///   2. its package's `package-info.java` does not say "This package is not a
///      supported Kafka API" at [`java::AUDIENCE_REF`];
///   3. its class is annotated `@InterfaceAudience.Public` at
///      [`java::AUDIENCE_REF`] ([`PUBLIC_AUDIENCE_LIST`]); a nested class
///      takes the audience of the top-level class declaring it.
///
/// A public item without a Java class (a Rust-only helper, a JDK type) must be
/// listed in [`PUBLIC_AUDIENCE_ALLOWLIST`] with the reason it is public, as
/// must a Java class that is public on purpose despite the rules (a test-jar
/// helper). An entry that no longer lets anything through is reported, so the
/// list cannot rot.
///
/// **Rust:** every `pub` struct, enum, union, trait, type alias, fn, const and
/// static another crate can name — defined in a reachable module or
/// re-exported by a `pub use` chain from one — and the error types of the
/// error macros, found by their Java marker (see [`crate::java`]). The C FFI
/// module is left to the C half. The module wrapping the generated protocol
/// code (a single `include!`, which the parser cannot see into) must not be
/// `pub` nor re-exported by `pub use`: the protocol messages are not public
/// API.
///
/// **C:** every `#[no_mangle]` function and every `kafka_*_t` type of the FFI
/// module, and every `[export] include` of `cbindgen.toml`. A name
/// `kafka_<pkg>_<Class>_…` names Java class `<Class>` of package
/// `clients.<pkg>`, else `<pkg>` (`Error` → `Exception` for an error, with or
/// without the package prefix of CLAUDE.md §2); a class not public by the rules
/// cannot have C bindings, and a name that resolves to no class is an error.
///
/// Leaks through signatures (a public fn taking a `pub(crate)` type) are not
/// checked here: rustc's `unnameable_types` lint, enabled in `src/lib.rs`,
/// catches them under `cargo xtask lint`. `private_interfaces` /
/// `private_bounds` do not, because they ignore a `pub` item declared inside a
/// `pub(crate)` module — the shape every privatised package here has.
struct PublicAudience {
    index: JavaIndex,
    /// [`PUBLIC_AUDIENCE_LIST`].
    public: BTreeSet<String>,
    /// The dotted packages (below `org.apache.kafka`) carrying the
    /// unsupported-API disclaimer.
    unsupported: BTreeSet<String>,
    allow: AllowList,
    /// The malformed lines of the allow-list.
    allow_errors: Vec<String>,
    /// Where the `[export] include` list is read from.
    cbindgen: PathBuf,
    /// Where the disclaimers were read from, for the skip reason.
    unsupported_source: String,
}

impl PublicAudience {
    fn new() -> Self {
        let index = JavaIndex::load();
        let public = parse_public_list(&fs::read_to_string(PUBLIC_AUDIENCE_LIST).unwrap_or_default());
        let (unsupported, unsupported_source) = match java::unsupported_packages_at_ref(java::AUDIENCE_REF) {
            Some(packages) => (packages, format!("`kafka` at {}", java::AUDIENCE_REF)),
            // Without the ref (a shallow clone), the working tree's disclaimers.
            None => (java::unsupported_packages_in_tree(), format!("`{}`", java::JAVA_MAIN_ROOT)),
        };
        let (allow, allow_errors) = AllowList::load(Path::new(PUBLIC_AUDIENCE_ALLOWLIST));
        PublicAudience {
            index,
            public,
            unsupported,
            allow,
            allow_errors,
            cbindgen: PathBuf::from(CBINDGEN_TOML),
            unsupported_source,
        }
    }

    /// Why top-level Java class `fqn` (`org.apache.kafka.<package>.<Class>`)
    /// may not be public, one reason per rule it fails; empty when it may.
    fn audience_failures(&self, fqn: &str) -> Vec<String> {
        let path = fqn.strip_prefix(java::MARKER_PREFIX).unwrap_or(fqn);
        let (package, _) = path.rsplit_once('.').unwrap_or(("", path));
        let mut reasons = Vec::new();
        if package.split('.').any(|seg| seg.contains("internal")) {
            reasons.push(format!("its package `{package}` is internal"));
        }
        if self.unsupported.contains(package) {
            reasons.push(format!("its package `{package}` is not a supported Kafka API"));
        }
        if !self.public.contains(fqn) {
            reasons.push(format!("it is not `@InterfaceAudience.Public` in Kafka {}", java::AUDIENCE_REF));
        }
        reasons
    }

    /// The Rust half: public items and the generated module.
    fn check_rust(&self, krate: &Crate, findings: &mut Vec<String>) -> usize {
        let mut checked = 0;
        for (module, name) in krate.public_names(|item| pub_def(item).map(|(name, _)| name)) {
            if module.first().is_some_and(|m| m == "ffi") {
                continue;
            }
            let m = &krate.modules[&module];
            let file = m.file.display();
            // Every item of the name (a type and a fn may share it through a macro).
            for (_, attrs) in m.items.iter().filter_map(pub_def).filter(|(n, _)| *n == name) {
                checked += 1;
                let full = format!("{}::{name}", module_path(&module));
                // The path §2 imports it by: its file module dropped.
                let canonical = match module.split_last() {
                    Some((file_mod, parent)) if *file_mod == java::snake_case(&name) => {
                        format!("{}::{name}", module_path(parent))
                    },
                    _ => full.clone(),
                };
                let classes: BTreeSet<String> = java_markers(&attrs)
                    .iter()
                    .filter_map(|marker| marker.split('#').next())
                    .map(str::to_string)
                    .collect();
                let tops: Vec<String> = classes.iter().map(|c| top_level_class(c).to_string()).collect();
                let mut keys: Vec<&str> = vec![&full, &canonical];
                keys.extend(classes.iter().map(String::as_str));
                keys.extend(tops.iter().map(String::as_str));
                if classes.is_empty() {
                    if !self.allow.allows(&keys) {
                        findings.push(format!(
                            "{file}: `{canonical}` is public but carries no Java marker; mark the class it translates, or \
                             add it to {PUBLIC_AUDIENCE_ALLOWLIST} with the reason it is public"
                        ));
                    }
                    continue;
                }
                let failures: Vec<String> = tops
                    .iter()
                    .filter_map(|top| {
                        let reasons = self.audience_failures(top);
                        (!reasons.is_empty()).then(|| format!("`{top}`: {}", reasons.join(", ")))
                    })
                    .collect();
                if !failures.is_empty() && !self.allow.allows(&keys) {
                    findings.push(format!(
                        "{file}: `{canonical}` is public but translates {}",
                        failures.join("; ")
                    ));
                }
            }
        }
        checked + check_generated_module(krate, findings)
    }

    /// The Java class C symbol prefix `kafka_<pkg>_<Class>` names, as its
    /// (possibly nested) marker and its top-level marker; `None` if it names
    /// none.
    fn resolve_ffi_class(&self, pkg: &[&str], class: &str) -> Option<(String, String)> {
        let dotted = pkg.join(".");
        let packages = [format!("clients.{dotted}"), dotted];
        let mut names = vec![class.to_string()];
        if let Some(base) = class.strip_suffix("Error") {
            names.push(format!("{base}Exception"));
        }
        let top = |c: &java::JavaClass| format!("{}{}.{}", java::MARKER_PREFIX, c.package, c.path[0]);
        for package in &packages {
            for name in &names {
                if let Some(c) = self.index.classes.iter().find(|c| c.package == *package && c.name() == name) {
                    return Some((c.marker(), top(c)));
                }
                let fqn = format!("{}{package}.{name}", java::MARKER_PREFIX);
                if self.public.contains(&fqn) {
                    return Some((fqn.clone(), fqn));
                }
            }
        }
        // An error payload hangs off the `kafka_common_Error_t` handle
        // (`kafka_common_ResourceNotFoundError_t`, CLAUDE.md §4) under its Rust
        // name, whatever the exception's package: `common.errors`, or another
        // package whose prefix the name carries (`ConsumerOffsetOutOfRangeError`,
        // §2). Those names are unique by construction.
        if class.ends_with("Error") {
            let error = self
                .index
                .classes
                .iter()
                .find(|c| !c.is_test && c.name().ends_with("Exception") && c.rust_name() == class);
            if let Some(c) = error {
                return Some((c.marker(), top(c)));
            }
        }
        None
    }

    /// Where Java declares a class named as C `class` when the C package does
    /// not match, to say so in the finding (`; Java declares it as …`).
    fn misplaced_ffi_class(&self, class: &str) -> String {
        let exception = class.strip_suffix("Error").map(|b| format!("{b}Exception"));
        let found: BTreeSet<String> = self
            .index
            .classes
            .iter()
            .filter(|c| c.name() == class || Some(c.name()) == exception.as_deref() || c.rust_name() == class)
            .map(|c| format!("`{}`", c.marker()))
            .collect();
        if found.is_empty() {
            String::new()
        } else {
            let names: Vec<String> = found.into_iter().collect();
            format!("; Java declares it as {}", names.join(", "))
        }
    }

    /// The C half: the FFI symbols and the header's include list, reported once
    /// per `kafka_<pkg>_<Class>` prefix.
    fn check_c(&self, krate: &Crate, findings: &mut Vec<String>) -> usize {
        // symbol -> file it is declared in.
        let mut symbols: BTreeMap<String, String> = BTreeMap::new();
        for (path, module) in &krate.modules {
            if path.first().is_none_or(|m| m != "ffi") {
                continue;
            }
            for item in &module.items {
                let name = match item {
                    syn::Item::Fn(f) if is_no_mangle(&f.attrs) => f.sig.ident.to_string(),
                    syn::Item::Struct(s) => s.ident.to_string(),
                    syn::Item::Enum(e) => e.ident.to_string(),
                    syn::Item::Type(t) => t.ident.to_string(),
                    syn::Item::Union(u) => u.ident.to_string(),
                    _ => continue,
                };
                if matches!(item, syn::Item::Fn(_)) || (name.starts_with("kafka_") && name.ends_with("_t")) {
                    symbols.entry(name).or_insert_with(|| module.file.display().to_string());
                }
            }
        }
        let cbindgen = self.cbindgen.display().to_string();
        for name in cbindgen_includes(&fs::read_to_string(&self.cbindgen).unwrap_or_default()) {
            symbols.entry(name).or_insert_with(|| cbindgen.clone());
        }

        // prefix -> (file, symbol count, finding reason)
        let mut by_prefix: BTreeMap<String, (String, usize, Option<String>)> = BTreeMap::new();
        for (symbol, file) in &symbols {
            let parsed = ffi_class(symbol);
            let prefix = parsed
                .as_ref()
                .map_or_else(|| symbol.clone(), |(pkg, class)| format!("kafka_{}_{class}", pkg.join("_")));
            if let Some(entry) = by_prefix.get_mut(&prefix) {
                entry.1 += 1;
                continue;
            }
            let reason = match parsed.as_ref().and_then(|(pkg, class)| self.resolve_ffi_class(pkg, class)) {
                None if self.allow.allows(&[symbol, &prefix]) => None,
                None => Some(format!(
                    "names no Java class (`kafka_<pkg>_<Class>_…`, with `<pkg>` the Java package without \
                     `clients`){}",
                    parsed
                        .as_ref()
                        .map_or_else(String::new, |(_, class)| self.misplaced_ffi_class(class))
                )),
                Some((class, top)) => {
                    let failures = self.audience_failures(&top);
                    if failures.is_empty() || self.allow.allows(&[symbol, &prefix, &class, &top]) {
                        None
                    } else {
                        Some(format!("binds `{class}`, which may not be public: {}", failures.join(", ")))
                    }
                },
            };
            by_prefix.insert(prefix, (file.clone(), 1, reason));
        }
        for (prefix, (file, count, reason)) in &by_prefix {
            if let Some(reason) = reason {
                findings.push(format!("{file}: C `{prefix}` ({count} symbol(s)) {reason}"));
            }
        }
        symbols.len()
    }
}

/// The name a public item defines, and its attributes: a `pub` struct, enum,
/// union, trait, type alias, fn, const or static, or an error macro's type
/// (always `pub`).
fn pub_def(item: &syn::Item) -> Option<(String, Vec<syn::Attribute>)> {
    let is_pub = |vis: &syn::Visibility| matches!(vis, syn::Visibility::Public(_));
    let (ident, attrs) = match item {
        syn::Item::Struct(s) if is_pub(&s.vis) => (&s.ident, &s.attrs),
        syn::Item::Enum(e) if is_pub(&e.vis) => (&e.ident, &e.attrs),
        syn::Item::Union(u) if is_pub(&u.vis) => (&u.ident, &u.attrs),
        syn::Item::Trait(t) if is_pub(&t.vis) => (&t.ident, &t.attrs),
        syn::Item::Type(t) if is_pub(&t.vis) => (&t.ident, &t.attrs),
        syn::Item::Fn(f) if is_pub(&f.vis) => (&f.sig.ident, &f.attrs),
        syn::Item::Const(c) if is_pub(&c.vis) => (&c.ident, &c.attrs),
        syn::Item::Static(s) if is_pub(&s.vis) => (&s.ident, &s.attrs),
        syn::Item::Macro(m) => return error_macro_type_def(m).map(|def| (def.name, def.attrs)),
        _ => return None,
    };
    Some((ident.to_string(), attrs.clone()))
}

/// The top-level class of a class marker: `…Outer$Nested` → `…Outer`.
fn top_level_class(marker: &str) -> &str {
    marker.split('$').next().unwrap_or(marker)
}

/// The generated-code check: a crate-root module wrapping an `include!` must
/// not be `pub`, nor be re-exported by a `pub use`.
fn check_generated_module(krate: &Crate, findings: &mut Vec<String>) -> usize {
    let Some(root) = krate.modules.get(&Vec::new()) else {
        return 0;
    };
    let file = root.file.display();
    let mut generated = BTreeSet::new();
    for item in &root.items {
        let syn::Item::Mod(m) = item else { continue };
        let Some((_, inner)) = &m.content else { continue };
        let includes = inner
            .iter()
            .any(|i| matches!(i, syn::Item::Macro(mac) if mac.mac.path.is_ident("include")));
        if !includes {
            continue;
        }
        generated.insert(m.ident.to_string());
        if matches!(m.vis, syn::Visibility::Public(_)) {
            findings.push(format!(
                "{file}: `pub mod {}` makes the generated protocol code public; make it `pub(crate)`",
                m.ident
            ));
        }
    }
    for item in &root.items {
        let syn::Item::Use(u) = item else { continue };
        if !matches!(u.vis, syn::Visibility::Public(_)) {
            continue;
        }
        let mut leaves = Vec::new();
        flatten_use(&u.tree, &mut Vec::new(), &mut leaves);
        for (segments, _) in leaves {
            let first = segments.iter().find(|s| *s != "self" && *s != "crate");
            if let Some(module) = first.filter(|s| generated.contains(*s)) {
                findings.push(format!(
                    "{file}: a `pub use` re-exports the generated module `{module}`; make it `pub(crate) use`"
                ));
            }
        }
    }
    generated.len()
}

fn is_no_mangle(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|a| a.meta.to_token_stream().to_string().contains("no_mangle"))
}

/// A C symbol `kafka_<pkg>_<Class>_…` as (package segments, class); `None`
/// when it has no lower-case package segment followed by a PascalCase class.
fn ffi_class(symbol: &str) -> Option<(Vec<&str>, &str)> {
    let rest = symbol.strip_prefix("kafka_")?;
    let mut pkg = Vec::new();
    for seg in rest.split('_') {
        if seg.starts_with(|c: char| c.is_ascii_uppercase()) {
            return (!pkg.is_empty()).then_some((pkg, seg));
        }
        if seg.is_empty() || !seg.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()) {
            return None;
        }
        pkg.push(seg);
    }
    None
}

/// The quoted names of `cbindgen.toml`'s `[export] include = [..]`.
fn cbindgen_includes(toml: &str) -> Vec<String> {
    let mut in_export = false;
    let mut collecting = false;
    let mut list = String::new();
    for line in toml.lines() {
        let trimmed = line.trim();
        if !collecting && trimmed.starts_with('[') {
            in_export = trimmed == "[export]";
            continue;
        }
        if in_export && !collecting && trimmed.starts_with("include") {
            collecting = true;
        }
        if collecting {
            list.push_str(trimmed.split_once('#').map_or(trimmed, |(code, _)| code));
            if trimmed.contains(']') {
                break;
            }
        }
    }
    list.split('"').skip(1).step_by(2).map(str::to_string).collect()
}

/// The fully-qualified names of the Public list.
fn parse_public_list(text: &str) -> BTreeSet<String> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_string)
        .collect()
}

impl Rule for PublicAudience {
    fn name(&self) -> &'static str {
        "check-public-audience"
    }

    fn skip_reason(&self) -> Option<String> {
        if self.index.is_empty() {
            return Some(format!(
                "no Java sources at `{}` (run `git submodule update --init kafka`)",
                java::JAVA_MAIN_ROOT
            ));
        }
        if self.public.is_empty() {
            return Some(format!("no Public list at `{PUBLIC_AUDIENCE_LIST}`"));
        }
        self.unsupported
            .is_empty()
            .then(|| format!("no unsupported-API disclaimers found in {}", self.unsupported_source))
    }

    fn check(&self, krate: &Crate, findings: &mut Vec<String>) -> usize {
        findings.extend(self.allow_errors.iter().cloned());
        let checked = self.check_rust(krate, findings) + self.check_c(krate, findings);
        for entry in self.allow.entries.iter().filter(|e| !e.used.get()) {
            findings.push(format!(
                "{PUBLIC_AUDIENCE_ALLOWLIST}:{}: `{}` lets nothing through any more; remove it",
                entry.line, entry.key
            ));
        }
        checked
    }

    fn hint(&self) -> &'static str {
        "   Make the item `pub(crate)` and drop its C binding: only a class Java
   publishes may be public — outside `internal*` packages, outside packages whose
   package-info.java says \"not a supported Kafka API\", and annotated
   `@InterfaceAudience.Public` in Kafka 4.4. A Rust-only item, or a deliberate
   exception, goes in xtask/public-audience-allowlist.txt with its reason.
   A public signature naming a crate-private type is caught by rustc's
   `unnameable_types` (enabled in src/lib.rs) under `cargo xtask lint`."
    }
}

// ---------------------------------------------------------------------------
// Rule: check-dyn-compatible
// ---------------------------------------------------------------------------

/// Checks that every public trait is dyn-compatible, or has a dyn-compatible
/// `Dyn<Name>` companion (CLAUDE.md §3, forward compatibility).
///
/// Java code puts objects of different classes in one collection typed by the
/// interface they implement (`List<Serializer<String>>`); the Rust equivalent is
/// `Vec<Box<dyn Serializer<String>>>`, which only compiles for a dyn-compatible
/// trait. A trait that is not dyn-compatible can never become one without a
/// breaking change, so the property is checked up front.
///
/// The exception is a performance-oriented trait whose methods return
/// `impl Future` (e.g. `Producer`, so a generic caller does not box a future per
/// call): it must be paired with a public `Dyn<Name>` trait (e.g. `DynProducer`)
/// that is dyn-compatible and blanket-implemented for it, so the boxing cost is
/// paid only by callers who need dynamic dispatch.
///
/// The check mirrors the reference's dyn-compatibility rules on the syntax: no
/// `Sized`-implying or `Self`-parameterized supertrait (nor a trait-level
/// `where Self: Sized`), no associated consts or
/// generic associated types, and every method not opted out with
/// `where Self: Sized` has a receiver, no type parameters, no `impl Trait` in
/// argument or return position, is not a native `async fn` (an
/// `#[async_trait]` one is boxed and fine), and does not mention `Self` outside
/// the receiver except through a projection such as `Self::Item`.
///
/// Scope: the same public items as [`NoPublicField`], traits only.
struct DynCompatible;

impl DynCompatible {
    /// The reasons `t` is not dyn-compatible; empty when it is.
    fn violations(t: &syn::ItemTrait) -> Vec<String> {
        let mut reasons = Vec::new();
        let is_async_trait = t
            .attrs
            .iter()
            .any(|a| a.path().segments.last().is_some_and(|s| s.ident == "async_trait"));

        if requires_sized(&t.generics) {
            reasons.push("trait-level `where Self: Sized`".to_string());
        }
        for bound in &t.supertraits {
            let syn::TypeParamBound::Trait(tb) = bound else {
                continue;
            };
            let Some(last) = tb.path.segments.last() else { continue };
            let name = last.ident.to_string();
            // These require `Self: Sized`, or default a type parameter to `Self`.
            let self_param = matches!(name.as_str(), "PartialEq" | "PartialOrd")
                && (last.arguments.is_empty() || mentions_bare_self(&last.arguments.to_token_stream().to_string()));
            if matches!(name.as_str(), "Sized" | "Clone" | "Copy" | "Default" | "Eq" | "Ord" | "Hash") || self_param {
                reasons.push(format!("supertrait `{name}`"));
            }
        }

        for item in &t.items {
            match item {
                syn::TraitItem::Const(c) => reasons.push(format!("associated const `{}`", c.ident)),
                syn::TraitItem::Type(ty) if !ty.generics.params.is_empty() => {
                    reasons.push(format!("generic associated type `{}`", ty.ident));
                },
                syn::TraitItem::Fn(f) => {
                    let sig = &f.sig;
                    if requires_sized(&sig.generics) {
                        continue;
                    }
                    let name = &sig.ident;
                    if sig.receiver().is_none() {
                        reasons.push(format!("`{name}` has no `self` receiver"));
                    }
                    if sig.generics.params.iter().any(|p| !matches!(p, syn::GenericParam::Lifetime(_))) {
                        reasons.push(format!("`{name}` has type or const parameters"));
                    }
                    if sig.asyncness.is_some() && !is_async_trait {
                        reasons.push(format!("`{name}` is a native `async fn`"));
                    }
                    for arg in &sig.inputs {
                        let syn::FnArg::Typed(pt) = arg else { continue };
                        let ty = pt.ty.to_token_stream().to_string();
                        if ty.split_whitespace().any(|tok| tok == "impl") {
                            reasons.push(format!("`{name}` takes `impl Trait`"));
                        }
                        if mentions_bare_self(&ty) {
                            reasons.push(format!("`{name}` takes `Self`"));
                        }
                    }
                    if let syn::ReturnType::Type(_, ret) = &sig.output {
                        let ty = ret.to_token_stream().to_string();
                        if ty.split_whitespace().any(|tok| tok == "impl") {
                            reasons.push(format!("`{name}` returns `impl Trait`"));
                        }
                        if mentions_bare_self(&ty) {
                            reasons.push(format!("`{name}` returns `Self`"));
                        }
                    }
                },
                _ => {},
            }
        }
        reasons
    }
}

/// Whether `generics` has a `where Self: Sized` bound, which exempts a method
/// from the dyn-compatibility rules.
fn requires_sized(generics: &syn::Generics) -> bool {
    generics.where_clause.as_ref().is_some_and(|w| {
        w.predicates.iter().any(|p| {
            let syn::WherePredicate::Type(pt) = p else { return false };
            pt.bounded_ty.to_token_stream().to_string() == "Self"
                && pt.bounds.iter().any(|b| b.to_token_stream().to_string() == "Sized")
        })
    })
}

/// Whether stringified `tokens` mention `Self` other than as a projection
/// (`Self :: Item`).
fn mentions_bare_self(tokens: &str) -> bool {
    let toks: Vec<&str> = tokens.split_whitespace().collect();
    toks.iter()
        .enumerate()
        .any(|(i, tok)| *tok == "Self" && toks.get(i + 1) != Some(&"::"))
}

impl Rule for DynCompatible {
    fn name(&self) -> &'static str {
        "check-dyn-compatible"
    }

    fn check(&self, krate: &Crate, findings: &mut Vec<String>) -> usize {
        let public = krate.public_types();
        let traits: Vec<(&ModPath, &syn::ItemTrait)> = public
            .iter()
            .filter_map(|(path, name)| match krate.type_item(path, name) {
                syn::Item::Trait(t) => Some((path, t)),
                _ => None,
            })
            .collect();
        let dyn_compatible: BTreeSet<String> = traits
            .iter()
            .filter(|(_, t)| Self::violations(t).is_empty())
            .map(|(_, t)| t.ident.to_string())
            .collect();

        for (path, t) in &traits {
            let reasons = Self::violations(t);
            if reasons.is_empty() || dyn_compatible.contains(&format!("Dyn{}", t.ident)) {
                continue;
            }
            findings.push(format!(
                "{}: public trait `{}` is not dyn-compatible and has no dyn-compatible `Dyn{}`: {}",
                krate.modules[*path].file.display(),
                t.ident,
                t.ident,
                reasons.join(", ")
            ));
        }
        traits.len()
    }

    fn hint(&self) -> &'static str {
        "   Make the trait dyn-compatible (`where Self: Sized` on methods that cannot be,
   `#[async_trait]` for async methods off the hot path), or — for a
   performance-oriented trait — add a public dyn-compatible `Dyn<Name>` trait
   blanket-implemented for it, as `DynProducer` is for `Producer`."
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PUBLIC_CONSUMER: &str = "org.apache.kafka.clients.consumer.KafkaConsumer";

    /// A crate whose every public item exercises one outcome of
    /// `check-public-audience`.
    const FIXTURE_LIB: &str = r#"
        pub mod consumer {
            pub mod internals {
                #[doc(alias = "org.apache.kafka.clients.consumer.internals.Fetcher")]
                pub struct Fetcher;
            }
            #[doc(alias = "org.apache.kafka.clients.consumer.KafkaConsumer")]
            pub struct KafkaConsumer;
            #[doc(alias = "org.apache.kafka.clients.consumer.MockConsumer")]
            pub struct MockConsumer;
            pub struct Unmarked;
            pub struct Allowed;
            pub(crate) struct Hidden;
        }
        pub mod common {
            pub mod network {
                #[doc(alias = "org.apache.kafka.common.network.Selector")]
                pub struct Selector;
            }
            #[doc(alias = "org.apache.kafka.common.Cluster")]
            pub struct Cluster;
        }
        pub mod ffi {
            pub struct kafka_consumer_KafkaConsumer_t;
            #[unsafe(no_mangle)]
            pub extern "C" fn kafka_consumer_KafkaConsumer_new() {}
            #[unsafe(no_mangle)]
            pub extern "C" fn kafka_common_Cluster_new() {}
            #[unsafe(no_mangle)]
            pub extern "C" fn kafka_consumer_Nothing_new() {}
            #[unsafe(no_mangle)]
            pub extern "C" fn kafka_consumer_StringList_new() {}
            #[unsafe(no_mangle)]
            pub extern "C" fn kafka_common_TopicAuthorizationError_topics() {}
        }
        pub mod generated {
            include!("generated.rs");
        }
        pub use generated::Foo;
    "#;

    const FIXTURE_CBINDGEN: &str = r#"
        [parse]
        include = ["ignored"]

        [export]
        include = [
            "kafka_consumer_KafkaConsumer_t",  # a comment
            "kafka_common_Error_t",
        ]
    "#;

    const ALLOW: &str = "\
        # the fixture allow-list\n\
        crate::consumer::Allowed  # Rust-only\n\
        org.apache.kafka.clients.consumer.MockConsumer  # a test helper\n\
        kafka_consumer_StringList  # a Java `Collection<String>`\n\
        kafka_common_Error  # the error handle\n";

    fn class(package: &str, path: &[&str]) -> java::JavaClass {
        java::JavaClass {
            module: package.split('.').filter(|s| *s != "clients").map(str::to_string).collect(),
            package: package.to_string(),
            path: path.iter().map(|s| s.to_string()).collect(),
            methods: BTreeSet::new(),
            overloads: Vec::new(),
            fields: BTreeMap::new(),
            deprecated: false,
            is_test: false,
        }
    }

    fn fixture_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("xtask-public-audience-{name}-{}", std::process::id()))
    }

    /// The fixture crate written to its own temp dir, and the rule over a
    /// synthetic Java index with allow-list `allow`.
    fn fixture(name: &str, allow: &str) -> (Crate, PublicAudience) {
        let dir = fixture_dir(name);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("lib.rs"), FIXTURE_LIB).unwrap();
        fs::write(dir.join("cbindgen.toml"), FIXTURE_CBINDGEN).unwrap();
        let krate = Crate::load(&dir.join("lib.rs")).unwrap();
        let (allow, allow_errors) = AllowList::parse("allow.txt", allow);
        let rule = PublicAudience {
            index: JavaIndex {
                classes: vec![
                    class("clients.consumer", &["KafkaConsumer"]),
                    class("clients.consumer", &["MockConsumer"]),
                    class("clients.consumer.internals", &["Fetcher"]),
                    class("common.network", &["Selector"]),
                    class("common", &["Cluster"]),
                    class("common.errors", &["TopicAuthorizationException"]),
                ],
            },
            public: [
                PUBLIC_CONSUMER,
                "org.apache.kafka.common.errors.TopicAuthorizationException",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            unsupported: ["common.network".to_string()].into(),
            allow,
            allow_errors,
            cbindgen: dir.join("cbindgen.toml"),
            unsupported_source: "fixture".to_string(),
        };
        (krate, rule)
    }

    fn run(name: &str, allow: &str) -> Vec<String> {
        let (krate, rule) = fixture(name, allow);
        let mut findings = Vec::new();
        rule.check(&krate, &mut findings);
        fs::remove_dir_all(fixture_dir(name)).unwrap();
        findings
    }

    fn with<'a>(findings: &'a [String], needle: &str) -> Vec<&'a str> {
        findings.iter().filter(|f| f.contains(needle)).map(String::as_str).collect()
    }

    #[test]
    fn test_public_internal_item_fails_the_internal_and_public_rules() {
        let findings = run("internal", ALLOW);
        let expected = format!(
            "{}: `crate::consumer::internals::Fetcher` is public but translates \
             `org.apache.kafka.clients.consumer.internals.Fetcher`: its package `clients.consumer.internals` is \
             internal, it is not `@InterfaceAudience.Public` in Kafka {}",
            fixture_dir("internal").join("lib.rs").display(),
            java::AUDIENCE_REF
        );
        assert_eq!(with(&findings, "Fetcher"), [expected]);
    }

    #[test]
    fn test_public_disclaimer_item_fails_the_disclaimer_and_public_rules() {
        let findings = run("disclaimer", ALLOW);
        let expected = format!(
            "{}: `crate::common::network::Selector` is public but translates \
             `org.apache.kafka.common.network.Selector`: its package `common.network` is not a supported Kafka \
             API, it is not `@InterfaceAudience.Public` in Kafka {}",
            fixture_dir("disclaimer").join("lib.rs").display(),
            java::AUDIENCE_REF
        );
        assert_eq!(with(&findings, "Selector"), [expected]);
    }

    #[test]
    fn test_public_item_outside_the_public_list_fails_only_that_rule() {
        let findings = run("not-public", ALLOW);
        let expected = format!(
            "{}: `crate::common::Cluster` is public but translates `org.apache.kafka.common.Cluster`: it is not \
             `@InterfaceAudience.Public` in Kafka {}",
            fixture_dir("not-public").join("lib.rs").display(),
            java::AUDIENCE_REF
        );
        assert_eq!(with(&findings, "`crate::common::Cluster`"), [expected]);
    }

    #[test]
    fn test_public_and_allow_listed_items_pass() {
        let findings = run("pass", ALLOW);
        // Public in Java, in Rust and in C.
        assert!(with(&findings, "KafkaConsumer").is_empty(), "{findings:#?}");
        // Allow-listed by Rust path, by Java class and by C prefix.
        assert!(with(&findings, "Allowed").is_empty(), "{findings:#?}");
        assert!(with(&findings, "MockConsumer").is_empty(), "{findings:#?}");
        assert!(with(&findings, "StringList").is_empty(), "{findings:#?}");
        assert!(with(&findings, "kafka_common_Error").is_empty(), "{findings:#?}");
        // A C error handle resolves to its exception in `common.errors`.
        assert!(with(&findings, "TopicAuthorizationError").is_empty(), "{findings:#?}");
        // Not public at all.
        assert!(with(&findings, "Hidden").is_empty(), "{findings:#?}");
    }

    #[test]
    fn test_unmarked_public_item_needs_an_allow_list_entry() {
        let findings = run("unmarked", ALLOW);
        let expected = format!(
            "{}: `crate::consumer::Unmarked` is public but carries no Java marker; mark the class it translates, or \
             add it to {PUBLIC_AUDIENCE_ALLOWLIST} with the reason it is public",
            fixture_dir("unmarked").join("lib.rs").display()
        );
        assert_eq!(with(&findings, "Unmarked"), [expected]);
        // Without the allow-list, the Rust-only item is a finding too.
        let bare = run("unmarked-bare", "");
        assert_eq!(
            with(&bare, "`crate::consumer::Allowed` is public but carries no Java marker").len(),
            1
        );
    }

    #[test]
    fn test_pub_generated_module_and_its_pub_use_are_findings() {
        let findings = run("generated", ALLOW);
        let file = fixture_dir("generated").join("lib.rs").display().to_string();
        assert_eq!(
            with(&findings, "generated module").len() + with(&findings, "generated protocol").len(),
            2,
            "{findings:#?}"
        );
        assert!(findings.contains(&format!(
            "{file}: `pub mod generated` makes the generated protocol code public; make it `pub(crate)`"
        )));
        assert!(findings.contains(&format!(
            "{file}: a `pub use` re-exports the generated module `generated`; make it `pub(crate) use`"
        )));
    }

    #[test]
    fn test_ffi_names_for_a_non_public_or_unknown_class_are_findings() {
        let findings = run("ffi", ALLOW);
        let file = fixture_dir("ffi").join("lib.rs").display().to_string();
        assert_eq!(
            with(&findings, "C `kafka_common_Cluster`"),
            [format!(
                "{file}: C `kafka_common_Cluster` (1 symbol(s)) binds `org.apache.kafka.common.Cluster`, which may \
                 not be public: it is not `@InterfaceAudience.Public` in Kafka {}",
                java::AUDIENCE_REF
            )]
        );
        assert_eq!(
            with(&findings, "C `kafka_consumer_Nothing`"),
            [format!(
                "{file}: C `kafka_consumer_Nothing` (1 symbol(s)) names no Java class (`kafka_<pkg>_<Class>_…`, with \
                 `<pkg>` the Java package without `clients`)"
            )]
        );
        // The C `KafkaConsumer` prefix groups the fn with the `_t` type of both
        // the module and the include list.
        assert!(with(&findings, "C `kafka_consumer_KafkaConsumer`").is_empty(), "{findings:#?}");
    }

    #[test]
    fn test_ffi_name_in_the_wrong_package_says_where_java_declares_it() {
        let (_, mut rule) = fixture("misplaced", ALLOW);
        fs::remove_dir_all(fixture_dir("misplaced")).unwrap();
        rule.index.classes.push(class("common", &["TopicPartition"]));
        assert_eq!(rule.resolve_ffi_class(&["consumer"], "TopicPartition"), None);
        assert_eq!(
            rule.misplaced_ffi_class("TopicPartition"),
            "; Java declares it as `org.apache.kafka.common.TopicPartition`"
        );
        assert_eq!(rule.misplaced_ffi_class("Nothing"), "");
    }

    #[test]
    fn test_stale_and_malformed_allow_list_entries_are_findings() {
        let allow = format!("{ALLOW}crate::consumer::Gone  # removed\nno_reason\n");
        let findings = run("stale", &allow);
        assert_eq!(
            with(&findings, "allow.txt:"),
            ["allow.txt:7: expected `<key>  # <reason>`, found `no_reason`"]
        );
        assert_eq!(
            with(&findings, "lets nothing through"),
            [format!(
                "{PUBLIC_AUDIENCE_ALLOWLIST}:6: `crate::consumer::Gone` lets nothing through any more; remove it"
            )]
        );
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("crate::common::Error", "crate::common::Error"));
        assert!(!glob_match("crate::common::Error", "crate::common::Errors"));
        assert!(glob_match("crate::common::Local*Error", "crate::common::LocalTimeoutError"));
        assert!(!glob_match("crate::common::Local*Error", "crate::common::LocalTimeout"));
        assert!(glob_match(
            "crate::consumer::*ConsumerHandle",
            "crate::consumer::async_kafka_consumer::ConsumerHandle"
        ));
        assert!(glob_match("a*b*c", "a-b-b-c"));
        assert!(!glob_match("a*bc*bc", "abc"));
        assert!(glob_match("x*", "x"));
    }

    #[test]
    fn test_ffi_class_parses_package_and_class() {
        assert_eq!(
            ffi_class("kafka_consumer_KafkaConsumer_poll"),
            Some((vec!["consumer"], "KafkaConsumer"))
        );
        assert_eq!(
            ffi_class("kafka_common_acl_AclBinding_t"),
            Some((vec!["common", "acl"], "AclBinding"))
        );
        assert_eq!(ffi_class("kafka_consumer_string_destroy"), None);
        assert_eq!(ffi_class("kafka_Error_t"), None);
        assert_eq!(ffi_class("other_Thing"), None);
    }

    #[test]
    fn test_cbindgen_includes_reads_only_the_export_list() {
        assert_eq!(
            cbindgen_includes(FIXTURE_CBINDGEN),
            ["kafka_consumer_KafkaConsumer_t", "kafka_common_Error_t"]
        );
        assert!(cbindgen_includes("[export]\nexclude = [\"a\"]\n").is_empty());
    }

    #[test]
    fn test_parse_public_list_skips_comments_and_blanks() {
        let list = parse_public_list("# header\n\norg.apache.kafka.A\n  org.apache.kafka.B  \n");
        assert_eq!(
            list,
            ["org.apache.kafka.A".to_string(), "org.apache.kafka.B".to_string()].into()
        );
    }
}

#[cfg(test)]
mod dyn_compatible_tests {
    use super::*;

    /// A crate whose every public trait exercises one outcome of
    /// `check-dyn-compatible`.
    const FIXTURE_LIB: &str = r#"
        pub trait Plain {
            fn get(&self) -> i32;
            fn projection(&self) -> Option<Self::Item>;
            type Item;
        }
        pub trait Generic {
            fn map<T>(&self, t: T);
        }
        pub trait ImplReturn {
            fn run(&self) -> impl std::future::Future<Output = ()>;
        }
        pub trait ImplArg {
            fn run(&self, f: impl Fn());
        }
        pub trait SelfReturn {
            fn dup(&self) -> Self;
        }
        pub trait SelfArg {
            fn merge(&self, other: Self);
        }
        pub trait NoReceiver {
            fn create() -> i32;
        }
        pub trait CloneSuper: Clone {}
        pub trait PartialEqSuper: PartialEq {}
        pub trait PartialEqOther: PartialEq<i32> {}
        pub trait TraitSized where Self: Sized {}
        pub trait Consts {
            const N: i32;
        }
        pub trait Gat {
            type Out<'a>;
        }
        pub trait MethodSized {
            fn get(&self) -> i32;
            fn dup(&self) -> Self where Self: Sized;
            fn create() -> Self where Self: Sized;
            fn map<T>(&self, t: T) where Self: Sized;
        }
        pub trait NativeAsync {
            async fn run(&self);
        }
        #[async_trait::async_trait]
        pub trait AsyncTrait {
            async fn run(&self);
        }
        pub trait Fast {
            fn run(&self) -> impl std::future::Future<Output = ()>;
        }
        pub trait DynFast {
            fn run(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + '_>>;
        }
        pub trait Slow {
            fn run(&self) -> impl std::future::Future<Output = ()>;
        }
        pub trait DynSlow {
            fn run<T>(&self, t: T);
        }
        pub(crate) trait Hidden {
            fn map<T>(&self, t: T);
        }
        mod private {
            pub trait Unreachable {
                fn map<T>(&self, t: T);
            }
        }
    "#;

    /// The findings over the fixture crate, written to its own temp dir `name`
    /// because the tests run in parallel.
    fn run(name: &str) -> Vec<String> {
        let dir = std::env::temp_dir().join(format!("xtask-dyn-compatible-{name}-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("lib.rs"), FIXTURE_LIB).unwrap();
        let krate = Crate::load(&dir.join("lib.rs")).unwrap();
        let mut findings = Vec::new();
        let checked = DynCompatible.check(&krate, &mut findings);
        fs::remove_dir_all(&dir).unwrap();
        // Every `pub trait` above except `Hidden` and `Unreachable`.
        assert_eq!(checked, 20);
        findings
    }

    /// The finding for trait `name`, if any.
    fn finding<'a>(findings: &'a [String], name: &str) -> Option<&'a str> {
        let needle = format!("public trait `{name}` ");
        findings.iter().find(|f| f.contains(&needle)).map(String::as_str)
    }

    #[test]
    fn test_violations_are_reported_with_their_reason() {
        let findings = run("violations");
        for (name, reason) in [
            ("Generic", "`map` has type or const parameters"),
            ("ImplReturn", "`run` returns `impl Trait`"),
            ("ImplArg", "`run` takes `impl Trait`"),
            ("SelfReturn", "`dup` returns `Self`"),
            ("SelfArg", "`merge` takes `Self`"),
            ("NoReceiver", "`create` has no `self` receiver"),
            ("CloneSuper", "supertrait `Clone`"),
            ("PartialEqSuper", "supertrait `PartialEq`"),
            ("TraitSized", "trait-level `where Self: Sized`"),
            ("Consts", "associated const `N`"),
            ("Gat", "generic associated type `Out`"),
            ("NativeAsync", "`run` is a native `async fn`"),
            ("Slow", "`run` returns `impl Trait`"),
            ("DynSlow", "`run` has type or const parameters"),
        ] {
            let f = finding(&findings, name).unwrap_or_else(|| panic!("no finding for `{name}`: {findings:#?}"));
            assert!(f.contains(reason), "`{name}` finding lacks `{reason}`: {f}");
        }
        assert_eq!(findings.len(), 14, "{findings:#?}");
    }

    #[test]
    fn test_dyn_compatible_traits_pass() {
        let findings = run("pass");
        // `Self::Item` is a projection, not a bare `Self`; `PartialEq<i32>` does
        // not default its parameter to `Self`; `where Self: Sized` exempts a
        // method; `#[async_trait]` boxes the future.
        for name in ["Plain", "PartialEqOther", "MethodSized", "AsyncTrait", "DynFast"] {
            assert_eq!(finding(&findings, name), None);
        }
    }

    #[test]
    fn test_dyn_companion_exempts_only_when_itself_dyn_compatible() {
        let findings = run("companion");
        assert_eq!(finding(&findings, "Fast"), None);
        assert!(finding(&findings, "Slow").unwrap().contains("has no dyn-compatible `DynSlow`"));
    }

    #[test]
    fn test_non_public_traits_are_ignored() {
        let findings = run("hidden");
        assert_eq!(finding(&findings, "Hidden"), None);
        assert_eq!(finding(&findings, "Unreachable"), None);
    }
}
