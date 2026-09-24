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
        // Disabled until every public struct is confirmed to need being public:
        // making traits dyn-compatible is wasted on types that should become
        // `pub(crate)`. The trait changes it requires are parked on branch
        // `wip/dyn-compatible-traits`.
        // Box::new(DynCompatible),
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
        // The names each module exposes (`pub` items and `pub use`s), each
        // resolved to the (module, name) that may define it.
        let mut public: BTreeSet<(ModPath, String)> = BTreeSet::new();
        let mut work: Vec<(ModPath, String)> = Vec::new();
        let mut glob_work: Vec<ModPath> = Vec::new();

        for (path, module) in &self.modules {
            if !self.is_reachable(path) {
                continue;
            }
            for ident in module.items.iter().filter_map(pub_type_ident) {
                work.push((path.clone(), ident.to_string()));
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
                    for ident in m.items.iter().filter_map(pub_type_ident) {
                        work.push((module.clone(), ident.to_string()));
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
                    .is_some_and(|m| m.items.iter().filter_map(pub_type_ident).any(|ident| ident == name))
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
        JavaName { index, package_modules }
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

/// The first segment of each path a `use` tree imports, skipping a leading
/// `self`: `use a::{b, c}` → `a`; `use {a::b, c}` → `a`, `c`.
fn use_roots(tree: &syn::UseTree, out: &mut Vec<syn::Ident>) {
    match tree {
        syn::UseTree::Path(p) if p.ident == "self" => use_roots(&p.tree, out),
        syn::UseTree::Path(p) => out.push(p.ident.clone()),
        syn::UseTree::Name(n) => out.push(n.ident.clone()),
        syn::UseTree::Rename(r) => out.push(r.ident.clone()),
        syn::UseTree::Glob(_) => {},
        syn::UseTree::Group(g) => g.items.iter().for_each(|t| use_roots(t, out)),
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
    /// another package. Macros the file defines may be re-exported too.
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
            let mut roots = Vec::new();
            use_roots(&u.tree, &mut roots);
            for root in roots {
                checked += 1;
                let name = root.to_string();
                let mut child = module.to_vec();
                child.push(name.clone());
                let foreign = if macros.contains(&name) {
                    None
                } else if !children.contains(&name) {
                    Some(format!("`{name}`, outside this module"))
                } else if self.package_modules.contains(&child) {
                    Some(format!("package `{}`", module_path(&child)))
                } else {
                    None
                };
                if let Some(from) = foreign {
                    findings.push(format!(
                        "{file}: `{}` re-exports from {from} into `{}`; import it from its own package instead",
                        u.to_token_stream().to_string().replace(" :: ", "::"),
                        module_path(module)
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
/// `Sized`-implying or `Self`-parameterized supertrait, no associated consts or
/// generic associated types, and every method not opted out with
/// `where Self: Sized` has a receiver, no type parameters, no `impl Trait` in
/// argument or return position, is not a native `async fn` (an
/// `#[async_trait]` one is boxed and fine), and does not mention `Self` outside
/// the receiver except through a projection such as `Self::Item`.
///
/// Scope: the same public items as [`NoPublicField`], traits only.
// Not run yet: see the commented-out entry in `rules()`.
#[allow(dead_code)]
struct DynCompatible;

#[allow(dead_code)]
impl DynCompatible {
    /// The reasons `t` is not dyn-compatible; empty when it is.
    fn violations(t: &syn::ItemTrait) -> Vec<String> {
        let mut reasons = Vec::new();
        let is_async_trait = t
            .attrs
            .iter()
            .any(|a| a.path().segments.last().is_some_and(|s| s.ident == "async_trait"));

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
#[allow(dead_code)]
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
#[allow(dead_code)]
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
