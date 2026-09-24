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
}

/// The rules `lint-custom` runs, in order.
fn rules() -> Vec<Box<dyn Rule>> {
    vec![Box::new(NoDataCarryingEnumVariants), Box::new(NoPublicField)]
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

    /// The `pub struct`s and `pub enum`s another crate can name, as (defining
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

/// The name of a `pub struct` or `pub enum` item; `None` for any other item.
fn pub_type_ident(item: &syn::Item) -> Option<&syn::Ident> {
    match item {
        syn::Item::Struct(s) if matches!(s.vis, syn::Visibility::Public(_)) => Some(&s.ident),
        syn::Item::Enum(e) if matches!(e.vis, syn::Visibility::Public(_)) => Some(&e.ident),
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
