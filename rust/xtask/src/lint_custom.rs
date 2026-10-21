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

mod derive_translation;
mod ffi_translation;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use quote::ToTokens;
use syn::punctuated::Punctuated;

use crate::java::{self, same_name, squash, JavaIndex};

/// A module path from the crate root, e.g. `["producer", "producer_record"]`.
type ModPath = Vec<String>;

/// What every rule is built from: the Java index, loaded once, and the
/// options of this run.
struct Context {
    java: Rc<JavaIndex>,
    /// The `check-ffi-translation` baseline to apply; `None` shows every finding.
    ffi_baseline: Option<PathBuf>,
    /// The cbindgen configuration whose `[enum] prefix_with_name` decides how
    /// the C enumerators are spelled; `None` skips that check.
    cbindgen_config: Option<PathBuf>,
}

/// One source-level rule run by `lint-custom`.
trait Rule {
    /// The rule's name, as printed in the report.
    fn name(&self) -> &'static str;

    /// Checks the crate, pushing one line per violation onto `findings`, and
    /// returns how many items it inspected.
    fn check(&self, krate: &Crate, findings: &mut Vec<String>) -> usize;

    /// How to fix a violation, printed once after the rule's findings.
    fn hint(&self) -> &'static str;

    /// Why the rule cannot run in this checkout, if it cannot. A rule that
    /// cannot run fails the lint: a skip that exits 0 would let a checkout
    /// without the `kafka` submodule pass while running only half the rules.
    fn skip_reason(&self) -> Option<String> {
        None
    }
}

/// The rules `lint-custom` runs, in order.
fn rules(ctx: &Context) -> Vec<Box<dyn Rule>> {
    vec![
        Box::new(NoDataCarryingEnumVariants),
        Box::new(EnumTranslation::new(ctx)),
        Box::new(FunctionalInterfaceTrait::new(ctx)),
        Box::new(NoPublicField),
        Box::new(NoFixedSizeArray),
        Box::new(JavaName::new(ctx)),
        Box::new(NoDeprecatedTranslation::new(ctx)),
        Box::new(PublicAudience::new(ctx)),
        Box::new(DynCompatible),
        Box::new(DynForwarding),
        Box::new(ConsumerHandleSurface),
        Box::new(ConsumerReceiver),
        Box::new(SendFuture),
        Box::new(ErrorPredicate),
        Box::new(derive_translation::DeriveTranslation::new(ctx)),
        Box::new(derive_translation::ThreadSafetyProbe::new()),
        Box::new(ffi_translation::FfiTranslation::new(ctx)),
    ]
}

/// Runs every rule — or the one `args` names — and fails if any of them
/// reports a finding. `--no-baseline` shows the `check-ffi-translation`
/// findings the baseline waives.
pub fn lint_custom(args: &[String]) -> anyhow::Result<()> {
    let mut only: Option<&str> = None;
    let mut baseline = true;
    for arg in args {
        match arg.as_str() {
            "--no-baseline" => baseline = false,
            name if name.starts_with("check-") && only.is_none() => only = Some(name),
            other => {
                anyhow::bail!("unknown lint-custom argument `{other}`; usage: lint-custom [rule-name] [--no-baseline]")
            },
        }
    }
    println!("🔍 Running custom lint rules...");

    let ctx = Context {
        java: Rc::new(JavaIndex::load()),
        ffi_baseline: baseline.then(|| PathBuf::from(ffi_translation::FFI_BASELINE)),
        cbindgen_config: Some(PathBuf::from(ffi_translation::CBINDGEN_CONFIG)),
    };
    let rules: Vec<Box<dyn Rule>> = rules(&ctx).into_iter().filter(|r| only.is_none_or(|o| o == r.name())).collect();
    if let Some(name) = only.filter(|_| rules.is_empty()) {
        anyhow::bail!("no custom lint rule named `{name}`");
    }

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

    for rule in rules {
        if let Some(reason) = rule.skip_reason() {
            eprintln!("\n❌ {}: cannot run, {reason}", rule.name());
            failed += 1;
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

/// `cargo xtask ffi-baseline`: rewrites [`ffi_translation::FFI_BASELINE`] with
/// every current `check-ffi-translation` finding, so the C FFI refactor can
/// burn it down phase by phase (CLAUDE.md §4).
pub fn write_ffi_baseline() -> anyhow::Result<()> {
    println!("🔧 Writing the check-ffi-translation baseline...");
    let ctx = Context {
        java: Rc::new(JavaIndex::load()),
        ffi_baseline: None,
        cbindgen_config: Some(PathBuf::from(ffi_translation::CBINDGEN_CONFIG)),
    };
    let rule = ffi_translation::FfiTranslation::new(&ctx);
    if let Some(reason) = rule.skip_reason() {
        anyhow::bail!("cannot run check-ffi-translation: {reason}");
    }
    let krate = Crate::load(Path::new("src/lib.rs"))?;
    if !krate.parse_errors.is_empty() {
        anyhow::bail!(
            "{} file(s) could not be parsed: {}",
            krate.parse_errors.len(),
            krate.parse_errors.join("; ")
        );
    }
    let (findings, _) = rule.findings(&krate);
    let mut out = String::from(
        "# check-ffi-translation findings the C FFI refactor has not reached yet (CLAUDE.md §4).\n\
         # One `<kind> <symbol>` per line, or `prefix:<C prefix>` to waive every symbol under it.\n\
         # A line matching no finding any more is itself a finding: delete it. Rewrite with\n\
         # `cargo xtask ffi-baseline`; see everything with\n\
         # `cargo xtask lint-custom check-ffi-translation --no-baseline`.\n",
    );
    let mut keys: Vec<String> = findings.iter().map(ffi_translation::Finding::key).collect();
    keys.dedup();
    for key in &keys {
        out.push_str(key);
        out.push('\n');
    }
    fs::write(ffi_translation::FFI_BASELINE, out)?;
    println!("✅ Wrote {} finding(s) to {}", keys.len(), ffi_translation::FFI_BASELINE);
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
    /// [`EnumTranslation`] uses.
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

    /// Every trait of the crate, public or not, with its module path.
    fn traits(&self) -> impl Iterator<Item = (&ModPath, &syn::ItemTrait)> {
        self.modules.iter().flat_map(|(path, module)| {
            module.items.iter().filter_map(move |item| match item {
                syn::Item::Trait(t) => Some((path, t)),
                _ => None,
            })
        })
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

    /// The attributes of the item `path` names from `module` (`internals::RecordHeader`),
    /// following `use` chains to where it is defined; `None` if it is not in
    /// this crate.
    fn definition_attrs(&self, module: &[String], path: &[String]) -> Option<Vec<syn::Attribute>> {
        let (name, parent) = path.split_last()?;
        let mut work = vec![self.resolve_module(module, parent)?];
        let mut seen = BTreeSet::new();
        while let Some(module) = work.pop() {
            if !seen.insert(module.clone()) {
                continue;
            }
            let Some(m) = self.modules.get(&module) else { continue };
            if let Some((_, attrs)) = m.items.iter().filter_map(item_def).find(|(n, _)| n == name) {
                return Some(attrs);
            }
            for (target, used) in self.uses(&module, false) {
                if used.as_deref() == Some(name.as_str()) {
                    work.push(target);
                }
            }
        }
        None
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

/// Whether one of `attrs` is a `#[cfg(..)]` whose predicate holds only when
/// `test` is set — `cfg(test)`, `cfg(all(test, ..))` — so the item never ships
/// in a non-test build. `cfg(not(test))`, `cfg(feature = "..test..")` and any
/// predicate that can hold without `test` do not count: those items ship.
fn is_cfg_test(attrs: &[syn::Attribute]) -> bool {
    attrs
        .iter()
        .filter(|a| a.path().is_ident("cfg"))
        .any(|a| a.parse_args::<syn::Meta>().is_ok_and(|pred| cfg_implies_test(&pred)))
}

/// Whether cfg predicate `pred` can hold only when `test` is set. Errs toward
/// `false`, which keeps the item under the lint rules.
fn cfg_implies_test(pred: &syn::Meta) -> bool {
    match pred {
        syn::Meta::Path(path) => path.is_ident("test"),
        syn::Meta::NameValue(_) => false,
        syn::Meta::List(list) => {
            let Ok(args) = list.parse_args_with(Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated) else {
                return false;
            };
            if list.path.is_ident("all") {
                // Every conjunct must hold, so one requiring `test` suffices.
                args.iter().any(cfg_implies_test)
            } else if list.path.is_ident("any") {
                // Any disjunct may hold, so each must require `test`.
                !args.is_empty() && args.iter().all(cfg_implies_test)
            } else if list.path.is_ident("not") {
                // `not(not(p))` is `p`; any other negation can hold without `test`.
                match args.first() {
                    Some(syn::Meta::List(inner)) if args.len() == 1 && inner.path.is_ident("not") => {
                        inner.parse_args::<syn::Meta>().is_ok_and(|p| cfg_implies_test(&p))
                    },
                    _ => false,
                }
            } else {
                false
            }
        },
    }
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
/// matches the Java source: a data-carrying enum translates a Java class
/// hierarchy (a Java enum cannot give its constants per-constant shapes), so
/// one named type per variant is the faithful shape. That the Java type is a
/// hierarchy is [`EnumTranslation`]'s check, not this one's.
///
/// The wrapped type must be one this crate defines — a struct, an enum, a trait
/// (`Box<dyn Gauge>`) or an error macro's type — possibly behind `Box`, `Arc`
/// or `Rc`, or one of the enum's own type parameters. A type from `std` or
/// another crate (`TopicIds(Vec<Uuid>)`, `Timestamp(i64)`) cannot carry the
/// variant's forward compatibility: the field the Java subclass keeps private
/// is the payload itself, constructed and destructured by any caller, so the
/// field a later Kafka version adds to the subclass has nowhere to go. A
/// `rust-only` enum stands for no Java hierarchy — it is a value wrapper, such
/// as `MetricValue::Double(f64)` for the `Object` Java's `metricValue()`
/// returns — so its newtype variants may wrap any type; the inline-fields
/// check still applies to it.
///
/// Marking the *variant* `#[non_exhaustive]` is deliberately not the rule: on a
/// tuple variant that attribute makes the variant wholly private to other crates
/// (E0603) — not merely unconstructable but *unmatchable*, so
/// `match err { Error::Api(e) => .. }` stops compiling downstream.
///
/// Scope: every `pub enum` another crate can reach, through a `pub` module
/// chain or a `pub use` ([`Crate::public_names`]). An enum of a `pub(crate)`
/// module, such as `compress::CompressingWriter`, owes no forward compatibility.
struct NoDataCarryingEnumVariants;

impl Rule for NoDataCarryingEnumVariants {
    fn name(&self) -> &'static str {
        "check-no-data-carrying-enum-variants"
    }

    fn check(&self, krate: &Crate, findings: &mut Vec<String>) -> usize {
        // The types the crate defines, whatever their visibility or module:
        // a wrapped type the crate owns can be given private fields or
        // `#[non_exhaustive]`; one it does not own cannot.
        let defined: BTreeSet<String> = krate
            .modules
            .values()
            .flat_map(|m| m.items.iter())
            .filter_map(|item| match item {
                syn::Item::Struct(s) => Some(s.ident.to_string()),
                syn::Item::Enum(e) => Some(e.ident.to_string()),
                syn::Item::Trait(t) => Some(t.ident.to_string()),
                syn::Item::Macro(m) => error_macro_type_def(m).map(|def| def.name),
                // A type alias is transparent: it hides nothing.
                _ => None,
            })
            .collect();
        let pub_enum = |item: &syn::Item| match item {
            syn::Item::Enum(e) if matches!(e.vis, syn::Visibility::Public(_)) => Some(e.ident.to_string()),
            _ => None,
        };
        let mut checked = 0usize;
        for (path, name) in krate.public_names(pub_enum) {
            let module = &krate.modules[&path];
            let Some(syn::Item::Enum(e)) = module.items.iter().find(|item| pub_enum(item).as_deref() == Some(&name))
            else {
                continue;
            };
            let value_wrapper = has_tag(&e.attrs, RUST_ONLY);
            for v in &e.variants {
                checked += 1;
                let why = match &v.fields {
                    // Carries nothing.
                    syn::Fields::Unit => continue,
                    // A newtype delegates forward compatibility to the type it
                    // wraps, which must therefore be the crate's own — unless
                    // the enum translates no Java hierarchy.
                    syn::Fields::Unnamed(f) if f.unnamed.len() == 1 => {
                        let ty = &f.unnamed[0].ty;
                        let owned = wrapped_type_name(ty).is_some_and(|name| {
                            defined.contains(&name) || e.generics.type_params().any(|p| p.ident == name)
                        });
                        if owned || value_wrapper {
                            continue;
                        }
                        format!("wraps `{}`, a type the crate does not define", compact(ty))
                    },
                    // Two or more fields have nothing to delegate to and can
                    // be destructured positionally downstream.
                    syn::Fields::Unnamed(f) => format!("holds data inline ({} unnamed fields)", f.unnamed.len()),
                    syn::Fields::Named(_) => "holds data inline (inline named fields)".to_string(),
                };
                findings.push(format!("{}: `{}::{}` {why}", module.file.display(), e.ident, v.ident));
            }
        }
        checked
    }

    fn hint(&self) -> &'static str {
        "   Move the fields into their own struct and wrap it, e.g.:
       pub struct GzipCompression { level: i32 }
       Gzip(GzipCompression),   // instead of Gzip { level: i32 }
   A newtype must wrap a type this crate defines, as Java's subclass does:
       pub struct TimestampSpec { timestamp: i64 }
       Timestamp(TimestampSpec), // instead of Timestamp(i64)
   The struct then carries forward compatibility (private fields, or
   #[non_exhaustive]); do NOT mark the variant itself."
    }
}

/// The type a newtype variant delegates to: `ty` with `Box` / `Arc` / `Rc`
/// peeled, named by its last path segment (`dyn Trait` by the trait). `None`
/// for a shape that names no type (a tuple, a slice, a reference, a pointer).
fn wrapped_type_name(ty: &syn::Type) -> Option<String> {
    match ty {
        syn::Type::Path(p) => {
            let last = p.path.segments.last()?;
            let ident = last.ident.to_string();
            if matches!(ident.as_str(), "Box" | "Arc" | "Rc") {
                let syn::PathArguments::AngleBracketed(args) = &last.arguments else {
                    return None;
                };
                return match args.args.first()? {
                    syn::GenericArgument::Type(inner) => wrapped_type_name(inner),
                    _ => None,
                };
            }
            Some(ident)
        },
        syn::Type::TraitObject(t) => t.bounds.iter().find_map(|b| match b {
            syn::TypeParamBound::Trait(t) => t.path.segments.last().map(|s| s.ident.to_string()),
            _ => None,
        }),
        syn::Type::Paren(p) => wrapped_type_name(&p.elem),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Rule: check-enum-translation
// ---------------------------------------------------------------------------

/// Checks that a public enum translates a Java enum or a Java class hierarchy,
/// never a plain class (CLAUDE.md §3, forward compatibility).
///
/// A Java class keeps its state in private fields behind getters, so a later
/// minor can add a field, change how one is stored, or add a constructor. An
/// enum makes its variants and their payloads public API instead: another
/// crate matches them and destructures what they carry, so the same Java
/// change breaks it. That is why a class becomes a struct with private fields
/// (`check-no-public-field`), and an enum stands for a Java type only when
/// Java's own type is a closed set of shapes:
///   - a Java `enum`;
///   - a class hierarchy, each variant one subclass: an `interface`, an
///     `abstract` class, or a class that some class of its package extends
///     (`OffsetSpec` and its nested `EarliestSpec`, `LatestSpec`, ...).
///
/// A class nothing extends — `DescribeTopicsResult`, whose two nullable maps
/// once became `ByTopicId` / `ByTopicName` variants — is a finding, however
/// convenient the enum is for an invariant Java enforces in its constructor.
///
/// Scope: every `pub enum` of [`NoDataCarryingEnumVariants`]'s scope with a
/// class marker (`#[doc(alias = "org.apache.kafka.<package>.<Class>")]`, no
/// `#member`) the Java index resolves; a marker it cannot resolve is
/// `check-java-name`'s. Runs only when the `kafka` submodule is checked out.
struct EnumTranslation {
    index: JavaIndex,
}

impl EnumTranslation {
    fn new(ctx: &Context) -> Self {
        EnumTranslation { index: (*ctx.java).clone() }
    }
}

impl Rule for EnumTranslation {
    fn name(&self) -> &'static str {
        "check-enum-translation"
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
                for marker in java_markers(&e.attrs).iter().filter(|m| !m.contains('#')) {
                    let Some(class) = self.index.resolve(marker) else {
                        continue;
                    };
                    checked += 1;
                    let is_hierarchy =
                        class.is_abstract || class.kind == java::JavaKind::Interface || self.index.has_subclass(class);
                    if class.kind == java::JavaKind::Enum || is_hierarchy {
                        continue;
                    }
                    let kind = match class.kind {
                        java::JavaKind::Record => "a record",
                        _ => "a class",
                    };
                    findings.push(format!(
                        "{}: `{}` is an enum but translates `{marker}`, {kind} that is neither a Java enum nor \
                         extended by any class",
                        module.file.display(),
                        e.ident
                    ));
                }
            }
        }
        checked
    }

    fn hint(&self) -> &'static str {
        "   Translate the class as a struct with private fields, its constructor and getters, as Java declares
   them. An invariant Java checks in its constructor stays a check there, not a choice of variants."
    }

    fn skip_reason(&self) -> Option<String> {
        self.index.is_empty().then(|| {
            format!(
                "no Java sources at `{}` (run `git submodule update --init kafka`)",
                java::JAVA_MAIN_ROOT
            )
        })
    }
}

// ---------------------------------------------------------------------------
// Rule: check-functional-interface-trait
// ---------------------------------------------------------------------------

/// Checks that a public Java `@FunctionalInterface` is translated as a trait
/// (CLAUDE.md §3, forward compatibility).
///
/// A functional interface has one abstract method today, but a minor Java
/// release may add methods to it: `default` overloads of that method, as
/// `Deserializer` gained in 2.1 and 3.6, or `default` helpers. A trait takes
/// each as a default method, and the Rust name of an overload is derived as
/// for any other method (CLAUDE.md §2). A closure type (`Box<dyn Fn ..>`, an
/// `impl Fn` parameter), an enum or a struct has nowhere to put them, so the
/// same Java change would break the Rust API.
///
/// Public: declared `public` (implicitly so inside an interface), and its
/// top-level class may be public by CLAUDE.md §2 ([`audience_failures`]).
/// Each such interface is a finding when:
///   - a Rust item translating it (its class marker), of any visibility and
///     outside the C FFI module, is not a trait: a type alias of a closure,
///     an enum, a struct;
///   - no Rust item translates it, yet a public Rust method translates
///     a Java method taking it, so the interface was turned into a closure
///     parameter. The Java method is the one the Rust method's
///     `Class#method` marker names (an overload when the marker gives its
///     parameters, else every overload of the name); it takes the interface
///     when a parameter's type is the interface's simple name and the method
///     is declared in the interface's package. The index records no imports,
///     so a use from another package is not seen.
///
///   - the trait translating it lacks a factory: a public fn of the trait's
///     module taking a function of the method's shape and returning
///     `impl <Trait>` (CLAUDE.md §3). The abstract method's is `new`; each
///     `default` overload of it gets `with_<params>`, named by the parameters
///     it adds (CLAUDE.md §2). An interface declaring no abstract method uses
///     the one it inherits from the first supertype the index resolves
///     (`Gauge` from `MetricValueProvider`). A nested interface's factories
///     live in the outer class's module, prefixed with its name
///     (`kafka_future::BaseFunction_new`, CLAUDE.md §2).
///
/// Runs only when the `kafka` submodule and the Public list are present.
struct FunctionalInterfaceTrait {
    index: JavaIndex,
    /// [`PUBLIC_AUDIENCE_LIST`].
    public: BTreeSet<String>,
    /// The dotted packages (below `org.apache.kafka`) carrying the
    /// unsupported-API disclaimer.
    unsupported: BTreeSet<String>,
}

impl FunctionalInterfaceTrait {
    fn new(ctx: &Context) -> Self {
        FunctionalInterfaceTrait {
            index: (*ctx.java).clone(),
            public: parse_public_list(&fs::read_to_string(PUBLIC_AUDIENCE_LIST).unwrap_or_default()),
            unsupported: java::unsupported_packages_at_ref(java::AUDIENCE_REF).unwrap_or_default(),
        }
    }

    /// Whether `class` is a public `@FunctionalInterface`.
    fn is_public_functional(&self, class: &java::JavaClass) -> bool {
        let top = format!("{}{}.{}", java::MARKER_PREFIX, class.package, class.path[0]);
        class.functional_interface
            && !class.is_test
            && class.visibility == java::Visibility::Public
            && audience_failures(&self.public, &self.unsupported, &top).is_empty()
    }

    /// Whether Java method `marker` (`Class#method`, or `Class#method(T1,T2)`)
    /// takes `interface`, as the rule's docs define it.
    fn takes(&self, marker: &str, interface: &java::JavaClass) -> bool {
        let (Some(owner), Some(member)) = (self.index.resolve(marker), JavaIndex::marker_member(marker)) else {
            return false;
        };
        owner.package == interface.package
            && owner
                .overloads
                .iter()
                .filter(|o| {
                    if member.contains('(') {
                        o.signature() == member
                    } else {
                        o.name == member
                    }
                })
                .any(|o| o.params.iter().any(|p| p == interface.name()))
    }

    /// The interface declaring `class`'s functional method: `class` itself,
    /// or the first supertype in the index that declares one (`Gauge`
    /// inherits `MetricValueProvider.value`).
    fn functional_owner<'a>(&'a self, class: &'a java::JavaClass) -> Option<&'a java::JavaClass> {
        if class.overloads.iter().any(|o| !o.is_static && !o.is_default) {
            return Some(class);
        }
        class.supertypes.iter().find_map(|s| {
            let supertype = self.index.classes.iter().find(|c| {
                let nested = c.path.join(".");
                !c.is_test && ((c.package == class.package && nested == *s) || format!("{}.{nested}", c.package) == *s)
            })?;
            self.functional_owner(supertype)
        })
    }

    /// The factories the module of the trait translating `interface` must
    /// define, each with the Java method it stands for: `new` for the abstract
    /// method, and `with_<params>` for each `default` overload of it, named by
    /// the parameters it adds (CLAUDE.md §2), or by their types when it adds
    /// none. A nested interface's carry its name as a prefix
    /// (`BaseFunction_new`), as they live in the outer class's module.
    fn expected_factories(&self, interface: &java::JavaClass) -> Vec<(String, String)> {
        let prefix = if interface.path.len() > 1 {
            format!("{}_", interface.name())
        } else {
            String::new()
        };
        let shown = |class: &java::JavaClass, member: String| {
            format!("{}#{member}", class.marker().trim_start_matches(java::MARKER_PREFIX))
        };
        let owner = self.functional_owner(interface);
        let functional = owner.and_then(|c| c.overloads.iter().find(|o| !o.is_static && !o.is_default));
        let (Some(owner), Some(functional)) = (owner, functional) else {
            let java = interface.marker().trim_start_matches(java::MARKER_PREFIX).to_string();
            return vec![(format!("{prefix}new"), java)];
        };
        let mut out = vec![(format!("{prefix}new"), shown(owner, functional.signature()))];
        for overload in owner
            .overloads
            .iter()
            .filter(|o| !o.is_static && o.is_default && o.name == functional.name)
        {
            let mut added: Vec<String> = overload
                .param_names
                .iter()
                .filter(|p| !functional.param_names.contains(p))
                .map(|p| java::snake_case(p))
                .collect();
            if added.is_empty() {
                added = overload
                    .params
                    .iter()
                    .filter(|t| !functional.params.contains(t))
                    .map(|t| java::snake_case(t))
                    .collect();
            }
            let name = format!("{prefix}with_{}", added.join("_"));
            if !out.iter().any(|(n, _)| *n == name) {
                out.push((name, shown(owner, overload.signature())));
            }
        }
        out
    }
}

/// The public fns of `module` returning `impl <trait_name>[<..>]`.
fn trait_factories(module: &Module, trait_name: &str) -> BTreeSet<String> {
    module
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Fn(f) if matches!(f.vis, syn::Visibility::Public(_)) => {
                let syn::ReturnType::Type(_, ty) = &f.sig.output else {
                    return None;
                };
                let syn::Type::ImplTrait(returned) = &**ty else {
                    return None;
                };
                returned
                    .bounds
                    .iter()
                    .any(|b| {
                        matches!(b, syn::TypeParamBound::Trait(t)
                            if t.path.segments.last().is_some_and(|s| s.ident == trait_name))
                    })
                    .then(|| f.sig.ident.to_string())
            },
            _ => None,
        })
        .collect()
}

impl Rule for FunctionalInterfaceTrait {
    fn name(&self) -> &'static str {
        "check-functional-interface-trait"
    }

    fn check(&self, krate: &Crate, findings: &mut Vec<String>) -> usize {
        let items = public_items(krate);
        // Every module-level item outside the C FFI module, with its file and kind.
        let defs: Vec<(&Module, &'static str, String, Vec<syn::Attribute>)> = krate
            .modules
            .iter()
            .filter(|(path, _)| path.first().is_none_or(|m| m != "ffi"))
            .flat_map(|(_, module)| {
                module.items.iter().filter_map(move |item| {
                    let (name, attrs) = item_def(item)?;
                    Some((module, item_kind(item), name, attrs))
                })
            })
            .collect();
        let mut checked = 0usize;
        for interface in self.index.classes.iter().filter(|c| self.is_public_functional(c)) {
            checked += 1;
            let marker = interface.marker();
            let shown = marker.trim_start_matches(java::MARKER_PREFIX);
            let translations: Vec<_> = defs
                .iter()
                .filter(|(_, _, _, attrs)| {
                    java_markers(attrs)
                        .iter()
                        .filter(|m| !m.contains('#'))
                        .any(|m| self.index.resolve(m).is_some_and(|c| c.marker() == marker))
                })
                .collect();
            for (module, kind, name, _) in &translations {
                if *kind != "a trait" {
                    findings.push(format!(
                        "{}: `{name}` translates `{shown}`, a public `@FunctionalInterface`, as {kind}; it must be \
                         a trait",
                        module.file.display(),
                    ));
                    continue;
                }
                let present = trait_factories(module, name);
                for (factory, java) in self.expected_factories(interface) {
                    if !present.contains(&factory) {
                        findings.push(format!(
                            "{}: `{name}` translates `{shown}`, a public `@FunctionalInterface`, but its module has \
                             no `pub fn {factory}(function) -> impl {name}` for `{java}`",
                            module.file.display(),
                        ));
                    }
                }
            }
            if !translations.is_empty() {
                continue;
            }
            let taker = items.iter().find_map(|(file, item)| {
                let method = java_markers(&item.attrs).into_iter().find(|m| self.takes(m, interface))?;
                item.is_fn.then_some((file, item, method))
            });
            if let Some((file, item, method)) = taker {
                let name = match &item.owner {
                    Some(owner) => format!("{owner}::{}", item.name),
                    None => item.name.clone(),
                };
                findings.push(format!(
                    "{}: `{name}` translates `{}`, which takes `{shown}`, a public `@FunctionalInterface` that no \
                     trait translates",
                    file.display(),
                    method.trim_start_matches(java::MARKER_PREFIX)
                ));
            }
        }
        checked
    }

    fn hint(&self) -> &'static str {
        "   Translate the interface as a public trait marked with its class, its method a trait method, e.g.
       #[doc(alias = \"org.apache.kafka.common.KafkaFuture$BaseFunction\")]
       pub trait BaseFunction<A, B>: Send + Sync { fn apply(&self, a: A) -> B; }
   and take it where Java does (`impl BaseFunction<A, B>`, or `Box<dyn ..>` when stored); a method
   Java adds later becomes a default method. Its module defines a factory per function shape,
   returning a private adapter that calls the function:
       pub fn new<F: FnOnce(..) + Send + Sync>(function: F) -> impl Callback { .. }
   `with_<params>` for each `default` overload of the method, and `<Inner>_new` in the outer
   class's module for a nested interface (`kafka_future::BaseFunction_new`)."
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
        self.unsupported.is_empty().then(|| {
            format!(
                "no unsupported-API disclaimers at Kafka `{}`: the ref is not in the `kafka` submodule \
                 (run `cargo xtask fetch-java-refs`)",
                java::AUDIENCE_REF
            )
        })
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
// Rule: check-no-fixed-size-array
// ---------------------------------------------------------------------------

/// Checks that no public interface accepts or returns a fixed-size array
/// `[T; N]` (CLAUDE.md §3, forward compatibility).
///
/// An array's length is part of its type, so a Java `byte[]` or `T[]`
/// translated as `[T; N]` fixes a length Java leaves open: as a parameter it
/// rejects every other length at compile time, and as a return type a new value
/// (one more enum constant in `values()`) becomes a breaking change. A slice
/// (`&[T]`) or a `Vec<T>` carries the length at run time instead.
///
/// Scope: the public items of [`NoDeprecatedTranslation`] — each parameter and
/// the return type of a public function or method, and the type a public `type`
/// alias names — plus the trait arguments and associated types of a trait impl
/// for a public type (`impl From<[u8; 16]> for Uuid`). An array anywhere in the
/// type counts: `&[u8; 16]`, `Option<[u8; 16]>`,
/// `impl Iterator<Item = [u8; 4]>`. The C FFI (`src/ffi`) and `#[cfg(test)]`
/// functions are excluded.
struct NoFixedSizeArray;

impl Rule for NoFixedSizeArray {
    fn name(&self) -> &'static str {
        "check-no-fixed-size-array"
    }

    fn check(&self, krate: &Crate, findings: &mut Vec<String>) -> usize {
        let is_ffi = |module: &[String]| module.first().is_some_and(|m| m == "ffi");
        let mut checked = 0usize;
        for (file, item) in public_items(krate) {
            if item.types.is_empty() || is_ffi(&item.module) {
                continue;
            }
            checked += 1;
            let name = item
                .owner
                .map_or_else(|| item.name.clone(), |owner| format!("{owner}::{}", item.name));
            for (role, ty) in &item.types {
                let mut arrays = Arrays::default();
                syn::visit::Visit::visit_type(&mut arrays, ty);
                for array in arrays.0 {
                    findings.push(format!(
                        "{}: {role} of `{name}` holds the fixed-size array `{array}`",
                        file.display()
                    ));
                }
            }
        }
        let types = krate.public_types();
        for (path, module) in krate.modules.iter().filter(|(path, _)| !is_ffi(path)) {
            for item in &module.items {
                let syn::Item::Impl(i) = item else { continue };
                let Some((_, trait_path, _)) = &i.trait_ else { continue };
                let syn::Type::Path(self_ty) = &*i.self_ty else {
                    continue;
                };
                let Some(self_ty) = self_ty.path.segments.last() else {
                    continue;
                };
                if !types.contains(&(path.clone(), self_ty.ident.to_string())) {
                    continue;
                }
                checked += 1;
                let header = format!("`impl {} for {}`", compact(trait_path), self_ty.ident);
                let mut arrays = Arrays::default();
                syn::visit::Visit::visit_path(&mut arrays, trait_path);
                let mut found: Vec<_> = arrays.0.into_iter().map(|a| ("the trait", a)).collect();
                for it in &i.items {
                    if let syn::ImplItem::Type(t) = it {
                        let mut arrays = Arrays::default();
                        syn::visit::Visit::visit_type(&mut arrays, &t.ty);
                        found.extend(arrays.0.into_iter().map(|a| ("an associated type", a)));
                    }
                }
                for (role, array) in found {
                    findings.push(format!(
                        "{}: {role} of {header} holds the fixed-size array `{array}`",
                        module.file.display()
                    ));
                }
            }
        }
        checked
    }

    fn hint(&self) -> &'static str {
        "   Accept a slice (`&[T]`), checking its length where Java does and returning
   `IllegalArgumentError`; return a `&'static [T]`, a `Vec<T>` or an iterator;
   or make the item `pub(crate)` if Java does not expose it."
    }
}

/// The outermost fixed-size arrays a visited type holds, as written.
#[derive(Default)]
struct Arrays(Vec<String>);

impl<'ast> syn::visit::Visit<'ast> for Arrays {
    fn visit_type_array(&mut self, array: &'ast syn::TypeArray) {
        // An array of arrays is one finding: its elements are not visited.
        self.0.push(compact(array));
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
/// Mentioning a Java class in a doc comment claims nothing, so non-public
/// helpers that cite the class they serve are not held to its name.
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
/// The C FFI (`src/ffi`) is out of scope: its symbols derive from the Rust
/// items they bind, which `check-ffi-translation` checks name by name.
///
/// Every public method carries a method marker: a `pub fn` of an inherent
/// `impl` of a public type, a public trait's method, or a `pub fn` of a
/// reachable module (the scope of [`public_items`]; trait-impl methods,
/// `#[cfg(test)]` functions and the C FFI are out of it). A public method
/// translating no Java method is a finding — make it `pub(crate)` — unless it
/// is tagged [`RUST_ONLY`], with a comment giving the reason a rule needs it
/// public (CLAUDE.md §12.4's `Error::is_*_error`).
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
///     Java package — grouping folders such as `admin/options/`. A method of an
///     impl or trait is placed by its owner type, so it may name the supertype
///     that declares it in another package, e.g. `Partitioner::configure` marked
///     `common.Configurable#configure`.
///   - **mapping**: no two Java packages map to one module, and no two classes
///     of a package map to one Rust name.
///   - **re-exports**: a module's `pub use` lifts only from its own class files
///     and grouping folders — never from another package (`crate::`, `super::`,
///     a sub-package), whose class could collide with one of this package's —
///     unless the lifted item is tagged public on purpose ([`RUST_ONLY`],
///     [`PUBLIC_IN_RUST`]): `common::header` lifts
///     `internals::{RecordHeader, RecordHeaders}`.
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
    fn new(ctx: &Context) -> Self {
        let index = (*ctx.java).clone();
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
    /// Whether it is a method of an impl or trait, placed by its owner type.
    member: bool,
    /// Whether it is a function taking `self`.
    has_self: bool,
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
        out.push(Marked {
            kind,
            name: ident.to_string(),
            attrs: attrs.to_vec(),
            member: false,
            has_self: false,
        })
    };
    let push_member = |sig: &syn::Signature, attrs: &[syn::Attribute], out: &mut Vec<Marked>| {
        out.push(Marked {
            kind: ItemKind::Fn,
            name: sig.ident.to_string(),
            attrs: attrs.to_vec(),
            member: true,
            has_self: sig.receiver().is_some(),
        })
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
                        push_member(&f.sig, &f.attrs, out);
                    }
                }
            },
            syn::Item::Fn(f) => push(ItemKind::Fn, &f.sig.ident, &f.attrs, out),
            syn::Item::Impl(i) if i.trait_.is_none() => {
                for it in &i.items {
                    if let syn::ImplItem::Fn(f) = it {
                        push_member(&f.sig, &f.attrs, out);
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
                    out.push(Marked {
                        kind: ItemKind::Type,
                        name: def.name,
                        attrs: def.attrs,
                        member: false,
                        has_self: false,
                    });
                }
            },
            _ => {},
        }
    }
}

/// A type defined by an error macro: its name, the attributes written inside
/// the macro, and the predicates its `extends:` list names.
struct TypeDef {
    name: String,
    attrs: Vec<syn::Attribute>,
    /// The `ErrorHierarchy` predicates the type answers `true` to; empty for a
    /// macro with no `extends:` list.
    extends: Vec<String>,
}

/// The type a `kafka_error_type!` / `message_only_error!` invocation defines:
/// the leading `#[..]* Name` of its body.
fn error_macro_type_def(m: &syn::ItemMacro) -> Option<TypeDef> {
    let is_error_macro = m
        .mac
        .path
        .segments
        .last()
        .is_some_and(|s| s.ident == "kafka_error_type" || s.ident == "message_only_error");
    if !is_error_macro {
        return None;
    }
    m.mac
        .parse_body_with(|input: syn::parse::ParseStream| {
            let attrs = input.call(syn::Attribute::parse_outer)?;
            let ident: syn::Ident = input.parse()?;
            // Of the rest of the body (`code: ..`, `extends: [..]`), only the
            // `extends` list is needed.
            let extends = input.step(|cursor| {
                let mut extends = Vec::new();
                let mut rest = *cursor;
                while !rest.eof() {
                    let list = rest
                        .ident()
                        .filter(|(i, _)| i == "extends")
                        .and_then(|(_, after)| after.punct())
                        .filter(|(p, _)| p.as_char() == ':')
                        .and_then(|(_, after)| after.any_group());
                    if let Some((mut inside, _, _, after)) = list {
                        while let Some((_, next)) = inside.token_tree() {
                            if let Some((i, _)) = inside.ident() {
                                extends.push(i.to_string());
                            }
                            inside = next;
                        }
                        rest = after;
                        continue;
                    }
                    let Some((_, next)) = rest.token_tree() else { break };
                    rest = next;
                }
                Ok((extends, rest))
            })?;
            Ok(TypeDef { name: ident.to_string(), attrs, extends })
        })
        .ok()
}

/// The values of `attrs`' `#[doc(alias = "..")]`s.
fn doc_aliases(attrs: &[syn::Attribute]) -> Vec<String> {
    let mut out = Vec::new();
    for attr in attrs.iter().filter(|a| a.path().is_ident("doc")) {
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("alias") {
                let lit: syn::LitStr = meta.value()?.parse()?;
                out.push(lit.value());
            } else if meta.input.peek(syn::Token![=]) {
                // Skip the value of any other `doc(key = value)`.
                let _: syn::Expr = meta.value()?.parse()?;
            }
            Ok(())
        });
    }
    out
}

/// The markers among `attrs`' `#[doc(alias = "..")]`s.
fn java_markers(attrs: &[syn::Attribute]) -> Vec<String> {
    doc_aliases(attrs)
        .into_iter()
        .filter(|alias| alias.starts_with(java::MARKER_PREFIX))
        .collect()
}

/// The tag of a public item that translates no Java API — a Rust-only helper, a
/// JDK type, a C-only handle — yet is public on purpose:
/// `#[doc(alias = "rust-only")]`, with the reason in a comment above it. Read by
/// [`JavaName`] and [`PublicAudience`].
const RUST_ONLY: &str = "rust-only";

/// The tag of a public item translating Java API that the audience rules keep
/// non-public — a test-jar class, a class of an `internals` package, a
/// non-`public` method — yet is public on purpose:
/// `#[doc(alias = "public-in-rust")]`, with the reason in a comment above it.
const PUBLIC_IN_RUST: &str = "public-in-rust";

/// Whether `attrs` carry tag `tag` ([`RUST_ONLY`] or [`PUBLIC_IN_RUST`]).
fn has_tag(attrs: &[syn::Attribute], tag: &str) -> bool {
    doc_aliases(attrs).iter().any(|alias| alias == tag)
}

/// An item that may carry a tag: its file, the type or trait it is a member of,
/// and its name.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TagSite {
    file: PathBuf,
    owner: Option<String>,
    name: String,
    tag: &'static str,
}

impl TagSite {
    fn new(file: &Path, owner: Option<&str>, name: &str, tag: &'static str) -> Self {
        TagSite {
            file: file.to_path_buf(),
            owner: owner.map(str::to_string),
            name: name.to_string(),
            tag,
        }
    }
}

/// Every tag of `krate`, on an item of any visibility: a module-level item, an
/// error macro's type, or a member of a trait or an inherent `impl`.
fn tag_sites(krate: &Crate) -> BTreeSet<TagSite> {
    let mut out = BTreeSet::new();
    for module in krate.modules.values() {
        let file = module.file.as_path();
        let mut add = |owner: Option<&syn::Ident>, name: &str, attrs: &[syn::Attribute]| {
            let owner = owner.map(ToString::to_string);
            for tag in [RUST_ONLY, PUBLIC_IN_RUST].into_iter().filter(|t| has_tag(attrs, t)) {
                out.insert(TagSite::new(file, owner.as_deref(), name, tag));
            }
        };
        for item in &module.items {
            match item {
                syn::Item::Trait(t) => {
                    add(None, &t.ident.to_string(), &t.attrs);
                    for it in &t.items {
                        match it {
                            syn::TraitItem::Fn(f) => add(Some(&t.ident), &f.sig.ident.to_string(), &f.attrs),
                            syn::TraitItem::Const(c) => add(Some(&t.ident), &c.ident.to_string(), &c.attrs),
                            _ => {},
                        }
                    }
                },
                syn::Item::Impl(i) if i.trait_.is_none() => {
                    let syn::Type::Path(ty) = &*i.self_ty else { continue };
                    let Some(ty) = ty.path.segments.last() else { continue };
                    for it in &i.items {
                        match it {
                            syn::ImplItem::Fn(f) => add(Some(&ty.ident), &f.sig.ident.to_string(), &f.attrs),
                            syn::ImplItem::Const(c) => add(Some(&ty.ident), &c.ident.to_string(), &c.attrs),
                            _ => {},
                        }
                    }
                },
                _ => {
                    if let Some((name, attrs)) = item_def(item) {
                        add(None, &name, &attrs);
                    }
                },
            }
        }
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

/// A public function carrying no Java marker.
struct UnmarkedMethod {
    file: PathBuf,
    /// The type or trait it is a member of; `None` for a free function.
    owner: Option<String>,
    name: String,
    /// Whether it is tagged [`RUST_ONLY`].
    rust_only: bool,
    /// Whether its owner is tagged [`RUST_ONLY`]: a type translating no Java
    /// class has no Java method for its members to name, so they inherit it.
    owner_rust_only: bool,
}

impl UnmarkedMethod {
    /// `Owner::method`, or the bare name of a free function.
    fn display(&self) -> String {
        self.owner
            .as_ref()
            .map_or_else(|| self.name.clone(), |owner| format!("{owner}::{}", self.name))
    }

    /// Where its [`RUST_ONLY`] tag sits.
    fn tag_site(&self) -> TagSite {
        TagSite::new(&self.file, self.owner.as_deref(), &self.name, RUST_ONLY)
    }
}

/// The public functions of `krate` outside the C FFI that carry no Java
/// marker, and how many public functions were inspected. The scope is
/// [`public_items`]'s: a `pub fn` of an inherent `impl` of a public type, a
/// public trait's methods, a `pub fn` of a reachable module.
fn unmarked_public_methods(krate: &Crate) -> (Vec<UnmarkedMethod>, usize) {
    let mut out = Vec::new();
    let mut checked = 0;
    for (file, item) in public_items(krate) {
        if !item.is_fn || item.module.first().is_some_and(|m| m == "ffi") {
            continue;
        }
        checked += 1;
        if !java_markers(&item.attrs).is_empty() {
            continue;
        }
        let owner_rust_only = item.owner.as_ref().is_some_and(|owner| {
            krate
                .definition_attrs(&item.module, std::slice::from_ref(owner))
                .is_some_and(|attrs| has_tag(&attrs, RUST_ONLY))
        });
        out.push(UnmarkedMethod {
            file: file.to_path_buf(),
            rust_only: has_tag(&item.attrs, RUST_ONLY),
            owner_rust_only,
            owner: item.owner,
            name: item.name,
        });
    }
    (out, checked)
}

/// The Rust module `src/` file `path` defines: `src/a/b.rs` and `src/a/b/mod.rs`
/// are `a::b`, `src/lib.rs` is the root. `None` outside `src/`.
fn rust_module(path: &Path) -> Option<ModPath> {
    let rel = path.strip_prefix("src").ok()?;
    let mut module: ModPath = rel.iter().map(|s| s.to_string_lossy().into_owned()).collect();
    let stem = module.pop()?.trim_end_matches(".rs").to_string();
    if !matches!(stem.as_str(), "mod" | "lib") {
        module.push(stem);
    }
    Some(module)
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
    /// module; `None` outside `src/`). A method of an impl or trait is exempt from
    /// the module check: its owner type's marker places it.
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
        if let Some(module) = module.filter(|m| **m != class.module && !item.member) {
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
                    if !class.fields.contains_key(method) {
                        findings.push(format!(
                            "{file}: `{}` is marked {origin}, but `{}` declares no `{method}`",
                            item.name,
                            class.name()
                        ));
                    } else if !class.is_rust_accessor_of(method, &item.name) {
                        let base = java::rust_method_bases(method).swap_remove(0);
                        findings.push(format!(
                            "{file}: `{}` translates field {origin} and must be named `{base}` (getter) or \
                             `set_{base}` (setter)",
                            item.name
                        ));
                    }
                    return;
                }
                if !class.is_rust_name_of(method, &item.name, item.has_self) {
                    let statics = class.static_rust_names(method);
                    let shared =
                        !statics.is_empty() && class.overloads.iter().any(|o| o.name == *method && !o.is_static);
                    let expected = if shared && !item.has_self {
                        statics.iter().map(|b| format!("`{b}`")).collect::<Vec<_>>().join(" or ")
                    } else if method == class.name() {
                        "`new` or `with_<params>`".to_string()
                    } else {
                        class
                            .rust_bases(method)
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
    /// may an item tagged public on purpose ([`RUST_ONLY`], [`PUBLIC_IN_RUST`]),
    /// found in `krate` from `rust_module`, the Rust module of the file.
    fn check_reexports(
        &self,
        krate: &Crate,
        file: &str,
        (module, rust_module): (&[String], &[String]),
        items: &[syn::Item],
        findings: &mut Vec<String>,
    ) -> usize {
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
                let tagged = || {
                    krate
                        .definition_attrs(rust_module, &path)
                        .is_some_and(|attrs| has_tag(&attrs, RUST_ONLY) || has_tag(&attrs, PUBLIC_IN_RUST))
                };
                let foreign = if macros.contains(name) || tagged() {
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

    /// The unmarked-method check: every public function carries a Java
    /// marker, unless it is tagged [`RUST_ONLY`]. A function carrying only a
    /// class marker is reported by [`JavaName::check_marker`]. A needless tag is
    /// reported by [`PublicAudience`], which sees every tag.
    fn check_unmarked_methods(&self, krate: &Crate, findings: &mut Vec<String>) -> usize {
        let (unmarked, checked) = unmarked_public_methods(krate);
        for method in unmarked.iter().filter(|m| !m.rust_only && !m.owner_rust_only) {
            findings.push(format!(
                "{}: public method `{}` carries no Java method marker; mark the Java method it translates \
                 (#[doc(alias = \"org.apache.kafka.<package>.<Class>#<method>\")]), make it `pub(crate)`, or tag \
                 it #[doc(alias = \"{RUST_ONLY}\")] under a comment giving the reason it is public",
                method.file.display(),
                method.display()
            ));
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

    fn check(&self, krate: &Crate, findings: &mut Vec<String>) -> usize {
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
            if let (Some(module), Some(rust_module)) = (&module, rust_module(path)) {
                checked += self.check_reexports(krate, &file, (module, &rust_module), &parsed.items, findings);
            }
        }
        checked + self.check_mapping(findings) + self.check_unmarked_methods(krate, findings)
    }

    fn hint(&self) -> &'static str {
        "   Name the item as Java does, adapted only by the translation rules
   (CLAUDE.md §2: Exception -> Error with the package prefix outside `common`,
   camelCase -> snake_case, `_with_<params>` for overloads, `new`/`with_..` for
   constructors, a nested class under its bare name). Fix the marker instead if
   it names the wrong Java class or method; a Rust-only helper carries none.
   A public method must carry the marker of the Java method it translates. One
   that translates none is made `pub(crate)`, or, if a rule needs it public
   (CLAUDE.md §12.4's `Error::is_*_error`), tagged #[doc(alias = \"rust-only\")]
   under a `//` comment giving the reason."
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
    fn new(ctx: &Context) -> Self {
        let mut index = (*ctx.java).clone();
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

    /// Compares the checked-in list with the refs. A ref that cannot be read
    /// is a finding too: otherwise a missing or mistyped ref would leave the
    /// list unchecked while the rule passes.
    fn stale_list(list: &str) -> Option<String> {
        let mut expected = BTreeSet::new();
        for reference in java::DEPRECATION_REFS {
            let Some(classes) = java::load_ref(reference) else {
                return Some(format!(
                    "cannot check {} against Kafka `{reference}`: the ref is not in the `kafka` submodule \
                     (run `cargo xtask fetch-java-refs`)",
                    java::DEPRECATED_LIST
                ));
            };
            expected.extend(java::deprecated_items(&classes));
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
    /// The module defining the item.
    module: ModPath,
    /// The type or trait a method or associated const belongs to; `None` for
    /// a module-level item.
    owner: Option<String>,
    /// Whether the item is a function: a method or a free function.
    is_fn: bool,
    /// Whether the item is an `async fn`. A method of an `#[async_trait]`
    /// trait is not: the attribute boxes its future as `Send`.
    is_async: bool,
    /// The value of a `const` initialized by a string literal.
    str_value: Option<String>,
    /// The types a caller passes or receives through the item, each with its
    /// role: a function's parameters and return type, the type a `type` alias
    /// names.
    types: Vec<(String, syn::Type)>,
}

/// What [`public_items`] records about an item beyond its name and attributes.
#[derive(Default)]
struct Shape {
    is_fn: bool,
    is_async: bool,
    str_value: Option<String>,
    types: Vec<(String, syn::Type)>,
}

impl Shape {
    /// A function with signature `sig`; `boxed` for a method of an
    /// `#[async_trait]` trait, whose `async` is not native.
    fn of_fn(sig: &syn::Signature, boxed: bool) -> Self {
        Shape {
            is_fn: true,
            is_async: sig.asyncness.is_some() && !boxed,
            str_value: None,
            types: sig_types(sig),
        }
    }

    /// A `const` initialized by `expr`.
    fn of_const(expr: Option<&syn::Expr>) -> Self {
        let str_value = match expr {
            Some(syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(s), .. })) => Some(s.value()),
            _ => None,
        };
        Shape { str_value, ..Shape::default() }
    }
}

/// Whether `attrs` carry `#[async_trait]`, under any path.
fn is_async_trait(attrs: &[syn::Attribute]) -> bool {
    attrs
        .iter()
        .any(|a| a.path().segments.last().is_some_and(|s| s.ident == "async_trait"))
}

/// The parameter types and the return type of `sig`, each with its role.
fn sig_types(sig: &syn::Signature) -> Vec<(String, syn::Type)> {
    let mut out: Vec<_> = sig
        .inputs
        .iter()
        .filter_map(|arg| match arg {
            syn::FnArg::Typed(pt) => Some((format!("parameter `{}`", compact(&pt.pat)), (*pt.ty).clone())),
            syn::FnArg::Receiver(_) => None,
        })
        .collect();
    if let syn::ReturnType::Type(_, ty) = &sig.output {
        out.push(("the return type".to_string(), (**ty).clone()));
    }
    out
}

/// `t`'s tokens, spaced as rustfmt would for a type: `From<[u8; 16]>`.
fn compact(t: &impl ToTokens) -> String {
    t.to_token_stream()
        .to_string()
        .replace(" ;", ";")
        .replace("& ", "&")
        .replace(" < ", "<")
        .replace("< ", "<")
        .replace(" >", ">")
        .replace(" ,", ",")
        .replace(" :: ", "::")
}

/// The public items of `krate` (see [`NoDeprecatedTranslation`] for the scope).
/// A `#[cfg(test)]` function is skipped: it never ships.
fn public_items(krate: &Crate) -> Vec<(&Path, PublicItem)> {
    let types = krate.public_types();
    let is_pub = |vis: &syn::Visibility| matches!(vis, syn::Visibility::Public(_));
    let mut out = Vec::new();
    for (path, module) in &krate.modules {
        let file = module.file.as_path();
        let reachable = krate.is_reachable(path);
        let is_public_type = |name: &str| types.contains(&(path.clone(), name.to_string()));
        for item in &module.items {
            let mut push = |name: &syn::Ident, attrs: &[syn::Attribute], owner: Option<&syn::Ident>, shape: Shape| {
                if shape.is_fn && is_cfg_test(attrs) {
                    return;
                }
                out.push((
                    file,
                    PublicItem {
                        name: name.to_string(),
                        attrs: attrs.to_vec(),
                        module: path.clone(),
                        owner: owner.map(ToString::to_string),
                        is_fn: shape.is_fn,
                        is_async: shape.is_async,
                        str_value: shape.str_value,
                        types: shape.types,
                    },
                ))
            };
            match item {
                syn::Item::Struct(s) if is_public_type(&s.ident.to_string()) => {
                    push(&s.ident, &s.attrs, None, Shape::default())
                },
                syn::Item::Enum(e) if is_public_type(&e.ident.to_string()) => {
                    push(&e.ident, &e.attrs, None, Shape::default())
                },
                syn::Item::Trait(t) if is_public_type(&t.ident.to_string()) => {
                    push(&t.ident, &t.attrs, None, Shape::default());
                    let boxed = is_async_trait(&t.attrs);
                    for it in &t.items {
                        match it {
                            syn::TraitItem::Fn(f) => {
                                push(&f.sig.ident, &f.attrs, Some(&t.ident), Shape::of_fn(&f.sig, boxed))
                            },
                            syn::TraitItem::Const(c) => push(
                                &c.ident,
                                &c.attrs,
                                Some(&t.ident),
                                Shape::of_const(c.default.as_ref().map(|(_, e)| e)),
                            ),
                            _ => {},
                        }
                    }
                },
                syn::Item::Type(t) if reachable && is_pub(&t.vis) => push(
                    &t.ident,
                    &t.attrs,
                    None,
                    Shape {
                        types: vec![("the aliased type".to_string(), (*t.ty).clone())],
                        ..Shape::default()
                    },
                ),
                syn::Item::Fn(f) if reachable && is_pub(&f.vis) => {
                    push(&f.sig.ident, &f.attrs, None, Shape::of_fn(&f.sig, false))
                },
                syn::Item::Const(c) if reachable && is_pub(&c.vis) => {
                    push(&c.ident, &c.attrs, None, Shape::of_const(Some(&c.expr)))
                },
                syn::Item::Impl(i) if i.trait_.is_none() => {
                    let syn::Type::Path(ty) = &*i.self_ty else { continue };
                    let Some(ty) = ty.path.segments.last() else { continue };
                    if !is_public_type(&ty.ident.to_string()) {
                        continue;
                    }
                    for it in &i.items {
                        match it {
                            syn::ImplItem::Fn(f) if is_pub(&f.vis) => {
                                push(&f.sig.ident, &f.attrs, Some(&ty.ident), Shape::of_fn(&f.sig, false))
                            },
                            syn::ImplItem::Const(c) if is_pub(&c.vis) => {
                                push(&c.ident, &c.attrs, Some(&ty.ident), Shape::of_const(Some(&c.expr)))
                            },
                            _ => {},
                        }
                    }
                },
                syn::Item::Macro(m) => {
                    // The error types are public: every error is a variant of
                    // the public `Error` (CLAUDE.md §12).
                    if let Some(def) = error_macro_type_def(m) {
                        out.push((
                            file,
                            PublicItem {
                                name: def.name,
                                attrs: def.attrs,
                                module: path.clone(),
                                owner: None,
                                is_fn: false,
                                is_async: false,
                                str_value: None,
                                types: Vec::new(),
                            },
                        ));
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

/// Where the C header's `[export] include` list lives.
const CBINDGEN_TOML: &str = "cbindgen.toml";

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
/// A public item without a Java class (a Rust-only helper, a JDK type, a C-only
/// handle) must be tagged [`RUST_ONLY`], and an item translating Java API that is
/// public on purpose despite the rules (a test-jar helper) [`PUBLIC_IN_RUST`],
/// each under a comment giving the reason. A C prefix is let through by a tag on
/// any of its symbols. [`JavaName`] reads [`RUST_ONLY`] on a public method that
/// translates no Java method. A tag that lets nothing through, for either rule,
/// is reported, so tags cannot pile up.
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
///
/// **Members:** a public method another crate can call — a `pub fn` of an
/// inherent `impl` of a public type, or a method of a public trait — must not
/// translate a Java method that is not `public` (CLAUDE.md §3): a `private` or
/// package-private one (a "Visible for testing" getter, a private static
/// helper) or a `protected` one, which Java exposes only to subclasses, a
/// relation Rust has no counterpart for. The method is found by its
/// `Class#method` marker; a marker naming a method only by name matches its
/// widest overload, and an interface member without a modifier is `public`.
/// A method marked with a field, or unmarked, is not checked here.
///
/// **Configurations:** a [`RUST_ONLY`] public method of a type translating a
/// subclass of `AbstractConfig` (`ConsumerConfig`, `ProducerConfig`,
/// `AdminClientConfig`) must take a trait object: the only Rust-only methods
/// such a class may have are the setters CLAUDE.md §2 prescribes for a
/// class-valued key (`ProducerConfig::set_partitioner`). A typed getter or
/// setter of a plain key is reported. A getter freezes the key's Java type,
/// which Java changes in minors (`linger.ms` was a `long`, an `int`, then a
/// `long` again), and a setter skips the `ConfigDef` validators that
/// `new(&props)` runs; so the string map stays the only typed input, and the
/// typed getters the client reads are `pub(crate)`.
///
/// **Constants:** a public associated const of a type translating a Java class
/// translates the field of the same name, which Java must declare `public` on
/// that class or a supertype (Java inherits static fields:
/// `ConsumerConfig.CONFIG_PROVIDERS_CONFIG` is `AbstractConfig`'s). A const
/// naming no Java field is a Rust addition and is tagged [`RUST_ONLY`]; one
/// re-declaring another class's field (`ProducerConfig::SASL_JAAS_CONFIG` for
/// `SaslConfigs.SASL_JAAS_CONFIG`) is removed, since CLAUDE.md §2 exports a
/// constant only from the struct defining it. And no public const holds an
/// `internal.*` key: Java defines those with `defineInternal` and removes them
/// in minor releases (`internal.leave.group.on.close`, 4.2).
struct PublicAudience {
    index: JavaIndex,
    /// [`PUBLIC_AUDIENCE_LIST`].
    public: BTreeSet<String>,
    /// The dotted packages (below `org.apache.kafka`) carrying the
    /// unsupported-API disclaimer.
    unsupported: BTreeSet<String>,
    /// Where the `[export] include` list is read from.
    cbindgen: PathBuf,
}

impl PublicAudience {
    fn new(ctx: &Context) -> Self {
        let index = (*ctx.java).clone();
        let public = parse_public_list(&fs::read_to_string(PUBLIC_AUDIENCE_LIST).unwrap_or_default());
        // Read at the ref, never from the working tree: the tree is the
        // translated source (4.3.1), not the release whose disclaimers count.
        let unsupported = java::unsupported_packages_at_ref(java::AUDIENCE_REF).unwrap_or_default();
        PublicAudience { index, public, unsupported, cbindgen: PathBuf::from(CBINDGEN_TOML) }
    }

    /// Why top-level Java class `fqn` may not be public; see [`audience_failures`].
    fn audience_failures(&self, fqn: &str) -> Vec<String> {
        audience_failures(&self.public, &self.unsupported, fqn)
    }

    /// The Rust half: public items and the generated module. Each tag that lets
    /// an item through is recorded in `used`.
    fn check_rust(&self, krate: &Crate, findings: &mut Vec<String>, used: &mut BTreeSet<TagSite>) -> usize {
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
                // The path §2 imports it by: its file module dropped.
                let canonical = match module.split_last() {
                    Some((file_mod, parent)) if *file_mod == java::snake_case(&name) => {
                        format!("{}::{name}", module_path(parent))
                    },
                    _ => format!("{}::{name}", module_path(&module)),
                };
                // Whether the item carries `tag`, recording it as used if so.
                let mut tagged = |tag| {
                    let hit = has_tag(&attrs, tag);
                    if hit {
                        used.insert(TagSite::new(&m.file, None, &name, tag));
                    }
                    hit
                };
                let classes: BTreeSet<String> = java_markers(&attrs)
                    .iter()
                    .filter_map(|marker| marker.split('#').next())
                    .map(str::to_string)
                    .collect();
                let tops: Vec<String> = classes.iter().map(|c| top_level_class(c).to_string()).collect();
                if classes.is_empty() {
                    if !tagged(RUST_ONLY) {
                        findings.push(format!(
                            "{file}: `{canonical}` is public but carries no Java marker; mark the class it translates, or \
                             tag it #[doc(alias = \"{RUST_ONLY}\")] under a comment giving the reason it is public"
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
                if !failures.is_empty() && !tagged(PUBLIC_IN_RUST) {
                    findings.push(format!(
                        "{file}: `{canonical}` is public but translates {}",
                        failures.join("; ")
                    ));
                }
            }
        }
        checked + self.check_members(krate, findings, used) + check_generated_module(krate, findings)
    }

    /// The member half: no public method translates a Java method that is not
    /// `public`, unless it is tagged [`PUBLIC_IN_RUST`] (recorded in `used`).
    fn check_members(&self, krate: &Crate, findings: &mut Vec<String>, used: &mut BTreeSet<TagSite>) -> usize {
        let mut checked = 0;
        for (file, item) in public_items(krate) {
            for marker in java_markers(&item.attrs) {
                let Some(member) = JavaIndex::marker_member(&marker) else {
                    continue;
                };
                let Some(class) = self.index.resolve(&marker) else {
                    continue;
                };
                let Some(visibility) = class.visibility(member) else {
                    continue;
                };
                checked += 1;
                if has_tag(&item.attrs, PUBLIC_IN_RUST) {
                    used.insert(TagSite::new(file, item.owner.as_deref(), &item.name, PUBLIC_IN_RUST));
                    continue;
                }
                let visibility = match visibility {
                    java::MemberVisibility::Known(visibility) => visibility,
                    java::MemberVisibility::Ambiguous(public) => {
                        findings.push(format!(
                            "{}: `{}` is marked `{}`, only some of whose overloads are public in Java; mark the \
                             overload it translates: {}",
                            file.display(),
                            item.name,
                            marker.trim_start_matches(java::MARKER_PREFIX),
                            public.iter().map(|s| format!("`#{s}`")).collect::<Vec<_>>().join(", ")
                        ));
                        continue;
                    },
                };
                if visibility != java::Visibility::Public {
                    findings.push(format!(
                        "{}: `{}` is public but translates `{}`, which is {} in Java; make it `pub(crate)`",
                        file.display(),
                        item.name,
                        marker.trim_start_matches(java::MARKER_PREFIX),
                        visibility.keyword()
                    ));
                }
            }
        }
        checked
    }

    /// The Java class C symbol prefix `kafka_<pkg>_<Class>` names, as its
    /// (possibly nested) marker and its top-level marker; `None` if it names
    /// none.
    fn resolve_ffi_class(&self, pkg: &[&str], class: &str) -> Option<(String, String)> {
        if pkg.is_empty() {
            // A package-less `kafka_<Type>_…` stands for a JDK type (`kafka_List_t`,
            // CLAUDE.md §4), never a Kafka class; its `rust-only` tag admits it.
            return None;
        }
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
    /// A prefix is let through by a tag on any of its symbols, which are then
    /// recorded in `used`.
    fn check_c(&self, krate: &Crate, findings: &mut Vec<String>, used: &mut BTreeSet<TagSite>) -> usize {
        // symbol -> file it is declared in.
        let mut symbols: BTreeMap<String, String> = BTreeMap::new();
        // prefix -> the tags on its symbols.
        let mut tags: BTreeMap<String, Vec<TagSite>> = BTreeMap::new();
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
                    if let Some((_, attrs)) = item_def(item) {
                        for tag in [RUST_ONLY, PUBLIC_IN_RUST].into_iter().filter(|t| has_tag(&attrs, t)) {
                            let site = TagSite::new(&module.file, None, &name, tag);
                            tags.entry(ffi_prefix(&name)).or_default().push(site);
                        }
                    }
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
            let prefix = ffi_prefix(symbol);
            if let Some(entry) = by_prefix.get_mut(&prefix) {
                entry.1 += 1;
                continue;
            }
            // Whether a symbol of the prefix carries `tag`, recording the tags if so.
            let mut tagged = |tag| {
                let sites: Vec<&TagSite> = tags.get(&prefix).into_iter().flatten().filter(|s| s.tag == tag).collect();
                used.extend(sites.iter().map(|s| (*s).clone()));
                !sites.is_empty()
            };
            let reason = match parsed.as_ref().and_then(|(pkg, class)| self.resolve_ffi_class(pkg, class)) {
                None if tagged(RUST_ONLY) => None,
                None => Some(format!(
                    "names no Java class (`kafka_<pkg>_<Class>_…`, with `<pkg>` the Java package without \
                     `clients`){}",
                    parsed
                        .as_ref()
                        .map_or_else(String::new, |(_, class)| self.misplaced_ffi_class(class))
                )),
                Some((class, top)) => {
                    let failures = self.audience_failures(&top);
                    if failures.is_empty() || tagged(PUBLIC_IN_RUST) {
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

/// Why top-level Java class `fqn` (`org.apache.kafka.<package>.<Class>`) may
/// not be public by CLAUDE.md §2, one reason per rule it fails; empty when it
/// may. `public` is the [`PUBLIC_AUDIENCE_LIST`], `unsupported` the dotted
/// packages carrying the unsupported-API disclaimer.
fn audience_failures(public: &BTreeSet<String>, unsupported: &BTreeSet<String>, fqn: &str) -> Vec<String> {
    let path = fqn.strip_prefix(java::MARKER_PREFIX).unwrap_or(fqn);
    let (package, _) = path.rsplit_once('.').unwrap_or(("", path));
    let mut reasons = Vec::new();
    if package.split('.').any(|seg| seg.contains("internal")) {
        reasons.push(format!("its package `{package}` is internal"));
    }
    if unsupported.contains(package) {
        reasons.push(format!("its package `{package}` is not a supported Kafka API"));
    }
    if !public.contains(fqn) {
        reasons.push(format!("it is not `@InterfaceAudience.Public` in Kafka {}", java::AUDIENCE_REF));
    }
    reasons
}

/// The name a public item defines, and its attributes: a `pub` struct, enum,
/// union, trait, type alias, fn, const or static, or an error macro's type
/// (always `pub`).
fn pub_def(item: &syn::Item) -> Option<(String, Vec<syn::Attribute>)> {
    let vis = match item {
        syn::Item::Struct(s) => &s.vis,
        syn::Item::Enum(e) => &e.vis,
        syn::Item::Union(u) => &u.vis,
        syn::Item::Trait(t) => &t.vis,
        syn::Item::Type(t) => &t.vis,
        syn::Item::Fn(f) => &f.vis,
        syn::Item::Const(c) => &c.vis,
        syn::Item::Static(s) => &s.vis,
        syn::Item::Macro(_) => return item_def(item),
        _ => return None,
    };
    if matches!(vis, syn::Visibility::Public(_)) {
        item_def(item)
    } else {
        None
    }
}

/// [`pub_def`], whatever the item's visibility.
fn item_def(item: &syn::Item) -> Option<(String, Vec<syn::Attribute>)> {
    let (ident, attrs) = match item {
        syn::Item::Struct(s) => (&s.ident, &s.attrs),
        syn::Item::Enum(e) => (&e.ident, &e.attrs),
        syn::Item::Union(u) => (&u.ident, &u.attrs),
        syn::Item::Trait(t) => (&t.ident, &t.attrs),
        syn::Item::Type(t) => (&t.ident, &t.attrs),
        syn::Item::Fn(f) => (&f.sig.ident, &f.attrs),
        syn::Item::Const(c) => (&c.ident, &c.attrs),
        syn::Item::Static(s) => (&s.ident, &s.attrs),
        syn::Item::Macro(m) => return error_macro_type_def(m).map(|def| (def.name, def.attrs)),
        _ => return None,
    };
    Some((ident.to_string(), attrs.clone()))
}

/// What `item` is declared as, with its article: `a struct`, `an enum`, `a
/// trait`, `a type alias`, ... (an error macro's type is `an error type`).
fn item_kind(item: &syn::Item) -> &'static str {
    match item {
        syn::Item::Struct(_) => "a struct",
        syn::Item::Enum(_) => "an enum",
        syn::Item::Union(_) => "a union",
        syn::Item::Trait(_) => "a trait",
        syn::Item::Type(_) => "a type alias",
        syn::Item::Fn(_) => "a fn",
        syn::Item::Const(_) => "a const",
        syn::Item::Static(_) => "a static",
        syn::Item::Macro(_) => "an error type",
        _ => "an item",
    }
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

/// The prefix C `symbol` is reported under: `kafka_<pkg>_<Class>` (`kafka_<Class>`
/// for a package-less helper), or the whole symbol when it names no class.
fn ffi_prefix(symbol: &str) -> String {
    ffi_class(symbol).map_or_else(
        || symbol.to_string(),
        |(pkg, class)| {
            if pkg.is_empty() {
                format!("kafka_{class}")
            } else {
                format!("kafka_{}_{class}", pkg.join("_"))
            }
        },
    )
}

/// A C symbol `kafka_<pkg>_<Class>_…` as (package segments, class); the
/// segments are empty for a package-less `kafka_<Class>_…` helper standing for
/// a JDK type (`kafka_List_t`, CLAUDE.md §4). `None` when no PascalCase class
/// follows the lower-case segments.
fn ffi_class(symbol: &str) -> Option<(Vec<&str>, &str)> {
    let rest = symbol.strip_prefix("kafka_")?;
    let mut pkg = Vec::new();
    for seg in rest.split('_') {
        if seg.starts_with(|c: char| c.is_ascii_uppercase()) {
            return Some((pkg, seg));
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

impl PublicAudience {
    /// The configuration half (see the type's docs): every [`RUST_ONLY`] public
    /// method of a type translating a subclass of [`ABSTRACT_CONFIG`] is a typed
    /// accessor of a configuration key unless it takes a trait object. Returns
    /// how many such methods were inspected.
    fn check_configs(&self, krate: &Crate, findings: &mut Vec<String>) -> usize {
        let mut checked = 0;
        for (file, item) in public_items(krate) {
            if !item.is_fn || item.module.first().is_some_and(|m| m == "ffi") || !has_tag(&item.attrs, RUST_ONLY) {
                continue;
            }
            let Some(owner) = &item.owner else { continue };
            let Some(class) = krate
                .definition_attrs(&item.module, std::slice::from_ref(owner))
                .and_then(|attrs| java_markers(&attrs).iter().find_map(|marker| self.index.resolve(marker)))
            else {
                continue;
            };
            let qualified = format!(".{ABSTRACT_CONFIG}");
            if !class.supertypes.iter().any(|s| s == ABSTRACT_CONFIG || s.ends_with(&qualified)) {
                continue;
            }
            checked += 1;
            if item
                .types
                .iter()
                .any(|(role, ty)| role.starts_with("parameter") && names_trait_object(ty))
            {
                continue;
            }
            findings.push(format!(
                "{}: `{owner}::{}` is a typed accessor of a configuration key on `{}`, a translation of an \
                 `AbstractConfig`: make it `pub(crate)` so the string map stays the only typed input (`new(&props)` \
                 validates it as Java's `ConfigDef` does), or take a trait object for a class-valued key (CLAUDE.md §2)",
                file.display(),
                item.name,
                class.marker()
            ));
        }
        checked
    }
}

impl PublicAudience {
    /// The constant half (see the type's docs). A public associated const
    /// without a member marker translates the field of the same name on its
    /// type's Java class, which must be `public` there; one naming no field of
    /// that class must be tagged [`RUST_ONLY`]. And no public const holds an
    /// [`INTERNAL_KEY_PREFIX`] key, whatever Java's visibility. A tag that lets
    /// a const through is recorded in `used`. Returns how many consts were
    /// inspected.
    fn check_constants(&self, krate: &Crate, findings: &mut Vec<String>, used: &mut BTreeSet<TagSite>) -> usize {
        let mut checked = 0;
        for (file, item) in public_items(krate) {
            if item.is_fn || item.module.first().is_some_and(|m| m == "ffi") {
                continue;
            }
            let name = item
                .owner
                .as_ref()
                .map_or_else(|| item.name.clone(), |o| format!("{o}::{}", item.name));
            let mut tagged = |tag| {
                let hit = has_tag(&item.attrs, tag);
                if hit {
                    used.insert(TagSite::new(file, item.owner.as_deref(), &item.name, tag));
                }
                hit
            };
            if let Some(key) = item.str_value.as_deref().filter(|v| v.starts_with(INTERNAL_KEY_PREFIX)) {
                checked += 1;
                if !tagged(PUBLIC_IN_RUST) {
                    findings.push(format!(
                        "{}: `{name}` is public but holds `{key}`, an internal configuration key Java may remove in a \
                         minor release; make it `pub(crate)`",
                        file.display()
                    ));
                }
                continue;
            }
            // An associated const: a method is skipped above, and only an
            // associated item has an owner.
            let Some(owner) = &item.owner else { continue };
            if java_markers(&item.attrs).iter().any(|m| JavaIndex::marker_member(m).is_some()) {
                // `check_members` resolves a marked member.
                continue;
            }
            let Some(class) = krate
                .definition_attrs(&item.module, std::slice::from_ref(owner))
                .and_then(|attrs| java_markers(&attrs).iter().find_map(|marker| self.index.resolve(marker)))
            else {
                continue;
            };
            checked += 1;
            match self.field(class, &item.name) {
                Some((_, java::Visibility::Public)) => {},
                Some((declaring, visibility)) => {
                    if !tagged(PUBLIC_IN_RUST) {
                        findings.push(format!(
                            "{}: `{name}` is public but translates `{}#{}`, which is {} in Java; make it `pub(crate)`",
                            file.display(),
                            declaring.marker(),
                            item.name,
                            visibility.keyword()
                        ));
                    }
                },
                None => {
                    if !tagged(RUST_ONLY) {
                        findings.push(format!(
                            "{}: `{name}` is public but `{}` declares no field `{}`; give it the name of the public \
                             Java field it translates, make it `pub(crate)`, or tag it #[doc(alias = \"{RUST_ONLY}\")] \
                             under a comment giving the reason it is public",
                            file.display(),
                            class.marker(),
                            item.name
                        ));
                    }
                },
            }
        }
        checked
    }
}

impl PublicAudience {
    /// The field `name` as Java code reaches it through `class`: declared
    /// there or, since Java inherits static fields, on a supertype
    /// (`ConsumerConfig.CONFIG_PROVIDERS_CONFIG` is `AbstractConfig`'s). With
    /// the class declaring it and its visibility there.
    fn field<'a>(&'a self, class: &'a java::JavaClass, name: &str) -> Option<(&'a java::JavaClass, java::Visibility)> {
        self.field_from(class, name, &mut BTreeSet::new())
    }

    fn field_from<'a>(
        &'a self,
        class: &'a java::JavaClass,
        name: &str,
        seen: &mut BTreeSet<String>,
    ) -> Option<(&'a java::JavaClass, java::Visibility)> {
        if !seen.insert(class.marker()) {
            return None;
        }
        if let Some(&visibility) = class.field_visibility.get(name) {
            return Some((class, visibility));
        }
        class
            .supertypes
            .iter()
            .filter_map(|s| self.index.supertype(class, s))
            .find_map(|supertype| self.field_from(supertype, name, seen))
    }
}

/// The prefix of the configuration keys Java defines with `defineInternal`
/// and removes in minor releases (`internal.leave.group.on.close`, 4.2).
const INTERNAL_KEY_PREFIX: &str = "internal.";

/// Java's base class of every client configuration,
/// `org.apache.kafka.common.config.AbstractConfig`, as a `supertypes` entry
/// names it.
const ABSTRACT_CONFIG: &str = "AbstractConfig";

/// Whether `ty` holds a trait object anywhere: `Box<dyn Partitioner<K, V>>`,
/// `Vec<Arc<dyn ConsumerInterceptor<K, V>>>`. `impl Trait` does not count:
/// `impl Into<String>` is a plain string.
fn names_trait_object(ty: &syn::Type) -> bool {
    struct Finder(bool);
    impl syn::visit::Visit<'_> for Finder {
        fn visit_type_trait_object(&mut self, _: &syn::TypeTraitObject) {
            self.0 = true;
        }
    }
    let mut finder = Finder(false);
    syn::visit::Visit::visit_type(&mut finder, ty);
    finder.0
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
        self.unsupported.is_empty().then(|| {
            format!(
                "no unsupported-API disclaimers at Kafka `{}`: the ref is not in the `kafka` submodule \
                 (run `cargo xtask fetch-java-refs`)",
                java::AUDIENCE_REF
            )
        })
    }

    fn check(&self, krate: &Crate, findings: &mut Vec<String>) -> usize {
        let mut used = BTreeSet::new();
        let checked = self.check_rust(krate, findings, &mut used)
            + self.check_c(krate, findings, &mut used)
            + self.check_configs(krate, findings)
            + self.check_constants(krate, findings, &mut used);
        // `check-java-name` lets a `rust-only` public method through.
        used.extend(
            unmarked_public_methods(krate)
                .0
                .iter()
                .filter(|m| m.rust_only && !m.owner_rust_only)
                .map(UnmarkedMethod::tag_site),
        );
        for site in tag_sites(krate).difference(&used) {
            let name = site
                .owner
                .as_ref()
                .map_or_else(|| site.name.clone(), |o| format!("{o}::{}", site.name));
            findings.push(format!(
                "{}: `{name}` is tagged `{}` but would pass without it; remove the tag",
                site.file.display(),
                site.tag
            ));
        }
        checked
    }

    fn hint(&self) -> &'static str {
        "   Make the item `pub(crate)` and drop its C binding: only a class Java
   publishes may be public — outside `internal*` packages, outside packages whose
   package-info.java says \"not a supported Kafka API\", and annotated
   `@InterfaceAudience.Public` in Kafka 4.4. A Rust-only item (a C-only handle
   included) is tagged #[doc(alias = \"rust-only\")], a deliberate exception
   #[doc(alias = \"public-in-rust\")], under a `//` comment giving the reason;
   for C, tag any symbol of the prefix, preferably its `_t` type.
   Likewise a public method may only translate a Java method that is `public`
   (an interface member without a modifier is): make the others `pub(crate)`,
   or tag the method #[doc(alias = \"public-in-rust\")] with the reason.
   A tag on an item that would pass without it is reported: remove it.
   A Rust-only method of a configuration (a subclass of `AbstractConfig`) may
   only set a class-valued key through a trait object: a typed getter or setter
   of a plain key is `pub(crate)`, and users configure through the string map.
   A public constant translates a `public` field of its class's Java class (or
   one it inherits): a constant Java keeps non-public is `pub(crate)`, one that
   re-declares another class's field is removed (CLAUDE.md §2), and a Rust-only
   one is tagged; a constant holding an `internal.*` key is never public.
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
/// call): it must be paired with a `Dyn<Name>` trait (e.g. `DynProducer`) that
/// is dyn-compatible and blanket-implemented for it, so the boxing cost is paid
/// only by callers who need dynamic dispatch. The companion is public when the
/// crate hands the trait object out or callers hold one (`DynProducer`,
/// `DynConsumer`), and crate-private when the crate only takes the object in
/// and boxes it itself (`DynSerializer` and `DynDeserializer`, behind the
/// `impl Serializer<K>` / `impl Deserializer<K>` constructor parameters): a
/// private companion can be published later in a minor release, while a
/// public one cannot be withdrawn without a major one.
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
/// Scope: the same public items as [`NoPublicField`], traits only; the
/// `Dyn<Name>` companions are looked up in every module, public or not.
struct DynCompatible;

impl DynCompatible {
    /// The reasons `t` is not dyn-compatible; empty when it is.
    fn violations(t: &syn::ItemTrait) -> Vec<String> {
        let mut reasons = Vec::new();
        let is_async_trait = is_async_trait(&t.attrs);

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
        // A companion may be crate-private (see above), so every trait of the
        // crate is a candidate, not only the public ones.
        let dyn_compatible: BTreeSet<String> = krate
            .traits()
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
   performance-oriented trait — add a dyn-compatible `Dyn<Name>` trait
   blanket-implemented for it: public when the crate hands the trait object
   out, as `DynProducer` is for `Producer`; `pub(crate)` when the crate only
   takes it in and boxes it itself, as `DynSerializer` is for `Serializer`."
    }
}

// ---------------------------------------------------------------------------
// Rule: check-dyn-forwarding
// ---------------------------------------------------------------------------

/// Checks that a `Dyn<Name>` companion and its trait forward to each other
/// method for method (CLAUDE.md §3, forward compatibility).
///
/// [`DynCompatible`] lets a performance-oriented trait `Name` (`Producer`) off
/// with a dyn-compatible `Dyn<Name>` (`DynProducer`). The pair is three blocks
/// kept in step by hand: the `Dyn<Name>` trait, the blanket
/// `impl<P: Name> Dyn<Name> for P` and `impl Name for dyn Dyn<Name>`. A method
/// Java adds to the interface compiles when added to `Name` alone if it has a
/// default body, and a `dyn Dyn<Name>` then answers with that default instead
/// of the boxed producer's override; a forward to the wrong method, or with
/// its arguments swapped, compiles whenever the types agree. So for every
/// public trait `Name` with a trait `Dyn<Name>`, public or crate-private (see
/// [`DynCompatible`]):
///
///  - the two traits declare the same methods;
///  - a blanket `impl Dyn<Name> for P` (`P` a type parameter) defines each of
///    them as a call to `Name::<method>(self, <its parameters in order>)`;
///  - an `impl Name for dyn Dyn<Name>` defines each of them, defaulted ones
///    included, as a call to `Dyn<Name>::<method>(self, <parameters>)`.
///
/// The call may sit anywhere in the body (`Box::pin(…)`, `….await`).
struct DynForwarding;

/// One side of a `Name` / `Dyn<Name>` pair: what implementing it must forward to.
struct Forward<'a> {
    /// How a finding names the impl: `impl DynProducer for P`.
    label: String,
    /// The trait the bodies call: `Producer` for the blanket impl.
    to: &'a str,
}

impl DynForwarding {
    /// The method names of `t`, in declaration order.
    fn methods(t: &syn::ItemTrait) -> Vec<String> {
        t.items
            .iter()
            .filter_map(|item| match item {
                syn::TraitItem::Fn(f) => Some(f.sig.ident.to_string()),
                _ => None,
            })
            .collect()
    }

    /// The name of the trait `imp` implements, if it implements one.
    fn implemented(imp: &syn::ItemImpl) -> Option<String> {
        imp.trait_
            .as_ref()
            .and_then(|(_, path, _)| path.segments.last())
            .map(|s| s.ident.to_string())
    }

    /// Whether `imp` is for a bare type parameter of its own: `impl<P: X> Y for P`.
    fn is_blanket(imp: &syn::ItemImpl) -> bool {
        let syn::Type::Path(tp) = &*imp.self_ty else {
            return false;
        };
        tp.qself.is_none()
            && tp
                .path
                .get_ident()
                .is_some_and(|ident| imp.generics.type_params().any(|p| p.ident == *ident))
    }

    /// Whether `imp` is for `dyn <dyn_name> + …`.
    fn is_for_dyn(imp: &syn::ItemImpl, dyn_name: &str) -> bool {
        let syn::Type::TraitObject(object) = &*imp.self_ty else {
            return false;
        };
        object.bounds.iter().any(|bound| {
            matches!(bound, syn::TypeParamBound::Trait(tb)
                if tb.path.segments.last().is_some_and(|s| s.ident == dyn_name))
        })
    }

    /// The parameter names of `sig` after the receiver; `None` for a
    /// parameter that is not a plain name.
    fn params(sig: &syn::Signature) -> Vec<Option<String>> {
        sig.inputs
            .iter()
            .filter_map(|arg| match arg {
                syn::FnArg::Receiver(_) => None,
                syn::FnArg::Typed(pt) => Some(match &*pt.pat {
                    syn::Pat::Ident(pi) => Some(pi.ident.to_string()),
                    _ => None,
                }),
            })
            .collect()
    }

    /// Whether `f`'s body calls `<to>::<f's name>(self, <f's parameters>)`.
    fn forwards(f: &syn::ImplItemFn, to: &str) -> bool {
        struct Finder<'a> {
            to: &'a str,
            method: &'a syn::Ident,
            args: Vec<String>,
            found: bool,
        }
        impl syn::visit::Visit<'_> for Finder<'_> {
            fn visit_expr_call(&mut self, call: &syn::ExprCall) {
                if let syn::Expr::Path(func) = &*call.func {
                    let segments: Vec<_> = func.path.segments.iter().collect();
                    let names_method = segments.len() >= 2
                        && segments[segments.len() - 1].ident == *self.method
                        && segments[segments.len() - 2].ident == self.to;
                    let args: Vec<String> = call
                        .args
                        .iter()
                        .map(|arg| match arg {
                            syn::Expr::Path(p) => p.path.get_ident().map_or_else(String::new, ToString::to_string),
                            _ => String::new(),
                        })
                        .collect();
                    if names_method && args == self.args {
                        self.found = true;
                    }
                }
                syn::visit::visit_expr_call(self, call);
            }
        }
        let Some(params) = Self::params(&f.sig).into_iter().collect::<Option<Vec<_>>>() else {
            return false;
        };
        let mut finder = Finder {
            to,
            method: &f.sig.ident,
            args: std::iter::once("self".to_string()).chain(params).collect(),
            found: false,
        };
        syn::visit::Visit::visit_block(&mut finder, &f.block);
        finder.found
    }

    /// Checks that `imp` defines each of `methods` as a forward to `side.to`.
    fn check_impl(file: &Path, imp: &syn::ItemImpl, methods: &[String], side: &Forward, findings: &mut Vec<String>) {
        for method in methods {
            let defined = imp.items.iter().find_map(|item| match item {
                syn::ImplItem::Fn(f) if f.sig.ident == method => Some(f),
                _ => None,
            });
            match defined {
                None => findings.push(format!(
                    "{}: `{}` does not define `{method}`; define it as a forward to `{}::{method}`",
                    file.display(),
                    side.label,
                    side.to
                )),
                Some(f) if !Self::forwards(f, side.to) => {
                    let params: Vec<String> = Self::params(&f.sig)
                        .into_iter()
                        .map(|p| p.unwrap_or_else(|| "_".to_string()))
                        .collect();
                    findings.push(format!(
                        "{}: `{}`'s `{method}` does not call `{}::{method}({})`",
                        file.display(),
                        side.label,
                        side.to,
                        std::iter::once("self".to_string()).chain(params).collect::<Vec<_>>().join(", ")
                    ));
                },
                Some(_) => {},
            }
        }
    }
}

impl Rule for DynForwarding {
    fn name(&self) -> &'static str {
        "check-dyn-forwarding"
    }

    fn check(&self, krate: &Crate, findings: &mut Vec<String>) -> usize {
        let mut traits: BTreeMap<String, (&ModPath, &syn::ItemTrait)> = krate
            .public_types()
            .iter()
            .filter_map(|(path, name)| match krate.type_item(path, name) {
                syn::Item::Trait(t) => Some((t.ident.to_string(), (krate.modules.get_key_value(path)?.0, t))),
                _ => None,
            })
            .collect();
        // A crate-private companion (`DynSerializer`) is checked like a public one.
        for (path, t) in krate.traits().filter(|(_, t)| t.ident.to_string().starts_with("Dyn")) {
            traits.entry(t.ident.to_string()).or_insert((path, t));
        }
        let impls: Vec<(&Path, &syn::ItemImpl)> = krate
            .modules
            .values()
            .flat_map(|m| {
                m.items.iter().filter_map(move |item| match item {
                    syn::Item::Impl(imp) => Some((m.file.as_path(), imp)),
                    _ => None,
                })
            })
            .collect();

        let mut checked = 0;
        for (name, (_, base)) in &traits {
            let dyn_name = format!("Dyn{name}");
            let Some((dyn_path, dyn_trait)) = traits.get(&dyn_name) else {
                continue;
            };
            checked += 1;
            let dyn_file = &krate.modules[*dyn_path].file;
            let (base_methods, dyn_methods) = (Self::methods(base), Self::methods(dyn_trait));
            for method in base_methods.iter().filter(|m| !dyn_methods.contains(m)) {
                findings.push(format!(
                    "{}: `{dyn_name}` lacks `{method}` of `{name}`; declare it and forward it both ways",
                    dyn_file.display()
                ));
            }
            for method in dyn_methods.iter().filter(|m| !base_methods.contains(m)) {
                findings.push(format!(
                    "{}: `{dyn_name}` declares `{method}`, which `{name}` lacks; remove it",
                    dyn_file.display()
                ));
            }
            let shared: Vec<String> = base_methods.iter().filter(|m| dyn_methods.contains(m)).cloned().collect();

            let blanket = impls
                .iter()
                .find(|(_, imp)| Self::implemented(imp).as_deref() == Some(&dyn_name) && Self::is_blanket(imp));
            let side = Forward { label: format!("impl {dyn_name} for P"), to: name };
            match blanket {
                Some((file, imp)) => Self::check_impl(file, imp, &shared, &side, findings),
                None => findings.push(format!(
                    "{}: no blanket `impl<P: {name}> {dyn_name} for P`; every `{name}` must be a `{dyn_name}`",
                    dyn_file.display()
                )),
            }
            let on_dyn = impls.iter().find(|(_, imp)| {
                Self::implemented(imp).as_deref() == Some(name.as_str()) && Self::is_for_dyn(imp, &dyn_name)
            });
            let side = Forward { label: format!("impl {name} for dyn {dyn_name}"), to: &dyn_name };
            match on_dyn {
                Some((file, imp)) => Self::check_impl(file, imp, &shared, &side, findings),
                None => findings.push(format!(
                    "{}: no `impl {name} for dyn {dyn_name}`; a `dyn {dyn_name}` must be a `{name}`",
                    dyn_file.display()
                )),
            }
        }
        checked
    }

    fn hint(&self) -> &'static str {
        "   Keep a `Dyn<Name>` companion in step with `Name` (CLAUDE.md §3): the two
   traits declare the same methods; the blanket `impl<P: Name> Dyn<Name> for P`
   defines each as `Name::<method>(self, <parameters>)` (boxed as needed), and
   `impl Name for dyn Dyn<Name>` defines each, defaulted ones included, as
   `Dyn<Name>::<method>(self, <parameters>)`, so a boxed implementation's
   overrides are the ones a trait object runs."
    }
}

// ---------------------------------------------------------------------------
// Rule: check-consumer-handle
// ---------------------------------------------------------------------------

/// Checks that `ConsumerHandle`'s public methods are exactly the `Consumer`
/// methods a rebalance listener or commit callback may call back into
/// (`consumer-threading.md` §31, KIP-1306).
///
/// `ConsumerHandle` is the reentrant view of a consumer: the listener runs
/// inside the caller's `poll()`, which holds the consumer `&mut`, so the
/// listener reaches the consumer through this `&self` handle instead. KIP-1306
/// (KAFKA-20385, Kafka 4.5) names that view `RebalanceConsumer`: the `Consumer`
/// interface minus the methods that would re-enter or end the running call —
/// `poll`, `close`, every `subscribe`, `unsubscribe`, `assign`, `wakeup`,
/// `enforceRebalance` and the two metric-subscription methods — and minus
/// `subscription()` and `metrics()`, which it does not carry either. The handle
/// is `rust-only` (Java passes a `RebalanceConsumer` into the callback; Rust
/// captures a handle), so `check-java-name` skips its methods and nothing else
/// ties them to the trait.
///
/// One difference from `RebalanceConsumer`: the handle keeps `wakeup`. Java's
/// `Consumer.wakeup()` is the one method another thread may call, on the
/// consumer object itself, so KIP-1306 has no need to put it on the callback
/// view. The Rust consumer's methods take `&mut self`, so no other thread can
/// hold the consumer; the `Send + Sync` handle is the only object through which
/// Java's cross-thread `wakeup()` contract is honoured. So, with the `Consumer`
/// trait as the contract:
///
///  - every `Consumer` method outside that set is a `pub fn` of the inherent
///    `impl ConsumerHandle`, taking `&self` and the trait method's parameters,
///    returning the trait method's type — `impl Future<Output = R> + Send`
///    where the trait method is `async fn … -> R`;
///  - no method of that set, nor an overload of one (`close_with_options`,
///    `subscribe_with_topics`), is a `pub fn` of the handle;
///  - every `pub fn` of the handle is a `Consumer` method: a handle-only name
///    (`commit_async_offsets`) is off the §2 overload scheme the trait follows.
///
/// A `pub(crate)` method is not checked: the C binding reads `metrics()` and the
/// mock's flags through such methods so they answer while another call holds
/// the consumer, as Java's unguarded `metrics()` does.
struct ConsumerHandleSurface;

/// A method signature reduced to what the rule compares: receiver, named
/// parameters and return type, as normalized token strings.
#[derive(PartialEq, Eq)]
struct HandleSig {
    name: String,
    receiver: String,
    params: Vec<(String, String)>,
    ret: String,
}

impl std::fmt::Display for HandleSig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "fn {}({}", self.name, self.receiver)?;
        for (name, ty) in &self.params {
            write!(f, ", {name}: {ty}")?;
        }
        write!(f, ")")?;
        if self.ret != "()" {
            write!(f, " -> {}", self.ret)?;
        }
        Ok(())
    }
}

impl ConsumerHandleSurface {
    /// The module and name of the trait that is the contract.
    const TRAIT_MODULE: &'static [&'static str] = &["consumer"];
    const TRAIT: &'static str = "Consumer";
    /// The handle the rule checks.
    const HANDLE: &'static str = "ConsumerHandle";
    /// The §2 base names of the `Consumer` methods a listener may not call,
    /// KIP-1306's `RebalanceConsumer` omissions in Rust spelling except
    /// `wakeup` (see the type's documentation), plus `handle`, the Rust-only
    /// accessor that yields the handle itself.
    const EXCLUDED: &'static [&'static str] = &[
        "poll",
        "close",
        "subscribe",
        "unsubscribe",
        "assign",
        "enforce_rebalance",
        "subscription",
        "metrics",
        "handle",
    ];

    /// The §2 base name of `method`: `close_with_options` -> `close`.
    fn base(method: &str) -> &str {
        method.split("_with_").next().unwrap_or(method)
    }

    fn is_excluded(method: &str) -> bool {
        Self::EXCLUDED.contains(&Self::base(method))
    }

    /// The finding for an excluded `method` the handle defines.
    fn excluded_finding(file: &Path, method: &str) -> String {
        let base = Self::base(method);
        let overload = if base == method {
            String::new()
        } else {
            format!(", an overload of `{base}`,")
        };
        format!(
            "{}: `{}` defines `{method}`{overload} which KIP-1306's `RebalanceConsumer` keeps from a listener; \
             remove it",
            file.display(),
            Self::HANDLE
        )
    }

    /// `ty`'s tokens with every path reduced to its last segment, so the
    /// handle's `crate::consumer::OffsetCommitCallback` equals the trait's
    /// imported `OffsetCommitCallback`.
    fn normalize(ty: &syn::Type) -> String {
        let mut s = compact(ty);
        while let Some(end) = s.find("::") {
            let start = s[..end]
                .rfind(|c: char| !(c.is_alphanumeric() || c == '_'))
                .map_or(0, |i| i + 1);
            s.replace_range(start..end + 2, "");
        }
        s
    }

    /// The return type as the rule compares it: `()` for none, and for an
    /// `async fn` the `impl Future` its handle counterpart must return. The
    /// built type is re-parsed so both sides are spaced by [`compact`].
    fn ret(sig: &syn::Signature) -> String {
        let inner = match &sig.output {
            syn::ReturnType::Default => "()".to_string(),
            syn::ReturnType::Type(_, ty) => Self::normalize(ty),
        };
        if sig.asyncness.is_none() {
            return inner;
        }
        let future = format!("impl Future<Output = {inner}> + Send");
        syn::parse_str::<syn::Type>(&future).map_or(future, |ty| Self::normalize(&ty))
    }

    /// `sig` reduced for comparison; `receiver` overrides the receiver, which
    /// is how the expected handle signature is derived from a trait method.
    fn reduce(sig: &syn::Signature, receiver: Option<&str>) -> HandleSig {
        let mut actual_receiver = "self".to_string();
        let mut params = Vec::new();
        for arg in &sig.inputs {
            match arg {
                syn::FnArg::Receiver(r) => {
                    actual_receiver = match (&r.reference, &r.mutability) {
                        (Some(_), Some(_)) => "&mut self",
                        (Some(_), None) => "&self",
                        (None, _) => "self",
                    }
                    .to_string();
                },
                syn::FnArg::Typed(pt) => {
                    let name = match &*pt.pat {
                        syn::Pat::Ident(pi) => pi.ident.to_string(),
                        other => compact(other),
                    };
                    params.push((name, Self::normalize(&pt.ty)));
                },
            }
        }
        HandleSig {
            name: sig.ident.to_string(),
            receiver: receiver.map_or(actual_receiver, str::to_string),
            params,
            ret: Self::ret(sig),
        }
    }

    /// The `pub fn`s of every inherent `impl ConsumerHandle`, with their file.
    fn handle_methods(krate: &Crate) -> Vec<(&Path, &syn::ImplItemFn)> {
        let mut out = Vec::new();
        for module in krate.modules.values() {
            for item in &module.items {
                let syn::Item::Impl(imp) = item else {
                    continue;
                };
                if imp.trait_.is_some() {
                    continue;
                }
                let syn::Type::Path(tp) = &*imp.self_ty else {
                    continue;
                };
                if tp.path.segments.last().is_none_or(|s| s.ident != Self::HANDLE) {
                    continue;
                }
                for item in &imp.items {
                    if let syn::ImplItem::Fn(f) = item {
                        if matches!(f.vis, syn::Visibility::Public(_)) {
                            out.push((module.file.as_path(), f));
                        }
                    }
                }
            }
        }
        out
    }
}

impl Rule for ConsumerHandleSurface {
    fn name(&self) -> &'static str {
        "check-consumer-handle"
    }

    fn check(&self, krate: &Crate, findings: &mut Vec<String>) -> usize {
        let module_path: ModPath = Self::TRAIT_MODULE.iter().map(ToString::to_string).collect();
        let Some(module) = krate.modules.get(&module_path) else {
            return 0;
        };
        let Some(consumer) = module.items.iter().find_map(|item| match item {
            syn::Item::Trait(t) if t.ident == Self::TRAIT => Some(t),
            _ => None,
        }) else {
            return 0;
        };
        let trait_methods: Vec<&syn::TraitItemFn> = consumer
            .items
            .iter()
            .filter_map(|item| match item {
                syn::TraitItem::Fn(f) => Some(f),
                _ => None,
            })
            .collect();
        let handle_methods = Self::handle_methods(krate);
        let handle_file = handle_methods.first().map_or(module.file.as_path(), |(file, _)| file);
        let handle = Self::HANDLE;
        let contract = Self::TRAIT;

        for tm in &trait_methods {
            let name = tm.sig.ident.to_string();
            let defined = handle_methods.iter().find(|(_, f)| f.sig.ident == name);
            if Self::is_excluded(&name) {
                if let Some((file, _)) = defined {
                    findings.push(Self::excluded_finding(file, &name));
                }
                continue;
            }
            let expected = Self::reduce(&tm.sig, Some("&self"));
            match defined {
                None => findings.push(format!(
                    "{}: `{handle}` lacks `{name}` of `{contract}`; define it as `{expected}`",
                    handle_file.display()
                )),
                Some((file, f)) => {
                    let actual = Self::reduce(&f.sig, None);
                    if actual != expected {
                        findings.push(format!(
                            "{}: `{handle}::{name}` is `{actual}`; `{contract}::{name}` needs `{expected}`",
                            file.display()
                        ));
                    }
                },
            }
        }
        for (file, f) in &handle_methods {
            let name = f.sig.ident.to_string();
            if trait_methods.iter().any(|tm| tm.sig.ident == name) {
                continue;
            }
            if Self::is_excluded(&name) {
                findings.push(Self::excluded_finding(file, &name));
            } else {
                findings.push(format!(
                    "{}: `{handle}` defines `{name}`, which `{contract}` lacks; a handle method is a `{contract}` \
                     method under its CLAUDE.md §2 name",
                    file.display()
                ));
            }
        }
        1
    }

    fn hint(&self) -> &'static str {
        "   `ConsumerHandle` is the `Consumer` a rebalance listener or commit callback
   calls back into (consumer-threading.md §31; KIP-1306's `RebalanceConsumer`).
   Keep its `pub fn`s equal to the `Consumer` trait's methods, each taking
   `&self` and the trait method's parameters and returning its type (an
   `async fn … -> R` becomes `impl Future<Output = R> + Send`), minus `poll`,
   `close`, `subscribe`, `unsubscribe`, `assign`, `enforce_rebalance`,
   `subscription`, `metrics` and `handle` with their `_with_` overloads.
   `wakeup` stays: the handle is the only `Send + Sync` object another thread
   can call it on. A handle-only method has no Java counterpart: add it to
   `Consumer` under its CLAUDE.md §2 name or drop it."
    }
}

// ---------------------------------------------------------------------------
// Rule: check-consumer-receiver
// ---------------------------------------------------------------------------

/// Checks that every `Consumer` / `DynConsumer` method takes `&mut self`,
/// except `wakeup`, `metrics` and `handle` (`consumer-threading.md` §1–§2).
///
/// Java's `KafkaConsumer` is single-threaded: its methods run
/// `acquireAndEnsureOpen()`, which throws `ConcurrentModificationException`
/// when a second thread is inside the consumer. Rust enforces that at compile
/// time: the consumer is `Send + !Sync`, and its guarded methods take
/// `&mut self`, so no two calls can overlap. A method taking `&self` would
/// compile today and stay callable through a shared reference forever after,
/// so an implementation that later needs to mutate state for it (Java adding
/// guarded work to a getter) could not, without a breaking change.
///
/// A method keeps `&self` only when Java leaves it outside the guard, plus the
/// Rust-only `handle`. Java's unguarded methods are `wakeup()` (cross-thread
/// by design), `metrics()`, `clientInstanceId(Duration)`,
/// `registerMetricForSubscription` and `unregisterMetricFromSubscription`
/// (`AsyncKafkaConsumer.java:887-902,1301,1855-1861`); the last three are not
/// translated yet, and each joins [`Self::SHARED`] when it is. `handle` hands
/// out the `Send + Sync` handle for callbacks and other tasks.
///
/// Scope: the public traits named `Consumer` and `DynConsumer`. Either missing
/// is a finding, so the rule cannot silently check nothing.
struct ConsumerReceiver;

impl ConsumerReceiver {
    /// The traits whose methods are checked.
    const TRAITS: [&'static str; 2] = ["Consumer", "DynConsumer"];

    /// The methods that take `&self`: the translated methods Java leaves
    /// outside `acquireAndEnsureOpen()`, and the Rust-only `handle`. When
    /// `client_instance_id`, `register_metric_for_subscription` or
    /// `unregister_metric_from_subscription` is translated, it is added here.
    const SHARED: [&'static str; 3] = ["wakeup", "metrics", "handle"];

    /// The receiver of `sig` as written: `&mut self`, `&self`, `self`, or
    /// `None` for an associated function.
    fn receiver(sig: &syn::Signature) -> Option<&'static str> {
        sig.inputs.iter().find_map(|arg| match arg {
            syn::FnArg::Receiver(r) => Some(match (&r.reference, &r.mutability) {
                (Some(_), Some(_)) => "&mut self",
                (Some(_), None) => "&self",
                (None, _) => "self",
            }),
            syn::FnArg::Typed(_) => None,
        })
    }
}

impl Rule for ConsumerReceiver {
    fn name(&self) -> &'static str {
        "check-consumer-receiver"
    }

    fn check(&self, krate: &Crate, findings: &mut Vec<String>) -> usize {
        let mut checked = 0;
        for name in Self::TRAITS {
            let found = krate.public_types().into_iter().find_map(|(path, ident)| {
                if ident != name {
                    return None;
                }
                match krate.type_item(&path, &ident) {
                    syn::Item::Trait(t) => Some((krate.modules[&path].file.clone(), t)),
                    _ => None,
                }
            });
            let Some((file, t)) = found else {
                findings.push(format!("no public trait `{name}`; the rule checks its method receivers"));
                continue;
            };
            for item in &t.items {
                let syn::TraitItem::Fn(f) = item else {
                    continue;
                };
                checked += 1;
                let method = f.sig.ident.to_string();
                let expected = if Self::SHARED.contains(&method.as_str()) {
                    "&self"
                } else {
                    "&mut self"
                };
                let actual = Self::receiver(&f.sig).unwrap_or("no receiver");
                if actual != expected {
                    findings.push(format!(
                        "{}: `{name}::{method}` takes `{actual}`; it must take `{expected}`",
                        file.display()
                    ));
                }
            }
        }
        checked
    }

    fn hint(&self) -> &'static str {
        "   The consumer is single-threaded, as Java's `acquireAndEnsureOpen()` makes it:
   every `Consumer` / `DynConsumer` method Java guards takes `&mut self`, so the
   compiler rules out two overlapping calls and a getter can later mutate state
   without a breaking change. A method takes `&self` only when Java leaves it
   unguarded (`wakeup`, `metrics`; `client_instance_id`,
   `register_metric_for_subscription` and `unregister_metric_from_subscription`
   once translated, added to `SHARED`), plus the Rust-only `handle`
   (consumer-threading.md §1-§2). A guarded getter that cannot fail in Java's
   sense panics with Java's message instead of taking `&self`."
    }
}

// ---------------------------------------------------------------------------
// Rule: check-send-future
// ---------------------------------------------------------------------------

/// Checks that no public method is a native `async fn` (CLAUDE.md §3, forward
/// compatibility).
///
/// The future of an `async fn` is `Send` exactly when everything its body
/// holds across an `.await` is, and the compiler infers that from the body.
/// A caller spawning it on a multi-threaded runtime depends on the inference;
/// a later change that keeps a non-`Send` value (an `Rc`, a `MutexGuard`)
/// across an `.await` removes the `Send` and breaks that caller, with nothing
/// in the signature having changed. Declaring
/// `fn name(…) -> impl Future<Output = T> + Send` with an `async move` body
/// makes `Send` part of the contract, so such a change fails to compile in
/// this crate instead, as the `Producer` trait already does.
///
/// An `#[async_trait]` method is exempt: the macro boxes its future as
/// `Pin<Box<dyn Future + Send>>`, `Send` included in the signature.
///
/// Scope: the public fns of [`public_items`], outside the C FFI module.
struct SendFuture;

impl Rule for SendFuture {
    fn name(&self) -> &'static str {
        "check-send-future"
    }

    fn check(&self, krate: &Crate, findings: &mut Vec<String>) -> usize {
        let mut checked = 0;
        for (file, item) in public_items(krate) {
            if !item.is_fn || item.module.first().is_some_and(|m| m == "ffi") {
                continue;
            }
            checked += 1;
            if item.is_async {
                let name = item
                    .owner
                    .as_ref()
                    .map_or_else(|| item.name.clone(), |o| format!("{o}::{}", item.name));
                findings.push(format!(
                    "{}: `{name}` is a public `async fn`, whose future is `Send` only while its body keeps it so; \
                     declare `fn {}(…) -> impl Future<Output = …> + Send` with an `async move` body",
                    file.display(),
                    item.name
                ));
            }
        }
        checked
    }

    fn hint(&self) -> &'static str {
        "   A public `async fn` leaves `Send` to inference from its body. Write
   `fn name(&self, …) -> impl Future<Output = T> + Send + '_` (edition 2024
   captures the parameters' lifetimes) whose body is `async move { … }`, so a
   change that drops `Send` fails here rather than in a caller's `tokio::spawn`.
   `#[async_trait]` methods already box a `Send` future and are exempt."
    }
}

// ---------------------------------------------------------------------------
// Rule: check-error-predicate
// ---------------------------------------------------------------------------

/// The module the crate's flat error enum lives in.
const ERROR_MODULE: [&str; 2] = ["common", "error"];

/// Checks that every variant of `common::Error` has its class predicate
/// (CLAUDE.md §3, forward compatibility).
///
/// Java's `catch (TopicExistsException e)` also catches a subclass a later Kafka
/// release adds. Here that subclass becomes a new variant, so a caller's
/// `matches!(e, Error::TopicExists(_))` would miss it, while
/// `e.is_topic_exists_error()` keeps matching once the new payload lists its
/// parent's predicate. So users classify with predicates, and every variant needs
/// one: `is_` + its payload type snake_cased with a trailing `Error` dropped +
/// `_error` (`TopicExistsError` → `is_topic_exists_error`, `KafkaError` →
/// `is_kafka_error`).
///
/// A predicate is spelled out in several places. Missing any one of them
/// compiles, but then quietly gives the wrong answer or isn't reachable, so this
/// rule checks every one:
///
///  - a method on `ErrorHierarchy`, so a payload can override it — an inherent
///    `matches!` could never answer for a future subclass;
///  - the variant's own payload answering `true`: its `kafka_error_type!` /
///    `message_only_error!` `extends:` list names it, or its hand-written
///    `impl ErrorHierarchy` overrides it.
///
/// And for every `ErrorHierarchy` method, intermediate classes included:
///
///  - a forwarder in `impl ErrorHierarchy for Box<T>` — without it a boxed
///    payload silently answers the trait's `false`;
///  - a public forwarder on `Error` calling the trait through UFCS (a method
///    call would resolve to the inherent method itself and recurse);
///  - a C export `kafka_common_Error_<predicate>` in `src/ffi` (CLAUDE.md §4).
///
/// The rule checks that the predicates exist, not how the crate's own code
/// classifies errors.
struct ErrorPredicate;

impl Rule for ErrorPredicate {
    fn name(&self) -> &'static str {
        "check-error-predicate"
    }

    fn check(&self, krate: &Crate, findings: &mut Vec<String>) -> usize {
        let path: ModPath = ERROR_MODULE.iter().map(ToString::to_string).collect();
        let Some(module) = krate.modules.get(&path) else {
            findings.push(format!("no `{}` module to find `Error` in", ERROR_MODULE.join("::")));
            return 0;
        };
        let file = module.file.display();
        let Some(error) = module.items.iter().find_map(|item| match item {
            syn::Item::Enum(e) if e.ident == "Error" => Some(e),
            _ => None,
        }) else {
            findings.push(format!("{file}: no `enum Error`"));
            return 0;
        };

        let trait_methods = hierarchy_trait_methods(&module.items);
        let box_forwarders = hierarchy_impl_methods(&module.items, |ty| ty == "Box");
        let error_forwarders = error_forwarders(&module.items);
        let overrides = payload_overrides(krate);
        let c_exports = c_exports(krate);

        let mut checked = 0usize;
        for variant in &error.variants {
            checked += 1;
            let Some(payload) = variant_payload(variant) else {
                findings.push(format!(
                    "{file}: `Error::{}` does not wrap a single payload type",
                    variant.ident
                ));
                continue;
            };
            let predicate = class_predicate(&payload);
            if !trait_methods.contains(&predicate) {
                findings.push(format!(
                    "{file}: `Error::{}` has no class predicate: `ErrorHierarchy` declares no `{predicate}`",
                    variant.ident
                ));
            }
            if !overrides.get(&payload).is_some_and(|preds| preds.contains(&predicate)) {
                findings.push(format!("{file}: `{payload}` does not answer `true` to its own `{predicate}`"));
            }
        }
        for method in &trait_methods {
            checked += 1;
            if !box_forwarders.contains(method) {
                findings.push(format!("{file}: `impl ErrorHierarchy for Box<T>` does not forward `{method}`"));
            }
            if !error_forwarders.contains(method) {
                findings.push(format!(
                    "{file}: `Error` has no `pub fn {method}(&self) -> bool` forwarding to `ErrorHierarchy::{method}(self)`"
                ));
            }
            if !c_exports.contains(&format!("kafka_common_Error_{method}")) {
                findings.push(format!(
                    "{file}: `{method}` has no C export `kafka_common_Error_{method}` in `ffi`"
                ));
            }
        }
        checked
    }

    fn hint(&self) -> &'static str {
        "   Every `Error` variant needs its class predicate (CLAUDE.md §3), named
   `is_` + payload type snake_cased without a trailing `Error` + `_error`:
     1. a default-false `fn is_x_error(&self) -> bool` on `ErrorHierarchy`;
     2. its forwarder in `impl ErrorHierarchy for Box<T>`;
     3. `pub fn is_x_error(&self) -> bool { ErrorHierarchy::is_x_error(self) }`
        on `Error`;
     4. the payload answering `true`: `is_x_error` in its `extends:` list, or
        an override in its hand-written `impl ErrorHierarchy` — and in every
        payload whose Java class extends it;
     5. the C export, regenerated with `cargo xtask generate-error-predicates`."
    }
}

/// `is_` + `payload` snake_cased with a trailing `Error` dropped + `_error`.
fn class_predicate(payload: &str) -> String {
    let base = payload.strip_suffix("Error").filter(|b| !b.is_empty()).unwrap_or(payload);
    format!("is_{}_error", java::snake_case(base))
}

/// The payload type `variant` wraps, looking through a `Box`: `Timeout(TimeoutError)`
/// and `QuotaViolation(Box<QuotaViolationError>)` give `TimeoutError` and
/// `QuotaViolationError`.
fn variant_payload(variant: &syn::Variant) -> Option<String> {
    let syn::Fields::Unnamed(fields) = &variant.fields else {
        return None;
    };
    let [field] = fields.unnamed.iter().collect::<Vec<_>>()[..] else {
        return None;
    };
    let mut ty = &field.ty;
    loop {
        let syn::Type::Path(p) = ty else { return None };
        let last = p.path.segments.last()?;
        if last.ident != "Box" {
            return Some(last.ident.to_string());
        }
        let syn::PathArguments::AngleBracketed(args) = &last.arguments else {
            return None;
        };
        let Some(syn::GenericArgument::Type(inner)) = args.args.first() else {
            return None;
        };
        ty = inner;
    }
}

/// Whether `path` names the `ErrorHierarchy` trait, however qualified.
fn is_hierarchy_path(path: &syn::Path) -> bool {
    path.segments.last().is_some_and(|s| s.ident == "ErrorHierarchy")
}

/// The methods `trait ErrorHierarchy` declares among `items`.
fn hierarchy_trait_methods(items: &[syn::Item]) -> BTreeSet<String> {
    items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Trait(t) if t.ident == "ErrorHierarchy" => Some(t),
            _ => None,
        })
        .flat_map(|t| &t.items)
        .filter_map(|item| match item {
            syn::TraitItem::Fn(f) => Some(f.sig.ident.to_string()),
            _ => None,
        })
        .collect()
}

/// The last path segment of `ty`, if it is a path type.
fn type_ident(ty: &syn::Type) -> Option<String> {
    let syn::Type::Path(p) = ty else { return None };
    p.path.segments.last().map(|s| s.ident.to_string())
}

/// The methods of the `impl ErrorHierarchy for T` blocks among `items` whose `T`
/// satisfies `self_ty`.
fn hierarchy_impl_methods(items: &[syn::Item], self_ty: impl Fn(&str) -> bool) -> BTreeSet<String> {
    items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Impl(i)
                if i.trait_.as_ref().is_some_and(|(_, p, _)| is_hierarchy_path(p))
                    && type_ident(&i.self_ty).is_some_and(|t| self_ty(&t)) =>
            {
                Some(i)
            },
            _ => None,
        })
        .flat_map(|i| &i.items)
        .filter_map(|item| match item {
            syn::ImplItem::Fn(f) => Some(f.sig.ident.to_string()),
            _ => None,
        })
        .collect()
}

/// The predicates `Error` forwards publicly: each `pub fn is_x_error(&self) -> bool`
/// of an inherent `impl Error` whose body calls `ErrorHierarchy::is_x_error`.
fn error_forwarders(items: &[syn::Item]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for item in items {
        let syn::Item::Impl(i) = item else { continue };
        if i.trait_.is_some() || type_ident(&i.self_ty).as_deref() != Some("Error") {
            continue;
        }
        for item in &i.items {
            let syn::ImplItem::Fn(f) = item else { continue };
            let name = f.sig.ident.to_string();
            let returns_bool = matches!(&f.sig.output, syn::ReturnType::Type(_, ty) if compact(&**ty) == "bool");
            let body = f.block.to_token_stream().to_string().replace(' ', "");
            let ufcs = body.contains(&format!("ErrorHierarchy::{name}(self)"));
            if matches!(f.vis, syn::Visibility::Public(_)) && returns_bool && ufcs {
                out.insert(name);
            }
        }
    }
    out
}

/// The predicates each error payload answers `true` to, keyed by payload type:
/// the `extends:` list of its `kafka_error_type!` / `message_only_error!`, plus
/// the methods of its hand-written `impl ErrorHierarchy`, from anywhere in the
/// crate.
fn payload_overrides(krate: &Crate) -> BTreeMap<String, BTreeSet<String>> {
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for module in krate.modules.values() {
        for item in &module.items {
            match item {
                syn::Item::Macro(m) => {
                    if let Some(def) = error_macro_type_def(m) {
                        out.entry(def.name).or_default().extend(def.extends);
                    }
                },
                syn::Item::Impl(i) if i.trait_.as_ref().is_some_and(|(_, p, _)| is_hierarchy_path(p)) => {
                    let Some(ty) = type_ident(&i.self_ty) else { continue };
                    let methods = i.items.iter().filter_map(|item| match item {
                        syn::ImplItem::Fn(f) => Some(f.sig.ident.to_string()),
                        _ => None,
                    });
                    out.entry(ty).or_default().extend(methods);
                },
                _ => {},
            }
        }
    }
    out
}

/// The `#[no_mangle]` functions of the C FFI (`ffi` and its submodules).
fn c_exports(krate: &Crate) -> BTreeSet<String> {
    krate
        .modules
        .iter()
        .filter(|(path, _)| path.first().is_some_and(|p| p == "ffi"))
        .flat_map(|(_, m)| &m.items)
        .filter_map(|item| match item {
            syn::Item::Fn(f) if is_no_mangle(&f.attrs) => Some(f.sig.ident.to_string()),
            _ => None,
        })
        .collect()
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
            impl KafkaConsumer {
                #[doc(alias = "rust-only")]
                pub fn rust_only(&self) {}
                #[doc(alias = "rust-only")]
                pub(crate) fn needless(&self) {}
            }
            #[doc(alias = "org.apache.kafka.clients.consumer.MockConsumer")]
            #[doc(alias = "public-in-rust")]
            pub struct MockConsumer;
            #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerConfig")]
            pub struct ConsumerConfig;
            impl ConsumerConfig {
                // Typed accessors of a plain key: findings.
                #[doc(alias = "rust-only")]
                pub fn request_timeout_ms(&self) -> i32 { 0 }
                #[doc(alias = "rust-only")]
                pub fn set_client_id(self, client_id: impl Into<String>) -> Self { self }
                // A class-valued key set through a trait object (CLAUDE.md §2).
                #[doc(alias = "rust-only")]
                pub fn set_partitioner(self, partitioner: Box<dyn Partitioner>) -> Self { self }
                // Constants: a public Java field, one inherited from
                // `AbstractConfig` and a tagged Rust-only key pass.
                pub const GROUP_ID_CONFIG: &str = "group.id";
                pub const CONFIG_PROVIDERS_CONFIG: &str = "config.providers";
                // selects a built-in partitioner in place of a class-valued key
                #[doc(alias = "rust-only")]
                pub const PARTITIONER_TYPE_CONFIG: &str = "partitioner.type";
                // A package-private Java field, a name Java lacks and an
                // internal key: findings.
                pub const DEFAULT_ISOLATION_LEVEL: &str = "read_uncommitted";
                pub const SASL_JAAS_CONFIG: &str = "sasl.jaas.config";
                pub const LEAVE_GROUP_ON_CLOSE_CONFIG: &str = "internal.leave.group.on.close";
                pub(crate) const THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED: &str =
                    "internal.throw.on.fetch.stable.offset.unsupported";
            }
            // An internal key outside any class: a finding, tag or not.
            // a key of no Java class
            #[doc(alias = "rust-only")]
            pub const INTERNAL_KEY: &str = "internal.auto.downgrade.txn.commit";
            #[doc(alias = "rust-only")]
            pub trait Partitioner {}
            pub struct Unmarked;
            #[doc(alias = "rust-only")]
            pub struct Allowed;
            pub(crate) struct Hidden;
            #[doc(alias = "rust-only")]
            pub(crate) struct NeedlessTag;
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
            #[doc(alias = "rust-only")]
            pub struct kafka_common_Error_t;
            #[unsafe(no_mangle)]
            pub extern "C" fn kafka_consumer_KafkaConsumer_new() {}
            #[unsafe(no_mangle)]
            pub extern "C" fn kafka_common_Cluster_new() {}
            #[unsafe(no_mangle)]
            pub extern "C" fn kafka_consumer_Nothing_new() {}
            #[doc(alias = "rust-only")]
            #[unsafe(no_mangle)]
            pub extern "C" fn kafka_consumer_StringList_new() {}
            #[unsafe(no_mangle)]
            pub extern "C" fn kafka_consumer_StringList_destroy() {}
            #[doc(alias = "public-in-rust")]
            #[unsafe(no_mangle)]
            pub extern "C" fn kafka_consumer_KafkaConsumer_destroy() {}
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

    fn class(package: &str, path: &[&str]) -> java::JavaClass {
        java::JavaClass {
            module: package.split('.').filter(|s| *s != "clients").map(str::to_string).collect(),
            package: package.to_string(),
            path: path.iter().map(|s| s.to_string()).collect(),
            methods: BTreeSet::new(),
            overloads: Vec::new(),
            fields: BTreeMap::new(),
            field_visibility: BTreeMap::new(),
            enum_constants: Vec::new(),
            deprecated: false,
            is_test: false,
            kind: java::JavaKind::Class,
            visibility: java::Visibility::Public,
            is_abstract: false,
            functional_interface: false,
            supertypes: Vec::new(),
        }
    }

    /// `ConsumerConfig`, a subclass of `AbstractConfig`.
    fn config_class() -> java::JavaClass {
        let mut class = class("clients.consumer", &["ConsumerConfig"]);
        class.supertypes = vec!["AbstractConfig".to_string()];
        class.field_visibility = [
            ("GROUP_ID_CONFIG", java::Visibility::Public),
            ("DEFAULT_ISOLATION_LEVEL", java::Visibility::Package),
            ("LEAVE_GROUP_ON_CLOSE_CONFIG", java::Visibility::Public),
        ]
        .into_iter()
        .map(|(name, visibility)| (name.to_string(), visibility))
        .collect();
        class
    }

    /// `AbstractConfig`, declaring the field `ConsumerConfig` inherits.
    fn abstract_config_class() -> java::JavaClass {
        let mut class = class("common.config", &["AbstractConfig"]);
        class.field_visibility = [("CONFIG_PROVIDERS_CONFIG".to_string(), java::Visibility::Public)].into();
        class
    }

    fn fixture_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("xtask-public-audience-{name}-{}", std::process::id()))
    }

    /// The fixture crate written to its own temp dir, and the rule over a
    /// synthetic Java index.
    fn fixture(name: &str) -> (Crate, PublicAudience) {
        let dir = fixture_dir(name);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("lib.rs"), FIXTURE_LIB).unwrap();
        fs::write(dir.join("cbindgen.toml"), FIXTURE_CBINDGEN).unwrap();
        let krate = Crate::load(&dir.join("lib.rs")).unwrap();
        let rule = PublicAudience {
            index: JavaIndex {
                classes: vec![
                    class("clients.consumer", &["KafkaConsumer"]),
                    class("clients.consumer", &["MockConsumer"]),
                    config_class(),
                    abstract_config_class(),
                    class("clients.consumer.internals", &["Fetcher"]),
                    class("common.network", &["Selector"]),
                    class("common", &["Cluster"]),
                    class("common.errors", &["TopicAuthorizationException"]),
                ],
            },
            public: [
                PUBLIC_CONSUMER,
                "org.apache.kafka.clients.consumer.ConsumerConfig",
                "org.apache.kafka.common.errors.TopicAuthorizationException",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            unsupported: ["common.network".to_string()].into(),
            cbindgen: dir.join("cbindgen.toml"),
        };
        (krate, rule)
    }

    fn run(name: &str) -> Vec<String> {
        let (krate, rule) = fixture(name);
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
        let findings = run("internal");
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
        let findings = run("disclaimer");
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
        let findings = run("not-public");
        let expected = format!(
            "{}: `crate::common::Cluster` is public but translates `org.apache.kafka.common.Cluster`: it is not \
             `@InterfaceAudience.Public` in Kafka {}",
            fixture_dir("not-public").join("lib.rs").display(),
            java::AUDIENCE_REF
        );
        assert_eq!(with(&findings, "`crate::common::Cluster`"), [expected]);
    }

    #[test]
    fn test_public_and_tagged_items_pass() {
        let findings = run("pass");
        // Public in Java, in Rust and in C.
        assert!(with(&findings, "`crate::consumer::KafkaConsumer`").is_empty(), "{findings:#?}");
        assert!(with(&findings, "C `kafka_consumer_KafkaConsumer`").is_empty(), "{findings:#?}");
        // Tagged: a Rust-only type, a Java class public on purpose, and C
        // prefixes through a tagged fn and a tagged `_t` type of the include list.
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
    fn test_unmarked_public_item_needs_a_rust_only_tag() {
        let findings = run("unmarked");
        let expected = format!(
            "{}: `crate::consumer::Unmarked` is public but carries no Java marker; mark the class it translates, or \
             tag it #[doc(alias = \"rust-only\")] under a comment giving the reason it is public",
            fixture_dir("unmarked").join("lib.rs").display()
        );
        assert_eq!(with(&findings, "Unmarked"), [expected]);
    }

    #[test]
    fn test_pub_generated_module_and_its_pub_use_are_findings() {
        let findings = run("generated");
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
        let findings = run("ffi");
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
        let (_, mut rule) = fixture("misplaced");
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
    fn test_typed_accessor_of_a_config_key_is_a_finding() {
        let findings = run("config");
        let file = fixture_dir("config").join("lib.rs").display().to_string();
        let finding = |method: &str| {
            format!(
                "{file}: `ConsumerConfig::{method}` is a typed accessor of a configuration key on \
                 `org.apache.kafka.clients.consumer.ConsumerConfig`, a translation of an `AbstractConfig`: make it \
                 `pub(crate)` so the string map stays the only typed input (`new(&props)` validates it as Java's \
                 `ConfigDef` does), or take a trait object for a class-valued key (CLAUDE.md §2)"
            )
        };
        let mut typed = with(&findings, "typed accessor of a configuration key");
        typed.sort();
        assert_eq!(typed, [finding("request_timeout_ms"), finding("set_client_id")]);
        // A class-valued key set through a trait object is CLAUDE.md §2's shape,
        // and `impl Into<String>` is not a trait object.
        assert!(with(&findings, "set_partitioner").is_empty(), "{findings:#?}");
        // The only method findings on the class are those two: its tags are
        // not needless. Its constants are `test_constants_translate_public_java_fields`'.
        let methods = with(&findings, "ConsumerConfig");
        assert_eq!(
            methods.iter().filter(|f| !f.contains("is public but")).count(),
            2,
            "{findings:#?}"
        );
    }

    #[test]
    fn test_constants_translate_public_java_fields() {
        let findings = run("constants");
        let file = fixture_dir("constants").join("lib.rs").display().to_string();
        // Class findings name the item by its path (`crate::…`).
        let mut constants: Vec<_> = with(&findings, "is public but")
            .into_iter()
            .filter(|f| !f.contains("`crate::"))
            .collect();
        constants.sort();
        assert_eq!(
            constants,
            [
                format!(
                    "{file}: `ConsumerConfig::DEFAULT_ISOLATION_LEVEL` is public but translates \
                     `org.apache.kafka.clients.consumer.ConsumerConfig#DEFAULT_ISOLATION_LEVEL`, which is \
                     package-private in Java; make it `pub(crate)`"
                ),
                // Java's visibility does not matter for an internal key.
                format!(
                    "{file}: `ConsumerConfig::LEAVE_GROUP_ON_CLOSE_CONFIG` is public but holds \
                     `internal.leave.group.on.close`, an internal configuration key Java may remove in a minor \
                     release; make it `pub(crate)`"
                ),
                format!(
                    "{file}: `ConsumerConfig::SASL_JAAS_CONFIG` is public but \
                     `org.apache.kafka.clients.consumer.ConsumerConfig` declares no field `SASL_JAAS_CONFIG`; give \
                     it the name of the public Java field it translates, make it `pub(crate)`, or tag it \
                     #[doc(alias = \"rust-only\")] under a comment giving the reason it is public"
                ),
                format!(
                    "{file}: `INTERNAL_KEY` is public but holds `internal.auto.downgrade.txn.commit`, an internal \
                     configuration key Java may remove in a minor release; make it `pub(crate)`"
                ),
            ]
        );
        // The `rust-only` tag on `PARTITIONER_TYPE_CONFIG` lets it through.
        assert!(with(&findings, "PARTITIONER_TYPE_CONFIG").is_empty(), "{findings:#?}");
    }

    #[test]
    fn test_needless_tags_are_findings() {
        let findings = run("needless");
        let file = fixture_dir("needless").join("lib.rs").display().to_string();
        let mut needless = with(&findings, "would pass without it");
        needless.sort();
        assert_eq!(
            needless,
            [
                format!("{file}: `KafkaConsumer::needless` is tagged `rust-only` but would pass without it; remove the tag"),
                format!("{file}: `NeedlessTag` is tagged `rust-only` but would pass without it; remove the tag"),
                format!(
                    "{file}: `kafka_consumer_KafkaConsumer_destroy` is tagged `public-in-rust` but would pass without it; \
                     remove the tag"
                ),
            ]
        );
        // The `rust-only` tag lets `check-java-name` through for a public
        // unmarked method, so it is not needless.
        assert!(with(&findings, "rust_only").is_empty(), "{findings:#?}");
    }

    /// A crate whose every function exercises one outcome of the unmarked-method
    /// check of `check-java-name`.
    const UNMARKED_LIB: &str = r#"
        pub mod producer {
            mod p {
                pub struct P;
                impl P {
                    #[doc(alias = "org.apache.kafka.clients.producer.P#send")]
                    pub fn send(&self) {}
                    pub fn helper(&self) {}
                    // Rust-only, and public on purpose.
                    #[doc(alias = "rust-only")]
                    pub fn allowed(&self) {}
                    pub(crate) fn internal(&self) {}
                    #[cfg(test)]
                    pub fn test_only(&self) {}
                }
                impl Clone for P {
                    fn clone(&self) -> Self { P }
                }
            }
            pub use p::P;
            pub trait T {
                #[doc(alias = "org.apache.kafka.clients.producer.T#m")]
                fn m(&self);
                fn rust_only(&self);
            }
            pub fn free_helper() {}
        }
        mod private {
            pub struct Q;
            impl Q {
                pub fn hidden(&self) {}
            }
        }
        pub mod ffi {
            #[unsafe(no_mangle)]
            pub extern "C" fn kafka_producer_P_new() {}
        }
    "#;

    #[test]
    fn test_unmarked_public_methods_are_findings() {
        let dir = fixture_dir("unmarked-methods");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("lib.rs"), UNMARKED_LIB).unwrap();
        let krate = Crate::load(&dir.join("lib.rs")).unwrap();
        fs::remove_dir_all(&dir).unwrap();
        let rule = JavaName { index: JavaIndex { classes: Vec::new() }, package_modules: BTreeSet::new() };

        let mut findings = Vec::new();
        // `send`, `helper`, `allowed`, `m`, `rust_only` and `free_helper`.
        assert_eq!(rule.check_unmarked_methods(&krate, &mut findings), 6);
        let file = dir.join("lib.rs");
        let expected = |name: &str| {
            format!(
                "{}: public method `{name}` carries no Java method marker; mark the Java method it translates \
                 (#[doc(alias = \"org.apache.kafka.<package>.<Class>#<method>\")]), make it `pub(crate)`, or tag \
                 it #[doc(alias = \"rust-only\")] under a comment giving the reason it is public",
                file.display()
            )
        };
        let mut want = vec![expected("P::helper"), expected("T::rust_only"), expected("free_helper")];
        findings.sort();
        want.sort();
        assert_eq!(findings, want);
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
        // A package-less helper standing for a JDK type.
        assert_eq!(ffi_class("kafka_List_t"), Some((vec![], "List")));
        assert_eq!(ffi_prefix("kafka_List_add"), "kafka_List");
        assert_eq!(ffi_class("kafka_string_destroy"), None);
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
        pub trait Quiet {
            fn run(&self) -> impl std::future::Future<Output = ()>;
        }
        pub(crate) trait DynQuiet {
            fn run(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + '_>>;
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
        // Every `pub trait` above: not the `pub(crate)` `DynQuiet` and `Hidden`,
        // nor `Unreachable`.
        assert_eq!(checked, 21);
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

    /// A `pub(crate)` companion exempts its trait like a public one
    /// (`DynSerializer` for `Serializer`).
    #[test]
    fn test_crate_private_dyn_companion_exempts() {
        let findings = run("private-companion");
        assert_eq!(finding(&findings, "Quiet"), None);
    }

    #[test]
    fn test_non_public_traits_are_ignored() {
        let findings = run("hidden");
        assert_eq!(finding(&findings, "Hidden"), None);
        assert_eq!(finding(&findings, "Unreachable"), None);
    }
}

#[cfg(test)]
mod dyn_forwarding_tests {
    use super::*;

    /// `Good` / `DynGood` forward correctly; `Bad` / `DynBad` break every rule
    /// once; `Lone` / `DynLone` have no impls at all.
    const FIXTURE_LIB: &str = r#"
        use std::future::Future;
        use std::pin::Pin;
        type BoxFuture<'a> = Pin<Box<dyn Future<Output = i32> + Send + 'a>>;

        pub trait Good {
            fn run(&self) -> impl Future<Output = i32> + Send;
            fn add(&self, x: i32, y: i32) -> i32;
            fn get(&self) -> i32 { 0 }
        }
        pub trait DynGood {
            fn run<'a>(&'a self) -> BoxFuture<'a>;
            fn add(&self, x: i32, y: i32) -> i32;
            fn get(&self) -> i32;
        }
        impl<P: Good + Sync> DynGood for P {
            fn run<'a>(&'a self) -> BoxFuture<'a> { Box::pin(Good::run(self)) }
            fn add(&self, x: i32, y: i32) -> i32 { Good::add(self, x, y) }
            fn get(&self) -> i32 { Good::get(self) }
        }
        impl Good for dyn DynGood + '_ {
            async fn run(&self) -> i32 { DynGood::run(self).await }
            fn add(&self, x: i32, y: i32) -> i32 { DynGood::add(self, x, y) }
            fn get(&self) -> i32 { DynGood::get(self) }
        }

        pub trait Bad {
            fn run(&self) -> i32;
            fn add(&self, x: i32, y: i32) -> i32;
            fn get(&self) -> i32 { 0 }
            fn only_here(&self);
        }
        pub trait DynBad {
            fn run(&self) -> i32;
            fn add(&self, x: i32, y: i32) -> i32;
            fn get(&self) -> i32;
            fn only_dyn(&self);
        }
        impl<P: Bad> DynBad for P {
            fn run(&self) -> i32 { Bad::get(self) }
            fn add(&self, x: i32, y: i32) -> i32 { Bad::add(self, y, x) }
            fn get(&self) -> i32 { Bad::get(self) }
            fn only_dyn(&self) {}
        }
        impl Bad for dyn DynBad + '_ {
            fn run(&self) -> i32 { DynBad::run(self) }
            fn add(&self, x: i32, y: i32) -> i32 { DynBad::add(self, x, y) }
            fn only_here(&self) {}
        }

        pub trait Lone {
            fn run(&self);
        }
        pub trait DynLone {
            fn run(&self);
        }

        pub trait Quiet {
            fn run(&self) -> impl Future<Output = i32> + Send;
            fn get(&self) -> i32 { 0 }
        }
        pub(crate) trait DynQuiet {
            fn run<'a>(&'a self) -> BoxFuture<'a>;
            fn get(&self) -> i32;
        }
        impl<P: Quiet + Sync> DynQuiet for P {
            fn run<'a>(&'a self) -> BoxFuture<'a> { Box::pin(Quiet::run(self)) }
            fn get(&self) -> i32 { Quiet::get(self) }
        }
        impl Quiet for dyn DynQuiet + '_ {
            async fn run(&self) -> i32 { DynQuiet::run(self).await }
        }
    "#;

    fn run(name: &str) -> (Vec<String>, String) {
        let dir = std::env::temp_dir().join(format!("xtask-dyn-forwarding-{name}-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("lib.rs"), FIXTURE_LIB).unwrap();
        let krate = Crate::load(&dir.join("lib.rs")).unwrap();
        let mut findings = Vec::new();
        let checked = DynForwarding.check(&krate, &mut findings);
        fs::remove_dir_all(&dir).unwrap();
        // The four pairs, the `pub(crate)` `DynQuiet` included.
        assert_eq!(checked, 4);
        (findings, dir.join("lib.rs").display().to_string())
    }

    #[test]
    fn test_correct_forwarding_passes() {
        let (findings, _) = run("good");
        assert!(findings.iter().all(|f| !f.contains("Good")), "{findings:#?}");
    }

    /// A `pub(crate)` companion is checked like a public one: `DynQuiet`
    /// forwards `run` correctly and leaves the defaulted `get` out of its
    /// `impl Quiet for dyn DynQuiet`.
    #[test]
    fn test_crate_private_companion_is_checked() {
        let (findings, file) = run("private");
        let about_quiet: Vec<&String> = findings.iter().filter(|f| f.contains("Quiet")).collect();
        assert_eq!(
            about_quiet,
            [&format!(
                "{file}: `impl Quiet for dyn DynQuiet` does not define `get`; define it as a forward to `DynQuiet::get`"
            )]
        );
    }

    #[test]
    fn test_each_broken_link_is_a_finding() {
        let (mut findings, file) = run("bad");
        findings.sort();
        let mut want = vec![
            format!("{file}: `DynBad` lacks `only_here` of `Bad`; declare it and forward it both ways"),
            format!("{file}: `DynBad` declares `only_dyn`, which `Bad` lacks; remove it"),
            // A forward to the wrong method, or with its arguments swapped,
            // compiles whenever the types agree.
            format!("{file}: `impl DynBad for P`'s `run` does not call `Bad::run(self)`"),
            format!("{file}: `impl DynBad for P`'s `add` does not call `Bad::add(self, x, y)`"),
            // A defaulted method left out runs the default on a trait object,
            // not the boxed implementation's override.
            format!("{file}: `impl Bad for dyn DynBad` does not define `get`; define it as a forward to `DynBad::get`"),
            format!("{file}: no blanket `impl<P: Lone> DynLone for P`; every `Lone` must be a `DynLone`"),
            format!("{file}: no `impl Lone for dyn DynLone`; a `dyn DynLone` must be a `Lone`"),
            // The same defaulted-method gap on the `pub(crate)` companion.
            format!("{file}: `impl Quiet for dyn DynQuiet` does not define `get`; define it as a forward to `DynQuiet::get`"),
        ];
        want.sort();
        assert_eq!(findings, want);
    }
}

#[cfg(test)]
mod consumer_handle_tests {
    use super::*;

    /// The `Consumer` trait both fixtures share: two sync getters, one async
    /// method per shape the rule compares, and one method of each excluded kind.
    const TRAIT: &str = r#"
            pub trait Consumer<K, V> {
                fn assignment(&self) -> HashSet<TopicPartition>;
                fn subscription(&self) -> HashSet<String>;
                async fn poll(&mut self, timeout: Duration) -> Result<ConsumerRecords<K, V>, Error>;
                async fn commit_sync(&mut self) -> Result<(), Error>;
                async fn commit_sync_with_timeout(&mut self, timeout: Duration) -> Result<(), Error>;
                async fn commit_async_with_callback(
                    &mut self,
                    callback: Arc<dyn OffsetCommitCallback>,
                ) -> Result<(), Error>;
                async fn seek_with_offset(&mut self, partition: TopicPartition, offset: i64) -> Result<(), Error>;
                async fn close_with_options(&mut self, options: CloseOptions) -> Result<(), Error>;
                fn wakeup(&self);
                fn handle(&self) -> ConsumerHandle;
            }
    "#;

    /// A handle with exactly the permitted methods. The callback type is
    /// spelled through its module path, as the real handle spells it, and the
    /// `pub(crate)` and private methods are outside the rule.
    const GOOD_HANDLE: &str = r#"
            pub struct ConsumerHandle;
            impl ConsumerHandle {
                pub fn assignment(&self) -> HashSet<TopicPartition> { todo!() }
                pub fn commit_sync(&self) -> impl Future<Output = Result<(), Error>> + Send { async {} }
                pub fn commit_sync_with_timeout(
                    &self,
                    timeout: Duration,
                ) -> impl Future<Output = Result<(), Error>> + Send { async {} }
                pub fn commit_async_with_callback(
                    &self,
                    callback: Arc<dyn crate::consumer::OffsetCommitCallback>,
                ) -> impl Future<Output = Result<(), Error>> + Send { async {} }
                pub fn seek_with_offset(
                    &self,
                    partition: TopicPartition,
                    offset: i64,
                ) -> impl Future<Output = Result<(), Error>> + Send { async {} }
                pub fn wakeup(&self) {}
                pub(crate) fn metrics(&self) -> HashMap<MetricName, Arc<KafkaMetric>> { todo!() }
                fn async_state(&self) -> i32 { 0 }
            }
    "#;

    /// A handle breaking each rule once: an excluded method, an excluded
    /// overload, a handle-only method, a missing async method and a missing
    /// sync one (`wakeup`, which the handle must keep), and three signature
    /// drifts (receiver, parameter name, return type).
    const BAD_HANDLE: &str = r#"
            pub struct ConsumerHandle;
            impl ConsumerHandle {
                pub fn assignment(&self) -> HashSet<TopicPartition> { todo!() }
                pub fn subscription(&self) -> HashSet<String> { todo!() }
                pub fn close_with_options(&self, options: CloseOptions) -> impl Future<Output = Result<(), Error>> + Send { async {} }
                pub fn commit_async_offsets(&self) -> impl Future<Output = Result<(), Error>> + Send { async {} }
                pub fn commit_sync(&self) -> Result<(), Error> { Ok(()) }
                pub fn commit_async_with_callback(
                    &self,
                    cb: Arc<dyn OffsetCommitCallback>,
                ) -> impl Future<Output = Result<(), Error>> + Send { async {} }
                pub fn seek_with_offset(
                    &mut self,
                    partition: TopicPartition,
                    offset: i64,
                ) -> impl Future<Output = Result<(), Error>> + Send { async {} }
            }
    "#;

    fn run(name: &str, handle: &str) -> (Vec<String>, String) {
        let dir = std::env::temp_dir().join(format!("xtask-consumer-handle-{name}-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("lib.rs"), format!("pub mod consumer {{ {TRAIT} {handle} }}")).unwrap();
        let krate = Crate::load(&dir.join("lib.rs")).unwrap();
        assert!(krate.parse_errors.is_empty(), "{:#?}", krate.parse_errors);
        let mut findings = Vec::new();
        let checked = ConsumerHandleSurface.check(&krate, &mut findings);
        fs::remove_dir_all(&dir).unwrap();
        assert_eq!(checked, 1);
        (findings, dir.join("lib.rs").display().to_string())
    }

    #[test]
    fn test_permitted_surface_passes() {
        let (findings, _) = run("good", GOOD_HANDLE);
        assert!(findings.is_empty(), "{findings:#?}");
    }

    #[test]
    fn test_each_deviation_is_a_finding() {
        let (mut findings, file) = run("bad", BAD_HANDLE);
        findings.sort();
        let future = "impl Future<Output = Result<(), Error>> + Send";
        let mut want = vec![
            format!(
                "{file}: `ConsumerHandle` defines `subscription` which KIP-1306's `RebalanceConsumer` keeps from a \
                 listener; remove it"
            ),
            format!(
                "{file}: `ConsumerHandle` defines `close_with_options`, an overload of `close`, which KIP-1306's \
                 `RebalanceConsumer` keeps from a listener; remove it"
            ),
            format!(
                "{file}: `ConsumerHandle` defines `commit_async_offsets`, which `Consumer` lacks; a handle method is \
                 a `Consumer` method under its CLAUDE.md §2 name"
            ),
            format!(
                "{file}: `ConsumerHandle` lacks `commit_sync_with_timeout` of `Consumer`; define it as \
                 `fn commit_sync_with_timeout(&self, timeout: Duration) -> {future}`"
            ),
            format!("{file}: `ConsumerHandle` lacks `wakeup` of `Consumer`; define it as `fn wakeup(&self)`"),
            format!(
                "{file}: `ConsumerHandle::commit_sync` is `fn commit_sync(&self) -> Result<(), Error>`; \
                 `Consumer::commit_sync` needs `fn commit_sync(&self) -> {future}`"
            ),
            format!(
                "{file}: `ConsumerHandle::commit_async_with_callback` is `fn commit_async_with_callback(&self, cb: \
                 Arc<dyn OffsetCommitCallback>) -> {future}`; `Consumer::commit_async_with_callback` needs `fn \
                 commit_async_with_callback(&self, callback: Arc<dyn OffsetCommitCallback>) -> {future}`"
            ),
            format!(
                "{file}: `ConsumerHandle::seek_with_offset` is `fn seek_with_offset(&mut self, partition: \
                 TopicPartition, offset: i64) -> {future}`; `Consumer::seek_with_offset` needs `fn \
                 seek_with_offset(&self, partition: TopicPartition, offset: i64) -> {future}`"
            ),
        ];
        want.sort();
        assert_eq!(findings, want);
    }
}

#[cfg(test)]
mod send_future_tests {
    use super::*;

    const FIXTURE_LIB: &str = r#"
        use std::future::Future;

        pub struct Handle;
        impl Handle {
            // A native `async fn`: a finding.
            pub async fn commit(&self) -> i32 { 0 }
            // `Send` in the signature passes.
            pub fn flush(&self) -> impl Future<Output = i32> + Send { async move { 0 } }
            // Not public.
            pub(crate) async fn helper(&self) {}
            async fn private(&self) {}
        }
        pub trait Native {
            async fn run(&self);
        }
        #[async_trait::async_trait]
        pub trait Boxed {
            async fn run(&self);
        }
        pub async fn free() {}
        pub(crate) struct Hidden;
        impl Hidden {
            pub async fn run(&self) {}
        }
        pub mod ffi {
            pub async fn bridged() {}
        }
    "#;

    #[test]
    fn test_native_async_fns_are_findings() {
        let dir = std::env::temp_dir().join(format!("xtask-send-future-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("lib.rs"), FIXTURE_LIB).unwrap();
        let krate = Crate::load(&dir.join("lib.rs")).unwrap();
        let mut findings = Vec::new();
        let checked = SendFuture.check(&krate, &mut findings);
        fs::remove_dir_all(&dir).unwrap();
        // `commit`, `flush`, both `run`s of the public traits and `free`.
        assert_eq!(checked, 5);
        let file = dir.join("lib.rs").display().to_string();
        let finding = |name: &str, method: &str| {
            format!(
                "{file}: `{name}` is a public `async fn`, whose future is `Send` only while its body keeps it so; \
                 declare `fn {method}(…) -> impl Future<Output = …> + Send` with an `async move` body"
            )
        };
        findings.sort();
        let mut want = vec![
            finding("Handle::commit", "commit"),
            finding("Native::run", "run"),
            finding("free", "free"),
        ];
        want.sort();
        // The `#[async_trait]` method boxes a `Send` future; `Hidden` is not
        // public; the FFI module is the C half's.
        assert_eq!(findings, want);
    }
}

#[cfg(test)]
mod cfg_test_tests {
    use super::*;

    fn cfg_test(attr: &str) -> bool {
        let item: syn::ItemMod = syn::parse_str(&format!("{attr} mod m {{}}")).unwrap();
        is_cfg_test(&item.attrs)
    }

    #[test]
    fn test_only_cfgs_are_test_only() {
        assert!(cfg_test("#[cfg(test)]"));
        assert!(cfg_test(r#"#[cfg(all(test, feature = "x"))]"#));
        assert!(cfg_test(r#"#[cfg(all(feature = "x", test))]"#));
        assert!(cfg_test(r#"#[cfg(any(test, all(test, feature = "x")))]"#));
        assert!(cfg_test("#[cfg(not(not(test)))]"));
        assert!(cfg_test("#[doc = \"m\"] #[cfg(test)]"));
    }

    #[test]
    fn cfgs_that_ship_are_not_test_only() {
        assert!(!cfg_test("#[cfg(not(test))]"));
        assert!(!cfg_test(r#"#[cfg(feature = "foo-tests")]"#));
        assert!(!cfg_test(r#"#[cfg(feature = "integration-tests")]"#));
        assert!(!cfg_test(r#"#[cfg(feature = "test")]"#));
        assert!(!cfg_test(r#"#[cfg(any(test, feature = "test-utils"))]"#));
        assert!(!cfg_test("#[cfg(any())]"));
        assert!(!cfg_test("#[cfg(unix)]"));
        assert!(!cfg_test("#[cfg_attr(test, allow(dead_code))]"));
        assert!(!cfg_test(r#"#[doc = "test"]"#));
        assert!(!cfg_test(""));
    }
}

#[cfg(test)]
mod no_fixed_size_array_tests {
    use super::*;

    /// A crate whose every public item exercises one outcome of
    /// `check-no-fixed-size-array`.
    const FIXTURE_LIB: &str = r#"
        pub struct Uuid;
        impl Uuid {
            pub fn with_bytes(bytes: [u8; 16]) -> Self {}
            pub fn with_slice(bytes: &[u8]) -> Self {}
            pub fn to_bytes(&self) -> [u8; 16] {}
            pub fn borrowed(&self) -> &'static [u8; 16] {}
            pub fn nested(&self, values: Option<Vec<[u8; 4]>>) {}
            pub fn matrix(&self) -> [[u8; 4]; 2] {}
            pub fn chunks(&self) -> impl Iterator<Item = [u8; 4]> {}
            pub fn to_vec(&self) -> Vec<u8> {}
            pub(crate) fn internal(&self) -> [u8; 16] {}
            #[cfg(test)]
            pub fn test_only() -> [u8; 2] {}
            fn private(&self) -> [u8; 16] {}
        }
        impl From<[u8; 16]> for Uuid {
            fn from(bytes: [u8; 16]) -> Self {}
        }
        impl std::ops::Index<usize> for Uuid {
            type Output = [u8; 2];
            fn index(&self, i: usize) -> &[u8; 2] {}
        }
        impl Clone for Uuid {
            fn clone(&self) -> Self {}
        }
        pub trait Source {
            fn read(&self, buf: &mut [u8; 8]);
            fn bytes(&self) -> &[u8];
        }
        pub type Key = [u8; 32];
        pub fn pair() -> (i32, [u8; 3]) {}
        pub fn slices(a: &[u8], b: &mut [u8]) -> Vec<u8> {}
        pub(crate) struct Hidden;
        impl From<[u8; 1]> for Hidden {
            fn from(bytes: [u8; 1]) -> Self {}
        }
        mod private {
            pub struct Unreachable;
            impl Unreachable {
                pub fn hidden() -> [u8; 1] {}
            }
        }
        pub mod ffi {
            pub struct Buffer;
            impl Buffer {
                pub fn bytes(&self) -> [u8; 1] {}
            }
            #[unsafe(no_mangle)]
            pub extern "C" fn kafka_common_Uuid_new(bytes: *const [u8; 16]) {}
        }
    "#;

    #[test]
    fn test_fixed_size_arrays_in_public_signatures_are_findings() {
        let dir = std::env::temp_dir().join(format!("xtask-no-fixed-size-array-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("lib.rs"), FIXTURE_LIB).unwrap();
        let krate = Crate::load(&dir.join("lib.rs")).unwrap();
        let mut findings = Vec::new();
        let checked = NoFixedSizeArray.check(&krate, &mut findings);
        fs::remove_dir_all(&dir).unwrap();
        let file = dir.join("lib.rs").display().to_string();
        let mut expected: Vec<String> = [
            "parameter `bytes` of `Uuid::with_bytes` holds the fixed-size array `[u8; 16]`",
            "the return type of `Uuid::to_bytes` holds the fixed-size array `[u8; 16]`",
            "the return type of `Uuid::borrowed` holds the fixed-size array `[u8; 16]`",
            "parameter `values` of `Uuid::nested` holds the fixed-size array `[u8; 4]`",
            "the return type of `Uuid::matrix` holds the fixed-size array `[[u8; 4]; 2]`",
            "the return type of `Uuid::chunks` holds the fixed-size array `[u8; 4]`",
            "the trait of `impl From<[u8; 16]> for Uuid` holds the fixed-size array `[u8; 16]`",
            "an associated type of `impl std::ops::Index<usize> for Uuid` holds the fixed-size array `[u8; 2]`",
            "parameter `buf` of `Source::read` holds the fixed-size array `[u8; 8]`",
            "the aliased type of `Key` holds the fixed-size array `[u8; 32]`",
            "the return type of `pair` holds the fixed-size array `[u8; 3]`",
        ]
        .iter()
        .map(|f| format!("{file}: {f}"))
        .collect();
        expected.sort();
        findings.sort();
        assert_eq!(findings, expected);
        // The public functions and the alias with types (`Uuid`'s eight,
        // `Source`'s two, `Key`, `pair`, `slices`), and the three trait impls
        // for `Uuid`.
        assert_eq!(checked, 16);
    }
}

#[cfg(test)]
mod error_predicate_tests {
    use super::*;

    /// A crate whose `Error` has one variant per outcome of
    /// `check-error-predicate`: two fully wired, then one missing each piece.
    const FIXTURE_LIB: &str = r#"
        pub mod common {
            pub mod error {
                pub enum Error {
                    Timeout(TimeoutError),
                    QuotaViolation(Box<QuotaViolationError>),
                    Orphan(OrphanError),
                    Forgetful(ForgetfulError),
                    Unboxed(UnboxedError),
                    Recursive(RecursiveError),
                    Unexported(UnexportedError),
                }

                pub(crate) trait ErrorHierarchy {
                    fn is_retriable_error(&self) -> bool { false }
                    fn is_timeout_error(&self) -> bool { false }
                    fn is_quota_violation_error(&self) -> bool { false }
                    fn is_forgetful_error(&self) -> bool { false }
                    fn is_unboxed_error(&self) -> bool { false }
                    fn is_recursive_error(&self) -> bool { false }
                    fn is_unexported_error(&self) -> bool { false }
                }

                impl<T: ErrorHierarchy + ?Sized> ErrorHierarchy for Box<T> {
                    fn is_retriable_error(&self) -> bool { (**self).is_retriable_error() }
                    fn is_timeout_error(&self) -> bool { (**self).is_timeout_error() }
                    fn is_quota_violation_error(&self) -> bool { (**self).is_quota_violation_error() }
                    fn is_forgetful_error(&self) -> bool { (**self).is_forgetful_error() }
                    fn is_recursive_error(&self) -> bool { (**self).is_recursive_error() }
                    fn is_unexported_error(&self) -> bool { (**self).is_unexported_error() }
                }

                impl Error {
                    pub fn is_retriable_error(&self) -> bool { ErrorHierarchy::is_retriable_error(self) }
                    pub fn is_timeout_error(&self) -> bool { ErrorHierarchy::is_timeout_error(self) }
                    pub fn is_quota_violation_error(&self) -> bool { ErrorHierarchy::is_quota_violation_error(self) }
                    pub fn is_forgetful_error(&self) -> bool { ErrorHierarchy::is_forgetful_error(self) }
                    pub fn is_unboxed_error(&self) -> bool { ErrorHierarchy::is_unboxed_error(self) }
                    pub fn is_recursive_error(&self) -> bool { self.is_recursive_error() }
                    pub fn is_unexported_error(&self) -> bool { ErrorHierarchy::is_unexported_error(self) }
                }

                kafka_error_type! {
                    /// Wired through its macro.
                    TimeoutError,
                    code: RequestTimedOut,
                    extends: [is_retriable_error, is_timeout_error,],
                }
                kafka_error_type! { OrphanError, code: Orphan, extends: [is_retriable_error,], }
                kafka_error_type! { ForgetfulError, code: Forgetful, extends: [is_retriable_error,], }
                kafka_error_type! { UnboxedError, code: Unboxed, extends: [is_unboxed_error,], }
                kafka_error_type! { RecursiveError, code: Recursive, extends: [is_recursive_error,], }
                kafka_error_type! { UnexportedError, code: Unexported, extends: [is_unexported_error,], }
            }
            pub mod metrics {
                /// Wired through a hand-written impl, and boxed in its variant.
                pub struct QuotaViolationError;
                impl crate::common::error::ErrorHierarchy for QuotaViolationError {
                    fn is_quota_violation_error(&self) -> bool { true }
                }
            }
        }
        pub mod ffi {
            pub mod error_predicates {
                #[unsafe(no_mangle)]
                pub unsafe extern "C" fn kafka_common_Error_is_retriable_error() -> bool { false }
                #[unsafe(no_mangle)]
                pub unsafe extern "C" fn kafka_common_Error_is_timeout_error() -> bool { false }
                #[unsafe(no_mangle)]
                pub unsafe extern "C" fn kafka_common_Error_is_quota_violation_error() -> bool { false }
                #[unsafe(no_mangle)]
                pub unsafe extern "C" fn kafka_common_Error_is_forgetful_error() -> bool { false }
                #[unsafe(no_mangle)]
                pub unsafe extern "C" fn kafka_common_Error_is_unboxed_error() -> bool { false }
                #[unsafe(no_mangle)]
                pub unsafe extern "C" fn kafka_common_Error_is_recursive_error() -> bool { false }
                // Not `#[no_mangle]`, so C cannot link it.
                pub unsafe extern "C" fn kafka_common_Error_is_unexported_error() -> bool { false }
            }
        }
    "#;

    /// The findings over the fixture crate, written to its own temp dir `name`
    /// because the tests run in parallel.
    fn run(name: &str) -> Vec<String> {
        let dir = std::env::temp_dir().join(format!("xtask-error-predicate-{name}-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("lib.rs"), FIXTURE_LIB).unwrap();
        let krate = Crate::load(&dir.join("lib.rs")).unwrap();
        let mut findings = Vec::new();
        let checked = ErrorPredicate.check(&krate, &mut findings);
        fs::remove_dir_all(&dir).unwrap();
        // 7 variants and 7 trait methods.
        assert_eq!(checked, 14, "{findings:#?}");
        findings
    }

    fn with<'a>(findings: &'a [String], needle: &str) -> Vec<&'a str> {
        findings.iter().filter(|f| f.contains(needle)).map(String::as_str).collect()
    }

    #[test]
    fn test_class_predicate_name_drops_a_trailing_error() {
        assert_eq!(class_predicate("TopicExistsError"), "is_topic_exists_error");
        assert_eq!(class_predicate("KafkaError"), "is_kafka_error");
        assert_eq!(
            class_predicate("InvalidRegularExpression"),
            "is_invalid_regular_expression_error"
        );
        assert_eq!(class_predicate("Error"), "is_error_error");
    }

    #[test]
    fn test_wired_predicates_pass() {
        let findings = run("pass");
        for wired in [
            "Timeout",
            "timeout_error",
            "retriable_error",
            "QuotaViolation",
            "quota_violation_error",
        ] {
            assert!(with(&findings, wired).is_empty(), "{wired}: {findings:#?}");
        }
    }

    #[test]
    fn test_each_missing_piece_is_reported() {
        let findings = run("violations");
        let fixture = |what: &str| format!("lib.rs: {what}");
        // Variants in declaration order, then trait methods in name order.
        let expected = [
            fixture("`Error::Orphan` has no class predicate: `ErrorHierarchy` declares no `is_orphan_error`"),
            fixture("`OrphanError` does not answer `true` to its own `is_orphan_error`"),
            fixture("`ForgetfulError` does not answer `true` to its own `is_forgetful_error`"),
            fixture(
                "`Error` has no `pub fn is_recursive_error(&self) -> bool` forwarding to \
                 `ErrorHierarchy::is_recursive_error(self)`",
            ),
            fixture("`impl ErrorHierarchy for Box<T>` does not forward `is_unboxed_error`"),
            fixture("`is_unexported_error` has no C export `kafka_common_Error_is_unexported_error` in `ffi`"),
        ];
        assert_eq!(findings.len(), expected.len(), "{findings:#?}");
        for (finding, expected) in findings.iter().zip(&expected) {
            assert!(
                finding.ends_with(expected.as_str()),
                "{finding}\n  does not end with\n{expected}"
            );
        }
    }
}

#[cfg(test)]
mod no_data_carrying_enum_variants_tests {
    use super::*;

    /// A crate whose public enums exercise every outcome of
    /// `check-no-data-carrying-enum-variants`: the newtype variants of
    /// `Wrapped` each delegate to a type the crate owns, those of `Leaky` do
    /// not or carry several fields, `Value` is a `rust-only` value wrapper,
    /// and `Reexported` is reached only through a `pub use`.
    const FIXTURE_LIB: &str = r#"
        pub struct Payload { level: i32 }
        pub trait Provider {}
        message_only_error! { QuotaViolationError code: 1 }
        pub enum Wrapped<T> {
            Unit,
            Struct(Payload),
            Boxed(Box<Payload>),
            Shared(std::sync::Arc<Payload>),
            Interface(Box<dyn Provider + Send>),
            Macro(Box<QuotaViolationError>),
            Param(T),
        }
        pub enum Leaky {
            Ids(Vec<u8>),
            Stamp(i64),
            Tuple((u8, u8)),
            Pair(u8, u8),
            Named { x: u8 },
        }
        #[doc(alias = "rust-only")]
        pub enum Value {
            Double(f64),
            Pair(u8, u8),
        }
        mod reexported {
            pub enum Reexported { Raw(i64) }
        }
        pub use reexported::Reexported;
        pub(crate) enum CratePrivate { A(i64) }
        mod hidden {
            pub enum Unreachable { A(i64) }
        }
    "#;

    #[test]
    fn test_newtype_must_wrap_a_type_the_crate_defines() {
        let dir = std::env::temp_dir().join(format!("xtask-no-data-carrying-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("lib.rs"), FIXTURE_LIB).unwrap();
        let krate = Crate::load(&dir.join("lib.rs")).unwrap();
        let mut findings = Vec::new();
        let checked = NoDataCarryingEnumVariants.check(&krate, &mut findings);
        fs::remove_dir_all(&dir).unwrap();
        let file = dir.join("lib.rs").display().to_string();
        findings.sort();
        let mut expected = [
            format!("{file}: `Leaky::Ids` wraps `Vec<u8>`, a type the crate does not define"),
            format!("{file}: `Leaky::Stamp` wraps `i64`, a type the crate does not define"),
            format!("{file}: `Leaky::Tuple` wraps `(u8, u8)`, a type the crate does not define"),
            format!("{file}: `Leaky::Pair` holds data inline (2 unnamed fields)"),
            format!("{file}: `Leaky::Named` holds data inline (inline named fields)"),
            // A `rust-only` value wrapper may wrap `f64`, but not hold fields inline.
            format!("{file}: `Value::Pair` holds data inline (2 unnamed fields)"),
            // Reached through `pub use` from a private module.
            format!("{file}: `Reexported::Raw` wraps `i64`, a type the crate does not define"),
        ];
        expected.sort();
        assert_eq!(findings, expected);
        // The fifteen variants of the four reachable `pub` enums; the
        // `pub(crate)` enum and the one under a private module nothing
        // re-exports are out of scope.
        assert_eq!(checked, 15);
    }
}

#[cfg(test)]
mod enum_translation_tests {
    use super::*;

    /// A crate whose every public enum exercises one outcome of
    /// `check-enum-translation`.
    const FIXTURE_LIB: &str = r#"
        #[doc(alias = "org.apache.kafka.clients.admin.DescribeTopicsResult")]
        pub enum DescribeTopicsResult { ByTopicId(u8), ByTopicName(u8) }
        #[doc(alias = "org.apache.kafka.clients.admin.TopicRecord")]
        pub enum TopicRecord { A(u8) }
        #[doc(alias = "org.apache.kafka.clients.admin.OffsetSpec")]
        pub enum OffsetSpec { Earliest, Latest }
        #[doc(alias = "org.apache.kafka.common.TopicCollection")]
        pub enum TopicCollection { Ids(u8) }
        #[doc(alias = "org.apache.kafka.common.metrics.MetricValueProvider")]
        pub enum MetricValueProvider { Gauge(u8) }
        #[doc(alias = "org.apache.kafka.clients.admin.AlterConfigOp$OpType")]
        pub enum OpType { Set }
        #[doc(alias = "org.apache.kafka.clients.admin.Unknown")]
        pub enum Unresolved { A }
        #[doc(alias = "org.apache.kafka.clients.admin.DescribeTopicsResult#topicIdValues")]
        pub enum MemberMarker { A }
        #[doc(alias = "org.apache.kafka.clients.admin.DescribeTopicsResult")]
        pub(crate) enum CratePrivate { A }
        mod private {
            #[doc(alias = "org.apache.kafka.clients.admin.DescribeTopicsResult")]
            pub enum Unreachable { A }
        }
    "#;

    fn class(
        package: &str,
        path: &[&str],
        kind: java::JavaKind,
        is_abstract: bool,
        supertypes: &[&str],
    ) -> java::JavaClass {
        java::JavaClass {
            module: package.split('.').filter(|s| *s != "clients").map(str::to_string).collect(),
            package: package.to_string(),
            path: path.iter().map(|s| s.to_string()).collect(),
            methods: BTreeSet::new(),
            overloads: Vec::new(),
            fields: BTreeMap::new(),
            field_visibility: BTreeMap::new(),
            enum_constants: Vec::new(),
            deprecated: false,
            is_test: false,
            kind,
            visibility: java::Visibility::Public,
            is_abstract,
            functional_interface: false,
            supertypes: supertypes.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn test_enum_translating_a_plain_class_or_record_is_a_finding() {
        use java::JavaKind::{Class, Enum, Interface, Record};
        let dir = std::env::temp_dir().join(format!("xtask-enum-translation-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("lib.rs"), FIXTURE_LIB).unwrap();
        let krate = Crate::load(&dir.join("lib.rs")).unwrap();
        let rule = EnumTranslation {
            index: JavaIndex {
                classes: vec![
                    class("clients.admin", &["DescribeTopicsResult"], Class, false, &[]),
                    class("clients.admin", &["TopicRecord"], Record, false, &[]),
                    class("clients.admin", &["OffsetSpec"], Class, false, &[]),
                    class("clients.admin", &["OffsetSpec", "EarliestSpec"], Class, false, &["OffsetSpec"]),
                    class("common", &["TopicCollection"], Class, true, &[]),
                    class("common.metrics", &["MetricValueProvider"], Interface, false, &[]),
                    class("clients.admin", &["AlterConfigOp", "OpType"], Enum, false, &[]),
                    // A subclass in the test tree does not make a hierarchy.
                    class("clients.admin", &["TestResult"], Class, false, &["DescribeTopicsResult"]),
                ]
                .into_iter()
                .map(|mut c| {
                    c.is_test = c.path == ["TestResult"];
                    c
                })
                .collect(),
            },
        };
        let mut findings = Vec::new();
        let checked = rule.check(&krate, &mut findings);
        fs::remove_dir_all(&dir).unwrap();
        let file = dir.join("lib.rs").display().to_string();
        assert_eq!(
            findings,
            [
                format!(
                    "{file}: `DescribeTopicsResult` is an enum but translates \
                     `org.apache.kafka.clients.admin.DescribeTopicsResult`, a class that is neither a Java enum nor \
                     extended by any class"
                ),
                format!(
                    "{file}: `TopicRecord` is an enum but translates `org.apache.kafka.clients.admin.TopicRecord`, a \
                     record that is neither a Java enum nor extended by any class"
                ),
            ]
        );
        // The six public enums with a class marker the index resolves.
        assert_eq!(checked, 6);
    }
}

#[cfg(test)]
mod functional_interface_trait_tests {
    use super::*;

    /// A crate exercising every outcome of `check-functional-interface-trait`.
    const FIXTURE_LIB: &str = r#"
        pub mod producer {
            #[doc(alias = "org.apache.kafka.clients.producer.Callback")]
            pub type Callback = Box<dyn FnOnce() + Send>;
            #[doc(alias = "org.apache.kafka.clients.producer.KafkaProducer")]
            pub struct KafkaProducer;
            impl KafkaProducer {
                #[doc(alias = "org.apache.kafka.clients.producer.KafkaProducer#send")]
                pub fn send(&self, callback: Callback) {}
            }
        }
        pub mod consumer {
            #[doc(alias = "org.apache.kafka.clients.consumer.OffsetCommitCallback")]
            pub trait OffsetCommitCallback {}
            pub fn new(function: fn()) -> impl OffsetCommitCallback {}
        }
        pub mod common {
            #[doc(alias = "org.apache.kafka.common.KafkaFuture")]
            pub struct KafkaFuture;
            impl KafkaFuture {
                #[doc(alias = "org.apache.kafka.common.KafkaFuture#thenApply")]
                pub fn then_apply<F: Fn(u8) -> u8>(&self, function: F) {}
                #[doc(alias = "org.apache.kafka.common.KafkaFuture#whenComplete(long)")]
                pub fn when_complete_with_timeout(&self, timeout: i64) {}
                #[doc(alias = "org.apache.kafka.common.KafkaFuture#hide")]
                pub fn hide<F: Fn()>(&self, hidden: F) {}
            }
            #[doc(alias = "org.apache.kafka.common.Plain")]
            pub type Plain = fn();
            pub mod metrics {
                mod metric_value_provider {
                    #[doc(alias = "org.apache.kafka.common.metrics.MetricValueProvider")]
                    pub enum MetricValueProvider { A }
                }
            }
            pub mod utils {
                #[doc(alias = "org.apache.kafka.common.utils.Utils$SwallowAction")]
                pub type SwallowAction = fn();
            }
        }
        mod ffi {
            #[doc(alias = "org.apache.kafka.common.metrics.Gauge")]
            pub struct kafka_common_metrics_Gauge_t;
        }
    "#;

    fn overload(name: &str, params: &[&str]) -> java::Overload {
        java::Overload {
            name: name.to_string(),
            params: params.iter().map(|s| s.to_string()).collect(),
            deprecated: false,
            visibility: java::Visibility::Public,
            is_static: false,
            is_default: false,
            param_names: params.iter().map(|_| "p".to_string()).collect(),
            returns_class: false,
        }
    }

    fn class(package: &str, path: &[&str], functional: bool, overloads: Vec<java::Overload>) -> java::JavaClass {
        java::JavaClass {
            module: package.split('.').filter(|s| *s != "clients").map(str::to_string).collect(),
            package: package.to_string(),
            path: path.iter().map(|s| s.to_string()).collect(),
            methods: overloads.iter().map(|o| o.name.clone()).collect(),
            overloads,
            fields: BTreeMap::new(),
            field_visibility: BTreeMap::new(),
            enum_constants: Vec::new(),
            deprecated: false,
            is_test: false,
            kind: if functional {
                java::JavaKind::Interface
            } else {
                java::JavaKind::Class
            },
            visibility: java::Visibility::Public,
            is_abstract: false,
            functional_interface: functional,
            supertypes: Vec::new(),
        }
    }

    #[test]
    fn test_public_functional_interface_not_translated_as_a_trait_is_a_finding() {
        let dir = std::env::temp_dir().join(format!("xtask-functional-interface-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("lib.rs"), FIXTURE_LIB).unwrap();
        let krate = Crate::load(&dir.join("lib.rs")).unwrap();
        let mut hidden = class("common", &["KafkaFuture", "Hidden"], true, Vec::new());
        hidden.visibility = java::Visibility::Package;
        let mut plain = class("common", &["Plain"], false, Vec::new());
        plain.kind = java::JavaKind::Interface;
        let rule = FunctionalInterfaceTrait {
            index: JavaIndex {
                classes: vec![
                    class("clients.producer", &["Callback"], true, Vec::new()),
                    class(
                        "clients.producer",
                        &["KafkaProducer"],
                        false,
                        vec![overload("send", &["ProducerRecord", "Callback"])],
                    ),
                    class("clients.consumer", &["OffsetCommitCallback"], true, Vec::new()),
                    class(
                        "common",
                        &["KafkaFuture"],
                        false,
                        vec![
                            overload("thenApply", &["BaseFunction"]),
                            overload("whenComplete", &["BiConsumer"]),
                            overload("whenComplete", &["long"]),
                            overload("hide", &["Hidden"]),
                        ],
                    ),
                    class("common", &["KafkaFuture", "BaseFunction"], true, Vec::new()),
                    // Taken only by the overload no Rust method translates.
                    class("common", &["KafkaFuture", "BiConsumer"], true, Vec::new()),
                    // Not `public`.
                    hidden,
                    // Not a functional interface.
                    plain,
                    class("common.metrics", &["MetricValueProvider"], true, Vec::new()),
                    // Translated by the C FFI module only, and taken by nothing.
                    class("common.metrics", &["Gauge"], true, Vec::new()),
                    // `Utils` is not on the Public list.
                    class("common.utils", &["Utils", "SwallowAction"], true, Vec::new()),
                ],
            },
            public: [
                "clients.producer.Callback",
                "clients.producer.KafkaProducer",
                "clients.consumer.OffsetCommitCallback",
                "common.KafkaFuture",
                "common.Plain",
                "common.metrics.MetricValueProvider",
                "common.metrics.Gauge",
            ]
            .iter()
            .map(|c| format!("{}{c}", java::MARKER_PREFIX))
            .collect(),
            unsupported: BTreeSet::new(),
        };
        let mut findings = Vec::new();
        let checked = rule.check(&krate, &mut findings);
        fs::remove_dir_all(&dir).unwrap();
        let file = dir.join("lib.rs").display().to_string();
        assert_eq!(
            findings,
            [
                format!(
                    "{file}: `Callback` translates `clients.producer.Callback`, a public `@FunctionalInterface`, as a \
                     type alias; it must be a trait"
                ),
                format!(
                    "{file}: `KafkaFuture::then_apply` translates `common.KafkaFuture#thenApply`, which takes \
                     `common.KafkaFuture$BaseFunction`, a public `@FunctionalInterface` that no trait translates"
                ),
                format!(
                    "{file}: `MetricValueProvider` translates `common.metrics.MetricValueProvider`, a public \
                     `@FunctionalInterface`, as an enum; it must be a trait"
                ),
            ]
        );
        // Callback, OffsetCommitCallback, BaseFunction, BiConsumer, MetricValueProvider and Gauge.
        assert_eq!(checked, 6);
    }

    /// Traits for `check-functional-interface-trait`'s factory requirement.
    const FACTORY_FIXTURE_LIB: &str = r#"
        pub mod producer {
            #[doc(alias = "org.apache.kafka.clients.producer.Callback")]
            pub trait Callback {}
        }
        pub mod consumer {
            #[doc(alias = "org.apache.kafka.clients.consumer.OffsetCommitCallback")]
            pub trait OffsetCommitCallback {}
            pub fn new<F: Fn()>(function: F) -> impl OffsetCommitCallback {}
        }
        pub mod common {
            #[doc(alias = "org.apache.kafka.common.KafkaFuture")]
            pub struct KafkaFuture;
            #[doc(alias = "org.apache.kafka.common.KafkaFuture$BaseFunction")]
            pub trait BaseFunction<A, B> {}
            #[expect(non_snake_case)]
            pub fn BaseFunction_new<F>(function: F) -> impl BaseFunction<u8, u8> {}
            #[doc(alias = "org.apache.kafka.common.KafkaFuture$BiConsumer")]
            pub trait BiConsumer<A, B> {}
            pub fn new<F>(function: F) -> impl BiConsumer<u8, u8> {}
            pub mod serialization {
                #[doc(alias = "org.apache.kafka.common.serialization.Deserializer")]
                pub trait Deserializer<T> {}
                pub fn new<F>(function: F) -> impl Deserializer<u8> {}
                fn with_headers<F>(function: F) -> impl Deserializer<u8> {}
            }
            pub mod metrics {
                pub mod metric_value_provider {
                    #[doc(alias = "org.apache.kafka.common.metrics.MetricValueProvider")]
                    pub trait MetricValueProvider {}
                    pub fn new<F>(function: F) -> impl MetricValueProvider {}
                }
                pub mod gauge {
                    #[doc(alias = "org.apache.kafka.common.metrics.Gauge")]
                    pub trait Gauge {}
                    pub fn new<F>(function: F) -> Box<dyn Gauge> {}
                }
            }
        }
    "#;

    fn method(name: &str, params: &[(&str, &str)], is_default: bool) -> java::Overload {
        java::Overload {
            params: params.iter().map(|(t, _)| t.to_string()).collect(),
            param_names: params.iter().map(|(_, n)| n.to_string()).collect(),
            is_default,
            ..overload(name, &[])
        }
    }

    #[test]
    fn test_functional_interface_trait_without_its_factories_is_a_finding() {
        let dir = std::env::temp_dir().join(format!("xtask-functional-factory-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("lib.rs"), FACTORY_FIXTURE_LIB).unwrap();
        let krate = Crate::load(&dir.join("lib.rs")).unwrap();
        let mut static_deserialize = method("deserialize", &[("String", "topic")], false);
        static_deserialize.is_static = true;
        let mut gauge = class("common.metrics", &["Gauge"], true, Vec::new());
        gauge.supertypes = vec!["MetricValueProvider".to_string()];
        let rule = FunctionalInterfaceTrait {
            index: JavaIndex {
                classes: vec![
                    class(
                        "clients.producer",
                        &["Callback"],
                        true,
                        vec![method(
                            "onCompletion",
                            &[("RecordMetadata", "metadata"), ("Exception", "exception")],
                            false,
                        )],
                    ),
                    class(
                        "clients.consumer",
                        &["OffsetCommitCallback"],
                        true,
                        vec![method(
                            "onComplete",
                            &[("Map", "offsets"), ("Exception", "exception")],
                            false,
                        )],
                    ),
                    class("common", &["KafkaFuture"], false, Vec::new()),
                    class(
                        "common",
                        &["KafkaFuture", "BaseFunction"],
                        true,
                        vec![method("apply", &[("A", "a")], false)],
                    ),
                    class(
                        "common",
                        &["KafkaFuture", "BiConsumer"],
                        true,
                        vec![method("accept", &[("A", "a"), ("B", "b")], false)],
                    ),
                    class(
                        "common.serialization",
                        &["Deserializer"],
                        true,
                        vec![
                            method("deserialize", &[("String", "topic"), ("byte[]", "data")], false),
                            method(
                                "deserialize",
                                &[("String", "topic"), ("Headers", "headers"), ("byte[]", "data")],
                                true,
                            ),
                            // The same added parameter: one factory serves both.
                            method(
                                "deserialize",
                                &[("String", "topic"), ("Headers", "headers"), ("ByteBuffer", "data")],
                                true,
                            ),
                            // A static or a default helper is no function shape.
                            static_deserialize,
                            method("close", &[], true),
                        ],
                    ),
                    class(
                        "common.metrics",
                        &["MetricValueProvider"],
                        true,
                        vec![method("value", &[("MetricConfig", "config"), ("long", "now")], false)],
                    ),
                    gauge,
                ],
            },
            public: [
                "clients.producer.Callback",
                "clients.consumer.OffsetCommitCallback",
                "common.KafkaFuture",
                "common.serialization.Deserializer",
                "common.metrics.MetricValueProvider",
                "common.metrics.Gauge",
            ]
            .iter()
            .map(|c| format!("{}{c}", java::MARKER_PREFIX))
            .collect(),
            unsupported: BTreeSet::new(),
        };
        let mut findings = Vec::new();
        let checked = rule.check(&krate, &mut findings);
        fs::remove_dir_all(&dir).unwrap();
        let file = dir.join("lib.rs").display().to_string();
        assert_eq!(
            findings,
            [
                format!(
                    "{file}: `Callback` translates `clients.producer.Callback`, a public `@FunctionalInterface`, but \
                     its module has no `pub fn new(function) -> impl Callback` for \
                     `clients.producer.Callback#onCompletion(RecordMetadata,Exception)`"
                ),
                // A plain `new` does not serve a nested interface.
                format!(
                    "{file}: `BiConsumer` translates `common.KafkaFuture$BiConsumer`, a public \
                     `@FunctionalInterface`, but its module has no `pub fn BiConsumer_new(function) -> impl \
                     BiConsumer` for `common.KafkaFuture$BiConsumer#accept(A,B)`"
                ),
                // A private factory does not count.
                format!(
                    "{file}: `Deserializer` translates `common.serialization.Deserializer`, a public \
                     `@FunctionalInterface`, but its module has no `pub fn with_headers(function) -> impl \
                     Deserializer` for `common.serialization.Deserializer#deserialize(String,Headers,byte[])`"
                ),
                // Inherited from `MetricValueProvider`; a boxed return is not `impl Gauge`.
                format!(
                    "{file}: `Gauge` translates `common.metrics.Gauge`, a public `@FunctionalInterface`, but its \
                     module has no `pub fn new(function) -> impl Gauge` for \
                     `common.metrics.MetricValueProvider#value(MetricConfig,long)`"
                ),
            ]
        );
        assert_eq!(checked, 7);
    }
}

#[cfg(test)]
mod consumer_receiver_tests {
    use super::*;

    /// The findings over `lib`, written to its own temp dir `name` because the
    /// tests run in parallel, and the number of methods checked.
    fn run(name: &str, lib: &str) -> (usize, Vec<String>) {
        let dir = std::env::temp_dir().join(format!("xtask-consumer-receiver-{name}-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("lib.rs"), lib).unwrap();
        let krate = Crate::load(&dir.join("lib.rs")).unwrap();
        let mut findings = Vec::new();
        let checked = ConsumerReceiver.check(&krate, &mut findings);
        fs::remove_dir_all(&dir).unwrap();
        (checked, findings)
    }

    #[test]
    fn test_mut_receivers_and_the_three_shared_methods_pass() {
        let (checked, findings) = run(
            "pass",
            r#"
            pub trait Consumer {
                fn assignment(&mut self) -> Vec<i32>;
                fn poll(&mut self) -> impl std::future::Future<Output = ()> + Send;
                fn wakeup(&self);
                fn metrics(&self) -> Vec<i32>;
                fn handle(&self) -> i32;
            }
            pub trait DynConsumer {
                fn assignment(&mut self) -> Vec<i32>;
                fn wakeup(&self);
            }
            "#,
        );
        assert!(findings.is_empty(), "{findings:#?}");
        assert_eq!(checked, 7);
    }

    #[test]
    fn test_shared_receiver_on_a_guarded_method_is_reported() {
        let (_, findings) = run(
            "shared",
            r#"
            pub trait Consumer {
                fn assignment(&self) -> Vec<i32>;
                fn current_lag(&self, partition: i32) -> Option<i64>;
                fn wakeup(&self);
            }
            pub trait DynConsumer {
                fn paused(&self) -> Vec<i32>;
                fn wakeup(&mut self);
                fn build() -> Self where Self: Sized;
            }
            "#,
        );
        let expected = [
            "`Consumer::assignment` takes `&self`; it must take `&mut self`",
            "`Consumer::current_lag` takes `&self`; it must take `&mut self`",
            "`DynConsumer::paused` takes `&self`; it must take `&mut self`",
            "`DynConsumer::wakeup` takes `&mut self`; it must take `&self`",
            "`DynConsumer::build` takes `no receiver`; it must take `&mut self`",
        ];
        for e in expected {
            assert!(findings.iter().any(|f| f.contains(e)), "missing `{e}` in {findings:#?}");
        }
        assert_eq!(findings.len(), expected.len(), "{findings:#?}");
    }

    #[test]
    fn test_missing_or_private_trait_is_reported() {
        let (_, findings) = run(
            "missing",
            r#"
            pub trait Consumer {
                fn assignment(&mut self) -> Vec<i32>;
            }
            pub(crate) trait DynConsumer {
                fn paused(&self) -> Vec<i32>;
            }
            "#,
        );
        assert_eq!(
            findings,
            vec!["no public trait `DynConsumer`; the rule checks its method receivers".to_string()]
        );
    }
}
