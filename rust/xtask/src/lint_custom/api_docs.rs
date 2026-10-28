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

//! `check-api-docs`: the public API documentation lists every parameter, the
//! return value and the Javadoc's errors, stays within 150% of the Javadoc,
//! and changes no contract without an accepted entry (CLAUDE.md §6,
//! `.claude/rules/documentation.md`).
//!
//! The items checked are the Rust items of the published API and every C
//! export (`#[unsafe(no_mangle)]`). An item finds its Javadoc through its Java
//! marker, a C export through the Rust method it translates (the mapping
//! `check-ffi-translation` derives). The finding kinds:
//!
//!   - `missing`: the item has no doc;
//!   - `param`: a parameter is not listed under `# Arguments` / `# Parameters`;
//!   - `returns`: a value is returned, with no `# Returns` and no summary
//!     starting "Returns";
//!   - `errors`: a Javadoc `@throws` not named under `# Errors` / `# Panics`,
//!     or a `Result` fn or fallible C export (`out_err`, a `_cb` twin) with
//!     no `# Errors`;
//!   - `size`: the doc exceeds 150% of the Javadoc, counting only what has a
//!     Java counterpart: the description, `# Translation notes`, and the
//!     parameter, return and error entries Java has a tag for;
//!   - `layout`: the sections are out of order, a notes section where Java has
//!     no Javadoc, a `WARNING:` outside the notes, translation boilerplate
//!     (any case) outside the notes or on an item with no Javadoc;
//!   - `warning`: a `WARNING:` line with no accepted entry in
//!     [`CONTRACT_CHANGES`];
//!   - `contract-stale`, `contract-format`: an accepted entry whose item is
//!     gone or has no `WARNING:` any more, a malformed entry;
//!   - `java-line`: a comment in `src/` or `tests/` citing a Java file and
//!     line, one finding per file.
//!
//! **Baseline.** [`DOC_BASELINE`] lists the finding keys still to burn down;
//! a line matching no finding is itself a finding. `cargo xtask doc-baseline`
//! rewrites it; `cargo xtask lint-custom check-api-docs --no-baseline` shows
//! everything.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use super::baseline::{Baseline, Waivable};
use super::ffi_translation::{COrigin, FfiTranslation};
use super::{compact, error_macro_type_def, is_cfg_test, is_no_mangle, java_markers, type_ident, Context, Crate, Rule};
use crate::java::{self, squash, JavaIndex};
use crate::javadoc::{self, doc_parts, doc_text, word_count, Throw, Translation, Translator, NOTES_HEADING};

/// The burn-down baseline, relative to `rust/`.
pub(super) const DOC_BASELINE: &str = "xtask/doc-baseline.txt";

/// The accepted contract changes, relative to `rust/`.
pub(super) const CONTRACT_CHANGES: &str = "../design/current/doc-contract-changes.md";

/// The directories whose comments `java-line` scans, relative to `rust/`.
const JAVA_LINE_DIRS: &[&str] = &["src", "tests"];

/// The sub-items of a contract-change entry, in order (§6).
const ENTRY_FIELDS: [&str; 4] = ["Java member", "Contract change", "Why", "Accepted"];

/// The translation boilerplate allowed only in `# Translation notes` (§2).
const BOILERPLATE: &[&str] = &[
    "Translated from",
    "Corresponds to",
    "Mirrors Java",
    "CLAUDE.md",
    "DoD #",
];

/// The `@throws` classes a Rust doc need not name: Rust has no null to pass.
const UNDOCUMENTED_THROWS: &[&str] = &["NullPointerException"];

/// The headings a parameter list may sit under: `# Arguments` in Rust,
/// `# Parameters` in the C header.
const PARAM_HEADINGS: &[&str] = &["Arguments", "Parameters"];

// ---------------------------------------------------------------------------
// Findings
// ---------------------------------------------------------------------------

/// One defect; `key` is what the baseline lists.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Finding {
    pub(super) kind: &'static str,
    /// The Rust path (`crate::…`), the C symbol, the file of a `java-line`,
    /// or the title of a contract entry.
    pub(super) item: String,
    /// The file to fix.
    pub(super) file: String,
    pub(super) detail: String,
}

impl Finding {
    pub(super) fn key(&self) -> String {
        format!("{} {}", self.kind, self.item)
    }

    fn message(&self) -> String {
        format!("{}: {} `{}`: {}", self.file, self.kind, self.item, self.detail)
    }
}

impl Waivable for Finding {
    fn key(&self) -> String {
        Finding::key(self)
    }

    fn subject(&self) -> &str {
        &self.item
    }

    fn baseline_finding(file: &str, line: String, detail: &str) -> Self {
        Finding { kind: "baseline", item: line, file: file.to_string(), detail: detail.to_string() }
    }
}

// ---------------------------------------------------------------------------
// Contract changes
// ---------------------------------------------------------------------------

/// One entry of [`CONTRACT_CHANGES`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ContractEntry {
    /// The Rust path or C symbol, without backticks.
    pub(super) title: String,
    /// The sub-items, by name, in file order.
    pub(super) fields: Vec<(String, String)>,
}

impl ContractEntry {
    /// Whether the user filled `Accepted`.
    pub(super) fn is_accepted(&self) -> bool {
        self.fields
            .iter()
            .any(|(name, value)| name == "Accepted" && !value.trim().is_empty())
    }
}

/// The entries after `## Entries` of the contract-changes file `text`, and a
/// `contract-format` finding per malformed one. Fenced blocks are skipped.
pub(super) fn parse_contract_changes(file: &str, text: &str) -> (Vec<ContractEntry>, Vec<Finding>) {
    let mut entries: Vec<ContractEntry> = Vec::new();
    let mut findings = Vec::new();
    let mut lines_of: Vec<usize> = Vec::new();
    let mut in_entries = false;
    let mut fence = false;
    for (n, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.starts_with("```") {
            fence = !fence;
            continue;
        }
        if fence {
            continue;
        }
        if line.starts_with("## ") {
            in_entries = trimmed == "## Entries";
            continue;
        }
        if !in_entries || trimmed.is_empty() {
            continue;
        }
        let number_len = line.chars().take_while(char::is_ascii_digit).count();
        if number_len > 0 && line[number_len..].starts_with(". ") {
            let title = line[number_len + 2..].trim().trim_matches('`').to_string();
            entries.push(ContractEntry { title, fields: Vec::new() });
            lines_of.push(n + 1);
            continue;
        }
        let Some(entry) = entries.last_mut() else {
            findings.push(Finding {
                kind: "contract-format",
                item: format!("line {}", n + 1),
                file: file.to_string(),
                detail: "text before the first entry; an entry starts `N. `<item>``".to_string(),
            });
            continue;
        };
        let sub_item = trimmed.strip_prefix("- ").and_then(|rest| rest.split_once(':'));
        match sub_item {
            Some((name, value)) if line.starts_with(char::is_whitespace) => {
                entry.fields.push((name.trim().to_string(), value.trim().to_string()));
            },
            _ => match entry.fields.last_mut() {
                // A continuation line of the previous sub-item.
                Some((_, value)) => {
                    value.push(' ');
                    value.push_str(trimmed);
                },
                None => findings.push(Finding {
                    kind: "contract-format",
                    item: entry.title.clone(),
                    file: file.to_string(),
                    detail: format!("line {}: text before the first sub-item", n + 1),
                }),
            },
        }
    }
    let mut titles = BTreeSet::new();
    for (entry, line) in entries.iter().zip(&lines_of) {
        let names: Vec<&str> = entry.fields.iter().map(|(n, _)| n.as_str()).collect();
        let mut problems = Vec::new();
        for name in &names {
            if !ENTRY_FIELDS.contains(name) {
                problems.push(format!("unknown sub-item `{name}`"));
            }
        }
        for name in ENTRY_FIELDS {
            if !names.contains(&name) {
                problems.push(format!("missing sub-item `{name}`"));
            }
        }
        if problems.is_empty() && names != ENTRY_FIELDS {
            problems.push(format!("the sub-items must be, in order: {}", ENTRY_FIELDS.join(", ")));
        }
        if entry.title.is_empty() {
            problems.push("no title".to_string());
        }
        if !titles.insert(entry.title.clone()) {
            problems.push("the title is listed twice".to_string());
        }
        if !problems.is_empty() {
            findings.push(Finding {
                kind: "contract-format",
                item: entry.title.clone(),
                file: file.to_string(),
                detail: format!("entry at line {line}: {}", problems.join("; ")),
            });
        }
    }
    (entries, findings)
}

// ---------------------------------------------------------------------------
// Items
// ---------------------------------------------------------------------------

/// An item of the public API documentation (documentation.md §1).
pub(super) struct DocItem {
    /// `crate::module::Type::member`, or the C symbol.
    pub(super) key: String,
    pub(super) file: String,
    pub(super) attrs: Vec<syn::Attribute>,
    pub(super) markers: Vec<String>,
    /// The signature of a fn.
    pub(super) sig: Option<syn::Signature>,
    /// An `#[unsafe(no_mangle)]` C export.
    pub(super) c_export: bool,
    /// For a C export, the Rust method it translates (`Type::method`), when
    /// `check-ffi-translation` derives one.
    pub(super) origin: Option<String>,
}

/// The Rust path of `name` in module `module`, whose items live in `file`,
/// under type `owner` if any. A file module (`producer_record.rs`) is left
/// out when it is the type's own (`ProducerRecord`), as the re-export does; a
/// directory module (`admin/mod.rs` for `Admin`) is kept.
pub(super) fn item_path(module: &[String], file: &Path, owner: Option<&str>, name: &str) -> String {
    let mut segs: Vec<String> = vec!["crate".to_string()];
    segs.extend(module.iter().cloned());
    let ty = owner.unwrap_or(name);
    let file_module = file.file_stem().and_then(|s| s.to_str()) == module.last().map(String::as_str);
    if file_module
        && segs
            .last()
            .is_some_and(|last| squash(last) == squash(ty.trim_start_matches('<').split(' ').next().unwrap_or(ty)))
    {
        segs.pop();
    }
    if let Some(owner) = owner {
        segs.push(owner.to_string());
    }
    segs.push(name.to_string());
    segs.join("::")
}

/// Whether `attrs` hold `#[doc(hidden)]`.
fn is_doc_hidden(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|a| {
        a.path().is_ident("doc")
            && matches!(&a.meta, syn::Meta::List(list) if list.tokens.to_string().split(',').any(|t| t.trim() == "hidden"))
    })
}

/// The Rust items of the published API: the public types and the members of
/// public traits, of inherent impls of public types and of public enums, the
/// public fns, consts, statics and aliases of reachable modules, the error
/// types, and the trait-impl methods of public types that have a doc of their
/// own (documentation.md §1). C exports are left to [`c_items`].
pub(super) fn rust_items(krate: &Crate) -> Vec<DocItem> {
    let types = krate.public_types();
    let crate_traits: BTreeSet<String> = krate.traits().map(|(_, t)| t.ident.to_string()).collect();
    let public_traits: BTreeSet<String> = krate
        .traits()
        .filter(|(path, t)| types.contains(&((*path).clone(), t.ident.to_string())))
        .map(|(_, t)| t.ident.to_string())
        .collect();
    let is_pub = |vis: &syn::Visibility| matches!(vis, syn::Visibility::Public(_));
    let mut out = Vec::new();
    for (module, m) in &krate.modules {
        let file = m.file.display().to_string();
        let reachable = krate.is_reachable(module);
        let is_public_type = |name: &str| types.contains(&(module.clone(), name.to_string()));
        let mut push = |owner: Option<&str>, name: &str, attrs: &[syn::Attribute], sig: Option<&syn::Signature>| {
            if is_doc_hidden(attrs) || (sig.is_some() && is_cfg_test(attrs)) {
                return;
            }
            out.push(DocItem {
                key: item_path(module, &m.file, owner, name),
                file: file.clone(),
                attrs: attrs.to_vec(),
                markers: java_markers(attrs),
                sig: sig.cloned(),
                c_export: false,
                origin: None,
            });
        };
        for item in &m.items {
            match item {
                syn::Item::Struct(s) if is_public_type(&s.ident.to_string()) => {
                    push(None, &s.ident.to_string(), &s.attrs, None)
                },
                syn::Item::Enum(e) if is_public_type(&e.ident.to_string()) => {
                    let name = e.ident.to_string();
                    push(None, &name, &e.attrs, None);
                    if !is_doc_hidden(&e.attrs) {
                        for v in &e.variants {
                            push(Some(&name), &v.ident.to_string(), &v.attrs, None);
                        }
                    }
                },
                syn::Item::Trait(t) if is_public_type(&t.ident.to_string()) => {
                    let name = t.ident.to_string();
                    push(None, &name, &t.attrs, None);
                    if is_doc_hidden(&t.attrs) {
                        continue;
                    }
                    for it in &t.items {
                        match it {
                            syn::TraitItem::Fn(f) => {
                                push(Some(&name), &f.sig.ident.to_string(), &f.attrs, Some(&f.sig))
                            },
                            syn::TraitItem::Const(c) => push(Some(&name), &c.ident.to_string(), &c.attrs, None),
                            _ => {},
                        }
                    }
                },
                syn::Item::Type(t) if reachable && is_pub(&t.vis) => push(None, &t.ident.to_string(), &t.attrs, None),
                syn::Item::Const(c) if reachable && is_pub(&c.vis) => push(None, &c.ident.to_string(), &c.attrs, None),
                syn::Item::Static(s) if reachable && is_pub(&s.vis) => push(None, &s.ident.to_string(), &s.attrs, None),
                syn::Item::Fn(f) if reachable && is_pub(&f.vis) && !is_no_mangle(&f.attrs) => {
                    push(None, &f.sig.ident.to_string(), &f.attrs, Some(&f.sig))
                },
                syn::Item::Impl(i) => {
                    let Some(ty) = type_ident(&i.self_ty) else { continue };
                    if !is_public_type(&ty) || is_doc_hidden(&i.attrs) {
                        continue;
                    }
                    match &i.trait_ {
                        None => {
                            for it in &i.items {
                                match it {
                                    syn::ImplItem::Fn(f) if is_pub(&f.vis) => {
                                        push(Some(&ty), &f.sig.ident.to_string(), &f.attrs, Some(&f.sig))
                                    },
                                    syn::ImplItem::Const(c) if is_pub(&c.vis) => {
                                        push(Some(&ty), &c.ident.to_string(), &c.attrs, None)
                                    },
                                    _ => {},
                                }
                            }
                        },
                        Some((_, path, _)) => {
                            let tr = path.segments.last().map(|s| s.ident.to_string()).unwrap_or_default();
                            // A crate-private trait's methods are not in the API.
                            if crate_traits.contains(&tr) && !public_traits.contains(&tr) {
                                continue;
                            }
                            let owner = format!("<{ty} as {tr}>");
                            for it in &i.items {
                                // Without a doc of its own, rustdoc shows the
                                // trait method's.
                                if let syn::ImplItem::Fn(f) = it {
                                    if doc_text(&f.attrs).is_some() {
                                        push(Some(&owner), &f.sig.ident.to_string(), &f.attrs, Some(&f.sig));
                                    }
                                }
                            }
                        },
                    }
                },
                syn::Item::Macro(mac) => {
                    // Every error type is public, a variant of `Error`.
                    if let Some(def) = error_macro_type_def(mac) {
                        push(None, &def.name, &def.attrs, None);
                    }
                },
                _ => {},
            }
        }
    }
    out
}

/// Every C export of the crate, with the markers of the Rust method it
/// translates.
pub(super) fn c_items(krate: &Crate, origins: &BTreeMap<String, COrigin>) -> Vec<DocItem> {
    let mut out = Vec::new();
    for m in krate.modules.values() {
        for item in &m.items {
            let syn::Item::Fn(f) = item else { continue };
            if !is_no_mangle(&f.attrs) || is_cfg_test(&f.attrs) || is_doc_hidden(&f.attrs) {
                continue;
            }
            let symbol = f.sig.ident.to_string();
            let mut markers = java_markers(&f.attrs);
            let origin = origins.get(&symbol);
            if let Some(origin) = origin {
                for marker in &origin.markers {
                    if !markers.contains(marker) {
                        markers.push(marker.clone());
                    }
                }
            }
            out.push(DocItem {
                key: symbol,
                file: m.file.display().to_string(),
                attrs: f.attrs.clone(),
                markers,
                sig: Some(f.sig.clone()),
                c_export: true,
                origin: origin.map(|o| o.rust.clone()),
            });
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Signatures and sections
// ---------------------------------------------------------------------------

/// The parameter names `sig` must document: every plain-name parameter but
/// `self`, and for a C export (`c_export`) also `self_` and a trailing
/// `out_err` (documentation.md §1). A C export's names are the header's, which
/// cbindgen writes without Rust's raw-identifier prefix (`r#type` -> `type`).
pub(super) fn documented_params(sig: &syn::Signature, c_export: bool) -> Vec<String> {
    use syn::ext::IdentExt;
    let mut names: Vec<String> = Vec::new();
    for input in &sig.inputs {
        let syn::FnArg::Typed(pt) = input else { continue };
        let syn::Pat::Ident(p) = &*pt.pat else { continue };
        names.push(if c_export {
            p.ident.unraw().to_string()
        } else {
            p.ident.to_string()
        });
    }
    if c_export {
        names.retain(|n| n != "self_");
        if names.last().is_some_and(|n| n == "out_err") {
            names.pop();
        }
    }
    names
}

/// The names of `sig`'s plain-name parameters.
fn param_names(sig: &syn::Signature) -> Vec<String> {
    sig.inputs
        .iter()
        .filter_map(|input| match input {
            syn::FnArg::Typed(pt) => match &*pt.pat {
                syn::Pat::Ident(p) => Some(p.ident.to_string()),
                _ => None,
            },
            syn::FnArg::Receiver(_) => None,
        })
        .collect()
}

/// Whether C export `sig` is a `_cb` twin: named `<fn>_cb` and taking the
/// completion `cb` (CLAUDE.md §4).
pub(super) fn is_cb_twin(sig: &syn::Signature) -> bool {
    sig.ident.to_string().ends_with("_cb") && param_names(sig).iter().any(|n| n == "cb")
}

/// Whether C export `sig` is fallible (CLAUDE.md §4): it takes the trailing
/// `out_err`, or it is a `_cb` twin, whose completion delivers the error.
pub(super) fn is_fallible_c(sig: &syn::Signature) -> bool {
    param_names(sig).last().is_some_and(|n| n == "out_err") || is_cb_twin(sig)
}

/// The last path segment of `ty`, if it is a path.
fn last_segment(ty: &syn::Type) -> Option<&syn::PathSegment> {
    match ty {
        syn::Type::Path(p) => p.path.segments.last(),
        _ => None,
    }
}

/// The type arguments of a path segment.
fn type_args(seg: &syn::PathSegment) -> Vec<&syn::Type> {
    match &seg.arguments {
        syn::PathArguments::AngleBracketed(a) => a
            .args
            .iter()
            .filter_map(|g| match g {
                syn::GenericArgument::Type(t) => Some(t),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// `T` for a future of `T`: `impl Future<Output = T>`, `dyn Future<Output =
/// T>`, either behind `Pin` / `Box`, and the crate's `InlineFuture<'_, T>`.
fn future_output(ty: &syn::Type) -> Option<&syn::Type> {
    let bounds = match ty {
        syn::Type::Paren(p) => return future_output(&p.elem),
        syn::Type::Group(g) => return future_output(&g.elem),
        syn::Type::ImplTrait(t) => &t.bounds,
        syn::Type::TraitObject(t) => &t.bounds,
        syn::Type::Path(_) => {
            let seg = last_segment(ty)?;
            let args = type_args(seg);
            return match seg.ident.to_string().as_str() {
                "Pin" | "Box" => args.first().and_then(|inner| future_output(inner)),
                "InlineFuture" | "BoxFuture" => args.first().copied(),
                _ => None,
            };
        },
        _ => return None,
    };
    bounds.iter().find_map(|b| {
        let syn::TypeParamBound::Trait(t) = b else { return None };
        let seg = t.path.segments.last().filter(|s| s.ident == "Future")?;
        let syn::PathArguments::AngleBracketed(a) = &seg.arguments else {
            return None;
        };
        a.args.iter().find_map(|g| match g {
            syn::GenericArgument::AssocType(at) if at.ident == "Output" => Some(&at.ty),
            _ => None,
        })
    })
}

/// The type `sig` returns, a future's output for an `async fn` or a fn
/// returning a future; `None` for `()`.
pub(super) fn return_type(sig: &syn::Signature) -> Option<syn::Type> {
    let syn::ReturnType::Type(_, ty) = &sig.output else {
        return None;
    };
    let mut ty: &syn::Type = ty;
    if sig.asyncness.is_none() {
        if let Some(out) = future_output(ty) {
            ty = out;
        }
    }
    while let syn::Type::Paren(p) = ty {
        ty = &p.elem;
    }
    match ty {
        syn::Type::Tuple(t) if t.elems.is_empty() => None,
        _ => Some(ty.clone()),
    }
}

/// Whether `ty` is a `Result`.
pub(super) fn is_result(ty: &syn::Type) -> bool {
    last_segment(ty).is_some_and(|s| s.ident == "Result")
}

/// Whether returning `ty` needs no `# Returns`: `!`, or `Result<(), _>`.
fn returns_nothing(ty: &syn::Type) -> bool {
    match ty {
        syn::Type::Never(_) => true,
        _ if is_result(ty) => last_segment(ty)
            .and_then(|s| type_args(s).first().copied())
            .is_some_and(|ok| matches!(ok, syn::Type::Tuple(t) if t.elems.is_empty())),
        _ => false,
    }
}

/// Whether `item`, returning `ty`, is a constructor, which needs no `# Returns`
/// as a Java constructor has no `@return` (documentation.md §4): its marker
/// names a Java constructor (the member named like its class), or it takes no
/// `self` and returns `Self` or its owner type, directly or in a `Result`.
fn is_constructor(item: &DocItem, ty: &syn::Type) -> bool {
    let java_ctor = item.markers.iter().any(|m| {
        let Some((class, member)) = m.split_once('#') else {
            return false;
        };
        let class = class.rsplit(['.', '$']).next().unwrap_or(class);
        member.split('(').next() == Some(class)
    });
    if java_ctor {
        return true;
    }
    let has_receiver = item.sig.as_ref().is_some_and(|s| s.receiver().is_some());
    if has_receiver {
        return false;
    }
    let mut ty = ty;
    if is_result(ty) {
        match last_segment(ty).and_then(|s| type_args(s).first().copied()) {
            Some(ok) => ty = ok,
            None => return false,
        }
    }
    let Some(name) = last_segment(ty).map(|s| s.ident.to_string()) else {
        return false;
    };
    // The owner is the segment before the item's name, when it is a type.
    let owner = item.key.rsplit("::").nth(1).unwrap_or_default();
    name == "Self" || (!item.c_export && owner.starts_with(|c: char| c.is_ascii_uppercase()) && name == owner)
}

/// The sections of the original part of a doc: the text before the first
/// heading under `""`, then each heading's text. Fenced blocks stay in the
/// section they open in.
pub(super) fn sections(original: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = vec![(String::new(), String::new())];
    let mut fence = false;
    for line in original.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("```") {
            fence = !fence;
        }
        let hashes = trimmed.len() - trimmed.trim_start_matches('#').len();
        if !fence && hashes > 0 && trimmed[hashes..].starts_with(' ') {
            out.push((trimmed[hashes..].trim().to_string(), String::new()));
            continue;
        }
        if let Some((_, text)) = out.last_mut() {
            text.push_str(line);
            text.push('\n');
        }
    }
    out
}

/// The list entries of a section's `text`: each `- ` line with the lines
/// that continue it, and before them any text that is not an entry.
fn entries(text: &str) -> (String, Vec<String>) {
    let mut lead = String::new();
    let mut out: Vec<String> = Vec::new();
    for line in text.lines() {
        if line.trim_start().starts_with("- ") || line.trim_start().starts_with("* ") {
            out.push(line.to_string());
        } else if let Some(last) = out.last_mut().filter(|_| line.starts_with(' ') || line.is_empty()) {
            last.push('\n');
            last.push_str(line);
        } else {
            lead.push_str(line);
            lead.push('\n');
        }
    }
    (lead, out)
}

/// The words of a doc that count toward its size against Javadoc `java`
/// (documentation.md §5): the description and every section but the ones
/// below, `# Translation notes`, each `# Arguments` / `# Parameters` entry of
/// a parameter Java has an `@param` for, `# Returns` when Java has an
/// `@return`, and each `# Errors` / `# Panics` entry naming an error of a
/// Java `@throws`. Text Java has no tag for (a Rust-only or C parameter, a
/// `# Returns` or error Java does not document) and `# Safety` are not.
pub(super) fn size_words(parts: &javadoc::DocParts, java: &Translation) -> usize {
    let java_params: BTreeSet<&str> = java.params.iter().map(|(n, _)| n.trim_start_matches('_')).collect();
    let names_throw = |entry: &str| {
        java.throws.iter().any(|t| {
            t.names.iter().any(|n| {
                has_word(entry, n)
                    || n.strip_suffix("Error").is_some_and(|v| entry.contains(&format!("Error::{v}")))
                    || names_predicate(entry, &predicate_of(n))
            })
        })
    };
    let mut words = parts.notes.as_deref().map_or(0, word_count);
    for (heading, text) in sections(&parts.original) {
        let h = heading.as_str();
        if PARAM_HEADINGS.contains(&h) {
            for entry in entries(&text).1 {
                let name = entry.split('`').nth(1).unwrap_or_default().trim_start_matches('_');
                if java_params.contains(name) {
                    words += word_count(&entry);
                }
            }
        } else if h == "Returns" {
            if java.returns.is_some() {
                words += word_count(&text);
            }
        } else if h == "Errors" || h == "Panics" {
            words += entries(&text)
                .1
                .iter()
                .filter(|e| names_throw(e))
                .map(|e| word_count(e))
                .sum::<usize>();
        } else {
            words += word_count(&text);
        }
    }
    words
}

/// The text of every section of `sections` under one of `headings`.
fn section_text(sections: &[(String, String)], headings: &[&str]) -> Option<String> {
    let found: Vec<&str> = sections
        .iter()
        .filter(|(h, _)| headings.contains(&h.as_str()))
        .map(|(_, t)| t.as_str())
        .collect();
    (!found.is_empty()).then(|| found.join("\n"))
}

/// Whether `text` holds `word` as a whole identifier.
fn has_word(text: &str, word: &str) -> bool {
    let is_ident = |c: char| c.is_alphanumeric() || c == '_';
    text.match_indices(word).any(|(at, _)| {
        !text[..at].chars().next_back().is_some_and(is_ident)
            && !text[at + word.len()..].chars().next().is_some_and(is_ident)
    })
}

/// The references of a Javadoc that only points elsewhere: no text but `@see`
/// tags (`@see KafkaConsumer#commitSync()`), or a body that is only "See" and
/// `{@link}`s (`See {@link KafkaProducer#close()}`), with no tags but `@see`
/// and `@since`. Empty for any other Javadoc.
fn pointer_refs(doc: &str) -> Vec<String> {
    let parsed = javadoc::parse(doc);
    if !parsed.tags.iter().all(|t| t.name == "see" || t.name == "since") {
        return Vec::new();
    }
    let mut refs: Vec<String> = Vec::new();
    let mut rest = String::new();
    let mut body = parsed.body.as_str();
    while let Some(start) = body.find("{@link") {
        rest.push_str(&body[..start]);
        let Some(len) = body[start..].find('}') else {
            return Vec::new();
        };
        let inner = &body[start + 1..start + len];
        let target = inner.split_whitespace().nth(1).unwrap_or_default();
        refs.push(target.to_string());
        body = &body[start + len + 1..];
    }
    rest.push_str(body);
    if rest
        .split(|c: char| !c.is_alphanumeric())
        .any(|w| !w.is_empty() && !w.eq_ignore_ascii_case("see"))
    {
        return Vec::new();
    }
    refs.extend(parsed.tags.iter().filter(|t| t.name == "see").map(|t| {
        // `@see Foo#bar() label`: the reference is the first word, the
        // parameter list possibly spaced.
        let text = t.text.as_str();
        match text.find(')') {
            Some(end) if text[..end].contains('(') => text[..=end].to_string(),
            _ => text.split_whitespace().next().unwrap_or_default().to_string(),
        }
    }));
    refs
}

/// The `Error` predicate of error type `name` (CLAUDE.md §12.4):
/// `TimeoutError` -> `is_timeout_error`.
pub(super) fn predicate_of(name: &str) -> String {
    format!("is_{}", java::snake_case(name))
}

/// Whether `text` names predicate `predicate` (`is_timeout_error`), alone,
/// as `Error::is_timeout_error` or inside its C symbol
/// (`kafka_common_Error_is_timeout_error`).
fn names_predicate(text: &str, predicate: &str) -> bool {
    text.match_indices(predicate).any(|(at, _)| {
        !text[..at].chars().next_back().is_some_and(char::is_alphanumeric)
            && !text[at + predicate.len()..]
                .chars()
                .next()
                .is_some_and(|c| c.is_alphanumeric() || c == '_')
    })
}

/// The `is_*` predicates of `Error`: the methods of the crate's
/// `ErrorHierarchy` trait (CLAUDE.md §12.4).
pub(super) fn error_predicates(krate: &Crate) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for module in krate.modules.values() {
        for item in &module.items {
            let syn::Item::Trait(t) = item else { continue };
            if t.ident != "ErrorHierarchy" {
                continue;
            }
            for member in &t.items {
                if let syn::TraitItem::Fn(f) = member {
                    let name = f.sig.ident.to_string();
                    if name.starts_with("is_") {
                        out.insert(name);
                    }
                }
            }
        }
    }
    out
}

/// Whether `text` names error type `name` (`TimeoutError`) in a panic
/// description: as the type, as its `Error` variant (`Error::Timeout`), or by
/// its predicate (`is_timeout_error`, also inside a C symbol).
fn names_error(text: &str, name: &str) -> bool {
    let variant = name.strip_suffix("Error").filter(|v| !v.is_empty()).unwrap_or(name);
    has_word(text, name)
        || has_word(text, &format!("Error::{variant}"))
        || text.contains(&format!("is_{}", java::snake_case(name)))
}

/// Whether parameter `name` is listed in a parameter section `text`, as
/// `` `name` ``, with or without a leading `_`.
/// The `WARNING:` paragraphs of `notes`: each from its `WARNING:` line to the
/// next blank line, joined.
fn warning_paragraphs(notes: &str) -> String {
    let mut out = String::new();
    let mut inside = false;
    for line in notes.lines() {
        let line = line.trim();
        if line.starts_with("WARNING:") {
            inside = true;
        } else if line.is_empty() {
            inside = false;
        }
        if inside {
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

fn lists_param(text: &str, name: &str) -> bool {
    let bare = name.trim_start_matches('_');
    text.contains(&format!("`{bare}`")) || text.contains(&format!("`_{bare}`")) || text.contains(&format!("`{name}`"))
}

// ---------------------------------------------------------------------------
// java-line
// ---------------------------------------------------------------------------

/// The comments of Rust source `src`, each with its first line (1-based).
/// String, byte-string, raw-string and character literals are skipped.
pub(super) fn comments(src: &str) -> Vec<(usize, String)> {
    let chars: Vec<char> = src.chars().collect();
    let mut out = Vec::new();
    let mut line = 1usize;
    let mut i = 0usize;
    let ident = |c: char| c.is_alphanumeric() || c == '_';
    while i < chars.len() {
        let c = chars[i];
        let next = chars.get(i + 1).copied();
        if c == '\n' {
            line += 1;
            i += 1;
        } else if c == '/' && next == Some('/') {
            let start = i;
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            out.push((line, chars[start..i].iter().collect()));
        } else if c == '/' && next == Some('*') {
            let (start, first) = (i, line);
            let mut depth = 0usize;
            while i < chars.len() {
                if chars[i] == '/' && chars.get(i + 1) == Some(&'*') {
                    depth += 1;
                    i += 2;
                } else if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
                    depth -= 1;
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    if chars[i] == '\n' {
                        line += 1;
                    }
                    i += 1;
                }
            }
            out.push((first, chars[start..i.min(chars.len())].iter().collect()));
        } else if (c == 'r' || (c == 'b' && next == Some('r'))) && (i == 0 || !ident(chars[i - 1])) && {
            let mut j = i + if c == 'b' { 2 } else { 1 };
            while chars.get(j) == Some(&'#') {
                j += 1;
            }
            chars.get(j) == Some(&'"')
        } {
            // A raw string: `r#"…"#`, closed by `"` and as many `#`s.
            let mut j = i + if c == 'b' { 2 } else { 1 };
            let mut hashes = 0;
            while chars[j] == '#' {
                hashes += 1;
                j += 1;
            }
            j += 1;
            loop {
                match chars.get(j) {
                    None => break,
                    Some('"') if (1..=hashes).all(|k| chars.get(j + k) == Some(&'#')) => {
                        j += 1 + hashes;
                        break;
                    },
                    Some('\n') => {
                        line += 1;
                        j += 1;
                    },
                    Some(_) => j += 1,
                }
            }
            i = j;
        } else if c == '"' {
            i += 1;
            while i < chars.len() && chars[i] != '"' {
                if chars[i] == '\\' {
                    i += 1;
                }
                if chars.get(i) == Some(&'\n') {
                    line += 1;
                }
                i += 1;
            }
            i += 1;
        } else if c == '\'' {
            // A character literal (`'x'`, `'\n'`, `'\u{1F600}'`), else a lifetime.
            if next == Some('\\') {
                let mut j = i + 2;
                while j < chars.len() && chars[j] != '\'' {
                    j += 1;
                }
                i = j + 1;
            } else if chars.get(i + 2) == Some(&'\'') {
                i += 3;
            } else {
                i += 1;
            }
        } else {
            i += 1;
        }
    }
    out
}

/// The Java file:line references in `text`, a comment with no preceding
/// comment line: see [`java_line_refs_run`].
#[cfg(test)]
pub(super) fn java_line_refs(text: &str) -> Vec<String> {
    java_line_refs_run(&[text]).into_iter().map(|(_, r)| r).collect()
}

/// The Java file:line references starting in comment `text`, `context`
/// being the comment line before it in the same run: see
/// [`java_line_refs_run`].
#[cfg(test)]
pub(super) fn java_line_refs_in(text: &str, context: &str) -> Vec<String> {
    java_line_refs_run(&[context, text])
        .into_iter()
        .filter(|(line, _)| *line == 1)
        .map(|(_, r)| r)
        .collect()
}

/// What a candidate reference needs from its comment to count.
#[derive(Clone, Copy, PartialEq, Eq)]
enum JavaLineHit {
    /// Nothing: the form names a Java file or the word `Java` itself.
    Always,
    /// `line N`: a Java name nearby and no Rust name on its lines.
    LineWord,
    /// `at N`: a Java file or class in the run and no Rust name on its lines.
    At,
}

/// The Java file:line references in `lines`, one run of consecutive comment
/// lines, as (index of the line the reference starts on, reference). The
/// lines are joined, their comment markers dropped, so a reference that wraps
/// onto the next line counts:
///
/// - `Foo.java:12`, `Foo.java:12-34`, `` `Foo.java` 12-34 ``, and the short
///   `` `:142` ``;
/// - `Java 73`, `Java 1067-1072`, `Java 967/972`, `Java line 140-142`: a
///   number after the word `Java` (not a version: `Java 4.4`, `Java 11's`);
/// - `line 73`, `lines 62-67`, when its line or the one above names Java (the
///   word `Java`, a `.java` file, or a Java-style name in a code span, such as
///   `` `MockProducerTest.testAutoCompleteMock` ``) and its lines name no Rust
///   file (`.rs`) and do not say `Rust`;
/// - `at 2373-2375`, a range, and `` `assertNotNull(md)` at 727 ``, a number
///   right after a Java name in a code span, when the run names a Java file or
///   class (a `.java` file or a Java-style name in a code span) and its lines
///   name no Rust file and do not say `Rust`.
pub(super) fn java_line_refs_run(lines: &[&str]) -> Vec<(usize, String)> {
    let mut joined = String::new();
    let mut starts = Vec::with_capacity(lines.len());
    for line in lines {
        if !starts.is_empty() {
            joined.push(' ');
        }
        starts.push(joined.len());
        joined.push_str(comment_body(line));
    }
    let line_of = |at: usize| starts.partition_point(|&start| start <= at) - 1;
    // Lines `first..=last`, without the separator after the last.
    let span = |first: usize, last: usize| &joined[starts[first]..starts.get(last + 1).map_or(joined.len(), |s| s - 1)];
    let rust = |text: &str| text.contains(".rs") || !word_starts(text, "Rust").is_empty();
    let java_class = names_java_file_or_class(&joined);
    java_line_hits(&joined)
        .into_iter()
        .filter(|&(start, end, kind, _)| {
            let (first, last) = (line_of(start), line_of(end.saturating_sub(1).max(start)));
            match kind {
                JavaLineHit::Always => true,
                JavaLineHit::LineWord => !rust(span(first, last)) && names_java(span(first.saturating_sub(1), last)),
                JavaLineHit::At => !rust(span(first, last)) && java_class,
            }
        })
        .map(|(start, _, _, reference)| (line_of(start), reference))
        .collect()
}

/// `line` without its comment marker (`//`, `///`, `//!`, `/*`, `/**`, or the
/// `*` of a block comment's inner line) and the blanks around it.
fn comment_body(line: &str) -> &str {
    let line = line.trim_start();
    ["///", "//!", "//", "/**", "/*!", "/*", "*"]
        .iter()
        .find_map(|marker| line.strip_prefix(marker))
        .unwrap_or(line)
        .trim()
}

/// The candidate references in `text`: (start, end, what it needs, reference).
fn java_line_hits(text: &str) -> Vec<(usize, usize, JavaLineHit, String)> {
    let mut out = Vec::new();
    for (at, _) in text.match_indices(".java") {
        let before = &text[..at];
        let name_len = before.chars().rev().take_while(|c| c.is_alphanumeric() || *c == '_').count();
        if name_len == 0 {
            continue;
        }
        let start = before.len() - before.chars().rev().take(name_len).map(char::len_utf8).sum::<usize>();
        let after = &text[at + ".java".len()..];
        if let Some(after) = after.strip_prefix(':') {
            let digits = after.chars().take_while(char::is_ascii_digit).count();
            if digits > 0 {
                let end = at + ".java:".len() + digits;
                out.push((start, end, JavaLineHit::Always, text[start..end].to_string()));
            }
            continue;
        }
        // `Foo.java 12-34`, `` `Foo.java` 12-34 ``.
        let spaced = after.strip_prefix('`').unwrap_or(after);
        let Some(rest) = spaced.strip_prefix(char::is_whitespace) else {
            continue;
        };
        let rest = rest.trim_start();
        if let Some(number) = line_number(rest) {
            let end = text.len() - rest.len() + number.len();
            out.push((start, end, JavaLineHit::Always, format!("{}.java {number}", &text[start..at])));
        }
    }
    for (at, _) in text.match_indices("`:") {
        let after = &text[at + 2..];
        let digits = after.chars().take_while(char::is_ascii_digit).count();
        if digits == 0 {
            continue;
        }
        let mut end = digits;
        if after[end..].starts_with('-') {
            let more = after[end + 1..].chars().take_while(char::is_ascii_digit).count();
            if more > 0 {
                end += 1 + more;
            }
        }
        if after[end..].starts_with('`') {
            out.push((at, at + 2 + end + 1, JavaLineHit::Always, format!("`:{}`", &after[..end])));
        }
    }
    for at in word_starts(text, "Java") {
        let rest = &text[at + "Java".len()..];
        let Some(rest) = rest.strip_prefix(char::is_whitespace) else {
            continue;
        };
        let rest = rest.trim_start();
        let rest = skip_line_word(rest).unwrap_or(rest);
        if let Some(number) = line_number(rest) {
            let end = text.len() - rest.len() + number.len();
            out.push((at, end, JavaLineHit::Always, format!("Java {number}")));
        }
    }
    for at in word_starts(text, "line").into_iter().chain(word_starts(text, "lines")) {
        // `Java line 12` is already reported above.
        if text[..at].trim_end().ends_with("Java") {
            continue;
        }
        let Some(rest) = skip_line_word(&text[at..]) else {
            continue;
        };
        if let Some(number) = line_number(rest) {
            let end = text.len() - rest.len() + number.len();
            out.push((at, end, JavaLineHit::LineWord, format!("line {number}")));
        }
    }
    for at in word_starts(text, "at") {
        let rest = &text[at + "at".len()..];
        let Some(rest) = rest.strip_prefix(char::is_whitespace) else {
            continue;
        };
        let rest = rest.trim_start();
        let Some(number) = line_number(rest) else {
            continue;
        };
        // A bare `at N` is mostly a value (`at 0`, `bounded at 50 ms`), so
        // only a range or a number right after a Java name in a code span
        // (`` `assertNotNull(md)` at 727 ``) counts.
        let range = number.contains(['-', '–', '/']);
        let before = text[..at].trim_end();
        let after_java_span = before
            .strip_suffix('`')
            .and_then(|inner| inner.rfind('`'))
            .is_some_and(|open| names_java_file_or_class(&before[open..]));
        if range || after_java_span {
            let end = text.len() - rest.len() + number.len();
            out.push((at, end, JavaLineHit::At, format!("at {number}")));
        }
    }
    out.sort_by_key(|hit| hit.0);
    out
}

/// The byte offsets of `word` in `text` as a whole word.
fn word_starts(text: &str, word: &str) -> Vec<usize> {
    let ident = |c: char| c.is_alphanumeric() || c == '_';
    text.match_indices(word)
        .map(|(at, _)| at)
        .filter(|&at| {
            !text[..at].chars().next_back().is_some_and(ident)
                && !text[at + word.len()..].chars().next().is_some_and(ident)
        })
        .collect()
}

/// `s` after a leading `line` or `lines` and its whitespace.
fn skip_line_word(s: &str) -> Option<&str> {
    let rest = s.strip_prefix("lines").or_else(|| s.strip_prefix("line"))?;
    let trimmed = rest.trim_start();
    (trimmed.len() < rest.len()).then_some(trimmed)
}

/// The line number or range `s` starts with (`12`, `12-34`, `12–34`,
/// `967/972`), unless it is a version (`4.4`, `11's`), part of a word, or `0`,
/// which no line is (`` `lastMetadataUpdateMs` at 0 `` is a value).
fn line_number(s: &str) -> Option<&str> {
    let digits = s.chars().take_while(char::is_ascii_digit).count();
    if digits == 0 || s[..digits].bytes().all(|b| b == b'0') {
        return None;
    }
    let mut end = digits;
    for sep in ['-', '–', '/'] {
        if let Some(after) = s[end..].strip_prefix(sep) {
            let more = after.chars().take_while(char::is_ascii_digit).count();
            if more > 0 {
                end += sep.len_utf8() + more;
            }
            break;
        }
    }
    let rest = &s[end..];
    let version = rest.starts_with('.') && rest[1..].starts_with(|c: char| c.is_ascii_digit());
    let word = rest.starts_with(|c: char| c.is_alphanumeric() || c == '_') || rest.starts_with("'s");
    (!version && !word).then(|| &s[..end])
}

/// Whether `text` names Java: the word `Java`, a `.java` file, or a code span
/// holding a Java-style name: `Class#member`, a lowerCamelCase segment
/// (`MockProducerTest.testAutoCompleteMock`, `toOffsetCommitRequestData`), or
/// a `…Test` class. A span with `_` or `::` is Rust.
fn names_java(text: &str) -> bool {
    !word_starts(text, "Java").is_empty() || names_java_file_or_class(text)
}

/// Whether `text` names a Java file or class: a `.java` file, or a code span
/// holding a Java-style name (see [`names_java`]).
fn names_java_file_or_class(text: &str) -> bool {
    if text.contains(".java") {
        return true;
    }
    text.split('`').skip(1).step_by(2).any(|span| {
        if span.contains('_') || span.contains("::") {
            return false;
        }
        let name: String = span
            .chars()
            .take_while(|c| c.is_alphanumeric() || matches!(c, '.' | '#'))
            .collect();
        name.contains('#')
            || name.ends_with("Test")
            || name.split('.').any(|segment| {
                segment.starts_with(|c: char| c.is_ascii_lowercase()) && segment.chars().any(|c| c.is_ascii_uppercase())
            })
    })
}

/// Every `.rs` file under `dir`, sorted.
fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else { return };
    let mut paths: Vec<PathBuf> = entries.filter_map(|e| e.ok().map(|e| e.path())).collect();
    paths.sort();
    for path in paths {
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

// ---------------------------------------------------------------------------
// Gaps: what `param`, `returns` and `errors` find, for the rule and doc-fill
// ---------------------------------------------------------------------------

/// What a C export needs of the Rust method it translates: whether that
/// method returns a `Result`, and the text of its `# Errors` and `# Panics`.
#[derive(Debug, Default, Clone)]
pub(super) struct OriginDoc {
    pub(super) is_result: bool,
    pub(super) errors: String,
    pub(super) panics: String,
}

impl OriginDoc {
    /// The Rust items of `items` by `Type::method`, the key a C export's
    /// origin carries; the first item wins when two share one.
    pub(super) fn index(items: &[DocItem]) -> BTreeMap<String, OriginDoc> {
        let mut out = BTreeMap::new();
        for item in items.iter().filter(|i| !i.c_export) {
            let Some(sig) = &item.sig else { continue };
            let mut segs = item.key.rsplit("::");
            let (Some(method), Some(ty)) = (segs.next(), segs.next()) else {
                continue;
            };
            let original = doc_text(&item.attrs).map(|d| doc_parts(&d).original).unwrap_or_default();
            let secs = sections(&original);
            out.entry(format!("{ty}::{method}")).or_insert_with(|| OriginDoc {
                is_result: return_type(sig).as_ref().is_some_and(is_result),
                errors: section_text(&secs, &["Errors"]).unwrap_or_default(),
                panics: section_text(&secs, &["Panics"]).unwrap_or_default(),
            });
        }
        out
    }

    /// Whether the Rust method keeps Java's `throw` as a panic (CLAUDE.md
    /// §12.5): it returns no `Result`, or its `# Panics` names the throw (or
    /// describes a JDK throw in prose) while its `# Errors` does not name the
    /// throw's predicate.
    fn keeps_as_panic(&self, throw: &Throw) -> bool {
        if !self.is_result {
            return true;
        }
        if throw.names.iter().any(|n| names_predicate(&self.errors, &predicate_of(n))) {
            return false;
        }
        throw.names.iter().any(|n| names_error(&self.panics, n))
            || (throw.names.iter().all(|n| n.starts_with("Local")) && !self.panics.trim().is_empty())
    }
}

/// A Java `@throws` a fn's doc does not document.
#[derive(Debug, Clone)]
pub(super) struct ThrowGap {
    pub(super) throw: Throw,
    /// Whether the fn reports it as an error (`# Errors`), not a panic.
    pub(super) error: bool,
    /// For an error, the predicate to name it by: the first of its Rust names
    /// whose `is_*` predicate exists on `Error`; `None` when none has one.
    pub(super) predicate: Option<String>,
}

/// What the doc of a fn leaves out (documentation.md §4).
#[derive(Debug, Default)]
pub(super) struct Gaps {
    /// Whether the fn is a C export (`# Parameters`, not `# Arguments`).
    pub(super) c_export: bool,
    /// Each parameter not listed, with Java's `@param` text.
    pub(super) params: Vec<(String, Option<String>)>,
    /// The returned type, when no `# Returns` documents it, with Java's
    /// `@return` text.
    pub(super) returns: Option<(String, Option<String>)>,
    /// Whether the fn is fallible and has no `# Errors`.
    pub(super) result_without_errors: bool,
    /// Whether the fn is fallible: it returns a `Result`, or it is a fallible
    /// C export ([`is_fallible_c`]).
    pub(super) fallible: bool,
    /// Each Java `@throws` not documented.
    pub(super) throws: Vec<ThrowGap>,
}

impl Gaps {
    /// The gaps of fn `item` whose doc has original part `original`, against
    /// its translated Javadocs and the `Error` predicates `predicates`; for a
    /// C export, `origin` is the doc of the Rust method it translates.
    /// `None` for an item that is not a fn.
    pub(super) fn of(
        item: &DocItem,
        original: &str,
        javadocs: &[Translation],
        predicates: &BTreeSet<String>,
        origin: Option<&OriginDoc>,
    ) -> Option<Gaps> {
        let sig = item.sig.as_ref()?;
        let secs = sections(original);
        let params = documented_params(sig, item.c_export);
        let best = ApiDocs::best_overload(javadocs, &params);
        let mut gaps = Gaps { c_export: item.c_export, ..Gaps::default() };

        // param
        let listed = section_text(&secs, PARAM_HEADINGS).unwrap_or_default();
        gaps.params = params
            .iter()
            .filter(|p| !lists_param(&listed, p))
            .map(|p| {
                let java = best.and_then(|t| t.params.iter().find(|(n, _)| n == p.trim_start_matches('_')));
                (p.clone(), java.map(|(_, text)| text.clone()))
            })
            .collect();

        // returns
        let returned = return_type(sig);
        if let Some(ty) = returned.as_ref().filter(|t| !returns_nothing(t) && !is_constructor(item, t)) {
            let summary = secs.first().map(|(_, t)| t.trim_start()).unwrap_or_default();
            if section_text(&secs, &["Returns"]).is_none() && !summary.starts_with("Returns") {
                let java = best.or(javadocs.first()).and_then(|t| t.returns.clone());
                gaps.returns = Some((compact(ty), java));
            }
        }

        // errors
        let errors = section_text(&secs, &["Errors"]).unwrap_or_default();
        let errors_or_panics = section_text(&secs, &["Errors", "Panics"]).unwrap_or_default();
        let has_panics = section_text(&secs, &["Panics"]).is_some_and(|t| !t.trim().is_empty());
        gaps.fallible = returned.as_ref().is_some_and(is_result) || (item.c_export && is_fallible_c(sig));
        gaps.result_without_errors = gaps.fallible && section_text(&secs, &["Errors"]).is_none();
        for throw in best.map(|t| t.throws.as_slice()).unwrap_or_default() {
            if UNDOCUMENTED_THROWS.contains(&throw.java.as_str()) {
                continue;
            }
            // A fallible C export reports a throw as an error, unless the
            // Rust method it translates keeps it as a panic (CLAUDE.md §12.5).
            let error = gaps.fallible && !(item.c_export && origin.is_some_and(|o| o.keeps_as_panic(throw)));
            // A JDK precondition throw kept as a panic is described in
            // `# Panics` in prose (CLAUDE.md §12.5). A C export's is decided
            // by its Rust method, above.
            let jdk_panic =
                throw.names.iter().all(|n| n.starts_with("Local")) && has_panics && !(item.c_export && error);
            if jdk_panic {
                continue;
            }
            if error {
                // A returned error is classified by its predicate (CLAUDE.md
                // §3, §12.4), so `# Errors` names the predicate.
                let existing: Vec<String> = throw
                    .names
                    .iter()
                    .map(|n| predicate_of(n))
                    .filter(|p| predicates.contains(p))
                    .collect();
                if !existing.iter().any(|p| names_predicate(&errors, p)) {
                    gaps.throws
                        .push(ThrowGap { throw: throw.clone(), error, predicate: existing.into_iter().next() });
                }
            } else if !throw.names.iter().any(|n| names_error(&errors_or_panics, n)) {
                gaps.throws.push(ThrowGap { throw: throw.clone(), error, predicate: None });
            }
        }
        Some(gaps)
    }

    /// The heading parameters are listed under.
    pub(super) fn param_heading(&self) -> &'static str {
        if self.c_export {
            "Parameters"
        } else {
            "Arguments"
        }
    }
}

// ---------------------------------------------------------------------------
// The rule
// ---------------------------------------------------------------------------

/// See the module documentation.
pub(super) struct ApiDocs {
    pub(super) java: Rc<JavaIndex>,
    ffi: FfiTranslation,
    baseline: Option<Baseline>,
    /// The contract-changes file.
    pub(super) contracts: PathBuf,
    /// The directory `java-line` file keys are relative to.
    root: PathBuf,
    /// The directories `java-line` scans.
    java_line_dirs: Vec<PathBuf>,
}

impl ApiDocs {
    pub(super) fn new(ctx: &Context) -> Self {
        ApiDocs {
            java: Rc::clone(&ctx.java),
            ffi: FfiTranslation::new(ctx),
            baseline: ctx.doc_baseline.as_deref().map(Baseline::load),
            contracts: PathBuf::from(CONTRACT_CHANGES),
            root: PathBuf::from("."),
            java_line_dirs: JAVA_LINE_DIRS.iter().map(PathBuf::from).collect(),
        }
    }

    /// Every item checked, Rust items first, then C exports.
    pub(super) fn items(&self, krate: &Crate) -> Vec<DocItem> {
        let mut items = rust_items(krate);
        items.extend(c_items(krate, &self.ffi.c_origins(krate)));
        items
    }

    /// The translator the rule and its helpers share.
    pub(super) fn translator(&self) -> Translator<'_> {
        Translator { index: Some(&self.java), panics: false }
    }

    /// The translated Javadocs of `item`'s markers, the empty ones (only
    /// dropped tags, `@since`) left out: the member then counts as having no
    /// Javadoc.
    pub(super) fn javadocs(&self, item: &DocItem) -> Vec<Translation> {
        let translator = self.translator();
        let mut docs: Vec<Translation> = Vec::new();
        for marker in &item.markers {
            for doc in self.followed_javadocs(marker, &mut BTreeSet::new()) {
                let t = translator.translate(&doc);
                if word_count(&t.rustdoc) > 0 && !docs.iter().any(|d| d.rustdoc == t.rustdoc) {
                    docs.push(t);
                }
            }
        }
        docs
    }

    /// The raw Javadocs of `marker`, a Javadoc that only points elsewhere
    /// ([`pointer_refs`]) replaced by the Javadocs it points to
    /// (documentation.md §4).
    fn followed_javadocs(&self, marker: &str, seen: &mut BTreeSet<String>) -> Vec<String> {
        if !seen.insert(marker.to_string()) {
            return Vec::new();
        }
        let mut out = Vec::new();
        for doc in self.java.marker_javadocs(marker).unwrap_or_default() {
            let followed: Vec<String> = pointer_refs(&doc)
                .iter()
                .filter_map(|r| self.see_marker(marker, r))
                .flat_map(|target| self.followed_javadocs(&target, seen))
                .collect();
            if followed.is_empty() {
                out.push(doc);
            } else {
                out.extend(followed);
            }
        }
        out
    }

    /// The marker a reference in the Javadoc of `from` names, when it names a
    /// known class or one of its members: `KafkaConsumer`,
    /// `KafkaConsumer#commitSync(Duration)`, `#commitSync()` (a member of
    /// `from`'s own class). The class is found by simple name, `from`'s
    /// package first; main-tree classes only.
    fn see_marker(&self, from: &str, reference: &str) -> Option<String> {
        let reference = reference.split_whitespace().collect::<String>();
        let (class_name, member) = reference.split_once('#').unwrap_or((&reference, ""));
        let from_class = self.java.resolve(from)?;
        let class = if class_name.is_empty() {
            from_class
        } else {
            let simple = class_name.rsplit('.').next().unwrap_or(class_name);
            let mut candidates: Vec<&java::JavaClass> = self
                .java
                .classes
                .iter()
                .filter(|c| !c.is_test && c.path.last().is_some_and(|n| n == simple))
                .collect();
            candidates.sort_by_key(|c| c.package != from_class.package);
            candidates.first().copied()?
        };
        if member.is_empty() {
            return Some(class.marker());
        }
        let name = member.split('(').next().unwrap_or(member);
        // A signature naming an overload is kept; one that names none (a
        // fully-qualified parameter type) falls back to every overload.
        let member = if member.contains('(') && class.overloads.iter().any(|o| o.signature() == member) {
            member
        } else {
            name
        };
        Some(format!("{}#{member}", class.marker()))
    }

    /// The Javadoc whose `@param` names best match `params` (the first on a
    /// tie), for `param` and `errors` (documentation.md §4).
    pub(super) fn best_overload<'a>(javadocs: &'a [Translation], params: &[String]) -> Option<&'a Translation> {
        let score = |t: &Translation| {
            params
                .iter()
                .filter(|p| t.params.iter().any(|(n, _)| n == p.trim_start_matches('_')))
                .count()
        };
        let best = javadocs.iter().map(score).max()?;
        javadocs.iter().find(|t| score(t) == best)
    }

    /// The entries of the contract-changes file, and its `contract-format`
    /// findings.
    pub(super) fn contract_entries(&self) -> (Vec<ContractEntry>, Vec<Finding>) {
        let contracts_file = self.contracts.display().to_string();
        match fs::read_to_string(&self.contracts) {
            Ok(text) => parse_contract_changes(&contracts_file, &text),
            Err(e) => (
                Vec::new(),
                vec![Finding {
                    kind: "contract-format",
                    item: contracts_file.clone(),
                    file: contracts_file,
                    detail: format!("cannot read the file: {e}"),
                }],
            ),
        }
    }

    /// The findings before the baseline, and how many items were checked.
    pub(super) fn findings(&self, krate: &Crate) -> (Vec<Finding>, usize) {
        let contracts_file = self.contracts.display().to_string();
        let (entries, mut findings) = self.contract_entries();
        let accepted: BTreeSet<&str> = entries.iter().filter(|e| e.is_accepted()).map(|e| e.title.as_str()).collect();

        let items = self.items(krate);
        let predicates = error_predicates(krate);
        let origins = OriginDoc::index(&items);
        let mut warned: BTreeSet<String> = BTreeSet::new();
        let mut seen: BTreeSet<String> = BTreeSet::new();
        for item in &items {
            seen.insert(item.key.clone());
            let doc = doc_text(&item.attrs);
            if doc.as_deref().is_some_and(|d| d.lines().any(is_warning_line)) {
                warned.insert(item.key.clone());
            }
            let origin = item.origin.as_ref().and_then(|o| origins.get(o));
            self.check_item(item, doc.as_deref(), &accepted, &predicates, origin, &mut findings);
        }

        for entry in entries.iter().filter(|e| e.is_accepted()) {
            let problem = if !seen.contains(&entry.title) {
                Some("no item has this path or symbol any more: delete the entry")
            } else if !warned.contains(&entry.title) {
                Some("the item's doc has no `WARNING:` line any more: delete the entry or restore the warning")
            } else {
                None
            };
            if let Some(problem) = problem {
                findings.push(Finding {
                    kind: "contract-stale",
                    item: entry.title.clone(),
                    file: contracts_file.clone(),
                    detail: problem.to_string(),
                });
            }
        }

        findings.extend(self.java_line_findings());
        findings.sort();
        // Two items can share a path (two impl blocks of a generic type).
        findings.dedup_by(|a, b| a.key() == b.key());
        (findings, items.len())
    }

    /// The findings of one item with doc `doc` (documentation.md §4).
    fn check_item(
        &self,
        item: &DocItem,
        doc: Option<&str>,
        accepted: &BTreeSet<&str>,
        predicates: &BTreeSet<String>,
        origin: Option<&OriginDoc>,
        out: &mut Vec<Finding>,
    ) {
        let javadocs = self.javadocs(item);
        let Some(doc) = doc else {
            let detail = if javadocs.is_empty() {
                "the item has no doc: describe its contract".to_string()
            } else {
                format!(
                    "the item has no doc: `cargo xtask javadoc {}` prints Java's, translated",
                    item.key
                )
            };
            out.push(self.finding("missing", item, detail));
            return;
        };
        let parts = doc_parts(doc);
        let has_javadoc = !javadocs.is_empty();

        // warning
        let warnings = parts.warnings_in_notes + parts.warnings_outside_notes;
        if warnings > 0 && !accepted.contains(item.key.as_str()) {
            out.push(self.finding(
                "warning",
                item,
                format!(
                    "{warnings} `WARNING:` line(s) with no accepted entry in {CONTRACT_CHANGES}: add a proposed entry \
                     (empty `Accepted`) and leave this finding in the baseline until the user accepts it"
                ),
            ));
        }

        // layout
        let mut layout: Vec<String> = parts
            .layout
            .iter()
            // With no Javadoc there are no notes for a warning to sit in.
            .filter(|l| has_javadoc || !l.starts_with("a `WARNING:`"))
            .cloned()
            .collect();
        if !has_javadoc && parts.notes.is_some() {
            layout.push(format!(
                "the item has no Javadoc, so no `# {NOTES_HEADING}`: write the remarks as plain doc text"
            ));
        }
        // Boilerplate is allowed only in the notes, so an item with no Javadoc,
        // which has no notes, may not carry it at all. The match ignores case.
        let original = parts.original.to_lowercase();
        for phrase in BOILERPLATE {
            let lower = phrase.to_lowercase();
            if original.contains(&lower) && !javadocs.iter().any(|j| j.rustdoc.to_lowercase().contains(&lower)) {
                layout.push(if has_javadoc {
                    format!("\"{phrase}\" outside `# {NOTES_HEADING}`: the marker records the origin")
                } else {
                    format!(
                        "\"{phrase}\" on an item with no Javadoc, which has no `# {NOTES_HEADING}`: drop it or \
                         state the contract in plain text"
                    )
                });
            }
        }
        if !layout.is_empty() {
            out.push(self.finding("layout", item, layout.join("; ")));
        }

        if let Some(mut gaps) = Gaps::of(item, &parts.original, &javadocs, predicates, origin) {
            // An accepted contract change whose `WARNING:` names a Java
            // `@throws` accounts for it: the item does not keep that throw
            // (documentation.md §4, §6).
            if accepted.contains(item.key.as_str()) {
                let warned = warning_paragraphs(parts.notes.as_deref().unwrap_or_default());
                gaps.throws.retain(|g| !warned.contains(&format!("`{}`", g.throw.java)));
            }
            if !gaps.params.is_empty() {
                let missing: Vec<String> = gaps
                    .params
                    .iter()
                    .map(|(p, java)| match java {
                        Some(text) => format!("`{p}` (Java: \"{text}\")"),
                        None => format!("`{p}`"),
                    })
                    .collect();
                out.push(self.finding(
                    "param",
                    item,
                    format!("not listed under `# {}`: {}", gaps.param_heading(), missing.join(", ")),
                ));
            }
            if let Some((ty, java)) = &gaps.returns {
                out.push(self.finding(
                    "returns",
                    item,
                    match java {
                        Some(text) => format!("returns `{ty}`: add `# Returns` (Java: \"{text}\")"),
                        None => format!("returns `{ty}`: add `# Returns`, or start the summary with \"Returns\""),
                    },
                ));
            }
            let mut problems: Vec<String> = Vec::new();
            if gaps.result_without_errors {
                problems.push(if item.c_export {
                    "a fallible C export (`out_err` or a `_cb` twin) with no `# Errors` section".to_string()
                } else {
                    "returns a `Result` with no `# Errors` section".to_string()
                });
            }
            for ThrowGap { throw, error, predicate } in &gaps.throws {
                problems.push(match predicate {
                    Some(p) => format!(
                        "Java's `@throws {}` is not named by its predicate `{p}` under `# Errors`",
                        throw.java
                    ),
                    None if *error => format!(
                        "Java's `@throws {}` has no predicate on `Error` to name (`{}`)",
                        throw.java,
                        throw.names.iter().map(|n| predicate_of(n)).collect::<Vec<_>>().join("` or `")
                    ),
                    None => format!(
                        "Java's `@throws {}` is not named as `{}`",
                        throw.java,
                        throw.names.join("` or `")
                    ),
                });
            }
            if !problems.is_empty() {
                out.push(self.finding("errors", item, problems.join("; ")));
            }
        }

        // size: against the Javadoc it fits best.
        let best = javadocs
            .iter()
            .map(|t| (size_words(&parts, t), word_count(&t.rustdoc)))
            .min_by_key(|&(words, java)| (words * 2).cast_signed() - (java * 3).cast_signed());
        if let Some((words, java)) = best {
            if words * 2 > java * 3 {
                out.push(self.finding(
                    "size",
                    item,
                    format!(
                        "{words} words, more than 150% of the Javadoc's {java} (at most {})",
                        java * 3 / 2
                    ),
                ));
            }
        }
    }

    fn finding(&self, kind: &'static str, item: &DocItem, detail: String) -> Finding {
        Finding { kind, item: item.key.clone(), file: item.file.clone(), detail }
    }

    /// One `java-line` finding per file citing a Java file and line.
    fn java_line_findings(&self) -> Vec<Finding> {
        let mut files = Vec::new();
        for dir in &self.java_line_dirs {
            rust_files(&self.root.join(dir), &mut files);
        }
        let mut out = Vec::new();
        for path in files {
            let Ok(src) = fs::read_to_string(&path) else { continue };
            let comments = comments(&src);
            // Runs of consecutive comment lines, each line with its number.
            let mut runs: Vec<Vec<(usize, &str)>> = Vec::new();
            for (line, text) in &comments {
                for (offset, physical) in text.split('\n').enumerate() {
                    let number = line + offset;
                    match runs.last_mut() {
                        Some(run) if run.last().is_some_and(|(last, _)| last + 1 == number) => {
                            run.push((number, physical));
                        },
                        _ => runs.push(vec![(number, physical)]),
                    }
                }
            }
            let mut refs: Vec<(usize, String)> = Vec::new();
            for run in &runs {
                let texts: Vec<&str> = run.iter().map(|(_, text)| *text).collect();
                refs.extend(java_line_refs_run(&texts).into_iter().map(|(index, r)| (run[index].0, r)));
            }
            let Some((line, first)) = refs.first() else { continue };
            let rel = path.strip_prefix(&self.root).unwrap_or(&path);
            let rel = rel.strip_prefix(".").unwrap_or(rel).display().to_string();
            out.push(Finding {
                kind: "java-line",
                item: rel.clone(),
                file: rel,
                detail: format!(
                    "{} Java file:line reference(s) in comments, the first `{first}` at line {line}: name the Java \
                     member instead (`org.apache.kafka.…Class#member`)",
                    refs.len()
                ),
            });
        }
        out
    }

    /// `cargo xtask javadoc <query>`: the translated Javadoc of a marker, or
    /// of the item with Rust path or C symbol `query`, with its size limit.
    pub(super) fn explain(&self, krate: &Crate, query: &str) -> anyhow::Result<String> {
        let translator = self.translator();
        if query.starts_with(java::MARKER_PREFIX) {
            if self.java.resolve(query).is_none() {
                anyhow::bail!("no indexed Java class for marker `{query}`");
            }
            let docs = self.followed_javadocs(query, &mut BTreeSet::new());
            if docs.is_empty() {
                return Ok(format!("`{query}` has no Javadoc: the item keeps its own plain doc.\n"));
            }
            let rendered: Vec<String> = docs.iter().map(|d| translator.to_rustdoc(d)).collect();
            return Ok(rendered.join("\n\n----\n\n") + "\n");
        }
        let items = self.items(krate);
        let item = items.iter().find(|i| i.key == query).ok_or_else(|| {
            anyhow::anyhow!("no public item or C export `{query}`; use a `crate::…` path or a C symbol")
        })?;
        let mut out = format!("{} ({})\nmarkers: {}\n\n", item.key, item.file, item.markers.join(", "));
        let docs = self.javadocs(item);
        if docs.is_empty() {
            out.push_str("No Javadoc: the item keeps its own plain doc, with no size limit.\n");
            return Ok(out);
        }
        for d in &docs {
            out.push_str(&d.rustdoc);
            out.push_str("\n\n");
        }
        let parts = doc_parts(&doc_text(&item.attrs).unwrap_or_default());
        let (own, java) = docs
            .iter()
            .map(|t| (size_words(&parts, t), word_count(&t.rustdoc)))
            .min_by_key(|&(words, java)| (words * 2).cast_signed() - (java * 3).cast_signed())
            .unwrap_or_default();
        out.push_str(&format!(
            "size: {own} words counted (entries with no Java tag left out), at most {} (150% of {java})\n",
            java * 3 / 2
        ));
        Ok(out)
    }
}

/// Whether a doc line is a `WARNING:` line (documentation.md §6).
fn is_warning_line(line: &str) -> bool {
    line.trim_start().trim_start_matches(['-', '*', ' ']).starts_with("WARNING:")
}

/// The module `doc-report` groups a finding of `file` under: `src/<x>` by its
/// top-level module (`producer`, `ffi`, `common/requests` split out of
/// `common`), a root file as `root`, `tests/…` as `tests`.
pub(super) fn report_module(file: &str) -> String {
    let file = file.strip_prefix("./").unwrap_or(file);
    if file.starts_with("tests/") {
        return "tests".to_string();
    }
    if file.ends_with(".md") {
        return "contract changes".to_string();
    }
    let Some(rest) = file.strip_prefix("src/") else {
        return file.to_string();
    };
    let segs: Vec<&str> = rest.split('/').collect();
    match segs.as_slice() {
        [_] => "root".to_string(),
        ["common", "requests", ..] => "common/requests".to_string(),
        [first, ..] => (*first).to_string(),
        [] => file.to_string(),
    }
}

impl Rule for ApiDocs {
    fn name(&self) -> &'static str {
        "check-api-docs"
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
        "   A public item's doc carries Java's contract translated, every parameter under
   `# Arguments` (C: `# Parameters`), `# Returns` (or a summary starting \"Returns\"),
   each Javadoc @throws under `# Errors` / `# Panics` (every `Result` fn has
   `# Errors`), at most 150% of the Javadoc's words, then `# Translation notes`, then
   `# Safety` (.claude/rules/documentation.md). `cargo xtask javadoc <item>` prints
   the Javadoc translated; `cargo xtask doc-fill` inserts the missing entries Java has.
   A `WARNING:` needs an accepted entry in design/current/doc-contract-changes.md.
   Name Java members, never `X.java:N`. A finding not burnt down yet is listed in
   xtask/doc-baseline.txt (`cargo xtask doc-baseline` rewrites it); a line matching
   nothing must go."
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::java::push_classes;

    const JAVA_RECORD: &str = r#"
        package org.apache.kafka.clients.producer;
        /**
         * A key/value pair to be sent to Kafka.
         */
        public class ProducerRecord<K, V> {
            // An undocumented field named like the documented getter `topic()`.
            private final String topic;
            /**
             * Creates a record to be sent to a specified topic and partition.
             *
             * @param topic The topic the record will be appended to
             * @param partition The partition to which the record should be sent
             * @throws IllegalArgumentException if the partition is negative
             * @throws NullPointerException if the topic is null
             */
            public ProducerRecord(String topic, Integer partition) {}
            /**
             * Creates a record with every field set explicitly.
             */
            public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value, Iterable<Header> headers) {}
            /**
             * Creates a record.
             *
             * @param topic The topic
             * @param partition The partition
             */
            public ProducerRecord(String topic, Integer partition, Long timestamp) {}
            /**
             * @return The topic this record is being sent to
             */
            public String topic() { return null; }
            public Integer partition() { return null; }
            /**
             * @since 4.4
             */
            public Long timestamp() { return null; }
        }
    "#;

    /// A Java interface whose Javadocs only point elsewhere, but `abort`'s.
    const JAVA_PRODUCER: &str = r#"
        package org.apache.kafka.clients.producer;
        public interface Producer {
            /**
             * @see KafkaProducer#flush()
             */
            void flush();
            /**
             * See {@link KafkaProducer#close()}
             */
            void close();
            /**
             * See {@link KafkaProducer#close()} for how closing works.
             */
            void abort();
        }
    "#;

    const JAVA_KAFKA_PRODUCER: &str = r#"
        package org.apache.kafka.clients.producer;
        public class KafkaProducer implements Producer {
            /**
             * Flushes all buffered records, waiting until each one is acknowledged by the broker.
             */
            public void flush() {}
            /**
             * Closes this producer, releasing every resource it holds right now.
             */
            public void close() {}
        }
    "#;

    fn fixture_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("xtask-api-docs-{name}-{}", std::process::id()))
    }

    /// Runs the rule over a crate whose `lib.rs` is `lib`, with `contracts`
    /// as the contract-changes file and `baseline` as the baseline.
    fn run(name: &str, lib: &str, contracts: &str, baseline: Option<&str>) -> Vec<Finding> {
        let dir = fixture_dir(name);
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("src")).unwrap();
        fs::write(dir.join("src/lib.rs"), lib).unwrap();
        fs::write(dir.join("contracts.md"), format!("# Changes\n\n## Entries\n\n{contracts}")).unwrap();
        let mut classes = Vec::new();
        for java in [JAVA_RECORD, JAVA_PRODUCER, JAVA_KAFKA_PRODUCER] {
            push_classes(&["clients".into(), "producer".into()], java, false, &mut classes);
        }
        let ctx = Context {
            java: Rc::new(JavaIndex { classes }),
            ffi_baseline: None,
            doc_baseline: None,
            cbindgen_config: None,
        };
        let mut rule = ApiDocs::new(&ctx);
        rule.contracts = dir.join("contracts.md");
        rule.root = dir.clone();
        rule.java_line_dirs = vec![PathBuf::from("src")];
        rule.baseline = baseline.map(|text| Baseline::parse("doc-baseline.txt", text));
        let krate = Crate::load(&dir.join("src/lib.rs")).unwrap();
        let (mut findings, _) = rule.findings(&krate);
        if let Some(baseline) = &rule.baseline {
            findings = baseline.apply(findings);
        }
        let _ = fs::remove_dir_all(&dir);
        findings
    }

    fn keys(findings: &[Finding]) -> Vec<String> {
        findings.iter().map(Finding::key).collect()
    }

    const CLASS: &str = "org.apache.kafka.clients.producer.ProducerRecord";

    /// `text` as `///` lines indented by `indent`.
    fn doc(text: &str, indent: &str) -> String {
        text.lines().map(|l| format!("{indent}/// {l}\n")).collect()
    }

    const CLASS_DOC: &str = "A key/value pair to be sent to Kafka.";

    const CTOR_DOC: &str = "Creates a record to be sent to a specified topic and partition.\n\n\
                            # Arguments\n\n\
                            - `topic` - The topic the record will be appended to\n\
                            - `partition` - The partition to which the record should be sent\n\n\
                            # Returns\n\nThe record.\n\n\
                            # Errors\n\n\
                            - `Error::is_local_illegal_argument_error` if the partition is negative";

    const TOPIC_DOC: &str = "# Returns\n\nThe topic this record is being sent to";

    /// A `lib.rs` with `ProducerRecord` documented by `class_doc`, its
    /// constructor by `ctor_doc` and `topic()` by `topic_doc`.
    fn lib_with(class_doc: &str, ctor_doc: &str, topic_doc: &str) -> String {
        format!(
            "{}#[doc(alias = \"{CLASS}\")]\npub struct ProducerRecord;\nimpl ProducerRecord {{\n\
             {}    #[doc(alias = \"{CLASS}#ProducerRecord(String,Integer)\")]\n    \
             pub fn with_topic_partition(topic: &str, partition: i32) -> Result<Self, Error> {{ Ok(Self) }}\n\
             {}    #[doc(alias = \"{CLASS}#topic\")]\n    pub fn topic(&self) -> &str {{ \"\" }}\n    \
             /// Plain text written for the Rust item, as long as it needs to be, {}.\n    \
             #[doc(alias = \"{CLASS}#partition\")]\n    pub fn partition(&self) {{}}\n}}\n\
             pub(crate) trait ErrorHierarchy {{\n    fn is_local_illegal_argument_error(&self) -> bool;\n}}\n",
            doc(class_doc, ""),
            doc(ctor_doc, "    "),
            doc(topic_doc, "    "),
            "word ".repeat(60).trim_end(),
        )
    }

    fn faithful() -> String {
        lib_with(CLASS_DOC, CTOR_DOC, TOPIC_DOC)
    }

    #[test]
    fn test_faithful_docs_pass() {
        assert!(run("faithful", &faithful(), "", None).is_empty());
    }

    #[test]
    fn test_missing() {
        let findings = run("missing", &lib_with("", CTOR_DOC, TOPIC_DOC), "", None);
        assert_eq!(keys(&findings), ["missing crate::ProducerRecord"]);
        assert_eq!(
            findings[0].detail,
            "the item has no doc: `cargo xtask javadoc crate::ProducerRecord` prints Java's, translated"
        );
        // An item without Javadoc (here: only a dropped `@since`) is missing
        // too; `missing` is its only finding.
        let lib = format!(
            "{}#[doc(alias = \"{CLASS}#timestamp\")]\npub fn timestamp(x: i32) -> i64 {{ 0 }}\n",
            faithful()
        );
        let findings = run("missing-plain", &lib, "", None);
        assert_eq!(keys(&findings), ["missing crate::timestamp"]);
        assert_eq!(findings[0].detail, "the item has no doc: describe its contract");
    }

    #[test]
    fn test_param() {
        let ctor = CTOR_DOC.replace("- `partition` - The partition to which the record should be sent\n", "");
        let findings = run("param", &lib_with(CLASS_DOC, &ctor, TOPIC_DOC), "", None);
        assert_eq!(keys(&findings), ["param crate::ProducerRecord::with_topic_partition"]);
        assert_eq!(
            findings[0].detail,
            "not listed under `# Arguments`: `partition` (Java: \"The partition to which the record should be sent\")"
        );
        // `# Parameters` is accepted, and `_name` matches `name`.
        let lib = faithful()
            .replace("# Arguments", "# Parameters")
            .replace("(topic: &str, partition: i32)", "(_topic: &str, partition: i32)");
        assert!(run("param-parameters", &lib, "", None).is_empty());
        // A parameter Java does not have is offered no text; a pattern is
        // skipped.
        let lib = lib_with("Plain.", "", "");
        let extra = format!("{lib}/// Plain.\npub fn plain(count: i32, (a, b): (i32, i32)) {{}}\n");
        let findings = run("param-extra", &extra, "", None);
        assert!(keys(&findings).contains(&"param crate::plain".to_string()), "{findings:?}");
        let plain = findings.iter().find(|f| f.item == "crate::plain").unwrap();
        assert_eq!(plain.detail, "not listed under `# Arguments`: `count`");
    }

    #[test]
    fn test_c_export_params() {
        let c =
            "/// Plain.\n///\n/// # Parameters\n///\n/// - `count` - how many\n///\n/// # Errors\n///\n/// - fails\n\
                 #[unsafe(no_mangle)]\n\
                 pub unsafe extern \"C\" fn kafka_x(self_: *mut i32, count: i32, out_err: *mut *mut i32) {}\n\
                 /// Plain.\n#[unsafe(no_mangle)]\n\
                 pub unsafe extern \"C\" fn kafka_y(self_: *mut i32, out_err: *mut *mut i32, n: i32) {}\n";
        let findings = run("c-export", &format!("{}{c}", faithful()), "", None);
        // `self_` and a trailing `out_err` are exempt; an `out_err` that is
        // not last is not.
        assert_eq!(keys(&findings), ["param kafka_y"]);
        assert_eq!(findings[0].detail, "not listed under `# Parameters`: `out_err`, `n`");
    }

    #[test]
    fn test_c_export_raw_identifier_param() {
        // The header names the parameter `type`, as cbindgen drops the `r#`.
        let c = "/// Plain.\n///\n/// # Parameters\n///\n/// - `type` - the type\n\
                 #[unsafe(no_mangle)]\n\
                 pub extern \"C\" fn kafka_x(r#type: i8) {}\n\
                 /// Plain.\n///\n/// # Parameters\n///\n/// - `r#type` - the type\n\
                 #[unsafe(no_mangle)]\n\
                 pub extern \"C\" fn kafka_y(r#type: i8) {}\n";
        let findings = run("c-export-raw", &format!("{}{c}", faithful()), "", None);
        assert_eq!(keys(&findings), ["param kafka_y"]);
        assert_eq!(findings[0].detail, "not listed under `# Parameters`: `type`");
    }

    #[test]
    fn test_fallible_c_exports_need_errors() {
        let params = "///\n/// # Parameters\n///\n/// - `cb` - done\n/// - `opaque` - passed to `cb`\n";
        let c = format!(
            "/// Plain.\n#[unsafe(no_mangle)]\n\
             pub unsafe extern \"C\" fn kafka_a(self_: *mut i32, out_err: *mut *mut i32) {{}}\n\
             /// Plain.\n{params}#[unsafe(no_mangle)]\n\
             pub unsafe extern \"C\" fn kafka_a_cb(self_: *mut i32, cb: kafka_a_cb_t, opaque: *mut c_void) {{}}\n\
             /// Plain.\n{params}///\n/// # Errors\n///\n/// - fails\n#[unsafe(no_mangle)]\n\
             pub unsafe extern \"C\" fn kafka_b_cb(self_: *mut i32, cb: kafka_b_cb_t, opaque: *mut c_void) {{}}\n\
             /// Plain.\n#[unsafe(no_mangle)]\npub unsafe extern \"C\" fn kafka_c(self_: *mut i32) {{}}\n\
             /// Plain.\n///\n/// # Parameters\n///\n/// - `n` - n\n#[unsafe(no_mangle)]\n\
             pub unsafe extern \"C\" fn kafka_d_cb(self_: *mut i32, n: i32) {{}}\n"
        );
        let findings = run("c-fallible", &format!("{}{c}", faithful()), "", None);
        // `out_err` and a `_cb` twin (named `_cb`, taking `cb`) are fallible.
        assert_eq!(keys(&findings), ["errors kafka_a", "errors kafka_a_cb"]);
        assert_eq!(
            findings[0].detail,
            "a fallible C export (`out_err` or a `_cb` twin) with no `# Errors` section"
        );
        // A Java throw of a fallible C export is an error, named by its C
        // predicate symbol; its type under `# Panics` does not count.
        let export = |sections: &str| {
            format!(
                "{}{}#[doc(alias = \"{CLASS}#ProducerRecord(String,Integer)\")]\n#[unsafe(no_mangle)]\n\
                 pub unsafe extern \"C\" fn kafka_r(topic: *const c_char, partition: i32, out_err: *mut *mut i32) {{}}\n",
                faithful(),
                doc(&format!("Creates.\n\n# Parameters\n\n- `topic` - t\n- `partition` - p\n\n{sections}"), "")
            )
        };
        let panics = export("# Errors\n\n- fails\n\n# Panics\n\n- `LocalIllegalArgumentError` if negative");
        let findings = run("c-fallible-panics", &panics, "", None);
        assert_eq!(keys(&findings), ["errors kafka_r"]);
        assert_eq!(
            findings[0].detail,
            "Java's `@throws IllegalArgumentException` is not named by its predicate \
             `is_local_illegal_argument_error` under `# Errors`"
        );
        let errors = export("# Errors\n\n- `kafka_common_Error_is_local_illegal_argument_error` if negative");
        assert!(run("c-fallible-errors", &errors, "", None).is_empty());
    }

    #[test]
    fn test_c_export_throw_kept_as_panic_by_its_rust_method() {
        let f: syn::ItemFn =
            syn::parse_str("pub unsafe extern \"C\" fn kafka_x(self_: *mut i32, out_err: *mut *mut i32) {}").unwrap();
        let item = DocItem {
            key: "kafka_x".into(),
            file: "lib.rs".into(),
            attrs: Vec::new(),
            markers: Vec::new(),
            sig: Some(f.sig),
            c_export: true,
            origin: Some("X::x".into()),
        };
        let javadoc = Translation {
            throws: vec![Throw {
                java: "IllegalStateException".into(),
                rust: "LocalIllegalStateError".into(),
                names: vec!["LocalIllegalStateError".into()],
                text: "if it is closed".into(),
            }],
            ..Translation::default()
        };
        let predicates: BTreeSet<String> = ["is_local_illegal_state_error".to_string()].into();
        let gap = |origin: Option<&OriginDoc>| {
            let gaps = Gaps::of(&item, "Plain.\n", std::slice::from_ref(&javadoc), &predicates, origin).unwrap();
            assert!(gaps.fallible && gaps.result_without_errors);
            gaps.throws.iter().map(|g| (g.error, g.predicate.clone())).collect::<Vec<_>>()
        };
        let error = vec![(true, Some("is_local_illegal_state_error".to_string()))];
        let panic = vec![(false, None)];
        // No Rust method known: an error.
        assert_eq!(gap(None), error);
        // A Rust method returning no `Result` keeps every throw as a panic.
        assert_eq!(gap(Some(&OriginDoc::default())), panic);
        // A `Result` method keeps it as a panic when its `# Panics` names it
        // (or describes a JDK throw) and its `# Errors` does not.
        let result = |errors: &str, panics: &str| OriginDoc {
            is_result: true,
            errors: errors.to_string(),
            panics: panics.to_string(),
        };
        assert_eq!(gap(Some(&result("- fails", ""))), error);
        assert_eq!(gap(Some(&result("", "- `LocalIllegalStateError` if closed"))), panic);
        assert_eq!(gap(Some(&result("", "If it is closed."))), panic);
        assert_eq!(
            gap(Some(&result("- `Error::is_local_illegal_state_error` if closed", "If so."))),
            error
        );
    }

    #[test]
    fn test_returns() {
        let findings = run("returns", &lib_with(CLASS_DOC, CTOR_DOC, "The topic."), "", None);
        assert_eq!(keys(&findings), ["returns crate::ProducerRecord::topic"]);
        assert_eq!(
            findings[0].detail,
            "returns `&str`: add `# Returns` (Java: \"The topic this record is being sent to\")"
        );
        // A summary starting "Returns" is enough.
        let lib = lib_with(CLASS_DOC, CTOR_DOC, "Returns the topic.");
        assert!(run("returns-summary", &lib, "", None).is_empty());
        // The output of a future is what is returned.
        let extra =
            "/// Plain.\n///\n/// # Errors\n///\n/// - fails\npub async fn a() -> Result<(), Error> { Ok(()) }\n\
                     /// Plain.\npub fn b() -> impl Future<Output = ()> + Send { async {} }\n\
                     /// Plain.\npub fn c() -> Pin<Box<dyn Future<Output = i64> + Send>> { todo() }\n\
                     /// Plain.\npub fn d() -> ! { loop {} }\n";
        let findings = run("returns-future", &format!("{}{extra}", faithful()), "", None);
        assert_eq!(keys(&findings), ["returns crate::c"]);
        assert_eq!(
            findings[0].detail,
            "returns `i64`: add `# Returns`, or start the summary with \"Returns\""
        );
        // A constructor needs none: a Java constructor's marker, or an
        // associated fn returning `Self` or its type; a method returning
        // `Self` does.
        let ctor = CTOR_DOC.replace("# Returns\n\nThe record.\n\n", "");
        assert!(run("returns-ctor", &lib_with(CLASS_DOC, &ctor, TOPIC_DOC), "", None).is_empty());
        let extra = "/// Plain.\npub struct S;\nimpl S {\n    /// Plain.\n    pub fn new() -> Self { S }\n    \
                     /// Plain.\n    pub fn with_x(x: i32) -> S { S }\n    \
                     /// Plain.\n    pub fn set_x(self) -> Self { self }\n}\n";
        let findings = run("returns-self", &format!("{}{extra}", faithful()), "", None);
        assert_eq!(keys(&findings), ["param crate::S::with_x", "returns crate::S::set_x"]);
        // A builder setter returning `Self` needs one too, and a predicate
        // whose summary starts "Whether" is not documented by it.
        let extra = "/// Plain.\npub struct B;\nimpl B {\n    /// Sets x.\n    pub fn set_x(self, x: i32) -> Self { self }\n    \
                     /// Whether it is x.\n    pub fn is_x(&self) -> bool { true }\n    \
                     /// Returns whether it is y.\n    pub fn is_y(&self) -> bool { true }\n    \
                     /// Whether it is z.\n    ///\n    /// # Returns\n    ///\n    /// `true` if z.\n    \
                     pub fn is_z(&self) -> bool { true }\n}\n";
        let findings = run("returns-builder", &format!("{}{extra}", faithful()), "", None);
        assert_eq!(
            keys(&findings),
            [
                "param crate::B::set_x",
                "returns crate::B::is_x",
                "returns crate::B::set_x"
            ]
        );
    }

    #[test]
    fn test_errors() {
        let ctor = CTOR_DOC.replace(
            "\n\n# Errors\n\n- `Error::is_local_illegal_argument_error` if the partition is negative",
            "",
        );
        let findings = run("errors", &lib_with(CLASS_DOC, &ctor, TOPIC_DOC), "", None);
        assert_eq!(keys(&findings), ["errors crate::ProducerRecord::with_topic_partition"]);
        // `NullPointerException` is exempt.
        assert_eq!(
            findings[0].detail,
            "returns a `Result` with no `# Errors` section; Java's `@throws IllegalArgumentException` is not named \
             by its predicate `is_local_illegal_argument_error` under `# Errors`"
        );
        // A name that only contains the predicate does not count.
        let wrong = CTOR_DOC.replace("is_local_illegal_argument_error`", "is_local_illegal_argument_errorx`");
        let findings = run("errors-word", &lib_with(CLASS_DOC, &wrong, TOPIC_DOC), "", None);
        assert_eq!(keys(&findings), ["errors crate::ProducerRecord::with_topic_partition"]);
        // `# Panics` names a throw too, but a `Result` fn still needs `# Errors`.
        let panics = CTOR_DOC.replace("# Errors", "# Panics");
        let findings = run("errors-panics", &lib_with(CLASS_DOC, &panics, TOPIC_DOC), "", None);
        assert_eq!(findings[0].detail, "returns a `Result` with no `# Errors` section");
        // The predicate, alone, through `Error` or in its C symbol, documents
        // a returned error; the type, the variant or a description does not.
        let named = "`Error::is_local_illegal_argument_error`";
        for predicate in [
            "`is_local_illegal_argument_error`",
            "[`Error::is_local_illegal_argument_error`]",
            "`kafka_common_Error_is_local_illegal_argument_error`",
        ] {
            let ctor = CTOR_DOC.replace(named, predicate);
            assert!(
                run("errors-predicate", &lib_with(CLASS_DOC, &ctor, TOPIC_DOC), "", None).is_empty(),
                "{predicate}"
            );
        }
        for other in [
            "`LocalIllegalArgumentError`",
            "[`Error::LocalIllegalArgument`]",
            "an illegal-argument error",
        ] {
            let ctor = CTOR_DOC.replace(named, other);
            let findings = run("errors-type", &lib_with(CLASS_DOC, &ctor, TOPIC_DOC), "", None);
            assert_eq!(
                keys(&findings),
                ["errors crate::ProducerRecord::with_topic_partition"],
                "{other}"
            );
        }
        // A throw whose predicate `Error` lacks cannot be documented.
        let lib = faithful().replace("fn is_local_illegal_argument_error", "fn is_other_error");
        let findings = run("errors-no-predicate", &lib, "", None);
        assert_eq!(
            findings[0].detail,
            "Java's `@throws IllegalArgumentException` has no predicate on `Error` to name \
             (`is_local_illegal_argument_error`)"
        );
        // A JDK throw kept as a panic is described in prose under `# Panics`.
        let prose = CTOR_DOC.replace(
            "- `Error::is_local_illegal_argument_error` if the partition is negative",
            "- on an I/O failure\n\n# Panics\n\nIf the partition is negative.",
        );
        assert!(run("errors-jdk-panic", &lib_with(CLASS_DOC, &prose, TOPIC_DOC), "", None).is_empty());
    }

    #[test]
    fn test_pointer_javadocs_are_followed() {
        let producer = "org.apache.kafka.clients.producer.Producer";
        let words = "word ".repeat(18);
        let lib = format!(
            "{}/// {words}\n#[doc(alias = \"{producer}#flush\")]\npub fn flush() {{}}\n\
             /// {words}\n#[doc(alias = \"{producer}#close\")]\npub fn close() {{}}\n\
             /// {words}\n#[doc(alias = \"{producer}#abort\")]\npub fn abort() {{}}\n",
            faithful()
        );
        // `flush` and `close` take `KafkaProducer`'s Javadocs (14 and 10
        // words); `abort`'s has text of its own, so it is not followed.
        let findings = run("pointer", &lib, "", None);
        assert_eq!(keys(&findings), ["size crate::abort", "size crate::close"]);
        assert_eq!(findings[1].detail, "18 words, more than 150% of the Javadoc's 10 (at most 15)");
        assert_eq!(
            pointer_refs("@see KafkaConsumer#commitSync(Duration)\n@since 2.0"),
            ["KafkaConsumer#commitSync(Duration)"]
        );
        assert_eq!(pointer_refs("See {@link KafkaProducer#close()}."), ["KafkaProducer#close()"]);
        assert_eq!(
            pointer_refs("@see KafkaConsumer\n@see MockConsumer"),
            ["KafkaConsumer", "MockConsumer"]
        );
        assert!(pointer_refs("Closes it.\n@see KafkaProducer#close()").is_empty());
        assert!(pointer_refs("@param x the x\n@see KafkaProducer#close()").is_empty());
    }

    #[test]
    fn test_size_counts_only_what_java_documents() {
        let six = "org.apache.kafka.clients.producer.ProducerRecord#ProducerRecord(String,Integer,Long,K,V,Iterable)";
        let three = "org.apache.kafka.clients.producer.ProducerRecord#ProducerRecord(String,Integer,Long)";
        let entries = |n: usize| {
            ["topic", "partition", "timestamp", "key", "value", "headers"][..n]
                .iter()
                .map(|p| format!("- `{p}` - the {p} of the record, as the broker will store it\n"))
                .collect::<String>()
        };
        let ctor = |marker: &str, n: usize, summary: &str, extra: &str| {
            let params = [
                "topic: &str",
                "partition: i32",
                "timestamp: i64",
                "key: K",
                "value: V",
                "headers: H",
            ][..n]
                .join(", ");
            format!(
                "{}    #[doc(alias = \"{marker}\")]\n    pub fn ctor{n}({params}) {{}}\n",
                doc(&format!("{summary}\n\n# Arguments\n\n{}{extra}", entries(n)), "    ")
            )
        };
        let lib = |body: String| format!("{}impl ProducerRecord {{\n{body}}}\n", faithful());
        // Java's doc is 8 words and has no `@param`: the six required
        // entries (66 words) are not counted, so the 8-word summary passes.
        let summary = "Creates a record with every field set explicitly.";
        assert!(run("size-no-params", &lib(ctor(six, 6, summary, "")), "", None).is_empty());
        // A `# Returns` and errors Java does not document are not counted
        // either; `# Translation notes` is.
        let extra = "\n# Returns\n\nThe record, ready to be sent to the cluster by a producer.\n\n\
                     # Errors\n\n- `Error::is_local_illegal_argument_error` if the partition is negative.\n";
        assert!(run("size-untagged", &lib(ctor(six, 6, summary, extra)), "", None).is_empty());
        let notes = format!("{extra}\n# Translation notes\n\n{}", "w ".repeat(5));
        let findings = run("size-notes", &lib(ctor(six, 6, summary, &notes)), "", None);
        assert_eq!(keys(&findings), ["size crate::ProducerRecord::ctor6"]);
        assert_eq!(findings[0].detail, "13 words, more than 150% of the Javadoc's 8 (at most 12)");
        // Java's `@param`s count, on both sides: 3 + 3 + 3 Java words; the
        // Rust summary (3) and the entries of `topic` and `partition` (12
        // each) count, the one of `timestamp`, which Java does not document,
        // does not.
        let findings = run("size-params", &lib(ctor(three, 3, "Creates a record.", "")), "", None);
        assert_eq!(keys(&findings), ["size crate::ProducerRecord::ctor3"]);
        assert_eq!(findings[0].detail, "27 words, more than 150% of the Javadoc's 9 (at most 13)");
    }

    #[test]
    fn test_size_boundary() {
        // The class Javadoc has 9 words: at most 13 (150%).
        let notes = |n: usize| format!("{CLASS_DOC}\n\n# Translation notes\n\n{}", "w ".repeat(n));
        assert!(run("size-ok", &lib_with(&notes(4), CTOR_DOC, TOPIC_DOC), "", None).is_empty());
        let findings = run("size-over", &lib_with(&notes(5), CTOR_DOC, TOPIC_DOC), "", None);
        assert_eq!(keys(&findings), ["size crate::ProducerRecord"]);
        assert_eq!(findings[0].detail, "14 words, more than 150% of the Javadoc's 9 (at most 13)");
        // `# Safety` is not counted, nor are headings.
        let safety = format!("{}\n\n# Safety\n\n{}", notes(4), "s ".repeat(50));
        assert!(run("size-safety", &lib_with(&safety, CTOR_DOC, TOPIC_DOC), "", None).is_empty());
        // An item without Javadoc has no limit (`partition`, 70 words).
    }

    #[test]
    fn test_layout() {
        let bad = format!("{CLASS_DOC}\n\n# Safety\n\ns.\n\n# Translation notes\n\nn.");
        let findings = run("layout", &lib_with(&bad, CTOR_DOC, TOPIC_DOC), "", None);
        assert_eq!(keys(&findings), ["layout crate::ProducerRecord"]);
        assert_eq!(
            findings[0].detail,
            "`# Safety` comes before `# Translation notes`; it must be last"
        );
        // Boilerplate in the original part, unless Java says it.
        let boiler = format!("{CLASS_DOC} Mirrors Java.");
        let findings = run("layout-boiler", &lib_with(&boiler, CTOR_DOC, TOPIC_DOC), "", None);
        assert_eq!(
            findings[0].detail,
            "\"Mirrors Java\" outside `# Translation notes`: the marker records the origin"
        );
        // Ignoring case.
        let lower = format!("{CLASS_DOC} As in claude.md, mirrors java.");
        let findings = run("layout-boiler-lower", &lib_with(&lower, CTOR_DOC, TOPIC_DOC), "", None);
        assert_eq!(
            findings[0].detail,
            "\"Mirrors Java\" outside `# Translation notes`: the marker records the origin; \"CLAUDE.md\" outside \
             `# Translation notes`: the marker records the origin"
        );
        let in_notes = format!("{CLASS_DOC}\n\n# Translation notes\n\nMirrors Java.");
        assert!(run("layout-boiler-notes", &lib_with(&in_notes, CTOR_DOC, TOPIC_DOC), "", None).is_empty());
    }

    #[test]
    fn test_member_without_javadoc() {
        // `partition()` has no Javadoc: a notes heading on it is a layout
        // finding.
        let with_notes = faithful().replace(
            "/// Plain text written",
            "/// Plain.\n    ///\n    /// # Translation notes\n    ///\n    /// Plain text written",
        );
        let findings = run("no-javadoc-notes", &with_notes, "", None);
        assert_eq!(keys(&findings), ["layout crate::ProducerRecord::partition"]);
        // A WARNING in its plain text is no layout finding, but still needs
        // an accepted entry.
        let warned = faithful().replace("/// Plain text written", "/// WARNING: differs.\n    /// Plain text written");
        let findings = run("no-javadoc-warning", &warned, "", None);
        assert_eq!(keys(&findings), ["warning crate::ProducerRecord::partition"]);
        // Boilerplate, in any case, is a layout finding: there are no notes
        // for it to move to.
        let boiler = faithful().replace(
            "/// Plain text written",
            "/// As java does; mirrors java.\n    /// Plain text written",
        );
        let findings = run("no-javadoc-boiler", &boiler, "", None);
        assert_eq!(keys(&findings), ["layout crate::ProducerRecord::partition"]);
        assert!(
            findings[0].detail.contains("\"Mirrors Java\" on an item with no Javadoc"),
            "{}",
            findings[0].detail
        );
    }

    #[test]
    fn test_scope() {
        let extra = "#[doc(hidden)]\npub fn hidden(x: i32) {}\nfn private(x: i32) {}\n\
                     /// Plain.\npub enum Kind { A, /// Plain.\nB }\n\
                     impl std::fmt::Display for ProducerRecord { fn fmt(&self, f: &mut Formatter) -> Result { Ok(()) } }\n\
                     impl Clone for ProducerRecord {\n    /// Own doc.\n    fn clone(&self) -> Self { Self }\n}\n\
                     trait Sealed { fn s(&self, x: i32); }\n\
                     impl Sealed for ProducerRecord {\n    /// Own doc.\n    fn s(&self, x: i32) {}\n}\n\
                     mod private_mod { pub fn f(x: i32) {} }\n";
        let findings = run("scope", &format!("{}{extra}", faithful()), "", None);
        assert_eq!(
            keys(&findings),
            [
                "missing crate::Kind::A",
                "returns crate::<ProducerRecord as Clone>::clone"
            ]
        );
    }

    fn entry(title: &str, accepted: &str) -> String {
        format!(
            "1. `{title}`\n   - Java member: `{CLASS}#topic`\n   - Contract change: differs\n     more text\n   \
             - Why: because\n   - Accepted: {accepted}\n"
        )
    }

    #[test]
    fn test_warning_needs_an_accepted_entry() {
        let topic = format!("{TOPIC_DOC}\n\n# Translation notes\n\nWARNING: Rust differs.");
        let lib = lib_with(CLASS_DOC, CTOR_DOC, &topic);
        let findings = run("warning", &lib, "", None);
        assert_eq!(keys(&findings), ["warning crate::ProducerRecord::topic"]);
        // A proposed entry is not enough.
        let proposed = entry("crate::ProducerRecord::topic", "");
        assert_eq!(
            keys(&run("warning-proposed", &lib, &proposed, None)),
            ["warning crate::ProducerRecord::topic"]
        );
        let accepted = entry("crate::ProducerRecord::topic", "2026-10-22 Name");
        assert!(run("warning-accepted", &lib, &accepted, None).is_empty());
        // A WARNING outside the notes is a layout finding.
        let outside = format!("{TOPIC_DOC}\n\nWARNING: Rust differs.");
        let findings = run("warning-outside", &lib_with(CLASS_DOC, CTOR_DOC, &outside), &accepted, None);
        assert_eq!(keys(&findings), ["layout crate::ProducerRecord::topic"]);
    }

    #[test]
    fn test_accepted_warning_accounts_for_a_throw() {
        // `Error` has no predicate for the throw, so `# Errors` cannot name it.
        let lib_for = |ctor: &str| {
            lib_with(CLASS_DOC, ctor, TOPIC_DOC).replace("fn is_local_illegal_argument_error", "fn is_other_error")
        };
        let key = "crate::ProducerRecord::with_topic_partition";
        let names =
            format!("{CTOR_DOC}\n\n# Translation notes\n\nWARNING: never fails with `IllegalArgumentException`.");
        let accepted = entry(key, "2026-10-22 Name");
        assert!(run("throw-warned", &lib_for(&names), &accepted, None).is_empty());
        // Only while accepted: a proposed entry leaves both findings.
        assert_eq!(
            keys(&run("throw-proposed", &lib_for(&names), &entry(key, ""), None)),
            [format!("errors {key}"), format!("warning {key}")]
        );
        // The WARNING must name the throw.
        let other = format!("{CTOR_DOC}\n\n# Translation notes\n\nWARNING: Rust differs.");
        assert_eq!(
            keys(&run("throw-unnamed", &lib_for(&other), &accepted, None)),
            [format!("errors {key}")]
        );
    }

    #[test]
    fn test_contract_stale() {
        let entries = format!(
            "{}{}",
            entry("crate::ProducerRecord::topic", "2026-10-22 Name"),
            entry("crate::Gone", "2026-10-22 Name").replace("1. ", "2. ")
        );
        let findings = run("stale", &faithful(), &entries, None);
        assert_eq!(
            keys(&findings),
            [
                "contract-stale crate::Gone",
                "contract-stale crate::ProducerRecord::topic"
            ]
        );
    }

    #[test]
    fn test_contract_file_parsing() {
        let good = entry("crate::a::B::c", "2026-10-22 Name");
        let (entries, findings) = parse_contract_changes("f", &format!("## Entries\n\n{good}"));
        assert!(findings.is_empty(), "{findings:?}");
        assert_eq!(entries[0].title, "crate::a::B::c");
        assert_eq!(
            entries[0].fields[1],
            ("Contract change".to_string(), "differs more text".to_string())
        );
        assert!(entries[0].is_accepted());

        // Entries before `## Entries` and inside fences are not read.
        let fenced = format!("```markdown\n{good}```\n## Entries\n\n```markdown\n{good}```\n");
        assert!(parse_contract_changes("f", &fenced).0.is_empty());

        let missing = good.replace("   - Why: because\n", "");
        let (_, findings) = parse_contract_changes("f", &format!("## Entries\n\n{missing}"));
        assert_eq!(findings[0].detail, "entry at line 3: missing sub-item `Why`");

        let unknown = good.replace("   - Why: because\n", "   - Why: because\n   - Notes: x\n");
        let (_, findings) = parse_contract_changes("f", &format!("## Entries\n\n{unknown}"));
        assert_eq!(findings[0].detail, "entry at line 3: unknown sub-item `Notes`");

        let swapped = good
            .replace("   - Why: because\n", "")
            .replace("   - Accepted:", "   - Why: x\n   - Accepted:");
        let reordered = swapped
            .replace(
                "   - Java member: `org.apache.kafka.clients.producer.ProducerRecord#topic`\n",
                "",
            )
            .replace("   - Accepted:", "   - Java member: m\n   - Accepted:");
        let (_, findings) = parse_contract_changes("f", &format!("## Entries\n\n{reordered}"));
        assert_eq!(
            findings[0].detail,
            "entry at line 3: the sub-items must be, in order: Java member, Contract change, Why, Accepted"
        );

        let (_, findings) = parse_contract_changes("f", &format!("## Entries\n\n{good}{}", good.replace("1. ", "2. ")));
        assert_eq!(keys(&findings), ["contract-format crate::a::B::c"]);
        assert!(
            findings[0].detail.ends_with("the title is listed twice"),
            "{}",
            findings[0].detail
        );
    }

    #[test]
    fn test_java_line_both_forms() {
        let lib = format!(
            "{}// See ProducerRecord.java:69 for the check.\nconst S: &str = \"Foo.java:1 in a string\";\n",
            faithful()
        );
        let findings = run("java-line", &lib, "", None);
        assert_eq!(keys(&findings), ["java-line src/lib.rs"]);
        assert!(
            findings[0]
                .detail
                .starts_with("1 Java file:line reference(s) in comments, the first `ProducerRecord.java:69` at line"),
            "{}",
            findings[0].detail
        );
        assert_eq!(
            java_line_refs("at `:142` and `:12-34`, not `:x` or `a:1`"),
            ["`:142`", "`:12-34`"]
        );
        assert_eq!(java_line_refs("Foo.java:12-34 and Bar.java: no"), ["Foo.java:12"]);
        // The file is waived as a whole.
        assert!(run("java-line-waived", &lib, "", Some("java-line src/lib.rs\n")).is_empty());
    }

    #[test]
    fn test_java_line_word_forms() {
        // A number after the word `Java`.
        assert_eq!(
            java_line_refs("(Java 73), Java line 140-142, (Java 967/972), Java lines 12–34."),
            ["Java 73", "Java 140-142", "Java 967/972", "Java 12–34"]
        );
        // Not a version, a word or another word.
        assert!(java_line_refs("Java 4.4 removed it; Java 11's constructors; JavaScript 5; Java's 3 forms").is_empty());
        // `line N` after a Java name in a code span, in the same line or the
        // one above.
        assert_eq!(
            java_line_refs("Translated from `MockProducerTest.testAutoCompleteMock` (line 73)."),
            ["line 73"]
        );
        assert_eq!(java_line_refs("`toOffsetCommitRequestData`, lines 62-67"), ["line 62-67"]);
        assert_eq!(
            java_line_refs_in("(line 85): `assignment()` holds tp.", "`testFoo` checks"),
            ["line 85"]
        );
        assert_eq!(java_line_refs("the Java `poll` at line 12"), ["line 12"]);
        // Rust lines, a Rust name, no Java name, and port numbers do not count.
        assert!(java_line_refs("`close_internal` line 2675").is_empty());
        assert!(java_line_refs("`KafkaProducer` at line 30").is_empty());
        assert!(java_line_refs("Rust's `ConsumerHeartbeatRequestManager.poll()` (lines 797-812").is_empty());
        assert!(java_line_refs("`testFoo` (producer_perf_test.rs closes at line 944)").is_empty());
        assert!(java_line_refs("the broker at localhost:9092 on line 3 of the config").is_empty());
        assert!(java_line_refs_in("(line 85)", "a `test_foo` translation").is_empty());
    }

    #[test]
    fn test_java_line_wrapped_across_lines() {
        // The number wraps onto the next comment line; the reference is
        // reported on the line it starts on.
        assert_eq!(
            java_line_refs_run(&["// Update the position (Java line", "// 303-306). Java uses `seek`."]),
            [(0, "Java 303-306".to_string())]
        );
        assert_eq!(
            java_line_refs_run(&["/// of `ConsumerBounceTest` (lines", "/// 79-103) on a cluster"]),
            [(0, "line 79-103".to_string())]
        );
        assert_eq!(
            java_line_refs_run(&[
                "    // ends with `config.providers` (`ProducerConfig.java`",
                "    // 613-618);"
            ]),
            [(0, "ProducerConfig.java 613-618".to_string())]
        );
        // Inner lines of a block comment, and a reference on a later line.
        assert_eq!(
            java_line_refs_run(&[
                "/*",
                " * Translated from `KafkaProducerTest.foo` (Java",
                " * 1989-1993)",
                " */"
            ]),
            [(1, "Java 1989-1993".to_string())]
        );
        // The Java name may sit on the line above the `line` word, but a
        // Rust name on the lines the reference spans clears it.
        assert_eq!(
            java_line_refs_run(&["/// Translates `testFooBar`", "/// (line", "/// 278)."]),
            [(1, "line 278".to_string())]
        );
        assert!(java_line_refs_run(&["/// `testFooBar` is in foo.rs (line", "/// 278)."]).is_empty());
        assert!(java_line_refs_run(&["/// see the config file, line", "/// 12."]).is_empty());
        // Values that wrap are not lines.
        assert!(java_line_refs_run(&["// the `SenderTest` clock starts at", "// 1000 for the same reason"]).is_empty());
    }

    #[test]
    fn test_java_line_at_forms() {
        // A range, when the run names a Java file or class.
        assert_eq!(
            java_line_refs_run(&["/// for `testSplitBatchAndSend`, Java spells that out at 2373-2375 with"]),
            [(0, "at 2373-2375".to_string())]
        );
        assert_eq!(
            java_line_refs_run(&["// `SenderTest#testFoo` covers it,", "// the call at", "// 12-14."]),
            [(1, "at 12-14".to_string())]
        );
        // A number right after a Java name in a code span.
        assert_eq!(java_line_refs("and `assertNotNull(md)` at 727 does not close it"), ["at 727"]);
        assert_eq!(java_line_refs("`SenderTest.java` at 88"), ["at 88"]);
        // Values: a plain number, no Java file or class, or a Rust name.
        assert!(java_line_refs("`testFoo` stops at 10, where the first fetch leaves it").is_empty());
        assert!(java_line_refs("bounded at 50 ms, as `pollTimeout` says").is_empty());
        assert!(java_line_refs("partitions 0 at 3-5 are fetched").is_empty());
        assert!(java_line_refs("`test_foo` at 727").is_empty());
        assert!(java_line_refs("`SenderTest` in sender.rs, at 12-14").is_empty());
        // `0` is never a line: a Java field left at it is a value.
        assert!(java_line_refs("the bootstrap cluster leaves `lastMetadataUpdateMs` at 0").is_empty());
        assert!(
            java_line_refs_run(&["/// `SenderTest#testFoo` reads `lastSendMs`", "/// at 0 and line 0."]).is_empty()
        );
    }

    #[test]
    fn test_comments_skip_literals() {
        let src = "let a = \"// not\"; // one\nlet b = r#\"/* not \"# ; /* two\n /* nested */ */\nlet c = '\"'; \
                   fn f<'a>(x: &'a str) {} // three\nlet d = b\"//\"; let e = br\"//\"; let f = '\\''; // four";
        let found: Vec<(usize, String)> = comments(src);
        assert_eq!(
            found,
            [
                (1, "// one".to_string()),
                (2, "/* two\n /* nested */ */".to_string()),
                (4, "// three".to_string()),
                (5, "// four".to_string()),
            ]
        );
    }

    #[test]
    fn test_stale_baseline_line() {
        let findings = run("baseline-stale", &faithful(), "", Some("size crate::ProducerRecord\n"));
        assert_eq!(keys(&findings), ["baseline size crate::ProducerRecord"]);
        assert_eq!(
            findings[0].detail,
            "stale entry: no finding matches it any more; delete the line"
        );
    }

    #[test]
    fn test_item_paths() {
        let m = |s: &[&str]| s.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        let rs = |stem: &str| PathBuf::from(format!("src/{stem}.rs"));
        assert_eq!(
            item_path(
                &m(&["producer", "producer_record"]),
                &rs("producer_record"),
                None,
                "ProducerRecord"
            ),
            "crate::producer::ProducerRecord"
        );
        assert_eq!(
            item_path(
                &m(&["producer", "producer_record"]),
                &rs("producer_record"),
                Some("ProducerRecord"),
                "topic"
            ),
            "crate::producer::ProducerRecord::topic"
        );
        assert_eq!(
            item_path(&m(&["producer"]), &rs("mod"), None, "Callback"),
            "crate::producer::Callback"
        );
        // A directory module named like its type stays.
        assert_eq!(item_path(&m(&["admin"]), &rs("mod"), None, "Admin"), "crate::admin::Admin");
        // An inline module is not a file module.
        assert_eq!(
            item_path(&m(&["a", "admin"]), &rs("a"), None, "Admin"),
            "crate::a::admin::Admin"
        );
        assert_eq!(
            item_path(
                &m(&["common", "metrics", "kafka_metric"]),
                &rs("kafka_metric"),
                Some("<KafkaMetric as Display>"),
                "fmt"
            ),
            "crate::common::metrics::<KafkaMetric as Display>::fmt"
        );
    }

    #[test]
    fn test_return_type() {
        let ret = |src: &str| {
            let f: syn::ItemFn = syn::parse_str(&format!("{src} {{}}")).unwrap();
            return_type(&f.sig).map(|t| compact(&t))
        };
        assert_eq!(ret("fn a()"), None);
        assert_eq!(ret("fn a() -> ()"), None);
        assert_eq!(ret("async fn a() -> i32").as_deref(), Some("i32"));
        assert_eq!(
            ret("fn a() -> impl Future<Output = Result<(), Error>> + Send + 'a").as_deref(),
            Some("Result<(), Error>")
        );
        assert_eq!(ret("fn a() -> Pin<Box<dyn Future<Output = ()> + Send>>"), None);
        assert_eq!(ret("fn a() -> InlineFuture<'_, T>").as_deref(), Some("T"));
        assert_eq!(ret("fn a() -> Box<Vec<u8>>").as_deref(), Some("Box<Vec<u8>>"));
        // An `async fn` returning a future returns the future.
        assert_eq!(
            ret("async fn a() -> Pin<Box<dyn Future<Output = ()>>>").as_deref(),
            Some("Pin<Box<dyn Future<Output = ()>>>")
        );
        let ty = |s: &str| syn::parse_str::<syn::Type>(s).unwrap();
        assert!(returns_nothing(&ty("Result<(), Error>")));
        assert!(returns_nothing(&ty("!")));
        assert!(!returns_nothing(&ty("Result<i32, Error>")));
        assert!(is_result(&ty("std::result::Result<i32, Error>")));
    }

    #[test]
    fn test_sections_and_words() {
        let secs = sections("Summary.\n\n# Arguments\n\n- `a` - x\n```\n# not a heading\n```\n# Errors\n\n- e");
        let names: Vec<&str> = secs.iter().map(|(h, _)| h.as_str()).collect();
        assert_eq!(names, ["", "Arguments", "Errors"]);
        assert!(secs[1].1.contains("# not a heading"));
        assert!(has_word("`XError` if", "XError"));
        assert!(!has_word("`XErrors` if", "XError"));
        assert!(!has_word("LocalXError", "XError"));
    }
}
