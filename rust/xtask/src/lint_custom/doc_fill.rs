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

//! `cargo xtask doc-fill [--write] <path>...`: inserts into the `///` docs of
//! the fns under the given files or directories what `check-api-docs` finds
//! missing and the Javadoc has (`.claude/rules/documentation.md` §8):
//!
//!   - an `# Arguments` (`# Parameters` for a C export) entry for each
//!     parameter not listed whose Java `@param` has text;
//!   - a `# Returns` section from Java's `@return`;
//!   - an entry for each Java `@throws` not documented: for a fn returning a
//!     `Result` or a fallible C export (`out_err`, or a `_cb` twin), under
//!     `# Errors`, naming the error's predicate (`Error::is_producer_fenced_error`;
//!     the C symbol for a C export), and only when `Error` has that predicate;
//!     otherwise, and for a throw the C export's Rust method keeps as a panic,
//!     under `# Panics`, naming the error type.
//!
//! Java's text is reworded (§3): "exception" as "error"; for a C export
//! `None` as `NULL`, `Self::foo` as the C export of the same class and no
//! intra-doc links. Every other line of the doc stays as it is: an entry joins
//! the end of its section, a new section goes where the doc layout puts it
//! (§2), before `# Translation notes` and `# Safety`. Nothing is written
//! without Java's text but a `_cb` twin's `cb` and `opaque` entries, whose
//! meaning CLAUDE.md §4 fixes, so a parameter Java does not have, a missing
//! summary and a `Result` fn's own errors stay findings. A second run finds
//! nothing more to insert.
//!
//! Without `--write` the insertions are printed. An item whose doc comes from
//! `#[doc = ..]` or a macro, or whose `///` block cannot be found in its file,
//! is skipped and reported, as is an item with no doc at all (`cargo xtask
//! javadoc` drafts one).

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use super::api_docs::{error_predicates, is_cb_twin, ApiDocs, DocItem, Gaps, OriginDoc};
use super::Crate;
use crate::javadoc::{doc_parts, doc_text, NOTES_HEADING};

/// The column an inserted doc line ends at, at most.
const WIDTH: usize = 100;

/// The sections `doc-fill` writes, in their layout order (§2). A heading not
/// listed sorts after them.
const ORDER: &[&[&str]] = &[&["Arguments", "Parameters"], &["Returns"], &["Errors"], &["Panics"]];

/// The text to insert into one section.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Addition {
    heading: &'static str,
    /// The lines to add, unprefixed: list entries, or a paragraph.
    lines: Vec<String>,
}

/// One item's insertions.
#[derive(Debug)]
pub(super) struct Edit {
    pub(super) key: String,
    /// The `///` lines it replaces (0-based), consecutive.
    removed: Vec<usize>,
    /// The new lines, prefix and indentation included.
    lines: Vec<String>,
    /// The lines inserted, unprefixed, for the dry run.
    added: Vec<String>,
}

/// What `doc-fill` would do.
#[derive(Debug, Default)]
pub(super) struct Plan {
    /// The insertions, by file.
    pub(super) edits: BTreeMap<String, Vec<Edit>>,
    /// The items skipped, with the file and why.
    pub(super) skipped: Vec<(String, String, String)>,
}

impl Plan {
    /// How many items it changes.
    pub(super) fn len(&self) -> usize {
        self.edits.values().map(Vec::len).sum()
    }

    /// Writes every insertion.
    pub(super) fn write(&self) -> anyhow::Result<()> {
        for (file, edits) in &self.edits {
            let text = fs::read_to_string(file)?;
            fs::write(file, rewrite(&text, edits))?;
        }
        Ok(())
    }

    /// The dry-run listing: each item with the lines it gains, then the
    /// skipped items.
    pub(super) fn describe(&self) -> String {
        let mut out = String::new();
        for (file, edits) in &self.edits {
            for edit in edits {
                out.push_str(&format!("{file}: {}\n", edit.key));
                for line in &edit.added {
                    out.push_str(&format!("    + {line}\n"));
                }
            }
        }
        for (key, file, why) in &self.skipped {
            out.push_str(&format!("{file}: skipped {key}: {why}\n"));
        }
        out
    }
}

/// `text` with `edits` applied.
fn rewrite(text: &str, edits: &[Edit]) -> String {
    let removed: BTreeSet<usize> = edits.iter().flat_map(|e| e.removed.iter().copied()).collect();
    let inserts: BTreeMap<usize, &Edit> = edits.iter().filter_map(|e| Some((*e.removed.first()?, e))).collect();
    let mut out: Vec<&str> = Vec::new();
    for (n, line) in text.split('\n').enumerate() {
        if let Some(edit) = inserts.get(&n) {
            out.extend(edit.lines.iter().map(String::as_str));
        }
        if !removed.contains(&n) {
            out.push(line);
        }
    }
    out.join("\n")
}

/// A block of `///`, `//` and attribute lines in front of a declaration.
struct Run {
    /// The `///` lines: index and the text after `///`.
    docs: Vec<(usize, String)>,
    /// The attribute and comment text, joined.
    attrs: String,
    /// Whether an attribute sets the doc as a string (`#[doc = ..]`).
    doc_attr: bool,
    /// The declaration line after the block.
    decl: String,
}

/// How many `[` the `#[..]` text `line` leaves open, string contents skipped.
fn open_brackets(line: &str, mut depth: i32) -> i32 {
    let mut in_str = false;
    let mut escaped = false;
    for c in line.chars() {
        if in_str {
            match (escaped, c) {
                (true, _) => escaped = false,
                (false, '\\') => escaped = true,
                (false, '"') => in_str = false,
                _ => {},
            }
            continue;
        }
        match c {
            '"' => in_str = true,
            '[' => depth += 1,
            ']' => depth -= 1,
            _ => {},
        }
    }
    depth
}

/// Whether attribute text `attrs` sets a doc as a string.
fn sets_doc_string(attrs: &str) -> bool {
    ["doc =", "doc="].iter().any(|pat| {
        attrs.match_indices(pat).any(|(at, _)| {
            attrs[..at]
                .chars()
                .next_back()
                .is_none_or(|c| !(c.is_alphanumeric() || c == '_'))
        })
    })
}

/// The doc runs of a file's `lines`.
fn runs(lines: &[&str]) -> Vec<Run> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let start = i;
        let mut docs = Vec::new();
        let mut attrs = String::new();
        let mut any = false;
        while i < lines.len() {
            let t = lines[i].trim_start();
            if let Some(rest) = t.strip_prefix("///") {
                if rest.starts_with('/') {
                    break;
                }
                docs.push((i, rest.to_string()));
                i += 1;
                any = true;
            } else if t.starts_with("#[") {
                let mut depth = open_brackets(t, 0);
                attrs.push_str(t);
                attrs.push('\n');
                i += 1;
                while depth > 0 && i < lines.len() {
                    depth = open_brackets(lines[i], depth);
                    attrs.push_str(lines[i]);
                    attrs.push('\n');
                    i += 1;
                }
                any = true;
            } else if t.starts_with("//") && !t.starts_with("//!") {
                attrs.push_str(t);
                attrs.push('\n');
                i += 1;
            } else {
                break;
            }
        }
        if !any {
            i = start + 1;
            continue;
        }
        let decl = lines.get(i).map(|l| (*l).to_string()).unwrap_or_default();
        out.push(Run { docs, doc_attr: sets_doc_string(&attrs), attrs, decl });
    }
    out
}

/// Whether `line` contains `name` as a whole identifier.
fn has_ident(line: &str, name: &str) -> bool {
    let ident = |c: char| c.is_alphanumeric() || c == '_';
    line.match_indices(name).any(|(at, _)| {
        line[..at].chars().next_back().is_none_or(|c| !ident(c))
            && line[at + name.len()..].chars().next().is_none_or(|c| !ident(c))
    })
}

/// The values of `item`'s doc attributes, one per `///` line.
fn doc_values(item: &DocItem) -> Vec<String> {
    let mut out = Vec::new();
    for attr in &item.attrs {
        let syn::Meta::NameValue(nv) = &attr.meta else { continue };
        if !nv.path.is_ident("doc") {
            continue;
        }
        if let syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(s), .. }) = &nv.value {
            out.push(s.value());
        }
    }
    out
}

/// Whether a wrapped line may not start with `word`: Markdown would read it
/// as a heading, a list item, a quote, a table row or a fence.
fn starts_block(word: &str) -> bool {
    let digits = word.chars().take_while(char::is_ascii_digit).count();
    word.starts_with(['#', '-', '*', '+', '>', '|', '='])
        || word.starts_with("```")
        || digits > 0 && matches!(&word[digits..], "." | ")")
}

/// `words` filled into lines of at most `width` columns, the first prefixed
/// `first`, the others `rest`. A word too long for a line gets one of its own.
fn fill(words: &[&str], width: usize, first: &str, rest: &str, out: &mut Vec<String>) {
    let mut line = first.to_string();
    let mut empty = true;
    for word in words {
        if !empty && line.len() + 1 + word.len() > width && !starts_block(word) {
            out.push(line);
            line = rest.to_string();
            empty = true;
        }
        if !empty {
            line.push(' ');
        }
        line.push_str(word);
        empty = false;
    }
    out.push(line);
}

/// `text` wrapped at `width` columns. Headings, fences and their contents,
/// table rows and blank lines stay as they are; a list item gets a hanging
/// indent.
fn wrap(text: &str, width: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut fence = false;
    for line in text.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("```") {
            fence = !fence;
            out.push(line.to_string());
            continue;
        }
        if fence || trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with('|') || line.len() <= width {
            out.push(line.to_string());
            continue;
        }
        let indent = &line[..line.len() - trimmed.len()];
        let digits = trimmed.chars().take_while(char::is_ascii_digit).count();
        let marker = if trimmed.starts_with("- ") || trimmed.starts_with("* ") {
            2
        } else if digits > 0 && trimmed[digits..].starts_with(". ") {
            digits + 2
        } else {
            0
        };
        let words: Vec<&str> = trimmed[marker..].split_whitespace().collect();
        let first = format!("{indent}{}", &trimmed[..marker]);
        let rest = format!("{indent}{}", " ".repeat(marker));
        fill(&words, width, &first, &rest, &mut out);
    }
    out
}

/// What a C export's inserted text refers to (documentation.md §3).
#[derive(Debug)]
struct CText<'a> {
    /// The C prefixes the export's class may have, most likely first: the
    /// one its Rust method gives (`kafka_producer_Producer`), then the
    /// export's own name cut at each `_` from the right, down to three
    /// segments. `Self::foo` becomes the first `<prefix>_foo` that exists.
    prefixes: Vec<String>,
    /// Every C export.
    symbols: &'a BTreeSet<String>,
}

impl CText<'_> {
    /// The candidate prefixes of export `symbol`, which translates Rust
    /// method `origin` when known (see [`CText::prefixes`]).
    fn prefixes(symbol: &str, origin: Option<&str>) -> Vec<String> {
        let mut out: Vec<String> = origin.and_then(|o| c_prefix(symbol, o)).into_iter().collect();
        let mut rest = symbol;
        while let Some(at) = rest.rfind('_') {
            rest = &rest[..at];
            if rest.matches('_').count() < 2 {
                break;
            }
            if !out.iter().any(|p| p == rest) {
                out.push(rest.to_string());
            }
        }
        out
    }

    /// The C form of code span `code`: `None` as `NULL`, `Self::foo` as the
    /// export of the same class or else `foo`, `Type::foo` as the one export
    /// ending `_Type_foo` when there is exactly one.
    fn code(&self, code: &str) -> String {
        if code == "None" {
            return "NULL".to_string();
        }
        if let Some(name) = code.strip_prefix("Self::") {
            return self
                .prefixes
                .iter()
                .map(|p| format!("{p}_{name}"))
                .find(|s| self.symbols.contains(s))
                .unwrap_or_else(|| name.to_string());
        }
        let ident = |s: &str| !s.is_empty() && s.chars().all(|c| c.is_alphanumeric() || c == '_');
        if let Some((ty, name)) = code.split_once("::").filter(|(t, n)| ident(t) && ident(n)) {
            let suffix = format!("_{ty}_{name}");
            let mut found = self.symbols.iter().filter(|s| s.ends_with(&suffix));
            if let (Some(one), None) = (found.next(), found.next()) {
                return one.clone();
            }
        }
        code.to_string()
    }
}

/// How the text `doc-fill` inserts into one item is worded.
#[derive(Debug, Default)]
struct Wording<'a> {
    /// For a C export, what its text refers to.
    c: Option<CText<'a>>,
    /// For a `_cb` twin, what its completion `cb` receives besides `opaque`;
    /// `None` when its typedef is not found.
    cb_value: Option<Option<CbShape>>,
}

/// What the completion `cb` of a `_cb` twin receives besides `opaque`, read
/// from its `_cb_t` typedef.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CbShape {
    /// A value and the error.
    ValueAndError,
    /// The error only.
    Error,
    /// A value only.
    Value,
    /// Nothing but `opaque`.
    Opaque,
}

/// Prose `text` (no code spans) with the word "exception" as "error" and
/// "exceptionally" as "with an error" (CLAUDE.md §2, §4), and for C `None`
/// as `NULL`.
fn reword_prose(text: &str, c: bool) -> String {
    let mut out = String::with_capacity(text.len());
    let mut word = String::new();
    let flush = |word: &mut String, out: &mut String| {
        match word.as_str() {
            "exception" => out.push_str("error"),
            "exceptions" => out.push_str("errors"),
            "Exception" => out.push_str("Error"),
            "Exceptions" => out.push_str("Errors"),
            // `completes exceptionally`, Java's `CompletableFuture` wording.
            "exceptionally" => out.push_str("with an error"),
            "None" if c => out.push_str("`NULL`"),
            w => out.push_str(w),
        }
        word.clear();
    };
    for ch in text.chars() {
        if ch.is_alphanumeric() || ch == '_' {
            word.push(ch);
        } else {
            flush(&mut word, &mut out);
            out.push(ch);
        }
    }
    flush(&mut word, &mut out);
    out
}

/// Java's translated `text` worded for the item it goes into: "exception"
/// as "error" outside code spans, and for a C export no Rust-only forms
/// (`None`, `Self::foo`, intra-doc links), as [`CText::code`] says.
fn adapt(text: &str, wording: &Wording) -> String {
    let c = wording.c.as_ref();
    if text.contains("``") {
        // A code span holding a backtick: prose rules only.
        return reword_prose(text, false);
    }
    let mut text = text.to_string();
    if c.is_some() {
        // An intra-doc link resolves only in Rust: keep its code span.
        while let Some(start) = text.find("[`") {
            let Some(len) = text[start + 2..].find("`]") else { break };
            let inner = text[start + 2..start + 2 + len].to_string();
            text.replace_range(start..start + len + 4, &format!("`{inner}`"));
        }
    }
    let mut out = String::with_capacity(text.len());
    for (i, part) in text.split('`').enumerate() {
        if i > 0 {
            out.push('`');
        }
        if i % 2 == 0 {
            out.push_str(&reword_prose(part, c.is_some()));
        } else {
            out.push_str(&c.map_or_else(|| part.to_string(), |c| c.code(part)));
        }
    }
    out
}

/// The entry of a `_cb` twin's parameter `name` (`cb`, `opaque`), whose
/// meaning CLAUDE.md §4 fixes, so it is written without Java's text.
fn cb_entry(name: &str, shape: Option<CbShape>) -> Option<&'static str> {
    match (name, shape) {
        ("cb", Some(CbShape::Error)) => Some("the completion, receiving the error, `NULL` on success"),
        ("cb", Some(CbShape::ValueAndError)) => {
            Some("the completion, receiving the value and the error, `NULL` on success")
        },
        ("cb", Some(CbShape::Value)) => Some("the completion, receiving the value"),
        ("cb", Some(CbShape::Opaque)) => Some("the completion, receiving only `opaque`"),
        ("cb", None) => Some("the completion, receiving the error, `NULL` on success, and any value"),
        ("opaque", _) => Some("passed to `cb`"),
        _ => None,
    }
}

/// What to insert to close `gaps`, from Java's text only (and the fixed
/// entries of a `_cb` twin's `cb` and `opaque`), worded by `wording` and
/// wrapped at `width`.
fn additions(gaps: &Gaps, width: usize, wording: &Wording) -> Vec<Addition> {
    let mut out = Vec::new();
    let params: Vec<String> = gaps
        .params
        .iter()
        .filter_map(|(name, java)| match java.as_deref().filter(|t| !t.trim().is_empty()) {
            Some(text) => Some((name, adapt(text.trim(), wording))),
            None => wording.cb_value.and_then(|v| cb_entry(name, v)).map(|t| (name, t.to_string())),
        })
        .flat_map(|(name, text)| wrap(&format!("- `{name}` - {text}"), width))
        .collect();
    if !params.is_empty() {
        out.push(Addition { heading: gaps.param_heading(), lines: params });
    }
    if let Some((_, Some(text))) = &gaps.returns {
        if !text.trim().is_empty() {
            out.push(Addition { heading: "Returns", lines: wrap(&adapt(text.trim(), wording), width) });
        }
    }
    let mut errors: Vec<String> = Vec::new();
    let mut panics: Vec<String> = Vec::new();
    for gap in &gaps.throws {
        let t = &gap.throw;
        let name = if gap.error {
            // A returned error is named by its predicate; one without a
            // predicate on `Error` stays a finding.
            let Some(p) = gap.predicate.as_ref() else { continue };
            if gaps.c_export {
                format!("kafka_common_Error_{p}")
            } else {
                format!("Error::{p}")
            }
        } else if t.names.contains(&t.rust) {
            // A panic: the name the lint looks for, the translator's when it
            // is one.
            t.rust.clone()
        } else {
            t.names.first().unwrap_or(&t.rust).clone()
        };
        let entry = format!("- `{name}` {}", adapt(t.text.trim(), wording));
        let lines = wrap(entry.trim_end(), width);
        if gap.error {
            errors.extend(lines);
        } else {
            panics.extend(lines);
        }
    }
    if !errors.is_empty() {
        out.push(Addition { heading: "Errors", lines: errors });
    }
    if !panics.is_empty() {
        out.push(Addition { heading: "Panics", lines: panics });
    }
    out
}

/// The C prefix of the class of export `symbol`, which translates Rust
/// method `origin` (`Type::method`): the symbol less `_cb` and `_method`,
/// else the symbol up to its `_Type`.
fn c_prefix(symbol: &str, origin: &str) -> Option<String> {
    let (ty, method) = origin.split_once("::")?;
    let base = symbol.strip_suffix("_cb").unwrap_or(symbol);
    if let Some(prefix) = base.strip_suffix(&format!("_{method}")) {
        return Some(prefix.to_string());
    }
    let at = symbol.rfind(&format!("_{ty}_"))?;
    Some(symbol[..at + 1 + ty.len()].to_string())
}

/// What the completion `cb` of `_cb` twin `sig` receives besides the opaque
/// pointer, from its typedef in `krate`: a parameter named `error` / `err`
/// or typed `*Error_t` is the error, `opaque` the opaque pointer, any other
/// a value. `None` when the typedef is not found.
fn cb_shape(krate: &Crate, sig: &syn::Signature) -> Option<CbShape> {
    let ty = sig.inputs.iter().find_map(|input| match input {
        syn::FnArg::Typed(pt) if matches!(&*pt.pat, syn::Pat::Ident(p) if p.ident == "cb") => Some(&*pt.ty),
        _ => None,
    })?;
    let name = super::type_ident(ty)?;
    for module in krate.modules.values() {
        for item in &module.items {
            let syn::Item::Type(t) = item else { continue };
            if t.ident != name {
                continue;
            }
            return bare_fn(&t.ty).map(shape_of);
        }
    }
    None
}

/// The [`CbShape`] of completion type `f`.
fn shape_of(f: &syn::TypeBareFn) -> CbShape {
    let (mut value, mut error) = (false, false);
    for arg in &f.inputs {
        let name = arg.name.as_ref().map(|(ident, _)| ident.to_string()).unwrap_or_default();
        let ty = &arg.ty;
        let ty_text = quote::quote!(#ty).to_string();
        if name == "opaque" {
            continue;
        }
        if name == "error" || name == "err" || ty_text.contains("Error_t") {
            error = true;
        } else {
            value = true;
        }
    }
    match (value, error) {
        (true, true) => CbShape::ValueAndError,
        (false, true) => CbShape::Error,
        (true, false) => CbShape::Value,
        (false, false) => CbShape::Opaque,
    }
}

/// The `fn` type of `ty`, also inside an `Option`.
fn bare_fn(ty: &syn::Type) -> Option<&syn::TypeBareFn> {
    match ty {
        syn::Type::BareFn(f) => Some(f),
        syn::Type::Path(p) => {
            let seg = p.path.segments.last()?;
            let syn::PathArguments::AngleBracketed(args) = &seg.arguments else {
                return None;
            };
            args.args.iter().find_map(|a| match a {
                syn::GenericArgument::Type(t) => bare_fn(t),
                _ => None,
            })
        },
        _ => None,
    }
}

/// The rank of `heading` in [`ORDER`]; a heading not listed ranks last.
fn rank(heading: &str) -> usize {
    ORDER.iter().position(|hs| hs.contains(&heading)).unwrap_or(ORDER.len())
}

/// The heading a doc line is, outside a fence.
fn heading(line: &str) -> Option<&str> {
    let trimmed = line.trim_start();
    let hashes = trimmed.len() - trimmed.trim_start_matches('#').len();
    (hashes > 0 && trimmed[hashes..].starts_with(' ')).then(|| trimmed[hashes..].trim())
}

/// The headings of `lines` (doc text, unprefixed) outside fences: index and
/// name.
fn headings(lines: &[String]) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    let mut fence = false;
    for (n, line) in lines.iter().enumerate() {
        if line.trim_start().starts_with("```") {
            fence = !fence;
        }
        if !fence {
            if let Some(h) = heading(line) {
                out.push((n, h.to_string()));
            }
        }
    }
    out
}

/// `lines` (doc text, unprefixed) with `add` inserted: at the end of its
/// section when the original part has one, else as a new section before the
/// first section ranked after it, or at the end of the original part.
fn insert(lines: &mut Vec<String>, add: &Addition) {
    let found = headings(lines);
    let end = found
        .iter()
        .find(|(_, h)| h == NOTES_HEADING || h == "Safety")
        .map_or(lines.len(), |(n, _)| *n);
    let original: Vec<&(usize, String)> = found.iter().filter(|(n, _)| *n < end).collect();
    let same = ORDER.iter().find(|hs| hs.contains(&add.heading)).copied().unwrap_or(&[]);
    if let Some(pos) = original.iter().position(|(_, h)| same.contains(&h.as_str())) {
        let start = original[pos].0;
        let stop = original.get(pos + 1).map_or(end, |(n, _)| *n);
        // After the section's last list, its continuation lines included, so
        // a paragraph that follows the list stays after the entries; after
        // the section's last line, as a list of its own, when it has none.
        let item = (start + 1..stop)
            .rev()
            .find(|&n| lines[n].starts_with("- ") || lines[n].starts_with("* "));
        let last = match item {
            Some(item) => (item + 1..stop)
                .take_while(|&n| lines[n].starts_with(' ') && !lines[n].trim().is_empty())
                .last()
                .unwrap_or(item),
            None => (start..stop).rev().find(|&n| !lines[n].trim().is_empty()).unwrap_or(start),
        };
        let mut new = Vec::new();
        if item.is_none() {
            new.push(String::new());
        }
        new.extend(add.lines.iter().cloned());
        lines.splice(last + 1..last + 1, new);
        return;
    }
    let r = rank(add.heading);
    let at = original.iter().find(|(_, h)| rank(h) > r).map_or(end, |(n, _)| *n);
    // The text before `at`, trailing blank lines left out.
    let before = (0..at).rev().find(|&n| !lines[n].trim().is_empty());
    let mut new = Vec::new();
    if before.is_some() {
        new.push(String::new());
    }
    new.push(format!("# {}", add.heading));
    new.push(String::new());
    new.extend(add.lines.iter().cloned());
    if at < lines.len() {
        new.push(String::new());
    }
    let from = before.map_or(0, |n| n + 1);
    lines.splice(from..at, new);
}

/// Whether `file` is one of `paths` or under one.
fn selected(file: &str, paths: &[PathBuf]) -> bool {
    let file = Path::new(file.strip_prefix("./").unwrap_or(file));
    paths.iter().any(|p| file.starts_with(p.strip_prefix(".").unwrap_or(p)))
}

/// The insertions for the fns under `paths`.
pub(super) fn plan(rule: &ApiDocs, krate: &Crate, paths: &[PathBuf]) -> anyhow::Result<Plan> {
    let mut plan = Plan::default();
    let predicates = error_predicates(krate);
    let items = rule.items(krate);
    let origins = OriginDoc::index(&items);
    let symbols: BTreeSet<String> = items.iter().filter(|i| i.c_export).map(|i| i.key.clone()).collect();
    // Per file: its lines, its doc runs, and the runs already matched.
    type FileRuns = (Vec<String>, Vec<Run>, BTreeSet<usize>);
    let mut sources: BTreeMap<String, FileRuns> = BTreeMap::new();
    for item in items {
        let Some(sig) = &item.sig else { continue };
        if !selected(&item.file, paths) {
            continue;
        }
        let javadocs = rule.javadocs(&item);
        if javadocs.is_empty() {
            continue;
        }
        let mut skip = |why: &str| plan.skipped.push((item.key.clone(), item.file.clone(), why.to_string()));
        let Some(doc) = doc_text(&item.attrs) else {
            skip("it has no doc: `cargo xtask javadoc` drafts one");
            continue;
        };
        let origin = item.origin.as_ref().and_then(|o| origins.get(o));
        let Some(gaps) = Gaps::of(&item, &doc_parts(&doc).original, &javadocs, &predicates, origin) else {
            continue;
        };
        let wording = Wording {
            c: item
                .c_export
                .then(|| CText { prefixes: CText::prefixes(&item.key, item.origin.as_deref()), symbols: &symbols }),
            cb_value: (item.c_export && is_cb_twin(sig)).then(|| cb_shape(krate, sig)),
        };
        if !sources.contains_key(&item.file) {
            let text = fs::read_to_string(&item.file)?;
            let lines: Vec<String> = text.split('\n').map(str::to_string).collect();
            let refs: Vec<&str> = lines.iter().map(String::as_str).collect();
            let found = runs(&refs);
            sources.insert(item.file.clone(), (lines, found, BTreeSet::new()));
        }
        let Some((lines, found, used)) = sources.get_mut(&item.file) else {
            continue;
        };
        let values = doc_values(&item);
        let name = sig.ident.to_string();
        let candidate = |run: &Run| {
            has_ident(&run.decl, &name)
                && (item.c_export || item.markers.iter().any(|m| run.attrs.contains(&format!("\"{m}\""))))
        };
        let matched = found
            .iter()
            .enumerate()
            .find(|(n, run)| !used.contains(n) && candidate(run) && run.docs.iter().map(|(_, d)| d).eq(values.iter()));
        let Some((n, run)) = matched else {
            if found.iter().any(|run| candidate(run) && run.doc_attr) {
                skip("its doc is set by `#[doc = ..]`; edit it by hand");
            } else {
                skip("no `///` block in front of its declaration matches its doc (a macro?); edit it by hand");
            }
            continue;
        };
        used.insert(n);
        if run.doc_attr {
            skip("its doc is set by `#[doc = ..]`; edit it by hand");
            continue;
        }
        let Some(&(first, _)) = run.docs.first() else { continue };
        let indent: String = lines[first].chars().take_while(|c| c.is_whitespace()).collect();
        let width = WIDTH.saturating_sub(indent.len() + 4).max(20);
        let adds = additions(&gaps, width, &wording);
        if adds.is_empty() {
            continue;
        }
        // The doc lines without the one space after `///`.
        let mut text: Vec<String> = run
            .docs
            .iter()
            .map(|(_, d)| d.strip_prefix(' ').unwrap_or(d).to_string())
            .collect();
        for add in &adds {
            insert(&mut text, add);
        }
        let new_lines = text
            .iter()
            .map(|line| {
                if line.is_empty() {
                    format!("{indent}///")
                } else {
                    format!("{indent}/// {line}")
                }
            })
            .collect();
        plan.edits.entry(item.file.clone()).or_default().push(Edit {
            key: item.key.clone(),
            removed: run.docs.iter().map(|(line, _)| *line).collect(),
            lines: new_lines,
            added: adds
                .iter()
                .flat_map(|a| std::iter::once(format!("# {}", a.heading)).chain(a.lines.iter().cloned()))
                .collect(),
        });
    }
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use super::*;
    use crate::java::{push_classes, JavaIndex};
    use crate::lint_custom::api_docs::ThrowGap;
    use crate::lint_custom::Context;

    const JAVA: &str = r#"
        package org.apache.kafka.clients.producer;
        /**
         * A key/value pair to be sent to Kafka.
         */
        public class ProducerRecord<K, V> {
            /**
             * Creates a record.
             *
             * @param topic The topic the record will be appended to
             * @param partition The partition to which the record should be sent
             * @throws IllegalArgumentException if the partition is negative
             * @throws NullPointerException if the topic is null
             */
            public ProducerRecord(String topic, Integer partition) {}
            /**
             * The topic.
             *
             * @return The topic this record is being sent to, or null
             */
            public String topic() { return null; }
            /**
             * Sends it.
             *
             * @param timeoutMs how long to wait
             * @return the offset
             * @throws TimeoutException if the wait times out
             * @throws InterruptException if it is interrupted
             */
            public long send(long timeoutMs) { return 0; }
            /**
             * Checks it.
             *
             * @param strict how strict to be
             * @throws IllegalStateException if it is closed
             */
            public void check(boolean strict) {}
            /**
             * Closes it.
             *
             * @param timeoutMs how long to wait, as {@link #send} does
             * @return the exception it failed with, or null
             * @throws TimeoutException if the exception was a timeout
             */
            public Object close(long timeoutMs) { return null; }
        }
    "#;

    const CLASS: &str = "org.apache.kafka.clients.producer.ProducerRecord";

    /// Runs `doc-fill --write` on a crate whose `lib.rs` is `lib`, `times`
    /// times, and returns the file and the last plan.
    fn fill_lib(name: &str, lib: &str, times: usize) -> (String, Plan) {
        let dir = std::env::temp_dir().join(format!("xtask-doc-fill-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("src")).unwrap();
        let lib_rs = dir.join("src/lib.rs");
        fs::write(&lib_rs, lib).unwrap();
        let mut classes = Vec::new();
        push_classes(&["clients".into(), "producer".into()], JAVA, false, &mut classes);
        let ctx = Context {
            java: Rc::new(JavaIndex { classes }),
            ffi_baseline: None,
            doc_baseline: None,
            cbindgen_config: None,
        };
        let rule = ApiDocs::new(&ctx);
        let mut last = Plan::default();
        for _ in 0..times {
            let krate = Crate::load(&lib_rs).unwrap();
            last = plan(&rule, &krate, std::slice::from_ref(&dir)).unwrap();
            last.write().unwrap();
        }
        let out = fs::read_to_string(&lib_rs).unwrap();
        let _ = fs::remove_dir_all(&dir);
        (out, last)
    }

    fn lib() -> String {
        format!(
            r#"/// A pair.
#[doc(alias = "{CLASS}")]
pub struct ProducerRecord;

impl ProducerRecord {{
    /// Creates a record.
    ///
    /// # Arguments
    ///
    /// - `topic` - Where it goes.
    ///
    /// # Translation notes
    ///
    /// Rust keeps an `Arc<str>`.
    #[doc(alias = "{CLASS}#ProducerRecord(String,Integer)")]
    #[must_use]
    pub fn with_topic_partition(topic: &str, partition: i32) -> Result<Self, Error> {{ Ok(Self) }}

    /// The topic.
    #[doc(alias = "{CLASS}#topic")]
    // A plain comment.
    pub fn topic(&self) -> Option<&str> {{ None }}

    /// Sends it.
    ///
    /// # Errors
    ///
    /// - on an I/O failure
    ///
    /// # Safety
    ///
    /// Call it right.
    #[doc(alias = "{CLASS}#send")]
    pub fn send(&self, timeout_ms: i64, extra: i32) -> Result<i64, Error> {{ Ok(0) }}

    /// Checks it.
    ///
    /// # Examples
    ///
    /// ```
    /// # Panics
    /// ```
    #[doc(alias = "{CLASS}#check")]
    pub fn check(&self, strict: bool) {{}}

    #[doc = "Set by an attribute."]
    #[doc(alias = "{CLASS}#topic")]
    pub fn topic_attr(&self) -> &str {{ "" }}

    #[doc(alias = "{CLASS}#topic")]
    pub fn topic_bare(&self) -> &str {{ "" }}
}}

pub(crate) trait ErrorHierarchy {{
    fn is_local_illegal_argument_error(&self) -> bool;
    fn is_local_timeout_error(&self) -> bool;
}}
"#
        )
    }

    #[test]
    fn test_fills_only_what_is_missing() {
        let (out, plan) = fill_lib("fill", &lib(), 1);
        let keys: Vec<&str> = plan.edits.values().flatten().map(|e| e.key.as_str()).collect();
        assert_eq!(
            keys,
            [
                "crate::ProducerRecord::with_topic_partition",
                "crate::ProducerRecord::topic",
                "crate::ProducerRecord::send",
                "crate::ProducerRecord::check"
            ]
        );
        let skipped: Vec<(&str, &str)> = plan.skipped.iter().map(|(k, _, why)| (k.as_str(), why.as_str())).collect();
        assert_eq!(
            skipped,
            [
                (
                    "crate::ProducerRecord::topic_attr",
                    "its doc is set by `#[doc = ..]`; edit it by hand"
                ),
                (
                    "crate::ProducerRecord::topic_bare",
                    "it has no doc: `cargo xtask javadoc` drafts one"
                ),
            ]
        );
        let expected = format!(
            r#"/// A pair.
#[doc(alias = "{CLASS}")]
pub struct ProducerRecord;

impl ProducerRecord {{
    /// Creates a record.
    ///
    /// # Arguments
    ///
    /// - `topic` - Where it goes.
    /// - `partition` - The partition to which the record should be sent
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` if the partition is negative
    ///
    /// # Translation notes
    ///
    /// Rust keeps an `Arc<str>`.
    #[doc(alias = "{CLASS}#ProducerRecord(String,Integer)")]
    #[must_use]
    pub fn with_topic_partition(topic: &str, partition: i32) -> Result<Self, Error> {{ Ok(Self) }}

    /// The topic.
    ///
    /// # Returns
    ///
    /// The topic this record is being sent to, or None
    #[doc(alias = "{CLASS}#topic")]
    // A plain comment.
    pub fn topic(&self) -> Option<&str> {{ None }}

    /// Sends it.
    ///
    /// # Arguments
    ///
    /// - `timeout_ms` - how long to wait
    ///
    /// # Returns
    ///
    /// the offset
    ///
    /// # Errors
    ///
    /// - on an I/O failure
    /// - `Error::is_local_timeout_error` if the wait times out
    ///
    /// # Safety
    ///
    /// Call it right.
    #[doc(alias = "{CLASS}#send")]
    pub fn send(&self, timeout_ms: i64, extra: i32) -> Result<i64, Error> {{ Ok(0) }}

    /// Checks it.
    ///
    /// # Arguments
    ///
    /// - `strict` - how strict to be
    ///
    /// # Panics
    ///
    /// - `LocalIllegalStateError` if it is closed
    ///
    /// # Examples
    ///
    /// ```
    /// # Panics
    /// ```
    #[doc(alias = "{CLASS}#check")]
    pub fn check(&self, strict: bool) {{}}

    #[doc = "Set by an attribute."]
    #[doc(alias = "{CLASS}#topic")]
    pub fn topic_attr(&self) -> &str {{ "" }}

    #[doc(alias = "{CLASS}#topic")]
    pub fn topic_bare(&self) -> &str {{ "" }}
}}

pub(crate) trait ErrorHierarchy {{
    fn is_local_illegal_argument_error(&self) -> bool;
    fn is_local_timeout_error(&self) -> bool;
}}
"#
        );
        assert_eq!(out, expected);
        // The dry run lists the same insertions.
        assert!(plan
            .describe()
            .contains("    + - `partition` - The partition to which the record should be sent\n"));
    }

    #[test]
    fn test_idempotent() {
        let (once, _) = fill_lib("once", &lib(), 1);
        let (twice, plan) = fill_lib("twice", &lib(), 2);
        assert_eq!(once, twice);
        assert_eq!(plan.len(), 0);
    }

    #[test]
    fn test_error_entries_name_the_predicate() {
        let throw = |java: &str, rust: &str| crate::javadoc::Throw {
            java: java.to_string(),
            rust: rust.to_string(),
            names: vec![rust.to_string()],
            text: "if so".to_string(),
        };
        let gaps = |c_export, error: [bool; 2]| Gaps {
            c_export,
            fallible: error[0] || error[1],
            throws: vec![
                ThrowGap {
                    throw: throw("ProducerFencedException", "ProducerFencedError"),
                    error: error[0],
                    predicate: error[0].then(|| "is_producer_fenced_error".into()),
                },
                ThrowGap {
                    throw: throw("InterruptException", "InterruptError"),
                    error: error[1],
                    predicate: None,
                },
            ],
            ..Gaps::default()
        };
        let entries = |g: &Gaps| {
            additions(g, 100, &Wording::default())
                .into_iter()
                .map(|a| (a.heading, a.lines))
                .collect::<Vec<_>>()
        };
        // A returned error: its predicate, the C symbol for a C export; one
        // without a predicate is left to the author.
        assert_eq!(
            entries(&gaps(false, [true, true])),
            [("Errors", vec!["- `Error::is_producer_fenced_error` if so".to_string()])]
        );
        assert_eq!(
            entries(&gaps(true, [true, true])),
            [(
                "Errors",
                vec!["- `kafka_common_Error_is_producer_fenced_error` if so".to_string()]
            )]
        );
        // A panic: the error type.
        assert_eq!(
            entries(&gaps(false, [false, false])),
            [(
                "Panics",
                vec![
                    "- `ProducerFencedError` if so".to_string(),
                    "- `InterruptError` if so".to_string()
                ]
            )]
        );
        // A fallible C export whose Rust method keeps one throw as a panic.
        assert_eq!(
            entries(&gaps(true, [true, false])),
            [
                (
                    "Errors",
                    vec!["- `kafka_common_Error_is_producer_fenced_error` if so".to_string()]
                ),
                ("Panics", vec!["- `InterruptError` if so".to_string()])
            ]
        );
    }

    #[test]
    fn test_adapt_rewords_for_the_item() {
        let symbols: BTreeSet<String> = [
            "kafka_producer_Producer_poll".to_string(),
            "kafka_consumer_Consumer_wakeup".to_string(),
        ]
        .into();
        let c = Wording {
            c: Some(CText { prefixes: vec!["kafka_producer_Producer".into()], symbols: &symbols }),
            cb_value: None,
        };
        let rust = Wording::default();
        let text = "if [`Self::poll`] fails with an exception, or `None`; None when \
                    `Consumer::wakeup` completes exceptionally. Exceptions: see the exception";
        // Rust: only the word "exception" changes; code spans stay.
        assert_eq!(
            adapt(text, &rust),
            "if [`Self::poll`] fails with an error, or `None`; None when \
             `Consumer::wakeup` completes with an error. Errors: see the error"
        );
        // C: links lose their brackets, `Self::` and `Type::` name the C
        // export, `None` is `NULL` in code and prose.
        assert_eq!(
            adapt(text, &c),
            "if `kafka_producer_Producer_poll` fails with an error, or `NULL`; `NULL` when \
             `kafka_consumer_Consumer_wakeup` completes with an error. Errors: see the error"
        );
        // `Self::` with no such export, and an unknown `Type::`, keep plain
        // words.
        assert_eq!(adapt("as `Self::send` and `Other::x` do", &c), "as `send` and `Other::x` do");
        assert_eq!(
            CText::prefixes("kafka_producer_Producer_send_cb", Some("Producer::send")),
            ["kafka_producer_Producer", "kafka_producer_Producer_send"]
        );
        assert_eq!(
            CText::prefixes("kafka_producer_Producer_send_cb", None),
            ["kafka_producer_Producer_send", "kafka_producer_Producer"]
        );
        // Words merely containing "exception" or "None" are kept.
        assert_eq!(
            reword_prose("ProducerFencedException Nones", true),
            "ProducerFencedException Nones"
        );
    }

    #[test]
    fn test_c_exports() {
        let lib = format!(
            r#"pub type kafka_producer_ProducerRecord_close_cb_t =
    Option<unsafe extern "C" fn(result: *mut c_void, err: *mut i32, opaque: *mut c_void)>;

/// Closes it.
#[doc(alias = "{CLASS}#close")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecord_close_cb(
    self_: *mut i32,
    timeout_ms: i64,
    cb: kafka_producer_ProducerRecord_close_cb_t,
    opaque: *mut c_void,
) {{}}

/// Sends it.
#[doc(alias = "{CLASS}#send")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecord_send(
    self_: *mut i32,
    timeout_ms: i64,
    out_err: *mut *mut i32,
) -> i64 {{ 0 }}

/// The topic.
#[doc(alias = "{CLASS}#topic")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecord_topic(self_: *mut i32) -> *const c_char {{ 0 }}

pub(crate) trait ErrorHierarchy {{
    fn is_local_timeout_error(&self) -> bool;
}}
"#
        );
        let (out, _) = fill_lib("c", &lib, 1);
        let expected = format!(
            r#"pub type kafka_producer_ProducerRecord_close_cb_t =
    Option<unsafe extern "C" fn(result: *mut c_void, err: *mut i32, opaque: *mut c_void)>;

/// Closes it.
///
/// # Parameters
///
/// - `timeout_ms` - how long to wait, as `kafka_producer_ProducerRecord_send` does
/// - `cb` - the completion, receiving the value and the error, `NULL` on success
/// - `opaque` - passed to `cb`
///
/// # Errors
///
/// - `kafka_common_Error_is_local_timeout_error` if the error was a timeout
#[doc(alias = "{CLASS}#close")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecord_close_cb(
    self_: *mut i32,
    timeout_ms: i64,
    cb: kafka_producer_ProducerRecord_close_cb_t,
    opaque: *mut c_void,
) {{}}

/// Sends it.
///
/// # Parameters
///
/// - `timeout_ms` - how long to wait
///
/// # Returns
///
/// the offset
///
/// # Errors
///
/// - `kafka_common_Error_is_local_timeout_error` if the wait times out
#[doc(alias = "{CLASS}#send")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecord_send(
    self_: *mut i32,
    timeout_ms: i64,
    out_err: *mut *mut i32,
) -> i64 {{ 0 }}

/// The topic.
///
/// # Returns
///
/// The topic this record is being sent to, or `NULL`
#[doc(alias = "{CLASS}#topic")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecord_topic(self_: *mut i32) -> *const c_char {{ 0 }}

pub(crate) trait ErrorHierarchy {{
    fn is_local_timeout_error(&self) -> bool;
}}
"#
        );
        assert_eq!(out, expected);
    }

    #[test]
    fn test_cb_entry_follows_the_completion_typedef() {
        let export = |name: &str, ty: &str| {
            format!(
                r#"/// Closes it.
#[doc(alias = "{CLASS}#close")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn {name}(self_: *mut i32, cb: {ty}, opaque: *mut c_void) {{}}

"#
            )
        };
        let lib = format!(
            r#"pub type opaque_only_cb_t = unsafe extern "C" fn(opaque: *mut c_void);
pub type error_only_cb_t =
    Option<unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void)>;
pub type value_only_cb_t = unsafe extern "C" fn(value: i64, opaque: *mut c_void);
pub type value_error_cb_t =
    unsafe extern "C" fn(value: i64, error: *mut kafka_common_Error_t, opaque: *mut c_void);

{}{}{}{}"#,
            export("kafka_producer_ProducerRecord_a_cb", "opaque_only_cb_t"),
            export("kafka_producer_ProducerRecord_b_cb", "error_only_cb_t"),
            export("kafka_producer_ProducerRecord_c_cb", "value_only_cb_t"),
            export("kafka_producer_ProducerRecord_d_cb", "value_error_cb_t"),
        );
        let (out, _) = fill_lib("cb-shapes", &lib, 1);
        let entries: Vec<&str> = out.lines().filter(|l| l.starts_with("/// - `cb` - ")).collect();
        assert_eq!(
            entries,
            [
                "/// - `cb` - the completion, receiving only `opaque`",
                "/// - `cb` - the completion, receiving the error, `NULL` on success",
                "/// - `cb` - the completion, receiving the value",
                "/// - `cb` - the completion, receiving the value and the error, `NULL` on success",
            ]
        );
    }

    #[test]
    fn test_insert_positions() {
        let lines = |s: &str| s.lines().map(str::to_string).collect::<Vec<_>>();
        let add = |heading, line: &str| Addition { heading, lines: vec![line.to_string()] };
        // An empty doc section gets a blank line after its heading.
        let mut doc = lines("Sum.\n\n# Errors");
        insert(&mut doc, &add("Errors", "- e"));
        assert_eq!(doc, lines("Sum.\n\n# Errors\n\n- e"));
        // A new section goes before the first one ranked after it.
        let mut doc = lines("Sum.\n\n# Panics\n\nIf so.\n\n# Safety\n\nOk.");
        insert(&mut doc, &add("Returns", "The value."));
        assert_eq!(
            doc,
            lines("Sum.\n\n# Returns\n\nThe value.\n\n# Panics\n\nIf so.\n\n# Safety\n\nOk.")
        );
        // An entry joins the section's list, before a paragraph that follows
        // it; a section without a list gets one after its text.
        let mut doc = lines("Sum.\n\n# Errors\n\n- a\n  more\n\nThen prose.\n\n# Panics\n\nIf so.");
        insert(&mut doc, &add("Errors", "- e"));
        assert_eq!(
            doc,
            lines("Sum.\n\n# Errors\n\n- a\n  more\n- e\n\nThen prose.\n\n# Panics\n\nIf so.")
        );
        let mut doc = lines("Sum.\n\n# Errors\n\nWhen it fails.");
        insert(&mut doc, &add("Errors", "- e"));
        assert_eq!(doc, lines("Sum.\n\n# Errors\n\nWhen it fails.\n\n- e"));
        // `# Parameters` is the C spelling of `# Arguments`.
        let mut doc = lines("Sum.\n\n# Parameters\n\n- `a` - A.");
        insert(&mut doc, &add("Arguments", "- `b` - B."));
        assert_eq!(doc, lines("Sum.\n\n# Parameters\n\n- `a` - A.\n- `b` - B."));
    }

    #[test]
    fn test_wrap() {
        let long = format!("{} end", "word ".repeat(30).trim_end());
        assert_eq!(
            wrap(&long, 40),
            [
                "word word word word word word word word",
                "word word word word word word word word",
                "word word word word word word word word",
                "word word word word word word end"
            ]
        );
        // A list item gets a hanging indent; a word Markdown would read as a
        // block start never begins a line.
        assert_eq!(wrap("- aaaa bbbb cccc - dddd", 13), ["- aaaa bbbb", "  cccc - dddd"]);
        assert_eq!(wrap("aaaa bbbb 1. cccc", 9), ["aaaa bbbb 1.", "cccc"]);
        // Fences and headings are kept.
        let fenced = format!("# {}\n```java\n{}\n```", "h ".repeat(30), "c ".repeat(30));
        assert_eq!(wrap(&fenced, 10), fenced.lines().collect::<Vec<_>>());
    }
}
