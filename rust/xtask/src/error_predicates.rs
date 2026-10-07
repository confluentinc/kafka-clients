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

//! `cargo xtask generate-error-predicates`: the C exports of `Error`'s predicates.
//!
//! CLAUDE.md §4 wants every predicate on `Error` to have a C counterpart,
//! `kafka_common_Error_<predicate>`: C cannot see enum variants, so the
//! predicates are the only way a C caller classifies an error beyond its code.
//! There is one predicate per error class (CLAUDE.md §3), well over a hundred,
//! and cbindgen expands no macros, so each export has to be spelled out in
//! source. This generates them from `impl Error` in `src/common/error.rs`, and
//! `check-generated` fails when the generated file is stale.
//!
//! Each export's doc is the first paragraph of the Rust predicate's doc, so the
//! two say the same thing.

use std::fs;
use std::process::exit;

use quote::ToTokens;

const PREDICATE_SOURCE: &str = "src/common/error.rs";
const PREDICATE_RS: &str = "src/ffi/error_predicates.rs";

/// One predicate on `Error`: its name and the first paragraph of its doc.
struct Predicate {
    name: String,
    summary: String,
}

/// Every predicate `impl Error` forwards: `pub fn is_*_error(&self) -> bool`
/// calling `ErrorHierarchy::is_*_error(self)`, in source order.
fn parse_predicates() -> anyhow::Result<Vec<Predicate>> {
    let source = fs::read_to_string(PREDICATE_SOURCE)?;
    let file = syn::parse_file(&source)?;
    let mut out = Vec::new();
    for item in &file.items {
        let syn::Item::Impl(i) = item else { continue };
        let is_error = matches!(&*i.self_ty, syn::Type::Path(p) if p.path.is_ident("Error"));
        if i.trait_.is_some() || !is_error {
            continue;
        }
        for item in &i.items {
            let syn::ImplItem::Fn(f) = item else { continue };
            let name = f.sig.ident.to_string();
            let body = f.block.to_token_stream().to_string().replace(' ', "");
            let forwards = body.contains(&format!("ErrorHierarchy::{name}(self)"));
            if !matches!(f.vis, syn::Visibility::Public(_)) || !forwards {
                continue;
            }
            let summary = doc_summary(&f.attrs);
            if summary.is_empty() {
                anyhow::bail!("{PREDICATE_SOURCE}: `Error::{name}` has no doc to copy into its C export");
            }
            out.push(Predicate { name, summary });
        }
    }
    if out.is_empty() {
        anyhow::bail!("{PREDICATE_SOURCE}: no predicates found on `impl Error`");
    }
    Ok(out)
}

/// The first paragraph of `attrs`' doc comment, joined into one line, with
/// intra-doc links reduced to their text (C has nothing for them to resolve to).
fn doc_summary(attrs: &[syn::Attribute]) -> String {
    let mut lines = Vec::new();
    for attr in attrs {
        let syn::Meta::NameValue(nv) = &attr.meta else { continue };
        if !nv.path.is_ident("doc") {
            continue;
        }
        let syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(s), .. }) = &nv.value else {
            continue;
        };
        let line = s.value();
        let line = line.trim();
        if line.is_empty() {
            if lines.is_empty() {
                continue;
            }
            break;
        }
        lines.push(line.to_string());
    }
    strip_links(&lines.join(" "))
}

/// `text` with each Markdown link `[label](target)` replaced by its label, and
/// each bare reference link `` [`X`] `` by its code span.
fn strip_links(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(open) = rest.find('[') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        let Some(close) = after.find(']') else {
            out.push_str(&rest[open..]);
            return out;
        };
        let label = &after[..close];
        let tail = &after[close + 1..];
        out.push_str(label);
        rest = match tail.strip_prefix('(').and_then(|t| t.find(')').map(|end| &t[end + 1..])) {
            Some(after_target) => after_target,
            None => tail,
        };
    }
    out.push_str(rest);
    out
}

/// `text` as `///` lines no wider than rustfmt's `max_width`, at item level.
fn doc_lines(text: &str) -> String {
    const WIDTH: usize = 100;
    let mut out = String::new();
    let mut line = String::new();
    for word in text.split_whitespace() {
        if !line.is_empty() && 4 + line.len() + 1 + word.len() > WIDTH {
            out.push_str(&format!("/// {line}\n"));
            line.clear();
        }
        if !line.is_empty() {
            line.push(' ');
        }
        line.push_str(word);
    }
    if !line.is_empty() {
        out.push_str(&format!("/// {line}\n"));
    }
    out
}

fn predicates_rust(predicates: &[Predicate]) -> anyhow::Result<String> {
    let mut out = String::from(
        r#"// Copyright 2025 Confluent Inc.
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

//! C exports of `Error`'s predicates -- GENERATED, DO NOT EDIT.
//!
//! Generated from the predicates on `impl Error` in `src/common/error.rs` by
//! `cargo xtask generate-error-predicates`, and checked for staleness by
//! `cargo xtask check-generated`.
//!
//! One export per predicate, named after it behind the type prefix
//! (`is_retriable_error` -> `kafka_common_Error_is_retriable_error`,
//! CLAUDE.md §4). C cannot see the error's variants, so these predicates are
//! how a C caller classifies an error beyond its numeric code
//! (`kafka_common_Error_code`). Every one is `false` for a null handle.

use super::common::{error_ref, kafka_common_Error_t};
"#,
    );
    for p in predicates {
        out.push('\n');
        out.push_str(&doc_lines(&p.summary));
        out.push_str(&format!(
            r#"///
/// Mirrors `Error::{name}`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_{name}(error: *const kafka_common_Error_t) -> bool {{
    if error.is_null() {{
        return false;
    }}
    unsafe {{ error_ref(error) }}.error.{name}()
}}
"#,
            name = p.name
        ));
    }
    out.push_str(
        r#"
/// Every export above, by the name of the predicate it mirrors, for the tests
/// that check each export against every error class.
#[cfg(test)]
pub(crate) const PREDICATES: &[(&str, unsafe extern "C" fn(*const kafka_common_Error_t) -> bool)] = &[
"#,
    );
    for p in predicates {
        out.push_str(&format!("    (\"{name}\", kafka_common_Error_{name}),\n", name = p.name));
    }
    out.push_str("];\n");
    crate::rustfmt(&out)
}

pub fn generate_error_predicates() -> anyhow::Result<()> {
    println!("🔧 Generating the C exports of Error's predicates from {PREDICATE_SOURCE}...");
    let predicates = parse_predicates()?;
    fs::write(PREDICATE_RS, predicates_rust(&predicates)?)?;
    println!("✅ Wrote {} exports to {PREDICATE_RS}", predicates.len());
    Ok(())
}

/// Fail when the generated exports no longer match `Error`'s predicates.
pub fn check_error_predicates_up_to_date() -> anyhow::Result<()> {
    println!("🔍 Checking the generated C exports of Error's predicates...");
    let predicates = parse_predicates()?;
    let expected = predicates_rust(&predicates)?;
    if !fs::read_to_string(PREDICATE_RS).is_ok_and(|actual| actual == expected) {
        eprintln!("\n❌ Stale generated predicate exports: {PREDICATE_RS}");
        eprintln!("   Run: cargo xtask generate-error-predicates");
        exit(1);
    }
    println!("✅ Predicate exports are up to date ({} predicates)", predicates.len());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_links_keeps_labels() {
        assert_eq!(
            strip_links("Prefer this to matching [`Error::Timeout`](Self::Timeout): a match."),
            "Prefer this to matching `Error::Timeout`: a match."
        );
        assert_eq!(
            strip_links("See [`ErrorHierarchy`] and [docs]."),
            "See `ErrorHierarchy` and docs."
        );
        assert_eq!(strip_links("no links"), "no links");
        assert_eq!(strip_links("unclosed [bracket"), "unclosed [bracket");
    }

    #[test]
    fn test_doc_summary_is_the_first_paragraph() {
        let f: syn::ItemFn = syn::parse_quote! {
            /// Whether this error's Java class is, or extends,
            /// `org.apache.kafka.common.errors.TopicExistsException`.
            ///
            /// Prefer this to matching [`Error::TopicExists`](Self::TopicExists).
            fn f() {}
        };
        assert_eq!(
            doc_summary(&f.attrs),
            "Whether this error's Java class is, or extends, `org.apache.kafka.common.errors.TopicExistsException`."
        );
    }

    #[test]
    fn test_doc_lines_wrap_at_the_width() {
        let text = "word ".repeat(40);
        let lines = doc_lines(&text);
        assert!(lines.lines().all(|l| l.len() <= 100 && l.starts_with("/// ")), "{lines}");
        assert_eq!(lines.split_whitespace().filter(|w| *w == "word").count(), 40);
    }
}
