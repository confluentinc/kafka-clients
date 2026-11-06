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

//! The generated files of the Node.js client (`javascript/lib/`), from the
//! same Java sources and FFI enum as the Python client's
//! (`javascript/CLAUDE.md`, "Generated files"):
//!
//!   - the error hierarchy ([`crate::error_hierarchy::js`]): one class per
//!     Python error class, in the folder of its Java package, plus
//!     `lib/_error_registry.js`;
//!   - the config classes' constants ([`crate::config_consts::js`]);
//!   - the producer's key-type table `lib/_config_types.js`
//!     ([`crate::config_types::js`]).
//!
//! Every generated module carries [`GENERATED_MARKER`]. A folder holding only
//! generated modules gets a generated `index.js`; a folder that also holds
//! hand-written ones ([`MIXED_DIRS`]) keeps its hand-written `index.js`, and the
//! generator owns the two marked blocks in it ([`REQUIRES_BEGIN`],
//! [`EXPORTS_BEGIN`]). A generated module no generator produces any more is
//! removed by `generate` and reported by `check-generated`.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

type Res<T> = anyhow::Result<T>;

/// The Node.js client's sources, relative to the repository root.
pub(crate) const JS_LIB: &str = "javascript/lib";

/// The marker of every generated module (in its leading comment block).
pub(crate) const GENERATED_MARKER: &str = "GENERATED, DO NOT EDIT";

/// The license header of a generated module.
pub(crate) const LICENSE: &str = "// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the \"License\");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an \"AS IS\" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
";

/// The header of a generated module: the license, then (after a blank line, so
/// `make types` copies only the license into the `.d.ts`) the marker naming
/// what the module is generated from.
pub(crate) fn header(from: &str) -> String {
    format!(
        "{LICENSE}\n// {GENERATED_MARKER}. Produced by `cargo xtask generate-error-codes` from\n// {from};\n// `cargo xtask check-generated` checks it is up to date.\n\n'use strict';\n"
    )
}

/// The lib/ folders whose `index.js` is hand-written (they also hold
/// hand-written modules); the generator owns only the marked blocks there.
/// `""` is `lib/` itself.
const MIXED_DIRS: &[&str] = &["", "common", "common/config", "producer"];

/// The lib/ folders whose `index.js` is hand-written and which hold no
/// generated module: their parents' blocks do not require them.
const HAND_WRITTEN_DIRS: &[&str] = &["common/security", "common/security/auth"];

pub(crate) const REQUIRES_BEGIN: &str = "// BEGIN GENERATED REQUIRES (cargo xtask generate-error-codes; do not edit)";
pub(crate) const REQUIRES_END: &str = "// END GENERATED REQUIRES";
pub(crate) const EXPORTS_BEGIN: &str = "  // BEGIN GENERATED EXPORTS (cargo xtask generate-error-codes; do not edit)";
pub(crate) const EXPORTS_END: &str = "  // END GENERATED EXPORTS";

/// A generated file and its content.
pub(crate) type GeneratedFile = (PathBuf, String);

/// One generated public module: the class `name`, defined in
/// `lib/<dir>/<stem>.js` (`dir` is `""` for `lib/` itself).
pub(crate) struct JsModule {
    pub dir: String,
    pub stem: String,
    pub name: String,
    pub content: String,
}

/// `require` path from a module in `from_dir` to `lib/<to>` (`to` a path under
/// lib/ without `.js`).
pub(crate) fn rel_require(from_dir: &str, to: &str) -> String {
    let from: Vec<&str> = from_dir.split('/').filter(|s| !s.is_empty()).collect();
    let target: Vec<&str> = to.split('/').collect();
    let (dirs, file) = target.split_at(target.len() - 1);
    let common = from.iter().zip(dirs).take_while(|(a, b)| a == b).count();
    let up = from.len() - common;
    let mut parts: Vec<&str> = Vec::new();
    parts.extend(dirs[common..].iter().copied());
    parts.extend(file.iter().copied());
    let down = parts.join("/");
    if up == 0 {
        format!("./{down}")
    } else {
        format!("{}{down}", "../".repeat(up))
    }
}

/// `text` as the lines of a JSDoc block at `indent`, wrapped at 80 columns.
/// Python's reStructuredText ``code`` becomes Markdown `code`.
pub(crate) fn jsdoc(text: &str, indent: &str) -> String {
    let text = text.replace("``", "`").replace("*/", "*\\/");
    let mut out = vec![format!("{indent}/**")];
    let width = 80usize.saturating_sub(indent.len() + 3);
    for (i, para) in text.split("\n\n").enumerate() {
        if i > 0 {
            out.push(format!("{indent} *"));
        }
        let mut line = String::new();
        for word in para.split_whitespace() {
            if !line.is_empty() && line.len() + 1 + word.len() > width {
                out.push(format!("{indent} * {line}"));
                line.clear();
            }
            if !line.is_empty() {
                line.push(' ');
            }
            line.push_str(word);
        }
        if !line.is_empty() {
            out.push(format!("{indent} * {line}"));
        }
    }
    out.push(format!("{indent} */"));
    out.join("\n")
}

/// A JavaScript single-quoted string literal.
pub(crate) fn js_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        match c {
            '\'' => out.push_str("\\'"),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

fn parent_dir(dir: &str) -> Option<String> {
    if dir.is_empty() {
        None
    } else {
        Some(dir.rsplit_once('/').map(|(p, _)| p.to_string()).unwrap_or_default())
    }
}

fn last_segment(dir: &str) -> &str {
    dir.rsplit('/').next().unwrap_or(dir)
}

/// The `index.js` files and blocks for `modules`.
fn index_outputs(repo_root: &Path, modules: &[JsModule]) -> Res<Vec<(PathBuf, String)>> {
    let lib = repo_root.join(JS_LIB);
    let mut by_dir: BTreeMap<String, Vec<&JsModule>> = BTreeMap::new();
    for m in modules {
        by_dir.entry(m.dir.clone()).or_default().push(m);
    }
    // Folders the generator makes (with their generated parents up to a mixed one).
    let mut generated_dirs: BTreeSet<String> = BTreeSet::new();
    for dir in by_dir.keys() {
        let mut d = Some(dir.clone());
        while let Some(cur) = d {
            if MIXED_DIRS.contains(&cur.as_str()) {
                break;
            }
            if HAND_WRITTEN_DIRS.contains(&cur.as_str()) {
                anyhow::bail!("javascript: generated module in the hand-written folder lib/{cur}");
            }
            generated_dirs.insert(cur.clone());
            d = parent_dir(&cur);
        }
    }
    let children = |dir: &str| -> Vec<String> {
        generated_dirs
            .iter()
            .filter(|d| parent_dir(d).as_deref() == Some(dir))
            .cloned()
            .collect()
    };
    let mut out = Vec::new();
    let all_dirs: BTreeSet<String> = by_dir.keys().cloned().chain(generated_dirs.iter().cloned()).collect();
    let mut mixed_seen: BTreeSet<String> = BTreeSet::new();
    for dir in &all_dirs {
        let mut members: Vec<&JsModule> = by_dir.get(dir).cloned().unwrap_or_default();
        members.sort_by(|a, b| a.name.cmp(&b.name));
        let subdirs = children(dir);
        let mut requires: Vec<String> = members
            .iter()
            .map(|m| format!("const {{ {} }} = require('./{}');", m.name, m.stem))
            .collect();
        requires.extend(subdirs.iter().map(|d| {
            let n = last_segment(d);
            format!("const {n} = require('./{n}');")
        }));
        let mut names: Vec<String> = members.iter().map(|m| m.name.clone()).collect();
        names.extend(subdirs.iter().map(|d| last_segment(d).to_string()));
        let path = if dir.is_empty() {
            lib.join("index.js")
        } else {
            lib.join(dir).join("index.js")
        };
        if MIXED_DIRS.contains(&dir.as_str()) {
            mixed_seen.insert(dir.clone());
            let current = fs::read_to_string(&path).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
            let block = format!("{REQUIRES_BEGIN}\n{}\n{REQUIRES_END}", requires.join("\n"));
            let with = splice(&current, REQUIRES_BEGIN, REQUIRES_END, &block, &path)?;
            let exports: Vec<String> = names.iter().map(|n| format!("  {n},")).collect();
            let block = format!("{EXPORTS_BEGIN}\n{}\n{EXPORTS_END}", exports.join("\n"));
            out.push((path.clone(), splice(&with, EXPORTS_BEGIN, EXPORTS_END, &block, &path)?));
            continue;
        }
        let what = if members.is_empty() {
            "a parent folder of generated modules".to_string()
        } else {
            format!("the generated modules of lib/{dir}/")
        };
        let mut text = header(&format!("the Java sources ({what})"));
        text.push('\n');
        text.push_str(&format!(
            "// `{}`: {what}.\n\n",
            if dir.is_empty() {
                "lib".to_string()
            } else {
                format!("lib/{dir}")
            }
        ));
        for r in &requires {
            text.push_str(r);
            text.push('\n');
        }
        if !requires.is_empty() {
            text.push('\n');
        }
        if names.is_empty() {
            text.push_str("module.exports = {};\n");
        } else {
            text.push_str("module.exports = {\n");
            for n in &names {
                text.push_str(&format!("  {n},\n"));
            }
            text.push_str("};\n");
        }
        out.push((path, text));
    }
    // A mixed folder with nothing generated keeps empty blocks.
    for dir in MIXED_DIRS {
        if mixed_seen.contains(*dir) {
            continue;
        }
        let subdirs = children(dir);
        let path = if dir.is_empty() {
            lib.join("index.js")
        } else {
            lib.join(dir).join("index.js")
        };
        let current = fs::read_to_string(&path).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
        let requires: Vec<String> = subdirs
            .iter()
            .map(|d| {
                let n = last_segment(d);
                format!("const {n} = require('./{n}');")
            })
            .collect();
        let block = format!("{REQUIRES_BEGIN}\n{}\n{REQUIRES_END}", requires.join("\n")).replace("\n\n", "\n");
        let with = splice(&current, REQUIRES_BEGIN, REQUIRES_END, &block, &path)?;
        let exports: Vec<String> = subdirs.iter().map(|d| format!("  {},", last_segment(d))).collect();
        let block = format!("{EXPORTS_BEGIN}\n{}\n{EXPORTS_END}", exports.join("\n")).replace("\n\n", "\n");
        out.push((path.clone(), splice(&with, EXPORTS_BEGIN, EXPORTS_END, &block, &path)?));
    }
    Ok(out)
}

/// `current` with the text from `begin` to `end` (both included) replaced by
/// `block`; the hand-written file must carry both markers.
fn splice(current: &str, begin: &str, end: &str, block: &str, path: &Path) -> Res<String> {
    match (current.find(begin), current.find(end)) {
        (Some(a), Some(b)) if a < b => Ok(format!("{}{}{}", &current[..a], block, &current[b + end.len()..])),
        _ => anyhow::bail!(
            "{}: the hand-written index.js lacks the `{begin}` / `{end}` markers",
            path.display()
        ),
    }
}

/// Every generated file of the Node.js client: `(path, content)`.
pub fn outputs(repo_root: &Path) -> Res<Vec<(PathBuf, String)>> {
    let lib = repo_root.join(JS_LIB);
    let (mut modules, registry) = crate::error_hierarchy::js::outputs(repo_root)?;
    let mut out = vec![registry];
    modules.extend(crate::config_consts::js::modules(repo_root)?);
    out.push(crate::config_types::js::output(repo_root)?);
    let mut seen: BTreeSet<(String, String)> = BTreeSet::new();
    for m in &modules {
        if !seen.insert((m.dir.clone(), m.stem.clone())) {
            anyhow::bail!("javascript: two generated modules for lib/{}/{}.js", m.dir, m.stem);
        }
        let dir = if m.dir.is_empty() {
            lib.clone()
        } else {
            lib.join(&m.dir)
        };
        out.push((dir.join(format!("{}.js", m.stem)), m.content.clone()));
    }
    out.extend(index_outputs(repo_root, &modules)?);
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

/// Generated modules under lib/ that no generator produces any more: `.js`
/// files carrying [`GENERATED_MARKER`] that are not among `outputs`.
fn orphaned(repo_root: &Path, outputs: &[(PathBuf, String)]) -> Res<Vec<PathBuf>> {
    let expected: BTreeSet<&PathBuf> = outputs.iter().map(|(p, _)| p).collect();
    let mut orphans = Vec::new();
    let mut stack = vec![repo_root.join(JS_LIB)];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)? {
            let path = entry?.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or_default();
            if !name.ends_with(".js") || expected.contains(&path) {
                continue;
            }
            let text = fs::read_to_string(&path)?;
            if text.lines().take(20).any(|l| l.contains(GENERATED_MARKER)) {
                orphans.push(path);
            }
        }
    }
    orphans.sort();
    Ok(orphans)
}

/// Write every generated file and remove the orphaned ones. The `.d.ts` beside
/// each are `make -C javascript types`'s (tsc, from the JSDoc).
pub fn generate(repo_root: &Path) -> Res<()> {
    let outputs = outputs(repo_root)?;
    for (path, content) in &outputs {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, content)?;
    }
    let orphans = orphaned(repo_root, &outputs)?;
    for path in &orphans {
        fs::remove_file(path)?;
    }
    println!(
        "✅ Wrote {} generated Node.js file(s), removed {} orphaned (then run `make -C javascript types`)",
        outputs.len(),
        orphans.len()
    );
    Ok(())
}

/// The generated files that are stale, missing or orphaned.
pub fn check_up_to_date(repo_root: &Path) -> Res<Vec<PathBuf>> {
    let outputs = outputs(repo_root)?;
    let mut stale = Vec::new();
    for (path, expected) in &outputs {
        match fs::read_to_string(path) {
            Ok(actual) if &actual == expected => {},
            _ => stale.push(path.clone()),
        }
    }
    stale.extend(orphaned(repo_root, &outputs)?);
    Ok(stale)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rel_require() {
        assert_eq!(rel_require("", "_throwable"), "./_throwable");
        assert_eq!(rel_require("common/errors", "_throwable"), "../../_throwable");
        assert_eq!(rel_require("producer", "common/kafka_error"), "../common/kafka_error");
        assert_eq!(rel_require("common/errors", "common/kafka_error"), "../kafka_error");
        assert_eq!(rel_require("common/errors", "common/errors/api_error"), "./api_error");
    }

    #[test]
    fn test_jsdoc_wraps_and_escapes() {
        let doc = jsdoc("A ``code`` */ word.\n\nSecond paragraph.", "");
        assert_eq!(doc, "/**\n * A `code` *\\/ word.\n *\n * Second paragraph.\n */");
    }

    #[test]
    fn test_splice_needs_markers() {
        let p = Path::new("x/index.js");
        let s = format!("a\n{REQUIRES_BEGIN}\nold\n{REQUIRES_END}\nb");
        let got = splice(&s, REQUIRES_BEGIN, REQUIRES_END, "NEW", p).unwrap();
        assert_eq!(got, "a\nNEW\nb");
        assert!(splice("a", REQUIRES_BEGIN, REQUIRES_END, "NEW", p).is_err());
    }
}
