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

//! The generated files of the Node.js client (`javascript/lib/`, TypeScript
//! sources that `make -C javascript build` compiles into `javascript/dist/`),
//! from the same Java sources and FFI enum as the Python client's
//! (`javascript/CLAUDE.md`, "Generated files"):
//!
//!   - the error hierarchy ([`crate::error_hierarchy::js`]): one class per
//!     Python error class, in the folder of its Java package, plus
//!     `lib/_error_registry.ts`;
//!   - the config classes' constants ([`crate::config_consts::js`]);
//!   - the producer's key-type table `lib/_config_types.ts`
//!     ([`crate::config_types::js`]).
//!
//! Every generated module carries [`GENERATED_MARKER`]. A folder holding only
//! generated modules gets a generated `index.ts`; a folder that also holds
//! hand-written ones ([`MIXED_DIRS`]) keeps its hand-written `index.ts`, and the
//! generator owns the marked block in it ([`EXPORTS_BEGIN`]). A generated
//! module no generator produces any more (a `.ts`, or a `.js` left from the
//! JavaScript sources the client had before) is removed by `generate` and
//! reported by `check-generated`.

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

/// The header of a generated module: the license, then (after a blank line)
/// the marker naming what the module is generated from. tsc emits the strict
/// mode directive itself, so the module has none.
pub(crate) fn header(from: &str) -> String {
    format!(
        "{LICENSE}\n// {GENERATED_MARKER}. Produced by `cargo xtask generate-error-codes` from\n// {from};\n// `cargo xtask check-generated` checks it is up to date.\n"
    )
}

/// The lib/ folders whose `index.ts` is hand-written (they also hold
/// hand-written modules); the generator owns only the marked block there.
/// `""` is `lib/` itself.
const MIXED_DIRS: &[&str] = &["", "common", "common/config", "producer"];

/// The lib/ folders whose `index.ts` is hand-written and which hold no
/// generated module: their parents' blocks do not export them.
const HAND_WRITTEN_DIRS: &[&str] = &["common/security", "common/security/auth"];

pub(crate) const EXPORTS_BEGIN: &str = "// BEGIN GENERATED EXPORTS (cargo xtask generate-error-codes; do not edit)";
pub(crate) const EXPORTS_END: &str = "// END GENERATED EXPORTS";

/// The declaration every public `index.ts` carries (no JS is emitted for it):
/// tsc's CommonJS output sets `exports.__esModule`, so an interop default
/// import (`esModuleInterop`, Babel) reads a `default` that does not exist.
/// Declared, it makes TypeScript reject such an import (TS1192) instead of
/// letting it crash at run time; Node's own ESM default import still works.
pub(crate) const ES_MODULE_MARKER: &str = "export declare const __esModule: true;";

/// A generated file and its content.
pub(crate) type GeneratedFile = (PathBuf, String);

/// One generated public module: the class `name`, defined in
/// `lib/<dir>/<stem>.ts` (`dir` is `""` for `lib/` itself).
pub(crate) struct JsModule {
    pub dir: String,
    pub stem: String,
    pub name: String,
    pub content: String,
}

/// `import` path from a module in `from_dir` to `lib/<to>` (`to` a path under
/// lib/ without `.ts`).
pub(crate) fn rel_import(from_dir: &str, to: &str) -> String {
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

/// `text` as the lines of a TSDoc block at `indent`, wrapped at 80 columns.
/// Python's reStructuredText ``code`` becomes Markdown `code`.
pub(crate) fn tsdoc(text: &str, indent: &str) -> String {
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

/// A TypeScript (JavaScript) single-quoted string literal.
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

/// The `export` lines of a folder's index for `members` and its generated
/// child folders `subdirs`: a class from its module, a child folder as a
/// namespace.
fn export_lines(members: &[&JsModule], subdirs: &[String]) -> Vec<String> {
    let mut lines: Vec<String> = members
        .iter()
        .map(|m| format!("export {{ {} }} from './{}';", m.name, m.stem))
        .collect();
    lines.extend(subdirs.iter().map(|d| {
        let n = last_segment(d);
        format!("export * as {n} from './{n}';")
    }));
    lines
}

/// The marked block of a hand-written `index.ts` holding `lines`.
fn exports_block(lines: &[String]) -> String {
    let mut block = format!("{EXPORTS_BEGIN}\n");
    for l in lines {
        block.push_str(l);
        block.push('\n');
    }
    block.push_str(EXPORTS_END);
    block
}

/// The `index.ts` files and blocks for `modules`.
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
    let index_path = |dir: &str| -> PathBuf {
        if dir.is_empty() {
            lib.join("index.ts")
        } else {
            lib.join(dir).join("index.ts")
        }
    };
    let mut out = Vec::new();
    let all_dirs: BTreeSet<String> = by_dir.keys().cloned().chain(generated_dirs.iter().cloned()).collect();
    let mut mixed_seen: BTreeSet<String> = BTreeSet::new();
    for dir in &all_dirs {
        let mut members: Vec<&JsModule> = by_dir.get(dir).cloned().unwrap_or_default();
        members.sort_by(|a, b| a.name.cmp(&b.name));
        let lines = export_lines(&members, &children(dir));
        let path = index_path(dir);
        if MIXED_DIRS.contains(&dir.as_str()) {
            mixed_seen.insert(dir.clone());
            let current = fs::read_to_string(&path).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
            out.push((
                path.clone(),
                splice(&current, EXPORTS_BEGIN, EXPORTS_END, &exports_block(&lines), &path)?,
            ));
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
        for l in &lines {
            text.push_str(l);
            text.push('\n');
        }
        if !lines.is_empty() {
            text.push('\n');
        }
        text.push_str(&format!(
            "// tsc's CommonJS output sets `exports.__esModule`: declared, a default import\n\
             // under `esModuleInterop` is a compile error rather than `undefined`.\n\
             {ES_MODULE_MARKER}\n"
        ));
        out.push((path, text));
    }
    // A mixed folder with nothing generated keeps a block of its generated
    // child folders (empty when it has none).
    for dir in MIXED_DIRS {
        if mixed_seen.contains(*dir) {
            continue;
        }
        let path = index_path(dir);
        let current = fs::read_to_string(&path).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
        let lines = export_lines(&[], &children(dir));
        out.push((
            path.clone(),
            splice(&current, EXPORTS_BEGIN, EXPORTS_END, &exports_block(&lines), &path)?,
        ));
    }
    Ok(out)
}

/// `current` with the text from `begin` to `end` (both included) replaced by
/// `block`; the hand-written file must carry both markers.
fn splice(current: &str, begin: &str, end: &str, block: &str, path: &Path) -> Res<String> {
    match (current.find(begin), current.find(end)) {
        (Some(a), Some(b)) if a < b => Ok(format!("{}{}{}", &current[..a], block, &current[b + end.len()..])),
        _ => anyhow::bail!(
            "{}: the hand-written index.ts lacks the `{begin}` / `{end}` markers",
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
            anyhow::bail!("javascript: two generated modules for lib/{}/{}.ts", m.dir, m.stem);
        }
        let dir = if m.dir.is_empty() {
            lib.clone()
        } else {
            lib.join(&m.dir)
        };
        out.push((dir.join(format!("{}.ts", m.stem)), m.content.clone()));
    }
    out.extend(index_outputs(repo_root, &modules)?);
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

/// Generated modules under lib/ that no generator produces any more: `.ts`
/// files carrying [`GENERATED_MARKER`] that are not among `outputs`, and any
/// `.js` one (the generated JavaScript modules the client had before its
/// sources became TypeScript).
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
            if !(name.ends_with(".ts") || name.ends_with(".js")) || expected.contains(&path) {
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

/// Write every generated file and remove the orphaned ones. `make -C
/// javascript build` compiles them (tsc) into `javascript/dist/`.
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
        "✅ Wrote {} generated Node.js file(s), removed {} orphaned",
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
    fn test_rel_import() {
        assert_eq!(rel_import("", "_throwable"), "./_throwable");
        assert_eq!(rel_import("common/errors", "_throwable"), "../../_throwable");
        assert_eq!(rel_import("producer", "common/kafka_error"), "../common/kafka_error");
        assert_eq!(rel_import("common/errors", "common/kafka_error"), "../kafka_error");
        assert_eq!(rel_import("common/errors", "common/errors/api_error"), "./api_error");
    }

    #[test]
    fn test_tsdoc_wraps_and_escapes() {
        let doc = tsdoc("A ``code`` */ word.\n\nSecond paragraph.", "");
        assert_eq!(doc, "/**\n * A `code` *\\/ word.\n *\n * Second paragraph.\n */");
    }

    #[test]
    fn test_splice_needs_markers() {
        let p = Path::new("x/index.ts");
        let s = format!("a\n{EXPORTS_BEGIN}\nold\n{EXPORTS_END}\nb");
        let got = splice(&s, EXPORTS_BEGIN, EXPORTS_END, "NEW", p).unwrap();
        assert_eq!(got, "a\nNEW\nb");
        assert!(splice("a", EXPORTS_BEGIN, EXPORTS_END, "NEW", p).is_err());
    }

    #[test]
    fn test_export_lines_and_block() {
        let m = JsModule {
            dir: "producer".into(),
            stem: "x_error".into(),
            name: "XError".into(),
            content: String::new(),
        };
        let lines = export_lines(&[&m], &["common/errors".to_string()]);
        assert_eq!(
            lines,
            [
                "export { XError } from './x_error';",
                "export * as errors from './errors';"
            ]
        );
        assert_eq!(
            exports_block(&lines),
            format!("{EXPORTS_BEGIN}\n{}\n{}\n{EXPORTS_END}", lines[0], lines[1])
        );
        assert_eq!(exports_block(&[]), format!("{EXPORTS_BEGIN}\n{EXPORTS_END}"));
    }

    #[test]
    fn test_generated_index_declares_the_es_module_marker() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().and_then(Path::parent).unwrap();
        let all = outputs(root).unwrap();
        let errors = all
            .iter()
            .find(|(p, _)| p.ends_with("javascript/lib/common/errors/index.ts"))
            .unwrap();
        assert!(errors.1.ends_with(&format!("{ES_MODULE_MARKER}\n")));
        assert!(errors.1.contains("export { ApiError } from './api_error';\n"));
    }

    #[test]
    fn test_header_has_no_strict_directive() {
        let h = header("x");
        assert!(h.starts_with(LICENSE));
        assert!(h.contains(GENERATED_MARKER));
        assert!(!h.contains("use strict"));
    }
}
