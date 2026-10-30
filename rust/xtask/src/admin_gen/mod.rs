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

//! Shared machinery for generating the admin client's Python classes from the
//! Java sources (CLAUDE.md, Python Binding Conventions).
//!
//! - [`parse_java_class`] reads one top-level Java class into its members, in
//!   declaration order, with each member's Javadoc, preceding `//` comments,
//!   annotations and (normalized) body. It reads only the shape; deciding
//!   whether a body is a known pattern is the caller's job.
//! - [`Stop`] is the "stop on unknown pattern" report: a class whose Java uses a
//!   body or type outside the known patterns is not guessed at; generation of
//!   that class stops and the reason is listed.
//! - The Python layout helpers ([`wrap_signature`], [`docstring`], [`Imports`])
//!   produce the exact layout of the hand-written reference set
//!   (`python/confluent_kafka/admin/_reference_set.md`), so a generated file
//!   can be compared byte for byte with it ([`first_difference`]).
//! - [`Prose`] holds the docstring text that cannot be derived from Javadoc
//!   (the module notes, Javadoc the reference set reworded); everything else
//!   is derived.

pub mod options;
pub mod results;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::path::Path;

pub type Res<T> = anyhow::Result<T>;

/// The Java admin package, relative to the repository root.
pub const ADMIN_JAVA: &str = "kafka/clients/src/main/java/org/apache/kafka/clients/admin";
/// The Java client source root, relative to the repository root.
pub const JAVA_ROOT: &str = "kafka/clients/src/main/java";
/// The Python admin package, relative to the repository root.
pub const ADMIN_PY: &str = "python/confluent_kafka/admin";
/// The deprecated Java API list (`cargo xtask java-deprecated`).
pub const DEPRECATED_LIST: &str = "design/current/java-deprecated.txt";
/// Line width of generated Python code.
pub const WIDTH: usize = 100;
/// Line width of generated docstrings.
pub const DOC_WIDTH: usize = 88;

/// The Apache 2.0 header every generated Python file starts with.
pub const LICENSE: &str = "# Copyright 2025 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the \"License\");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an \"AS IS\" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
";

// ---------------------------------------------------------------------------
// Stop on unknown pattern
// ---------------------------------------------------------------------------

/// Generation of one class stopped: its Java has a construct outside the
/// known patterns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stop {
    pub class: String,
    pub reason: String,
}

impl fmt::Display for Stop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.class, self.reason)
    }
}

/// Build a [`Stop`] for `class`.
pub fn stop(class: &str, reason: impl Into<String>) -> Stop {
    Stop { class: class.to_string(), reason: reason.into() }
}

// ---------------------------------------------------------------------------
// Java reading
// ---------------------------------------------------------------------------

/// A Java parameter: type (whitespace normalized) and name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JParam {
    pub ty: String,
    pub name: String,
}

/// What a class member is.
#[derive(Clone, Debug)]
pub enum JKind {
    Field {
        ty: String,
        name: String,
        init: Option<String>,
    },
    /// `body` normalized ([`normalize`]); `raw` as written, for the layout of
    /// Java's own line breaks.
    Method {
        ret: String,
        name: String,
        params: Vec<JParam>,
        body: Option<String>,
        raw: Option<String>,
    },
    Ctor {
        params: Vec<JParam>,
        body: String,
    },
    /// A nested type or initializer block: its header.
    Other(String),
}

/// One member of a Java class, in declaration order.
#[derive(Clone, Debug)]
pub struct JMember {
    /// The `/** ... */` block right before the member.
    pub javadoc: Option<String>,
    /// The `//` comments right before the member, text after `//` trimmed.
    pub comments: Vec<String>,
    /// Annotation names without `@` and arguments (`Override`, `Deprecated`).
    pub annotations: Vec<String>,
    pub modifiers: Vec<String>,
    pub kind: JKind,
    /// The source text of the declaration (modifiers, type, name, initializer).
    pub decl: String,
}

impl JMember {
    pub fn is_public(&self) -> bool {
        self.modifiers.iter().any(|m| m == "public")
    }

    pub fn is_static(&self) -> bool {
        self.modifiers.iter().any(|m| m == "static")
    }

    pub fn has_annotation(&self, name: &str) -> bool {
        self.annotations.iter().any(|a| a == name)
    }
}

/// One top-level Java class.
#[derive(Clone, Debug)]
pub struct JClass {
    pub package: String,
    pub name: String,
    pub modifiers: Vec<String>,
    /// The `extends` clause as written, generics kept (`AbstractOptions<X>`).
    pub extends: Option<String>,
    pub javadoc: Option<String>,
    /// `import` lines: simple name -> fully-qualified name.
    pub imports: BTreeMap<String, String>,
    pub members: Vec<JMember>,
}

impl JClass {
    pub fn fqcn(&self) -> String {
        format!("{}.{}", self.package, self.name)
    }

    pub fn is_abstract(&self) -> bool {
        self.modifiers.iter().any(|m| m == "abstract")
    }

    /// The fully-qualified name of a simple type name used in this class:
    /// imported, or else in the class's own package.
    pub fn resolve(&self, simple: &str) -> String {
        self.imports
            .get(simple)
            .cloned()
            .unwrap_or_else(|| format!("{}.{}", self.package, simple))
    }
}

const MODIFIERS: [&str; 10] = [
    "public",
    "protected",
    "private",
    "static",
    "final",
    "abstract",
    "synchronized",
    "volatile",
    "transient",
    "default",
];

/// Collapse whitespace runs to one space and drop spaces the layout of the
/// Java source adds around `.`, `(` and `)`, so bodies compare as text.
pub fn normalize(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut in_str: Option<char> = None;
    let mut pending_space = false;
    let mut prev = ' ';
    let mut escaped = false;
    for c in s.chars() {
        if let Some(q) = in_str {
            out.push(c);
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == q {
                in_str = None;
            }
            prev = c;
            continue;
        }
        if c.is_whitespace() {
            pending_space = true;
            continue;
        }
        if pending_space {
            pending_space = false;
            let drop = matches!(c, '.' | ')' | ';') || matches!(prev, '(' | '.' | ' ') || out.is_empty();
            if !drop {
                out.push(' ');
            }
        }
        if c == '"' || c == '\'' {
            in_str = Some(c);
        }
        out.push(c);
        prev = c;
    }
    out
}

/// The index just past the bracket closing the one at `open` (`(`, `{`, `<`),
/// skipping strings, chars and comments.
fn matching(b: &[u8], open: usize) -> Res<usize> {
    let (o, c) = match b[open] {
        b'(' => (b'(', b')'),
        b'{' => (b'{', b'}'),
        b'<' => (b'<', b'>'),
        _ => anyhow::bail!("not a bracket"),
    };
    let mut depth = 0usize;
    let mut i = open;
    while i < b.len() {
        match b[i] {
            b'"' | b'\'' => {
                let q = b[i];
                i += 1;
                while i < b.len() && b[i] != q {
                    if b[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
            },
            b'/' if b.get(i + 1) == Some(&b'/') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            },
            b'/' if b.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                    i += 1;
                }
                i += 1;
            },
            x if x == o => depth += 1,
            x if x == c => {
                depth -= 1;
                if depth == 0 {
                    return Ok(i + 1);
                }
            },
            _ => {},
        }
        i += 1;
    }
    anyhow::bail!("unbalanced brackets")
}

/// Split `s` at `sep` outside brackets and strings.
pub fn split_top(s: &str, sep: char) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut cur = String::new();
    let mut in_str: Option<char> = None;
    let mut escaped = false;
    for c in s.chars() {
        if let Some(q) = in_str {
            cur.push(c);
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == q {
                in_str = None;
            }
            continue;
        }
        match c {
            '"' | '\'' => in_str = Some(c),
            '(' | '<' | '{' | '[' => depth += 1,
            ')' | '>' | '}' | ']' => depth -= 1,
            _ => {},
        }
        if c == sep && depth == 0 {
            out.push(cur.trim().to_string());
            cur.clear();
        } else {
            cur.push(c);
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

fn parse_params(s: &str) -> Res<Vec<JParam>> {
    let mut out = Vec::new();
    for part in split_top(s, ',') {
        let words: Vec<&str> = part
            .split_whitespace()
            .filter(|w| *w != "final" && !w.starts_with('@'))
            .collect();
        let joined = words.join(" ");
        let (ty, name) = joined
            .rsplit_once(' ')
            .ok_or_else(|| anyhow::anyhow!("cannot parse parameter `{part}`"))?;
        out.push(JParam { ty: normalize(ty), name: name.to_string() });
    }
    Ok(out)
}

/// Peel leading annotations and modifiers off a member header.
fn peel(header: &str) -> (Vec<String>, Vec<String>, String) {
    let mut annotations = Vec::new();
    let mut modifiers = Vec::new();
    let mut rest = header.trim();
    loop {
        if let Some(after) = rest.strip_prefix('@') {
            let name: String = after.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
            let mut tail = &after[name.len()..];
            if tail.starts_with('(') {
                let end = matching(tail.as_bytes(), 0).unwrap_or(tail.len());
                tail = &tail[end..];
            }
            annotations.push(name);
            rest = tail.trim_start();
            continue;
        }
        let word: String = rest.chars().take_while(|c| c.is_alphanumeric()).collect();
        if MODIFIERS.contains(&word.as_str()) && rest[word.len()..].starts_with(char::is_whitespace) {
            modifiers.push(word.clone());
            rest = rest[word.len()..].trim_start();
            continue;
        }
        break;
    }
    (annotations, modifiers, rest.to_string())
}

/// Read one Java source file's top-level class.
pub fn parse_java_class(raw: &str) -> Res<JClass> {
    let package = raw
        .lines()
        .find_map(|l| {
            l.trim()
                .strip_prefix("package ")
                .map(|p| p.trim_end_matches(';').trim().to_string())
        })
        .ok_or_else(|| anyhow::anyhow!("no package declaration"))?;
    let mut imports = BTreeMap::new();
    for l in raw.lines() {
        if let Some(i) = l.trim().strip_prefix("import ") {
            let i = i.trim_end_matches(';').trim();
            if i.starts_with("static ") {
                continue;
            }
            if let Some(simple) = i.rsplit('.').next() {
                imports.insert(simple.to_string(), i.to_string());
            }
        }
    }
    // The top-level declaration: the first line starting with `public ... class`.
    let mut offset = 0;
    let mut decl_at = None;
    for line in raw.split_inclusive('\n') {
        let t = line.trim_start();
        if t.starts_with("public ") && (t.contains(" class ") || t.contains(" interface ")) {
            decl_at = Some(offset + (line.len() - t.len()));
            break;
        }
        offset += line.len();
    }
    let decl_at = decl_at.ok_or_else(|| anyhow::anyhow!("no public class declaration"))?;
    let b = raw.as_bytes();
    let open = decl_at + raw[decl_at..].find('{').ok_or_else(|| anyhow::anyhow!("no class body"))?;
    let header = normalize(&raw[decl_at..open]);
    let (_, modifiers, rest) = peel(&header);
    let rest = rest
        .strip_prefix("class ")
        .ok_or_else(|| anyhow::anyhow!("not a class: `{header}`"))?;
    let name: String = rest.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
    let extends = rest
        .split_once(" extends ")
        .map(|(_, e)| e.split(" implements ").next().unwrap_or(e).trim().to_string());
    let javadoc = raw[..decl_at].rfind("/**").and_then(|s| {
        let e = s + raw[s..].find("*/")? + 2;
        // Only annotations may stand between the Javadoc and the declaration.
        let between = raw[e..decl_at].trim();
        (between.is_empty() || between.starts_with('@')).then(|| raw[s..e].to_string())
    });
    let close = matching(b, open)?;
    let members = parse_members(&raw[open + 1..close - 1])?;
    Ok(JClass { package, name, modifiers, extends, javadoc, imports, members })
}

fn parse_members(body: &str) -> Res<Vec<JMember>> {
    let b = body.as_bytes();
    let mut out = Vec::new();
    let mut javadoc: Option<String> = None;
    let mut comments: Vec<String> = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if body[i..].starts_with("/**") {
            let e = i + body[i..].find("*/").ok_or_else(|| anyhow::anyhow!("open Javadoc"))? + 2;
            javadoc = Some(body[i..e].to_string());
            i = e;
            continue;
        }
        if body[i..].starts_with("/*") {
            i += body[i..].find("*/").ok_or_else(|| anyhow::anyhow!("open comment"))? + 2;
            continue;
        }
        if body[i..].starts_with("//") {
            let e = body[i..].find('\n').map_or(b.len(), |e| i + e);
            comments.push(body[i + 2..e].trim().to_string());
            i = e;
            continue;
        }
        // A member ends at `;` or at the end of its `{...}` block, outside
        // parentheses.
        let start = i;
        let mut j = i;
        let (end, block) = loop {
            if j >= b.len() {
                anyhow::bail!("unterminated member `{}`", body[start..].trim());
            }
            match b[j] {
                b'"' | b'\'' => {
                    let q = b[j];
                    j += 1;
                    while j < b.len() && b[j] != q {
                        if b[j] == b'\\' {
                            j += 1;
                        }
                        j += 1;
                    }
                    j += 1;
                },
                b'(' => j = matching(b, j)?,
                b';' => break (j + 1, None),
                b'{' => {
                    let e = matching(b, j)?;
                    break (e, Some((j, e)));
                },
                _ => j += 1,
            }
        };
        let header_src = match block {
            Some((o, _)) => &body[start..o],
            None => &body[start..end - 1],
        };
        let header = normalize(header_src);
        let (annotations, modifiers, rest) = peel(&header);
        let kind = classify(&rest, block.map(|(o, e)| body[o + 1..e - 1].to_string()))?;
        out.push(JMember {
            javadoc: javadoc.take(),
            comments: std::mem::take(&mut comments),
            annotations,
            modifiers,
            kind,
            decl: header.clone(),
        });
        i = end;
    }
    Ok(out)
}

fn classify(rest: &str, block: Option<String>) -> Res<JKind> {
    let paren = rest.find('(');
    let eq = rest.find('=');
    let is_call = paren.is_some_and(|p| eq.is_none_or(|e| p < e));
    if rest.starts_with("class ") || rest.starts_with("enum ") || rest.starts_with("interface ") || rest.is_empty() {
        return Ok(JKind::Other(rest.to_string()));
    }
    if !is_call {
        let (decl, init) = match rest.split_once('=') {
            Some((d, e)) => (d.trim().to_string(), Some(normalize(e.trim()))),
            None => (rest.trim().to_string(), None),
        };
        let (ty, name) = decl
            .rsplit_once(' ')
            .ok_or_else(|| anyhow::anyhow!("cannot parse field `{rest}`"))?;
        return Ok(JKind::Field { ty: normalize(ty), name: name.to_string(), init });
    }
    let p = paren.unwrap_or(0);
    let close = matching(rest.as_bytes(), p)?;
    let params = parse_params(&rest[p + 1..close - 1])?;
    let before = rest[..p].trim();
    match before.rsplit_once(' ') {
        Some((ret, name)) => Ok(JKind::Method {
            ret: normalize(ret),
            name: name.to_string(),
            params,
            body: block.as_ref().map(|b| normalize(&strip_line_comments(b))),
            raw: block,
        }),
        None => Ok(JKind::Ctor { params, body: normalize(&strip_line_comments(&block.unwrap_or_default())) }),
    }
}

fn strip_line_comments(s: &str) -> String {
    s.lines()
        .map(|l| l.split_once("//").map_or(l, |(a, _)| a))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The statements of a normalized body, `;`-terminated, outside brackets.
pub fn statements(body: &str) -> Vec<String> {
    split_top(body, ';').into_iter().filter(|s| !s.is_empty()).collect()
}

/// The deprecated items of [`DEPRECATED_LIST`] (`pkg.Class#method(types)`, or
/// a whole class).
pub fn deprecated_items(repo_root: &Path) -> Res<BTreeSet<String>> {
    let text = fs::read_to_string(repo_root.join(DEPRECATED_LIST))?;
    Ok(text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_string)
        .collect())
}

/// The erasure of a Java type, as the deprecated list writes it (`long`,
/// `Collection`).
pub fn erasure(ty: &str) -> String {
    ty.split('<').next().unwrap_or(ty).trim().to_string()
}

// ---------------------------------------------------------------------------
// Names
// ---------------------------------------------------------------------------

/// `camelCase` / `PascalCase` -> `snake_case`.
pub fn snake(name: &str) -> String {
    let chars: Vec<char> = name.chars().collect();
    let mut out = String::new();
    for (i, &c) in chars.iter().enumerate() {
        if c.is_uppercase() {
            let prev_lower = i > 0 && (chars[i - 1].is_lowercase() || chars[i - 1].is_ascii_digit());
            let next_lower = chars.get(i + 1).is_some_and(|n| n.is_lowercase());
            if i > 0 && (prev_lower || (next_lower && chars[i - 1].is_uppercase())) {
                out.push('_');
            }
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Javadoc -> docstring text
// ---------------------------------------------------------------------------

/// A Javadoc block as docstring material: its paragraphs and its `@param` /
/// `@return` tags.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Doc {
    pub paragraphs: Vec<String>,
    /// `(name, text)` per `@param`, Java names.
    pub params: Vec<(String, String)>,
    pub returns: Option<String>,
}

impl Doc {
    pub fn is_empty(&self) -> bool {
        self.paragraphs.is_empty() && self.params.is_empty() && self.returns.is_none()
    }
}

/// Inline Javadoc markup to docstring markup: `{@code null}` -> ``None``,
/// `{@link Admin#fooBar(X)}` -> ``Admin.foo_bar()``, HTML dropped.
pub fn inline_markup(text: &str) -> String {
    let mut text = text.to_string();
    while let Some(start) = text.find("{@") {
        let Some(end_rel) = text[start..].find('}') else { break };
        let inside = text[start + 2..start + end_rel].to_string();
        let (tag, rest) = inside.split_once(' ').unwrap_or((inside.as_str(), ""));
        let rest = rest.trim();
        let shown = if tag == "link" || tag == "linkplain" {
            let (reference, label) = match rest.find([' ', '(']) {
                Some(k) if rest.as_bytes()[k] == b' ' => (&rest[..k], rest[k..].trim()),
                Some(k) => {
                    let close = rest[k..].find(')').map_or(rest.len(), |c| k + c + 1);
                    (&rest[..close], rest[close..].trim())
                },
                None => (rest, ""),
            };
            if label.is_empty() {
                let base = reference.split('(').next().unwrap_or(reference);
                let shown = match base.split_once('#') {
                    Some((cls, member)) => {
                        let cls = if cls == "AdminClient" { "Admin" } else { cls };
                        let member = snake(member);
                        if cls.is_empty() {
                            format!("{member}()")
                        } else {
                            format!("{cls}.{member}()")
                        }
                    },
                    None => base.to_string(),
                };
                format!("``{shown}``")
            } else {
                label.to_string()
            }
        } else if tag == "code" || tag == "literal" {
            match rest {
                "null" => "``None``".to_string(),
                "true" => "``True``".to_string(),
                "false" => "``False``".to_string(),
                _ => format!("``{rest}``"),
            }
        } else {
            rest.to_string()
        };
        text.replace_range(start..start + end_rel + 1, &shown);
    }
    let mut out = String::new();
    let mut in_tag = false;
    for c in text.chars() {
        match c {
            '<' => in_tag = true,
            '>' if in_tag => in_tag = false,
            _ if !in_tag => out.push(c),
            _ => {},
        }
    }
    let out = out.replace("&gt;", ">").replace("&lt;", "<").replace("&amp;", "&");
    dedupe_links(&out)
}

/// Two Java overloads linked side by side read as one Python name:
/// "``A.b()`` and ``A.b()``" -> "``A.b()``".
fn dedupe_links(text: &str) -> String {
    let mut out = text.to_string();
    let mut from = 0;
    while let Some(rel) = out[from..].find("`` and ``") {
        let mid = from + rel;
        let Some(open) = out[..mid].rfind("``") else { break };
        let left = out[open..mid + 2].to_string();
        if out[mid + 7..].starts_with(&left) {
            out.replace_range(mid + 2..mid + 7 + left.len(), "");
            from = open;
        } else {
            from = mid + 2;
        }
    }
    out
}

/// Read a `/** ... */` block.
pub fn read_doc(block: &str) -> Doc {
    let inner = block.trim().trim_start_matches("/**").trim_end_matches("*/");
    let mut body: Vec<String> = Vec::new();
    let mut tags: Vec<String> = Vec::new();
    for line in inner.lines() {
        let l = line.trim().trim_start_matches('*').trim();
        if l.starts_with('@') {
            tags.push(l.to_string());
        } else if let Some(last) = tags.last_mut() {
            last.push(' ');
            last.push_str(l);
        } else {
            body.push(l.replace("<p>", "\n\n"));
        }
    }
    let text = body.join("\n");
    let mut paragraphs = Vec::new();
    for para in text.split("\n\n") {
        let joined = inline_markup(para).split_whitespace().collect::<Vec<_>>().join(" ");
        if !joined.is_empty() {
            paragraphs.push(joined);
        }
    }
    let mut doc = Doc { paragraphs, ..Doc::default() };
    for tag in tags {
        let clean = inline_markup(&tag).split_whitespace().collect::<Vec<_>>().join(" ");
        if let Some(rest) = clean.strip_prefix("@param ") {
            let (name, text) = rest.split_once(' ').unwrap_or((rest, ""));
            doc.params.push((name.to_string(), text.trim().to_string()));
        } else if let Some(rest) = clean.strip_prefix("@return") {
            doc.returns = Some(rest.trim().to_string());
        }
    }
    doc
}

// ---------------------------------------------------------------------------
// Python layout
// ---------------------------------------------------------------------------

/// Lay out `head item, item, ...tail` at `indent` within [`WIDTH`]: one line
/// when it fits (not counting the last `slack` characters of `tail`, the
/// ` ...` of a stub), else items aligned after `head`, else a hanging indent
/// of `hang` columns with `head` alone on the first line.
pub fn wrap_signature(indent: usize, head: &str, items: &[String], tail: &str, hang: usize, slack: usize) -> String {
    let pad = " ".repeat(indent);
    let one = format!("{pad}{head}{}{tail}", items.join(", "));
    if one.chars().count() <= WIDTH + slack {
        return one;
    }
    let texts: Vec<String> = items
        .iter()
        .enumerate()
        .map(|(k, it)| {
            if k + 1 == items.len() {
                format!("{it}{tail}")
            } else {
                format!("{it},")
            }
        })
        .collect();
    let col = indent + head.chars().count();
    if let Some(aligned) = fill(&format!("{pad}{head}"), col, &texts, true) {
        return aligned;
    }
    let first = format!("{pad}{head}");
    let rest = fill(&" ".repeat(indent + hang), indent + hang, &texts, false).unwrap_or_default();
    format!("{first}\n{rest}")
}

/// Fill `texts` after `start` (already `start` wide); continuation lines
/// start at column `col`. `aligned` fills the first line greedily and then
/// puts one text per line, failing when a text does not fit; otherwise the
/// lines are filled greedily.
fn fill(start: &str, col: usize, texts: &[String], aligned: bool) -> Option<String> {
    let mut lines: Vec<String> = Vec::new();
    let mut cur = start.to_string();
    let mut empty = true;
    let mut first_line = true;
    for t in texts {
        let add = if empty {
            t.chars().count()
        } else {
            t.chars().count() + 1
        };
        let greedy = !aligned || first_line;
        if (empty || greedy) && cur.chars().count() + add <= WIDTH {
            if !empty {
                cur.push(' ');
            }
            cur.push_str(t);
            empty = false;
            continue;
        }
        if empty {
            if aligned {
                return None;
            }
            cur.push_str(t);
            empty = false;
            continue;
        }
        lines.push(cur);
        first_line = false;
        cur = format!("{}{t}", " ".repeat(col));
        if aligned && cur.chars().count() > WIDTH {
            return None;
        }
    }
    lines.push(cur);
    Some(lines.join("\n"))
}

/// One piece of an expression laid out by [`wrap_expression`]: `lead` is
/// written before `text` unless the piece opens the expression (`+ `,
/// `and `, `== `).
#[derive(Clone, Debug)]
pub struct Piece {
    pub lead: String,
    pub text: String,
}

/// `prefix` + the expression on one line when it fits; else in parentheses,
/// filled greedily by atom (an atom moves to the next line whole, and is cut
/// into its pieces only when it does not fit on a line of its own), with
/// continuation lines aligned after the opening parenthesis.
pub fn wrap_expression(prefix: &str, atoms: &[Vec<Piece>]) -> String {
    let flat: Vec<&Piece> = atoms.iter().flatten().collect();
    let render = |pieces: &[&Piece], first: bool| {
        let mut s = String::new();
        for (k, p) in pieces.iter().enumerate() {
            if k > 0 || !first {
                s.push_str(&p.lead);
            }
            s.push_str(&p.text);
            if k + 1 < pieces.len() {
                s.push(' ');
            }
        }
        s
    };
    let one = format!("{prefix}{}", render(&flat, true));
    if one.chars().count() <= WIDTH {
        return one;
    }
    let col = prefix.chars().count() + 1;
    let pad = " ".repeat(col);
    let mut lines: Vec<String> = Vec::new();
    let mut cur = format!("{prefix}(");
    let mut at_start = true;
    let mut opened = true;
    let n = atoms.len();
    for (a, atom) in atoms.iter().enumerate() {
        let pieces: Vec<&Piece> = atom.iter().collect();
        let close = if a + 1 == n { ")" } else { "" };
        let whole = render(&pieces, opened);
        let joined = if at_start {
            format!("{whole}{close}")
        } else {
            format!(" {whole}{close}")
        };
        if cur.chars().count() + joined.chars().count() <= WIDTH {
            cur.push_str(&joined);
            at_start = false;
            opened = false;
            continue;
        }
        if !at_start && col + whole.chars().count() + close.len() <= WIDTH {
            lines.push(std::mem::replace(&mut cur, format!("{pad}{whole}{close}")));
            opened = false;
            continue;
        }
        for (k, p) in pieces.iter().enumerate() {
            let c = if k + 1 == pieces.len() { close } else { "" };
            let text = render(&[*p], opened);
            let joined = if at_start {
                format!("{text}{c}")
            } else {
                format!(" {text}{c}")
            };
            if at_start || cur.chars().count() + joined.chars().count() <= WIDTH {
                cur.push_str(&joined);
            } else {
                lines.push(std::mem::replace(&mut cur, format!("{pad}{text}{c}")));
            }
            at_start = false;
            opened = false;
        }
    }
    lines.push(cur);
    lines.join("\n")
}

/// Greedy word wrap of `text` at [`DOC_WIDTH`], the first line prefixed with
/// `first`, the others with `indent` spaces.
pub fn wrap_words(text: &str, indent: usize, first: &str) -> Vec<String> {
    wrap_hanging(text, indent, first, indent)
}

/// [`wrap_words`] with the lines after the first indented by `hang` spaces.
fn wrap_hanging(text: &str, indent: usize, first: &str, hang: usize) -> Vec<String> {
    let pad = " ".repeat(indent);
    let rest = " ".repeat(hang);
    let mut lines = Vec::new();
    let mut cur = format!("{pad}{first}");
    let mut empty = true;
    for w in text.split(' ') {
        let add = if empty {
            w.chars().count()
        } else {
            w.chars().count() + 1
        };
        if !empty && cur.chars().count() + add > DOC_WIDTH {
            lines.push(cur);
            cur = format!("{rest}{w}");
            continue;
        }
        if !empty {
            cur.push(' ');
        }
        cur.push_str(w);
        empty = false;
    }
    lines.push(cur);
    lines
}

/// A docstring at `indent`: the paragraphs, then `fields` (`:param x: …`)
/// one per line, a field's wrapped lines indented four more, as reST reads a
/// field's body. One paragraph and no fields closes on its last line;
/// otherwise the closing quotes stand alone.
pub fn docstring(indent: usize, paragraphs: &[String], fields: &[String]) -> String {
    let pad = " ".repeat(indent);
    let mut lines: Vec<String> = Vec::new();
    let mut first = true;
    for p in paragraphs {
        if !first {
            lines.push(String::new());
        }
        lines.extend(wrap_words(p, indent, if first { "\"\"\"" } else { "" }));
        first = false;
    }
    if !fields.is_empty() {
        if !first {
            lines.push(String::new());
        }
        for f in fields {
            lines.extend(wrap_hanging(f, indent, if first { "\"\"\"" } else { "" }, indent + 4));
            first = false;
        }
    }
    if paragraphs.len() == 1 && fields.is_empty() {
        let last = lines.pop().unwrap_or_default();
        if last.chars().count() + 3 <= DOC_WIDTH {
            lines.push(format!("{last}\"\"\""));
        } else {
            let (head, tail) = last.rsplit_once(' ').unwrap_or(("", &last));
            if head.trim().is_empty() {
                lines.push(format!("{last}\"\"\""));
            } else {
                lines.push(head.to_string());
                lines.push(format!("{pad}{tail}\"\"\""));
            }
        }
    } else {
        lines.push(format!("{pad}\"\"\""));
    }
    lines.join("\n")
}

/// Indent a stored docstring literal (see [`Prose`]) by `indent`.
pub fn indent_literal(lines: &[String], indent: usize) -> String {
    let pad = " ".repeat(indent);
    lines
        .iter()
        .map(|l| {
            if l.is_empty() {
                String::new()
            } else {
                format!("{pad}{l}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The module docstring's first line(s): ``"""``X``: Java's ``fqcn``."""``,
/// wrapped at [`WIDTH`].
pub fn module_title(class: &str, fqcn: &str) -> String {
    let one = format!("\"\"\"``{class}``: Java's ``{fqcn}``.");
    if one.chars().count() <= WIDTH {
        one
    } else {
        format!("\"\"\"``{class}``: Java's\n``{fqcn}``.")
    }
}

/// The imports of one Python module, grouped and sorted as isort does
/// (`profile = black`, `order_by_type`).
#[derive(Clone, Debug, Default)]
pub struct Imports {
    /// `import x` lines (stdlib).
    pub plain: BTreeSet<String>,
    /// stdlib `from m import n`.
    pub stdlib: BTreeMap<String, BTreeSet<String>>,
    /// first-party `from m import n`.
    pub first: BTreeMap<String, BTreeSet<String>>,
    /// `if TYPE_CHECKING:` imports.
    pub checking: BTreeMap<String, BTreeSet<String>>,
}

impl Imports {
    pub fn std(&mut self, module: &str, name: &str) {
        self.stdlib.entry(module.to_string()).or_default().insert(name.to_string());
    }

    pub fn first(&mut self, module: &str, name: &str) {
        self.first.entry(module.to_string()).or_default().insert(name.to_string());
    }

    pub fn checking(&mut self, module: &str, name: &str) {
        self.checking.entry(module.to_string()).or_default().insert(name.to_string());
    }

    fn names(names: &BTreeSet<String>) -> String {
        // isort's order_by_type: CONSTANTS, then Classes, then functions.
        let rank = |n: &str| {
            if n.chars().all(|c| c.is_uppercase() || c == '_' || c.is_ascii_digit()) {
                0
            } else if n.starts_with(char::is_uppercase) {
                1
            } else {
                2
            }
        };
        let mut v: Vec<&String> = names.iter().collect();
        v.sort_by(|a, b| rank(a).cmp(&rank(b)).then(a.cmp(b)));
        v.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ")
    }

    fn from_lines(map: &BTreeMap<String, BTreeSet<String>>, indent: &str) -> Vec<String> {
        map.iter()
            .map(|(m, n)| format!("{indent}from {m} import {}", Self::names(n)))
            .collect()
    }

    /// The import block after `from __future__ import annotations` (if any),
    /// each group followed by one blank line. `between` goes after the stdlib
    /// group (the stub's `Self` block).
    pub fn render(&self, between: &str) -> String {
        let mut out = String::new();
        let mut std: Vec<String> = self.plain.iter().map(|p| format!("import {p}")).collect();
        std.extend(Self::from_lines(&self.stdlib, ""));
        if !std.is_empty() {
            out.push_str(&std.join("\n"));
            out.push_str("\n\n");
        }
        out.push_str(between);
        let first = Self::from_lines(&self.first, "");
        if !first.is_empty() {
            out.push_str(&first.join("\n"));
            out.push_str("\n\n");
        }
        if !self.checking.is_empty() {
            out.push_str("if TYPE_CHECKING:\n");
            out.push_str(&Self::from_lines(&self.checking, "    ").join("\n"));
            out.push_str("\n\n");
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Prose
// ---------------------------------------------------------------------------

/// Docstring text that is not derived from Javadoc, read from a data file of
/// blocks `=== <Class|*> <slot>` followed by the text: for slot `module` the
/// module docstring after its title line, for `class` / `method <name>` the
/// docstring literal, unindented. A `*` block applies to every class without
/// its own block.
#[derive(Clone, Debug, Default)]
pub struct Prose {
    blocks: BTreeMap<(String, String), Vec<String>>,
}

impl Prose {
    pub fn parse(text: &str) -> Res<Self> {
        let mut blocks = BTreeMap::new();
        let mut key: Option<(String, String)> = None;
        let mut lines: Vec<String> = Vec::new();
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix("=== ") {
                if let Some(k) = key.take() {
                    blocks.insert(k, std::mem::take(&mut lines));
                }
                let (class, slot) = rest
                    .split_once(' ')
                    .ok_or_else(|| anyhow::anyhow!("bad prose header `{line}`"))?;
                key = Some((class.to_string(), slot.to_string()));
            } else if key.is_some() {
                lines.push(line.to_string());
            } else if !line.trim().is_empty() && !line.starts_with('#') {
                anyhow::bail!("prose text before the first block: `{line}`");
            }
        }
        if let Some(k) = key {
            blocks.insert(k, lines);
        }
        Ok(Prose { blocks })
    }

    pub fn get(&self, class: &str, slot: &str) -> Option<&Vec<String>> {
        self.blocks
            .get(&(class.to_string(), slot.to_string()))
            .or_else(|| self.blocks.get(&("*".to_string(), slot.to_string())))
    }
}

// ---------------------------------------------------------------------------
// Golden comparison
// ---------------------------------------------------------------------------

/// `None` when equal, else the first differing line with its number.
pub fn first_difference(expected: &str, actual: &str) -> Option<String> {
    if expected == actual {
        return None;
    }
    let e: Vec<&str> = expected.split('\n').collect();
    let a: Vec<&str> = actual.split('\n').collect();
    for k in 0..e.len().max(a.len()) {
        let (x, y) = (e.get(k).copied(), a.get(k).copied());
        if x != y {
            return Some(format!(
                "line {}:\n  on disk:   {}\n  generated: {}",
                k + 1,
                x.unwrap_or("<end of file>"),
                y.unwrap_or("<end of file>")
            ));
        }
    }
    Some("files differ".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snake() {
        assert_eq!(snake("timeoutMs"), "timeout_ms");
        assert_eq!(snake("ListGroupsOptions"), "list_groups_options");
        assert_eq!(snake("maxlifeTimeMs"), "maxlife_time_ms");
    }

    #[test]
    fn test_wrap_signature_aligned_then_hanging() {
        let items = [
            "self".to_string(),
            "*".to_string(),
            "include_authorized_operations: bool | None = None".to_string(),
        ];
        let s = wrap_signature(4, "def include_authorized_operations(", &items, ") -> Any:", 8, 0);
        assert_eq!(
            s,
            "    def include_authorized_operations(self, *,\n                                      include_authorized_operations: bool | None = None) -> Any:"
        );
        let items = [
            "self".to_string(),
            "*".to_string(),
            "partition_size_limit_per_response: int | None = None".to_string(),
        ];
        let s = wrap_signature(4, "def partition_size_limit_per_response(", &items, ") -> Any:", 8, 0);
        assert_eq!(
            s,
            "    def partition_size_limit_per_response(\n            self, *, partition_size_limit_per_response: int | None = None) -> Any:"
        );
    }

    #[test]
    fn test_parse_members_keeps_order_comments_and_javadoc() {
        let src = "package p;\nimport java.util.Set;\n/** Doc. */\npublic class A extends B<A> {\n\
                   private int x = 1;\n /** Set x. */\n // kept\n @Deprecated\n public A x(int x) {\n this.x = x;\n return this;\n }\n}\n";
        let c = parse_java_class(src).unwrap();
        assert_eq!(c.name, "A");
        assert_eq!(c.extends.as_deref(), Some("B<A>"));
        assert_eq!(c.members.len(), 2);
        assert_eq!(c.members[1].comments, vec!["kept".to_string()]);
        assert!(c.members[1].has_annotation("Deprecated"));
        let JKind::Method { body, .. } = &c.members[1].kind else {
            panic!()
        };
        assert_eq!(body.as_deref(), Some("this.x = x; return this;"));
    }

    #[test]
    fn test_repeated_link_reads_once() {
        let t = inline_markup("For {@link Admin#listX(java.util.Map)} and {@link Admin#listX(String)}.");
        assert_eq!(t, "For ``Admin.list_x()``.");
        assert_eq!(inline_markup("{@link A#b()} and {@link A#c()}"), "``A.b()`` and ``A.c()``");
    }

    #[test]
    fn test_read_doc_markup() {
        let d = read_doc("/**\n * Set it or {@code null} for {@link Admin#listTopics()}.\n *\n * @param listInternal  Whether.\n * @return  This.\n */");
        assert_eq!(
            d.paragraphs,
            vec!["Set it or ``None`` for ``Admin.list_topics()``.".to_string()]
        );
        assert_eq!(d.params, vec![("listInternal".to_string(), "Whether.".to_string())]);
        assert_eq!(d.returns.as_deref(), Some("This."));
    }
}
