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

//! A small, fail-closed reader for the Java exception sources.
//!
//! The Python error classes are generated from their Java constructors, fields,
//! getters and nested enums (CLAUDE.md, Python Binding Conventions, Errors).
//! Exception classes use a narrow slice of Java: constructors that call
//! `super(...)` / `this(...)` and assign fields, one-line getters, string
//! concatenation, a `== null` ternary, and a few collection factories. This
//! module parses exactly that slice and returns an error on anything else, so a
//! new Java construct stops generation instead of being mistranslated.

use std::collections::BTreeMap;

/// A Java expression, restricted to what the exception sources use.
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Null,
    Bool(bool),
    Int(i64),
    Str(String),
    /// A dotted name: a parameter, `this.field`, or a qualified constant.
    Name(Vec<String>),
    /// A call on a dotted name: `Collections.emptySet()`, `Set.copyOf(x)`.
    Call(Vec<String>, Vec<Expr>),
    /// `new Type<...>(args)`; the type is its simple name without generics.
    New(String, Vec<Expr>),
    /// `a + b`.
    Add(Box<Expr>, Box<Expr>),
    /// `a == b`.
    Eq(Box<Expr>, Box<Expr>),
    /// `c ? a : b`.
    Ternary(Box<Expr>, Box<Expr>, Box<Expr>),
}

/// A constructor parameter: its Java type (generics kept, whitespace
/// normalized) and its Java name.
#[derive(Clone, Debug)]
pub struct Param {
    pub ty: String,
    pub name: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallKind {
    This,
    Super,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Visibility {
    Public,
    Protected,
    Private,
    Package,
}

#[derive(Clone, Debug)]
pub struct Ctor {
    pub visibility: Visibility,
    pub params: Vec<Param>,
    /// The explicit `super(...)` / `this(...)` call; `None` is an implicit
    /// `super()`.
    pub call: Option<(CallKind, Vec<Expr>)>,
    /// `this.f = e;` / `f = e;` in body order.
    pub assigns: Vec<(String, Expr)>,
    /// Java's `@deprecated` note when the constructor is `@Deprecated`.
    pub deprecated: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Field {
    pub ty: String,
    pub name: String,
    pub is_static: bool,
    pub init: Option<Expr>,
}

/// What a public getter returns.
#[derive(Clone, Debug)]
pub enum GetterBody {
    /// `return field;`
    Field(String),
    /// `return field.keySet();`
    KeySet(String),
    /// `public abstract T name();`
    Abstract,
}

#[derive(Clone, Debug)]
pub struct Getter {
    pub name: String,
    pub ret: String,
    pub body: GetterBody,
}

#[derive(Clone, Debug)]
pub struct NestedEnum {
    pub name: String,
    pub constants: Vec<String>,
}

/// A `public static final <Class> NAME = new <Class>(args);` singleton.
#[derive(Clone, Debug)]
pub struct Singleton {
    pub name: String,
    pub args: Vec<Expr>,
}

#[derive(Clone, Debug)]
pub struct JavaClass {
    pub package: String,
    pub simple: String,
    pub is_abstract: bool,
    /// The parent's simple name as written after `extends`.
    pub parent_simple: String,
    /// `import` lines: simple name -> fully-qualified name.
    pub imports: BTreeMap<String, String>,
    /// The class Javadoc, as plain text.
    pub javadoc: String,
    pub ctors: Vec<Ctor>,
    pub fields: Vec<Field>,
    pub getters: Vec<Getter>,
    pub enums: Vec<NestedEnum>,
    pub singletons: Vec<Singleton>,
}

type Res<T> = anyhow::Result<T>;

/// Remove `//` and `/* */` comments, keeping string and char literals intact.
pub fn strip_comments(src: &str) -> String {
    let b = src.as_bytes();
    let mut out = String::with_capacity(src.len());
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'"' || c == b'\'' {
            let quote = c;
            let start = i;
            i += 1;
            while i < b.len() && b[i] != quote {
                if b[i] == b'\\' {
                    i += 1;
                }
                i += 1;
            }
            i = (i + 1).min(b.len());
            out.push_str(&src[start..i]);
        } else if c == b'/' && i + 1 < b.len() && b[i + 1] == b'/' {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
        } else if c == b'/' && i + 1 < b.len() && b[i + 1] == b'*' {
            i += 2;
            while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                i += 1;
            }
            i += 2;
            out.push(' ');
        } else {
            out.push(c as char);
            i += 1;
        }
    }
    out
}

/// Index just past the bracket matching the one at `open` (`(`, `{` or `<`),
/// skipping string literals.
fn matching(s: &str, open: usize) -> Res<usize> {
    let b = s.as_bytes();
    let (o, c) = match b[open] {
        b'(' => (b'(', b')'),
        b'{' => (b'{', b'}'),
        b'<' => (b'<', b'>'),
        other => anyhow::bail!("matching(): not an opening bracket: {}", other as char),
    };
    let mut depth = 0i32;
    let mut i = open;
    while i < b.len() {
        let ch = b[i];
        if ch == b'"' || ch == b'\'' {
            let quote = ch;
            i += 1;
            while i < b.len() && b[i] != quote {
                if b[i] == b'\\' {
                    i += 1;
                }
                i += 1;
            }
        } else if ch == o {
            depth += 1;
        } else if ch == c {
            depth -= 1;
            if depth == 0 {
                return Ok(i + 1);
            }
        }
        i += 1;
    }
    anyhow::bail!("unbalanced brackets")
}

/// Split `s` at top-level occurrences of `sep` (outside brackets and strings).
fn split_top(s: &str, sep: u8) -> Vec<String> {
    let b = s.as_bytes();
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut start = 0;
    let mut i = 0;
    while i < b.len() {
        let ch = b[i];
        if ch == b'"' || ch == b'\'' {
            let quote = ch;
            i += 1;
            while i < b.len() && b[i] != quote {
                if b[i] == b'\\' {
                    i += 1;
                }
                i += 1;
            }
        } else if ch == b'(' || ch == b'{' || ch == b'<' || ch == b'[' {
            depth += 1;
        } else if ch == b')' || ch == b'}' || ch == b'>' || ch == b']' {
            depth -= 1;
        } else if ch == sep && depth == 0 {
            parts.push(s[start..i].to_string());
            start = i + 1;
        }
        i += 1;
    }
    let last = s[start..].trim();
    if !last.is_empty() || !parts.is_empty() {
        parts.push(s[start..].to_string());
    }
    parts
        .into_iter()
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect()
}

fn normalize_ws(s: &str) -> String {
    s.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .replace("< ", "<")
        .replace(" >", ">")
}

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum Tok {
    Ident(String),
    Str(String),
    Int(i64),
    Punct(&'static str),
}

fn tokenize(s: &str) -> Res<Vec<Tok>> {
    let chars: Vec<char> = s.chars().collect();
    let mut toks = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '"' {
            let mut v = String::new();
            i += 1;
            while i < chars.len() && chars[i] != '"' {
                if chars[i] == '\\' && i + 1 < chars.len() {
                    i += 1;
                    v.push(match chars[i] {
                        'n' => '\n',
                        't' => '\t',
                        other => other,
                    });
                } else {
                    v.push(chars[i]);
                }
                i += 1;
            }
            i += 1;
            toks.push(Tok::Str(v));
        } else if c.is_ascii_digit() {
            let start = i;
            while i < chars.len() && chars[i].is_ascii_digit() {
                i += 1;
            }
            let n: i64 = chars[start..i].iter().collect::<String>().parse()?;
            if i < chars.len() && (chars[i] == 'L' || chars[i] == 'l') {
                i += 1;
            }
            toks.push(Tok::Int(n));
        } else if c.is_alphabetic() || c == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            toks.push(Tok::Ident(chars[start..i].iter().collect()));
        } else {
            let two: String = chars[i..(i + 2).min(chars.len())].iter().collect();
            let p: &'static str = match two.as_str() {
                "==" => "==",
                "!=" => "!=",
                _ => match c {
                    '+' => "+",
                    '-' => "-",
                    '?' => "?",
                    ':' => ":",
                    '(' => "(",
                    ')' => ")",
                    ',' => ",",
                    '.' => ".",
                    '<' => "<",
                    '>' => ">",
                    other => anyhow::bail!("unsupported character `{other}` in expression `{s}`"),
                },
            };
            i += p.len();
            toks.push(Tok::Punct(p));
        }
    }
    Ok(toks)
}

struct ExprParser {
    toks: Vec<Tok>,
    pos: usize,
    src: String,
}

impl ExprParser {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn is_punct(&self, p: &str) -> bool {
        matches!(self.peek(), Some(Tok::Punct(q)) if *q == p)
    }

    fn expect(&mut self, p: &str) -> Res<()> {
        if self.is_punct(p) {
            self.pos += 1;
            Ok(())
        } else {
            anyhow::bail!("expected `{p}` in `{}`", self.src)
        }
    }

    fn ternary(&mut self) -> Res<Expr> {
        let cond = self.equality()?;
        if self.is_punct("?") {
            self.pos += 1;
            let a = self.ternary()?;
            self.expect(":")?;
            let b = self.ternary()?;
            return Ok(Expr::Ternary(Box::new(cond), Box::new(a), Box::new(b)));
        }
        Ok(cond)
    }

    fn equality(&mut self) -> Res<Expr> {
        let left = self.additive()?;
        if self.is_punct("==") {
            self.pos += 1;
            let right = self.additive()?;
            return Ok(Expr::Eq(Box::new(left), Box::new(right)));
        }
        Ok(left)
    }

    fn additive(&mut self) -> Res<Expr> {
        let mut left = self.primary()?;
        while self.is_punct("+") {
            self.pos += 1;
            let right = self.primary()?;
            left = Expr::Add(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn args(&mut self) -> Res<Vec<Expr>> {
        self.expect("(")?;
        let mut out = Vec::new();
        if self.is_punct(")") {
            self.pos += 1;
            return Ok(out);
        }
        loop {
            out.push(self.ternary()?);
            if self.is_punct(",") {
                self.pos += 1;
                continue;
            }
            self.expect(")")?;
            return Ok(out);
        }
    }

    fn skip_generics(&mut self) -> Res<()> {
        if self.is_punct("<") {
            let mut depth = 0;
            loop {
                match self.peek() {
                    Some(Tok::Punct("<")) => depth += 1,
                    Some(Tok::Punct(">")) => {
                        depth -= 1;
                        if depth == 0 {
                            self.pos += 1;
                            return Ok(());
                        }
                    },
                    None => anyhow::bail!("unbalanced generics in `{}`", self.src),
                    _ => {},
                }
                self.pos += 1;
            }
        }
        Ok(())
    }

    fn primary(&mut self) -> Res<Expr> {
        match self.peek().cloned() {
            Some(Tok::Str(s)) => {
                self.pos += 1;
                Ok(Expr::Str(s))
            },
            Some(Tok::Int(n)) => {
                self.pos += 1;
                Ok(Expr::Int(n))
            },
            Some(Tok::Punct("-")) => {
                self.pos += 1;
                match self.peek().cloned() {
                    Some(Tok::Int(n)) => {
                        self.pos += 1;
                        Ok(Expr::Int(-n))
                    },
                    _ => anyhow::bail!("unsupported unary minus in `{}`", self.src),
                }
            },
            Some(Tok::Punct("(")) => {
                self.pos += 1;
                let e = self.ternary()?;
                self.expect(")")?;
                Ok(e)
            },
            Some(Tok::Ident(id)) if id == "null" => {
                self.pos += 1;
                Ok(Expr::Null)
            },
            Some(Tok::Ident(id)) if id == "true" || id == "false" => {
                self.pos += 1;
                Ok(Expr::Bool(id == "true"))
            },
            Some(Tok::Ident(id)) if id == "new" => {
                self.pos += 1;
                let Some(Tok::Ident(ty)) = self.peek().cloned() else {
                    anyhow::bail!("expected a type after `new` in `{}`", self.src)
                };
                self.pos += 1;
                self.skip_generics()?;
                let args = self.args()?;
                Ok(Expr::New(ty, args))
            },
            Some(Tok::Ident(id)) => {
                self.pos += 1;
                let mut name = vec![id];
                while self.is_punct(".") {
                    self.pos += 1;
                    match self.peek().cloned() {
                        Some(Tok::Ident(n)) => {
                            self.pos += 1;
                            name.push(n);
                        },
                        _ => anyhow::bail!("expected a name after `.` in `{}`", self.src),
                    }
                }
                if self.is_punct("(") {
                    let args = self.args()?;
                    return Ok(Expr::Call(name, args));
                }
                Ok(Expr::Name(name))
            },
            other => anyhow::bail!("unsupported expression token {other:?} in `{}`", self.src),
        }
    }
}

pub fn parse_expr(s: &str) -> Res<Expr> {
    let mut p = ExprParser { toks: tokenize(s)?, pos: 0, src: s.to_string() };
    let e = p.ternary()?;
    if p.pos != p.toks.len() {
        anyhow::bail!("trailing tokens in expression `{s}`");
    }
    Ok(e)
}

// ---------------------------------------------------------------------------
// Javadoc
// ---------------------------------------------------------------------------

/// Plain text of a `/** ... */` block: leading `*` stripped, `{@link X}` and
/// `{@code X}` reduced to ``X``, HTML tags dropped, block tags (`@see`, …)
/// removed. Paragraphs are separated by one blank line.
pub fn javadoc_text(block: &str) -> String {
    let inner = block.trim().trim_start_matches("/**").trim_end_matches("*/");
    let mut lines = Vec::new();
    for line in inner.lines() {
        let l = line.trim().trim_start_matches('*').trim();
        if l.starts_with('@') {
            break;
        }
        lines.push(l.to_string());
    }
    let mut text = lines.join("\n");
    // {@link A#b(C) label} -> ``label`` (else ``A.b(C)``); {@code x} -> ``x``.
    while let Some(start) = text.find("{@") {
        let Some(end_rel) = text[start..].find('}') else { break };
        let inside = text[start + 2..start + end_rel].to_string();
        let (tag, rest) = inside.split_once(' ').unwrap_or((inside.as_str(), ""));
        let rest = rest.trim();
        let shown = if tag == "link" || tag == "linkplain" {
            // The reference ends at the first space outside parentheses.
            let mut depth = 0;
            let mut end = rest.len();
            for (k, c) in rest.char_indices() {
                match c {
                    '(' => depth += 1,
                    ')' => depth -= 1,
                    ' ' if depth == 0 => {
                        end = k;
                        break;
                    },
                    _ => {},
                }
            }
            let (reference, label) = rest.split_at(end);
            let label = label.trim();
            if label.is_empty() {
                reference.trim_start_matches('#').replace('#', ".")
            } else {
                label.to_string()
            }
        } else {
            rest.to_string()
        };
        text.replace_range(start..start + end_rel + 1, &format!("``{shown}``"));
    }
    for tag in [
        "<p>", "</p>", "<ul>", "</ul>", "<li>", "</li>", "<b>", "</b>", "<i>", "</i>",
    ] {
        let sub = if tag == "<p>" { "\n\n" } else { "" };
        text = text.replace(tag, sub);
    }
    // Collapse runs of blank lines and trailing whitespace.
    let mut out: Vec<String> = Vec::new();
    for para in text.split("\n\n") {
        let joined = para.split_whitespace().collect::<Vec<_>>().join(" ");
        if !joined.is_empty() {
            out.push(joined);
        }
    }
    out.join("\n\n")
}

/// The `@deprecated` text of a `/** ... */` block, if it has one.
fn deprecated_note(block: &str) -> Option<String> {
    let inner = block.trim().trim_start_matches("/**").trim_end_matches("*/");
    let mut collecting = false;
    let mut words: Vec<String> = Vec::new();
    for line in inner.lines() {
        let l = line.trim().trim_start_matches('*').trim();
        if let Some(rest) = l.strip_prefix("@deprecated") {
            collecting = true;
            words.push(rest.trim().to_string());
            continue;
        }
        if collecting {
            if l.starts_with('@') {
                break;
            }
            words.push(l.to_string());
        }
    }
    if !collecting {
        return None;
    }
    Some(javadoc_text(&format!("/** {} */", words.join(" "))))
}

// ---------------------------------------------------------------------------
// Class members
// ---------------------------------------------------------------------------

fn parse_params(s: &str) -> Res<Vec<Param>> {
    let mut out = Vec::new();
    for part in split_top(s, b',') {
        let part = normalize_ws(&part.replace("final ", ""));
        let (ty, name) = part
            .rsplit_once(' ')
            .ok_or_else(|| anyhow::anyhow!("cannot parse parameter `{part}`"))?;
        out.push(Param { ty: ty.trim().to_string(), name: name.trim().to_string() });
    }
    Ok(out)
}

/// A constructor body: its `super(...)` / `this(...)` call and its field
/// assignments.
type CtorBody = (Option<(CallKind, Vec<Expr>)>, Vec<(String, Expr)>);

fn parse_ctor_body(body: &str) -> Res<CtorBody> {
    let mut call = None;
    let mut assigns = Vec::new();
    for stmt in split_top(body, b';') {
        let stmt = stmt.trim();
        if stmt.is_empty() || stmt == "Thread.currentThread().interrupt()" {
            continue;
        }
        let kind = if stmt.starts_with("super(") {
            Some(CallKind::Super)
        } else if stmt.starts_with("this(") {
            Some(CallKind::This)
        } else {
            None
        };
        if let Some(kind) = kind {
            let open = stmt.find('(').unwrap();
            let close = matching(stmt, open)?;
            let mut args = Vec::new();
            for a in split_top(&stmt[open + 1..close - 1], b',') {
                args.push(parse_expr(&a)?);
            }
            call = Some((kind, args));
            continue;
        }
        if let Some((lhs, rhs)) = stmt.split_once('=') {
            let lhs = lhs.trim().trim_start_matches("this.").trim();
            if lhs.chars().all(|c| c.is_alphanumeric() || c == '_') {
                assigns.push((lhs.to_string(), parse_expr(rhs.trim())?));
                continue;
            }
        }
        anyhow::bail!("unsupported constructor statement `{stmt}`");
    }
    Ok((call, assigns))
}

fn visibility_of(header: &str) -> Visibility {
    let words: Vec<&str> = header.split_whitespace().collect();
    if words.contains(&"public") {
        Visibility::Public
    } else if words.contains(&"protected") {
        Visibility::Protected
    } else if words.contains(&"private") {
        Visibility::Private
    } else {
        Visibility::Package
    }
}

/// Parse one exception class source.
pub fn parse_class(raw: &str) -> Res<JavaClass> {
    let src = strip_comments(raw);
    let package = src
        .lines()
        .find_map(|l| {
            l.trim()
                .strip_prefix("package ")
                .map(|p| p.trim_end_matches(';').trim().to_string())
        })
        .ok_or_else(|| anyhow::anyhow!("no package declaration"))?;
    let mut imports = BTreeMap::new();
    for l in src.lines() {
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

    // The top-level declaration: `public [abstract|final] class X extends Y {`.
    let decl = ["public abstract class ", "public final class ", "public class "]
        .iter()
        .find_map(|n| src.find(n).map(|i| (i, *n)))
        .ok_or_else(|| anyhow::anyhow!("no public class declaration"))?;
    let is_abstract = decl.1.contains("abstract");
    let after = &src[decl.0 + decl.1.len()..];
    let simple: String = after.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
    let ext = after
        .find(" extends ")
        .ok_or_else(|| anyhow::anyhow!("{simple}: no `extends`"))?;
    let parent_simple: String = after[ext + " extends ".len()..]
        .trim_start()
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    let open = decl.0 + decl.1.len() + after.find('{').ok_or_else(|| anyhow::anyhow!("{simple}: no body"))?;
    let close = matching(&src, open)?;
    let body = &src[open + 1..close - 1];

    // Class Javadoc: the last `/** */` block before the declaration in `raw`.
    let raw_decl = ["public abstract class ", "public final class ", "public class "]
        .iter()
        .find_map(|n| raw.find(n))
        .unwrap_or(0);
    let javadoc = raw[..raw_decl]
        .rfind("/**")
        .map(|s| {
            let e = raw[s..].find("*/").map(|e| s + e + 2).unwrap_or(raw_decl);
            javadoc_text(&raw[s..e])
        })
        .unwrap_or_default();

    // Deprecation notes by constructor parameter-type signature, from `raw`.
    let mut deprecated_by_sig: BTreeMap<String, String> = BTreeMap::new();
    let mut search = 0;
    while let Some(rel) = raw[search..].find("/**") {
        let start = search + rel;
        let Some(end_rel) = raw[start..].find("*/") else { break };
        let end = start + end_rel + 2;
        search = end;
        let Some(note) = deprecated_note(&raw[start..end]) else {
            continue;
        };
        let rest = &raw[end..];
        let needle = format!("{simple}(");
        let Some(at) = rest.find(&needle) else { continue };
        let paren = at + needle.len() - 1;
        let Ok(close_p) = matching(rest, paren) else { continue };
        let params = parse_params(&rest[paren + 1..close_p - 1])?;
        let sig = params.iter().map(|p| p.ty.clone()).collect::<Vec<_>>().join(",");
        deprecated_by_sig.insert(sig, note);
    }

    let mut ctors = Vec::new();
    let mut fields = Vec::new();
    let mut getters = Vec::new();
    let mut enums = Vec::new();
    let mut singletons = Vec::new();

    // Walk the members at depth 1: each ends with `;` or a `{...}` block.
    let b = body.as_bytes();
    let mut i = 0;
    let mut start = 0;
    while i < b.len() {
        let ch = b[i];
        if ch == b'"' || ch == b'\'' {
            let quote = ch;
            i += 1;
            while i < b.len() && b[i] != quote {
                if b[i] == b'\\' {
                    i += 1;
                }
                i += 1;
            }
            i += 1;
            continue;
        }
        if ch == b'(' {
            i = matching(body, i)?;
            continue;
        }
        if ch == b';' {
            let item = body[start..i].trim();
            start = i + 1;
            i += 1;
            if item.is_empty() {
                continue;
            }
            let header = normalize_ws(item);
            // A field, a singleton, or an abstract method.
            if header.contains("abstract ") && header.contains('(') {
                let before = header[..header.find('(').unwrap()].trim();
                let words: Vec<&str> = before.split(' ').collect();
                let name = words.last().unwrap().to_string();
                let ret = words[..words.len() - 1]
                    .iter()
                    .filter(|w| !matches!(**w, "public" | "abstract" | "protected"))
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(" ");
                getters.push(Getter { name, ret, body: GetterBody::Abstract });
                continue;
            }
            let (decl, init) = match header.split_once('=') {
                Some((d, e)) => (d.trim().to_string(), Some(e.trim().to_string())),
                None => (header.clone(), None),
            };
            let words: Vec<&str> = decl.split(' ').collect();
            let name = words.last().unwrap().to_string();
            let is_static = words.contains(&"static");
            let ty = words[..words.len() - 1]
                .iter()
                .filter(|w| !matches!(**w, "public" | "private" | "protected" | "static" | "final" | "volatile"))
                .cloned()
                .collect::<Vec<_>>()
                .join(" ");
            if name == "serialVersionUID" {
                continue;
            }
            let init_expr = init.as_deref().map(parse_expr).transpose()?;
            if is_static && words.contains(&"public") {
                match init_expr {
                    Some(Expr::New(cls, args)) if cls == simple => {
                        singletons.push(Singleton { name, args });
                        continue;
                    },
                    _ => anyhow::bail!("{simple}: unsupported public static member `{header}`"),
                }
            }
            fields.push(Field { ty, name, is_static, init: init_expr });
            continue;
        }
        if ch == b'{' {
            let end = matching(body, i)?;
            let header = normalize_ws(&body[start..i]);
            let block = &body[i + 1..end - 1];
            start = end;
            i = end;
            if header.contains("enum ") {
                let name = header.split("enum ").nth(1).unwrap().trim().to_string();
                let constants_part = block.split(';').next().unwrap_or("");
                let constants = split_top(constants_part, b',')
                    .into_iter()
                    .map(|c| {
                        c.chars()
                            .take_while(|ch| ch.is_alphanumeric() || *ch == '_')
                            .collect::<String>()
                    })
                    .filter(|c| !c.is_empty())
                    .collect();
                enums.push(NestedEnum { name, constants });
                continue;
            }
            let Some(paren) = header.find('(') else {
                anyhow::bail!("{simple}: unsupported member `{header}`")
            };
            let before = header[..paren].trim();
            let name = before.rsplit(' ').next().unwrap_or("").to_string();
            let close_p = matching(&header, paren)?;
            let params_src = &header[paren + 1..close_p - 1];
            if name == simple {
                let params = parse_params(params_src)?;
                let (call, assigns) = parse_ctor_body(block)?;
                let sig = params.iter().map(|p| p.ty.clone()).collect::<Vec<_>>().join(",");
                let deprecated = if header.contains("@Deprecated") {
                    Some(deprecated_by_sig.get(&sig).cloned().unwrap_or_default())
                } else {
                    None
                };
                ctors.push(Ctor { visibility: visibility_of(before), params, call, assigns, deprecated });
                continue;
            }
            // A method: only public no-argument getters matter.
            if visibility_of(before) != Visibility::Public || !params_src.trim().is_empty() {
                continue;
            }
            if matches!(name.as_str(), "toString" | "fillInStackTrace" | "hashCode") {
                continue;
            }
            let ret = before
                .split(' ')
                .filter(|w| !w.starts_with('@') && !matches!(*w, "public" | "final" | "static" | "synchronized"))
                .collect::<Vec<_>>();
            let ret = ret[..ret.len() - 1].join(" ");
            let stmt = block.trim().trim_end_matches(';').trim();
            let Some(expr) = stmt.strip_prefix("return ") else {
                anyhow::bail!("{simple}.{name}(): unsupported getter body `{stmt}`")
            };
            let expr = expr.trim().trim_start_matches("this.");
            let body = if let Some(field) = expr.strip_suffix(".keySet()") {
                GetterBody::KeySet(field.to_string())
            } else if expr.chars().all(|c| c.is_alphanumeric() || c == '_') {
                GetterBody::Field(expr.to_string())
            } else {
                anyhow::bail!("{simple}.{name}(): unsupported getter body `{stmt}`")
            };
            getters.push(Getter { name, ret, body });
            continue;
        }
        i += 1;
    }

    Ok(JavaClass {
        package,
        simple,
        is_abstract,
        parent_simple,
        imports,
        javadoc,
        ctors,
        fields,
        getters,
        enums,
        singletons,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_comments_but_not_strings() {
        let s = strip_comments("a /* x */ b // y\n\"// z\" '/'");
        assert_eq!(s, "a   b \n\"// z\" '/'");
    }

    #[test]
    fn parses_concatenation_and_ternary() {
        let e =
            parse_expr("\"Invalid value \" + value + \" for \" + name + (message == null ? \"\" : \": \" + message)")
                .unwrap();
        let Expr::Add(_, last) = e else { panic!() };
        assert!(matches!(*last, Expr::Ternary(..)));
    }

    #[test]
    fn parses_new_with_generics_and_calls() {
        assert_eq!(parse_expr("new HashSet<>()").unwrap(), Expr::New("HashSet".into(), vec![]));
        assert_eq!(
            parse_expr("Collections.singleton(partition)").unwrap(),
            Expr::Call(
                vec!["Collections".into(), "singleton".into()],
                vec![Expr::Name(vec!["partition".into()])]
            )
        );
    }

    #[test]
    fn javadoc_is_plain_text() {
        let t = javadoc_text("/**\n * The base {@link Foo} of <p> all {@code x}.\n * @see Bar\n */");
        assert_eq!(t, "The base ``Foo`` of\n\nall ``x``.");
    }
}
