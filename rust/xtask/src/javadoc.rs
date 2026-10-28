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

//! The one Javadoc translator (`.claude/rules/documentation.md` §3) and the
//! word count `check-api-docs` measures docs with (§5).
//!
//! - [`Translator::to_rustdoc`] turns a Javadoc into the rustdoc a translated
//!   item's original documentation should read as: the body as Markdown, then
//!   `# Arguments`, `# Returns`, `# Errors` (or `# Panics`) and `# See also`
//!   from the block tags, with CLAUDE.md §2's renames applied;
//!   [`Translator::translate`] also returns those tags one by one.
//! - [`plain_text`] is the plain-text form the Python error classes carry
//!   (`error_hierarchy`): the body only, code as ``` ``x`` ```.
//! - [`word_count`] counts the words of a doc, headings left out.
//!
//! The inputs are Javadocs as [`crate::java::clean_javadoc`] leaves them, or
//! whole `/** .. */` blocks for [`plain_text`].

use crate::java::{self, JavaIndex};

/// A Javadoc split into its main description and its block tags.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Javadoc {
    pub body: String,
    pub tags: Vec<BlockTag>,
}

/// One block tag: `@param name text`, `@throws X text`, `@return text`, ...
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockTag {
    /// The tag without its `@`.
    pub name: String,
    /// The word naming what the tag is about: the parameter of `@param`, the
    /// class of `@throws` / `@exception`; empty for other tags.
    pub arg: String,
    pub text: String,
}

/// The tags whose first word is an argument rather than text.
const TAGS_WITH_ARG: &[&str] = &["param", "throws", "exception"];

/// Splits a cleaned Javadoc into its body and block tags. A block tag starts a
/// line, outside `<pre>`.
pub fn parse(doc: &str) -> Javadoc {
    let mut body: Vec<&str> = Vec::new();
    let mut tags: Vec<BlockTag> = Vec::new();
    let mut pre = 0usize;
    for line in doc.lines() {
        let trimmed = line.trim();
        let starts_tag =
            pre == 0 && trimmed.starts_with('@') && trimmed[1..].starts_with(|c: char| c.is_ascii_alphabetic());
        if starts_tag {
            let (name, rest) = split_word(&trimmed[1..]);
            let (arg, text) = if TAGS_WITH_ARG.contains(&name) {
                split_word(rest)
            } else {
                ("", rest)
            };
            tags.push(BlockTag { name: name.to_string(), arg: arg.to_string(), text: text.to_string() });
        } else if let Some(tag) = tags.last_mut() {
            tag.text.push('\n');
            tag.text.push_str(line);
        } else {
            body.push(line);
        }
        let lower = line.to_ascii_lowercase();
        pre += lower.matches("<pre").count();
        pre = pre.saturating_sub(lower.matches("</pre").count());
    }
    for tag in &mut tags {
        tag.text = tag.text.trim().to_string();
    }
    Javadoc { body: body.join("\n").trim().to_string(), tags }
}

/// The first whitespace-separated word of `s` and the rest, trimmed.
fn split_word(s: &str) -> (&str, &str) {
    let s = s.trim_start();
    match s.find(char::is_whitespace) {
        Some(end) => (&s[..end], s[end..].trim()),
        None => (s, ""),
    }
}

/// `own` with each `{@inheritDoc}` replaced by the matching part of
/// `inherited`: the main description in the body, the same tag (same
/// parameter, same class) in a block tag. As Javadoc does, a `@param`,
/// `@return` or `@throws` that `own` leaves out is inherited too.
pub fn inherit_doc(own: &str, inherited: &str) -> String {
    let mut own = parse(own);
    let inherited = parse(inherited);
    own.body = own.body.replace("{@inheritDoc}", &inherited.body);
    for tag in &mut own.tags {
        if tag.text.contains("{@inheritDoc}") {
            let parent = inherited
                .tags
                .iter()
                .find(|t| t.name == tag.name && t.arg == tag.arg)
                .map_or("", |t| t.text.as_str());
            tag.text = tag.text.replace("{@inheritDoc}", parent);
        }
    }
    for tag in &inherited.tags {
        let inheritable = matches!(tag.name.as_str(), "param" | "return" | "throws" | "exception");
        if inheritable && !own.tags.iter().any(|t| t.name == tag.name && t.arg == tag.arg) {
            own.tags.push(tag.clone());
        }
    }
    let mut out = own.body;
    for tag in &own.tags {
        out.push_str(&format!("\n@{} {} {}", tag.name, tag.arg, tag.text));
    }
    out
}

// ---------------------------------------------------------------------------
// Inline tags
// ---------------------------------------------------------------------------

/// The inline tag `{@tag content}` starting at byte `start` of `text`: its
/// name, its content and the byte just past its closing brace. Braces inside
/// the content nest, as in `{@code Map<String, {x}>}`.
fn inline_tag(text: &str, start: usize) -> Option<(&str, &str, usize)> {
    let rest = text.get(start..)?.strip_prefix("{@")?;
    let mut depth = 1usize;
    let mut end = None;
    for (k, c) in rest.char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    end = Some(k);
                    break;
                }
            },
            _ => {},
        }
    }
    let end = end?;
    let inside = &rest[..end];
    let (tag, content) = match inside.find(char::is_whitespace) {
        Some(at) => (&inside[..at], &inside[at..]),
        None => (inside, ""),
    };
    Some((tag, content, start + 2 + end + 1))
}

/// A `{@link}` content split into its reference and its label; the reference
/// ends at the first space outside parentheses.
fn split_link(content: &str) -> (&str, &str) {
    let content = content.trim();
    let mut depth = 0i32;
    for (k, c) in content.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            c if c.is_whitespace() && depth == 0 => return (&content[..k], content[k..].trim()),
            _ => {},
        }
    }
    (content, "")
}

// ---------------------------------------------------------------------------
// Plain text (the Python error classes)
// ---------------------------------------------------------------------------

/// Plain text of a `/** ... */` block: leading `*` stripped, `{@link X}` and
/// `{@code X}` reduced to ``X``, HTML tags dropped, block tags (`@see`, …)
/// removed. Paragraphs are separated by one blank line.
pub fn plain_text(block: &str) -> String {
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
    let mut from = 0;
    while let Some(rel) = text[from..].find("{@") {
        let start = from + rel;
        let Some((tag, content, end)) = inline_tag(&text, start) else {
            break;
        };
        let shown = if tag == "link" || tag == "linkplain" {
            let (reference, label) = split_link(content);
            if label.is_empty() {
                reference.trim_start_matches('#').replace('#', ".")
            } else {
                label.to_string()
            }
        } else {
            content.trim().to_string()
        };
        let replacement = format!("``{shown}``");
        text.replace_range(start..end, &replacement);
        from = start + replacement.len();
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

// ---------------------------------------------------------------------------
// Rustdoc
// ---------------------------------------------------------------------------

/// One block of translated text.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Block {
    Para(String),
    /// A list item: its marker (`- `, `1. `) and text.
    Item(String, String),
    Heading(String),
    /// A `<pre>` block's lines.
    Code(String),
    /// A `<table>`: its rows of cells, the first row the header.
    Table(Vec<Vec<String>>),
}

/// What the current text belongs to while converting.
enum Open {
    Para,
    Item(String),
    Heading,
}

/// Translates Javadoc into rustdoc, naming classes as the Rust crate does.
pub struct Translator<'a> {
    /// The index Java class names are looked up in, to take their Rust names
    /// (`OffsetOutOfRangeException` in `clients.consumer` →
    /// `ConsumerOffsetOutOfRangeError`); without it only CLAUDE.md §2's word
    /// substitutions apply.
    pub index: Option<&'a JavaIndex>,
    /// Whether `@throws` becomes `# Panics` rather than `# Errors`: the Rust
    /// item keeps Java's throw as a panic (CLAUDE.md §12.5).
    pub panics: bool,
}

/// A Javadoc translated by [`Translator::translate`].
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Translation {
    /// The whole rustdoc.
    pub rustdoc: String,
    /// Each `@param`: the parameter name, snake_cased (a type parameter as
    /// written), and its text.
    pub params: Vec<(String, String)>,
    /// The `@return` text.
    pub returns: Option<String>,
    /// Each `@throws` / `@exception`.
    pub throws: Vec<Throw>,
}

/// One `@throws` tag, translated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Throw {
    /// The Java class's simple name.
    pub java: String,
    /// The Rust name the translator writes.
    pub rust: String,
    /// Every Rust name that documents it ([`Translator::error_names`]).
    pub names: Vec<String>,
    pub text: String,
}

impl Translator<'_> {
    /// The rustdoc of cleaned Javadoc `doc` (documentation.md §3).
    pub fn to_rustdoc(&self, doc: &str) -> String {
        self.translate(doc).rustdoc
    }

    /// The rustdoc of cleaned Javadoc `doc`, and its `@param`, `@return` and
    /// `@throws` tags translated one by one.
    pub fn translate(&self, doc: &str) -> Translation {
        let parsed = parse(doc);
        let mut blocks = self.blocks(&parsed.body);
        let mut out = Translation::default();
        let mut arguments = Vec::new();
        let mut returns = Vec::new();
        let mut throws = Vec::new();
        let mut see = Vec::new();
        for tag in &parsed.tags {
            match tag.name.as_str() {
                "param" => {
                    let name = match tag.arg.strip_prefix('<').and_then(|a| a.strip_suffix('>')) {
                        Some(type_param) => type_param.to_string(),
                        None => java::snake_case(&tag.arg),
                    };
                    let text = self.inline_text(&tag.text);
                    arguments.push(Block::Item("- ".into(), format!("`{name}` - {text}").trim_end().to_string()));
                    out.params.push((name, text));
                },
                "return" => {
                    let found = self.blocks(&tag.text);
                    out.returns = Some(render(&found));
                    returns.extend(found);
                },
                "throws" | "exception" => {
                    let class = tag.arg.rsplit('.').next().unwrap_or(&tag.arg);
                    let rust = self.class_name(class);
                    let text = self.inline_text(&tag.text);
                    throws.push(Block::Item("- ".into(), format!("`{rust}` {text}").trim_end().to_string()));
                    out.throws
                        .push(Throw { java: class.to_string(), names: self.error_names(&tag.arg), rust, text });
                },
                "see" => see.push(Block::Item("- ".into(), self.see_ref(&tag.text))),
                "apiNote" | "implNote" | "implSpec" => blocks.extend(self.blocks(&tag.text)),
                // `@since`, `@author`, `@deprecated`, `@serial`, `@version`, ...
                _ => {},
            }
        }
        let mut sections = vec![blocks];
        for (heading, list) in [
            ("Arguments", arguments),
            ("Returns", returns),
            (if self.panics { "Panics" } else { "Errors" }, throws),
            ("See also", see),
        ] {
            if !list.is_empty() {
                let mut section = vec![Block::Heading(heading.to_string())];
                section.extend(list);
                sections.push(section);
            }
        }
        out.rustdoc = render(&sections.concat());
        out
    }

    /// Every Rust name a `@throws` of Java class `arg` may be documented as:
    /// the Rust name of each indexed class of that simple name, or, for a JDK
    /// class (qualified `java.…`, or not indexed), its `Local…Error`.
    pub fn error_names(&self, arg: &str) -> Vec<String> {
        let simple = arg.rsplit('.').next().unwrap_or(arg);
        let simple = simple.split('<').next().unwrap_or(simple);
        let mut names: Vec<String> = Vec::new();
        if !arg.starts_with("java.") {
            if let Some(index) = self.index {
                for class in index.classes.iter().filter(|c| !c.is_test && c.name() == simple) {
                    let name = class.rust_name();
                    if !names.contains(&name) {
                        names.push(name);
                    }
                }
            }
        }
        if names.is_empty() {
            names.push(self.jdk_name(simple));
        }
        names
    }

    /// Converts text with HTML and inline tags into blocks.
    fn blocks(&self, text: &str) -> Vec<Block> {
        let mut out = Vec::new();
        let mut cur = String::new();
        let mut open = Open::Para;
        let mut lists: Vec<(bool, usize)> = Vec::new();
        // The rows of the `<table>` being read, and whether a cell is open.
        let mut table: Option<Vec<Vec<String>>> = None;
        let mut in_cell = false;
        let cell = |cur: &mut String, table: &mut Option<Vec<Vec<String>>>, in_cell: &mut bool| {
            if std::mem::replace(in_cell, false) {
                let text = self.rename(&collapse(cur));
                cur.clear();
                if let Some(rows) = table.as_mut() {
                    if rows.is_empty() {
                        rows.push(Vec::new());
                    }
                    rows.last_mut().expect("a row").push(text);
                }
            }
        };
        let flush = |cur: &mut String, open: &mut Open, out: &mut Vec<Block>| {
            let t = self.rename(&collapse(cur));
            cur.clear();
            let kind = std::mem::replace(open, Open::Para);
            if t.is_empty() {
                return;
            }
            out.push(match kind {
                Open::Para => Block::Para(t),
                Open::Item(marker) => Block::Item(marker, t),
                Open::Heading => Block::Heading(t),
            });
        };
        let mut i = 0;
        while i < text.len() {
            let rest = &text[i..];
            let c = rest.chars().next().unwrap_or(' ');
            if c == '<' {
                if let Some((name, closing, end)) = html_tag(rest) {
                    match name.as_str() {
                        "table" => {
                            cell(&mut cur, &mut table, &mut in_cell);
                            flush(&mut cur, &mut open, &mut out);
                            if closing {
                                if let Some(rows) = table.take() {
                                    let rows: Vec<Vec<String>> = rows.into_iter().filter(|r| !r.is_empty()).collect();
                                    if !rows.is_empty() {
                                        out.push(Block::Table(rows));
                                    }
                                }
                            } else {
                                table = Some(Vec::new());
                            }
                        },
                        "tr" if table.is_some() => {
                            cell(&mut cur, &mut table, &mut in_cell);
                            // Text between cells (a caption) stays a paragraph.
                            flush(&mut cur, &mut open, &mut out);
                            if !closing {
                                if let Some(rows) = table.as_mut() {
                                    rows.push(Vec::new());
                                }
                            }
                        },
                        "th" | "td" if table.is_some() => {
                            cell(&mut cur, &mut table, &mut in_cell);
                            if !closing {
                                cur.clear();
                                in_cell = true;
                            }
                        },
                        "p" | "br" | "div" | "tr" | "blockquote" | "dl" if !in_cell => {
                            flush(&mut cur, &mut open, &mut out)
                        },
                        "ul" | "ol" => {
                            flush(&mut cur, &mut open, &mut out);
                            if closing {
                                lists.pop();
                            } else {
                                lists.push((name == "ol", 0));
                            }
                        },
                        "li" | "dt" | "dd" => {
                            flush(&mut cur, &mut open, &mut out);
                            if !closing {
                                let marker = match lists.last_mut() {
                                    Some((true, n)) => {
                                        *n += 1;
                                        format!("{n}. ")
                                    },
                                    _ => "- ".to_string(),
                                };
                                open = Open::Item(marker);
                            }
                        },
                        "h1" | "h2" | "h3" | "h4" | "h5" | "h6" => {
                            flush(&mut cur, &mut open, &mut out);
                            if !closing {
                                open = Open::Heading;
                            }
                        },
                        "pre" if !closing => {
                            flush(&mut cur, &mut open, &mut out);
                            let body = &rest[end..];
                            let close = body.to_ascii_lowercase().find("</pre").unwrap_or(body.len());
                            out.push(Block::Code(pre_text(&body[..close])));
                            let after = &body[close..];
                            i += end + close + after.find('>').map_or(after.len(), |g| g + 1);
                            continue;
                        },
                        "b" | "strong" => cur.push_str("**"),
                        "i" | "em" => cur.push('*'),
                        "code" | "tt" => cur.push('`'),
                        _ => {},
                    }
                    i += end;
                    continue;
                }
            } else if c == '{' && rest.starts_with("{@") {
                if let Some((tag, content, end)) = inline_tag(text, i) {
                    cur.push_str(&self.inline_tag(tag, content));
                    i = end;
                    continue;
                }
            } else if c == '&' {
                if let Some((decoded, len)) = entity(rest) {
                    cur.push(decoded);
                    i += len;
                    continue;
                }
            }
            cur.push(if c == '\n' { ' ' } else { c });
            i += c.len_utf8();
        }
        cell(&mut cur, &mut table, &mut in_cell);
        flush(&mut cur, &mut open, &mut out);
        if let Some(rows) = table.take().filter(|rows| rows.iter().any(|r| !r.is_empty())) {
            // An unclosed `<table>` still keeps its cells apart.
            out.push(Block::Table(rows.into_iter().filter(|r| !r.is_empty()).collect()));
        }
        out
    }

    /// The blocks of `text` as one line, for a list item.
    fn inline_text(&self, text: &str) -> String {
        self.blocks(text)
            .into_iter()
            .map(|b| match b {
                Block::Para(t) | Block::Heading(t) => t,
                Block::Item(m, t) => format!("{m}{t}"),
                Block::Code(c) => format!("`{}`", c.split_whitespace().collect::<Vec<_>>().join(" ")),
                Block::Table(rows) => rows.iter().map(|r| r.join(" | ")).collect::<Vec<_>>().join("; "),
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// One inline tag, rendered.
    fn inline_tag(&self, tag: &str, content: &str) -> String {
        match tag {
            "code" => code_span(&self.code(content.trim())),
            "literal" => content.trim().to_string(),
            "link" | "linkplain" => {
                let (reference, label) = split_link(content);
                if label.is_empty() {
                    format!("`{}`", self.rust_ref(reference))
                } else {
                    self.inline_text(label)
                }
            },
            "value" => {
                let reference = content.trim();
                if reference.is_empty() {
                    String::new()
                } else {
                    format!("`{}`", self.rust_ref(reference))
                }
            },
            "inheritDoc" | "docRoot" => String::new(),
            _ => content.trim().to_string(),
        }
    }

    /// A `@see` reference: a Java member, an `<a href>` or a quoted string.
    fn see_ref(&self, text: &str) -> String {
        let text = text.trim();
        if let Some(quoted) = text.strip_prefix('"') {
            return quoted.trim_end_matches('"').to_string();
        }
        if text.starts_with('<') {
            return self.inline_text(text);
        }
        let (reference, label) = split_link(text);
        if label.is_empty() {
            format!("`{}`", self.rust_ref(reference))
        } else {
            self.inline_text(label)
        }
    }

    /// A Java reference `pkg.Class#member(args)` as a Rust path: the class's
    /// Rust name, the member snake_cased (`new` for a constructor, a constant
    /// unchanged), `Self` for a member of the documented class.
    fn rust_ref(&self, reference: &str) -> String {
        let (class, member) = match reference.split_once('#') {
            Some((c, m)) => (c, Some(m)),
            None => (reference, None),
        };
        let simple = class.rsplit('.').next().unwrap_or(class);
        let owner = if simple.is_empty() {
            "Self".to_string()
        } else {
            self.class_name(simple)
        };
        let Some(member) = member else { return owner };
        let name = member.split('(').next().unwrap_or(member);
        let rust = if !simple.is_empty() && name == simple {
            "new".to_string()
        } else if is_constant(name) {
            name.to_string()
        } else {
            java::rust_method_bases(name).swap_remove(0)
        };
        format!("{owner}::{rust}")
    }

    /// The content of a `{@code}`: a method call snake_cased (`send(record)`
    /// → `send(record)`, `#fooBar()` → `foo_bar()`), `null` as `None`.
    fn code(&self, content: &str) -> String {
        let content = content.strip_prefix('#').unwrap_or(content);
        let ident_len = content
            .find(|c: char| !(c.is_alphanumeric() || c == '_'))
            .unwrap_or(content.len());
        let (ident, rest) = content.split_at(ident_len);
        if ident.starts_with(|c: char| c.is_ascii_lowercase()) && rest.starts_with('(') {
            return self.rename(&format!("{}{rest}", java::rust_method_bases(ident).swap_remove(0)));
        }
        self.rename(content)
    }

    /// The Rust name of Java class `simple`: the indexed class's
    /// [`java::JavaClass::rust_name`], else a JDK exception's `Local…Error`,
    /// else the name with CLAUDE.md §2's word substitutions.
    fn class_name(&self, simple: &str) -> String {
        let simple = simple.split('<').next().unwrap_or(simple);
        if let Some(index) = self.index {
            if let Some(class) = index.classes.iter().find(|c| !c.is_test && c.name() == simple) {
                return class.rust_name();
            }
        }
        self.jdk_name(simple)
    }

    /// The Rust name of a class not in the index: a JDK exception's
    /// `Local…Error` (only known with an index), else the name with CLAUDE.md
    /// §2's word substitutions.
    fn jdk_name(&self, simple: &str) -> String {
        if self.index.is_some() && simple.ends_with("Exception") {
            return format!("Local{}", java::translate_camel_words(simple));
        }
        java::translate_camel_words(simple)
    }

    /// `text` with `null` as `None` and Java exception class names as their
    /// Rust names, word by word.
    fn rename(&self, text: &str) -> String {
        let mut out = String::with_capacity(text.len());
        let mut word = String::new();
        let flush = |word: &mut String, out: &mut String| {
            if word == "null" {
                out.push_str("None");
            } else if word.starts_with(char::is_uppercase)
                && (word.ends_with("Exception") || word.ends_with("Exceptions"))
            {
                let plural = word.ends_with('s');
                let class = self.class_name(word.trim_end_matches('s'));
                out.push_str(&class);
                if plural {
                    out.push('s');
                }
            } else {
                out.push_str(word);
            }
            word.clear();
        };
        for c in text.chars() {
            if c.is_alphanumeric() || c == '_' {
                word.push(c);
            } else {
                flush(&mut word, &mut out);
                out.push(c);
            }
        }
        flush(&mut word, &mut out);
        out
    }
}

/// Whether `name` is a constant's: upper case, digits and underscores.
fn is_constant(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

/// `code` as a Markdown code span, with enough backticks to hold its own.
fn code_span(code: &str) -> String {
    if code.contains('`') {
        format!("`` {code} ``")
    } else {
        format!("`{code}`")
    }
}

/// The HTML tag at the start of `s`: its lowercase name, whether it closes,
/// and its length. `None` when `<` starts no tag (`a < b`).
fn html_tag(s: &str) -> Option<(String, bool, usize)> {
    let inner = s.strip_prefix('<')?;
    let (closing, name_start) = match inner.strip_prefix('/') {
        Some(rest) => (true, rest),
        None => (false, inner),
    };
    if !name_start.starts_with(|c: char| c.is_ascii_alphabetic()) && !inner.starts_with('!') {
        return None;
    }
    let end = s.find('>')?;
    let name: String = name_start
        .chars()
        .take_while(char::is_ascii_alphanumeric)
        .collect::<String>()
        .to_ascii_lowercase();
    Some((name, closing, end + 1))
}

/// The HTML entity at the start of `s`, decoded, and its length.
fn entity(s: &str) -> Option<(char, usize)> {
    let end = s.get(..10.min(s.len()))?.find(';')?;
    let decoded = match &s[1..end] {
        "lt" => '<',
        "gt" => '>',
        "amp" => '&',
        "quot" => '"',
        "apos" => '\'',
        "nbsp" => ' ',
        num if num.starts_with('#') => {
            let digits = &num[1..];
            let code = match digits.strip_prefix(['x', 'X']) {
                Some(hex) => u32::from_str_radix(hex, 16).ok()?,
                None => digits.parse().ok()?,
            };
            char::from_u32(code)?
        },
        _ => return None,
    };
    Some((decoded, end + 1))
}

/// The lines of a `<pre>` block: a `{@code ..}` wrapper unwrapped, entities
/// decoded, the common indentation and the blank edges removed.
fn pre_text(raw: &str) -> String {
    let mut text = raw.trim().to_string();
    if let Some(inner) = text.strip_prefix("{@code").and_then(|t| t.trim_end().strip_suffix('}')) {
        text = inner.to_string();
    }
    let mut decoded = String::with_capacity(text.len());
    let mut i = 0;
    while i < text.len() {
        let rest = &text[i..];
        if let Some((c, len)) = rest.starts_with('&').then(|| entity(rest)).flatten() {
            decoded.push(c);
            i += len;
        } else {
            let c = rest.chars().next().unwrap_or(' ');
            decoded.push(c);
            i += c.len_utf8();
        }
    }
    let lines: Vec<&str> = decoded.lines().collect();
    let indent = lines
        .iter()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.len() - l.trim_start().len())
        .min()
        .unwrap_or(0);
    let lines: Vec<&str> = lines.iter().map(|l| l.get(indent..).unwrap_or("").trim_end()).collect();
    let first = lines.iter().position(|l| !l.is_empty()).unwrap_or(lines.len());
    let last = lines.iter().rposition(|l| !l.is_empty()).map_or(first, |l| l + 1);
    lines[first..last].join("\n")
}

/// `s` with its whitespace runs collapsed to one space, trimmed.
fn collapse(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Blocks as Markdown: list items on consecutive lines, everything else
/// separated by a blank line.
fn render(blocks: &[Block]) -> String {
    let mut out = String::new();
    let mut prev_item = false;
    for block in blocks {
        let is_item = matches!(block, Block::Item(..));
        if !out.is_empty() {
            out.push_str(if is_item && prev_item { "\n" } else { "\n\n" });
        }
        match block {
            Block::Para(t) => out.push_str(t),
            Block::Item(marker, t) => {
                out.push_str(marker);
                out.push_str(t);
            },
            Block::Heading(t) => {
                out.push_str("# ");
                out.push_str(t);
            },
            Block::Code(c) => {
                out.push_str("```java\n");
                out.push_str(c);
                out.push_str("\n```");
            },
            Block::Table(rows) => {
                // A Markdown table: the first row is the header.
                let width = rows.iter().map(Vec::len).max().unwrap_or(0);
                let line = |cells: &[String]| {
                    let mut l = String::from("|");
                    for i in 0..width {
                        let text = cells.get(i).map_or("", String::as_str);
                        l.push(' ');
                        l.push_str(text);
                        l.push_str(" |");
                    }
                    l
                };
                out.push_str(&line(&rows[0]));
                out.push('\n');
                out.push_str(&format!("|{}", " --- |".repeat(width)));
                for row in &rows[1..] {
                    out.push('\n');
                    out.push_str(&line(row));
                }
            },
        }
        prev_item = is_item;
    }
    out
}

// ---------------------------------------------------------------------------
// Rust doc comments
// ---------------------------------------------------------------------------

/// The doc comment of `attrs`: every `#[doc = ".."]` (`///`) line, one leading
/// space removed. `None` when there is none.
pub fn doc_text(attrs: &[syn::Attribute]) -> Option<String> {
    let mut lines: Vec<String> = Vec::new();
    for attr in attrs {
        let syn::Meta::NameValue(nv) = &attr.meta else { continue };
        if !nv.path.is_ident("doc") {
            continue;
        }
        let syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(s), .. }) = &nv.value else {
            continue;
        };
        // `split`, not `lines`: an empty `///` line is a blank line.
        for line in s.value().split('\n') {
            lines.push(line.strip_prefix(' ').unwrap_or(line).trim_end().to_string());
        }
    }
    let text = lines.join("\n").trim().to_string();
    (!text.is_empty()).then_some(text)
}

/// The first paragraph of `attrs`' doc comment, joined into one line, with
/// intra-doc links reduced to their text (C has nothing for them to resolve to).
pub fn doc_summary(attrs: &[syn::Attribute]) -> String {
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
pub fn strip_links(text: &str) -> String {
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

/// The parts of a translated item's doc (documentation.md §2).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct DocParts {
    /// Everything before `# Translation notes` (or `# Safety`).
    pub original: String,
    /// The text under `# Translation notes`, its heading excluded.
    pub notes: Option<String>,
    /// The `# Safety` section, heading included.
    pub safety: Option<String>,
    /// The headings of `original`, without their `#`s.
    pub original_headings: Vec<String>,
    /// How the layout is broken, one line per defect.
    pub layout: Vec<String>,
    /// The lines starting `WARNING:` inside the notes, and outside them.
    pub warnings_in_notes: usize,
    pub warnings_outside_notes: usize,
}

/// The heading of the notes section.
pub const NOTES_HEADING: &str = "Translation notes";

/// Splits a doc into its original part, notes and safety section, and checks
/// their order (documentation.md §2, §6).
pub fn doc_parts(doc: &str) -> DocParts {
    #[derive(PartialEq)]
    enum Part {
        Original,
        Notes,
        Safety,
    }
    let mut parts = DocParts::default();
    let mut part = Part::Original;
    let (mut original, mut notes, mut safety) = (Vec::new(), Vec::new(), Vec::new());
    let mut fence = false;
    let mut saw_notes = false;
    for line in doc.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("```") {
            fence = !fence;
        }
        let heading = (!fence)
            .then(|| {
                let hashes = trimmed.len() - trimmed.trim_start_matches('#').len();
                (hashes > 0 && trimmed[hashes..].starts_with(' ')).then(|| trimmed[hashes..].trim().to_string())
            })
            .flatten();
        if let Some(h) = &heading {
            if h == NOTES_HEADING {
                saw_notes = true;
                match part {
                    Part::Original => part = Part::Notes,
                    Part::Notes => parts.layout.push(format!("`# {NOTES_HEADING}` appears twice")),
                    Part::Safety => parts
                        .layout
                        .push(format!("`# Safety` comes before `# {NOTES_HEADING}`; it must be last")),
                }
                continue;
            }
            if h == "Safety" {
                part = Part::Safety;
            } else if part == Part::Notes {
                parts.layout.push(format!(
                    "heading `# {h}` inside `# {NOTES_HEADING}`: use paragraphs or lists there"
                ));
            } else if part == Part::Safety {
                parts
                    .layout
                    .push(format!("heading `# {h}` after `# Safety`, which must be last"));
            } else {
                parts.original_headings.push(h.clone());
            }
        }
        if !fence && is_warning(trimmed) {
            if part == Part::Notes {
                parts.warnings_in_notes += 1;
            } else {
                parts.warnings_outside_notes += 1;
            }
        }
        match part {
            Part::Original => original.push(line),
            Part::Notes => notes.push(line),
            Part::Safety => safety.push(line),
        }
    }
    if parts.warnings_outside_notes > 0 {
        parts.layout.push(format!("a `WARNING:` line outside `# {NOTES_HEADING}`"));
    }
    parts.original = original.join("\n").trim().to_string();
    parts.notes = saw_notes.then(|| notes.join("\n").trim().to_string());
    let safety_text = safety.join("\n").trim().to_string();
    parts.safety = (!safety_text.is_empty()).then_some(safety_text);
    parts
}

/// Whether a doc line is a contract-change marker: it starts `WARNING:`,
/// possibly as a list item.
fn is_warning(trimmed: &str) -> bool {
    trimmed.trim_start_matches(['-', '*', ' ']).starts_with("WARNING:")
}

// ---------------------------------------------------------------------------
// Size
// ---------------------------------------------------------------------------

/// The words of a doc (documentation.md §5): runs of letters, digits and
/// `_`, after Markdown links are reduced to their text, and each code span as
/// one word, so a symbol such as `kafka_common_Error_is_kafka_error` or
/// `Self::foo_bar` is one. Heading lines and code-fence lines are not
/// counted.
pub fn word_count(text: &str) -> usize {
    let mut fence = false;
    let mut kept: Vec<&str> = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("```") {
            fence = !fence;
            continue;
        }
        let hashes = trimmed.len() - trimmed.trim_start_matches('#').len();
        if !fence && hashes > 0 && trimmed[hashes..].starts_with(' ') {
            continue;
        }
        kept.push(line);
    }
    let text = strip_links(&kept.join("\n"));
    let parts: Vec<&str> = text.split('`').collect();
    // With an odd number of backticks the last one opens no span.
    let spans = if parts.len().is_multiple_of(2) {
        parts.len() - 2
    } else {
        parts.len()
    };
    parts
        .iter()
        .enumerate()
        .map(|(i, part)| {
            if i % 2 == 1 && i < spans {
                usize::from(!part.trim().is_empty())
            } else {
                part.split(|c: char| !(c.is_alphanumeric() || c == '_'))
                    .filter(|w| !w.is_empty())
                    .count()
            }
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rustdoc(doc: &str) -> String {
        Translator { index: None, panics: false }.to_rustdoc(doc)
    }

    #[test]
    fn test_parse_splits_body_and_block_tags() {
        let doc = parse("Sends.\n<pre>\n@notATag\n</pre>\n@param record the\n  record\n@return a future\n@since 0.10");
        assert_eq!(doc.body, "Sends.\n<pre>\n@notATag\n</pre>");
        assert_eq!(
            doc.tags,
            [
                BlockTag { name: "param".into(), arg: "record".into(), text: "the\n  record".into() },
                BlockTag { name: "return".into(), arg: String::new(), text: "a future".into() },
                BlockTag { name: "since".into(), arg: String::new(), text: "0.10".into() },
            ]
        );
    }

    #[test]
    fn test_body_text_and_paragraphs() {
        assert_eq!(
            rustdoc("One line\ncontinued.\n<p>\nSecond paragraph.</p>"),
            "One line continued.\n\nSecond paragraph."
        );
    }

    #[test]
    fn test_lists() {
        assert_eq!(
            rustdoc("Either:\n<ul>\n<li>a</li>\n<li>b\n</ul>\nAfter."),
            "Either:\n\n- a\n- b\n\nAfter."
        );
        assert_eq!(rustdoc("<ol><li>first<li>second</ol>"), "1. first\n2. second");
        // An `<li>` outside any list (`ProducerRecord`'s class doc) is a bullet.
        assert_eq!(rustdoc("Cases:\n<li>one</li>\n<li>two</li>"), "Cases:\n\n- one\n- two");
    }

    #[test]
    fn test_tables() {
        // `GroupState`'s table: cells kept apart, empty cells kept, a
        // Markdown table.
        let doc = "The table:\n<table>\n<thead>\n<tr><th>State</th><th>Classic</th><th>Streams</th></tr>\n</thead>\n\
                   <tbody>\n<tr><td>UNKNOWN</td><td>Yes</td><td>Yes</td></tr>\n\
                   <tr><td>NOT_READY</td><td></td><td>Yes</td></tr>\n</tbody>\n</table>\nAfter.";
        assert_eq!(
            rustdoc(doc),
            "The table:\n\n| State | Classic | Streams |\n| --- | --- | --- |\n| UNKNOWN | Yes | Yes |\n\
             | NOT_READY |  | Yes |\n\nAfter."
        );
        // Words count once per cell; the separators count nothing.
        assert_eq!(word_count(&rustdoc(doc)), 11);
        // A caption stays a paragraph; an unclosed table still separates cells.
        assert_eq!(
            rustdoc("<table><caption>Modes</caption><tr><td>a</td><td>b</td>"),
            "Modes\n\n| a | b |\n| --- | --- |"
        );
    }

    #[test]
    fn test_headings() {
        assert_eq!(rustdoc("<h3>Usage Examples</h3>\nText."), "# Usage Examples\n\nText.");
    }

    #[test]
    fn test_pre_and_code() {
        assert_eq!(
            rustdoc("Example:\n<pre>{@code\n    Map<String, Object> m = new HashMap<>();\n    m.put(&quot;a&quot;, 1);\n}</pre>\nDone."),
            "Example:\n\n```java\nMap<String, Object> m = new HashMap<>();\nm.put(\"a\", 1);\n```\n\nDone."
        );
        assert_eq!(
            rustdoc("Use {@code max.block.ms} or <code>x</code>."),
            "Use `max.block.ms` or `x`."
        );
        assert_eq!(
            rustdoc("Call {@code commitSync()} or {@code #fooBar(x)}."),
            "Call `commit_sync()` or `foo_bar(x)`."
        );
        assert_eq!(rustdoc("A {@code Map<K, {V}>}."), "A `Map<K, {V}>`.");
    }

    #[test]
    fn test_bold_and_italic() {
        assert_eq!(
            rustdoc("<b>Note</b> and <i>this</i>, <strong>s</strong> <em>e</em>."),
            "**Note** and *this*, **s** *e*."
        );
    }

    #[test]
    fn test_links() {
        assert_eq!(
            rustdoc("See {@link org.apache.kafka.common.TopicPartition#topic() the topic}."),
            "See the topic."
        );
        assert_eq!(
            rustdoc("See {@linkplain KafkaProducer#send(ProducerRecord) send}."),
            "See send."
        );
        assert_eq!(
            rustdoc("See {@link KafkaProducer#initTransactions()}."),
            "See `KafkaProducer::init_transactions`."
        );
        assert_eq!(rustdoc("See {@link #partitionsFor(String)}."), "See `Self::partitions_for`.");
        assert_eq!(
            rustdoc("See {@link #NO_TIMESTAMP} and {@link Foo}."),
            "See `Self::NO_TIMESTAMP` and `Foo`."
        );
        assert_eq!(
            rustdoc("Built by {@link ProducerRecord#ProducerRecord(String, Object)}."),
            "Built by `ProducerRecord::new`."
        );
    }

    #[test]
    fn test_param_tag() {
        assert_eq!(
            rustdoc("Sends.\n@param record The record\n@param maxBlockMs how long\n@param <K> the key type"),
            "Sends.\n\n# Arguments\n\n- `record` - The record\n- `max_block_ms` - how long\n- `K` - the key type"
        );
    }

    #[test]
    fn test_return_tag() {
        assert_eq!(
            rustdoc("Gets.\n@return the {@code null} value"),
            "Gets.\n\n# Returns\n\nthe `None` value"
        );
    }

    #[test]
    fn test_throws_tag() {
        assert_eq!(
            rustdoc("Waits.\n@throws org.apache.kafka.common.errors.TimeoutException if it times out\n@exception InterruptException on interrupt"),
            "Waits.\n\n# Errors\n\n- `TimeoutError` if it times out\n- `InterruptError` on interrupt"
        );
        let panics =
            Translator { index: None, panics: true }.to_rustdoc("Gets.\n@throws IllegalStateException if closed");
        assert_eq!(panics, "Gets.\n\n# Panics\n\n- `IllegalStateError` if closed");
    }

    #[test]
    fn test_throws_uses_the_indexed_rust_name() {
        let mut classes = Vec::new();
        crate::java::push_classes(
            &["clients".into(), "consumer".into()],
            "package x; public class OffsetOutOfRangeException extends RuntimeException {}",
            false,
            &mut classes,
        );
        let index = JavaIndex { classes };
        let t = Translator { index: Some(&index), panics: false };
        assert_eq!(
            t.to_rustdoc("Seeks.\n@throws OffsetOutOfRangeException if out of range\n@throws IllegalArgumentException if bad"),
            "Seeks.\n\n# Errors\n\n- `ConsumerOffsetOutOfRangeError` if out of range\n- `LocalIllegalArgumentError` if bad"
        );
        assert_eq!(
            t.to_rustdoc("May throw a OffsetOutOfRangeException."),
            "May throw a ConsumerOffsetOutOfRangeError."
        );
    }

    #[test]
    fn test_see_tag() {
        assert_eq!(
            rustdoc("Body.\n@see #close()\n@see KafkaConsumer#poll(Duration) polling\n@see \"The docs\"\n@see <a href=\"https://kafka.apache.org\">Kafka</a>"),
            "Body.\n\n# See also\n\n- `Self::close`\n- polling\n- The docs\n- Kafka"
        );
    }

    #[test]
    fn test_dropped_and_kept_tags() {
        assert_eq!(
            rustdoc("Body.\n@since 3.0\n@author someone\n@deprecated use another\n@apiNote An api note.\n@implNote An impl note."),
            "Body.\n\nAn api note.\n\nAn impl note."
        );
    }

    #[test]
    fn test_inherit_doc_tag() {
        let own = "Before {@inheritDoc} after.\n@param x {@inheritDoc}";
        let inherited = "The parent body.\n@param x the parent x\n@return the parent result";
        assert_eq!(
            inherit_doc(own, inherited),
            "Before The parent body. after.\n@param x the parent x\n@return  the parent result"
        );
        assert_eq!(
            rustdoc(&inherit_doc(own, inherited)),
            "Before The parent body. after.\n\n# Arguments\n\n- `x` - the parent x\n\n# Returns\n\nthe parent result"
        );
    }

    #[test]
    fn test_null_becomes_none_and_entities_decode() {
        assert_eq!(
            rustdoc("May be null; a &lt; b &amp;&amp; c &#64;."),
            "May be None; a < b && c @."
        );
        // A word containing `null` is left alone.
        assert_eq!(rustdoc("nullable"), "nullable");
    }

    #[test]
    fn test_plain_text_keeps_the_python_form() {
        let t = plain_text("/**\n * The base {@link Foo} of <p> all {@code x}.\n * @see Bar\n */");
        assert_eq!(t, "The base ``Foo`` of\n\nall ``x``.");
        assert_eq!(
            plain_text("/** See {@link A#b(C, D) the b} and {@link #c}. */"),
            "See ``the b`` and ``c``."
        );
    }

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
    fn test_word_count() {
        // Links count by their text; heading and fence lines are not
        // counted, the code between fences is.
        assert_eq!(
            word_count("# Returns\n\nReturns [`None`](Option::None) if it isn't.\n```java\nx y\n```"),
            8
        );
        assert_eq!(word_count("## Not a heading inside\n```\n# kept\n```"), 1);
        assert_eq!(word_count(""), 0);
        // A code span, or a symbol, is one word; an unpaired backtick opens
        // none.
        assert_eq!(word_count("names `kafka_common_Error_is_kafka_error` or `Self::foo_bar`"), 4);
        assert_eq!(word_count("out_err is set"), 3);
        assert_eq!(word_count("a ` b c"), 3);
    }

    #[test]
    fn test_translate_splits_the_tags() {
        let doc = "Sends.\n@param maxBlockMs how long\n@return the {@code null} value\n@throws java.util.concurrent.TimeoutException late";
        let t = Translator { index: None, panics: false }.translate(doc);
        assert_eq!(t.params, [("max_block_ms".to_string(), "how long".to_string())]);
        assert_eq!(t.returns.as_deref(), Some("the `None` value"));
        assert_eq!(t.throws[0].java, "TimeoutException");
        assert_eq!(t.throws[0].rust, "TimeoutError");
        assert_eq!(t.rustdoc, Translator { index: None, panics: false }.to_rustdoc(doc));
    }

    #[test]
    fn test_error_names() {
        let mut classes = Vec::new();
        crate::java::push_classes(
            &["common".into(), "errors".into()],
            "package x; public class TimeoutException extends RuntimeException {}",
            false,
            &mut classes,
        );
        let index = JavaIndex { classes };
        let t = Translator { index: Some(&index), panics: false };
        assert_eq!(t.error_names("TimeoutException"), ["TimeoutError"]);
        assert_eq!(
            t.error_names("org.apache.kafka.common.errors.TimeoutException"),
            ["TimeoutError"]
        );
        // A JDK class, qualified or not indexed, is the `Local` one.
        assert_eq!(t.error_names("java.util.concurrent.TimeoutException"), ["LocalTimeoutError"]);
        assert_eq!(t.error_names("IllegalStateException"), ["LocalIllegalStateError"]);
    }

    #[test]
    fn test_doc_parts_layout() {
        let good = doc_parts(
            "Java text.\n\n# Errors\n\n- x\n\n# Translation notes\n\nWARNING: differs.\n\n# Safety\n\nPointers.",
        );
        assert_eq!(good.original, "Java text.\n\n# Errors\n\n- x");
        assert_eq!(good.notes.as_deref(), Some("WARNING: differs."));
        assert_eq!(good.safety.as_deref(), Some("# Safety\n\nPointers."));
        assert_eq!(good.original_headings, ["Errors"]);
        assert!(good.layout.is_empty(), "{:?}", good.layout);
        assert_eq!(good.warnings_in_notes, 1);

        let safety_first = doc_parts("Text.\n\n# Safety\n\nP.\n\n# Translation notes\n\nN.");
        assert_eq!(
            safety_first.layout,
            ["`# Safety` comes before `# Translation notes`; it must be last"]
        );
        let twice = doc_parts("T.\n\n# Translation notes\n\nA.\n\n# Translation notes\n\nB.");
        assert_eq!(twice.layout, ["`# Translation notes` appears twice"]);
        let heading_in_notes = doc_parts("T.\n\n# Translation notes\n\n# Errors\n\nB.");
        assert_eq!(
            heading_in_notes.layout,
            ["heading `# Errors` inside `# Translation notes`: use paragraphs or lists there"]
        );
        let warning_outside = doc_parts("WARNING: not here.\n\n# Translation notes\n\nN.");
        assert_eq!(warning_outside.layout, ["a `WARNING:` line outside `# Translation notes`"]);
        // A `#` line inside a code block is not a heading.
        let fenced = doc_parts("T.\n\n```\n# not a heading\n```");
        assert!(fenced.original_headings.is_empty());
        assert_eq!(fenced.notes, None);
    }
}
