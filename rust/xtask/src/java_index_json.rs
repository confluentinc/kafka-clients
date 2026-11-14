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

//! The Node.js client's Java index, `javascript/java-index.json`: the
//! [`JavaIndex`] the Rust lint builds, written as JSON by `cargo xtask
//! java-api` ([`crate::java_api`], in the same scan as the .NET binding's
//! list) and committed, so the client's rule linter and unit tests read Java
//! without the `kafka` submodule.
//!
//! The file holds the main classes only, sorted by package then nested path,
//! one class per line, and only what the client's readers use (its loader,
//! `javascript/scripts/lint/java_index.js`, fills in what is left out):
//!
//! ```text
//! { "schema": 2, "audience_ref": "4.4.0-rc3", "deprecation_refs": ["4.3.1", "4.4.0-rc3"],
//!   "classes": [
//!     { "package", "path", "kind"?, "visibility"?, "abstract"?, "deprecated"?,
//!       "public_audience"?, "unsupported_package"?, "supertypes"?, "supertype_fqns"?,
//!       "overloads"?: [ { "name", "params", "param_names"?, "param_types"?, "return_type"?,
//!                         "visibility"?, "static"?, "default"?, "deprecated"?, "fixed_message"? } ],
//!       "fields"?: [ { "name", "type", "visibility"?, "static"?, "final"?, "deprecated"?, "value"? } ],
//!       "enum_constants"? } ] }
//! ```
//!
//! A key marked `?` is left out at its default: `kind` "class", `visibility`
//! "public", a flag `false`, a list empty, `return_type` `null`, and
//! `param_types` equal to `params`; a private overload has no `param_names`,
//! `param_types` or `return_type` (it is only ever named: a translation of it
//! is reported as not public). `params` holds the erased simple type
//! names the form keys use (`BiConsumer`), `param_types` the types as written
//! (`BiConsumer<? super T, ? super Throwable>`). The class's fully qualified
//! name is `org.apache.kafka.<package>.<path joined by $>`.
//!
//! Besides what the scanner reads, three things are read from the source of a
//! top-level class, for the client's unit tests:
//!
//! - `value`: the literal of a `public static final String NAME = "literal";`;
//! - `fixed_message`: the text of the string literals a Throwable class's
//!   constructor passes first to its `super(...)` / `this(...)` call;
//! - `supertype_fqns`: each supertype resolved by [`JavaIndex::supertype`],
//!   `null` for a JDK type.
//!
//! Before writing, `java-api` checks the index for fields the scanner may
//! take from what is not a field (a `throws` clause, a lambda body) and fails
//! if it finds one, so the readers never see one.
//!
//! The JSON is written by hand (xtask takes no serialization dependency), with
//! keys in a fixed order, so the output is byte-for-byte deterministic.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use crate::java::{self, Deprecation, JavaClass, JavaIndex, JavaKind, Visibility};

/// The schema version, raised on any incompatible change to the layout.
const SCHEMA: usize = 2;

/// The source of `class`'s top-level class, read from the `kafka` submodule.
pub fn source_of(class: &JavaClass) -> Option<String> {
    let mut path = Path::new(java::JAVA_MAIN_ROOT).to_path_buf();
    for part in class.package.split('.') {
        path.push(part);
    }
    path.push(format!("{}.java", class.path.first()?));
    fs::read_to_string(path).ok()
}

/// The fields of `index`'s main classes that cannot be fields, as
/// `Class#name (type)`: the type is empty, is `throws` or holds a lambda's
/// `->`, or the name is a Throwable class's, as a lone `throws X;` leaves it.
pub fn phantom_fields(index: &JavaIndex) -> Vec<String> {
    let throwables: BTreeSet<&str> = index
        .classes
        .iter()
        .filter(|c| !c.is_test && index.is_throwable(c))
        .map(JavaClass::name)
        .collect();
    let mut out = Vec::new();
    for class in index.classes.iter().filter(|c| !c.is_test) {
        for name in class.fields.keys() {
            let ty = class.field_decls.get(name).map_or("", |d| d.ty.as_str());
            if ty.is_empty() || ty == "throws" || ty.contains("->") || throwables.contains(name.as_str()) {
                out.push(format!("{}#{name} ({ty:?})", class.marker()));
            }
        }
    }
    out
}

/// The JSON document of `index`'s main classes. `public` is the Public list
/// (top-level fully-qualified names), `unsupported` the dotted packages below
/// `org.apache.kafka` carrying the unsupported-API disclaimer, `sources` the
/// source of a class's top-level class.
pub fn to_json(
    index: &JavaIndex,
    public: &BTreeSet<String>,
    unsupported: &BTreeSet<String>,
    sources: &dyn Fn(&JavaClass) -> Option<String>,
) -> String {
    let mut classes: Vec<&JavaClass> = index.classes.iter().filter(|c| !c.is_test).collect();
    classes.sort_by(|a, b| (&a.package, &a.path).cmp(&(&b.package, &b.path)));
    let mut read: BTreeMap<(String, String), Option<String>> = BTreeMap::new();
    let mut out = String::new();
    out.push_str("{\"schema\":");
    out.push_str(&SCHEMA.to_string());
    out.push_str(",\"audience_ref\":");
    out.push_str(&string(java::AUDIENCE_REF));
    out.push_str(",\"deprecation_refs\":");
    out.push_str(&array(java::DEPRECATION_REFS.iter().map(|r| string(r))));
    out.push_str(",\"classes\":[");
    for (n, class) in classes.iter().enumerate() {
        let top = (class.package.clone(), class.path[0].clone());
        let source = read.entry(top).or_insert_with(|| sources(class)).clone();
        out.push_str(if n == 0 { "\n" } else { ",\n" });
        out.push_str(&class_json(index, class, public, unsupported, source.as_deref()));
    }
    out.push_str("\n]}\n");
    out
}

/// One class's object, its keys left out at their defaults.
fn class_json(
    index: &JavaIndex,
    class: &JavaClass,
    public: &BTreeSet<String>,
    unsupported: &BTreeSet<String>,
    source: Option<&str>,
) -> String {
    let top_level = format!("{}{}.{}", java::MARKER_PREFIX, class.package, class.path[0]);
    let simple = class.name();
    let literals = if class.path.len() == 1 {
        source.map(string_literals).unwrap_or_default()
    } else {
        BTreeMap::new()
    };
    let messages = if class.path.len() == 1 && index.is_throwable(class) {
        source.map(|s| fixed_messages(s, simple)).unwrap_or_default()
    } else {
        Vec::new()
    };
    let overloads: Vec<String> = class
        .overloads
        .iter()
        .map(|o| {
            let signature = o.signature();
            let mut e = vec![
                ("name", string(&o.name)),
                ("params", array(o.params.iter().map(|p| string(p)))),
            ];
            // A private overload is only ever named (a translation of it is
            // reported as not public): its parameter names and types as
            // written, and its return type, are left out.
            if o.visibility != Visibility::Private {
                e.push(("param_names", array(o.param_names.iter().map(|p| string(p)))));
                if o.param_types != o.params {
                    e.push(("param_types", array(o.param_types.iter().map(|p| string(p)))));
                }
                if let Some(r) = &o.return_type {
                    e.push(("return_type", string(r)));
                }
            }
            if o.visibility != Visibility::Public {
                e.push(("visibility", string(visibility(o.visibility))));
            }
            flag(&mut e, "static", o.is_static);
            flag(&mut e, "default", o.is_default);
            flag(&mut e, "deprecated", class.deprecation(Some(&signature)) == Deprecation::Yes);
            if o.name == simple && o.return_type.is_none() {
                if let Some((_, text)) = messages.iter().find(|(params, _)| *params == o.params) {
                    e.push(("fixed_message", string(text)));
                }
            }
            object(&e)
        })
        .collect();
    let fields: Vec<String> = class
        .fields
        .iter()
        .map(|(name, deprecated)| {
            let decl = class.field_decls.get(name);
            let vis = class.field_visibility.get(name).copied().unwrap_or(Visibility::Package);
            let mut e = vec![
                ("name", string(name)),
                ("type", decl.map_or_else(null, |d| string(&d.ty))),
            ];
            if vis != Visibility::Public {
                e.push(("visibility", string(visibility(vis))));
            }
            flag(&mut e, "static", decl.is_some_and(|d| d.is_static));
            flag(&mut e, "final", decl.is_some_and(|d| d.is_final));
            flag(&mut e, "deprecated", *deprecated || class.deprecated);
            if let Some(value) = literals.get(name) {
                e.push(("value", string(value)));
            }
            object(&e)
        })
        .collect();
    let mut e = vec![
        ("package", string(&class.package)),
        ("path", array(class.path.iter().map(|p| string(p)))),
    ];
    if class.kind != JavaKind::Class {
        e.push(("kind", string(kind(class.kind))));
    }
    if class.visibility != Visibility::Public {
        e.push(("visibility", string(visibility(class.visibility))));
    }
    flag(&mut e, "abstract", class.is_abstract);
    flag(&mut e, "deprecated", class.deprecation(None) == Deprecation::Yes);
    flag(&mut e, "public_audience", public.contains(&top_level));
    flag(&mut e, "unsupported_package", unsupported.contains(&class.package));
    if !class.supertypes.is_empty() {
        e.push(("supertypes", array(class.supertypes.iter().map(|s| string(s)))));
        e.push((
            "supertype_fqns",
            array(
                class
                    .supertypes
                    .iter()
                    .map(|s| index.supertype(class, s).map_or_else(null, |c| string(&c.marker()))),
            ),
        ));
    }
    if !overloads.is_empty() {
        e.push(("overloads", format!("[{}]", overloads.join(","))));
    }
    if !fields.is_empty() {
        e.push(("fields", format!("[{}]", fields.join(","))));
    }
    if !class.enum_constants.is_empty() {
        e.push(("enum_constants", array(class.enum_constants.iter().map(|c| string(c)))));
    }
    object(&e)
}

/// Adds `key: true` to `entries` when `value` holds; `false` is left out.
fn flag(entries: &mut Vec<(&str, String)>, key: &'static str, value: bool) {
    if value {
        entries.push((key, boolean(true)));
    }
}

/// The `public static final String NAME = "literal";` constants of `source`,
/// name to the literal's value.
pub fn string_literals(source: &str) -> BTreeMap<String, String> {
    const DECL: &str = "public static final String ";
    let mut out = BTreeMap::new();
    let mut rest = source;
    while let Some(at) = rest.find(DECL) {
        rest = &rest[at + DECL.len()..];
        let name: String = rest.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
        if name.is_empty() {
            continue;
        }
        let after = rest[name.len()..].trim_start();
        let Some(after) = after.strip_prefix('=') else { continue };
        let after = after.trim_start();
        if let Some((value, tail)) = literal(after) {
            if tail.starts_with(';') {
                out.insert(name, value);
            }
        }
    }
    out
}

/// The parameter types and fixed message of each constructor of Throwable
/// class `simple` in `source` whose body starts `super("…" + "…", …)` or
/// `this("…", …)`: the types erased to their simple names, as the index's
/// `params` gives them.
pub fn fixed_messages(source: &str, simple: &str) -> Vec<(Vec<String>, String)> {
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(rel) = source[from..].find(simple) {
        let at = from + rel;
        from = at + simple.len();
        let before = source[..at].trim_end();
        if !(before.ends_with("public") || before.ends_with("protected")) {
            continue;
        }
        let after = source[from..].trim_start();
        let Some(params_text) = after.strip_prefix('(') else {
            continue;
        };
        let Some(close) = params_text.find(')') else { continue };
        let params: Vec<String> = if params_text[..close].trim().is_empty() {
            Vec::new()
        } else {
            params_text[..close].split(',').map(erased_param).collect()
        };
        let body = params_text[close + 1..].trim_start();
        let Some(body) = body.strip_prefix('{') else { continue };
        let body = body.trim_start();
        let Some(call) = body.strip_prefix("super").or_else(|| body.strip_prefix("this")) else {
            continue;
        };
        let Some(mut call) = call.trim_start().strip_prefix('(') else {
            continue;
        };
        let mut text = String::new();
        let mut any = false;
        loop {
            call = call.trim_start();
            let Some((value, tail)) = literal(call) else { break };
            any = true;
            text.push_str(&value);
            match tail.trim_start().strip_prefix('+') {
                Some(more) => call = more,
                None => break,
            }
        }
        if any {
            out.push((params, text));
        }
    }
    out
}

/// A parameter declaration's type, erased to its simple name: `final
/// Map<String, Object> configs` -> `Map`.
fn erased_param(param: &str) -> String {
    let words: Vec<&str> = param
        .split_whitespace()
        .filter(|w| *w != "final" && !w.starts_with('@'))
        .collect();
    let ty = words[..words.len().saturating_sub(1)].join(" ");
    let base = ty.split('<').next().unwrap_or(&ty).trim();
    let simple = base.rsplit('.').next().unwrap_or(base);
    let dims = ty.matches("[]").count() + usize::from(ty.contains("..."));
    format!("{simple}{}", "[]".repeat(dims))
}

/// The Java string literal at the start of `s`, decoded, and what follows it.
fn literal(s: &str) -> Option<(String, &str)> {
    let mut chars = s.strip_prefix('"')?.char_indices();
    let mut out = String::new();
    while let Some((i, c)) = chars.next() {
        match c {
            '"' => return Some((out, &s[i + 2..])),
            '\\' => {
                let (_, e) = chars.next()?;
                match e {
                    'n' => out.push('\n'),
                    't' => out.push('\t'),
                    'r' => out.push('\r'),
                    'b' => out.push('\u{8}'),
                    'f' => out.push('\u{c}'),
                    '0' => out.push('\0'),
                    'u' => {
                        let hex: String = (0..4).filter_map(|_| chars.next().map(|(_, h)| h)).collect();
                        out.push(char::from_u32(u32::from_str_radix(&hex, 16).ok()?)?);
                    },
                    other => out.push(other),
                }
            },
            '\n' => return None,
            c => out.push(c),
        }
    }
    None
}

/// The Java keyword of `v`, `package-private` for none.
fn visibility(v: Visibility) -> &'static str {
    v.keyword()
}

/// The lower-case name of `k`.
fn kind(k: JavaKind) -> &'static str {
    match k {
        JavaKind::Class => "class",
        JavaKind::Interface => "interface",
        JavaKind::Enum => "enum",
        JavaKind::Record => "record",
    }
}

fn null() -> String {
    "null".to_string()
}

fn boolean(b: bool) -> String {
    b.to_string()
}

/// `s` as a JSON string: `"` and `\` escaped, every control character as
/// its short escape or `\u00XX`; everything else, non-ASCII included, as is.
pub fn string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// A JSON array of already-encoded values.
fn array(items: impl Iterator<Item = String>) -> String {
    let items: Vec<String> = items.collect();
    format!("[{}]", items.join(","))
}

/// A JSON object of already-encoded values, keys in the order given.
fn object(entries: &[(&str, String)]) -> String {
    let body: Vec<String> = entries.iter().map(|(k, v)| format!("{}:{v}", string(k))).collect();
    format!("{{{}}}", body.join(","))
}

/// The longest text of a member [`describe_difference`] quotes.
const QUOTED: usize = 160;

/// How a stale line of the index differs from the line `java-api` writes, in
/// a few words: the class the line holds and its first differing member, as
///
/// ```text
/// org.apache.kafka.common.Node: `overloads[2].return_type` should be `"return_type":"int"` but is `"return_type":"long"`
/// ```
///
/// (or `nothing`, for a member one line lacks). `None` for a line that holds no
/// class (the first and the last).
pub fn describe_difference(expected: &str, found: &str) -> Option<String> {
    let wanted = class_name(expected);
    let got = class_name(found);
    if wanted.is_none() && got.is_none() {
        return None;
    }
    if wanted != got {
        let name = |n: Option<String>| n.map_or_else(|| "no class".to_owned(), |n| format!("class {n}"));
        return Some(format!("it should hold {} but holds {}", name(wanted), name(got)));
    }
    let offset = expected.bytes().zip(found.bytes()).take_while(|(a, b)| a == b).count();
    let (path, start) = member_at(expected, offset);
    let quote = |line: &str| {
        let text = member_text(line, start);
        if text.is_empty() {
            return "nothing".to_owned();
        }
        if text.len() <= QUOTED {
            return format!("`{text}`");
        }
        let mut end = QUOTED;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        format!("`{}…`", &text[..end])
    };
    let path = if path.is_empty() {
        "the class".to_owned()
    } else {
        format!("`{path}`")
    };
    Some(format!(
        "class {}: {path} should be {} but is {}",
        wanted.unwrap_or_default(),
        quote(expected),
        quote(found)
    ))
}

/// The class a line holds, named as the readers name it
/// (`org.apache.kafka.<package>.<path joined by $>`).
fn class_name(line: &str) -> Option<String> {
    let package = line.strip_prefix("{\"package\":\"")?;
    let package = &package[..package.find('"')?];
    let path = &line[line.find("\"path\":[")? + 8..];
    let path = &path[..path.find(']')?];
    let path: Vec<&str> = path.split(',').map(|p| p.trim_matches('"')).collect();
    Some(format!("org.apache.kafka.{package}.{}", path.join("$")))
}

/// One open object or array while [`member_at`] walks a line.
struct Frame {
    /// Whether it is an object (else an array).
    object: bool,
    /// Where its current member (key included) or element starts.
    start: usize,
    /// The current member's key, in an object.
    name: String,
    /// The index of the current element, in an array.
    index: usize,
}

/// The path of the innermost member or element of `line` that holds `offset`
/// (`overloads[2].return_type`), and where that member starts.
fn member_at(line: &str, offset: usize) -> (String, usize) {
    let bytes = line.as_bytes();
    let mut frames: Vec<Frame> = Vec::new();
    let mut i = 0;
    // A comma where the lines part starts the member one line has and the
    // other lacks: walk it too.
    let end = if bytes.get(offset) == Some(&b',') {
        offset + 1
    } else {
        offset
    };
    while i < end.min(bytes.len()) {
        match bytes[i] {
            b'"' => {
                let begin = i + 1;
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    i += if bytes[i] == b'\\' { 2 } else { 1 };
                }
                if let Some(f) = frames.last_mut().filter(|f| f.object && f.name.is_empty()) {
                    f.name = line[begin..i.min(bytes.len())].to_owned();
                }
            },
            open @ (b'{' | b'[') => {
                frames.push(Frame { object: open == b'{', start: i + 1, name: String::new(), index: 0 })
            },
            b'}' | b']' => {
                frames.pop();
            },
            b',' => {
                if let Some(f) = frames.last_mut() {
                    f.start = i + 1;
                    f.name.clear();
                    f.index += 1;
                }
            },
            _ => {},
        }
        i += 1;
    }
    // A member the walk stopped at the start of: its key is just ahead.
    if let Some(f) = frames.last_mut().filter(|f| f.object && f.name.is_empty()) {
        if let Some(key) = line[f.start.min(line.len())..].strip_prefix('"') {
            f.name = key[..key.find('"').unwrap_or(key.len())].to_owned();
        }
    }
    let mut path = String::new();
    // The outermost frame is the class object itself: its members are the
    // path's roots.
    for f in &frames {
        if f.object {
            if !f.name.is_empty() {
                if !path.is_empty() {
                    path.push('.');
                }
                path.push_str(&f.name);
            }
        } else {
            path.push_str(&format!("[{}]", f.index));
        }
    }
    (path, frames.last().map_or(0, |f| f.start))
}

/// The member or element of `line` that starts at `start`, up to the comma or
/// bracket that ends it.
fn member_text(line: &str, start: usize) -> &str {
    let bytes = line.as_bytes();
    let mut depth = 0usize;
    let mut i = start.min(bytes.len());
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    i += if bytes[i] == b'\\' { 2 } else { 1 };
                }
            },
            b'{' | b'[' => depth += 1,
            b'}' | b']' if depth == 0 => break,
            b'}' | b']' => depth -= 1,
            b',' if depth == 0 => break,
            _ => {},
        }
        i += 1;
    }
    &line[start.min(bytes.len())..i.min(bytes.len())]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index(src: &str) -> JavaIndex {
        let mut classes = Vec::new();
        java::push_classes(&["clients".into(), "producer".into()], src, false, &mut classes);
        java::push_classes(&["clients".into(), "producer".into()], "class Hidden { }", true, &mut classes);
        JavaIndex { classes }
    }

    const FIXTURE: &str = r#"
        package org.apache.kafka.clients.producer;
        /** A "record". */
        public final class Rec<K, V> implements Comparable<Rec<K, V>> {
            /** The topic.
             * Tab	here. */
            public static final String TOPIC_CONFIG = "topic";
            private final Map<String, List<Integer>> parts;
            protected final K key;
            int count = 0;
            /**
             * Sends it.
             *
             * @param timeout how long
             * @param <T> the type
             * @return the {@code Future}
             * @throws IllegalStateException if closed
             * @exception org.apache.kafka.common.KafkaException otherwise
             */
            public Future<RecordMetadata> send(Duration timeout) { return null; }
            @Deprecated
            public static <T> Map<String, ? extends List<T>>[] all(final @Nullable String... names) { return null; }
            Rec(String topic) { }
            private final Predicate<String> accept = s -> true;
            @Deprecated Rec(int x) { }
            @java.lang.Deprecated Rec(long x) { }
            public interface Callback {
                void onCompletion(RecordMetadata metadata, Exception exception);
                void commit() throws ProducerFencedException;
            }
            public enum Mode { ON, OFF; }
        }
    "#;

    #[test]
    fn records_return_and_field_types() {
        let index = index(FIXTURE);
        let rec = index.classes.iter().find(|c| c.path == ["Rec"]).unwrap();
        let returns: Vec<(&str, Option<&str>)> = rec
            .overloads
            .iter()
            .map(|o| (o.name.as_str(), o.return_type.as_deref()))
            .collect();
        assert_eq!(
            returns,
            [
                ("send", Some("Future<RecordMetadata>")),
                ("all", Some("Map<String, ? extends List<T>>[]")),
                ("Rec", None),
                // After an annotation, a constructor still has no return type.
                ("Rec", None),
                ("Rec", None),
            ]
        );
        // Neither a lambda's body nor a lone `throws X;` is a field.
        let fields: Vec<&str> = rec.fields.keys().map(String::as_str).collect();
        assert_eq!(fields, ["TOPIC_CONFIG", "accept", "count", "key", "parts"]);
        assert_eq!(rec.field_decls["accept"].ty, "Predicate<String>");
        let ty = |name: &str| rec.field_decls[name].clone();
        assert_eq!(
            ty("TOPIC_CONFIG"),
            java::FieldDecl { ty: "String".into(), is_static: true, is_final: true }
        );
        assert_eq!(
            ty("parts"),
            java::FieldDecl { ty: "Map<String, List<Integer>>".into(), is_static: false, is_final: true }
        );
        assert_eq!(ty("key"), java::FieldDecl { ty: "K".into(), is_static: false, is_final: true });
        assert_eq!(
            ty("count"),
            java::FieldDecl { ty: "int".into(), is_static: false, is_final: false }
        );
        let mode = index.classes.iter().find(|c| c.path == ["Rec", "Mode"]).unwrap();
        assert_eq!(
            mode.field_decls["ON"],
            java::FieldDecl { ty: "Mode".into(), is_static: true, is_final: true }
        );
        let implicit: Vec<Option<&str>> = mode.overloads.iter().map(|o| o.return_type.as_deref()).collect();
        // `values()`, `valueOf(String)`, `name()`, `ordinal()`, then the
        // implicit (none here) constructor.
        assert_eq!(implicit, [Some("Mode[]"), Some("Mode"), Some("String"), Some("int")]);
        let callback = index.classes.iter().find(|c| c.path == ["Rec", "Callback"]).unwrap();
        assert_eq!(callback.overloads[0].return_type.as_deref(), Some("void"));
        assert!(callback.fields.is_empty(), "{:?}", callback.fields);
        assert!(phantom_fields(&index).is_empty(), "{:?}", phantom_fields(&index));
    }

    #[test]
    fn param_types_keep_the_type_arguments_the_erased_params_drop() {
        let index = index(
            r#"
            package org.apache.kafka.clients.producer;
            public abstract class Fut<T> {
                public abstract Fut<T> whenComplete(BiConsumer<? super T, ? super Throwable> action);
                public static Fut<Void> allOf(Fut<?>... futures) { return null; }
                public abstract T get(long timeout, final @Nullable TimeUnit unit);
            }
            "#,
        );
        let fut = index.classes.iter().find(|c| c.path == ["Fut"]).unwrap();
        let of = |name: &str| fut.overloads.iter().find(|o| o.name == name).unwrap();
        assert_eq!(of("whenComplete").params, ["BiConsumer"]);
        assert_eq!(of("whenComplete").param_types, ["BiConsumer<? super T, ? super Throwable>"]);
        assert_eq!(of("allOf").params, ["Fut[]"]);
        assert_eq!(of("allOf").param_types, ["Fut<?>[]"]);
        assert_eq!(of("get").param_types, ["long", "TimeUnit"]);
        assert_eq!(of("get").param_names, ["timeout", "unit"]);
    }

    #[test]
    fn writes_schema_2_compact_one_class_per_line() {
        let mut index = index(FIXTURE);
        index.merge_deprecation_list("org.apache.kafka.clients.producer.Rec#TOPIC_CONFIG\n");
        let public: BTreeSet<String> = ["org.apache.kafka.clients.producer.Rec".to_string()].into();
        let unsupported: BTreeSet<String> = ["clients.producer".to_string()].into();
        let sources = |_: &JavaClass| Some(FIXTURE.to_string());
        let json = to_json(&index, &public, &unsupported, &sources);
        let lines: Vec<&str> = json.lines().map(|l| l.trim_end_matches(',')).collect();
        assert_eq!(
            lines[0],
            r#"{"schema":2,"audience_ref":"4.4.0-rc3","deprecation_refs":["4.3.1","4.4.0-rc3"],"classes":["#
        );
        // One class per line, sorted by nested path; the test class is left out.
        assert_eq!(lines.len(), 5, "{json}");
        assert_eq!(lines[4], "]}");
        let rec = lines[1];
        let has = |line: &str, part: &str| assert!(line.contains(part), "{part}\nnot in\n{line}");
        // A key at its default is left out: kind class, visibility public, false flags; no Javadoc.
        assert!(
            rec.starts_with(concat!(
                r#"{"package":"clients.producer","path":["Rec"],"public_audience":true,"unsupported_package":true,"#,
                r#""supertypes":["Comparable"],"supertype_fqns":[null],"overloads":["#
            )),
            "{rec}"
        );
        assert!(!rec.contains("javadoc"), "{rec}");
        has(
            rec,
            r#"{"name":"send","params":["Duration"],"param_names":["timeout"],"return_type":"Future<RecordMetadata>"}"#,
        );
        has(
            rec,
            concat!(
                r#"{"name":"all","params":["String[]"],"param_names":["names"],"#,
                r#""return_type":"Map<String, ? extends List<T>>[]","static":true,"deprecated":true}"#
            ),
        );
        has(
            rec,
            r#"{"name":"Rec","params":["String"],"param_names":["topic"],"visibility":"package-private"}"#,
        );
        // The listed field is deprecated, and its literal is read from the source.
        has(
            rec,
            r#"{"name":"TOPIC_CONFIG","type":"String","static":true,"final":true,"deprecated":true,"value":"topic"}"#,
        );
        // A private member is only named: no parameter names, types as written or return type.
        has(
            rec,
            r#"{"name":"parts","type":"Map<String, List<Integer>>","visibility":"private","final":true}"#,
        );
        has(rec, r#"{"name":"count","type":"int","visibility":"package-private"}"#);
        assert!(rec.ends_with("]}"), "{rec}");
        // A nested class takes its top-level class's audience, and no literal of the top-level source.
        has(
            lines[2],
            r#"{"package":"clients.producer","path":["Rec","Callback"],"kind":"interface","#,
        );
        has(lines[2], r#""public_audience":true,"#);
        has(lines[3], r#""path":["Rec","Mode"],"kind":"enum","#);
        assert!(lines[3].ends_with(r#""enum_constants":["ON","OFF"]}"#), "{}", lines[3]);
        // Deterministic.
        assert_eq!(json, to_json(&index, &public, &unsupported, &sources));
    }

    #[test]
    fn a_private_overload_is_only_named() {
        let index =
            index("public class P { private Map<String, Integer> pick(final List<String> names) { return null; } }");
        let json = to_json(&index, &BTreeSet::new(), &BTreeSet::new(), &|_| None);
        assert!(
            json.contains(r#""overloads":[{"name":"pick","params":["List"],"visibility":"private"}"#),
            "{json}"
        );
    }

    #[test]
    fn flags_follow_the_lists() {
        let mut index = index("public class A { } public class B { }");
        index.merge_deprecation_list("org.apache.kafka.clients.producer.B\n");
        let json = to_json(&index, &BTreeSet::new(), &BTreeSet::new(), &|_| None);
        assert!(json.contains(r#"{"package":"clients.producer","path":["A"]"#), "{json}");
        assert!(
            json.contains(r#"{"package":"clients.producer","path":["B"],"deprecated":true"#),
            "{json}"
        );
    }

    #[test]
    fn string_literals_are_read_from_the_source() {
        let source = r#"
            public static final String A_CONFIG = "a.b";
            public static final String ESCAPED = "say \"hi\"\né";
            public static final String JOINED = "a" + "b";
            public static final String REF = OTHER;
            public static final int N = 1;
        "#;
        let got = string_literals(source);
        assert_eq!(
            got.into_iter().collect::<Vec<_>>(),
            [
                ("A_CONFIG".to_string(), "a.b".to_string()),
                ("ESCAPED".to_string(), "say \"hi\"\n\u{e9}".to_string())
            ]
        );
    }

    #[test]
    fn fixed_messages_are_the_literals_super_or_this_starts_with() {
        let source = r#"
            public class AbortedError extends RuntimeException {
                public AbortedError(String message) { super(message); }
                public AbortedError() { super("Failing batch " + "since aborted"); }
                protected AbortedError(final Throwable cause) {
                    this("cause: ", cause);
                }
                AbortedError(int x) { super("package-private"); }
            }
        "#;
        assert_eq!(
            fixed_messages(source, "AbortedError"),
            [
                (vec![], "Failing batch since aborted".to_string()),
                (vec!["Throwable".to_string()], "cause: ".to_string()),
            ]
        );
    }

    #[test]
    fn phantom_fields_are_reported() {
        let mut index = index(
            "public class Boom extends RuntimeException { } \
             public interface Api { int SIZE = 1; }",
        );
        let api = index.classes.iter_mut().find(|c| c.path == ["Api"]).unwrap();
        for (name, ty) in [
            ("Boom", "Boom"),
            ("x", "throws"),
            ("y", "Function<A, B> -> C"),
            ("z", ""),
        ] {
            api.fields.insert(name.into(), false);
            api.field_decls
                .insert(name.into(), java::FieldDecl { ty: ty.into(), is_static: true, is_final: true });
        }
        assert_eq!(
            phantom_fields(&index),
            [
                r#"org.apache.kafka.clients.producer.Api#Boom ("Boom")"#,
                r#"org.apache.kafka.clients.producer.Api#x ("throws")"#,
                r#"org.apache.kafka.clients.producer.Api#y ("Function<A, B> -> C")"#,
                r#"org.apache.kafka.clients.producer.Api#z ("")"#,
            ]
        );
    }

    #[test]
    fn escapes_strings() {
        assert_eq!(string("a\"b\\c\nd\re\tf\u{8}\u{c}\u{1}é"), r#""a\"b\\c\nd\re\tf\b\f\u0001é""#);
        assert_eq!(string(""), r#""""#);
    }
}
