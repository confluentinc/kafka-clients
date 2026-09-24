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

//! An index of the Java client's classes and methods, and the rules that turn
//! their names into Rust and C names (CLAUDE.md §2, §4).
//!
//! The scanner is deliberately small: it strips comments and literals, then
//! follows braces to find class declarations and the methods declared directly
//! in each class body. That is all the naming rules need, and it avoids a Java
//! parser dependency.
//!
//! Items translated from Java carry a marker naming their origin:
//!
//! ```text
//! #[doc(alias = "org.apache.kafka.clients.producer.ProducerRecord")]         // a class
//! #[doc(alias = "org.apache.kafka.clients.producer.ProducerRecord$Nested")]  // a nested class
//! #[doc(alias = "org.apache.kafka.clients.producer.KafkaProducer#send")]     // a method
//! ```
//!
//! A method marker names the method, not an overload: every overload becomes a
//! Rust function whose name derives from that one Java name (§2, `_with_..`).

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

/// Where the Java client's main sources live: the `kafka` git submodule.
pub const JAVA_MAIN_ROOT: &str = "kafka/clients/src/main/java/org/apache/kafka";

/// Where the Java client's tests live.
pub const JAVA_TEST_ROOT: &str = "kafka/clients/src/test/java/org/apache/kafka";

/// The prefix every marker starts with.
pub const MARKER_PREFIX: &str = "org.apache.kafka.";

/// A Java class, interface, enum or record, possibly nested.
#[derive(Debug, Clone)]
pub struct JavaClass {
    /// The Rust module path of its package: the Java package after
    /// `org.apache.kafka`, without `clients` (CLAUDE.md §2).
    pub module: Vec<String>,
    /// The dotted Java package, e.g. `clients.producer`.
    pub package: String,
    /// The class names from the top-level one down, e.g. `["ProducerRecord"]`
    /// or `["ConsumerPartitionAssignor", "Assignment"]`.
    pub path: Vec<String>,
    /// The methods declared directly in the class body, constructors included
    /// (under the class's own name).
    pub methods: BTreeSet<String>,
    /// Whether it comes from the test tree.
    pub is_test: bool,
}

impl JavaClass {
    /// The simple name of the (innermost) class.
    pub fn name(&self) -> &str {
        self.path.last().map(String::as_str).unwrap_or_default()
    }

    /// The marker naming this class, e.g.
    /// `org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Assignment`.
    pub fn marker(&self) -> String {
        format!("{MARKER_PREFIX}{}.{}", self.package, self.path.join("$"))
    }

    /// The Rust type name the translation rules give this class (CLAUDE.md §2):
    /// `Exception` becomes `Error`, prefixed by the top-level package for an
    /// error outside `common` (`consumer.OffsetOutOfRangeException` ->
    /// `ConsumerOffsetOutOfRangeError`).
    pub fn rust_name(&self) -> String {
        let name = self.name();
        let Some(base) = name.strip_suffix("Exception") else {
            return name.to_string();
        };
        let prefix = match self.module.first().map(String::as_str) {
            None | Some("common") => String::new(),
            Some(pkg) => upper_first(pkg),
        };
        format!("{prefix}{base}Error")
    }

    /// The C FFI type name (CLAUDE.md §4): `kafka_<package>_<Name>_t`, the Java
    /// package without `clients`. `common.errors` maps to `kafka_common_`, per
    /// CLAUDE.md's `kafka_common_ResourceNotFoundError_t`.
    pub fn ffi_name(&self) -> String {
        format!("{}_t", self.ffi_prefix())
    }

    /// `kafka_<package>_<Name>`, the prefix of the C type and its functions.
    pub fn ffi_prefix(&self) -> String {
        let module = if self.module == ["common", "errors"] {
            &self.module[..1]
        } else {
            &self.module[..]
        };
        format!("kafka_{}_{}", module.join("_"), self.rust_name())
    }

    /// Whether `rust` is a Rust name the translation rules give Java method
    /// `method` of this class (CLAUDE.md §2):
    ///   - camelCase → snake_case, with `Exception` → `Error` and `throw` →
    ///     `return` (`maybeThrowAnyException` → `maybe_return_any_error`);
    ///   - a getter's `get` prefix may be dropped (`getFoo` → `foo`);
    ///   - an overload appends `_with_<params>` (`foo_bar_with_b`);
    ///   - a constructor becomes `new`, or `with_<params>`;
    ///   - a name that is a Rust keyword takes a trailing `_`.
    pub fn is_rust_name_of(&self, method: &str, rust: &str) -> bool {
        if method == self.name() {
            return rust == "new" || rust.starts_with("with_");
        }
        rust_method_bases(method).iter().any(|base| {
            rust == base
                || rust
                    .strip_prefix(base.as_str())
                    .is_some_and(|rest| rest.starts_with("_with_") || rest == "_")
        })
    }
}

/// The Rust names a Java method translates to, before any `_with_..` suffix.
pub fn rust_method_bases(method: &str) -> Vec<String> {
    let mut bases = vec![translate_words(&snake_case(method))];
    if let Some(rest) = method.strip_prefix("get").filter(|r| r.starts_with(char::is_uppercase)) {
        bases.push(translate_words(&snake_case(rest)));
    }
    bases
}

/// Applies CLAUDE.md §2's word substitutions to a snake_case name.
fn translate_words(snake: &str) -> String {
    snake
        .split('_')
        .map(|w| match w {
            "exception" => "error",
            "exceptions" => "errors",
            "throw" => "return",
            "throws" => "returns",
            other => other,
        })
        .collect::<Vec<_>>()
        .join("_")
}

/// camelCase → snake_case, keeping acronyms together (`getAPIVersion` →
/// `get_api_version`).
pub fn snake_case(name: &str) -> String {
    let chars: Vec<char> = name.chars().collect();
    let mut out = String::with_capacity(name.len() + 4);
    for (i, &c) in chars.iter().enumerate() {
        if c.is_uppercase() && i > 0 {
            let prev = chars[i - 1];
            let next_lower = chars.get(i + 1).is_some_and(|n| n.is_lowercase());
            if prev.is_lowercase() || prev.is_ascii_digit() || (prev.is_uppercase() && next_lower) {
                out.push('_');
            }
        }
        out.extend(c.to_lowercase());
    }
    out
}

/// `name` with its acronyms in Rust casing: `SSLContext` → `SslContext`,
/// `OAuthBearer` unchanged.
pub fn rust_casing(name: &str) -> String {
    let chars: Vec<char> = name.chars().collect();
    let mut out = String::with_capacity(name.len());
    for (i, &c) in chars.iter().enumerate() {
        let prev_upper = i > 0 && chars[i - 1].is_uppercase();
        let next_lower = chars.get(i + 1).is_some_and(|n| n.is_lowercase());
        if c.is_uppercase() && prev_upper && !next_lower {
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// Whether Rust name `actual` is `expected` up to acronym casing.
pub fn same_name(actual: &str, expected: &str) -> bool {
    actual == expected || actual == rust_casing(expected)
}

/// `s` lowercased without underscores, to compare a file stem with a type name.
pub fn squash(s: &str) -> String {
    s.chars().filter(|c| *c != '_').flat_map(char::to_lowercase).collect()
}

fn upper_first(s: &str) -> String {
    let mut chars = s.chars();
    chars
        .next()
        .map_or_else(String::new, |f| f.to_uppercase().chain(chars).collect())
}

/// Every class of the Java client, main and test.
pub struct JavaIndex {
    pub classes: Vec<JavaClass>,
}

impl JavaIndex {
    /// Loads both trees; empty when the `kafka` submodule is not checked out.
    pub fn load() -> Self {
        let mut classes = Vec::new();
        collect(Path::new(JAVA_MAIN_ROOT), &mut Vec::new(), false, &mut classes);
        collect(Path::new(JAVA_TEST_ROOT), &mut Vec::new(), true, &mut classes);
        JavaIndex { classes }
    }

    pub fn is_empty(&self) -> bool {
        self.classes.is_empty()
    }

    /// The class a marker names (ignoring any `#method`).
    pub fn resolve(&self, marker: &str) -> Option<&JavaClass> {
        let path = marker.strip_prefix(MARKER_PREFIX)?;
        let class_path = path.split('#').next()?;
        let (package, class) = class_path.rsplit_once('.')?;
        let nested: Vec<&str> = class.split('$').collect();
        self.classes
            .iter()
            .find(|c| c.package == package && c.path.iter().map(String::as_str).eq(nested.iter().copied()))
    }

    /// The `#method` part of a marker, if any.
    pub fn marker_method(marker: &str) -> Option<&str> {
        marker.split_once('#').map(|(_, m)| m)
    }
}

/// Collects the classes of every `*.java` file under `dir`, whose package
/// below `org.apache.kafka` is `package`.
fn collect(dir: &Path, package: &mut Vec<String>, is_test: bool, out: &mut Vec<JavaClass>) {
    let Ok(entries) = fs::read_dir(dir) else { return };
    let mut entries: Vec<_> = entries.filter_map(Result::ok).collect();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let name = entry.file_name().to_string_lossy().into_owned();
        if entry.path().is_dir() {
            package.push(name);
            collect(&entry.path(), package, is_test, out);
            package.pop();
        } else if name.ends_with(".java") && name != "package-info.java" && name != "module-info.java" {
            let Ok(source) = fs::read_to_string(entry.path()) else {
                continue;
            };
            let module = match package.split_first() {
                Some((first, rest)) if first == "clients" => rest.to_vec(),
                _ => package.clone(),
            };
            for (path, methods) in scan(&source) {
                out.push(JavaClass { module: module.clone(), package: package.join("."), path, methods, is_test });
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    Punct(char),
}

/// Tokenizes Java source, dropping comments, string/char/text-block literals
/// and numbers: only identifiers and punctuation remain.
fn tokenize(src: &str) -> Vec<Tok> {
    let b: Vec<char> = src.chars().collect();
    let mut toks = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c == '/' && b.get(i + 1) == Some(&'/') {
            while i < b.len() && b[i] != '\n' {
                i += 1;
            }
        } else if c == '/' && b.get(i + 1) == Some(&'*') {
            i += 2;
            while i + 1 < b.len() && !(b[i] == '*' && b[i + 1] == '/') {
                i += 1;
            }
            i += 2;
        } else if c == '"' && b.get(i + 1) == Some(&'"') && b.get(i + 2) == Some(&'"') {
            i += 3;
            while i + 2 < b.len() && !(b[i] == '"' && b[i + 1] == '"' && b[i + 2] == '"' && b[i - 1] != '\\') {
                i += 1;
            }
            i += 3;
            toks.push(Tok::Ident("\"\"".into()));
        } else if c == '"' || c == '\'' {
            i += 1;
            while i < b.len() && b[i] != c {
                if b[i] == '\\' {
                    i += 1;
                }
                i += 1;
            }
            i += 1;
            toks.push(Tok::Ident("\"\"".into()));
        } else if c.is_alphabetic() || c == '_' || c == '$' {
            let start = i;
            while i < b.len() && (b[i].is_alphanumeric() || b[i] == '_' || b[i] == '$') {
                i += 1;
            }
            toks.push(Tok::Ident(b[start..i].iter().collect()));
        } else if c.is_ascii_digit() {
            while i < b.len() && (b[i].is_alphanumeric() || b[i] == '.' || b[i] == '_') {
                i += 1;
            }
            toks.push(Tok::Ident("0".into()));
        } else {
            if !c.is_whitespace() {
                toks.push(Tok::Punct(c));
            }
            i += 1;
        }
    }
    toks
}

const KEYWORDS: &[&str] = &[
    "new",
    "return",
    "throw",
    "if",
    "for",
    "while",
    "switch",
    "catch",
    "synchronized",
    "else",
    "case",
    "assert",
    "super",
    "this",
    "try",
    "do",
    "yield",
];
const MODIFIERS: &[&str] = &[
    "public",
    "protected",
    "private",
    "static",
    "final",
    "abstract",
    "default",
    "native",
    "strictfp",
    "synchronized",
    "transient",
    "volatile",
    "sealed",
];

/// The classes declared in a Java file, each with the methods declared directly
/// in its body.
fn scan(src: &str) -> Vec<(Vec<String>, BTreeSet<String>)> {
    let toks = tokenize(src);
    let mut out: Vec<(Vec<String>, BTreeSet<String>)> = Vec::new();
    // Open class bodies: (index into `out`, brace depth inside the body).
    let mut stack: Vec<(usize, usize)> = Vec::new();
    let mut depth = 0usize;
    let mut paren = 0usize;
    // A class declaration seen, waiting for its `{`.
    let mut pending: Option<String> = None;
    let mut i = 0;
    while i < toks.len() {
        match &toks[i] {
            Tok::Punct('{') => {
                depth += 1;
                if let Some(name) = pending.take() {
                    let mut path: Vec<String> = stack.last().map(|(idx, _)| out[*idx].0.clone()).unwrap_or_default();
                    path.push(name);
                    out.push((path, BTreeSet::new()));
                    stack.push((out.len() - 1, depth));
                }
            },
            Tok::Punct('}') => {
                if stack.last().is_some_and(|(_, d)| *d == depth) {
                    stack.pop();
                }
                depth = depth.saturating_sub(1);
            },
            Tok::Punct('(') => paren += 1,
            Tok::Punct(')') => paren = paren.saturating_sub(1),
            Tok::Ident(kw) if matches!(kw.as_str(), "class" | "interface" | "enum" | "record") && paren == 0 => {
                // `Foo.class` is a literal, not a declaration; `@interface` is one.
                let is_literal = i > 0 && toks[i - 1] == Tok::Punct('.');
                if let (false, Some(Tok::Ident(name))) = (is_literal, toks.get(i + 1)) {
                    pending = Some(name.clone());
                    i += 2;
                    continue;
                }
            },
            Tok::Ident(name) if paren == 0 && toks.get(i + 1) == Some(&Tok::Punct('(')) => {
                if let Some(&(idx, body)) = stack.last() {
                    if depth == body && pending.is_none() && is_method_decl(&toks, i, &out[idx].0) {
                        out[idx].1.insert(name.clone());
                    }
                }
            },
            _ => {},
        }
        i += 1;
    }
    out
}

/// Whether the identifier at `i`, followed by `(` in a class body, declares a
/// method or constructor rather than calling something in an initializer.
fn is_method_decl(toks: &[Tok], i: usize, class: &[String]) -> bool {
    let Tok::Ident(name) = &toks[i] else { return false };
    if KEYWORDS.contains(&name.as_str()) {
        return false;
    }
    let Some(prev) = i.checked_sub(1).map(|p| &toks[p]) else {
        return false;
    };
    let is_ctor = class.last() == Some(name);
    match prev {
        // `Type name(`, `<T> T name(`, `int[] name(`, `List<X> name(`.
        Tok::Ident(p) if MODIFIERS.contains(&p.as_str()) => is_ctor || !name.starts_with(char::is_uppercase),
        Tok::Ident(p) => !KEYWORDS.contains(&p.as_str()) && p != "\"\"" && p != "0",
        Tok::Punct('>' | ']') => true,
        // A constructor with no modifier.
        Tok::Punct(';' | '{' | '}') => is_ctor,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scans_classes_and_methods() {
        let src = r#"
            package x;
            /** doc with { brace */
            public class Outer<K> implements Foo {
                private final Map<String, Integer> m = new HashMap<>();
                private static final String S = "a { b";
                public Outer(int a) { this.a = compute(a); }
                Outer() {}
                public <T> List<T> items(String s) throws FooException { return x.call(); }
                int[] values() { return null; }
                @Override public String toString() { return Outer.class.getName(); }
                public static class Inner { void run() { Runnable r = () -> go(); } }
                enum Kind { A("a"), B("b") { void over() {} }; Kind(String s) {} String code() { return ""; } }
                interface Api { void call(int a); default boolean ok() { return true; } }
            }
        "#;
        let classes = scan(src);
        let get = |path: &[&str]| {
            classes
                .iter()
                .find(|(p, _)| p.iter().map(String::as_str).eq(path.iter().copied()))
                .map(|(_, m)| m.iter().cloned().collect::<Vec<_>>())
                .unwrap()
        };
        assert_eq!(get(&["Outer"]), ["Outer", "items", "toString", "values"]);
        assert_eq!(get(&["Outer", "Inner"]), ["run"]);
        assert_eq!(get(&["Outer", "Kind"]), ["Kind", "code"]);
        assert_eq!(get(&["Outer", "Api"]), ["call", "ok"]);
    }

    #[test]
    fn maps_method_names() {
        let class = JavaClass {
            module: vec!["producer".into()],
            package: "clients.producer".into(),
            path: vec!["KafkaProducer".into()],
            methods: BTreeSet::new(),
            is_test: false,
        };
        assert!(class.is_rust_name_of("send", "send"));
        assert!(class.is_rust_name_of("send", "send_with_callback"));
        assert!(!class.is_rust_name_of("send", "sender"));
        assert!(class.is_rust_name_of("maybeThrowAnyException", "maybe_return_any_error"));
        assert!(class.is_rust_name_of("getAPIVersion", "get_api_version"));
        assert!(class.is_rust_name_of("getAPIVersion", "api_version"));
        assert!(class.is_rust_name_of("KafkaProducer", "new"));
        assert!(class.is_rust_name_of("KafkaProducer", "with_config"));
        assert!(class.is_rust_name_of("type", "type_"));
        assert!(!class.is_rust_name_of("KafkaProducer", "create"));
    }

    #[test]
    fn maps_class_names() {
        let class = |module: &[&str], package: &str, name: &str| JavaClass {
            module: module.iter().map(|s| s.to_string()).collect(),
            package: package.into(),
            path: vec![name.into()],
            methods: BTreeSet::new(),
            is_test: false,
        };
        let oor = class(&["consumer"], "clients.consumer", "OffsetOutOfRangeException");
        assert_eq!(oor.rust_name(), "ConsumerOffsetOutOfRangeError");
        assert_eq!(oor.ffi_name(), "kafka_consumer_ConsumerOffsetOutOfRangeError_t");
        let rnf = class(&["common", "errors"], "common.errors", "ResourceNotFoundException");
        assert_eq!(rnf.ffi_name(), "kafka_common_ResourceNotFoundError_t");
        let acl = class(&["common", "acl"], "common.acl", "AclBinding");
        assert_eq!(acl.ffi_name(), "kafka_common_acl_AclBinding_t");
        assert!(same_name("SslFactory", "SSLFactory"));
    }
}
