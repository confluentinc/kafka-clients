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
//! Where only some overloads of a name are `@Deprecated`, the marker names the
//! overload by its parameter types, so the deprecation check can tell them
//! apart:
//!
//! ```text
//! #[doc(alias = "org.apache.kafka.clients.admin.MemberDescription#MemberDescription(String,Optional,Optional,String,String,MemberAssignment,Optional,Optional,Optional)")]
//! ```
//!
//! The scanner also records `@Deprecated` on classes, overloads and fields
//! (CLAUDE.md §3: deprecated API is not translated).

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};

/// Where the Java client's main sources live: the `kafka` git submodule.
pub const JAVA_MAIN_ROOT: &str = "kafka/clients/src/main/java/org/apache/kafka";

/// Where the Java client's tests live.
pub const JAVA_TEST_ROOT: &str = "kafka/clients/src/test/java/org/apache/kafka";

/// The Kafka versions whose `@Deprecated`s count (CLAUDE.md §3: deprecated API
/// is not translated): the source reference and the next release.
pub const DEPRECATION_REFS: &[&str] = &["4.3.1", "4.4.0-rc2"];

/// The checked-in list of the deprecated items of [`DEPRECATION_REFS`],
/// written by `cargo xtask java-deprecated`. The lint reads it because CI's
/// shallow `kafka` clone has only the working tree, not the other refs.
pub const DEPRECATED_LIST: &str = "design/current/java-deprecated.txt";

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
    /// Every overload of `methods`, in declaration order.
    pub overloads: Vec<Overload>,
    /// The fields and enum constants declared directly in the class body, each
    /// with whether it is `@Deprecated`.
    pub fields: BTreeMap<String, bool>,
    /// Whether the class, or a class enclosing it, is `@Deprecated`.
    pub deprecated: bool,
    /// Whether it comes from the test tree.
    pub is_test: bool,
}

/// One method or constructor overload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Overload {
    pub name: String,
    /// The simple names of its parameter types, generics dropped:
    /// `close(Duration)` → `["Duration"]`, `f(Map<K, V>... m)` → `["Map[]"]`.
    pub params: Vec<String>,
    /// Whether it is `@Deprecated` (its class's deprecation not included).
    pub deprecated: bool,
}

impl Overload {
    /// `name(T1,T2)`, the form a marker names it by.
    pub fn signature(&self) -> String {
        format!("{}({})", self.name, self.params.join(","))
    }
}

/// Whether a marker's target is deprecated.
#[derive(Debug, PartialEq, Eq)]
pub enum Deprecation {
    /// Not deprecated.
    No,
    /// Deprecated.
    Yes,
    /// A method name some of whose overloads are deprecated and some not: the
    /// marker must name the overload. Holds the non-deprecated signatures.
    Ambiguous(Vec<String>),
    /// The marker names a member the class does not declare.
    Unknown,
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

impl JavaClass {
    /// Whether `member` — a method name, an overload signature `name(T1,T2)`,
    /// or a field — is deprecated, directly or through its class.
    pub fn deprecation(&self, member: Option<&str>) -> Deprecation {
        let Some(member) = member else {
            return if self.deprecated {
                Deprecation::Yes
            } else {
                Deprecation::No
            };
        };
        let (name, signature) = match member.split_once('(') {
            Some((name, _)) => (name, Some(member)),
            None => (member, None),
        };
        let overloads: Vec<&Overload> = self.overloads.iter().filter(|o| o.name == name).collect();
        let found = if let Some(signature) = signature {
            match overloads.iter().find(|o| o.signature() == signature) {
                Some(o) => o.deprecated,
                None => return Deprecation::Unknown,
            }
        } else if !overloads.is_empty() {
            let live: Vec<String> = overloads.iter().filter(|o| !o.deprecated).map(|o| o.signature()).collect();
            if live.is_empty() {
                true
            } else if live.len() == overloads.len() {
                false
            } else {
                return if self.deprecated {
                    Deprecation::Yes
                } else {
                    Deprecation::Ambiguous(live)
                };
            }
        } else if let Some(&field) = self.fields.get(name) {
            field
        } else {
            return Deprecation::Unknown;
        };
        if found || self.deprecated {
            Deprecation::Yes
        } else {
            Deprecation::No
        }
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

    /// The `#method` part of a marker, if any, without an overload's
    /// `(params)`.
    pub fn marker_method(marker: &str) -> Option<&str> {
        Self::marker_member(marker).map(|m| m.split_once('(').map_or(m, |(name, _)| name))
    }

    /// The `#member` part of a marker, if any, with an overload's `(params)`.
    pub fn marker_member(marker: &str) -> Option<&str> {
        marker.split_once('#').map(|(_, m)| m)
    }

    /// Whether the item a marker names is deprecated.
    pub fn deprecation(&self, marker: &str) -> Deprecation {
        match self.resolve(marker) {
            Some(class) => class.deprecation(Self::marker_member(marker)),
            None => Deprecation::Unknown,
        }
    }

    /// Adds the deprecations listed in `list`, in the format of
    /// [`deprecated_items`]: an item is deprecated if the list says so.
    pub fn merge_deprecation_list(&mut self, list: &str) {
        for line in list.lines().map(str::trim).filter(|l| !l.is_empty() && !l.starts_with('#')) {
            let (class_marker, member) = match line.split_once('#') {
                Some((c, m)) => (c, Some(m)),
                None => (line, None),
            };
            let Some(path) = class_marker.strip_prefix(MARKER_PREFIX) else {
                continue;
            };
            let Some((package, class)) = path.rsplit_once('.') else {
                continue;
            };
            let nested: Vec<&str> = class.split('$').collect();
            let nested_prefix = format!("{}$", nested.join("$"));
            for c in self.classes.iter_mut().filter(|c| !c.is_test && c.package == package) {
                let joined = c.path.join("$");
                match member {
                    None if joined == class || joined.starts_with(&nested_prefix) => c.deprecated = true,
                    Some(member) if joined == class => {
                        if let Some(o) = c.overloads.iter_mut().find(|o| o.signature() == member) {
                            o.deprecated = true;
                        } else if let Some(f) = c.fields.get_mut(member) {
                            *f = true;
                        }
                    },
                    _ => {},
                }
            }
        }
    }
}

/// Every deprecated item of `classes`' main classes, one per line, sorted:
///   - a deprecated class, which covers its members and nested classes:
///     `org.apache.kafka.clients.admin.ConsumerGroupListing`;
///   - a deprecated overload of a class that is not: `…Consumer#close(Duration)`;
///   - a deprecated field or enum constant: `…ProducerConfig#SOME_CONFIG`.
pub fn deprecated_items(classes: &[JavaClass]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for class in classes.iter().filter(|c| !c.is_test) {
        let marker = class.marker();
        if class.deprecated {
            // A nested class of a deprecated class is covered by it.
            let outer_deprecated = classes.iter().any(|o| {
                !o.is_test
                    && o.deprecated
                    && o.package == class.package
                    && o.path.len() < class.path.len()
                    && class.path.starts_with(&o.path)
            });
            if !outer_deprecated {
                out.insert(marker);
            }
            continue;
        }
        for o in class.overloads.iter().filter(|o| o.deprecated) {
            out.insert(format!("{marker}#{}", o.signature()));
        }
        for (name, _) in class.fields.iter().filter(|(_, d)| **d) {
            out.insert(format!("{marker}#{name}"));
        }
    }
    out
}

/// The main classes of the Java client at git ref `reference` of the `kafka`
/// submodule, read with `git` so the working tree need not be checked out
/// there. `None` if the ref is not available (e.g. a shallow clone).
pub fn load_ref(reference: &str) -> Option<Vec<JavaClass>> {
    let files = ref_files(reference, |path| {
        path.ends_with(".java") && !path.ends_with("package-info.java") && !path.ends_with("module-info.java")
    })?;
    let mut classes = Vec::new();
    for (rel, body) in &files {
        let mut package: Vec<String> = rel.split('/').map(str::to_string).collect();
        package.pop();
        push_classes(&package, &String::from_utf8_lossy(body), false, &mut classes);
    }
    Some(classes)
}

/// The Kafka version whose `@InterfaceAudience.Public` annotations and
/// unsupported-API disclaimers decide what the crate may make public.
pub const AUDIENCE_REF: &str = "4.4.0-rc2";

/// The sentence a `package-info.java` carries when its package is not part of
/// the supported API ("This package is not a supported Kafka API; the
/// implementation may change without warning ...").
const UNSUPPORTED_API_DISCLAIMER: &str = "is not a supported Kafka API";

/// The dotted package (below `org.apache.kafka`) of `package-info.java` file
/// `rel` (a path below [`JAVA_MAIN_ROOT`]), if its `source` carries the
/// unsupported-API disclaimer.
pub fn unsupported_package(rel: &str, source: &str) -> Option<String> {
    let dir = rel.strip_suffix("package-info.java")?.trim_end_matches('/');
    (!dir.is_empty() && source.contains(UNSUPPORTED_API_DISCLAIMER)).then(|| dir.replace('/', "."))
}

/// The packages carrying the unsupported-API disclaimer at git ref
/// `reference` of the `kafka` submodule; `None` if the ref is not available.
pub fn unsupported_packages_at_ref(reference: &str) -> Option<BTreeSet<String>> {
    let files = ref_files(reference, |path| path.ends_with("/package-info.java"))?;
    Some(
        files
            .iter()
            .filter_map(|(rel, body)| unsupported_package(rel, &String::from_utf8_lossy(body)))
            .collect(),
    )
}

/// The packages carrying the unsupported-API disclaimer in the `kafka` working
/// tree; empty when it is not checked out.
pub fn unsupported_packages_in_tree() -> BTreeSet<String> {
    fn walk(dir: &Path, rel: &mut Vec<String>, out: &mut BTreeSet<String>) {
        let Ok(entries) = fs::read_dir(dir) else { return };
        for entry in entries.filter_map(Result::ok) {
            let name = entry.file_name().to_string_lossy().into_owned();
            if entry.path().is_dir() {
                rel.push(name);
                walk(&entry.path(), rel, out);
                rel.pop();
            } else if name == "package-info.java" {
                let source = fs::read_to_string(entry.path()).unwrap_or_default();
                let path = [rel.as_slice(), &[name]].concat().join("/");
                out.extend(unsupported_package(&path, &source));
            }
        }
    }
    let mut out = BTreeSet::new();
    walk(Path::new(JAVA_MAIN_ROOT), &mut Vec::new(), &mut out);
    out
}

/// The files below [`JAVA_MAIN_ROOT`] at git ref `reference` whose path
/// satisfies `keep`, as (path below the root, contents). `None` if the ref is
/// not available (e.g. a shallow clone).
fn ref_files(reference: &str, keep: impl Fn(&str) -> bool) -> Option<Vec<(String, Vec<u8>)>> {
    let root = JAVA_MAIN_ROOT.strip_prefix("kafka/")?;
    let listing = Command::new("git")
        .args(["-C", "kafka", "ls-tree", "-r", reference, "--", root])
        .output()
        .ok()
        .filter(|o| o.status.success())?;
    let mut files = Vec::new();
    for line in String::from_utf8_lossy(&listing.stdout).lines() {
        let Some((meta, path)) = line.split_once('\t') else {
            continue;
        };
        let Some(sha) = meta.split_whitespace().nth(2) else {
            continue;
        };
        if keep(path) {
            files.push((sha.to_string(), path.to_string()));
        }
    }
    let mut child = Command::new("git")
        .args(["-C", "kafka", "cat-file", "--batch"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .ok()?;
    let mut stdin = child.stdin.take()?;
    let shas: String = files.iter().map(|(sha, _)| format!("{sha}\n")).collect();
    let writer = std::thread::spawn(move || stdin.write_all(shas.as_bytes()));
    let mut out = Vec::new();
    child.stdout.take()?.read_to_end(&mut out).ok()?;
    writer.join().ok()?.ok()?;
    child.wait().ok()?;
    let mut bodies = Vec::with_capacity(files.len());
    let mut rest = out.as_slice();
    for (_, path) in &files {
        let header_end = rest.iter().position(|&b| b == b'\n')?;
        let size: usize = std::str::from_utf8(&rest[..header_end])
            .ok()?
            .rsplit(' ')
            .next()?
            .parse()
            .ok()?;
        let body = rest[header_end + 1..header_end + 1 + size].to_vec();
        rest = &rest[header_end + 1 + size + 1..];
        let rel = path.strip_prefix(root)?.trim_start_matches('/');
        bodies.push((rel.to_string(), body));
    }
    Some(bodies)
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
            push_classes(package, &source, is_test, out);
        }
    }
}

/// Pushes the classes of one Java file of package `package` (below
/// `org.apache.kafka`).
fn push_classes(package: &[String], source: &str, is_test: bool, out: &mut Vec<JavaClass>) {
    let module = match package.split_first() {
        Some((first, rest)) if first == "clients" => rest.to_vec(),
        _ => package.to_vec(),
    };
    for class in scan(source) {
        out.push(JavaClass {
            module: module.clone(),
            package: package.join("."),
            path: class.path,
            methods: class.overloads.iter().map(|o| o.name.clone()).collect(),
            overloads: class.overloads,
            fields: class.fields,
            deprecated: class.deprecated,
            is_test,
        });
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

/// A class found by [`scan`].
#[derive(Debug)]
struct Scanned {
    path: Vec<String>,
    overloads: Vec<Overload>,
    fields: BTreeMap<String, bool>,
    deprecated: bool,
}

/// An open class body while scanning.
struct Open {
    /// Its index in the output.
    idx: usize,
    /// The brace depth inside its body.
    body: usize,
    /// Whether it is an enum still in its constant list (before the first `;`).
    enum_constants: bool,
}

/// The classes declared in a Java file, each with the methods, constructors
/// and fields declared directly in its body, and their `@Deprecated`s.
///
/// An `@Deprecated` annotation at class-body level (or before a top-level
/// class) applies to the next declaration — a class, an overload or a field;
/// any other statement ending first clears it. Other annotations are skipped,
/// so `@Deprecated @Override void close(Duration)` still counts.
fn scan(src: &str) -> Vec<Scanned> {
    let toks = tokenize(src);
    let mut out: Vec<Scanned> = Vec::new();
    let mut stack: Vec<Open> = Vec::new();
    let mut depth = 0usize;
    let mut paren = 0usize;
    // A class declaration seen, waiting for its `{`: (name, deprecated, is enum).
    let mut pending: Option<(String, bool, bool)> = None;
    // An `@Deprecated` seen, waiting for the declaration it annotates.
    let mut deprecated = false;
    let mut i = 0;
    while i < toks.len() {
        let member_level = paren == 0 && stack.last().map_or(depth == 0, |o| depth == o.body);
        match &toks[i] {
            Tok::Punct('@') if paren == 0 && toks.get(i + 1) != Some(&Tok::Ident("interface".into())) => {
                let (name, next) = annotation(&toks, i);
                if member_level && name == "Deprecated" {
                    deprecated = true;
                }
                i = next;
                continue;
            },
            Tok::Punct('{') => {
                if let Some((name, dep, is_enum)) = pending.take() {
                    let parent = stack.last().map(|o| &out[o.idx]);
                    let mut path: Vec<String> = parent.map(|p| p.path.clone()).unwrap_or_default();
                    let dep = dep || parent.is_some_and(|p| p.deprecated);
                    path.push(name);
                    out.push(Scanned { path, overloads: Vec::new(), fields: BTreeMap::new(), deprecated: dep });
                    depth += 1;
                    stack.push(Open { idx: out.len() - 1, body: depth, enum_constants: is_enum });
                    i += 1;
                    continue;
                }
                if member_level {
                    deprecated = false;
                }
                depth += 1;
            },
            Tok::Punct('}') => {
                if stack.last().is_some_and(|o| o.body == depth) {
                    stack.pop();
                }
                depth = depth.saturating_sub(1);
                deprecated = false;
            },
            Tok::Punct(';') if member_level => {
                deprecated = false;
                if let Some(o) = stack.last_mut() {
                    o.enum_constants = false;
                }
            },
            Tok::Punct('(') => paren += 1,
            Tok::Punct(')') => paren = paren.saturating_sub(1),
            Tok::Ident(kw) if matches!(kw.as_str(), "class" | "interface" | "enum" | "record") && paren == 0 => {
                // `Foo.class` is a literal, not a declaration; `@interface` is one.
                let is_literal = i > 0 && toks[i - 1] == Tok::Punct('.');
                if let (false, Some(Tok::Ident(name))) = (is_literal, toks.get(i + 1)) {
                    pending = Some((name.clone(), std::mem::take(&mut deprecated), kw == "enum"));
                    i += 2;
                    continue;
                }
            },
            Tok::Ident(name) if member_level && pending.is_none() => {
                let Some(open) = stack.last() else {
                    i += 1;
                    continue;
                };
                let next = toks.get(i + 1);
                let is_constant = open.enum_constants
                    && matches!(next, Some(Tok::Punct(',' | ';' | '(' | '{' | '}')))
                    && !matches!(toks.get(i - 1), Some(Tok::Punct('.')));
                let idx = open.idx;
                if is_constant {
                    out[idx].fields.insert(name.clone(), std::mem::take(&mut deprecated));
                } else if next == Some(&Tok::Punct('(')) {
                    if is_method_decl(&toks, i, &out[idx].path) {
                        let params = params(&toks, i + 1);
                        out[idx].overloads.push(Overload {
                            name: name.clone(),
                            params,
                            deprecated: std::mem::take(&mut deprecated),
                        });
                    }
                } else if matches!(next, Some(Tok::Punct('=' | ';')))
                    && name != "\"\""
                    && name != "0"
                    && !KEYWORDS.contains(&name.as_str())
                {
                    out[idx].fields.insert(name.clone(), std::mem::take(&mut deprecated));
                }
            },
            _ => {},
        }
        i += 1;
    }
    out
}

/// The annotation starting at `@` index `at`: its simple name, and the index
/// just past it (and past its `(..)` arguments).
fn annotation(toks: &[Tok], at: usize) -> (String, usize) {
    let mut j = at + 1;
    let mut name = String::new();
    while let Some(Tok::Ident(part)) = toks.get(j) {
        name = part.clone();
        j += 1;
        if toks.get(j) == Some(&Tok::Punct('.')) && matches!(toks.get(j + 1), Some(Tok::Ident(_))) {
            j += 1;
        } else {
            break;
        }
    }
    if toks.get(j) == Some(&Tok::Punct('(')) {
        j = past_parens(toks, j);
    }
    (name, j)
}

/// The index just past the `)` matching the `(` at `open`.
fn past_parens(toks: &[Tok], open: usize) -> usize {
    let mut depth = 0usize;
    for (j, t) in toks.iter().enumerate().skip(open) {
        match t {
            Tok::Punct('(') => depth += 1,
            Tok::Punct(')') => {
                depth -= 1;
                if depth == 0 {
                    return j + 1;
                }
            },
            _ => {},
        }
    }
    toks.len()
}

/// The parameter types of the declaration whose `(` is at `open`, as simple
/// names: annotations, `final`, generic arguments and parameter names are
/// dropped, and arrays and varargs become `[]`.
fn params(toks: &[Tok], open: usize) -> Vec<String> {
    let close = past_parens(toks, open) - 1;
    let mut out = Vec::new();
    let mut current: Vec<Tok> = Vec::new();
    let mut angle = 0usize;
    let mut j = open + 1;
    while j < close {
        match &toks[j] {
            Tok::Punct('@') => {
                j = annotation(toks, j).1;
                continue;
            },
            Tok::Punct('<') => angle += 1,
            Tok::Punct('>') => angle = angle.saturating_sub(1),
            Tok::Punct(',') if angle == 0 => out.push(param_type(&std::mem::take(&mut current))),
            Tok::Ident(f) if f == "final" => {},
            t if angle == 0 => current.push(t.clone()),
            _ => {},
        }
        j += 1;
    }
    if !current.is_empty() {
        out.push(param_type(&current));
    }
    out
}

/// The simple type of one parameter's tokens (`java.util.Map m` → `Map`,
/// `byte[] b` / `String... s` → `byte[]` / `String[]`).
fn param_type(toks: &[Tok]) -> String {
    let name = toks.iter().rposition(|t| matches!(t, Tok::Ident(_))).unwrap_or(toks.len());
    let ty = &toks[..name];
    let simple = ty
        .iter()
        .rev()
        .find_map(|t| match t {
            Tok::Ident(s) => Some(s.as_str()),
            Tok::Punct(_) => None,
        })
        .unwrap_or_default();
    let arrays = ty.iter().filter(|t| **t == Tok::Punct('[')).count()
        + ty.windows(3).filter(|w| w.iter().all(|t| *t == Tok::Punct('.'))).count();
    format!("{simple}{}", "[]".repeat(arrays))
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
                .find(|c| c.path.iter().map(String::as_str).eq(path.iter().copied()))
                .map(|c| {
                    c.overloads
                        .iter()
                        .map(|o| o.name.clone())
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect::<Vec<_>>()
                })
                .unwrap()
        };
        assert_eq!(get(&["Outer"]), ["Outer", "items", "toString", "values"]);
        assert_eq!(get(&["Outer", "Inner"]), ["run"]);
        assert_eq!(get(&["Outer", "Kind"]), ["Kind", "code"]);
        assert_eq!(get(&["Outer", "Api"]), ["call", "ok"]);
        let outer = classes.iter().find(|c| c.path == ["Outer"]).unwrap();
        assert_eq!(outer.fields.keys().collect::<Vec<_>>(), ["S", "m"]);
        let kind = classes.iter().find(|c| c.path == ["Outer", "Kind"]).unwrap();
        assert_eq!(kind.fields.keys().collect::<Vec<_>>(), ["A", "B"]);
    }

    fn class_of(src: &str, path: &[&str]) -> JavaClass {
        let mut out = Vec::new();
        push_classes(&["clients".into(), "x".into()], src, false, &mut out);
        out.into_iter()
            .find(|c| c.path.iter().map(String::as_str).eq(path.iter().copied()))
            .unwrap()
    }

    #[test]
    fn scans_deprecations() {
        let src = r#"
            package x;
            public class Api {
                /** @deprecated Since 4.0, not an annotation: ignored. */
                public void docOnly() {}
                @Deprecated
                public static final String OLD_CONFIG = "old";
                public static final String NEW_CONFIG = "new";
                @Deprecated(since = "4.2", forRemoval = true)
                public Api(String groupId) {}
                public Api(String groupId, int generation) {}
                @Deprecated @Override public void close(java.time.Duration timeout) {}
                public void close(final CloseOptions options) {}
                @SuppressWarnings("unchecked") public void keep(@Nullable Map<String, List<Integer>> m, byte[] b, String... s) {}
                @Deprecated
                ;
                public void notLeaked() {}
                @java.lang.Deprecated
                public static class Old { void inner() {} public static class Deeper {} }
                enum Kind { A, @Deprecated B, C("c"); Kind() {} Kind(String s) {} }
            }
            @Deprecated
            class Gone { int x; }
        "#;
        let api = class_of(src, &["Api"]);
        assert!(!api.deprecated);
        let sigs: Vec<(String, bool)> = api.overloads.iter().map(|o| (o.signature(), o.deprecated)).collect();
        assert_eq!(
            sigs,
            [
                ("docOnly()".to_string(), false),
                ("Api(String)".to_string(), true),
                ("Api(String,int)".to_string(), false),
                ("close(Duration)".to_string(), true),
                ("close(CloseOptions)".to_string(), false),
                ("keep(Map,byte[],String[])".to_string(), false),
                ("notLeaked()".to_string(), false),
            ]
        );
        assert_eq!(api.fields.get("OLD_CONFIG"), Some(&true));
        assert_eq!(api.fields.get("NEW_CONFIG"), Some(&false));
        assert_eq!(api.deprecation(None), Deprecation::No);
        assert_eq!(api.deprecation(Some("OLD_CONFIG")), Deprecation::Yes);
        assert_eq!(api.deprecation(Some("NEW_CONFIG")), Deprecation::No);
        assert_eq!(api.deprecation(Some("docOnly")), Deprecation::No);
        assert_eq!(
            api.deprecation(Some("close")),
            Deprecation::Ambiguous(vec!["close(CloseOptions)".into()])
        );
        assert_eq!(api.deprecation(Some("close(Duration)")), Deprecation::Yes);
        assert_eq!(api.deprecation(Some("close(CloseOptions)")), Deprecation::No);
        assert_eq!(api.deprecation(Some("close(String)")), Deprecation::Unknown);
        assert_eq!(
            api.deprecation(Some("Api")),
            Deprecation::Ambiguous(vec!["Api(String,int)".into()])
        );
        assert_eq!(api.deprecation(Some("missing")), Deprecation::Unknown);

        let old = class_of(src, &["Api", "Old"]);
        assert!(old.deprecated);
        assert_eq!(old.deprecation(Some("inner")), Deprecation::Yes);
        assert!(class_of(src, &["Api", "Old", "Deeper"]).deprecated);
        let kind = class_of(src, &["Api", "Kind"]);
        assert!(!kind.deprecated);
        assert_eq!(
            kind.fields.iter().map(|(k, v)| (k.as_str(), *v)).collect::<Vec<_>>(),
            [("A", false), ("B", true), ("C", false)]
        );
        assert!(class_of(src, &["Gone"]).deprecated);
    }

    #[test]
    fn lists_and_merges_deprecations() {
        let src = r#"
            package x;
            public class Api {
                @Deprecated public static final String OLD = "o";
                public static final String LATER = "l";
                @Deprecated public void close(Duration d) {}
                public void close(CloseOptions o) {}
                @Deprecated public static class Old { public static class Deeper {} }
            }
        "#;
        let mut classes = Vec::new();
        push_classes(&["clients".into(), "x".into()], src, false, &mut classes);
        let listed: Vec<String> = deprecated_items(&classes).into_iter().collect();
        assert_eq!(
            listed,
            [
                "org.apache.kafka.clients.x.Api#OLD",
                "org.apache.kafka.clients.x.Api#close(Duration)",
                "org.apache.kafka.clients.x.Api$Old",
            ]
        );

        let mut index = JavaIndex { classes };
        assert_eq!(index.deprecation("org.apache.kafka.clients.x.Api#LATER"), Deprecation::No);
        index.merge_deprecation_list("# header\norg.apache.kafka.clients.x.Api#LATER\n");
        assert_eq!(index.deprecation("org.apache.kafka.clients.x.Api#LATER"), Deprecation::Yes);
        assert_eq!(index.deprecation("org.apache.kafka.clients.x.Api$Old$Deeper"), Deprecation::Yes);
        assert_eq!(JavaIndex::marker_method("a.B#close(Duration)"), Some("close"));
    }

    #[test]
    fn maps_method_names() {
        let class = JavaClass {
            module: vec!["producer".into()],
            package: "clients.producer".into(),
            path: vec!["KafkaProducer".into()],
            methods: BTreeSet::new(),
            overloads: Vec::new(),
            fields: BTreeMap::new(),
            deprecated: false,
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
            overloads: Vec::new(),
            fields: BTreeMap::new(),
            deprecated: false,
            is_test: false,
        };
        let oor = class(&["consumer"], "clients.consumer", "OffsetOutOfRangeException");
        assert_eq!(oor.rust_name(), "ConsumerOffsetOutOfRangeError");
        let rnf = class(&["common", "errors"], "common.errors", "ResourceNotFoundException");
        assert_eq!(rnf.rust_name(), "ResourceNotFoundError");
        let acl = class(&["common", "acl"], "common.acl", "AclBinding");
        assert_eq!(acl.rust_name(), "AclBinding");
        assert!(same_name("SslFactory", "SSLFactory"));
    }

    #[test]
    fn test_unsupported_package_reads_the_disclaimer() {
        let disclaimer = "/**\n * Provides the network API.\n * <strong>This package is not a supported Kafka API; \
                          the implementation may change without warning between minor or patch releases.</strong>\n */\n\
                          package org.apache.kafka.common.network;\n";
        assert_eq!(
            unsupported_package("common/network/package-info.java", disclaimer).as_deref(),
            Some("common.network")
        );
        assert_eq!(
            unsupported_package("common/record/internal/package-info.java", disclaimer).as_deref(),
            Some("common.record.internal")
        );
        // A supported package, and a file that is no package-info.
        let supported =
            "/**\n * Provides the API used by Kafka clients.\n */\npackage org.apache.kafka.clients.producer;\n";
        assert_eq!(unsupported_package("clients/producer/package-info.java", supported), None);
        assert_eq!(unsupported_package("common/network/Selector.java", disclaimer), None);
    }
}
