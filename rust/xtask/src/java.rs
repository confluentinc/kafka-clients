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

/// The `kafka` git submodule, at the repository root. xtask runs from the Rust
/// workspace (`rust/`), so the submodule is one level up.
pub const KAFKA_SUBMODULE: &str = "../kafka";

/// Where the Java client's main sources live: the `kafka` git submodule.
pub const JAVA_MAIN_ROOT: &str = "../kafka/clients/src/main/java/org/apache/kafka";

/// Where the Java client's tests live.
pub const JAVA_TEST_ROOT: &str = "../kafka/clients/src/test/java/org/apache/kafka";

/// The Kafka versions whose `@Deprecated`s count (CLAUDE.md §3: deprecated API
/// is not translated): the source reference and the next release.
pub const DEPRECATION_REFS: &[&str] = &["4.3.1", "4.4.0-rc3"];

/// The checked-in list of the deprecated items of [`DEPRECATION_REFS`],
/// written by `cargo xtask java-deprecated`. The lint reads it because CI's
/// shallow `kafka` clone has only the working tree, not the other refs.
pub const DEPRECATED_LIST: &str = "../design/current/java-deprecated.txt";

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
    /// The access level of each of `fields`: an enum constant, or a field of
    /// an interface, is `public`.
    pub field_visibility: BTreeMap<String, Visibility>,
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
    /// Its access modifier, or the implicit one: `public` in an interface,
    /// `private` for an enum constructor, package-private otherwise.
    pub visibility: Visibility,
    /// Whether it is `static`.
    pub is_static: bool,
    /// The names of its parameters, in order.
    pub param_names: Vec<String>,
    /// Whether it returns its own class: a static one is a factory.
    pub returns_class: bool,
}

/// A Java access level, narrowest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Visibility {
    Private,
    /// No modifier, outside an interface.
    Package,
    Protected,
    Public,
}

impl Visibility {
    /// The Java keyword, `package-private` for none.
    pub fn keyword(self) -> &'static str {
        match self {
            Visibility::Private => "private",
            Visibility::Package => "package-private",
            Visibility::Protected => "protected",
            Visibility::Public => "public",
        }
    }
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
    /// the words `Exception` and `Throw` become `Error` and `Return` wherever
    /// they appear (`DeserializationExceptionOrigin` ->
    /// `DeserializationErrorOrigin`), and an error outside `common` is prefixed
    /// by its top-level package (`consumer.OffsetOutOfRangeException` ->
    /// `ConsumerOffsetOutOfRangeError`).
    pub fn rust_name(&self) -> String {
        let name = self.name();
        let translated = translate_camel_words(name);
        if !name.ends_with("Exception") {
            return translated;
        }
        let prefix = match self.module.first().map(String::as_str) {
            None | Some("common") => String::new(),
            Some(pkg) => upper_first(pkg),
        };
        format!("{prefix}{translated}")
    }

    /// Whether `rust` is a Rust name the translation rules give Java method
    /// `method` of this class (CLAUDE.md §2):
    ///   - camelCase → snake_case, with `Exception` → `Error` and `throw` →
    ///     `return` (`maybeThrowAnyException` → `maybe_return_any_error`);
    ///   - a getter's `get` prefix may be dropped (`getFoo` → `foo`);
    ///   - an overload appends `_with_<params>` (`foo_bar_with_b`);
    ///   - a constructor becomes `new`, or `with_<params>`;
    ///   - a setter takes `set_`: an overload with parameters of a method
    ///     sharing its name with a getter (`timeoutMs(Integer)` →
    ///     `set_timeout_ms`), or any setter (`validateOnly(boolean)` →
    ///     `set_validate_only`);
    ///   - a name that is a Rust keyword takes a trailing `_`;
    ///   - a static method sharing its name with an instance one, translated to
    ///     an associated fn without `self` (`has_self` false), follows
    ///     [`Self::is_rust_static_name_of`].
    pub fn is_rust_name_of(&self, method: &str, rust: &str, has_self: bool) -> bool {
        let has = |is_static| self.overloads.iter().any(|o| o.name == method && o.is_static == is_static);
        if !has_self && has(true) && has(false) {
            return self.is_rust_static_name_of(method, rust);
        }
        if method == self.name() {
            return rust == "new" || rust.starts_with("with_");
        }
        let rust = rust.strip_prefix("r#").unwrap_or(rust);
        let is_setter = self.overloads.iter().any(|o| o.name == method && !o.params.is_empty());
        let named_set = method.strip_prefix("set").is_some_and(|r| r.starts_with(char::is_uppercase));
        let rust = match rust.strip_prefix("set_") {
            Some(rest) if is_setter && !named_set => rest,
            _ => rust,
        };
        self.rust_bases(method).iter().any(|base| is_derived_name(rust, base))
    }

    /// Whether `rust` names static `method`, which shares its name with an
    /// instance method that keeps the plain name (CLAUDE.md §2):
    ///   - a factory, returning its class, is `with_<params>`, or
    ///     `new_with_<params>` when an instance method is already
    ///     `with_<params>`: `CloseOptions.timeout(Duration timeout)` beside the
    ///     getter `timeout()` and `withTimeout(Duration)` is `new_with_timeout`;
    ///   - any other static method is `do_<method>`, plus `_with_<params>` for an
    ///     overload.
    pub fn is_rust_static_name_of(&self, method: &str, rust: &str) -> bool {
        let rust = rust.strip_prefix("r#").unwrap_or(rust);
        self.static_rust_names(method)
            .iter()
            .any(|name| match name.strip_prefix("do_") {
                Some(_) => is_derived_name(rust, name),
                None => rust == name,
            })
    }

    /// The Rust names [`Self::is_rust_static_name_of`] accepts for static
    /// `method`, before any `_with_<params>` suffix of a `do_<method>`.
    pub fn static_rust_names(&self, method: &str) -> Vec<String> {
        let rust_of = |name: &str| translate_words(&snake_case(name));
        let mut names = Vec::new();
        for o in self.overloads.iter().filter(|o| o.name == method && o.is_static) {
            let name = if !o.returns_class {
                format!("do_{}", rust_of(method))
            } else if o.param_names.is_empty() {
                "new".to_string()
            } else {
                let params = o.param_names.iter().map(|n| rust_of(n)).collect::<Vec<_>>().join("_");
                let with = format!("with_{params}");
                let taken = self.overloads.iter().any(|i| !i.is_static && rust_of(&i.name) == with);
                if taken {
                    format!("new_{with}")
                } else {
                    with
                }
            };
            if !names.contains(&name) {
                names.push(name);
            }
        }
        names
    }

    /// The Rust base names of this class's method `method`: its snake_case name,
    /// and for a getter `getX` also `x` (CLAUDE.md §2), unless the class declares
    /// an `x` of its own, which owns that name.
    pub fn rust_bases(&self, method: &str) -> Vec<String> {
        let owned = |base: &str| {
            self.methods
                .iter()
                .any(|m| m != method && translate_words(&snake_case(m)) == base)
        };
        let mut bases = rust_method_bases(method);
        bases.truncate(if bases.len() > 1 && owned(&bases[1]) {
            1
        } else {
            bases.len()
        });
        bases
    }

    /// Whether `rust` is a Rust name the translation rules give public Java
    /// field `field` of this class (CLAUDE.md §3: no public fields, so a getter
    /// `field` and a setter `set_field`).
    pub fn is_rust_accessor_of(&self, field: &str, rust: &str) -> bool {
        let base = translate_words(&snake_case(field));
        let rust = rust.strip_prefix("r#").unwrap_or(rust);
        let rust = rust.strip_prefix("set_").unwrap_or(rust);
        is_derived_name(rust, &base)
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

    /// The visibility of method `member` — a name or an overload signature
    /// `name(T1,T2)`. For a name, the widest of its overloads when they are
    /// all public or all not; when only some are public the name does not say
    /// which one the Rust item translates, and the result is
    /// [`MemberVisibility::Ambiguous`]. `None` when the class declares no such
    /// method (a field, or an unknown member).
    pub fn visibility(&self, member: &str) -> Option<MemberVisibility> {
        let name = member.split_once('(').map_or(member, |(name, _)| name);
        let is_signature = name.len() < member.len();
        let overloads: Vec<&Overload> = self
            .overloads
            .iter()
            .filter(|o| {
                if is_signature {
                    o.signature() == member
                } else {
                    o.name == name
                }
            })
            .collect();
        let Some(widest) = overloads.iter().map(|o| o.visibility).max() else {
            // A field a getter or setter translates (CLAUDE.md §3).
            return (!is_signature)
                .then(|| self.field_visibility.get(name))
                .flatten()
                .map(|&v| MemberVisibility::Known(v));
        };
        let public: Vec<String> = overloads
            .iter()
            .filter(|o| o.visibility == Visibility::Public)
            .map(|o| o.signature())
            .collect();
        if public.is_empty() || public.len() == overloads.len() {
            Some(MemberVisibility::Known(widest))
        } else {
            Some(MemberVisibility::Ambiguous(public))
        }
    }
}

/// The visibility of a marker's method.
#[derive(Debug, PartialEq, Eq)]
pub enum MemberVisibility {
    /// The method's visibility (for a name, the widest of its overloads).
    Known(Visibility),
    /// A method name some of whose overloads are public and some not: the
    /// marker must name the overload. Holds the public signatures.
    Ambiguous(Vec<String>),
}

/// Whether `rust` is `base`, an overload of it (`base_with_..`) or its
/// keyword-escaped form (`type_`).
fn is_derived_name(rust: &str, base: &str) -> bool {
    rust == base
        || rust
            .strip_prefix(base)
            .is_some_and(|rest| rest.starts_with("_with_") || rest == "_")
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

/// Applies CLAUDE.md §2's word substitutions to a PascalCase name, keeping
/// every other word as written (`SSLException` → `SSLError`).
fn translate_camel_words(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut start = 0;
    for (i, c) in name.char_indices().skip(1).chain([(name.len(), 'A')]) {
        if c.is_uppercase() {
            out.push_str(match &name[start..i] {
                "Exception" => "Error",
                "Exceptions" => "Errors",
                "Throw" => "Return",
                "Throws" => "Returns",
                other => other,
            });
            start = i;
        }
    }
    out
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
#[derive(Clone)]
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
pub const AUDIENCE_REF: &str = "4.4.0-rc3";

/// Every ref the lint reads: [`DEPRECATION_REFS`] and [`AUDIENCE_REF`],
/// without duplicates.
pub fn lint_refs() -> Vec<&'static str> {
    let mut refs = DEPRECATION_REFS.to_vec();
    if !refs.contains(&AUDIENCE_REF) {
        refs.push(AUDIENCE_REF);
    }
    refs
}

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

/// The files below [`JAVA_MAIN_ROOT`] at git ref `reference` whose path
/// satisfies `keep`, as (path below the root, contents). `None` if the ref is
/// not available (e.g. a shallow clone).
fn ref_files(reference: &str, keep: impl Fn(&str) -> bool) -> Option<Vec<(String, Vec<u8>)>> {
    let root = JAVA_MAIN_ROOT.strip_prefix(KAFKA_SUBMODULE)?.strip_prefix('/')?;
    let listing = Command::new("git")
        .args(["-C", KAFKA_SUBMODULE, "ls-tree", "-r", reference, "--", root])
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
        .args(["-C", KAFKA_SUBMODULE, "cat-file", "--batch"])
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
    for mut class in scan(source) {
        class.add_implicit_members();
        out.push(JavaClass {
            module: module.clone(),
            package: package.join("."),
            path: class.path,
            methods: class.overloads.iter().map(|o| o.name.clone()).collect(),
            overloads: class.overloads,
            fields: class.fields,
            field_visibility: class.field_visibility,
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
    /// Whether it is an interface (or an `@interface`), whose members are
    /// implicitly `public`.
    is_interface: bool,
    /// Whether it is an enum.
    is_enum: bool,
    /// Its access modifier, or the implicit one (`public` inside an interface).
    visibility: Visibility,
    overloads: Vec<Overload>,
    fields: BTreeMap<String, bool>,
    field_visibility: BTreeMap<String, Visibility>,
    deprecated: bool,
}

impl Scanned {
    /// The members Java declares implicitly: an enum's `values()`,
    /// `valueOf(String)`, `name()` and `ordinal()`, and the default
    /// constructor of a class declaring none, as accessible as its class.
    fn add_implicit_members(&mut self) {
        let class = self.path.last().cloned().unwrap_or_default();
        let mut implicit = Vec::new();
        if self.is_enum {
            for (name, params) in [
                ("values", &[][..]),
                ("valueOf", &["String"][..]),
                ("name", &[]),
                ("ordinal", &[]),
            ] {
                implicit.push((name.to_string(), params, Visibility::Public));
            }
        } else if !self.is_interface {
            implicit.push((class, &[][..], self.visibility));
        }
        for (name, params, visibility) in implicit {
            if !self.overloads.iter().any(|o| o.name == name) {
                let is_static = self.is_enum && matches!(name.as_str(), "values" | "valueOf");
                self.overloads.push(Overload {
                    name,
                    params: params.iter().map(ToString::to_string).collect(),
                    deprecated: false,
                    visibility,
                    is_static,
                    param_names: Vec::new(),
                    returns_class: false,
                });
            }
        }
    }
}

/// An open class body while scanning.
struct Open {
    /// Its index in the output.
    idx: usize,
    /// The brace depth inside its body.
    body: usize,
    /// Whether it is an enum still in its constant list (before the first `;`).
    enum_constants: bool,
    /// Whether it is an enum, whose constructors are implicitly `private`.
    is_enum: bool,
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
    // A class declaration seen, waiting for its `{`: (name, deprecated,
    // keyword, access modifier).
    let mut pending: Option<(String, bool, String, Option<Visibility>)> = None;
    // An `@Deprecated` seen, waiting for the declaration it annotates.
    let mut deprecated = false;
    // An access modifier seen, waiting for the declaration it applies to.
    let mut visibility: Option<Visibility> = None;
    // A `static` seen, waiting for the declaration it applies to.
    let mut is_static = false;
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
                if let Some((name, dep, keyword, access)) = pending.take() {
                    let parent = stack.last().map(|o| &out[o.idx]);
                    let mut path: Vec<String> = parent.map(|p| p.path.clone()).unwrap_or_default();
                    let dep = dep || parent.is_some_and(|p| p.deprecated);
                    let implicit = if parent.is_some_and(|p| p.is_interface) {
                        Visibility::Public
                    } else {
                        Visibility::Package
                    };
                    path.push(name);
                    out.push(Scanned {
                        path,
                        is_interface: keyword == "interface",
                        is_enum: keyword == "enum",
                        visibility: access.unwrap_or(implicit),
                        overloads: Vec::new(),
                        fields: BTreeMap::new(),
                        field_visibility: BTreeMap::new(),
                        deprecated: dep,
                    });
                    depth += 1;
                    let is_enum = keyword == "enum";
                    stack.push(Open { idx: out.len() - 1, body: depth, enum_constants: is_enum, is_enum });
                    i += 1;
                    continue;
                }
                if member_level {
                    deprecated = false;
                    visibility = None;
                    is_static = false;
                }
                depth += 1;
            },
            Tok::Punct('}') => {
                if stack.last().is_some_and(|o| o.body == depth) {
                    stack.pop();
                }
                depth = depth.saturating_sub(1);
                deprecated = false;
                visibility = None;
                is_static = false;
            },
            Tok::Punct(';') if member_level => {
                deprecated = false;
                visibility = None;
                is_static = false;
                if let Some(o) = stack.last_mut() {
                    o.enum_constants = false;
                }
            },
            Tok::Punct('(') => paren += 1,
            Tok::Punct(')') => paren = paren.saturating_sub(1),
            // `record` is only a contextual keyword: `void record(double v)` is a
            // method, `record Point(int x)` a declaration.
            Tok::Ident(kw)
                if paren == 0
                    && (matches!(kw.as_str(), "class" | "interface" | "enum")
                        || (kw == "record" && matches!(toks.get(i + 1), Some(Tok::Ident(_))))) =>
            {
                // `Foo.class` is a literal, not a declaration; `@interface` is one.
                let is_literal = i > 0 && toks[i - 1] == Tok::Punct('.');
                if let (false, Some(Tok::Ident(name))) = (is_literal, toks.get(i + 1)) {
                    pending = Some((name.clone(), std::mem::take(&mut deprecated), kw.clone(), visibility.take()));
                    is_static = false;
                    i += 2;
                    continue;
                }
            },
            Tok::Ident(m) if member_level && m == "static" => is_static = true,
            Tok::Ident(m) if member_level && matches!(m.as_str(), "public" | "protected" | "private") => {
                visibility = Some(match m.as_str() {
                    "public" => Visibility::Public,
                    "protected" => Visibility::Protected,
                    _ => Visibility::Private,
                });
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
                let is_enum = open.is_enum;
                if is_constant {
                    out[idx].fields.insert(name.clone(), std::mem::take(&mut deprecated));
                    out[idx].field_visibility.insert(name.clone(), Visibility::Public);
                } else if next == Some(&Tok::Punct('(')) {
                    if is_method_decl(&toks, i, &out[idx].path) {
                        let (params, param_names) = params(&toks, i + 1);
                        let returns_class = returns_class(&toks, i, &out[idx].path);
                        let implicit = if out[idx].is_interface {
                            Visibility::Public
                        } else if is_enum && out[idx].path.last() == Some(name) {
                            Visibility::Private
                        } else {
                            Visibility::Package
                        };
                        out[idx].overloads.push(Overload {
                            name: name.clone(),
                            params,
                            deprecated: std::mem::take(&mut deprecated),
                            visibility: visibility.take().unwrap_or(implicit),
                            is_static: std::mem::take(&mut is_static),
                            param_names,
                            returns_class,
                        });
                    }
                } else if matches!(next, Some(Tok::Punct('=' | ';')))
                    && name != "\"\""
                    && name != "0"
                    && !KEYWORDS.contains(&name.as_str())
                {
                    let implicit = if out[idx].is_interface {
                        Visibility::Public
                    } else {
                        Visibility::Package
                    };
                    out[idx].fields.insert(name.clone(), std::mem::take(&mut deprecated));
                    out[idx]
                        .field_visibility
                        .insert(name.clone(), visibility.take().unwrap_or(implicit));
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
/// names (annotations, `final`, generic arguments and parameter names are
/// dropped, and arrays and varargs become `[]`), and the parameter names.
fn params(toks: &[Tok], open: usize) -> (Vec<String>, Vec<String>) {
    let close = past_parens(toks, open) - 1;
    let mut out = Vec::new();
    let mut names = Vec::new();
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
            Tok::Punct(',') if angle == 0 => {
                let param = std::mem::take(&mut current);
                out.push(param_type(&param));
                names.push(param_name(&param));
            },
            Tok::Ident(f) if f == "final" => {},
            t if angle == 0 => current.push(t.clone()),
            _ => {},
        }
        j += 1;
    }
    if !current.is_empty() {
        out.push(param_type(&current));
        names.push(param_name(&current));
    }
    (out, names)
}

/// The name of one parameter from its tokens: the last identifier.
fn param_name(toks: &[Tok]) -> String {
    toks.iter()
        .rev()
        .find_map(|t| match t {
            Tok::Ident(s) => Some(s.clone()),
            Tok::Punct(_) => None,
        })
        .unwrap_or_default()
}

/// Whether the method declared by the identifier at `i` returns `class`: its
/// return type, the token before the name, is the class's simple name, possibly
/// with generic arguments (`Opts<K> name(`).
fn returns_class(toks: &[Tok], i: usize, class: &[String]) -> bool {
    let mut j = i;
    if toks.get(j.wrapping_sub(1)) == Some(&Tok::Punct('>')) {
        let mut depth = 0usize;
        while j > 0 {
            j -= 1;
            match toks[j] {
                Tok::Punct('>') => depth += 1,
                Tok::Punct('<') => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                },
                _ => {},
            }
        }
    }
    matches!(j.checked_sub(1).map(|p| &toks[p]), Some(Tok::Ident(t)) if class.last() == Some(t))
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

    #[test]
    fn maps_setter_and_field_names() {
        let src = r#"
            package x;
            public class Opts {
                public Integer timeoutMs() { return null; }
                public Opts timeoutMs(Integer timeoutMs) { return this; }
                public Opts validateOnly(boolean validateOnly) { return this; }
                public void setFoo(int foo) {}
                public void record(double value) {}
                record Point(int x) {}
                public int size() { return 0; }
                public Opts getSensor(String name) { return null; }
                public Opts sensor(String name) { return null; }
                public int getCount() { return 0; }
                public static Opts timeout(long timeout) { return null; }
                public long timeout() { return 0; }
                public Opts withTimeout(long timeout) { return this; }
                public static Opts limit(int max) { return null; }
                public int limit() { return 0; }
                public static <K> Opts<K> keyed(K key) { return null; }
                public boolean keyed() { return false; }
                public static long total(int a) { return 0; }
                public long total() { return 0; }
                public static Opts of(long ms) { return null; }
                public long eventCount;
                public static final Opts ANY = new Opts();
            }
        "#;
        let opts = class_of(src, &["Opts"]);
        // A method sharing its name with a getter, or any method with
        // parameters, may be a setter (CLAUDE.md §2).
        assert!(opts.is_rust_name_of("timeoutMs", "timeout_ms", true));
        assert!(opts.is_rust_name_of("timeoutMs", "set_timeout_ms", true));
        assert!(opts.is_rust_name_of("validateOnly", "set_validate_only", true));
        assert!(opts.is_rust_name_of("setFoo", "set_foo", true));
        assert!(!opts.is_rust_name_of("setFoo", "foo", true));
        assert!(!opts.is_rust_name_of("size", "set_size", true));
        // `getX` becomes `x`, unless the class declares an `x` that owns it.
        assert!(opts.is_rust_name_of("getCount", "count", true));
        assert!(opts.is_rust_name_of("getSensor", "get_sensor", true));
        assert!(!opts.is_rust_name_of("getSensor", "sensor", true));
        assert!(opts.is_rust_name_of("sensor", "sensor_with_parents", true));
        // A static method sharing its name with an instance one: the instance
        // one keeps the name, a factory is `with_<params>`, or `new_with_<params>`
        // when an instance method is `with_<params>`, and any other is `do_<name>`.
        assert!(opts.is_rust_name_of("timeout", "timeout", true));
        assert!(!opts.is_rust_name_of("timeout", "timeout", false));
        assert!(opts.is_rust_name_of("timeout", "new_with_timeout", false));
        assert!(!opts.is_rust_name_of("timeout", "with_timeout", false));
        assert!(!opts.is_rust_name_of("timeout", "new_timeout", false));
        assert!(opts.is_rust_name_of("limit", "with_max", false));
        assert!(!opts.is_rust_name_of("limit", "new_with_max", false));
        assert!(opts.is_rust_name_of("keyed", "with_key", false));
        assert!(opts.is_rust_name_of("total", "do_total", false));
        assert!(opts.is_rust_name_of("total", "do_total_with_a", false));
        assert!(!opts.is_rust_name_of("total", "with_a", false));
        assert!(opts.is_rust_name_of("total", "total", true));
        // A static method with no instance namesake is named as usual.
        assert!(opts.is_rust_name_of("of", "of", false));
        assert!(!opts.is_rust_name_of("of", "with_ms", false));
        // `record` names a method unless a declaration follows it.
        assert!(opts.methods.contains("record"));
        assert!(class_of(src, &["Opts", "Point"]).methods.contains("Point"));
        // A public field becomes a getter and a setter (CLAUDE.md §3).
        assert!(opts.is_rust_accessor_of("eventCount", "event_count"));
        assert!(opts.is_rust_accessor_of("eventCount", "set_event_count"));
        assert!(opts.is_rust_accessor_of("ANY", "any"));
        assert!(!opts.is_rust_accessor_of("eventCount", "count"));
    }

    #[test]
    fn adds_implicit_members() {
        let src = r#"
            package x;
            public class Plain {
                private int count;
                long size;
                public static class Nested {}
                class Inner {}
                enum Kind { A, B; public String code() { return ""; } }
                interface Api { int LIMIT = 1; void call(); }
            }
        "#;
        let vis = |class: &JavaClass, member: &str| match class.visibility(member) {
            Some(MemberVisibility::Known(v)) => Some(v),
            other => panic!("`{member}`: {other:?}"),
        };
        // A class declaring no constructor has a default one, as accessible
        // as the class.
        let plain = class_of(src, &["Plain"]);
        assert_eq!(vis(&plain, "Plain()"), Visibility::Public.into());
        assert_eq!(vis(&class_of(src, &["Plain", "Nested"]), "Nested()"), Visibility::Public.into());
        assert_eq!(vis(&class_of(src, &["Plain", "Inner"]), "Inner()"), Visibility::Package.into());
        assert_eq!(vis(&plain, "count"), Visibility::Private.into());
        assert_eq!(vis(&plain, "size"), Visibility::Package.into());
        // An enum has `values`, `valueOf`, `name` and `ordinal`, and no
        // default constructor; an interface has neither.
        let kind = class_of(src, &["Plain", "Kind"]);
        for member in ["values()", "valueOf(String)", "name()", "ordinal()"] {
            assert_eq!(vis(&kind, member), Visibility::Public.into(), "{member}");
        }
        assert_eq!(kind.visibility("Kind"), None);
        let api = class_of(src, &["Plain", "Api"]);
        assert_eq!(api.methods.iter().collect::<Vec<_>>(), ["call"]);
        assert_eq!(vis(&api, "LIMIT"), Visibility::Public.into());
    }

    #[test]
    fn scans_visibilities() {
        let src = r#"
            package x;
            public class Outer {
                public Outer() {}
                private Outer(int a) {}
                protected void hook() {}
                void helper() {}
                /** @deprecated */ @Deprecated public static final int F = 1;
                private static List<String> tags(String... names) { return null; }
                public Set<String> tags() { return null; }
                @Override public String toString() { return ""; }
                interface Api { void call(); private void inner() {} default boolean ok() { return true; } }
                enum Kind { A; Kind() {} public String code() { return ""; } int rank() { return 0; } }
            }
        "#;
        let outer = class_of(src, &["Outer"]);
        let vis = |class: &JavaClass, member: &str| match class.visibility(member) {
            Some(MemberVisibility::Known(v)) => Some(v),
            Some(MemberVisibility::Ambiguous(public)) => panic!("`{member}` is ambiguous: {public:?}"),
            None => None,
        };
        assert_eq!(vis(&outer, "Outer()"), Some(Visibility::Public));
        assert_eq!(vis(&outer, "Outer(int)"), Some(Visibility::Private));
        // A name alone, when only some of its overloads are public, does not
        // say which one it translates.
        assert_eq!(
            outer.visibility("Outer"),
            Some(MemberVisibility::Ambiguous(vec!["Outer()".into()]))
        );
        assert_eq!(
            outer.visibility("tags"),
            Some(MemberVisibility::Ambiguous(vec!["tags()".into()]))
        );
        assert_eq!(vis(&outer, "hook"), Some(Visibility::Protected));
        assert_eq!(vis(&outer, "helper"), Some(Visibility::Package));
        assert_eq!(vis(&outer, "tags(String[])"), Some(Visibility::Private));
        assert_eq!(vis(&outer, "tags()"), Some(Visibility::Public));
        assert_eq!(vis(&outer, "toString"), Some(Visibility::Public));
        // A field a getter translates has its own visibility; an unknown
        // member has none.
        assert_eq!(vis(&outer, "F"), Some(Visibility::Public));
        assert_eq!(vis(&outer, "F()"), None);
        assert_eq!(vis(&outer, "missing"), None);
        let api = class_of(src, &["Outer", "Api"]);
        assert_eq!(vis(&api, "call"), Some(Visibility::Public));
        assert_eq!(vis(&api, "inner"), Some(Visibility::Private));
        assert_eq!(vis(&api, "ok"), Some(Visibility::Public));
        let kind = class_of(src, &["Outer", "Kind"]);
        assert_eq!(vis(&kind, "Kind"), Some(Visibility::Private));
        assert_eq!(vis(&kind, "code"), Some(Visibility::Public));
        assert_eq!(vis(&kind, "A"), Some(Visibility::Public));
        assert_eq!(vis(&kind, "rank"), Some(Visibility::Package));
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
            field_visibility: BTreeMap::new(),
            deprecated: false,
            is_test: false,
        };
        assert!(class.is_rust_name_of("send", "send", true));
        assert!(class.is_rust_name_of("send", "send_with_callback", true));
        assert!(!class.is_rust_name_of("send", "sender", true));
        assert!(class.is_rust_name_of("maybeThrowAnyException", "maybe_return_any_error", true));
        assert!(class.is_rust_name_of("getAPIVersion", "get_api_version", true));
        assert!(class.is_rust_name_of("getAPIVersion", "api_version", true));
        assert!(class.is_rust_name_of("KafkaProducer", "new", true));
        assert!(class.is_rust_name_of("KafkaProducer", "with_config", true));
        assert!(class.is_rust_name_of("type", "type_", true));
        assert!(class.is_rust_name_of("type", "r#type", true));
        assert!(!class.is_rust_name_of("KafkaProducer", "create", true));
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
            field_visibility: BTreeMap::new(),
            deprecated: false,
            is_test: false,
        };
        let oor = class(&["consumer"], "clients.consumer", "OffsetOutOfRangeException");
        assert_eq!(oor.rust_name(), "ConsumerOffsetOutOfRangeError");
        let rnf = class(&["common", "errors"], "common.errors", "ResourceNotFoundException");
        assert_eq!(rnf.rust_name(), "ResourceNotFoundError");
        let acl = class(&["common", "acl"], "common.acl", "AclBinding");
        assert_eq!(acl.rust_name(), "AclBinding");
        let mut origin = class(&["common", "errors"], "common.errors", "RecordDeserializationException");
        origin.path.push("DeserializationExceptionOrigin".into());
        assert_eq!(origin.rust_name(), "DeserializationErrorOrigin");
        let ssl = class(&["common", "errors"], "common.errors", "SSLException");
        assert_eq!(ssl.rust_name(), "SSLError");
        let handler = class(&["consumer"], "clients.consumer", "ExceptionsThrowHandler");
        assert_eq!(handler.rust_name(), "ErrorsReturnHandler");
        let exceptional = class(&["common"], "common", "ExceptionalThrowable");
        assert_eq!(exceptional.rust_name(), "ExceptionalThrowable");
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
