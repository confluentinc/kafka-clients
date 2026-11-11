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

//! `cargo xtask java-index --json [<out>]`: the [`JavaIndex`] the Rust lint
//! builds, written as JSON for the Node.js client's rule linter
//! (`javascript/scripts/lint_rules.js`), so both lints read one Java truth.
//!
//! The file holds the main classes only, sorted by package then nested path,
//! each with the flags the audience and deprecation rules decide on:
//!
//! ```text
//! { "schema": 1, "audience_ref": "4.4.0-rc3", "deprecation_refs": ["4.3.1", "4.4.0-rc3"],
//!   "classes": [ { "fqn", "package", "path", "kind", "visibility", "abstract",
//!                  "functional_interface", "supertypes", "public_audience",
//!                  "unsupported_package", "deprecated", "javadoc",
//!                  "overloads": [ { "name", "params", "param_names", "param_types", "return_type",
//!                                   "visibility", "static", "default", "deprecated",
//!                                   "javadoc" } ],
//!                  "fields": [ { "name", "type", "visibility", "static", "final",
//!                                "deprecated", "javadoc" } ],
//!                  "enum_constants" } ] }
//! ```
//!
//! `params` holds the erased simple type names the form keys use
//! (`BiConsumer`), `param_types` the types as written
//! (`BiConsumer<? super T, ? super Throwable>`).
//!
//! A `javadoc` is `null` or `{ "text", "params": [{ "name", "text" }],
//! "returns", "throws": [{ "class", "text" }], "words" }`: the text as
//! [`java::clean_javadoc`] leaves it (an overload's after
//! [`JavaIndex::overload_doc`] resolves inheritance), its block tags split out
//! by [`javadoc::parse`], and the word count of its translation, counted as
//! the `check-api-docs` size rule counts words. The count is the doc's own: a
//! Javadoc that only points elsewhere (`@see KafkaConsumer#commitSync()`,
//! `See {@link KafkaProducer#close()}`) counts its pointer, where
//! `check-api-docs` measures the Javadoc pointed to
//! (`.claude/rules/documentation.md` §4).
//!
//! Before writing, the command checks the index for fields the scanner may
//! take from what is not a field (a `throws` clause, a lambda body) and fails
//! if it finds one, so the rules reading the file never see one.
//!
//! The JSON is written by hand (xtask takes no serialization dependency), with
//! keys in a fixed order, so the output is byte-for-byte deterministic.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use crate::java::{self, Deprecation, JavaClass, JavaIndex, JavaKind, Visibility};
use crate::javadoc::{self, Translator};
use crate::lint_custom::{parse_public_list, PUBLIC_AUDIENCE_LIST};

/// Where the index is written when no path is given: the Node.js client's
/// git-ignored build directory, relative to `rust/`.
pub const DEFAULT_OUT: &str = "../javascript/build/java-index.json";

/// The schema version, raised on any incompatible change to the layout.
const SCHEMA: usize = 1;

/// Runs the command: `args` are what follows `java-index`.
pub fn run(args: &[String]) -> anyhow::Result<()> {
    let mut json = false;
    let mut out: Option<PathBuf> = None;
    for arg in args {
        match arg.as_str() {
            "--json" => json = true,
            other if !other.starts_with('-') && out.is_none() => out = Some(PathBuf::from(other)),
            other => anyhow::bail!("unknown java-index argument `{other}`; usage: java-index --json [<out>]"),
        }
    }
    if !json {
        anyhow::bail!("java-index writes JSON only; usage: java-index --json [<out>]");
    }
    let out = out.unwrap_or_else(|| PathBuf::from(DEFAULT_OUT));

    let mut index = JavaIndex::load();
    if index.is_empty() {
        anyhow::bail!(
            "no Java sources at `{}` (run `git submodule update --init kafka`)",
            java::JAVA_MAIN_ROOT
        );
    }
    let list = fs::read_to_string(java::DEPRECATED_LIST)
        .map_err(|e| anyhow::anyhow!("cannot read the deprecation list `{}`: {e}", java::DEPRECATED_LIST))?;
    index.merge_deprecation_list(&list);
    let public = parse_public_list(
        &fs::read_to_string(PUBLIC_AUDIENCE_LIST)
            .map_err(|e| anyhow::anyhow!("cannot read the Public list `{PUBLIC_AUDIENCE_LIST}`: {e}"))?,
    );
    if public.is_empty() {
        anyhow::bail!("the Public list `{PUBLIC_AUDIENCE_LIST}` names no class");
    }
    let unsupported = java::unsupported_packages_at_ref(java::AUDIENCE_REF)
        .filter(|packages| !packages.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no unsupported-API disclaimers at Kafka `{}`: the ref is not in the `kafka` submodule \
                 (run `cargo xtask fetch-java-refs`)",
                java::AUDIENCE_REF
            )
        })?;

    let phantoms = phantom_fields(&index);
    if !phantoms.is_empty() {
        anyhow::bail!(
            "the Java scanner took {} non-field(s) for fields, which the index must not list:\n  {}",
            phantoms.len(),
            phantoms.join("\n  ")
        );
    }
    let text = to_json(&index, &public, &unsupported);
    write(&out, &text)?;
    let classes = index.classes.iter().filter(|c| !c.is_test).count();
    println!("✅ Wrote {classes} Java class(es) to {}", out.display());
    Ok(())
}

/// Writes `text` to `out`, creating its directory.
fn write(out: &Path, text: &str) -> anyhow::Result<()> {
    if let Some(dir) = out.parent().filter(|d| !d.as_os_str().is_empty()) {
        fs::create_dir_all(dir).map_err(|e| anyhow::anyhow!("cannot create `{}`: {e}", dir.display()))?;
    }
    fs::write(out, text).map_err(|e| anyhow::anyhow!("cannot write `{}`: {e}", out.display()))
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
/// `org.apache.kafka` carrying the unsupported-API disclaimer.
pub fn to_json(index: &JavaIndex, public: &BTreeSet<String>, unsupported: &BTreeSet<String>) -> String {
    let translator = Translator { index: Some(index), panics: false };
    let mut classes: Vec<&JavaClass> = index.classes.iter().filter(|c| !c.is_test).collect();
    classes.sort_by(|a, b| (&a.package, &a.path).cmp(&(&b.package, &b.path)));
    let mut out = String::new();
    out.push_str("{\"schema\":");
    out.push_str(&SCHEMA.to_string());
    out.push_str(",\"audience_ref\":");
    out.push_str(&string(java::AUDIENCE_REF));
    out.push_str(",\"deprecation_refs\":");
    out.push_str(&array(java::DEPRECATION_REFS.iter().map(|r| string(r))));
    out.push_str(",\"classes\":[");
    for (n, class) in classes.iter().enumerate() {
        out.push_str(if n == 0 { "\n" } else { ",\n" });
        out.push_str(&class_json(index, &translator, class, public, unsupported));
    }
    out.push_str("\n]}\n");
    out
}

/// One class's object.
fn class_json(
    index: &JavaIndex,
    translator: &Translator<'_>,
    class: &JavaClass,
    public: &BTreeSet<String>,
    unsupported: &BTreeSet<String>,
) -> String {
    let top_level = format!("{}{}.{}", java::MARKER_PREFIX, class.package, class.path[0]);
    let overloads = class.overloads.iter().map(|o| {
        let signature = o.signature();
        object(&[
            ("name", string(&o.name)),
            ("params", array(o.params.iter().map(|p| string(p)))),
            ("param_names", array(o.param_names.iter().map(|p| string(p)))),
            ("param_types", array(o.param_types.iter().map(|p| string(p)))),
            ("return_type", o.return_type.as_deref().map_or_else(null, string)),
            ("visibility", string(visibility(o.visibility))),
            ("static", boolean(o.is_static)),
            ("default", boolean(o.is_default)),
            ("deprecated", boolean(class.deprecation(Some(&signature)) == Deprecation::Yes)),
            ("javadoc", doc_json(translator, index.overload_doc(class, o).as_deref())),
        ])
    });
    let fields = class.fields.iter().map(|(name, deprecated)| {
        let decl = class.field_decls.get(name);
        object(&[
            ("name", string(name)),
            ("type", decl.map_or_else(null, |d| string(&d.ty))),
            (
                "visibility",
                string(visibility(
                    class.field_visibility.get(name).copied().unwrap_or(Visibility::Package),
                )),
            ),
            ("static", boolean(decl.is_some_and(|d| d.is_static))),
            ("final", boolean(decl.is_some_and(|d| d.is_final))),
            ("deprecated", boolean(*deprecated || class.deprecated)),
            ("javadoc", doc_json(translator, class.field_docs.get(name).map(String::as_str))),
        ])
    });
    object(&[
        ("fqn", string(&class.marker())),
        ("package", string(&class.package)),
        ("path", array(class.path.iter().map(|p| string(p)))),
        ("kind", string(kind(class.kind))),
        ("visibility", string(visibility(class.visibility))),
        ("abstract", boolean(class.is_abstract)),
        ("functional_interface", boolean(class.functional_interface)),
        ("supertypes", array(class.supertypes.iter().map(|s| string(s)))),
        ("public_audience", boolean(public.contains(&top_level))),
        ("unsupported_package", boolean(unsupported.contains(&class.package))),
        ("deprecated", boolean(class.deprecation(None) == Deprecation::Yes)),
        ("javadoc", doc_json(translator, class.javadoc.as_deref())),
        ("overloads", array(overloads)),
        ("fields", array(fields)),
        ("enum_constants", array(class.enum_constants.iter().map(|c| string(c)))),
    ])
}

/// A Javadoc's object, or `null`.
fn doc_json(translator: &Translator<'_>, doc: Option<&str>) -> String {
    let Some(doc) = doc else { return null() };
    let parsed = javadoc::parse(doc);
    let params = parsed
        .tags
        .iter()
        .filter(|t| t.name == "param")
        .map(|t| object(&[("name", string(&t.arg)), ("text", string(&t.text))]));
    let returns = parsed
        .tags
        .iter()
        .find(|t| t.name == "return")
        .map_or_else(null, |t| string(&t.text));
    let throws = parsed
        .tags
        .iter()
        .filter(|t| t.name == "throws" || t.name == "exception")
        .map(|t| object(&[("class", string(&t.arg)), ("text", string(&t.text))]));
    let words = javadoc::word_count(&translator.to_rustdoc(doc));
    object(&[
        ("text", string(doc)),
        ("params", array(params)),
        ("returns", returns),
        ("throws", array(throws)),
        ("words", words.to_string()),
    ])
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
    fn writes_the_schema_with_flags_and_tags() {
        let mut index = index(FIXTURE);
        index.merge_deprecation_list("org.apache.kafka.clients.producer.Rec#TOPIC_CONFIG\n");
        let public: BTreeSet<String> = ["org.apache.kafka.clients.producer.Rec".to_string()].into();
        let unsupported: BTreeSet<String> = ["clients.producer".to_string()].into();
        let json = to_json(&index, &public, &unsupported);
        let lines: Vec<&str> = json.lines().map(|l| l.trim_end_matches(',')).collect();
        assert_eq!(
            lines[0],
            r#"{"schema":1,"audience_ref":"4.4.0-rc3","deprecation_refs":["4.3.1","4.4.0-rc3"],"classes":["#
        );
        // One class per line, sorted by nested path; the test class is left out.
        assert_eq!(lines.len(), 5, "{json}");
        assert_eq!(lines[4], "]}");
        let rec = lines[1];
        let has = |line: &str, part: &str| assert!(line.contains(part), "{part}\nnot in\n{line}");
        assert!(
            rec.starts_with(concat!(
                r#"{"fqn":"org.apache.kafka.clients.producer.Rec","package":"clients.producer","path":["Rec"],"#,
                r#""kind":"class","visibility":"public","abstract":false,"functional_interface":false,"#,
                r#""supertypes":["Comparable"],"public_audience":true,"unsupported_package":true,"deprecated":false,"#,
                r#""javadoc":{"text":"A \"record\".","params":[],"returns":null,"throws":[],"words":2},"overloads":["#
            )),
            "{rec}"
        );
        has(
            rec,
            concat!(
                r#"{"name":"send","params":["Duration"],"param_names":["timeout"],"param_types":["Duration"],"#,
                r#""return_type":"Future<RecordMetadata>","visibility":"public","static":false,"default":false,"#,
                r#""deprecated":false,"javadoc":{"text":"Sends it.\n\n@param timeout how long\n@param <T> the type\n"#,
                r#"@return the {@code Future}\n@throws IllegalStateException if closed\n"#,
                r#"@exception org.apache.kafka.common.KafkaException otherwise","#,
                r#""params":[{"name":"timeout","text":"how long"},{"name":"<T>","text":"the type"}],"#,
                r#""returns":"the {@code Future}","#,
                r#""throws":[{"class":"IllegalStateException","text":"if closed"},"#,
                r#"{"class":"org.apache.kafka.common.KafkaException","text":"otherwise"}],"words":"#
            ),
        );
        has(
            rec,
            concat!(
                r#"{"name":"all","params":["String[]"],"param_names":["names"],"param_types":["String[]"],"#,
                r#""return_type":"Map<String, ? extends List<T>>[]","visibility":"public","static":true,"#,
                r#""default":false,"deprecated":true,"javadoc":null}"#
            ),
        );
        has(
            rec,
            concat!(
                r#"{"name":"Rec","params":["String"],"param_names":["topic"],"param_types":["String"],"return_type":null,"#,
                r#""visibility":"package-private","static":false,"default":false,"deprecated":false,"javadoc":null}"#
            ),
        );
        // A tab in a Javadoc is escaped; the listed field is deprecated.
        has(
            rec,
            concat!(
                r#"{"name":"TOPIC_CONFIG","type":"String","visibility":"public","static":true,"final":true,"#,
                r#""deprecated":true,"javadoc":{"text":"The topic.\nTab\there.","#
            ),
        );
        has(
            rec,
            concat!(
                r#"{"name":"count","type":"int","visibility":"package-private","static":false,"final":false,"#,
                r#""deprecated":false,"javadoc":null}"#
            ),
        );
        assert!(rec.ends_with(r#""enum_constants":[]}"#), "{rec}");
        // A nested class takes its top-level class's audience.
        has(lines[2], r#"{"fqn":"org.apache.kafka.clients.producer.Rec$Callback","#);
        has(lines[2], r#""kind":"interface","visibility":"public","#);
        has(lines[2], r#""public_audience":true,"#);
        has(lines[3], r#""fqn":"org.apache.kafka.clients.producer.Rec$Mode""#);
        assert!(lines[3].ends_with(r#""enum_constants":["ON","OFF"]}"#), "{}", lines[3]);
        // Deterministic.
        assert_eq!(json, to_json(&index, &public, &unsupported));
    }

    #[test]
    fn flags_follow_the_lists() {
        let mut index = index("public class A { } public class B { }");
        index.merge_deprecation_list("org.apache.kafka.clients.producer.B\n");
        let json = to_json(&index, &BTreeSet::new(), &BTreeSet::new());
        assert!(json.contains(r#""fqn":"org.apache.kafka.clients.producer.A""#));
        assert!(json.contains(r#""public_audience":false,"unsupported_package":false,"deprecated":false"#));
        assert!(json.contains(r#""public_audience":false,"unsupported_package":false,"deprecated":true"#));
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
