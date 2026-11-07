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

//! `cargo xtask java-api <file>`: writes the public API of the Java client to
//! `<file>`, one line per type and member, for the bindings' Java-name checks
//! (the .NET binding's CKRDA0013 reads `dotnet/analyzers/java-api.txt`). The
//! list depends only on the Java sources, so every binding can share it.
//! `cargo xtask java-api --check <file>` writes nothing: it fails when `<file>`
//! differs from what the command would write.
//!
//! A type is listed when its top-level class is on the `@InterfaceAudience.Public`
//! list, is one of [`TEST_JAR_CLASSES`], or is an exception class the error
//! generator makes a class for, and every class on its path is `public`. A
//! member is listed when it is `public`. Deprecated types and members are left
//! out, whether the `kafka` working tree or [`java::DEPRECATED_LIST`] marks them:
//! deprecated API is not translated. The Java built-in exceptions the bindings
//! translate, whose source is not in the tree, are listed from the error
//! generator's table of their Java 11 constructors.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;

use crate::error_hierarchy::{self, JdkCtor};
use crate::java::{self, JavaClass, JavaIndex, JavaKind, Overload, Visibility};

/// The `@InterfaceAudience.Public` list, relative to `rust/`.
const PUBLIC_AUDIENCE_LIST: &str = "../design/current/interface-audience-public-4.4.txt";

/// The bindings' copies of the list, relative to `rust/`: `cargo xtask check-generated`
/// fails when one is out of date with the Java sources.
pub(crate) const BINDING_COPIES: &[&str] = &["../dotnet/analyzers/java-api.txt"];

/// Classes Java ships in its test jar that the bindings translate as public
/// API: `MockAdminClient` (`.claude/rules/admin-client.md` §9).
const TEST_JAR_CLASSES: &[&str] = &["org.apache.kafka.clients.admin.MockAdminClient"];

const HEADER: &str = "\
# The public API of the Java client, read from the `kafka` submodule by `cargo xtask java-api <file>`,
# for the bindings' Java-name checks. Do not edit; rerun the command.
# A type is listed when its top-level class is in design/current/interface-audience-public-4.4.txt,
# is MockAdminClient (from the test jar) or is an exception class the error generator makes a class
# for, and every class on its path is public; only public members are listed, and nothing in
# design/current/java-deprecated.txt. The Java built-in exceptions the bindings translate are listed
# with their Java 11 constructors. Members Java declares implicitly are listed too: a class's default
# constructor, and an enum's name(), ordinal(), values() and valueOf(String); a parameter whose name
# the source does not spell is `?`.
# One tab-separated line per item; a nested class is Outer$Inner:
#   type      <class>  <class|abstract class|interface|enum|record>  <supertypes, comma-separated, or ->
#   ctor      <class>  <Name(Type name, ...)>
#   method    <class>  <static|instance>  <name(Type name, ...)>
#   field     <class>  <NAME>
#   constant  <class>  <NAME>   (an enum constant)
";

/// What the command was asked to do with its file.
#[derive(Debug, PartialEq, Eq)]
enum Mode<'a> {
    /// Write the list to the file.
    Write(&'a str),
    /// Fail when the file differs from the list.
    Check(&'a str),
}

/// `<file>` or `--check <file>`.
fn parse_args(args: &[String]) -> anyhow::Result<Mode<'_>> {
    match args {
        [flag, file] if flag == "--check" => Ok(Mode::Check(file)),
        [file] if !file.starts_with("--") => Ok(Mode::Write(file)),
        _ => anyhow::bail!(
            "usage: cargo xtask java-api [--check] <file>, e.g. `cargo xtask java-api ../dotnet/analyzers/java-api.txt`"
        ),
    }
}

/// Writes the list to the file `args` names, or, with `--check`, fails when
/// that file is out of date.
pub fn run(args: &[String]) -> anyhow::Result<()> {
    let mode = parse_args(args)?;
    let (text, types, members) = generate()?;
    match mode {
        Mode::Write(file) => {
            fs::write(file, text).map_err(|e| anyhow::anyhow!("{file}: {e}"))?;
            println!("✅ Wrote {types} types and {members} members to {file}");
        },
        Mode::Check(file) => {
            let on_disk = fs::read_to_string(file)
                .map_err(|e| anyhow::anyhow!("{file}: {e}: run `cargo xtask java-api {file}`"))?;
            check(file, &on_disk, &text)?;
            println!("✅ {file} is up to date ({types} types and {members} members)");
        },
    }
    Ok(())
}

/// Fails, naming the file and its first line that differs, when a binding's copy
/// of the list is out of date.
pub(crate) fn check_binding_copies() -> anyhow::Result<()> {
    let (text, _, _) = generate()?;
    for file in BINDING_COPIES {
        let on_disk =
            fs::read_to_string(file).map_err(|e| anyhow::anyhow!("{file}: {e}: run `cargo xtask java-api {file}`"))?;
        check(file, &on_disk, &text)?;
    }
    Ok(())
}

/// Fails, naming the first line that differs, when `on_disk` is not `expected`.
fn check(file: &str, on_disk: &str, expected: &str) -> anyhow::Result<()> {
    if on_disk == expected {
        return Ok(());
    }
    let mut found = on_disk.lines();
    let mut wanted = expected.lines();
    let mut line = 1;
    loop {
        match (wanted.next(), found.next()) {
            (Some(w), Some(f)) if w == f => line += 1,
            (None, None) => anyhow::bail!(
                "{file} is out of date with the Java sources: it differs only in its line endings or its final \
                 newline; run `cargo xtask java-api {file}`"
            ),
            (w, f) => {
                anyhow::bail!(
                    "{file} is out of date with the Java sources: line {line} should be `{}` but is `{}`; \
                     run `cargo xtask java-api {file}`",
                    w.unwrap_or("<end of file>"),
                    f.unwrap_or("<end of file>"),
                )
            },
        }
    }
}

/// The list's text, with how many types and members it holds, read from the
/// `kafka` submodule and the two lists.
fn generate() -> anyhow::Result<(String, usize, usize)> {
    let mut index = JavaIndex::load();
    anyhow::ensure!(
        !index.is_empty(),
        "the `kafka` submodule is not checked out (../kafka is empty)"
    );
    let deprecated =
        fs::read_to_string(java::DEPRECATED_LIST).map_err(|e| anyhow::anyhow!("{}: {e}", java::DEPRECATED_LIST))?;
    index.merge_deprecation_list(&deprecated);
    let mut public = parse_list(
        &fs::read_to_string(PUBLIC_AUDIENCE_LIST).map_err(|e| anyhow::anyhow!("{PUBLIC_AUDIENCE_LIST}: {e}"))?,
    );
    public.extend(error_hierarchy::generated_kafka_classes().map(str::to_string));
    let jdk: Vec<_> = error_hierarchy::jdk_exceptions().collect();
    Ok(render(&index, &public, &jdk))
}

/// The fully qualified names of a list file: one per line, `#` comments.
fn parse_list(text: &str) -> BTreeSet<String> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_string)
        .collect()
}

/// The list's text, with how many types and members it holds: the classes of
/// `index` that `public` lists, and the JDK classes of `jdk` (FQN, parent FQN,
/// constructors), which sort before them.
pub(crate) fn render(
    index: &JavaIndex,
    public: &BTreeSet<String>,
    jdk: &[(&str, &str, &[JdkCtor])],
) -> (String, usize, usize) {
    let by_path: BTreeMap<(&str, &[String], bool), &JavaClass> = index
        .classes
        .iter()
        .map(|c| ((c.package.as_str(), c.path.as_slice(), c.is_test), c))
        .collect();
    let mut listed: Vec<&JavaClass> = index.classes.iter().filter(|c| is_listed(c, public, &by_path)).collect();
    listed.sort_by_key(|c| c.marker());

    let mut out = String::from(HEADER);
    let mut members = 0;
    let mut jdk = jdk.to_vec();
    jdk.sort_by_key(|(fqn, _, _)| *fqn);
    for (fqn, parent, ctors) in &jdk {
        out.push_str(&format!("type\t{fqn}\tclass\t{parent}\n"));
        let simple = fqn.rsplit('.').next().unwrap_or(fqn);
        let mut lines: Vec<String> = ctors
            .iter()
            .map(|params| {
                let params: Vec<String> = params.iter().map(|(ty, name)| format!("{ty} {name}")).collect();
                format!("ctor\t{fqn}\t{simple}({})", params.join(", "))
            })
            .collect();
        lines.sort();
        members += lines.len();
        for line in lines {
            out.push_str(&line);
            out.push('\n');
        }
    }
    for class in &listed {
        let marker = class.marker();
        let supertypes: Vec<String> = class
            .supertypes
            .iter()
            .map(|written| {
                index
                    .supertype(class, written)
                    .map_or_else(|| written.clone(), JavaClass::marker)
            })
            .collect();
        let supertypes = if supertypes.is_empty() {
            "-".to_string()
        } else {
            supertypes.join(",")
        };
        out.push_str(&format!("type\t{marker}\t{}\t{supertypes}\n", kind(class)));

        let mut lines = Vec::new();
        for o in class
            .overloads
            .iter()
            .filter(|o| o.visibility == Visibility::Public && !o.deprecated)
        {
            if o.name == class.name() {
                lines.push(format!("ctor\t{marker}\t{}", signature(o)));
            } else {
                let scope = if o.is_static { "static" } else { "instance" };
                lines.push(format!("method\t{marker}\t{scope}\t{}", signature(o)));
            }
        }
        for (name, &deprecated) in &class.fields {
            if deprecated || class.field_visibility.get(name) != Some(&Visibility::Public) {
                continue;
            }
            let item = if class.enum_constants.contains(name) {
                "constant"
            } else {
                "field"
            };
            lines.push(format!("{item}\t{marker}\t{name}"));
        }
        lines.sort();
        lines.dedup();
        members += lines.len();
        for line in lines {
            out.push_str(&line);
            out.push('\n');
        }
    }
    (out, jdk.len() + listed.len(), members)
}

/// Whether `class` is public API that is not deprecated.
fn is_listed(
    class: &JavaClass,
    public: &BTreeSet<String>,
    by_path: &BTreeMap<(&str, &[String], bool), &JavaClass>,
) -> bool {
    let top = class.marker().split('$').next().unwrap_or_default().to_string();
    let audience = if class.is_test {
        TEST_JAR_CLASSES.contains(&top.as_str())
    } else {
        public.contains(&top)
    };
    if !audience || class.deprecated {
        return false;
    }
    // The class and every class enclosing it must be public.
    (1..=class.path.len()).all(|depth| {
        by_path
            .get(&(class.package.as_str(), &class.path[..depth], class.is_test))
            .is_some_and(|c| c.visibility == Visibility::Public)
    })
}

fn kind(class: &JavaClass) -> &'static str {
    match class.kind {
        JavaKind::Class if class.is_abstract => "abstract class",
        JavaKind::Class => "class",
        JavaKind::Interface => "interface",
        JavaKind::Enum => "enum",
        JavaKind::Record => "record",
    }
}

/// `name(Type name, ...)`; a parameter whose name the scanner did not read is `?`.
fn signature(o: &Overload) -> String {
    let params: Vec<String> = o
        .params
        .iter()
        .enumerate()
        .map(|(i, ty)| format!("{ty} {}", o.param_names.get(i).map_or("?", String::as_str)))
        .collect();
    format!("{}({})", o.name, params.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::java::push_classes;

    fn index(sources: &[(&[&str], &str, bool)]) -> JavaIndex {
        let mut classes = Vec::new();
        for (package, source, is_test) in sources {
            let package: Vec<String> = package.iter().map(|s| s.to_string()).collect();
            push_classes(&package, source, *is_test, &mut classes);
        }
        JavaIndex { classes }
    }

    fn public(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    /// The list without its header.
    fn body(index: &JavaIndex, public: &BTreeSet<String>) -> Vec<String> {
        render(index, public, &[])
            .0
            .lines()
            .filter(|l| !l.starts_with('#'))
            .map(str::to_string)
            .collect()
    }

    #[test]
    fn lists_public_members_only() {
        let src = r#"
            package org.apache.kafka.common;
            public class Node {
                public static final int NO_ID = -1;
                private final int id;
                public Node(int id, String host, int port) { }
                Node(int id) { }
                public int id() { return id; }
                public static Node noNode() { return null; }
                private void hidden() { }
                protected void forSubclasses() { }
            }
        "#;
        let index = index(&[(&["common"], src, false)]);
        assert_eq!(
            body(&index, &public(&["org.apache.kafka.common.Node"])),
            [
                "type\torg.apache.kafka.common.Node\tclass\t-",
                "ctor\torg.apache.kafka.common.Node\tNode(int id, String host, int port)",
                "field\torg.apache.kafka.common.Node\tNO_ID",
                "method\torg.apache.kafka.common.Node\tinstance\tid()",
                "method\torg.apache.kafka.common.Node\tstatic\tnoNode()",
            ]
        );
    }

    #[test]
    fn leaves_out_classes_not_on_the_public_list() {
        let src = "package x; public class Internal { public void run() { } }";
        let index = index(&[(&["common"], src, false)]);
        assert!(body(&index, &public(&[])).is_empty());
    }

    #[test]
    fn lists_public_nested_classes_of_public_classes_only() {
        let src = r#"
            package x;
            public class Outer {
                public static class Open { public int a() { return 0; } }
                private static class Closed { public int b() { return 0; } }
                static class PackagePrivate {
                    public static class Inner { public int c() { return 0; } }
                }
            }
        "#;
        let index = index(&[(&["clients", "admin"], src, false)]);
        let lines = body(&index, &public(&["org.apache.kafka.clients.admin.Outer"]));
        assert_eq!(
            lines,
            [
                "type\torg.apache.kafka.clients.admin.Outer\tclass\t-",
                "ctor\torg.apache.kafka.clients.admin.Outer\tOuter()",
                "type\torg.apache.kafka.clients.admin.Outer$Open\tclass\t-",
                "ctor\torg.apache.kafka.clients.admin.Outer$Open\tOpen()",
                "method\torg.apache.kafka.clients.admin.Outer$Open\tinstance\ta()",
            ]
        );
    }

    #[test]
    fn leaves_out_deprecated_classes_and_overloads() {
        let src = r#"
            package x;
            public class Consumer {
                public void close() { }
                @Deprecated
                public void close(long timeout) { }
                @Deprecated
                public static class Old { public void f() { } }
            }
        "#;
        let index = index(&[(&["clients", "consumer"], src, false)]);
        assert_eq!(
            body(&index, &public(&["org.apache.kafka.clients.consumer.Consumer"])),
            [
                "type\torg.apache.kafka.clients.consumer.Consumer\tclass\t-",
                "ctor\torg.apache.kafka.clients.consumer.Consumer\tConsumer()",
                "method\torg.apache.kafka.clients.consumer.Consumer\tinstance\tclose()",
            ]
        );
    }

    #[test]
    fn leaves_out_what_the_deprecated_list_names() {
        let src = "package x; public class Admin { public void a() { } public void b() { } }";
        let mut index = index(&[(&["clients", "admin"], src, false)]);
        index.merge_deprecation_list("org.apache.kafka.clients.admin.Admin#b()\n");
        assert_eq!(
            body(&index, &public(&["org.apache.kafka.clients.admin.Admin"])),
            [
                "type\torg.apache.kafka.clients.admin.Admin\tclass\t-",
                "ctor\torg.apache.kafka.clients.admin.Admin\tAdmin()",
                "method\torg.apache.kafka.clients.admin.Admin\tinstance\ta()",
            ]
        );
    }

    #[test]
    fn lists_interface_methods_and_enum_constants() {
        let callback =
            "package x; public interface Callback { void onCompletion(RecordMetadata metadata, Exception exception); }";
        let level = r#"
            package x;
            public enum IsolationLevel {
                READ_UNCOMMITTED((byte) 0), READ_COMMITTED((byte) 1);
                private final byte id;
                IsolationLevel(byte id) { this.id = id; }
                public byte id() { return id; }
            }
        "#;
        let index = index(&[(&["clients", "producer"], callback, false), (&["common"], level, false)]);
        let lines = body(
            &index,
            &public(&[
                "org.apache.kafka.clients.producer.Callback",
                "org.apache.kafka.common.IsolationLevel",
            ]),
        );
        assert_eq!(
            lines,
            [
                "type\torg.apache.kafka.clients.producer.Callback\tinterface\t-",
                "method\torg.apache.kafka.clients.producer.Callback\tinstance\tonCompletion(RecordMetadata metadata, Exception exception)",
                "type\torg.apache.kafka.common.IsolationLevel\tenum\t-",
                "constant\torg.apache.kafka.common.IsolationLevel\tREAD_COMMITTED",
                "constant\torg.apache.kafka.common.IsolationLevel\tREAD_UNCOMMITTED",
                "method\torg.apache.kafka.common.IsolationLevel\tinstance\tid()",
                "method\torg.apache.kafka.common.IsolationLevel\tinstance\tname()",
                "method\torg.apache.kafka.common.IsolationLevel\tinstance\tordinal()",
                "method\torg.apache.kafka.common.IsolationLevel\tstatic\tvalueOf(String ?)",
                "method\torg.apache.kafka.common.IsolationLevel\tstatic\tvalues()",
            ]
        );
    }

    #[test]
    fn resolves_kafka_supertypes_and_keeps_jdk_ones() {
        let records = "package x; public class ConsumerRecords<K, V> implements Iterable<ConsumerRecord<K, V>> { }";
        let options = "package x; public abstract class AbstractOptions<T> { }";
        let create = "package x; public class CreateTopicsOptions extends AbstractOptions<CreateTopicsOptions> { }";
        let index = index(&[
            (&["clients", "consumer"], records, false),
            (&["clients", "admin"], options, false),
            (&["clients", "admin"], create, false),
        ]);
        let lines = body(
            &index,
            &public(&[
                "org.apache.kafka.clients.consumer.ConsumerRecords",
                "org.apache.kafka.clients.admin.CreateTopicsOptions",
            ]),
        );
        assert_eq!(
            lines,
            [
                "type\torg.apache.kafka.clients.admin.CreateTopicsOptions\tclass\torg.apache.kafka.clients.admin.AbstractOptions",
                "ctor\torg.apache.kafka.clients.admin.CreateTopicsOptions\tCreateTopicsOptions()",
                "type\torg.apache.kafka.clients.consumer.ConsumerRecords\tclass\tIterable",
                "ctor\torg.apache.kafka.clients.consumer.ConsumerRecords\tConsumerRecords()",
            ]
        );
    }

    #[test]
    fn marks_abstract_classes() {
        let src = r#"
            package org.apache.kafka.common.errors;
            public abstract class RetriableException extends RuntimeException {
                public RetriableException(String message) { }
            }
            public class TimeoutException extends RetriableException { }
        "#;
        let index = index(&[(&["common", "errors"], src, false)]);
        let lines = body(
            &index,
            &public(&[
                "org.apache.kafka.common.errors.RetriableException",
                "org.apache.kafka.common.errors.TimeoutException",
            ]),
        );
        assert!(
            lines.contains(
                &"type\torg.apache.kafka.common.errors.RetriableException\tabstract class\tRuntimeException"
                    .to_string()
            ),
            "{lines:?}"
        );
        assert!(
            lines.contains(
                &"type\torg.apache.kafka.common.errors.TimeoutException\tclass\torg.apache.kafka.common.errors.RetriableException"
                    .to_string()
            ),
            "{lines:?}"
        );
    }

    #[test]
    fn lists_jdk_classes_first_with_their_constructors() {
        let src = "package x; public class Node { public int id() { return 0; } }";
        let index = index(&[(&["common"], src, false)]);
        let four: &[JdkCtor] = &[&[], &[("String", "s")], &[("Throwable", "cause")]];
        let two: &[JdkCtor] = &[&[], &[("String", "message")]];
        let jdk = [
            ("java.util.concurrent.TimeoutException", "java.lang.Exception", two),
            ("java.lang.IllegalStateException", "java.lang.RuntimeException", four),
        ];
        let (text, types, members) = render(&index, &public(&["org.apache.kafka.common.Node"]), &jdk);
        let lines: Vec<&str> = text.lines().filter(|l| !l.starts_with('#')).collect();
        assert_eq!(
            lines,
            [
                "type\tjava.lang.IllegalStateException\tclass\tjava.lang.RuntimeException",
                "ctor\tjava.lang.IllegalStateException\tIllegalStateException()",
                "ctor\tjava.lang.IllegalStateException\tIllegalStateException(String s)",
                "ctor\tjava.lang.IllegalStateException\tIllegalStateException(Throwable cause)",
                "type\tjava.util.concurrent.TimeoutException\tclass\tjava.lang.Exception",
                "ctor\tjava.util.concurrent.TimeoutException\tTimeoutException()",
                "ctor\tjava.util.concurrent.TimeoutException\tTimeoutException(String message)",
                "type\torg.apache.kafka.common.Node\tclass\t-",
                "ctor\torg.apache.kafka.common.Node\tNode()",
                "method\torg.apache.kafka.common.Node\tinstance\tid()",
            ]
        );
        assert_eq!((types, members), (3, 7));
    }

    #[test]
    fn lists_only_the_named_test_jar_classes() {
        let mock = "package x; public class MockAdminClient { public MockAdminClient() { } }";
        let other = "package x; public class MockClient { public void poll() { } }";
        let index = index(&[(&["clients", "admin"], mock, true), (&["clients"], other, true)]);
        assert_eq!(
            body(&index, &public(&["org.apache.kafka.clients.MockClient"])),
            [
                "type\torg.apache.kafka.clients.admin.MockAdminClient\tclass\t-",
                "ctor\torg.apache.kafka.clients.admin.MockAdminClient\tMockAdminClient()",
            ]
        );
    }

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parses_a_file_or_check_and_a_file() {
        assert_eq!(parse_args(&args(&["out.txt"])).unwrap(), Mode::Write("out.txt"));
        assert_eq!(parse_args(&args(&["--check", "out.txt"])).unwrap(), Mode::Check("out.txt"));
    }

    #[test]
    fn refuses_any_other_arguments() {
        let usage = "usage: cargo xtask java-api [--check] <file>, \
                     e.g. `cargo xtask java-api ../dotnet/analyzers/java-api.txt`";
        for bad in [
            &[][..],
            &["--check"],
            &["--write", "out.txt"],
            &["a.txt", "b.txt"],
            &["--check", "a", "b"],
        ] {
            assert_eq!(parse_args(&args(bad)).unwrap_err().to_string(), usage, "{bad:?}");
        }
    }

    #[test]
    fn check_passes_on_an_up_to_date_file() {
        let src = "package x; public class Node { public int id() { return 0; } }";
        let index = index(&[(&["common"], src, false)]);
        let (text, _, _) = render(&index, &public(&["org.apache.kafka.common.Node"]), &[]);
        assert!(check("java-api.txt", &text, &text).is_ok());
    }

    #[test]
    fn check_names_the_first_line_that_differs() {
        let expected = "# header\ntype\tx.A\tclass\t-\nmethod\tx.A\tinstance\ta()\n";
        let on_disk = "# header\ntype\tx.A\tclass\t-\nmethod\tx.A\tinstance\told()\n";
        assert_eq!(
            check("java-api.txt", on_disk, expected).unwrap_err().to_string(),
            "java-api.txt is out of date with the Java sources: line 3 should be \
             `method\tx.A\tinstance\ta()` but is `method\tx.A\tinstance\told()`; \
             run `cargo xtask java-api java-api.txt`"
        );
    }

    #[test]
    fn check_reports_a_missing_or_an_extra_line() {
        let expected = "# header\ntype\tx.A\tclass\t-\n";
        assert_eq!(
            check("f.txt", "# header\n", expected).unwrap_err().to_string(),
            "f.txt is out of date with the Java sources: line 2 should be `type\tx.A\tclass\t-` \
             but is `<end of file>`; run `cargo xtask java-api f.txt`"
        );
        assert_eq!(
            check("f.txt", "# header\ntype\tx.A\tclass\t-\ntype\tx.B\tclass\t-\n", expected)
                .unwrap_err()
                .to_string(),
            "f.txt is out of date with the Java sources: line 3 should be `<end of file>` \
             but is `type\tx.B\tclass\t-`; run `cargo xtask java-api f.txt`"
        );
    }

    #[test]
    fn check_reports_a_difference_in_line_endings_only() {
        let expected = "# header\ntype\tx.A\tclass\t-\n";
        for on_disk in ["# header\r\ntype\tx.A\tclass\t-\r\n", "# header\ntype\tx.A\tclass\t-"] {
            assert_eq!(
                check("f.txt", on_disk, expected).unwrap_err().to_string(),
                "f.txt is out of date with the Java sources: it differs only in its line endings or its final \
                 newline; run `cargo xtask java-api f.txt`",
                "{on_disk:?}"
            );
        }
    }
}
