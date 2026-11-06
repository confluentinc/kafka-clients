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

//! Generate the C# exception hierarchy for the .NET binding.
//!
//! Renders the class graph [`build_graph`](super::build_graph) builds into one C# class per Java
//! exception class (`dotnet/CLAUDE.md` §2, Errors), in the namespace and folder of its Java
//! package, plus the id registry `Internal/ErrorClasses.g.cs` and the layout test's rows. Unlike
//! the Python renderer it does not flatten a constructor's chain: each Java constructor becomes
//! one C# constructor with the same `this(...)` / `super(...)` call and the same field
//! assignments, so the expressions are translated one constructor at a time.
//!
//! Generation stops on a Java construct or type with no C# form, a package with no folder rule,
//! and two constructors with one C# signature.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use super::{
    base_type, compatible, flatten_add, split_plus, throwable_like, ClassInfo, Graph, PyVal, GENERATED_MARKER,
    KAFKA_EXCEPTION_FQN, TERMINAL_BASES,
};
use crate::java_parse::{CallKind, Ctor, Expr, GetterBody, Visibility};

/// The library project, relative to the repository root.
pub(super) const CS_SRC: &str = "dotnet/src/Confluent.Kafka";

/// The unit-test project, which receives the layout test's rows.
pub(super) const CS_TESTS: &str = "dotnet/tests/Confluent.Kafka.UnitTests";

/// Classes the binding writes by hand (`dotnet/CLAUDE.md` §2): `KafkaException` holds the core
/// path's reader, and `RecordDeserializationException` has a .NET-specific shape.
pub(super) const CSHARP_HAND_WRITTEN: &[&str] = &[
    KAFKA_EXCEPTION_FQN,
    "org.apache.kafka.common.errors.RecordDeserializationException",
];

/// The .NET base of each Java built-in exception: the built-in the binding maps the class to,
/// so `catch` blocks for it keep working.
const CSHARP_JDK_BASES: &[(&str, &str)] = &[
    ("java.lang.IllegalStateException", "System.InvalidOperationException"),
    ("java.lang.IllegalArgumentException", "System.ArgumentException"),
    ("java.util.ConcurrentModificationException", "System.InvalidOperationException"),
    ("java.util.concurrent.TimeoutException", "System.TimeoutException"),
    ("java.lang.UnsupportedOperationException", "System.NotSupportedException"),
    ("java.util.concurrent.ExecutionException", "System.Exception"),
    ("java.util.NoSuchElementException", "System.InvalidOperationException"),
    // `System.IndexOutOfRangeException` is sealed.
    ("java.lang.IndexOutOfBoundsException", "System.ArgumentOutOfRangeException"),
];

/// The names a getter method, which takes no arguments, cannot take. A getter would clash by name
/// with a property or an event, and would hide a parameterless method, of any class it can inherit
/// from: `object`, `System.Exception`, `KafkaException` (whose members are internal, but visible to
/// the generated classes in its assembly), and the .NET built-ins of `CSHARP_JDK_BASES`. Of those,
/// `System.ArgumentException` adds `ParamName`, and `System.ArgumentOutOfRangeException` adds
/// `ParamName` and `ActualValue`; those are reserved under every root, as no Java 4.4 getter has
/// either name.
const RESERVED_MEMBERS: &[&str] = &[
    "Message",
    "Data",
    "Source",
    "HResult",
    "HelpLink",
    "StackTrace",
    "TargetSite",
    "InnerException",
    "SerializeObjectState",
    "ParamName",
    "ActualValue",
    "Code",
    "FfiId",
    "GetBaseException",
    "GetType",
    "GetHashCode",
    "ToString",
    "MemberwiseClone",
    "Finalize",
    "TypeAndMessage",
];

/// C# keywords that are not Java keywords, so a Java parameter may carry one.
const CS_ONLY_KEYWORDS: &[&str] = &[
    "as",
    "base",
    "bool",
    "checked",
    "decimal",
    "delegate",
    "event",
    "explicit",
    "extern",
    "fixed",
    "foreach",
    "implicit",
    "in",
    "internal",
    "is",
    "lock",
    "namespace",
    "object",
    "operator",
    "out",
    "override",
    "params",
    "readonly",
    "ref",
    "sbyte",
    "sealed",
    "sizeof",
    "stackalloc",
    "string",
    "struct",
    "typeof",
    "uint",
    "ulong",
    "unchecked",
    "unsafe",
    "ushort",
    "using",
    "virtual",
];

const NS_SYSTEM: &str = "System";
const NS_SCG: &str = "System.Collections.Generic";
const NS_INTERNAL: &str = "Confluent.Kafka.Internal";
const NS_INTEROP: &str = "Confluent.Kafka.Internal.Interop";
const NS_COMMON: &str = "Confluent.Kafka.Common";
const NS_CONSUMER: &str = "Confluent.Kafka.Consumer";
const NS_METRICS: &str = "Confluent.Kafka.Common.Metrics";

const CS_HEADER: &str = "// Copyright 2026 Confluent Inc.
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

// GENERATED, DO NOT EDIT. Produced from the Java source by `cargo xtask generate-error-codes`
// and checked for staleness by `cargo xtask check-generated`.
";

// ---------------------------------------------------------------------------
// Names and types
// ---------------------------------------------------------------------------

fn pascal(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        None => String::new(),
    }
}

/// The namespace of a Java package (`dotnet/CLAUDE.md` §2): `org.apache.kafka.` and `clients.`
/// dropped, each segment PascalCased, under `Confluent.Kafka`; a JDK package is the root
/// `Confluent.Kafka` itself, as Python keeps its JDK classes at `confluent_kafka`.
fn cs_namespace(package: &str) -> anyhow::Result<String> {
    let rest = if let Some(r) = package.strip_prefix("org.apache.kafka.") {
        r.strip_prefix("clients.").unwrap_or(r)
    } else if package.starts_with("java.") {
        return Ok("Confluent.Kafka".to_string());
    } else {
        anyhow::bail!("error hierarchy (C#): no folder rule for the Java package `{package}`");
    };
    let segments: Vec<String> = rest.split('.').map(pascal).collect();
    Ok(format!("Confluent.Kafka.{}", segments.join(".")))
}

fn simple_name(info: &ClassInfo) -> &str {
    &info.java.simple
}

fn cs_fqn(info: &ClassInfo) -> anyhow::Result<String> {
    Ok(format!("{}.{}", cs_namespace(&info.java.package)?, simple_name(info)))
}

fn cs_path(repo_root: &Path, info: &ClassInfo) -> anyhow::Result<PathBuf> {
    let ns = cs_namespace(&info.java.package)?;
    let mut p = repo_root.join(CS_SRC);
    for seg in ns.split('.').skip(2) {
        p.push(seg);
    }
    p.push(format!("{}.cs", simple_name(info)));
    Ok(p)
}

/// A Java type's C# form.
#[derive(Clone, Debug, PartialEq)]
struct CsTy {
    /// The type without a nullable mark.
    text: String,
    reference: bool,
    /// A Java `String`, `Object` or `Throwable`, which C# always types nullable.
    always_nullable: bool,
}

impl CsTy {
    fn decl(&self, nullable: bool) -> String {
        if self.reference && (self.always_nullable || nullable) {
            format!("{}?", self.text)
        } else {
            self.text.clone()
        }
    }

    /// The element type of an `IReadOnlyCollection<E>`.
    fn element(&self) -> Option<&str> {
        self.text.strip_prefix("IReadOnlyCollection<")?.strip_suffix('>')
    }

    /// The key and value types of an `IReadOnlyDictionary<K, V>`.
    fn key_value(&self) -> Option<(&str, &str)> {
        self.text
            .strip_prefix("IReadOnlyDictionary<")?
            .strip_suffix('>')?
            .split_once(", ")
    }
}

/// The C# type of a Java type (the plan's D3 table); every other type stops generation.
fn cs_type(java: &str, usings: &mut BTreeSet<&'static str>) -> anyhow::Result<CsTy> {
    let t: String = java.chars().filter(|c| !c.is_whitespace()).collect();
    let (text, reference, always_nullable, ns): (&str, bool, bool, &[&'static str]) = match t.as_str() {
        "String" => ("string", true, true, &[]),
        "Object" => ("object", true, true, &[]),
        "int" => ("int", false, false, &[]),
        "long" => ("long", false, false, &[]),
        "double" => ("double", false, false, &[]),
        "boolean" => ("bool", false, false, &[]),
        "TopicPartition" => ("TopicPartition", false, false, &[NS_COMMON]),
        "Set<String>" => ("IReadOnlyCollection<string>", true, false, &[NS_SCG]),
        "Set<TopicPartition>" | "Collection<TopicPartition>" => {
            ("IReadOnlyCollection<TopicPartition>", true, false, &[NS_SCG, NS_COMMON])
        },
        "Map<TopicPartition,Long>" => ("IReadOnlyDictionary<TopicPartition, long>", true, false, &[NS_SCG, NS_COMMON]),
        "Map<TopicPartition,OffsetAndMetadata>" => (
            "IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>",
            true,
            false,
            &[NS_SCG, NS_COMMON, NS_CONSUMER],
        ),
        "KafkaMetric" => ("KafkaMetric", true, false, &[NS_METRICS]),
        other if throwable_like(other) => ("Exception", true, true, &[NS_SYSTEM]),
        other => anyhow::bail!("error hierarchy (C#): no C# type for the Java type `{other}`"),
    };
    usings.extend(ns);
    Ok(CsTy { text: text.to_string(), reference, always_nullable })
}

/// Whether a C# type is a value type, so a copy of a collection of it has no null element to
/// check.
fn is_value_type(cs: &str) -> bool {
    matches!(cs, "int" | "long" | "double" | "bool" | "TopicPartition")
}

fn cs_literal(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

/// Javadoc plain text as XML doc text: escaped, with its ``x`` and `<code>x</code>` marks as
/// `<c>x</c>`.
fn doc_text(s: &str) -> String {
    let escaped = xml_escape(&s.replace("<code>", "``").replace("</code>", "``"));
    let mut out = String::new();
    let mut rest = escaped.as_str();
    while let Some(start) = rest.find("``") {
        let Some(len) = rest[start + 2..].find("``") else { break };
        out.push_str(&rest[..start]);
        out.push_str("<c>");
        out.push_str(&rest[start + 2..start + 2 + len]);
        out.push_str("</c>");
        rest = &rest[start + 2 + len + 2..];
    }
    out.push_str(rest);
    out
}

/// `/// <summary>…</summary>` lines, wrapped under 120 columns.
fn summary(text: &str, indent: &str) -> Vec<String> {
    let one = format!("{indent}/// <summary>{text}</summary>");
    if one.len() <= 120 {
        return vec![one];
    }
    let mut out = vec![format!("{indent}/// <summary>")];
    out.extend(wrap(text, indent));
    out.push(format!("{indent}/// </summary>"));
    out
}

/// `/// ` lines holding `text`, wrapped under 120 columns.
fn wrap(text: &str, indent: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut line = String::new();
    for word in text.split_whitespace() {
        if !line.is_empty() && indent.len() + 4 + line.len() + 1 + word.len() > 120 {
            out.push(format!("{indent}/// {line}"));
            line.clear();
        }
        if !line.is_empty() {
            line.push(' ');
        }
        line.push_str(word);
    }
    if !line.is_empty() {
        out.push(format!("{indent}/// {line}"));
    }
    out
}

/// A Java constructor or factory signature, for a doc: `Name(String message, Throwable cause)`.
fn java_signature(name: &str, params: &[crate::java_parse::Param]) -> String {
    let ps: Vec<String> = params.iter().map(|p| format!("{} {}", p.ty, p.name)).collect();
    format!("{name}({})", ps.join(", "))
}

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

/// A constructor or factory parameter, with its C# name and type.
#[derive(Clone, Debug)]
struct CsParam {
    java_name: String,
    java_ty: String,
    name: String,
    ty: CsTy,
    nullable: bool,
}

impl CsParam {
    fn decl(&self) -> String {
        format!("{} {}", self.ty.decl(self.nullable), self.name)
    }
}

struct Scope<'a> {
    params: &'a [CsParam],
}

impl Scope<'_> {
    fn param(&self, name: &[String]) -> anyhow::Result<&CsParam> {
        let [n] = name else {
            anyhow::bail!("error hierarchy (C#): unsupported name `{}`", name.join("."));
        };
        self.params
            .iter()
            .find(|p| p.java_name == *n)
            .ok_or_else(|| anyhow::anyhow!("error hierarchy (C#): unknown name `{n}`"))
    }
}

/// The Java type of an argument expression, for overload resolution (`None` for `null`).
fn java_ty_of(e: &Expr, scope: &Scope) -> anyhow::Result<Option<String>> {
    Ok(match e {
        Expr::Null => None,
        Expr::Str(_) | Expr::Add(..) => Some("String".into()),
        Expr::Int(_) => Some("int".into()),
        Expr::Bool(_) | Expr::Eq(..) => Some("boolean".into()),
        Expr::Name(n) => Some(scope.param(n)?.java_ty.clone()),
        Expr::Ternary(_, a, b) => match java_ty_of(a, scope)? {
            Some(t) => Some(t),
            None => java_ty_of(b, scope)?,
        },
        Expr::Call(name, _) => {
            let n: Vec<&str> = name.iter().map(String::as_str).collect();
            Some(
                match n.as_slice() {
                    ["Collections", "emptySet" | "singleton" | "unmodifiableSet"] | ["Set", "of" | "copyOf"] => "Set",
                    ["Collections", "emptyMap" | "unmodifiableMap"] | ["Map", "copyOf"] => "Map",
                    ["Collections", "emptyList"] => "List",
                    _ => anyhow::bail!("error hierarchy (C#): unsupported call `{}`", name.join(".")),
                }
                .to_string(),
            )
        },
        Expr::New(ty, _) => Some(match ty.as_str() {
            "HashSet" | "LinkedHashSet" => "Set".to_string(),
            "HashMap" => "Map".to_string(),
            other => other.to_string(),
        }),
    })
}

/// Whether an expression may be `null`, for the nullability of what it is assigned to.
fn may_be_null(e: &Expr, scope: &Scope) -> bool {
    match e {
        Expr::Null => true,
        Expr::New(ty, _) => ty == "InterruptedException",
        Expr::Name(n) => scope
            .param(n)
            .is_ok_and(|p| p.ty.reference && (p.ty.always_nullable || p.nullable)),
        Expr::Ternary(_, a, b) => may_be_null(a, scope) || may_be_null(b, scope),
        _ => false,
    }
}

/// An expression as C#, `target` being the C# type it is assigned or passed to.
fn cs_expr(
    e: &Expr,
    scope: &Scope,
    target: Option<&CsTy>,
    usings: &mut BTreeSet<&'static str>,
) -> anyhow::Result<String> {
    let need = |t: Option<&CsTy>, what: &str| -> anyhow::Result<CsTy> {
        t.cloned()
            .ok_or_else(|| anyhow::anyhow!("error hierarchy (C#): `{what}` with no target type"))
    };
    Ok(match e {
        Expr::Null => "null".into(),
        Expr::Bool(b) => b.to_string(),
        Expr::Int(n) => n.to_string(),
        Expr::Str(s) => cs_literal(s),
        Expr::Name(n) => scope.param(n)?.name.clone(),
        Expr::Add(..) => {
            let mut parts = Vec::new();
            flatten_add(e, &mut parts);
            if !parts.iter().any(|p| matches!(p, Expr::Str(_))) {
                anyhow::bail!("error hierarchy (C#): unsupported arithmetic `+`");
            }
            let mut out: Vec<String> = Vec::new();
            let mut literal: Option<String> = None;
            for p in parts {
                if let Expr::Str(s) = p {
                    literal.get_or_insert_with(String::new).push_str(s);
                    continue;
                }
                if let Some(l) = literal.take() {
                    out.push(cs_literal(&l));
                }
                let string_ternary =
                    matches!(p, Expr::Ternary(..)) && java_ty_of(p, scope)?.as_deref() == Some("String");
                let text = cs_expr(p, scope, None, usings)?;
                if string_ternary {
                    out.push(text);
                } else {
                    usings.insert(NS_INTERNAL);
                    out.push(format!("JavaText.ValueOf({text})"));
                }
            }
            if let Some(l) = literal.take() {
                out.push(cs_literal(&l));
            }
            out.join(" + ")
        },
        Expr::Eq(a, b) => {
            let (Expr::Name(_), Expr::Null) = (a.as_ref(), b.as_ref()) else {
                anyhow::bail!("error hierarchy (C#): unsupported `==`");
            };
            format!("{} is null", cs_expr(a, scope, None, usings)?)
        },
        Expr::Ternary(c, a, b) => format!(
            "({} ? {} : {})",
            cs_expr(c, scope, None, usings)?,
            cs_expr(a, scope, target, usings)?,
            cs_expr(b, scope, target, usings)?
        ),
        Expr::Call(name, args) => {
            let n: Vec<&str> = name.iter().map(String::as_str).collect();
            match (n.as_slice(), args.as_slice()) {
                (["Collections", "emptySet"] | ["Set", "of"], []) => {
                    let t = need(target, "emptySet")?;
                    let elem = t
                        .element()
                        .ok_or_else(|| anyhow::anyhow!("error hierarchy (C#): a set for `{}`", t.text))?;
                    usings.insert(NS_SYSTEM);
                    format!("Array.Empty<{elem}>()")
                },
                (["Collections", "singleton"], [x]) => format!("new[] {{ {} }}", cs_expr(x, scope, None, usings)?),
                (["Set", "copyOf"], [x @ Expr::Name(_)]) => {
                    let t = need(target, "Set.copyOf")?;
                    let elem = t
                        .element()
                        .ok_or_else(|| anyhow::anyhow!("error hierarchy (C#): a set for `{}`", t.text))?;
                    if !is_value_type(elem) {
                        anyhow::bail!("error hierarchy (C#): `Set.copyOf` of `{elem}` would need a null-element check");
                    }
                    let x = cs_expr(x, scope, None, usings)?;
                    usings.insert(NS_SYSTEM);
                    usings.insert(NS_SCG);
                    format!("new HashSet<{elem}>({x} ?? throw new ArgumentNullException(nameof({x})))")
                },
                (["Collections", "unmodifiableSet" | "unmodifiableMap"], [x @ Expr::Name(_)]) => {
                    let x = cs_expr(x, scope, None, usings)?;
                    usings.insert(NS_SYSTEM);
                    format!("{x} ?? throw new ArgumentNullException(nameof({x}))")
                },
                (["Collections", "emptyMap"], []) => empty_map(need(target, "emptyMap")?, usings)?,
                _ => anyhow::bail!("error hierarchy (C#): unsupported call `{}`", name.join(".")),
            }
        },
        Expr::New(ty, args) => match (ty.as_str(), args.as_slice()) {
            ("HashSet" | "LinkedHashSet", []) => {
                let t = need(target, "new HashSet")?;
                let elem = t
                    .element()
                    .ok_or_else(|| anyhow::anyhow!("error hierarchy (C#): a set for `{}`", t.text))?;
                usings.insert(NS_SYSTEM);
                format!("Array.Empty<{elem}>()")
            },
            ("HashMap", []) => empty_map(need(target, "new HashMap")?, usings)?,
            // .NET has no `InterruptedException`: the cause is left empty, as Python leaves it.
            ("InterruptedException", []) => "null".into(),
            _ => anyhow::bail!("error hierarchy (C#): unsupported `new {ty}(...)`"),
        },
    })
}

fn empty_map(t: CsTy, usings: &mut BTreeSet<&'static str>) -> anyhow::Result<String> {
    let (k, v) = t
        .key_value()
        .ok_or_else(|| anyhow::anyhow!("error hierarchy (C#): a map for `{}`", t.text))?;
    usings.insert(NS_INTERNAL);
    Ok(format!("EmptyReadOnlyDictionary<{k}, {v}>.Instance"))
}

// ---------------------------------------------------------------------------
// The class model
// ---------------------------------------------------------------------------

/// One getter method: a Java getter, with Java's name.
#[derive(Debug)]
struct CsGetter {
    java_getter: String,
    name: String,
    ty: String,
    modifier: &'static str,
    /// `None` for an abstract method; else the expression body.
    body: Option<String>,
}

/// What a Java instance field becomes.
#[derive(Debug)]
struct CsField {
    java_name: String,
    /// The private backing field a constructor assigns (`private readonly T _x;`).
    member: String,
    ty: CsTy,
    nullable: bool,
}

/// The chain of `fqn` and its ancestors in the graph, `fqn` first.
fn chain<'g>(g: &'g Graph, fqn: &str) -> Vec<&'g ClassInfo> {
    let mut out = Vec::new();
    let mut cur = fqn.to_string();
    while let Some(info) = g.classes.get(&cur) {
        out.push(info);
        cur = info.parent_fqn.clone();
    }
    out
}

fn is_ancestor_or_self(g: &Graph, ancestor: &str, fqn: &str) -> bool {
    chain(g, fqn).iter().any(|c| c.java_fqn == ancestor)
}

/// The C# parameters of a Java constructor of `fqn`.
fn ctor_params(
    g: &Graph,
    fqn: &str,
    idx: usize,
    null_fed: &BTreeSet<(usize, usize)>,
    usings: &mut BTreeSet<&'static str>,
) -> anyhow::Result<Vec<CsParam>> {
    let ctor = &g.class(fqn)?.java.ctors[idx];
    let message = message_param(g, fqn, idx, 0)?;
    let mut out: Vec<CsParam> = Vec::new();
    for (i, p) in ctor.params.iter().enumerate() {
        let ty = cs_type(&p.ty, usings)?;
        let name = if Some(i) == message {
            "message".to_string()
        } else if throwable_like(&p.ty) {
            "innerException".to_string()
        } else {
            p.name.clone()
        };
        if CS_ONLY_KEYWORDS.contains(&name.as_str()) {
            anyhow::bail!("error hierarchy (C#): {fqn}: the parameter `{name}` is a C# keyword");
        }
        if out.iter().any(|o| o.name == name) {
            anyhow::bail!("error hierarchy (C#): {fqn}: two parameters named `{name}` in one constructor");
        }
        out.push(CsParam {
            java_name: p.name.clone(),
            java_ty: p.ty.clone(),
            name,
            ty,
            nullable: null_fed.contains(&(idx, i)),
        });
    }
    Ok(out)
}

/// The index of the parameter of constructor `idx` of `fqn` that reaches `Throwable` unchanged as
/// its message, through the `this(...)` / `super(...)` chain.
fn message_param(g: &Graph, fqn: &str, idx: usize, depth: usize) -> anyhow::Result<Option<usize>> {
    if depth > 16 {
        anyhow::bail!("error hierarchy (C#): {fqn}: constructor chain too deep");
    }
    let info = g.class(fqn)?;
    let ctor = &info.java.ctors[idx];
    let Some((kind, args)) = &ctor.call else {
        return Ok(None);
    };
    let scope_params: Vec<CsParam> = ctor
        .params
        .iter()
        .map(|p| CsParam {
            java_name: p.name.clone(),
            java_ty: p.ty.clone(),
            name: p.name.clone(),
            ty: CsTy { text: String::new(), reference: true, always_nullable: true },
            nullable: true,
        })
        .collect();
    let scope = Scope { params: &scope_params };
    let target = match kind {
        CallKind::This => fqn.to_string(),
        CallKind::Super => info.parent_fqn.clone(),
    };
    let pos = if TERMINAL_BASES.contains(&target.as_str()) {
        match args.first() {
            Some(a) if java_ty_of(a, &scope)?.as_deref() == Some("String") => Some(0),
            _ => None,
        }
    } else {
        let vals = arg_vals(args, &scope)?;
        let ti = g.resolve(&target, &vals, *kind == CallKind::Super)?;
        message_param(g, &target, ti, depth + 1)?
    };
    Ok(pos.and_then(|p| match &args[p] {
        Expr::Name(n) if n.len() == 1 => {
            ctor.params.iter().position(|q| q.name == n[0] && base_type(&q.ty) == "String")
        },
        _ => None,
    }))
}

fn arg_vals(args: &[Expr], scope: &Scope) -> anyhow::Result<Vec<PyVal>> {
    args.iter()
        .map(|a| Ok(PyVal { expr: String::new(), ty: java_ty_of(a, scope)?, konst: None }))
        .collect()
}

/// The arguments of a `this(...)` / `super(...)` / `new X(...)` call as C#, a `null` cast to its
/// parameter's type where two constructors of the target could take it.
fn call_args(
    g: &Graph,
    target: &str,
    args: &[Expr],
    scope: &Scope,
    from_subclass: bool,
    usings: &mut BTreeSet<&'static str>,
) -> anyhow::Result<Vec<String>> {
    let vals = arg_vals(args, scope)?;
    let ti = g.resolve(target, &vals, from_subclass)?;
    let tinfo = g.class(target)?;
    let tctor = &tinfo.java.ctors[ti];
    let applicable = tinfo
        .java
        .ctors
        .iter()
        .filter(|c| !(from_subclass && c.visibility == Visibility::Private))
        .filter(|c| c.params.len() == vals.len())
        .filter(|c| c.params.iter().zip(&vals).all(|(p, v)| compatible(&v.ty, &p.ty)))
        .count();
    let mut out = Vec::new();
    for (a, p) in args.iter().zip(&tctor.params) {
        let ty = cs_type(&p.ty, usings)?;
        if matches!(a, Expr::Null) && applicable > 1 {
            out.push(format!("({})null", ty.decl(true)));
        } else {
            out.push(cs_expr(a, scope, Some(&ty), usings)?);
        }
    }
    Ok(out)
}

/// The `(constructor, parameter)` positions some call in the class passes a `null` literal to.
fn null_fed_params(g: &Graph, info: &ClassInfo) -> anyhow::Result<BTreeSet<(usize, usize)>> {
    let fqn = &info.java_fqn;
    let mut out = BTreeSet::new();
    let mut mark = |args: &[Expr], scope: &Scope| -> anyhow::Result<()> {
        let vals = arg_vals(args, scope)?;
        let ti = g.resolve(fqn, &vals, false)?;
        for (i, a) in args.iter().enumerate() {
            if matches!(a, Expr::Null) {
                out.insert((ti, i));
            }
        }
        Ok(())
    };
    let empty: Vec<CsParam> = Vec::new();
    for c in &info.java.ctors {
        if let Some((CallKind::This, args)) = &c.call {
            let ps = loose_params(&c.params);
            mark(args, &Scope { params: &ps })?;
        }
    }
    for f in &info.java.factories {
        let ps = loose_params(&f.params);
        mark(&f.args, &Scope { params: &ps })?;
    }
    for s in &info.java.singletons {
        mark(&s.args, &Scope { params: &empty })?;
    }
    Ok(out)
}

/// Parameters typed for resolution only.
fn loose_params(params: &[crate::java_parse::Param]) -> Vec<CsParam> {
    params
        .iter()
        .map(|p| CsParam {
            java_name: p.name.clone(),
            java_ty: p.ty.clone(),
            name: p.name.clone(),
            ty: CsTy { text: String::new(), reference: true, always_nullable: true },
            nullable: true,
        })
        .collect()
}

/// The constructors C# keeps: every one but a deprecated one.
fn kept_ctors(info: &ClassInfo) -> anyhow::Result<Vec<usize>> {
    let mut out = Vec::new();
    for (i, c) in info.java.ctors.iter().enumerate() {
        if c.deprecated.is_some() {
            continue;
        }
        if c.visibility == Visibility::Package {
            anyhow::bail!(
                "error hierarchy (C#): {}: a package-private constructor has no C# form",
                info.java_fqn
            );
        }
        out.push(i);
    }
    Ok(out)
}

/// The instance fields of a class, with their C# members.
fn class_fields(
    g: &Graph,
    info: &ClassInfo,
    null_fed: &BTreeSet<(usize, usize)>,
    usings: &mut BTreeSet<&'static str>,
) -> anyhow::Result<Vec<CsField>> {
    let mut out = Vec::new();
    for f in info.java.fields.iter().filter(|f| !f.is_static) {
        if let Some(init) = &f.init {
            if !matches!(init, Expr::Null | Expr::Int(0) | Expr::Bool(false)) {
                anyhow::bail!(
                    "error hierarchy (C#): {}: the field `{}` has an initializer that is not its default",
                    info.java_fqn,
                    f.name
                );
            }
        }
        if !info
            .java
            .getters
            .iter()
            .any(|gt| matches!(&gt.body, GetterBody::Field(x) if *x == f.name))
        {
            anyhow::bail!("error hierarchy (C#): {}: the field `{}` has no getter", info.java_fqn, f.name);
        }
        let ty = cs_type(&f.ty, usings)?;
        let mut nullable = matches!(f.init, Some(Expr::Null));
        for idx in kept_ctors(info)? {
            let c = &info.java.ctors[idx];
            if matches!(c.call, Some((CallKind::This, _))) {
                continue;
            }
            let ps = ctor_params(g, &info.java_fqn, idx, null_fed, usings)?;
            let scope = Scope { params: &ps };
            match c.assigns.iter().find(|(n, _)| *n == f.name) {
                None => nullable = true,
                Some((_, e)) => nullable |= may_be_null(e, &scope),
            }
        }
        out.push(CsField {
            java_name: f.name.clone(),
            member: format!("_{}", f.name),
            ty,
            nullable: nullable && f.init != Some(Expr::Int(0)),
        });
    }
    Ok(out)
}

/// Every instance field of the chain of `fqn`, ancestors first, as core-path parameters.
fn chain_fields(g: &Graph, fqn: &str, usings: &mut BTreeSet<&'static str>) -> anyhow::Result<Vec<(String, String)>> {
    let mut out = Vec::new();
    for info in chain(g, fqn).iter().rev() {
        if CSHARP_HAND_WRITTEN.contains(&info.java_fqn.as_str()) {
            continue;
        }
        let null_fed = null_fed_params(g, info)?;
        for f in class_fields(g, info, &null_fed, usings)? {
            out.push((f.java_name.clone(), f.ty.decl(f.nullable)));
        }
    }
    Ok(out)
}

fn getters(
    g: &Graph,
    info: &ClassInfo,
    fields: &[CsField],
    usings: &mut BTreeSet<&'static str>,
) -> anyhow::Result<Vec<CsGetter>> {
    let mut out = Vec::new();
    for gt in &info.java.getters {
        let name = pascal(&gt.name);
        if RESERVED_MEMBERS.contains(&name.as_str()) || name == simple_name(info) {
            anyhow::bail!(
                "error hierarchy (C#): {}: the getter `{}` would hide `{name}`",
                info.java_fqn,
                gt.name
            );
        }
        let in_ancestor = chain(g, &info.parent_fqn)
            .iter()
            .any(|c| c.java.getters.iter().any(|x| x.name == gt.name));
        let in_descendant = g.classes.values().any(|c| {
            c.java_fqn != info.java_fqn
                && is_ancestor_or_self(g, &info.java_fqn, &c.java_fqn)
                && c.java.getters.iter().any(|x| x.name == gt.name)
        });
        let ret = cs_type(&gt.ret, usings)?;
        let (modifier, body, ty) = match &gt.body {
            GetterBody::Abstract => ("abstract", None, ret.decl(false)),
            GetterBody::Field(f) => {
                let field = fields.iter().find(|x| x.java_name == *f).ok_or_else(|| {
                    anyhow::anyhow!(
                        "error hierarchy (C#): {}: the getter `{}` reads no field",
                        info.java_fqn,
                        gt.name
                    )
                })?;
                if field.ty.text != ret.text {
                    anyhow::bail!(
                        "error hierarchy (C#): {}: the getter `{}` returns another type than its field",
                        info.java_fqn,
                        gt.name
                    );
                }
                (
                    override_modifier(in_ancestor, in_descendant),
                    Some(field.member.clone()),
                    field.ty.decl(field.nullable),
                )
            },
            GetterBody::KeySet(f) => {
                let field = fields.iter().find(|x| x.java_name == *f).ok_or_else(|| {
                    anyhow::anyhow!(
                        "error hierarchy (C#): {}: the getter `{}` reads no field",
                        info.java_fqn,
                        gt.name
                    )
                })?;
                let (k, v) = field.ty.key_value().ok_or_else(|| {
                    anyhow::anyhow!("error hierarchy (C#): {}: `keySet()` of a field that is no map", info.java_fqn)
                })?;
                if field.nullable {
                    anyhow::bail!("error hierarchy (C#): {}: `keySet()` of a nullable map", info.java_fqn);
                }
                usings.insert(NS_INTERNAL);
                (
                    override_modifier(in_ancestor, in_descendant),
                    Some(format!("new KeySetView<{k}, {v}>({})", field.member)),
                    ret.decl(false),
                )
            },
        };
        out.push(CsGetter { java_getter: gt.name.clone(), name, ty, modifier, body });
    }
    Ok(out)
}

fn override_modifier(in_ancestor: bool, in_descendant: bool) -> &'static str {
    if in_ancestor {
        "override"
    } else if in_descendant {
        "virtual"
    } else {
        ""
    }
}

/// The text a Java `toString()` override gives after `getClass().getName() + ": "`, as a C#
/// expression over the constructor's assignments: `+` parts that are string literals, fields, or
/// `metricName()` on a `KafkaMetric` field.
fn to_string_message(
    info: &ClassInfo,
    to_string: &str,
    ctor: &Ctor,
    scope: &Scope,
    usings: &mut BTreeSet<&'static str>,
) -> anyhow::Result<String> {
    let fqn = &info.java_fqn;
    let parts: Vec<String> = split_plus(to_string).into_iter().map(|p| p.trim().to_string()).collect();
    if parts.first().map(String::as_str) != Some("getClass().getName()") {
        anyhow::bail!("error hierarchy (C#): {fqn}: `toString()` does not start with the class name");
    }
    let mut out: Vec<String> = Vec::new();
    for (i, part) in parts.iter().enumerate().skip(1) {
        if let Some(lit) = part.strip_prefix('"').and_then(|p| p.strip_suffix('"')) {
            if lit.contains('\\') {
                anyhow::bail!("error hierarchy (C#): {fqn}: an escape in `toString()`");
            }
            let lit = if i == 1 {
                lit.strip_prefix(": ")
                    .ok_or_else(|| anyhow::anyhow!("error hierarchy (C#): {fqn}: `toString()` lacks \": \""))?
            } else {
                lit
            };
            if !lit.is_empty() {
                out.push(cs_literal(lit));
            }
            continue;
        }
        let (field, method) = match part.split_once('.') {
            Some((f, m)) => (f, Some(m)),
            None => (part.as_str(), None),
        };
        let field = field.strip_prefix("this.").unwrap_or(field);
        let (_, assigned) = ctor.assigns.iter().find(|(n, _)| n == field).ok_or_else(|| {
            anyhow::anyhow!("error hierarchy (C#): {fqn}: `toString()` reads an unassigned `{field}`")
        })?;
        let Expr::Name(n) = assigned else {
            anyhow::bail!("error hierarchy (C#): {fqn}: `toString()` reads a computed field `{field}`");
        };
        let p = scope.param(n)?;
        let text = match method {
            None => p.name.clone(),
            Some("metricName()") if base_type(&p.java_ty) == "KafkaMetric" => {
                usings.insert(NS_SYSTEM);
                format!(
                    "({} ?? throw new ArgumentNullException(nameof({}))).MetricName()",
                    p.name, p.name
                )
            },
            Some(m) => anyhow::bail!("error hierarchy (C#): {fqn}: unsupported call `{m}` in `toString()`"),
        };
        usings.insert(NS_INTERNAL);
        out.push(format!("JavaText.ValueOf({text})"));
    }
    Ok(out.join(" + "))
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

fn using_block(usings: &BTreeSet<&'static str>, own_ns: &str) -> Vec<String> {
    let kept: Vec<&str> = usings
        .iter()
        .copied()
        .filter(|u| *u != own_ns && !own_ns.starts_with(&format!("{u}.")))
        .collect();
    let mut groups: BTreeMap<(bool, &str), Vec<&str>> = BTreeMap::new();
    for u in kept {
        let first = u.split('.').next().unwrap_or(u);
        groups.entry((first != "System", first)).or_default().push(u);
    }
    let mut out = Vec::new();
    for members in groups.values() {
        let mut members = members.clone();
        members.sort_unstable();
        for u in members {
            out.push(format!("using {u};"));
        }
        out.push(String::new());
    }
    out
}

fn file_text(usings: &BTreeSet<&'static str>, ns: &str, body: Vec<String>) -> String {
    file_text_with(usings, ns, body, false)
}

/// A file's text; a `.g.cs` file, which the compiler treats as generated, enables nullable annotations
/// itself.
fn file_text_with(usings: &BTreeSet<&'static str>, ns: &str, body: Vec<String>, g_cs: bool) -> String {
    let mut lines: Vec<String> = vec![CS_HEADER.trim_end().to_string(), String::new()];
    if g_cs {
        lines.push("#nullable enable".into());
        lines.push(String::new());
    }
    lines.extend(using_block(usings, ns));
    lines.push(format!("namespace {ns};"));
    lines.push(String::new());
    lines.extend(body);
    lines.join("\n") + "\n"
}

/// The class summary: the first paragraph of the Javadoc, the `<ul>` list that follows it, and
/// the Java class.
fn class_summary(info: &ClassInfo) -> Vec<String> {
    let mut paragraphs = info.java.javadoc.split("\n\n").filter(|p| !p.trim().is_empty());
    let first = paragraphs
        .next()
        .map(|p| doc_text(&p.split_whitespace().collect::<Vec<_>>().join(" ")));
    // The list's items, in the plain text, are the paragraph after the first.
    let list = match (info.java.javadoc_list.first(), paragraphs.next()) {
        (Some(item), Some(next)) if next.starts_with(item.as_str()) => &info.java.javadoc_list[..],
        _ => &[],
    };
    let java = if info.is_jdk {
        format!("Java's built-in <c>{}</c>.", info.java_fqn)
    } else {
        format!("Java's <c>{}</c>.", info.java_fqn)
    };
    if list.is_empty() {
        let mut text = String::new();
        if let Some(first) = first {
            text.push_str(&first);
            if !text.ends_with('.') {
                text.push('.');
            }
            text.push(' ');
        }
        text.push_str(&java);
        return summary(&text, "");
    }
    let mut out = vec!["/// <summary>".to_string()];
    out.extend(wrap(first.as_deref().unwrap_or_default(), ""));
    out.push("/// <list type=\"bullet\">".into());
    for item in list {
        out.extend(wrap(&format!("<item><description>{}</description></item>", doc_text(item)), ""));
    }
    out.push("/// </list>".into());
    out.push(format!("/// {java}"));
    out.push("/// </summary>".into());
    out
}

fn block(lines: &mut Vec<String>, head: Vec<String>, init: Option<String>, body: &[String]) {
    lines.extend(head);
    if let Some(i) = init {
        lines.push(format!("        : {i}"));
    }
    lines.push("    {".into());
    for b in body {
        lines.push(format!("        {b}"));
    }
    lines.push("    }".into());
    lines.push(String::new());
}

/// One rendered class: its path and text.
pub(super) fn render_class(g: &Graph, info: &ClassInfo, repo_root: &Path) -> anyhow::Result<(PathBuf, String)> {
    let path = cs_path(repo_root, info)?;
    let ns = cs_namespace(&info.java.package)?;
    let mut usings: BTreeSet<&'static str> = BTreeSet::new();
    let body = if info.is_jdk {
        render_jdk(g, info, &mut usings)?
    } else {
        render_kafka(g, info, &mut usings)?
    };
    Ok((path, file_text(&usings, &ns, body)))
}

fn base_of(g: &Graph, info: &ClassInfo) -> anyhow::Result<String> {
    if let Some((_, b)) = CSHARP_JDK_BASES.iter().find(|(f, _)| *f == info.java_fqn) {
        return Ok(format!("global::{b}"));
    }
    if TERMINAL_BASES.contains(&info.parent_fqn.as_str()) {
        anyhow::bail!("error hierarchy (C#): {}: no .NET base for a JDK parent", info.java_fqn);
    }
    Ok(format!("global::{}", cs_fqn(g.class(&info.parent_fqn)?)?))
}

fn render_jdk(g: &Graph, info: &ClassInfo, usings: &mut BTreeSet<&'static str>) -> anyhow::Result<Vec<String>> {
    let simple = simple_name(info);
    let base = base_of(g, info)?;
    usings.insert(NS_SYSTEM);
    usings.insert(NS_INTERNAL);
    let mut lines = class_summary(info);
    lines.push(format!("public class {simple} : {base}, IExceptionText"));
    lines.push("{".into());
    lines.push("    private readonly string? _givenMessage;".into());
    lines.push(String::new());
    let mut sigs: Vec<String> = Vec::new();
    for c in &info.java.ctors {
        let tys: Vec<&str> = c.params.iter().map(|p| base_type(&p.ty)).collect();
        let (decl, init) = match tys.as_slice() {
            [] => (String::new(), "this(default(CoreError), null, null)".to_string()),
            ["String"] => ("string? message".into(), "this(default(CoreError), message, null)".into()),
            ["String", "Throwable"] => (
                "string? message, Exception? innerException".into(),
                "this(default(CoreError), message, innerException)".into(),
            ),
            ["Throwable"] => (
                "Exception? innerException".into(),
                "this(default(CoreError), innerException is null ? null : JavaText.ToString(innerException), \
                 innerException)"
                    .into(),
            ),
            other => anyhow::bail!("error hierarchy (C#): {}: a JDK constructor {other:?}", info.java_fqn),
        };
        sigs.push(decl.replace('?', ""));
        let mut head = summary(
            &format!(
                "Initializes the exception, as Java's <c>{}</c>.",
                xml_escape(&java_signature(simple, &c.params))
            ),
            "    ",
        );
        head.push(format!("    public {simple}({decl})"));
        block(&mut lines, head, Some(init), &[]);
    }
    check_clash(
        &info.java_fqn,
        &mut sigs,
        "CoreError _, string message, Exception innerException",
    )?;
    block(
        &mut lines,
        vec![format!(
            "    internal {simple}(CoreError _, string? message, Exception? innerException)"
        )],
        Some("base(message ?? string.Empty, innerException)".into()),
        &["_givenMessage = message;".to_string()],
    );
    lines.push("    string IExceptionText.TypeAndMessage() => JavaText.TypeAndMessage(this, _givenMessage);".into());
    lines.push("}".into());
    Ok(lines)
}

fn check_clash(fqn: &str, sigs: &mut Vec<String>, core: &str) -> anyhow::Result<()> {
    sigs.push(core.replace('?', ""));
    let mut seen = BTreeSet::new();
    for s in sigs.iter() {
        if !seen.insert(s.clone()) {
            anyhow::bail!("error hierarchy (C#): {fqn}: two constructors with the C# signature `({s})`");
        }
    }
    Ok(())
}

fn render_kafka(g: &Graph, info: &ClassInfo, usings: &mut BTreeSet<&'static str>) -> anyhow::Result<Vec<String>> {
    let fqn = &info.java_fqn;
    let simple = simple_name(info);
    let base = base_of(g, info)?;
    let null_fed = null_fed_params(g, info)?;
    let fields = class_fields(g, info, &null_fed, usings)?;
    let props = getters(g, info, &fields, usings)?;
    let is_api = is_ancestor_or_self(g, "org.apache.kafka.common.errors.ApiException", fqn);

    let mut lines = class_summary(info);
    let abstract_kw = if info.is_abstract { "abstract " } else { "" };
    lines.push(format!("public {abstract_kw}class {simple} : {base}"));
    lines.push("{".into());

    // Singletons.
    for s in &info.java.singletons {
        let empty: Vec<CsParam> = Vec::new();
        let args = call_args(g, fqn, &s.args, &Scope { params: &empty }, false, usings)?;
        let mut doc = format!(
            "The instance Java's <c>{}</c> holds. An error the client throws is never this instance, so compare \
             by type.",
            s.name
        );
        if is_api {
            doc.push_str(" .NET records a stack trace on every throw, where Java's <c>ApiException</c> records none.");
        }
        lines.extend(summary(&doc, "    "));
        // Java's own name and spelling, as for every `public static final` field (`INSTANCE`).
        lines.push(format!(
            "    public static readonly {simple} {} = new {simple}({});",
            s.name,
            args.join(", ")
        ));
        lines.push(String::new());
    }

    // Backing fields.
    for f in &fields {
        lines.push(format!("    private readonly {} {};", f.ty.decl(f.nullable), f.member));
        lines.push(String::new());
    }

    // Constructors.
    let mut sigs: Vec<String> = Vec::new();
    for idx in kept_ctors(info)? {
        let c = &info.java.ctors[idx];
        let ps = ctor_params(g, fqn, idx, &null_fed, usings)?;
        let scope = Scope { params: &ps };
        let visibility = match c.visibility {
            Visibility::Public if info.is_abstract => "protected",
            Visibility::Public => "public",
            Visibility::Protected => "protected",
            Visibility::Private => "private",
            Visibility::Package => unreachable!("kept_ctors refuses it"),
        };
        sigs.push(ps.iter().map(|p| p.ty.text.clone()).collect::<Vec<_>>().join(", "));
        let init = match (&c.call, &info.java.to_string) {
            (None, Some(ts)) => Some(format!("base({})", to_string_message(info, ts, c, &scope, usings)?)),
            (None, None) => None,
            (Some(_), Some(_)) => {
                anyhow::bail!("error hierarchy (C#): {fqn}: a `toString()` override with a message of its own")
            },
            (Some((kind, args)), None) => {
                let (target, word) = match kind {
                    CallKind::This => (fqn.clone(), "this"),
                    CallKind::Super => (info.parent_fqn.clone(), "base"),
                };
                let args = call_args(g, &target, args, &scope, *kind == CallKind::Super, usings)?;
                if args.is_empty() && *kind == CallKind::Super {
                    None
                } else {
                    Some(format!("{word}({})", args.join(", ")))
                }
            },
        };
        let mut body = Vec::new();
        for (name, e) in &c.assigns {
            let f = fields.iter().find(|f| f.java_name == *name).ok_or_else(|| {
                anyhow::anyhow!("error hierarchy (C#): {fqn}: assigns `{name}`, which is not an instance field")
            })?;
            body.push(format!("{} = {};", f.member, cs_expr(e, &scope, Some(&f.ty), usings)?));
        }
        let mut head = Vec::new();
        if visibility != "private" {
            head = summary(
                &format!(
                    "Initializes the exception, as Java's <c>{}</c>.",
                    xml_escape(&java_signature(simple, &c.params))
                ),
                "    ",
            );
        }
        let decls: Vec<String> = ps.iter().map(CsParam::decl).collect();
        head.push(format!("    {visibility} {simple}({})", decls.join(", ")));
        block(&mut lines, head, init, &body);
    }

    // The core path's constructor.
    usings.insert(NS_INTERNAL);
    usings.insert(NS_SYSTEM);
    let all = chain_fields(g, fqn, usings)?;
    let base_fields = chain_fields(g, &info.parent_fqn, usings)?;
    for (n, _) in &all {
        if matches!(n.as_str(), "message" | "innerException" | "_") {
            anyhow::bail!("error hierarchy (C#): {fqn}: a field named `{n}` clashes with the core path");
        }
    }
    let mut core_params = vec![
        "CoreError _".to_string(),
        "string? message".into(),
        "Exception? innerException".into(),
    ];
    core_params.extend(all.iter().map(|(n, t)| format!("{t} {n}")));
    let core_sig: Vec<String> = core_params
        .iter()
        .map(|p| p.rsplit_once(' ').map_or(p.clone(), |(t, _)| t.to_string()))
        .collect();
    check_clash(fqn, &mut sigs, &core_sig.join(", "))?;
    let mut base_args = vec!["_".to_string(), "message".into(), "innerException".into()];
    base_args.extend(base_fields.iter().map(|(n, _)| n.clone()));
    let own: Vec<String> = fields.iter().map(|f| format!("{} = {};", f.member, f.java_name)).collect();
    block(
        &mut lines,
        vec![format!("    internal {simple}({})", core_params.join(", "))],
        Some(format!("base({})", base_args.join(", "))),
        &own,
    );

    // Getter methods, keeping the Java getters' order; factories where Java declares them.
    let mut factories: Vec<&crate::java_parse::Factory> = info.java.factories.iter().collect();
    factories.sort_by_key(|f| f.after_getters);
    let mut fi = 0;
    for (pi, p) in props.iter().enumerate() {
        while fi < factories.len() && factories[fi].after_getters <= pi {
            render_factory(g, info, factories[fi], &mut lines, usings)?;
            fi += 1;
        }
        let doc = if p.modifier == "override" {
            vec!["    /// <inheritdoc/>".to_string()]
        } else {
            summary(&format!("Java's <c>{}()</c>.", p.java_getter), "    ")
        };
        lines.extend(doc);
        let m = if p.modifier.is_empty() {
            String::new()
        } else {
            format!("{} ", p.modifier)
        };
        let decl = match &p.body {
            None => format!("    public {m}{} {}();", p.ty, p.name),
            Some(b) => format!("    public {m}{} {}() => {b};", p.ty, p.name),
        };
        lines.push(decl);
        lines.push(String::new());
    }
    while fi < factories.len() {
        render_factory(g, info, factories[fi], &mut lines, usings)?;
        fi += 1;
    }

    // The id and the Java `toString()`.
    // `FfiId` is `KafkaException`'s member; a class under a JDK base has its id in the registry only.
    if !info.is_abstract && is_ancestor_or_self(g, KAFKA_EXCEPTION_FQN, fqn) {
        if let Some((_, id)) = &info.ffi_id {
            lines.push(format!("    internal override int FfiId => {id};"));
            lines.push(String::new());
        }
    }
    if info.java.to_string.is_some() {
        lines.push("    internal override string TypeAndMessage() => JavaText.TypeAndMessage(this, Message);".into());
        lines.push(String::new());
    }
    while lines.last().is_some_and(String::is_empty) {
        lines.pop();
    }
    lines.push("}".into());
    Ok(lines)
}

fn render_factory(
    g: &Graph,
    info: &ClassInfo,
    f: &crate::java_parse::Factory,
    lines: &mut Vec<String>,
    usings: &mut BTreeSet<&'static str>,
) -> anyhow::Result<()> {
    let simple = simple_name(info);
    let mut ps = Vec::new();
    for p in &f.params {
        let ty = cs_type(&p.ty, usings)?;
        let name = if throwable_like(&p.ty) {
            "innerException".to_string()
        } else {
            p.name.clone()
        };
        if CS_ONLY_KEYWORDS.contains(&name.as_str()) {
            anyhow::bail!(
                "error hierarchy (C#): {}: the parameter `{name}` is a C# keyword",
                info.java_fqn
            );
        }
        ps.push(CsParam { java_name: p.name.clone(), java_ty: p.ty.clone(), name, ty, nullable: false });
    }
    let scope = Scope { params: &ps };
    let args = call_args(g, &info.java_fqn, &f.args, &scope, false, usings)?;
    lines.extend(summary(
        &format!(
            "Creates the exception, as Java's <c>{}</c>.",
            xml_escape(&java_signature(&f.name, &f.params))
        ),
        "    ",
    ));
    let decls: Vec<String> = ps.iter().map(CsParam::decl).collect();
    lines.push(format!("    public static {simple} {}({})", pascal(&f.name), decls.join(", ")));
    lines.push("    {".into());
    lines.push(format!("        return new {simple}({});", args.join(", ")));
    lines.push("    }".into());
    lines.push(String::new());
    Ok(())
}

// ---------------------------------------------------------------------------
// The registry, the layout rows, and the checks
// ---------------------------------------------------------------------------

/// Whether a class has instance fields anywhere in its chain, so the core path reads them.
fn has_fields(g: &Graph, info: &ClassInfo) -> bool {
    chain(g, &info.java_fqn)
        .iter()
        .any(|c| c.java.fields.iter().any(|f| !f.is_static))
}

fn render_registry(g: &Graph) -> anyhow::Result<String> {
    let mut by_id: Vec<(i32, &ClassInfo)> = g
        .classes
        .values()
        .filter_map(|c| c.ffi_id.as_ref().map(|(_, id)| (*id, c)))
        .collect();
    by_id.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.java_fqn.cmp(&b.1.java_fqn)));
    let mut readers = BTreeSet::new();
    let mut factories = Vec::new();
    for (id, c) in &by_id {
        if *id == -1 || c.is_abstract || CSHARP_HAND_WRITTEN.contains(&c.java_fqn.as_str()) {
            continue;
        }
        let fq = cs_fqn(c)?;
        if has_fields(g, c) {
            let reader = format!("Read{}", simple_name(c));
            if !readers.insert(reader.clone()) {
                anyhow::bail!("error hierarchy (C#): two payload readers named `{reader}`");
            }
            factories.push(format!(
                "            [{id}] = (message, innerException, error) => ErrorPayloadMarshal.{reader}(message, innerException, error),"
            ));
        } else {
            factories.push(format!(
                "            [{id}] = (message, innerException, _) => new global::{fq}(default(CoreError), message, innerException),"
            ));
        }
    }
    let mut ids = Vec::new();
    for (id, c) in &by_id {
        ids.push(format!("            [typeof(global::{})] = {id},", cs_fqn(c)?));
    }
    let usings: BTreeSet<&'static str> = [NS_SYSTEM, NS_SCG, NS_INTEROP].into_iter().collect();
    let ty = "Func<string?, Exception?, IntPtr, Exception>";
    let mut body = summary(
        "The generated error classes: the factory of each C id, and the C id of each class.",
        "",
    );
    body.push("internal static class ErrorClasses".into());
    body.push("{".into());
    body.push(
        "    /// <summary>Builds the class of a C id from the core's message, cause and error handle.</summary>".into(),
    );
    body.push(format!(
        "    internal static IReadOnlyDictionary<int, {ty}> Factories {{ get; }} ="
    ));
    body.push(format!("        new Dictionary<int, {ty}>"));
    body.push("        {".into());
    body.extend(factories);
    body.push("        };".into());
    body.push(String::new());
    body.push("    /// <summary>The C id of each class that has one.</summary>".into());
    body.push("    internal static IReadOnlyDictionary<Type, int> Ids { get; } =".into());
    body.push("        new Dictionary<Type, int>".into());
    body.push("        {".into());
    body.extend(ids);
    body.push("        };".into());
    body.push("}".into());
    Ok(file_text_with(&usings, NS_INTERNAL, body, true))
}

fn render_rows(g: &Graph) -> String {
    let mut rows: Vec<String> = g
        .classes
        .values()
        .map(|c| format!("        (\"{}\", \"{}\"),", simple_name(c), c.java_fqn))
        .collect();
    rows.sort();
    let usings: BTreeSet<&'static str> = [NS_SCG].into_iter().collect();
    let mut body = summary(
        "The error classes the generator knows, hand-written ones included: the .NET name and the Java class.",
        "",
    );
    body.push("internal static class ErrorClassRows".into());
    body.push("{".into());
    body.push("    /// <summary>One row per class: its name below its namespace, and its Java class.</summary>".into());
    body.push("    internal static IReadOnlyList<(string DotNet, string Java)> Rows { get; } = new[]".into());
    body.push("    {".into());
    body.extend(rows);
    body.push("    };".into());
    body.push("}".into());
    file_text_with(&usings, "Confluent.Kafka.UnitTests", body, true)
}

/// Every C# output: one file per generated class, the registry and the layout rows.
pub(super) fn outputs(repo_root: &Path, g: &Graph) -> anyhow::Result<Vec<(PathBuf, String)>> {
    let mut out = Vec::new();
    for info in g.classes.values() {
        if CSHARP_HAND_WRITTEN.contains(&info.java_fqn.as_str()) {
            continue;
        }
        out.push(render_class(g, info, repo_root)?);
    }
    out.push((repo_root.join(CS_SRC).join("Internal/ErrorClasses.g.cs"), render_registry(g)?));
    out.push((repo_root.join(CS_TESTS).join("ErrorClassRows.g.cs"), render_rows(g)));
    Ok(out)
}

/// Generated `.cs` files under the two projects that the Java sources no longer produce.
pub(super) fn orphans(repo_root: &Path, outputs: &[(PathBuf, String)]) -> anyhow::Result<Vec<PathBuf>> {
    let expected: BTreeSet<&PathBuf> = outputs.iter().map(|(p, _)| p).collect();
    let mut found = Vec::new();
    let mut stack: Vec<PathBuf> = [CS_SRC, CS_TESTS]
        .iter()
        .map(|d| repo_root.join(d))
        .filter(|d| d.is_dir())
        .collect();
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)? {
            let path = entry?.path();
            if path.is_dir() {
                if path.file_name().is_some_and(|n| n != "bin" && n != "obj") {
                    stack.push(path);
                }
                continue;
            }
            if path.extension().is_none_or(|e| e != "cs") || expected.contains(&path) {
                continue;
            }
            let text = fs::read_to_string(&path)?;
            if text.lines().take(25).any(|l| l.contains(GENERATED_MARKER)) {
                found.push(path);
            }
        }
    }
    found.sort();
    Ok(found)
}

/// C# outputs outside the two projects, which no build would compile.
pub(super) fn outside_projects(repo_root: &Path, outputs: &[(PathBuf, String)]) -> Vec<String> {
    let roots = [repo_root.join(CS_SRC), repo_root.join(CS_TESTS)];
    outputs
        .iter()
        .filter(|(p, _)| p.extension().is_some_and(|e| e == "cs"))
        .filter(|(p, _)| !roots.iter().any(|r| p.starts_with(r)))
        .map(|(p, _)| p.display().to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};

    use super::super::{build_graph, ClassInfo, Graph};
    use super::{cs_namespace, render_class};
    use crate::java_parse::{Ctor, Param, Visibility};

    fn repo_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .unwrap()
            .to_path_buf()
    }

    fn graph() -> BTreeMap<String, ClassInfo> {
        build_graph(&repo_root()).unwrap()
    }

    fn render(classes: &BTreeMap<String, ClassInfo>, fqn: &str) -> String {
        let g = Graph { classes };
        render_class(&g, &classes[fqn], &repo_root()).unwrap().1
    }

    fn errors(simple: &str) -> String {
        format!("org.apache.kafka.common.errors.{simple}")
    }

    #[test]
    fn namespaces_follow_the_folder_rule() {
        assert_eq!(
            cs_namespace("org.apache.kafka.common.errors").unwrap(),
            "Confluent.Kafka.Common.Errors"
        );
        assert_eq!(
            cs_namespace("org.apache.kafka.clients.consumer").unwrap(),
            "Confluent.Kafka.Consumer"
        );
        assert_eq!(cs_namespace("java.util.concurrent").unwrap(), "Confluent.Kafka");
        assert!(cs_namespace("com.example").is_err());
    }

    #[test]
    fn a_plain_class_chains_each_constructor_and_overrides_its_id() {
        let c = graph();
        let cs = render(&c, &errors("TopicExistsException"));
        assert!(cs.contains("GENERATED, DO NOT EDIT"));
        assert!(cs.contains("namespace Confluent.Kafka.Common.Errors;"));
        assert!(cs.contains("public class TopicExistsException : global::Confluent.Kafka.Common.Errors.ApiException"));
        assert!(cs.contains("    public TopicExistsException(string? message, Exception? innerException)\n        : base(message, innerException)"));
        assert!(cs.contains("    internal TopicExistsException(CoreError _, string? message, Exception? innerException)\n        : base(_, message, innerException)"));
        assert!(cs.contains("    internal override int FfiId => 36;"));
    }

    #[test]
    fn an_abstract_base_has_protected_constructors_and_no_id() {
        let c = graph();
        let cs = render(&c, &errors("InvalidMetadataException"));
        assert!(cs.contains("public abstract class InvalidMetadataException"));
        assert!(cs.contains("    protected InvalidMetadataException(string? message)"));
        assert!(!cs.contains("public InvalidMetadataException("));
        assert!(!cs.contains("FfiId"));
    }

    #[test]
    fn a_payload_class_assigns_its_field_and_takes_it_on_the_core_path() {
        let c = graph();
        let cs = render(&c, &errors("GroupAuthorizationException"));
        assert!(cs.contains("    private readonly string? _groupId;"));
        assert!(cs.contains("    public GroupAuthorizationException(string? message, string? groupId)\n        : base(message)\n    {\n        _groupId = groupId;\n    }"));
        assert!(cs.contains("    public GroupAuthorizationException(string? message)\n        : this(message, null)"));
        assert!(cs.contains("    internal GroupAuthorizationException(CoreError _, string? message, Exception? innerException, string? groupId)"));
        assert!(cs.contains(
            "    /// <summary>Java's <c>groupId()</c>.</summary>\n    public string? GroupId() => _groupId;"
        ));
        assert!(!cs.contains("{ get; }"));
    }

    #[test]
    fn a_factory_becomes_a_static_method() {
        let c = graph();
        let cs = render(&c, &errors("GroupAuthorizationException"));
        assert!(cs.contains("    public static GroupAuthorizationException ForGroupId(string? groupId)\n    {\n        return new GroupAuthorizationException(\"Not authorized to access group: \" + JavaText.ValueOf(groupId), groupId);\n    }"));
    }

    #[test]
    fn a_singleton_keeps_javas_name_and_a_private_constructor() {
        let c = graph();
        let cs = render(&c, &errors("CoordinatorNotAvailableException"));
        assert!(cs.contains("    public static readonly CoordinatorNotAvailableException INSTANCE = new CoordinatorNotAvailableException();"));
        assert!(cs.contains("    private CoordinatorNotAvailableException()\n"));
        assert!(cs.contains("compare by"));
    }

    #[test]
    fn an_abstract_getter_and_its_key_set_override() {
        let c = graph();
        let base = render(&c, "org.apache.kafka.clients.consumer.InvalidOffsetException");
        assert!(base.contains("    public abstract IReadOnlyCollection<TopicPartition> Partitions();"));
        let sub = render(&c, "org.apache.kafka.clients.consumer.OffsetOutOfRangeException");
        assert!(sub.contains("    public IReadOnlyDictionary<TopicPartition, long> OffsetOutOfRangePartitions() => _offsetOutOfRangePartitions;"));
        assert!(sub.contains("    /// <inheritdoc/>\n    public override IReadOnlyCollection<TopicPartition> Partitions() => new KeySetView<TopicPartition, long>(_offsetOutOfRangePartitions);"));
    }

    #[test]
    fn two_constructors_with_one_csharp_signature_stop_generation() {
        let mut c = graph();
        let fqn = errors("SerializationException");
        let extra = Ctor {
            visibility: Visibility::Public,
            params: vec![Param { ty: "String".into(), name: "other".into() }],
            call: None,
            assigns: Vec::new(),
            deprecated: None,
        };
        c.get_mut(&fqn).unwrap().java.ctors.push(extra);
        let g = Graph { classes: &c };
        let err = render_class(&g, &c[&fqn], &repo_root()).unwrap_err().to_string();
        assert!(err.contains("two constructors with the C# signature `(string)`"), "{err}");
    }

    #[test]
    fn a_message_parameter_is_named_message_whatever_java_calls_it() {
        let c = graph();
        // `msg` in a Kafka class, `s` in a JDK class.
        let cs = render(&c, &errors("ProducerFencedException"));
        assert!(cs.contains("    public ProducerFencedException(string? message)\n        : base(message)"));
        let jdk = render(&c, "java.lang.IllegalArgumentException");
        assert!(jdk.contains(
            "    public IllegalArgumentException(string? message)\n        : this(default(CoreError), message, null)"
        ));
        assert!(jdk.contains("IllegalArgumentException(String s)"));
    }

    #[test]
    fn a_set_a_map_and_a_double_in_a_message_print_as_java() {
        let c = graph();
        let set = render(&c, &errors("InvalidTopicException"));
        assert!(set.contains(": base(\"Invalid topics: \" + JavaText.ValueOf(invalidTopics))"));
        let map = render(&c, "org.apache.kafka.clients.consumer.LogTruncationException");
        assert!(map.contains(
            "\"Truncated partitions detected with divergent offsets \" + JavaText.ValueOf(divergentOffsets)"
        ));
        let double = render(&c, "org.apache.kafka.common.metrics.QuotaViolationException");
        assert!(double.contains(
            "\"' violated quota. Actual: \" + JavaText.ValueOf(value) + \", Threshold: \" + JavaText.ValueOf(bound)"
        ));
        assert!(double.contains("(metric ?? throw new ArgumentNullException(nameof(metric))).MetricName()"));
        assert!(double.contains("internal override string TypeAndMessage() => JavaText.TypeAndMessage(this, Message);"));
    }

    #[test]
    fn a_base_whose_name_dotnet_shares_is_global() {
        let c = graph();
        let cs = render(&c, "org.apache.kafka.clients.producer.BufferExhaustedException");
        assert!(cs.contains(
            "public class BufferExhaustedException : global::Confluent.Kafka.Common.Errors.TimeoutException"
        ));
        let jdk = render(&c, "java.util.concurrent.TimeoutException");
        assert!(jdk.contains("public class TimeoutException : global::System.TimeoutException, IExceptionText"));
    }

    #[test]
    fn a_class_under_a_jdk_base_has_no_ffi_id_member() {
        let c = graph();
        let cs = render(&c, "org.apache.kafka.common.requests.CorrelationIdMismatchException");
        assert!(cs.contains(": global::Confluent.Kafka.IllegalStateException"));
        assert!(!cs.contains("FfiId"));
    }

    #[test]
    fn a_getter_named_like_an_inherited_method_stops_generation() {
        for (java, cs) in [
            ("getType", "GetType"),
            ("typeAndMessage", "TypeAndMessage"),
            ("paramName", "ParamName"),
        ] {
            let mut c = graph();
            let fqn = errors("GroupAuthorizationException");
            c.get_mut(&fqn).unwrap().java.getters[0].name = java.into();
            let g = Graph { classes: &c };
            let err = render_class(&g, &c[&fqn], &repo_root()).unwrap_err().to_string();
            assert!(err.contains(&format!("would hide `{cs}`")), "{err}");
        }
    }
}
