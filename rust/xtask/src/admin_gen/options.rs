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

//! `cargo xtask generate-admin-options`: the admin `*Options` Python classes
//! (`python/confluent_kafka/admin/<class>.py` + `.pyi`) from the Java sources
//! (CLAUDE.md, Python Binding Conventions: Class family and Signatures).
//!
//! Each class is read member by member and every body must be one of the known
//! patterns below; anything else stops generation for that class ([`Stop`]),
//! and the class is then written by hand ([`HAND_WRITTEN`]).
//!
//! Known patterns:
//! - setter: `this.f = <store>; return this;` (or `return (T) this;`, or a
//!   `void` setter without the return), `<store>` one of `p`,
//!   `OptionalInt.of(p)`, `new HashSet<>(p)`,
//!   `(p == null || p.isEmpty()) ? Set.of() : Set.copyOf(p)`;
//! - getter: `return f;`, `return Optional.ofNullable(f);`, `return f.isEmpty();`;
//! - static factory: `return new X().setter(Set.of(...))...;`;
//! - `toString` as a concatenation of literals and fields, `equals` comparing
//!   fields with `==` / `Objects.equals`, `hashCode` as `Objects.hash(fields)`;
//! - constructors: none, an empty no-argument one (a class Java builds with
//!   setters), or the `RemoveMembersFromConsumerGroupOptions` pair (`()`
//!   stores an empty set, `(Collection)` rejects an empty one and copies it).
//!
//! The reference set (`python/confluent_kafka/admin/_reference_set.md`) is the
//! golden output: `test_golden_reference_options` fails on any byte that differs.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use super::{
    deprecated_items, docstring, erasure, first_difference, indent_literal, module_title, parse_java_class, read_doc,
    snake, split_top, statements, stop, wrap_expression, wrap_signature, wrap_words, Doc, Imports, JClass, JKind,
    JMember, JParam, Piece, Prose, Res, Stop, ADMIN_JAVA, ADMIN_PY, JAVA_ROOT, LICENSE, WIDTH,
};

/// The generated classes: `AbstractOptions` plus the Options class of each of
/// the 45 `Admin` RPCs the Python client covers (admin inventory I1; `incrementalAlterConfigs`
/// takes `AlterConfigsOptions`, `forceTerminateTransaction`
/// `TerminateTransactionOptions`).
pub const CLASSES: [&str; 46] = [
    "AbortTransactionOptions",
    "AbstractOptions",
    "AlterClientQuotasOptions",
    "AlterConfigsOptions",
    "AlterConsumerGroupOffsetsOptions",
    "AlterPartitionReassignmentsOptions",
    "AlterReplicaLogDirsOptions",
    "AlterUserScramCredentialsOptions",
    "CreateAclsOptions",
    "CreateDelegationTokenOptions",
    "CreatePartitionsOptions",
    "CreateTopicsOptions",
    "DeleteAclsOptions",
    "DeleteConsumerGroupOffsetsOptions",
    "DeleteConsumerGroupsOptions",
    "DeleteRecordsOptions",
    "DeleteTopicsOptions",
    "DescribeAclsOptions",
    "DescribeClassicGroupsOptions",
    "DescribeClientQuotasOptions",
    "DescribeClusterOptions",
    "DescribeConfigsOptions",
    "DescribeConsumerGroupsOptions",
    "DescribeDelegationTokenOptions",
    "DescribeFeaturesOptions",
    "DescribeLogDirsOptions",
    "DescribeProducersOptions",
    "DescribeReplicaLogDirsOptions",
    "DescribeTopicsOptions",
    "DescribeTransactionsOptions",
    "DescribeUserScramCredentialsOptions",
    "ElectLeadersOptions",
    "ExpireDelegationTokenOptions",
    "FenceProducersOptions",
    "ListConfigResourcesOptions",
    "ListConsumerGroupOffsetsOptions",
    "ListGroupsOptions",
    "ListOffsetsOptions",
    "ListPartitionReassignmentsOptions",
    "ListTopicsOptions",
    "ListTransactionsOptions",
    "RemoveMembersFromConsumerGroupOptions",
    "RenewDelegationTokenOptions",
    "TerminateTransactionOptions",
    "UnregisterControllerOptions",
    "UpdateFeaturesOptions",
];

/// The Options classes of the hand-written reference set
/// (`python/confluent_kafka/admin/_reference_set.md`): the golden files, which
/// `generate` never overwrites with different content.
pub const REFERENCE: [&str; 12] = [
    "AbstractOptions",
    "CreateDelegationTokenOptions",
    "CreateTopicsOptions",
    "DeleteRecordsOptions",
    "DescribeFeaturesOptions",
    "DescribeProducersOptions",
    "DescribeTopicsOptions",
    "ListGroupsOptions",
    "ListOffsetsOptions",
    "ListTopicsOptions",
    "ListTransactionsOptions",
    "RemoveMembersFromConsumerGroupOptions",
];

/// Classes the generator stops on, written by hand in the reference style.
pub const HAND_WRITTEN: [&str; 0] = [];

/// The docstring text not derived from Javadoc (see [`Prose`]).
const PROSE: &str = include_str!("options_prose.txt");

/// A stub line may run this far past [`WIDTH`] before it wraps (the reference
/// set keeps 101-character stubs on one line and wraps 104-character ones).
const STUB_SLACK: usize = 3;

const REFERENCE_SET: &str = "python/confluent_kafka/admin/_reference_set.md";
const INIT: &str = "python/confluent_kafka/admin/__init__.py";
const INIT_BEGIN: &str = "# BEGIN OPTIONS";
const INIT_END: &str = "# END OPTIONS";

// ---------------------------------------------------------------------------
// The model
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Field {
    is_final: bool,
    java: String,
    py: String,
    ty: String,
    init: Option<String>,
    decl: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Store {
    AsGiven,
    OptionalOf,
    NewHashSet,
    CopyOf,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ret {
    This,
    SelfType,
    Void,
}

#[derive(Clone, Debug)]
struct Setter {
    name: String,
    param: JParam,
    field: String,
    store: Store,
    rhs: String,
    ret: Ret,
    doc: Doc,
    comments: Vec<String>,
    /// What the Javadoc of the setter and its getter says about `null`.
    doc_null: Option<bool>,
    /// The field's getter is tested for null by Java.
    null_tested: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GetBody {
    Field,
    OfNullable,
    IsEmpty,
}

#[derive(Clone, Debug)]
struct Getter {
    name: String,
    ret: String,
    field: String,
    body: GetBody,
    doc: Doc,
}

#[derive(Clone, Debug)]
enum Elem {
    Str(String),
    /// `Class.CONSTANT`: an enum constant of a public class, or a constant of
    /// an internal class (then a private module constant).
    Constant(String, String),
}

#[derive(Clone, Debug)]
struct Factory {
    name: String,
    chain: Vec<(String, Vec<Elem>)>,
    doc: Doc,
}

#[derive(Clone, Debug)]
enum Term {
    Lit(String),
    Field(String),
}

#[derive(Clone, Debug)]
enum Item {
    /// A setter and the getter sharing its name (`None`: the getter is the
    /// parent's).
    Merged(usize, Option<usize>),
    Setter(usize),
    Getter(usize),
    Factory(usize),
    Str(Vec<Vec<Term>>),
    Eq(Vec<String>),
    Hash(Vec<String>),
}

#[derive(Clone, Debug)]
enum Construction {
    /// Java's `abstract`: no keyword constructor.
    Abstract,
    /// Built with setters: one keyword per setter field.
    Keywords,
    /// `()` stores an empty set in `field`, `(Collection p)` rejects an empty
    /// collection with `message` and copies it.
    EmptyOrNonEmpty {
        field: String,
        param: JParam,
        message: String,
    },
    /// `()` calls `this(Class.CONSTANT)`, `(T p)` stores `p` in `field`: one
    /// keyword defaulting to the constant (a Java-given default).
    Delegating {
        field: String,
        param: JParam,
        default: (String, String),
    },
}

#[derive(Clone, Debug)]
struct Options {
    class: JClass,
    fields: Vec<Field>,
    setters: Vec<Setter>,
    getters: Vec<Getter>,
    factories: Vec<Factory>,
    items: Vec<Item>,
    construction: Construction,
    /// Java's deprecated members, not generated: `name(types)`.
    deprecated: Vec<String>,
    /// Re-declares a parent setter.
    redeclares: bool,
}

/// What the parent (`AbstractOptions`) contributes to a subclass.
#[derive(Clone, Debug, Default)]
struct Parent {
    fields: Vec<Field>,
    setters: Vec<Setter>,
    getters: Vec<Getter>,
}

fn is_primitive(ty: &str) -> bool {
    matches!(ty, "boolean" | "int" | "long" | "short" | "byte" | "double" | "float" | "char")
}

/// Javadoc phrases that make an argument non-null; they win over any other.
const NON_NULL: [&str; 4] = ["non-null", "not be null", "cannot be null", "never null"];
/// Javadoc phrases that give `null` a meaning (`{@code null}` reads ``None``).
const ALLOWS_NULL: [&str; 6] = [
    "``none``",
    "is null",
    "or null",
    "can be null",
    "may be null",
    "null means",
];

/// What the Javadoc of a setter and its getter says about `null`
/// (CLAUDE.md, Python Binding Conventions, Signatures, Nullability):
/// `Some(false)` for "Non-null", `Some(true)` for a phrase allowing it,
/// `None` when it does not mention it. Any other mention of `null` is not
/// guessed at: `Err` (generation stops).
fn doc_nullability(docs: &[&Doc]) -> Result<Option<bool>, String> {
    let mut text = String::new();
    for d in docs {
        for t in d
            .paragraphs
            .iter()
            .chain(d.params.iter().map(|(_, t)| t))
            .chain(d.returns.iter())
        {
            text.push(' ');
            text.push_str(&t.to_lowercase());
        }
    }
    // `{@code null}` reads ``None`` after markup; the non-null phrases match
    // either spelling ("must not be {@code null}").
    let as_null = text.replace("``none``", "null");
    if NON_NULL.iter().any(|p| as_null.contains(p)) {
        return Ok(Some(false));
    }
    if ALLOWS_NULL.iter().any(|p| text.contains(p)) {
        return Ok(Some(true));
    }
    if text.split(|c: char| !c.is_alphanumeric()).any(|w| w == "null") {
        return Err(format!("Javadoc mentions null in no known phrase:{text}"));
    }
    Ok(None)
}

/// What the Java sources outside the class tell the generator.
#[derive(Clone, Debug, Default)]
struct Known {
    deprecated: BTreeSet<String>,
    /// Getters Java tests for null somewhere in the admin package
    /// (`options.reason() == null`).
    null_tested: BTreeSet<String>,
}

/// The getters the admin package's Java tests for null: `.name() == null` /
/// `.name() != null`.
fn null_tested_getters(repo_root: &Path) -> Res<BTreeSet<String>> {
    let mut out = BTreeSet::new();
    let mut dirs = vec![repo_root.join(ADMIN_JAVA)];
    while let Some(dir) = dirs.pop() {
        for entry in fs::read_dir(&dir)? {
            let path = entry?.path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.extension().is_some_and(|e| e == "java") {
                let src = fs::read_to_string(&path)?;
                for op in ["() == null", "() != null"] {
                    for (at, _) in src.match_indices(op) {
                        let name: String = src[..at]
                            .chars()
                            .rev()
                            .take_while(|c| c.is_alphanumeric() || *c == '_')
                            .collect::<Vec<_>>()
                            .into_iter()
                            .rev()
                            .collect();
                        if src[..at - name.len()].ends_with('.') && !name.is_empty() {
                            out.insert(name);
                        }
                    }
                }
            }
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Reading the Java class
// ---------------------------------------------------------------------------

fn field_rhs<'a>(stmt: &'a str, fields: &[&str]) -> Option<(String, &'a str)> {
    let (lhs, rhs) = stmt.split_once(" = ")?;
    let lhs = lhs.trim().trim_start_matches("this.");
    fields.contains(&lhs).then(|| (lhs.to_string(), rhs.trim()))
}

fn analyze(class: JClass, parent: &Parent, known: &Known) -> Result<Options, Stop> {
    let name = class.name.clone();
    let fqcn = class.fqcn();
    let st = |r: String| stop(&name, r);
    let mut fields = Vec::new();
    let mut methods: Vec<&JMember> = Vec::new();
    let mut ctors: Vec<(Vec<JParam>, String)> = Vec::new();
    for m in &class.members {
        match &m.kind {
            JKind::Field { ty, name: f, init } => {
                if m.is_static() {
                    return Err(st(format!("static field `{}`", m.decl)));
                }
                fields.push(Field {
                    is_final: m.modifiers.iter().any(|x| x == "final"),
                    java: f.clone(),
                    py: snake(f),
                    ty: ty.clone(),
                    init: init.clone(),
                    decl: m.decl.clone(),
                });
            },
            JKind::Method { .. } => methods.push(m),
            JKind::Ctor { params, body } => {
                if !m.is_public() {
                    return Err(st("a non-public constructor".into()));
                }
                ctors.push((params.clone(), body.clone()));
            },
            JKind::Other(h) => return Err(st(format!("nested member `{h}`"))),
        }
    }
    let mut all_fields: Vec<&str> = parent.fields.iter().map(|f| f.java.as_str()).collect();
    all_fields.extend(fields.iter().map(|f| f.java.as_str()));

    let mut setters = Vec::new();
    let mut getters = Vec::new();
    let mut factories = Vec::new();
    let mut raw_items: Vec<Item> = Vec::new();
    let mut deprecated = Vec::new();
    for m in methods {
        let JKind::Method { ret, name: mname, params, body, raw } = &m.kind else {
            continue;
        };
        if !m.is_public() {
            return Err(st(format!("non-public method `{}`", m.decl)));
        }
        let body = body.clone().ok_or_else(|| st(format!("method without body `{}`", m.decl)))?;
        let sig = params.iter().map(|p| erasure(&p.ty)).collect::<Vec<_>>().join(",");
        if m.has_annotation("Deprecated") || known.deprecated.contains(&format!("{fqcn}#{mname}({sig})")) {
            deprecated.push(format!("{mname}({sig})"));
            continue;
        }
        let doc = m.javadoc.as_deref().map(read_doc).unwrap_or_default();
        let stmts = statements(&body);
        if m.has_annotation("Override") {
            match (mname.as_str(), params.len()) {
                ("toString", 0) => raw_items.push(Item::Str(
                    parse_to_string(&stmts, raw.as_deref().unwrap_or(""), &all_fields).map_err(st)?,
                )),
                ("equals", 1) => {
                    raw_items.push(Item::Eq(parse_equals(&stmts, &params[0].name, &name, &all_fields).map_err(st)?))
                },
                ("hashCode", 0) => raw_items.push(Item::Hash(parse_hash(&stmts, &all_fields).map_err(st)?)),
                _ => return Err(st(format!("override `{}`", m.decl))),
            }
            continue;
        }
        if m.is_static() {
            if !params.is_empty() || ret != &name {
                return Err(st(format!("static method `{}`", m.decl)));
            }
            let chain = parse_factory(&stmts, &name).map_err(st)?;
            factories.push(Factory { name: mname.clone(), chain, doc });
            raw_items.push(Item::Factory(factories.len() - 1));
            continue;
        }
        match params.len() {
            1 => {
                let p = &params[0];
                let r = if ret == "void" {
                    Ret::Void
                } else if ret == &name {
                    Ret::This
                } else if class.is_abstract() && ret == "T" {
                    Ret::SelfType
                } else {
                    return Err(st(format!("setter `{mname}` returns `{ret}`")));
                };
                let expect_ret: &[&str] = match r {
                    Ret::Void => &[],
                    Ret::This => &["return this"],
                    Ret::SelfType => &["return (T) this"],
                };
                if stmts.len() != 1 + expect_ret.len()
                    || stmts[1..].iter().map(String::as_str).ne(expect_ret.iter().copied())
                {
                    return Err(st(format!("setter body `{body}`")));
                }
                let (field, rhs) =
                    field_rhs(&stmts[0], &all_fields).ok_or_else(|| st(format!("setter body `{body}`")))?;
                let pn = &p.name;
                let store = if rhs == pn {
                    Store::AsGiven
                } else if rhs == format!("OptionalInt.of({pn})") {
                    Store::OptionalOf
                } else if rhs == format!("new HashSet<>({pn})") {
                    Store::NewHashSet
                } else if rhs == format!("({pn} == null || {pn}.isEmpty()) ? Set.of() : Set.copyOf({pn})") {
                    Store::CopyOf
                } else {
                    return Err(st(format!("setter `{mname}` stores `{rhs}`")));
                };
                if setters.iter().any(|s: &Setter| &s.name == mname) {
                    return Err(st(format!("overloaded setter `{mname}`")));
                }
                setters.push(Setter {
                    name: mname.clone(),
                    param: p.clone(),
                    field,
                    store,
                    rhs: rhs.to_string(),
                    ret: r,
                    doc,
                    comments: m.comments.clone(),
                    doc_null: None,
                    null_tested: false,
                });
                raw_items.push(Item::Setter(setters.len() - 1));
            },
            0 => {
                if stmts.len() != 1 {
                    return Err(st(format!("getter body `{body}`")));
                }
                let e = stmts[0]
                    .strip_prefix("return ")
                    .ok_or_else(|| st(format!("getter body `{body}`")))?;
                let e = e.trim_start_matches("this.");
                let (field, gb) =
                    if let Some(f) = e.strip_prefix("Optional.ofNullable(").and_then(|r| r.strip_suffix(')')) {
                        (f.to_string(), GetBody::OfNullable)
                    } else if let Some(f) = e.strip_suffix(".isEmpty()") {
                        (f.to_string(), GetBody::IsEmpty)
                    } else {
                        (e.to_string(), GetBody::Field)
                    };
                if !all_fields.contains(&field.as_str()) {
                    return Err(st(format!("getter body `{body}`")));
                }
                getters.push(Getter { name: mname.clone(), ret: ret.clone(), field, body: gb, doc });
                raw_items.push(Item::Getter(getters.len() - 1));
            },
            _ => return Err(st(format!("method `{}` with several parameters", m.decl))),
        }
    }

    // A setter and a getter sharing one name are one method, at the place of
    // the first of the two (members keep Java's declaration order).
    let mut items = Vec::new();
    let mut done_setters = BTreeSet::new();
    let mut done_getters = BTreeSet::new();
    for it in raw_items {
        match it {
            Item::Setter(s) if !done_setters.contains(&s) => {
                done_setters.insert(s);
                match getters.iter().position(|g| g.name == setters[s].name) {
                    Some(g) => {
                        done_getters.insert(g);
                        items.push(Item::Merged(s, Some(g)));
                    },
                    None if parent.getters.iter().any(|g| g.name == setters[s].name) => {
                        items.push(Item::Merged(s, None));
                    },
                    None => items.push(Item::Setter(s)),
                }
            },
            Item::Getter(g) if !done_getters.contains(&g) => {
                done_getters.insert(g);
                match setters.iter().position(|s| s.name == getters[g].name) {
                    Some(s) => {
                        done_setters.insert(s);
                        items.push(Item::Merged(s, Some(g)));
                    },
                    None => items.push(Item::Getter(g)),
                }
            },
            Item::Setter(_) | Item::Getter(_) => {},
            other => items.push(other),
        }
    }
    // Nullability of each non-primitive setter argument, from the Javadoc of
    // the setter and of its getter, and from Java's null tests of the getter.
    for setter in setters.iter_mut() {
        if is_primitive(&setter.param.ty) {
            continue;
        }
        let mut docs = vec![&setter.doc];
        docs.extend(
            getters
                .iter()
                .chain(parent.getters.iter())
                .filter(|g| g.name == setter.name || g.field == setter.field)
                .map(|g| &g.doc),
        );
        let doc_null = doc_nullability(&docs).map_err(|r| st(format!("`{}`: {r}", setter.name)))?;
        setter.null_tested = getters
            .iter()
            .chain(parent.getters.iter())
            .any(|g| g.field == setter.field && known.null_tested.contains(&g.name));
        setter.doc_null = doc_null;
    }
    let redeclares = setters.iter().any(|s| parent.setters.iter().any(|p| p.name == s.name));

    if !class.is_abstract() && class.extends.as_deref() != Some(&format!("AbstractOptions<{name}>")) {
        return Err(st(format!("extends `{}`", class.extends.as_deref().unwrap_or(""))));
    }
    let construction = if class.is_abstract() {
        Construction::Abstract
    } else if ctors.is_empty() || (ctors.len() == 1 && ctors[0].0.is_empty() && ctors[0].1.is_empty()) {
        Construction::Keywords
    } else {
        empty_or_non_empty(&ctors, &all_fields)
            .or_else(|| delegating(&ctors, &all_fields))
            .ok_or_else(|| st("constructors outside the known patterns".into()))?
    };
    Ok(Options {
        class,
        fields,
        setters,
        getters,
        factories,
        items,
        construction,
        deprecated,
        redeclares,
    })
}

fn empty_or_non_empty(ctors: &[(Vec<JParam>, String)], fields: &[&str]) -> Option<Construction> {
    if ctors.len() != 2 {
        return None;
    }
    let (empty, full) = if ctors[0].0.is_empty() {
        (&ctors[0], &ctors[1])
    } else {
        (&ctors[1], &ctors[0])
    };
    let empty_stmts = statements(&empty.1);
    let (f, rhs) = field_rhs(empty_stmts.first()?, fields)?;
    if rhs != "Collections.emptySet()" || full.0.len() != 1 || !full.0[0].ty.starts_with("Collection<") {
        return None;
    }
    let p = &full.0[0].name;
    let prefix = format!("if ({p}.isEmpty()) {{ throw new IllegalArgumentException(\"");
    let suffix = format!("\"); }} this.{f} = new HashSet<>({p});");
    let message = full.1.strip_prefix(&prefix)?.strip_suffix(&suffix)?;
    if message.contains('"') {
        return None;
    }
    Some(Construction::EmptyOrNonEmpty { field: f, param: full.0[0].clone(), message: message.to_string() })
}

/// `()` calling `this(Class.CONSTANT)` and `(T p)` storing `p` in a field.
fn delegating(ctors: &[(Vec<JParam>, String)], fields: &[&str]) -> Option<Construction> {
    if ctors.len() != 2 {
        return None;
    }
    let (empty, full) = if ctors[0].0.is_empty() {
        (&ctors[0], &ctors[1])
    } else {
        (&ctors[1], &ctors[0])
    };
    let [param] = full.0.as_slice() else { return None };
    let call = empty.1.strip_prefix("this(")?.strip_suffix(");")?;
    let (class, constant) = call.split_once('.')?;
    let full_stmts = statements(&full.1);
    let [assign] = full_stmts.as_slice() else { return None };
    let (field, rhs) = field_rhs(assign, fields)?;
    (rhs == param.name && class == param.ty && constant.chars().all(|c| c.is_uppercase() || c == '_')).then(|| {
        Construction::Delegating { field, param: param.clone(), default: (class.to_string(), constant.to_string()) }
    })
}

/// The terms of a `toString` concatenation, grouped by the Java source line
/// they stand on (the layout keeps Java's grouping).
fn parse_to_string(stmts: &[String], raw: &str, fields: &[&str]) -> Result<Vec<Vec<Term>>, String> {
    let [s] = stmts else { return Err("toString body".into()) };
    s.strip_prefix("return ").ok_or("toString body")?;
    let mut atoms = Vec::new();
    for line in raw.lines() {
        let line = line.trim().trim_start_matches("return ").trim_end_matches(';');
        let mut terms = Vec::new();
        for t in split_top(line, '+') {
            if t.starts_with('"') && t.ends_with('"') {
                terms.push(Term::Lit(t.clone()));
            } else if t.len() == 3 && t.starts_with('\'') && t.ends_with('\'') {
                terms.push(Term::Lit(format!("\"{}\"", &t[1..2])));
            } else if fields.contains(&t.trim_start_matches("this.")) {
                terms.push(Term::Field(t.trim_start_matches("this.").to_string()));
            } else {
                return Err(format!("toString term `{t}`"));
            }
        }
        if !terms.is_empty() {
            atoms.push(terms);
        }
    }
    Ok(atoms)
}

fn parse_equals(stmts: &[String], o: &str, class: &str, fields: &[&str]) -> Result<Vec<String>, String> {
    let expected = [
        format!("if (this == {o}) return true"),
        format!("if ({o} == null || getClass() != {o}.getClass()) return false"),
        format!("{class} that = ({class}) {o}"),
    ];
    if stmts.len() != 4 || stmts[..3] != expected {
        return Err("equals body".into());
    }
    let e = stmts[3].strip_prefix("return ").ok_or("equals body")?;
    let mut out = Vec::new();
    for t in split_top(&e.replace("&&", "\u{1}"), '\u{1}') {
        let f = if let Some(inner) = t.strip_prefix("Objects.equals(").and_then(|r| r.strip_suffix(')')) {
            let (a, b) = inner.split_once(", ").ok_or("equals term")?;
            (b == format!("that.{a}")).then_some(a.to_string())
        } else {
            t.split_once(" == ")
                .and_then(|(a, b)| (b == format!("that.{a}")).then(|| a.to_string()))
        };
        match f {
            Some(f) if fields.contains(&f.as_str()) => out.push(f),
            _ => return Err(format!("equals term `{t}`")),
        }
    }
    Ok(out)
}

fn parse_hash(stmts: &[String], fields: &[&str]) -> Result<Vec<String>, String> {
    let [s] = stmts else { return Err("hashCode body".into()) };
    let inner = s
        .strip_prefix("return Objects.hash(")
        .and_then(|r| r.strip_suffix(')'))
        .ok_or("hashCode body")?;
    let out: Vec<String> = split_top(inner, ',');
    if out.iter().any(|f| !fields.contains(&f.as_str())) {
        return Err(format!("hashCode fields `{inner}`"));
    }
    Ok(out)
}

fn parse_factory(stmts: &[String], class: &str) -> Result<Vec<(String, Vec<Elem>)>, String> {
    let [s] = stmts else { return Err("factory body".into()) };
    let mut rest = s.strip_prefix(&format!("return new {class}()")).ok_or("factory body")?;
    let mut chain = Vec::new();
    while let Some(r) = rest.strip_prefix('.') {
        let open = r.find('(').ok_or("factory call")?;
        let setter = r[..open].to_string();
        let close = super::matching(r.as_bytes(), open).map_err(|e| e.to_string())?;
        let arg = &r[open + 1..close - 1];
        let elems = arg
            .strip_prefix("Set.of(")
            .and_then(|a| a.strip_suffix(')'))
            .ok_or(format!("factory argument `{arg}`"))?;
        let mut out = Vec::new();
        for e in split_top(elems, ',') {
            if e.starts_with('"') {
                out.push(Elem::Str(e));
            } else if let Some((c, k)) = e.split_once('.') {
                out.push(Elem::Constant(c.to_string(), k.to_string()));
            } else {
                return Err(format!("factory element `{e}`"));
            }
        }
        chain.push((setter, out));
        rest = &r[close..];
    }
    if !rest.is_empty() {
        return Err(format!("factory tail `{rest}`"));
    }
    Ok(chain)
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Dir {
    In,
    Out,
}

/// The Python module a Java type lives in (CLAUDE.md, Python Binding
/// Conventions, Modules): an admin type by its file, any other by its package.
fn py_module(fqcn: &str) -> Option<String> {
    let (pkg, simple) = fqcn.rsplit_once('.')?;
    if pkg == "org.apache.kafka.clients.admin" {
        return Some(format!("confluent_kafka.admin.{}", snake(simple)));
    }
    if pkg.contains(".internals") {
        return None;
    }
    if pkg == "org.apache.kafka.common.config" || pkg.starts_with("org.apache.kafka.common.config.") {
        return Some("confluent_kafka.common.config".into());
    }
    if pkg.starts_with("org.apache.kafka.common.serialization") {
        return Some("confluent_kafka.common.serialization".into());
    }
    if pkg == "org.apache.kafka.common" || pkg.starts_with("org.apache.kafka.common.") {
        return Some("confluent_kafka.common".into());
    }
    None
}

struct Types<'a> {
    class: &'a JClass,
    /// Every non-builtin type used: (module, name).
    used: BTreeSet<(String, String)>,
}

impl Types<'_> {
    fn base(&mut self, ty: &str) -> Result<String, String> {
        Ok(match ty {
            "boolean" | "Boolean" => "bool".into(),
            "int" | "Integer" | "long" | "Long" | "short" | "Short" | "byte" | "Byte" => "int".into(),
            "double" | "Double" | "float" | "Float" => "float".into(),
            "String" => "str".into(),
            simple if simple.chars().all(|c| c.is_alphanumeric()) && simple.starts_with(char::is_uppercase) => {
                let fqcn = self.class.resolve(simple);
                let module = py_module(&fqcn).ok_or(format!("type `{simple}` ({fqcn}) has no Python module"))?;
                self.used.insert((module, simple.to_string()));
                simple.to_string()
            },
            other => return Err(format!("type `{other}`")),
        })
    }

    fn py(&mut self, ty: &str, dir: Dir) -> Result<String, String> {
        if let Some((outer, inner)) = ty.split_once('<') {
            let inner = inner.strip_suffix('>').ok_or(format!("type `{ty}`"))?;
            let e = self.base(inner.trim())?;
            return Ok(match (outer, dir) {
                ("List", Dir::In) => format!("Sequence[{e}]"),
                ("List", Dir::Out) => format!("list[{e}]"),
                ("Set" | "Collection", Dir::In) => format!("Iterable[{e}]"),
                ("Set", Dir::Out) => format!("set[{e}]"),
                ("Optional", Dir::Out) => format!("{e} | None"),
                _ => return Err(format!("type `{ty}`")),
            });
        }
        match ty {
            "OptionalInt" | "OptionalLong" if dir == Dir::Out => Ok("int | None".into()),
            _ => self.base(ty),
        }
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Uses {
    any: bool,
    unset: bool,
    /// A setter takes a collection of `String`: `reject_str` guards it.
    reject_str: bool,
    java_str: bool,
    copy_of: bool,
    illegal_argument: bool,
    iterable_py: bool,
    sequence_py: bool,
    iterable_pyi: bool,
    sequence_pyi: bool,
    overload: bool,
    internal_consts: BTreeMap<String, (String, String)>,
    /// Types imported at module level for use at run time: (module, name).
    top_runtime: BTreeSet<(String, String)>,
}

impl Uses {
    fn note_py(&mut self, s: &str) {
        self.iterable_py |= s.contains("Iterable[");
        self.sequence_py |= s.contains("Sequence[");
    }

    fn note_pyi(&mut self, s: &str) {
        self.iterable_pyi |= s.contains("Iterable[");
        self.sequence_pyi |= s.contains("Sequence[");
    }
}

/// How a field is stored in Python.
#[derive(Clone, Debug)]
struct Storage {
    /// The annotation of the field (`frozenset[GroupState]`, `int | None`).
    ty: String,
    /// The Python value of Java's starting value.
    init: String,
    /// The Java starting value, when Python writes it differently.
    comment: Option<String>,
    frozen: bool,
    nullable: bool,
}

struct Renderer<'a> {
    o: &'a Options,
    parent: &'a Parent,
    prose: &'a Prose,
    repo_root: &'a Path,
    types: Types<'a>,
    uses: Uses,
}

/// Lower-case the first letter of a sentence, unless the word is an acronym.
fn lc_first(s: &str) -> String {
    let mut c = s.chars();
    match (c.next(), c.next()) {
        (Some(a), Some(b)) if a.is_uppercase() && !b.is_uppercase() => {
            let mut out: String = a.to_lowercase().collect();
            out.push_str(&s[a.len_utf8()..]);
            out
        },
        _ => s.to_string(),
    }
}

impl Renderer<'_> {
    fn name(&self) -> &str {
        &self.o.class.name
    }

    fn st(&self, r: String) -> Stop {
        stop(self.name(), r)
    }

    fn field(&self, java: &str) -> &Field {
        self.o
            .fields
            .iter()
            .chain(self.parent.fields.iter())
            .find(|f| f.java == java)
            .expect("known field")
    }

    fn setters_of(&self, java: &str) -> Vec<&Setter> {
        self.o.setters.iter().filter(|s| s.field == java).collect()
    }

    fn getter_of(&self, java: &str) -> Option<&Getter> {
        self.o
            .getters
            .iter()
            .chain(self.parent.getters.iter())
            .find(|g| g.field == java)
    }

    fn storage(&mut self, f: &Field) -> Result<Storage, Stop> {
        let setters: Vec<Store> = self
            .o
            .setters
            .iter()
            .chain(self.parent.setters.iter())
            .filter(|s| s.field == f.java)
            .map(|s| s.store)
            .collect();
        let init = f.init.as_deref();
        let ty = f.ty.as_str();
        let reference = !is_primitive(ty);
        // A `final` field without initializer is set by every constructor.
        let nullable =
            (reference && (init == Some("null") || (init.is_none() && !f.is_final))) || ty.starts_with("Optional");
        let st = |s: &Self, r: String| s.st(r);
        let (value, comment) = match init {
            None | Some("null") if reference => ("None".to_string(), None),
            None if ty == "boolean" => ("False".to_string(), None),
            None => ("0".to_string(), None),
            Some("true") => ("True".to_string(), None),
            Some("false") => ("False".to_string(), None),
            Some("new LinkedList<>()" | "new ArrayList<>()") => ("[]".to_string(), None),
            Some("Set.of()") => ("frozenset()".to_string(), None),
            Some("Collections.emptySet()") => ("set()".to_string(), Some("Collections.emptySet()".to_string())),
            Some("OptionalInt.empty()") => ("None".to_string(), Some("OptionalInt.empty()".to_string())),
            Some(v) if v.trim_end_matches('L').parse::<i64>().is_ok() => (v.trim_end_matches('L').to_string(), None),
            Some(v) => return Err(st(self, format!("field `{}` starts as `{v}`", f.java))),
        };
        let hashed = self.o.items.iter().any(|i| matches!(i, Item::Hash(h) if h.contains(&f.java)));
        let frozen = init == Some("Set.of()") || setters.contains(&Store::CopyOf) || (hashed && ty.starts_with("Set<"));
        let mut pty = if ty.starts_with("OptionalInt") || ty.starts_with("OptionalLong") {
            "int".to_string()
        } else {
            self.types.py(ty, Dir::Out).map_err(|r| st(self, r))?
        };
        if frozen {
            pty = pty.replacen("set[", "frozenset[", 1);
            let (value, comment) = if value == "set()" {
                ("frozenset()".to_string(), None)
            } else {
                (value, comment)
            };
            return Ok(Storage { ty: pty, init: value, comment, frozen, nullable });
        }
        if nullable {
            pty.push_str(" | None");
        }
        Ok(Storage { ty: pty, init: value, comment, frozen, nullable })
    }

    /// CLAUDE.md, Signatures, Nullability: a Javadoc "Non-null" wins; else the
    /// argument is nullable when the Javadoc allows null, the body has a null
    /// branch (`Set.copyOf` ternary) or wraps it in `Optional.ofNullable`, or it
    /// is stored in a field that holds null until set and Java tests for null.
    fn param_nullable(&self, s: &Setter) -> bool {
        if is_primitive(&s.param.ty) || s.doc_null == Some(false) {
            return false;
        }
        let f = self.field(&s.field);
        let starts_null = matches!(f.init.as_deref(), None | Some("null")) && !f.is_final;
        s.doc_null == Some(true)
            || s.store == Store::CopyOf
            || self.getter_of(&s.field).is_some_and(|g| g.body == GetBody::OfNullable)
            || (starts_null && s.null_tested)
    }

    /// The setter's argument type: `(base, nullable)`.
    fn param_type(&mut self, s: &Setter) -> Result<(String, bool), Stop> {
        let base = self.types.py(&s.param.ty, Dir::In).map_err(|r| self.st(r))?;
        Ok((base, self.param_nullable(s)))
    }

    fn getter_type(&mut self, g: &Getter) -> Result<String, Stop> {
        let mut t = self.types.py(&g.ret, Dir::Out).map_err(|r| self.st(r))?;
        if g.body == GetBody::IsEmpty {
            return Ok(t);
        }
        let f = self.field(&g.field).clone();
        let storage = self.storage(&f)?;
        if storage.nullable && !t.ends_with("| None") {
            t.push_str(" | None");
        }
        Ok(t)
    }

    /// The `reject_str` guard of a setter whose Java parameter is a collection of
    /// `String` (CLAUDE.md, Python Binding Conventions, Signatures): a `str` would be
    /// iterated by character. A nullable parameter keeps `None` as "not set".
    fn str_guard(&mut self, s: &Setter, nullable: bool) -> Vec<String> {
        let p = snake(&s.param.name);
        if !matches!(s.param.ty.as_str(), "Set<String>" | "Collection<String>" | "List<String>") {
            return Vec::new();
        }
        self.uses.reject_str = true;
        if nullable {
            vec![
                format!("        if {p} is not None:"),
                format!("            reject_str(\"{p}\", {p})"),
            ]
        } else {
            vec![format!("        reject_str(\"{p}\", {p})")]
        }
    }

    /// The setter's Python store expression and its comment.
    fn store_expr(&mut self, s: &Setter) -> Result<(String, Option<String>), Stop> {
        let p = snake(&s.param.name);
        let f = self.field(&s.field).clone();
        let storage = self.storage(&f)?;
        let nullable = self.param_nullable(s);
        let wrap = |conv: &str| {
            if nullable {
                format!("None if {p} is None else {conv}({p})")
            } else {
                format!("{conv}({p})")
            }
        };
        Ok(match s.store {
            Store::AsGiven if s.param.ty.starts_with("List<") => (wrap("list"), None),
            Store::AsGiven if s.param.ty.starts_with("Set<") || s.param.ty.starts_with("Collection<") => {
                (wrap(if storage.frozen { "frozenset" } else { "set" }), None)
            },
            Store::AsGiven => (p, None),
            Store::OptionalOf => (p, Some(s.rhs.clone())),
            Store::NewHashSet => (wrap(if storage.frozen { "frozenset" } else { "set" }), None),
            Store::CopyOf => {
                self.uses.copy_of = true;
                (format!("_copy_of({p})"), None)
            },
        })
    }

    fn return_type(&self, s: &Setter, stub: bool) -> String {
        match s.ret {
            Ret::Void => "None".into(),
            Ret::SelfType if stub => "Self".into(),
            Ret::SelfType => "Any".into(),
            Ret::This => self.name().to_string(),
        }
    }

    /// The keyword constructor's keywords: (field, setter), the parent's first.
    fn keywords(&self) -> Result<Vec<(Field, Setter)>, Stop> {
        let mut out = Vec::new();
        for f in self.parent.fields.iter() {
            let s = self.parent.setters.iter().find(|s| s.field == f.java);
            if let Some(s) = s {
                out.push((f.clone(), s.clone()));
            }
        }
        for f in &self.o.fields {
            let all = self.setters_of(&f.java);
            let chosen: Vec<&&Setter> = all.iter().filter(|s| s.name == f.java).collect();
            let s = match (chosen.as_slice(), all.as_slice()) {
                ([s], _) => (**s).clone(),
                ([], []) => continue,
                ([], [s]) => (*s).clone(),
                _ => return Err(self.st(format!("several setters write `{}`", f.java))),
            };
            out.push((f.clone(), s));
        }
        // Dependency order: a setter that reads another field would run after
        // its writers; the known setter patterns read no field.
        Ok(out)
    }

    fn stub_default(&mut self, f: &Field, s: &Setter) -> Result<String, Stop> {
        let storage = self.storage(f)?;
        let nullable = self.param_nullable(s);
        Ok(match storage.init.as_str() {
            _ if storage.comment.as_deref() == Some("OptionalInt.empty()") => "...".into(),
            "None" if nullable => "None".into(),
            "None" => "...".into(),
            "[]" | "set()" | "frozenset()" => "()".into(),
            v => v.to_string(),
        })
    }

    // -- docstrings ----------------------------------------------------------

    fn prose_or(&self, slot: &str, indent: usize, make: impl FnOnce() -> Option<String>) -> Option<String> {
        match self.prose.get(self.name(), slot) {
            Some(lines) => Some(indent_literal(lines, indent)),
            None => make(),
        }
    }

    fn doc_fields(doc: &Doc) -> Vec<String> {
        let mut out: Vec<String> = doc.params.iter().map(|(n, t)| format!(":param {}: {t}", snake(n))).collect();
        if let Some(r) = &doc.returns {
            out.push(format!(":return: {r}"));
        }
        out
    }

    fn plain_doc(&self, slot: &str, doc: &Doc) -> Option<String> {
        self.prose_or(slot, 8, || {
            (!doc.is_empty()).then(|| docstring(8, &doc.paragraphs, &Self::doc_fields(doc)))
        })
    }

    fn merged_doc(&self, py: &str, s: &Setter, g: Option<&Getter>) -> Option<String> {
        let p = snake(&s.param.name);
        let slot = format!("method {py}");
        self.prose_or(&slot, 8, || {
            let ret = if s.ret == Ret::Void {
                "return ``None`` (Java's ``void``)"
            } else {
                "return this object"
            };
            // One sentence takes the return clause; several get it as a sentence.
            let first = match s.doc.paragraphs.first() {
                None => format!("set it and {ret}."),
                Some(t) if t.trim_end_matches('.').contains(". ") => {
                    let mut r = ret.to_string();
                    r.replace_range(..1, &r[..1].to_uppercase());
                    format!("{} {r}.", lc_first(t))
                },
                Some(t) => format!("{}, and {ret}.", lc_first(t.trim_end_matches('.'))),
            };
            let mut paras = vec![format!("With ``{p}``: {first}")];
            paras.extend(s.doc.paragraphs.iter().skip(1).cloned());
            let gdoc = g.map(|g| g.doc.clone()).unwrap_or_default();
            match gdoc.paragraphs.first() {
                Some(t) => paras.push(format!("Without: {}", lc_first(t))),
                None => paras.push("Without: the current value.".to_string()),
            }
            paras.extend(gdoc.paragraphs.iter().skip(1).cloned());
            Some(docstring(8, &paras, &Self::doc_fields(&s.doc)))
        })
    }

    fn module_doc(&self) -> String {
        let title = module_title(self.name(), &self.o.class.fqcn());
        let body = match self.prose.get(self.name(), "module") {
            Some(lines) => lines.join("\n"),
            None => {
                let mut paras = Vec::new();
                let own = self.o.fields.iter().any(|f| !self.setters_of(&f.java).is_empty());
                match &self.o.construction {
                    Construction::Keywords if own => paras.push(
                        "Java builds it with setters, so the constructor also takes one keyword per setter field, \
                         the parent's ``timeout_ms`` first, then the class's own in Java's field order (CLAUDE.md, \
                         Python Binding Conventions, Signatures). Each keyword given, ``None`` included, calls its \
                         setter; one left out leaves the field at Java's starting value."
                            .to_string(),
                    ),
                    Construction::Keywords => paras.push(
                        "Java builds it with setters, so the constructor also takes one keyword per setter field, \
                         here only the inherited ``timeout_ms`` (CLAUDE.md, Python Binding Conventions, \
                         Signatures). A keyword left out leaves its setter uncalled."
                            .to_string(),
                    ),
                    Construction::Abstract => {},
                    Construction::EmptyOrNonEmpty { .. } | Construction::Delegating { .. } => paras.push(
                        "Java has a constructor that takes arguments, so the class keeps only Java's constructors \
                         (CLAUDE.md, Python Binding Conventions, Signatures)."
                            .to_string(),
                    ),
                }
                let mut extra = Vec::new();
                if self.o.items.iter().any(|i| matches!(i, Item::Merged(_, Some(_)))) {
                    extra.push(
                        "A setter sharing its name with its getter is one method with two stubs: no argument \
                         reads, one argument sets (Signatures).",
                    );
                }
                if self.o.redeclares {
                    extra.push(
                        "Java re-declares ``timeoutMs`` (binary compatibility with 0.11) with its own return type; \
                         so does this class.",
                    );
                }
                let has = |f: fn(&Item) -> bool| self.o.items.iter().any(f);
                if has(|i| matches!(i, Item::Eq(_))) {
                    extra.push(
                        "Java's ``equals`` / ``hashCode`` / ``toString`` are ``__eq__`` / ``__hash__`` / \
                         ``__str__`` (Idiom translations).",
                    );
                } else if has(|i| matches!(i, Item::Str(_))) {
                    extra.push("Java's ``toString`` is ``__str__`` (Idiom translations).");
                }
                if !extra.is_empty() {
                    match paras.last_mut() {
                        Some(last) => {
                            for e in extra {
                                last.push(' ');
                                last.push_str(e);
                            }
                        },
                        None => paras.push(extra.join(" ")),
                    }
                }
                if !self.o.deprecated.is_empty() {
                    let names: Vec<String> = self.o.deprecated.iter().map(|d| format!("``{d}``")).collect();
                    paras.push(format!("Not generated (deprecated): {}.", names.join(", ")));
                }
                // The body follows the title line; the closing quotes stand alone.
                let body: Vec<String> = paras.iter().map(|p| wrap_words(p, 0, "").join("\n")).collect();
                format!("{}\n\"\"\"", body.join("\n\n"))
            },
        };
        format!("{title}\n\n{body}")
    }

    fn class_doc(&self) -> String {
        self.prose_or("class", 4, || {
            let doc = self.o.class.javadoc.as_deref().map(read_doc).unwrap_or_default();
            let mut paras = vec![doc.paragraphs.join(" ")];
            paras.retain(|p| !p.is_empty());
            let suffix = if self.o.class.is_abstract() {
                " (``abstract``)"
            } else {
                ""
            };
            paras.push(format!("Java: ``{}``{suffix}.", self.o.class.fqcn()));
            Some(docstring(4, &paras, &[]))
        })
        .unwrap_or_default()
    }

    // -- members -------------------------------------------------------------

    fn slots(&self) -> String {
        let mut names: Vec<String> = self.o.fields.iter().map(|f| format!("\"_{}\"", f.py)).collect();
        if self.o.class.is_abstract() {
            names.push("\"_setters\"".into());
        }
        match names.as_slice() {
            [] => "    __slots__ = ()".to_string(),
            [one] => format!("    __slots__ = ({one},)"),
            _ => wrap_signature(4, "__slots__ = (", &names, ")", 4, 0),
        }
    }

    fn init_fields(&mut self, skip: Option<&str>) -> Result<Vec<String>, Stop> {
        let mut out = Vec::new();
        for f in self.o.fields.clone() {
            if Some(f.java.as_str()) == skip {
                continue;
            }
            let s = self.storage(&f)?;
            if self.o.class.is_abstract() {
                out.push(format!("        # Java's field initializer: ``{}``.", f.decl));
            }
            let literal = !matches!(s.init.as_str(), "None" | "[]" | "set()" | "frozenset()");
            let ann = if literal && !s.nullable {
                String::new()
            } else {
                format!(": {}", s.ty)
            };
            let comment = s.comment.map(|c| format!("  # {c}")).unwrap_or_default();
            out.push(format!("        self._{}{ann} = {}{comment}", f.py, s.init));
        }
        Ok(out)
    }

    fn init_py(&mut self) -> Result<Option<String>, Stop> {
        match self.o.construction.clone() {
            Construction::Abstract => {
                self.uses.any = true;
                let mut lines = vec![
                    "    def __init__(self, *args: Any, **kwargs: Any) -> None:".to_string(),
                    format!("        if type(self) is {}:", self.name()),
                    format!(
                        "            raise TypeError(\"{} is an abstract class; use one of its subclasses\")",
                        self.name()
                    ),
                ];
                lines.extend(self.init_fields(None)?);
                lines.push(
                    "        # The setters that ran, the latest last (``_record``): the client runs the same\n        \
                     # ones on the C options (CLAUDE.md conversion table, \"an Options object\").\n        \
                     self._setters: dict[str, Any] = {}"
                        .into(),
                );
                Ok(Some(lines.join("\n")))
            },
            Construction::Keywords => {
                let kws = self.keywords()?;
                let mut items = vec!["self".to_string(), "*".to_string()];
                let mut calls = Vec::new();
                for (f, s) in &kws {
                    let (base, nullable) = self.param_type(s)?;
                    let none = if nullable { " | None" } else { "" };
                    items.push(format!("{}: {base}{none} = UNSET", f.py));
                    let call = wrap_signature(
                        12,
                        &format!("self.{}(", snake(&s.name)),
                        &[format!("{}={}", snake(&s.param.name), f.py)],
                        ")",
                        4,
                        0,
                    );
                    calls.push(format!("        if {} is not UNSET:\n{call}", f.py));
                }
                self.uses.unset = true;
                let head = wrap_signature(4, "def __init__(", &items, ") -> None:", 8, 0);
                self.uses.note_py(&head);
                let mut lines = vec![head, "        super().__init__()".to_string()];
                lines.extend(self.init_fields(None)?);
                lines.extend(calls);
                Ok(Some(lines.join("\n")))
            },
            Construction::Delegating { field, param, default: (c, k) } => {
                let pt = self.types.py(&param.ty, Dir::In).map_err(|r| self.st(r))?;
                let fqcn = self.o.class.resolve(&c);
                let module = py_module(&fqcn).ok_or_else(|| self.st(format!("type `{c}`")))?;
                let file = if module.starts_with("confluent_kafka.admin.") {
                    module
                } else {
                    format!("{module}.{}", snake(&c))
                };
                self.uses.top_runtime.insert((file, c.clone()));
                let p = snake(&param.name);
                let items = ["self".to_string(), "*".to_string(), format!("{p}: {pt} = {c}.{k}")];
                let head = wrap_signature(4, "def __init__(", &items, ") -> None:", 8, 0);
                let mut lines = vec![head, "        super().__init__()".to_string()];
                lines.extend(self.init_fields(Some(&field))?);
                lines.push(format!("        self._{} = {p}", self.field(&field).py));
                Ok(Some(lines.join("\n")))
            },
            Construction::EmptyOrNonEmpty { field, param, message } => {
                let pt = self.types.py(&param.ty, Dir::In).map_err(|r| self.st(r))?;
                let p = snake(&param.name);
                let f = self.field(&field).clone();
                let elem = self.types.py(&f.ty, Dir::Out).map_err(|r| self.st(r))?;
                let head = format!("    def __init__(self, *, {p}: {pt} | None = None) -> None:");
                self.uses.note_py(&head);
                self.uses.illegal_argument = true;
                let mut lines = vec![head, "        super().__init__()".to_string()];
                lines.extend(self.init_fields(Some(&field))?);
                lines.extend([
                    format!("        if {p} is None:"),
                    format!("            self._{}: {elem} = set()  # Collections.emptySet()", f.py),
                    "            return".to_string(),
                    format!("        copied = set({p})"),
                    "        if not copied:".to_string(),
                    format!("            raise IllegalArgumentError(message=\"{message}\")"),
                    format!("        self._{} = copied", f.py),
                ]);
                Ok(Some(lines.join("\n")))
            },
        }
    }

    fn init_pyi(&mut self) -> Result<Option<String>, Stop> {
        match self.o.construction.clone() {
            Construction::Abstract => Ok(None),
            Construction::Keywords => {
                let kws = self.keywords()?;
                let mut items = vec!["self".to_string(), "*".to_string()];
                for (f, s) in &kws {
                    let (base, nullable) = self.param_type(s)?;
                    let none = if nullable { " | None" } else { "" };
                    let d = self.stub_default(f, s)?;
                    items.push(format!("{}: {base}{none} = {d}", f.py));
                }
                let s = wrap_signature(4, "def __init__(", &items, ") -> None: ...", 8, STUB_SLACK);
                self.uses.note_pyi(&s);
                Ok(Some(s))
            },
            Construction::Delegating { param, default: (c, k), .. } => {
                let pt = self.types.py(&param.ty, Dir::In).map_err(|r| self.st(r))?;
                let items = [
                    "self".to_string(),
                    "*".to_string(),
                    format!("{}: {pt} = {c}.{k}", snake(&param.name)),
                ];
                Ok(Some(wrap_signature(
                    4,
                    "def __init__(",
                    &items,
                    ") -> None: ...",
                    8,
                    STUB_SLACK,
                )))
            },
            Construction::EmptyOrNonEmpty { param, .. } => {
                let pt = self.types.py(&param.ty, Dir::In).map_err(|r| self.st(r))?;
                let s = format!(
                    "    def __init__(self, *, {}: {pt} | None = None) -> None: ...",
                    snake(&param.name)
                );
                self.uses.note_pyi(&s);
                Ok(Some(s))
            },
        }
    }

    fn getter_body(&mut self, g: &Getter) -> Result<String, Stop> {
        let f = self.field(&g.field).clone();
        let storage = self.storage(&f)?;
        Ok(match g.body {
            GetBody::IsEmpty => format!("return not self._{}", f.py),
            GetBody::OfNullable => format!("return self._{}  # Optional.ofNullable({})", f.py, f.java),
            GetBody::Field if storage.frozen => format!("return set(self._{})", f.py),
            GetBody::Field => format!("return self._{}", f.py),
        })
    }

    fn comments(s: &Setter) -> Vec<String> {
        s.comments.iter().map(|c| format!("        # {c}")).collect()
    }

    fn item_py(&mut self, item: &Item) -> Result<String, Stop> {
        match item {
            Item::Merged(si, gi) => {
                let s = self.o.setters[*si].clone();
                let g = match gi {
                    Some(g) => Some(self.o.getters[*g].clone()),
                    None => self.parent.getters.iter().find(|g| g.name == s.name).cloned(),
                };
                let py = snake(&s.name);
                let p = snake(&s.param.name);
                let (base, nullable) = self.param_type(&s)?;
                let default = if nullable { "UNSET" } else { "None" };
                self.uses.any = true;
                self.uses.unset |= nullable;
                let head = wrap_signature(
                    4,
                    &format!("def {py}("),
                    &[
                        "self".to_string(),
                        "*".to_string(),
                        format!("{p}: {base} | None = {default}"),
                    ],
                    ") -> Any:",
                    8,
                    0,
                );
                self.uses.note_py(&head);
                let mut lines = vec![head];
                lines.extend(self.merged_doc(&py, &s, g.as_ref()));
                lines.extend(Self::comments(&s));
                let read = match &g {
                    Some(g) => self.getter_body(g)?,
                    None => format!("return self._{}", self.field(&s.field).py),
                };
                let (store, comment) = self.store_expr(&s)?;
                let comment = comment.map(|c| format!("  # {c}")).unwrap_or_default();
                lines.push(format!("        if {p} is {default}:"));
                lines.push(format!("            {read}"));
                lines.extend(self.str_guard(&s, nullable));
                lines.push(format!("        self._{} = {store}{comment}", self.field(&s.field).py));
                // What the setter stored (Java's copy of a collection, `Set.copyOf`,
                // `new HashSet<>(x)`): a one-shot iterator crosses as that copy.
                lines.push(format!("        self._record(\"{py}\", self._{})", self.field(&s.field).py));
                lines.push(if s.ret == Ret::Void {
                    "        return None".into()
                } else {
                    "        return self".into()
                });
                Ok(lines.join("\n"))
            },
            Item::Setter(si) => {
                let s = self.o.setters[*si].clone();
                let py = snake(&s.name);
                let p = snake(&s.param.name);
                let (base, nullable) = self.param_type(&s)?;
                let none = if nullable { " | None" } else { "" };
                let ret = self.return_type(&s, false);
                let head = wrap_signature(
                    4,
                    &format!("def {py}("),
                    &["self".to_string(), "*".to_string(), format!("{p}: {base}{none}")],
                    &format!(") -> {ret}:"),
                    8,
                    0,
                );
                self.uses.note_py(&head);
                let mut lines = vec![head];
                lines.extend(self.plain_doc(&format!("method {py}"), &s.doc));
                lines.extend(Self::comments(&s));
                let (store, comment) = self.store_expr(&s)?;
                let comment = comment.map(|c| format!("  # {c}")).unwrap_or_default();
                lines.extend(self.str_guard(&s, nullable));
                lines.push(format!("        self._{} = {store}{comment}", self.field(&s.field).py));
                // What the setter stored (Java's copy of a collection, `Set.copyOf`,
                // `new HashSet<>(x)`): a one-shot iterator crosses as that copy.
                lines.push(format!("        self._record(\"{py}\", self._{})", self.field(&s.field).py));
                if s.ret != Ret::Void {
                    lines.push("        return self".into());
                }
                Ok(lines.join("\n"))
            },
            Item::Getter(gi) => {
                let g = self.o.getters[*gi].clone();
                let py = snake(&g.name);
                let t = self.getter_type(&g)?;
                let mut lines = vec![format!("    def {py}(self) -> {t}:")];
                lines.extend(self.plain_doc(&format!("method {py}"), &g.doc));
                lines.push(format!("        {}", self.getter_body(&g)?));
                Ok(lines.join("\n"))
            },
            Item::Factory(fi) => {
                let f = self.o.factories[*fi].clone();
                let py = snake(&f.name);
                let cls = self.name().to_string();
                let mut lines = vec!["    @staticmethod".to_string(), format!("    def {py}() -> {cls}:")];
                lines.extend(self.plain_doc(&format!("method {py}"), &f.doc));
                let mut runtime: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
                let mut calls = Vec::new();
                for (setter, elems) in &f.chain {
                    let s = self.o.setters.iter().find(|s| &s.name == setter).cloned();
                    let s = s.ok_or_else(|| self.st(format!("factory calls unknown setter `{setter}`")))?;
                    let mut vals = Vec::new();
                    for e in elems {
                        vals.push(match e {
                            Elem::Str(s) => s.clone(),
                            Elem::Constant(c, k) => {
                                let fqcn = self.o.class.resolve(c);
                                match py_module(&fqcn) {
                                    Some(m) => {
                                        runtime.entry(m.clone()).or_default().insert(c.clone());
                                        self.types.used.insert((m, c.clone()));
                                        format!("{c}.{k}")
                                    },
                                    None => self.internal_const(c, k, &fqcn)?,
                                }
                            },
                        });
                    }
                    calls.push(format!(".{}({}={{{}}})", snake(setter), snake(&s.param.name), vals.join(", ")));
                }
                for (m, names) in &runtime {
                    let names: Vec<&str> = names.iter().map(String::as_str).collect();
                    lines.push(format!("        from {m} import {}", names.join(", ")));
                }
                if !runtime.is_empty() {
                    lines.push(String::new());
                }
                let one = format!("        return {cls}(){}", calls.concat());
                if one.chars().count() <= WIDTH {
                    lines.push(one);
                } else {
                    let pad = " ".repeat(16);
                    let mut chain = vec![format!("        return ({cls}()")];
                    chain.extend(calls.iter().map(|c| format!("{pad}{c}")));
                    let last = chain.pop().unwrap_or_default();
                    chain.push(format!("{last})"));
                    lines.extend(chain);
                }
                Ok(lines.join("\n"))
            },
            Item::Str(atoms) => {
                self.uses.java_str = true;
                let mut lines = vec!["    def __str__(self) -> str:".to_string()];
                let mut out_atoms = Vec::new();
                let mut first = true;
                for atom in atoms {
                    let mut pieces = Vec::new();
                    for t in atom {
                        let text = match t {
                            Term::Lit(l) => l.clone(),
                            Term::Field(f) => {
                                let fld = self.field(f).clone();
                                if fld.ty == "OptionalInt" || fld.ty == "OptionalLong" {
                                    // Java's `OptionalInt.toString()`, in a local.
                                    let k = &fld.ty;
                                    let p = &fld.py;
                                    let head = format!("        {p} = (\"{k}.empty\" if self._{p} is None");
                                    let tail = format!("else \"{k}[\" + java_str(self._{p}) + \"]\")");
                                    let one = format!("{head} {tail}");
                                    if one.chars().count() <= WIDTH {
                                        lines.push(one);
                                    } else {
                                        let pad = " ".repeat(8 + p.len() + 4);
                                        lines.push(format!("{head}\n{pad}{tail}"));
                                    }
                                    p.clone()
                                } else {
                                    format!("java_str(self._{})", fld.py)
                                }
                            },
                        };
                        let lead = if first { String::new() } else { "+ ".to_string() };
                        first = false;
                        pieces.push(Piece { lead, text });
                    }
                    out_atoms.push(pieces);
                }
                lines.push(wrap_expression("        return ", &out_atoms));
                Ok(lines.join("\n"))
            },
            Item::Eq(fields) => {
                let atoms: Vec<Vec<Piece>> = fields
                    .iter()
                    .enumerate()
                    .map(|(k, f)| {
                        let p = &self.field(f).py;
                        let lead = if k == 0 { String::new() } else { "and ".to_string() };
                        vec![
                            Piece { lead, text: format!("self._{p}") },
                            Piece { lead: "== ".to_string(), text: format!("other._{p}") },
                        ]
                    })
                    .collect();
                let cls = self.name();
                Ok([
                    "    def __eq__(self, other: object) -> bool:".to_string(),
                    "        if self is other:".to_string(),
                    "            return True".to_string(),
                    "        if type(other) is not type(self):".to_string(),
                    "            return NotImplemented".to_string(),
                    format!("        assert isinstance(other, {cls})"),
                    wrap_expression("        return ", &atoms),
                ]
                .join("\n"))
            },
            Item::Hash(fields) => {
                let mut vals = Vec::new();
                for f in fields {
                    let fld = self.field(f).clone();
                    let s = self.storage(&fld)?;
                    vals.push(if s.ty.starts_with("set[") {
                        format!("frozenset(self._{})", fld.py)
                    } else if s.ty.starts_with("list[") {
                        format!("tuple(self._{})", fld.py)
                    } else {
                        format!("self._{}", fld.py)
                    });
                }
                let body = if vals.len() == 1 {
                    format!("        return hash(({},))", vals[0])
                } else {
                    wrap_signature(8, "return hash((", &vals, "))", 4, 0)
                };
                Ok(format!("    def __hash__(self) -> int:\n{body}"))
            },
        }
    }

    fn internal_const(&mut self, class: &str, constant: &str, fqcn: &str) -> Result<String, Stop> {
        let rel = format!("{}.java", fqcn.replace('.', "/"));
        let path = self.repo_root.join(JAVA_ROOT).join(&rel);
        let src = fs::read_to_string(&path).map_err(|e| self.st(format!("{rel}: {e}")))?;
        let needle = format!(" {constant} = ");
        let (line_no, line) = src
            .lines()
            .enumerate()
            .find(|(_, l)| l.contains(&needle) && l.contains("static final"))
            .ok_or_else(|| self.st(format!("{class}.{constant} not found in {rel}")))?;
        let value = line
            .split_once(&needle)
            .map(|(_, v)| v.trim().trim_end_matches(';').trim())
            .unwrap_or("");
        if !(value.starts_with('"') || value.parse::<i64>().is_ok()) {
            return Err(self.st(format!("{class}.{constant} = `{value}`")));
        }
        let a: Vec<String> = snake(class).to_uppercase().split('_').map(str::to_string).collect();
        let b: Vec<String> = constant.split('_').map(str::to_string).collect();
        let overlap = (1..=a.len().min(b.len()))
            .rev()
            .find(|&k| a[a.len() - k..] == b[..k])
            .unwrap_or(0);
        let mut words = a.clone();
        words.extend(b[overlap..].iter().cloned());
        let py = format!("_{}", words.join("_"));
        let shown = rel.strip_prefix("org/apache/kafka/clients/").unwrap_or(&rel).to_string();
        let block = format!("# ``{class}.{constant}`` ({shown}:{}).\n{py} = {value}", line_no + 1);
        self.uses.internal_consts.insert(py.clone(), (block, String::new()));
        Ok(py)
    }

    fn item_pyi(&mut self, item: &Item) -> Result<Option<String>, Stop> {
        let cls = self.name().to_string();
        let out = match item {
            Item::Merged(si, gi) => {
                let s = self.o.setters[*si].clone();
                let g = match gi {
                    Some(g) => Some(self.o.getters[*g].clone()),
                    None => self.parent.getters.iter().find(|g| g.name == s.name).cloned(),
                };
                let g = g.ok_or_else(|| self.st(format!("no getter for `{}`", s.name)))?;
                let py = snake(&s.name);
                let p = snake(&s.param.name);
                let (base, nullable) = self.param_type(&s)?;
                let none = if nullable { " | None" } else { "" };
                let gt = self.getter_type(&g)?;
                let ret = self.return_type(&s, true);
                self.uses.overload = true;
                let read = format!("    @overload\n    def {py}(self) -> {gt}: ...");
                let set = wrap_signature(
                    4,
                    &format!("def {py}("),
                    &["self".to_string(), "*".to_string(), format!("{p}: {base}{none}")],
                    &format!(") -> {ret}: ..."),
                    8,
                    STUB_SLACK,
                );
                self.uses.note_pyi(&set);
                format!("{read}\n    @overload\n{set}")
            },
            Item::Setter(si) => {
                let s = self.o.setters[*si].clone();
                let (base, nullable) = self.param_type(&s)?;
                let none = if nullable { " | None" } else { "" };
                let ret = self.return_type(&s, true);
                let set = wrap_signature(
                    4,
                    &format!("def {}(", snake(&s.name)),
                    &[
                        "self".to_string(),
                        "*".to_string(),
                        format!("{}: {base}{none}", snake(&s.param.name)),
                    ],
                    &format!(") -> {ret}: ..."),
                    8,
                    STUB_SLACK,
                );
                self.uses.note_pyi(&set);
                set
            },
            Item::Getter(gi) => {
                let g = self.o.getters[*gi].clone();
                let t = self.getter_type(&g)?;
                format!("    def {}(self) -> {t}: ...", snake(&g.name))
            },
            Item::Factory(fi) => {
                let f = &self.o.factories[*fi];
                format!("    @staticmethod\n    def {}() -> {cls}: ...", snake(&f.name))
            },
            Item::Str(_) => return Ok(None),
            Item::Eq(_) => "    def __eq__(self, other: object) -> bool: ...".to_string(),
            Item::Hash(_) => "    def __hash__(self) -> int: ...".to_string(),
        };
        Ok(Some(out))
    }

    // -- files ---------------------------------------------------------------

    fn render(&mut self) -> Result<(String, String), Stop> {
        let cls = self.name().to_string();
        let abstract_ = self.o.class.is_abstract();
        let mut py_members = vec![self.slots()];
        let mut pyi_members = Vec::new();
        if let Some(i) = self.init_py()? {
            py_members.push(i);
        }
        if let Some(i) = self.init_pyi()? {
            pyi_members.push(i);
        }
        for item in self.o.items.clone() {
            py_members.push(self.item_py(&item)?);
            if let Some(s) = self.item_pyi(&item)? {
                pyi_members.push(s);
            }
        }
        if abstract_ {
            py_members.push(RECORD_PY.to_string());
            pyi_members.push(RECORD_PYI.to_string());
        }
        let base = if abstract_ {
            String::new()
        } else {
            "(AbstractOptions)".to_string()
        };

        // .py
        let mut imp = Imports::default();
        if self.uses.iterable_py || self.uses.copy_of {
            imp.std("collections.abc", "Iterable");
        }
        if self.uses.sequence_py {
            imp.std("collections.abc", "Sequence");
        }
        if self.uses.any {
            imp.std("typing", "Any");
        }
        if self.uses.copy_of {
            imp.std("typing", "TypeVar");
        }
        if self.uses.unset {
            imp.first("confluent_kafka._args", "UNSET");
        }
        if self.uses.reject_str {
            imp.first("confluent_kafka._args", "reject_str");
        }
        if self.uses.java_str {
            imp.first("confluent_kafka._java", "java_str");
        }
        if !abstract_ {
            imp.first("confluent_kafka.admin.abstract_options", "AbstractOptions");
        }
        if self.uses.illegal_argument {
            imp.first("confluent_kafka.illegal_argument_error", "IllegalArgumentError");
        }
        for (m, n) in self.uses.top_runtime.clone() {
            imp.first(&m, &n);
        }
        for (m, n) in self.types.used.clone() {
            if !self.uses.top_runtime.iter().any(|(_, t)| t == &n) {
                imp.checking(&m, &n);
            }
        }
        if !imp.checking.is_empty() {
            imp.std("typing", "TYPE_CHECKING");
        }
        let mut py = format!(
            "{LICENSE}\n{}\n\nfrom __future__ import annotations\n\n{}__all__ = [\"{cls}\"]\n",
            self.module_doc(),
            imp.render("")
        );
        let mut top: Vec<String> = Vec::new();
        if self.uses.copy_of {
            top.push("_E = TypeVar(\"_E\")".into());
        }
        top.extend(self.uses.internal_consts.values().map(|(b, _)| b.clone()));
        for t in &top {
            py.push('\n');
            py.push_str(t);
            py.push('\n');
        }
        if self.uses.copy_of {
            py.push_str(
                "\n\ndef _copy_of(values: Iterable[_E] | None) -> frozenset[_E]:\n    \
                 \"\"\"``(values == null || values.isEmpty()) ? Set.of() : Set.copyOf(values)``.\"\"\"\n    \
                 if values is None:\n        return frozenset()\n    return frozenset(values)\n",
            );
        }
        py.push_str(&format!("\n\nclass {cls}{base}:\n{}\n", self.class_doc()));
        for m in &py_members {
            py.push('\n');
            py.push_str(m);
            py.push('\n');
        }

        // .pyi
        let mut imp = Imports::default();
        let self_block = if abstract_ {
            imp.plain.insert("sys".into());
            "if sys.version_info >= (3, 11):\n    from typing import Self\nelse:\n    from typing_extensions import Self\n\n"
        } else {
            ""
        };
        if self.uses.iterable_pyi {
            imp.std("collections.abc", "Iterable");
        }
        if self.uses.sequence_pyi {
            imp.std("collections.abc", "Sequence");
        }
        if abstract_ {
            imp.std("typing", "Any");
        }
        if self.uses.overload {
            imp.std("typing", "overload");
        }
        if !abstract_ {
            imp.first("confluent_kafka.admin.abstract_options", "AbstractOptions");
        }
        for (m, n) in self.types.used.clone() {
            imp.first(&m, &n);
        }
        let mut pyi = format!(
            "{LICENSE}\n# Stub for confluent_kafka.admin.{cls}.\n\n{}__all__ = [\"{cls}\"]\n\nclass {cls}{base}:\n",
            imp.render(self_block)
        );
        if pyi_members.is_empty() {
            pyi_members.push("    ...".into());
        }
        pyi.push_str(&pyi_members.join("\n"));
        pyi.push('\n');
        Ok((py, pyi))
    }
}

/// `AbstractOptions`' record of the setters that ran: each generated setter
/// calls `_record` with the value it stored, and the client hands
/// `_setters_ran()` to the C extension, which runs the same setters on the C
/// options (CLAUDE.md conversion table, "an Options object").
const RECORD_PY: &str = "    def _record(self, setter: str, value: Any) -> None:
        \"\"\"Record that ``setter`` ran with ``value`` (the latest run last).\"\"\"
        self._setters.pop(setter, None)
        self._setters[setter] = value

    def _setters_ran(self) -> list[tuple[str, Any]]:
        \"\"\"The ``(setter, value)`` pairs of the setters that ran, in the order they
        last ran.\"\"\"
        return list(self._setters.items())";

const RECORD_PYI: &str = "    def _record(self, setter: str, value: Any) -> None: ...
    def _setters_ran(self) -> list[tuple[str, Any]]: ...";

// ---------------------------------------------------------------------------
// Driving
// ---------------------------------------------------------------------------

/// The outcome of one generation run.
pub struct Generated {
    /// (path relative to the repository root, content), `.py` and `.pyi`.
    pub files: Vec<(PathBuf, String)>,
    /// Classes whose Java is outside the known patterns.
    pub stops: Vec<Stop>,
    /// The `# BEGIN OPTIONS` … `# END OPTIONS` section of the package
    /// `__init__.py`, markers included.
    pub init_section: String,
}

fn read_class(repo_root: &Path, name: &str) -> Res<JClass> {
    let path = repo_root.join(ADMIN_JAVA).join(format!("{name}.java"));
    let raw = fs::read_to_string(&path).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
    parse_java_class(&raw).map_err(|e| anyhow::anyhow!("{name}: {e}"))
}

fn render_one(repo_root: &Path, o: &Options, parent: &Parent, prose: &Prose) -> Result<(String, String), Stop> {
    let mut r = Renderer {
        o,
        parent,
        prose,
        repo_root,
        types: Types { class: &o.class, used: BTreeSet::new() },
        uses: Uses::default(),
    };
    r.render()
}

/// Render every class of [`CLASSES`].
pub fn render_all(repo_root: &Path) -> Res<Generated> {
    let known = Known {
        deprecated: deprecated_items(repo_root)?,
        null_tested: null_tested_getters(repo_root)?,
    };
    let prose = Prose::parse(PROSE)?;
    let abstract_options = analyze(read_class(repo_root, "AbstractOptions")?, &Parent::default(), &known)
        .map_err(|s| anyhow::anyhow!("{s}"))?;
    let parent = Parent {
        fields: abstract_options.fields.clone(),
        setters: abstract_options.setters.clone(),
        getters: abstract_options.getters.clone(),
    };
    let mut files = Vec::new();
    let mut stops = Vec::new();
    for name in CLASSES {
        let class = read_class(repo_root, name)?;
        let result = if name == "AbstractOptions" {
            render_one(repo_root, &abstract_options, &Parent::default(), &prose)
        } else {
            analyze(class, &parent, &known).and_then(|o| render_one(repo_root, &o, &parent, &prose))
        };
        match result {
            Ok((py, pyi)) => {
                let base = Path::new(ADMIN_PY).join(snake(name));
                files.push((base.with_extension("py"), py));
                files.push((base.with_extension("pyi"), pyi));
            },
            Err(s) => stops.push(s),
        }
    }
    Ok(Generated { files, stops, init_section: init_section() })
}

fn init_section() -> String {
    let mut classes: Vec<&str> = CLASSES.to_vec();
    classes.sort_by_key(|c| snake(c));
    let mut out = format!("{INIT_BEGIN}\n");
    for c in &classes {
        let line = format!("from .{} import {c} as {c}", snake(c));
        if line.chars().count() <= WIDTH {
            out.push_str(&line);
            out.push('\n');
        } else {
            out.push_str(&format!("from .{} import (\n    {c} as {c},\n)\n", snake(c)));
        }
    }
    let mut names = classes.clone();
    names.sort();
    out.push_str("\n__all__ += [\n");
    for c in names {
        out.push_str(&format!("    \"{c}\",\n"));
    }
    out.push_str("]\n");
    out.push_str(INIT_END);
    out
}

/// Replace the options section of `init` with `section`.
fn with_init_section(init: &str, section: &str) -> Res<String> {
    let begin = init
        .find(INIT_BEGIN)
        .ok_or_else(|| anyhow::anyhow!("{INIT}: no `{INIT_BEGIN}` marker"))?;
    let end = init[begin..]
        .find(INIT_END)
        .ok_or_else(|| anyhow::anyhow!("{INIT}: no `{INIT_END}` marker"))?
        + begin;
    Ok(format!("{}{section}{}", &init[..begin], &init[end + INIT_END.len()..]))
}

fn unexpected_stops(g: &Generated) -> Vec<&Stop> {
    g.stops.iter().filter(|s| !HAND_WRITTEN.contains(&s.class.as_str())).collect()
}

/// `cargo xtask generate-admin-options`.
pub fn generate(repo_root: &Path) -> Res<()> {
    let g = render_all(repo_root)?;
    let bad = unexpected_stops(&g);
    if !bad.is_empty() {
        let list: Vec<String> = bad.iter().map(ToString::to_string).collect();
        anyhow::bail!(
            "generation stopped for classes outside the known patterns:\n  {}",
            list.join("\n  ")
        );
    }
    // The reference set is golden: a hand-written file the generator does not
    // reproduce is a generator defect, never overwritten.
    let mut manifest = reference_set(repo_root)?;
    manifest.sort();
    if manifest != REFERENCE {
        anyhow::bail!("{REFERENCE_SET} lists {manifest:?}, not the reference set {REFERENCE:?}");
    }
    let refs: BTreeSet<PathBuf> = REFERENCE
        .iter()
        .flat_map(|c| ["py", "pyi"].map(|e| Path::new(ADMIN_PY).join(snake(c)).with_extension(e)))
        .collect();
    let mut golden = Vec::new();
    for (path, content) in &g.files {
        if refs.contains(path) {
            let on_disk = fs::read_to_string(repo_root.join(path)).unwrap_or_default();
            if let Some(d) = first_difference(&on_disk, content) {
                golden.push(format!("{}: {d}", path.display()));
            }
        }
    }
    if !golden.is_empty() {
        anyhow::bail!("the generator does not reproduce the reference set:\n{}", golden.join("\n"));
    }
    for (path, content) in &g.files {
        fs::write(repo_root.join(path), content)?;
    }
    let init_path = repo_root.join(INIT);
    let init = fs::read_to_string(&init_path)?;
    fs::write(&init_path, with_init_section(&init, &g.init_section)?)?;
    println!("✅ Wrote {} admin Options modules to {ADMIN_PY}", g.files.len() / 2);
    for s in &g.stops {
        println!("   hand-written (generation stops): {s}");
    }
    Ok(())
}

/// The stale generated files (empty when up to date).
pub fn check_up_to_date(repo_root: &Path) -> Res<Vec<PathBuf>> {
    let g = render_all(repo_root)?;
    let mut stale: Vec<PathBuf> = Vec::new();
    for s in unexpected_stops(&g) {
        eprintln!("   generation stops: {s}");
        stale.push(Path::new(ADMIN_JAVA).join(format!("{}.java", s.class)));
    }
    for (path, content) in &g.files {
        let on_disk = fs::read_to_string(repo_root.join(path)).unwrap_or_default();
        if let Some(d) = first_difference(&on_disk, content) {
            eprintln!("   {}: {d}", path.display());
            stale.push(path.clone());
        }
    }
    let init = fs::read_to_string(repo_root.join(INIT))?;
    if with_init_section(&init, &g.init_section)? != init {
        stale.push(PathBuf::from(INIT));
    }
    Ok(stale)
}

/// The Options classes of the reference set (the golden files).
pub fn reference_set(repo_root: &Path) -> Res<Vec<String>> {
    let text = fs::read_to_string(repo_root.join(REFERENCE_SET))?;
    let section = text
        .split("## Options classes")
        .nth(1)
        .and_then(|s| s.split("\n## ").next())
        .ok_or_else(|| anyhow::anyhow!("{REFERENCE_SET}: no Options section"))?;
    Ok(section
        .lines()
        .filter_map(|l| {
            l.strip_prefix("| `")
                .and_then(|r| r.split_once('`'))
                .map(|(c, _)| c.to_string())
        })
        .filter(|c| c.ends_with("Options"))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .unwrap()
            .to_path_buf()
    }

    /// The golden test: every reference class is generated byte for byte as
    /// the hand-written file on disk.
    #[test]
    fn test_golden_reference_options() {
        let root = repo_root();
        let mut manifest = reference_set(&root).unwrap();
        manifest.sort();
        assert_eq!(manifest, REFERENCE, "the manifest's Options rows");
        let refs = REFERENCE;
        let g = render_all(&root).unwrap();
        if let Ok(dir) = std::env::var("ADMIN_OPTIONS_DUMP") {
            for (p, c) in &g.files {
                fs::write(Path::new(&dir).join(p.file_name().unwrap()), c).unwrap();
            }
        }
        let mut failures = Vec::new();
        for class in &refs {
            if let Some(s) = g.stops.iter().find(|s| &s.class == class) {
                failures.push(format!("{class}: generation stopped: {}", s.reason));
                continue;
            }
            for ext in ["py", "pyi"] {
                let rel = Path::new(ADMIN_PY).join(snake(class)).with_extension(ext);
                let on_disk = fs::read_to_string(root.join(&rel)).unwrap_or_default();
                let generated = &g.files.iter().find(|(p, _)| p == &rel).unwrap().1;
                if let Some(d) = first_difference(&on_disk, generated) {
                    failures.push(format!("{}: {d}", rel.display()));
                }
            }
        }
        assert!(failures.is_empty(), "golden differences:\n{}", failures.join("\n"));
    }

    #[test]
    fn test_generation_is_deterministic() {
        let root = repo_root();
        let a = render_all(&root).unwrap();
        let b = render_all(&root).unwrap();
        assert_eq!(a.files, b.files);
        assert_eq!(a.init_section, b.init_section);
    }

    #[test]
    fn test_only_hand_written_classes_stop() {
        let g = render_all(&repo_root()).unwrap();
        let stopped: Vec<&str> = g.stops.iter().map(|s| s.class.as_str()).collect();
        assert_eq!(stopped, HAND_WRITTEN.to_vec(), "{:?}", g.stops);
    }

    #[test]
    fn test_unknown_setter_body_stops() {
        let src =
            "package org.apache.kafka.clients.admin;\npublic class XOptions extends AbstractOptions<XOptions> {\n\
                   private int x = 1;\n public XOptions x(int x) {\n this.x = x + 1;\n return this;\n }\n}\n";
        let class = parse_java_class(src).unwrap();
        let err = analyze(class, &Parent::default(), &Known::default()).unwrap_err();
        assert_eq!(err.class, "XOptions");
        assert!(err.reason.contains("stores `x + 1`"), "{}", err.reason);
    }

    fn doc(text: &str) -> Doc {
        read_doc(&format!("/**\n * {text}\n */"))
    }

    #[test]
    fn test_doc_nullability() {
        // A Javadoc "Non-null" wins, also over a phrase allowing null.
        assert_eq!(
            doc_nullability(&[&doc("@param owners the owners, must not be {@code null}")]),
            Ok(Some(false))
        );
        // A conditional on "not null" is no "Non-null": not guessed, generation stops.
        assert!(doc_nullability(&[&doc("If owners is not null, only their tokens are returned.")]).is_err());
        assert_eq!(doc_nullability(&[&doc("The owners. Non-null.")]), Ok(Some(false)));
        assert_eq!(
            doc_nullability(&[&doc("@param owners The owners, must not be null")]),
            Ok(Some(false))
        );
        assert_eq!(
            doc_nullability(&[&doc("If owners is null, all."), &doc("Non-null.")]),
            Ok(Some(false))
        );
        // Phrases allowing null, in the setter's or the getter's Javadoc.
        assert_eq!(
            doc_nullability(&[&doc("The timeout or {@code null} for the default.")]),
            Ok(Some(true))
        );
        assert_eq!(
            doc_nullability(&[&doc("Set it."), &doc("If the filter is null, all.")]),
            Ok(Some(true))
        );
        // "optional" says nothing about null.
        assert_eq!(doc_nullability(&[&doc("Sets an optional reason.")]), Ok(None));
        // Any other mention of null stops generation.
        assert!(doc_nullability(&[&doc("Null values are rejected by the broker.")]).is_err());
    }

    #[test]
    fn test_non_null_javadoc_makes_the_argument_non_nullable() {
        let src = "package org.apache.kafka.clients.admin;\nimport java.util.List;\n\
                   import org.apache.kafka.common.security.auth.KafkaPrincipal;\n\
                   public class DescribeDelegationTokenOptions extends AbstractOptions<DescribeDelegationTokenOptions> {\n\
                   private List<KafkaPrincipal> owners;\n\
                   /**\n * The owners to describe. Non-null.\n * @param owners The owners, must not be null\n */\n\
                   public DescribeDelegationTokenOptions owners(List<KafkaPrincipal> owners) {\n this.owners = owners;\n return this;\n }\n\
                   public List<KafkaPrincipal> owners() {\n return owners;\n }\n}\n";
        let o = analyze(parse_java_class(src).unwrap(), &Parent::default(), &Known::default()).unwrap();
        assert_eq!(o.setters[0].doc_null, Some(false));
        let bad = src.replace("Non-null.", "Null is rejected.").replace(", must not be null", "");
        let err = analyze(parse_java_class(&bad).unwrap(), &Parent::default(), &Known::default()).unwrap_err();
        assert!(err.reason.contains("Javadoc mentions null"), "{}", err.reason);
    }

    #[test]
    fn test_internal_constant_name_merges_overlap() {
        let root = repo_root();
        let g = render_all(&root).unwrap();
        let py = &g.files.iter().find(|(p, _)| p.ends_with("list_groups_options.py")).unwrap().1;
        assert!(py.contains("# ``ConsumerProtocol.PROTOCOL_TYPE`` (consumer/internals/ConsumerProtocol.java:45).\n_CONSUMER_PROTOCOL_TYPE = \"consumer\""));
    }
}
