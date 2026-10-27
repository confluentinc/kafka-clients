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

//! `cargo xtask generate-admin-results`: the admin `*Result` Python classes
//! (`python/confluent_kafka/admin/<class>.py` + `.pyi`) from the Java sources
//! (CLAUDE.md, Python Binding Conventions: Idiom translations on `KafkaFuture`
//! and private helpers; Class family on the client-kind marker and `_new`).
//!
//! Each class is read member by member. The constructor must store each
//! parameter in one field (`this.f = p;`, after `DeleteTopicsResult`'s
//! argument checks), and every method body must match one of the known
//! patterns of [`classify`] (Java bodies as [`normalize`](super::normalize)
//! gives them, matched by [`pmatch`]); anything else stops generation for that
//! class ([`Stop`]). The classes the reference manifest lists as outside every
//! pattern are [`HAND_WRITTEN`]: the generator never writes them, and stops on
//! them if asked to.
//!
//! Known method patterns (and the reference class that shows each):
//! - `return f;`: a stored future or map of futures (`AlterConfigsResult`,
//!   `DescribeClusterResult`);
//! - a `new HashMap<>(f)` / `putAll` copy (`DeleteConsumerGroupsResult`);
//! - `KafkaFuture.allOf(f.values()...)` (`AlterConfigsResult`);
//! - `allOf(...).thenApply(collect)`, by entry or by `forEach`, keyed by the
//!   key or by `key.idValue` (`ListOffsetsResult`, `DescribeTransactionsResult`);
//! - a lookup or `IllegalArgumentException`, by the key or by
//!   `CoordinatorKey.byTransactionalId` (`ListOffsetsResult`,
//!   `DescribeTransactionsResult`);
//! - `thenApply(Map::values | Map::keySet)` (`ListTopicsResult`);
//! - a new future completed from `whenComplete`, relaying the value or failing
//!   with the first present `Optional<Throwable>` (the
//!   `DeleteConsumerGroupOffsetsResult` shape);
//! - the class-specific bodies of `CreateTopicsResult`, `DeleteTopicsResult`,
//!   `DeleteConsumerGroupOffsetsResult` and `ListTransactionsResult`, each a
//!   pattern of its own that no other class has.
//!
//! Public nested value classes (`ListOffsetsResultInfo`,
//! `PartitionProducerState`) become nested Python classes: a keyword
//! constructor storing its parameters, `return f;` getters, a concatenation
//! `toString`.
//!
//! The reference set (`python/confluent_kafka/admin/_reference_set.md`) is the
//! golden output: `test_golden_reference_results` fails on any byte that
//! differs, and `generate` refuses to write while a reference file is not
//! reproduced.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use super::{
    docstring, first_difference, indent_literal, module_title, parse_java_class, read_doc, snake, split_top,
    statements, stop, Doc, Imports, JClass, JKind, JMember, JParam, Prose, Res, Stop, ADMIN_JAVA, ADMIN_PY, LICENSE,
    WIDTH,
};

/// The Result class of each of the 45 `Admin` RPCs the Python client covers.
pub const CLASSES: [&str; 45] = [
    "AbortTransactionResult",
    "AlterClientQuotasResult",
    "AlterConfigsResult",
    "AlterConsumerGroupOffsetsResult",
    "AlterPartitionReassignmentsResult",
    "AlterReplicaLogDirsResult",
    "AlterUserScramCredentialsResult",
    "CreateAclsResult",
    "CreateDelegationTokenResult",
    "CreatePartitionsResult",
    "CreateTopicsResult",
    "DeleteAclsResult",
    "DeleteConsumerGroupOffsetsResult",
    "DeleteConsumerGroupsResult",
    "DeleteRecordsResult",
    "DeleteTopicsResult",
    "DescribeAclsResult",
    "DescribeClassicGroupsResult",
    "DescribeClientQuotasResult",
    "DescribeClusterResult",
    "DescribeConfigsResult",
    "DescribeConsumerGroupsResult",
    "DescribeDelegationTokenResult",
    "DescribeFeaturesResult",
    "DescribeLogDirsResult",
    "DescribeProducersResult",
    "DescribeReplicaLogDirsResult",
    "DescribeTopicsResult",
    "DescribeTransactionsResult",
    "DescribeUserScramCredentialsResult",
    "ElectLeadersResult",
    "ExpireDelegationTokenResult",
    "FenceProducersResult",
    "ListConfigResourcesResult",
    "ListConsumerGroupOffsetsResult",
    "ListGroupsResult",
    "ListOffsetsResult",
    "ListPartitionReassignmentsResult",
    "ListTopicsResult",
    "ListTransactionsResult",
    "RemoveMembersFromConsumerGroupResult",
    "RenewDelegationTokenResult",
    "TerminateTransactionResult",
    "UnregisterControllerResult",
    "UpdateFeaturesResult",
];

/// The Results outside every pattern (decision log 44; manifest Coverage):
/// hand-written, never touched by the generator.
pub const HAND_WRITTEN: [&str; 10] = [
    "AlterConsumerGroupOffsetsResult",
    "AlterUserScramCredentialsResult",
    "DeleteAclsResult",
    "DescribeReplicaLogDirsResult",
    "DescribeTopicsResult",
    "DescribeUserScramCredentialsResult",
    "FenceProducersResult",
    "ListConsumerGroupOffsetsResult",
    "ListGroupsResult",
    "RemoveMembersFromConsumerGroupResult",
];

/// The `Collection` fields the Java implementation is checked to build as no
/// `Set` (a `List` or a map values view), so `list` in Python (Types): (class,
/// field, where).
const LIST_COLLECTIONS: [(&str, &str, &str); 4] = [
    (
        "DescribeAclsResult",
        "future",
        "DescribeAclsResponse.aclBindings: Collectors.toList()",
    ),
    (
        "DescribeClusterResult",
        "nodes",
        "KafkaAdminClient.describeCluster: Map.values() / response.brokers()",
    ),
    (
        "ListConfigResourcesResult",
        "future",
        "ListConfigResourcesResponse.configResources: Collectors.toList()",
    ),
    (
        "ListTransactionsResult",
        "future",
        "ListTransactionsHandler.handleResponse: Collectors.toList()",
    ),
];

/// The docstring text not derived from Javadoc (see [`Prose`]).
const PROSE: &str = include_str!("results_prose.txt");

/// Code lines may run this far past [`WIDTH`] where the reference set lets
/// them (a `message=` concatenation, a hanging signature, a stub).
const SLACK: usize = 3;
/// A stub's parameter line above a `) -> R: ...` line, and a stub `_new` or
/// static factory, fill to this width (the reference set keeps a 90-character
/// `_new` line and wraps a 91-character one).
const STUB_PARAMS_WIDTH: usize = 90;
/// The `TypeError` message keeps `client's` on its first line up to this
/// width (the reference set keeps 92 characters and moves it at 96).
const TYPE_ERROR_WIDTH: usize = 92;
/// isort's `profile = black` line length: a longer `from` import is wrapped.
const IMPORT_WIDTH: usize = 88;

const REFERENCE_SET: &str = "python/confluent_kafka/admin/_reference_set.md";
const INIT: &str = "python/confluent_kafka/admin/__init__.py";
const INIT_BEGIN: &str = "# BEGIN RESULTS";
const INIT_END: &str = "# END RESULTS";

// ---------------------------------------------------------------------------
// Pattern matching on normalized Java
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum Tok {
    Lit(String),
    /// `$name`: a Java identifier; a name used twice must match the same text.
    Id(String),
    /// `#name`: any text, shortest first.
    Any(String),
}

fn tokens(pat: &str) -> Vec<Tok> {
    let mut out = Vec::new();
    let mut lit = String::new();
    let mut chars = pat.chars().peekable();
    while let Some(c) = chars.next() {
        if (c == '$' || c == '#') && chars.peek().is_some_and(|n| n.is_ascii_alphabetic()) {
            if !lit.is_empty() {
                out.push(Tok::Lit(std::mem::take(&mut lit)));
            }
            let mut name = String::new();
            while let Some(&n) = chars.peek() {
                if !(n.is_ascii_alphanumeric() || n == '_') {
                    break;
                }
                name.push(n);
                chars.next();
            }
            out.push(if c == '$' { Tok::Id(name) } else { Tok::Any(name) });
        } else {
            lit.push(c);
        }
    }
    if !lit.is_empty() {
        out.push(Tok::Lit(lit));
    }
    out
}

fn match_from(toks: &[Tok], text: &str, caps: &mut BTreeMap<String, String>) -> bool {
    let Some(first) = toks.first() else {
        return text.is_empty();
    };
    match first {
        Tok::Lit(l) => text
            .strip_prefix(l.as_str())
            .is_some_and(|rest| match_from(&toks[1..], rest, caps)),
        Tok::Id(name) => {
            let len = text.bytes().take_while(|b| b.is_ascii_alphanumeric() || *b == b'_').count();
            if len == 0 {
                return false;
            }
            let (id, rest) = text.split_at(len);
            if let Some(prev) = caps.get(name) {
                return prev == id && match_from(&toks[1..], rest, caps);
            }
            caps.insert(name.clone(), id.to_string());
            if match_from(&toks[1..], rest, caps) {
                return true;
            }
            caps.remove(name);
            false
        },
        Tok::Any(name) => {
            for k in 0..=text.len() {
                if !text.is_char_boundary(k) {
                    continue;
                }
                caps.insert(name.clone(), text[..k].to_string());
                if match_from(&toks[1..], &text[k..], caps) {
                    return true;
                }
            }
            caps.remove(name);
            false
        },
    }
}

/// Match `text` against `pat` (`$id` an identifier, `#any` any text), giving
/// the captures.
fn pmatch(pat: &str, text: &str) -> Option<BTreeMap<String, String>> {
    let mut caps = BTreeMap::new();
    match_from(&tokens(pat), text, &mut caps).then_some(caps)
}

// ---------------------------------------------------------------------------
// Java types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
struct JT {
    name: String,
    args: Vec<JT>,
}

fn parse_type(s: &str) -> JT {
    let s = s.trim();
    match s.find('<') {
        Some(k) if s.ends_with('>') => JT {
            name: s[..k].trim().to_string(),
            args: split_top(&s[k + 1..s.len() - 1], ',').iter().map(|a| parse_type(a)).collect(),
        },
        _ => JT { name: s.to_string(), args: Vec::new() },
    }
}

impl JT {
    fn is_future(&self) -> bool {
        matches!(self.name.as_str(), "KafkaFuture" | "KafkaFutureImpl")
    }

    fn is_map(&self) -> bool {
        self.name == "Map"
    }

    fn mentions(&self, name: &str) -> bool {
        self.name == name || self.args.iter().any(|a| a.mentions(name))
    }

    fn arg(&self, k: usize) -> Result<&JT, String> {
        self.args.get(k).ok_or(format!("type `{}` without argument {k}", self.name))
    }
}

/// How a future is written: in a runtime annotation, or in a stub of a kind.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Flavor {
    Any,
    Sync,
    Async,
}

impl Flavor {
    fn future(self) -> &'static str {
        match self {
            // Either kind: written as the union of the two (`Types::py`).
            Flavor::Any => "KafkaFuture",
            Flavor::Sync => "KafkaFuture",
            Flavor::Async => "AsyncKafkaFuture",
        }
    }
}

/// The Python module a Java type lives in (CLAUDE.md, Python Binding
/// Conventions, Modules): an admin type by its file, any other by its Java package.
fn py_module(fqcn: &str) -> Option<String> {
    let (pkg, simple) = fqcn.rsplit_once('.')?;
    if pkg == "org.apache.kafka.clients.admin" {
        return Some(format!("confluent_kafka.admin.{}", snake(simple)));
    }
    if pkg.contains(".internals") {
        return None;
    }
    // Any other type: the module of its Java package (`common.acl.AclBinding` ->
    // `confluent_kafka.common.acl`).
    if pkg == "org.apache.kafka.common" || pkg.starts_with("org.apache.kafka.common.") {
        return Some(format!("confluent_kafka.{}", &pkg["org.apache.kafka.".len()..]));
    }
    None
}

struct Types<'a> {
    class: &'a JClass,
    /// Nested classes: simple name -> Python name.
    nested: BTreeMap<String, String>,
    /// Value types used: (module, name).
    used: BTreeSet<(String, String)>,
    /// `_internals` helpers used as types.
    internals: BTreeSet<String>,
}

impl Types<'_> {
    /// `top`: a parameter's own type (`Mapping`, `Iterable`, `Sequence`) rather
    /// than a value (`dict`, `set`, `list`); `nullable`: a future's value may
    /// be `None`.
    fn py(&mut self, t: &JT, flavor: Flavor, top: bool, nullable: bool) -> Result<String, String> {
        Ok(match t.name.as_str() {
            "KafkaFuture" | "KafkaFutureImpl" => {
                let inner = self.py(t.arg(0)?, flavor, false, false)?;
                let inner = if nullable { format!("{inner} | None") } else { inner };
                if flavor == Flavor::Any {
                    format!("KafkaFuture[{inner}] | AsyncKafkaFuture[{inner}]")
                } else {
                    format!("{}[{inner}]", flavor.future())
                }
            },
            "Map" => {
                let k = self.py(t.arg(0)?, flavor, false, false)?;
                let v = self.py(t.arg(1)?, flavor, false, nullable)?;
                if top {
                    format!("Mapping[{k}, {v}]")
                } else {
                    format!("dict[{k}, {v}]")
                }
            },
            "Collection" | "List" => {
                let e = self.py(t.arg(0)?, flavor, false, false)?;
                if top {
                    format!("Sequence[{e}]")
                } else {
                    format!("list[{e}]")
                }
            },
            "Set" => {
                let e = self.py(t.arg(0)?, flavor, false, false)?;
                if top {
                    format!("Iterable[{e}]")
                } else {
                    format!("set[{e}]")
                }
            },
            "Optional" if t.arg(0)?.name == "Throwable" => "BaseException | None".into(),
            "Optional" => format!("{} | None", self.py(t.arg(0)?, flavor, false, false)?),
            "Throwable" => "BaseException".into(),
            // Java's internal `Errors`, carried as what `Errors.exception()` gives.
            "Errors" => "BaseException | None".into(),
            "Void" => "None".into(),
            "boolean" | "Boolean" => "bool".into(),
            "int" | "Integer" | "long" | "Long" | "short" | "Short" => "int".into(),
            "double" | "Double" | "float" | "Float" => "float".into(),
            "String" => "str".into(),
            "CoordinatorKey" => {
                self.internals.insert("_CoordinatorKey".into());
                "_CoordinatorKey".into()
            },
            simple if self.nested.contains_key(simple) => self.nested[simple].clone(),
            simple if simple.chars().all(|c| c.is_alphanumeric()) && simple.starts_with(char::is_uppercase) => {
                let fqcn = self.class.resolve(simple);
                let module = py_module(&fqcn).ok_or(format!("type `{simple}` ({fqcn}) has no Python module"))?;
                self.used.insert((module, simple.to_string()));
                simple.to_string()
            },
            other => return Err(format!("type `{other}`")),
        })
    }
}

// ---------------------------------------------------------------------------
// The model
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Field {
    java: String,
    py: String,
    ty: JT,
    /// The getter's Javadoc says the future's value may be `null`.
    nullable: bool,
    /// The field itself may be `None`: a constructor check compares its
    /// parameter with `null` (`DeleteTopicsResult`).
    optional: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Visibility {
    Public,
    Protected,
    Package,
}

impl Visibility {
    fn java(self) -> &'static str {
        match self {
            Visibility::Public => "public",
            Visibility::Protected => "protected",
            Visibility::Package => "package-private",
        }
    }
}

/// How a collected map's key is read from the stored map's key.
#[derive(Clone, Debug, PartialEq, Eq)]
enum KeyOf {
    Key,
    IdValue,
}

/// One term of a Java string concatenation.
#[derive(Clone, Debug)]
enum Term {
    Lit(String),
    Var(String),
}

#[derive(Clone, Debug)]
enum Pat {
    Stored(String),
    Copy(String),
    AllOf(String),
    Collect {
        field: String,
        key: KeyOf,
        /// The `forEach` key name, empty for the entry form.
        var: String,
    },
    Lookup {
        field: String,
        param: String,
        coordinator: bool,
        message: Vec<Term>,
    },
    MapView {
        field: String,
        keys: bool,
    },
    Relay {
        field: String,
        value: String,
        error: String,
    },
    FirstError {
        field: String,
        getter: String,
        value: String,
        error: String,
        item: String,
    },
    /// `CreateTopicsResult.values()`.
    ValuesToVoid(String),
    /// `CreateTopicsResult.config(topic)` and its siblings.
    PerKeyGetter {
        field: String,
        param: String,
        getter: String,
    },
    /// `DeleteTopicsResult.all()`.
    EitherAllOf {
        first: String,
        second: String,
    },
    /// `DeleteConsumerGroupOffsetsResult.partitionResult(partition)`.
    SubErrorOne {
        future: String,
        partitions: String,
        message: Vec<Term>,
    },
    /// `DeleteConsumerGroupOffsetsResult.all()`.
    SubErrorAll {
        future: String,
        partitions: String,
    },
    /// `DeleteConsumerGroupOffsetsResult.maybeCompleteExceptionally(...)`.
    SubErrorHelper {
        message: Vec<Term>,
    },
    /// `ListTransactionsResult.all()`.
    Flatten {
        getter: String,
    },
    /// `ListTransactionsResult.byBrokerId()`.
    CopyOnComplete(String),
    /// `ListTransactionsResult.allByBrokerId()`.
    AllByKey(String),
}

#[derive(Clone, Debug)]
struct Method {
    java: String,
    py: String,
    params: Vec<JParam>,
    ret: JT,
    private: bool,
    doc: Doc,
    /// The Javadoc as written (markup kept), whitespace collapsed: the null
    /// wording is read here, before `{@code null}` becomes ``None``.
    raw_doc: String,
    pat: Pat,
}

/// A public nested value class.
#[derive(Clone, Debug)]
struct Nested {
    name: String,
    fqcn: String,
    doc: Doc,
    fields: Vec<(String, JT)>,
    /// (Java name, field, Javadoc).
    getters: Vec<(String, String, Doc)>,
    to_string: Vec<Term>,
}

#[derive(Clone, Debug)]
struct ResultClass {
    class: JClass,
    fields: Vec<Field>,
    visibility: Visibility,
    /// Constructor parameters, each with the field it is stored in.
    ctor: Vec<(JParam, String)>,
    /// `DeleteTopicsResult`'s argument checks: the `&&`-joined comparisons of a
    /// parameter with `null` (`true`: `== null`), and the message literal.
    checks: Vec<(Vec<(String, bool)>, String)>,
    /// The constructor's Javadoc.
    ctor_doc: Doc,
    /// `DeleteTopicsResult`'s static factories: (Java name, parameter,
    /// constructor arguments).
    factories: Vec<(String, JParam, Vec<String>)>,
    methods: Vec<Method>,
    nested: Vec<Nested>,
    /// `CreateTopicsResult.TopicMetadataAndConfig`: a private nested holder.
    holder: Option<String>,
    /// The admin method that returns the class (from the class Javadoc).
    admin_method: String,
}

/// A member's Javadoc text as written, `*` margins dropped, whitespace collapsed.
fn raw_javadoc(m: &JMember) -> String {
    let block = m.javadoc.as_deref().unwrap_or("");
    let inner = block.trim().trim_start_matches("/**").trim_end_matches("*/");
    inner
        .lines()
        .map(|l| l.trim().trim_start_matches('*').trim())
        .collect::<Vec<_>>()
        .join(" ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// The understood Javadoc null wording; any other mention of `null` stops.
const NULL_PHRASES: [&str; 4] = [
    // `DescribeClusterResult.authorizedOperations`: the future's value may be null.
    "non-null",
    "null otherwise",
    // `DeleteTopicsResult`: the getter returns null for the map not set.
    "Otherwise return null",
    // `CreateTopicsResult.config`: about the value's own fields.
    "fields will be null",
];

fn member_doc(m: &JMember) -> Doc {
    m.javadoc.as_deref().map(read_doc).unwrap_or_default()
}

/// A Java string concatenation as terms.
fn concat_terms(expr: &str) -> Result<Vec<Term>, String> {
    let mut out = Vec::new();
    for part in split_top(expr, '+') {
        let p = part.trim();
        if p.len() >= 2 && p.starts_with('"') && p.ends_with('"') {
            out.push(Term::Lit(p.to_string()));
        } else if p.len() == 3 && p.starts_with('\'') && p.ends_with('\'') {
            out.push(Term::Lit(format!("\"{}\"", &p[1..2])));
        } else if !p.is_empty() && p.chars().all(|c| c.is_alphanumeric() || c == '_') {
            out.push(Term::Var(p.to_string()));
        } else {
            return Err(format!("concatenation term `{p}`"));
        }
    }
    Ok(out)
}

const ALL_OF: &str = "KafkaFuture.allOf($f.values().toArray(new KafkaFuture<?>[0]))";

/// Recognize a method body (see the module docs for the patterns).
fn classify(name: &str, params: &[JParam], body: &str, fields: &[&str]) -> Result<Pat, String> {
    let field = |c: &BTreeMap<String, String>, k: &str| -> Result<String, String> {
        let f = c[k].clone();
        if fields.contains(&f.as_str()) {
            Ok(f)
        } else {
            Err(format!("method `{name}`: `{f}` is not a field"))
        }
    };
    let param = |c: &BTreeMap<String, String>, k: &str| -> Result<String, String> {
        let p = c[k].clone();
        if params.iter().any(|x| x.name == p) {
            Ok(p)
        } else {
            Err(format!("method `{name}`: `{p}` is not a parameter"))
        }
    };
    if let Some(c) = pmatch("return $f;", body) {
        return Ok(Pat::Stored(field(&c, "f")?));
    }
    if let Some(c) = pmatch("return new HashMap<>($f);", body) {
        return Ok(Pat::Copy(field(&c, "f")?));
    }
    if let Some(c) = pmatch("#t $v = new HashMap<>($f.size()); $v.putAll($f); return $v;", body) {
        return Ok(Pat::Copy(field(&c, "f")?));
    }
    if let Some(c) = pmatch(&format!("return {ALL_OF};"), body) {
        return Ok(Pat::AllOf(field(&c, "f")?));
    }
    let collect_entry = format!(
        "return {ALL_OF}.thenApply($n -> {{ #t $m = new HashMap<>($f.size()); for (Map.Entry<#e> $x : $f.entrySet()) \
         {{ try {{ $m.put($x.getKey()#k, $x.getValue().get()); }} catch (InterruptedException | ExecutionException $y) \
         {{ throw new #r($y); }} }} return $m; }});"
    );
    if let Some(c) = pmatch(&collect_entry, body) {
        let key = match c["k"].as_str() {
            "" => KeyOf::Key,
            ".idValue" => KeyOf::IdValue,
            k => return Err(format!("method `{name}`: collected key `{k}`")),
        };
        return Ok(Pat::Collect { field: field(&c, "f")?, key, var: String::new() });
    }
    let collect_each = format!(
        "return {ALL_OF}.thenApply($n -> {{ #t $m = new HashMap<>($f.size()); $f.forEach(($k, $v) -> {{ try {{ \
         $m.put($k, $v.get()); }} catch (InterruptedException | ExecutionException $y) {{ throw new #r($y); }} }}); \
         return $m; }});"
    );
    if let Some(c) = pmatch(&collect_each, body) {
        return Ok(Pat::Collect { field: field(&c, "f")?, key: KeyOf::Key, var: c["k"].clone() });
    }
    let lookup = "#t $v = $f.get($p); if ($v == null) { throw new IllegalArgumentException(#m); } return $v;";
    let by_txn = format!(
        "CoordinatorKey $k = CoordinatorKey.byTransactionalId($p); {}",
        lookup.replace("get($p)", "get($k)")
    );
    if let Some(c) = pmatch(&by_txn, body) {
        return Ok(Pat::Lookup {
            field: field(&c, "f")?,
            param: param(&c, "p")?,
            coordinator: true,
            message: concat_terms(&c["m"])?,
        });
    }
    if let Some(c) = pmatch(lookup, body) {
        return Ok(Pat::Lookup {
            field: field(&c, "f")?,
            param: param(&c, "p")?,
            coordinator: false,
            message: concat_terms(&c["m"])?,
        });
    }
    if let Some(c) = pmatch("return $f.thenApply(Map::values);", body) {
        return Ok(Pat::MapView { field: field(&c, "f")?, keys: false });
    }
    if let Some(c) = pmatch("return $f.thenApply(Map::keySet);", body) {
        return Ok(Pat::MapView { field: field(&c, "f")?, keys: true });
    }
    let relay =
        "final KafkaFutureImpl<#t> $r = new KafkaFutureImpl<>(); $f.whenComplete(($v, $e) -> { if ($e != null) \
                 { $r.completeExceptionally($e); } else { $r.complete($v); } }); return $r;";
    if let Some(c) = pmatch(relay, body) {
        return Ok(Pat::Relay { field: field(&c, "f")?, value: c["v"].clone(), error: c["e"].clone() });
    }
    let first_error = "final KafkaFutureImpl<Void> $r = new KafkaFutureImpl<>(); $g().whenComplete(($v, $e) -> { if \
                       ($e != null) { $r.completeExceptionally($e); } else { for (Optional<Throwable> $x : $v.values()) \
                       { if ($x.isPresent()) { $r.completeExceptionally($x.get()); return; } } $r.complete(null); } }); \
                       return $r;";
    if let Some(c) = pmatch(first_error, body) {
        return Ok(Pat::FirstError {
            field: String::new(),
            getter: c["g"].clone(),
            value: c["v"].clone(),
            error: c["e"].clone(),
            item: c["x"].clone(),
        });
    }
    let values_void = "return $f.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> \
                       e.getValue().thenApply(v -> null)));";
    if let Some(c) = pmatch(values_void, body) {
        return Ok(Pat::ValuesToVoid(field(&c, "f")?));
    }
    if let Some(c) = pmatch("return $f.get($p).thenApply($h::$g);", body) {
        return Ok(Pat::PerKeyGetter { field: field(&c, "f")?, param: param(&c, "p")?, getter: c["g"].clone() });
    }
    let either = format!(
        "return ($a == null) ? {} : {};",
        ALL_OF.replace("$f", "$b"),
        ALL_OF.replace("$f", "$a")
    );
    if let Some(c) = pmatch(&either, body) {
        return Ok(Pat::EitherAllOf { first: field(&c, "a")?, second: field(&c, "b")? });
    }
    let when = "final KafkaFutureImpl<Void> result = new KafkaFutureImpl<>(); this.$f.whenComplete((topicPartitions, \
                throwable) -> { if (throwable != null) { result.completeExceptionally(throwable); } ";
    let one = format!(
        "if (!$s.contains($p)) {{ throw new IllegalArgumentException(#m); }} {when}else if \
         (!maybeCompleteExceptionally(topicPartitions, $p, result)) {{ result.complete(null); }} }}); return result;"
    );
    if let Some(c) = pmatch(&one, body) {
        return Ok(Pat::SubErrorOne {
            future: field(&c, "f")?,
            partitions: field(&c, "s")?,
            message: concat_terms(&c["m"])?,
        });
    }
    let all = format!(
        "{when}else {{ for (TopicPartition $p : $s) {{ if (maybeCompleteExceptionally(topicPartitions, $p, result)) \
         {{ return; }} }} result.complete(null); }} }}); return result;"
    );
    if let Some(c) = pmatch(&all, body) {
        return Ok(Pat::SubErrorAll { future: field(&c, "f")?, partitions: field(&c, "s")? });
    }
    let helper = "Throwable exception = KafkaAdminClient.getSubLevelError($e, $p, #m); if (exception != null) { \
                  result.completeExceptionally(exception); return true; } else { return false; }";
    if let Some(c) = pmatch(helper, body) {
        if name == "maybeCompleteExceptionally" && params.len() == 3 {
            return Ok(Pat::SubErrorHelper { message: concat_terms(&c["m"])? });
        }
    }
    let flatten = "return $g().thenApply(map -> { List<#t> allListings = new ArrayList<>(); for (Collection<#u> \
                   listings : map.values()) { allListings.addAll(listings); } return allListings; });";
    if let Some(c) = pmatch(flatten, body) {
        return Ok(Pat::Flatten { getter: c["g"].clone() });
    }
    let copy_on =
        "KafkaFutureImpl<#t> result = new KafkaFutureImpl<>(); $f.whenComplete((brokerFutures, exception) -> \
                   { if (brokerFutures != null) { #u brokerFuturesCopy = new HashMap<>(brokerFutures.size()); \
                   brokerFuturesCopy.putAll(brokerFutures); result.complete(brokerFuturesCopy); } else { \
                   result.completeExceptionally(exception); } }); return result;";
    if let Some(c) = pmatch(copy_on, body) {
        return Ok(Pat::CopyOnComplete(field(&c, "f")?));
    }
    let all_by = "KafkaFutureImpl<#t> allFuture = new KafkaFutureImpl<>(); #u allListingsMap = new HashMap<>(); \
                  $f.whenComplete((map, topLevelException) -> { if (topLevelException != null) { \
                  allFuture.completeExceptionally(topLevelException); return; } Set<Integer> remainingResponses = new \
                  HashSet<>(map.keySet()); map.forEach((brokerId, future) -> future.whenComplete((listings, \
                  brokerException) -> { if (brokerException != null) { \
                  allFuture.completeExceptionally(brokerException); } else if (!allFuture.isDone()) { \
                  allListingsMap.put(brokerId, listings); remainingResponses.remove(brokerId); if \
                  (remainingResponses.isEmpty()) { allFuture.complete(allListingsMap); } } })); }); return allFuture;";
    if let Some(c) = pmatch(all_by, body) {
        return Ok(Pat::AllByKey(field(&c, "f")?));
    }
    Err(format!("method `{name}` body outside the known patterns: `{body}`"))
}

/// The source of the nested class `name` in `raw`, as a parseable unit.
fn nested_source(raw: &str, package: &str, name: &str) -> Option<String> {
    let at = raw.find(&format!("public static class {name} "))?;
    let open = at + raw[at..].find('{')?;
    let mut depth = 0usize;
    let mut end = None;
    for (i, c) in raw.bytes().enumerate().skip(open) {
        match c {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    end = Some(i + 1);
                    break;
                }
            },
            _ => {},
        }
    }
    let start = raw[..at]
        .rfind("/**")
        .filter(|s| raw[*s..at].trim_end().ends_with("*/"))
        .unwrap_or(at);
    Some(format!(
        "package {package};\n{}\n",
        raw[start..end?].replace("public static class", "public class")
    ))
}

fn analyze_nested(raw: &str, outer: &JClass, name: &str) -> Result<Nested, String> {
    let src = nested_source(raw, &outer.package, name).ok_or(format!("nested class `{name}` not found"))?;
    let class = parse_java_class(&src).map_err(|e| format!("nested `{name}`: {e}"))?;
    let mut fields: Vec<(String, JT)> = Vec::new();
    let mut getters = Vec::new();
    let mut to_string = Vec::new();
    let mut ctors = 0;
    for m in &class.members {
        match &m.kind {
            JKind::Field { ty, name: f, init: None } if !m.is_static() => fields.push((f.clone(), parse_type(ty))),
            JKind::Ctor { params, body } => {
                ctors += 1;
                let expect: Vec<String> = params.iter().map(|p| format!("this.{0} = {0};", p.name)).collect();
                let stores_all =
                    params.len() == fields.len() && params.iter().zip(&fields).all(|(p, (f, _))| &p.name == f);
                if !m.is_public() || body.trim() != expect.join(" ") || !stores_all {
                    return Err(format!("nested `{name}`: constructor `{body}`"));
                }
            },
            JKind::Method { name: mname, params, body: Some(body), .. } if mname == "toString" && params.is_empty() => {
                let expr = body
                    .strip_prefix("return ")
                    .and_then(|b| b.strip_suffix(';'))
                    .ok_or(format!("nested `{name}`: toString `{body}`"))?;
                to_string = concat_terms(expr)?;
            },
            JKind::Method { name: mname, params, body: Some(body), .. } if params.is_empty() && m.is_public() => {
                match pmatch("return $f;", body) {
                    Some(c) if fields.iter().any(|(f, _)| f == &c["f"]) => {
                        getters.push((mname.clone(), c["f"].clone(), member_doc(m)))
                    },
                    _ => return Err(format!("nested `{name}`: method `{mname}` body `{body}`")),
                }
            },
            other => return Err(format!("nested `{name}`: member `{other:?}`")),
        }
    }
    if ctors != 1 {
        return Err(format!("nested `{name}` has {ctors} constructors"));
    }
    let doc = class.javadoc.as_deref().map(read_doc).unwrap_or_default();
    Ok(Nested {
        name: name.to_string(),
        fqcn: format!("{}.{name}", outer.fqcn()),
        doc,
        fields,
        getters,
        to_string,
    })
}

/// Recognize `CreateTopicsResult.TopicMetadataAndConfig`, the one private
/// nested holder: its two constructors, `ensureSuccess` and the getters.
fn check_holder(raw: &str, outer: &JClass, name: &str) -> Result<(), String> {
    let src = nested_source(raw, &outer.package, name).ok_or(format!("nested class `{name}` not found"))?;
    let class = parse_java_class(&src).map_err(|e| format!("nested `{name}`: {e}"))?;
    let shape: Vec<String> = class
        .members
        .iter()
        .map(|m| match &m.kind {
            JKind::Field { name, .. } => format!("F {name}"),
            JKind::Ctor { params, body } => {
                let ps: Vec<String> = params.iter().map(|p| format!("{} {}", p.ty, p.name)).collect();
                format!("C ({}) {body}", ps.join(", "))
            },
            JKind::Method { name, body, .. } => format!("M {name} {}", body.clone().unwrap_or_default()),
            JKind::Other(h) => format!("O {h}"),
        })
        .collect();
    let expected = [
        "F exception",
        "F topicId",
        "F numPartitions",
        "F replicationFactor",
        "F config",
        "C (Uuid topicId, int numPartitions, int replicationFactor, Config config) this.exception = null; this.topicId \
         = topicId; this.numPartitions = numPartitions; this.replicationFactor = replicationFactor; this.config = \
         config;",
        "C (ApiException exception) this.exception = exception; this.topicId = Uuid.ZERO_UUID; this.numPartitions = \
         UNKNOWN; this.replicationFactor = UNKNOWN; this.config = null;",
        "M topicId ensureSuccess(); return topicId;",
        "M numPartitions ensureSuccess(); return numPartitions;",
        "M replicationFactor ensureSuccess(); return replicationFactor;",
        "M config ensureSuccess(); return config;",
        "M ensureSuccess if (exception != null) throw exception;",
    ];
    if shape != expected {
        return Err(format!("nested `{name}` outside the known holder pattern: {shape:?}"));
    }
    Ok(())
}

/// A Java message with Python's spelling of `null` (CLAUDE.md, Python Binding
/// Conventions, Types): the standalone word `null` reads `None`.
fn none_spelling(message: &str) -> String {
    let bytes = message.as_bytes();
    let mut out = String::with_capacity(message.len());
    let mut i = 0;
    while i < bytes.len() {
        let word_start = i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_');
        let word_end = i + 4 >= bytes.len() || !(bytes[i + 4].is_ascii_alphanumeric() || bytes[i + 4] == b'_');
        if word_start && word_end && message[i..].starts_with("null") {
            out.push_str("None");
            i += 4;
        } else {
            out.push(message[i..].chars().next().unwrap());
            i += message[i..].chars().next().unwrap().len_utf8();
        }
    }
    out
}

/// `create_topics` -> `createTopics`: the Java method name of an admin call.
fn camel(snake: &str) -> String {
    let mut out = String::new();
    let mut upper = false;
    for c in snake.chars() {
        if c == '_' {
            upper = true;
        } else if upper {
            out.extend(c.to_uppercase());
            upper = false;
        } else {
            out.push(c);
        }
    }
    out
}

/// The `Admin` method returning each result class: the first declaration
/// `[default] XResult name(` in `Admin.java` (the interface is the contract;
/// a class Javadoc link can name the wrong method, as
/// `RenewDelegationTokenResult`'s does).
fn admin_methods(repo_root: &Path) -> Res<BTreeMap<String, String>> {
    let admin = read_raw(repo_root, "Admin")?;
    let mut out = BTreeMap::new();
    for line in admin.lines() {
        let l = line.trim_start().trim_start_matches("default ");
        let mut words = l.splitn(2, ' ');
        let (Some(ret), Some(rest)) = (words.next(), words.next()) else {
            continue;
        };
        let name: String = rest.chars().take_while(|c| c.is_alphanumeric()).collect();
        if ret.ends_with("Result") && !name.is_empty() && rest[name.len()..].starts_with('(') {
            out.entry(ret.to_string()).or_insert_with(|| snake(&name));
        }
    }
    Ok(out)
}

fn analyze(class: JClass, raw: &str, admin: &BTreeMap<String, String>) -> Result<ResultClass, Stop> {
    let cls = class.name.clone();
    let st = |r: String| stop(&cls, r);
    if HAND_WRITTEN.contains(&cls.as_str()) {
        return Err(st("listed as hand-written: outside every pattern (decision log 44)".into()));
    }
    let mut fields: Vec<Field> = Vec::new();
    let mut ctor_member: Option<(&JMember, Vec<JParam>, String)> = None;
    let mut nested_names = Vec::new();
    let mut constants = Vec::new();
    for m in &class.members {
        match &m.kind {
            JKind::Field { ty, name, init } => {
                if m.is_static() {
                    constants.push((name.clone(), init.clone()));
                } else if init.is_some() {
                    return Err(st(format!("field `{name}` has an initializer")));
                } else {
                    fields.push(Field {
                        java: name.clone(),
                        py: snake(name),
                        ty: parse_type(ty),
                        nullable: false,
                        optional: false,
                    });
                }
            },
            JKind::Ctor { params, body } => {
                if ctor_member.is_some() {
                    return Err(st("several constructors".into()));
                }
                ctor_member = Some((m, params.clone(), body.clone()));
            },
            JKind::Other(h) => match h.strip_prefix("class ") {
                Some(n) if m.is_public() && m.is_static() => nested_names.push(n.trim().to_string()),
                _ => return Err(st(format!("member `{h}`"))),
            },
            JKind::Method { .. } => {},
        }
    }
    let field_names: Vec<String> = fields.iter().map(|f| f.java.clone()).collect();
    let field_refs: Vec<&str> = field_names.iter().map(String::as_str).collect();
    let (ctor_m, ctor_params, ctor_body) = ctor_member.ok_or_else(|| st("no constructor".into()))?;
    let visibility = if ctor_m.is_public() {
        Visibility::Public
    } else if ctor_m.modifiers.iter().any(|m| m == "protected") {
        Visibility::Protected
    } else if ctor_m.modifiers.iter().any(|m| m == "private") {
        return Err(st("private constructor".into()));
    } else {
        Visibility::Package
    };
    // The constructor: argument checks, then one store per parameter.
    let mut checks = Vec::new();
    let mut ctor = Vec::new();
    for s in statements(&ctor_body) {
        if let Some(c) = pmatch("if (#c) throw new IllegalArgumentException(#m)", &s) {
            let mut terms = Vec::new();
            for t in c["c"].split(" && ") {
                let cmp = pmatch("$p == null", t)
                    .map(|x| (x["p"].clone(), true))
                    .or_else(|| pmatch("$p != null", t).map(|x| (x["p"].clone(), false)))
                    .filter(|(p, _)| ctor_params.iter().any(|x| &x.name == p))
                    .ok_or_else(|| st(format!("constructor check `{}` is not `&&`-joined null comparisons", c["c"])))?;
                terms.push(cmp);
            }
            if !(c["m"].starts_with('"') && c["m"].ends_with('"')) {
                return Err(st(format!("constructor check message `{}`", c["m"])));
            }
            checks.push((terms, none_spelling(&c["m"])));
            continue;
        }
        let c = pmatch("this.$f = $p", &s).ok_or_else(|| st(format!("constructor statement `{s}`")))?;
        let p = ctor_params
            .iter()
            .find(|p| p.name == c["p"])
            .ok_or_else(|| st(format!("constructor stores `{}`", c["p"])))?;
        if !field_refs.contains(&c["f"].as_str()) {
            return Err(st(format!("constructor stores into `{}`", c["f"])));
        }
        ctor.push((p.clone(), c["f"].clone()));
    }
    if ctor.len() != fields.len() || ctor.len() != ctor_params.len() {
        return Err(st("the constructor does not store each parameter in one field".into()));
    }
    // A parameter that a check needs null together with others (Java throws only
    // when both of `DeleteTopicsResult`'s maps are null) has a non-throwing null
    // branch, so its field may be `None` (Signatures, Nullability). A check that
    // throws on the parameter's null alone keeps it non-nullable.
    let alone = |p: &str| {
        checks
            .iter()
            .any(|(terms, _)| matches!(terms.as_slice(), [(x, true)] if x == p))
    };
    let together = |p: &str| {
        checks
            .iter()
            .any(|(terms, _)| terms.len() > 1 && terms.iter().any(|(x, is_null)| x == p && *is_null))
    };
    for (p, f) in &ctor {
        if together(&p.name) && !alone(&p.name) {
            if let Some(field) = fields.iter_mut().find(|x| &x.java == f) {
                field.optional = true;
            }
        }
    }
    // `Collection` is `list` only where the Java implementation builds a `List`
    // (Types); every other `Collection` stops.
    for f in &fields {
        if f.ty.mentions("Collection") && !LIST_COLLECTIONS.iter().any(|(c, x, _)| *c == cls && *x == f.java) {
            return Err(st(format!(
                "field `{}`: a `Collection` not checked to be built as no `Set` (`LIST_COLLECTIONS`)",
                f.java
            )));
        }
    }
    let ctor_doc = member_doc(ctor_m);
    let mut nested = Vec::new();
    let mut holder = None;
    for n in &nested_names {
        let reached = class.members.iter().any(|m| match &m.kind {
            JKind::Method { ret, .. } => m.is_public() && parse_type(ret).mentions(n),
            _ => false,
        });
        if reached {
            nested.push(analyze_nested(raw, &class, n).map_err(st)?);
        } else {
            check_holder(raw, &class, n).map_err(st)?;
            holder = Some(n.clone());
        }
    }
    match (&holder, constants.as_slice()) {
        (None, []) => {},
        (Some(_), [(name, Some(init))]) if name == "UNKNOWN" && init == "-1" => {},
        _ => return Err(st(format!("static fields {constants:?}"))),
    }
    let mut factories = Vec::new();
    let mut methods = Vec::new();
    for m in &class.members {
        let JKind::Method { ret, name, params, body: Some(body), .. } = &m.kind else {
            continue;
        };
        if m.is_static() {
            let c = pmatch(&format!("return new {cls}(#a);"), body)
                .ok_or_else(|| st(format!("static method `{name}` body `{body}`")))?;
            let args = split_top(&c["a"], ',');
            match params.as_slice() {
                [p] if !m.is_public() && args.len() == ctor_params.len() => {
                    factories.push((name.clone(), p.clone(), args))
                },
                _ => return Err(st(format!("static method `{name}`"))),
            }
            continue;
        }
        if m.has_annotation("Deprecated") {
            return Err(st(format!("deprecated method `{name}`")));
        }
        let pat = classify(name, params, body, &field_refs).map_err(st)?;
        let private = m.modifiers.iter().any(|x| x == "private");
        if !private && !m.is_public() {
            return Err(st(format!("package-private method `{name}`")));
        }
        methods.push(Method {
            java: name.clone(),
            py: if private {
                format!("_{}", snake(name))
            } else {
                snake(name)
            },
            params: params.clone(),
            ret: parse_type(ret),
            private,
            doc: member_doc(m),
            raw_doc: raw_javadoc(m),
            pat,
        });
    }
    // A future getter whose Javadoc says the value may be null (Signatures,
    // Nullability).
    // Only the phrases of [`NULL_PHRASES`] are understood, read in the raw
    // Javadoc (so `{@code null}` counts); any other mention of `null` stops.
    for m in &methods {
        let t = &m.raw_doc;
        if !t.contains("null") {
            continue;
        }
        let rest = NULL_PHRASES.iter().fold(t.clone(), |acc, p| acc.replace(p, ""));
        let stored = match &m.pat {
            Pat::Stored(f) => fields.iter_mut().find(|x| &x.java == f),
            _ => None,
        };
        match stored {
            _ if rest.contains("null") => {
                return Err(st(format!("method `{}`: Javadoc null wording not understood: `{t}`", m.java)));
            },
            Some(field) if t.contains("null otherwise") && field.ty.is_future() => field.nullable = true,
            Some(field) if t.contains("Otherwise return null") && field.optional => {},
            None if t.contains("fields will be null") => {},
            _ => return Err(st(format!("method `{}`: Javadoc null wording not understood: `{t}`", m.java))),
        }
    }
    // `FirstError` reads the field behind its getter.
    let getters: BTreeMap<String, String> = methods
        .iter()
        .filter_map(|m| match &m.pat {
            Pat::Stored(f) if m.params.is_empty() => Some((m.java.clone(), f.clone())),
            _ => None,
        })
        .collect();
    for m in &mut methods {
        if let Pat::FirstError { field, getter, .. } = &mut m.pat {
            *field = getters
                .get(getter.as_str())
                .cloned()
                .ok_or_else(|| st(format!("`{getter}()` is not a stored getter")))?;
        }
    }
    let admin_method = admin
        .get(&cls)
        .cloned()
        .ok_or_else(|| st("no `Admin` method returns the class".into()))?;
    Ok(ResultClass {
        class,
        fields,
        visibility,
        ctor,
        checks,
        ctor_doc,
        factories,
        methods,
        nested,
        holder,
        admin_method,
    })
}

// ---------------------------------------------------------------------------
// Layout
// ---------------------------------------------------------------------------

fn width(s: &str) -> usize {
    s.chars().count()
}

/// Fill `texts` greedily after `first`, continuation lines at column `col`.
fn fill(first: &str, col: usize, texts: &[String], limit: usize) -> Vec<String> {
    let mut lines = Vec::new();
    let mut cur = first.to_string();
    let mut empty = true;
    for t in texts {
        if empty || width(&cur) + 1 + width(t) <= limit {
            if !empty {
                cur.push(' ');
            }
            cur.push_str(t);
            empty = false;
        } else {
            lines.push(std::mem::replace(&mut cur, format!("{}{t}", " ".repeat(col))));
        }
    }
    lines.push(cur);
    lines
}

fn fits(lines: &[String], limit: usize) -> bool {
    lines.iter().all(|l| width(l) <= limit)
}

/// Cut a too long line at its first comma inside brackets, the rest aligned
/// after that bracket (`x: Mapping[K,` / `V]`).
fn split_nested(line: &str) -> Option<Vec<String>> {
    let mut stack = Vec::new();
    for (k, c) in line.char_indices() {
        match c {
            '[' => stack.push(k),
            ']' => {
                stack.pop();
            },
            ',' if line[k + 1..].starts_with(' ') => {
                let open = *stack.last()?;
                let col = width(&line[..open]) + 1;
                return Some(vec![line[..=k].to_string(), format!("{}{}", " ".repeat(col), &line[k + 2..])]);
            },
            _ => {},
        }
    }
    None
}

/// Every item but the last ends with `,`, the last with `tail`.
fn with_commas(items: &[String], tail: &str) -> Vec<String> {
    let n = items.len();
    items
        .iter()
        .enumerate()
        .map(|(k, it)| {
            if k + 1 == n {
                format!("{it}{tail}")
            } else {
                format!("{it},")
            }
        })
        .collect()
}

/// A Python `def`: one line; else aligned after `(` and filled; else (when
/// `nested`) aligned with a too long item cut inside its brackets; else a
/// hanging indent of 8.
fn py_def(indent: usize, name: &str, params: &[String], ret: &str, nested: bool) -> String {
    let head = format!("{}def {name}(", " ".repeat(indent));
    let tail = format!(") -> {ret}:");
    let one = format!("{head}{}{tail}", params.join(", "));
    if width(&one) <= WIDTH {
        return one;
    }
    let texts = with_commas(params, &tail);
    let aligned = fill(&head, width(&head), &texts, WIDTH);
    if fits(&aligned, WIDTH) {
        return aligned.join("\n");
    }
    if nested {
        let mut cut = Vec::new();
        for l in &aligned {
            match split_nested(l) {
                _ if width(l) <= WIDTH => cut.push(l.clone()),
                Some(parts) if fits(&parts, WIDTH) => cut.extend(parts),
                _ => break,
            }
        }
        if cut.len() > aligned.len() && fits(&cut, WIDTH) {
            return cut.join("\n");
        }
    }
    let hang = fill(&" ".repeat(indent + 8), indent + 8, &texts, WIDTH + SLACK);
    format!("{head}\n{}", hang.join("\n"))
}

/// A stub `def ...: ...`: one line; else the parameters on one line and
/// `) -> R: ...` aligned below them; else aligned and filled; else that
/// `) -> R: ...` line cut inside its brackets; else an item cut inside its
/// brackets with `) -> R: ...` on its own line.
fn pyi_def(indent: usize, name: &str, params: &[String], ret: &str) -> String {
    let limit = stub_limit(ret);
    let head = format!("{}def {name}(", " ".repeat(indent));
    let col = width(&head);
    let tail = format!(") -> {ret}: ...");
    let one = format!("{head}{}{tail}", params.join(", "));
    if width(&one) <= limit {
        return one;
    }
    let first = format!("{head}{}", params.join(", "));
    let own = format!("{}{tail}", " ".repeat(col));
    if width(&first) <= STUB_PARAMS_WIDTH && width(&own) <= limit {
        return format!("{first}\n{own}");
    }
    let aligned = fill(&head, col, &with_commas(params, &tail), limit);
    if fits(&aligned, limit) {
        return aligned.join("\n");
    }
    if width(&first) <= STUB_PARAMS_WIDTH {
        if let Some(parts) = split_nested(&own) {
            if fits(&parts, limit) {
                return format!("{first}\n{}", parts.join("\n"));
            }
        }
    }
    let mut lines = Vec::new();
    for l in fill(&head, col, &with_commas(params, ""), limit) {
        if width(&l) <= limit {
            lines.push(l);
        } else {
            lines.extend(split_nested(&l).unwrap_or_else(|| vec![l.clone()]));
        }
    }
    lines.push(format!("{}) -> {ret}: ...", " ".repeat(col)));
    lines.join("\n")
}

/// The width of a stub line: [`WIDTH`], and [`SLACK`] more for a sync one
/// (the reference set keeps 103-character sync stubs on one line and wraps a
/// 102-character async one).
fn stub_limit(ret: &str) -> usize {
    if ret.contains(Flavor::Sync.future()) && !ret.contains(Flavor::Async.future()) {
        WIDTH + SLACK
    } else {
        WIDTH
    }
}

/// A stub whose return type moves after its outermost `[`:
/// `def m(self) -> F[` / `    T]: ...` (`ListTransactionsResult.byBrokerId`).
fn pyi_bracket_break(indent: usize, name: &str, params: &[String], ret: &str) -> String {
    let (outer, inner) = ret.split_once('[').unwrap_or((ret, ""));
    format!(
        "{0}def {name}({1}) -> {outer}[\n{0}    {inner}: ...",
        " ".repeat(indent),
        params.join(", ")
    )
}

/// A stub `_new` or static factory: filled to [`STUB_PARAMS_WIDTH`], the tail
/// attached.
fn pyi_filled(name: &str, params: &[String], ret: &str) -> String {
    let head = format!("    def {name}(");
    fill(
        &head,
        width(&head),
        &with_commas(params, &format!(") -> {ret}: ...")),
        STUB_PARAMS_WIDTH,
    )
    .join("\n")
}

/// `<prefix><terms><close>` at `indent`: one line within [`WIDTH`] +
/// [`SLACK`], else broken before ` + ` greedily, continuation lines starting
/// with `+ `.
fn concat_lines(indent: usize, prefix: &str, terms: &[String], close: &str) -> String {
    let pad = " ".repeat(indent);
    let one = format!("{pad}{prefix}{}{close}", terms.join(" + "));
    if width(&one) <= WIDTH + SLACK {
        return one;
    }
    let mut lines = Vec::new();
    let mut cur = format!("{pad}{prefix}{}", terms[0]);
    for (k, t) in terms.iter().enumerate().skip(1) {
        let piece = if k + 1 == terms.len() {
            format!("{t}{close}")
        } else {
            t.clone()
        };
        if width(&cur) + 3 + width(&piece) <= WIDTH + SLACK {
            cur.push_str(" + ");
            cur.push_str(&piece);
        } else {
            lines.push(std::mem::replace(&mut cur, format!("{pad}+ {piece}")));
        }
    }
    lines.push(cur);
    lines.join("\n")
}

/// Wrap a long `from m import a, b` the way isort's black profile does.
fn wrap_imports(block: &str) -> String {
    block
        .split('\n')
        .map(|l| {
            let pad = " ".repeat(width(l) - width(l.trim_start()));
            match l.trim_start().split_once(" import ") {
                Some((from, names)) if from.starts_with("from ") && width(l) > IMPORT_WIDTH => {
                    let items: Vec<String> = names.split(", ").map(|n| format!("{pad}    {n},")).collect();
                    format!("{pad}{from} import (\n{}\n{pad})", items.join("\n"))
                },
                _ => l.to_string(),
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Uses {
    helpers: BTreeSet<&'static str>,
    java_str: bool,
    illegal_argument: bool,
    sub_level_error: bool,
    mapping_py: bool,
    iterable_py: bool,
    sequence_py: bool,
    mapping_pyi: bool,
    iterable_pyi: bool,
    sequence_pyi: bool,
    callable_py: bool,
    callable_pyi: bool,
}

impl Uses {
    fn note_py(&mut self, s: &str) {
        self.mapping_py |= s.contains("Mapping[");
        self.iterable_py |= s.contains("Iterable[");
        self.sequence_py |= s.contains("Sequence[");
    }

    fn note_pyi(&mut self, s: &str) {
        self.mapping_pyi |= s.contains("Mapping[");
        self.iterable_pyi |= s.contains("Iterable[");
        self.sequence_pyi |= s.contains("Sequence[");
    }

    /// A new future completed from another's `whenComplete`: Java's
    /// `new KafkaFutureImpl<>()` is `_KafkaFutureImpl`.
    fn when_complete(&mut self) {
        self.helpers.insert("_KafkaFutureImpl");
    }
}

/// Docstring text derived from Javadoc: an `Admin` link for a client one,
/// error names for exception names.
fn clean(text: &str) -> String {
    let mut t = text
        .replace("``KafkaAdminClient.", "``Admin.")
        .replace("``AdminClient.", "``Admin.")
        // Java's `{@link ...}}` typo (`DescribeConsumerGroupsResult`).
        .replace("``} ", "`` ");
    for (java, py) in [
        ("IllegalArgumentException", "IllegalArgumentError"),
        ("UnsupportedVersionException", "UnsupportedVersionError"),
        ("TopicAuthorizationException", "TopicAuthorizationError"),
    ] {
        t = t.replace(java, py);
    }
    t
}

/// A module docstring body from paragraphs, closing quotes on their own line.
fn module_body(paras: &[String]) -> String {
    let doc = docstring(0, paras, &[]);
    let doc = doc.trim_start_matches("\"\"\"").to_string();
    if doc.ends_with("\n\"\"\"") {
        doc
    } else {
        format!("{}\n\"\"\"", &doc[..doc.len() - 3])
    }
}

struct Renderer<'a> {
    r: &'a ResultClass,
    prose: &'a Prose,
    types: Types<'a>,
    uses: Uses,
}

impl Renderer<'_> {
    fn name(&self) -> &str {
        &self.r.class.name
    }

    fn st(&self, reason: String) -> Stop {
        stop(self.name(), reason)
    }

    fn field(&self, java: &str) -> Field {
        self.r
            .fields
            .iter()
            .find(|f| f.java == java)
            .cloned()
            .expect("an analyzed field")
    }

    fn ty(&mut self, t: &JT, flavor: Flavor, top: bool, nullable: bool) -> Result<String, Stop> {
        let name = self.name().to_string();
        self.types.py(t, flavor, top, nullable).map_err(|e| stop(&name, e))
    }

    fn arg_ty(&mut self, t: &JT, path: &[usize], flavor: Flavor, top: bool) -> Result<String, Stop> {
        let mut cur = t.clone();
        for &k in path {
            cur = cur.arg(k).map_err(|e| self.st(e))?.clone();
        }
        self.ty(&cur, flavor, top, false)
    }

    /// Whether the class holds a future of a map of futures
    /// (`ListTransactionsResult`): `_BrokerFutures` in Python.
    fn broker_futures(&self) -> bool {
        self.r
            .methods
            .iter()
            .any(|m| matches!(m.pat, Pat::CopyOnComplete(_) | Pat::AllByKey(_)))
    }

    fn field_ty(&mut self, f: &Field, flavor: Flavor, top: bool) -> Result<String, Stop> {
        self.ty(&f.ty, flavor, top, f.nullable)
    }

    /// The return type of a method.
    fn ret_ty(&mut self, m: &Method, flavor: Flavor) -> Result<String, Stop> {
        match &m.pat {
            Pat::Stored(f) | Pat::Copy(f) => {
                let f = self.field(f);
                // A stored map is annotated as Java's `Map` at run time; the
                // stubs say `dict`, which `_new` stores.
                let nullable_map = f.ty.is_map() && f.optional;
                let top = matches!(m.pat, Pat::Stored(_)) && flavor == Flavor::Any && !nullable_map;
                let t = self.field_ty(&f, flavor, top)?;
                // `DeleteTopicsResult`: exactly one of the maps is set.
                Ok(if nullable_map { format!("{t} | None") } else { t })
            },
            _ => self.ty(&m.ret, flavor, false, false),
        }
    }

    fn param_list(&mut self, m: &Method, flavor: Flavor) -> Result<Vec<String>, Stop> {
        let mut out = Vec::new();
        for p in &m.params {
            let ty = self.ty(&parse_type(&p.ty), flavor, true, false)?;
            out.push(format!("{}: {ty}", snake(&p.name)));
        }
        Ok(out)
    }

    fn prose_or(&self, slot: &str, indent: usize, make: impl FnOnce() -> Option<String>) -> Option<String> {
        match self.prose.get(self.name(), slot) {
            Some(lines) => Some(indent_literal(lines, indent)),
            None => make(),
        }
    }

    fn method_doc(&self, m: &Method) -> Option<String> {
        self.prose_or(&format!("method {}", m.py), 8, || {
            if m.doc.is_empty() {
                return None;
            }
            let paras: Vec<String> = m.doc.paragraphs.iter().map(|p| clean(p)).collect();
            let mut fields: Vec<String> = m
                .doc
                .params
                .iter()
                .map(|(n, t)| format!(":param {}: {}", snake(n), clean(t)))
                .collect();
            if let Some(r) = &m.doc.returns {
                fields.push(format!(":return: {}", clean(r)));
            }
            Some(docstring(8, &paras, &fields))
        })
    }

    fn class_doc(&self) -> String {
        self.prose_or("class", 4, || {
            let doc = self.r.class.javadoc.as_deref().map(read_doc).unwrap_or_else(|| Doc {
                paragraphs: vec![format!("The result of the ``Admin.{}()`` call.", self.r.admin_method)],
                ..Doc::default()
            });
            // Each paragraph a sentence (some Javadoc first lines lack the stop).
            let mut paras: Vec<String> = doc
                .paragraphs
                .iter()
                .filter(|p| !p.starts_with("The API of this class is evolving"))
                .map(|p| {
                    let p = clean(p);
                    if p.ends_with('.') {
                        p
                    } else {
                        format!("{p}.")
                    }
                })
                .collect();
            paras.push(format!("Java: ``{}``.", self.r.class.fqcn()));
            Some(docstring(4, &paras, &[]))
        })
        .unwrap_or_default()
    }

    fn module_doc(&self) -> String {
        let title = module_title(self.name(), &self.r.class.fqcn());
        if let Some(lines) = self.prose.get(self.name(), "module") {
            return format!("{title}\n\n{}", lines.join("\n"));
        }
        let mut text = match self.r.visibility {
            Visibility::Public => "Java's constructor is public, so it stays the keyword-only ``__init__``; the \
                                   client-kind marker is read from the futures given (``AsyncKafkaFuture``\\\\ s: the \
                                   async kind, else the sync one), and the clients build it through ``_new``, which \
                                   sets the kind explicitly, as for every result class (CLAUDE.md, Python Binding \
                                   Conventions, Class family)."
                .to_string(),
            v => format!(
                "Java's constructor is {}: ``__init__`` raises ``TypeError`` and ``_new`` builds it (CLAUDE.md, Python \
                 Binding Conventions, Class family).",
                v.java()
            ),
        };
        let mut extra: Vec<String> = Vec::new();
        for m in &self.r.methods {
            let j = &m.java;
            match &m.pat {
                Pat::Copy(_) => extra.push(format!(
                    "``{j}()`` returns a copy of the stored map, so each call gives a new ``dict``."
                )),
                Pat::AllOf(_) => extra.push(format!(
                    "``{j}()`` is Java's ``KafkaFuture.allOf``, the ``all_of`` of the kind's future class (Futures)."
                )),
                Pat::Collect { .. } => extra.push(format!(
                    "``{j}()`` is Java's ``allOf(...).thenApply(...)``, through the futures' ``all_of`` and \
                     ``then_apply``."
                )),
                Pat::Lookup { .. } => extra.push(format!(
                    "``{j}()`` raises ``IllegalArgumentError`` with Java's message for a key that was not part of the \
                     call."
                )),
                Pat::Relay { .. } | Pat::FirstError { .. } => extra.push(format!(
                    "``{j}()`` makes a new future completed from the stored future's ``when_complete``: Java's \
                     ``new KafkaFutureImpl<>()`` is the private ``_KafkaFutureImpl``."
                )),
                _ => {},
            }
        }
        let field_types: Vec<&JT> = self.r.fields.iter().map(|f| &f.ty).collect();
        if field_types.iter().any(|t| t.mentions("Collection")) {
            extra.push(
                "A ``Collection`` yields a ``list``: Java's ``KafkaAdminClient`` builds no ``Set`` for it (Types)."
                    .into(),
            );
        }
        if field_types.iter().any(|t| t.mentions("Optional")) {
            extra.push("Java's ``Optional<Throwable>`` is ``BaseException | None`` (Types).".into());
        }
        for n in &self.r.nested {
            extra.push(format!(
                "Java's public nested ``{0}`` is the nested class ``{1}.{0}`` (Idiom translations); it has no \
                 ``equals`` / ``hashCode`` in Java, so neither here.",
                n.name,
                self.name()
            ));
        }
        for e in extra {
            text.push(' ');
            text.push_str(&e);
        }
        format!("{title}\n\n{}", module_body(&[text]))
    }

    // -- members -------------------------------------------------------------

    fn slots(&self, indent: usize, names: &[String]) -> String {
        let pad = " ".repeat(indent);
        let quoted: Vec<String> = names.iter().map(|n| format!("\"_{n}\"")).collect();
        if quoted.len() == 1 {
            return format!("{pad}__slots__ = ({},)", quoted[0]);
        }
        let head = format!("{pad}__slots__ = (");
        let one = format!("{head}{})", quoted.join(", "));
        if width(&one) <= WIDTH {
            return one;
        }
        fill(&head, width(&head), &with_commas(&quoted, ")"), WIDTH).join("\n")
    }

    fn type_error_init(&self) -> String {
        let cls = self.name();
        let m = &self.r.admin_method;
        let long = format!("\"{cls} cannot be constructed directly; use the admin client's \"");
        let (a, b) = if 12 + width(&long) <= TYPE_ERROR_WIDTH {
            (long, format!("\"{m}()\")"))
        } else {
            (
                format!("\"{cls} cannot be constructed directly; use the admin \""),
                format!("\"client's {m}()\")"),
            )
        };
        format!(
            "    def __init__(self, *args: Any, **kwargs: Any) -> None:\n        raise TypeError(\n            {a}\n            \
             {b}"
        )
    }

    /// The constructor's keyword parameters (`_new`, a public `__init__`).
    fn ctor_params(&mut self, flavor: Flavor, stub: bool) -> Result<Vec<String>, Stop> {
        let mut out = Vec::new();
        for (p, f) in self.r.ctor.clone() {
            let field = self.field(&f);
            let mut t = if self.broker_futures() && !stub {
                "KafkaFuture[_BrokerFutures] | AsyncKafkaFuture[_BrokerFutures]".to_string()
            } else if self.broker_futures() {
                // A future of a map of futures: the map is a parameter's map.
                let k = self.arg_ty(&field.ty, &[0, 0], flavor, false)?;
                let v = self.arg_ty(&field.ty, &[0, 1], flavor, false)?;
                if flavor == Flavor::Any {
                    format!("KafkaFuture[Mapping[{k}, {v}]] | AsyncKafkaFuture[Mapping[{k}, {v}]]")
                } else {
                    format!("{}[Mapping[{k}, {v}]]", flavor.future())
                }
            } else {
                self.field_ty(&field, flavor, true)?
            };
            if field.optional {
                t.push_str(" | None");
            }
            out.push(format!("{}: {t}", snake(&p.name)));
        }
        Ok(out)
    }

    fn store(&self, target: &str, p: &JParam, f: &Field) -> String {
        let p = snake(&p.name);
        let value = if f.ty.is_map() {
            format!("dict({p})")
        } else if f.ty.name == "Set" {
            format!("set({p})")
        } else {
            p.clone()
        };
        // A copy keeps a `None` as `None`, as Java's constructor stores a null
        // (CLAUDE.md, Python Binding Conventions, Class family).
        if f.optional || value != p {
            format!("{target}._{} = None if {p} is None else {value}", f.py)
        } else {
            format!("{target}._{} = {value}", f.py)
        }
    }

    /// The constructor Javadoc's `@param`s as docstring fields.
    fn ctor_param_fields(&self) -> Vec<String> {
        self.r
            .ctor_doc
            .params
            .iter()
            .map(|(n, t)| format!(":param {}: {}", snake(n), clean(t)))
            .collect()
    }

    fn java_ctor(&self) -> String {
        let ps: Vec<&str> = self.r.ctor.iter().map(|(p, _)| p.name.as_str()).collect();
        format!("{}({})", self.name(), ps.join(", "))
    }

    fn new_py(&mut self) -> Result<String, Stop> {
        let mut params = vec!["cls".to_string(), "*".to_string()];
        let ps = self.ctor_params(Flavor::Any, false)?;
        for p in &ps {
            self.uses.note_py(p);
        }
        params.extend(ps);
        params.push("kind: type[Sync] | type[Async]".into());
        let sig = py_def(4, "_new", &params, &format!("{}[Any]", self.name()), false);
        let doc = match self.r.visibility {
            Visibility::Public => format!(
                "The clients' builder: Java's public ``{}`` with the client kind set by the caller.",
                self.java_ctor()
            ),
            v => format!("Java's {} ``{}``.", v.java(), self.java_ctor()),
        };
        // Java's constructor `@param`s follow, as for methods. Without them: one
        // line when it fits the code width, else wrapped as a docstring.
        let fields = self.ctor_param_fields();
        let one = format!("        \"\"\"{doc}\"\"\"");
        let mut body = vec![if fields.is_empty() && width(&one) <= WIDTH {
            one
        } else {
            docstring(8, &[doc], &fields)
        }];
        for (terms, msg) in self.r.checks.clone() {
            let cond: Vec<String> = terms
                .iter()
                .map(|(p, is_null)| format!("{} is {}None", snake(p), if *is_null { "" } else { "not " }))
                .collect();
            self.uses.illegal_argument = true;
            body.push(format!(
                "        if {}:\n            raise IllegalArgumentError(\n                message={msg})",
                cond.join(" and ")
            ));
        }
        body.push("        result = cls.__new__(cls)".into());
        for (p, f) in self.r.ctor.clone() {
            let field = self.field(&f);
            body.push(format!("        {}", self.store("result", &p, &field)));
        }
        body.push("        result._kind = kind".into());
        if self.r.holder.is_some() {
            body.push("        result._refine = None".into());
        }
        body.push("        return result".into());
        Ok(format!("    @classmethod\n{sig}\n{}", body.join("\n")))
    }

    /// `CreateTopicsResult`'s client builder: the futures are the core's
    /// per-topic `values()` futures, and each per-topic getter reads the core's
    /// own future through `refine(topic, accessor)` (CLAUDE.md, Python Binding
    /// Conventions, Futures: no Python-side check for a topic not in the
    /// request).
    fn of_client_py(&mut self) -> String {
        let cls = self.name().to_string();
        let params = [
            "cls".to_string(),
            "*".to_string(),
            "futures: Mapping[str, KafkaFuture[Any] | AsyncKafkaFuture[Any]]".to_string(),
            "refine: Callable[[str, str], Any]".to_string(),
            "kind: type[Sync] | type[Async]".to_string(),
        ];
        self.uses.mapping_py = true;
        self.uses.callable_py = true;
        let sig = py_def(4, "_of_client", &params, &format!("{cls}[Any]"), false);
        let doc = docstring(
            8,
            &[
                "The client's result: ``futures`` are the core's per-topic ``values()`` futures, and the per-topic \
               getters return the core's own future of the topic, ``refine(topic, accessor)``, which fails with \
               ``IllegalArgumentError`` for a topic not in the request."
                    .to_string(),
            ],
            &[],
        );
        format!(
            "    @classmethod\n{sig}\n{doc}\n        result = cls._new(futures=futures, kind=kind)\n        \
             result._refine = refine\n        return result"
        )
    }

    fn public_init_py(&mut self) -> Result<String, Stop> {
        let mut params = vec!["self".to_string(), "*".to_string()];
        let ps = self.ctor_params(Flavor::Any, false)?;
        for p in &ps {
            self.uses.note_py(p);
        }
        params.extend(ps);
        let sig = py_def(4, "__init__", &params, "None", true);
        let mut body = Vec::new();
        // Java's constructor Javadoc, as a method's.
        let paras: Vec<String> = self.r.ctor_doc.paragraphs.iter().map(|p| clean(p)).collect();
        let fields = self.ctor_param_fields();
        if !paras.is_empty() || !fields.is_empty() {
            body.push(docstring(8, &paras, &fields));
        }
        let [f] = self.r.fields.as_slice() else {
            return Err(self.st("a public constructor with several fields".into()));
        };
        // The kind is read from the futures, so they are required: the one
        // constructor that does not store a `None` (Class family).
        let (f_py, f_optional) = (f.py.clone(), f.optional);
        let mut guarded = None;
        if !f_optional {
            let ctor = self.r.ctor.clone();
            if let Some((p, _)) = ctor.iter().find(|(_, g)| self.field(g).py == f_py) {
                let p = snake(&p.name);
                body.push(format!(
                    "        if {p} is None:\n            raise TypeError(\"{p} must not be None\")"
                ));
                guarded = Some(p);
            }
        }
        for (p, f) in self.r.ctor.clone() {
            let field = self.field(&f);
            let mut store = self.store("self", &p, &field);
            if let Some(g) = &guarded {
                store = store.replace(&format!("None if {g} is None else "), "");
            }
            body.push(format!("        {store}"));
        }
        let f = &self.r.fields[0];
        let futures = if f.ty.is_map() {
            format!("self._{}.values()", f.py)
        } else {
            format!("(self._{},)", f.py)
        };
        self.uses.helpers.insert("_kind_of");
        body.push(format!("        self._kind: type[Sync] | type[Async] = _kind_of({futures})"));
        Ok(format!("{sig}\n{}", body.join("\n")))
    }

    fn factory_py(&mut self, name: &str, p: &JParam, args: &[String]) -> Result<String, Stop> {
        let cls = self.name().to_string();
        let ty = self.ty(&parse_type(&p.ty), Flavor::Any, true, false)?;
        self.uses.note_py(&ty);
        let params = vec![
            "*".to_string(),
            format!("{}: {ty}", snake(&p.name)),
            "kind: type[Sync] | type[Async]".into(),
        ];
        let sig = py_def(4, &format!("_{}", snake(name)), &params, &format!("{cls}[Any]"), false);
        let doc = docstring(8, &[format!("Java's package-private ``{name}({})``.", p.name)], &[]);
        let kw: Vec<String> = self
            .r
            .ctor
            .iter()
            .zip(args)
            .map(|((cp, _), a)| format!("{}={}", snake(&cp.name), if a == "null" { "None".into() } else { snake(a) }))
            .collect();
        // The private `kind` keyword goes on a line of its own.
        let head = format!("        return {cls}._new(");
        let call = fill(&head, width(&head), &with_commas(&kw, ","), WIDTH);
        Ok(format!(
            "    @staticmethod\n{sig}\n{doc}\n{}\n{}kind=kind)",
            call.join("\n"),
            " ".repeat(width(&head))
        ))
    }

    /// A Java concatenation's terms as Python (`java_str` for a non-`String`).
    fn py_terms(&mut self, terms: &[Term]) -> Vec<String> {
        terms
            .iter()
            .map(|t| match t {
                Term::Lit(l) => l.clone(),
                // A `String` parameter through `java_str` too: Java's concatenation
                // spells a `null` (as Python's `None`), which a bare `+` cannot.
                Term::Var(v) => {
                    self.uses.java_str = true;
                    format!("java_str({})", snake(v))
                },
            })
            .collect()
    }

    fn action_def(&mut self, value: &str, value_ty: &str, error: &str) -> String {
        let params = [
            format!("{value}: {value_ty} | None"),
            format!("{error}: BaseException | None"),
        ];
        py_def(8, "action", &params, "None", false)
    }

    fn method_body(&mut self, m: &Method, ret: &str) -> Result<String, Stop> {
        let sf = |r: &Self, j: &str| format!("self._{}", r.field(j).py);
        Ok(match &m.pat {
            Pat::Stored(j) => format!("        return {}", sf(self, j)),
            Pat::Copy(j) => format!("        return dict({})", sf(self, j)),
            Pat::AllOf(j) => format!(
                "        return self._kind._FUTURE.all_of(futures=tuple({}.values()))",
                sf(self, j)
            ),
            Pat::Collect { field, key, var } => {
                let fd = self.field(field);
                let local = fd.py.clone();
                let var = if var.is_empty() {
                    match fd.ty.arg(0).map_err(|e| self.st(e))?.name.as_str() {
                        "TopicPartition" => "partition".to_string(),
                        "ConfigResource" => "resource".to_string(),
                        "Integer" => "broker_id".to_string(),
                        _ => "key".to_string(),
                    }
                } else {
                    snake(var)
                };
                let key_expr = match key {
                    KeyOf::Key => var.clone(),
                    KeyOf::IdValue => format!("{var}.id_value"),
                };
                let collected = self.arg_ty(&m.ret, &[0], Flavor::Any, false)?;
                COLLECT
                    .replace("{L}", &local)
                    .replace("{T}", &collected)
                    .replace("{K}", &key_expr)
                    .replace("{V}", &var)
            },
            Pat::Lookup { field, param, coordinator, message } => {
                self.uses.illegal_argument = true;
                let terms = self.py_terms(message);
                let p = snake(param);
                let mut s = String::new();
                let key = if *coordinator {
                    s.push_str(&format!("        key = _CoordinatorKey.by_transactional_id({p}={p})\n"));
                    "key".to_string()
                } else {
                    p
                };
                s.push_str(&format!(
                    "        future = {}.get({key})\n        if future is None:\n            raise \
                     IllegalArgumentError(\n{}\n        return future",
                    sf(self, field),
                    concat_lines(16, "message=", &terms, ")")
                ));
                s
            },
            Pat::MapView { field, keys } => {
                let fd = self.field(field);
                let value = fd.ty.arg(0).and_then(|t| t.arg(1)).map_err(|e| self.st(e))?.name.clone();
                let v = format!("{}s", snake(&value).rsplit('_').next().unwrap_or("value"));
                let expr = if *keys {
                    format!("set({v}.keys())")
                } else {
                    format!("list({v}.values())")
                };
                format!("        return {}.then_apply(function=lambda {v}: {expr})", sf(self, field))
            },
            Pat::Relay { field, value, error } => {
                self.uses.when_complete();
                let fd = self.field(field);
                let inner = self.arg_ty(&fd.ty, &[0], Flavor::Any, false)?;
                let action = self.action_def(&snake(value), &inner, &snake(error));
                let made = self.arg_ty(&m.ret, &[0], Flavor::Any, false)?;
                RELAY
                    .replace("{ACTION}", &action)
                    .replace("{R}", &made)
                    .replace("{F}", &sf(self, field))
                    .replace("{V}", &snake(value))
                    .replace("{E}", &snake(error))
            },
            Pat::FirstError { field, getter, value, error, item } => {
                self.uses.when_complete();
                let fd = self.field(field);
                let inner = self.arg_ty(&fd.ty, &[0], Flavor::Any, true)?;
                self.uses.note_py(&inner);
                let action = self.action_def(&snake(value), &inner, &snake(error));
                let made = self.arg_ty(&m.ret, &[0], Flavor::Any, false)?;
                FIRST_ERROR
                    .replace("{ACTION}", &action)
                    .replace("{R}", &made)
                    .replace("{F}", &sf(self, field))
                    .replace("{G}", &snake(getter))
                    .replace("{V}", &snake(value))
                    .replace("{E}", &snake(error))
                    .replace("{X}", &snake(item))
            },
            Pat::ValuesToVoid(field) => {
                let value = self.arg_ty(&m.ret, &[1], Flavor::Any, false)?;
                VALUES_TO_VOID
                    .replace("{R}", ret)
                    .replace("{V}", &value)
                    .replace("{F}", &sf(self, field))
            },
            Pat::PerKeyGetter { param, getter, .. } => {
                let holder = self.r.holder.clone().unwrap_or_default();
                let (p, g) = (snake(param), snake(getter));
                format!(
                    "        if self._refine is not None:\n            return self._refine({p}, \"{g}\")\n        return \
                     self._future_of({p}).then_apply(function=_{holder}.{g})"
                )
            },
            Pat::EitherAllOf { first, second } => {
                let (a, b) = (sf(self, first), sf(self, second));
                format!(
                    "        if {a} is None:\n            assert {b} is not None\n            return \
                     self._kind._FUTURE.all_of(futures=tuple({b}.values()))\n        return \
                     self._kind._FUTURE.all_of(futures=tuple({a}.values()))"
                )
            },
            Pat::SubErrorOne { future, partitions, message } => {
                self.uses.when_complete();
                self.uses.illegal_argument = true;
                let terms = self.py_terms(message);
                let errs = self.sub_error_map(future)?;
                let action = self.action_def("topic_partitions", &errs, "throwable");
                SUB_ERROR_ONE
                    .replace("{MSG}", &concat_lines(16, "message=", &terms, ")"))
                    .replace("{ACTION}", &action)
                    .replace("{S}", &sf(self, partitions))
                    .replace("{F}", &sf(self, future))
            },
            Pat::SubErrorAll { future, partitions } => {
                self.uses.when_complete();
                let errs = self.sub_error_map(future)?;
                let action = self.action_def("topic_partitions", &errs, "throwable");
                SUB_ERROR_ALL
                    .replace("{ACTION}", &action)
                    .replace("{S}", &sf(self, partitions))
                    .replace("{F}", &sf(self, future))
            },
            Pat::SubErrorHelper { message } => {
                self.uses.sub_level_error = true;
                let terms = self.py_terms(message);
                let errors = snake(&m.params[0].name);
                format!(
                    "        assert {errors} is not None\n        exception = _sub_level_error(\n            {errors}, \
                     {},\n{})\n        if exception is not None:\n            {}.complete_with_error(exception)\n            \
                     return True\n        return False",
                    snake(&m.params[1].name),
                    concat_lines(12, "", &terms, ""),
                    snake(&m.params[2].name)
                )
            },
            Pat::Flatten { getter } => {
                self.uses.mapping_py = true;
                let elem = self.arg_ty(&m.ret, &[0, 0], Flavor::Any, false)?;
                FLATTEN.replace("{T}", &elem).replace("{G}", &snake(getter))
            },
            Pat::CopyOnComplete(field) => {
                self.uses.when_complete();
                let made = self.arg_ty(&m.ret, &[0], Flavor::Any, false)?;
                COPY_ON_COMPLETE.replace("{R}", &made).replace("{F}", &sf(self, field))
            },
            Pat::AllByKey(field) => {
                self.uses.when_complete();
                let elem = self.arg_ty(&m.ret, &[0, 1], Flavor::Any, false)?;
                let made = self.arg_ty(&m.ret, &[0], Flavor::Any, false)?;
                ALL_BY_KEY
                    .replace("{R}", &made)
                    .replace("{F}", &sf(self, field))
                    .replace("{T}", &elem)
            },
        })
    }

    fn sub_error_map(&mut self, future: &str) -> Result<String, Stop> {
        let fd = self.field(future);
        let t = self.arg_ty(&fd.ty, &[0], Flavor::Any, true)?;
        self.uses.note_py(&t);
        Ok(t)
    }

    fn method_py(&mut self, m: &Method) -> Result<String, Stop> {
        let ret = self.ret_ty(m, Flavor::Any)?;
        self.uses.note_py(&ret);
        let mut params = vec!["self".to_string()];
        let mut ps = self.param_list(m, Flavor::Any)?;
        if let Pat::SubErrorHelper { .. } = m.pat {
            // The map argument is the `whenComplete` value, `None` on failure;
            // Java's `KafkaFutureImpl` result is the private `_KafkaFutureImpl`.
            ps[0].push_str(" | None");
            if let Some(last) = ps.last_mut() {
                if let Some((name, _)) = last.split_once(':') {
                    *last = format!("{name}: _KafkaFutureImpl[None]");
                }
            }
        } else if !ps.is_empty() {
            params.push("*".into());
        }
        for p in &ps {
            self.uses.note_py(p);
        }
        params.extend(ps);
        let sig = py_def(4, &m.py, &params, &ret, false);
        let body = self.method_body(m, &ret)?;
        Ok(match self.method_doc(m) {
            Some(d) => format!("{sig}\n{d}\n{body}"),
            None => format!("{sig}\n{body}"),
        })
    }

    fn method_pyi(&mut self, m: &Method) -> Result<Option<String>, Stop> {
        if m.private {
            return Ok(None);
        }
        let cls = self.name().to_string();
        let mut out = Vec::new();
        for (flavor, kind) in [(Flavor::Sync, "Sync"), (Flavor::Async, "Async")] {
            let ret = self.ret_ty(m, flavor)?;
            let mut params = vec![format!("self: {cls}[{kind}]")];
            let ps = self.param_list(m, flavor)?;
            if !ps.is_empty() {
                params.push("*".into());
            }
            params.extend(ps);
            for p in &params {
                self.uses.note_pyi(p);
            }
            self.uses.note_pyi(&ret);
            let def = if matches!(m.pat, Pat::CopyOnComplete(_)) {
                pyi_bracket_break(4, &m.py, &params, &ret)
            } else {
                pyi_def(4, &m.py, &params, &ret)
            };
            out.push(format!("    @overload\n{def}"));
        }
        Ok(Some(out.join("\n")))
    }

    fn nested_params(&mut self, n: &Nested, stub: bool) -> Result<Vec<String>, Stop> {
        let mut params = vec!["self".to_string(), "*".to_string()];
        for (f, t) in &n.fields {
            let ty = self.ty(t, Flavor::Any, true, false)?;
            if stub {
                self.uses.note_pyi(&ty);
            } else {
                self.uses.note_py(&ty);
            }
            params.push(format!("{}: {ty}", snake(f)));
        }
        Ok(params)
    }

    fn nested_getter_ty(&mut self, n: &Nested, field: &str) -> Result<String, Stop> {
        let t = n.fields.iter().find(|(x, _)| x == field).map(|(_, t)| t.clone());
        let t = t.ok_or_else(|| self.st(format!("nested `{}`: no field `{field}`", n.name)))?;
        self.ty(&t, Flavor::Any, false, false)
    }

    fn nested_py(&mut self, n: &Nested) -> Result<String, Stop> {
        let doc = self
            .prose_or(&format!("nested {}", n.name), 8, || {
                let mut paras: Vec<String> = n.doc.paragraphs.iter().map(|p| clean(p)).collect();
                paras.push(format!("Java: ``{}``.", n.fqcn));
                Some(docstring(8, &paras, &[]))
            })
            .unwrap_or_default();
        let names: Vec<String> = n.fields.iter().map(|(f, _)| snake(f)).collect();
        let mut s = format!("    class {}:\n{doc}\n\n{}\n\n", n.name, self.slots(8, &names));
        let params = self.nested_params(n, false)?;
        s.push_str(&py_def(8, "__init__", &params, "None", false));
        for (f, t) in &n.fields {
            // A copy keeps a `None` as `None`, as Java stores a null (Class family).
            let v = if matches!(t.name.as_str(), "List" | "Collection") {
                format!("None if {0} is None else list({0})", snake(f))
            } else {
                snake(f)
            };
            s.push_str(&format!("\n            self._{} = {v}", snake(f)));
        }
        for (g, f, doc) in n.getters.clone() {
            let ty = self.nested_getter_ty(n, &f)?;
            s.push_str(&format!("\n\n{}", py_def(8, &snake(&g), &["self".into()], &ty, false)));
            if !doc.is_empty() {
                s.push_str(&format!("\n{}", docstring(12, &doc.paragraphs, &[])));
            }
            s.push_str(&format!("\n            return self._{}", snake(&f)));
        }
        if !n.to_string.is_empty() {
            s.push_str(&format!("\n\n        def __str__(self) -> str:\n{}", self.str_body(n)));
        }
        Ok(s)
    }

    /// Java's concatenation `toString`: one line, else one literal and the
    /// value after it per line, in parentheses. An `Optional` field prints as
    /// Java's `Optional.toString()`.
    fn str_body(&mut self, n: &Nested) -> String {
        self.uses.java_str = true;
        let mut pre = String::new();
        let mut atoms: Vec<String> = Vec::new();
        for t in &n.to_string {
            match t {
                Term::Lit(l) => atoms.push(l.clone()),
                Term::Var(v) => {
                    let optional = n.fields.iter().any(|(x, t)| x == v && t.name == "Optional");
                    let f = snake(v);
                    if optional {
                        let local = f.rsplit('_').next().unwrap_or("value").to_string();
                        let pad = " ".repeat(12 + width(&local) + 4);
                        pre.push_str(&format!(
                            "            {local} = (\"Optional.empty\" if self._{f} is None\n{pad}else \"Optional[\" + \
                             java_str(self._{f}) + \"]\")\n"
                        ));
                        atoms.push(local);
                    } else {
                        atoms.push(format!("java_str(self._{f})"));
                    }
                },
            }
        }
        let one = format!("            return {}", atoms.join(" + "));
        if width(&one) <= WIDTH {
            return format!("{pre}{one}");
        }
        let mut lines: Vec<String> = Vec::new();
        let mut cur = String::new();
        let n_atoms = atoms.len();
        for (k, a) in atoms.iter().enumerate() {
            let starts_group = a.starts_with('"') && !cur.is_empty() && !cur.ends_with('"') && k + 1 < n_atoms;
            if starts_group {
                lines.push(std::mem::take(&mut cur));
            }
            if !cur.is_empty() {
                cur.push_str(" + ");
            }
            cur.push_str(a);
        }
        lines.push(cur);
        if lines.iter().any(|l| 20 + width(l) + 2 > WIDTH) {
            // A group too long for a line: filled atom by atom instead.
            lines = vec![atoms[0].clone()];
            for (k, a) in atoms.iter().enumerate().skip(1) {
                let close = if k + 1 == n_atoms { 2 } else { 0 };
                let last = lines.last_mut().expect("one line");
                if 20 + width(last) + 3 + width(a) + close <= WIDTH {
                    last.push_str(" + ");
                    last.push_str(a);
                } else {
                    lines.push(a.clone());
                }
            }
        }
        let mut out = format!("{pre}            return ({}", lines[0]);
        for l in &lines[1..] {
            out.push_str(&format!("\n                    + {l}"));
        }
        out.push(')');
        out
    }

    fn nested_pyi(&mut self, n: &Nested) -> Result<String, Stop> {
        let params = self.nested_params(n, true)?;
        let mut s = format!("    class {}:\n{}", n.name, pyi_def(8, "__init__", &params, "None"));
        for (g, f, _) in n.getters.clone() {
            let ty = self.nested_getter_ty(n, &f)?;
            self.uses.note_pyi(&ty);
            s.push_str(&format!("\n{}", pyi_def(8, &snake(&g), &["self".into()], &ty)));
        }
        Ok(s)
    }

    fn future_of_py(&mut self) -> Result<Option<String>, Stop> {
        let found = self.r.methods.iter().find_map(|m| match &m.pat {
            Pat::PerKeyGetter { field, param, .. } => Some((field.clone(), param.clone())),
            _ => None,
        });
        let Some((field, param)) = found else { return Ok(None) };
        self.uses.illegal_argument = true;
        let f = self.field(&field);
        let v = self.arg_ty(&f.ty, &[1], Flavor::Any, false)?;
        let p = snake(&param);
        let mut key = p.clone();
        if let Some(first) = key.get_mut(0..1) {
            first.make_ascii_uppercase();
        }
        let method = camel(&self.r.admin_method);
        Ok(Some(format!(
            "    def _future_of(self, {p}: str) -> {v}:\n        future = self._{}.get({p})\n        if future is None:\n            \
             # ``{}.get({param}).thenApply(...)`` on a {p} not in the call: Java's\n            \
             # NullPointerException, with the core's message.\n            raise IllegalArgumentError(\n                \
             message=f\"{key} {{{p}}} was not part of the {method} request\")\n        \
             return future",
            f.py, f.java
        )))
    }

    fn render(&mut self) -> Result<(String, String), Stop> {
        let cls = self.name().to_string();
        let public = self.r.visibility == Visibility::Public;
        let methods = self.r.methods.clone();

        let mut py_members: Vec<String> = Vec::new();
        let mut pyi_members: Vec<String> = Vec::new();
        for n in self.r.nested.clone() {
            py_members.push(self.nested_py(&n)?);
            pyi_members.push(format!("{}\n", self.nested_pyi(&n)?));
        }
        let mut slot_names: Vec<String> = self.r.fields.iter().map(|f| f.py.clone()).collect();
        slot_names.push("kind".into());
        if self.r.holder.is_some() {
            slot_names.push("refine".into());
        }
        py_members.push(self.slots(4, &slot_names));
        py_members.push(if public {
            self.public_init_py()?
        } else {
            self.type_error_init()
        });
        py_members.push(self.new_py()?);
        if self.r.holder.is_some() {
            let of_client = self.of_client_py();
            py_members.push(of_client);
        }
        for (name, p, args) in self.r.factories.clone() {
            py_members.push(self.factory_py(&name, &p, &args)?);
        }
        for m in &methods {
            py_members.push(self.method_py(m)?);
        }
        if let Some(s) = self.future_of_py()? {
            py_members.push(s);
        }

        if public {
            for (flavor, kind) in [(Flavor::Sync, "Sync"), (Flavor::Async, "Async")] {
                let mut params = vec![format!("self: {cls}[{kind}]"), "*".to_string()];
                params.extend(self.ctor_params(flavor, true)?);
                for p in &params {
                    self.uses.note_pyi(p);
                }
                pyi_members.push(format!("    @overload\n{}", pyi_def(4, "__init__", &params, "None")));
            }
        }
        let mut params = vec!["cls".to_string(), "*".to_string()];
        params.extend(self.ctor_params(Flavor::Any, true)?);
        params.push("kind: type[KindT]".into());
        for p in &params {
            self.uses.note_pyi(p);
        }
        let new = if self.broker_futures() {
            // A future of a map of futures: the keywords start on a line of
            // their own.
            let rest = pyi_filled("_new", &params[2..], &format!("{cls}[KindT]"));
            format!(
                "    def _new(cls, *,\n{}{}",
                " ".repeat(13),
                rest.trim_start().trim_start_matches("def _new(")
            )
        } else {
            pyi_filled("_new", &params, &format!("{cls}[KindT]"))
        };
        pyi_members.push(format!("    @classmethod\n{new}"));
        if self.r.holder.is_some() {
            self.uses.mapping_pyi = true;
            self.uses.callable_pyi = true;
            pyi_members.push(format!(
                "    @classmethod\n    def _of_client(cls, *, futures: Mapping[str, KafkaFuture[Any] | \
                 AsyncKafkaFuture[Any]],\n                   refine: Callable[[str, str], Any],\n                   \
                 kind: type[KindT]) -> {cls}[KindT]: ..."
            ));
        }
        for (name, p, _) in self.r.factories.clone() {
            let ty = self.ty(&parse_type(&p.ty), Flavor::Any, true, false)?;
            self.uses.note_pyi(&ty);
            let params = vec![
                "*".to_string(),
                format!("{}: {ty}", snake(&p.name)),
                "kind: type[KindT]".into(),
            ];
            let def = pyi_filled(&format!("_{}", snake(&name)), &params, &format!("{cls}[KindT]"));
            pyi_members.push(format!("    @staticmethod\n{def}"));
        }
        for m in &methods {
            if let Some(s) = self.method_pyi(m)? {
                pyi_members.push(s);
            }
        }

        // Module-level items.
        let mut py_items = String::new();
        let mut pyi_items = String::new();
        if let Some(h) = self.r.holder.clone() {
            py_items.push_str(&HOLDER_PY.replace("{H}", &h));
            pyi_items.push_str(&HOLDER_PYI.replace("{H}", &h));
            for (m, n) in [
                ("confluent_kafka.admin.config", "Config"),
                ("confluent_kafka.common", "Uuid"),
                ("confluent_kafka.common.errors", "ApiError"),
            ] {
                self.types.used.insert((m.into(), n.into()));
            }
        }
        if self.broker_futures() {
            let f = self.r.fields[0].clone();
            let v = self.arg_ty(&f.ty, &[0, 1, 0], Flavor::Any, false)?;
            let quoted = v.replacen('[', "[\"", 1).replacen(']', "\"]", 1);
            py_items.push_str(&format!(
                "\n_BrokerFutures = Mapping[int, \"KafkaFuture[{v}] | AsyncKafkaFuture[{v}]\"]\n"
            ));
            let _ = quoted;
            self.uses.mapping_py = true;
        }

        // Imports.
        let mut imp = Imports::default();
        for (on, name) in [
            (self.uses.callable_py, "Callable"),
            (self.uses.iterable_py, "Iterable"),
            (self.uses.mapping_py, "Mapping"),
            (self.uses.sequence_py, "Sequence"),
        ] {
            if on {
                imp.std("collections.abc", name);
            }
        }
        imp.std("typing", "Any");
        imp.std("typing", "Generic");
        // `KafkaFuture` / `AsyncKafkaFuture` are always annotated.
        imp.std("typing", "TYPE_CHECKING");
        if self.uses.java_str {
            imp.first("confluent_kafka._java", "java_str");
        }
        if self.r.holder.is_some() {
            imp.first("confluent_kafka._args", "MISSING");
            imp.first("confluent_kafka._args", "Form");
            imp.first("confluent_kafka._args", "java_forms");
        }
        for n in ["Async", "KindT", "Sync"] {
            imp.first("confluent_kafka.admin._futures", n);
        }
        for h in self.uses.helpers.clone() {
            match h {
                "_KafkaFutureImpl" => imp.first("confluent_kafka.common.kafka_future", h),
                _ => imp.first("confluent_kafka.admin._futures", h),
            }
        }
        for n in ["AsyncKafkaFuture", "KafkaFuture"] {
            imp.checking("confluent_kafka.common.kafka_future", n);
        }
        if self.uses.sub_level_error {
            imp.first("confluent_kafka.admin._internals", "_sub_level_error");
        }
        for i in self.types.internals.clone() {
            imp.first("confluent_kafka.admin._internals", &i);
        }
        if self.uses.illegal_argument {
            imp.first("confluent_kafka.illegal_argument_error", "IllegalArgumentError");
        }
        for (m, n) in &self.types.used {
            imp.checking(m, n);
        }
        let py_imports = wrap_imports(&imp.render(""));

        let mut si = Imports::default();
        for (on, name) in [
            (self.uses.callable_pyi, "Callable"),
            (self.uses.iterable_pyi, "Iterable"),
            (self.uses.mapping_pyi, "Mapping"),
            (self.uses.sequence_pyi, "Sequence"),
        ] {
            if on {
                si.std("collections.abc", name);
            }
        }
        if self.r.holder.is_some() {
            si.std("typing", "Any");
        }
        si.std("typing", "Generic");
        si.std("typing", "overload");
        for n in ["Async", "KindT", "Sync"] {
            si.first("confluent_kafka.admin._futures", n);
        }
        for n in ["AsyncKafkaFuture", "KafkaFuture"] {
            si.first("confluent_kafka.common.kafka_future", n);
        }

        for i in &self.types.internals {
            si.first("confluent_kafka.admin._internals", i);
        }
        for (m, n) in &self.types.used {
            si.first(m, n);
        }
        let pyi_imports = wrap_imports(&si.render(""));

        let py = format!(
            "{LICENSE}\n{}\n\nfrom __future__ import annotations\n\n{py_imports}__all__ = [\"{cls}\"]\n{py_items}\n\nclass \
             {cls}(Generic[KindT]):\n{}\n\n{}\n",
            self.module_doc(),
            self.class_doc(),
            py_members.join("\n\n")
        );
        let pyi = format!(
            "{LICENSE}\n# Stub for confluent_kafka.admin.{cls}.\n\n{pyi_imports}__all__ = [\"{cls}\"]\n\n{pyi_items}class \
             {cls}(Generic[KindT]):\n{}\n",
            pyi_members.join("\n")
        );
        Ok((py, pyi))
    }
}

// ---------------------------------------------------------------------------
// Bodies of the multi-line patterns
// ---------------------------------------------------------------------------

/// `allOf(...).thenApply(collect)`.
const COLLECT: &str = "        {L} = self._{L}

        def collect(_: None) -> {T}:
            # allOf ensured that all the futures completed successfully.
            return {{K}: future.get_now(value_if_absent=None) for {V}, future in {L}.items()}

        return self._kind._FUTURE.all_of(futures=tuple({L}.values())).then_apply(function=collect)";

/// `CreateTopicsResult.values()`: one derived future per key, as Java's
/// `thenApply(v -> (Void) null)`.
const VALUES_TO_VOID: &str = "        values: {R} = {}
        for topic, future in {F}.items():
            values[topic] = future.then_apply(function=lambda v: None)
        return values";

/// A new future relaying the stored one's outcome.
const RELAY: &str = "        result: _KafkaFutureImpl[{R}] = _KafkaFutureImpl()

{ACTION}
            if {E} is not None:
                result.complete_with_error({E})
            else:
                assert {V} is not None
                result.complete({V})

        {F}.when_complete(action=action)
        return self._kind._FUTURE._of_impl(result)";

/// A new future failing with the first present `Optional<Throwable>`.
const FIRST_ERROR: &str = "        result: _KafkaFutureImpl[{R}] = _KafkaFutureImpl()

{ACTION}
            if {E} is not None:
                result.complete_with_error({E})
            else:
                assert {V} is not None
                for {X} in {V}.values():
                    if {X} is not None:
                        result.complete_with_error({X})
                        return
                result.complete(None)

        self.{G}().when_complete(action=action)
        return self._kind._FUTURE._of_impl(result)";

const SUB_ERROR_ONE: &str = "        if partition not in {S}:
            raise IllegalArgumentError(
{MSG}
        result: _KafkaFutureImpl[None] = _KafkaFutureImpl()

{ACTION}
            if throwable is not None:
                result.complete_with_error(throwable)
            elif not self._maybe_complete_exceptionally(topic_partitions, partition, result):
                result.complete(None)

        {F}.when_complete(action=action)
        return self._kind._FUTURE._of_impl(result)";

const SUB_ERROR_ALL: &str = "        result: _KafkaFutureImpl[None] = _KafkaFutureImpl()

{ACTION}
            if throwable is not None:
                result.complete_with_error(throwable)
            else:
                for partition in {S}:
                    if self._maybe_complete_exceptionally(topic_partitions, partition, result):
                        return
                result.complete(None)

        {F}.when_complete(action=action)
        return self._kind._FUTURE._of_impl(result)";

const FLATTEN: &str = "        def flatten(by_broker: Mapping[int, list[{T}]]) -> list[{T}]:
            all_listings: list[{T}] = []
            for listings in by_broker.values():
                all_listings.extend(listings)
            return all_listings

        return self.{G}().then_apply(function=flatten)";

const COPY_ON_COMPLETE: &str = "        result: _KafkaFutureImpl[{R}] = (
            _KafkaFutureImpl())

        def action(broker_futures: _BrokerFutures | None, exception: BaseException | None) -> None:
            if broker_futures is not None:
                result.complete(dict(broker_futures))
            else:
                assert exception is not None
                result.complete_with_error(exception)

        {F}.when_complete(action=action)
        return self._kind._FUTURE._of_impl(result)";

const ALL_BY_KEY: &str = "        all_future: _KafkaFutureImpl[{R}] = (
            _KafkaFutureImpl())
        all_listings_map: dict[int, {T}] = {}

        def action(by_broker: _BrokerFutures | None,
                   top_level_exception: BaseException | None) -> None:
            if top_level_exception is not None:
                all_future.complete_with_error(top_level_exception)
                return
            assert by_broker is not None
            remaining_responses = set(by_broker.keys())

            def on_broker(broker_id: int) -> Any:
                def broker_action(listings: {T} | None,
                                  broker_exception: BaseException | None) -> None:
                    if broker_exception is not None:
                        all_future.complete_with_error(broker_exception)
                    elif not all_future.is_done():
                        assert listings is not None
                        all_listings_map[broker_id] = listings
                        remaining_responses.discard(broker_id)
                        if not remaining_responses:
                            all_future.complete(all_listings_map)
                return broker_action

            for broker_id, future in by_broker.items():
                future.when_complete(action=on_broker(broker_id))

        {F}.when_complete(action=action)
        return self._kind._FUTURE._of_impl(all_future)";

/// `CreateTopicsResult.TopicMetadataAndConfig` and `UNKNOWN` (decision log 6:
/// a private helper, as no public signature reaches it).
const HOLDER_PY: &str = r#"
# Java's package-private ``static final int UNKNOWN = -1``.
_UNKNOWN = -1


class _{H}:
    """Java's ``CreateTopicsResult.{H}``: a topic's metadata and
    configs, or the error that replaced them (private: no public signature reaches
    it)."""

    __slots__ = ("_exception", "_topic_id", "_num_partitions", "_replication_factor",
                 "_config")

    @java_forms(
        Form("topic_id", "num_partitions", "replication_factor", "config"),
        Form("exception"),
        name="{H}",
    )
    def __init__(self, *, topic_id: Uuid | None = MISSING, num_partitions: int | None = MISSING,
                 replication_factor: int | None = MISSING, config: Config | None = MISSING,
                 exception: ApiError | None = MISSING) -> None:
        if exception is MISSING:
            self._exception: ApiError | None = None
            self._topic_id = topic_id
            self._num_partitions = num_partitions
            self._replication_factor = replication_factor
            self._config = config
            return
        from confluent_kafka.common import Uuid

        self._exception = exception
        self._topic_id = Uuid.ZERO_UUID
        self._num_partitions = _UNKNOWN
        self._replication_factor = _UNKNOWN
        self._config = None

    def topic_id(self) -> Uuid | None:
        self._ensure_success()
        return self._topic_id

    def num_partitions(self) -> int | None:
        self._ensure_success()
        return self._num_partitions

    def replication_factor(self) -> int | None:
        self._ensure_success()
        return self._replication_factor

    def config(self) -> Config | None:
        self._ensure_success()
        return self._config

    def _ensure_success(self) -> None:
        if self._exception is not None:
            raise self._exception
"#;

const HOLDER_PYI: &str = "_UNKNOWN: int

class _{H}:
    @overload
    def __init__(self, *, topic_id: Uuid, num_partitions: int, replication_factor: int,
                 config: Config) -> None: ...
    @overload
    def __init__(self, *, exception: ApiError) -> None: ...
    def topic_id(self) -> Uuid: ...
    def num_partitions(self) -> int: ...
    def replication_factor(self) -> int: ...
    def config(self) -> Config: ...

";

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

/// The generated files, the classes generation stopped on, and the
/// `__init__` results section.
pub struct Generated {
    pub files: Vec<(PathBuf, String)>,
    pub stops: Vec<Stop>,
    pub init_section: String,
}

fn read_raw(repo_root: &Path, name: &str) -> Res<String> {
    let path = repo_root.join(ADMIN_JAVA).join(format!("{name}.java"));
    fs::read_to_string(&path).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))
}

/// Analyze and render one Java source: `Err(Stop)` when it is outside the
/// known patterns.
fn render_source(raw: &str, prose: &Prose, admin: &BTreeMap<String, String>) -> Res<Result<(String, String), Stop>> {
    let class = parse_java_class(raw)?;
    let r = match analyze(class, raw, admin) {
        Ok(r) => r,
        Err(s) => return Ok(Err(s)),
    };
    let mut nested: BTreeMap<String, String> = r
        .nested
        .iter()
        .map(|n| (n.name.clone(), format!("{}.{}", r.class.name, n.name)))
        .collect();
    if let Some(h) = &r.holder {
        nested.insert(h.clone(), format!("_{h}"));
    }
    let mut renderer = Renderer {
        r: &r,
        prose,
        types: Types { class: &r.class, nested, used: BTreeSet::new(), internals: BTreeSet::new() },
        uses: Uses::default(),
    };
    Ok(renderer.render())
}

/// Render every class of [`CLASSES`] but the [`HAND_WRITTEN`] ones.
pub fn render_all(repo_root: &Path) -> Res<Generated> {
    let prose = Prose::parse(PROSE)?;
    let admin = admin_methods(repo_root)?;
    let mut files = Vec::new();
    let mut stops = Vec::new();
    for name in CLASSES {
        if HAND_WRITTEN.contains(&name) {
            continue;
        }
        match render_source(&read_raw(repo_root, name)?, &prose, &admin)? {
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

/// The `__init__` results section: every Result of [`CLASSES`], generated and
/// hand-written alike, sorted.
fn init_section() -> String {
    let mut classes: Vec<&str> = CLASSES.to_vec();
    classes.sort_by_key(|c| snake(c));
    let mut out = format!("{INIT_BEGIN}\n");
    for c in &classes {
        let line = format!("from .{} import {c} as {c}", snake(c));
        if width(&line) <= WIDTH {
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

/// The Result classes of the reference set (the golden files).
pub fn reference_set(repo_root: &Path) -> Res<Vec<String>> {
    let text = fs::read_to_string(repo_root.join(REFERENCE_SET))?;
    let section = text
        .split("## Result classes")
        .nth(1)
        .and_then(|s| s.split("\n## ").next())
        .ok_or_else(|| anyhow::anyhow!("{REFERENCE_SET}: no Result classes section"))?;
    Ok(section
        .lines()
        .filter_map(|l| {
            l.strip_prefix("| `")
                .and_then(|r| r.split_once('`'))
                .map(|(c, _)| c.to_string())
        })
        .filter(|c| c.ends_with("Result"))
        .collect())
}

/// Each reference file the generator does not reproduce, with its first
/// differing line.
fn golden_differences(repo_root: &Path, g: &Generated) -> Res<Vec<String>> {
    let mut out = Vec::new();
    for class in reference_set(repo_root)? {
        if let Some(s) = g.stops.iter().find(|s| s.class == class) {
            out.push(format!("{class}: generation stopped: {}", s.reason));
            continue;
        }
        for ext in ["py", "pyi"] {
            let rel = Path::new(ADMIN_PY).join(snake(&class)).with_extension(ext);
            let on_disk = fs::read_to_string(repo_root.join(&rel)).unwrap_or_default();
            let generated = g.files.iter().find(|(p, _)| p == &rel).map_or("", |(_, c)| c.as_str());
            if let Some(d) = first_difference(&on_disk, generated) {
                out.push(format!("{}: {d}", rel.display()));
            }
        }
    }
    Ok(out)
}

/// `cargo xtask generate-admin-results`.
pub fn generate(repo_root: &Path) -> Res<()> {
    let g = render_all(repo_root)?;
    if !g.stops.is_empty() {
        let list: Vec<String> = g.stops.iter().map(ToString::to_string).collect();
        anyhow::bail!(
            "generation stopped for classes outside the known patterns:\n  {}",
            list.join("\n  ")
        );
    }
    // The reference set is golden: a hand-written file the generator does not
    // reproduce is a generator defect, never overwritten.
    let golden = golden_differences(repo_root, &g)?;
    if !golden.is_empty() {
        anyhow::bail!("the generator does not reproduce the reference set:\n{}", golden.join("\n"));
    }
    for (path, content) in &g.files {
        fs::write(repo_root.join(path), content)?;
    }
    let init_path = repo_root.join(INIT);
    let init = fs::read_to_string(&init_path)?;
    fs::write(&init_path, with_init_section(&init, &g.init_section)?)?;
    println!("✅ Wrote {} admin Result modules to {ADMIN_PY}", g.files.len() / 2);
    println!("   hand-written, not generated: {}", HAND_WRITTEN.join(", "));
    Ok(())
}

/// The stale generated files (empty when up to date).
pub fn check_up_to_date(repo_root: &Path) -> Res<Vec<PathBuf>> {
    let g = render_all(repo_root)?;
    let mut stale: Vec<PathBuf> = Vec::new();
    for s in &g.stops {
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
    fn test_golden_reference_results() {
        let root = repo_root();
        let refs = reference_set(&root).unwrap();
        assert_eq!(refs.len(), 10, "reference set: {refs:?}");
        let g = render_all(&root).unwrap();
        if let Ok(dir) = std::env::var("ADMIN_RESULTS_DUMP") {
            for (p, c) in &g.files {
                fs::write(Path::new(&dir).join(p.file_name().unwrap()), c).unwrap();
            }
        }
        let failures = golden_differences(&root, &g).unwrap();
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

    /// Every class outside [`HAND_WRITTEN`] is generated: 35 of 45.
    #[test]
    fn test_no_generated_class_stops() {
        let g = render_all(&repo_root()).unwrap();
        assert!(g.stops.is_empty(), "{:?}", g.stops);
        assert_eq!(g.files.len(), 2 * (CLASSES.len() - HAND_WRITTEN.len()));
    }

    /// The generator never writes a hand-written Result, and stops on one if
    /// asked to render it.
    #[test]
    fn test_hand_written_results_stop() {
        let root = repo_root();
        let prose = Prose::parse(PROSE).unwrap();
        let admin = admin_methods(&root).unwrap();
        let g = render_all(&root).unwrap();
        for name in HAND_WRITTEN {
            let rel = Path::new(ADMIN_PY).join(snake(name)).with_extension("py");
            assert!(!g.files.iter().any(|(p, _)| p == &rel), "{name} generated");
            let err = render_source(&read_raw(&root, name).unwrap(), &prose, &admin)
                .unwrap()
                .unwrap_err();
            assert_eq!(err.class, name);
            assert!(err.reason.contains("hand-written"), "{}", err.reason);
        }
    }

    #[test]
    fn test_unknown_method_body_stops() {
        let src =
            "package org.apache.kafka.clients.admin;\n/** The result of {@link Admin#x()}. */\npublic class XResult \
                   {\n private final KafkaFuture<Void> future;\n XResult(KafkaFuture<Void> future) {\n this.future = \
                   future;\n }\n public KafkaFuture<Void> all() {\n return future.thenApply(v -> v);\n }\n}\n";
        let admin = BTreeMap::from([("XResult".to_string(), "x".to_string())]);
        let err = render_source(src, &Prose::default(), &admin).unwrap().unwrap_err();
        assert_eq!(err.class, "XResult");
        assert!(
            err.reason.contains("method `all` body outside the known patterns"),
            "{}",
            err.reason
        );
    }

    fn stop_reason(class: &str, members: &str) -> String {
        let src = format!("package org.apache.kafka.clients.admin;\npublic class {class} {{\n{members}\n}}\n");
        let admin = BTreeMap::from([(class.to_string(), "x".to_string())]);
        render_source(&src, &Prose::default(), &admin).unwrap().unwrap_err().reason
    }

    /// Only `&&`-joined null comparisons of parameters are translated.
    #[test]
    fn test_unknown_constructor_check_stops() {
        let reason = stop_reason(
            "XResult",
            "private final Map<String, KafkaFuture<Void>> futures;\n XResult(Map<String, KafkaFuture<Void>> futures) {\n\
             if (futures.isEmpty() || futures == null) throw new IllegalArgumentException(\"bad\");\n\
             this.futures = futures;\n }",
        );
        assert!(reason.contains("is not `&&`-joined null comparisons"), "{reason}");
    }

    /// A `Collection` not known to be built as no `Set` stops.
    #[test]
    fn test_unchecked_collection_stops() {
        let reason = stop_reason(
            "XResult",
            "private final KafkaFuture<Collection<String>> future;\n XResult(KafkaFuture<Collection<String>> future) {\n\
             this.future = future;\n }",
        );
        assert!(reason.contains("field `future`: a `Collection`"), "{reason}");
    }

    /// A Javadoc mention of `null` outside the understood phrases stops.
    #[test]
    fn test_unknown_null_wording_stops() {
        let reason = stop_reason(
            "XResult",
            "private final KafkaFuture<String> future;\n XResult(KafkaFuture<String> future) {\n this.future = future;\n }\n\
             /** The value may be null. */\n public KafkaFuture<String> value() {\n return future;\n }",
        );
        assert!(reason.contains("Javadoc null wording not understood"), "{reason}");
    }

    /// A check throwing on one parameter's null alone keeps it non-nullable
    /// (and keeps the raise); only a null needed together with another one
    /// (`DeleteTopicsResult`) makes a parameter `| None`.
    #[test]
    fn test_single_null_check_is_not_nullable() {
        let src = "package org.apache.kafka.clients.admin;\npublic class XResult {\n\
                   private final Map<String, KafkaFuture<Void>> futures;\n\
                   XResult(Map<String, KafkaFuture<Void>> futures) {\n\
                   if (futures == null) throw new IllegalArgumentException(\"futures cannot be null\");\n\
                   this.futures = futures;\n }\n\
                   public Map<String, KafkaFuture<Void>> values() {\n return futures;\n }\n}\n";
        let admin = BTreeMap::from([("XResult".to_string(), "x".to_string())]);
        let (py, pyi) = render_source(src, &Prose::default(), &admin).unwrap().unwrap();
        assert!(
            py.contains("futures: Mapping[str, KafkaFuture[None] | AsyncKafkaFuture[None]],\n"),
            "{py}"
        );
        assert!(
            py.contains(
                "        if futures is None:\n            raise IllegalArgumentError(\n                \
                             message=\"futures cannot be None\")"
            ),
            "{py}"
        );
        assert!(
            py.contains("result._futures = None if futures is None else dict(futures)"),
            "{py}"
        );
        assert!(!pyi.contains("| None"), "{pyi}");
    }

    /// `{@code null}` is read before it becomes ``None``.
    #[test]
    fn test_code_null_wording_stops() {
        let reason = stop_reason(
            "XResult",
            "private final KafkaFuture<String> future;\n XResult(KafkaFuture<String> future) {\n this.future = future;\n }\n\
             /** Returns the future, which may be {@code null}. */\n public KafkaFuture<String> value() {\n return future;\n }",
        );
        assert!(reason.contains("Javadoc null wording not understood"), "{reason}");
    }

    /// The derived module-docstring sentences no reference class has (no
    /// reference Result relays a `whenComplete` value or holds an `Optional`),
    /// pinned on the generated classes that have them.
    #[test]
    fn test_derived_module_docstring_without_golden_case() {
        let g = render_all(&repo_root()).unwrap();
        let file = |name: &str| {
            let rel = Path::new(ADMIN_PY).join(name);
            g.files.iter().find(|(p, _)| p == &rel).map(|(_, c)| c.clone()).unwrap()
        };
        let joined = |text: String| text.split_whitespace().collect::<Vec<_>>().join(" ");
        let elect = joined(file("elect_leaders_result.py"));
        assert!(
            elect.contains(
                "``all()`` makes a new future completed from the stored future's ``when_complete``: Java's \
                 ``new KafkaFutureImpl<>()`` is the private ``_KafkaFutureImpl``. Java's \
                 ``Optional<Throwable>`` is ``BaseException | None`` (Types)."
            ),
            "{elect}"
        );
        let list = joined(file("list_config_resources_result.py"));
        assert!(
            list.contains(
                "``all()`` makes a new future completed from the stored future's ``when_complete``: Java's \
                 ``new KafkaFutureImpl<>()`` is the private ``_KafkaFutureImpl``. A ``Collection`` \
                 yields a ``list``: Java's ``KafkaAdminClient`` builds no ``Set`` for it (Types)."
            ),
            "{list}"
        );
    }

    #[test]
    fn test_pmatch_captures_and_backreferences() {
        let c = pmatch(
            "return $f.get($p).thenApply($h::$g);",
            "return futures.get(topic).thenApply(T::config);",
        )
        .unwrap();
        assert_eq!(c["f"], "futures");
        assert_eq!(c["g"], "config");
        assert!(pmatch("$a + $a", "x + y").is_none());
        assert_eq!(pmatch("a(#m);", "a(\"x\" + y);").unwrap()["m"], "\"x\" + y");
    }

    #[test]
    fn test_init_section_lists_every_result() {
        let s = init_section();
        for c in CLASSES {
            assert!(s.contains(&format!("    \"{c}\",\n")), "{c}");
        }
    }
}
