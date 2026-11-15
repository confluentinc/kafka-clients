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

//! The Node.js client's error hierarchy: the classes of the Python one
//! ([`super::build_graph`]), each in the src/ folder of its Java package
//! (`org.apache.kafka.common.errors` -> `src/common/errors/`, `java.*` ->
//! `src/`), with Java's `extends` chain, plus `src/internal/error_registry.ts`
//! (`javascript/CLAUDE.md`, "Errors").
//!
//! A class keeps Java's name with the `Exception` suffix as `Error`, its
//! Javadoc, and its FFI id, in the private static `_ffiId` (`@internal`, so
//! `stripInternal` leaves it out of the declarations). The
//! chains start at `Error`, except `IllegalArgumentError` and
//! `IndexOutOfBoundsError`, which extend JavaScript's `RangeError`. A class
//! takes Java's constructor forms whose parameters are a message and a cause
//! (Python's rule: `message` is the string Java passes to `super(...)`,
//! `cause` every `Throwable` parameter), matched by name through the form
//! matcher (`internal/throwable.setForms`), one TypeScript signature per form
//! plus the bare value of a one-parameter form; each form's message is the one
//! Java's constructor chain gives (`super::build_model`). The constructors
//! that take a payload (Java's other parameters, by Java's names, Q39) are
//! generated too: their message and the payload fields each form sets are
//! Java's constructor chain evaluated as JavaScript ([`js_eval`]), each
//! payload parameter taken by its src/internal/payload.ts kind, and the
//! payload getters are methods over private fields. A constructor or getter of
//! a type the client does not translate (the consumer's `OffsetAndMetadata`,
//! `TimestampType`) is left out and listed in the module's header comment, and
//! so is a class in [`LEFT_OUT_PAYLOADS`]. An error the core reports carries
//! its payload, read through the FFI's view of its class
//! (`native/error_payload.c`, generated from [`CORE_READS`]); a payload field
//! the view has no accessor for keeps its default and is listed. Java's
//! `public static final` instances (`DisconnectException.INSTANCE`) are
//! static properties of their class, built by Java's no-argument chain
//! whatever its constructors' visibility, not writable, as Java's `final`
//! field; a class with no other constructor cannot be constructed (a
//! `protected` constructor that throws `TypeError`).

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::{
    attr_name, base_type, build_graph, build_model, field_nullable, flatten_add, is_root_class, jdk_class,
    parse_ffi_enum_ids, python_name, snake, split_plus, throwable_like, CallKind, ClassInfo, Expr, Factory, GetterBody,
    Graph, JdkCtor, PyForm, PyModel, PyVal, KAFKA_EXCEPTION_FQN, TERMINAL_BASES,
};
use crate::javascript_gen::{header, js_str, rel_import, tsdoc, GeneratedFile, JsModule, JS_SRC};

type Res<T> = anyhow::Result<T>;

/// The Java built-ins whose JavaScript class extends `RangeError`, not `Error`:
/// JavaScript's own class for a value out of its range.
const RANGE_ERRORS: &[&str] = &[
    "java.lang.IllegalArgumentException",
    "java.lang.IndexOutOfBoundsException",
];

/// The Java built-ins only this client has a class for, as `JDK_BUILTINS` rows
/// (FQN, parent FQN, constructors) without an FFI id: Java's
/// `CancellationException`, which a cancelled `KafkaFuture`'s `get()` and
/// `getNow()` throw. The Python client answers it with the inner future's own
/// `CancelledError` instead, so the shared table does not list it. It keeps
/// Java's `extends IllegalStateException`.
const JS_ONLY_BUILTINS: &[(&str, &str, &[JdkCtor])] = &[(
    "java.util.concurrent.CancellationException",
    "java.lang.IllegalStateException",
    &[&[], &[("String", "message")]],
)];

/// The classes whose payload is not generated, with the reason: their
/// constructors that take a payload, their payload getters and the payload of
/// an error the core reports are left out, and listed in the module's header.
const LEFT_OUT_PAYLOADS: &[(&str, &str)] = &[(
    "org.apache.kafka.common.errors.RecordDeserializationException",
    "its payload holds the consumer's `TimestampType` and Java's nested `DeserializationExceptionOrigin`, which the \
     client does not translate before the consumer",
)];

/// A Java payload type the client translates: one of src/internal/payload.ts's
/// kinds (how it is taken, read from the core and returned).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    String,
    Int,
    Long,
    Double,
    Object,
    StringSet,
    TopicPartitions,
    TopicPartition,
    TopicPartitionLongMap,
    KafkaMetric,
}

impl Kind {
    /// The kind of a Java type; `None` for a type the client does not translate.
    fn of(java: &str) -> Option<Kind> {
        let t: String = java.chars().filter(|c| !c.is_whitespace()).collect();
        Some(match t.as_str() {
            "String" => Kind::String,
            "int" | "Integer" => Kind::Int,
            "long" | "Long" => Kind::Long,
            "double" | "Double" => Kind::Double,
            "Object" => Kind::Object,
            "Set<String>" | "Collection<String>" => Kind::StringSet,
            "Set<TopicPartition>" | "Collection<TopicPartition>" => Kind::TopicPartitions,
            "TopicPartition" => Kind::TopicPartition,
            "Map<TopicPartition,Long>" => Kind::TopicPartitionLongMap,
            "KafkaMetric" => Kind::KafkaMetric,
            _ => return None,
        })
    }

    /// Its export in src/internal/payload.ts.
    fn export(self) -> &'static str {
        match self {
            Kind::String => "string",
            Kind::Int => "int",
            Kind::Long => "long",
            Kind::Double => "double",
            Kind::Object => "object",
            Kind::StringSet => "stringSet",
            Kind::TopicPartitions => "topicPartitions",
            Kind::TopicPartition => "topicPartition",
            Kind::TopicPartitionLongMap => "topicPartitionLongMap",
            Kind::KafkaMetric => "kafkaMetric",
        }
    }

    /// The TypeScript type a constructor takes (root CLAUDE.md, JavaScript
    /// Binding Conventions, Types, Q23 and Q25).
    fn taken(self) -> &'static str {
        match self {
            Kind::String => "string",
            Kind::Int | Kind::Double => "number",
            Kind::Long => "bigint | number",
            Kind::Object => "unknown",
            Kind::StringSet => "Iterable<string>",
            Kind::TopicPartitions => "Iterable<TopicPartition>",
            Kind::TopicPartition => "TopicPartition",
            Kind::TopicPartitionLongMap => "TopicPartitionMap<bigint | number>",
            Kind::KafkaMetric => "KafkaMetric",
        }
    }

    /// The TypeScript type a getter returns.
    fn returned(self) -> &'static str {
        match self {
            Kind::String => "string",
            Kind::Int | Kind::Double => "number",
            Kind::Long => "bigint",
            Kind::Object => "unknown",
            Kind::StringSet => "Set<string>",
            Kind::TopicPartitions => "TopicPartitionSet",
            Kind::TopicPartition => "TopicPartition",
            Kind::TopicPartitionLongMap => "TopicPartitionMap<bigint>",
            Kind::KafkaMetric => "KafkaMetric",
        }
    }

    /// Whether a value of the kind may be null (a Java reference type).
    fn reference(self) -> bool {
        !matches!(self, Kind::Int | Kind::Long | Kind::Double)
    }

    /// The type of a private field holding it.
    fn stored(self) -> String {
        if self.reference() && self != Kind::Object {
            format!("{} | null", self.returned())
        } else {
            self.returned().to_string()
        }
    }

    /// Java's default value of a field of the kind, as JavaScript.
    fn java_default(self) -> &'static str {
        match self {
            Kind::Int | Kind::Double => "0",
            Kind::Long => "0n",
            _ => "null",
        }
    }

    /// The TypeScript classes its taken type names.
    fn taken_names(self) -> &'static [&'static str] {
        match self {
            Kind::TopicPartitions | Kind::TopicPartition => &["TopicPartition"],
            Kind::TopicPartitionLongMap => &["TopicPartitionMap"],
            Kind::KafkaMetric => &["KafkaMetric"],
            _ => &[],
        }
    }

    /// The TypeScript classes its returned (and stored) type names.
    fn returned_names(self) -> &'static [&'static str] {
        match self {
            Kind::TopicPartitions => &["TopicPartitionSet"],
            Kind::TopicPartition => &["TopicPartition"],
            Kind::TopicPartitionLongMap => &["TopicPartitionMap"],
            Kind::KafkaMetric => &["KafkaMetric"],
            _ => &[],
        }
    }
}

/// Where a class's type comes from, under src/.
fn type_module(name: &str) -> &'static str {
    match name {
        "TopicPartition" => "common/topic_partition",
        "TopicPartitionSet" => "common/topic_partition_set",
        "TopicPartitionMap" => "common/topic_partition_map",
        "KafkaMetric" => "common/metrics/kafka_metric",
        other => unreachable!("no module for `{other}`"),
    }
}

/// How native/error.c reads a payload field through the FFI (its
/// `payload_<conv>` helper).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CoreConv {
    StringList,
    TopicPartitionList,
    TopicPartitionLongMap,
    String,
    Int32,
    Double,
}

impl CoreConv {
    /// The C type the accessor returns.
    fn c_type(self) -> &'static str {
        match self {
            CoreConv::StringList | CoreConv::TopicPartitionList => "kafka_List_t*",
            CoreConv::TopicPartitionLongMap => "kafka_Map_t*",
            CoreConv::String => "const char*",
            CoreConv::Int32 => "int32_t",
            CoreConv::Double => "double",
        }
    }

    /// The C value the accessor's result starts as.
    fn c_init(self) -> &'static str {
        match self {
            CoreConv::Int32 => "0",
            CoreConv::Double => "0.0",
            _ => "NULL",
        }
    }

    fn helper(self) -> &'static str {
        match self {
            CoreConv::StringList => "payload_string_list",
            CoreConv::TopicPartitionList => "payload_topic_partition_list",
            CoreConv::TopicPartitionLongMap => "payload_topic_partition_long_map",
            CoreConv::String => "payload_string",
            CoreConv::Int32 => "payload_int32",
            CoreConv::Double => "payload_double",
        }
    }
}

/// The payload of an error the core reports, as the FFI's view of the class
/// gives it (CLAUDE.md §4: `kafka_common_Error_<class>` returns the class's
/// `kafka_common_<Class>Error_t`, then its accessors): the Java class, its
/// `kafka_common_Error_e` enumerator, the view function and type, and each
/// field the view has an accessor for, with how native/error_payload.c reads
/// it. A payload field without an accessor keeps its default
/// ([`CORE_DEFAULTS`], else Java's) and is listed in its module's header.
type CoreRead = (
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static [(&'static str, &'static str, CoreConv)],
);

const CORE_READS: &[CoreRead] = &[
    (
        "org.apache.kafka.common.errors.TopicAuthorizationException",
        "TOPIC_AUTHORIZATION_FAILED",
        "kafka_common_Error_topic_authorization_error_err",
        "kafka_common_TopicAuthorizationError_t",
        &[(
            "unauthorizedTopics",
            "kafka_common_TopicAuthorizationError_unauthorized_topics_err",
            CoreConv::StringList,
        )],
    ),
    (
        "org.apache.kafka.common.errors.InvalidTopicException",
        "INVALID_TOPIC_ERROR",
        "kafka_common_Error_invalid_topic_err",
        "kafka_common_InvalidTopicError_t",
        &[(
            "invalidTopics",
            "kafka_common_InvalidTopicError_invalid_topics_err",
            CoreConv::StringList,
        )],
    ),
    (
        "org.apache.kafka.common.errors.GroupAuthorizationException",
        "GROUP_AUTHORIZATION_FAILED",
        "kafka_common_Error_group_authorization_error_err",
        "kafka_common_GroupAuthorizationError_t",
        &[("groupId", "kafka_common_GroupAuthorizationError_group_id_err", CoreConv::String)],
    ),
    (
        "org.apache.kafka.common.errors.ThrottlingQuotaExceededException",
        "THROTTLING_QUOTA_EXCEEDED",
        "kafka_common_Error_throttling_quota_exceeded_error_err",
        "kafka_common_ThrottlingQuotaExceededError_t",
        &[(
            "throttleTimeMs",
            "kafka_common_ThrottlingQuotaExceededError_throttle_time_ms_err",
            CoreConv::Int32,
        )],
    ),
    (
        "org.apache.kafka.common.errors.RecordTooLargeException",
        "MESSAGE_TOO_LARGE",
        "kafka_common_Error_record_too_large_error_err",
        "kafka_common_RecordTooLargeError_t",
        &[(
            "recordTooLargePartitions",
            "kafka_common_RecordTooLargeError_record_too_large_partitions_err",
            CoreConv::TopicPartitionLongMap,
        )],
    ),
    (
        "org.apache.kafka.common.errors.ResourceNotFoundException",
        "RESOURCE_NOT_FOUND",
        "kafka_common_Error_resource_not_found_err",
        "kafka_common_ResourceNotFoundError_t",
        &[("resource", "kafka_common_ResourceNotFoundError_resource_err", CoreConv::String)],
    ),
    (
        "org.apache.kafka.common.errors.DuplicateResourceException",
        "DUPLICATE_RESOURCE",
        "kafka_common_Error_duplicate_resource_err",
        "kafka_common_DuplicateResourceError_t",
        &[("resource", "kafka_common_DuplicateResourceError_resource_err", CoreConv::String)],
    ),
    (
        "org.apache.kafka.clients.consumer.OffsetOutOfRangeException",
        "CONSUMER_OFFSET_OUT_OF_RANGE",
        "kafka_common_Error_consumer_offset_out_of_range_err",
        "kafka_common_ConsumerOffsetOutOfRangeError_t",
        &[(
            "offsetOutOfRangePartitions",
            "kafka_common_ConsumerOffsetOutOfRangeError_offset_out_of_range_partitions_err",
            CoreConv::TopicPartitionLongMap,
        )],
    ),
    (
        "org.apache.kafka.clients.consumer.NoOffsetForPartitionException",
        "CONSUMER_NO_OFFSET_FOR_PARTITION",
        "kafka_common_Error_consumer_no_offset_for_partition_err",
        "kafka_common_ConsumerNoOffsetForPartitionError_t",
        &[(
            "partitions",
            "kafka_common_ConsumerNoOffsetForPartitionError_partitions_err",
            CoreConv::TopicPartitionList,
        )],
    ),
    (
        "org.apache.kafka.clients.consumer.LogTruncationException",
        "CONSUMER_LOG_TRUNCATION",
        "kafka_common_Error_consumer_log_truncation_err",
        "kafka_common_ConsumerLogTruncationError_t",
        &[(
            "offsetOutOfRangePartitions",
            "kafka_common_ConsumerLogTruncationError_offset_out_of_range_partitions_err",
            CoreConv::TopicPartitionLongMap,
        )],
    ),
    (
        "org.apache.kafka.common.metrics.QuotaViolationException",
        "QUOTA_VIOLATION",
        "kafka_common_Error_quota_violation_err",
        "kafka_common_QuotaViolationError_t",
        &[
            ("value", "kafka_common_QuotaViolationError_value_err", CoreConv::Double),
            ("bound", "kafka_common_QuotaViolationError_bound_err", CoreConv::Double),
        ],
    ),
];

/// A payload field's value in an error the core reports without it, where it
/// is not Java's default: (Java class, field, JavaScript value, why).
const CORE_DEFAULTS: &[(&str, &str, &str, &str)] = &[
    (
        "org.apache.kafka.common.requests.CorrelationIdMismatchException",
        "requestCorrelationId",
        "-1",
        "-1, a value Java's counter never assigns, as the Python client reads it",
    ),
    (
        "org.apache.kafka.common.requests.CorrelationIdMismatchException",
        "responseCorrelationId",
        "-1",
        "-1, a value Java's counter never assigns, as the Python client reads it",
    ),
];

/// The class graph of the Python client plus [`JS_ONLY_BUILTINS`].
fn js_graph(repo_root: &Path) -> Res<BTreeMap<String, ClassInfo>> {
    let mut classes = build_graph(repo_root)?;
    for (fqn, parent, ctors) in JS_ONLY_BUILTINS {
        if !classes.contains_key(*parent) {
            anyhow::bail!("javascript errors: `{fqn}` extends `{parent}`, which has no class");
        }
        let simple = fqn.rsplit('.').next().unwrap();
        classes.insert(
            (*fqn).to_string(),
            ClassInfo {
                java_fqn: (*fqn).to_string(),
                py_name: python_name(simple),
                ffi_id: None,
                is_abstract: false,
                parent_fqn: (*parent).to_string(),
                py_module: "confluent_kafka".to_string(),
                java: jdk_class(fqn, parent, ctors),
                is_jdk: true,
                unserializable: Vec::new(),
            },
        );
    }
    Ok(classes)
}

/// The src/ folder of a Python module (`confluent_kafka.common.errors` ->
/// `common/errors`, `confluent_kafka` -> `""`).
fn js_dir(py_module: &str) -> String {
    py_module
        .strip_prefix("confluent_kafka")
        .unwrap_or(py_module)
        .trim_start_matches('.')
        .replace('.', "/")
}

fn js_path(info: &ClassInfo) -> String {
    let dir = js_dir(&info.py_module);
    if dir.is_empty() {
        info.file_stem()
    } else {
        format!("{dir}/{}", info.file_stem())
    }
}

/// The message a form gives `Throwable`, as `internal/throwable`'s `FormMessage`.
#[derive(Clone, Debug, PartialEq)]
enum Message {
    /// The `message` parameter.
    Param,
    /// The cause's `toString()`, Java's `Throwable(Throwable)`.
    CauseString,
    /// Java's `null`.
    Null,
    /// A fixed text.
    Text(String),
    /// A message Java builds from the form's values (`v`), as JavaScript.
    Expr(String),
}

/// One parameter of a form: its JavaScript name (`message`, `cause`, or
/// Java's name of a payload parameter), its Java type, its payload kind and
/// whether it takes null.
#[derive(Clone, Debug, PartialEq)]
struct JsParam {
    name: String,
    java_ty: String,
    kind: Option<Kind>,
    nullable: bool,
}

/// One generated constructor form: its parameters, its message and the
/// payload fields it sets (Java field name, JavaScript value over `v`).
#[derive(Clone, Debug, PartialEq)]
struct JsForm {
    params: Vec<String>,
    message: Message,
    typed: Vec<JsParam>,
    fields: Vec<(String, String)>,
}

/// A payload getter: Java's name, the field it reads, its kind and what it
/// returns.
#[derive(Clone, Debug)]
enum JsGetter {
    Field {
        name: String,
        field: String,
        kind: Kind,
        nullable: bool,
        overrides: bool,
    },
    KeySet {
        name: String,
        field: String,
        overrides: bool,
    },
    Abstract {
        name: String,
        ret: Kind,
    },
}

/// A class's payload: its fields (declared here, and over the whole chain,
/// with each one's value in an error the core reports without it), its
/// getters, Java's public static factories and its `toString()` override, and
/// what is left out for want of a translated type or an FFI accessor.
#[derive(Clone, Debug, Default)]
struct JsPayload {
    order: Vec<String>,
    own_fields: Vec<(String, Kind)>,
    chain_fields: Vec<(String, Kind, String)>,
    getters: Vec<JsGetter>,
    factories: Vec<String>,
    to_string: Option<String>,
    left_out_members: Vec<String>,
    core_missing: Vec<String>,
    java_string: bool,
    java_double: bool,
}

/// Decode the Python string literals `"a" + "b"` the constructor evaluation
/// writes for a fixed message; `None` when `text` is anything else.
fn literal_text(text: &str) -> Option<String> {
    let mut out = String::new();
    let mut rest = text.trim();
    loop {
        let mut chars = rest.strip_prefix('"')?.chars();
        let mut used = 1;
        loop {
            let c = chars.next()?;
            used += c.len_utf8();
            match c {
                '"' => break,
                '\\' => {
                    let e = chars.next()?;
                    used += e.len_utf8();
                    out.push(match e {
                        'n' => '\n',
                        't' => '\t',
                        other => other,
                    });
                },
                c => out.push(c),
            }
        }
        rest = rest[used..].trim_start();
        if rest.is_empty() {
            return Some(out);
        }
        rest = rest.strip_prefix('+')?.trim_start();
    }
}

/// The form of a Java constructor, or `None` when it takes a parameter other
/// than a message and a cause (a payload).
fn js_form(fqn: &str, form: &PyForm) -> Res<Option<JsForm>> {
    if form.params.iter().any(|p| p != "message" && p != "cause") {
        return Ok(None);
    }
    let has_cause = form.params.iter().any(|p| p == "cause");
    let want_cause = if has_cause { "cause" } else { "None" };
    if form.effect.cause != want_cause {
        anyhow::bail!(
            "javascript errors: `{fqn}({})` has the cause `{}`",
            form.params.join(", "),
            form.effect.cause
        );
    }
    let m = form.effect.message.as_str();
    let message = if m == "message" && form.params.iter().any(|p| p == "message") {
        Message::Param
    } else if m == "None" {
        Message::Null
    } else if m == "_throwable.cause_message(cause)" && has_cause {
        Message::CauseString
    } else if let Some(text) = literal_text(m) {
        Message::Text(text)
    } else {
        anyhow::bail!("javascript errors: `{fqn}({})` has the message `{m}`", form.params.join(", "))
    };
    let typed = form.params.iter().map(|p| plain_param(p)).collect();
    Ok(Some(JsForm { params: form.params.clone(), message, typed, fields: Vec::new() }))
}

/// A `message` or `cause` parameter.
fn plain_param(name: &str) -> JsParam {
    let java_ty = if name == "message" { "String" } else { "Throwable" };
    JsParam { name: name.to_string(), java_ty: java_ty.to_string(), kind: None, nullable: true }
}

/// A class's generated forms, in Java declaration order (one per parameter
/// set, the first), and the Java constructors left out, as `Simple(Type, ...)`.
fn class_forms(g: &Graph, info: &ClassInfo) -> Res<(Vec<JsForm>, Vec<String>)> {
    let model = build_model(g, info)?;
    let mut forms: Vec<JsForm> = Vec::new();
    let mut left_out = Vec::new();
    for form in &model.forms {
        match js_form(&info.java_fqn, form)? {
            Some(f) => {
                if !forms.iter().any(|x| x.params == f.params) {
                    forms.push(f);
                }
            },
            None => {
                let types: Vec<String> = info.java.ctors[form.ctor].params.iter().map(|p| p.ty.clone()).collect();
                left_out.push(format!("`{}({})`", info.java.simple, types.join(", ")));
            },
        }
    }
    Ok((forms, left_out))
}

// ---------------------------------------------------------------------------
// Payloads: Java's constructors evaluated as JavaScript
// ---------------------------------------------------------------------------

/// A JavaScript value translating a Java expression: its source, its Java
/// type (`None` for `null`), the text of a string literal, and whether it is
/// a constant.
#[derive(Clone, Debug)]
struct JsVal {
    expr: String,
    ty: Option<String>,
    text: Option<String>,
    konst: bool,
}

impl JsVal {
    fn konst(expr: &str, ty: Option<&str>) -> JsVal {
        JsVal { expr: expr.to_string(), ty: ty.map(str::to_string), text: None, konst: true }
    }

    /// A value for overload resolution ([`Graph::resolve`] reads its type).
    fn as_py(&self) -> PyVal {
        PyVal {
            expr: self.expr.clone(),
            ty: self.ty.clone(),
            konst: self.konst.then(String::new),
        }
    }
}

type JsEnv = BTreeMap<String, JsVal>;

/// The message a constructor gives `Throwable`.
#[derive(Clone, Debug)]
enum JsMessage {
    Val(JsVal),
    CauseString,
    Null,
}

/// What a constructor does, as JavaScript: its message, its cause and the
/// fields it sets (Java field name -> value), over the whole chain.
#[derive(Clone, Debug)]
struct JsEffect {
    message: JsMessage,
    cause: String,
    fields: BTreeMap<String, JsVal>,
}

/// The parameter name a translated value reads (`v.partition` -> `partition`).
fn param_of(v: &JsVal) -> String {
    v.expr.strip_prefix("v.").unwrap_or(&v.expr).to_string()
}

fn js_translate(e: &Expr, env: &JsEnv) -> Res<JsVal> {
    Ok(match e {
        Expr::Null => JsVal::konst("null", None),
        Expr::Bool(b) => JsVal::konst(if *b { "true" } else { "false" }, Some("boolean")),
        Expr::Int(n) => JsVal::konst(&n.to_string(), Some("int")),
        Expr::Str(s) => JsVal { expr: js_str(s), ty: Some("String".into()), text: Some(s.clone()), konst: true },
        Expr::Name(name) => {
            let name: &[String] = if name.first().map(String::as_str) == Some("this") {
                &name[1..]
            } else {
                name
            };
            match name {
                [one] => env
                    .get(one)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("javascript errors: unknown name `{one}`"))?,
                _ => anyhow::bail!("javascript errors: unsupported constant `{}`", name.join(".")),
            }
        },
        Expr::Call(name, args) => {
            let vals: Vec<JsVal> = args.iter().map(|a| js_translate(a, env)).collect::<Res<_>>()?;
            let n: Vec<&str> = name.iter().map(String::as_str).collect();
            match (n.as_slice(), vals.as_slice()) {
                (["Collections", "emptySet"] | ["Set", "of"], []) => JsVal::konst("new Set<string>()", Some("Set")),
                (["Collections", "singleton"], [x]) => JsVal {
                    expr: format!("P.singleton({}, {})", x.expr, js_str(&param_of(x))),
                    ty: Some("Set".into()),
                    text: None,
                    konst: false,
                },
                // Each copy dereferences its argument: Java's
                // NullPointerException is `TypeError` naming it. The kind
                // that took the argument already made the copy.
                (["Set", "copyOf"] | ["Collections", "unmodifiableSet"], [x]) => JsVal {
                    expr: format!("P.nonNull({}, {})", x.expr, js_str(&param_of(x))),
                    ty: Some("Set".into()),
                    text: None,
                    konst: false,
                },
                (["Map", "copyOf"] | ["Collections", "unmodifiableMap"], [x]) => JsVal {
                    expr: format!("P.nonNull({}, {})", x.expr, js_str(&param_of(x))),
                    ty: Some("Map".into()),
                    text: None,
                    konst: false,
                },
                _ => anyhow::bail!("javascript errors: unsupported call `{}`", name.join(".")),
            }
        },
        Expr::New(ty, args) => match (ty.as_str(), args.as_slice()) {
            ("HashSet" | "LinkedHashSet", []) => JsVal::konst("new Set<string>()", Some("Set")),
            _ => anyhow::bail!("javascript errors: unsupported `new {ty}(...)`"),
        },
        Expr::Add(..) => {
            let mut parts = Vec::new();
            flatten_add(e, &mut parts);
            if !parts.iter().any(|p| matches!(p, Expr::Str(_))) {
                anyhow::bail!("javascript errors: unsupported arithmetic `+`");
            }
            let mut out: Vec<String> = Vec::new();
            for p in parts {
                let v = js_translate(p, env)?;
                if v.text.as_deref() == Some("") {
                    // An empty literal (a folded `null ? "" : ...`) adds nothing.
                    continue;
                }
                let text = match p {
                    Expr::Str(_) => v.expr,
                    Expr::Ternary(..) if v.ty.as_deref() == Some("String") => v.expr,
                    _ if matches!(v.ty.as_deref().map(base_type), Some("double" | "Double" | "float" | "Float")) => {
                        format!("javaDoubleString({} as number)", v.expr)
                    },
                    _ => format!("javaString({})", v.expr),
                };
                out.push(text);
            }
            JsVal { expr: out.join(" + "), ty: Some("String".into()), text: None, konst: false }
        },
        Expr::Eq(a, b) => {
            let (Expr::Name(_), Expr::Null) = (a.as_ref(), b.as_ref()) else {
                anyhow::bail!("javascript errors: unsupported `==`")
            };
            let x = js_translate(a, env)?;
            if x.konst {
                // A constant (a shorter constructor's `null`) compares at
                // generation time.
                return Ok(JsVal::konst(if x.expr == "null" { "true" } else { "false" }, Some("boolean")));
            }
            JsVal {
                expr: format!("{} === null", x.expr),
                ty: Some("boolean".into()),
                text: None,
                konst: false,
            }
        },
        Expr::Ternary(c, a, b) => {
            let c = js_translate(c, env)?;
            let a = js_translate(a, env)?;
            let b = js_translate(b, env)?;
            if c.konst {
                return Ok(if c.expr == "true" { a } else { b });
            }
            JsVal {
                expr: format!("({} ? {} : {})", c.expr, a.expr, b.expr),
                ty: a.ty.clone().or(b.ty),
                text: None,
                konst: false,
            }
        },
    })
}

fn js_bind(g: &Graph, fqn: &str, idx: usize, vals: Vec<JsVal>) -> Res<JsEnv> {
    let ctor = &g.class(fqn)?.java.ctors[idx];
    let mut env = JsEnv::new();
    for (p, mut v) in ctor.params.iter().zip(vals) {
        if v.ty.is_none() {
            v.ty = Some(p.ty.clone());
        }
        env.insert(p.name.clone(), v);
    }
    Ok(env)
}

/// Constructor `idx` of `fqn` evaluated with the arguments in `env`, as
/// [`Graph::eval`] evaluates it for Python.
fn js_eval(g: &Graph, fqn: &str, idx: usize, env: &JsEnv, depth: usize) -> Res<JsEffect> {
    if depth > 16 {
        anyhow::bail!("{fqn}: constructor chain too deep");
    }
    let info = g.class(fqn)?;
    let ctor = &info.java.ctors[idx];
    let resolve = |cls: &str, vals: &[JsVal], sub: bool| {
        let py: Vec<PyVal> = vals.iter().map(JsVal::as_py).collect();
        g.resolve(cls, &py, sub)
    };
    let mut eff = match &ctor.call {
        Some((CallKind::This, args)) => {
            let vals: Vec<JsVal> = args.iter().map(|a| js_translate(a, env)).collect::<Res<_>>()?;
            let target = resolve(fqn, &vals, false)?;
            let env2 = js_bind(g, fqn, target, vals)?;
            js_eval(g, fqn, target, &env2, depth + 1)?
        },
        other => {
            let args = other.as_ref().map(|(_, a)| a.clone()).unwrap_or_default();
            let vals: Vec<JsVal> = args.iter().map(|a| js_translate(a, env)).collect::<Res<_>>()?;
            let parent = info.parent_fqn.as_str();
            let mut e = if TERMINAL_BASES.contains(&parent) {
                let (message, cause) = match vals.as_slice() {
                    [] => (JsMessage::Null, "null".to_string()),
                    [one] if one.ty.as_deref().is_some_and(throwable_like) => {
                        (JsMessage::CauseString, one.expr.clone())
                    },
                    [one] => (JsMessage::Val(one.clone()), "null".to_string()),
                    [m, c] => (JsMessage::Val(m.clone()), c.expr.clone()),
                    _ => anyhow::bail!("Throwable has no constructor with {} arguments", vals.len()),
                };
                JsEffect { message, cause, fields: BTreeMap::new() }
            } else {
                let target = resolve(parent, &vals, true)?;
                let env2 = js_bind(g, parent, target, vals)?;
                js_eval(g, parent, target, &env2, depth + 1)?
            };
            // Instance initializers run after `super(...)`.
            for f in info.java.fields.iter().filter(|f| !f.is_static) {
                if let Some(init) = &f.init {
                    e.fields.insert(f.name.clone(), js_translate(init, &JsEnv::new())?);
                }
            }
            e
        },
    };
    for (field, value) in &ctor.assigns {
        eff.fields.insert(field.clone(), js_translate(value, env)?);
    }
    Ok(eff)
}

/// Whether a class's payload is generated: not left out, and with a payload
/// parameter, a field, a getter, a factory or a `toString()` override.
fn has_payload(g: &Graph, info: &ClassInfo, model: &PyModel) -> bool {
    if LEFT_OUT_PAYLOADS.iter().any(|(c, _)| *c == info.java_fqn) {
        return false;
    }
    !g.chain_fields(&info.java_fqn).is_empty()
        || !info.java.getters.is_empty()
        || !info.java.factories.is_empty()
        || info.java.to_string.is_some()
        || model.params.iter().any(|p| p.name != "message" && p.name != "cause")
}

/// The chain's payload fields the client translates, ancestors first: (Java
/// field name, kind).
fn translated_fields(g: &Graph, fqn: &str) -> Vec<(String, Kind)> {
    let mut chain = Vec::new();
    let mut cur = fqn.to_string();
    while let Some(info) = g.classes.get(&cur) {
        chain.push(info);
        cur = info.parent_fqn.clone();
    }
    let mut out: Vec<(String, Kind)> = Vec::new();
    for info in chain.iter().rev() {
        for f in info.java.fields.iter().filter(|f| !f.is_static) {
            if let Some(kind) = Kind::of(&f.ty) {
                if !out.iter().any(|(x, _)| *x == f.name) {
                    out.push((f.name.clone(), kind));
                }
            }
        }
    }
    out
}

/// The field value as the private field stores it: a `long` constant as a bigint.
fn stored_value(kind: Kind, v: &JsVal) -> String {
    if kind == Kind::Long && v.konst && v.expr.parse::<i64>().is_ok() {
        format!("{}n", v.expr)
    } else {
        v.expr.clone()
    }
}

/// Whether an ancestor of `info` has a getter named `name` (the getter overrides it).
fn ancestor_getter(g: &Graph, info: &ClassInfo, name: &str) -> bool {
    let mut cur = g.classes.get(&info.parent_fqn);
    while let Some(c) = cur {
        if c.java.getters.iter().any(|x| x.name == name) {
            return true;
        }
        cur = g.classes.get(&c.parent_fqn);
    }
    false
}

/// Java's public static factory `f` (`GroupAuthorizationException.forGroupId`)
/// as a static method building the class through the constructor Java calls:
/// its form table's declaration, then `\n\0\n`, then the method. The
/// factory's one parameter is given on its own or by name, as a method's is
/// (root CLAUDE.md, JavaScript Binding Conventions, Signatures).
fn render_factory(g: &Graph, info: &ClassInfo, model: &PyModel, f: &Factory) -> Res<String> {
    let [p] = f.params.as_slice() else {
        anyhow::bail!(
            "javascript errors: `{}.{}` has {} parameters; only one is translated",
            info.java_fqn,
            f.name,
            f.params.len()
        );
    };
    let kind = Kind::of(&p.ty)
        .filter(|k| *k == Kind::String)
        .ok_or_else(|| anyhow::anyhow!("javascript errors: `{}.{}` takes a `{}`", info.java_fqn, f.name, p.ty))?;
    let mut env = JsEnv::new();
    env.insert(
        p.name.clone(),
        JsVal { expr: p.name.clone(), ty: Some(p.ty.clone()), text: None, konst: false },
    );
    let vals: Vec<JsVal> = f.args.iter().map(|a| js_translate(a, &env)).collect::<Res<_>>()?;
    let py: Vec<PyVal> = vals.iter().map(JsVal::as_py).collect();
    let target = g.resolve(&info.java_fqn, &py, false)?;
    let form = model
        .forms
        .iter()
        .find(|form| form.ctor == target)
        .ok_or_else(|| anyhow::anyhow!("{}.{}: calls a non-public constructor", info.java_fqn, f.name))?;
    let ctor = &info.java.ctors[target];
    let mut keys = Vec::new();
    for ((jp, py), v) in ctor.params.iter().zip(&form.params).zip(&vals) {
        let key = if py == "message" || py == "cause" {
            py.clone()
        } else {
            jp.name.clone()
        };
        let value = if v.expr == p.name {
            format!("{} as string | null", v.expr)
        } else {
            v.expr.clone()
        };
        keys.push(format!("{key}: {value}"));
    }
    let consts = format!("{}_FORMS", snake(&f.name).to_uppercase());
    let decl = format!(
        "const {consts} = new JavaForms({}, [[{}]], {{\n  bareTypes: {{ {}: (value) => value === null || typeof value === 'string' }},\n}});",
        js_str(&f.name),
        js_str(&p.name),
        p.name
    );
    let cls = &info.py_name;
    let pn = &p.name;
    let name = &f.name;
    let jt = &p.ty;
    let ty = format!("{} | null", kind.taken());
    let method = format!(
        "{}\n  static {name}({pn}: {ty}): {cls};\n{}\n  static {name}(options: {{\n    /** Java's `{jt} {pn}`. */\n    {pn}: {ty};\n  }}): {cls};\n  static {name}(options: unknown): {cls} {{\n    {consts}.match(options, arguments.length);\n    const {pn} = {consts}.value(options, {});\n    return new {cls}({{ {} }});\n  }}",
        tsdoc(
            &format!(
                "Java's static `{name}({jt} {pn})`.\n\n@param {pn} - Java's `{pn}`\n@returns the error\n\
                 @throws `IllegalArgumentError` when the given parameters are not `{{{pn}}}`"
            ),
            "  "
        ),
        tsdoc(
            &format!("The same as `{name}({pn})`, with the {pn} named.\n\n@param options - the {pn}\n@returns the error"),
            "  "
        ),
        js_str(pn),
        keys.join(", "),
    );
    Ok(format!("{decl}\n\u{0}\n{method}"))
}

/// A payload class's forms, its left-out constructors and its payload.
fn payload_forms(g: &Graph, info: &ClassInfo) -> Res<(Vec<JsForm>, Vec<String>, JsPayload)> {
    let fqn = info.java_fqn.as_str();
    let model = build_model(g, info)?;
    let chain = translated_fields(g, fqn);
    let mut payload = JsPayload::default();
    let mut forms: Vec<JsForm> = Vec::new();
    let mut left_out = Vec::new();
    let py_param = |py: &str| model.params.iter().find(|p| p.name == py);
    for form in &model.forms {
        let ctor = &info.java.ctors[form.ctor];
        let mut typed = Vec::new();
        let mut env = JsEnv::new();
        let mut untranslated = false;
        for (p, py) in ctor.params.iter().zip(&form.params) {
            let (name, kind) = if py == "message" || py == "cause" {
                (py.clone(), None)
            } else {
                match Kind::of(&p.ty) {
                    Some(k) => (p.name.clone(), Some(k)),
                    None => {
                        untranslated = true;
                        (p.name.clone(), None)
                    },
                }
            };
            let nullable = py == "message"
                || py == "cause"
                || py_param(py).is_some_and(|q| q.nullable || q.field_nullable || q.evidenced);
            env.insert(
                p.name.clone(),
                JsVal { expr: format!("v.{name}"), ty: Some(p.ty.clone()), text: None, konst: false },
            );
            typed.push(JsParam { name, java_ty: p.ty.clone(), kind, nullable });
        }
        if untranslated {
            let types: Vec<String> = ctor.params.iter().map(|p| p.ty.clone()).collect();
            left_out.push(format!("`{}({})`", info.java.simple, types.join(", ")));
            continue;
        }
        let eff = js_eval(g, fqn, form.ctor, &env, 0)?;
        let has_cause = typed.iter().any(|p| p.name == "cause");
        if eff.cause != if has_cause { "v.cause" } else { "null" } {
            anyhow::bail!(
                "javascript errors: `{fqn}` form {:?} has the cause `{}`",
                form.params,
                eff.cause
            );
        }
        let message = match &eff.message {
            JsMessage::Null => Message::Null,
            JsMessage::CauseString => Message::CauseString,
            JsMessage::Val(v) if v.expr == "v.message" => Message::Param,
            JsMessage::Val(v) if v.expr == "null" => Message::Null,
            JsMessage::Val(v) => match &v.text {
                Some(t) => Message::Text(t.clone()),
                None => {
                    payload.java_string |= v.expr.contains("javaString(");
                    payload.java_double |= v.expr.contains("javaDoubleString(");
                    Message::Expr(v.expr.clone())
                },
            },
        };
        let mut fields = Vec::new();
        for (field, kind) in &chain {
            let value = match eff.fields.get(field) {
                Some(v) => stored_value(*kind, v),
                None => kind.java_default().to_string(),
            };
            fields.push((field.clone(), value));
        }
        let params: Vec<String> = typed.iter().map(|p| p.name.clone()).collect();
        if !forms.iter().any(|x| x.params == params) {
            forms.push(JsForm { params, message, typed, fields });
        }
    }
    // The names in Java's order (merge_order over the Python forms), as
    // the JavaScript names.
    for p in &model.params {
        let js = if p.name == "message" || p.name == "cause" {
            Some(p.name.clone())
        } else {
            forms
                .iter()
                .flat_map(|f| &f.typed)
                .find(|t| snake(&t.name) == p.name)
                .map(|t| t.name.clone())
        };
        if let Some(js) = js {
            if !payload.order.contains(&js) {
                payload.order.push(js);
            }
        }
    }
    for f in info.java.fields.iter().filter(|f| !f.is_static) {
        match Kind::of(&f.ty) {
            Some(k) => payload.own_fields.push((f.name.clone(), k)),
            None => payload.left_out_members.push(format!("the field `{} {}`", f.ty, f.name)),
        }
    }
    let core = CORE_READS.iter().find(|r| r.0 == fqn);
    for (field, kind) in &chain {
        let read = core.is_some_and(|r| r.4.iter().any(|(f, _, _)| f == field));
        let default = match CORE_DEFAULTS.iter().find(|(c, f, _, _)| *c == fqn && f == field) {
            Some((_, _, v, _)) => (*v).to_string(),
            None => kind.java_default().to_string(),
        };
        if !read {
            let why = CORE_DEFAULTS
                .iter()
                .find(|(c, f, _, _)| *c == fqn && f == field)
                .map(|(_, _, _, w)| (*w).to_string())
                .unwrap_or_else(|| format!("`{}`, Java's default", kind.java_default()));
            payload.core_missing.push(format!("`{field}` (it is {why})"));
        }
        payload.chain_fields.push((field.clone(), *kind, default));
    }
    for gt in &info.java.getters {
        let overrides = ancestor_getter(g, info, &gt.name);
        match &gt.body {
            GetterBody::Field(f) => {
                let fty = g
                    .field_type(fqn, f)
                    .ok_or_else(|| anyhow::anyhow!("{fqn}: getter of unknown field `{f}`"))?;
                match Kind::of(&fty) {
                    Some(kind) => {
                        let attr = attr_name(f);
                        let evidenced = model.params.iter().any(|q| q.evidenced && attr == format!("_{}", q.name));
                        let nullable = kind.reference() && (evidenced || field_nullable(&model, &attr, &fty));
                        payload.getters.push(JsGetter::Field {
                            name: gt.name.clone(),
                            field: f.clone(),
                            kind,
                            nullable,
                            overrides,
                        });
                    },
                    None => payload.left_out_members.push(format!("`{}()`", gt.name)),
                }
            },
            GetterBody::KeySet(f) => {
                payload
                    .getters
                    .push(JsGetter::KeySet { name: gt.name.clone(), field: f.clone(), overrides });
            },
            GetterBody::Abstract => match Kind::of(&gt.ret) {
                Some(ret) => {
                    payload.getters.push(JsGetter::Abstract { name: gt.name.clone(), ret });
                },
                None => payload.left_out_members.push(format!("`{}()`", gt.name)),
            },
        }
    }
    for f in &info.java.factories {
        payload.factories.push(render_factory(g, info, &model, f)?);
    }
    if let Some(expr) = &info.java.to_string {
        payload.to_string = Some(expr.clone());
    }
    Ok((forms, left_out, payload))
}

/// Every class's generated forms and left-out constructors, by Java FQN.
type Forms = BTreeMap<String, (Vec<JsForm>, Vec<String>, Option<JsPayload>)>;

/// One TypeScript constructor signature: its TSDoc, its parameter list and,
/// for a `super(...)` cast, its tuple type.
#[derive(Clone, Debug, PartialEq)]
struct Sig {
    doc: String,
    params: String,
    tuple: String,
}

fn message_doc(m: &Message) -> String {
    match m {
        Message::Param => String::new(),
        Message::CauseString => {
            " The message is the cause's `String()`, as Java's is `cause.toString()` (`''` for a null cause).".into()
        },
        Message::Null => " The message is empty, Java's null.".into(),
        Message::Text(t) => format!(" The message is Java's own: \"{t}\"."),
        Message::Expr(_) => " The message is the one Java's constructor builds from the payload.".into(),
    }
}

/// The signatures of a form: the options object, and the bare value of a
/// one-parameter form. `bare_cause` is the type a bare cause has.
fn form_sigs(form: &JsForm, bare_cause: &str) -> Vec<Sig> {
    let java = |p: &JsParam| match p.name.as_str() {
        "message" => "String message".to_string(),
        "cause" => "Throwable cause".to_string(),
        name => format!("{} {name}", p.java_ty),
    };
    let java_sig: Vec<String> = form.typed.iter().map(java).collect();
    let summary = if java_sig.is_empty() {
        format!("Java's no-argument constructor.{}", message_doc(&form.message))
    } else {
        format!("Java's `({})` constructor.{}", java_sig.join(", "), message_doc(&form.message))
    };
    let field = |p: &JsParam| match p.name.as_str() {
        "message" => (
            "message: string | null".to_string(),
            "The message; `null` reads as `''`.".to_string(),
            "message".to_string(),
        ),
        "cause" => (
            "cause: unknown".to_string(),
            "The cause (`Error`'s `cause`).".to_string(),
            "cause".to_string(),
        ),
        name => {
            let kind = p.kind.expect("a payload parameter has a kind");
            let null = if p.nullable && kind.reference() && kind != Kind::Object {
                " | null"
            } else {
                ""
            };
            (
                format!("{name}: {}{null}", kind.taken()),
                format!("Java's `{} {name}`.", p.java_ty),
                format!("`{name}`"),
            )
        },
    };
    if form.params.is_empty() {
        return vec![Sig { doc: summary, params: String::new(), tuple: "[]".into() }];
    }
    let owned: Vec<(String, String, String)> = form.typed.iter().map(field).collect();
    let fields: Vec<(&str, &str, &str)> = owned.iter().map(|(a, b, c)| (a.as_str(), b.as_str(), c.as_str())).collect();
    // `name?: never` closes the options type against an `Error`, which has a
    // string `message` (and, from ES2022, a `cause`) and would otherwise match
    // it structurally: `new ConfigError(err)` is refused at compile time, as
    // Java refuses `new ConfigException(throwable)`. Every `Error` has a `name`;
    // the forms that take one (`ConfigException(String name, Object value)`)
    // are closed by their other required keys, which no `Error` has. Each key
    // is documented on the options type.
    let close = if form.params.iter().any(|p| p == "name") {
        ""
    } else {
        "; name?: never"
    };
    let plain = format!("{{ {}{close} }}", fields.iter().map(|f| f.0).collect::<Vec<_>>().join("; "));
    let documented = format!(
        "{{ {}{close} }}",
        fields
            .iter()
            .map(|f| format!("/** {} */ {}", f.1, f.0))
            .collect::<Vec<_>>()
            .join("; ")
    );
    let mut sigs = vec![Sig {
        doc: format!(
            "{summary}\n\n@param options - the {}",
            fields.iter().map(|f| f.2).collect::<Vec<_>>().join(" and the ")
        ),
        params: format!("options: {documented}"),
        tuple: format!("[{plain}]"),
    }];
    // A payload parameter is given by name only (root CLAUDE.md, JavaScript
    // Binding Conventions, Errors, Q39).
    if let [p] = form
        .params
        .as_slice()
        .iter()
        .filter(|p| *p == "message" || *p == "cause")
        .collect::<Vec<_>>()[..]
    {
        if form.params.len() != 1 {
            return sigs;
        }
        let (decl, doc) = if p == "message" {
            (
                "message: string | null".to_string(),
                "the message; `null` reads as `''`".to_string(),
            )
        } else {
            (format!("cause: {bare_cause}"), "the cause (`Error`'s `cause`)".to_string())
        };
        let ty = decl.split_once(": ").unwrap().1.to_string();
        sigs.push(Sig {
            doc: format!("{summary} Given the {p} on its own, the same as `{{ {p} }}`.\n\n@param {p} - {doc}"),
            params: decl,
            tuple: format!("[{ty}]"),
        });
    }
    sigs
}

/// A class's TypeScript signatures; empty for a class with no form.
fn class_sigs(forms: &[JsForm]) -> Vec<Sig> {
    let single_message = forms.iter().any(|f| f.params == ["message"]);
    let bare_cause = if single_message { "Error" } else { "Error | null" };
    forms.iter().flat_map(|f| form_sigs(f, bare_cause)).collect()
}

fn class_doc(info: &ClassInfo, constructible: bool) -> String {
    let mut parts = Vec::new();
    if !info.java.javadoc.is_empty() {
        parts.push(info.java.javadoc.clone());
    }
    if info.is_jdk {
        parts.push(format!("Java's built-in ``{}``.", info.java_fqn));
    } else {
        parts.push(format!("Java: ``{}``.", info.java_fqn));
    }
    if info.is_abstract {
        parts.push("A catch-only base: constructing it throws ``TypeError``.".into());
    } else if !constructible {
        parts.push(
            "Not constructible yet: Java's constructors take a payload the client does not translate yet; \
             the client builds this error when the core reports it."
                .into(),
        );
    }
    parts.join("\n\n")
}

/// The JavaScript form table of `setForms`.
fn forms_literal(forms: &[JsForm]) -> String {
    let rows: Vec<String> = forms
        .iter()
        .map(|f| {
            let params: Vec<String> = f.params.iter().map(|p| js_str(p)).collect();
            let message = match &f.message {
                Message::Param => "'message'".to_string(),
                Message::CauseString => "'cause.toString()'".to_string(),
                Message::Null => "null".to_string(),
                Message::Text(t) => format!("{{ text: {} }}", js_str(t)),
                Message::Expr(e) => format!("(v) => {e}"),
            };
            if f.fields.is_empty() {
                return format!("  [[{}], {message}],", params.join(", "));
            }
            let fields: Vec<String> = f.fields.iter().map(|(k, v)| format!("{k}: {v}")).collect();
            let fields = fields.join(", ");
            let arg = if fields.contains("v.") { "(v)" } else { "()" };
            format!("  [[{}], {message}, {arg} => ({{ {fields} }})],", params.join(", "))
        })
        .collect();
    if rows.is_empty() {
        "[]".into()
    } else {
        format!("[\n{}\n]", rows.join("\n"))
    }
}

/// The signatures a class's TypeScript constructor shows, those of its forms
/// (a class that declares no constructor shows its parent's, which are the
/// same); `None` for a class with no form, whose `protected` constructor
/// takes `...args`.
fn effective_sigs(own: &Forms, fqn: &str) -> Option<Vec<Sig>> {
    let sigs = class_sigs(&own[fqn].0);
    (!sigs.is_empty()).then_some(sigs)
}

/// Whether `info` declares a TypeScript constructor: the root and the abstract
/// bases always, a class with no form, and a class whose signatures differ
/// from its parent's.
fn declares_constructor(own: &Forms, info: &ClassInfo) -> bool {
    let (forms, _, payload) = &own[&info.java_fqn];
    if is_root_class(info) || info.is_abstract || forms.is_empty() {
        return true;
    }
    if payload.as_ref().is_some_and(|p| !p.own_fields.is_empty()) {
        return true;
    }
    effective_sigs(own, &info.java_fqn) != effective_sigs(own, &info.parent_fqn)
}

/// Appends `note` to a module header as `//` lines wrapped at 79 columns,
/// after a `//` line.
fn push_note(text: &mut String, note: &str) {
    let mut line = String::from("//");
    text.push_str("//\n");
    for word in note.split_whitespace() {
        if line.len() + 1 + word.len() > 79 {
            text.push_str(&line);
            text.push('\n');
            line = String::from("//");
        }
        line.push(' ');
        line.push_str(word);
    }
    text.push_str(&line);
    text.push('\n');
}

/// `items` as an English list: `a`, `a and b`, `a, b and c`.
fn and_list(items: &[String]) -> String {
    match items {
        [] => String::new(),
        [one] => one.clone(),
        [init @ .., last] => format!("{} and {last}", init.join(", ")),
    }
}

/// The `setForms` options of a payload class: the names in Java's order,
/// each payload parameter's kind, and each field's kind and core default.
fn payload_options(forms: &[JsForm], payload: &JsPayload) -> String {
    let mut params: Vec<(String, Kind)> = Vec::new();
    for p in forms.iter().flat_map(|f| &f.typed) {
        if let Some(k) = p.kind {
            if !params.iter().any(|(n, _)| *n == p.name) {
                params.push((p.name.clone(), k));
            }
        }
    }
    let mut parts = Vec::new();
    if !params.is_empty() {
        let order: Vec<String> = payload.order.iter().map(|n| js_str(n)).collect();
        parts.push(format!("  order: [{}],", order.join(", ")));
    }
    if !params.is_empty() {
        let p: Vec<String> = params.iter().map(|(n, k)| format!("{n}: P.{}", k.export())).collect();
        parts.push(format!("  params: {{ {} }},", p.join(", ")));
    }
    if !payload.chain_fields.is_empty() {
        let f: Vec<String> = payload
            .chain_fields
            .iter()
            .map(|(n, k, d)| format!("{n}: [P.{}, {d}]", k.export()))
            .collect();
        parts.push(format!("  fields: {{ {} }},", f.join(", ")));
    }
    if parts.is_empty() {
        return String::new();
    }
    format!("{{\n{}\n}}", parts.join("\n"))
}

/// A getter of a payload class, as TypeScript.
fn render_getter(gt: &JsGetter, own: &[(String, Kind)]) -> Res<String> {
    let own_kind = |field: &str| {
        own.iter().find(|(f, _)| f == field).map(|(_, k)| *k).ok_or_else(|| {
            anyhow::anyhow!("javascript errors: a getter reads `{field}`, which the class does not declare")
        })
    };
    let modifier = |overrides: bool| if overrides { "override " } else { "" };
    Ok(match gt {
        JsGetter::Field { name, field, kind, nullable, overrides } => {
            let stored = own_kind(field)?;
            let ret = if *nullable {
                format!("{} | null", kind.returned())
            } else {
                kind.returned().to_string()
            };
            let body = match kind {
                Kind::StringSet | Kind::TopicPartitions | Kind::TopicPartitionLongMap => {
                    format!("return P.{}.copy(this.#{field}) as {ret};", kind.export())
                },
                _ if stored.stored() != ret => format!("return this.#{field} as {ret};"),
                _ => format!("return this.#{field};"),
            };
            let copy = matches!(kind, Kind::StringSet | Kind::TopicPartitions | Kind::TopicPartitionLongMap);
            let returns = if copy {
                format!(
                    "Java's `{field}`, as a new {} the caller may change",
                    kind.returned().split('<').next().unwrap()
                )
            } else {
                format!("Java's `{field}`")
            };
            format!(
                "{}\n  {}{name}(): {ret} {{\n    P.readOnly({}, arguments.length);\n    {body}\n  }}",
                tsdoc(
                    &format!("Java's `{name}()`.\n\n@returns {returns}\n@throws `TypeError` when it is given an argument: it only reads"),
                    "  "
                ),
                modifier(*overrides),
                js_str(name)
            )
        },
        JsGetter::KeySet { name, field, overrides } => {
            let stored = own_kind(field)?;
            if stored != Kind::TopicPartitionLongMap {
                anyhow::bail!(
                    "javascript errors: `{name}()` is the key set of `{field}`, which is no TopicPartitionMap"
                );
            }
            format!(
                "{}\n  {}{name}(): TopicPartitionSet {{\n    P.readOnly({}, arguments.length);\n    return new TopicPartitionSet(P.nonNull(this.#{field}, {}).keys());\n  }}",
                tsdoc(
                    &format!(
                        "Java's `{name}()`: the partitions of `{field}`.\n\n@returns a new TopicPartitionSet of the keys of `{field}`\n\
                         @throws `TypeError` when it is given an argument (it only reads), or `{field}` is null"
                    ),
                    "  "
                ),
                modifier(*overrides),
                js_str(name),
                js_str(field)
            )
        },
        JsGetter::Abstract { name, ret } => format!(
            "{}\n  abstract {name}(): {};",
            tsdoc(
                &format!("Java's abstract `{name}()`.\n\n@returns what the subclass gives"),
                "  "
            ),
            ret.returned()
        ),
    })
}

/// Java's `toString()` override of a payload class, as TypeScript: the
/// override's `+` chain, Java's class name as the error's `name`. A field it
/// dereferences that is null (an error the core reports without it) prints
/// `Error`'s text instead.
fn render_to_string(fqn: &str, expr: &str, own: &[(String, Kind)]) -> Res<String> {
    let parts = split_plus(expr);
    let bad = |what: &str| anyhow::anyhow!("{fqn}.toString(): unsupported {what} in `{expr}`");
    let field_of = |s: &str| -> Option<(String, Kind)> {
        let name = s.strip_prefix("this.").unwrap_or(s);
        own.iter().find(|(f, _)| f == name).cloned()
    };
    let mut js = Vec::new();
    let mut guards: Vec<String> = Vec::new();
    for (i, part) in parts.iter().enumerate() {
        let part = part.trim();
        let code = if part == "getClass().getName()" {
            if i != 0 {
                return Err(bad("class name not first"));
            }
            "this.name".to_string()
        } else if part.starts_with('"') && part.ends_with('"') && part.len() >= 2 {
            if part.contains('\\') {
                return Err(bad(&format!("escaped literal `{part}`")));
            }
            js_str(&part[1..part.len() - 1])
        } else if let Some(call) = part.strip_suffix("()") {
            let (target, method) = call.rsplit_once('.').ok_or_else(|| bad(&format!("call `{part}`")))?;
            let (field, _) = field_of(target).ok_or_else(|| bad(&format!("call `{part}` (not on a field)")))?;
            if !guards.contains(&field) {
                guards.push(field.clone());
            }
            format!("javaString(this.#{field}.{method}())")
        } else if let Some((field, kind)) = field_of(part) {
            if kind == Kind::Double {
                format!("javaDoubleString(this.#{field})")
            } else {
                format!("javaString(this.#{field})")
            }
        } else {
            return Err(bad(&format!("part `{part}`")));
        };
        js.push(code);
    }
    let guard: Vec<String> = guards.iter().map(|f| format!("this.#{f} === null")).collect();
    let fallback = if guard.is_empty() {
        String::new()
    } else {
        format!("    if ({}) return super.toString();\n", guard.join(" || "))
    };
    Ok(format!(
        "{}\n  override toString(): string {{\n{fallback}    return {};\n  }}",
        tsdoc(
            "Java's `toString()` override, Java's class name written as the error's `name`; `Error`'s text for an \
             error the core reports without the field it reads.\n\n@returns the error's text",
            "  "
        ),
        js.join(" + ")
    ))
}

fn render(g: &BTreeMap<String, ClassInfo>, own: &Forms, info: &ClassInfo) -> Res<JsModule> {
    let dir = js_dir(&info.py_module);
    let (forms, left_out, payload) = &own[&info.java_fqn];
    let name = &info.py_name;
    let mut text = header(&format!("the Java source of `{}`", info.java_fqn));
    let mut notes: Vec<String> = Vec::new();
    let mut push_note = |text: &mut String, note: &str| {
        notes.push(note.to_string());
        push_note(text, note);
    };
    if let Some((_, why)) = LEFT_OUT_PAYLOADS.iter().find(|(c, _)| *c == info.java_fqn) {
        let getters: Vec<String> = info.java.getters.iter().map(|gt| format!("`{}()`", gt.name)).collect();
        push_note(
            &mut text,
            &format!(
                "Not generated: Java's payload, as {why}: the constructors {}, the getters {}, and the payload of an \
                 error the core reports.",
                and_list(left_out),
                and_list(&getters)
            ),
        );
    } else if !left_out.is_empty() {
        push_note(
            &mut text,
            &format!(
                "Not generated: Java's constructors that take a type the client does not translate: {}.",
                and_list(left_out)
            ),
        );
    }
    if let Some(p) = payload {
        if !p.left_out_members.is_empty() {
            push_note(
                &mut text,
                &format!(
                    "Not generated, for a type the client does not translate: {}.",
                    and_list(&p.left_out_members)
                ),
            );
        }
        if !p.core_missing.is_empty() {
            push_note(
                &mut text,
                &format!(
                    "An error the core reports has no payload for {}: the FFI's view of the class has no accessor \
                     for it.",
                    and_list(&p.core_missing)
                ),
            );
        }
    }
    text.push('\n');
    let root = is_root_class(info);
    let mut helpers: Vec<&str> = Vec::new();
    if root {
        helpers.extend(["errorArgs", "init"]);
    }
    helpers.push("setForms");
    if info.is_abstract {
        helpers.push("isCreate");
    }
    if !info.java.singletons.is_empty() {
        helpers.push("create");
    }
    if payload.as_ref().is_some_and(|p| !p.own_fields.is_empty()) {
        helpers.push("payloadOf");
    }
    helpers.sort_unstable();
    let mut imports: Vec<String> = Vec::new();
    if let Some(p) = payload {
        if !p.factories.is_empty() {
            imports.push(format!(
                "import {{ JavaForms }} from '{}';\n",
                rel_import(&dir, "internal/args")
            ));
        }
        let mut strings: Vec<&str> = Vec::new();
        let rendered_strings =
            p.java_string || p.to_string.is_some() || p.factories.iter().any(|f| f.contains("javaString("));
        if p.java_double
            || p.to_string
                .as_ref()
                .is_some_and(|_| p.own_fields.iter().any(|(_, k)| *k == Kind::Double))
        {
            strings.push("javaDoubleString");
        }
        if rendered_strings {
            strings.push("javaString");
        }
        if !strings.is_empty() {
            imports.push(format!(
                "import {{ {} }} from '{}';\n",
                strings.join(", "),
                rel_import(&dir, "internal/java_string")
            ));
        }
        let uses_p = !p.chain_fields.is_empty()
            || p.getters.iter().any(|gt| !matches!(gt, JsGetter::Abstract { .. }))
            || forms
                .iter()
                .any(|f| f.typed.iter().any(|t| t.kind.is_some()) || f.fields.iter().any(|(_, v)| v.contains("P.")));
        if uses_p {
            imports.push(format!("import * as P from '{}';\n", rel_import(&dir, "internal/payload")));
        }
    }
    imports.push(format!(
        "import {{ {} }} from '{}';\n",
        helpers.join(", "),
        rel_import(&dir, "internal/throwable")
    ));
    if let Some(p) = payload {
        let key_set = p.getters.iter().any(|gt| matches!(gt, JsGetter::KeySet { .. }));
        let mut names: BTreeSet<&str> = BTreeSet::new();
        // The types a signature, the parent's signature this constructor
        // casts its arguments to, a field or a getter names.
        let parent_first = own.get(&info.parent_fqn).and_then(|(f, _, _)| f.first());
        for t in forms
            .iter()
            .flat_map(|f| &f.typed)
            .chain(parent_first.into_iter().flat_map(|f| &f.typed))
        {
            if let Some(k) = t.kind {
                names.extend(k.taken_names());
            }
        }
        for (_, k) in &p.own_fields {
            names.extend(k.returned_names());
        }
        for gt in &p.getters {
            match gt {
                JsGetter::Field { kind, .. } => names.extend(kind.returned_names()),
                JsGetter::Abstract { ret, .. } => names.extend(ret.returned_names()),
                JsGetter::KeySet { .. } => {
                    names.insert("TopicPartitionSet");
                },
            }
        }
        for n in names {
            let kw = if n == "TopicPartitionSet" && key_set {
                "import"
            } else {
                "import type"
            };
            imports.push(format!("{kw} {{ {n} }} from '{}';\n", rel_import(&dir, type_module(n))));
        }
    }
    let parent = if root {
        if RANGE_ERRORS.contains(&info.java_fqn.as_str()) {
            "RangeError".to_string()
        } else {
            "Error".to_string()
        }
    } else {
        let parent = g
            .get(&info.parent_fqn)
            .ok_or_else(|| anyhow::anyhow!("javascript errors: `{}` has no parent class", info.java_fqn))?;
        let from = rel_import(&dir, &js_path(parent));
        imports.push(format!("import {{ {} }} from '{from}';\n", parent.py_name));
        parent.py_name.clone()
    };
    // A payload class's imports sorted by their module path, as the
    // hand-written modules order them.
    if payload.is_some() {
        imports.sort_by_key(|line| line.rsplit_once(" from ").map(|(_, m)| m.to_string()).unwrap_or_default());
    }
    for line in &imports {
        text.push_str(line);
    }
    if let Some(p) = payload {
        for f in &p.factories {
            if let Some((decl, _)) = f.split_once("\n\u{0}\n") {
                text.push('\n');
                text.push_str(decl);
                text.push('\n');
            }
        }
    }
    text.push('\n');
    text.push_str(&tsdoc(&class_doc(info, !forms.is_empty()), ""));
    text.push('\n');
    let kw = if info.is_abstract {
        "export abstract class"
    } else {
        "export class"
    };
    let mut body: Vec<String> = Vec::new();
    if root {
        body.push(
            r"  /**
   * The error's cause, Java's `getCause()`: set by `super()` (`Error`'s
   * `options.cause`), declared here for the targets whose `lib` predates
   * ES2022's `Error.cause`. A `declare` field, which emits nothing: a class
   * field would overwrite it.
   */
  declare readonly cause: unknown;"
                .to_string(),
        );
    }
    // A private member of the class's own, so TypeScript checks an error by
    // its class's name, not by its shape (root conventions, Stability): two
    // classes with the same public members are not assignable to each other.
    body.push(format!(
        "{}\n  readonly #brand = true;",
        tsdoc(
            &format!("Makes `{name}` nominal: TypeScript checks it by name, not by shape. It holds no state."),
            "  "
        )
    ));
    if let Some(p) = payload {
        for (field, kind) in &p.own_fields {
            let java = info
                .java
                .fields
                .iter()
                .find(|f| f.name == *field)
                .map(|f| f.ty.clone())
                .unwrap_or_default();
            body.push(format!(
                "{}\n  readonly #{field}: {};",
                tsdoc(&format!("Java's field `{java} {field}`."), "  "),
                kind.stored()
            ));
        }
    }
    if let Some((constant, value)) = &info.ffi_id {
        let note = if info.java_fqn == KAFKA_EXCEPTION_FQN {
            " A bare KafkaException is reported as UNKNOWN_SERVER_ERROR; UnknownServerError owns that id in the \
             id -> class table."
        } else {
            ""
        };
        // A class below one with an id overrides its ancestor's static.
        let mut inherits = false;
        let mut up = g.get(&info.parent_fqn);
        while let Some(parent) = up {
            if parent.ffi_id.is_some() {
                inherits = true;
                break;
            }
            up = g.get(&parent.parent_fqn);
        }
        let modifier = if inherits { "override " } else { "" };
        body.push(format!(
            "{}\n  static {modifier}readonly _ffiId: number = {value};",
            tsdoc(
                &format!(
                    "The FFI id of the class, `kafka_common_Error_{constant}`, for the binding and the test \
                     harness.{note}\n\n@internal"
                ),
                "  "
            )
        ));
    }
    for single in &info.java.singletons {
        body.push(format!(
            "{}
  declare static readonly {}: {name};",
            tsdoc(
                &format!("Java's `public static final {} {}`.", info.java.simple, single.name),
                "  "
            ),
            single.name
        ));
    }
    let own_fields: &[(String, Kind)] = payload.as_ref().map(|p| p.own_fields.as_slice()).unwrap_or(&[]);
    if declares_constructor(own, info) {
        let sigs = class_sigs(forms);
        let mut ctor = Vec::new();
        if sigs.is_empty() {
            ctor.push(tsdoc(
                "Not constructible yet: Java's constructors take a type the client does not translate yet.\n\n\
                 @param args - refused: constructing the error throws `TypeError`",
                "  ",
            ));
            ctor.push("  protected constructor(...args: unknown[]) {".into());
        } else {
            for s in &sigs {
                ctor.push(tsdoc(&s.doc, "  "));
                ctor.push(format!("  constructor({});", s.params));
            }
            ctor.push("  constructor(...args: unknown[]) {".into());
        }
        if info.is_abstract {
            ctor.push(format!("    if (new.target === {name} && !isCreate(args)) {{"));
            ctor.push(format!(
                "      throw new TypeError('{name} is an abstract catch-only base; it is never thrown directly');"
            ));
            ctor.push("    }".into());
        }
        if root {
            ctor.push("    super(...errorArgs(new.target, args));".into());
            ctor.push("    init(this, new.target);".into());
        } else {
            match effective_sigs(own, &info.parent_fqn).as_ref().and_then(|s| s.first().cloned()) {
                None => ctor.push("    super(...args);".into()),
                Some(first) => ctor.push(format!("    super(...(args as {}));", first.tuple)),
            }
        }
        if !own_fields.is_empty() {
            // The payload fields the root's constructor computed for the
            // form given (or read from the core's payload), this class's own.
            ctor.push("    const fields = payloadOf(this);".into());
            for (field, kind) in own_fields {
                ctor.push(format!("    this.#{field} = fields.{field} as {};", kind.stored()));
            }
        }
        ctor.push("  }".into());
        body.push(ctor.join("\n"));
    }
    if let Some(p) = payload {
        for f in &p.factories {
            let method = f.split_once("\n\u{0}\n").map(|(_, m)| m).unwrap_or(f);
            body.push(method.to_string());
        }
        for gt in &p.getters {
            body.push(render_getter(gt, own_fields)?);
        }
        if let Some(expr) = &p.to_string {
            body.push(render_to_string(&info.java_fqn, expr, own_fields)?);
        }
    }
    if body.is_empty() {
        text.push_str(&format!("{kw} {name} extends {parent} {{}}\n"));
    } else {
        text.push_str(&format!("{kw} {name} extends {parent} {{\n{}\n}}\n", body.join("\n\n")));
    }
    match payload {
        Some(p) if !payload_options(forms, p).is_empty() => text.push_str(&format!(
            "\nsetForms({name}, {}, {});\n",
            forms_literal(forms),
            payload_options(forms, p)
        )),
        _ => text.push_str(&format!("\nsetForms({name}, {});\n", forms_literal(forms))),
    }
    for single in &info.java.singletons {
        // Built as the core's errors are (`create`), whatever constructors
        // the class has public (Java's may be private: the no-argument one of
        // `CoordinatorNotAvailableException`): Java's no-argument chain, an
        // empty message (Java's null) and no cause. A singleton built with
        // arguments stops generation until their values are translated.
        if !single.args.is_empty() {
            anyhow::bail!(
                "javascript errors: `{}.{}` is built with arguments, which the generator does not translate",
                info.java_fqn,
                single.name
            );
        }
        text.push_str(&format!(
            "Object.defineProperty({name}, '{0}', {{ value: create({name}, ''), enumerable: true }});\n",
            single.name
        ));
    }
    Ok(JsModule { dir, stem: info.file_stem(), name: info.py_name.clone(), content: text, notes })
}

/// native/error_payload.c: the payload of an error the core reports, read
/// through the FFI's view of its class ([`CORE_READS`]), as a plain object
/// keyed by Java field name, which src/internal/throwable.ts turns into the
/// class's fields. Each field is read by native/error.c's `payload_<conv>`
/// helper; a view the error is not (NULL) gives no payload.
fn c_payload_source(classes: &BTreeMap<String, ClassInfo>, ids: &BTreeMap<String, i32>) -> Res<String> {
    let mut out = String::from(
        "// Copyright 2026 Confluent Inc.\n\
         //\n\
         // Licensed under the Apache License, Version 2.0 (the \"License\");\n\
         // you may not use this file except in compliance with the License.\n\
         // You may obtain a copy of the License at\n\
         //\n\
         //     http://www.apache.org/licenses/LICENSE-2.0\n\
         //\n\
         // Unless required by applicable law or agreed to in writing, software\n\
         // distributed under the License is distributed on an \"AS IS\" BASIS,\n\
         // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n\
         // See the License for the specific language governing permissions and\n\
         // limitations under the License.\n\
         \n\
         // GENERATED, DO NOT EDIT. Produced by `cargo xtask generate-error-codes` from\n\
         // the table of the FFI's error views in rust/xtask/src/error_hierarchy/js.rs;\n\
         // `cargo xtask check-generated` checks it is up to date.\n\
         //\n\
         // The payload of an error the core reports (root CLAUDE.md, JavaScript Binding\n\
         // Conventions, Errors, Q39): the FFI's view of its class\n\
         // (kafka_common_Error_<class>, NULL for another class), then the view's\n\
         // accessors, each field set on a plain object by native/error.c's payload_*\n\
         // helper, keyed by its Java field name. NULL when the class has none.\n\
         \n\
         #include \"util.h\"\n\
         \n\
         napi_value error_payload_to_js(napi_env env, int code, const kafka_common_Error_t* e) {\n    \
         kafka_common_Error_t* ffi_err = NULL;\n    \
         napi_value obj = NULL;\n    \
         switch (code) {\n",
    );
    for (fqn, code, view, view_ty, fields) in CORE_READS {
        if !classes.contains_key(*fqn) {
            anyhow::bail!("javascript errors: the FFI view of `{fqn}` names no generated class");
        }
        if !ids.contains_key(*code) {
            anyhow::bail!("javascript errors: `kafka_common_Error_{code}` is no FFI id");
        }
        out.push_str(&format!(
            "    case kafka_common_Error_{code}: {{\n        \
             const {view_ty}* view = NULL;\n        \
             error_discard(FFI_SET(view, {view}(e, &ffi_err)));\n        \
             if (view == NULL || napi_create_object(env, &obj) != napi_ok) return NULL;\n"
        ));
        for (field, accessor, conv) in *fields {
            // The accessor's owned error, or NULL, goes to the helper, which
            // leaves the field out on a failure.
            out.push_str(&format!(
                "        {{\n            {} value = {};\n            \
                 kafka_common_Error_t* failed = FFI_SET(value, {accessor}(view, &ffi_err));\n            \
                 if ({}(env, obj, \"{field}\", value, failed)) return NULL;\n        }}\n",
                conv.c_type(),
                conv.c_init(),
                conv.helper()
            ));
        }
        out.push_str("        return obj;\n    }\n");
    }
    out.push_str("    default:\n        return NULL;\n    }\n}\n");
    Ok(out)
}

/// The class modules and `src/internal/error_registry.ts`.
pub(crate) fn outputs(repo_root: &Path) -> Res<(Vec<JsModule>, Vec<GeneratedFile>)> {
    let classes = js_graph(repo_root)?;
    let graph = Graph { classes: &classes };
    let mut own = BTreeMap::new();
    for (fqn, info) in &classes {
        let model = build_model(&graph, info)?;
        let entry = if has_payload(&graph, info, &model) {
            let (forms, left_out, payload) = payload_forms(&graph, info)?;
            // A form of a message and a cause only is the same either way.
            let (plain, _) = class_forms(&graph, info)?;
            for f in forms.iter().filter(|f| f.params.iter().all(|p| p == "message" || p == "cause")) {
                let same = plain.iter().find(|x| x.params == f.params);
                if same.map(|x| &x.message) != Some(&f.message) {
                    anyhow::bail!(
                        "javascript errors: `{fqn}` form {:?}: {:?} is not {:?}",
                        f.params,
                        f.message,
                        same
                    );
                }
            }
            (forms, left_out, Some(payload))
        } else {
            let (forms, left_out) = class_forms(&graph, info)?;
            (forms, left_out, None)
        };
        own.insert(fqn.clone(), entry);
    }
    let mut modules = Vec::new();
    for info in classes.values() {
        modules.push(render(&classes, &own, info)?);
    }
    let mut rows: Vec<(String, String)> = classes
        .values()
        .map(|c| {
            (
                c.module_file(),
                format!(
                    "  [{}, {}, {}],",
                    js_str(&format!("../{}", js_path(c))),
                    js_str(&c.py_name),
                    js_str(&c.java_fqn)
                ),
            )
        })
        .collect();
    rows.sort();
    let names: Vec<String> = {
        let mut ids: Vec<(i32, String)> = parse_ffi_enum_ids(repo_root)?.into_iter().map(|(n, v)| (v, n)).collect();
        ids.sort();
        ids.into_iter().map(|(v, n)| format!("  [{v}, {}],", js_str(&n))).collect()
    };
    let mut registry = header("the Java exception sources and the FFI enum `kafka_common_Error_e`");
    registry.push_str(
        "\n// Every generated error class, as [module (relative to src/internal/), class\n\
         // name, Java class] (python/confluent_kafka/_error_registry.py's ERRORS), and\n\
         // the enumerator name of every kafka_common_Error_e value (private).\n\n",
    );
    registry.push_str(
        "/** A generated error class: its module, its class name and its Java class. */\n\
         type ErrorRow = readonly [module: string, name: string, java: string];\n\n\
         export const ERRORS: ReadonlyArray<ErrorRow> = Object.freeze<ErrorRow[]>([\n",
    );
    for (_, row) in &rows {
        registry.push_str(row);
        registry.push('\n');
    }
    registry.push_str("]);\n\nexport const FFI_NAMES: ReadonlyMap<number, string> = new Map<number, string>([\n");
    for n in &names {
        registry.push_str(n);
        registry.push('\n');
    }
    registry.push_str("]);\n");
    let path = repo_root.join(JS_SRC).join("internal").join("error_registry.ts");
    let payload = c_payload_source(&classes, &parse_ffi_enum_ids(repo_root)?)?;
    let c_path = repo_root.join("javascript").join("native").join("error_payload.c");
    Ok((modules, vec![(path, registry), (c_path, payload)]))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn repo_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .unwrap()
            .to_path_buf()
    }

    #[test]
    fn test_js_dirs() {
        assert_eq!(js_dir("confluent_kafka"), "");
        assert_eq!(js_dir("confluent_kafka.common.errors"), "common/errors");
    }

    #[test]
    fn test_literal_text() {
        assert_eq!(literal_text(r#""a" + "b\"c""#).as_deref(), Some("ab\"c"));
        assert_eq!(literal_text(r#""line\n""#).as_deref(), Some("line\n"));
        assert_eq!(literal_text("message"), None);
        assert_eq!(literal_text(r#""a" + java_str(x)"#), None);
    }

    #[test]
    fn test_classes_keep_the_java_chain() {
        let (modules, files) = outputs(&repo_root()).unwrap();
        let registry = &files[0];
        let find = |name: &str| modules.iter().find(|m| m.name == name).unwrap();
        let sasl = find("SaslAuthenticationError");
        assert_eq!(sasl.dir, "common/errors");
        assert!(sasl
            .content
            .contains("export class SaslAuthenticationError extends AuthenticationError {"));
        assert!(sasl
            .content
            .contains("import { AuthenticationError } from './authentication_error';"));
        assert!(!sasl.content.contains("require("));
        assert!(!sasl.content.contains("module.exports"));
        assert!(sasl.content.contains("import { setForms } from '../../internal/throwable';"));
        assert!(sasl.content.contains("\n  static override readonly _ffiId: number = 58;\n"));
        let kafka = find("KafkaError");
        assert_eq!(kafka.dir, "common");
        assert!(kafka.content.contains("export class KafkaError extends Error {"));
        assert!(kafka
            .content
            .contains("import { errorArgs, init, setForms } from '../internal/throwable';"));
        // No public field but `cause` (the FFI id is private).
        for gone in ["code", "errorName", "retriable", "apiError"] {
            assert!(!kafka.content.contains(&format!("readonly {gone}")), "{gone}");
        }
        assert!(kafka.content.contains("  declare readonly cause: unknown;\n"));
        // Java's four constructors, in Java order, each by name and, with one
        // parameter, bare.
        assert!(kafka
            .content
            .contains("  constructor(options: { /** The message; `null` reads as `''`. */ message: string | null; /** The cause (`Error`'s `cause`). */ cause: unknown; name?: never });\n"));
        assert!(kafka.content.contains("  constructor(message: string | null);\n"));
        assert!(kafka.content.contains(
            "  constructor(options: { /** The cause (`Error`'s `cause`). */ cause: unknown; name?: never });\n"
        ));
        assert!(kafka.content.contains("  constructor(cause: Error);\n"));
        assert!(kafka.content.contains("  constructor();\n"));
        assert!(kafka.content.contains(
            "  constructor(...args: unknown[]) {\n    super(...errorArgs(new.target, args));\n    init(this, new.target);\n  }\n"
        ));
        assert!(kafka.content.contains(
            "setForms(KafkaError, [\n  [['message', 'cause'], 'message'],\n  [['message'], 'message'],\n  [['cause'], 'cause.toString()'],\n  [[], null],\n]);\n"
        ));
        let state = find("IllegalStateError");
        assert_eq!(state.dir, "");
        assert!(state.content.contains("export class IllegalStateError extends Error {"));
        assert!(state.content.contains("\n  static readonly _ffiId: number = -4;\n"));
        // JavaScript's RangeError for the two out-of-range Java built-ins.
        assert!(find("IllegalArgumentError")
            .content
            .contains("export class IllegalArgumentError extends RangeError {"));
        assert!(find("IndexOutOfBoundsError")
            .content
            .contains("export class IndexOutOfBoundsError extends RangeError {"));
        // ExecutionException has no (String) constructor: a bare cause may be null.
        let execution = find("ExecutionError");
        assert!(execution.content.contains("  constructor(cause: Error | null);\n"));
        assert!(!execution.content.contains("  constructor(message: string | null);\n"));
        // Java's CancellationException, the client's own: it keeps Java's chain.
        let cancellation = find("CancellationError");
        assert_eq!(cancellation.dir, "");
        assert!(cancellation
            .content
            .contains("export class CancellationError extends IllegalStateError {"));
        assert!(cancellation
            .content
            .contains("import { IllegalStateError } from './illegal_state_error';"));
        assert!(cancellation
            .content
            .contains("setForms(CancellationError, [\n  [[], null],\n  [['message'], 'message'],\n]);\n"));
        assert!(!cancellation.content.contains("_ffiId"));
        let retriable = find("RetriableError");
        // Java's abstract base is abstract: TypeScript rejects `new`, the runtime guard stays for JavaScript.
        assert!(retriable
            .content
            .contains("export abstract class RetriableError extends ApiError {"));
        assert!(retriable
            .content
            .contains("if (new.target === RetriableError && !isCreate(args)) {"));
        assert!(!retriable.content.contains("_ffiId"));
        assert!(retriable.content.contains("\nsetForms(RetriableError, ["));
        let api = find("ApiError");
        assert!(api.content.contains("import { KafkaError } from '../kafka_error';"));
        // Java's payload constructors, by name, and their getters over private fields.
        let topic = find("TopicAuthorizationError");
        assert!(!topic.content.contains("Not generated"));
        assert!(topic.content.contains(
            "  [['unauthorizedTopics'], (v) => 'Not authorized to access topics: ' + javaString(v.unauthorizedTopics), \
             (v) => ({ unauthorizedTopics: v.unauthorizedTopics })],\n"
        ));
        assert!(topic
            .content
            .contains("  [['message'], 'message', () => ({ unauthorizedTopics: new Set<string>() })],\n"));
        assert!(topic.content.contains("  params: { unauthorizedTopics: P.stringSet },\n"));
        assert!(topic.content.contains("  readonly #unauthorizedTopics: Set<string> | null;\n"));
        assert!(topic.content.contains(
            "  unauthorizedTopics(): Set<string> {\n    P.readOnly('unauthorizedTopics', arguments.length);\n    \
             return P.stringSet.copy(this.#unauthorizedTopics) as Set<string>;\n  }\n"
        ));
        assert!(topic.content.contains(
            "/** Java's `Set<String> unauthorizedTopics`. */ unauthorizedTopics: Iterable<string>; name?: never });"
        ));
        // A form whose parameter has a type the client does not translate is
        // left out and listed, with the members of that type.
        let truncation = find("LogTruncationError");
        assert!(truncation
            .content
            .contains("// Not generated: Java's constructors that take a type the client does not\n"));
        assert!(truncation.content.contains("`divergentOffsets()`.\n"));
        assert!(truncation
            .content
            .contains("  fields: { offsetOutOfRangePartitions: [P.topicPartitionLongMap, null] },\n"));
        // A payload field the FFI has no accessor for is listed, with its default.
        let mismatch = find("CorrelationIdMismatchError");
        assert!(mismatch
            .content
            .contains("// An error the core reports has no payload for `requestCorrelationId` (it is\n"));
        assert!(mismatch
            .content
            .contains("  fields: { requestCorrelationId: [P.int, -1], responseCorrelationId: [P.int, -1] },\n"));
        // A Map<TopicPartition, Long> is a TopicPartitionMap; keySet() a TopicPartitionSet.
        let range = modules
            .iter()
            .find(|m| m.name == "OffsetOutOfRangeError" && m.dir == "consumer")
            .unwrap();
        assert!(range
            .content
            .contains("offsetOutOfRangePartitions: TopicPartitionMap<bigint | number>"));
        assert!(range.content.contains("  override partitions(): TopicPartitionSet {\n"));
        assert!(range
            .content
            .contains("import { TopicPartitionSet } from '../common/topic_partition_set';\n"));
        // Java's static factory, the value on its own or by name.
        let group = find("GroupAuthorizationError");
        assert!(group
            .content
            .contains("  static forGroupId(groupId: string | null): GroupAuthorizationError;\n"));
        assert!(group.content.contains(
            "    return new GroupAuthorizationError({ message: 'Not authorized to access group: ' + javaString(groupId), \
             groupId: groupId as string | null });\n"
        ));
        // A subclass with forms of its own passes its arguments on.
        assert!(topic
            .content
            .contains("    super(...(args as [{ message: string | null; name?: never }]));\n"));
        // A fixed Java message is kept as Java's text.
        let commit = find("RetriableCommitFailedError");
        assert!(commit.content.contains(
            "  [['cause'], { text: 'Offset commit failed with a retriable exception. You should retry committing the latest consumed offsets.' }],\n"
        ));
        // A class whose every constructor takes an untranslated type cannot be constructed yet.
        assert!(truncation.content.contains("  protected constructor(...args: unknown[]) {\n"));
        assert!(truncation.content.contains("\nsetForms(LogTruncationError, [], {\n"));
        // Java's toString() override, and Java's Double.toString for a double.
        let quota = find("QuotaViolationError");
        assert!(quota.content.contains("javaDoubleString(this.#value)"));
        assert!(quota
            .content
            .contains("    if (this.#metric === null) return super.toString();\n"));
        // The generated C reads each payload through the FFI's view of its class.
        let c = &files[1].1;
        assert!(files[1].0.ends_with("javascript/native/error_payload.c"));
        assert!(c.contains(
            "    case kafka_common_Error_TOPIC_AUTHORIZATION_FAILED: {\n        \
             const kafka_common_TopicAuthorizationError_t* view = NULL;\n        \
             error_discard(FFI_SET(view, kafka_common_Error_topic_authorization_error_err(e, &ffi_err)));\n"
        ));
        assert!(c.contains(
            "            kafka_Map_t* value = NULL;\n            \
             kafka_common_Error_t* failed = FFI_SET(value, \
             kafka_common_RecordTooLargeError_record_too_large_partitions_err(view, &ffi_err));\n            \
             if (payload_topic_partition_long_map(env, obj, \"recordTooLargePartitions\", value, failed)) return NULL;\n"
        ));
        // A class with its parent's signatures declares no constructor.
        let buffer = find("BufferExhaustedError");
        assert_eq!(buffer.dir, "producer");
        assert!(registry.1.contains(
            "  ['../common/errors/sasl_authentication_error', 'SaslAuthenticationError', \
             'org.apache.kafka.common.errors.SaslAuthenticationException'],"
        ));
        assert!(registry.1.contains("  [-4, 'LOCAL_ILLEGAL_STATE'],"));
        assert!(registry.0.ends_with("javascript/src/internal/error_registry.ts"));
        assert!(registry
            .1
            .contains("export const FFI_NAMES: ReadonlyMap<number, string> = new Map<number, string>([\n"));
    }

    #[test]
    fn test_a_class_with_its_parents_signatures_declares_no_constructor() {
        let (modules, _) = outputs(&repo_root()).unwrap();
        let undeclared: Vec<&str> = modules
            .iter()
            .filter(|m| !m.content.contains("constructor("))
            .map(|m| m.name.as_str())
            .collect();
        assert!(!undeclared.is_empty());
        for m in &modules {
            assert!(m.content.contains(&format!("\nsetForms({}, ", m.name)), "{}", m.name);
        }
    }
}
