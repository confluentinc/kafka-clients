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
//! Java's constructor chain gives (`super::build_model`). *(Deviation from the
//! Python classes, recorded in `javascript/CLAUDE.md` and in each module's
//! header comment:)* the constructors that take a payload are not generated
//! (they come with the payload getters), nor is
//! `RecordDeserializationError.DeserializationExceptionOrigin`. Java's
//! `public static final` instances (`DisconnectException.INSTANCE`) are
//! static properties of their class, built by Java's no-argument chain
//! whatever its constructors' visibility, not writable, as Java's `final`
//! field; a class with
//! no other constructor cannot be constructed (a `protected` constructor that
//! throws `TypeError`), and the addon reads no payload across the FFI.

use std::collections::BTreeMap;
use std::path::Path;

use super::{
    build_graph, build_model, is_root_class, jdk_class, parse_ffi_enum_ids, python_name, ClassInfo, Graph, JdkCtor,
    PyForm, KAFKA_EXCEPTION_FQN,
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
}

/// One generated constructor form: its parameters (`message`, `cause`) and its message.
#[derive(Clone, Debug, PartialEq)]
struct JsForm {
    params: Vec<String>,
    message: Message,
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
    Ok(Some(JsForm { params: form.params.clone(), message }))
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

/// Every class's generated forms and left-out constructors, by Java FQN.
type Forms = BTreeMap<String, (Vec<JsForm>, Vec<String>)>;

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
    }
}

/// The signatures of a form: the options object, and the bare value of a
/// one-parameter form. `bare_cause` is the type a bare cause has.
fn form_sigs(form: &JsForm, bare_cause: &str) -> Vec<Sig> {
    let java = |p: &str| {
        if p == "message" {
            "String message"
        } else {
            "Throwable cause"
        }
    };
    let java_sig: Vec<&str> = form.params.iter().map(|p| java(p)).collect();
    let summary = if java_sig.is_empty() {
        format!("Java's no-argument constructor.{}", message_doc(&form.message))
    } else {
        format!("Java's `({})` constructor.{}", java_sig.join(", "), message_doc(&form.message))
    };
    let field = |p: &str| {
        if p == "message" {
            ("message: string | null", "The message; `null` reads as `''`.", "message")
        } else {
            ("cause: unknown", "The cause (`Error`'s `cause`).", "cause")
        }
    };
    if form.params.is_empty() {
        return vec![Sig { doc: summary, params: String::new(), tuple: "[]".into() }];
    }
    let fields: Vec<(&str, &str, &str)> = form.params.iter().map(|p| field(p)).collect();
    // `name?: never` closes the options type against an `Error`, which has a
    // string `message` (and, from ES2022, a `cause`) and would otherwise match
    // it structurally: `new ConfigError(err)` is refused at compile time, as
    // Java refuses `new ConfigException(throwable)`. Every `Error` has a `name`;
    // no form takes one. Each key is documented on the options type.
    let plain = format!(
        "{{ {}; name?: never }}",
        fields.iter().map(|f| f.0).collect::<Vec<_>>().join("; ")
    );
    let documented = format!(
        "{{ {}; name?: never }}",
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
    if let [p] = form.params.as_slice() {
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
            };
            format!("  [[{}], {message}],", params.join(", "))
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
    if is_root_class(info) || info.is_abstract || own[&info.java_fqn].0.is_empty() {
        return true;
    }
    effective_sigs(own, &info.java_fqn) != effective_sigs(own, &info.parent_fqn)
}

fn render(g: &BTreeMap<String, ClassInfo>, own: &Forms, info: &ClassInfo) -> Res<JsModule> {
    let dir = js_dir(&info.py_module);
    let (forms, left_out) = &own[&info.java_fqn];
    let name = &info.py_name;
    let mut text = header(&format!("the Java source of `{}`", info.java_fqn));
    if !left_out.is_empty() {
        let note = format!(
            "Not generated: Java's constructors that take a payload, which come with the payload getters: {}.",
            left_out.join(", ")
        );
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
    text.push('\n');
    let root = is_root_class(info);
    let mut helpers: Vec<&str> = Vec::new();
    if root {
        helpers.extend(["errorArgs", "init"]);
    }
    helpers.push("setForms");
    if !info.java.singletons.is_empty() {
        helpers.push("create");
    }
    helpers.sort_unstable();
    text.push_str(&format!(
        "import {{ {} }} from '{}';\n",
        helpers.join(", "),
        rel_import(&dir, "internal/throwable")
    ));
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
        text.push_str(&format!("import {{ {} }} from '{from}';\n", parent.py_name));
        parent.py_name.clone()
    };
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
    if declares_constructor(own, info) {
        let sigs = class_sigs(forms);
        let mut ctor = Vec::new();
        if sigs.is_empty() {
            ctor.push(tsdoc(
                "Not constructible yet: Java's constructors take a payload the client does not translate yet.\n\n\
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
            ctor.push(format!("    if (new.target === {name}) {{"));
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
        ctor.push("  }".into());
        body.push(ctor.join("\n"));
    }
    if body.is_empty() {
        text.push_str(&format!("{kw} {name} extends {parent} {{}}\n"));
    } else {
        text.push_str(&format!("{kw} {name} extends {parent} {{\n{}\n}}\n", body.join("\n\n")));
    }
    text.push_str(&format!("\nsetForms({name}, {});\n", forms_literal(forms)));
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
    Ok(JsModule { dir, stem: info.file_stem(), name: info.py_name.clone(), content: text })
}

/// The class modules and `src/internal/error_registry.ts`.
pub(crate) fn outputs(repo_root: &Path) -> Res<(Vec<JsModule>, GeneratedFile)> {
    let classes = js_graph(repo_root)?;
    let graph = Graph { classes: &classes };
    let mut own = BTreeMap::new();
    for (fqn, info) in &classes {
        own.insert(fqn.clone(), class_forms(&graph, info)?);
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
    Ok((modules, (path, registry)))
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
        let (modules, registry) = outputs(&repo_root()).unwrap();
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
        assert!(retriable.content.contains("if (new.target === RetriableError) {"));
        assert!(!retriable.content.contains("_ffiId"));
        assert!(retriable.content.contains("\nsetForms(RetriableError, ["));
        let api = find("ApiError");
        assert!(api.content.contains("import { KafkaError } from '../kafka_error';"));
        // The payload constructors are left out and listed in the module's header.
        let topic = find("TopicAuthorizationError");
        assert!(topic.content.contains(
            "// Not generated: Java's constructors that take a payload, which come with the\n\
             // payload getters: `TopicAuthorizationException(String, Set<String>)`,\n\
             // `TopicAuthorizationException(Set<String>)`.\n"
        ));
        assert!(topic
            .content
            .contains("setForms(TopicAuthorizationError, [\n  [['message'], 'message'],\n]);"));
        // A subclass with forms of its own passes its arguments on.
        assert!(topic
            .content
            .contains("    super(...(args as [{ message: string | null; name?: never }]));\n"));
        // A fixed Java message is kept as Java's text.
        let commit = find("RetriableCommitFailedError");
        assert!(commit.content.contains(
            "  [['cause'], { text: 'Offset commit failed with a retriable exception. You should retry committing the latest consumed offsets.' }],\n"
        ));
        // A class whose every constructor takes a payload cannot be constructed yet.
        let quota = find("QuotaViolationError");
        assert!(quota.content.contains("  protected constructor(...args: unknown[]) {\n"));
        assert!(quota.content.contains("\nsetForms(QuotaViolationError, []);\n"));
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
