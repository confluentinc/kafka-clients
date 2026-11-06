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
//! ([`super::build_graph`]), each in the lib/ folder of its Java package
//! (`org.apache.kafka.common.errors` -> `lib/common/errors/`, `java.*` ->
//! `lib/`), with Java's `extends` chain, plus `lib/_error_registry.ts`
//! (`javascript/CLAUDE.md`, "Errors").
//!
//! A class keeps Java's name with the `Exception` suffix as `Error`, its
//! Javadoc, and its FFI id (`_throwable.setFfiId`). *(Deviation from the Python
//! classes, recorded in `javascript/CLAUDE.md` ("Java shape, JS idiom") and the
//! CHANGELOG's known limitations, not in each class's doc:)* Java's
//! constructors are not translated: every class takes JavaScript's
//! `(message?, {cause}?)`; the payload getters (`TopicAuthorizationError`'s
//! topics, ...), the `INSTANCE` singletons and
//! `RecordDeserializationError.DeserializationExceptionOrigin` are not
//! generated, and the addon reads no payload across the FFI.

use std::path::Path;

use super::{build_graph, is_root_class, parse_ffi_enum_ids, ClassInfo, KAFKA_EXCEPTION_FQN};
use crate::javascript_gen::{header, js_str, rel_import, tsdoc, GeneratedFile, JsModule, JS_LIB};

type Res<T> = anyhow::Result<T>;

/// The lib/ folder of a Python module (`confluent_kafka.common.errors` ->
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

fn class_doc(info: &ClassInfo) -> String {
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
    }
    parts.join("\n\n")
}

/// The class at the top of a chain (its parent is Java's `RuntimeException` /
/// `Exception`): it declares the fields every client error carries.
fn root_body(info: &ClassInfo) -> String {
    let name = &info.py_name;
    format!(
        r#"export class {name} extends Error {{
  /**
   * The `kafka_common_Error_e` value of the error: for an error the client
   * reports, the core's; for one constructed here, its class's (`-1`,
   * `UNKNOWN_SERVER_ERROR`, for a class without one).
   */
  readonly code!: number;

  /**
   * The `kafka_common_Error_e` enumerator name of `code`
   * (`SASL_AUTHENTICATION_FAILED`, `LOCAL_ILLEGAL_STATE`, ...); `null` for a
   * class the core has no value for.
   */
  readonly errorName!: string | null;

  /**
   * Java's `instanceof RetriableException`.
   */
  readonly retriable!: boolean;

  /**
   * Java's `instanceof ApiException`.
   */
  readonly apiError!: boolean;

  /**
   * The error's cause, Java's `getCause()`: set by `super()` (`Error`'s
   * `options.cause`), declared here for the targets whose `lib` predates
   * ES2022's `Error.cause`. A `declare` field, which emits nothing: a class
   * field would overwrite it.
   */
  declare readonly cause: unknown;

  /**
   * Java's constructors as JavaScript's `Error`'s: `message` is Java's
   * message (`null` reads as `''`), `options.cause` its cause. Given a cause
   * and no message, the message is the cause's `String()`, as Java's
   * `(Throwable cause)` constructor takes `cause.toString()`.
   *
   * @param message - Java's message; `null` reads as `''`
   * @param options - `cause`: Java's cause
   */
  constructor(message?: string | null, options?: {{ cause?: unknown }}) {{
    super(messageOf(message, options), optionsOf(options));
    init(this, new.target);
  }}
}}"#
    )
}

fn abstract_body(info: &ClassInfo, parent: &str) -> String {
    let name = &info.py_name;
    format!(
        r#"export class {name} extends {parent} {{
  /**
   * A catch-only base: constructing it directly throws `TypeError`.
   *
   * @param message - the message a subclass passes on, Java's message
   * @param options - `cause`: the cause a subclass passes on, Java's cause
   * @throws `TypeError` when constructed directly
   */
  constructor(message?: string | null, options?: {{ cause?: unknown }}) {{
    if (new.target === {name}) {{
      throw new TypeError('{name} is an abstract catch-only base; it is never thrown directly');
    }}
    super(message, options);
  }}
}}"#
    )
}

fn render(g: &std::collections::BTreeMap<String, ClassInfo>, info: &ClassInfo) -> Res<JsModule> {
    let dir = js_dir(&info.py_module);
    let mut text = header(&format!("the Java source of `{}`", info.java_fqn));
    text.push('\n');
    let root = is_root_class(info);
    // What the class takes from `_throwable`: a root class its constructor's
    // helpers, a class with an FFI id `setFfiId`.
    let mut helpers: Vec<&str> = Vec::new();
    if root {
        helpers.extend(["init", "messageOf", "optionsOf"]);
    }
    if info.ffi_id.is_some() {
        helpers.push("setFfiId");
    }
    if !helpers.is_empty() {
        text.push_str(&format!(
            "import {{ {} }} from '{}';\n",
            helpers.join(", "),
            rel_import(&dir, "_throwable")
        ));
    }
    let parent_name = if root {
        None
    } else {
        let parent = g
            .get(&info.parent_fqn)
            .ok_or_else(|| anyhow::anyhow!("javascript errors: `{}` has no parent class", info.java_fqn))?;
        let from = rel_import(&dir, &js_path(parent));
        text.push_str(&format!("import {{ {} }} from '{from}';\n", parent.py_name));
        Some(parent.py_name.clone())
    };
    text.push('\n');
    text.push_str(&tsdoc(&class_doc(info), ""));
    text.push('\n');
    match (&parent_name, info.is_abstract) {
        (None, false) => text.push_str(&root_body(info)),
        (None, true) => anyhow::bail!("javascript errors: the abstract `{}` has no Kafka parent", info.java_fqn),
        (Some(p), true) => text.push_str(&abstract_body(info, p)),
        (Some(p), false) => text.push_str(&format!("export class {} extends {p} {{}}", info.py_name)),
    }
    text.push('\n');
    if let Some((constant, value)) = &info.ffi_id {
        if info.java_fqn == KAFKA_EXCEPTION_FQN {
            text.push_str(
                "\n// A bare KafkaException is reported as UNKNOWN_SERVER_ERROR; UnknownServerError\n// owns that id in the id -> class table.\n",
            );
        } else {
            text.push('\n');
        }
        text.push_str(&format!(
            "setFfiId({}, {value}); // kafka_common_Error_{constant}\n",
            info.py_name
        ));
    }
    Ok(JsModule { dir, stem: info.file_stem(), name: info.py_name.clone(), content: text })
}

/// The class modules and `lib/_error_registry.ts`.
pub(crate) fn outputs(repo_root: &Path) -> Res<(Vec<JsModule>, GeneratedFile)> {
    let classes = build_graph(repo_root)?;
    let mut modules = Vec::new();
    for info in classes.values() {
        modules.push(render(&classes, info)?);
    }
    let mut rows: Vec<(String, String)> = classes
        .values()
        .map(|c| {
            (
                c.module_file(),
                format!(
                    "  [{}, {}, {}],",
                    js_str(&format!("./{}", js_path(c))),
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
        "\n// Every generated error class, as [module (relative to lib/), class name, Java\n\
         // class] (python/confluent_kafka/_error_registry.py's ERRORS), and the\n\
         // enumerator name of every kafka_common_Error_e value (private).\n\n",
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
    let path = repo_root.join(JS_LIB).join("_error_registry.ts");
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
    fn test_classes_keep_the_java_chain() {
        let (modules, registry) = outputs(&repo_root()).unwrap();
        let find = |name: &str| modules.iter().find(|m| m.name == name).unwrap();
        let sasl = find("SaslAuthenticationError");
        assert_eq!(sasl.dir, "common/errors");
        assert!(sasl
            .content
            .contains("export class SaslAuthenticationError extends AuthenticationError {}"));
        assert!(sasl
            .content
            .contains("import { AuthenticationError } from './authentication_error';"));
        assert!(!sasl.content.contains("require("));
        assert!(!sasl.content.contains("module.exports"));
        assert!(sasl.content.contains("import { setFfiId } from '../../_throwable';"));
        assert!(sasl.content.contains("\nsetFfiId(SaslAuthenticationError, 58);"));
        let kafka = find("KafkaError");
        assert_eq!(kafka.dir, "common");
        assert!(kafka.content.contains("export class KafkaError extends Error {"));
        assert!(kafka
            .content
            .contains("import { init, messageOf, optionsOf, setFfiId } from '../_throwable';"));
        assert!(kafka.content.contains("  readonly code!: number;\n"));
        assert!(kafka.content.contains("  readonly errorName!: string | null;\n"));
        assert!(kafka.content.contains("  declare readonly cause: unknown;\n"));
        assert!(kafka
            .content
            .contains("  constructor(message?: string | null, options?: { cause?: unknown }) {\n"));
        let state = find("IllegalStateError");
        assert_eq!(state.dir, "");
        assert!(state.content.contains("\nsetFfiId(IllegalStateError, -4);"));
        let retriable = find("RetriableError");
        assert!(retriable.content.contains("if (new.target === RetriableError) {"));
        assert!(!retriable.content.contains("setFfiId"));
        let api = find("ApiError");
        assert!(api.content.contains("import { KafkaError } from '../kafka_error';"));
        let buffer = find("BufferExhaustedError");
        assert_eq!(buffer.dir, "producer");
        assert!(registry.1.contains(
            "  ['./common/errors/sasl_authentication_error', 'SaslAuthenticationError', \
             'org.apache.kafka.common.errors.SaslAuthenticationException'],"
        ));
        assert!(registry.1.contains("  [-4, 'LOCAL_ILLEGAL_STATE'],"));
        assert!(registry.0.ends_with("javascript/lib/_error_registry.ts"));
        assert!(registry
            .1
            .contains("export const FFI_NAMES: ReadonlyMap<number, string> = new Map<number, string>([\n"));
    }
}
