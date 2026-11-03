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
//! `lib/`), with Java's `extends` chain, plus `lib/_error_registry.js`
//! (`javascript/CLAUDE.md`, "Errors").
//!
//! A class keeps Java's name with the `Exception` suffix as `Error`, its
//! Javadoc, and its FFI id (`_throwable.setFfiId`). *(Deviation from the Python
//! classes, recorded in each class's doc:)* Java's constructors are not
//! translated: every class takes JavaScript's `(message?, {cause}?)`, and the
//! fields of the classes that carry a payload (`TopicAuthorizationError`'s
//! topics, ...) are not read across the FFI by the addon.

use std::path::Path;

use super::{build_graph, is_root_class, parse_ffi_enum_ids, ClassInfo, KAFKA_EXCEPTION_FQN};
use crate::javascript_gen::{header, js_str, jsdoc, rel_require, GeneratedFile, JsModule, JS_LIB};

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

/// The constructor of a class at the top of a chain (its parent is Java's
/// `RuntimeException` / `Exception`): it declares the fields every client
/// error carries.
fn root_body(info: &ClassInfo) -> String {
    let name = &info.py_name;
    format!(
        r#"class {name} extends Error {{
  /**
   * The `kafka_common_Error_e` value of the error: for an error the client
   * reports, the core's; for one constructed here, its class's (`-1`,
   * `UNKNOWN_SERVER_ERROR`, for a class without one).
   * @readonly
   * @type {{number}}
   */
  code;

  /**
   * The `kafka_common_Error_e` enumerator name of `code`
   * (`SASL_AUTHENTICATION_FAILED`, `LOCAL_ILLEGAL_STATE`, ...); `null` for a
   * class the core has no value for.
   * @readonly
   * @type {{string | null}}
   */
  errorName;

  /**
   * Java's `instanceof RetriableException`.
   * @readonly
   * @type {{boolean}}
   */
  retriable;

  /**
   * Java's `instanceof ApiException`.
   * @readonly
   * @type {{boolean}}
   */
  apiError;

  /**
   * Java's constructors as JavaScript's `Error`'s: `message` is Java's
   * message (`null` reads as `''`), `options.cause` its cause. Given a cause
   * and no message, the message is the cause's `String()`, as Java's
   * `(Throwable cause)` constructor takes `cause.toString()`.
   *
   * @param {{string | null}} [message]
   * @param {{{{cause?: unknown}}}} [options]
   */
  constructor(message, options) {{
    super(_throwable.messageOf(message, options), _throwable.optionsOf(options));
    _throwable.init(this, new.target);
    /**
     * The error's cause, Java's `getCause()`: set by `super()` (`Error`'s
     * `options.cause`), declared here for the targets whose `lib` predates
     * ES2022's `Error.cause`. Not a class field, which would overwrite it.
     * @readonly
     * @type {{unknown}}
     */
    this.cause;
  }}
}}"#
    )
}

fn abstract_body(info: &ClassInfo, parent: &str) -> String {
    let name = &info.py_name;
    format!(
        r#"class {name} extends {parent} {{
  /**
   * A catch-only base: constructing it directly throws `TypeError`.
   *
   * @param {{string | null}} [message]
   * @param {{{{cause?: unknown}}}} [options]
   */
  constructor(message, options) {{
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
    let needs_throwable = root || info.ffi_id.is_some();
    if needs_throwable {
        text.push_str(&format!("const _throwable = require('{}');\n", rel_require(&dir, "_throwable")));
    }
    let parent_name = if root {
        None
    } else {
        let parent = g
            .get(&info.parent_fqn)
            .ok_or_else(|| anyhow::anyhow!("javascript errors: `{}` has no parent class", info.java_fqn))?;
        let from = rel_require(&dir, &js_path(parent));
        text.push_str(&format!("const {{ {} }} = require('{from}');\n", parent.py_name));
        Some(parent.py_name.clone())
    };
    text.push('\n');
    text.push_str(&jsdoc(&class_doc(info), ""));
    text.push('\n');
    match (&parent_name, info.is_abstract) {
        (None, false) => text.push_str(&root_body(info)),
        (None, true) => anyhow::bail!("javascript errors: the abstract `{}` has no Kafka parent", info.java_fqn),
        (Some(p), true) => text.push_str(&abstract_body(info, p)),
        (Some(p), false) => text.push_str(&format!("class {} extends {p} {{}}", info.py_name)),
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
            "_throwable.setFfiId({}, {value}); // kafka_common_Error_{constant}\n",
            info.py_name
        ));
    }
    text.push_str(&format!("\nmodule.exports = {{ {} }};\n", info.py_name));
    Ok(JsModule { dir, stem: info.file_stem(), name: info.py_name.clone(), content: text })
}

/// The class modules and `lib/_error_registry.js`.
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
    registry.push_str("const ERRORS = Object.freeze([\n");
    for (_, row) in &rows {
        registry.push_str(row);
        registry.push('\n');
    }
    registry.push_str("]);\n\n/** @type {ReadonlyMap<number, string>} */\nconst FFI_NAMES = new Map([\n");
    for n in &names {
        registry.push_str(n);
        registry.push('\n');
    }
    registry.push_str("]);\n\nmodule.exports = { ERRORS, FFI_NAMES };\n");
    let path = repo_root.join(JS_LIB).join("_error_registry.js");
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
            .contains("class SaslAuthenticationError extends AuthenticationError {}"));
        assert!(sasl
            .content
            .contains("const { AuthenticationError } = require('./authentication_error');"));
        assert!(sasl.content.contains("_throwable.setFfiId(SaslAuthenticationError, 58);"));
        let kafka = find("KafkaError");
        assert_eq!(kafka.dir, "common");
        assert!(kafka.content.contains("class KafkaError extends Error {"));
        assert!(kafka.content.contains("const _throwable = require('../_throwable');"));
        let state = find("IllegalStateError");
        assert_eq!(state.dir, "");
        assert!(state.content.contains("_throwable.setFfiId(IllegalStateError, -4);"));
        let retriable = find("RetriableError");
        assert!(retriable.content.contains("if (new.target === RetriableError) {"));
        assert!(!retriable.content.contains("setFfiId"));
        let api = find("ApiError");
        assert!(api.content.contains("const { KafkaError } = require('../kafka_error');"));
        let buffer = find("BufferExhaustedError");
        assert_eq!(buffer.dir, "producer");
        assert!(registry.1.contains(
            "  ['./common/errors/sasl_authentication_error', 'SaslAuthenticationError', \
             'org.apache.kafka.common.errors.SaslAuthenticationException'],"
        ));
        assert!(registry.1.contains("  [-4, 'LOCAL_ILLEGAL_STATE'],"));
    }
}
