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

//! The Node.js client's config classes: the constants of the Python ones
//! ([`super::SPECS`]) that a producer reaches, each a class of static
//! properties in the lib/ folder of its Java package.

use std::fs;
use std::path::Path;

use super::{constants_of, simple, Kind, Reader, Value, DEPRECATED_LIST, SPECS};
use crate::javascript_gen::{header, js_str, tsdoc, JsModule};

type Res<T> = anyhow::Result<T>;

/// The config classes of the Node.js client: Java class and lib/ folder. The
/// others the Python client generates (`ConsumerConfig`, `AdminClientConfig`,
/// `TopicConfig`, `LogLevelConfig`) wait for a consumer and an admin client.
const JS_SPECS: &[(&str, &str)] = &[
    ("org.apache.kafka.clients.producer.ProducerConfig", "producer"),
    ("org.apache.kafka.common.config.SaslConfigs", "common/config"),
    ("org.apache.kafka.common.config.SslConfigs", "common/config"),
    ("org.apache.kafka.common.config.SecurityConfig", "common/config"),
];

/// Translation notes on single constants: Java's value is kept, and the note
/// says what the Rust core does instead.
const NOTES: &[(&str, &str, &str)] = &[
    (
        "org.apache.kafka.common.config.SslConfigs",
        "DEFAULT_SSL_KEYSTORE_TYPE",
        "Translation note: Java's value. The Rust core's default `ssl.keystore.type` is `PEM`, \
         the only key store format it reads (no JKS or PKCS12).",
    ),
    (
        "org.apache.kafka.common.config.SslConfigs",
        "DEFAULT_SSL_TRUSTSTORE_TYPE",
        "Translation note: Java's value. The Rust core's default `ssl.truststore.type` is `PEM`, \
         the only trust store format it reads (no JKS or PKCS12).",
    ),
];

/// A constant's TypeScript value and its type.
fn js_value(v: &Value) -> Res<(String, String)> {
    Ok(match v {
        Value::Str(s) => (js_str(s), "string".into()),
        Value::Int(n) => {
            if n.unsigned_abs() > (1u64 << 53) {
                anyhow::bail!("config constants: {n} does not fit a JavaScript number");
            }
            (n.to_string(), "number".into())
        },
        Value::Double(d) => (format!("{d:?}"), "number".into()),
        Value::Bool(b) => (b.to_string(), "boolean".into()),
        Value::Null => ("null".into(), "null".into()),
        // A constant `List` or `Set` is a frozen array, in Java's order.
        Value::List(items) | Value::Set(items) => {
            let mut parts = Vec::new();
            let mut ty: Option<String> = None;
            for i in items {
                let (p, t) = js_value(i)?;
                if ty.as_ref().is_some_and(|x| *x != t) {
                    anyhow::bail!("config constants: a collection of mixed types {items:?}");
                }
                ty = Some(t);
                parts.push(p);
            }
            let ty = ty.unwrap_or_else(|| "string".into());
            (format!("Object.freeze([{}])", parts.join(", ")), format!("ReadonlyArray<{ty}>"))
        },
        other => anyhow::bail!("config constants: no JavaScript form for {other:?}"),
    })
}

/// The generated config-class modules.
pub(crate) fn modules(repo_root: &Path) -> Res<Vec<JsModule>> {
    let deprecated: Vec<String> = fs::read_to_string(repo_root.join(DEPRECATED_LIST))
        .map_err(|e| anyhow::anyhow!("{DEPRECATED_LIST}: {e}"))?
        .lines()
        .filter(|l| !l.starts_with('#') && !l.trim().is_empty())
        .map(|l| l.trim().to_string())
        .collect();
    let mut reader = Reader::new(repo_root);
    let mut out = Vec::new();
    for (java, dir) in JS_SPECS {
        let spec = SPECS
            .iter()
            .find(|s| s.java == *java)
            .ok_or_else(|| anyhow::anyhow!("config constants: {java} is not a Python config class"))?;
        let name = simple(spec.java);
        let class = reader.class(spec.java)?;
        let mut consts = constants_of(&mut reader, spec.java, &deprecated)?;
        for parent in spec.flatten {
            for (n, v) in constants_of(&mut reader, parent, &deprecated)? {
                if !consts.iter().any(|(m, _)| *m == n) {
                    consts.push((n, v));
                }
            }
        }
        let mut skipped: Vec<String> = class
            .public_methods
            .iter()
            .filter(|m| !m.starts_with("toString("))
            .map(|m| format!("`{m}`"))
            .collect();
        skipped.dedup();
        let mut about = format!(
            "The constants of Java's `{}`: every `public static final` field, evaluated from the Java \
             source, is a static property of the same name; a deprecated one is left out. A `List` or \
             `Set` constant is a frozen array in Java's order.",
            spec.java
        );
        if !spec.flatten.is_empty() {
            about.push_str(
                " The constants Java's class inherits from `AbstractConfig` (`CONFIG_PROVIDERS_CONFIG`, ...) \
                 are properties of the class too.",
            );
        }
        if spec.kind == Kind::ConstantsOnly {
            about.push_str(&format!(
                " The `ConfigDef` machinery is not translated (a configuration is a plain object of these \
                 keys, and the `ConfigDef` types are the types the client checks), so `{name}` is not \
                 instantiable: constructing it throws `TypeError`."
            ));
        }
        if !skipped.is_empty() {
            about.push_str(&format!(
                " Not generated: {}, the `ConfigDef` machinery or methods that take a type the client \
                 does not translate (`ConfigDef`, `AbstractConfig`).",
                skipped.join(", ")
            ));
        }
        about.push_str(&format!("\n\nJava: `{}`.", spec.java));
        let mut text = header(&format!("Java's `{}`", spec.java));
        text.push('\n');
        text.push_str(&tsdoc(&about, ""));
        text.push_str(&format!("\nexport class {name} {{\n"));
        for (n, v) in &consts {
            let (val, ty) = js_value(v)?;
            let note = NOTES.iter().find(|(c, k, _)| c == java && k == n).map(|(_, _, t)| *t);
            if let Some(t) = note {
                text.push_str(&tsdoc(t, "  "));
                text.push('\n');
            }
            // The type is written out: a `static readonly` with a literal
            // initializer would otherwise be typed as that literal.
            text.push_str(&format!("  static readonly {n}: {ty} = {val};\n"));
        }
        if spec.kind == Kind::ConstantsOnly {
            text.push_str(&format!(
                "\n  /**\n   * Not instantiable: throws `TypeError`.\n   */\n  \
                 constructor(...args: unknown[]) {{\n    void args;\n    throw new TypeError('{name} is not instantiable: its ConfigDef machinery is not ' +\n      \
                 'translated; a configuration is a plain object of its keys');\n  }}\n"
            ));
        }
        for (c, k, _) in NOTES {
            if c == java && !consts.iter().any(|(n, _)| n == k) {
                anyhow::bail!("config constants: the note on `{c}.{k}` names no constant");
            }
        }
        text.push_str("}\n");
        let stem = crate::error_hierarchy::snake(name);
        out.push(JsModule { dir: (*dir).to_string(), stem, name: name.to_string(), content: text });
    }
    Ok(out)
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
    fn test_config_classes() {
        let modules = modules(&repo_root()).unwrap();
        let ssl = modules.iter().find(|m| m.name == "SslConfigs").unwrap();
        assert_eq!((ssl.dir.as_str(), ssl.stem.as_str()), ("common/config", "ssl_configs"));
        assert!(ssl.content.contains("export class SslConfigs {\n"));
        assert!(ssl
            .content
            .contains("  static readonly SSL_PROTOCOL_CONFIG: string = 'ssl.protocol';\n"));
        assert!(ssl
            .content
            .contains("  static readonly DEFAULT_SSL_KEYSTORE_TYPE: string = 'JKS';\n"));
        assert!(!ssl.content.contains("@type"));
        assert!(!ssl.content.contains("module.exports"));
        assert!(ssl
            .content
            .contains("   * Translation note: Java's value. The Rust core's default `ssl.keystore.type`\n"));
        assert!(ssl.content.contains(
            "  static readonly RECONFIGURABLE_CONFIGS: ReadonlyArray<string> = Object.freeze(['ssl.keystore.type',"
        ));
        let producer = modules.iter().find(|m| m.name == "ProducerConfig").unwrap();
        assert_eq!((producer.dir.as_str(), producer.stem.as_str()), ("producer", "producer_config"));
        assert!(producer
            .content
            .contains("  static readonly CONFIG_PROVIDERS_CONFIG: string = 'config.providers';\n"));
        assert!(!producer.content.contains("PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG"));
        assert!(producer
            .content
            .contains("throw new TypeError('ProducerConfig is not instantiable"));
        assert!(producer.content.contains("  constructor(...args: unknown[]) {\n"));
        let sasl = modules.iter().find(|m| m.name == "SaslConfigs").unwrap();
        assert!(sasl
            .content
            .contains("  static readonly DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR: number = 0.8;\n"));
    }
}
