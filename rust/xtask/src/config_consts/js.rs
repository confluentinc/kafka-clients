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
//! properties in the src/ folder of its Java package.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use super::{constants_of, simple, Kind, Reader, Value, DEPRECATED_LIST, JAVA_ROOT, SPECS};
use crate::javascript_gen::{header, js_str, tsdoc, JsModule};

type Res<T> = anyhow::Result<T>;

/// The producer's Java config class, whose `ConfigDef` documents its keys.
const PRODUCER_CONFIG: &str = "org.apache.kafka.clients.producer.ProducerConfig";

/// The config classes of the Node.js client: Java class and src/ folder. The
/// others the Python client generates (`ConsumerConfig`, `AdminClientConfig`,
/// `TopicConfig`, `LogLevelConfig`) wait for a consumer and an admin client.
const JS_SPECS: &[(&str, &str)] = &[
    (PRODUCER_CONFIG, "producer"),
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

/// The TSDoc text of constant `name` (value `value`) of Java class `owner`
/// (PLAN.md §8 decision 17, as amended), Java's own documentation of it,
/// translated as a Javadoc is:
/// 1. the constant's own Javadoc (`javadoc`), when Java documents it, so the
///    TSDoc stays within 150% of it, as Rust's constants do (check-doc-size);
/// 2. else, for a configuration key, the documentation argument of the
///    `define` call of `ProducerConfig`'s `ConfigDef` for that key (`docs`),
///    which may be another class's `_DOC`
///    (`CommonClientConfigs.BOOTSTRAP_SERVERS_DOC`);
/// 3. else the sibling `<NAME>_DOC` or, for an `X_CONFIG`, `X_DOC` (any
///    visibility) of `owner`;
/// 4. else a line naming it: the key of an `X_CONFIG`, the constant an
///    `X_DOC` documents, or Java's constant itself.
fn constant_doc(
    reader: &mut Reader,
    docs: &BTreeMap<String, (String, String)>,
    owner: &str,
    name: &str,
    value: &Value,
    javadoc: Option<&str>,
) -> Res<String> {
    // The docs are read as Markdown (TypeDoc), so they keep Java's links and
    // nested lists.
    let translator = crate::javadoc::Translator { index: None, panics: false, markdown: true };
    if let Some(own) = javadoc.map(|d| translator.to_rustdoc(d)).filter(|d| !d.trim().is_empty()) {
        return Ok(own);
    }
    if let Value::Str(key) = value {
        let is_key_constant = !name.ends_with("_DOC") && !name.starts_with("DEFAULT_");
        if let Some((class, src)) = docs.get(key).filter(|_| is_key_constant) {
            match reader
                .eval_src(class, src)
                .map_err(|e| anyhow::anyhow!("the doc of `{key}` in `{class}`: {e}"))?
            {
                Value::Str(text) => return Ok(translator.to_rustdoc(&text)),
                other => anyhow::bail!("config constants: the doc of `{key}` in `{class}` is not a string: {other:?}"),
            }
        }
    }
    let class = reader.class(owner)?;
    let stem = name.strip_suffix("_CONFIG");
    let siblings = [Some(format!("{name}_DOC")), stem.map(|s| format!("{s}_DOC"))];
    for sibling in siblings.iter().flatten() {
        let Some(field) = class.fields.iter().find(|f| f.name == *sibling && f.static_final) else {
            continue;
        };
        let Some(init) = field.init.clone() else {
            continue;
        };
        let doc = reader
            .eval_src(owner, &init)
            .map_err(|e| anyhow::anyhow!("`{owner}.{sibling}`: {e}"))?;
        if let Value::Str(text) = doc {
            return Ok(translator.to_rustdoc(&text));
        }
    }
    let java = simple(owner);
    if let (Some(_), Value::Str(key)) = (stem, value) {
        return Ok(format!("The `{key}` configuration key."));
    }
    if let Some(stem) = name.strip_suffix("_DOC") {
        let config = format!("{stem}_CONFIG");
        let target = if class.fields.iter().any(|f| f.name == config) {
            config
        } else {
            stem.to_string()
        };
        return Ok(format!("The documentation of `{java}.{target}`."));
    }
    Ok(format!("Java's `{java}.{name}`."))
}

/// A constant's TypeScript value and its type: a `long` constant (`long`
/// true) is a `bigint`, as the JS conventions' Types table maps Java's long.
fn js_value(v: &Value, long: bool) -> Res<(String, String)> {
    Ok(match v {
        Value::Str(s) => (js_str(s), "string".into()),
        Value::Int(n) if long => (format!("{n}n"), "bigint".into()),
        Value::Int(n) => {
            if n.unsigned_abs() > (1u64 << 53) {
                anyhow::bail!("config constants: {n} does not fit a JavaScript number");
            }
            (n.to_string(), "number".into())
        },
        Value::Double(d) => (format!("{d:?}"), "number".into()),
        Value::Bool(b) => (b.to_string(), "boolean".into()),
        Value::Null => ("null".into(), "null".into()),
        // A constant `List` is a frozen array, and a constant `Set` a read-only
        // set whose `add`, `delete` and `clear` throw, both in Java's order.
        Value::List(items) | Value::Set(items) => {
            let mut parts = Vec::new();
            let mut ty: Option<String> = None;
            for i in items {
                let (p, t) = js_value(i, false)?;
                if ty.as_ref().is_some_and(|x| *x != t) {
                    anyhow::bail!("config constants: a collection of mixed types {items:?}");
                }
                ty = Some(t);
                parts.push(p);
            }
            let ty = ty.unwrap_or_else(|| "string".into());
            if matches!(v, Value::Set(_)) {
                (format!("readOnlySet([{}])", parts.join(", ")), format!("ReadonlySet<{ty}>"))
            } else {
                (format!("Object.freeze([{}])", parts.join(", ")), format!("ReadonlyArray<{ty}>"))
            }
        },
        other => anyhow::bail!("config constants: no JavaScript form for {other:?}"),
    })
}

/// Top-level Java class `fqn` as the Java scanner reads it, its Javadoc and
/// its fields' Javadocs included; `None` when its source declares no such
/// class.
fn scanned_class(repo_root: &Path, fqn: &str) -> Res<Option<crate::java::JavaClass>> {
    let path = repo_root.join(JAVA_ROOT).join(format!("{}.java", fqn.replace('.', "/")));
    let source = fs::read_to_string(&path).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
    let below = fqn
        .strip_prefix(crate::java::MARKER_PREFIX)
        .ok_or_else(|| anyhow::anyhow!("config constants: {fqn} is not below org.apache.kafka"))?;
    let parts: Vec<String> = below.split('.').map(str::to_string).collect();
    let (name, package) = parts
        .split_last()
        .ok_or_else(|| anyhow::anyhow!("config constants: {fqn} has no class"))?;
    let mut classes = Vec::new();
    crate::java::push_classes(package, &source, false, &mut classes);
    Ok(classes.into_iter().find(|c| c.path == [name.clone()]))
}

/// The Javadoc of top-level Java class `fqn`, translated for TypeDoc; `None`
/// when it has none.
fn class_javadoc(repo_root: &Path, fqn: &str) -> Res<Option<String>> {
    let translator = crate::javadoc::Translator { index: None, panics: false, markdown: true };
    Ok(scanned_class(repo_root, fqn)?
        .and_then(|c| c.javadoc)
        .map(|doc| translator.to_rustdoc(&doc))
        .filter(|doc| !doc.trim().is_empty()))
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
    // The documentation of every key a producer's configuration takes.
    let docs = crate::config_types::define_docs(repo_root, PRODUCER_CONFIG)?;
    // The Javadoc of each constant Java documents, by the class declaring it.
    let mut field_docs: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    let mut out = Vec::new();
    for (java, dir) in JS_SPECS {
        let spec = SPECS
            .iter()
            .find(|s| s.java == *java)
            .ok_or_else(|| anyhow::anyhow!("config constants: {java} is not a Python config class"))?;
        let name = simple(spec.java);
        let class = reader.class(spec.java)?;
        // Each constant with the Java class declaring it, where its `_DOC` sibling is.
        let mut consts: Vec<(String, Value, &str)> = constants_of(&mut reader, spec.java, &deprecated)?
            .into_iter()
            .map(|c| (c.name, c.value, spec.java))
            .collect();
        for parent in spec.flatten {
            for c in constants_of(&mut reader, parent, &deprecated)? {
                if !consts.iter().any(|(m, _, _)| *m == c.name) {
                    consts.push((c.name, c.value, parent));
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
        // A class Java documents keeps Java's doc, within 150% of it
        // (check-doc-size): what it leaves out is listed in its package's
        // documentation.
        let javadoc = class_javadoc(repo_root, spec.java)?;
        let mut about = format!(
            "The constants of Java's `{}`: every `public static final` field, evaluated from the Java \
             source, is a static property of the same name; a deprecated one is left out. A `List` \
             constant is a frozen array and a `Set` constant a read-only set whose `add`, `delete` and \
             `clear` throw `UnsupportedOperationError`, both in Java's order.",
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
        if let Some(doc) = javadoc {
            about = doc;
            if spec.kind == Kind::ConstantsOnly {
                about.push_str("\n\n@remarks *(deviation)* Constants only: `new` throws `TypeError`.");
            }
        }
        let mut text = header(&format!("Java's `{}`", spec.java));
        text.push('\n');
        if consts.iter().any(|(_, v, _)| matches!(v, Value::Set(_))) {
            text.push_str(&format!(
                "import {{ readOnlySet }} from '{}';\n\n",
                crate::javascript_gen::rel_import(dir, "internal/read_only_set")
            ));
        }
        text.push_str(&tsdoc(&about, ""));
        text.push_str(&format!("\nexport class {name} {{\n"));
        for (n, v, owner) in &consts {
            let long = reader
                .class(owner)?
                .fields
                .iter()
                .any(|f| f.name == *n && matches!(f.ty.as_str(), "long" | "Long"));
            let (val, ty) = js_value(v, long)?;
            if !field_docs.contains_key(*owner) {
                let documented = scanned_class(repo_root, owner)?.map(|c| c.field_docs).unwrap_or_default();
                field_docs.insert(owner.to_string(), documented);
            }
            let javadoc = field_docs.get(*owner).and_then(|d| d.get(n)).map(String::as_str);
            let mut doc = constant_doc(&mut reader, &docs, owner, n, v, javadoc)?;
            if let Some((_, _, t)) = NOTES.iter().find(|(c, k, _)| c == java && k == n) {
                doc.push_str("\n\n");
                doc.push_str(t);
            }
            text.push_str(&tsdoc(&doc, "  "));
            text.push('\n');
            // The type is written out: a `static readonly` with a literal
            // initializer would otherwise be typed as that literal.
            text.push_str(&format!("  static readonly {n}: {ty} = {val};\n"));
        }
        if spec.kind == Kind::ConstantsOnly {
            text.push_str(&format!(
                "\n  /**\n   * Not instantiable: throws `TypeError`. Private, so TypeScript rejects `new` too.\n   *\n   * @param args - ignored\n   * @throws `TypeError` always\n   */\n  \
                 private constructor(...args: unknown[]) {{\n    void args;\n    throw new TypeError('{name} is not instantiable: its ConfigDef machinery is not ' +\n      \
                 'translated; a configuration is a plain object of its keys');\n  }}\n"
            ));
        }
        if spec.kind != Kind::ConstantsOnly {
            // A key-name class is translated in full (javascript/CLAUDE.md,
            // JavaScript Binding Conventions, Scope): Java's implicit public
            // constructor too.
            text.push_str(
                "\n  /**\n   * Java's default constructor. The class holds only its constants, which are static.\n   */\n  \
                 constructor() {}\n",
            );
        }
        for (c, k, _) in NOTES {
            if c == java && !consts.iter().any(|(n, _, _)| n == k) {
                anyhow::bail!("config constants: the note on `{c}.{k}` names no constant");
            }
        }
        text.push_str("}\n");
        let stem = crate::error_hierarchy::snake(name);
        out.push(JsModule {
            dir: (*dir).to_string(),
            stem,
            name: name.to_string(),
            content: text,
            notes: Vec::new(),
        });
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
            "  static readonly RECONFIGURABLE_CONFIGS: ReadonlySet<string> = readOnlySet(['ssl.keystore.type',"
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
        assert!(producer.content.contains("  private constructor(...args: unknown[]) {\n"));
        assert!(!producer.content.contains("  constructor() {}\n"));
        assert!(ssl.content.contains("  constructor() {}\n"));
        let sasl = modules.iter().find(|m| m.name == "SaslConfigs").unwrap();
        assert!(sasl
            .content
            .contains("  static readonly DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR: number = 0.8;\n"));
        // Java's long is a bigint; an int stays a number.
        assert!(sasl
            .content
            .contains("  static readonly DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN: bigint = 60000n;\n"));
        assert!(sasl
            .content
            .contains("  static readonly DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS: bigint = 3600000n;\n"));
    }

    #[test]
    fn test_constant_docs_come_from_java_doc_strings() {
        let modules = modules(&repo_root()).unwrap();
        let producer = &modules.iter().find(|m| m.name == "ProducerConfig").unwrap().content;
        let sasl = &modules.iter().find(|m| m.name == "SaslConfigs").unwrap().content;
        let ssl = &modules.iter().find(|m| m.name == "SslConfigs").unwrap().content;
        // A constant Java documents takes its own Javadoc: `<code>x</code>` is a
        // code span, a link keeps its target.
        assert!(
            producer.contains("  /**\n   * `metadata.max.age.ms`\n   */\n  static readonly METADATA_MAX_AGE_CONFIG")
        );
        assert!(!producer.contains("   * <code>"));
        assert!(sasl.contains("[here](http://www.iana.org/assignments/sasl-mechanisms/sasl-mechanisms.xhtml)"));
        // One Java leaves undocumented: the ConfigDef's doc of its key
        // (`SslConfigs.SSL_PROTOCOL_DOC`, through the producer's SSL support).
        assert!(
            ssl.contains("  /**\n   * The SSL protocol used to generate the SSLContext. The default is 'TLSv1.3',\n")
        );
        let mut reader = Reader::new(&repo_root());
        let key = Value::Str("bootstrap.servers".into());
        let doc = |reader: &mut Reader, javadoc: Option<&str>| {
            constant_doc(
                reader,
                &BTreeMap::new(),
                PRODUCER_CONFIG,
                "BOOTSTRAP_SERVERS_CONFIG",
                &key,
                javadoc,
            )
            .unwrap()
        };
        assert_eq!(doc(&mut reader, Some("<code>bootstrap.servers</code>")), "`bootstrap.servers`");
        // Without its Javadoc, the ConfigDef's doc and a sibling in the class: the key.
        assert_eq!(doc(&mut reader, None), "The `bootstrap.servers` configuration key.");
        // A Javadoc of no words (only `@since`) counts as none.
        assert_eq!(
            doc(&mut reader, Some("@since 1.0")),
            "The `bootstrap.servers` configuration key."
        );
        // A list keeps one item per line, a link its target.
        assert!(sasl.contains("   * - `org.apache.kafka.common.security.oauthbearer.ClientCredentialsJwtRetriever`\n"));
        assert!(sasl.contains("[here](https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html)"));
        assert!(producer.contains(
            "  /**\n   * The documentation of `ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG`.\n   */\n  static readonly ENABLE_IDEMPOTENCE_DOC"
        ));
        assert!(producer.contains(
            "  /**\n   * Java's `ProducerConfig.DEFAULT_CLIENT_RACK`.\n   */\n  static readonly DEFAULT_CLIENT_RACK"
        ));
        // Every constant has a TSDoc comment.
        for m in &modules {
            let lines: Vec<&str> = m.content.lines().collect();
            for (i, l) in lines.iter().enumerate() {
                if l.starts_with("  static readonly ") {
                    assert_eq!(lines[i - 1], "   */", "{}: {l}", m.name);
                }
            }
        }
        // A translation note follows Java's text.
        assert!(ssl.contains("   * Translation note: Java's value."));
    }
}
