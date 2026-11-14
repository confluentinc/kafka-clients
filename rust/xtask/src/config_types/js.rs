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

//! The Node.js client's `src/producer/producer_configs.ts`: the type of a
//! producer's configuration, which lists every config key Java's
//! `ProducerConfig` defines (with its SSL and SASL keys) for autocompletion and
//! still takes any string, through `(string & {})`. The binding has no
//! `ConfigDef` table and checks no value (root CLAUDE.md, JavaScript Binding
//! Conventions, Configuration): only the key names are read from Java, a type
//! with no run-time part. The producer's serde keys, `key.serializer` and
//! `value.serializer`, take a synchronous function `({ configs, isKey }) =>
//! serializer` instead (Serialization, Q53), and so may an unknown key.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use super::{client_types, Reader};
use crate::javascript_gen::{header, js_str, JS_SRC};

type Res<T> = anyhow::Result<T>;

const PRODUCER_CONFIG: &str = "org.apache.kafka.clients.producer.ProducerConfig";

/// `src/producer/producer_configs.ts` and `src/internal/producer_config_keys.ts`,
/// with their content.
pub(crate) fn outputs(repo_root: &Path) -> Res<Vec<(PathBuf, String)>> {
    let mut reader = Reader { root: repo_root.to_path_buf(), cache: BTreeMap::new() };
    let keys: Vec<String> = client_types(&mut reader, PRODUCER_CONFIG)?.into_keys().collect();
    let core_only = core_only_keys(repo_root, &keys)?;
    Ok(vec![configs_type(repo_root, &keys), known_keys(repo_root, &keys, &core_only)])
}

/// The core's producer configuration: `rust/src/producer/producer_config.rs`,
/// whose `CONFIG` `ConfigDef` names each key by a `ProducerConfig` or
/// `CommonClientConfigs` constant.
const CORE_PRODUCER_CONFIG: &str = "rust/src/producer/producer_config.rs";
const CORE_COMMON_CLIENT_CONFIGS: &str = "rust/src/common_client_configs.rs";

/// The producer keys the Rust core's `ProducerConfig` defines beyond Java's
/// (`partitioner.type`, which names a built-in partitioner in place of Java's
/// class-valued `partitioner.class`), sorted: the keys of the `define...`
/// calls of its `CONFIG`, resolved through their constants, minus `java`.
/// The SSL and SASL keys its `with_client_..._support()` adds are Java's.
fn core_only_keys(repo_root: &Path, java: &[String]) -> Res<Vec<String>> {
    use syn::visit::Visit;

    let parse = |rel: &str| -> Res<syn::File> {
        let path = repo_root.join(rel);
        let text =
            std::fs::read_to_string(&path).map_err(|e| anyhow::anyhow!("config types: {}: {e}", path.display()))?;
        syn::parse_file(&text).map_err(|e| anyhow::anyhow!("config types: {rel}: {e}"))
    };
    let producer = parse(CORE_PRODUCER_CONFIG)?;
    let common = parse(CORE_COMMON_CLIENT_CONFIGS)?;

    // `(type, constant)` -> its string value, from the inherent impls; a
    // constant that aliases another (`ProducerConfig::CLIENT_RACK_CONFIG` is
    // `CommonClientConfigs::CLIENT_RACK_CONFIG`) is resolved below.
    let mut consts: BTreeMap<(String, String), String> = BTreeMap::new();
    let mut aliases: Vec<((String, String), (String, String))> = Vec::new();
    for file in [&producer, &common] {
        for item in &file.items {
            let syn::Item::Impl(imp) = item else { continue };
            if imp.trait_.is_some() {
                continue;
            }
            let syn::Type::Path(ty) = &*imp.self_ty else { continue };
            let Some(ty) = ty.path.segments.last() else { continue };
            for item in &imp.items {
                let syn::ImplItem::Const(c) = item else { continue };
                let at = (ty.ident.to_string(), c.ident.to_string());
                match &c.expr {
                    syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(v), .. }) => {
                        consts.insert(at, v.value());
                    },
                    syn::Expr::Path(p) if p.path.segments.len() == 2 => {
                        let to = (p.path.segments[0].ident.to_string(), p.path.segments[1].ident.to_string());
                        aliases.push((at, to));
                    },
                    _ => {},
                }
            }
        }
    }
    // An alias of an alias resolves on a later pass.
    while !aliases.is_empty() {
        let before = aliases.len();
        aliases.retain(|(at, to)| match consts.get(to).cloned() {
            Some(v) => {
                consts.insert(at.clone(), v);
                false
            },
            None => true,
        });
        if aliases.len() == before {
            break;
        }
    }

    let init = producer
        .items
        .iter()
        .find_map(|item| match item {
            syn::Item::Static(s) if s.ident == "CONFIG" => Some(&*s.expr),
            _ => None,
        })
        .ok_or_else(|| anyhow::anyhow!("config types: {CORE_PRODUCER_CONFIG} has no `CONFIG`"))?;

    struct Defines<'a> {
        consts: &'a BTreeMap<(String, String), String>,
        keys: Vec<String>,
        unresolved: Vec<String>,
    }
    impl<'ast> Visit<'ast> for Defines<'_> {
        fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
            if call.method.to_string().starts_with("define") {
                match call.args.first() {
                    Some(syn::Expr::Path(p)) if p.path.segments.len() == 2 => {
                        let ty = p.path.segments[0].ident.to_string();
                        let name = p.path.segments[1].ident.to_string();
                        match self.consts.get(&(ty.clone(), name.clone())) {
                            Some(key) => self.keys.push(key.clone()),
                            None => self.unresolved.push(format!("{ty}::{name}")),
                        }
                    },
                    Some(syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(v), .. })) => self.keys.push(v.value()),
                    _ => self.unresolved.push(call.method.to_string()),
                }
            }
            syn::visit::visit_expr_method_call(self, call);
        }
    }
    let mut defines = Defines { consts: &consts, keys: Vec::new(), unresolved: Vec::new() };
    defines.visit_expr(init);
    if !defines.unresolved.is_empty() {
        anyhow::bail!(
            "config types: {CORE_PRODUCER_CONFIG}: cannot resolve the keys of {}",
            defines.unresolved.join(", ")
        );
    }
    if defines.keys.is_empty() {
        anyhow::bail!("config types: {CORE_PRODUCER_CONFIG} `CONFIG` defines no keys");
    }
    let mut out: Vec<String> = defines.keys.into_iter().filter(|k| !java.contains(k)).collect();
    out.sort();
    out.dedup();
    Ok(out)
}

/// `src/internal/producer_config_keys.ts`: the same keys at run time, which
/// the producer keeps in the configuration the core gets even when a serde
/// config function reads them (javascript/CLAUDE.md, the serde config keys).
fn known_keys(repo_root: &Path, keys: &[String], core_only: &[String]) -> (PathBuf, String) {
    let mut text = header(
        "Java's `ProducerConfig` (with `SslConfigs` / `SaslConfigs`) and the\n// core's `rust/src/producer/producer_config.rs`",
    );
    text.push_str(
        "\n// The producer config keys (private): those Java's `ProducerConfig` defines,\n\
         // then those only the Rust core defines. A key a serde config function\n\
         // reads still reaches the core when it is one of them.\n\
         \n\
         const KEYS: ReadonlySet<string> = new Set([\n",
    );
    for k in keys {
        text.push_str(&format!("  {},\n", js_str(k)));
    }
    if !core_only.is_empty() {
        text.push_str("  // Defined by the Rust core only.\n");
    }
    for k in core_only {
        text.push_str(&format!("  {},\n", js_str(k)));
    }
    text.push_str(
        "]);\n\
         \n\
         /**\n \
         * Whether `key` is a producer config key, Java's or the core's.\n \
         *\n \
         * @param key - the config key\n \
         * @returns true for a key of Java's producer configuration or the core's\n \
         */\n\
         export function isProducerConfigKey(key: string): boolean {\n  \
         return KEYS.has(key);\n\
         }\n",
    );
    (repo_root.join(JS_SRC).join("internal").join("producer_config_keys.ts"), text)
}

/// `src/producer/producer_configs.ts` and its content.
fn configs_type(repo_root: &Path, keys: &[String]) -> (PathBuf, String) {
    let mut text = header("Java's `ProducerConfig` (with `SslConfigs` / `SaslConfigs`)");
    text.push_str("\nimport type { Headers } from '../common/header/headers';\n");
    text.push_str(
        "\n/**\n \
         * A producer's configuration: Java's `ProducerConfig` keys, as a plain object\n \
         * (a `Map` is refused). Each value is written as the string the core parses\n \
         * (`true` as `\"true\"`, numbers and bigints in decimal, arrays joined with\n \
         * `,`, anything else by `String()`); `null` and `undefined` leave the key\n \
         * out. The core checks every key and value, with Java's messages.\n \
         * `key.serializer` and `value.serializer` take a synchronous function\n \
         * `({ configs, isKey }) => serializer`, such as `stringSerializer`, and\n \
         * never reach the core.\n \
         *\n \
         * @remarks *(js-only)* The type of the configuration the constructor takes,\n \
         * Java's `Map<String, Object>` and `Properties` as one. It lists the keys\n \
         * Java's `ProducerConfig` defines, for autocompletion, and takes any other\n \
         * string too, through `(string & {})`, since an unknown key goes to the\n \
         * core.\n \
         */\n\
         export type ProducerConfigs = {\n  \
         [key in\n",
    );
    for k in keys {
        text.push_str(&format!("    | {}\n", js_str(k)));
    }
    text.push_str(
        "    // Any other key, keeping the known keys' autocompletion: the core\n    \
         // lists the keys it does not use.\n    \
         | (string & {})]?: key extends 'key.serializer' | 'value.serializer'\n    \
         ? SerializerConfig\n    \
         : string extends key\n      \
         ? ConfigValue | SerializerConfig\n      \
         : ConfigValue;\n\
         };\n\
         \n\
         // A value the core parses.\n\
         type ConfigValue = string | number | boolean | bigint | ReadonlyArray<string | number | bigint> | null | undefined;\n\
         \n\
         // A serde config key's value: the function that makes the serializer.\n\
         type SerializerConfig =\n  \
         | ((options: { configs: Readonly<Record<string, unknown>>; isKey: boolean }) => AnySerializer)\n  \
         | null\n  \
         | undefined;\n\
         \n\
         // A serializer of any data: its `data` is left out, so that a configuration\n\
         // typed `ProducerConfigs` binds no type argument of the producer, which\n\
         // infers them only from the functions an object literal holds.\n\
         type AnySerializer = {\n  \
         serialize(options: { topic: string; headers: Headers }): Uint8Array | null | Promise<Uint8Array | null>;\n  \
         close?(): void;\n\
         };\n",
    );
    (repo_root.join(JS_SRC).join("producer").join("producer_configs.ts"), text)
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

    #[test]
    fn test_producer_keys() {
        let outputs = outputs(&repo_root()).unwrap();
        let (path, text) = &outputs[0];
        assert!(path.ends_with("javascript/src/producer/producer_configs.ts"));
        assert!(text.contains("export type ProducerConfigs = {\n  [key in\n"));
        for line in [
            "    | 'bootstrap.servers'\n",
            "    | 'ssl.keystore.password'\n",
            "    | 'sasl.jaas.config'\n",
            "    | 'linger.ms'\n",
            "    | (string & {})]?: key extends 'key.serializer' | 'value.serializer'\n",
            "type ConfigValue = string | number | boolean | bigint",
            "=> AnySerializer)",
            "serialize(options: { topic: string; headers: Headers })",
        ] {
            assert!(text.contains(line), "missing {line}");
        }
        // No ConfigDef table: the type names keys only.
        assert!(!text.contains("'LIST'"));
        assert!(!text.contains("max.poll.records"));
        let (path, text) = &outputs[1];
        assert!(path.ends_with("javascript/src/internal/producer_config_keys.ts"));
        assert!(text.contains("  'bootstrap.servers',\n"));
        assert!(text.contains("export function isProducerConfigKey(key: string): boolean {"));
        // The keys only the core defines follow Java's.
        let core = text.find("  // Defined by the Rust core only.\n").expect("core-only keys");
        assert!(text[core..].contains("  'partitioner.type',\n"));
        assert!(!text[..core].contains("'partitioner.type'"));
    }

    #[test]
    fn test_core_only_keys() {
        let java = vec!["bootstrap.servers".to_string(), "acks".to_string()];
        let keys = core_only_keys(&repo_root(), &java).unwrap();
        assert!(keys.contains(&"partitioner.type".to_string()));
        assert!(keys.contains(&"linger.ms".to_string()));
        assert!(!keys.contains(&"bootstrap.servers".to_string()));
        assert!(keys.windows(2).all(|w| w[0] < w[1]));
    }
}
