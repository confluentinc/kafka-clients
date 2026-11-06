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

//! The Node.js client's `lib/_config_types.ts`: the `ConfigDef` type of every
//! producer config key and what its validator does with `null`, as the Python
//! client's `_config_types.py` (the producer table only: the Node.js client
//! has no consumer or admin client yet).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use super::{client_types, null_rule, NullRule, Reader};
use crate::config_consts;
use crate::javascript_gen::{header, js_str, JS_LIB};

type Res<T> = anyhow::Result<T>;

const PRODUCER_CONFIG: &str = "org.apache.kafka.clients.producer.ProducerConfig";

/// `lib/_config_types.ts` and its content.
pub(crate) fn output(repo_root: &Path) -> Res<(PathBuf, String)> {
    let mut reader = Reader { root: repo_root.to_path_buf(), cache: BTreeMap::new() };
    let mut consts = config_consts::Reader::new(repo_root);
    let mut types = Vec::new();
    let mut nulls = Vec::new();
    for (key, def) in client_types(&mut reader, PRODUCER_CONFIG)? {
        // Java's null is JavaScript's null, spelled `null` in a message.
        let rule = null_rule(&mut reader, &mut consts, &key, &def, "null")?;
        types.push(format!("  [{}, {}],", js_str(&key), js_str(&def.ty)));
        let (form, kind, message) = match rule {
            NullRule::Allowed => continue,
            NullRule::Named { kind, message } => ("named", kind, message),
            NullRule::Plain { kind, message } => ("plain", kind, message),
        };
        nulls.push(format!(
            "  [{}, [{}, {}]], // {kind}",
            js_str(&key),
            js_str(form),
            js_str(&message)
        ));
    }
    let mut text = header("Java's `ProducerConfig` (with `SslConfigs` / `SaslConfigs`)");
    text.push_str(
        "\n// The `ConfigDef` type of every producer config key (Java's `ProducerConfig`,\n\
         // with `withClientSslSupport()` / `withClientSaslSupport()`), and what its\n\
         // validator does with `null` (private; python/confluent_kafka/_config_types.py's\n\
         // PRODUCER / PRODUCER_NULL). `_config` checks a value's JavaScript type against\n\
         // its key's type and passes every other key to the core as given.\n\
         //\n\
         // PRODUCER_NULL maps each key whose `ConfigDef` validator rejects an explicit\n\
         // `null` (`ConfigDef.parseValue` runs the validator on it) to the\n\
         // `ConfigException` Java throws: ['named', message] for\n\
         // `ConfigException(name, null, message)`, ['plain', message] for\n\
         // `ConfigException(message)`. A key that is not there holds `null` in Java.\n\n",
    );
    text.push_str("export const PRODUCER: ReadonlyMap<string, string> = new Map<string, string>([\n");
    for t in &types {
        text.push_str(t);
        text.push('\n');
    }
    text.push_str(
        "]);\n\n/** The `ConfigException` a key's validator throws for `null`: its form and message. */\n\
         type NullRule = readonly ['named' | 'plain', string];\n\n\
         export const PRODUCER_NULL: ReadonlyMap<string, NullRule> = new Map<string, NullRule>([\n",
    );
    for n in &nulls {
        text.push_str(n);
        text.push('\n');
    }
    text.push_str("]);\n");
    Ok((repo_root.join(JS_LIB).join("_config_types.ts"), text))
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
    fn test_producer_table() {
        let (path, text) = output(&repo_root()).unwrap();
        assert!(path.ends_with("javascript/lib/_config_types.ts"));
        assert!(text.contains("export const PRODUCER: ReadonlyMap<string, string> = new Map<string, string>([\n"));
        assert!(
            text.contains("export const PRODUCER_NULL: ReadonlyMap<string, NullRule> = new Map<string, NullRule>([\n")
        );
        assert!(!text.contains("module.exports"));
        for line in [
            "  ['bootstrap.servers', 'LIST'],",
            "  ['ssl.keystore.password', 'PASSWORD'],",
            "  ['sasl.jaas.config', 'PASSWORD'],",
            "  ['sasl.login.refresh.window.factor', 'DOUBLE'],",
            "  ['security.protocol', ['named', 'String must be one of (case insensitive): SASL_SSL, PLAINTEXT, SSL, SASL_PLAINTEXT']], // CaseInsensitiveValidString",
            "  ['bootstrap.servers', ['plain', 'Configuration \\'bootstrap.servers\\' values must not be null.']], // ValidList",
        ] {
            assert!(text.contains(line), "missing {line}");
        }
        assert!(!text.contains("max.poll.records"));
    }
}
