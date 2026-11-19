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
//! with no run-time part.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use super::{client_types, Reader};
use crate::javascript_gen::{header, js_str, JS_SRC};

type Res<T> = anyhow::Result<T>;

const PRODUCER_CONFIG: &str = "org.apache.kafka.clients.producer.ProducerConfig";

/// `src/producer/producer_configs.ts` and its content.
pub(crate) fn output(repo_root: &Path) -> Res<(PathBuf, String)> {
    let mut reader = Reader { root: repo_root.to_path_buf(), cache: BTreeMap::new() };
    let keys: Vec<String> = client_types(&mut reader, PRODUCER_CONFIG)?.into_keys().collect();
    let mut text = header("Java's `ProducerConfig` (with `SslConfigs` / `SaslConfigs`)");
    text.push_str(
        "\n/**\n \
         * A producer's configuration: Java's `ProducerConfig` keys, as a plain object\n \
         * (a `Map` is refused). Each value is written as the string the core parses\n \
         * (`true` as `\"true\"`, numbers and bigints in decimal, arrays joined with\n \
         * `,`, anything else by `String()`); `null` and `undefined` leave the key\n \
         * out. The core checks every key and value, with Java's messages.\n \
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
    for k in &keys {
        text.push_str(&format!("    | {}\n", js_str(k)));
    }
    text.push_str(
        "    // Any other key, keeping the known keys' autocompletion: the core\n    \
         // lists the keys it does not use.\n    \
         | (string & {})]?: string | number | boolean | bigint | ReadonlyArray<string | number | bigint> | null | undefined;\n\
         };\n",
    );
    Ok((repo_root.join(JS_SRC).join("producer").join("producer_configs.ts"), text))
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
        let (path, text) = output(&repo_root()).unwrap();
        assert!(path.ends_with("javascript/src/producer/producer_configs.ts"));
        assert!(text.contains("export type ProducerConfigs = {\n  [key in\n"));
        for line in [
            "    | 'bootstrap.servers'\n",
            "    | 'ssl.keystore.password'\n",
            "    | 'sasl.jaas.config'\n",
            "    | 'linger.ms'\n",
            "    | (string & {})]?: string | number | boolean | bigint",
        ] {
            assert!(text.contains(line), "missing {line}");
        }
        // No ConfigDef table: the type names keys only.
        assert!(!text.contains("'LIST'"));
        assert!(!text.contains("max.poll.records"));
    }
}
