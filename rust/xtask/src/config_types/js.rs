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
//! `ConfigDef` table and checks no value (javascript/CLAUDE.md, JavaScript Binding
//! Conventions, Configuration): only the key names are read from Java, a type
//! with no run-time part. The producer's serde keys, `key.serializer` and
//! `value.serializer`, take a function `({ configs, isKey }) => serializer`
//! instead, one returning a Promise of it included, which only
//! `KafkaProducer.create()` waits for (Serialization and Lifetime), and so may
//! an unknown key.
//!
//! It also writes `src/consumer/consumer_configs.ts`, the type of a
//! consumer's configuration: every key Java's `ConsumerConfig` defines, the
//! same way. The consumer's serde keys, `key.deserializer` and
//! `value.deserializer`, take a function `({ configs, isKey }) => deserializer`,
//! as the producer's take serializers.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use super::{client_types, Reader};
use crate::javascript_gen::{header, js_str, JS_SRC};

type Res<T> = anyhow::Result<T>;

const PRODUCER_CONFIG: &str = "org.apache.kafka.clients.producer.ProducerConfig";
const CONSUMER_CONFIG: &str = "org.apache.kafka.clients.consumer.ConsumerConfig";

/// `src/producer/producer_configs.ts` and `src/consumer/consumer_configs.ts`,
/// with their content.
pub(crate) fn outputs(repo_root: &Path) -> Res<Vec<(PathBuf, String)>> {
    let mut reader = Reader { root: repo_root.to_path_buf(), cache: BTreeMap::new() };
    let keys: Vec<String> = client_types(&mut reader, PRODUCER_CONFIG)?.into_keys().collect();
    let consumer_keys: Vec<String> = client_types(&mut reader, CONSUMER_CONFIG)?.into_keys().collect();
    Ok(vec![
        configs_type(repo_root, &keys),
        consumer_configs_type(repo_root, &consumer_keys),
    ])
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
         * `key.serializer` and `value.serializer` take a function\n \
         * `({ configs, isKey }) => serializer`, such as `() => new StringSerializer()`,\n \
         * and never reach the core. The constructor takes only a synchronous one;\n \
         * `KafkaProducer.create()` also waits for one that returns a Promise.\n \
         *\n \
         * @remarks *(js-only)* The type of the configuration the constructor and\n \
         * `create()` take, Java's `Map<String, Object>` and `Properties` as one. It\n \
         * lists the keys Java's `ProducerConfig` defines, for autocompletion, and\n \
         * takes any other string too, through `(string & {})`, since an unknown key\n \
         * goes to the core.\n \
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
         // A serde config key's value: the function that makes the serializer,\n\
         // or a Promise of it, which only `create()` waits for.\n\
         type SerializerConfig =\n  \
         | ((options: { configs: Readonly<Record<string, unknown>>; isKey: boolean }) => AnySerializer | PromiseLike<AnySerializer>)\n  \
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

/// `src/consumer/consumer_configs.ts` and its content: the consumer's keys, as
/// `configs_type` writes the producer's, `key.deserializer` and
/// `value.deserializer` taking the function that makes a deserializer.
fn consumer_configs_type(repo_root: &Path, keys: &[String]) -> (PathBuf, String) {
    let mut text = header("Java's `ConsumerConfig` (with `SslConfigs` / `SaslConfigs`)");
    text.push_str("\nimport type { Headers } from '../common/header/headers';\n");
    text.push_str(
        "\n/**\n \
         * A consumer's configuration: Java's `ConsumerConfig` keys, as a plain object\n \
         * (a `Map` is refused). Each value is written as the string the core parses\n \
         * (`true` as `\"true\"`, numbers and bigints in decimal, arrays joined with\n \
         * `,`, anything else by `String()`); `null` and `undefined` leave the key\n \
         * out. The core checks every key and value, with Java's messages.\n \
         * `key.deserializer` and `value.deserializer` take a function\n \
         * `({ configs, isKey }) => deserializer`, such as\n \
         * `() => new StringDeserializer()`, and never reach the core. The\n \
         * constructor takes only a synchronous one; `KafkaConsumer.create()` also\n \
         * waits for one that returns a Promise.\n \
         *\n \
         * @remarks *(js-only)* The type of the configuration the constructor and\n \
         * `create()` take, Java's `Map<String, Object>` and `Properties` as one. It\n \
         * lists the keys Java's `ConsumerConfig` defines, for autocompletion, and\n \
         * takes any other string too, through `(string & {})`, since an unknown key\n \
         * goes to the core.\n \
         */\n\
         export type ConsumerConfigs = {\n  \
         [key in\n",
    );
    // The serde keys are left out of the list: what a configuration typed
    // `ConsumerConfigs` holds there would bind the consumer's type arguments
    // to `unknown`, the deserializers' type here, where an omitted one binds
    // `Buffer`. The consumer's own types list them, typed by the
    // deserializers their functions return.
    for k in keys.iter().filter(|k| *k != "key.deserializer" && *k != "value.deserializer") {
        text.push_str(&format!("    | {}\n", js_str(k)));
    }
    text.push_str(
        "    // Any other key, keeping the known keys' autocompletion: the core\n    \
         // lists the keys it does not use. `key.deserializer` and\n    \
         // `value.deserializer` are among them, typed by the consumer's\n    \
         // constructor, so that a configuration of this type binds none of its\n    \
         // type arguments.\n    \
         | (string & {})]?: string extends key ? ConfigValue | DeserializerConfig : ConfigValue;\n\
         };\n\
         \n\
         // A value the core parses.\n\
         type ConfigValue = string | number | boolean | bigint | ReadonlyArray<string | number | bigint> | null | undefined;\n\
         \n\
         // A serde config key's value: the function that makes the deserializer,\n\
         // or a Promise of it, which only `create()` waits for.\n\
         type DeserializerConfig =\n  \
         | ((options: { configs: Readonly<Record<string, unknown>>; isKey: boolean }) => AnyDeserializer | PromiseLike<AnyDeserializer>)\n  \
         | null\n  \
         | undefined;\n\
         \n\
         // A deserializer of any data: what it returns is left unknown, so that a\n\
         // configuration typed `ConsumerConfigs` binds no type argument of the\n\
         // consumer, which infers them only from the functions an object literal\n\
         // holds.\n\
         type AnyDeserializer = {\n  \
         deserialize(options: { topic: string; headers: Headers; data: Buffer }): unknown;\n  \
         close?(): void;\n\
         };\n",
    );
    (repo_root.join(JS_SRC).join("consumer").join("consumer_configs.ts"), text)
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

    /// The content of the output whose path ends with `path`.
    fn output<'a>(outputs: &'a [(PathBuf, String)], path: &str) -> &'a str {
        outputs
            .iter()
            .find(|(p, _)| p.ends_with(path))
            .map(|(_, text)| text.as_str())
            .unwrap_or_else(|| panic!("no output {path}"))
    }

    #[test]
    fn test_producer_keys() {
        let outputs = outputs(&repo_root()).unwrap();
        let text = output(&outputs, "javascript/src/producer/producer_configs.ts");
        assert!(text.contains("export type ProducerConfigs = {\n  [key in\n"));
        for line in [
            "    | 'bootstrap.servers'\n",
            "    | 'ssl.keystore.password'\n",
            "    | 'sasl.jaas.config'\n",
            "    | 'linger.ms'\n",
            "    | (string & {})]?: key extends 'key.serializer' | 'value.serializer'\n",
            "type ConfigValue = string | number | boolean | bigint",
            "=> AnySerializer | PromiseLike<AnySerializer>)",
            "`KafkaProducer.create()` also waits for one that returns a Promise.",
            "serialize(options: { topic: string; headers: Headers })",
        ] {
            assert!(text.contains(line), "missing {line}");
        }
        // No ConfigDef table: the type names keys only.
        assert!(!text.contains("'LIST'"));
        assert!(!text.contains("max.poll.records"));
    }

    #[test]
    fn test_consumer_keys() {
        let outputs = outputs(&repo_root()).unwrap();
        let text = output(&outputs, "javascript/src/consumer/consumer_configs.ts");
        assert!(text.contains("export type ConsumerConfigs = {\n  [key in\n"));
        for line in [
            "    | 'bootstrap.servers'\n",
            "    | 'group.id'\n",
            "    | 'group.protocol'\n",
            "    | 'ssl.keystore.password'\n",
            "    | 'sasl.jaas.config'\n",
            "    | (string & {})]?: string extends key ? ConfigValue | DeserializerConfig : ConfigValue;\n",
            "=> AnyDeserializer | PromiseLike<AnyDeserializer>)",
            "`KafkaConsumer.create()` also\n * waits for one that returns a Promise.",
            "deserialize(options: { topic: string; headers: Headers; data: Buffer }): unknown;",
        ] {
            assert!(text.contains(line), "missing {line}");
        }
        // The producer's keys are not the consumer's, and the serde keys are
        // the constructor's to type.
        assert!(!text.contains("'linger.ms'"));
        assert!(!text.contains("    | 'key.deserializer'\n"));
        assert!(!text.contains("Serializer"));
        // No run-time key list is generated: every key reaches the core.
        assert!(!outputs
            .iter()
            .any(|(p, _)| p.ends_with("javascript/src/internal/consumer_config_keys.ts")));
        assert!(!outputs
            .iter()
            .any(|(p, _)| p.ends_with("javascript/src/internal/producer_config_keys.ts")));
    }
}
