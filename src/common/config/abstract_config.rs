// Copyright 2025 Confluent Inc.
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

//! Translation of `org.apache.kafka.common.config.AbstractConfig`.
//!
//! Java's `AbstractConfig` is the base class for `ProducerConfig`,
//! `ConsumerConfig`, `AdminClientConfig`, etc. It bundles a parsed
//! `Map<String, Object>` (typed values) with the originals
//! (`Map<String, String>` so plug-ins can retrieve free-form keys) plus
//! "used keys" tracking and type-safe accessors.
//!
//! For Phase 1 we translate the slice the producer needs:
//!
//! * Constructor that runs `ConfigDef::parse`.
//! * Typed accessors: `get_string`, `get_int`, `get_long`, `get_short`,
//!   `get_double`, `get_boolean`, `get_list`, `get_class`, `get_password`.
//! * `originals()` and `originals_strings()` for raw key access.
//! * `unused()` and `log_unused()` (logging the leftover keys).
//! * `values()` returning the typed map.
//!
//! Skipped:
//! * `getConfiguredInstance(...)` (reflective instantiation — the Rust
//!   client wires concrete types directly).
//! * `addSerializerToConfig`, `addDeserializerToConfig` (Connect-style helpers).
//! * `valuesWithPrefixOverride`, `originalsWithPrefix` (used by Connect).

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use crate::common::config::config_def::{ConfigDef, ConfigValue, Password};
use crate::common::errors::KafkaError;

/// Parsed configuration. Mirrors `AbstractConfig`.
pub struct AbstractConfig {
    /// Typed values keyed by config name. Mirrors Java's `values` map.
    values: HashMap<String, ConfigValue>,
    /// The raw originals provided by the user. Mirrors `originals`.
    originals: HashMap<String, String>,
    /// Set of keys touched via the typed accessors. Mirrors `used`.
    used: Mutex<HashSet<String>>,
}

impl AbstractConfig {
    /// Construct from a [`ConfigDef`] schema and the user-supplied raw
    /// `Map<String, String>`. Mirrors Java's
    /// `AbstractConfig(ConfigDef, Map<String, ?>)`.
    pub fn new(definition: &ConfigDef, originals: HashMap<String, String>) -> Result<Self, KafkaError> {
        let values = definition.parse(&originals)?;
        Ok(AbstractConfig { values, originals, used: Mutex::new(HashSet::new()) })
    }

    /// Borrow the parsed value map. Mirrors `values()`.
    pub fn values(&self) -> &HashMap<String, ConfigValue> {
        &self.values
    }

    /// Borrow the raw originals. Mirrors `originals()`.
    pub fn originals(&self) -> &HashMap<String, String> {
        &self.originals
    }

    /// Originals as strings (Java's `originalsStrings()` — kept for
    /// translation parity even though our `originals` is already `String`-typed).
    pub fn originals_strings(&self) -> HashMap<String, String> {
        self.originals.clone()
    }

    fn touch(&self, name: &str) {
        self.used.lock().unwrap().insert(name.to_owned());
    }

    /// Names that have not been read via the typed accessors. Mirrors `unused()`.
    pub fn unused(&self) -> Vec<String> {
        let used = self.used.lock().unwrap();
        self.originals.keys().filter(|k| !used.contains(k.as_str())).cloned().collect()
    }

    /// Log unused keys at WARN level. Mirrors `logUnused()`.
    pub fn log_unused(&self) {
        for name in self.unused() {
            log::warn!("The configuration '{name}' was supplied but isn't a known config.");
        }
    }

    fn get(&self, name: &str) -> Result<&ConfigValue, KafkaError> {
        self.touch(name);
        self.values
            .get(name)
            .ok_or_else(|| KafkaError::Config(format!("Unknown configuration '{name}'")))
    }

    /// `getString(String)`.
    pub fn get_string(&self, name: &str) -> Result<&str, KafkaError> {
        self.get(name).and_then(|v| {
            v.as_str()
                .ok_or_else(|| KafkaError::Config(format!("Configuration '{name}' is not a string: {v:?}")))
        })
    }

    /// `getInt(String)`.
    pub fn get_int(&self, name: &str) -> Result<i32, KafkaError> {
        self.get(name).and_then(|v| {
            v.as_i32()
                .ok_or_else(|| KafkaError::Config(format!("Configuration '{name}' is not an int: {v:?}")))
        })
    }

    /// `getShort(String)`.
    pub fn get_short(&self, name: &str) -> Result<i16, KafkaError> {
        self.get(name).and_then(|v| {
            v.as_i16()
                .ok_or_else(|| KafkaError::Config(format!("Configuration '{name}' is not a short: {v:?}")))
        })
    }

    /// `getLong(String)`.
    pub fn get_long(&self, name: &str) -> Result<i64, KafkaError> {
        self.get(name).and_then(|v| {
            v.as_i64()
                .ok_or_else(|| KafkaError::Config(format!("Configuration '{name}' is not a long: {v:?}")))
        })
    }

    /// `getDouble(String)`.
    pub fn get_double(&self, name: &str) -> Result<f64, KafkaError> {
        self.get(name).and_then(|v| {
            v.as_f64()
                .ok_or_else(|| KafkaError::Config(format!("Configuration '{name}' is not a double: {v:?}")))
        })
    }

    /// `getBoolean(String)`.
    pub fn get_boolean(&self, name: &str) -> Result<bool, KafkaError> {
        self.get(name).and_then(|v| {
            v.as_bool()
                .ok_or_else(|| KafkaError::Config(format!("Configuration '{name}' is not a boolean: {v:?}")))
        })
    }

    /// `getList(String)`.
    pub fn get_list(&self, name: &str) -> Result<&[String], KafkaError> {
        self.get(name).and_then(|v| {
            v.as_list()
                .ok_or_else(|| KafkaError::Config(format!("Configuration '{name}' is not a list: {v:?}")))
        })
    }

    /// `getClass(String)` — returns the FQCN string. We do not perform
    /// reflective class loading in the Rust client.
    pub fn get_class(&self, name: &str) -> Result<&str, KafkaError> {
        self.get(name).and_then(|v| match v {
            ConfigValue::Class(s) | ConfigValue::String(s) => Ok(s.as_str()),
            other => Err(KafkaError::Config(format!("Configuration '{name}' is not a class: {other:?}"))),
        })
    }

    /// `getPassword(String)`.
    pub fn get_password(&self, name: &str) -> Result<&Password, KafkaError> {
        self.get(name).and_then(|v| match v {
            ConfigValue::Password(p) => Ok(p),
            other => Err(KafkaError::Config(format!(
                "Configuration '{name}' is not a password: {other:?}"
            ))),
        })
    }
}

#[cfg(test)]
mod tests {
    // The Java client has `AbstractConfigTest` but it's tightly coupled to
    // reflection-based plug-in instantiation that we explicitly skip. We
    // test the typed-accessor + originals + unused contract directly.

    use std::sync::Arc;

    use super::*;
    use crate::common::config::config_def::{Importance, Range, Type, Validator};

    fn def() -> ConfigDef {
        let mut d = ConfigDef::new();
        let range: Arc<dyn Validator> = Arc::new(Range::at_least(0));
        d.define("a", Type::Int, Some(ConfigValue::Int(1)), Some(range), Importance::High, "")
            .unwrap();
        d.define(
            "b",
            Type::String,
            Some(ConfigValue::String("hi".into())),
            None,
            Importance::Low,
            "",
        )
        .unwrap();
        d
    }

    #[test]
    fn typed_accessors_return_parsed_values() {
        let config = AbstractConfig::new(&def(), HashMap::new()).unwrap();
        assert_eq!(config.get_int("a").unwrap(), 1);
        assert_eq!(config.get_string("b").unwrap(), "hi");
    }

    #[test]
    fn originals_round_trip() {
        let originals = [("a", "5"), ("b", "world"), ("extra", "ignored")]
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        let config = AbstractConfig::new(&def(), originals).unwrap();
        assert_eq!(config.originals().get("extra").map(String::as_str), Some("ignored"));
        assert_eq!(config.get_int("a").unwrap(), 5);
        assert_eq!(config.get_string("b").unwrap(), "world");
    }

    #[test]
    fn unused_lists_keys_not_touched() {
        let originals = [("a", "1"), ("b", "2"), ("extra", "ignored")]
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        let config = AbstractConfig::new(&def(), originals).unwrap();
        // Touch "a" but not "b".
        let _ = config.get_int("a").unwrap();
        let unused = config.unused();
        assert!(!unused.contains(&"a".to_owned()));
        assert!(unused.contains(&"extra".to_owned()));
        // "b" is in originals but not touched -> unused.
        assert!(unused.contains(&"b".to_owned()));
    }

    #[test]
    fn type_mismatch_returns_config_error() {
        let config = AbstractConfig::new(&def(), HashMap::new()).unwrap();
        let err = config.get_long("a").unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)));
    }

    #[test]
    fn unknown_key_returns_config_error() {
        let config = AbstractConfig::new(&def(), HashMap::new()).unwrap();
        let err = config.get_string("missing").unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)));
    }
}
