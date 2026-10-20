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

//! Translated from `org.apache.kafka.common.config.AbstractConfig`: the
//! originals a client was given, the values its [`ConfigDef`] parsed from
//! them, and the set of keys the client read, which [`AbstractConfig::unused`]
//! and [`AbstractConfig::log_unused`] report.
//!
//! Crate-private: each client config (`ProducerConfig`, `ConsumerConfig`,
//! `AdminClientConfig`) holds one and forwards Java's public getters to it, so
//! no new public type is needed. Java needs `AbstractConfig` public only for
//! those inherited getters.
//!
//! What is translated: the constructor with its `preProcessParsedConfig` /
//! `postProcessParsedConfig` hooks and the re-parse that validates the
//! post-processed values, `get` and the typed getters, `ignore`, `unused`,
//! `documentationOf`, `logUnused`, `logAll`, `equals` and `hashCode`.
//!
//! What is not, each deferred to the public translation (see `CHANGELOG.md`):
//!
//! - Config providers and variable resolution (`resolveConfigVariables`,
//!   `config.providers`, `AUTOMATIC_CONFIG_PROVIDERS_PROPERTY`): the client
//!   configs do not define `config.providers`.
//! - `originals*()`, `values*()` and `nonInternalValues()`: Java returns a
//!   `RecordingMap`, which marks the keys read through it as used; the
//!   crate reads [`AbstractConfig::originals_map`] and
//!   [`AbstractConfig::parsed_values`] directly and marks keys with
//!   [`AbstractConfig::get`] or [`AbstractConfig::ignore`].
//! - `getClass`, `getConfiguredInstance(s)`: a configuration never takes a
//!   class (CLAUDE.md §2).
//! - `typeOf`: it returns `ConfigDef.Type`, not public.

// Until the client configs are rebuilt on `AbstractConfig` (the commits that
// follow this one), only the tests reach it.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the client configs are moved onto AbstractConfig by the following commits"
    )
)]

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{Mutex, PoisonError};

use crate::common::Error;
use crate::common::config::config_def::{ConfigDef, ParsedValue, Type, java_hash_table_order};
use crate::common::config::types::Password;

/// A configuration map as Java's `Map<String, ?>`: `None` is a `null` value.
pub(crate) type ConfigMap = HashMap<String, Option<ParsedValue>>;

/// Java's `AbstractConfig.CONFIG_PROVIDERS_DOC`.
pub(crate) const CONFIG_PROVIDERS_DOC: &str = "Comma-separated alias names for classes implementing the \
    <code>ConfigProvider</code> interface. This enables loading configuration data (such as passwords, API \
    keys, and other credentials) from external sources. For example, see \
    <a href=\"https://kafka.apache.org/documentation/#config_providers\">Configuration Providers</a>.";

/// A convenient base class for configurations to extend, Java's
/// `AbstractConfig`.
///
/// This class holds both the original configuration that was provided as well
/// as the parsed configuration.
#[doc(alias = "org.apache.kafka.common.config.AbstractConfig")]
pub(crate) struct AbstractConfig {
    /// Configs for which values have been requested, used to detect unused
    /// configs. Modified through `&self`, as Java's concurrent set is.
    used: Mutex<HashSet<String>>,
    /// The original values passed in by the user.
    originals: ConfigMap,
    /// The parsed values.
    values: ConfigMap,
    definition: &'static ConfigDef,
}

impl AbstractConfig {
    /// Construct a configuration with a `ConfigDef` and the configuration
    /// properties: Java's `AbstractConfig(definition, originals, doLog)`
    /// without hooks.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] if a value is missing, cannot be parsed or fails its
    /// validator.
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#AbstractConfig")]
    pub(crate) fn new(definition: &'static ConfigDef, originals: ConfigMap, do_log: bool) -> Result<Self, Error> {
        Self::with_hooks(
            definition,
            originals,
            do_log,
            "AbstractConfig",
            |parsed| parsed,
            |_| Ok(ConfigMap::new()),
        )
    }

    /// Java's `AbstractConfig(definition, originals, configProviderProps,
    /// doLog)` with a subclass's overrides of the two hooks, without config
    /// providers.
    ///
    /// - `pre_process` is Java's `preProcessParsedConfig`: called directly
    ///   after user configs got parsed (and thus default values is not set),
    ///   it returns the map that becomes the originals.
    /// - `post_process` is Java's `postProcessParsedConfig`: called directly
    ///   after user configs got parsed (and thus default values got set), with
    ///   the configuration as parsed so far, it returns a map of updates that
    ///   should be applied to the configuration. The updated values are parsed
    ///   again to prevent bad updates. Its reads through [`Self::get`] mark the
    ///   keys used, as Java's do.
    ///
    /// `class_name` is the subclass's simple name, which [`Self::log_all`]
    /// prints.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] if a value is missing, cannot be parsed or fails its
    /// validator, before or after the updates, or the error `post_process`
    /// returns.
    pub(crate) fn with_hooks(
        definition: &'static ConfigDef,
        originals: ConfigMap,
        do_log: bool,
        class_name: &str,
        pre_process: impl FnOnce(ConfigMap) -> ConfigMap,
        post_process: impl FnOnce(&AbstractConfig) -> Result<ConfigMap, Error>,
    ) -> Result<Self, Error> {
        let originals = pre_process(originals);
        let values = definition.parse(&originals)?;
        let mut config = Self { used: Mutex::new(HashSet::new()), originals, values, definition };
        let config_updates = post_process(&config)?;
        config.values.extend(config_updates);
        definition.parse(&config.values)?;
        if do_log {
            config.log_all(class_name);
        }
        Ok(config)
    }

    /// The configuration definition.
    pub(crate) fn definition(&self) -> &'static ConfigDef {
        self.definition
    }

    /// The originals as given, without marking any key used: the crate's
    /// stand-in for Java's `originals()`, whose `RecordingMap` marks a key
    /// only when it is read with `get`.
    pub(crate) fn originals_map(&self) -> &ConfigMap {
        &self.originals
    }

    /// The parsed values, without marking any key used: the crate's stand-in
    /// for Java's `values()`.
    pub(crate) fn parsed_values(&self) -> &ConfigMap {
        &self.values
    }

    /// Java's `get(key)`: the parsed value of `key`, marking it used. `None`
    /// is Java's `null`.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] `Unknown configuration '<key>'` if `key` has no
    /// value.
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#get")]
    pub(crate) fn get(&self, key: &str) -> Result<Option<&ParsedValue>, Error> {
        let Some(value) = self.values.get(key) else {
            return Err(Error::config_message(format!("Unknown configuration '{key}'")));
        };
        self.used.lock().unwrap_or_else(PoisonError::into_inner).insert(key.to_string());
        Ok(value.as_ref())
    }

    /// Marks `key` used without reading it.
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#ignore")]
    pub(crate) fn ignore(&self, key: &str) {
        self.used.lock().unwrap_or_else(PoisonError::into_inner).insert(key.to_string());
    }

    /// Java's `getShort(key)`.
    ///
    /// # Errors
    ///
    /// As [`Self::get`]; [`Error::LocalIllegalArgument`] if the value is not
    /// a `SHORT`, where Java's cast throws a `ClassCastException`.
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#getShort")]
    pub(crate) fn get_short(&self, key: &str) -> Result<Option<i16>, Error> {
        as_short(key, self.get(key)?)
    }

    /// Java's `getInt(key)`.
    ///
    /// # Errors
    ///
    /// As [`Self::get_short`], for an `INT`.
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#getInt")]
    pub(crate) fn get_int(&self, key: &str) -> Result<Option<i32>, Error> {
        as_int(key, self.get(key)?)
    }

    /// Java's `getLong(key)`.
    ///
    /// # Errors
    ///
    /// As [`Self::get_short`], for a `LONG`.
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#getLong")]
    pub(crate) fn get_long(&self, key: &str) -> Result<Option<i64>, Error> {
        as_long(key, self.get(key)?)
    }

    /// Java's `getDouble(key)`.
    ///
    /// # Errors
    ///
    /// As [`Self::get_short`], for a `DOUBLE`.
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#getDouble")]
    pub(crate) fn get_double(&self, key: &str) -> Result<Option<f64>, Error> {
        as_double(key, self.get(key)?)
    }

    /// Java's `getList(key)`, borrowed from the parsed values.
    ///
    /// # Errors
    ///
    /// As [`Self::get_short`], for a `LIST`.
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#getList")]
    pub(crate) fn get_list(&self, key: &str) -> Result<Option<&[String]>, Error> {
        as_list(key, self.get(key)?)
    }

    /// Java's `getBoolean(key)`.
    ///
    /// # Errors
    ///
    /// As [`Self::get_short`], for a `BOOLEAN`.
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#getBoolean")]
    pub(crate) fn get_boolean(&self, key: &str) -> Result<Option<bool>, Error> {
        as_boolean(key, self.get(key)?)
    }

    /// Java's `getString(key)`: the value trimmed as Java's `String.trim()`
    /// does.
    ///
    /// # Errors
    ///
    /// As [`Self::get_short`], for a `STRING`; a set `PASSWORD` is not a
    /// `STRING`, an unset one (`null`) answers `None`, as Java's cast of
    /// `null` succeeds.
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#getString")]
    pub(crate) fn get_string(&self, key: &str) -> Result<Option<&str>, Error> {
        as_string(key, self.get(key)?)
    }

    /// Java's `getPassword(key)`.
    ///
    /// # Errors
    ///
    /// As [`Self::get_short`], for a `PASSWORD`.
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#getPassword")]
    pub(crate) fn get_password(&self, key: &str) -> Result<Option<&Password>, Error> {
        as_password(key, self.get(key)?)
    }

    /// Java's `documentationOf(key)`: the documentation of `key`, `None` when
    /// it is not defined.
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#documentationOf")]
    pub(crate) fn documentation_of(&self, key: &str) -> Option<&'static str> {
        self.definition.config_key(key).map(|k| k.documentation.as_str())
    }

    /// The keys of the originals that were never read: Java's `unused()`.
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#unused")]
    pub(crate) fn unused(&self) -> HashSet<String> {
        let used = self.used.lock().unwrap_or_else(PoisonError::into_inner);
        self.originals.keys().filter(|k| !used.contains(*k)).cloned().collect()
    }

    /// Info level log for any unused configurations: Java's `logUnused()`.
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#logUnused")]
    pub(crate) fn log_unused(&self) {
        if let Some(message) = self.unused_message() {
            log::info!("{message}");
        }
    }

    /// The line [`Self::log_unused`] logs, `None` when every key was used.
    ///
    /// The keys print as Java prints the `HashSet` `unused()` returns: it
    /// copies `originals.keySet()` with `new HashSet<>(keys)`, sized to
    /// `max(n / 0.75 + 1, 16)` buckets rounded up to a power of two, and
    /// iterates by bucket. Within a bucket the keys print in key order, where
    /// Java prints them in the originals' own iteration order.
    fn unused_message(&self) -> Option<String> {
        let unused = self.unused();
        if unused.is_empty() {
            return None;
        }
        let capacity = ((self.originals.len() * 4 / 3) + 1).max(16).next_power_of_two();
        let mut keys: Vec<String> = unused.into_iter().collect();
        keys.sort();
        let keys = java_hash_table_order(keys, capacity);
        Some(format!(
            "These configurations '[{}]' were supplied but are not used yet.",
            keys.join(", ")
        ))
    }

    /// Java's private `logAll()`: logs every parsed value at INFO, sorted by
    /// key, `class_name` being Java's `getClass().getSimpleName()`. A
    /// `PASSWORD` prints as `[hidden]`. A client calls it when it is built,
    /// where Java's client builds its configuration with `doLog = true`.
    pub(crate) fn log_all(&self, class_name: &str) {
        log::info!("{}", self.all_message(class_name));
    }

    /// The text [`Self::log_all`] logs.
    fn all_message(&self, class_name: &str) -> String {
        let mut entries: Vec<(&String, &Option<ParsedValue>)> = self.values.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        let mut b = format!("{class_name} values: \n");
        for (key, value) in entries {
            b.push('\t');
            b.push_str(key);
            b.push_str(" = ");
            match value {
                Some(v) => b.push_str(&v.to_string()),
                None => b.push_str("null"),
            }
            b.push('\n');
        }
        b
    }
}

/// The `ClassCastException` stand-in of a typed getter: the same message the
/// consumer's `get_string` already returns.
fn wrong_type(key: &str, value: &ParsedValue, expected: Type) -> Error {
    Error::local_illegal_argument(format!(
        "Configuration '{key}' is of type {}, not {expected}",
        value.config_type()
    ))
}

/// The `SHORT` value of `key`, as Java's cast of `get(key)` to its boxed type.
fn as_short(key: &str, value: Option<&ParsedValue>) -> Result<Option<i16>, Error> {
    match value {
        None => Ok(None),
        Some(ParsedValue::Short(v)) => Ok(Some(*v)),
        Some(other) => Err(wrong_type(key, other, Type::Short)),
    }
}

/// The `INT` value of `key`, as Java's cast of `get(key)` to its boxed type.
fn as_int(key: &str, value: Option<&ParsedValue>) -> Result<Option<i32>, Error> {
    match value {
        None => Ok(None),
        Some(ParsedValue::Int(v)) => Ok(Some(*v)),
        Some(other) => Err(wrong_type(key, other, Type::Int)),
    }
}

/// The `LONG` value of `key`, as Java's cast of `get(key)` to its boxed type.
fn as_long(key: &str, value: Option<&ParsedValue>) -> Result<Option<i64>, Error> {
    match value {
        None => Ok(None),
        Some(ParsedValue::Long(v)) => Ok(Some(*v)),
        Some(other) => Err(wrong_type(key, other, Type::Long)),
    }
}

/// The `DOUBLE` value of `key`, as Java's cast of `get(key)` to its boxed type.
fn as_double(key: &str, value: Option<&ParsedValue>) -> Result<Option<f64>, Error> {
    match value {
        None => Ok(None),
        Some(ParsedValue::Double(v)) => Ok(Some(*v)),
        Some(other) => Err(wrong_type(key, other, Type::Double)),
    }
}

/// The `LIST` value of `key`, as Java's cast of `get(key)` to its boxed type.
fn as_list<'a>(key: &str, value: Option<&'a ParsedValue>) -> Result<Option<&'a [String]>, Error> {
    match value {
        None => Ok(None),
        Some(ParsedValue::List(v)) => Ok(Some(v)),
        Some(other) => Err(wrong_type(key, other, Type::List)),
    }
}

/// The `BOOLEAN` value of `key`, as Java's cast of `get(key)` to its boxed type.
fn as_boolean(key: &str, value: Option<&ParsedValue>) -> Result<Option<bool>, Error> {
    match value {
        None => Ok(None),
        Some(ParsedValue::Boolean(v)) => Ok(Some(*v)),
        Some(other) => Err(wrong_type(key, other, Type::Boolean)),
    }
}

/// The `STRING` value, trimmed, of `key`, as Java's cast of `get(key)` to its boxed type.
fn as_string<'a>(key: &str, value: Option<&'a ParsedValue>) -> Result<Option<&'a str>, Error> {
    match value {
        None => Ok(None),
        Some(ParsedValue::String(v)) => Ok(Some(ConfigDef::trim(v))),
        Some(other) => Err(wrong_type(key, other, Type::String)),
    }
}

/// The `PASSWORD` value of `key`, as Java's cast of `get(key)` to its boxed type.
fn as_password<'a>(key: &str, value: Option<&'a ParsedValue>) -> Result<Option<&'a Password>, Error> {
    match value {
        None => Ok(None),
        Some(ParsedValue::Password(v)) => Ok(Some(v)),
        Some(other) => Err(wrong_type(key, other, Type::Password)),
    }
}
/// Reads a client configuration's parsed values into its typed fields,
/// recording each key it reads.
///
/// Not a Java class (DoD #7). Java's clients read their configuration through
/// `AbstractConfig`'s getters while they build themselves, which marks each
/// key used. The Rust configs read the parsed values once, into the typed
/// fields the clients use, when the configuration is built; marking those keys
/// then would make [`AbstractConfig::unused`] on a configuration the user
/// holds answer as if a client had read it. So the reader records the keys
/// instead, and the client marks them ([`AbstractConfig::ignore`]) when it is
/// built from the configuration, where Java's would read them. The reads and
/// the record are one call, so the fields and the marking cannot drift.
pub(crate) struct ValuesReader<'a> {
    config: &'a AbstractConfig,
    read: Vec<&'static str>,
}

impl<'a> ValuesReader<'a> {
    /// A reader over `config`'s parsed values, with no key read yet.
    pub(crate) fn new(config: &'a AbstractConfig) -> Self {
        Self { config, read: Vec::new() }
    }

    /// The value of `key`, recorded as read and not marked used.
    fn value(&mut self, key: &'static str) -> Result<Option<&'a ParsedValue>, Error> {
        let Some(value) = self.config.values.get(key) else {
            return Err(Error::config_message(format!("Unknown configuration '{key}'")));
        };
        self.read.push(key);
        Ok(value.as_ref())
    }

    /// The `INT` value of `key`, which its definition never leaves `null`.
    pub(crate) fn int(&mut self, key: &'static str) -> Result<i32, Error> {
        let value = self.value(key)?;
        non_null(key, as_int(key, value)?)
    }

    /// The `LONG` value of `key`, which its definition never leaves `null`.
    pub(crate) fn long(&mut self, key: &'static str) -> Result<i64, Error> {
        let value = self.value(key)?;
        non_null(key, as_long(key, value)?)
    }

    /// The `BOOLEAN` value of `key`, which its definition never leaves `null`.
    pub(crate) fn boolean(&mut self, key: &'static str) -> Result<bool, Error> {
        let value = self.value(key)?;
        non_null(key, as_boolean(key, value)?)
    }

    /// The `STRING` value of `key`, trimmed, `None` for `null`.
    pub(crate) fn string_or_null(&mut self, key: &'static str) -> Result<Option<&'a str>, Error> {
        let value = self.value(key)?;
        as_string(key, value)
    }

    /// The `STRING` value of `key`, trimmed, which its definition never leaves
    /// `null`.
    pub(crate) fn string(&mut self, key: &'static str) -> Result<&'a str, Error> {
        let value = self.string_or_null(key)?;
        non_null(key, value)
    }

    /// The `LIST` value of `key`, `None` for `null`.
    pub(crate) fn list_or_null(&mut self, key: &'static str) -> Result<Option<&'a [String]>, Error> {
        let value = self.value(key)?;
        as_list(key, value)
    }

    /// The `LIST` value of `key`, which its definition never leaves `null`.
    pub(crate) fn list(&mut self, key: &'static str) -> Result<&'a [String], Error> {
        let value = self.list_or_null(key)?;
        non_null(key, value)
    }

    /// The `PASSWORD` value of `key`, `None` for `null`.
    pub(crate) fn password_or_null(&mut self, key: &'static str) -> Result<Option<&'a Password>, Error> {
        let value = self.value(key)?;
        as_password(key, value)
    }

    /// Moves the keys `other` read into this reader's record.
    pub(crate) fn append(&mut self, other: ValuesReader<'_>) {
        self.read.extend(other.read);
    }

    /// The keys read, in the order they were read.
    pub(crate) fn into_read_keys(self) -> Vec<&'static str> {
        self.read
    }
}

/// A value its definition never leaves `null`: a non-null default and, for a
/// value given, a parse that never yields `null`. `null` would be a defect of
/// the definition, not of the user's input.
fn non_null<T>(key: &str, value: Option<T>) -> Result<T, Error> {
    value.ok_or_else(|| Error::local_illegal_state(format!("Configuration '{key}' has no value")))
}

impl Clone for AbstractConfig {
    /// A copy with its own used set, starting as a copy of this one's.
    fn clone(&self) -> Self {
        Self {
            used: Mutex::new(self.used.lock().unwrap_or_else(PoisonError::into_inner).clone()),
            originals: self.originals.clone(),
            values: self.values.clone(),
            definition: self.definition,
        }
    }
}

impl PartialEq for AbstractConfig {
    /// Java's `equals`: the originals are equal.
    fn eq(&self, other: &Self) -> bool {
        self.originals == other.originals
    }
}

impl Eq for AbstractConfig {}

impl Hash for AbstractConfig {
    /// Java's `hashCode`, over the originals; the entries are hashed in key
    /// order, `HashMap` having no `Hash` of its own.
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut entries: Vec<(&String, &Option<ParsedValue>)> = self.originals.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        entries.hash(state);
    }
}

impl fmt::Debug for AbstractConfig {
    /// The keys of the originals only, sorted: a value may be a secret
    /// that a `PASSWORD` key does not cover (for example an unknown key).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut keys: Vec<&String> = self.originals.keys().collect();
        keys.sort();
        f.debug_struct("AbstractConfig")
            .field("originals", &keys)
            .finish_non_exhaustive()
    }
}

/// Java's originals for a client built from a string map: every value is a
/// `String`.
pub(crate) fn string_originals(props: &HashMap<String, String>) -> ConfigMap {
    props
        .iter()
        .map(|(k, v)| (k.clone(), Some(ParsedValue::String(v.clone()))))
        .collect()
}

/// Translated from `AbstractConfigTest`.
///
/// Not translated, with the reason:
///
/// - `testConfiguredInstances`, `testConfiguredInstancesClosedOnFailure`,
///   `testClassConfigs`: they need class-valued configs, which this crate does
///   not take (CLAUDE.md §2). `testUnusedConfigs` reads the extra config with
///   `get` instead of through a configured reporter's `configure`.
/// - `testOriginalsWithPrefix`, `testValuesWithPrefixOverride`,
///   `testValuesWithSecondaryPrefix`, `testValuesWithPrefixAllOrNothing`,
///   `testOriginalWithOverrides`: `originals*()` / `values*()` are deferred
///   (Java's `RecordingMap`).
/// - `testOriginalsWithConfigProvidersProps*`, `testConfigProvidersPropsAsParam`,
///   `testAutomaticConfigProvidersWithFullClassName`,
///   `testImmutableOriginalsWithConfigProvidersProps`,
///   `testAutoConfigResolution*`, `testConfigProviderConfigurationWithConfigParams`:
///   config providers are not translated.
#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use super::*;
    use crate::common::config::config_def::{DefaultValue, Importance, NonNullValidator};

    fn map(entries: &[(&str, ParsedValue)]) -> ConfigMap {
        entries.iter().map(|(k, v)| ((*k).to_string(), Some(v.clone()))).collect()
    }

    fn config_error_message<T: fmt::Debug>(result: Result<T, Error>) -> String {
        match result {
            Err(Error::Config(e)) => e.message().to_string(),
            other => panic!("expected a ConfigError, got {other:?}"),
        }
    }

    static EMPTY_LIST_DEF: LazyLock<ConfigDef> = LazyLock::new(|| {
        ConfigDef::new().define_with_validator("a", Type::List, "", NonNullValidator, Importance::High, "doc")
    });

    #[test]
    fn test_empty_list() {
        let conf = AbstractConfig::new(&EMPTY_LIST_DEF, ConfigMap::new(), true).unwrap();
        assert_eq!(conf.get_list("a").unwrap(), Some(&[] as &[String]));

        let conf = AbstractConfig::new(&EMPTY_LIST_DEF, map(&[("a", "".into())]), true).unwrap();
        assert_eq!(conf.get_list("a").unwrap(), Some(&[] as &[String]));

        let conf = AbstractConfig::new(&EMPTY_LIST_DEF, map(&[("a", "b,c,d".into())]), true).unwrap();
        assert_eq!(conf.get_list("a").unwrap().unwrap(), ["b", "c", "d"]);
    }

    const METRIC_REPORTER_CLASSES_CONFIG: &str = "metric.reporters";
    const PREPROCESSOR_CONFIG: &str = "preprocess";
    const EXTRA_CONFIG: &str = "metric.extra_config";

    static TEST_CONFIG_DEF: LazyLock<ConfigDef> = LazyLock::new(|| {
        ConfigDef::new()
            .define(
                METRIC_REPORTER_CLASSES_CONFIG,
                Type::List,
                "",
                Importance::Low,
                "A list of classes to use as metrics reporters.",
            )
            .define(
                PREPROCESSOR_CONFIG,
                Type::String,
                "",
                Importance::Low,
                "Override from preprocess step.",
            )
    });

    /// Java's `TestConfig`, whose `preProcessParsedConfig` sets `preprocess`.
    fn test_config(props: ConfigMap) -> AbstractConfig {
        AbstractConfig::with_hooks(
            &TEST_CONFIG_DEF,
            props,
            true,
            "TestConfig",
            |mut parsed| {
                parsed.insert("preprocess".to_string(), Some("success".into()));
                parsed
            },
            |_| Ok(ConfigMap::new()),
        )
        .unwrap()
    }

    #[test]
    fn test_preprocess_config() {
        let config = test_config(ConfigMap::new());
        assert_eq!(
            config.get(PREPROCESSOR_CONFIG).unwrap(),
            Some(&ParsedValue::String("success".into()))
        );
    }

    #[test]
    fn test_unused_configs() {
        let config = test_config(map(&[
            (METRIC_REPORTER_CLASSES_CONFIG, "".into()),
            (EXTRA_CONFIG, "my_value".into()),
        ]));
        assert!(
            config.unused().contains(EXTRA_CONFIG),
            "{EXTRA_CONFIG} should be marked unused before it is read"
        );
        // Java reads it through the reporter's `configure`, from a
        // `RecordingMap`; a read is a read.
        config.ignore(EXTRA_CONFIG);
        assert!(
            !config.unused().contains(EXTRA_CONFIG),
            "{EXTRA_CONFIG} should be marked as used"
        );
    }

    const INDIRECT_CONFIGS: &str = "indirect.variables";
    const INDIRECT_CONFIGS_DOC: &str = "Variables whose values can be obtained from ConfigProviders";

    static INDIRECT_DEF: LazyLock<ConfigDef> = LazyLock::new(|| {
        ConfigDef::new().define(INDIRECT_CONFIGS, Type::List, "", Importance::Low, INDIRECT_CONFIGS_DOC)
    });

    #[test]
    fn test_documentation_of() {
        let config = AbstractConfig::new(&INDIRECT_DEF, ConfigMap::new(), true).unwrap();
        assert_eq!(config.documentation_of(INDIRECT_CONFIGS), Some(INDIRECT_CONFIGS_DOC));
    }

    #[test]
    fn test_documentation_of_expect_null() {
        let config = AbstractConfig::new(&INDIRECT_DEF, ConfigMap::new(), true).unwrap();
        assert_eq!(config.documentation_of("xyz"), None);
        assert!(std::ptr::eq(config.definition(), &*INDIRECT_DEF));
    }

    static TYPED_DEF: LazyLock<ConfigDef> = LazyLock::new(|| {
        ConfigDef::new()
            .define("short", Type::Short, 1i16, Importance::High, "docs")
            .define("int", Type::Int, 2, Importance::High, "docs")
            .define("long", Type::Long, 3i64, Importance::High, "docs")
            .define("double", Type::Double, 4.5, Importance::High, "docs")
            .define("boolean", Type::Boolean, true, Importance::High, "docs")
            .define("string", Type::String, " s ", Importance::High, "docs")
            .define("list", Type::List, "a, b", Importance::High, "docs")
            .define("password", Type::Password, DefaultValue::Null, Importance::High, "docs")
            .define("null.int", Type::Int, DefaultValue::Null, Importance::High, "docs")
    });

    /// Each typed getter answers its type, Java's `null` as `None`, and marks
    /// the key used.
    #[test]
    fn test_typed_getters() {
        let config = AbstractConfig::new(&TYPED_DEF, map(&[("x", "y".into())]), false).unwrap();
        assert_eq!(config.get_short("short").unwrap(), Some(1));
        assert_eq!(config.get_int("int").unwrap(), Some(2));
        assert_eq!(config.get_long("long").unwrap(), Some(3));
        assert_eq!(config.get_double("double").unwrap(), Some(4.5));
        assert_eq!(config.get_boolean("boolean").unwrap(), Some(true));
        // The default was trimmed by `parseType`; `getString` trims again.
        assert_eq!(config.get_string("string").unwrap(), Some("s"));
        assert_eq!(config.get_list("list").unwrap().unwrap(), ["a", "b"]);
        assert_eq!(config.get_password("password").unwrap(), None);
        assert_eq!(config.get_int("null.int").unwrap(), None);
        // An unset `PASSWORD` is `null`, which casts to `String`.
        assert_eq!(config.get_string("password").unwrap(), None);
        // `null` casts to every class.
        assert_eq!(config.get_string("null.int").unwrap(), None);
        // Only the original `x` was supplied, and it was never read.
        assert_eq!(config.unused(), HashSet::from(["x".to_string()]));
    }

    /// `get` fails with Java's message for a key without a value, and a cast
    /// to another class fails as the consumer's `get_string` does.
    #[test]
    fn test_getter_errors() {
        let config = AbstractConfig::new(&TYPED_DEF, map(&[("password", "secret".into())]), false).unwrap();
        assert_eq!(config_error_message(config.get_int("xyz")), "Unknown configuration 'xyz'");
        let err = config.get_string("int").unwrap_err();
        assert!(err.is_local_illegal_argument_error(), "{err:?}");
        assert_eq!(
            err.to_string(),
            "LocalIllegalArgumentError: Configuration 'int' is of type INT, not STRING"
        );
        let err = config.get_string("password").unwrap_err();
        assert!(
            err.to_string()
                .ends_with("Configuration 'password' is of type PASSWORD, not STRING"),
            "{err}"
        );
        assert_eq!(config.get_password("password").unwrap(), Some(&Password::new("secret")));
        assert!(config.get_long("int").is_err());
        assert!(config.get_short("int").is_err());
        assert!(config.get_double("int").is_err());
        assert!(config.get_boolean("int").is_err());
        assert!(config.get_list("int").is_err());
        assert!(config.get_int("long").is_err());
        assert!(config.get_password("string").is_err());
    }

    /// `logUnused`'s line lists the unread originals as Java prints the set.
    #[test]
    fn test_log_unused_message() {
        let config = AbstractConfig::new(
            &TYPED_DEF,
            map(&[
                ("int", "7".into()),
                ("b.unknown", "1".into()),
                ("a.unknown", "2".into()),
            ]),
            false,
        )
        .unwrap();
        config.ignore("int");
        assert_eq!(
            config.unused_message().unwrap(),
            // Java's `HashSet` order: `b.unknown` hashes to bucket 0,
            // `a.unknown` to bucket 4.
            "These configurations '[b.unknown, a.unknown]' were supplied but are not used yet."
        );
        config.ignore("a.unknown");
        config.ignore("b.unknown");
        assert_eq!(config.unused_message(), None);
        config.log_unused();
    }

    /// `logAll` prints the values sorted by key, with passwords hidden.
    #[test]
    fn test_log_all_message() {
        let config = AbstractConfig::new(&TYPED_DEF, map(&[("password", "secret".into())]), false).unwrap();
        assert_eq!(
            config.all_message("TestConfig"),
            "TestConfig values: \n\tboolean = true\n\tdouble = 4.5\n\tint = 2\n\tlist = [a, b]\n\tlong = 3\n\tnull.int \
             = null\n\tpassword = [hidden]\n\tshort = 1\n\tstring = s\n"
        );
    }

    static POST_DEF: LazyLock<ConfigDef> = LazyLock::new(|| {
        ConfigDef::new()
            .define("a", Type::Int, 1, Importance::High, "docs")
            .define_with_validator(
                "b",
                Type::Int,
                1,
                crate::common::config::config_def::Range::at_least(0),
                Importance::High,
                "docs",
            )
    });

    /// The post-process updates are applied and validated by a second parse;
    /// the hook's reads mark keys used.
    #[test]
    fn test_post_process_parsed_config() {
        let config = AbstractConfig::with_hooks(
            &POST_DEF,
            map(&[("a", "5".into())]),
            false,
            "TestConfig",
            |parsed| parsed,
            |config| {
                let a = config.get_int("a")?.unwrap_or_default();
                Ok(ConfigMap::from([("b".to_string(), Some(ParsedValue::Int(a * 2)))]))
            },
        )
        .unwrap();
        assert_eq!(config.get_int("b").unwrap(), Some(10));
        assert!(config.unused().is_empty());

        let err = AbstractConfig::with_hooks(
            &POST_DEF,
            ConfigMap::new(),
            false,
            "TestConfig",
            |parsed| parsed,
            |_| Ok(ConfigMap::from([("b".to_string(), Some(ParsedValue::Int(-1)))])),
        )
        .unwrap_err();
        assert_eq!(
            config_error_message::<()>(Err(err)),
            "Invalid value -1 for configuration b: Value must be at least 0"
        );
    }

    /// Java's `equals` / `hashCode` compare the originals only; a clone copies
    /// the used set without sharing it.
    #[test]
    fn test_equals_hash_and_clone() {
        use std::collections::hash_map::DefaultHasher;
        let hash = |c: &AbstractConfig| {
            let mut h = DefaultHasher::new();
            c.hash(&mut h);
            h.finish()
        };
        let a = AbstractConfig::new(&TYPED_DEF, map(&[("int", "7".into()), ("x", "1".into())]), false).unwrap();
        let b = AbstractConfig::new(&TYPED_DEF, map(&[("x", "1".into()), ("int", "7".into())]), false).unwrap();
        let c = AbstractConfig::new(&TYPED_DEF, map(&[("int", "8".into())]), false).unwrap();
        assert_eq!(a, b);
        assert_eq!(hash(&a), hash(&b));
        assert_ne!(a, c);

        a.ignore("x");
        let clone = a.clone();
        assert_eq!(clone.unused(), HashSet::from(["int".to_string()]));
        clone.ignore("int");
        assert_eq!(a.unused(), HashSet::from(["int".to_string()]));
        assert_eq!(format!("{a:?}"), "AbstractConfig { originals: [\"int\", \"x\"], .. }");
    }
}
