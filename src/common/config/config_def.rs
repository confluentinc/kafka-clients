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

//! Translation of `org.apache.kafka.common.config.ConfigDef` (subset used by
//! `ProducerConfig`).
//!
//! Java's `ConfigDef` is a 1700-line schema-validation framework with
//! recommenders, dependents, dynamic visibility, and Connect-style group
//! ordering — most of which the Rust producer does not consume. We translate
//! the slice that `ProducerConfig` actually exercises:
//!
//! * Type-tagged values (`Boolean`, `String`, `Int`, `Short`, `Long`,
//!   `Double`, `List`, `Class`, `Password`).
//! * Importance levels.
//! * Validators (`Range::at_least`, `Range::between`, `ValidString::in_set`,
//!   `NonNullValidator`).
//! * `define(name, type, default, validator, importance, doc)`.
//! * `parse(map)` returning a typed-value map.
//!
//! Skipped (translated lazily as later phases need them):
//! * `Recommender` interface.
//! * `Dependents`, `Width`, `displayName`, `group`, `orderInGroup`.
//! * `parseSslKeyValueAndExpand`.
//! * `validate(props)` (separate `ConfigValue` walk).
//! * Programmatic doc generation (`toHtml`, `toRst`).
//!
//! Adding any of these later is a non-breaking extension.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use crate::common::config::config_exception;
use crate::common::errors::KafkaError;

// Java's `ConfigDef.NO_DEFAULT_VALUE` sentinel. We use `Option::None` in
// `ConfigKey::default_value` instead of carrying a dedicated singleton.

/// All supported config types. Mirrors `ConfigDef.Type`.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Type {
    /// `boolean`.
    Boolean,
    /// `string`.
    String,
    /// `int`.
    Int,
    /// `short`.
    Short,
    /// `long`.
    Long,
    /// `double`.
    Double,
    /// Comma-separated `List<String>`.
    List,
    /// `Class<?>` — translated as a `String` carrying the fully-qualified
    /// class name. We do not load classes reflectively in Rust; downstream
    /// callers convert this string to a Rust type via `match`.
    Class,
    /// `Password` — string with masked `Display`.
    Password,
}

/// Importance level. Mirrors `ConfigDef.Importance`.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Importance {
    Low,
    Medium,
    High,
}

/// Typed value of a parsed configuration entry.
#[derive(Clone, Debug, PartialEq)]
pub enum ConfigValue {
    Boolean(bool),
    String(String),
    Int(i32),
    Short(i16),
    Long(i64),
    Double(f64),
    List(Vec<String>),
    Class(String),
    /// A password value. Wrapping in [`Password`] marks the value as
    /// sensitive (`Display` redacts it).
    Password(Password),
    /// Equivalent to Java's `null` value for an explicitly-defined config
    /// with no default. Distinct from "not present" (which is filtered out
    /// before `parse` is consulted).
    Null,
}

impl ConfigValue {
    /// Try to extract this value as a bool. Returns `None` if the variant is
    /// not [`ConfigValue::Boolean`].
    pub fn as_bool(&self) -> Option<bool> {
        if let ConfigValue::Boolean(b) = self {
            Some(*b)
        } else {
            None
        }
    }

    /// Try to extract this value as a string slice. Works on `String`,
    /// `Class`, and `Password`.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            ConfigValue::String(s) | ConfigValue::Class(s) => Some(s),
            ConfigValue::Password(p) => Some(p.value()),
            _ => None,
        }
    }

    /// Try to extract this value as an `i32`.
    pub fn as_i32(&self) -> Option<i32> {
        if let ConfigValue::Int(v) = self { Some(*v) } else { None }
    }

    /// Try to extract this value as an `i16`.
    pub fn as_i16(&self) -> Option<i16> {
        if let ConfigValue::Short(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    /// Try to extract this value as an `i64`.
    pub fn as_i64(&self) -> Option<i64> {
        if let ConfigValue::Long(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    /// Try to extract this value as an `f64`.
    pub fn as_f64(&self) -> Option<f64> {
        if let ConfigValue::Double(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    /// Try to extract this value as a `Vec<String>`.
    pub fn as_list(&self) -> Option<&[String]> {
        if let ConfigValue::List(v) = self { Some(v) } else { None }
    }
}

impl fmt::Display for ConfigValue {
    /// Mirrors Java's `Object.toString()` semantics for each variant so that
    /// `ConfigException` messages match the Java client's text exactly. In
    /// particular, [`ConfigValue::String`] / [`ConfigValue::Class`] print the
    /// raw value without quotes (Java's `String.toString()` is the string
    /// itself, not a `Debug`-quoted form), and [`ConfigValue::List`] mirrors
    /// Java's `AbstractCollection.toString()` (`[a, b, c]`).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigValue::Boolean(b) => write!(f, "{b}"),
            ConfigValue::String(s) | ConfigValue::Class(s) => f.write_str(s),
            ConfigValue::Int(v) => write!(f, "{v}"),
            ConfigValue::Short(v) => write!(f, "{v}"),
            ConfigValue::Long(v) => write!(f, "{v}"),
            ConfigValue::Double(v) => write!(f, "{v}"),
            ConfigValue::List(items) => {
                f.write_str("[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    f.write_str(item)?;
                }
                f.write_str("]")
            },
            ConfigValue::Password(p) => fmt::Display::fmt(p, f),
            ConfigValue::Null => f.write_str("null"),
        }
    }
}

/// Sensitive string value. Mirrors `org.apache.kafka.common.config.types.Password`.
/// `Display` returns `[hidden]` so the value is never accidentally logged.
#[derive(Clone, PartialEq, Eq)]
pub struct Password(String);

impl Password {
    pub const HIDDEN: &'static str = "[hidden]";

    pub fn new<S: Into<String>>(value: S) -> Self {
        Password(value.into())
    }

    pub fn value(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Password {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(Self::HIDDEN)
    }
}

impl fmt::Debug for Password {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(Self::HIDDEN)
    }
}

/// Validator trait. Mirrors `ConfigDef.Validator`.
///
/// Implementations return `Err` (with the same message text Java uses) when
/// the value is invalid. The error wraps `KafkaError::Config` for parity with
/// Java's `ConfigException`.
pub trait Validator: Send + Sync + std::fmt::Debug {
    fn ensure_valid(&self, name: &str, value: &ConfigValue) -> Result<(), KafkaError>;
    /// Human-readable description for diagnostics. Mirrors `Validator.toString`.
    fn description(&self) -> String;
}

/// Validator that requires a numeric value to lie in `[lower, upper]` (both
/// optional). Mirrors `ConfigDef.Range`.
#[derive(Debug)]
pub struct Range {
    lower: Option<f64>,
    upper: Option<f64>,
}

impl Range {
    /// `Range.atLeast(min)` — inclusive lower bound, no upper bound.
    pub fn at_least<N: Into<f64>>(min: N) -> Self {
        Range { lower: Some(min.into()), upper: None }
    }

    /// `Range.between(min, max)` — inclusive on both ends.
    pub fn between<N: Into<f64>, M: Into<f64>>(min: N, max: M) -> Self {
        Range { lower: Some(min.into()), upper: Some(max.into()) }
    }

    fn as_f64(value: &ConfigValue) -> Option<f64> {
        match value {
            ConfigValue::Int(v) => Some(*v as f64),
            ConfigValue::Short(v) => Some(*v as f64),
            ConfigValue::Long(v) => Some(*v as f64),
            ConfigValue::Double(v) => Some(*v),
            _ => None,
        }
    }
}

impl Validator for Range {
    fn ensure_valid(&self, name: &str, value: &ConfigValue) -> Result<(), KafkaError> {
        let v = Range::as_f64(value)
            .ok_or_else(|| config_exception::new(name, value, "Value must be numeric to validate against a Range"))?;
        if let Some(lo) = self.lower
            && v < lo
        {
            return Err(config_exception::new(name, value, &format!("Value must be at least {lo}")));
        }
        if let Some(hi) = self.upper
            && v > hi
        {
            return Err(config_exception::new(name, value, &format!("Value must be no more than {hi}")));
        }
        Ok(())
    }

    fn description(&self) -> String {
        match (self.lower, self.upper) {
            (Some(lo), None) => format!("[{lo},...]"),
            (None, Some(hi)) => format!("[...,{hi}]"),
            (Some(lo), Some(hi)) => format!("[{lo},...,{hi}]"),
            (None, None) => "[any]".to_owned(),
        }
    }
}

/// Validator that constrains a string to a fixed set. Mirrors
/// `ConfigDef.ValidString`.
#[derive(Debug)]
pub struct ValidString {
    valid_values: Vec<String>,
}

impl ValidString {
    /// `ConfigDef.ValidString.in(list)`.
    pub fn in_set<I, S>(values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        ValidString { valid_values: values.into_iter().map(Into::into).collect() }
    }
}

impl Validator for ValidString {
    fn ensure_valid(&self, name: &str, value: &ConfigValue) -> Result<(), KafkaError> {
        let s = value
            .as_str()
            .ok_or_else(|| config_exception::new(name, value, "Value must be a string for ValidString validator"))?;
        if self.valid_values.iter().any(|v| v == s) {
            Ok(())
        } else {
            Err(config_exception::new(
                name,
                value,
                &format!("String must be one of: {}", self.valid_values.join(", ")),
            ))
        }
    }

    fn description(&self) -> String {
        format!("[{}]", self.valid_values.join(", "))
    }
}

/// Validator that rejects `null` (i.e. [`ConfigValue::Null`]). Mirrors
/// `ConfigDef.NonNullValidator`.
#[derive(Debug)]
pub struct NonNullValidator;

impl Validator for NonNullValidator {
    fn ensure_valid(&self, name: &str, value: &ConfigValue) -> Result<(), KafkaError> {
        if matches!(value, ConfigValue::Null) {
            Err(config_exception::new(name, value, "entry must be non null"))
        } else {
            Ok(())
        }
    }

    fn description(&self) -> String {
        "non-null string".to_owned()
    }
}

/// Validator that constrains a string to a fixed set, case-insensitively.
/// Mirrors `ConfigDef.CaseInsensitiveValidString`.
#[derive(Debug)]
pub struct CaseInsensitiveValidString {
    /// Original (mixed-case) values for the diagnostic message.
    valid_values: Vec<String>,
    /// Pre-uppercased values for the membership check.
    valid_values_upper: Vec<String>,
}

impl CaseInsensitiveValidString {
    /// `ConfigDef.CaseInsensitiveValidString.in(...)`.
    pub fn in_set<I, S>(values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let valid_values: Vec<String> = values.into_iter().map(Into::into).collect();
        let valid_values_upper = valid_values.iter().map(|s| s.to_ascii_uppercase()).collect();
        CaseInsensitiveValidString { valid_values, valid_values_upper }
    }
}

impl Validator for CaseInsensitiveValidString {
    fn ensure_valid(&self, name: &str, value: &ConfigValue) -> Result<(), KafkaError> {
        // Java treats `null` (here [`ConfigValue::Null`]) as an invalid value
        // because it cannot be a member of any set. The original Java
        // throws `ConfigException(name, null, ...)` and the message text is
        // shown below.
        let s = match value {
            ConfigValue::Null => {
                return Err(config_exception::new(
                    name,
                    value,
                    &format!("String must be one of (case insensitive): {}", self.valid_values.join(", ")),
                ));
            },
            _ => value.as_str().ok_or_else(|| {
                config_exception::new(name, value, "Value must be a string for CaseInsensitiveValidString validator")
            })?,
        };
        let upper = s.to_ascii_uppercase();
        if self.valid_values_upper.iter().any(|v| v == &upper) {
            Ok(())
        } else {
            Err(config_exception::new(
                name,
                value,
                &format!("String must be one of (case insensitive): {}", self.valid_values.join(", ")),
            ))
        }
    }

    fn description(&self) -> String {
        format!("(case insensitive) [{}]", self.valid_values.join(", "))
    }
}

/// Validator that rejects empty strings. `null` (i.e. [`ConfigValue::Null`])
/// is allowed by this validator — Java's check is `s != null && s.isEmpty()`,
/// so callers must pair it with [`NonNullValidator`] when null must also
/// be rejected. Mirrors `ConfigDef.NonEmptyString`.
#[derive(Debug)]
pub struct NonEmptyString;

impl Validator for NonEmptyString {
    fn ensure_valid(&self, name: &str, value: &ConfigValue) -> Result<(), KafkaError> {
        match value {
            ConfigValue::Null => Ok(()),
            _ => {
                let s = value.as_str().ok_or_else(|| {
                    config_exception::new(name, value, "Value must be a string for NonEmptyString validator")
                })?;
                if s.is_empty() {
                    Err(config_exception::new(name, value, "String must be non-empty"))
                } else {
                    Ok(())
                }
            },
        }
    }

    fn description(&self) -> String {
        "non-empty string".to_owned()
    }
}

/// Validator for [`Type::List`] config values. Mirrors `ConfigDef.ValidList`.
///
/// Constructed via [`ValidList::any_non_duplicate_values`] (the producer's
/// usage) which permits any string value but rejects duplicates and
/// (optionally) emptiness/nullness.
#[derive(Debug)]
pub struct ValidList {
    /// Allowed values; if empty, any string is permitted (Java's
    /// `anyNonDuplicateValues`). When non-empty, each entry must match one
    /// of these strings.
    valid_strings: Vec<String>,
    is_empty_allowed: bool,
    is_null_allowed: bool,
}

impl ValidList {
    /// `ConfigDef.ValidList.anyNonDuplicateValues(isEmptyAllowed,
    /// isNullAllowed)`. Permits any string value; rejects duplicates and
    /// (depending on the flags) empty / null lists.
    pub fn any_non_duplicate_values(is_empty_allowed: bool, is_null_allowed: bool) -> Self {
        ValidList { valid_strings: Vec::new(), is_empty_allowed, is_null_allowed }
    }

    /// `ConfigDef.ValidList.in(String...)` — only the given strings are
    /// permitted. Java derives `isEmptyAllowed=true, isNullAllowed=false`.
    pub fn in_set<I, S>(values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        ValidList {
            valid_strings: values.into_iter().map(Into::into).collect(),
            is_empty_allowed: true,
            is_null_allowed: false,
        }
    }
}

impl Validator for ValidList {
    fn ensure_valid(&self, name: &str, value: &ConfigValue) -> Result<(), KafkaError> {
        // Null handling matches Java: if `isNullAllowed`, return; else error.
        if matches!(value, ConfigValue::Null) {
            if self.is_null_allowed {
                return Ok(());
            }
            return Err(config_exception::message(format!(
                "Configuration '{name}' values must not be null."
            )));
        }
        let list = value
            .as_list()
            .ok_or_else(|| config_exception::new(name, value, "Value must be a list for ValidList validator"))?;

        if !self.is_empty_allowed && list.is_empty() {
            let valid_str = if self.valid_strings.is_empty() {
                "any non-empty value".to_owned()
            } else {
                format!("[{}]", self.valid_strings.join(", "))
            };
            return Err(config_exception::message(format!(
                "Configuration '{name}' must not be empty. Valid values include: {valid_str}"
            )));
        }

        // Duplicate detection mirrors Java's `Set.copyOf(values).size() !=
        // values.size()` check.
        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::with_capacity(list.len());
        for v in list {
            if !seen.insert(v.as_str()) {
                return Err(config_exception::message(format!(
                    "Configuration '{name}' values must not be duplicated."
                )));
            }
        }

        // Per-value checks: empty entries always rejected; if a fixed
        // valid_strings set is present, each value must belong to it.
        let has_valid_strings = !self.valid_strings.is_empty();
        for entry in list {
            if entry.is_empty() {
                return Err(config_exception::message(format!(
                    "Configuration '{name}' values must not be empty."
                )));
            }
            if has_valid_strings && !self.valid_strings.iter().any(|v| v == entry) {
                let single = ConfigValue::String(entry.clone());
                return Err(config_exception::new(
                    name,
                    &single,
                    &format!("String must be one of: {}", self.valid_strings.join(", ")),
                ));
            }
        }
        Ok(())
    }

    fn description(&self) -> String {
        if self.valid_strings.is_empty() {
            String::new()
        } else {
            format!("[{}]", self.valid_strings.join(", "))
        }
    }
}

/// A single configuration key. Mirrors `ConfigDef.ConfigKey`.
pub struct ConfigKey {
    pub name: String,
    pub kind: Type,
    pub default_value: Option<ConfigValue>,
    pub validator: Option<Arc<dyn Validator>>,
    pub importance: Importance,
    pub documentation: String,
}

impl ConfigKey {
    /// True iff the key has an explicit default. Mirrors Java's `hasDefault`
    /// (which compares against the `NO_DEFAULT_VALUE` sentinel).
    pub fn has_default(&self) -> bool {
        self.default_value.is_some()
    }
}

impl fmt::Debug for ConfigKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConfigKey")
            .field("name", &self.name)
            .field("type", &self.kind)
            .field("default_value", &self.default_value)
            .field("importance", &self.importance)
            .finish_non_exhaustive()
    }
}

/// Schema definition for a set of configuration keys. Mirrors `ConfigDef`.
///
/// The Java `ConfigDef` uses a `LinkedHashMap` to preserve insertion order.
/// We use [`indexmap::IndexMap`] for the same property since other phases
/// (e.g. configuration documentation generation in Phase 7) rely on the
/// order of `define` calls.
#[derive(Default, Debug)]
pub struct ConfigDef {
    config_keys: indexmap::IndexMap<String, ConfigKey>,
}

impl ConfigDef {
    /// Construct an empty schema. Mirrors `new ConfigDef()`.
    pub fn new() -> Self {
        ConfigDef { config_keys: indexmap::IndexMap::new() }
    }

    /// Define a new configuration. Mirrors the
    /// `define(String, Type, Object, Validator, Importance, String)` overload.
    pub fn define(
        &mut self,
        name: impl Into<String>,
        kind: Type,
        default_value: Option<ConfigValue>,
        validator: Option<Arc<dyn Validator>>,
        importance: Importance,
        documentation: impl Into<String>,
    ) -> Result<&mut Self, KafkaError> {
        let key = ConfigKey {
            name: name.into(),
            kind,
            default_value,
            validator,
            importance,
            documentation: documentation.into(),
        };
        self.add_key(key)
    }

    fn add_key(&mut self, key: ConfigKey) -> Result<&mut Self, KafkaError> {
        if self.config_keys.contains_key(&key.name) {
            return Err(config_exception::message(format!(
                "Configuration {} is defined twice.",
                key.name
            )));
        }
        // Validate the default against the declared validator immediately
        // so configuration mistakes surface at definition time, not parse
        // time.
        if let (Some(default), Some(validator)) = (&key.default_value, &key.validator) {
            validator.ensure_valid(&key.name, default)?;
        }
        self.config_keys.insert(key.name.clone(), key);
        Ok(self)
    }

    /// Return the set of defined config names in insertion order.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.config_keys.keys().map(String::as_str)
    }

    /// Borrow the [`ConfigKey`] for `name`, if defined.
    pub fn config_key(&self, name: &str) -> Option<&ConfigKey> {
        self.config_keys.get(name)
    }

    /// Parse `props` (raw `Map<String, String>` from Java) into a typed
    /// value map. Mirrors `ConfigDef.parse(Map<?, ?>)`.
    ///
    /// Behaviour:
    /// 1. For each defined key, look up the raw value in `props`.
    /// 2. If absent, fall back to the default value. If neither default nor
    ///    raw value is available, raise [`KafkaError::Config`] (Java throws
    ///    `ConfigException`).
    /// 3. Parse the raw string into the declared [`Type`].
    /// 4. Run the validator (if any).
    pub fn parse(&self, props: &HashMap<String, String>) -> Result<HashMap<String, ConfigValue>, KafkaError> {
        let mut values = HashMap::with_capacity(self.config_keys.len());
        for key in self.config_keys.values() {
            let raw = props.get(&key.name);
            let value = match raw {
                Some(s) => parse_typed(&key.name, key.kind, s)?,
                None => match &key.default_value {
                    Some(v) => v.clone(),
                    None => {
                        return Err(config_exception::message(format!(
                            "Missing required configuration \"{}\" which has no default value.",
                            key.name
                        )));
                    },
                },
            };
            if let Some(v) = &key.validator {
                v.ensure_valid(&key.name, &value)?;
            }
            values.insert(key.name.clone(), value);
        }
        // Surface unknown keys (not strictly required by Java's `parse` but
        // the ProducerConfig wraps `parse` with a "log unknown configs"
        // call). For Phase 1 we keep the same lenient contract Java uses
        // and silently ignore unknowns.
        Ok(values)
    }
}

fn parse_typed(name: &str, kind: Type, raw: &str) -> Result<ConfigValue, KafkaError> {
    let trimmed = raw.trim();
    match kind {
        Type::Boolean => match trimmed.to_ascii_lowercase().as_str() {
            "true" => Ok(ConfigValue::Boolean(true)),
            "false" => Ok(ConfigValue::Boolean(false)),
            other => Err(config_exception::new(name, other, "Expected value to be either true or false")),
        },
        Type::String => Ok(ConfigValue::String(trimmed.to_owned())),
        Type::Int => trimmed
            .parse::<i32>()
            .map(ConfigValue::Int)
            .map_err(|e| config_exception::new(name, trimmed, &format!("Not an int: {e}"))),
        Type::Short => trimmed
            .parse::<i16>()
            .map(ConfigValue::Short)
            .map_err(|e| config_exception::new(name, trimmed, &format!("Not a short: {e}"))),
        Type::Long => trimmed
            .parse::<i64>()
            .map(ConfigValue::Long)
            .map_err(|e| config_exception::new(name, trimmed, &format!("Not a long: {e}"))),
        Type::Double => trimmed
            .parse::<f64>()
            .map(ConfigValue::Double)
            .map_err(|e| config_exception::new(name, trimmed, &format!("Not a double: {e}"))),
        Type::List => Ok(ConfigValue::List(
            trimmed
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect(),
        )),
        Type::Class => Ok(ConfigValue::Class(trimmed.to_owned())),
        Type::Password => Ok(ConfigValue::Password(Password::new(trimmed))),
    }
}

#[cfg(test)]
mod tests {
    // Translation of the `ConfigDefTest` cases that exercise types and
    // validators relevant to producer keys: parsing each `Type`,
    // `Range::atLeast` / `Range::between`, `ValidString.in`, defaulting,
    // and the `Configuration <name> is defined twice.` error.

    use super::*;

    fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect()
    }

    #[test]
    fn parse_basic_types() {
        let mut def = ConfigDef::new();
        def.define("a", Type::Int, None, None, Importance::Low, "")
            .unwrap()
            .define("b", Type::Boolean, None, None, Importance::Low, "")
            .unwrap()
            .define("c", Type::String, None, None, Importance::Low, "")
            .unwrap()
            .define("d", Type::Long, None, None, Importance::Low, "")
            .unwrap()
            .define("e", Type::Double, None, None, Importance::Low, "")
            .unwrap()
            .define("f", Type::List, None, None, Importance::Low, "")
            .unwrap();

        let parsed = def
            .parse(&map(&[
                ("a", "42"),
                ("b", "true"),
                ("c", "hello"),
                ("d", "1234"),
                ("e", "2.5"),
                ("f", "x, y, z"),
            ]))
            .unwrap();

        assert_eq!(parsed["a"].as_i32(), Some(42));
        assert_eq!(parsed["b"].as_bool(), Some(true));
        assert_eq!(parsed["c"].as_str(), Some("hello"));
        assert_eq!(parsed["d"].as_i64(), Some(1234));
        assert_eq!(parsed["e"].as_f64(), Some(2.5));
        let list = parsed["f"].as_list().unwrap();
        assert_eq!(list, &["x", "y", "z"]);
    }

    #[test]
    fn missing_required_value_raises_config_error() {
        let mut def = ConfigDef::new();
        def.define("a", Type::Int, None, None, Importance::Low, "").unwrap();
        let err = def.parse(&HashMap::new()).unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)));
        assert!(err.message().contains("Missing required configuration \"a\""));
    }

    #[test]
    fn default_value_is_used_when_missing() {
        let mut def = ConfigDef::new();
        def.define("a", Type::Int, Some(ConfigValue::Int(7)), None, Importance::Low, "")
            .unwrap();
        let parsed = def.parse(&HashMap::new()).unwrap();
        assert_eq!(parsed["a"].as_i32(), Some(7));
    }

    #[test]
    fn double_define_raises_error() {
        let mut def = ConfigDef::new();
        def.define("a", Type::Int, None, None, Importance::Low, "").unwrap();
        let err = def.define("a", Type::Int, None, None, Importance::Low, "").unwrap_err();
        assert!(err.message().contains("defined twice"));
    }

    #[test]
    fn range_at_least_rejects_below() {
        let mut def = ConfigDef::new();
        let validator: Arc<dyn Validator> = Arc::new(Range::at_least(0));
        def.define("a", Type::Int, None, Some(validator), Importance::Low, "").unwrap();
        let err = def.parse(&map(&[("a", "-1")])).unwrap_err();
        // Java's `ConfigException` formats the value via `Object.toString()` —
        // for a numeric value the message reads "Invalid value -1 ..." not
        // "Invalid value Int(-1) ...". Assert the Java-equivalent text.
        assert_eq!(err.message(), "Invalid value -1 for configuration a: Value must be at least 0");
    }

    #[test]
    fn range_between_accepts_only_in_range() {
        let mut def = ConfigDef::new();
        let validator: Arc<dyn Validator> = Arc::new(Range::between(1, 10));
        def.define("a", Type::Int, None, Some(validator), Importance::Low, "").unwrap();
        assert!(def.parse(&map(&[("a", "5")])).is_ok());
        let err = def.parse(&map(&[("a", "11")])).unwrap_err();
        assert!(err.message().contains("Value must be no more than 10"));
    }

    #[test]
    fn valid_string_in_set() {
        let mut def = ConfigDef::new();
        let validator: Arc<dyn Validator> = Arc::new(ValidString::in_set(["A", "B"]));
        def.define("a", Type::String, None, Some(validator), Importance::Low, "")
            .unwrap();
        assert_eq!(def.parse(&map(&[("a", "A")])).unwrap()["a"].as_str(), Some("A"));
        let err = def.parse(&map(&[("a", "Z")])).unwrap_err();
        assert!(err.message().contains("String must be one of: A, B"));
    }

    #[test]
    fn parse_boolean_is_case_insensitive() {
        let mut def = ConfigDef::new();
        def.define("a", Type::Boolean, None, None, Importance::Low, "").unwrap();
        assert_eq!(def.parse(&map(&[("a", "TRUE")])).unwrap()["a"].as_bool(), Some(true));
        assert_eq!(def.parse(&map(&[("a", "False")])).unwrap()["a"].as_bool(), Some(false));
        let err = def.parse(&map(&[("a", "yes")])).unwrap_err();
        assert!(err.message().contains("Expected value to be either true or false"));
    }

    #[test]
    fn invalid_default_at_define_time_is_rejected() {
        let mut def = ConfigDef::new();
        let validator: Arc<dyn Validator> = Arc::new(Range::at_least(0));
        let err = def
            .define("a", Type::Int, Some(ConfigValue::Int(-1)), Some(validator), Importance::Low, "")
            .unwrap_err();
        assert!(err.message().contains("Value must be at least 0"));
    }

    #[test]
    fn password_redacts_in_display_and_debug() {
        let p = Password::new("secret");
        assert_eq!(p.to_string(), Password::HIDDEN);
        assert_eq!(format!("{p:?}"), Password::HIDDEN);
        assert_eq!(p.value(), "secret");
    }

    #[test]
    fn parse_password_round_trip() {
        let mut def = ConfigDef::new();
        def.define("pwd", Type::Password, None, None, Importance::High, "").unwrap();
        let parsed = def.parse(&map(&[("pwd", "secret")])).unwrap();
        if let ConfigValue::Password(p) = &parsed["pwd"] {
            assert_eq!(p.value(), "secret");
        } else {
            panic!("expected Password variant");
        }
    }

    #[test]
    fn list_skips_empty_entries() {
        let mut def = ConfigDef::new();
        def.define("l", Type::List, None, None, Importance::Low, "").unwrap();
        let parsed = def.parse(&map(&[("l", " a, , b ")])).unwrap();
        assert_eq!(parsed["l"].as_list().unwrap(), &["a", "b"]);
    }

    #[test]
    fn class_is_stored_as_string() {
        let mut def = ConfigDef::new();
        def.define("c", Type::Class, None, None, Importance::Low, "").unwrap();
        let parsed = def
            .parse(&map(&[("c", "org.apache.kafka.common.serialization.StringSerializer")]))
            .unwrap();
        assert_eq!(
            parsed["c"].as_str(),
            Some("org.apache.kafka.common.serialization.StringSerializer")
        );
    }

    #[test]
    fn non_null_validator_rejects_null() {
        let mut def = ConfigDef::new();
        let validator: Arc<dyn Validator> = Arc::new(NonNullValidator);
        // Defaulting to Null and validator rejecting Null = error at define
        // time.
        let err = def
            .define("a", Type::String, Some(ConfigValue::Null), Some(validator), Importance::Low, "")
            .unwrap_err();
        assert!(err.message().contains("entry must be non null"));
    }

    #[test]
    fn case_insensitive_valid_string_accepts_any_case() {
        let v = CaseInsensitiveValidString::in_set(["PLAINTEXT", "SSL"]);
        v.ensure_valid("k", &ConfigValue::String("plaintext".into())).unwrap();
        v.ensure_valid("k", &ConfigValue::String("Ssl".into())).unwrap();
        v.ensure_valid("k", &ConfigValue::String("SSL".into())).unwrap();
    }

    #[test]
    fn case_insensitive_valid_string_rejects_unknown_with_key_in_message() {
        let v = CaseInsensitiveValidString::in_set(["PLAINTEXT", "SSL"]);
        let err = v
            .ensure_valid("security.protocol", &ConfigValue::String("abc".into()))
            .unwrap_err();
        let msg = err.message();
        assert!(msg.contains("security.protocol"), "got: {msg}");
        assert!(msg.contains("(case insensitive)"), "got: {msg}");
    }

    #[test]
    fn case_insensitive_valid_string_rejects_null() {
        let v = CaseInsensitiveValidString::in_set(["A", "B"]);
        let err = v.ensure_valid("k", &ConfigValue::Null).unwrap_err();
        assert!(err.message().contains("(case insensitive)"));
    }

    #[test]
    fn non_empty_string_allows_null() {
        // Java behaviour: NonEmptyString lets null pass — only empty strings
        // are rejected. Pair with NonNullValidator to also reject null.
        NonEmptyString.ensure_valid("k", &ConfigValue::Null).unwrap();
    }

    #[test]
    fn non_empty_string_rejects_empty() {
        let err = NonEmptyString
            .ensure_valid("transactional.id", &ConfigValue::String(String::new()))
            .unwrap_err();
        assert!(err.message().contains("non-empty"));
    }

    #[test]
    fn non_empty_string_accepts_non_empty() {
        NonEmptyString.ensure_valid("k", &ConfigValue::String("foo".into())).unwrap();
    }

    #[test]
    fn valid_list_any_rejects_duplicates() {
        let v = ValidList::any_non_duplicate_values(true, false);
        let value = ConfigValue::List(vec!["a".into(), "b".into(), "a".into()]);
        let err = v.ensure_valid("interceptor.classes", &value).unwrap_err();
        assert!(err.message().contains("must not be duplicated"));
    }

    #[test]
    fn valid_list_any_accepts_unique() {
        let v = ValidList::any_non_duplicate_values(true, false);
        v.ensure_valid("k", &ConfigValue::List(vec!["a".into(), "b".into()])).unwrap();
    }

    #[test]
    fn valid_list_rejects_empty_when_not_allowed() {
        let v = ValidList::any_non_duplicate_values(false, false);
        let err = v.ensure_valid("bootstrap.servers", &ConfigValue::List(vec![])).unwrap_err();
        assert!(err.message().contains("must not be empty"));
    }

    #[test]
    fn valid_list_accepts_empty_when_allowed() {
        let v = ValidList::any_non_duplicate_values(true, false);
        v.ensure_valid("k", &ConfigValue::List(vec![])).unwrap();
    }

    #[test]
    fn valid_list_rejects_null_when_not_allowed() {
        let v = ValidList::any_non_duplicate_values(true, false);
        let err = v.ensure_valid("bootstrap.servers", &ConfigValue::Null).unwrap_err();
        assert!(err.message().contains("must not be null"));
    }

    #[test]
    fn valid_list_accepts_null_when_allowed() {
        let v = ValidList::any_non_duplicate_values(true, true);
        v.ensure_valid("k", &ConfigValue::Null).unwrap();
    }

    #[test]
    fn valid_list_rejects_empty_entries() {
        let v = ValidList::any_non_duplicate_values(true, false);
        let value = ConfigValue::List(vec!["a".into(), String::new(), "b".into()]);
        let err = v.ensure_valid("k", &value).unwrap_err();
        assert!(err.message().contains("values must not be empty"));
    }
}
