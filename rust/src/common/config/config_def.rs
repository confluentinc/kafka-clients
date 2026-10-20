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

//! Translated from `org.apache.kafka.common.config.ConfigDef`: the definition
//! of a client's configuration keys (name, type, default, validator,
//! importance and documentation) and the parser that turns the user's values
//! into typed values with Java's checks and messages.
//!
//! Crate-private: the client configs (`ProducerConfig`, `ConsumerConfig`,
//! `AdminClientConfig`) are built on it, but a public `ConfigDef`, its
//! validators and `ConfigValue` would be new public types, deferred to later
//! work (see `CHANGELOG.md`).
//!
//! What is translated: [`Type`], [`Importance`], [`ConfigKey`],
//! [`ConfigDef::NO_DEFAULT_VALUE`], the `define` / `defineInternal` chains,
//! `parse`, `parseType`, `names`, and the validators [`Range`], [`ValidList`],
//! [`ValidString`], [`CaseInsensitiveValidString`], [`NonEmptyString`],
//! [`NonNullValidator`], [`CompositeValidator`] and [`ListSize`].
//!
//! What is not:
//!
//! - The `CLASS` type and every branch handling it: a configuration never
//!   takes a class (CLAUDE.md §2).
//! - The documentation renderers (`toHtml`, `toRst`, `toEnrichedRst`,
//!   `convertToString`, `niceMemoryUnits`, `niceTimeUnits`), which nothing in
//!   the clients calls.
//! - `validate` / `validateAll`, `ConfigValue`, `Recommender`, groups, widths,
//!   display names, dependents and `LambdaValidator`: only `validate` uses
//!   them, and it is a public API of the deferred public `ConfigDef`.
//! - `NonEmptyStringWithoutControlChars`, which no client key uses.
//!
//! Java's `define` overloads differ only in which of the optional arguments
//! they take; the ones the client configs call collapse to [`ConfigDef::define`]
//! (no validator) and [`ConfigDef::define_with_validator`], each taking the
//! default as a [`DefaultValue`] so [`ConfigDef::NO_DEFAULT_VALUE`] stands in
//! for the overloads without a default.

// Until the client configs are rebuilt on `AbstractConfig` (the commits that
// follow this one), only `trim`, `parse_list` and `ValidList` are reached
// outside the tests.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the client configs are moved onto ConfigDef by the following commits"
    )
)]

use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::common::Error;
use crate::common::config::types::Password;

/// The type of a configuration key, Java's `ConfigDef.Type` without `CLASS`
/// (CLAUDE.md §2), in Java's declaration order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[doc(alias = "org.apache.kafka.common.config.ConfigDef$Type")]
pub(crate) enum Type {
    /// Used for boolean values. Values can be provided as a `Boolean` object
    /// or as a `String` with values `true` or `false` (this is not case
    /// sensitive), otherwise a `ConfigException` is thrown.
    Boolean,
    /// Used for string values. Values must be provided as a `String` object,
    /// otherwise a `ConfigException` is thrown.
    String,
    /// Used for numerical values within the Java Integer range. Values must
    /// be provided as an `Integer` object or as a `String` being a valid
    /// integer value, otherwise a `ConfigException` is thrown.
    Int,
    /// Used for numerical values within the Java Short range. Values must be
    /// provided as a `Short` object or as a `String` being a valid short
    /// value, otherwise a `ConfigException` is thrown.
    Short,
    /// Used for numerical values within the Java Long range. Values must be
    /// provided as a `Long` object, as an `Integer` object or as a `String`
    /// being a valid long value, otherwise a `ConfigException` is thrown.
    Long,
    /// Used for numerical values within the Java Double range. Values must be
    /// provided as a `Number` object, as a `Double` object or as a `String`
    /// being a valid double value, otherwise a `ConfigException` is thrown.
    Double,
    /// Used for list values. Values must be provided as a `List` object, as a
    /// `String` object, otherwise a `ConfigException` is thrown. When the
    /// value is provided as a `String` it must use commas to separate the
    /// different entries (for example: `first-entry, second-entry`) and an
    /// empty `String` maps to an empty `List`.
    List,
    /// Used for string values containing sensitive data such as a password or
    /// key. The values of configurations with of this type are not included
    /// in logs and instead replaced with `[hidden]`. Values must be provided
    /// as a `String` object, otherwise a `ConfigException` is thrown.
    Password,
}

impl Type {
    /// Whether this type contains sensitive data such as a password or key.
    #[doc(alias = "org.apache.kafka.common.config.ConfigDef$Type#isSensitive")]
    pub(crate) fn is_sensitive(self) -> bool {
        matches!(self, Type::Password)
    }
}

impl fmt::Display for Type {
    /// Java's `Enum.toString()`: the constant's name.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Type::Boolean => "BOOLEAN",
            Type::String => "STRING",
            Type::Int => "INT",
            Type::Short => "SHORT",
            Type::Long => "LONG",
            Type::Double => "DOUBLE",
            Type::List => "LIST",
            Type::Password => "PASSWORD",
        })
    }
}

/// The importance level for a configuration, Java's `ConfigDef.Importance`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[doc(alias = "org.apache.kafka.common.config.ConfigDef$Importance")]
pub(crate) enum Importance {
    High,
    Medium,
    Low,
}

/// A configuration value, either as given by the user or as parsed: the Rust
/// stand-in for the `Object` Java's `ConfigDef` stores.
///
/// Not a Java class (DoD #7). Java's maps hold `Object`, which is one of
/// `Boolean`, `String`, `Integer`, `Short`, `Long`, `Double`, `List<String>`
/// or `Password` for the non-class types; Rust needs a closed type, so there
/// is one variant per such class. A `String` variant is also what a user's
/// `String` value is before parsing, which is why `parse_type` takes and
/// returns the same type. Java's `null` is `Option::None` around it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ParsedValue {
    Boolean(bool),
    String(String),
    Int(i32),
    Short(i16),
    Long(i64),
    Double(f64),
    List(Vec<String>),
    Password(Password),
}

impl ParsedValue {
    /// Java's `value.getClass().getName()`, which `parseType`'s messages
    /// print.
    pub(crate) fn java_class_name(&self) -> &'static str {
        match self {
            ParsedValue::Boolean(_) => "java.lang.Boolean",
            ParsedValue::String(_) => "java.lang.String",
            ParsedValue::Int(_) => "java.lang.Integer",
            ParsedValue::Short(_) => "java.lang.Short",
            ParsedValue::Long(_) => "java.lang.Long",
            ParsedValue::Double(_) => "java.lang.Double",
            ParsedValue::List(_) => "java.util.ArrayList",
            ParsedValue::Password(_) => "org.apache.kafka.common.config.types.Password",
        }
    }

    /// Java's `Number.doubleValue()`, `None` when the value is not a
    /// `Number`.
    fn double_value(&self) -> Option<f64> {
        match self {
            ParsedValue::Int(v) => Some(f64::from(*v)),
            ParsedValue::Short(v) => Some(f64::from(*v)),
            // `Long.doubleValue()` rounds to the nearest double, as `as` does.
            ParsedValue::Long(v) => Some(*v as f64),
            ParsedValue::Double(v) => Some(*v),
            _ => None,
        }
    }
}

impl fmt::Display for ParsedValue {
    /// Java's `toString()` of the value's class: a list prints as
    /// `[a, b]` (`AbstractCollection.toString`), a password as `[hidden]`, a
    /// double as `Double.toString` does.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParsedValue::Boolean(v) => write!(f, "{v}"),
            ParsedValue::String(v) => f.write_str(v),
            ParsedValue::Int(v) => write!(f, "{v}"),
            ParsedValue::Short(v) => write!(f, "{v}"),
            ParsedValue::Long(v) => write!(f, "{v}"),
            ParsedValue::Double(v) => f.write_str(&java_double_to_string(*v)),
            ParsedValue::List(v) => write!(f, "[{}]", v.join(", ")),
            ParsedValue::Password(v) => write!(f, "{v}"),
        }
    }
}

macro_rules! parsed_value_from {
    ($($from:ty => $variant:ident),* $(,)?) => {
        $(
            impl From<$from> for ParsedValue {
                fn from(value: $from) -> Self {
                    ParsedValue::$variant(value.into())
                }
            }
        )*
    };
}

parsed_value_from!(
    bool => Boolean,
    String => String,
    &str => String,
    i32 => Int,
    i16 => Short,
    i64 => Long,
    f64 => Double,
    Vec<String> => List,
    Password => Password,
);

impl From<&[&str]> for ParsedValue {
    fn from(value: &[&str]) -> Self {
        ParsedValue::List(value.iter().map(|s| (*s).to_string()).collect())
    }
}

/// The default of a configuration key: Java's `Object defaultValue`, which is
/// `ConfigDef.NO_DEFAULT_VALUE` for a required key, `null`, or a value.
///
/// Not a Java class (DoD #7): Java tells the three apart by identity
/// (`NO_DEFAULT_VALUE` is a unique `Object`), which Rust spells as variants.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DefaultValue {
    /// Java's `ConfigDef.NO_DEFAULT_VALUE`: the key is required.
    NoDefault,
    /// Java's `null` default.
    Null,
    /// A default value, parsed with the key's type when the key is defined.
    Value(ParsedValue),
}

impl<T: Into<ParsedValue>> From<T> for DefaultValue {
    fn from(value: T) -> Self {
        DefaultValue::Value(value.into())
    }
}

/// Validation logic the user may provide to perform single configuration
/// validation, Java's `ConfigDef.Validator`.
///
/// `Display` is Java's `toString()`, the text documentation prints for the
/// valid values.
#[doc(alias = "org.apache.kafka.common.config.ConfigDef$Validator")]
pub(crate) trait Validator: fmt::Display + Send + Sync {
    /// Perform single configuration validation. `None` is Java's `null`.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] if the value is invalid; [`Error::LocalIllegalArgument`]
    /// where Java's validator would throw a `ClassCastException` because the
    /// value is not of the class it expects.
    fn ensure_valid(&self, name: &str, value: Option<&ParsedValue>) -> Result<(), Error>;

    /// Java's `validator instanceof ValidList`, which `parseValue` tests to
    /// remove duplicates from a list value.
    fn is_valid_list(&self) -> bool {
        false
    }
}

/// Java's `ClassCastException` message for a value a validator casts to a
/// class it is not.
fn class_cast(value: &ParsedValue, target: &str) -> Error {
    Error::local_illegal_argument(format!("class {} cannot be cast to class {}", value.java_class_name(), target))
}

/// Java's `(String) o` in a validator: `None` stays `None`.
fn as_string(value: Option<&ParsedValue>) -> Result<Option<&str>, Error> {
    match value {
        None => Ok(None),
        Some(ParsedValue::String(s)) => Ok(Some(s)),
        Some(other) => Err(class_cast(other, "java.lang.String")),
    }
}

/// Java's `(List) o` in a validator: `None` stays `None`.
fn as_list(value: Option<&ParsedValue>) -> Result<Option<&[String]>, Error> {
    match value {
        None => Ok(None),
        Some(ParsedValue::List(l)) => Ok(Some(l)),
        Some(other) => Err(class_cast(other, "java.util.List")),
    }
}

/// Java's `ConfigException(name, value, message)` where `value` may be
/// `null`, which prints as `null`.
fn config_error(name: &str, value: Option<&ParsedValue>, message: impl fmt::Display) -> Error {
    match value {
        Some(v) => Error::config_name_value_message(name, v, message),
        None => Error::config_name_value_message(name, "null", message),
    }
}

/// The definition of a configuration key, Java's `ConfigDef.ConfigKey`,
/// without the fields only `validate` and the renderers read (group,
/// order in group, width, display name, dependents, recommender, alternative
/// string). Java's `public final` fields are `pub(crate)` here.
#[doc(alias = "org.apache.kafka.common.config.ConfigDef$ConfigKey")]
pub(crate) struct ConfigKey {
    pub(crate) name: String,
    pub(crate) r#type: Type,
    pub(crate) documentation: String,
    /// The default, already parsed with [`Self::r#type`] (Java's constructor
    /// runs `parseType` on it).
    pub(crate) default_value: DefaultValue,
    pub(crate) validator: Option<Box<dyn Validator>>,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "read only by the documentation renderers, not translated")
    )]
    pub(crate) importance: Importance,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "read only by the documentation renderers, not translated")
    )]
    pub(crate) internal_config: bool,
}

impl ConfigKey {
    /// Java's `ConfigKey` constructor: parses the default with the key's type
    /// and validates it.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] if the default cannot be parsed or fails the
    /// validator.
    fn new(
        name: &str,
        r#type: Type,
        default_value: DefaultValue,
        validator: Option<Box<dyn Validator>>,
        importance: Importance,
        documentation: &str,
        internal_config: bool,
    ) -> Result<Self, Error> {
        let default_value = match default_value {
            DefaultValue::NoDefault => DefaultValue::NoDefault,
            DefaultValue::Null => DefaultValue::Null,
            DefaultValue::Value(v) => match ConfigDef::parse_type(name, Some(&v), r#type)? {
                Some(parsed) => DefaultValue::Value(parsed),
                None => DefaultValue::Null,
            },
        };
        if let Some(validator) = &validator {
            match &default_value {
                DefaultValue::NoDefault => {},
                DefaultValue::Null => validator.ensure_valid(name, None)?,
                DefaultValue::Value(v) => validator.ensure_valid(name, Some(v))?,
            }
        }
        Ok(Self {
            name: name.to_string(),
            r#type,
            documentation: documentation.to_string(),
            default_value,
            validator,
            importance,
            internal_config,
        })
    }

    /// Java's `ConfigKey.hasDefault()`.
    #[doc(alias = "org.apache.kafka.common.config.ConfigDef$ConfigKey#hasDefault")]
    pub(crate) fn has_default(&self) -> bool {
        !matches!(self.default_value, DefaultValue::NoDefault)
    }
}

/// This class is used for specifying the set of expected configurations.
/// For each configuration, you can specify the name, the type, the default
/// value, the documentation, the group information, the order in the group,
/// the width of the configuration value and the name suitable for display in
/// the UI.
///
/// The keys keep their definition order, as Java's `LinkedHashMap` does.
#[doc(alias = "org.apache.kafka.common.config.ConfigDef")]
#[derive(Default)]
pub(crate) struct ConfigDef {
    config_keys: Vec<ConfigKey>,
    index: HashMap<String, usize>,
}

impl ConfigDef {
    /// A unique Java object we use as a default value to mean the
    /// configuration must be set explicitly.
    #[doc(alias = "org.apache.kafka.common.config.ConfigDef#NO_DEFAULT_VALUE")]
    pub(crate) const NO_DEFAULT_VALUE: DefaultValue = DefaultValue::NoDefault;

    /// Java's `new ConfigDef()`.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Java's `define(ConfigKey key)`.
    ///
    /// # Panics
    ///
    /// With Java's `ConfigException` message `Configuration <name> is
    /// defined twice.` if the key is already defined. Definitions are fixed
    /// at compile time, so this is a programming error, as it is in Java,
    /// where it fails the class initializer.
    fn define_key(mut self, key: ConfigKey) -> Self {
        if self.index.contains_key(&key.name) {
            panic!("Configuration {} is defined twice.", key.name);
        }
        self.index.insert(key.name.clone(), self.config_keys.len());
        self.config_keys.push(key);
        self
    }

    /// Builds a [`ConfigKey`] and defines it.
    ///
    /// # Panics
    ///
    /// With the `ConfigException` message Java's `ConfigKey` constructor
    /// throws when the default cannot be parsed or fails the validator, and
    /// as [`Self::define_key`].
    #[expect(clippy::too_many_arguments, reason = "Java's ConfigKey constructor arguments")]
    fn define_full(
        self,
        name: &str,
        r#type: Type,
        default_value: DefaultValue,
        validator: Option<Box<dyn Validator>>,
        importance: Importance,
        documentation: &str,
        internal_config: bool,
    ) -> Self {
        match ConfigKey::new(
            name,
            r#type,
            default_value,
            validator,
            importance,
            documentation,
            internal_config,
        ) {
            Ok(key) => self.define_key(key),
            // Java's `ConfigException` message, without the Rust type prefix.
            Err(Error::Config(e)) => panic!("{}", e.message()),
            Err(e) => panic!("{e}"),
        }
    }

    /// Define a new configuration with no special validation logic: Java's
    /// `define(name, type, defaultValue, importance, documentation)`, and,
    /// with [`Self::NO_DEFAULT_VALUE`], `define(name, type, importance,
    /// documentation)`.
    ///
    /// # Panics
    ///
    /// As [`Self::define_full`].
    pub(crate) fn define(
        self,
        name: &str,
        r#type: Type,
        default_value: impl Into<DefaultValue>,
        importance: Importance,
        documentation: &str,
    ) -> Self {
        self.define_full(name, r#type, default_value.into(), None, importance, documentation, false)
    }

    /// Define a new configuration with a validator: Java's `define(name, type,
    /// defaultValue, validator, importance, documentation)`.
    ///
    /// # Panics
    ///
    /// As [`Self::define_full`].
    pub(crate) fn define_with_validator(
        self,
        name: &str,
        r#type: Type,
        default_value: impl Into<DefaultValue>,
        validator: impl Validator + 'static,
        importance: Importance,
        documentation: &str,
    ) -> Self {
        self.define_full(
            name,
            r#type,
            default_value.into(),
            Some(Box::new(validator)),
            importance,
            documentation,
            false,
        )
    }

    /// Define a new internal configuration. Internal configuration won't show
    /// up in the docs and aren't intended for general use: Java's
    /// `defineInternal(name, type, defaultValue, importance)`, whose
    /// documentation is `""`.
    ///
    /// # Panics
    ///
    /// As [`Self::define_full`].
    pub(crate) fn define_internal(
        self,
        name: &str,
        r#type: Type,
        default_value: impl Into<DefaultValue>,
        importance: Importance,
    ) -> Self {
        self.define_full(name, r#type, default_value.into(), None, importance, "", true)
    }

    /// Java's `defineInternal(name, type, defaultValue, validator,
    /// importance)`.
    ///
    /// # Panics
    ///
    /// As [`Self::define_full`].
    pub(crate) fn define_internal_with_validator(
        self,
        name: &str,
        r#type: Type,
        default_value: impl Into<DefaultValue>,
        validator: impl Validator + 'static,
        importance: Importance,
    ) -> Self {
        self.define_full(
            name,
            r#type,
            default_value.into(),
            Some(Box::new(validator)),
            importance,
            "",
            true,
        )
    }

    /// Add standard SSL client configuration options: Java's
    /// `withClientSslSupport()`.
    pub(crate) fn with_client_ssl_support(self) -> Self {
        crate::common::config::SslConfigs::add_client_ssl_support(self)
    }

    /// Add standard SASL client configuration options: Java's
    /// `withClientSaslSupport()`.
    pub(crate) fn with_client_sasl_support(self) -> Self {
        crate::common::config::SaslConfigs::add_client_sasl_support(self)
    }

    /// Returns unmodifiable set of the names of the configs.
    #[doc(alias = "org.apache.kafka.common.config.ConfigDef#names")]
    pub(crate) fn names(&self) -> HashSet<String> {
        self.config_keys.iter().map(|k| k.name.clone()).collect()
    }

    /// The configuration keys in definition order: Java's
    /// `configKeys().values()`.
    #[doc(alias = "org.apache.kafka.common.config.ConfigDef#configKeys")]
    pub(crate) fn config_keys(&self) -> impl Iterator<Item = &ConfigKey> {
        self.config_keys.iter()
    }

    /// Java's `configKeys().get(name)`.
    pub(crate) fn config_key(&self, name: &str) -> Option<&ConfigKey> {
        self.index.get(name).map(|&i| &self.config_keys[i])
    }

    /// Parse and validate configs against this configuration definition. The
    /// input is a map of configs. The values can either be strings or they
    /// may already be of the appropriate type (int, string, etc).
    ///
    /// Returns parsed and validated configs. The key will be the config name
    /// and the value will be the value parsed into the appropriate type (int,
    /// string, etc). Every defined key is present; `None` is Java's `null`.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] with Java's message for the first key, in definition
    /// order, that is missing without a default, cannot be parsed or fails
    /// its validator.
    #[doc(alias = "org.apache.kafka.common.config.ConfigDef#parse")]
    pub(crate) fn parse(
        &self,
        props: &HashMap<String, Option<ParsedValue>>,
    ) -> Result<HashMap<String, Option<ParsedValue>>, Error> {
        // Java first checks that every dependent config is defined; dependents
        // are not translated, so there is nothing to check.
        let mut values = HashMap::with_capacity(self.config_keys.len());
        for key in &self.config_keys {
            let value = props.get(&key.name);
            let parsed = self.parse_value(key, value.and_then(Option::as_ref), value.is_some())?;
            values.insert(key.name.clone(), parsed);
        }
        Ok(values)
    }

    /// Java's `parseValue(key, value, isSet)`.
    fn parse_value(
        &self,
        key: &ConfigKey,
        value: Option<&ParsedValue>,
        is_set: bool,
    ) -> Result<Option<ParsedValue>, Error> {
        let mut parsed_value = if is_set {
            Self::parse_type(&key.name, value, key.r#type)?
        } else {
            match &key.default_value {
                // props map doesn't contain setting, the key is required
                // because no default value specified - its an error
                DefaultValue::NoDefault => {
                    return Err(Error::config_message(format!(
                        "Missing required configuration \"{}\" which has no default value.",
                        key.name
                    )));
                },
                // otherwise assign setting its default value
                DefaultValue::Null => None,
                DefaultValue::Value(v) => Some(v.clone()),
            }
        };
        if let (Some(validator), Some(ParsedValue::List(original))) = (&key.validator, &parsed_value)
            && validator.is_valid_list()
        {
            let deduped = distinct(original);
            if deduped.len() != original.len() {
                log::warn!(
                    "Configuration key \"{}\" contains duplicate values. Duplicates will be removed. The original value \
                     is: [{}], the updated value is: [{}]",
                    key.name,
                    original.join(", "),
                    deduped.join(", ")
                );
                parsed_value = Some(ParsedValue::List(deduped));
            }
        }
        if let Some(validator) = &key.validator {
            validator.ensure_valid(&key.name, parsed_value.as_ref())?;
        }
        Ok(parsed_value)
    }

    /// Parse a value according to its expected type: Java's static
    /// `parseType(name, value, type)`. `None` (Java's `null`) parses to
    /// `None`.
    ///
    /// A `String` value is trimmed as Java's `String.trim()` does
    /// ([`Self::trim`]) before it is parsed. Numbers parse as Java's
    /// `Integer.parseInt`, `Short.parseShort`, `Long.parseLong` and
    /// `Double.parseDouble` do, with one known divergence: Java accepts any
    /// Unicode decimal digit (`Character.digit`) and hexadecimal floating
    /// point literals (`0x1p3`), this parser only ASCII digits and decimal
    /// literals.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] with Java's message when the value is not of the
    /// type or cannot be parsed as it.
    #[doc(alias = "org.apache.kafka.common.config.ConfigDef#parseType")]
    pub(crate) fn parse_type(
        name: &str,
        value: Option<&ParsedValue>,
        r#type: Type,
    ) -> Result<Option<ParsedValue>, Error> {
        let Some(value) = value else {
            return Ok(None);
        };
        let trimmed = match value {
            ParsedValue::String(s) => Some(Self::trim(s)),
            _ => None,
        };
        let not_a_number = || Error::config_name_value_message(name, value, format!("Not a number of type {type}"));
        let wrong_class = |message: &str| {
            Error::config_name_value_message(name, value, format!("{message}{}", value.java_class_name()))
        };
        let parsed = match (r#type, value) {
            (Type::Boolean, ParsedValue::Boolean(_)) => value.clone(),
            (Type::Boolean, ParsedValue::String(_)) => {
                let trimmed = trimmed.unwrap_or_default();
                if java_equals_ignore_case(trimmed, "true") {
                    ParsedValue::Boolean(true)
                } else if java_equals_ignore_case(trimmed, "false") {
                    ParsedValue::Boolean(false)
                } else {
                    return Err(Error::config_name_value_message(
                        name,
                        value,
                        "Expected value to be either true or false",
                    ));
                }
            },
            (Type::Boolean, _) => {
                return Err(Error::config_name_value_message(
                    name,
                    value,
                    "Expected value to be either true or false",
                ));
            },
            (Type::Password, ParsedValue::Password(_)) => value.clone(),
            (Type::Password, ParsedValue::String(_)) => {
                ParsedValue::Password(Password::new(trimmed.unwrap_or_default()))
            },
            (Type::String, ParsedValue::String(_)) => ParsedValue::String(trimmed.unwrap_or_default().to_string()),
            (Type::Password | Type::String, _) => {
                return Err(wrong_class("Expected value to be a string, but it was a "));
            },
            (Type::Int, ParsedValue::Int(_)) => value.clone(),
            (Type::Int, ParsedValue::String(_)) => {
                ParsedValue::Int(parse_java_integer(trimmed.unwrap_or_default()).ok_or_else(not_a_number)?)
            },
            (Type::Int, _) => return Err(wrong_class("Expected value to be a 32-bit integer, but it was a ")),
            (Type::Short, ParsedValue::Short(_)) => value.clone(),
            (Type::Short, ParsedValue::String(_)) => {
                ParsedValue::Short(parse_java_integer(trimmed.unwrap_or_default()).ok_or_else(not_a_number)?)
            },
            (Type::Short, _) => {
                return Err(wrong_class("Expected value to be a 16-bit integer (short), but it was a "));
            },
            (Type::Long, ParsedValue::Int(v)) => ParsedValue::Long(i64::from(*v)),
            (Type::Long, ParsedValue::Long(_)) => value.clone(),
            (Type::Long, ParsedValue::String(_)) => {
                ParsedValue::Long(parse_java_integer(trimmed.unwrap_or_default()).ok_or_else(not_a_number)?)
            },
            (Type::Long, _) => return Err(wrong_class("Expected value to be a 64-bit integer (long), but it was a ")),
            (Type::Double, ParsedValue::String(_)) => {
                ParsedValue::Double(parse_java_double(trimmed.unwrap_or_default()).ok_or_else(not_a_number)?)
            },
            (Type::Double, _) => match value.double_value() {
                Some(d) => ParsedValue::Double(d),
                None => return Err(wrong_class("Expected value to be a double, but it was a ")),
            },
            (Type::List, ParsedValue::List(_)) => value.clone(),
            (Type::List, ParsedValue::String(s)) => ParsedValue::List(Self::parse_list(s)),
            (Type::List, _) => {
                return Err(Error::config_name_value_message(
                    name,
                    value,
                    "Expected a comma separated list.",
                ));
            },
        };
        Ok(Some(parsed))
    }

    /// Java's `String.trim()`, which `ConfigDef.parseType` applies to every
    /// `String` value before parsing it (`ConfigDef.java:707-709`).
    ///
    /// Unlike [`str::trim`] it strips every character `<= U+0020` (all ASCII
    /// control characters and the space) and nothing else — in particular not
    /// Unicode whitespace such as U+00A0.
    pub(crate) fn trim(value: &str) -> &str {
        value.trim_matches(|c: char| c <= ' ')
    }

    /// The `Type.LIST` arm of Java's `ConfigDef.parseType` for a `String`
    /// value (`ConfigDef.java:768-777`):
    ///
    /// ```java
    /// if (trimmed.isEmpty())
    ///     return List.of();
    /// else
    ///     return Arrays.asList(COMMA_WITH_WHITESPACE.split(trimmed, -1));
    /// ```
    ///
    /// with `COMMA_WITH_WHITESPACE = Pattern.compile("\\s*,\\s*")`. So the
    /// whole value is trimmed, then split on commas together with any
    /// surrounding regex whitespace (`[ \t\n\x0B\f\r]`). The `-1` limit keeps
    /// empty elements, trailing ones included: `"a,,b,"` is
    /// `["a", "", "b", ""]`. Whitespace inside an element is kept, so
    /// `"a b,c"` is `["a b", "c"]`.
    pub(crate) fn parse_list(value: &str) -> Vec<String> {
        let trimmed = Self::trim(value);
        if trimmed.is_empty() {
            return Vec::new();
        }
        // Splitting on ',' and stripping regex whitespace from each piece is
        // the same split: `\s*` around a comma absorbs exactly the whitespace
        // adjacent to it, and the two outer ends are already trimmed (Java's
        // `trim` strips a superset of `\s`), so stripping them is a no-op.
        trimmed
            .split(',')
            .map(|element| element.trim_matches(Self::is_regex_whitespace).to_string())
            .collect()
    }

    /// Java regex `\s`: `[ \t\n\x0B\f\r]`.
    fn is_regex_whitespace(c: char) -> bool {
        matches!(c, ' ' | '\t' | '\n' | '\u{0B}' | '\u{0C}' | '\r')
    }
}

/// Java's `list.stream().distinct().collect(toList())`: first occurrences, in
/// order.
fn distinct(values: &[String]) -> Vec<String> {
    let mut seen = HashSet::with_capacity(values.len());
    values.iter().filter(|v| seen.insert(v.as_str())).cloned().collect()
}

/// Java's `Integer.parseInt` / `Short.parseShort` / `Long.parseLong` on an
/// already-trimmed string: an optional `+` or `-` and ASCII digits, in range.
/// Rust's `FromStr` for the integer types accepts exactly that.
fn parse_java_integer<T: std::str::FromStr>(s: &str) -> Option<T> {
    s.parse().ok()
}

/// Java's `Double.parseDouble` on an already-trimmed string, for decimal
/// literals: an optional sign, then `NaN`, `Infinity`, or digits with an
/// optional point and exponent, optionally followed by one of `fFdD`. Rust's
/// `f64::from_str` accepts the same decimal syntax but also `inf` / `nan` in
/// any case, and rejects the type suffix, so both are handled here.
fn parse_java_double(s: &str) -> Option<f64> {
    let (negative, body) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    let magnitude = match body {
        "NaN" => f64::NAN,
        "Infinity" => f64::INFINITY,
        _ => {
            let digits = body.strip_suffix(['f', 'F', 'd', 'D']).unwrap_or(body);
            let valid = digits.bytes().next().is_some_and(|b| b.is_ascii_digit() || b == b'.')
                && digits
                    .bytes()
                    .all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'e' | b'E' | b'+' | b'-'));
            if !valid {
                return None;
            }
            digits.parse::<f64>().ok()?
        },
    };
    Some(if negative { -magnitude } else { magnitude })
}

/// Java's `Double.toString(d)`: the shortest decimal that uniquely
/// distinguishes `d`, in plain notation with at least one fraction digit for
/// `10^-3 <= |d| < 10^7`, otherwise in computerized scientific notation
/// (`1.024E8`).
pub(crate) fn java_double_to_string(d: f64) -> String {
    if d.is_nan() {
        return "NaN".to_string();
    }
    if d.is_infinite() {
        return if d > 0.0 { "Infinity" } else { "-Infinity" }.to_string();
    }
    if d == 0.0 {
        return if d.is_sign_negative() { "-0.0" } else { "0.0" }.to_string();
    }
    let sign = if d < 0.0 { "-" } else { "" };
    let magnitude = d.abs();
    if (1e-3..1e7).contains(&magnitude) {
        let mut plain = format!("{magnitude}");
        if !plain.contains('.') {
            plain.push_str(".0");
        }
        return format!("{sign}{plain}");
    }
    // `{:e}` prints the same shortest digits as `d.ddde<exp>`.
    let scientific = format!("{magnitude:e}");
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((&scientific, "0"));
    let mantissa = if mantissa.contains('.') {
        mantissa.to_string()
    } else {
        format!("{mantissa}.0")
    };
    format!("{sign}{mantissa}E{exponent}")
}

/// Java's `String.equalsIgnoreCase`, char by char: equal, or equal after
/// `Character.toUpperCase`, or after `toLowerCase` of that.
fn java_equals_ignore_case(a: &str, b: &str) -> bool {
    fn single(mut it: impl ExactSizeIterator<Item = char>, c: char) -> char {
        if it.len() == 1 { it.next().unwrap_or(c) } else { c }
    }
    let upper = |c: char| single(c.to_uppercase(), c);
    let lower = |c: char| single(c.to_lowercase(), c);
    a.chars().count() == b.chars().count()
        && a.chars().zip(b.chars()).all(|(x, y)| {
            x == y || {
                let (ux, uy) = (upper(x), upper(y));
                ux == uy || lower(ux) == lower(uy)
            }
        })
}

/// The iteration order of a Java `HashSet<String>` built by adding `values`
/// in order to a default-capacity set (`Collectors.toSet()`): by bucket
/// (`hash ^ (hash >>> 16)` masked by the table size), and in insertion order
/// within a bucket. Messages that print such a set therefore print it in
/// Java's order.
fn java_hash_set_order(values: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut distinct: Vec<String> = Vec::new();
    for v in values {
        if !distinct.contains(&v) {
            distinct.push(v);
        }
    }
    // `HashMap`'s default capacity is 16 and it doubles once its size passes
    // three quarters of the capacity.
    let mut capacity: usize = 16;
    while distinct.len() * 4 > capacity * 3 {
        capacity *= 2;
    }
    let bucket = |s: &String| {
        let h = s
            .encode_utf16()
            .fold(0i32, |h, unit| h.wrapping_mul(31).wrapping_add(i32::from(unit)));
        let spread = (h ^ ((h as u32) >> 16) as i32) as u32;
        spread as usize & (capacity - 1)
    };
    // A stable sort keeps insertion order within a bucket, as Java's bucket
    // lists (and its order-preserving resize split) do.
    distinct.sort_by_key(bucket);
    distinct
}

/// Validation logic for numeric ranges, Java's `ConfigDef.Range`.
#[doc(alias = "org.apache.kafka.common.config.ConfigDef$Range")]
pub(crate) struct Range {
    min: Option<RangeBound>,
    max: Option<RangeBound>,
}

/// A `Range` bound: the `Number` Java's `Range` holds, which prints as its
/// class's `toString()`.
///
/// Not a Java class (DoD #7): Java stores a `Number`, which prints `5` for an
/// `Integer` and `5.0` for a `Double`; the two variants keep that difference.
#[derive(Debug, Clone, Copy)]
pub(crate) enum RangeBound {
    Integer(i64),
    Double(f64),
}

impl RangeBound {
    fn double_value(self) -> f64 {
        match self {
            // `Number.doubleValue()`.
            RangeBound::Integer(v) => v as f64,
            RangeBound::Double(v) => v,
        }
    }
}

impl fmt::Display for RangeBound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RangeBound::Integer(v) => write!(f, "{v}"),
            RangeBound::Double(v) => f.write_str(&java_double_to_string(*v)),
        }
    }
}

impl From<i32> for RangeBound {
    fn from(value: i32) -> Self {
        RangeBound::Integer(i64::from(value))
    }
}

impl From<i16> for RangeBound {
    fn from(value: i16) -> Self {
        RangeBound::Integer(i64::from(value))
    }
}

impl From<i64> for RangeBound {
    fn from(value: i64) -> Self {
        RangeBound::Integer(value)
    }
}

impl From<f64> for RangeBound {
    fn from(value: f64) -> Self {
        RangeBound::Double(value)
    }
}

impl Range {
    /// A numeric range that checks only the lower bound.
    #[doc(alias = "org.apache.kafka.common.config.ConfigDef$Range#atLeast")]
    pub(crate) fn at_least(min: impl Into<RangeBound>) -> Self {
        Self { min: Some(min.into()), max: None }
    }

    /// A numeric range that checks both the upper (inclusive) and lower
    /// bound.
    #[doc(alias = "org.apache.kafka.common.config.ConfigDef$Range#between")]
    pub(crate) fn between(min: impl Into<RangeBound>, max: impl Into<RangeBound>) -> Self {
        Self { min: Some(min.into()), max: Some(max.into()) }
    }
}

impl Validator for Range {
    fn ensure_valid(&self, name: &str, value: Option<&ParsedValue>) -> Result<(), Error> {
        let Some(value) = value else {
            return Err(config_error(name, None, "Value must be non-null"));
        };
        let Some(n) = value.double_value() else {
            return Err(class_cast(value, "java.lang.Number"));
        };
        if let Some(min) = self.min
            && n < min.double_value()
        {
            return Err(config_error(name, Some(value), format!("Value must be at least {min}")));
        }
        if let Some(max) = self.max
            && n > max.double_value()
        {
            return Err(config_error(name, Some(value), format!("Value must be no more than {max}")));
        }
        Ok(())
    }
}

impl fmt::Display for Range {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.min, self.max) {
            (None, None) => f.write_str("[...]"),
            (None, Some(max)) => write!(f, "[...,{max}]"),
            (Some(min), None) => write!(f, "[{min},...]"),
            (Some(min), Some(max)) => write!(f, "[{min},...,{max}]"),
        }
    }
}

/// Java's `ConfigDef.ValidList`: validates a parsed `LIST` value.
///
/// Java's `ConfigDef.parseValue` calls `ensureValid` directly, without
/// wrapping (`ConfigDef.java:553`), and `ValidList` throws the
/// single-message `ConfigException(String)` for its own checks. So those
/// errors are [`Error::config_message`], with no `Invalid value ... for
/// configuration` prefix; an element outside the valid strings fails with
/// [`ValidString`]'s message, which has it.
#[doc(alias = "org.apache.kafka.common.config.ConfigDef$ValidList")]
pub(crate) struct ValidList {
    valid_string: ValidString,
    is_empty_allowed: bool,
    is_null_allowed: bool,
}

impl ValidList {
    /// Java's `ValidList.anyNonDuplicateValues(isEmptyAllowed, isNullAllowed)`:
    /// any elements are allowed, provided none is empty or duplicated.
    /// `is_empty_allowed` decides whether the list itself may be empty.
    #[doc(alias = "org.apache.kafka.common.config.ConfigDef$ValidList#anyNonDuplicateValues")]
    pub(crate) fn any_non_duplicate_values(is_empty_allowed: bool, is_null_allowed: bool) -> Self {
        Self { valid_string: ValidString::new(Vec::new()), is_empty_allowed, is_null_allowed }
    }

    /// Java's `ValidList.in(String... validStrings)`: each element must be one
    /// of `valid_strings`; the list may be empty but not null.
    #[doc(alias = "org.apache.kafka.common.config.ConfigDef$ValidList#in")]
    #[cfg_attr(not(test), expect(dead_code, reason = "no client key uses it; ConfigDefTest does"))]
    pub(crate) fn r#in(valid_strings: &[&str]) -> Self {
        Self {
            valid_string: ValidString::r#in(valid_strings),
            is_empty_allowed: true,
            is_null_allowed: false,
        }
    }

    /// Java's `ValidList.in(boolean isEmptyAllowed, String... validStrings)`.
    ///
    /// # Panics
    ///
    /// With Java's `IllegalArgumentException` message `At least one valid
    /// string must be provided when empty values are not allowed` if
    /// `is_empty_allowed` is false and `valid_strings` is empty, a
    /// programming error.
    #[doc(alias = "org.apache.kafka.common.config.ConfigDef$ValidList#in")]
    pub(crate) fn in_with_is_empty_allowed_valid_strings(is_empty_allowed: bool, valid_strings: &[&str]) -> Self {
        assert!(
            is_empty_allowed || !valid_strings.is_empty(),
            "At least one valid string must be provided when empty values are not allowed"
        );
        Self {
            valid_string: ValidString::r#in(valid_strings),
            is_empty_allowed,
            is_null_allowed: false,
        }
    }

    /// Java's `ValidList.ensureValid(name, value)` on a list value, with
    /// Java's check order: null, then an empty list, then duplicates, then
    /// each element.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] with Java's exact message when the value is null and
    /// nulls are not allowed, the list is empty and empty lists are not
    /// allowed, an element is repeated, an element is the empty string, or an
    /// element is not one of the valid strings.
    ///
    /// The list-typed body of [`Validator::ensure_valid`], which also serves
    /// [`Self::parse_any_non_duplicate_values`]'s already-parsed list.
    fn check_list(&self, name: &str, value: Option<&[String]>) -> Result<(), Error> {
        let Some(values) = value else {
            if self.is_null_allowed {
                return Ok(());
            }
            return Err(Error::config_message(format!(
                "Configuration '{name}' values must not be null."
            )));
        };
        if !self.is_empty_allowed && values.is_empty() {
            let valid_string = if self.valid_string.valid_strings.is_empty() {
                "any non-empty value".to_string()
            } else {
                self.valid_string.to_string()
            };
            return Err(Error::config_message(format!(
                "Configuration '{name}' must not be empty. Valid values include: {valid_string}"
            )));
        }
        // `Set.copyOf(values).size() != values.size()`.
        let unique: HashSet<&str> = values.iter().map(String::as_str).collect();
        if unique.len() != values.len() {
            return Err(Error::config_message(format!(
                "Configuration '{name}' values must not be duplicated."
            )));
        }
        // `validateIndividualValues`.
        let has_valid_strings = !self.valid_string.valid_strings.is_empty();
        for value in values {
            if value.is_empty() {
                return Err(Error::config_message(format!(
                    "Configuration '{name}' values must not be empty."
                )));
            }
            if has_valid_strings {
                self.valid_string.ensure_valid_string(name, Some(value))?;
            }
        }
        Ok(())
    }

    /// Parse `value` as a `LIST` ([`ConfigDef::parse_list`]) and validate it
    /// with `anyNonDuplicateValues(is_empty_allowed, false)`, which is what
    /// `ConfigDef.parse` does for each such key.
    ///
    /// As in `ConfigDef.parseValue` (`ConfigDef.java:544-551`), duplicates are
    /// removed first, keeping the order of first occurrence, with a warning;
    /// the deduplicated list is validated and returned. The duplicate check in
    /// [`Self::check_list`] therefore cannot fire here, as in Java.
    ///
    /// # Errors
    ///
    /// As [`Self::check_list`].
    pub(crate) fn parse_any_non_duplicate_values(
        name: &str,
        value: &str,
        is_empty_allowed: bool,
    ) -> Result<Vec<String>, Error> {
        let original = ConfigDef::parse_list(value);
        let values = distinct(&original);
        if values.len() != original.len() {
            log::warn!(
                "Configuration key \"{}\" contains duplicate values. Duplicates will be removed. \
                 The original value is: [{}], the updated value is: [{}]",
                name,
                original.join(", "),
                values.join(", ")
            );
        }
        Self::any_non_duplicate_values(is_empty_allowed, false).check_list(name, Some(&values))?;
        Ok(values)
    }
}

impl Validator for ValidList {
    #[doc(alias = "org.apache.kafka.common.config.ConfigDef$ValidList#ensureValid")]
    fn ensure_valid(&self, name: &str, value: Option<&ParsedValue>) -> Result<(), Error> {
        self.check_list(name, as_list(value)?)
    }

    fn is_valid_list(&self) -> bool {
        true
    }
}

impl fmt::Display for ValidList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.valid_string.valid_strings.is_empty() {
            Ok(())
        } else {
            self.valid_string.fmt(f)
        }
    }
}

/// Java's `ConfigDef.ValidString`: the value must be one of a list of
/// strings, compared case-sensitively.
#[doc(alias = "org.apache.kafka.common.config.ConfigDef$ValidString")]
pub(crate) struct ValidString {
    valid_strings: Vec<String>,
}

impl ValidString {
    fn new(valid_strings: Vec<String>) -> Self {
        Self { valid_strings }
    }

    /// Java's `ValidString.in(String... validStrings)`.
    #[doc(alias = "org.apache.kafka.common.config.ConfigDef$ValidString#in")]
    pub(crate) fn r#in(valid_strings: &[&str]) -> Self {
        Self::new(valid_strings.iter().map(|s| (*s).to_string()).collect())
    }

    /// Java's `ensureValid` on a `String` value (`None` is `null`, which no
    /// list of strings contains).
    fn ensure_valid_string(&self, name: &str, value: Option<&str>) -> Result<(), Error> {
        if value.is_some_and(|s| self.valid_strings.iter().any(|v| v == s)) {
            return Ok(());
        }
        Err(Error::config_name_value_message(
            name,
            value.unwrap_or("null"),
            format!("String must be one of: {}", self.valid_strings.join(", ")),
        ))
    }
}

impl Validator for ValidString {
    fn ensure_valid(&self, name: &str, value: Option<&ParsedValue>) -> Result<(), Error> {
        self.ensure_valid_string(name, as_string(value)?)
    }
}

impl fmt::Display for ValidString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}]", self.valid_strings.join(", "))
    }
}

/// Java's `ConfigDef.CaseInsensitiveValidString`: the value must be one of a
/// set of strings, compared after upper-casing both (`Locale.ROOT`).
#[doc(alias = "org.apache.kafka.common.config.ConfigDef$CaseInsensitiveValidString")]
pub(crate) struct CaseInsensitiveValidString {
    /// The upper-cased strings, in the iteration order of the Java `HashSet`
    /// that holds them, which its messages print.
    valid_strings: Vec<String>,
}

impl CaseInsensitiveValidString {
    /// Java's `CaseInsensitiveValidString.in(String... validStrings)`.
    #[doc(alias = "org.apache.kafka.common.config.ConfigDef$CaseInsensitiveValidString#in")]
    pub(crate) fn r#in(valid_strings: &[&str]) -> Self {
        Self {
            valid_strings: java_hash_set_order(valid_strings.iter().map(|s| s.to_uppercase())),
        }
    }
}

impl Validator for CaseInsensitiveValidString {
    fn ensure_valid(&self, name: &str, value: Option<&ParsedValue>) -> Result<(), Error> {
        let s = as_string(value)?;
        if s.is_some_and(|s| self.valid_strings.contains(&s.to_uppercase())) {
            return Ok(());
        }
        Err(Error::config_name_value_message(
            name,
            s.unwrap_or("null"),
            format!("String must be one of (case insensitive): {}", self.valid_strings.join(", ")),
        ))
    }
}

impl fmt::Display for CaseInsensitiveValidString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(case insensitive) [{}]", self.valid_strings.join(", "))
    }
}

/// Java's `ConfigDef.NonNullValidator`.
#[doc(alias = "org.apache.kafka.common.config.ConfigDef$NonNullValidator")]
pub(crate) struct NonNullValidator;

impl Validator for NonNullValidator {
    fn ensure_valid(&self, name: &str, value: Option<&ParsedValue>) -> Result<(), Error> {
        if value.is_none() {
            // Pass in the string null to avoid the spotbugs warning
            return Err(Error::config_name_value_message(name, "null", "entry must be non null"));
        }
        Ok(())
    }
}

impl fmt::Display for NonNullValidator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("non-null string")
    }
}

/// Java's `ConfigDef.CompositeValidator`: every validator, in order.
#[doc(alias = "org.apache.kafka.common.config.ConfigDef$CompositeValidator")]
pub(crate) struct CompositeValidator {
    validators: Vec<Box<dyn Validator>>,
}

impl CompositeValidator {
    /// Java's `CompositeValidator.of(Validator... validators)`.
    #[doc(alias = "org.apache.kafka.common.config.ConfigDef$CompositeValidator#of")]
    pub(crate) fn of(validators: Vec<Box<dyn Validator>>) -> Self {
        Self { validators }
    }
}

impl Validator for CompositeValidator {
    fn ensure_valid(&self, name: &str, value: Option<&ParsedValue>) -> Result<(), Error> {
        for validator in &self.validators {
            validator.ensure_valid(name, value)?;
        }
        Ok(())
    }
}

impl fmt::Display for CompositeValidator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, validator) in self.validators.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            validator.fmt(f)?;
        }
        Ok(())
    }
}

/// Java's `ConfigDef.NonEmptyString`: a `null` value passes, an empty string
/// does not.
#[doc(alias = "org.apache.kafka.common.config.ConfigDef$NonEmptyString")]
pub(crate) struct NonEmptyString;

impl Validator for NonEmptyString {
    fn ensure_valid(&self, name: &str, value: Option<&ParsedValue>) -> Result<(), Error> {
        if as_string(value)?.is_some_and(str::is_empty) {
            return Err(config_error(name, value, "String must be non-empty"));
        }
        Ok(())
    }
}

impl fmt::Display for NonEmptyString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("non-empty string")
    }
}

/// Java's `ConfigDef.ListSize`: a list of at most `max_size` elements.
#[doc(alias = "org.apache.kafka.common.config.ConfigDef$ListSize")]
pub(crate) struct ListSize {
    max_size: usize,
}

impl ListSize {
    /// Java's `ListSize.atMostOfSize(int maxSize)`.
    #[doc(alias = "org.apache.kafka.common.config.ConfigDef$ListSize#atMostOfSize")]
    pub(crate) fn at_most_of_size(max_size: usize) -> Self {
        Self { max_size }
    }
}

impl Validator for ListSize {
    /// Java casts the value to a `List` and calls `size()` on it, so a `null`
    /// value throws a `NullPointerException`; with no such error class here,
    /// it is an [`Error::LocalIllegalArgument`] with the JDK's message.
    fn ensure_valid(&self, name: &str, value: Option<&ParsedValue>) -> Result<(), Error> {
        let Some(values) = as_list(value)? else {
            return Err(Error::local_illegal_argument(
                "Cannot invoke \"java.util.List.size()\" because \"values\" is null",
            ));
        };
        if values.len() > self.max_size {
            return Err(config_error(
                name,
                value,
                format!("exceeds maximum list size of [{}].", self.max_size),
            ));
        }
        Ok(())
    }
}

impl fmt::Display for ListSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "List containing maximum of {} elements", self.max_size)
    }
}

/// Translated from `ConfigDefTest`.
///
/// Not translated, with the reason:
///
/// - `testNestedClass`, `testClassWithAlias`,
///   `testConvertValueToStringClass`, `testConvertValueToStringNestedClass`:
///   the `CLASS` type is not translated (CLAUDE.md §2). For the same reason
///   the `CLASS` key `f` of `testBasicTypes` and the `CLASS` row of
///   `testBadInputs` are dropped.
/// - `testBadInputs`'s `new Object()` rows: a value of a class outside the
///   config types cannot be built, `ParsedValue` being closed.
/// - `testValidators`' `NonEmptyStringWithoutControlChars` row: the
///   validator is not translated, no client key uses it.
/// - `testGroupInference`, `testParseForValidate`, `testValidate`,
///   `testValidateMissingConfigKey`, `testValidateCannotParse`:
///   `validate`, `ConfigValue`, `Recommender` and groups are not translated
///   (public API of the deferred public `ConfigDef`). `testCanAddInternalConfig`
///   checks through `parse` instead of `validate`.
/// - `testInternalConfigDoesntShowUpInDocs`, `testDynamicUpdateModeInDocs`,
///   `toRst`, `toEnrichedRst`, `testNiceMemoryUnits`, `testNiceTimeUnits`,
///   the `testConvertValueToString*` tests: the documentation renderers and
///   `convertToString` are not translated. The double cases of
///   `testConvertValueToStringDouble` are kept against
///   [`java_double_to_string`], which the messages use.
/// - `testMissingDependentConfigs`, `testBaseConfigDefDependents`: dependents
///   are not translated.
#[cfg(test)]
mod tests {
    use super::*;

    fn props(entries: &[(&str, ParsedValue)]) -> HashMap<String, Option<ParsedValue>> {
        entries.iter().map(|(k, v)| ((*k).to_string(), Some(v.clone()))).collect()
    }

    fn config_error_message<T: fmt::Debug>(result: Result<T, Error>) -> String {
        match result {
            Err(Error::Config(e)) => e.message().to_string(),
            other => panic!("expected a ConfigError, got {other:?}"),
        }
    }

    fn panic_message(f: impl FnOnce() + std::panic::UnwindSafe) -> String {
        let payload = std::panic::catch_unwind(f).expect_err("expected a panic");
        payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_string()))
            .unwrap_or_default()
    }

    #[test]
    fn test_basic_types() {
        let def = ConfigDef::new()
            .define_with_validator("a", Type::Int, 5, Range::between(0, 14), Importance::High, "docs")
            .define("b", Type::Long, ConfigDef::NO_DEFAULT_VALUE, Importance::High, "docs")
            .define("c", Type::String, "hello", Importance::High, "docs")
            .define("d", Type::List, ConfigDef::NO_DEFAULT_VALUE, Importance::High, "docs")
            .define("e", Type::Double, ConfigDef::NO_DEFAULT_VALUE, Importance::High, "docs")
            .define("g", Type::Boolean, ConfigDef::NO_DEFAULT_VALUE, Importance::High, "docs")
            .define("h", Type::Boolean, ConfigDef::NO_DEFAULT_VALUE, Importance::High, "docs")
            .define("i", Type::Boolean, ConfigDef::NO_DEFAULT_VALUE, Importance::High, "docs")
            .define("j", Type::Password, ConfigDef::NO_DEFAULT_VALUE, Importance::High, "docs");

        let props = props(&[
            ("a", "1   ".into()),
            // Java puts the `Integer` 2 for the LONG key.
            ("b", 2.into()),
            ("d", " a , b, c".into()),
            ("e", 42.5.into()),
            ("g", "true".into()),
            ("h", "FalSE".into()),
            ("i", "TRUE".into()),
            ("j", "password".into()),
        ]);

        let vals = def.parse(&props).unwrap();
        assert_eq!(vals["a"], Some(ParsedValue::Int(1)));
        assert_eq!(vals["b"], Some(ParsedValue::Long(2)));
        assert_eq!(vals["c"], Some(ParsedValue::String("hello".into())));
        assert_eq!(vals["d"], Some(ParsedValue::List(vec!["a".into(), "b".into(), "c".into()])));
        assert_eq!(vals["e"], Some(ParsedValue::Double(42.5)));
        assert_eq!(vals["g"], Some(ParsedValue::Boolean(true)));
        assert_eq!(vals["h"], Some(ParsedValue::Boolean(false)));
        assert_eq!(vals["i"], Some(ParsedValue::Boolean(true)));
        assert_eq!(vals["j"], Some(ParsedValue::Password(Password::new("password"))));
        assert_eq!(vals["j"].as_ref().unwrap().to_string(), Password::HIDDEN);
    }

    #[test]
    fn test_invalid_default() {
        assert_eq!(
            panic_message(|| {
                ConfigDef::new().define("a", Type::Int, "hello", Importance::High, "docs");
            }),
            "Invalid value hello for configuration a: Not a number of type INT"
        );
    }

    #[test]
    fn test_null_default() {
        let def = ConfigDef::new().define("a", Type::Int, DefaultValue::Null, Importance::High, "docs");
        let vals = def.parse(&HashMap::new()).unwrap();
        assert_eq!(vals["a"], None);
    }

    #[test]
    fn test_missing_required() {
        let def = ConfigDef::new().define("a", Type::Int, ConfigDef::NO_DEFAULT_VALUE, Importance::High, "docs");
        assert_eq!(
            config_error_message(def.parse(&HashMap::new())),
            "Missing required configuration \"a\" which has no default value."
        );
    }

    #[test]
    fn test_parsing_empty_default_value_for_string_field_should_succeed() {
        ConfigDef::new()
            .define("a", Type::String, "", Importance::High, "docs")
            .parse(&HashMap::new())
            .unwrap();
    }

    #[test]
    fn test_defined_twice() {
        assert_eq!(
            panic_message(|| {
                ConfigDef::new()
                    .define("a", Type::String, ConfigDef::NO_DEFAULT_VALUE, Importance::High, "docs")
                    .define("a", Type::Int, ConfigDef::NO_DEFAULT_VALUE, Importance::High, "docs");
            }),
            "Configuration a is defined twice."
        );
    }

    fn check_bad_inputs(r#type: Type, values: &[(ParsedValue, &str)]) {
        for (value, message) in values {
            let def = ConfigDef::new().define("name", r#type, ConfigDef::NO_DEFAULT_VALUE, Importance::High, "docs");
            assert_eq!(
                config_error_message(def.parse(&props(&[("name", value.clone())]))),
                *message,
                "Expected a config exception on bad input for value {value}"
            );
        }
    }

    #[test]
    fn test_bad_inputs() {
        let long_max = i64::MAX.to_string();
        let long_max_00 = format!("{}00", i64::MAX);
        check_bad_inputs(
            Type::Int,
            &[
                (
                    "hello".into(),
                    "Invalid value hello for configuration name: Not a number of type INT",
                ),
                (
                    "42.5".into(),
                    "Invalid value 42.5 for configuration name: Not a number of type INT",
                ),
                (
                    42.5.into(),
                    "Invalid value 42.5 for configuration name: Expected value to be a 32-bit integer, but it was a \
                     java.lang.Double",
                ),
                (
                    i64::MAX.into(),
                    "Invalid value 9223372036854775807 for configuration name: Expected value to be a 32-bit integer, \
                     but it was a java.lang.Long",
                ),
                (
                    long_max.as_str().into(),
                    "Invalid value 9223372036854775807 for configuration name: Not a number of type INT",
                ),
            ],
        );
        check_bad_inputs(
            Type::Long,
            &[
                (
                    "hello".into(),
                    "Invalid value hello for configuration name: Not a number of type LONG",
                ),
                (
                    "42.5".into(),
                    "Invalid value 42.5 for configuration name: Not a number of type LONG",
                ),
                (
                    long_max_00.as_str().into(),
                    "Invalid value 922337203685477580700 for configuration name: Not a number of type LONG",
                ),
            ],
        );
        check_bad_inputs(
            Type::Double,
            &[(
                "hello".into(),
                "Invalid value hello for configuration name: Not a number of type DOUBLE",
            )],
        );
        check_bad_inputs(
            Type::List,
            &[(
                53.into(),
                "Invalid value 53 for configuration name: Expected a comma separated list.",
            )],
        );
        check_bad_inputs(
            Type::Boolean,
            &[
                (
                    "hello".into(),
                    "Invalid value hello for configuration name: Expected value to be either true or false",
                ),
                (
                    "truee".into(),
                    "Invalid value truee for configuration name: Expected value to be either true or false",
                ),
                (
                    "fals".into(),
                    "Invalid value fals for configuration name: Expected value to be either true or false",
                ),
            ],
        );
        // Java's `new Object()` row for STRING, with a value of another class.
        check_bad_inputs(
            Type::String,
            &[(
                1.into(),
                "Invalid value 1 for configuration name: Expected value to be a string, but it was a java.lang.Integer",
            )],
        );
    }

    /// `Double.parseDouble` accepts what Java accepts, which Rust's parser
    /// spells differently.
    #[test]
    fn test_parse_java_double() {
        assert_eq!(parse_java_double("1.5d"), Some(1.5));
        assert_eq!(parse_java_double("1.5F"), Some(1.5));
        assert_eq!(parse_java_double("-Infinity"), Some(f64::NEG_INFINITY));
        assert!(parse_java_double("NaN").is_some_and(f64::is_nan));
        assert_eq!(parse_java_double(".5"), Some(0.5));
        assert_eq!(parse_java_double("1e3"), Some(1000.0));
        for bad in ["inf", "nan", "infinity", "", "+", "-+1", "1.5dd", "d", "."] {
            assert_eq!(parse_java_double(bad), None, "{bad}");
        }
    }

    #[test]
    fn test_invalid_default_range() {
        assert_eq!(
            panic_message(|| {
                ConfigDef::new().define_with_validator(
                    "name",
                    Type::Int,
                    -1,
                    Range::between(0, 10),
                    Importance::High,
                    "docs",
                );
            }),
            "Invalid value -1 for configuration name: Value must be at least 0"
        );
    }

    #[test]
    fn test_invalid_default_string() {
        assert_eq!(
            panic_message(|| {
                ConfigDef::new().define_with_validator(
                    "name",
                    Type::String,
                    "bad",
                    ValidString::r#in(&["valid", "values"]),
                    Importance::High,
                    "docs",
                );
            }),
            "Invalid value bad for configuration name: String must be one of: valid, values"
        );
    }

    fn check_validators(
        r#type: Type,
        validator: impl Validator + 'static,
        default_val: ParsedValue,
        ok_values: &[ParsedValue],
        bad_values: &[(Option<ParsedValue>, &str)],
    ) {
        let def =
            ConfigDef::new().define_with_validator("name", r#type, default_val, validator, Importance::High, "docs");
        for value in ok_values {
            def.parse(&props(&[("name", value.clone())])).unwrap();
        }
        for (value, message) in bad_values {
            let mut m = HashMap::new();
            m.insert("name".to_string(), value.clone());
            assert_eq!(
                config_error_message(def.parse(&m)),
                *message,
                "Expected a config exception due to invalid value {value:?}"
            );
        }
    }

    #[test]
    fn test_validators() {
        check_validators(
            Type::Int,
            Range::between(0, 10),
            5.into(),
            &[1.into(), 5.into(), 9.into()],
            &[
                (
                    Some((-1).into()),
                    "Invalid value -1 for configuration name: Value must be at least 0",
                ),
                (
                    Some(11.into()),
                    "Invalid value 11 for configuration name: Value must be no more than 10",
                ),
                (None, "Invalid value null for configuration name: Value must be non-null"),
            ],
        );
        check_validators(
            Type::String,
            ValidString::r#in(&["good", "values", "default"]),
            "default".into(),
            &["good".into(), "values".into(), "default".into()],
            &[
                (
                    Some("bad".into()),
                    "Invalid value bad for configuration name: String must be one of: good, values, default",
                ),
                (
                    Some("inputs".into()),
                    "Invalid value inputs for configuration name: String must be one of: good, values, default",
                ),
                (
                    Some("DEFAULT".into()),
                    "Invalid value DEFAULT for configuration name: String must be one of: good, values, default",
                ),
                (
                    None,
                    "Invalid value null for configuration name: String must be one of: good, values, default",
                ),
            ],
        );
        // Java prints the upper-cased strings in its `HashSet`'s order.
        check_validators(
            Type::String,
            CaseInsensitiveValidString::r#in(&["good", "values", "default"]),
            "default".into(),
            &["gOOd".into(), "VALUES".into(), "default".into()],
            &[
                (
                    Some("Bad".into()),
                    "Invalid value Bad for configuration name: String must be one of (case insensitive): VALUES, \
                     GOOD, DEFAULT",
                ),
                (
                    Some("iNPUts".into()),
                    "Invalid value iNPUts for configuration name: String must be one of (case insensitive): VALUES, \
                     GOOD, DEFAULT",
                ),
                (
                    None,
                    "Invalid value null for configuration name: String must be one of (case insensitive): VALUES, \
                     GOOD, DEFAULT",
                ),
            ],
        );
        check_validators(
            Type::List,
            ValidList::r#in(&["1", "2", "3"]),
            "1".into(),
            &["1".into(), "2".into(), "3".into()],
            &[
                (
                    Some("4".into()),
                    "Invalid value 4 for configuration name: String must be one of: 1, 2, 3",
                ),
                (
                    Some("5".into()),
                    "Invalid value 5 for configuration name: String must be one of: 1, 2, 3",
                ),
                (
                    Some("6".into()),
                    "Invalid value 6 for configuration name: String must be one of: 1, 2, 3",
                ),
            ],
        );
        check_validators(
            Type::String,
            NonNullValidator,
            "a".into(),
            &["abb".into()],
            &[(None, "Invalid value null for configuration name: entry must be non null")],
        );
        check_validators(
            Type::String,
            CompositeValidator::of(vec![Box::new(NonNullValidator), Box::new(ValidString::r#in(&["a", "b"]))]),
            "a".into(),
            &["a".into(), "b".into()],
            &[
                (None, "Invalid value null for configuration name: entry must be non null"),
                (
                    Some((-1).into()),
                    "Invalid value -1 for configuration name: Expected value to be a string, but it was a \
                     java.lang.Integer",
                ),
                (
                    Some("c".into()),
                    "Invalid value c for configuration name: String must be one of: a, b",
                ),
            ],
        );
    }

    /// The `HashSet` order Java prints for `group.protocol`'s
    /// `CaseInsensitiveValidString.in("classic", "consumer")`, and the
    /// `toString` forms of the validators.
    #[test]
    fn test_validator_to_string() {
        assert_eq!(
            CaseInsensitiveValidString::r#in(&["classic", "consumer"]).to_string(),
            "(case insensitive) [CONSUMER, CLASSIC]"
        );
        assert_eq!(Range::between(0, 14).to_string(), "[0,...,14]");
        assert_eq!(Range::at_least(0.5).to_string(), "[0.5,...]");
        assert_eq!(ValidString::r#in(&["a", "b"]).to_string(), "[a, b]");
        assert_eq!(ValidList::r#in(&["a", "b"]).to_string(), "[a, b]");
        assert_eq!(ValidList::any_non_duplicate_values(true, true).to_string(), "");
        assert_eq!(NonNullValidator.to_string(), "non-null string");
        assert_eq!(NonEmptyString.to_string(), "non-empty string");
        assert_eq!(
            CompositeValidator::of(vec![Box::new(NonNullValidator), Box::new(NonEmptyString)]).to_string(),
            "non-null string, non-empty string"
        );
    }

    #[test]
    fn test_non_empty_string() {
        assert!(NonEmptyString.ensure_valid("name", None).is_ok());
        assert!(NonEmptyString.ensure_valid("name", Some(&"a".into())).is_ok());
        assert_eq!(
            config_error_message(NonEmptyString.ensure_valid("name", Some(&"".into()))),
            "Invalid value  for configuration name: String must be non-empty"
        );
    }

    #[test]
    fn test_ssl_passwords() {
        let def = ConfigDef::new().with_client_ssl_support();
        let props = props(&[
            (
                crate::common::config::SslConfigs::SSL_KEY_PASSWORD_CONFIG,
                "key_password".into(),
            ),
            (
                crate::common::config::SslConfigs::SSL_KEYSTORE_PASSWORD_CONFIG,
                "keystore_password".into(),
            ),
            (
                crate::common::config::SslConfigs::SSL_TRUSTSTORE_PASSWORD_CONFIG,
                "truststore_password".into(),
            ),
        ]);
        let vals = def.parse(&props).unwrap();
        for (key, password) in [
            (crate::common::config::SslConfigs::SSL_KEY_PASSWORD_CONFIG, "key_password"),
            (
                crate::common::config::SslConfigs::SSL_KEYSTORE_PASSWORD_CONFIG,
                "keystore_password",
            ),
            (
                crate::common::config::SslConfigs::SSL_TRUSTSTORE_PASSWORD_CONFIG,
                "truststore_password",
            ),
        ] {
            assert_eq!(vals[key], Some(ParsedValue::Password(Password::new(password))));
            assert_eq!(vals[key].as_ref().unwrap().to_string(), Password::HIDDEN);
        }
    }

    #[test]
    fn test_null_default_with_validator() {
        let key = "enum_test";
        let def = ConfigDef::new().define_with_validator(
            key,
            Type::String,
            ConfigDef::NO_DEFAULT_VALUE,
            ValidString::r#in(&["ONE", "TWO", "THREE"]),
            Importance::High,
            "docs",
        );
        let vals = def.parse(&props(&[(key, "ONE".into())])).unwrap();
        assert_eq!(vals[key], Some(ParsedValue::String("ONE".into())));
    }

    #[test]
    fn test_can_add_internal_config() {
        let config_name = "internal.config";
        let config_def = ConfigDef::new().define_internal(config_name, Type::String, "", Importance::Low);
        let vals = config_def.parse(&props(&[(config_name, "value".into())])).unwrap();
        assert_eq!(vals[config_name], Some(ParsedValue::String("value".into())));
        let key = config_def.config_key(config_name).unwrap();
        assert!(key.internal_config);
        assert_eq!(key.importance, Importance::Low);
        assert_eq!(key.documentation, "");
    }

    #[test]
    fn test_names() {
        let config_def = ConfigDef::new()
            .define("a", Type::String, ConfigDef::NO_DEFAULT_VALUE, Importance::Low, "docs")
            .define("b", Type::String, ConfigDef::NO_DEFAULT_VALUE, Importance::Low, "docs");
        assert_eq!(config_def.names(), HashSet::from(["a".to_string(), "b".to_string()]));
        // Definition order is kept, as Java's `LinkedHashMap` keeps it.
        let ordered: Vec<&str> = config_def.config_keys().map(|k| k.name.as_str()).collect();
        assert_eq!(ordered, ["a", "b"]);
    }

    /// The double cases of `testConvertValueToStringDouble`.
    #[test]
    fn test_java_double_to_string() {
        assert_eq!(java_double_to_string(3.125), "3.125");
        assert_eq!(java_double_to_string(f64::MAX), "1.7976931348623157E308");
        assert_eq!(java_double_to_string(102400000.0), "1.024E8");
        assert_eq!(java_double_to_string(-102400000.0), "-1.024E8");
        assert_eq!(java_double_to_string(0.5), "0.5");
        assert_eq!(java_double_to_string(1.0), "1.0");
        assert_eq!(java_double_to_string(1e7), "1.0E7");
        assert_eq!(java_double_to_string(1e-4), "1.0E-4");
        assert_eq!(java_double_to_string(0.001), "0.001");
        assert_eq!(java_double_to_string(-0.0), "-0.0");
    }

    #[test]
    fn test_throws_exception_when_list_size_exceeds_limit() {
        assert_eq!(
            panic_message(|| {
                ConfigDef::new().define_with_validator(
                    "lst",
                    Type::List,
                    vec!["a".to_string(), "b".to_string()],
                    ListSize::at_most_of_size(1),
                    Importance::High,
                    "lst doc",
                );
            }),
            "Invalid value [a, b] for configuration lst: exceeds maximum list size of [1]."
        );
    }

    #[test]
    fn test_no_exception_is_thrown_when_list_size_equals_the_limit() {
        let lst = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let size = lst.len();
        ConfigDef::new().define_with_validator(
            "lst",
            Type::List,
            lst,
            ListSize::at_most_of_size(size),
            Importance::High,
            "lst doc",
        );
    }

    #[test]
    fn test_no_exception_is_thrown_when_list_size_is_below_the_limit() {
        ConfigDef::new().define_with_validator(
            "lst",
            Type::List,
            vec!["a".to_string(), "b".to_string()],
            ListSize::at_most_of_size(3),
            Importance::High,
            "lst doc",
        );
    }

    #[test]
    fn test_list_size_validator_to_string() {
        assert_eq!(
            ListSize::at_most_of_size(5).to_string(),
            "List containing maximum of 5 elements"
        );
    }

    fn list(values: &[&str]) -> Vec<String> {
        values.iter().map(|s| (*s).to_string()).collect()
    }

    fn ensure_valid_message(validator: &ValidList, value: Option<&[&str]>) -> String {
        let value = value.map(list);
        config_error_message(validator.check_list("test.config", value.as_deref()))
    }

    #[test]
    fn test_list_validator_any_non_duplicate_values() {
        let duplicated = "Configuration 'test.config' values must not be duplicated.";
        let empty_value = "Configuration 'test.config' values must not be empty.";
        let null = "Configuration 'test.config' values must not be null.";
        let empty_list = "Configuration 'test.config' must not be empty. Valid values include: any non-empty value";

        let v = ValidList::any_non_duplicate_values(true, true);
        assert!(v.check_list("test.config", Some(&list(&["a", "b", "c"]))).is_ok());
        assert!(v.check_list("test.config", Some(&[])).is_ok());
        assert!(v.check_list("test.config", None).is_ok());
        assert_eq!(ensure_valid_message(&v, Some(&["a", "a"])), duplicated);
        assert_eq!(ensure_valid_message(&v, Some(&[""])), empty_value);
        assert_eq!(ensure_valid_message(&v, Some(&["a", "", "b"])), empty_value);

        let v = ValidList::any_non_duplicate_values(false, true);
        assert!(v.check_list("test.config", Some(&list(&["a", "b", "c"]))).is_ok());
        assert!(v.check_list("test.config", None).is_ok());
        assert_eq!(ensure_valid_message(&v, Some(&[])), empty_list);
        assert_eq!(ensure_valid_message(&v, Some(&["a", "a"])), duplicated);
        assert_eq!(ensure_valid_message(&v, Some(&[""])), empty_value);
        assert_eq!(ensure_valid_message(&v, Some(&["a", "", "b"])), empty_value);

        let v = ValidList::any_non_duplicate_values(true, false);
        assert!(v.check_list("test.config", Some(&list(&["a", "b", "c"]))).is_ok());
        assert!(v.check_list("test.config", Some(&[])).is_ok());
        assert_eq!(ensure_valid_message(&v, None), null);
        assert_eq!(ensure_valid_message(&v, Some(&["a", "a"])), duplicated);
        assert_eq!(ensure_valid_message(&v, Some(&[""])), empty_value);
        assert_eq!(ensure_valid_message(&v, Some(&["a", "", "b"])), empty_value);

        let v = ValidList::any_non_duplicate_values(false, false);
        assert!(v.check_list("test.config", Some(&list(&["a", "b", "c"]))).is_ok());
        assert_eq!(ensure_valid_message(&v, None), null);
        assert_eq!(ensure_valid_message(&v, Some(&[])), empty_list);
        assert_eq!(ensure_valid_message(&v, Some(&["a", "a"])), duplicated);
        assert_eq!(ensure_valid_message(&v, Some(&[""])), empty_value);
        assert_eq!(ensure_valid_message(&v, Some(&["a", "", "b"])), empty_value);
    }

    #[test]
    fn test_list_validator_in() {
        let null = "Configuration 'test.config' values must not be null.";
        let not_one_of = "Invalid value d for configuration test.config: String must be one of: a, b, c";
        let duplicated = "Configuration 'test.config' values must not be duplicated.";

        let allow_empty = ValidList::in_with_is_empty_allowed_valid_strings(true, &["a", "b", "c"]);
        assert!(allow_empty.check_list("test.config", Some(&list(&["a", "b"]))).is_ok());
        assert!(allow_empty.check_list("test.config", Some(&[])).is_ok());
        assert_eq!(ensure_valid_message(&allow_empty, None), null);
        assert_eq!(ensure_valid_message(&allow_empty, Some(&["d"])), not_one_of);
        assert_eq!(ensure_valid_message(&allow_empty, Some(&["a", "a"])), duplicated);

        let not_allow_empty = ValidList::in_with_is_empty_allowed_valid_strings(false, &["a", "b", "c"]);
        assert!(not_allow_empty.check_list("test.config", Some(&list(&["a", "b"]))).is_ok());
        assert_eq!(
            ensure_valid_message(&not_allow_empty, Some(&[])),
            "Configuration 'test.config' must not be empty. Valid values include: [a, b, c]"
        );
        assert_eq!(ensure_valid_message(&not_allow_empty, None), null);
        assert_eq!(ensure_valid_message(&not_allow_empty, Some(&["d"])), not_one_of);
        assert_eq!(ensure_valid_message(&not_allow_empty, Some(&["a", "a"])), duplicated);

        assert_eq!(
            panic_message(|| {
                ValidList::in_with_is_empty_allowed_valid_strings(false, &[]);
            }),
            "At least one valid string must be provided when empty values are not allowed"
        );
    }

    #[test]
    fn test_parsed_value_will_remove_duplicates_in_valid_list() {
        let def = ConfigDef::new().define_with_validator(
            "list",
            Type::List,
            Vec::<String>::new(),
            ValidList::any_non_duplicate_values(true, true),
            Importance::High,
            "list doc",
        );
        let parsed = def.parse(&props(&[("list", "a,b,c,a,b".into())])).unwrap();
        assert_eq!(parsed["list"], Some(ParsedValue::List(list(&["a", "b", "c"]))));
    }

    #[test]
    fn test_type_is_sensitive() {
        assert!(Type::Password.is_sensitive());
        assert!(!Type::String.is_sensitive());
        assert_eq!(Type::Password.to_string(), "PASSWORD");
    }

    // The tests below predate the ConfigDefTest translation and cover the
    // shared `String` parsing the client configs use.

    /// The value is trimmed as a whole and split on `\s*,\s*`.
    #[test]
    fn test_parse_list() {
        assert_eq!(ConfigDef::parse_list(" a , b, c"), vec!["a", "b", "c"]);
        assert_eq!(ConfigDef::parse_list("a:1,b:2"), vec!["a:1", "b:2"]);
        assert_eq!(ConfigDef::parse_list("a:1, b:2"), vec!["a:1", "b:2"]);
        assert_eq!(ConfigDef::parse_list(" a:1 ,b:2 "), vec!["a:1", "b:2"]);
        assert_eq!(ConfigDef::parse_list("a:1 , b:2 "), vec!["a:1", "b:2"]);
        assert_eq!(ConfigDef::parse_list("a:1\t,\nb:2"), vec!["a:1", "b:2"]);
        assert_eq!(ConfigDef::parse_list("a"), vec!["a"]);
    }

    /// An empty or blank value is the empty list, not `[""]`.
    #[test]
    fn test_parse_list_empty() {
        assert!(ConfigDef::parse_list("").is_empty());
        assert!(ConfigDef::parse_list("   ").is_empty());
    }

    /// `split(trimmed, -1)` keeps empty elements, trailing ones included, and
    /// whitespace inside an element is not touched.
    #[test]
    fn test_parse_list_keeps_empty_elements_and_inner_whitespace() {
        assert_eq!(ConfigDef::parse_list("a,,b"), vec!["a", "", "b"]);
        assert_eq!(ConfigDef::parse_list("a, ,b"), vec!["a", "", "b"]);
        assert_eq!(ConfigDef::parse_list("a,"), vec!["a", ""]);
        assert_eq!(ConfigDef::parse_list(",a"), vec!["", "a"]);
        assert_eq!(ConfigDef::parse_list("a b,c"), vec!["a b", "c"]);
    }

    /// `String.trim()` strips `<= U+0020` only; `\s` does not include U+00A0.
    #[test]
    fn test_parse_list_java_whitespace_classes() {
        assert_eq!(ConfigDef::trim("\u{1}a\u{1F} "), "a");
        assert_eq!(ConfigDef::trim("\u{A0}a"), "\u{A0}a");
        assert_eq!(ConfigDef::parse_list("a,\u{A0}b"), vec!["a", "\u{A0}b"]);
    }

    /// `ValidList.anyNonDuplicateValues` rejects an empty element with
    /// Java's exact message, including one produced by blank space between
    /// commas.
    #[test]
    fn test_valid_list_rejects_empty_element() {
        for value in ["a,,b", "a, ,b", "a,", ",a"] {
            assert_eq!(
                config_error_message(ValidList::parse_any_non_duplicate_values("k", value, true)),
                "Configuration 'k' values must not be empty."
            );
        }
    }

    /// `ConfigDef.parseValue` removes repeated elements (after whitespace
    /// around the commas is stripped), keeping first-occurrence order, before
    /// validating. `",,"` dedupes to `[""]`, which fails as empty.
    #[test]
    fn test_valid_list_removes_duplicates() {
        assert_eq!(
            ValidList::parse_any_non_duplicate_values("k", "a:1,a:1", false).unwrap(),
            vec!["a:1"]
        );
        assert_eq!(ValidList::parse_any_non_duplicate_values("k", "a,a", true).unwrap(), vec!["a"]);
        assert_eq!(
            ValidList::parse_any_non_duplicate_values("k", "b, a ,b", true).unwrap(),
            vec!["b", "a"]
        );
        assert_eq!(
            config_error_message(ValidList::parse_any_non_duplicate_values("k", ",,", true)),
            "Configuration 'k' values must not be empty."
        );
    }

    /// An empty (or blank) value is the empty list, which only
    /// `isEmptyAllowed = true` accepts.
    #[test]
    fn test_valid_list_empty_list() {
        for value in ["", "  "] {
            assert_eq!(
                config_error_message(ValidList::parse_any_non_duplicate_values("k", value, false)),
                "Configuration 'k' must not be empty. Valid values include: any non-empty value"
            );
            assert!(ValidList::parse_any_non_duplicate_values("k", value, true).unwrap().is_empty());
        }
    }

    /// A list of distinct non-empty elements passes and is returned parsed.
    #[test]
    fn test_valid_list_accepts_valid_list() {
        assert_eq!(
            ValidList::parse_any_non_duplicate_values("k", " a:1 , b:2", false).unwrap(),
            vec!["a:1", "b:2"]
        );
    }

    /// Java's `ConfigKey.hasDefault()`: only `NO_DEFAULT_VALUE` has none; a
    /// `null` default is a default.
    #[test]
    fn test_has_default() {
        let def = ConfigDef::new()
            .define("required", Type::String, ConfigDef::NO_DEFAULT_VALUE, Importance::High, "docs")
            .define("null", Type::String, DefaultValue::Null, Importance::High, "docs")
            .define("value", Type::Int, 5, Importance::High, "docs");
        assert!(!def.config_key("required").unwrap().has_default());
        assert!(def.config_key("null").unwrap().has_default());
        assert!(def.config_key("value").unwrap().has_default());
    }

    /// `defineInternal` with a validator validates the default and the value,
    /// as `define` with a validator does, and marks the key internal.
    #[test]
    fn test_can_add_internal_config_with_validator() {
        let name = "internal.config";
        let def =
            ConfigDef::new().define_internal_with_validator(name, Type::Int, 1, Range::at_least(0), Importance::Low);
        let key = def.config_key(name).unwrap();
        assert!(key.internal_config);
        assert_eq!(key.documentation, "");
        assert_eq!(
            def.parse(&props(&[(name, "3".into())])).unwrap()[name],
            Some(ParsedValue::Int(3))
        );
        assert_eq!(
            config_error_message(def.parse(&props(&[(name, "-1".into())]))),
            "Invalid value -1 for configuration internal.config: Value must be at least 0"
        );
        assert_eq!(
            panic_message(|| {
                let _ = ConfigDef::new().define_internal_with_validator(
                    name,
                    Type::Int,
                    -1,
                    Range::at_least(0),
                    Importance::Low,
                );
            }),
            "Invalid value -1 for configuration internal.config: Value must be at least 0"
        );
    }

    /// The `SHORT` arms of `parseType`, which `testBasicTypes` does not cover.
    #[test]
    fn test_parse_short() {
        let def = ConfigDef::new().define("s", Type::Short, 7i16, Importance::High, "docs");
        assert_eq!(def.parse(&props(&[])).unwrap()["s"], Some(ParsedValue::Short(7)));
        assert_eq!(
            def.parse(&props(&[("s", " 12 ".into())])).unwrap()["s"],
            Some(ParsedValue::Short(12))
        );
        assert_eq!(
            def.parse(&props(&[("s", 3i16.into())])).unwrap()["s"],
            Some(ParsedValue::Short(3))
        );
        assert_eq!(
            config_error_message(def.parse(&props(&[("s", "40000".into())]))),
            "Invalid value 40000 for configuration s: Not a number of type SHORT"
        );
        assert_eq!(
            config_error_message(def.parse(&props(&[("s", 1i32.into())]))),
            "Invalid value 1 for configuration s: Expected value to be a 16-bit integer (short), but it was a \
             java.lang.Integer"
        );
    }

    /// `SaslConfigs.addClientSaslSupport` defines the client `sasl.*` keys
    /// with Java's defaults and validators, without the class-valued keys.
    #[test]
    fn test_client_sasl_support() {
        use crate::common::config::SaslConfigs;
        let def = ConfigDef::new().with_client_sasl_support();
        let vals = def.parse(&props(&[])).unwrap();
        assert_eq!(vals[SaslConfigs::SASL_MECHANISM], Some(ParsedValue::String("GSSAPI".into())));
        assert_eq!(vals[SaslConfigs::SASL_JAAS_CONFIG], None);
        assert_eq!(
            vals["sasl.kerberos.ticket.renew.window.factor"],
            Some(ParsedValue::Double(0.80))
        );
        assert_eq!(vals["sasl.login.refresh.min.period.seconds"], Some(ParsedValue::Short(60)));
        assert_eq!(vals["sasl.login.refresh.buffer.seconds"], Some(ParsedValue::Short(300)));
        assert_eq!(vals["sasl.login.retry.backoff.ms"], Some(ParsedValue::Long(100)));
        assert_eq!(
            vals["sasl.oauthbearer.assertion.algorithm"],
            Some(ParsedValue::String("RS256".into()))
        );
        for class_key in [
            "sasl.client.callback.handler.class",
            "sasl.login.callback.handler.class",
            "sasl.login.class",
            "sasl.oauthbearer.jwt.retriever.class",
            "sasl.oauthbearer.jwt.validator.class",
        ] {
            assert!(def.config_key(class_key).is_none(), "{class_key}");
        }
        assert!(def.config_key(SaslConfigs::SASL_JAAS_CONFIG).unwrap().r#type.is_sensitive());
        assert!(!def.config_key(SaslConfigs::SASL_MECHANISM).unwrap().documentation.is_empty());
        assert_eq!(
            config_error_message(def.parse(&props(&[("sasl.login.refresh.buffer.seconds", "3601".into())]))),
            "Invalid value 3601 for configuration sasl.login.refresh.buffer.seconds: Value must be no more than 3600"
        );
        assert_eq!(
            config_error_message(def.parse(&props(&[("sasl.oauthbearer.assertion.algorithm", "HS256".into())]))),
            "Invalid value HS256 for configuration sasl.oauthbearer.assertion.algorithm: String must be one of \
             (case insensitive): ES256, RS256"
        );
    }

    /// `SslConfigs.addClientSslSupport` keeps the PEM store-type defaults.
    #[test]
    fn test_client_ssl_support_defaults() {
        use crate::common::config::SslConfigs;
        let def = ConfigDef::new().with_client_ssl_support();
        let vals = def.parse(&props(&[])).unwrap();
        assert_eq!(
            vals[SslConfigs::SSL_KEYSTORE_TYPE_CONFIG],
            Some(ParsedValue::String("PEM".into()))
        );
        assert_eq!(
            vals[SslConfigs::SSL_TRUSTSTORE_TYPE_CONFIG],
            Some(ParsedValue::String("PEM".into()))
        );
        assert!(def.config_key("ssl.engine.factory.class").is_none());
    }
}
