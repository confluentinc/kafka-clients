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

//! A configuration entry containing name, value and additional metadata.

use crate::common::Error;

/// Data type of configuration entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.clients.admin.ConfigEntry$ConfigType")]
pub enum ConfigType {
    /// Unknown data type.
    #[default]
    Unknown,
    /// Boolean.
    Boolean,
    /// String.
    String,
    /// Integer.
    Int,
    /// Short.
    Short,
    /// Long.
    Long,
    /// Double.
    Double,
    /// List.
    List,
    /// Class.
    Class,
    /// Password.
    Password,
}

impl ConfigType {
    /// Maps a `DescribeConfigsResponse.ConfigType` wire id to the public
    /// [`ConfigType`].
    ///
    /// Mirrors the composition of `DescribeConfigsResponse.ConfigType.forId`
    /// and `.type()`. Unlike Java's `forId`, which throws
    /// `IllegalArgumentException` for a negative id, this returns
    /// [`ConfigType::Unknown`] for any unrecognized id so response parsing never
    /// panics on a recoverable path.
    pub(crate) fn for_id(id: i8) -> ConfigType {
        match id {
            1 => ConfigType::Boolean,
            2 => ConfigType::String,
            3 => ConfigType::Int,
            4 => ConfigType::Short,
            5 => ConfigType::Long,
            6 => ConfigType::Double,
            7 => ConfigType::List,
            8 => ConfigType::Class,
            9 => ConfigType::Password,
            _ => ConfigType::Unknown,
        }
    }

    /// The name of this constant in Java's
    /// `org.apache.kafka.clients.admin.ConfigEntry$ConfigType`. The Java enum does not override
    /// `toString()`, so `Enum.toString()` returns this name.
    fn name(&self) -> &'static str {
        match self {
            ConfigType::Unknown => "UNKNOWN",
            ConfigType::Boolean => "BOOLEAN",
            ConfigType::String => "STRING",
            ConfigType::Int => "INT",
            ConfigType::Short => "SHORT",
            ConfigType::Long => "LONG",
            ConfigType::Double => "DOUBLE",
            ConfigType::List => "LIST",
            ConfigType::Class => "CLASS",
            ConfigType::Password => "PASSWORD",
        }
    }
}

/// Renders the Java constant name (`PASSWORD`, `UNKNOWN`, ...), as Java's
/// `Enum.toString()` does.
impl std::fmt::Display for ConfigType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Source of configuration entries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.clients.admin.ConfigEntry$ConfigSource")]
pub enum ConfigSource {
    /// Dynamic topic config that is configured for a specific topic.
    DynamicTopicConfig,
    /// Dynamic broker logger config that is configured for a specific broker.
    DynamicBrokerLoggerConfig,
    /// Dynamic broker config that is configured for a specific broker.
    DynamicBrokerConfig,
    /// Dynamic broker config that is configured as default for all brokers in
    /// the cluster.
    DynamicDefaultBrokerConfig,
    /// Dynamic client metrics subscription config that is configured for all
    /// clients.
    DynamicClientMetricsConfig,
    /// Dynamic group config that is configured for a specific group.
    DynamicGroupConfig,
    /// Static broker config provided as broker properties at start up (e.g.
    /// `server.properties` file).
    StaticBrokerConfig,
    /// Built-in default configuration for configs that have a default value.
    DefaultConfig,
    /// Source unknown, e.g. in the `ConfigEntry` used for alter requests where
    /// source is not set.
    Unknown,
}

impl ConfigSource {
    /// Maps a `DescribeConfigsResponse.ConfigSource` wire id (also carried on
    /// `CreatableTopicConfigs.configSource`) to the public [`ConfigSource`].
    ///
    /// Mirrors the composition of `DescribeConfigsResponse.ConfigSource.forId`
    /// and `KafkaAdminClient.configSource`. Unlike Java's `configSource`, which
    /// throws `IllegalArgumentException` for the `UNKNOWN` / client-metrics /
    /// group ids, this returns [`ConfigSource::Unknown`] for unrecognized ids so
    /// response parsing never panics on a recoverable path.
    pub(crate) fn for_id(id: i8) -> ConfigSource {
        match id {
            1 => ConfigSource::DynamicTopicConfig,
            2 => ConfigSource::DynamicBrokerConfig,
            3 => ConfigSource::DynamicDefaultBrokerConfig,
            4 => ConfigSource::StaticBrokerConfig,
            5 => ConfigSource::DefaultConfig,
            6 => ConfigSource::DynamicBrokerLoggerConfig,
            7 => ConfigSource::DynamicClientMetricsConfig,
            8 => ConfigSource::DynamicGroupConfig,
            _ => ConfigSource::Unknown,
        }
    }

    /// The name of this constant in Java's
    /// `org.apache.kafka.clients.admin.ConfigEntry$ConfigSource`. The Java enum does not override
    /// `toString()`, so `Enum.toString()` returns this name.
    fn name(&self) -> &'static str {
        match self {
            ConfigSource::DynamicTopicConfig => "DYNAMIC_TOPIC_CONFIG",
            ConfigSource::DynamicBrokerLoggerConfig => "DYNAMIC_BROKER_LOGGER_CONFIG",
            ConfigSource::DynamicBrokerConfig => "DYNAMIC_BROKER_CONFIG",
            ConfigSource::DynamicDefaultBrokerConfig => "DYNAMIC_DEFAULT_BROKER_CONFIG",
            ConfigSource::DynamicClientMetricsConfig => "DYNAMIC_CLIENT_METRICS_CONFIG",
            ConfigSource::DynamicGroupConfig => "DYNAMIC_GROUP_CONFIG",
            ConfigSource::StaticBrokerConfig => "STATIC_BROKER_CONFIG",
            ConfigSource::DefaultConfig => "DEFAULT_CONFIG",
            ConfigSource::Unknown => "UNKNOWN",
        }
    }
}

/// Renders the Java constant name (`DYNAMIC_BROKER_CONFIG`, `UNKNOWN`, ...), as
/// Java's `Enum.toString()` does.
impl std::fmt::Display for ConfigSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Class representing a configuration synonym of a [`ConfigEntry`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[doc(alias = "org.apache.kafka.clients.admin.ConfigEntry$ConfigSynonym")]
pub struct ConfigSynonym {
    name: String,
    value: Option<String>,
    source: ConfigSource,
}

impl ConfigSynonym {
    /// Create a configuration synonym with the provided values.
    ///
    /// Package-private in Java; the caller is `describeConfigs` response parsing.
    #[doc(alias = "org.apache.kafka.clients.admin.ConfigEntry$ConfigSynonym#ConfigSynonym")]
    pub(crate) fn new(name: String, value: Option<String>, source: ConfigSource) -> Self {
        Self { name, value, source }
    }

    /// Returns the name of this configuration.
    #[doc(alias = "org.apache.kafka.clients.admin.ConfigEntry$ConfigSynonym#name")]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the value of this configuration, which may be `None` if the
    /// configuration is sensitive.
    #[doc(alias = "org.apache.kafka.clients.admin.ConfigEntry$ConfigSynonym#value")]
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    /// Returns the source of this configuration.
    #[doc(alias = "org.apache.kafka.clients.admin.ConfigEntry$ConfigSynonym#source")]
    pub fn source(&self) -> ConfigSource {
        self.source
    }
}

/// Java's `ConfigSynonym.toString()`: the
/// source renders as its Java constant name and a missing value as `null`.
impl std::fmt::Display for ConfigSynonym {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ConfigSynonym(name={}, value={}, source={})",
            self.name,
            self.value.as_deref().unwrap_or("null"),
            self.source
        )
    }
}

/// What Java's `ConfigEntry.toString()` prints in place of a sensitive value.
const REDACTED: &str = "Redacted";

/// A class representing a configuration entry containing name, value and
/// additional metadata.
///
/// # Translation notes
///
/// `Debug` renders what `Display` renders.
#[derive(Clone, PartialEq, Eq, Hash)]
#[doc(alias = "org.apache.kafka.clients.admin.ConfigEntry")]
pub struct ConfigEntry {
    name: String,
    value: Option<String>,
    source: ConfigSource,
    is_sensitive: bool,
    is_read_only: bool,
    synonyms: Vec<ConfigSynonym>,
    config_type: ConfigType,
    documentation: Option<String>,
}

/// The parameters of Java's widest `ConfigEntry` constructor
/// (`ConfigEntry(String, String, ConfigSource, boolean, boolean, List, ConfigType, String)`),
/// taken by [`ConfigEntry::with_options`].
///
/// It has no `Default`: `name` and `value` are mandatory. Build it with
/// [`ConfigEntryOptionsBuilder`], whose [`build`](ConfigEntryOptionsBuilder::build)
/// returns an error if either was not set.
///
/// `Debug` renders a sensitive `value` as `"Redacted"`, as the [`ConfigEntry`]
/// built from it renders it.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq)]
// the options of ConfigEntry's constructor overloads
#[doc(alias = "rust-only")]
pub struct ConfigEntryOptions {
    /// The non-null config name. Java's `name`.
    pub(crate) name: String,
    /// The config value or `None`. Java's `value`.
    pub(crate) value: Option<String>,
    /// The source of this config entry. Java's `source`; starts as
    /// [`ConfigSource::Unknown`], as in the two-argument constructor.
    pub(crate) source: ConfigSource,
    /// Whether the config value is sensitive; the broker never returns the
    /// value if it is sensitive. Java's `isSensitive`; starts as `false`.
    pub(crate) is_sensitive: bool,
    /// Whether the config is read-only and cannot be updated. Java's
    /// `isReadOnly`; starts as `false`.
    pub(crate) is_read_only: bool,
    /// Synonym configs in order of precedence. Java's `synonyms`; starts empty.
    pub(crate) synonyms: Vec<ConfigSynonym>,
    /// The config data type. Java's `type`; starts as [`ConfigType::Unknown`].
    pub(crate) config_type: ConfigType,
    /// The config documentation. Java's `documentation`; starts as `None`.
    pub(crate) documentation: Option<String>,
}

/// Renders every field as the derive it replaces did, except that the value of
/// a sensitive entry renders as `"Redacted"`, the word Java's
/// `ConfigEntry.toString()` prints for it.
///
/// This struct has no Java counterpart, so it has no Java rendering of its
/// own. It carries the values of the [`ConfigEntry`] it builds, which never
/// renders a sensitive value, so a derived `Debug` here would print what the
/// entry itself hides.
impl std::fmt::Debug for ConfigEntryOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Destructured exhaustively, so a field added to the struct must be
        // considered here.
        let Self {
            name,
            value,
            source,
            is_sensitive,
            is_read_only,
            synonyms,
            config_type,
            documentation,
        } = self;
        let mut debug = f.debug_struct("ConfigEntryOptions");
        debug.field("name", name);
        if *is_sensitive {
            debug.field("value", &REDACTED);
        } else {
            debug.field("value", value);
        }
        debug
            .field("source", source)
            .field("is_sensitive", is_sensitive)
            .field("is_read_only", is_read_only)
            .field("synonyms", synonyms)
            .field("config_type", config_type)
            .field("documentation", documentation)
            .finish()
    }
}

/// Fluent builder for [`ConfigEntryOptions`].
///
/// [`Self::new`] takes no parameters, every parameter has a fluent setter, and
/// [`Self::build`] returns an error if a mandatory one (`name`, `value`) was not
/// set.
#[derive(Debug)]
// builds ConfigEntryOptions
#[doc(alias = "rust-only")]
pub struct ConfigEntryOptionsBuilder {
    name: Option<String>,
    value: Option<Option<String>>,
    source: ConfigSource,
    is_sensitive: bool,
    is_read_only: bool,
    synonyms: Vec<ConfigSynonym>,
    config_type: ConfigType,
    documentation: Option<String>,
}

impl Default for ConfigEntryOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigEntryOptionsBuilder {
    /// Creates a builder with every mandatory parameter unset and every other
    /// parameter at the value Java passes on the caller's behalf.
    pub fn new() -> Self {
        Self {
            name: None,
            value: None,
            source: ConfigSource::Unknown,
            is_sensitive: false,
            is_read_only: false,
            synonyms: Vec::new(),
            config_type: ConfigType::Unknown,
            documentation: None,
        }
    }

    /// Sets `name`, a mandatory parameter: [`Self::build`] returns an error if it
    /// was not set.
    ///
    /// # Arguments
    ///
    /// - `name` - the non-None config name
    ///
    /// # Returns
    ///
    /// This builder.
    pub fn set_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }
    /// Sets `value`, a mandatory parameter: [`Self::build`] returns an error if it
    /// was not set.
    ///
    /// # Arguments
    ///
    /// - `value` - the config value or `None`
    ///
    /// # Returns
    ///
    /// This builder.
    pub fn set_value(mut self, value: Option<String>) -> Self {
        self.value = Some(value);
        self
    }
    /// Sets `source`.
    ///
    /// # Arguments
    ///
    /// - `source` - the source of this config entry
    ///
    /// # Returns
    ///
    /// This builder.
    pub fn set_source(mut self, source: ConfigSource) -> Self {
        self.source = source;
        self
    }
    /// Sets `is_sensitive`.
    ///
    /// # Arguments
    ///
    /// - `is_sensitive` - whether the config value is sensitive, the broker never returns the value
    ///   if
    ///   it is sensitive
    ///
    /// # Returns
    ///
    /// This builder.
    pub fn set_is_sensitive(mut self, is_sensitive: bool) -> Self {
        self.is_sensitive = is_sensitive;
        self
    }
    /// Sets `is_read_only`.
    ///
    /// # Arguments
    ///
    /// - `is_read_only` - whether the config is read-only and cannot be updated
    ///
    /// # Returns
    ///
    /// This builder.
    pub fn set_is_read_only(mut self, is_read_only: bool) -> Self {
        self.is_read_only = is_read_only;
        self
    }
    /// Sets `synonyms`.
    ///
    /// # Arguments
    ///
    /// - `synonyms` - synonym configs in order of precedence
    ///
    /// # Returns
    ///
    /// This builder.
    pub fn set_synonyms(mut self, synonyms: Vec<ConfigSynonym>) -> Self {
        self.synonyms = synonyms;
        self
    }
    /// Sets `config_type`.
    ///
    /// # Arguments
    ///
    /// - `config_type` - the config data type
    ///
    /// # Returns
    ///
    /// This builder.
    pub fn set_config_type(mut self, config_type: ConfigType) -> Self {
        self.config_type = config_type;
        self
    }
    /// Sets `documentation`.
    ///
    /// # Arguments
    ///
    /// - `documentation` - the config documentation
    ///
    /// # Returns
    ///
    /// This builder.
    pub fn set_documentation(mut self, documentation: Option<String>) -> Self {
        self.documentation = documentation;
        self
    }

    /// Returns the built options.
    ///
    /// Only the presence of the mandatory parameters is checked here; semantic
    /// validation belongs to [`ConfigEntry::with_options`].
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` if `name` or `value` was not set,
    ///   naming the first one missing.
    pub fn build(self) -> Result<ConfigEntryOptions, Error> {
        Ok(ConfigEntryOptions {
            name: self.name.ok_or_else(|| Self::missing("name"))?,
            value: self.value.ok_or_else(|| Self::missing("value"))?,
            source: self.source,
            is_sensitive: self.is_sensitive,
            is_read_only: self.is_read_only,
            synonyms: self.synonyms,
            config_type: self.config_type,
            documentation: self.documentation,
        })
    }

    /// Builds the [`Error::LocalIllegalArgument`] naming a mandatory parameter
    /// [`Self::build`] found unset.
    fn missing(parameter: &str) -> Error {
        Error::local_illegal_argument(format!(
            "ConfigEntryOptionsBuilder::build: mandatory parameter `{parameter}` was not set"
        ))
    }
}

impl ConfigEntry {
    /// Create a configuration entry with the provided values.
    ///
    /// # Arguments
    ///
    /// - `name` - the non-None config name
    /// - `value` - the config value or `None`
    #[doc(alias = "org.apache.kafka.clients.admin.ConfigEntry#ConfigEntry")]
    pub fn new(name: String, value: Option<String>) -> Self {
        Self::with_options(
            ConfigEntryOptionsBuilder::new()
                .set_name(name)
                .set_value(value)
                .build()
                .expect("ConfigEntryOptionsBuilder::build: every mandatory parameter is set above"),
        )
    }

    /// Create a configuration with the provided values.
    ///
    /// # Arguments
    ///
    /// - `options` - the parameters of Java's widest constructor: `name`, `value`,
    ///   `source`, `is_sensitive`, `is_read_only`, `synonyms`, `config_type` and
    ///   `documentation`
    #[doc(alias = "org.apache.kafka.clients.admin.ConfigEntry#ConfigEntry")]
    pub fn with_options(options: ConfigEntryOptions) -> Self {
        let ConfigEntryOptions {
            name,
            value,
            source,
            is_sensitive,
            is_read_only,
            synonyms,
            config_type,
            documentation,
        } = options;
        Self {
            name,
            value,
            source,
            is_sensitive,
            is_read_only,
            synonyms,
            config_type,
            documentation,
        }
    }

    /// Returns the config name.
    #[doc(alias = "org.apache.kafka.clients.admin.ConfigEntry#name")]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the value or `None`. `None` is returned if the config is unset or
    /// if `is_sensitive` is true.
    #[doc(alias = "org.apache.kafka.clients.admin.ConfigEntry#value")]
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    /// Returns the source of this configuration entry.
    #[doc(alias = "org.apache.kafka.clients.admin.ConfigEntry#source")]
    pub fn source(&self) -> ConfigSource {
        self.source
    }

    /// Returns whether the config value is the default or if it's been
    /// explicitly set.
    #[doc(alias = "org.apache.kafka.clients.admin.ConfigEntry#isDefault")]
    pub fn is_default(&self) -> bool {
        self.source == ConfigSource::DefaultConfig
    }

    /// Returns whether the config value is sensitive. The value is always set to
    /// `None` by the broker if the config value is sensitive.
    #[doc(alias = "org.apache.kafka.clients.admin.ConfigEntry#isSensitive")]
    pub fn is_sensitive(&self) -> bool {
        self.is_sensitive
    }

    /// Returns whether the config is read-only and cannot be updated.
    #[doc(alias = "org.apache.kafka.clients.admin.ConfigEntry#isReadOnly")]
    pub fn is_read_only(&self) -> bool {
        self.is_read_only
    }

    /// Returns all config values that may be used as the value of this config
    /// along with their source, in the order of precedence. The list starts
    /// with the value returned in this `ConfigEntry`. The list is empty if
    /// synonyms were not requested.
    #[doc(alias = "org.apache.kafka.clients.admin.ConfigEntry#synonyms")]
    pub fn synonyms(&self) -> &[ConfigSynonym] {
        &self.synonyms
    }

    /// Returns the config data type.
    #[doc(alias = "org.apache.kafka.clients.admin.ConfigEntry#type")]
    pub fn r#type(&self) -> ConfigType {
        self.config_type
    }

    /// Returns the config documentation.
    #[doc(alias = "org.apache.kafka.clients.admin.ConfigEntry#documentation")]
    pub fn documentation(&self) -> Option<&str> {
        self.documentation.as_deref()
    }
}

impl std::fmt::Display for ConfigEntry {
    /// Redacts sensitive value.
    ///
    /// Java's `toString()`: the source and
    /// type render as their Java constant names, and the synonyms as Java's
    /// `AbstractCollection.toString()` renders the list (`[a, b]`, `[]` when
    /// empty), each synonym through its own `Display`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = if self.is_sensitive {
            REDACTED
        } else {
            self.value.as_deref().unwrap_or("null")
        };
        write!(
            f,
            "ConfigEntry(name={}, value={}, source={}, isSensitive={}, isReadOnly={}, synonyms=[",
            self.name, value, self.source, self.is_sensitive, self.is_read_only
        )?;
        for (i, synonym) in self.synonyms.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{synonym}")?;
        }
        write!(
            f,
            "], type={}, documentation={})",
            self.config_type,
            self.documentation.as_deref().unwrap_or("null")
        )
    }
}

/// Renders exactly what the redacting [`Display`](std::fmt::Display) renders.
///
/// Java has a single `toString()`, which prints
/// `Redacted` for the value of a sensitive entry; a derived `Debug` would be a
/// second, unredacted rendering. [`Config`](crate::admin::Config) and
/// [`AlterConfigOp`](crate::admin::AlterConfigOp) embed entries, so their
/// derived `Debug` renders through this one.
impl std::fmt::Debug for ConfigEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_constructor_defaults() {
        let entry = ConfigEntry::new("k".to_string(), Some("v".to_string()));
        assert_eq!(entry.name(), "k");
        assert_eq!(entry.value(), Some("v"));
        assert_eq!(entry.source(), ConfigSource::Unknown);
        assert!(!entry.is_sensitive());
        assert!(!entry.is_read_only());
        assert!(entry.synonyms().is_empty());
        assert_eq!(entry.r#type(), ConfigType::Unknown);
        assert_eq!(entry.documentation(), None);
    }

    #[test]
    fn is_default_only_for_default_config_source() {
        let default = ConfigEntry::with_options(
            ConfigEntryOptionsBuilder::new()
                .set_name("k".to_string())
                .set_value(None)
                .set_source(ConfigSource::DefaultConfig)
                .set_config_type(ConfigType::String)
                .build()
                .unwrap(),
        );
        assert!(default.is_default());
        assert!(!ConfigEntry::new("k".to_string(), None).is_default());
    }

    #[test]
    fn display_redacts_sensitive_value() {
        let entry = ConfigEntry::with_options(
            ConfigEntryOptionsBuilder::new()
                .set_name("password".to_string())
                .set_value(Some("secret".to_string()))
                .set_is_sensitive(true)
                .set_config_type(ConfigType::Password)
                .build()
                .unwrap(),
        );
        let s = entry.to_string();
        assert!(s.contains("value=Redacted"), "{s}");
        assert!(!s.contains("secret"), "{s}");
    }

    /// New test, no Java original (Java has no `ConfigEntryTest`): every
    /// `ConfigType` renders its Java constant name, as Java's `Enum.toString()`
    /// does (`org.apache.kafka.clients.admin.ConfigEntry$ConfigType`).
    #[test]
    fn config_type_display_matches_java_constant_names() {
        for (config_type, java) in [
            (ConfigType::Unknown, "UNKNOWN"),
            (ConfigType::Boolean, "BOOLEAN"),
            (ConfigType::String, "STRING"),
            (ConfigType::Int, "INT"),
            (ConfigType::Short, "SHORT"),
            (ConfigType::Long, "LONG"),
            (ConfigType::Double, "DOUBLE"),
            (ConfigType::List, "LIST"),
            (ConfigType::Class, "CLASS"),
            (ConfigType::Password, "PASSWORD"),
        ] {
            assert_eq!(config_type.to_string(), java);
        }
    }

    /// New test, no Java original: every `ConfigSource` renders its Java
    /// constant name, as Java's `Enum.toString()` does
    /// (`org.apache.kafka.clients.admin.ConfigEntry$ConfigSource`).
    #[test]
    fn config_source_display_matches_java_constant_names() {
        for (source, java) in [
            (ConfigSource::DynamicTopicConfig, "DYNAMIC_TOPIC_CONFIG"),
            (ConfigSource::DynamicBrokerLoggerConfig, "DYNAMIC_BROKER_LOGGER_CONFIG"),
            (ConfigSource::DynamicBrokerConfig, "DYNAMIC_BROKER_CONFIG"),
            (ConfigSource::DynamicDefaultBrokerConfig, "DYNAMIC_DEFAULT_BROKER_CONFIG"),
            (ConfigSource::DynamicClientMetricsConfig, "DYNAMIC_CLIENT_METRICS_CONFIG"),
            (ConfigSource::DynamicGroupConfig, "DYNAMIC_GROUP_CONFIG"),
            (ConfigSource::StaticBrokerConfig, "STATIC_BROKER_CONFIG"),
            (ConfigSource::DefaultConfig, "DEFAULT_CONFIG"),
            (ConfigSource::Unknown, "UNKNOWN"),
        ] {
            assert_eq!(source.to_string(), java);
        }
    }

    /// New test, no Java original: `ConfigSynonym`'s `Display` is the string
    /// Java's `ConfigSynonym.toString()` builds,
    /// with a missing value rendered as `null`.
    #[test]
    fn config_synonym_display_matches_java_to_string() {
        let synonym = ConfigSynonym::new(
            "log.retention.ms".to_string(),
            Some("604800000".to_string()),
            ConfigSource::DynamicBrokerConfig,
        );
        assert_eq!(
            synonym.to_string(),
            "ConfigSynonym(name=log.retention.ms, value=604800000, source=DYNAMIC_BROKER_CONFIG)"
        );
        let without_value = ConfigSynonym::new("log.retention.hours".to_string(), None, ConfigSource::DefaultConfig);
        assert_eq!(
            without_value.to_string(),
            "ConfigSynonym(name=log.retention.hours, value=null, source=DEFAULT_CONFIG)"
        );
    }

    /// New test, no Java original: `Display` is the string Java's
    /// `ConfigEntry.toString()` builds for an entry
    /// with two synonyms and a non-default source, type and documentation. The
    /// synonyms render as Java's list of `ConfigSynonym.toString()`s.
    #[test]
    fn display_matches_java_to_string() {
        let entry = ConfigEntry::with_options(
            ConfigEntryOptionsBuilder::new()
                .set_name("log.retention.ms".to_string())
                .set_value(Some("604800000".to_string()))
                .set_source(ConfigSource::DynamicBrokerConfig)
                .set_is_read_only(true)
                .set_synonyms(vec![
                    ConfigSynonym::new(
                        "log.retention.ms".to_string(),
                        Some("604800000".to_string()),
                        ConfigSource::DynamicBrokerConfig,
                    ),
                    ConfigSynonym::new(
                        "log.retention.hours".to_string(),
                        Some("168".to_string()),
                        ConfigSource::DefaultConfig,
                    ),
                ])
                .set_config_type(ConfigType::Long)
                .set_documentation(Some("The retention time.".to_string()))
                .build()
                .unwrap(),
        );
        assert_eq!(
            entry.to_string(),
            "ConfigEntry(name=log.retention.ms, value=604800000, source=DYNAMIC_BROKER_CONFIG, \
             isSensitive=false, isReadOnly=true, \
             synonyms=[ConfigSynonym(name=log.retention.ms, value=604800000, \
             source=DYNAMIC_BROKER_CONFIG), ConfigSynonym(name=log.retention.hours, value=168, \
             source=DEFAULT_CONFIG)], type=LONG, documentation=The retention time.)"
        );
    }

    /// New test, no Java original: the entry Java's two-argument constructor
    /// builds renders an empty synonym list as `[]`,
    /// the `UNKNOWN` source and type, and `null` for a missing value and
    /// documentation; a sensitive entry renders `Redacted` with the same shape.
    #[test]
    fn display_matches_java_to_string_for_defaults_and_sensitive_entry() {
        assert_eq!(
            ConfigEntry::new("cleanup.policy".to_string(), None).to_string(),
            "ConfigEntry(name=cleanup.policy, value=null, source=UNKNOWN, isSensitive=false, \
             isReadOnly=false, synonyms=[], type=UNKNOWN, documentation=null)"
        );
        assert_eq!(
            sensitive_entry().to_string(),
            "ConfigEntry(name=ssl.keystore.password, value=Redacted, source=DYNAMIC_BROKER_CONFIG, \
             isSensitive=true, isReadOnly=false, synonyms=[], type=PASSWORD, documentation=null)"
        );
    }

    /// A distinctive secret for the `Debug` tests.
    const SECRET: &str = "ssl-keystore-S3cr3t-5b72";

    /// A sensitive entry holding [`SECRET`].
    fn sensitive_entry() -> ConfigEntry {
        ConfigEntry::with_options(sensitive_options())
    }

    /// Options for a sensitive entry holding [`SECRET`].
    fn sensitive_options() -> ConfigEntryOptions {
        ConfigEntryOptionsBuilder::new()
            .set_name("ssl.keystore.password".to_string())
            .set_value(Some(SECRET.to_string()))
            .set_source(ConfigSource::DynamicBrokerConfig)
            .set_is_sensitive(true)
            .set_config_type(ConfigType::Password)
            .build()
            .unwrap()
    }

    /// New test, no Java original: `Debug` renders exactly what `Display` does
    /// (Java has a single `toString()`): `Redacted` for a sensitive value,
    /// while a non-sensitive value still prints.
    #[test]
    fn debug_redacts_sensitive_value() {
        let sensitive = sensitive_entry();
        assert_eq!(sensitive.value(), Some(SECRET));
        for rendered in [format!("{sensitive:?}"), format!("{sensitive:#?}")] {
            assert_eq!(rendered, sensitive.to_string());
            assert!(rendered.contains("name=ssl.keystore.password"), "{rendered}");
            assert!(rendered.contains("value=Redacted"), "{rendered}");
            assert!(!rendered.contains(SECRET), "secret leaked: {rendered}");
        }

        let plain = ConfigEntry::new("retention.ms".to_string(), Some("604800000".to_string()));
        for rendered in [format!("{plain:?}"), format!("{plain:#?}")] {
            assert_eq!(rendered, plain.to_string());
            assert!(rendered.contains("value=604800000"), "{rendered}");
        }
    }

    /// New test, no Java original (Java has no such struct): `Debug` renders a
    /// sensitive value as `"Redacted"`, as the entry built from the options
    /// renders it, and every other field as the derive did.
    #[test]
    fn options_debug_redacts_sensitive_value() {
        let options = sensitive_options();
        assert_eq!(options.value.as_deref(), Some(SECRET));
        let rendered = format!("{options:?}");
        assert_eq!(
            rendered,
            "ConfigEntryOptions { name: \"ssl.keystore.password\", value: \"Redacted\", \
             source: DynamicBrokerConfig, is_sensitive: true, is_read_only: false, synonyms: [], \
             config_type: Password, documentation: None }"
        );
        let pretty = format!("{options:#?}");
        assert!(pretty.contains("value: \"Redacted\""), "{pretty}");
        for rendered in [rendered, pretty] {
            assert!(!rendered.contains(SECRET), "secret leaked: {rendered}");
        }
    }

    /// New test, no Java original: the options of a non-sensitive entry render
    /// the value, as the derive did.
    #[test]
    fn options_debug_renders_non_sensitive_value() {
        let options = ConfigEntryOptionsBuilder::new()
            .set_name("retention.ms".to_string())
            .set_value(Some("604800000".to_string()))
            .build()
            .unwrap();
        let rendered = format!("{options:?}");
        assert!(rendered.contains("value: Some(\"604800000\")"), "{rendered}");
        assert!(rendered.contains("is_sensitive: false"), "{rendered}");
    }

    #[test]
    fn equality() {
        let a = ConfigEntry::new("k".to_string(), Some("v".to_string()));
        let b = ConfigEntry::new("k".to_string(), Some("v".to_string()));
        let c = ConfigEntry::new("k".to_string(), Some("other".to_string()));
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    /// The mandatory parameters are validated in
    /// [`ConfigEntryOptionsBuilder::build`], not named in the constructor, so a
    /// builder left untouched panics naming the first one it finds unset.
    #[test]
    fn config_entry_options_builder_build_errors_when_no_mandatory_parameter_is_set() {
        let Err(error) = ConfigEntryOptionsBuilder::new().build() else {
            panic!("build must reject the unset mandatory parameter");
        };
        assert!(matches!(error, Error::LocalIllegalArgument(_)), "{error:?}");
        assert_eq!(
            error.message(),
            "ConfigEntryOptionsBuilder::build: mandatory parameter `name` was not set"
        );
    }

    /// Validation covers every mandatory parameter, not just the first: setting
    /// all but one still panics, naming the one left unset.
    #[test]
    fn config_entry_options_builder_build_errors_when_only_value_is_unset() {
        let Err(error) = ConfigEntryOptionsBuilder::new()
            .set_name("compression.type".to_string())
            .build()
        else {
            panic!("build must reject the unset mandatory parameter");
        };
        assert!(matches!(error, Error::LocalIllegalArgument(_)), "{error:?}");
        assert_eq!(
            error.message(),
            "ConfigEntryOptionsBuilder::build: mandatory parameter `value` was not set"
        );
    }
}
