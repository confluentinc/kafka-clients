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
//!
//! Corresponds to `org.apache.kafka.clients.admin.ConfigEntry`.

use crate::common::Error;

/// Data type of configuration entry.
///
/// Corresponds to `ConfigEntry.ConfigType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
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
    /// panics on a recoverable path (CLAUDE.md §10).
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
}

/// Source of configuration entries.
///
/// Corresponds to `ConfigEntry.ConfigSource`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
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
    /// response parsing never panics on a recoverable path (CLAUDE.md §10).
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
}

/// A configuration synonym of a [`ConfigEntry`].
///
/// Corresponds to `ConfigEntry.ConfigSynonym`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigSynonym {
    name: String,
    value: Option<String>,
    source: ConfigSource,
}

impl ConfigSynonym {
    /// Create a configuration synonym with the provided values.
    ///
    /// Package-private in Java; the first caller is `describeConfigs` response
    /// parsing (Tier 1 Phase 3), so it is not yet referenced in Phase 1.
    #[allow(dead_code)]
    pub(crate) fn new(name: String, value: Option<String>, source: ConfigSource) -> Self {
        Self { name, value, source }
    }

    /// Returns the name of this configuration (this may be different from the
    /// name of the associated [`ConfigEntry`]).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the value of this configuration, which may be `None` if the
    /// configuration is sensitive.
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    /// Returns the source of this configuration.
    pub fn source(&self) -> ConfigSource {
        self.source
    }
}

impl std::fmt::Display for ConfigSynonym {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ConfigSynonym(name={}, value={}, source={:?})",
            self.name,
            self.value.as_deref().unwrap_or("null"),
            self.source
        )
    }
}

/// A configuration entry containing name, value and additional metadata.
///
/// Corresponds to `org.apache.kafka.clients.admin.ConfigEntry`.
#[derive(Clone, Debug, PartialEq, Eq)]
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
/// (`ConfigEntry(String, String, ConfigSource, boolean, boolean, List, ConfigType, String)`,
/// `ConfigEntry.java:59`).
///
/// This struct has **no Java counterpart** (DoD #7). It exists solely to satisfy
/// CLAUDE.md §2's cap on derived overload names: that constructor differs from
/// the group's intersection `{name, value}` by six parameters, so the cap fires
/// and this struct becomes the method's *only* parameter, carrying every Java
/// parameter including the intersection's own.
///
/// It deliberately has **no** `Default`. `name` and `value` are what even Java's
/// narrow constructor (`:44`) takes from its caller, so neither has a
/// Java-derived default, and a synthesised empty name would produce an entry
/// naming no config at all. Construct it with [`ConfigEntryOptionsBuilder::new`]
/// and set them: [`ConfigEntryOptionsBuilder::build`] returns an error if any of `name`, `value` was not set.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigEntryOptions {
    /// The non-null config name. Java's `name`.
    pub name: String,
    /// The config value or `None`. Java's `value`.
    pub value: Option<String>,
    /// The source of this config entry. Java's `source`; starts as
    /// [`ConfigSource::Unknown`], as in `:44`.
    pub source: ConfigSource,
    /// Whether the config value is sensitive; the broker never returns the
    /// value if it is sensitive. Java's `isSensitive`; starts as `false`, as in
    /// `:44`.
    pub is_sensitive: bool,
    /// Whether the config is read-only and cannot be updated. Java's
    /// `isReadOnly`; starts as `false`, as in `:44`.
    pub is_read_only: bool,
    /// Synonym configs in order of precedence. Java's `synonyms`; starts empty,
    /// as in `:44` (`Collections.emptyList()`).
    pub synonyms: Vec<ConfigSynonym>,
    /// The config data type. Java's `type`; starts as [`ConfigType::Unknown`],
    /// as in `:44`.
    pub config_type: ConfigType,
    /// The config documentation. Java's `documentation`; starts as `None`, as in
    /// `:44`.
    pub documentation: Option<String>,
}

/// Fluent builder for [`ConfigEntryOptions`].
///
/// Per CLAUDE.md §2 [`Self::new`] takes no parameters, every parameter has a
/// fluent setter, and [`Self::build`] validates the mandatory ones — returning
/// [`Error::LocalIllegalArgument`] if they were not set. Like [`ConfigEntryOptions`] it has no Java counterpart and
/// exists solely to satisfy that naming rule (DoD #7).
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

    /// Sets [`ConfigEntryOptions::name`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }
    /// Sets [`ConfigEntryOptions::value`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_value(mut self, value: Option<String>) -> Self {
        self.value = Some(value);
        self
    }
    /// Sets [`ConfigEntryOptions::source`].
    pub fn set_source(mut self, source: ConfigSource) -> Self {
        self.source = source;
        self
    }
    /// Sets [`ConfigEntryOptions::is_sensitive`].
    pub fn set_is_sensitive(mut self, is_sensitive: bool) -> Self {
        self.is_sensitive = is_sensitive;
        self
    }
    /// Sets [`ConfigEntryOptions::is_read_only`].
    pub fn set_is_read_only(mut self, is_read_only: bool) -> Self {
        self.is_read_only = is_read_only;
        self
    }
    /// Sets [`ConfigEntryOptions::synonyms`].
    pub fn set_synonyms(mut self, synonyms: Vec<ConfigSynonym>) -> Self {
        self.synonyms = synonyms;
        self
    }
    /// Sets [`ConfigEntryOptions::config_type`].
    pub fn set_config_type(mut self, config_type: ConfigType) -> Self {
        self.config_type = config_type;
        self
    }
    /// Sets [`ConfigEntryOptions::documentation`].
    pub fn set_documentation(mut self, documentation: Option<String>) -> Self {
        self.documentation = documentation;
        self
    }

    /// Returns the built options.
    ///
    /// Per CLAUDE.md §2 the mandatory parameters are validated here rather than
    /// being named in the constructor, so a later Java version that makes one of
    /// them optional changes the set this accepts instead of adding a second
    /// constructor. Today there is one mandatory set: `name`, `value`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LocalIllegalArgument`] naming the first parameter of that
    /// set which was not given a setter call. Only presence is checked here;
    /// semantic validation belongs to the method the options are passed to
    /// (CLAUDE.md §2).
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
    /// Create a configuration entry with the provided name and value.
    ///
    /// Corresponds to Java's `ConfigEntry(String, String)`
    /// (`ConfigEntry.java:44`), whose parameters `{name, value}` are the
    /// intersection across both constructors — so it owns the plain name
    /// (CLAUDE.md §2).
    ///
    /// * `name` - the non-null config name
    /// * `value` - the config value or `None`
    pub fn new(name: String, value: Option<String>) -> Self {
        Self::new_options(
            ConfigEntryOptionsBuilder::new()
                .set_name(name)
                .set_value(value)
                .build()
                .expect("ConfigEntryOptionsBuilder::build: every mandatory parameter is set above"),
        )
    }

    /// Create a configuration entry with all values.
    ///
    /// Corresponds to Java's widest constructor (`ConfigEntry.java:59`). Its
    /// eight parameters exceed CLAUDE.md §2's three-parameter cap on derived
    /// overload names, so [`ConfigEntryOptions`] is this method's only
    /// parameter and carries all of them.
    ///
    /// * `options` - every parameter of Java's widest constructor
    pub fn new_options(options: ConfigEntryOptions) -> Self {
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

    /// Return the config name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the value or `None`. `None` is returned if the config is unset or
    /// if `is_sensitive` is true.
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    /// Return the source of this configuration entry.
    pub fn source(&self) -> ConfigSource {
        self.source
    }

    /// Return whether the config value is the default or if it's been
    /// explicitly set.
    pub fn is_default(&self) -> bool {
        self.source == ConfigSource::DefaultConfig
    }

    /// Return whether the config value is sensitive. The value is always set to
    /// `None` by the broker if the config value is sensitive.
    pub fn is_sensitive(&self) -> bool {
        self.is_sensitive
    }

    /// Return whether the config is read-only and cannot be updated.
    pub fn is_read_only(&self) -> bool {
        self.is_read_only
    }

    /// Returns all config values that may be used as the value of this config
    /// along with their source, in the order of precedence. The list starts
    /// with the value returned in this `ConfigEntry`. The list is empty if
    /// synonyms were not requested.
    pub fn synonyms(&self) -> &[ConfigSynonym] {
        &self.synonyms
    }

    /// Return the config data type.
    pub fn config_type(&self) -> ConfigType {
        self.config_type
    }

    /// Return the config documentation.
    pub fn documentation(&self) -> Option<&str> {
        self.documentation.as_deref()
    }
}

impl std::fmt::Display for ConfigEntry {
    /// Redacts sensitive value.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = if self.is_sensitive {
            "Redacted".to_string()
        } else {
            self.value.as_deref().unwrap_or("null").to_string()
        };
        write!(
            f,
            "ConfigEntry(name={}, value={}, source={:?}, isSensitive={}, isReadOnly={}, \
             synonyms={:?}, type={:?}, documentation={})",
            self.name,
            value,
            self.source,
            self.is_sensitive,
            self.is_read_only,
            self.synonyms,
            self.config_type,
            self.documentation.as_deref().unwrap_or("null")
        )
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
        assert_eq!(entry.config_type(), ConfigType::Unknown);
        assert_eq!(entry.documentation(), None);
    }

    #[test]
    fn is_default_only_for_default_config_source() {
        let default = ConfigEntry::new_options(
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
        let entry = ConfigEntry::new_options(
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

    #[test]
    fn equality() {
        let a = ConfigEntry::new("k".to_string(), Some("v".to_string()));
        let b = ConfigEntry::new("k".to_string(), Some("v".to_string()));
        let c = ConfigEntry::new("k".to_string(), Some("other".to_string()));
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    /// CLAUDE.md §2: the mandatory parameters are validated in
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
