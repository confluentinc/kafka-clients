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

impl ConfigEntry {
    /// Create a configuration entry with the provided name and value.
    ///
    /// * `name` - the non-null config name
    /// * `value` - the config value or `None`
    pub fn new(name: String, value: Option<String>) -> Self {
        Self::with_metadata(
            name,
            value,
            ConfigSource::Unknown,
            false,
            false,
            Vec::new(),
            ConfigType::Unknown,
            None,
        )
    }

    /// Create a configuration entry with all values.
    ///
    /// * `name` - the non-null config name
    /// * `value` - the config value or `None`
    /// * `source` - the source of this config entry
    /// * `is_sensitive` - whether the config value is sensitive; the broker
    ///   never returns the value if it is sensitive
    /// * `is_read_only` - whether the config is read-only and cannot be updated
    /// * `synonyms` - synonym configs in order of precedence
    /// * `config_type` - the config data type
    /// * `documentation` - the config documentation
    #[allow(clippy::too_many_arguments)]
    pub fn with_metadata(
        name: String,
        value: Option<String>,
        source: ConfigSource,
        is_sensitive: bool,
        is_read_only: bool,
        synonyms: Vec<ConfigSynonym>,
        config_type: ConfigType,
        documentation: Option<String>,
    ) -> Self {
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
        let default = ConfigEntry::with_metadata(
            "k".to_string(),
            None,
            ConfigSource::DefaultConfig,
            false,
            false,
            Vec::new(),
            ConfigType::String,
            None,
        );
        assert!(default.is_default());
        assert!(!ConfigEntry::new("k".to_string(), None).is_default());
    }

    #[test]
    fn display_redacts_sensitive_value() {
        let entry = ConfigEntry::with_metadata(
            "password".to_string(),
            Some("secret".to_string()),
            ConfigSource::Unknown,
            true,
            false,
            Vec::new(),
            ConfigType::Password,
            None,
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
}
