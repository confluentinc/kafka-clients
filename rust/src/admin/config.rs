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

//! A configuration object containing the configuration entries for a resource.

use std::collections::HashMap;

use crate::admin::ConfigEntry;

/// A configuration object containing the configuration entries for a resource.
///
/// # Translation notes
///
/// `Debug` redacts sensitive values.
#[derive(Clone, Debug, PartialEq, Eq)]
#[doc(alias = "org.apache.kafka.clients.admin.Config")]
pub struct Config {
    entries: HashMap<String, ConfigEntry>,
}

impl Config {
    /// Create a configuration instance with the provided entries.
    ///
    /// # Arguments
    ///
    /// - `entries` - the configuration entries.
    #[doc(alias = "org.apache.kafka.clients.admin.Config#Config")]
    pub fn new(entries: impl IntoIterator<Item = ConfigEntry>) -> Self {
        let mut map = HashMap::new();
        for entry in entries {
            map.insert(entry.name().to_string(), entry);
        }
        Self { entries: map }
    }

    /// Returns the configuration entries for a resource.
    #[doc(alias = "org.apache.kafka.clients.admin.Config#entries")]
    pub fn entries(&self) -> impl Iterator<Item = &ConfigEntry> {
        self.entries.values()
    }

    /// Returns the configuration entry with the provided name or `None` if there
    /// isn't one.
    ///
    /// # Arguments
    ///
    /// - `name` - the name of the configuration entry.
    #[doc(alias = "org.apache.kafka.clients.admin.Config#get")]
    pub fn get(&self, name: &str) -> Option<&ConfigEntry> {
        self.entries.get(name)
    }
}

impl std::hash::Hash for Config {
    // Java's `Config.hashCode()` is `entries.hashCode()`, the order-independent
    // sum of a `HashMap`'s entry hashes.
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::common::utils::Utils::hash_unordered(&self.entries, state);
    }
}

impl std::fmt::Display for Config {
    /// Renders each entry through `ConfigEntry`'s redacting `Display`, as Java's
    /// `org.apache.kafka.clients.admin.Config#toString` does.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let entries: Vec<String> = self.entries.values().map(|e| e.to_string()).collect();
        write!(f, "Config(entries=[{}])", entries.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::ConfigEntryOptionsBuilder;
    use crate::admin::config_entry::ConfigType;

    #[test]
    fn new_indexes_entries_by_name() {
        let config = Config::new([
            ConfigEntry::new("a".to_string(), Some("1".to_string())),
            ConfigEntry::new("b".to_string(), Some("2".to_string())),
        ]);
        assert_eq!(config.get("a").unwrap().value(), Some("1"));
        assert_eq!(config.get("b").unwrap().value(), Some("2"));
        assert!(config.get("missing").is_none());
        assert_eq!(config.entries().count(), 2);
    }

    #[test]
    fn equality_is_order_independent() {
        let a = Config::new([
            ConfigEntry::new("a".to_string(), Some("1".to_string())),
            ConfigEntry::new("b".to_string(), Some("2".to_string())),
        ]);
        let b = Config::new([
            ConfigEntry::new("b".to_string(), Some("2".to_string())),
            ConfigEntry::new("a".to_string(), Some("1".to_string())),
        ]);
        assert_eq!(a, b);
    }

    /// A distinctive secret for the redaction tests.
    const SECRET: &str = "ssl-truststore-S3cr3t-91c4";

    /// A config holding a sensitive entry with [`SECRET`] and a non-sensitive one.
    fn config_with_secret() -> Config {
        Config::new([
            ConfigEntry::with_options(
                ConfigEntryOptionsBuilder::new()
                    .set_name("ssl.truststore.password".to_string())
                    .set_value(Some(SECRET.to_string()))
                    .set_is_sensitive(true)
                    .set_config_type(Some(ConfigType::Password))
                    .build()
                    .unwrap(),
            ),
            ConfigEntry::new("retention.ms".to_string(), Some("604800000".to_string())),
        ])
    }

    /// Translated from `ConfigTest.shouldImplementToStringProperly`: the
    /// rendering contains each entry's own rendering. Java's fixture holds two plain entries
    /// (`a=b`, `c=d`); this
    /// one makes the first entry sensitive, so the test also shows that
    /// `Display` renders it through the entry's redacting `Display`, as Java's
    /// `toString()` does, and the secret does not appear.
    #[test]
    fn display_redacts_sensitive_values() {
        let config = config_with_secret();
        let sensitive = config.get("ssl.truststore.password").unwrap();
        assert_eq!(sensitive.value(), Some(SECRET));
        let rendered = config.to_string();
        // Java's two assertions: the rendering contains each entry's `toString()`.
        assert!(rendered.contains(&sensitive.to_string()), "{rendered}");
        assert!(
            rendered.contains(&config.get("retention.ms").unwrap().to_string()),
            "{rendered}"
        );
        assert!(rendered.starts_with("Config(entries=["), "{rendered}");
        assert!(rendered.contains("name=ssl.truststore.password, value=Redacted"), "{rendered}");
        assert!(rendered.contains("name=retention.ms, value=604800000"), "{rendered}");
        assert!(!rendered.contains(SECRET), "secret leaked: {rendered}");
    }

    /// The `Debug` counterpart of `ConfigTest.shouldImplementToStringProperly`:
    /// the derived `Debug` renders each entry through
    /// its `Debug`, which is the entry's redacting `Display`, so it contains
    /// each entry's `to_string()`, while a non-sensitive value still prints.
    #[test]
    fn debug_redacts_sensitive_values() {
        let config = config_with_secret();
        for rendered in [format!("{config:?}"), format!("{config:#?}")] {
            for entry in config.entries() {
                assert!(rendered.contains(&entry.to_string()), "{rendered}");
            }
            assert!(rendered.starts_with("Config"), "{rendered}");
            assert!(rendered.contains("name=ssl.truststore.password, value=Redacted"), "{rendered}");
            assert!(rendered.contains("name=retention.ms, value=604800000"), "{rendered}");
            assert!(!rendered.contains(SECRET), "secret leaked: {rendered}");
        }
    }
}
