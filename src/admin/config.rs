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
//!
//! Corresponds to `org.apache.kafka.clients.admin.Config`.

use std::collections::HashMap;

use crate::admin::ConfigEntry;

/// A configuration object containing the configuration entries for a resource.
///
/// Corresponds to `org.apache.kafka.clients.admin.Config`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    entries: HashMap<String, ConfigEntry>,
}

impl Config {
    /// Create a configuration instance with the provided entries.
    pub fn new(entries: impl IntoIterator<Item = ConfigEntry>) -> Self {
        let mut map = HashMap::new();
        for entry in entries {
            map.insert(entry.name().to_string(), entry);
        }
        Self { entries: map }
    }

    /// Configuration entries for a resource.
    pub fn entries(&self) -> impl Iterator<Item = &ConfigEntry> {
        self.entries.values()
    }

    /// Get the configuration entry with the provided name or `None` if there
    /// isn't one.
    pub fn get(&self, name: &str) -> Option<&ConfigEntry> {
        self.entries.get(name)
    }
}

impl std::fmt::Display for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let entries: Vec<String> = self.entries.values().map(|e| e.to_string()).collect();
        write!(f, "Config(entries=[{}])", entries.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
