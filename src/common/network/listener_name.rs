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

//! Listener name for Kafka network listeners.
//!
//! Translated from `org.apache.kafka.common.network.ListenerName`.

use std::fmt;

/// Prefix for listener-specific configuration keys.
const CONFIG_STATIC_PREFIX: &str = "listener.name";

/// A named listener, used to identify the security configuration for a connection.
///
/// In Java, `ListenerName` is used in conjunction with `SecurityProtocol` to configure
/// per-listener settings such as SSL keystores and SASL mechanisms.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ListenerName {
    value: String,
}

impl ListenerName {
    /// Creates a new `ListenerName` with the given value.
    ///
    /// # Panics
    ///
    /// Panics if `value` is empty, matching Java's `Objects.requireNonNull`.
    pub fn new(value: &str) -> Self {
        assert!(!value.is_empty(), "value should not be empty");
        Self { value: value.to_string() }
    }

    /// Create an instance with the provided value converted to uppercase.
    ///
    /// # Errors
    ///
    /// Returns an error if the value is blank (empty or whitespace only),
    /// matching Java's `ConfigException`.
    pub fn normalised(value: &str) -> Result<Self, String> {
        if value.trim().is_empty() {
            return Err("The provided listener name is null or empty string".to_string());
        }
        Ok(Self { value: value.to_uppercase() })
    }

    /// Returns the listener name value.
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Returns the configuration prefix for this listener.
    ///
    /// Format: `listener.name.<value>.`
    pub fn config_prefix(&self) -> String {
        format!("{CONFIG_STATIC_PREFIX}.{}.\"", self.value.to_lowercase())
    }

    /// Returns the SASL mechanism configuration prefix for this listener.
    ///
    /// Format: `listener.name.<value>.<mechanism>.`
    pub fn sasl_mechanism_config_prefix(&self, sasl_mechanism: &str) -> String {
        format!("{}{}", self.config_prefix(), Self::sasl_mechanism_prefix(sasl_mechanism))
    }

    /// Returns the SASL mechanism prefix.
    ///
    /// Format: `<mechanism>.`
    pub fn sasl_mechanism_prefix(sasl_mechanism: &str) -> String {
        format!("{}.", sasl_mechanism.to_lowercase())
    }
}

impl fmt::Display for ListenerName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ListenerName({})", self.value)
    }
}
