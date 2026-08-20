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

//! Configuration for the `KafkaAdminClient`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.AdminClientConfig`.

use std::collections::HashMap;

use crate::common::Error;

/// Configuration for the admin client.
///
/// Corresponds to `org.apache.kafka.clients.admin.AdminClientConfig`. Unknown
/// keys are accepted silently, matching Java's `AbstractConfig` behavior.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdminClientConfig {
    bootstrap_servers: Vec<String>,
    client_id: String,
    request_timeout_ms: i32,
    default_api_timeout_ms: i32,
    retries: i32,
    retry_backoff_ms: i64,
    retry_backoff_max_ms: i64,
    reconnect_backoff_ms: i64,
    reconnect_backoff_max_ms: i64,
    connections_max_idle_ms: i64,
    metadata_max_age_ms: i64,
    socket_connection_setup_timeout_ms: i64,
}

impl AdminClientConfig {
    /// `bootstrap.servers`
    pub const BOOTSTRAP_SERVERS_CONFIG: &'static str = "bootstrap.servers";
    /// `client.id`
    pub const CLIENT_ID_CONFIG: &'static str = "client.id";
    /// `request.timeout.ms`
    pub const REQUEST_TIMEOUT_MS_CONFIG: &'static str = "request.timeout.ms";
    /// `default.api.timeout.ms`
    pub const DEFAULT_API_TIMEOUT_MS_CONFIG: &'static str = "default.api.timeout.ms";
    /// `retries`
    pub const RETRIES_CONFIG: &'static str = "retries";
    /// `retry.backoff.ms`
    pub const RETRY_BACKOFF_MS_CONFIG: &'static str = "retry.backoff.ms";
    /// `retry.backoff.max.ms`
    pub const RETRY_BACKOFF_MAX_MS_CONFIG: &'static str = "retry.backoff.max.ms";
    /// `reconnect.backoff.ms`
    pub const RECONNECT_BACKOFF_MS_CONFIG: &'static str = "reconnect.backoff.ms";
    /// `reconnect.backoff.max.ms`
    pub const RECONNECT_BACKOFF_MAX_MS_CONFIG: &'static str = "reconnect.backoff.max.ms";
    /// `connections.max.idle.ms`
    pub const CONNECTIONS_MAX_IDLE_MS_CONFIG: &'static str = "connections.max.idle.ms";
    /// `metadata.max.age.ms`
    pub const METADATA_MAX_AGE_MS_CONFIG: &'static str = "metadata.max.age.ms";
    /// `socket.connection.setup.timeout.ms`
    pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG: &'static str = "socket.connection.setup.timeout.ms";

    /// Creates a config from a property map. `bootstrap.servers` is required.
    ///
    /// # Errors
    ///
    /// Returns [`Error::IllegalArgument`] if `bootstrap.servers` is missing
    /// or a numeric value fails to parse.
    pub fn from_properties(props: &HashMap<String, String>) -> Result<Self, Error> {
        let mut config = Self::default();
        let mut bootstrap_set = false;

        for (key, value) in props {
            match key.as_str() {
                Self::BOOTSTRAP_SERVERS_CONFIG => {
                    config.bootstrap_servers = value.split(',').map(|s| s.trim().to_string()).collect();
                    bootstrap_set = true;
                },
                Self::CLIENT_ID_CONFIG => config.client_id = value.to_string(),
                Self::REQUEST_TIMEOUT_MS_CONFIG => config.request_timeout_ms = parse_i32(key, value)?,
                Self::DEFAULT_API_TIMEOUT_MS_CONFIG => config.default_api_timeout_ms = parse_i32(key, value)?,
                Self::RETRIES_CONFIG => config.retries = parse_i32(key, value)?,
                Self::RETRY_BACKOFF_MS_CONFIG => config.retry_backoff_ms = parse_i64(key, value)?,
                Self::RETRY_BACKOFF_MAX_MS_CONFIG => config.retry_backoff_max_ms = parse_i64(key, value)?,
                Self::RECONNECT_BACKOFF_MS_CONFIG => config.reconnect_backoff_ms = parse_i64(key, value)?,
                Self::RECONNECT_BACKOFF_MAX_MS_CONFIG => config.reconnect_backoff_max_ms = parse_i64(key, value)?,
                Self::CONNECTIONS_MAX_IDLE_MS_CONFIG => config.connections_max_idle_ms = parse_i64(key, value)?,
                Self::METADATA_MAX_AGE_MS_CONFIG => config.metadata_max_age_ms = parse_i64(key, value)?,
                Self::SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG => {
                    config.socket_connection_setup_timeout_ms = parse_i64(key, value)?;
                },
                // Unknown keys are accepted silently, as in Java.
                _ => {},
            }
        }

        if !bootstrap_set || config.bootstrap_servers.is_empty() {
            return Err(Error::config(format!(
                "Missing required configuration \"{}\" which has no default value.",
                Self::BOOTSTRAP_SERVERS_CONFIG
            )));
        }
        Ok(config)
    }

    /// The `bootstrap.servers` list.
    pub fn bootstrap_servers(&self) -> &[String] {
        &self.bootstrap_servers
    }

    /// The `client.id`.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// `request.timeout.ms`.
    pub fn request_timeout_ms(&self) -> i32 {
        self.request_timeout_ms
    }

    /// `default.api.timeout.ms`.
    pub fn default_api_timeout_ms(&self) -> i32 {
        self.default_api_timeout_ms
    }

    /// `retries`.
    pub fn retries(&self) -> i32 {
        self.retries
    }

    /// `retry.backoff.ms`.
    pub fn retry_backoff_ms(&self) -> i64 {
        self.retry_backoff_ms
    }

    /// `retry.backoff.max.ms`.
    pub fn retry_backoff_max_ms(&self) -> i64 {
        self.retry_backoff_max_ms
    }

    /// `reconnect.backoff.ms`.
    pub fn reconnect_backoff_ms(&self) -> i64 {
        self.reconnect_backoff_ms
    }

    /// `reconnect.backoff.max.ms`.
    pub fn reconnect_backoff_max_ms(&self) -> i64 {
        self.reconnect_backoff_max_ms
    }

    /// `connections.max.idle.ms`.
    pub fn connections_max_idle_ms(&self) -> i64 {
        self.connections_max_idle_ms
    }

    /// `metadata.max.age.ms`.
    pub fn metadata_max_age_ms(&self) -> i64 {
        self.metadata_max_age_ms
    }

    /// `socket.connection.setup.timeout.ms`.
    pub fn socket_connection_setup_timeout_ms(&self) -> i64 {
        self.socket_connection_setup_timeout_ms
    }
}

impl Default for AdminClientConfig {
    /// Defaults match `AdminClientConfig`'s `ConfigDef` (Apache Kafka 4.2).
    fn default() -> Self {
        Self {
            bootstrap_servers: Vec::new(),
            client_id: String::new(),
            request_timeout_ms: 30_000,
            default_api_timeout_ms: 60_000,
            retries: i32::MAX,
            retry_backoff_ms: 100,
            retry_backoff_max_ms: 1_000,
            reconnect_backoff_ms: 50,
            reconnect_backoff_max_ms: 1_000,
            connections_max_idle_ms: 300_000,
            metadata_max_age_ms: 300_000,
            socket_connection_setup_timeout_ms: 10_000,
        }
    }
}

fn parse_i32(key: &str, value: &str) -> Result<i32, Error> {
    value.trim().parse::<i32>().map_err(|_| Error::config_value(key, value))
}

fn parse_i64(key: &str, value: &str) -> Result<i64, Error> {
    value.trim().parse::<i64>().map_err(|_| Error::config_value(key, value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_required_bootstrap() {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "a:9092, b:9092".to_string());
        let config = AdminClientConfig::from_properties(&props).unwrap();
        assert_eq!(config.bootstrap_servers(), &["a:9092".to_string(), "b:9092".to_string()]);
        assert_eq!(config.request_timeout_ms(), 30_000);
        assert_eq!(config.default_api_timeout_ms(), 60_000);
        assert_eq!(config.retries(), i32::MAX);
    }

    #[test]
    fn missing_bootstrap_is_error_with_exact_message() {
        let props = HashMap::new();
        let err = AdminClientConfig::from_properties(&props).unwrap_err();
        assert_eq!(
            err.message(),
            "Missing required configuration \"bootstrap.servers\" which has no default value."
        );
    }

    #[test]
    fn overrides_and_unknown_keys() {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "a:9092".to_string());
        props.insert("client.id".to_string(), "admin-1".to_string());
        props.insert("request.timeout.ms".to_string(), "5000".to_string());
        props.insert("some.unknown.key".to_string(), "ignored".to_string());
        let config = AdminClientConfig::from_properties(&props).unwrap();
        assert_eq!(config.client_id(), "admin-1");
        assert_eq!(config.request_timeout_ms(), 5000);
    }

    #[test]
    fn invalid_numeric_value_is_error() {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "a:9092".to_string());
        props.insert("request.timeout.ms".to_string(), "not-a-number".to_string());
        assert!(AdminClientConfig::from_properties(&props).is_err());
    }
}
