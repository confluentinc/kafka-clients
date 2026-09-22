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

use crate::CommonClientConfigs;
use crate::common::Error;
use crate::common::config::{SaslConfig, SaslConfigs, SslConfig};
use crate::common::security::SecurityProtocol;

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

    // --- Security ---
    /// `security.protocol` - Protocol used to communicate with brokers.
    /// Default: `SecurityProtocol::Plaintext`.
    security_protocol: SecurityProtocol,

    /// SASL configuration (mechanism, JAAS config, credentials).
    sasl_config: SaslConfig,

    /// SSL/TLS configuration.
    ssl_config: SslConfig,
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
    /// `security.protocol`
    pub const SECURITY_PROTOCOL_CONFIG: &'static str = CommonClientConfigs::SECURITY_PROTOCOL_CONFIG;
    /// `sasl.mechanism`
    pub const SASL_MECHANISM_CONFIG: &'static str = SaslConfigs::SASL_MECHANISM;
    /// `sasl.jaas.config`
    pub const SASL_JAAS_CONFIG: &'static str = SaslConfigs::SASL_JAAS_CONFIG;

    /// Creates a config from a property map. `bootstrap.servers` is required.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LocalIllegalArgument`] if `bootstrap.servers` is missing
    /// or a numeric value fails to parse.
    pub fn new(props: &HashMap<String, String>) -> Result<Self, Error> {
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
                Self::SECURITY_PROTOCOL_CONFIG => {
                    config.security_protocol = SecurityProtocol::for_name(value).ok_or_else(|| {
                        Error::config_name_value_message(
                            key,
                            value,
                            format!("Valid values are: {:?}", SecurityProtocol::names()),
                        )
                    })?;
                },
                Self::SASL_MECHANISM_CONFIG => {
                    config.sasl_config.mechanism = value.to_string();
                },
                Self::SASL_JAAS_CONFIG => {
                    config.sasl_config.jaas_config = if value.is_empty() {
                        None
                    } else {
                        Some(value.to_string())
                    };
                },
                key if key.starts_with("ssl.") => {
                    SslConfig::apply_ssl_config_key(&mut config.ssl_config, key, value);
                },
                // Unknown keys are accepted silently, as in Java.
                _ => {},
            }
        }

        if !bootstrap_set || config.bootstrap_servers.is_empty() {
            return Err(Error::config_message(format!(
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

    /// `security.protocol`.
    pub fn security_protocol(&self) -> SecurityProtocol {
        self.security_protocol
    }

    /// SASL configuration (mechanism, JAAS config, credentials).
    pub fn sasl_config(&self) -> &SaslConfig {
        &self.sasl_config
    }

    /// SSL/TLS configuration.
    pub fn ssl_config(&self) -> &SslConfig {
        &self.ssl_config
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
            security_protocol: SecurityProtocol::Plaintext,
            sasl_config: SaslConfig::default(),
            ssl_config: SslConfig::default(),
        }
    }
}

fn parse_i32(key: &str, value: &str) -> Result<i32, Error> {
    value.trim().parse::<i32>().map_err(|_| Error::config_name_value(key, value))
}

fn parse_i64(key: &str, value: &str) -> Result<i64, Error> {
    value.trim().parse::<i64>().map_err(|_| Error::config_name_value(key, value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_required_bootstrap() {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "a:9092, b:9092".to_string());
        let config = AdminClientConfig::new(&props).unwrap();
        assert_eq!(config.bootstrap_servers(), &["a:9092".to_string(), "b:9092".to_string()]);
        assert_eq!(config.request_timeout_ms(), 30_000);
        assert_eq!(config.default_api_timeout_ms(), 60_000);
        assert_eq!(config.retries(), i32::MAX);
        assert_eq!(config.security_protocol(), SecurityProtocol::Plaintext);
    }

    #[test]
    fn missing_bootstrap_is_error_with_exact_message() {
        let props = HashMap::new();
        let err = AdminClientConfig::new(&props).unwrap_err();
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
        let config = AdminClientConfig::new(&props).unwrap();
        assert_eq!(config.client_id(), "admin-1");
        assert_eq!(config.request_timeout_ms(), 5000);
    }

    #[test]
    fn invalid_numeric_value_is_error() {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "a:9092".to_string());
        props.insert("request.timeout.ms".to_string(), "not-a-number".to_string());
        assert!(AdminClientConfig::new(&props).is_err());
    }

    /// Translated from `ProducerConfigTest.testInvalidSecurityProtocol`, adapted
    /// to `AdminClientConfig`.
    #[test]
    fn test_invalid_security_protocol() {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        props.insert("security.protocol".to_string(), "abc".to_string());
        let err = AdminClientConfig::new(&props).unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("security.protocol"),
            "Error message should contain config key, got: {}",
            msg
        );
    }

    /// Translated from `ProducerConfigTest.testCaseInsensitiveSecurityProtocol`,
    /// adapted to `AdminClientConfig`.
    #[test]
    fn test_case_insensitive_security_protocol() {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        props.insert("security.protocol".to_string(), "sasl_ssl".to_string());
        let config = AdminClientConfig::new(&props).unwrap();
        assert_eq!(config.security_protocol(), SecurityProtocol::SaslSsl);
    }

    #[test]
    fn test_sasl_config_from_properties() {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        props.insert("sasl.mechanism".to_string(), "PLAIN".to_string());
        props.insert(
            "sasl.jaas.config".to_string(),
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"alice\" password=\"secret\";"
                .to_string(),
        );
        let config = AdminClientConfig::new(&props).unwrap();
        assert_eq!(config.sasl_config().mechanism, "PLAIN");
        assert_eq!(config.sasl_config().resolve_username(), Some("alice"));
        assert_eq!(config.sasl_config().resolve_password(), Some("secret"));
    }

    #[test]
    fn test_ssl_config_from_properties() {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        props.insert("ssl.truststore.location".to_string(), "/path/to/truststore.pem".to_string());
        props.insert("ssl.keystore.location".to_string(), "/path/to/keystore.pem".to_string());
        props.insert("ssl.endpoint.identification.algorithm".to_string(), String::new());
        let config = AdminClientConfig::new(&props).unwrap();
        assert_eq!(
            config.ssl_config().truststore_location.as_deref(),
            Some("/path/to/truststore.pem")
        );
        assert_eq!(config.ssl_config().keystore_location.as_deref(), Some("/path/to/keystore.pem"));
        assert_eq!(config.ssl_config().endpoint_identification_algorithm, "");
    }
}
