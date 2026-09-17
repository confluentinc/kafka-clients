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

#![allow(dead_code)]
//! SSL/TLS configuration for Kafka connections.
//!
//! Translated from `org.apache.kafka.common.config.SslConfigs`.
//!
//! In Java this is a class of static string constants used as config keys with
//! `AbstractConfig`. In Rust we define a typed struct holding the actual config
//! values the client needs, plus the config key constants for compatibility.
//!
//! Java-specific settings that are not applicable to rustls are omitted:
//! - `ssl.provider` — Java security provider
//! - `ssl.secure.random.implementation` — Java SecureRandom PRNG
//! - `ssl.engine.factory.class` — Java SSLEngine factory
//! - `ssl.keymanager.algorithm` — Java JSSE key manager
//! - `ssl.trustmanager.algorithm` — Java JSSE trust manager
//! - `ssl.cipher.suites` — rustls has sensible defaults
//! - `ssl.protocol` — rustls handles protocol negotiation automatically

// ---------------------------------------------------------------------------
// Config key constants (matching Java SslConfigs constant values)
// ---------------------------------------------------------------------------

/// Config key: `ssl.protocol`.
pub const SSL_PROTOCOL_CONFIG: &str = "ssl.protocol";

/// Default SSL protocol.
pub const DEFAULT_SSL_PROTOCOL: &str = "TLSv1.3";

/// Config key: `ssl.provider`.
pub const SSL_PROVIDER_CONFIG: &str = "ssl.provider";

/// Config key: `ssl.cipher.suites`.
pub const SSL_CIPHER_SUITES_CONFIG: &str = "ssl.cipher.suites";

/// Config key: `ssl.enabled.protocols`.
pub const SSL_ENABLED_PROTOCOLS_CONFIG: &str = "ssl.enabled.protocols";

/// Default enabled SSL protocols.
pub const DEFAULT_SSL_ENABLED_PROTOCOLS: &str = "TLSv1.2,TLSv1.3";

/// Config key: `ssl.keystore.type`.
pub const SSL_KEYSTORE_TYPE_CONFIG: &str = "ssl.keystore.type";

/// Default keystore type in Java. Rust defaults to PEM (see [`SslConfig::default`]).
pub const DEFAULT_SSL_KEYSTORE_TYPE: &str = "JKS";

/// Config key: `ssl.keystore.key`.
pub const SSL_KEYSTORE_KEY_CONFIG: &str = "ssl.keystore.key";

/// Config key: `ssl.keystore.certificate.chain`.
pub const SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG: &str = "ssl.keystore.certificate.chain";

/// Config key: `ssl.truststore.certificates`.
pub const SSL_TRUSTSTORE_CERTIFICATES_CONFIG: &str = "ssl.truststore.certificates";

/// Config key: `ssl.keystore.location`.
pub const SSL_KEYSTORE_LOCATION_CONFIG: &str = "ssl.keystore.location";

/// Config key: `ssl.keystore.password`.
pub const SSL_KEYSTORE_PASSWORD_CONFIG: &str = "ssl.keystore.password";

/// Config key: `ssl.key.password`.
pub const SSL_KEY_PASSWORD_CONFIG: &str = "ssl.key.password";

/// Config key: `ssl.truststore.type`.
pub const SSL_TRUSTSTORE_TYPE_CONFIG: &str = "ssl.truststore.type";

/// Default truststore type in Java. Rust defaults to PEM (see [`SslConfig::default`]).
pub const DEFAULT_SSL_TRUSTSTORE_TYPE: &str = "JKS";

/// Config key: `ssl.truststore.location`.
pub const SSL_TRUSTSTORE_LOCATION_CONFIG: &str = "ssl.truststore.location";

/// Config key: `ssl.truststore.password`.
pub const SSL_TRUSTSTORE_PASSWORD_CONFIG: &str = "ssl.truststore.password";

/// Config key: `ssl.keymanager.algorithm`.
pub const SSL_KEYMANAGER_ALGORITHM_CONFIG: &str = "ssl.keymanager.algorithm";

/// Config key: `ssl.trustmanager.algorithm`.
pub const SSL_TRUSTMANAGER_ALGORITHM_CONFIG: &str = "ssl.trustmanager.algorithm";

/// Config key: `ssl.endpoint.identification.algorithm`.
pub const SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG: &str = "ssl.endpoint.identification.algorithm";

/// Default endpoint identification algorithm (enables hostname verification).
pub const DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM: &str = "https";

/// Config key: `ssl.secure.random.implementation`.
pub const SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG: &str = "ssl.secure.random.implementation";

/// Config key: `ssl.engine.factory.class`.
pub const SSL_ENGINE_FACTORY_CLASS_CONFIG: &str = "ssl.engine.factory.class";

// ---------------------------------------------------------------------------
// SslConfig struct
// ---------------------------------------------------------------------------

/// SSL/TLS configuration for Kafka connections.
///
/// Maps to Java's `SslConfigs` — only the client-relevant subset.
/// Java defaults to JKS keystores; Rust/rustls works natively with PEM,
/// so `truststore_type` and `keystore_type` default to `"PEM"`.
///
/// Translated from `org.apache.kafka.common.config.SslConfigs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SslConfig {
    /// Path to the trust store file (CA certificates).
    /// Corresponds to `ssl.truststore.location`.
    pub truststore_location: Option<String>,

    /// Password for the trust store file.
    /// Corresponds to `ssl.truststore.password`.
    pub truststore_password: Option<String>,

    /// Trusted certificates in PEM format (alternative to `truststore_location`).
    /// Corresponds to `ssl.truststore.certificates`.
    pub truststore_certificates: Option<String>,

    /// Trust store format: `"JKS"`, `"PKCS12"`, or `"PEM"`.
    /// Corresponds to `ssl.truststore.type`. Default: `"PEM"`.
    pub truststore_type: String,

    /// Path to the key store file (client certificate for mTLS).
    /// Corresponds to `ssl.keystore.location`.
    pub keystore_location: Option<String>,

    /// Password for the key store file.
    /// Corresponds to `ssl.keystore.password`.
    pub keystore_password: Option<String>,

    /// Private key in PEM format (alternative to `keystore_location`).
    /// Corresponds to `ssl.keystore.key`.
    pub keystore_key: Option<String>,

    /// Certificate chain in PEM format (alternative to `keystore_location`).
    /// Corresponds to `ssl.keystore.certificate.chain`.
    pub keystore_certificate_chain: Option<String>,

    /// Key store format: `"JKS"`, `"PKCS12"`, or `"PEM"`.
    /// Corresponds to `ssl.keystore.type`. Default: `"PEM"`.
    pub keystore_type: String,

    /// Password for the private key.
    /// Corresponds to `ssl.key.password`.
    pub key_password: Option<String>,

    /// Endpoint identification algorithm for hostname verification.
    /// `"https"` enables hostname verification (default). Empty string disables it.
    /// Corresponds to `ssl.endpoint.identification.algorithm`.
    pub endpoint_identification_algorithm: String,

    /// Enabled TLS protocol versions.
    /// Corresponds to `ssl.enabled.protocols`. Default: `["TLSv1.2", "TLSv1.3"]`.
    pub enabled_protocols: Vec<String>,
}

impl Default for SslConfig {
    /// Returns an `SslConfig` with sensible defaults for Rust/rustls:
    ///
    /// - PEM format for both truststore and keystore
    /// - TLSv1.2 and TLSv1.3 enabled
    /// - `"https"` endpoint identification (hostname verification enabled)
    /// - All optional paths/passwords are `None`
    fn default() -> Self {
        SslConfig {
            truststore_location: None,
            truststore_password: None,
            truststore_certificates: None,
            truststore_type: "PEM".to_owned(),
            keystore_location: None,
            keystore_password: None,
            keystore_key: None,
            keystore_certificate_chain: None,
            keystore_type: "PEM".to_owned(),
            key_password: None,
            endpoint_identification_algorithm: DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM.to_owned(),
            enabled_protocols: vec!["TLSv1.2".to_owned(), "TLSv1.3".to_owned()],
        }
    }
}

/// Applies a single `ssl.*` configuration key/value pair to `ssl`.
///
/// Shared by [`crate::producer::ProducerConfig`] and
/// [`crate::consumer::ConsumerConfig`] so the `ssl.*` parsing logic lives in
/// exactly one place. Unknown `ssl.*` keys are logged and ignored, matching
/// Java's `AbstractConfig` behavior (unknown keys are accepted silently).
///
/// The caller is responsible for matching the `"ssl."` prefix before calling
/// this; `key` is the full Java config key (e.g. `"ssl.truststore.location"`).
pub(crate) fn apply_ssl_config_key(ssl: &mut SslConfig, key: &str, value: &str) {
    match key {
        SSL_TRUSTSTORE_LOCATION_CONFIG => {
            ssl.truststore_location = Some(value.to_string());
        },
        SSL_TRUSTSTORE_PASSWORD_CONFIG => {
            ssl.truststore_password = Some(value.to_string());
        },
        SSL_TRUSTSTORE_CERTIFICATES_CONFIG => {
            ssl.truststore_certificates = Some(value.to_string());
        },
        SSL_TRUSTSTORE_TYPE_CONFIG => {
            ssl.truststore_type = value.to_string();
        },
        SSL_KEYSTORE_LOCATION_CONFIG => {
            ssl.keystore_location = Some(value.to_string());
        },
        SSL_KEYSTORE_PASSWORD_CONFIG => {
            ssl.keystore_password = Some(value.to_string());
        },
        SSL_KEYSTORE_KEY_CONFIG => {
            ssl.keystore_key = Some(value.to_string());
        },
        SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG => {
            ssl.keystore_certificate_chain = Some(value.to_string());
        },
        SSL_KEYSTORE_TYPE_CONFIG => {
            ssl.keystore_type = value.to_string();
        },
        SSL_KEY_PASSWORD_CONFIG => {
            ssl.key_password = Some(value.to_string());
        },
        SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG => {
            ssl.endpoint_identification_algorithm = value.to_string();
        },
        SSL_ENABLED_PROTOCOLS_CONFIG => {
            ssl.enabled_protocols = value.split(',').map(|s| s.trim().to_string()).collect();
        },
        _ => {
            log::warn!("Unknown SSL configuration key: {}", key);
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_truststore_type() {
        let config = SslConfig::default();
        assert_eq!(config.truststore_type, "PEM");
    }

    #[test]
    fn test_default_keystore_type() {
        let config = SslConfig::default();
        assert_eq!(config.keystore_type, "PEM");
    }

    #[test]
    fn test_default_endpoint_identification() {
        let config = SslConfig::default();
        assert_eq!(config.endpoint_identification_algorithm, "https");
    }

    #[test]
    fn test_default_enabled_protocols() {
        let config = SslConfig::default();
        assert_eq!(config.enabled_protocols, vec!["TLSv1.2", "TLSv1.3"]);
    }

    #[test]
    fn test_default_optional_fields_are_none() {
        let config = SslConfig::default();
        assert!(config.truststore_location.is_none());
        assert!(config.truststore_password.is_none());
        assert!(config.truststore_certificates.is_none());
        assert!(config.keystore_location.is_none());
        assert!(config.keystore_password.is_none());
        assert!(config.keystore_key.is_none());
        assert!(config.keystore_certificate_chain.is_none());
        assert!(config.key_password.is_none());
    }

    #[test]
    fn test_custom_config() {
        let config = SslConfig {
            truststore_location: Some("/path/to/truststore.pem".to_owned()),
            truststore_password: Some("changeit".to_owned()),
            keystore_location: Some("/path/to/keystore.pem".to_owned()),
            keystore_password: Some("secret".to_owned()),
            keystore_key: Some("-----BEGIN PRIVATE KEY-----\n...".to_owned()),
            keystore_certificate_chain: Some("-----BEGIN CERTIFICATE-----\n...".to_owned()),
            keystore_type: "PKCS12".to_owned(),
            key_password: Some("keypass".to_owned()),
            endpoint_identification_algorithm: String::new(),
            ..SslConfig::default()
        };
        assert_eq!(config.truststore_location.as_deref(), Some("/path/to/truststore.pem"));
        assert_eq!(config.truststore_password.as_deref(), Some("changeit"));
        assert_eq!(config.keystore_type, "PKCS12");
        assert_eq!(config.endpoint_identification_algorithm, "");
        // Inherited from default
        assert_eq!(config.truststore_type, "PEM");
    }

    #[test]
    fn test_clone() {
        let config = SslConfig::default();
        let cloned = config.clone();
        assert_eq!(config.truststore_type, cloned.truststore_type);
        assert_eq!(config.keystore_type, cloned.keystore_type);
        assert_eq!(config.enabled_protocols, cloned.enabled_protocols);
        assert_eq!(
            config.endpoint_identification_algorithm,
            cloned.endpoint_identification_algorithm
        );
    }

    #[test]
    fn test_config_key_constants() {
        assert_eq!(SSL_TRUSTSTORE_LOCATION_CONFIG, "ssl.truststore.location");
        assert_eq!(SSL_TRUSTSTORE_PASSWORD_CONFIG, "ssl.truststore.password");
        assert_eq!(SSL_TRUSTSTORE_CERTIFICATES_CONFIG, "ssl.truststore.certificates");
        assert_eq!(SSL_TRUSTSTORE_TYPE_CONFIG, "ssl.truststore.type");
        assert_eq!(SSL_KEYSTORE_LOCATION_CONFIG, "ssl.keystore.location");
        assert_eq!(SSL_KEYSTORE_PASSWORD_CONFIG, "ssl.keystore.password");
        assert_eq!(SSL_KEYSTORE_KEY_CONFIG, "ssl.keystore.key");
        assert_eq!(SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, "ssl.keystore.certificate.chain");
        assert_eq!(SSL_KEYSTORE_TYPE_CONFIG, "ssl.keystore.type");
        assert_eq!(SSL_KEY_PASSWORD_CONFIG, "ssl.key.password");
        assert_eq!(
            SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
            "ssl.endpoint.identification.algorithm"
        );
        assert_eq!(SSL_ENABLED_PROTOCOLS_CONFIG, "ssl.enabled.protocols");
        assert_eq!(SSL_PROTOCOL_CONFIG, "ssl.protocol");
        assert_eq!(SSL_PROVIDER_CONFIG, "ssl.provider");
        assert_eq!(SSL_CIPHER_SUITES_CONFIG, "ssl.cipher.suites");
        assert_eq!(SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, "ssl.secure.random.implementation");
        assert_eq!(SSL_ENGINE_FACTORY_CLASS_CONFIG, "ssl.engine.factory.class");
        assert_eq!(SSL_KEYMANAGER_ALGORITHM_CONFIG, "ssl.keymanager.algorithm");
        assert_eq!(SSL_TRUSTMANAGER_ALGORITHM_CONFIG, "ssl.trustmanager.algorithm");
    }

    #[test]
    fn test_default_constants() {
        assert_eq!(DEFAULT_SSL_PROTOCOL, "TLSv1.3");
        assert_eq!(DEFAULT_SSL_ENABLED_PROTOCOLS, "TLSv1.2,TLSv1.3");
        assert_eq!(DEFAULT_SSL_KEYSTORE_TYPE, "JKS");
        assert_eq!(DEFAULT_SSL_TRUSTSTORE_TYPE, "JKS");
        assert_eq!(DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "https");
    }

    #[test]
    fn test_apply_ssl_config_key_sets_each_field() {
        let mut ssl = SslConfig::default();
        apply_ssl_config_key(&mut ssl, SSL_TRUSTSTORE_LOCATION_CONFIG, "/ts.pem");
        apply_ssl_config_key(&mut ssl, SSL_TRUSTSTORE_PASSWORD_CONFIG, "ts-pass");
        apply_ssl_config_key(&mut ssl, SSL_TRUSTSTORE_CERTIFICATES_CONFIG, "-----BEGIN CERTIFICATE-----");
        apply_ssl_config_key(&mut ssl, SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12");
        apply_ssl_config_key(&mut ssl, SSL_KEYSTORE_LOCATION_CONFIG, "/ks.pem");
        apply_ssl_config_key(&mut ssl, SSL_KEYSTORE_PASSWORD_CONFIG, "ks-pass");
        apply_ssl_config_key(&mut ssl, SSL_KEYSTORE_KEY_CONFIG, "-----BEGIN PRIVATE KEY-----");
        apply_ssl_config_key(&mut ssl, SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, "-----BEGIN CERTIFICATE-----");
        apply_ssl_config_key(&mut ssl, SSL_KEYSTORE_TYPE_CONFIG, "JKS");
        apply_ssl_config_key(&mut ssl, SSL_KEY_PASSWORD_CONFIG, "key-pass");
        apply_ssl_config_key(&mut ssl, SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        apply_ssl_config_key(&mut ssl, SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.2, TLSv1.3");

        assert_eq!(ssl.truststore_location.as_deref(), Some("/ts.pem"));
        assert_eq!(ssl.truststore_password.as_deref(), Some("ts-pass"));
        assert_eq!(ssl.truststore_certificates.as_deref(), Some("-----BEGIN CERTIFICATE-----"));
        assert_eq!(ssl.truststore_type, "PKCS12");
        assert_eq!(ssl.keystore_location.as_deref(), Some("/ks.pem"));
        assert_eq!(ssl.keystore_password.as_deref(), Some("ks-pass"));
        assert_eq!(ssl.keystore_key.as_deref(), Some("-----BEGIN PRIVATE KEY-----"));
        assert_eq!(ssl.keystore_certificate_chain.as_deref(), Some("-----BEGIN CERTIFICATE-----"));
        assert_eq!(ssl.keystore_type, "JKS");
        assert_eq!(ssl.key_password.as_deref(), Some("key-pass"));
        assert_eq!(ssl.endpoint_identification_algorithm, "");
        assert_eq!(ssl.enabled_protocols, vec!["TLSv1.2", "TLSv1.3"]);
    }

    #[test]
    fn test_apply_ssl_config_key_unknown_key_is_ignored() {
        let mut ssl = SslConfig::default();
        // Unknown key must not panic and must leave defaults untouched.
        apply_ssl_config_key(&mut ssl, "ssl.unknown.key", "value");
        assert_eq!(ssl.truststore_type, "PEM");
        assert!(ssl.truststore_location.is_none());
    }
}
