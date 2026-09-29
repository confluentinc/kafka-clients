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

use crate::common::config::types::Password;

// ---------------------------------------------------------------------------
// Config key constants (matching Java SslConfigs constant values)
// ---------------------------------------------------------------------------

/// Translates the Java constants class
/// `org.apache.kafka.common.config.SslConfigs`,
/// which has no instance state, so it becomes a unit struct hosting its
/// statics as associated items.
pub struct SslConfigs;

impl SslConfigs {
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
}

// ---------------------------------------------------------------------------
// SslConfig struct
// ---------------------------------------------------------------------------

/// SSL/TLS configuration for Kafka connections.
///
/// Maps to Java's `SslConfigs` — only the client-relevant subset.
/// Java defaults to JKS keystores; Rust/rustls works natively with PEM,
/// so `truststore_type` and `keystore_type` default to `"PEM"`.
///
/// Every field whose key Java defines as `ConfigDef.Type.PASSWORD` is a
/// [`Password`], so the derived `Debug` renders each of them as `[hidden]`.
///
/// Translated from `org.apache.kafka.common.config.SslConfigs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SslConfig {
    /// Path to the trust store file (CA certificates).
    /// Corresponds to `ssl.truststore.location`.
    pub truststore_location: Option<String>,

    /// Password for the trust store file.
    /// Corresponds to `ssl.truststore.password`, which Java defines as
    /// `Type.PASSWORD` (`SslConfigs.java:140`).
    pub truststore_password: Option<Password>,

    /// Trusted certificates in PEM format (alternative to `truststore_location`).
    /// Corresponds to `ssl.truststore.certificates`. The certificates are
    /// public, but Java defines the key as `Type.PASSWORD`
    /// (`SslConfigs.java:137`) and hides them, so this client does too.
    pub truststore_certificates: Option<Password>,

    /// Trust store format: `"JKS"`, `"PKCS12"`, or `"PEM"`.
    /// Corresponds to `ssl.truststore.type`. Default: `"PEM"`.
    pub truststore_type: String,

    /// Path to the key store file (client certificate for mTLS).
    /// Corresponds to `ssl.keystore.location`.
    pub keystore_location: Option<String>,

    /// Password for the key store file.
    /// Corresponds to `ssl.keystore.password`, which Java defines as
    /// `Type.PASSWORD` (`SslConfigs.java:133`).
    pub keystore_password: Option<Password>,

    /// Private key in PEM format (alternative to `keystore_location`).
    /// Corresponds to `ssl.keystore.key`, which Java defines as
    /// `Type.PASSWORD` (`SslConfigs.java:135`).
    pub keystore_key: Option<Password>,

    /// Certificate chain in PEM format (alternative to `keystore_location`).
    /// Corresponds to `ssl.keystore.certificate.chain`. The certificates are
    /// public, but Java defines the key as `Type.PASSWORD`
    /// (`SslConfigs.java:136`) and hides them, so this client does too.
    pub keystore_certificate_chain: Option<Password>,

    /// Key store format: `"JKS"`, `"PKCS12"`, or `"PEM"`.
    /// Corresponds to `ssl.keystore.type`. Default: `"PEM"`.
    pub keystore_type: String,

    /// Password for the private key.
    /// Corresponds to `ssl.key.password`, which Java defines as
    /// `Type.PASSWORD` (`SslConfigs.java:134`).
    pub key_password: Option<Password>,

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
            endpoint_identification_algorithm: SslConfigs::DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM.to_owned(),
            enabled_protocols: vec!["TLSv1.2".to_owned(), "TLSv1.3".to_owned()],
        }
    }
}

impl SslConfig {
    /// Applies a single `ssl.*` configuration key/value pair to `ssl`.
    ///
    /// Shared by [`crate::producer::ProducerConfig`] and
    /// [`crate::consumer::ConsumerConfig`] so the `ssl.*` parsing logic lives in
    /// exactly one place. Unknown `ssl.*` keys are logged and ignored, matching
    /// Java's `AbstractConfig` behavior (unknown keys are accepted silently).
    ///
    /// The caller is responsible for matching the `"ssl."` prefix before calling
    /// this; `key` is the full Java config key (e.g. `"ssl.truststore.location"`).
    ///
    /// Values of the six keys Java defines as `Type.PASSWORD`
    /// (`SslConfigs.java:133-137`, `:140`) are wrapped in a [`Password`].
    pub(crate) fn apply_ssl_config_key(ssl: &mut SslConfig, key: &str, value: &str) {
        match key {
            SslConfigs::SSL_TRUSTSTORE_LOCATION_CONFIG => {
                ssl.truststore_location = Some(value.to_string());
            },
            SslConfigs::SSL_TRUSTSTORE_PASSWORD_CONFIG => {
                ssl.truststore_password = Some(Password::new(value));
            },
            SslConfigs::SSL_TRUSTSTORE_CERTIFICATES_CONFIG => {
                ssl.truststore_certificates = Some(Password::new(value));
            },
            SslConfigs::SSL_TRUSTSTORE_TYPE_CONFIG => {
                ssl.truststore_type = value.to_string();
            },
            SslConfigs::SSL_KEYSTORE_LOCATION_CONFIG => {
                ssl.keystore_location = Some(value.to_string());
            },
            SslConfigs::SSL_KEYSTORE_PASSWORD_CONFIG => {
                ssl.keystore_password = Some(Password::new(value));
            },
            SslConfigs::SSL_KEYSTORE_KEY_CONFIG => {
                ssl.keystore_key = Some(Password::new(value));
            },
            SslConfigs::SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG => {
                ssl.keystore_certificate_chain = Some(Password::new(value));
            },
            SslConfigs::SSL_KEYSTORE_TYPE_CONFIG => {
                ssl.keystore_type = value.to_string();
            },
            SslConfigs::SSL_KEY_PASSWORD_CONFIG => {
                ssl.key_password = Some(Password::new(value));
            },
            SslConfigs::SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG => {
                ssl.endpoint_identification_algorithm = value.to_string();
            },
            SslConfigs::SSL_ENABLED_PROTOCOLS_CONFIG => {
                ssl.enabled_protocols = value.split(',').map(|s| s.trim().to_string()).collect();
            },
            _ => {
                log::warn!("Unknown SSL configuration key: {}", key);
            },
        }
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
            truststore_password: Some(Password::new("changeit")),
            keystore_location: Some("/path/to/keystore.pem".to_owned()),
            keystore_password: Some(Password::new("secret")),
            keystore_key: Some(Password::new("-----BEGIN PRIVATE KEY-----\n...")),
            keystore_certificate_chain: Some(Password::new("-----BEGIN CERTIFICATE-----\n...")),
            keystore_type: "PKCS12".to_owned(),
            key_password: Some(Password::new("keypass")),
            endpoint_identification_algorithm: String::new(),
            ..SslConfig::default()
        };
        assert_eq!(config.truststore_location.as_deref(), Some("/path/to/truststore.pem"));
        assert_eq!(config.truststore_password.as_ref().map(Password::value), Some("changeit"));
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
        assert_eq!(SslConfigs::SSL_TRUSTSTORE_LOCATION_CONFIG, "ssl.truststore.location");
        assert_eq!(SslConfigs::SSL_TRUSTSTORE_PASSWORD_CONFIG, "ssl.truststore.password");
        assert_eq!(SslConfigs::SSL_TRUSTSTORE_CERTIFICATES_CONFIG, "ssl.truststore.certificates");
        assert_eq!(SslConfigs::SSL_TRUSTSTORE_TYPE_CONFIG, "ssl.truststore.type");
        assert_eq!(SslConfigs::SSL_KEYSTORE_LOCATION_CONFIG, "ssl.keystore.location");
        assert_eq!(SslConfigs::SSL_KEYSTORE_PASSWORD_CONFIG, "ssl.keystore.password");
        assert_eq!(SslConfigs::SSL_KEYSTORE_KEY_CONFIG, "ssl.keystore.key");
        assert_eq!(
            SslConfigs::SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG,
            "ssl.keystore.certificate.chain"
        );
        assert_eq!(SslConfigs::SSL_KEYSTORE_TYPE_CONFIG, "ssl.keystore.type");
        assert_eq!(SslConfigs::SSL_KEY_PASSWORD_CONFIG, "ssl.key.password");
        assert_eq!(
            SslConfigs::SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
            "ssl.endpoint.identification.algorithm"
        );
        assert_eq!(SslConfigs::SSL_ENABLED_PROTOCOLS_CONFIG, "ssl.enabled.protocols");
        assert_eq!(SslConfigs::SSL_PROTOCOL_CONFIG, "ssl.protocol");
        assert_eq!(SslConfigs::SSL_PROVIDER_CONFIG, "ssl.provider");
        assert_eq!(SslConfigs::SSL_CIPHER_SUITES_CONFIG, "ssl.cipher.suites");
        assert_eq!(
            SslConfigs::SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG,
            "ssl.secure.random.implementation"
        );
        assert_eq!(SslConfigs::SSL_ENGINE_FACTORY_CLASS_CONFIG, "ssl.engine.factory.class");
        assert_eq!(SslConfigs::SSL_KEYMANAGER_ALGORITHM_CONFIG, "ssl.keymanager.algorithm");
        assert_eq!(SslConfigs::SSL_TRUSTMANAGER_ALGORITHM_CONFIG, "ssl.trustmanager.algorithm");
    }

    #[test]
    fn test_default_constants() {
        assert_eq!(SslConfigs::DEFAULT_SSL_PROTOCOL, "TLSv1.3");
        assert_eq!(SslConfigs::DEFAULT_SSL_ENABLED_PROTOCOLS, "TLSv1.2,TLSv1.3");
        assert_eq!(SslConfigs::DEFAULT_SSL_KEYSTORE_TYPE, "JKS");
        assert_eq!(SslConfigs::DEFAULT_SSL_TRUSTSTORE_TYPE, "JKS");
        assert_eq!(SslConfigs::DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "https");
    }

    #[test]
    fn test_apply_ssl_config_key_sets_each_field() {
        let mut ssl = SslConfig::default();
        SslConfig::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_TRUSTSTORE_LOCATION_CONFIG, "/ts.pem");
        SslConfig::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_TRUSTSTORE_PASSWORD_CONFIG, "ts-pass");
        SslConfig::apply_ssl_config_key(
            &mut ssl,
            SslConfigs::SSL_TRUSTSTORE_CERTIFICATES_CONFIG,
            "-----BEGIN CERTIFICATE-----",
        );
        SslConfig::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12");
        SslConfig::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_KEYSTORE_LOCATION_CONFIG, "/ks.pem");
        SslConfig::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_KEYSTORE_PASSWORD_CONFIG, "ks-pass");
        SslConfig::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_KEYSTORE_KEY_CONFIG, "-----BEGIN PRIVATE KEY-----");
        SslConfig::apply_ssl_config_key(
            &mut ssl,
            SslConfigs::SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG,
            "-----BEGIN CERTIFICATE-----",
        );
        SslConfig::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_KEYSTORE_TYPE_CONFIG, "JKS");
        SslConfig::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_KEY_PASSWORD_CONFIG, "key-pass");
        SslConfig::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        SslConfig::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.2, TLSv1.3");

        assert_eq!(ssl.truststore_location.as_deref(), Some("/ts.pem"));
        assert_eq!(ssl.truststore_password.as_ref().map(Password::value), Some("ts-pass"));
        assert_eq!(
            ssl.truststore_certificates.as_ref().map(Password::value),
            Some("-----BEGIN CERTIFICATE-----")
        );
        assert_eq!(ssl.truststore_type, "PKCS12");
        assert_eq!(ssl.keystore_location.as_deref(), Some("/ks.pem"));
        assert_eq!(ssl.keystore_password.as_ref().map(Password::value), Some("ks-pass"));
        assert_eq!(
            ssl.keystore_key.as_ref().map(Password::value),
            Some("-----BEGIN PRIVATE KEY-----")
        );
        assert_eq!(
            ssl.keystore_certificate_chain.as_ref().map(Password::value),
            Some("-----BEGIN CERTIFICATE-----")
        );
        assert_eq!(ssl.keystore_type, "JKS");
        assert_eq!(ssl.key_password.as_ref().map(Password::value), Some("key-pass"));
        assert_eq!(ssl.endpoint_identification_algorithm, "");
        assert_eq!(ssl.enabled_protocols, vec!["TLSv1.2", "TLSv1.3"]);
    }

    #[test]
    fn test_apply_ssl_config_key_unknown_key_is_ignored() {
        let mut ssl = SslConfig::default();
        // Unknown key must not panic and must leave defaults untouched.
        SslConfig::apply_ssl_config_key(&mut ssl, "ssl.unknown.key", "value");
        assert_eq!(ssl.truststore_type, "PEM");
        assert!(ssl.truststore_location.is_none());
    }

    /// `{:?}` hides the six `Type.PASSWORD` fields and keeps the others.
    #[test]
    fn test_debug_redacts_password_fields() {
        let config = SslConfig {
            truststore_location: Some("/etc/kafka/visible-truststore.pem".to_owned()),
            truststore_password: Some(Password::new("truststore-S3cr3t")),
            truststore_certificates: Some(Password::new("TRUSTSTORE-CERTIFICATES-PEM")),
            keystore_location: Some("/etc/kafka/visible-keystore.pem".to_owned()),
            keystore_password: Some(Password::new("keystore-S3cr3t")),
            keystore_key: Some(Password::new("-----BEGIN PRIVATE KEY-----KEYSTORE-KEY-PEM")),
            keystore_certificate_chain: Some(Password::new("KEYSTORE-CERTIFICATE-CHAIN-PEM")),
            keystore_type: "PKCS12".to_owned(),
            key_password: Some(Password::new("key-S3cr3t")),
            ..SslConfig::default()
        };

        for rendered in [format!("{config:?}"), format!("{config:#?}")] {
            for secret in [
                "truststore-S3cr3t",
                "TRUSTSTORE-CERTIFICATES-PEM",
                "keystore-S3cr3t",
                "PRIVATE KEY",
                "KEYSTORE-KEY-PEM",
                "KEYSTORE-CERTIFICATE-CHAIN-PEM",
                "key-S3cr3t",
            ] {
                assert!(!rendered.contains(secret), "{secret:?} leaked: {rendered}");
            }
            assert_eq!(rendered.matches(Password::HIDDEN).count(), 6, "{rendered}");
            assert!(rendered.contains("/etc/kafka/visible-truststore.pem"), "{rendered}");
            assert!(rendered.contains("/etc/kafka/visible-keystore.pem"), "{rendered}");
            assert!(rendered.contains("\"PKCS12\""), "{rendered}");
        }
        let rendered = format!("{config:?}");
        for field in [
            "truststore_password",
            "truststore_certificates",
            "keystore_password",
            "keystore_key",
            "keystore_certificate_chain",
            "key_password",
        ] {
            let hidden = format!("{field}: Some([hidden])");
            assert!(rendered.contains(&hidden), "{hidden} missing: {rendered}");
        }
        assert!(
            rendered.contains("truststore_location: Some(\"/etc/kafka/visible-truststore.pem\")"),
            "{rendered}"
        );
        assert!(rendered.contains("keystore_type: \"PKCS12\""), "{rendered}");
    }
}
