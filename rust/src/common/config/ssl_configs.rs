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

//! SSL/TLS configuration for Kafka connections.
//!
//! Translated from `org.apache.kafka.common.config.SslConfigs`.
//!
//! In Java this is a class of static string constants used as config keys with
//! `AbstractConfig`. In Rust we define a typed struct holding the actual config
//! values the client needs, plus the config key constants for compatibility.
//!
//! Java-specific settings that rustls does not use are defined, parsed and
//! validated as in Java (`SslConfigs.addClientSslSupport`), but are not
//! honoured:
//! - `ssl.provider` — Java security provider
//! - `ssl.secure.random.implementation` — Java SecureRandom PRNG
//! - `ssl.keymanager.algorithm` — Java JSSE key manager
//! - `ssl.trustmanager.algorithm` — Java JSSE trust manager
//!
//! `ssl.protocol` and `ssl.cipher.suites` are honoured by the
//! [`SslFactory`](crate::common::security::ssl::SslFactory) as JSSE honours
//! them: `ssl.protocol` selects Java's `SSLContext.getInstance(protocol)`,
//! whose default protocols and default cipher suites apply when
//! `ssl.enabled.protocols` or `ssl.cipher.suites` is empty (so `TLSv1.2` keeps
//! TLS 1.3 out only with the default suites), and `ssl.cipher.suites`
//! restricts and orders the TLS provider's cipher suites.
//!
//! Four of Java's keys are not defined:
//! - `ssl.engine.factory.class`, the Java SSLEngine factory: a configuration
//!   never takes a class (CLAUDE.md §2).
//! - `ssl.keystore.password` and `ssl.truststore.password`: PEM is the only
//!   store format here, and Java rejects a store password with PEM stores
//!   (`DefaultSslEngineFactory.java:289,296,316,321`), so the keys stay
//!   undefined until another store format is implemented.
//! - `ssl.key.password`: encrypted PEM private keys are not decrypted (Java
//!   decrypts them with it, `DefaultSslEngineFactory.java:505-520`), so the
//!   key stays undefined until that is implemented.
//!
//! A value set for an undefined key is reported as unused.

use crate::common::Error;
use crate::common::config::abstract_config::ValuesReader;
use crate::common::config::config_def::{ConfigDef, DefaultValue, Importance, Type, ValidList};
use crate::common::config::types::Password;

// ---------------------------------------------------------------------------
// Config key constants (matching Java SslConfigs constant values)
// ---------------------------------------------------------------------------

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

    /// Default for `ssl.keystore.type`: `PEM`.
    ///
    /// This deviates from Java, whose `DEFAULT_SSL_KEYSTORE_TYPE` is `JKS`
    /// (`SslConfigs.java:63`): the rustls-based SSL factory reads only PEM and
    /// rejects JKS and PKCS12, so Java's default would make every TLS client
    /// fail to construct.
    pub const DEFAULT_SSL_KEYSTORE_TYPE: &str = "PEM";

    /// Config key: `ssl.keystore.key`.
    pub const SSL_KEYSTORE_KEY_CONFIG: &str = "ssl.keystore.key";

    /// Config key: `ssl.keystore.certificate.chain`.
    pub const SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG: &str = "ssl.keystore.certificate.chain";

    /// Config key: `ssl.truststore.certificates`.
    pub const SSL_TRUSTSTORE_CERTIFICATES_CONFIG: &str = "ssl.truststore.certificates";

    /// Config key: `ssl.keystore.location`.
    pub const SSL_KEYSTORE_LOCATION_CONFIG: &str = "ssl.keystore.location";

    /// Config key: `ssl.truststore.type`.
    pub const SSL_TRUSTSTORE_TYPE_CONFIG: &str = "ssl.truststore.type";

    /// Default for `ssl.truststore.type`: `PEM`, deviating from Java's `JKS`
    /// (`SslConfigs.java:93`) for the reason given at
    /// [`DEFAULT_SSL_KEYSTORE_TYPE`](Self::DEFAULT_SSL_KEYSTORE_TYPE).
    pub const DEFAULT_SSL_TRUSTSTORE_TYPE: &str = "PEM";

    /// Config key: `ssl.truststore.location`.
    pub const SSL_TRUSTSTORE_LOCATION_CONFIG: &str = "ssl.truststore.location";

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
}

// ---------------------------------------------------------------------------
// SslConfigs value fields
// ---------------------------------------------------------------------------

/// SSL/TLS configuration for Kafka connections.
///
/// Translated from `org.apache.kafka.common.config.SslConfigs`. The Java class
/// hosts only the `ssl.*` config keys and defaults; the values themselves
/// travel in the `Map<String, ?>` handed to `SslFactory.configure`. The Rust
/// struct keeps Java's name and constants, and additionally holds the
/// client-relevant subset of those values, parsed once from the client config
/// properties. The fields are crate-private: callers set them through the
/// `ssl.*` properties of a client config, as a Java caller does.
///
/// `truststore_type` and `keystore_type` default to `"PEM"`
/// ([`DEFAULT_SSL_KEYSTORE_TYPE`](Self::DEFAULT_SSL_KEYSTORE_TYPE)), where
/// Java defaults to JKS.
///
/// Every field whose key Java defines as `ConfigDef.Type.PASSWORD` is a
/// `Password`, so the derived `Debug` renders each of them as `[hidden]`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.common.config.SslConfigs")]
pub struct SslConfigs {
    /// Path to the trust store file (CA certificates).
    /// Corresponds to `ssl.truststore.location`.
    pub(crate) truststore_location: Option<String>,

    /// Trusted certificates in PEM format (alternative to `truststore_location`).
    /// Corresponds to `ssl.truststore.certificates`. The certificates are
    /// public, but Java defines the key as `Type.PASSWORD`
    /// (`SslConfigs.java:139`) and hides them, so this client does too.
    pub(crate) truststore_certificates: Option<Password>,

    /// Trust store format: `"JKS"`, `"PKCS12"`, or `"PEM"`.
    /// Corresponds to `ssl.truststore.type`. Default: `"PEM"`.
    pub(crate) truststore_type: String,

    /// Path to the key store file (client certificate for mTLS).
    /// Corresponds to `ssl.keystore.location`.
    pub(crate) keystore_location: Option<String>,

    /// Private key in PEM format (alternative to `keystore_location`).
    /// Corresponds to `ssl.keystore.key`, which Java defines as
    /// `Type.PASSWORD` (`SslConfigs.java:137`).
    pub(crate) keystore_key: Option<Password>,

    /// Certificate chain in PEM format (alternative to `keystore_location`).
    /// Corresponds to `ssl.keystore.certificate.chain`. The certificates are
    /// public, but Java defines the key as `Type.PASSWORD`
    /// (`SslConfigs.java:138`) and hides them, so this client does too.
    pub(crate) keystore_certificate_chain: Option<Password>,

    /// Key store format: `"JKS"`, `"PKCS12"`, or `"PEM"`.
    /// Corresponds to `ssl.keystore.type`. Default: `"PEM"`.
    pub(crate) keystore_type: String,

    /// Endpoint identification algorithm for hostname verification.
    /// `"https"` enables hostname verification (default). Empty string disables it.
    /// Corresponds to `ssl.endpoint.identification.algorithm`.
    pub(crate) endpoint_identification_algorithm: String,

    /// Enabled TLS protocol versions.
    /// Corresponds to `ssl.enabled.protocols`. Default: `["TLSv1.2", "TLSv1.3"]`.
    pub(crate) enabled_protocols: Vec<String>,

    /// The protocol of Java's `SSLContext`, which gives the default protocols
    /// and cipher suites. Corresponds to `ssl.protocol`. Default: `"TLSv1.3"`.
    pub(crate) protocol: String,

    /// The enabled cipher suites, in preference order; empty for the TLS
    /// provider's defaults. Corresponds to `ssl.cipher.suites`. Default: empty.
    pub(crate) cipher_suites: Vec<String>,
}

impl Default for SslConfigs {
    /// Returns an `SslConfigs` with sensible defaults for Rust/rustls:
    ///
    /// - PEM format for both truststore and keystore
    /// - TLSv1.2 and TLSv1.3 enabled, `TLSv1.3` as the protocol
    /// - the TLS provider's default cipher suites
    /// - `"https"` endpoint identification (hostname verification enabled)
    /// - All optional locations and PEM values are `None`
    fn default() -> Self {
        SslConfigs {
            truststore_location: None,
            truststore_certificates: None,
            truststore_type: SslConfigs::DEFAULT_SSL_TRUSTSTORE_TYPE.to_owned(),
            keystore_location: None,
            keystore_key: None,
            keystore_certificate_chain: None,
            keystore_type: SslConfigs::DEFAULT_SSL_KEYSTORE_TYPE.to_owned(),
            endpoint_identification_algorithm: SslConfigs::DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM.to_owned(),
            enabled_protocols: vec!["TLSv1.2".to_owned(), "TLSv1.3".to_owned()],
            protocol: SslConfigs::DEFAULT_SSL_PROTOCOL.to_owned(),
            cipher_suites: Vec::new(),
        }
    }
}

impl SslConfigs {
    /// The `ssl.*` settings of a configuration parsed with
    /// [`Self::add_client_ssl_support`]'s definitions, the keys Java's
    /// `SslFactory` reads through `DefaultSslEngineFactory.configure`.
    ///
    /// # Errors
    ///
    /// [`Error::LocalIllegalState`] if a key's definition is missing or of
    /// another type, a defect of the definition rather than of the input.
    pub(crate) fn from_values(values: &mut ValuesReader<'_>) -> Result<SslConfigs, Error> {
        Ok(SslConfigs {
            truststore_location: values
                .string_or_null(SslConfigs::SSL_TRUSTSTORE_LOCATION_CONFIG)?
                .map(str::to_string),
            truststore_certificates: values
                .password_or_null(SslConfigs::SSL_TRUSTSTORE_CERTIFICATES_CONFIG)?
                .cloned(),
            truststore_type: values.string(SslConfigs::SSL_TRUSTSTORE_TYPE_CONFIG)?.to_string(),
            keystore_location: values
                .string_or_null(SslConfigs::SSL_KEYSTORE_LOCATION_CONFIG)?
                .map(str::to_string),
            keystore_key: values.password_or_null(SslConfigs::SSL_KEYSTORE_KEY_CONFIG)?.cloned(),
            keystore_certificate_chain: values
                .password_or_null(SslConfigs::SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG)?
                .cloned(),
            keystore_type: values.string(SslConfigs::SSL_KEYSTORE_TYPE_CONFIG)?.to_string(),
            endpoint_identification_algorithm: values
                .string(SslConfigs::SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG)?
                .to_string(),
            enabled_protocols: values.list(SslConfigs::SSL_ENABLED_PROTOCOLS_CONFIG)?.to_vec(),
            protocol: values.string(SslConfigs::SSL_PROTOCOL_CONFIG)?.to_string(),
            cipher_suites: values.list(SslConfigs::SSL_CIPHER_SUITES_CONFIG)?.to_vec(),
        })
    }

    /// The `ssl.*` settings parsed from `props` with
    /// [`Self::add_client_ssl_support`]'s definitions, for the crate's tests.
    #[cfg(test)]
    pub(crate) fn parse_for_test(props: &[(&str, &str)]) -> Result<SslConfigs, Error> {
        use std::collections::HashMap;
        use std::sync::LazyLock;

        use crate::common::config::abstract_config::{AbstractConfig, string_originals};

        static DEFINITION: LazyLock<ConfigDef> = LazyLock::new(|| ConfigDef::new().with_client_ssl_support());
        let props: HashMap<String, String> = props.iter().map(|&(k, v)| (k.to_string(), v.to_string())).collect();
        let config = AbstractConfig::new(&DEFINITION, string_originals(&props), false)?;
        SslConfigs::from_values(&mut ValuesReader::new(&config))
    }
}

impl SslConfigs {
    /// Java's `SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM`, which is
    /// `KeyManagerFactory.getDefaultAlgorithm()`, the JVM's default: `SunX509`
    /// unless the `ssl.KeyManagerFactory.algorithm` security property says
    /// otherwise. Without a JVM it is the literal default.
    const DEFAULT_SSL_KEYMANGER_ALGORITHM: &str = "SunX509";

    /// Java's `SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM`, which is
    /// `TrustManagerFactory.getDefaultAlgorithm()`: `PKIX` on a stock JVM.
    const DEFAULT_SSL_TRUSTMANAGER_ALGORITHM: &str = "PKIX";

    /// Defines the client `ssl.*` keys on `config`: Java's
    /// `SslConfigs.addClientSslSupport(ConfigDef)`, in Java's order, except
    /// `ssl.engine.factory.class`, a class-valued key (CLAUDE.md §2), and
    /// `ssl.keystore.password`, `ssl.key.password` and
    /// `ssl.truststore.password`, which cannot be honoured (module docs).
    pub(crate) fn add_client_ssl_support(config: ConfigDef) -> ConfigDef {
        config
            .define(
                SslConfigs::SSL_PROTOCOL_CONFIG,
                Type::String,
                SslConfigs::DEFAULT_SSL_PROTOCOL,
                Importance::Medium,
            )
            .define(
                SslConfigs::SSL_PROVIDER_CONFIG,
                Type::String,
                DefaultValue::Null,
                Importance::Medium,
            )
            .define_with_validator(
                SslConfigs::SSL_CIPHER_SUITES_CONFIG,
                Type::List,
                Vec::<String>::new(),
                ValidList::any_non_duplicate_values(true, false),
                Importance::Low,
            )
            .define_with_validator(
                SslConfigs::SSL_ENABLED_PROTOCOLS_CONFIG,
                Type::List,
                SslConfigs::DEFAULT_SSL_ENABLED_PROTOCOLS,
                ValidList::any_non_duplicate_values(true, false),
                Importance::Medium,
            )
            .define(
                SslConfigs::SSL_KEYSTORE_TYPE_CONFIG,
                Type::String,
                SslConfigs::DEFAULT_SSL_KEYSTORE_TYPE,
                Importance::Medium,
            )
            .define(
                SslConfigs::SSL_KEYSTORE_LOCATION_CONFIG,
                Type::String,
                DefaultValue::Null,
                Importance::High,
            )
            // Java defines `ssl.keystore.password` here (`SslConfigs.java:82,135`);
            // it stays undefined because Java rejects a key store password with
            // PEM stores (`DefaultSslEngineFactory.java:289,296`) and PEM is the
            // only format here, until another format is implemented, so a value
            // set for it is reported as unused.
            //
            // Java defines `ssl.key.password` here (`SslConfigs.java:87,136`); it
            // stays undefined because encrypted PEM private keys are not
            // decrypted (`DefaultSslEngineFactory.java:505-520`), until that is
            // implemented, so a value set for it is reported as unused.
            .define(
                SslConfigs::SSL_KEYSTORE_KEY_CONFIG,
                Type::Password,
                DefaultValue::Null,
                Importance::High,
            )
            .define(
                SslConfigs::SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG,
                Type::Password,
                DefaultValue::Null,
                Importance::High,
            )
            .define(
                SslConfigs::SSL_TRUSTSTORE_CERTIFICATES_CONFIG,
                Type::Password,
                DefaultValue::Null,
                Importance::High,
            )
            .define(
                SslConfigs::SSL_TRUSTSTORE_TYPE_CONFIG,
                Type::String,
                SslConfigs::DEFAULT_SSL_TRUSTSTORE_TYPE,
                Importance::Medium,
            )
            .define(
                SslConfigs::SSL_TRUSTSTORE_LOCATION_CONFIG,
                Type::String,
                DefaultValue::Null,
                Importance::High,
            )
            // Java defines `ssl.truststore.password` here (`SslConfigs.java:98,142`);
            // it stays undefined because Java rejects a trust store password
            // with PEM stores (`DefaultSslEngineFactory.java:316,321`) and PEM is
            // the only format here, until another format is implemented, so a
            // value set for it is reported as unused.
            .define(
                SslConfigs::SSL_KEYMANAGER_ALGORITHM_CONFIG,
                Type::String,
                SslConfigs::DEFAULT_SSL_KEYMANGER_ALGORITHM,
                Importance::Low,
            )
            .define(
                SslConfigs::SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
                Type::String,
                SslConfigs::DEFAULT_SSL_TRUSTMANAGER_ALGORITHM,
                Importance::Low,
            )
            .define(
                SslConfigs::SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
                Type::String,
                SslConfigs::DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM,
                Importance::Low,
            )
            .define(
                SslConfigs::SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG,
                Type::String,
                DefaultValue::Null,
                Importance::Low,
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_truststore_type() {
        let config = SslConfigs::default();
        assert_eq!(config.truststore_type, "PEM");
    }

    #[test]
    fn test_default_keystore_type() {
        let config = SslConfigs::default();
        assert_eq!(config.keystore_type, "PEM");
    }

    #[test]
    fn test_default_endpoint_identification() {
        let config = SslConfigs::default();
        assert_eq!(config.endpoint_identification_algorithm, "https");
    }

    #[test]
    fn test_default_enabled_protocols() {
        let config = SslConfigs::default();
        assert_eq!(config.enabled_protocols, vec!["TLSv1.2", "TLSv1.3"]);
    }

    #[test]
    fn test_default_optional_fields_are_none() {
        let config = SslConfigs::default();
        assert!(config.truststore_location.is_none());
        assert!(config.truststore_certificates.is_none());
        assert!(config.keystore_location.is_none());
        assert!(config.keystore_key.is_none());
        assert!(config.keystore_certificate_chain.is_none());
    }

    #[test]
    fn test_custom_config() {
        let config = SslConfigs {
            truststore_location: Some("/path/to/truststore.pem".to_owned()),
            keystore_location: Some("/path/to/keystore.pem".to_owned()),
            keystore_key: Some(Password::new("-----BEGIN PRIVATE KEY-----\n...")),
            keystore_certificate_chain: Some(Password::new("-----BEGIN CERTIFICATE-----\n...")),
            keystore_type: "PKCS12".to_owned(),
            endpoint_identification_algorithm: String::new(),
            ..SslConfigs::default()
        };
        assert_eq!(config.truststore_location.as_deref(), Some("/path/to/truststore.pem"));
        assert_eq!(config.keystore_location.as_deref(), Some("/path/to/keystore.pem"));
        assert_eq!(config.keystore_type, "PKCS12");
        assert_eq!(config.endpoint_identification_algorithm, "");
        // Inherited from default
        assert_eq!(config.truststore_type, "PEM");
    }

    #[test]
    fn test_clone() {
        let config = SslConfigs::default();
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
        assert_eq!(SslConfigs::SSL_TRUSTSTORE_CERTIFICATES_CONFIG, "ssl.truststore.certificates");
        assert_eq!(SslConfigs::SSL_TRUSTSTORE_TYPE_CONFIG, "ssl.truststore.type");
        assert_eq!(SslConfigs::SSL_KEYSTORE_LOCATION_CONFIG, "ssl.keystore.location");
        assert_eq!(SslConfigs::SSL_KEYSTORE_KEY_CONFIG, "ssl.keystore.key");
        assert_eq!(
            SslConfigs::SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG,
            "ssl.keystore.certificate.chain"
        );
        assert_eq!(SslConfigs::SSL_KEYSTORE_TYPE_CONFIG, "ssl.keystore.type");
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
        assert_eq!(SslConfigs::SSL_KEYMANAGER_ALGORITHM_CONFIG, "ssl.keymanager.algorithm");
        assert_eq!(SslConfigs::SSL_TRUSTMANAGER_ALGORITHM_CONFIG, "ssl.trustmanager.algorithm");
    }

    /// Java's `ssl.keystore.password`, `ssl.key.password` and
    /// `ssl.truststore.password` (`SslConfigs.java:82,87,98`) are not defined,
    /// so a value set for one is reported as unused.
    #[test]
    fn test_password_keys_not_defined() {
        let names = SslConfigs::add_client_ssl_support(ConfigDef::new()).names();
        for key in ["ssl.keystore.password", "ssl.key.password", "ssl.truststore.password"] {
            assert!(!names.contains(key), "{key}");
        }
        assert!(names.contains(SslConfigs::SSL_KEYSTORE_KEY_CONFIG));
    }

    #[test]
    fn test_default_constants() {
        assert_eq!(SslConfigs::DEFAULT_SSL_PROTOCOL, "TLSv1.3");
        assert_eq!(SslConfigs::DEFAULT_SSL_ENABLED_PROTOCOLS, "TLSv1.2,TLSv1.3");
        assert_eq!(SslConfigs::DEFAULT_SSL_KEYSTORE_TYPE, "PEM");
        assert_eq!(SslConfigs::DEFAULT_SSL_TRUSTSTORE_TYPE, "PEM");
        assert_eq!(SslConfigs::DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "https");
    }

    #[test]
    fn test_from_values_sets_each_field() {
        let ssl = SslConfigs::parse_for_test(&[
            (SslConfigs::SSL_TRUSTSTORE_LOCATION_CONFIG, "/ts.pem"),
            (SslConfigs::SSL_TRUSTSTORE_CERTIFICATES_CONFIG, "-----BEGIN CERTIFICATE-----"),
            (SslConfigs::SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12"),
            (SslConfigs::SSL_KEYSTORE_LOCATION_CONFIG, "/ks.pem"),
            (SslConfigs::SSL_KEYSTORE_KEY_CONFIG, "-----BEGIN PRIVATE KEY-----"),
            (SslConfigs::SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, "-----BEGIN CERTIFICATE-----"),
            (SslConfigs::SSL_KEYSTORE_TYPE_CONFIG, "JKS"),
            (SslConfigs::SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ""),
            (SslConfigs::SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.2, TLSv1.3"),
            (SslConfigs::SSL_PROTOCOL_CONFIG, "TLSv1.2"),
            (
                SslConfigs::SSL_CIPHER_SUITES_CONFIG,
                "TLS_AES_128_GCM_SHA256, TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
            ),
        ])
        .unwrap();

        assert_eq!(ssl.truststore_location.as_deref(), Some("/ts.pem"));
        assert_eq!(
            ssl.truststore_certificates.as_ref().map(Password::value),
            Some("-----BEGIN CERTIFICATE-----")
        );
        assert_eq!(ssl.truststore_type, "PKCS12");
        assert_eq!(ssl.keystore_location.as_deref(), Some("/ks.pem"));
        assert_eq!(
            ssl.keystore_key.as_ref().map(Password::value),
            Some("-----BEGIN PRIVATE KEY-----")
        );
        assert_eq!(
            ssl.keystore_certificate_chain.as_ref().map(Password::value),
            Some("-----BEGIN CERTIFICATE-----")
        );
        assert_eq!(ssl.keystore_type, "JKS");
        assert_eq!(ssl.endpoint_identification_algorithm, "");
        assert_eq!(ssl.enabled_protocols, vec!["TLSv1.2", "TLSv1.3"]);
        assert_eq!(ssl.protocol, "TLSv1.2");
        assert_eq!(
            ssl.cipher_suites,
            vec!["TLS_AES_128_GCM_SHA256", "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"]
        );
    }

    #[test]
    fn test_default_protocol_and_cipher_suites() {
        let ssl = SslConfigs::parse_for_test(&[]).unwrap();
        assert_eq!(ssl.protocol, "TLSv1.3");
        assert!(ssl.cipher_suites.is_empty());
        assert_eq!(ssl, SslConfigs::default());
    }

    /// `ssl.cipher.suites` is validated with Java's
    /// `ValidList.anyNonDuplicateValues(true, false)` (`SslConfigs.java:131`):
    /// an empty suite is rejected with `ConfigDef`'s exact message, duplicates
    /// are removed, and an empty list is allowed.
    #[test]
    fn test_cipher_suites_valid_list() {
        for value in ["A,,B", ",,"] {
            match SslConfigs::parse_for_test(&[(SslConfigs::SSL_CIPHER_SUITES_CONFIG, value)]) {
                Err(Error::Config(e)) => assert_eq!(
                    e.message(),
                    "Configuration 'ssl.cipher.suites' values must not be empty.",
                    "{value:?}"
                ),
                other => panic!("expected a ConfigError for {value:?}, got {other:?}"),
            }
        }
        let ssl = SslConfigs::parse_for_test(&[(SslConfigs::SSL_CIPHER_SUITES_CONFIG, "A, B")]).unwrap();
        assert_eq!(ssl.cipher_suites, vec!["A", "B"]);
        let ssl = SslConfigs::parse_for_test(&[(SslConfigs::SSL_CIPHER_SUITES_CONFIG, "A,A")]).unwrap();
        assert_eq!(ssl.cipher_suites, vec!["A"]);
        let ssl = SslConfigs::parse_for_test(&[(SslConfigs::SSL_CIPHER_SUITES_CONFIG, "")]).unwrap();
        assert!(ssl.cipher_suites.is_empty());
    }

    #[test]
    fn test_from_values_ignores_an_unknown_key() {
        // An `ssl.*` key the definitions do not have is accepted, as Java's
        // `AbstractConfig` accepts any key, and leaves the defaults untouched.
        let ssl = SslConfigs::parse_for_test(&[("ssl.unknown.key", "value")]).unwrap();
        assert_eq!(ssl.truststore_type, "PEM");
        assert!(ssl.truststore_location.is_none());
    }

    /// `ssl.enabled.protocols` is a `Type.LIST`, parsed like every list by
    /// `ConfigDef::parse_list`: whitespace around commas and at the ends is
    /// dropped, and a blank value is the empty list.
    #[test]
    fn test_enabled_protocols_list_parsing() {
        for value in ["TLSv1.2,TLSv1.3", "TLSv1.2, TLSv1.3", " TLSv1.2 ,TLSv1.3 "] {
            let ssl = SslConfigs::parse_for_test(&[(SslConfigs::SSL_ENABLED_PROTOCOLS_CONFIG, value)]).unwrap();
            assert_eq!(ssl.enabled_protocols, vec!["TLSv1.2", "TLSv1.3"], "{value:?}");
        }
        let ssl = SslConfigs::parse_for_test(&[(SslConfigs::SSL_ENABLED_PROTOCOLS_CONFIG, " ")]).unwrap();
        assert!(ssl.enabled_protocols.is_empty());
    }

    /// `ssl.enabled.protocols` is validated with Java's
    /// `ValidList.anyNonDuplicateValues(true, false)` (`SslConfigs.java:132`):
    /// an empty protocol is rejected with `ConfigDef`'s exact message,
    /// duplicates are removed, and an empty list is allowed.
    #[test]
    fn test_enabled_protocols_valid_list() {
        for (value, expected) in [
            (
                "TLSv1.2,,TLSv1.3",
                "Configuration 'ssl.enabled.protocols' values must not be empty.",
            ),
            (",,", "Configuration 'ssl.enabled.protocols' values must not be empty."),
        ] {
            match SslConfigs::parse_for_test(&[(SslConfigs::SSL_ENABLED_PROTOCOLS_CONFIG, value)]) {
                Err(Error::Config(e)) => assert_eq!(e.message(), expected, "{value:?}"),
                other => panic!("expected a ConfigError for {value:?}, got {other:?}"),
            }
        }
        let ssl = SslConfigs::parse_for_test(&[(SslConfigs::SSL_ENABLED_PROTOCOLS_CONFIG, "")]).unwrap();
        assert!(ssl.enabled_protocols.is_empty());
        // Duplicates are removed by `ConfigDef.parseValue`, not rejected.
        let ssl =
            SslConfigs::parse_for_test(&[(SslConfigs::SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.2, TLSv1.2")]).unwrap();
        assert_eq!(ssl.enabled_protocols, vec!["TLSv1.2".to_string()]);
    }

    /// `{:?}` hides the three `Type.PASSWORD` fields and keeps the others.
    #[test]
    fn test_debug_redacts_password_fields() {
        let config = SslConfigs {
            truststore_location: Some("/etc/kafka/visible-truststore.pem".to_owned()),
            truststore_certificates: Some(Password::new("TRUSTSTORE-CERTIFICATES-PEM")),
            keystore_location: Some("/etc/kafka/visible-keystore.pem".to_owned()),
            keystore_key: Some(Password::new("-----BEGIN PRIVATE KEY-----KEYSTORE-KEY-PEM")),
            keystore_certificate_chain: Some(Password::new("KEYSTORE-CERTIFICATE-CHAIN-PEM")),
            keystore_type: "PKCS12".to_owned(),
            ..SslConfigs::default()
        };

        for rendered in [format!("{config:?}"), format!("{config:#?}")] {
            for secret in [
                "TRUSTSTORE-CERTIFICATES-PEM",
                "PRIVATE KEY",
                "KEYSTORE-KEY-PEM",
                "KEYSTORE-CERTIFICATE-CHAIN-PEM",
            ] {
                assert!(!rendered.contains(secret), "{secret:?} leaked: {rendered}");
            }
            assert_eq!(rendered.matches(Password::HIDDEN).count(), 3, "{rendered}");
            assert!(rendered.contains("/etc/kafka/visible-truststore.pem"), "{rendered}");
            assert!(rendered.contains("/etc/kafka/visible-keystore.pem"), "{rendered}");
            assert!(rendered.contains("\"PKCS12\""), "{rendered}");
        }
        let rendered = format!("{config:?}");
        for field in ["truststore_certificates", "keystore_key", "keystore_certificate_chain"] {
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
