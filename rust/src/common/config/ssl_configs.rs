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
//! Java-specific settings that are not applicable to rustls are omitted:
//! - `ssl.provider` — Java security provider
//! - `ssl.secure.random.implementation` — Java SecureRandom PRNG
//! - `ssl.engine.factory.class` — Java SSLEngine factory
//! - `ssl.keymanager.algorithm` — Java JSSE key manager
//! - `ssl.trustmanager.algorithm` — Java JSSE trust manager
//! - `ssl.cipher.suites` — rustls has sensible defaults
//! - `ssl.protocol` — rustls handles protocol negotiation automatically

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

    /// Config key: `ssl.keystore.password`.
    pub const SSL_KEYSTORE_PASSWORD_CONFIG: &str = "ssl.keystore.password";

    /// Config key: `ssl.key.password`.
    pub const SSL_KEY_PASSWORD_CONFIG: &str = "ssl.key.password";

    /// Config key: `ssl.truststore.type`.
    pub const SSL_TRUSTSTORE_TYPE_CONFIG: &str = "ssl.truststore.type";

    /// Default for `ssl.truststore.type`: `PEM`, deviating from Java's `JKS`
    /// (`SslConfigs.java:93`) for the reason given at
    /// [`DEFAULT_SSL_KEYSTORE_TYPE`](Self::DEFAULT_SSL_KEYSTORE_TYPE).
    pub const DEFAULT_SSL_TRUSTSTORE_TYPE: &str = "PEM";

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

    /// Password for the trust store file.
    /// Corresponds to `ssl.truststore.password`, which Java defines as
    /// `Type.PASSWORD` (`SslConfigs.java:140`).
    pub(crate) truststore_password: Option<Password>,

    /// Trusted certificates in PEM format (alternative to `truststore_location`).
    /// Corresponds to `ssl.truststore.certificates`. The certificates are
    /// public, but Java defines the key as `Type.PASSWORD`
    /// (`SslConfigs.java:137`) and hides them, so this client does too.
    pub(crate) truststore_certificates: Option<Password>,

    /// Trust store format: `"JKS"`, `"PKCS12"`, or `"PEM"`.
    /// Corresponds to `ssl.truststore.type`. Default: `"PEM"`.
    pub(crate) truststore_type: String,

    /// Path to the key store file (client certificate for mTLS).
    /// Corresponds to `ssl.keystore.location`.
    pub(crate) keystore_location: Option<String>,

    /// Password for the key store file.
    /// Corresponds to `ssl.keystore.password`, which Java defines as
    /// `Type.PASSWORD` (`SslConfigs.java:133`).
    pub(crate) keystore_password: Option<Password>,

    /// Private key in PEM format (alternative to `keystore_location`).
    /// Corresponds to `ssl.keystore.key`, which Java defines as
    /// `Type.PASSWORD` (`SslConfigs.java:135`).
    pub(crate) keystore_key: Option<Password>,

    /// Certificate chain in PEM format (alternative to `keystore_location`).
    /// Corresponds to `ssl.keystore.certificate.chain`. The certificates are
    /// public, but Java defines the key as `Type.PASSWORD`
    /// (`SslConfigs.java:136`) and hides them, so this client does too.
    pub(crate) keystore_certificate_chain: Option<Password>,

    /// Key store format: `"JKS"`, `"PKCS12"`, or `"PEM"`.
    /// Corresponds to `ssl.keystore.type`. Default: `"PEM"`.
    pub(crate) keystore_type: String,

    /// Password for the private key.
    /// Corresponds to `ssl.key.password`, which Java defines as
    /// `Type.PASSWORD` (`SslConfigs.java:134`).
    pub(crate) key_password: Option<Password>,

    /// Endpoint identification algorithm for hostname verification.
    /// `"https"` enables hostname verification (default). Empty string disables it.
    /// Corresponds to `ssl.endpoint.identification.algorithm`.
    pub(crate) endpoint_identification_algorithm: String,

    /// Enabled TLS protocol versions.
    /// Corresponds to `ssl.enabled.protocols`. Default: `["TLSv1.2", "TLSv1.3"]`.
    pub(crate) enabled_protocols: Vec<String>,
}

impl Default for SslConfigs {
    /// Returns an `SslConfigs` with sensible defaults for Rust/rustls:
    ///
    /// - PEM format for both truststore and keystore
    /// - TLSv1.2 and TLSv1.3 enabled
    /// - `"https"` endpoint identification (hostname verification enabled)
    /// - All optional paths/passwords are `None`
    fn default() -> Self {
        SslConfigs {
            truststore_location: None,
            truststore_password: None,
            truststore_certificates: None,
            truststore_type: SslConfigs::DEFAULT_SSL_TRUSTSTORE_TYPE.to_owned(),
            keystore_location: None,
            keystore_password: None,
            keystore_key: None,
            keystore_certificate_chain: None,
            keystore_type: SslConfigs::DEFAULT_SSL_KEYSTORE_TYPE.to_owned(),
            key_password: None,
            endpoint_identification_algorithm: SslConfigs::DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM.to_owned(),
            enabled_protocols: vec!["TLSv1.2".to_owned(), "TLSv1.3".to_owned()],
        }
    }
}

impl SslConfigs {
    /// The `ssl.*` settings of a configuration parsed with
    /// [`Self::add_client_ssl_support`]'s definitions, the keys Java's
    /// `SslFactory` reads through `DefaultSslEngineFactory.configure`.
    ///
    /// `ssl.cipher.suites` is read but not modelled: the TLS provider picks
    /// the suites, so a non-empty list is logged and ignored. An empty list is
    /// Java's default (`List.of()`, the provider's defaults).
    ///
    /// # Errors
    ///
    /// [`Error::LocalIllegalState`] if a key's definition is missing or of
    /// another type, a defect of the definition rather than of the input.
    pub(crate) fn from_values(values: &mut ValuesReader<'_>) -> Result<SslConfigs, Error> {
        let ssl = SslConfigs {
            truststore_location: values
                .string_or_null(SslConfigs::SSL_TRUSTSTORE_LOCATION_CONFIG)?
                .map(str::to_string),
            truststore_password: values.password_or_null(SslConfigs::SSL_TRUSTSTORE_PASSWORD_CONFIG)?.cloned(),
            truststore_certificates: values
                .password_or_null(SslConfigs::SSL_TRUSTSTORE_CERTIFICATES_CONFIG)?
                .cloned(),
            truststore_type: values.string(SslConfigs::SSL_TRUSTSTORE_TYPE_CONFIG)?.to_string(),
            keystore_location: values
                .string_or_null(SslConfigs::SSL_KEYSTORE_LOCATION_CONFIG)?
                .map(str::to_string),
            keystore_password: values.password_or_null(SslConfigs::SSL_KEYSTORE_PASSWORD_CONFIG)?.cloned(),
            keystore_key: values.password_or_null(SslConfigs::SSL_KEYSTORE_KEY_CONFIG)?.cloned(),
            keystore_certificate_chain: values
                .password_or_null(SslConfigs::SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG)?
                .cloned(),
            keystore_type: values.string(SslConfigs::SSL_KEYSTORE_TYPE_CONFIG)?.to_string(),
            key_password: values.password_or_null(SslConfigs::SSL_KEY_PASSWORD_CONFIG)?.cloned(),
            endpoint_identification_algorithm: values
                .string(SslConfigs::SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG)?
                .to_string(),
            enabled_protocols: values.list(SslConfigs::SSL_ENABLED_PROTOCOLS_CONFIG)?.to_vec(),
        };
        if !values.list(SslConfigs::SSL_CIPHER_SUITES_CONFIG)?.is_empty() {
            log::warn!(
                "{} is not supported by this client and is ignored; the TLS provider's default cipher suites are used",
                SslConfigs::SSL_CIPHER_SUITES_CONFIG
            );
        }
        Ok(ssl)
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
    ///
    /// Values of the six keys Java defines as `Type.PASSWORD`
    /// (`SslConfigs.java:133-137`, `:140`) are wrapped in a [`Password`].
    ///
    /// # Errors
    ///
    /// [`Error::Config`] if `ssl.enabled.protocols` or `ssl.cipher.suites`
    /// fails Java's `ValidList.anyNonDuplicateValues(true, false)`
    /// (`SslConfigs.java:129-130`): an empty element (duplicates are removed).
    pub(crate) fn apply_ssl_config_key(ssl: &mut SslConfigs, key: &str, value: &str) -> Result<(), Error> {
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
                ssl.enabled_protocols = ValidList::parse_any_non_duplicate_values(key, value, true)?;
            },
            SslConfigs::SSL_CIPHER_SUITES_CONFIG => {
                // Not modelled (the TLS provider picks the suites), but Java
                // still validates it with `ValidList.anyNonDuplicateValues(true,
                // false)` (`SslConfigs.java:129`), so a value Java rejects is
                // rejected here too before the key is ignored. An empty list
                // is Java's default (`List.of()`, meaning the provider's
                // defaults), so only a non-empty list is actually ignored.
                if !ValidList::parse_any_non_duplicate_values(key, value, true)?.is_empty() {
                    log::warn!(
                        "{} is not supported by this client and is ignored; the TLS provider's default cipher suites are used",
                        key
                    );
                }
            },
            _ => {
                log::warn!("Unknown SSL configuration key: {}", key);
            },
        }
        Ok(())
    }
}

// Documentation of the `ssl.*` keys, Java's `SslConfigs.*_DOC` constants,
// which Java declares public and this crate keeps private: only the
// crate-private `ConfigDef` reads them.
const SSL_PROTOCOL_DOC: &str = "The SSL protocol used to generate the SSLContext. The default is 'TLSv1.3', which should \
    be fine for most use cases. A typical alternative to the default is 'TLSv1.2'. Allowed \
    values for this config are dependent on the JVM. Clients using the defaults for this \
    config and 'ssl.enabled.protocols' will downgrade to 'TLSv1.2' if the server does not \
    support 'TLSv1.3'. If this config is set to 'TLSv1.2', however, clients will not use \
    'TLSv1.3' even if it is one of the values in <code>ssl.enabled.protocols</code> and the \
    server only supports 'TLSv1.3'.";
const SSL_PROVIDER_DOC: &str = "The name of the security provider used for SSL connections. Default value is the default \
    security provider of the JVM.";
const SSL_CIPHER_SUITES_DOC: &str = "A list of cipher suites. This is a named combination of authentication, encryption, MAC \
    and key exchange algorithm used to negotiate the security settings for a network \
    connection using TLS or SSL network protocol. By default all the available cipher suites \
    are supported.";
const SSL_ENABLED_PROTOCOLS_DOC: &str = "The list of protocols enabled for SSL connections. The default is 'TLSv1.2,TLSv1.3'. This \
    means that clients and servers will prefer TLSv1.3 if both support it and fallback to \
    TLSv1.2 otherwise (assuming both support at least TLSv1.2). This default should be fine \
    for most use cases. If this configuration is set to an empty list, Kafka will use the \
    protocols enabled by default in the underlying SSLEngine, which may include additional \
    protocols depending on the JVM version. Also see the config documentation for \
    <code>ssl.protocol</code> to understand how it can impact the TLS version negotiation \
    behavior.";
const SSL_KEYSTORE_TYPE_DOC: &str = "The file format of the key store file. This is optional for client. The values currently \
    supported by the default <code>ssl.engine.factory.class</code> are [JKS, PKCS12, PEM].";
const SSL_KEYSTORE_KEY_DOC: &str = "Private key in the format specified by 'ssl.keystore.type'. Default SSL engine factory \
    supports only PEM format with PKCS#8 keys. If the key is encrypted, key password must be \
    specified using 'ssl.key.password'";
const SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC: &str = "Certificate chain in the format specified by 'ssl.keystore.type'. Default SSL engine \
    factory supports only PEM format with a list of X.509 certificates";
const SSL_TRUSTSTORE_CERTIFICATES_DOC: &str = "Trusted certificates in the format specified by 'ssl.truststore.type'. Default SSL engine \
    factory supports only PEM format with X.509 certificates.";
const SSL_KEYSTORE_LOCATION_DOC: &str = "The location of the key store file. This is optional for client and can be used for \
    two-way authentication for client.";
const SSL_KEYSTORE_PASSWORD_DOC: &str = "The store password for the key store file. This is optional for client and only needed if \
    'ssl.keystore.location' is configured. Key store password is not supported for PEM format.";
const SSL_KEY_PASSWORD_DOC: &str = "The password of the private key in the key store file or the PEM key specified in \
    'ssl.keystore.key'.";
const SSL_TRUSTSTORE_TYPE_DOC: &str = "The file format of the trust store file. The values currently supported by the default \
    <code>ssl.engine.factory.class</code> are [JKS, PKCS12, PEM].";
const SSL_TRUSTSTORE_LOCATION_DOC: &str = "The location of the trust store file.";
const SSL_TRUSTSTORE_PASSWORD_DOC: &str = "The password for the trust store file. If a password is not set, trust store file \
    configured will still be used, but integrity checking is disabled. Trust store password is \
    not supported for PEM format.";
const SSL_KEYMANAGER_ALGORITHM_DOC: &str = "The algorithm used by key manager factory for SSL connections. Default value is the key \
    manager factory algorithm configured for the Java Virtual Machine.";
const SSL_TRUSTMANAGER_ALGORITHM_DOC: &str = "The algorithm used by trust manager factory for SSL connections. Default value is the \
    trust manager factory algorithm configured for the Java Virtual Machine.";
const SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC: &str = "The endpoint identification algorithm to validate server hostname using server \
    certificate. ";
const SSL_SECURE_RANDOM_IMPLEMENTATION_DOC: &str =
    "The SecureRandom PRNG implementation to use for SSL cryptography operations. ";

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
    /// `ssl.engine.factory.class`, a class-valued key (CLAUDE.md §2).
    pub(crate) fn add_client_ssl_support(config: ConfigDef) -> ConfigDef {
        config
            .define(
                SslConfigs::SSL_PROTOCOL_CONFIG,
                Type::String,
                SslConfigs::DEFAULT_SSL_PROTOCOL,
                Importance::Medium,
                SSL_PROTOCOL_DOC,
            )
            .define(
                SslConfigs::SSL_PROVIDER_CONFIG,
                Type::String,
                DefaultValue::Null,
                Importance::Medium,
                SSL_PROVIDER_DOC,
            )
            .define_with_validator(
                SslConfigs::SSL_CIPHER_SUITES_CONFIG,
                Type::List,
                Vec::<String>::new(),
                ValidList::any_non_duplicate_values(true, false),
                Importance::Low,
                SSL_CIPHER_SUITES_DOC,
            )
            .define_with_validator(
                SslConfigs::SSL_ENABLED_PROTOCOLS_CONFIG,
                Type::List,
                SslConfigs::DEFAULT_SSL_ENABLED_PROTOCOLS,
                ValidList::any_non_duplicate_values(true, false),
                Importance::Medium,
                SSL_ENABLED_PROTOCOLS_DOC,
            )
            .define(
                SslConfigs::SSL_KEYSTORE_TYPE_CONFIG,
                Type::String,
                SslConfigs::DEFAULT_SSL_KEYSTORE_TYPE,
                Importance::Medium,
                SSL_KEYSTORE_TYPE_DOC,
            )
            .define(
                SslConfigs::SSL_KEYSTORE_LOCATION_CONFIG,
                Type::String,
                DefaultValue::Null,
                Importance::High,
                SSL_KEYSTORE_LOCATION_DOC,
            )
            .define(
                SslConfigs::SSL_KEYSTORE_PASSWORD_CONFIG,
                Type::Password,
                DefaultValue::Null,
                Importance::High,
                SSL_KEYSTORE_PASSWORD_DOC,
            )
            .define(
                SslConfigs::SSL_KEY_PASSWORD_CONFIG,
                Type::Password,
                DefaultValue::Null,
                Importance::High,
                SSL_KEY_PASSWORD_DOC,
            )
            .define(
                SslConfigs::SSL_KEYSTORE_KEY_CONFIG,
                Type::Password,
                DefaultValue::Null,
                Importance::High,
                SSL_KEYSTORE_KEY_DOC,
            )
            .define(
                SslConfigs::SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG,
                Type::Password,
                DefaultValue::Null,
                Importance::High,
                SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC,
            )
            .define(
                SslConfigs::SSL_TRUSTSTORE_CERTIFICATES_CONFIG,
                Type::Password,
                DefaultValue::Null,
                Importance::High,
                SSL_TRUSTSTORE_CERTIFICATES_DOC,
            )
            .define(
                SslConfigs::SSL_TRUSTSTORE_TYPE_CONFIG,
                Type::String,
                SslConfigs::DEFAULT_SSL_TRUSTSTORE_TYPE,
                Importance::Medium,
                SSL_TRUSTSTORE_TYPE_DOC,
            )
            .define(
                SslConfigs::SSL_TRUSTSTORE_LOCATION_CONFIG,
                Type::String,
                DefaultValue::Null,
                Importance::High,
                SSL_TRUSTSTORE_LOCATION_DOC,
            )
            .define(
                SslConfigs::SSL_TRUSTSTORE_PASSWORD_CONFIG,
                Type::Password,
                DefaultValue::Null,
                Importance::High,
                SSL_TRUSTSTORE_PASSWORD_DOC,
            )
            .define(
                SslConfigs::SSL_KEYMANAGER_ALGORITHM_CONFIG,
                Type::String,
                SslConfigs::DEFAULT_SSL_KEYMANGER_ALGORITHM,
                Importance::Low,
                SSL_KEYMANAGER_ALGORITHM_DOC,
            )
            .define(
                SslConfigs::SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
                Type::String,
                SslConfigs::DEFAULT_SSL_TRUSTMANAGER_ALGORITHM,
                Importance::Low,
                SSL_TRUSTMANAGER_ALGORITHM_DOC,
            )
            .define(
                SslConfigs::SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
                Type::String,
                SslConfigs::DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM,
                Importance::Low,
                SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC,
            )
            .define(
                SslConfigs::SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG,
                Type::String,
                DefaultValue::Null,
                Importance::Low,
                SSL_SECURE_RANDOM_IMPLEMENTATION_DOC,
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
        let config = SslConfigs {
            truststore_location: Some("/path/to/truststore.pem".to_owned()),
            truststore_password: Some(Password::new("changeit")),
            keystore_location: Some("/path/to/keystore.pem".to_owned()),
            keystore_password: Some(Password::new("secret")),
            keystore_key: Some(Password::new("-----BEGIN PRIVATE KEY-----\n...")),
            keystore_certificate_chain: Some(Password::new("-----BEGIN CERTIFICATE-----\n...")),
            keystore_type: "PKCS12".to_owned(),
            key_password: Some(Password::new("keypass")),
            endpoint_identification_algorithm: String::new(),
            ..SslConfigs::default()
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
        assert_eq!(SslConfigs::SSL_KEYMANAGER_ALGORITHM_CONFIG, "ssl.keymanager.algorithm");
        assert_eq!(SslConfigs::SSL_TRUSTMANAGER_ALGORITHM_CONFIG, "ssl.trustmanager.algorithm");
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
    fn test_apply_ssl_config_key_sets_each_field() {
        let mut ssl = SslConfigs::default();
        SslConfigs::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_TRUSTSTORE_LOCATION_CONFIG, "/ts.pem").unwrap();
        SslConfigs::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_TRUSTSTORE_PASSWORD_CONFIG, "ts-pass").unwrap();
        SslConfigs::apply_ssl_config_key(
            &mut ssl,
            SslConfigs::SSL_TRUSTSTORE_CERTIFICATES_CONFIG,
            "-----BEGIN CERTIFICATE-----",
        )
        .unwrap();
        SslConfigs::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12").unwrap();
        SslConfigs::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_KEYSTORE_LOCATION_CONFIG, "/ks.pem").unwrap();
        SslConfigs::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_KEYSTORE_PASSWORD_CONFIG, "ks-pass").unwrap();
        SslConfigs::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_KEYSTORE_KEY_CONFIG, "-----BEGIN PRIVATE KEY-----")
            .unwrap();
        SslConfigs::apply_ssl_config_key(
            &mut ssl,
            SslConfigs::SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG,
            "-----BEGIN CERTIFICATE-----",
        )
        .unwrap();
        SslConfigs::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_KEYSTORE_TYPE_CONFIG, "JKS").unwrap();
        SslConfigs::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_KEY_PASSWORD_CONFIG, "key-pass").unwrap();
        SslConfigs::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "")
            .unwrap();
        SslConfigs::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.2, TLSv1.3")
            .unwrap();

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
        let mut ssl = SslConfigs::default();
        // Unknown key must not panic and must leave defaults untouched.
        SslConfigs::apply_ssl_config_key(&mut ssl, "ssl.unknown.key", "value").unwrap();
        assert_eq!(ssl.truststore_type, "PEM");
        assert!(ssl.truststore_location.is_none());
    }

    /// `ssl.enabled.protocols` is a `Type.LIST`, parsed like every list by
    /// `ConfigDef::parse_list`: whitespace around commas and at the ends is
    /// dropped, and a blank value is the empty list.
    #[test]
    fn test_enabled_protocols_list_parsing() {
        for value in ["TLSv1.2,TLSv1.3", "TLSv1.2, TLSv1.3", " TLSv1.2 ,TLSv1.3 "] {
            let mut ssl = SslConfigs::default();
            SslConfigs::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_ENABLED_PROTOCOLS_CONFIG, value).unwrap();
            assert_eq!(ssl.enabled_protocols, vec!["TLSv1.2", "TLSv1.3"], "{value:?}");
        }
        let mut ssl = SslConfigs::default();
        SslConfigs::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_ENABLED_PROTOCOLS_CONFIG, " ").unwrap();
        assert!(ssl.enabled_protocols.is_empty());
    }

    /// `ssl.enabled.protocols` is validated with Java's
    /// `ValidList.anyNonDuplicateValues(true, false)` (`SslConfigs.java:130`):
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
            let mut ssl = SslConfigs::default();
            match SslConfigs::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_ENABLED_PROTOCOLS_CONFIG, value) {
                Err(Error::Config(e)) => assert_eq!(e.message(), expected, "{value:?}"),
                other => panic!("expected a ConfigError for {value:?}, got {other:?}"),
            }
        }
        let mut ssl = SslConfigs::default();
        SslConfigs::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_ENABLED_PROTOCOLS_CONFIG, "").unwrap();
        assert!(ssl.enabled_protocols.is_empty());
        // Duplicates are removed by `ConfigDef.parseValue`, not rejected.
        SslConfigs::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.2, TLSv1.2")
            .unwrap();
        assert_eq!(ssl.enabled_protocols, vec!["TLSv1.2".to_string()]);
    }

    /// `ssl.cipher.suites` is not modelled, but is validated like Java
    /// (`SslConfigs.java:129`) before being ignored.
    #[test]
    fn test_cipher_suites_valid_list() {
        for (value, expected) in [
            ("A,,B", "Configuration 'ssl.cipher.suites' values must not be empty."),
            (",,", "Configuration 'ssl.cipher.suites' values must not be empty."),
        ] {
            let mut ssl = SslConfigs::default();
            match SslConfigs::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_CIPHER_SUITES_CONFIG, value) {
                Err(Error::Config(e)) => assert_eq!(e.message(), expected, "{value:?}"),
                other => panic!("expected a ConfigError for {value:?}, got {other:?}"),
            }
        }
        let mut ssl = SslConfigs::default();
        SslConfigs::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_CIPHER_SUITES_CONFIG, "A, B").unwrap();
        SslConfigs::apply_ssl_config_key(&mut ssl, SslConfigs::SSL_CIPHER_SUITES_CONFIG, "A,A").unwrap();
    }

    /// `{:?}` hides the six `Type.PASSWORD` fields and keeps the others.
    #[test]
    fn test_debug_redacts_password_fields() {
        let config = SslConfigs {
            truststore_location: Some("/etc/kafka/visible-truststore.pem".to_owned()),
            truststore_password: Some(Password::new("truststore-S3cr3t")),
            truststore_certificates: Some(Password::new("TRUSTSTORE-CERTIFICATES-PEM")),
            keystore_location: Some("/etc/kafka/visible-keystore.pem".to_owned()),
            keystore_password: Some(Password::new("keystore-S3cr3t")),
            keystore_key: Some(Password::new("-----BEGIN PRIVATE KEY-----KEYSTORE-KEY-PEM")),
            keystore_certificate_chain: Some(Password::new("KEYSTORE-CERTIFICATE-CHAIN-PEM")),
            keystore_type: "PKCS12".to_owned(),
            key_password: Some(Password::new("key-S3cr3t")),
            ..SslConfigs::default()
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
