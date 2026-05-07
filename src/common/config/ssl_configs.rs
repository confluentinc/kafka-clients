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

//! Translation of `org.apache.kafka.common.config.SslConfigs`.
//!
//! Constants and the `add_client_ssl_support` schema-extension helper. The
//! Rust client uses `tokio_rustls` rather than the JVM's `SSLEngine`, but
//! the property names are part of the public API surface and must match
//! the Java values.
//!
//! Defaults that hinge on JVM behaviour (`KeyManagerFactory.getDefaultAlgorithm()`,
//! `TrustManagerFactory.getDefaultAlgorithm()`) are passed through as
//! `null` (i.e. [`crate::common::config::config_def::ConfigValue::Null`]);
//! the SSL transport layer (Phase 5b) resolves those at handshake time
//! against `rustls`'s own defaults.

use std::sync::Arc;

use crate::common::config::config_def::{ConfigDef, ConfigValue, Importance, Type, ValidList, Validator};
use crate::common::errors::KafkaError;

pub const SSL_PROTOCOL_CONFIG: &str = "ssl.protocol";
pub const SSL_PROTOCOL_DOC: &str = concat!(
    "The SSL protocol used to generate the SSLContext. The default is 'TLSv1.3', ",
    "which should be fine for most use cases. A typical alternative to the default is 'TLSv1.2'. Allowed values for ",
    "this config are dependent on the JVM. ",
    "Clients using the defaults for this config and 'ssl.enabled.protocols' will downgrade to 'TLSv1.2' if ",
    "the server does not support 'TLSv1.3'. If this config is set to 'TLSv1.2', however, clients will not use 'TLSv1.3' even ",
    "if it is one of the values in <code>ssl.enabled.protocols</code> and the server only supports 'TLSv1.3'.",
);
pub const DEFAULT_SSL_PROTOCOL: &str = "TLSv1.3";

pub const SSL_PROVIDER_CONFIG: &str = "ssl.provider";
pub const SSL_PROVIDER_DOC: &str = "The name of the security provider used for SSL connections. Default value is the default security provider of the JVM.";

pub const SSL_CIPHER_SUITES_CONFIG: &str = "ssl.cipher.suites";
pub const SSL_CIPHER_SUITES_DOC: &str = concat!(
    "A list of cipher suites. This is a named combination of authentication, encryption, MAC and key exchange algorithm ",
    "used to negotiate the security settings for a network connection using TLS or SSL network protocol. By default ",
    "all the available cipher suites are supported.",
);

pub const SSL_ENABLED_PROTOCOLS_CONFIG: &str = "ssl.enabled.protocols";
pub const SSL_ENABLED_PROTOCOLS_DOC: &str = concat!(
    "The list of protocols enabled for SSL connections. ",
    "The default is 'TLSv1.2,TLSv1.3'. This means that clients and servers will prefer TLSv1.3 if both support it ",
    "and fallback to TLSv1.2 otherwise (assuming both support at least TLSv1.2). This default should be fine for most use ",
    "cases. If this configuration is set to an empty list, Kafka will use the protocols enabled by default in the underlying SSLEngine, ",
    "which may include additional protocols depending on the JVM version. ",
    "Also see the config documentation for <code>ssl.protocol</code> to understand how it can impact the TLS version negotiation behavior.",
);
pub const DEFAULT_SSL_ENABLED_PROTOCOLS: &str = "TLSv1.2,TLSv1.3";

pub const SSL_KEYSTORE_TYPE_CONFIG: &str = "ssl.keystore.type";
pub const SSL_KEYSTORE_TYPE_DOC: &str = concat!(
    "The file format of the key store file. This is optional for client. The values currently supported by the default ",
    "`ssl.engine.factory.class` are [JKS, PKCS12, PEM].",
);
pub const DEFAULT_SSL_KEYSTORE_TYPE: &str = "JKS";

pub const SSL_KEYSTORE_KEY_CONFIG: &str = "ssl.keystore.key";
pub const SSL_KEYSTORE_KEY_DOC: &str = concat!(
    "Private key in the format specified by 'ssl.keystore.type'. Default SSL engine factory supports only PEM format with ",
    "PKCS#8 keys. If the key is encrypted, key password must be specified using 'ssl.key.password'",
);

pub const SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG: &str = "ssl.keystore.certificate.chain";
pub const SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC: &str = concat!(
    "Certificate chain in the format specified by 'ssl.keystore.type'. Default SSL engine factory supports only PEM format ",
    "with a list of X.509 certificates",
);

pub const SSL_TRUSTSTORE_CERTIFICATES_CONFIG: &str = "ssl.truststore.certificates";
pub const SSL_TRUSTSTORE_CERTIFICATES_DOC: &str = concat!(
    "Trusted certificates in the format specified by 'ssl.truststore.type'. Default SSL engine factory supports only PEM ",
    "format with X.509 certificates.",
);

pub const SSL_KEYSTORE_LOCATION_CONFIG: &str = "ssl.keystore.location";
pub const SSL_KEYSTORE_LOCATION_DOC: &str = "The location of the key store file. This is optional for client and can be used for two-way authentication for client.";

pub const SSL_KEYSTORE_PASSWORD_CONFIG: &str = "ssl.keystore.password";
pub const SSL_KEYSTORE_PASSWORD_DOC: &str = concat!(
    "The store password for the key store file. This is optional for client and only needed if 'ssl.keystore.location' is configured. ",
    "Key store password is not supported for PEM format.",
);

pub const SSL_KEY_PASSWORD_CONFIG: &str = "ssl.key.password";
pub const SSL_KEY_PASSWORD_DOC: &str =
    "The password of the private key in the key store file or the PEM key specified in 'ssl.keystore.key'.";

pub const SSL_TRUSTSTORE_TYPE_CONFIG: &str = "ssl.truststore.type";
pub const SSL_TRUSTSTORE_TYPE_DOC: &str = concat!(
    "The file format of the trust store file. The values currently supported by the default `ssl.engine.factory.class` ",
    "are [JKS, PKCS12, PEM].",
);
pub const DEFAULT_SSL_TRUSTSTORE_TYPE: &str = "JKS";

pub const SSL_TRUSTSTORE_LOCATION_CONFIG: &str = "ssl.truststore.location";
pub const SSL_TRUSTSTORE_LOCATION_DOC: &str = "The location of the trust store file.";

pub const SSL_TRUSTSTORE_PASSWORD_CONFIG: &str = "ssl.truststore.password";
pub const SSL_TRUSTSTORE_PASSWORD_DOC: &str = concat!(
    "The password for the trust store file. If a password is not set, trust store file configured will still be used, but ",
    "integrity checking is disabled. Trust store password is not supported for PEM format.",
);

pub const SSL_KEYMANAGER_ALGORITHM_CONFIG: &str = "ssl.keymanager.algorithm";
pub const SSL_KEYMANAGER_ALGORITHM_DOC: &str = concat!(
    "The algorithm used by key manager factory for SSL connections. Default value is the key manager factory algorithm ",
    "configured for the Java Virtual Machine.",
);

pub const SSL_TRUSTMANAGER_ALGORITHM_CONFIG: &str = "ssl.trustmanager.algorithm";
pub const SSL_TRUSTMANAGER_ALGORITHM_DOC: &str = concat!(
    "The algorithm used by trust manager factory for SSL connections. Default value is the trust manager factory ",
    "algorithm configured for the Java Virtual Machine.",
);

pub const SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG: &str = "ssl.secure.random.implementation";
pub const SSL_SECURE_RANDOM_IMPLEMENTATION_DOC: &str =
    "The SecureRandom PRNG implementation to use for SSL cryptography operations. ";

pub const SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG: &str = "ssl.endpoint.identification.algorithm";
pub const SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC: &str =
    "The endpoint identification algorithm to validate server hostname using server certificate. ";
pub const DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM: &str = "https";

pub const SSL_CLIENT_AUTH_CONFIG: &str = "ssl.client.auth";

pub const SSL_ENGINE_FACTORY_CLASS_CONFIG: &str = "ssl.engine.factory.class";
pub const SSL_ENGINE_FACTORY_CLASS_DOC: &str = concat!(
    "The class of type org.apache.kafka.common.security.auth.SslEngineFactory to provide SSLEngine objects. ",
    "Default value is org.apache.kafka.common.security.ssl.DefaultSslEngineFactory",
);

/// Add the standard SSL client configuration options to `def`. Mirrors
/// `SslConfigs.addClientSslSupport(ConfigDef)`.
///
/// Defaults that depend on JVM behaviour
/// (`KeyManagerFactory.getDefaultAlgorithm()` /
/// `TrustManagerFactory.getDefaultAlgorithm()`) are registered as
/// [`ConfigValue::Null`] and resolved by the SSL transport (Phase 5b)
/// against `rustls`'s own defaults at handshake time.
pub fn add_client_ssl_support(def: &mut ConfigDef) -> Result<(), KafkaError> {
    let any_list_no_null: Arc<dyn Validator> = Arc::new(ValidList::any_non_duplicate_values(true, false));

    def.define(
        SSL_PROTOCOL_CONFIG,
        Type::String,
        Some(ConfigValue::String(DEFAULT_SSL_PROTOCOL.to_owned())),
        None,
        Importance::Medium,
        SSL_PROTOCOL_DOC,
    )?
    .define(
        SSL_PROVIDER_CONFIG,
        Type::String,
        Some(ConfigValue::Null),
        None,
        Importance::Medium,
        SSL_PROVIDER_DOC,
    )?
    .define(
        SSL_CIPHER_SUITES_CONFIG,
        Type::List,
        Some(ConfigValue::List(Vec::new())),
        Some(any_list_no_null.clone()),
        Importance::Low,
        SSL_CIPHER_SUITES_DOC,
    )?
    .define(
        SSL_ENABLED_PROTOCOLS_CONFIG,
        Type::List,
        Some(ConfigValue::List(
            DEFAULT_SSL_ENABLED_PROTOCOLS.split(',').map(str::to_owned).collect(),
        )),
        Some(any_list_no_null),
        Importance::Medium,
        SSL_ENABLED_PROTOCOLS_DOC,
    )?
    .define(
        SSL_KEYSTORE_TYPE_CONFIG,
        Type::String,
        Some(ConfigValue::String(DEFAULT_SSL_KEYSTORE_TYPE.to_owned())),
        None,
        Importance::Medium,
        SSL_KEYSTORE_TYPE_DOC,
    )?
    .define(
        SSL_KEYSTORE_LOCATION_CONFIG,
        Type::String,
        Some(ConfigValue::Null),
        None,
        Importance::High,
        SSL_KEYSTORE_LOCATION_DOC,
    )?
    .define(
        SSL_KEYSTORE_PASSWORD_CONFIG,
        Type::Password,
        Some(ConfigValue::Null),
        None,
        Importance::High,
        SSL_KEYSTORE_PASSWORD_DOC,
    )?
    .define(
        SSL_KEY_PASSWORD_CONFIG,
        Type::Password,
        Some(ConfigValue::Null),
        None,
        Importance::High,
        SSL_KEY_PASSWORD_DOC,
    )?
    .define(
        SSL_KEYSTORE_KEY_CONFIG,
        Type::Password,
        Some(ConfigValue::Null),
        None,
        Importance::High,
        SSL_KEYSTORE_KEY_DOC,
    )?
    .define(
        SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG,
        Type::Password,
        Some(ConfigValue::Null),
        None,
        Importance::High,
        SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC,
    )?
    .define(
        SSL_TRUSTSTORE_CERTIFICATES_CONFIG,
        Type::Password,
        Some(ConfigValue::Null),
        None,
        Importance::High,
        SSL_TRUSTSTORE_CERTIFICATES_DOC,
    )?
    .define(
        SSL_TRUSTSTORE_TYPE_CONFIG,
        Type::String,
        Some(ConfigValue::String(DEFAULT_SSL_TRUSTSTORE_TYPE.to_owned())),
        None,
        Importance::Medium,
        SSL_TRUSTSTORE_TYPE_DOC,
    )?
    .define(
        SSL_TRUSTSTORE_LOCATION_CONFIG,
        Type::String,
        Some(ConfigValue::Null),
        None,
        Importance::High,
        SSL_TRUSTSTORE_LOCATION_DOC,
    )?
    .define(
        SSL_TRUSTSTORE_PASSWORD_CONFIG,
        Type::Password,
        Some(ConfigValue::Null),
        None,
        Importance::High,
        SSL_TRUSTSTORE_PASSWORD_DOC,
    )?
    .define(
        SSL_KEYMANAGER_ALGORITHM_CONFIG,
        Type::String,
        Some(ConfigValue::Null),
        None,
        Importance::Low,
        SSL_KEYMANAGER_ALGORITHM_DOC,
    )?
    .define(
        SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
        Type::String,
        Some(ConfigValue::Null),
        None,
        Importance::Low,
        SSL_TRUSTMANAGER_ALGORITHM_DOC,
    )?
    .define(
        SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
        Type::String,
        Some(ConfigValue::String(DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM.to_owned())),
        None,
        Importance::Low,
        SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC,
    )?
    .define(
        SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG,
        Type::String,
        Some(ConfigValue::Null),
        None,
        Importance::Low,
        SSL_SECURE_RANDOM_IMPLEMENTATION_DOC,
    )?
    .define(
        SSL_ENGINE_FACTORY_CLASS_CONFIG,
        Type::Class,
        Some(ConfigValue::Null),
        None,
        Importance::Low,
        SSL_ENGINE_FACTORY_CLASS_DOC,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_client_ssl_support_registers_all_keys() {
        let mut def = ConfigDef::new();
        add_client_ssl_support(&mut def).unwrap();
        for key in [
            SSL_PROTOCOL_CONFIG,
            SSL_PROVIDER_CONFIG,
            SSL_CIPHER_SUITES_CONFIG,
            SSL_ENABLED_PROTOCOLS_CONFIG,
            SSL_KEYSTORE_TYPE_CONFIG,
            SSL_KEYSTORE_LOCATION_CONFIG,
            SSL_KEYSTORE_PASSWORD_CONFIG,
            SSL_KEY_PASSWORD_CONFIG,
            SSL_KEYSTORE_KEY_CONFIG,
            SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG,
            SSL_TRUSTSTORE_CERTIFICATES_CONFIG,
            SSL_TRUSTSTORE_TYPE_CONFIG,
            SSL_TRUSTSTORE_LOCATION_CONFIG,
            SSL_TRUSTSTORE_PASSWORD_CONFIG,
            SSL_KEYMANAGER_ALGORITHM_CONFIG,
            SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
            SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
            SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG,
            SSL_ENGINE_FACTORY_CLASS_CONFIG,
        ] {
            assert!(def.config_key(key).is_some(), "missing SSL key: {key}");
        }
    }

    #[test]
    fn add_client_ssl_support_default_protocol() {
        let mut def = ConfigDef::new();
        add_client_ssl_support(&mut def).unwrap();
        let key = def.config_key(SSL_PROTOCOL_CONFIG).unwrap();
        assert_eq!(
            key.default_value.as_ref().and_then(ConfigValue::as_str),
            Some(DEFAULT_SSL_PROTOCOL),
        );
    }
}
