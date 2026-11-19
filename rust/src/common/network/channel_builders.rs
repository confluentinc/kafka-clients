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

//! Factory for creating the appropriate `ChannelBuilder` based on `SecurityProtocol`.
//!
//! Translated from `org.apache.kafka.common.network.ChannelBuilders.clientChannelBuilder()`
//! and the private `create()` method (Java).
//!
//! In Java, `ChannelBuilders` switches on `SecurityProtocol` to instantiate the
//! correct builder. In Rust, the channel builders already accept typed config
//! structs in their constructors, so there is no separate `configure()` step.
//!
//! Excluded Java code:
//! - `channelBuilderConfigs()` -- Java's config extraction from `AbstractConfig` is not applicable
//! - `serverChannelBuilder()` -- server-side, out of scope
//! - `createPrincipalBuilder()` -- server-side, out of scope
//! - `requireNonNullMode()` -- replaced by Rust exhaustive match

use std::io;

use crate::common::Error;
use crate::common::config::{SaslConfigs, SslConfigs};
use crate::common::network::ChannelBuilder;
use crate::common::network::ListenerName;
use crate::common::network::PlaintextChannelBuilder;
use crate::common::network::SaslChannelBuilder;
#[cfg(feature = "rustls")]
use crate::common::network::SslChannelBuilder;
use crate::common::security::JaasContext;
use crate::common::security::auth::SecurityProtocol;
#[cfg(feature = "rustls")]
use crate::common::security::ssl::SslFactory;
use crate::common::utils::internals::LogContext;

/// Translates the Java static-utility class `org.apache.kafka.common.network.ChannelBuilders`,
/// which has no instance state, so it becomes a unit struct hosting its
/// statics as associated items.
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.common.network.ChannelBuilders")]
pub struct ChannelBuilders;

impl ChannelBuilders {
    /// Creates a client-side `ChannelBuilder` for the given security protocol.
    ///
    /// Translated from `ChannelBuilders.clientChannelBuilder()` (Java). For
    /// `SASL_PLAINTEXT` and `SASL_SSL` it first loads the client's JAAS context
    /// from `sasl.jaas.config`, as Java's `ChannelBuilders#create` does before
    /// it builds the `SaslChannelBuilder` (whose `configure` then builds the
    /// `SslFactory` of `SASL_SSL`), so a value Java rejects fails here, and
    /// only for those protocols. (Without the `rustls` feature, `SSL` and
    /// `SASL_SSL` are refused before any configuration is read.)
    ///
    /// # Arguments
    ///
    /// * `security_protocol` - The security protocol to use
    /// * `ssl_config` - Required for `SSL` and `SASL_SSL` protocols
    /// * `sasl_config` - Required for `SASL_PLAINTEXT` and `SASL_SSL` protocols
    /// * `listener_name` - Optional listener name (server-side only, `None` for clients)
    /// * `client_id` - The Kafka client ID
    /// * `log_context` - Contextual log prefix
    ///
    /// # Errors
    ///
    /// - [`Error::is_kafka_error`] `Failed to create new NetworkClient`, caused
    ///   by Java's `IllegalArgumentException`
    ///   ([`Error::is_local_illegal_argument_error`]) when `sasl.jaas.config`
    ///   is not a valid JAAS configuration of one allowed login module
    ///   ([`JaasContext::load_client_context`]). The clients create their
    ///   channel builder where Java's `ClientUtils#createNetworkClient` does,
    ///   which wraps the failure in that `KafkaException`, so the clients'
    ///   constructors report Java's chain.
    /// - [`Error::is_config_error`] `Failed to create channel builder:
    ///   <reason>` for any other failure: a missing sub-configuration, an SSL
    ///   configuration the `SslFactory` rejects, a SASL mechanism other than
    ///   PLAIN or missing PLAIN credentials (see [`SaslChannelBuilder::new`]),
    ///   or a TLS protocol without the `rustls` feature.
    #[doc(alias = "org.apache.kafka.common.network.ChannelBuilders#clientChannelBuilder")]
    pub fn client_channel_builder(
        security_protocol: SecurityProtocol,
        ssl_config: Option<&SslConfigs>,
        sasl_config: Option<&SaslConfigs>,
        listener_name: Option<ListenerName>,
        client_id: &str,
        log_context: LogContext,
    ) -> Result<Box<dyn ChannelBuilder>, Error> {
        match security_protocol {
            SecurityProtocol::Plaintext => Ok(Box::new(PlaintextChannelBuilder::new(listener_name))),
            #[cfg(feature = "rustls")]
            SecurityProtocol::Ssl => {
                let ssl_config = ssl_config.ok_or_else(|| Self::invalid_input("SSL protocol requires ssl_config"))?;
                let ssl_factory = SslFactory::new(ssl_config).map_err(Self::config_error)?;
                Ok(Box::new(SslChannelBuilder::new(ssl_factory, listener_name)))
            },
            SecurityProtocol::SaslPlaintext => {
                let sasl_config =
                    sasl_config.ok_or_else(|| Self::invalid_input("SASL_PLAINTEXT protocol requires sasl_config"))?;
                let jaas_context = Self::load_client_context(sasl_config)?;
                let channel_builder = SaslChannelBuilder::new(
                    SecurityProtocol::SaslPlaintext,
                    sasl_config,
                    jaas_context.as_ref(),
                    None,
                    listener_name,
                    client_id,
                    log_context,
                )
                .map_err(Self::config_error)?;
                Ok(Box::new(channel_builder))
            },
            #[cfg(feature = "rustls")]
            SecurityProtocol::SaslSsl => {
                let ssl_config =
                    ssl_config.ok_or_else(|| Self::invalid_input("SASL_SSL protocol requires ssl_config"))?;
                let sasl_config =
                    sasl_config.ok_or_else(|| Self::invalid_input("SASL_SSL protocol requires sasl_config"))?;
                let jaas_context = Self::load_client_context(sasl_config)?;
                let ssl_factory = SslFactory::new(ssl_config).map_err(Self::config_error)?;
                let channel_builder = SaslChannelBuilder::new(
                    SecurityProtocol::SaslSsl,
                    sasl_config,
                    jaas_context.as_ref(),
                    Some(ssl_factory),
                    listener_name,
                    client_id,
                    log_context,
                )
                .map_err(Self::config_error)?;
                Ok(Box::new(channel_builder))
            },
            #[cfg(not(feature = "rustls"))]
            SecurityProtocol::Ssl | SecurityProtocol::SaslSsl => {
                let _ = ssl_config;
                Err(Self::config_error(Self::tls_disabled_error(security_protocol)))
            },
        }
    }

    /// The client's JAAS context ([`JaasContext::load_client_context`]), its
    /// failure wrapped as Java's `ClientUtils#createNetworkClient` wraps it.
    fn load_client_context(sasl_config: &SaslConfigs) -> Result<Option<JaasContext>, Error> {
        JaasContext::load_client_context(sasl_config)
            .map_err(|e| Error::kafka_message_source("Failed to create new NetworkClient", e))
    }

    /// A channel builder failure other than the JAAS context's, as the
    /// clients have reported it.
    fn config_error(e: io::Error) -> Error {
        Error::config_message(format!("Failed to create channel builder: {e}"))
    }

    /// The error of a missing sub-configuration.
    fn invalid_input(message: &str) -> Error {
        Self::config_error(io::Error::new(io::ErrorKind::InvalidInput, message))
    }

    /// The error a TLS security protocol fails with on a client built without
    /// the `rustls` feature.
    #[cfg(not(feature = "rustls"))]
    pub(crate) fn tls_disabled_error(security_protocol: SecurityProtocol) -> io::Error {
        io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "Security protocol {} is not available: the client was built without the `rustls` feature",
                security_protocol.name()
            ),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::config::types::Password;

    #[test]
    fn test_plaintext_builder() {
        let result = ChannelBuilders::client_channel_builder(
            SecurityProtocol::Plaintext,
            None,
            None,
            None,
            "test",
            LogContext::empty(),
        );
        assert!(result.is_ok(), "Plaintext should not require any configs");
    }

    #[cfg(feature = "rustls")]
    #[test]
    fn test_ssl_builder() {
        let ssl_config = SslConfigs::default();
        let result = ChannelBuilders::client_channel_builder(
            SecurityProtocol::Ssl,
            Some(&ssl_config),
            None,
            None,
            "test",
            LogContext::empty(),
        );
        assert!(result.is_ok(), "SSL with valid config should succeed");
    }

    #[cfg(feature = "rustls")]
    #[test]
    fn test_ssl_missing_config() {
        let result = ChannelBuilders::client_channel_builder(
            SecurityProtocol::Ssl,
            None,
            None,
            None,
            "test",
            LogContext::empty(),
        );
        let err = result.err().expect("Should return an error");
        assert!(
            err.to_string().contains("ssl_config"),
            "Error should mention ssl_config: {}",
            err
        );
    }

    #[test]
    fn test_sasl_plaintext_builder() {
        let sasl_config = SaslConfigs {
            mechanism: "PLAIN".to_string(),
            username: Some("alice".to_string()),
            password: Some(Password::new("secret")),
            ..SaslConfigs::default()
        };
        let result = ChannelBuilders::client_channel_builder(
            SecurityProtocol::SaslPlaintext,
            None,
            Some(&sasl_config),
            None,
            "test",
            LogContext::empty(),
        );
        assert!(result.is_ok(), "SASL_PLAINTEXT with valid config should succeed");
    }

    #[cfg(feature = "rustls")]
    #[test]
    fn test_sasl_ssl_builder() {
        let ssl_config = SslConfigs::default();
        let sasl_config = SaslConfigs {
            mechanism: "PLAIN".to_string(),
            username: Some("alice".to_string()),
            password: Some(Password::new("secret")),
            ..SaslConfigs::default()
        };
        let result = ChannelBuilders::client_channel_builder(
            SecurityProtocol::SaslSsl,
            Some(&ssl_config),
            Some(&sasl_config),
            None,
            "test",
            LogContext::empty(),
        );
        assert!(result.is_ok(), "SASL_SSL with both configs should succeed");
    }

    #[cfg(feature = "rustls")]
    #[test]
    fn test_sasl_ssl_missing_ssl_config() {
        let sasl_config = SaslConfigs {
            mechanism: "PLAIN".to_string(),
            username: Some("alice".to_string()),
            password: Some(Password::new("secret")),
            ..SaslConfigs::default()
        };
        let result = ChannelBuilders::client_channel_builder(
            SecurityProtocol::SaslSsl,
            None,
            Some(&sasl_config),
            None,
            "test",
            LogContext::empty(),
        );
        let err = result.err().expect("Should return an error");
        assert!(
            err.to_string().contains("ssl_config"),
            "Error should mention ssl_config: {}",
            err
        );
    }

    #[test]
    fn test_sasl_plaintext_missing_sasl_config() {
        let result = ChannelBuilders::client_channel_builder(
            SecurityProtocol::SaslPlaintext,
            None,
            None,
            None,
            "test",
            LogContext::empty(),
        );
        let err = result.err().expect("Should return an error");
        assert!(
            err.to_string().contains("sasl_config"),
            "Error should mention sasl_config: {}",
            err
        );
    }

    #[cfg(not(feature = "rustls"))]
    #[test]
    fn test_tls_protocols_need_rustls() {
        let ssl_config = SslConfigs::default();
        let sasl_config = SaslConfigs {
            mechanism: "PLAIN".to_string(),
            username: Some("alice".to_string()),
            password: Some(Password::new("secret")),
            ..SaslConfigs::default()
        };
        for (protocol, name) in [(SecurityProtocol::Ssl, "SSL"), (SecurityProtocol::SaslSsl, "SASL_SSL")] {
            let err = ChannelBuilders::client_channel_builder(
                protocol,
                Some(&ssl_config),
                Some(&sasl_config),
                None,
                "test",
                LogContext::empty(),
            )
            .err()
            .expect("TLS needs the rustls feature");
            assert!(err.is_config_error(), "{err:?}");
            assert_eq!(
                err.message(),
                format!(
                    "Failed to create channel builder: Security protocol {name} is not available: the client was \
                     built without the `rustls` feature"
                )
            );
        }
    }

    /// Java's `ChannelBuilders#create` loads the client's JAAS context for the
    /// two SASL protocols, before `SASL_SSL`'s `SslFactory` is configured, and
    /// `ClientUtils#createNetworkClient` wraps its failure in
    /// `KafkaException("Failed to create new NetworkClient")`. Each message is
    /// Kafka 4.4.0's for that value.
    #[test]
    fn test_sasl_protocols_reject_the_jaas_config_java_rejects() {
        const PLAIN: &str = "org.apache.kafka.common.security.plain.PlainLoginModule";
        let cases = [
            (
                format!("{PLAIN} required username=\"a\" password=\"b\""),
                "JAAS config entry not terminated by semi-colon",
            ),
            (
                format!("{PLAIN} requiredX username=\"a\" password=\"b\";"),
                "Invalid login module control flag 'requiredX' in JAAS config",
            ),
            (
                format!("{PLAIN} username=\"a\" password=\"b\";"),
                "Invalid login module control flag 'username' in JAAS config",
            ),
            (String::new(), "Login module not specified in JAAS config"),
            (
                format!(
                    "{PLAIN} required username=\"a\" password=\"b\"; {PLAIN} required username=\"c\" password=\"d\";"
                ),
                "JAAS config property contains 2 login modules, should be 1 module",
            ),
        ];
        // An SSL configuration `SslFactory` rejects: the JAAS context fails first.
        let ssl_config = SslConfigs {
            truststore_location: Some("/nonexistent/truststore.pem".to_string()),
            ..SslConfigs::default()
        };
        for (jaas, message) in cases {
            let sasl_config = SaslConfigs {
                mechanism: "PLAIN".to_string(),
                jaas_config: Some(Password::new(jaas.clone())),
                ..SaslConfigs::default()
            };
            #[cfg(feature = "rustls")]
            let protocols = [SecurityProtocol::SaslPlaintext, SecurityProtocol::SaslSsl];
            // Without `rustls`, `SASL_SSL` is refused before its configs are read.
            #[cfg(not(feature = "rustls"))]
            let protocols = [SecurityProtocol::SaslPlaintext];
            for protocol in protocols {
                let err = ChannelBuilders::client_channel_builder(
                    protocol,
                    Some(&ssl_config),
                    Some(&sasl_config),
                    None,
                    "test",
                    LogContext::empty(),
                )
                .err()
                .unwrap_or_else(|| panic!("{protocol:?} {jaas:?} must be refused"));
                assert!(err.is_kafka_error(), "{err:?}");
                assert_eq!("Failed to create new NetworkClient", err.message());
                let cause = err.source().expect("the JAAS error is the cause");
                assert!(cause.is_local_illegal_argument_error(), "{cause:?}");
                assert_eq!(message, cause.message(), "{protocol:?} {jaas:?}");
            }
            // The other protocols never read `sasl.jaas.config`.
            ChannelBuilders::client_channel_builder(
                SecurityProtocol::Plaintext,
                Some(&ssl_config),
                Some(&sasl_config),
                None,
                "test",
                LogContext::empty(),
            )
            .unwrap();
        }
    }

    /// The credentials of a valid `sasl.jaas.config` reach the builder: its
    /// login module's `username` and `password` options.
    #[test]
    fn test_sasl_plaintext_builder_from_jaas_config() {
        let sasl_config = SaslConfigs {
            mechanism: "PLAIN".to_string(),
            jaas_config: Some(Password::new(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"alice\" password=\"secret\";",
            )),
            ..SaslConfigs::default()
        };
        ChannelBuilders::client_channel_builder(
            SecurityProtocol::SaslPlaintext,
            None,
            Some(&sasl_config),
            None,
            "test",
            LogContext::empty(),
        )
        .unwrap();

        // Without a password option the builder still refuses PLAIN, as before.
        let sasl_config = SaslConfigs {
            mechanism: "PLAIN".to_string(),
            jaas_config: Some(Password::new(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"alice\";",
            )),
            ..SaslConfigs::default()
        };
        let err = ChannelBuilders::client_channel_builder(
            SecurityProtocol::SaslPlaintext,
            None,
            Some(&sasl_config),
            None,
            "test",
            LogContext::empty(),
        )
        .err()
        .expect("no password");
        assert!(err.is_config_error(), "{err:?}");
        assert_eq!(
            "Failed to create channel builder: SASL PLAIN authentication requires a password",
            err.message()
        );
    }
}
