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

//! SASL channel builder that creates channels with SASL authentication.
//!
//! Translated from `org.apache.kafka.common.network.SaslChannelBuilder`.
//!
//! In Java, `SaslChannelBuilder` creates a `KafkaChannel` with either a
//! `PlaintextTransportLayer` (for `SASL_PLAINTEXT`) or `SslTransportLayer`
//! (for `SASL_SSL`), paired with a `SaslClientAuthenticator`.
//!
//! This is a simplified client-only translation that supports the PLAIN
//! mechanism only; the client configurations reject any other `sasl.mechanism`
//! when they are built (`CommonClientConfigs::post_validate_supported_sasl_mechanism`).
//! Server-side logic (JAAS contexts, callback handlers, login managers) is omitted.

use super::ChannelBuilder;
use super::ChannelMetadataRegistry;
use super::KafkaChannel;
use super::ListenerName;
use super::PlaintextTransportLayer;
use super::SslTransportLayer;

use crate::common::config::SaslConfigs;
use crate::common::config::types::Password;
use crate::common::security::auth::SecurityProtocol;
use crate::common::security::authenticator::SaslClientAuthenticator;
use crate::common::security::ssl::SslFactory;
use crate::common::utils::SystemTime;
use crate::common::utils::internals::LogContext;

use std::io;
use std::sync::Arc;

use tokio::net::TcpStream;

/// SASL channel builder that creates channels with SASL authentication.
///
/// For `SASL_PLAINTEXT`, creates a `PlaintextTransportLayer` paired with a
/// `SaslClientAuthenticator`.
/// For `SASL_SSL`, creates an `SslTransportLayer` paired with a
/// `SaslClientAuthenticator`.
///
/// Translated from `org.apache.kafka.common.network.SaslChannelBuilder`.
impl std::fmt::Debug for SaslChannelBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaslChannelBuilder")
            .field("security_protocol", &self.security_protocol)
            .field("client_sasl_mechanism", &self.client_sasl_mechanism)
            .field("ssl_factory", &self.ssl_factory.is_some())
            .field("client_id", &self.client_id)
            .finish_non_exhaustive()
    }
}

#[doc(alias = "org.apache.kafka.common.network.SaslChannelBuilder")]
pub struct SaslChannelBuilder {
    /// The security protocol (SASL_PLAINTEXT or SASL_SSL).
    security_protocol: SecurityProtocol,
    /// The client's SASL mechanism (Java's `clientSaslMechanism`).
    client_sasl_mechanism: String,
    /// The username the authenticator sends, resolved from the SASL
    /// configuration when the builder was created.
    username: String,
    /// The password the authenticator sends, resolved likewise.
    password: Password,
    /// SSL factory for SASL_SSL connections.
    ssl_factory: Option<SslFactory>,
    /// The listener name, if any (server-side only).
    #[expect(dead_code)]
    listener_name: Option<ListenerName>,
    /// The Kafka client ID for request headers.
    client_id: String,
    /// Contextual log prefix forwarded to the authenticator.
    log_context: LogContext,
}

impl SaslChannelBuilder {
    /// Creates a new `SaslChannelBuilder`.
    ///
    /// # Arguments
    ///
    /// * `security_protocol` - Must be `SASL_PLAINTEXT` or `SASL_SSL`
    /// * `sasl_config` - SASL configuration (mechanism, username, password),
    ///   read here: the builder keeps the mechanism and the credentials
    /// * `ssl_factory` - Required for `SASL_SSL`, `None` for `SASL_PLAINTEXT`
    /// * `listener_name` - The listener name (server-side only, `None` for clients)
    /// * `client_id` - The Kafka client ID
    /// * `log_context` - Contextual log prefix
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The security protocol is not `SASL_PLAINTEXT` or `SASL_SSL`
    /// - The SASL mechanism is not `PLAIN`
    /// - Username or password is missing in the SASL config
    /// - `SASL_SSL` is requested but no `ssl_factory` is provided
    #[doc(alias = "org.apache.kafka.common.network.SaslChannelBuilder#SaslChannelBuilder")]
    pub fn new(
        security_protocol: SecurityProtocol,
        sasl_config: &SaslConfigs,
        ssl_factory: Option<SslFactory>,
        listener_name: Option<ListenerName>,
        client_id: &str,
        log_context: LogContext,
    ) -> io::Result<Self> {
        // Validate security protocol
        if security_protocol != SecurityProtocol::SaslPlaintext && security_protocol != SecurityProtocol::SaslSsl {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "SaslChannelBuilder requires SASL_PLAINTEXT or SASL_SSL, got {:?}",
                    security_protocol
                ),
            ));
        }

        // The authenticator builds only the PLAIN token (RFC 4616). Java picks a
        // `SaslClient` per mechanism; without SCRAM, GSSAPI and OAUTHBEARER clients, running
        // them through PLAIN would send the cleartext password under the wrong mechanism.
        if sasl_config.mechanism != SaslConfigs::PLAIN_MECHANISM {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "SASL mechanism '{}' is not supported by this client: only {} is implemented",
                    sasl_config.mechanism,
                    SaslConfigs::PLAIN_MECHANISM
                ),
            ));
        }

        // Validate credentials
        let Some(username) = sasl_config.resolve_username() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SASL PLAIN authentication requires a username",
            ));
        };
        let Some(password) = sasl_config.resolve_password() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SASL PLAIN authentication requires a password",
            ));
        };

        // Validate SSL factory for SASL_SSL
        if security_protocol == SecurityProtocol::SaslSsl && ssl_factory.is_none() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "SASL_SSL requires an SslFactory"));
        }

        Ok(Self {
            security_protocol,
            client_sasl_mechanism: sasl_config.mechanism.clone(),
            username: username.to_string(),
            password: Password::new(password),
            ssl_factory,
            listener_name,
            client_id: client_id.to_string(),
            log_context,
        })
    }
}

impl ChannelBuilder for SaslChannelBuilder {
    fn build_channel(
        &self,
        id: &str,
        stream: TcpStream,
        peer_host: &str,
        max_receive_size: i32,
        metadata_registry: Box<dyn ChannelMetadataRegistry>,
    ) -> io::Result<KafkaChannel> {
        let transport_layer: Box<dyn crate::common::network::TransportLayer> =
            if self.security_protocol == SecurityProtocol::SaslSsl {
                let ssl_factory = self.ssl_factory.as_ref().unwrap();
                let domain = SslFactory::create_server_name(peer_host)?;
                let conn = rustls::ClientConnection::new(Arc::clone(ssl_factory.client_config()), domain.clone())
                    .map_err(|e| io::Error::other(format!("Failed to construct TLS client connection: {e}")))?;
                Box::new(SslTransportLayer::new(stream, conn, domain))
            } else {
                Box::new(PlaintextTransportLayer::new(stream))
            };

        let authenticator = Box::new(SaslClientAuthenticator::new(
            &self.client_sasl_mechanism,
            &self.username,
            self.password.value(),
            id,
            peer_host,
            &self.client_id,
            // Java passes the client's `Time`. The selector's clock, against
            // which the channel checks the re-authentication time this computes,
            // is `SystemTime` too, and every `SystemTime` shares one monotonic
            // origin.
            Arc::new(SystemTime),
            self.log_context.clone(),
        ));

        Ok(KafkaChannel::new(
            id,
            transport_layer,
            authenticator,
            max_receive_size,
            metadata_registry,
        ))
    }

    fn close(&mut self) {
        // no-op — resources are owned and will be dropped naturally
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::config::SslConfigs;
    use crate::common::config::types::Password;
    use crate::common::network::DefaultChannelMetadataRegistry;

    /// Test 1: Build channel with SASL_PLAINTEXT creates a channel that is not ready.
    #[tokio::test]
    async fn test_build_channel_sasl_plaintext() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let stream = TcpStream::connect(addr).await.unwrap();

        let sasl_config = SaslConfigs {
            mechanism: "PLAIN".to_string(),
            username: Some("alice".to_string()),
            password: Some(Password::new("secret")),
            ..SaslConfigs::default()
        };
        let builder = SaslChannelBuilder::new(
            SecurityProtocol::SaslPlaintext,
            &sasl_config,
            None,
            None,
            "test-client",
            LogContext::empty(),
        )
        .unwrap();
        let metadata_registry = Box::new(DefaultChannelMetadataRegistry::new());

        let channel = builder
            .build_channel("test-0", stream, "broker1", 1024 * 1024, metadata_registry)
            .unwrap();

        // Channel should not be ready because SASL authentication hasn't happened
        assert!(!channel.ready());
        assert_eq!(channel.id(), "test-0");
    }

    /// Test 2: Build channel with SASL_SSL creates a channel with SSL transport.
    #[tokio::test]
    async fn test_build_channel_sasl_ssl() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let stream = TcpStream::connect(addr).await.unwrap();

        let sasl_config = SaslConfigs {
            mechanism: "PLAIN".to_string(),
            username: Some("alice".to_string()),
            password: Some(Password::new("secret")),
            ..SaslConfigs::default()
        };
        let ssl_factory = SslFactory::new(&SslConfigs::default()).unwrap();
        let builder = SaslChannelBuilder::new(
            SecurityProtocol::SaslSsl,
            &sasl_config,
            Some(ssl_factory),
            None,
            "test-client",
            LogContext::empty(),
        )
        .unwrap();
        let metadata_registry = Box::new(DefaultChannelMetadataRegistry::new());

        let channel = builder
            .build_channel("test-0", stream, "localhost", 1024 * 1024, metadata_registry)
            .unwrap();

        // Channel should not be ready because neither TLS handshake nor SASL auth happened
        assert!(!channel.ready());
        assert_eq!(channel.id(), "test-0");
    }

    /// Test 3: Missing credentials should return an error.
    #[test]
    fn test_missing_credentials_error() {
        // Missing username
        let sasl_config = SaslConfigs {
            mechanism: "PLAIN".to_string(),
            password: Some(Password::new("secret")),
            ..SaslConfigs::default()
        };
        let result = SaslChannelBuilder::new(
            SecurityProtocol::SaslPlaintext,
            &sasl_config,
            None,
            None,
            "test-client",
            LogContext::empty(),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("username"));

        // Missing password
        let sasl_config = SaslConfigs {
            mechanism: "PLAIN".to_string(),
            username: Some("alice".to_string()),
            ..SaslConfigs::default()
        };
        let result = SaslChannelBuilder::new(
            SecurityProtocol::SaslPlaintext,
            &sasl_config,
            None,
            None,
            "test-client",
            LogContext::empty(),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("password"));
    }

    /// Test 4: Invalid security protocol should return an error.
    #[test]
    fn test_invalid_security_protocol() {
        let sasl_config = SaslConfigs {
            mechanism: "PLAIN".to_string(),
            username: Some("alice".to_string()),
            password: Some(Password::new("secret")),
            ..SaslConfigs::default()
        };
        let result = SaslChannelBuilder::new(
            SecurityProtocol::Plaintext,
            &sasl_config,
            None,
            None,
            "test-client",
            LogContext::empty(),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("SASL_PLAINTEXT or SASL_SSL"));
    }

    /// Regression test: the authenticator only implements PLAIN, so any other
    /// `sasl.mechanism` (including the `GSSAPI` default) must be rejected up
    /// front instead of sending the cleartext PLAIN token under a SCRAM, GSSAPI
    /// or OAUTHBEARER handshake.
    #[test]
    fn test_unsupported_sasl_mechanism_is_rejected() {
        for mechanism in ["SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI", "OAUTHBEARER", "plain", ""] {
            let sasl_config = SaslConfigs {
                mechanism: mechanism.to_string(),
                username: Some("alice".to_string()),
                password: Some(Password::new("alice-secret")),
                ..SaslConfigs::default()
            };
            let result = SaslChannelBuilder::new(
                SecurityProtocol::SaslPlaintext,
                &sasl_config,
                None,
                None,
                "test-client",
                LogContext::empty(),
            );
            let message = result
                .err()
                .unwrap_or_else(|| panic!("{mechanism:?} must be rejected"))
                .to_string();
            assert_eq!(
                format!("SASL mechanism '{mechanism}' is not supported by this client: only PLAIN is implemented"),
                message
            );
            assert!(!message.contains("alice-secret"));
        }
    }

    /// The default `sasl.mechanism` is `GSSAPI` (as in Java), so a config that
    /// forgets `sasl.mechanism=PLAIN` fails instead of leaking its password.
    #[test]
    fn test_default_sasl_mechanism_is_rejected() {
        let sasl_config = SaslConfigs {
            username: Some("alice".to_string()),
            password: Some(Password::new("alice-secret")),
            ..SaslConfigs::default()
        };
        let result = SaslChannelBuilder::new(
            SecurityProtocol::SaslSsl,
            &sasl_config,
            Some(SslFactory::new(&SslConfigs::default()).unwrap()),
            None,
            "test-client",
            LogContext::empty(),
        );
        assert!(result.err().unwrap().to_string().contains("'GSSAPI' is not supported"));
    }

    /// `{:?}` of a builder holding credentials renders neither the password
    /// nor the JAAS string: the hand-written `Debug` lists only the
    /// non-secret fields.
    #[test]
    fn test_debug_hides_credentials() {
        let jaas = "org.apache.kafka.common.security.plain.PlainLoginModule required \
                    username=\"jaas-user\" password=\"jaas-S3cr3t\";";
        let sasl_config = SaslConfigs {
            mechanism: "PLAIN".to_string(),
            jaas_config: Some(Password::new(jaas)),
            username: Some("alice".to_string()),
            password: Some(Password::new("direct-S3cr3t")),
        };
        let builder = SaslChannelBuilder::new(
            SecurityProtocol::SaslPlaintext,
            &sasl_config,
            None,
            None,
            "visible-client",
            LogContext::empty(),
        )
        .unwrap();

        for rendered in [format!("{builder:?}"), format!("{builder:#?}")] {
            for secret in [jaas, "jaas-S3cr3t", "direct-S3cr3t", "PlainLoginModule", "S3cr3t"] {
                assert!(!rendered.contains(secret), "{secret:?} leaked: {rendered}");
            }
            assert!(rendered.contains("\"PLAIN\""), "{rendered}");
            assert!(rendered.contains("\"visible-client\""), "{rendered}");
        }
    }
}
