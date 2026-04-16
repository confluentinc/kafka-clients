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
//! This is a simplified client-only translation that supports PLAIN mechanism.
//! Server-side logic (JAAS contexts, callback handlers, login managers) is omitted.

use super::channel_builder::ChannelBuilder;
use super::channel_metadata_registry::ChannelMetadataRegistry;
use super::kafka_channel::KafkaChannel;
use super::listener_name::ListenerName;
use super::plaintext_transport_layer::PlaintextTransportLayer;
use super::ssl_transport_layer::SslTransportLayer;

use crate::common::config::SaslConfig;
use crate::common::security::auth::SecurityProtocol;
use crate::common::security::authenticator::SaslClientAuthenticator;
use crate::common::security::ssl::SslFactory;

use std::io;

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
            .field("mechanism", &self.sasl_config.mechanism)
            .field("ssl_factory", &self.ssl_factory.is_some())
            .field("client_id", &self.client_id)
            .finish_non_exhaustive()
    }
}

pub struct SaslChannelBuilder {
    /// The security protocol (SASL_PLAINTEXT or SASL_SSL).
    security_protocol: SecurityProtocol,
    /// SASL configuration (mechanism, credentials).
    sasl_config: SaslConfig,
    /// SSL factory for SASL_SSL connections.
    ssl_factory: Option<SslFactory>,
    /// The listener name, if any (server-side only).
    #[allow(dead_code)]
    listener_name: Option<ListenerName>,
    /// The Kafka client ID for request headers.
    client_id: String,
}

impl SaslChannelBuilder {
    /// Creates a new `SaslChannelBuilder`.
    ///
    /// # Arguments
    ///
    /// * `security_protocol` - Must be `SASL_PLAINTEXT` or `SASL_SSL`
    /// * `sasl_config` - SASL configuration (mechanism, username, password)
    /// * `ssl_factory` - Required for `SASL_SSL`, `None` for `SASL_PLAINTEXT`
    /// * `listener_name` - The listener name (server-side only, `None` for clients)
    /// * `client_id` - The Kafka client ID
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The security protocol is not `SASL_PLAINTEXT` or `SASL_SSL`
    /// - Username or password is missing in the SASL config
    /// - `SASL_SSL` is requested but no `ssl_factory` is provided
    pub fn new(
        security_protocol: SecurityProtocol,
        sasl_config: SaslConfig,
        ssl_factory: Option<SslFactory>,
        listener_name: Option<ListenerName>,
        client_id: &str,
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

        // Validate credentials
        if sasl_config.resolve_username().is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SASL PLAIN authentication requires a username",
            ));
        }
        if sasl_config.resolve_password().is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SASL PLAIN authentication requires a password",
            ));
        }

        // Validate SSL factory for SASL_SSL
        if security_protocol == SecurityProtocol::SaslSsl && ssl_factory.is_none() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "SASL_SSL requires an SslFactory"));
        }

        Ok(Self {
            security_protocol,
            sasl_config,
            ssl_factory,
            listener_name,
            client_id: client_id.to_string(),
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
        let transport_layer: Box<dyn crate::common::network::transport_layer::TransportLayer> =
            if self.security_protocol == SecurityProtocol::SaslSsl {
                let ssl_factory = self.ssl_factory.as_ref().unwrap();
                let connector = ssl_factory.create_tls_connector();
                let domain = SslFactory::create_server_name(peer_host)?;
                Box::new(SslTransportLayer::new(stream, connector, domain))
            } else {
                Box::new(PlaintextTransportLayer::new(stream))
            };

        let username = self.sasl_config.resolve_username().unwrap();
        let password = self.sasl_config.resolve_password().unwrap();

        let authenticator = Box::new(SaslClientAuthenticator::new(
            &self.sasl_config.mechanism,
            username,
            password,
            id,
            peer_host,
            &self.client_id,
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
    use crate::common::config::SslConfig;
    use crate::common::network::channel_metadata_registry::DefaultChannelMetadataRegistry;

    /// Test 1: Build channel with SASL_PLAINTEXT creates a channel that is not ready.
    #[tokio::test]
    async fn test_build_channel_sasl_plaintext() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let stream = TcpStream::connect(addr).await.unwrap();

        let sasl_config = SaslConfig {
            mechanism: "PLAIN".to_string(),
            username: Some("alice".to_string()),
            password: Some("secret".to_string()),
            ..SaslConfig::default()
        };
        let builder =
            SaslChannelBuilder::new(SecurityProtocol::SaslPlaintext, sasl_config, None, None, "test-client").unwrap();
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

        let sasl_config = SaslConfig {
            mechanism: "PLAIN".to_string(),
            username: Some("alice".to_string()),
            password: Some("secret".to_string()),
            ..SaslConfig::default()
        };
        let ssl_factory = SslFactory::new(&SslConfig::default()).unwrap();
        let builder =
            SaslChannelBuilder::new(SecurityProtocol::SaslSsl, sasl_config, Some(ssl_factory), None, "test-client")
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
        let sasl_config = SaslConfig {
            mechanism: "PLAIN".to_string(),
            password: Some("secret".to_string()),
            ..SaslConfig::default()
        };
        let result = SaslChannelBuilder::new(SecurityProtocol::SaslPlaintext, sasl_config, None, None, "test-client");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("username"));

        // Missing password
        let sasl_config = SaslConfig {
            mechanism: "PLAIN".to_string(),
            username: Some("alice".to_string()),
            ..SaslConfig::default()
        };
        let result = SaslChannelBuilder::new(SecurityProtocol::SaslPlaintext, sasl_config, None, None, "test-client");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("password"));
    }

    /// Test 4: Invalid security protocol should return an error.
    #[test]
    fn test_invalid_security_protocol() {
        let sasl_config = SaslConfig {
            mechanism: "PLAIN".to_string(),
            username: Some("alice".to_string()),
            password: Some("secret".to_string()),
            ..SaslConfig::default()
        };
        let result = SaslChannelBuilder::new(SecurityProtocol::Plaintext, sasl_config, None, None, "test-client");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("SASL_PLAINTEXT or SASL_SSL"));
    }
}
