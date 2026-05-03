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

//! SSL channel builder that creates TLS-encrypted channels.
//!
//! Translated from `org.apache.kafka.common.network.SslChannelBuilder`.
//!
//! In Java, `SslChannelBuilder` creates an `SslTransportLayer` from a
//! `SelectionKey` and wraps it with a `PlaintextAuthenticator` in a `KafkaChannel`.
//! SSL authentication is performed at the transport layer level during the TLS
//! handshake, not via the `Authenticator` interface.
//!
//! In Rust, the builder receives a raw `TcpStream`, wraps it in an
//! `SslTransportLayer` (which handles the TLS handshake), and pairs it with a
//! `PlaintextAuthenticator`.

use super::ChannelBuilder;
use super::ChannelMetadataRegistry;
use super::KafkaChannel;
use super::ListenerName;
use super::PlaintextAuthenticator;
use super::SslTransportLayer;

use crate::common::security::SslFactory;

use std::io;
use std::sync::Arc;

use tokio::net::TcpStream;

/// SSL channel builder that creates TLS-encrypted channels.
///
/// Holds an `SslFactory` that provides the compiled TLS client configuration.
/// The `listener_name` is non-null when instantiated in the broker and `None`
/// otherwise (client mode).
///
/// Translated from `org.apache.kafka.common.network.SslChannelBuilder`.
pub struct SslChannelBuilder {
    /// SSL factory for creating TLS connectors.
    ssl_factory: SslFactory,
    /// The listener name, if any (server-side only).
    #[allow(dead_code)]
    listener_name: Option<ListenerName>,
}

impl SslChannelBuilder {
    /// Creates a new `SslChannelBuilder` with the given SSL factory.
    ///
    /// `listener_name` is `Some` when instantiated in the broker and `None` otherwise.
    pub fn new(ssl_factory: SslFactory, listener_name: Option<ListenerName>) -> Self {
        Self { ssl_factory, listener_name }
    }
}

impl ChannelBuilder for SslChannelBuilder {
    fn build_channel(
        &self,
        id: &str,
        stream: TcpStream,
        peer_host: &str,
        max_receive_size: i32,
        metadata_registry: Box<dyn ChannelMetadataRegistry>,
    ) -> io::Result<KafkaChannel> {
        let domain = SslFactory::create_server_name(peer_host)?;
        let conn = rustls::ClientConnection::new(Arc::clone(self.ssl_factory.client_config()), domain.clone())
            .map_err(|e| io::Error::other(format!("Failed to construct TLS client connection: {e}")))?;
        let transport_layer = Box::new(SslTransportLayer::new(stream, conn, domain));

        // SSL authentication happens during the TLS handshake (in the transport
        // layer), so we use a PlaintextAuthenticator — matching Java's
        // SslChannelBuilder which uses a no-op Authenticator.
        let authenticator = Box::new(PlaintextAuthenticator::new());

        Ok(KafkaChannel::new(
            id,
            transport_layer,
            authenticator,
            max_receive_size,
            metadata_registry,
        ))
    }

    fn close(&mut self) {
        // no-op — the SslFactory is owned and will be dropped naturally
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::config::SslConfig;
    use crate::common::network::DefaultChannelMetadataRegistry;

    /// Test that SslChannelBuilder creates a channel that is not immediately ready
    /// (TLS handshake has not been performed yet).
    #[tokio::test]
    async fn test_build_channel_not_ready() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let stream = TcpStream::connect(addr).await.unwrap();

        let ssl_factory = SslFactory::new(&SslConfig::default()).unwrap();
        let builder = SslChannelBuilder::new(ssl_factory, None);
        let metadata_registry = Box::new(DefaultChannelMetadataRegistry::new());

        let channel = builder
            .build_channel("test-0", stream, "localhost", 1024 * 1024, metadata_registry)
            .unwrap();

        // Channel should not be ready because TLS handshake hasn't happened
        assert!(!channel.ready());
        assert_eq!(channel.id(), "test-0");
    }

    /// Test that invalid peer_host returns an error.
    #[tokio::test]
    async fn test_build_channel_invalid_peer_host() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let stream = TcpStream::connect(addr).await.unwrap();

        let ssl_factory = SslFactory::new(&SslConfig::default()).unwrap();
        let builder = SslChannelBuilder::new(ssl_factory, None);
        let metadata_registry = Box::new(DefaultChannelMetadataRegistry::new());

        // Empty peer_host should produce an error
        let result = builder.build_channel("test-0", stream, "", 1024 * 1024, metadata_registry);
        assert!(result.is_err(), "Empty peer_host should produce an error");
    }
}
