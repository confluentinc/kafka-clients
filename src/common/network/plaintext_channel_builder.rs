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

//! Translation of `org.apache.kafka.common.network.PlaintextChannelBuilder`.

use std::sync::Arc;

use tokio::net::TcpStream;

use crate::common::errors::KafkaError;
use crate::common::network::authenticator::PlaintextAuthenticator;
use crate::common::network::channel_builder::ChannelBuilder;
use crate::common::network::kafka_channel::BoxedMetadataRegistry;
use crate::common::network::{KafkaChannel, ListenerName, PlaintextTransportLayer};

/// Builds [`KafkaChannel`]s wrapping a [`PlaintextTransportLayer`].
/// Mirrors Java's `PlaintextChannelBuilder`.
pub struct PlaintextChannelBuilder {
    /// Listener name. Non-`None` only when instantiated on the broker.
    /// Mirrors Java's nullable `ListenerName listenerName` field. The
    /// Phase 5b-3 producer always passes `None` (Java's `null`).
    listener_name: Option<ListenerName>,
}

impl PlaintextChannelBuilder {
    /// Constructs a plaintext channel builder. Mirrors Java's
    /// `PlaintextChannelBuilder(ListenerName)`.
    pub fn new(listener_name: Option<ListenerName>) -> Self {
        PlaintextChannelBuilder { listener_name }
    }

    /// Borrow the listener name configured on this builder. Mirrors
    /// Java's package-private field access.
    pub fn listener_name(&self) -> Option<&ListenerName> {
        self.listener_name.as_ref()
    }
}

impl ChannelBuilder for PlaintextChannelBuilder {
    fn build_channel(
        &self,
        id: Arc<str>,
        stream: TcpStream,
        max_receive_size: i32,
        metadata_registry: BoxedMetadataRegistry,
    ) -> Result<KafkaChannel, KafkaError> {
        // Java wraps the construction in a try/catch that closes the
        // transport on error and re-throws as `KafkaException`. Our
        // `PlaintextTransportLayer::new` is infallible so the catch
        // is effectively unreachable for plaintext — keep the
        // structural mirror so the SSL builder (which has fallible
        // construction via `rustls::ClientConnection::new`) reads the
        // same way.
        let transport = PlaintextTransportLayer::new(stream);
        let authenticator = PlaintextAuthenticator::new();
        Ok(KafkaChannel::new(
            id,
            Box::new(transport),
            Box::new(authenticator),
            max_receive_size,
            metadata_registry,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use tokio::net::{TcpListener, TcpStream};

    use super::*;
    use crate::common::network::channel_metadata_registry::DefaultChannelMetadataRegistry;

    async fn connected_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr: SocketAddr = listener.local_addr().expect("local_addr");
        let connect = TcpStream::connect(addr);
        let accept = listener.accept();
        let (client, accepted) = tokio::join!(connect, accept);
        let (server, _) = accepted.expect("accept");
        (client.expect("connect"), server)
    }

    #[tokio::test]
    async fn builds_channel_over_plaintext() {
        let (client, _server) = connected_pair().await;
        let builder = PlaintextChannelBuilder::new(None);
        let channel = builder
            .build_channel(Arc::from("0"), client, 1024, Box::new(DefaultChannelMetadataRegistry::new()))
            .expect("build");
        assert_eq!(channel.id(), "0");
        assert!(channel.ready(), "plaintext channel is ready immediately");
    }

    #[tokio::test]
    async fn build_channel_carries_listener_name() {
        let listener_name = ListenerName::new("INTERNAL");
        let builder = PlaintextChannelBuilder::new(Some(listener_name.clone()));
        assert_eq!(builder.listener_name(), Some(&listener_name));
    }

    #[test]
    fn close_is_noop() {
        let mut builder = PlaintextChannelBuilder::new(None);
        // Mirrors Java's empty close().
        builder.close();
    }
}
