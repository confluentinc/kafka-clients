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

//! Translation of `org.apache.kafka.common.network.SslChannelBuilder`.

use std::sync::Arc;

use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use tokio::net::TcpStream;

use crate::common::errors::KafkaError;
use crate::common::network::authenticator::SslAuthenticator;
use crate::common::network::channel_builder::ChannelBuilder;
use crate::common::network::connection_mode::ConnectionMode;
use crate::common::network::kafka_channel::BoxedMetadataRegistry;
use crate::common::network::{KafkaChannel, ListenerName, SslTransportLayer};

/// Builds [`KafkaChannel`]s wrapping an [`SslTransportLayer`].
/// Mirrors Java's `SslChannelBuilder`.
///
/// Java holds a stateful `SslFactory` that maintains the `KeyManager` /
/// `TrustManager` chain. The rustls-based translation receives a
/// pre-built [`Arc<rustls::ClientConfig>`] — config loading (PEM cert
/// loading, root-store building) is the caller's responsibility, with
/// helpers in [`crate::common::network::channel_builders`]. This keeps
/// the builder pure-data so it can be shared across many channels.
///
/// The `ListenerReconfigurable` interface from Java is intentionally
/// not translated: dynamic broker reconfiguration of listener TLS is
/// a Phase 9 concern.
pub struct SslChannelBuilder {
    /// Listener name. Non-`None` only when instantiated on the broker.
    /// Mirrors Java's nullable `ListenerName listenerName` field.
    listener_name: Option<ListenerName>,
    /// Whether or not this listener is used for inter-broker requests.
    /// Mirrors Java's `boolean isInterBrokerListener`. The producer
    /// always passes `false`.
    is_inter_broker_listener: bool,
    /// `CLIENT` or `SERVER`. Mirrors Java's `ConnectionMode connectionMode`.
    connection_mode: ConnectionMode,
    /// rustls client configuration, shared across all channels built
    /// from this builder.
    client_config: Arc<ClientConfig>,
}

impl SslChannelBuilder {
    /// Constructs an SSL channel builder with the given rustls
    /// [`ClientConfig`]. Mirrors Java's
    /// `SslChannelBuilder(ConnectionMode, ListenerName, boolean)` plus
    /// `configure(Map<String, ?>)` folded into one call (the rustls
    /// config is constructed by the caller — see
    /// [`crate::common::network::channel_builders`] for the helpers).
    pub fn new(
        connection_mode: ConnectionMode,
        listener_name: Option<ListenerName>,
        is_inter_broker_listener: bool,
        client_config: Arc<ClientConfig>,
    ) -> Self {
        SslChannelBuilder { listener_name, is_inter_broker_listener, connection_mode, client_config }
    }

    /// Borrow the listener name configured on this builder. Mirrors
    /// the Java `listenerName()` method on the
    /// `ListenerReconfigurable` interface.
    pub fn listener_name(&self) -> Option<&ListenerName> {
        self.listener_name.as_ref()
    }

    /// `true` iff this listener is used for inter-broker requests.
    /// Mirrors the Java field accessor.
    pub fn is_inter_broker_listener(&self) -> bool {
        self.is_inter_broker_listener
    }

    /// Mirrors Java's `connectionMode` field accessor.
    pub fn connection_mode(&self) -> ConnectionMode {
        self.connection_mode
    }

    /// Build an SSL channel against the given `server_name` (used for
    /// SNI and certificate verification).
    ///
    /// Java's signature does not take a `server_name` — it derives the
    /// peer hostname from the `SocketChannel.socket().getInetAddress()`
    /// of the connected socket. The rustls translation requires the
    /// caller to provide the SNI hostname explicitly because the
    /// connecting code (Phase 5c Selector) already knows it from the
    /// resolved bootstrap address; reverse-DNS would be redundant and
    /// could disagree with the cert's SAN.
    pub fn build_ssl_channel(
        &self,
        id: Arc<str>,
        stream: TcpStream,
        server_name: ServerName<'static>,
        max_receive_size: i32,
        metadata_registry: BoxedMetadataRegistry,
    ) -> Result<KafkaChannel, KafkaError> {
        // Mirror Java: wrap construction in a try/catch that closes the
        // transport on error.
        let transport = SslTransportLayer::new(id.as_ref(), stream, Arc::clone(&self.client_config), server_name)
            .map_err(|e| KafkaError::Network(e.to_string()))?;
        // SslAuthenticator is stateless — it queries
        // `transport.peer_principal()` lazily on every `principal()`
        // call (mirrors Java's `transportLayer.sslSession()` lookup).
        // This is intentional: at construction time the TLS handshake
        // has not run yet, so an eager fetch would freeze the
        // pre-handshake anonymous principal forever.
        let authenticator = SslAuthenticator::new();
        Ok(KafkaChannel::new(
            id,
            Box::new(transport),
            Box::new(authenticator),
            max_receive_size,
            metadata_registry,
        ))
    }
}

impl ChannelBuilder for SslChannelBuilder {
    fn build_channel(
        &self,
        _id: Arc<str>,
        _stream: TcpStream,
        _max_receive_size: i32,
        _metadata_registry: BoxedMetadataRegistry,
    ) -> Result<KafkaChannel, KafkaError> {
        // The trait method does not carry a `server_name` parameter
        // because the Java `ChannelBuilder.buildChannel` signature is
        // identical for plaintext/SSL/SASL. SSL callers must use
        // [`Self::build_ssl_channel`] to supply the SNI server name —
        // we surface a clear error here so a misuse is obvious rather
        // than silently picking a default.
        Err(KafkaError::IllegalState(
            "SslChannelBuilder requires a server name; call build_ssl_channel(...) instead".to_owned(),
        ))
    }

    fn close(&mut self) {
        // Java releases the SslFactory; rustls's ClientConfig is freed
        // automatically when the last `Arc` is dropped. No-op.
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rustls::ClientConfig;
    use rustls::RootCertStore;

    use super::*;

    fn empty_client_config() -> Arc<ClientConfig> {
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let cfg = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("client versions")
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth();
        Arc::new(cfg)
    }

    #[test]
    fn build_channel_via_trait_method_returns_illegal_state() {
        // The trait method on `ChannelBuilder` doesn't carry the SNI
        // server name; using it for SSL is a programming error.
        let builder = SslChannelBuilder::new(ConnectionMode::Client, None, false, empty_client_config());
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("rt");
        runtime.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let addr = listener.local_addr().expect("local_addr");
            let connect = tokio::net::TcpStream::connect(addr);
            let accept = listener.accept();
            let (client, accepted) = tokio::join!(connect, accept);
            let _server = accepted.expect("accept").0;
            let stream = client.expect("connect");
            let err = builder
                .build_channel(
                    Arc::from("0"),
                    stream,
                    1024,
                    Box::new(crate::common::network::channel_metadata_registry::DefaultChannelMetadataRegistry::new()),
                )
                .expect_err("trait method must fail");
            assert!(matches!(err, KafkaError::IllegalState(_)));
        });
    }

    #[test]
    fn metadata_accessors() {
        let listener_name = ListenerName::new("INTERNAL");
        let builder =
            SslChannelBuilder::new(ConnectionMode::Server, Some(listener_name.clone()), true, empty_client_config());
        assert_eq!(builder.listener_name(), Some(&listener_name));
        assert!(builder.is_inter_broker_listener());
        assert_eq!(builder.connection_mode(), ConnectionMode::Server);
    }
}
