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

//! Translation of `org.apache.kafka.common.network.ChannelBuilder`.

use std::sync::Arc;

use rustls::pki_types::ServerName;
use tokio::net::TcpStream;

use crate::common::errors::KafkaError;
use crate::common::network::KafkaChannel;
use crate::common::network::kafka_channel::BoxedMetadataRegistry;

/// A `ChannelBuilder` constructs a [`KafkaChannel`] from an already-
/// connected [`TcpStream`]. Mirrors the Java `ChannelBuilder`
/// interface.
///
/// **Java→Rust signature differences:**
///
/// 1. The Java `buildChannel` takes a `SelectionKey` (which encapsulates
///    the underlying `SocketChannel`); the Tokio translation passes the
///    [`TcpStream`] directly. The selector that holds the readiness
///    state is reachable through other channels in the Phase 5c
///    Selector — the `KafkaChannel` itself does not need a back-channel
///    to it.
///
/// 2. The Java `MemoryPool memoryPool` parameter is omitted — the Phase
///    5a [`crate::common::network::NetworkReceive`] does its own
///    allocation, sized to the parsed length-prefix. See
///    [`crate::common::network::kafka_channel`] for the deferral note.
///
/// 3. Java's `Configurable.configure(Map<String, ?> configs)` is folded
///    into each builder's constructor since the Rust translation
///    accepts a typed config struct rather than a stringly-keyed map.
pub trait ChannelBuilder: std::marker::Send {
    /// Construct a [`KafkaChannel`] tagged with the given id over the
    /// already-connected `stream`. Mirrors Java's
    /// `buildChannel(String id, SelectionKey key, int maxReceiveSize,
    /// MemoryPool memoryPool, ChannelMetadataRegistry metadataRegistry)`.
    ///
    /// The builder owns the construction of:
    /// * the [`crate::common::network::TransportLayer`] (concrete
    ///   plaintext or SSL transport);
    /// * the [`crate::common::network::authenticator::Authenticator`]
    ///   (always-complete plaintext / SSL authenticator in this phase);
    /// * the [`KafkaChannel`] wiring them together.
    fn build_channel(
        &self,
        id: Arc<str>,
        stream: TcpStream,
        max_receive_size: i32,
        metadata_registry: BoxedMetadataRegistry,
    ) -> Result<KafkaChannel, KafkaError>;

    /// SNI-aware variant of [`Self::build_channel`]. The default
    /// implementation discards `server_name` and delegates to
    /// [`Self::build_channel`] — the plaintext / SASL_PLAINTEXT builders
    /// don't need it. The SSL / SASL_SSL builders override to require
    /// `Some(server_name)` and call their typed
    /// `build_ssl_channel(...)` / `build_sasl_ssl_channel(...)` entry
    /// points (which the trait method itself cannot reach because it has
    /// no `server_name` parameter).
    ///
    /// **Java parity note.** Java's `SslTransportLayer` derives the SNI
    /// hostname from `SocketChannel.socket().getInetAddress()` (reverse-
    /// DNS lookup on the resolved peer). The Rust translation passes the
    /// original hostname explicitly because the connecting code already
    /// knows it from the unresolved bootstrap entry — this avoids a
    /// reverse-DNS roundtrip that could disagree with the cert's SAN
    /// (the design rationale is also recorded in
    /// [`crate::common::network::SslChannelBuilder::build_ssl_channel`]).
    fn build_channel_with_server_name(
        &self,
        id: Arc<str>,
        stream: TcpStream,
        server_name: Option<ServerName<'static>>,
        max_receive_size: i32,
        metadata_registry: BoxedMetadataRegistry,
    ) -> Result<KafkaChannel, KafkaError> {
        let _ = server_name;
        self.build_channel(id, stream, max_receive_size, metadata_registry)
    }

    /// Releases any resources held by the builder. Mirrors Java's
    /// `close()` (the `AutoCloseable` impl). Default no-op — only the
    /// SSL builder holds a long-lived resource (the rustls `ClientConfig`
    /// is `Arc`-shared and freed on last reference).
    fn close(&mut self) {}
}
