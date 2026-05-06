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

    /// Releases any resources held by the builder. Mirrors Java's
    /// `close()` (the `AutoCloseable` impl). Default no-op — only the
    /// SSL builder holds a long-lived resource (the rustls `ClientConfig`
    /// is `Arc`-shared and freed on last reference).
    fn close(&mut self) {}
}
