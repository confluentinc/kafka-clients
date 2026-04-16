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

//! A channel builder interface to build channels based on configs.
//!
//! Translated from `org.apache.kafka.common.network.ChannelBuilder`.
//!
//! In Java, `ChannelBuilder` takes a `SelectionKey` to build a `KafkaChannel`.
//! In Rust, it takes a `TcpStream` and `peer_host` since we eliminate `SelectionKey`
//! — the Selector creates the TCP connection and passes the raw stream to the builder,
//! which wraps it in the appropriate transport layer (plaintext or SSL/TLS).

use super::ChannelMetadataRegistry;
use super::KafkaChannel;

use std::io;

use tokio::net::TcpStream;

/// A channel builder interface to build channels based on configuration.
///
/// Translated from the Java `ChannelBuilder` interface.
///
/// In the Rust adaptation, `build_channel` takes a `TcpStream` and `peer_host` instead
/// of Java's `SelectionKey`. The builder wraps the stream in the appropriate transport
/// layer (e.g., `PlaintextTransportLayer` or `SslTransportLayer`) and pairs it with
/// an `Authenticator` inside a `KafkaChannel`.
pub trait ChannelBuilder: Send {
    /// Returns a `KafkaChannel` with `TransportLayer` and `Authenticator` configured.
    ///
    /// # Arguments
    ///
    /// * `id` - Channel ID
    /// * `stream` - The raw TCP stream for this channel
    /// * `peer_host` - The hostname of the remote peer (used for TLS SNI and hostname verification)
    /// * `max_receive_size` - Maximum size of a single receive buffer to allocate
    /// * `metadata_registry` - Registry which stores the metadata about the channels
    ///
    /// # Errors
    ///
    /// Returns an error if the channel cannot be built.
    fn build_channel(
        &self,
        id: &str,
        stream: TcpStream,
        peer_host: &str,
        max_receive_size: i32,
        metadata_registry: Box<dyn ChannelMetadataRegistry>,
    ) -> io::Result<KafkaChannel>;

    /// Closes this channel builder.
    fn close(&mut self);
}
