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
//! In Rust, it takes a `Box<dyn TransportLayer>` since we eliminate `SelectionKey`
//! — the Selector creates the transport layer and passes it to the builder.

use super::channel_metadata_registry::ChannelMetadataRegistry;
use super::kafka_channel::KafkaChannel;
use super::transport_layer::TransportLayer;

use std::io;

/// A channel builder interface to build channels based on configuration.
///
/// Translated from the Java `ChannelBuilder` interface.
///
/// In the Rust adaptation, `build_channel` takes a `Box<dyn TransportLayer>` instead of
/// Java's `SelectionKey`, since `SelectionKey` is eliminated in favor of HashMap-based
/// channel lookup by ID.
pub trait ChannelBuilder: Send {
    /// Returns a `KafkaChannel` with `TransportLayer` and `Authenticator` configured.
    ///
    /// # Arguments
    ///
    /// * `id` - Channel ID
    /// * `transport_layer` - The transport layer for this channel
    /// * `max_receive_size` - Maximum size of a single receive buffer to allocate
    /// * `metadata_registry` - Registry which stores the metadata about the channels
    ///
    /// # Errors
    ///
    /// Returns an error if the channel cannot be built.
    fn build_channel(
        &self,
        id: &str,
        transport_layer: Box<dyn TransportLayer>,
        max_receive_size: i32,
        metadata_registry: Box<dyn ChannelMetadataRegistry>,
    ) -> io::Result<KafkaChannel>;

    /// Closes this channel builder.
    fn close(&mut self);
}
