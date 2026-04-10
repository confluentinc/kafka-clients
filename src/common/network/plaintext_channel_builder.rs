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

//! Plaintext channel builder that creates unencrypted channels.
//!
//! Translated from `org.apache.kafka.common.network.PlaintextChannelBuilder`.
//!
//! In Java, `PlaintextChannelBuilder` creates a `PlaintextTransportLayer` from a
//! `SelectionKey` and wraps it with a `PlaintextAuthenticator` in a `KafkaChannel`.
//!
//! In Rust, the Selector creates the `PlaintextTransportLayer` and passes it as a
//! `Box<dyn TransportLayer>` to the builder. The builder wraps it with a
//! `PlaintextAuthenticator` in a `KafkaChannel`.

use super::authenticator::PlaintextAuthenticator;
use super::channel_builder::ChannelBuilder;
use super::channel_metadata_registry::ChannelMetadataRegistry;
use super::kafka_channel::KafkaChannel;
use super::listener_name::ListenerName;
use super::transport_layer::TransportLayer;

use std::io;

/// Plaintext channel builder that creates unencrypted channels.
///
/// The `listener_name` is non-null when instantiated in the broker and `None` otherwise
/// (client mode).
pub struct PlaintextChannelBuilder {
    /// The listener name, if any (server-side only).
    #[allow(dead_code)]
    listener_name: Option<ListenerName>,
}

impl PlaintextChannelBuilder {
    /// Creates a new `PlaintextChannelBuilder`.
    ///
    /// `listener_name` is `Some` when instantiated in the broker and `None` otherwise.
    pub fn new(listener_name: Option<ListenerName>) -> Self {
        Self { listener_name }
    }
}

impl ChannelBuilder for PlaintextChannelBuilder {
    fn build_channel(
        &self,
        id: &str,
        transport_layer: Box<dyn TransportLayer>,
        max_receive_size: i32,
        metadata_registry: Box<dyn ChannelMetadataRegistry>,
    ) -> io::Result<KafkaChannel> {
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
        // no-op for plaintext
    }
}
