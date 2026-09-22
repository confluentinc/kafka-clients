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

//! Network transport layer for Kafka (org.apache.kafka.common.network)
//!
//! This module provides the low-level TCP I/O and Kafka protocol framing.
//! It sits between the wire protocol and the higher-level channel abstraction.

mod authentication_error;
mod authenticator;
mod byte_buffer_send;
mod channel_builder;
mod channel_builders;
mod channel_metadata_registry;
pub mod channel_state;
mod cipher_information;
mod client_information;
mod connection_mode;
mod invalid_receive_error;
pub mod kafka_channel;
mod listener_name;
mod mock_selector;
mod network_receive;
mod network_send;
mod plaintext_channel_builder;
mod plaintext_transport_layer;
mod receive;
mod sasl_channel_builder;
mod selectable;
mod selector;
mod send;
mod ssl_channel_builder;
mod ssl_transport_layer;
mod transport_layer;

// `AuthenticationError` itself is not re-exported here: the single translation of
// `org.apache.kafka.common.errors.AuthenticationException` lives at
// `crate::common::errors::AuthenticationError`, and one type deserves one path.
pub use authentication_error::{
    auth_io_error, auth_io_error_with_source, authentication_error_message, is_authentication_error,
};
pub use authenticator::{Authenticator, PlaintextAuthenticator};
pub use byte_buffer_send::ByteBufferSend;
pub use channel_builder::ChannelBuilder;
pub use channel_builders::ChannelBuilders;
pub use channel_metadata_registry::{ChannelMetadataRegistry, DefaultChannelMetadataRegistry};
pub use channel_state::ChannelState;
pub use cipher_information::CipherInformation;
pub use client_information::ClientInformation;
pub use connection_mode::ConnectionMode;
pub use invalid_receive_error::InvalidReceiveError;
pub use kafka_channel::KafkaChannel;
pub use listener_name::ListenerName;
pub use mock_selector::{DelayedReceive, MockSelector};
pub use network_receive::NetworkReceive;
pub use network_send::NetworkSend;
pub use plaintext_channel_builder::PlaintextChannelBuilder;
pub use plaintext_transport_layer::PlaintextTransportLayer;
pub use receive::Receive;
pub use sasl_channel_builder::SaslChannelBuilder;
pub use selectable::Selectable;
pub use selector::Selector;
pub use send::KafkaSend;
pub use ssl_channel_builder::SslChannelBuilder;
pub use ssl_transport_layer::SslTransportLayer;
pub use transport_layer::{InterestOps, TransportLayer};
