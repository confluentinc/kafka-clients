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

pub mod authenticator;
pub mod byte_buffer_send;
pub mod channel_builder;
pub mod channel_metadata_registry;
pub mod channel_state;
pub mod cipher_information;
pub mod client_information;
pub mod connection_mode;
pub mod invalid_receive_error;
pub mod kafka_channel;
pub mod listener_name;
pub mod mock_selector;
pub mod network_receive;
pub mod network_send;
pub mod plaintext_channel_builder;
pub mod plaintext_transport_layer;
pub mod receive;
pub mod sasl_channel_builder;
pub mod selectable;
pub mod selector;
pub mod send;
pub mod ssl_channel_builder;
pub mod ssl_transport_layer;
pub mod transport_layer;

pub use authenticator::{Authenticator, PlaintextAuthenticator};
pub use byte_buffer_send::ByteBufferSend;
pub use channel_builder::ChannelBuilder;
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
pub use selectable::{Selectable, USE_DEFAULT_BUFFER_SIZE};
pub use selector::Selector;
pub use send::KafkaSend;
pub use ssl_channel_builder::SslChannelBuilder;
pub use ssl_transport_layer::SslTransportLayer;
pub use transport_layer::TransportLayer;
