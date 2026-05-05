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

//! Translation of `org.apache.kafka.common.network`.
//!
//! Phase 5a wires the network primitives that do not depend on a concrete
//! transport: the `Send`/`Receive` traits, their plain-buffer
//! implementations (`ByteBufferSend`, `NetworkSend`, `NetworkReceive`), the
//! `TransferableChannel` trait, channel-state value types, and `ListenerName`.
//!
//! Phase 5b adds the concrete transport layers (`TransportLayer` trait,
//! `PlaintextTransportLayer`, `SslTransportLayer`) and
//! `KafkaChannel`. The `Selector` and connection-state plumbing arrive in
//! Phase 5c.

pub mod byte_buffer_send;
pub mod channel_metadata_registry;
pub mod channel_state;
pub mod cipher_information;
pub mod client_information;
pub mod connection_mode;
pub mod invalid_receive_error;
pub mod listener_name;
pub mod network_receive;
pub mod network_send;
pub mod plaintext_transport_layer;
pub mod receive;
pub mod send;
pub mod server_connection_id;
pub mod ssl_transport_layer;
pub mod transferable_channel;
pub mod transport_layer;

pub use byte_buffer_send::ByteBufferSend;
pub use channel_metadata_registry::{ChannelMetadataRegistry, DefaultChannelMetadataRegistry};
pub use channel_state::{ChannelState, ChannelStateName};
pub use cipher_information::CipherInformation;
pub use client_information::ClientInformation;
pub use connection_mode::ConnectionMode;
pub use invalid_receive_error::InvalidReceiveError;
pub use listener_name::ListenerName;
pub use network_receive::NetworkReceive;
pub use network_send::NetworkSend;
pub use plaintext_transport_layer::PlaintextTransportLayer;
pub use receive::Receive;
pub use send::Send;
pub use server_connection_id::ServerConnectionId;
pub use ssl_transport_layer::SslTransportLayer;
pub use transferable_channel::TransferableChannel;
pub use transport_layer::TransportLayer;
