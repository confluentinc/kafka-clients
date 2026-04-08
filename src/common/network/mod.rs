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

pub mod byte_buffer_send;
pub mod invalid_receive_exception;
pub mod network_receive;
pub mod network_send;
pub mod plaintext_transport_layer;
pub mod receive;
pub mod send;
pub mod transport_layer;

pub use byte_buffer_send::ByteBufferSend;
pub use invalid_receive_exception::InvalidReceiveException;
pub use network_receive::NetworkReceive;
pub use network_send::NetworkSend;
pub use plaintext_transport_layer::PlaintextTransportLayer;
pub use receive::Receive;
pub use send::KafkaSend;
pub use transport_layer::TransportLayer;
