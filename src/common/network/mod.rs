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
//! The TLS/PLAINTEXT transport layers, `KafkaChannel`, and `Selector` arrive
//! in Phase 5b/5c.

pub mod byte_buffer_send;
pub mod invalid_receive_error;
pub mod network_receive;
pub mod network_send;
pub mod receive;
pub mod send;
pub mod transferable_channel;

pub use byte_buffer_send::ByteBufferSend;
pub use invalid_receive_error::InvalidReceiveError;
pub use network_receive::NetworkReceive;
pub use network_send::NetworkSend;
pub use receive::Receive;
pub use send::Send;
pub use transferable_channel::TransferableChannel;
