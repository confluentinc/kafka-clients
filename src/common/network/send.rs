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

//! Translation of `org.apache.kafka.common.network.Send`.

use std::io;

use super::TransferableChannel;

/// Models the in-progress sending of data to a [`TransferableChannel`].
///
/// Mirrors the Java interface `org.apache.kafka.common.network.Send`.
///
/// Note: the Rust trait is named `Send` to match the Java name. It must
/// always be imported qualified (`use crate::common::network::send::Send as
/// NetworkSendTrait;` or `use crate::common::network;` then `network::Send`)
/// so it does not clash with `std::marker::Send`.
pub trait Send {
    /// Is this send complete? Mirrors `Send.completed()`.
    fn completed(&self) -> bool;

    /// Write some as-yet unwritten bytes from this send to the provided
    /// channel. It may take multiple calls for the send to be completely
    /// written. Returns the number of bytes written.
    ///
    /// Mirrors `Send.writeTo(TransferableChannel)`.
    fn write_to(&mut self, channel: &mut dyn TransferableChannel) -> io::Result<u64>;

    /// Total size of the send, in bytes. Mirrors `Send.size()`.
    fn size(&self) -> u64;
}
