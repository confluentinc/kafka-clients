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

//! The `KafkaSend` trait models the in-progress sending of data.
//!
//! Translated from `org.apache.kafka.common.network.Send`.
//!
//! Renamed from `Send` to `KafkaSend` to avoid conflict with `std::marker::Send`.

use super::transport_layer::TransportLayer;

use std::future::Future;
use std::io;
use std::pin::Pin;

/// Models the in-progress sending of data.
///
/// This trait represents a send operation that may require multiple calls to
/// [`write_to`](KafkaSend::write_to) before all data is fully written.
///
/// All I/O is async per CLAUDE.md rule 8.
pub trait KafkaSend: Send {
    /// Returns `true` if this send is complete.
    fn completed(&self) -> bool;

    /// Writes some as-yet unwritten bytes from this send to the provided channel.
    ///
    /// It may take multiple calls for the send to be completely written.
    ///
    /// # Arguments
    ///
    /// * `channel` - The channel to write to
    ///
    /// # Returns
    ///
    /// The number of bytes written.
    ///
    /// # Errors
    ///
    /// Returns an error if the write fails.
    fn write_to<'a>(
        &'a mut self,
        channel: &'a mut dyn TransportLayer,
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>>;

    /// Returns the total size of this send in bytes.
    fn size(&self) -> usize;
}
