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

//! The `Receive` trait models the in-progress reading of data from a channel.
//!
//! Translated from `org.apache.kafka.common.network.Receive`.
//!
//! In Java, `readFrom` takes a `ScatteringByteChannel`. Since `TransportLayer`
//! extends `ScatteringByteChannel`, in Rust `read_from` takes `&mut dyn TransportLayer`
//! to preserve the same composability.

use super::TransportLayer;

use std::future::Future;
use std::io;
use std::pin::Pin;

/// Models the in-progress reading of data from a channel identified by a source string.
///
/// Data is read incrementally: [`read_from`](Receive::read_from) may need to be called
/// multiple times before [`complete`](Receive::complete) returns `true`.
pub trait Receive: Send {
    /// The identifier of the source from which we are receiving data.
    fn source(&self) -> &str;

    /// Returns `true` if we are done receiving data.
    fn complete(&self) -> bool;

    /// Reads bytes into this receive from the given channel.
    ///
    /// In Java, this takes a `ScatteringByteChannel`. Since `TransportLayer` extends
    /// `ScatteringByteChannel`, in Rust we take `&mut dyn TransportLayer` directly.
    ///
    /// # Arguments
    ///
    /// * `channel` - The transport layer to read from
    ///
    /// # Returns
    ///
    /// The number of bytes read.
    ///
    /// # Errors
    ///
    /// Returns an error if the reading fails, including `UnexpectedEof` if the
    /// remote end closes the connection before the receive is complete.
    fn read_from<'a>(
        &'a mut self,
        channel: &'a mut dyn TransportLayer,
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>>;

    /// Returns `true` if we know how much memory is required to fully read this receive.
    fn required_memory_amount_known(&self) -> bool;

    /// Returns `true` if the underlying memory required to complete reading has been allocated.
    fn memory_allocated(&self) -> bool;
}
