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

use std::io;

/// Models the in-progress reading of data from a channel identified by a source string.
///
/// Data is read incrementally: [`read_from`](Receive::read_from) may need to be called
/// multiple times before [`complete`](Receive::complete) returns `true`.
pub trait Receive {
    /// The identifier of the source from which we are receiving data.
    fn source(&self) -> &str;

    /// Returns `true` if we are done receiving data.
    fn complete(&self) -> bool;

    /// Reads bytes into this receive from the given readable source.
    ///
    /// # Arguments
    ///
    /// * `channel` - The readable source to read from
    ///
    /// # Returns
    ///
    /// The number of bytes read.
    ///
    /// # Errors
    ///
    /// Returns an error if the reading fails.
    fn read_from(&mut self, channel: &mut dyn io::Read) -> io::Result<usize>;

    /// Returns `true` if we know how much memory is required to fully read this receive.
    fn required_memory_amount_known(&self) -> bool;

    /// Returns `true` if the underlying memory required to complete reading has been allocated.
    fn memory_allocated(&self) -> bool;
}
