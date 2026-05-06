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

//! Translation of `org.apache.kafka.common.protocol.Message`.

use crate::common::errors::KafkaError;
use crate::common::protocol::types::RawTaggedField;
use crate::common::protocol::{MessageSizeAccumulator, ObjectSerializationCache, Readable, Writable};

/// An object that can serialise itself. The serialisation protocol is versioned.
///
/// Translation of the Java `Message` interface. Implementors are emitted by
/// the `generator/` crate for each `*.json` schema. The trait surface mirrors
/// the Java contract; deviations are noted on individual methods.
pub trait Message {
    /// Returns the lowest supported API version, inclusive.
    fn lowest_supported_version(&self) -> i16;

    /// Returns the highest supported API version, inclusive.
    fn highest_supported_version(&self) -> i16;

    /// Add the size of this message to `size`.
    ///
    /// Java contract: implementors call into the [`MessageSizeAccumulator`]
    /// (mutating both `total_size` and `zero_copy_size`) and may populate
    /// `cache` with intermediate sizes / pre-encoded byte sequences for the
    /// subsequent [`Message::write`] pass.
    fn add_size(&self, size: &mut MessageSizeAccumulator, cache: &mut ObjectSerializationCache, version: i16);

    /// Returns the number of bytes [`Message::write`] would emit. Mirrors the
    /// Java default `size(cache, version)`.
    fn size(&self, cache: &mut ObjectSerializationCache, version: i16) -> i32 {
        let mut acc = MessageSizeAccumulator::new();
        self.add_size(&mut acc, cache, version);
        acc.total_size()
    }

    /// Write this message to `writable`. The caller must have previously
    /// invoked [`Message::add_size`] on the same `cache`.
    fn write(
        &self,
        writable: &mut dyn Writable,
        cache: &ObjectSerializationCache,
        version: i16,
    ) -> Result<(), KafkaError>;

    /// Reads this message from `readable`, overwriting all relevant fields.
    fn read(&mut self, readable: &mut dyn Readable, version: i16) -> Result<(), KafkaError>;

    /// Returns the list of tagged fields that this software does not
    /// understand.
    fn unknown_tagged_fields(&self) -> &[RawTaggedField];
}
