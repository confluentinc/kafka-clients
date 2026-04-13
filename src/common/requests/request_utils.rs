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

//! Utility methods for request/response serialization.
//!
//! Corresponds to `org.apache.kafka.common.requests.RequestUtils`.

use crate::common::protocol::ByteBufferAccessor;
use crate::common::protocol::message::Message;
use crate::common::protocol::object_serialization_cache::ObjectSerializationCache;

use std::io;

use super::NO_PARTITION_LEADER_EPOCH;

/// Returns `Some(leader_epoch)` if the given epoch is valid (not
/// [`NO_PARTITION_LEADER_EPOCH`]), or `None` otherwise.
///
/// Corresponds to `RequestUtils.getLeaderEpoch` in Java.
pub fn get_leader_epoch(leader_epoch: i32) -> Option<i32> {
    if leader_epoch == NO_PARTITION_LEADER_EPOCH {
        None
    } else {
        Some(leader_epoch)
    }
}

/// Serializes a header and an API message into a single byte buffer.
///
/// The buffer is returned with position set to 0, ready for reading.
/// No size prefix is added (unlike `SendBuilder::build_send` which prepends a 4-byte size).
///
/// Corresponds to `RequestUtils.serialize` in Java.
///
/// # Errors
///
/// Returns an error if size calculation or serialization fails.
pub fn serialize(
    header: &impl Message,
    header_version: i16,
    api_message: &impl Message,
    api_version: i16,
) -> io::Result<ByteBufferAccessor> {
    let mut cache = ObjectSerializationCache::new();

    let header_size = header.size(&mut cache, header_version)?;
    let message_size = api_message.size(&mut cache, api_version)?;
    let mut writable = ByteBufferAccessor::new((header_size + message_size) as usize);

    header.write(&mut writable, &cache, header_version)?;
    api_message.write(&mut writable, &cache, api_version)?;

    writable.flip();
    Ok(writable)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_leader_epoch_valid() {
        assert_eq!(get_leader_epoch(5), Some(5));
        assert_eq!(get_leader_epoch(0), Some(0));
        assert_eq!(get_leader_epoch(100), Some(100));
    }

    #[test]
    fn test_get_leader_epoch_no_epoch() {
        assert_eq!(get_leader_epoch(NO_PARTITION_LEADER_EPOCH), None);
    }
}
