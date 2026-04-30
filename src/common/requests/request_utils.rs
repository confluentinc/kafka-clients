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

//! Translation of `org.apache.kafka.common.requests.RequestUtils`.

use crate::common::errors::KafkaError;
use crate::common::protocol::Message;
use crate::common::protocol::byte_buffer_accessor::ByteBufferAccessor;
use crate::common::protocol::object_serialization_cache::ObjectSerializationCache;

/// Mirrors Java's `Records.NO_PARTITION_LEADER_EPOCH = -1`. Defined here
/// because we don't have a dedicated `RecordBatch` translation yet (Phase 3).
pub const NO_PARTITION_LEADER_EPOCH: i32 = -1;

/// Mirrors `RequestUtils.getLeaderEpoch(int leaderEpoch)`.
pub fn get_leader_epoch(leader_epoch: i32) -> Option<i32> {
    if leader_epoch == NO_PARTITION_LEADER_EPOCH {
        None
    } else {
        Some(leader_epoch)
    }
}

/// Serialize a header and body into a single byte vector (no length prefix).
/// Mirrors `RequestUtils.serialize(Message header, short headerVersion,
/// Message apiMessage, short apiVersion)`.
///
/// TODO Phase 4: drop the `to_vec()` on the inline buffer. Phase 2 only
/// uses this helper from tests and `AbstractRequest::serialize_with_header`
/// (which itself is test-only at this milestone). When Phase 4 rebuilds the
/// producer to construct `Send`s through `SendBuilder` directly, this
/// helper can be reshaped to return a `ByteBufferAccessor` (read-mode) so
/// callers can slice without an extra copy. See COMMENTS.0.md Issue 8.
pub fn serialize(
    header: &dyn Message,
    header_version: i16,
    api_message: &dyn Message,
    api_version: i16,
) -> Result<Vec<u8>, KafkaError> {
    let mut cache = ObjectSerializationCache::new();
    let header_size = header.size(&mut cache, header_version);
    let message_size = api_message.size(&mut cache, api_version);
    let mut writable = ByteBufferAccessor::allocate((header_size + message_size) as usize);
    header.write(&mut writable, &cache, header_version)?;
    api_message.write(&mut writable, &cache, api_version)?;
    writable.flip();
    Ok(writable.buffer().to_vec())
}

/// Mirrors `RequestUtils.isFatalException(Throwable)`. Translates the
/// hard-coded Java type list against our `KafkaError` variants.
pub fn is_fatal_exception(error: &KafkaError) -> bool {
    matches!(
        error,
        KafkaError::Authentication(_)
            | KafkaError::Authorization(_)
            | KafkaError::TopicAuthorization(_)
            | KafkaError::ClusterAuthorization(_)
            | KafkaError::UnsupportedVersion(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translation of `RequestUtilsTest#testIsFatalException`.
    /// Note: `MismatchedEndpointTypeException`, `SecurityDisabledException`,
    /// `UnsupportedEndpointTypeException`, and `UnsupportedForMessageFormatException`
    /// are not in the producer-relevant `KafkaError` set (Phase 1 collapsed
    /// the exception hierarchy to a producer-focused subset). They are
    /// included in Java's `RequestUtils.isFatalException` for the broker
    /// path; the producer path never sees them. If/when Phase 5+ wires up
    /// those error mappings, extend `is_fatal_exception` accordingly.
    #[test]
    fn is_fatal_exception_matches_java() {
        assert!(is_fatal_exception(&KafkaError::Authentication(String::new())));
        assert!(is_fatal_exception(&KafkaError::Authorization(String::new())));
        assert!(is_fatal_exception(&KafkaError::UnsupportedVersion(String::new())));

        // Retriable exceptions (Disconnect ↔ Java's `DisconnectException`)
        // must NOT be fatal.
        assert!(!is_fatal_exception(&KafkaError::Disconnect(String::new())));
    }

    #[test]
    fn get_leader_epoch_translates_sentinel() {
        assert_eq!(get_leader_epoch(NO_PARTITION_LEADER_EPOCH), None);
        assert_eq!(get_leader_epoch(0), Some(0));
        assert_eq!(get_leader_epoch(7), Some(7));
    }
}
