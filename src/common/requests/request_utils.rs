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

use crate::common::ByteBufferAccessor;
use crate::common::Error;
use crate::common::protocol::Message;
use crate::common::protocol::ObjectSerializationCache;

use std::io;

use super::RECORD_BATCH_NO_PARTITION_LEADER_EPOCH;

/// Translates the Java static-utility class `org.apache.kafka.common.requests.RequestUtils`,
/// which has no instance state, so it becomes a unit struct hosting its
/// statics as associated items.
pub struct RequestUtils;

impl RequestUtils {
    /// Whether the exception is fatal — retrying is pointless because the condition
    /// cannot clear on its own.
    ///
    /// A direct translation of Java's `RequestUtils.isFatalException(Throwable)`
    /// (`common/requests/RequestUtils.java:88`), an `instanceof` chain over seven
    /// classes. It is deliberately a free function, not a method on [`Error`] or a
    /// predicate on [`ErrorHierarchy`](crate::common::error::ErrorHierarchy):
    /// fatality is not a property of an exception's *type* (the same class is fatal
    /// in one context and recoverable in another — Streams and the transaction
    /// manager use entirely different notions), so it does not belong in the
    /// `extends`-encoding trait. In Java it is a static with exactly one caller
    /// (`AdminMetadataManager`); this mirrors that shape.
    ///
    /// The two `instanceof` tests on base classes (`AuthenticationException`,
    /// `AuthorizationException`) become the corresponding hierarchy predicates; the
    /// five standalone classes become variant matches, exactly as Java lists them:
    ///
    /// ```java
    /// return e instanceof AuthenticationException ||
    ///     e instanceof AuthorizationException ||
    ///     e instanceof MismatchedEndpointTypeException ||
    ///     e instanceof SecurityDisabledException ||
    ///     e instanceof UnsupportedVersionException ||
    ///     e instanceof UnsupportedEndpointTypeException ||
    ///     e instanceof UnsupportedForMessageFormatException;
    /// ```
    pub fn is_fatal_error(e: &Error) -> bool {
        e.is_authentication_error()
            || e.is_authorization_error()
            || matches!(
                e,
                Error::MismatchedEndpointType(_)
                    | Error::SecurityDisabled(_)
                    | Error::UnsupportedVersion(_)
                    | Error::UnsupportedEndpointType(_)
                    | Error::UnsupportedForMessageFormat(_)
            )
    }

    /// Returns `Some(leader_epoch)` if the given epoch is valid (not
    /// [`RECORD_BATCH_NO_PARTITION_LEADER_EPOCH`]), or `None` otherwise.
    ///
    /// Corresponds to `RequestUtils.getLeaderEpoch` in Java.
    pub fn get_leader_epoch(leader_epoch: i32) -> Option<i32> {
        if leader_epoch == RECORD_BATCH_NO_PARTITION_LEADER_EPOCH {
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
        api_message: &mut impl Message,
        api_version: i16,
    ) -> io::Result<ByteBufferAccessor> {
        let mut cache = ObjectSerializationCache::new();

        let header_size = header.size(&mut cache, header_version)?;
        let message_size = api_message.size(&mut cache, api_version)?;
        let mut writable = ByteBufferAccessor::new(Vec::with_capacity((header_size + message_size) as usize));

        let mut header_clone = header.clone();
        header_clone.write(&mut writable, &cache, header_version)?;
        api_message.write(&mut writable, &cache, api_version)?;

        writable.flip();
        Ok(writable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_leader_epoch_valid() {
        assert_eq!(RequestUtils::get_leader_epoch(5), Some(5));
        assert_eq!(RequestUtils::get_leader_epoch(0), Some(0));
        assert_eq!(RequestUtils::get_leader_epoch(100), Some(100));
    }

    #[test]
    fn test_get_leader_epoch_no_epoch() {
        assert_eq!(RequestUtils::get_leader_epoch(RECORD_BATCH_NO_PARTITION_LEADER_EPOCH), None);
    }
}
