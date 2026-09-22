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

//! `OffsetDelete` response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.OffsetDeleteResponse`.
//!
//! Possible top-level error codes:
//! - `COORDINATOR_LOAD_IN_PROGRESS`
//! - `COORDINATOR_NOT_AVAILABLE`
//! - `NOT_COORDINATOR`
//! - `GROUP_AUTHORIZATION_FAILED`
//! - `INVALID_GROUP_ID`
//! - `GROUP_ID_NOT_FOUND`
//! - `NON_EMPTY_GROUP`
//!
//! Possible per-partition error codes:
//! - `GROUP_SUBSCRIBED_TO_TOPIC`
//! - `TOPIC_AUTHORIZATION_FAILED`
//! - `UNKNOWN_TOPIC_OR_PARTITION`

use std::collections::HashMap;
use std::io;

use crate::OffsetDeleteResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::AbstractResponse;

/// An `OffsetDelete` response.
///
/// Corresponds to `org.apache.kafka.common.requests.OffsetDeleteResponse`.
#[derive(Debug, Clone)]
pub struct OffsetDeleteResponse {
    data: OffsetDeleteResponseData,
}

impl OffsetDeleteResponse {
    /// Creates a new `OffsetDeleteResponse` from the underlying data.
    ///
    /// Mirrors Java's constructor `OffsetDeleteResponse(OffsetDeleteResponseData)`.
    pub fn new(data: OffsetDeleteResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::OFFSET_DELETE
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &OffsetDeleteResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut OffsetDeleteResponseData {
        &mut self.data
    }

    /// Returns the throttle time in milliseconds.
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Returns the error counts aggregated across the top-level error and all
    /// partition responses. Mirrors Java's `errorCounts`.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        AbstractResponse::update_error_counts(&mut counts, Errors::for_code(self.data.error_code));
        for topic in &self.data.topics {
            for partition in &topic.partitions {
                AbstractResponse::update_error_counts(&mut counts, Errors::for_code(partition.error_code));
            }
        }
        counts
    }

    /// Parses an `OffsetDeleteResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = OffsetDeleteResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Whether the client should throttle on this response (all versions).
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 0
    }
}

impl std::fmt::Display for OffsetDeleteResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offset_delete_response_data::{OffsetDeleteResponsePartition, OffsetDeleteResponseTopic};

    /// `error_counts` aggregates the top-level and partition error codes.
    #[test]
    fn error_counts_aggregates_top_level_and_partitions() {
        let mut data = OffsetDeleteResponseData::new();
        data.set_error_code(Errors::None.code());
        let mut topic = OffsetDeleteResponseTopic::new();
        topic.set_name("t".to_string());
        let mut p0 = OffsetDeleteResponsePartition::new();
        p0.set_partition_index(0);
        p0.set_error_code(Errors::GroupSubscribedToTopic.code());
        let mut p1 = OffsetDeleteResponsePartition::new();
        p1.set_partition_index(1);
        p1.set_error_code(Errors::None.code());
        topic.set_partitions(vec![p0, p1]);
        data.set_topics(vec![topic]);

        let response = OffsetDeleteResponse::new(data);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::GroupSubscribedToTopic).copied().unwrap_or(0), 1);
        // None appears twice: top-level + one partition.
        assert_eq!(counts.get(&Errors::None).copied().unwrap_or(0), 2);
    }

    /// Byte-level wire-decoding check for the v0 (non-flexible) response.
    /// Field-by-field (big-endian):
    ///   error_code 15 -> 0x00 0x0F
    ///   throttle_time_ms 0 -> 0x00 0x00 0x00 0x00
    ///   topics: array len 1 -> 0x00 0x00 0x00 0x01
    ///     name "t": string len 1 -> 0x00 0x01, then 0x74
    ///     partitions: array len 1 -> 0x00 0x00 0x00 0x01
    ///       partition_index 0 -> 0x00 0x00 0x00 0x00
    ///       error_code 0 -> 0x00 0x00
    #[test]
    fn parse_known_byte_vector_v0() {
        let bytes = vec![
            0x00, 0x0F, // error_code 15
            0x00, 0x00, 0x00, 0x00, // throttle_time_ms 0
            0x00, 0x00, 0x00, 0x01, // topics len 1
            0x00, 0x01, 0x74, // name "t"
            0x00, 0x00, 0x00, 0x01, // partitions len 1
            0x00, 0x00, 0x00, 0x00, // partition_index 0
            0x00, 0x00, // error_code 0
        ];
        let mut readable = crate::common::ByteBufferAccessor::new(bytes);
        let response = OffsetDeleteResponse::parse(&mut readable, 0).unwrap();
        assert_eq!(response.data().error_code, Errors::CoordinatorNotAvailable.code());
        assert_eq!(response.data().topics.len(), 1);
        assert_eq!(response.data().topics[0].name, "t");
        assert_eq!(response.data().topics[0].partitions[0].partition_index, 0);
        assert_eq!(response.data().topics[0].partitions[0].error_code, Errors::None.code());
    }

    /// `should_client_throttle` returns true for all versions.
    #[test]
    fn should_client_throttle_all_versions() {
        let response = OffsetDeleteResponse::new(OffsetDeleteResponseData::new());
        assert!(response.should_client_throttle(0));
    }

    /// `maybe_set_throttle_time_ms` propagates the new value.
    #[test]
    fn maybe_set_throttle_time_ms_updates_field() {
        let mut response = OffsetDeleteResponse::new(OffsetDeleteResponseData::new());
        response.maybe_set_throttle_time_ms(123);
        assert_eq!(response.throttle_time_ms(), 123);
    }
}
