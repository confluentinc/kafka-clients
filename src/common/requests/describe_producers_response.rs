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

//! DescribeProducers response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.DescribeProducersResponse`.

use std::collections::HashMap;
use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::describe_producers_response_data::DescribeProducersResponseData;

use super::abstract_response::update_error_counts;

/// A DescribeProducers response.
///
/// Corresponds to `org.apache.kafka.common.requests.DescribeProducersResponse`.
#[derive(Debug, Clone)]
pub struct DescribeProducersResponse {
    data: DescribeProducersResponseData,
}

impl DescribeProducersResponse {
    /// Creates a new `DescribeProducersResponse` from the underlying data.
    pub fn new(data: DescribeProducersResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_PRODUCERS
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DescribeProducersResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DescribeProducersResponseData {
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

    /// Whether the client should throttle on this response.
    ///
    /// `DescribeProducersResponse` does not override `shouldClientThrottle` in
    /// Java, so it inherits `AbstractResponse`'s default of `false`.
    pub fn should_client_throttle(&self, _version: i16) -> bool {
        false
    }

    /// Returns the error counts aggregated across all partition responses.
    ///
    /// Mirrors `DescribeProducersResponse.errorCounts`.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        for topic in &self.data.topics {
            for partition in &topic.partitions {
                update_error_counts(&mut counts, Errors::for_code(partition.error_code));
            }
        }
        counts
    }

    /// Parses a `DescribeProducersResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DescribeProducersResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }
}

impl std::fmt::Display for DescribeProducersResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DescribeProducersResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::describe_producers_response_data::{PartitionResponse, TopicResponse};

    #[test]
    fn error_counts_aggregates_partitions() {
        let mut topic = TopicResponse::new();
        topic.set_name("foo".to_string());
        let mut p0 = PartitionResponse::new();
        p0.set_partition_index(0);
        p0.set_error_code(Errors::None.code());
        let mut p1 = PartitionResponse::new();
        p1.set_partition_index(1);
        p1.set_error_code(Errors::NotLeaderOrFollower.code());
        topic.set_partitions(vec![p0, p1]);
        let mut data = DescribeProducersResponseData::new();
        data.set_topics(vec![topic]);
        let response = DescribeProducersResponse::new(data);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::None), Some(&1));
        assert_eq!(counts.get(&Errors::NotLeaderOrFollower), Some(&1));
    }

    #[test]
    fn serialize_parse_round_trip() {
        let mut data = DescribeProducersResponseData::new();
        data.set_throttle_time_ms(7);
        let mut concrete = super::super::ConcreteResponse::DescribeProducers(DescribeProducersResponse::new(data));
        let bytes = concrete.serialize(0).unwrap();
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes.into_buffer());
        let parsed = DescribeProducersResponse::parse(&mut readable, 0).unwrap();
        assert_eq!(parsed.data().throttle_time_ms, 7);
    }

    /// Byte-level encoding vector. DescribeProducers response v0 is flexible:
    ///   throttle_time_ms: int32 = 7 (00 00 00 07)
    ///   topics: compact array (empty -> 0x01)
    ///   _tagged_fields: 0x00
    #[test]
    fn serialize_known_byte_vector_v0() {
        let mut data = DescribeProducersResponseData::new();
        data.set_throttle_time_ms(7);
        let mut concrete = super::super::ConcreteResponse::DescribeProducers(DescribeProducersResponse::new(data));
        let bytes = concrete.serialize(0).unwrap();
        let expected: &[u8] = &[
            0x00, 0x00, 0x00, 0x07, // throttle_time_ms = 7
            0x01, // topics array (empty)
            0x00, // response tagged fields
        ];
        assert_eq!(bytes.into_buffer().as_slice(), expected);
    }
}
