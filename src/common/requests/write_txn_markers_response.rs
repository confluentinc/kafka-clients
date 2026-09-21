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

//! WriteTxnMarkers response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.WriteTxnMarkersResponse`.

use std::collections::HashMap;
use std::io;

use crate::WriteTxnMarkersResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::AbstractResponse;

/// A WriteTxnMarkers response.
///
/// Corresponds to `org.apache.kafka.common.requests.WriteTxnMarkersResponse`.
#[derive(Debug, Clone)]
pub struct WriteTxnMarkersResponse {
    data: WriteTxnMarkersResponseData,
}

impl WriteTxnMarkersResponse {
    /// Creates a new `WriteTxnMarkersResponse` from the underlying data.
    pub fn new(data: WriteTxnMarkersResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::WRITE_TXN_MARKERS
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &WriteTxnMarkersResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut WriteTxnMarkersResponseData {
        &mut self.data
    }

    /// Returns the throttle time in milliseconds.
    ///
    /// `WriteTxnMarkersResponse` has no throttle time field in its schema, so
    /// Java returns `DEFAULT_THROTTLE_TIME` (0).
    pub fn throttle_time_ms(&self) -> i32 {
        0
    }

    /// Sets the throttle time in the response.
    ///
    /// The response schema does not support a throttle time, so this is a no-op
    /// (mirrors Java's overridden `maybeSetThrottleTimeMs`).
    pub fn maybe_set_throttle_time_ms(&mut self, _throttle_time_ms: i32) {}

    /// Whether the client should throttle on this response.
    ///
    /// `WriteTxnMarkersResponse` does not override `shouldClientThrottle` in
    /// Java, so it inherits `AbstractResponse`'s default of `false`.
    pub fn should_client_throttle(&self, _version: i16) -> bool {
        false
    }

    /// Returns the error counts aggregated across all partition results.
    ///
    /// Mirrors `WriteTxnMarkersResponse.errorCounts`.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        for marker in &self.data.markers {
            for topic in &marker.topics {
                for partition in &topic.partitions {
                    AbstractResponse::update_error_counts(&mut counts, Errors::for_code(partition.error_code));
                }
            }
        }
        counts
    }

    /// Parses a `WriteTxnMarkersResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = WriteTxnMarkersResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }
}

impl std::fmt::Display for WriteTxnMarkersResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WriteTxnMarkersResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write_txn_markers_response_data::{
        WritableTxnMarkerPartitionResult, WritableTxnMarkerResult, WritableTxnMarkerTopicResult,
    };

    fn marker_result(producer_id: i64, error: Errors) -> WritableTxnMarkerResult {
        let mut p = WritableTxnMarkerPartitionResult::new();
        p.set_partition_index(0);
        p.set_error_code(error.code());
        let mut t = WritableTxnMarkerTopicResult::new();
        t.set_name("foo".to_string());
        t.set_partitions(vec![p]);
        let mut m = WritableTxnMarkerResult::new();
        m.set_producer_id(producer_id);
        m.set_topics(vec![t]);
        m
    }

    #[test]
    fn error_counts_aggregates_partitions() {
        let mut data = WriteTxnMarkersResponseData::new();
        data.set_markers(vec![marker_result(42, Errors::InvalidProducerEpoch)]);
        let response = WriteTxnMarkersResponse::new(data);
        assert_eq!(response.error_counts().get(&Errors::InvalidProducerEpoch), Some(&1));
    }

    #[test]
    fn serialize_parse_round_trip() {
        let mut data = WriteTxnMarkersResponseData::new();
        data.set_markers(vec![marker_result(42, Errors::None)]);
        let mut concrete = super::super::ConcreteResponse::WriteTxnMarkers(WriteTxnMarkersResponse::new(data));
        let bytes = concrete.serialize(1).unwrap();
        let mut readable = crate::common::ByteBufferAccessor::new(bytes.into_buffer());
        let parsed = WriteTxnMarkersResponse::parse(&mut readable, 1).unwrap();
        assert_eq!(parsed.data().markers.len(), 1);
        assert_eq!(parsed.data().markers[0].producer_id, 42);
        assert_eq!(parsed.data().markers[0].topics[0].name, "foo");
    }

    /// Byte-level encoding vector. WriteTxnMarkers response v1 is flexible:
    ///   markers: compact array (len+1 = 0x02)
    ///     producer_id: int64 = 42 (00 00 00 00 00 00 00 2a)
    ///     topics: compact array (len+1 = 0x02)
    ///       name: compact string "foo" (0x04, 0x66 0x6f 0x6f)
    ///       partitions: compact array (len+1 = 0x02)
    ///         partition_index: int32 = 0 (00 00 00 00)
    ///         error_code: int16 = 0 (00 00)
    ///         _tagged_fields: 0x00
    ///       _tagged_fields: 0x00
    ///     _tagged_fields: 0x00
    ///   _tagged_fields: 0x00
    #[test]
    fn serialize_known_byte_vector_v1() {
        let mut data = WriteTxnMarkersResponseData::new();
        data.set_markers(vec![marker_result(42, Errors::None)]);
        let mut concrete = super::super::ConcreteResponse::WriteTxnMarkers(WriteTxnMarkersResponse::new(data));
        let bytes = concrete.serialize(1).unwrap();
        let expected: &[u8] = &[
            0x02, // markers array length + 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, // producer_id = 42
            0x02, // topics array length + 1
            0x04, 0x66, 0x6f, 0x6f, // name "foo"
            0x02, // partitions array length + 1
            0x00, 0x00, 0x00, 0x00, // partition_index = 0
            0x00, 0x00, // error_code = 0
            0x00, // partition tagged fields
            0x00, // topic tagged fields
            0x00, // marker tagged fields
            0x00, // response tagged fields
        ];
        assert_eq!(bytes.into_buffer().as_slice(), expected);
    }
}
