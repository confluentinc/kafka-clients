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

//! WriteTxnMarkers request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.WriteTxnMarkersRequest`.

use std::io;

use crate::WriteTxnMarkersRequestData;
use crate::WriteTxnMarkersResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::write_txn_markers_response_data::{
    WritableTxnMarkerPartitionResult, WritableTxnMarkerResult, WritableTxnMarkerTopicResult,
};

use super::{ConcreteRequest, ConcreteResponse, RequestBuilder, WriteTxnMarkersResponse};

/// A WriteTxnMarkers request.
///
/// Corresponds to `org.apache.kafka.common.requests.WriteTxnMarkersRequest`.
#[derive(Debug, Clone)]
pub struct WriteTxnMarkersRequest {
    data: WriteTxnMarkersRequestData,
    version: i16,
}

impl WriteTxnMarkersRequest {
    /// Creates a new `WriteTxnMarkersRequest` from data and version.
    pub fn new(data: WriteTxnMarkersRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &WriteTxnMarkersRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut WriteTxnMarkersRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::WRITE_TXN_MARKERS
    }

    /// Creates an error response for this request.
    ///
    /// Mirrors `WriteTxnMarkersRequest.getErrorResponse`: every requested
    /// partition of every marker echoes the single error code.
    pub fn get_error_response(&self, _throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut markers = Vec::new();
        for marker_entry in &self.data.markers {
            let mut topics = Vec::new();
            for topic in &marker_entry.topics {
                let mut partitions = Vec::new();
                for &partition_idx in &topic.partition_indexes {
                    let mut partition_result = WritableTxnMarkerPartitionResult::new();
                    partition_result.set_partition_index(partition_idx);
                    partition_result.set_error_code(error.code());
                    partitions.push(partition_result);
                }
                let mut topic_result = WritableTxnMarkerTopicResult::new();
                topic_result.set_name(topic.name.clone());
                topic_result.set_partitions(partitions);
                topics.push(topic_result);
            }
            let mut marker_result = WritableTxnMarkerResult::new();
            marker_result.set_producer_id(marker_entry.producer_id);
            marker_result.set_topics(topics);
            markers.push(marker_result);
        }
        let mut response = WriteTxnMarkersResponseData::new();
        response.set_markers(markers);
        ConcreteResponse::WriteTxnMarkers(WriteTxnMarkersResponse::new(response))
    }

    /// Parses a `WriteTxnMarkersRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = WriteTxnMarkersRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for WriteTxnMarkersRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WriteTxnMarkersRequest(version={}, data={:?})", self.version, self.data)
    }
}

/// Builder for [`WriteTxnMarkersRequest`].
///
/// Corresponds to `WriteTxnMarkersRequest.Builder`.
#[derive(Debug, Clone)]
pub struct WriteTxnMarkersRequestBuilder {
    data: WriteTxnMarkersRequestData,
}

impl WriteTxnMarkersRequestBuilder {
    /// Creates a builder from the given request data.
    pub fn new(data: WriteTxnMarkersRequestData) -> Self {
        Self { data }
    }
}

impl RequestBuilder for WriteTxnMarkersRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::WRITE_TXN_MARKERS
    }

    fn oldest_allowed_version(&self) -> i16 {
        ApiKeys::WRITE_TXN_MARKERS.oldest_version()
    }

    fn latest_allowed_version(&self) -> i16 {
        ApiKeys::WRITE_TXN_MARKERS.latest_version()
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::WriteTxnMarkers(WriteTxnMarkersRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write_txn_markers_request_data::{WritableTxnMarker, WritableTxnMarkerTopic};

    fn marker(producer_id: i64, topic: &str, partition: i32) -> WritableTxnMarker {
        let mut m = WritableTxnMarker::new();
        m.set_producer_id(producer_id);
        m.set_producer_epoch(3);
        m.set_coordinator_epoch(1);
        m.set_transaction_result(false);
        let mut t = WritableTxnMarkerTopic::new();
        t.set_name(topic.to_string());
        t.set_partition_indexes(vec![partition]);
        m.set_topics(vec![t]);
        m
    }

    #[test]
    fn get_error_response_echoes_markers() {
        let mut data = WriteTxnMarkersRequestData::new();
        data.set_markers(vec![marker(42, "foo", 0)]);
        let request = WriteTxnMarkersRequest::new(data, 0);
        let response = request.get_error_response(0, &Errors::NotLeaderOrFollower);
        let ConcreteResponse::WriteTxnMarkers(r) = response else {
            panic!("expected WriteTxnMarkers response");
        };
        assert_eq!(r.data().markers.len(), 1);
        assert_eq!(r.data().markers[0].producer_id, 42);
        assert_eq!(
            r.data().markers[0].topics[0].partitions[0].error_code,
            Errors::NotLeaderOrFollower.code()
        );
    }

    #[test]
    fn serialize_parse_round_trip() {
        let mut data = WriteTxnMarkersRequestData::new();
        data.set_markers(vec![marker(42, "foo", 5)]);
        let mut builder = WriteTxnMarkersRequestBuilder::new(data);
        let mut request = builder.build_version(1).unwrap();
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::new(bytes.into_buffer());
        let parsed = WriteTxnMarkersRequest::parse(&mut readable, 1).unwrap();
        assert_eq!(parsed.data().markers.len(), 1);
        assert_eq!(parsed.data().markers[0].producer_id, 42);
        assert_eq!(parsed.data().markers[0].producer_epoch, 3);
        assert_eq!(parsed.data().markers[0].coordinator_epoch, 1);
        assert_eq!(parsed.data().markers[0].topics[0].name, "foo");
        assert_eq!(parsed.data().markers[0].topics[0].partition_indexes, vec![5]);
    }

    /// Byte-level encoding vector. WriteTxnMarkers v1 is flexible; the body is:
    ///   markers: compact array (len+1 = 0x02)
    ///     producer_id: int64 = 42 (00 00 00 00 00 00 00 2a)
    ///     producer_epoch: int16 = 3 (00 03)
    ///     transaction_result: bool = false (00)
    ///     topics: compact array (len+1 = 0x02)
    ///       name: compact string "foo" (0x04, 0x66 0x6f 0x6f)
    ///       partition_indexes: compact array (len+1 = 0x02) -> [5] (00 00 00 05)
    ///       _tagged_fields: 0x00
    ///     coordinator_epoch: int32 = 1 (00 00 00 01)
    ///     _tagged_fields: 0x00
    ///   _tagged_fields: 0x00
    #[test]
    fn serialize_known_byte_vector_v1() {
        let mut data = WriteTxnMarkersRequestData::new();
        data.set_markers(vec![marker(42, "foo", 5)]);
        let mut builder = WriteTxnMarkersRequestBuilder::new(data);
        let mut request = builder.build_version(1).unwrap();
        let bytes = request.serialize().unwrap();
        let expected: &[u8] = &[
            0x02, // markers array length + 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, // producer_id = 42
            0x00, 0x03, // producer_epoch = 3
            0x00, // transaction_result = false
            0x02, // topics array length + 1
            0x04, 0x66, 0x6f, 0x6f, // name "foo"
            0x02, // partition_indexes array length + 1
            0x00, 0x00, 0x00, 0x05, // partition 5
            0x00, // topic tagged fields
            0x00, 0x00, 0x00, 0x01, // coordinator_epoch = 1
            0x00, // marker tagged fields
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.into_buffer().as_slice(), expected);
    }
}
