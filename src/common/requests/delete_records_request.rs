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

//! DeleteRecords request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.DeleteRecordsRequest`.

use std::io;

use crate::DeleteRecordsRequestData;
use crate::DeleteRecordsResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::delete_records_response_data::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult};

use super::{ConcreteRequest, ConcreteResponse, DeleteRecordsResponse, RequestBuilder};

/// A DeleteRecords request.
///
/// Corresponds to `org.apache.kafka.common.requests.DeleteRecordsRequest`.
#[derive(Debug, Clone)]
pub struct DeleteRecordsRequest {
    data: DeleteRecordsRequestData,
    version: i16,
}

impl DeleteRecordsRequest {
    /// Creates a new `DeleteRecordsRequest` from data and version.
    pub fn new(data: DeleteRecordsRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DeleteRecordsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DeleteRecordsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DELETE_RECORDS
    }

    /// Creates an error response for this request, failing every requested
    /// partition with the given error.
    ///
    /// Mirrors `DeleteRecordsRequest.getErrorResponse` (which uses
    /// `Errors.forException`); the enum-dispatch caller supplies the mapped
    /// [`Errors`] directly.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut result = DeleteRecordsResponseData::new();
        result.set_throttle_time_ms(throttle_time_ms);
        let mut topics = Vec::new();
        for topic in &self.data.topics {
            let mut topic_result = DeleteRecordsTopicResult::new();
            topic_result.set_name(topic.name.clone());
            let mut partitions = Vec::new();
            for partition in &topic.partitions {
                let mut partition_result = DeleteRecordsPartitionResult::new();
                partition_result.set_partition_index(partition.partition_index);
                partition_result.set_error_code(error.code());
                partition_result.set_low_watermark(DeleteRecordsResponse::INVALID_LOW_WATERMARK);
                partitions.push(partition_result);
            }
            topic_result.set_partitions(partitions);
            topics.push(topic_result);
        }
        result.set_topics(topics);
        ConcreteResponse::DeleteRecords(DeleteRecordsResponse::new(result))
    }

    /// Parses a `DeleteRecordsRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DeleteRecordsRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for DeleteRecordsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeleteRecordsRequest(version={}, data={:?})", self.version, self.data)
    }
}

/// Builder for [`DeleteRecordsRequest`].
///
/// Corresponds to `DeleteRecordsRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct DeleteRecordsRequestBuilder {
    data: DeleteRecordsRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl DeleteRecordsRequestBuilder {
    /// Creates a builder from existing data.
    pub fn new(data: DeleteRecordsRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::DELETE_RECORDS.oldest_version(),
            latest_allowed_version: ApiKeys::DELETE_RECORDS.latest_version(),
        }
    }
}

impl RequestBuilder for DeleteRecordsRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DELETE_RECORDS
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::DeleteRecords(DeleteRecordsRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delete_records_request_data::{DeleteRecordsPartition, DeleteRecordsTopic};

    fn topic(name: &str, partitions: &[(i32, i64)]) -> DeleteRecordsTopic {
        let mut t = DeleteRecordsTopic::new();
        t.set_name(name.to_string());
        t.set_partitions(
            partitions
                .iter()
                .map(|(idx, offset)| {
                    let mut p = DeleteRecordsPartition::new();
                    p.set_partition_index(*idx);
                    p.set_offset(*offset);
                    p
                })
                .collect(),
        );
        t
    }

    #[test]
    fn get_error_response_fails_every_partition() {
        let mut data = DeleteRecordsRequestData::new();
        data.set_topics(vec![topic("t", &[(0, 10), (1, 20)])]);
        let request = DeleteRecordsRequest::new(data, 2);
        let response = request.get_error_response(100, &Errors::NotLeaderOrFollower);
        if let ConcreteResponse::DeleteRecords(r) = response {
            assert_eq!(r.data().throttle_time_ms, 100);
            assert_eq!(r.data().topics.len(), 1);
            assert_eq!(r.data().topics[0].partitions.len(), 2);
            for partition in &r.data().topics[0].partitions {
                assert_eq!(partition.error_code, Errors::NotLeaderOrFollower.code());
                assert_eq!(partition.low_watermark, DeleteRecordsResponse::INVALID_LOW_WATERMARK);
            }
        } else {
            panic!("expected DeleteRecords response");
        }
    }

    /// Round-trips a request through the shared `ConcreteRequest` serialize /
    /// parse path, exercising the enum wiring end-to-end.
    #[test]
    fn serialize_parse_round_trip() {
        let mut data = DeleteRecordsRequestData::new();
        data.set_topics(vec![topic("round-trip-topic", &[(0, 42), (3, 100)])]);
        data.set_timeout_ms(30000);
        let mut request = ConcreteRequest::DeleteRecords(DeleteRecordsRequest::new(data, 2));
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::new(bytes.into_buffer());
        let parsed = DeleteRecordsRequest::parse(&mut readable, 2).unwrap();
        assert_eq!(parsed.data().topics.len(), 1);
        assert_eq!(parsed.data().topics[0].name, "round-trip-topic");
        assert_eq!(parsed.data().topics[0].partitions.len(), 2);
        assert_eq!(parsed.data().topics[0].partitions[0].partition_index, 0);
        assert_eq!(parsed.data().topics[0].partitions[0].offset, 42);
        assert_eq!(parsed.data().topics[0].partitions[1].partition_index, 3);
        assert_eq!(parsed.data().topics[0].partitions[1].offset, 100);
        assert_eq!(parsed.data().timeout_ms, 30000);
    }

    /// Byte-level encoding test against a known vector. DeleteRecords v2 is a
    /// flexible version, so the body is:
    ///   topics: compact array (len+1 = 0x02)
    ///     name: compact string "t" (0x02, 0x74)
    ///     partitions: compact array (len+1 = 0x02)
    ///       partition_index: int32 = 0 (00 00 00 00)
    ///       offset: int64 = 5 (00 00 00 00 00 00 00 05)
    ///       _tagged_fields: 0x00
    ///     _tagged_fields: 0x00
    ///   timeout_ms: int32 = 100 (00 00 00 64)
    ///   _tagged_fields: 0x00
    #[test]
    fn serialize_known_byte_vector_v2() {
        let mut data = DeleteRecordsRequestData::new();
        data.set_topics(vec![topic("t", &[(0, 5)])]);
        data.set_timeout_ms(100);
        let mut request = ConcreteRequest::DeleteRecords(DeleteRecordsRequest::new(data, 2));
        let bytes = request.serialize().unwrap();
        let expected: &[u8] = &[
            0x02, // topics array length + 1
            0x02, 0x74, // name "t"
            0x02, // partitions array length + 1
            0x00, 0x00, 0x00, 0x00, // partition_index = 0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, // offset = 5
            0x00, // partition tagged fields
            0x00, // topic tagged fields
            0x00, 0x00, 0x00, 0x64, // timeout_ms = 100
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.into_buffer().as_slice(), expected);
    }
}
