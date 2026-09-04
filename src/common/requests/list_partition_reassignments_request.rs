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

//! ListPartitionReassignments request handling.
//!
//! Corresponds to
//! `org.apache.kafka.common.requests.ListPartitionReassignmentsRequest`.

use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::list_partition_reassignments_request_data::ListPartitionReassignmentsRequestData;
use crate::list_partition_reassignments_response_data::{
    ListPartitionReassignmentsResponseData, OngoingPartitionReassignment, OngoingTopicReassignment,
};

use super::{ConcreteRequest, ConcreteResponse, ListPartitionReassignmentsResponse, RequestBuilder};

/// A ListPartitionReassignments request.
///
/// Corresponds to
/// `org.apache.kafka.common.requests.ListPartitionReassignmentsRequest`.
#[derive(Debug, Clone)]
pub struct ListPartitionReassignmentsRequest {
    data: ListPartitionReassignmentsRequestData,
    version: i16,
}

impl ListPartitionReassignmentsRequest {
    /// Creates a new `ListPartitionReassignmentsRequest` from data and version.
    pub fn new(data: ListPartitionReassignmentsRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ListPartitionReassignmentsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ListPartitionReassignmentsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::LIST_PARTITION_REASSIGNMENTS
    }

    /// Creates an error response for this request.
    ///
    /// Mirrors `ListPartitionReassignmentsRequest.getErrorResponse`, echoing the
    /// requested topics/partitions with the top-level error code.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut topics = Vec::new();
        if let Some(request_topics) = &self.data.topics {
            for topic in request_topics {
                let mut ongoing = OngoingTopicReassignment::new();
                ongoing.set_name(topic.name.clone());
                ongoing.set_partitions(
                    topic
                        .partition_indexes
                        .iter()
                        .map(|partition_index| {
                            let mut p = OngoingPartitionReassignment::new();
                            p.set_partition_index(*partition_index);
                            p
                        })
                        .collect(),
                );
                topics.push(ongoing);
            }
        }
        let mut data = ListPartitionReassignmentsResponseData::new();
        data.set_topics(topics);
        data.set_error_code(error.code());
        data.set_throttle_time_ms(throttle_time_ms);
        ConcreteResponse::ListPartitionReassignments(ListPartitionReassignmentsResponse::new(data))
    }

    /// Parses a `ListPartitionReassignmentsRequest` from a readable buffer at the
    /// given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ListPartitionReassignmentsRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for ListPartitionReassignmentsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ListPartitionReassignmentsRequest(version={}, data={:?})",
            self.version, self.data
        )
    }
}

/// Builder for [`ListPartitionReassignmentsRequest`].
///
/// Corresponds to `ListPartitionReassignmentsRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct ListPartitionReassignmentsRequestBuilder {
    data: ListPartitionReassignmentsRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl ListPartitionReassignmentsRequestBuilder {
    /// Creates a builder from existing data.
    pub fn new(data: ListPartitionReassignmentsRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::LIST_PARTITION_REASSIGNMENTS.oldest_version(),
            latest_allowed_version: ApiKeys::LIST_PARTITION_REASSIGNMENTS.latest_version(),
        }
    }
}

impl RequestBuilder for ListPartitionReassignmentsRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::LIST_PARTITION_REASSIGNMENTS
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::ListPartitionReassignments(
            ListPartitionReassignmentsRequest::new(self.data.clone(), version),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::list_partition_reassignments_request_data::ListPartitionReassignmentsTopics;

    fn topic(name: &str, partitions: Vec<i32>) -> ListPartitionReassignmentsTopics {
        let mut t = ListPartitionReassignmentsTopics::new();
        t.set_name(name.to_string());
        t.set_partition_indexes(partitions);
        t
    }

    #[test]
    fn get_error_response_echoes_topics() {
        let mut data = ListPartitionReassignmentsRequestData::new();
        data.set_topics(Some(vec![topic("A", vec![0, 1])]));
        let request = ListPartitionReassignmentsRequest::new(data, 0);
        let response = request.get_error_response(20, &Errors::UnknownTopicOrPartition);
        if let ConcreteResponse::ListPartitionReassignments(r) = response {
            assert_eq!(r.data().throttle_time_ms, 20);
            assert_eq!(r.data().error_code, Errors::UnknownTopicOrPartition.code());
            assert_eq!(r.data().topics.len(), 1);
            assert_eq!(r.data().topics[0].partitions.len(), 2);
        } else {
            panic!("expected ListPartitionReassignments response");
        }
    }

    /// Round-trips a request through the shared `ConcreteRequest` serialize /
    /// parse path.
    #[test]
    fn serialize_parse_round_trip() {
        let mut data = ListPartitionReassignmentsRequestData::new();
        data.set_timeout_ms(30000);
        data.set_topics(Some(vec![topic("A", vec![0, 2])]));
        let mut request = ConcreteRequest::ListPartitionReassignments(ListPartitionReassignmentsRequest::new(data, 0));
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes.into_buffer());
        let parsed = ListPartitionReassignmentsRequest::parse(&mut readable, 0).unwrap();
        assert_eq!(parsed.data().timeout_ms, 30000);
        let topics = parsed.data().topics.as_ref().unwrap();
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].name, "A");
        assert_eq!(topics[0].partition_indexes, vec![0, 2]);
    }

    /// Byte-level encoding test against a known vector. ListPartitionReassignments
    /// v0 is a flexible version, so the body is:
    ///   timeout_ms: int32 = 100 (00 00 00 64)
    ///   topics: compact array (len+1 = 0x02)
    ///     name: compact string "A" (0x02, 0x41)
    ///     partition_indexes: compact int32 array (len+1 = 0x02), partition 0 (00 00 00 00)
    ///     _tagged_fields: 0x00
    ///   _tagged_fields: 0x00
    #[test]
    fn serialize_known_byte_vector_v0() {
        let mut data = ListPartitionReassignmentsRequestData::new();
        data.set_timeout_ms(100);
        data.set_topics(Some(vec![topic("A", vec![0])]));
        let mut request = ConcreteRequest::ListPartitionReassignments(ListPartitionReassignmentsRequest::new(data, 0));
        let bytes = request.serialize().unwrap();
        let expected: &[u8] = &[
            0x00, 0x00, 0x00, 0x64, // timeout_ms = 100
            0x02, // topics array length + 1
            0x02, 0x41, // name "A"
            0x02, // partition_indexes array length + 1
            0x00, 0x00, 0x00, 0x00, // partition 0
            0x00, // topic tagged fields
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.into_buffer().as_slice(), expected);
    }
}
