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

//! DescribeProducers request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.DescribeProducersRequest`.

use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::describe_producers_request_data::DescribeProducersRequestData;
use crate::describe_producers_response_data::{DescribeProducersResponseData, PartitionResponse, TopicResponse};

use super::{ConcreteRequest, ConcreteResponse, DescribeProducersResponse, RequestBuilder};

/// A DescribeProducers request.
///
/// Corresponds to `org.apache.kafka.common.requests.DescribeProducersRequest`.
#[derive(Debug, Clone)]
pub struct DescribeProducersRequest {
    data: DescribeProducersRequestData,
    version: i16,
}

impl DescribeProducersRequest {
    /// Creates a new `DescribeProducersRequest` from data and version.
    pub fn new(data: DescribeProducersRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DescribeProducersRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DescribeProducersRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_PRODUCERS
    }

    /// Creates an error response for this request.
    ///
    /// Mirrors `DescribeProducersRequest.getErrorResponse`.
    pub fn get_error_response(&self, _throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut response = DescribeProducersResponseData::new();
        let mut topics = Vec::new();
        for topic_request in &self.data.topics {
            let mut topic_response = TopicResponse::new();
            topic_response.set_name(topic_request.name.clone());
            let mut partitions = Vec::new();
            for &partition_id in &topic_request.partition_indexes {
                let mut partition_response = PartitionResponse::new();
                partition_response.set_partition_index(partition_id);
                partition_response.set_error_code(error.code());
                partitions.push(partition_response);
            }
            topic_response.set_partitions(partitions);
            topics.push(topic_response);
        }
        response.set_topics(topics);
        ConcreteResponse::DescribeProducers(DescribeProducersResponse::new(response))
    }

    /// Parses a `DescribeProducersRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DescribeProducersRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for DescribeProducersRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DescribeProducersRequest(version={}, data={:?})", self.version, self.data)
    }
}

/// Builder for [`DescribeProducersRequest`].
///
/// Corresponds to `DescribeProducersRequest.Builder`.
#[derive(Debug, Clone)]
pub struct DescribeProducersRequestBuilder {
    data: DescribeProducersRequestData,
}

impl DescribeProducersRequestBuilder {
    /// Creates a builder from the given request data.
    pub fn new(data: DescribeProducersRequestData) -> Self {
        Self { data }
    }
}

impl RequestBuilder for DescribeProducersRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_PRODUCERS
    }

    fn oldest_allowed_version(&self) -> i16 {
        ApiKeys::DESCRIBE_PRODUCERS.oldest_version()
    }

    fn latest_allowed_version(&self) -> i16 {
        ApiKeys::DESCRIBE_PRODUCERS.latest_version()
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::DescribeProducers(DescribeProducersRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::describe_producers_request_data::TopicRequest;

    fn topic(name: &str, partitions: Vec<i32>) -> TopicRequest {
        let mut t = TopicRequest::new();
        t.set_name(name.to_string());
        t.set_partition_indexes(partitions);
        t
    }

    #[test]
    fn get_error_response_echoes_topics_and_partitions() {
        let mut data = DescribeProducersRequestData::new();
        data.set_topics(vec![topic("foo", vec![0, 1])]);
        let request = DescribeProducersRequest::new(data, 0);
        let response = request.get_error_response(0, &Errors::NotLeaderOrFollower);
        let ConcreteResponse::DescribeProducers(r) = response else {
            panic!("expected DescribeProducers response");
        };
        assert_eq!(r.data().topics.len(), 1);
        assert_eq!(r.data().topics[0].partitions.len(), 2);
        for partition in &r.data().topics[0].partitions {
            assert_eq!(partition.error_code, Errors::NotLeaderOrFollower.code());
        }
    }

    #[test]
    fn serialize_parse_round_trip() {
        let mut data = DescribeProducersRequestData::new();
        data.set_topics(vec![topic("foo", vec![0, 3])]);
        let mut builder = DescribeProducersRequestBuilder::new(data);
        let mut request = builder.build_version(0).unwrap();
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes.into_buffer());
        let parsed = DescribeProducersRequest::parse(&mut readable, 0).unwrap();
        assert_eq!(parsed.data().topics.len(), 1);
        assert_eq!(parsed.data().topics[0].name, "foo");
        assert_eq!(parsed.data().topics[0].partition_indexes, vec![0, 3]);
    }

    /// Byte-level encoding test against a known vector. DescribeProducers v0 is
    /// flexible; the body is:
    ///   topics: compact array (len+1 = 0x02)
    ///     name: compact string "foo" (0x04, 0x66 0x6f 0x6f)
    ///     partition_indexes: compact array (len+1 = 0x02) -> [0] (00 00 00 00)
    ///     _tagged_fields: 0x00
    ///   _tagged_fields: 0x00
    #[test]
    fn serialize_known_byte_vector_v0() {
        let mut data = DescribeProducersRequestData::new();
        data.set_topics(vec![topic("foo", vec![0])]);
        let mut builder = DescribeProducersRequestBuilder::new(data);
        let mut request = builder.build_version(0).unwrap();
        let bytes = request.serialize().unwrap();
        let expected: &[u8] = &[
            0x02, // topics array length + 1
            0x04, 0x66, 0x6f, 0x6f, // name "foo"
            0x02, // partition_indexes array length + 1
            0x00, 0x00, 0x00, 0x00, // partition 0
            0x00, // topic tagged fields
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.into_buffer().as_slice(), expected);
    }
}
