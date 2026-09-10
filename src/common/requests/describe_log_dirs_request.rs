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

//! DescribeLogDirs request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.DescribeLogDirsRequest`.

use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::describe_log_dirs_request_data::DescribeLogDirsRequestData;
use crate::describe_log_dirs_response_data::DescribeLogDirsResponseData;

use super::{ConcreteRequest, ConcreteResponse, DescribeLogDirsResponse, RequestBuilder};

/// A DescribeLogDirs request.
///
/// Corresponds to `org.apache.kafka.common.requests.DescribeLogDirsRequest`.
#[derive(Debug, Clone)]
pub struct DescribeLogDirsRequest {
    data: DescribeLogDirsRequestData,
    version: i16,
}

impl DescribeLogDirsRequest {
    /// Creates a new `DescribeLogDirsRequest` from data and version.
    pub fn new(data: DescribeLogDirsRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DescribeLogDirsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DescribeLogDirsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_LOG_DIRS
    }

    /// Whether the request asks for all topic partitions in all log directories.
    ///
    /// Mirrors `DescribeLogDirsRequest.isAllTopicPartitions` (topics == null).
    pub fn is_all_topic_partitions(&self) -> bool {
        self.data.topics.is_none()
    }

    /// Creates an error response for this request.
    ///
    /// Mirrors `DescribeLogDirsRequest.getErrorResponse`, which sets the
    /// top-level error code (v3+) to the mapped error and echoes the throttle
    /// time.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut response = DescribeLogDirsResponseData::new();
        response.set_throttle_time_ms(throttle_time_ms);
        response.set_error_code(error.code());
        ConcreteResponse::DescribeLogDirs(DescribeLogDirsResponse::new(response))
    }

    /// Parses a `DescribeLogDirsRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DescribeLogDirsRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for DescribeLogDirsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DescribeLogDirsRequest(version={}, data={:?})", self.version, self.data)
    }
}

/// Builder for [`DescribeLogDirsRequest`].
///
/// Corresponds to `DescribeLogDirsRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct DescribeLogDirsRequestBuilder {
    data: DescribeLogDirsRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl DescribeLogDirsRequestBuilder {
    /// Creates a builder from existing data.
    pub fn new(data: DescribeLogDirsRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::DESCRIBE_LOG_DIRS.oldest_version(),
            latest_allowed_version: ApiKeys::DESCRIBE_LOG_DIRS.latest_version(),
        }
    }
}

impl RequestBuilder for DescribeLogDirsRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_LOG_DIRS
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::DescribeLogDirs(DescribeLogDirsRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::describe_log_dirs_request_data::DescribableLogDirTopic;

    fn topic(name: &str, partitions: Vec<i32>) -> DescribableLogDirTopic {
        let mut t = DescribableLogDirTopic::new();
        t.set_topic(name.to_string());
        t.set_partitions(partitions);
        t
    }

    #[test]
    fn is_all_topic_partitions_when_topics_null() {
        let data = DescribeLogDirsRequestData::new();
        let request = DescribeLogDirsRequest::new(data, 2);
        assert!(request.is_all_topic_partitions());

        let mut data = DescribeLogDirsRequestData::new();
        data.set_topics(Some(vec![topic("t", vec![0])]));
        let request = DescribeLogDirsRequest::new(data, 2);
        assert!(!request.is_all_topic_partitions());
    }

    #[test]
    fn get_error_response_sets_top_level_error() {
        let data = DescribeLogDirsRequestData::new();
        let request = DescribeLogDirsRequest::new(data, 4);
        let response = request.get_error_response(123, &Errors::ClusterAuthorizationFailed);
        if let ConcreteResponse::DescribeLogDirs(r) = response {
            assert_eq!(r.data().throttle_time_ms, 123);
            assert_eq!(r.data().error_code, Errors::ClusterAuthorizationFailed.code());
        } else {
            panic!("expected DescribeLogDirs response");
        }
    }

    /// Round-trips a request through the shared `ConcreteRequest` serialize /
    /// parse path, exercising the enum wiring end-to-end.
    #[test]
    fn serialize_parse_round_trip() {
        let mut data = DescribeLogDirsRequestData::new();
        data.set_topics(Some(vec![topic("round-trip-topic", vec![1, 2, 3])]));
        let mut request = ConcreteRequest::DescribeLogDirs(DescribeLogDirsRequest::new(data, 2));
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes.into_buffer());
        let parsed = DescribeLogDirsRequest::parse(&mut readable, 2).unwrap();
        let topics = parsed.data().topics.as_ref().unwrap();
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].topic, "round-trip-topic");
        assert_eq!(topics[0].partitions, vec![1, 2, 3]);
    }

    /// Byte-level encoding test against a known vector. DescribeLogDirs v2 is a
    /// flexible version, so the body is:
    ///   topics: compact nullable array (len+1 = 0x02)
    ///     topic: compact string "t" (len+1 = 0x02, 0x74)
    ///     partitions: compact array [0] (len+1 = 0x02, 0x00 00 00 00)
    ///     _tagged_fields: 0x00
    ///   _tagged_fields: 0x00
    #[test]
    fn serialize_known_byte_vector_v2() {
        let mut data = DescribeLogDirsRequestData::new();
        data.set_topics(Some(vec![topic("t", vec![0])]));
        let mut request = ConcreteRequest::DescribeLogDirs(DescribeLogDirsRequest::new(data, 2));
        let bytes = request.serialize().unwrap();
        let expected: &[u8] = &[
            0x02, // topics array length + 1
            0x02, 0x74, // topic "t"
            0x02, // partitions array length + 1
            0x00, 0x00, 0x00, 0x00, // partition 0
            0x00, // topic tagged fields
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.into_buffer().as_slice(), expected);
    }

    /// Byte-level encoding of the `topics == null` (all-partitions) request at
    /// v2. The compact nullable array encodes null as a single 0x00 byte.
    #[test]
    fn serialize_known_byte_vector_all_partitions_v2() {
        let mut data = DescribeLogDirsRequestData::new();
        data.set_topics(None);
        let mut request = ConcreteRequest::DescribeLogDirs(DescribeLogDirsRequest::new(data, 2));
        let bytes = request.serialize().unwrap();
        let expected: &[u8] = &[
            0x00, // topics = null
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.into_buffer().as_slice(), expected);
    }
}
