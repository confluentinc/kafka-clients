// Copyright 2026 Confluent Inc.
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

//! DescribeTopicPartitions request handling (KIP-966).
//!
//! Corresponds to `org.apache.kafka.common.requests.DescribeTopicPartitionsRequest`.

use std::io;

use crate::DescribeTopicPartitionsRequestData;
use crate::DescribeTopicPartitionsResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::describe_topic_partitions_request_data::TopicRequest;
use crate::describe_topic_partitions_response_data::DescribeTopicPartitionsResponseTopic;

use super::{AbstractRequest, ConcreteResponse, DescribeTopicPartitionsResponse, RequestBuilder};

/// A DescribeTopicPartitions request.
///
/// Corresponds to `org.apache.kafka.common.requests.DescribeTopicPartitionsRequest`.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.DescribeTopicPartitionsRequest")]
pub struct DescribeTopicPartitionsRequest {
    data: DescribeTopicPartitionsRequestData,
    version: i16,
}

impl DescribeTopicPartitionsRequest {
    /// Creates a version-0 request from data.
    ///
    /// Java's `DescribeTopicPartitionsRequest(DescribeTopicPartitionsRequestData)`,
    /// which passes `(short) 0` to `super`.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeTopicPartitionsRequest#DescribeTopicPartitionsRequest")]
    pub fn new(data: DescribeTopicPartitionsRequestData) -> Self {
        Self::with_version(data, 0)
    }

    /// Creates a request from data at the given version.
    ///
    /// Java's `DescribeTopicPartitionsRequest(DescribeTopicPartitionsRequestData, short)`.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeTopicPartitionsRequest#DescribeTopicPartitionsRequest")]
    pub fn with_version(data: DescribeTopicPartitionsRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeTopicPartitionsRequest#data")]
    pub fn data(&self) -> &DescribeTopicPartitionsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DescribeTopicPartitionsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_TOPIC_PARTITIONS
    }

    /// Creates an error response for this request: every requested topic
    /// carries `error`, is not internal and has no partitions.
    ///
    /// Mirrors `DescribeTopicPartitionsRequest.getErrorResponse`.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeTopicPartitionsRequest#getErrorResponse")]
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut response_data = DescribeTopicPartitionsResponseData::new();
        let topics = self
            .data
            .topics
            .iter()
            .map(|topic| {
                let mut response_topic = DescribeTopicPartitionsResponseTopic::new();
                response_topic
                    .set_name(Some(topic.name.clone()))
                    .set_error_code(error.code())
                    .set_is_internal(false)
                    .set_partitions(Vec::new());
                response_topic
            })
            .collect();
        response_data.set_topics(topics);
        response_data.set_throttle_time_ms(throttle_time_ms);
        ConcreteResponse::DescribeTopicPartitions(DescribeTopicPartitionsResponse::new(response_data))
    }

    /// Parses a `DescribeTopicPartitionsRequest` from a readable buffer at the
    /// given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeTopicPartitionsRequest#parse")]
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DescribeTopicPartitionsRequestData::read(readable, version)?;
        Ok(Self::with_version(data, version))
    }
}

impl std::fmt::Display for DescribeTopicPartitionsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DescribeTopicPartitionsRequest(version={}, data={:?})",
            self.version, self.data
        )
    }
}

/// Builder for [`DescribeTopicPartitionsRequest`].
///
/// Corresponds to `DescribeTopicPartitionsRequest.Builder` in Java.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.DescribeTopicPartitionsRequest$Builder")]
pub struct Builder {
    data: DescribeTopicPartitionsRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl Builder {
    /// Creates a builder from existing data.
    ///
    /// Java's `Builder(DescribeTopicPartitionsRequestData)` calls
    /// `super(ApiKeys.DESCRIBE_TOPIC_PARTITIONS)`, which caps the version at the
    /// latest *released* one (`AbstractRequest.java:46-51`), hence
    /// `latest_version_enable_unstable_last_version(false)`
    /// (`producer-transactions.md` §12).
    #[doc(alias = "org.apache.kafka.common.requests.DescribeTopicPartitionsRequest$Builder#Builder")]
    pub fn with_data(data: DescribeTopicPartitionsRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::DESCRIBE_TOPIC_PARTITIONS.oldest_version(),
            latest_allowed_version: ApiKeys::DESCRIBE_TOPIC_PARTITIONS
                .latest_version_enable_unstable_last_version(false),
        }
    }

    /// Creates a builder requesting the given topics.
    ///
    /// Java's `Builder(List<String>)` calls `super(apiKey,
    /// apiKey.oldestVersion(), apiKey.latestVersion())`, so its upper bound is the
    /// unstable-inclusive `latestVersion()`, translated verbatim as
    /// `latest_version()` (`producer-transactions.md` §12). This is deliberate,
    /// not an oversight: it is the bound Java passes.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeTopicPartitionsRequest$Builder#Builder")]
    pub fn with_topics(topics: &[&str]) -> Self {
        let mut data = DescribeTopicPartitionsRequestData::new();
        data.set_topics(
            topics
                .iter()
                .map(|topic_name| {
                    let mut topic = TopicRequest::new();
                    topic.set_name((*topic_name).to_string());
                    topic
                })
                .collect(),
        );
        Self {
            data,
            oldest_allowed_version: ApiKeys::DESCRIBE_TOPIC_PARTITIONS.oldest_version(),
            latest_allowed_version: ApiKeys::DESCRIBE_TOPIC_PARTITIONS.latest_version(),
        }
    }

    /// Returns the request data this builder builds from.
    pub fn data(&self) -> &DescribeTopicPartitionsRequestData {
        &self.data
    }
}

impl std::fmt::Display for Builder {
    /// Java's `Builder.toString()` returns `data.toString()`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.data)
    }
}

impl RequestBuilder for Builder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_TOPIC_PARTITIONS
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<AbstractRequest> {
        Ok(AbstractRequest::DescribeTopicPartitions(
            DescribeTopicPartitionsRequest::with_version(self.data.clone(), version),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::ByteBufferAccessor;
    use crate::describe_topic_partitions_request_data::Cursor;

    /// `RequestResponseTest.createDescribeTopicPartitionsRequest`: one topic
    /// `foo`, cursor `(foo, 1)`, and the default response partition limit.
    fn java_request_data() -> DescribeTopicPartitionsRequestData {
        let mut topic = TopicRequest::new();
        topic.set_name("foo".to_string());
        let mut cursor = Cursor::new();
        cursor.set_topic_name("foo".to_string()).set_partition_index(1);
        let mut data = DescribeTopicPartitionsRequestData::new();
        data.set_topics(vec![topic]).set_cursor(Some(cursor));
        data
    }

    fn serialize(data: DescribeTopicPartitionsRequestData) -> Vec<u8> {
        let mut request = AbstractRequest::DescribeTopicPartitions(DescribeTopicPartitionsRequest::new(data));
        request.serialize().unwrap().into_buffer().as_slice().to_vec()
    }

    /// Byte-level encoding against a known vector: v0 is flexible, so arrays
    /// and strings are compact (length + 1 as an unsigned varint), the nullable
    /// `Cursor` struct is prefixed by an int8 presence marker (1 = present,
    /// -1 = null), and every struct ends in an empty tagged-field section.
    #[test]
    fn serialize_known_byte_vector_v0() {
        let expected: &[u8] = &[
            0x02, // topics: compact array, 1 element
            0x04, b'f', b'o', b'o', // name "foo"
            0x00, // TopicRequest tagged fields
            0x00, 0x00, 0x07, 0xd0, // response_partition_limit = 2000 (the default)
            0x01, // cursor present
            0x04, b'f', b'o', b'o', // cursor.topic_name "foo"
            0x00, 0x00, 0x00, 0x01, // cursor.partition_index = 1
            0x00, // Cursor tagged fields
            0x00, // request tagged fields
        ];
        assert_eq!(serialize(java_request_data()), expected);
    }

    /// The null cursor (the first page) is the single byte `-1`, and a custom
    /// `ResponsePartitionLimit` (Java's `partitionSizeLimitPerResponse`) is
    /// written verbatim.
    #[test]
    fn serialize_known_byte_vector_v0_null_cursor() {
        let mut data = java_request_data();
        data.set_cursor(None).set_response_partition_limit(1);
        let expected: &[u8] = &[
            0x02, 0x04, b'f', b'o', b'o', 0x00, // topics ["foo"]
            0x00, 0x00, 0x00, 0x01, // response_partition_limit = 1
            0xff, // cursor null
            0x00, // request tagged fields
        ];
        assert_eq!(serialize(data), expected);
    }

    /// Decoding the known vector yields the Java fixture back.
    #[test]
    fn parse_known_byte_vector_v0() {
        let bytes: Vec<u8> = vec![
            0x02, 0x04, b'f', b'o', b'o', 0x00, 0x00, 0x00, 0x07, 0xd0, 0x01, 0x04, b'f', b'o', b'o', 0x00, 0x00, 0x00,
            0x01, 0x00, 0x00,
        ];
        let mut readable = ByteBufferAccessor::new(bytes);
        let parsed = DescribeTopicPartitionsRequest::parse(&mut readable, 0).unwrap();
        assert_eq!(parsed.version(), 0);
        assert_eq!(parsed.data(), &java_request_data());
        let cursor = parsed.data().cursor.as_ref().unwrap();
        assert_eq!(cursor.topic_name, "foo");
        assert_eq!(cursor.partition_index, 1);
    }

    /// The `AbstractRequest` arms (`admin-client.md` §7): parsing by API key
    /// reaches this type, and `api_key` / `version` / `get_error_response`
    /// dispatch to it.
    #[test]
    fn concrete_request_arms_dispatch_to_describe_topic_partitions() {
        let bytes = serialize(java_request_data());
        let size = bytes.len();
        let mut readable = ByteBufferAccessor::new(bytes);
        let parsed = AbstractRequest::parse_request(&ApiKeys::DESCRIBE_TOPIC_PARTITIONS, 0, &mut readable).unwrap();
        assert_eq!(parsed.size, size);
        let request = parsed.request;
        assert_eq!(request.api_key(), &ApiKeys::DESCRIBE_TOPIC_PARTITIONS);
        assert_eq!(request.version(), 0);
        let AbstractRequest::DescribeTopicPartitions(inner) = &request else {
            panic!("expected a DescribeTopicPartitions request, got {request}");
        };
        assert_eq!(inner.data(), &java_request_data());
        let Some(ConcreteResponse::DescribeTopicPartitions(response)) =
            request.get_error_response(0, &Errors::UnknownServerError)
        else {
            panic!("expected a DescribeTopicPartitions error response");
        };
        assert_eq!(response.data().topics[0].error_code, Errors::UnknownServerError.code());
    }

    /// `getErrorResponse` fails every requested topic with the error, marks it
    /// not internal, gives it no partitions, and carries the throttle time.
    #[test]
    fn get_error_response_fails_every_topic() {
        let mut data = java_request_data();
        let mut bar = TopicRequest::new();
        bar.set_name("bar".to_string());
        data.topics.push(bar);
        let request = DescribeTopicPartitionsRequest::new(data);
        let ConcreteResponse::DescribeTopicPartitions(response) =
            request.get_error_response(7, &Errors::TopicAuthorizationFailed)
        else {
            panic!("expected a DescribeTopicPartitions response");
        };
        assert_eq!(response.data().throttle_time_ms, 7);
        let names: Vec<_> = response.data().topics.iter().map(|t| t.name.as_deref()).collect();
        assert_eq!(names, vec![Some("foo"), Some("bar")]);
        for topic in &response.data().topics {
            assert_eq!(topic.error_code, Errors::TopicAuthorizationFailed.code());
            assert!(!topic.is_internal);
            assert!(topic.partitions.is_empty());
        }
        assert_eq!(response.data().next_cursor, None);
    }

    /// `Builder(data)` is capped at the latest released version and
    /// `Builder(List<String>)` at `latestVersion()`, exactly as Java's two
    /// `super(...)` calls. Both build the data they were given.
    #[test]
    fn builder_version_bounds_follow_java_super_calls() {
        let mut from_data = Builder::with_data(java_request_data());
        assert_eq!(from_data.oldest_allowed_version(), 0);
        assert_eq!(
            from_data.latest_allowed_version(),
            ApiKeys::DESCRIBE_TOPIC_PARTITIONS.latest_version_enable_unstable_last_version(false)
        );
        let AbstractRequest::DescribeTopicPartitions(built) = from_data.build_version(0).unwrap() else {
            panic!("expected a DescribeTopicPartitions request");
        };
        assert_eq!(built.version(), 0);
        assert_eq!(built.data(), &java_request_data());

        let from_topics = Builder::with_topics(&["a", "b"]);
        assert_eq!(
            from_topics.latest_allowed_version(),
            ApiKeys::DESCRIBE_TOPIC_PARTITIONS.latest_version()
        );
        let names: Vec<_> = from_topics.data().topics.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, vec!["a", "b"]);
        assert_eq!(from_topics.data().response_partition_limit, 2000);
        assert_eq!(from_topics.data().cursor, None);
    }
}
