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

//! CreateTopics request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.CreateTopicsRequest`.

use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::create_topics_request_data::CreateTopicsRequestData;
use crate::create_topics_response_data::{CreatableTopicResult, CreateTopicsResponseData};

use super::{ConcreteRequest, ConcreteResponse, CreateTopicsResponse, RequestBuilder};

/// The number of partitions was not specified (a replica assignment was given
/// instead).
pub const NO_NUM_PARTITIONS: i32 = -1;
/// The replication factor was not specified (a replica assignment was given
/// instead).
pub const NO_REPLICATION_FACTOR: i16 = -1;

/// A CreateTopics request.
///
/// Corresponds to `org.apache.kafka.common.requests.CreateTopicsRequest`.
#[derive(Debug, Clone)]
pub struct CreateTopicsRequest {
    data: CreateTopicsRequestData,
    version: i16,
}

impl CreateTopicsRequest {
    /// Creates a new `CreateTopicsRequest` from data and version.
    pub fn new(data: CreateTopicsRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &CreateTopicsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut CreateTopicsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::CREATE_TOPICS
    }

    /// Creates an error response for this request, failing every requested
    /// topic with the given error.
    ///
    /// Mirrors `CreateTopicsRequest.getErrorResponse` (which uses
    /// `ApiError.fromThrowable`); the enum-dispatch caller supplies the mapped
    /// [`Errors`] directly.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut response = CreateTopicsResponseData::new();
        if self.version >= 2 {
            response.set_throttle_time_ms(throttle_time_ms);
        }
        let mut topics = Vec::new();
        for topic in &self.data.topics {
            let mut result = CreatableTopicResult::new();
            result.set_name(topic.name.clone());
            result.set_error_code(error.code());
            result.set_error_message(Some(error.message().to_string()));
            topics.push(result);
        }
        response.set_topics(topics);
        ConcreteResponse::CreateTopics(CreateTopicsResponse::new(response))
    }

    /// Parses a `CreateTopicsRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = CreateTopicsRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for CreateTopicsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CreateTopicsRequest(version={}, data={:?})", self.version, self.data)
    }
}

/// Builder for [`CreateTopicsRequest`].
///
/// Corresponds to `CreateTopicsRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct CreateTopicsRequestBuilder {
    data: CreateTopicsRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl CreateTopicsRequestBuilder {
    /// Creates a builder from existing data.
    pub fn from_data(data: CreateTopicsRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::CREATE_TOPICS.oldest_version(),
            latest_allowed_version: ApiKeys::CREATE_TOPICS.latest_version(),
        }
    }
}

impl RequestBuilder for CreateTopicsRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::CREATE_TOPICS
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        if self.data.validate_only && version == 0 {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "validateOnly is not supported in version 0 of CreateTopicsRequest",
            ));
        }
        // Topics created with default partitions/replication factor need v4+.
        let topics_with_defaults: Vec<&str> = self
            .data
            .topics
            .iter()
            .filter(|t| t.assignments.is_empty())
            .filter(|t| t.num_partitions == NO_NUM_PARTITIONS || t.replication_factor == NO_REPLICATION_FACTOR)
            .map(|t| t.name.as_str())
            .collect();
        if !topics_with_defaults.is_empty() && version < 4 {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "Creating topics with default partitions/replication factor are only supported in \
                     CreateTopicRequest version 4+. The following topics need values for partitions and \
                     replicas: {topics_with_defaults:?}"
                ),
            ));
        }
        Ok(ConcreteRequest::CreateTopics(CreateTopicsRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_topics_request_data::CreatableTopic;

    fn topic(name: &str, num_partitions: i32, replication_factor: i16) -> CreatableTopic {
        let mut t = CreatableTopic::new();
        t.set_name(name.to_string());
        t.set_num_partitions(num_partitions);
        t.set_replication_factor(replication_factor);
        t
    }

    #[test]
    fn build_rejects_validate_only_v0() {
        let mut data = CreateTopicsRequestData::new();
        data.set_validate_only(true);
        let mut builder = CreateTopicsRequestBuilder::from_data(data);
        assert!(builder.build_version(0).is_err());
    }

    #[test]
    fn build_rejects_defaults_below_v4() {
        let mut data = CreateTopicsRequestData::new();
        data.set_topics(vec![topic("t", NO_NUM_PARTITIONS, NO_REPLICATION_FACTOR)]);
        let mut builder = CreateTopicsRequestBuilder::from_data(data);
        let err = builder.build_version(3).unwrap_err();
        assert!(err.to_string().contains("version 4+"), "{err}");
        // v4 accepts defaults.
        assert!(builder.build_version(4).is_ok());
    }

    #[test]
    fn get_error_response_fails_every_topic() {
        let mut data = CreateTopicsRequestData::new();
        data.set_topics(vec![topic("a", 1, 1), topic("b", 1, 1)]);
        let request = CreateTopicsRequest::new(data, 7);
        let response = request.get_error_response(100, &Errors::InvalidTopicException);
        if let ConcreteResponse::CreateTopics(r) = response {
            assert_eq!(r.data().topics.len(), 2);
            assert_eq!(r.data().throttle_time_ms, 100);
            for t in &r.data().topics {
                assert_eq!(t.error_code, Errors::InvalidTopicException.code());
            }
        } else {
            panic!("expected CreateTopics response");
        }
    }

    /// Round-trips a request through the shared `ConcreteRequest` serialize /
    /// parse path, exercising the enum wiring end-to-end.
    #[test]
    fn serialize_parse_round_trip() {
        let mut data = CreateTopicsRequestData::new();
        data.set_topics(vec![topic("round-trip-topic", 3, 2)]);
        data.set_timeout_ms(30000);
        let mut request = ConcreteRequest::CreateTopics(CreateTopicsRequest::new(data, 7));
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes.into_buffer());
        let parsed = CreateTopicsRequest::parse(&mut readable, 7).unwrap();
        assert_eq!(parsed.data().topics.len(), 1);
        assert_eq!(parsed.data().topics[0].name, "round-trip-topic");
        assert_eq!(parsed.data().topics[0].num_partitions, 3);
        assert_eq!(parsed.data().topics[0].replication_factor, 2);
        assert_eq!(parsed.data().timeout_ms, 30000);
    }
}
