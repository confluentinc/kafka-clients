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

//! DeleteTopics request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.DeleteTopicsRequest`.

use std::io;

use crate::common::Uuid;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::delete_topics_request_data::{DeleteTopicState, DeleteTopicsRequestData};
use crate::delete_topics_response_data::{DeletableTopicResult, DeleteTopicsResponseData};

use super::{ConcreteRequest, ConcreteResponse, DeleteTopicsResponse, RequestBuilder};

/// A DeleteTopics request.
///
/// Corresponds to `org.apache.kafka.common.requests.DeleteTopicsRequest`.
#[derive(Debug, Clone)]
pub struct DeleteTopicsRequest {
    data: DeleteTopicsRequestData,
    version: i16,
}

impl DeleteTopicsRequest {
    /// Creates a new `DeleteTopicsRequest` from data and version.
    pub fn new(data: DeleteTopicsRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DeleteTopicsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DeleteTopicsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DELETE_TOPICS
    }

    /// Returns the topics being deleted as `DeleteTopicState` entries,
    /// normalising the pre-v6 `topic_names` list into topic states.
    ///
    /// Mirrors `DeleteTopicsRequest.topics()`.
    pub fn topics(&self) -> Vec<DeleteTopicState> {
        if self.version >= 6 {
            self.data.topics.clone()
        } else {
            self.data
                .topic_names
                .iter()
                .map(|name| {
                    let mut state = DeleteTopicState::new();
                    state.set_name(Some(name.clone()));
                    state
                })
                .collect()
        }
    }

    /// Creates an error response for this request, failing every requested
    /// topic with the given error.
    ///
    /// Mirrors `DeleteTopicsRequest.getErrorResponse`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut response = DeleteTopicsResponseData::new();
        if self.version >= 1 {
            response.set_throttle_time_ms(throttle_time_ms);
        }
        let mut responses = Vec::new();
        for topic in self.topics() {
            let mut result = DeletableTopicResult::new();
            result.set_name(topic.name.clone());
            result.set_topic_id(topic.topic_id);
            result.set_error_code(error.code());
            responses.push(result);
        }
        response.set_responses(responses);
        ConcreteResponse::DeleteTopics(DeleteTopicsResponse::new(response))
    }

    /// Parses a `DeleteTopicsRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DeleteTopicsRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for DeleteTopicsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeleteTopicsRequest(version={}, data={:?})", self.version, self.data)
    }
}

/// Builder for [`DeleteTopicsRequest`].
///
/// Corresponds to `DeleteTopicsRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct DeleteTopicsRequestBuilder {
    data: DeleteTopicsRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl DeleteTopicsRequestBuilder {
    /// Creates a builder from existing data.
    pub fn new(data: DeleteTopicsRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::DELETE_TOPICS.oldest_version(),
            latest_allowed_version: ApiKeys::DELETE_TOPICS.latest_version(),
        }
    }
}

impl RequestBuilder for DeleteTopicsRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DELETE_TOPICS
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        // v6+ carries topics as DeleteTopicState (with topic ids); older
        // versions use the topic_names list. Mirror Builder.build's grouping.
        if version >= 6 && !self.data.topic_names.is_empty() {
            let topics: Vec<DeleteTopicState> = self
                .data
                .topic_names
                .iter()
                .map(|name| {
                    let mut state = DeleteTopicState::new();
                    state.set_name(Some(name.clone()));
                    state
                })
                .collect();
            self.data.set_topics(topics);
        }
        if version < 6 {
            for topic in &self.data.topics {
                if topic.topic_id != Uuid::zero() {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        format!("DeleteTopicsRequest version {version} does not support topic IDs."),
                    ));
                }
            }
        }
        Ok(ConcreteRequest::DeleteTopics(DeleteTopicsRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topics_normalizes_names_below_v6() {
        let mut data = DeleteTopicsRequestData::new();
        data.set_topic_names(vec!["a".to_string(), "b".to_string()]);
        let request = DeleteTopicsRequest::new(data, 5);
        let topics = request.topics();
        assert_eq!(topics.len(), 2);
        assert_eq!(topics[0].name.as_deref(), Some("a"));
    }

    #[test]
    fn get_error_response_fails_every_topic() {
        let mut data = DeleteTopicsRequestData::new();
        data.set_topic_names(vec!["a".to_string(), "b".to_string()]);
        let request = DeleteTopicsRequest::new(data, 5);
        let response = request.get_error_response(0, &Errors::UnknownTopicOrPartition);
        if let ConcreteResponse::DeleteTopics(r) = response {
            assert_eq!(r.data().responses.len(), 2);
            for result in &r.data().responses {
                assert_eq!(result.error_code, Errors::UnknownTopicOrPartition.code());
            }
        } else {
            panic!("expected DeleteTopics response");
        }
    }

    #[test]
    fn serialize_parse_round_trip_names() {
        let mut data = DeleteTopicsRequestData::new();
        data.set_topic_names(vec!["to-delete".to_string()]);
        data.set_timeout_ms(15000);
        let mut request = ConcreteRequest::DeleteTopics(DeleteTopicsRequest::new(data, 5));
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes.into_buffer());
        let parsed = DeleteTopicsRequest::parse(&mut readable, 5).unwrap();
        assert_eq!(parsed.data().topic_names, vec!["to-delete".to_string()]);
        assert_eq!(parsed.data().timeout_ms, 15000);
    }
}
