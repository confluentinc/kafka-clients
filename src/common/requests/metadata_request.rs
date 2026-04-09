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

//! Metadata request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.MetadataRequest`.

use std::collections::HashSet;
use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::uuid::Uuid;
use crate::metadata_request_data::{MetadataRequestData, MetadataRequestTopic};
use crate::metadata_response_data::{MetadataResponseData, MetadataResponseTopic};

use super::ConcreteRequest;
use super::abstract_request::RequestBuilder;
use super::abstract_response::ConcreteResponse;
use super::metadata_response::MetadataResponse;

/// A Metadata request.
///
/// Corresponds to `org.apache.kafka.common.requests.MetadataRequest`.
#[derive(Debug, Clone)]
pub struct MetadataRequest {
    data: MetadataRequestData,
    version: i16,
}

impl MetadataRequest {
    /// Creates a new `MetadataRequest` from data and version.
    pub fn new(data: MetadataRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &MetadataRequestData {
        &self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::METADATA
    }

    /// Returns whether this is a request for all topics.
    ///
    /// In version 0, an empty topic list indicates "request metadata for all topics."
    pub fn is_all_topics(&self) -> bool {
        self.data.topics.is_none() || (self.data.topics.as_ref().is_some_and(|t| t.is_empty()) && self.version == 0)
    }

    /// Returns the list of topic names in this request.
    ///
    /// Returns `None` if this is an "all topics" request.
    pub fn topics(&self) -> Option<Vec<&str>> {
        if self.is_all_topics() {
            // In version 0, we return None for empty topic list
            None
        } else {
            Some(
                self.data
                    .topics
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|t| t.name.as_deref().unwrap_or(""))
                    .collect(),
            )
        }
    }

    /// Returns the list of topic IDs in this request.
    ///
    /// Returns empty if this is an all-topics request or version < 10.
    pub fn topic_ids(&self) -> Vec<Uuid> {
        if self.is_all_topics() || self.version < 10 {
            Vec::new()
        } else {
            self.data.topics.as_ref().unwrap().iter().map(|t| t.topic_id).collect()
        }
    }

    /// Returns whether auto-creation of topics is allowed.
    pub fn allow_auto_topic_creation(&self) -> bool {
        self.data.allow_auto_topic_creation
    }

    /// Creates an error response for this request.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut response_data = MetadataResponseData::new();
        if let Some(topics) = &self.data.topics {
            let mut response_topics = Vec::new();
            for topic in topics {
                // the response does not allow null, so convert to empty string if necessary
                let topic_name = topic.name.as_deref().unwrap_or("");
                let mut t = MetadataResponseTopic::new();
                t.set_name(Some(topic_name.to_string()));
                t.set_topic_id(topic.topic_id);
                t.set_error_code(error.code());
                t.set_is_internal(false);
                t.set_partitions(Vec::new());
                response_topics.push(t);
            }
            response_data.set_topics(response_topics);
        }

        response_data.set_throttle_time_ms(throttle_time_ms);
        response_data.set_error_code(error.code());
        ConcreteResponse::Metadata(MetadataResponse::from_data(response_data, true))
    }

    /// Parses a `MetadataRequest` from a readable buffer at the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = MetadataRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }

    /// Converts a collection of topic names to `MetadataRequestTopic` entries.
    pub fn convert_to_metadata_request_topic(topics: &[&str]) -> Vec<MetadataRequestTopic> {
        topics
            .iter()
            .map(|&topic| {
                let mut t = MetadataRequestTopic::new();
                t.set_name(Some(topic.to_string()));
                t
            })
            .collect()
    }

    /// Converts a collection of topic IDs to `MetadataRequestTopic` entries.
    pub fn convert_topic_ids_to_metadata_request_topic(topic_ids: &[Uuid]) -> Vec<MetadataRequestTopic> {
        topic_ids
            .iter()
            .map(|&topic_id| {
                let mut t = MetadataRequestTopic::new();
                t.set_topic_id(topic_id);
                t
            })
            .collect()
    }
}

impl std::fmt::Display for MetadataRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MetadataRequest(version={}, data={:?})", self.version, self.data)
    }
}

/// Builder for [`MetadataRequest`].
///
/// Corresponds to `MetadataRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct MetadataRequestBuilder {
    data: MetadataRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl MetadataRequestBuilder {
    /// Creates a builder from existing data.
    pub fn from_data(data: MetadataRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::METADATA.oldest_version(),
            latest_allowed_version: ApiKeys::METADATA.latest_version(),
        }
    }

    /// Creates a builder with the given topics and auto-creation flag.
    pub fn new(topics: Option<&[&str]>, allow_auto_topic_creation: bool) -> Self {
        Self::new_with_version_range(
            topics,
            allow_auto_topic_creation,
            ApiKeys::METADATA.oldest_version(),
            ApiKeys::METADATA.latest_version(),
        )
    }

    /// Creates a builder targeting a specific version.
    pub fn new_with_version(topics: Option<&[&str]>, allow_auto_topic_creation: bool, version: i16) -> Self {
        Self::new_with_version_range(topics, allow_auto_topic_creation, version, version)
    }

    /// Creates a builder with the given topics, auto-creation flag, and version range.
    pub fn new_with_version_range(
        topics: Option<&[&str]>,
        allow_auto_topic_creation: bool,
        min_version: i16,
        max_version: i16,
    ) -> Self {
        let data = Self::request_topic_names_or_all_topics(topics, allow_auto_topic_creation);
        Self { data, oldest_allowed_version: min_version, latest_allowed_version: max_version }
    }

    fn request_topic_names_or_all_topics(
        topics: Option<&[&str]>,
        allow_auto_topic_creation: bool,
    ) -> MetadataRequestData {
        let mut data = MetadataRequestData::new();
        match topics {
            None => data.set_topics(None),
            Some(topic_list) => {
                let request_topics: Vec<MetadataRequestTopic> = topic_list
                    .iter()
                    .map(|&topic| {
                        let mut t = MetadataRequestTopic::new();
                        t.set_name(Some(topic.to_string()));
                        t
                    })
                    .collect();
                data.set_topics(Some(request_topics))
            },
        };
        data.set_allow_auto_topic_creation(allow_auto_topic_creation);
        data
    }

    fn request_topic_ids(topic_ids: &HashSet<Uuid>) -> MetadataRequestData {
        let mut data = MetadataRequestData::new();
        let topics: Vec<MetadataRequestTopic> = topic_ids
            .iter()
            .map(|&topic_id| {
                let mut t = MetadataRequestTopic::new();
                t.set_topic_id(topic_id);
                t
            })
            .collect();
        data.set_topics(Some(topics));
        data.set_allow_auto_topic_creation(false); // can't auto-create without topic name
        data
    }

    /// Creates a builder for requesting metadata about all topics.
    ///
    /// This never causes auto-creation, but we set the boolean to `true` because that is
    /// the default value when deserializing V2 and older. This way, the value is consistent
    /// after serialization and deserialization.
    pub fn all_topics() -> Self {
        let mut data = MetadataRequestData::new();
        data.set_topics(None);
        data.set_allow_auto_topic_creation(true);
        Self::from_data(data)
    }

    /// Creates a builder for metadata request using topic names.
    pub fn for_topic_names(topic_names: &[&str], allow_auto_topic_creation: bool) -> Self {
        Self::new(Some(topic_names), allow_auto_topic_creation)
    }

    /// Creates a builder for metadata request using topic IDs.
    pub fn for_topic_ids(topic_ids: &HashSet<Uuid>) -> Self {
        Self::from_data(Self::request_topic_ids(topic_ids))
    }

    /// Returns whether this builder has an empty topic list.
    pub fn empty_topic_list(&self) -> bool {
        self.data.topics.as_ref().is_some_and(|t| t.is_empty())
    }

    /// Returns whether this is an all-topics request.
    pub fn is_all_topics(&self) -> bool {
        self.data.topics.is_none()
    }

    /// Returns the list of topic IDs from the builder data.
    pub fn topic_ids(&self) -> Vec<Uuid> {
        self.data
            .topics
            .as_ref()
            .map(|topics| topics.iter().map(|t| t.topic_id).collect())
            .unwrap_or_default()
    }

    /// Returns the list of topic names from the builder data.
    pub fn topics(&self) -> Vec<&str> {
        self.data
            .topics
            .as_ref()
            .map(|topics| topics.iter().map(|t| t.name.as_deref().unwrap_or("")).collect())
            .unwrap_or_default()
    }
}

impl RequestBuilder for MetadataRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::METADATA
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&self, version: i16) -> ConcreteRequest {
        // MetadataRequest versions older than 1 are not supported.
        assert!(version >= 1, "MetadataRequest versions older than 1 are not supported.");
        assert!(
            self.data.allow_auto_topic_creation || version >= 4,
            "MetadataRequest versions older than 4 don't support the allowAutoTopicCreation field"
        );
        if let Some(topics) = &self.data.topics {
            for topic in topics {
                assert!(
                    topic.name.is_some() || version >= 12,
                    "MetadataRequest version {version} does not support null topic names."
                );
                assert!(
                    Uuid::zero() == topic.topic_id || version >= 12,
                    "MetadataRequest version {version} does not support non-zero topic IDs."
                );
            }
        }
        ConcreteRequest::Metadata(MetadataRequest::new(self.data.clone(), version))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translated from `MetadataRequestTest.testEmptyMeansAllTopicsV0`.
    #[test]
    fn test_empty_means_all_topics_v0() {
        let data = MetadataRequestData::new();
        let parsed_request = MetadataRequest::new(data, 0);
        assert!(parsed_request.is_all_topics());
        assert!(parsed_request.topics().is_none());
    }

    /// Translated from `MetadataRequestTest.testEmptyMeansEmptyForVersionsAboveV0`.
    #[test]
    fn test_empty_means_empty_for_versions_above_v0() {
        for i in 1..=MetadataRequestData::HIGHEST_SUPPORTED_VERSION {
            let mut data = MetadataRequestData::new();
            data.set_allow_auto_topic_creation(true);
            // MetadataRequestData::new() creates topics as None, but in Java the default
            // is an empty list. We need to set topics to Some(vec![]) to match.
            data.set_topics(Some(Vec::new()));
            let parsed_request = MetadataRequest::new(data, i);
            assert!(!parsed_request.is_all_topics(), "version {i}");
            let topics = parsed_request.topics().unwrap();
            assert!(topics.is_empty(), "version {i}");
        }
    }

    /// Translated from `MetadataRequestTest.testMetadataRequestVersion`.
    #[test]
    fn test_metadata_request_version() {
        let builder = MetadataRequestBuilder::new(Some(&["topic"]), false);
        assert_eq!(ApiKeys::METADATA.oldest_version(), builder.oldest_allowed_version());
        assert_eq!(ApiKeys::METADATA.latest_version(), builder.latest_allowed_version());

        let version: i16 = 5;
        let builder2 = MetadataRequestBuilder::new_with_version(Some(&["topic"]), false, version);
        assert_eq!(version, builder2.oldest_allowed_version());
        assert_eq!(version, builder2.latest_allowed_version());

        let min_version: i16 = 1;
        let max_version: i16 = 6;
        let builder3 =
            MetadataRequestBuilder::new_with_version_range(Some(&["topic"]), false, min_version, max_version);
        assert_eq!(min_version, builder3.oldest_allowed_version());
        assert_eq!(max_version, builder3.latest_allowed_version());
    }

    /// Translated from `MetadataRequestTest.testTopicIdAndNullTopicNameRequests`.
    #[test]
    fn test_topic_id_and_null_topic_name_requests() {
        let uuid1 = Uuid::random_uuid();
        let uuid2 = Uuid::random_uuid();
        let uuid3 = Uuid::random_uuid();

        // Construct invalid MetadataRequestTopics
        let topics = vec![
            {
                let mut t = MetadataRequestTopic::new();
                t.set_name(None);
                t.set_topic_id(uuid1);
                t
            },
            {
                let mut t = MetadataRequestTopic::new();
                t.set_name(None);
                t
            },
            {
                let mut t = MetadataRequestTopic::new();
                t.set_topic_id(uuid2);
                t
            },
            {
                let mut t = MetadataRequestTopic::new();
                t.set_name(Some("topic".to_string()));
                t.set_topic_id(uuid3);
                t
            },
        ];

        // if version is 10 or 11, the invalid topic metadata should return an error (panic in Rust)
        let invalid_versions: Vec<i16> = vec![10, 11];
        for version in &invalid_versions {
            for topic in &topics {
                let mut data = MetadataRequestData::new();
                data.set_topics(Some(vec![topic.clone()]));
                let builder = MetadataRequestBuilder::from_data(data);
                let result = std::panic::catch_unwind(|| builder.build_version(*version));
                assert!(result.is_err(), "Expected panic for version {version} with topic {:?}", topic);
            }
        }
    }

    /// Translated from `MetadataRequestTest.testTopicIdWithZeroUuid`.
    #[test]
    fn test_topic_id_with_zero_uuid() {
        let topics = vec![
            {
                let mut t = MetadataRequestTopic::new();
                t.set_name(Some("topic".to_string()));
                t.set_topic_id(Uuid::zero());
                t
            },
            {
                let mut t = MetadataRequestTopic::new();
                t.set_name(Some("topic".to_string()));
                t.set_topic_id(Uuid::new(0, 0));
                t
            },
            {
                let mut t = MetadataRequestTopic::new();
                t.set_name(Some("topic".to_string()));
                t
            },
        ];

        let invalid_versions: Vec<i16> = vec![10, 11];
        for version in &invalid_versions {
            for topic in &topics {
                let mut data = MetadataRequestData::new();
                data.set_topics(Some(vec![topic.clone()]));
                let builder = MetadataRequestBuilder::from_data(data);
                // Should NOT panic since topic_id is zero UUID
                let result = std::panic::catch_unwind(|| builder.build_version(*version));
                assert!(result.is_ok(), "Should not panic for version {version} with topic {:?}", topic);
            }
        }
    }
}
