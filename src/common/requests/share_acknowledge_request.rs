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

//! `ShareAcknowledge` request handling (KIP-932).
//!
//! Corresponds to `org.apache.kafka.common.requests.ShareAcknowledgeRequest`.

use std::io;

use crate::common::TopicIdPartition;
use crate::common::Uuid;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::share_acknowledge_request_data::{
    AcknowledgePartition, AcknowledgeTopic, AcknowledgementBatch, ShareAcknowledgeRequestData,
};
use crate::share_acknowledge_response_data::ShareAcknowledgeResponseData;

use super::ConcreteRequest;
use super::ConcreteResponse;
use super::RequestBuilder;
use super::ShareAcknowledgeResponse;
use super::ShareRequestMetadata;

/// A `ShareAcknowledge` request.
///
/// Corresponds to `org.apache.kafka.common.requests.ShareAcknowledgeRequest`.
#[derive(Debug, Clone)]
pub struct ShareAcknowledgeRequest {
    data: ShareAcknowledgeRequestData,
    version: i16,
}

impl ShareAcknowledgeRequest {
    /// Creates a new `ShareAcknowledgeRequest` from data and version.
    pub fn new(data: ShareAcknowledgeRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ShareAcknowledgeRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ShareAcknowledgeRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::SHARE_ACKNOWLEDGE
    }

    /// Builds the canonical error response for this request, matching Java's
    /// `ShareAcknowledgeRequest.getErrorResponse(throttleTimeMs, Throwable)`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut data = ShareAcknowledgeResponseData::new();
        data.set_throttle_time_ms(throttle_time_ms).set_error_code(error.code());
        ConcreteResponse::ShareAcknowledge(ShareAcknowledgeResponse::new(data))
    }

    /// Parses a `ShareAcknowledgeRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ShareAcknowledgeRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for ShareAcknowledgeRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ShareAcknowledgeRequest(version={}, data={})", self.version, self.data)
    }
}

/// Builder for [`ShareAcknowledgeRequest`].
///
/// Corresponds to `ShareAcknowledgeRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct ShareAcknowledgeRequestBuilder {
    data: ShareAcknowledgeRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl ShareAcknowledgeRequestBuilder {
    /// Creates a builder wrapping the given data with the full supported
    /// version range.
    pub fn new(data: ShareAcknowledgeRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::SHARE_ACKNOWLEDGE.oldest_version(),
            latest_allowed_version: ApiKeys::SHARE_ACKNOWLEDGE.latest_version(),
        }
    }

    /// Builds a consumer `ShareAcknowledge` request, mirroring Java's
    /// `ShareAcknowledgeRequest.Builder.forConsumer(...)`.
    pub fn for_consumer(
        group_id: &str,
        metadata: Option<&ShareRequestMetadata>,
        is_renew_ack: bool,
        acknowledgements_map: &[(TopicIdPartition, Vec<AcknowledgementBatch>)],
    ) -> Self {
        let mut data = ShareAcknowledgeRequestData::new();
        data.set_group_id(Some(group_id.to_string()));
        if let Some(metadata) = metadata {
            data.set_member_id(Some(metadata.member_id().to_string()));
            data.set_share_session_epoch(metadata.epoch());
        }
        data.set_is_renew_ack(is_renew_ack);

        // The auto-generated data uses plain `Vec`s (not Java's keyed
        // `*Collection`), so the `find`/`add` pattern is translated into linear
        // lookups.
        let mut ack_topics: Vec<AcknowledgeTopic> = Vec::new();
        for (tip, batches) in acknowledgements_map {
            let topic_idx = Self::find_or_add_topic(&mut ack_topics, tip.topic_id());
            let partition_idx = Self::find_or_add_partition(&mut ack_topics[topic_idx], tip.partition());
            ack_topics[topic_idx].partitions[partition_idx].set_acknowledgement_batches(batches.clone());
        }

        data.set_topics(ack_topics);
        Self::new(data)
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ShareAcknowledgeRequestData {
        &self.data
    }

    fn find_or_add_topic(ack_topics: &mut Vec<AcknowledgeTopic>, topic_id: Uuid) -> usize {
        if let Some(idx) = ack_topics.iter().position(|t| t.topic_id == topic_id) {
            idx
        } else {
            let mut ack_topic = AcknowledgeTopic::new();
            ack_topic.set_topic_id(topic_id);
            ack_topic.set_partitions(Vec::new());
            ack_topics.push(ack_topic);
            ack_topics.len() - 1
        }
    }

    fn find_or_add_partition(ack_topic: &mut AcknowledgeTopic, partition_index: i32) -> usize {
        if let Some(idx) = ack_topic.partitions.iter().position(|p| p.partition_index == partition_index) {
            idx
        } else {
            let mut ack_partition = AcknowledgePartition::new();
            ack_partition.set_partition_index(partition_index);
            ack_topic.partitions.push(ack_partition);
            ack_topic.partitions.len() - 1
        }
    }
}

impl RequestBuilder for ShareAcknowledgeRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::SHARE_ACKNOWLEDGE
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        // Mirrors `ShareAcknowledgeRequest.Builder.build(short version)`.
        if version < 2 && self.data.is_renew_ack {
            // The v1 does not support AcknowledgeType RENEW.
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "The v1 ShareAcknowledge does not support AcknowledgeType.RENEW",
            ));
        }
        Ok(ConcreteRequest::ShareAcknowledge(ShareAcknowledgeRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_for_consumer_groups_by_topic() {
        let foo = Uuid::new(1, 1);
        let member = Uuid::new(9, 9);
        let metadata = ShareRequestMetadata::new(member, 3);
        let mut batch = AcknowledgementBatch::new();
        batch.set_first_offset(0).set_last_offset(0).set_acknowledge_types(vec![1]);
        let acks = vec![(TopicIdPartition::from_parts(foo, 0, "foo"), vec![batch])];
        let builder = ShareAcknowledgeRequestBuilder::for_consumer("G1", Some(&metadata), false, &acks);
        let data = builder.data();
        assert_eq!(data.group_id.as_deref(), Some("G1"));
        assert_eq!(data.share_session_epoch, 3);
        assert_eq!(data.topics.len(), 1);
        assert_eq!(data.topics[0].partitions.len(), 1);
        assert_eq!(data.topics[0].partitions[0].acknowledgement_batches.len(), 1);
    }

    #[test]
    fn test_v1_rejects_renew_ack() {
        let mut data = ShareAcknowledgeRequestData::new();
        data.set_is_renew_ack(true);
        let mut builder = ShareAcknowledgeRequestBuilder::new(data);
        let err = builder.build_version(1).expect_err("v1 with renew must be rejected");
        assert!(err.to_string().contains("does not support AcknowledgeType.RENEW"), "got: {err}");
    }

    #[test]
    fn test_get_error_response() {
        let data = ShareAcknowledgeRequestData::new();
        let req = ShareAcknowledgeRequest::new(data, 2);
        let resp = req.get_error_response(50, &Errors::InvalidShareSessionEpoch);
        match resp {
            ConcreteResponse::ShareAcknowledge(r) => {
                assert_eq!(r.throttle_time_ms(), 50);
                assert_eq!(r.error(), Errors::InvalidShareSessionEpoch);
            },
            other => panic!("expected ShareAcknowledge, got {other:?}"),
        }
    }
}
