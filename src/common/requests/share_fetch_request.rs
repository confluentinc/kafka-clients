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

//! `ShareFetch` request handling (KIP-932).
//!
//! Corresponds to `org.apache.kafka.common.requests.ShareFetchRequest`.

use std::collections::HashMap;
use std::io;

use crate::common::TopicIdPartition;
use crate::common::TopicPartition;
use crate::common::Uuid;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::consumer::internals::share_acquire_mode::ShareAcquireMode;
use crate::share_fetch_request_data::{
    AcknowledgementBatch, FetchPartition, FetchTopic, ForgottenTopic, ShareFetchRequestData,
};

use super::ConcreteRequest;
use super::ConcreteResponse;
use super::RequestBuilder;
use super::ShareFetchResponse;
use super::ShareRequestMetadata;

/// A `ShareFetch` request.
///
/// Corresponds to `org.apache.kafka.common.requests.ShareFetchRequest`.
#[derive(Debug, Clone)]
pub struct ShareFetchRequest {
    data: ShareFetchRequestData,
    version: i16,
}

impl ShareFetchRequest {
    /// Creates a new `ShareFetchRequest` from data and version.
    pub fn new(data: ShareFetchRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ShareFetchRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ShareFetchRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::SHARE_FETCH
    }

    /// Returns the requested minimum number of bytes.
    pub fn min_bytes(&self) -> i32 {
        self.data.min_bytes
    }

    /// Returns the requested maximum number of bytes.
    pub fn max_bytes(&self) -> i32 {
        self.data.max_bytes
    }

    /// Returns the requested maximum wait in milliseconds.
    pub fn max_wait(&self) -> i32 {
        self.data.max_wait_ms
    }

    /// Returns the list of topic-partitions to fetch, resolving topic names
    /// from `topic_names` (name is empty if it cannot be resolved).
    ///
    /// Corresponds to Java's `shareFetchData(Map<Uuid, String>)`. Java caches
    /// the result in a volatile field for thread-safety; the Rust translation
    /// recomputes on demand — the result is a pure function of `data` and
    /// `topic_names`, so behavior is identical.
    pub fn share_fetch_data(&self, topic_names: &HashMap<Uuid, String>) -> Vec<TopicIdPartition> {
        let mut out = Vec::new();
        for share_fetch_topic in &self.data.topics {
            let name = topic_names.get(&share_fetch_topic.topic_id).cloned().unwrap_or_default();
            for share_fetch_partition in &share_fetch_topic.partitions {
                out.push(TopicIdPartition::from_parts(
                    share_fetch_topic.topic_id,
                    share_fetch_partition.partition_index,
                    name.clone(),
                ));
            }
        }
        out
    }

    /// Returns the list of topic-partitions in the forget list, resolving topic
    /// names from `topic_names`.
    ///
    /// Corresponds to Java's `forgottenTopics(Map<Uuid, String>)`.
    pub fn forgotten_topics(&self, topic_names: &HashMap<Uuid, String>) -> Vec<TopicIdPartition> {
        let mut out = Vec::new();
        for forgotten_topic in &self.data.forgotten_topics_data {
            let name = topic_names.get(&forgotten_topic.topic_id).cloned().unwrap_or_default();
            for &partition_id in &forgotten_topic.partitions {
                out.push(TopicIdPartition::new(
                    forgotten_topic.topic_id,
                    TopicPartition::new(name.clone(), partition_id),
                ));
            }
        }
        out
    }

    /// Builds the canonical error response for this request, matching Java's
    /// `ShareFetchRequest.getErrorResponse(throttleTimeMs, Throwable)`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        ConcreteResponse::ShareFetch(ShareFetchResponse::of(*error, throttle_time_ms, Vec::new(), &[], 0))
    }

    /// Parses a `ShareFetchRequest` from a readable buffer at the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ShareFetchRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for ShareFetchRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ShareFetchRequest(version={}, data={})", self.version, self.data)
    }
}

/// Builder for [`ShareFetchRequest`].
///
/// Corresponds to `ShareFetchRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct ShareFetchRequestBuilder {
    data: ShareFetchRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl ShareFetchRequestBuilder {
    /// Creates a builder wrapping the given data with the full supported
    /// version range.
    pub fn new(data: ShareFetchRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::SHARE_FETCH.oldest_version(),
            latest_allowed_version: ApiKeys::SHARE_FETCH.latest_version(),
        }
    }

    /// Builds a consumer `ShareFetch` request, mirroring Java's
    /// `ShareFetchRequest.Builder.forConsumer(...)`.
    #[allow(clippy::too_many_arguments)]
    pub fn for_consumer(
        group_id: &str,
        metadata: Option<&ShareRequestMetadata>,
        max_wait: i32,
        min_bytes: i32,
        max_bytes: i32,
        max_records: i32,
        batch_size: i32,
        share_acquire_mode: i8,
        is_renew_ack: bool,
        send: &[TopicIdPartition],
        forget: &[TopicIdPartition],
        acknowledgements_map: &[(TopicIdPartition, Vec<AcknowledgementBatch>)],
    ) -> Self {
        let mut data = ShareFetchRequestData::new();
        data.set_group_id(Some(group_id.to_string()));
        let mut is_closing_share_session = false;
        if let Some(metadata) = metadata {
            data.set_member_id(Some(metadata.member_id().to_string()));
            data.set_share_session_epoch(metadata.epoch());
            if metadata.is_final_epoch() {
                is_closing_share_session = true;
            }
        }
        data.set_max_wait_ms(max_wait);
        data.set_min_bytes(min_bytes);
        data.set_max_bytes(max_bytes);
        data.set_max_records(max_records);
        data.set_batch_size(batch_size);
        data.set_share_acquire_mode(share_acquire_mode);
        data.set_is_renew_ack(is_renew_ack);

        // Build a list of topics to fetch keyed by topic ID, and within each a
        // list of partitions keyed by index. The auto-generated data uses plain
        // `Vec`s (not Java's keyed `*Collection`), so the `find`/`add` pattern
        // is translated into linear lookups.
        let mut fetch_topics: Vec<FetchTopic> = Vec::new();

        // First, start by adding the list of topic-partitions we are fetching.
        if !is_closing_share_session {
            for tip in send {
                let topic_idx = Self::find_or_add_topic(&mut fetch_topics, tip.topic_id());
                Self::find_or_add_partition(&mut fetch_topics[topic_idx], tip.partition());
            }
        }

        // Next, add acknowledgements that we are piggybacking onto the fetch.
        // Generally, the list of topic-partitions will be a subset, but if the
        // assignment changes, there might be new entries to add.
        for (tip, batches) in acknowledgements_map {
            let topic_idx = Self::find_or_add_topic(&mut fetch_topics, tip.topic_id());
            let partition_idx = Self::find_or_add_partition(&mut fetch_topics[topic_idx], tip.partition());
            fetch_topics[topic_idx].partitions[partition_idx].set_acknowledgement_batches(batches.clone());
        }

        // Build up the data to fetch.
        data.set_topics(fetch_topics);

        let mut builder = Self::new(data);
        // And finally, forget the topic-partitions that are no longer in the session.
        if !forget.is_empty() {
            builder.data.set_forgotten_topics_data(Vec::new());
            builder.update_forgotten_data(forget);
        }
        builder
    }

    /// Appends the given partitions to the request's forgotten-topics data.
    ///
    /// Corresponds to Java's `Builder.updateForgottenData(List<TopicIdPartition>)`.
    pub fn update_forgotten_data(&mut self, forget: &[TopicIdPartition]) {
        // Java uses a HashMap here (iteration order unspecified); we preserve
        // first-seen order for determinism, which the wire protocol tolerates.
        let mut forget_map: Vec<(Uuid, Vec<i32>)> = Vec::new();
        for tip in forget {
            match forget_map.iter_mut().find(|(id, _)| *id == tip.topic_id()) {
                Some((_, parts)) => parts.push(tip.partition()),
                None => forget_map.push((tip.topic_id(), vec![tip.partition()])),
            }
        }
        for (topic_id, part_list) in forget_map {
            let mut forget_topic = ForgottenTopic::new();
            forget_topic.set_topic_id(topic_id);
            forget_topic.set_partitions(part_list);
            self.data.forgotten_topics_data.push(forget_topic);
        }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ShareFetchRequestData {
        &self.data
    }

    /// Finds the index of the fetch topic with the given id, adding a new one
    /// if it is not present. Returns the index in `fetch_topics`.
    fn find_or_add_topic(fetch_topics: &mut Vec<FetchTopic>, topic_id: Uuid) -> usize {
        if let Some(idx) = fetch_topics.iter().position(|t| t.topic_id == topic_id) {
            idx
        } else {
            let mut fetch_topic = FetchTopic::new();
            fetch_topic.set_topic_id(topic_id);
            fetch_topic.set_partitions(Vec::new());
            fetch_topics.push(fetch_topic);
            fetch_topics.len() - 1
        }
    }

    /// Finds the index of the fetch partition with the given index within
    /// `fetch_topic`, adding a new one if not present. Returns the index in the
    /// topic's partitions vector.
    fn find_or_add_partition(fetch_topic: &mut FetchTopic, partition_index: i32) -> usize {
        if let Some(idx) = fetch_topic.partitions.iter().position(|p| p.partition_index == partition_index) {
            idx
        } else {
            let mut fetch_partition = FetchPartition::new();
            fetch_partition.set_partition_index(partition_index);
            fetch_topic.partitions.push(fetch_partition);
            fetch_topic.partitions.len() - 1
        }
    }
}

impl RequestBuilder for ShareFetchRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::SHARE_FETCH
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        // Mirrors `ShareFetchRequest.Builder.build(short version)`.
        if version < 2 {
            // The v1 does not support AcknowledgeType RENEW.
            if self.data.is_renew_ack {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "The v1 ShareFetch does not support AcknowledgeType.RENEW",
                ));
            }
            // The v1 only supports ShareAcquireMode.BATCH_OPTIMIZED.
            if self.data.share_acquire_mode != ShareAcquireMode::BatchOptimized.id() {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "The v1 ShareFetch only supports ShareAcquireMode.BATCH_OPTIMIZED",
                ));
            }
        }
        Ok(ConcreteRequest::ShareFetch(ShareFetchRequest::new(self.data.clone(), version)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_key() {
        let builder = ShareFetchRequestBuilder::new(ShareFetchRequestData::new());
        assert_eq!(RequestBuilder::api_key(&builder), &ApiKeys::SHARE_FETCH);
    }

    #[test]
    fn test_for_consumer_builds_topics_and_forget() {
        let foo = Uuid::new(1, 1);
        let bar = Uuid::new(2, 2);
        let member = Uuid::new(9, 9);
        let metadata = ShareRequestMetadata::initial_epoch(member);
        let send = vec![
            TopicIdPartition::from_parts(foo, 0, "foo"),
            TopicIdPartition::from_parts(foo, 1, "foo"),
        ];
        let forget = vec![TopicIdPartition::from_parts(bar, 0, "bar")];
        let builder = ShareFetchRequestBuilder::for_consumer(
            "G1",
            Some(&metadata),
            500,
            1,
            100,
            500,
            500,
            ShareAcquireMode::BatchOptimized.id(),
            false,
            &send,
            &forget,
            &[],
        );
        let data = builder.data();
        assert_eq!(data.group_id.as_deref(), Some("G1"));
        assert_eq!(data.member_id.as_deref(), Some(member.to_string().as_str()));
        assert_eq!(data.topics.len(), 1);
        assert_eq!(data.topics[0].partitions.len(), 2);
        assert_eq!(data.forgotten_topics_data.len(), 1);
        assert_eq!(data.forgotten_topics_data[0].partitions, vec![0]);
    }

    #[test]
    fn test_v1_rejects_renew_ack() {
        let mut data = ShareFetchRequestData::new();
        data.set_is_renew_ack(true);
        let mut builder = ShareFetchRequestBuilder::new(data);
        let err = builder.build_version(1).expect_err("v1 with renew must be rejected");
        assert!(err.to_string().contains("does not support AcknowledgeType.RENEW"), "got: {err}");
    }

    #[test]
    fn test_v1_rejects_record_limit_acquire_mode() {
        let mut data = ShareFetchRequestData::new();
        data.set_share_acquire_mode(ShareAcquireMode::RecordLimit.id());
        let mut builder = ShareFetchRequestBuilder::new(data);
        let err = builder.build_version(1).expect_err("v1 with record_limit must be rejected");
        assert!(
            err.to_string().contains("only supports ShareAcquireMode.BATCH_OPTIMIZED"),
            "got: {err}"
        );
    }

    #[test]
    fn test_share_fetch_data_and_forgotten_topics_resolve_names() {
        let foo = Uuid::new(1, 1);
        let member = Uuid::new(9, 9);
        let metadata = ShareRequestMetadata::initial_epoch(member);
        let send = vec![TopicIdPartition::from_parts(foo, 3, "foo")];
        let builder = ShareFetchRequestBuilder::for_consumer(
            "G1",
            Some(&metadata),
            500,
            1,
            100,
            500,
            500,
            ShareAcquireMode::BatchOptimized.id(),
            false,
            &send,
            &[],
            &[],
        );
        let req = match builder.clone().build_version(2).unwrap() {
            ConcreteRequest::ShareFetch(r) => r,
            other => panic!("expected ShareFetch, got {other:?}"),
        };
        let mut names = HashMap::new();
        names.insert(foo, "foo".to_string());
        let fetch_data = req.share_fetch_data(&names);
        assert_eq!(fetch_data.len(), 1);
        assert_eq!(fetch_data[0].topic(), "foo");
        assert_eq!(fetch_data[0].partition(), 3);
    }
}
