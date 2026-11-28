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

//! `ShareFetch` request handling (KIP-932).
//!
//! Corresponds to `org.apache.kafka.common.requests.ShareFetchRequest`.

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::sync::OnceLock;

use crate::ShareFetchRequestData;
use crate::common::errors::UnsupportedVersionError;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::{TopicIdPartition, TopicPartition, Uuid};
use crate::consumer::internals::ShareAcquireMode;
use crate::share_fetch_request_data::{AcknowledgementBatch, FetchPartition, FetchTopic, ForgottenTopic};

use super::AbstractRequest;
use super::ConcreteResponse;
use super::RequestBuilder;
use super::ShareFetchResponse;
use super::ShareRequestMetadata;

/// A `ShareFetch` request.
///
/// Corresponds to `org.apache.kafka.common.requests.ShareFetchRequest`.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.ShareFetchRequest")]
pub struct ShareFetchRequest {
    data: ShareFetchRequestData,
    version: i16,
    /// Lazily built by [`Self::share_fetch_data`]; Java's `volatile` field plus
    /// double-checked locking.
    share_fetch_data: OnceLock<Vec<TopicIdPartition>>,
    /// Lazily built by [`Self::forgotten_topics`]; Java's `toForget`.
    to_forget: OnceLock<Vec<TopicIdPartition>>,
}

impl ShareFetchRequest {
    /// Creates a new `ShareFetchRequest` from data and version.
    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchRequest#ShareFetchRequest")]
    pub fn new(data: ShareFetchRequestData, version: i16) -> Self {
        Self { data, version, share_fetch_data: OnceLock::new(), to_forget: OnceLock::new() }
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchRequest#data")]
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

    /// Builds the error response for this request: the top-level error and the
    /// throttle time, with no partitions, no node endpoints and a zero
    /// acquisition lock timeout.
    ///
    /// Translates `ShareFetchRequest.getErrorResponse(int, Throwable)`; the
    /// caller supplies `Errors.forException(e)`.
    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchRequest#getErrorResponse")]
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        ConcreteResponse::ShareFetch(ShareFetchResponse::of(*error, throttle_time_ms, Vec::new(), &[], 0))
    }

    /// Parses a `ShareFetchRequest` from a readable buffer at the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchRequest#parse")]
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ShareFetchRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }

    /// Returns the minimum bytes to accumulate in the response.
    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchRequest#minBytes")]
    pub fn min_bytes(&self) -> i32 {
        self.data.min_bytes
    }

    /// Returns the maximum bytes to fetch.
    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchRequest#maxBytes")]
    pub fn max_bytes(&self) -> i32 {
        self.data.max_bytes
    }

    /// Returns the maximum time in milliseconds to wait for the response.
    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchRequest#maxWait")]
    pub fn max_wait(&self) -> i32 {
        self.data.max_wait_ms
    }

    /// Returns the partitions to fetch, resolving each topic name through
    /// `topic_names`.
    ///
    /// The list is computed on the first call and returned unchanged by every
    /// later call, whatever map it is given, as in Java. A topic id missing from
    /// `topic_names` gets an empty name (Java: `null`).
    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchRequest#shareFetchData")]
    pub fn share_fetch_data(&self, topic_names: &HashMap<Uuid, String>) -> &[TopicIdPartition] {
        self.share_fetch_data.get_or_init(|| {
            let mut share_fetch_data = Vec::new();
            for share_fetch_topic in &self.data.topics {
                // Topic name may be empty here if the topic name was unable to be
                // resolved using the topic_names map.
                let name = topic_names.get(&share_fetch_topic.topic_id).map(String::as_str).unwrap_or("");
                for share_fetch_partition in &share_fetch_topic.partitions {
                    share_fetch_data.push(TopicIdPartition::with_partition_topic(
                        share_fetch_topic.topic_id,
                        share_fetch_partition.partition_index,
                        name,
                    ));
                }
            }
            share_fetch_data
        })
    }

    /// Returns the partitions to remove from the share session, resolving each
    /// topic name through `topic_names`.
    ///
    /// Computed once, like [`Self::share_fetch_data`]. A topic id missing from
    /// `topic_names` gets an empty name (Java: `null`).
    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchRequest#forgottenTopics")]
    pub fn forgotten_topics(&self, topic_names: &HashMap<Uuid, String>) -> &[TopicIdPartition] {
        self.to_forget.get_or_init(|| {
            let mut to_forget = Vec::new();
            for forgotten_topic in &self.data.forgotten_topics_data {
                // Topic name may be empty here if the topic name was unable to be
                // resolved using the topic_names map.
                let name = topic_names.get(&forgotten_topic.topic_id).cloned().unwrap_or_default();
                for &partition_id in &forgotten_topic.partitions {
                    to_forget.push(TopicIdPartition::new(
                        forgotten_topic.topic_id,
                        TopicPartition::new(name.clone(), partition_id),
                    ));
                }
            }
            to_forget
        })
    }
}

impl fmt::Display for ShareFetchRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.data)
    }
}

/// Returns the index of the fetch topic `topic_id` in `topics`, appending an
/// empty one first if there is none: the `find`-then-`add` of Java's
/// `FetchTopicCollection` (keyed by topic id).
fn find_or_add_fetch_topic(topics: &mut Vec<FetchTopic>, topic_id: Uuid) -> usize {
    match topics.iter().position(|topic| topic.topic_id == topic_id) {
        Some(index) => index,
        None => {
            let mut topic = FetchTopic::new();
            topic.set_topic_id(topic_id);
            topics.push(topic);
            topics.len() - 1
        },
    }
}

/// Returns the fetch partition `partition` of `topic`, appending an empty one
/// first if there is none: the `find`-then-`add` of Java's
/// `FetchPartitionCollection` (keyed by partition index).
fn find_or_add_fetch_partition(topic: &mut FetchTopic, partition: i32) -> &mut FetchPartition {
    let index = match topic.partitions.iter().position(|p| p.partition_index == partition) {
        Some(index) => index,
        None => {
            let mut fetch_partition = FetchPartition::new();
            fetch_partition.set_partition_index(partition);
            topic.partitions.push(fetch_partition);
            topic.partitions.len() - 1
        },
    };
    &mut topic.partitions[index]
}

/// Builder for [`ShareFetchRequest`].
///
/// Corresponds to `ShareFetchRequest.Builder` in Java.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.ShareFetchRequest$Builder")]
pub struct Builder {
    data: ShareFetchRequestData,
}

impl Builder {
    /// Creates a builder wrapping the given data.
    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchRequest$Builder#Builder")]
    pub fn new(data: ShareFetchRequestData) -> Self {
        Self { data }
    }

    /// Builds the request a share consumer sends to one broker.
    ///
    /// With `metadata`, the request carries its member id and epoch; when that
    /// epoch is [`ShareRequestMetadata::FINAL_EPOCH`] the request closes the
    /// share session and fetches nothing, so `send` is ignored. The partitions
    /// of `send` come first, in order, then any partition that only carries
    /// acknowledgements, in `acknowledgements_map` order; each partition takes
    /// its acknowledgement batches from the map. `forget` lists the partitions
    /// to drop from the session.
    ///
    /// Translates `ShareFetchRequest.Builder.forConsumer`. Java takes a `Map`
    /// (a `LinkedHashMap` from `ShareSessionHandler`); the iterator keeps
    /// whatever order the caller's map has, and moves the batches in instead of
    /// sharing the lists.
    #[expect(clippy::too_many_arguments)]
    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchRequest$Builder#forConsumer")]
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
        acknowledgements_map: impl IntoIterator<Item = (TopicIdPartition, Vec<AcknowledgementBatch>)>,
    ) -> Self {
        let mut data = ShareFetchRequestData::new();
        data.set_group_id(Some(group_id.to_string()));
        let mut is_closing_share_session = false;
        if let Some(metadata) = metadata {
            data.set_member_id(Some(metadata.member_id().to_string()))
                .set_share_session_epoch(metadata.epoch());
            if metadata.is_final_epoch() {
                is_closing_share_session = true;
            }
        }
        data.set_max_wait_ms(max_wait)
            .set_min_bytes(min_bytes)
            .set_max_bytes(max_bytes)
            .set_max_records(max_records)
            .set_batch_size(batch_size)
            .set_share_acquire_mode(share_acquire_mode)
            .set_is_renew_ack(is_renew_ack);

        // Build a map of topics to fetch keyed by topic ID, and within each a map of
        // partitions keyed by index.
        let mut fetch_topics: Vec<FetchTopic> = Vec::new();

        // First, start by adding the list of topic-partitions we are fetching.
        if !is_closing_share_session {
            for tip in send {
                let topic = find_or_add_fetch_topic(&mut fetch_topics, tip.topic_id());
                find_or_add_fetch_partition(&mut fetch_topics[topic], tip.partition());
            }
        }

        // Next, add acknowledgements that we are piggybacking onto the fetch. Generally,
        // the list of topic-partitions will be a subset, but if the assignment changes,
        // there might be new entries to add.
        for (tip, batches) in acknowledgements_map {
            let topic = find_or_add_fetch_topic(&mut fetch_topics, tip.topic_id());
            find_or_add_fetch_partition(&mut fetch_topics[topic], tip.partition()).set_acknowledgement_batches(batches);
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

    /// Appends `forget` to the request's forgotten topics, one entry per topic
    /// id holding its partitions in `forget` order.
    ///
    /// Translates `ShareFetchRequest.Builder.updateForgottenData`. Java groups
    /// the partitions in a `HashMap`, so the order of the topics is unspecified;
    /// the `HashMap` here keeps that (translation-checklist R5).
    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchRequest$Builder#updateForgottenData")]
    pub fn update_forgotten_data(&mut self, forget: &[TopicIdPartition]) {
        let mut forget_map: HashMap<Uuid, Vec<i32>> = HashMap::new();
        for tip in forget {
            forget_map.entry(tip.topic_id()).or_default().push(tip.partition());
        }
        for (topic_id, part_list) in forget_map {
            let mut forget_topic = ForgottenTopic::new();
            forget_topic.set_topic_id(topic_id).set_partitions(part_list);
            self.data.forgotten_topics_data.push(forget_topic);
        }
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchRequest$Builder#data")]
    pub fn data(&self) -> &ShareFetchRequestData {
        &self.data
    }
}

impl fmt::Display for Builder {
    /// Matches Java's `Builder.toString()`: the data.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.data)
    }
}

impl RequestBuilder for Builder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::SHARE_FETCH
    }

    fn oldest_allowed_version(&self) -> i16 {
        ApiKeys::SHARE_FETCH.oldest_version()
    }

    fn latest_allowed_version(&self) -> i16 {
        // Mirrors Java's `super(ApiKeys)` → `latestVersion(false)`
        // (producer-transactions.md §12).
        ApiKeys::SHARE_FETCH.latest_version_enable_unstable_last_version(false)
    }

    /// Translates `ShareFetchRequest.Builder.build(short)`: v1 cannot carry a
    /// RENEW acknowledgement nor an acquire mode other than BATCH_OPTIMIZED.
    /// Both fields are v2+; `IsRenewAck` is not ignorable, so the serializer
    /// would reject it too, but `ShareAcquireMode` is ignorable and would be
    /// dropped silently, which is why the check lives here.
    fn build_version(&mut self, version: i16) -> io::Result<AbstractRequest> {
        if version < 2 {
            // The v1 does not support AcknowledgeType RENEW.
            if self.data.is_renew_ack {
                return Err(
                    UnsupportedVersionError::new("The v1 ShareFetch does not support AcknowledgeType.RENEW")
                        .into_io_error(),
                );
            }
            // The v1 only supports ShareAcquireMode.BATCH_OPTIMIZED.
            if self.data.share_acquire_mode != ShareAcquireMode::BatchOptimized.id() {
                return Err(UnsupportedVersionError::new(
                    "The v1 ShareFetch only supports ShareAcquireMode.BATCH_OPTIMIZED",
                )
                .into_io_error());
            }
        }
        Ok(AbstractRequest::ShareFetch(ShareFetchRequest::new(self.data.clone(), version)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Error;
    use crate::common::protocol::ByteBufferAccessor;
    use crate::common::requests::request_response_test::{check_error_response, check_request};

    fn unwrap(request: AbstractRequest) -> ShareFetchRequest {
        match request {
            AbstractRequest::ShareFetch(request) => request,
            other => panic!("expected ShareFetch, got {other:?}"),
        }
    }

    fn batch(first: i64, last: i64, types: Vec<i8>) -> AcknowledgementBatch {
        let mut batch = AcknowledgementBatch::new();
        batch.set_first_offset(first).set_last_offset(last).set_acknowledge_types(types);
        batch
    }

    fn assert_unsupported_version(error: &io::Error, message: &str) {
        assert_eq!(io::ErrorKind::Unsupported, error.kind());
        let error = Error::UnsupportedVersion(UnsupportedVersionError::from_io_error(error));
        assert_eq!(message, error.message());
    }

    /// `RequestResponseTest.createShareFetchRequest(short)`.
    fn create_share_fetch_request(version: i16) -> ShareFetchRequest {
        let mut partition = FetchPartition::new();
        partition.set_partition_index(0);
        let mut topic = FetchTopic::new();
        topic.set_topic_id(Uuid::random_uuid()).set_partitions(vec![partition]);
        let mut data = ShareFetchRequestData::new();
        data.set_group_id(Some("group".to_string()))
            .set_member_id(Some(Uuid::random_uuid().to_string()))
            .set_topics(vec![topic]);
        unwrap(Builder::new(data).build_version(version).expect("build"))
    }

    /// `RequestResponseTest.testSerialization`'s `SHARE_FETCH` arm, at every
    /// version: `checkRequest` and `checkErrorResponse` (the `checkResponse`
    /// half lives in `share_fetch_response.rs`).
    #[test]
    fn test_serialization_all_versions() {
        for version in ApiKeys::SHARE_FETCH.all_versions() {
            let request = AbstractRequest::ShareFetch(create_share_fetch_request(version));
            check_request(&request);
            check_error_response(&request);
        }
    }

    #[test]
    fn test_builder_version_range() {
        let builder = Builder::new(ShareFetchRequestData::new());
        assert_eq!(&ApiKeys::SHARE_FETCH, builder.api_key());
        assert_eq!(1, builder.oldest_allowed_version());
        assert_eq!(2, builder.latest_allowed_version());
    }

    /// `Builder.build(short)` rejects a RENEW acknowledgement below v2 with
    /// Java's `UnsupportedVersionException` message, and accepts it at v2.
    #[test]
    fn test_build_rejects_renew_ack_below_v2() {
        let mut data = ShareFetchRequestData::new();
        data.set_is_renew_ack(true);
        let mut builder = Builder::new(data);
        assert_unsupported_version(
            &builder.build_version(1).unwrap_err(),
            "The v1 ShareFetch does not support AcknowledgeType.RENEW",
        );
        assert!(unwrap(builder.build_version(2).unwrap()).data().is_renew_ack());
    }

    /// `Builder.build(short)` rejects a non-BATCH_OPTIMIZED acquire mode below
    /// v2, and accepts it at v2.
    #[test]
    fn test_build_rejects_record_limit_acquire_mode_below_v2() {
        let mut data = ShareFetchRequestData::new();
        data.set_share_acquire_mode(ShareAcquireMode::RecordLimit.id());
        let mut builder = Builder::new(data);
        assert_unsupported_version(
            &builder.build_version(1).unwrap_err(),
            "The v1 ShareFetch only supports ShareAcquireMode.BATCH_OPTIMIZED",
        );
        assert_eq!(1, unwrap(builder.build_version(2).unwrap()).data().share_acquire_mode());
        // BATCH_OPTIMIZED builds at v1.
        let mut data = ShareFetchRequestData::new();
        data.set_share_acquire_mode(ShareAcquireMode::BatchOptimized.id());
        assert_eq!(1, unwrap(Builder::new(data).build_version(1).unwrap()).version());
    }

    /// The renew check runs first, as in Java's `build`.
    #[test]
    fn test_build_checks_renew_before_acquire_mode() {
        let mut data = ShareFetchRequestData::new();
        data.set_is_renew_ack(true)
            .set_share_acquire_mode(ShareAcquireMode::RecordLimit.id());
        assert_unsupported_version(
            &Builder::new(data).build_version(1).unwrap_err(),
            "The v1 ShareFetch does not support AcknowledgeType.RENEW",
        );
    }

    /// `forConsumer`: send partitions in order, then acknowledgement-only
    /// partitions, batches attached to the right partition, metadata copied.
    #[test]
    fn test_for_consumer_builds_topics_in_send_then_acknowledgement_order() {
        let topic_b = Uuid::new(0, 2);
        let topic_a = Uuid::new(0, 1);
        let member_id = Uuid::new(9, 9);
        let metadata = ShareRequestMetadata::new(member_id, 4);
        let send = [
            TopicIdPartition::with_partition_topic(topic_b, 3, "b"),
            TopicIdPartition::with_partition_topic(topic_a, 1, "a"),
            TopicIdPartition::with_partition_topic(topic_b, 0, "b"),
        ];
        let acks = vec![
            (
                TopicIdPartition::with_partition_topic(topic_b, 0, "b"),
                vec![batch(5, 6, vec![1])],
            ),
            (
                TopicIdPartition::with_partition_topic(topic_a, 7, "a"),
                vec![batch(8, 8, vec![2])],
            ),
        ];
        let builder = Builder::for_consumer("grp", Some(&metadata), 500, 1, 1024, 50, 10, 0, false, &send, &[], acks);
        let data = builder.data();
        assert_eq!(Some("grp"), data.group_id().as_deref());
        assert_eq!(Some(member_id.to_string()), data.member_id().clone());
        assert_eq!(4, data.share_session_epoch());
        assert_eq!(500, data.max_wait_ms());
        assert_eq!(1, data.min_bytes());
        assert_eq!(1024, data.max_bytes());
        assert_eq!(50, data.max_records());
        assert_eq!(10, data.batch_size());
        assert_eq!(0, data.share_acquire_mode());
        assert!(!data.is_renew_ack());
        assert!(data.forgotten_topics_data().is_empty());

        let layout: Vec<(Uuid, Vec<(i32, usize)>)> = data
            .topics()
            .iter()
            .map(|t| {
                (
                    t.topic_id(),
                    t.partitions()
                        .iter()
                        .map(|p| (p.partition_index(), p.acknowledgement_batches().len()))
                        .collect(),
                )
            })
            .collect();
        assert_eq!(vec![(topic_b, vec![(3, 0), (0, 1)]), (topic_a, vec![(1, 0), (7, 1)])], layout);
        assert_eq!(5, data.topics()[0].partitions()[1].acknowledgement_batches()[0].first_offset());
        assert_eq!(8, data.topics()[1].partitions()[1].acknowledgement_batches()[0].first_offset());
    }

    /// With the final epoch the request closes the session: the send list is
    /// skipped, but acknowledgements still go out.
    #[test]
    fn test_for_consumer_with_final_epoch_skips_send() {
        let topic = Uuid::new(0, 1);
        let metadata = ShareRequestMetadata::new(Uuid::new(1, 1), ShareRequestMetadata::FINAL_EPOCH);
        let send = [TopicIdPartition::with_partition_topic(topic, 0, "t")];
        let acks = vec![(
            TopicIdPartition::with_partition_topic(topic, 2, "t"),
            vec![batch(0, 0, vec![1])],
        )];
        let builder = Builder::for_consumer("g", Some(&metadata), 0, 0, 0, 0, 0, 0, false, &send, &[], acks);
        let data = builder.data();
        assert_eq!(-1, data.share_session_epoch());
        assert_eq!(1, data.topics().len());
        let partitions: Vec<i32> = data.topics()[0].partitions().iter().map(|p| p.partition_index()).collect();
        assert_eq!(vec![2], partitions);
    }

    /// Without metadata, the member id keeps its default and the epoch is 0.
    #[test]
    fn test_for_consumer_without_metadata() {
        let builder = Builder::for_consumer("g", None, 0, 0, 0, 0, 0, 1, true, &[], &[], Vec::new());
        let data = builder.data();
        assert_eq!(Some(""), data.member_id().as_deref());
        assert_eq!(0, data.share_session_epoch());
        assert_eq!(1, data.share_acquire_mode());
        assert!(data.is_renew_ack());
        assert!(data.topics().is_empty());
    }

    /// `forget` groups the partitions per topic id, keeping their order.
    #[test]
    fn test_for_consumer_forgets_partitions_grouped_by_topic() {
        let topic_a = Uuid::new(0, 1);
        let topic_b = Uuid::new(0, 2);
        let forget = [
            TopicIdPartition::with_partition_topic(topic_a, 2, "a"),
            TopicIdPartition::with_partition_topic(topic_b, 0, "b"),
            TopicIdPartition::with_partition_topic(topic_a, 1, "a"),
        ];
        let builder = Builder::for_consumer("g", None, 0, 0, 0, 0, 0, 0, false, &[], &forget, Vec::new());
        let mut forgotten: Vec<(Uuid, Vec<i32>)> = builder
            .data()
            .forgotten_topics_data()
            .iter()
            .map(|t| (t.topic_id(), t.partitions().clone()))
            .collect();
        forgotten.sort_by_key(|(id, _)| id.least_significant_bits());
        assert_eq!(vec![(topic_a, vec![2, 1]), (topic_b, vec![0])], forgotten);
    }

    /// `shareFetchData` / `forgottenTopics` resolve names and cache the first
    /// answer, like Java's lazily-initialized fields.
    #[test]
    fn test_share_fetch_data_and_forgotten_topics_are_cached() {
        let known = Uuid::new(0, 1);
        let unknown = Uuid::new(0, 2);
        let forget = [TopicIdPartition::with_partition_topic(unknown, 4, "")];
        let send = [
            TopicIdPartition::with_partition_topic(known, 0, ""),
            TopicIdPartition::with_partition_topic(unknown, 1, ""),
        ];
        let mut builder = Builder::for_consumer("g", None, 0, 0, 0, 0, 0, 0, false, &send, &forget, Vec::new());
        let request = unwrap(builder.build_version(2).unwrap());
        let names = HashMap::from([(known, "known".to_string())]);

        let fetch_data = request.share_fetch_data(&names);
        assert_eq!(
            vec![
                TopicIdPartition::with_partition_topic(known, 0, "known"),
                TopicIdPartition::with_partition_topic(unknown, 1, ""),
            ],
            fetch_data
        );
        assert_eq!(
            vec![TopicIdPartition::with_partition_topic(unknown, 4, "")],
            request.forgotten_topics(&names)
        );

        let other_names = HashMap::from([(known, "renamed".to_string()), (unknown, "u".to_string())]);
        assert_eq!("known", request.share_fetch_data(&other_names)[0].topic());
        assert_eq!("", request.forgotten_topics(&other_names)[0].topic());
    }

    #[test]
    fn test_accessors() {
        let mut data = ShareFetchRequestData::new();
        data.set_min_bytes(1).set_max_bytes(2).set_max_wait_ms(3);
        let request = ShareFetchRequest::new(data, 2);
        assert_eq!(1, request.min_bytes());
        assert_eq!(2, request.max_bytes());
        assert_eq!(3, request.max_wait());
        assert_eq!(2, request.version());
        assert_eq!(&ApiKeys::SHARE_FETCH, request.api_key());
    }

    /// `getErrorResponse` is `ShareFetchResponse.of(error, throttle, {}, [], 0)`.
    #[test]
    fn test_get_error_response() {
        let request = create_share_fetch_request(2);
        match request.get_error_response(33, &Errors::GroupAuthorizationFailed) {
            ConcreteResponse::ShareFetch(response) => {
                assert_eq!(33, response.throttle_time_ms());
                assert_eq!(Errors::GroupAuthorizationFailed, response.error());
                assert!(response.data().responses().is_empty());
                assert!(response.data().node_endpoints().is_empty());
                assert_eq!(0, response.data().acquisition_lock_timeout_ms());
            },
            other => panic!("expected ShareFetch, got {other:?}"),
        }
    }

    /// The fields both versions share, then the topic with one acknowledging
    /// partition and the forgotten topics, as the flexible encoding writes them.
    fn encoding_data(renew: bool) -> ShareFetchRequestData {
        let mut partition = FetchPartition::new();
        partition
            .set_partition_index(1)
            .set_acknowledgement_batches(vec![batch(10, 11, vec![1, 3])]);
        let mut topic = FetchTopic::new();
        topic
            .set_topic_id(Uuid::new(0x0102_0304_0506_0708, 0x090a_0b0c_0d0e_0f10))
            .set_partitions(vec![partition]);
        let mut forgotten = ForgottenTopic::new();
        forgotten.set_topic_id(Uuid::new(0, 0x11)).set_partitions(vec![4]);
        let mut data = ShareFetchRequestData::new();
        data.set_group_id(Some("g".to_string()))
            .set_member_id(Some("m".to_string()))
            .set_share_session_epoch(2)
            .set_max_wait_ms(500)
            .set_min_bytes(1)
            .set_max_bytes(1024)
            .set_max_records(5)
            .set_batch_size(6)
            .set_share_acquire_mode(0)
            .set_is_renew_ack(renew)
            .set_topics(vec![topic])
            .set_forgotten_topics_data(vec![forgotten]);
        data
    }

    fn header() -> Vec<u8> {
        let mut bytes = vec![0x02, b'g', 0x02, b'm']; // GroupId, MemberId
        bytes.extend_from_slice(&2i32.to_be_bytes()); // ShareSessionEpoch
        bytes.extend_from_slice(&500i32.to_be_bytes()); // MaxWaitMs
        bytes.extend_from_slice(&1i32.to_be_bytes()); // MinBytes
        bytes.extend_from_slice(&1024i32.to_be_bytes()); // MaxBytes
        bytes.extend_from_slice(&5i32.to_be_bytes()); // MaxRecords (v1+)
        bytes.extend_from_slice(&6i32.to_be_bytes()); // BatchSize (v1+)
        bytes
    }

    fn topics_and_trailer() -> Vec<u8> {
        let mut bytes = vec![0x02]; // Topics: compact array of 1
        bytes.extend_from_slice(&[
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
        ]); // TopicId
        bytes.push(0x02); // Partitions: compact array of 1
        bytes.extend_from_slice(&1i32.to_be_bytes()); // PartitionIndex
        // PartitionMaxBytes is v0 only, so absent at v1+.
        bytes.push(0x02); // AcknowledgementBatches: compact array of 1
        bytes.extend_from_slice(&10i64.to_be_bytes()); // FirstOffset
        bytes.extend_from_slice(&11i64.to_be_bytes()); // LastOffset
        bytes.extend_from_slice(&[0x03, 0x01, 0x03]); // AcknowledgeTypes: [1, 3]
        bytes.push(0x00); // batch tagged fields
        bytes.push(0x00); // partition tagged fields
        bytes.push(0x00); // topic tagged fields
        bytes.push(0x02); // ForgottenTopicsData: compact array of 1
        bytes.extend_from_slice(&0i64.to_be_bytes()); // TopicId (msb)
        bytes.extend_from_slice(&0x11i64.to_be_bytes()); // TopicId (lsb)
        bytes.push(0x02); // Partitions: compact array of 1
        bytes.extend_from_slice(&4i32.to_be_bytes());
        bytes.push(0x00); // forgotten topic tagged fields
        bytes.push(0x00); // request tagged fields
        bytes
    }

    /// Byte-level v2 encoding, derived by hand from `ShareFetchRequest.json` in
    /// field order: v2 adds `ShareAcquireMode` (int8) and `IsRenewAck` (bool)
    /// after `BatchSize`.
    #[test]
    fn test_v2_encoding() {
        let mut expected = header();
        expected.push(0x00); // ShareAcquireMode: BATCH_OPTIMIZED
        expected.push(0x01); // IsRenewAck: true
        expected.extend(topics_and_trailer());
        let mut request = Builder::new(encoding_data(true)).build_version(2).unwrap();
        assert_eq!(expected, request.serialize().unwrap().into_buffer());

        let parsed = ShareFetchRequest::parse(&mut ByteBufferAccessor::new(expected), 2).unwrap();
        assert_eq!(&encoding_data(true), parsed.data());
    }

    /// The same request at v1 drops the two v2 fields.
    #[test]
    fn test_v1_encoding() {
        let mut expected = header();
        expected.extend(topics_and_trailer());
        let mut request = Builder::new(encoding_data(false)).build_version(1).unwrap();
        assert_eq!(expected, request.serialize().unwrap().into_buffer());
    }
}
