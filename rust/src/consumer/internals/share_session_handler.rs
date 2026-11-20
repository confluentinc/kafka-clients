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

//! Per-node share session state (KIP-932).
//!
//! Translated from `org.apache.kafka.clients.consumer.internals.ShareSessionHandler`.
//! Java's `LogContext` is dropped, as for the other consumer internals; the
//! `log` crate carries the messages.

use std::collections::HashMap;

use indexmap::IndexMap;
use log::{debug, info, log_enabled};

use crate::common::Error;
use crate::common::TopicIdPartition;
use crate::common::TopicPartition;
use crate::common::Uuid;
use crate::common::protocol::Errors;
use crate::common::requests::ShareAcknowledgeResponse;
use crate::common::requests::ShareFetchResponse;
use crate::common::requests::ShareRequestMetadata;
use crate::common::requests::share_acknowledge_request;
use crate::common::requests::share_fetch_request;
use crate::consumer::AcknowledgeType;
use crate::consumer::internals::Acknowledgements;
use crate::consumer::internals::ShareFetchConfig;
use crate::{share_acknowledge_request_data, share_fetch_request_data};

/// Maintains the share session state for connecting to a broker.
///
/// Using the protocol outlined by KIP-932, clients can create share sessions.
/// These sessions allow the client to fetch data from a set of
/// share-partitions repeatedly, without explicitly enumerating all the
/// partitions in the request and response.
///
/// `ShareSessionHandler` tracks the partitions which are in the session. It
/// also determines which partitions need to be included in each
/// `ShareFetch` / `ShareAcknowledge` request.
#[derive(Debug)]
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareSessionHandler")]
pub(crate) struct ShareSessionHandler {
    node: i32,
    member_id: Uuid,
    /// The metadata for the next `ShareFetchRequest` / `ShareAcknowledgeRequest`.
    next_metadata: ShareRequestMetadata,
    /// All the partitions in the share session. A `LinkedHashMap` in Java
    /// (translation-checklist R5).
    session_partitions: IndexMap<TopicPartition, TopicIdPartition>,
    /// The partitions to be included in the next `ShareFetch` request.
    next_partitions: IndexMap<TopicPartition, TopicIdPartition>,
    /// The acknowledgements to be included in the next `ShareFetch` /
    /// `ShareAcknowledge` request.
    next_acknowledgements: IndexMap<TopicIdPartition, Acknowledgements>,
}

impl ShareSessionHandler {
    /// Creates the handler for the share session of `member_id` on broker
    /// `node`, starting at the initial epoch.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareSessionHandler#ShareSessionHandler")]
    pub(crate) fn new(node: i32, member_id: Uuid) -> Self {
        Self {
            node,
            member_id,
            next_metadata: ShareRequestMetadata::initial_epoch(member_id),
            session_partitions: IndexMap::new(),
            next_partitions: IndexMap::new(),
            next_acknowledgements: IndexMap::new(),
        }
    }

    /// All the partitions in the share session, in the order they joined it.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareSessionHandler#sessionPartitionMap")]
    pub(crate) fn session_partition_map(&self) -> &IndexMap<TopicPartition, TopicIdPartition> {
        &self.session_partitions
    }

    /// Adds a partition to fetch from in the next `ShareFetch`, with the
    /// acknowledgements to piggy-back on it, if any.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareSessionHandler#addPartitionToFetch")]
    pub(crate) fn add_partition_to_fetch(
        &mut self,
        topic_id_partition: TopicIdPartition,
        partition_acknowledgements: Option<Acknowledgements>,
    ) {
        self.next_partitions
            .insert(topic_id_partition.topic_partition().clone(), topic_id_partition.clone());
        if let Some(partition_acknowledgements) = partition_acknowledgements {
            self.next_acknowledgements
                .insert(topic_id_partition, partition_acknowledgements);
        }
    }

    /// Adds acknowledgements for a partition that is not being fetched from.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareSessionHandler#addPartitionToAcknowledgeOnly")]
    pub(crate) fn add_partition_to_acknowledge_only(
        &mut self,
        topic_id_partition: TopicIdPartition,
        partition_acknowledgements: Acknowledgements,
    ) {
        self.next_acknowledgements
            .insert(topic_id_partition, partition_acknowledgements);
    }

    /// Whether the next request starts a new share session.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareSessionHandler#isNewSession")]
    pub(crate) fn is_new_session(&self) -> bool {
        self.next_metadata.is_new_session()
    }

    /// Whether the share session has no partitions.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareSessionHandler#isSessionEmpty")]
    pub(crate) fn is_session_empty(&self) -> bool {
        self.session_partitions.is_empty()
    }

    /// Builds the next `ShareFetch` request from the partitions and
    /// acknowledgements added since the last one, and updates the session
    /// partitions. `None` (Java's `null`) when the request can be skipped: it
    /// changes nothing in the session, carries no acknowledgements and does
    /// not close the session, and either the session is empty or
    /// `can_skip_if_request_empty` is set.
    ///
    /// The partitions and acknowledgements added are consumed in either case.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareSessionHandler#newShareFetchBuilder")]
    pub(crate) fn new_share_fetch_builder(
        &mut self,
        group_id: &str,
        share_fetch_config: &ShareFetchConfig,
        can_skip_if_request_empty: bool,
    ) -> Option<share_fetch_request::Builder> {
        let mut added: Vec<TopicIdPartition> = Vec::new();
        let mut removed: Vec<TopicIdPartition> = Vec::new();
        let mut replaced: Vec<TopicIdPartition> = Vec::new();

        if self.next_metadata.is_new_session() {
            // Add any new partitions to the session
            for (topic_partition, topic_id_partition) in &self.next_partitions {
                self.session_partitions
                    .insert(topic_partition.clone(), topic_id_partition.clone());
            }

            // If it's a new session, all the partitions must be added to the request
            added.extend(self.session_partitions.values().cloned());
        } else {
            // Iterate over the session partitions, tallying which were added.
            // `retain` visits in order and removes in place, keeping the order
            // of the rest, as Java's `Iterator#remove` on a `LinkedHashMap`.
            let next_partitions = &mut self.next_partitions;
            self.session_partitions.retain(|topic_partition, prev_data| {
                match next_partitions.shift_remove(topic_partition) {
                    Some(next_data) => {
                        // If the topic ID does not match, the topic has been recreated
                        if *prev_data != next_data {
                            next_partitions.insert(topic_partition.clone(), next_data.clone());
                            replaced.push(std::mem::replace(prev_data, next_data));
                        }
                        true
                    },
                    None => {
                        // This partition is not in the builder, so we need to remove it from the session
                        removed.push(prev_data.clone());
                        false
                    },
                }
            });

            // Add any new partitions to the session
            for (topic_partition, topic_id_partition) in &self.next_partitions {
                self.session_partitions
                    .insert(topic_partition.clone(), topic_id_partition.clone());
                added.push(topic_id_partition.clone());
            }
        }

        // The replaced topic-partitions need to be removed, and their replacements are already added
        removed.append(&mut replaced);

        let mut has_renew_acknowledgements = false;
        // A `HashMap` in Java, so the order of the acknowledgement-only
        // partitions in the request is unspecified (translation-checklist R5).
        let mut acknowledgement_batches: HashMap<
            TopicIdPartition,
            Vec<share_fetch_request_data::AcknowledgementBatch>,
        > = HashMap::new();
        for (tip, partition_acks) in &self.next_acknowledgements {
            for ack_batch in partition_acks.acknowledgement_batches() {
                if ack_batch.acknowledge_types().contains(&AcknowledgeType::Renew.id()) {
                    has_renew_acknowledgements = true;
                }
                acknowledgement_batches
                    .entry(tip.clone())
                    .or_default()
                    .push(ack_batch.to_share_fetch_request());
            }

            // If the partition is only being included in the request to send acknowledgements, we need to
            // remove it so that it doesn't get added into the share session for fetching
            let session_tip = self.session_partitions.get(tip.topic_partition());
            if session_tip != Some(tip) && !removed.contains(tip) {
                removed.push(tip.clone());
            }
        }

        self.next_partitions = IndexMap::new();
        self.next_acknowledgements = IndexMap::new();

        // If there are no changes to the share session and no acknowledgements, we can sometimes skip sending an
        // empty request
        if added.is_empty()
            && removed.is_empty()
            && acknowledgement_batches.is_empty()
            && !self.next_metadata.is_final_epoch()
        {
            // If the share session is empty, there are no partitions to fetch from and we can always skip
            if self.session_partitions.is_empty() {
                debug!("Skipping sending empty ShareFetch because share partitions empty");
                return None;
            }

            // If the share session is not empty, but we do not want to fetch records for this node, we can skip
            if can_skip_if_request_empty {
                debug!("Skipping sending empty ShareFetch because no share session changes or acknowledgements");
                return None;
            }
        }

        if log_enabled!(log::Level::Debug) {
            debug!(
                "Build ShareFetch {} for node {}. Added {}, removed {} out of {}",
                self.next_metadata,
                self.node,
                Self::topic_id_partitions_to_log_string(added.iter()),
                Self::topic_id_partitions_to_log_string(removed.iter()),
                Self::topic_id_partitions_to_log_string(self.session_partitions.values()),
            );
        }

        let acquire_mode = share_fetch_config.share_acquire_mode.id();
        let builder = if has_renew_acknowledgements {
            // If the request has renew acknowledgements, the ShareFetch is only used to send the acknowledgements
            // and potentially update the share session. The parameters for wait time, number of bytes and number of
            // records are all zero.
            share_fetch_request::Builder::for_consumer(
                group_id,
                Some(&self.next_metadata),
                0,
                0,
                0,
                0,
                0,
                acquire_mode,
                true,
                &added,
                &removed,
                acknowledgement_batches,
            )
        } else if can_skip_if_request_empty {
            // The request contains changes to the share session or acknowledgements only. The parameters for wait
            // time, number of bytes and number of records are all zero.
            share_fetch_request::Builder::for_consumer(
                group_id,
                Some(&self.next_metadata),
                0,
                0,
                0,
                0,
                0,
                acquire_mode,
                false,
                &added,
                &removed,
                acknowledgement_batches,
            )
        } else {
            share_fetch_request::Builder::for_consumer(
                group_id,
                Some(&self.next_metadata),
                share_fetch_config.max_wait_ms,
                share_fetch_config.min_bytes,
                share_fetch_config.max_bytes,
                share_fetch_config.max_poll_records,
                share_fetch_config.max_poll_records,
                acquire_mode,
                false,
                &added,
                &removed,
                acknowledgement_batches,
            )
        };
        Some(builder)
    }

    /// Builds the next `ShareAcknowledge` request from the acknowledgements
    /// added since the last request. `None` (Java's `null`) when the session
    /// has not started yet: a share session cannot be started with a
    /// `ShareAcknowledge` request, and the partitions and acknowledgements
    /// added are dropped (the caller completes the acknowledgements).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareSessionHandler#newShareAcknowledgeBuilder")]
    pub(crate) fn new_share_acknowledge_builder(
        &mut self,
        group_id: &str,
    ) -> Option<share_acknowledge_request::Builder> {
        if self.next_metadata.is_new_session() {
            // A share session cannot be started with a ShareAcknowledge request. The caller handles completing the
            // acks.
            self.next_partitions.clear();
            self.next_acknowledgements.clear();
            return None;
        }

        let mut has_renew_acknowledgements = false;
        // A `HashMap` in Java (translation-checklist R5).
        let mut acknowledgement_batches: HashMap<
            TopicIdPartition,
            Vec<share_acknowledge_request_data::AcknowledgementBatch>,
        > = HashMap::new();
        for (tip, partition_acks) in &self.next_acknowledgements {
            for ack_batch in partition_acks.acknowledgement_batches() {
                if ack_batch.acknowledge_types().contains(&AcknowledgeType::Renew.id()) {
                    has_renew_acknowledgements = true;
                }
                acknowledgement_batches
                    .entry(tip.clone())
                    .or_default()
                    .push(ack_batch.to_share_acknowledge_request());
            }
        }

        self.next_acknowledgements = IndexMap::new();

        Some(share_acknowledge_request::Builder::for_consumer(
            group_id,
            Some(&self.next_metadata),
            has_renew_acknowledgements,
            acknowledgement_batches,
        ))
    }

    /// The partitions for a log message: their count, or each of them at
    /// TRACE level.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareSessionHandler#topicIdPartitionsToLogString")]
    fn topic_id_partitions_to_log_string<'a>(
        partitions: impl ExactSizeIterator<Item = &'a TopicIdPartition>,
    ) -> String {
        if !log_enabled!(log::Level::Trace) {
            return format!("{} partition(s)", partitions.len());
        }
        let joined: Vec<String> = partitions.map(ToString::to_string).collect();
        format!("({})", joined.join(", "))
    }

    /// Handles the `ShareFetch` response.
    ///
    /// Returns `true` if the response is well-formed; `false` if it can't be
    /// processed because of a top-level error, in which case the next request
    /// either re-establishes the session (`SHARE_SESSION_NOT_FOUND`,
    /// `INVALID_SHARE_SESSION_EPOCH`, `SHARE_SESSION_LIMIT_REACHED`) or moves
    /// to the next epoch.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareSessionHandler#handleResponse")]
    pub(crate) fn handle_response_with_share_fetch_response(
        &mut self,
        response: &ShareFetchResponse,
        _version: i16,
    ) -> bool {
        let error = response.error();
        if matches!(
            error,
            Errors::ShareSessionNotFound | Errors::InvalidShareSessionEpoch | Errors::ShareSessionLimitReached
        ) {
            // These share-session errors are handled the same way: close the existing session (if any) and
            // re-establish it by re-sending a full ShareFetch request. For SHARE_SESSION_NOT_FOUND /
            // INVALID_SHARE_SESSION_EPOCH this recovers on the next request; for SHARE_SESSION_LIMIT_REACHED it
            // succeeds once the broker's session cache has capacity.
            debug!(
                "Node {} was unable to process the ShareFetch request with {}: {}. Re-sending a full ShareFetch \
                 request, which closes the existing session on the broker and establishes a new one.",
                self.node, self.next_metadata, error
            );
            self.next_metadata = self.next_metadata.next_close_existing_attempt_new();
            return false;
        }

        if error != Errors::None {
            info!(
                "Node {} was unable to process the ShareFetch request with {}: {}.",
                self.node, self.next_metadata, error
            );
            self.next_metadata = self.next_metadata.next_epoch();
            return false;
        }

        // The share session was continued by the server
        debug!(
            "Node {} sent a ShareFetch response with throttleTimeMs = {} for session {}",
            self.node,
            response.throttle_time_ms(),
            self.member_id
        );
        self.next_metadata = self.next_metadata.next_epoch();
        true
    }

    /// Handles the `ShareAcknowledge` response.
    ///
    /// Returns `true` if the response is well-formed; `false` if it can't be
    /// processed because of a top-level error, in which case the next request
    /// either re-establishes the session (`SHARE_SESSION_NOT_FOUND`,
    /// `INVALID_SHARE_SESSION_EPOCH`) or moves to the next epoch.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareSessionHandler#handleResponse")]
    pub(crate) fn handle_response_with_share_acknowledge_response(
        &mut self,
        response: &ShareAcknowledgeResponse,
        _version: i16,
    ) -> bool {
        let error = response.error();
        if matches!(error, Errors::ShareSessionNotFound | Errors::InvalidShareSessionEpoch) {
            // These share-session errors are handled the same way: close the existing session (if any) and
            // re-establish it by re-sending a full ShareAcknowledge request, which recovers on the next request.
            debug!(
                "Node {} was unable to process the ShareAcknowledge request with {}: {}. Re-sending a full \
                 ShareAcknowledge request, which closes the existing session on the broker and establishes a new \
                 one.",
                self.node, self.next_metadata, error
            );
            self.next_metadata = self.next_metadata.next_close_existing_attempt_new();
            return false;
        }

        if error != Errors::None {
            info!(
                "Node {} was unable to process the ShareAcknowledge request with {}: {}.",
                self.node, self.next_metadata, error
            );
            self.next_metadata = self.next_metadata.next_epoch();
            return false;
        }

        // The share session was continued by the server
        debug!(
            "Node {} sent a ShareAcknowledge response with throttleTimeMs = {} for session {}",
            self.node,
            response.throttle_time_ms(),
            self.member_id
        );
        self.next_metadata = self.next_metadata.next_epoch();
        true
    }

    /// The client will initiate the session close on the next `ShareFetch`
    /// request.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareSessionHandler#notifyClose")]
    pub(crate) fn notify_close(&mut self) {
        debug!(
            "Set the metadata for next ShareFetch request to close the share session memberId={}",
            self.next_metadata.member_id()
        );
        self.next_metadata = self.next_metadata.final_epoch();
    }

    /// Handles an error sending the prepared request. When a network error
    /// occurs, the next request closes any existing share session and tries
    /// to create a new one.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareSessionHandler#handleError")]
    pub(crate) fn handle_error(&mut self, t: &Error) {
        info!(
            "Error sending fetch request {} to node {}: {}",
            self.next_metadata, self.node, t
        );
        self.next_metadata = self.next_metadata.next_close_existing_attempt_new();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ShareFetchRequestData;
    use crate::common::IsolationLevel;
    use crate::common::protocol::ApiKeys;
    use crate::common::requests::{AbstractRequest, RequestBuilder};
    use crate::consumer::ConsumerConfig;
    use crate::consumer::internals::ShareAcquireMode;
    use crate::share_fetch_response_data::PartitionData;

    const GROUP_ID: &str = "G1";

    /// `ShareSessionHandlerTest.DEFAULT_SHARE_FETCH_CONFIG`.
    fn default_share_fetch_config() -> ShareFetchConfig {
        share_fetch_config(ShareAcquireMode::BatchOptimized)
    }

    /// `ShareSessionHandlerTest.SHARE_FETCH_CONFIG_RECORD_LIMIT`.
    fn share_fetch_config_record_limit() -> ShareFetchConfig {
        share_fetch_config(ShareAcquireMode::RecordLimit)
    }

    fn share_fetch_config(share_acquire_mode: ShareAcquireMode) -> ShareFetchConfig {
        ShareFetchConfig::new(
            ConsumerConfig::DEFAULT_FETCH_MIN_BYTES,
            ConsumerConfig::DEFAULT_FETCH_MAX_BYTES,
            ConsumerConfig::DEFAULT_FETCH_MAX_WAIT_MS,
            ConsumerConfig::DEFAULT_MAX_PARTITION_FETCH_BYTES,
            ConsumerConfig::DEFAULT_MAX_POLL_RECORDS,
            true,
            ConsumerConfig::DEFAULT_CLIENT_RACK,
            IsolationLevel::ReadUncommitted,
            share_acquire_mode,
        )
    }

    /// `ShareSessionHandlerTest.shareFetchConfigProvider()`: the
    /// `@MethodSource` rows, in order.
    fn share_fetch_config_provider() -> [ShareFetchConfig; 2] {
        [default_share_fetch_config(), share_fetch_config_record_limit()]
    }

    fn tip(topic_id: Uuid, partition: i32, topic: &str) -> TopicIdPartition {
        TopicIdPartition::with_partition_topic(topic_id, partition, topic)
    }

    fn req_map(entries: &[TopicIdPartition]) -> IndexMap<TopicPartition, TopicIdPartition> {
        entries
            .iter()
            .map(|entry| (entry.topic_partition().clone(), entry.clone()))
            .collect()
    }

    fn req_fetch_list(
        request_data: &ShareFetchRequestData,
        topic_names: &HashMap<Uuid, String>,
    ) -> Vec<TopicIdPartition> {
        request_data
            .topics
            .iter()
            .flat_map(|topic| {
                topic
                    .partitions
                    .iter()
                    .map(|partition| tip(topic.topic_id, partition.partition_index, &topic_names[&topic.topic_id]))
            })
            .collect()
    }

    fn req_forget_list(
        request_data: &ShareFetchRequestData,
        topic_names: &HashMap<Uuid, String>,
    ) -> Vec<TopicIdPartition> {
        request_data
            .forgotten_topics_data
            .iter()
            .flat_map(|topic| {
                topic
                    .partitions
                    .iter()
                    .map(|&partition| tip(topic.topic_id, partition, &topic_names[&topic.topic_id]))
            })
            .collect()
    }

    /// `assertMapEquals`: same entries in the same order.
    fn assert_map_equals(
        expected: &IndexMap<TopicPartition, TopicIdPartition>,
        actual: &IndexMap<TopicPartition, TopicIdPartition>,
    ) {
        let mut actual_iter = actual.iter();
        for (i, (expected_key, expected_value)) in expected.iter().enumerate() {
            let i = i + 1;
            let Some((actual_key, actual_value)) = actual_iter.next() else {
                panic!("Element {i} not found.");
            };
            assert_eq!(
                expected_key, actual_key,
                "Element {i} had a different TopicPartition than expected."
            );
            assert_eq!(
                expected_value, actual_value,
                "Element {i} had different PartitionData than expected."
            );
        }
        if actual_iter.next().is_some() {
            panic!("Unexpected element {} found.", expected.len() + 1);
        }
    }

    /// `assertListEquals`: the same partitions, in any order.
    fn assert_list_equals(expected: &[TopicIdPartition], actual: &[TopicIdPartition]) {
        for expected_part in expected {
            assert!(
                actual.contains(expected_part),
                "Failed to find expected partition {expected_part}"
            );
        }
        for actual_part in actual {
            assert!(expected.contains(actual_part), "Found unexpected partition {actual_part}");
        }
    }

    /// `ShareSessionHandlerTest.RespEntry` + `buildResponseData`.
    fn build_response_data(entries: &[(&str, i32, Uuid)]) -> Vec<(TopicIdPartition, PartitionData)> {
        entries
            .iter()
            .map(|&(topic, partition, topic_id)| {
                let mut partition_data = PartitionData::new();
                partition_data.set_partition_index(partition);
                (tip(topic_id, partition, topic), partition_data)
            })
            .collect()
    }

    fn response(error: Errors, entries: &[(&str, i32, Uuid)]) -> ShareFetchResponse {
        ShareFetchResponse::of(error, 0, build_response_data(entries), &[], 0)
    }

    fn add_topic_id(topic_names: &mut HashMap<Uuid, String>, name: &str) -> Uuid {
        let id = Uuid::random_uuid();
        topic_names.insert(id, name.to_string());
        id
    }

    /// Java's `builder.build().data()`.
    fn build_data(builder: Option<share_fetch_request::Builder>) -> ShareFetchRequestData {
        let mut builder = builder.expect("a ShareFetch request was built");
        match builder.build().unwrap() {
            AbstractRequest::ShareFetch(request) => request.data().clone(),
            other => panic!("expected ShareFetch, got {other:?}"),
        }
    }

    fn latest() -> i16 {
        ApiKeys::SHARE_FETCH.latest_version()
    }

    /// `ShareSessionHandlerTest.testShareSession`, `@EnumSource` over
    /// `INVALID_SHARE_SESSION_EPOCH`, `SHARE_SESSION_NOT_FOUND`,
    /// `SHARE_SESSION_LIMIT_REACHED`.
    #[test]
    fn test_share_session() {
        for error in [
            Errors::InvalidShareSessionEpoch,
            Errors::ShareSessionNotFound,
            Errors::ShareSessionLimitReached,
        ] {
            let member_id = Uuid::random_uuid();
            let mut handler = ShareSessionHandler::new(1, member_id);

            let mut topic_names = HashMap::new();
            let foo_id = add_topic_id(&mut topic_names, "foo");
            let foo0 = tip(foo_id, 0, "foo");
            let foo1 = tip(foo_id, 1, "foo");
            handler.add_partition_to_fetch(foo0.clone(), None);
            handler.add_partition_to_fetch(foo1.clone(), None);
            let request_data1 =
                build_data(handler.new_share_fetch_builder(GROUP_ID, &default_share_fetch_config(), false));
            let expected_to_send1 = vec![tip(foo_id, 0, "foo"), tip(foo_id, 1, "foo")];
            assert_list_equals(&expected_to_send1, &req_fetch_list(&request_data1, &topic_names));
            assert_eq!(Some(member_id.to_string()), request_data1.member_id);

            let resp = response(Errors::None, &[("foo", 0, foo_id), ("foo", 1, foo_id)]);
            handler.handle_response_with_share_fetch_response(&resp, latest());

            // Test a fetch request which adds one partition
            let bar_id = add_topic_id(&mut topic_names, "bar");
            let bar0 = tip(bar_id, 0, "bar");
            handler.add_partition_to_fetch(foo0.clone(), None);
            handler.add_partition_to_fetch(foo1.clone(), None);
            handler.add_partition_to_fetch(bar0.clone(), None);
            let request_data2 =
                build_data(handler.new_share_fetch_builder(GROUP_ID, &default_share_fetch_config(), false));
            assert_map_equals(
                &req_map(&[tip(foo_id, 0, "foo"), tip(foo_id, 1, "foo"), tip(bar_id, 0, "bar")]),
                handler.session_partition_map(),
            );
            let expected_to_send2 = vec![tip(bar_id, 0, "bar")];
            assert_list_equals(&expected_to_send2, &req_fetch_list(&request_data2, &topic_names));

            let resp2 = response(Errors::None, &[("foo", 1, foo_id)]);
            handler.handle_response_with_share_fetch_response(&resp2, latest());

            // A top-level error code will reset the session epoch
            let resp3 = response(error, &[]);
            handler.handle_response_with_share_fetch_response(&resp3, latest());

            let request_data4 =
                build_data(handler.new_share_fetch_builder(GROUP_ID, &default_share_fetch_config(), false));
            assert_eq!(request_data2.member_id, request_data4.member_id);
            assert_eq!(ShareRequestMetadata::INITIAL_EPOCH, request_data4.share_session_epoch);
            assert_map_equals(
                &req_map(&[tip(foo_id, 0, "foo"), tip(foo_id, 1, "foo"), tip(bar_id, 0, "bar")]),
                handler.session_partition_map(),
            );
            let expected_to_send4 = vec![tip(foo_id, 0, "foo"), tip(foo_id, 1, "foo"), tip(bar_id, 0, "bar")];
            assert_list_equals(&expected_to_send4, &req_fetch_list(&request_data4, &topic_names));
        }
    }

    /// `ShareSessionHandlerTest.testPartitionRemoval`, over
    /// `shareFetchConfigProvider`.
    #[test]
    fn test_partition_removal() {
        for share_fetch_config in share_fetch_config_provider() {
            let member_id = Uuid::random_uuid();
            let mut handler = ShareSessionHandler::new(1, member_id);

            let mut topic_names = HashMap::new();
            let foo_id = add_topic_id(&mut topic_names, "foo");
            let bar_id = add_topic_id(&mut topic_names, "bar");
            let foo0 = tip(foo_id, 0, "foo");
            let foo1 = tip(foo_id, 1, "foo");
            let bar0 = tip(bar_id, 0, "bar");
            handler.add_partition_to_fetch(foo0, None);
            handler.add_partition_to_fetch(foo1.clone(), None);
            handler.add_partition_to_fetch(bar0, None);
            let request_data1 = build_data(handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, false));
            assert_map_equals(
                &req_map(&[tip(foo_id, 0, "foo"), tip(foo_id, 1, "foo"), tip(bar_id, 0, "bar")]),
                handler.session_partition_map(),
            );
            let expected_to_send1 = vec![tip(foo_id, 0, "foo"), tip(foo_id, 1, "foo"), tip(bar_id, 0, "bar")];
            assert_list_equals(&expected_to_send1, &req_fetch_list(&request_data1, &topic_names));
            assert_eq!(Some(member_id.to_string()), request_data1.member_id);

            let resp = response(Errors::None, &[("foo", 0, foo_id), ("foo", 1, foo_id), ("bar", 0, bar_id)]);
            handler.handle_response_with_share_fetch_response(&resp, latest());

            // Test a fetch request which removes two partitions
            handler.add_partition_to_fetch(foo1.clone(), None);
            let request_data2 = build_data(handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, false));
            assert_eq!(Some(member_id.to_string()), request_data2.member_id);
            assert_eq!(1, request_data2.share_session_epoch);
            assert_map_equals(&req_map(&[tip(foo_id, 1, "foo")]), handler.session_partition_map());
            assert!(request_data2.topics.is_empty());
            let expected_to_forget2 = vec![tip(foo_id, 0, "foo"), tip(bar_id, 0, "bar")];
            assert_list_equals(&expected_to_forget2, &req_forget_list(&request_data2, &topic_names));

            // A top-level error code will reset the session epoch
            let resp2 = response(Errors::InvalidShareSessionEpoch, &[]);
            handler.handle_response_with_share_fetch_response(&resp2, latest());

            handler.add_partition_to_fetch(foo1, None);
            let request_data3 = build_data(handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, false));
            assert_eq!(Some(member_id.to_string()), request_data3.member_id);
            assert_eq!(ShareRequestMetadata::INITIAL_EPOCH, request_data3.share_session_epoch);
            assert_map_equals(&req_map(&[tip(foo_id, 1, "foo")]), handler.session_partition_map());
            let expected_to_send3 = vec![tip(foo_id, 1, "foo")];
            assert_list_equals(&expected_to_send3, &req_fetch_list(&request_data3, &topic_names));
        }
    }

    /// `ShareSessionHandlerTest.testTopicIdReplaced`, over
    /// `shareFetchConfigProvider`.
    #[test]
    fn test_topic_id_replaced() {
        for share_fetch_config in share_fetch_config_provider() {
            let member_id = Uuid::random_uuid();
            let mut handler = ShareSessionHandler::new(1, member_id);

            let mut topic_names = HashMap::new();
            let topic_id1 = add_topic_id(&mut topic_names, "foo");
            let tp = tip(topic_id1, 0, "foo");
            handler.add_partition_to_fetch(tp.clone(), None);
            let request_data1 = build_data(handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, false));
            assert_map_equals(&req_map(&[tip(topic_id1, 0, "foo")]), handler.session_partition_map());
            let expected_to_send1 = vec![tip(topic_id1, 0, "foo")];
            assert_list_equals(&expected_to_send1, &req_fetch_list(&request_data1, &topic_names));

            let resp = response(Errors::None, &[("foo", 0, topic_id1)]);
            handler.handle_response_with_share_fetch_response(&resp, latest());

            // Try to add a new topic ID
            let topic_id2 = add_topic_id(&mut topic_names, "foo");
            let tp2 = tip(topic_id2, 0, "foo");
            // Use the same data besides the topic ID
            handler.add_partition_to_fetch(tp2.clone(), None);
            let request_data2 = build_data(handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, false));

            // If we started with an ID, only a new ID will count towards replaced.
            // The old topic ID partition should be forgotten, and the new one should be fetched.
            assert_eq!(vec![tp], req_forget_list(&request_data2, &topic_names));
            assert_map_equals(&req_map(&[tip(topic_id2, 0, "foo")]), handler.session_partition_map());
            assert_list_equals(&[tp2], &req_fetch_list(&request_data2, &topic_names));

            // Should have the same session ID, and next epoch and can use topic IDs if it ended with topic IDs.
            assert_eq!(Some(member_id.to_string()), request_data2.member_id, "Did not use same session");
            assert_eq!(1, request_data2.share_session_epoch, "Did not have correct epoch");
        }
    }

    /// `ShareSessionHandlerTest.testPartitionForgottenOnAcknowledgeOnly`, over
    /// `shareFetchConfigProvider`.
    #[test]
    fn test_partition_forgotten_on_acknowledge_only() {
        for share_fetch_config in share_fetch_config_provider() {
            let member_id = Uuid::random_uuid();
            let mut handler = ShareSessionHandler::new(1, member_id);

            // We want to test when all topics are removed from the session
            let mut topic_names = HashMap::new();
            let topic_id = add_topic_id(&mut topic_names, "foo");
            let foo0 = tip(topic_id, 0, "foo");
            handler.add_partition_to_fetch(foo0.clone(), None);
            let request_data1 = build_data(handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, false));
            assert_map_equals(&req_map(std::slice::from_ref(&foo0)), handler.session_partition_map());
            let expected_to_send1 = vec![tip(topic_id, 0, "foo")];
            assert_list_equals(&expected_to_send1, &req_fetch_list(&request_data1, &topic_names));

            let resp = response(Errors::None, &[("foo", 0, topic_id)]);
            handler.handle_response_with_share_fetch_response(&resp, latest());

            // Remove the topic from the session by setting acknowledgements only - this is not asking to fetch
            // records
            let request_data2 = build_data(handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, false));
            handler.add_partition_to_acknowledge_only(foo0.clone(), Acknowledgements::empty());
            assert_eq!(vec![foo0], req_forget_list(&request_data2, &topic_names));

            // Should have the same session ID, next epoch, and same ID usage
            assert_eq!(Some(member_id.to_string()), request_data2.member_id, "Did not use same session");
            assert_eq!(1, request_data2.share_session_epoch, "Did not have correct epoch");
        }
    }

    /// `ShareSessionHandlerTest.testForgottenPartitions`, over
    /// `shareFetchConfigProvider`.
    #[test]
    fn test_forgotten_partitions() {
        for share_fetch_config in share_fetch_config_provider() {
            let member_id = Uuid::random_uuid();
            let mut handler = ShareSessionHandler::new(1, member_id);

            // We want to test when all topics are removed from the session
            let mut topic_names = HashMap::new();
            let topic_id = add_topic_id(&mut topic_names, "foo");
            let foo0 = tip(topic_id, 0, "foo");
            handler.add_partition_to_fetch(foo0.clone(), None);
            let request_data1 = build_data(handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, false));
            assert_map_equals(&req_map(std::slice::from_ref(&foo0)), handler.session_partition_map());
            let expected_to_send1 = vec![tip(topic_id, 0, "foo")];
            assert_list_equals(&expected_to_send1, &req_fetch_list(&request_data1, &topic_names));

            let resp = response(Errors::None, &[("foo", 0, topic_id)]);
            handler.handle_response_with_share_fetch_response(&resp, latest());

            // Remove the topic from the session
            let request_data2 = build_data(handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, false));
            assert_eq!(vec![foo0], req_forget_list(&request_data2, &topic_names));

            // Should have the same session ID, next epoch, and same ID usage
            assert_eq!(Some(member_id.to_string()), request_data2.member_id, "Did not use same session");
            assert_eq!(1, request_data2.share_session_epoch, "Did not have correct epoch");
        }
    }

    /// `ShareSessionHandlerTest.testAddNewIdAfterTopicRemovedFromSession`,
    /// over `shareFetchConfigProvider`.
    #[test]
    fn test_add_new_id_after_topic_removed_from_session() {
        for share_fetch_config in share_fetch_config_provider() {
            let member_id = Uuid::random_uuid();
            let mut handler = ShareSessionHandler::new(1, member_id);

            let mut topic_names = HashMap::new();
            let topic_id = add_topic_id(&mut topic_names, "foo");
            handler.add_partition_to_fetch(tip(topic_id, 0, "foo"), None);
            let request_data1 = build_data(handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, false));
            assert_map_equals(&req_map(&[tip(topic_id, 0, "foo")]), handler.session_partition_map());
            let expected_to_send1 = vec![tip(topic_id, 0, "foo")];
            assert_list_equals(&expected_to_send1, &req_fetch_list(&request_data1, &topic_names));

            let resp = response(Errors::None, &[("foo", 0, topic_id)]);
            handler.handle_response_with_share_fetch_response(&resp, latest());

            // Remove the partition from the session
            let request_data2 = build_data(handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, false));
            assert!(handler.session_partition_map().is_empty());
            assert!(request_data2.topics.is_empty());
            let resp2 = response(Errors::None, &[]);
            handler.handle_response_with_share_fetch_response(&resp2, latest());

            // After the topic is removed, add a recreated topic with a new ID
            let topic_id2 = add_topic_id(&mut topic_names, "foo");
            handler.add_partition_to_fetch(tip(topic_id2, 0, "foo"), None);
            let request_data3 = build_data(handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, false));

            // Should have the same session ID and epoch 2.
            assert_eq!(Some(member_id.to_string()), request_data3.member_id, "Did not use same session");
            assert_eq!(2, request_data3.share_session_epoch, "Did not have the correct session epoch");
        }
    }

    /// `ShareSessionHandlerTest.testNextAcknowledgementsClearedOnInvalidRequest`,
    /// over `shareFetchConfigProvider`.
    #[test]
    fn test_next_acknowledgements_cleared_on_invalid_request() {
        for share_fetch_config in share_fetch_config_provider() {
            let member_id = Uuid::random_uuid();
            let mut handler = ShareSessionHandler::new(1, member_id);

            let mut topic_names = HashMap::new();
            let foo_id = add_topic_id(&mut topic_names, "foo");
            let foo0 = tip(foo_id, 0, "foo");

            let mut acknowledgements = Acknowledgements::empty();
            acknowledgements.add(0, Some(AcknowledgeType::Accept));

            handler.add_partition_to_fetch(foo0, Some(acknowledgements));

            // As we start with a ShareAcknowledge on epoch 0, we expect a null response.
            assert!(handler.new_share_acknowledge_builder(GROUP_ID).is_none());

            // Attempt a new ShareFetch
            let foo1 = tip(foo_id, 1, "foo");
            handler.add_partition_to_fetch(foo1, None);
            let request_data = build_data(handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, false));

            // We should have cleared the unsent acknowledgements before this ShareFetch.
            assert_eq!(0, request_data.topics[0].partitions[0].acknowledgement_batches.len());

            let expected_to_send1 = vec![tip(foo_id, 1, "foo")];
            assert_list_equals(&expected_to_send1, &req_fetch_list(&request_data, &topic_names));
            assert_eq!(Some(member_id.to_string()), request_data.member_id);
        }
    }

    /// `ShareSessionHandlerTest.testCanSkipIfRequestEmpty`.
    #[test]
    fn test_can_skip_if_request_empty() {
        let share_fetch_config = share_fetch_config_record_limit();

        let member_id = Uuid::random_uuid();
        let mut handler = ShareSessionHandler::new(1, member_id);

        let mut topic_names = HashMap::new();
        let foo_id = add_topic_id(&mut topic_names, "foo");
        let foo0 = tip(foo_id, 0, "foo");

        let mut acknowledgements = Acknowledgements::empty();
        acknowledgements.add(0, Some(AcknowledgeType::Accept));

        // The request cannot be skipped when a topic-partition is added to the share session.
        handler.add_partition_to_fetch(foo0.clone(), None);
        let builder = handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, true);
        assert!(builder.is_some());

        let resp = response(Errors::None, &[("foo", 0, foo_id)]);
        handler.handle_response_with_share_fetch_response(&resp, latest());

        // The request can be skipped when the same topic-partition is already in the share session.
        handler.add_partition_to_fetch(foo0.clone(), None);
        let builder = handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, true);
        assert!(builder.is_none());

        // The request cannot be skipped when there are acknowledgements.
        handler.add_partition_to_fetch(foo0, Some(acknowledgements));
        let builder = handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, true);
        assert!(builder.is_some());
        handler.handle_response_with_share_fetch_response(&resp, latest());

        // The request cannot be skipped when the topic-partition is removed from the share session.
        let builder = handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, true);
        assert!(builder.is_some());
        handler.handle_response_with_share_fetch_response(&response(Errors::None, &[]), latest());

        // The request can be skipped when the share session is empty.
        let builder = handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, true);
        assert!(builder.is_none());
    }

    /// `ShareSessionHandlerTest.testSkipEmptyShareFetchWhenSessionEmpty`.
    #[test]
    fn test_skip_empty_share_fetch_when_session_empty() {
        // Using the default (non record_limit) config, canSkipIfRequestEmpty is false.
        let share_fetch_config = default_share_fetch_config();

        let member_id = Uuid::random_uuid();
        let mut handler = ShareSessionHandler::new(1, member_id);

        let mut topic_names = HashMap::new();
        let foo_id = add_topic_id(&mut topic_names, "foo");
        let foo0 = tip(foo_id, 0, "foo");

        // Adding a partition to the session builds a request.
        handler.add_partition_to_fetch(foo0.clone(), None);
        assert!(handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, false).is_some());

        let resp = response(Errors::None, &[("foo", 0, foo_id)]);
        handler.handle_response_with_share_fetch_response(&resp, latest());

        // A non-empty session with no changes still builds a request in the default mode, since we want to fetch
        // records.
        handler.add_partition_to_fetch(foo0, None);
        assert!(handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, false).is_some());
        handler.handle_response_with_share_fetch_response(&resp, latest());

        // Removing the only partition from the session builds a request to forget it.
        assert!(handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, false).is_some());
        handler.handle_response_with_share_fetch_response(&response(Errors::None, &[]), latest());

        // Once the session is empty, no request is built even though canSkipIfRequestEmpty is false.
        assert!(handler.session_partition_map().is_empty());
        assert!(handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, false).is_none());
    }

    /// `ShareSessionHandlerTest.testAcknowledgeOnlyPartitionNotInSessionIsForgotten`,
    /// over `shareFetchConfigProvider`.
    #[test]
    fn test_acknowledge_only_partition_not_in_session_is_forgotten() {
        for share_fetch_config in share_fetch_config_provider() {
            let member_id = Uuid::random_uuid();
            let mut handler = ShareSessionHandler::new(1, member_id);

            let mut topic_names = HashMap::new();
            let foo_id = add_topic_id(&mut topic_names, "foo");
            let bar_id = add_topic_id(&mut topic_names, "bar");
            let foo0 = tip(foo_id, 0, "foo");
            let bar0 = tip(bar_id, 0, "bar");

            // Establish the session with foo0.
            handler.add_partition_to_fetch(foo0.clone(), None);
            handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, false);
            let resp = response(Errors::None, &[("foo", 0, foo_id), ("bar", 0, bar_id)]);
            handler.handle_response_with_share_fetch_response(&resp, latest());

            // Continue fetching foo0, and send acknowledgements for bar0, which is not part of the session.
            let mut acknowledgements = Acknowledgements::empty();
            acknowledgements.add(0, Some(AcknowledgeType::Accept));
            handler.add_partition_to_fetch(foo0.clone(), None);
            handler.add_partition_to_acknowledge_only(bar0.clone(), acknowledgements);
            let request_data = build_data(handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, false));

            // bar0 is not added to the session; it is placed in the forgotten list so that it is removed from the
            // share session on the broker once its acknowledgements have been sent.
            assert_map_equals(&req_map(&[foo0]), handler.session_partition_map());
            assert_eq!(vec![bar0], req_forget_list(&request_data, &topic_names));

            // The acknowledgements for bar0 are still included in the request.
            let bar_partition = request_data
                .topics
                .iter()
                .filter(|topic| topic.topic_id == bar_id)
                .flat_map(|topic| topic.partitions.iter())
                .next()
                .unwrap();
            assert_eq!(1, bar_partition.acknowledgement_batches.len());
        }
    }

    /// `ShareSessionHandlerTest.testCloseEmptySessionBuildsFinalEpochShareFetch`,
    /// over `shareFetchConfigProvider`.
    #[test]
    fn test_close_empty_session_builds_final_epoch_share_fetch() {
        for share_fetch_config in share_fetch_config_provider() {
            let member_id = Uuid::random_uuid();
            let mut handler = ShareSessionHandler::new(1, member_id);

            let mut topic_names = HashMap::new();
            let foo_id = add_topic_id(&mut topic_names, "foo");
            let foo0 = tip(foo_id, 0, "foo");

            // Establish the session with foo0.
            handler.add_partition_to_fetch(foo0, None);
            assert!(handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, false).is_some());
            assert!(!handler.is_session_empty());
            let resp = response(Errors::None, &[("foo", 0, foo_id)]);
            handler.handle_response_with_share_fetch_response(&resp, latest());

            // Remove the only partition by building without adding the partition, emptying it.
            assert!(handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, false).is_some());
            handler.handle_response_with_share_fetch_response(&response(Errors::None, &[]), latest());
            assert!(handler.is_session_empty());

            // Closing the empty session builds a ShareFetch request with the final epoch to close it on the broker.
            handler.notify_close();
            let builder = handler.new_share_fetch_builder(GROUP_ID, &share_fetch_config, true);
            assert!(builder.is_some());
            let request_data = build_data(builder);
            assert_eq!(Some(member_id.to_string()), request_data.member_id);
            assert_eq!(ShareRequestMetadata::FINAL_EPOCH, request_data.share_session_epoch);
            assert!(request_data.topics.is_empty());
            assert!(request_data.forgotten_topics_data.is_empty());
        }
    }

    /// Rust-only (no Java counterpart): the session starts new, a successful
    /// response continues it, and a send error (`handleError`) makes the next
    /// request start a new session again, keeping the member id.
    #[test]
    fn test_handle_error_restarts_the_session() {
        let member_id = Uuid::random_uuid();
        let mut handler = ShareSessionHandler::new(1, member_id);
        assert!(handler.is_new_session());
        let foo_id = Uuid::random_uuid();
        handler.add_partition_to_fetch(tip(foo_id, 0, "foo"), None);
        build_data(handler.new_share_fetch_builder(GROUP_ID, &default_share_fetch_config(), false));
        assert!(handler.handle_response_with_share_fetch_response(&response(Errors::None, &[]), latest()));
        assert!(!handler.is_new_session());

        handler.handle_error(&Error::kafka_message("disconnected"));
        assert!(handler.is_new_session());
        handler.add_partition_to_fetch(tip(foo_id, 0, "foo"), None);
        let request_data = build_data(handler.new_share_fetch_builder(GROUP_ID, &default_share_fetch_config(), false));
        assert_eq!(Some(member_id.to_string()), request_data.member_id);
        assert_eq!(ShareRequestMetadata::INITIAL_EPOCH, request_data.share_session_epoch);
    }

    /// Rust-only (no Java counterpart): the share-fetch handling of a
    /// top-level error other than the three session errors moves to the next
    /// epoch and reports the response as not processable.
    #[test]
    fn test_other_share_fetch_error_moves_to_next_epoch() {
        let mut handler = ShareSessionHandler::new(1, Uuid::random_uuid());
        let foo_id = Uuid::random_uuid();
        handler.add_partition_to_fetch(tip(foo_id, 0, "foo"), None);
        build_data(handler.new_share_fetch_builder(GROUP_ID, &default_share_fetch_config(), false));
        assert!(
            !handler.handle_response_with_share_fetch_response(&response(Errors::UnknownServerError, &[]), latest())
        );
        assert!(!handler.is_new_session());
        handler.add_partition_to_fetch(tip(foo_id, 0, "foo"), None);
        let request_data = build_data(handler.new_share_fetch_builder(GROUP_ID, &default_share_fetch_config(), false));
        assert_eq!(1, request_data.share_session_epoch);
    }

    /// Rust-only (no Java counterpart in `ShareSessionHandlerTest`): the
    /// `ShareAcknowledge` overload of `handleResponse`. `SHARE_SESSION_NOT_FOUND`
    /// and `INVALID_SHARE_SESSION_EPOCH` restart the session;
    /// `SHARE_SESSION_LIMIT_REACHED`, unlike on the `ShareFetch` path, is an
    /// ordinary error that moves to the next epoch; success moves to the next
    /// epoch. A RENEW acknowledgement sets `IsRenewAck`.
    #[test]
    fn test_share_acknowledge_response_handling() {
        for (error, well_formed, new_session) in [
            (Errors::None, true, false),
            (Errors::ShareSessionNotFound, false, true),
            (Errors::InvalidShareSessionEpoch, false, true),
            (Errors::ShareSessionLimitReached, false, false),
        ] {
            let member_id = Uuid::random_uuid();
            let mut handler = ShareSessionHandler::new(1, member_id);
            let foo0 = tip(Uuid::random_uuid(), 0, "foo");
            handler.add_partition_to_fetch(foo0.clone(), None);
            build_data(handler.new_share_fetch_builder(GROUP_ID, &default_share_fetch_config(), false));
            assert!(handler.handle_response_with_share_fetch_response(&response(Errors::None, &[]), latest()));

            let mut acknowledgements = Acknowledgements::empty();
            acknowledgements.add(3, Some(AcknowledgeType::Renew));
            handler.add_partition_to_acknowledge_only(foo0, acknowledgements);
            let builder = handler
                .new_share_acknowledge_builder(GROUP_ID)
                .expect("the session has started");
            assert_eq!(Some(member_id.to_string()), builder.data().member_id);
            assert_eq!(1, builder.data().share_session_epoch);
            assert!(builder.data().is_renew_ack);
            assert_eq!(1, builder.data().topics[0].partitions[0].acknowledgement_batches.len());

            let ack_response = ShareAcknowledgeResponse::of(error, 0, Vec::new(), &[], 0);
            assert_eq!(
                well_formed,
                handler.handle_response_with_share_acknowledge_response(&ack_response, latest()),
                "{error}"
            );
            assert_eq!(new_session, handler.is_new_session(), "{error}");
        }
    }
}
