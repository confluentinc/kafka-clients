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

//! Per-node share session state machine (KIP-932).
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ShareSessionHandler`.
//!
//! Using the protocol outlined by KIP-932, clients can create share sessions.
//! These sessions allow the client to fetch data from a set of share-partitions
//! repeatedly, without explicitly enumerating all the partitions in the request
//! and response.
//!
//! `ShareSessionHandler` tracks the partitions which are in the session. It also
//! determines which partitions need to be included in each
//! `ShareFetch`/`ShareAcknowledge` request.

// Phase 1 (M9) translates the share wire/session layer; the share request
// manager that drives this handler lands in a later phase. Until then, the
// handler is exercised only by tests (same precedent as `fetch_session_handler`).
#![allow(dead_code)]

use indexmap::IndexMap;
use log::{debug, info};

use crate::common::TopicIdPartition;
use crate::common::TopicPartition;
use crate::common::Uuid;
use crate::common::protocol::Errors;
use crate::common::requests::{ShareAcknowledgeRequestBuilder, ShareFetchRequestBuilder, ShareRequestMetadata};
use crate::common::utils::LogContext;
use crate::consumer::AcknowledgeType;
use crate::consumer::internals::acknowledgements::Acknowledgements;
use crate::consumer::internals::share_fetch_config::ShareFetchConfig;
use crate::share_acknowledge_request_data::AcknowledgementBatch as ShareAcknowledgeAckBatch;
use crate::share_fetch_request_data::AcknowledgementBatch as ShareFetchAckBatch;

/// Maintains the share session state for connecting to a broker.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.ShareSessionHandler`.
pub(crate) struct ShareSessionHandler {
    /// The broker node id this handler tracks.
    node: i32,
    /// The consumer's member id.
    member_id: Uuid,
    /// The metadata for the next ShareFetch/ShareAcknowledge request.
    next_metadata: ShareRequestMetadata,
    /// All the partitions in the share session, in insertion order.
    session_partitions: IndexMap<TopicPartition, TopicIdPartition>,
    /// The partitions to be included in the next ShareFetch request.
    next_partitions: IndexMap<TopicPartition, TopicIdPartition>,
    /// The acknowledgements to be included in the next
    /// ShareFetch/ShareAcknowledge request.
    next_acknowledgements: IndexMap<TopicIdPartition, Acknowledgements>,
}

impl ShareSessionHandler {
    /// Constructs a fresh handler for the given broker node id and member id.
    ///
    /// The `LogContext` is accepted for parity with Java but not stored — the
    /// `log` crate is used for logging (same precedent as `AbstractFetch`).
    pub(crate) fn new(_log_context: &LogContext, node: i32, member_id: Uuid) -> Self {
        Self {
            node,
            member_id,
            next_metadata: ShareRequestMetadata::initial_epoch(member_id),
            session_partitions: IndexMap::new(),
            next_partitions: IndexMap::new(),
            next_acknowledgements: IndexMap::new(),
        }
    }

    /// Returns the map of partitions in the share session.
    ///
    /// Corresponds to Java's package-private `sessionPartitionMap()`.
    pub(crate) fn session_partition_map(&self) -> &IndexMap<TopicPartition, TopicIdPartition> {
        &self.session_partitions
    }

    /// Returns the partitions in the share session.
    ///
    /// Corresponds to Java's `sessionPartitions()`.
    pub(crate) fn session_partitions(&self) -> Vec<TopicIdPartition> {
        self.session_partitions.values().cloned().collect()
    }

    /// Adds a partition to be fetched in the next request, optionally carrying
    /// acknowledgements.
    ///
    /// Corresponds to Java's `addPartitionToFetch(TopicIdPartition, Acknowledgements)`.
    pub(crate) fn add_partition_to_fetch(
        &mut self,
        topic_id_partition: TopicIdPartition,
        partition_acknowledgements: Option<Acknowledgements>,
    ) {
        self.next_partitions
            .insert(topic_id_partition.topic_partition().clone(), topic_id_partition.clone());
        if let Some(acks) = partition_acknowledgements {
            self.next_acknowledgements.insert(topic_id_partition, acks);
        }
    }

    /// Adds a partition to be acknowledged only (not fetched) in the next
    /// request.
    ///
    /// Corresponds to Java's `addPartitionToAcknowledgeOnly(TopicIdPartition, Acknowledgements)`.
    pub(crate) fn add_partition_to_acknowledge_only(
        &mut self,
        topic_id_partition: TopicIdPartition,
        partition_acknowledgements: Acknowledgements,
    ) {
        self.next_acknowledgements
            .insert(topic_id_partition, partition_acknowledgements);
    }

    /// Whether the next request would create a new share session.
    ///
    /// Corresponds to Java's `isNewSession()`.
    pub(crate) fn is_new_session(&self) -> bool {
        self.next_metadata.is_new_session()
    }

    /// Builds the next `ShareFetch` request builder, updating the session state.
    /// Returns `None` when the request can be skipped (mirrors Java returning
    /// `null`).
    ///
    /// Corresponds to Java's `newShareFetchBuilder(String, ShareFetchConfig, boolean)`.
    pub(crate) fn new_share_fetch_builder(
        &mut self,
        group_id: &str,
        share_fetch_config: &ShareFetchConfig,
        can_skip_if_request_empty: bool,
    ) -> Option<ShareFetchRequestBuilder> {
        let mut added: Vec<TopicIdPartition> = Vec::new();
        let mut removed: Vec<TopicIdPartition> = Vec::new();
        let mut replaced: Vec<TopicIdPartition> = Vec::new();

        if self.next_metadata.is_new_session() {
            // Add any new partitions to the session.
            for (topic_partition, topic_id_partition) in &self.next_partitions {
                self.session_partitions
                    .insert(topic_partition.clone(), topic_id_partition.clone());
            }

            // If it's a new session, all the partitions must be added to the request.
            added.extend(self.session_partitions.values().cloned());
        } else {
            // Iterate over the session partitions, tallying which were added.
            let session_keys: Vec<TopicPartition> = self.session_partitions.keys().cloned().collect();
            for topic_partition in &session_keys {
                let prev_data = self.session_partitions.get(topic_partition).expect("key present").clone();
                match self.next_partitions.shift_remove(topic_partition) {
                    Some(next_data) => {
                        // If the topic ID does not match, the topic has been recreated.
                        if prev_data != next_data {
                            self.next_partitions.insert(topic_partition.clone(), next_data.clone());
                            self.session_partitions.insert(topic_partition.clone(), next_data);
                            replaced.push(prev_data);
                        }
                    },
                    None => {
                        // This partition is not in the builder, so we need to remove it from the session.
                        self.session_partitions.shift_remove(topic_partition);
                        removed.push(prev_data);
                    },
                }
            }

            // Add any new partitions to the session.
            for (topic_partition, topic_id_partition) in &self.next_partitions {
                self.session_partitions
                    .insert(topic_partition.clone(), topic_id_partition.clone());
                added.push(topic_id_partition.clone());
            }
        }

        // The replaced topic-partitions need to be removed, and their replacements are already added.
        removed.extend(replaced.iter().cloned());

        let mut has_renew_acknowledgements = false;
        // Insertion-ordered map of acknowledgement batches keyed by partition.
        // Java uses a `HashMap` here (unordered); an `IndexMap` keeps the
        // translation deterministic without changing the wire result.
        let mut acknowledgement_batches: IndexMap<TopicIdPartition, Vec<ShareFetchAckBatch>> = IndexMap::new();
        if !self.next_acknowledgements.is_empty() {
            for (topic_id_partition, acks) in &self.next_acknowledgements {
                for ack_batch in acks.get_acknowledgement_batches() {
                    if ack_batch.acknowledge_types().contains(&AcknowledgeType::Renew.id()) {
                        has_renew_acknowledgements = true;
                    }
                    acknowledgement_batches
                        .entry(topic_id_partition.clone())
                        .or_default()
                        .push(ack_batch.to_share_fetch_request());
                }
            }
        }

        self.next_partitions = IndexMap::new();
        self.next_acknowledgements = IndexMap::new();

        if can_skip_if_request_empty && added.is_empty() && removed.is_empty() && acknowledgement_batches.is_empty() {
            return None;
        }

        debug!(
            "Build ShareFetch {} for node {}. Added {}, removed {}, replaced {} out of {}",
            self.next_metadata,
            self.node,
            added.len(),
            removed.len(),
            replaced.len(),
            self.session_partitions.len()
        );

        let ack_pairs: Vec<(TopicIdPartition, Vec<ShareFetchAckBatch>)> = acknowledgement_batches.into_iter().collect();

        let builder = if has_renew_acknowledgements {
            // If the request has renew acknowledgements, the ShareFetch is only used to send the
            // acknowledgements and potentially update the share session. The parameters for wait time,
            // number of bytes and number of records are all zero.
            ShareFetchRequestBuilder::for_consumer(
                group_id,
                Some(&self.next_metadata),
                0,
                0,
                0,
                0,
                0,
                share_fetch_config.share_acquire_mode.id(),
                true,
                &added,
                &removed,
                &ack_pairs,
            )
        } else if can_skip_if_request_empty {
            // The request contains changes to the share session or acknowledgements only. The
            // parameters for wait time, number of bytes and number of records are all zero.
            ShareFetchRequestBuilder::for_consumer(
                group_id,
                Some(&self.next_metadata),
                0,
                0,
                0,
                0,
                0,
                share_fetch_config.share_acquire_mode.id(),
                false,
                &added,
                &removed,
                &ack_pairs,
            )
        } else {
            ShareFetchRequestBuilder::for_consumer(
                group_id,
                Some(&self.next_metadata),
                share_fetch_config.max_wait_ms,
                share_fetch_config.min_bytes,
                share_fetch_config.max_bytes,
                share_fetch_config.max_poll_records,
                share_fetch_config.max_poll_records,
                share_fetch_config.share_acquire_mode.id(),
                false,
                &added,
                &removed,
                &ack_pairs,
            )
        };
        Some(builder)
    }

    /// Builds the next `ShareAcknowledge` request builder, updating the session
    /// state. Returns `None` when a share session cannot be started with a
    /// `ShareAcknowledge` request (mirrors Java returning `null`).
    ///
    /// Corresponds to Java's `newShareAcknowledgeBuilder(String, ShareFetchConfig)`.
    pub(crate) fn new_share_acknowledge_builder(
        &mut self,
        group_id: &str,
        _share_fetch_config: &ShareFetchConfig,
    ) -> Option<ShareAcknowledgeRequestBuilder> {
        if self.next_metadata.is_new_session() {
            // A share session cannot be started with a ShareAcknowledge request.
            self.next_partitions.clear();
            self.next_acknowledgements.clear();
            return None;
        }

        let mut has_renew_acknowledgements = false;
        let mut acknowledgement_batches: IndexMap<TopicIdPartition, Vec<ShareAcknowledgeAckBatch>> = IndexMap::new();
        if !self.next_acknowledgements.is_empty() {
            for (topic_id_partition, acks) in &self.next_acknowledgements {
                for ack_batch in acks.get_acknowledgement_batches() {
                    if ack_batch.acknowledge_types().contains(&AcknowledgeType::Renew.id()) {
                        has_renew_acknowledgements = true;
                    }
                    acknowledgement_batches
                        .entry(topic_id_partition.clone())
                        .or_default()
                        .push(ack_batch.to_share_acknowledge_request());
                }
            }
        }

        self.next_acknowledgements = IndexMap::new();

        let ack_pairs: Vec<(TopicIdPartition, Vec<ShareAcknowledgeAckBatch>)> =
            acknowledgement_batches.into_iter().collect();
        Some(ShareAcknowledgeRequestBuilder::for_consumer(
            group_id,
            Some(&self.next_metadata),
            has_renew_acknowledgements,
            &ack_pairs,
        ))
    }

    /// Handles the `ShareFetch` response, advancing the session epoch.
    ///
    /// Returns `true` if the response is well-formed; `false` if it can't be
    /// processed because of missing or unexpected partitions.
    ///
    /// Corresponds to Java's `handleResponse(ShareFetchResponse, short)`.
    pub(crate) fn handle_fetch_response(
        &mut self,
        response: &crate::common::requests::ShareFetchResponse,
        _version: i16,
    ) -> bool {
        let error = response.error();
        if error == Errors::ShareSessionNotFound
            || error == Errors::InvalidShareSessionEpoch
            || error == Errors::ShareSessionLimitReached
        {
            info!(
                "Node {} was unable to process the ShareFetch request with {}: {:?}.",
                self.node, self.next_metadata, error
            );
            self.next_metadata = self.next_metadata.next_close_existing_attempt_new();
            return false;
        }

        if error != Errors::None {
            info!(
                "Node {} was unable to process the ShareFetch request with {}: {:?}.",
                self.node, self.next_metadata, error
            );
            self.next_metadata = self.next_metadata.next_epoch();
            return false;
        }

        // The share session was continued by the server.
        debug!(
            "Node {} sent a ShareFetch response with throttleTimeMs = {} for session {}",
            self.node,
            response.throttle_time_ms(),
            self.member_id
        );
        self.next_metadata = self.next_metadata.next_epoch();
        true
    }

    /// Handles the `ShareAcknowledge` response, advancing the session epoch.
    ///
    /// Returns `true` if the response is well-formed; `false` if it can't be
    /// processed because of missing or unexpected partitions.
    ///
    /// Corresponds to Java's `handleResponse(ShareAcknowledgeResponse, short)`.
    pub(crate) fn handle_acknowledge_response(
        &mut self,
        response: &crate::common::requests::ShareAcknowledgeResponse,
        _version: i16,
    ) -> bool {
        let error = response.error();
        if error == Errors::ShareSessionNotFound || error == Errors::InvalidShareSessionEpoch {
            info!(
                "Node {} was unable to process the ShareAcknowledge request with {}: {:?}.",
                self.node, self.next_metadata, error
            );
            self.next_metadata = self.next_metadata.next_close_existing_attempt_new();
            return false;
        }

        if error != Errors::None {
            info!(
                "Node {} was unable to process the ShareAcknowledge request with {}: {:?}.",
                self.node, self.next_metadata, error
            );
            self.next_metadata = self.next_metadata.next_epoch();
            return false;
        }

        // The share session was continued by the server.
        debug!(
            "Node {} sent a ShareAcknowledge response with throttleTimeMs = {} for session {}",
            self.node,
            response.throttle_time_ms(),
            self.member_id
        );
        self.next_metadata = self.next_metadata.next_epoch();
        true
    }

    /// The client will initiate the session close on the next ShareFetch
    /// request.
    ///
    /// Corresponds to Java's `notifyClose()`.
    pub(crate) fn notify_close(&mut self) {
        debug!(
            "Set the metadata for next ShareFetch request to close the share session memberId={}",
            self.next_metadata.member_id()
        );
        self.next_metadata = self.next_metadata.final_epoch();
    }

    /// Handles an error sending the prepared request. When a network error
    /// occurs, we close any existing share session on our next request, and try
    /// to create a new session.
    ///
    /// Corresponds to Java's `handleError(Throwable)`.
    pub(crate) fn handle_error(&mut self, error: &crate::common::KafkaError) {
        info!(
            "Error sending fetch request {} to node {}: {}",
            self.next_metadata, self.node, error
        );
        self.next_metadata = self.next_metadata.next_close_existing_attempt_new();
    }
}

/// Unit tests translated from Java's `ShareSessionHandlerTest`.
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::common::IsolationLevel;
    use crate::common::protocol::ApiKeys;
    use crate::common::requests::share_request_metadata::INITIAL_EPOCH;
    use crate::common::requests::{ConcreteRequest, RequestBuilder, ShareFetchResponse};
    use crate::consumer::consumer_config::ConsumerConfig;
    use crate::consumer::internals::share_acquire_mode::ShareAcquireMode;
    use crate::share_fetch_request_data::ShareFetchRequestData;
    use crate::share_fetch_response_data::PartitionData;

    const NODE: i32 = 1;

    fn log_context() -> LogContext {
        LogContext::new("[ShareSessionHandler]=")
    }

    // Mirrors DEFAULT_SHARE_FETCH_CONFIG. Java uses ConsumerConfig.DEFAULT_CLIENT_RACK
    // which is the empty string.
    fn default_share_fetch_config() -> ShareFetchConfig {
        ShareFetchConfig::new(
            ConsumerConfig::DEFAULT_FETCH_MIN_BYTES,
            ConsumerConfig::DEFAULT_FETCH_MAX_BYTES,
            ConsumerConfig::DEFAULT_FETCH_MAX_WAIT_MS,
            ConsumerConfig::DEFAULT_MAX_PARTITION_FETCH_BYTES,
            ConsumerConfig::DEFAULT_MAX_POLL_RECORDS,
            true,
            "",
            IsolationLevel::ReadUncommitted,
            ShareAcquireMode::BatchOptimized,
        )
    }

    // Mirrors SHARE_FETCH_CONFIG_RECORD_LIMIT.
    fn record_limit_share_fetch_config() -> ShareFetchConfig {
        ShareFetchConfig::new(
            ConsumerConfig::DEFAULT_FETCH_MIN_BYTES,
            ConsumerConfig::DEFAULT_FETCH_MAX_BYTES,
            ConsumerConfig::DEFAULT_FETCH_MAX_WAIT_MS,
            ConsumerConfig::DEFAULT_MAX_PARTITION_FETCH_BYTES,
            ConsumerConfig::DEFAULT_MAX_POLL_RECORDS,
            true,
            "",
            IsolationLevel::ReadUncommitted,
            ShareAcquireMode::RecordLimit,
        )
    }

    // The two configs used by the @MethodSource("shareFetchConfigProvider") tests.
    fn share_fetch_config_provider() -> Vec<ShareFetchConfig> {
        vec![default_share_fetch_config(), record_limit_share_fetch_config()]
    }

    fn add_topic_id(topic_names: &mut HashMap<Uuid, String>, name: &str) -> Uuid {
        let id = Uuid::random_uuid();
        topic_names.insert(id, name.to_string());
        id
    }

    /// Builds a [`ShareFetchRequestData`] from a (non-null) builder, mirroring
    /// Java's `handler.newShareFetchBuilder(...).build().data()`.
    fn build_data(builder: Option<ShareFetchRequestBuilder>) -> ShareFetchRequestData {
        let mut b = builder.expect("expected a non-null ShareFetch builder");
        match b.build().expect("build ShareFetch request") {
            ConcreteRequest::ShareFetch(r) => r.data().clone(),
            other => panic!("expected ShareFetch, got {other:?}"),
        }
    }

    fn req_fetch_list(
        request_data: &ShareFetchRequestData,
        topic_names: &HashMap<Uuid, String>,
    ) -> Vec<TopicIdPartition> {
        let mut tips = Vec::new();
        for topic in &request_data.topics {
            for partition in &topic.partitions {
                tips.push(TopicIdPartition::from_parts(
                    topic.topic_id,
                    partition.partition_index,
                    topic_names.get(&topic.topic_id).cloned().unwrap_or_default(),
                ));
            }
        }
        tips
    }

    fn req_forget_list(
        request_data: &ShareFetchRequestData,
        topic_names: &HashMap<Uuid, String>,
    ) -> Vec<TopicIdPartition> {
        let mut tips = Vec::new();
        for topic in &request_data.forgotten_topics_data {
            for &partition in &topic.partitions {
                tips.push(TopicIdPartition::from_parts(
                    topic.topic_id,
                    partition,
                    topic_names.get(&topic.topic_id).cloned().unwrap_or_default(),
                ));
            }
        }
        tips
    }

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

    /// Mirrors Java's `assertMapEquals`, comparing keys and values in iteration
    /// order. `expected` is the ordered list of topic-id-partitions (the Java
    /// `reqMap` helper), each keyed by its topic-partition.
    fn assert_map_equals(expected: &[TopicIdPartition], actual: &IndexMap<TopicPartition, TopicIdPartition>) {
        assert_eq!(expected.len(), actual.len(), "map size differs");
        for (i, (expected_tip, (actual_tp, actual_tip))) in expected.iter().zip(actual.iter()).enumerate() {
            assert_eq!(
                expected_tip.topic_partition(),
                actual_tp,
                "Element {} had a different TopicPartition than expected.",
                i + 1
            );
            assert_eq!(
                expected_tip,
                actual_tip,
                "Element {} had different PartitionData than expected.",
                i + 1
            );
        }
    }

    /// Builds an ordered [`ShareFetchResponse`] from `(topic, partition, topicId)`
    /// entries, mirroring Java's `buildResponseData` + `ShareFetchResponse.of`.
    fn response_of(error: Errors, entries: &[(&str, i32, Uuid)]) -> ShareFetchResponse {
        let response_data: Vec<(TopicIdPartition, PartitionData)> = entries
            .iter()
            .map(|(topic, partition, topic_id)| {
                let mut pd = PartitionData::new();
                pd.set_partition_index(*partition);
                (TopicIdPartition::from_parts(*topic_id, *partition, *topic), pd)
            })
            .collect();
        ShareFetchResponse::of(error, 0, response_data, &[], 0)
    }

    /// `@ParameterizedTest @EnumSource(INVALID_SHARE_SESSION_EPOCH,
    /// SHARE_SESSION_NOT_FOUND, SHARE_SESSION_LIMIT_REACHED)`.
    #[test]
    fn test_share_session() {
        for error in [
            Errors::InvalidShareSessionEpoch,
            Errors::ShareSessionNotFound,
            Errors::ShareSessionLimitReached,
        ] {
            let group_id = "G1";
            let member_id = Uuid::random_uuid();
            let mut handler = ShareSessionHandler::new(&log_context(), NODE, member_id);
            let version = ApiKeys::SHARE_FETCH.latest_version();

            let mut topic_names = HashMap::new();
            let foo_id = add_topic_id(&mut topic_names, "foo");
            let foo0 = TopicIdPartition::from_parts(foo_id, 0, "foo");
            let foo1 = TopicIdPartition::from_parts(foo_id, 1, "foo");
            handler.add_partition_to_fetch(foo0.clone(), None);
            handler.add_partition_to_fetch(foo1.clone(), None);
            let request_data1 =
                build_data(handler.new_share_fetch_builder(group_id, &default_share_fetch_config(), false));
            let expected_to_send1 = vec![
                TopicIdPartition::from_parts(foo_id, 0, "foo"),
                TopicIdPartition::from_parts(foo_id, 1, "foo"),
            ];
            assert_list_equals(&expected_to_send1, &req_fetch_list(&request_data1, &topic_names));
            assert_eq!(request_data1.member_id.as_deref(), Some(member_id.to_string().as_str()));

            let resp = response_of(Errors::None, &[("foo", 0, foo_id), ("foo", 1, foo_id)]);
            handler.handle_fetch_response(&resp, version);

            // Test a fetch request which adds one partition.
            let bar_id = add_topic_id(&mut topic_names, "bar");
            let bar0 = TopicIdPartition::from_parts(bar_id, 0, "bar");
            handler.add_partition_to_fetch(foo0.clone(), None);
            handler.add_partition_to_fetch(foo1.clone(), None);
            handler.add_partition_to_fetch(bar0.clone(), None);
            let request_data2 =
                build_data(handler.new_share_fetch_builder(group_id, &default_share_fetch_config(), false));
            assert_map_equals(
                &[
                    TopicIdPartition::from_parts(foo_id, 0, "foo"),
                    TopicIdPartition::from_parts(foo_id, 1, "foo"),
                    TopicIdPartition::from_parts(bar_id, 0, "bar"),
                ],
                handler.session_partition_map(),
            );
            let expected_to_send2 = vec![TopicIdPartition::from_parts(bar_id, 0, "bar")];
            assert_list_equals(&expected_to_send2, &req_fetch_list(&request_data2, &topic_names));

            let resp2 = response_of(Errors::None, &[("foo", 1, foo_id)]);
            handler.handle_fetch_response(&resp2, version);

            // A top-level error code will reset the session epoch.
            let resp3 = response_of(error, &[]);
            handler.handle_fetch_response(&resp3, version);

            let request_data4 =
                build_data(handler.new_share_fetch_builder(group_id, &default_share_fetch_config(), false));
            assert_eq!(request_data4.member_id, request_data2.member_id);
            assert_eq!(request_data4.share_session_epoch, INITIAL_EPOCH);
            assert_map_equals(
                &[
                    TopicIdPartition::from_parts(foo_id, 0, "foo"),
                    TopicIdPartition::from_parts(foo_id, 1, "foo"),
                    TopicIdPartition::from_parts(bar_id, 0, "bar"),
                ],
                handler.session_partition_map(),
            );
            let expected_to_send4 = vec![
                TopicIdPartition::from_parts(foo_id, 0, "foo"),
                TopicIdPartition::from_parts(foo_id, 1, "foo"),
                TopicIdPartition::from_parts(bar_id, 0, "bar"),
            ];
            assert_list_equals(&expected_to_send4, &req_fetch_list(&request_data4, &topic_names));
        }
    }

    /// `@ParameterizedTest @MethodSource("shareFetchConfigProvider")`.
    #[test]
    fn test_partition_removal() {
        for share_fetch_config in share_fetch_config_provider() {
            let group_id = "G1";
            let member_id = Uuid::random_uuid();
            let mut handler = ShareSessionHandler::new(&log_context(), NODE, member_id);
            let version = ApiKeys::SHARE_FETCH.latest_version();

            let mut topic_names = HashMap::new();
            let foo_id = add_topic_id(&mut topic_names, "foo");
            let bar_id = add_topic_id(&mut topic_names, "bar");
            let foo0 = TopicIdPartition::from_parts(foo_id, 0, "foo");
            let foo1 = TopicIdPartition::from_parts(foo_id, 1, "foo");
            let bar0 = TopicIdPartition::from_parts(bar_id, 0, "bar");
            handler.add_partition_to_fetch(foo0, None);
            handler.add_partition_to_fetch(foo1.clone(), None);
            handler.add_partition_to_fetch(bar0, None);
            let request_data1 = build_data(handler.new_share_fetch_builder(group_id, &share_fetch_config, false));
            assert_map_equals(
                &[
                    TopicIdPartition::from_parts(foo_id, 0, "foo"),
                    TopicIdPartition::from_parts(foo_id, 1, "foo"),
                    TopicIdPartition::from_parts(bar_id, 0, "bar"),
                ],
                handler.session_partition_map(),
            );
            let expected_to_send1 = vec![
                TopicIdPartition::from_parts(foo_id, 0, "foo"),
                TopicIdPartition::from_parts(foo_id, 1, "foo"),
                TopicIdPartition::from_parts(bar_id, 0, "bar"),
            ];
            assert_list_equals(&expected_to_send1, &req_fetch_list(&request_data1, &topic_names));
            assert_eq!(request_data1.member_id.as_deref(), Some(member_id.to_string().as_str()));

            let resp = response_of(Errors::None, &[("foo", 0, foo_id), ("foo", 1, foo_id), ("bar", 0, bar_id)]);
            handler.handle_fetch_response(&resp, version);

            // Test a fetch request which removes two partitions.
            handler.add_partition_to_fetch(foo1.clone(), None);
            let request_data2 = build_data(handler.new_share_fetch_builder(group_id, &share_fetch_config, false));
            assert_eq!(request_data2.member_id.as_deref(), Some(member_id.to_string().as_str()));
            assert_eq!(request_data2.share_session_epoch, 1);
            assert_map_equals(
                &[TopicIdPartition::from_parts(foo_id, 1, "foo")],
                handler.session_partition_map(),
            );
            assert!(request_data2.topics.is_empty());
            let expected_to_forget2 = vec![
                TopicIdPartition::from_parts(foo_id, 0, "foo"),
                TopicIdPartition::from_parts(bar_id, 0, "bar"),
            ];
            assert_list_equals(&expected_to_forget2, &req_forget_list(&request_data2, &topic_names));

            // A top-level error code will reset the session epoch.
            let resp2 = response_of(Errors::InvalidShareSessionEpoch, &[]);
            handler.handle_fetch_response(&resp2, version);

            handler.add_partition_to_fetch(foo1, None);
            let request_data3 = build_data(handler.new_share_fetch_builder(group_id, &share_fetch_config, false));
            assert_eq!(request_data3.member_id.as_deref(), Some(member_id.to_string().as_str()));
            assert_eq!(request_data3.share_session_epoch, INITIAL_EPOCH);
            assert_map_equals(
                &[TopicIdPartition::from_parts(foo_id, 1, "foo")],
                handler.session_partition_map(),
            );
            let expected_to_send3 = vec![TopicIdPartition::from_parts(foo_id, 1, "foo")];
            assert_list_equals(&expected_to_send3, &req_fetch_list(&request_data3, &topic_names));
        }
    }

    /// `@ParameterizedTest @MethodSource("shareFetchConfigProvider")`.
    #[test]
    fn test_topic_id_replaced() {
        for share_fetch_config in share_fetch_config_provider() {
            let group_id = "G1";
            let member_id = Uuid::random_uuid();
            let mut handler = ShareSessionHandler::new(&log_context(), NODE, member_id);
            let version = ApiKeys::SHARE_FETCH.latest_version();

            let mut topic_names = HashMap::new();
            let topic_id1 = add_topic_id(&mut topic_names, "foo");
            let tp = TopicIdPartition::from_parts(topic_id1, 0, "foo");
            handler.add_partition_to_fetch(tp.clone(), None);
            let request_data1 = build_data(handler.new_share_fetch_builder(group_id, &share_fetch_config, false));
            assert_map_equals(
                &[TopicIdPartition::from_parts(topic_id1, 0, "foo")],
                handler.session_partition_map(),
            );
            let expected_to_send1 = vec![TopicIdPartition::from_parts(topic_id1, 0, "foo")];
            assert_list_equals(&expected_to_send1, &req_fetch_list(&request_data1, &topic_names));

            let resp = response_of(Errors::None, &[("foo", 0, topic_id1)]);
            handler.handle_fetch_response(&resp, version);

            // Try to add a new topic ID.
            let topic_id2 = add_topic_id(&mut topic_names, "foo");
            let tp2 = TopicIdPartition::from_parts(topic_id2, 0, "foo");
            // Use the same data besides the topic ID.
            handler.add_partition_to_fetch(tp2.clone(), None);
            let request_data2 = build_data(handler.new_share_fetch_builder(group_id, &share_fetch_config, false));

            // If we started with an ID, only a new ID will count towards replaced.
            // The old topic ID partition should be forgotten, and the new one fetched.
            assert_eq!(vec![tp.clone()], req_forget_list(&request_data2, &topic_names));
            assert_map_equals(
                &[TopicIdPartition::from_parts(topic_id2, 0, "foo")],
                handler.session_partition_map(),
            );
            assert_list_equals(&[tp2], &req_fetch_list(&request_data2, &topic_names));

            // Should have the same session ID and next epoch.
            assert_eq!(
                request_data2.member_id.as_deref(),
                Some(member_id.to_string().as_str()),
                "Did not use same session"
            );
            assert_eq!(request_data2.share_session_epoch, 1, "Did not have correct epoch");
        }
    }

    /// `@ParameterizedTest @MethodSource("shareFetchConfigProvider")`.
    #[test]
    fn test_partition_forgotten_on_acknowledge_only() {
        for share_fetch_config in share_fetch_config_provider() {
            let group_id = "G1";
            let member_id = Uuid::random_uuid();
            let mut handler = ShareSessionHandler::new(&log_context(), NODE, member_id);
            let version = ApiKeys::SHARE_FETCH.latest_version();

            let mut topic_names = HashMap::new();
            let topic_id = add_topic_id(&mut topic_names, "foo");
            let foo0 = TopicIdPartition::from_parts(topic_id, 0, "foo");
            handler.add_partition_to_fetch(foo0.clone(), None);
            let request_data1 = build_data(handler.new_share_fetch_builder(group_id, &share_fetch_config, false));
            assert_map_equals(std::slice::from_ref(&foo0), handler.session_partition_map());
            let expected_to_send1 = vec![TopicIdPartition::from_parts(topic_id, 0, "foo")];
            assert_list_equals(&expected_to_send1, &req_fetch_list(&request_data1, &topic_names));

            let resp = response_of(Errors::None, &[("foo", 0, topic_id)]);
            handler.handle_fetch_response(&resp, version);

            // Remove the topic from the session by setting acknowledgements only.
            let request_data2 = build_data(handler.new_share_fetch_builder(group_id, &share_fetch_config, false));
            handler.add_partition_to_acknowledge_only(foo0.clone(), Acknowledgements::empty());
            assert_eq!(vec![foo0], req_forget_list(&request_data2, &topic_names));

            assert_eq!(
                request_data2.member_id.as_deref(),
                Some(member_id.to_string().as_str()),
                "Did not use same session"
            );
            assert_eq!(request_data2.share_session_epoch, 1, "Did not have correct epoch");
        }
    }

    /// `@ParameterizedTest @MethodSource("shareFetchConfigProvider")`.
    #[test]
    fn test_forgotten_partitions() {
        for share_fetch_config in share_fetch_config_provider() {
            let group_id = "G1";
            let member_id = Uuid::random_uuid();
            let mut handler = ShareSessionHandler::new(&log_context(), NODE, member_id);
            let version = ApiKeys::SHARE_FETCH.latest_version();

            let mut topic_names = HashMap::new();
            let topic_id = add_topic_id(&mut topic_names, "foo");
            let foo0 = TopicIdPartition::from_parts(topic_id, 0, "foo");
            handler.add_partition_to_fetch(foo0.clone(), None);
            let request_data1 = build_data(handler.new_share_fetch_builder(group_id, &share_fetch_config, false));
            assert_map_equals(std::slice::from_ref(&foo0), handler.session_partition_map());
            let expected_to_send1 = vec![TopicIdPartition::from_parts(topic_id, 0, "foo")];
            assert_list_equals(&expected_to_send1, &req_fetch_list(&request_data1, &topic_names));

            let resp = response_of(Errors::None, &[("foo", 0, topic_id)]);
            handler.handle_fetch_response(&resp, version);

            // Remove the topic from the session.
            let request_data2 = build_data(handler.new_share_fetch_builder(group_id, &share_fetch_config, false));
            assert_eq!(vec![foo0], req_forget_list(&request_data2, &topic_names));

            assert_eq!(
                request_data2.member_id.as_deref(),
                Some(member_id.to_string().as_str()),
                "Did not use same session"
            );
            assert_eq!(request_data2.share_session_epoch, 1, "Did not have correct epoch");
        }
    }

    /// `@ParameterizedTest @MethodSource("shareFetchConfigProvider")`.
    #[test]
    fn test_add_new_id_after_topic_removed_from_session() {
        for share_fetch_config in share_fetch_config_provider() {
            let group_id = "G1";
            let member_id = Uuid::random_uuid();
            let mut handler = ShareSessionHandler::new(&log_context(), NODE, member_id);
            let version = ApiKeys::SHARE_FETCH.latest_version();

            let mut topic_names = HashMap::new();
            let topic_id = add_topic_id(&mut topic_names, "foo");
            handler.add_partition_to_fetch(TopicIdPartition::from_parts(topic_id, 0, "foo"), None);
            let request_data1 = build_data(handler.new_share_fetch_builder(group_id, &share_fetch_config, false));
            assert_map_equals(
                &[TopicIdPartition::from_parts(topic_id, 0, "foo")],
                handler.session_partition_map(),
            );
            let expected_to_send1 = vec![TopicIdPartition::from_parts(topic_id, 0, "foo")];
            assert_list_equals(&expected_to_send1, &req_fetch_list(&request_data1, &topic_names));

            let resp = response_of(Errors::None, &[("foo", 0, topic_id)]);
            handler.handle_fetch_response(&resp, version);

            // Remove the partition from the session.
            let request_data2 = build_data(handler.new_share_fetch_builder(group_id, &share_fetch_config, false));
            assert!(handler.session_partition_map().is_empty());
            assert!(request_data2.topics.is_empty());
            let resp2 = response_of(Errors::None, &[]);
            handler.handle_fetch_response(&resp2, version);

            // After the topic is removed, add a recreated topic with a new ID.
            let topic_id2 = add_topic_id(&mut topic_names, "foo");
            handler.add_partition_to_fetch(TopicIdPartition::from_parts(topic_id2, 0, "foo"), None);
            let request_data3 = build_data(handler.new_share_fetch_builder(group_id, &share_fetch_config, false));

            // Should have the same session ID and epoch 2.
            assert_eq!(
                request_data3.member_id.as_deref(),
                Some(member_id.to_string().as_str()),
                "Did not use same session"
            );
            assert_eq!(request_data3.share_session_epoch, 2, "Did not have the correct session epoch");
        }
    }

    /// `@ParameterizedTest @MethodSource("shareFetchConfigProvider")`.
    #[test]
    fn test_next_acknowledgements_cleared_on_invalid_request() {
        for share_fetch_config in share_fetch_config_provider() {
            let group_id = "G1";
            let member_id = Uuid::random_uuid();
            let mut handler = ShareSessionHandler::new(&log_context(), NODE, member_id);

            let mut topic_names = HashMap::new();
            let foo_id = add_topic_id(&mut topic_names, "foo");
            let foo0 = TopicIdPartition::from_parts(foo_id, 0, "foo");

            let mut acknowledgements = Acknowledgements::empty();
            acknowledgements.add(0, AcknowledgeType::Accept);

            handler.add_partition_to_fetch(foo0, Some(acknowledgements));

            // Starting with a ShareAcknowledge on epoch 0 yields a null response.
            assert!(handler.new_share_acknowledge_builder(group_id, &share_fetch_config).is_none());

            // Attempt a new ShareFetch.
            let foo1 = TopicIdPartition::from_parts(foo_id, 1, "foo");
            handler.add_partition_to_fetch(foo1, None);
            let request_data = build_data(handler.new_share_fetch_builder(group_id, &share_fetch_config, false));

            // We should have cleared the unsent acknowledgements before this ShareFetch.
            assert_eq!(request_data.topics[0].partitions[0].acknowledgement_batches.len(), 0);

            let expected_to_send1 = vec![TopicIdPartition::from_parts(foo_id, 1, "foo")];
            assert_list_equals(&expected_to_send1, &req_fetch_list(&request_data, &topic_names));
            assert_eq!(request_data.member_id.as_deref(), Some(member_id.to_string().as_str()));
        }
    }

    /// `@Test testCanSkipIfRequestEmpty`.
    #[test]
    fn test_can_skip_if_request_empty() {
        let share_fetch_config = record_limit_share_fetch_config();

        let group_id = "G1";
        let member_id = Uuid::random_uuid();
        let mut handler = ShareSessionHandler::new(&log_context(), NODE, member_id);
        let version = ApiKeys::SHARE_FETCH.latest_version();

        let mut topic_names = HashMap::new();
        let foo_id = add_topic_id(&mut topic_names, "foo");
        let foo0 = TopicIdPartition::from_parts(foo_id, 0, "foo");

        let mut acknowledgements = Acknowledgements::empty();
        acknowledgements.add(0, AcknowledgeType::Accept);

        // The request cannot be skipped when a topic-partition is added to the session.
        handler.add_partition_to_fetch(foo0.clone(), None);
        assert!(handler.new_share_fetch_builder(group_id, &share_fetch_config, true).is_some());

        let resp = response_of(Errors::None, &[("foo", 0, foo_id)]);
        handler.handle_fetch_response(&resp, version);

        // The request can be skipped when the same topic-partition is already in the session.
        handler.add_partition_to_fetch(foo0.clone(), None);
        assert!(handler.new_share_fetch_builder(group_id, &share_fetch_config, true).is_none());

        // The request cannot be skipped when there are acknowledgements.
        handler.add_partition_to_fetch(foo0, Some(acknowledgements));
        assert!(handler.new_share_fetch_builder(group_id, &share_fetch_config, true).is_some());
        handler.handle_fetch_response(&resp, version);

        // The request cannot be skipped when the topic-partition is removed from the session.
        assert!(handler.new_share_fetch_builder(group_id, &share_fetch_config, true).is_some());
        handler.handle_fetch_response(&response_of(Errors::None, &[]), version);

        // The request can be skipped when the share session is empty.
        assert!(handler.new_share_fetch_builder(group_id, &share_fetch_config, true).is_none());
    }
}
