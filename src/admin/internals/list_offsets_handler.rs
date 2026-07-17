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

//! Handler for the `listOffsets` API (partition-leader targeted).
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.ListOffsetsHandler`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::admin::list_offsets_result::ListOffsetsResultInfo;
use crate::admin::options::ListOffsetsOptions;
use crate::common::protocol::Errors;
use crate::common::requests::list_offsets_request::{
    EARLIEST_LOCAL_TIMESTAMP, EARLIEST_PENDING_UPLOAD_TIMESTAMP, LATEST_TIERED_TIMESTAMP, MAX_TIMESTAMP,
};
use crate::common::requests::list_offsets_response::UNKNOWN_EPOCH;
use crate::common::requests::{ConcreteResponse, ListOffsetsRequestBuilder, RequestBuilder};
use crate::common::utils::LogContext;
use crate::common::{KafkaError, Node, TopicPartition};
use crate::kafka_debug;
use crate::list_offsets_request_data::{ListOffsetsPartition, ListOffsetsTopic};

use super::admin_api_handler::{AdminApiHandler, ApiResult, RequestAndKeys};
use super::admin_api_lookup_strategy::AdminApiLookupStrategy;
use super::partition_leader_cache::PartitionLeaderCache;
use super::partition_leader_strategy::{PartitionLeaderFuture, PartitionLeaderStrategy};

/// Handler for `listOffsets`.
///
/// Corresponds to `ListOffsetsHandler` (a `Batched` handler over
/// `TopicPartition` keys yielding [`ListOffsetsResultInfo`] values).
pub(crate) struct ListOffsetsHandler {
    offset_timestamps_by_partition: HashMap<TopicPartition, i64>,
    options: ListOffsetsOptions,
    log_context: LogContext,
    lookup_strategy: PartitionLeaderStrategy,
    default_api_timeout_ms: i32,
}

impl ListOffsetsHandler {
    /// Creates a handler for the given per-partition offset timestamps.
    pub(crate) fn new(
        offset_timestamps_by_partition: HashMap<TopicPartition, i64>,
        options: ListOffsetsOptions,
        log_context: LogContext,
        default_api_timeout_ms: i32,
    ) -> Self {
        // Java: `new PartitionLeaderStrategy(logContext, false)` — do NOT
        // tolerate unknown-topic errors.
        let lookup_strategy = PartitionLeaderStrategy::with_tolerate_unknown_topics(log_context.clone(), false);
        Self {
            offset_timestamps_by_partition,
            options,
            log_context,
            lookup_strategy,
            default_api_timeout_ms,
        }
    }

    /// Creates the future bundle that the driver completes as partitions are
    /// resolved.
    ///
    /// Mirrors `ListOffsetsHandler.newFuture`.
    pub(crate) fn new_future(
        topic_partitions: HashSet<TopicPartition>,
        partition_leader_cache: Arc<PartitionLeaderCache>,
    ) -> PartitionLeaderFuture<ListOffsetsResultInfo> {
        PartitionLeaderFuture::new(topic_partitions, partition_leader_cache)
    }

    /// Builds a single batched `ListOffsets` request builder for the given keys.
    ///
    /// Mirrors `buildBatchedRequest`. Returns the concrete builder so the
    /// version-selection logic (`oldest_allowed_version`) can be inspected.
    pub(crate) fn build_batched_request(
        &self,
        _broker_id: i32,
        keys: &HashSet<TopicPartition>,
    ) -> ListOffsetsRequestBuilder {
        let mut topics_by_name: HashMap<String, ListOffsetsTopic> = HashMap::new();
        for topic_partition in keys {
            let offset_timestamp = self.offset_timestamps_by_partition.get(topic_partition).copied().unwrap_or(0);
            let topic = topics_by_name.entry(topic_partition.topic().to_string()).or_insert_with(|| {
                let mut t = ListOffsetsTopic::new();
                t.set_name(topic_partition.topic().to_string());
                t
            });
            let mut partition = ListOffsetsPartition::new();
            partition.set_partition_index(topic_partition.partition());
            partition.set_timestamp(offset_timestamp);
            topic.partitions.push(partition);
        }

        let supports_max_timestamp = keys
            .iter()
            .any(|key| self.offset_timestamps_by_partition.get(key) == Some(&MAX_TIMESTAMP));
        let require_earliest_local_timestamp = keys
            .iter()
            .any(|key| self.offset_timestamps_by_partition.get(key) == Some(&EARLIEST_LOCAL_TIMESTAMP));
        let require_tiered_storage_timestamp = keys
            .iter()
            .any(|key| self.offset_timestamps_by_partition.get(key) == Some(&LATEST_TIERED_TIMESTAMP));
        let require_earliest_pending_upload_timestamp = keys
            .iter()
            .any(|key| self.offset_timestamps_by_partition.get(key) == Some(&EARLIEST_PENDING_UPLOAD_TIMESTAMP));

        let timeout_ms = self.options.timeout().unwrap_or(self.default_api_timeout_ms);
        let mut builder = ListOffsetsRequestBuilder::for_consumer_with_features(
            true,
            self.options.isolation_level(),
            supports_max_timestamp,
            require_earliest_local_timestamp,
            require_tiered_storage_timestamp,
            require_earliest_pending_upload_timestamp,
        );
        builder
            .set_target_times(topics_by_name.into_values().collect())
            .set_timeout_ms(timeout_ms);
        builder
    }

    /// Classifies a partition error into unmapped (invalid metadata), retriable
    /// (left out of the result to retry) or failed (fatal).
    ///
    /// Mirrors `handlePartitionError`.
    fn handle_partition_error(
        &self,
        topic_partition: &TopicPartition,
        error: Errors,
        failed: &mut HashMap<TopicPartition, KafkaError>,
        unmapped: &mut Vec<TopicPartition>,
        retriable: &mut HashSet<TopicPartition>,
    ) {
        if error == Errors::NotLeaderOrFollower || error == Errors::LeaderNotAvailable {
            kafka_debug!(
                self.log_context,
                "ListOffsets lookup request for topic partition {} will be retried due to invalid leader metadata {:?}",
                topic_partition,
                error
            );
            unmapped.push(topic_partition.clone());
        } else if error.is_retriable() {
            kafka_debug!(
                self.log_context,
                "ListOffsets fulfillment request for topic partition {} will be retried due to {:?}",
                topic_partition,
                error
            );
            retriable.insert(topic_partition.clone());
        } else {
            kafka_debug!(
                self.log_context,
                "ListOffsets request for topic partition {} failed due to an unexpected error {:?}",
                topic_partition,
                error
            );
            failed.insert(topic_partition.clone(), KafkaError::new(error));
        }
    }
}

impl AdminApiHandler<TopicPartition, ListOffsetsResultInfo> for ListOffsetsHandler {
    fn api_name(&self) -> &str {
        "listOffsets"
    }

    fn build_request(&self, broker_id: i32, keys: &HashSet<TopicPartition>) -> Vec<RequestAndKeys<TopicPartition>> {
        let builder = self.build_batched_request(broker_id, keys);
        vec![RequestAndKeys { request: Box::new(builder) as Box<dyn RequestBuilder>, keys: keys.clone() }]
    }

    fn handle_response(
        &self,
        broker: &Node,
        keys: &HashSet<TopicPartition>,
        response: &ConcreteResponse,
    ) -> ApiResult<TopicPartition, ListOffsetsResultInfo> {
        let ConcreteResponse::ListOffsets(response) = response else {
            return ApiResult::new(HashMap::new(), HashMap::new(), Vec::new());
        };
        let mut completed: HashMap<TopicPartition, ListOffsetsResultInfo> = HashMap::new();
        let mut failed: HashMap<TopicPartition, KafkaError> = HashMap::new();
        let mut unmapped: Vec<TopicPartition> = Vec::new();
        let mut retriable: HashSet<TopicPartition> = HashSet::new();

        for topic in response.topics() {
            for partition in &topic.partitions {
                let topic_partition = TopicPartition::new(topic.name.as_str(), partition.partition_index);
                let error = Errors::for_code(partition.error_code);
                if !self.offset_timestamps_by_partition.contains_key(&topic_partition) {
                    kafka_debug!(
                        self.log_context,
                        "ListOffsets response includes unknown topic partition {}",
                        topic_partition
                    );
                } else if error == Errors::None {
                    let leader_epoch = if partition.leader_epoch == UNKNOWN_EPOCH {
                        None
                    } else {
                        Some(partition.leader_epoch)
                    };
                    completed.insert(
                        topic_partition,
                        ListOffsetsResultInfo::new(partition.offset, partition.timestamp, leader_epoch),
                    );
                } else {
                    self.handle_partition_error(&topic_partition, error, &mut failed, &mut unmapped, &mut retriable);
                }
            }
        }

        // Sanity-check that the current leader returned results for every key.
        for topic_partition in keys {
            if unmapped.is_empty()
                && !completed.contains_key(topic_partition)
                && !failed.contains_key(topic_partition)
                && !retriable.contains(topic_partition)
            {
                let sanity_check_error = KafkaError::with_message(
                    Errors::UnknownServerError,
                    format!(
                        "The response from broker {} did not contain a result for topic partition {}",
                        broker.id(),
                        topic_partition
                    ),
                );
                failed.insert(topic_partition.clone(), sanity_check_error);
            }
        }

        ApiResult::new(completed, failed, unmapped)
    }

    fn handle_unsupported_version_exception(
        &self,
        _broker_id: i32,
        exception: &KafkaError,
        keys: &HashSet<TopicPartition>,
    ) -> HashMap<TopicPartition, KafkaError> {
        // Only partitions with a MAX_TIMESTAMP spec can be failed by an
        // unsupported-version downgrade; if there are none (or all keys are
        // MAX_TIMESTAMP), every key is failed. Mirrors
        // `handleUnsupportedVersionException`.
        let mut max_timestamp_partitions: HashMap<TopicPartition, KafkaError> = HashMap::new();
        for topic_partition in keys {
            if self.offset_timestamps_by_partition.get(topic_partition) == Some(&MAX_TIMESTAMP) {
                max_timestamp_partitions.insert(topic_partition.clone(), exception.clone());
            }
        }
        if max_timestamp_partitions.is_empty() {
            keys.iter().map(|k| (k.clone(), exception.clone())).collect()
        } else {
            max_timestamp_partitions
        }
    }

    fn lookup_strategy(&self) -> &dyn AdminApiLookupStrategy<TopicPartition> {
        &self.lookup_strategy
    }
}
