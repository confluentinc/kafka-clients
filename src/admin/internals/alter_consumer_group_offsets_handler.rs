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

//! The `alterConsumerGroupOffsets` admin API handler.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.AlterConsumerGroupOffsetsHandler`.

use std::collections::{HashMap, HashSet};

use crate::OffsetCommitRequestData;
use crate::common::Errors;
use crate::common::requests::{ConcreteResponse, CoordinatorType, OffsetCommitRequestBuilder, RequestBuilder};
use crate::common::utils::LogContext;
use crate::common::{Error, Node, TopicPartition};
use crate::consumer::OffsetAndMetadata;
use crate::kafka_warn;
use crate::offset_commit_request_data::{OffsetCommitRequestPartition, OffsetCommitRequestTopic};

use super::AdminApiLookupStrategy;
use super::CoordinatorKey;
use super::CoordinatorStrategy;
use super::SimpleAdminApiFuture;
use super::{AdminApiHandler, ApiResult, RequestAndKeys};

/// The per-partition commit result value produced by this handler.
type PartitionErrors = HashMap<TopicPartition, Errors>;

/// The `alterConsumerGroupOffsets` handler.
///
/// Corresponds to `AlterConsumerGroupOffsetsHandler`.
pub(crate) struct AlterConsumerGroupOffsetsHandler {
    group_id: CoordinatorKey,
    offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    log_context: LogContext,
    lookup_strategy: CoordinatorStrategy,
}

impl AlterConsumerGroupOffsetsHandler {
    /// Creates a handler.
    pub(crate) fn new(
        group_id: &str,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        log_context: LogContext,
    ) -> Self {
        Self {
            group_id: CoordinatorKey::by_group_id(group_id),
            offsets,
            lookup_strategy: CoordinatorStrategy::new(CoordinatorType::Group, log_context.clone()),
            log_context,
        }
    }

    /// Creates the future bundle for the given group id.
    ///
    /// Mirrors `AlterConsumerGroupOffsetsHandler.newFuture`.
    pub(crate) fn new_future(group_id: &str) -> SimpleAdminApiFuture<CoordinatorKey, PartitionErrors> {
        SimpleAdminApiFuture::for_keys(HashSet::from([CoordinatorKey::by_group_id(group_id)]))
    }

    /// Mirrors `validateKeys`: the requested keys must be exactly the single
    /// group id owned by this handler.
    fn validate_keys(&self, group_ids: &HashSet<CoordinatorKey>) {
        let expected = HashSet::from([self.group_id.clone()]);
        assert!(
            group_ids == &expected,
            "Received unexpected group ids {group_ids:?} (expected only {expected:?})"
        );
    }

    /// Builds the single `OffsetCommit` request. Mirrors `buildBatchedRequest`.
    pub(crate) fn build_batched_request(
        &self,
        _coordinator_id: i32,
        group_ids: &HashSet<CoordinatorKey>,
    ) -> OffsetCommitRequestBuilder {
        self.validate_keys(group_ids);

        let mut offset_data: HashMap<String, OffsetCommitRequestTopic> = HashMap::new();
        for (topic_partition, offset_and_metadata) in &self.offsets {
            let topic = offset_data.entry(topic_partition.topic().to_string()).or_insert_with(|| {
                let mut topic = OffsetCommitRequestTopic::new();
                topic.set_name(topic_partition.topic().to_string());
                topic
            });
            let mut partition = OffsetCommitRequestPartition::new();
            partition
                .set_committed_offset(offset_and_metadata.offset())
                .set_committed_leader_epoch(offset_and_metadata.leader_epoch().unwrap_or(-1))
                .set_committed_metadata(Some(offset_and_metadata.metadata().to_string()))
                .set_partition_index(topic_partition.partition());
            topic.partitions.push(partition);
        }

        let mut data = OffsetCommitRequestData::new();
        data.set_group_id(self.group_id.id_value.clone());
        data.set_topics(offset_data.into_values().collect());
        OffsetCommitRequestBuilder::for_topic_names(data)
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_error(
        &self,
        topic_partition: TopicPartition,
        error: Errors,
        partition_results: &mut PartitionErrors,
        groups_to_unmap: &mut HashSet<CoordinatorKey>,
        groups_to_retry: &mut HashSet<CoordinatorKey>,
    ) {
        match error {
            // If the coordinator is loading, or a rebalance is in progress, retry.
            Errors::CoordinatorLoadInProgress | Errors::RebalanceInProgress => {
                kafka_warn!(
                    self.log_context,
                    "OffsetCommit request for group id {} returned error {:?}. Will retry.",
                    self.group_id.id_value,
                    error
                );
                groups_to_retry.insert(self.group_id.clone());
            },
            // If the coordinator is not available, unmap and retry.
            Errors::CoordinatorNotAvailable | Errors::NotCoordinator => {
                kafka_warn!(
                    self.log_context,
                    "OffsetCommit request for group id {} returned error {:?}. Will rediscover the \
                     coordinator and retry.",
                    self.group_id.id_value,
                    error
                );
                groups_to_unmap.insert(self.group_id.clone());
            },
            // Group-level and member-level errors: record the partition result.
            Errors::InvalidGroupId
            | Errors::InvalidCommitOffsetSize
            | Errors::GroupAuthorizationFailed
            | Errors::GroupIdNotFound
            | Errors::UnknownMemberId
            | Errors::StaleMemberEpoch => {
                kafka_warn!(
                    self.log_context,
                    "OffsetCommit request for group id {} failed due to error {:?}.",
                    self.group_id.id_value,
                    error
                );
                partition_results.insert(topic_partition, error);
            },
            // TopicPartition-level errors: record the partition result.
            Errors::UnknownTopicOrPartition | Errors::OffsetMetadataTooLarge | Errors::TopicAuthorizationFailed => {
                kafka_warn!(
                    self.log_context,
                    "OffsetCommit request for group id {} and partition {} failed due to error {:?}.",
                    self.group_id.id_value,
                    topic_partition,
                    error
                );
                partition_results.insert(topic_partition, error);
            },
            other => {
                kafka_warn!(
                    self.log_context,
                    "OffsetCommit request for group id {} and partition {} failed due to unexpected error {:?}.",
                    self.group_id.id_value,
                    topic_partition,
                    other
                );
                partition_results.insert(topic_partition, other);
            },
        }
    }
}

impl AdminApiHandler<CoordinatorKey, PartitionErrors> for AlterConsumerGroupOffsetsHandler {
    fn api_name(&self) -> &str {
        "offsetCommit"
    }

    fn build_request(
        &self,
        broker_id: i32,
        group_ids: &HashSet<CoordinatorKey>,
    ) -> Vec<RequestAndKeys<CoordinatorKey>> {
        vec![RequestAndKeys {
            request: Box::new(self.build_batched_request(broker_id, group_ids)) as Box<dyn RequestBuilder>,
            keys: group_ids.clone(),
        }]
    }

    fn handle_response(
        &self,
        _coordinator: &Node,
        group_ids: &HashSet<CoordinatorKey>,
        response: &ConcreteResponse,
    ) -> ApiResult<CoordinatorKey, PartitionErrors> {
        self.validate_keys(group_ids);

        let ConcreteResponse::OffsetCommit(response) = response else {
            // Java's `(OffsetCommitResponse) abstractResponse` downcast throws
            // `ClassCastException`, which `KafkaAdminClient.java:1387-1391` catches
            // and turns into `call.fail(now, t)` — this RPC fails, the client keeps
            // serving everything else. A `panic!` here instead killed the admin
            // background task and poisoned the driver mutex.
            return ApiResult::failed_all(
                group_ids,
                Error::local_illegal_state("AlterConsumerGroupOffsetsHandler received an unexpected response type"),
            );
        };

        let mut groups_to_unmap = HashSet::new();
        let mut groups_to_retry = HashSet::new();
        let mut partition_results: PartitionErrors = HashMap::new();

        for topic in response.topics() {
            for partition in &topic.partitions {
                let topic_partition = TopicPartition::new(topic.name.clone(), partition.partition_index);
                let error = Errors::for_code(partition.error_code);
                if error != Errors::None {
                    self.handle_error(
                        topic_partition,
                        error,
                        &mut partition_results,
                        &mut groups_to_unmap,
                        &mut groups_to_retry,
                    );
                } else {
                    partition_results.insert(topic_partition, error);
                }
            }
        }

        if groups_to_unmap.is_empty() && groups_to_retry.is_empty() {
            ApiResult::new(
                HashMap::from([(self.group_id.clone(), partition_results)]),
                HashMap::new(),
                Vec::new(),
            )
        } else {
            // Empty completed/failed + (possibly empty) unmapped list: an empty
            // unmapped list means "retry", a non-empty one means "re-lookup".
            ApiResult::new(HashMap::new(), HashMap::new(), groups_to_unmap.into_iter().collect())
        }
    }

    fn lookup_strategy(&self) -> &dyn AdminApiLookupStrategy<CoordinatorKey> {
        &self.lookup_strategy
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::common::requests::{ConcreteResponse, OffsetCommitResponse};

    const GROUP_ID: &str = "group-id";
    const OFFSET: i64 = 1;

    fn log_context() -> LogContext {
        LogContext::new(String::new())
    }

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic, partition)
    }

    fn node() -> Node {
        Node::new(1, "host".to_string(), 1234)
    }

    fn key() -> CoordinatorKey {
        CoordinatorKey::by_group_id(GROUP_ID)
    }

    fn keys() -> HashSet<CoordinatorKey> {
        HashSet::from([key()])
    }

    fn partitions() -> HashMap<TopicPartition, OffsetAndMetadata> {
        [tp("t0", 0), tp("t0", 1), tp("t1", 0), tp("t1", 1)]
            .into_iter()
            .map(|tp| (tp, OffsetAndMetadata::new(OFFSET).unwrap()))
            .collect()
    }

    fn handler() -> AlterConsumerGroupOffsetsHandler {
        AlterConsumerGroupOffsetsHandler::new(GROUP_ID, partitions(), log_context())
    }

    fn response(partition_results: &PartitionErrors) -> ConcreteResponse {
        ConcreteResponse::OffsetCommit(OffsetCommitResponse::with_throttle_time_ms_response_data(0, partition_results))
    }

    fn partition_errors(error: Errors) -> PartitionErrors {
        partitions().keys().map(|tp| (tp.clone(), error)).collect()
    }

    fn handle(partition_results: &PartitionErrors) -> ApiResult<CoordinatorKey, PartitionErrors> {
        handler().handle_response(&node(), &keys(), &response(partition_results))
    }

    /// Translated from `testBuildRequest`.
    #[test]
    fn test_build_request() {
        let request = handler().build_batched_request(-1, &keys());
        let data = request.data();
        assert_eq!(data.group_id, GROUP_ID);
        assert_eq!(data.topics.len(), 2);
        for topic in &data.topics {
            assert_eq!(topic.partitions.len(), 2);
            for partition in &topic.partitions {
                assert_eq!(partition.committed_offset, OFFSET);
            }
        }
    }

    /// Translated from `testHandleSuccessfulResponse`.
    #[test]
    fn test_handle_successful_response() {
        let response_data = PartitionErrors::from([(tp("t0", 0), Errors::None)]);
        let result = handle(&response_data);
        assert!(result.failed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
        assert_eq!(result.completed_keys.get(&key()), Some(&response_data));
    }

    fn assert_fatal_error(partition_results: PartitionErrors) {
        let result = handle(&partition_results);
        assert_eq!(result.completed_keys.get(&key()), Some(&partition_results));
        assert!(result.unmapped_keys.is_empty());
        assert!(result.failed_keys.is_empty());
    }

    fn assert_retriable_error(partition_results: PartitionErrors) {
        let result = handle(&partition_results);
        assert!(result.completed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
        assert!(result.failed_keys.is_empty());
    }

    fn assert_unmapped_key(partition_results: PartitionErrors) {
        let result = handle(&partition_results);
        assert!(result.completed_keys.is_empty());
        assert!(result.failed_keys.is_empty());
        assert_eq!(result.unmapped_keys, vec![key()]);
    }

    /// Translated from `testHandleRetriableResponse`.
    #[test]
    fn test_handle_retriable_response() {
        assert_unmapped_key(partition_errors(Errors::NotCoordinator));
        assert_unmapped_key(partition_errors(Errors::CoordinatorNotAvailable));
        assert_retriable_error(partition_errors(Errors::CoordinatorLoadInProgress));
        assert_retriable_error(partition_errors(Errors::RebalanceInProgress));
    }

    /// Translated from `testHandleErrorResponse`.
    #[test]
    fn test_handle_error_response() {
        for error in [
            Errors::TopicAuthorizationFailed,
            Errors::GroupAuthorizationFailed,
            Errors::InvalidGroupId,
            Errors::UnknownTopicOrPartition,
            Errors::OffsetMetadataTooLarge,
            Errors::IllegalGeneration,
            Errors::UnknownMemberId,
            Errors::InvalidCommitOffsetSize,
            Errors::UnknownServerError,
        ] {
            assert_fatal_error(partition_errors(error));
        }
    }

    /// Translated from `testHandleMultipleErrorsResponse`.
    #[test]
    fn test_handle_multiple_errors_response() {
        let partition_errors = PartitionErrors::from([
            (tp("t0", 0), Errors::UnknownTopicOrPartition),
            (tp("t0", 1), Errors::InvalidCommitOffsetSize),
            (tp("t1", 0), Errors::TopicAuthorizationFailed),
            (tp("t1", 1), Errors::OffsetMetadataTooLarge),
        ]);
        assert_fatal_error(partition_errors);
    }
}
