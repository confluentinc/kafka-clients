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

//! The `deleteConsumerGroupOffsets` admin API handler.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.DeleteConsumerGroupOffsetsHandler`.
//!
//! Deletion uses a dedicated `OffsetDelete` RPC (NOT an `OffsetCommit` with a
//! sentinel offset).

use std::collections::{HashMap, HashSet};

use crate::common::protocol::Errors;
use crate::common::requests::{ConcreteResponse, CoordinatorType, OffsetDeleteRequestBuilder, RequestBuilder};
use crate::common::utils::LogContext;
use crate::common::{Error, Node, TopicPartition};
use crate::kafka_warn;
use crate::offset_delete_request_data::{
    OffsetDeleteRequestData, OffsetDeleteRequestPartition, OffsetDeleteRequestTopic,
};

use super::admin_api_future::SimpleAdminApiFuture;
use super::admin_api_handler::{AdminApiHandler, ApiResult, RequestAndKeys};
use super::admin_api_lookup_strategy::AdminApiLookupStrategy;
use super::coordinator_key::CoordinatorKey;
use super::coordinator_strategy::CoordinatorStrategy;

/// The per-partition delete result value produced by this handler.
type PartitionErrors = HashMap<TopicPartition, Errors>;

/// The `deleteConsumerGroupOffsets` handler.
///
/// Corresponds to `DeleteConsumerGroupOffsetsHandler`.
pub(crate) struct DeleteConsumerGroupOffsetsHandler {
    group_id: CoordinatorKey,
    partitions: HashSet<TopicPartition>,
    log_context: LogContext,
    lookup_strategy: CoordinatorStrategy,
}

impl DeleteConsumerGroupOffsetsHandler {
    /// Creates a handler.
    pub(crate) fn new(group_id: &str, partitions: HashSet<TopicPartition>, log_context: LogContext) -> Self {
        Self {
            group_id: CoordinatorKey::by_group_id(group_id),
            partitions,
            lookup_strategy: CoordinatorStrategy::new(CoordinatorType::Group, log_context.clone()),
            log_context,
        }
    }

    /// Creates the future bundle for the given group id.
    ///
    /// Mirrors `DeleteConsumerGroupOffsetsHandler.newFuture`.
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

    /// Builds the single `OffsetDelete` request. Mirrors `buildBatchedRequest`.
    pub(crate) fn build_batched_request(
        &self,
        _coordinator_id: i32,
        group_ids: &HashSet<CoordinatorKey>,
    ) -> OffsetDeleteRequestBuilder {
        self.validate_keys(group_ids);

        let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
        for tp in &self.partitions {
            by_topic.entry(tp.topic().to_string()).or_default().push(tp.partition());
        }
        let topics = by_topic
            .into_iter()
            .map(|(name, partition_indexes)| {
                let mut topic = OffsetDeleteRequestTopic::new();
                topic.set_name(name);
                topic.set_partitions(
                    partition_indexes
                        .into_iter()
                        .map(|partition_index| {
                            let mut partition = OffsetDeleteRequestPartition::new();
                            partition.set_partition_index(partition_index);
                            partition
                        })
                        .collect(),
                );
                topic
            })
            .collect();

        let mut data = OffsetDeleteRequestData::new();
        data.set_group_id(self.group_id.id_value.clone());
        data.set_topics(topics);
        OffsetDeleteRequestBuilder::new(data)
    }

    fn handle_group_error(
        &self,
        error: Errors,
        failed: &mut HashMap<CoordinatorKey, Error>,
        groups_to_unmap: &mut HashSet<CoordinatorKey>,
    ) {
        match error {
            Errors::GroupAuthorizationFailed
            | Errors::GroupIdNotFound
            | Errors::InvalidGroupId
            | Errors::NonEmptyGroup => {
                kafka_warn!(
                    self.log_context,
                    "`OffsetDelete` request for group id {} failed due to error {:?}.",
                    self.group_id.id_value,
                    error
                );
                failed.insert(self.group_id.clone(), Error::new(error));
            },
            Errors::CoordinatorLoadInProgress => {
                // If the coordinator is loading, we just need to retry.
                kafka_warn!(
                    self.log_context,
                    "`OffsetDelete` request for group id {} failed because the coordinator is still in the \
                     process of loading state. Will retry.",
                    self.group_id.id_value
                );
            },
            Errors::CoordinatorNotAvailable | Errors::NotCoordinator => {
                // Unmap so we retry the `FindCoordinator` request.
                kafka_warn!(
                    self.log_context,
                    "`OffsetDelete` request for group id {} returned error {:?}. Will attempt to find the \
                     coordinator again and retry.",
                    self.group_id.id_value,
                    error
                );
                groups_to_unmap.insert(self.group_id.clone());
            },
            other => {
                kafka_warn!(
                    self.log_context,
                    "`OffsetDelete` request for group id {} failed due to unexpected error {:?}.",
                    self.group_id.id_value,
                    other
                );
                failed.insert(self.group_id.clone(), Error::new(other));
            },
        }
    }
}

impl AdminApiHandler<CoordinatorKey, PartitionErrors> for DeleteConsumerGroupOffsetsHandler {
    fn api_name(&self) -> &str {
        "offsetDelete"
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

        let ConcreteResponse::OffsetDelete(response) = response else {
            panic!("DeleteConsumerGroupOffsetsHandler received an unexpected response type: {response:?}");
        };

        let error = Errors::for_code(response.data().error_code);

        if error != Errors::None {
            let mut failed = HashMap::new();
            let mut groups_to_unmap = HashSet::new();
            self.handle_group_error(error, &mut failed, &mut groups_to_unmap);
            ApiResult::new(HashMap::new(), failed, groups_to_unmap.into_iter().collect())
        } else {
            let mut partition_results: PartitionErrors = HashMap::new();
            for topic in &response.data().topics {
                for partition in &topic.partitions {
                    partition_results.insert(
                        TopicPartition::new(topic.name.clone(), partition.partition_index),
                        Errors::for_code(partition.error_code),
                    );
                }
            }
            ApiResult::new(
                HashMap::from([(self.group_id.clone(), partition_results)]),
                HashMap::new(),
                Vec::new(),
            )
        }
    }

    fn lookup_strategy(&self) -> &dyn AdminApiLookupStrategy<CoordinatorKey> {
        &self.lookup_strategy
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::requests::{ConcreteResponse, OffsetDeleteResponse};
    use crate::offset_delete_response_data::{
        OffsetDeleteResponseData, OffsetDeleteResponsePartition, OffsetDeleteResponseTopic,
    };

    const GROUP_ID: &str = "group-id";

    fn log_context() -> LogContext {
        LogContext::new(String::new())
    }

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic, partition)
    }

    fn tps() -> HashSet<TopicPartition> {
        HashSet::from([tp("t0", 0), tp("t0", 1), tp("t1", 0)])
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

    fn handler() -> DeleteConsumerGroupOffsetsHandler {
        DeleteConsumerGroupOffsetsHandler::new(GROUP_ID, tps(), log_context())
    }

    fn topic_with_partition(topic: &str, partition: i32, error: Errors) -> OffsetDeleteResponseTopic {
        let mut p = OffsetDeleteResponsePartition::new();
        p.set_partition_index(partition).set_error_code(error.code());
        let mut t = OffsetDeleteResponseTopic::new();
        t.set_name(topic.to_string()).set_partitions(vec![p]);
        t
    }

    fn build_group_error_response(error: Errors) -> ConcreteResponse {
        let mut data = OffsetDeleteResponseData::new();
        data.set_error_code(error.code());
        if error == Errors::None {
            data.set_throttle_time_ms(0);
            data.set_topics(vec![topic_with_partition("t0", 0, error)]);
        }
        ConcreteResponse::OffsetDelete(OffsetDeleteResponse::new(data))
    }

    fn build_partition_error_response(error: Errors) -> ConcreteResponse {
        let mut data = OffsetDeleteResponseData::new();
        data.set_throttle_time_ms(0);
        data.set_topics(vec![topic_with_partition("t0", 0, error)]);
        ConcreteResponse::OffsetDelete(OffsetDeleteResponse::new(data))
    }

    fn handle_with_group_error(error: Errors) -> ApiResult<CoordinatorKey, PartitionErrors> {
        handler().handle_response(&node(), &keys(), &build_group_error_response(error))
    }

    fn handle_with_partition_error(error: Errors) -> ApiResult<CoordinatorKey, PartitionErrors> {
        handler().handle_response(&node(), &keys(), &build_partition_error_response(error))
    }

    /// Translated from `testBuildRequest`.
    #[test]
    fn test_build_request() {
        let request = handler().build_batched_request(1, &keys());
        let data = request.data();
        assert_eq!(data.group_id, GROUP_ID);
        assert_eq!(data.topics.len(), 2);
        let t0 = data.topics.iter().find(|t| t.name == "t0").unwrap();
        assert_eq!(t0.partitions.len(), 2);
        let t1 = data.topics.iter().find(|t| t.name == "t1").unwrap();
        assert_eq!(t1.partitions.len(), 1);
    }

    /// Translated from `testSuccessfulHandleResponse`.
    #[test]
    fn test_successful_handle_response() {
        let result = handle_with_group_error(Errors::None);
        assert!(result.failed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
        assert_eq!(
            result.completed_keys.get(&key()),
            Some(&PartitionErrors::from([(tp("t0", 0), Errors::None)]))
        );
    }

    /// Translated from `testUnmappedHandleResponse`.
    #[test]
    fn test_unmapped_handle_response() {
        for error in [Errors::NotCoordinator, Errors::CoordinatorNotAvailable] {
            let result = handle_with_group_error(error);
            assert!(result.completed_keys.is_empty());
            assert!(result.failed_keys.is_empty());
            assert_eq!(result.unmapped_keys, vec![key()]);
        }
    }

    /// Translated from `testRetriableHandleResponse`.
    #[test]
    fn test_retriable_handle_response() {
        let result = handle_with_group_error(Errors::CoordinatorLoadInProgress);
        assert!(result.completed_keys.is_empty());
        assert!(result.failed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
    }

    /// Translated from `testFailedHandleResponseWithGroupError`.
    #[test]
    fn test_failed_handle_response_with_group_error() {
        for error in [
            Errors::GroupAuthorizationFailed,
            Errors::GroupIdNotFound,
            Errors::InvalidGroupId,
            Errors::NonEmptyGroup,
        ] {
            let result = handle_with_group_error(error);
            assert!(result.completed_keys.is_empty());
            assert!(result.unmapped_keys.is_empty());
            assert_eq!(result.failed_keys.get(&key()).unwrap().error(), error);
        }
    }

    /// Translated from `testFailedHandleResponseWithPartitionError`.
    #[test]
    fn test_failed_handle_response_with_partition_error() {
        for error in [
            Errors::GroupSubscribedToTopic,
            Errors::TopicAuthorizationFailed,
            Errors::UnknownTopicOrPartition,
        ] {
            let result = handle_with_partition_error(error);
            assert!(result.unmapped_keys.is_empty());
            assert!(result.failed_keys.is_empty());
            assert_eq!(result.completed_keys.len(), 1);
            assert_eq!(
                result.completed_keys.get(&key()),
                Some(&PartitionErrors::from([(tp("t0", 0), error)]))
            );
        }
    }
}
