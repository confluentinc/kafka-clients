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

//! The `listConsumerGroupOffsets` admin API handler.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.ListConsumerGroupOffsetsHandler`.

use std::collections::{HashMap, HashSet};

use crate::OffsetFetchRequestData;
use crate::admin::{GroupOffsets, ListConsumerGroupOffsetsSpec};
use crate::common::Errors;
use crate::common::requests::RequestUtils;
use crate::common::requests::{ConcreteResponse, CoordinatorType, OffsetFetchRequestBuilder, RequestBuilder};
use crate::common::utils::LogContext;
use crate::common::{Error, Node, TopicPartition};
use crate::consumer::OffsetAndMetadata;
use crate::kafka_warn;
use crate::offset_fetch_request_data::{OffsetFetchRequestGroup, OffsetFetchRequestTopics};

use super::AdminApiLookupStrategy;
use super::CoordinatorKey;
use super::CoordinatorStrategy;
use super::SimpleAdminApiFuture;
use super::{AdminApiHandler, ApiResult, RequestAndKeys};

/// The `listConsumerGroupOffsets` handler.
///
/// Corresponds to `ListConsumerGroupOffsetsHandler`. `V` is
/// [`GroupOffsets`] (Java's `Map<TopicPartition, OffsetAndMetadata>`, where a
/// `None` value means "no committed offset").
pub(crate) struct ListConsumerGroupOffsetsHandler {
    require_stable: bool,
    group_specs: HashMap<String, ListConsumerGroupOffsetsSpec>,
    log_context: LogContext,
    lookup_strategy: CoordinatorStrategy,
}

impl ListConsumerGroupOffsetsHandler {
    /// Creates a handler.
    pub(crate) fn new(
        group_specs: HashMap<String, ListConsumerGroupOffsetsSpec>,
        require_stable: bool,
        log_context: LogContext,
    ) -> Self {
        Self {
            require_stable,
            group_specs,
            lookup_strategy: CoordinatorStrategy::new(CoordinatorType::Group, log_context.clone()),
            log_context,
        }
    }

    /// Creates the future bundle for the given group ids.
    ///
    /// Mirrors `ListConsumerGroupOffsetsHandler.newFuture`.
    pub(crate) fn new_future(group_ids: &[String]) -> SimpleAdminApiFuture<CoordinatorKey, GroupOffsets> {
        SimpleAdminApiFuture::for_keys(Self::coordinator_keys(group_ids))
    }

    fn coordinator_keys<'a>(group_ids: impl IntoIterator<Item = &'a String>) -> HashSet<CoordinatorKey> {
        group_ids.into_iter().map(CoordinatorKey::by_group_id).collect()
    }

    /// Mirrors `validateKeys`: the requested keys must be a subset of the keys
    /// owned by this handler (its group specs). A violation is a programming
    /// error (Java throws `IllegalArgumentException`); the driver only ever
    /// passes keys drawn from the handler's own future.
    fn validate_keys(&self, group_ids: &HashSet<CoordinatorKey>) {
        let keys = Self::coordinator_keys(self.group_specs.keys());
        assert!(
            group_ids.iter().all(|k| keys.contains(k)),
            "Received unexpected group ids {group_ids:?} (expected one of {keys:?})"
        );
    }

    /// Builds a single (possibly batched) `OffsetFetch` request for the given
    /// group ids. Mirrors `buildBatchedRequest`.
    pub(crate) fn build_batched_request(&self, group_ids: &HashSet<CoordinatorKey>) -> OffsetFetchRequestBuilder {
        let mut data = OffsetFetchRequestData::new();
        data.set_require_stable(self.require_stable);
        let groups = group_ids
            .iter()
            .map(|group_id| {
                let spec = self
                    .group_specs
                    .get(&group_id.id_value)
                    .expect("validated: key belongs to a spec");
                let topics = spec.topic_partitions().map(|partitions| {
                    let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
                    for tp in partitions {
                        by_topic.entry(tp.topic().to_string()).or_default().push(tp.partition());
                    }
                    by_topic
                        .into_iter()
                        .map(|(name, partition_indexes)| {
                            let mut topic = OffsetFetchRequestTopics::new();
                            topic.set_name(name);
                            topic.set_partition_indexes(partition_indexes);
                            topic
                        })
                        .collect::<Vec<_>>()
                });
                let mut group = OffsetFetchRequestGroup::new();
                group.set_group_id(group_id.id_value.clone());
                group.set_topics(topics);
                group
            })
            .collect();
        data.set_groups(groups);
        OffsetFetchRequestBuilder::for_topic_names(data, false)
    }

    fn handle_group_error(
        &self,
        group_id: CoordinatorKey,
        error: Errors,
        failed: &mut HashMap<CoordinatorKey, Error>,
        groups_to_unmap: &mut Vec<CoordinatorKey>,
    ) {
        match error {
            Errors::GroupAuthorizationFailed | Errors::UnknownMemberId | Errors::StaleMemberEpoch => {
                kafka_warn!(
                    self.log_context,
                    "`OffsetFetch` request for group id {} failed due to error {:?}",
                    group_id.id_value,
                    error
                );
                failed.insert(group_id, Error::new(error));
            },
            Errors::CoordinatorLoadInProgress => {
                // If the coordinator is loading, we just need to retry.
                kafka_warn!(
                    self.log_context,
                    "`OffsetFetch` request for group id {} failed because the coordinator is still in the \
                     process of loading state. Will retry",
                    group_id.id_value
                );
            },
            Errors::CoordinatorNotAvailable | Errors::NotCoordinator => {
                // Unmap so we retry the `FindCoordinator` request.
                kafka_warn!(
                    self.log_context,
                    "`OffsetFetch` request for group id {} returned error {:?}. Will attempt to find the \
                     coordinator again and retry",
                    group_id.id_value,
                    error
                );
                groups_to_unmap.push(group_id);
            },
            other => {
                kafka_warn!(
                    self.log_context,
                    "`OffsetFetch` request for group id {} failed due to unexpected error {:?}",
                    group_id.id_value,
                    other
                );
                failed.insert(group_id, Error::new(other));
            },
        }
    }
}

impl AdminApiHandler<CoordinatorKey, GroupOffsets> for ListConsumerGroupOffsetsHandler {
    fn api_name(&self) -> &str {
        "offsetFetch"
    }

    fn build_request(
        &self,
        _broker_id: i32,
        group_ids: &HashSet<CoordinatorKey>,
    ) -> Vec<RequestAndKeys<CoordinatorKey>> {
        self.validate_keys(group_ids);

        // When the OffsetFetchRequest fails with NoBatchedOffsetFetchRequestException,
        // the driver disables batching end-to-end (including FindCoordinator).
        if self.lookup_strategy.batch() {
            vec![RequestAndKeys {
                request: Box::new(self.build_batched_request(group_ids)) as Box<dyn RequestBuilder>,
                keys: group_ids.clone(),
            }]
        } else {
            group_ids
                .iter()
                .map(|group_id| {
                    let keys = HashSet::from([group_id.clone()]);
                    RequestAndKeys {
                        request: Box::new(self.build_batched_request(&keys)) as Box<dyn RequestBuilder>,
                        keys,
                    }
                })
                .collect()
        }
    }

    fn handle_response(
        &self,
        _coordinator: &Node,
        group_ids: &HashSet<CoordinatorKey>,
        response: &ConcreteResponse,
    ) -> ApiResult<CoordinatorKey, GroupOffsets> {
        self.validate_keys(group_ids);

        let ConcreteResponse::OffsetFetch(response) = response else {
            // `KafkaAdminClient.java:1387-1391` fails this one call on a response-type
            // mismatch; see `ApiResult::failed_all`.
            return ApiResult::failed_all(
                group_ids,
                Error::local_illegal_state("ListConsumerGroupOffsetsHandler received an unexpected response type"),
            );
        };

        let mut completed = HashMap::new();
        let mut failed = HashMap::new();
        let mut unmapped = Vec::new();

        for coordinator_key in group_ids {
            let group = response
                .group(&coordinator_key.id_value)
                .expect("requested group is present in the response");
            let error = Errors::for_code(group.error_code);

            if error != Errors::None {
                self.handle_group_error(coordinator_key.clone(), error, &mut failed, &mut unmapped);
            } else {
                let mut offsets: GroupOffsets = HashMap::new();
                for topic in &group.topics {
                    for partition in &topic.partitions {
                        let tp = TopicPartition::new(topic.name.clone(), partition.partition_index);
                        let partition_error = Errors::for_code(partition.error_code);
                        if partition_error == Errors::None {
                            // A negative offset indicates that the group has no
                            // committed offset for this partition.
                            if partition.committed_offset < 0 {
                                offsets.insert(tp, None);
                            } else {
                                let offset_and_metadata = OffsetAndMetadata::new_leader_epoch_metadata(
                                    partition.committed_offset,
                                    RequestUtils::get_leader_epoch(partition.committed_leader_epoch),
                                    partition.metadata.clone().unwrap_or_default(),
                                )
                                .expect("committed offset is non-negative in this branch");
                                offsets.insert(tp, Some(offset_and_metadata));
                            }
                        } else {
                            kafka_warn!(
                                self.log_context,
                                "Skipping return offset for {} due to error {:?}.",
                                tp,
                                partition_error
                            );
                        }
                    }
                }
                completed.insert(coordinator_key.clone(), offsets);
            }
        }

        ApiResult::new(completed, failed, unmapped)
    }

    fn lookup_strategy(&self) -> &dyn AdminApiLookupStrategy<CoordinatorKey> {
        &self.lookup_strategy
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap, HashSet};

    use super::*;
    use crate::OffsetFetchResponseData;
    use crate::common::ApiKeys;
    use crate::common::requests::{ConcreteResponse, OffsetFetchResponse};
    use crate::offset_fetch_response_data::{
        OffsetFetchResponseGroup, OffsetFetchResponsePartitions, OffsetFetchResponseTopics,
    };

    const GROUP0: &str = "group0";
    const GROUP1: &str = "group1";
    const GROUP2: &str = "group2";
    const GROUP3: &str = "group3";

    fn log_context() -> LogContext {
        LogContext::new(String::new())
    }

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic, partition)
    }

    fn spec(partitions: &[TopicPartition]) -> ListConsumerGroupOffsetsSpec {
        ListConsumerGroupOffsetsSpec::new().set_topic_partitions(Some(partitions.to_vec()))
    }

    fn single_group_spec() -> HashMap<String, ListConsumerGroupOffsetsSpec> {
        HashMap::from([(GROUP0.to_string(), spec(&[tp("t0", 0), tp("t0", 1), tp("t1", 0), tp("t1", 1)]))])
    }

    fn multi_group_specs() -> HashMap<String, ListConsumerGroupOffsetsSpec> {
        HashMap::from([
            (GROUP0.to_string(), spec(&[tp("t0", 0)])),
            (GROUP1.to_string(), spec(&[tp("t0", 0), tp("t1", 0), tp("t1", 1)])),
            (
                GROUP2.to_string(),
                spec(&[
                    tp("t0", 0),
                    tp("t1", 0),
                    tp("t1", 1),
                    tp("t2", 0),
                    tp("t2", 1),
                    tp("t2", 2),
                ]),
            ),
        ])
    }

    fn coordinator_keys(group_ids: &[&str]) -> HashSet<CoordinatorKey> {
        group_ids.iter().map(|g| CoordinatorKey::by_group_id(*g)).collect()
    }

    fn node() -> Node {
        Node::new(1, "host".to_string(), 1234)
    }

    /// Normalises a request group into (group_id, set of (topic, sorted
    /// partition indexes)) so equality checks do not depend on the
    /// (HashMap-derived) topic / partition order — Java's `groupingBy` has the
    /// same non-determinism.
    fn group_shape(group: &OffsetFetchRequestGroup) -> (String, BTreeSet<(String, Vec<i32>)>) {
        let topics = group
            .topics
            .as_ref()
            .map(|topics| {
                topics
                    .iter()
                    .map(|t| {
                        let mut partitions = t.partition_indexes.clone();
                        partitions.sort_unstable();
                        (t.name.clone(), partitions)
                    })
                    .collect()
            })
            .unwrap_or_default();
        (group.group_id.clone(), topics)
    }

    fn build_offset_fetch(mut request: Box<dyn RequestBuilder>) -> OffsetFetchRequestData {
        match request.build().unwrap() {
            crate::common::requests::ConcreteRequest::OffsetFetch(r) => r.data().clone(),
            other => panic!("expected OffsetFetch request, got {}", other.api_key().name()),
        }
    }

    fn request_group_ids(request: Box<dyn RequestBuilder>) -> HashSet<String> {
        build_offset_fetch(request).groups.iter().map(|g| g.group_id.clone()).collect()
    }

    /// Translated from `testBuildRequest`.
    #[test]
    fn test_build_request() {
        let handler = ListConsumerGroupOffsetsHandler::new(single_group_spec(), false, log_context());
        let data = handler.build_batched_request(&coordinator_keys(&[GROUP0])).data().clone();
        assert_eq!(data.groups.len(), 1);
        assert_eq!(
            group_shape(&data.groups[0]),
            (
                GROUP0.to_string(),
                BTreeSet::from([("t0".to_string(), vec![0, 1]), ("t1".to_string(), vec![0, 1])])
            )
        );
    }

    /// Translated from `testBuildRequestWithMultipleGroups`.
    #[test]
    fn test_build_request_with_multiple_groups() {
        let mut specs = multi_group_specs();
        specs.insert(GROUP3.to_string(), spec(&[tp("t3", 0), tp("t3", 1)]));
        let handler = ListConsumerGroupOffsetsHandler::new(specs, false, log_context());

        let request1 = handler.build_batched_request(&coordinator_keys(&[GROUP0, GROUP1, GROUP2]));
        let shapes1: BTreeSet<_> = request1.data().groups.iter().map(group_shape).collect();
        assert_eq!(
            shapes1,
            BTreeSet::from([
                (GROUP0.to_string(), BTreeSet::from([("t0".to_string(), vec![0])])),
                (
                    GROUP1.to_string(),
                    BTreeSet::from([("t0".to_string(), vec![0]), ("t1".to_string(), vec![0, 1])])
                ),
                (
                    GROUP2.to_string(),
                    BTreeSet::from([
                        ("t0".to_string(), vec![0]),
                        ("t1".to_string(), vec![0, 1]),
                        ("t2".to_string(), vec![0, 1, 2])
                    ])
                ),
            ])
        );

        let request2 = handler.build_batched_request(&coordinator_keys(&[GROUP3]));
        let shapes2: BTreeSet<_> = request2.data().groups.iter().map(group_shape).collect();
        assert_eq!(
            shapes2,
            BTreeSet::from([(GROUP3.to_string(), BTreeSet::from([("t3".to_string(), vec![0, 1])]))])
        );
    }

    /// Translated from `testBuildRequestBatchGroups`.
    #[test]
    fn test_build_request_batch_groups() {
        let handler = ListConsumerGroupOffsetsHandler::new(multi_group_specs(), false, log_context());
        let mut requests = handler.build_request(1, &coordinator_keys(&[GROUP0, GROUP1, GROUP2]));
        assert_eq!(requests.len(), 1);
        assert_eq!(
            request_group_ids(requests.remove(0).request),
            HashSet::from([GROUP0.to_string(), GROUP1.to_string(), GROUP2.to_string()])
        );
    }

    /// Translated from `testBuildRequestDoesNotBatchGroup`.
    #[test]
    fn test_build_request_does_not_batch_group() {
        let handler = ListConsumerGroupOffsetsHandler::new(multi_group_specs(), false, log_context());
        // Disable batching (mirrors the driver's NoBatched downgrade).
        handler.lookup_strategy().disable_batch();
        let requests = handler.build_request(1, &coordinator_keys(&[GROUP0, GROUP1, GROUP2]));
        assert_eq!(requests.len(), 3);
        let group_sets: HashSet<BTreeSet<String>> = requests
            .into_iter()
            .map(|rk| request_group_ids(rk.request).into_iter().collect())
            .collect();
        assert_eq!(
            group_sets,
            HashSet::from([
                BTreeSet::from([GROUP0.to_string()]),
                BTreeSet::from([GROUP1.to_string()]),
                BTreeSet::from([GROUP2.to_string()]),
            ])
        );
    }

    fn build_response(groups: Vec<OffsetFetchResponseGroup>) -> ConcreteResponse {
        let mut data = OffsetFetchResponseData::new();
        data.set_groups(groups);
        ConcreteResponse::OffsetFetch(OffsetFetchResponse::new(data, ApiKeys::OFFSET_FETCH.latest_version()))
    }

    fn group_with_error(group_id: &str, error: Errors) -> OffsetFetchResponseGroup {
        let mut group = OffsetFetchResponseGroup::new();
        group.set_group_id(group_id.to_string()).set_error_code(error.code());
        group
    }

    fn handle_single_group_error(error: Errors) -> ApiResult<CoordinatorKey, GroupOffsets> {
        let handler = ListConsumerGroupOffsetsHandler::new(single_group_spec(), false, log_context());
        handler.handle_response(
            &node(),
            &coordinator_keys(&[GROUP0]),
            &build_response(vec![group_with_error(GROUP0, error)]),
        )
    }

    fn partition(index: i32, offset: i64, error: Errors) -> OffsetFetchResponsePartitions {
        let mut p = OffsetFetchResponsePartitions::new();
        p.set_partition_index(index)
            .set_committed_offset(offset)
            .set_error_code(error.code());
        p
    }

    fn topic(name: &str, partitions: Vec<OffsetFetchResponsePartitions>) -> OffsetFetchResponseTopics {
        let mut t = OffsetFetchResponseTopics::new();
        t.set_name(name.to_string()).set_partitions(partitions);
        t
    }

    fn oam(offset: i64) -> OffsetAndMetadata {
        OffsetAndMetadata::new(offset).unwrap()
    }

    fn assert_completed(result: &ApiResult<CoordinatorKey, GroupOffsets>, group: &str, expected: &GroupOffsets) {
        let key = CoordinatorKey::by_group_id(group);
        assert!(result.failed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
        assert_eq!(result.completed_keys.get(&key), Some(expected));
    }

    fn assert_unmapped(result: &ApiResult<CoordinatorKey, GroupOffsets>, groups: &[&str]) {
        assert!(result.completed_keys.is_empty());
        assert!(result.failed_keys.is_empty());
        assert_eq!(
            result.unmapped_keys.iter().cloned().collect::<HashSet<_>>(),
            coordinator_keys(groups)
        );
    }

    fn assert_retriable(result: &ApiResult<CoordinatorKey, GroupOffsets>) {
        assert!(result.completed_keys.is_empty());
        assert!(result.failed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
    }

    fn assert_failed(result: &ApiResult<CoordinatorKey, GroupOffsets>, group: &str, error: Errors) {
        let key = CoordinatorKey::by_group_id(group);
        assert!(result.completed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
        assert_eq!(result.failed_keys.get(&key).unwrap().error(), error);
    }

    /// Translated from `testSuccessfulHandleResponse`.
    #[test]
    fn test_successful_handle_response() {
        assert_completed(&handle_single_group_error(Errors::None), GROUP0, &HashMap::new());
    }

    /// Translated from `testSuccessfulHandleResponseWithOnePartitionError`.
    #[test]
    fn test_successful_handle_response_with_one_partition_error() {
        for error in [
            Errors::UnknownTopicOrPartition,
            Errors::TopicAuthorizationFailed,
            Errors::UnstableOffsetCommit,
        ] {
            let handler = ListConsumerGroupOffsetsHandler::new(single_group_spec(), false, log_context());
            let mut group = OffsetFetchResponseGroup::new();
            group.set_group_id(GROUP0.to_string()).set_topics(vec![topic(
                "t0",
                vec![partition(0, 10, Errors::None), partition(1, 10, error)],
            )]);
            let result = handler.handle_response(&node(), &coordinator_keys(&[GROUP0]), &build_response(vec![group]));
            assert_completed(&result, GROUP0, &HashMap::from([(tp("t0", 0), Some(oam(10)))]));
        }
    }

    /// Translated from `testSuccessfulHandleResponseWithOnePartitionErrorWithMultipleGroups`.
    #[test]
    fn test_successful_handle_response_with_one_partition_error_with_multiple_groups() {
        for error in [
            Errors::UnknownTopicOrPartition,
            Errors::TopicAuthorizationFailed,
            Errors::UnstableOffsetCommit,
        ] {
            let handler = ListConsumerGroupOffsetsHandler::new(multi_group_specs(), false, log_context());

            let mut g0 = OffsetFetchResponseGroup::new();
            g0.set_group_id(GROUP0.to_string())
                .set_topics(vec![topic("t0", vec![partition(0, 10, Errors::None)])]);
            let mut g1 = OffsetFetchResponseGroup::new();
            g1.set_group_id(GROUP1.to_string()).set_topics(vec![
                topic("t0", vec![partition(0, 10, error)]),
                topic("t1", vec![partition(0, 10, error), partition(1, 10, Errors::None)]),
            ]);
            let mut g2 = OffsetFetchResponseGroup::new();
            g2.set_group_id(GROUP2.to_string()).set_topics(vec![
                topic("t0", vec![partition(0, 10, error)]),
                topic("t1", vec![partition(0, 10, error), partition(1, 10, error)]),
                topic(
                    "t2",
                    vec![
                        partition(0, 10, error),
                        partition(1, 10, error),
                        partition(2, 10, Errors::None),
                    ],
                ),
            ]);

            let result = handler.handle_response(
                &node(),
                &coordinator_keys(&[GROUP0, GROUP1, GROUP2]),
                &build_response(vec![g0, g1, g2]),
            );
            assert_completed(&result, GROUP0, &HashMap::from([(tp("t0", 0), Some(oam(10)))]));
            assert_completed(&result, GROUP1, &HashMap::from([(tp("t1", 1), Some(oam(10)))]));
            assert_completed(&result, GROUP2, &HashMap::from([(tp("t2", 2), Some(oam(10)))]));
        }
    }

    /// Translated from `testSuccessfulHandleResponseWithMultipleGroups`.
    #[test]
    fn test_successful_handle_response_with_multiple_groups() {
        let handler = ListConsumerGroupOffsetsHandler::new(multi_group_specs(), false, log_context());
        let groups = vec![
            group_with_error(GROUP0, Errors::None),
            group_with_error(GROUP1, Errors::None),
            group_with_error(GROUP2, Errors::None),
        ];
        let result =
            handler.handle_response(&node(), &coordinator_keys(&[GROUP0, GROUP1, GROUP2]), &build_response(groups));
        for group in [GROUP0, GROUP1, GROUP2] {
            assert_completed(&result, group, &HashMap::new());
        }
    }

    /// Translated from `testUnmappedHandleResponse`.
    #[test]
    fn test_unmapped_handle_response() {
        assert_unmapped(&handle_single_group_error(Errors::CoordinatorNotAvailable), &[GROUP0]);
        assert_unmapped(&handle_single_group_error(Errors::NotCoordinator), &[GROUP0]);
    }

    /// Translated from `testUnmappedHandleResponseWithMultipleGroups`.
    #[test]
    fn test_unmapped_handle_response_with_multiple_groups() {
        let handler = ListConsumerGroupOffsetsHandler::new(multi_group_specs(), false, log_context());
        let groups = vec![
            group_with_error(GROUP0, Errors::NotCoordinator),
            group_with_error(GROUP1, Errors::CoordinatorNotAvailable),
            group_with_error(GROUP2, Errors::NotCoordinator),
        ];
        let result =
            handler.handle_response(&node(), &coordinator_keys(&[GROUP0, GROUP1, GROUP2]), &build_response(groups));
        assert_unmapped(&result, &[GROUP0, GROUP1, GROUP2]);
    }

    /// Translated from `testRetriableHandleResponse`.
    #[test]
    fn test_retriable_handle_response() {
        assert_retriable(&handle_single_group_error(Errors::CoordinatorLoadInProgress));
    }

    /// Translated from `testRetriableHandleResponseWithMultipleGroups`.
    #[test]
    fn test_retriable_handle_response_with_multiple_groups() {
        let handler = ListConsumerGroupOffsetsHandler::new(multi_group_specs(), false, log_context());
        let groups = vec![
            group_with_error(GROUP0, Errors::CoordinatorLoadInProgress),
            group_with_error(GROUP1, Errors::CoordinatorLoadInProgress),
            group_with_error(GROUP2, Errors::CoordinatorLoadInProgress),
        ];
        let result =
            handler.handle_response(&node(), &coordinator_keys(&[GROUP0, GROUP1, GROUP2]), &build_response(groups));
        assert_retriable(&result);
    }

    /// Translated from `testFailedHandleResponse`.
    #[test]
    fn test_failed_handle_response() {
        assert_failed(
            &handle_single_group_error(Errors::GroupAuthorizationFailed),
            GROUP0,
            Errors::GroupAuthorizationFailed,
        );
        assert_failed(
            &handle_single_group_error(Errors::GroupIdNotFound),
            GROUP0,
            Errors::GroupIdNotFound,
        );
        assert_failed(
            &handle_single_group_error(Errors::InvalidGroupId),
            GROUP0,
            Errors::InvalidGroupId,
        );
    }

    /// Translated from `testFailedHandleResponseWithMultipleGroups`.
    #[test]
    fn test_failed_handle_response_with_multiple_groups() {
        let handler = ListConsumerGroupOffsetsHandler::new(multi_group_specs(), false, log_context());
        let groups = vec![
            group_with_error(GROUP0, Errors::GroupAuthorizationFailed),
            group_with_error(GROUP1, Errors::GroupIdNotFound),
            group_with_error(GROUP2, Errors::InvalidGroupId),
        ];
        let result =
            handler.handle_response(&node(), &coordinator_keys(&[GROUP0, GROUP1, GROUP2]), &build_response(groups));
        assert_failed(&result, GROUP0, Errors::GroupAuthorizationFailed);
        assert_failed(&result, GROUP1, Errors::GroupIdNotFound);
        assert_failed(&result, GROUP2, Errors::InvalidGroupId);
    }
}
