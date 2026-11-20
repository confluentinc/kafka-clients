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

//! The `listShareGroupOffsets` admin API handler.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.ListShareGroupOffsetsHandler`, an
//! `AdminApiHandler.Batched`: one `DescribeShareGroupOffsets` request per
//! coordinator for all its groups.

use std::collections::{HashMap, HashSet};

use crate::DescribeShareGroupOffsetsRequestData;
use crate::admin::list_share_group_offsets_result::ShareGroupOffsets;
use crate::admin::{ListShareGroupOffsetsSpec, SharePartitionOffsetInfo};
use crate::common::protocol::Errors;
use crate::common::requests::{
    ConcreteResponse, CoordinatorType, RequestBuilder, describe_share_group_offsets_request,
};
use crate::common::utils::internals::LogContext;
use crate::common::{Error, Node, TopicPartition};
use crate::describe_share_group_offsets_request_data::{
    DescribeShareGroupOffsetsRequestGroup, DescribeShareGroupOffsetsRequestTopic,
};
use crate::{kafka_debug, kafka_error, kafka_warn};

use super::AdminApiLookupStrategy;
use super::CoordinatorKey;
use super::CoordinatorStrategy;
use super::SimpleAdminApiFuture;
use super::{AdminApiHandler, ApiResult, RequestAndKeys};

/// The `listShareGroupOffsets` handler.
///
/// Corresponds to `ListShareGroupOffsetsHandler`.
#[doc(alias = "org.apache.kafka.clients.admin.internals.ListShareGroupOffsetsHandler")]
pub(crate) struct ListShareGroupOffsetsHandler {
    group_specs: HashMap<String, ListShareGroupOffsetsSpec>,
    log_context: LogContext,
    lookup_strategy: CoordinatorStrategy,
}

impl ListShareGroupOffsetsHandler {
    /// Creates a handler.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.ListShareGroupOffsetsHandler#ListShareGroupOffsetsHandler")]
    pub(crate) fn new(group_specs: HashMap<String, ListShareGroupOffsetsSpec>, log_context: LogContext) -> Self {
        Self {
            group_specs,
            lookup_strategy: CoordinatorStrategy::new(CoordinatorType::Group, log_context.clone()),
            log_context,
        }
    }

    /// Creates the future bundle for the given group ids.
    ///
    /// Mirrors `ListShareGroupOffsetsHandler.newFuture`.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.ListShareGroupOffsetsHandler#newFuture")]
    pub(crate) fn new_future(group_ids: &[String]) -> SimpleAdminApiFuture<CoordinatorKey, ShareGroupOffsets> {
        SimpleAdminApiFuture::for_keys(Self::coordinator_keys(group_ids.iter()))
    }

    #[doc(alias = "org.apache.kafka.clients.admin.internals.ListShareGroupOffsetsHandler#coordinatorKeys")]
    fn coordinator_keys<'a>(group_ids: impl Iterator<Item = &'a String>) -> HashSet<CoordinatorKey> {
        group_ids.map(CoordinatorKey::by_group_id).collect()
    }

    /// Mirrors `validateKeys`: every key must be one of the groups this handler
    /// lists.
    ///
    /// Java throws `IllegalArgumentException`; `handle_response` turns the `Err`
    /// into a failure of every key, as Java's `KafkaAdminClient` does when
    /// `handleResponse` throws (see `ApiResult::failed_all`).
    #[doc(alias = "org.apache.kafka.clients.admin.internals.ListShareGroupOffsetsHandler#validateKeys")]
    fn validate_keys(&self, group_ids: &HashSet<CoordinatorKey>) -> Result<(), Error> {
        let keys = Self::coordinator_keys(self.group_specs.keys());
        if group_ids.is_subset(&keys) {
            return Ok(());
        }
        // Java prints the sets with `Set.toString()`: `[k1, k2]`.
        let to_string =
            |set: &HashSet<CoordinatorKey>| set.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ");
        Err(Error::local_illegal_argument(format!(
            "Received unexpected group ids [{}] (expected one of [{}])",
            to_string(group_ids),
            to_string(&keys)
        )))
    }

    /// Builds the single batched `DescribeShareGroupOffsets` request for all keys.
    ///
    /// Mirrors `ListShareGroupOffsetsHandler.buildBatchedRequest`.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.ListShareGroupOffsetsHandler#buildBatchedRequest")]
    pub(crate) fn build_batched_request(
        &self,
        _coordinator_id: i32,
        keys: &HashSet<CoordinatorKey>,
    ) -> describe_share_group_offsets_request::Builder {
        // Java throws `IllegalArgumentException` from `validateKeys` here. The keys
        // come from `new_future` over this handler's own specs, so this cannot fire;
        // `build_request` has no error channel, as in the sibling
        // `AlterConsumerGroupOffsetsHandler::build_batched_request`.
        if let Err(error) = self.validate_keys(keys) {
            panic!("{}", error.message());
        }

        let groups = keys
            .iter()
            .map(|coordinator_key| {
                let group_id = &coordinator_key.id_value;
                let mut request_group = DescribeShareGroupOffsetsRequestGroup::new();
                request_group.set_group_id(group_id.clone());
                // A missing spec cannot happen after `validate_keys`; Java would
                // dereference `null` here.
                let topic_partitions = self
                    .group_specs
                    .get(group_id)
                    .and_then(ListShareGroupOffsetsSpec::topic_partitions);
                match topic_partitions {
                    Some(topic_partitions) => {
                        // Java groups the partitions by topic through a `HashMap`, so the
                        // topic order is unspecified; Rust keeps first-seen order. (Java
                        // also builds a second, unused `topicPartitionMap`.)
                        let mut request_topics: Vec<DescribeShareGroupOffsetsRequestTopic> = Vec::new();
                        let mut topic_index: HashMap<&str, usize> = HashMap::new();
                        for tp in topic_partitions {
                            let index = *topic_index.entry(tp.topic()).or_insert_with(|| {
                                let mut topic = DescribeShareGroupOffsetsRequestTopic::new();
                                topic.set_topic_name(tp.topic().to_string()).set_partitions(Vec::new());
                                request_topics.push(topic);
                                request_topics.len() - 1
                            });
                            request_topics[index].partitions.push(tp.partition());
                        }
                        request_group.set_topics(Some(request_topics));
                    },
                    None => {
                        request_group.set_topics(None);
                    },
                }
                request_group
            })
            .collect();
        let mut data = DescribeShareGroupOffsetsRequestData::new();
        data.set_groups(groups);
        describe_share_group_offsets_request::Builder::new(data)
    }

    #[doc(alias = "org.apache.kafka.clients.admin.internals.ListShareGroupOffsetsHandler#handleGroupError")]
    fn handle_group_error(
        &self,
        group_id: &CoordinatorKey,
        error: &Error,
        failed: &mut HashMap<CoordinatorKey, Error>,
        groups_to_unmap: &mut Vec<CoordinatorKey>,
    ) {
        // Java's `Errors.forException(exception)`.
        let code = error.error();
        match code {
            Errors::GroupAuthorizationFailed | Errors::UnknownMemberId | Errors::StaleMemberEpoch => {
                kafka_debug!(
                    self.log_context,
                    "`DescribeShareGroupOffsets` request for group id {} failed due to error {}",
                    group_id.id_value,
                    code
                );
                failed.insert(group_id.clone(), error.clone());
            },
            Errors::CoordinatorLoadInProgress => {
                // If the coordinator is in the middle of loading, then we just need to retry
                kafka_debug!(
                    self.log_context,
                    "`DescribeShareGroupOffsets` request for group id {} failed because the coordinator is still \
                     in the process of loading state. Will retry",
                    group_id.id_value
                );
            },
            Errors::CoordinatorNotAvailable | Errors::NotCoordinator => {
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                kafka_debug!(
                    self.log_context,
                    "`DescribeShareGroupOffsets` request for group id {} returned error {}. Will attempt to find \
                     the coordinator again and retry",
                    group_id.id_value,
                    code
                );
                groups_to_unmap.push(group_id.clone());
            },
            other => {
                kafka_error!(
                    self.log_context,
                    "`DescribeShareGroupOffsets` request for group id {} failed due to unexpected error {}",
                    group_id.id_value,
                    other
                );
                failed.insert(group_id.clone(), error.clone());
            },
        }
    }
}

impl AdminApiHandler<CoordinatorKey, ShareGroupOffsets> for ListShareGroupOffsetsHandler {
    fn api_name(&self) -> &str {
        "describeShareGroupOffsets"
    }

    fn build_request(&self, broker_id: i32, keys: &HashSet<CoordinatorKey>) -> Vec<RequestAndKeys<CoordinatorKey>> {
        vec![RequestAndKeys {
            request: Box::new(self.build_batched_request(broker_id, keys)) as Box<dyn RequestBuilder>,
            keys: keys.clone(),
        }]
    }

    fn handle_response(
        &self,
        _coordinator: &Node,
        group_ids: &HashSet<CoordinatorKey>,
        response: &ConcreteResponse,
    ) -> ApiResult<CoordinatorKey, ShareGroupOffsets> {
        if let Err(error) = self.validate_keys(group_ids) {
            return ApiResult::failed_all(group_ids, error);
        }
        let ConcreteResponse::DescribeShareGroupOffsets(response) = response else {
            // `org.apache.kafka.clients.admin.KafkaAdminClient.AdminClientRunnable#handleResponses`
            // fails this one call on a response-type mismatch; see
            // `ApiResult::failed_all`.
            return ApiResult::failed_all(
                group_ids,
                Error::local_illegal_state("ListShareGroupOffsetsHandler received an unexpected response type"),
            );
        };

        let mut completed = HashMap::new();
        let mut failed = HashMap::new();
        let mut unmapped = Vec::new();

        for coordinator_key in group_ids {
            let group_id = &coordinator_key.id_value;
            if let Some(group_error) = response.group_error(group_id) {
                self.handle_group_error(coordinator_key, group_error, &mut failed, &mut unmapped);
                continue;
            }
            let mut group_offsets_listing: ShareGroupOffsets = HashMap::new();
            for group_response in response.data().groups.iter().filter(|g| &g.group_id == group_id) {
                for topic_response in &group_response.topics {
                    for partition_response in &topic_response.partitions {
                        let tp =
                            TopicPartition::new(topic_response.topic_name.clone(), partition_response.partition_index);
                        if partition_response.error_code == Errors::None.code() {
                            let start_offset = partition_response.start_offset;
                            let leader_epoch =
                                (partition_response.leader_epoch >= 0).then_some(partition_response.leader_epoch);
                            let lag = (partition_response.lag >= 0).then_some(partition_response.lag);
                            // Negative offset indicates there is no start offset for this partition
                            if start_offset < 0 {
                                group_offsets_listing.insert(tp, None);
                            } else {
                                group_offsets_listing
                                    .insert(tp, Some(SharePartitionOffsetInfo::new(start_offset, leader_epoch, lag)));
                            }
                        } else {
                            kafka_warn!(
                                self.log_context,
                                "Skipping return offset for {} due to error {}: {}.",
                                tp,
                                partition_response.error_code,
                                partition_response.error_message.as_deref().unwrap_or("null")
                            );
                        }
                    }
                }
            }
            completed.insert(coordinator_key.clone(), group_offsets_listing);
        }
        ApiResult::new(completed, failed, unmapped)
    }

    fn lookup_strategy(&self) -> &dyn AdminApiLookupStrategy<CoordinatorKey> {
        &self.lookup_strategy
    }
}

#[cfg(test)]
mod tests {
    //! Java has no `ListShareGroupOffsetsHandlerTest`; these Rust tests pin the
    //! handler's `handleGroupError` branches and the request and response
    //! conversions, which `KafkaAdminClientShareGroupTest` exercises only end
    //! to end.

    use super::*;
    use crate::DescribeShareGroupOffsetsResponseData;
    use crate::common::requests::{AbstractRequest, DescribeShareGroupOffsetsResponse};
    use crate::describe_share_group_offsets_response_data::{
        DescribeShareGroupOffsetsResponseGroup, DescribeShareGroupOffsetsResponsePartition,
        DescribeShareGroupOffsetsResponseTopic,
    };

    const GROUP_ID: &str = "group-id";

    fn key() -> CoordinatorKey {
        CoordinatorKey::by_group_id(GROUP_ID)
    }

    fn keys() -> HashSet<CoordinatorKey> {
        HashSet::from([key()])
    }

    fn node() -> Node {
        Node::new(1, "host".to_string(), 1234)
    }

    fn handler(spec: ListShareGroupOffsetsSpec) -> ListShareGroupOffsetsHandler {
        ListShareGroupOffsetsHandler::new(HashMap::from([(GROUP_ID.to_string(), spec)]), LogContext::new(String::new()))
    }

    fn partition(
        index: i32,
        start_offset: i64,
        leader_epoch: i32,
        lag: i64,
    ) -> DescribeShareGroupOffsetsResponsePartition {
        let mut partition = DescribeShareGroupOffsetsResponsePartition::new();
        partition
            .set_partition_index(index)
            .set_start_offset(start_offset)
            .set_leader_epoch(leader_epoch)
            .set_lag(lag);
        partition
    }

    fn response(groups: Vec<DescribeShareGroupOffsetsResponseGroup>) -> ConcreteResponse {
        let mut data = DescribeShareGroupOffsetsResponseData::new();
        data.set_groups(groups);
        ConcreteResponse::DescribeShareGroupOffsets(DescribeShareGroupOffsetsResponse::new(data))
    }

    fn group_error(error: Errors) -> ConcreteResponse {
        let mut group = DescribeShareGroupOffsetsResponseGroup::new();
        group
            .set_group_id(GROUP_ID.to_string())
            .set_error_code(error.code())
            .set_error_message(Some("group failed".to_string()));
        response(vec![group])
    }

    fn request_group(request: AbstractRequest) -> DescribeShareGroupOffsetsRequestGroup {
        match request {
            AbstractRequest::DescribeShareGroupOffsets(request) => {
                assert_eq!(request.data().groups.len(), 1);
                request.data().groups[0].clone()
            },
            other => panic!("expected DescribeShareGroupOffsets, got {}", other.api_key().name()),
        }
    }

    #[test]
    fn build_request_without_partitions_asks_for_every_topic() {
        let mut builder = handler(ListShareGroupOffsetsSpec::new()).build_batched_request(1, &keys());
        let group = request_group(builder.build().unwrap());
        assert_eq!(group.group_id, GROUP_ID);
        assert_eq!(group.topics, None);
    }

    #[test]
    fn build_request_groups_the_partitions_by_topic() {
        let spec = ListShareGroupOffsetsSpec::new().set_topic_partitions(Some(vec![
            TopicPartition::new("b", 1),
            TopicPartition::new("a", 0),
            TopicPartition::new("b", 0),
        ]));
        let mut builder = handler(spec).build_batched_request(1, &keys());
        let group = request_group(builder.build().unwrap());
        let topics = group.topics.expect("topics");
        let topics: Vec<(String, Vec<i32>)> =
            topics.iter().map(|t| (t.topic_name.clone(), t.partitions.clone())).collect();
        assert_eq!(topics, vec![("b".to_string(), vec![1, 0]), ("a".to_string(), vec![0])]);
    }

    #[test]
    fn response_is_converted_skipping_failed_partitions() {
        let mut failed = partition(2, 30, 0, 0);
        failed.set_error_code(Errors::NotCoordinator.code());
        let mut topic = DescribeShareGroupOffsetsResponseTopic::new();
        topic.set_topic_name("t".to_string()).set_partitions(vec![
            partition(0, 10, 1, 5),
            partition(1, -1, -1, -1),
            failed,
            partition(3, 40, -1, -1),
        ]);
        let mut group = DescribeShareGroupOffsetsResponseGroup::new();
        group.set_group_id(GROUP_ID.to_string()).set_topics(vec![topic]);

        let result =
            handler(ListShareGroupOffsetsSpec::new()).handle_response(&node(), &keys(), &response(vec![group]));
        assert!(result.failed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
        assert_eq!(
            result.completed_keys[&key()],
            HashMap::from([
                (
                    TopicPartition::new("t", 0),
                    Some(SharePartitionOffsetInfo::new(10, Some(1), Some(5)))
                ),
                (TopicPartition::new("t", 1), None),
                (TopicPartition::new("t", 3), Some(SharePartitionOffsetInfo::new(40, None, None))),
            ])
        );
    }

    #[test]
    fn group_errors_fail_retry_or_unmap() {
        for error in [
            Errors::GroupAuthorizationFailed,
            Errors::UnknownMemberId,
            Errors::StaleMemberEpoch,
            Errors::InvalidRequest,
        ] {
            let result =
                handler(ListShareGroupOffsetsSpec::new()).handle_response(&node(), &keys(), &group_error(error));
            assert!(result.completed_keys.is_empty());
            assert!(result.unmapped_keys.is_empty());
            let failure = &result.failed_keys[&key()];
            assert_eq!(failure.error(), error);
            assert_eq!(failure.message(), "group failed");
        }
        let result = handler(ListShareGroupOffsetsSpec::new()).handle_response(
            &node(),
            &keys(),
            &group_error(Errors::CoordinatorLoadInProgress),
        );
        assert!(result.completed_keys.is_empty());
        assert!(result.failed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
        for error in [Errors::CoordinatorNotAvailable, Errors::NotCoordinator] {
            let result =
                handler(ListShareGroupOffsetsSpec::new()).handle_response(&node(), &keys(), &group_error(error));
            assert!(result.completed_keys.is_empty());
            assert!(result.failed_keys.is_empty());
            assert_eq!(result.unmapped_keys, vec![key()], "{error}");
        }
    }

    #[test]
    fn unexpected_keys_fail_with_java_message() {
        let other = HashSet::from([CoordinatorKey::by_group_id("other")]);
        let result = handler(ListShareGroupOffsetsSpec::new()).handle_response(&node(), &other, &response(Vec::new()));
        let failure = &result.failed_keys[&CoordinatorKey::by_group_id("other")];
        assert!(failure.is_local_illegal_argument_error(), "{failure:?}");
        assert_eq!(
            failure.message(),
            "Received unexpected group ids [CoordinatorKey(idValue='other', type=GROUP)] (expected one of \
             [CoordinatorKey(idValue='group-id', type=GROUP)])"
        );
    }
}
