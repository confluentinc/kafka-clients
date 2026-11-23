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

//! The `describeShareGroups` admin API handler.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.DescribeShareGroupsHandler`, an
//! `AdminApiHandler.Batched`: one `ShareGroupDescribe` request per coordinator
//! for all its keys.

use std::collections::{HashMap, HashSet};

use crate::ShareGroupDescribeRequestData;
use crate::admin::internals::AdminUtils;
use crate::admin::{ShareGroupDescription, ShareMemberAssignment, ShareMemberDescription};
use crate::common::protocol::Errors;
use crate::common::requests::{ConcreteResponse, CoordinatorType, RequestBuilder, share_group_describe_request};
use crate::common::utils::internals::LogContext;
use crate::common::{Error, GroupState, Node, TopicPartition};
use crate::share_group_describe_response_data::{Assignment, DescribedGroup};
use crate::{kafka_debug, kafka_error};

use super::AdminApiLookupStrategy;
use super::CoordinatorKey;
use super::CoordinatorStrategy;
use super::SimpleAdminApiFuture;
use super::{AdminApiHandler, ApiResult, RequestAndKeys};

/// The `describeShareGroups` handler.
///
/// Corresponds to `DescribeShareGroupsHandler`.
#[doc(alias = "org.apache.kafka.clients.admin.internals.DescribeShareGroupsHandler")]
pub(crate) struct DescribeShareGroupsHandler {
    include_authorized_operations: bool,
    log_context: LogContext,
    lookup_strategy: CoordinatorStrategy,
}

impl DescribeShareGroupsHandler {
    /// Creates a handler.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.DescribeShareGroupsHandler#DescribeShareGroupsHandler")]
    pub(crate) fn new(include_authorized_operations: bool, log_context: LogContext) -> Self {
        Self {
            include_authorized_operations,
            lookup_strategy: CoordinatorStrategy::new(CoordinatorType::Group, log_context.clone()),
            log_context,
        }
    }

    #[doc(alias = "org.apache.kafka.clients.admin.internals.DescribeShareGroupsHandler#buildKeySet")]
    fn build_key_set(group_ids: &[String]) -> HashSet<CoordinatorKey> {
        group_ids.iter().map(CoordinatorKey::by_group_id).collect()
    }

    /// Creates the future bundle for the given group ids.
    ///
    /// Mirrors `DescribeShareGroupsHandler.newFuture`.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.DescribeShareGroupsHandler#newFuture")]
    pub(crate) fn new_future(group_ids: &[String]) -> SimpleAdminApiFuture<CoordinatorKey, ShareGroupDescription> {
        SimpleAdminApiFuture::for_keys(Self::build_key_set(group_ids))
    }

    /// Builds the single batched `ShareGroupDescribe` request for all keys.
    ///
    /// Mirrors `DescribeShareGroupsHandler.buildBatchedRequest`.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.DescribeShareGroupsHandler#buildBatchedRequest")]
    pub(crate) fn build_batched_request(
        &self,
        _coordinator_id: i32,
        keys: &HashSet<CoordinatorKey>,
    ) -> share_group_describe_request::Builder {
        let group_ids: Vec<String> = keys
            .iter()
            .map(|key| {
                // Java throws `IllegalArgumentException` here. The keys come from
                // `new_future`, which only builds group keys, so this cannot fire;
                // `build_request` has no error channel, as in the sibling
                // `DescribeClassicGroupsHandler::build_batched_request`.
                assert!(
                    key.coordinator_type == CoordinatorType::Group,
                    "Invalid group coordinator key {key} when building `DescribeShareGroups` request"
                );
                key.id_value.clone()
            })
            .collect();
        let mut data = ShareGroupDescribeRequestData::new();
        data.set_group_ids(group_ids);
        data.set_include_authorized_operations(self.include_authorized_operations);
        share_group_describe_request::Builder::new(data)
    }

    #[doc(alias = "org.apache.kafka.clients.admin.internals.DescribeShareGroupsHandler#convertAssignment")]
    fn convert_assignment(assignment: &Assignment) -> HashSet<TopicPartition> {
        assignment
            .topic_partitions
            .iter()
            .flat_map(|topic| {
                topic
                    .partitions
                    .iter()
                    .map(|partition| TopicPartition::new(topic.topic_name.clone(), *partition))
            })
            .collect()
    }

    #[doc(alias = "org.apache.kafka.clients.admin.internals.DescribeShareGroupsHandler#handleError")]
    fn handle_error(
        &self,
        group_id: &CoordinatorKey,
        described_group: &DescribedGroup,
        error: Errors,
        failed: &mut HashMap<CoordinatorKey, Error>,
        groups_to_unmap: &mut HashSet<CoordinatorKey>,
    ) {
        let error_msg = described_group.error_message.as_deref();
        match error {
            Errors::GroupAuthorizationFailed | Errors::TopicAuthorizationFailed => {
                kafka_debug!(
                    self.log_context,
                    "`DescribeShareGroups` request for group id {} failed due to error {}",
                    group_id.id_value,
                    error
                );
                // The topic auth response received on DescribeShareGroup is a generic one not
                // including topic names, so we just pass it on unchanged here.
                failed.insert(group_id.clone(), error.error_with_optional_message(error_msg));
            },
            Errors::CoordinatorLoadInProgress => {
                // If the coordinator is in the middle of loading, then we just need to retry
                kafka_debug!(
                    self.log_context,
                    "`DescribeShareGroups` request for group id {} failed because the coordinator is still in \
                     the process of loading state. Will retry",
                    group_id.id_value
                );
            },
            Errors::CoordinatorNotAvailable | Errors::NotCoordinator => {
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                kafka_debug!(
                    self.log_context,
                    "`DescribeShareGroups` request for group id {} returned error {}. Will attempt to find the \
                     coordinator again and retry",
                    group_id.id_value,
                    error
                );
                groups_to_unmap.insert(group_id.clone());
            },
            Errors::GroupIdNotFound => {
                kafka_debug!(
                    self.log_context,
                    "`DescribeShareGroups` request for group id {} failed because the group does not exist. {}",
                    group_id.id_value,
                    error_msg.unwrap_or("")
                );
                failed.insert(group_id.clone(), error.error_with_optional_message(error_msg));
            },
            other => {
                kafka_error!(
                    self.log_context,
                    "`DescribeShareGroups` request for group id {} failed due to unexpected error {}",
                    group_id.id_value,
                    other
                );
                failed.insert(group_id.clone(), other.error_with_optional_message(error_msg));
            },
        }
    }
}

impl AdminApiHandler<CoordinatorKey, ShareGroupDescription> for DescribeShareGroupsHandler {
    fn api_name(&self) -> &str {
        "describeShareGroups"
    }

    fn build_request(&self, broker_id: i32, keys: &HashSet<CoordinatorKey>) -> Vec<RequestAndKeys<CoordinatorKey>> {
        vec![RequestAndKeys {
            request: Box::new(self.build_batched_request(broker_id, keys)) as Box<dyn RequestBuilder>,
            keys: keys.clone(),
        }]
    }

    fn handle_response(
        &self,
        coordinator: &Node,
        keys: &HashSet<CoordinatorKey>,
        response: &ConcreteResponse,
    ) -> ApiResult<CoordinatorKey, ShareGroupDescription> {
        let ConcreteResponse::ShareGroupDescribe(response) = response else {
            // `org.apache.kafka.clients.admin.KafkaAdminClient.AdminClientRunnable#handleResponses`
            // fails this one call on a response-type mismatch; see
            // `ApiResult::failed_all`.
            return ApiResult::failed_all(
                keys,
                Error::local_illegal_state("DescribeShareGroupsHandler received an unexpected response type"),
            );
        };
        let mut completed = HashMap::new();
        let mut failed = HashMap::new();
        let mut groups_to_unmap = HashSet::new();

        for described_group in &response.data().groups {
            let group_id_key = CoordinatorKey::by_group_id(described_group.group_id.clone());
            let error = Errors::for_code(described_group.error_code);
            if error != Errors::None {
                self.handle_error(&group_id_key, described_group, error, &mut failed, &mut groups_to_unmap);
                continue;
            }

            let authorized_operations = AdminUtils::valid_acl_operations(described_group.authorized_operations);
            let member_descriptions = described_group
                .members
                .iter()
                .map(|group_member| {
                    ShareMemberDescription::new(
                        group_member.member_id.clone(),
                        group_member.rack_id.clone(),
                        group_member.client_id.clone(),
                        group_member.client_host.clone(),
                        ShareMemberAssignment::new(Self::convert_assignment(&group_member.assignment)),
                        group_member.member_epoch,
                    )
                })
                .collect();

            let share_group_description = ShareGroupDescription::with_authorized_operations(
                group_id_key.id_value.clone(),
                member_descriptions,
                GroupState::parse(&described_group.group_state),
                Some(coordinator.clone()),
                described_group.group_epoch,
                described_group.assignment_epoch,
                authorized_operations,
            );
            completed.insert(group_id_key, share_group_description);
        }

        ApiResult::new(completed, failed, groups_to_unmap.into_iter().collect())
    }

    fn lookup_strategy(&self) -> &dyn AdminApiLookupStrategy<CoordinatorKey> {
        &self.lookup_strategy
    }
}

#[cfg(test)]
mod tests {
    //! Java has no `DescribeShareGroupsHandlerTest`; these Rust tests pin the
    //! handler's `handleError` branches and the response conversion, which
    //! `KafkaAdminClientShareGroupTest` exercises only end to end.

    use super::*;
    use crate::ShareGroupDescribeResponseData;
    use crate::common::acl::AclOperation;
    use crate::common::requests::{AbstractRequest, ShareGroupDescribeResponse};
    use crate::share_group_describe_response_data::{Member, TopicPartitions};
    use std::collections::BTreeSet;

    const GROUP_ID: &str = "group-id";

    fn handler() -> DescribeShareGroupsHandler {
        DescribeShareGroupsHandler::new(true, LogContext::new(String::new()))
    }

    fn key() -> CoordinatorKey {
        CoordinatorKey::by_group_id(GROUP_ID)
    }

    fn keys() -> HashSet<CoordinatorKey> {
        HashSet::from([key()])
    }

    fn node() -> Node {
        Node::new(1, "host".to_string(), 1234)
    }

    fn error_response(error: Errors, message: Option<&str>) -> ConcreteResponse {
        let mut group = DescribedGroup::new();
        group
            .set_group_id(GROUP_ID.to_string())
            .set_error_code(error.code())
            .set_error_message(message.map(str::to_string));
        let mut data = ShareGroupDescribeResponseData::new();
        data.set_groups(vec![group]);
        ConcreteResponse::ShareGroupDescribe(ShareGroupDescribeResponse::new(data))
    }

    #[test]
    fn build_request_carries_the_group_ids_and_the_operations_flag() {
        let keys = HashSet::from([key(), CoordinatorKey::by_group_id("other")]);
        match handler().build_batched_request(1, &keys).build().unwrap() {
            AbstractRequest::ShareGroupDescribe(request) => {
                let mut ids = request.data().group_ids.clone();
                ids.sort();
                assert_eq!(ids, vec![GROUP_ID.to_string(), "other".to_string()]);
                assert!(request.data().include_authorized_operations);
            },
            other => panic!("expected ShareGroupDescribe, got {}", other.api_key().name()),
        }
    }

    #[test]
    fn successful_response_is_converted() {
        let mut topic = TopicPartitions::new();
        topic.set_topic_name("t".to_string()).set_partitions(vec![0, 1]);
        let mut assignment = Assignment::new();
        assignment.set_topic_partitions(vec![topic]);
        let mut member = Member::new();
        member
            .set_member_id("m".to_string())
            .set_rack_id(Some("rack".to_string()))
            .set_client_id("c".to_string())
            .set_client_host("h".to_string())
            .set_member_epoch(7)
            .set_assignment(assignment);
        let mut group = DescribedGroup::new();
        group
            .set_group_id(GROUP_ID.to_string())
            .set_group_state("Stable".to_string())
            .set_group_epoch(4)
            .set_assignment_epoch(3)
            .set_authorized_operations(1 << AclOperation::Read.code())
            .set_members(vec![member]);
        let mut data = ShareGroupDescribeResponseData::new();
        data.set_groups(vec![group]);
        let response = ConcreteResponse::ShareGroupDescribe(ShareGroupDescribeResponse::new(data));

        let result = handler().handle_response(&node(), &keys(), &response);
        assert!(result.failed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
        let expected = ShareGroupDescription::with_authorized_operations(
            GROUP_ID,
            vec![ShareMemberDescription::new(
                "m",
                Some("rack".to_string()),
                "c",
                "h",
                ShareMemberAssignment::new(HashSet::from([TopicPartition::new("t", 0), TopicPartition::new("t", 1)])),
                7,
            )],
            GroupState::Stable,
            Some(node()),
            4,
            3,
            Some(BTreeSet::from([AclOperation::Read])),
        );
        assert_eq!(result.completed_keys.get(&key()), Some(&expected));
    }

    #[test]
    fn unmapped_on_coordinator_change() {
        for error in [Errors::NotCoordinator, Errors::CoordinatorNotAvailable] {
            let result = handler().handle_response(&node(), &keys(), &error_response(error, None));
            assert!(result.completed_keys.is_empty());
            assert!(result.failed_keys.is_empty());
            assert_eq!(result.unmapped_keys, vec![key()], "{error}");
        }
    }

    #[test]
    fn retried_while_the_coordinator_loads() {
        let result =
            handler().handle_response(&node(), &keys(), &error_response(Errors::CoordinatorLoadInProgress, None));
        assert!(result.completed_keys.is_empty());
        assert!(result.failed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
    }

    #[test]
    fn failed_with_the_broker_message() {
        for error in [
            Errors::GroupAuthorizationFailed,
            Errors::TopicAuthorizationFailed,
            Errors::GroupIdNotFound,
            Errors::InvalidRequest,
        ] {
            let result = handler().handle_response(&node(), &keys(), &error_response(error, Some("broker says no")));
            assert!(result.completed_keys.is_empty());
            assert!(result.unmapped_keys.is_empty());
            let failure = &result.failed_keys[&key()];
            assert_eq!(failure.error(), error);
            assert_eq!(failure.message(), "broker says no");
        }
    }

    #[test]
    fn unexpected_response_type_fails_every_key() {
        let response = ConcreteResponse::DeleteGroups(crate::common::requests::DeleteGroupsResponse::new(
            crate::DeleteGroupsResponseData::new(),
        ));
        let result = handler().handle_response(&node(), &keys(), &response);
        let failure = &result.failed_keys[&key()];
        assert!(failure.is_local_illegal_state_error(), "{failure:?}");
        assert_eq!(
            failure.message(),
            "DescribeShareGroupsHandler received an unexpected response type"
        );
    }
}
