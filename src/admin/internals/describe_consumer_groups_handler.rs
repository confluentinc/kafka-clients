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

//! The `describeConsumerGroups` admin API handler.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.DescribeConsumerGroupsHandler`.
//!
//! Tries the KIP-848 `ConsumerGroupDescribe` API first and falls back per-group
//! to the classic `DescribeGroups` API on `UNSUPPORTED_VERSION` /
//! `GROUP_ID_NOT_FOUND`.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use crate::admin::internals::admin_utils::valid_acl_operations;
use crate::admin::{ConsumerGroupDescription, MemberAssignment, MemberDescription};
use crate::common::protocol::Errors;
use crate::common::requests::{
    ConcreteResponse, ConsumerGroupDescribeRequestBuilder, CoordinatorType, DescribeGroupsRequestBuilder,
    RequestBuilder,
};
use crate::common::utils::LogContext;
use crate::common::{Error, GroupState, GroupType, Node, TopicPartition};
use crate::consumer::internals::consumer_protocol::{ConsumerProtocol, PROTOCOL_TYPE};
use crate::consumer_group_describe_request_data::ConsumerGroupDescribeRequestData;
use crate::consumer_group_describe_response_data::Assignment as WireAssignment;
use crate::describe_groups_request_data::DescribeGroupsRequestData;
use crate::{kafka_debug, kafka_error};

use super::admin_api_future::SimpleAdminApiFuture;
use super::admin_api_handler::{AdminApiHandler, ApiResult, RequestAndKeys};
use super::admin_api_lookup_strategy::AdminApiLookupStrategy;
use super::coordinator_key::CoordinatorKey;
use super::coordinator_strategy::CoordinatorStrategy;

/// The `describeConsumerGroups` handler.
///
/// Corresponds to `DescribeConsumerGroupsHandler`.
pub(crate) struct DescribeConsumerGroupsHandler {
    include_authorized_operations: bool,
    log_context: LogContext,
    lookup_strategy: CoordinatorStrategy,
    /// Group ids that should use the classic `DescribeGroups` API (populated
    /// after a new-API `UNSUPPORTED_VERSION` / `GROUP_ID_NOT_FOUND`).
    use_classic_group_api: Mutex<HashSet<String>>,
    /// The more-informative `ConsumerGroupDescribe` `GROUP_ID_NOT_FOUND` error
    /// message, kept so it can override the classic API's message.
    group_id_not_found_error_messages: Mutex<HashMap<String, String>>,
}

impl DescribeConsumerGroupsHandler {
    /// Creates a handler.
    pub(crate) fn new(include_authorized_operations: bool, log_context: LogContext) -> Self {
        Self {
            include_authorized_operations,
            lookup_strategy: CoordinatorStrategy::new(CoordinatorType::Group, log_context.clone()),
            log_context,
            use_classic_group_api: Mutex::new(HashSet::new()),
            group_id_not_found_error_messages: Mutex::new(HashMap::new()),
        }
    }

    /// Builds the key set for a collection of group ids.
    fn build_key_set(group_ids: &[String]) -> HashSet<CoordinatorKey> {
        group_ids.iter().map(CoordinatorKey::by_group_id).collect()
    }

    /// Creates the future bundle for the given group ids.
    ///
    /// Mirrors `DescribeConsumerGroupsHandler.newFuture`.
    pub(crate) fn new_future(group_ids: &[String]) -> SimpleAdminApiFuture<CoordinatorKey, ConsumerGroupDescription> {
        SimpleAdminApiFuture::for_keys(Self::build_key_set(group_ids))
    }

    fn convert_assignment(assignment: &WireAssignment) -> HashSet<TopicPartition> {
        let mut partitions = HashSet::new();
        for topic in &assignment.topic_partitions {
            for partition in &topic.partitions {
                partitions.insert(TopicPartition::new(topic.topic_name.clone(), *partition));
            }
        }
        partitions
    }

    fn handle_consumer_group_response(
        &self,
        coordinator: &Node,
        response: &crate::consumer_group_describe_response_data::ConsumerGroupDescribeResponseData,
        completed: &mut HashMap<CoordinatorKey, ConsumerGroupDescription>,
        failed: &mut HashMap<CoordinatorKey, Error>,
        groups_to_unmap: &mut HashSet<CoordinatorKey>,
    ) {
        for described_group in &response.groups {
            let group_id_key = CoordinatorKey::by_group_id(described_group.group_id.clone());
            let error = Errors::for_code(described_group.error_code);
            if error != Errors::None {
                self.handle_error(
                    &group_id_key,
                    error,
                    described_group.error_message.as_deref(),
                    failed,
                    groups_to_unmap,
                    true,
                );
                continue;
            }

            let authorized_operations = valid_acl_operations(described_group.authorized_operations);
            let mut member_descriptions = Vec::with_capacity(described_group.members.len());
            for group_member in &described_group.members {
                let upgraded = match group_member.member_type {
                    -1 => None,
                    other => Some(other == 1),
                };
                member_descriptions.push(MemberDescription::new(
                    group_member.member_id.clone(),
                    group_member.instance_id.clone(),
                    group_member.rack_id.clone(),
                    group_member.client_id.clone(),
                    group_member.client_host.clone(),
                    MemberAssignment::new(Self::convert_assignment(&group_member.assignment)),
                    Some(MemberAssignment::new(Self::convert_assignment(&group_member.target_assignment))),
                    Some(group_member.member_epoch),
                    upgraded,
                ));
            }

            let description = ConsumerGroupDescription::new(
                group_id_key.id_value.clone(),
                false,
                member_descriptions,
                described_group.assignor_name.clone(),
                GroupType::Consumer,
                GroupState::parse(&described_group.group_state),
                Some(coordinator.clone()),
                authorized_operations,
                Some(described_group.group_epoch),
                Some(described_group.assignment_epoch),
            );
            completed.insert(group_id_key, description);
        }
    }

    fn handle_classic_group_response(
        &self,
        coordinator: &Node,
        response: &crate::describe_groups_response_data::DescribeGroupsResponseData,
        completed: &mut HashMap<CoordinatorKey, ConsumerGroupDescription>,
        failed: &mut HashMap<CoordinatorKey, Error>,
        groups_to_unmap: &mut HashSet<CoordinatorKey>,
    ) {
        for described_group in &response.groups {
            let group_id_key = CoordinatorKey::by_group_id(described_group.group_id.clone());
            let error = Errors::for_code(described_group.error_code);
            if error != Errors::None {
                self.handle_error(
                    &group_id_key,
                    error,
                    described_group.error_message.as_deref(),
                    failed,
                    groups_to_unmap,
                    false,
                );
                continue;
            }
            let protocol_type = &described_group.protocol_type;
            if protocol_type == PROTOCOL_TYPE || protocol_type.is_empty() {
                let authorized_operations = valid_acl_operations(described_group.authorized_operations);
                let mut member_descriptions = Vec::with_capacity(described_group.members.len());
                let mut deserialize_error = None;
                for group_member in &described_group.members {
                    let mut partitions = HashSet::new();
                    if !group_member.member_assignment.is_empty() {
                        // Only classic consumer groups carry a deserializable assignment.
                        match ConsumerProtocol::deserialize_assignment(&group_member.member_assignment) {
                            Ok(assignment) => partitions = assignment.partitions().iter().cloned().collect(),
                            Err(e) => {
                                deserialize_error = Some(e);
                                break;
                            },
                        }
                    }
                    member_descriptions.push(MemberDescription::new(
                        group_member.member_id.clone(),
                        group_member.group_instance_id.clone(),
                        None,
                        group_member.client_id.clone(),
                        group_member.client_host.clone(),
                        MemberAssignment::new(partitions),
                        None,
                        None,
                        None,
                    ));
                }
                // Java's `ConsumerProtocol.deserializeAssignment` throws a
                // SchemaException that escapes `handleResponse`; the Rust port
                // surfaces it as a per-group failure instead of panicking.
                if let Some(error) = deserialize_error {
                    failed.insert(group_id_key.clone(), error);
                    continue;
                }
                let description = ConsumerGroupDescription::new(
                    group_id_key.id_value.clone(),
                    protocol_type.is_empty(),
                    member_descriptions,
                    described_group.protocol_data.clone(),
                    GroupType::Classic,
                    GroupState::parse(&described_group.group_state),
                    Some(coordinator.clone()),
                    authorized_operations,
                    None,
                    None,
                );
                completed.insert(group_id_key, description);
            } else {
                failed.insert(
                    group_id_key.clone(),
                    Error::local_illegal_argument(format!(
                        "GroupId {} is not a consumer group ({}).",
                        group_id_key.id_value, protocol_type
                    )),
                );
            }
        }
    }

    fn handle_error(
        &self,
        group_id: &CoordinatorKey,
        error: Errors,
        error_msg: Option<&str>,
        failed: &mut HashMap<CoordinatorKey, Error>,
        groups_to_unmap: &mut HashSet<CoordinatorKey>,
        is_consumer_group_response: bool,
    ) {
        let api_name = if is_consumer_group_response {
            "ConsumerGroupDescribe"
        } else {
            "DescribeGroups"
        };
        match error {
            Errors::GroupAuthorizationFailed | Errors::TopicAuthorizationFailed => {
                kafka_debug!(
                    self.log_context,
                    "`{}` request for group id {} failed due to error {:?}.",
                    api_name,
                    group_id.id_value,
                    error
                );
                failed.insert(group_id.clone(), error_with_optional_message(error, error_msg));
            },
            Errors::CoordinatorLoadInProgress => {
                kafka_debug!(
                    self.log_context,
                    "`{}` request for group id {} failed because the coordinator is still loading. Will retry.",
                    api_name,
                    group_id.id_value
                );
            },
            Errors::CoordinatorNotAvailable | Errors::NotCoordinator => {
                kafka_debug!(
                    self.log_context,
                    "`{}` request for group id {} returned error {:?}. Will find the coordinator again and retry.",
                    api_name,
                    group_id.id_value,
                    error
                );
                groups_to_unmap.insert(group_id.clone());
            },
            Errors::UnsupportedVersion => {
                if is_consumer_group_response {
                    kafka_debug!(
                        self.log_context,
                        "`{}` request for group id {} failed because the API is not supported. \
                         Will retry with `DescribeGroups`.",
                        api_name,
                        group_id.id_value
                    );
                    self.use_classic_group_api.lock().unwrap().insert(group_id.id_value.clone());
                } else {
                    kafka_error!(
                        self.log_context,
                        "`{}` request for group id {} failed because the `ConsumerGroupDescribe` API is not supported.",
                        api_name,
                        group_id.id_value
                    );
                    failed.insert(group_id.clone(), error_with_optional_message(error, error_msg));
                }
            },
            Errors::GroupIdNotFound => {
                if is_consumer_group_response {
                    kafka_debug!(
                        self.log_context,
                        "`{}` request for group id {} failed because the group is not a new consumer group. \
                         Will retry with `DescribeGroups`.",
                        api_name,
                        group_id.id_value
                    );
                    self.use_classic_group_api.lock().unwrap().insert(group_id.id_value.clone());
                    // The ConsumerGroupDescribe message is more informative; keep it so we can use
                    // it if the classic API also returns GROUP_ID_NOT_FOUND.
                    self.group_id_not_found_error_messages
                        .lock()
                        .unwrap()
                        .insert(group_id.id_value.clone(), error_msg.unwrap_or("").to_string());
                } else {
                    kafka_debug!(
                        self.log_context,
                        "`{}` request for group id {} failed because the group does not exist.",
                        api_name,
                        group_id.id_value
                    );
                    let preferred = self
                        .group_id_not_found_error_messages
                        .lock()
                        .unwrap()
                        .get(&group_id.id_value)
                        .cloned();
                    let message = preferred.or_else(|| error_msg.map(str::to_string));
                    failed.insert(group_id.clone(), error_with_optional_message(error, message.as_deref()));
                }
            },
            other => {
                kafka_error!(
                    self.log_context,
                    "`{}` request for group id {} failed due to unexpected error {:?}.",
                    api_name,
                    group_id.id_value,
                    other
                );
                failed.insert(group_id.clone(), error_with_optional_message(other, error_msg));
            },
        }
    }
}

/// Builds a `Error` for `error`, using `message` when present (mirrors
/// Java's `Errors.exception(String)`, which falls back to the default text when
/// the message is null).
fn error_with_optional_message(error: Errors, message: Option<&str>) -> Error {
    // `Errors.exception(String)` (`Errors.java:462-469`), reached from
    // `error.exception(errorMsg)` (`DescribeConsumerGroupsHandler.java:339`), falls back to the code's
    // default text ONLY on `null`; a non-null EMPTY message is used verbatim
    // (finding 245).
    match message {
        Some(msg) => Error::with_message(error, msg.to_string()),
        None => Error::new(error),
    }
}

impl AdminApiHandler<CoordinatorKey, ConsumerGroupDescription> for DescribeConsumerGroupsHandler {
    fn api_name(&self) -> &str {
        "describeConsumerGroups"
    }

    fn build_request(&self, _broker_id: i32, keys: &HashSet<CoordinatorKey>) -> Vec<RequestAndKeys<CoordinatorKey>> {
        let use_classic = self.use_classic_group_api.lock().unwrap();
        let mut new_keys = HashSet::new();
        let mut new_ids = Vec::new();
        let mut old_keys = HashSet::new();
        let mut old_ids = Vec::new();

        for key in keys {
            assert!(
                key.coordinator_type == CoordinatorType::Group,
                "Invalid group coordinator key {key} when building `DescribeGroups` request"
            );
            if use_classic.contains(&key.id_value) {
                old_ids.push(key.id_value.clone());
                old_keys.insert(key.clone());
            } else {
                new_ids.push(key.id_value.clone());
                new_keys.insert(key.clone());
            }
        }
        drop(use_classic);

        let mut requests = Vec::new();
        if !new_keys.is_empty() {
            let mut data = ConsumerGroupDescribeRequestData::new();
            data.set_group_ids(new_ids);
            data.set_include_authorized_operations(self.include_authorized_operations);
            requests.push(RequestAndKeys {
                request: Box::new(ConsumerGroupDescribeRequestBuilder::new(data)) as Box<dyn RequestBuilder>,
                keys: new_keys,
            });
        }
        if !old_keys.is_empty() {
            let mut data = DescribeGroupsRequestData::new();
            data.set_groups(old_ids);
            data.set_include_authorized_operations(self.include_authorized_operations);
            requests.push(RequestAndKeys {
                request: Box::new(DescribeGroupsRequestBuilder::new(data)) as Box<dyn RequestBuilder>,
                keys: old_keys,
            });
        }
        requests
    }

    fn handle_response(
        &self,
        broker: &Node,
        keys: &HashSet<CoordinatorKey>,
        response: &ConcreteResponse,
    ) -> ApiResult<CoordinatorKey, ConsumerGroupDescription> {
        let mut completed = HashMap::new();
        let mut failed = HashMap::new();
        let mut groups_to_unmap = HashSet::new();

        match response {
            ConcreteResponse::DescribeGroups(r) => {
                self.handle_classic_group_response(broker, r.data(), &mut completed, &mut failed, &mut groups_to_unmap);
            },
            ConcreteResponse::ConsumerGroupDescribe(r) => {
                self.handle_consumer_group_response(
                    broker,
                    r.data(),
                    &mut completed,
                    &mut failed,
                    &mut groups_to_unmap,
                );
            },
            // `KafkaAdminClient.java:1387-1391` fails this one call on a response-type
            // mismatch; see `ApiResult::failed_all`.
            _ => {
                return ApiResult::failed_all(
                    keys,
                    Error::local_illegal_state("DescribeConsumerGroupsHandler received an unexpected response type"),
                );
            },
        }

        ApiResult::new(completed, failed, groups_to_unmap.into_iter().collect())
    }

    fn handle_unsupported_version_error(
        &self,
        _broker_id: i32,
        error: &Error,
        keys: &HashSet<CoordinatorKey>,
    ) -> HashMap<CoordinatorKey, Error> {
        let mut errors = HashMap::new();
        let mut use_classic = self.use_classic_group_api.lock().unwrap();
        for key in keys {
            // `insert` returns false if the id was already present — i.e. we
            // already tried the classic API, so this key must fail now.
            if !use_classic.insert(key.id_value.clone()) {
                errors.insert(key.clone(), error.clone());
            }
        }
        errors
    }

    fn lookup_strategy(&self) -> &dyn AdminApiLookupStrategy<CoordinatorKey> {
        &self.lookup_strategy
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::admin::internals::admin_api_handler::ApiResult;
    use crate::common::requests::{
        ConcreteResponse, ConsumerGroupDescribeResponse, DescribeGroupsResponse, RequestBuilder,
    };
    use crate::consumer::consumer_partition_assignor::Assignment;
    use crate::consumer_group_describe_response_data::{
        Assignment as WireResponseAssignment, ConsumerGroupDescribeResponseData, DescribedGroup as CgDescribedGroup,
        Member as CgMember, TopicPartitions as CgTopicPartitions,
    };
    use crate::describe_groups_response_data::{
        DescribeGroupsResponseData, DescribedGroup as ClassicDescribedGroup, DescribedGroupMember,
    };

    fn log_context() -> LogContext {
        LogContext::new(String::new())
    }

    fn coordinator() -> Node {
        Node::new(1, "host".to_string(), 1234)
    }

    const GROUP_ID1: &str = "group-id1";
    const GROUP_ID2: &str = "group-id2";

    fn keys() -> HashSet<CoordinatorKey> {
        HashSet::from([
            CoordinatorKey::by_group_id(GROUP_ID1),
            CoordinatorKey::by_group_id(GROUP_ID2),
        ])
    }

    fn tps() -> Vec<TopicPartition> {
        vec![TopicPartition::new("foo", 0), TopicPartition::new("bar", 1)]
    }

    /// Extracts `(group_ids, include_authorized_operations)` from a built
    /// request, whichever concrete variant it is.
    fn request_ids(mut request: Box<dyn RequestBuilder>) -> (Vec<String>, bool, &'static str) {
        match request.build().unwrap() {
            crate::common::requests::ConcreteRequest::ConsumerGroupDescribe(r) => {
                (r.data().group_ids.clone(), r.data().include_authorized_operations, "consumer")
            },
            crate::common::requests::ConcreteRequest::DescribeGroups(r) => {
                (r.data().groups.clone(), r.data().include_authorized_operations, "classic")
            },
            other => panic!("unexpected request {other:?}"),
        }
    }

    fn sorted(mut v: Vec<String>) -> Vec<String> {
        v.sort();
        v
    }

    /// Translated from `testBuildRequestWithMultipleGroupTypes` (both bool values).
    #[test]
    fn test_build_request_with_multiple_group_types() {
        for include_authorized_operations in [true, false] {
            let handler = DescribeConsumerGroupsHandler::new(include_authorized_operations, log_context());

            // First build: one ConsumerGroupDescribe request covering both groups.
            let mut requests = handler.build_request(1, &keys());
            assert_eq!(requests.len(), 1);
            let rk = requests.remove(0);
            assert_eq!(rk.keys, keys());
            let (ids, auth, kind) = request_ids(rk.request);
            assert_eq!(kind, "consumer");
            assert_eq!(sorted(ids), vec![GROUP_ID1.to_string(), GROUP_ID2.to_string()]);
            assert_eq!(auth, include_authorized_operations);

            // Group1 retriable, group2 GROUP_ID_NOT_FOUND -> group2 falls back to classic.
            let mut g1 = CgDescribedGroup::new();
            g1.set_group_id(GROUP_ID1.to_string())
                .set_error_code(Errors::CoordinatorLoadInProgress.code());
            let mut g2 = CgDescribedGroup::new();
            g2.set_group_id(GROUP_ID2.to_string())
                .set_error_code(Errors::GroupIdNotFound.code());
            let mut data = ConsumerGroupDescribeResponseData::new();
            data.set_groups(vec![g1, g2]);
            handler.handle_response(
                &coordinator(),
                &keys(),
                &ConcreteResponse::ConsumerGroupDescribe(ConsumerGroupDescribeResponse::new(data)),
            );

            // Second build: one new-API request for group1, one classic request for group2.
            let requests = handler.build_request(1, &keys());
            assert_eq!(requests.len(), 2);
            // new-API request is pushed first, classic second.
            let (ids0, _, kind0) = request_ids(requests.into_iter().map(|rk| rk.request).next().unwrap());
            assert_eq!(kind0, "consumer");
            assert_eq!(ids0, vec![GROUP_ID1.to_string()]);

            // Re-run to inspect the classic (second) request.
            let requests = handler.build_request(1, &keys());
            let second = requests.into_iter().nth(1).unwrap();
            assert_eq!(second.keys, HashSet::from([CoordinatorKey::by_group_id(GROUP_ID2)]));
            let (ids1, _, kind1) = request_ids(second.request);
            assert_eq!(kind1, "classic");
            assert_eq!(ids1, vec![GROUP_ID2.to_string()]);
        }
    }

    /// Translated from `testInvalidBuildRequest`.
    #[test]
    #[should_panic(expected = "Invalid group coordinator key")]
    fn test_invalid_build_request() {
        let handler = DescribeConsumerGroupsHandler::new(false, log_context());
        handler.build_request(1, &HashSet::from([CoordinatorKey::by_transactional_id("tId")]));
    }

    fn cg_topic_partitions(topic: &str, partition: i32) -> WireResponseAssignment {
        let mut tp = CgTopicPartitions::new();
        tp.set_topic_name(topic.to_string()).set_partitions(vec![partition]);
        let mut assignment = WireResponseAssignment::new();
        assignment.set_topic_partitions(vec![tp]);
        assignment
    }

    /// Translated from `testSuccessfulHandleConsumerGroupResponse`.
    #[test]
    fn test_successful_handle_consumer_group_response() {
        let handler = DescribeConsumerGroupsHandler::new(false, log_context());

        let mut m1 = CgMember::new();
        m1.set_member_id("memberId".to_string())
            .set_instance_id(Some("instanceId".to_string()))
            .set_client_host("host".to_string())
            .set_client_id("clientId".to_string())
            .set_member_epoch(10)
            .set_rack_id(Some("rackId".to_string()))
            .set_assignment(cg_topic_partitions("foo", 0))
            .set_target_assignment(cg_topic_partitions("foo", 1))
            .set_member_type(1);
        let mut m2 = CgMember::new();
        m2.set_member_id("memberId-classic".to_string())
            .set_instance_id(Some("instanceId-classic".to_string()))
            .set_client_host("host".to_string())
            .set_client_id("clientId-classic".to_string())
            .set_member_epoch(9)
            .set_rack_id(None)
            .set_assignment(cg_topic_partitions("bar", 0))
            .set_target_assignment(cg_topic_partitions("bar", 1))
            .set_member_type(0);
        let mut group = CgDescribedGroup::new();
        group
            .set_group_id(GROUP_ID1.to_string())
            .set_group_state("Stable".to_string())
            .set_group_epoch(10)
            .set_assignment_epoch(10)
            .set_assignor_name("range".to_string())
            .set_members(vec![m1, m2]);
        let mut data = ConsumerGroupDescribeResponseData::new();
        data.set_groups(vec![group]);

        let result = handler.handle_response(
            &coordinator(),
            &HashSet::from([CoordinatorKey::by_group_id(GROUP_ID1)]),
            &ConcreteResponse::ConsumerGroupDescribe(ConsumerGroupDescribeResponse::new(data)),
        );

        let expected_members = vec![
            MemberDescription::new(
                "memberId",
                Some("instanceId".to_string()),
                Some("rackId".to_string()),
                "clientId",
                "host",
                MemberAssignment::new(HashSet::from([TopicPartition::new("foo", 0)])),
                Some(MemberAssignment::new(HashSet::from([TopicPartition::new("foo", 1)]))),
                Some(10),
                Some(true),
            ),
            MemberDescription::new(
                "memberId-classic",
                Some("instanceId-classic".to_string()),
                None,
                "clientId-classic",
                "host",
                MemberAssignment::new(HashSet::from([TopicPartition::new("bar", 0)])),
                Some(MemberAssignment::new(HashSet::from([TopicPartition::new("bar", 1)]))),
                Some(9),
                Some(false),
            ),
        ];
        let expected = ConsumerGroupDescription::new(
            GROUP_ID1,
            false,
            expected_members,
            "range",
            GroupType::Consumer,
            GroupState::Stable,
            Some(coordinator()),
            std::collections::BTreeSet::new(),
            Some(10),
            Some(10),
        );
        assert_completed(&result, &expected);
    }

    fn build_describe_groups_response(error: Errors, protocol_type: &str) -> ConcreteResponse {
        let assignment_bytes = ConsumerProtocol::serialize_assignment(&Assignment::with_partitions(tps())).unwrap();
        let mut member = DescribedGroupMember::new();
        member
            .set_client_host("host".to_string())
            .set_client_id("clientId".to_string())
            .set_member_id("memberId".to_string())
            .set_member_assignment(assignment_bytes);
        let mut group = ClassicDescribedGroup::new();
        group
            .set_error_code(error.code())
            .set_group_id(GROUP_ID1.to_string())
            .set_group_state(GroupState::Stable.to_string())
            .set_protocol_type(protocol_type.to_string())
            .set_protocol_data("assignor".to_string())
            .set_members(vec![member]);
        let mut data = DescribeGroupsResponseData::new();
        data.set_groups(vec![group]);
        ConcreteResponse::DescribeGroups(DescribeGroupsResponse::new(data))
    }

    fn build_consumer_group_describe_response(error: Errors) -> ConcreteResponse {
        let mut group = CgDescribedGroup::new();
        group.set_group_id(GROUP_ID1.to_string()).set_error_code(error.code());
        let mut data = ConsumerGroupDescribeResponseData::new();
        data.set_groups(vec![group]);
        ConcreteResponse::ConsumerGroupDescribe(ConsumerGroupDescribeResponse::new(data))
    }

    fn handle_classic_group_with_error(
        error: Errors,
        protocol_type: &str,
    ) -> ApiResult<CoordinatorKey, ConsumerGroupDescription> {
        let handler = DescribeConsumerGroupsHandler::new(true, log_context());
        handler.handle_response(
            &coordinator(),
            &HashSet::from([CoordinatorKey::by_group_id(GROUP_ID1)]),
            &build_describe_groups_response(error, protocol_type),
        )
    }

    fn handle_consumer_group_with_error(error: Errors) -> ApiResult<CoordinatorKey, ConsumerGroupDescription> {
        let handler = DescribeConsumerGroupsHandler::new(true, log_context());
        handler.handle_response(
            &coordinator(),
            &HashSet::from([CoordinatorKey::by_group_id(GROUP_ID1)]),
            &build_consumer_group_describe_response(error),
        )
    }

    fn assert_unmapped(result: &ApiResult<CoordinatorKey, ConsumerGroupDescription>) {
        assert!(result.completed_keys.is_empty());
        assert!(result.failed_keys.is_empty());
        assert_eq!(result.unmapped_keys, vec![CoordinatorKey::by_group_id(GROUP_ID1)]);
    }

    fn assert_retriable(result: &ApiResult<CoordinatorKey, ConsumerGroupDescription>) {
        assert!(result.completed_keys.is_empty());
        assert!(result.failed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
    }

    fn assert_completed(
        result: &ApiResult<CoordinatorKey, ConsumerGroupDescription>,
        expected: &ConsumerGroupDescription,
    ) {
        let key = CoordinatorKey::by_group_id(GROUP_ID1);
        assert!(result.failed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
        assert_eq!(
            result.completed_keys.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from([key.clone()])
        );
        assert_eq!(result.completed_keys.get(&key).unwrap(), expected);
    }

    fn assert_failed_error(expected: Errors, result: &ApiResult<CoordinatorKey, ConsumerGroupDescription>) {
        let key = CoordinatorKey::by_group_id(GROUP_ID1);
        assert!(result.completed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
        assert_eq!(
            result.failed_keys.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from([key.clone()])
        );
        assert_eq!(result.failed_keys.get(&key).unwrap().error(), expected);
    }

    fn assert_failed_illegal_argument(result: &ApiResult<CoordinatorKey, ConsumerGroupDescription>) {
        let key = CoordinatorKey::by_group_id(GROUP_ID1);
        assert!(result.completed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
        assert!(matches!(result.failed_keys.get(&key).unwrap(), Error::LocalIllegalArgument(_)));
    }

    /// Translated from `testSuccessfulHandleClassicGroupResponse`.
    #[test]
    fn test_successful_handle_classic_group_response() {
        let result = handle_classic_group_with_error(Errors::None, "");
        let members = vec![MemberDescription::new(
            "memberId",
            None,
            None,
            "clientId",
            "host",
            MemberAssignment::new(tps().into_iter().collect()),
            None,
            None,
            None,
        )];
        let expected = ConsumerGroupDescription::new(
            GROUP_ID1,
            true,
            members,
            "assignor",
            GroupType::Classic,
            GroupState::Stable,
            Some(coordinator()),
            std::collections::BTreeSet::new(),
            None,
            None,
        );
        assert_completed(&result, &expected);
    }

    /// Translated from `testUnmappedHandleClassicGroupResponse`.
    #[test]
    fn test_unmapped_handle_classic_group_response() {
        assert_unmapped(&handle_classic_group_with_error(Errors::CoordinatorNotAvailable, ""));
        assert_unmapped(&handle_classic_group_with_error(Errors::NotCoordinator, ""));
    }

    /// Translated from `testRetriableHandleClassicGroupResponse`.
    #[test]
    fn test_retriable_handle_classic_group_response() {
        assert_retriable(&handle_classic_group_with_error(Errors::CoordinatorLoadInProgress, ""));
    }

    /// Translated from `testFailedHandleClassicGroupResponse`.
    #[test]
    fn test_failed_handle_classic_group_response() {
        assert_failed_error(
            Errors::UnsupportedVersion,
            &handle_classic_group_with_error(Errors::UnsupportedVersion, ""),
        );
        assert_failed_error(
            Errors::GroupAuthorizationFailed,
            &handle_classic_group_with_error(Errors::GroupAuthorizationFailed, ""),
        );
        assert_failed_error(
            Errors::GroupIdNotFound,
            &handle_classic_group_with_error(Errors::GroupIdNotFound, ""),
        );
        assert_failed_error(
            Errors::InvalidGroupId,
            &handle_classic_group_with_error(Errors::InvalidGroupId, ""),
        );
        assert_failed_illegal_argument(&handle_classic_group_with_error(Errors::None, "custom-protocol"));
    }

    /// Translated from `testUnmappedHandleConsumerGroupResponse`.
    #[test]
    fn test_unmapped_handle_consumer_group_response() {
        assert_unmapped(&handle_consumer_group_with_error(Errors::CoordinatorNotAvailable));
        assert_unmapped(&handle_consumer_group_with_error(Errors::NotCoordinator));
    }

    /// Translated from `testRetriableHandleConsumerGroupResponse`.
    #[test]
    fn test_retriable_handle_consumer_group_response() {
        assert_retriable(&handle_consumer_group_with_error(Errors::CoordinatorLoadInProgress));
        assert_retriable(&handle_consumer_group_with_error(Errors::GroupIdNotFound));
        assert_retriable(&handle_consumer_group_with_error(Errors::UnsupportedVersion));
    }

    /// Explicit classic-fallback state machine (required by the phase plan):
    /// a new-API `UNSUPPORTED_VERSION` moves the group to the classic API, and a
    /// subsequent build issues a `DescribeGroups` request for it.
    #[test]
    fn test_unsupported_version_falls_back_to_classic_build() {
        let handler = DescribeConsumerGroupsHandler::new(false, log_context());
        let mut group = CgDescribedGroup::new();
        group
            .set_group_id(GROUP_ID1.to_string())
            .set_error_code(Errors::UnsupportedVersion.code());
        let mut data = ConsumerGroupDescribeResponseData::new();
        data.set_groups(vec![group]);
        let result = handler.handle_response(
            &coordinator(),
            &HashSet::from([CoordinatorKey::by_group_id(GROUP_ID1)]),
            &ConcreteResponse::ConsumerGroupDescribe(ConsumerGroupDescribeResponse::new(data)),
        );
        // Retriable: neither completed nor failed nor unmapped.
        assert_retriable(&result);

        let requests = handler.build_request(1, &HashSet::from([CoordinatorKey::by_group_id(GROUP_ID1)]));
        assert_eq!(requests.len(), 1);
        let (ids, _, kind) = request_ids(requests.into_iter().next().unwrap().request);
        assert_eq!(kind, "classic");
        assert_eq!(ids, vec![GROUP_ID1.to_string()]);
    }

    /// The more-informative `ConsumerGroupDescribe` `GROUP_ID_NOT_FOUND` message
    /// is preserved when the classic `DescribeGroups` retry also returns
    /// `GROUP_ID_NOT_FOUND` (mirrors `groupIdNotFoundErrorMessages`).
    #[test]
    fn test_group_id_not_found_message_preserved_across_fallback() {
        let handler = DescribeConsumerGroupsHandler::new(false, log_context());
        // 1) New API returns GROUP_ID_NOT_FOUND with an informative message.
        let mut group = CgDescribedGroup::new();
        group
            .set_group_id(GROUP_ID1.to_string())
            .set_error_code(Errors::GroupIdNotFound.code())
            .set_error_message(Some("Group group-id1 is not a new consumer group.".to_string()));
        let mut data = ConsumerGroupDescribeResponseData::new();
        data.set_groups(vec![group]);
        let first = handler.handle_response(
            &coordinator(),
            &HashSet::from([CoordinatorKey::by_group_id(GROUP_ID1)]),
            &ConcreteResponse::ConsumerGroupDescribe(ConsumerGroupDescribeResponse::new(data)),
        );
        assert_retriable(&first);

        // 2) Classic API also returns GROUP_ID_NOT_FOUND with a terser message.
        let mut classic = ClassicDescribedGroup::new();
        classic
            .set_group_id(GROUP_ID1.to_string())
            .set_error_code(Errors::GroupIdNotFound.code())
            .set_error_message(Some("classic message".to_string()));
        let mut classic_data = DescribeGroupsResponseData::new();
        classic_data.set_groups(vec![classic]);
        let second = handler.handle_response(
            &coordinator(),
            &HashSet::from([CoordinatorKey::by_group_id(GROUP_ID1)]),
            &ConcreteResponse::DescribeGroups(DescribeGroupsResponse::new(classic_data)),
        );
        let key = CoordinatorKey::by_group_id(GROUP_ID1);
        let err = second.failed_keys.get(&key).unwrap();
        assert_eq!(err.error(), Errors::GroupIdNotFound);
        assert_eq!(err.message(), "Group group-id1 is not a new consumer group.");
    }

    /// Translated from `testFailedHandleConsumerGroupResponse`.
    #[test]
    fn test_failed_handle_consumer_group_response() {
        assert_failed_error(
            Errors::GroupAuthorizationFailed,
            &handle_consumer_group_with_error(Errors::GroupAuthorizationFailed),
        );
        assert_failed_error(
            Errors::TopicAuthorizationFailed,
            &handle_consumer_group_with_error(Errors::TopicAuthorizationFailed),
        );
        assert_failed_error(
            Errors::InvalidGroupId,
            &handle_consumer_group_with_error(Errors::InvalidGroupId),
        );
    }
}
