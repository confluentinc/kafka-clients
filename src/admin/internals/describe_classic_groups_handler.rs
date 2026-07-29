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

//! The `describeClassicGroups` admin API handler.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.DescribeClassicGroupsHandler`.
//!
//! Always uses the classic `DescribeGroups` API (batched: one request per
//! coordinator for all its keys).

use std::collections::{HashMap, HashSet};

use crate::admin::internals::admin_utils::valid_acl_operations;
use crate::admin::{ClassicGroupDescription, MemberAssignment, MemberDescription};
use crate::common::protocol::Errors;
use crate::common::requests::{ConcreteResponse, CoordinatorType, DescribeGroupsRequestBuilder, RequestBuilder};
use crate::common::utils::LogContext;
use crate::common::{ClassicGroupState, KafkaError, Node, TopicPartition};
use crate::consumer::internals::consumer_protocol::{ConsumerProtocol, PROTOCOL_TYPE};
use crate::describe_groups_request_data::DescribeGroupsRequestData;
use crate::{kafka_debug, kafka_error};

use super::admin_api_future::SimpleAdminApiFuture;
use super::admin_api_handler::{AdminApiHandler, ApiResult, RequestAndKeys};
use super::admin_api_lookup_strategy::AdminApiLookupStrategy;
use super::coordinator_key::CoordinatorKey;
use super::coordinator_strategy::CoordinatorStrategy;

/// The `describeClassicGroups` handler.
///
/// Corresponds to `DescribeClassicGroupsHandler` (an `AdminApiHandler.Batched`).
pub(crate) struct DescribeClassicGroupsHandler {
    include_authorized_operations: bool,
    log_context: LogContext,
    lookup_strategy: CoordinatorStrategy,
}

impl DescribeClassicGroupsHandler {
    /// Creates a handler.
    pub(crate) fn new(include_authorized_operations: bool, log_context: LogContext) -> Self {
        Self {
            include_authorized_operations,
            lookup_strategy: CoordinatorStrategy::new(CoordinatorType::Group, log_context.clone()),
            log_context,
        }
    }

    fn build_key_set(group_ids: &[String]) -> HashSet<CoordinatorKey> {
        group_ids.iter().map(CoordinatorKey::by_group_id).collect()
    }

    /// Creates the future bundle for the given group ids.
    ///
    /// Mirrors `DescribeClassicGroupsHandler.newFuture`.
    pub(crate) fn new_future(group_ids: &[String]) -> SimpleAdminApiFuture<CoordinatorKey, ClassicGroupDescription> {
        SimpleAdminApiFuture::for_keys(Self::build_key_set(group_ids))
    }

    /// Builds the single batched `DescribeGroups` request for all keys.
    ///
    /// Mirrors `DescribeClassicGroupsHandler.buildBatchedRequest`.
    fn build_batched_request(&self, keys: &HashSet<CoordinatorKey>) -> DescribeGroupsRequestBuilder {
        let group_ids: Vec<String> = keys
            .iter()
            .map(|key| {
                assert!(
                    key.coordinator_type == CoordinatorType::Group,
                    "Invalid group coordinator key {key} when building `DescribeGroups` request"
                );
                key.id_value.clone()
            })
            .collect();
        let mut data = DescribeGroupsRequestData::new();
        data.set_groups(group_ids);
        data.set_include_authorized_operations(self.include_authorized_operations);
        DescribeGroupsRequestBuilder::new(data)
    }

    fn handle_error(
        &self,
        group_id: &CoordinatorKey,
        error: Errors,
        error_msg: Option<&str>,
        failed: &mut HashMap<CoordinatorKey, KafkaError>,
        groups_to_unmap: &mut HashSet<CoordinatorKey>,
    ) {
        match error {
            Errors::GroupAuthorizationFailed => {
                kafka_debug!(
                    self.log_context,
                    "`DescribeGroups` request for group id {} failed due to error {:?}.",
                    group_id.id_value,
                    error
                );
                failed.insert(group_id.clone(), exception_with_optional_message(error, error_msg));
            },
            Errors::CoordinatorLoadInProgress => {
                kafka_debug!(
                    self.log_context,
                    "`DescribeGroups` request for group id {} failed because the coordinator is still loading. Will retry.",
                    group_id.id_value
                );
            },
            Errors::CoordinatorNotAvailable | Errors::NotCoordinator => {
                kafka_debug!(
                    self.log_context,
                    "`DescribeGroups` request for group id {} returned error {:?}. Will find the coordinator again and retry.",
                    group_id.id_value,
                    error
                );
                groups_to_unmap.insert(group_id.clone());
            },
            other => {
                kafka_error!(
                    self.log_context,
                    "`DescribeGroups` request for group id {} failed due to unexpected error {:?}.",
                    group_id.id_value,
                    other
                );
                failed.insert(group_id.clone(), exception_with_optional_message(other, error_msg));
            },
        }
    }
}

/// Builds a `KafkaError` for `error`, using `message` when present.
fn exception_with_optional_message(error: Errors, message: Option<&str>) -> KafkaError {
    match message {
        Some(msg) if !msg.is_empty() => KafkaError::with_message(error, msg.to_string()),
        _ => KafkaError::new(error),
    }
}

impl AdminApiHandler<CoordinatorKey, ClassicGroupDescription> for DescribeClassicGroupsHandler {
    fn api_name(&self) -> &str {
        "describeClassicGroups"
    }

    fn build_request(&self, _broker_id: i32, keys: &HashSet<CoordinatorKey>) -> Vec<RequestAndKeys<CoordinatorKey>> {
        vec![RequestAndKeys {
            request: Box::new(self.build_batched_request(keys)) as Box<dyn RequestBuilder>,
            keys: keys.clone(),
        }]
    }

    fn handle_response(
        &self,
        coordinator: &Node,
        _keys: &HashSet<CoordinatorKey>,
        response: &ConcreteResponse,
    ) -> ApiResult<CoordinatorKey, ClassicGroupDescription> {
        let ConcreteResponse::DescribeGroups(response) = response else {
            panic!("Received an unexpected response type: {response:?}");
        };
        let mut completed = HashMap::new();
        let mut failed = HashMap::new();
        let mut groups_to_unmap = HashSet::new();

        for described_group in &response.data().groups {
            let group_id_key = CoordinatorKey::by_group_id(described_group.group_id.clone());
            let error = Errors::for_code(described_group.error_code);
            if error != Errors::None {
                self.handle_error(&group_id_key, error, Some(error.message()), &mut failed, &mut groups_to_unmap);
                continue;
            }

            let authorized_operations = valid_acl_operations(described_group.authorized_operations);
            let protocol_type = &described_group.protocol_type;
            let is_consumer_group = protocol_type == PROTOCOL_TYPE || protocol_type.is_empty();
            let mut member_descriptions = Vec::with_capacity(described_group.members.len());
            let mut deserialize_error = None;
            for group_member in &described_group.members {
                let mut partitions = HashSet::new();
                if is_consumer_group && !group_member.member_assignment.is_empty() {
                    match ConsumerProtocol::deserialize_assignment(&group_member.member_assignment) {
                        Ok(assignment) => {
                            partitions = assignment.partitions().iter().cloned().collect::<HashSet<TopicPartition>>();
                        },
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
            if let Some(error) = deserialize_error {
                failed.insert(group_id_key.clone(), error);
                continue;
            }

            let description = ClassicGroupDescription::new(
                group_id_key.id_value.clone(),
                protocol_type.clone(),
                described_group.protocol_data.clone(),
                member_descriptions,
                ClassicGroupState::parse(&described_group.group_state),
                Some(coordinator.clone()),
                authorized_operations,
            );
            completed.insert(group_id_key, description);
        }

        ApiResult::new(completed, failed, groups_to_unmap.into_iter().collect())
    }

    fn lookup_strategy(&self) -> &dyn AdminApiLookupStrategy<CoordinatorKey> {
        &self.lookup_strategy
    }
}
