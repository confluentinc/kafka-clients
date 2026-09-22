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

//! The `removeMembersFromConsumerGroup` admin API handler.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.RemoveMembersFromConsumerGroupHandler`.

use std::collections::{HashMap, HashSet};

use crate::common::Errors;
use crate::common::requests::{ConcreteResponse, CoordinatorType, LeaveGroupRequestBuilder, RequestBuilder};
use crate::common::utils::LogContext;
use crate::common::{Error, Node};
use crate::kafka_debug;
use crate::leave_group_request_data::MemberIdentity;

use super::AdminApiLookupStrategy;
use super::CoordinatorKey;
use super::CoordinatorStrategy;
use super::SimpleAdminApiFuture;
use super::{AdminApiHandler, ApiResult, RequestAndKeys};

/// The per-member removal result value produced by this handler.
type MemberErrors = HashMap<MemberIdentity, Errors>;

/// The `removeMembersFromConsumerGroup` handler.
///
/// Corresponds to `RemoveMembersFromConsumerGroupHandler`.
pub(crate) struct RemoveMembersFromConsumerGroupHandler {
    group_id: CoordinatorKey,
    members: Vec<MemberIdentity>,
    log_context: LogContext,
    lookup_strategy: CoordinatorStrategy,
}

impl RemoveMembersFromConsumerGroupHandler {
    /// Creates a handler for removing `members` from `group_id`.
    pub(crate) fn new(group_id: &str, members: Vec<MemberIdentity>, log_context: LogContext) -> Self {
        Self {
            group_id: CoordinatorKey::by_group_id(group_id),
            members,
            lookup_strategy: CoordinatorStrategy::new(CoordinatorType::Group, log_context.clone()),
            log_context,
        }
    }

    /// Creates the future bundle for the given group id.
    ///
    /// Mirrors `RemoveMembersFromConsumerGroupHandler.newFuture`.
    pub(crate) fn new_future(group_id: &str) -> SimpleAdminApiFuture<CoordinatorKey, MemberErrors> {
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

    /// Builds the single batched `LeaveGroup` request. Mirrors
    /// `buildBatchedRequest`.
    pub(crate) fn build_batched_request(
        &self,
        _coordinator_id: i32,
        group_ids: &HashSet<CoordinatorKey>,
    ) -> LeaveGroupRequestBuilder {
        self.validate_keys(group_ids);
        LeaveGroupRequestBuilder::new(self.group_id.id_value.clone(), self.members.clone())
    }

    fn handle_group_error(
        &self,
        group_id: CoordinatorKey,
        error: Errors,
        failed: &mut HashMap<CoordinatorKey, Error>,
        groups_to_unmap: &mut HashSet<CoordinatorKey>,
    ) {
        match error {
            Errors::GroupAuthorizationFailed => {
                kafka_debug!(
                    self.log_context,
                    "`LeaveGroup` request for group id {} failed due to error {:?}",
                    group_id.id_value,
                    error
                );
                failed.insert(group_id, Error::new(error));
            },
            Errors::CoordinatorLoadInProgress => {
                // If the coordinator is in the middle of loading, then we just
                // need to retry.
                kafka_debug!(
                    self.log_context,
                    "`LeaveGroup` request for group id {} failed because the coordinator is still in \
                     the process of loading state. Will retry",
                    group_id.id_value
                );
            },
            Errors::CoordinatorNotAvailable | Errors::NotCoordinator => {
                // If the coordinator is unavailable or there was a coordinator
                // change, then we unmap the key so that we retry the
                // `FindCoordinator` request.
                kafka_debug!(
                    self.log_context,
                    "`LeaveGroup` request for group id {} returned error {:?}. Will attempt to find \
                     the coordinator again and retry",
                    group_id.id_value,
                    error
                );
                groups_to_unmap.insert(group_id);
            },
            other => {
                kafka_debug!(
                    self.log_context,
                    "`LeaveGroup` request for group id {} failed due to unexpected error {:?}",
                    group_id.id_value,
                    other
                );
                failed.insert(group_id, Error::new(other));
            },
        }
    }
}

impl AdminApiHandler<CoordinatorKey, MemberErrors> for RemoveMembersFromConsumerGroupHandler {
    fn api_name(&self) -> &str {
        "leaveGroup"
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
    ) -> ApiResult<CoordinatorKey, MemberErrors> {
        self.validate_keys(group_ids);

        let ConcreteResponse::LeaveGroup(response) = response else {
            // `KafkaAdminClient.java:1387-1391` fails this one call on a response-type
            // mismatch; see `ApiResult::failed_all`.
            return ApiResult::failed_all(
                group_ids,
                Error::local_illegal_state(
                    "RemoveMembersFromConsumerGroupHandler received an unexpected response type",
                ),
            );
        };

        let error = response.top_level_error();
        if error != Errors::None {
            let mut failed: HashMap<CoordinatorKey, Error> = HashMap::new();
            let mut groups_to_unmap: HashSet<CoordinatorKey> = HashSet::new();
            self.handle_group_error(self.group_id.clone(), error, &mut failed, &mut groups_to_unmap);
            ApiResult::new(HashMap::new(), failed, groups_to_unmap.into_iter().collect())
        } else {
            let mut member_errors: MemberErrors = HashMap::new();
            for member_response in response.member_responses() {
                let mut identity = MemberIdentity::new();
                identity
                    .set_member_id(member_response.member_id.clone())
                    .set_group_instance_id(member_response.group_instance_id.clone());
                member_errors.insert(identity, Errors::for_code(member_response.error_code));
            }
            ApiResult::new(
                HashMap::from([(self.group_id.clone(), member_errors)]),
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
    use crate::LeaveGroupResponseData;
    use crate::common::requests::{LeaveGroupResponse, RequestBuilder};
    use crate::leave_group_response_data::MemberResponse;

    const GROUP_ID: &str = "group-id";

    fn log_context() -> LogContext {
        LogContext::new(String::new())
    }

    fn m(member_id: &str, group_instance_id: &str) -> MemberIdentity {
        let mut identity = MemberIdentity::new();
        identity
            .set_member_id(member_id.to_string())
            .set_group_instance_id(Some(group_instance_id.to_string()));
        identity
    }

    fn members() -> Vec<MemberIdentity> {
        vec![m("m1", "m1-gii"), m("m2", "m2-gii")]
    }

    fn handler() -> RemoveMembersFromConsumerGroupHandler {
        RemoveMembersFromConsumerGroupHandler::new(GROUP_ID, members(), log_context())
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

    fn build_response(error: Errors) -> ConcreteResponse {
        let mut member = MemberResponse::new();
        member
            .set_error_code(Errors::None.code())
            .set_member_id("m1".to_string())
            .set_group_instance_id(Some("m1-gii".to_string()));
        let mut data = LeaveGroupResponseData::new();
        data.set_error_code(error.code()).set_members(vec![member]);
        ConcreteResponse::LeaveGroup(LeaveGroupResponse::new(data))
    }

    fn build_response_with_member_error(error: Errors) -> ConcreteResponse {
        let mut member = MemberResponse::new();
        member
            .set_error_code(error.code())
            .set_member_id("m1".to_string())
            .set_group_instance_id(Some("m1-gii".to_string()));
        let mut data = LeaveGroupResponseData::new();
        data.set_error_code(Errors::None.code()).set_members(vec![member]);
        ConcreteResponse::LeaveGroup(LeaveGroupResponse::new(data))
    }

    fn handle_with_group_error(error: Errors) -> ApiResult<CoordinatorKey, MemberErrors> {
        handler().handle_response(&node(), &keys(), &build_response(error))
    }

    fn handle_with_member_error(error: Errors) -> ApiResult<CoordinatorKey, MemberErrors> {
        handler().handle_response(&node(), &keys(), &build_response_with_member_error(error))
    }

    fn assert_unmapped(result: &ApiResult<CoordinatorKey, MemberErrors>) {
        assert!(result.completed_keys.is_empty());
        assert!(result.failed_keys.is_empty());
        assert_eq!(result.unmapped_keys, vec![key()]);
    }

    fn assert_retriable(result: &ApiResult<CoordinatorKey, MemberErrors>) {
        assert!(result.completed_keys.is_empty());
        assert!(result.failed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
    }

    fn assert_completed(result: &ApiResult<CoordinatorKey, MemberErrors>, expected: &MemberErrors) {
        assert!(result.failed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
        assert_eq!(result.completed_keys.keys().cloned().collect::<HashSet<_>>(), keys());
        assert_eq!(result.completed_keys.get(&key()), Some(expected));
    }

    fn assert_failed(expected_error: Errors, result: &ApiResult<CoordinatorKey, MemberErrors>) {
        assert!(result.completed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
        assert_eq!(result.failed_keys.keys().cloned().collect::<HashSet<_>>(), keys());
        assert_eq!(result.failed_keys.get(&key()).map(Error::error), Some(expected_error));
    }

    /// Translated from `testBuildRequest`.
    #[test]
    fn test_build_request() {
        use crate::common::requests::ConcreteRequest;
        let mut builder = handler().build_batched_request(1, &keys());
        match builder.build().unwrap() {
            ConcreteRequest::LeaveGroup(r) => {
                assert_eq!(r.data().group_id, GROUP_ID);
                assert_eq!(r.data().members.len(), 2);
            },
            other => panic!("expected LeaveGroup, got {}", other.api_key().name()),
        }
    }

    /// Translated from `testSuccessfulHandleResponse`.
    #[test]
    fn test_successful_handle_response() {
        let expected = MemberErrors::from([(m("m1", "m1-gii"), Errors::None)]);
        assert_completed(&handle_with_group_error(Errors::None), &expected);
    }

    /// Translated from `testUnmappedHandleResponse`.
    #[test]
    fn test_unmapped_handle_response() {
        assert_unmapped(&handle_with_group_error(Errors::CoordinatorNotAvailable));
        assert_unmapped(&handle_with_group_error(Errors::NotCoordinator));
    }

    /// Translated from `testRetriableHandleResponse`.
    #[test]
    fn test_retriable_handle_response() {
        assert_retriable(&handle_with_group_error(Errors::CoordinatorLoadInProgress));
    }

    /// Translated from `testFailedHandleResponse`.
    #[test]
    fn test_failed_handle_response() {
        assert_failed(
            Errors::GroupAuthorizationFailed,
            &handle_with_group_error(Errors::GroupAuthorizationFailed),
        );
        assert_failed(Errors::UnknownServerError, &handle_with_group_error(Errors::UnknownServerError));
    }

    /// Translated from `testFailedHandleResponseInMemberLevel`.
    #[test]
    fn test_failed_handle_response_in_member_level() {
        for error in [Errors::FencedInstanceId, Errors::UnknownMemberId] {
            let expected = MemberErrors::from([(m("m1", "m1-gii"), error)]);
            assert_completed(&handle_with_member_error(error), &expected);
        }
    }
}
