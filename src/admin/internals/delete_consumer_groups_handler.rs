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

//! The `deleteConsumerGroups` admin API handler.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.DeleteConsumerGroupsHandler` — the
//! thin subclass of [`DeleteGroupsHandler`](super::delete_groups_handler) that
//! only overrides `apiName()` / `displayName()`.

use crate::common::utils::LogContext;

use super::DeleteGroupsHandler;

/// The `apiName()` reported by `DeleteConsumerGroupsHandler`.
const API_NAME: &str = "deleteConsumerGroups";
/// The `displayName()` reported by `DeleteConsumerGroupsHandler`.
const DISPLAY_NAME: &str = "DeleteConsumerGroups";

/// Namespace for the `deleteConsumerGroups` handler factory.
///
/// Corresponds to `DeleteConsumerGroupsHandler`. Because the Java subclass adds
/// no state and only overrides the two name accessors, the Rust translation
/// exposes a factory that configures the base [`DeleteGroupsHandler`] with the
/// subclass's names rather than a distinct newtype (which would only forward
/// every trait method).
pub(crate) struct DeleteConsumerGroupsHandler;

impl DeleteConsumerGroupsHandler {
    /// Creates a `DeleteGroups` handler configured as
    /// `DeleteConsumerGroupsHandler`. Mirrors
    /// `new DeleteConsumerGroupsHandler(logContext)`.
    ///
    /// Returns the configured base [`DeleteGroupsHandler`] rather than `Self`
    /// (the subclass adds no state — see the module docs), so
    /// `clippy::new_ret_no_self` does not apply here.
    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new(log_context: LogContext) -> DeleteGroupsHandler {
        DeleteGroupsHandler::new(API_NAME, DISPLAY_NAME, log_context)
    }
}

#[cfg(test)]
mod tests {
    //! Translated from `DeleteConsumerGroupsHandlerTest extends
    //! DeleteGroupsHandlerTest`: the abstract base test's methods run against
    //! the concrete `DeleteConsumerGroups` handler.

    use std::collections::HashSet;

    use super::*;
    use crate::DeleteGroupsResponseData;
    use crate::admin::internals::CoordinatorKey;
    use crate::admin::internals::{AdminApiHandler, ApiResult};
    use crate::common::Errors;
    use crate::common::requests::{ConcreteResponse, DeleteGroupsResponse, RequestBuilder};
    use crate::common::{Error, Node};
    use crate::delete_groups_response_data::DeletableGroupResult;

    const GROUP_ID1: &str = "group-id1";

    fn log_context() -> LogContext {
        LogContext::new(String::new())
    }

    fn handler() -> DeleteGroupsHandler {
        DeleteConsumerGroupsHandler::new(log_context())
    }

    fn key() -> CoordinatorKey {
        CoordinatorKey::by_group_id(GROUP_ID1)
    }

    fn keys() -> HashSet<CoordinatorKey> {
        HashSet::from([key()])
    }

    fn node() -> Node {
        Node::new(1, "host".to_string(), 1234)
    }

    fn build_response(error: Errors) -> ConcreteResponse {
        let mut result = DeletableGroupResult::new();
        result.set_group_id(GROUP_ID1.to_string()).set_error_code(error.code());
        let mut data = DeleteGroupsResponseData::new();
        data.set_results(vec![result]);
        ConcreteResponse::DeleteGroups(DeleteGroupsResponse::new(data))
    }

    fn handle_with_error(error: Errors) -> ApiResult<CoordinatorKey, ()> {
        handler().handle_response(&node(), &keys(), &build_response(error))
    }

    fn assert_unmapped(result: &ApiResult<CoordinatorKey, ()>) {
        assert!(result.completed_keys.is_empty());
        assert!(result.failed_keys.is_empty());
        assert_eq!(result.unmapped_keys, vec![key()]);
    }

    fn assert_retriable(result: &ApiResult<CoordinatorKey, ()>) {
        assert!(result.completed_keys.is_empty());
        assert!(result.failed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
    }

    fn assert_completed(result: &ApiResult<CoordinatorKey, ()>) {
        assert!(result.failed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
        assert_eq!(result.completed_keys.keys().cloned().collect::<HashSet<_>>(), keys());
    }

    fn assert_failed(expected_error: Errors, result: &ApiResult<CoordinatorKey, ()>) {
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
            ConcreteRequest::DeleteGroups(r) => {
                assert_eq!(r.data().groups_names.len(), 1);
                assert_eq!(r.data().groups_names[0], GROUP_ID1);
            },
            other => panic!("expected DeleteGroups, got {}", other.api_key().name()),
        }
    }

    /// Translated from `testSuccessfulHandleResponse`.
    #[test]
    fn test_successful_handle_response() {
        assert_completed(&handle_with_error(Errors::None));
    }

    /// Translated from `testUnmappedHandleResponse`.
    #[test]
    fn test_unmapped_handle_response() {
        assert_unmapped(&handle_with_error(Errors::NotCoordinator));
        assert_unmapped(&handle_with_error(Errors::CoordinatorNotAvailable));
    }

    /// Translated from `testRetriableHandleResponse`.
    #[test]
    fn test_retriable_handle_response() {
        assert_retriable(&handle_with_error(Errors::CoordinatorLoadInProgress));
    }

    /// Translated from `testFailedHandleResponse`.
    #[test]
    fn test_failed_handle_response() {
        assert_failed(
            Errors::GroupAuthorizationFailed,
            &handle_with_error(Errors::GroupAuthorizationFailed),
        );
        assert_failed(Errors::GroupIdNotFound, &handle_with_error(Errors::GroupIdNotFound));
        assert_failed(Errors::InvalidGroupId, &handle_with_error(Errors::InvalidGroupId));
        assert_failed(Errors::NonEmptyGroup, &handle_with_error(Errors::NonEmptyGroup));
    }

    /// The concrete handler reports the subclass display name.
    #[test]
    fn test_display_name() {
        assert_eq!(handler().display_name(), DISPLAY_NAME);
        assert_eq!(handler().api_name(), API_NAME);
    }
}
