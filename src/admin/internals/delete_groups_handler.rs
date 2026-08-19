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

//! The abstract `DeleteGroups` admin API handler.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.DeleteGroupsHandler`.
//!
//! Java models this as an abstract base carrying all `DeleteGroupsRequest`/
//! `DeleteGroupsResponse` handling, with a thin subclass
//! (`DeleteConsumerGroupsHandler`) overriding only `apiName()` / `displayName()`.
//! The Rust translation preserves that split by composition: this struct holds
//! the `api_name` / `display_name` chosen by the subclass factory (see
//! [`super::delete_consumer_groups_handler`]) and performs all the real work.

use std::collections::{HashMap, HashSet};

use crate::common::protocol::Errors;
use crate::common::requests::{ConcreteResponse, CoordinatorType, DeleteGroupsRequestBuilder, RequestBuilder};
use crate::common::utils::LogContext;
use crate::common::{Error, Node};
use crate::delete_groups_request_data::DeleteGroupsRequestData;
use crate::{kafka_debug, kafka_error};

use super::admin_api_future::SimpleAdminApiFuture;
use super::admin_api_handler::{AdminApiHandler, ApiResult, RequestAndKeys};
use super::admin_api_lookup_strategy::AdminApiLookupStrategy;
use super::coordinator_key::CoordinatorKey;
use super::coordinator_strategy::CoordinatorStrategy;

/// The abstract `DeleteGroups` handler.
///
/// Corresponds to `DeleteGroupsHandler` (the abstract base of
/// `DeleteConsumerGroupsHandler`).
pub(crate) struct DeleteGroupsHandler {
    /// The `apiName()` of the concrete subclass (e.g. `"deleteConsumerGroups"`).
    api_name: &'static str,
    /// The `displayName()` of the concrete subclass (e.g. `"DeleteConsumerGroups"`).
    display_name: &'static str,
    log_context: LogContext,
    lookup_strategy: CoordinatorStrategy,
}

impl DeleteGroupsHandler {
    /// Creates a handler with the subclass-provided `api_name` / `display_name`.
    ///
    /// Mirrors `DeleteGroupsHandler(LogContext, Class<?>)` plus the subclass's
    /// `apiName()` / `displayName()` overrides.
    pub(crate) fn new(api_name: &'static str, display_name: &'static str, log_context: LogContext) -> Self {
        Self {
            api_name,
            display_name,
            lookup_strategy: CoordinatorStrategy::new(CoordinatorType::Group, log_context.clone()),
            log_context,
        }
    }

    /// The display name used in log messages. Mirrors `displayName()`.
    #[cfg(test)]
    pub(crate) fn display_name(&self) -> &str {
        self.display_name
    }

    /// Creates the future bundle for the given group ids.
    ///
    /// Mirrors `DeleteGroupsHandler.newFuture`.
    pub(crate) fn new_future(group_ids: &[String]) -> SimpleAdminApiFuture<CoordinatorKey, ()> {
        SimpleAdminApiFuture::for_keys(group_ids.iter().map(CoordinatorKey::by_group_id).collect())
    }

    /// Builds the single batched `DeleteGroups` request. Mirrors
    /// `buildBatchedRequest`.
    pub(crate) fn build_batched_request(
        &self,
        _coordinator_id: i32,
        keys: &HashSet<CoordinatorKey>,
    ) -> DeleteGroupsRequestBuilder {
        let group_ids: Vec<String> = keys.iter().map(|key| key.id_value.clone()).collect();
        let mut data = DeleteGroupsRequestData::new();
        data.set_groups_names(group_ids);
        DeleteGroupsRequestBuilder::new(data)
    }

    fn handle_error(
        &self,
        group_id: CoordinatorKey,
        error: Errors,
        failed: &mut HashMap<CoordinatorKey, Error>,
        groups_to_unmap: &mut HashSet<CoordinatorKey>,
    ) {
        match error {
            Errors::GroupAuthorizationFailed
            | Errors::InvalidGroupId
            | Errors::NonEmptyGroup
            | Errors::GroupIdNotFound => {
                kafka_debug!(
                    self.log_context,
                    "`{}` request for group id {} failed due to error {:?}",
                    self.display_name,
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
                    "`{}` request for group id {} failed because the coordinator is still in the \
                     process of loading state. Will retry",
                    self.display_name,
                    group_id.id_value
                );
            },
            Errors::CoordinatorNotAvailable | Errors::NotCoordinator => {
                // If the coordinator is unavailable or there was a coordinator
                // change, then we unmap the key so that we retry the
                // `FindCoordinator` request.
                kafka_debug!(
                    self.log_context,
                    "`{}` request for group id {} returned error {:?}. Will attempt to find the \
                     coordinator again and retry",
                    self.display_name,
                    group_id.id_value,
                    error
                );
                groups_to_unmap.insert(group_id);
            },
            other => {
                kafka_error!(
                    self.log_context,
                    "`{}` request for group id {} failed due to unexpected error {:?}",
                    self.display_name,
                    group_id.id_value,
                    other
                );
                failed.insert(group_id, Error::new(other));
            },
        }
    }
}

impl AdminApiHandler<CoordinatorKey, ()> for DeleteGroupsHandler {
    fn api_name(&self) -> &str {
        self.api_name
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
        _keys: &HashSet<CoordinatorKey>,
        response: &ConcreteResponse,
    ) -> ApiResult<CoordinatorKey, ()> {
        let ConcreteResponse::DeleteGroups(response) = response else {
            panic!("DeleteGroupsHandler received an unexpected response type: {response:?}");
        };

        let mut completed: HashMap<CoordinatorKey, ()> = HashMap::new();
        let mut failed: HashMap<CoordinatorKey, Error> = HashMap::new();
        let mut groups_to_unmap: HashSet<CoordinatorKey> = HashSet::new();

        for deleted_group in &response.data().results {
            let group_id_key = CoordinatorKey::by_group_id(&deleted_group.group_id);
            let error = Errors::for_code(deleted_group.error_code);
            if error != Errors::None {
                self.handle_error(group_id_key, error, &mut failed, &mut groups_to_unmap);
                continue;
            }
            completed.insert(group_id_key, ());
        }

        ApiResult::new(completed, failed, groups_to_unmap.into_iter().collect())
    }

    fn lookup_strategy(&self) -> &dyn AdminApiLookupStrategy<CoordinatorKey> {
        &self.lookup_strategy
    }
}
