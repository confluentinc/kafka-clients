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

//! The `deleteShareGroupOffsets` admin API handler.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.DeleteShareGroupOffsetsHandler`, an
//! `AdminApiHandler.Batched` for the single group whose offsets are deleted.

use std::collections::{HashMap, HashSet};

use crate::DeleteShareGroupOffsetsRequestData;
use crate::common::protocol::Errors;
use crate::common::requests::{ConcreteResponse, CoordinatorType, RequestBuilder, delete_share_group_offsets_request};
use crate::common::utils::internals::LogContext;
use crate::common::{Error, Node};
use crate::delete_share_group_offsets_request_data::DeleteShareGroupOffsetsRequestTopic;
use crate::{kafka_debug, kafka_error};

use super::AdminApiLookupStrategy;
use super::CoordinatorKey;
use super::CoordinatorStrategy;
use super::SimpleAdminApiFuture;
use super::{AdminApiHandler, ApiResult, RequestAndKeys};

/// The per-topic result produced by this handler: `None` for a topic whose
/// offsets were deleted (Java's `null` `ApiException`).
type TopicErrors = HashMap<String, Option<Error>>;

/// The `deleteShareGroupOffsets` handler.
///
/// Corresponds to `DeleteShareGroupOffsetsHandler`.
#[doc(alias = "org.apache.kafka.clients.admin.internals.DeleteShareGroupOffsetsHandler")]
pub(crate) struct DeleteShareGroupOffsetsHandler {
    group_id: CoordinatorKey,
    topics: HashSet<String>,
    log_context: LogContext,
    lookup_strategy: CoordinatorStrategy,
}

impl DeleteShareGroupOffsetsHandler {
    /// Creates a handler.
    #[doc(
        alias = "org.apache.kafka.clients.admin.internals.DeleteShareGroupOffsetsHandler#DeleteShareGroupOffsetsHandler"
    )]
    pub(crate) fn new(group_id: &str, topics: HashSet<String>, log_context: LogContext) -> Self {
        Self {
            group_id: CoordinatorKey::by_group_id(group_id),
            topics,
            lookup_strategy: CoordinatorStrategy::new(CoordinatorType::Group, log_context.clone()),
            log_context,
        }
    }

    /// Creates the future bundle for the given group id.
    ///
    /// Mirrors `DeleteShareGroupOffsetsHandler.newFuture`.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.DeleteShareGroupOffsetsHandler#newFuture")]
    pub(crate) fn new_future(group_id: &str) -> SimpleAdminApiFuture<CoordinatorKey, TopicErrors> {
        SimpleAdminApiFuture::for_keys(HashSet::from([CoordinatorKey::by_group_id(group_id)]))
    }

    /// Mirrors `validateKeys`: the keys must be exactly the single group id
    /// whose offsets this handler deletes.
    ///
    /// Java throws `IllegalArgumentException`; `handle_response` turns the `Err`
    /// into a failure of every key, as Java's `KafkaAdminClient` does when
    /// `handleResponse` throws (see `ApiResult::failed_all`).
    #[doc(alias = "org.apache.kafka.clients.admin.internals.DeleteShareGroupOffsetsHandler#validateKeys")]
    fn validate_keys(&self, group_ids: &HashSet<CoordinatorKey>) -> Result<(), Error> {
        if group_ids.len() == 1 && group_ids.contains(&self.group_id) {
            return Ok(());
        }
        // Java prints the sets with `Set.toString()`: `[k1, k2]`.
        let received = group_ids.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ");
        Err(Error::local_illegal_argument(format!(
            "Received unexpected group ids [{received}] (expected only [{}])",
            self.group_id
        )))
    }

    /// Builds the single `DeleteShareGroupOffsets` request. Mirrors
    /// `buildBatchedRequest`.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.DeleteShareGroupOffsetsHandler#buildBatchedRequest")]
    pub(crate) fn build_batched_request(
        &self,
        _broker_id: i32,
        group_ids: &HashSet<CoordinatorKey>,
    ) -> delete_share_group_offsets_request::Builder {
        // Java throws `IllegalArgumentException` from `validateKeys` here. The keys
        // come from `new_future` for this handler's group, so this cannot fire;
        // `build_request` has no error channel, as in the sibling
        // `DeleteConsumerGroupOffsetsHandler::build_batched_request`.
        if let Err(error) = self.validate_keys(group_ids) {
            panic!("{}", error.message());
        }

        // Java walks a `Set`, whose order is unspecified; sorted here so the request
        // is deterministic.
        let mut topic_names: Vec<&String> = self.topics.iter().collect();
        topic_names.sort();
        let request_topics = topic_names
            .into_iter()
            .map(|topic_name| {
                let mut topic = DeleteShareGroupOffsetsRequestTopic::new();
                topic.set_topic_name(topic_name.clone());
                topic
            })
            .collect();

        let mut data = DeleteShareGroupOffsetsRequestData::new();
        data.set_group_id(self.group_id.id_value.clone()).set_topics(request_topics);
        delete_share_group_offsets_request::Builder::new(data)
    }

    #[doc(alias = "org.apache.kafka.clients.admin.internals.DeleteShareGroupOffsetsHandler#handleGroupError")]
    fn handle_group_error(
        &self,
        error: Errors,
        error_message: Option<&str>,
        failed: &mut HashMap<CoordinatorKey, Error>,
        groups_to_unmap: &mut HashSet<CoordinatorKey>,
    ) {
        // Java logs the message with `{}`, which prints a `null` one as "null".
        let message_to_log = error_message.unwrap_or("null");
        match error {
            Errors::CoordinatorLoadInProgress | Errors::RebalanceInProgress => {
                // If the coordinator is in the middle of loading, then we just need to retry
                kafka_debug!(
                    self.log_context,
                    "DeleteShareGroupOffsets request for group id {} failed because the coordinator is still in \
                     the process of loading state. Will retry. {}",
                    self.group_id.id_value,
                    message_to_log
                );
            },
            Errors::CoordinatorNotAvailable | Errors::NotCoordinator => {
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                kafka_debug!(
                    self.log_context,
                    "DeleteShareGroupOffsets request for group id {} returned error {}. Will rediscover the \
                     coordinator and retry. {}",
                    self.group_id.id_value,
                    error,
                    message_to_log
                );
                groups_to_unmap.insert(self.group_id.clone());
            },
            Errors::InvalidGroupId
            | Errors::GroupIdNotFound
            | Errors::NonEmptyGroup
            | Errors::InvalidRequest
            | Errors::UnknownServerError
            | Errors::KafkaStorageError
            | Errors::GroupAuthorizationFailed => {
                kafka_debug!(
                    self.log_context,
                    "DeleteShareGroupOffsets request for group id {} failed due to error {}. {}",
                    self.group_id.id_value,
                    error,
                    message_to_log
                );
                failed.insert(self.group_id.clone(), error.error_with_optional_message(error_message));
            },
            other => {
                kafka_error!(
                    self.log_context,
                    "DeleteShareGroupOffsets request for group id {} failed due to unexpected error {}. {}",
                    self.group_id.id_value,
                    other,
                    message_to_log
                );
                failed.insert(self.group_id.clone(), other.error_with_optional_message(error_message));
            },
        }
    }
}

impl AdminApiHandler<CoordinatorKey, TopicErrors> for DeleteShareGroupOffsetsHandler {
    fn api_name(&self) -> &str {
        "deleteShareGroupOffsets"
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
    ) -> ApiResult<CoordinatorKey, TopicErrors> {
        if let Err(error) = self.validate_keys(group_ids) {
            return ApiResult::failed_all(group_ids, error);
        }
        let ConcreteResponse::DeleteShareGroupOffsets(response) = response else {
            // `org.apache.kafka.clients.admin.KafkaAdminClient.AdminClientRunnable#handleResponses`
            // fails this one call on a response-type mismatch; see
            // `ApiResult::failed_all`.
            return ApiResult::failed_all(
                group_ids,
                Error::local_illegal_state("DeleteShareGroupOffsetsHandler received an unexpected response type"),
            );
        };

        let data = response.data();
        let group_error = Errors::for_code(data.error_code);
        if group_error != Errors::None {
            let mut groups_to_unmap = HashSet::new();
            let mut groups_failed = HashMap::new();
            self.handle_group_error(
                group_error,
                data.error_message.as_deref(),
                &mut groups_failed,
                &mut groups_to_unmap,
            );
            return ApiResult::new(HashMap::new(), groups_failed, groups_to_unmap.into_iter().collect());
        }

        let mut topic_results: TopicErrors = HashMap::new();
        for topic in &data.responses {
            let topic_error = Errors::for_code(topic.error_code);
            if topic_error != Errors::None {
                kafka_debug!(
                    self.log_context,
                    "DeleteShareGroupOffsets request for group id {} and topic {} failed and returned error {}. {}",
                    self.group_id.id_value,
                    topic.topic_name,
                    topic_error,
                    topic.error_message.as_deref().unwrap_or("null")
                );
            }
            // Java's `Errors.forCode(code).exception(message)`: `null` for `NONE`.
            let result = (topic_error != Errors::None)
                .then(|| topic_error.error_with_optional_message(topic.error_message.as_deref()));
            topic_results.insert(topic.topic_name.clone(), result);
        }
        ApiResult::new(
            HashMap::from([(self.group_id.clone(), topic_results)]),
            HashMap::new(),
            Vec::new(),
        )
    }

    fn lookup_strategy(&self) -> &dyn AdminApiLookupStrategy<CoordinatorKey> {
        &self.lookup_strategy
    }
}

#[cfg(test)]
mod tests {
    //! Java has no `DeleteShareGroupOffsetsHandlerTest`; these Rust tests pin
    //! the handler's `handleGroupError` branches and the request shape, which
    //! `KafkaAdminClientShareGroupTest` exercises only end to end.

    use super::*;
    use crate::DeleteShareGroupOffsetsResponseData;
    use crate::common::requests::{AbstractRequest, DeleteShareGroupOffsetsResponse};
    use crate::delete_share_group_offsets_response_data::DeleteShareGroupOffsetsResponseTopic;

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

    fn handler() -> DeleteShareGroupOffsetsHandler {
        DeleteShareGroupOffsetsHandler::new(
            GROUP_ID,
            HashSet::from(["foo".to_string(), "bar".to_string()]),
            LogContext::new(String::new()),
        )
    }

    fn group_error(error: Errors) -> ConcreteResponse {
        let mut data = DeleteShareGroupOffsetsResponseData::new();
        data.set_error_code(error.code())
            .set_error_message(Some("group failed".to_string()));
        ConcreteResponse::DeleteShareGroupOffsets(DeleteShareGroupOffsetsResponse::new(data))
    }

    #[test]
    fn build_request_names_the_group_and_topics() {
        match handler().build_batched_request(1, &keys()).build().unwrap() {
            AbstractRequest::DeleteShareGroupOffsets(request) => {
                assert_eq!(request.data().group_id, GROUP_ID);
                let topics: Vec<&str> = request.data().topics.iter().map(|t| t.topic_name.as_str()).collect();
                assert_eq!(topics, vec!["bar", "foo"]);
            },
            other => panic!("expected DeleteShareGroupOffsets, got {}", other.api_key().name()),
        }
    }

    #[test]
    fn topic_results_carry_each_topic_error() {
        let mut ok = DeleteShareGroupOffsetsResponseTopic::new();
        ok.set_topic_name("bar".to_string());
        let mut failed = DeleteShareGroupOffsetsResponseTopic::new();
        failed
            .set_topic_name("foo".to_string())
            .set_error_code(Errors::KafkaStorageError.code())
            .set_error_message(Some("disk".to_string()));
        let mut data = DeleteShareGroupOffsetsResponseData::new();
        data.set_responses(vec![ok, failed]);
        let response = ConcreteResponse::DeleteShareGroupOffsets(DeleteShareGroupOffsetsResponse::new(data));

        let result = handler().handle_response(&node(), &keys(), &response);
        assert!(result.failed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
        let topics = &result.completed_keys[&key()];
        assert!(topics["bar"].is_none());
        let error = topics["foo"].as_ref().expect("an error");
        assert_eq!(error.error(), Errors::KafkaStorageError);
        assert_eq!(error.message(), "disk");
    }

    #[test]
    fn group_errors_fail_retry_or_unmap() {
        for error in [
            Errors::InvalidGroupId,
            Errors::GroupIdNotFound,
            Errors::NonEmptyGroup,
            Errors::InvalidRequest,
            Errors::UnknownServerError,
            Errors::KafkaStorageError,
            Errors::GroupAuthorizationFailed,
            Errors::UnknownTopicOrPartition,
        ] {
            let result = handler().handle_response(&node(), &keys(), &group_error(error));
            assert!(result.completed_keys.is_empty());
            assert!(result.unmapped_keys.is_empty());
            let failure = &result.failed_keys[&key()];
            assert_eq!(failure.error(), error);
            assert_eq!(failure.message(), "group failed");
        }
        for error in [Errors::CoordinatorLoadInProgress, Errors::RebalanceInProgress] {
            let result = handler().handle_response(&node(), &keys(), &group_error(error));
            assert!(result.completed_keys.is_empty(), "{error}");
            assert!(result.failed_keys.is_empty());
            assert!(result.unmapped_keys.is_empty());
        }
        for error in [Errors::CoordinatorNotAvailable, Errors::NotCoordinator] {
            let result = handler().handle_response(&node(), &keys(), &group_error(error));
            assert!(result.completed_keys.is_empty(), "{error}");
            assert!(result.failed_keys.is_empty());
            assert_eq!(result.unmapped_keys, vec![key()]);
        }
    }

    #[test]
    fn unexpected_keys_fail_with_java_message() {
        let other = HashSet::from([CoordinatorKey::by_group_id("other")]);
        let result = handler().handle_response(&node(), &other, &group_error(Errors::None));
        let failure = &result.failed_keys[&CoordinatorKey::by_group_id("other")];
        assert!(failure.is_local_illegal_argument_error(), "{failure:?}");
        assert_eq!(
            failure.message(),
            "Received unexpected group ids [CoordinatorKey(idValue='other', type=GROUP)] (expected only \
             [CoordinatorKey(idValue='group-id', type=GROUP)])"
        );
    }
}
