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

//! The `alterShareGroupOffsets` admin API handler.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.AlterShareGroupOffsetsHandler`, an
//! `AdminApiHandler.Batched` for the single group being altered.

use std::collections::{HashMap, HashSet};

use crate::AlterShareGroupOffsetsRequestData;
use crate::alter_share_group_offsets_request_data::{
    AlterShareGroupOffsetsRequestPartition, AlterShareGroupOffsetsRequestTopic,
};
use crate::common::protocol::Errors;
use crate::common::requests::{ConcreteResponse, CoordinatorType, RequestBuilder, alter_share_group_offsets_request};
use crate::common::utils::internals::LogContext;
use crate::common::{Error, Node, TopicPartition};
use crate::{kafka_debug, kafka_error};

use super::AdminApiLookupStrategy;
use super::CoordinatorKey;
use super::CoordinatorStrategy;
use super::SimpleAdminApiFuture;
use super::{AdminApiHandler, ApiResult, RequestAndKeys};

/// The per-partition result produced by this handler: `None` for a partition
/// whose offset was altered (Java's `null` `ApiException`).
type PartitionErrors = HashMap<TopicPartition, Option<Error>>;

/// The `alterShareGroupOffsets` handler.
///
/// Corresponds to `AlterShareGroupOffsetsHandler`.
#[doc(alias = "org.apache.kafka.clients.admin.internals.AlterShareGroupOffsetsHandler")]
pub(crate) struct AlterShareGroupOffsetsHandler {
    group_id: CoordinatorKey,
    offsets: HashMap<TopicPartition, i64>,
    log_context: LogContext,
    lookup_strategy: CoordinatorStrategy,
}

impl AlterShareGroupOffsetsHandler {
    /// Creates a handler.
    #[doc(
        alias = "org.apache.kafka.clients.admin.internals.AlterShareGroupOffsetsHandler#AlterShareGroupOffsetsHandler"
    )]
    pub(crate) fn new(group_id: &str, offsets: HashMap<TopicPartition, i64>, log_context: LogContext) -> Self {
        Self {
            group_id: CoordinatorKey::by_group_id(group_id),
            offsets,
            lookup_strategy: CoordinatorStrategy::new(CoordinatorType::Group, log_context.clone()),
            log_context,
        }
    }

    /// Creates the future bundle for the given group id.
    ///
    /// Mirrors `AlterShareGroupOffsetsHandler.newFuture`.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AlterShareGroupOffsetsHandler#newFuture")]
    pub(crate) fn new_future(group_id: &str) -> SimpleAdminApiFuture<CoordinatorKey, PartitionErrors> {
        SimpleAdminApiFuture::for_keys(HashSet::from([CoordinatorKey::by_group_id(group_id)]))
    }

    /// Mirrors `validateKeys`: the keys must be exactly the single group id this
    /// handler alters.
    ///
    /// Java throws `IllegalArgumentException`; the caller turns the `Err` into a
    /// failure of every key, as Java's `KafkaAdminClient` does when
    /// `handleResponse` throws (see `ApiResult::failed_all`).
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AlterShareGroupOffsetsHandler#validateKeys")]
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

    /// Builds the single `AlterShareGroupOffsets` request. Mirrors
    /// `buildBatchedRequest`, which, unlike `handleResponse`, does not validate the
    /// keys.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AlterShareGroupOffsetsHandler#buildBatchedRequest")]
    pub(crate) fn build_batched_request(
        &self,
        _broker_id: i32,
        _group_ids: &HashSet<CoordinatorKey>,
    ) -> alter_share_group_offsets_request::Builder {
        let mut data = AlterShareGroupOffsetsRequestData::new();
        data.set_group_id(self.group_id.id_value.clone());
        // Java finds each topic in the request's keyed topic collection, adding it on
        // first use; the generated Rust collection is a `Vec`, so the topic's index is
        // kept alongside it. Java walks a `HashMap`, so the order is unspecified on
        // both sides.
        let mut topic_index: HashMap<&str, usize> = HashMap::new();
        for (tp, offset) in &self.offsets {
            let index = *topic_index.entry(tp.topic()).or_insert_with(|| {
                let mut topic = AlterShareGroupOffsetsRequestTopic::new();
                topic.set_topic_name(tp.topic().to_string());
                data.topics.push(topic);
                data.topics.len() - 1
            });
            let mut partition = AlterShareGroupOffsetsRequestPartition::new();
            partition.set_partition_index(tp.partition()).set_start_offset(*offset);
            data.topics[index].partitions.push(partition);
        }
        alter_share_group_offsets_request::Builder::new(data)
    }

    #[doc(alias = "org.apache.kafka.clients.admin.internals.AlterShareGroupOffsetsHandler#handleError")]
    fn handle_error(
        &self,
        topic_partition: &TopicPartition,
        error: Errors,
        error_message: Option<&str>,
        partition_results: &mut PartitionErrors,
        groups_to_unmap: &mut HashSet<CoordinatorKey>,
        groups_to_retry: &mut HashSet<CoordinatorKey>,
    ) {
        let error_message_to_log = error_message.unwrap_or("");
        match error {
            Errors::CoordinatorLoadInProgress | Errors::RebalanceInProgress => {
                kafka_debug!(
                    self.log_context,
                    "AlterShareGroupOffsets request for group id {} returned error {}. Will retry. {}",
                    self.group_id.id_value,
                    error,
                    error_message_to_log
                );
                groups_to_retry.insert(self.group_id.clone());
            },
            Errors::CoordinatorNotAvailable | Errors::NotCoordinator => {
                kafka_debug!(
                    self.log_context,
                    "AlterShareGroupOffsets request for group id {} returned error {}. Will rediscover the \
                     coordinator and retry. {}",
                    self.group_id.id_value,
                    error,
                    error_message_to_log
                );
                groups_to_unmap.insert(self.group_id.clone());
            },
            Errors::GroupIdNotFound
            | Errors::NonEmptyGroup
            | Errors::InvalidRequest
            | Errors::UnknownServerError
            | Errors::KafkaStorageError
            | Errors::GroupAuthorizationFailed => {
                kafka_debug!(
                    self.log_context,
                    "AlterShareGroupOffsets request for group id {} failed due to error {}. {}",
                    self.group_id.id_value,
                    error,
                    error_message_to_log
                );
                partition_results
                    .insert(topic_partition.clone(), Some(error.error_with_optional_message(error_message)));
            },
            other => {
                kafka_error!(
                    self.log_context,
                    "AlterShareGroupOffsets request for group id {} failed due to unexpected error {}. {}",
                    self.group_id.id_value,
                    other,
                    error_message_to_log
                );
                partition_results
                    .insert(topic_partition.clone(), Some(other.error_with_optional_message(error_message)));
            },
        }
    }
}

impl AdminApiHandler<CoordinatorKey, PartitionErrors> for AlterShareGroupOffsetsHandler {
    fn api_name(&self) -> &str {
        "alterShareGroupOffsets"
    }

    fn build_request(&self, broker_id: i32, keys: &HashSet<CoordinatorKey>) -> Vec<RequestAndKeys<CoordinatorKey>> {
        vec![RequestAndKeys {
            request: Box::new(self.build_batched_request(broker_id, keys)) as Box<dyn RequestBuilder>,
            keys: keys.clone(),
        }]
    }

    fn handle_response(
        &self,
        _broker: &Node,
        keys: &HashSet<CoordinatorKey>,
        response: &ConcreteResponse,
    ) -> ApiResult<CoordinatorKey, PartitionErrors> {
        if let Err(error) = self.validate_keys(keys) {
            return ApiResult::failed_all(keys, error);
        }
        let ConcreteResponse::AlterShareGroupOffsets(response) = response else {
            // `org.apache.kafka.clients.admin.KafkaAdminClient.AdminClientRunnable#handleResponses`
            // fails this one call on a response-type mismatch; see
            // `ApiResult::failed_all`.
            return ApiResult::failed_all(
                keys,
                Error::local_illegal_state("AlterShareGroupOffsetsHandler received an unexpected response type"),
            );
        };

        let mut groups_to_unmap = HashSet::new();
        let mut groups_to_retry = HashSet::new();
        let mut partition_results: PartitionErrors = HashMap::new();

        let data = response.data();
        if data.error_code != Errors::None.code() {
            let top_level_error = Errors::for_code(data.error_code);
            let top_level_error_message = data.error_message.as_deref();
            for topic_partition in self.offsets.keys() {
                self.handle_error(
                    topic_partition,
                    top_level_error,
                    top_level_error_message,
                    &mut partition_results,
                    &mut groups_to_unmap,
                    &mut groups_to_retry,
                );
            }
        } else {
            for topic in &data.responses {
                for partition in &topic.partitions {
                    let partition_error = Errors::for_code(partition.error_code);
                    if partition_error != Errors::None {
                        kafka_debug!(
                            self.log_context,
                            "AlterShareGroupOffsets request for group id {} and topic-partition {}-{} failed and \
                             returned error {}. {}",
                            self.group_id.id_value,
                            topic.topic_name,
                            partition.partition_index,
                            partition_error,
                            partition.error_message.as_deref().unwrap_or("")
                        );
                    }
                    // Java's `partitionError.exception(message)`: `null` for `NONE`.
                    let result = (partition_error != Errors::None)
                        .then(|| partition_error.error_with_optional_message(partition.error_message.as_deref()));
                    partition_results
                        .insert(TopicPartition::new(topic.topic_name.clone(), partition.partition_index), result);
                }
            }
        }

        if groups_to_unmap.is_empty() && groups_to_retry.is_empty() {
            ApiResult::new(
                HashMap::from([(self.group_id.clone(), partition_results)]),
                HashMap::new(),
                Vec::new(),
            )
        } else {
            // Java's `ApiResult.unmapped(groupsToUnmap)`: with nothing to unmap the
            // key is neither completed nor failed, so the driver retries it.
            ApiResult::new(HashMap::new(), HashMap::new(), groups_to_unmap.into_iter().collect())
        }
    }

    fn lookup_strategy(&self) -> &dyn AdminApiLookupStrategy<CoordinatorKey> {
        &self.lookup_strategy
    }
}

#[cfg(test)]
mod tests {
    //! Java has no `AlterShareGroupOffsetsHandlerTest`; these Rust tests pin
    //! the handler's `handleError` branches and the request shape, which
    //! `KafkaAdminClientShareGroupTest` exercises only end to end.

    use super::*;
    use crate::AlterShareGroupOffsetsResponseData;
    use crate::alter_share_group_offsets_response_data::{
        AlterShareGroupOffsetsResponsePartition, AlterShareGroupOffsetsResponseTopic,
    };
    use crate::common::requests::{AbstractRequest, AlterShareGroupOffsetsResponse};

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

    fn offsets() -> HashMap<TopicPartition, i64> {
        HashMap::from([
            (TopicPartition::new("foo", 0), 1),
            (TopicPartition::new("foo", 1), 2),
            (TopicPartition::new("bar", 0), 3),
        ])
    }

    fn handler() -> AlterShareGroupOffsetsHandler {
        AlterShareGroupOffsetsHandler::new(GROUP_ID, offsets(), LogContext::new(String::new()))
    }

    fn top_level_error(error: Errors, message: Option<&str>) -> ConcreteResponse {
        let mut data = AlterShareGroupOffsetsResponseData::new();
        data.set_error_code(error.code()).set_error_message(message.map(str::to_string));
        ConcreteResponse::AlterShareGroupOffsets(AlterShareGroupOffsetsResponse::new(data))
    }

    #[test]
    fn build_request_groups_the_offsets_by_topic() {
        match handler().build_batched_request(1, &keys()).build().unwrap() {
            AbstractRequest::AlterShareGroupOffsets(request) => {
                let data = request.data();
                assert_eq!(data.group_id, GROUP_ID);
                let mut offsets: Vec<(String, i32, i64)> = data
                    .topics
                    .iter()
                    .flat_map(|t| {
                        t.partitions
                            .iter()
                            .map(|p| (t.topic_name.clone(), p.partition_index, p.start_offset))
                    })
                    .collect();
                offsets.sort();
                assert_eq!(
                    offsets,
                    vec![
                        ("bar".to_string(), 0, 3),
                        ("foo".to_string(), 0, 1),
                        ("foo".to_string(), 1, 2)
                    ]
                );
                assert_eq!(data.topics.len(), 2);
            },
            other => panic!("expected AlterShareGroupOffsets, got {}", other.api_key().name()),
        }
    }

    #[test]
    fn partition_results_carry_each_partition_error() {
        let mut ok = AlterShareGroupOffsetsResponsePartition::new();
        ok.set_partition_index(0);
        let mut failed = AlterShareGroupOffsetsResponsePartition::new();
        failed
            .set_partition_index(1)
            .set_error_code(Errors::TopicAuthorizationFailed.code())
            .set_error_message(Some("Topic authorization failed.".to_string()));
        let mut topic = AlterShareGroupOffsetsResponseTopic::new();
        topic.set_topic_name("foo".to_string()).set_partitions(vec![ok, failed]);
        let mut data = AlterShareGroupOffsetsResponseData::new();
        data.set_responses(vec![topic]);
        let response = ConcreteResponse::AlterShareGroupOffsets(AlterShareGroupOffsetsResponse::new(data));

        let result = handler().handle_response(&node(), &keys(), &response);
        assert!(result.failed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
        let partitions = &result.completed_keys[&key()];
        assert_eq!(partitions.len(), 2);
        assert!(partitions[&TopicPartition::new("foo", 0)].is_none());
        let error = partitions[&TopicPartition::new("foo", 1)].as_ref().expect("an error");
        assert_eq!(error.error(), Errors::TopicAuthorizationFailed);
        assert_eq!(error.message(), "Topic authorization failed.");
    }

    #[test]
    fn top_level_error_is_recorded_for_every_partition() {
        for error in [
            Errors::GroupIdNotFound,
            Errors::NonEmptyGroup,
            Errors::InvalidRequest,
            Errors::UnknownServerError,
            Errors::KafkaStorageError,
            Errors::GroupAuthorizationFailed,
            Errors::InvalidGroupId,
        ] {
            let result = handler().handle_response(&node(), &keys(), &top_level_error(error, Some("denied")));
            assert!(result.failed_keys.is_empty());
            assert!(result.unmapped_keys.is_empty());
            let partitions = &result.completed_keys[&key()];
            assert_eq!(partitions.len(), 3, "{error}");
            for failure in partitions.values() {
                let failure = failure.as_ref().expect("an error");
                assert_eq!(failure.error(), error);
                assert_eq!(failure.message(), "denied");
            }
        }
    }

    #[test]
    fn retried_or_unmapped_on_coordinator_errors() {
        for error in [Errors::CoordinatorLoadInProgress, Errors::RebalanceInProgress] {
            let result = handler().handle_response(&node(), &keys(), &top_level_error(error, None));
            assert!(result.completed_keys.is_empty(), "{error}");
            assert!(result.failed_keys.is_empty());
            assert!(result.unmapped_keys.is_empty());
        }
        for error in [Errors::CoordinatorNotAvailable, Errors::NotCoordinator] {
            let result = handler().handle_response(&node(), &keys(), &top_level_error(error, None));
            assert!(result.completed_keys.is_empty(), "{error}");
            assert!(result.failed_keys.is_empty());
            assert_eq!(result.unmapped_keys, vec![key()]);
        }
    }

    #[test]
    fn unexpected_keys_fail_with_java_message() {
        let other = HashSet::from([CoordinatorKey::by_group_id("other")]);
        let result = handler().handle_response(&node(), &other, &top_level_error(Errors::None, None));
        let failure = &result.failed_keys[&CoordinatorKey::by_group_id("other")];
        assert!(failure.is_local_illegal_argument_error(), "{failure:?}");
        assert_eq!(
            failure.message(),
            "Received unexpected group ids [CoordinatorKey(idValue='other', type=GROUP)] (expected only \
             [CoordinatorKey(idValue='group-id', type=GROUP)])"
        );
    }
}
