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

//! Handler for the `listOffsets` API (partition-leader targeted).
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.ListOffsetsHandler`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::admin::list_offsets_result::ListOffsetsResultInfo;
use crate::admin::options::ListOffsetsOptions;
use crate::common::protocol::Errors;
use crate::common::requests::list_offsets_request::{
    EARLIEST_LOCAL_TIMESTAMP, EARLIEST_PENDING_UPLOAD_TIMESTAMP, LATEST_TIERED_TIMESTAMP, MAX_TIMESTAMP,
};
use crate::common::requests::list_offsets_response::UNKNOWN_EPOCH;
use crate::common::requests::{ConcreteResponse, ListOffsetsRequestBuilder, RequestBuilder};
use crate::common::utils::LogContext;
use crate::common::{Error, Node, TopicPartition};
use crate::kafka_debug;
use crate::list_offsets_request_data::{ListOffsetsPartition, ListOffsetsTopic};

use super::admin_api_handler::{AdminApiHandler, ApiResult, RequestAndKeys};
use super::admin_api_lookup_strategy::AdminApiLookupStrategy;
use super::partition_leader_cache::PartitionLeaderCache;
use super::partition_leader_strategy::{PartitionLeaderFuture, PartitionLeaderStrategy};

/// Handler for `listOffsets`.
///
/// Corresponds to `ListOffsetsHandler` (a `Batched` handler over
/// `TopicPartition` keys yielding [`ListOffsetsResultInfo`] values).
pub(crate) struct ListOffsetsHandler {
    offset_timestamps_by_partition: HashMap<TopicPartition, i64>,
    options: ListOffsetsOptions,
    log_context: LogContext,
    lookup_strategy: PartitionLeaderStrategy,
    default_api_timeout_ms: i32,
}

impl ListOffsetsHandler {
    /// Creates a handler for the given per-partition offset timestamps.
    pub(crate) fn new(
        offset_timestamps_by_partition: HashMap<TopicPartition, i64>,
        options: ListOffsetsOptions,
        log_context: LogContext,
        default_api_timeout_ms: i32,
    ) -> Self {
        // Java: `new PartitionLeaderStrategy(logContext, false)` — do NOT
        // tolerate unknown-topic errors.
        let lookup_strategy = PartitionLeaderStrategy::with_tolerate_unknown_topics(log_context.clone(), false);
        Self {
            offset_timestamps_by_partition,
            options,
            log_context,
            lookup_strategy,
            default_api_timeout_ms,
        }
    }

    /// Creates the future bundle that the driver completes as partitions are
    /// resolved.
    ///
    /// Mirrors `ListOffsetsHandler.newFuture`.
    pub(crate) fn new_future(
        topic_partitions: HashSet<TopicPartition>,
        partition_leader_cache: Arc<PartitionLeaderCache>,
    ) -> PartitionLeaderFuture<ListOffsetsResultInfo> {
        PartitionLeaderFuture::new(topic_partitions, partition_leader_cache)
    }

    /// Builds a single batched `ListOffsets` request builder for the given keys.
    ///
    /// Mirrors `buildBatchedRequest`. Returns the concrete builder so the
    /// version-selection logic (`oldest_allowed_version`) can be inspected.
    pub(crate) fn build_batched_request(
        &self,
        _broker_id: i32,
        keys: &HashSet<TopicPartition>,
    ) -> ListOffsetsRequestBuilder {
        let mut topics_by_name: HashMap<String, ListOffsetsTopic> = HashMap::new();
        for topic_partition in keys {
            let offset_timestamp = self.offset_timestamps_by_partition.get(topic_partition).copied().unwrap_or(0);
            let topic = topics_by_name.entry(topic_partition.topic().to_string()).or_insert_with(|| {
                let mut t = ListOffsetsTopic::new();
                t.set_name(topic_partition.topic().to_string());
                t
            });
            let mut partition = ListOffsetsPartition::new();
            partition.set_partition_index(topic_partition.partition());
            partition.set_timestamp(offset_timestamp);
            topic.partitions.push(partition);
        }

        let supports_max_timestamp = keys
            .iter()
            .any(|key| self.offset_timestamps_by_partition.get(key) == Some(&MAX_TIMESTAMP));
        let require_earliest_local_timestamp = keys
            .iter()
            .any(|key| self.offset_timestamps_by_partition.get(key) == Some(&EARLIEST_LOCAL_TIMESTAMP));
        let require_tiered_storage_timestamp = keys
            .iter()
            .any(|key| self.offset_timestamps_by_partition.get(key) == Some(&LATEST_TIERED_TIMESTAMP));
        let require_earliest_pending_upload_timestamp = keys
            .iter()
            .any(|key| self.offset_timestamps_by_partition.get(key) == Some(&EARLIEST_PENDING_UPLOAD_TIMESTAMP));

        let timeout_ms = self.options.timeout().unwrap_or(self.default_api_timeout_ms);
        let mut builder = ListOffsetsRequestBuilder::for_consumer_with_features(
            true,
            self.options.isolation_level(),
            supports_max_timestamp,
            require_earliest_local_timestamp,
            require_tiered_storage_timestamp,
            require_earliest_pending_upload_timestamp,
        );
        builder
            .set_target_times(topics_by_name.into_values().collect())
            .set_timeout_ms(timeout_ms);
        builder
    }

    /// Classifies a partition error into unmapped (invalid metadata), retriable
    /// (left out of the result to retry) or failed (fatal).
    ///
    /// Mirrors `handlePartitionError`.
    fn handle_partition_error(
        &self,
        topic_partition: &TopicPartition,
        error: Errors,
        failed: &mut HashMap<TopicPartition, Error>,
        unmapped: &mut Vec<TopicPartition>,
        retriable: &mut HashSet<TopicPartition>,
    ) {
        if error == Errors::NotLeaderOrFollower || error == Errors::LeaderNotAvailable {
            kafka_debug!(
                self.log_context,
                "ListOffsets lookup request for topic partition {} will be retried due to invalid leader metadata {:?}",
                topic_partition,
                error
            );
            unmapped.push(topic_partition.clone());
        } else if error.is_retriable() {
            kafka_debug!(
                self.log_context,
                "ListOffsets fulfillment request for topic partition {} will be retried due to {:?}",
                topic_partition,
                error
            );
            retriable.insert(topic_partition.clone());
        } else {
            kafka_debug!(
                self.log_context,
                "ListOffsets request for topic partition {} failed due to an unexpected error {:?}",
                topic_partition,
                error
            );
            failed.insert(topic_partition.clone(), Error::new(error));
        }
    }
}

impl AdminApiHandler<TopicPartition, ListOffsetsResultInfo> for ListOffsetsHandler {
    fn api_name(&self) -> &str {
        "listOffsets"
    }

    fn build_request(&self, broker_id: i32, keys: &HashSet<TopicPartition>) -> Vec<RequestAndKeys<TopicPartition>> {
        let builder = self.build_batched_request(broker_id, keys);
        vec![RequestAndKeys { request: Box::new(builder) as Box<dyn RequestBuilder>, keys: keys.clone() }]
    }

    fn handle_response(
        &self,
        broker: &Node,
        keys: &HashSet<TopicPartition>,
        response: &ConcreteResponse,
    ) -> ApiResult<TopicPartition, ListOffsetsResultInfo> {
        let ConcreteResponse::ListOffsets(response) = response else {
            return ApiResult::new(HashMap::new(), HashMap::new(), Vec::new());
        };
        let mut completed: HashMap<TopicPartition, ListOffsetsResultInfo> = HashMap::new();
        let mut failed: HashMap<TopicPartition, Error> = HashMap::new();
        let mut unmapped: Vec<TopicPartition> = Vec::new();
        let mut retriable: HashSet<TopicPartition> = HashSet::new();

        for topic in response.topics() {
            for partition in &topic.partitions {
                let topic_partition = TopicPartition::new(topic.name.as_str(), partition.partition_index);
                let error = Errors::for_code(partition.error_code);
                if !self.offset_timestamps_by_partition.contains_key(&topic_partition) {
                    kafka_debug!(
                        self.log_context,
                        "ListOffsets response includes unknown topic partition {}",
                        topic_partition
                    );
                } else if error == Errors::None {
                    let leader_epoch = if partition.leader_epoch == UNKNOWN_EPOCH {
                        None
                    } else {
                        Some(partition.leader_epoch)
                    };
                    completed.insert(
                        topic_partition,
                        ListOffsetsResultInfo::new(partition.offset, partition.timestamp, leader_epoch),
                    );
                } else {
                    self.handle_partition_error(&topic_partition, error, &mut failed, &mut unmapped, &mut retriable);
                }
            }
        }

        // Sanity-check that the current leader returned results for every key.
        for topic_partition in keys {
            if unmapped.is_empty()
                && !completed.contains_key(topic_partition)
                && !failed.contains_key(topic_partition)
                && !retriable.contains(topic_partition)
            {
                let sanity_check_error = Error::with_message(
                    Errors::UnknownServerError,
                    format!(
                        "The response from broker {} did not contain a result for topic partition {}",
                        broker.id(),
                        topic_partition
                    ),
                );
                failed.insert(topic_partition.clone(), sanity_check_error);
            }
        }

        ApiResult::new(completed, failed, unmapped)
    }

    fn handle_unsupported_version_exception(
        &self,
        _broker_id: i32,
        exception: &Error,
        keys: &HashSet<TopicPartition>,
    ) -> HashMap<TopicPartition, Error> {
        // Only partitions with a MAX_TIMESTAMP spec can be failed by an
        // unsupported-version downgrade; if there are none (or all keys are
        // MAX_TIMESTAMP), every key is failed. Mirrors
        // `handleUnsupportedVersionException`.
        let mut max_timestamp_partitions: HashMap<TopicPartition, Error> = HashMap::new();
        for topic_partition in keys {
            if self.offset_timestamps_by_partition.get(topic_partition) == Some(&MAX_TIMESTAMP) {
                max_timestamp_partitions.insert(topic_partition.clone(), exception.clone());
            }
        }
        if max_timestamp_partitions.is_empty() {
            keys.iter().map(|k| (k.clone(), exception.clone())).collect()
        } else {
            max_timestamp_partitions
        }
    }

    fn lookup_strategy(&self) -> &dyn AdminApiLookupStrategy<TopicPartition> {
        &self.lookup_strategy
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::IsolationLevel;
    use crate::common::requests::list_offsets_request::EARLIEST_TIMESTAMP;
    use crate::common::requests::{ConcreteResponse, ListOffsetsResponse};
    use crate::list_offsets_response_data::{
        ListOffsetsPartitionResponse, ListOffsetsResponseData, ListOffsetsTopicResponse,
    };

    const DEFAULT_API_TIMEOUT_MS: i32 = 100;

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic, partition)
    }

    fn node() -> Node {
        Node::new(1, "host".to_string(), 1234)
    }

    /// The six-partition offset-spec fixture from `ListOffsetsHandlerTest`.
    fn offset_timestamps() -> HashMap<TopicPartition, i64> {
        use crate::common::requests::list_offsets_request::{LATEST_TIERED_TIMESTAMP, LATEST_TIMESTAMP};
        [
            (tp("t0", 0), LATEST_TIMESTAMP),
            (tp("t0", 1), EARLIEST_TIMESTAMP),
            (tp("t1", 0), 123),
            (tp("t1", 1), MAX_TIMESTAMP),
            (tp("t2", 0), EARLIEST_LOCAL_TIMESTAMP),
            (tp("t2", 1), LATEST_TIERED_TIMESTAMP),
        ]
        .into_iter()
        .collect()
    }

    fn handler(options: ListOffsetsOptions) -> ListOffsetsHandler {
        ListOffsetsHandler::new(offset_timestamps(), options, LogContext::new("[test] "), DEFAULT_API_TIMEOUT_MS)
    }

    /// Builds a synthetic ListOffsets response covering `specs`, applying the
    /// per-partition error codes from `errors_by_partition` (default NONE). The
    /// offset value is arbitrary (the handler tests never assert on it).
    fn create_response(
        errors_by_partition: &HashMap<TopicPartition, i16>,
        specs: &HashMap<TopicPartition, i64>,
    ) -> ConcreteResponse {
        let mut responses_by_topic: HashMap<String, ListOffsetsTopicResponse> = HashMap::new();
        for topic_partition in specs.keys() {
            let topic_response = responses_by_topic
                .entry(topic_partition.topic().to_string())
                .or_insert_with(|| {
                    let mut t = ListOffsetsTopicResponse::new();
                    t.set_name(topic_partition.topic().to_string());
                    t
                });
            let mut partition_response = ListOffsetsPartitionResponse::new();
            partition_response.set_partition_index(topic_partition.partition());
            partition_response.set_offset(1024);
            partition_response.set_error_code(errors_by_partition.get(topic_partition).copied().unwrap_or(0));
            topic_response.partitions.push(partition_response);
        }
        let mut data = ListOffsetsResponseData::new();
        data.set_topics(responses_by_topic.into_values().collect());
        ConcreteResponse::ListOffsets(ListOffsetsResponse::new(data))
    }

    fn handle(errors_by_partition: &HashMap<TopicPartition, i16>) -> ApiResult<TopicPartition, ListOffsetsResultInfo> {
        handle_with_specs(errors_by_partition, &offset_timestamps())
    }

    fn handle_with_specs(
        errors_by_partition: &HashMap<TopicPartition, i16>,
        specs: &HashMap<TopicPartition, i64>,
    ) -> ApiResult<TopicPartition, ListOffsetsResultInfo> {
        let keys: HashSet<TopicPartition> = offset_timestamps().into_keys().collect();
        handler(ListOffsetsOptions::new()).handle_response(&node(), &keys, &create_response(errors_by_partition, specs))
    }

    /// Mirrors `assertResult`.
    fn assert_result(
        result: &ApiResult<TopicPartition, ListOffsetsResultInfo>,
        expected_completed: HashSet<TopicPartition>,
        expected_failed: HashSet<TopicPartition>,
        expected_unmapped: Vec<TopicPartition>,
        expected_retriable: HashSet<TopicPartition>,
    ) {
        assert_eq!(
            result.completed_keys.keys().cloned().collect::<HashSet<_>>(),
            expected_completed
        );
        assert_eq!(result.failed_keys.keys().cloned().collect::<HashSet<_>>(), expected_failed);
        assert_eq!(result.unmapped_keys, expected_unmapped);
        let mut actual_retriable: HashSet<TopicPartition> = offset_timestamps().into_keys().collect();
        for k in result.completed_keys.keys() {
            actual_retriable.remove(k);
        }
        for k in result.failed_keys.keys() {
            actual_retriable.remove(k);
        }
        for k in &result.unmapped_keys {
            actual_retriable.remove(k);
        }
        assert_eq!(actual_retriable, expected_retriable);
    }

    /// Mirrors `testBuildRequestSimple`.
    #[test]
    fn build_request_simple() {
        let handler = handler(ListOffsetsOptions::new());
        let keys: HashSet<TopicPartition> = [tp("t0", 0), tp("t0", 1)].into_iter().collect();
        let builder = handler.build_batched_request(node().id(), &keys);
        assert_eq!(builder.data().topics.len(), 1);
        assert_eq!(builder.data().topics[0].partitions.len(), 2);
        for partition in &builder.data().topics[0].partitions {
            let topic_partition = tp(&builder.data().topics[0].name, partition.partition_index);
            assert_eq!(partition.timestamp, offset_timestamps()[&topic_partition]);
        }
        assert_eq!(builder.data().isolation_level, IsolationLevel::ReadUncommitted.id() as i8);
    }

    /// Mirrors `testBuildRequestMultipleTopicsWithReadCommitted`.
    #[test]
    fn build_request_multiple_topics_with_read_committed() {
        let handler = handler(ListOffsetsOptions::with_isolation_level(IsolationLevel::ReadCommitted));
        let keys: HashSet<TopicPartition> = offset_timestamps().into_keys().collect();
        let builder = handler.build_batched_request(node().id(), &keys);
        assert_eq!(builder.data().topics.len(), 3);
        let mut partitions: HashMap<TopicPartition, i64> = HashMap::new();
        for topic in &builder.data().topics {
            for partition in &topic.partitions {
                partitions.insert(tp(&topic.name, partition.partition_index), partition.timestamp);
            }
        }
        assert_eq!(partitions.len(), 6);
        for (topic_partition, timestamp) in &partitions {
            assert_eq!(*timestamp, offset_timestamps()[topic_partition]);
        }
        assert_eq!(builder.data().isolation_level, IsolationLevel::ReadCommitted.id() as i8);
    }

    /// Mirrors `testBuildRequestAllowedVersions`.
    #[test]
    fn build_request_allowed_versions() {
        let default_handler = handler(ListOffsetsOptions::new());
        let builder = default_handler
            .build_batched_request(node().id(), &[tp("t0", 0), tp("t0", 1), tp("t1", 0)].into_iter().collect());
        assert_eq!(builder.oldest_allowed_version(), 1);

        let read_committed = handler(ListOffsetsOptions::with_isolation_level(IsolationLevel::ReadCommitted));
        let builder = read_committed
            .build_batched_request(node().id(), &[tp("t0", 0), tp("t0", 1), tp("t1", 0)].into_iter().collect());
        assert_eq!(builder.oldest_allowed_version(), 2);

        let builder = read_committed.build_batched_request(
            node().id(),
            &[tp("t0", 0), tp("t0", 1), tp("t1", 0), tp("t1", 1)].into_iter().collect(),
        );
        assert_eq!(builder.oldest_allowed_version(), 7);

        let builder = read_committed.build_batched_request(
            node().id(),
            &[tp("t0", 0), tp("t0", 1), tp("t1", 0), tp("t1", 1), tp("t2", 0)]
                .into_iter()
                .collect(),
        );
        assert_eq!(builder.oldest_allowed_version(), 8);

        let builder = read_committed.build_batched_request(
            node().id(),
            &[
                tp("t0", 0),
                tp("t0", 1),
                tp("t1", 0),
                tp("t1", 1),
                tp("t2", 0),
                tp("t2", 1),
            ]
            .into_iter()
            .collect(),
        );
        assert_eq!(builder.oldest_allowed_version(), 9);
    }

    /// Mirrors `testHandleSuccessfulResponse`.
    #[test]
    fn handle_successful_response() {
        let result = handle(&HashMap::new());
        assert_result(
            &result,
            offset_timestamps().into_keys().collect(),
            HashSet::new(),
            Vec::new(),
            HashSet::new(),
        );
    }

    /// Mirrors `testHandleRetriablePartitionTimeoutResponse`.
    #[test]
    fn handle_retriable_partition_timeout_response() {
        let error_partition = tp("t0", 0);
        let errors: HashMap<TopicPartition, i16> = [(error_partition.clone(), Errors::RequestTimedOut.code())]
            .into_iter()
            .collect();
        let result = handle(&errors);
        let retriable: HashSet<TopicPartition> = [error_partition.clone()].into_iter().collect();
        let mut completed: HashSet<TopicPartition> = offset_timestamps().into_keys().collect();
        completed.remove(&error_partition);
        assert_result(&result, completed, HashSet::new(), Vec::new(), retriable);
    }

    /// Mirrors `testHandleLookupRetriablePartitionInvalidMetadataResponse`.
    #[test]
    fn handle_lookup_retriable_partition_invalid_metadata_response() {
        let error_partition = tp("t0", 0);
        let errors: HashMap<TopicPartition, i16> = [(error_partition.clone(), Errors::NotLeaderOrFollower.code())]
            .into_iter()
            .collect();
        let result = handle(&errors);
        let unmapped = vec![error_partition.clone()];
        let mut completed: HashSet<TopicPartition> = offset_timestamps().into_keys().collect();
        completed.remove(&error_partition);
        assert_result(&result, completed, HashSet::new(), unmapped, HashSet::new());
    }

    /// Mirrors `testHandleUnexpectedPartitionErrorResponse`.
    #[test]
    fn handle_unexpected_partition_error_response() {
        let error_partition = tp("t0", 0);
        let errors: HashMap<TopicPartition, i16> = [(error_partition.clone(), Errors::UnknownServerError.code())]
            .into_iter()
            .collect();
        let result = handle(&errors);
        let failed: HashSet<TopicPartition> = [error_partition.clone()].into_iter().collect();
        let mut completed: HashSet<TopicPartition> = offset_timestamps().into_keys().collect();
        completed.remove(&error_partition);
        assert_result(&result, completed, failed, Vec::new(), HashSet::new());
    }

    /// Mirrors `testHandleResponseSanityCheck`.
    #[test]
    fn handle_response_sanity_check() {
        let error_partition = tp("t0", 0);
        let mut specs = offset_timestamps();
        specs.remove(&error_partition);
        let result = handle_with_specs(&HashMap::new(), &specs);
        assert_eq!(result.completed_keys.len(), offset_timestamps().len() - 1);
        assert_eq!(result.failed_keys.len(), 1);
        let (failed_key, failed_err) = result.failed_keys.iter().next().unwrap();
        assert_eq!(failed_key, &error_partition);
        assert!(failed_err.message().contains("did not contain a result for topic partition"));
        assert!(result.unmapped_keys.is_empty());
    }

    /// Mirrors `testHandleResponseUnsupportedVersion`.
    #[test]
    fn handle_response_unsupported_version() {
        let broker_id = 1;
        let uve = Error::unsupported_version("");
        let handler = handler(ListOffsetsOptions::new());
        let max_timestamp_partitions: HashSet<TopicPartition> = [tp("t1", 1)].into_iter().collect();
        let all_keys: HashSet<TopicPartition> = offset_timestamps().into_keys().collect();
        let non_max: HashSet<TopicPartition> = all_keys.difference(&max_timestamp_partitions).cloned().collect();

        // Cannot be handled if there is no partition with a MAX_TIMESTAMP spec.
        let result = handler.handle_unsupported_version_exception(broker_id, &uve, &non_max);
        assert_eq!(result.keys().cloned().collect::<HashSet<_>>(), non_max);

        // Cannot be handled if there are only MAX_TIMESTAMP partitions.
        let result = handler.handle_unsupported_version_exception(broker_id, &uve, &max_timestamp_partitions);
        assert_eq!(result.keys().cloned().collect::<HashSet<_>>(), max_timestamp_partitions);

        // A mix can be handled: only the MAX_TIMESTAMP partitions are failed.
        let result = handler.handle_unsupported_version_exception(broker_id, &uve, &all_keys);
        assert_eq!(result.keys().cloned().collect::<HashSet<_>>(), max_timestamp_partitions);
    }

    /// Mirrors `testBuildRequestWithDefaultApiTimeoutMs`.
    #[test]
    fn build_request_with_default_api_timeout_ms() {
        let handler = handler(ListOffsetsOptions::new());
        let keys: HashSet<TopicPartition> = [tp("t0", 0), tp("t0", 1)].into_iter().collect();
        let builder = handler.build_batched_request(node().id(), &keys);
        assert_eq!(builder.data().timeout_ms, DEFAULT_API_TIMEOUT_MS);
    }

    /// Mirrors `testBuildRequestWithTimeoutMs`.
    #[test]
    fn build_request_with_timeout_ms() {
        let handler = handler(ListOffsetsOptions::new().timeout_ms(Some(200)));
        let keys: HashSet<TopicPartition> = [tp("t0", 0), tp("t0", 1)].into_iter().collect();
        let builder = handler.build_batched_request(node().id(), &keys);
        assert_eq!(builder.data().timeout_ms, 200);
    }
}
