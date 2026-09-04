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

//! Handler for the `abortTransaction` API (partition-leader targeted, uses
//! `WriteTxnMarkers`).
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.AbortTransactionHandler`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::admin::abort_transaction_spec::AbortTransactionSpec;
use crate::common::protocol::Errors;
use crate::common::requests::{ConcreteResponse, RequestBuilder, WriteTxnMarkersRequestBuilder};
use crate::common::utils::LogContext;
use crate::common::{Error, Node, TopicPartition};
use crate::kafka_error;
use crate::write_txn_markers_request_data::{WritableTxnMarker, WritableTxnMarkerTopic, WriteTxnMarkersRequestData};

use super::admin_api_handler::{AdminApiHandler, ApiResult, RequestAndKeys};
use super::admin_api_lookup_strategy::AdminApiLookupStrategy;
use super::partition_leader_cache::PartitionLeaderCache;
use super::partition_leader_strategy::{PartitionLeaderFuture, PartitionLeaderStrategy};

/// Handler for `abortTransaction`.
///
/// Corresponds to `AbortTransactionHandler` (a `Batched` handler over
/// `TopicPartition` keys yielding `()` values).
pub(crate) struct AbortTransactionHandler {
    log_context: LogContext,
    abort_spec: AbortTransactionSpec,
    lookup_strategy: PartitionLeaderStrategy,
}

impl AbortTransactionHandler {
    /// Creates a handler for the given abort spec.
    pub(crate) fn new(abort_spec: AbortTransactionSpec, log_context: LogContext) -> Self {
        let lookup_strategy = PartitionLeaderStrategy::new(log_context.clone());
        Self { log_context, abort_spec, lookup_strategy }
    }

    /// Creates the future bundle that the driver completes when the abort
    /// completes.
    ///
    /// Mirrors `AbortTransactionHandler.newFuture`.
    pub(crate) fn new_future(
        topic_partitions: HashSet<TopicPartition>,
        partition_leader_cache: Arc<PartitionLeaderCache>,
    ) -> PartitionLeaderFuture<()> {
        PartitionLeaderFuture::new(topic_partitions, partition_leader_cache)
    }

    /// Builds the `WriteTxnMarkers` request for the abort spec's partition.
    ///
    /// Mirrors `buildBatchedRequest`.
    fn build_batched_request(&self, topic_partitions: &HashSet<TopicPartition>) -> WriteTxnMarkersRequestData {
        self.validate_topic_partitions(topic_partitions);

        let mut topic = WritableTxnMarkerTopic::new();
        topic.set_name(self.abort_spec.topic_partition().topic().to_string());
        topic.set_partition_indexes(vec![self.abort_spec.topic_partition().partition()]);

        let mut marker = WritableTxnMarker::new();
        marker.set_coordinator_epoch(self.abort_spec.coordinator_epoch());
        marker.set_producer_epoch(self.abort_spec.producer_epoch());
        marker.set_producer_id(self.abort_spec.producer_id());
        marker.set_transaction_result(false);
        marker.set_topics(vec![topic]);

        let mut data = WriteTxnMarkersRequestData::new();
        data.set_markers(vec![marker]);
        data
    }

    /// Classifies a partition-level error into failed (fatal) or unmapped
    /// (retry via a fresh leader lookup).
    ///
    /// Mirrors `handleError`.
    fn handle_error(&self, error: Errors) -> ApiResult<TopicPartition, ()> {
        let tp = self.abort_spec.topic_partition().clone();
        match error {
            Errors::ClusterAuthorizationFailed => {
                kafka_error!(
                    self.log_context,
                    "WriteTxnMarkers request for abort spec {} failed cluster authorization",
                    self.abort_spec
                );
                failed(
                    tp,
                    Error::with_message(
                        error,
                        format!(
                            "WriteTxnMarkers request with {} failed due to cluster authorization error",
                            self.abort_spec
                        ),
                    ),
                )
            },
            Errors::InvalidProducerEpoch => {
                kafka_error!(
                    self.log_context,
                    "WriteTxnMarkers request for abort spec {} failed due to an invalid producer epoch",
                    self.abort_spec
                );
                failed(
                    tp,
                    Error::with_message(
                        error,
                        format!(
                            "WriteTxnMarkers request with {} failed due an invalid producer epoch",
                            self.abort_spec
                        ),
                    ),
                )
            },
            Errors::TransactionCoordinatorFenced => {
                kafka_error!(
                    self.log_context,
                    "WriteTxnMarkers request for abort spec {} failed because the coordinator epoch is fenced",
                    self.abort_spec
                );
                failed(
                    tp,
                    Error::with_message(
                        error,
                        format!(
                            "WriteTxnMarkers request with {} failed since the provided coordinator epoch {} has been fenced by the active coordinator",
                            self.abort_spec,
                            self.abort_spec.coordinator_epoch()
                        ),
                    ),
                )
            },
            Errors::NotLeaderOrFollower
            | Errors::ReplicaNotAvailable
            | Errors::BrokerNotAvailable
            | Errors::UnknownTopicOrPartition => {
                crate::kafka_debug!(
                    self.log_context,
                    "WriteTxnMarkers request for abort spec {} failed due to {:?}. Will retry after attempting to find the leader again",
                    self.abort_spec,
                    error
                );
                unmapped(tp)
            },
            _ => {
                kafka_error!(
                    self.log_context,
                    "WriteTxnMarkers request for abort spec {} failed due to an unexpected error {:?}",
                    self.abort_spec,
                    error
                );
                failed(
                    tp,
                    Error::with_message(
                        error,
                        format!(
                            "WriteTxnMarkers request with {} failed due to unexpected error: {}",
                            self.abort_spec,
                            error.message()
                        ),
                    ),
                )
            },
        }
    }

    /// Mirrors `validateTopicPartitions`. Panics (mirroring Java's
    /// `IllegalArgumentException`) when the driver hands over a key set other
    /// than the single partition of the abort spec — a programming error the
    /// driver never triggers in production.
    fn validate_topic_partitions(&self, topic_partitions: &HashSet<TopicPartition>) {
        let expected: HashSet<TopicPartition> = HashSet::from([self.abort_spec.topic_partition().clone()]);
        if topic_partitions != &expected {
            panic!("Received unexpected topic partitions {topic_partitions:?} (expected only {expected:?})");
        }
    }
}

/// Mirrors `ApiResult.failed(key, error)`.
fn failed(tp: TopicPartition, error: Error) -> ApiResult<TopicPartition, ()> {
    ApiResult::new(HashMap::new(), HashMap::from([(tp, error)]), Vec::new())
}

/// Mirrors `ApiResult.unmapped(singletonList(key))`.
fn unmapped(tp: TopicPartition) -> ApiResult<TopicPartition, ()> {
    ApiResult::new(HashMap::new(), HashMap::new(), vec![tp])
}

/// Mirrors `ApiResult.completed(key, null)`.
fn completed(tp: TopicPartition) -> ApiResult<TopicPartition, ()> {
    ApiResult::new(HashMap::from([(tp, ())]), HashMap::new(), Vec::new())
}

/// A bare Java `KafkaException` (no error code). Rust's [`Error`] always
/// carries an [`Errors`] code, so the neutral `UnknownServerError` code is used
/// while the message preserves the Java text.
fn bare_kafka_error(message: String) -> Error {
    Error::kafka_message(message)
}

impl AdminApiHandler<TopicPartition, ()> for AbortTransactionHandler {
    fn api_name(&self) -> &str {
        "abortTransaction"
    }

    fn build_request(&self, _broker_id: i32, keys: &HashSet<TopicPartition>) -> Vec<RequestAndKeys<TopicPartition>> {
        let data = self.build_batched_request(keys);
        vec![RequestAndKeys {
            request: Box::new(WriteTxnMarkersRequestBuilder::new(data)) as Box<dyn RequestBuilder>,
            keys: keys.clone(),
        }]
    }

    fn handle_response(
        &self,
        _broker: &Node,
        topic_partitions: &HashSet<TopicPartition>,
        response: &ConcreteResponse,
    ) -> ApiResult<TopicPartition, ()> {
        self.validate_topic_partitions(topic_partitions);

        let ConcreteResponse::WriteTxnMarkers(response) = response else {
            return failed(
                self.abort_spec.topic_partition().clone(),
                bare_kafka_error("WriteTxnMarkers response was of an unexpected type".to_string()),
            );
        };
        let marker_responses = &response.data().markers;

        if marker_responses.len() != 1 || marker_responses[0].producer_id != self.abort_spec.producer_id() {
            return failed(
                self.abort_spec.topic_partition().clone(),
                bare_kafka_error(format!(
                    "WriteTxnMarkers response included unexpected marker entries: {marker_responses:?}(expected to find exactly one entry with producerId {})",
                    self.abort_spec.producer_id()
                )),
            );
        }

        let marker_response = &marker_responses[0];
        let topic_responses = &marker_response.topics;

        if topic_responses.len() != 1 || topic_responses[0].name != self.abort_spec.topic_partition().topic() {
            return failed(
                self.abort_spec.topic_partition().clone(),
                bare_kafka_error(format!(
                    "WriteTxnMarkers response included unexpected topic entries: {marker_responses:?}(expected to find exactly one entry with topic partition {})",
                    self.abort_spec.topic_partition()
                )),
            );
        }

        let topic_response = &topic_responses[0];
        let partition_responses = &topic_response.partitions;

        if partition_responses.len() != 1
            || partition_responses[0].partition_index != self.abort_spec.topic_partition().partition()
        {
            return failed(
                self.abort_spec.topic_partition().clone(),
                bare_kafka_error(format!(
                    "WriteTxnMarkers response included unexpected partition entries for topic {}: {marker_responses:?}(expected to find exactly one entry with partition {})",
                    self.abort_spec.topic_partition().topic(),
                    self.abort_spec.topic_partition().partition()
                )),
            );
        }

        let partition_response = &partition_responses[0];
        let error = Errors::for_code(partition_response.error_code);

        if error != Errors::None {
            self.handle_error(error)
        } else {
            completed(self.abort_spec.topic_partition().clone())
        }
    }

    fn lookup_strategy(&self) -> &dyn AdminApiLookupStrategy<TopicPartition> {
        &self.lookup_strategy
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::requests::WriteTxnMarkersResponse;
    use crate::write_txn_markers_response_data::{
        WritableTxnMarkerPartitionResult, WritableTxnMarkerResult, WritableTxnMarkerTopicResult,
        WriteTxnMarkersResponseData,
    };

    fn log_context() -> LogContext {
        LogContext::new("[test] ")
    }

    fn topic_partition() -> TopicPartition {
        TopicPartition::new("foo", 5)
    }

    fn abort_spec() -> AbortTransactionSpec {
        AbortTransactionSpec::new(topic_partition(), 12345, 15, 4321)
    }

    fn node() -> Node {
        Node::new(1, "host".to_string(), 1234)
    }

    fn write_txn_markers_response(data: WriteTxnMarkersResponseData) -> ConcreteResponse {
        ConcreteResponse::WriteTxnMarkers(WriteTxnMarkersResponse::new(data))
    }

    // Mirrors `AbortTransactionHandlerTest.testInvalidBuildRequestCall`.
    #[test]
    fn invalid_build_request_call() {
        let handler = AbortTransactionHandler::new(abort_spec(), log_context());
        for keys in [
            HashSet::new(),
            HashSet::from([TopicPartition::new("foo", 1)]),
            HashSet::from([topic_partition(), TopicPartition::new("foo", 1)]),
        ] {
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler.build_batched_request(&keys)));
            // Java throws `IllegalArgumentException` here.
            assert!(result.is_err(), "expected an illegal-argument error for keys {keys:?}");
        }
    }

    // Mirrors `AbortTransactionHandlerTest.testValidBuildRequestCall`.
    #[test]
    fn valid_build_request_call() {
        let handler = AbortTransactionHandler::new(abort_spec(), log_context());
        let data = handler.build_batched_request(&HashSet::from([topic_partition()]));
        assert_eq!(data.markers.len(), 1);
        let marker = &data.markers[0];
        assert_eq!(marker.producer_id, abort_spec().producer_id());
        assert_eq!(marker.producer_epoch, abort_spec().producer_epoch());
        assert_eq!(marker.coordinator_epoch, abort_spec().coordinator_epoch());
        assert_eq!(marker.topics.len(), 1);
        assert_eq!(marker.topics[0].name, abort_spec().topic_partition().topic());
        assert_eq!(
            marker.topics[0].partition_indexes,
            vec![abort_spec().topic_partition().partition()]
        );
    }

    // Mirrors `AbortTransactionHandlerTest.testInvalidHandleResponseCall`.
    #[test]
    fn invalid_handle_response_call() {
        let handler = AbortTransactionHandler::new(abort_spec(), log_context());
        let response = write_txn_markers_response(WriteTxnMarkersResponseData::new());
        for keys in [
            HashSet::new(),
            HashSet::from([TopicPartition::new("foo", 1)]),
            HashSet::from([topic_partition(), TopicPartition::new("foo", 1)]),
        ] {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                handler.handle_response(&node(), &keys, &response)
            }));
            // Java throws `IllegalArgumentException` here.
            assert!(result.is_err(), "expected an illegal-argument error for keys {keys:?}");
        }
    }

    fn assert_failed(tp: &TopicPartition, result: &ApiResult<TopicPartition, ()>) {
        assert!(result.completed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
        assert_eq!(
            result.failed_keys.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from([tp.clone()])
        );
    }

    // Mirrors `AbortTransactionHandlerTest.testInvalidResponse`: each malformed
    // response shape fails the partition with a `KafkaException`.
    #[test]
    fn invalid_response() {
        let handler = AbortTransactionHandler::new(abort_spec(), log_context());
        let tp = topic_partition();

        // Empty markers.
        let mut data = WriteTxnMarkersResponseData::new();
        assert_failed(
            &tp,
            &handler.handle_response(&node(), &HashSet::from([tp.clone()]), &write_txn_markers_response(data.clone())),
        );

        // A marker with the wrong producer id (default 0 != 12345).
        let mut marker = WritableTxnMarkerResult::new();
        data.markers.push(marker.clone());
        assert_failed(
            &tp,
            &handler.handle_response(&node(), &HashSet::from([tp.clone()]), &write_txn_markers_response(data.clone())),
        );

        // Correct producer id, but no topics.
        marker.set_producer_id(abort_spec().producer_id());
        data.markers = vec![marker.clone()];
        assert_failed(
            &tp,
            &handler.handle_response(&node(), &HashSet::from([tp.clone()]), &write_txn_markers_response(data.clone())),
        );

        // A topic entry with the wrong (default empty) name.
        let mut topic = WritableTxnMarkerTopicResult::new();
        marker.topics.push(topic.clone());
        data.markers = vec![marker.clone()];
        assert_failed(
            &tp,
            &handler.handle_response(&node(), &HashSet::from([tp.clone()]), &write_txn_markers_response(data.clone())),
        );

        // Correct topic name, but no partitions.
        topic.set_name(abort_spec().topic_partition().topic().to_string());
        marker.topics = vec![topic.clone()];
        data.markers = vec![marker.clone()];
        assert_failed(
            &tp,
            &handler.handle_response(&node(), &HashSet::from([tp.clone()]), &write_txn_markers_response(data.clone())),
        );

        // A partition entry, correct index but a mismatched topic name.
        let mut partition = WritableTxnMarkerPartitionResult::new();
        topic.partitions.push(partition.clone());
        partition.set_partition_index(abort_spec().topic_partition().partition());
        topic.partitions = vec![partition.clone()];
        topic.set_name(format!("{}random", abort_spec().topic_partition().topic()));
        marker.topics = vec![topic.clone()];
        data.markers = vec![marker.clone()];
        assert_failed(
            &tp,
            &handler.handle_response(&node(), &HashSet::from([tp.clone()]), &write_txn_markers_response(data.clone())),
        );

        // Correct topic name, but a mismatched producer id.
        topic.set_name(abort_spec().topic_partition().topic().to_string());
        marker.topics = vec![topic.clone()];
        marker.set_producer_id(abort_spec().producer_id() + 1);
        data.markers = vec![marker];
        assert_failed(
            &tp,
            &handler.handle_response(&node(), &HashSet::from([tp.clone()]), &write_txn_markers_response(data)),
        );
    }

    fn handle_with_error(error: Errors) -> ApiResult<TopicPartition, ()> {
        let handler = AbortTransactionHandler::new(abort_spec(), log_context());
        let mut partition = WritableTxnMarkerPartitionResult::new();
        partition.set_partition_index(abort_spec().topic_partition().partition());
        partition.set_error_code(error.code());
        let mut topic = WritableTxnMarkerTopicResult::new();
        topic.set_name(abort_spec().topic_partition().topic().to_string());
        topic.set_partitions(vec![partition]);
        let mut marker = WritableTxnMarkerResult::new();
        marker.set_producer_id(abort_spec().producer_id());
        marker.set_topics(vec![topic]);
        let mut data = WriteTxnMarkersResponseData::new();
        data.set_markers(vec![marker]);
        handler.handle_response(
            &node(),
            &HashSet::from([abort_spec().topic_partition().clone()]),
            &write_txn_markers_response(data),
        )
    }

    // Mirrors `AbortTransactionHandlerTest.testSuccessfulResponse`.
    #[test]
    fn successful_response() {
        let result = handle_with_error(Errors::None);
        assert!(result.failed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
        assert_eq!(
            result.completed_keys.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from([topic_partition()])
        );
    }

    // Mirrors `AbortTransactionHandlerTest.testRetriableErrors`.
    #[test]
    fn retriable_errors() {
        for error in [
            Errors::NotLeaderOrFollower,
            Errors::UnknownTopicOrPartition,
            Errors::ReplicaNotAvailable,
            Errors::BrokerNotAvailable,
        ] {
            let result = handle_with_error(error);
            assert!(result.completed_keys.is_empty());
            assert!(result.failed_keys.is_empty());
            assert_eq!(result.unmapped_keys, vec![topic_partition()]);
        }
    }

    // Mirrors `AbortTransactionHandlerTest.testFatalErrors`.
    #[test]
    fn fatal_errors() {
        for error in [
            Errors::ClusterAuthorizationFailed,
            Errors::InvalidProducerEpoch,
            Errors::TransactionCoordinatorFenced,
            Errors::UnknownServerError,
        ] {
            let result = handle_with_error(error);
            assert_failed(&topic_partition(), &result);
            assert_eq!(result.failed_keys.get(&topic_partition()).unwrap().error(), error);
        }
    }
}
