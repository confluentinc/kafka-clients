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

//! Handler for the `describeProducers` API (partition-leader targeted, or a
//! static broker when `brokerId` is set in the options).
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.DescribeProducersHandler`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::admin::describe_producers_result::PartitionProducerState;
use crate::admin::options::DescribeProducersOptions;
use crate::admin::producer_state::ProducerState;
use crate::common::protocol::Errors;
use crate::common::requests::{ConcreteResponse, DescribeProducersRequestBuilder, RequestBuilder};
use crate::common::utils::LogContext;
use crate::common::{Error, Node, TopicPartition};
use crate::describe_producers_request_data::{DescribeProducersRequestData, TopicRequest};
use crate::{kafka_debug, kafka_error};

use super::admin_api_handler::{AdminApiHandler, ApiResult, RequestAndKeys};
use super::admin_api_lookup_strategy::AdminApiLookupStrategy;
use super::partition_leader_cache::PartitionLeaderCache;
use super::partition_leader_strategy::{PartitionLeaderFuture, PartitionLeaderStrategy};
use super::static_broker_strategy::StaticBrokerStrategy;

/// Handler for `describeProducers`.
///
/// Corresponds to `DescribeProducersHandler` (a `Batched` handler over
/// `TopicPartition` keys yielding [`PartitionProducerState`] values).
pub(crate) struct DescribeProducersHandler {
    log_context: LogContext,
    options: DescribeProducersOptions,
    lookup_strategy: Box<dyn AdminApiLookupStrategy<TopicPartition>>,
}

impl DescribeProducersHandler {
    /// Creates a handler. When `options.broker_id` is set, a
    /// [`StaticBrokerStrategy`] targets that broker directly; otherwise a
    /// [`PartitionLeaderStrategy`] looks up each partition's leader.
    pub(crate) fn new(options: DescribeProducersOptions, log_context: LogContext) -> Self {
        let lookup_strategy: Box<dyn AdminApiLookupStrategy<TopicPartition>> = match options.broker_id_opt() {
            Some(broker_id) => Box::new(StaticBrokerStrategy::new(broker_id)),
            None => Box::new(PartitionLeaderStrategy::new(log_context.clone())),
        };
        Self { log_context, options, lookup_strategy }
    }

    /// Creates the future bundle that the driver completes as partitions are
    /// resolved.
    ///
    /// Mirrors `DescribeProducersHandler.newFuture`.
    pub(crate) fn new_future(
        topic_partitions: HashSet<TopicPartition>,
        partition_leader_cache: Arc<PartitionLeaderCache>,
    ) -> PartitionLeaderFuture<PartitionProducerState> {
        PartitionLeaderFuture::new(topic_partitions, partition_leader_cache)
    }

    /// Builds a single batched `DescribeProducers` request for the given keys.
    ///
    /// Mirrors `buildBatchedRequest`.
    fn build_batched_request(&self, topic_partitions: &HashSet<TopicPartition>) -> DescribeProducersRequestData {
        let mut topics: HashMap<String, TopicRequest> = HashMap::new();
        for tp in topic_partitions {
            let topic = topics.entry(tp.topic().to_string()).or_insert_with(|| {
                let mut t = TopicRequest::new();
                t.set_name(tp.topic().to_string());
                t
            });
            topic.partition_indexes.push(tp.partition());
        }
        let mut data = DescribeProducersRequestData::new();
        data.set_topics(topics.into_values().collect());
        data
    }

    /// Classifies a partition-level error into failed (fatal) or unmapped
    /// (retry via a fresh leader lookup); retriable errors are left out of the
    /// result so the driver retries.
    ///
    /// Mirrors `handlePartitionError`.
    fn handle_partition_error(
        &self,
        topic_partition: &TopicPartition,
        error: Errors,
        failed: &mut HashMap<TopicPartition, Error>,
        unmapped: &mut Vec<TopicPartition>,
    ) {
        match error {
            Errors::NotLeaderOrFollower => {
                if let Some(broker_id) = self.options.broker_id_opt() {
                    // Typically these errors are retriable, but if the user
                    // specified the brokerId explicitly, then they are fatal.
                    kafka_error!(
                        self.log_context,
                        "Not leader error in `DescribeProducers` response for partition {} for brokerId {} set in options",
                        topic_partition,
                        broker_id
                    );
                    failed.insert(
                        topic_partition.clone(),
                        Error::with_message(
                            error,
                            format!(
                                "Failed to describe active producers for partition {topic_partition} on brokerId {broker_id}"
                            ),
                        ),
                    );
                } else {
                    // Otherwise, we unmap the partition so that we can find the new leader.
                    kafka_debug!(
                        self.log_context,
                        "Not leader error in `DescribeProducers` response for partition {}. Will retry later.",
                        topic_partition
                    );
                    unmapped.push(topic_partition.clone());
                }
            },
            Errors::UnknownTopicOrPartition => {
                kafka_debug!(
                    self.log_context,
                    "Unknown topic/partition error in `DescribeProducers` response for partition {}. Will retry later.",
                    topic_partition
                );
            },
            Errors::InvalidTopicException => {
                kafka_error!(
                    self.log_context,
                    "Invalid topic in `DescribeProducers` response for partition {}",
                    topic_partition
                );
                failed.insert(
                    topic_partition.clone(),
                    Error::invalid_topics(HashSet::from([topic_partition.topic().to_string()])),
                );
            },
            Errors::TopicAuthorizationFailed => {
                kafka_error!(
                    self.log_context,
                    "Authorization failed in `DescribeProducers` response for partition {}",
                    topic_partition
                );
                failed.insert(
                    topic_partition.clone(),
                    Error::topic_authorization(HashSet::from([topic_partition.topic().to_string()])),
                );
            },
            _ => {
                kafka_error!(
                    self.log_context,
                    "Unexpected error in `DescribeProducers` response for partition {}",
                    topic_partition
                );
                failed.insert(
                    topic_partition.clone(),
                    Error::with_message(
                        error,
                        format!("Failed to describe active producers for partition {topic_partition} due to unexpected error"),
                    ),
                );
            },
        }
    }
}

impl AdminApiHandler<TopicPartition, PartitionProducerState> for DescribeProducersHandler {
    fn api_name(&self) -> &str {
        "describeProducers"
    }

    fn build_request(&self, _broker_id: i32, keys: &HashSet<TopicPartition>) -> Vec<RequestAndKeys<TopicPartition>> {
        let data = self.build_batched_request(keys);
        vec![RequestAndKeys {
            request: Box::new(DescribeProducersRequestBuilder::new(data)) as Box<dyn RequestBuilder>,
            keys: keys.clone(),
        }]
    }

    fn handle_response(
        &self,
        _broker: &Node,
        _keys: &HashSet<TopicPartition>,
        response: &ConcreteResponse,
    ) -> ApiResult<TopicPartition, PartitionProducerState> {
        let ConcreteResponse::DescribeProducers(response) = response else {
            return ApiResult::new(HashMap::new(), HashMap::new(), Vec::new());
        };
        let mut completed: HashMap<TopicPartition, PartitionProducerState> = HashMap::new();
        let mut failed: HashMap<TopicPartition, Error> = HashMap::new();
        let mut unmapped: Vec<TopicPartition> = Vec::new();

        for topic_response in &response.data().topics {
            for partition_response in &topic_response.partitions {
                let topic_partition =
                    TopicPartition::new(topic_response.name.as_str(), partition_response.partition_index);
                let error = Errors::for_code(partition_response.error_code);
                if error != Errors::None {
                    self.handle_partition_error(&topic_partition, error, &mut failed, &mut unmapped);
                    continue;
                }

                let active_producers: Vec<ProducerState> = partition_response
                    .active_producers
                    .iter()
                    .map(|active_producer| {
                        let current_transaction_first_offset = if active_producer.current_txn_start_offset < 0 {
                            None
                        } else {
                            Some(active_producer.current_txn_start_offset)
                        };
                        let coordinator_epoch = if active_producer.coordinator_epoch < 0 {
                            None
                        } else {
                            Some(active_producer.coordinator_epoch)
                        };
                        ProducerState::new(
                            active_producer.producer_id,
                            active_producer.producer_epoch,
                            active_producer.last_sequence,
                            active_producer.last_timestamp,
                            coordinator_epoch,
                            current_transaction_first_offset,
                        )
                    })
                    .collect();

                completed.insert(topic_partition, PartitionProducerState::new(active_producers));
            }
        }

        ApiResult::new(completed, failed, unmapped)
    }

    fn lookup_strategy(&self) -> &dyn AdminApiLookupStrategy<TopicPartition> {
        &*self.lookup_strategy
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::requests::DescribeProducersResponse;
    use crate::describe_producers_response_data::{
        DescribeProducersResponseData, PartitionResponse, ProducerState as WireProducerState, TopicResponse,
    };

    fn new_handler(options: DescribeProducersOptions) -> DescribeProducersHandler {
        DescribeProducersHandler::new(options, LogContext::new("[test] "))
    }

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic, partition)
    }

    fn describe_producers_response(
        topic_partition: &TopicPartition,
        partition_response: PartitionResponse,
    ) -> ConcreteResponse {
        let mut topic_response = TopicResponse::new();
        topic_response.set_name(topic_partition.topic().to_string());
        topic_response.set_partitions(vec![partition_response]);
        let mut data = DescribeProducersResponseData::new();
        data.set_topics(vec![topic_response]);
        ConcreteResponse::DescribeProducers(DescribeProducersResponse::new(data))
    }

    fn handle_response_with_error(
        options: DescribeProducersOptions,
        topic_partition: &TopicPartition,
        error: Errors,
    ) -> ApiResult<TopicPartition, PartitionProducerState> {
        let handler = new_handler(options);
        let mut partition_response = PartitionResponse::new();
        partition_response.set_partition_index(topic_partition.partition());
        partition_response.set_error_code(error.code());
        let response = describe_producers_response(topic_partition, partition_response);
        let node = Node::new(3, "host".to_string(), 1);
        handler.handle_response(&node, &HashSet::from([topic_partition.clone()]), &response)
    }

    // Mirrors `DescribeProducersHandlerTest.testBrokerIdSetInOptions`.
    #[test]
    fn broker_id_set_in_options() {
        let broker_id = 3;
        let handler = new_handler(DescribeProducersOptions::new().broker_id(broker_id));
        for tp in [tp("foo", 5), tp("bar", 3), tp("foo", 4)] {
            let scope = handler.lookup_strategy().lookup_scope(&tp);
            assert_eq!(scope.destination_broker_id(), Some(broker_id), "Unexpected brokerId for {tp}");
        }
    }

    // Mirrors `DescribeProducersHandlerTest.testBrokerIdNotSetInOptions`.
    #[test]
    fn broker_id_not_set_in_options() {
        let handler = new_handler(DescribeProducersOptions::new());
        for tp in [tp("foo", 5), tp("bar", 3), tp("foo", 4)] {
            let scope = handler.lookup_strategy().lookup_scope(&tp);
            assert_eq!(scope.destination_broker_id(), None, "Unexpected brokerId for {tp}");
        }
    }

    // Mirrors `DescribeProducersHandlerTest.testBuildRequest`.
    #[test]
    fn build_request() {
        let handler = new_handler(DescribeProducersOptions::new());
        let keys = HashSet::from([tp("foo", 5), tp("bar", 3), tp("foo", 4)]);
        let requests = handler.build_request(3, &keys);
        assert_eq!(requests.len(), 1);
        let data = handler.build_batched_request(&keys);
        let topic_names: HashSet<String> = data.topics.iter().map(|t| t.name.clone()).collect();
        assert_eq!(topic_names, HashSet::from(["foo".to_string(), "bar".to_string()]));
        for topic in &data.topics {
            let expected: HashSet<i32> = if topic.name == "foo" {
                HashSet::from([4, 5])
            } else {
                HashSet::from([3])
            };
            assert_eq!(topic.partition_indexes.iter().copied().collect::<HashSet<_>>(), expected);
        }
    }

    // Mirrors `DescribeProducersHandlerTest.testAuthorizationFailure`.
    #[test]
    fn authorization_failure() {
        let topic_partition = tp("foo", 5);
        let result = handle_response_with_error(
            DescribeProducersOptions::new(),
            &topic_partition,
            Errors::TopicAuthorizationFailed,
        );
        assert!(result.completed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
        assert_eq!(
            result.failed_keys.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from([topic_partition.clone()])
        );
        match result.failed_keys.get(&topic_partition).unwrap() {
            Error::TopicAuthorization(e) => assert_eq!(e.unauthorized_topics, HashSet::from(["foo".to_string()])),
            other => panic!("expected TopicAuthorization, got {other:?}"),
        }
    }

    // Mirrors `DescribeProducersHandlerTest.testInvalidTopic`.
    #[test]
    fn invalid_topic() {
        let topic_partition = tp("foo", 5);
        let result = handle_response_with_error(
            DescribeProducersOptions::new(),
            &topic_partition,
            Errors::InvalidTopicException,
        );
        match result.failed_keys.get(&topic_partition).unwrap() {
            Error::InvalidTopic(e) => assert_eq!(e.invalid_topics, HashSet::from(["foo".to_string()])),
            other => panic!("expected InvalidTopic, got {other:?}"),
        }
    }

    // Mirrors `DescribeProducersHandlerTest.testUnexpectedError`.
    #[test]
    fn unexpected_error() {
        let topic_partition = tp("foo", 5);
        let result =
            handle_response_with_error(DescribeProducersOptions::new(), &topic_partition, Errors::UnknownServerError);
        assert_eq!(
            result.failed_keys.get(&topic_partition).unwrap().error(),
            Errors::UnknownServerError
        );
    }

    // Mirrors `DescribeProducersHandlerTest.testRetriableErrors`.
    #[test]
    fn retriable_errors() {
        let topic_partition = tp("foo", 5);
        let result = handle_response_with_error(
            DescribeProducersOptions::new(),
            &topic_partition,
            Errors::UnknownTopicOrPartition,
        );
        assert!(result.failed_keys.is_empty());
        assert!(result.completed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
    }

    // Mirrors `DescribeProducersHandlerTest.testUnmappedAfterNotLeaderError`.
    #[test]
    fn unmapped_after_not_leader_error() {
        let topic_partition = tp("foo", 5);
        let result =
            handle_response_with_error(DescribeProducersOptions::new(), &topic_partition, Errors::NotLeaderOrFollower);
        assert!(result.failed_keys.is_empty());
        assert!(result.completed_keys.is_empty());
        assert_eq!(result.unmapped_keys, vec![topic_partition]);
    }

    // Mirrors `DescribeProducersHandlerTest.testFatalNotLeaderErrorIfStaticMapped`.
    #[test]
    fn fatal_not_leader_error_if_static_mapped() {
        let topic_partition = tp("foo", 5);
        let options = DescribeProducersOptions::new().broker_id(1);
        let result = handle_response_with_error(options, &topic_partition, Errors::NotLeaderOrFollower);
        assert!(result.completed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
        assert_eq!(
            result.failed_keys.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from([topic_partition.clone()])
        );
        assert_eq!(
            result.failed_keys.get(&topic_partition).unwrap().error(),
            Errors::NotLeaderOrFollower
        );
    }

    // Mirrors `DescribeProducersHandlerTest.testCompletedResult`.
    #[test]
    fn completed_result() {
        let topic_partition = tp("foo", 5);
        let options = DescribeProducersOptions::new().broker_id(1);
        let handler = new_handler(options);

        let mut wire0 = WireProducerState::new();
        wire0.set_producer_id(12345);
        wire0.set_producer_epoch(15);
        wire0.set_last_sequence(75);
        wire0.set_last_timestamp(1_600_000_000_000);
        wire0.set_current_txn_start_offset(-1);
        wire0.set_coordinator_epoch(-1);
        let mut wire1 = WireProducerState::new();
        wire1.set_producer_id(98765);
        wire1.set_producer_epoch(30);
        wire1.set_last_sequence(150);
        wire1.set_last_timestamp(1_599_999_995_000);
        wire1.set_current_txn_start_offset(5000);
        wire1.set_coordinator_epoch(-1);

        let mut partition_response = PartitionResponse::new();
        partition_response.set_partition_index(topic_partition.partition());
        partition_response.set_error_code(Errors::None.code());
        partition_response.set_active_producers(vec![wire0, wire1]);
        let response = describe_producers_response(&topic_partition, partition_response);
        let node = Node::new(3, "host".to_string(), 1);

        let result = handler.handle_response(&node, &HashSet::from([topic_partition.clone()]), &response);
        assert_eq!(
            result.completed_keys.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from([topic_partition.clone()])
        );
        assert!(result.failed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());

        let producer_state = result.completed_keys.get(&topic_partition).unwrap();
        assert_eq!(producer_state.active_producers().len(), 2);
        let by_id: HashMap<i64, &ProducerState> =
            producer_state.active_producers().iter().map(|p| (p.producer_id(), p)).collect();
        assert_eq!(by_id[&12345].producer_epoch(), 15);
        assert_eq!(by_id[&12345].last_sequence(), 75);
        assert_eq!(by_id[&12345].current_transaction_start_offset(), None);
        assert_eq!(by_id[&98765].current_transaction_start_offset(), Some(5000));
    }
}
