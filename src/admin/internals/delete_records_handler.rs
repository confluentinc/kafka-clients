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

//! Handler for the `deleteRecords` API (partition-leader targeted).
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.DeleteRecordsHandler`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::admin::deleted_records::DeletedRecords;
use crate::admin::records_to_delete::RecordsToDelete;
use crate::common::errors::ApiError;
use crate::common::protocol::Errors;
use crate::common::requests::{ConcreteResponse, DeleteRecordsRequestBuilder, RequestBuilder};
use crate::common::utils::LogContext;
use crate::common::{Error, Node, TopicPartition};
use crate::delete_records_request_data::{DeleteRecordsPartition, DeleteRecordsRequestData, DeleteRecordsTopic};
use crate::kafka_debug;

use super::admin_api_handler::{AdminApiHandler, ApiResult, RequestAndKeys};
use super::admin_api_lookup_strategy::AdminApiLookupStrategy;
use super::partition_leader_cache::PartitionLeaderCache;
use super::partition_leader_strategy::{PartitionLeaderFuture, PartitionLeaderStrategy};

/// Handler for `deleteRecords`.
///
/// Corresponds to `DeleteRecordsHandler` (a `Batched` handler over
/// `TopicPartition` keys yielding [`DeletedRecords`] values).
pub(crate) struct DeleteRecordsHandler {
    records_to_delete: HashMap<TopicPartition, RecordsToDelete>,
    log_context: LogContext,
    lookup_strategy: PartitionLeaderStrategy,
    timeout: i32,
}

impl DeleteRecordsHandler {
    /// Creates a handler for the given per-partition deletion offsets.
    pub(crate) fn new(
        records_to_delete: HashMap<TopicPartition, RecordsToDelete>,
        log_context: LogContext,
        timeout: i32,
    ) -> Self {
        let lookup_strategy = PartitionLeaderStrategy::new(log_context.clone());
        Self { records_to_delete, log_context, lookup_strategy, timeout }
    }

    /// Creates the future bundle that the driver completes as partitions are
    /// resolved.
    ///
    /// Mirrors `DeleteRecordsHandler.newFuture`.
    pub(crate) fn new_future(
        topic_partitions: HashSet<TopicPartition>,
        partition_leader_cache: Arc<PartitionLeaderCache>,
    ) -> PartitionLeaderFuture<DeletedRecords> {
        PartitionLeaderFuture::new(topic_partitions, partition_leader_cache)
    }

    /// Builds a single batched `DeleteRecords` request for the given keys.
    ///
    /// Mirrors `buildBatchedRequest`.
    pub(crate) fn build_batched_request(
        &self,
        _broker_id: i32,
        keys: &HashSet<TopicPartition>,
    ) -> DeleteRecordsRequestData {
        let mut deletions_for_topic: HashMap<String, DeleteRecordsTopic> = HashMap::new();
        for topic_partition in keys {
            let to_delete = self.records_to_delete.get(topic_partition);
            let offset = to_delete.map(RecordsToDelete::before_offset_value).unwrap_or(-1);
            let topic = deletions_for_topic
                .entry(topic_partition.topic().to_string())
                .or_insert_with(|| {
                    let mut t = DeleteRecordsTopic::new();
                    t.set_name(topic_partition.topic().to_string());
                    t
                });
            let mut partition = DeleteRecordsPartition::new();
            partition.set_partition_index(topic_partition.partition());
            partition.set_offset(offset);
            topic.partitions.push(partition);
        }
        let mut data = DeleteRecordsRequestData::new();
        data.set_topics(deletions_for_topic.into_values().collect());
        data.set_timeout_ms(self.timeout);
        data
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
        if error.error().is_some_and(|e| e.is_invalid_metadata_error()) {
            kafka_debug!(
                self.log_context,
                "DeleteRecords lookup request for topic partition {} will be retried due to invalid leader metadata {:?}",
                topic_partition,
                error
            );
            unmapped.push(topic_partition.clone());
        } else if error.error().is_some_and(|e| e.is_retriable_error()) {
            kafka_debug!(
                self.log_context,
                "DeleteRecords fulfillment request for topic partition {} will be retried due to {:?}",
                topic_partition,
                error
            );
            retriable.insert(topic_partition.clone());
        } else {
            kafka_debug!(
                self.log_context,
                "DeleteRecords request for topic partition {} failed due to error {:?}",
                topic_partition,
                error
            );
            failed.insert(topic_partition.clone(), Error::new(error));
        }
    }
}

impl AdminApiHandler<TopicPartition, DeletedRecords> for DeleteRecordsHandler {
    fn api_name(&self) -> &str {
        "deleteRecords"
    }

    fn build_request(&self, broker_id: i32, keys: &HashSet<TopicPartition>) -> Vec<RequestAndKeys<TopicPartition>> {
        let data = self.build_batched_request(broker_id, keys);
        vec![RequestAndKeys {
            request: Box::new(DeleteRecordsRequestBuilder::from_data(data)) as Box<dyn RequestBuilder>,
            keys: keys.clone(),
        }]
    }

    fn handle_response(
        &self,
        broker: &Node,
        keys: &HashSet<TopicPartition>,
        response: &ConcreteResponse,
    ) -> ApiResult<TopicPartition, DeletedRecords> {
        let ConcreteResponse::DeleteRecords(response) = response else {
            // Java fails the call once (`KafkaAdminClient.java:1387-1391`); an empty
            // result would silently re-issue the request until the deadline. See
            // `ApiResult::failed_all`.
            return ApiResult::failed_all(
                keys,
                Error::local_illegal_state("DeleteRecordsHandler received an unexpected response type"),
            );
        };
        let mut completed: HashMap<TopicPartition, DeletedRecords> = HashMap::new();
        let mut failed: HashMap<TopicPartition, Error> = HashMap::new();
        let mut unmapped: Vec<TopicPartition> = Vec::new();
        let mut retriable: HashSet<TopicPartition> = HashSet::new();

        for topic_result in &response.data().topics {
            for partition_result in &topic_result.partitions {
                let error = Errors::for_code(partition_result.error_code);
                let topic_partition = TopicPartition::new(topic_result.name.as_str(), partition_result.partition_index);
                if error == Errors::None {
                    completed.insert(topic_partition, DeletedRecords::new(partition_result.low_watermark));
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
                // `new ApiException(..)` (`DeleteRecordsHandler.java:136-140`) — the
                // concrete base class, which `Error::Api` translates. Spelling it
                // `Errors::UnknownServerError` resolved to the `UnknownServerError`
                // *subclass* instead (finding 246).
                let sanity_check_error = Error::Api(ApiError::new(format!(
                    "The response from broker {} did not contain a result for topic partition {}",
                    broker.id(),
                    topic_partition
                )));
                failed.insert(topic_partition.clone(), sanity_check_error);
            }
        }

        ApiResult::new(completed, failed, unmapped)
    }

    fn lookup_strategy(&self) -> &dyn AdminApiLookupStrategy<TopicPartition> {
        &self.lookup_strategy
    }
}

#[cfg(test)]
mod tests {
    use super::super::admin_api_future::AdminApiFuture;
    use super::*;
    use crate::common::protocol::ApiKeys;
    use crate::common::requests::{DeleteRecordsResponse, MetadataResponse};
    use crate::delete_records_response_data::{
        DeleteRecordsPartitionResult, DeleteRecordsResponseData, DeleteRecordsTopicResult,
    };
    use crate::metadata_response_data::{MetadataResponseData, MetadataResponsePartition, MetadataResponseTopic};

    const TIMEOUT: i32 = 2000;

    fn tp(partition: i32) -> TopicPartition {
        TopicPartition::new("t0", partition)
    }

    fn node(id: i32) -> Node {
        Node::new(id, "host".to_string(), 1234)
    }

    fn records_to_delete() -> HashMap<TopicPartition, RecordsToDelete> {
        [tp(0), tp(1), tp(2), tp(3)]
            .into_iter()
            .map(|k| (k, RecordsToDelete::before_offset(10)))
            .collect()
    }

    fn handler() -> DeleteRecordsHandler {
        DeleteRecordsHandler::new(records_to_delete(), LogContext::new("[test] "), TIMEOUT)
    }

    /// Builds a DeleteRecords response covering `topic_partitions`, applying the
    /// per-partition error codes from `errors_by_partition` (default NONE).
    fn create_response(
        errors_by_partition: &HashMap<TopicPartition, i16>,
        topic_partitions: &HashSet<TopicPartition>,
    ) -> ConcreteResponse {
        let mut topics: Vec<DeleteRecordsTopicResult> = Vec::new();
        for topic_partition in topic_partitions {
            let mut partition_result = DeleteRecordsPartitionResult::new();
            partition_result.set_partition_index(topic_partition.partition());
            partition_result.set_error_code(errors_by_partition.get(topic_partition).copied().unwrap_or(0));
            match topics.iter_mut().find(|t| t.name == topic_partition.topic()) {
                Some(existing) => existing.partitions.push(partition_result),
                None => {
                    let mut t = DeleteRecordsTopicResult::new();
                    t.set_name(topic_partition.topic().to_string());
                    t.set_partitions(vec![partition_result]);
                    topics.push(t);
                },
            }
        }
        let mut data = DeleteRecordsResponseData::new();
        data.set_topics(topics);
        ConcreteResponse::DeleteRecords(DeleteRecordsResponse::new(data))
    }

    fn handle(response: &ConcreteResponse) -> ApiResult<TopicPartition, DeletedRecords> {
        let keys: HashSet<TopicPartition> = records_to_delete().into_keys().collect();
        handler().handle_response(&node(1), &keys, response)
    }

    /// Mirrors `assertResult`: verifies completed keys, failed keys, unmapped
    /// keys, and the derived "retriable" set.
    fn assert_result(
        result: &ApiResult<TopicPartition, DeletedRecords>,
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
        let mut actual_retriable: HashSet<TopicPartition> = records_to_delete().into_keys().collect();
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

    #[test]
    fn build_request_simple() {
        let handler = handler();
        let keys: HashSet<TopicPartition> = [tp(0), tp(1)].into_iter().collect();
        let requests = handler.build_request(node(1).id(), &keys);
        assert_eq!(requests.len(), 1);
        // Re-derive the built data to assert its shape.
        let data = handler.build_batched_request(node(1).id(), &keys);
        assert_eq!(data.topics.len(), 1);
        assert_eq!(data.topics[0].partitions.len(), 2);
    }

    #[test]
    fn handle_successful_response() {
        let keys: HashSet<TopicPartition> = records_to_delete().into_keys().collect();
        let result = handle(&create_response(&HashMap::new(), &keys));
        assert_result(&result, keys, HashSet::new(), Vec::new(), HashSet::new());
    }

    #[test]
    fn handle_retriable_partition_timeout_response() {
        let error_partition = tp(0);
        let errors: HashMap<TopicPartition, i16> = [(error_partition.clone(), Errors::RequestTimedOut.code())]
            .into_iter()
            .collect();
        let keys: HashSet<TopicPartition> = records_to_delete().into_keys().collect();
        let result = handle(&create_response(&errors, &keys));
        let retriable: HashSet<TopicPartition> = [error_partition.clone()].into_iter().collect();
        let mut completed: HashSet<TopicPartition> = records_to_delete().into_keys().collect();
        completed.remove(&error_partition);
        assert_result(&result, completed, HashSet::new(), Vec::new(), retriable);
    }

    #[test]
    fn handle_lookup_retriable_partition_invalid_metadata_response() {
        let error_partition = tp(0);
        let errors: HashMap<TopicPartition, i16> = [(error_partition.clone(), Errors::NotLeaderOrFollower.code())]
            .into_iter()
            .collect();
        let keys: HashSet<TopicPartition> = records_to_delete().into_keys().collect();
        let result = handle(&create_response(&errors, &keys));
        let unmapped = vec![error_partition.clone()];
        let mut completed: HashSet<TopicPartition> = records_to_delete().into_keys().collect();
        completed.remove(&error_partition);
        assert_result(&result, completed, HashSet::new(), unmapped, HashSet::new());
    }

    #[test]
    fn handle_partition_error_response() {
        let error_partition = tp(0);
        let errors: HashMap<TopicPartition, i16> = [(error_partition.clone(), Errors::TopicAuthorizationFailed.code())]
            .into_iter()
            .collect();
        let keys: HashSet<TopicPartition> = records_to_delete().into_keys().collect();
        let result = handle(&create_response(&errors, &keys));
        let failed: HashSet<TopicPartition> = [error_partition.clone()].into_iter().collect();
        let mut completed: HashSet<TopicPartition> = records_to_delete().into_keys().collect();
        completed.remove(&error_partition);
        assert_result(&result, completed, failed, Vec::new(), HashSet::new());
        assert_eq!(
            result.failed_keys.get(&error_partition).unwrap().error(),
            Errors::TopicAuthorizationFailed
        );
    }

    #[test]
    fn handle_unexpected_partition_error_response() {
        let error_partition = tp(0);
        let errors: HashMap<TopicPartition, i16> = [(error_partition.clone(), Errors::UnknownServerError.code())]
            .into_iter()
            .collect();
        let keys: HashSet<TopicPartition> = records_to_delete().into_keys().collect();
        let result = handle(&create_response(&errors, &keys));
        let failed: HashSet<TopicPartition> = [error_partition.clone()].into_iter().collect();
        let mut completed: HashSet<TopicPartition> = records_to_delete().into_keys().collect();
        completed.remove(&error_partition);
        assert_result(&result, completed, failed, Vec::new(), HashSet::new());
    }

    #[test]
    fn mixed_response() {
        let errors: HashMap<TopicPartition, i16> = [
            (tp(0), Errors::UnknownServerError.code()),
            (tp(1), Errors::NotLeaderOrFollower.code()),
            (tp(2), Errors::RequestTimedOut.code()),
        ]
        .into_iter()
        .collect();
        let keys: HashSet<TopicPartition> = records_to_delete().into_keys().collect();
        let result = handle(&create_response(&errors, &keys));

        let failed: HashSet<TopicPartition> = [tp(0)].into_iter().collect();
        let unmapped = vec![tp(1)];
        let retriable: HashSet<TopicPartition> = [tp(2)].into_iter().collect();
        let mut completed: HashSet<TopicPartition> = records_to_delete().into_keys().collect();
        completed.remove(&tp(0));
        completed.remove(&tp(1));
        completed.remove(&tp(2));
        assert_result(&result, completed, failed, unmapped, retriable);
    }

    /// A response of the wrong type is what Java's
    /// `catch (Throwable t) { call.fail(now, t) }`
    /// (`KafkaAdminClient.java:1387-1391`) exists for: the `(XResponse)
    /// abstractResponse` downcast throws `ClassCastException`, the affected call
    /// fails once, and the client keeps serving everything else.
    ///
    /// This handler used to return an empty `ApiResult`, which completes nothing,
    /// fails nothing and unmaps nothing — the driver has already cleared the
    /// in-flight request, so it re-issued the identical request under backoff until
    /// the deadline and the caller got a generic timeout with the real cause gone.
    #[test]
    fn an_unexpected_response_type_fails_every_key_of_the_request() {
        let keys: HashSet<TopicPartition> = records_to_delete().into_keys().collect();
        // Any other variant: `Metadata` is what the lookup stage of this same driver
        // uses, so it is the realistic mis-route.
        let wrong = ConcreteResponse::Metadata(crate::common::requests::MetadataResponse::new(
            crate::metadata_response_data::MetadataResponseData::new(),
            0,
        ));
        let result = handler().handle_response(&node(1), &keys, &wrong);

        assert!(result.completed_keys.is_empty(), "nothing may be reported as completed");
        assert!(result.unmapped_keys.is_empty(), "a type mismatch is not a lookup problem");
        assert_eq!(
            result.failed_keys.keys().cloned().collect::<HashSet<_>>(),
            keys,
            "every key the request covered must be failed, as `AdminApiDriver.onFailure` does"
        );
        for error in result.failed_keys.values() {
            assert_eq!(error.message(), "DeleteRecordsHandler received an unexpected response type");
            assert!(
                !error.is_retriable_error(),
                "a wire-plumbing bug must not be retried: {error:?}"
            );
        }
    }

    #[test]
    fn handle_response_sanity_check() {
        let error_partition = tp(0);
        let mut present: HashSet<TopicPartition> = records_to_delete().into_keys().collect();
        present.remove(&error_partition);
        let result = handle(&create_response(&HashMap::new(), &present));

        assert_eq!(result.completed_keys.len(), records_to_delete().len() - 1);
        assert_eq!(result.failed_keys.len(), 1);
        let (failed_key, failed_err) = result.failed_keys.iter().next().unwrap();
        assert_eq!(failed_key, &error_partition);
        assert!(failed_err.message().contains("did not contain a result for topic partition"));
        assert!(result.unmapped_keys.is_empty());
    }

    // node1 leads t0p0 and t0p2, while node2 leads t0p1 and t0p3.
    #[test]
    fn build_request_multiple_leaders() {
        let node1 = node(1);
        let node2 = node(2);
        let mut topic_metadata = MetadataResponseTopic::new();
        topic_metadata.set_name(Some("t0".to_string()));
        topic_metadata.set_error_code(Errors::None.code());
        for (partition, leader) in [(0, node1.id()), (1, node2.id()), (2, node1.id()), (3, node2.id())] {
            let mut p = MetadataResponsePartition::new();
            p.set_partition_index(partition);
            p.set_leader_id(leader);
            p.set_error_code(Errors::None.code());
            topic_metadata.partitions.push(p);
        }
        let mut metadata = MetadataResponseData::new();
        metadata.set_topics(vec![topic_metadata]);
        let metadata_response =
            ConcreteResponse::Metadata(MetadataResponse::new(metadata, ApiKeys::METADATA.latest_version()));

        let handler = handler();
        let strategy = handler.lookup_strategy();
        let tp_set: HashSet<TopicPartition> = [tp(0), tp(1), tp(2), tp(3)].into_iter().collect();
        let lookup = strategy.handle_response(&tp_set, &metadata_response);
        assert!(lookup.failed_keys.is_empty());
        assert_eq!(lookup.mapped_keys.keys().cloned().collect::<HashSet<_>>(), tp_set);

        let mut partitions_per_broker: HashMap<i32, HashSet<TopicPartition>> = HashMap::new();
        for (tp, node) in lookup.mapped_keys {
            partitions_per_broker.entry(node).or_default().insert(tp);
        }

        let node1_data = handler.build_batched_request(node1.id(), &partitions_per_broker[&node1.id()]);
        assert_eq!(node1_data.topics[0].partitions.len(), 2);
        let node1_partitions: HashSet<TopicPartition> = node1_data.topics[0]
            .partitions
            .iter()
            .map(|p| TopicPartition::new("t0", p.partition_index))
            .collect();
        assert_eq!(node1_partitions, HashSet::from([tp(0), tp(2)]));

        let node2_data = handler.build_batched_request(node2.id(), &partitions_per_broker[&node2.id()]);
        assert_eq!(node2_data.topics[0].partitions.len(), 2);
        let node2_partitions: HashSet<TopicPartition> = node2_data.topics[0]
            .partitions
            .iter()
            .map(|p| TopicPartition::new("t0", p.partition_index))
            .collect();
        assert_eq!(node2_partitions, HashSet::from([tp(1), tp(3)]));
    }

    #[test]
    fn new_future_exposes_all_partitions() {
        let cache = Arc::new(PartitionLeaderCache::new());
        let keys: HashSet<TopicPartition> = records_to_delete().into_keys().collect();
        let future = DeleteRecordsHandler::new_future(keys.clone(), cache);
        assert_eq!(future.all().keys().cloned().collect::<HashSet<_>>(), keys);
        assert_eq!(future.lookup_keys(), keys);
    }
}
