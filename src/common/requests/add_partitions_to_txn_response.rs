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

//! `AddPartitionsToTxn` response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.AddPartitionsToTxnResponse`.
//!
//! Possible error codes:
//!  - `CoordinatorLoadInProgress` (14)
//!  - `CoordinatorNotAvailable` (15)
//!  - `NotCoordinator` (16)
//!  - `InvalidTxnState` (48)
//!  - `InvalidProducerIdMapping` (49)
//!  - `InvalidProducerEpoch` (47) — for version <= 1
//!  - `TopicAuthorizationFailed` (29)
//!  - `TransactionalIdAuthorizationFailed` (53)
//!  - `UnknownTopicOrPartition` (3)
//!  - `ProducerFenced` (90)

use std::collections::HashMap;
use std::io;

use crate::AddPartitionsToTxnResponseData;
use crate::add_partitions_to_txn_response_data::{
    AddPartitionsToTxnPartitionResult, AddPartitionsToTxnResult, AddPartitionsToTxnTopicResult,
};
use crate::common::TopicPartition;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::AbstractResponse;

/// An `AddPartitionsToTxn` response.
///
/// Corresponds to `org.apache.kafka.common.requests.AddPartitionsToTxnResponse`.
#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnResponse {
    data: AddPartitionsToTxnResponseData,
}

impl AddPartitionsToTxnResponse {
    /// The key under which a v3-and-below response's errors are reported.
    ///
    /// Below v4 the response carries no transactional id — the request was for a
    /// single transaction — so [`AddPartitionsToTxnResponse::errors`] files those
    /// results under the empty string. Corresponds to
    /// `AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID`.
    pub const V3_AND_BELOW_TXN_ID: &str = "";

    /// Creates a new `AddPartitionsToTxnResponse` from the underlying data.
    pub fn new(data: AddPartitionsToTxnResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::ADD_PARTITIONS_TO_TXN
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &AddPartitionsToTxnResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut AddPartitionsToTxnResponseData {
        &mut self.data
    }

    /// Returns the throttle time in milliseconds.
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Whether the client should throttle upon receiving this response.
    ///
    /// Returns `true` for v1+.
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 1
    }

    /// Per-transaction, per-partition errors.
    ///
    /// Corresponds to Java's `errors()`. A v3-and-below response contributes one
    /// entry keyed by [`Self::V3_AND_BELOW_TXN_ID`]; a v4+ response contributes one
    /// entry per transaction. The producer reads the former — it is the only
    /// shape it ever sends.
    pub fn errors(&self) -> HashMap<String, HashMap<TopicPartition, Errors>> {
        let mut errors_map = HashMap::new();

        if !self.data.results_by_topic_v3_and_below.is_empty() {
            errors_map.insert(
                AddPartitionsToTxnResponse::V3_AND_BELOW_TXN_ID.to_string(),
                Self::errors_for_transaction(&self.data.results_by_topic_v3_and_below),
            );
        }

        for result in &self.data.results_by_transaction {
            errors_map.insert(
                result.transactional_id.clone(),
                Self::errors_for_transaction(&result.topic_results),
            );
        }

        errors_map
    }

    /// Flattens a topic-result collection into per-partition errors.
    ///
    /// Corresponds to Java's static `errorsForTransaction`.
    pub fn errors_for_transaction(topic_results: &[AddPartitionsToTxnTopicResult]) -> HashMap<TopicPartition, Errors> {
        let mut results = HashMap::new();
        for topic_result in topic_results {
            for partition_result in &topic_result.results_by_partition {
                results.insert(
                    TopicPartition::new(topic_result.name.clone(), partition_result.partition_index),
                    Errors::for_code(partition_result.partition_error_code),
                );
            }
        }
        results
    }

    /// The topic results for a specific transactional id.
    ///
    /// Corresponds to Java's `getTransactionTopicResults(String)`. Java calls
    /// `find(..)` and dereferences it, which throws on a missing id; this returns
    /// `None` instead, since a Rust caller cannot catch that.
    pub fn get_transaction_topic_results(&self, transactional_id: &str) -> Option<&[AddPartitionsToTxnTopicResult]> {
        self.data
            .results_by_transaction
            .iter()
            .find(|result| result.transactional_id == transactional_id)
            .map(|result| result.topic_results.as_slice())
    }

    /// Builds a per-transaction result from a map of partition errors.
    ///
    /// Corresponds to Java's static `resultForTransaction`.
    pub fn result_for_transaction(
        transactional_id: impl Into<String>,
        errors: &HashMap<TopicPartition, Errors>,
    ) -> AddPartitionsToTxnResult {
        let mut result = AddPartitionsToTxnResult::new();
        result
            .set_transactional_id(transactional_id.into())
            .set_topic_results(Self::topic_collection_for_errors(errors));
        result
    }

    /// Groups partition errors by topic.
    ///
    /// Corresponds to Java's private static `topicCollectionForErrors`. Java uses
    /// a `HashMap` and so has unspecified ordering; this sorts by topic name and
    /// then partition index so the encoding is deterministic.
    fn topic_collection_for_errors(errors: &HashMap<TopicPartition, Errors>) -> Vec<AddPartitionsToTxnTopicResult> {
        let mut by_topic: HashMap<&str, Vec<(i32, Errors)>> = HashMap::new();
        for (topic_partition, error) in errors {
            by_topic
                .entry(topic_partition.topic())
                .or_default()
                .push((topic_partition.partition(), *error));
        }

        let mut names: Vec<&str> = by_topic.keys().copied().collect();
        names.sort_unstable();

        names
            .into_iter()
            .map(|name| {
                let mut partitions = by_topic[name].clone();
                partitions.sort_unstable_by_key(|(index, _)| *index);

                let results_by_partition = partitions
                    .into_iter()
                    .map(|(index, error)| {
                        let mut partition_result = AddPartitionsToTxnPartitionResult::new();
                        partition_result
                            .set_partition_index(index)
                            .set_partition_error_code(error.code());
                        partition_result
                    })
                    .collect();

                let mut topic_result = AddPartitionsToTxnTopicResult::new();
                topic_result
                    .set_name(name.to_string())
                    .set_results_by_partition(results_by_partition);
                topic_result
            })
            .collect()
    }

    /// Returns error counts by [`Errors`].
    ///
    /// Mirrors Java exactly, including the version inference: when
    /// `results_by_topic_v3_and_below` is empty the response must be v4+, so the
    /// top-level `error_code` is counted. Otherwise only the per-partition errors
    /// are counted — a v3-and-below response has no meaningful top-level code.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();

        if self.data.results_by_topic_v3_and_below.is_empty() {
            AbstractResponse::update_error_counts(&mut counts, Errors::for_code(self.data.error_code));
        }

        for errors in self.errors().values() {
            for error in errors.values() {
                AbstractResponse::update_error_counts(&mut counts, *error);
            }
        }
        counts
    }

    /// Parses an `AddPartitionsToTxnResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = AddPartitionsToTxnResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }
}

impl std::fmt::Display for AddPartitionsToTxnResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::requests::AddPartitionsToTxnRequest;

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic.to_string(), partition)
    }

    /// Builds a v3-and-below response carrying the given partition errors.
    fn v3_response(errors: &[(TopicPartition, Errors)]) -> AddPartitionsToTxnResponse {
        let map: HashMap<TopicPartition, Errors> = errors.iter().cloned().collect();
        let mut data = AddPartitionsToTxnResponseData::new();
        data.set_results_by_topic_v3_and_below(AddPartitionsToTxnResponse::topic_collection_for_errors(&map));
        AddPartitionsToTxnResponse::new(data)
    }

    #[test]
    fn test_errors_v3_and_below_keyed_by_empty_string() {
        let response = v3_response(&[
            (tp("topic-a", 0), Errors::NotCoordinator),
            (tp("topic-b", 1), Errors::None),
        ]);

        let errors = response.errors();
        assert_eq!(errors.len(), 1, "one entry for the single implicit transaction");
        let per_partition = &errors[AddPartitionsToTxnResponse::V3_AND_BELOW_TXN_ID];
        assert_eq!(per_partition[&tp("topic-a", 0)], Errors::NotCoordinator);
        assert_eq!(per_partition[&tp("topic-b", 1)], Errors::None);
    }

    #[test]
    fn test_errors_v4_keyed_by_transactional_id() {
        let mut first = HashMap::new();
        first.insert(tp("topic-a", 0), Errors::InvalidTxnState);
        let mut second = HashMap::new();
        second.insert(tp("topic-b", 3), Errors::ProducerFenced);

        let mut data = AddPartitionsToTxnResponseData::new();
        data.set_results_by_transaction(vec![
            AddPartitionsToTxnResponse::result_for_transaction("txn-1", &first),
            AddPartitionsToTxnResponse::result_for_transaction("txn-2", &second),
        ]);
        let response = AddPartitionsToTxnResponse::new(data);

        let errors = response.errors();
        assert_eq!(errors.len(), 2);
        assert_eq!(errors["txn-1"][&tp("topic-a", 0)], Errors::InvalidTxnState);
        assert_eq!(errors["txn-2"][&tp("topic-b", 3)], Errors::ProducerFenced);
        // The v3-and-below key must be absent.
        assert!(!errors.contains_key(AddPartitionsToTxnResponse::V3_AND_BELOW_TXN_ID));
    }

    #[test]
    fn test_errors_is_empty_for_an_empty_response() {
        let response = AddPartitionsToTxnResponse::new(AddPartitionsToTxnResponseData::new());
        assert!(response.errors().is_empty());
    }

    #[test]
    fn test_errors_for_transaction_flattens_all_partitions() {
        let mut map = HashMap::new();
        map.insert(tp("topic-a", 0), Errors::None);
        map.insert(tp("topic-a", 4), Errors::UnknownTopicOrPartition);
        map.insert(tp("topic-b", 2), Errors::NotCoordinator);

        let collection = AddPartitionsToTxnResponse::topic_collection_for_errors(&map);
        let flattened = AddPartitionsToTxnResponse::errors_for_transaction(&collection);
        assert_eq!(flattened, map, "round-trips through the wire shape");
    }

    /// Grouping is deterministic: topics sorted by name, partitions by index.
    #[test]
    fn test_topic_collection_for_errors_is_deterministic() {
        let mut map = HashMap::new();
        for (topic, partition) in [("topic-b", 5), ("topic-a", 9), ("topic-b", 1), ("topic-a", 0)] {
            map.insert(tp(topic, partition), Errors::None);
        }

        let collection = AddPartitionsToTxnResponse::topic_collection_for_errors(&map);
        assert_eq!(collection[0].name, "topic-a");
        assert_eq!(
            collection[0]
                .results_by_partition
                .iter()
                .map(|p| p.partition_index)
                .collect::<Vec<_>>(),
            vec![0, 9]
        );
        assert_eq!(collection[1].name, "topic-b");
        assert_eq!(
            collection[1]
                .results_by_partition
                .iter()
                .map(|p| p.partition_index)
                .collect::<Vec<_>>(),
            vec![1, 5]
        );
    }

    /// Java infers the version from whether the v3-and-below field is populated:
    /// only a v4+ response has a meaningful top-level error code.
    #[test]
    fn test_error_counts_includes_top_level_only_when_v4() {
        // v4+: the top-level code is counted.
        let mut data = AddPartitionsToTxnResponseData::new();
        data.set_error_code(Errors::NotCoordinator.code());
        let counts = AddPartitionsToTxnResponse::new(data).error_counts();
        assert_eq!(counts.get(&Errors::NotCoordinator), Some(&1));

        // v3-and-below: the top-level code is NOT counted, only per-partition.
        let response = v3_response(&[(tp("topic-a", 0), Errors::InvalidTxnState)]);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::InvalidTxnState), Some(&1));
        assert_eq!(counts.get(&Errors::None), None, "no top-level code counted below v4");
    }

    #[test]
    fn test_error_counts_aggregates_repeats() {
        let response = v3_response(&[
            (tp("topic-a", 0), Errors::NotCoordinator),
            (tp("topic-a", 1), Errors::NotCoordinator),
            (tp("topic-b", 0), Errors::InvalidTxnState),
        ]);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::NotCoordinator), Some(&2));
        assert_eq!(counts.get(&Errors::InvalidTxnState), Some(&1));
    }

    #[test]
    fn test_get_transaction_topic_results() {
        let mut map = HashMap::new();
        map.insert(tp("topic-a", 0), Errors::None);
        let mut data = AddPartitionsToTxnResponseData::new();
        data.set_results_by_transaction(vec![AddPartitionsToTxnResponse::result_for_transaction("txn-1", &map)]);
        let response = AddPartitionsToTxnResponse::new(data);

        let results = response.get_transaction_topic_results("txn-1").expect("present");
        assert_eq!(results[0].name, "topic-a");
        // Java throws for a missing id; Rust returns None.
        assert!(response.get_transaction_topic_results("absent").is_none());
    }

    #[test]
    fn test_throttle_time_round_trip() {
        let mut response = AddPartitionsToTxnResponse::new(AddPartitionsToTxnResponseData::new());
        assert_eq!(response.throttle_time_ms(), 0);
        response.maybe_set_throttle_time_ms(250);
        assert_eq!(response.throttle_time_ms(), 250);
    }

    #[test]
    fn test_should_client_throttle() {
        let response = AddPartitionsToTxnResponse::new(AddPartitionsToTxnResponseData::new());
        assert!(!response.should_client_throttle(0));
        assert!(response.should_client_throttle(1));
    }

    #[test]
    fn test_api_key() {
        let response = AddPartitionsToTxnResponse::new(AddPartitionsToTxnResponseData::new());
        assert_eq!(response.api_key(), &ApiKeys::ADD_PARTITIONS_TO_TXN);
    }

    /// Translated from `AddPartitionsToTxnResponseTest.testParse`, both branches.
    ///
    /// Java parameterises over every API version via `@ApiKeyVersionsSource`;
    /// per `definition-of-done.md` §3 that becomes a loop. Unlike the request
    /// test, both branches are in scope here — a response is something a client
    /// *receives*, and every method used is translated.
    #[test]
    fn test_parse() {
        use super::super::ConcreteResponse;

        const THROTTLE_TIME_MS: i32 = 10;
        let error_one = Errors::CoordinatorNotAvailable;
        let error_two = Errors::NotCoordinator;
        // Java declares tp1 in setUp but `testParse` only asserts on tp2.
        let tp2 = tp("topic2", 2);

        // One topic carrying two partitions with different errors.
        let topic_collection = {
            let mut partitions = Vec::new();
            for (index, error) in [(1, error_one), (2, error_two)] {
                let mut partition = AddPartitionsToTxnPartitionResult::new();
                partition.set_partition_index(index).set_partition_error_code(error.code());
                partitions.push(partition);
            }
            let mut topic = AddPartitionsToTxnTopicResult::new();
            topic.set_name("topic1".to_string()).set_results_by_partition(partitions);
            vec![topic]
        };

        for version in ApiKeys::ADD_PARTITIONS_TO_TXN.oldest_version()..=ApiKeys::ADD_PARTITIONS_TO_TXN.latest_version()
        {
            let data = if version < AddPartitionsToTxnRequest::EARLIEST_BROKER_VERSION {
                let mut data = AddPartitionsToTxnResponseData::new();
                data.set_results_by_topic_v3_and_below(topic_collection.clone())
                    .set_throttle_time_ms(THROTTLE_TIME_MS);
                data
            } else {
                let mut txn_two_errors = HashMap::new();
                txn_two_errors.insert(tp2.clone(), error_one);

                let mut first = AddPartitionsToTxnResult::new();
                first
                    .set_transactional_id("txn1".to_string())
                    .set_topic_results(topic_collection.clone());

                let mut data = AddPartitionsToTxnResponseData::new();
                data.set_results_by_transaction(vec![
                    first,
                    AddPartitionsToTxnResponse::result_for_transaction("txn2", &txn_two_errors),
                ])
                .set_throttle_time_ms(THROTTLE_TIME_MS);
                data
            };

            let response = AddPartitionsToTxnResponse::new(data);

            // Round-trip through the wire, then assert on the parsed copy —
            // Java does `parse(response.serialize(version), version)`.
            let mut concrete = ConcreteResponse::AddPartitionsToTxn(response.clone());
            let mut buffer = concrete.serialize(version).expect("serialize");
            buffer.flip();
            let parsed = AddPartitionsToTxnResponse::parse(&mut buffer, version).expect("parse");

            assert_eq!(parsed.throttle_time_ms(), THROTTLE_TIME_MS, "v{version}");
            assert_eq!(parsed.should_client_throttle(version), version >= 1, "v{version}");

            let counts = parsed.error_counts();
            if version < AddPartitionsToTxnRequest::EARLIEST_BROKER_VERSION {
                assert_eq!(counts.get(&error_one), Some(&1), "v{version}");
                assert_eq!(counts.get(&error_two), Some(&1), "v{version}");
                assert_eq!(counts.len(), 2, "no top-level error counted below v4, v{version}");
            } else {
                // v4+: the top-level code (None here) counts as one, plus
                // error_one twice (once per transaction) and error_two once.
                assert_eq!(counts.get(&Errors::None), Some(&1), "top level, v{version}");
                assert_eq!(counts.get(&error_one), Some(&2), "v{version}");
                assert_eq!(counts.get(&error_two), Some(&1), "v{version}");
                // Java asserts the whole map; pin the cardinality too so a
                // spurious extra entry cannot slip through.
                assert_eq!(counts.len(), 3, "v{version}");

                let mut expected = HashMap::new();
                expected.insert(tp2.clone(), error_one);
                assert_eq!(
                    AddPartitionsToTxnResponse::errors_for_transaction(
                        response.get_transaction_topic_results("txn2").expect("txn2 present")
                    ),
                    expected,
                    "v{version}"
                );
            }
        }
    }

    /// Translated from `AddPartitionsToTxnResponseTest.testBatchedErrors`.
    #[test]
    fn test_batched_errors() {
        let tp1 = tp("topic1", 1);
        let error_one = Errors::CoordinatorNotAvailable;

        let mut txn1_errors = HashMap::new();
        txn1_errors.insert(tp1.clone(), error_one);
        let mut txn2_errors = HashMap::new();
        txn2_errors.insert(tp1.clone(), error_one);

        let mut data = AddPartitionsToTxnResponseData::new();
        data.set_results_by_transaction(vec![
            AddPartitionsToTxnResponse::result_for_transaction("txn1", &txn1_errors),
            AddPartitionsToTxnResponse::result_for_transaction("txn2", &txn2_errors),
        ]);
        let response = AddPartitionsToTxnResponse::new(data);

        assert_eq!(
            AddPartitionsToTxnResponse::errors_for_transaction(
                response.get_transaction_topic_results("txn1").expect("txn1")
            ),
            txn1_errors
        );
        assert_eq!(
            AddPartitionsToTxnResponse::errors_for_transaction(
                response.get_transaction_topic_results("txn2").expect("txn2")
            ),
            txn2_errors
        );

        let mut expected = HashMap::new();
        expected.insert("txn1".to_string(), txn1_errors);
        expected.insert("txn2".to_string(), txn2_errors);
        assert_eq!(response.errors(), expected);
    }
}
