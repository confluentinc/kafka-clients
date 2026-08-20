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

//! Handler for the `describeTransactions` API (transaction-coordinator targeted).
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.DescribeTransactionsHandler`.

use std::collections::{HashMap, HashSet};

use crate::admin::transaction_description::TransactionDescription;
use crate::admin::transaction_state::TransactionState;
use crate::common::protocol::Errors;
use crate::common::requests::{ConcreteResponse, CoordinatorType, DescribeTransactionsRequestBuilder, RequestBuilder};
use crate::common::utils::LogContext;
use crate::common::{Error, Node, TopicPartition};
use crate::describe_transactions_request_data::DescribeTransactionsRequestData;
use crate::{kafka_debug, kafka_warn};

use super::admin_api_future::SimpleAdminApiFuture;
use super::admin_api_handler::{AdminApiHandler, ApiResult, RequestAndKeys};
use super::admin_api_lookup_strategy::AdminApiLookupStrategy;
use super::coordinator_key::CoordinatorKey;
use super::coordinator_strategy::CoordinatorStrategy;

/// Handler for `describeTransactions`.
///
/// Corresponds to `DescribeTransactionsHandler` (a `Batched` handler over
/// `CoordinatorKey` keys yielding [`TransactionDescription`] values).
pub(crate) struct DescribeTransactionsHandler {
    log_context: LogContext,
    lookup_strategy: CoordinatorStrategy,
}

impl DescribeTransactionsHandler {
    /// Creates a handler backed by a transaction-coordinator lookup strategy.
    pub(crate) fn new(log_context: LogContext) -> Self {
        Self {
            lookup_strategy: CoordinatorStrategy::new(CoordinatorType::Transaction, log_context.clone()),
            log_context,
        }
    }

    /// Builds the key set for a collection of transactional ids.
    fn build_key_set(transactional_ids: &[String]) -> HashSet<CoordinatorKey> {
        transactional_ids.iter().map(CoordinatorKey::by_transactional_id).collect()
    }

    /// Creates the future bundle for the given transactional ids.
    ///
    /// Mirrors `DescribeTransactionsHandler.newFuture`.
    pub(crate) fn new_future(
        transactional_ids: &[String],
    ) -> SimpleAdminApiFuture<CoordinatorKey, TransactionDescription> {
        SimpleAdminApiFuture::for_keys(Self::build_key_set(transactional_ids))
    }

    /// Builds a batched `DescribeTransactions` request for the given keys.
    ///
    /// Mirrors `buildBatchedRequest`.
    fn build_batched_request(&self, keys: &HashSet<CoordinatorKey>) -> DescribeTransactionsRequestData {
        let transactional_ids: Vec<String> = keys
            .iter()
            .map(|key| {
                assert!(
                    key.coordinator_type == CoordinatorType::Transaction,
                    "Invalid group coordinator key {key} when building `DescribeTransaction` request"
                );
                key.id_value.clone()
            })
            .collect();
        let mut data = DescribeTransactionsRequestData::new();
        data.set_transactional_ids(transactional_ids);
        data
    }

    /// Classifies a transaction-level error.
    ///
    /// Mirrors `handleError`.
    fn handle_error(
        &self,
        transactional_id_key: &CoordinatorKey,
        error: Errors,
        failed: &mut HashMap<CoordinatorKey, Error>,
        unmapped: &mut Vec<CoordinatorKey>,
    ) {
        match error {
            Errors::TransactionalIdAuthorizationFailed => {
                failed.insert(
                    transactional_id_key.clone(),
                    Error::with_message(
                        error,
                        format!(
                            "DescribeTransactions request for transactionalId `{}` failed due to authorization failure",
                            transactional_id_key.id_value
                        ),
                    ),
                );
            },
            Errors::TransactionalIdNotFound => {
                failed.insert(
                    transactional_id_key.clone(),
                    Error::with_message(
                        error,
                        format!(
                            "DescribeTransactions request for transactionalId `{}` failed because the ID could not be found",
                            transactional_id_key.id_value
                        ),
                    ),
                );
            },
            Errors::CoordinatorLoadInProgress => {
                // If the coordinator is in the middle of loading, then we just need to retry.
                kafka_debug!(
                    self.log_context,
                    "DescribeTransactions request for transactionalId `{}` failed because the coordinator is still in the process of loading state. Will retry",
                    transactional_id_key.id_value
                );
            },
            Errors::NotCoordinator | Errors::CoordinatorNotAvailable => {
                // If the coordinator is unavailable or there was a coordinator change, then we
                // unmap the key so that we retry the `FindCoordinator` request.
                unmapped.push(transactional_id_key.clone());
                kafka_debug!(
                    self.log_context,
                    "DescribeTransactions request for transactionalId `{}` returned error {:?}. Will attempt to find the coordinator again and retry",
                    transactional_id_key.id_value,
                    error
                );
            },
            _ => {
                failed.insert(
                    transactional_id_key.clone(),
                    Error::with_message(
                        error,
                        format!(
                            "DescribeTransactions request for transactionalId `{}` failed due to unexpected error",
                            transactional_id_key.id_value
                        ),
                    ),
                );
            },
        }
    }
}

impl AdminApiHandler<CoordinatorKey, TransactionDescription> for DescribeTransactionsHandler {
    fn api_name(&self) -> &str {
        "describeTransactions"
    }

    fn build_request(&self, _broker_id: i32, keys: &HashSet<CoordinatorKey>) -> Vec<RequestAndKeys<CoordinatorKey>> {
        let data = self.build_batched_request(keys);
        vec![RequestAndKeys {
            request: Box::new(DescribeTransactionsRequestBuilder::new(data)) as Box<dyn RequestBuilder>,
            keys: keys.clone(),
        }]
    }

    fn handle_response(
        &self,
        broker: &Node,
        keys: &HashSet<CoordinatorKey>,
        response: &ConcreteResponse,
    ) -> ApiResult<CoordinatorKey, TransactionDescription> {
        let ConcreteResponse::DescribeTransactions(response) = response else {
            return ApiResult::new(HashMap::new(), HashMap::new(), Vec::new());
        };
        let mut completed: HashMap<CoordinatorKey, TransactionDescription> = HashMap::new();
        let mut failed: HashMap<CoordinatorKey, Error> = HashMap::new();
        let mut unmapped: Vec<CoordinatorKey> = Vec::new();

        for transaction_state in &response.data().transaction_states {
            let transactional_id_key = CoordinatorKey::by_transactional_id(transaction_state.transactional_id.clone());
            if !keys.contains(&transactional_id_key) {
                kafka_warn!(
                    self.log_context,
                    "Response included transactionalId `{}`, which was not requested",
                    transaction_state.transactional_id
                );
                continue;
            }

            let error = Errors::for_code(transaction_state.error_code);
            if error != Errors::None {
                self.handle_error(&transactional_id_key, error, &mut failed, &mut unmapped);
                continue;
            }

            let transaction_start_time_ms = if transaction_state.transaction_start_time_ms < 0 {
                None
            } else {
                Some(transaction_state.transaction_start_time_ms)
            };

            let mut topic_partitions: HashSet<TopicPartition> = HashSet::new();
            for topic_data in &transaction_state.topics {
                for &partition_id in &topic_data.partitions {
                    topic_partitions.insert(TopicPartition::new(topic_data.topic.clone(), partition_id));
                }
            }

            completed.insert(
                transactional_id_key,
                TransactionDescription::new(
                    broker.id(),
                    TransactionState::parse(&transaction_state.transaction_state),
                    transaction_state.producer_id,
                    transaction_state.producer_epoch as i32,
                    transaction_state.transaction_timeout_ms as i64,
                    transaction_start_time_ms,
                    topic_partitions,
                ),
            );
        }

        ApiResult::new(completed, failed, unmapped)
    }

    fn lookup_strategy(&self) -> &dyn AdminApiLookupStrategy<CoordinatorKey> {
        &self.lookup_strategy
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::requests::DescribeTransactionsResponse;
    use crate::describe_transactions_response_data::{
        DescribeTransactionsResponseData, TopicData, TransactionState as WireTransactionState,
    };

    fn log_context() -> LogContext {
        LogContext::new("[test] ")
    }

    fn node() -> Node {
        Node::new(1, "host".to_string(), 1234)
    }

    fn key(id: &str) -> CoordinatorKey {
        CoordinatorKey::by_transactional_id(id)
    }

    fn response(states: Vec<WireTransactionState>) -> ConcreteResponse {
        let mut data = DescribeTransactionsResponseData::new();
        data.set_transaction_states(states);
        ConcreteResponse::DescribeTransactions(DescribeTransactionsResponse::new(data))
    }

    fn sample_state_1(transactional_id: &str) -> WireTransactionState {
        let mut foo = TopicData::new();
        foo.set_topic("foo".to_string());
        foo.set_partitions(vec![1, 3, 5]);
        let mut bar = TopicData::new();
        bar.set_topic("bar".to_string());
        bar.set_partitions(vec![1, 3, 5]);
        let mut s = WireTransactionState::new();
        s.set_error_code(Errors::None.code());
        s.set_transaction_state("Ongoing".to_string());
        s.set_transactional_id(transactional_id.to_string());
        s.set_producer_id(12345);
        s.set_producer_epoch(15);
        s.set_transaction_start_time_ms(1_599_151_791);
        s.set_transaction_timeout_ms(10000);
        s.set_topics(vec![foo, bar]);
        s
    }

    fn sample_state_2(transactional_id: &str) -> WireTransactionState {
        let mut s = WireTransactionState::new();
        s.set_error_code(Errors::None.code());
        s.set_transaction_state("Empty".to_string());
        s.set_transactional_id(transactional_id.to_string());
        s.set_producer_id(98765);
        s.set_producer_epoch(30);
        s.set_transaction_start_time_ms(-1);
        s
    }

    // Mirrors `DescribeTransactionsHandlerTest.testBuildRequest`.
    #[test]
    fn build_request() {
        let handler = DescribeTransactionsHandler::new(log_context());
        for ids in [
            vec!["foo".to_string(), "bar".to_string(), "baz".to_string()],
            vec!["foo".to_string()],
            vec!["bar".to_string(), "baz".to_string()],
        ] {
            let keys: HashSet<CoordinatorKey> = ids.iter().map(|s| key(s)).collect();
            let data = handler.build_batched_request(&keys);
            assert_eq!(
                data.transactional_ids.iter().cloned().collect::<HashSet<_>>(),
                ids.into_iter().collect::<HashSet<_>>()
            );
        }
    }

    // Mirrors `DescribeTransactionsHandlerTest.testHandleSuccessfulResponse`.
    #[test]
    fn handle_successful_response() {
        let handler = DescribeTransactionsHandler::new(log_context());
        let keys = HashSet::from([key("foo"), key("bar")]);
        let result =
            handler.handle_response(&node(), &keys, &response(vec![sample_state_1("foo"), sample_state_2("bar")]));
        assert_eq!(result.completed_keys.keys().cloned().collect::<HashSet<_>>(), keys);

        let foo = result.completed_keys.get(&key("foo")).unwrap();
        assert_eq!(foo.coordinator_id(), node().id());
        assert_eq!(foo.state(), TransactionState::Ongoing);
        assert_eq!(foo.producer_id(), 12345);
        assert_eq!(foo.producer_epoch(), 15);
        assert_eq!(foo.transaction_timeout_ms(), 10000);
        assert_eq!(foo.transaction_start_time_ms(), Some(1_599_151_791));
        assert_eq!(
            foo.topic_partitions(),
            &HashSet::from([
                TopicPartition::new("foo", 1),
                TopicPartition::new("foo", 3),
                TopicPartition::new("foo", 5),
                TopicPartition::new("bar", 1),
                TopicPartition::new("bar", 3),
                TopicPartition::new("bar", 5),
            ])
        );

        let bar = result.completed_keys.get(&key("bar")).unwrap();
        assert_eq!(bar.state(), TransactionState::Empty);
        assert_eq!(bar.transaction_start_time_ms(), None);
    }

    fn handle_response_error(error: Errors) -> ApiResult<CoordinatorKey, TransactionDescription> {
        let handler = DescribeTransactionsHandler::new(log_context());
        let mut s = WireTransactionState::new();
        s.set_error_code(error.code());
        s.set_transactional_id("foo".to_string());
        let result = handler.handle_response(&node(), &HashSet::from([key("foo")]), &response(vec![s]));
        assert!(result.completed_keys.is_empty());
        result
    }

    // Mirrors `DescribeTransactionsHandlerTest.testHandleErrorResponse`.
    #[test]
    fn handle_error_response() {
        for error in [
            Errors::TransactionalIdAuthorizationFailed,
            Errors::TransactionalIdNotFound,
            Errors::UnknownServerError,
        ] {
            let result = handle_response_error(error);
            assert!(result.unmapped_keys.is_empty());
            assert_eq!(
                result.failed_keys.keys().cloned().collect::<HashSet<_>>(),
                HashSet::from([key("foo")])
            );
            assert_eq!(result.failed_keys.get(&key("foo")).unwrap().error(), error);
        }
        // Retriable: nothing recorded.
        let result = handle_response_error(Errors::CoordinatorLoadInProgress);
        assert!(result.unmapped_keys.is_empty());
        assert!(result.failed_keys.is_empty());
        // Unmapped: retry FindCoordinator.
        for error in [Errors::NotCoordinator, Errors::CoordinatorNotAvailable] {
            let result = handle_response_error(error);
            assert!(result.failed_keys.is_empty());
            assert_eq!(result.unmapped_keys, vec![key("foo")]);
        }
    }
}
