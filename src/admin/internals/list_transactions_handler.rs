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

//! Handler for the `listTransactions` API (fans out to all brokers).
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.ListTransactionsHandler`.

use std::collections::HashSet;

use crate::admin::options::ListTransactionsOptions;
use crate::admin::transaction_listing::TransactionListing;
use crate::admin::transaction_state::TransactionState;
use crate::common::protocol::Errors;
use crate::common::requests::{ConcreteResponse, ListTransactionsRequestBuilder, RequestBuilder};
use crate::common::utils::LogContext;
use crate::common::{Error, Node};
use crate::list_transactions_request_data::ListTransactionsRequestData;
use crate::{kafka_debug, kafka_error};

use super::admin_api_handler::{AdminApiHandler, ApiResult, RequestAndKeys};
use super::admin_api_lookup_strategy::AdminApiLookupStrategy;
use super::all_brokers_strategy::{AllBrokersFuture, AllBrokersStrategy, BrokerKey};

/// Handler for `listTransactions`.
///
/// Corresponds to `ListTransactionsHandler` (a `Batched` handler over
/// [`BrokerKey`] keys yielding a `Vec<TransactionListing>` value per broker).
pub(crate) struct ListTransactionsHandler {
    log_context: LogContext,
    options: ListTransactionsOptions,
    lookup_strategy: AllBrokersStrategy,
}

impl ListTransactionsHandler {
    /// Creates a handler backed by an [`AllBrokersStrategy`].
    pub(crate) fn new(options: ListTransactionsOptions, log_context: LogContext) -> Self {
        Self {
            lookup_strategy: AllBrokersStrategy::new(log_context.clone()),
            log_context,
            options,
        }
    }

    /// Creates the future bundle for the RPC.
    ///
    /// Mirrors `ListTransactionsHandler.newFuture`.
    pub(crate) fn new_future() -> AllBrokersFuture<Vec<TransactionListing>> {
        AllBrokersFuture::new()
    }

    /// Builds a batched `ListTransactions` request from the options.
    ///
    /// Mirrors `buildBatchedRequest`.
    fn build_batched_request(&self) -> ListTransactionsRequestData {
        let mut data = ListTransactionsRequestData::new();
        data.set_producer_id_filters(self.options.filtered_producer_ids().iter().copied().collect());
        data.set_state_filters(self.options.filtered_states().iter().map(TransactionState::to_string).collect());
        data.set_duration_filter(self.options.filtered_duration());
        if let Some(pattern) = self.options.filtered_transactional_id_pattern()
            && !pattern.is_empty()
        {
            data.set_transactional_id_pattern(Some(pattern.to_string()));
        }
        data
    }

    /// Mirrors `requireSingleton`: the fulfillment key set must be exactly the
    /// broker key for the responding broker. Panics (Java's
    /// `IllegalArgumentException`) otherwise — a programming error the driver
    /// never triggers in production.
    fn require_singleton(keys: &HashSet<BrokerKey>, broker_id: i32) -> BrokerKey {
        assert!(keys.len() == 1, "Unexpected key set: {keys:?}");
        let key = keys.iter().next().expect("checked len == 1").clone();
        assert!(key.broker_id == Some(broker_id), "Unexpected broker key: {key}");
        key
    }
}

impl AdminApiHandler<BrokerKey, Vec<TransactionListing>> for ListTransactionsHandler {
    fn api_name(&self) -> &str {
        "listTransactions"
    }

    fn build_request(&self, _broker_id: i32, keys: &HashSet<BrokerKey>) -> Vec<RequestAndKeys<BrokerKey>> {
        let data = self.build_batched_request();
        vec![RequestAndKeys {
            request: Box::new(ListTransactionsRequestBuilder::new(data)) as Box<dyn RequestBuilder>,
            keys: keys.clone(),
        }]
    }

    fn handle_response(
        &self,
        broker: &Node,
        keys: &HashSet<BrokerKey>,
        response: &ConcreteResponse,
    ) -> ApiResult<BrokerKey, Vec<TransactionListing>> {
        let broker_id = broker.id();
        let key = Self::require_singleton(keys, broker_id);

        let ConcreteResponse::ListTransactions(response) = response else {
            // Java fails the call once (`KafkaAdminClient.java:1387-1391`); an empty
            // result would silently re-issue the request until the deadline. See
            // `ApiResult::failed_all`.
            return ApiResult::failed_all(
                keys,
                Error::illegal_state("ListTransactionsHandler received an unexpected response type"),
            );
        };
        let error = Errors::for_code(response.data().error_code);

        if error == Errors::CoordinatorLoadInProgress {
            kafka_debug!(
                self.log_context,
                "The `ListTransactions` request sent to broker {} failed because the coordinator is still loading state. Will try again after backing off",
                broker_id
            );
            ApiResult::new(std::collections::HashMap::new(), std::collections::HashMap::new(), Vec::new())
        } else if error == Errors::CoordinatorNotAvailable {
            kafka_debug!(
                self.log_context,
                "The `ListTransactions` request sent to broker {} failed because the coordinator is shutting down",
                broker_id
            );
            ApiResult::new(
                std::collections::HashMap::new(),
                std::collections::HashMap::from([(
                    key,
                    Error::with_message(
                        error,
                        format!(
                            "ListTransactions request sent to broker {broker_id} failed because the coordinator is shutting down"
                        ),
                    ),
                )]),
                Vec::new(),
            )
        } else if error != Errors::None {
            kafka_error!(
                self.log_context,
                "The `ListTransactions` request sent to broker {} failed because of an unexpected error {:?}",
                broker_id,
                error
            );
            ApiResult::new(
                std::collections::HashMap::new(),
                std::collections::HashMap::from([(
                    key,
                    Error::with_message(
                        error,
                        format!("ListTransactions request sent to broker {broker_id} failed with an unexpected error"),
                    ),
                )]),
                Vec::new(),
            )
        } else {
            let listings: Vec<TransactionListing> = response
                .data()
                .transaction_states
                .iter()
                .map(|s| {
                    TransactionListing::new(
                        s.transactional_id.clone(),
                        s.producer_id,
                        TransactionState::parse(&s.transaction_state),
                    )
                })
                .collect();
            ApiResult::new(
                std::collections::HashMap::from([(key, listings)]),
                std::collections::HashMap::new(),
                Vec::new(),
            )
        }
    }

    fn lookup_strategy(&self) -> &dyn AdminApiLookupStrategy<BrokerKey> {
        &self.lookup_strategy
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::requests::ListTransactionsResponse;
    use crate::list_transactions_response_data::{ListTransactionsResponseData, TransactionState as WireTxnState};

    fn log_context() -> LogContext {
        LogContext::new("[test] ")
    }

    fn node() -> Node {
        Node::new(1, "host".to_string(), 1234)
    }

    fn broker_key(id: i32) -> BrokerKey {
        BrokerKey::new(Some(id))
    }

    fn handler(options: ListTransactionsOptions) -> ListTransactionsHandler {
        ListTransactionsHandler::new(options, log_context())
    }

    // Mirrors `ListTransactionsHandlerTest.testBuildRequestWithoutFilters`.
    #[test]
    fn build_request_without_filters() {
        let data = handler(ListTransactionsOptions::new()).build_batched_request();
        assert!(data.producer_id_filters.is_empty());
        assert!(data.state_filters.is_empty());
    }

    // Mirrors `ListTransactionsHandlerTest.testBuildRequestWithFilteredProducerId`.
    #[test]
    fn build_request_with_filtered_producer_id() {
        let data = handler(ListTransactionsOptions::new().filter_producer_ids([23423])).build_batched_request();
        assert_eq!(data.producer_id_filters, vec![23423]);
        assert!(data.state_filters.is_empty());
    }

    // Mirrors `ListTransactionsHandlerTest.testBuildRequestWithFilteredState`.
    #[test]
    fn build_request_with_filtered_state() {
        let data =
            handler(ListTransactionsOptions::new().filter_states([TransactionState::Ongoing])).build_batched_request();
        assert_eq!(data.state_filters, vec![TransactionState::Ongoing.to_string()]);
        assert!(data.producer_id_filters.is_empty());
    }

    // Mirrors `ListTransactionsHandlerTest.testBuildRequestWithFilteredTransactionalIdPattern`.
    #[test]
    fn build_request_with_filtered_transactional_id_pattern() {
        let data =
            handler(ListTransactionsOptions::new().filter_on_transactional_id_pattern(Some("^special-.*".to_string())))
                .build_batched_request();
        assert_eq!(data.transactional_id_pattern.as_deref(), Some("^special-.*"));
        assert!(data.state_filters.is_empty());
    }

    // Mirrors `ListTransactionsHandlerTest.testBuildRequestWithNullFilteredTransactionalIdPattern`
    // and `...WithEmptyFilteredTransactionalIdPattern`: null/empty leaves the
    // field unset.
    #[test]
    fn build_request_with_null_or_empty_transactional_id_pattern() {
        let null_data =
            handler(ListTransactionsOptions::new().filter_on_transactional_id_pattern(None)).build_batched_request();
        assert_eq!(null_data.transactional_id_pattern, None);
        let empty_data =
            handler(ListTransactionsOptions::new().filter_on_transactional_id_pattern(Some(String::new())))
                .build_batched_request();
        assert_eq!(empty_data.transactional_id_pattern, None);
    }

    // Mirrors the data-level parts of
    // `ListTransactionsHandlerTest.testBuildRequestWithDurationFilter` (default
    // -1, and a set value). Java's version-3 case asserts `build((short) 0)`
    // throws `UnsupportedVersionException` for a set duration filter; the Rust
    // generated message code silently omits below-min-version fields at
    // serialization rather than throwing (a message-generator-level difference,
    // out of scope for this phase), so that case is not translated.
    #[test]
    fn build_request_with_duration_filter() {
        let default_data = handler(ListTransactionsOptions::new()).build_batched_request();
        assert_eq!(default_data.duration_filter, -1);
        let set_data = handler(ListTransactionsOptions::new().filter_on_duration(10)).build_batched_request();
        assert_eq!(set_data.duration_filter, 10);
        assert!(set_data.producer_id_filters.is_empty());
    }

    fn sample_response() -> ConcreteResponse {
        let mut s1 = WireTxnState::new();
        s1.set_transactional_id("foo".to_string());
        s1.set_producer_id(12345);
        s1.set_transaction_state("Ongoing".to_string());
        let mut s2 = WireTxnState::new();
        s2.set_transactional_id("bar".to_string());
        s2.set_producer_id(98765);
        s2.set_transaction_state("PrepareAbort".to_string());
        let mut data = ListTransactionsResponseData::new();
        data.set_error_code(Errors::None.code());
        data.set_transaction_states(vec![s1, s2]);
        ConcreteResponse::ListTransactions(ListTransactionsResponse::new(data))
    }

    // Mirrors `ListTransactionsHandlerTest.testHandleSuccessfulResponse`.
    #[test]
    fn handle_successful_response() {
        let h = handler(ListTransactionsOptions::new());
        let result = h.handle_response(&node(), &HashSet::from([broker_key(1)]), &sample_response());
        assert_eq!(
            result.completed_keys.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from([broker_key(1)])
        );
        let listings = result.completed_keys.get(&broker_key(1)).unwrap();
        assert_eq!(listings.len(), 2);
        let foo = listings.iter().find(|l| l.transactional_id() == "foo").unwrap();
        assert_eq!(foo.producer_id(), 12345);
        assert_eq!(foo.state(), TransactionState::Ongoing);
        let bar = listings.iter().find(|l| l.transactional_id() == "bar").unwrap();
        assert_eq!(bar.state(), TransactionState::PrepareAbort);
    }

    fn handle_response_with_error(error: Errors) -> ApiResult<BrokerKey, Vec<TransactionListing>> {
        let h = handler(ListTransactionsOptions::new());
        let mut data = ListTransactionsResponseData::new();
        data.set_error_code(error.code());
        h.handle_response(
            &node(),
            &HashSet::from([broker_key(1)]),
            &ConcreteResponse::ListTransactions(ListTransactionsResponse::new(data)),
        )
    }

    // Mirrors `ListTransactionsHandlerTest.testCoordinatorLoadingErrorIsRetriable`.
    #[test]
    fn coordinator_loading_error_is_retriable() {
        let result = handle_response_with_error(Errors::CoordinatorLoadInProgress);
        assert!(result.completed_keys.is_empty());
        assert!(result.failed_keys.is_empty());
        assert!(result.unmapped_keys.is_empty());
    }

    // Mirrors `ListTransactionsHandlerTest.testHandleResponseWithFatalErrors`.
    #[test]
    fn handle_response_with_fatal_errors() {
        for error in [Errors::CoordinatorNotAvailable, Errors::UnknownServerError] {
            let result = handle_response_with_error(error);
            assert!(result.completed_keys.is_empty());
            assert!(result.unmapped_keys.is_empty());
            assert_eq!(
                result.failed_keys.keys().cloned().collect::<HashSet<_>>(),
                HashSet::from([broker_key(1)])
            );
            assert_eq!(result.failed_keys.get(&broker_key(1)).unwrap().error(), error);
        }
    }
}
