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

//! Handler for the `fenceProducers` API (transaction-coordinator targeted, uses
//! `InitProducerId`).
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.FenceProducersHandler`.

use std::collections::{HashMap, HashSet};

use crate::admin::options::FenceProducersOptions;
use crate::common::protocol::Errors;
use crate::common::requests::{ConcreteResponse, CoordinatorType, InitProducerIdRequestBuilder, RequestBuilder};
use crate::common::utils::{LogContext, ProducerIdAndEpoch};
use crate::common::{Error, Node};
use crate::init_producer_id_request_data::InitProducerIdRequestData;
use crate::kafka_debug;

use super::admin_api_future::SimpleAdminApiFuture;
use super::admin_api_handler::{AdminApiHandler, ApiResult, RequestAndKeys};
use super::admin_api_lookup_strategy::AdminApiLookupStrategy;
use super::coordinator_key::CoordinatorKey;
use super::coordinator_strategy::CoordinatorStrategy;

/// Handler for `fenceProducers`.
///
/// Corresponds to `FenceProducersHandler` (an `Unbatched` handler over
/// `CoordinatorKey` keys yielding [`ProducerIdAndEpoch`] values — one
/// `InitProducerId` request per transactional id).
///
/// Note on the `Unbatched` translation: Java's `Unbatched.buildRequest` returns
/// one request per key and the driver sends them all in a single cycle. The
/// Rust `AdminApiDriver` issues only the first request produced per broker per
/// poll cycle, so multiple transactional ids that resolve to the *same*
/// coordinator are fenced serially (one per completed request) rather than in
/// parallel. This is functionally equivalent (all keys still complete) and is
/// not observable by the in-scope tests / `forceTerminateTransaction`, which
/// each fence a single id.
pub(crate) struct FenceProducersHandler {
    log_context: LogContext,
    lookup_strategy: CoordinatorStrategy,
    txn_timeout_ms: i32,
}

impl FenceProducersHandler {
    /// Creates a handler. The transaction timeout is the option's timeout when
    /// set, otherwise the client's request timeout.
    pub(crate) fn new(options: &FenceProducersOptions, log_context: LogContext, request_timeout_ms: i32) -> Self {
        let txn_timeout_ms = options.timeout().unwrap_or(request_timeout_ms);
        Self {
            lookup_strategy: CoordinatorStrategy::new(CoordinatorType::Transaction, log_context.clone()),
            log_context,
            txn_timeout_ms,
        }
    }

    /// Builds the key set for a collection of transactional ids.
    fn build_key_set(transactional_ids: &[String]) -> HashSet<CoordinatorKey> {
        transactional_ids.iter().map(CoordinatorKey::by_transactional_id).collect()
    }

    /// Creates the future bundle for the given transactional ids.
    ///
    /// Mirrors `FenceProducersHandler.newFuture`.
    pub(crate) fn new_future(transactional_ids: &[String]) -> SimpleAdminApiFuture<CoordinatorKey, ProducerIdAndEpoch> {
        SimpleAdminApiFuture::for_keys(Self::build_key_set(transactional_ids))
    }

    /// Builds a single `InitProducerId` request for the given key.
    ///
    /// Mirrors `buildSingleRequest`.
    fn build_single_request(&self, key: &CoordinatorKey) -> InitProducerIdRequestData {
        assert!(
            key.coordinator_type == CoordinatorType::Transaction,
            "Invalid group coordinator key {key} when building `InitProducerId` request"
        );
        let mut data = InitProducerIdRequestData::new();
        // Because we never include a producer epoch or ID in this request, we
        // expect that some errors (such as PRODUCER_FENCED) will never be
        // returned in the corresponding broker response.
        data.set_producer_epoch(ProducerIdAndEpoch::NONE.epoch);
        data.set_producer_id(ProducerIdAndEpoch::NONE.producer_id);
        data.set_transactional_id(Some(key.id_value.clone()));
        // This timeout is used by the coordinator to append the record with the
        // new producer epoch to the transaction log.
        data.set_transaction_timeout_ms(self.txn_timeout_ms);
        data
    }

    /// Classifies an `InitProducerId` error.
    ///
    /// Mirrors `handleError`.
    fn handle_error(&self, key: &CoordinatorKey, error: Errors) -> ApiResult<CoordinatorKey, ProducerIdAndEpoch> {
        match error {
            Errors::ClusterAuthorizationFailed => failed(
                key.clone(),
                Error::with_message(
                    error,
                    format!(
                        "InitProducerId request for transactionalId `{}` failed due to cluster authorization failure",
                        key.id_value
                    ),
                ),
            ),
            Errors::TransactionalIdAuthorizationFailed => failed(
                key.clone(),
                Error::with_message(
                    error,
                    format!(
                        "InitProducerId request for transactionalId `{}` failed due to transactional ID authorization failure",
                        key.id_value
                    ),
                ),
            ),
            Errors::CoordinatorLoadInProgress => {
                // If the coordinator is in the middle of loading, then we just need to retry.
                kafka_debug!(
                    self.log_context,
                    "InitProducerId request for transactionalId `{}` failed because the coordinator is still in the process of loading state. Will retry",
                    key.id_value
                );
                empty()
            },
            Errors::ConcurrentTransactions => {
                kafka_debug!(
                    self.log_context,
                    "InitProducerId request for transactionalId `{}` failed because of a concurrent transaction. Will retry",
                    key.id_value
                );
                empty()
            },
            Errors::NotCoordinator | Errors::CoordinatorNotAvailable => {
                // If the coordinator is unavailable or there was a coordinator change, then we
                // unmap the key so that we retry the `FindCoordinator` request.
                kafka_debug!(
                    self.log_context,
                    "InitProducerId request for transactionalId `{}` returned error {:?}. Will attempt to find the coordinator again and retry",
                    key.id_value,
                    error
                );
                unmapped(key.clone())
            },
            // We intentionally omit cases for PRODUCER_FENCED, TRANSACTIONAL_ID_NOT_FOUND, and
            // INVALID_PRODUCER_EPOCH since those errors should never happen when our
            // InitProducerIdRequest doesn't include a producer epoch or ID, and should therefore
            // fall under the "unexpected error" catch-all case below.
            _ => failed(
                key.clone(),
                Error::with_message(
                    error,
                    format!(
                        "InitProducerId request for transactionalId `{}` failed due to unexpected error",
                        key.id_value
                    ),
                ),
            ),
        }
    }
}

/// Mirrors `ApiResult.failed(key, error)`.
fn failed(key: CoordinatorKey, error: Error) -> ApiResult<CoordinatorKey, ProducerIdAndEpoch> {
    ApiResult::new(HashMap::new(), HashMap::from([(key, error)]), Vec::new())
}

/// Mirrors `ApiResult.unmapped(singletonList(key))`.
fn unmapped(key: CoordinatorKey) -> ApiResult<CoordinatorKey, ProducerIdAndEpoch> {
    ApiResult::new(HashMap::new(), HashMap::new(), vec![key])
}

/// Mirrors `ApiResult.empty()`.
fn empty() -> ApiResult<CoordinatorKey, ProducerIdAndEpoch> {
    ApiResult::new(HashMap::new(), HashMap::new(), Vec::new())
}

impl AdminApiHandler<CoordinatorKey, ProducerIdAndEpoch> for FenceProducersHandler {
    fn api_name(&self) -> &str {
        "fenceProducer"
    }

    fn build_request(&self, _broker_id: i32, keys: &HashSet<CoordinatorKey>) -> Vec<RequestAndKeys<CoordinatorKey>> {
        // Unbatched: one `InitProducerId` request per key.
        keys.iter()
            .map(|key| {
                let data = self.build_single_request(key);
                RequestAndKeys {
                    request: Box::new(InitProducerIdRequestBuilder::new(data)) as Box<dyn RequestBuilder>,
                    keys: HashSet::from([key.clone()]),
                }
            })
            .collect()
    }

    fn handle_response(
        &self,
        _broker: &Node,
        keys: &HashSet<CoordinatorKey>,
        response: &ConcreteResponse,
    ) -> ApiResult<CoordinatorKey, ProducerIdAndEpoch> {
        // Unbatched handlers receive exactly one key per response.
        let key = keys
            .iter()
            .next()
            .expect("fenceProducers response must carry exactly one key")
            .clone();
        let ConcreteResponse::InitProducerId(response) = response else {
            // NOT `ApiResult.empty()`: Java reaches `empty()` only from the
            // per-partition loop, never for a response-type mismatch. That case is
            // `KafkaAdminClient.java:1387-1391`'s `catch (Throwable t)` →
            // `call.fail(now, t)`, which fails the call once instead of leaving the
            // driver to re-issue the request until the deadline. See
            // `ApiResult::failed_all`.
            return ApiResult::failed_all(
                keys,
                Error::illegal_state("FenceProducersHandler received an unexpected response type"),
            );
        };

        let error = Errors::for_code(response.data().error_code);
        if error != Errors::None {
            return self.handle_error(&key, error);
        }

        let value = ProducerIdAndEpoch::new(response.data().producer_id, response.data().producer_epoch);
        ApiResult::new(HashMap::from([(key, value)]), HashMap::new(), Vec::new())
    }

    fn lookup_strategy(&self) -> &dyn AdminApiLookupStrategy<CoordinatorKey> {
        &self.lookup_strategy
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::requests::InitProducerIdResponse;
    use crate::init_producer_id_response_data::InitProducerIdResponseData;

    fn log_context() -> LogContext {
        LogContext::new("[test] ")
    }

    fn node() -> Node {
        Node::new(1, "host".to_string(), 1234)
    }

    fn key(id: &str) -> CoordinatorKey {
        CoordinatorKey::by_transactional_id(id)
    }

    const REQUEST_TIMEOUT_MS: i32 = 30000;

    fn handler(options: FenceProducersOptions) -> FenceProducersHandler {
        FenceProducersHandler::new(&options, log_context(), REQUEST_TIMEOUT_MS)
    }

    fn init_producer_id_response(data: InitProducerIdResponseData) -> ConcreteResponse {
        ConcreteResponse::InitProducerId(InitProducerIdResponse::new(data))
    }

    // Mirrors `FenceProducersHandlerTest.testBuildRequest`.
    #[test]
    fn build_request() {
        let h = handler(FenceProducersOptions::new());
        for id in ["foo", "bar", "baz"] {
            let data = h.build_single_request(&key(id));
            assert_eq!(data.transactional_id.as_deref(), Some(id));
            assert_eq!(data.transaction_timeout_ms, REQUEST_TIMEOUT_MS);
        }
    }

    // Mirrors `FenceProducersHandlerTest.testBuildRequestOptionsTimeout`.
    #[test]
    fn build_request_options_timeout() {
        let options_timeout_ms = 50000;
        let h = handler(FenceProducersOptions::new().timeout_ms(Some(options_timeout_ms)));
        for id in ["foo", "bar", "baz"] {
            let data = h.build_single_request(&key(id));
            assert_eq!(data.transactional_id.as_deref(), Some(id));
            assert_eq!(data.transaction_timeout_ms, options_timeout_ms);
        }
    }

    // Mirrors `FenceProducersHandlerTest.testHandleSuccessfulResponse`.
    #[test]
    fn handle_successful_response() {
        let h = handler(FenceProducersOptions::new());
        let epoch: i16 = 57;
        let producer_id: i64 = 7;
        let mut data = InitProducerIdResponseData::new();
        data.set_producer_epoch(epoch);
        data.set_producer_id(producer_id);
        let result = h.handle_response(&node(), &HashSet::from([key("foo")]), &init_producer_id_response(data));
        assert!(result.unmapped_keys.is_empty());
        assert!(result.failed_keys.is_empty());
        assert_eq!(
            result.completed_keys.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from([key("foo")])
        );
        assert_eq!(
            result.completed_keys.get(&key("foo")).unwrap(),
            &ProducerIdAndEpoch::new(producer_id, epoch)
        );
    }

    fn handle_response_error(error: Errors) -> ApiResult<CoordinatorKey, ProducerIdAndEpoch> {
        let h = handler(FenceProducersOptions::new());
        let mut data = InitProducerIdResponseData::new();
        data.set_error_code(error.code());
        let result = h.handle_response(&node(), &HashSet::from([key("foo")]), &init_producer_id_response(data));
        assert!(result.completed_keys.is_empty());
        result
    }

    // Mirrors `FenceProducersHandlerTest.testHandleErrorResponse`.
    #[test]
    fn handle_error_response() {
        for error in [
            Errors::TransactionalIdAuthorizationFailed,
            Errors::ClusterAuthorizationFailed,
            Errors::UnknownServerError,
            Errors::ProducerFenced,
            Errors::TransactionalIdNotFound,
            Errors::InvalidProducerEpoch,
        ] {
            let result = handle_response_error(error);
            assert!(result.unmapped_keys.is_empty());
            assert_eq!(
                result.failed_keys.keys().cloned().collect::<HashSet<_>>(),
                HashSet::from([key("foo")])
            );
            assert_eq!(result.failed_keys.get(&key("foo")).unwrap().error(), error);
        }
        for error in [Errors::CoordinatorLoadInProgress, Errors::ConcurrentTransactions] {
            let result = handle_response_error(error);
            assert!(result.unmapped_keys.is_empty());
            assert!(result.failed_keys.is_empty());
        }
        for error in [Errors::NotCoordinator, Errors::CoordinatorNotAvailable] {
            let result = handle_response_error(error);
            assert!(result.failed_keys.is_empty());
            assert_eq!(result.unmapped_keys, vec![key("foo")]);
        }
    }
}
