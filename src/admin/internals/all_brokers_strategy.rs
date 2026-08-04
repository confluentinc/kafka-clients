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

//! The lookup strategy for use cases which fan out to *all* brokers in the
//! cluster.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.AllBrokersStrategy`.
//!
//! This is a slightly degenerate lookup strategy: the broker ids are both the
//! keys and the values, and — unlike [`CoordinatorStrategy`](super::coordinator_strategy::CoordinatorStrategy)
//! and [`PartitionLeaderStrategy`](super::partition_leader_strategy::PartitionLeaderStrategy)
//! — the set of keys is *not* known ahead of time. A single lookup
//! ([`Metadata`]) request discovers the broker ids, which then become the
//! fulfillment keys. This dynamic-key discovery is expressed through the
//! [`LookupResult`]'s `mapped_keys` (brand-new keys the driver did not start
//! with) plus `completed_keys` (the sentinel [`any_broker`] lookup key), and is
//! surfaced to the caller through the more complex
//! `Future<Map<Integer, Future<V>>>` shape of [`AllBrokersFuture::all`].

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use crate::common::KafkaError;
use crate::common::kafka_future::{KafkaFuture, KafkaFutureImpl};
use crate::common::requests::{ConcreteResponse, MetadataRequestBuilder, RequestBuilder};
use crate::common::utils::LogContext;
use crate::kafka_debug;

use super::admin_api_future::AdminApiFuture;
use super::admin_api_lookup_strategy::{AdminApiLookupStrategy, LookupResult};
use super::api_request_scope::ApiRequestScope;

/// A key used by [`AllBrokersStrategy`]. The sentinel key (broker id `None`,
/// [`any_broker`]) drives the single lookup request; each discovered broker
/// becomes a key with `Some(broker_id)`.
///
/// Corresponds to `AllBrokersStrategy.BrokerKey`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct BrokerKey {
    /// The broker id, or `None` for the pre-lookup sentinel ([`any_broker`]).
    pub(crate) broker_id: Option<i32>,
}

impl BrokerKey {
    /// Creates a broker key for the given (optional) broker id.
    pub(crate) fn new(broker_id: Option<i32>) -> Self {
        Self { broker_id }
    }
}

impl std::fmt::Display for BrokerKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BrokerKey(brokerId={:?})", self.broker_id)
    }
}

/// The sentinel "any broker" lookup key.
///
/// Mirrors `AllBrokersStrategy.ANY_BROKER`.
pub(crate) fn any_broker() -> BrokerKey {
    BrokerKey::new(None)
}

/// The (single) set of lookup keys, containing only [`any_broker`].
///
/// Mirrors `AllBrokersStrategy.LOOKUP_KEYS`.
pub(crate) fn lookup_keys() -> HashSet<BrokerKey> {
    HashSet::from([any_broker()])
}

/// The lookup strategy for use cases which require requests to be sent to all
/// brokers in the cluster.
///
/// Corresponds to `AllBrokersStrategy`.
pub(crate) struct AllBrokersStrategy {
    log_context: LogContext,
}

impl AllBrokersStrategy {
    /// Creates a strategy.
    pub(crate) fn new(log_context: LogContext) -> Self {
        Self { log_context }
    }

    /// Mirrors `validateLookupKeys`. Panics (mirroring Java's
    /// `IllegalArgumentException`) if the key set is anything other than the
    /// singleton `{any_broker}` — a programming error the driver never triggers
    /// in production.
    fn validate_lookup_keys(keys: &HashSet<BrokerKey>) {
        assert!(keys.len() == 1, "Unexpected key set: {keys:?}");
        let key = keys.iter().next().expect("checked len == 1");
        assert!(key == &any_broker(), "Unexpected key set: {keys:?}");
    }
}

impl AdminApiLookupStrategy<BrokerKey> for AllBrokersStrategy {
    fn lookup_scope(&self, _key: &BrokerKey) -> ApiRequestScope {
        // A single shared lookup scope (Java's `SINGLE_REQUEST_SCOPE`).
        ApiRequestScope::SingleLookup
    }

    fn build_request(&self, keys: &HashSet<BrokerKey>) -> Box<dyn RequestBuilder> {
        Self::validate_lookup_keys(keys);
        // Send an empty `Metadata` request; we are only interested in the
        // brokers from the response.
        Box::new(MetadataRequestBuilder::new(Some(&[]), false))
    }

    fn handle_response(&self, keys: &HashSet<BrokerKey>, response: &ConcreteResponse) -> LookupResult<BrokerKey> {
        Self::validate_lookup_keys(keys);
        let ConcreteResponse::Metadata(response) = response else {
            return LookupResult::new(HashMap::new(), HashMap::new());
        };

        let brokers = &response.data().brokers;
        if brokers.is_empty() {
            kafka_debug!(
                self.log_context,
                "Metadata response contained no brokers. Will backoff and retry"
            );
            // `LookupResult.empty()` — no completed/mapped/failed keys, so the
            // driver retries the lookup.
            return LookupResult {
                completed_keys: Vec::new(),
                mapped_keys: HashMap::new(),
                failed_keys: HashMap::new(),
            };
        }
        kafka_debug!(self.log_context, "Discovered all brokers {:?} to send requests to", brokers);

        let mapped_keys: HashMap<BrokerKey, i32> = brokers
            .iter()
            .map(|broker| (BrokerKey::new(Some(broker.node_id)), broker.node_id))
            .collect();

        // The sentinel lookup key is "completed" by the lookup itself; every
        // discovered broker id is a brand-new mapped fulfillment key.
        LookupResult { completed_keys: vec![any_broker()], mapped_keys, failed_keys: HashMap::new() }
    }
}

/// The future bundle backing an all-brokers RPC.
///
/// Corresponds to `AllBrokersStrategy.AllBrokersFuture<V>`. The top-level
/// [`future`](Self::all) completes when the broker list is discovered, yielding
/// a map from broker id to that broker's per-request future; each per-broker
/// future completes when its fulfillment response arrives.
pub(crate) struct AllBrokersFuture<V: Clone + Send + Sync + 'static> {
    future: KafkaFutureImpl<HashMap<i32, KafkaFuture<V>>>,
    broker_futures: Mutex<HashMap<i32, KafkaFutureImpl<V>>>,
}

impl<V: Clone + Send + Sync + 'static> AllBrokersFuture<V> {
    /// Creates an empty all-brokers future bundle.
    pub(crate) fn new() -> Self {
        Self { future: KafkaFutureImpl::new(), broker_futures: Mutex::new(HashMap::new()) }
    }

    /// The top-level future yielding the per-broker future map once the broker
    /// list is discovered.
    ///
    /// Mirrors `AllBrokersFuture.all`.
    pub(crate) fn all(&self) -> KafkaFuture<HashMap<i32, KafkaFuture<V>>> {
        self.future.future()
    }

    /// Completes the per-broker future for `broker_id`. Mirrors the private
    /// `AllBrokersFuture.futureOrThrow(key).complete(value)`; panics on an
    /// unknown broker id (Java's `IllegalArgumentException`).
    fn complete_broker(&self, broker_id: i32, value: V) {
        let futures = self.broker_futures.lock().unwrap();
        let future = futures
            .get(&broker_id)
            .unwrap_or_else(|| panic!("Attempt to complete with unknown broker id: {broker_id}"));
        future.complete(value);
    }

    fn complete_broker_exceptionally(&self, broker_id: i32, error: KafkaError) {
        let futures = self.broker_futures.lock().unwrap();
        let future = futures
            .get(&broker_id)
            .unwrap_or_else(|| panic!("Attempt to complete with unknown broker id: {broker_id}"));
        future.complete_exceptionally(error);
    }
}

impl<V: Clone + Send + Sync + 'static> AdminApiFuture<BrokerKey, V> for AllBrokersFuture<V> {
    fn lookup_keys(&self) -> HashSet<BrokerKey> {
        lookup_keys()
    }

    fn complete_lookup(&self, broker_id_mapping: HashMap<BrokerKey, i32>) {
        let public_map = {
            let mut broker_futures = self.broker_futures.lock().unwrap();
            for (broker_key, broker_id) in &broker_id_mapping {
                assert!(
                    broker_id == &broker_key.broker_id.unwrap_or(-1),
                    "Invalid lookup mapping {broker_key} -> {broker_id}"
                );
                broker_futures.insert(*broker_id, KafkaFutureImpl::new());
            }
            broker_futures
                .iter()
                .map(|(id, f)| (*id, f.future()))
                .collect::<HashMap<i32, KafkaFuture<V>>>()
        };
        self.future.complete(public_map);
    }

    fn complete_lookup_exceptionally(&self, lookup_errors: HashMap<BrokerKey, KafkaError>) {
        assert!(
            lookup_errors.keys().cloned().collect::<HashSet<_>>() == lookup_keys(),
            "Unexpected keys among lookup errors: {lookup_errors:?}"
        );
        let error = lookup_errors.into_values().next().expect("lookup_keys is non-empty");
        self.future.complete_exceptionally(error);
    }

    fn complete(&self, values: HashMap<BrokerKey, V>) {
        for (key, value) in values {
            assert!(key != any_broker(), "Invalid attempt to complete with lookup key sentinel");
            let broker_id = key.broker_id.expect("non-sentinel broker key has a broker id");
            self.complete_broker(broker_id, value);
        }
    }

    fn complete_exceptionally(&self, errors: HashMap<BrokerKey, KafkaError>) {
        for (key, error) in errors {
            match key.broker_id {
                None => {
                    self.future.complete_exceptionally(error);
                },
                Some(broker_id) => self.complete_broker_exceptionally(broker_id, error),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::ApiKeys;
    use crate::common::requests::MetadataResponse;
    use crate::metadata_response_data::{MetadataResponseBroker, MetadataResponseData};

    fn log_context() -> LogContext {
        LogContext::new("[test] ")
    }

    fn metadata_response(broker_ids: &[i32]) -> ConcreteResponse {
        let mut data = MetadataResponseData::new();
        let brokers: Vec<MetadataResponseBroker> = broker_ids
            .iter()
            .map(|&id| {
                let mut b = MetadataResponseBroker::new();
                b.set_node_id(id);
                b.set_host(format!("host{id}"));
                b.set_port(9092);
                b
            })
            .collect();
        data.set_brokers(brokers);
        ConcreteResponse::Metadata(MetadataResponse::new(data, ApiKeys::METADATA.latest_version()))
    }

    // Mirrors `AllBrokersStrategyTest.testBuildRequest`.
    #[test]
    fn build_request() {
        let strategy = AllBrokersStrategy::new(log_context());
        let request = strategy.build_request(&lookup_keys()).build().unwrap();
        let crate::common::requests::ConcreteRequest::Metadata(m) = request else {
            panic!("expected metadata request");
        };
        assert_eq!(m.topics(), Some(Vec::new()));
    }

    // Mirrors `AllBrokersStrategyTest.testBuildRequestWithInvalidLookupKeys`.
    #[test]
    fn build_request_with_invalid_lookup_keys() {
        let strategy = AllBrokersStrategy::new(log_context());
        let key1 = any_broker();
        let key2 = BrokerKey::new(Some(1));
        for keys in [
            HashSet::from([key2.clone()]),
            HashSet::from([key1.clone(), key2.clone()]),
        ] {
            assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| strategy.build_request(&keys))).is_err());
        }
        // A superset of the lookup keys is also invalid.
        let mut keys = lookup_keys();
        keys.insert(key2);
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| strategy.build_request(&keys))).is_err());
    }

    // Mirrors `AllBrokersStrategyTest.testHandleResponse`.
    #[test]
    fn handle_response() {
        let strategy = AllBrokersStrategy::new(log_context());
        let result = strategy.handle_response(&lookup_keys(), &metadata_response(&[1, 2]));
        assert!(result.failed_keys.is_empty());
        assert_eq!(
            result.mapped_keys.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from([BrokerKey::new(Some(1)), BrokerKey::new(Some(2))])
        );
        for (broker_key, broker_id) in &result.mapped_keys {
            assert_eq!(broker_key.broker_id, Some(*broker_id));
        }
        assert_eq!(result.completed_keys, vec![any_broker()]);
    }

    // Mirrors `AllBrokersStrategyTest.testHandleResponseWithNoBrokers`.
    #[test]
    fn handle_response_with_no_brokers() {
        let strategy = AllBrokersStrategy::new(log_context());
        let result = strategy.handle_response(&lookup_keys(), &metadata_response(&[]));
        assert!(result.failed_keys.is_empty());
        assert!(result.mapped_keys.is_empty());
    }

    // Mirrors `AllBrokersStrategyTest.testHandleResponseWithInvalidLookupKeys`.
    #[test]
    fn handle_response_with_invalid_lookup_keys() {
        let strategy = AllBrokersStrategy::new(log_context());
        let key1 = any_broker();
        let key2 = BrokerKey::new(Some(1));
        let response = metadata_response(&[]);
        for keys in [
            HashSet::from([key2.clone()]),
            HashSet::from([key1.clone(), key2.clone()]),
        ] {
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| strategy.handle_response(&keys, &response)))
                    .is_err()
            );
        }
        let mut keys = lookup_keys();
        keys.insert(key2);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| strategy.handle_response(&keys, &response)))
                .is_err()
        );
    }
}

/// Translation of `AllBrokersStrategyIntegrationTest`: drives the real
/// [`AdminApiDriver`](super::admin_api_driver::AdminApiDriver) with
/// [`AllBrokersStrategy`] + [`AllBrokersFuture`] end-to-end, proving the Tier-1
/// driver cleanly supports dynamically-discovered lookup keys.
#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::admin::internals::admin_api_driver::AdminApiDriver;
    use crate::admin::internals::admin_api_handler::{AdminApiHandler, ApiResult, RequestAndKeys};
    use crate::common::Node;
    use crate::common::protocol::{ApiKeys, Errors};
    use crate::common::requests::MetadataResponse;
    use crate::common::utils::ExponentialBackoff;
    use crate::metadata_response_data::{MetadataResponseBroker, MetadataResponseData};

    const TIMEOUT_MS: i64 = 5000;
    const RETRY_BACKOFF_MS: i64 = 100;
    const NOW: i64 = 0;

    /// Mirrors the anonymous `MockApiHandler` (`Batched`): a metadata fulfillment
    /// request per broker, completing each key with the responding broker's id.
    struct MockApiHandler {
        strategy: AllBrokersStrategy,
    }

    impl AdminApiHandler<BrokerKey, i32> for MockApiHandler {
        fn api_name(&self) -> &str {
            "mock-api"
        }

        fn build_request(&self, _broker_id: i32, keys: &HashSet<BrokerKey>) -> Vec<RequestAndKeys<BrokerKey>> {
            vec![RequestAndKeys {
                request: Box::new(MetadataRequestBuilder::new(Some(&[]), false)),
                keys: keys.clone(),
            }]
        }

        fn handle_response(
            &self,
            broker: &Node,
            keys: &HashSet<BrokerKey>,
            _response: &ConcreteResponse,
        ) -> ApiResult<BrokerKey, i32> {
            let key = keys.iter().next().expect("exactly one key").clone();
            ApiResult::new(HashMap::from([(key, broker.id())]), HashMap::new(), Vec::new())
        }

        fn lookup_strategy(&self) -> &dyn AdminApiLookupStrategy<BrokerKey> {
            &self.strategy
        }
    }

    fn build_driver(future: AllBrokersFuture<i32>) -> AdminApiDriver<BrokerKey, i32> {
        let backoff = ExponentialBackoff::new(RETRY_BACKOFF_MS, 2, RETRY_BACKOFF_MS, 0.0).unwrap();
        AdminApiDriver::new(
            Box::new(MockApiHandler { strategy: AllBrokersStrategy::new(LogContext::new("[test] ")) }),
            Box::new(future),
            NOW + TIMEOUT_MS,
            backoff,
            LogContext::new("[test] "),
        )
    }

    fn response_with_brokers(broker_ids: &[i32]) -> ConcreteResponse {
        let mut data = MetadataResponseData::new();
        let brokers: Vec<MetadataResponseBroker> = broker_ids
            .iter()
            .map(|&id| {
                let mut b = MetadataResponseBroker::new();
                b.set_node_id(id);
                b.set_host(format!("host{id}"));
                b.set_port(9092);
                b
            })
            .collect();
        data.set_brokers(brokers);
        ConcreteResponse::Metadata(MetadataResponse::new(data, ApiKeys::METADATA.latest_version()))
    }

    fn placeholder_response() -> ConcreteResponse {
        ConcreteResponse::Metadata(MetadataResponse::new(
            MetadataResponseData::new(),
            ApiKeys::METADATA.latest_version(),
        ))
    }

    fn unknown_server_error() -> KafkaError {
        KafkaError::new(Errors::UnknownServerError)
    }

    fn network_exception() -> KafkaError {
        // Java's `DisconnectException`; signalled to the driver as NetworkException.
        KafkaError::new(Errors::NetworkException)
    }

    // Mirrors `testFatalLookupError`.
    #[tokio::test]
    async fn fatal_lookup_error() {
        let result = AllBrokersFuture::<i32>::new();
        let all = result.all();
        let mut driver = build_driver(result);

        let specs = driver.poll();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].keys, lookup_keys());

        driver.on_failure(NOW, &specs[0].scope, &specs[0].keys, &unknown_server_error());
        assert!(all.is_done());
        assert_eq!(all.get().await.unwrap_err().error(), Errors::UnknownServerError);
        assert!(driver.poll().is_empty());
    }

    // Mirrors `testRetryLookupAfterDisconnect`.
    #[tokio::test]
    async fn retry_lookup_after_disconnect() {
        let result = AllBrokersFuture::<i32>::new();
        let mut driver = build_driver(result);

        let specs = driver.poll();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].keys, lookup_keys());

        driver.on_failure(NOW, &specs[0].scope, &specs[0].keys, &network_exception());
        let retry_specs = driver.poll();
        assert_eq!(retry_specs.len(), 1);
        assert_eq!(retry_specs[0].keys, lookup_keys());
        assert_eq!(retry_specs[0].next_allowed_try_ms, NOW);
        assert!(driver.poll().is_empty());
    }

    // Mirrors `testMultiBrokerCompletion`.
    #[tokio::test]
    async fn multi_broker_completion() {
        let result = AllBrokersFuture::<i32>::new();
        let all = result.all();
        let mut driver = build_driver(result);

        let lookup_specs = driver.poll();
        assert_eq!(lookup_specs.len(), 1);
        driver.on_response(
            NOW,
            &lookup_specs[0].scope,
            &lookup_specs[0].keys,
            &response_with_brokers(&[1, 2]),
            Node::no_node(),
        );
        assert!(all.is_done());
        let broker_futures = all.get().await.unwrap();

        let specs = driver.poll();
        assert_eq!(specs.len(), 2);

        let broker_id1 = specs[0].scope.destination_broker_id().expect("fulfillment scope");
        assert!([1, 2].contains(&broker_id1));
        driver.on_response(NOW, &specs[0].scope, &specs[0].keys, &placeholder_response(), Node::no_node());
        assert!(broker_futures[&broker_id1].is_done());

        let broker_id2 = specs[1].scope.destination_broker_id().expect("fulfillment scope");
        assert_ne!(broker_id1, broker_id2);
        assert!([1, 2].contains(&broker_id2));
        driver.on_response(NOW, &specs[1].scope, &specs[1].keys, &placeholder_response(), Node::no_node());
        assert!(broker_futures[&broker_id2].is_done());
        assert!(driver.poll().is_empty());
    }

    // Mirrors `testRetryFulfillmentAfterDisconnect`.
    #[tokio::test]
    async fn retry_fulfillment_after_disconnect() {
        let result = AllBrokersFuture::<i32>::new();
        let all = result.all();
        let mut driver = build_driver(result);

        let lookup_specs = driver.poll();
        assert_eq!(lookup_specs.len(), 1);
        let broker_id = 1;
        driver.on_response(
            NOW,
            &lookup_specs[0].scope,
            &lookup_specs[0].keys,
            &response_with_brokers(&[broker_id]),
            Node::no_node(),
        );
        assert!(all.is_done());
        let broker_futures = all.get().await.unwrap();
        let future = broker_futures[&broker_id].clone();
        assert!(!future.is_done());

        let specs = driver.poll();
        assert_eq!(specs.len(), 1);
        driver.on_failure(NOW, &specs[0].scope, &specs[0].keys, &network_exception());
        assert!(!future.is_done());

        let retry_specs = driver.poll();
        assert_eq!(retry_specs.len(), 1);
        assert_eq!(retry_specs[0].next_allowed_try_ms, NOW + RETRY_BACKOFF_MS);
        assert_eq!(retry_specs[0].scope.destination_broker_id(), Some(broker_id));

        let node = Node::new(broker_id, "host".to_string(), 1234);
        driver.on_response(NOW, &retry_specs[0].scope, &retry_specs[0].keys, &placeholder_response(), &node);
        assert!(future.is_done());
        assert_eq!(future.get().await.unwrap(), broker_id);
        assert!(driver.poll().is_empty());
    }

    // Mirrors `testFatalFulfillmentError`.
    #[tokio::test]
    async fn fatal_fulfillment_error() {
        let result = AllBrokersFuture::<i32>::new();
        let all = result.all();
        let mut driver = build_driver(result);

        let lookup_specs = driver.poll();
        assert_eq!(lookup_specs.len(), 1);
        let broker_id = 1;
        driver.on_response(
            NOW,
            &lookup_specs[0].scope,
            &lookup_specs[0].keys,
            &response_with_brokers(&[broker_id]),
            Node::no_node(),
        );
        let broker_futures = all.get().await.unwrap();
        let future = broker_futures[&broker_id].clone();
        assert!(!future.is_done());

        let specs = driver.poll();
        assert_eq!(specs.len(), 1);
        driver.on_failure(NOW, &specs[0].scope, &specs[0].keys, &unknown_server_error());
        assert!(future.is_done());
        assert_eq!(future.get().await.unwrap_err().error(), Errors::UnknownServerError);
        assert!(driver.poll().is_empty());
    }
}
