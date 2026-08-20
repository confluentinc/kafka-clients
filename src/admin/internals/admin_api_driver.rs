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

//! The multi-stage (lookup then fulfillment) request driver.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.AdminApiDriver`.
//!
//! This engine backs admin RPCs that must first discover the target broker for
//! each key (e.g. the partition leader for `deleteRecords`) and then send the
//! fulfillment request to that broker, with bidirectional transitions between
//! the two stages. It is driven by the single admin background task via the
//! `Call` bridge in `kafka_admin_client` (mirroring Java's `newCall` /
//! `maybeSendRequests`).

use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::hash::Hash;

use crate::common::protocol::Errors;
use crate::common::requests::{ConcreteResponse, RequestBuilder};
use crate::common::utils::{ExponentialBackoff, LogContext};
use crate::common::{Error, Node};
use crate::kafka_debug;

use super::admin_api_future::AdminApiFuture;
use super::admin_api_handler::AdminApiHandler;
use super::api_request_scope::ApiRequestScope;

/// A request that needs to be sent, produced by [`AdminApiDriver::poll`].
///
/// Corresponds to `AdminApiDriver.RequestSpec`. Unlike Java, the built request
/// builder is carried by value (moved into the `Call` created for the spec);
/// the driver's [`RequestState`] tracks only whether a request is in flight.
pub(crate) struct RequestSpec<K> {
    /// Human-readable name, e.g. `deleteRecords(api=DeleteRecords)`.
    pub(crate) name: String,
    /// The request scope (lookup vs. fulfillment-to-broker).
    pub(crate) scope: ApiRequestScope,
    /// The keys covered by this request.
    pub(crate) keys: HashSet<K>,
    /// The built request.
    pub(crate) request: Box<dyn RequestBuilder>,
    /// The earliest time this request may be attempted (backoff gate).
    pub(crate) next_allowed_try_ms: i64,
    /// The overall deadline for the driver.
    pub(crate) deadline_ms: i64,
    /// The number of attempts already made within this scope.
    pub(crate) tries: i32,
}

/// Tracks the request state within a request scope, enforcing at most one
/// in-flight request and the backoff/retry state.
///
/// Corresponds to `AdminApiDriver.RequestState`. Java stores the in-flight
/// `RequestSpec`, but only ever reads its presence, so a bool suffices here.
struct RequestState {
    has_inflight: bool,
    tries: i32,
    next_allowed_retry_ms: i64,
}

impl RequestState {
    fn new() -> Self {
        Self { has_inflight: false, tries: 0, next_allowed_retry_ms: 0 }
    }

    fn clear_inflight(&mut self, next_allowed_retry_ms: i64) {
        self.has_inflight = false;
        self.next_allowed_retry_ms = next_allowed_retry_ms;
    }

    fn set_inflight(&mut self) {
        self.has_inflight = true;
        self.tries += 1;
    }
}

/// A bi-directional mapping from a scope to a set of keys, where each key maps
/// to one and only one scope.
///
/// Corresponds to `AdminApiDriver.BiMultimap`.
struct BiMultimap<K> {
    reverse_map: HashMap<K, ApiRequestScope>,
    map: HashMap<ApiRequestScope, HashSet<K>>,
}

impl<K: Clone + Eq + Hash> BiMultimap<K> {
    fn new() -> Self {
        Self { reverse_map: HashMap::new(), map: HashMap::new() }
    }

    fn put(&mut self, scope: ApiRequestScope, key: K) {
        self.remove(&key);
        self.reverse_map.insert(key.clone(), scope.clone());
        self.map.entry(scope).or_default().insert(key);
    }

    fn remove(&mut self, key: &K) {
        if let Some(scope) = self.reverse_map.remove(key)
            && let Some(set) = self.map.get_mut(&scope)
        {
            set.remove(key);
            if set.is_empty() {
                self.map.remove(&scope);
            }
        }
    }

    /// A snapshot of the entries as owned `(scope, keys)` pairs, so callers can
    /// iterate while mutating other driver fields.
    fn entries_snapshot(&self) -> Vec<(ApiRequestScope, HashSet<K>)> {
        self.map.iter().map(|(scope, keys)| (scope.clone(), keys.clone())).collect()
    }
}

/// The multi-stage request driver. Generic over the key type `K` (routing
/// granularity, e.g. `TopicPartition`) and value type `V` (the per-key result).
///
/// Corresponds to `AdminApiDriver<K, V>`.
pub(crate) struct AdminApiDriver<K, V> {
    handler: Box<dyn AdminApiHandler<K, V>>,
    future: Box<dyn AdminApiFuture<K, V>>,
    deadline_ms: i64,
    retry_backoff: ExponentialBackoff,
    log_context: LogContext,
    lookup_map: BiMultimap<K>,
    fulfillment_map: BiMultimap<K>,
    request_states: HashMap<ApiRequestScope, RequestState>,
}

impl<K, V> AdminApiDriver<K, V>
where
    K: Clone + Eq + Hash + Display + Send + 'static,
    V: Send + 'static,
{
    /// Creates a driver, seeding the fulfillment/lookup maps from the future's
    /// cached key-to-broker mapping.
    pub(crate) fn new(
        handler: Box<dyn AdminApiHandler<K, V>>,
        future: Box<dyn AdminApiFuture<K, V>>,
        deadline_ms: i64,
        retry_backoff: ExponentialBackoff,
        log_context: LogContext,
    ) -> Self {
        let mut driver = Self {
            handler,
            future,
            deadline_ms,
            retry_backoff,
            log_context,
            lookup_map: BiMultimap::new(),
            fulfillment_map: BiMultimap::new(),
            request_states: HashMap::new(),
        };
        // For cached keys we can skip straight to fulfillment; unknown keys go
        // to the lookup stage.
        for (key, broker_id) in driver.future.cached_key_broker_id_mapping() {
            if broker_id == super::admin_api_future::UNKNOWN_BROKER_ID {
                driver.unmap(&key);
            } else {
                driver.fulfillment_map.put(ApiRequestScope::Fulfillment(broker_id), key);
            }
        }
        driver
    }

    /// Associates a key with a broker id (after lookup reveals the mapping).
    ///
    /// Mirrors `map`.
    fn map(&mut self, key: K, broker_id: i32) {
        self.lookup_map.remove(&key);
        self.fulfillment_map.put(ApiRequestScope::Fulfillment(broker_id), key);
    }

    /// Disassociates a key from its broker, sending it back to lookup.
    ///
    /// Mirrors `unmap`.
    fn unmap(&mut self, key: &K) {
        self.fulfillment_map.remove(key);

        let lookup_scope = self.handler.lookup_strategy().lookup_scope(key);
        match lookup_scope.destination_broker_id() {
            Some(broker_id) => {
                self.fulfillment_map.put(ApiRequestScope::Fulfillment(broker_id), key.clone());
            },
            None => {
                self.lookup_map.put(lookup_scope, key.clone());
            },
        }
    }

    fn clear(&mut self, keys: impl IntoIterator<Item = K>) {
        for key in keys {
            self.lookup_map.remove(&key);
            self.fulfillment_map.remove(&key);
        }
    }

    /// Completes the given keys exceptionally and removes them from both stages.
    ///
    /// Mirrors `completeExceptionally`.
    fn complete_exceptionally(&mut self, errors: HashMap<K, Error>) {
        if !errors.is_empty() {
            let keys: Vec<K> = errors.keys().cloned().collect();
            self.future.complete_exceptionally(errors);
            self.clear(keys);
        }
    }

    fn complete_lookup_exceptionally(&mut self, errors: HashMap<K, Error>) {
        if !errors.is_empty() {
            let keys: Vec<K> = errors.keys().cloned().collect();
            self.future.complete_lookup_exceptionally(errors);
            self.clear(keys);
        }
    }

    fn retry_lookup(&mut self, keys: impl IntoIterator<Item = K>) {
        for key in keys {
            self.unmap(&key);
        }
    }

    /// Completes the given keys and removes them from both stages.
    ///
    /// Mirrors `complete`.
    fn complete(&mut self, values: HashMap<K, V>) {
        if !values.is_empty() {
            let keys: Vec<K> = values.keys().cloned().collect();
            self.future.complete(values);
            self.clear(keys);
        }
    }

    fn complete_lookup(&mut self, broker_id_mapping: HashMap<K, i32>) {
        if !broker_id_mapping.is_empty() {
            self.future.complete_lookup(broker_id_mapping.clone());
            for (key, broker_id) in broker_id_mapping {
                self.map(key, broker_id);
            }
        }
    }

    /// Returns the requests that need to be sent. Call after construction and
    /// after each [`on_response`](Self::on_response) / [`on_failure`](Self::on_failure).
    ///
    /// Mirrors `poll`.
    pub(crate) fn poll(&mut self) -> Vec<RequestSpec<K>> {
        let mut requests = Vec::new();
        self.collect_lookup_requests(&mut requests);
        self.collect_fulfillment_requests(&mut requests);
        requests
    }

    fn collect_lookup_requests(&mut self, requests: &mut Vec<RequestSpec<K>>) {
        for (scope, keys) in self.lookup_map.entries_snapshot() {
            if keys.is_empty() {
                continue;
            }
            if self.request_states.get(&scope).is_some_and(|s| s.has_inflight) {
                continue;
            }
            let request = self.handler.lookup_strategy().build_request(&keys);
            let name = format!("{}(api={})", self.handler.api_name(), request.api_key().name());
            let state = self.request_states.entry(scope.clone()).or_insert_with(RequestState::new);
            let spec = RequestSpec {
                name,
                scope,
                keys,
                request,
                next_allowed_try_ms: state.next_allowed_retry_ms,
                deadline_ms: self.deadline_ms,
                tries: state.tries,
            };
            state.set_inflight();
            requests.push(spec);
        }
    }

    fn collect_fulfillment_requests(&mut self, requests: &mut Vec<RequestSpec<K>>) {
        for (scope, keys) in self.fulfillment_map.entries_snapshot() {
            if keys.is_empty() {
                continue;
            }
            if self.request_states.get(&scope).is_some_and(|s| s.has_inflight) {
                continue;
            }
            let broker_id = match scope.destination_broker_id() {
                Some(id) => id,
                None => continue,
            };
            let mut new_requests = self.handler.build_request(broker_id, &keys);
            if new_requests.is_empty() {
                // Java `return`s here (stops collecting) — preserve that.
                return;
            }
            // Only the first request is issued per broker per cycle.
            let new_request = new_requests.remove(0);
            let request = new_request.request;
            let name = format!("{}(api={})", self.handler.api_name(), request.api_key().name());
            let state = self.request_states.entry(scope.clone()).or_insert_with(RequestState::new);
            let spec = RequestSpec {
                name,
                scope,
                keys: new_request.keys,
                request,
                next_allowed_try_ms: state.next_allowed_retry_ms,
                deadline_ms: self.deadline_ms,
                tries: state.tries,
            };
            state.set_inflight();
            requests.push(spec);
        }
    }

    /// Callback invoked when a `Call` returns a response successfully.
    ///
    /// Mirrors `onResponse`.
    pub(crate) fn on_response(
        &mut self,
        now: i64,
        scope: &ApiRequestScope,
        keys: &HashSet<K>,
        response: &ConcreteResponse,
        node: &Node,
    ) {
        self.clear_inflight_request(now, scope);

        if matches!(scope, ApiRequestScope::Fulfillment(_)) {
            let result = self.handler.handle_response(node, keys, response);
            self.complete(result.completed_keys);
            self.complete_exceptionally(result.failed_keys);
            self.retry_lookup(result.unmapped_keys);
        } else {
            let result = self.handler.lookup_strategy().handle_response(keys, response);
            for key in &result.completed_keys {
                self.lookup_map.remove(key);
            }
            self.complete_lookup(result.mapped_keys);
            self.complete_lookup_exceptionally(result.failed_keys);
        }
    }

    /// Callback invoked when a `Call` fails.
    ///
    /// Mirrors `onFailure`. `is_disconnect` reports whether the failure was a
    /// node disconnect (Java's `DisconnectException`), signalled by the admin
    /// runnable as [`Errors::NetworkError`].
    pub(crate) fn on_failure(&mut self, now: i64, scope: &ApiRequestScope, keys: &HashSet<K>, error: &Error) {
        self.clear_inflight_request(now, scope);

        let is_fulfillment = matches!(scope, ApiRequestScope::Fulfillment(_));

        if error.error() == Errors::NetworkError {
            kafka_debug!(
                self.log_context,
                "Node disconnected before response could be received for request. Will attempt retry"
            );
            // After a disconnect, retry lookup so we can find a new leader.
            let lookup_keys = self.future.lookup_keys();
            let to_unmap: Vec<K> = keys.iter().filter(|k| lookup_keys.contains(*k)).cloned().collect();
            self.retry_lookup(to_unmap);
        } else if is_no_batched_support(error) {
            // Mirrors Java's `NoBatchedFindCoordinatorsException` /
            // `NoBatchedOffsetFetchRequestException` branch: the broker cannot
            // handle a batched request, so disable batching end-to-end and retry
            // the lookup for the affected keys. This branch must precede the
            // generic `UnsupportedVersion` branch because those Java exceptions
            // are `UnsupportedVersionException` subclasses.
            kafka_debug!(
                self.log_context,
                "Batched request is unsupported by the broker. Disabling batching and retrying the lookup."
            );
            self.handler.lookup_strategy().disable_batch();
            let lookup_keys = self.future.lookup_keys();
            let to_unmap: Vec<K> = keys.iter().filter(|k| lookup_keys.contains(*k)).cloned().collect();
            self.retry_lookup(to_unmap);
        } else if error.error() == Errors::UnsupportedVersion {
            if is_fulfillment {
                let broker_id = scope.destination_broker_id().unwrap_or(-1);
                let unrecoverable = self.handler.handle_unsupported_version_exception(broker_id, error, keys);
                self.complete_exceptionally(unrecoverable);
            } else {
                let unrecoverable = self.handler.lookup_strategy().handle_unsupported_version_exception(error, keys);
                let to_unmap: Vec<K> = keys.iter().filter(|k| !unrecoverable.contains_key(*k)).cloned().collect();
                self.complete_lookup_exceptionally(unrecoverable);
                self.retry_lookup(to_unmap);
            }
        } else {
            let errors: HashMap<K, Error> = keys.iter().map(|k| (k.clone(), error.clone())).collect();
            if is_fulfillment {
                self.complete_exceptionally(errors);
            } else {
                self.complete_lookup_exceptionally(errors);
            }
        }
    }

    fn clear_inflight_request(&mut self, now: i64, scope: &ApiRequestScope) {
        let tries = match self.request_states.get(scope) {
            Some(state) => state.tries,
            None => return,
        };
        // Only apply backoff for fulfillment retries, not lookup retries.
        let next_allowed = if matches!(scope, ApiRequestScope::Fulfillment(_)) {
            now + self.retry_backoff.backoff((tries - 1).max(0) as i64)
        } else {
            now
        };
        if let Some(state) = self.request_states.get_mut(scope) {
            state.clear_inflight(next_allowed);
        }
    }

    /// Rebuilds the request for a spec's scope + keys (used only on the rare
    /// non-disconnect retriable re-send of the same `Call`, mirroring Java
    /// re-using `spec.request`).
    pub(crate) fn build_request_for_spec(
        &self,
        scope: &ApiRequestScope,
        keys: &HashSet<K>,
    ) -> Option<Box<dyn RequestBuilder>> {
        match scope.destination_broker_id() {
            Some(broker_id) => {
                let mut requests = self.handler.build_request(broker_id, keys);
                if requests.is_empty() {
                    None
                } else {
                    Some(requests.remove(0).request)
                }
            },
            None => Some(self.handler.lookup_strategy().build_request(keys)),
        }
    }
}

/// Whether `error` is the "broker does not support batching" flavor of
/// `UnsupportedVersion` (Java's `NoBatchedFindCoordinatorsException` /
/// `NoBatchedOffsetFetchRequestException`, both `UnsupportedVersionException`
/// subclasses thrown by the request builders at build time).
///
/// Rust flattens a build-time `UnsupportedVersion` failure into a
/// `Error::KafkaError(UnsupportedVersion)` carrying the builder's message
/// (see `NetworkClient`'s version-mismatch path), losing Java's exception type.
/// The two request builders emit distinctive messages, so we recover the
/// distinction by matching them. These substrings mirror
/// `FindCoordinatorRequest.Builder.build` and
/// `OffsetFetchRequest.Builder.throwIfBatchingIsUnsupported`.
fn is_no_batched_support(error: &Error) -> bool {
    if error.error() != Errors::UnsupportedVersion {
        return false;
    }
    let message = error.message();
    // OffsetFetch: "Broker does not support batching groups for fetch offset request on version N".
    message.contains("does not support batching groups")
        // FindCoordinator: "Cannot create a vN FindCoordinator request because we require
        // features supported only in M or later."
        || message.contains("FindCoordinator request because we require features")
}

#[cfg(test)]
impl<K, V> AdminApiDriver<K, V>
where
    K: Eq + Hash,
{
    /// Test-only accessor mirroring Java's `AdminApiDriver.keyToBrokerId`: the
    /// broker id a key is currently mapped to for fulfillment, or `None` if the
    /// key is (still / again) in the lookup stage.
    pub(crate) fn key_to_broker_id(&self, key: &K) -> Option<i32> {
        self.fulfillment_map
            .reverse_map
            .get(key)
            .and_then(|scope| scope.destination_broker_id())
    }
}

/// Shared test scaffolding: a fake [`AdminApiHandler`] / [`AdminApiFuture`] /
/// [`AdminApiLookupStrategy`] driving the pure driver logic, mirroring the
/// `MockAdminApiHandler` / `MockLookupStrategy` / `TestContext` machinery of the
/// Java `AdminApiDriverTest`. Exposed `pub(crate)` (test-only) so the
/// `KafkaAdminClient` `maybe_retry`-hook tests can build a real driver `Call`
/// around the same fakes.
///
/// Deviation from Java: Java's `MockRequestScope` carries an arbitrary lookup
/// context id, so two dynamic keys with different contexts (`c1`, `c2`) produce
/// two separate lookup requests. The Rust `ApiRequestScope` is a closed enum
/// (`SingleLookup` / `Fulfillment`) matching the only in-scope lookup strategy
/// (`PartitionLeaderStrategy`), which batches every dynamic key into one
/// `SingleLookup` request. The driver's stage-transition logic — the subject of
/// these tests — is identical either way; only the initial lookup fan-out
/// coalesces. This is called out per-test where it changes a request count.
#[cfg(test)]
#[allow(dead_code)]
pub(crate) mod test_support {
    use std::collections::{BTreeSet, HashMap, HashSet};
    use std::sync::{Arc, Mutex};

    use crate::admin::internals::admin_api_future::AdminApiFuture;
    use crate::admin::internals::admin_api_handler::{AdminApiHandler, ApiResult, RequestAndKeys};
    use crate::admin::internals::admin_api_lookup_strategy::{AdminApiLookupStrategy, LookupResult};
    use crate::admin::internals::api_request_scope::ApiRequestScope;
    use crate::common::protocol::ApiKeys;
    use crate::common::requests::{ConcreteResponse, MetadataRequestBuilder, MetadataResponse, RequestBuilder};
    use crate::common::utils::{ExponentialBackoff, LogContext};
    use crate::common::{Error, Node};
    use crate::metadata_response_data::MetadataResponseData;

    use super::{AdminApiDriver, RequestSpec};

    pub(crate) const API_TIMEOUT_MS: i64 = 30_000;
    pub(crate) const RETRY_BACKOFF_MS: i64 = 100;
    pub(crate) const RETRY_BACKOFF_MAX_MS: i64 = 1_000;
    pub(crate) const RETRY_BACKOFF_EXP_BASE: i32 = 2;
    pub(crate) const RETRY_BACKOFF_JITTER: f64 = 0.2;
    /// A fixed "now" for the (non-advancing) clock, mirroring Java's `MockTime`.
    pub(crate) const NOW: i64 = 1_000;

    /// A placeholder response: the fakes key their programmed results off the
    /// request's key set, so the response body is never inspected (mirrors the
    /// Java tests, which always pass an empty `MetadataResponse`).
    pub(crate) fn placeholder_response() -> ConcreteResponse {
        ConcreteResponse::Metadata(MetadataResponse::new(
            MetadataResponseData::new(),
            ApiKeys::METADATA.latest_version(),
        ))
    }

    fn key_set(keys: &[&str]) -> BTreeSet<String> {
        keys.iter().map(|k| (*k).to_string()).collect()
    }

    /// Cloneable expectation for a lookup response (`LookupResult` is not `Clone`).
    #[derive(Clone, Default)]
    pub(crate) struct ExpectedLookup {
        pub(crate) mapped_keys: HashMap<String, i32>,
        pub(crate) failed_keys: HashMap<String, Error>,
    }

    /// Cloneable expectation for a fulfillment response (`ApiResult` is not `Clone`).
    #[derive(Clone, Default)]
    pub(crate) struct ExpectedApiResult {
        pub(crate) completed_keys: HashMap<String, i64>,
        pub(crate) failed_keys: HashMap<String, Error>,
        pub(crate) unmapped_keys: Vec<String>,
    }

    /// Mirrors Java `mapped(...)`.
    pub(crate) fn mapped(pairs: &[(&str, i32)]) -> ExpectedLookup {
        ExpectedLookup {
            mapped_keys: pairs.iter().map(|(k, b)| ((*k).to_string(), *b)).collect(),
            failed_keys: HashMap::new(),
        }
    }

    /// Mirrors Java `failedLookup(...)`.
    pub(crate) fn failed_lookup(key: &str, error: Error) -> ExpectedLookup {
        ExpectedLookup {
            mapped_keys: HashMap::new(),
            failed_keys: HashMap::from([(key.to_string(), error)]),
        }
    }

    /// Mirrors Java `emptyLookup()`.
    pub(crate) fn empty_lookup() -> ExpectedLookup {
        ExpectedLookup::default()
    }

    /// Mirrors Java `completed(...)`.
    pub(crate) fn completed(pairs: &[(&str, i64)]) -> ExpectedApiResult {
        ExpectedApiResult {
            completed_keys: pairs.iter().map(|(k, v)| ((*k).to_string(), *v)).collect(),
            failed_keys: HashMap::new(),
            unmapped_keys: Vec::new(),
        }
    }

    /// Mirrors Java `failed(...)`.
    pub(crate) fn failed(key: &str, error: Error) -> ExpectedApiResult {
        ExpectedApiResult {
            completed_keys: HashMap::new(),
            failed_keys: HashMap::from([(key.to_string(), error)]),
            unmapped_keys: Vec::new(),
        }
    }

    /// Mirrors Java `unmapped(...)`. Represents a fulfillment response carrying a
    /// stale-leader partition error (e.g. `NOT_LEADER_OR_FOLLOWER`), which the
    /// real handler classifies into `unmapped_keys` (see the
    /// `delete_records_handler` classification test).
    pub(crate) fn unmapped(keys: &[&str]) -> ExpectedApiResult {
        ExpectedApiResult {
            completed_keys: HashMap::new(),
            failed_keys: HashMap::new(),
            unmapped_keys: keys.iter().map(|k| (*k).to_string()).collect(),
        }
    }

    /// Mirrors Java `emptyFulfillment()`.
    pub(crate) fn empty_fulfillment() -> ExpectedApiResult {
        ExpectedApiResult::default()
    }

    type LookupTable = Arc<Mutex<HashMap<BTreeSet<String>, ExpectedLookup>>>;
    type RequestTable = Arc<Mutex<HashMap<BTreeSet<String>, ExpectedApiResult>>>;
    type StateTable = Arc<Mutex<HashMap<String, Option<Result<i64, Error>>>>>;

    /// Fake lookup strategy: returns each key's fixed scope and replays the
    /// programmed lookup results. Mirrors Java `MockLookupStrategy`.
    struct FakeLookupStrategy {
        scopes: HashMap<String, ApiRequestScope>,
        expected: LookupTable,
    }

    impl AdminApiLookupStrategy<String> for FakeLookupStrategy {
        fn lookup_scope(&self, key: &String) -> ApiRequestScope {
            self.scopes
                .get(key)
                .cloned()
                .unwrap_or_else(|| panic!("no scope for key {key}"))
        }

        fn build_request(&self, keys: &HashSet<String>) -> Box<dyn RequestBuilder> {
            let set: BTreeSet<String> = keys.iter().cloned().collect();
            assert!(
                self.expected.lock().unwrap().contains_key(&set),
                "Unexpected lookup request for keys {set:?}"
            );
            Box::new(MetadataRequestBuilder::new(None, false))
        }

        fn handle_response(&self, keys: &HashSet<String>, _response: &ConcreteResponse) -> LookupResult<String> {
            let set: BTreeSet<String> = keys.iter().cloned().collect();
            let expected = self
                .expected
                .lock()
                .unwrap()
                .get(&set)
                .cloned()
                .unwrap_or_else(|| panic!("Unexpected lookup request for keys {set:?}"));
            LookupResult {
                completed_keys: Vec::new(),
                mapped_keys: expected.mapped_keys,
                failed_keys: expected.failed_keys,
            }
        }
    }

    /// Fake handler: replays the programmed fulfillment results. Mirrors Java
    /// `MockAdminApiHandler` (`Batched`: one request per broker).
    struct FakeHandler {
        lookup_strategy: FakeLookupStrategy,
        expected: RequestTable,
    }

    impl AdminApiHandler<String, i64> for FakeHandler {
        fn api_name(&self) -> &str {
            "mock-api"
        }

        fn build_request(&self, _broker_id: i32, keys: &HashSet<String>) -> Vec<RequestAndKeys<String>> {
            let set: BTreeSet<String> = keys.iter().cloned().collect();
            assert!(
                self.expected.lock().unwrap().contains_key(&set),
                "Unexpected fulfillment request for keys {set:?}"
            );
            vec![RequestAndKeys { request: Box::new(MetadataRequestBuilder::new(None, false)), keys: keys.clone() }]
        }

        fn handle_response(
            &self,
            _broker: &Node,
            keys: &HashSet<String>,
            _response: &ConcreteResponse,
        ) -> ApiResult<String, i64> {
            let set: BTreeSet<String> = keys.iter().cloned().collect();
            let expected = self
                .expected
                .lock()
                .unwrap()
                .get(&set)
                .cloned()
                .unwrap_or_else(|| panic!("Unexpected fulfillment request for keys {set:?}"));
            ApiResult {
                completed_keys: expected.completed_keys,
                failed_keys: expected.failed_keys,
                unmapped_keys: expected.unmapped_keys,
            }
        }

        fn lookup_strategy(&self) -> &dyn AdminApiLookupStrategy<String> {
            &self.lookup_strategy
        }
    }

    /// Fake future: records per-key completion so tests can assert terminal state.
    /// Uses the default `cached_key_broker_id_mapping` (all keys `UNKNOWN`), so
    /// static keys reach fulfillment via their scope's `destination_broker_id`
    /// during the driver's constructor `unmap`, exactly as Java's
    /// `SimpleAdminApiFuture` does.
    struct FakeFuture {
        lookup_keys: HashSet<String>,
        states: StateTable,
    }

    impl AdminApiFuture<String, i64> for FakeFuture {
        fn lookup_keys(&self) -> HashSet<String> {
            self.lookup_keys.clone()
        }

        fn complete(&self, values: HashMap<String, i64>) {
            let mut states = self.states.lock().unwrap();
            for (key, value) in values {
                states.insert(key, Some(Ok(value)));
            }
        }

        fn complete_exceptionally(&self, errors: HashMap<String, Error>) {
            let mut states = self.states.lock().unwrap();
            for (key, error) in errors {
                states.insert(key, Some(Err(error)));
            }
        }
    }

    /// Owns the driver plus shared handles to the fakes' programmable state.
    /// Mirrors Java's `AdminApiDriverTest.TestContext`.
    pub(crate) struct TestContext {
        pub(crate) now: i64,
        pub(crate) driver: AdminApiDriver<String, i64>,
        states: StateTable,
        lookups: LookupTable,
        requests: RequestTable,
    }

    impl TestContext {
        /// Mirrors Java `new TestContext(staticKeys, dynamicKeys)`.
        pub(crate) fn with(static_keys: &[(&str, i32)], dynamic_keys: &[&str]) -> Self {
            let mut scopes = HashMap::new();
            let mut all_keys = HashSet::new();
            for (key, broker) in static_keys {
                scopes.insert((*key).to_string(), ApiRequestScope::Fulfillment(*broker));
                all_keys.insert((*key).to_string());
            }
            for key in dynamic_keys {
                scopes.insert((*key).to_string(), ApiRequestScope::SingleLookup);
                all_keys.insert((*key).to_string());
            }

            let lookups: LookupTable = Arc::new(Mutex::new(HashMap::new()));
            let requests: RequestTable = Arc::new(Mutex::new(HashMap::new()));
            let states: StateTable = Arc::new(Mutex::new(all_keys.iter().map(|k| (k.clone(), None)).collect()));

            let strategy = FakeLookupStrategy { scopes, expected: Arc::clone(&lookups) };
            let handler = FakeHandler { lookup_strategy: strategy, expected: Arc::clone(&requests) };
            let future = FakeFuture { lookup_keys: all_keys, states: Arc::clone(&states) };

            let retry_backoff = ExponentialBackoff::new(
                RETRY_BACKOFF_MS,
                RETRY_BACKOFF_EXP_BASE,
                RETRY_BACKOFF_MAX_MS,
                RETRY_BACKOFF_JITTER,
            )
            .unwrap();
            let driver = AdminApiDriver::new(
                Box::new(handler),
                Box::new(future),
                NOW + API_TIMEOUT_MS,
                retry_backoff,
                LogContext::new("[test] "),
            );

            let ctx = Self { now: NOW, driver, states, lookups, requests };
            // Verify the seeded stage placement (mirrors Java's constructor asserts).
            for (key, broker) in static_keys {
                ctx.assert_mapped_key(key, *broker);
            }
            for key in dynamic_keys {
                ctx.assert_unmapped_key(key);
            }
            ctx
        }

        /// Mirrors Java `TestContext.staticMapped(...)`.
        pub(crate) fn static_mapped(static_keys: &[(&str, i32)]) -> Self {
            Self::with(static_keys, &[])
        }

        /// Mirrors Java `TestContext.dynamicMapped(...)`.
        pub(crate) fn dynamic_mapped(dynamic_keys: &[&str]) -> Self {
            Self::with(&[], dynamic_keys)
        }

        pub(crate) fn expect_lookup(&self, keys: &[&str], result: ExpectedLookup) {
            self.lookups.lock().unwrap().insert(key_set(keys), result);
        }

        pub(crate) fn expect_request(&self, keys: &[&str], result: ExpectedApiResult) {
            self.requests.lock().unwrap().insert(key_set(keys), result);
        }

        fn key_state(&self, key: &str) -> Option<Result<i64, Error>> {
            self.states.lock().unwrap().get(key).cloned().flatten()
        }

        pub(crate) fn assert_mapped_key(&self, key: &str, expected_broker: i32) {
            assert_eq!(
                self.driver.key_to_broker_id(&key.to_string()),
                Some(expected_broker),
                "expected {key} mapped to broker {expected_broker}"
            );
        }

        pub(crate) fn assert_unmapped_key(&self, key: &str) {
            assert_eq!(self.driver.key_to_broker_id(&key.to_string()), None, "expected {key} unmapped");
            assert!(self.key_state(key).is_none(), "expected {key} future not done while unmapped");
        }

        fn assert_completed_key(&self, key: &str, expected: i64) {
            match self.key_state(key) {
                Some(Ok(value)) => assert_eq!(value, expected, "unexpected completion value for {key}"),
                other => panic!("expected {key} completed with {expected}, got {other:?}"),
            }
        }

        fn assert_failed_key(&self, key: &str, expected: &Error) {
            match self.key_state(key) {
                Some(Err(error)) => assert_eq!(error.error(), expected.error(), "unexpected failure error for {key}"),
                other => panic!("expected {key} failed with {:?}, got {other:?}", expected.error()),
            }
        }

        fn assert_lookup_response(&mut self, spec: &RequestSpec<String>, expected: &ExpectedLookup) {
            for key in &spec.keys {
                self.assert_unmapped_key(key);
            }
            let response = placeholder_response();
            self.driver
                .on_response(self.now, &spec.scope, &spec.keys, &response, Node::no_node());
            for (key, broker) in &expected.mapped_keys {
                self.assert_mapped_key(key, *broker);
            }
            for (key, error) in &expected.failed_keys {
                self.assert_failed_key(key, error);
            }
        }

        fn assert_response(&mut self, spec: &RequestSpec<String>, expected: &ExpectedApiResult) {
            let broker_id = spec
                .scope
                .destination_broker_id()
                .expect("fulfillment requests must specify a target broker");
            for key in &spec.keys {
                self.assert_mapped_key(key, broker_id);
            }
            let response = placeholder_response();
            let node = Node::new(broker_id, "host".to_string(), 1234);
            self.driver.on_response(self.now, &spec.scope, &spec.keys, &response, &node);
            for key in &expected.unmapped_keys {
                self.assert_unmapped_key(key);
            }
            for (key, error) in &expected.failed_keys {
                self.assert_failed_key(key, error);
            }
            for (key, value) in &expected.completed_keys {
                self.assert_completed_key(key, *value);
            }
        }

        /// Mirrors Java `TestContext.poll(expectedLookups, expectedRequests)`:
        /// program the fakes, poll the driver, and drive each produced request
        /// through the matching lookup/fulfillment assertion.
        pub(crate) fn poll(
            &mut self,
            expected_lookups: &[(&[&str], ExpectedLookup)],
            expected_requests: &[(&[&str], ExpectedApiResult)],
        ) {
            if !expected_lookups.is_empty() {
                self.lookups.lock().unwrap().clear();
                for (keys, result) in expected_lookups {
                    self.expect_lookup(keys, result.clone());
                }
            }
            self.requests.lock().unwrap().clear();
            for (keys, result) in expected_requests {
                self.expect_request(keys, result.clone());
            }

            let specs = self.driver.poll();
            assert_eq!(
                specs.len(),
                expected_lookups.len() + expected_requests.len(),
                "driver generated an unexpected number of requests"
            );

            for spec in specs {
                let key_set: BTreeSet<String> = spec.keys.iter().cloned().collect();
                if let Some((_, result)) = expected_lookups.iter().find(|(keys, _)| key_set == key_set_from(keys)) {
                    let result = result.clone();
                    self.assert_lookup_response(&spec, &result);
                } else if let Some((_, result)) =
                    expected_requests.iter().find(|(keys, _)| key_set == key_set_from(keys))
                {
                    let result = result.clone();
                    self.assert_response(&spec, &result);
                } else {
                    panic!("Unexpected request for keys {key_set:?}");
                }
            }
        }
    }

    fn key_set_from(keys: &[&str]) -> BTreeSet<String> {
        keys.iter().map(|k| (*k).to_string()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use crate::common::protocol::Errors;
    use crate::common::{Error, Node};

    fn network_exception() -> Error {
        // Java's `DisconnectException`; the admin runnable signals disconnects as
        // `NetworkException`, which the driver treats as the retry-lookup trigger.
        Error::new(Errors::NetworkError)
    }

    // Mirrors `AdminApiDriverTest.testCoalescedLookup`.
    #[test]
    fn coalesced_lookup() {
        let mut ctx = TestContext::dynamic_mapped(&["foo", "bar"]);
        // Deviation: closed-enum SingleLookup coalesces both keys into one lookup
        // (Java issues one because both share context "c1").
        ctx.poll(&[(&["foo", "bar"], mapped(&[("foo", 1), ("bar", 2)]))], &[]);
        ctx.poll(
            &[],
            &[
                (&["foo"], completed(&[("foo", 15)])),
                (&["bar"], completed(&[("bar", 30)])),
            ],
        );
        ctx.poll(&[], &[]);
    }

    // Mirrors `AdminApiDriverTest.testCoalescedFulfillment`: two keys mapped to
    // the same broker collapse into a single fulfillment request.
    #[test]
    fn coalesced_fulfillment() {
        let mut ctx = TestContext::dynamic_mapped(&["foo", "bar"]);
        ctx.poll(&[(&["foo", "bar"], mapped(&[("foo", 1), ("bar", 1)]))], &[]);
        ctx.poll(&[], &[(&["foo", "bar"], completed(&[("foo", 15), ("bar", 30)]))]);
        ctx.poll(&[], &[]);
    }

    // Mirrors `AdminApiDriverTest.testStaticMapping`: statically mapped keys skip
    // lookup and go straight to fulfillment.
    #[test]
    fn static_mapping() {
        let mut ctx = TestContext::static_mapped(&[("foo", 0), ("bar", 1), ("baz", 1)]);
        ctx.poll(
            &[],
            &[
                (&["foo"], completed(&[("foo", 15)])),
                (&["bar", "baz"], completed(&[("bar", 30), ("baz", 45)])),
            ],
        );
        ctx.poll(&[], &[]);
    }

    // Branch (1): fulfillment -> unmap -> re-lookup -> re-fulfillment.
    // Mirrors `AdminApiDriverTest.testFulfillmentUnmapping`.
    #[test]
    fn fulfillment_unmapping() {
        let mut ctx = TestContext::dynamic_mapped(&["foo", "bar"]);
        // Initial (coalesced) lookup maps foo->0, bar->1.
        ctx.poll(&[(&["foo", "bar"], mapped(&[("foo", 0), ("bar", 1)]))], &[]);
        // Fulfillment: foo completes; bar's leader is stale (NOT_LEADER_OR_FOLLOWER
        // -> unmapped), sending bar back to the lookup stage.
        ctx.poll(&[], &[(&["foo"], completed(&[("foo", 15)])), (&["bar"], unmapped(&["bar"]))]);
        // bar is re-looked-up (to a possibly new leader) ...
        ctx.poll(&[(&["bar"], mapped(&[("bar", 1)]))], &[]);
        // ... then re-fulfilled and completed (poll asserts the completion value).
        ctx.poll(&[], &[(&["bar"], completed(&[("bar", 30)]))]);
        ctx.poll(&[], &[]);
    }

    // Mirrors `AdminApiDriverTest.testRecoalescedLookup`: both keys unmap from
    // fulfillment and re-coalesce into one lookup.
    #[test]
    fn recoalesced_lookup() {
        let mut ctx = TestContext::dynamic_mapped(&["foo", "bar"]);
        ctx.poll(&[(&["foo", "bar"], mapped(&[("foo", 1), ("bar", 2)]))], &[]);
        ctx.poll(&[], &[(&["foo"], unmapped(&["foo"])), (&["bar"], unmapped(&["bar"]))]);
        ctx.poll(&[(&["foo", "bar"], mapped(&[("foo", 3), ("bar", 3)]))], &[]);
        ctx.poll(&[], &[(&["foo", "bar"], completed(&[("foo", 15), ("bar", 30)]))]);
        ctx.poll(&[], &[]);
    }

    // Branch (2): a disconnect on a fulfillment request re-drives the lookup
    // rather than failing the key. Mirrors `AdminApiDriverTest.testRetryLookupAfterDisconnect`.
    #[test]
    fn retry_lookup_after_disconnect() {
        let mut ctx = TestContext::dynamic_mapped(&["foo"]);
        let initial_leader = 1;

        ctx.poll(&[(&["foo"], mapped(&[("foo", initial_leader)]))], &[]);
        ctx.assert_mapped_key("foo", initial_leader);

        // Obtain the fulfillment request against the initial leader.
        ctx.expect_request(&["foo"], completed(&[("foo", 15)]));
        let specs = ctx.driver.poll();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].scope.destination_broker_id(), Some(initial_leader));

        // Disconnect -> the key is unmapped and returns to the lookup stage.
        ctx.driver
            .on_failure(ctx.now, &specs[0].scope, &specs[0].keys, &network_exception());
        ctx.assert_unmapped_key("foo");

        // The retry lookup is issued immediately (no backoff for lookups) and the
        // lookup scope carries over its try count.
        let retry_leader = 2;
        ctx.expect_lookup(&["foo"], mapped(&[("foo", retry_leader)]));
        let retry_specs = ctx.driver.poll();
        assert_eq!(retry_specs.len(), 1);
        assert_eq!(retry_specs[0].next_allowed_try_ms, ctx.now);
        assert_eq!(retry_specs[0].tries, 1);

        // The re-lookup + re-fulfillment complete the key against the new leader.
        ctx.driver.on_response(
            ctx.now,
            &retry_specs[0].scope,
            &retry_specs[0].keys,
            &placeholder_response(),
            Node::no_node(),
        );
        ctx.assert_mapped_key("foo", retry_leader);
        // The fulfillment against the new leader completes the key (poll asserts
        // the terminal completion value).
        ctx.poll(&[], &[(&["foo"], completed(&[("foo", 30)]))]);
    }

    // Mirrors `AdminApiDriverTest.testLookupRetryBookkeeping`: an empty lookup
    // result retries the lookup with tries incremented and no backoff.
    #[test]
    fn lookup_retry_bookkeeping() {
        let mut ctx = TestContext::dynamic_mapped(&["foo"]);
        ctx.expect_lookup(&["foo"], empty_lookup());

        let specs = ctx.driver.poll();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].tries, 0);
        assert_eq!(specs[0].next_allowed_try_ms, 0);
        ctx.driver.on_response(
            ctx.now,
            &specs[0].scope,
            &specs[0].keys,
            &placeholder_response(),
            Node::no_node(),
        );

        let retry = ctx.driver.poll();
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].tries, 1);
        assert_eq!(retry[0].next_allowed_try_ms, ctx.now);
    }

    // Mirrors `AdminApiDriverTest.testFulfillmentRetryBookkeeping`: an empty
    // fulfillment result retries with tries incremented and one backoff step.
    #[test]
    fn fulfillment_retry_bookkeeping() {
        let mut ctx = TestContext::static_mapped(&[("foo", 0)]);
        ctx.expect_request(&["foo"], empty_fulfillment());

        let specs = ctx.driver.poll();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].tries, 0);
        assert_eq!(specs[0].next_allowed_try_ms, 0);
        ctx.driver.on_response(
            ctx.now,
            &specs[0].scope,
            &specs[0].keys,
            &placeholder_response(),
            Node::no_node(),
        );

        let retry = ctx.driver.poll();
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].tries, 1);
        // Fulfillment retries apply one jittered backoff step: now + backoff(0).
        let lo = ctx.now + (RETRY_BACKOFF_MS as f64 * (1.0 - RETRY_BACKOFF_JITTER)) as i64;
        let hi = ctx.now + (RETRY_BACKOFF_MS as f64 * (1.0 + RETRY_BACKOFF_JITTER)) as i64;
        assert!(
            (lo..=hi).contains(&retry[0].next_allowed_try_ms),
            "fulfillment retry next_allowed_try_ms {} not within one backoff step [{lo}, {hi}]",
            retry[0].next_allowed_try_ms
        );
    }
}
