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
use crate::common::{KafkaError, Node};
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
    fn complete_exceptionally(&mut self, errors: HashMap<K, KafkaError>) {
        if !errors.is_empty() {
            let keys: Vec<K> = errors.keys().cloned().collect();
            self.future.complete_exceptionally(errors);
            self.clear(keys);
        }
    }

    fn complete_lookup_exceptionally(&mut self, errors: HashMap<K, KafkaError>) {
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
    /// runnable as [`Errors::NetworkException`].
    pub(crate) fn on_failure(&mut self, now: i64, scope: &ApiRequestScope, keys: &HashSet<K>, error: &KafkaError) {
        self.clear_inflight_request(now, scope);

        let is_fulfillment = matches!(scope, ApiRequestScope::Fulfillment(_));

        if error.error() == Errors::NetworkException {
            kafka_debug!(
                self.log_context,
                "Node disconnected before response could be received for request. Will attempt retry"
            );
            // After a disconnect, retry lookup so we can find a new leader.
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
            let errors: HashMap<K, KafkaError> = keys.iter().map(|k| (k.clone(), error.clone())).collect();
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
