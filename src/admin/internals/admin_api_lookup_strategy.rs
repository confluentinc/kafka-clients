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

//! The lookup strategy that finds the broker id which will handle each key.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.AdminApiLookupStrategy`.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use crate::common::Error;
use crate::common::requests::{ConcreteResponse, RequestBuilder};

use super::api_request_scope::ApiRequestScope;

/// The result of a lookup response: which keys mapped to a broker, which failed
/// fatally, and which completed during lookup.
///
/// Corresponds to `AdminApiLookupStrategy.LookupResult`.
pub(crate) struct LookupResult<K> {
    /// Keys completed by the lookup phase itself (the driver attempts neither
    /// lookup nor fulfillment for them).
    pub(crate) completed_keys: Vec<K>,
    /// Keys mapped to a specific broker for fulfillment.
    pub(crate) mapped_keys: HashMap<K, i32>,
    /// Keys that encountered a fatal error during lookup.
    pub(crate) failed_keys: HashMap<K, Error>,
}

impl<K: Eq + Hash> LookupResult<K> {
    /// Creates a result with only failed and mapped keys (no completed keys).
    pub(crate) fn new(failed_keys: HashMap<K, Error>, mapped_keys: HashMap<K, i32>) -> Self {
        Self { completed_keys: Vec::new(), mapped_keys, failed_keys }
    }
}

impl<K: Clone + Eq + Hash> LookupResult<K> {
    /// Fails every key the lookup request covered with the same error.
    ///
    /// The lookup-stage counterpart of [`ApiResult::failed_all`]: Java's
    /// `AdminApiDriver.onFailure` generic `else` branch routes a
    /// `LookupRequestScope` spec to `completeLookupExceptionally(errors)`
    /// (`AdminApiDriver.java:311`).
    ///
    /// [`ApiResult::failed_all`]: super::admin_api_handler::ApiResult::failed_all
    pub(crate) fn failed_all(keys: &HashSet<K>, error: Error) -> Self {
        let failed_keys = keys.iter().map(|key| (key.clone(), error.clone())).collect();
        Self::new(failed_keys, HashMap::new())
    }
}

/// Finds the broker id which will handle each respective key.
///
/// Corresponds to `AdminApiLookupStrategy<T>`. Kept a plain (non-`async`) trait
/// per `.claude/rules/admin-client.md` §2 — it runs on the background task.
pub(crate) trait AdminApiLookupStrategy<K>: Send {
    /// Defines the scope of a given key for lookup, controlling how lookups are
    /// batched together.
    ///
    /// Mirrors `lookupScope`.
    fn lookup_scope(&self, key: &K) -> ApiRequestScope;

    /// Builds the lookup request for a set of keys.
    ///
    /// Mirrors `buildRequest`.
    fn build_request(&self, keys: &std::collections::HashSet<K>) -> Box<dyn RequestBuilder>;

    /// Handles a successful lookup response, returning which keys mapped to a
    /// broker and which failed fatally.
    ///
    /// Mirrors `handleResponse`.
    fn handle_response(&self, keys: &std::collections::HashSet<K>, response: &ConcreteResponse) -> LookupResult<K>;

    /// Handles an `UnsupportedVersionException` on a lookup request. The default
    /// maps every key to the exception (the request should not be retried).
    ///
    /// Mirrors `handleUnsupportedVersionException`.
    fn handle_unsupported_version_error(&self, error: &Error, keys: &std::collections::HashSet<K>) -> HashMap<K, Error>
    where
        K: Clone + Eq + Hash,
    {
        keys.iter().map(|k| (k.clone(), error.clone())).collect()
    }

    /// Disables batched lookups for this strategy after the broker signals it
    /// does not support batching (Java's `NoBatchedFindCoordinatorsException` /
    /// `NoBatchedOffsetFetchRequestException`).
    ///
    /// The default is a no-op; only [`CoordinatorStrategy`](super::coordinator_strategy::CoordinatorStrategy)
    /// overrides it. Java expresses this by downcasting
    /// `handler.lookupStrategy()` to `CoordinatorStrategy` and calling
    /// `disableBatch()`; Rust models the downcast as a defaulted trait method.
    fn disable_batch(&self) {}
}
