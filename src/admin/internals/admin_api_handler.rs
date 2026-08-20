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

//! The per-API handler that builds fulfillment requests and parses responses.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.AdminApiHandler`.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use crate::common::requests::{ConcreteResponse, RequestBuilder};
use crate::common::{Error, Node};

use super::admin_api_lookup_strategy::AdminApiLookupStrategy;

/// The result of handling a fulfillment response: which keys completed, which
/// failed fatally, and which must be sent back to the lookup stage.
///
/// Corresponds to `AdminApiHandler.ApiResult`.
pub(crate) struct ApiResult<K, V> {
    /// Keys that have been completed with their values.
    pub(crate) completed_keys: HashMap<K, V>,
    /// Keys that failed with an unrecoverable error.
    pub(crate) failed_keys: HashMap<K, Error>,
    /// Keys that must be "unmapped" and retried from the lookup stage.
    pub(crate) unmapped_keys: Vec<K>,
}

impl<K, V> ApiResult<K, V> {
    /// Creates a result from its three components.
    pub(crate) fn new(completed_keys: HashMap<K, V>, failed_keys: HashMap<K, Error>, unmapped_keys: Vec<K>) -> Self {
        Self { completed_keys, failed_keys, unmapped_keys }
    }
}

impl<K: Clone + Eq + Hash, V> ApiResult<K, V> {
    /// Fails every key the request covered with the same error.
    ///
    /// This is what Java does when `handleResponse` throws: `KafkaAdminClient`'s
    /// `catch (Throwable t)` (`KafkaAdminClient.java:1387-1391`) calls
    /// `call.fail(now, t)`, which on the driver path reaches
    /// `AdminApiDriver.onFailure`'s generic `else` branch
    /// (`AdminApiDriver.java:303-312`):
    ///
    /// ```java
    /// Map<K, Throwable> errors = spec.keys.stream().collect(Collectors.toMap(
    ///     Function.identity(), key -> t));
    /// ```
    ///
    /// The dominant reason that catch exists is the `(XResponse) abstractResponse`
    /// downcast at the top of every `handleResponse`: a `ClassCastException` fails
    /// **one** call and leaves the client serving everything else. A `panic!` there
    /// instead kills the admin background task and poisons the driver mutex, and an
    /// empty `ApiResult` completes nothing, fails nothing and unmaps nothing — so
    /// the driver re-issues the identical request until the deadline and the caller
    /// gets a generic timeout with the real cause discarded.
    pub(crate) fn failed_all(keys: &HashSet<K>, error: Error) -> Self {
        let failed_keys = keys.iter().map(|key| (key.clone(), error.clone())).collect();
        Self::new(HashMap::new(), failed_keys, Vec::new())
    }
}

/// A built request together with the keys it covers.
///
/// Corresponds to `AdminApiHandler.RequestAndKeys`.
pub(crate) struct RequestAndKeys<K> {
    /// The request builder for the covered keys.
    pub(crate) request: Box<dyn RequestBuilder>,
    /// The keys covered by this request.
    pub(crate) keys: HashSet<K>,
}

/// Builds the requests for a set of keys and parses the responses.
///
/// Corresponds to `AdminApiHandler<K, V>`. Kept a plain (non-`async`) trait per
/// `.claude/rules/admin-client.md` §2 — it runs on the background task.
///
/// Note: Java splits request building into `Batched` / `Unbatched` sub-traits.
/// The only handler in scope (`DeleteRecordsHandler`) is `Batched` (a single
/// request per broker for all its keys), so [`build_request`](Self::build_request)
/// is implemented directly to return one [`RequestAndKeys`]; the `Batched` /
/// `Unbatched` abstraction is not modelled until a second handler needs it.
pub(crate) trait AdminApiHandler<K, V>: Send {
    /// A user-friendly name for the API this handler implements.
    ///
    /// Mirrors `apiName`.
    fn api_name(&self) -> &str;

    /// Builds the requests necessary for the given keys targeting `broker_id`.
    ///
    /// Mirrors `buildRequest`.
    fn build_request(&self, broker_id: i32, keys: &HashSet<K>) -> Vec<RequestAndKeys<K>>;

    /// Handles a successful fulfillment response.
    ///
    /// Mirrors `handleResponse`.
    fn handle_response(&self, broker: &Node, keys: &HashSet<K>, response: &ConcreteResponse) -> ApiResult<K, V>;

    /// Handles an `UnsupportedVersionException` on a fulfillment request. The
    /// default maps every key to the exception (the request should not be
    /// retried).
    ///
    /// Mirrors `handleUnsupportedVersionException`.
    fn handle_unsupported_version_exception(
        &self,
        _broker_id: i32,
        exception: &Error,
        keys: &HashSet<K>,
    ) -> HashMap<K, Error>
    where
        K: Clone + Eq + Hash,
    {
        keys.iter().map(|k| (k.clone(), exception.clone())).collect()
    }

    /// The lookup strategy responsible for finding the broker id for each key.
    ///
    /// Mirrors `lookupStrategy`.
    fn lookup_strategy(&self) -> &dyn AdminApiLookupStrategy<K>;
}
