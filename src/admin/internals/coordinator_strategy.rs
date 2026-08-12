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

//! The lookup strategy that finds the coordinator broker for a group or
//! transactional id via `FindCoordinator`.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.CoordinatorStrategy`.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::common::Error;
use crate::common::protocol::Errors;
use crate::common::requests::{
    ConcreteResponse, CoordinatorType, FindCoordinatorRequestBuilder, FindCoordinatorResponse, RequestBuilder,
};
use crate::common::utils::LogContext;
use crate::find_coordinator_request_data::FindCoordinatorRequestData;
use crate::{kafka_debug, kafka_error};

use super::admin_api_lookup_strategy::{AdminApiLookupStrategy, LookupResult};
use super::api_request_scope::ApiRequestScope;
use super::coordinator_key::CoordinatorKey;

/// The uppercase Java enum name for a coordinator type (used in error
/// messages to mirror Java's `CoordinatorType.toString`).
fn type_name(coordinator_type: CoordinatorType) -> &'static str {
    match coordinator_type {
        CoordinatorType::Group => "GROUP",
        CoordinatorType::Transaction => "TRANSACTION",
        CoordinatorType::Share => "SHARE",
    }
}

/// Finds the coordinator broker for each [`CoordinatorKey`] via
/// `FindCoordinator`.
///
/// Corresponds to `CoordinatorStrategy`. Generic over coordinator type
/// (`GROUP` now; `TRANSACTION` reused in Tier 3).
pub(crate) struct CoordinatorStrategy {
    log_context: LogContext,
    coordinator_type: CoordinatorType,
    /// Whether the `FindCoordinator` API supports batched lookups. Interior
    /// mutability because the [`AdminApiLookupStrategy`] trait methods take
    /// `&self` but [`disable_batch`](Self::disable_batch) flips this flag.
    batch: AtomicBool,
}

impl CoordinatorStrategy {
    /// Creates a strategy for the given coordinator type.
    pub(crate) fn new(coordinator_type: CoordinatorType, log_context: LogContext) -> Self {
        Self { log_context, coordinator_type, batch: AtomicBool::new(true) }
    }

    /// Whether batched lookups are enabled. Mirrors `CoordinatorStrategy.batch`.
    pub(crate) fn batch(&self) -> bool {
        self.batch.load(Ordering::Acquire)
    }

    /// Builds the `FindCoordinator` lookup request for a set of keys.
    ///
    /// Mirrors `CoordinatorStrategy.buildRequest`. Returns a `Result` (rather
    /// than the infallible trait method) so the precondition violations Java
    /// raises as `IllegalArgumentException` are surfaced and testable.
    ///
    /// Note: Java filters "unrepresentable" (null id) keys into `failedKeys`.
    /// In Rust a [`CoordinatorKey`]'s `id_value` is a non-null `String`, so no
    /// key can be unrepresentable — the filter is unrepresentable in the type
    /// system and always a no-op.
    pub(crate) fn build_lookup_request(
        &self,
        keys: &HashSet<CoordinatorKey>,
    ) -> Result<FindCoordinatorRequestBuilder, Error> {
        if self.batch() {
            self.ensure_same_type(keys)?;
            let mut data = FindCoordinatorRequestData::new();
            data.set_key_type(self.coordinator_type.id());
            data.set_coordinator_keys(keys.iter().map(|k| k.id_value.clone()).collect());
            Ok(FindCoordinatorRequestBuilder::new(data))
        } else {
            let key = self.require_singleton_and_type(keys)?;
            let mut data = FindCoordinatorRequestData::new();
            data.set_key(key.id_value.clone());
            data.set_key_type(key.coordinator_type.id());
            Ok(FindCoordinatorRequestBuilder::new(data))
        }
    }

    /// Handles a `FindCoordinator` response.
    ///
    /// Mirrors `CoordinatorStrategy.handleResponse`. Returns a `Result` so the
    /// `IllegalArgumentException` Java raises for a malformed old-version
    /// response (`requireSingletonAndType`) is surfaced and testable.
    pub(crate) fn handle_lookup_response(
        &self,
        keys: &HashSet<CoordinatorKey>,
        response: &FindCoordinatorResponse,
    ) -> Result<LookupResult<CoordinatorKey>, Error> {
        let mut mapped_keys = std::collections::HashMap::new();
        let mut failed_keys = std::collections::HashMap::new();

        for coordinator in response.coordinators() {
            // Java keys off `coordinator.key() == null` (old version without
            // batching). The Rust `FindCoordinatorResponse` synthesizes an
            // empty-string key for the v<=3 single-coordinator representation,
            // so an empty key signals the old-version path here.
            let key = if coordinator.key.is_empty() {
                self.require_singleton_and_type(keys)?.clone()
            } else if self.coordinator_type == CoordinatorType::Group {
                CoordinatorKey::by_group_id(coordinator.key.clone())
            } else {
                CoordinatorKey::by_transactional_id(coordinator.key.clone())
            };
            self.handle_error(
                Errors::for_code(coordinator.error_code),
                key,
                coordinator.node_id,
                &mut mapped_keys,
                &mut failed_keys,
            );
        }
        Ok(LookupResult::new(failed_keys, mapped_keys))
    }

    fn require_singleton_and_type<'a>(&self, keys: &'a HashSet<CoordinatorKey>) -> Result<&'a CoordinatorKey, Error> {
        if keys.len() != 1 {
            return Err(Error::illegal_argument(format!(
                "Unexpected size of key set: expected 1, but got {}",
                keys.len()
            )));
        }
        let key = keys.iter().next().expect("len checked to be 1");
        if key.coordinator_type != self.coordinator_type {
            return Err(Error::illegal_argument(format!(
                "Unexpected key type: expected key to be of type {}, but got {}",
                type_name(self.coordinator_type),
                type_name(key.coordinator_type)
            )));
        }
        Ok(key)
    }

    fn ensure_same_type(&self, keys: &HashSet<CoordinatorKey>) -> Result<(), Error> {
        if keys.is_empty() {
            return Err(Error::illegal_argument("Unexpected size of key set: expected >= 1, but got 0"));
        }
        if keys.iter().any(|k| k.coordinator_type != self.coordinator_type) {
            return Err(Error::illegal_argument(format!(
                "Unexpected key set: expected all key to be of type {}, but some key were not",
                type_name(self.coordinator_type)
            )));
        }
        Ok(())
    }

    fn handle_error(
        &self,
        error: Errors,
        key: CoordinatorKey,
        node_id: i32,
        mapped_keys: &mut std::collections::HashMap<CoordinatorKey, i32>,
        failed_keys: &mut std::collections::HashMap<CoordinatorKey, Error>,
    ) {
        match error {
            Errors::None => {
                mapped_keys.insert(key, node_id);
            },
            Errors::CoordinatorNotAvailable | Errors::CoordinatorLoadInProgress => {
                kafka_debug!(
                    self.log_context,
                    "FindCoordinator request for key {} returned topic-level error {:?}. Will retry",
                    key,
                    error
                );
            },
            Errors::GroupAuthorizationFailed => {
                let id_value = key.id_value.clone();
                failed_keys.insert(
                    key.clone(),
                    Error::group_authorization_with_message(
                        id_value,
                        format!("FindCoordinator request for groupId `{key}` failed due to authorization failure"),
                    ),
                );
            },
            Errors::TransactionalIdAuthorizationFailed => {
                failed_keys.insert(
                    key.clone(),
                    Error::with_message(
                        Errors::TransactionalIdAuthorizationFailed,
                        format!(
                            "FindCoordinator request for transactionalId `{key}` failed due to authorization failure"
                        ),
                    ),
                );
            },
            other => {
                failed_keys.insert(
                    key.clone(),
                    Error::with_message(
                        other,
                        format!("FindCoordinator request for key `{key}` failed due to an unexpected error"),
                    ),
                );
            },
        }
    }
}

impl AdminApiLookupStrategy<CoordinatorKey> for CoordinatorStrategy {
    /// Disables batched lookups (each key gets its own request). Mirrors
    /// `CoordinatorStrategy.disableBatch`; invoked by the `AdminApiDriver` when
    /// the broker signals a `NoBatched*` condition.
    fn disable_batch(&self) {
        self.batch.store(false, Ordering::Release);
    }

    fn lookup_scope(&self, key: &CoordinatorKey) -> ApiRequestScope {
        if self.batch() {
            ApiRequestScope::SingleLookup
        } else {
            // Without batching, each key needs a separate lookup context.
            ApiRequestScope::CoordinatorLookup(key.clone())
        }
    }

    fn build_request(&self, keys: &HashSet<CoordinatorKey>) -> Box<dyn RequestBuilder> {
        // The precondition violations (`IllegalArgumentException` in Java) are
        // programming errors — CLAUDE.md §10.1 permits panic for unrecoverable
        // programming errors. In the normal admin group-describe flow all keys
        // are representable group keys of the strategy's type, so this never
        // fails; the fallible variant `build_lookup_request` is exercised by
        // the unit tests for the error conditions.
        match self.build_lookup_request(keys) {
            Ok(builder) => Box::new(builder),
            Err(e) => {
                kafka_error!(
                    self.log_context,
                    "CoordinatorStrategy.build_request precondition violated: {}",
                    e
                );
                panic!("CoordinatorStrategy.build_request precondition violated: {e}");
            },
        }
    }

    fn handle_response(
        &self,
        keys: &HashSet<CoordinatorKey>,
        response: &ConcreteResponse,
    ) -> LookupResult<CoordinatorKey> {
        let ConcreteResponse::FindCoordinator(resp) = response else {
            panic!("CoordinatorStrategy received an unexpected response type: {response:?}");
        };
        match self.handle_lookup_response(keys, resp) {
            Ok(result) => result,
            Err(e) => {
                kafka_error!(
                    self.log_context,
                    "CoordinatorStrategy.handle_response precondition violated: {}",
                    e
                );
                panic!("CoordinatorStrategy.handle_response precondition violated: {e}");
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;
    use crate::find_coordinator_response_data::{Coordinator, FindCoordinatorResponseData};

    fn strategy(coordinator_type: CoordinatorType) -> CoordinatorStrategy {
        CoordinatorStrategy::new(coordinator_type, LogContext::new(String::new()))
    }

    fn keys(items: &[CoordinatorKey]) -> HashSet<CoordinatorKey> {
        items.iter().cloned().collect()
    }

    /// Translated from `testBuildOldLookupRequest`.
    #[test]
    fn test_build_old_lookup_request() {
        let s = strategy(CoordinatorType::Group);
        s.disable_batch();
        let builder = s.build_lookup_request(&keys(&[CoordinatorKey::by_group_id("foo")])).unwrap();
        assert_eq!(builder.data().key, "foo");
        assert_eq!(builder.data().key_type, CoordinatorType::Group.id());
    }

    /// Translated from `testBuildLookupRequest`.
    #[test]
    fn test_build_lookup_request() {
        let s = strategy(CoordinatorType::Group);
        let builder = s
            .build_lookup_request(&keys(&[CoordinatorKey::by_group_id("foo"), CoordinatorKey::by_group_id("bar")]))
            .unwrap();
        assert_eq!(builder.data().key, "");
        assert_eq!(builder.data().coordinator_keys.len(), 2);
        assert_eq!(builder.data().key_type, CoordinatorType::Group.id());
    }

    /// Adapted from `testBuildLookupRequestNonRepresentable`. Java passes a set
    /// containing a `null` key, which Rust's type system forbids
    /// (`CoordinatorKey.id_value` is a non-null `String`), so the "unrepresentable
    /// key" filter is a no-op; here we simply confirm a representable key is
    /// carried through.
    #[test]
    fn test_build_lookup_request_representable_only() {
        let s = strategy(CoordinatorType::Group);
        let builder = s.build_lookup_request(&keys(&[CoordinatorKey::by_group_id("foo")])).unwrap();
        assert_eq!(builder.data().key, "");
        assert_eq!(builder.data().coordinator_keys.len(), 1);
    }

    /// Translated from `testBuildOldLookupRequestRequiresOneKey`.
    #[test]
    fn test_build_old_lookup_request_requires_one_key() {
        let s = strategy(CoordinatorType::Group);
        s.disable_batch();
        assert!(s.build_lookup_request(&HashSet::new()).is_err());
        let two = keys(&[CoordinatorKey::by_group_id("foo"), CoordinatorKey::by_group_id("bar")]);
        assert!(s.build_lookup_request(&two).is_err());
    }

    /// Translated from `testBuildOldLookupRequestRequiresAtLeastOneKey`.
    #[test]
    fn test_build_old_lookup_request_requires_matching_type() {
        let s = strategy(CoordinatorType::Group);
        s.disable_batch();
        let wrong = keys(&[CoordinatorKey::by_transactional_id("txnid")]);
        assert!(s.build_lookup_request(&wrong).is_err());
    }

    /// Translated from `testBuildLookupRequestRequiresAtLeastOneKey`.
    #[test]
    fn test_build_lookup_request_requires_at_least_one_key() {
        let s = strategy(CoordinatorType::Group);
        assert!(s.build_lookup_request(&HashSet::new()).is_err());
    }

    /// Translated from `testBuildLookupRequestRequiresKeySameType`.
    #[test]
    fn test_build_lookup_request_requires_key_same_type() {
        let s = strategy(CoordinatorType::Group);
        let mixed = keys(&[
            CoordinatorKey::by_group_id("group"),
            CoordinatorKey::by_transactional_id("txnid"),
        ]);
        assert!(s.build_lookup_request(&mixed).is_err());
    }

    fn old_response(data: FindCoordinatorResponseData) -> FindCoordinatorResponse {
        FindCoordinatorResponse::new(data)
    }

    /// Translated from `testHandleOldResponseRequiresOneKey`.
    #[test]
    fn test_handle_old_response_requires_one_key() {
        let mut data = FindCoordinatorResponseData::new();
        data.set_error_code(Errors::None.code());
        let response = old_response(data);
        let s = strategy(CoordinatorType::Group);
        s.disable_batch();
        assert!(s.handle_lookup_response(&HashSet::new(), &response).is_err());
        let two = keys(&[CoordinatorKey::by_group_id("foo"), CoordinatorKey::by_group_id("bar")]);
        assert!(s.handle_lookup_response(&two, &response).is_err());
    }

    fn run_old_lookup(key: CoordinatorKey, data: FindCoordinatorResponseData) -> LookupResult<CoordinatorKey> {
        let s = strategy(key.coordinator_type);
        s.disable_batch();
        s.handle_lookup_response(&keys(&[key]), &old_response(data)).unwrap()
    }

    fn run_lookup(key_set: HashSet<CoordinatorKey>, data: FindCoordinatorResponseData) -> LookupResult<CoordinatorKey> {
        let coordinator_type = key_set.iter().next().unwrap().coordinator_type;
        let s = strategy(coordinator_type);
        let _ = s.build_lookup_request(&key_set);
        s.handle_lookup_response(&key_set, &FindCoordinatorResponse::new(data)).unwrap()
    }

    /// Translated from `testSuccessfulOldCoordinatorLookup`.
    #[test]
    fn test_successful_old_coordinator_lookup() {
        let group = CoordinatorKey::by_group_id("foo");
        let mut data = FindCoordinatorResponseData::new();
        data.set_error_code(Errors::None.code())
            .set_host("localhost".to_string())
            .set_port(9092)
            .set_node_id(1);
        let result = run_old_lookup(group.clone(), data);
        assert_eq!(result.mapped_keys, HashMap::from([(group, 1)]));
        assert!(result.failed_keys.is_empty());
    }

    fn coordinator(key: &str, error: Errors, node_id: i32) -> Coordinator {
        let mut c = Coordinator::new();
        c.set_key(key.to_string())
            .set_error_code(error.code())
            .set_host("localhost".to_string())
            .set_port(9092)
            .set_node_id(node_id);
        c
    }

    /// Translated from `testSuccessfulCoordinatorLookup`.
    #[test]
    fn test_successful_coordinator_lookup() {
        let group1 = CoordinatorKey::by_group_id("foo");
        let group2 = CoordinatorKey::by_group_id("bar");
        let mut data = FindCoordinatorResponseData::new();
        data.set_coordinators(vec![coordinator("foo", Errors::None, 1), coordinator("bar", Errors::None, 2)]);
        let result = run_lookup(keys(&[group1.clone(), group2.clone()]), data);
        assert_eq!(result.mapped_keys, HashMap::from([(group1, 1), (group2, 2)]));
        assert!(result.failed_keys.is_empty());
    }

    /// Translated from `testRetriableOldCoordinatorLookup`.
    #[test]
    fn test_retriable_old_coordinator_lookup() {
        for error in [Errors::CoordinatorLoadInProgress, Errors::CoordinatorNotAvailable] {
            let mut data = FindCoordinatorResponseData::new();
            data.set_error_code(error.code());
            let result = run_old_lookup(CoordinatorKey::by_group_id("foo"), data);
            assert!(result.failed_keys.is_empty());
            assert!(result.mapped_keys.is_empty());
        }
    }

    /// Translated from `testRetriableCoordinatorLookup`.
    #[test]
    fn test_retriable_coordinator_lookup() {
        for error in [Errors::CoordinatorLoadInProgress, Errors::CoordinatorNotAvailable] {
            let group1 = CoordinatorKey::by_group_id("foo");
            let group2 = CoordinatorKey::by_group_id("bar");
            let mut c1 = Coordinator::new();
            c1.set_key("foo".to_string()).set_error_code(error.code());
            let mut data = FindCoordinatorResponseData::new();
            data.set_coordinators(vec![c1, coordinator("bar", Errors::None, 2)]);
            let result = run_lookup(keys(&[group1, group2.clone()]), data);
            assert!(result.failed_keys.is_empty());
            assert_eq!(result.mapped_keys, HashMap::from([(group2, 2)]));
        }
    }

    fn assert_fatal_old_lookup(key: CoordinatorKey, error: Errors) -> Error {
        let mut data = FindCoordinatorResponseData::new();
        data.set_error_code(error.code());
        let result = run_old_lookup(key.clone(), data);
        assert!(result.mapped_keys.is_empty());
        assert_eq!(
            result.failed_keys.keys().cloned().collect::<HashSet<_>>(),
            keys(std::slice::from_ref(&key))
        );
        result.failed_keys.get(&key).unwrap().clone()
    }

    /// Translated from `testFatalErrorOldLookupResponses`.
    #[test]
    fn test_fatal_error_old_lookup_responses() {
        let group = CoordinatorKey::by_transactional_id("foo");
        assert_eq!(
            assert_fatal_old_lookup(group.clone(), Errors::TransactionalIdAuthorizationFailed).error(),
            Errors::TransactionalIdAuthorizationFailed
        );
        assert_eq!(
            assert_fatal_old_lookup(group.clone(), Errors::UnknownServerError).error(),
            Errors::UnknownServerError
        );
        let throwable = assert_fatal_old_lookup(group, Errors::GroupAuthorizationFailed);
        match throwable {
            Error::GroupAuthorization(e) => assert_eq!(e.group_id, "foo"),
            other => panic!("expected GroupAuthorization, got {other:?}"),
        }
    }

    fn assert_fatal_lookup(key: CoordinatorKey, error: Errors) -> Error {
        let mut c = Coordinator::new();
        c.set_key(key.id_value.clone()).set_error_code(error.code());
        let mut data = FindCoordinatorResponseData::new();
        data.set_coordinators(vec![c]);
        let result = run_lookup(keys(std::slice::from_ref(&key)), data);
        assert!(result.mapped_keys.is_empty());
        assert_eq!(
            result.failed_keys.keys().cloned().collect::<HashSet<_>>(),
            keys(std::slice::from_ref(&key))
        );
        result.failed_keys.get(&key).unwrap().clone()
    }

    /// Translated from `testFatalErrorLookupResponses`.
    #[test]
    fn test_fatal_error_lookup_responses() {
        let group = CoordinatorKey::by_transactional_id("foo");
        assert_eq!(
            assert_fatal_lookup(group.clone(), Errors::TransactionalIdAuthorizationFailed).error(),
            Errors::TransactionalIdAuthorizationFailed
        );
        assert_eq!(
            assert_fatal_lookup(group.clone(), Errors::UnknownServerError).error(),
            Errors::UnknownServerError
        );
        let throwable = assert_fatal_lookup(group, Errors::GroupAuthorizationFailed);
        match throwable {
            Error::GroupAuthorization(e) => assert_eq!(e.group_id, "foo"),
            other => panic!("expected GroupAuthorization, got {other:?}"),
        }
    }
}
