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

//! Translated from
//! `org.apache.kafka.clients.consumer.internals.PositionsValidator`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};

use crate::ApiVersions;
use crate::common::{Error, TopicPartition};

use super::ConsumerMetadata;
use super::{FetchPosition, SubscriptionState};

/// As named, this struct validates positions in the [`SubscriptionState`]
/// based on the current [`ConsumerMetadata`] version. It maintains just
/// enough shared state to determine when it can avoid costly inter-task
/// communication in `Consumer::poll`.
///
/// Callers from the application task should not mutate any of the state
/// contained within this struct. It should be considered as *read-only*,
/// and only the background task should mutate the state.
///
/// # Ownership
///
/// One instance is shared three ways, exactly as Java shares it
/// (`AsyncKafkaConsumer.java:405,517`): the application task holds it to
/// run [`Self::can_skip_update_fetch_positions`] on the `poll()` critical
/// path, and the same `Arc` is threaded through
/// [`OffsetsRequestManager`](super::OffsetsRequestManager) into
/// [`OffsetFetcherUtils`](super::OffsetFetcherUtils) on the background
/// side. The shared state is what makes the app-side skip decision
/// correct, so a per-owner instance would silently break it.
///
/// # Deviations from Java
///
/// - Java's `Time time` field is not carried: the surrounding consumer
///   code passes `now_ms` explicitly (the convention throughout
///   `OffsetFetcherUtils`), so [`Self::refresh_and_get_partitions_to_validate`]
///   takes it as a parameter rather than reading a clock field.
/// - Java's `Logger log` field is implicit — the `log` crate's macros
///   carry the module path.
/// - `AtomicReference<RuntimeException>` becomes `Mutex<Option<Error>>`:
///   `Error` is not a pointer-sized value, so there is no atomic swap to
///   mirror. The critical sections are a single `take()` / `is_none()`
///   test and never `.await` (CLAUDE.md §9.6).
pub(crate) struct PositionsValidator {
    metadata: Arc<ConsumerMetadata>,
    subscriptions: Arc<Mutex<SubscriptionState>>,
    /// Error that occurred while validating positions, that will be
    /// propagated on the next call to validate positions. This could be an
    /// error received in the `OffsetsForLeaderEpoch` response, or a
    /// `LogTruncationError` detected when using a successful response to
    /// validate the positions. It will be cleared when returned.
    cached_validate_positions_error: Mutex<Option<Error>>,
    metadata_update_version: AtomicI32,
}

impl PositionsValidator {
    /// Java: `PositionsValidator(LogContext, Time, SubscriptionState, ConsumerMetadata)`
    /// (`PositionsValidator.java:63`).
    pub(crate) fn new(subscriptions: Arc<Mutex<SubscriptionState>>, metadata: Arc<ConsumerMetadata>) -> Self {
        Self {
            metadata,
            subscriptions,
            cached_validate_positions_error: Mutex::new(None),
            metadata_update_version: AtomicI32::new(-1),
        }
    }

    /// This method is called by the background task in response to
    /// `AsyncPollEvent` and `CheckAndUpdatePositionsEvent`.
    ///
    /// Mirrors `PositionsValidator.refreshAndGetPartitionsToValidate`.
    ///
    /// # Errors
    ///
    /// Propagates any error cached by [`Self::maybe_set_error`].
    pub(crate) fn refresh_and_get_partitions_to_validate(
        &self,
        api_versions: &ApiVersions,
        now_ms: i64,
    ) -> Result<HashMap<TopicPartition, FetchPosition>, Error> {
        self.maybe_return_error()?;

        // Validate each partition against the current leader and epoch
        // If we see a new metadata version, check all partitions
        self.validate_positions_on_metadata_change(api_versions);

        // Collect positions needing validation, with backoff
        let subs = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
        Ok(subs.partitions_needing_validation(now_ms))
    }

    /// If we have seen new metadata (as tracked by
    /// [`Metadata::update_version`](crate::Metadata::update_version)),
    /// then we should check that all the assignments have a valid position.
    ///
    /// Mirrors `PositionsValidator.validatePositionsOnMetadataChange`.
    pub(crate) fn validate_positions_on_metadata_change(&self, api_versions: &ApiVersions) {
        let metadata_arc = self.metadata.metadata_arc();
        let new_metadata_update_version = metadata_arc.update_version();
        if self
            .metadata_update_version
            .swap(new_metadata_update_version, Ordering::Relaxed)
            == new_metadata_update_version
        {
            return;
        }
        // Snapshot the assigned partitions before iterating: Java's
        // `forEach` body calls back into `subscriptions`, which in Rust
        // means re-locking the same non-reentrant mutex (§16).
        let assigned: Vec<TopicPartition> = {
            let subs = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
            subs.assigned_partitions().into_iter().collect()
        };
        for tp in &assigned {
            let leader_and_epoch = metadata_arc.current_leader(tp);
            let mut subs = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
            subs.maybe_validate_position_for_current_leader(api_versions, tp, &leader_and_epoch);
        }
    }

    /// Mirrors `PositionsValidator.maybeSetError`. Stores `error` for
    /// propagation on the next [`Self::maybe_return_error`]; a second error
    /// arriving while one is pending is discarded, as in Java.
    pub(crate) fn maybe_set_error(&self, error: Error) {
        let mut guard = self.cached_validate_positions_error.lock().expect("validate cache poisoned");
        if guard.is_none() {
            *guard = Some(error);
        } else {
            log::error!("Discarding error validating positions because another error is pending: {error}");
        }
    }

    /// Mirrors `PositionsValidator.maybeThrowError` — CLAUDE.md §2 renames
    /// a Java `throw` to a Rust `return Err(..)`, so this is
    /// `maybe_return_error`.
    ///
    /// # Errors
    ///
    /// Returns the cached validate-positions error, clearing it.
    pub(crate) fn maybe_return_error(&self) -> Result<(), Error> {
        let cached = self
            .cached_validate_positions_error
            .lock()
            .expect("validate cache poisoned")
            .take();
        match cached {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// This method is used by `AsyncKafkaConsumer` to determine if it can
    /// skip the step of validating positions as this is in the critical
    /// path for `Consumer::poll`. If the application task can safely and
    /// accurately determine that it doesn't need to perform the
    /// [`OffsetsRequestManager::update_fetch_positions`](super::OffsetsRequestManager::update_fetch_positions)
    /// call, a big performance savings can be realized.
    ///
    /// 1. Checks for previous errors from validation, and returns the error
    ///    if present
    /// 2. Checks that the current
    ///    [`Metadata::update_version`](crate::Metadata::update_version)
    ///    matches its current cached value to ensure that it is not stale
    /// 3. Checks that all positions are in the `FetchStates::Fetching` state
    ///    ([`SubscriptionState::has_all_fetch_positions`])
    ///
    /// If any checks fail, this method will return `false`, otherwise it
    /// will return `true`, which signals to the application task that the
    /// position validation step can be skipped.
    ///
    /// Mirrors `PositionsValidator.canSkipUpdateFetchPositions`.
    ///
    /// # Errors
    ///
    /// Returns the cached validate-positions error, clearing it.
    pub(crate) fn can_skip_update_fetch_positions(&self) -> Result<bool, Error> {
        self.maybe_return_error()?;

        if self.metadata_update_version.load(Ordering::Relaxed) != self.metadata.metadata_arc().update_version() {
            return Ok(false);
        }

        // If there are no partitions in the AWAIT_RESET, AWAIT_VALIDATION, or
        // INITIALIZING states, it's ok to skip.
        let subs = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
        Ok(subs.has_all_fetch_positions())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    use crate::common::Errors;
    use crate::common::Node;
    use crate::common::internals::ClusterResourceListeners;
    use crate::consumer::AutoOffsetResetStrategy;
    use crate::consumer::ConsumerConfig;
    use crate::metadata::LeaderAndEpoch;

    /// A validator over a fresh subscription state / metadata pair, plus the
    /// subscriptions handle so a test can seed positions.
    fn fixture() -> (PositionsValidator, Arc<Mutex<SubscriptionState>>, Arc<ConsumerMetadata>) {
        let config = ConsumerConfig::new(&std::collections::HashMap::from([
            ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
            ("group.id".to_string(), "g".to_string()),
        ]))
        .expect("config");
        let subscriptions = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::EARLIEST)));
        let metadata = Arc::new(ConsumerMetadata::new_config(
            &config,
            Arc::clone(&subscriptions),
            ClusterResourceListeners::new(),
        ));
        let validator = PositionsValidator::new(Arc::clone(&subscriptions), Arc::clone(&metadata));
        (validator, subscriptions, metadata)
    }

    /// Assigns `tp` and gives it a validated (i.e. `FETCHING`) position, so
    /// `has_all_fetch_positions()` answers `true`.
    fn assign_with_valid_position(subscriptions: &Arc<Mutex<SubscriptionState>>, tp: &TopicPartition) {
        let leader = Node::new(0, "localhost".to_string(), 1969);
        let position = FetchPosition::with_leader(10, Some(1), LeaderAndEpoch::new(Some(leader), Some(1)));
        let mut subs = subscriptions.lock().expect("subs");
        subs.assign_from_user(HashSet::from([tp.clone()])).expect("assign");
        subs.seek_validated(tp, position).expect("seek");
    }

    /// Java: `maybeSetError` uses `compareAndSet(null, e)`, so the FIRST
    /// error wins and later ones are logged and dropped;
    /// `maybeThrowError` is a `getAndSet(null)`, so it clears.
    #[test]
    fn maybe_set_error_keeps_the_first_error_and_maybe_return_error_clears_it() {
        let (validator, _subs, _metadata) = fixture();
        assert!(validator.maybe_return_error().is_ok(), "no error cached initially");

        validator.maybe_set_error(Error::new(Errors::UnknownServerError));
        validator.maybe_set_error(Error::new(Errors::InvalidTopicError));

        let err = validator.maybe_return_error().expect_err("cached error must surface");
        assert_eq!(err.error(), Errors::UnknownServerError, "the FIRST error is kept");
        assert!(validator.maybe_return_error().is_ok(), "the cache is cleared when returned");
    }

    /// `refreshAndGetPartitionsToValidate` starts with `maybeThrowError()`.
    #[test]
    fn refresh_and_get_partitions_to_validate_returns_the_cached_error_once() {
        let (validator, _subs, _metadata) = fixture();
        let api_versions = ApiVersions::new();
        validator.maybe_set_error(Error::new(Errors::UnknownServerError));

        let err = validator
            .refresh_and_get_partitions_to_validate(&api_versions, 0)
            .expect_err("the cached error must surface");
        assert_eq!(err.error(), Errors::UnknownServerError);

        assert!(
            validator.refresh_and_get_partitions_to_validate(&api_versions, 0).is_ok(),
            "the error is cleared, so the next call proceeds"
        );
    }

    /// `canSkipUpdateFetchPositions` check 1: a pending validation error is
    /// returned rather than silently skipped.
    #[test]
    fn can_skip_update_fetch_positions_returns_the_cached_error() {
        let (validator, subs, _metadata) = fixture();
        let tp = TopicPartition::new("t".to_string(), 0);
        assign_with_valid_position(&subs, &tp);
        validator.validate_positions_on_metadata_change(&ApiVersions::new());
        validator.maybe_set_error(Error::new(Errors::UnknownServerError));

        let err = validator
            .can_skip_update_fetch_positions()
            .expect_err("a pending error must surface even on the fast path");
        assert_eq!(err.error(), Errors::UnknownServerError);
    }

    /// `canSkipUpdateFetchPositions` check 2: a validator that has never
    /// observed the current metadata version holds the sentinel `-1`, so it
    /// cannot vouch for the positions.
    #[test]
    fn can_skip_update_fetch_positions_is_false_when_the_metadata_version_is_stale() {
        let (validator, subs, _metadata) = fixture();
        let tp = TopicPartition::new("t".to_string(), 0);
        assign_with_valid_position(&subs, &tp);

        assert!(
            !validator.can_skip_update_fetch_positions().expect("no cached error"),
            "the cached metadata version is still the -1 sentinel"
        );
    }

    /// `canSkipUpdateFetchPositions`: all three checks pass.
    #[test]
    fn can_skip_update_fetch_positions_is_true_when_version_matches_and_positions_are_valid() {
        let (validator, subs, _metadata) = fixture();
        let tp = TopicPartition::new("t".to_string(), 0);
        assign_with_valid_position(&subs, &tp);
        // Brings `metadata_update_version` up to the metadata's current value.
        validator.validate_positions_on_metadata_change(&ApiVersions::new());

        assert!(
            validator.can_skip_update_fetch_positions().expect("no cached error"),
            "version matches and every assigned partition has a valid position"
        );
    }

    /// `canSkipUpdateFetchPositions` check 3: an assigned partition without a
    /// position (Java's `INITIALIZING`) blocks the skip.
    #[test]
    fn can_skip_update_fetch_positions_is_false_when_a_position_is_missing() {
        let (validator, subs, _metadata) = fixture();
        let tp = TopicPartition::new("t".to_string(), 0);
        {
            let mut guard = subs.lock().expect("subs");
            guard.assign_from_user(HashSet::from([tp.clone()])).expect("assign");
        }
        validator.validate_positions_on_metadata_change(&ApiVersions::new());

        assert!(
            !validator.can_skip_update_fetch_positions().expect("no cached error"),
            "an assigned partition with no valid position must block the skip"
        );
    }
}
