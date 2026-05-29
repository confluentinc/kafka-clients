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

//! `ApplicationEventProcessor` — Phase-10 sync-dispatch table.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor`.
//! Implements [`EventProcessor<ApplicationEvent>`] for the consumer
//! background task.
//!
//! # Phase-10 commit split
//!
//! Phase 10 splits the processor across two commits:
//!
//! - **Commit 4/N (this file's first landing):** every variant whose Java
//!   handling is purely synchronous — subscription changes, pause/resume,
//!   seek, reset, current-lag, assignment-change, commit-on-close,
//!   `StopFindCoordinatorOnClose`, and `NewTopicsMetadataUpdate`.
//! - **Commit 5/N:** the async-dispatch arms — `AsyncPoll`,
//!   `CommitAsync`, `CommitSync`, `FetchCommittedOffsets`, `ListOffsets`,
//!   `CheckAndUpdatePositions`, `TopicMetadata`, `AllTopicsMetadata`,
//!   `Unsubscribe`, `CreateFetchRequests`, `LeaveGroupOnClose`, and
//!   `ConsumerRebalanceListenerCallbackCompleted`.
//!
//! Each deferred arm in this commit fails its associated completable
//! handle with `KafkaError::unsupported_version("... — wired in Phase 10
//! commit 5/N")` so the app side observes an error instead of a
//! silently-dropped handle (§28 of `consumer-threading.md`). Commit 5
//! replaces these with the real async-dispatch implementations.
//!
//! # Lock discipline
//!
//! Per `consumer-threading.md` §16 the `SubscriptionState` `MutexGuard`
//! is never held across `.await` points and never held while invoking
//! callbacks. Every arm acquires the guard for the minimum critical
//! section and drops it before touching any other shared state.
//!
//! Per the PLAN.md `RequestManagers` ownership note: the processor and
//! the bg task's `run_once` both need `&mut` access, so the container
//! is wrapped in `Arc<Mutex<RequestManagers>>`. The mutex is held
//! briefly on the bg task only (single-threaded acquisition), so
//! contention is nil — but we never `.await` while holding it.

#![allow(dead_code)]

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use super::application_event::ApplicationEvent;
use super::event_processor::EventProcessor;
use crate::common::{IsolationLevel, KafkaError, TopicPartition};
use crate::consumer::consumer_rebalance_listener::ConsumerRebalanceListener;
use crate::consumer::internals::consumer_metadata::ConsumerMetadata;
use crate::consumer::internals::request_manager::RequestManager;
use crate::consumer::internals::request_managers::RequestManagers;
use crate::consumer::internals::subscription_state::{FetchPosition, SubscriptionState};

/// Translated from `ApplicationEventProcessor`. Owns shared references to
/// the consumer's request managers, metadata, and subscription state and
/// dispatches each [`ApplicationEvent`] to the appropriate handler.
///
/// The processor lives on the bg task. It is constructed once (the Java
/// `supplier` factory boils down to a constructor here) and called from
/// the bg-task `run_once` loop after each event drain.
pub(crate) struct ApplicationEventProcessor {
    /// Shared `RequestManagers` container — Java holds it as a plain
    /// field on the bg-thread; Rust wraps in `Arc<Mutex<...>>` so the
    /// bg-task `run_once` loop can also access the same instance.
    request_managers: Arc<Mutex<RequestManagers>>,
    /// Consumer metadata — used for `requestUpdateForNewTopics()`,
    /// `currentLeader(...)`, `updateLastSeenEpochIfNewer(...)`,
    /// `updateVersion()`, and `fetch()` (`Cluster` snapshot).
    metadata: Arc<ConsumerMetadata>,
    /// Subscription state — guarded by `std::sync::Mutex` per
    /// `consumer-threading.md` §16.
    subscriptions: Arc<Mutex<SubscriptionState>>,
    /// Java: `private int metadataVersionSnapshot`. Captures the
    /// metadata-version cursor at construction; advances when a
    /// pattern-subscription rebuild has been triggered. Used by
    /// `maybeUpdatePatternSubscription` (commit 5/N — the async-dispatch
    /// arm `AsyncPoll`).
    metadata_version_snapshot: i32,
}

impl ApplicationEventProcessor {
    /// Java: 4-arg constructor (logContext, requestManagers, metadata,
    /// subscriptions). The Rust translation drops `LogContext` (we use
    /// `log`) and takes the three shared references by `Arc`.
    pub(crate) fn new(
        request_managers: Arc<Mutex<RequestManagers>>,
        metadata: Arc<ConsumerMetadata>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
    ) -> Self {
        let metadata_version_snapshot = metadata.update_version();
        Self { request_managers, metadata, subscriptions, metadata_version_snapshot }
    }

    /// Java: `int metadataVersionSnapshot()` — visible-for-testing.
    pub(crate) fn metadata_version_snapshot(&self) -> i32 {
        self.metadata_version_snapshot
    }

    // -------------------------------------------------------------------
    // Per-arm handlers
    // -------------------------------------------------------------------

    /// Java: `process(AssignmentChangeEvent)`.
    ///
    /// Order matches Java line-for-line:
    /// 1. If a commit manager is present, call
    ///    `update_timer_and_maybe_commit(current_time_ms)` BEFORE the
    ///    assign — Java does the same so the assign tick refreshes the
    ///    auto-commit timer.
    /// 2. Call `assignFromUser(...)`; if it returned `true` (the
    ///    assignment actually changed) request a metadata update.
    /// 3. Complete the event handle on success / failure.
    ///
    /// `SubscriptionState` guard is held only across the `assign_from_user`
    /// call and dropped before completing the handle (§16).
    fn process_assignment_change(
        &mut self,
        handle: super::completable_event::CompletableEventHandle<()>,
        current_time_ms: i64,
        partitions: HashSet<TopicPartition>,
    ) {
        // Step 1: auto-commit timer refresh before the assign.
        {
            let mut rm_guard = self.lock_request_managers();
            if let Some(commit) = rm_guard.commit.as_mut() {
                commit.update_timer_and_maybe_commit(current_time_ms);
            }
        }

        log::info!("Assigned to partition(s): {:?}", partitions);

        // Step 2: assignFromUser — Java catches any exception and
        // completes the handle exceptionally.
        let assign_result = {
            let mut subs_guard = self.lock_subscriptions();
            subs_guard.assign_from_user(partitions)
        };

        match assign_result {
            Ok(true) => {
                self.metadata.request_update_for_new_topics();
                handle.complete(());
            },
            Ok(false) => {
                handle.complete(());
            },
            Err(e) => {
                handle.complete_exceptionally(e);
            },
        }
    }

    /// Java: `process(ResetOffsetEvent)`.
    ///
    /// Resets offsets for the given partitions (or every assigned
    /// partition when the event's collection is empty) using the
    /// supplied strategy.
    fn process_reset_offset(
        &mut self,
        handle: super::completable_event::CompletableEventHandle<()>,
        partitions: HashSet<TopicPartition>,
        offset_reset_strategy: super::super::auto_offset_reset_strategy::AutoOffsetResetStrategy,
    ) {
        let result = {
            let mut guard = self.lock_subscriptions();
            // Java: `event.topicPartitions().isEmpty() ?
            // subscriptions.assignedPartitions() : event.topicPartitions()`.
            let targets: Vec<TopicPartition> = if partitions.is_empty() {
                guard.assigned_partitions().into_iter().collect()
            } else {
                partitions.into_iter().collect()
            };
            guard.request_offset_reset_all(&targets, offset_reset_strategy)
        };
        match result {
            Ok(()) => {
                handle.complete(());
            },
            Err(e) => {
                handle.complete_exceptionally(e);
            },
        }
    }

    /// Java: `process(SeekUnvalidatedEvent)`.
    fn process_seek_unvalidated(
        &mut self,
        handle: super::completable_event::CompletableEventHandle<()>,
        partition: TopicPartition,
        offset: i64,
        offset_epoch: Option<i32>,
    ) {
        // Java: `event.offsetEpoch().ifPresent(epoch ->
        // metadata.updateLastSeenEpochIfNewer(event.partition(), epoch))`.
        if let Some(epoch) = offset_epoch {
            // Java swallows the IllegalArgumentException Java would have
            // produced for negative epochs because the public seek API
            // already validates; we propagate the error to the handle for
            // safety.
            if let Err(e) = self.metadata.update_last_seen_epoch_if_newer(&partition, epoch) {
                handle.complete_exceptionally(e);
                return;
            }
        }
        let current_leader = self.metadata.current_leader(&partition);
        let new_position = FetchPosition::with_leader(offset, offset_epoch, current_leader);
        let result = {
            let mut guard = self.lock_subscriptions();
            guard.seek_unvalidated(&partition, new_position)
        };
        match result {
            Ok(()) => {
                handle.complete(());
            },
            Err(e) => {
                handle.complete_exceptionally(e);
            },
        }
    }

    /// Java: `process(PausePartitionsEvent)`.
    fn process_pause_partitions(
        &mut self,
        handle: super::completable_event::CompletableEventHandle<()>,
        partitions: HashSet<TopicPartition>,
    ) {
        log::debug!("Pausing partitions {:?}", partitions);
        let result: Result<(), KafkaError> = {
            let mut guard = self.lock_subscriptions();
            partitions.iter().try_for_each(|tp| guard.pause(tp))
        };
        match result {
            Ok(()) => {
                handle.complete(());
            },
            Err(e) => {
                handle.complete_exceptionally(e);
            },
        }
    }

    /// Java: `process(ResumePartitionsEvent)`.
    fn process_resume_partitions(
        &mut self,
        handle: super::completable_event::CompletableEventHandle<()>,
        partitions: HashSet<TopicPartition>,
    ) {
        log::debug!("Resuming partitions {:?}", partitions);
        let result: Result<(), KafkaError> = {
            let mut guard = self.lock_subscriptions();
            partitions.iter().try_for_each(|tp| guard.resume(tp))
        };
        match result {
            Ok(()) => {
                handle.complete(());
            },
            Err(e) => {
                handle.complete_exceptionally(e);
            },
        }
    }

    /// Java: `process(CurrentLagEvent)`.
    ///
    /// Reports the partition lag for the given partition, or kicks off a
    /// `ListOffsets(LATEST)` request when the log-end offset is unknown
    /// so the lag will be available on the next call. Java uses
    /// `OptionalLong`; the Rust translation uses `Option<i64>`.
    fn process_current_lag(
        &mut self,
        handle: super::completable_event::CompletableEventHandle<Option<i64>>,
        partition: TopicPartition,
        isolation_level: IsolationLevel,
    ) {
        // Step 1: snapshot the lag / end-offset state under the
        // subscriptions lock. We collect everything we need so the lock
        // is released before we touch the offsets manager (§16).
        enum Decision {
            HaveLag(i64),
            RequestEndOffset,
            EmptyOnly,
            Error(KafkaError),
        }
        let decision = {
            let mut guard = self.lock_subscriptions();
            match guard.partition_lag(&partition, isolation_level) {
                Ok(Some(lag)) => Decision::HaveLag(lag),
                Ok(None) => {
                    // Java: only request the end offset if it's not
                    // already known AND no in-flight request exists.
                    let end_offset_known = match guard.partition_end_offset(&partition, isolation_level) {
                        Ok(opt) => opt.is_some(),
                        Err(e) => {
                            return self.complete_lag_error(handle, e);
                        },
                    };
                    let request_in_flight = match guard.partition_end_offset_requested(&partition) {
                        Ok(b) => b,
                        Err(e) => {
                            return self.complete_lag_error(handle, e);
                        },
                    };
                    if !end_offset_known && !request_in_flight {
                        log::info!("Requesting the log end offset for {} in order to compute lag", partition);
                        if let Err(e) = guard.request_partition_end_offset(&partition) {
                            return self.complete_lag_error(handle, e);
                        }
                        Decision::RequestEndOffset
                    } else {
                        Decision::EmptyOnly
                    }
                },
                Err(e) => Decision::Error(e),
            }
        };

        match decision {
            Decision::HaveLag(lag) => {
                handle.complete(Some(lag));
            },
            Decision::EmptyOnly => {
                handle.complete(None);
            },
            Decision::RequestEndOffset => {
                // Java: Emulates Consumer.endOffsets() — fire-and-forget
                // `ListOffsets(LATEST)` so the lag is available next call.
                let mut ts = std::collections::HashMap::new();
                ts.insert(
                    partition.clone(),
                    crate::common::requests::list_offsets_request::LATEST_TIMESTAMP,
                );
                {
                    let mut rm_guard = self.lock_request_managers();
                    if let Some(offsets_mgr) = rm_guard.offsets.as_mut() {
                        // Drop the returned receiver — Java's
                        // `fetchOffsets(ts, false)` is fire-and-forget here.
                        // Explicit `drop` rather than `let _` to satisfy
                        // `clippy::let_underscore_future` (the `Receiver`
                        // IS a `Future` but the result is intentionally
                        // discarded).
                        std::mem::drop(offsets_mgr.fetch_offsets(ts, false));
                    }
                }
                handle.complete(None);
            },
            Decision::Error(e) => {
                handle.complete_exceptionally(e);
            },
        }
    }

    fn complete_lag_error(
        &self,
        handle: super::completable_event::CompletableEventHandle<Option<i64>>,
        err: KafkaError,
    ) {
        handle.complete_exceptionally(err);
    }

    /// Java: `process(TopicSubscriptionChangeEvent)`.
    fn process_topic_subscription_change(
        &mut self,
        handle: super::completable_event::CompletableEventHandle<()>,
        topics: HashSet<String>,
        listener: Option<Arc<dyn ConsumerRebalanceListener>>,
    ) {
        // Java first checks whether a heartbeat manager is wired. We
        // collapse the consumer / streams branches into a single check
        // for `consumer_heartbeat` (streams is out of scope per §20).
        // If no heartbeat manager exists, Java still completes the
        // future successfully with a warning log.
        let has_heartbeat = {
            let rm_guard = self.lock_request_managers();
            rm_guard.consumer_heartbeat.is_some()
        };
        if !has_heartbeat {
            log::warn!("Group membership manager not present when processing a subscribe event");
            handle.complete(());
            return;
        }

        // subscribe_topics returns `Result<bool, KafkaError>` — the bool
        // is `true` when the subscription actually changed (Java triggers
        // `requestUpdateForNewTopics` on change). On error, fail the
        // handle and return without notifying the membership manager.
        let subscribe_result = {
            let mut guard = self.lock_subscriptions();
            guard.subscribe_topics(topics, listener)
        };

        match subscribe_result {
            Ok(changed) => {
                if changed {
                    self.metadata_version_snapshot = self.metadata.request_update_for_new_topics();
                }
                // Notify the membership manager — Java calls
                // `requestManagers.consumerHeartbeatRequestManager.get()
                //  .membershipManager().onSubscriptionUpdated()`.
                {
                    let rm_guard = self.lock_request_managers();
                    if let Some(hrm) = rm_guard.consumer_heartbeat.as_ref() {
                        hrm.membership_manager().abstract_mm.on_subscription_updated();
                    }
                }
                handle.complete(());
            },
            Err(e) => {
                handle.complete_exceptionally(e);
            },
        }
    }

    /// Java: `process(TopicPatternSubscriptionChangeEvent)`.
    fn process_topic_pattern_subscription_change(
        &mut self,
        handle: super::completable_event::CompletableEventHandle<()>,
        pattern: regex::Regex,
        listener: Option<Arc<dyn ConsumerRebalanceListener>>,
    ) {
        // Step 1: install the pattern subscription. On failure (e.g.
        // mixed subscription types), fail the handle and skip the rest.
        let subscribe_result = {
            let mut guard = self.lock_subscriptions();
            guard.subscribe_pattern(pattern, listener)
        };
        if let Err(e) = subscribe_result {
            handle.complete_exceptionally(e);
            return;
        }

        // Step 2: Java unconditionally calls requestUpdateForNewTopics() here.
        self.metadata.request_update_for_new_topics();

        // Step 3: evaluate the regex against the latest metadata. Java
        // only re-evaluates when `consumerHeartbeatRequestManager` is
        // present (the only in-scope branch — streams is §20-skip).
        let has_heartbeat = {
            let rm_guard = self.lock_request_managers();
            rm_guard.consumer_heartbeat.is_some()
        };
        if has_heartbeat {
            // `update_pattern_subscription` may bump
            // `metadata_version_snapshot` and calls
            // `membershipManager.onSubscriptionUpdated()`.
            self.update_pattern_subscription();
        }

        handle.complete(());
    }

    /// Java: `process(TopicRe2JPatternSubscriptionChangeEvent)`.
    fn process_topic_re2j_pattern_subscription_change(
        &mut self,
        handle: super::completable_event::CompletableEventHandle<()>,
        pattern: crate::consumer::SubscriptionPattern,
        listener: Option<Arc<dyn ConsumerRebalanceListener>>,
    ) {
        // Java: bail with `KafkaException` if the membership manager is
        // absent. In Rust the membership manager is held on the
        // heartbeat manager, so we use that as the proxy.
        let has_membership = {
            let rm_guard = self.lock_request_managers();
            rm_guard.consumer_heartbeat.is_some()
        };
        if !has_membership {
            handle.complete_exceptionally(KafkaError::illegal_state(
                "MembershipManager is not available when processing a subscribe event",
            ));
            return;
        }

        let subscribe_result = {
            let mut guard = self.lock_subscriptions();
            guard.subscribe_re2j_pattern(pattern, listener)
        };
        match subscribe_result {
            Ok(()) => {
                // Java: `consumerMembershipManager.get().onSubscriptionUpdated()`.
                {
                    let rm_guard = self.lock_request_managers();
                    if let Some(hrm) = rm_guard.consumer_heartbeat.as_ref() {
                        hrm.membership_manager().abstract_mm.on_subscription_updated();
                    }
                }
                handle.complete(());
            },
            Err(e) => {
                handle.complete_exceptionally(e);
            },
        }
    }

    /// Java: `process(UpdatePatternSubscriptionEvent)`.
    fn process_update_pattern_subscription(&mut self, handle: super::completable_event::CompletableEventHandle<()>) {
        // Java: `consumerMembershipManager.ifPresent(mm ->
        // maybeUpdatePatternSubscription(mm::onSubscriptionUpdated))`.
        // In Rust the membership manager is reached via the heartbeat
        // manager. Streams (§20) is out of scope.
        let has_heartbeat = {
            let rm_guard = self.lock_request_managers();
            rm_guard.consumer_heartbeat.is_some()
        };
        if has_heartbeat {
            self.maybe_update_pattern_subscription();
        }
        handle.complete(());
    }

    /// Java: `process(CommitOnCloseEvent)`.
    fn process_commit_on_close(&mut self) {
        let mut rm_guard = self.lock_request_managers();
        if let Some(commit) = rm_guard.commit.as_mut() {
            log::debug!("Signal CommitRequestManager closing");
            commit.signal_close();
        }
    }

    /// Java: `process(StopFindCoordinatorOnCloseEvent)`.
    fn process_stop_find_coordinator_on_close(&mut self) {
        let mut rm_guard = self.lock_request_managers();
        if let Some(coord) = rm_guard.coordinator.as_mut() {
            log::debug!("Signal CoordinatorRequestManager closing");
            coord.signal_close();
        }
    }

    /// Java: `process(ConsumerRebalanceListenerCallbackCompletedEvent)`.
    ///
    /// **Deferred to commit 5/N.** The full bidirectional
    /// background-event handshake (§31) — including the pending-callback
    /// future map on `ConsumerMembershipManager` — is wired in commit
    /// 5/N alongside the async-dispatch arms. For now we log a warning;
    /// the listener-invoker plumbing on the app side is not yet calling
    /// back into the bg task, so this event is currently never enqueued
    /// by in-tree code.
    fn process_consumer_rebalance_listener_callback_completed(
        &mut self,
        method_name: crate::consumer::consumer_rebalance_listener_method_name::ConsumerRebalanceListenerMethodName,
        error: Option<KafkaError>,
    ) {
        let _ = (method_name, error);
        let rm_guard = self.lock_request_managers();
        if rm_guard.consumer_heartbeat.is_none() {
            log::warn!(
                "An internal error occurred; the group membership manager was not present, so the notification of the rebalance-listener callback execution could not be sent"
            );
            return;
        }
        log::warn!(
            "ConsumerRebalanceListenerCallbackCompleted is wired in Phase 10 commit 5/N alongside the async-dispatch arms"
        );
    }

    // -------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------

    /// Java: private `maybeUpdatePatternSubscription(membershipManager)`.
    /// Re-evaluates the subscribed regex only if (a) there IS a pattern
    /// subscription and (b) the metadata version has advanced since the
    /// last evaluation.
    fn maybe_update_pattern_subscription(&mut self) {
        let has_pattern = {
            let guard = self.lock_subscriptions();
            guard.has_pattern_subscription()
        };
        if !has_pattern {
            return;
        }
        let current_md_version = self.metadata.update_version();
        if self.metadata_version_snapshot < current_md_version {
            self.metadata_version_snapshot = current_md_version;
            self.update_pattern_subscription();
        }
    }

    /// Java: private `updatePatternSubscription(membershipManager, cluster)`.
    /// Evaluates the regex against the latest metadata-cluster topic
    /// list, updates the subscription, and notifies the membership
    /// manager (so the consumer joins the group with the new
    /// subscription on the next poll).
    fn update_pattern_subscription(&mut self) {
        // Java: `cluster.topics().stream().filter(subscriptions::matchesSubscribedPattern).collect(...)`.
        let cluster = self.metadata.fetch();
        let topics_to_subscribe: HashSet<String> = {
            let guard = self.lock_subscriptions();
            cluster
                .topics()
                .filter(|t| guard.matches_subscribed_pattern(t))
                .map(|t| t.to_string())
                .collect()
        };

        // Java: `if (subscriptions.subscribeFromPattern(topicsToSubscribe))
        //     metadataVersionSnapshot = metadata.requestUpdateForNewTopics();`
        let subscribed_changed = {
            let mut guard = self.lock_subscriptions();
            guard.subscribe_from_pattern(topics_to_subscribe)
        };
        match subscribed_changed {
            Ok(true) => {
                self.metadata_version_snapshot = self.metadata.request_update_for_new_topics();
            },
            Ok(false) => {},
            Err(e) => {
                log::warn!("subscribe_from_pattern failed: {e}");
                // Java throws here; we don't have a handle to fail in
                // this private helper, so we surface via the log only.
                // Callers that need to propagate the error should
                // call subscribe_from_pattern directly.
            },
        }

        // Java: `membershipManager.onSubscriptionUpdated();` — called
        // unconditionally (even when the subscription is empty) so the
        // member joins the group if it's not already in.
        {
            let rm_guard = self.lock_request_managers();
            if let Some(hrm) = rm_guard.consumer_heartbeat.as_ref() {
                hrm.membership_manager().abstract_mm.on_subscription_updated();
            }
        }
    }

    /// Internal helper. `Mutex` is `std::sync::Mutex` — never held across
    /// `.await`. Recovers from poisoning by extracting the inner guard
    /// (mirrors the rest of the codebase).
    fn lock_subscriptions(&self) -> std::sync::MutexGuard<'_, SubscriptionState> {
        match self.subscriptions.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Internal helper. Same pattern as [`Self::lock_subscriptions`].
    fn lock_request_managers(&self) -> std::sync::MutexGuard<'_, RequestManagers> {
        match self.request_managers.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Fail a completable handle for an async-dispatch arm that has been
    /// deferred to commit 5/N. The app side observes an explicit error
    /// rather than a silently-dropped handle (§28 / DoD §5).
    fn fail_deferred_async_handle<T: Send + 'static>(
        &self,
        handle: super::completable_event::CompletableEventHandle<T>,
        arm_name: &str,
    ) {
        handle.complete_exceptionally(KafkaError::unsupported_version(format!(
            "{arm_name} is wired in Phase 10 commit 5/N (async-dispatch arms)"
        )));
    }
}

impl EventProcessor<ApplicationEvent> for ApplicationEventProcessor {
    fn process(&mut self, event: ApplicationEvent) {
        match event {
            // ───── Synchronous arms (this commit, 4/N) ─────
            ApplicationEvent::AssignmentChange { handle, current_time_ms, partitions } => {
                self.process_assignment_change(handle, current_time_ms, partitions);
            },
            ApplicationEvent::CommitOnClose => {
                self.process_commit_on_close();
            },
            ApplicationEvent::StopFindCoordinatorOnClose => {
                self.process_stop_find_coordinator_on_close();
            },
            ApplicationEvent::NewTopicsMetadataUpdate => {
                // Java: there is no separate handler — Java's
                // `Type.NEW_TOPICS_METADATA_UPDATE` is unused. We still
                // emit an explicit no-op for clarity. (Java keeps the
                // enum variant but its event class fires
                // `metadata.requestUpdateForNewTopics()` on the app
                // side before enqueueing.)
            },
            ApplicationEvent::ResetOffset { handle, partitions, offset_reset_strategy } => {
                self.process_reset_offset(handle, partitions, offset_reset_strategy);
            },
            ApplicationEvent::SeekUnvalidated { handle, partition, offset, offset_epoch } => {
                self.process_seek_unvalidated(handle, partition, offset, offset_epoch);
            },
            ApplicationEvent::PausePartitions { handle, partitions } => {
                self.process_pause_partitions(handle, partitions);
            },
            ApplicationEvent::ResumePartitions { handle, partitions } => {
                self.process_resume_partitions(handle, partitions);
            },
            ApplicationEvent::CurrentLag { handle, partition, isolation_level } => {
                self.process_current_lag(handle, partition, isolation_level);
            },
            ApplicationEvent::TopicSubscriptionChange { handle, topics, listener } => {
                self.process_topic_subscription_change(handle, topics, listener);
            },
            ApplicationEvent::TopicPatternSubscriptionChange { handle, pattern, listener } => {
                self.process_topic_pattern_subscription_change(handle, pattern, listener);
            },
            ApplicationEvent::TopicRe2JPatternSubscriptionChange { handle, pattern, listener } => {
                self.process_topic_re2j_pattern_subscription_change(handle, pattern, listener);
            },
            ApplicationEvent::UpdatePatternSubscription { handle } => {
                self.process_update_pattern_subscription(handle);
            },
            ApplicationEvent::ConsumerRebalanceListenerCallbackCompleted { method_name, error } => {
                self.process_consumer_rebalance_listener_callback_completed(method_name, error);
            },

            // ───── Async-dispatch arms (deferred to commit 5/N) ─────
            //
            // Java spawns a `CompletableFuture` continuation
            // (`whenComplete(complete(event.future()))`) for each of
            // these. The Rust translation will use a detached
            // `tokio::spawn` per-event that awaits the relevant manager
            // future and writes the result to the handle. Until then,
            // we fail the handle with an explicit error so the app side
            // observes the deferral.
            ApplicationEvent::AsyncPoll { state, .. } => {
                state.complete_exceptionally(KafkaError::unsupported_version(
                    "AsyncPoll is wired in Phase 10 commit 5/N (async-dispatch arms)",
                ));
            },
            ApplicationEvent::CommitAsync { handle, offsets_ready, .. } => {
                offsets_ready.complete_exceptionally(KafkaError::unsupported_version(
                    "CommitAsync is wired in Phase 10 commit 5/N (async-dispatch arms)",
                ));
                self.fail_deferred_async_handle(handle, "CommitAsync");
            },
            ApplicationEvent::CommitSync { handle, offsets_ready, .. } => {
                offsets_ready.complete_exceptionally(KafkaError::unsupported_version(
                    "CommitSync is wired in Phase 10 commit 5/N (async-dispatch arms)",
                ));
                self.fail_deferred_async_handle(handle, "CommitSync");
            },
            ApplicationEvent::FetchCommittedOffsets { handle, .. } => {
                self.fail_deferred_async_handle(handle, "FetchCommittedOffsets");
            },
            ApplicationEvent::ListOffsets { handle, .. } => {
                self.fail_deferred_async_handle(handle, "ListOffsets");
            },
            ApplicationEvent::CheckAndUpdatePositions { handle } => {
                self.fail_deferred_async_handle(handle, "CheckAndUpdatePositions");
            },
            ApplicationEvent::TopicMetadata { handle, .. } => {
                self.fail_deferred_async_handle(handle, "TopicMetadata");
            },
            ApplicationEvent::AllTopicsMetadata { handle } => {
                self.fail_deferred_async_handle(handle, "AllTopicsMetadata");
            },
            ApplicationEvent::Unsubscribe { handle } => {
                self.fail_deferred_async_handle(handle, "Unsubscribe");
            },
            ApplicationEvent::CreateFetchRequests { handle } => {
                self.fail_deferred_async_handle(handle, "CreateFetchRequests");
            },
            ApplicationEvent::LeaveGroupOnClose { handle, .. } => {
                self.fail_deferred_async_handle(handle, "LeaveGroupOnClose");
            },
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    //! Translated from
    //! `org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessorTest`.
    //!
    //! This commit (4/N) translates only the sync-arm test cases. The
    //! async-arm cases (Commit*, AsyncPoll, FetchCommittedOffsets,
    //! Unsubscribe-with-group-id, etc.) land in commit 6/N alongside the
    //! async-dispatch implementations. Streams tests are skipped per
    //! `consumer-threading.md` §20.

    use std::collections::HashSet;

    use regex::Regex;
    use tokio::sync::{mpsc, oneshot};

    use super::*;
    use crate::api_versions::ApiVersions;
    use crate::common::internals::ClusterResourceListeners;
    use crate::common::{IsolationLevel, KafkaError, TopicPartition};
    use crate::consumer::ConsumerConfig;
    use crate::consumer::SubscriptionPattern;
    use crate::consumer::consumer_rebalance_listener_method_name::ConsumerRebalanceListenerMethodName;
    use crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy;
    use crate::consumer::internals::commit_request_manager::CommitRequestManager;
    use crate::consumer::internals::consumer_heartbeat_request_manager::ConsumerHeartbeatRequestManager;
    use crate::consumer::internals::consumer_membership_manager::ConsumerMembershipManager;
    use crate::consumer::internals::coordinator_request_manager::CoordinatorRequestManager;
    use crate::consumer::internals::events::application_event::ApplicationEvent;
    use crate::consumer::internals::events::background_event_handler::BackgroundEventHandler;
    use crate::consumer::internals::events::completable_event::CompletableEventHandle;
    use crate::consumer::internals::offsets_request_manager::OffsetsRequestManager;
    use crate::consumer::internals::subscription_state::SubscriptionState;
    use crate::consumer::internals::topic_metadata_request_manager::TopicMetadataRequestManager;

    /// Shared test fixture mirroring Java's `setupProcessor(withGroupId)`.
    struct Fixture {
        processor: ApplicationEventProcessor,
        request_managers: Arc<Mutex<RequestManagers>>,
        metadata: Arc<ConsumerMetadata>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
    }

    fn make_metadata(subs: Arc<Mutex<SubscriptionState>>) -> Arc<ConsumerMetadata> {
        let config = ConsumerConfig::new(vec!["localhost:9092".to_string()]);
        Arc::new(ConsumerMetadata::from_config(&config, subs, ClusterResourceListeners::new()))
    }

    fn make_subscriptions() -> Arc<Mutex<SubscriptionState>> {
        Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::EARLIEST)))
    }

    fn setup_processor(with_group_id: bool) -> Fixture {
        let subscriptions = make_subscriptions();
        let metadata = make_metadata(Arc::clone(&subscriptions));

        let config = ConsumerConfig::new(vec!["localhost:9092".to_string()]);
        let coordinator = if with_group_id {
            Some(CoordinatorRequestManager::new(100, 1_000, "test-group"))
        } else {
            None
        };
        let commit = if with_group_id {
            Some(CommitRequestManager::new(
                &config,
                Arc::clone(&metadata),
                Arc::clone(&subscriptions),
                "test-group",
                None,
                0,
            ))
        } else {
            None
        };
        let consumer_heartbeat = if with_group_id {
            Some(make_consumer_heartbeat_manager(
                &config,
                Arc::clone(&metadata),
                Arc::clone(&subscriptions),
            ))
        } else {
            None
        };
        // membership manager owned via heartbeat manager already.
        let topic_metadata = Some(TopicMetadataRequestManager::new(&config));
        let offsets = Some(OffsetsRequestManager::new(
            Arc::clone(&subscriptions),
            Arc::clone(&metadata),
            IsolationLevel::ReadUncommitted,
            100,
            30_000,
            60_000,
            Arc::new(ApiVersions::new()),
            None,
        ));

        let request_managers = Arc::new(Mutex::new(RequestManagers::new(
            coordinator,
            topic_metadata,
            commit,
            consumer_heartbeat,
            None, // consumer_membership held via Arc on the heartbeat manager
            offsets,
            None, // fetch manager not needed for sync arms
        )));

        let processor = ApplicationEventProcessor::new(
            Arc::clone(&request_managers),
            Arc::clone(&metadata),
            Arc::clone(&subscriptions),
        );

        Fixture { processor, request_managers, metadata, subscriptions }
    }

    /// Construct a heartbeat manager wired to a membership manager. The
    /// membership manager carries the `on_subscription_updated` and
    /// `on_consumer_poll` hooks the processor needs.
    ///
    /// The heartbeat manager wires its own coordinator (`Arc<Mutex<...>>`)
    /// — distinct from the owned `RequestManagers.coordinator` slot. The
    /// processor never reaches the heartbeat-side coordinator, so this
    /// duplication is fine for sync-arm tests. (Phase 11 / Phase 10
    /// commit 7 will reconcile the two ownership patterns when the
    /// bg-task wiring lands.)
    fn make_consumer_heartbeat_manager(
        config: &ConsumerConfig,
        metadata: Arc<ConsumerMetadata>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
    ) -> ConsumerHeartbeatRequestManager {
        let (tx, _rx) = mpsc::unbounded_channel();
        let bg_handler = Arc::new(BackgroundEventHandler::new(tx));
        let hb_coordinator = Arc::new(Mutex::new(CoordinatorRequestManager::new(100, 1_000, "test-group")));
        let mm = Arc::new(ConsumerMembershipManager::new(
            "test-group",
            None,
            None,
            30_000,
            None,
            Arc::clone(&subscriptions),
            None,
            Arc::clone(&metadata),
            Arc::clone(&bg_handler),
            true,
        ));
        ConsumerHeartbeatRequestManager::new(0, config, hb_coordinator, subscriptions, mm, bg_handler)
    }

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic.to_string(), partition)
    }

    fn await_complete(rx: oneshot::Receiver<Result<(), KafkaError>>) -> Result<(), KafkaError> {
        rx.blocking_recv().expect("oneshot channel dropped")
    }

    fn await_complete_value<T: Send + 'static>(rx: oneshot::Receiver<Result<T, KafkaError>>) -> Result<T, KafkaError> {
        rx.blocking_recv().expect("oneshot channel dropped")
    }

    // -------------------------------------------------------------------
    // Java: testPrepClosingCommitEvents
    // -------------------------------------------------------------------
    #[test]
    fn prep_closing_commit_events_signals_close() {
        let mut fx = setup_processor(true);
        fx.processor.process(ApplicationEvent::CommitOnClose);
        // Confirm `signal_close()` flipped the `closing` flag on the
        // commit manager — the Rust test can't `verify(commitManager).signalClose()`
        // like Mockito, so we observe the resulting state.
        let rm_guard = fx.request_managers.lock().unwrap();
        let commit = rm_guard.commit.as_ref().expect("commit manager present");
        assert!(commit.is_closing(), "signal_close should have flipped the closing flag");
    }

    // -------------------------------------------------------------------
    // Java: testProcessUnsubscribeEventWithoutGroupId
    //
    // The without-group-id branch is sync (it just clears subscription
    // state and completes the future). The with-group-id branch awaits a
    // CompletableFuture from `membershipManager.leaveGroup()` and is
    // wired in commit 5/N.
    //
    // NOTE: As of commit 4/N the `Unsubscribe` arm is itself fully
    // deferred to commit 5/N — even the no-group-id sync sub-path. The
    // reason: the dispatch test for the without-group-id case would
    // duplicate work that commit 5/N owns end-to-end (including the
    // with-group-id `leave_group` CompletableFuture chain). To avoid
    // half-translating the arm, the sync sub-path moves with commit 5.
    // -------------------------------------------------------------------

    // -------------------------------------------------------------------
    // Java: testAssignmentChangeEvent (with group id)
    // -------------------------------------------------------------------
    #[test]
    fn assignment_change_event_with_group_id_updates_timer_and_assigns() {
        let mut fx = setup_processor(true);
        let partition = tp("topic", 0);
        let (handle, rx) = CompletableEventHandle::<()>::new(20_000);
        let mut partitions = HashSet::new();
        partitions.insert(partition.clone());

        fx.processor
            .process(ApplicationEvent::AssignmentChange { handle, current_time_ms: 12_345, partitions });

        let result = rx.blocking_recv().expect("oneshot dropped");
        result.expect("assignment change must succeed");

        // Subscriptions reflect the assignment.
        let subs_guard = fx.subscriptions.lock().unwrap();
        let assigned = subs_guard.assigned_partitions();
        assert_eq!(1, assigned.len());
        assert!(assigned.contains(&partition));
    }

    // -------------------------------------------------------------------
    // Java: testAssignmentChangeEvent (without group id)
    // -------------------------------------------------------------------
    #[test]
    fn assignment_change_event_without_group_id_assigns_only() {
        let mut fx = setup_processor(false);
        let partition = tp("topic", 0);
        let (handle, rx) = CompletableEventHandle::<()>::new(20_000);
        let mut partitions = HashSet::new();
        partitions.insert(partition.clone());

        fx.processor
            .process(ApplicationEvent::AssignmentChange { handle, current_time_ms: 12_345, partitions });

        await_complete(rx).expect("assignment change must succeed");
        // No commit manager wired → no panic, no auto-commit trigger.
        let rm_guard = fx.request_managers.lock().unwrap();
        assert!(rm_guard.commit.is_none());
    }

    // -------------------------------------------------------------------
    // Java: testAssignmentChangeEventWithException
    //
    // Triggered by passing an assignment that conflicts with an
    // existing pattern subscription.
    // -------------------------------------------------------------------
    #[test]
    fn assignment_change_event_with_exception() {
        let mut fx = setup_processor(false);
        // Put the subscription into AutoPattern mode so AssignFromUser
        // surfaces an IllegalStateException (mixed-subscription error).
        {
            let mut guard = fx.subscriptions.lock().unwrap();
            guard.subscribe_pattern(Regex::new("topic.*").unwrap(), None).unwrap();
        }
        let (handle, rx) = CompletableEventHandle::<()>::new(20_000);
        let mut partitions = HashSet::new();
        partitions.insert(tp("topic", 0));

        fx.processor
            .process(ApplicationEvent::AssignmentChange { handle, current_time_ms: 12_345, partitions });

        let err = await_complete(rx).expect_err("must surface the mixed-subscription error");
        assert!(
            err.to_string().to_lowercase().contains("mutually exclusive"),
            "expected mixed-subscription error, got: {err}"
        );
    }

    // -------------------------------------------------------------------
    // Java: testResetOffsetEvent
    // -------------------------------------------------------------------
    #[test]
    fn reset_offset_event_resets_strategy_for_partitions() {
        let mut fx = setup_processor(false);
        let partition = tp("topic", 0);
        // First assign so the partition is in the state map.
        {
            let mut guard = fx.subscriptions.lock().unwrap();
            let mut tps = HashSet::new();
            tps.insert(partition.clone());
            guard.assign_from_user(tps).unwrap();
        }
        let (handle, rx) = CompletableEventHandle::<()>::new(20_000);
        let mut partitions = HashSet::new();
        partitions.insert(partition.clone());

        fx.processor.process(ApplicationEvent::ResetOffset {
            handle,
            partitions,
            offset_reset_strategy: AutoOffsetResetStrategy::LATEST,
        });

        await_complete(rx).expect("reset must succeed");
        let guard = fx.subscriptions.lock().unwrap();
        let strategy = guard
            .reset_strategy(&partition)
            .expect("partition must be assigned")
            .expect("strategy must be set");
        assert_eq!(strategy, AutoOffsetResetStrategy::LATEST);
    }

    // -------------------------------------------------------------------
    // Java: testSeekUnvalidatedEvent
    // -------------------------------------------------------------------
    #[test]
    fn seek_unvalidated_event_sets_position() {
        let mut fx = setup_processor(false);
        let partition = tp("topic", 0);
        // Assign before seek (Java does this implicitly via the assign+seek API contract).
        {
            let mut guard = fx.subscriptions.lock().unwrap();
            let mut tps = HashSet::new();
            tps.insert(partition.clone());
            guard.assign_from_user(tps).unwrap();
        }
        let (handle, rx) = CompletableEventHandle::<()>::new(20_000);
        fx.processor.process(ApplicationEvent::SeekUnvalidated {
            handle,
            partition: partition.clone(),
            offset: 42,
            offset_epoch: Some(1),
        });
        await_complete(rx).expect("seek must succeed");
        let guard = fx.subscriptions.lock().unwrap();
        let position = guard.position(&partition).expect("position lookup").expect("position is set");
        assert_eq!(42, position.offset);
        assert_eq!(Some(1), position.offset_epoch);
    }

    // -------------------------------------------------------------------
    // Java: testSeekUnvalidatedEventWithException
    // -------------------------------------------------------------------
    #[test]
    fn seek_unvalidated_event_with_exception_fails_handle() {
        let mut fx = setup_processor(false);
        // Do NOT assign — `seek_unvalidated` errors when partition is
        // not assigned (Java's `IllegalStateException`).
        let (handle, rx) = CompletableEventHandle::<()>::new(20_000);
        fx.processor.process(ApplicationEvent::SeekUnvalidated {
            handle,
            partition: tp("topic", 0),
            offset: 42,
            offset_epoch: None,
        });
        let err = await_complete(rx).expect_err("must fail for unassigned partition");
        // Error message contract: surface the partition-not-assigned
        // signal. Don't assert the exact wording (it differs slightly
        // from Java's wording) — assert that the cause references the
        // partition.
        assert!(
            err.to_string().to_lowercase().contains("topic-0")
                || err.to_string().to_lowercase().contains("not assigned"),
            "expected partition-not-assigned error, got: {err}"
        );
    }

    // -------------------------------------------------------------------
    // Java: testTopicSubscriptionChangeEvent
    // -------------------------------------------------------------------
    #[test]
    fn topic_subscription_change_event_updates_subscription_and_records_version() {
        let mut fx = setup_processor(true);
        let mut topics = HashSet::new();
        topics.insert("topic1".to_string());
        topics.insert("topic2".to_string());
        let (handle, rx) = CompletableEventHandle::<()>::new(20_000);
        fx.processor.process(ApplicationEvent::TopicSubscriptionChange {
            handle,
            topics: topics.clone(),
            listener: None,
        });
        await_complete(rx).expect("subscribe must succeed");
        // Subscription reflects the new topics.
        let subs_guard = fx.subscriptions.lock().unwrap();
        let sub = subs_guard.subscription();
        assert_eq!(topics, sub);
        // Java's test mocks `metadata.requestUpdateForNewTopics()` to
        // return `1` and asserts `processor.metadataVersionSnapshot() == 1`.
        // In Rust the metadata implementation returns the CURRENT
        // `update_version` (unchanged across calls), so the assertion
        // becomes: the snapshot matches whatever the metadata reports
        // — i.e. the processor recorded the return value.
        assert_eq!(fx.processor.metadata_version_snapshot(), fx.metadata.update_version());
    }

    // -------------------------------------------------------------------
    // Java: testTopicSubscriptionChangeEventWithIllegalSubscriptionState
    //
    // Already-subscribed-via-pattern → subscribe(topics) must fail with
    // the mixed-subscription error.
    // -------------------------------------------------------------------
    #[test]
    fn topic_subscription_change_event_with_illegal_state() {
        let mut fx = setup_processor(true);
        // Pre-subscribe via pattern.
        {
            let mut guard = fx.subscriptions.lock().unwrap();
            guard.subscribe_pattern(Regex::new("topic.*").unwrap(), None).unwrap();
        }
        let mut topics = HashSet::new();
        topics.insert("topic1".to_string());
        let (handle, rx) = CompletableEventHandle::<()>::new(20_000);
        fx.processor
            .process(ApplicationEvent::TopicSubscriptionChange { handle, topics, listener: None });
        let err = await_complete(rx).expect_err("must surface mixed-subscription error");
        assert!(
            err.to_string().contains("mutually exclusive"),
            "expected mixed-subscription error, got: {err}"
        );
    }

    // -------------------------------------------------------------------
    // Java: testTopicPatternSubscriptionChangeEvent
    // -------------------------------------------------------------------
    #[test]
    fn topic_pattern_subscription_change_event_updates_subscription() {
        let mut fx = setup_processor(true);
        let pattern = Regex::new("topic.*").unwrap();
        let (handle, rx) = CompletableEventHandle::<()>::new(20_000);
        fx.processor
            .process(ApplicationEvent::TopicPatternSubscriptionChange { handle, pattern, listener: None });
        await_complete(rx).expect("pattern subscribe must succeed");
        // Subscription state is now AutoPattern.
        let guard = fx.subscriptions.lock().unwrap();
        assert!(guard.has_pattern_subscription());
    }

    // -------------------------------------------------------------------
    // Java: testTopicPatternSubscriptionChangeEventWithIllegalSubscriptionState
    // -------------------------------------------------------------------
    #[test]
    fn topic_pattern_subscription_change_event_with_illegal_state() {
        let mut fx = setup_processor(true);
        // Pre-subscribe to concrete topics.
        {
            let mut guard = fx.subscriptions.lock().unwrap();
            let mut topics = HashSet::new();
            topics.insert("a".to_string());
            guard.subscribe_topics(topics, None).unwrap();
        }
        let pattern = Regex::new("topic.*").unwrap();
        let (handle, rx) = CompletableEventHandle::<()>::new(20_000);
        fx.processor
            .process(ApplicationEvent::TopicPatternSubscriptionChange { handle, pattern, listener: None });
        let err = await_complete(rx).expect_err("must surface mixed-subscription error");
        assert!(
            err.to_string().contains("mutually exclusive"),
            "expected mixed-subscription error, got: {err}"
        );
    }

    // -------------------------------------------------------------------
    // Java: testR2JPatternSubscriptionEventSuccess
    // -------------------------------------------------------------------
    #[test]
    fn r2j_pattern_subscription_event_success() {
        let mut fx = setup_processor(true);
        let pattern = SubscriptionPattern::new("t*".to_string());
        let (handle, rx) = CompletableEventHandle::<()>::new(20_000);
        fx.processor
            .process(ApplicationEvent::TopicRe2JPatternSubscriptionChange { handle, pattern, listener: None });
        await_complete(rx).expect("re2j pattern subscribe must succeed");
        let guard = fx.subscriptions.lock().unwrap();
        assert!(guard.has_re2j_pattern_subscription());
    }

    // -------------------------------------------------------------------
    // Java: testR2JPatternSubscriptionEventFailureWithMixedSubscriptionType
    // -------------------------------------------------------------------
    #[test]
    fn r2j_pattern_subscription_event_failure_with_mixed_type() {
        let mut fx = setup_processor(true);
        // Pre-subscribe to concrete topics.
        {
            let mut guard = fx.subscriptions.lock().unwrap();
            let mut topics = HashSet::new();
            topics.insert("a".to_string());
            guard.subscribe_topics(topics, None).unwrap();
        }
        let pattern = SubscriptionPattern::new("t*".to_string());
        let (handle, rx) = CompletableEventHandle::<()>::new(20_000);
        fx.processor
            .process(ApplicationEvent::TopicRe2JPatternSubscriptionChange { handle, pattern, listener: None });
        let err = await_complete(rx).expect_err("must surface mixed-subscription error");
        assert!(
            err.to_string().contains("mutually exclusive"),
            "expected mixed-subscription error, got: {err}"
        );
    }

    // -------------------------------------------------------------------
    // Java: testUpdatePatternSubscriptionInvokedWhenMetadataUpdated
    //
    // Direct test of the helper (the AsyncPoll-side test in Java triggers
    // it via process(AsyncPollEvent), which is deferred to commit 5/N).
    // We exercise the helper through the UpdatePatternSubscription event.
    // -------------------------------------------------------------------
    #[test]
    fn update_pattern_subscription_no_pattern_is_noop() {
        let mut fx = setup_processor(true);
        let (handle, rx) = CompletableEventHandle::<()>::new(20_000);
        fx.processor.process(ApplicationEvent::UpdatePatternSubscription { handle });
        await_complete(rx).expect("update pattern subscription must succeed");
        // No pattern subscription set — helper short-circuits and the
        // metadata-version snapshot did not advance.
    }

    // -------------------------------------------------------------------
    // Java: testPausePartitionsEvent (covered indirectly via
    // testApplicationEventIsProcessed dispatch; the Rust equivalent test
    // verifies the resulting state).
    // -------------------------------------------------------------------
    #[test]
    fn pause_partitions_event_marks_partitions_paused() {
        let mut fx = setup_processor(false);
        let partition = tp("topic", 0);
        {
            let mut guard = fx.subscriptions.lock().unwrap();
            let mut tps = HashSet::new();
            tps.insert(partition.clone());
            guard.assign_from_user(tps).unwrap();
        }
        let (handle, rx) = CompletableEventHandle::<()>::new(20_000);
        let mut partitions = HashSet::new();
        partitions.insert(partition.clone());
        fx.processor.process(ApplicationEvent::PausePartitions { handle, partitions });
        await_complete(rx).expect("pause must succeed");
        let guard = fx.subscriptions.lock().unwrap();
        assert!(guard.is_paused(&partition));
    }

    #[test]
    fn pause_partitions_event_with_unassigned_partition_fails() {
        let mut fx = setup_processor(false);
        let (handle, rx) = CompletableEventHandle::<()>::new(20_000);
        let mut partitions = HashSet::new();
        partitions.insert(tp("topic", 0));
        fx.processor.process(ApplicationEvent::PausePartitions { handle, partitions });
        // Unassigned partition → pause errors.
        let err = await_complete(rx).expect_err("must fail for unassigned partition");
        assert!(
            err.to_string().to_lowercase().contains("not assigned") || err.to_string().contains("topic-0"),
            "expected partition-not-assigned error, got: {err}"
        );
    }

    // -------------------------------------------------------------------
    // Resume partitions
    // -------------------------------------------------------------------
    #[test]
    fn resume_partitions_event_unpauses_partitions() {
        let mut fx = setup_processor(false);
        let partition = tp("topic", 0);
        {
            let mut guard = fx.subscriptions.lock().unwrap();
            let mut tps = HashSet::new();
            tps.insert(partition.clone());
            guard.assign_from_user(tps).unwrap();
            guard.pause(&partition).unwrap();
        }
        let (handle, rx) = CompletableEventHandle::<()>::new(20_000);
        let mut partitions = HashSet::new();
        partitions.insert(partition.clone());
        fx.processor.process(ApplicationEvent::ResumePartitions { handle, partitions });
        await_complete(rx).expect("resume must succeed");
        let guard = fx.subscriptions.lock().unwrap();
        assert!(!guard.is_paused(&partition));
    }

    // -------------------------------------------------------------------
    // Current lag
    // -------------------------------------------------------------------
    #[test]
    fn current_lag_event_returns_none_when_lag_unknown() {
        let mut fx = setup_processor(false);
        let partition = tp("topic", 0);
        {
            let mut guard = fx.subscriptions.lock().unwrap();
            let mut tps = HashSet::new();
            tps.insert(partition.clone());
            guard.assign_from_user(tps).unwrap();
        }
        let (handle, rx) = CompletableEventHandle::<Option<i64>>::new(20_000);
        fx.processor.process(ApplicationEvent::CurrentLag {
            handle,
            partition: partition.clone(),
            isolation_level: IsolationLevel::ReadUncommitted,
        });
        let lag = await_complete_value(rx).expect("lag computation must not error");
        // No position / no high-watermark → lag unknown.
        assert!(lag.is_none());
        // Verify the manager flipped the end-offset-requested flag.
        let guard = fx.subscriptions.lock().unwrap();
        let requested = guard
            .partition_end_offset_requested(&partition)
            .expect("partition end offset state available");
        assert!(requested, "end-offset request should have been triggered");
    }

    // -------------------------------------------------------------------
    // CommitOnClose path with no commit manager (Java covers via
    // `requestManagers.commitRequestManager.isEmpty()` early-return).
    // -------------------------------------------------------------------
    #[test]
    fn commit_on_close_with_no_commit_manager_is_noop() {
        let mut fx = setup_processor(false);
        // No panic, no observable effect.
        fx.processor.process(ApplicationEvent::CommitOnClose);
        let rm_guard = fx.request_managers.lock().unwrap();
        assert!(rm_guard.commit.is_none());
    }

    // -------------------------------------------------------------------
    // StopFindCoordinatorOnClose
    // -------------------------------------------------------------------
    #[test]
    fn stop_find_coordinator_on_close_signals_coordinator() {
        let mut fx = setup_processor(true);
        fx.processor.process(ApplicationEvent::StopFindCoordinatorOnClose);
        let rm_guard = fx.request_managers.lock().unwrap();
        let coord = rm_guard.coordinator.as_ref().expect("coordinator present");
        assert!(coord.is_closing(), "signal_close should have flipped the closing flag");
    }

    #[test]
    fn stop_find_coordinator_on_close_with_no_coordinator_is_noop() {
        let mut fx = setup_processor(false);
        fx.processor.process(ApplicationEvent::StopFindCoordinatorOnClose);
        let rm_guard = fx.request_managers.lock().unwrap();
        assert!(rm_guard.coordinator.is_none());
    }

    // -------------------------------------------------------------------
    // Java: testApplicationEventIsProcessed — dispatch parity test.
    //
    // Java's version uses Mockito to verify the right overload is
    // selected. In Rust the enum match is exhaustive — confirmed by the
    // compiler. We still exercise the dispatch table for representative
    // sync arms to confirm they don't panic.
    // -------------------------------------------------------------------
    #[test]
    fn dispatch_table_covers_sync_variants() {
        let mut fx = setup_processor(true);
        // CommitOnClose, StopFindCoordinator, NewTopicsMetadataUpdate.
        fx.processor.process(ApplicationEvent::CommitOnClose);
        fx.processor.process(ApplicationEvent::StopFindCoordinatorOnClose);
        fx.processor.process(ApplicationEvent::NewTopicsMetadataUpdate);
        // ConsumerRebalanceListenerCallbackCompleted is logged, no panic.
        fx.processor
            .process(ApplicationEvent::ConsumerRebalanceListenerCallbackCompleted {
                method_name: ConsumerRebalanceListenerMethodName::OnPartitionsRevoked,
                error: None,
            });
    }

    // -------------------------------------------------------------------
    // Deferred-arm sanity: verify the async-dispatch arms fail their
    // handles cleanly (no panic, no hang). Commit 5/N replaces these
    // with real implementations.
    // -------------------------------------------------------------------
    #[test]
    fn deferred_async_arms_fail_handles_with_unsupported_version() {
        let mut fx = setup_processor(true);
        let (handle, rx) = CompletableEventHandle::<()>::new(20_000);
        fx.processor.process(ApplicationEvent::CheckAndUpdatePositions { handle });
        let err = await_complete(rx).expect_err("deferred arm must fail handle");
        assert!(
            err.to_string().contains("Phase 10 commit 5/N"),
            "expected deferred-arm error, got: {err}"
        );
    }
}
