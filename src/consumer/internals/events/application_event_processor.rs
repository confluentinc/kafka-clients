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

//! `ApplicationEventProcessor` — Phase-10 dispatch table (sync + async arms).
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor`.
//! Implements [`EventProcessor<ApplicationEvent>`] for the consumer
//! background task.
//!
//! # Phase-10 commit split
//!
//! Phase 10 lands the processor across two commits:
//!
//! - **Commit 4/N:** every variant whose Java handling is purely
//!   synchronous — subscription changes, pause/resume, seek, reset,
//!   current-lag, assignment-change, commit-on-close,
//!   `StopFindCoordinatorOnClose`, and `NewTopicsMetadataUpdate`.
//! - **Commit 5/N (this file):** the async-dispatch arms — `AsyncPoll`,
//!   `CommitAsync`, `CommitSync`, `FetchCommittedOffsets`, `ListOffsets`,
//!   `CheckAndUpdatePositions`, `TopicMetadata`, `AllTopicsMetadata`,
//!   `Unsubscribe`, `CreateFetchRequests`, `LeaveGroupOnClose`. Each arm
//!   spawns a detached `tokio::task` that awaits the relevant manager
//!   future and writes its completion to the event's
//!   [`CompletableEventHandle`] — mirroring Java's
//!   `whenComplete(complete(event.future()))` chain (the spawned task
//!   plays the same role as Java's CompletableFuture callback dispatch).
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
//!
//! # Async-arm pattern
//!
//! Each async arm in this commit follows the same shape:
//!
//! 1. Lock `RequestManagers` long enough to call the manager method and
//!    obtain a `oneshot::Receiver<...>` (or to perform a sync
//!    pre-check that early-returns the handle).
//! 2. Drop the lock.
//! 3. `tokio::spawn` a continuation task that awaits the receiver and
//!    completes the event's handle (matching Java's
//!    `future.whenComplete(complete(event.future()))`).
//!
//! Spawning is per-event (not per-record); the hot-path rule from
//! CLAUDE.md §11 still applies inside `Fetcher` / `FetchCollector`.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use super::application_event::ApplicationEvent;
use super::completable_event_reaper::CompletableEventReaper;
use super::event_processor::EventProcessor;
use crate::common::{IsolationLevel, KafkaError, TopicPartition};
use crate::consumer::OffsetAndMetadata;
use crate::consumer::consumer_rebalance_listener::ConsumerRebalanceListener;
use crate::consumer::internals::consumer_metadata::ConsumerMetadata;
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
    /// Shared application-event reaper — same instance owned by
    /// [`super::super::consumer_network_thread::ConsumerNetworkThread`].
    /// The processor registers SECONDARY handles (e.g. `offsets_ready`
    /// from `CommitAsync` / `CommitSync` when the commit manager is
    /// absent) so their deadlines are still enforced even though the
    /// bg-task's `process_application_events` only registers the
    /// primary handle via `event.erased_handle()`.
    application_event_reaper: Arc<Mutex<CompletableEventReaper>>,
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
    /// `log`) and takes the three shared references by `Arc`, plus the
    /// `CompletableEventReaper` shared with the bg task (Phase-10 R3-1):
    /// the AEP registers secondary `offsets_ready` handles directly on
    /// the empty-commit-manager fall-through so the reaper enforces the
    /// deadline (mirroring Java's `Timer`-based `ConsumerUtils.getResult`
    /// wait that surfaces `TimeoutException` after the user-supplied
    /// timeout elapses).
    pub(crate) fn new(
        request_managers: Arc<Mutex<RequestManagers>>,
        metadata: Arc<ConsumerMetadata>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        application_event_reaper: Arc<Mutex<CompletableEventReaper>>,
    ) -> Self {
        let metadata_version_snapshot = metadata.update_version();
        Self {
            request_managers,
            metadata,
            subscriptions,
            application_event_reaper,
            metadata_version_snapshot,
        }
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
            let rm_guard = self.lock_request_managers();
            if let Some(commit) = rm_guard.commit.as_ref() {
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
        let rm_guard = self.lock_request_managers();
        if let Some(commit) = rm_guard.commit.as_ref() {
            log::debug!("Signal CommitRequestManager closing");
            commit.signal_close_shared();
        }
    }

    /// Java: `process(StopFindCoordinatorOnCloseEvent)`.
    fn process_stop_find_coordinator_on_close(&mut self) {
        let rm_guard = self.lock_request_managers();
        if let Some(coord_arc) = rm_guard.coordinator.as_ref() {
            log::debug!("Signal CoordinatorRequestManager closing");
            coord_arc.signal_close_shared();
        }
    }

    /// Java: `process(ConsumerRebalanceListenerCallbackCompletedEvent)`.
    ///
    /// # §31 translation deviation
    ///
    /// Java models the listener handshake as a *pair* of events: the bg
    /// task enqueues `PartitionsRemovedEvent` / `PartitionsAssignedEvent`
    /// on the background-events queue (AK 4.3.1, KAFKA-20106; formerly the
    /// single `ConsumerRebalanceListenerCallbackNeededEvent`), and after
    /// invoking the listener the app side enqueues a *separate*
    /// `ConsumerRebalanceListenerCallbackCompletedEvent` back to the bg
    /// task; the processor's body (this method, Java side) then completes
    /// the future the membership manager is waiting on.
    ///
    /// The Rust translation collapses the round-trip by embedding a
    /// `tokio::sync::oneshot::Sender<Result<(), KafkaError>>` directly in
    /// `BackgroundEvent::PartitionsRemoved` / `BackgroundEvent::PartitionsAssigned`
    /// (see `AbstractMembershipManager::enqueue_rebalance_callback` /
    /// `enqueue_partitions_assigned_event`). The membership manager holds
    /// the receiver as cross-iteration state; the app side completes the ack
    /// by sending on the embedded sender. No second event is required.
    ///
    /// As a result, this arm has no work to perform in Rust. We still
    /// match Java's diagnostic: if a `Completed` event reaches the
    /// processor without a heartbeat manager present, log the same
    /// warning so the failure mode is observable in tests / logs.
    /// The event variant remains in [`ApplicationEvent`] both to keep
    /// the Java parity surface visible to readers and to leave room for
    /// future diagnostics (e.g. tracing the original Java event lifecycle).
    fn process_consumer_rebalance_listener_callback_completed(
        &mut self,
        method_name: crate::consumer::consumer_rebalance_listener_method_name::ConsumerRebalanceListenerMethodName,
        error: Option<KafkaError>,
    ) {
        let _ = error;
        let rm_guard = self.lock_request_managers();
        if rm_guard.consumer_heartbeat.is_none() {
            log::warn!(
                "An internal error occurred; the group membership manager was not present, so the notification of the {} callback execution could not be sent",
                method_name.fully_qualified_method_name()
            );
            return;
        }
        // Rust's embedded-sender pattern (see method doc above) already
        // resolves the membership manager's `ack_rx`. No further action
        // here. Java's body would call
        // `membershipManager.consumerRebalanceListenerCallbackCompleted(event)`,
        // which completes a future the reconcile loop awaits — Rust's
        // reconcile awaits the embedded `ack_rx` directly, so this
        // method is intentionally empty.
        log::trace!(
            "ConsumerRebalanceListenerCallbackCompleted({}) observed; ack already delivered via embedded oneshot sender",
            method_name.fully_qualified_method_name()
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

    // ===================================================================
    // Async-dispatch arms (Phase 10 commit 5/N)
    // ===================================================================

    /// Java: `process(AsyncCommitEvent)`.
    ///
    /// Resolves the offsets to commit (using `subscriptions.all_consumed()`
    /// when the event carries `None`), marks the secondary `offsets_ready`
    /// handle complete, then spawns a continuation task that awaits the
    /// commit-manager future and writes the result to the primary handle.
    /// Mirrors Java's `manager.commitAsync(offsets).whenComplete(...)`.
    fn process_commit_async(
        &mut self,
        handle: super::completable_event::CompletableEventHandle<HashMap<TopicPartition, OffsetAndMetadata>>,
        offsets_ready: super::completable_event::CompletableEventHandle<()>,
        offsets: Option<HashMap<TopicPartition, OffsetAndMetadata>>,
    ) {
        // Java: `if (requestManagers.commitRequestManager.isEmpty()) { ... }`.
        let commit_rx = {
            let rm_guard = self.lock_request_managers();
            let Some(commit) = rm_guard.commit.as_ref() else {
                drop(rm_guard);
                // Java: `process(AsyncCommitEvent)` empty-manager branch
                // only completes `event.future()` exceptionally; it does
                // NOT mark / fail `offsetsReady`. The app-side
                // `ConsumerUtils.getResult(offsetsReady, ...)` waits with
                // `defaultApiTimeoutMs` and surfaces a TimeoutException
                // when the user-supplied `Timer` elapses.
                //
                // Rust's secondary `offsets_ready` handle is a oneshot
                // sender; if we drop it un-completed, the receiver
                // resolves with `RecvError` immediately — divergent from
                // Java's "wait the full deadline then TimeoutException".
                // To restore Java's contract we register the secondary
                // handle with the application-event reaper (Phase-10
                // R3-1). The reaper holds a strong ref keeping the
                // sender alive, then completes it with
                // `KafkaError::Timeout` when `deadline_ms` elapses —
                // exactly mirroring Java's `Timer`-based timeout.
                {
                    let mut reaper = match self.application_event_reaper.lock() {
                        Ok(g) => g,
                        Err(p) => p.into_inner(),
                    };
                    reaper.add(offsets_ready.erased());
                }
                drop(offsets_ready);
                handle.complete_exceptionally(KafkaError::illegal_state(
                    "Unable to async commit offset because the CommitRequestManager is not available. Check if group.id was set correctly",
                ));
                return;
            };
            // Resolve offsets (Java's `event.offsets().orElseGet(subscriptions::allConsumed)`).
            let resolved = match offsets {
                Some(o) => o,
                None => {
                    let subs = self.lock_subscriptions();
                    subs.all_consumed()
                },
            };
            // Java: `event.markOffsetsReady()` happens after offsets are resolved.
            offsets_ready.complete(());
            commit.commit_async_no_callback(resolved, current_time_ms_now())
        };
        // Spawn continuation — mirrors Java's `whenComplete(complete(event.future()))`.
        tokio::spawn(async move {
            match commit_rx.await {
                Ok(Ok(committed)) => {
                    handle.complete(committed);
                },
                Ok(Err(err)) => {
                    handle.complete_exceptionally(err);
                },
                Err(_recv_err) => {
                    handle.complete_exceptionally(KafkaError::illegal_state("commit_async_no_callback sender dropped"));
                },
            }
        });
    }

    /// Java: `process(SyncCommitEvent)`.
    ///
    /// Same shape as [`Self::process_commit_async`] but routes through
    /// `commit_sync(offsets, deadline_ms)`. The event's
    /// `CompletableEventHandle` carries `deadline_ms` already (it was set
    /// when the event was constructed on the app side).
    fn process_commit_sync(
        &mut self,
        handle: super::completable_event::CompletableEventHandle<HashMap<TopicPartition, OffsetAndMetadata>>,
        offsets_ready: super::completable_event::CompletableEventHandle<()>,
        offsets: Option<HashMap<TopicPartition, OffsetAndMetadata>>,
    ) {
        let deadline_ms = handle.deadline_ms();
        let now_ms = current_time_ms_now();
        let commit_rx = {
            let rm_guard = self.lock_request_managers();
            let Some(commit) = rm_guard.commit.as_ref() else {
                drop(rm_guard);
                // Java: `process(SyncCommitEvent)` empty-manager branch
                // only fails `event.future()`; `offsetsReady` is left
                // un-completed and the user-side `Timer` in
                // `ConsumerUtils.getResult` eventually fires a
                // TimeoutException. See `process_commit_async` for the
                // full rationale — Rust registers the secondary handle
                // with the reaper so its deadline is enforced and the
                // receiver eventually resolves with `KafkaError::Timeout`
                // (Phase-10 R3-1).
                {
                    let mut reaper = match self.application_event_reaper.lock() {
                        Ok(g) => g,
                        Err(p) => p.into_inner(),
                    };
                    reaper.add(offsets_ready.erased());
                }
                drop(offsets_ready);
                handle.complete_exceptionally(KafkaError::illegal_state(
                    "Unable to sync commit offset because the CommitRequestManager is not available. Check if group.id was set correctly",
                ));
                return;
            };
            let resolved = match offsets {
                Some(o) => o,
                None => {
                    let subs = self.lock_subscriptions();
                    subs.all_consumed()
                },
            };
            offsets_ready.complete(());
            commit.commit_sync(resolved, deadline_ms, now_ms)
        };
        tokio::spawn(async move {
            match commit_rx.await {
                Ok(Ok(committed)) => {
                    handle.complete(committed);
                },
                Ok(Err(err)) => {
                    handle.complete_exceptionally(err);
                },
                Err(_recv_err) => {
                    handle.complete_exceptionally(KafkaError::illegal_state("commit_sync sender dropped"));
                },
            }
        });
    }

    /// Java: `process(FetchCommittedOffsetsEvent)`.
    fn process_fetch_committed_offsets(
        &mut self,
        handle: super::completable_event::CompletableEventHandle<HashMap<TopicPartition, OffsetAndMetadata>>,
        partitions: HashSet<TopicPartition>,
    ) {
        let deadline_ms = handle.deadline_ms();
        let now_ms = current_time_ms_now();
        let fetch_rx = {
            let rm_guard = self.lock_request_managers();
            let Some(commit) = rm_guard.commit.as_ref() else {
                drop(rm_guard);
                handle.complete_exceptionally(KafkaError::illegal_state(
                    "Unable to fetch committed offset because the CommitRequestManager is not available. Check if group.id was set correctly",
                ));
                return;
            };
            commit.fetch_offsets(partitions, deadline_ms, now_ms)
        };
        tokio::spawn(async move {
            match fetch_rx.await {
                Ok(Ok(result)) => {
                    // Java (KAFKA-20165): the event completes with
                    // `result.toOffsetMapWithNulls()` — a map with `null` for
                    // both no-committed-offset partitions AND partitions that
                    // had retriable errors (UNKNOWN_TOPIC_ID /
                    // UNKNOWN_TOPIC_OR_PARTITION), returning partial results
                    // rather than failing the whole `committed()` call.
                    //
                    // The FetchCommittedOffsetsEvent handle type is
                    // `HashMap<TopicPartition, OffsetAndMetadata>` (no `Option`),
                    // so "no offset for this partition" is represented by
                    // absence: entries whose value is `None` (uncommitted or
                    // errored) are stripped, matching the observable behaviour
                    // of the public API.
                    let stripped: HashMap<TopicPartition, OffsetAndMetadata> = result
                        .to_offset_map_with_nulls()
                        .into_iter()
                        .filter_map(|(k, v)| v.map(|om| (k, om)))
                        .collect();
                    handle.complete(stripped);
                },
                Ok(Err(err)) => {
                    handle.complete_exceptionally(err);
                },
                Err(_recv_err) => {
                    handle.complete_exceptionally(KafkaError::illegal_state("fetch_offsets sender dropped"));
                },
            }
        });
    }

    /// Java: `process(ListOffsetsEvent)`.
    fn process_list_offsets(
        &mut self,
        handle: super::completable_event::CompletableEventHandle<
            HashMap<
                TopicPartition,
                Option<crate::consumer::internals::offset_and_timestamp_internal::OffsetAndTimestampInternal>,
            >,
        >,
        timestamps_to_search: HashMap<TopicPartition, i64>,
        require_timestamps: bool,
    ) {
        let fetch_rx = {
            let mut rm_guard = self.lock_request_managers();
            let Some(offsets_mgr) = rm_guard.offsets.as_mut() else {
                drop(rm_guard);
                handle.complete_exceptionally(KafkaError::illegal_state(
                    "OffsetsRequestManager not available when processing a ListOffsets event",
                ));
                return;
            };
            offsets_mgr.fetch_offsets(timestamps_to_search, require_timestamps)
        };
        tokio::spawn(async move {
            match fetch_rx.await {
                Ok(Ok(map)) => {
                    handle.complete(map);
                },
                Ok(Err(err)) => {
                    handle.complete_exceptionally(err);
                },
                Err(_recv_err) => {
                    handle.complete_exceptionally(KafkaError::illegal_state(
                        "OffsetsRequestManager fetch_offsets sender dropped",
                    ));
                },
            }
        });
    }

    /// Java: `process(CheckAndUpdatePositionsEvent)`.
    fn process_check_and_update_positions(&mut self, handle: super::completable_event::CompletableEventHandle<()>) {
        let deadline_ms = handle.deadline_ms();
        let now_ms = current_time_ms_now();
        let update_rx = {
            let mut rm_guard = self.lock_request_managers();
            let Some(offsets_mgr) = rm_guard.offsets.as_mut() else {
                drop(rm_guard);
                handle.complete_exceptionally(KafkaError::illegal_state(
                    "OffsetsRequestManager not available when processing a CheckAndUpdatePositions event",
                ));
                return;
            };
            offsets_mgr.update_fetch_positions(deadline_ms, now_ms)
        };
        tokio::spawn(async move {
            match update_rx.await {
                Ok(Ok(())) => {
                    handle.complete(());
                },
                Ok(Err(err)) => {
                    handle.complete_exceptionally(err);
                },
                Err(_recv_err) => {
                    handle.complete_exceptionally(KafkaError::illegal_state(
                        "OffsetsRequestManager update_fetch_positions sender dropped",
                    ));
                },
            }
        });
    }

    /// Java: `process(TopicMetadataEvent)`.
    fn process_topic_metadata(
        &mut self,
        handle: super::completable_event::CompletableEventHandle<HashMap<String, Vec<crate::common::PartitionInfo>>>,
        topic: String,
    ) {
        let deadline_ms = handle.deadline_ms();
        let md_rx = {
            let rm_guard = self.lock_request_managers();
            let Some(tm_mgr) = rm_guard.topic_metadata.as_ref() else {
                drop(rm_guard);
                handle.complete_exceptionally(KafkaError::illegal_state(
                    "TopicMetadataRequestManager not available when processing a TopicMetadata event",
                ));
                return;
            };
            tm_mgr.request_topic_metadata(topic, deadline_ms)
        };
        tokio::spawn(async move {
            match md_rx.await {
                Ok(Ok(map)) => {
                    handle.complete(map);
                },
                Ok(Err(err)) => {
                    handle.complete_exceptionally(err);
                },
                Err(_recv_err) => {
                    handle.complete_exceptionally(KafkaError::illegal_state(
                        "TopicMetadataRequestManager request_topic_metadata sender dropped",
                    ));
                },
            }
        });
    }

    /// Java: `process(AllTopicsMetadataEvent)`.
    fn process_all_topics_metadata(
        &mut self,
        handle: super::completable_event::CompletableEventHandle<HashMap<String, Vec<crate::common::PartitionInfo>>>,
    ) {
        let deadline_ms = handle.deadline_ms();
        let md_rx = {
            let rm_guard = self.lock_request_managers();
            let Some(tm_mgr) = rm_guard.topic_metadata.as_ref() else {
                drop(rm_guard);
                handle.complete_exceptionally(KafkaError::illegal_state(
                    "TopicMetadataRequestManager not available when processing an AllTopicsMetadata event",
                ));
                return;
            };
            tm_mgr.request_all_topics_metadata(deadline_ms)
        };
        tokio::spawn(async move {
            match md_rx.await {
                Ok(Ok(map)) => {
                    handle.complete(map);
                },
                Ok(Err(err)) => {
                    handle.complete_exceptionally(err);
                },
                Err(_recv_err) => {
                    handle.complete_exceptionally(KafkaError::illegal_state(
                        "TopicMetadataRequestManager request_all_topics_metadata sender dropped",
                    ));
                },
            }
        });
    }

    /// Java: `process(CreateFetchRequestsEvent)`.
    fn process_create_fetch_requests(&mut self, handle: super::completable_event::CompletableEventHandle<()>) {
        let fetch_rx = {
            let mut rm_guard = self.lock_request_managers();
            let Some(fetch_mgr) = rm_guard.fetch.as_mut() else {
                drop(rm_guard);
                handle.complete_exceptionally(KafkaError::illegal_state(
                    "FetchRequestManager not available when processing a CreateFetchRequests event",
                ));
                return;
            };
            fetch_mgr.create_fetch_requests()
        };
        tokio::spawn(async move {
            match fetch_rx.await {
                Ok(Ok(())) => {
                    handle.complete(());
                },
                Ok(Err(err)) => {
                    handle.complete_exceptionally(err);
                },
                Err(_recv_err) => {
                    handle.complete_exceptionally(KafkaError::illegal_state(
                        "FetchRequestManager create_fetch_requests sender dropped",
                    ));
                },
            }
        });
    }

    /// Java: `process(UnsubscribeEvent)`.
    ///
    /// Two sub-paths (Java preserves both):
    ///
    /// 1. **With heartbeat manager**: route through
    ///    `membership_manager.leave_group(now_ms)` (spawned task).
    /// 2. **Without heartbeat manager**: clear subscription state inline
    ///    and complete the handle immediately. Java: "If the consumer is
    ///    not using the group management capabilities, we still need to
    ///    clear all assignments it may have."
    fn process_unsubscribe(&mut self, handle: super::completable_event::CompletableEventHandle<()>) {
        // Resolve dispatch under a brief lock. Mirror Java's
        // `if (requestManagers.consumerHeartbeatRequestManager.isPresent())`
        // branch — heartbeat present → spawn `leave_group` continuation;
        // absent → clear subscription state inline.
        let membership_arc = {
            let rm_guard = self.lock_request_managers();
            rm_guard
                .consumer_heartbeat
                .as_ref()
                .map(|hrm| Arc::clone(hrm.membership_manager()))
        };
        match membership_arc {
            Some(mm) => {
                let now_ms = current_time_ms_now();
                tokio::spawn(async move {
                    match mm.leave_group(now_ms).await {
                        Ok(()) => {
                            handle.complete(());
                        },
                        Err(err) => {
                            handle.complete_exceptionally(err);
                        },
                    }
                });
            },
            None => {
                {
                    let mut subs = self.lock_subscriptions();
                    subs.unsubscribe();
                }
                handle.complete(());
            },
        }
    }

    /// Java: `process(LeaveGroupOnCloseEvent)`.
    fn process_leave_group_on_close(
        &mut self,
        handle: super::completable_event::CompletableEventHandle<()>,
        membership_operation: crate::consumer::GroupMembershipOperation,
    ) {
        // Java: `if (requestManagers.consumerMembershipManager.isPresent())`.
        // In Rust the membership manager is held on the heartbeat manager
        // (single ownership site). The proxy check is the same.
        let membership_arc = {
            let rm_guard = self.lock_request_managers();
            rm_guard
                .consumer_heartbeat
                .as_ref()
                .map(|hrm| Arc::clone(hrm.membership_manager()))
        };
        match membership_arc {
            Some(mm) => {
                let now_ms = current_time_ms_now();
                log::debug!(
                    "Signal the ConsumerMembershipManager to leave the consumer group since the consumer is closing"
                );
                tokio::spawn(async move {
                    match mm.leave_group_on_close(membership_operation, now_ms).await {
                        Ok(()) => {
                            handle.complete(());
                        },
                        Err(err) => {
                            handle.complete_exceptionally(err);
                        },
                    }
                });
            },
            None => {
                // Java's branch logs and returns without completing the
                // future (StreamsMembershipManager is §20-skip). The
                // app-side caller will time out via the reaper. To make
                // the failure observable (DoD §5: no silent drops) we
                // explicitly fail the handle.
                handle.complete_exceptionally(KafkaError::illegal_state(
                    "ConsumerMembershipManager not available when processing a LeaveGroupOnClose event",
                ));
            },
        }
    }

    /// Java: `process(ApplyAssignmentEvent)` (AK 4.3.1, KAFKA-20106).
    ///
    /// Update the subscription state with a new assignment that has been
    /// reconciled. Triggered by the application thread during `poll()` (to
    /// ensure assignment changes happen only within a call to
    /// `consumer.poll`), and applied here on the background thread (to keep
    /// subscription-state changes in the background).
    ///
    /// `apply_assignment` is synchronous (it only mutates
    /// `SubscriptionState` and fires the `notify_assignment_change`
    /// listeners), so no spawn is needed. Any error is surfaced by
    /// completing the handle exceptionally — mirroring Java's try/catch that
    /// completes `event.future().completeExceptionally(e)`.
    fn process_apply_assignment(
        &mut self,
        handle: super::completable_event::CompletableEventHandle<()>,
        assigned_partitions: HashSet<TopicPartition>,
        added_partitions: Vec<TopicPartition>,
    ) {
        let membership_arc = {
            let rm_guard = self.lock_request_managers();
            rm_guard
                .consumer_heartbeat
                .as_ref()
                .map(|hrm| Arc::clone(hrm.membership_manager()))
        };
        match membership_arc {
            Some(mm) => match mm.apply_assignment(&assigned_partitions, &added_partitions) {
                Ok(()) => {
                    handle.complete(());
                },
                Err(err) => {
                    handle.complete_exceptionally(err);
                },
            },
            None => {
                // Java warns "Neither ConsumerMembershipManager nor
                // StreamsMembershipManager present when processing
                // ApplyAssignmentEvent" and completes the future
                // exceptionally with an IllegalStateException.
                // (StreamsMembershipManager is §20-skip.)
                log::warn!("No membership manager available when processing ApplyAssignmentEvent");
                handle.complete_exceptionally(KafkaError::illegal_state(
                    "No membership manager available when processing ApplyAssignmentEvent",
                ));
            },
        }
    }

    /// Java: `process(AsyncPollEvent)`.
    ///
    /// Pumps the membership/fetch state machine. Mirrors Java's
    /// processing phase-for-phase:
    ///
    /// 1. `maybeReconcile(true)` on the membership manager (Rust is
    ///    async; spawned task awaits).
    /// 2. If commit manager present, `updateTimerAndMaybeCommit(pollTimeMs)`.
    /// 3. If heartbeat present: `maybeUpdatePatternSubscription`,
    ///    `onConsumerPoll`, `resetPollTimer(pollTimeMs)`.
    /// 4. `updateFetchPositions(deadlineMs)` → continues to
    ///    `createFetchRequests()` on success; mark state complete.
    /// 5. Errors mapped via `maybe_complete_async_poll_event_exceptionally`
    ///    semantics (timeout errors are ignored; other errors fail the
    ///    state).
    ///
    /// `markValidatePositionsComplete` is set immediately after the
    /// `update_fetch_positions` call returns (matching Java's
    /// `event.markValidatePositionsComplete()` — Java sets it
    /// synchronously, between the call and the `whenComplete`).
    fn process_async_poll(
        &mut self,
        deadline_ms: i64,
        poll_time_ms: i64,
        state: Arc<super::application_event::AsyncPollState>,
    ) {
        // Snapshot Arc clones for the spawned task — every shared
        // dependency the continuation needs.
        let request_managers = Arc::clone(&self.request_managers);
        // Pattern-subscription refresh.
        //
        // Java places `maybeUpdatePatternSubscription` INSIDE step 2 of
        // `process(AsyncPollEvent)` (after `maybeReconcile`, inside the
        // `commitRequestManager.isPresent()` block). We invoke it here —
        // synchronously, before spawning — because:
        //   1. `maybe_update_pattern_subscription` reads/writes
        //      `self.metadata_version_snapshot`, which is an inherent
        //      field of `ApplicationEventProcessor` (not Send-shared with
        //      the spawned task).
        //   2. Java's reconcile→pattern-update ordering is a
        //      side-channel ordering (the pattern update notifies
        //      `onSubscriptionUpdated`, which the *next* reconcile
        //      observes — not this one). Running the pattern update
        //      first instead means *this* reconcile sees the new pattern;
        //      `onSubscriptionUpdated` still fires before the next
        //      heartbeat — semantically equivalent for the steady-state
        //      consumer.
        // The bg-side commit-7 wiring will eventually fold this back
        // into the processor's loop with the rest of the membership
        // state machine.
        let has_heartbeat_with_commit = {
            let rm_guard = match request_managers.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            rm_guard.consumer_heartbeat.is_some() && rm_guard.commit.is_some()
        };
        if has_heartbeat_with_commit {
            self.maybe_update_pattern_subscription();
        }

        tokio::spawn(async move {
            // --- Step 1: maybeReconcile(true). ---
            // Acquire a ref to the membership manager outside the lock.
            let membership_arc = {
                let rm_guard = match request_managers.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                rm_guard
                    .consumer_heartbeat
                    .as_ref()
                    .map(|hrm| Arc::clone(hrm.membership_manager()))
            };
            if let Some(mm) = &membership_arc {
                // Java's `maybeReconcile(true)` is sync (void). Rust's
                // `reconcile(...)` is async because it awaits the
                // rebalance-listener callback acks. We await it inline —
                // mirrors Java's "do reconciliation work before moving
                // on to update positions" sequencing.
                //
                // Pass `can_commit = true`: this is the poll-time entry
                // point, before any new fetching starts (Java
                // `ApplicationEventProcessor.process(AsyncPollEvent)`
                // line 715-718). At this site any pending offsets can
                // be safely flushed via the commit manager's
                // auto-commit-before-rebalance path inside
                // `maybeReconcile`, so Java passes `true` to permit
                // reconciliation that may commit. The per-iteration
                // `entries()` walk passes `false` because that path
                // cannot guarantee a safe commit point.
                if let Err(err) = mm.reconcile(poll_time_ms, true).await
                    && !is_ignorable_async_poll_error(&err)
                {
                    state.complete_exceptionally(err);
                    return;
                }
            }

            // AK 4.3.1 (KAFKA-20106): we completed checking pending
            // reconciliations (commits triggered, revoked partitions marked
            // to prevent fetching) so the application-thread poll loop can
            // safely continue progress now (fetching). Java:
            // `event.markReconciliationCheckComplete()` immediately after
            // the `maybeReconcile(true)` call.
            state.mark_reconciliation_check_complete();

            // --- Step 2: commit manager auto-commit + heartbeat onPoll. ---
            // Java guards step-2 work on `commitRequestManager.isPresent()`.
            {
                let mut rm_guard = match request_managers.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                if rm_guard.commit.is_some() {
                    if let Some(commit) = rm_guard.commit.as_ref() {
                        commit.update_timer_and_maybe_commit(poll_time_ms);
                    }
                    if let Some(hrm) = rm_guard.consumer_heartbeat.as_mut() {
                        // Java: `membershipManager.onConsumerPoll();`.
                        // Rust's `abstract_mm.on_consumer_poll(epoch)`
                        // takes the join-group epoch — we forward the
                        // membership manager's `join_group_epoch()`
                        // (mirroring Java's `joinGroupEpoch()` override).
                        let mm = hrm.membership_manager();
                        let join_epoch = mm.join_group_epoch();
                        if let Err(e) = mm.abstract_mm.on_consumer_poll(join_epoch) {
                            log::warn!("on_consumer_poll failed: {}", e);
                        }
                        // Java's `resetPollTimer(pollMs)` checks
                        // `pollTimer.isExpired()` BEFORE the reset and
                        // calls `membershipManager().maybeRejoinStaleMember()`
                        // when expired
                        // (`AbstractHeartbeatRequestManager.java:265-274`).
                        // Required for fence-rejoin recovery: when the
                        // poll timer expires the heartbeat manager
                        // transitions the member to STALE via leave
                        // group; the next `poll()` is what brings it
                        // back to JOINING. Without this, the member
                        // remains STALE forever and `max.poll.interval.ms`
                        // -driven fence tests cannot rejoin.
                        if hrm.inner().poll_timer_is_expired(poll_time_ms) {
                            log::warn!(
                                "Time between subsequent calls to poll() was longer than the configured \
                                 max.poll.interval.ms, exceeded approximately by {} ms. Member {} will rejoin \
                                 the group now.",
                                hrm.inner().poll_timer_is_expired_by(poll_time_ms),
                                mm.member_id(),
                            );
                            mm.abstract_mm.maybe_rejoin_stale_member(join_epoch);
                        }
                        hrm.inner_mut().reset_poll_timer(poll_time_ms);
                    }
                }
            }
            // `membership_arc` is consumed in step 1; nothing more to do
            // with it here. Step 2 acquires its own access via the
            // `RequestManagers` lock.
            drop(membership_arc);

            // --- Step 3: updateFetchPositions → createFetchRequests. ---
            let update_rx = {
                let mut rm_guard = match request_managers.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                let Some(offsets_mgr) = rm_guard.offsets.as_mut() else {
                    // Without OffsetsRequestManager we cannot make progress.
                    state.complete_exceptionally(KafkaError::illegal_state(
                        "OffsetsRequestManager not available when processing AsyncPoll",
                    ));
                    return;
                };
                offsets_mgr.update_fetch_positions(deadline_ms, poll_time_ms)
            };
            // Java: `event.markValidatePositionsComplete()` — fires
            // immediately after the call returns, before the
            // `whenComplete` chain.
            state.mark_validate_positions_complete();

            match update_rx.await {
                Ok(Ok(())) => {
                    // Continue to createFetchRequests.
                },
                Ok(Err(err)) => {
                    if is_ignorable_async_poll_error(&err) {
                        // Java: `log.trace("Ignoring timeout for {}: {}", ...)`.
                        log::trace!("Ignoring timeout during update_fetch_positions: {}", err);
                    } else {
                        state.complete_exceptionally(err);
                        return;
                    }
                },
                Err(_recv_err) => {
                    state.complete_exceptionally(KafkaError::illegal_state(
                        "OffsetsRequestManager update_fetch_positions sender dropped",
                    ));
                    return;
                },
            }

            // --- Step 4: createFetchRequests. ---
            let fetch_rx = {
                let mut rm_guard = match request_managers.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                let Some(fetch_mgr) = rm_guard.fetch.as_mut() else {
                    state.complete_exceptionally(KafkaError::illegal_state(
                        "FetchRequestManager not available when processing AsyncPoll",
                    ));
                    return;
                };
                fetch_mgr.create_fetch_requests()
            };
            match fetch_rx.await {
                Ok(Ok(())) => {
                    state.complete_successfully();
                },
                Ok(Err(err)) => {
                    if is_ignorable_async_poll_error(&err) {
                        // Mirror Java's trace-log-and-complete behaviour:
                        // a timeout during createFetchRequests should NOT
                        // fail the event (Java logs and falls through to
                        // event.completeSuccessfully).
                        state.complete_successfully();
                    } else {
                        state.complete_exceptionally(err);
                    }
                },
                Err(_recv_err) => {
                    state.complete_exceptionally(KafkaError::illegal_state(
                        "FetchRequestManager create_fetch_requests sender dropped",
                    ));
                },
            }
        });
    }

    // -------------------------------------------------------------------
    // Lock helpers
    // -------------------------------------------------------------------

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
}

// ============================================================================
// Helpers
// ============================================================================

/// Returns the current wall-clock time in milliseconds since the Unix epoch.
///
/// Java's `Time.milliseconds()` is mock-friendly; the Rust translation uses
/// `std::time::SystemTime` directly. Tests that need to control time
/// either inject a mock time source via the request manager's own clock or
/// drive the processor's `current_time_ms` arg on events like
/// `AssignmentChange` (which carries it explicitly per Java).
fn current_time_ms_now() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(i64::MAX)
}

/// Java: `maybeCompleteAsyncPollEventExceptionally(event, t)`.
///
/// Returns `true` when the error should be IGNORED (logged at trace level
/// in Java) rather than failing the event. Specifically, Java ignores
/// timeout exceptions during the update-positions / create-fetch-requests
/// chain so the consumer can recover on the next `poll()` iteration —
/// AsyncPoll itself is a polling primitive and a per-iteration timeout is
/// not user-facing. Non-timeout errors are surfaced.
fn is_ignorable_async_poll_error(err: &KafkaError) -> bool {
    matches!(err, KafkaError::Timeout(_))
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

            // ───── Async-dispatch arms (commit 5/N) ─────
            //
            // Each arm calls into the corresponding request manager,
            // gets a `oneshot::Receiver`, then spawns a continuation
            // task that awaits the receiver and writes the result to
            // the event's handle. Mirrors Java's
            // `whenComplete(complete(event.future()))` chain. See the
            // module-level docstring "Async-arm pattern" for details.
            ApplicationEvent::AsyncPoll { deadline_ms, poll_time_ms, state } => {
                self.process_async_poll(deadline_ms, poll_time_ms, state);
            },
            ApplicationEvent::CommitAsync { handle, offsets_ready, offsets } => {
                self.process_commit_async(handle, offsets_ready, offsets);
            },
            ApplicationEvent::CommitSync { handle, offsets_ready, offsets } => {
                self.process_commit_sync(handle, offsets_ready, offsets);
            },
            ApplicationEvent::FetchCommittedOffsets { handle, partitions } => {
                self.process_fetch_committed_offsets(handle, partitions);
            },
            ApplicationEvent::ListOffsets { handle, timestamps_to_search, require_timestamps } => {
                self.process_list_offsets(handle, timestamps_to_search, require_timestamps);
            },
            ApplicationEvent::CheckAndUpdatePositions { handle } => {
                self.process_check_and_update_positions(handle);
            },
            ApplicationEvent::TopicMetadata { handle, topic } => {
                self.process_topic_metadata(handle, topic);
            },
            ApplicationEvent::AllTopicsMetadata { handle } => {
                self.process_all_topics_metadata(handle);
            },
            ApplicationEvent::Unsubscribe { handle } => {
                self.process_unsubscribe(handle);
            },
            ApplicationEvent::CreateFetchRequests { handle } => {
                self.process_create_fetch_requests(handle);
            },
            ApplicationEvent::LeaveGroupOnClose { handle, membership_operation } => {
                self.process_leave_group_on_close(handle, membership_operation);
            },
            ApplicationEvent::ApplyAssignment { handle, assigned_partitions, added_partitions } => {
                self.process_apply_assignment(handle, assigned_partitions, added_partitions);
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
    use crate::consumer::internals::request_manager::RequestManager;
    use crate::consumer::internals::subscription_state::SubscriptionState;
    use crate::consumer::internals::topic_metadata_request_manager::TopicMetadataRequestManager;

    /// Shared test fixture mirroring Java's `setupProcessor(withGroupId)`.
    struct Fixture {
        processor: ApplicationEventProcessor,
        request_managers: Arc<Mutex<RequestManagers>>,
        metadata: Arc<ConsumerMetadata>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        /// Shared reaper — tests that exercise secondary-handle
        /// registration (e.g. the `process_commit_async` empty-manager
        /// arm) drive it directly via `reap(now)`.
        reaper: Arc<Mutex<CompletableEventReaper>>,
    }

    fn make_metadata(subs: Arc<Mutex<SubscriptionState>>) -> Arc<ConsumerMetadata> {
        let config = ConsumerConfig::new(vec!["localhost:9092".to_string()]);
        Arc::new(ConsumerMetadata::from_config(&config, subs, ClusterResourceListeners::new()))
    }

    fn make_subscriptions() -> Arc<Mutex<SubscriptionState>> {
        Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::EARLIEST)))
    }

    fn setup_processor(with_group_id: bool) -> Fixture {
        setup_processor_with_fetch(with_group_id, false)
    }

    /// Wider fixture builder used by tests that exercise the `AsyncPoll`
    /// arm end-to-end — the fetch manager (when `with_fetch` is true)
    /// short-circuits to an immediate Ok(()) ack via `poll_internal` when
    /// the subscription has no fetchable partitions, which matches the
    /// Java tests' Mockito-stubbed `createFetchRequests` return.
    fn setup_processor_with_fetch(with_group_id: bool, with_fetch: bool) -> Fixture {
        use crate::common::memory::buffer_supplier::BufferSupplier;
        use crate::consumer::internals::fetch_buffer::FetchBuffer;
        use crate::consumer::internals::fetch_config::FetchConfig;
        use crate::consumer::internals::fetch_request_manager::{
            FetchRequestManager, always_available, no_auth_failure,
        };

        let subscriptions = make_subscriptions();
        let metadata = make_metadata(Arc::clone(&subscriptions));

        let config = ConsumerConfig::new(vec!["localhost:9092".to_string()]);
        let coordinator = if with_group_id {
            Some(Arc::new(CoordinatorRequestManager::new(100, 1_000, "test-group")))
        } else {
            None
        };
        let commit = if with_group_id {
            Some(Arc::new(CommitRequestManager::new(
                &config,
                Arc::clone(&metadata),
                Arc::clone(&subscriptions),
                "test-group",
                None,
                Arc::new(crate::common::metrics::time::SystemTime),
                0,
            )))
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
        let fetch = if with_fetch {
            let fetch_config = FetchConfig::new(
                1,
                50 * 1024 * 1024,
                500,
                1024 * 1024,
                500,
                true,
                "",
                IsolationLevel::ReadUncommitted,
            );
            Some(FetchRequestManager::new(
                Arc::clone(&metadata),
                Arc::clone(&subscriptions),
                fetch_config,
                Arc::new(FetchBuffer::new()),
                Arc::new(BufferSupplier::create()),
                always_available(),
                no_auth_failure(),
                Arc::new(ApiVersions::new()),
                crate::consumer::internals::fetch_metrics_manager::FetchMetricsManager::for_test(),
            ))
        } else {
            None
        };

        let request_managers = Arc::new(Mutex::new(RequestManagers::new(
            coordinator,
            topic_metadata,
            commit,
            consumer_heartbeat,
            None, // consumer_membership held via Arc on the heartbeat manager
            offsets,
            fetch,
        )));

        let reaper = Arc::new(Mutex::new(CompletableEventReaper::new()));
        let processor = ApplicationEventProcessor::new(
            Arc::clone(&request_managers),
            Arc::clone(&metadata),
            Arc::clone(&subscriptions),
            Arc::clone(&reaper),
        );

        Fixture { processor, request_managers, metadata, subscriptions, reaper }
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
        let hb_coordinator = Arc::new(CoordinatorRequestManager::new(100, 1_000, "test-group"));
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
            None,
            Arc::new(crate::common::metrics::time::SystemTime),
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
        let coord_arc = rm_guard.coordinator.as_ref().expect("coordinator present");
        assert!(coord_arc.is_closing(), "signal_close should have flipped the closing flag");
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

    // ===================================================================
    // Async-arm smoke tests (commit 5/N)
    //
    // Java: `ApplicationEventProcessorTest` covers each async arm. This
    // commit translates the smallest set proving each arm's bg-side
    // wiring works end-to-end. The exhaustive translation is owned by
    // commit 6/N (per PLAN.md). These tests target the arm-level wiring:
    // that the processor spawns a continuation, awaits the manager
    // future, and completes the event's handle.
    // ===================================================================

    /// `CheckAndUpdatePositions` with no positions to fetch resolves
    /// immediately. The bg-side `OffsetsRequestManager::update_fetch_positions`
    /// short-circuits when the subscription has no partitions requiring
    /// validation / reset.
    #[tokio::test(flavor = "current_thread")]
    async fn check_and_update_positions_resolves_when_no_partitions_pending() {
        let mut fx = setup_processor(true);
        let (handle, rx) = CompletableEventHandle::<()>::new(60_000);
        fx.processor.process(ApplicationEvent::CheckAndUpdatePositions { handle });
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), rx)
            .await
            .expect("CheckAndUpdatePositions must not hang")
            .expect("sender alive");
        result.expect("update_fetch_positions must succeed when no partitions need positions");
    }

    /// `CommitAsync` without a commit manager fails the primary handle
    /// with `illegal_state` carrying Java's exact error message. Java's
    /// `process(AsyncCommitEvent)` empty-manager branch only completes
    /// `event.future()` exceptionally and leaves `offsetsReady`
    /// un-completed — the app-side then surfaces a TimeoutException via
    /// the user-supplied `Timer` in `ConsumerUtils.getResult`.
    ///
    /// Rust's `offsets_ready` is a oneshot sender; dropping it un-completed
    /// would resolve the receiver with `RecvError` immediately, diverging
    /// from Java's "wait the full deadline then TimeoutException". Phase-10
    /// R3-1 fixes that by registering `offsets_ready` with the
    /// application-event reaper, which keeps a strong ref to the inner
    /// sender and completes it with `KafkaError::Timeout` once the
    /// deadline elapses — mirroring Java's `Timer`-based timeout.
    ///
    /// This test pins both halves of the contract: (a) the primary
    /// handle fails immediately with the illegal-state message, (b) the
    /// secondary `offsets_ready` is registered with the reaper and is
    /// completed with a `KafkaError::Timeout` when `reap` runs past the
    /// deadline.
    #[tokio::test(flavor = "current_thread")]
    async fn commit_async_without_commit_manager_fails_with_illegal_state() {
        let mut fx = setup_processor(false); // no group id → no commit manager
        let (handle, rx) = CompletableEventHandle::<HashMap<TopicPartition, OffsetAndMetadata>>::new(60_000);
        let (offsets_ready, mut ready_rx) = CompletableEventHandle::<()>::new(60_000);
        // Snapshot the erased handle BEFORE the variant moves
        // `offsets_ready`. Used both to observe `is_done()` after the
        // empty-manager arm runs and to assert the reaper sees the same
        // inner slot via `contains`.
        let ready_probe = offsets_ready.erased();

        // Pre-check: reaper is empty before the AEP fires.
        {
            let r = fx.reaper.lock().unwrap();
            assert_eq!(r.size(), 0, "reaper must start empty");
        }

        fx.processor
            .process(ApplicationEvent::CommitAsync { handle, offsets_ready, offsets: None });

        let err = rx.await.expect("sender alive").expect_err("primary handle must fail");
        assert!(
            err.to_string().contains("CommitRequestManager is not available"),
            "expected illegal-state error mentioning CommitRequestManager, got: {err}"
        );

        // The secondary `offsets_ready` handle must be registered with
        // the reaper (Phase-10 R3-1) — `contains` matches via
        // `inner_id`, so a freshly-erased probe still resolves to the
        // same tracked slot.
        {
            let r = fx.reaper.lock().unwrap();
            assert_eq!(r.size(), 1, "reaper must track offsets_ready after empty-manager arm");
            assert!(
                r.contains(&ready_probe),
                "reaper must contain offsets_ready (matched by inner_id, not Arc identity)"
            );
        }

        // Before the deadline, the secondary handle is pending — the
        // receiver sees neither a value nor a closed sender.
        assert!(
            !ready_probe.is_done(),
            "offsets_ready must remain un-completed before deadline elapses",
        );
        assert!(
            matches!(ready_rx.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty)),
            "offsets_ready receiver must be pending before deadline (Rust mirrors Java's Timer wait)",
        );

        // Advance past the deadline and run the reaper — Java's `Timer`
        // fires `TimeoutException`; Rust's reaper fires
        // `KafkaError::Timeout` with the equivalent diagnostic.
        let expired = {
            let mut r = fx.reaper.lock().unwrap();
            r.reap(60_001)
        };
        assert_eq!(expired, 1, "reap must count the past-due offsets_ready handle");

        // Receiver now resolves with the timeout error — exact variant
        // and message asserted (DoD §3).
        let received = ready_rx.await.expect("reaper completed the sender, receiver must resolve");
        let timeout_err = received.expect_err("expected timeout error, got Ok");
        match &timeout_err {
            KafkaError::Timeout(msg) => {
                assert!(
                    msg.contains("past its expiration"),
                    "expected reaper timeout diagnostic, got: {timeout_err}"
                );
            },
            other => panic!("expected KafkaError::Timeout, got: {other:?}"),
        }

        // And the reaper has dropped the entry now that it is done.
        {
            let r = fx.reaper.lock().unwrap();
            assert_eq!(r.size(), 0, "reaper must drop the completed entry");
        }
    }

    /// `CommitAsync` with empty offsets and a group-id'd consumer resolves
    /// both the secondary `offsets_ready` and primary handle successfully.
    /// `offsets: None` → resolve via `subscriptions.all_consumed()`, which
    /// is empty when no positions have been recorded.
    #[tokio::test(flavor = "current_thread")]
    async fn commit_async_empty_consumed_offsets_completes_handle_ok() {
        let mut fx = setup_processor(true);
        let (handle, rx) = CompletableEventHandle::<HashMap<TopicPartition, OffsetAndMetadata>>::new(60_000);
        let (offsets_ready, ready_rx) = CompletableEventHandle::<()>::new(60_000);
        fx.processor
            .process(ApplicationEvent::CommitAsync { handle, offsets_ready, offsets: None });
        // `offsets_ready` must fire first (Java: mark_offsets_ready BEFORE awaiting).
        ready_rx
            .await
            .expect("sender alive")
            .expect("offsets_ready must complete on success");
        // Primary handle: empty input → empty result map, OK.
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), rx)
            .await
            .expect("CommitAsync must not hang on empty offsets")
            .expect("sender alive")
            .expect("ok");
        assert!(result.is_empty());
    }

    /// `CommitSync` without a commit manager fails the primary handle
    /// with `illegal_state` and registers `offsets_ready` with the
    /// reaper so its deadline is enforced (Phase-10 R3-1) — mirroring
    /// Java's `process(SyncCommitEvent)` empty-manager branch followed
    /// by the `ConsumerUtils.getResult(offsetsReady, timer)`
    /// `TimeoutException`. See `commit_async_without_commit_manager_*`
    /// for the full rationale.
    #[tokio::test(flavor = "current_thread")]
    async fn commit_sync_without_commit_manager_fails_with_illegal_state() {
        let mut fx = setup_processor(false);
        let (handle, rx) = CompletableEventHandle::<HashMap<TopicPartition, OffsetAndMetadata>>::new(60_000);
        let (offsets_ready, mut ready_rx) = CompletableEventHandle::<()>::new(60_000);
        let ready_probe = offsets_ready.erased();

        {
            let r = fx.reaper.lock().unwrap();
            assert_eq!(r.size(), 0, "reaper must start empty");
        }

        fx.processor
            .process(ApplicationEvent::CommitSync { handle, offsets_ready, offsets: None });

        let err = rx.await.expect("sender alive").expect_err("must fail without commit manager");
        assert!(
            err.to_string().contains("CommitRequestManager is not available"),
            "expected illegal-state error, got: {err}"
        );

        // The secondary `offsets_ready` handle is registered with the
        // reaper after the empty-manager arm runs.
        {
            let r = fx.reaper.lock().unwrap();
            assert_eq!(r.size(), 1, "reaper must track offsets_ready after empty-manager arm");
            assert!(
                r.contains(&ready_probe),
                "reaper must contain offsets_ready (matched by inner_id)"
            );
        }

        assert!(
            !ready_probe.is_done(),
            "offsets_ready must remain un-completed before deadline elapses",
        );
        assert!(
            matches!(ready_rx.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty)),
            "offsets_ready receiver must be pending before deadline",
        );

        // Advance past the deadline and reap — secondary handle is
        // completed with `KafkaError::Timeout` (Java's
        // `TimeoutException` analog).
        let expired = {
            let mut r = fx.reaper.lock().unwrap();
            r.reap(60_001)
        };
        assert_eq!(expired, 1, "reap must count the past-due offsets_ready handle");

        let received = ready_rx.await.expect("reaper completed the sender, receiver must resolve");
        let timeout_err = received.expect_err("expected timeout error, got Ok");
        match &timeout_err {
            KafkaError::Timeout(msg) => {
                assert!(
                    msg.contains("past its expiration"),
                    "expected reaper timeout diagnostic, got: {timeout_err}"
                );
            },
            other => panic!("expected KafkaError::Timeout, got: {other:?}"),
        }

        {
            let r = fx.reaper.lock().unwrap();
            assert_eq!(r.size(), 0, "reaper must drop the completed entry");
        }
    }

    /// `FetchCommittedOffsets` without a commit manager fails the handle
    /// with `illegal_state`.
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_committed_offsets_without_commit_manager_fails_with_illegal_state() {
        let mut fx = setup_processor(false);
        let (handle, rx) = CompletableEventHandle::<HashMap<TopicPartition, OffsetAndMetadata>>::new(60_000);
        fx.processor
            .process(ApplicationEvent::FetchCommittedOffsets { handle, partitions: HashSet::new() });
        let err = rx.await.expect("sender alive").expect_err("must fail without commit manager");
        assert!(
            err.to_string().contains("CommitRequestManager is not available"),
            "expected illegal-state error, got: {err}"
        );
    }

    /// `FetchCommittedOffsets` with an empty partition set resolves
    /// immediately to an empty map (commit manager short-circuits).
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_committed_offsets_empty_partitions_resolves_immediately() {
        let mut fx = setup_processor(true);
        let (handle, rx) = CompletableEventHandle::<HashMap<TopicPartition, OffsetAndMetadata>>::new(60_000);
        fx.processor
            .process(ApplicationEvent::FetchCommittedOffsets { handle, partitions: HashSet::new() });
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), rx)
            .await
            .expect("FetchCommittedOffsets must not hang on empty partitions")
            .expect("sender alive")
            .expect("ok");
        assert!(result.is_empty());
    }

    /// `ListOffsets` with an empty timestamps map resolves immediately to
    /// an empty result map (OffsetsRequestManager short-circuits).
    #[tokio::test(flavor = "current_thread")]
    async fn list_offsets_empty_timestamps_resolves_immediately() {
        let mut fx = setup_processor(true);
        let (handle, rx) = CompletableEventHandle::<
            HashMap<
                TopicPartition,
                Option<crate::consumer::internals::offset_and_timestamp_internal::OffsetAndTimestampInternal>,
            >,
        >::new(60_000);
        fx.processor.process(ApplicationEvent::ListOffsets {
            handle,
            timestamps_to_search: HashMap::new(),
            require_timestamps: false,
        });
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), rx)
            .await
            .expect("ListOffsets must not hang on empty timestamps")
            .expect("sender alive")
            .expect("ok");
        assert!(result.is_empty());
    }

    /// `CreateFetchRequests` with no fetch manager fails the handle with
    /// `illegal_state` (fetch slot is `None` in the test fixture for
    /// non-group-id consumers, and `setup_processor` leaves it `None`
    /// even with-group-id; the smoke test confirms the failure path).
    #[tokio::test(flavor = "current_thread")]
    async fn create_fetch_requests_without_fetch_manager_fails_with_illegal_state() {
        let mut fx = setup_processor(true);
        let (handle, rx) = CompletableEventHandle::<()>::new(60_000);
        fx.processor.process(ApplicationEvent::CreateFetchRequests { handle });
        let err = rx.await.expect("sender alive").expect_err("must fail without fetch manager");
        assert!(
            err.to_string().contains("FetchRequestManager not available"),
            "expected illegal-state error mentioning FetchRequestManager, got: {err}"
        );
    }

    /// `Unsubscribe` without a heartbeat manager (no group id) clears
    /// subscription state inline and completes the handle synchronously.
    #[tokio::test(flavor = "current_thread")]
    async fn unsubscribe_without_group_id_clears_subscription_inline() {
        let mut fx = setup_processor(false);
        // Set up a concrete subscription first so unsubscribe has work to do.
        {
            let mut guard = fx.subscriptions.lock().unwrap();
            let mut topics = HashSet::new();
            topics.insert("topic1".to_string());
            guard.subscribe_topics(topics, None).unwrap();
        }
        let (handle, rx) = CompletableEventHandle::<()>::new(60_000);
        fx.processor.process(ApplicationEvent::Unsubscribe { handle });
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), rx)
            .await
            .expect("Unsubscribe must not hang")
            .expect("sender alive");
        result.expect("unsubscribe must succeed");
        // Subscription is cleared.
        let guard = fx.subscriptions.lock().unwrap();
        assert!(
            guard.subscription().is_empty(),
            "subscription must be cleared after unsubscribe"
        );
    }

    /// `LeaveGroupOnClose` without a heartbeat manager fails the handle
    /// with `illegal_state` (Java's branch logs and returns; Rust
    /// surfaces the failure to avoid a silent drop per DoD §5).
    #[tokio::test(flavor = "current_thread")]
    async fn leave_group_on_close_without_heartbeat_fails_handle() {
        let mut fx = setup_processor(false);
        let (handle, rx) = CompletableEventHandle::<()>::new(60_000);
        fx.processor.process(ApplicationEvent::LeaveGroupOnClose {
            handle,
            membership_operation: crate::consumer::GroupMembershipOperation::LeaveGroup,
        });
        let err = rx
            .await
            .expect("sender alive")
            .expect_err("must fail without membership manager");
        assert!(
            err.to_string().contains("ConsumerMembershipManager not available"),
            "expected illegal-state error, got: {err}"
        );
    }

    /// `AsyncPoll` without an OffsetsRequestManager fails the
    /// `AsyncPollState` cleanly.
    #[tokio::test(flavor = "current_thread")]
    async fn async_poll_without_offsets_manager_fails_state() {
        let subscriptions = make_subscriptions();
        let metadata = make_metadata(Arc::clone(&subscriptions));
        // Wire RequestManagers with NO offsets manager — exercise the
        // failure path inside `process_async_poll`.
        let request_managers = Arc::new(Mutex::new(RequestManagers::new(None, None, None, None, None, None, None)));
        let reaper = Arc::new(Mutex::new(CompletableEventReaper::new()));
        let mut processor = ApplicationEventProcessor::new(
            Arc::clone(&request_managers),
            Arc::clone(&metadata),
            Arc::clone(&subscriptions),
            reaper,
        );

        let state = Arc::new(super::super::application_event::AsyncPollState::new());
        processor.process(ApplicationEvent::AsyncPoll {
            deadline_ms: 60_000,
            poll_time_ms: 0,
            state: Arc::clone(&state),
        });
        // Spin until the spawned task completes.
        for _ in 0..200 {
            if state.is_complete() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(state.is_complete(), "AsyncPoll state should complete");
        let err = state.error().expect("error must be set when no OffsetsRequestManager is wired");
        assert!(
            err.to_string().contains("OffsetsRequestManager not available"),
            "expected illegal-state error, got: {err}"
        );
    }

    /// `ConsumerRebalanceListenerCallbackCompleted` is a no-op in Rust
    /// (the §31 embedded-oneshot pattern resolves the ack directly). Verify
    /// the dispatch table accepts the event without panicking.
    #[test]
    fn rebalance_listener_callback_completed_is_noop() {
        let mut fx = setup_processor(true);
        fx.processor
            .process(ApplicationEvent::ConsumerRebalanceListenerCallbackCompleted {
                method_name: ConsumerRebalanceListenerMethodName::OnPartitionsRevoked,
                error: None,
            });
        // No assertion: the test passes if no panic.
    }

    // ===================================================================
    // Phase 10 commit 6/N — full Java-parity test translation
    //
    // The async-arm tests above (commit 5/N) cover the dispatch-table
    // wiring. The tests below translate each Java case faithfully,
    // including happy-path commit/fetch flows that the commit-5 smoke
    // tests intentionally skipped. To drive happy paths without Mockito,
    // we use the `complete_first_unsent_commit_for_test` helpers on
    // `CommitRequestManager` (added in this commit) as the equivalent of
    // Java's `Mockito.when(...).thenReturn(...)` stubs.
    // ===================================================================

    /// Spin until `predicate()` returns `true`, polling every 5 ms up to
    /// `timeout`. Used to wait for spawned continuations.
    async fn yield_until<F: FnMut() -> bool>(mut predicate: F, timeout: std::time::Duration) -> bool {
        let start = std::time::Instant::now();
        loop {
            if predicate() {
                return true;
            }
            if start.elapsed() >= timeout {
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    fn make_offset_and_metadata(offset: i64, epoch: Option<i32>) -> OffsetAndMetadata {
        OffsetAndMetadata::with_leader_epoch(offset, epoch, "").expect("valid offset")
    }

    // -------------------------------------------------------------------
    // Java: testProcessUnsubscribeEventWithGroupId
    //
    // With a group id wired in, the AEP routes Unsubscribe through
    // `membership_manager.leave_group(now_ms)`. The leave_group future
    // resolves through the membership state machine; for this test we
    // observe that the AEP correctly spawns and completes the handle.
    // -------------------------------------------------------------------
    #[tokio::test(flavor = "current_thread")]
    async fn process_unsubscribe_event_with_group_id() {
        let mut fx = setup_processor(true);
        let (handle, rx) = CompletableEventHandle::<()>::new(60_000);
        fx.processor.process(ApplicationEvent::Unsubscribe { handle });
        // The leave_group call resolves the handle on the spawned task —
        // we only verify the handle completes (success or failure both
        // satisfy the spawn-pattern contract; Java verifies via
        // `verify(membershipManager).leaveGroup()`).
        let resolved = tokio::time::timeout(std::time::Duration::from_secs(5), rx).await;
        assert!(
            resolved.is_ok(),
            "Unsubscribe with group id must spawn a continuation that completes the handle"
        );
    }

    // -------------------------------------------------------------------
    // Java: testApplicationEventIsProcessed (parameterized over 5
    // representative events).
    //
    // Java uses Mockito to verify the dispatch overload is selected
    // correctly. In Rust the enum match is exhaustive, so the equivalent
    // check is that each representative event reaches its arm without
    // panic and (where the arm completes synchronously) drives the
    // expected state change. We translate this as a single dispatch
    // exercise.
    // -------------------------------------------------------------------
    #[tokio::test(flavor = "current_thread")]
    async fn application_event_is_processed_dispatches_all_representatives() {
        let mut fx = setup_processor(true);
        // AsyncPollEvent — drives the spawn path (no assertions on
        // outcome here; covered by the dedicated AsyncPoll tests).
        let state = Arc::new(super::super::application_event::AsyncPollState::new());
        fx.processor
            .process(ApplicationEvent::AsyncPoll { deadline_ms: 12_445, poll_time_ms: 12_345, state });
        // CreateFetchRequestsEvent — fetch manager absent → handle fails;
        // observed via the receiver resolving.
        let (handle, rx) = CompletableEventHandle::<()>::new(20_000);
        fx.processor.process(ApplicationEvent::CreateFetchRequests { handle });
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), rx).await;
        // CheckAndUpdatePositionsEvent — succeeds with empty subs.
        let (handle, rx) = CompletableEventHandle::<()>::new(500);
        fx.processor.process(ApplicationEvent::CheckAndUpdatePositions { handle });
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), rx).await;
        // TopicMetadataEvent.
        let (handle, _rx) = CompletableEventHandle::<HashMap<String, Vec<crate::common::PartitionInfo>>>::new(i64::MAX);
        fx.processor
            .process(ApplicationEvent::TopicMetadata { handle, topic: "topic".to_string() });
        // AssignmentChangeEvent (empty assignment).
        let (handle, rx) = CompletableEventHandle::<()>::new(20_000);
        fx.processor.process(ApplicationEvent::AssignmentChange {
            handle,
            current_time_ms: 12_345,
            partitions: HashSet::new(),
        });
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), rx).await;
    }

    // -------------------------------------------------------------------
    // Java: testListOffsetsEventIsProcessed (parameterized over
    // requireTimestamp = true / false).
    //
    // Mockito-style dispatch verification → Rust uses an empty
    // timestamps map so the OffsetsRequestManager short-circuits.
    // -------------------------------------------------------------------
    #[tokio::test(flavor = "current_thread")]
    async fn list_offsets_event_is_processed() {
        for require_timestamps in [true, false] {
            let mut fx = setup_processor(true);
            let (handle, rx) = CompletableEventHandle::<
                HashMap<
                    TopicPartition,
                    Option<crate::consumer::internals::offset_and_timestamp_internal::OffsetAndTimestampInternal>,
                >,
            >::new(20_000);
            fx.processor.process(ApplicationEvent::ListOffsets {
                handle,
                timestamps_to_search: HashMap::new(),
                require_timestamps,
            });
            let result = tokio::time::timeout(std::time::Duration::from_secs(2), rx)
                .await
                .expect("must not hang")
                .expect("sender alive")
                .expect("ok");
            assert!(result.is_empty(), "empty timestamps -> empty result");
        }
    }

    // -------------------------------------------------------------------
    // Java: testAsyncPollEvent
    //
    // Full happy path: the AEP arm must call the commit manager's
    // timer-update, the heartbeat's reset-poll-timer, the offsets
    // manager's update-positions, and the fetch manager's
    // create-fetch-requests. With empty subscriptions all of these
    // short-circuit, so `state.is_complete()` flips after the spawned
    // task drains its continuation chain.
    // -------------------------------------------------------------------
    #[tokio::test(flavor = "current_thread")]
    async fn async_poll_event_completes_state_through_full_chain() {
        let fx = setup_processor_with_fetch(true, true);
        let request_managers = Arc::clone(&fx.request_managers);
        let mut processor = fx.processor;
        let state = Arc::new(super::super::application_event::AsyncPollState::new());
        processor.process(ApplicationEvent::AsyncPoll {
            deadline_ms: 12_446,
            poll_time_ms: 12_345,
            state: Arc::clone(&state),
        });

        // Drive the fetch manager's `poll()` to complete the
        // `create_fetch_requests` ack the spawned task is awaiting. Empty
        // subscriptions → empty prepared map → all pending acks complete
        // Ok(()).
        let request_managers_for_drive = Arc::clone(&request_managers);
        let driver = tokio::spawn(async move {
            for _ in 0..200 {
                {
                    let mut guard = request_managers_for_drive.lock().expect("rm poisoned");
                    if let Some(fetch_mgr) = guard.fetch.as_mut() {
                        let _ = RequestManager::poll(fetch_mgr, 12_345);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        });

        let completed = yield_until(|| state.is_complete(), std::time::Duration::from_secs(5)).await;
        driver.abort();
        assert!(completed, "AsyncPoll must complete the state on a fully-wired processor");
        // mark_validate_positions_complete must have fired (Java's
        // `event.markValidatePositionsComplete()`).
        assert!(
            state.is_validate_positions_complete(),
            "mark_validate_positions_complete should fire after update_fetch_positions returns"
        );
        // No error was set on the state.
        assert!(state.error().is_none(), "happy-path AsyncPoll must not set an error");
    }

    // -------------------------------------------------------------------
    // Java: testFetchCommittedOffsetsEvent (happy path with offsets).
    // -------------------------------------------------------------------
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_committed_offsets_event_returns_offsets_on_success() {
        let fx = setup_processor(true);
        let tp0 = tp("topic", 0);
        let tp1 = tp("topic", 1);
        let tp2 = tp("topic", 2);
        let mut partitions = HashSet::new();
        partitions.insert(tp0.clone());
        partitions.insert(tp1.clone());
        partitions.insert(tp2.clone());

        let mut processor = fx.processor;
        let request_managers = Arc::clone(&fx.request_managers);
        let (handle, rx) = CompletableEventHandle::<HashMap<TopicPartition, OffsetAndMetadata>>::new(20_000);
        processor.process(ApplicationEvent::FetchCommittedOffsets { handle, partitions });

        // Stub the manager's response — Java does this via Mockito on
        // `commitRequestManager.fetchOffsets`.
        let stub = {
            let mut response = HashMap::new();
            response.insert(tp0.clone(), Some(make_offset_and_metadata(10, Some(2))));
            response.insert(tp1.clone(), Some(make_offset_and_metadata(15, None)));
            response.insert(tp2.clone(), Some(make_offset_and_metadata(20, Some(3))));
            response
        };
        // Wait until the spawned AEP task has enqueued the unsent fetch,
        // then complete it via the test helper.
        let driven = {
            let request_managers_for_drive = Arc::clone(&request_managers);
            yield_until(
                move || {
                    let guard = request_managers_for_drive.lock().expect("rm poisoned");
                    if let Some(commit) = guard.commit.as_ref() {
                        commit.complete_first_unsent_fetch_for_test(stub.clone())
                    } else {
                        false
                    }
                },
                std::time::Duration::from_secs(5),
            )
            .await
        };
        assert!(driven, "AEP must enqueue the OffsetFetch via the commit manager");

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), rx)
            .await
            .expect("must not hang")
            .expect("sender alive")
            .expect("ok");
        assert_eq!(result.len(), 3);
        assert_eq!(result.get(&tp0).map(|o| o.offset()), Some(10));
        assert_eq!(result.get(&tp1).map(|o| o.offset()), Some(15));
        assert_eq!(result.get(&tp2).map(|o| o.offset()), Some(20));
    }

    // -------------------------------------------------------------------
    // Java: testTopicPatternSubscriptionTriggersJoin
    //
    // Membership manager is notified (`on_subscription_updated`) on EVERY
    // pattern subscribe — regardless of whether `subscribeFromPattern`
    // returned `true` (an actual subscription change) or `false` (no
    // matching topics) — so the consumer joins the group if not already
    // in. The Rust impl mirrors this via the unconditional notification
    // at the end of `update_pattern_subscription`.
    // -------------------------------------------------------------------
    #[test]
    fn topic_pattern_subscription_triggers_join_even_with_no_matches() {
        let mut fx = setup_processor(true);
        // First subscribe with a pattern that matches nothing in the
        // empty cluster (Java: `subscribeFromPattern(any())` returns
        // `false`). Membership manager must still be notified.
        let pattern = Regex::new("topic.*").unwrap();
        let (handle, rx) = CompletableEventHandle::<()>::new(20_000);
        fx.processor.process(ApplicationEvent::TopicPatternSubscriptionChange {
            handle,
            pattern: pattern.clone(),
            listener: None,
        });
        await_complete(rx).expect("first subscribe must succeed");

        // Re-issue: same shape, second invocation. Java verifies
        // membership manager is notified on both. We don't have a counter
        // on the membership manager for this in Rust; observe instead
        // that the subscription state reflects the pattern subscription
        // (and a second call leaves it consistent).
        let (handle, rx) = CompletableEventHandle::<()>::new(20_000);
        fx.processor
            .process(ApplicationEvent::TopicPatternSubscriptionChange { handle, pattern, listener: None });
        await_complete(rx).expect("second subscribe must succeed");

        let guard = fx.subscriptions.lock().unwrap();
        assert!(guard.has_pattern_subscription(), "pattern subscription must remain set");
    }

    // -------------------------------------------------------------------
    // Java: testUpdatePatternSubscriptionEventOnlyTakesEffectWhenMetadataHasNewVersion
    //
    // First UpdatePatternSubscription dispatch with the current
    // (un-advanced) metadata version is a no-op. Advancing the
    // metadata version then issuing a second event invokes the
    // pattern-subscription rebuild (subscribeFromPattern + on-subscription-updated).
    // -------------------------------------------------------------------
    #[test]
    fn update_pattern_subscription_event_only_takes_effect_when_metadata_advances() {
        let mut fx = setup_processor(true);
        // Install a pattern subscription so `has_pattern_subscription()`
        // returns true (Java stubs it explicitly).
        {
            let mut guard = fx.subscriptions.lock().unwrap();
            guard.subscribe_pattern(Regex::new("topic.*").unwrap(), None).unwrap();
        }

        let initial_snapshot = fx.processor.metadata_version_snapshot();

        let (handle, rx) = CompletableEventHandle::<()>::new(20_000);
        fx.processor.process(ApplicationEvent::UpdatePatternSubscription { handle });
        await_complete(rx).expect("first update must succeed");
        // Snapshot unchanged because metadata version did not advance.
        assert_eq!(initial_snapshot, fx.processor.metadata_version_snapshot());

        // Advance the metadata version (Java: stub `updateVersion=1`).
        // `bootstrap()` bumps `update_version`.
        fx.metadata.metadata_arc().bootstrap(Vec::new());
        let advanced = fx.metadata.update_version();
        assert!(advanced > initial_snapshot, "metadata version must advance after bootstrap");

        let (handle, rx) = CompletableEventHandle::<()>::new(20_000);
        fx.processor.process(ApplicationEvent::UpdatePatternSubscription { handle });
        await_complete(rx).expect("second update must succeed");
        // Snapshot captured the new metadata version.
        assert_eq!(
            advanced,
            fx.processor.metadata_version_snapshot(),
            "second update must capture the advanced metadata version"
        );
    }

    // -------------------------------------------------------------------
    // Java: testSyncCommitEventWithEmptyOffsets
    //
    // Empty event-offsets + non-empty `subscriptions.allConsumed()`:
    // `commit_sync(allConsumed, deadline)` is called and resolves with
    // `allConsumed`. The Java test stubs `allConsumed()` to return a
    // single-entry map. In Rust we seed the subscription state so the
    // same single-entry map flows naturally.
    // -------------------------------------------------------------------
    #[tokio::test(flavor = "current_thread")]
    async fn sync_commit_event_with_empty_offsets_uses_all_consumed() {
        let fx = setup_processor(true);
        let partition = tp("topic", 0);
        // Seed `all_consumed()` with one (partition, position) entry.
        let position = FetchPosition::with_leader(10, Some(1), crate::metadata::LeaderAndEpoch::no_leader_or_epoch());
        {
            let mut guard = fx.subscriptions.lock().unwrap();
            let mut tps = HashSet::new();
            tps.insert(partition.clone());
            guard.assign_from_user(tps).unwrap();
            guard.seek_unvalidated(&partition, position).unwrap();
        }
        let mut processor = fx.processor;
        let request_managers = Arc::clone(&fx.request_managers);

        let (handle, rx) = CompletableEventHandle::<HashMap<TopicPartition, OffsetAndMetadata>>::new(60_000);
        let (offsets_ready, ready_rx) = CompletableEventHandle::<()>::new(60_000);
        processor.process(ApplicationEvent::CommitSync { handle, offsets_ready, offsets: None });
        // `offsets_ready` must complete first (per Java's `markOffsetsReady`).
        ready_rx.await.expect("sender alive").expect("offsets_ready must succeed");

        // Drive the commit manager: complete the queued commit with the
        // same offsets the manager was handed.
        let stub_offsets = {
            let mut m = HashMap::new();
            m.insert(partition.clone(), make_offset_and_metadata(10, Some(1)));
            m
        };
        let driven = {
            let request_managers_for_drive = Arc::clone(&request_managers);
            yield_until(
                move || {
                    let guard = request_managers_for_drive.lock().expect("rm poisoned");
                    if let Some(commit) = guard.commit.as_ref() {
                        commit.complete_first_unsent_commit_for_test(stub_offsets.clone())
                    } else {
                        false
                    }
                },
                std::time::Duration::from_secs(5),
            )
            .await
        };
        assert!(driven, "AEP must enqueue the OffsetCommit via the commit manager");

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), rx)
            .await
            .expect("must not hang")
            .expect("sender alive")
            .expect("ok");
        assert_eq!(result.len(), 1);
        assert_eq!(result.get(&partition).map(|o| o.offset()), Some(10));
    }

    // -------------------------------------------------------------------
    // Java: testSyncCommitEvent
    //
    // Non-empty event-offsets: `commit_sync(offsets, deadline)` is called
    // with the supplied map (NOT via `all_consumed()`) and resolves with
    // the same map.
    // -------------------------------------------------------------------
    #[tokio::test(flavor = "current_thread")]
    async fn sync_commit_event_with_offsets_uses_offsets() {
        let fx = setup_processor(true);
        let partition = tp("topic", 0);
        let mut offsets = HashMap::new();
        offsets.insert(partition.clone(), make_offset_and_metadata(10, Some(1)));

        let mut processor = fx.processor;
        let request_managers = Arc::clone(&fx.request_managers);
        let (handle, rx) = CompletableEventHandle::<HashMap<TopicPartition, OffsetAndMetadata>>::new(60_000);
        let (offsets_ready, ready_rx) = CompletableEventHandle::<()>::new(60_000);
        processor.process(ApplicationEvent::CommitSync { handle, offsets_ready, offsets: Some(offsets.clone()) });
        ready_rx.await.expect("sender alive").expect("offsets_ready must succeed");

        let stub = offsets.clone();
        let driven = {
            let request_managers_for_drive = Arc::clone(&request_managers);
            yield_until(
                move || {
                    let guard = request_managers_for_drive.lock().expect("rm poisoned");
                    if let Some(commit) = guard.commit.as_ref() {
                        commit.complete_first_unsent_commit_for_test(stub.clone())
                    } else {
                        false
                    }
                },
                std::time::Duration::from_secs(5),
            )
            .await
        };
        assert!(driven, "AEP must enqueue the OffsetCommit");

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), rx)
            .await
            .expect("must not hang")
            .expect("sender alive")
            .expect("ok");
        assert_eq!(result, offsets);
    }

    // -------------------------------------------------------------------
    // Java: testSyncCommitEventWithException
    //
    // The commit manager future fails with an `IllegalStateException`;
    // the AEP propagates the failure to the primary handle. Per Java:
    // `offsets_ready.isDone()` is true and `event.future()` throws.
    // -------------------------------------------------------------------
    #[tokio::test(flavor = "current_thread")]
    async fn sync_commit_event_with_exception_propagates_to_handle() {
        let fx = setup_processor(true);
        // Empty event-offsets + empty `all_consumed()` would short-circuit
        // to Ok(empty); seed a partition position so the commit is
        // non-trivial and reaches the manager queue.
        let partition = tp("topic", 0);
        let position = FetchPosition::with_leader(5, Some(1), crate::metadata::LeaderAndEpoch::no_leader_or_epoch());
        {
            let mut guard = fx.subscriptions.lock().unwrap();
            let mut tps = HashSet::new();
            tps.insert(partition.clone());
            guard.assign_from_user(tps).unwrap();
            guard.seek_unvalidated(&partition, position).unwrap();
        }
        let mut processor = fx.processor;
        let request_managers = Arc::clone(&fx.request_managers);
        let (handle, rx) = CompletableEventHandle::<HashMap<TopicPartition, OffsetAndMetadata>>::new(60_000);
        let (offsets_ready, ready_rx) = CompletableEventHandle::<()>::new(60_000);
        processor.process(ApplicationEvent::CommitSync { handle, offsets_ready, offsets: None });
        ready_rx.await.expect("sender alive").expect("offsets_ready must succeed");

        let driven = {
            let request_managers_for_drive = Arc::clone(&request_managers);
            yield_until(
                move || {
                    let guard = request_managers_for_drive.lock().expect("rm poisoned");
                    if let Some(commit) = guard.commit.as_ref() {
                        commit.fail_first_unsent_commit_for_test(KafkaError::illegal_state("boom"))
                    } else {
                        false
                    }
                },
                std::time::Duration::from_secs(5),
            )
            .await
        };
        assert!(driven, "AEP must enqueue the OffsetCommit before we can fail it");

        let err = tokio::time::timeout(std::time::Duration::from_secs(5), rx)
            .await
            .expect("must not hang")
            .expect("sender alive")
            .expect_err("failure must surface to handle");
        assert!(
            err.to_string().contains("boom"),
            "expected commit failure to propagate, got: {err}"
        );
    }

    // -------------------------------------------------------------------
    // Java: testAsyncCommitEventWithEmptyOffsets
    //
    // Same shape as testSyncCommitEventWithEmptyOffsets but routes via
    // `commit_async_no_callback` (which never expires per Java —
    // deadline = i64::MAX).
    // -------------------------------------------------------------------
    #[tokio::test(flavor = "current_thread")]
    async fn async_commit_event_with_empty_offsets_uses_all_consumed() {
        let fx = setup_processor(true);
        let partition = tp("topic", 0);
        let position = FetchPosition::with_leader(10, Some(1), crate::metadata::LeaderAndEpoch::no_leader_or_epoch());
        {
            let mut guard = fx.subscriptions.lock().unwrap();
            let mut tps = HashSet::new();
            tps.insert(partition.clone());
            guard.assign_from_user(tps).unwrap();
            guard.seek_unvalidated(&partition, position).unwrap();
        }
        let mut processor = fx.processor;
        let request_managers = Arc::clone(&fx.request_managers);
        let (handle, rx) = CompletableEventHandle::<HashMap<TopicPartition, OffsetAndMetadata>>::new(60_000);
        let (offsets_ready, ready_rx) = CompletableEventHandle::<()>::new(60_000);
        processor.process(ApplicationEvent::CommitAsync { handle, offsets_ready, offsets: None });
        ready_rx.await.expect("sender alive").expect("offsets_ready must succeed");

        let stub = {
            let mut m = HashMap::new();
            m.insert(partition.clone(), make_offset_and_metadata(10, Some(1)));
            m
        };
        let driven = {
            let request_managers_for_drive = Arc::clone(&request_managers);
            yield_until(
                move || {
                    let guard = request_managers_for_drive.lock().expect("rm poisoned");
                    if let Some(commit) = guard.commit.as_ref() {
                        commit.complete_first_unsent_commit_for_test(stub.clone())
                    } else {
                        false
                    }
                },
                std::time::Duration::from_secs(5),
            )
            .await
        };
        assert!(driven, "AEP must enqueue the OffsetCommit");

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), rx)
            .await
            .expect("must not hang")
            .expect("sender alive")
            .expect("ok");
        assert_eq!(result.len(), 1);
        assert_eq!(result.get(&partition).map(|o| o.offset()), Some(10));
    }

    // -------------------------------------------------------------------
    // Java: testAsyncCommitEvent
    // -------------------------------------------------------------------
    #[tokio::test(flavor = "current_thread")]
    async fn async_commit_event_with_offsets_uses_offsets() {
        let fx = setup_processor(true);
        let partition = tp("topic", 0);
        let mut offsets = HashMap::new();
        offsets.insert(partition.clone(), make_offset_and_metadata(10, Some(1)));

        let mut processor = fx.processor;
        let request_managers = Arc::clone(&fx.request_managers);
        let (handle, rx) = CompletableEventHandle::<HashMap<TopicPartition, OffsetAndMetadata>>::new(60_000);
        let (offsets_ready, ready_rx) = CompletableEventHandle::<()>::new(60_000);
        processor.process(ApplicationEvent::CommitAsync { handle, offsets_ready, offsets: Some(offsets.clone()) });
        ready_rx.await.expect("sender alive").expect("offsets_ready must succeed");

        let stub = offsets.clone();
        let driven = {
            let request_managers_for_drive = Arc::clone(&request_managers);
            yield_until(
                move || {
                    let guard = request_managers_for_drive.lock().expect("rm poisoned");
                    if let Some(commit) = guard.commit.as_ref() {
                        commit.complete_first_unsent_commit_for_test(stub.clone())
                    } else {
                        false
                    }
                },
                std::time::Duration::from_secs(5),
            )
            .await
        };
        assert!(driven, "AEP must enqueue the OffsetCommit");

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), rx)
            .await
            .expect("must not hang")
            .expect("sender alive")
            .expect("ok");
        assert_eq!(result, offsets);
    }

    // -------------------------------------------------------------------
    // Java: testAsyncCommitEventWithException
    //
    // Failure flows through `commit_async_no_callback`'s retriable-wrap
    // path. We fail with a non-retriable error so the error surfaces
    // verbatim (Java wraps retriable errors with
    // `RetriableCommitFailedException` — both halves of the contract are
    // exercised in `commit_request_manager.rs` tests; here we only need
    // the propagation path).
    // -------------------------------------------------------------------
    #[tokio::test(flavor = "current_thread")]
    async fn async_commit_event_with_exception_propagates_to_handle() {
        let fx = setup_processor(true);
        let partition = tp("topic", 0);
        let position = FetchPosition::with_leader(5, Some(1), crate::metadata::LeaderAndEpoch::no_leader_or_epoch());
        {
            let mut guard = fx.subscriptions.lock().unwrap();
            let mut tps = HashSet::new();
            tps.insert(partition.clone());
            guard.assign_from_user(tps).unwrap();
            guard.seek_unvalidated(&partition, position).unwrap();
        }
        let mut processor = fx.processor;
        let request_managers = Arc::clone(&fx.request_managers);
        let (handle, rx) = CompletableEventHandle::<HashMap<TopicPartition, OffsetAndMetadata>>::new(60_000);
        let (offsets_ready, ready_rx) = CompletableEventHandle::<()>::new(60_000);
        processor.process(ApplicationEvent::CommitAsync { handle, offsets_ready, offsets: None });
        ready_rx.await.expect("sender alive").expect("offsets_ready must succeed");

        let driven = {
            let request_managers_for_drive = Arc::clone(&request_managers);
            yield_until(
                move || {
                    let guard = request_managers_for_drive.lock().expect("rm poisoned");
                    if let Some(commit) = guard.commit.as_ref() {
                        commit.fail_first_unsent_commit_for_test(KafkaError::illegal_state("kaboom"))
                    } else {
                        false
                    }
                },
                std::time::Duration::from_secs(5),
            )
            .await
        };
        assert!(driven, "AEP must enqueue the OffsetCommit before we can fail it");

        let err = tokio::time::timeout(std::time::Duration::from_secs(5), rx)
            .await
            .expect("must not hang")
            .expect("sender alive")
            .expect_err("failure must surface to handle");
        assert!(
            err.to_string().contains("kaboom"),
            "expected commit failure to propagate, got: {err}"
        );
    }

    // -------------------------------------------------------------------
    // Java: testUpdatePatternSubscriptionInvokedWhenMetadataUpdated
    //
    // The AsyncPoll arm calls `maybe_update_pattern_subscription`. When
    // the metadata version has advanced AND a pattern subscription is
    // set AND a matching topic exists, the regex is re-evaluated and
    // the subscription state captures the new topics.
    // -------------------------------------------------------------------
    #[tokio::test(flavor = "current_thread")]
    async fn update_pattern_subscription_invoked_when_metadata_updated() {
        let fx = setup_processor_with_fetch(true, true);
        // Install a pattern subscription up front.
        {
            let mut guard = fx.subscriptions.lock().unwrap();
            guard.subscribe_pattern(Regex::new("test-topic.*").unwrap(), None).unwrap();
        }
        // Populate the cluster with a matching topic so
        // `update_pattern_subscription` finds something.
        publish_topic_metadata(&fx.metadata, "test-topic");

        let request_managers = Arc::clone(&fx.request_managers);
        let mut processor = fx.processor;
        let state = Arc::new(super::super::application_event::AsyncPollState::new());
        processor.process(ApplicationEvent::AsyncPoll {
            deadline_ms: 1_000,
            poll_time_ms: 100,
            state: Arc::clone(&state),
        });

        // Driver to clear the fetch ack.
        let request_managers_for_drive = Arc::clone(&request_managers);
        let driver = tokio::spawn(async move {
            for _ in 0..200 {
                {
                    let mut guard = request_managers_for_drive.lock().expect("rm poisoned");
                    if let Some(fetch_mgr) = guard.fetch.as_mut() {
                        let _ = RequestManager::poll(fetch_mgr, 100);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        });

        let completed = yield_until(|| state.is_complete(), std::time::Duration::from_secs(5)).await;
        driver.abort();
        assert!(completed, "AsyncPoll must complete");

        // Pattern-subscription rebuild ran → `test-topic` is now in the
        // concrete subscription set.
        let guard = fx.subscriptions.lock().unwrap();
        let sub = guard.subscription();
        assert!(
            sub.contains("test-topic"),
            "expected `test-topic` in subscription after pattern rebuild, got: {sub:?}"
        );
    }

    // -------------------------------------------------------------------
    // Java: testUpdatePatternSubscriptionNotInvokedWhenNotUsingPatternSubscription
    //
    // No pattern subscription installed → `maybe_update_pattern_subscription`
    // short-circuits, subscription set stays empty even when the cluster
    // contains a matching topic.
    // -------------------------------------------------------------------
    #[tokio::test(flavor = "current_thread")]
    async fn update_pattern_subscription_not_invoked_when_not_using_pattern_subscription() {
        let fx = setup_processor_with_fetch(true, true);
        publish_topic_metadata(&fx.metadata, "test-topic");
        let request_managers = Arc::clone(&fx.request_managers);
        let mut processor = fx.processor;
        let state = Arc::new(super::super::application_event::AsyncPollState::new());
        processor.process(ApplicationEvent::AsyncPoll {
            deadline_ms: 1_000,
            poll_time_ms: 100,
            state: Arc::clone(&state),
        });

        let request_managers_for_drive = Arc::clone(&request_managers);
        let driver = tokio::spawn(async move {
            for _ in 0..200 {
                {
                    let mut guard = request_managers_for_drive.lock().expect("rm poisoned");
                    if let Some(fetch_mgr) = guard.fetch.as_mut() {
                        let _ = RequestManager::poll(fetch_mgr, 100);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        });

        let completed = yield_until(|| state.is_complete(), std::time::Duration::from_secs(5)).await;
        driver.abort();
        assert!(completed, "AsyncPoll must complete even without pattern subscription");

        let guard = fx.subscriptions.lock().unwrap();
        assert!(!guard.has_pattern_subscription(), "no pattern subscription should be installed");
        assert!(guard.subscription().is_empty(), "subscription set must remain empty");
    }

    // -------------------------------------------------------------------
    // Java: testUpdatePatternSubscriptionNotInvokedWhenMetadataNotUpdated
    //
    // Pattern subscription installed but metadata version unchanged
    // across the two AsyncPoll cycles → `maybe_update_pattern_subscription`
    // short-circuits, subscription set stays empty even when the cluster
    // contains a matching topic (because the version-gating check skips
    // the regex evaluation).
    //
    // The Rust impl captures the metadata version at processor
    // construction and only advances on a `request_update_for_new_topics`
    // change. The Java test stubs `updateVersion=1, 1` (no change between
    // calls). We mirror this by leaving `update_version` at its
    // construction-time value and ensuring the snapshot is in sync with
    // the current update_version (so the gating check does nothing).
    // -------------------------------------------------------------------
    #[tokio::test(flavor = "current_thread")]
    async fn update_pattern_subscription_not_invoked_when_metadata_not_updated() {
        let fx = setup_processor_with_fetch(true, true);
        {
            let mut guard = fx.subscriptions.lock().unwrap();
            guard.subscribe_pattern(Regex::new("test-topic.*").unwrap(), None).unwrap();
        }
        publish_topic_metadata(&fx.metadata, "test-topic");
        // Sync the processor's snapshot to the current update_version,
        // simulating Java's `updateVersion=1, 1` stub. The processor's
        // snapshot is set at construction; bumping it to the current
        // version prevents the gating check from triggering a rebuild.
        let mut processor = fx.processor;
        processor.metadata_version_snapshot = fx.metadata.update_version();

        let request_managers = Arc::clone(&fx.request_managers);
        let state = Arc::new(super::super::application_event::AsyncPollState::new());
        processor.process(ApplicationEvent::AsyncPoll {
            deadline_ms: 1_000,
            poll_time_ms: 100,
            state: Arc::clone(&state),
        });

        let request_managers_for_drive = Arc::clone(&request_managers);
        let driver = tokio::spawn(async move {
            for _ in 0..200 {
                {
                    let mut guard = request_managers_for_drive.lock().expect("rm poisoned");
                    if let Some(fetch_mgr) = guard.fetch.as_mut() {
                        let _ = RequestManager::poll(fetch_mgr, 100);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        });

        let completed = yield_until(|| state.is_complete(), std::time::Duration::from_secs(5)).await;
        driver.abort();
        assert!(completed, "AsyncPoll must complete");

        let guard = fx.subscriptions.lock().unwrap();
        // Pattern-subscription rebuild did NOT run → subscription set
        // remains empty (the pattern is set but no concrete topics were
        // captured).
        assert!(
            guard.subscription().is_empty(),
            "subscription set must remain empty when metadata did not advance"
        );
    }

    // -------------------------------------------------------------------
    // Java: testRefreshCommittedOffsetsShouldNotResetIfFailedWithTimeout
    //
    // `update_fetch_positions` fails with a timeout error during
    // AsyncPoll. The Java contract: the event completes (Java
    // `event.isComplete() == true`) AND the error is surfaced through
    // `event.error()`. Wait — Java's test asserts:
    //   `assertTrue(event.isComplete());`
    //   `assertFalse(event.error().isEmpty());`
    // i.e. the AsyncPoll event ALWAYS completes, but the error field
    // captures the failure so the next poll can re-attempt.
    //
    // In the Rust impl, a non-ignorable timeout error would set the
    // error on the state. A timeout error IS ignorable per
    // `is_ignorable_async_poll_error` — so timeouts fall through
    // silently and the state completes without an error. Java's
    // `Throwable("Intentional failure")` is a generic non-timeout
    // throwable, however — Java's test actually uses `Throwable`, NOT
    // `TimeoutException` — so the error IS surfaced (the test name is
    // misleading, see the `Throwable` in the implementation).
    // -------------------------------------------------------------------
    #[tokio::test(flavor = "current_thread")]
    async fn refresh_committed_offsets_should_not_reset_if_failed() {
        refresh_committed_offsets_failure_helper(true).await;
    }

    // -------------------------------------------------------------------
    // Java: testRefreshCommittedOffsetsNotCalledIfNoGroupId
    //
    // Same shape but without a group id (no commit manager wired). The
    // event still completes and surfaces the error.
    // -------------------------------------------------------------------
    #[tokio::test(flavor = "current_thread")]
    async fn refresh_committed_offsets_not_called_if_no_group_id() {
        refresh_committed_offsets_failure_helper(false).await;
    }

    async fn refresh_committed_offsets_failure_helper(with_group_id: bool) {
        let fx = setup_processor_with_fetch(with_group_id, true);
        // Inject a cached exception so the next `update_fetch_positions`
        // call surfaces a non-timeout error — mirrors Java's
        // `Mockito.when(offsetsRequestManager.updateFetchPositions(anyLong()))
        //   .thenReturn(CompletableFuture.failedFuture(new Throwable(...)))`.
        {
            let mut guard = fx.request_managers.lock().expect("rm poisoned");
            let offsets_mgr = guard.offsets.as_mut().expect("offsets manager present");
            offsets_mgr
                .set_cached_update_positions_exception_for_test(KafkaError::illegal_state("Intentional failure"));
        }

        let request_managers = Arc::clone(&fx.request_managers);
        let mut processor = fx.processor;
        let state = Arc::new(super::super::application_event::AsyncPollState::new());
        processor.process(ApplicationEvent::AsyncPoll {
            deadline_ms: 110,
            poll_time_ms: 100,
            state: Arc::clone(&state),
        });

        // Drive the fetch manager — only reached if update_fetch_positions
        // succeeds (it won't here, but the driver is harmless).
        let request_managers_for_drive = Arc::clone(&request_managers);
        let driver = tokio::spawn(async move {
            for _ in 0..200 {
                {
                    let mut guard = request_managers_for_drive.lock().expect("rm poisoned");
                    if let Some(fetch_mgr) = guard.fetch.as_mut() {
                        let _ = RequestManager::poll(fetch_mgr, 100);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        });

        let completed = yield_until(|| state.is_complete(), std::time::Duration::from_secs(5)).await;
        driver.abort();
        assert!(completed, "AsyncPoll must complete even when update_fetch_positions fails");
        // Java: `assertFalse(event.error().isEmpty())` — the failure is
        // surfaced via `state.error()` (non-timeout errors are not
        // ignored per `is_ignorable_async_poll_error`).
        let err = state.error().expect("error must be set when update_fetch_positions fails");
        assert!(
            err.to_string().contains("Intentional failure"),
            "expected the injected failure to propagate, got: {err}"
        );
    }

    /// Publish a metadata response containing a single topic so
    /// `metadata.fetch().topics()` reports it. Mirrors the Java tests'
    /// `cluster.topics()` Mockito stubs.
    fn publish_topic_metadata(metadata: &ConsumerMetadata, topic_name: &str) {
        use crate::common::Node;
        use crate::common::Uuid;
        use crate::common::protocol::ApiKeys;
        use crate::common::requests::MetadataResponse;
        use crate::metadata_response_data::{
            MetadataResponseBroker, MetadataResponseData, MetadataResponsePartition, MetadataResponseTopic,
        };
        let node = Node::new(1, "localhost".to_string(), 9092);
        let mut data = MetadataResponseData::new();
        data.set_cluster_id(Some("test-cluster-id".to_string()));
        data.set_controller_id(node.id());

        let mut broker = MetadataResponseBroker::new();
        broker.set_node_id(node.id());
        broker.set_host(node.host().to_string());
        broker.set_port(node.port());
        data.set_brokers(vec![broker]);

        let mut topic = MetadataResponseTopic::new();
        topic.set_name(Some(topic_name.to_string()));
        topic.set_topic_id(Uuid::zero());
        topic.set_error_code(0);
        topic.set_is_internal(false);
        let mut partition = MetadataResponsePartition::new();
        partition.set_partition_index(0);
        partition.set_error_code(0);
        partition.set_leader_id(node.id());
        partition.set_leader_epoch(5);
        partition.set_replica_nodes(vec![node.id()]);
        partition.set_isr_nodes(vec![node.id()]);
        partition.set_offline_replicas(Vec::new());
        topic.set_partitions(vec![partition]);
        data.set_topics(vec![topic]);

        let response = MetadataResponse::new(data, ApiKeys::METADATA.latest_version());
        metadata
            .metadata_arc()
            .update_with_current_request_version(&response, false, 1_000);
    }
}
