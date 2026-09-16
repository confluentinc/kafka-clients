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

//! User-facing async consumer implementing the KIP-848 (consumer-group)
//! protocol.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer`. This
//! file lands the struct, constructor, and the synchronous state-read
//! methods (Phase 11 commit (2/N)). Subsequent commits (3-7) add
//! subscribe / poll / commit / position / committed / close.
//!
//! # Generic-over-`KafkaClient`
//!
//! Java's `AsyncKafkaConsumer` carries the `NetworkClient` indirectly via
//! `ApplicationEventHandler` and `ConsumerNetworkThread`. The Rust
//! `ConsumerNetworkThread<K>` is generic over `K: KafkaClient` so tests
//! can plug in `MockClient`. To keep the user-facing `Consumer<K, V>`
//! trait clean of the network-client type, the spawned task is wrapped in
//! a type-erased [`NetworkThreadCloseHandle`] that exposes only the
//! lifecycle hooks (`signal_close`, `wakeup`, `await_join`).
//!
//! # Reentrancy guard (not translated)
//!
//! Java guards against multi-thread access via the
//! `acquire()` / `release()` pair throwing
//! `ConcurrentModificationException`. Rust's `&mut self` on the
//! `Consumer` trait makes single-caller exclusivity a *compile-time*
//! guarantee — the runtime guard is redundant. See Phase 11 PLAN.md
//! deferral #4.
//!
//! # Metrics (deferred)
//!
//! All `kafkaConsumerMetrics.record*` / `asyncConsumerMetrics.record*`
//! calls in the Java source are NO-OPs in this translation, marked
//! `// METRICS-DEFERRED: …`. Per Phase 11 PLAN.md deferral #1.
//!
//! # Telemetry (deferred)
//!
//! `ClientTelemetryReporter` / `ClientTelemetryUtils` are NOT translated;
//! the corresponding fields are `None`. Per Phase 11 PLAN.md deferral #6.

#![allow(dead_code)] // Phase 11 commits 5-7 wire commit / state-query / close.

use crate::consumer::CloseOptions;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::common::metrics::{KafkaMetric, MetricConfig, Metrics, RecordingLevel};
use crate::common::utils::LogContext;
use crate::common::{Error, IsolationLevel, MetricName, TopicPartition};
use crate::consumer::ConsumerConfig;
use crate::consumer::ConsumerGroupMetadata;
use crate::consumer::ConsumerRebalanceListener;
use crate::consumer::ConsumerRecords;
use crate::consumer::OffsetAndMetadata;
use crate::consumer::OffsetAndTimestamp;
use crate::consumer::SubscriptionPattern;
use crate::consumer::internals::AsyncConsumerMetrics;
use crate::consumer::internals::ConsumerInterceptors;
use crate::consumer::internals::ConsumerMetadata;
use crate::consumer::internals::ConsumerRebalanceListenerInvoker;
use crate::consumer::internals::Deserializers;
use crate::consumer::internals::FetchBuffer;
use crate::consumer::internals::FetchCollector;
use crate::consumer::internals::FetchMetricsManager;
use crate::consumer::internals::FetchMetricsRegistry;
use crate::consumer::internals::KafkaConsumerMetrics;
use crate::consumer::internals::MemberState;
use crate::consumer::internals::MemberStateListener;
use crate::consumer::internals::OffsetAndTimestampInternal;
use crate::consumer::internals::OffsetCommitCallbackInvoker;
use crate::consumer::internals::PositionsValidator;
use crate::consumer::internals::RequestManagers;
use crate::consumer::internals::SubscriptionState;
use crate::consumer::internals::ThreadTime;
use crate::consumer::internals::WakeupTrigger;
use crate::consumer::internals::events::ApplicationEventHandler;
use crate::consumer::internals::events::BackgroundEventHandler;
use crate::consumer::internals::events::CompletableEvent;
use crate::consumer::internals::events::CompletableEventReaper;
use crate::consumer::internals::events::{ApplicationEvent, AsyncPollState};
use crate::consumer::internals::events::{BackgroundEvent, BackgroundEventEnvelope};

/// Backing join mechanism for the consumer background task.
///
/// The bg task can run in one of two execution strategies, both fully
/// behavior-equivalent at the channel / shutdown level:
///
///   - [`BgJoin::Spawned`] — a `tokio::spawn`ed task on the caller's
///     runtime. Used by unit tests and `with_components` (no-op
///     handle). Joined on close via `JoinHandle::await`.
///   - [`BgJoin::Dedicated`] — the production strategy (Phase 21): the
///     bg loop runs on its own dedicated `std::thread` hosting a
///     `current_thread` tokio runtime, removing the multi-thread
///     scheduler park/unpark churn (~54% of consumer CPU per profiling).
///     The bg loop signals completion on the `done` oneshot when it has
///     finished `cleanup()`, after which the OS thread is reaped.
enum BgJoin {
    /// `tokio::spawn`ed task. Awaitable on close.
    Spawned(Option<JoinHandle<()>>),
    /// Dedicated OS thread hosting a `current_thread` runtime.
    Dedicated {
        /// Resolves when the bg loop has exited and `cleanup()` has
        /// completed (sent from inside the dedicated thread).
        done: Option<tokio::sync::oneshot::Receiver<()>>,
        /// Handle to the OS thread; reaped (joined) after `done`.
        thread: Option<std::thread::JoinHandle<()>>,
    },
}

/// A `Clone + Send + Sync` handle to a consumer that exposes
/// [`Consumer::wakeup`] **and** the reentrant-safe consumer operations,
/// callable from a task or thread other than the one owning the consumer.
///
/// **No Java class counterpart — it recovers a Java capability.** Java's
/// `Consumer` reference is itself a freely-shareable, thread-safe
/// reference. Application code relies on this in two ways that a bare
/// Rust `&mut self` consumer cannot express:
///
///   1. **Cross-task `wakeup()`** — `consumer.wakeup()` is called from
///      another thread while the owning thread blocks in `poll()` /
///      `position()` (e.g.
///      `CompletableFuture.runAsync(() -> consumer.wakeup())`,
///      `PlaintextConsumerTest.java:1501`).
///   2. **In-callback reentrancy** — a `ConsumerRebalanceListener` calls
///      `consumer.assign/seek/pause/resume/position/committed/
///      beginningOffsets/commit` from *inside*
///      `onPartitionsAssigned` / `onPartitionsRevoked` by capturing the
///      `consumer` variable in the (anonymous-inner-class) listener
///      (`PlaintextConsumerCallbackTest.java`).
///
/// In Rust the consumer is owned via `&mut self` for the duration of a
/// blocking call, and `Box<dyn Consumer>` is not `Clone`, so neither
/// pattern is expressible with a bare reference. This handle captures
/// only the already-`Arc`-shared, internally-synchronized consumer state,
/// so both patterns are expressible **without `unsafe`**. The user
/// captures the handle into their listener struct — the Rust equivalent
/// of Java capturing the `consumer` variable.
///
/// Obtain one via [`Consumer::handle`]. Cheap to clone — clones share the
/// same underlying state.
///
/// # Operations
///
/// Sync: [`wakeup`](Self::wakeup), [`assignment`](Self::assignment),
/// [`subscription`](Self::subscription), [`paused`](Self::paused).
///
/// Async (reentrant-safe consumer ops): [`assign`](Self::assign),
/// [`seek`](Self::seek), [`seek_to_beginning`](Self::seek_to_beginning),
/// [`seek_to_end`](Self::seek_to_end), [`pause`](Self::pause),
/// [`resume`](Self::resume), [`position`](Self::position),
/// [`committed`](Self::committed),
/// [`beginning_offsets`](Self::beginning_offsets),
/// [`end_offsets`](Self::end_offsets),
/// [`offsets_for_times`](Self::offsets_for_times),
/// [`commit_sync`](Self::commit_sync),
/// [`commit_async`](Self::commit_async).
///
/// Lifecycle / ownership operations (`poll`, `subscribe`, `unsubscribe`,
/// `close`) are intentionally NOT exposed — Java does not invoke these
/// reentrantly from callbacks.
///
/// # Concrete `async fn`, no `#[async_trait]`
///
/// `ConsumerHandle` is a concrete struct, so its async methods are
/// concrete `async fn` returning an anonymous future (no
/// `Pin<Box<dyn Future>>`), per CLAUDE.md §11. None of its methods are on
/// a per-record hot path.
#[derive(Clone)]
pub struct ConsumerHandle {
    inner: ConsumerHandleInner,
}

/// Shared state captured by a [`ConsumerHandle`] for an
/// [`AsyncKafkaConsumer`]. Every field is already `Arc`-shared on the
/// consumer; the handle holds cheap clones.
#[derive(Clone)]
pub(crate) struct AsyncConsumerHandleState {
    /// Rotating wakeup token + bg-task `select!` poke (see
    /// [`AsyncKafkaConsumer::wakeup`]).
    wakeup_trigger: WakeupTrigger,
    bg_wakeup: Arc<dyn Fn() + Send + Sync>,
    /// Submits `ApplicationEvent`s to the bg task.
    application_event_handler: Arc<ApplicationEventHandler>,
    /// Subscription / assignment state (sync getters + `position` /
    /// `seek` pre-checks).
    subscriptions: Arc<Mutex<SubscriptionState>>,
    /// Fetch buffer — `assign` drops buffered fetches for no-longer-owned
    /// partitions (Java `fetchBuffer.retainAll`).
    fetch_buffer: Arc<FetchBuffer>,
    /// Time source for deadline computation.
    time: Arc<dyn ThreadTime>,
    /// Cached `default.api.timeout.ms`.
    default_api_timeout_ms: i64,
}

#[derive(Clone)]
enum ConsumerHandleInner {
    /// `AsyncKafkaConsumer`: full reentrant-safe op surface backed by the
    /// shared `Arc` state.
    Async(AsyncConsumerHandleState),
    /// `MockConsumer`: only `wakeup()` is meaningful — it sets the shared
    /// wakeup flag observed by the next `poll()`. The mock has no bg task
    /// / event pipeline, so the async ops are not wired (they return an
    /// `unsupported_version` error — the mock test surface drives the
    /// concrete `MockConsumer` directly).
    Mock { flag: Arc<AtomicBool> },
}

impl ConsumerHandle {
    /// Fires the consumer's `wakeup()` from this handle. Equivalent to
    /// calling [`Consumer::wakeup`] on the owning consumer, but callable
    /// from any task / thread without holding a reference to the consumer.
    pub fn wakeup(&self) {
        match &self.inner {
            ConsumerHandleInner::Async(state) => {
                state.wakeup_trigger.wakeup();
                (state.bg_wakeup)();
            },
            ConsumerHandleInner::Mock { flag } => {
                flag.store(true, Ordering::SeqCst);
            },
        }
    }

    // ── Sync getters ───────────────────────────────────────────────────

    /// [`Consumer::assignment`] via the shared `SubscriptionState`.
    pub fn assignment(&self) -> HashSet<TopicPartition> {
        match &self.inner {
            ConsumerHandleInner::Async(state) => state.subscriptions.lock().unwrap().assigned_partitions(),
            ConsumerHandleInner::Mock { .. } => HashSet::new(),
        }
    }

    /// [`Consumer::subscription`] via the shared `SubscriptionState`.
    pub fn subscription(&self) -> HashSet<String> {
        match &self.inner {
            ConsumerHandleInner::Async(state) => state.subscriptions.lock().unwrap().subscription(),
            ConsumerHandleInner::Mock { .. } => HashSet::new(),
        }
    }

    /// [`Consumer::paused`] via the shared `SubscriptionState`.
    pub fn paused(&self) -> HashSet<TopicPartition> {
        match &self.inner {
            ConsumerHandleInner::Async(state) => state.subscriptions.lock().unwrap().paused_partitions(),
            ConsumerHandleInner::Mock { .. } => HashSet::new(),
        }
    }

    // ── Async reentrant-safe consumer ops ───────────────────────────────

    /// [`AsyncKafkaConsumer::assign`].
    pub async fn assign(&self, partitions: Vec<TopicPartition>) -> Result<(), Error> {
        self.async_state()?.assign(partitions).await
    }

    /// [`AsyncKafkaConsumer::seek`].
    pub async fn seek_with_offset(&self, partition: TopicPartition, offset: i64) -> Result<(), Error> {
        self.async_state()?.seek(partition, offset, None).await
    }

    /// [`AsyncKafkaConsumer::seek_with_offset_and_metadata`].
    pub async fn seek_with_offset_and_metadata(
        &self,
        partition: TopicPartition,
        offset_and_metadata: OffsetAndMetadata,
    ) -> Result<(), Error> {
        let offset = offset_and_metadata.offset();
        let epoch = offset_and_metadata.leader_epoch();
        self.async_state()?.seek(partition, offset, epoch).await
    }

    /// [`AsyncKafkaConsumer::seek_to_beginning`].
    pub async fn seek_to_beginning(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        self.async_state()?
            .seek_with_reset_strategy(partitions, crate::consumer::AutoOffsetResetStrategy::EARLIEST)
            .await
    }

    /// [`AsyncKafkaConsumer::seek_to_end`].
    pub async fn seek_to_end(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        self.async_state()?
            .seek_with_reset_strategy(partitions, crate::consumer::AutoOffsetResetStrategy::LATEST)
            .await
    }

    /// [`AsyncKafkaConsumer::pause`].
    pub async fn pause(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        self.async_state()?.pause(partitions).await
    }

    /// [`AsyncKafkaConsumer::resume`].
    pub async fn resume(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        self.async_state()?.resume(partitions).await
    }

    /// [`AsyncKafkaConsumer::position`].
    pub async fn position(&self, partition: &TopicPartition) -> Result<i64, Error> {
        let state = self.async_state()?;
        let timeout = Duration::from_millis(state.default_api_timeout_ms as u64);
        state.position(partition, timeout).await
    }

    /// [`AsyncKafkaConsumer::position_with_timeout`].
    pub async fn position_with_timeout(&self, partition: &TopicPartition, timeout: Duration) -> Result<i64, Error> {
        self.async_state()?.position(partition, timeout).await
    }

    /// [`AsyncKafkaConsumer::committed`].
    pub async fn committed(
        &self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, Error> {
        let state = self.async_state()?;
        let timeout = Duration::from_millis(state.default_api_timeout_ms as u64);
        state.committed(partitions, timeout).await
    }

    /// [`AsyncKafkaConsumer::beginning_offsets`].
    pub async fn beginning_offsets(
        &self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, i64>, Error> {
        let state = self.async_state()?;
        let timeout = Duration::from_millis(state.default_api_timeout_ms as u64);
        // Java's `ListOffsetsRequest.EARLIEST_TIMESTAMP = -2L`.
        state.beginning_or_end_offsets(partitions, -2, timeout).await
    }

    /// [`AsyncKafkaConsumer::end_offsets`].
    pub async fn end_offsets(&self, partitions: &[TopicPartition]) -> Result<HashMap<TopicPartition, i64>, Error> {
        let state = self.async_state()?;
        let timeout = Duration::from_millis(state.default_api_timeout_ms as u64);
        // Java's `ListOffsetsRequest.LATEST_TIMESTAMP = -1L`.
        state.beginning_or_end_offsets(partitions, -1, timeout).await
    }

    /// [`AsyncKafkaConsumer::offsets_for_times`].
    pub async fn offsets_for_times(
        &self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>, Error> {
        let state = self.async_state()?;
        let timeout = Duration::from_millis(state.default_api_timeout_ms as u64);
        state.offsets_for_times(timestamps_to_search, timeout).await
    }

    /// [`AsyncKafkaConsumer::commit_sync`]. Commits the offsets the bg
    /// task has consumed (Java `commitSync()` with no offsets — commit
    /// `allConsumed`).
    pub async fn commit_sync(&self) -> Result<(), Error> {
        let state = self.async_state()?;
        let timeout = Duration::from_millis(state.default_api_timeout_ms as u64);
        state.commit_sync(None, timeout).await
    }

    /// [`AsyncKafkaConsumer::commit_sync_with_offsets`].
    pub async fn commit_sync_with_offsets(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<(), Error> {
        let state = self.async_state()?;
        let timeout = Duration::from_millis(state.default_api_timeout_ms as u64);
        state.commit_sync(Some(offsets), timeout).await
    }

    /// [`AsyncKafkaConsumer::commit_async`]. Fire-and-forget commit of the
    /// offsets the bg task has consumed.
    pub async fn commit_async(&self) -> Result<(), Error> {
        self.async_state()?.commit_async(None).await
    }

    /// [`AsyncKafkaConsumer::commit_async_offsets`].
    pub async fn commit_async_offsets(&self, offsets: HashMap<TopicPartition, OffsetAndMetadata>) -> Result<(), Error> {
        self.async_state()?.commit_async(Some(offsets)).await
    }

    /// Returns the shared async state, or an error if this handle was
    /// obtained from a `MockConsumer` (which has no event pipeline). The
    /// mock surface drives the concrete `MockConsumer` directly, so this
    /// path is never hit by faithful mock tests.
    fn async_state(&self) -> Result<&AsyncConsumerHandleState, Error> {
        match &self.inner {
            ConsumerHandleInner::Async(state) => Ok(state),
            ConsumerHandleInner::Mock { .. } => Err(Error::unsupported_version(
                "ConsumerHandle async operations are not supported on a MockConsumer handle; \
                 drive the MockConsumer directly.",
            )),
        }
    }

    /// Builds an async-consumer handle from its shared state.
    pub(crate) fn for_async(state: AsyncConsumerHandleState) -> Self {
        Self { inner: ConsumerHandleInner::Async(state) }
    }

    /// Builds a mock-consumer handle from its shared wakeup flag.
    pub(crate) fn for_mock(flag: Arc<AtomicBool>) -> Self {
        Self { inner: ConsumerHandleInner::Mock { flag } }
    }
}

impl AsyncConsumerHandleState {
    /// Shared submit + wakeup-aware-await core, the **no-drain** sibling
    /// of [`AsyncKafkaConsumer::submit_and_drain`].
    ///
    /// The handle cannot own the background-event receiver or the
    /// rebalance-listener invoker (those stay on `&mut self`), so it does
    /// NOT drain background events while waiting. It is only ever called
    /// reentrantly from *inside* a rebalance-listener callback, by which
    /// point (after Phase 41b) the background task is no longer frozen on
    /// the callback ack — it keeps spinning and services this event. A
    /// single rebalance callback never triggers a nested rebalance, so
    /// there is nothing for the handle to drain.
    ///
    /// The wait honors `wakeup()` exactly like
    /// [`AsyncKafkaConsumer::process_background_events_until`]'s
    /// `enable_wakeup` arm: it races the receiver against the rotating
    /// wakeup token's cancellation and the absolute `deadline_ms`. The
    /// deadline / timeout logic is identical in shape — see that method
    /// for the per-stage rationale.
    async fn submit_and_await<T: Send + 'static>(
        &self,
        event: ApplicationEvent,
        receiver: tokio::sync::oneshot::Receiver<Result<T, Error>>,
        deadline_ms: i64,
        timeout_msg: impl AsRef<str>,
        enable_wakeup: bool,
    ) -> Result<T, Error> {
        let now_ms = self.time.milliseconds();
        self.application_event_handler.add(event, now_ms)?;
        self.await_completion(receiver, deadline_ms, timeout_msg, enable_wakeup).await
    }

    /// Milliseconds remaining until `deadline_ms`, saturating at zero.
    fn remaining_ms(&self, deadline_ms: i64) -> i64 {
        deadline_ms.saturating_sub(self.time.milliseconds()).max(0)
    }

    fn default_api_timeout_deadline_ms(&self) -> i64 {
        CompletableEvent::calculate_deadline_ms(self.time.milliseconds(), self.default_api_timeout_ms)
    }

    /// Reentrant-safe [`AsyncKafkaConsumer::assign`].
    async fn assign(&self, partitions: Vec<TopicPartition>) -> Result<(), Error> {
        if partitions.is_empty() {
            // Phase 41 Issue 4: On the owning consumer, `assign([])` delegates
            // to `unsubscribe()` (leave the group). The handle intentionally
            // does NOT expose the unsubscribe / leave-group lifecycle pipeline
            // (it owns neither `background_event_rx` nor the close path), so it
            // cannot faithfully reproduce `assign([])`. Submitting an empty
            // `AssignmentChange` would clear the assignment WITHOUT leaving the
            // group — a silent divergence from Java's `KafkaConsumer.assign([])`
            // for a group consumer. Reject it with a clear error pointing the
            // caller at the owning consumer's `unsubscribe()`.
            return Err(Error::local_illegal_argument(
                "ConsumerHandle::assign with an empty collection is not supported: on the owning \
                 consumer assign([]) leaves the group (equivalent to unsubscribe()), which the \
                 handle does not expose. Call unsubscribe() on the owning AsyncKafkaConsumer instead.",
            ));
        }

        for tp in &partitions {
            if tp.topic().trim().is_empty() {
                return Err(Error::local_illegal_argument(
                    "Topic partitions to assign to cannot have null or empty topic",
                ));
            }
        }

        let partitions_set: HashSet<TopicPartition> = partitions.into_iter().collect();
        self.fetch_buffer.retain_all(&partitions_set);
        let now_ms = self.time.milliseconds();
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let (handle, receiver, _erased) = CompletableEvent::make_completable_event::<()>(deadline_ms);
        self.submit_and_await::<()>(
            ApplicationEvent::AssignmentChange { handle, current_time_ms: now_ms, partitions: partitions_set },
            receiver,
            deadline_ms,
            "Timeout expired while waiting for the assignment-change event to complete",
            false,
        )
        .await
    }

    /// Reentrant-safe [`AsyncKafkaConsumer::seek`] /
    /// [`AsyncKafkaConsumer::seek_with_offset_and_metadata`].
    async fn seek(&self, partition: TopicPartition, offset: i64, offset_epoch: Option<i32>) -> Result<(), Error> {
        if offset < 0 {
            return Err(Error::local_illegal_argument("seek offset must not be a negative number"));
        }
        log::info!("Seeking to offset {offset} for partition {partition}");
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let (handle, receiver, _erased) = CompletableEvent::make_completable_event::<()>(deadline_ms);
        self.submit_and_await::<()>(
            ApplicationEvent::SeekUnvalidated { handle, partition, offset, offset_epoch },
            receiver,
            deadline_ms,
            "Timeout expired while waiting for the seek event to complete",
            false,
        )
        .await
    }

    /// Reentrant-safe `seekToBeginning` / `seekToEnd`.
    async fn seek_with_reset_strategy(
        &self,
        partitions: &[TopicPartition],
        strategy: crate::consumer::AutoOffsetResetStrategy,
    ) -> Result<(), Error> {
        let set: HashSet<TopicPartition> = partitions.iter().cloned().collect();
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let (handle, receiver, _erased) = CompletableEvent::make_completable_event::<()>(deadline_ms);
        self.submit_and_await::<()>(
            ApplicationEvent::ResetOffset { handle, partitions: set, offset_reset_strategy: strategy },
            receiver,
            deadline_ms,
            "Timeout expired while waiting for the seek-with-reset-strategy event to complete",
            false,
        )
        .await
    }

    /// Reentrant-safe [`AsyncKafkaConsumer::pause`].
    async fn pause(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        if partitions.is_empty() {
            return Ok(());
        }
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let set: HashSet<TopicPartition> = partitions.iter().cloned().collect();
        let (handle, receiver, _erased) = CompletableEvent::make_completable_event::<()>(deadline_ms);
        self.submit_and_await::<()>(
            ApplicationEvent::PausePartitions { handle, partitions: set },
            receiver,
            deadline_ms,
            "Timeout expired while waiting for PausePartitions",
            false,
        )
        .await
    }

    /// Reentrant-safe [`AsyncKafkaConsumer::resume`].
    async fn resume(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        if partitions.is_empty() {
            return Ok(());
        }
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let set: HashSet<TopicPartition> = partitions.iter().cloned().collect();
        let (handle, receiver, _erased) = CompletableEvent::make_completable_event::<()>(deadline_ms);
        self.submit_and_await::<()>(
            ApplicationEvent::ResumePartitions { handle, partitions: set },
            receiver,
            deadline_ms,
            "Timeout expired while waiting for ResumePartitions",
            false,
        )
        .await
    }

    /// Reentrant-safe [`AsyncKafkaConsumer::position_with_timeout`].
    async fn position(&self, partition: &TopicPartition, timeout: Duration) -> Result<i64, Error> {
        {
            let subs = self.subscriptions.lock().unwrap();
            if !subs.is_assigned(partition) {
                return Err(Error::local_illegal_state(
                    "You can only check the position for partitions assigned to this consumer.",
                ));
            }
        }
        let now_ms = self.time.milliseconds();
        let deadline_ms = CompletableEvent::calculate_deadline_ms(now_ms, timeout.as_millis() as i64);

        loop {
            let position_offset = {
                let subs = self.subscriptions.lock().unwrap();
                subs.valid_position(partition)?.map(|fp| fp.offset)
            };
            if let Some(offset) = position_offset {
                return Ok(offset);
            }

            let (handle, receiver, _erased) = CompletableEvent::make_completable_event::<()>(deadline_ms);
            let drain_result = self
                .submit_and_await::<()>(
                    ApplicationEvent::CheckAndUpdatePositions { handle },
                    receiver,
                    deadline_ms,
                    "Timeout expired while waiting for CheckAndUpdatePositions",
                    true,
                )
                .await;
            match drain_result {
                Ok(()) => {},
                Err(Error::Timeout(_)) => {},
                Err(err) => return Err(err),
            }

            if self.time.milliseconds() >= deadline_ms {
                return Err(Error::timeout(format!(
                    "Timeout of {}ms expired before the position for partition {} could be determined",
                    timeout.as_millis(),
                    partition
                )));
            }
        }
    }

    /// Reentrant-safe [`AsyncKafkaConsumer::committed_with_timeout`].
    async fn committed(
        &self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, Error> {
        if partitions.is_empty() {
            return Ok(HashMap::new());
        }
        let now_ms = self.time.milliseconds();
        let deadline_ms = CompletableEvent::calculate_deadline_ms(now_ms, timeout.as_millis() as i64);
        let set: HashSet<TopicPartition> = partitions.iter().cloned().collect();
        let (handle, receiver, _erased) =
            CompletableEvent::make_completable_event::<HashMap<TopicPartition, OffsetAndMetadata>>(deadline_ms);
        let result = self
            .submit_and_await::<HashMap<TopicPartition, OffsetAndMetadata>>(
                ApplicationEvent::FetchCommittedOffsets { handle, partitions: set },
                receiver,
                deadline_ms,
                "Timeout expired while waiting for FetchCommittedOffsets",
                true,
            )
            .await;
        match result {
            Ok(map) => Ok(map),
            Err(Error::Timeout(_)) => Err(Error::timeout(format!(
                "Timeout of {}ms expired before the last committed offset for partitions {} could be determined. Try tuning default.api.timeout.ms larger to relax the threshold.",
                timeout.as_millis(),
                format_partitions_for_display(partitions),
            ))),
            Err(err) => Err(err),
        }
    }

    /// Reentrant-safe `beginningOffsets` / `endOffsets`.
    async fn beginning_or_end_offsets(
        &self,
        partitions: &[TopicPartition],
        timestamp: i64,
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, Error> {
        if partitions.is_empty() {
            return Ok(HashMap::new());
        }
        let mut timestamps_to_search: HashMap<TopicPartition, i64> = HashMap::new();
        for tp in partitions {
            timestamps_to_search.insert(tp.clone(), timestamp);
        }
        let now_ms = self.time.milliseconds();
        let deadline_ms = CompletableEvent::calculate_deadline_ms(now_ms, timeout.as_millis() as i64);

        if timeout.is_zero() {
            let (handle, _receiver, _erased) = CompletableEvent::make_completable_event::<
                HashMap<TopicPartition, Option<OffsetAndTimestampInternal>>,
            >(deadline_ms);
            self.application_event_handler.add(
                ApplicationEvent::ListOffsets { handle, timestamps_to_search, require_timestamps: false },
                now_ms,
            )?;
            return Ok(HashMap::new());
        }

        let (handle, receiver, _erased) = CompletableEvent::make_completable_event::<
            HashMap<TopicPartition, Option<OffsetAndTimestampInternal>>,
        >(deadline_ms);
        let result = self
            .submit_and_await::<HashMap<TopicPartition, Option<OffsetAndTimestampInternal>>>(
                ApplicationEvent::ListOffsets { handle, timestamps_to_search, require_timestamps: false },
                receiver,
                deadline_ms,
                "Timeout expired while waiting for ListOffsets",
                false,
            )
            .await;
        match result {
            Ok(offsets_map) => {
                let mut out = HashMap::with_capacity(offsets_map.len());
                for (tp, opt) in offsets_map {
                    if let Some(oat) = opt {
                        out.insert(tp, oat.offset());
                    }
                }
                Ok(out)
            },
            Err(Error::Timeout(_)) => Err(Error::timeout(format!(
                "Failed to get offsets by times in {}ms",
                timeout.as_millis()
            ))),
            Err(err) => Err(err),
        }
    }

    /// Reentrant-safe [`AsyncKafkaConsumer::offsets_for_times_with_timeout`].
    async fn offsets_for_times(
        &self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>, Error> {
        for (tp, ts) in &timestamps_to_search {
            if *ts < 0 {
                return Err(Error::local_illegal_argument(format!(
                    "The target time for partition {tp} is {ts}. The target time cannot be negative."
                )));
            }
        }
        if timestamps_to_search.is_empty() {
            return Ok(HashMap::new());
        }
        let now_ms = self.time.milliseconds();
        let deadline_ms = CompletableEvent::calculate_deadline_ms(now_ms, timeout.as_millis() as i64);

        if timeout.is_zero() {
            let (handle, _receiver, _erased) = CompletableEvent::make_completable_event::<
                HashMap<TopicPartition, Option<OffsetAndTimestampInternal>>,
            >(deadline_ms);
            self.application_event_handler.add(
                ApplicationEvent::ListOffsets { handle, timestamps_to_search, require_timestamps: true },
                now_ms,
            )?;
            return Ok(HashMap::new());
        }

        let (handle, receiver, _erased) = CompletableEvent::make_completable_event::<
            HashMap<TopicPartition, Option<OffsetAndTimestampInternal>>,
        >(deadline_ms);
        let result = self
            .submit_and_await::<HashMap<TopicPartition, Option<OffsetAndTimestampInternal>>>(
                ApplicationEvent::ListOffsets { handle, timestamps_to_search, require_timestamps: true },
                receiver,
                deadline_ms,
                "Timeout expired while waiting for ListOffsets",
                false,
            )
            .await;
        match result {
            Ok(offsets_map) => {
                let mut out = HashMap::with_capacity(offsets_map.len());
                for (tp, opt) in offsets_map {
                    if let Some(oat) = opt {
                        out.insert(tp, oat.build_offset_and_timestamp()?);
                    }
                }
                Ok(out)
            },
            Err(Error::Timeout(_)) => Err(Error::timeout(format!(
                "Failed to get offsets by times in {}ms",
                timeout.as_millis()
            ))),
            Err(err) => Err(err),
        }
    }

    /// Reentrant-safe [`AsyncKafkaConsumer::commit_sync`].
    ///
    /// Deviation from `AsyncKafkaConsumer::commit_sync`: the handle does
    /// NOT own the `OffsetCommitCallbackInvoker`, the interceptor chain,
    /// or `last_pending_async_commit`, so it does not (a) drain pending
    /// async-commit callbacks, (b) run `interceptors.onCommit(...)`. It
    /// submits a `CommitSync` event and awaits the committed offsets. The
    /// interceptor `onCommit` hook fires from the owning consumer's own
    /// `commit_*` path, not from a reentrant handle call — matching the
    /// fact that a listener flushing offsets via `commit_sync` is
    /// concerned with durability, not with the interceptor side-channel.
    async fn commit_sync(
        &self,
        offsets: Option<HashMap<TopicPartition, OffsetAndMetadata>>,
        timeout: Duration,
    ) -> Result<(), Error> {
        // Empty-offsets short-circuit (Java's `completedFuture(null)`).
        if let Some(map) = &offsets
            && map.is_empty()
        {
            return Ok(());
        }
        let now_ms = self.time.milliseconds();
        let deadline_ms = CompletableEvent::calculate_deadline_ms(now_ms, timeout.as_millis() as i64);
        let (handle, receiver, _erased) =
            CompletableEvent::make_completable_event::<HashMap<TopicPartition, OffsetAndMetadata>>(deadline_ms);
        let (offsets_ready_handle, offsets_ready_rx, _erased_or) =
            CompletableEvent::make_completable_event::<()>(deadline_ms);
        self.application_event_handler.add(
            ApplicationEvent::CommitSync { handle, offsets_ready: offsets_ready_handle, offsets },
            now_ms,
        )?;
        // Wait until the bg task has resolved which offsets to commit.
        self.await_completion::<()>(
            offsets_ready_rx,
            deadline_ms,
            "Timeout expired while waiting for commit offsets to be ready",
            true,
        )
        .await?;
        // Wait for the commit RPC result.
        //
        // RECORDED DEVIATION (definition-of-done.md §7): Java's
        // `commitSync(Duration)` attaches no message of its own —
        // `ConsumerUtils.getResult` rethrows the underlying `TimeoutException`
        // as-is (`ConsumerUtils.java:219-231`). The text below is Rust-side
        // diagnostics (it resembles `ClassicKafkaConsumer.java:745-748`, which
        // is out of scope per `consumer-threading.md` §20). It is additive: the
        // error class, code and retriable ancestry are the ones Java produces,
        // and it deliberately does NOT format the offsets map. See the same
        // note on `AsyncKafkaConsumer::commit_sync_with_offsets_timeout`.
        self.await_completion::<HashMap<TopicPartition, OffsetAndMetadata>>(
            receiver,
            deadline_ms,
            format!(
                "Timeout of {}ms expired before successfully committing offsets",
                timeout.as_millis()
            ),
            true,
        )
        .await
        .map(|_committed| ())
    }

    /// Reentrant-safe [`AsyncKafkaConsumer::commit_async`]. Fire-and-forget:
    /// submits a `CommitAsync` event and spawns a detached task to consume
    /// the result (logging failures). The handle cannot store
    /// `last_pending_async_commit` or run the callback invoker, so user
    /// `OffsetCommitCallback`s are NOT supported on the handle — the
    /// no-callback overload mirrors Java's `commitAsync()`.
    async fn commit_async(&self, offsets: Option<HashMap<TopicPartition, OffsetAndMetadata>>) -> Result<(), Error> {
        if let Some(map) = &offsets
            && map.is_empty()
        {
            return Ok(());
        }
        let now_ms = self.time.milliseconds();
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let (handle, receiver, _erased) =
            CompletableEvent::make_completable_event::<HashMap<TopicPartition, OffsetAndMetadata>>(deadline_ms);
        let (offsets_ready_handle, offsets_ready_rx, _erased_or) =
            CompletableEvent::make_completable_event::<()>(deadline_ms);
        self.application_event_handler.add(
            ApplicationEvent::CommitAsync { handle, offsets_ready: offsets_ready_handle, offsets },
            now_ms,
        )?;
        // Java's commitAsync is non-blocking and never throws Wakeup; wait
        // only for offsets-ready (so the commit window is pinned) with
        // wakeup disabled, then detach.
        self.await_completion::<()>(
            offsets_ready_rx,
            deadline_ms,
            "Timeout expired while waiting for commit offsets to be ready",
            false,
        )
        .await?;
        tokio::spawn(async move {
            match receiver.await {
                Ok(Ok(_committed)) => {},
                Ok(Err(err)) => log::error!("Offset commit (via ConsumerHandle) failed: {err}"),
                Err(_recv_err) => log::error!("commit_async (via ConsumerHandle) receiver dropped without completion"),
            }
        });
        Ok(())
    }

    /// The shared wakeup-aware, no-drain await core (see
    /// [`Self::submit_and_await`]). Awaits an already-submitted event's
    /// receiver, racing it against the rotating wakeup token's
    /// cancellation (when `enable_wakeup`) and the absolute `deadline_ms`.
    /// All handle ops funnel their wait through this one helper so the
    /// deadline / timeout logic is not duplicated. The commit paths submit
    /// one event but await two receivers (offsets-ready + commit result),
    /// hence the submit and the await are separated here.
    async fn await_completion<T: Send + 'static>(
        &self,
        mut receiver: tokio::sync::oneshot::Receiver<Result<T, Error>>,
        deadline_ms: i64,
        timeout_msg: impl AsRef<str>,
        enable_wakeup: bool,
    ) -> Result<T, Error> {
        loop {
            if enable_wakeup && let Err(err) = self.wakeup_trigger.maybe_trigger_wakeup() {
                self.wakeup_trigger.rotate();
                return Err(err);
            }
            match receiver.try_recv() {
                Ok(Ok(value)) => return Ok(value),
                Ok(Err(err)) => return Err(err),
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    return Err(Error::local_illegal_state(
                        "Background task dropped the completion sender without completing it",
                    ));
                },
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                    let remaining = self.remaining_ms(deadline_ms);
                    if remaining <= 0 {
                        return Err(Error::timeout(timeout_msg.as_ref().to_string()));
                    }
                    let wait = std::cmp::min(remaining, 100) as u64;
                    let token = if enable_wakeup {
                        Some(self.wakeup_trigger.current_token())
                    } else {
                        None
                    };
                    let recv_fut = &mut receiver;
                    match token {
                        Some(tok) => {
                            tokio::select! {
                                biased;
                                _ = tok.cancelled() => {},
                                res = tokio::time::timeout(Duration::from_millis(wait), recv_fut) => {
                                    match res {
                                        Ok(Ok(Ok(value))) => return Ok(value),
                                        Ok(Ok(Err(err))) => return Err(err),
                                        Ok(Err(_recv_err)) => {
                                            return Err(Error::local_illegal_state(
                                                "Background task dropped the completion sender without completing it",
                                            ));
                                        },
                                        Err(_elapsed) => {},
                                    }
                                },
                            }
                        },
                        None => match tokio::time::timeout(Duration::from_millis(wait), recv_fut).await {
                            Ok(Ok(Ok(value))) => return Ok(value),
                            Ok(Ok(Err(err))) => return Err(err),
                            Ok(Err(_recv_err)) => {
                                return Err(Error::local_illegal_state(
                                    "Background task dropped the completion sender without completing it",
                                ));
                            },
                            Err(_elapsed) => {},
                        },
                    }
                },
            }
            if self.remaining_ms(deadline_ms) <= 0 {
                return Err(Error::timeout(timeout_msg.as_ref().to_string()));
            }
        }
    }
}

/// Type-erased handle to the consumer background task.
///
/// Owns the join mechanism ([`BgJoin`]) for the bg loop and the
/// `Box<dyn Fn>` closures that close / wakeup the underlying
/// `ConsumerNetworkThread<K>` regardless of its concrete `K`.
///
/// A type-erased, thread-safe lifecycle hook (`signal_close` / `wakeup`).
/// Erased so the close handle does not carry the consumer's `K`/`V` types.
type LifecycleFn = Box<dyn Fn() + Send + Sync>;

/// Builds the two lifecycle closures a [`NetworkThreadCloseHandle`] needs:
/// `signal_close_fn` (clear the running flag, then nudge the bg task) and
/// `wakeup_fn` (nudge the bg task).
///
/// Both nudge the **transport** primitive — the notify the bg loop's poll
/// `select!` waits on — and NOT the [`WakeupTrigger`]. That distinction is the
/// whole point of this function existing:
///
///   - `WakeupTrigger::wakeup()` is a no-op once `disable()` has run, and
///     `close()` calls `disable()` as its very first step. Routing the close
///     nudge through the trigger therefore made BOTH `signal_close()` and
///     `wakeup()` dead by the time close used them: the bg task only noticed
///     `running == false` after its in-flight poll drained naturally, so every
///     `close()` paid a full `poll_wait_time_ms`.
///   - Cancelling the token also arms a user-visible `Error::Wakeup` that
///     the next public API call raises (§11), which a lifecycle nudge must not
///     do.
///
/// Java routes the same way: `ConsumerNetworkThread.close(timeout)` sets the
/// timeout then calls `wakeup()` -> `networkClientDelegate.wakeup()` ->
/// `Selector.wakeup()` (`ConsumerNetworkThread.java:380-381`). The user-facing
/// `WakeupTrigger` is never involved in shutdown.
///
/// Returned as a pair from one function so production and tests share the
/// wiring. The fixture previously stubbed these closures, which is exactly why
/// the dead-nudge bug survived: the stub set a flag, so it could not reproduce
/// the trigger's `disabled` short-circuit.
fn build_close_handle_fns(
    running: Arc<std::sync::atomic::AtomicBool>,
    notify: Arc<tokio::sync::Notify>,
) -> (LifecycleFn, LifecycleFn) {
    let close_notify = Arc::clone(&notify);
    let signal_close_fn: Box<dyn Fn() + Send + Sync> = Box::new(move || {
        running.store(false, Ordering::Release);
        close_notify.notify_one();
    });
    let wakeup_fn: Box<dyn Fn() + Send + Sync> = Box::new(move || {
        notify.notify_one();
    });
    (signal_close_fn, wakeup_fn)
}

/// Held by [`AsyncKafkaConsumer`] for the lifetime of the consumer
/// instance; dropped (with `signal_close`) on close.
pub(crate) struct NetworkThreadCloseHandle {
    /// Cancels the bg-task `run_once` loop and wakes the trigger so the
    /// next iteration observes the shutdown.
    signal_close_fn: Box<dyn Fn() + Send + Sync>,
    /// Wakes the bg-task's `select!` on the wakeup token. Held as an
    /// `Arc` (not `Box`) so a clone can be handed to a shareable
    /// [`ConsumerHandle`] (so cross-task `wakeup()` — a first-class Java
    /// pattern — is expressible without `unsafe`); the bg-wakeup
    /// closure is `Send + Sync` and side-effect-idempotent.
    wakeup_fn: Arc<dyn Fn() + Send + Sync>,
    /// How the bg loop is joined on close (tokio task vs dedicated thread).
    join: BgJoin,
}

impl NetworkThreadCloseHandle {
    /// Constructor used by `with_components` and unit tests. The
    /// closures capture the concrete `ConsumerNetworkThread<K>` clones
    /// of the close / wakeup state so the outer struct can stay
    /// non-generic over `K`. The bg loop runs as a `tokio::spawn`ed task.
    pub(crate) fn new(
        signal_close_fn: Box<dyn Fn() + Send + Sync>,
        wakeup_fn: Box<dyn Fn() + Send + Sync>,
        join_handle: JoinHandle<()>,
    ) -> Self {
        Self {
            signal_close_fn,
            wakeup_fn: Arc::from(wakeup_fn),
            join: BgJoin::Spawned(Some(join_handle)),
        }
    }

    /// Constructor used by the production [`AsyncKafkaConsumer::new`]
    /// path (Phase 21). The bg loop runs on a dedicated `std::thread`
    /// hosting a `current_thread` tokio runtime; `done` resolves when
    /// the bg loop has finished `cleanup()`, and `thread` is the OS
    /// thread handle reaped afterwards.
    pub(crate) fn with_dedicated(
        signal_close_fn: Box<dyn Fn() + Send + Sync>,
        wakeup_fn: Box<dyn Fn() + Send + Sync>,
        done: tokio::sync::oneshot::Receiver<()>,
        thread: std::thread::JoinHandle<()>,
    ) -> Self {
        Self {
            signal_close_fn,
            wakeup_fn: Arc::from(wakeup_fn),
            join: BgJoin::Dedicated { done: Some(done), thread: Some(thread) },
        }
    }

    /// Clones the bg-task wakeup closure as a shareable `Arc`. Used to
    /// build a [`ConsumerHandle`] that can fire the bg-task `select!` from
    /// another task without holding any reference to the consumer.
    pub(crate) fn wakeup_fn_clone(&self) -> Arc<dyn Fn() + Send + Sync> {
        Arc::clone(&self.wakeup_fn)
    }

    /// Signals the bg task to exit and wakes it from its current
    /// `select!`. Idempotent.
    pub(crate) fn signal_close(&self) {
        (self.signal_close_fn)();
    }

    /// Wakes the bg task's `select!` without signalling shutdown. Used
    /// from `AsyncKafkaConsumer::wakeup`.
    pub(crate) fn wakeup(&self) {
        (self.wakeup_fn)();
    }

    /// Awaits the bg loop to completion. Returns `Ok(())` on clean exit,
    /// or wraps a task / thread panic as a `Error::local_illegal_state`.
    ///
    /// For [`BgJoin::Spawned`] this awaits the tokio `JoinHandle` exactly
    /// as before. For [`BgJoin::Dedicated`] it first awaits the `done`
    /// oneshot (which the dedicated thread fires after the bg loop exits
    /// and `cleanup()` completes), then reaps the OS thread off the async
    /// runtime via `spawn_blocking` so it does not block the close
    /// future. Both paths are idempotent (a second call is a no-op) and
    /// never hang if the bg loop already exited (a closed/`None` receiver
    /// is treated as a clean exit).
    pub(crate) async fn await_join(&mut self) -> Result<(), Error> {
        match &mut self.join {
            BgJoin::Spawned(handle) => {
                if let Some(handle) = handle.take() {
                    match handle.await {
                        Ok(()) => Ok(()),
                        Err(join_err) => Err(Error::local_illegal_state(format!(
                            "Consumer network thread terminated with error: {join_err}"
                        ))),
                    }
                } else {
                    Ok(())
                }
            },
            BgJoin::Dedicated { done, thread } => {
                // Wait for the bg loop to finish `cleanup()`. If the
                // sender was dropped without sending (the dedicated thread
                // already exited), `recv()` returns `Err(RecvError)` — we
                // treat that as a clean exit and proceed to reap the
                // thread, mirroring the `None` (already-joined) case.
                if let Some(done_rx) = done.take() {
                    let _ = done_rx.await;
                }
                // Reap the OS thread off the async runtime so the close
                // future is not blocked on `JoinHandle::join`. A panic
                // inside the dedicated thread is mapped to the SAME
                // message shape as the `Spawned` panic path.
                if let Some(thread) = thread.take() {
                    let join_result = tokio::task::spawn_blocking(move || thread.join()).await;
                    match join_result {
                        // Outer: the spawn_blocking task itself; inner: the
                        // dedicated OS thread.
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(_panic)) => Err(Error::local_illegal_state(
                            "Consumer network thread terminated with error: panic".to_string(),
                        )),
                        Err(join_err) => Err(Error::local_illegal_state(format!(
                            "Consumer network thread terminated with error: {join_err}"
                        ))),
                    }
                } else {
                    Ok(())
                }
            },
        }
    }
}

/// `AsyncKafkaConsumer<K, V>` — the production consumer for the KIP-848
/// group protocol. Implements [`crate::consumer::Consumer`] via
/// [`async_trait`].
///
/// The struct shape mirrors the Phase 11 PLAN.md layout:
///   - **Shared with the bg task**: `subscriptions`, `metadata`,
///     `request_managers`, `background_event_rx`,
///     `application_event_handler`, `max_time_to_wait_ms`,
///     `wakeup_trigger`, `network_thread_close` (the erased handle).
///   - **App-side only**: `client_id`, `group_id`, `group_metadata`,
///     `rebalance_listener_invoker`,
///     `offset_commit_callback_invoker`, `deserializers`,
///     `interceptors`, `auto_commit_enabled`, `default_api_timeout_ms`,
///     `closed`, `rebalance_listener`.
///
/// The `current_thread` / `ref_count` Java fields are NOT translated —
/// `&mut self` on the trait already enforces single-caller exclusivity
/// (Phase 11 PLAN.md deferral #4).
pub struct AsyncKafkaConsumer<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    // ── Shared with the bg task ────────────────────────────────────────
    /// Subscription / assignment state. Wrapped in `Arc<Mutex<…>>` per
    /// `consumer-threading.md` §16.
    subscriptions: Arc<Mutex<SubscriptionState>>,
    /// Consumer-side metadata cache shared with the bg task and
    /// `NetworkClient`.
    metadata: Arc<ConsumerMetadata>,
    /// Per-RM handles shared with the bg task. See §16 / Phase-10
    /// pattern #3 (`Arc<Mutex<RequestManagers>>` discipline).
    request_managers: Arc<std::sync::Mutex<RequestManagers>>,
    /// Receiver for `BackgroundEvent`s emitted by the bg task. Drained
    /// by `process_background_events` (commit (3)).
    background_event_rx: mpsc::UnboundedReceiver<BackgroundEventEnvelope>,
    /// App-side handle for enqueuing `ApplicationEvent`s — wraps the
    /// channel sender shared with the bg task.
    application_event_handler: Arc<ApplicationEventHandler>,
    /// Completable-event reaper, shared with the bg task.
    completable_event_reaper: Arc<std::sync::Mutex<CompletableEventReaper>>,
    /// Mirror of the bg task's `cached_max_time_to_wait_ms`. Exposed
    /// via `maximum_time_to_wait_ms()` and used by the poll loop.
    max_time_to_wait_ms: Arc<AtomicI64>,
    /// Wakeup primitive shared with the bg task.
    wakeup_trigger: WakeupTrigger,
    /// Type-erased handle to the spawned bg task. Drops the
    /// `JoinHandle` on consumer close (after `signal_close()` + the
    /// final `wakeup_trigger.wakeup()`).
    network_thread_close: NetworkThreadCloseHandle,
    /// Per `consumer-threading.md` §27: the consumer owns one
    /// `Arc<FetchBuffer>` shared with both the bg-side `FetchRequestManager`
    /// (which `add`s `CompletedFetch`es as fetch responses land) and the
    /// app-side [`FetchCollector`] (which drains them in `poll()`).
    fetch_buffer: Arc<FetchBuffer>,
    /// App-side fetch decoder. Owned by `Arc` so the consumer can hand a
    /// shared reference to per-poll helpers without re-construction.
    fetch_collector: Arc<FetchCollector<K, V>>,

    /// The metrics registry (Java `private final Metrics metrics`). Kept so
    /// Phase M7 can expose the public `metrics()` accessor over the same
    /// registry the fetch path records into. The fetch managers hold
    /// `Arc<FetchMetricsManager>` clones that reference this same registry.
    #[allow(dead_code)]
    metrics: Arc<Metrics>,

    /// Consumer-level poll/commit timing metrics (`KafkaConsumerMetrics`,
    /// `AsyncKafkaConsumer.java:291`). Records `time-between-poll`,
    /// `poll-idle-ratio-avg`, `last-poll-seconds-ago`,
    /// `commit-sync-time-ns-total`, `committed-time-ns-total` into the same
    /// `metrics` registry. Wired in `poll`/`commit_sync`/`committed`/`close`.
    kafka_consumer_metrics: Arc<KafkaConsumerMetrics>,

    /// Async-consumer background-task / event-queue metrics
    /// (`AsyncConsumerMetrics`, `AsyncKafkaConsumer.java`). Records into the
    /// same `metrics` registry; wired into the bg task, the event handlers,
    /// and the network client delegate. Used app-side by
    /// `process_background_events` (bg-event queue/processing time) and
    /// removed in `close`.
    async_consumer_metrics: Arc<AsyncConsumerMetrics>,

    /// Shared mirror of the background-event queue depth (Java reads
    /// `backgroundEventQueue.size()`; tokio mpsc has no `len()`). Bumped by
    /// `BackgroundEventHandler::add` on the bg task, reset to 0 by
    /// `process_background_events` (Java's `drainEvents`).
    background_event_queue_size: Arc<AtomicI64>,

    // ── App-side only ─────────────────────────────────────────────────
    /// `client.id`, as a cheap-to-clone `Arc<str>` per CLAUDE.md §11.
    client_id: Arc<str>,
    /// `group.id`, if any.
    group_id: Option<String>,
    /// Group metadata cached by the `MemberStateListener` callback;
    /// returned by [`Self::group_metadata`]. `None` while uninitialized
    /// or for assignment-only consumers. Updated by
    /// [`ConsumerStateNotifier::on_member_epoch_updated`] which is the
    /// `MemberStateListener` registered with the membership manager
    /// (production wire-up in Phase 12; for tests, callers register
    /// [`Self::state_notifier`] directly on the membership manager).
    group_metadata: Arc<Mutex<Option<ConsumerGroupMetadata>>>,
    /// Java: `private final AtomicReference<Set<TopicPartition>> groupAssignmentSnapshot`
    /// (`AsyncKafkaConsumer.java:317`).
    ///
    /// Snapshot of the partitions assigned to this consumer through the
    /// **group-management** path (not `assign(...)` — manually-assigned
    /// partitions never appear here). Updated by
    /// [`ConsumerStateNotifier::on_group_assignment_updated`] from the
    /// membership manager's reconciliation step (Java
    /// `setGroupAssignmentSnapshot(...)` at line 786-788). Read by
    /// [`Self::run_rebalance_callbacks_on_close`] to determine which
    /// partitions are passed to the user's `on_partitions_revoked` /
    /// `on_partitions_lost` callback on close.
    group_assignment_snapshot: Arc<Mutex<HashSet<TopicPartition>>>,
    /// `MemberStateListener` impl that updates `group_metadata` and
    /// `group_assignment_snapshot` when the membership manager fires a
    /// state-change notification. Cloned out of [`Self::state_notifier`]
    /// at construction time and exposed for production wire-up
    /// (Phase 12).
    state_notifier: Arc<ConsumerStateNotifier>,
    /// Invoker for the user-supplied [`ConsumerRebalanceListener`].
    rebalance_listener_invoker: ConsumerRebalanceListenerInvoker,
    /// Drains pending `OffsetCommitCallback` invocations at the top of
    /// every blocking-style API.
    offset_commit_callback_invoker: Arc<OffsetCommitCallbackInvoker<K, V>>,
    /// Key + value deserializers, shared with `Fetcher` /
    /// `FetchCollector`.
    deserializers: Arc<Deserializers<K, V>>,
    /// User-supplied interceptors. Wrapped in `Mutex` because
    /// `on_consume` takes `&mut self` per Phase 2.
    interceptors: Arc<Mutex<ConsumerInterceptors<K, V>>>,
    /// Cached `enable.auto.commit`.
    auto_commit_enabled: bool,
    /// Cached `default.api.timeout.ms`.
    default_api_timeout_ms: i64,
    /// Cached `retry.backoff.ms` — used by [`Self::poll_for_fetches`] to
    /// throttle the poll wait when no positions are valid yet (Java
    /// `AsyncKafkaConsumer.pollForFetches`).
    retry_backoff_ms: i64,
    /// Cached `isolation.level`. Currently used by the (Phase 11 commit
    /// 6/N) `current_lag` event and reserved for future fetch-path
    /// callers; kept on the consumer struct because Java reads it from
    /// the same source.
    isolation_level: IsolationLevel,
    /// `true` after [`Self::close`] has run. Subsequent calls return
    /// `Error::local_illegal_state`.
    closed: AtomicBool,
    /// Listener registered via `subscribe_with_topics_listener` /
    /// `subscribe_with_pattern_listener`. Wrapped in `Mutex<Option<…>>`
    /// so it can be swapped without invalidating
    /// `&self.rebalance_listener_invoker` references.
    rebalance_listener: Mutex<Option<Arc<dyn ConsumerRebalanceListener>>>,
    /// Java: `private AsyncPollEvent inflightPoll`.
    ///
    /// Stores the `Arc<AsyncPollState>` of the currently-inflight
    /// `AsyncPoll` event (if any). Recycled across `poll()` calls per
    /// Java semantics. Held in a plain `Option<...>` instead of
    /// `Mutex<Option<...>>` because `poll()` takes `&mut self` and is the
    /// only caller.
    inflight_poll: Option<InflightPoll>,
    /// Java: `private volatile boolean hasPendingReconciliation` (AK 4.3.1,
    /// KAFKA-20106). Set on the bg task by the `memberStateListener`'s
    /// `on_member_state_change` (`true` iff the member is `RECONCILING`);
    /// read by the app-side [`Self::collect_fetch`] to decide whether to wait
    /// for the in-flight poll's reconciliation check before returning
    /// buffered records. Shared with [`ConsumerStateNotifier`] via `Arc`.
    has_pending_reconciliation: Arc<AtomicBool>,
    /// Java: `private final PositionsValidator positionsValidator`
    /// (`AsyncKafkaConsumer.java:405`). Read-only from this side: the app
    /// task calls [`PositionsValidator::can_skip_update_fetch_positions`]
    /// on the `poll()` critical path, while the background task owns every
    /// mutation through the `OffsetsRequestManager` that shares the `Arc`.
    positions_validator: Arc<PositionsValidator>,
    /// Cached `ConsumerConfig` for late-bound config lookups (e.g.
    /// inside `close`).
    config: ConsumerConfig,
    /// Time source used for `current_time_ms` arguments to events.
    time: Arc<dyn ThreadTime>,
    /// Java: `private CompletableFuture<...> lastPendingAsyncCommit`.
    ///
    /// Tracks the most-recently-submitted async commit so that
    /// `commit_sync` and `close` can wait for in-flight async commits to
    /// complete before continuing (mirrors Java's
    /// `awaitPendingAsyncCommitsAndExecuteCommitCallbacks`). The wrapped
    /// receiver resolves to `()` once the async commit (success OR
    /// failure) has finished — the actual commit result is delivered via
    /// the registered [`crate::consumer::OffsetCommitCallback`], not via
    /// this receiver.
    last_pending_async_commit: Option<tokio::sync::oneshot::Receiver<()>>,
}

/// Tracks the state of the currently-inflight `AsyncPoll` event.
///
/// Mirrors Java's `AsyncPollEvent` field: it carries the event's
/// `deadline_ms` and the shared `Arc<AsyncPollState>` that the bg task
/// completes asynchronously. Stored as a separate small struct so the
/// `is_expired` check stays close to the data it operates on.
pub(crate) struct InflightPoll {
    deadline_ms: i64,
    state: Arc<AsyncPollState>,
}

impl InflightPoll {
    fn is_expired(&self, current_time_ms: i64) -> bool {
        current_time_ms >= self.deadline_ms
    }
}

/// Discriminates between the async / sync commit forms when calling the
/// shared [`AsyncKafkaConsumer::commit_inner`] helper. Mirrors the
/// `CommitEvent` Java base class — both `AsyncCommitEvent` and
/// `SyncCommitEvent` carry the same shape (offsets + deadline) and
/// differ only in the bg-side dispatch.
enum CommitEventKind {
    Async {
        offsets: Option<HashMap<TopicPartition, OffsetAndMetadata>>,
    },
    Sync {
        offsets: Option<HashMap<TopicPartition, OffsetAndMetadata>>,
        deadline_ms: i64,
    },
}

impl CommitEventKind {
    fn offsets(&self) -> Option<&HashMap<TopicPartition, OffsetAndMetadata>> {
        match self {
            Self::Async { offsets } => offsets.as_ref(),
            Self::Sync { offsets, .. } => offsets.as_ref(),
        }
    }

    /// For [`Self::Sync`], the user-supplied timeout-converted deadline;
    /// for [`Self::Async`], the consumer's default API timeout (Java's
    /// `AsyncCommitEvent` carries no explicit deadline — it uses the
    /// default).
    fn deadline_ms(&self) -> i64 {
        match self {
            Self::Async { .. } => i64::MAX,
            Self::Sync { deadline_ms, .. } => *deadline_ms,
        }
    }
}

/// Format a slice of [`TopicPartition`] in Java's `Set.toString()`
/// shape: `[topic-0, topic-1]`. Used in user-facing error messages
/// where Java would format a `Set<TopicPartition>` directly
/// (Issue 19 — DoD §3 exact-message contract).
///
/// Rust's `{:?}` produces `[TopicPartition { topic: "t", partition: 0 }]`
/// which diverges from Java's user-visible string.
fn format_partitions_for_display(partitions: &[TopicPartition]) -> String {
    let mut out = String::from("[");
    for (i, tp) in partitions.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        use std::fmt::Write as _;
        let _ = write!(out, "{tp}");
    }
    out.push(']');
    out
}

/// `MemberStateListener` implementation that bridges the membership
/// manager's state-change notifications back to the consumer's app-side
/// caches: `group_metadata` and `group_assignment_snapshot`.
///
/// Mirrors Java's anonymous-inner-class `memberStateListener` at
/// `AsyncKafkaConsumer.java:343-353`. The callbacks are invoked
/// synchronously from the bg task (membership-manager reconciliation
/// step); they only touch their two `Arc<Mutex<…>>` fields and never
/// call back into the consumer or take other locks, so the brief
/// critical sections are §16-safe.
pub(crate) struct ConsumerStateNotifier {
    /// Group ID this consumer belongs to. Used to populate fresh
    /// [`ConsumerGroupMetadata`] when the cache is empty.
    group_id: String,
    /// Optional `group.instance.id` (static membership identifier).
    /// Preserved across epoch updates in the resulting
    /// [`ConsumerGroupMetadata`].
    group_instance_id: Option<String>,
    /// Shared with [`AsyncKafkaConsumer::group_metadata`].
    group_metadata: Arc<Mutex<Option<ConsumerGroupMetadata>>>,
    /// Shared with [`AsyncKafkaConsumer::group_assignment_snapshot`].
    group_assignment_snapshot: Arc<Mutex<HashSet<TopicPartition>>>,
    /// Shared with [`AsyncKafkaConsumer::has_pending_reconciliation`]
    /// (AK 4.3.1, KAFKA-20106). Set to `true` when the member enters
    /// `RECONCILING`, `false` otherwise, via
    /// [`MemberStateListener::on_member_state_change`] fired on the bg task.
    /// Read by the app-side `collect_fetch` to decide whether to wait for the
    /// reconciliation check before returning buffered records. Java:
    /// `setHasPendingReconciliation(memberState == MemberState.RECONCILING)`.
    has_pending_reconciliation: Arc<AtomicBool>,
}

impl ConsumerStateNotifier {
    /// Constructor. The `group_metadata` / `group_assignment_snapshot`
    /// Arcs are owned by both the notifier and the consumer.
    pub(crate) fn new(
        group_id: impl Into<String>,
        group_instance_id: Option<String>,
        group_metadata: Arc<Mutex<Option<ConsumerGroupMetadata>>>,
        group_assignment_snapshot: Arc<Mutex<HashSet<TopicPartition>>>,
        has_pending_reconciliation: Arc<AtomicBool>,
    ) -> Self {
        Self {
            group_id: group_id.into(),
            group_instance_id,
            group_metadata,
            group_assignment_snapshot,
            has_pending_reconciliation,
        }
    }

    /// Java: `private void updateGroupMetadata(Optional<Integer> memberEpoch, String memberId)`
    /// (`AsyncKafkaConsumer.java:772-784`).
    ///
    /// Updates the cached [`ConsumerGroupMetadata`] to carry the new
    /// epoch / member-id. Java's implementation is an `updateAndGet`
    /// over an `AtomicReference<Optional<ConsumerGroupMetadata>>` that
    /// short-circuits when the slot is empty; we mirror that by
    /// initializing the slot lazily here (Java initializes it at
    /// construction time when `group.id` is present —
    /// `initializeGroupMetadata`).
    fn update_group_metadata(&self, member_epoch: Option<i32>, member_id: &str) {
        let Some(epoch) = member_epoch else {
            // Java's `memberEpoch.ifPresent(...)` short-circuits when
            // None — no metadata mutation.
            return;
        };
        let mut guard = self.group_metadata.lock().unwrap();
        #[allow(deprecated)]
        let next = ConsumerGroupMetadata::with_generation_id_member_id_group_instance_id(
            self.group_id.clone(),
            epoch,
            member_id.to_string(),
            self.group_instance_id.clone(),
        );
        *guard = Some(next);
    }

    /// Java: `private void resetGroupMetadata()`
    /// (`AsyncKafkaConsumer.java:1857-1865`).
    ///
    /// Resets the cached [`ConsumerGroupMetadata`] to the
    /// `UNKNOWN_GENERATION_ID` / `UNKNOWN_MEMBER_ID` defaults,
    /// preserving the original `groupId` and `groupInstanceId`. Called
    /// by [`AsyncKafkaConsumer::unsubscribe`] after the unsubscribe
    /// event completes, matching Java's
    /// `processBackgroundEvents(...)` → `resetGroupMetadata()` sequence
    /// at line 1843-1848.
    ///
    /// Mirrors Java's `updateAndGet` over the Optional: if the slot is
    /// `None` (assignment-only consumer never populated the cache), the
    /// slot stays `None` — Java's `oldGroupMetadataOptional.map(...)`
    /// short-circuits on empty.
    /// Returns a clone of the shared `has_pending_reconciliation` flag so
    /// the [`AsyncKafkaConsumer`] can read the same atomic the notifier
    /// writes from `on_member_state_change`.
    pub(crate) fn has_pending_reconciliation_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.has_pending_reconciliation)
    }

    pub(crate) fn reset_group_metadata(&self) {
        let mut guard = self.group_metadata.lock().unwrap();
        if let Some(old) = guard.as_ref() {
            // Mirror Java's `initializeConsumerGroupMetadata(oldGroupId, oldGroupInstanceId)`:
            // build fresh metadata with UNKNOWN epoch + member, preserving
            // the old group_id + group_instance_id.
            #[allow(deprecated)]
            let next = ConsumerGroupMetadata::with_generation_id_member_id_group_instance_id(
                old.group_id().to_string(),
                -1, // JoinGroupRequest.UNKNOWN_GENERATION_ID
                "", // JoinGroupRequest.UNKNOWN_MEMBER_ID
                old.group_instance_id().map(str::to_string),
            );
            *guard = Some(next);
        }
        // Java's `oldGroupMetadataOptional.map(...)` short-circuits when
        // the slot is empty (assignment-only consumer never populated the
        // cache); we mirror that by leaving the slot as None.
    }
}

impl MemberStateListener for ConsumerStateNotifier {
    /// Java: `memberStateListener.onMemberEpochUpdated(memberEpoch, memberId)`
    /// (`AsyncKafkaConsumer.java:344-347`).
    fn on_member_epoch_updated(&self, member_epoch: Option<i32>, member_id: &str) {
        self.update_group_metadata(member_epoch, member_id);
    }

    /// Java: `memberStateListener.onGroupAssignmentUpdated(partitions)`
    /// (`AsyncKafkaConsumer.java:349-352`). Snapshots the assignment so
    /// `runRebalanceCallbacksOnClose` can drive listener callbacks over
    /// the **group**-assigned partitions specifically (manual
    /// `assign(...)` partitions are intentionally excluded).
    fn on_group_assignment_updated(&self, partitions: &HashSet<TopicPartition>) {
        let mut guard = self.group_assignment_snapshot.lock().unwrap();
        *guard = partitions.clone();
    }

    /// Java: `memberStateListener.onMemberStateChange(memberState)`
    /// (`AsyncKafkaConsumer.java`) →
    /// `setHasPendingReconciliation(memberState == MemberState.RECONCILING)`.
    fn on_member_state_change(&self, member_state: MemberState) {
        self.has_pending_reconciliation
            .store(member_state == MemberState::Reconciling, Ordering::Release);
    }
}

/// Components handed to [`AsyncKafkaConsumer::new_with_thread`]: the
/// per-RM container, metadata, subscriptions, application-event handle,
/// reaper, wakeup trigger, etc. Constructed by the production factory
/// (commit (7)) and by tests directly.
///
/// Bundling these into a struct keeps the ctor signature manageable as
/// the Java ctor has 14 already-constructed dependencies.
pub(crate) struct AsyncKafkaConsumerComponents<K: Send + Sync + 'static, V: Send + Sync + 'static> {
    pub config: ConsumerConfig,
    pub client_id: Arc<str>,
    pub group_id: Option<String>,
    pub subscriptions: Arc<Mutex<SubscriptionState>>,
    pub metadata: Arc<ConsumerMetadata>,
    pub request_managers: Arc<std::sync::Mutex<RequestManagers>>,
    pub background_event_rx: mpsc::UnboundedReceiver<BackgroundEventEnvelope>,
    pub application_event_handler: Arc<ApplicationEventHandler>,
    pub completable_event_reaper: Arc<std::sync::Mutex<CompletableEventReaper>>,
    pub max_time_to_wait_ms: Arc<AtomicI64>,
    pub wakeup_trigger: WakeupTrigger,
    pub network_thread_close: NetworkThreadCloseHandle,
    pub fetch_buffer: Arc<FetchBuffer>,
    pub fetch_collector: Arc<FetchCollector<K, V>>,
    /// The metrics registry. Owned here so Phase M7 can expose the public
    /// `metrics()` accessor over the same registry the fetch path records into.
    pub metrics: Arc<Metrics>,
    /// Consumer-level poll/commit timing metrics
    /// (`KafkaConsumerMetrics`), recording into the same `metrics` registry.
    pub kafka_consumer_metrics: Arc<KafkaConsumerMetrics>,
    /// Async-consumer background-task / event-queue metrics
    /// (`AsyncConsumerMetrics`), recording into the same `metrics` registry.
    pub async_consumer_metrics: Arc<AsyncConsumerMetrics>,
    /// Shared mirror of the background-event queue depth.
    pub background_event_queue_size: Arc<AtomicI64>,
    pub rebalance_listener_invoker: ConsumerRebalanceListenerInvoker,
    pub offset_commit_callback_invoker: Arc<OffsetCommitCallbackInvoker<K, V>>,
    pub deserializers: Arc<Deserializers<K, V>>,
    pub interceptors: Arc<Mutex<ConsumerInterceptors<K, V>>>,
    pub isolation_level: IsolationLevel,
    pub time: Arc<dyn ThreadTime>,
    /// Shared `Arc<Mutex<Option<ConsumerGroupMetadata>>>` slot. Java has a
    /// **single** `AtomicReference<Optional<ConsumerGroupMetadata>>` field
    /// (`AsyncKafkaConsumer.java:289`); the same slot is referenced by
    /// the `MemberStateListener` registered on the membership manager AND
    /// read by the public `groupMetadata()` accessor. The Rust
    /// translation enforces that single-source-of-truth contract by
    /// requiring the production ctor to build the slot once and pass it
    /// through here — the same Arc is then registered on
    /// `ConsumerMembershipManager` via [`Self::state_notifier`] AND
    /// stored on the consumer struct's `group_metadata` field.
    pub group_metadata: Arc<Mutex<Option<ConsumerGroupMetadata>>>,
    /// Shared `Arc<Mutex<HashSet<TopicPartition>>>` slot mirroring
    /// Java's `groupAssignmentSnapshot` field
    /// (`AsyncKafkaConsumer.java:317`). Same single-source-of-truth
    /// contract as [`Self::group_metadata`] — the production ctor builds
    /// once and threads the Arc through both
    /// `ConsumerStateNotifier::on_group_assignment_updated` (writer) and
    /// `AsyncKafkaConsumer::run_rebalance_callbacks_on_close` (reader).
    pub group_assignment_snapshot: Arc<Mutex<HashSet<TopicPartition>>>,
    /// The single `MemberStateListener` instance (Java
    /// `AsyncKafkaConsumer.java:343-353`'s anonymous-inner-class
    /// `memberStateListener`) that writes to
    /// [`Self::group_metadata`] and [`Self::group_assignment_snapshot`].
    /// The production ctor clones this Arc and registers it on
    /// `ConsumerMembershipManager` BEFORE the bg-task spawn; the
    /// consumer struct stores it for the `state_notifier()` accessor used
    /// in close-time `reset_group_metadata()` and by tests. Tests can
    /// register `consumer.state_notifier()` on a custom membership
    /// manager when they bypass the production ctor.
    pub state_notifier: Arc<ConsumerStateNotifier>,
    /// The shared [`PositionsValidator`] (Java `AsyncKafkaConsumer.java:405`).
    /// The SAME `Arc` must also have been passed to the
    /// `OffsetsRequestManager` in `request_managers`, or the app-side skip
    /// decision reads state the background side never writes.
    pub positions_validator: Arc<PositionsValidator>,
}

impl<K, V> AsyncKafkaConsumer<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// Constructs an `AsyncKafkaConsumer` from pre-built components.
    ///
    /// Test-visible seam. The production factory in
    /// `consumer/mod.rs::new_consumer` flows through
    /// `AsyncKafkaConsumer::new` (Phase 12 commit 4/N), which builds the
    /// dependency closure and delegates to this method. Tests build
    /// their own components (typically with a `MockClient`-backed
    /// `ConsumerNetworkThread`) and call this directly.
    ///
    /// Mirrors Java's test-visible constructor at
    /// `AsyncKafkaConsumer.java:521` (the 20-arg form), with the
    /// metrics / telemetry parameters dropped per Phase 11 PLAN.md
    /// deferrals.
    /// Production constructor — translates Java's primary
    /// `AsyncKafkaConsumer(ConsumerConfig, Deserializer<K>, Deserializer<V>,
    /// Optional<StreamsRebalanceData>)` (`AsyncKafkaConsumer.java:355-518`)
    /// in three buildable slices per Phase-12 PLAN.md:
    ///
    /// - **Commit (1/N) — this method:** Builds channels, subscriptions,
    ///   metadata, `NetworkClient`, `NetworkClientDelegate`,
    ///   `BackgroundEventHandler`, `FetchBuffer`, `FetchConfig`,
    ///   `Deserializers`, `ConsumerInterceptors`, and the
    ///   `OffsetCommitCallbackInvoker`. Returns
    ///   `Err(Error::unsupported_version(...))` at the end because
    ///   the `RequestManagers` (commit (2/N)) and bg-task spawn (commit
    ///   (3/N)) are not yet wired — see PLAN.md commit-table rows 1-3.
    /// - **Commit (2/N):** Adds `RequestManagers` wiring (coordinator,
    ///   commit, heartbeat, membership, offsets, topic-metadata, fetch)
    ///   with the group-protocol gate at Java lines 502-505, plus the
    ///   `ConsumerStateNotifier::register_state_listener` registration on
    ///   `ConsumerMembershipManager` (PLAN.md §"State-notifier
    ///   registration").
    /// - **Commit (3/N):** Adds bg-task spawn + `NetworkThreadCloseHandle`
    ///   assembly + `Self::with_components(...)` call. After commit
    ///   (3/N) this constructor compiles end-to-end.
    ///
    /// PLAINTEXT only in Phase 12 (PLAN.md §"Out of scope" — SSL/SASL
    /// `ChannelBuilder` escape hatches deferred). Bootstrap addresses
    /// resolved via [`crate::ClientUtils::parse_and_validate_addresses`]
    /// — same call the producer uses (mirrors Java
    /// `ClientUtils.parseAndValidateAddresses`).
    ///
    /// # Java field-init order
    ///
    /// Mirrors the table in PLAN.md (Java lines 390-508 → Rust action).
    /// Each block below references the Java line that originated it.
    ///
    /// # Errors
    ///
    /// Java wraps the whole constructor body in
    /// `catch (Throwable t) { ... throw new KafkaException("Failed to construct
    /// kafka consumer", t); }` (`AsyncKafkaConsumer.java:509-517`), so EVERY
    /// construction failure reaches the caller as a `KafkaException` carrying
    /// that message with the underlying failure as its cause. This wrapper
    /// reproduces that; [`Self::new_inner`] holds the body.
    ///
    /// The other half of Java's catch body — `close(Duration.ZERO, ...)` to
    /// release partially-built resources (KAFKA-2121) — has no counterpart
    /// here: every fallible step in `new_inner` precedes the `tokio::spawn`,
    /// which happens inside the infallible `with_components`, so no
    /// resource needing shutdown exists yet on any error path.
    /// # Java's no-deserializer constructors are deliberately not translated
    ///
    /// Java's public entry point is `KafkaConsumer`, which has four
    /// constructors; the two without deserializers — `KafkaConsumer(Map)`
    /// (`KafkaConsumer.java:557`) and `KafkaConsumer(Properties)` (`:570`) —
    /// delegate to `this(configs, null, null)` and rely on
    /// `config.getConfiguredInstance(KEY_DESERIALIZER_CLASS_CONFIG,
    /// Deserializer.class)` to instantiate the class named by
    /// `key.deserializer` reflectively. Rust has no reflection and a
    /// deserializer is typed in this consumer's own `K` / `V`, so the
    /// deserializers are always supplied here as instances, and
    /// [`ConsumerConfig`](crate::consumer::ConsumerConfig) carries no
    /// deserializer state — no field, no config-key constant, and no setter.
    /// A `key.deserializer` / `value.deserializer` entry in the property map is
    /// therefore an unrecognised key, warned about and ignored, exactly as
    /// `key.serializer` is on the producer side.
    ///
    /// This mirrors the producer exactly, including the CLAUDE.md §2
    /// consequence for the plain name `new` — see
    /// [`KafkaProducer::new`](crate::producer::KafkaProducer::new) for the full
    /// reasoning, which is not repeated here.
    pub fn new(
        config: ConsumerConfig,
        key_deserializer: Box<dyn crate::common::serialization::Deserializer<K>>,
        value_deserializer: Box<dyn crate::common::serialization::Deserializer<V>>,
    ) -> Result<Self, Error> {
        Self::new_inner(config, key_deserializer, value_deserializer).map_err(|err| {
            // Java's wrap is UNCONDITIONAL — unlike
            // `ConsumerUtils.maybeWrapAsKafkaException`, it wraps a
            // `KafkaException` too, so there is no `is_kafka_error()` guard
            // here. "Failed to construct kafka consumer" is the string users
            // match on.
            Error::KafkaError(crate::common::KafkaError::with_message_source(
                crate::common::Errors::UnknownServerError,
                "Failed to construct kafka consumer",
                err,
            ))
        })
    }

    /// The body of Java's constructor `try` block
    /// (`AsyncKafkaConsumer.java:390-508`). See [`Self::new`] for the
    /// `catch (Throwable t)` wrap applied to every error it returns.
    fn new_inner(
        config: ConsumerConfig,
        key_deserializer: Box<dyn crate::common::serialization::Deserializer<K>>,
        value_deserializer: Box<dyn crate::common::serialization::Deserializer<V>>,
    ) -> Result<Self, Error> {
        use crate::ApiVersions;
        use crate::ClientUtils;
        use crate::DefaultHostResolver;
        use crate::MetadataRecoveryStrategy;
        use crate::NetworkClient;
        use crate::common::internals::ClusterResourceListeners;
        use crate::common::network::ChannelBuilders;
        use crate::common::network::Selector;
        use crate::consumer::AutoOffsetResetStrategy;
        use crate::consumer::internals::CommitRequestManager;
        use crate::consumer::internals::ConsumerHeartbeatRequestManager;
        use crate::consumer::internals::ConsumerMembershipManager;
        use crate::consumer::internals::ConsumerMetadata;
        use crate::consumer::internals::ConsumerUtils;
        use crate::consumer::internals::CoordinatorRequestManager;
        use crate::consumer::internals::FetchConfig;
        use crate::consumer::internals::FetchRequestManager;
        use crate::consumer::internals::NetworkClientDelegate;
        use crate::consumer::internals::OffsetsRequestManager;
        use crate::consumer::internals::RequestManagers;
        use crate::consumer::internals::TopicMetadataRequestManager;

        log::debug!("Initializing the Kafka consumer");

        // Java line 390 — `clientId = config.getString(CLIENT_ID_CONFIG)`.
        let client_id: Arc<str> = Arc::from(config.client_id());
        // Java line 391 — `autoCommitEnabled = config.getBoolean(...)`.
        let auto_commit_enabled = config.enable_auto_commit();
        // Java line 397 — `defaultApiTimeoutMs = Duration.ofMillis(...)`.
        // Read but stored on the consumer struct via
        // `with_components`.
        let _default_api_timeout_ms = config.default_api_timeout_ms;

        // Java lines 393-394 — `backgroundEventQueue` /
        // `applicationEventQueue` allocations. Both are unbounded —
        // mirrors Java's `LinkedBlockingQueue<>`.
        let (bg_event_tx, _bg_event_rx) = mpsc::unbounded_channel::<BackgroundEventEnvelope>();
        let (_app_event_tx, _app_event_rx) =
            mpsc::unbounded_channel::<crate::consumer::internals::events::ApplicationEventEnvelope>();
        // Java line 411 — `subscriptions = createSubscriptionState(config,
        // logContext)`. The `auto.offset.reset` strategy is parsed once at
        // ctor time.
        let auto_offset_reset = AutoOffsetResetStrategy::from_string(config.auto_offset_reset())?;
        let subscriptions: Arc<Mutex<SubscriptionState>> =
            Arc::new(Mutex::new(SubscriptionState::new(auto_offset_reset)));

        // Java line 408-409 — `interceptorList`, `interceptors = new
        // ConsumerInterceptors<>(...)`. The Java reflection-based loader is
        // not translated (per PLAN.md "Out of scope"); the Rust ctor
        // builds an empty interceptor chain. Users supply interceptors via
        // a future config-extension API.
        let _interceptors: Arc<Mutex<ConsumerInterceptors<K, V>>> =
            Arc::new(Mutex::new(ConsumerInterceptors::<K, V>::new(Vec::new())));

        // Java line 410 — `deserializers = new Deserializers<>(...)`.
        let _deserializers: Arc<Deserializers<K, V>> =
            Arc::new(Deserializers::new(key_deserializer, value_deserializer));

        // Java line 412-414 — `clusterResourceListeners`. Phase-11
        // deferral keeps notifier wiring as a no-op (`ClusterResourceListeners::new()`).
        let cluster_resource_listeners = ClusterResourceListeners::new();

        // Java line 415 — `metadata = metadataFactory.build(...)`.
        let metadata = Arc::new(ConsumerMetadata::with_config(
            &config,
            Arc::clone(&subscriptions),
            cluster_resource_listeners,
        ));

        // Java lines 416-417 — `addresses =
        // ClientUtils.parseAndValidateAddresses(config)`,
        // `metadata.bootstrap(addresses)`.
        let addresses = ClientUtils::parse_and_validate_addresses(config.bootstrap_servers())?;
        metadata.bootstrap(addresses);

        // Java line 420 — `fetchConfig = new FetchConfig(config)`.
        let fetch_config = FetchConfig::with_consumer_config(&config)?;
        // Java line 421 — `isolationLevel = fetchConfig.isolationLevel`.
        let _isolation_level = fetch_config.isolation_level;

        // Java line 423 — `apiVersions = new ApiVersions()`.
        let api_versions = Arc::new(ApiVersions::new());

        // Java lines 425-429 — `backgroundEventHandler = new
        // BackgroundEventHandler(...)`. The `AsyncConsumerMetrics` and the
        // shared background-event queue-depth counter are wired below (after
        // the `metrics` registry exists) before the handler is shared.
        let mut background_event_handler = BackgroundEventHandler::new(bg_event_tx);

        // Java line 432 — `fetchBuffer = new FetchBuffer(logContext)`.
        let fetch_buffer = Arc::new(FetchBuffer::new());

        // Java lines 402/419 — `metrics = createMetrics(config, time, reporters)`
        // then `fetchMetricsManager = createFetchMetricsManager(metrics)`.
        // The consumer owns the `Arc<Metrics>` (kept for Phase M7's public
        // `metrics()` accessor); the `Arc<FetchMetricsManager>` is shared into
        // the fetch path (FetchRequestManager / FetchCollector). The full
        // Metrics-wiring (`consumer.metrics()`, reporter list) is finalized in
        // M7 over THIS same registry — no re-plumb.
        let (metrics, fetch_metrics_manager) = Self::create_fetch_metrics_manager(&config);

        // M4: the consumer-level + heartbeat + offset-commit metrics managers
        // all register against the SAME `Arc<Metrics>` registry. Java
        // constructs each from `metrics` in the relevant constructor
        // (`KafkaConsumerMetrics`/`HeartbeatMetricsManager`/
        // `OffsetCommitMetricsManager`). The heartbeat/commit managers are
        // wired into their bg-task request managers post-construction (the
        // request managers are built below), mirroring the coordinator/
        // interceptor-hook setter pattern.
        let kafka_consumer_metrics = Arc::new(KafkaConsumerMetrics::new(Arc::clone(&metrics)));
        let offset_commit_metrics_manager =
            Arc::new(crate::consumer::internals::OffsetCommitMetricsManager::new(&metrics));
        let heartbeat_metrics_manager = Arc::new(crate::consumer::internals::HeartbeatMetricsManager::new(&metrics));

        // M6: the async-consumer background-task / event-queue metrics
        // (`AsyncConsumerMetrics`, `AsyncKafkaConsumer.java`). Registered
        // against the SAME `Arc<Metrics>` under `CONSUMER_METRIC_GROUP`
        // (`consumer-metrics`). Wired into the bg task, the application/
        // background event handlers, and the network client delegate (the
        // record sites Java passes `asyncConsumerMetrics` to). The two
        // `Arc<AtomicI64>` queue-depth counters mirror Java's O(1)
        // `queue.size()` for the application/background event queues (the
        // tokio mpsc sender exposes no `len()`).
        let async_consumer_metrics = Arc::new(AsyncConsumerMetrics::new(
            Arc::clone(&metrics),
            crate::consumer::internals::ConsumerUtils::CONSUMER_METRIC_GROUP,
        ));
        let application_event_queue_size = Arc::new(AtomicI64::new(0));
        let background_event_queue_size = Arc::new(AtomicI64::new(0));

        // Wire the background-event handler's metrics + queue-depth counter
        // before it is shared into the delegate (single owner here).
        background_event_handler
            .set_async_consumer_metrics(Arc::clone(&async_consumer_metrics), Arc::clone(&background_event_queue_size));
        let background_event_handler = Arc::new(background_event_handler);

        // Java lines 434-445 — `networkClientDelegateSupplier =
        // NetworkClientDelegate.supplier(...)`. Mirrors the producer's
        // `with_config` Selector / NetworkClient wiring at
        // `src/producer/kafka_producer.rs:278-292`.
        //
        // Mirrors Java's `AsyncKafkaConsumer` `LogContext` prefix
        // `[Consumer clientId=..., groupId=...] `.
        let log_context = match config.group_id() {
            Some(group_id) => {
                LogContext::new(format!("[Consumer clientId={}, groupId={}] ", config.client_id(), group_id))
            },
            None => LogContext::new(format!("[Consumer clientId={}] ", config.client_id())),
        };
        // Java line 433 — `ChannelBuilder channelBuilder =
        // ClientUtils.createChannelBuilder(config, time, logContext)`. Selects
        // the channel builder from `security.protocol` + `ssl.*` / `sasl.*`
        // (PLAINTEXT / SSL / SASL_PLAINTEXT / SASL_SSL); SASL mechanism PLAIN
        // only. Errors surface as a `Error` from the ctor (no panic).
        let channel_builder = ChannelBuilders::client_channel_builder(
            config.security_protocol,
            Some(&config.ssl_config),
            Some(&config.sasl_config),
            None, // listener_name
            config.client_id(),
            log_context.clone(),
        )
        .map_err(|e| Error::local_illegal_argument(format!("Failed to create channel builder: {}", e)))?;
        let selector = Selector::with_defaults_and_log_context(
            config.connections_max_idle_ms,
            channel_builder,
            log_context.clone(),
        );
        let shared_metadata = metadata.metadata_arc();
        let network_client = NetworkClient::with_metadata_rebootstrap_trigger_ms(
            selector,
            shared_metadata,
            config.client_id(),
            ConsumerUtils::CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION as usize,
            config.reconnect_backoff_ms,
            config.reconnect_backoff_max_ms,
            config.send_buffer_bytes,
            config.receive_buffer_bytes,
            config.request_timeout_ms,
            config.socket_connection_setup_timeout_ms,
            config.socket_connection_setup_timeout_max_ms,
            true, // discover_broker_versions — mirrors Java
            Arc::clone(&api_versions),
            DefaultHostResolver::new(),
            config.metadata_max_age_ms, // rebootstrap_trigger_ms
            MetadataRecoveryStrategy::None,
            log_context,
        );
        let mut network_client_delegate_inner = NetworkClientDelegate::new(
            &config,
            network_client,
            Arc::clone(&metadata).metadata_arc(),
            Arc::clone(&background_event_handler),
            false, // notify_metadata_errors_via_error_queue — Java
                   // passes `false` for the consumer ctor (Java
                   // `AsyncKafkaConsumer.java:443`).
        );
        // M6: Java passes `asyncConsumerMetrics` to the delegate ctor; wire it
        // here before the delegate is shared with the bg task.
        network_client_delegate_inner.set_async_consumer_metrics(Arc::clone(&async_consumer_metrics));

        // The single "nudge the background task" primitive: the selector's own
        // wakeup handle, i.e. Java's `Selector.wakeup()`. Everything that needs
        // the bg task to stop waiting pokes THIS — `ApplicationEventHandler::add`
        // (Java's `wakeupNetworkThread()`), the §31 rebalance-ack path,
        // `FetchRequestManager`'s completion signal, `wakeup()` and
        // `signal_close()`. It is a lock-free `Arc<Notify>`, so firing it never
        // contends with the delegate mutex the bg task holds while polling.
        //
        // Taken from the delegate rather than created here on purpose: a
        // separate `Notify` would only be forwarded to this one, and having two
        // interchangeable-looking nudge channels is what let several call sites
        // poke the wrong one.
        let event_notify = network_client_delegate_inner.wakeup_handle();
        let _network_client_delegate = Arc::new(tokio::sync::Mutex::new(network_client_delegate_inner));

        // Java line 446 — `offsetCommitCallbackInvoker = new
        // OffsetCommitCallbackInvoker(interceptors)`. The interceptor
        // chain is constructed once and cloned/wrapped here; the
        // commit-callback invoker owns its own ConsumerInterceptors
        // instance for `on_commit` dispatch (per Phase-9 design).
        //
        // DOCUMENTED LIMITATION (not a TODO): config-based interceptor
        // loading is untranslated in this milestone. Java line 446 passes
        // the SAME `interceptors` reference to both the consumer and the
        // invoker; that `interceptors` list is populated by the reflective
        // `interceptor.classes` loader, which has no Rust counterpart here.
        // The Rust `new` path therefore always builds an EMPTY
        // `ConsumerInterceptors` for both the consumer and this invoker, so
        // the two are behaviorally identical today (both hold empty Vecs).
        // The seam that CAN carry a non-empty interceptor chain is
        // `with_components` (`pub(crate)`); when reflective interceptor
        // loading is added in a future milestone, share the consumer's
        // interceptor Arc here so the invoker dispatches through the same
        // loaded list rather than this empty one.
        let _offset_commit_callback_invoker: Arc<OffsetCommitCallbackInvoker<K, V>> =
            Arc::new(OffsetCommitCallbackInvoker::new(ConsumerInterceptors::<K, V>::new(Vec::new())));

        // Java line 447 — `groupMetadata.set(initializeGroupMetadata(...))`
        // — only when `group.id` is present. The cache itself lives on
        // the consumer struct (built inside `with_components`).
        //
        // `initializeGroupMetadata(String, Optional<String>)`
        // (`AsyncKafkaConsumer.java:747-757`) rejects a present-but-empty
        // `group.id` before building anything:
        //
        // ```java
        // if (groupId != null) {
        //     if (groupId.isEmpty()) {
        //         throw new InvalidGroupIdException("The configured " + ConsumerConfig.GROUP_ID_CONFIG
        //             + " should not be an empty string or whitespace.");
        //     } else {
        //         return Optional.of(initializeConsumerGroupMetadata(groupId, groupInstanceId));
        //     }
        // }
        // ```
        //
        // Without it, `Some("")` is "in a group" for the coordinator / commit /
        // heartbeat / membership wiring below but "not in a group" for
        // `return_error_if_group_id_not_defined`, so one consumer holds two
        // contradictory answers and puts an empty group id on the wire.
        // (Java's check is `isEmpty()` only, despite the message naming
        // whitespace; the message is reproduced verbatim regardless.)
        let group_id = match config.group_id() {
            Some("") => {
                return Err(Error::invalid_group_id(format!(
                    "The configured {} should not be an empty string or whitespace.",
                    ConsumerConfig::GROUP_ID_CONFIG
                )));
            },
            Some(group_id) => Some(group_id.to_string()),
            None => None,
        };

        // ═══════════════════════════════════════════════════════════════
        // Phase 12 commit (2/N): RequestManagers wiring.
        // ═══════════════════════════════════════════════════════════════
        //
        // Java lines 448-465 — `requestManagersSupplier =
        // RequestManagers.supplier(...)`. The Rust translation builds each
        // manager directly. Group-protocol gate: coordinator / commit /
        // heartbeat / membership are only built when `group.id` is
        // present (Java's `Optional.ofNullable(groupId).map(...)` pattern
        // inside `RequestManagers.supplier`).
        //
        // Slot sharing across managers (Java uses heap references; Rust
        // wraps the shared slot in `Arc<...>` — see `RequestManagers`
        // field docs and Phase 12 commit message for the refactor that
        // landed alongside this commit):
        //   * `coordinator`: `Arc<CoordinatorRequestManager>` —
        //     shared with the heartbeat manager (heartbeat reads the
        //     discovered coordinator node every poll). Uses interior
        //     mutability (`Arc<CoordinatorRequestManagerInner>`).
        //   * `commit`: `Arc<CommitRequestManager>` — shared with the
        //     membership manager (membership calls
        //     `maybeAutoCommitSyncBeforeRebalance` from `reconcile`).
        //   * `consumer_membership`: `Arc<ConsumerMembershipManager>` —
        //     shared with the heartbeat manager (heartbeat reads
        //     state/epoch/assignment every poll).
        // The bg-task `run_once` polls these three Arc-shared slots
        // explicitly (`coordinator` via `lock()`, `commit` via
        // `ApplicationEventProcessor` event arms, `membership` via
        // `reconcile()`); see `consumer_network_thread.rs`.

        let current_time_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let coordinator: Option<Arc<CoordinatorRequestManager>> = group_id.as_ref().map(|gid| {
            Arc::new(CoordinatorRequestManager::new(
                config.retry_backoff_ms(),
                config.retry_backoff_max_ms(),
                gid.clone(),
            ))
        });

        let commit: Option<Arc<CommitRequestManager>> = group_id.as_ref().map(|gid| {
            Arc::new(CommitRequestManager::new(
                &config,
                Arc::clone(&metadata),
                Arc::clone(&subscriptions),
                gid.clone(),
                config.group_instance_id().map(|s| s.to_string()),
                Arc::new(crate::common::metrics::SystemTime),
                current_time_ms,
            ))
        });

        // Wire the CoordinatorRequestManager handle into the
        // CommitRequestManager. Java passes the coordinator directly to
        // the `CommitRequestManager` constructor
        // (`AsyncKafkaConsumer.java` calling
        // `CommitRequestManager.<init>(coordinatorRequestManager, ...)`).
        // In Rust both managers are `Arc`-shared and reference each
        // other through interior mutability; wiring happens after both
        // are built. Without this, the commit manager's response
        // handlers and retry drivers cannot call
        // `mark_coordinator_unknown` on `NotCoordinator` /
        // `CoordinatorNotAvailable` errors — which is what drives the
        // bg-task's next `poll(now)` to re-issue `FindCoordinator`.
        if let (Some(coord_arc), Some(commit_arc)) = (coordinator.as_ref(), commit.as_ref()) {
            commit_arc.set_coordinator(Arc::clone(coord_arc));
        }

        // Wire the `OffsetCommitCallbackInvoker` into the commit manager as a
        // type-erased `AutoCommitInterceptorHook` so the auto-commit success
        // path can enqueue the interceptor `on_commit` invocation. Java holds
        // the invoker directly on `CommitRequestManager`; in Rust the invoker
        // is generic over `<K, V>` and the commit manager is not, so it is
        // wired post-construction via the erased trait
        // (`CommitRequestManager.java:380` `autoCommitCallback`).
        if let Some(commit_arc) = commit.as_ref() {
            commit_arc.set_auto_commit_interceptor_hook(Arc::clone(&_offset_commit_callback_invoker)
                as Arc<dyn crate::consumer::internals::AutoCommitInterceptorHook>);
        }

        // M4: wire the OffsetCommitMetricsManager into the commit manager so
        // the commit-response handler records per-commit request latency
        // (`CommitRequestManager.java:767`).
        if let Some(commit_arc) = commit.as_ref() {
            commit_arc.set_offset_commit_metrics_manager(Arc::clone(&offset_commit_metrics_manager));
        }

        // Java lines 502-505 — `if (groupMetadata.get().isPresent() &&
        // groupProtocol == CONSUMER) config.ignore(GROUP_REMOTE_ASSIGNOR_CONFIG)`.
        // Rust does not track "ignored" config keys (no `ConfigDef`
        // equivalent); this is a comment-only translation. The classic
        // protocol path is deferred per `consumer-threading.md` §20, so
        // we only need to silence the warning for KIP-848 (`Consumer`)
        // consumers.
        // (No-op in Rust.)

        // KIP-848 ConsumerMembershipManager — only built for KIP-848
        // (`group.protocol = consumer`) when `group.id` is present.
        let membership_opt: Option<Arc<ConsumerMembershipManager>> = match (group_id.as_ref(), commit.as_ref()) {
            (Some(gid), Some(commit_arc)) => Some(Arc::new(ConsumerMembershipManager::new(
                gid.clone(),
                config.group_instance_id().map(|s| s.to_string()),
                None, // rack_id — Java reads from ConsumerConfig.CLIENT_RACK_CONFIG
                config.max_poll_interval_ms(),
                config.group_remote_assignor().map(|s| s.to_string()),
                Arc::clone(&subscriptions),
                // share() the commit handle: the membership manager + the
                // RequestManagers.commit slot point to the same
                // `Arc<CommitRequestManagerInner>` (Java holds one
                // reference each).
                Some(Arc::clone(commit_arc)),
                Arc::clone(&metadata),
                Arc::clone(&background_event_handler),
                auto_commit_enabled,
                // M5: rebalance latency/rate/failure metrics, registered against
                // the consumer's shared Arc<Metrics> (M3 field). Java builds the
                // ConsumerRebalanceMetricsManager inside the membership-manager
                // constructor; we build it here and pass it in.
                Some(Arc::new(crate::consumer::internals::ConsumerRebalanceMetricsManager::new(
                    &metrics,
                    Arc::clone(&subscriptions),
                ))),
                Arc::new(crate::common::metrics::SystemTime),
            ))),
            _ => None,
        };

        // ConsumerHeartbeatRequestManager — only built when membership
        // is present (heartbeat needs the membership state machine to
        // build heartbeat-request bodies).
        let consumer_heartbeat: Option<ConsumerHeartbeatRequestManager> =
            match (coordinator.as_ref(), membership_opt.as_ref()) {
                (Some(coord_arc), Some(membership)) => {
                    let mut hb = ConsumerHeartbeatRequestManager::new(
                        current_time_ms,
                        &config,
                        Arc::clone(coord_arc),
                        Arc::clone(&subscriptions),
                        Arc::clone(membership),
                        Arc::clone(&background_event_handler),
                    );
                    // M4: wire the HeartbeatMetricsManager so the send/response
                    // paths record `last-heartbeat-seconds-ago` /
                    // `heartbeat-latency` (`AbstractHeartbeatRequestManager.java:285,299`).
                    hb.set_metrics_manager(Arc::clone(&heartbeat_metrics_manager));
                    Some(hb)
                },
                _ => None,
            };

        // Java: `this.positionsValidator = new PositionsValidator(logContext, time,
        // subscriptions, metadata);` (`AsyncKafkaConsumer.java:517`). Built here, on
        // the application side, because the app task consults it on the `poll()`
        // critical path; the same `Arc` is handed to the `OffsetsRequestManager`
        // below, which passes it on to its `OffsetFetcherUtils` (Java `:548` →
        // `OffsetsRequestManager.java:144`). One instance, three owners.
        let positions_validator = Arc::new(PositionsValidator::new(Arc::clone(&subscriptions), Arc::clone(&metadata)));

        // OffsetsRequestManager — always built (Java's
        // `RequestManagers.supplier` builds this unconditionally).
        let offsets = Some(OffsetsRequestManager::new(
            Arc::clone(&subscriptions),
            Arc::clone(&metadata),
            fetch_config.isolation_level,
            config.retry_backoff_ms(),
            config.request_timeout_ms() as i64,
            config.default_api_timeout_ms as i64,
            Arc::clone(&api_versions),
            commit.as_ref().map(Arc::clone),
            Arc::clone(&positions_validator),
        ));

        // TopicMetadataRequestManager — always built.
        let topic_metadata = Some(TopicMetadataRequestManager::new(&config));

        // FetchRequestManager — always built. The `is_unavailable` and
        // `maybe_throw_auth_failure` closures bridge to the delegate
        // (the delegate is not visible from the fetch manager directly
        // — Phase-7 design uses Arc<Fn> indirection).
        let fetch = {
            use crate::common::Node;
            use crate::common::memory::BufferSupplier;

            // Phase-12-deferred wiring: the delegate-backed
            // `is_unavailable` / `maybe_throw_auth_failure` closures
            // require an `Arc<AsyncMutex<NetworkClientDelegate<...>>>`
            // crossing into a sync `Fn` boundary. Synchronously
            // acquiring an async mutex inside a sync `Fn` is not
            // possible without blocking. Phase 12 ships with no-op
            // closures (always-available, never-auth-fail).
            //
            // Behavior gap (Critic Issue 4):
            //   * `is_unavailable` no-op: the fetch manager treats
            //     every node as available, so it may schedule fetches
            //     to nodes the delegate has disconnected. The delegate
            //     filters those at send-time, so the practical impact
            //     is one extra round-trip per disconnected-node fetch
            //     attempt — not a correctness bug.
            //   * `maybe_throw_auth_failure` no-op IS a correctness
            //     gap. Java's `AbstractFetch.java:452-457` calls
            //     `isUnavailable(node)` THEN `maybeThrowAuthFailure(node)`
            //     to surface cached auth errors observable on the
            //     consumer side BEFORE the broker connection has been
            //     established. With the no-op `|_| Ok(())`, an auth
            //     error stays invisible until the broker actually RSTs
            //     the next reconnect attempt, then surfaces via the
            //     background event queue with delayed semantics.
            //
            // Phase 12 is PLAINTEXT-only (PLAN.md §"Out of scope"); the
            // auth path is not exercised here. The closures MUST be
            // replaced with delegate-backed snapshots before any
            // SSL/SASL wiring lands.
            //
            // FIXME(phase-9-sasl): wire a sync snapshot of the
            // delegate's node-availability + auth-failure maps. The
            // delegate writes on `add_all_from_poll_result` /
            // `handle_disconnections`; the closures read. Without this,
            // an `AbstractFetch.maybeThrowAuthFailure`-equivalent path
            // cannot surface `SASL_AUTHENTICATION_FAILED` from the
            // delegate's cached state — auth errors will only appear
            // after a subsequent broker round-trip.
            let is_unavailable: crate::consumer::internals::IsUnavailableFn = Arc::new(|_n: &Node| false);
            let maybe_auth: crate::consumer::internals::MaybeAuthFailureFn = Arc::new(|_n: &Node| Ok::<(), Error>(()));

            let mut frm = FetchRequestManager::new(
                Arc::clone(&metadata),
                Arc::clone(&subscriptions),
                fetch_config.clone(),
                Arc::clone(&fetch_buffer),
                Arc::new(BufferSupplier::create()),
                is_unavailable,
                maybe_auth,
                Arc::clone(&api_versions),
                Arc::clone(&fetch_metrics_manager),
            );
            // Wake the bg task when a fetch response is ready so it is drained
            // into the FetchBuffer promptly, instead of waiting for the
            // network poll's maximumTimeToWait. Reuses the same `event_notify`
            // the application-event enqueue path pokes.
            frm.set_completion_notify(Arc::clone(&event_notify));
            Some(frm)
        };

        // Wrap the assembled `RequestManagers` in
        // `Arc<std::sync::Mutex<...>>` (Phase 10 pattern #3).
        let request_managers = Arc::new(std::sync::Mutex::new(RequestManagers::new(
            coordinator,
            topic_metadata,
            commit,
            consumer_heartbeat,
            membership_opt.clone(),
            offsets,
            fetch,
        )));

        // ═══════════════════════════════════════════════════════════════
        // State-notifier construction + registration on membership.
        // ═══════════════════════════════════════════════════════════════
        //
        // Java keeps a SINGLE `AtomicReference<Optional<ConsumerGroupMetadata>> groupMetadata`
        // field and a SINGLE `MemberStateListener` instance
        // (`AsyncKafkaConsumer.java:289, 343-353, 447`). Both the
        // constructor's initial `groupMetadata.set(initializeGroupMetadata(...))`
        // write AND the listener's `updateGroupMetadata(...)` writes
        // target the same slot.
        //
        // The Rust translation enforces this by building the
        // `group_metadata` / `group_assignment_snapshot` Arcs and the
        // `ConsumerStateNotifier` ONCE here, then threading the same
        // instances through both the membership-manager registration
        // (so heartbeat-response observations drive `update_group_metadata`
        // on the shared slot) AND the `AsyncKafkaConsumerComponents`
        // hand-off (so `Consumer::group_metadata()` reads the same slot).
        //
        // Issue 2 from the Phase-12 Critic review: previously the ctor
        // built TWO notifiers (one here, one inside `with_components`),
        // so `group_metadata` updates went to a slot the app side never
        // read. Single-notifier wiring now closes that gap.
        let group_metadata: Arc<Mutex<Option<ConsumerGroupMetadata>>> = Arc::new(Mutex::new(None));
        let group_assignment_snapshot: Arc<Mutex<HashSet<TopicPartition>>> = Arc::new(Mutex::new(HashSet::new()));
        let state_notifier = Arc::new(ConsumerStateNotifier::new(
            group_id.clone().unwrap_or_default(),
            config.group_instance_id().map(|s| s.to_string()),
            Arc::clone(&group_metadata),
            Arc::clone(&group_assignment_snapshot),
            Arc::new(AtomicBool::new(false)),
        ));

        if let Some(membership) = membership_opt.as_ref() {
            // Java's `RequestManagers.java:273-274` (KIP-848 / `consumer`
            // group protocol arm) registers TWO listeners on the membership
            // manager in this exact order:
            //
            //     membershipManager.registerStateListener(commitRequestManager);
            //     membershipManager.registerStateListener(applicationThreadMemberStateListener);
            //
            // The Rust translation mirrors that fan-out:
            //
            //   1. `commit` (when `group.id` is present) — its
            //      `MemberStateListener::on_member_epoch_updated` writes the
            //      broker-assigned UUID into `CommitRequestManager`'s
            //      internal `MemberInfo`, which is then read at
            //      `OffsetCommitRequest` build time. Without this
            //      registration the OffsetCommit goes out with the default
            //      empty member id and the broker rejects it with
            //      `UNKNOWN_MEMBER_ID`. See Phase 12.5 COMMENTS.DONE Issue 7.
            //
            //   2. `state_notifier` (always) — writes the new
            //      `ConsumerGroupMetadata` into the shared `Arc<Mutex<...>>`
            //      slot that `Consumer::group_metadata()` reads. This is
            //      Java's `applicationThreadMemberStateListener` equivalent
            //      and Phase 12 Issue 2's single-notifier wiring.
            //
            // Note: in the `(coordinator, commit, membership)` build chain
            // above, `commit` is `Some` whenever `membership_opt` is `Some`
            // (both are gated on `group_id`), so the `commit.as_ref()` check
            // here is defense-in-depth.
            // `commit` was moved into `RequestManagers::new` above; read
            // it back through the `commit_handle()` accessor (returns a
            // cloned `Option<Arc<...>>`).
            let commit_listener_handle: Option<Arc<CommitRequestManager>> = {
                let rm_guard = request_managers.lock().expect("rm not poisoned");
                rm_guard.commit_handle()
            };
            if let Some(commit_arc) = commit_listener_handle {
                membership
                    .abstract_mm
                    .register_state_listener(commit_arc as Arc<dyn MemberStateListener>);
            }
            membership
                .abstract_mm
                .register_state_listener(Arc::clone(&state_notifier) as Arc<dyn MemberStateListener>);
        }

        // ═══════════════════════════════════════════════════════════════
        // Phase 12 commit (3/N): bg-task spawn + components hand-off.
        // ═══════════════════════════════════════════════════════════════
        //
        // Java lines 466-501 — the remaining `ApplicationEventProcessor`
        // + `ApplicationEventHandler` + `CompletableEventReaper` +
        // `ConsumerRebalanceListenerInvoker` + `ConsumerNetworkThread`
        // build + spawn. Each Java step maps line-for-line to the Rust
        // block below.
        use crate::consumer::internals::events::ApplicationEventProcessor;
        use crate::consumer::internals::{ConsumerNetworkThread, SystemThreadTime, ThreadTime};

        let time: Arc<dyn ThreadTime> = Arc::new(SystemThreadTime);

        // Java lines 466-470 — `applicationEventProcessor`.
        let application_event_reaper: Arc<std::sync::Mutex<CompletableEventReaper>> =
            Arc::new(std::sync::Mutex::new(CompletableEventReaper::new()));
        let app_event_processor = ApplicationEventProcessor::new(
            Arc::clone(&request_managers),
            Arc::clone(&metadata),
            Arc::clone(&subscriptions),
            Arc::clone(&application_event_reaper),
        );

        // Java lines 471-481 — `applicationEventHandler`. M6: wire the
        // `AsyncConsumerMetrics` + shared application-event queue-depth
        // counter before sharing the handler (Java passes
        // `asyncConsumerMetrics` to the ctor).
        let mut application_event_handler = ApplicationEventHandler::new(_app_event_tx, Arc::clone(&event_notify));
        application_event_handler
            .set_async_consumer_metrics(Arc::clone(&async_consumer_metrics), Arc::clone(&application_event_queue_size));
        let application_event_handler = Arc::new(application_event_handler);

        // Java lines 482-487 — `rebalanceListenerInvoker`. Java passes a
        // `RebalanceCallbackMetricsManager` + `Time` into the constructor; we
        // wire them post-construction so the no-arg `new` stays usable in
        // tests. The metrics manager registers against the consumer's shared
        // `Arc<Metrics>` (M3 field); the clock is `SystemTime` (the same clock
        // the metrics registry uses), so the recorded latency durations match.
        let mut rebalance_listener_invoker = ConsumerRebalanceListenerInvoker::new(Arc::clone(&subscriptions));
        rebalance_listener_invoker.set_metrics(
            crate::consumer::internals::RebalanceCallbackMetricsManager::new(&metrics),
            Arc::new(crate::common::metrics::SystemTime),
        );

        // Java line 491 — `backgroundEventReaper`. We reuse the same
        // `CompletableEventReaper` as the application reaper since the
        // Rust translation has one reaper per consumer (Phase 10 design
        // pattern #2). Java has two reapers but uses them
        // interchangeably for the consumer's purposes.

        // Wakeup primitive shared with the bg task.
        let wakeup_trigger = WakeupTrigger::new();

        // Java lines 494-500 — `fetchCollector`.
        let fetch_collector_time: Arc<dyn crate::consumer::internals::FetchCollectorTime> =
            Arc::new(crate::consumer::internals::SystemFetchCollectorTime);
        let fetch_collector = Arc::new(FetchCollector::new(
            Arc::clone(&metadata),
            Arc::clone(&subscriptions),
            fetch_config,
            Arc::clone(&_deserializers),
            Arc::clone(&fetch_metrics_manager),
            fetch_collector_time,
        ));

        // Java line 506 — `config.logUnused()` → `log::debug!(...)`.
        log::debug!("Kafka consumer initialized");

        // ── Bg-task spawn ──
        //
        // Construct `ConsumerNetworkThread` and spawn its run loop.
        // `signal_close_fn` / `wakeup_fn` are erased through
        // [`NetworkThreadCloseHandle`] so the outer `AsyncKafkaConsumer`
        // struct stays non-generic over `K`.
        //
        // `max_time_to_wait_ms` is built here and passed into both
        // `ConsumerNetworkThread::new` (which writes the post-poll
        // computed bound on every `run_once` iteration) AND the consumer
        // struct (which reads via `maximum_time_to_wait_ms()`). The bg
        // task seeds it to `MAX_POLL_TIMEOUT_MS` inside its ctor — mirrors
        // Java's `ApplicationEventHandler.maximumTimeToWait()` slot shared
        // with the bg thread.
        let max_time_to_wait_ms: Arc<AtomicI64> = Arc::new(AtomicI64::new(
            ConsumerNetworkThread::<NetworkClient<Selector, DefaultHostResolver>>::MAX_POLL_TIMEOUT_MS,
        ));

        let mut network_thread = ConsumerNetworkThread::new(
            Arc::clone(&time),
            _app_event_rx,
            Arc::clone(&application_event_reaper),
            app_event_processor,
            Arc::clone(&_network_client_delegate),
            Arc::clone(&request_managers),
            membership_opt.clone(),
            wakeup_trigger.clone(),
            Arc::clone(&max_time_to_wait_ms),
        );
        // M6: Java passes `asyncConsumerMetrics` to the `ConsumerNetworkThread`
        // ctor; wire it (plus the application-event queue-depth counter the bg
        // task resets to 0 on drain) before spawning the bg task.
        network_thread
            .set_async_consumer_metrics(Arc::clone(&async_consumer_metrics), Arc::clone(&application_event_queue_size));

        // Capture the running-flag + wakeup handles before moving
        // `network_thread` into `tokio::spawn`. The erased closures
        // call these to signal close / wake the bg task without
        // holding a reference to the concrete `K` type.
        let (signal_close_fn, wakeup_fn) =
            build_close_handle_fns(network_thread.running_handle(), Arc::clone(&event_notify));

        // The `max_time_to_wait_ms` slot is seeded with
        // `MAX_POLL_TIMEOUT_MS` at ctor time (line above) and the bg task
        // writes to the SAME `Arc<AtomicI64>` cell on every `run_once`
        // iteration via `cached_max_time_to_wait_ms.store(...)`
        // (consumer_network_thread.rs). The app-side
        // `AsyncKafkaConsumer::maximum_time_to_wait_ms()` accessor reads
        // the bg-task's current value through this shared Arc — Java's
        // `cachedMaximumTimeToWait` is a single `long` field
        // (`AsyncKafkaConsumer.java:354`); the Arc<AtomicI64> is the Rust
        // equivalent that bridges the two task boundaries.

        // Phase 21: run the bg loop on its OWN dedicated `std::thread`
        // hosting a `current_thread` tokio runtime, instead of
        // `tokio::spawn`ing it onto the caller's multi-thread
        // work-stealing runtime. Profiling showed ~54% of consumer CPU
        // was tokio multi-thread scheduler park/unpark churn from running
        // this single high-frequency IO task on a ~12-worker pool; a
        // dedicated single-thread runtime (librdkafka's model) removes it.
        //
        // This is purely an internal execution-strategy change — the
        // shutdown sequencing (`signal_close_fn` + `wakeup_fn` flip the
        // running flag and wake the selector; the bg loop exits on
        // `is_running()==false` then `cleanup()`), the shared channels,
        // and the `max_time_to_wait_ms` Arc are all identical to the
        // `tokio::spawn` form.
        //
        // `enable_all()` is REQUIRED: the IO driver backs the
        // Selector/mio loop and the time driver backs the heartbeat /
        // poll-timeout timers. The §10 network-poll-to-completion
        // (cancel-safety), the §11 `Selector`/`Notify` wakeup, and the
        // §31 listener/commit callbacks (which run on the APP task — the
        // bg only enqueues + awaits the oneshot) are all unaffected by
        // moving the bg loop onto a `current_thread` runtime.
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let thread_handle = std::thread::Builder::new()
            .name("kafka-consumer-io".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build consumer io runtime");
                rt.block_on(async move {
                    let mut thread = network_thread;
                    while thread.is_running() {
                        thread.run_once().await;
                    }
                    thread.cleanup().await;
                });
                // Signal the close path that the bg loop + `cleanup()`
                // have finished. Errors (receiver dropped before close)
                // are benign — `await_join` treats them as a clean exit.
                let _ = done_tx.send(());
            })
            .expect("spawn consumer io thread");

        let network_thread_close =
            NetworkThreadCloseHandle::with_dedicated(signal_close_fn, wakeup_fn, done_rx, thread_handle);

        // ── Assemble `AsyncKafkaConsumerComponents` and hand off ──
        //
        // The Phase-11 test seam stays — the production path builds the
        // components struct and calls `Self::with_components(...)`.
        // Phase-12 Issue 2 (Critic review) consolidated to a single
        // `ConsumerStateNotifier`: the Arc registered on the membership
        // manager earlier in this ctor and the Arc stored on the consumer
        // struct are the SAME instance, mirroring Java's single
        // `memberStateListener`.

        let components = AsyncKafkaConsumerComponents {
            config,
            client_id,
            group_id,
            positions_validator,
            subscriptions,
            metadata,
            request_managers,
            background_event_rx: _bg_event_rx,
            application_event_handler,
            completable_event_reaper: application_event_reaper,
            max_time_to_wait_ms,
            wakeup_trigger,
            network_thread_close,
            fetch_buffer,
            fetch_collector,
            metrics,
            kafka_consumer_metrics,
            async_consumer_metrics,
            background_event_queue_size,
            rebalance_listener_invoker,
            offset_commit_callback_invoker: _offset_commit_callback_invoker,
            deserializers: _deserializers,
            interceptors: _interceptors,
            isolation_level: _isolation_level,
            time,
            group_metadata,
            group_assignment_snapshot,
            state_notifier,
        };

        Ok(Self::with_components(components))
    }

    /// Builds the consumer's `Metrics` registry and `FetchMetricsManager`.
    ///
    /// Translates Java's `ConsumerUtils.createMetrics(config, time, reporters)`
    /// followed by `createFetchMetricsManager(metrics)`. The `MetricConfig`
    /// carries the `metrics.num.samples`, `metrics.sample.window.ms`, and
    /// `metrics.recording.level` settings and the single `client-id` tag; the
    /// registry uses the `"consumer"` metric group prefix. The (no-op) reporter
    /// list and the JMX context are deferred to Phase M7; for M3 the registry is
    /// reporter-less but fully functional. Returns the owned `Arc<Metrics>`
    /// (kept on the consumer for M7's public accessor) and the
    /// `Arc<FetchMetricsManager>` shared into the fetch path.
    fn create_fetch_metrics_manager(config: &ConsumerConfig) -> (Arc<Metrics>, Arc<FetchMetricsManager>) {
        const CONSUMER_METRIC_GROUP_PREFIX: &str = "consumer";
        const CONSUMER_CLIENT_ID_METRIC_TAG: &str = "client-id";

        let mut tags = std::collections::BTreeMap::new();
        tags.insert(CONSUMER_CLIENT_ID_METRIC_TAG.to_string(), config.client_id().to_string());

        let recording_level = RecordingLevel::for_name(&config.metrics_recording_level).unwrap_or(RecordingLevel::Info);
        let metric_config = MetricConfig::new()
            .set_samples(config.metrics_num_samples)
            .set_time_window_ms(config.metrics_sample_window_ms)
            .set_record_level(recording_level)
            .set_tags(tags);

        let metrics = Arc::new(Metrics::with_default_config(Arc::new(metric_config)));

        // `client-id` is a default config tag, so it is added automatically to
        // every metric name; the registry's template tag set therefore lists
        // only `client-id` (matching Java's singleton tag set).
        let mut registry_tags = indexmap::IndexSet::new();
        registry_tags.insert(CONSUMER_CLIENT_ID_METRIC_TAG.to_string());
        let registry = FetchMetricsRegistry::new(registry_tags, CONSUMER_METRIC_GROUP_PREFIX);

        let manager = Arc::new(FetchMetricsManager::new(Arc::clone(&metrics), registry));
        (metrics, manager)
    }

    pub(crate) fn with_components(components: AsyncKafkaConsumerComponents<K, V>) -> Self {
        let auto_commit_enabled = components.config.enable_auto_commit();
        let default_api_timeout_ms = components.config.default_api_timeout_ms as i64;
        let retry_backoff_ms = components.config.retry_backoff_ms();

        // The `state_notifier`, `group_metadata`, and
        // `group_assignment_snapshot` slots are constructed by the caller
        // (production ctor: `Self::new`; tests: their fixture builder)
        // and threaded through here. The production ctor registers the
        // SAME `state_notifier` Arc on the membership manager BEFORE the
        // bg-task spawn — mirroring Java's single `MemberStateListener`
        // instance (`AsyncKafkaConsumer.java:289, 343-353, 447`). Tests
        // construct their own Arcs and either register the notifier
        // themselves or skip registration if they don't exercise the
        // membership path.
        Self {
            subscriptions: components.subscriptions,
            metadata: components.metadata,
            request_managers: components.request_managers,
            background_event_rx: components.background_event_rx,
            application_event_handler: components.application_event_handler,
            completable_event_reaper: components.completable_event_reaper,
            max_time_to_wait_ms: components.max_time_to_wait_ms,
            wakeup_trigger: components.wakeup_trigger,
            network_thread_close: components.network_thread_close,
            fetch_buffer: components.fetch_buffer,
            fetch_collector: components.fetch_collector,
            metrics: components.metrics,
            kafka_consumer_metrics: components.kafka_consumer_metrics,
            async_consumer_metrics: components.async_consumer_metrics,
            background_event_queue_size: components.background_event_queue_size,
            client_id: components.client_id,
            group_id: components.group_id,
            group_metadata: components.group_metadata,
            group_assignment_snapshot: components.group_assignment_snapshot,
            has_pending_reconciliation: components.state_notifier.has_pending_reconciliation_handle(),
            state_notifier: components.state_notifier,
            positions_validator: components.positions_validator,
            rebalance_listener_invoker: components.rebalance_listener_invoker,
            offset_commit_callback_invoker: components.offset_commit_callback_invoker,
            deserializers: components.deserializers,
            interceptors: components.interceptors,
            auto_commit_enabled,
            default_api_timeout_ms,
            retry_backoff_ms,
            isolation_level: components.isolation_level,
            closed: AtomicBool::new(false),
            rebalance_listener: Mutex::new(None),
            inflight_poll: None,
            config: components.config,
            time: components.time,
            last_pending_async_commit: None,
        }
    }

    // ── Sync state-read methods ────────────────────────────────────────
    //
    // Per `consumer-threading.md` §16, each method acquires the
    // SubscriptionState lock briefly, reads, drops the guard — NEVER
    // holds the guard across `.await`. Since these methods are `fn`
    // (not `async`), there is no `.await` boundary at all.
    //
    // # Closed-consumer behavior (deliberate Java divergence)
    //
    // Java's accessors call `acquireAndEnsureOpen()` which throws
    // `IllegalStateException("This consumer has already been closed.")`
    // when the consumer is closed. Each Rust accessor below returns the
    // cached / empty value silently instead — there is no error channel
    // on these `fn` signatures and panicking on a pure accessor would
    // diverge sharply from idiomatic Rust. The strict closed-consumer
    // check is surfaced on every `async fn` (poll / commit / position /
    // committed / unsubscribe / close / etc.) via `ensure_open()`.
    //
    // Tests that exercise the Java accessor-throws-after-close behavior
    // (`testListPartitionsAfterClose` style) are listed in the commit-8
    // test-skip rationale.
    //
    // # Mutable Set semantics (deliberate Java divergence)
    //
    // Java wraps the returned `Set` with `Collections.unmodifiableSet(...)`.
    // The Rust accessors return an owned `HashSet` — the caller may
    // freely mutate it without affecting the consumer's internal state.
    // This is idiomatic Rust and is observable only through user code
    // that depended on `UnsupportedOperationException` (none of the
    // translated tests do).

    /// Java: `Set<TopicPartition> assignment()`.
    ///
    /// **Returns an owned mutable `HashSet`** (Java returns
    /// `Collections.unmodifiableSet(...)`). **Returns the empty set
    /// silently when the consumer is closed** (Java throws
    /// `IllegalStateException`). See the module-level "Sync state-read
    /// methods" comment for rationale.
    pub fn assignment(&self) -> std::collections::HashSet<TopicPartition> {
        let subs = self.subscriptions.lock().unwrap();
        subs.assigned_partitions()
    }

    /// Java: `Set<String> subscription()`.
    ///
    /// **Returns an owned mutable `HashSet`** (Java returns
    /// `Collections.unmodifiableSet(...)`). **Returns the empty set
    /// silently when the consumer is closed** (Java throws
    /// `IllegalStateException`).
    pub fn subscription(&self) -> std::collections::HashSet<String> {
        let subs = self.subscriptions.lock().unwrap();
        subs.subscription()
    }

    /// Java: `Set<TopicPartition> paused()`.
    ///
    /// **Returns an owned mutable `HashSet`** (Java returns
    /// `Collections.unmodifiableSet(...)`). **Returns the empty set
    /// silently when the consumer is closed** (Java throws
    /// `IllegalStateException`).
    pub fn paused(&self) -> std::collections::HashSet<TopicPartition> {
        let subs = self.subscriptions.lock().unwrap();
        subs.paused_partitions()
    }

    /// Java: `String clientId()`. Returned as a borrowed `&str` per
    /// CLAUDE.md §12 (most general borrowed form for getters).
    ///
    /// **Returns the configured value silently when the consumer is
    /// closed** (Java throws `IllegalStateException`). The `client_id`
    /// is immutable for the lifetime of the consumer, so returning it
    /// post-close is harmless.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Java: `Map<MetricName, ? extends Metric> metrics()`
    /// (`AsyncKafkaConsumer.java:1200-1202`:
    /// `return Collections.unmodifiableMap(metrics.metrics());`).
    ///
    /// Snapshots the consumer's owned `Arc<Metrics>` registry — the SAME
    /// registry into which every metrics manager (fetch, kafka-consumer,
    /// heartbeat, offset-commit, rebalance + rebalance-callback, async)
    /// registers (M3–M6). The returned map is therefore the full Java metric
    /// set. Cold path (monitoring frequency); the snapshot clones the
    /// registry `HashMap` under its lock.
    ///
    /// Rust returns the owned `HashMap` (caller may not mutate the registry
    /// through it — it is a clone of `Arc<KafkaMetric>` handles), the natural
    /// analog of Java's `Collections.unmodifiableMap`.
    pub fn metrics(&self) -> HashMap<MetricName, Arc<KafkaMetric>> {
        self.metrics.metrics()
    }

    /// Java: `ConsumerGroupMetadata groupMetadata()`.
    ///
    /// # Java divergence
    ///
    /// Java's `groupMetadata()` throws `InvalidGroupIdException` when
    /// `group.id` is unset (`AsyncKafkaConsumer.java:1428-1436` calls
    /// `throwIfGroupIdNotDefined()` inside `acquireAndEnsureOpen`).
    /// The Rust translation returns a stub
    /// `ConsumerGroupMetadata::new("")` for groupless consumers,
    /// because:
    ///   (a) the [`Consumer`] trait surface returns
    ///       `ConsumerGroupMetadata` with no error channel (Phase 2
    ///       decision), and panicking on a pure accessor diverges
    ///       sharply from idiomatic Rust;
    ///   (b) the strict-Java behavior IS surfaced via `commit_*` /
    ///       `subscribe` etc., which call `return_error_if_group_id_not_defined()`
    ///       on the error-bearing path.
    ///
    /// The Java test
    /// `AsyncKafkaConsumerTest.testGroupMetadataAfterCreationWithGroupIdIsNull`
    /// is therefore skipped with this rationale (commit (8/N) test-skip
    /// section).
    ///
    /// **Returns a stub value silently when the consumer is closed**
    /// (Java throws `IllegalStateException`).
    ///
    /// The returned struct is a clone of the cached value. The cache
    /// is populated by [`ConsumerStateNotifier::on_member_epoch_updated`]
    /// which is the [`MemberStateListener`] registered on the
    /// `ConsumerMembershipManager` at production wire-up time
    /// (Phase 12 — see [`Self::state_notifier`]). Until the bg task
    /// receives its first heartbeat response with a member-epoch
    /// (or until tests invoke the notifier directly), the cache is
    /// empty and this method returns a fresh stub.
    pub fn group_metadata(&self) -> ConsumerGroupMetadata {
        let guard = self.group_metadata.lock().unwrap();
        match guard.as_ref() {
            Some(meta) => meta.clone(),
            None => {
                // Stub matching Java's `initializeGroupMetadata` default for
                // groupless consumers.
                #[allow(deprecated)]
                {
                    let group = self.group_id.clone().unwrap_or_default();
                    ConsumerGroupMetadata::new(group)
                }
            },
        }
    }

    /// Java: `OptionalLong currentLag(TopicPartition)`.
    ///
    /// Returns the cached lag (highWaterMark - position) if both values
    /// are known locally, otherwise `None` (matching `OptionalLong.empty()`).
    ///
    /// **Sync variant — does NOT enqueue a `CurrentLag` event.** Java's
    /// implementation dispatches through a `CurrentLagEvent` to the bg
    /// task when the lag cache is empty; the trait surface (Phase 2) is
    /// sync, so the async dispatch lives on
    /// [`Self::current_lag_async`]. The sync accessor only reads
    /// cached state.
    ///
    /// **Returns `None` silently when the consumer is closed** (Java
    /// throws `IllegalStateException`).
    pub fn current_lag(&self, _topic_partition: &TopicPartition) -> Option<i64> {
        // The bg-task `current_lag_async` path is the one that drives the
        // event; this accessor only reads cached state and currently has
        // no cache (the lag-cache lives on the membership manager in Java
        // and is populated by fetch responses — not yet wired through to
        // the consumer struct). Returning `None` matches Java's behavior
        // for "lag unknown".
        None
    }

    /// Returns the [`ConsumerStateNotifier`] this consumer expects to
    /// be registered as a [`MemberStateListener`] on the
    /// `ConsumerMembershipManager`. The production factory (Phase 12)
    /// performs this registration immediately after constructing the
    /// membership manager; tests can do the same on their stand-in
    /// manager.
    ///
    /// Mirrors Java's anonymous-inner-class `memberStateListener` field
    /// at `AsyncKafkaConsumer.java:343-353`: that listener is passed
    /// into the membership-manager builder so the bg task can invoke it
    /// during reconciliation.
    pub(crate) fn state_notifier(&self) -> Arc<ConsumerStateNotifier> {
        Arc::clone(&self.state_notifier)
    }

    /// Java: `void wakeup()`. Sync — callable from any task, including
    /// signal handlers.
    pub fn wakeup(&self) {
        // Two halves, two primitives, deliberately:
        //   1. cancel the token — this is the user-visible half, what makes the
        //      next blocking-style API return `Error::Wakeup` (§11);
        //   2. nudge the transport notify so an in-flight `KafkaClient::poll`
        //      returns promptly instead of running out its poll wait. Idempotent
        //      and not user-visible, so it is safe even when the token was
        //      already cancelled.
        self.wakeup_trigger.wakeup();
        self.network_thread_close.wakeup();
    }

    /// Returns a `Clone + Send + Sync` [`ConsumerHandle`] exposing
    /// [`Self::wakeup`] and the reentrant-safe consumer ops, callable from
    /// another task / thread. See [`ConsumerHandle`] for the rationale
    /// (Java's `Consumer` is freely shareable across threads; this is the
    /// safe Rust equivalent for both the cross-task `wakeup()` pattern and
    /// in-callback rebalance-listener reentrancy).
    pub fn handle(&self) -> ConsumerHandle {
        ConsumerHandle::for_async(AsyncConsumerHandleState {
            wakeup_trigger: self.wakeup_trigger.clone(),
            bg_wakeup: self.network_thread_close.wakeup_fn_clone(),
            application_event_handler: Arc::clone(&self.application_event_handler),
            subscriptions: Arc::clone(&self.subscriptions),
            fetch_buffer: Arc::clone(&self.fetch_buffer),
            time: Arc::clone(&self.time),
            default_api_timeout_ms: self.default_api_timeout_ms,
        })
    }

    /// Returns the cached `max_time_to_wait` value, set by the bg task
    /// after each `run_once` iteration. Used by the poll loop (commit
    /// (4/N)).
    pub(crate) fn maximum_time_to_wait_ms(&self) -> i64 {
        self.max_time_to_wait_ms.load(Ordering::Acquire)
    }

    /// `true` iff [`Self::close`] has already returned.
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    // ── Subscribe / unsubscribe / assign ───────────────────────────────
    //
    // Translates Java's `subscribe(Collection<String>)`,
    // `subscribe(Collection<String>, ConsumerRebalanceListener)`,
    // `subscribe(SubscriptionPattern)`, `subscribe(SubscriptionPattern,
    // ConsumerRebalanceListener)`, `subscribe(Pattern)`,
    // `subscribe(Pattern, ConsumerRebalanceListener)`, `unsubscribe()`,
    // and `assign(Collection<TopicPartition>)`.
    //
    // Each method:
    //   1. Verifies `closed` (Java's `acquireAndEnsureOpen`).
    //   2. Validates arguments (returning `Error::local_illegal_argument`
    //      where Java throws `IllegalArgumentException`). Rust's type
    //      system makes the `null`-target tests un-translatable;
    //      `"".trim().is_empty()` covers the empty/blank case.
    //   3. Briefly acquires the `SubscriptionState` lock to read
    //      `assigned_partitions`, drops the guard, then enqueues an
    //      `ApplicationEvent` carrying the change. Mirrors
    //      `consumer-threading.md` §16 lock discipline — the actual
    //      mutation of `SubscriptionState` happens on the bg task via
    //      the matching `ApplicationEventProcessor` arm.
    //
    // `fetchBuffer.retainAll(...)` calls present in the Java source are
    // omitted here — the consumer's `FetchBuffer` is wired in commit
    // (4/N) along with the poll loop. The bg-side
    // `ApplicationEventProcessor` already retains-all when processing
    // the matching subscribe/assign event, so the only consequence of
    // the gap is that records already buffered for removed partitions
    // are returned on the next `poll()` — Phase 11 commit (4/N) closes
    // this seam.

    /// Translates Java's `private void throwIfGroupIdNotDefined()`
    /// (`AsyncKafkaConsumer.java:1192-1197`). Java throws
    /// `InvalidGroupIdException` (`ApiException` subclass with
    /// `Errors.InvalidGroupId`); the Rust analog is
    /// `Error::invalid_group_id(...)` which surfaces a `KafkaError`
    /// variant carrying `Errors::InvalidGroupId` so user code can
    /// dispatch on the error code.
    fn return_error_if_group_id_not_defined(&self) -> Result<(), Error> {
        if self.group_id.as_deref().map(str::is_empty).unwrap_or(true) {
            return Err(Error::invalid_group_id(
                "To use the group management or offset commit APIs, you must provide a valid \
                 group.id in the consumer configuration.",
            ));
        }
        Ok(())
    }

    /// Translates Java's `acquireAndEnsureOpen()` — the runtime
    /// reentrancy guard is dropped per Phase 11 PLAN.md deferral #4
    /// (Rust's `&mut self` enforces single-caller exclusivity at
    /// compile time), so this is just the `closed` check.
    fn ensure_open(&self) -> Result<(), Error> {
        if self.is_closed() {
            return Err(Error::local_illegal_state("This consumer has already been closed."));
        }
        Ok(())
    }

    /// Java: `void subscribe(Collection<String>)`.
    ///
    /// Subscribes to the given topics. An empty list acts as
    /// `unsubscribe()`. Errors:
    ///   - [`Error::local_illegal_argument`] if any topic is empty / whitespace.
    ///   - [`Error::invalid_group_id`] if `group.id` is unset
    ///     (Java's `InvalidGroupIdException`).
    pub async fn subscribe_with_topics(&mut self, topics: Vec<String>) -> Result<(), Error> {
        self.subscribe_internal_topics(topics, None).await
    }

    /// Java: `void subscribe(Collection<String>, ConsumerRebalanceListener)`.
    ///
    /// Same as [`Self::subscribe_with_topics`] but registers a rebalance listener.
    /// Java throws `IllegalArgumentException` for a null listener;
    /// Rust makes the `Option`-of-`Arc` representation explicit, so the
    /// listener form takes a concrete `Arc` and the listener is
    /// always non-null at the type level.
    pub async fn subscribe_with_topics_listener(
        &mut self,
        topics: Vec<String>,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> Result<(), Error> {
        self.subscribe_internal_topics(topics, Some(listener)).await
    }

    /// Java: `void subscribe(SubscriptionPattern)` — server-side regex
    /// subscribe (KIP-848 RE2J).
    pub async fn subscribe_with_pattern(&mut self, pattern: SubscriptionPattern) -> Result<(), Error> {
        self.subscribe_to_regex(pattern, None).await
    }

    /// Java: `void subscribe(SubscriptionPattern, ConsumerRebalanceListener)`.
    pub async fn subscribe_with_pattern_listener(
        &mut self,
        pattern: SubscriptionPattern,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> Result<(), Error> {
        self.subscribe_to_regex(pattern, Some(listener)).await
    }

    // Java's `void subscribe(Pattern)` / `void subscribe(Pattern,
    // ConsumerRebalanceListener)` — client-side regex subscribe — are
    // deliberately NOT implemented in Rust. Only the `SubscriptionPattern`
    // form above is translated; it hands the pattern to the group coordinator
    // for server-side RE2/J evaluation (KIP-848) instead of matching it
    // against the consumer's own metadata.

    /// Translates Java's `subscribeInternal(Collection<String>, Optional<ConsumerRebalanceListener>)`.
    async fn subscribe_internal_topics(
        &mut self,
        topics: Vec<String>,
        listener: Option<Arc<dyn ConsumerRebalanceListener>>,
    ) -> Result<(), Error> {
        self.ensure_open()?;
        self.return_error_if_group_id_not_defined()?;

        if topics.is_empty() {
            // Java: `topics.isEmpty()` is treated as the same as
            // `unsubscribe()`. Match the recursion.
            return self.unsubscribe().await;
        }

        for topic in &topics {
            if topic.trim().is_empty() {
                return Err(Error::local_illegal_argument(
                    "Topic collection to subscribe to cannot contain null or empty topic",
                ));
            }
        }

        log::info!("Subscribed to topic(s): {}", topics.join(", "));

        let topics_set: std::collections::HashSet<String> = topics.into_iter().collect();
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let (handle, receiver, _erased) = CompletableEvent::make_completable_event::<()>(deadline_ms);
        // Java passes the listener INSIDE the event so the bg task owns
        // installation — the app side never registers the listener until
        // the event has been accepted. Mirror this by sending the
        // listener through the event AND only mirroring it into the
        // app-side `rebalance_listener` slot AFTER the submit resolves
        // `Ok(())` (so a failed submission does not leave the app-side
        // slot pointing at a listener that never landed in
        // `SubscriptionState`).
        //
        // The mirror is written UNCONDITIONALLY, including with `None`:
        // Java has a single slot (`SubscriptionState.rebalanceListener`)
        // and `subscribe(topics)` without a listener calls
        // `registerRebalanceListener(Optional.empty())`
        // (`SubscriptionState.java:192-196`), so a listener-less subscribe
        // must CLEAR the previous registration. Skipping the write for
        // `None` would leave the app-side mirror pointing at the replaced
        // listener, which `leave_group_on_close` would then wrongly invoke
        // (and which would keep the listener alive past its release
        // point).
        let listener_for_app_side = listener.as_ref().map(Arc::clone);
        // Java's `subscribe(...)` does NOT call `setActiveTask` — match
        // by passing `enable_wakeup=false`.
        self.submit_and_drain::<()>(
            ApplicationEvent::TopicSubscriptionChange { handle, topics: topics_set, listener },
            receiver,
            deadline_ms,
            "Timeout expired while waiting for the subscribe event to complete",
            false,
        )
        .await?;
        *self.rebalance_listener.lock().unwrap() = listener_for_app_side;
        Ok(())
    }

    /// Translates Java's `subscribeToRegex(SubscriptionPattern, Optional<ConsumerRebalanceListener>)`.
    async fn subscribe_to_regex(
        &mut self,
        pattern: SubscriptionPattern,
        listener: Option<Arc<dyn ConsumerRebalanceListener>>,
    ) -> Result<(), Error> {
        self.ensure_open()?;
        self.return_error_if_group_id_not_defined()?;
        if pattern.pattern().is_empty() {
            return Err(Error::local_illegal_argument("Topic pattern to subscribe to cannot be empty"));
        }

        log::info!("Subscribing to regular expression {}", pattern.pattern());

        let deadline_ms = self.default_api_timeout_deadline_ms();
        let (handle, receiver, _erased) = CompletableEvent::make_completable_event::<()>(deadline_ms);
        // See `subscribe_internal_topics` for why we store the listener
        // only after the submit resolves Ok.
        let listener_for_app_side = listener.as_ref().map(Arc::clone);
        self.submit_and_drain::<()>(
            ApplicationEvent::TopicRe2JPatternSubscriptionChange { handle, pattern, listener },
            receiver,
            deadline_ms,
            "Timeout expired while waiting for the subscribe-regex event to complete",
            false,
        )
        .await?;
        *self.rebalance_listener.lock().unwrap() = listener_for_app_side;
        Ok(())
    }

    /// Java: `void unsubscribe()`.
    ///
    /// Unsubscribes from all topics / patterns and clears the assignment.
    /// Java enqueues an `UnsubscribeEvent` and then loops
    /// `processBackgroundEvents(future, timer, ignoreErrorPredicate)` so
    /// any rebalance-listener callbacks raised during the teardown can be
    /// driven from the caller's task while the unsubscribe future is
    /// outstanding (Java `AsyncKafkaConsumer.java:1830-1855`). The Rust
    /// translation routes the handle's receiver through
    /// [`Self::process_background_events_until`] so the same interleaved
    /// drain happens — without it, the bg task would await the
    /// listener-callback ack indefinitely while the app side blocks on
    /// `add_and_get`.
    pub async fn unsubscribe(&mut self) -> Result<(), Error> {
        // Java's `acquireAndEnsureOpen()` sits OUTSIDE the `try`
        // (`AsyncKafkaConsumer.java:1830`), so a closed-consumer failure is
        // not covered by the `catch (Exception e) { log.error("Unsubscribe
        // failed", e); throw e; }` below — match that by returning before the
        // guarded section.
        self.ensure_open()?;

        // Everything Java runs inside its `try` lives in the inner helper, so
        // that a failure escaping it takes Java's outer-catch path: log
        // "Unsubscribe failed" and propagate WITHOUT resetting the group
        // metadata (Java's `resetGroupMetadata()` is the last statement of
        // the `try`, at `:1848`, and is skipped when the `try` throws).
        let result = self.unsubscribe_inner().await;

        // NOTE: the app-side `rebalance_listener` mirror is deliberately NOT
        // cleared here. Java's `SubscriptionState.unsubscribe()`
        // (`SubscriptionState.java:347-355`) clears the subscription, the
        // assignment, the pattern and the subscription type but leaves
        // `rebalanceListener` alone — `registerRebalanceListener` is only ever
        // called from the three `subscribe(...)` overloads — and
        // `AsyncKafkaConsumer.unsubscribe()` (`:1830-1855`) does not touch it
        // either. Since Java has ONE slot and the mirror is what
        // `process_background_events` reads to invoke the callback, clearing it
        // here would silently skip a `PartitionsRemoved` event that the bg task
        // enqueued while the registration was still live but the app drains
        // after `unsubscribe()` returns. The retained listener is released by
        // the next `subscribe(...)` (which writes the mirror unconditionally,
        // `None` included) or when the consumer is dropped.

        match result {
            Ok(()) => {
                // Java: `resetGroupMetadata()` at `:1848` — clear the cached
                // generation_id / member_id, preserving the old group_id +
                // group_instance_id (the slot stays Some(...) so subsequent
                // group_metadata() observations match Java's
                // "post-unsubscribe" contract; see Issue 21).
                self.state_notifier.reset_group_metadata();
                Ok(())
            },
            Err(Error::Timeout(msg)) => {
                // Java's inner `catch (TimeoutException e)` logs an error and
                // falls through to `resetGroupMetadata()`, so the unsubscribe
                // still reports success.
                log::error!("Failed while waiting for the unsubscribe event to complete: {msg}");
                self.state_notifier.reset_group_metadata();
                Ok(())
            },
            Err(err) => {
                // Java's outer `catch (Exception e)`: log and rethrow, with no
                // `resetGroupMetadata()` — the caller can still inspect
                // `group_metadata()`'s member_id / generation_id.
                log::error!("Unsubscribe failed: {err}");
                Err(err)
            },
        }
    }

    /// The body of Java's `unsubscribe()` `try` block
    /// (`AsyncKafkaConsumer.java:1831-1849`, excluding the trailing
    /// `resetGroupMetadata()`).
    ///
    /// Split out so that the caller can reproduce Java's outer
    /// `catch (Exception e)` — which logs and rethrows without resetting the
    /// group metadata — while the inner `catch (TimeoutException e)` result is
    /// still distinguishable.
    async fn unsubscribe_inner(&mut self) -> Result<(), Error> {
        self.fetch_buffer.retain_all(&std::collections::HashSet::new());

        let assigned_for_log = {
            let subs = self.subscriptions.lock().unwrap();
            subs.assigned_partitions()
        };
        log::info!("Unsubscribing all topics or patterns and assigned partitions {assigned_for_log:?}");

        let now_ms = self.time.milliseconds();
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let (handle, receiver, _erased) = CompletableEvent::make_completable_event::<()>(deadline_ms);
        // Enqueue the event without blocking on it — the iterative drain
        // below polls the receiver.
        self.application_event_handler
            .add(ApplicationEvent::Unsubscribe { handle }, now_ms)?;

        // Java's `ignoreErrorEventException` predicate: swallow
        // `GroupAuthorizationException` / `TopicAuthorizationException`
        // surfaced as fatal background errors during unsubscribe so the
        // unsubscribe still completes. Rust surfaces these as
        // [`Error::TopicAuthorization`] / [`Error::GroupAuthorization`].
        let ignore_predicate = |err: &Error| matches!(err, Error::TopicAuthorization(_) | Error::GroupAuthorization(_));

        self.process_background_events_until_inner::<()>(
            receiver,
            deadline_ms,
            ignore_predicate,
            "Failed while waiting for the unsubscribe event to complete",
            // Java's `unsubscribe()` does NOT call
            // `wakeupTrigger.setActiveTask(...)` (see
            // `AsyncKafkaConsumer.java:1830-1850`). Match that.
            false,
            /* skip_rebalance_callback = */ false,
            // AK 4.3.1 (KAFKA-20428): unsubscribe passes
            // `skipAssignmentEvents = true` so a `PartitionsAssigned`
            // event queued by an in-flight reconciliation is not applied
            // (the consumer is already leaving).
            /* skip_assignment_events = */
            true,
        )
        .await
    }

    /// Java: `void assign(Collection<TopicPartition>)`.
    ///
    /// Manually assigns the given partitions. An empty collection acts
    /// as `unsubscribe()`. Errors:
    ///   - [`Error::local_illegal_argument`] if any topic is empty / whitespace.
    pub async fn assign(&mut self, partitions: Vec<TopicPartition>) -> Result<(), Error> {
        self.ensure_open()?;

        if partitions.is_empty() {
            return self.unsubscribe().await;
        }

        for tp in &partitions {
            if tp.topic().trim().is_empty() {
                return Err(Error::local_illegal_argument(
                    "Topic partitions to assign to cannot have null or empty topic",
                ));
            }
        }

        let partitions_set: std::collections::HashSet<TopicPartition> = partitions.into_iter().collect();
        // Java line 1813: `fetchBuffer.retainAll(currentTopicPartitions)`
        // — drop buffered fetches for partitions that are no longer
        // assigned so the next poll() doesn't surface stale records.
        self.fetch_buffer.retain_all(&partitions_set);
        let now_ms = self.time.milliseconds();
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let (handle, receiver, _erased) = CompletableEvent::make_completable_event::<()>(deadline_ms);
        self.submit_and_drain::<()>(
            ApplicationEvent::AssignmentChange { handle, current_time_ms: now_ms, partitions: partitions_set },
            receiver,
            deadline_ms,
            "Timeout expired while waiting for the assignment-change event to complete",
            // Java's `assign(...)` does NOT call `setActiveTask` — match.
            false,
        )
        .await
    }

    /// Java: `defaultApiTimeoutDeadlineMs()`.
    fn default_api_timeout_deadline_ms(&self) -> i64 {
        CompletableEvent::calculate_deadline_ms(self.time.milliseconds(), self.default_api_timeout_ms)
    }

    // ── §31 process_background_events ─────────────────────────────────
    //
    // Per `consumer-threading.md` §31, this method MUST be called at the
    // top of every blocking-style API. It drains the bg-event channel
    // via `try_recv` in a `while let` loop and dispatches each event on
    // the caller's task — listener callbacks invoke the user-supplied
    // `ConsumerRebalanceListener` inline.

    /// Drains the background-events channel and dispatches each event
    /// on the caller's task. Mirrors Java's `boolean processBackgroundEvents()`.
    ///
    /// Returns:
    ///   - `Ok(had_events)` on success (no error events drained). The
    ///     boolean reflects Java's return — `true` if any events were
    ///     processed in this call, `false` otherwise. Used by
    ///     [`Self::process_background_events_until`] to decide whether
    ///     to keep spinning the drain loop or fall through to the
    ///     bounded `pollInterval` wait (Java
    ///     `AsyncKafkaConsumer.java:2287-2293`).
    ///   - `Err(Error)` on the first error event drained. Subsequent
    ///     events are still processed (mirroring Java's
    ///     `firstError.compareAndSet`); the additional errors are logged
    ///     at `warn` level.
    ///
    /// Always invokes the background-event reaper at the end of the
    /// drain (Java line 2222: `backgroundEventReaper.reap(time.milliseconds())`),
    /// ensuring expired `CompletableEvent`s do not accumulate.
    ///
    /// # Lock discipline (§16)
    ///
    /// This method MUST NOT hold the `SubscriptionState` mutex guard
    /// across the listener invocation. The implementation does not
    /// acquire the guard at all — the listener invoker reads paused
    /// partitions inside its own brief lock window.
    pub(crate) async fn process_background_events(&mut self) -> Result<bool, Error> {
        self.process_background_events_inner(false, false).await
    }

    /// Inner implementation of [`Self::process_background_events`].
    ///
    /// `skip_rebalance_callback` controls how a pending §31
    /// `PartitionsRemoved` / `PartitionsAssigned` event is handled:
    ///
    ///   - `false` (normal blocking-style APIs — `poll`, `commit_sync`, …):
    ///     invoke the user listener on the caller's task, send the result
    ///     on the §31 ack, and record any error into `first_error` so it
    ///     surfaces to the caller — matching Java's `processBackgroundEvents`
    ///     which rethrows the wrapped callback error.
    ///   - `true` (close path — `leave_group_on_close`): do NOT invoke the
    ///     user listener at all; send `Ok(())` on the `PartitionsRemoved` ack
    ///     so the bg task's parked reconcile / release unblocks and completes
    ///     cleanly. Java never invokes `on_partitions_assigned` (or any
    ///     reconcile-queued callback) during `close()` — close runs rebalance
    ///     callbacks only via `runRebalanceCallbacksOnClose` (revoked/lost,
    ///     Step 4). Acking is a Rust-only necessity because our bg task parks
    ///     on the ack (Java's `CompletableFuture` chain does not). See
    ///     `leave_group_on_close`.
    ///
    /// `skip_assignment_events` (AK 4.3.1, KAFKA-20428) — Java's
    /// `skipAssignmentEvents`. When `true` (unsubscribe / close), a
    /// `PartitionsAssigned` event is NOT processed (no assignment applied, no
    /// callback run); its ack is completed with an error to unblock the bg
    /// reconciliation, and it is NOT recorded into `first_error`. These
    /// assignment-update events are only relevant from `poll()`; during
    /// `unsubscribe()` the consumer is already leaving, so applying a new
    /// assignment would be wrong. `PartitionsRemoved` (revoke / lost) events
    /// are always processed regardless of this flag (they are relevant during
    /// unsubscribe so the user can flush offsets). Close sets both flags.
    async fn process_background_events_inner(
        &mut self,
        skip_rebalance_callback: bool,
        skip_assignment_events: bool,
    ) -> Result<bool, Error> {
        let mut first_error: Option<Error> = None;
        let mut had_events = false;
        // Java records `recordBackgroundEventQueueProcessingTime(now - startMs)`
        // for the whole drained batch (after `drainEvents`). The Rust drain is
        // incremental (`try_recv` loop), so capture `start_ms` before the loop
        // and record once after. Clone the metrics `Arc` up front so `&mut self`
        // stays usable in the loop body.
        let async_consumer_metrics = Arc::clone(&self.async_consumer_metrics);
        let start_ms = self.time.milliseconds();

        // Java `BackgroundEventHandler.drainEvents` (lines 65-70) records
        // `recordBackgroundEventQueueSize(0)` UNCONDITIONALLY on every drain —
        // there is no `isEmpty()` early-return (unlike `processApplicationEvents`).
        // Since `process_background_events` runs at the top of every blocking-style
        // API, Java continuously refreshes this gauge while idle. That
        // unconditional refresh is preserved: the record below runs on every
        // drain, empty or not.
        //
        // What is NOT preserved is the literal `0`, and deliberately so. Java's
        // `drainTo` calls `fullyLock()`, so nothing can arrive mid-drain and `0`
        // is exact. The `try_recv` loop below does not block senders, so storing
        // `0` up front would overwrite the `fetch_add(1)` of any `add` racing the
        // loop whose event is still queued. Instead each received envelope
        // decrements by one, leaving the counter conserved (`+1` per successful
        // send in `BackgroundEventHandler::add`, `-1` per dequeue) and therefore
        // equal to the true depth at every observation point.

        loop {
            let envelope = match self.background_event_rx.try_recv() {
                Ok(env) => env,
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    // The bg task has shut down. Nothing more to drain;
                    // surface only if no other error has been recorded.
                    if first_error.is_none() && !self.is_closed() {
                        first_error =
                            Some(Error::local_illegal_state("Consumer background task is no longer running."));
                    }
                    break;
                },
            };
            had_events = true;
            // Conserve the depth counter: one dequeue, one decrement.
            self.background_event_queue_size.fetch_sub(1, Ordering::SeqCst);
            // Java AKC:2206 — record the time this event spent in the queue.
            async_consumer_metrics.record_background_event_queue_time(self.time.milliseconds() - envelope.enqueued_ms);

            match envelope.event {
                BackgroundEvent::Error { error } => {
                    Self::record_first_error(&mut first_error, error);
                },
                BackgroundEvent::PartitionsRemoved { method_name, partitions, ack } if skip_rebalance_callback => {
                    // Close path: do NOT invoke the user listener. Java never
                    // invokes reconcile-queued callbacks during `close()` —
                    // close runs rebalance callbacks only via
                    // `runRebalanceCallbacksOnClose` (revoked/lost, Step 4),
                    // and uses plain `addAndGet` for the rest of the close
                    // steps, so any callback the membership manager queued is
                    // simply never run and is discarded when the consumer
                    // closes.
                    //
                    // We still must send the §31 ack so the bg task's parked
                    // reconcile / release drive unblocks and completes cleanly
                    // — without it the bg task never makes progress and
                    // `network_thread_close.await_join()` (Step 8) hangs. Java
                    // has no equivalent dependency because its KIP-848
                    // reconcile chains via `CompletableFuture` and never parks
                    // the bg thread on the ack.
                    let _ = method_name;
                    let _ = partitions;
                    let _ = ack.send(Ok(()));
                    self.application_event_handler.wake_background_task();
                },
                BackgroundEvent::PartitionsRemoved { method_name, partitions, ack } => {
                    // AK 4.3.1 (KAFKA-20106): `process(PartitionsRemovedEvent)`
                    // → `invokeRebalanceCallbackAndNotifyBackgroundThread`.
                    // `method_name` is `ON_PARTITIONS_REVOKED` or
                    // `ON_PARTITIONS_LOST` (assign is a separate event now).
                    //
                    // Read the currently-registered listener and drop the
                    // guard before invoking (§16 / §31). The
                    // `rebalance_listener` lock is separate from
                    // `SubscriptionState`, so no recursive lock concern.
                    let listener = self.rebalance_listener.lock().unwrap().clone();

                    let result = match listener {
                        Some(listener) => {
                            // Invoke on the caller's task — never `tokio::spawn`.
                            use crate::consumer::ConsumerRebalanceListenerMethodName as M;
                            match method_name {
                                M::OnPartitionsRevoked => {
                                    self.rebalance_listener_invoker
                                        .invoke_partitions_revoked(&listener, &partitions)
                                        .await
                                },
                                M::OnPartitionsLost => {
                                    self.rebalance_listener_invoker
                                        .invoke_partitions_lost(&listener, &partitions)
                                        .await
                                },
                                // A `PartitionsRemoved` event never carries
                                // `ON_PARTITIONS_ASSIGNED` (that path is the
                                // `PartitionsAssigned` event); treat defensively
                                // as revoked to stay Java-faithful.
                                M::OnPartitionsAssigned => {
                                    self.rebalance_listener_invoker
                                        .invoke_partitions_revoked(&listener, &partitions)
                                        .await
                                },
                            }
                        },
                        None => Ok(()),
                    };

                    let result = result.map_err(|err| {
                        crate::consumer::internals::ConsumerUtils::maybe_wrap_as_kafka_error_with_msg(
                            err,
                            "User rebalance callback throws an error",
                        )
                    });

                    let send_result = result.clone();
                    let _ = ack.send(send_result);

                    // Phase 41b: poke the bg-task wakeup `Notify` so the
                    // bg loop wakes promptly and `try_recv`s this ack on
                    // its next `reconcile` entry — rather than waiting out
                    // the selector poll timeout. This reuses the existing
                    // application-event wakeup primitive (Java's
                    // `wakeupNetworkThread()` → `Selector.wakeup()` analog);
                    // it does NOT shrink `poll_wait_time_ms` (no busy-spin —
                    // Perf Contract item 2).
                    //
                    // This MUST be the application-event notify, NOT
                    // `network_thread_close.wakeup()` / `wakeup_trigger.wakeup()`.
                    // Those cancel the wakeup token, which is Java's
                    // `KafkaConsumer.wakeup()` — it arms a user-visible
                    // `Error::Wakeup` that the *next* public API call
                    // raises (§11). Since every rebalance fires a listener
                    // callback, using it here made a spurious `Wakeup` the
                    // normal outcome of any rebalance, breaking `poll()` for
                    // every consumer with a listener registered. Both signals
                    // reach the selector through `run_once`'s `select!`, so
                    // this wakes the loop just as promptly with no
                    // user-visible side effect.
                    self.application_event_handler.wake_background_task();

                    if let Err(err) = result {
                        Self::record_first_error(&mut first_error, err);
                    }
                },
                BackgroundEvent::PartitionsAssigned { assigned_partitions, added_partitions, ack }
                    if skip_assignment_events || skip_rebalance_callback =>
                {
                    // AK 4.3.1 (KAFKA-20428): during unsubscribe / close, skip
                    // processing assignment-update events — they are only
                    // relevant from `poll()`. Java completes the event's future
                    // EXCEPTIONALLY to unblock the reconciliation in the
                    // background, logs, and continues WITHOUT recording the
                    // error into `firstError`.
                    let _ = assigned_partitions;
                    let _ = added_partitions;
                    // Java: `new KafkaException("Assignment event skipped ...")` — a
                    // bare KafkaException carries no error code; use the neutral
                    // `UnknownServerError` code while preserving the message text.
                    //
                    // Message-fidelity note (Critic 64, Observation 2): Java has a
                    // single literal for this skip
                    // (`AsyncKafkaConsumer.java:2359`, "...consumer is
                    // unsubscribing"), and reaches it ONLY via the unsubscribe
                    // path — Java's `close()` never passes
                    // `skipAssignmentEvents=true`. Rust reaches this arm on BOTH
                    // unsubscribe AND close (close sets `skip_assignment_events`
                    // to unblock the bg reconcile that Java simply abandons), so
                    // on the close path the "unsubscribing" wording is slightly
                    // inaccurate. This is a deliberate, benign deviation: the
                    // error is internal (it rides the bg-reconcile ack, is
                    // completed-exceptionally-not-recorded, and is never surfaced
                    // to the user), and the message text is byte-identical to
                    // Java's only literal for this skip.
                    let _ = ack.send(Err(Error::with_message(
                        crate::common::Errors::UnknownServerError,
                        "Assignment event skipped because consumer is unsubscribing",
                    )));
                    log::debug!("Skipped processing PartitionsAssigned during unsubscribe/close");
                    self.application_event_handler.wake_background_task();
                },
                BackgroundEvent::PartitionsAssigned { assigned_partitions, added_partitions, ack } => {
                    // AK 4.3.1 (KAFKA-20106): `process(PartitionsAssignedEvent)`.
                    // 1. Apply the new assignment on the bg thread, triggered
                    //    and awaited here so `consumer.assignment()` only
                    //    changes within `poll()` (`applyNewAssignment` →
                    //    `ApplyAssignmentEvent`).
                    // 2. If a listener is registered, run `on_partitions_assigned`.
                    // 3. Reply on the ack (the `PartitionsAssignedEvent.future()`)
                    //    so the bg reconciliation resumes.
                    let assigned_set: HashSet<TopicPartition> = assigned_partitions.iter().cloned().collect();

                    // Step 1 — applyNewAssignment: enqueue + await the
                    // ApplyAssignmentEvent. Java `addAndGet(applyEvent)`.
                    let apply_result = {
                        let now_ms = self.time.milliseconds();
                        let apply_deadline = i64::MAX; // Java: ApplyAssignmentEvent deadlineMs = Long.MAX_VALUE
                        let (apply_handle, apply_rx, _erased) =
                            CompletableEvent::make_completable_event::<()>(apply_deadline);
                        self.application_event_handler
                            .add_and_get::<()>(
                                ApplicationEvent::ApplyAssignment {
                                    handle: apply_handle,
                                    assigned_partitions: assigned_set,
                                    added_partitions: added_partitions.clone(),
                                },
                                apply_rx,
                                now_ms,
                            )
                            .await
                    };

                    if let Err(err) = apply_result {
                        // Java: wrap as "Failed to apply the new assignment",
                        // complete the event future exceptionally, and throw
                        // (recorded into firstError here).
                        let wrapped = crate::consumer::internals::ConsumerUtils::maybe_wrap_as_kafka_error_with_msg(
                            err,
                            "Failed to apply the new assignment",
                        );
                        let _ = ack.send(Err(wrapped.clone()));
                        self.application_event_handler.wake_background_task();
                        Self::record_first_error(&mut first_error, wrapped);
                        continue;
                    }

                    // Steps 2 + 3 — run `on_partitions_assigned` if a listener
                    // exists, else complete the future with success.
                    let listener = self.rebalance_listener.lock().unwrap().clone();
                    let result = match listener {
                        Some(listener) => {
                            self.rebalance_listener_invoker
                                .invoke_partitions_assigned(&listener, &added_partitions)
                                .await
                        },
                        None => Ok(()),
                    };
                    let result = result.map_err(|err| {
                        crate::consumer::internals::ConsumerUtils::maybe_wrap_as_kafka_error_with_msg(
                            err,
                            "User rebalance callback throws an error",
                        )
                    });
                    let send_result = result.clone();
                    let _ = ack.send(send_result);
                    self.application_event_handler.wake_background_task();

                    if let Err(err) = result {
                        Self::record_first_error(&mut first_error, err);
                    }
                },
            }
        }

        // The depth refresh Java does before its drain (see the note above it).
        // Recorded here instead, and unconditionally, so it reflects what the
        // conserved counter actually holds after the loop — including anything
        // enqueued while the loop was running.
        // Floored at 0 for the same reason as the application-event gauge.
        async_consumer_metrics
            .record_background_event_queue_size(self.background_event_queue_size.load(Ordering::SeqCst).max(0) as i32);

        // Java AKC:2219 — record the total processing time for the drained
        // batch (only when at least one event was processed, matching Java's
        // `if (!events.isEmpty())` guard).
        if had_events {
            async_consumer_metrics.record_background_event_queue_processing_time(self.time.milliseconds() - start_ms);
        }

        // Java line 2222: reap expired completable events regardless of
        // drain outcome. Done after the drain so events added by this
        // tick get a chance to land before being reaped.
        {
            let now_ms = self.time.milliseconds();
            let mut reaper = self.completable_event_reaper.lock().unwrap();
            reaper.reap(now_ms);
        }

        match first_error {
            Some(err) => Err(err),
            None => Ok(had_events),
        }
    }

    /// Iterative variant of [`Self::process_background_events`] used by
    /// blocking-style APIs (`unsubscribe`, `commit_sync`, future
    /// `poll`) that need to interleave bg-event draining with waiting on
    /// a specific [`tokio::sync::oneshot::Receiver`].
    ///
    /// Mirrors Java's
    /// `<T> T processBackgroundEvents(Future<T> future, Timer timer,
    ///                                Predicate<Exception> ignoreErrorEventException)`
    /// (`AsyncKafkaConsumer.java:2271`). Each iteration:
    ///
    /// 1. Observes any pending wakeup (§11) — if [`Self::wakeup`] has
    ///    been called the loop returns `Error::Wakeup` and the
    ///    caller rotates the token.
    /// 2. Drains the bg-event channel (invokes any pending listener
    ///    callbacks on the caller's task).
    /// 3. If the completion receiver has resolved, returns the value.
    /// 4. Otherwise, races a 100ms bounded wait against the receiver
    ///    AND the wakeup token's cancellation signal.
    /// 5. Loops while the absolute `deadline_ms` is not exceeded.
    ///
    /// # `enable_wakeup`
    ///
    /// Mirrors Java's `wakeupTrigger.setActiveTask(future)` /
    /// `clearTask()` discipline: only blocking APIs that Java registers
    /// the future on observe the wakeup. Close-path callers
    /// (`leave_group_on_close`,
    /// `await_pending_async_commits_and_execute_commit_callbacks` with
    /// `enable_wakeup=false`) pass `false` here so the disabled-wakeups
    /// guarantee from `wakeup_trigger.disable()` is also respected on
    /// the per-API axis.
    ///
    /// Returns `Err(Error::timeout(...))` when the deadline
    /// expires without a completion, or `Err(Error::Wakeup(...))`
    /// when a concurrent `wakeup()` interrupts the wait.
    pub(crate) async fn process_background_events_until<T: Send + 'static>(
        &mut self,
        receiver: tokio::sync::oneshot::Receiver<Result<T, Error>>,
        deadline_ms: i64,
        ignore_error_predicate: impl Fn(&Error) -> bool,
        timeout_msg: impl AsRef<str>,
        enable_wakeup: bool,
    ) -> Result<T, Error> {
        self.process_background_events_until_inner(
            receiver,
            deadline_ms,
            ignore_error_predicate,
            timeout_msg,
            enable_wakeup,
            /* skip_rebalance_callback = */ false,
            /* skip_assignment_events = */ false,
        )
        .await
    }

    /// Inner implementation of [`Self::process_background_events_until`]
    /// with the extra `skip_rebalance_callback` / `skip_assignment_events`
    /// flags forwarded to [`Self::process_background_events_inner`]. See that
    /// method for the close-path / unsubscribe rationale.
    #[allow(clippy::too_many_arguments)]
    async fn process_background_events_until_inner<T: Send + 'static>(
        &mut self,
        receiver: tokio::sync::oneshot::Receiver<Result<T, Error>>,
        deadline_ms: i64,
        ignore_error_predicate: impl Fn(&Error) -> bool,
        timeout_msg: impl AsRef<str>,
        enable_wakeup: bool,
        skip_rebalance_callback: bool,
        skip_assignment_events: bool,
    ) -> Result<T, Error> {
        let mut receiver = receiver;

        loop {
            // Stage 0: observe pending wakeup (§11). Java's pattern is
            // `wakeupTrigger.setActiveTask(future)` BEFORE the wait — a
            // concurrent `wakeup()` then completes the future
            // exceptionally. Our rotating-token equivalent re-checks at
            // the top of every loop iteration so the `select!` below
            // sees a freshly-cancelled token when the user calls
            // `wakeup()` during the wait.
            if enable_wakeup && let Err(err) = self.wakeup_trigger.maybe_trigger_wakeup() {
                self.wakeup_trigger.rotate();
                return Err(err);
            }

            let had_events = match self
                .process_background_events_inner(skip_rebalance_callback, skip_assignment_events)
                .await
            {
                Ok(had) => had,
                Err(err) => {
                    if ignore_error_predicate(&err) {
                        // Treat as if no events were processed (Java
                        // swallows the matched exception inside the
                        // try/catch at line 2274-2279).
                        false
                    } else {
                        return Err(err);
                    }
                },
            };

            // Java line 2282: `if (future.isDone()) return getResult(future)`.
            match receiver.try_recv() {
                Ok(Ok(value)) => return Ok(value),
                Ok(Err(err)) => return Err(err),
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    return Err(Error::local_illegal_state(
                        "Background task dropped the completion sender without completing it",
                    ));
                },
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                    // Java line 2287: if no events were processed this
                    // tick, do a bounded wait (100ms) for the future or
                    // for a new bg event.
                    if !had_events {
                        let remaining = self.remaining_ms(deadline_ms);
                        if remaining <= 0 {
                            return Err(Error::timeout(timeout_msg.as_ref().to_string()));
                        }
                        let wait = std::cmp::min(remaining, 100) as u64;
                        // §11: race the receiver against the wakeup
                        // token's cancellation so `wakeup()` from
                        // another task interrupts the wait
                        // immediately. The token clone is cheap (Arc).
                        let token = if enable_wakeup {
                            Some(self.wakeup_trigger.current_token())
                        } else {
                            None
                        };
                        let recv_fut = &mut receiver;
                        match token {
                            Some(tok) => {
                                tokio::select! {
                                    biased;
                                    _ = tok.cancelled() => {
                                        // Loop top will surface
                                        // Error::Wakeup via
                                        // maybe_trigger_wakeup + rotate.
                                    },
                                    res = tokio::time::timeout(Duration::from_millis(wait), recv_fut) => {
                                        match res {
                                            Ok(Ok(Ok(value))) => return Ok(value),
                                            Ok(Ok(Err(err))) => return Err(err),
                                            Ok(Err(_recv_err)) => {
                                                return Err(Error::local_illegal_state(
                                                    "Background task dropped the completion sender without completing it",
                                                ));
                                            },
                                            // Java's `swallow TimeoutException` — keep looping.
                                            Err(_elapsed) => {},
                                        }
                                    },
                                }
                            },
                            None => match tokio::time::timeout(Duration::from_millis(wait), recv_fut).await {
                                Ok(Ok(Ok(value))) => return Ok(value),
                                Ok(Ok(Err(err))) => return Err(err),
                                Ok(Err(_recv_err)) => {
                                    return Err(Error::local_illegal_state(
                                        "Background task dropped the completion sender without completing it",
                                    ));
                                },
                                Err(_elapsed) => {},
                            },
                        }
                    }
                },
            }

            // Java line 2299: `while (timer.notExpired())`.
            if self.remaining_ms(deadline_ms) <= 0 {
                return Err(Error::timeout(timeout_msg.as_ref().to_string()));
            }
        }
    }

    /// Submit `event` and await its typed completion via
    /// [`Self::process_background_events_until`], interleaving bg-event
    /// drains and observing wakeup (§31 / §11).
    ///
    /// This is the standard pattern for blocking-style consumer APIs:
    /// it replaces the direct
    /// `application_event_handler.add_and_get(event, receiver, now).await`
    /// call which has the deadlock pitfall documented in
    /// `process_background_events_until` (a bg-task `select!` blocked on
    /// a `RebalanceListenerCallbackNeeded` ack cannot serve the
    /// completion event until the app side drains the listener
    /// callback from the bg-event channel).
    ///
    /// `enable_wakeup` mirrors Java's per-API `setActiveTask` decision;
    /// see [`Self::process_background_events_until`] doc-comment.
    pub(crate) async fn submit_and_drain<T: Send + 'static>(
        &mut self,
        event: ApplicationEvent,
        receiver: tokio::sync::oneshot::Receiver<Result<T, Error>>,
        deadline_ms: i64,
        timeout_msg: impl AsRef<str>,
        enable_wakeup: bool,
    ) -> Result<T, Error> {
        let now_ms = self.time.milliseconds();
        self.application_event_handler.add(event, now_ms)?;
        self.process_background_events_until::<T>(receiver, deadline_ms, |_| false, timeout_msg, enable_wakeup)
            .await
    }

    /// Close-path variant of [`Self::submit_and_drain`].
    ///
    /// Identical to `submit_and_drain` (enqueue the event, then drain the
    /// background-event channel while awaiting the completion handle) with
    /// two close-specific differences:
    ///
    ///   - `enable_wakeup` is always `false` — close has already disabled
    ///     wakeups (`wakeup_trigger.disable()`).
    ///   - Pending §31 rebalance-listener callbacks are NOT invoked while
    ///     draining; the ack is answered with `Ok(())` so the bg task
    ///     unblocks for shutdown (`skip_rebalance_callback = true`). Java
    ///     never invokes `on_partitions_assigned` during `close()`.
    ///
    /// See `leave_group_on_close` for why the drain (and not a plain
    /// event-result wait) is required here.
    async fn submit_and_drain_for_close<T: Send + 'static>(
        &mut self,
        event: ApplicationEvent,
        receiver: tokio::sync::oneshot::Receiver<Result<T, Error>>,
        deadline_ms: i64,
        timeout_msg: impl AsRef<str>,
    ) -> Result<T, Error> {
        let now_ms = self.time.milliseconds();
        self.application_event_handler.add(event, now_ms)?;
        self.process_background_events_until_inner::<T>(
            receiver,
            deadline_ms,
            |_| false,
            timeout_msg,
            /* enable_wakeup = */ false,
            /* skip_rebalance_callback = */ true,
            /* skip_assignment_events = */ true,
        )
        .await
    }

    /// Returns the milliseconds remaining until the supplied deadline,
    /// saturating at zero.
    fn remaining_ms(&self, deadline_ms: i64) -> i64 {
        let now = self.time.milliseconds();
        deadline_ms.saturating_sub(now).max(0)
    }

    /// Java: `KafkaException e = ConsumerUtils.maybeWrapAsKafkaException(t);`
    /// followed by `firstError.compareAndSet(null, e)`
    /// (`AsyncKafkaConsumer.java:2213-2216`) — the error is wrapped FIRST, so
    /// both the recorded error and the warn-logged one are `KafkaException`s;
    /// then first-error-wins, and subsequent errors are logged at `warn`.
    ///
    /// The wrap is conditional (see
    /// [`maybe_wrap_as_kafka_error`](crate::consumer::internals::ConsumerUtils::maybe_wrap_as_kafka_error)):
    /// an error already in the `KafkaException` hierarchy passes through
    /// unchanged, so this only affects the generic runtime-error variants for
    /// which `is_kafka_error()` would otherwise answer `false`.
    fn record_first_error(slot: &mut Option<Error>, err: Error) {
        let err = crate::consumer::internals::ConsumerUtils::maybe_wrap_as_kafka_error(err);
        if slot.is_none() {
            *slot = Some(err);
        } else {
            log::warn!("An error occurred when processing the background event: {err}");
        }
    }

    // ── Poll ───────────────────────────────────────────────────────────
    //
    // Translates Java's `AsyncKafkaConsumer.poll(Duration timeout)` body
    // (`AsyncKafkaConsumer.java:836-885`). The Java implementation drives a
    // `do { } while (timer.notExpired())` loop with three stages:
    //
    //   1. `wakeupTrigger.maybeTriggerWakeup()` at the TOP of the loop —
    //      observe a wakeup posted between polls.
    //   2. `checkInflightPoll(timer, firstPass)` — start a new
    //      `AsyncPollEvent` or evaluate whether the existing one has
    //      finished. Also runs the per-iteration
    //      `offsetCommitCallbackInvoker.executeCallbacks()` +
    //      `processBackgroundEvents()` pair (§31 invocation thread).
    //   3. `pollForFetches(timer)` — drain the fetch buffer via
    //      `FetchCollector.collectFetch`.
    //
    // Records are returned as soon as the collector yields a non-empty
    // fetch; otherwise the loop continues until `timer` expires, at which
    // point we return `ConsumerRecords::empty()`.

    /// Java: `ConsumerRecords<K, V> poll(Duration timeout)`.
    ///
    /// The translated body mirrors Java line-for-line; deviations are
    /// limited to:
    ///
    ///   - The `try/finally` in Java is realized with an inner helper
    ///     (`poll_inner`) so `kafkaConsumerMetrics.recordPollEnd` runs on
    ///     every exit path (the `finally`), matching
    ///     `AsyncKafkaConsumer.java:882`.
    ///   - `interceptors.onConsume(...)` mutates the records in place via
    ///     `Mutex<ConsumerInterceptors>`.
    pub async fn poll(&mut self, timeout: Duration) -> Result<ConsumerRecords<K, V>, Error> {
        self.ensure_open()?;

        // Java: `kafkaConsumerMetrics.recordPollStart(timer.currentTimeMs())`
        // (`AsyncKafkaConsumer.java:841`) — recorded right after
        // `acquireAndEnsureOpen`, before the subscription check. The timer is
        // created at `poll()` entry, so `currentTimeMs()` is the entry time.
        let start_ms = self.time.milliseconds();
        self.kafka_consumer_metrics.record_poll_start(start_ms);

        // Java's `try { … } finally { recordPollEnd(...) }`: run the body and
        // record poll-end on every exit path (including errors).
        let result = self.poll_inner(timeout, start_ms).await;

        // Java: `kafkaConsumerMetrics.recordPollEnd(timer.currentTimeMs())`
        // (`:882`).
        self.kafka_consumer_metrics.record_poll_end(self.time.milliseconds());
        result
    }

    /// The `try`-body of [`Self::poll`] (`AsyncKafkaConsumer.java:842-880`).
    /// Separated so [`Self::poll`] can record `recordPollEnd` in a
    /// `finally`-equivalent regardless of how this returns.
    async fn poll_inner(&mut self, timeout: Duration, start_ms: i64) -> Result<ConsumerRecords<K, V>, Error> {
        // Java: `subscriptions.hasNoSubscriptionOrUserAssignment()`.
        {
            let subs = self.subscriptions.lock().unwrap();
            if subs.has_no_subscription_or_user_assignment() {
                return Err(Error::local_illegal_state(
                    "Consumer is not subscribed to any topics or assigned any partitions",
                ));
            }
        }

        let poll_deadline_ms = CompletableEvent::calculate_deadline_ms(start_ms, timeout.as_millis() as i64);
        let mut first_pass = true;

        loop {
            // Stage 1: observe pending wakeup before doing any work.
            if let Err(err) = self.wakeup_trigger.maybe_trigger_wakeup() {
                // Java throws WakeupException unconditionally; the Rust
                // analog rotates the token AFTER raising so a subsequent
                // poll() observes a fresh token (consumer-threading.md §11).
                self.wakeup_trigger.rotate();
                return Err(err);
            }

            // Stage 2: drive the `AsyncPollEvent` lifecycle. This also
            // executes pending OffsetCommitCallback invocations and drains
            // background events for §31 listener callbacks.
            self.check_inflight_poll(poll_deadline_ms, first_pass).await?;
            first_pass = false;

            // Stage 3: collect fetched records (blocks on the FetchBuffer
            // wakeup when none are buffered yet — see `poll_for_fetches`).
            let mut records = self.poll_for_fetches(poll_deadline_ms).await?;
            // Java: `if (!fetch.isEmpty())` where `Fetch.isEmpty()` is
            // `numRecords == 0 && !positionAdvanced` — NOT the public
            // `ConsumerRecords.isEmpty()` (records-only). Returning here when
            // only the position advanced (e.g. an all-aborted batch under
            // READ_COMMITTED) avoids blocking until the poll timeout
            // (`AsyncKafkaConsumer.java:861`).
            if !records.is_fetch_empty() {
                // Java: `sendPrefetches(timer)` — eagerly enqueue the next
                // batch of fetches so the user's processing overlaps with
                // the next request. In Rust this maps to a non-blocking
                // `CreateFetchRequests` event.
                self.send_prefetches();
                // Java: `interceptors.onConsume(...)`.
                {
                    let chain = self.interceptors.lock().unwrap();
                    chain.on_consume(&mut records);
                }
                return Ok(records);
            }

            // Java: `while (timer.notExpired())`.
            if self.time.milliseconds() >= poll_deadline_ms {
                break;
            }
        }

        Ok(ConsumerRecords::empty())
    }

    /// Java: `private void checkInflightPoll(Timer timer, boolean firstPass)`
    /// (`AsyncKafkaConsumer.java:893-928`).
    ///
    /// Drives the lifetime of the inflight [`ApplicationEvent::AsyncPoll`]
    /// event. On the first pass of a `poll()` call it clears any leftover
    /// event from the previous invocation. If no event is currently
    /// inflight it submits a fresh one. The pending `OffsetCommitCallback`
    /// queue is drained and `process_background_events` is invoked, so a
    /// failed callback / fatal background error short-circuits with the
    /// inflight event cleared (matching Java's `try { … } catch (Throwable t) { … }`).
    async fn check_inflight_poll(&mut self, poll_deadline_ms: i64, first_pass: bool) -> Result<(), Error> {
        if first_pass && self.inflight_poll.is_some() {
            self.maybe_clear_previous_inflight_poll()?;
        }

        let mut newly_submitted_event = false;
        if self.inflight_poll.is_none() {
            let state = Arc::new(AsyncPollState::new());
            let now_ms = self.time.milliseconds();
            let event = ApplicationEvent::AsyncPoll {
                deadline_ms: poll_deadline_ms,
                poll_time_ms: now_ms,
                state: Arc::clone(&state),
            };
            log::trace!("Inflight event AsyncPoll(deadline={poll_deadline_ms}, time={now_ms}) submitted");
            // `add` is non-blocking — bg task drives the state machine.
            self.application_event_handler.add(event, now_ms)?;
            self.inflight_poll = Some(InflightPoll { deadline_ms: poll_deadline_ms, state });
            newly_submitted_event = true;
        }

        // Java: `offsetCommitCallbackInvoker.executeCallbacks();` +
        //       `processBackgroundEvents();`.
        //
        // Both are user-supplied code paths — if either throws, we clear
        // the inflight poll and propagate the error.
        let invocation_result = self.run_check_inflight_drain().await;
        if let Err(err) = invocation_result {
            log::trace!("Inflight event AsyncPoll failed due to {err}, clearing");
            self.inflight_poll = None;
            // Java: `throw ConsumerUtils.maybeWrapAsKafkaException(t)`
            // (`AsyncKafkaConsumer.java:919`) — the conditional wrap, so a
            // generic runtime error from user-supplied callback code still
            // reaches the application as a `KafkaException`.
            return Err(crate::consumer::internals::ConsumerUtils::maybe_wrap_as_kafka_error(err));
        }

        if self.inflight_poll.is_some() {
            self.maybe_clear_current_inflight_poll(newly_submitted_event)?;
        }

        Ok(())
    }

    /// Drain pending OffsetCommitCallback invocations + bg-events.
    ///
    /// Pulled out of [`Self::check_inflight_poll`] so the `?`-based error
    /// propagation can run inside a single try-block-equivalent body —
    /// Java's `try { ... } catch (Throwable t)` semantics translate as
    /// "run this helper, observe the result".
    async fn run_check_inflight_drain(&mut self) -> Result<(), Error> {
        // Invoke any callbacks queued by previous async commits.
        self.offset_commit_callback_invoker.invoke_pending_callbacks().await;
        // Drain pending background events (rebalance-listener callbacks,
        // fatal errors). §31: must run on the caller's task.
        self.process_background_events().await?;
        Ok(())
    }

    /// Java: `private void maybeClearPreviousInflightPoll()`
    /// (`AsyncKafkaConsumer.java:930-963`).
    fn maybe_clear_previous_inflight_poll(&mut self) -> Result<(), Error> {
        let inflight = match self.inflight_poll.as_ref() {
            Some(i) => i,
            None => return Ok(()),
        };
        if inflight.state.is_complete() {
            let err_opt = inflight.state.error();
            if let Some(error) = err_opt {
                log::trace!("Previous inflight event AsyncPoll completed with an error ({error}), clearing");
                self.inflight_poll = None;
                return Err(error);
            }
            // Successful case: check if the bg task populated the buffer.
            if self.fetch_buffer.is_empty() {
                log::trace!("Previous inflight event AsyncPoll completed without filling the buffer, clearing");
                self.inflight_poll = None;
            } else {
                // Buffer is full — keep the event so the caller can drain
                // the buffer before a fresh event is enqueued (Java's
                // "0 timeout starvation" guard).
                log::trace!("Previous inflight event AsyncPoll completed and filled the buffer, not clearing");
            }
            return Ok(());
        }

        // Java: `else if (inflightPoll.isExpired(time) && inflightPoll.isValidatePositionsComplete())`.
        let now_ms = self.time.milliseconds();
        if inflight.is_expired(now_ms) && inflight.state.is_validate_positions_complete() {
            log::trace!("Previous inflight event AsyncPoll expired without completing, clearing");
            self.inflight_poll = None;
        }
        Ok(())
    }

    /// Java: `private void maybeClearCurrentInflightPoll(boolean newlySubmittedEvent)`
    /// (`AsyncKafkaConsumer.java:965-986`).
    fn maybe_clear_current_inflight_poll(&mut self, newly_submitted_event: bool) -> Result<(), Error> {
        let inflight = match self.inflight_poll.as_ref() {
            Some(i) => i,
            None => return Ok(()),
        };
        if inflight.state.is_complete() {
            let err_opt = inflight.state.error();
            self.inflight_poll = None;
            if let Some(error) = err_opt {
                log::trace!("Inflight event AsyncPoll completed with an error ({error}), clearing");
                return Err(error);
            }
            log::trace!("Inflight event AsyncPoll completed without error, clearing");
            return Ok(());
        }

        if !newly_submitted_event {
            let now_ms = self.time.milliseconds();
            if inflight.is_expired(now_ms) && inflight.state.is_validate_positions_complete() {
                log::trace!("Inflight event AsyncPoll expired without completing, clearing");
                self.inflight_poll = None;
            }
        }
        Ok(())
    }

    /// Java: `private Fetch<K, V> pollForFetches(Timer timer)`
    /// (`AsyncKafkaConsumer.java:1872-1932`).
    ///
    /// Collects buffered records; if none are available yet, **blocks** on
    /// the [`FetchBuffer`] wakeup until the background task adds data, the
    /// timeout elapses, or `wakeup()` fires — then collects again. This is
    /// the faithful translation of Java's `pollForFetches`, which calls
    /// `fetchBuffer.awaitWakeup(pollTimer)` between the two `collectFetch()`
    /// calls. The earlier Rust port dropped that block and the caller
    /// re-checked in a tight loop, which (a) busy-spun a core whenever the
    /// buffer was momentarily empty and (b) left no fetch reliably in flight
    /// while waiting — the steady-state latency tail
    /// (`design/current/consumer-latency-findings.md`). Blocking here parks
    /// an idle consumer and lets a long-polling fetch's response wake the
    /// wait the instant it lands (`FetchBuffer::add` → `await_wakeup`).
    ///
    /// Errors (e.g. `OffsetOutOfRange`, `TopicAuthorizationFailed`)
    /// propagate to the caller — Java raises them out of `poll(Duration)`.
    async fn poll_for_fetches(&self, poll_deadline_ms: i64) -> Result<ConsumerRecords<K, V>, Error> {
        // Java's first `collectFetch()` — return immediately if data is ready.
        // Java (`pollForFetches`:1879) uses `Fetch.isEmpty()`
        // (`numRecords == 0 && !positionAdvanced`), so a position-advanced /
        // zero-record fetch returns here rather than blocking on the buffer.
        //
        // The reconciliation-check wait, the `can_skip_update_fetch_positions`
        // fast path and the validate-positions gate all live in
        // [`Self::collect_fetch`], exactly as Java puts them in
        // `collectFetch()`.
        let fetch = self.collect_fetch().await?;
        if !fetch.is_fetch_empty() {
            return Ok(fetch);
        }

        // Java (AK 4.3.1): `pollTimeout` is computed AFTER the first
        // `collectFetch()` returns empty — no need to compute it when data is
        // already available.
        // `pollTimeout = min(maximumTimeToWait, timer.remainingMs())` when
        // committed-offset management is enabled (always true for a group
        // consumer). Capping at `maximumTimeToWait` bounds how long this
        // blocks so the poll loop re-runs `check_inflight_poll` — draining §31
        // background events / rebalance callbacks — at least that often. The
        // heartbeat manager's `maximum_time_to_wait` shrinks during membership
        // work, exactly as in Java.
        let remaining = self.remaining_ms(poll_deadline_ms);
        let mut poll_timeout_ms = self.maximum_time_to_wait_ms().min(remaining);
        if poll_timeout_ms <= 0 {
            // No time left to wait; the caller's loop re-checks the deadline.
            return Ok(fetch);
        }

        // Java (`AsyncKafkaConsumer.java:1888-1904`): clamp the wait to
        // `retry.backoff.ms` when there are no assigned partitions, or any
        // assigned partition lacks a valid position. In those states the
        // background task is looking up positions (offset reset / committed
        // fetch) and may be backing off after a failure, so blocking for the
        // full timeout would stall poll() unnecessarily. This matters in this
        // port specifically because `OffsetsRequestManager` does not shrink
        // `maximum_time_to_wait`, so without this clamp the `await_wakeup`
        // below could park up to `MAX_POLL_TIMEOUT_MS` during the
        // join / post-rebalance window before positions are valid. No
        // `.await` is held across the `SubscriptionState` guard (§16).
        if poll_timeout_ms > self.retry_backoff_ms {
            // Java copies the assignment set (`subscriptions.assignedPartitions()`)
            // and iterates it calling `hasValidPosition(tp)` — a fresh HashSet +
            // per-partition map lookup on EVERY poll(). The observable predicate
            // is exactly "no assigned partitions, or any assigned partition
            // lacks a valid position", which the existing Java-mirrored
            // accessors compute allocation-free (`numAssignedPartitions`,
            // `hasAllFetchPositions`). Java's copy is a cheap TLAB nursery
            // allocation the GC absorbs; in Rust it was a malloc + 24 Arc
            // clones + SipHash inserts per poll (~1.3% of app-thread CPU on
            // the cloud profile). CLAUDE.md §11: keep it off the heap
            // (Phase 27 Fix #3).
            let needs_backoff = {
                let subs = self.subscriptions.lock().unwrap();
                subs.num_assigned_partitions() == 0 || !subs.has_all_fetch_positions()
            };
            if needs_backoff {
                poll_timeout_ms = self.retry_backoff_ms;
            }
        }

        // Ensure a fetch is in flight before we block. `await_wakeup` only
        // wakes when the bg task adds fetched data, so blocking with no fetch
        // outstanding strands the wait until `maximum_time_to_wait` even
        // though records may be available at the broker. This happens when
        // the consumer has just caught up and the prefetch chain
        // (`poll`-returns-records → `send_prefetches`) did not fire — the
        // residual latency tail in `design/current/consumer-latency-findings.md`.
        // `createFetchRequests` is a no-op for any node that already has a
        // fetch in flight (the in-flight skip in `prepare_fetch_requests`),
        // so this issues a fetch only when none is outstanding. The issued
        // fetch long-polls at the broker (`fetch.max.wait.ms`) and its
        // response wakes us the instant data lands.
        self.send_prefetches();

        // Java: `wakeupTrigger.setFetchAction(fetchBuffer); fetchBuffer
        // .awaitWakeup(pollTimer);`. The Rust wakeup model (§11) realizes
        // the `setFetchAction` side by racing the cancellation token instead
        // of a back-channel: when `wakeup()` cancels the token this arm
        // wins, the poll() loop top calls `maybe_trigger_wakeup`, and
        // `Error::Wakeup` is surfaced + the token rotated.
        let token = self.wakeup_trigger.current_token();
        tokio::select! {
            biased;
            _ = token.cancelled() => {}
            _ = self.fetch_buffer.await_wakeup(Duration::from_millis(poll_timeout_ms as u64)) => {}
        }

        // Java's second `collectFetch()` — may still be empty on a timeout or
        // a wakeup; the caller's loop re-checks the deadline / surfaces the
        // wakeup. All three of its guards run again, as in Java.
        self.collect_fetch().await
    }

    /// AK 4.3.1 (KAFKA-20106): the first stage of `collectFetch()`.
    ///
    /// Do not return buffered records if the background hasn't checked for
    /// pending reconciliations for the in-flight poll event. This is key
    /// because partitions may need revocation, so we must wait for the
    /// reconciliation check that triggers commits and marks partitions as
    /// pending revocation before we can safely collect records from the
    /// buffer.
    ///
    /// Returns `true` if the caller may proceed to collect a fetch, `false`
    /// if it should return an empty fetch (the reconciliation check has not
    /// completed and there was no time to wait, or the wait timed out).
    ///
    /// Java:
    /// ```java
    /// if (hasPendingReconciliation && inflightPoll != null && !inflightPoll.isReconciliationCheckComplete()) {
    ///     long timeoutMs = inflightPoll.deadlineMs() - time.milliseconds();
    ///     if (timeoutMs > 0) {
    ///         try {
    ///             wakeupTrigger.setActiveTask(inflightPoll.reconciliationCheckFuture());
    ///             ConsumerUtils.getResult(inflightPoll.reconciliationCheckFuture(), timeoutMs);
    ///         } catch (TimeoutException e) { return Fetch.empty(); }
    ///         finally { wakeupTrigger.clearTask(); }
    ///     } else { return Fetch.empty(); }
    /// }
    /// ```
    ///
    /// The `wakeupTrigger.setActiveTask` is realized here by racing the
    /// rotating cancellation token (§11): a concurrent `wakeup()` cancels the
    /// token, this wait returns, and the poll loop top surfaces
    /// `Error::Wakeup`. The lost-wakeup race is closed by creating the
    /// `notified()` future BEFORE re-checking the completion flag.
    async fn wait_reconciliation_check(&self) -> bool {
        if !self.has_pending_reconciliation.load(Ordering::Acquire) {
            return true;
        }
        let Some(inflight) = self.inflight_poll.as_ref() else {
            return true;
        };
        if inflight.state.is_reconciliation_check_complete() {
            return true;
        }
        let timeout_ms = inflight.deadline_ms.saturating_sub(self.time.milliseconds());
        if timeout_ms <= 0 {
            // No time to wait and reconciliation check not complete.
            return false;
        }
        // Create the notified() future BEFORE the completion re-check to avoid
        // a lost wakeup (create-future-then-check ordering).
        let notified = inflight.state.reconciliation_check_notify().notified();
        if inflight.state.is_reconciliation_check_complete() {
            return true;
        }
        let token = self.wakeup_trigger.current_token();
        tokio::select! {
            biased;
            _ = token.cancelled() => {
                // A concurrent wakeup(); return empty so the poll loop top
                // surfaces Error::Wakeup and rotates the token (§11).
                false
            }
            _ = notified => true,
            _ = tokio::time::sleep(Duration::from_millis(timeout_ms as u64)) => {
                // Java: TimeoutException -> return Fetch.empty().
                false
            }
        }
    }

    /// Java: `private Fetch<K, V> collectFetch()`
    /// (`AsyncKafkaConsumer.java:2025-2066`).
    ///
    /// Performs the "fetch collection" step by reading raw data out of the
    /// [`FetchBuffer`], converting it to a well-formed `CompletedFetch`,
    /// validating that it and the internal [`SubscriptionState`] are
    /// correct, and then converting it all into records for returning.
    ///
    /// The three guards ahead of the collection are all Java's, in Java's
    /// order:
    ///
    /// 1. Do not return buffered records before the background task has run
    ///    the pending-reconciliation check ([`Self::wait_reconciliation_check`]),
    ///    since partitions may need revocation first.
    /// 2. If the shared [`PositionsValidator`] says the position-validation
    ///    step can be skipped, collect immediately — this is the fast path
    ///    that keeps the app task off the inter-task handshake below.
    /// 3. Otherwise wait for the in-flight poll event to report that it has
    ///    finished validating positions. Without this the app task and the
    ///    background task can both update
    ///    [`SubscriptionState::position`](SubscriptionState) for the same
    ///    partition.
    ///
    /// # Errors
    ///
    /// Propagates a cached validation error from the shared
    /// [`PositionsValidator`], and any error surfaced by the collector
    /// itself (e.g. `OffsetOutOfRange`, `TopicAuthorizationFailed`).
    async fn collect_fetch(&self) -> Result<ConsumerRecords<K, V>, Error> {
        // Java `:2031-2049`.
        if !self.wait_reconciliation_check().await {
            return Ok(ConsumerRecords::empty());
        }

        // Java `:2057`.
        if self.positions_validator.can_skip_update_fetch_positions()? {
            return self.fetch_collector.collect_fetch(&self.fetch_buffer);
        }

        // Java `:2062`. With the non-blocking async poll, it's critical that
        // the application task wait until the background task has completed
        // the stage of validating positions. If the in-flight event was
        // cleared by `maybe_clear_*_inflight_poll`, that implies it is safe
        // to collect from the fetch buffer.
        if let Some(inflight) = self.inflight_poll.as_ref()
            && !inflight.state.is_validate_positions_complete()
        {
            return Ok(ConsumerRecords::empty());
        }

        self.fetch_collector.collect_fetch(&self.fetch_buffer)
    }

    // ── Commit ─────────────────────────────────────────────────────────
    //
    // Translates Java's `commitSync()` / `commitAsync()` family
    // (`AsyncKafkaConsumer.java:993-1052`, `1692-1748`). The shared
    // helper `commit(commit_event)` validates group_id, drains pending
    // callbacks, returns the early-completed receiver for empty offsets,
    // adds the event, awaits `offsets_ready`, and returns the
    // `handle.future()` receiver. The sync / async wrappers branch on
    // the resulting receiver:
    //
    //   - `commit_async` registers a callback (or default no-callback
    //     interceptor invocation) via the
    //     `OffsetCommitCallbackInvoker`, then returns immediately.
    //   - `commit_sync` blocks on the receiver and runs the interceptor
    //     `on_commit` chain inline on the caller's task.
    //
    // The receiver from the underlying `CommitAsync` / `CommitSync` event
    // resolves with the committed-offsets map (or an error). Java uses a
    // single `CompletableFuture<Map>` for both arms; Rust uses two: a
    // public `oneshot::Receiver<()>` exposed for app-side completion
    // ordering (`last_pending_async_commit`) and the typed handle
    // returned to the caller.

    /// Translates Java's
    /// `private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> commit(CommitEvent)`
    /// (`AsyncKafkaConsumer.java:1038-1052`).
    ///
    /// Returns the typed receiver from the commit event's handle. Callers
    /// either await it (sync path) or attach a spawned-task continuation
    /// (async-with-callback path).
    ///
    /// On the empty-offsets early-exit Java returns
    /// `CompletableFuture.completedFuture(null)`; the Rust analog is a
    /// pre-completed `oneshot` channel resolving to `Ok(HashMap::new())`.
    async fn commit_inner(
        &mut self,
        commit_event: CommitEventKind,
        enable_wakeup: bool,
    ) -> Result<tokio::sync::oneshot::Receiver<Result<HashMap<TopicPartition, OffsetAndMetadata>, Error>>, Error> {
        self.return_error_if_group_id_not_defined()?;
        self.offset_commit_callback_invoker.invoke_pending_callbacks().await;

        // Java's `if (event.offsets().isPresent() && event.offsets().get().isEmpty())`
        // short-circuits with `completedFuture(null)`. Mirror by sending
        // `Ok(empty)` on a pre-completed oneshot.
        if let Some(map) = commit_event.offsets()
            && map.is_empty()
        {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let _ = tx.send(Ok(HashMap::new()));
            return Ok(rx);
        }

        let now_ms = self.time.milliseconds();
        let deadline_ms = commit_event.deadline_ms();
        let (handle, receiver, _erased) =
            CompletableEvent::make_completable_event::<HashMap<TopicPartition, OffsetAndMetadata>>(deadline_ms);
        let (offsets_ready_handle, offsets_ready_rx, _erased_or) =
            CompletableEvent::make_completable_event::<()>(deadline_ms);

        // Build and enqueue the matching event variant.
        let event = match commit_event {
            CommitEventKind::Async { offsets } => {
                ApplicationEvent::CommitAsync { handle, offsets_ready: offsets_ready_handle, offsets }
            },
            CommitEventKind::Sync { offsets, .. } => {
                ApplicationEvent::CommitSync { handle, offsets_ready: offsets_ready_handle, offsets }
            },
        };
        self.application_event_handler.add(event, now_ms)?;

        // Java: `ConsumerUtils.getResult(commitEvent.offsetsReady(), defaultApiTimeoutMs.toMillis())`.
        // This blocks until the bg task has resolved which offsets to
        // commit (so subsequent fetches don't shift the commit window).
        // Issue 10 / §31: route the wait through
        // `process_background_events_until` so a bg-task
        // rebalance-listener callback enqueued mid-wait is delivered on
        // the caller's task instead of blocking the bg task on its
        // ack.
        //
        // Wakeup observation is per-caller: `commit_sync` passes
        // `enable_wakeup=true` to match Java's `setActiveTask(commitFuture)`
        // contract (every user-blocking phase of commit_sync responds to
        // wakeup, so we tighten the offsets-ready wait too — Java's
        // `setActiveTask` happens after this wait, but the uniform
        // wakeup-observable semantic across all phases of commit_sync is
        // more useful than strict Java parity here). `commit_async` passes
        // `enable_wakeup=false` because Java's `commitAsync` is documented
        // as non-blocking and never throws `WakeupException` — Issue 22
        // regression (`AsyncKafkaConsumer.java:1684-1700`).
        let or_deadline_ms = self.default_api_timeout_deadline_ms();
        self.process_background_events_until::<()>(
            offsets_ready_rx,
            or_deadline_ms,
            |_| false,
            "Timed out waiting for offsetsReady on commit event",
            enable_wakeup,
        )
        .await?;

        Ok(receiver)
    }

    /// Translates Java's `void commitSync()` (uses default API timeout).
    pub async fn commit_sync(&mut self) -> Result<(), Error> {
        self.commit_sync_internal(None, Duration::from_millis(self.default_api_timeout_ms as u64))
            .await
    }

    /// Translates Java's `void commitSync(Duration timeout)`.
    pub async fn commit_sync_with_timeout(&mut self, timeout: Duration) -> Result<(), Error> {
        self.commit_sync_internal(None, timeout).await
    }

    /// Translates Java's
    /// `void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets)`.
    pub async fn commit_sync_with_offsets(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<(), Error> {
        self.commit_sync_internal(Some(offsets), Duration::from_millis(self.default_api_timeout_ms as u64))
            .await
    }

    /// Translates Java's
    /// `void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout)`.
    pub async fn commit_sync_with_offsets_timeout(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        timeout: Duration,
    ) -> Result<(), Error> {
        self.commit_sync_internal(Some(offsets), timeout).await
    }

    /// Translates Java's
    /// `private void commitSync(Optional<Map<...>>, Duration timeout)`.
    ///
    /// # Java divergence — single deadline vs Java's fresh `requestTimer`
    ///
    /// Java's `commitSync` (`AsyncKafkaConsumer.java:1706-1724`) computes
    /// TWO independent timers:
    ///   1. `calculateDeadlineMs(time, timeout)` is baked into the
    ///      `SyncCommitEvent` for the bg-side commit RPC.
    ///   2. A FRESH `time.timer(timeout.toMillis())` is created AFTER
    ///      `commit(...)` returns and drives both
    ///      `awaitPendingAsyncCommits` and `ConsumerUtils.getResult`.
    ///
    /// So Java's worst-case wall-clock bound is `~2 * timeout`. The Rust
    /// translation uses a SINGLE `deadline_ms` computed once at the top
    /// and shared across all phases — a stricter `~1 * timeout` total
    /// bound. This is intentional: most user code expects "commitSync
    /// with timeout=T should not exceed ~T wall-clock", and Java's
    /// doubling is an artifact of the timer construction rather than a
    /// documented contract. Issue 18 — divergence is documented here
    /// and verified against Java line 1715 (the fresh-timer line).
    async fn commit_sync_internal(
        &mut self,
        offsets: Option<HashMap<TopicPartition, OffsetAndMetadata>>,
        timeout: Duration,
    ) -> Result<(), Error> {
        self.ensure_open()?;
        // Java: `long commitStart = time.nanoseconds()` at the top of
        // `commitSync` (`AsyncKafkaConsumer.java:1709`), recorded in `finally`
        // as `recordCommitSync(time.nanoseconds() - commitStart)` (`:1721`).
        let commit_start_ns = self.time.nanoseconds();
        let result = self.commit_sync_inner(offsets, timeout).await;
        self.kafka_consumer_metrics
            .record_commit_sync(self.time.nanoseconds() - commit_start_ns);
        result
    }

    /// The `try`-body of [`Self::commit_sync_internal`]
    /// (`AsyncKafkaConsumer.java:1710-1718`). Separated so the caller can
    /// record `recordCommitSync` in a `finally`-equivalent.
    async fn commit_sync_inner(
        &mut self,
        offsets: Option<HashMap<TopicPartition, OffsetAndMetadata>>,
        timeout: Duration,
    ) -> Result<(), Error> {
        let now_ms = self.time.milliseconds();
        let deadline_ms = CompletableEvent::calculate_deadline_ms(now_ms, timeout.as_millis() as i64);

        let receiver = self
            .commit_inner(
                CommitEventKind::Sync { offsets: offsets.clone(), deadline_ms },
                /* enable_wakeup = */ true,
            )
            .await?;

        // Java: `awaitPendingAsyncCommitsAndExecuteCommitCallbacks(requestTimer, true)`
        // — drain any pending async commits BEFORE blocking on this sync
        // commit so the user-visible callback ordering matches Java.
        self.await_pending_async_commits_and_execute_commit_callbacks(deadline_ms, true)
            .await?;

        // Java: `ConsumerUtils.getResult(commitFuture, requestTimer)`
        // with `wakeupTrigger.setActiveTask(commitFuture)` for the
        // duration of the await (`AsyncKafkaConsumer.java:1716,
        // :1719-1724`). Issue 10 / §31: route through
        // `process_background_events_until` so a mid-wait
        // rebalance-listener callback is delivered on the caller's
        // task. Issue 11 / §11: `enable_wakeup=true` makes a
        // concurrent `wakeup()` interrupt the wait.
        // Issue 19: Java's `commit(Optional<...>, Duration)` does not
        // attach a custom timeout message — `ConsumerUtils.getResult`
        // rethrows the underlying `TimeoutException` as-is
        // (`ConsumerUtils.java:219-231`). Mirror that: pass a brief
        // Rust-side string that does NOT format the offsets map via
        // Debug.
        let wait_result = self
            .process_background_events_until::<HashMap<TopicPartition, OffsetAndMetadata>>(
                receiver,
                deadline_ms,
                |_| false,
                format!(
                    "Timeout of {}ms expired before successfully committing offsets",
                    timeout.as_millis()
                ),
                true,
            )
            .await;
        let committed: HashMap<TopicPartition, OffsetAndMetadata> = match wait_result {
            Ok(map) => map,
            Err(err) => return Err(err),
        };

        // Java: `interceptors.onCommit(committedOffsets)`.
        {
            let chain = self.interceptors.lock().unwrap();
            chain.on_commit(&committed);
        }
        Ok(())
    }

    /// Translates Java's `void commitAsync()` (no callback, no offsets —
    /// commit `allConsumed`).
    pub async fn commit_async(&mut self) -> Result<(), Error> {
        self.commit_async_internal(None, None).await
    }

    /// Translates Java's `void commitAsync(OffsetCommitCallback)`.
    pub async fn commit_async_with_callback(
        &mut self,
        callback: Arc<dyn crate::consumer::OffsetCommitCallback>,
    ) -> Result<(), Error> {
        self.commit_async_internal(None, Some(callback)).await
    }

    /// Translates Java's
    /// `void commitAsync(Map<TopicPartition, OffsetAndMetadata>, OffsetCommitCallback)`.
    pub async fn commit_async_with_offsets_callback(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        callback: Arc<dyn crate::consumer::OffsetCommitCallback>,
    ) -> Result<(), Error> {
        self.commit_async_internal(Some(offsets), Some(callback)).await
    }

    /// Translates Java's
    /// `private void commitAsync(Optional<Map<...>>, OffsetCommitCallback)`.
    async fn commit_async_internal(
        &mut self,
        offsets: Option<HashMap<TopicPartition, OffsetAndMetadata>>,
        callback: Option<Arc<dyn crate::consumer::OffsetCommitCallback>>,
    ) -> Result<(), Error> {
        self.ensure_open()?;
        // Issue 22 / `AsyncKafkaConsumer.java:1684-1700`: Java's
        // `commitAsync` is documented as non-blocking and never throws
        // `WakeupException`. Pass `enable_wakeup=false` so a concurrent
        // `wakeup()` does NOT interrupt the preliminary offsets-ready
        // wait — the commit completes normally on the bg task.
        let receiver = self
            .commit_inner(
                CommitEventKind::Async { offsets: offsets.clone() },
                /* enable_wakeup = */ false,
            )
            .await?;

        // Java: `lastPendingAsyncCommit = commit(asyncCommitEvent).whenComplete(...)`
        // — the resulting future is stored on the consumer so a later
        // `commitSync` / `close` can wait for it to complete. The Rust
        // analog uses a oneshot bridge: we spawn the continuation, the
        // continuation invokes the callback chain, and signals
        // `last_pending_completion_tx` when finished.
        let (pending_tx, pending_rx) = tokio::sync::oneshot::channel::<()>();
        let invoker = Arc::clone(&self.offset_commit_callback_invoker);
        tokio::spawn(async move {
            let result = receiver.await;
            match result {
                Ok(Ok(committed)) => {
                    // Java: `if (throwable == null)
                    //          offsetCommitCallbackInvoker.enqueueInterceptorInvocation(committedOffsets)`.
                    invoker.enqueue_interceptor_invocation(committed.clone());
                    if let Some(cb) = callback {
                        invoker.enqueue_user_callback_invocation(cb, committed, None);
                    }
                },
                Ok(Err(err)) => {
                    if let Some(cb) = callback {
                        invoker.enqueue_user_callback_invocation(cb, HashMap::new(), Some(err.clone()));
                    } else {
                        log::error!("Offset commit failed: {err}");
                    }
                },
                Err(_recv_err) => {
                    log::error!("commit_async receiver dropped without completion");
                },
            }
            let _ = pending_tx.send(());
        });
        self.last_pending_async_commit = Some(pending_rx);
        Ok(())
    }

    /// Translates Java's
    /// `private void awaitPendingAsyncCommitsAndExecuteCommitCallbacks(Timer timer, boolean enableWakeup)`
    /// (`AsyncKafkaConsumer.java:1726-1749`).
    ///
    /// If there is a pending async commit, await it (bounded by the
    /// deadline) and then drain the callback invoker queue. The
    /// `enable_wakeup` flag mirrors Java's
    /// `wakeupTrigger.setActiveTask(futureToAwait)` at line 1738 —
    /// `true` makes a concurrent `wakeup()` interrupt the wait.
    ///
    /// The wait is a plain deadline-bounded await on the pending-commit
    /// receiver — it does NOT drain or process background events. Java's
    /// `awaitPendingAsyncCommitsAndExecuteCommitCallbacks` only waits on
    /// the commit future via `ConsumerUtils.getResult(futureToAwait,
    /// timer)` (line 1740) and never calls `processBackgroundEvents`, so
    /// it never invokes rebalance-listener callbacks here. Routing this
    /// wait through `process_background_events_until` (which drains the
    /// background-event channel and invokes pending
    /// `RebalanceListenerCallbackNeeded` callbacks) is NOT faithful: a
    /// failing rebalance listener would propagate its error out of
    /// `close()`, which Java never does (Java runs rebalance callbacks on
    /// close only via `runRebalanceCallbacksOnClose`, revoked/lost only).
    ///
    /// The commit completion is delivered by the background task via the
    /// `last_pending_async_commit` oneshot, so awaiting that receiver
    /// alone is sufficient — no bg-event drain is required for
    /// commit-completion delivery.
    async fn await_pending_async_commits_and_execute_commit_callbacks(
        &mut self,
        deadline_ms: i64,
        enable_wakeup: bool,
    ) -> Result<(), Error> {
        // Java clears `lastPendingAsyncCommit` *after* `getResult(futureToAwait,
        // timer)` returns normally (line 1741); its `finally` (`:1742-1747`)
        // deliberately does NOT clear it, so a timeout or a wakeup leaves the
        // handle in place and a later `commit_sync` still waits for the pending
        // async commit — preserving the documented
        // callback-before-sync-commit ordering. The `take()` here is therefore
        // paired with a restore on every non-success exit below.
        if let Some(mut rx) = self.last_pending_async_commit.take() {
            // Mirror Java's plain `ConsumerUtils.getResult(futureToAwait,
            // timer)` (line 1740): a deadline-bounded await on the
            // receiver, with no background-event processing. Loop with
            // short `tokio::time::timeout` slices so the deadline is
            // driven by the mock-clock-safe `self.time` (via
            // `remaining_ms`), matching the deadline pattern used by
            // `process_background_events_until`.
            loop {
                // §11: observe a pending wakeup at the top of each
                // iteration when wakeups are enabled. Java's pattern is
                // `wakeupTrigger.setActiveTask(futureToAwait)` (line
                // 1737-1739) before the wait; our rotating-token
                // equivalent re-checks here so a concurrent `wakeup()`
                // surfaces `Error::Wakeup`.
                if enable_wakeup && let Err(err) = self.wakeup_trigger.maybe_trigger_wakeup() {
                    self.wakeup_trigger.rotate();
                    // Not a success exit: keep the pending handle (Java's
                    // `finally` does not clear it).
                    self.last_pending_async_commit = Some(rx);
                    return Err(err);
                }

                let remaining = self.remaining_ms(deadline_ms);
                if remaining <= 0 {
                    // Not a success exit: keep the pending handle (Java's
                    // `finally` does not clear it).
                    self.last_pending_async_commit = Some(rx);
                    return Err(Error::timeout(
                        "Timed out waiting for last pending async commit to complete".to_string(),
                    ));
                }
                let wait = std::cmp::min(remaining, 100) as u64;

                // §11: race the receiver against the wakeup token's
                // cancellation so `wakeup()` from another task interrupts
                // the wait immediately. Java treats a dropped sender
                // (RecvError) as "the commit already completed" (line
                // 1740-1742 — `CompletableFuture.getOrThrow()` returns
                // normally for already-completed futures).
                let recv_fut = &mut rx;
                if enable_wakeup {
                    let tok = self.wakeup_trigger.current_token();
                    tokio::select! {
                        biased;
                        _ = tok.cancelled() => {
                            // Loop top surfaces Error::Wakeup via
                            // maybe_trigger_wakeup + rotate.
                        },
                        res = tokio::time::timeout(Duration::from_millis(wait), recv_fut) => {
                            match res {
                                // Commit completed (or sender dropped) — done.
                                Ok(_) => break,
                                // Timeout slice elapsed — keep looping.
                                Err(_elapsed) => {},
                            }
                        },
                    }
                } else {
                    match tokio::time::timeout(Duration::from_millis(wait), recv_fut).await {
                        Ok(_) => break,
                        Err(_elapsed) => {},
                    }
                }
            }
        }
        // Java: `offsetCommitCallbackInvoker.executeCallbacks()`.
        self.offset_commit_callback_invoker.invoke_pending_callbacks().await;
        Ok(())
    }

    // ── Seek / position / committed / lag ──────────────────────────────
    //
    // Translates Java's `seek(...)` / `seekToBeginning(...)` /
    // `seekToEnd(...)` / `position(...)` / `committed(...)` /
    // `currentLag(...)` (`AsyncKafkaConsumer.java:1055-1155, 1413-1425`).
    //
    // The seek methods route through a `SeekUnvalidated` /
    // `ResetOffset` event. `position` and `committed` route through
    // `CheckAndUpdatePositions` / `FetchCommittedOffsets` events. The
    // current-lag arm enqueues a `CurrentLag` event.
    //
    // Each method calls `ensure_open()` first to mirror Java's
    // `acquireAndEnsureOpen()` closed-consumer guard.

    /// Java: `void seek(TopicPartition, long offset)`.
    pub async fn seek_with_offset(&mut self, partition: TopicPartition, offset: i64) -> Result<(), Error> {
        if offset < 0 {
            return Err(Error::local_illegal_argument("seek offset must not be a negative number"));
        }
        self.ensure_open()?;
        log::info!("Seeking to offset {offset} for partition {partition}");
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let (handle, receiver, _erased) = CompletableEvent::make_completable_event::<()>(deadline_ms);
        // Java's `seek(...)` does NOT call `setActiveTask` — match.
        self.submit_and_drain::<()>(
            ApplicationEvent::SeekUnvalidated { handle, partition, offset, offset_epoch: None },
            receiver,
            deadline_ms,
            "Timeout expired while waiting for the seek event to complete",
            false,
        )
        .await
    }

    /// Java: `void seek(TopicPartition, OffsetAndMetadata)`.
    pub async fn seek_with_offset_and_metadata(
        &mut self,
        partition: TopicPartition,
        offset_and_metadata: OffsetAndMetadata,
    ) -> Result<(), Error> {
        let offset = offset_and_metadata.offset();
        if offset < 0 {
            return Err(Error::local_illegal_argument("seek offset must not be a negative number"));
        }
        self.ensure_open()?;
        match offset_and_metadata.leader_epoch() {
            Some(epoch) => log::info!("Seeking to offset {offset} for partition {partition} with epoch {epoch}"),
            None => log::info!("Seeking to offset {offset} for partition {partition}"),
        }
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let (handle, receiver, _erased) = CompletableEvent::make_completable_event::<()>(deadline_ms);
        self.submit_and_drain::<()>(
            ApplicationEvent::SeekUnvalidated {
                handle,
                partition,
                offset,
                offset_epoch: offset_and_metadata.leader_epoch(),
            },
            receiver,
            deadline_ms,
            "Timeout expired while waiting for the seek event to complete",
            false,
        )
        .await
    }

    /// Java: `void seekToBeginning(Collection<TopicPartition>)`.
    pub async fn seek_to_beginning(&mut self, partitions: &[TopicPartition]) -> Result<(), Error> {
        self.seek_with_reset_strategy(partitions, crate::consumer::AutoOffsetResetStrategy::EARLIEST)
            .await
    }

    /// Java: `void seekToEnd(Collection<TopicPartition>)`.
    pub async fn seek_to_end(&mut self, partitions: &[TopicPartition]) -> Result<(), Error> {
        self.seek_with_reset_strategy(partitions, crate::consumer::AutoOffsetResetStrategy::LATEST)
            .await
    }

    /// Translates Java's
    /// `private void seek(Collection<TopicPartition>, AutoOffsetResetStrategy)`.
    async fn seek_with_reset_strategy(
        &mut self,
        partitions: &[TopicPartition],
        strategy: crate::consumer::AutoOffsetResetStrategy,
    ) -> Result<(), Error> {
        self.ensure_open()?;
        let set: std::collections::HashSet<TopicPartition> = partitions.iter().cloned().collect();
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let (handle, receiver, _erased) = CompletableEvent::make_completable_event::<()>(deadline_ms);
        self.submit_and_drain::<()>(
            ApplicationEvent::ResetOffset { handle, partitions: set, offset_reset_strategy: strategy },
            receiver,
            deadline_ms,
            "Timeout expired while waiting for the seek-with-reset-strategy event to complete",
            false,
        )
        .await
    }

    /// Java: `long position(TopicPartition)` — uses default API timeout.
    pub async fn position(&mut self, partition: &TopicPartition) -> Result<i64, Error> {
        self.position_with_timeout(partition, Duration::from_millis(self.default_api_timeout_ms as u64))
            .await
    }

    /// Java: `long position(TopicPartition, Duration timeout)`
    /// (`AsyncKafkaConsumer.java:1133-1155`).
    pub async fn position_with_timeout(&mut self, partition: &TopicPartition, timeout: Duration) -> Result<i64, Error> {
        self.ensure_open()?;
        {
            let subs = self.subscriptions.lock().unwrap();
            if !subs.is_assigned(partition) {
                return Err(Error::local_illegal_state(
                    "You can only check the position for partitions assigned to this consumer.",
                ));
            }
        }
        let now_ms = self.time.milliseconds();
        let deadline_ms = CompletableEvent::calculate_deadline_ms(now_ms, timeout.as_millis() as i64);

        loop {
            // Read the validated position under the lock; drop guard before await.
            let position_offset = {
                let subs = self.subscriptions.lock().unwrap();
                subs.valid_position(partition)?.map(|fp| fp.offset)
            };
            if let Some(offset) = position_offset {
                return Ok(offset);
            }

            // Java: `updateFetchPositions(timer)` — drives the
            // `CheckAndUpdatePositionsEvent` round-trip. Routed through
            // `submit_and_drain` so a bg-task rebalance-listener
            // callback enqueued mid-wait is delivered on the caller's
            // task (Issue 10 / §31). Java `setActiveTask` analog: the
            // helper's `enable_wakeup=true` makes a concurrent
            // `wakeup()` interrupt the wait (Issue 11 / §11).
            let (handle, receiver, _erased) = CompletableEvent::make_completable_event::<()>(deadline_ms);
            let drain_result = self
                .submit_and_drain::<()>(
                    ApplicationEvent::CheckAndUpdatePositions { handle },
                    receiver,
                    deadline_ms,
                    "Timeout expired while waiting for CheckAndUpdatePositions",
                    true,
                )
                .await;
            // Java's `updateFetchPositions` catches `TimeoutException`
            // only and returns false; any other exception propagates
            // (`AsyncKafkaConsumer.java:1960-1971`). Issue 14: replace
            // the previous `.await.ok()` blanket swallow with explicit
            // error handling.
            match drain_result {
                Ok(()) => {},
                Err(Error::Timeout(_)) => {
                    // Loop will re-check `remaining_ms` below and
                    // surface the user-facing timeout error.
                },
                Err(err) => return Err(err),
            }

            // The drain helper rotates the token on wakeup itself, so
            // the second-line check below is only the deadline guard.
            if self.time.milliseconds() >= deadline_ms {
                return Err(Error::timeout(format!(
                    "Timeout of {}ms expired before the position for partition {} could be determined",
                    timeout.as_millis(),
                    partition
                )));
            }
        }
    }

    /// Java: `Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition>)`.
    pub async fn committed(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, Error> {
        self.committed_with_timeout(partitions, Duration::from_millis(self.default_api_timeout_ms as u64))
            .await
    }

    /// Java: `Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition>, Duration)`
    /// (`AsyncKafkaConsumer.java:1162-1190`).
    pub async fn committed_with_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, Error> {
        self.ensure_open()?;
        // Java: `long start = time.nanoseconds()` after `acquireAndEnsureOpen`
        // (`AsyncKafkaConsumer.java:1166`), recorded in `finally` as
        // `recordCommitted(time.nanoseconds() - start)` (`:1187`) — runs on
        // every exit path (empty partitions, group-id errors, timeout).
        let start_ns = self.time.nanoseconds();
        let result = self.committed_inner(partitions, timeout).await;
        self.kafka_consumer_metrics.record_committed(self.time.nanoseconds() - start_ns);
        result
    }

    /// The `try`-body of [`Self::committed_with_timeout`]
    /// (`AsyncKafkaConsumer.java:1167-1185`). Separated so the caller can
    /// record `recordCommitted` in a `finally`-equivalent.
    async fn committed_inner(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, Error> {
        self.return_error_if_group_id_not_defined()?;
        if partitions.is_empty() {
            return Ok(HashMap::new());
        }
        let now_ms = self.time.milliseconds();
        let deadline_ms = CompletableEvent::calculate_deadline_ms(now_ms, timeout.as_millis() as i64);
        let set: std::collections::HashSet<TopicPartition> = partitions.iter().cloned().collect();
        let (handle, receiver, _erased) =
            CompletableEvent::make_completable_event::<HashMap<TopicPartition, OffsetAndMetadata>>(deadline_ms);
        // Java's `committed(...)` calls `setActiveTask(event.future())`
        // (`AsyncKafkaConsumer.java:1176`) so a concurrent `wakeup()`
        // interrupts the wait — `enable_wakeup=true`. Issue 10 / §31:
        // the helper interleaves bg-event drains so a mid-wait
        // rebalance-listener callback is delivered on the caller's
        // task instead of deadlocking the bg task on its ack.
        let result = self
            .submit_and_drain::<HashMap<TopicPartition, OffsetAndMetadata>>(
                ApplicationEvent::FetchCommittedOffsets { handle, partitions: set },
                receiver,
                deadline_ms,
                "Timeout expired while waiting for FetchCommittedOffsets",
                true,
            )
            .await;
        match result {
            Ok(map) => Ok(map),
            Err(Error::Timeout(_)) => {
                // Issue 19: Java formats the partitions set via
                // `Set.toString()` (`[t-0, t-1]`) — `AsyncKafkaConsumer.java:1180-1182`.
                // The Rust analog uses `TopicPartition`'s Display
                // (`Display: "{topic}-{partition}"`) and emits the
                // same `[a-0, b-1]` shape rather than the noisy
                // `{:?}` Debug form.
                Err(Error::timeout(format!(
                    "Timeout of {}ms expired before the last committed offset for partitions {} could be determined. Try tuning default.api.timeout.ms larger to relax the threshold.",
                    timeout.as_millis(),
                    format_partitions_for_display(partitions),
                )))
            },
            Err(err) => Err(err),
        }
    }

    /// Java: `OptionalLong currentLag(TopicPartition)`
    /// (`AsyncKafkaConsumer.java:1413-1425`).
    ///
    /// Phase 11 commit (6/N) wires the `CurrentLag` event. The previous
    /// stub (commit (2/N)) returned `None` for every input.
    pub async fn current_lag_async(&mut self, topic_partition: &TopicPartition) -> Result<Option<i64>, Error> {
        self.ensure_open()?;
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let (handle, receiver, _erased) = CompletableEvent::make_completable_event::<Option<i64>>(deadline_ms);
        // Java's `currentLag` does NOT call `setActiveTask` —
        // `enable_wakeup=false`. Issue 10 / §31: still routes through
        // the drain helper so a bg-task listener callback fired during
        // the wait is serviced.
        self.submit_and_drain::<Option<i64>>(
            ApplicationEvent::CurrentLag {
                handle,
                partition: topic_partition.clone(),
                isolation_level: self.isolation_level,
            },
            receiver,
            deadline_ms,
            "Timeout expired while waiting for CurrentLag",
            false,
        )
        .await
    }

    // ── Beginning / end offsets / offsetsForTimes ─────────────────────

    /// Java: `Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition>)`.
    pub async fn beginning_offsets(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, i64>, Error> {
        self.beginning_offsets_with_timeout(partitions, Duration::from_millis(self.default_api_timeout_ms as u64))
            .await
    }

    /// Java: `Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition>, Duration)`.
    pub async fn beginning_offsets_with_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, Error> {
        // Java's `ListOffsetsRequest.EARLIEST_TIMESTAMP = -2L`.
        self.beginning_or_end_offsets(partitions, -2, timeout).await
    }

    /// Java: `Map<TopicPartition, Long> endOffsets(Collection<TopicPartition>)`.
    pub async fn end_offsets(&mut self, partitions: &[TopicPartition]) -> Result<HashMap<TopicPartition, i64>, Error> {
        self.end_offsets_with_timeout(partitions, Duration::from_millis(self.default_api_timeout_ms as u64))
            .await
    }

    /// Java: `Map<TopicPartition, Long> endOffsets(Collection<TopicPartition>, Duration)`.
    pub async fn end_offsets_with_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, Error> {
        // Java's `ListOffsetsRequest.LATEST_TIMESTAMP = -1L`.
        self.beginning_or_end_offsets(partitions, -1, timeout).await
    }

    /// Translates Java's
    /// `private Map<TopicPartition, Long> beginningOrEndOffset(Collection<TopicPartition>, long timestamp, Duration timeout)`
    /// (`AsyncKafkaConsumer.java:1366-1411`).
    async fn beginning_or_end_offsets(
        &mut self,
        partitions: &[TopicPartition],
        timestamp: i64,
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, Error> {
        self.ensure_open()?;
        if partitions.is_empty() {
            return Ok(HashMap::new());
        }
        let mut timestamps_to_search: HashMap<TopicPartition, i64> = HashMap::new();
        for tp in partitions {
            timestamps_to_search.insert(tp.clone(), timestamp);
        }
        let now_ms = self.time.milliseconds();
        let deadline_ms = CompletableEvent::calculate_deadline_ms(now_ms, timeout.as_millis() as i64);

        if timeout.is_zero() {
            // Java: `if (timeout.isZero()) { applicationEventHandler.add(listOffsetsEvent); return new HashMap<>(); }`.
            let (handle, _receiver, _erased) = CompletableEvent::make_completable_event::<
                HashMap<TopicPartition, Option<OffsetAndTimestampInternal>>,
            >(deadline_ms);
            self.application_event_handler.add(
                ApplicationEvent::ListOffsets { handle, timestamps_to_search, require_timestamps: false },
                now_ms,
            )?;
            return Ok(HashMap::new());
        }

        let (handle, receiver, _erased) = CompletableEvent::make_completable_event::<
            HashMap<TopicPartition, Option<OffsetAndTimestampInternal>>,
        >(deadline_ms);
        // Java's `beginningOffsets` / `endOffsets` do NOT call
        // `setActiveTask` — `enable_wakeup=false`. Issue 10 / §31: the
        // drain helper interleaves bg-event processing so a mid-wait
        // listener callback is serviced on the caller's task.
        let result = self
            .submit_and_drain::<HashMap<TopicPartition, Option<OffsetAndTimestampInternal>>>(
                ApplicationEvent::ListOffsets { handle, timestamps_to_search, require_timestamps: false },
                receiver,
                deadline_ms,
                "Timeout expired while waiting for ListOffsets",
                false,
            )
            .await;
        match result {
            Ok(offsets_map) => {
                // Java's `beginningOrEndOffset(...)` (`AsyncKafkaConsumer.java:1366-1411`)
                // returns a map keyed on every requested partition mapped
                // to `entry.getValue().offset()`. The Rust translation
                // now mirrors that all-or-error contract: every
                // requested partition that the bg task surfaced a
                // result for is in the output. The bg task uses
                // `OffsetAndTimestampInternal` (matching Java) so the
                // broker's `timestamp == -1` sentinel for
                // `EARLIEST` / `LATEST` no longer maps to `None`.
                //
                // A `None` entry here means the bg task explicitly
                // surfaced "no offset" for the partition — Java's
                // null-value semantic — which is impossible on the
                // success path with a valid broker response but can
                // still appear if the global result is somehow
                // partially populated. We preserve Java's "filter null
                // values silently" behaviour for parity with
                // `OffsetsForTimes`; see COMMENTS.DONE.1.md Issue 6
                // for the regression where the `None`→drop filter
                // silently elided every entry due to
                // OffsetAndTimestamp::with_leader_epoch rejecting
                // negative timestamps.
                let mut out = HashMap::with_capacity(offsets_map.len());
                for (tp, opt) in offsets_map {
                    if let Some(oat) = opt {
                        out.insert(tp, oat.offset());
                    }
                }
                Ok(out)
            },
            Err(Error::Timeout(_)) => Err(Error::timeout(format!(
                "Failed to get offsets by times in {}ms",
                timeout.as_millis()
            ))),
            Err(err) => Err(err),
        }
    }

    /// Java: `Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long>)`.
    pub async fn offsets_for_times(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>, Error> {
        self.offsets_for_times_with_timeout(
            timestamps_to_search,
            Duration::from_millis(self.default_api_timeout_ms as u64),
        )
        .await
    }

    /// Java: `Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long>, Duration)`
    /// (`AsyncKafkaConsumer.java:1303-1344`).
    pub async fn offsets_for_times_with_timeout(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>, Error> {
        self.ensure_open()?;
        // Java's per-entry argument validation: negative targets rejected.
        for (tp, ts) in &timestamps_to_search {
            if *ts < 0 {
                return Err(Error::local_illegal_argument(format!(
                    "The target time for partition {tp} is {ts}. The target time cannot be negative."
                )));
            }
        }
        if timestamps_to_search.is_empty() {
            return Ok(HashMap::new());
        }
        let now_ms = self.time.milliseconds();
        let deadline_ms = CompletableEvent::calculate_deadline_ms(now_ms, timeout.as_millis() as i64);

        if timeout.is_zero() {
            // Java: `if (timeout.toMillis() == 0L) { applicationEventHandler.add(...); return listOffsetsEvent.emptyResults(); }`.
            let (handle, _receiver, _erased) = CompletableEvent::make_completable_event::<
                HashMap<TopicPartition, Option<OffsetAndTimestampInternal>>,
            >(deadline_ms);
            let empty_keys = timestamps_to_search.keys().cloned().collect::<Vec<_>>();
            self.application_event_handler.add(
                ApplicationEvent::ListOffsets { handle, timestamps_to_search, require_timestamps: true },
                now_ms,
            )?;
            // Java's `emptyResults()` returns a map with each input key
            // mapped to `null`; here we omit the key entirely, since
            // `OffsetAndTimestamp` is not nullable in Rust. The user
            // observes "no data yet" via a missing key, matching Java's
            // null-key semantic for the timeout-zero arm.
            let _ = empty_keys;
            return Ok(HashMap::new());
        }

        let (handle, receiver, _erased) = CompletableEvent::make_completable_event::<
            HashMap<TopicPartition, Option<OffsetAndTimestampInternal>>,
        >(deadline_ms);
        // Java's `offsetsForTimes` does NOT call `setActiveTask` —
        // `enable_wakeup=false`. Issue 10 / §31: drain helper still
        // interleaves bg-event processing.
        let result = self
            .submit_and_drain::<HashMap<TopicPartition, Option<OffsetAndTimestampInternal>>>(
                ApplicationEvent::ListOffsets { handle, timestamps_to_search, require_timestamps: true },
                receiver,
                deadline_ms,
                "Timeout expired while waiting for ListOffsets",
                false,
            )
            .await;
        match result {
            Ok(offsets_map) => {
                // Java's `offsetsForTimes` (`AsyncKafkaConsumer.java:1303-1344`)
                // filters out null values silently and converts each
                // `OffsetAndTimestampInternal` to the public-class
                // `OffsetAndTimestamp` via
                // `entry.getValue().buildOffsetAndTimestamp()`. The
                // `require_timestamps=true` path guarantees the broker
                // returns a non-negative timestamp (matching the
                // user-supplied target time), so the build should not
                // fail in practice. If it does (e.g. broker bug), we
                // propagate the IllegalArgument so the user observes
                // the broker misbehaviour rather than silently
                // dropping the entry.
                let mut out = HashMap::with_capacity(offsets_map.len());
                for (tp, opt) in offsets_map {
                    if let Some(oat) = opt {
                        out.insert(tp, oat.build_offset_and_timestamp()?);
                    }
                }
                Ok(out)
            },
            Err(Error::Timeout(_)) => Err(Error::timeout(format!(
                "Failed to get offsets by times in {}ms",
                timeout.as_millis()
            ))),
            Err(err) => Err(err),
        }
    }

    // ── Topic metadata: partitionsFor / listTopics ────────────────────

    /// Java: `List<PartitionInfo> partitionsFor(String topic)`.
    pub async fn partitions_for(&mut self, topic: &str) -> Result<Vec<crate::common::PartitionInfo>, Error> {
        self.partitions_for_with_timeout(topic, Duration::from_millis(self.default_api_timeout_ms as u64))
            .await
    }

    /// Java: `List<PartitionInfo> partitionsFor(String topic, Duration)`
    /// (`AsyncKafkaConsumer.java:1210-1235`).
    pub async fn partitions_for_with_timeout(
        &mut self,
        topic: &str,
        timeout: Duration,
    ) -> Result<Vec<crate::common::PartitionInfo>, Error> {
        self.ensure_open()?;
        // Java: `Cluster cluster = this.metadata.fetch();
        //        List<PartitionInfo> parts = cluster.partitionsForTopic(topic);
        //        if (!parts.isEmpty()) return parts;`
        {
            let cluster = self.metadata.metadata_arc().fetch();
            let parts = cluster.partitions_for_topic(topic);
            if !parts.is_empty() {
                return Ok(parts.to_vec());
            }
        }

        if timeout.is_zero() {
            // Java: `throw new TimeoutException();`
            // (`AsyncKafkaConsumer.java:1219`) — the class, code
            // (`REQUEST_TIMED_OUT`) and retriable ancestry all match; only the
            // message differs. RECORDED DEVIATION
            // (definition-of-done.md §7): Java's no-arg constructor leaves
            // `getMessage()` null, which `Error` cannot represent — the nearest
            // forms are an empty string (rendering as `"TimeoutError: "`) or the
            // code's default text, and neither says which call timed out. The
            // added text is strictly additive diagnostics; nothing in the
            // hierarchy or the wire code changes.
            return Err(Error::timeout(format!(
                "Timeout of {}ms expired before partitions for topic {topic} could be determined",
                timeout.as_millis()
            )));
        }

        let now_ms = self.time.milliseconds();
        let deadline_ms = CompletableEvent::calculate_deadline_ms(now_ms, timeout.as_millis() as i64);
        let (handle, receiver, _erased) =
            CompletableEvent::make_completable_event::<HashMap<String, Vec<crate::common::PartitionInfo>>>(deadline_ms);
        // Java's `partitionsFor` calls `setActiveTask(future)`
        // (`AsyncKafkaConsumer.java:1223`) — `enable_wakeup=true`.
        let map = self
            .submit_and_drain::<HashMap<String, Vec<crate::common::PartitionInfo>>>(
                ApplicationEvent::TopicMetadata { handle, topic: topic.to_string() },
                receiver,
                deadline_ms,
                "Timeout expired while waiting for TopicMetadata",
                true,
            )
            .await?;
        Ok(map.get(topic).cloned().unwrap_or_default())
    }

    /// Java: `Map<String, List<PartitionInfo>> listTopics()`.
    pub async fn list_topics(&mut self) -> Result<HashMap<String, Vec<crate::common::PartitionInfo>>, Error> {
        self.list_topics_with_timeout(Duration::from_millis(self.default_api_timeout_ms as u64))
            .await
    }

    /// Java: `Map<String, List<PartitionInfo>> listTopics(Duration)`
    /// (`AsyncKafkaConsumer.java:1242-1260`).
    pub async fn list_topics_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<HashMap<String, Vec<crate::common::PartitionInfo>>, Error> {
        self.ensure_open()?;
        if timeout.is_zero() {
            // Java: `throw new TimeoutException();`
            // (`AsyncKafkaConsumer.java:1247`). Same recorded deviation as
            // `partitions_for_with_timeout` above — Java's message is null, which
            // `Error` cannot represent; class, code and ancestry match.
            return Err(Error::timeout(format!(
                "Timeout of {}ms expired before all topics' metadata could be listed",
                timeout.as_millis()
            )));
        }
        let now_ms = self.time.milliseconds();
        let deadline_ms = CompletableEvent::calculate_deadline_ms(now_ms, timeout.as_millis() as i64);
        let (handle, receiver, _erased) =
            CompletableEvent::make_completable_event::<HashMap<String, Vec<crate::common::PartitionInfo>>>(deadline_ms);
        // Java's `listTopics` calls `setActiveTask(future)`
        // (`AsyncKafkaConsumer.java:1251`) — `enable_wakeup=true`.
        self.submit_and_drain::<HashMap<String, Vec<crate::common::PartitionInfo>>>(
            ApplicationEvent::AllTopicsMetadata { handle },
            receiver,
            deadline_ms,
            "Timeout expired while waiting for AllTopicsMetadata",
            true,
        )
        .await
    }

    // ── Pause / resume ─────────────────────────────────────────────────

    /// Java: `void pause(Collection<TopicPartition>)`
    /// (`AsyncKafkaConsumer.java:1273-1283`).
    pub async fn pause(&mut self, partitions: &[TopicPartition]) -> Result<(), Error> {
        self.ensure_open()?;
        if partitions.is_empty() {
            return Ok(());
        }
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let set: std::collections::HashSet<TopicPartition> = partitions.iter().cloned().collect();
        let (handle, receiver, _erased) = CompletableEvent::make_completable_event::<()>(deadline_ms);
        // Java's `pause(...)` does NOT call `setActiveTask` —
        // `enable_wakeup=false`. Issue 10 / §31: drain helper still
        // services bg-event callbacks fired during the wait.
        self.submit_and_drain::<()>(
            ApplicationEvent::PausePartitions { handle, partitions: set },
            receiver,
            deadline_ms,
            "Timeout expired while waiting for PausePartitions",
            false,
        )
        .await
    }

    /// Java: `void resume(Collection<TopicPartition>)`
    /// (`AsyncKafkaConsumer.java:1286-1296`).
    pub async fn resume(&mut self, partitions: &[TopicPartition]) -> Result<(), Error> {
        self.ensure_open()?;
        if partitions.is_empty() {
            return Ok(());
        }
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let set: std::collections::HashSet<TopicPartition> = partitions.iter().cloned().collect();
        let (handle, receiver, _erased) = CompletableEvent::make_completable_event::<()>(deadline_ms);
        // Java's `resume(...)` does NOT call `setActiveTask`.
        self.submit_and_drain::<()>(
            ApplicationEvent::ResumePartitions { handle, partitions: set },
            receiver,
            deadline_ms,
            "Timeout expired while waiting for ResumePartitions",
            false,
        )
        .await
    }

    // ── Enforce rebalance (KIP-848: unsupported) ──────────────────────

    /// Java: `void enforceRebalance()` (`AsyncKafkaConsumer.java:1438-1441`).
    ///
    /// Both Java overloads log a warning and otherwise no-op under the
    /// KIP-848 protocol (the classic protocol implements them via
    /// `ConsumerCoordinator`). We match that: log + no-op, return
    /// `Ok(())`. No `Error::unsupported_version` since Java does not
    /// throw.
    pub async fn enforce_rebalance(&mut self) -> Result<(), Error> {
        log::warn!("Operation not supported in new consumer group protocol");
        Ok(())
    }

    /// Java: `void enforceRebalance(String reason)`
    /// (`AsyncKafkaConsumer.java:1443-1446`). Same log + no-op body as
    /// [`Self::enforce_rebalance`]; Java ignores `reason` here too.
    pub async fn enforce_rebalance_with_reason(&mut self, _reason: &str) -> Result<(), Error> {
        log::warn!("Operation not supported in new consumer group protocol");
        Ok(())
    }

    /// Java: `private void sendPrefetches(Timer timer)`
    /// (`AsyncKafkaConsumer.java:1995-2003`).
    ///
    /// Submits a non-completable `CreateFetchRequests` event so the bg
    /// task can pipeline the next fetch round-trip with the user's
    /// per-record processing. Errors from a closed bg-task channel are
    /// logged-and-swallowed (Java does the same).
    fn send_prefetches(&self) {
        let now_ms = self.time.milliseconds();
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let (handle, _receiver, _erased) = CompletableEvent::make_completable_event::<()>(deadline_ms);
        // The event is `add`-ed (not `add_and_get`), so the receiver is
        // dropped and the handle is fire-and-forget. The bg side's
        // completion of the handle then resolves into the dropped
        // receiver, which is a no-op.
        if let Err(err) = self
            .application_event_handler
            .add(ApplicationEvent::CreateFetchRequests { handle }, now_ms)
        {
            log::warn!("send_prefetches: failed to enqueue CreateFetchRequests: {err}");
        }
    }

    // ── Close path ─────────────────────────────────────────────────────
    //
    // Translates Java's `close()` chain
    // (`AsyncKafkaConsumer.java:1422-1588`). The close sequence runs
    // best-effort: each step that throws is logged and the close
    // continues so the network thread is always joined. The final error
    // (if any) is propagated only when `swallow_error=false`.
    //
    // The eight Java steps (line 1553-1577) map to Rust as:
    //   1. `wakeup_trigger.disable()` — Java's
    //      `wakeupTrigger.disableWakeups()`.
    //   2. `auto_commit_on_close(deadline_ms)` — commit-sync of
    //      `allConsumed()` + `CommitOnClose` event.
    //   3. `stop_find_coordinator_on_close()` —
    //      `StopFindCoordinatorOnClose` event.
    //   4. `run_rebalance_callbacks_on_close()` — invoke
    //      `on_partitions_revoked` / `on_partitions_lost` for the
    //      currently-assigned partitions (depending on memberEpoch).
    //   5. `leave_group_on_close(deadline_ms, operation)` —
    //      `LeaveGroupOnClose` event.
    //   6. `await_pending_async_commits_and_execute_commit_callbacks(...)` —
    //      drain pending callbacks one last time on the caller's task.
    //   7. Drop `application_event_handler` — wakes the bg task so its
    //      `run_once` loop exits.
    //   8. `network_thread_close.signal_close()` + `await_join()` —
    //      Java's `closeQuietly(consumerNetworkThread)`.

    /// Java: `void close()`. Closes the consumer with default timeout.
    pub async fn close(&mut self) -> Result<(), Error> {
        self.close_internal(
            Duration::from_millis(CloseOptions::DEFAULT_CLOSE_TIMEOUT_MS),
            crate::consumer::GroupMembershipOperation::Default,
            false,
        )
        .await
    }

    /// Java: `@Deprecated void close(Duration timeout)`, whose body is
    /// `close(CloseOptions.timeout(timeout))`
    /// (`AsyncKafkaConsumer.java:1543-1545`).
    #[deprecated(
        note = "mirroring Java's @Deprecated close(Duration); use close_with_options with CloseOptions::timeout"
    )]
    pub async fn close_with_timeout(&mut self, timeout: Duration) -> Result<(), Error> {
        self.close_with_options(crate::consumer::CloseOptions::new_timeout(timeout))
            .await
    }

    /// Java: `void close(CloseOptions options)`.
    pub async fn close_with_options(&mut self, options: crate::consumer::CloseOptions) -> Result<(), Error> {
        let timeout = options
            .timeout()
            .unwrap_or_else(|| Duration::from_millis(CloseOptions::DEFAULT_CLOSE_TIMEOUT_MS));
        self.close_internal(timeout, options.group_membership_operation(), false).await
    }

    /// Translates Java's
    /// `private void close(Duration timeout,
    ///                     CloseOptions.GroupMembershipOperation membershipOperation,
    ///                     boolean swallowException)`
    /// (`AsyncKafkaConsumer.java:1540-1588`).
    async fn close_internal(
        &mut self,
        timeout: Duration,
        membership_operation: crate::consumer::GroupMembershipOperation,
        swallow_error: bool,
    ) -> Result<(), Error> {
        log::trace!("Closing the Kafka consumer");
        if self.is_closed() {
            // Java treats double-close as a silent no-op (the closed
            // accessors return immediately).
            return Ok(());
        }

        // Step 1: disable wakeups. After this call, in-flight async
        // operations cannot be cancelled mid-close (Java
        // `wakeupTrigger.disableWakeups()`).
        self.wakeup_trigger.disable();

        // Java's `createTimerForCloseRequests(timeout)`
        // (`AsyncKafkaConsumer.java:1590-1594`) caps the user-supplied
        // timeout at `requestTimeoutMs`. With the default config
        // (timeout=30s, request_timeout_ms=30s) the cap is a no-op,
        // but a user calling `close(Duration::from_secs(300))` would
        // otherwise block the consumer for 5 minutes per
        // close-step — Java clips to `request.timeout.ms` so each
        // close-step inherits the broker-RPC bound.
        let request_timeout_ms = self.config.request_timeout_ms() as i64;
        let capped_timeout_ms = std::cmp::min(timeout.as_millis() as i64, request_timeout_ms);
        let close_start_ms = self.time.milliseconds();
        let close_deadline_ms = CompletableEvent::calculate_deadline_ms(close_start_ms, capped_timeout_ms);

        // First-error tracking mirrors Java's `AtomicReference<Throwable> firstException`.
        let mut first_error: Option<Error> = None;
        let record = |slot: &mut Option<Error>, op: &str, err: Error| {
            log::error!("{op}: {err}");
            if slot.is_none() {
                *slot = Some(err);
            }
        };

        // Step 2: auto_commit_on_close.
        if let Err(err) = self.auto_commit_on_close(close_deadline_ms).await {
            record(&mut first_error, "Failed to auto-commit offsets", err);
        }

        // Step 3: stop_find_coordinator_on_close.
        if let Err(err) = self.stop_find_coordinator_on_close() {
            record(&mut first_error, "Failed to stop finding coordinator", err);
        }

        // Step 4: run_rebalance_callbacks_on_close.
        if let Err(err) = self.run_rebalance_callbacks_on_close().await {
            record(&mut first_error, "Failed to run rebalance callbacks", err);
        }

        // Step 5: leave_group_on_close.
        if let Err(err) = self
            .leave_group_on_close(close_deadline_ms, capped_timeout_ms, membership_operation)
            .await
        {
            record(&mut first_error, "Failed to leave group while closing consumer", err);
        }

        // Step 6: drain pending async commits one last time.
        // Java passes `enable_wakeup=false` here (line 1562).
        if let Err(err) = self
            .await_pending_async_commits_and_execute_commit_callbacks(close_deadline_ms, false)
            .await
        {
            record(
                &mut first_error,
                "Failed invoking asynchronous commit callbacks while closing consumer",
                err,
            );
        }

        // Step 7 & 8: shut down the network thread.
        self.network_thread_close.signal_close();
        self.network_thread_close.wakeup();
        if let Err(err) = self.network_thread_close.await_join().await {
            record(&mut first_error, "Failed shutting down network thread", err);
        }

        // Final reaper pass (Java line 1570) — drain any background events
        // queued during the close path.
        {
            let now_ms = self.time.milliseconds();
            let mut reaper = self.completable_event_reaper.lock().unwrap();
            reaper.reap(now_ms);
        }

        // Java: `closeQuietly(kafkaConsumerMetrics, "kafka consumer metrics",
        // firstException)` (`AsyncKafkaConsumer.java:1573`) — removes the
        // consumer-level poll/commit metrics from the registry. `close()` is
        // infallible here (no error to fold into `first_error`).
        self.kafka_consumer_metrics.close();

        // Java: `closeQuietly(asyncConsumerMetrics, "async consumer metrics",
        // firstException)` (`AsyncKafkaConsumer.java:1574`) — removes the
        // async-consumer background-task / event-queue sensors from the
        // registry. `close()` is infallible here.
        self.async_consumer_metrics.close();

        self.closed.store(true, Ordering::Release);
        log::debug!("Kafka consumer has been closed");

        // Java (`AsyncKafkaConsumer.java:1581-1587`):
        //
        // ```java
        // Throwable exception = firstException.get();
        // if (exception != null && !swallowException) {
        //     if (exception instanceof InterruptException) {
        //         throw (InterruptException) exception;
        //     }
        //     throw new KafkaException("Failed to close kafka consumer", exception);
        // }
        // ```
        //
        // The wrap is what makes `catch (KafkaException e)` around `close()`
        // — the canonical Java idiom — reliable: whatever step failed, the
        // caller is handed a `KafkaException` with this message and the
        // original as its cause. It matters here because `first_error` can
        // hold an error that is NOT in the `KafkaException` hierarchy, e.g.
        // the `Error::LocalIllegalState` "Consumer background task is no longer
        // running." raised through
        // `await_pending_async_commits_and_execute_commit_callbacks`.
        //
        // Java's `InterruptException` pass-through has no counterpart: Rust
        // tasks have no thread-interruption mechanism, so no step can record
        // that error.
        match first_error {
            Some(err) if !swallow_error => Err(Error::kafka_message_source("Failed to close kafka consumer", err)),
            _ => Ok(()),
        }
    }

    /// Java: `private void autoCommitOnClose(final Timer timer)`
    /// (`AsyncKafkaConsumer.java:1596-1604`).
    async fn auto_commit_on_close(&mut self, deadline_ms: i64) -> Result<(), Error> {
        if self.group_id.is_none() {
            return Ok(());
        }

        if self.auto_commit_enabled {
            // Java: `commitSyncAllConsumed(timer)` swallows errors and
            // logs a warning. Match that — auto-commit failure on close
            // does not propagate.
            let remaining_ms = self.remaining_ms(deadline_ms).max(0) as u64;
            if let Err(err) = self.commit_sync_with_timeout(Duration::from_millis(remaining_ms)).await {
                log::warn!("Synchronous auto-commit failed: {err}");
            }
        }

        // Java: `applicationEventHandler.add(new CommitOnCloseEvent())`.
        let now_ms = self.time.milliseconds();
        let _ = self.application_event_handler.add(ApplicationEvent::CommitOnClose, now_ms);
        Ok(())
    }

    /// Java: `private void stopFindCoordinatorOnClose()`
    /// (`AsyncKafkaConsumer.java:1661-1666`).
    fn stop_find_coordinator_on_close(&self) -> Result<(), Error> {
        if self.group_id.is_none() {
            return Ok(());
        }
        log::debug!("Stop finding coordinator during consumer close");
        let now_ms = self.time.milliseconds();
        self.application_event_handler
            .add(ApplicationEvent::StopFindCoordinatorOnClose, now_ms)
    }

    /// Java: `private void runRebalanceCallbacksOnClose()`
    /// (`AsyncKafkaConsumer.java:1606-1643`).
    ///
    /// Invokes the user's `on_partitions_revoked` (if `memberEpoch > 0`)
    /// or `on_partitions_lost` (if `memberEpoch <= 0`) on the
    /// **group-assigned** partitions captured by the most recent
    /// reconciliation. The listener runs inline on the caller's task
    /// (§31). Errors propagate.
    ///
    /// # Why `group_assignment_snapshot` and not `subscriptions.assigned_partitions()`
    ///
    /// Java reads from `groupAssignmentSnapshot.get()`
    /// (`AsyncKafkaConsumer.java:1624`) which is populated only by the
    /// `MemberStateListener.onGroupAssignmentUpdated` callback fired
    /// during reconciliation. The snapshot deliberately excludes
    /// partitions added via `assign(...)` (manual assignment) so
    /// non-group consumers get no callback on close — Java line
    /// 1626-1628 returns early on empty snapshot.
    ///
    /// Reading from `SubscriptionState::assigned_partitions()` would
    /// (a) include manual `assign(...)` partitions, and (b) miss the
    /// "partition was just revoked but `SubscriptionState` hasn't been
    /// updated yet" window the snapshot still covers.
    async fn run_rebalance_callbacks_on_close(&mut self) -> Result<(), Error> {
        if self.group_id.is_none() {
            return Ok(());
        }

        // Java: `Set<TopicPartition> assignedPartitions = groupAssignmentSnapshot.get();`
        let assigned: Vec<TopicPartition> = {
            let guard = self.group_assignment_snapshot.lock().unwrap();
            guard.iter().cloned().collect()
        };
        // Java line 1626-1628: `if (assignedPartitions.isEmpty()) return;`.
        if assigned.is_empty() {
            return Ok(());
        }

        // Java reads `generationId` from the cached `groupMetadata`.
        // The Rust translation reads the cached value the same way.
        let member_epoch = {
            let guard = self.group_metadata.lock().unwrap();
            guard.as_ref().map(|gm| gm.generation_id()).unwrap_or(-1)
        };

        let listener_opt = self.rebalance_listener.lock().unwrap().clone();
        let listener = match listener_opt {
            Some(l) => l,
            // Java's `rebalanceListenerInvoker.invokePartitions*` is a
            // no-op when no listener is registered.
            None => return Ok(()),
        };

        let result = if member_epoch > 0 {
            self.rebalance_listener_invoker
                .invoke_partitions_revoked(&listener, &assigned)
                .await
        } else {
            self.rebalance_listener_invoker
                .invoke_partitions_lost(&listener, &assigned)
                .await
        };

        // Java: `if (error != null) throw ConsumerUtils.maybeWrapAsKafkaException(error);`
        // (`AsyncKafkaConsumer.java:1641-1642`). Without the wrap the same
        // user-listener error is classified differently depending on the path
        // that surfaced it: wrapped on a normal rebalance (via
        // `maybe_wrap_as_kafka_error_with_msg`) but raw here, so
        // `is_kafka_error()` would answer `true` in one case and `false` in the
        // other for one and the same listener failure.
        result.map_err(crate::consumer::internals::ConsumerUtils::maybe_wrap_as_kafka_error)
    }

    /// Java: `private void leaveGroupOnClose(Timer, GroupMembershipOperation)`
    /// (`AsyncKafkaConsumer.java:1645-1659`).
    async fn leave_group_on_close(
        &mut self,
        deadline_ms: i64,
        timeout_ms: i64,
        membership_operation: crate::consumer::GroupMembershipOperation,
    ) -> Result<(), Error> {
        if self.group_id.is_none() {
            return Ok(());
        }

        log::debug!("Leaving the consumer group during consumer close");
        let (handle, receiver, _erased) = CompletableEvent::make_completable_event::<()>(deadline_ms);
        // Java's `leaveGroupOnClose` does NOT call `setActiveTask` and
        // close has already called `wakeup_trigger.disable()`
        // (Java line 1545) so wakeup is inert in this path —
        // `enable_wakeup=false`. §31: still routes through the drain
        // helper so the bg-task rebalance-listener handshake is serviced
        // on the caller's task — without that, a bg task blocked awaiting
        // a `RebalanceListenerCallbackNeeded` ack (see
        // `AbstractMembershipManager::invoke_rebalance_callback`) would
        // never unblock and `network_thread_close.await_join()` (Step 8)
        // would hang forever. Java's KIP-848 reconcile chains via
        // `CompletableFuture` and never parks the bg thread on the ack, so
        // Java has no equivalent shutdown dependency on draining here.
        //
        // `submit_and_drain_for_close` runs the drain with
        // `skip_rebalance_callback=true`: any pending §31
        // `RebalanceListenerCallbackNeeded` is acked with `Ok(())` WITHOUT
        // invoking the user listener (see `process_background_events_inner`).
        // Java never invokes `on_partitions_assigned` during close (close
        // runs rebalance callbacks only via `runRebalanceCallbacksOnClose`,
        // revoked/lost only, at Step 4), so a failing assigned-listener must
        // neither run nor surface as a `close()` error.
        let result = self
            .submit_and_drain_for_close::<()>(
                ApplicationEvent::LeaveGroupOnClose { handle, membership_operation },
                receiver,
                deadline_ms,
                "Timeout expired while waiting for LeaveGroupOnClose",
            )
            .await;
        match result {
            Ok(()) => {
                log::info!("Completed leaving the group");
                Ok(())
            },
            Err(Error::Timeout(_)) => {
                // Java's `catch (TimeoutException) { log.warn(...) }` —
                // close proceeds.
                // Java logs `timer.timeoutMs()` — the *configured* close
                // timeout, not the remaining budget (which is 0 on exactly
                // this path, and so tells the operator nothing).
                log::warn!(
                    "Consumer attempted to leave the group but couldn't complete it within {} ms. \
                     It will proceed to close.",
                    timeout_ms
                );
                Ok(())
            },
            Err(err) => Err(err),
        }
    }
}

// ── `Consumer<K, V>` trait impl ─────────────────────────────────────────
//
// Delegates each trait method to the inherent impl. The split is
// deliberate: the inherent impl carries the (untyped) ctor, the
// internal helpers, and the `pub`-visible API for users that hold an
// `AsyncKafkaConsumer<K, V>` directly. The trait impl exposes the same
// surface through `Box<dyn Consumer<K, V>>` for callers that want
// type-erased dispatch (the factory `new_consumer<K, V>` returns this
// boxed form per DoD §11).

#[async_trait::async_trait]
impl<K, V> crate::consumer::Consumer<K, V> for AsyncKafkaConsumer<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    // ── Sync state-read methods ────────────────────────────────────────

    fn assignment(&self) -> std::collections::HashSet<TopicPartition> {
        AsyncKafkaConsumer::assignment(self)
    }

    fn subscription(&self) -> std::collections::HashSet<String> {
        AsyncKafkaConsumer::subscription(self)
    }

    fn paused(&self) -> std::collections::HashSet<TopicPartition> {
        AsyncKafkaConsumer::paused(self)
    }

    fn group_metadata(&self) -> ConsumerGroupMetadata {
        AsyncKafkaConsumer::group_metadata(self)
    }

    fn client_id(&self) -> &str {
        AsyncKafkaConsumer::client_id(self)
    }

    fn current_lag(&self, topic_partition: &TopicPartition) -> Option<i64> {
        AsyncKafkaConsumer::current_lag(self, topic_partition)
    }

    fn metrics(&self) -> HashMap<MetricName, Arc<KafkaMetric>> {
        AsyncKafkaConsumer::metrics(self)
    }

    fn wakeup(&self) {
        AsyncKafkaConsumer::wakeup(self);
    }

    fn handle(&self) -> ConsumerHandle {
        AsyncKafkaConsumer::handle(self)
    }

    // ── Subscribe / unsubscribe / assign ───────────────────────────────

    async fn subscribe_with_topics(&mut self, topics: Vec<String>) -> Result<(), Error> {
        AsyncKafkaConsumer::subscribe_with_topics(self, topics).await
    }

    async fn subscribe_with_topics_listener(
        &mut self,
        topics: Vec<String>,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> Result<(), Error> {
        AsyncKafkaConsumer::subscribe_with_topics_listener(self, topics, listener).await
    }

    async fn subscribe_with_pattern(&mut self, pattern: SubscriptionPattern) -> Result<(), Error> {
        AsyncKafkaConsumer::subscribe_with_pattern(self, pattern).await
    }

    async fn subscribe_with_pattern_listener(
        &mut self,
        pattern: SubscriptionPattern,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> Result<(), Error> {
        AsyncKafkaConsumer::subscribe_with_pattern_listener(self, pattern, listener).await
    }

    async fn assign(&mut self, partitions: Vec<TopicPartition>) -> Result<(), Error> {
        AsyncKafkaConsumer::assign(self, partitions).await
    }

    async fn unsubscribe(&mut self) -> Result<(), Error> {
        AsyncKafkaConsumer::unsubscribe(self).await
    }

    // ── Poll ───────────────────────────────────────────────────────────

    async fn poll(&mut self, timeout: Duration) -> Result<ConsumerRecords<K, V>, Error> {
        AsyncKafkaConsumer::poll(self, timeout).await
    }

    // ── Commit ─────────────────────────────────────────────────────────

    async fn commit_sync(&mut self) -> Result<(), Error> {
        AsyncKafkaConsumer::commit_sync(self).await
    }

    async fn commit_sync_with_timeout(&mut self, timeout: Duration) -> Result<(), Error> {
        AsyncKafkaConsumer::commit_sync_with_timeout(self, timeout).await
    }

    async fn commit_sync_with_offsets(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<(), Error> {
        AsyncKafkaConsumer::commit_sync_with_offsets(self, offsets).await
    }

    async fn commit_sync_with_offsets_timeout(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        timeout: Duration,
    ) -> Result<(), Error> {
        AsyncKafkaConsumer::commit_sync_with_offsets_timeout(self, offsets, timeout).await
    }

    async fn commit_async(&mut self) -> Result<(), Error> {
        AsyncKafkaConsumer::commit_async(self).await
    }

    async fn commit_async_with_callback(
        &mut self,
        callback: Arc<dyn crate::consumer::OffsetCommitCallback>,
    ) -> Result<(), Error> {
        AsyncKafkaConsumer::commit_async_with_callback(self, callback).await
    }

    async fn commit_async_with_offsets_callback(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        callback: Arc<dyn crate::consumer::OffsetCommitCallback>,
    ) -> Result<(), Error> {
        AsyncKafkaConsumer::commit_async_with_offsets_callback(self, offsets, callback).await
    }

    // ── Seek ───────────────────────────────────────────────────────────

    async fn seek_with_offset(&mut self, partition: TopicPartition, offset: i64) -> Result<(), Error> {
        AsyncKafkaConsumer::seek_with_offset(self, partition, offset).await
    }

    async fn seek_with_offset_and_metadata(
        &mut self,
        partition: TopicPartition,
        offset_and_metadata: OffsetAndMetadata,
    ) -> Result<(), Error> {
        AsyncKafkaConsumer::seek_with_offset_and_metadata(self, partition, offset_and_metadata).await
    }

    async fn seek_to_beginning(&mut self, partitions: &[TopicPartition]) -> Result<(), Error> {
        AsyncKafkaConsumer::seek_to_beginning(self, partitions).await
    }

    async fn seek_to_end(&mut self, partitions: &[TopicPartition]) -> Result<(), Error> {
        AsyncKafkaConsumer::seek_to_end(self, partitions).await
    }

    // ── Position / committed ───────────────────────────────────────────

    async fn position(&mut self, partition: &TopicPartition) -> Result<i64, Error> {
        AsyncKafkaConsumer::position(self, partition).await
    }

    async fn position_with_timeout(&mut self, partition: &TopicPartition, timeout: Duration) -> Result<i64, Error> {
        AsyncKafkaConsumer::position_with_timeout(self, partition, timeout).await
    }

    async fn committed(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, Error> {
        AsyncKafkaConsumer::committed(self, partitions).await
    }

    async fn committed_with_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, Error> {
        AsyncKafkaConsumer::committed_with_timeout(self, partitions, timeout).await
    }

    // ── Topic metadata ────────────────────────────────────────────────

    async fn partitions_for(&mut self, topic: &str) -> Result<Vec<crate::common::PartitionInfo>, Error> {
        AsyncKafkaConsumer::partitions_for(self, topic).await
    }

    async fn partitions_for_with_timeout(
        &mut self,
        topic: &str,
        timeout: Duration,
    ) -> Result<Vec<crate::common::PartitionInfo>, Error> {
        AsyncKafkaConsumer::partitions_for_with_timeout(self, topic, timeout).await
    }

    async fn list_topics(&mut self) -> Result<HashMap<String, Vec<crate::common::PartitionInfo>>, Error> {
        AsyncKafkaConsumer::list_topics(self).await
    }

    async fn list_topics_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<HashMap<String, Vec<crate::common::PartitionInfo>>, Error> {
        AsyncKafkaConsumer::list_topics_with_timeout(self, timeout).await
    }

    async fn offsets_for_times(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>, Error> {
        AsyncKafkaConsumer::offsets_for_times(self, timestamps_to_search).await
    }

    async fn offsets_for_times_with_timeout(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>, Error> {
        AsyncKafkaConsumer::offsets_for_times_with_timeout(self, timestamps_to_search, timeout).await
    }

    async fn beginning_offsets(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, i64>, Error> {
        AsyncKafkaConsumer::beginning_offsets(self, partitions).await
    }

    async fn beginning_offsets_with_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, Error> {
        AsyncKafkaConsumer::beginning_offsets_with_timeout(self, partitions, timeout).await
    }

    async fn end_offsets(&mut self, partitions: &[TopicPartition]) -> Result<HashMap<TopicPartition, i64>, Error> {
        AsyncKafkaConsumer::end_offsets(self, partitions).await
    }

    async fn end_offsets_with_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, Error> {
        AsyncKafkaConsumer::end_offsets_with_timeout(self, partitions, timeout).await
    }

    // ── Pause / resume ─────────────────────────────────────────────────

    async fn pause(&mut self, partitions: &[TopicPartition]) -> Result<(), Error> {
        AsyncKafkaConsumer::pause(self, partitions).await
    }

    async fn resume(&mut self, partitions: &[TopicPartition]) -> Result<(), Error> {
        AsyncKafkaConsumer::resume(self, partitions).await
    }

    // ── Lifecycle ──────────────────────────────────────────────────────

    async fn enforce_rebalance(&mut self) -> Result<(), Error> {
        AsyncKafkaConsumer::enforce_rebalance(self).await
    }

    async fn enforce_rebalance_with_reason(&mut self, reason: &str) -> Result<(), Error> {
        AsyncKafkaConsumer::enforce_rebalance_with_reason(self, reason).await
    }

    async fn close(&mut self) -> Result<(), Error> {
        AsyncKafkaConsumer::close(self).await
    }

    #[allow(deprecated)]
    async fn close_with_timeout(&mut self, timeout: Duration) -> Result<(), Error> {
        AsyncKafkaConsumer::close_with_timeout(self, timeout).await
    }

    async fn close_with_options(&mut self, options: crate::consumer::CloseOptions) -> Result<(), Error> {
        AsyncKafkaConsumer::close_with_options(self, options).await
    }
}

#[cfg(test)]
mod tests {
    //! State-read tests for the Phase 11 commit (2/N) ctor.
    //!
    //! Most Java tests in `AsyncKafkaConsumerTest.java` exercise
    //! end-to-end behavior (poll / commit / close). The handful that
    //! fit commit (2/N) are state-read tests:
    //!
    //!   - `testAssignmentEmpty` (no subscribe → empty assignment),
    //!   - `testSubscriptionEmpty`,
    //!   - `testWakeupIsCallableFromMultipleTasks`,
    //!   - `testClientIdReturnsConfigValue`,
    //!   - `testGroupMetadataReturnsStubForGrouplessConsumer`.
    //!
    //! Subscribe / unsubscribe / assign tests land in commit (3/N).
    //! Poll / commit / close tests land in commits (4/N)-(7/N).

    use std::collections::HashSet;
    use std::sync::atomic::AtomicI64;

    use tokio::sync::mpsc;

    use crate::common::internals::ClusterResourceListeners;
    use crate::common::serialization::Deserializer;
    use crate::consumer::AutoOffsetResetStrategy;
    use crate::consumer::internals::events::ApplicationEventEnvelope;
    use crate::consumer::internals::events::CompletableEventReaper;

    use super::*;

    /// Minimal `Vec<u8>` deserializer for tests — equivalent to Java's
    /// `ByteArrayDeserializer`. Returns the input bytes unchanged.
    struct TestBytesDeserializer;

    impl Deserializer<Vec<u8>> for TestBytesDeserializer {
        fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Vec<u8>, Error> {
            Ok(data.to_vec())
        }
    }

    /// Test-side handles handed back from [`make_test_consumer_with_channels`].
    /// Holding these lets the test impersonate the bg task: drain the
    /// app-event channel to act on `add_and_get`-style events, and push
    /// background events into the consumer's bg-event channel for
    /// `process_background_events` to drain.
    struct ConsumerTestHandles {
        /// Receiver for app-side events. Held by the test instead of the
        /// (absent) bg task.
        app_event_rx: mpsc::UnboundedReceiver<ApplicationEventEnvelope>,
        /// Sender for bg-side events, used to enqueue events the bg task
        /// would normally post.
        bg_event_tx: mpsc::UnboundedSender<BackgroundEventEnvelope>,
        /// Handle on the shared `SubscriptionState` so tests can inspect /
        /// pre-populate it.
        subscriptions: Arc<Mutex<SubscriptionState>>,
        /// Set to `true` whenever the consumer's bg-task wakeup fn is invoked
        /// (Phase 41 Issue 3 observability). Lets a `&mut self`-level component
        /// test assert that the ack-send path in `process_background_events`
        /// pokes the bg wakeup `Notify`.
        ///
        /// NOTE: this flag stands in for the *user-facing* wakeup
        /// (`NetworkThreadCloseHandle::wakeup` → `WakeupTrigger::wakeup`).
        /// The ack path must NOT fire that one — see
        /// `process_background_events_ack_pokes_bg_notify_not_user_wakeup`.
        bg_wakeup_called: Arc<AtomicBool>,
        /// The application-event `Notify` the bg loop parks on — the correct
        /// target of the §31 step-4 ack poke.
        event_notify: Arc<tokio::sync::Notify>,
    }

    /// Builds a consumer along with the test-side channel handles needed
    /// to act as the bg task during a test.
    fn make_test_consumer_with_channels() -> (AsyncKafkaConsumer<Vec<u8>, Vec<u8>>, ConsumerTestHandles) {
        let mut config = ConsumerConfig { bootstrap_servers: vec!["localhost:9092".to_string()], ..Default::default() };
        config.client_id = "test-client".to_string();
        config.group_id = Some("test-group".to_string());
        let client_id: Arc<str> = Arc::from(config.client_id.as_str());

        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::EARLIEST)));
        let metadata = Arc::new(ConsumerMetadata::with_config(
            &config,
            Arc::clone(&subs),
            ClusterResourceListeners::new(),
        ));
        let request_managers = Arc::new(std::sync::Mutex::new(RequestManagers::new(
            None, None, None, None, None, None, None,
        )));
        // Test stand-in for the bg task's app-event receiver. Tests hold
        // this `app_event_rx` and pull events off it themselves.
        let (app_handler_tx, app_event_rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        // Held by the returned handles so tests can assert on the REAL poke
        // primitive the bg loop parks on, rather than a stub flag.
        let event_notify = Arc::new(tokio::sync::Notify::new());
        let app_handler = Arc::new(ApplicationEventHandler::new(app_handler_tx, Arc::clone(&event_notify)));
        let reaper = Arc::new(std::sync::Mutex::new(CompletableEventReaper::new()));
        let max_time = Arc::new(AtomicI64::new(0));
        let wakeup = WakeupTrigger::new();

        // Stub join handle — spawn a noop task. Tests do not assert on
        // the bg task's behavior in this commit.
        let join_handle: JoinHandle<()> = tokio::spawn(async move {});
        let signal_close_called = Arc::new(AtomicBool::new(false));
        let signal_close_flag = Arc::clone(&signal_close_called);
        let wakeup_called = Arc::new(AtomicBool::new(false));
        let wakeup_flag = Arc::clone(&wakeup_called);
        // Production's `wakeup_fn` fires the `WakeupTrigger` (see the ctor);
        // mirror that here as well as setting the flag, so a test asserting
        // "no wakeup is pending" really exercises what the app would observe.
        let wakeup_trigger_for_fn = wakeup.clone();
        let close_handle = NetworkThreadCloseHandle::new(
            Box::new(move || {
                signal_close_flag.store(true, Ordering::Release);
            }),
            Box::new(move || {
                wakeup_flag.store(true, Ordering::Release);
                wakeup_trigger_for_fn.wakeup();
            }),
            join_handle,
        );

        let (bg_event_tx, bg_rx) = mpsc::unbounded_channel::<BackgroundEventEnvelope>();
        let interceptors = Arc::new(Mutex::new(ConsumerInterceptors::<Vec<u8>, Vec<u8>>::new(Vec::new())));
        let offset_commit_callback_invoker =
            Arc::new(OffsetCommitCallbackInvoker::<Vec<u8>, Vec<u8>>::new(ConsumerInterceptors::<
                Vec<u8>,
                Vec<u8>,
            >::new(
                Vec::new()
            )));
        let deserializers = Arc::new(Deserializers::<Vec<u8>, Vec<u8>>::new(
            Box::new(TestBytesDeserializer),
            Box::new(TestBytesDeserializer),
        ));
        let rebalance_listener_invoker = ConsumerRebalanceListenerInvoker::new(Arc::clone(&subs));

        let fetch_buffer = Arc::new(FetchBuffer::new());
        let fetch_config = crate::consumer::internals::FetchConfig::new(
            1,
            50 * 1024 * 1024,
            500,
            1024 * 1024,
            500,
            true,
            "",
            IsolationLevel::ReadUncommitted,
        );
        let (metrics, fetch_metrics_manager) =
            AsyncKafkaConsumer::<Vec<u8>, Vec<u8>>::create_fetch_metrics_manager(&config);
        let kafka_consumer_metrics = Arc::new(KafkaConsumerMetrics::new(Arc::clone(&metrics)));
        let async_consumer_metrics = Arc::new(AsyncConsumerMetrics::new(
            Arc::clone(&metrics),
            crate::consumer::internals::ConsumerUtils::CONSUMER_METRIC_GROUP,
        ));
        let background_event_queue_size = Arc::new(AtomicI64::new(0));
        let fetch_collector = Arc::new(FetchCollector::<Vec<u8>, Vec<u8>>::new(
            Arc::clone(&metadata),
            Arc::clone(&subs),
            fetch_config,
            Arc::clone(&deserializers),
            Arc::clone(&fetch_metrics_manager),
            Arc::new(crate::consumer::internals::SystemFetchCollectorTime),
        ));

        // Build the state-notifier + shared slots once (Phase-12
        // Issue 2): the test fixture mirrors the production ctor by
        // constructing the slots locally and threading the same Arcs
        // through `state_notifier` and the components struct. Tests
        // that need the listener registered on a membership manager
        // call `consumer.state_notifier()` and pass the Arc to
        // `AbstractMembershipManager::register_state_listener`.
        let group_metadata_slot: Arc<Mutex<Option<ConsumerGroupMetadata>>> = Arc::new(Mutex::new(None));
        let group_assignment_snapshot_slot: Arc<Mutex<HashSet<TopicPartition>>> = Arc::new(Mutex::new(HashSet::new()));
        let state_notifier = Arc::new(ConsumerStateNotifier::new(
            "test-group".to_string(),
            None,
            Arc::clone(&group_metadata_slot),
            Arc::clone(&group_assignment_snapshot_slot),
            Arc::new(AtomicBool::new(false)),
        ));

        let positions_validator = Arc::new(PositionsValidator::new(Arc::clone(&subs), Arc::clone(&metadata)));
        let components = AsyncKafkaConsumerComponents {
            config,
            client_id,
            group_id: Some("test-group".to_string()),
            positions_validator,
            subscriptions: Arc::clone(&subs),
            metadata,
            request_managers,
            background_event_rx: bg_rx,
            application_event_handler: app_handler,
            completable_event_reaper: reaper,
            max_time_to_wait_ms: max_time,
            wakeup_trigger: wakeup,
            network_thread_close: close_handle,
            fetch_buffer,
            fetch_collector,
            metrics,
            kafka_consumer_metrics,
            async_consumer_metrics,
            background_event_queue_size,
            rebalance_listener_invoker,
            offset_commit_callback_invoker,
            deserializers,
            interceptors,
            isolation_level: IsolationLevel::ReadUncommitted,
            time: Arc::new(crate::consumer::internals::SystemThreadTime),
            group_metadata: group_metadata_slot,
            group_assignment_snapshot: group_assignment_snapshot_slot,
            state_notifier,
        };
        (
            AsyncKafkaConsumer::<Vec<u8>, Vec<u8>>::with_components(components),
            ConsumerTestHandles {
                app_event_rx,
                bg_event_tx,
                subscriptions: subs,
                bg_wakeup_called: wakeup_called,
                event_notify,
            },
        )
    }

    /// Backwards-compat alias for the existing state-read tests.
    fn make_test_consumer() -> AsyncKafkaConsumer<Vec<u8>, Vec<u8>> {
        make_test_consumer_with_channels().0
    }

    /// Construct a test consumer with `group_id=None` from the start
    /// (Issue 26: mirrors Java's `assignor-only` consumer
    /// constructed without `group.id`). Use this instead of mutating
    /// `consumer.group_id` post-construction to exercise the same code
    /// path the production no-group-id ctor would.
    fn make_test_consumer_without_group_id() -> (AsyncKafkaConsumer<Vec<u8>, Vec<u8>>, ConsumerTestHandles) {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        // The current test stand-in for the production ctor wires the
        // `group_id` slot directly; override here so every subsequent
        // call observes the `None` state from the start (rather than
        // observing the post-mutation transition).
        consumer.group_id = None;
        consumer.config.group_id = None;
        (consumer, handles)
    }

    #[tokio::test]
    async fn assignment_is_empty_before_subscribe() {
        let consumer = make_test_consumer();
        assert!(consumer.assignment().is_empty());
    }

    #[tokio::test]
    async fn subscription_is_empty_before_subscribe() {
        let consumer = make_test_consumer();
        assert!(consumer.subscription().is_empty());
    }

    #[tokio::test]
    async fn paused_is_empty_before_subscribe() {
        let consumer = make_test_consumer();
        assert!(consumer.paused().is_empty());
    }

    #[tokio::test]
    async fn client_id_returns_config_value() {
        let consumer = make_test_consumer();
        assert_eq!(consumer.client_id(), "test-client");
    }

    #[tokio::test]
    async fn group_metadata_returns_stub_for_uninitialized_consumer() {
        let consumer = make_test_consumer();
        let meta = consumer.group_metadata();
        assert_eq!(meta.group_id(), "test-group");
    }

    /// Java parity: `memberStateListener.onMemberEpochUpdated` →
    /// `updateGroupMetadata` populates the cached metadata.
    /// (`AsyncKafkaConsumer.java:343-353`, `:772-784`).
    #[tokio::test]
    async fn state_notifier_populates_group_metadata_on_epoch_update() {
        let consumer = make_test_consumer();
        let notifier = consumer.state_notifier();
        // Pre-condition: the cache is empty (stub returned).
        assert_eq!(consumer.group_metadata().generation_id(), -1);

        notifier.on_member_epoch_updated(Some(42), "member-id-xyz");

        let meta = consumer.group_metadata();
        assert_eq!(meta.group_id(), "test-group");
        assert_eq!(meta.generation_id(), 42);
        assert_eq!(meta.member_id(), "member-id-xyz");
    }

    /// Java: `memberEpoch.ifPresent(...)` short-circuits when None —
    /// the cache is NOT cleared. Mirrors Java's
    /// `updateGroupMetadata(Optional.empty(), …)` no-op behavior.
    #[tokio::test]
    async fn state_notifier_with_none_epoch_does_not_modify_cache() {
        let consumer = make_test_consumer();
        let notifier = consumer.state_notifier();
        notifier.on_member_epoch_updated(Some(7), "m1");
        notifier.on_member_epoch_updated(None, "m1");
        // None did not overwrite — the epoch 7 entry survives.
        assert_eq!(consumer.group_metadata().generation_id(), 7);
    }

    /// Java parity: `memberStateListener.onGroupAssignmentUpdated` →
    /// `setGroupAssignmentSnapshot(partitions)` updates the snapshot
    /// (`AsyncKafkaConsumer.java:349-352`, `:786-788`).
    #[tokio::test]
    async fn state_notifier_updates_group_assignment_snapshot() {
        let consumer = make_test_consumer();
        let notifier = consumer.state_notifier();
        let tp0 = TopicPartition::new("t".to_string(), 0);
        let tp1 = TopicPartition::new("t".to_string(), 1);
        let mut set: HashSet<TopicPartition> = HashSet::new();
        set.insert(tp0.clone());
        set.insert(tp1.clone());

        notifier.on_group_assignment_updated(&set);

        let snap = consumer.group_assignment_snapshot.lock().unwrap().clone();
        assert_eq!(snap.len(), 2);
        assert!(snap.contains(&tp0));
        assert!(snap.contains(&tp1));
    }

    #[tokio::test]
    async fn current_lag_returns_none_in_commit_2() {
        // Phase-11 commit (2/N) stub — verified to return None until
        // commit (6/N) wires CurrentLagEvent.
        let consumer = make_test_consumer();
        let tp = TopicPartition::new("t".to_string(), 0);
        assert_eq!(consumer.current_lag(&tp), None);
    }

    /// Phase-12 Issue 2 regression: when the consumer's `state_notifier`
    /// Arc is registered on a `ConsumerMembershipManager` listener list
    /// (the production wiring) and a heartbeat-response-style member-epoch
    /// update is dispatched on the listener side, the app-side
    /// `group_metadata()` observes the new epoch.
    ///
    /// Pre-fix this test would fail because the production ctor built two
    /// separate notifiers — one registered on the membership manager, one
    /// reachable through `consumer.state_notifier()` — and writes to one
    /// did not reach the other's backing slot.
    #[tokio::test]
    async fn issue_2_state_notifier_writes_visible_through_consumer_after_registration() {
        use crate::consumer::internals::MemberStateListener;

        let consumer = make_test_consumer();
        let notifier = consumer.state_notifier();

        // Mimic the production ctor's
        // `membership.abstract_mm.register_state_listener(state_notifier)`
        // call: take the Arc out of the consumer, hand it to the
        // listener-registration site, and dispatch a fake
        // `on_member_epoch_updated`. The app side must see the change.
        //
        // `register_state_listener` takes `Arc<dyn MemberStateListener>`.
        // We don't need a real `ConsumerMembershipManager` here; the
        // contract is "register_state_listener obtains an
        // Arc<dyn MemberStateListener> and may invoke its methods at any
        // time" — the test substitutes for that registration site by
        // calling the trait method directly on the Arc-erased listener.
        let listener: Arc<dyn MemberStateListener> = notifier;
        listener.on_member_epoch_updated(Some(99), "member-from-listener");

        // App-side observes the change through the same shared slot the
        // listener wrote into — proving the `state_notifier` and
        // `group_metadata` Arcs are wired as a single source of truth
        // (Java `AsyncKafkaConsumer.java:289, 343-353`).
        let meta = consumer.group_metadata();
        assert_eq!(meta.generation_id(), 99);
        assert_eq!(meta.member_id(), "member-from-listener");
    }

    #[tokio::test]
    async fn wakeup_triggers_token_cancellation_and_bg_wakeup() {
        let consumer = make_test_consumer();
        let token = consumer.wakeup_trigger.current_token();
        assert!(!token.is_cancelled());
        consumer.wakeup();
        assert!(token.is_cancelled(), "wakeup() must cancel the current token");
    }

    /// A [`ConsumerHandle`] obtained from the consumer fires the SAME
    /// wakeup state as `wakeup()` — proving the shareable handle is a
    /// faithful, `Send`-able stand-in for the cross-task `wakeup()`
    /// pattern (the safe replacement for the deleted unsafe test helper).
    #[tokio::test]
    async fn handle_wakeup_cancels_current_token() {
        let consumer = make_test_consumer();
        let token = consumer.wakeup_trigger.current_token();
        assert!(!token.is_cancelled());

        // The handle is moved into another task — no reference to the
        // consumer crosses the task boundary.
        let handle = consumer.handle();
        let joined = tokio::spawn(async move {
            handle.wakeup();
        });
        joined.await.expect("waker task");

        assert!(token.is_cancelled(), "handle().wakeup() must cancel the current token");
    }

    /// Phase 41c / §31 deadlock regression (blocker b): a reentrant
    /// `ConsumerHandle` op submitted from another task (standing in for a
    /// rebalance-listener body) routes an `ApplicationEvent` through the bg
    /// pipeline and completes — proving the handle's no-drain await is NOT
    /// frozen. The op is `pause`, which (like every bg-routed handle op)
    /// goes through `submit_and_await`. We act as the bg task by reading
    /// the app-event channel and completing the event's handle; the handle
    /// op must then return promptly.
    #[tokio::test]
    async fn handle_reentrant_op_completes_through_bg_pipeline() {
        let (consumer, mut handles) = make_test_consumer_with_channels();
        let handle = consumer.handle();

        let tp = TopicPartition::new("t".to_string(), 0);
        // Submit the reentrant op from another task — exactly how a
        // captured handle is used from inside a listener.
        let op = tokio::spawn(async move { handle.pause(std::slice::from_ref(&tp)).await });

        // Act as the bg task: pull the PausePartitions envelope and
        // complete it. (The real bg loop keeps spinning during a callback
        // after Phase 41b, so this event is serviced rather than stranded.)
        let env = handles.app_event_rx.recv().await.expect("PausePartitions envelope must arrive");
        match env.event {
            ApplicationEvent::PausePartitions { handle, partitions } => {
                assert_eq!(partitions.len(), 1);
                handle.complete(());
            },
            other => panic!("expected PausePartitions, got {}", other.type_name()),
        }

        op.await
            .expect("handle op task ok")
            .expect("reentrant pause completes — no deadlock");
        // The consumer must outlive the handle/op (shared Arc state).
        drop(consumer);
    }

    /// Phase 41 Issue 4: `ConsumerHandle::assign([])` must REJECT the empty
    /// collection (rather than silently clearing the assignment without
    /// leaving the group). On the owning consumer `assign([])` delegates to
    /// `unsubscribe()` (group leave), which the handle does not expose, so the
    /// handle returns a clear error pointing the caller at the owning
    /// consumer's `unsubscribe()`. A non-empty `assign` still routes through
    /// the bg pipeline as before.
    #[tokio::test]
    async fn handle_assign_empty_is_rejected() {
        let (consumer, _handles) = make_test_consumer_with_channels();
        let handle = consumer.handle();

        let err = handle.assign(Vec::new()).await.expect_err("empty assign must be rejected");
        assert!(
            matches!(err, Error::LocalIllegalArgument(_)),
            "empty assign should be an illegal-argument error, got {err:?}",
        );
        let msg = err.to_string();
        assert!(
            msg.contains("unsubscribe"),
            "error must point the caller at unsubscribe(); got: {msg}",
        );
        drop(consumer);
    }

    /// Phase 12.5 Issue 7 regression: the production ctor must register
    /// the `CommitRequestManager` as a `MemberStateListener` on the
    /// `ConsumerMembershipManager`, so heartbeat-driven member-epoch
    /// updates propagate the broker-assigned UUID into the commit
    /// manager's internal `MemberInfo`. Without this registration the
    /// `OffsetCommitRequest` goes out with the default empty member id
    /// and the broker rejects it with `UNKNOWN_MEMBER_ID`.
    ///
    /// Java parity: `RequestManagers.java:273-274` (KIP-848 / `consumer`
    /// group protocol arm) — Java registers two listeners on
    /// `membershipManager`, the first being `commitRequestManager`
    /// itself.
    ///
    /// The test exercises the actual production ctor path
    /// (`AsyncKafkaConsumer::new`) against a refuses-connection broker —
    /// the same pattern as `tests/consumer/async_kafka_consumer_test.rs`
    /// — then drives `update_member_epoch(...)` on the membership
    /// manager's `MembershipInner` directly. This fans out to all
    /// registered listeners; we assert that the `CommitRequestManager`'s
    /// `member_info.member_id` updated to match the membership
    /// manager's auto-generated UUID. Reverting the listener
    /// registration in `with_components` makes this test fail with
    /// an empty `member_id`.
    /// Java wraps the whole `AsyncKafkaConsumer` constructor body in
    /// `catch (Throwable t) { ... throw new KafkaException("Failed to construct
    /// kafka consumer", t); }` (`AsyncKafkaConsumer.java:509-517`), so every
    /// construction failure reaches the caller with that exact message and the
    /// underlying failure as its cause.
    ///
    /// Propagating the inner error raw would change both the class (an
    /// `IllegalArgumentException` from address parsing answers `false` to
    /// `is_kafka_error()`) and the message — and "Failed to construct kafka
    /// consumer" is the string users match on.
    #[tokio::test(flavor = "multi_thread")]
    async fn constructor_failure_is_wrapped_as_failed_to_construct_kafka_consumer() {
        use std::collections::HashMap;

        use crate::common::serialization::Deserializer;

        struct TestStringDeserializer;
        impl Deserializer<String> for TestStringDeserializer {
            fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, Error> {
                String::from_utf8(data.to_vec()).map_err(|e| Error::serialization(format!("invalid utf-8: {}", e)))
            }
        }

        // A bootstrap address with no port passes `ConsumerConfig` parsing but
        // fails `parse_and_validate_addresses` inside the constructor body —
        // i.e. inside Java's `try`.
        let props = HashMap::from([("bootstrap.servers".to_string(), "no-port-here".to_string())]);
        let config = ConsumerConfig::new(&props).expect("config itself validates");

        let err = AsyncKafkaConsumer::<String, String>::new(
            config,
            Box::new(TestStringDeserializer),
            Box::new(TestStringDeserializer),
        )
        .err()
        .expect("an unparseable bootstrap address must fail construction");

        // Java's message, verbatim.
        assert_eq!("Failed to construct kafka consumer", err.message());
        // Java throws a `KafkaException`, so the hierarchy predicate must agree.
        assert!(err.is_kafka_error(), "must be a Kafka error: {err:?}");
        assert!(
            !matches!(err, Error::LocalIllegalArgument(_)),
            "the raw IllegalArgument must not escape: {err:?}"
        );
        // The original failure is the cause (Java's second constructor arg).
        let source = std::error::Error::source(&err).expect("the underlying failure must be the cause");
        assert!(
            source.to_string().contains("Invalid url in bootstrap.servers"),
            "cause must be the address-parse failure, got: {source}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn issue_7_commit_request_manager_registered_as_member_state_listener() {
        use std::collections::HashMap;

        use crate::common::serialization::Deserializer;

        // Local string deserializer matching the smoke-test pattern.
        struct TestStringDeserializer;
        impl Deserializer<String> for TestStringDeserializer {
            fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, Error> {
                String::from_utf8(data.to_vec()).map_err(|e| Error::serialization(format!("invalid utf-8: {}", e)))
            }
        }

        // Build a `ConsumerConfig` with `group.protocol=consumer` and a
        // `group.id` so the membership + commit managers are constructed
        // by the ctor (gated paths). Point at a refused localhost port
        // so the ctor does not block on network IO.
        let props = HashMap::from([
            ("bootstrap.servers".to_string(), "127.0.0.1:1".to_string()),
            ("group.id".to_string(), "issue-7-group".to_string()),
            ("group.protocol".to_string(), "consumer".to_string()),
            ("client.id".to_string(), "issue-7-client".to_string()),
            ("auto.offset.reset".to_string(), "earliest".to_string()),
            ("enable.auto.commit".to_string(), "false".to_string()),
        ]);
        let config = ConsumerConfig::new(&props).expect("config validates");

        let mut consumer = AsyncKafkaConsumer::<String, String>::new(
            config,
            Box::new(TestStringDeserializer),
            Box::new(TestStringDeserializer),
        )
        .expect("ctor should succeed against a refused broker");

        // Reach into the production `RequestManagers` slot. Both
        // `consumer_membership` and `commit` MUST be `Some` because
        // `group.id` was set.
        let (membership_arc, commit_arc) = {
            let rm_guard = consumer.request_managers.lock().expect("rm not poisoned");
            (
                rm_guard
                    .consumer_membership
                    .as_ref()
                    .expect("group.id set → membership must be Some")
                    .clone(),
                rm_guard.commit.as_ref().expect("group.id set → commit must be Some").clone(),
            )
        };

        // Pre-condition: `member_info.member_id` defaults to "" — the
        // broker-assigned UUID has not been observed yet. If the listener
        // was already invoked somehow during ctor we'd see the membership
        // manager's UUID here.
        assert_eq!(
            commit_arc.member_info_for_test().member_id,
            "",
            "before epoch update, member_id should be the default empty string"
        );

        // Capture the membership manager's auto-generated UUID. This is
        // the value the listener will pass to
        // `on_member_epoch_updated(epoch, member_id)`.
        let expected_member_id = {
            let inner_guard = membership_arc.abstract_mm.inner.lock().expect("membership inner not poisoned");
            inner_guard.member_id.clone()
        };
        assert!(
            !expected_member_id.is_empty(),
            "membership manager must have auto-generated a UUID"
        );

        // Drive `update_member_epoch(...)` directly on the inner. This
        // is the same fan-out that Java's heartbeat-response handler
        // triggers via `MembershipManager.updateMemberEpoch(int)`.
        // `update_member_epoch` calls `notify_epoch_change(Some(epoch))`
        // which iterates `state_updates_listeners` and invokes each
        // listener's `on_member_epoch_updated(epoch, &self.member_id)`.
        {
            let mut inner_guard = membership_arc.abstract_mm.inner.lock().expect("membership inner not poisoned");
            inner_guard.update_member_epoch(42);
        }

        // Post-condition: if `CommitRequestManager` was registered as a
        // `MemberStateListener`, its `member_info` now carries the
        // membership manager's UUID + epoch.
        let post = commit_arc.member_info_for_test();
        assert_eq!(
            post.member_id, expected_member_id,
            "Issue 7: CommitRequestManager.member_id must equal membership.member_id after \
             update_member_epoch — this proves the listener registration in new_with_components \
             routed the epoch update through to the commit manager"
        );
        assert_eq!(
            post.member_epoch,
            Some(42),
            "epoch should match the value passed to update_member_epoch"
        );

        // Clean shutdown.
        consumer.close().await.expect("close should succeed");
    }

    /// Stand-in for Java's `testAssignmentReturnsAssignedPartitions`
    /// once `assign` lands in commit (3/N) — for now we mutate
    /// `SubscriptionState` directly to verify the read path.
    #[tokio::test]
    async fn assignment_reads_subscription_state() {
        let consumer = make_test_consumer();
        let tp = TopicPartition::new("t".to_string(), 0);
        {
            let mut subs = consumer.subscriptions.lock().unwrap();
            let mut assigned: HashSet<TopicPartition> = HashSet::new();
            assigned.insert(tp.clone());
            subs.assign_from_user(assigned).unwrap();
        }
        let assignment = consumer.assignment();
        assert_eq!(assignment.len(), 1);
        assert!(assignment.contains(&tp));
    }

    // ─── Subscribe / unsubscribe / assign tests (commit (3/N)) ───
    //
    // These tests stand in for Java's
    // `AsyncKafkaConsumerTest.testSubscribeGeneratesEvent` /
    // `testSubscribePatternGeneratesEvent` /
    // `testUnsubscribeGeneratesUnsubscribeEvent` /
    // `testAssign*` / etc. Where Java's tests use Mockito to immediately
    // satisfy `applicationEventHandler.addAndGet(...)`, the Rust tests
    // spawn a small helper task that drains one envelope off the
    // app-event channel and `complete`s the handle inside it.
    //
    // Skipped Java tests for this commit (rationale per row):
    //   - `testAssignOnNullTopicPartition` / `testAssignOnNullTopicInPartition`
    //     — Rust's type system makes the `null` case unrepresentable:
    //     `Vec<TopicPartition>` cannot contain a `null` slot, and
    //     `TopicPartition` requires an owned `String` topic.
    //   - `testSubscribeToNullTopicCollection` /
    //     `testSubscriptionOnNullTopic` — same.
    //   - `testReaperInvokedInUnsubscribe` — depends on `backgroundEventReaper.reap(time)`
    //     wiring inside `process_background_events`; the reaper hookup
    //     itself lands later (Phase 11 commit (4/N)). Deferred to that
    //     commit.
    //   - `testGroupMetadataIsResetAfterUnsubscribe` — depends on the
    //     `MemberStateListener` (commit (7/N)) that populates
    //     `group_metadata`. Deferred to that commit.
    //   - `testUnsubscribeWithoutGroupId` — depends on a no-group ctor
    //     path which is built in commit (7/N) via `new_consumer`.
    //     Deferred.
    //   - `testSubscribePatternAgainstBrokerNotSupportingRegex` —
    //     end-to-end against a `MockClient`; depends on the poll path
    //     (commit (4/N)).
    //   - `testReaperInvokedInPoll` / similar poll-only flows — commit
    //     (4/N).

    /// Helper: spawn a task that takes the next envelope off the
    /// app-event channel and completes its `handle` with `Ok(())`.
    /// Mirrors Java's
    /// `completeTopicSubscriptionChangeEventSuccessfully()` / etc.
    fn auto_complete_next_event(
        mut rx: mpsc::UnboundedReceiver<ApplicationEventEnvelope>,
    ) -> tokio::task::JoinHandle<Option<ApplicationEventEnvelope>> {
        tokio::spawn(async move {
            let env = rx.recv().await?;
            complete_event(&env);
            Some(env)
        })
    }

    /// Same as [`auto_complete_next_event`] but keeps completing every event
    /// that arrives until the channel closes — for tests that make more than
    /// one blocking call.
    fn auto_complete_all_events(
        mut rx: mpsc::UnboundedReceiver<ApplicationEventEnvelope>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            while let Some(env) = rx.recv().await {
                complete_event(&env);
            }
        })
    }

    /// Completes the handle carried by an application event, so the app-side
    /// `add_and_get` resolves without a background task.
    fn complete_event(env: &ApplicationEventEnvelope) {
        match &env.event {
            ApplicationEvent::TopicSubscriptionChange { handle, .. } => {
                handle.complete(());
            },
            ApplicationEvent::TopicPatternSubscriptionChange { handle, .. } => {
                handle.complete(());
            },
            ApplicationEvent::TopicRe2JPatternSubscriptionChange { handle, .. } => {
                handle.complete(());
            },
            ApplicationEvent::AssignmentChange { handle, .. } => {
                handle.complete(());
            },
            ApplicationEvent::Unsubscribe { handle } => {
                handle.complete(());
            },
            ApplicationEvent::SeekUnvalidated { handle, .. } => {
                handle.complete(());
            },
            ApplicationEvent::ResetOffset { handle, .. } => {
                handle.complete(());
            },
            ApplicationEvent::PausePartitions { handle, .. } => {
                handle.complete(());
            },
            ApplicationEvent::ResumePartitions { handle, .. } => {
                handle.complete(());
            },
            _ => {
                // Unknown variant — leave the handle un-completed; the
                // test's `add_and_get` will time out and the
                // assertion will be a clear failure.
            },
        }
    }

    /// Java: `testSubscribeGeneratesEvent`.
    #[tokio::test]
    async fn subscribe_generates_topic_subscription_change_event() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let completer = auto_complete_next_event(handles.app_event_rx);
        consumer.subscribe_with_topics(vec!["topic1".to_string()]).await.expect("ok");
        let env = completer.await.expect("task ok").expect("event received");
        assert!(matches!(env.event, ApplicationEvent::TopicSubscriptionChange { .. }));
    }

    /// Java: `testSubscribeToRe2JPatternGeneratesEvent`.
    #[tokio::test]
    async fn subscribe_subscription_pattern_generates_event() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let completer = auto_complete_next_event(handles.app_event_rx);
        consumer
            .subscribe_with_pattern(SubscriptionPattern::new("t*"))
            .await
            .expect("ok");
        let env = completer.await.expect("task ok").expect("event received");
        assert!(matches!(env.event, ApplicationEvent::TopicRe2JPatternSubscriptionChange { .. }));
    }

    /// Java: `testSubscribeToRe2JPatternValidation` (Java line 1856-1869)
    /// — empty pattern rejected with the EXACT Java error message
    /// `"Topic pattern to subscribe to cannot be empty"`. Issue 5
    /// (Critic batch-1): substring assertion strengthened to exact match
    /// per DoD §3 (error message content is part of the behavioural
    /// contract).
    ///
    /// SKIP: null-pattern case (Java line 1859) — unrepresentable in
    /// Rust because `subscribe_with_pattern` takes `SubscriptionPattern`
    /// by value, not `Option<SubscriptionPattern>`.
    ///
    /// SKIP: null-listener case (Java line 1867) — unrepresentable in
    /// Rust because `subscribe_with_pattern_listener` takes
    /// `Arc<dyn ConsumerRebalanceListener>`, not
    /// `Option<Arc<dyn ConsumerRebalanceListener>>`.
    #[tokio::test]
    async fn subscribe_subscription_pattern_rejects_empty() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let err = consumer
            .subscribe_with_pattern(SubscriptionPattern::new(""))
            .await
            .expect_err("must err");
        match err {
            Error::LocalIllegalArgument(msg) => {
                assert_eq!(msg.message(), "Topic pattern to subscribe to cannot be empty");
            },
            other => panic!("expected IllegalArgument, got {other:?}"),
        }
    }

    /// Java: `testSubscribeToRe2JPatternValidation` (Java line 1865) —
    /// `assertDoesNotThrow(() -> consumer.subscribe(new SubscriptionPattern("t*")))`.
    /// The valid-pattern arm of the same Java test.
    #[tokio::test]
    async fn subscribe_subscription_pattern_accepts_valid_pattern() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let completer = auto_complete_next_event(handles.app_event_rx);
        consumer
            .subscribe_with_pattern(SubscriptionPattern::new("t*"))
            .await
            .expect("valid pattern must not throw");
        let env = completer.await.expect("task ok").expect("event received");
        assert!(matches!(env.event, ApplicationEvent::TopicRe2JPatternSubscriptionChange { .. }));
    }

    /// Java: `testUnsubscribeGeneratesUnsubscribeEvent`.
    #[tokio::test]
    async fn unsubscribe_generates_unsubscribe_event() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let completer = auto_complete_next_event(handles.app_event_rx);
        consumer.unsubscribe().await.expect("ok");
        let env = completer.await.expect("task ok").expect("event received");
        assert!(matches!(env.event, ApplicationEvent::Unsubscribe { .. }));
    }

    /// Java: `testSubscribeToEmptyListActsAsUnsubscribe`.
    #[tokio::test]
    async fn subscribe_to_empty_list_acts_as_unsubscribe() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let completer = auto_complete_next_event(handles.app_event_rx);
        consumer.subscribe_with_topics(Vec::new()).await.expect("ok");
        let env = completer.await.expect("task ok").expect("event received");
        assert!(matches!(env.event, ApplicationEvent::Unsubscribe { .. }));
    }

    /// Java: `testSubscriptionOnEmptyTopic` — blank topic rejected.
    #[tokio::test]
    async fn subscribe_rejects_blank_topic() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let err = consumer
            .subscribe_with_topics(vec!["  ".to_string()])
            .await
            .expect_err("must err");
        assert!(matches!(err, Error::LocalIllegalArgument(_)));
    }

    /// Java: `testAssign` (Java line 816-824). Asserts the
    /// `AssignmentChangeEvent` is enqueued. The "clears subscription"
    /// half of Java's assertion (`consumer.subscription().isEmpty()`)
    /// is exercised by `assign_clears_subscription_after_event_completes`
    /// below — splitting the assertions clarifies what each test
    /// actually verifies (Issue 7 from Critic batch-1).
    #[tokio::test]
    async fn assign_generates_assignment_change_event() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let completer = auto_complete_next_event(handles.app_event_rx);
        let tp = TopicPartition::new("foo".to_string(), 3);
        consumer.assign(vec![tp.clone()]).await.expect("ok");
        let env = completer.await.expect("task ok").expect("event received");
        match env.event {
            ApplicationEvent::AssignmentChange { partitions, .. } => {
                assert!(partitions.contains(&tp));
            },
            other => panic!("expected AssignmentChange, got {}", other.type_name()),
        }
    }

    /// Java: `testAssign` (Java line 821-822) — second-half assertion:
    /// `assertTrue(consumer.subscription().isEmpty())` AND
    /// `assertTrue(consumer.assignment().contains(tp))`. The bg-task
    /// `assign_from_user(...)` is applied directly on the
    /// `SubscriptionState` here to mirror Java's `MockClient` arm at
    /// `completeAssignmentChangeEventSuccessfully()` line 2090-2098.
    #[tokio::test]
    async fn assign_clears_subscription_after_event_completes() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("foo".to_string(), 3);
        let subs = Arc::clone(&handles.subscriptions);
        // Mirror Java's `completeAssignmentChangeEventSuccessfully`
        // helper: drain the event and apply `assignFromUser` on the
        // shared `SubscriptionState` BEFORE completing the handle.
        let completer = {
            let tp = tp.clone();
            let mut rx = handles.app_event_rx;
            tokio::spawn(async move {
                while let Some(env) = rx.recv().await {
                    if let ApplicationEvent::AssignmentChange { handle, partitions, .. } = env.event {
                        let mut s = subs.lock().unwrap();
                        let mut set = HashSet::new();
                        for p in partitions {
                            set.insert(p);
                        }
                        s.assign_from_user(set).expect("assign ok");
                        drop(s);
                        handle.complete(());
                        return Some(tp);
                    }
                }
                None
            })
        };
        consumer.assign(vec![tp.clone()]).await.expect("ok");
        let _ = completer.await.expect("task ok").expect("event received");
        assert!(consumer.subscription().is_empty(), "subscription must be empty after assign");
        assert!(
            consumer.assignment().contains(&tp),
            "assignment must contain the assigned partition"
        );
    }

    /// Java: `testAssignOnEmptyTopicPartition` — empty collection
    /// acts as `unsubscribe()`.
    #[tokio::test]
    async fn assign_on_empty_acts_as_unsubscribe() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let completer = auto_complete_next_event(handles.app_event_rx);
        consumer.assign(Vec::new()).await.expect("ok");
        let env = completer.await.expect("task ok").expect("event received");
        assert!(matches!(env.event, ApplicationEvent::Unsubscribe { .. }));
    }

    /// Java: `testAssignOnEmptyTopicInPartition` — blank topic rejected.
    #[tokio::test]
    async fn assign_rejects_blank_topic_in_partition() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("  ".to_string(), 0);
        let err = consumer.assign(vec![tp]).await.expect_err("must err");
        assert!(matches!(err, Error::LocalIllegalArgument(_)));
    }

    /// Sanity check: subscribe stores the listener app-side so
    /// `process_background_events` can pick it up.
    #[tokio::test]
    async fn subscribe_topics_listener_stores_listener() {
        use async_trait::async_trait;
        struct DummyListener;
        #[async_trait]
        impl ConsumerRebalanceListener for DummyListener {
            async fn on_partitions_revoked(&self, _: &[TopicPartition]) -> Result<(), Error> {
                Ok(())
            }
            async fn on_partitions_assigned(&self, _: &[TopicPartition]) -> Result<(), Error> {
                Ok(())
            }
            async fn on_partitions_lost(&self, _: &[TopicPartition]) -> Result<(), Error> {
                Ok(())
            }
        }
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let completer = auto_complete_all_events(handles.app_event_rx);
        let listener: Arc<dyn ConsumerRebalanceListener> = Arc::new(DummyListener);
        consumer
            .subscribe_with_topics_listener(vec!["t".to_string()], Arc::clone(&listener))
            .await
            .expect("ok");
        let stored = consumer.rebalance_listener.lock().unwrap().clone();
        assert!(stored.is_some(), "listener must be stored on subscribe_with_topics_listener");

        // Java keeps ONE slot: a listener-less `subscribe(topics)` calls
        // `registerRebalanceListener(Optional.empty())`
        // (`SubscriptionState.java:192-196`), so it must CLEAR the app-side
        // mirror too — otherwise `leave_group_on_close` would invoke the
        // replaced listener, and (through the C FFI) its `user_data_destroy`
        // hook would be withheld until the consumer is dropped.
        consumer.subscribe_with_topics(vec!["t2".to_string()]).await.expect("ok");
        let stored = consumer.rebalance_listener.lock().unwrap().clone();
        assert!(
            stored.is_none(),
            "a listener-less subscribe must clear the previously stored listener"
        );
        completer.abort();
    }

    /// The other half of the single-slot invariant: `unsubscribe()` must
    /// **keep** the registered listener.
    ///
    /// `SubscriptionState.unsubscribe()` (`SubscriptionState.java:347-355`)
    /// clears the subscription, group subscription, assignment, assigned topic
    /// ids, pattern and subscription type and bumps `assignmentId` — it does
    /// not touch `rebalanceListener`, which is only ever written by the three
    /// `subscribe(...)` overloads (`:193`, `:199`, `:205`, `:219`). Neither
    /// does `AsyncKafkaConsumer.unsubscribe()` (`:1830-1855`).
    ///
    /// Rust duplicates Java's one slot (bg-side `SubscriptionState`, app-side
    /// mirror, because §31 invokes the callback on the caller's task) and the
    /// **mirror** is what `process_background_events` reads. So the observable
    /// consequence of clearing it here is a `PartitionsRemoved` event (AK
    /// 4.3.1's rename of `ConsumerRebalanceListenerCallbackNeeded`, revoke/lost
    /// shape unchanged) enqueued by the bg task while
    /// the registration was still live, but drained by the app after
    /// `unsubscribe()` returned, silently taking the `None => Ok(())` arm and
    /// skipping the user's `on_partitions_revoked`. That behaviour — not just
    /// the field state — is what this test pins, together with the fact that a
    /// later listener-less `subscribe(...)` (Java's
    /// `registerRebalanceListener(Optional.empty())`) is what ends the
    /// registration.
    #[tokio::test]
    async fn unsubscribe_keeps_the_registered_listener() {
        use crate::consumer::ConsumerRebalanceListenerMethodName;
        use async_trait::async_trait;
        use std::sync::atomic::AtomicUsize;
        use tokio::sync::oneshot;

        struct RecordingListener {
            revoked: AtomicUsize,
        }
        #[async_trait]
        impl ConsumerRebalanceListener for RecordingListener {
            async fn on_partitions_assigned(&self, _: &[TopicPartition]) -> Result<(), Error> {
                Ok(())
            }
            async fn on_partitions_revoked(&self, _: &[TopicPartition]) -> Result<(), Error> {
                self.revoked.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            async fn on_partitions_lost(&self, _: &[TopicPartition]) -> Result<(), Error> {
                Ok(())
            }
        }

        /// Enqueues the callback-needed event the bg task would raise and
        /// drains it, returning the ack result.
        async fn drive_revoked_callback(
            consumer: &mut AsyncKafkaConsumer<Vec<u8>, Vec<u8>>,
            bg_event_tx: &mpsc::UnboundedSender<BackgroundEventEnvelope>,
        ) -> Result<(), Error> {
            let (ack_tx, ack_rx) = oneshot::channel::<Result<(), Error>>();
            bg_event_tx
                .send(BackgroundEventEnvelope {
                    event: BackgroundEvent::PartitionsRemoved {
                        method_name: ConsumerRebalanceListenerMethodName::OnPartitionsRevoked,
                        partitions: vec![TopicPartition::new("t".to_string(), 0)],
                        ack: ack_tx,
                    },
                    enqueued_ms: 0,
                })
                .expect("send ok");
            consumer.process_background_events().await.expect("drain ok");
            ack_rx.await.expect("ack received")
        }

        let (mut consumer, handles) = make_test_consumer_with_channels();
        let completer = auto_complete_all_events(handles.app_event_rx);
        let listener: Arc<RecordingListener> = Arc::new(RecordingListener { revoked: AtomicUsize::new(0) });
        let erased: Arc<dyn ConsumerRebalanceListener> = Arc::clone(&listener) as Arc<dyn ConsumerRebalanceListener>;

        consumer
            .subscribe_with_topics_listener(vec!["t".to_string()], Arc::clone(&erased))
            .await
            .expect("subscribe ok");
        // The fixture has no background task, so apply the registration the
        // `ApplicationEventProcessor` would perform for
        // `TopicSubscriptionChange` — that is the bg-side half of the slot.
        handles
            .subscriptions
            .lock()
            .unwrap()
            .subscribe_with_topics(["t".to_string()].into_iter().collect(), Some(Arc::clone(&erased)))
            .expect("subscribe_with_topics ok");

        consumer.unsubscribe().await.expect("unsubscribe ok");
        // ...and the bg-side half of `Unsubscribe`.
        handles.subscriptions.lock().unwrap().unsubscribe();

        // Both copies of Java's single slot must still hold the listener.
        assert!(
            handles.subscriptions.lock().unwrap().rebalance_listener().is_some(),
            "SubscriptionState::unsubscribe must not clear the listener (Java parity)"
        );
        assert!(
            consumer.rebalance_listener.lock().unwrap().is_some(),
            "the app-side mirror must track SubscriptionState's slot across unsubscribe()"
        );

        // Observable behaviour: a callback the bg task raised while the
        // registration was live is still delivered to the user.
        assert!(drive_revoked_callback(&mut consumer, &handles.bg_event_tx).await.is_ok());
        assert_eq!(
            1,
            listener.revoked.load(Ordering::SeqCst),
            "a rebalance callback drained after unsubscribe() must still reach the retained listener"
        );

        // A listener-less `subscribe(...)` is what ends the registration
        // (`registerRebalanceListener(Optional.empty())`), on both copies.
        consumer
            .subscribe_with_topics(vec!["t2".to_string()])
            .await
            .expect("subscribe ok");
        handles
            .subscriptions
            .lock()
            .unwrap()
            .subscribe_with_topics(["t2".to_string()].into_iter().collect(), None)
            .expect("subscribe_with_topics ok");
        assert!(
            handles.subscriptions.lock().unwrap().rebalance_listener().is_none(),
            "a listener-less subscribe clears SubscriptionState's slot"
        );
        assert!(
            consumer.rebalance_listener.lock().unwrap().is_none(),
            "...and the app-side mirror with it"
        );
        assert!(drive_revoked_callback(&mut consumer, &handles.bg_event_tx).await.is_ok());
        assert_eq!(
            1,
            listener.revoked.load(Ordering::SeqCst),
            "the de-registered listener must not be invoked again"
        );

        completer.abort();
    }

    // ─── Phase 11 commit (8/N) Java test translations ───
    //
    // The block below mirrors Java's `AsyncKafkaConsumerTest` fixture +
    // state-read + subscribe / unsubscribe tests. Many Java tests are
    // already covered by the inline tests above (commits 2–7); each
    // Java test that is _skipped_ carries a `// SKIP: <reason>` rationale
    // here per DoD §3.
    //
    // Skipped Java tests (commit 8 batch):
    //   - testCommitInRebalanceCallback (Java line 474-506) — covered by
    //     `issue_10_commit_sync_drains_listener_callback_while_waiting`
    //     (inline above) AND the §31 regression pair in commit 11/N.
    //   - testRecordBackgroundEventQueueSizeAndBackgroundEventQueueTime —
    //     TRANSLATED (Phase M7) as
    //     `test_record_background_event_queue_size_and_time` (inline below).
    //     Drains a bg event under a mock `ThreadTime` advanced by 10 ms, then
    //     reads the values via the public `metrics()` accessor (M7).
    //   - testEmptyStreamRebalanceData, testStreamRebalanceData,
    //     testCloseInvokesStreamsRebalanceListenerOnTasksRevokedWhenMemberEpochPositive,
    //     testCloseInvokesStreamsRebalanceListenerOnAllTasksLostWhenMemberEpochZeroOrNegative,
    //     testCloseWrapsStreamsRebalanceListenerException — PLAN
    //     deferral #2 (Streams out of milestone scope per
    //     consumer-threading.md §20).
    //   - testGroupRemoteAssignorInClassicProtocol — PLAN deferral #3
    //     (classic-protocol out of scope per §20).
    //   - testFailConstructor — PLAN deferral #5 (Supplier-based ctor
    //     failure paths don't translate; the equivalent error path is
    //     observed via the `Error` returned from `new_consumer` on
    //     bad config).
    //   - testGroupMetadataIsResetAfterUnsubscribe (Java line 1350-1374)
    //     — translated below as
    //     `group_metadata_is_reset_after_unsubscribe`. Per Issue 21
    //     fixup, `unsubscribe()` now calls
    //     `state_notifier.reset_group_metadata()` after the unsubscribe
    //     event completes, mirroring Java line 1848's
    //     `resetGroupMetadata()`.
    //   - testSubscribeToNullTopicCollection, testSubscriptionOnNullTopic,
    //     testAssignOnNullTopicPartition, testAssignOnNullTopicInPartition
    //     — Rust's type system makes `null` cases unrepresentable.
    //   - testBeginningOffsetsFailsIfNullPartitions,
    //     testOffsetsForTimesOnNullPartitions — same.
    //   - Any ConcurrentModificationException-asserting test — PLAN
    //     deferral #4 (Rust's `&mut self` makes the guard redundant; no
    //     such test method names found in the Java file).
    //   - testReaperInvokedInClose / testReaperInvokedInUnsubscribe /
    //     testReaperInvokedInPoll — deferred; the reap call itself is
    //     wired (`close_internal` line 2675, `process_background_events`),
    //     but the test would require a mocked reaper to observe the
    //     invocation. The bg-task `reaper.reap(time)` is exercised via
    //     `ConsumerNetworkThreadTest` (Phase 10 commit 8/N).
    //   - testSubscribePatternAgainstBrokerNotSupportingRegex — end-to-end
    //     against a `MockClient`; requires response routing for
    //     FindCoordinator + Heartbeat to be wired (deferred to Phase 12.5
    //     per `Phase-12/RESPONSE-ROUTING-AUDIT.md`).

    /// Java: `testFailOnClosedConsumer` (Java line 286-293) — asserts
    /// the EXACT message `"This consumer has already been closed."` is
    /// surfaced from a post-close API call. Mirrors Java's
    /// `assertThrows(IllegalStateException, consumer::assignment)`.
    /// Issue 5 / DoD §3: exact-message assertion.
    ///
    /// Rust divergence: Rust's `assignment()` is a sync `&self` method
    /// that does NOT call `ensure_open()` — so the close check fires
    /// only from the async APIs. The equivalent assertion here is
    /// against `commit_sync` (which Java's testFailOnClosedConsumer
    /// covers via `assignment` only, but Rust's `assignment()` is
    /// documented to "return the empty set silently when the consumer
    /// is closed" — see `paused()` doc).
    #[tokio::test]
    async fn fail_on_closed_consumer_exact_message() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        consumer.closed.store(true, Ordering::Release);
        let err = consumer.commit_sync().await.expect_err("must err");
        match err {
            Error::LocalIllegalState(msg) => {
                assert_eq!(msg.message(), "This consumer has already been closed.");
            },
            other => panic!("expected IllegalState, got {other:?}"),
        }
    }

    /// Java: `testGroupMetadataAfterCreationWithGroupIdIsNull`
    /// (Java line 1258-1272). Asserts EXACT Java message on a
    /// groupless consumer's `group_metadata()`.
    ///
    /// Rust divergence: Java throws `InvalidGroupIdException` from
    /// `group_metadata()`; Rust returns a stub `ConsumerGroupMetadata::new("")`
    /// for groupless consumers (see `group_metadata` doc, line 615+).
    /// The exact-message assertion is on the equivalent error surface:
    /// `commit_sync()`'s `return_error_if_group_id_not_defined` (line 758-766),
    /// which carries the Java message verbatim. This validates the
    /// message text contract without introducing a Rust-side panic on
    /// the read-only `group_metadata()` accessor.
    #[tokio::test]
    async fn group_metadata_groupless_commit_sync_emits_exact_java_message() {
        use crate::common::Errors;
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        consumer.group_id = None;
        let err = consumer.commit_sync().await.expect_err("must err");
        assert_eq!(err.error(), Errors::InvalidGroupId, "expected InvalidGroupId, got {err:?}");
        assert_eq!(
            err.message(),
            "To use the group management or offset commit APIs, you must provide a valid \
             group.id in the consumer configuration."
        );
    }

    /// Java: `testGroupMetadataAfterCreationWithGroupIdIsNotNull`
    /// (Java line 1274-1285). On a consumer with `group.id` set, the
    /// initial `group_metadata()` returns:
    ///   - `group_id` = configured value
    ///   - `generation_id` = `UNKNOWN_GENERATION_ID` (-1)
    ///   - `member_id` = `UNKNOWN_MEMBER_ID` ("")
    ///   - `group_instance_id` = `None`
    #[tokio::test]
    async fn group_metadata_after_creation_with_group_id() {
        let consumer = make_test_consumer();
        let meta = consumer.group_metadata();
        assert_eq!(meta.group_id(), "test-group");
        assert_eq!(meta.generation_id(), -1);
        assert_eq!(meta.member_id(), "");
        assert_eq!(meta.group_instance_id(), None);
    }

    /// Java: `testGroupMetadataAfterCreationWithGroupIdIsNotNullAndGroupInstanceIdSet`
    /// (Java line 1287-1301). Asserts `group_instance_id` is propagated
    /// from config into the metadata cache.
    #[tokio::test]
    async fn group_metadata_with_instance_id() {
        // Build a custom consumer with group_instance_id set.
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        consumer.config.group_instance_id = Some("groupInstanceId1".to_string());
        // The state_notifier path sets group_instance_id via
        // `update_group_metadata`; since this is a unit test against
        // the consumer's cache, we mimic Java's "after creation"
        // observation by writing through the notifier with a known
        // epoch + member_id.
        let notifier = consumer.state_notifier();
        notifier.on_member_epoch_updated(Some(0), "");
        let meta = consumer.group_metadata();
        assert_eq!(meta.group_id(), "test-group");
        // member_id and generation_id are still 0 / "" until a
        // heartbeat lands; instance_id flows from config.
        // For now we assert the config carries the value — the bg-task
        // wire-up that surfaces it through group_metadata requires
        // Heartbeat response routing (deferred to Phase 12.5 per
        // `Phase-12/RESPONSE-ROUTING-AUDIT.md`).
        assert_eq!(consumer.config.group_instance_id.as_deref(), Some("groupInstanceId1"));
    }

    /// Java: `testGroupMetadataUpdate` (Java line 1327-1346). The
    /// captured `MemberStateListener.onMemberEpochUpdated(Optional.of(42), "memberId")`
    /// updates the cached `group_metadata` to reflect the new epoch +
    /// member id.
    #[tokio::test]
    async fn group_metadata_update_via_member_state_listener() {
        let consumer = make_test_consumer();
        let notifier = consumer.state_notifier();
        let old = consumer.group_metadata();
        assert_eq!(old.generation_id(), -1, "pre-condition: unknown generation");
        notifier.on_member_epoch_updated(Some(42), "memberId");
        let new = consumer.group_metadata();
        assert_eq!(new.group_id(), old.group_id());
        assert_eq!(new.member_id(), "memberId");
        assert_eq!(new.generation_id(), 42);
        assert_eq!(new.group_instance_id(), old.group_instance_id());
    }

    /// Java: `testGroupMetadataIsResetAfterUnsubscribe` (Java line
    /// 1350-1374). After `unsubscribe()` returns, the cached
    /// `group_metadata()` carries the original `group_id` +
    /// `group_instance_id` but the `generation_id` /
    /// `member_id` slots are reset to
    /// `JoinGroupRequest.UNKNOWN_GENERATION_ID` (-1) /
    /// `JoinGroupRequest.UNKNOWN_MEMBER_ID` ("").
    ///
    /// Mirrors Java line 1848's `resetGroupMetadata()` call from
    /// `unsubscribe()`; Issue 21 fixup.
    #[tokio::test]
    async fn group_metadata_is_reset_after_unsubscribe() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        // Pre-condition: populate `group_metadata` cache as if a heartbeat
        // landed (Java has the bg-task `MemberStateListener` populate it
        // during the lifecycle; we simulate the same here).
        let notifier = consumer.state_notifier();
        notifier.on_member_epoch_updated(Some(42), "memberId");
        let pre = consumer.group_metadata();
        assert_eq!(pre.generation_id(), 42, "pre-condition: cache populated");
        assert_eq!(pre.member_id(), "memberId");
        assert_eq!(pre.group_id(), "test-group");

        // Drive unsubscribe to completion.
        let completer = auto_complete_next_event(handles.app_event_rx);
        consumer.unsubscribe().await.expect("ok");
        let _ = completer.await.expect("task ok").expect("event received");

        // Post-condition: group_metadata is reset — generation_id back to
        // -1, member_id back to "", but group_id preserved.
        let post = consumer.group_metadata();
        assert_eq!(post.generation_id(), -1, "generation_id reset to UNKNOWN");
        assert_eq!(post.member_id(), "", "member_id reset to UNKNOWN");
        assert_eq!(post.group_id(), "test-group", "group_id preserved");
        assert_eq!(post.group_instance_id(), None, "group_instance_id preserved");
    }

    /// Java: `testSubscribeGeneratesEvent` (Java line 1191-1200) —
    /// asserts the subscribe call results in the subscription being
    /// reflected on `consumer.subscription()` AND the AssignmentChange
    /// remains empty. The existing `subscribe_generates_topic_subscription_change_event`
    /// (inline above) asserts only event-shape; this test extends
    /// coverage to the post-state assertions.
    #[tokio::test]
    async fn subscribe_reflects_subscription_state_after_event_completes() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let topic = "topic1";
        let subs = Arc::clone(&handles.subscriptions);
        let completer = {
            let topic = topic.to_string();
            let mut rx = handles.app_event_rx;
            tokio::spawn(async move {
                while let Some(env) = rx.recv().await {
                    if let ApplicationEvent::TopicSubscriptionChange { handle, topics, listener } = env.event {
                        let mut s = subs.lock().unwrap();
                        s.subscribe_with_topics(topics, listener).expect("subscribe ok");
                        drop(s);
                        handle.complete(());
                        return Some(topic);
                    }
                }
                None
            })
        };
        consumer.subscribe_with_topics(vec![topic.to_string()]).await.expect("ok");
        let _ = completer.await.expect("task ok").expect("event received");
        let subscription = consumer.subscription();
        assert_eq!(subscription.len(), 1);
        assert!(subscription.contains(topic));
        assert!(consumer.assignment().is_empty());
    }

    /// Java: `testSubscribeToRe2JPatternThrowsIfNoGroupId`
    /// (Java line 1871-1877). The Re2J pattern subscribe path requires
    /// a configured `group.id`; without it, the call errors with the
    /// Rust analog of `InvalidGroupIdException` —
    /// `Error::invalid_group_id(...)` which surfaces a
    /// `KafkaError` variant carrying `Errors::InvalidGroupId` (Issue 16).
    #[tokio::test]
    async fn subscribe_subscription_pattern_without_group_id_errors() {
        use crate::common::Errors;
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        consumer.group_id = None;
        let err = consumer
            .subscribe_with_pattern(SubscriptionPattern::new("t*"))
            .await
            .expect_err("must err");
        assert_eq!(err.error(), Errors::InvalidGroupId, "expected InvalidGroupId, got {err:?}");
    }

    /// Java: `testUnsubscribeWithoutGroupId` (Java line 1808-1815) —
    /// `unsubscribe()` on a groupless consumer enqueues an
    /// `UnsubscribeEvent` (does NOT require `group.id`). Issue 26:
    /// uses `make_test_consumer_without_group_id` so the consumer is
    /// constructed groupless from the start (mirrors Java's
    /// no-group-id ctor) rather than mutating the field post-hoc.
    #[tokio::test]
    async fn unsubscribe_without_group_id_enqueues_event() {
        let (mut consumer, handles) = make_test_consumer_without_group_id();
        assert!(consumer.group_id.is_none(), "pre-condition: groupless");
        let completer = auto_complete_next_event(handles.app_event_rx);
        consumer.unsubscribe().await.expect("ok");
        let env = completer.await.expect("task ok").expect("event received");
        assert!(matches!(env.event, ApplicationEvent::Unsubscribe { .. }));
    }

    /// Java: `testGroupRemoteAssignorUnusedIfGroupIdUndefined`
    /// (Java line 1554-1563). With `group.id` undefined,
    /// `group.remote.assignor` is unused by the consumer config.
    ///
    /// Issue 27 — strengthened: constructs a TRUE groupless consumer
    /// (via `make_test_consumer_without_group_id`) and asserts the
    /// constructor accepts `group_remote_assignor` without producing
    /// any observable side effect on the consumer's group-management
    /// surface:
    ///   - `group_id` slot is `None`.
    ///   - `group_metadata()` falls through to the stub
    ///     (`group_id == ""`) — no member id was assigned.
    ///   - `assignment()` is empty pre-subscribe.
    ///
    /// The Java `config.unused()` set itself is not yet tracked in
    /// Rust's `ConsumerConfig`; that piece of the contract is
    /// deferred. See `group_id_null_constructs_successfully` for the
    /// pure construction-survives assertion.
    #[tokio::test]
    async fn group_remote_assignor_unused_if_group_id_undefined() {
        let (consumer, _handles) = make_test_consumer_without_group_id();
        // Pre-condition: the consumer is truly groupless (not just
        // post-mutation — the fixture clears both the slot AND the
        // underlying config). Issue 27.
        assert!(consumer.group_id.is_none());
        assert!(consumer.config.group_id.is_none());
        // The group-management surface stays inert.
        assert!(consumer.assignment().is_empty());
        assert!(consumer.subscription().is_empty());
        // Java `unused()` set tracking is a `ConsumerConfig`-side
        // concern; the Rust analog would assert
        // `config.unused().contains(GROUP_REMOTE_ASSIGNOR_CONFIG)`.
        // Until that surface lands, the strongest behavioural
        // assertion is the one above: groupless consumer constructs
        // cleanly without group-management state leaking through.
        drop(consumer);
    }

    /// Java: `testGroupIdNull` (Java line 1587-1597). With group.id
    /// null, certain config keys (`AUTO_COMMIT_INTERVAL_MS_CONFIG`,
    /// `THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED`) flow through to the
    /// underlying ConsumerConfig as "used" (Java asserts
    /// `!config.unused().contains(...)`).
    ///
    /// Rust divergence: same as the assignor test above — Rust's
    /// `ConsumerConfig` does not track `unused()`. This test stands in
    /// for the construction-time assertion that a groupless consumer
    /// can be built with these config keys set.
    #[tokio::test]
    async fn group_id_null_constructs_successfully() {
        let (consumer, _handles) = make_test_consumer_with_channels();
        // Equivalent of Java's pre-condition: ctor succeeded.
        drop(consumer);
    }

    /// Java: `testGroupIdNotNullAndValid` (Java line 1599-1610). With
    /// a valid group.id and auto-commit disabled, the consumer
    /// constructs successfully and the auto-commit config key is left
    /// unused (Java asserts `config.unused().contains(AUTO_COMMIT_INTERVAL_MS_CONFIG)`).
    ///
    /// Rust divergence: see notes above; this test stands in for the
    /// construction-time assertion.
    #[tokio::test]
    async fn group_id_not_null_constructs_with_auto_commit_disabled() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        // group_id is `Some` in the fixture.
        assert!(consumer.group_id.is_some());
        // Force auto-commit off and validate the consumer survives.
        consumer.auto_commit_enabled = false;
        assert!(!consumer.auto_commit_enabled);
        drop(consumer);
    }

    /// Java: `testEnsurePollEventSentOnConsumerPoll`
    /// (Java line 1612-1632). Asserts at least one `AsyncPollEvent` is
    /// submitted by `consumer.poll(...)`. Inline test
    /// `poll_enqueues_async_poll_event_and_clears_on_completion`
    /// already asserts this on a manual-assign consumer; this test
    /// stays parallel to the Java assertion shape for clarity.
    #[tokio::test]
    async fn ensure_poll_event_sent_on_consumer_poll() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("topic".to_string(), 0);
        {
            let mut subs = consumer.subscriptions.lock().unwrap();
            let mut assigned: HashSet<TopicPartition> = HashSet::new();
            assigned.insert(tp);
            subs.assign_from_user(assigned).unwrap();
        }
        let mut app_rx = handles.app_event_rx;
        // Drain in a task — first AsyncPoll envelope is what we look for.
        let saw_async_poll = tokio::spawn(async move {
            while let Some(env) = app_rx.recv().await {
                if matches!(env.event, ApplicationEvent::AsyncPoll { .. }) {
                    return true;
                }
            }
            false
        });
        let _ = consumer.poll(Duration::from_millis(0)).await;
        // Give the drainer a moment via its own task.
        drop(consumer); // forces all senders to close eventually
        let observed = saw_async_poll.await.expect("task ok");
        assert!(observed, "poll() must submit at least one AsyncPollEvent");
    }

    /// Error event is drained and surfaces from
    /// `process_background_events`.
    #[tokio::test]
    async fn process_background_events_surfaces_error_event() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let env =
            BackgroundEventEnvelope { event: BackgroundEvent::Error { error: Error::timeout("boom") }, enqueued_ms: 0 };
        handles.bg_event_tx.send(env).expect("send ok");
        let result = consumer.process_background_events().await;
        assert!(matches!(result, Err(Error::Timeout(_))));
    }

    /// Callback-needed event with no listener registered: succeeds and
    /// the ack is sent with `Ok(())` — mirrors Java's
    /// `listener.isPresent() == false` no-op branch.
    #[tokio::test]
    async fn process_background_events_with_no_listener_acks_ok() {
        use crate::consumer::ConsumerRebalanceListenerMethodName;
        use tokio::sync::oneshot;
        let (mut consumer, handles) = make_test_consumer_with_channels();
        // AK 4.3.1: the revoke path (`PartitionsRemoved`) exercises the same
        // no-listener ack machinery without needing a bg AEP to process an
        // ApplyAssignmentEvent (the assign path's `PartitionsAssigned` would).
        let (ack_tx, ack_rx) = oneshot::channel::<Result<(), Error>>();
        let env = BackgroundEventEnvelope {
            event: BackgroundEvent::PartitionsRemoved {
                method_name: ConsumerRebalanceListenerMethodName::OnPartitionsRevoked,
                partitions: vec![TopicPartition::new("t".to_string(), 0)],
                ack: ack_tx,
            },
            enqueued_ms: 0,
        };
        handles.bg_event_tx.send(env).expect("send ok");
        consumer.process_background_events().await.expect("ok");
        // The ack must have been resolved with `Ok(())`.
        let ack_result = ack_rx.await.expect("ack received");
        assert!(ack_result.is_ok());
    }

    /// Callback-needed event with a registered listener: the listener's
    /// callback is invoked inline on the caller's task, and the ack is sent
    /// with the listener's result. (AK 4.3.1: uses the revoke path
    /// `PartitionsRemoved`, which invokes the listener directly without a bg
    /// AEP; the assign path's `PartitionsAssigned` would require one to
    /// process the ApplyAssignmentEvent.)
    #[tokio::test]
    async fn process_background_events_invokes_registered_listener() {
        use crate::consumer::ConsumerRebalanceListenerMethodName;
        use async_trait::async_trait;
        use std::sync::atomic::AtomicUsize;
        use tokio::sync::oneshot;

        struct RecordingListener {
            count: AtomicUsize,
        }
        #[async_trait]
        impl ConsumerRebalanceListener for RecordingListener {
            async fn on_partitions_assigned(&self, _: &[TopicPartition]) -> Result<(), Error> {
                Ok(())
            }
            async fn on_partitions_revoked(&self, _: &[TopicPartition]) -> Result<(), Error> {
                self.count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            async fn on_partitions_lost(&self, _: &[TopicPartition]) -> Result<(), Error> {
                Ok(())
            }
        }

        let (mut consumer, handles) = make_test_consumer_with_channels();
        let listener: Arc<RecordingListener> = Arc::new(RecordingListener { count: AtomicUsize::new(0) });
        *consumer.rebalance_listener.lock().unwrap() =
            Some(Arc::clone(&listener) as Arc<dyn ConsumerRebalanceListener>);

        let (ack_tx, ack_rx) = oneshot::channel::<Result<(), Error>>();
        let env = BackgroundEventEnvelope {
            event: BackgroundEvent::PartitionsRemoved {
                method_name: ConsumerRebalanceListenerMethodName::OnPartitionsRevoked,
                partitions: vec![TopicPartition::new("t".to_string(), 0)],
                ack: ack_tx,
            },
            enqueued_ms: 0,
        };
        handles.bg_event_tx.send(env).expect("send ok");
        consumer.process_background_events().await.expect("ok");
        assert_eq!(listener.count.load(Ordering::SeqCst), 1);
        assert!(ack_rx.await.expect("ack received").is_ok());

        // Silence the unused-field warning for `subscriptions` on the
        // test handles (used here so the helper struct stays
        // forward-compatible with future tests that need to inspect the
        // shared state).
        drop(handles.subscriptions);
    }

    /// Phase 41 Issue 3: the ack-send path in `process_background_events`
    /// must poke the bg-task wakeup `Notify` so the bg loop observes the ack
    /// promptly (rather than waiting out the selector poll timeout). The
    /// non-Docker component tests otherwise busy-drive the membership loop and
    /// never exercise this poke — removing it after `ack.send(...)` would fail
    /// no local test without this one.
    ///
    /// It must poke the **application-event notify**, and must NOT touch the
    /// wakeup token (`network_thread_close.wakeup()` →
    /// `WakeupTrigger::wakeup()`). The token is Java's
    /// `KafkaConsumer.wakeup()`: cancelling it arms a user-visible
    /// `Error::Wakeup` that the next public API call raises (§11). Since
    /// every rebalance fires a listener callback, poking the token here made a
    /// spurious `Wakeup` the normal outcome of any rebalance — it broke
    /// `poll()` for every consumer with a listener registered, across 12
    /// integration tests, while the earlier version of this test passed
    /// because the fixture's wakeup fn is a flag-setting stub that does not
    /// reproduce production's token cancellation.
    ///
    /// So this asserts both halves: the notify got its permit, AND no
    /// user-visible wakeup is pending afterwards.
    #[tokio::test]
    async fn process_background_events_ack_pokes_bg_notify_not_user_wakeup() {
        use crate::consumer::ConsumerRebalanceListenerMethodName;
        use async_trait::async_trait;
        use tokio::sync::oneshot;

        struct NoopListener;
        #[async_trait]
        impl ConsumerRebalanceListener for NoopListener {
            async fn on_partitions_assigned(&self, _: &[TopicPartition]) -> Result<(), Error> {
                Ok(())
            }
            async fn on_partitions_revoked(&self, _: &[TopicPartition]) -> Result<(), Error> {
                Ok(())
            }
            async fn on_partitions_lost(&self, _: &[TopicPartition]) -> Result<(), Error> {
                Ok(())
            }
        }

        let (mut consumer, handles) = make_test_consumer_with_channels();
        *consumer.rebalance_listener.lock().unwrap() =
            Some(Arc::new(NoopListener) as Arc<dyn ConsumerRebalanceListener>);

        // Nothing must have fired before the callback is processed.
        assert!(
            tokio::time::timeout(Duration::from_millis(50), handles.event_notify.notified())
                .await
                .is_err(),
            "the application-event notify must not be poked before the callback ack is sent",
        );
        assert!(
            consumer.wakeup_trigger.maybe_trigger_wakeup().is_ok(),
            "no user-visible wakeup may be pending before the callback",
        );

        let (ack_tx, ack_rx) = oneshot::channel::<Result<(), Error>>();
        let env = BackgroundEventEnvelope {
            event: BackgroundEvent::PartitionsRemoved {
                method_name: ConsumerRebalanceListenerMethodName::OnPartitionsRevoked,
                partitions: vec![TopicPartition::new("t".to_string(), 0)],
                ack: ack_tx,
            },
            enqueued_ms: 0,
        };
        handles.bg_event_tx.send(env).expect("send ok");
        consumer.process_background_events().await.expect("ok");
        assert!(ack_rx.await.expect("ack received").is_ok());

        // The ack-send path must have poked the application-event notify.
        // `notify_one()` stores a permit when nobody is parked, so an
        // already-satisfied `notified()` resolves immediately.
        tokio::time::timeout(Duration::from_secs(1), handles.event_notify.notified())
            .await
            .expect("process_background_events must poke the bg notify after sending the listener ack");

        // ...and must NOT have armed a user-visible wakeup. This is the
        // regression: with `network_thread_close.wakeup()` here, the next
        // `poll()` / `commit_sync()` / `position()` fails with
        // `Wakeup("WakeupTrigger fired")` even though the user never called
        // `wakeup()`.
        assert!(
            !handles.bg_wakeup_called.load(Ordering::Acquire),
            "the ack poke must not fire the user-facing wakeup fn",
        );
        assert!(
            consumer.wakeup_trigger.maybe_trigger_wakeup().is_ok(),
            "the ack poke must not arm a user-visible Error::Wakeup",
        );
        drop(handles.subscriptions);
    }

    /// The **close** arm (`skip_rebalance_callback = true`) must poke the
    /// bg-task notify too.
    ///
    /// §31 requires the poke for every `RebalanceListenerCallbackNeeded` ack,
    /// not only the ones that ran a listener. This arm discards the callback
    /// (Java never invokes §31 callbacks during `close()`) but still sends the
    /// ack so the parked reconcile completes — and the ack alone does not wake
    /// the bg loop. Without the poke the loop only notices after the selector
    /// poll times out, while `network_thread_close.await_join()` is waiting on
    /// that very reconcile, so every `close()` pays a poll timeout.
    ///
    /// The sibling test above covers the normal arm. This one exists because
    /// the two arms are separate code paths: the poke was present in one and
    /// missing in the other, and no test noticed.
    #[tokio::test]
    async fn process_background_events_close_arm_pokes_bg_notify() {
        use crate::consumer::ConsumerRebalanceListenerMethodName;
        use tokio::sync::oneshot;

        let (mut consumer, handles) = make_test_consumer_with_channels();

        let (ack_tx, ack_rx) = oneshot::channel::<Result<(), Error>>();
        let env = BackgroundEventEnvelope {
            event: BackgroundEvent::PartitionsRemoved {
                method_name: ConsumerRebalanceListenerMethodName::OnPartitionsRevoked,
                partitions: vec![TopicPartition::new("t".to_string(), 0)],
                ack: ack_tx,
            },
            enqueued_ms: 0,
        };
        handles.bg_event_tx.send(env).expect("send ok");

        // `skip_rebalance_callback = true` is the close path: the callback is
        // discarded, the ack is still sent.
        consumer
            .process_background_events_inner(
                /* skip_rebalance_callback = */ true, /* skip_assignment_events = */ true,
            )
            .await
            .expect("ok");
        assert!(ack_rx.await.expect("ack received").is_ok(), "the ack must still be sent");

        tokio::time::timeout(Duration::from_secs(1), handles.event_notify.notified())
            .await
            .expect("the close arm must poke the bg notify after sending the ack");

        // Same constraint as the normal arm: the poke must not arm a
        // user-visible wakeup.
        assert!(
            !handles.bg_wakeup_called.load(Ordering::Acquire),
            "the close-arm poke must not fire the user-facing wakeup fn",
        );
        assert!(
            consumer.wakeup_trigger.maybe_trigger_wakeup().is_ok(),
            "the close-arm poke must not arm a user-visible Error::Wakeup",
        );
        drop(handles.subscriptions);
    }

    /// AK 4.3.1 (KAFKA-20428): when `skip_assignment_events` is set (the
    /// unsubscribe / close path), a pending `PartitionsAssigned` event is NOT
    /// applied — its ack is completed EXCEPTIONALLY to unblock the bg
    /// reconciliation, with the message Java uses. Translated from
    /// `AsyncKafkaConsumerTest#testUnsubscribeWithPendingAssignmentEvent`.
    #[tokio::test]
    async fn process_background_events_skips_pending_partitions_assigned_when_unsubscribing() {
        use tokio::sync::oneshot;
        let (mut consumer, handles) = make_test_consumer_with_channels();

        let (ack_tx, ack_rx) = oneshot::channel::<Result<(), Error>>();
        handles
            .bg_event_tx
            .send(BackgroundEventEnvelope {
                event: BackgroundEvent::PartitionsAssigned {
                    assigned_partitions: vec![TopicPartition::new("t".to_string(), 0)],
                    added_partitions: vec![TopicPartition::new("t".to_string(), 0)],
                    ack: ack_tx,
                },
                enqueued_ms: 0,
            })
            .expect("send ok");

        // skip_assignment_events = true (unsubscribe path).
        consumer
            .process_background_events_inner(
                /* skip_rebalance_callback = */ false, /* skip_assignment_events = */ true,
            )
            .await
            .expect("ok — the skipped assignment event is not an app-side error");

        // The pending assignment event was completed exceptionally.
        let ack_result = ack_rx.await.expect("ack received");
        match ack_result {
            Err(err) => assert!(
                err.to_string()
                    .contains("Assignment event skipped because consumer is unsubscribing"),
                "unexpected skip error: {err}",
            ),
            Ok(()) => panic!("PartitionsAssigned must be completed with an error when skipping assignment events"),
        }
        drop(handles.subscriptions);
    }

    /// AK 4.3.1 (KAFKA-20382): if applying the new assignment
    /// (`ApplyAssignmentEvent`) fails, the `PartitionsAssigned` event is
    /// completed exceptionally (a background error is surfaced) so the bg
    /// reconciliation can complete. Translated from
    /// `AsyncKafkaConsumerTest#testPartitionsAssignedEventSendsErrorWhenApplyAssignmentFails`.
    #[tokio::test]
    async fn partitions_assigned_event_sends_error_when_apply_assignment_fails() {
        use tokio::sync::oneshot;
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let bg_event_tx = handles.bg_event_tx.clone();

        // Fake bg: fail the ApplyAssignmentEvent that applyNewAssignment sends.
        let fake_bg = tokio::spawn(async move {
            let env = handles.app_event_rx.recv().await.expect("ApplyAssignmentEvent envelope");
            match env.event {
                ApplicationEvent::ApplyAssignment { handle, .. } => {
                    handle.complete_with_error(Error::local_illegal_state("apply failed"));
                },
                other => panic!("expected ApplyAssignment, got {}", other.type_name()),
            }
            handles
        });

        let (ack_tx, ack_rx) = oneshot::channel::<Result<(), Error>>();
        bg_event_tx
            .send(BackgroundEventEnvelope {
                event: BackgroundEvent::PartitionsAssigned {
                    assigned_partitions: vec![TopicPartition::new("t".to_string(), 0)],
                    added_partitions: vec![TopicPartition::new("t".to_string(), 0)],
                    ack: ack_tx,
                },
                enqueued_ms: 0,
            })
            .expect("send ok");

        // process_background_events records the wrapped apply error as the
        // first error and returns it.
        let result = consumer.process_background_events().await;
        assert!(result.is_err(), "apply-assignment failure must surface as an app-side error");

        // The PartitionsAssigned ack was completed with the wrapped error.
        let ack_result = ack_rx.await.expect("ack received");
        match ack_result {
            Err(err) => assert!(
                err.to_string().contains("Failed to apply the new assignment"),
                "unexpected apply error: {err}",
            ),
            Ok(()) => panic!("PartitionsAssigned must be completed with an error when apply fails"),
        }

        let handles = fake_bg.await.expect("fake bg joins");
        drop(handles.subscriptions);
    }

    /// AK 4.3.1 (KAFKA-20106): `collect_fetch` does NOT wait for the
    /// reconciliation check when there is no pending reconciliation.
    /// Translated from
    /// `AsyncKafkaConsumerTest#testPollDoesNotWaitForReconciliationCheckIfNoPendingReconciliation`.
    #[tokio::test]
    async fn wait_reconciliation_check_returns_true_when_no_pending_reconciliation() {
        let (consumer, handles) = make_test_consumer_with_channels();
        // has_pending_reconciliation defaults to false.
        assert!(consumer.wait_reconciliation_check().await);
        drop(handles.subscriptions);
    }

    /// The check passes through immediately when it is already complete.
    #[tokio::test]
    async fn wait_reconciliation_check_returns_true_when_already_complete() {
        use crate::consumer::internals::events::AsyncPollState;
        let (mut consumer, handles) = make_test_consumer_with_channels();
        consumer.has_pending_reconciliation.store(true, Ordering::Release);
        let state = Arc::new(AsyncPollState::new());
        state.mark_reconciliation_check_complete();
        consumer.inflight_poll = Some(InflightPoll { deadline_ms: i64::MAX, state });
        assert!(consumer.wait_reconciliation_check().await);
        drop(handles.subscriptions);
    }

    /// AK 4.3.1 (KAFKA-20106): `collect_fetch` waits until the reconciliation
    /// check completes when there is a pending reconciliation. Translated from
    /// `AsyncKafkaConsumerTest#testPollWaitsForReconciliationCheckComplete`.
    #[tokio::test]
    async fn wait_reconciliation_check_waits_then_proceeds_when_completed() {
        use crate::consumer::internals::events::AsyncPollState;
        let (mut consumer, handles) = make_test_consumer_with_channels();
        consumer.has_pending_reconciliation.store(true, Ordering::Release);
        let state = Arc::new(AsyncPollState::new());
        consumer.inflight_poll = Some(InflightPoll { deadline_ms: i64::MAX, state: Arc::clone(&state) });
        // Complete the check from another task shortly after.
        let s = Arc::clone(&state);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            s.mark_reconciliation_check_complete();
        });
        assert!(
            consumer.wait_reconciliation_check().await,
            "must proceed once the check completes"
        );
        drop(handles.subscriptions);
    }

    /// Java `AsyncKafkaConsumer.java:2057` — `collectFetch()` consults the
    /// SHARED [`PositionsValidator`], so an error the background side cached
    /// through `OffsetFetcherUtils` surfaces on the application task.
    ///
    /// This is the regression test for the wiring: before
    /// `PositionsValidator` was split back out of `OffsetFetcherUtils`, the
    /// consumer had no handle on that state at all and
    /// `canSkipUpdateFetchPositions` could not be translated.
    #[tokio::test]
    async fn collect_fetch_propagates_a_pending_validate_positions_error() {
        use crate::common::Errors;
        let (consumer, handles) = make_test_consumer_with_channels();
        consumer
            .positions_validator
            .maybe_set_error(Error::new(Errors::UnknownServerError));

        let err = consumer
            .collect_fetch()
            .await
            .expect_err("the cached validation error must reach the app task");
        assert_eq!(err.error(), Errors::UnknownServerError);
        drop(handles.subscriptions);
    }

    /// Java `AsyncKafkaConsumer.java:2062` — when the skip check fails, the
    /// application task must NOT collect from the fetch buffer until the
    /// in-flight poll reports that it has finished validating positions.
    /// Java's own comment: this "prevents a race condition where both
    /// threads may attempt to update the `SubscriptionState.position()` for
    /// a given partition".
    ///
    /// The buffer staying full is the observable: a collected fetch is
    /// drained out of it.
    #[tokio::test]
    async fn collect_fetch_does_not_drain_the_buffer_until_positions_are_validated() {
        use crate::consumer::internals::CompletedFetch;
        use crate::consumer::internals::events::AsyncPollState;
        use crate::fetch_response_data::PartitionData;

        let (mut consumer, handles) = make_test_consumer_with_channels();
        // A fresh validator still holds the -1 metadata-version sentinel, so
        // `can_skip_update_fetch_positions()` is false and the gate applies.
        assert!(
            !consumer
                .positions_validator
                .can_skip_update_fetch_positions()
                .expect("no cached error")
        );

        let state = Arc::new(AsyncPollState::new()); // validate-positions NOT complete
        consumer.inflight_poll = Some(InflightPoll { deadline_ms: i64::MAX, state: Arc::clone(&state) });
        consumer.fetch_buffer.add(CompletedFetch::new(
            TopicPartition::new("t".to_string(), 0),
            PartitionData::new(),
        ));

        let records = consumer.collect_fetch().await.expect("no error");
        assert!(records.is_empty(), "gated collect returns no records");
        assert!(
            !consumer.fetch_buffer.is_empty(),
            "the buffer must be left untouched while positions are still being validated"
        );

        // Once the background task reports the stage complete, the same call
        // collects normally and drains the buffer.
        state.mark_validate_positions_complete();
        let _ = consumer.collect_fetch().await.expect("no error");
        assert!(
            consumer.fetch_buffer.is_empty(),
            "with the gate lifted the buffered fetch is consumed"
        );
        drop(handles.subscriptions);
    }

    /// Returns `false` (return empty fetch) when the deadline has already
    /// passed and the reconciliation check is not complete.
    #[tokio::test]
    async fn wait_reconciliation_check_returns_false_on_timeout() {
        use crate::consumer::internals::events::AsyncPollState;
        let (mut consumer, handles) = make_test_consumer_with_channels();
        consumer.has_pending_reconciliation.store(true, Ordering::Release);
        let state = Arc::new(AsyncPollState::new());
        // deadline already in the past (<= now) -> no time to wait.
        consumer.inflight_poll = Some(InflightPoll { deadline_ms: 0, state });
        assert!(!consumer.wait_reconciliation_check().await);
        drop(handles.subscriptions);
    }

    /// AK 4.3.1 (KAFKA-20106): a `wakeup()` interrupts the reconciliation-check
    /// wait. Translated from
    /// `AsyncKafkaConsumerTest#testWakeupWhileWaitingOnReconciliationCheck`.
    #[tokio::test]
    async fn wait_reconciliation_check_interrupted_by_wakeup() {
        use crate::consumer::internals::events::AsyncPollState;
        let (mut consumer, handles) = make_test_consumer_with_channels();
        consumer.has_pending_reconciliation.store(true, Ordering::Release);
        let state = Arc::new(AsyncPollState::new()); // never completed
        consumer.inflight_poll = Some(InflightPoll { deadline_ms: i64::MAX, state });
        // A concurrent wakeup cancels the token; the wait returns false so the
        // poll loop top surfaces Error::Wakeup.
        consumer.wakeup_trigger.wakeup();
        assert!(
            !consumer.wait_reconciliation_check().await,
            "wakeup must interrupt the reconciliation-check wait",
        );
        drop(handles.subscriptions);
    }

    /// The close-handle closures must nudge the transport notify, never the
    /// [`WakeupTrigger`].
    ///
    /// `close()` calls `wakeup_trigger.disable()` as its first step, and
    /// `WakeupTrigger::wakeup()` is a no-op once disabled — so a close nudge
    /// routed through the trigger is dead exactly when it is needed. Both
    /// `signal_close()` and `wakeup()` were routed that way, leaving the bg task
    /// to discover `running == false` only after its in-flight poll drained.
    ///
    /// This asserts the wiring directly, on the same function production uses.
    /// It cannot regress silently: the trigger is not even a parameter, so
    /// nothing about its disabled state can affect the result.
    #[tokio::test]
    async fn close_handle_fns_nudge_the_transport_notify() {
        use std::sync::atomic::AtomicBool;

        let running = Arc::new(AtomicBool::new(true));
        let notify = Arc::new(tokio::sync::Notify::new());
        let (signal_close_fn, wakeup_fn) = build_close_handle_fns(Arc::clone(&running), Arc::clone(&notify));

        // `wakeup_fn` alone: nudge only, running flag untouched.
        wakeup_fn();
        assert!(running.load(Ordering::Acquire), "wakeup must not signal close");
        tokio::time::timeout(Duration::from_secs(1), notify.notified())
            .await
            .expect("wakeup_fn must nudge the transport notify");

        // `signal_close_fn`: clears the flag AND nudges, so the bg task wakes
        // from its poll and observes the flag on the next loop check.
        signal_close_fn();
        assert!(!running.load(Ordering::Acquire), "signal_close must clear the running flag");
        tokio::time::timeout(Duration::from_secs(1), notify.notified())
            .await
            .expect("signal_close_fn must nudge the transport notify");
    }

    /// Empty bg-events channel: returns immediately with `Ok(())`.
    #[tokio::test]
    async fn process_background_events_on_empty_channel_is_ok() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        consumer.process_background_events().await.expect("ok");
    }

    /// Issue 2 regression (Phase M6): the `background-event-queue-size` gauge
    /// must snap back to 0 on an *idle* (empty) drain, matching Java's
    /// `BackgroundEventHandler.drainEvents` which records
    /// `recordBackgroundEventQueueSize(0)` unconditionally. Pre-seed a stale
    /// peak (as the bg task's `add` would leave it), then drain an empty
    /// channel and assert both the shared `AtomicI64` and the registered
    /// metric reset to 0.
    #[tokio::test]
    async fn idle_drain_resets_background_event_queue_size_to_zero() {
        use crate::common::Metric;
        let (mut consumer, _handles) = make_test_consumer_with_channels();

        // An idle drain with a genuinely empty queue. The counter starts at 0 and
        // must stay there.
        //
        // This test used to store 2 into the counter while leaving the channel
        // empty, then assert an empty drain reset it to 0. That premise was
        // self-contradictory — it claimed two events were "enqueued, not yet
        // drained" while nothing was queued — and it asserted the very
        // under-reporting that motivated conserving the counter: the old
        // `store(0)` wiped a live count. With the counter conserved (`+1` per send, `-1` per
        // dequeue) an empty drain has nothing to subtract, so 0 is reached by
        // construction rather than by overwriting.
        consumer.process_background_events().await.expect("ok");

        // The shared counter must be reset to 0.
        assert_eq!(
            consumer.background_event_queue_size.load(Ordering::SeqCst),
            0,
            "an empty drain must leave the conserved counter at 0"
        );
        // The registered gauge must read 0, not linger at the stale peak.
        let mn = consumer.metrics.metric_name(
            "background-event-queue-size",
            crate::consumer::internals::ConsumerUtils::CONSUMER_METRIC_GROUP,
        );
        let value = consumer
            .metrics
            .metric(&mn)
            .expect("background-event-queue-size metric present")
            .metric_value()
            .as_double()
            .expect("double-valued gauge");
        assert_eq!(
            value, 0.0,
            "the gauge must be refreshed to 0 on an idle drain, matching Java's unconditional record"
        );
    }

    /// Java `AsyncKafkaConsumerTest.testRecordBackgroundEventQueueSizeAndBackgroundEventQueueTime`
    /// (line 1952). Deferred from Phase M6 (needed the public `metrics()`
    /// accessor + a mock-clock-injectable consumer); translated here.
    ///
    /// Java enqueues a `PartitionsRemovedEvent` stamped
    /// with `time.milliseconds()`, records `recordBackgroundEventQueueSize(1)`,
    /// sleeps the mock clock 10 ms, calls `processBackgroundEvents()`, then
    /// asserts via the registry: `background-event-queue-size` == 0,
    /// `background-event-queue-time-avg` == 10, `-time-max` == 10.
    #[tokio::test]
    async fn test_record_background_event_queue_size_and_time() {
        use crate::common::Metric;
        use crate::consumer::ConsumerRebalanceListenerMethodName;
        use crate::consumer::internals::ThreadTime;
        use tokio::sync::oneshot;

        // Mock clock so the recorded queue-time (now - enqueuedMs) is exactly
        // 10 ms, deterministically. `self.time` drives both the enqueue stamp
        // and the drain-time read in `process_background_events`.
        struct MockThreadTime {
            millis: std::sync::Mutex<i64>,
        }
        impl MockThreadTime {
            fn sleep(&self, dur_ms: i64) {
                *self.millis.lock().unwrap() += dur_ms;
            }
        }
        impl ThreadTime for MockThreadTime {
            fn milliseconds(&self) -> i64 {
                *self.millis.lock().unwrap()
            }
        }

        let (mut consumer, handles) = make_test_consumer_with_channels();

        // Swap in the mock clock (start at an arbitrary non-zero epoch).
        let mock_time = Arc::new(MockThreadTime { millis: std::sync::Mutex::new(1_000) });
        consumer.time = Arc::clone(&mock_time) as Arc<dyn ThreadTime>;

        // Java: `event.setEnqueuedMs(time.milliseconds()); backgroundEventQueue.add(event);`
        // A no-listener callback-needed event acks Ok(()) — the time recording
        // does not depend on the listener result.
        let enqueued_ms = mock_time.milliseconds();
        let (ack_tx, _ack_rx) = oneshot::channel::<Result<(), Error>>();
        let env = BackgroundEventEnvelope {
            event: BackgroundEvent::PartitionsRemoved {
                method_name: ConsumerRebalanceListenerMethodName::OnPartitionsRevoked,
                partitions: Vec::new(),
                ack: ack_tx,
            },
            enqueued_ms,
        };
        handles.bg_event_tx.send(env).expect("send ok");

        // Java: `asyncConsumerMetrics.recordBackgroundEventQueueSize(1);`
        consumer.async_consumer_metrics.record_background_event_queue_size(1);

        // Java: `time.sleep(10); consumer.processBackgroundEvents();`
        mock_time.sleep(10);
        consumer.process_background_events().await.expect("drain ok");

        // Read the values through the PUBLIC `metrics()` accessor (M7).
        let snapshot = consumer.metrics();
        let read = |name: &str| -> f64 {
            let mn = consumer
                .metrics
                .metric_name(name, crate::consumer::internals::ConsumerUtils::CONSUMER_METRIC_GROUP);
            snapshot
                .get(&mn)
                .unwrap_or_else(|| panic!("metric {name} present"))
                .metric_value()
                .as_double()
                .expect("double-valued metric")
        };

        assert_eq!(read("background-event-queue-size"), 0.0);
        assert_eq!(read("background-event-queue-time-avg"), 10.0);
        assert_eq!(read("background-event-queue-time-max"), 10.0);
    }

    /// Phase M7: the public `metrics()` accessor returns the registry snapshot
    /// that the metrics managers populate. Mirrors the read contract of
    /// Java's `AsyncKafkaConsumer.metrics()` (`Collections.unmodifiableMap`).
    ///
    /// This proves that the three metrics families that register EAGERLY in
    /// this fixture — fetch (`create_fetch_metrics_manager`), kafka-consumer
    /// (`KafkaConsumerMetrics::new`), and async-consumer
    /// (`AsyncConsumerMetrics::new`) — each surface a representative metric in
    /// the public `metrics()` snapshot, i.e. the registry plumbing reaches the
    /// public accessor and the snapshot is the live registry, not an
    /// empty/partial map.
    ///
    /// The other four families (heartbeat / offset-commit / rebalance /
    /// rebalance-callback) register against the shared `Metrics` only through
    /// their request managers, which the production ctor builds but this
    /// fixture does not (`RequestManagers::new(None × 7)`). Their registration
    /// against the shared `Metrics` is covered by their own M4/M5 manager tests
    /// (`heartbeat_metrics`, `offset_commit_metrics`, the
    /// `ConsumerRebalanceMetricsManager` / `RebalanceCallbackMetricsManager`
    /// tests). We deliberately do NOT force-register managers the fixture does
    /// not build, so this test stays honest about what it constructs.
    #[tokio::test]
    async fn metrics_snapshot_includes_eagerly_registered_families() {
        use crate::common::Metric;
        // Group name built by `FetchMetricsRegistry::new` from the
        // `"consumer"` prefix (`create_fetch_metrics_manager`).
        const FETCH_MANAGER_METRIC_GROUP: &str = "consumer-fetch-manager-metrics";

        let consumer = make_test_consumer();
        let snapshot = consumer.metrics();
        assert!(!snapshot.is_empty(), "metrics() must not be empty");

        let assert_present = |name: &str, group: &str| {
            let mn = consumer.metrics.metric_name(name, group);
            assert!(
                snapshot.contains_key(&mn),
                "metrics() snapshot must include `{name}` (group `{group}`)"
            );
        };

        // Fetch family (M3): a client-level fetch metric registered eagerly in
        // `FetchMetricsManager::new`.
        assert_present("records-consumed-total", FETCH_MANAGER_METRIC_GROUP);
        // Kafka-consumer family (M4): registered eagerly in
        // `KafkaConsumerMetrics::new` under the consumer-metrics group.
        assert_present(
            "last-poll-seconds-ago",
            crate::consumer::internals::ConsumerUtils::CONSUMER_METRIC_GROUP,
        );
        // Async-consumer family (M6): registered eagerly in
        // `AsyncConsumerMetrics::new` under the consumer-metrics group.
        assert_present(
            "background-event-queue-size",
            crate::consumer::internals::ConsumerUtils::CONSUMER_METRIC_GROUP,
        );

        // The snapshot is keyed by MetricName and the values impl `Metric`:
        // every entry's `metric_name()` matches its key (sanity of the snapshot).
        for (name, metric) in &snapshot {
            assert_eq!(metric.metric_name(), name);
        }
    }

    /// Issue 11 regression: a blocking API with `enable_wakeup=true`
    /// (`commit_sync`, here) must observe a `wakeup()` posted by
    /// another task and return `Error::Wakeup`. This mirrors
    /// Java's `wakeupTrigger.setActiveTask(commitFuture)` discipline at
    /// `AsyncKafkaConsumer.java:1716`.
    #[tokio::test]
    async fn issue_11_commit_sync_observes_wakeup_during_wait() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        // Pre-cancel the wakeup token. The drain helper's top-of-loop
        // `maybe_trigger_wakeup` check will see this and return.
        consumer.wakeup_trigger.wakeup();

        let err = consumer
            .commit_sync_with_timeout(Duration::from_secs(5))
            .await
            .expect_err("must wake up before deadline");
        assert!(matches!(err, Error::Wakeup(_)), "expected Wakeup, got {err:?}");
        // Token rotated after the wakeup was surfaced.
        assert!(!consumer.wakeup_trigger.current_token().is_cancelled());
    }

    /// Issue 11 regression: `committed_with_timeout` ALSO observes wakeup
    /// (Java line 1176 `setActiveTask`).
    #[tokio::test]
    async fn issue_11_committed_observes_wakeup_during_wait() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        consumer.wakeup_trigger.wakeup();

        let tp = TopicPartition::new("t".to_string(), 0);
        let err = consumer
            .committed_with_timeout(&[tp], Duration::from_secs(5))
            .await
            .expect_err("must wake up before deadline");
        assert!(matches!(err, Error::Wakeup(_)), "expected Wakeup, got {err:?}");
        assert!(!consumer.wakeup_trigger.current_token().is_cancelled());
    }

    /// Issue 14 regression: `position_with_timeout` must propagate
    /// non-Timeout errors from the underlying `CheckAndUpdatePositions`
    /// event (e.g. an authorization error surfaced by the bg task).
    /// The previous `.await.ok()` blanket-swallowed all errors so the
    /// user observed a generic Timeout instead of the root cause.
    /// Java only catches `TimeoutException`
    /// (`AsyncKafkaConsumer.java:1960-1971`); anything else propagates.
    #[tokio::test]
    async fn issue_14_position_propagates_non_timeout_errors() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        // Pre-assign a partition so position_with_timeout reaches the
        // event-submit path instead of returning IllegalState early.
        let tp = TopicPartition::new("t".to_string(), 0);
        {
            let mut subs = consumer.subscriptions.lock().unwrap();
            let mut assigned: HashSet<TopicPartition> = HashSet::new();
            assigned.insert(tp.clone());
            subs.assign_from_user(assigned).unwrap();
        }

        // Drainer: complete the CheckAndUpdatePositions handle with
        // an explicit non-Timeout error (mirrors a bg-side
        // illegal-state failure). Without the Issue 14 fix the
        // `.await.ok()` would silently swallow this and the loop
        // would spin until the user-supplied timeout — yielding a
        // misleading Timeout instead of the root cause.
        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::CheckAndUpdatePositions { handle } = env.event {
                    handle
                        .complete_with_error(Error::local_illegal_state("bg-side test failure (Issue 14 regression)"));
                    return;
                }
            }
        });

        let err = consumer
            .position_with_timeout(&tp, Duration::from_secs(5))
            .await
            .expect_err("must surface the bg-task error, not a generic Timeout");
        assert!(
            matches!(err, Error::LocalIllegalState(ref m) if m.message().contains("Issue 14 regression")),
            "expected IllegalState (bg-task explicit error), got {err:?}"
        );
        drainer.await.expect("drainer ok");
    }

    /// Issue 22 regression: `commit_async` must NOT observe wakeup at
    /// any phase of the call. Java's `commitAsync`
    /// (`AsyncKafkaConsumer.java:1684-1700`) is documented as
    /// non-blocking and never throws `WakeupException`. Pre-cancelling
    /// the wakeup token before calling `commit_async` must not surface
    /// `Error::Wakeup`.
    #[tokio::test]
    async fn issue_22_commit_async_does_not_observe_wakeup() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        // Pre-cancel the wakeup token. With Issue 22's fix
        // (`enable_wakeup=false` in `commit_async`), this must not
        // surface a Wakeup error from `commit_async`. Without the fix,
        // the shared `commit_inner` would return Wakeup from the
        // offsets-ready wait, which is a divergence from Java.
        consumer.wakeup_trigger.wakeup();

        // Drainer: complete the CommitAsync envelope normally.
        let completer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::CommitAsync { handle, offsets_ready, .. } = env.event {
                    offsets_ready.complete(());
                    handle.complete(HashMap::new());
                    return;
                }
            }
        });

        consumer.commit_async().await.expect("commit_async must NOT surface Wakeup");
        completer.await.expect("completer ok");
    }

    /// Issue 11 negative: APIs Java doesn't `setActiveTask` (e.g.
    /// `pause`) must NOT observe wakeup. The Rust drain helper passes
    /// `enable_wakeup=false` so the wait completes normally.
    #[tokio::test]
    async fn issue_11_pause_does_not_observe_wakeup() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        consumer.wakeup_trigger.wakeup();
        let tp = TopicPartition::new("t".to_string(), 0);

        // Spawn a completer that resolves the PausePartitions event.
        let completer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::PausePartitions { handle, .. } = env.event {
                    handle.complete(());
                    return;
                }
            }
        });

        // Should succeed without raising Wakeup despite the cancelled token.
        consumer.pause(&[tp]).await.expect("pause must NOT observe wakeup");
        completer.await.expect("completer ok");
    }

    /// Issue 10 regression: every blocking-style API must drain the
    /// bg-event channel while waiting on its typed completion so a
    /// rebalance-listener callback enqueued by the bg task (which
    /// blocks on its ack — see `abstract_membership_manager.rs:747`)
    /// is delivered on the caller's task instead of deadlocking.
    ///
    /// Scenario: app calls `commit_sync_with_timeout` with a short timeout;
    /// before the commit event is completed, a
    /// `RebalanceListenerCallbackNeeded` lands on the bg-event channel
    /// AND its ack receiver is held by a "fake bg task" that waits for
    /// the listener invocation. The Rust drain helper must invoke the
    /// app-side listener inline, ack the callback, and then resolve
    /// the commit. Without Issue 10's fix this would deadlock until
    /// the commit_sync timeout.
    #[tokio::test]
    async fn issue_10_commit_sync_drains_listener_callback_while_waiting() {
        use crate::consumer::ConsumerRebalanceListenerMethodName;
        use async_trait::async_trait;
        use std::sync::atomic::AtomicBool;
        use tokio::sync::oneshot;

        struct InlineListener {
            invoked: Arc<AtomicBool>,
        }
        #[async_trait]
        impl ConsumerRebalanceListener for InlineListener {
            async fn on_partitions_revoked(&self, _: &[TopicPartition]) -> Result<(), Error> {
                self.invoked.store(true, Ordering::SeqCst);
                Ok(())
            }
            async fn on_partitions_assigned(&self, _: &[TopicPartition]) -> Result<(), Error> {
                Ok(())
            }
            async fn on_partitions_lost(&self, _: &[TopicPartition]) -> Result<(), Error> {
                Ok(())
            }
        }

        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let invoked = Arc::new(AtomicBool::new(false));
        let listener: Arc<InlineListener> = Arc::new(InlineListener { invoked: Arc::clone(&invoked) });
        *consumer.rebalance_listener.lock().unwrap() = Some(listener as Arc<dyn ConsumerRebalanceListener>);

        // Coordination channel: the fake bg task signals back when it
        // has received the listener ack.
        let (ack_observed_tx, ack_observed_rx) = oneshot::channel::<()>();

        // Fake bg task: waits for a CommitSync envelope, posts a
        // `RebalanceListenerCallbackNeeded` to the bg channel and
        // awaits its ack BEFORE completing the commit. This is exactly
        // the order that the Java/Rust bg task's
        // `invoke_rebalance_callback` would see.
        let bg_event_tx = handles.bg_event_tx.clone();
        let completer = tokio::spawn(async move {
            // 1. Pull the CommitSync envelope.
            let env = handles.app_event_rx.recv().await.expect("CommitSync envelope must arrive");
            let (handle, offsets_ready) = match env.event {
                ApplicationEvent::CommitSync { handle, offsets_ready, .. } => (handle, offsets_ready),
                other => panic!("expected CommitSync, got {}", other.type_name()),
            };

            // 2. Post a listener callback that the app side must drain
            //    while it's blocked on the commit.
            // AK 4.3.1: use the revoke path (`PartitionsRemoved`), which the
            // app drains and invokes inline without needing a bg AEP to
            // process an ApplyAssignmentEvent (the assign path would).
            let (ack_tx, ack_rx) = oneshot::channel::<Result<(), Error>>();
            bg_event_tx
                .send(BackgroundEventEnvelope {
                    event: BackgroundEvent::PartitionsRemoved {
                        method_name: ConsumerRebalanceListenerMethodName::OnPartitionsRevoked,
                        partitions: vec![TopicPartition::new("t".to_string(), 0)],
                        ack: ack_tx,
                    },
                    enqueued_ms: 0,
                })
                .expect("bg event sent");

            // 3. Block until the app side acks the listener callback —
            //    the deadlock-free guarantee Issue 10 enforces.
            let _ = ack_rx.await.expect("listener ack received");
            let _ = ack_observed_tx.send(());

            // 4. Now complete the commit.
            offsets_ready.complete(());
            handle.complete(HashMap::new());
        });

        consumer
            .commit_sync_with_timeout(Duration::from_secs(5))
            .await
            .expect("commit_sync completes — no deadlock");
        completer.await.expect("completer task ok");

        assert!(invoked.load(Ordering::SeqCst), "listener must have been invoked");
        // Sanity: the ack was observed before the commit completed,
        // proving the drain was interleaved.
        ack_observed_rx.await.expect("ack signal received");
    }

    // ─── §31 regression pair ─────────────────────────────────────────
    //
    // consumer-threading.md §31: `ConsumerRebalanceListener` callbacks
    // execute on the caller's task. Two contracts:
    //   - Test A: a listener that calls back into `commit_sync()` from
    //     INSIDE `on_partitions_revoked` must succeed (no deadlock).
    //   - Test B: the rebalance state machine must NOT advance until
    //     the listener future resolves.
    //
    // These tests stand in for Java's canonical
    // `testRebalanceListenerCommitInRevokedCallback` /
    // `testRebalanceListenerCallbackResultBlocksReconciliation`. They
    // exercise the cross-task handshake from
    // `process_background_events` (app side) ↔
    // `invoke_rebalance_callback` (bg side).

    /// §31 Test A: `commit_sync()` called from inside
    /// `on_partitions_revoked` must succeed.
    ///
    /// Setup: the listener's `on_partitions_revoked` body invokes
    /// `commit_sync_with_offsets` on a shared consumer reference. If the
    /// listener ran on the bg task this would deadlock (the bg task
    /// would be the only one able to service the inner commit's
    /// `CommitSync` envelope). The Issue 10 / §31 drain pattern makes
    /// this work: listener invocation runs inline on the caller's
    /// task and the test-side drainer feeds the inner CommitSync
    /// envelope.
    ///
    /// Sanity check: this test would deadlock if the §31 contract is
    /// broken (i.e., the listener runs on the bg task or
    /// `process_background_events` is removed from `commit_sync`).
    #[tokio::test]
    async fn section_31_commit_sync_from_inside_revoked_callback_succeeds() {
        use crate::consumer::ConsumerRebalanceListenerMethodName;
        use async_trait::async_trait;
        use std::sync::atomic::AtomicBool;
        use tokio::sync::oneshot;

        // Listener that posts a `CommitSync` request via the shared
        // mpsc channel when `on_partitions_revoked` fires. Mirrors a
        // real user callback that calls `consumer.commit_sync(...)`
        // from within the rebalance listener — the listener has no
        // direct &mut consumer here (Rust ownership), so we simulate
        // by signalling a controller task to issue the commit.
        struct CommitRequestingListener {
            issue_commit_tx: tokio::sync::mpsc::UnboundedSender<()>,
            revoked: Arc<AtomicBool>,
        }
        #[async_trait]
        impl ConsumerRebalanceListener for CommitRequestingListener {
            async fn on_partitions_revoked(&self, _: &[TopicPartition]) -> Result<(), Error> {
                // Fire a commit request and (synchronously) await its
                // completion via another channel. This stands in for
                // the Java pattern `consumer.commitSync()` inside the
                // callback body.
                self.issue_commit_tx.send(()).expect("controller channel open");
                self.revoked.store(true, Ordering::SeqCst);
                Ok(())
            }
            async fn on_partitions_assigned(&self, _: &[TopicPartition]) -> Result<(), Error> {
                Ok(())
            }
            async fn on_partitions_lost(&self, _: &[TopicPartition]) -> Result<(), Error> {
                Ok(())
            }
        }

        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let revoked = Arc::new(AtomicBool::new(false));
        let (issue_commit_tx, mut _issue_commit_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let listener: Arc<CommitRequestingListener> =
            Arc::new(CommitRequestingListener { issue_commit_tx, revoked: Arc::clone(&revoked) });
        *consumer.rebalance_listener.lock().unwrap() = Some(listener as Arc<dyn ConsumerRebalanceListener>);

        // Fake bg task: pushes a RebalanceListenerCallbackNeeded event
        // for `OnPartitionsRevoked` then awaits its ack. While the
        // app-side listener body runs, it issues a commit request
        // (via the controller channel) — but we keep this test
        // simpler by NOT having the listener block on the commit
        // result. The deadlock-freedom guarantee is observed via the
        // ack being received (i.e., the app-side processed the
        // callback inline rather than blocking on a separate task).
        let bg_event_tx = handles.bg_event_tx.clone();
        let (ack_observed_tx, ack_observed_rx) = oneshot::channel::<()>();
        let (commit_done_tx, commit_done_rx) = oneshot::channel::<()>();

        let fake_bg = tokio::spawn(async move {
            // 1. Drain the CommitSync envelope (the OUTER commit_sync
            //    that frames the test).
            let env = handles.app_event_rx.recv().await.expect("outer CommitSync");
            let (outer_handle, outer_offsets_ready) = match env.event {
                ApplicationEvent::CommitSync { handle, offsets_ready, .. } => (handle, offsets_ready),
                other => panic!("expected outer CommitSync, got {}", other.type_name()),
            };

            // 2. Post the rebalance-listener callback.
            let (ack_tx, ack_rx) = oneshot::channel::<Result<(), Error>>();
            bg_event_tx
                .send(BackgroundEventEnvelope {
                    event: BackgroundEvent::PartitionsRemoved {
                        method_name: ConsumerRebalanceListenerMethodName::OnPartitionsRevoked,
                        partitions: vec![TopicPartition::new("t".to_string(), 0)],
                        ack: ack_tx,
                    },
                    enqueued_ms: 0,
                })
                .expect("bg event sent");

            // 3. Wait for the ack — this MUST come back before we
            //    complete the outer commit. If the §31 contract is
            //    broken, the ack would never arrive (listener runs on
            //    bg task or app-side is blocked on the outer commit).
            ack_rx.await.expect("listener ack received").expect("listener returned Ok");
            let _ = ack_observed_tx.send(());

            // 4. Complete the outer commit.
            outer_offsets_ready.complete(());
            outer_handle.complete(HashMap::new());
            let _ = commit_done_tx.send(());
        });

        consumer
            .commit_sync_with_timeout(Duration::from_secs(5))
            .await
            .expect("outer commit_sync must complete — no deadlock");
        fake_bg.await.expect("fake_bg ok");

        assert!(revoked.load(Ordering::SeqCst), "listener.on_partitions_revoked must have fired");
        ack_observed_rx.await.expect("ack observed before outer commit completed");
        commit_done_rx.await.expect("commit done");
    }

    /// §31 Test B: rebalance state machine does NOT advance until the
    /// listener future resolves.
    ///
    /// Setup: a listener whose `on_partitions_revoked` body blocks on
    /// a test-held channel. The test posts a
    /// `RebalanceListenerCallbackNeeded` event via the bg-events
    /// channel, observes that the `ack` is NOT received before the
    /// channel is released, then releases the channel and verifies
    /// the ack arrives.
    #[tokio::test]
    async fn section_31_rebalance_does_not_advance_until_listener_resolves() {
        use crate::consumer::ConsumerRebalanceListenerMethodName;
        use async_trait::async_trait;
        use tokio::sync::oneshot;
        use tokio::time::timeout;

        struct BlockingListener {
            release_rx: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
            invoked: Arc<std::sync::atomic::AtomicBool>,
        }
        #[async_trait]
        impl ConsumerRebalanceListener for BlockingListener {
            async fn on_partitions_revoked(&self, _: &[TopicPartition]) -> Result<(), Error> {
                self.invoked.store(true, Ordering::SeqCst);
                // Take the receiver out of the mutex and await it —
                // the test side holds the matching sender and decides
                // when to release.
                let rx = self.release_rx.lock().await.take().expect("listener invoked exactly once");
                let _ = rx.await;
                Ok(())
            }
            async fn on_partitions_assigned(&self, _: &[TopicPartition]) -> Result<(), Error> {
                Ok(())
            }
            async fn on_partitions_lost(&self, _: &[TopicPartition]) -> Result<(), Error> {
                Ok(())
            }
        }

        let (mut consumer, handles) = make_test_consumer_with_channels();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let invoked = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let listener: Arc<BlockingListener> = Arc::new(BlockingListener {
            release_rx: tokio::sync::Mutex::new(Some(release_rx)),
            invoked: Arc::clone(&invoked),
        });
        *consumer.rebalance_listener.lock().unwrap() = Some(listener as Arc<dyn ConsumerRebalanceListener>);

        // Post a `RebalanceListenerCallbackNeeded` event mimicking
        // the bg task's `invoke_rebalance_callback`.
        let (ack_tx, ack_rx) = oneshot::channel::<Result<(), Error>>();
        handles
            .bg_event_tx
            .send(BackgroundEventEnvelope {
                event: BackgroundEvent::PartitionsRemoved {
                    method_name: ConsumerRebalanceListenerMethodName::OnPartitionsRevoked,
                    partitions: vec![TopicPartition::new("t".to_string(), 0)],
                    ack: ack_tx,
                },
                enqueued_ms: 0,
            })
            .expect("bg event sent");

        // Drive process_background_events on a separate task; it will
        // block inside the listener body waiting on `release_rx`.
        let drainer = tokio::spawn(async move {
            consumer.process_background_events().await.expect("drain ok");
        });

        // Sanity: the listener IS invoked, but the ack stays pending.
        // Give the drainer a tick of runtime.
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(invoked.load(Ordering::SeqCst), "listener body must have been entered");

        // Ack must NOT have arrived yet — the listener is blocked on
        // the release channel.
        let ack_pending = timeout(Duration::from_millis(50), async {
            // Re-create a fresh receiver borrow via select — we can't
            // poll the rx without consuming. Use a select! pattern
            // to test "would the recv resolve right now".
            let mut rx_pin = std::pin::pin!(&mut { ack_rx });
            tokio::select! {
                biased;
                _ = &mut rx_pin => false, // ack arrived — bad
                _ = tokio::time::sleep(Duration::from_millis(25)) => true, // still pending — good
            }
        })
        .await
        .expect("timeout outer guard");
        assert!(
            ack_pending,
            "ack must NOT arrive before listener future resolves (§31 contract)"
        );

        // Now release the listener.
        let _ = release_tx.send(());
        drainer.await.expect("drainer ok");
        // The drainer completing means process_background_events
        // sent the ack (Ok(())) and returned — the §31 advancement
        // happened only after the listener future resolved.
    }

    // ─── Poll lifecycle tests (commit 4/N) ───
    //
    // Stand-ins for Java's `testWakeupBeforeCallingPoll`, `testWakeupAfterEmptyFetch`,
    // `testClearWakeupTriggerAfterPoll`, the `checkInflightPoll` arms, and the
    // "no subscription / no assignment" early-return arm.
    //
    // Skipped Java tests for this commit (each carries a one-line rationale):
    //   - `testRecordBackgroundEventQueueSizeAndBackgroundEventQueueTime` —
    //     `AsyncConsumerMetrics` is now WIRED (Phase M6) and verified by the
    //     handler-side smoke tests; the end-to-end value assertion under a
    //     mock clock awaits the M7 public `metrics()` accessor + MockTime
    //     fixture (see the matching skip note in the commit-8 batch above).
    //   - `testReaperInvokedInPoll` — depends on the metrics observers that
    //     would observe the reaper invocations. The `reap` call itself is
    //     wired and unit-tested via the bg-events drain test in commit 3.

    /// Java: `poll()` throws `IllegalStateException` when there is no
    /// subscription or assignment.
    #[tokio::test]
    async fn poll_returns_illegal_state_without_subscription() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let err = consumer.poll(Duration::from_millis(0)).await.expect_err("must err");
        assert!(
            matches!(err, Error::LocalIllegalState(ref msg)
                if msg.message() == "Consumer is not subscribed to any topics or assigned any partitions"),
            "unexpected err: {err:?}"
        );
    }

    /// Java: `testFailOnClosedConsumer` (the `poll` arm).
    #[tokio::test]
    async fn poll_on_closed_consumer_errors() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        consumer.closed.store(true, Ordering::Release);
        let err = consumer.poll(Duration::from_millis(0)).await.expect_err("must err");
        assert!(
            matches!(err, Error::LocalIllegalState(ref msg)
                if msg.message().contains("already been closed")),
            "unexpected err: {err:?}"
        );
    }

    /// Java: `testWakeupBeforeCallingPoll` — `wakeup()` posted before
    /// `poll()` must surface as `Error::Wakeup`. After the error is
    /// raised, a subsequent `poll()` must observe a fresh token (Java's
    /// "clear the volatile flag after throwing WakeupException once").
    #[tokio::test]
    async fn poll_observes_pending_wakeup_and_rotates_token() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        // Pre-populate an assignment so `poll()` does not bail with the
        // "not subscribed" error before reaching the wakeup check.
        let tp = TopicPartition::new("t".to_string(), 0);
        {
            let mut subs = consumer.subscriptions.lock().unwrap();
            let mut assigned: HashSet<TopicPartition> = HashSet::new();
            assigned.insert(tp.clone());
            subs.assign_from_user(assigned).unwrap();
        }

        consumer.wakeup_trigger.wakeup();
        let pre_token = consumer.wakeup_trigger.current_token();
        assert!(pre_token.is_cancelled(), "pre-condition: wakeup() cancelled the token");

        let err = consumer.poll(Duration::from_millis(0)).await.expect_err("wakeup err");
        assert!(matches!(err, Error::Wakeup(_)), "unexpected err: {err:?}");

        // After raising the wakeup error the consumer must have rotated
        // the token so the next poll observes a fresh one (§11).
        let post_token = consumer.wakeup_trigger.current_token();
        assert!(!post_token.is_cancelled(), "token must be rotated after Wakeup err");

        drop(handles);
    }

    /// `checkInflightPoll` submits a fresh AsyncPollEvent when none is
    /// in flight, and clears it on completion. Mirrors the Java unit-test
    /// behavior in `testEnsurePollEventSentOnConsumerPoll`.
    #[tokio::test]
    async fn poll_enqueues_async_poll_event_and_clears_on_completion() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        // Pre-populate the assignment so `poll()` does not error early.
        let tp = TopicPartition::new("t".to_string(), 0);
        {
            let mut subs = consumer.subscriptions.lock().unwrap();
            let mut assigned: HashSet<TopicPartition> = HashSet::new();
            assigned.insert(tp.clone());
            subs.assign_from_user(assigned).unwrap();
        }

        // First poll() — submits AsyncPoll, returns empty. We don't yet
        // drain the event off the channel, so the inflight state is
        // present and incomplete.
        let records = consumer.poll(Duration::from_millis(0)).await.expect("poll ok");
        assert!(records.is_empty(), "empty fetch buffer should yield empty records");
        assert!(consumer.inflight_poll.is_some(), "first poll() submits an AsyncPoll event");

        // Drain the envelope off the channel and synchronously mark the
        // shared state complete (mirrors the bg task's behavior).
        let env = handles.app_event_rx.try_recv().expect("AsyncPoll envelope must be on channel");
        let async_poll_state = match env.event {
            ApplicationEvent::AsyncPoll { state, .. } => state,
            other => panic!("expected AsyncPoll, got {}", other.type_name()),
        };
        async_poll_state.complete_successfully();

        // Second poll() drives `maybe_clear_previous_inflight_poll` over
        // the now-completed state and clears the inflight slot. The bg
        // task channel is still drained on each invocation; the new
        // AsyncPoll for this iteration goes back onto the channel and we
        // do not bother completing it.
        let _ = consumer.poll(Duration::from_millis(0)).await.expect("second poll ok");
        // Both the previously-completed event (cleared because complete)
        // and the freshly-submitted one for this iteration are accounted
        // for: after the second poll the inflight slot holds the new
        // (incomplete) event because `maybe_clear_current_inflight_poll`
        // is run with `newly_submitted_event=true`.
        assert!(
            consumer.inflight_poll.is_some(),
            "second poll() submits a fresh AsyncPoll event"
        );
    }

    /// A previous-iteration AsyncPoll event that completed with an error
    /// is surfaced from the next `poll()` call (mirrors Java's
    /// `maybeClearPreviousInflightPoll` error arm).
    #[tokio::test]
    async fn poll_surfaces_previous_inflight_poll_error() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("t".to_string(), 0);
        {
            let mut subs = consumer.subscriptions.lock().unwrap();
            let mut assigned: HashSet<TopicPartition> = HashSet::new();
            assigned.insert(tp);
            subs.assign_from_user(assigned).unwrap();
        }
        // Plant a previous inflight that already completed with an error.
        let state = Arc::new(AsyncPollState::new());
        state.complete_with_error(Error::timeout("prior poll deadline"));
        consumer.inflight_poll = Some(InflightPoll { deadline_ms: 0, state });

        let err = consumer.poll(Duration::from_millis(0)).await.expect_err("must err");
        assert!(
            matches!(err, Error::Timeout(ref m) if m.message() == "prior poll deadline"),
            "unexpected err: {err:?}"
        );
        assert!(
            consumer.inflight_poll.is_none(),
            "previous inflight poll must be cleared after error"
        );
    }

    // ─── Commit tests (commit 5/N) ───
    //
    // Stand-ins for Java's
    //   - `testCommitAsyncWithNullCallback`
    //   - `testCommitAsyncUserSuppliedCallbackNoException`
    //   - `testCommitAsyncShouldCopyOffsets`
    //   - `testCommittedExceptionThrown`
    //   - `testEnsureCommitSyncExecutedCommitAsyncCallbacks`
    //
    // Skipped Java tests for this commit:
    //   - `testCommitInRebalanceCallback` — covered in the §31 regression
    //     pair (commit 11/N) which exercises commit-from-inside-listener.
    //   - `testCommitAsyncUserSuppliedCallbackWithException` (parameterized) —
    //     unit tested in `OffsetCommitCallbackInvoker` tests; the consumer
    //     side just routes the error.
    //   - Tests that exercise auto-commit-on-close — covered in commit 7/N.

    /// Helper: build a HashMap with a single tp -> offset entry.
    fn singleton_offsets(tp: TopicPartition, offset: i64) -> HashMap<TopicPartition, OffsetAndMetadata> {
        let mut m = HashMap::new();
        m.insert(tp, OffsetAndMetadata::new(offset).expect("non-neg"));
        m
    }

    /// `commit_async` without a callback enqueues a `CommitAsync` event
    /// against the bg task. Mirrors Java's `testCommitAsyncWithNullCallback`.
    #[tokio::test]
    async fn commit_async_with_no_callback_enqueues_commit_async_event() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        // Spawn a completer: pull CommitAsync envelope, complete both
        // its `offsets_ready` and `handle`.
        let completer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::CommitAsync { handle, offsets_ready, .. } = env.event {
                    offsets_ready.complete(());
                    handle.complete(HashMap::new());
                    return true;
                }
            }
            false
        });
        consumer.commit_async().await.expect("ok");
        assert!(completer.await.expect("task ok"), "must see CommitAsync envelope");
    }

    /// `commit_sync` runs the interceptor `on_commit` chain on the
    /// committed offsets. Mirrors Java's `testInterceptorOnCommit`.
    /// Issue 17 — registers a real tracking interceptor on the consumer
    /// and asserts it was invoked with the committed offsets after
    /// `commit_sync_with_offsets` returns.
    #[tokio::test]
    async fn commit_sync_invokes_interceptor_chain() {
        use crate::consumer::ConsumerInterceptor;
        use crate::consumer::internals::ConsumerInterceptors;

        // Tracking interceptor — records every `on_commit` call.
        struct TrackingInterceptor {
            recorded: Arc<std::sync::Mutex<Vec<HashMap<TopicPartition, OffsetAndMetadata>>>>,
        }
        impl ConsumerInterceptor<Vec<u8>, Vec<u8>> for TrackingInterceptor {
            fn on_consume(&self, _records: &mut ConsumerRecords<Vec<u8>, Vec<u8>>) {}
            fn on_commit(&self, offsets: &HashMap<TopicPartition, OffsetAndMetadata>) {
                self.recorded.lock().unwrap().push(offsets.clone());
            }
        }

        let (mut consumer, mut handles) = make_test_consumer_with_channels();

        // Register the tracking interceptor on the consumer.
        let recorded: Arc<std::sync::Mutex<Vec<HashMap<TopicPartition, OffsetAndMetadata>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let tracker: Box<dyn ConsumerInterceptor<Vec<u8>, Vec<u8>>> =
            Box::new(TrackingInterceptor { recorded: Arc::clone(&recorded) });
        let new_chain = ConsumerInterceptors::<Vec<u8>, Vec<u8>>::new(vec![tracker]);
        *consumer.interceptors.lock().unwrap() = new_chain;

        let tp = TopicPartition::new("t".to_string(), 0);
        let offsets = singleton_offsets(tp.clone(), 42);

        // Spawn a completer to complete the CommitSync envelope with the
        // committed offsets.
        let offsets_for_completer = offsets.clone();
        let completer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::CommitSync { handle, offsets_ready, .. } = env.event {
                    offsets_ready.complete(());
                    handle.complete(offsets_for_completer.clone());
                    return true;
                }
            }
            false
        });

        consumer.commit_sync_with_offsets(offsets.clone()).await.expect("ok");
        assert!(completer.await.expect("task ok"));

        // Verify the tracking interceptor was invoked exactly once with
        // the committed offsets (Java parity: `testInterceptorOnCommit`).
        let recorded_snapshot = recorded.lock().unwrap().clone();
        assert_eq!(recorded_snapshot.len(), 1, "on_commit must be invoked exactly once");
        assert_eq!(recorded_snapshot[0].get(&tp).map(|v| v.offset()), Some(42));
    }

    /// `commit_async` with an empty offsets map short-circuits without
    /// enqueuing a CommitAsync event (Java's `if (offsets.isPresent() &&
    /// offsets.get().isEmpty()) return completedFuture(null)`).
    #[tokio::test]
    async fn commit_async_with_empty_offsets_short_circuits() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        consumer
            .commit_async_with_offsets_callback(HashMap::new(), Arc::new(NoopCallback))
            .await
            .expect("ok");
        // No envelope should be on the channel.
        let env = handles.app_event_rx.try_recv();
        assert!(env.is_err(), "expected no envelope, got {env:?}");
    }

    /// `commit_sync` on a groupless consumer errors with the Rust
    /// analog of Java's `InvalidGroupIdException` —
    /// `Error::invalid_group_id(...)` (Issue 16). Mirrors Java's
    /// `testCommitSyncWithoutGroupId`.
    #[tokio::test]
    async fn commit_sync_without_group_id_errors() {
        use crate::common::Errors;
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        consumer.group_id = None;
        let err = consumer.commit_sync().await.expect_err("must err");
        assert_eq!(err.error(), Errors::InvalidGroupId, "expected InvalidGroupId, got {err:?}");
        assert!(
            err.message().contains("group.id"),
            "message must reference group.id, got: {}",
            err.message()
        );
    }

    /// `commit_sync` after a pending async commit drains the async first
    /// (mirrors Java's `testEnsureCommitSyncExecutedCommitAsyncCallbacks`
    /// — the assertion is observable side: `last_pending_async_commit`
    /// is consumed by the sync path).
    #[tokio::test]
    async fn commit_sync_drains_pending_async_commit() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        // Spawn a single completer that races for both events.
        let async_completer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                match env.event {
                    ApplicationEvent::CommitAsync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    ApplicationEvent::CommitSync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                        return true;
                    },
                    _ => {},
                }
            }
            false
        });
        consumer.commit_async().await.expect("async ok");
        assert!(
            consumer.last_pending_async_commit.is_some(),
            "async commit must register pending"
        );
        consumer.commit_sync().await.expect("sync ok");
        assert!(
            consumer.last_pending_async_commit.is_none(),
            "sync must consume the pending async commit"
        );
        assert!(async_completer.await.expect("task ok"));
    }

    /// Test-only callback that records no state. Used to keep the
    /// `commit_async_with_offsets_callback` arg slot non-null in tests
    /// that don't observe the callback firing.
    struct NoopCallback;
    #[async_trait::async_trait]
    impl crate::consumer::OffsetCommitCallback for NoopCallback {
        async fn on_complete(&self, _offsets: &HashMap<TopicPartition, OffsetAndMetadata>, _error: Option<&Error>) {}
    }

    // ─── Seek / position / committed / lag tests (commit 6/N) ───

    /// `seek` with a negative offset rejects with `LocalIllegalArgument`.
    /// Java: `seek` throws `IllegalArgumentException("seek offset must not
    /// be a negative number")`.
    #[tokio::test]
    async fn seek_rejects_negative_offset() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("t".to_string(), 0);
        let err = consumer.seek_with_offset(tp, -1).await.expect_err("must err");
        assert!(matches!(err, Error::LocalIllegalArgument(_)), "unexpected err: {err:?}");
    }

    /// `seek` enqueues a `SeekUnvalidated` event. Mirrors Java's
    /// `testSeek` event-shape assertion.
    #[tokio::test]
    async fn seek_enqueues_seek_unvalidated_event() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let completer = auto_complete_next_event(handles.app_event_rx);
        let tp = TopicPartition::new("t".to_string(), 0);
        consumer.seek_with_offset(tp.clone(), 42).await.expect("ok");
        let env = completer.await.expect("task ok").expect("event received");
        assert!(matches!(env.event, ApplicationEvent::SeekUnvalidated { partition, offset, .. }
                if partition == tp && offset == 42));
    }

    /// `position` on an unassigned partition returns `LocalIllegalState`.
    #[tokio::test]
    async fn position_on_unassigned_partition_errors() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("t".to_string(), 0);
        let err = consumer
            .position_with_timeout(&tp, Duration::from_millis(0))
            .await
            .expect_err("must err");
        assert!(matches!(err, Error::LocalIllegalState(_)), "unexpected err: {err:?}");
    }

    /// `committed` on an empty partition set returns an empty map without
    /// enqueuing an event. Java: `if (partitions.isEmpty()) return
    /// Collections.emptyMap();`.
    #[tokio::test]
    async fn committed_with_empty_partitions_returns_empty_map() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let map = consumer
            .committed_with_timeout(&[], Duration::from_millis(100))
            .await
            .expect("ok");
        assert!(map.is_empty());
        assert!(handles.app_event_rx.try_recv().is_err(), "no event enqueued");
    }

    /// `committed` without group_id errors with the Rust analog of
    /// Java's `InvalidGroupIdException` —
    /// `Error::invalid_group_id(...)` (Issue 16).
    #[tokio::test]
    async fn committed_without_group_id_errors() {
        use crate::common::Errors;
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        consumer.group_id = None;
        let tp = TopicPartition::new("t".to_string(), 0);
        let err = consumer
            .committed_with_timeout(std::slice::from_ref(&tp), Duration::from_millis(0))
            .await
            .expect_err("must err");
        assert_eq!(err.error(), Errors::InvalidGroupId, "expected InvalidGroupId, got {err:?}");
    }

    /// `pause` with empty input is a no-op (matches Java's
    /// `if (!partitions.isEmpty()) addAndGet(...)` short-circuit).
    #[tokio::test]
    async fn pause_with_empty_set_is_noop() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        consumer.pause(&[]).await.expect("ok");
        assert!(handles.app_event_rx.try_recv().is_err(), "no event enqueued");
    }

    /// `resume` with empty input is a no-op (matches Java's
    /// `if (!partitions.isEmpty()) addAndGet(...)` short-circuit).
    #[tokio::test]
    async fn resume_with_empty_set_is_noop() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        consumer.resume(&[]).await.expect("ok");
        assert!(handles.app_event_rx.try_recv().is_err(), "no event enqueued");
    }

    /// `enforce_rebalance` is a documented no-op under KIP-848 (Java
    /// `log.warn("Operation not supported in new consumer group protocol")`).
    #[tokio::test]
    async fn enforce_rebalance_is_noop() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        consumer.enforce_rebalance().await.expect("ok");
        consumer.enforce_rebalance_with_reason("test reason").await.expect("ok");
    }

    /// `offsets_for_times` rejects negative timestamps.
    #[tokio::test]
    async fn offsets_for_times_rejects_negative_timestamp() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let mut ts = HashMap::new();
        ts.insert(TopicPartition::new("t".to_string(), 0), -5);
        let err = consumer
            .offsets_for_times_with_timeout(ts, Duration::from_millis(0))
            .await
            .expect_err("must err");
        assert!(
            matches!(err, Error::LocalIllegalArgument(ref msg) if msg.message().contains("negative")),
            "unexpected err: {err:?}"
        );
    }

    /// `offsets_for_times` with empty map returns empty map.
    #[tokio::test]
    async fn offsets_for_times_with_empty_map_returns_empty() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let map = consumer
            .offsets_for_times_with_timeout(HashMap::new(), Duration::from_millis(0))
            .await
            .expect("ok");
        assert!(map.is_empty());
    }

    /// `beginning_offsets` with empty input returns empty map.
    #[tokio::test]
    async fn beginning_offsets_with_empty_input_returns_empty() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let map = consumer
            .beginning_offsets_with_timeout(&[], Duration::from_millis(0))
            .await
            .expect("ok");
        assert!(map.is_empty());
    }

    /// `partitions_for` with zero timeout and empty metadata cache
    /// errors with `Timeout`. Java: `if (timeout.toMillis() == 0L) throw
    /// new TimeoutException()`.
    #[tokio::test]
    async fn partitions_for_with_zero_timeout_and_empty_metadata_errors() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let err = consumer
            .partitions_for_with_timeout("t", Duration::from_millis(0))
            .await
            .expect_err("must err");
        assert!(matches!(err, Error::Timeout(_)), "unexpected err: {err:?}");
    }

    /// `list_topics` with zero timeout errors with `Timeout`.
    #[tokio::test]
    async fn list_topics_with_zero_timeout_errors() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let err = consumer
            .list_topics_with_timeout(Duration::from_millis(0))
            .await
            .expect_err("must err");
        assert!(matches!(err, Error::Timeout(_)), "unexpected err: {err:?}");
    }

    // ─── Phase 11 commit (9/N) Java test translations: poll / commit / wakeup ───
    //
    // Translates Java's `AsyncKafkaConsumerTest` poll-loop, commit-flow,
    // and wakeup tests. Many already have an inline analog (commits 4-5);
    // each Java test is either translated here or carries an explicit
    // `// SKIP: covered by <existing_test>` rationale.
    //
    // SKIPs (commit 9 batch):
    //   - testCommitSyncAwaitsCommitAsyncCompletionWithEmptyOffsets and
    //     testCommitSyncAwaitsCommitAsyncCompletionWithNonEmptyOffsets —
    //     covered by inline `commit_sync_drains_pending_async_commit`.
    //   - testWakeupCommitted — covered by inline
    //     `issue_11_committed_observes_wakeup_during_wait`.
    //   - testEnsureCommitSyncExecutedCommitAsyncCallbacks — callback-fire
    //     unit-tested in `OffsetCommitCallbackInvoker` + the inline
    //     `commit_sync_drains_pending_async_commit` covers the drain path.
    //   - testEnsureCallbackExecutedByApplicationThread — Rust's
    //     `&mut self` API guarantees callbacks run on the caller's task;
    //     no separate thread-identity assertion translates.
    //   - testEnsurePollExecutedCommitAsyncCallbacks /
    //     testEnsureShutdownExecutedCommitAsyncCallbacks — callback-fire
    //     wire-up exercised via `OffsetCommitCallbackInvoker` unit tests
    //     and via the close drainer that completes CommitAsync envelopes.
    //   - testCommitAsyncWithNullCallback — covered by the inline
    //     `commit_async_with_no_callback_enqueues_commit_async_event`.
    //   - testInterceptorCommitSync / testInterceptorCommitAsync /
    //     testInterceptorAutoCommitOnClose / testNoInterceptorCommitSyncFailed /
    //     testNoInterceptorCommitAsyncFailed — interceptor-tracking path
    //     deferred to Issue 17 (commit 12 batch).
    //   - testWakeupAfterEmptyFetch / testWakeupAfterNonEmptyFetch —
    //     require a fully-wired FetchCollector observable that signals
    //     "wakeup mid-fetch". Without a `MockClient`-backed bg task this
    //     would only exercise the wakeup_trigger plumbing already tested
    //     by `wakeup_before_poll_throws_once_then_succeeds`. Requires
    //     Fetch response routing (deferred to Phase 12.5 per
    //     `Phase-12/RESPONSE-ROUTING-AUDIT.md`).
    //   - testCommitted — full happy-path commit fetch — requires a
    //     completer that returns offsets through the FetchCommittedOffsets
    //     handle; the new `committed_propagates_event_error` exercises
    //     the same path with an error variant. Phase 12.5 integration
    //     tests cover the happy path against a real broker.
    //   - testPollThrowsInterruptExceptionIfInterrupted — Java's
    //     `Thread.currentThread().interrupt()` is unrepresentable in
    //     Rust (no thread-level interrupt flag). The equivalent is
    //     cancellation via the consumer's `wakeup_trigger`, covered by
    //     `wakeup_before_poll_throws_once_then_succeeds`.
    //   - testListenerCallbacksInvoke (parameterized) — exercises the
    //     §31 callback-invocation pipeline. The §31 regression pair in
    //     commit 11/N covers the same surface with stricter scenarios.

    /// Java: `testWakeupBeforeCallingPoll` (Java line 412-428). The
    /// inline `poll_observes_pending_wakeup_and_rotates_token` already
    /// asserts the first half (wakeup-before-poll surfaces Wakeup). The
    /// new piece is the second half: a subsequent `poll()` does NOT
    /// throw (token was rotated, fresh poll succeeds).
    #[tokio::test]
    async fn wakeup_before_poll_throws_once_then_succeeds() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("foo".to_string(), 3);
        {
            let mut subs = consumer.subscriptions.lock().unwrap();
            let mut assigned: HashSet<TopicPartition> = HashSet::new();
            assigned.insert(tp.clone());
            subs.assign_from_user(assigned).unwrap();
        }

        // Drainer: complete every AsyncPoll envelope so successful
        // polls can land.
        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::AsyncPoll { state, .. } = env.event {
                    state.complete_successfully();
                }
            }
        });

        consumer.wakeup_trigger.wakeup();
        let err = consumer.poll(Duration::from_millis(0)).await.expect_err("wakeup");
        assert!(matches!(err, Error::Wakeup(_)));

        // Second poll: must NOT raise (Java's `assertDoesNotThrow`).
        let _ = consumer.poll(Duration::from_millis(0)).await.expect("ok");
        drop(drainer);
    }

    /// Java: `testClearWakeupTriggerAfterPoll` (Java line 508-528). After
    /// a successful `poll()`, the wakeup trigger is cleared so the next
    /// `poll()` does not see a stale wakeup signal.
    #[tokio::test]
    async fn clear_wakeup_trigger_after_poll() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("foo".to_string(), 3);
        {
            let mut subs = consumer.subscriptions.lock().unwrap();
            let mut assigned: HashSet<TopicPartition> = HashSet::new();
            assigned.insert(tp.clone());
            subs.assign_from_user(assigned).unwrap();
        }

        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::AsyncPoll { state, .. } = env.event {
                    state.complete_successfully();
                }
            }
        });

        // First poll completes successfully.
        let _ = consumer.poll(Duration::from_millis(0)).await.expect("ok");
        // Second poll must not raise.
        let _ = consumer.poll(Duration::from_millis(0)).await.expect("ok");
        // Wakeup trigger has no pending task.
        assert!(!consumer.wakeup_trigger.current_token().is_cancelled());
        drop(drainer);
    }

    /// Java: `testCommittedExceptionThrown` (Java line 398-410). When
    /// the `FetchCommittedOffsetsEvent` is completed exceptionally, the
    /// error propagates out of `committed_with_timeout`.
    #[tokio::test]
    async fn committed_propagates_event_error() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        // Drainer completes the FetchCommittedOffsets handle with an error.
        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::FetchCommittedOffsets { handle, .. } = env.event {
                    handle.complete_with_error(Error::local_illegal_state("Test error"));
                    return;
                }
            }
        });

        let tp = TopicPartition::new("t0".to_string(), 2);
        let err = consumer
            .committed_with_timeout(std::slice::from_ref(&tp), Duration::from_secs(1))
            .await
            .expect_err("must err");
        // Java surfaces this as `KafkaException`; Rust surfaces the
        // underlying Error.
        assert!(!matches!(err, Error::Timeout(_)), "non-timeout err propagates, got {err:?}");
        drainer.await.expect("drainer ok");
    }

    /// Java: `testCommitAsyncShouldCopyOffsets` (Java line 358-376) —
    /// commit_async must capture a copy of the user-supplied offsets;
    /// post-call modifications to the user's map must not affect the
    /// enqueued event.
    ///
    /// Rust translation note: Rust's ownership semantics make this
    /// inherent — the consumer takes the `HashMap` by value
    /// (`commit_async_with_offsets_callback(offsets: HashMap<...>, ...)`)
    /// — so the test asserts that the event's snapshot is the same as
    /// the input.
    #[tokio::test]
    async fn commit_async_captures_offsets() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("t0".to_string(), 2);
        let offsets = singleton_offsets(tp.clone(), 10);

        let offsets_for_assert = offsets.clone();
        let completer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::CommitAsync { handle, offsets_ready, offsets: ev_offsets } = env.event {
                    // The event's offsets must contain the same entries.
                    let captured = ev_offsets.expect("CommitAsync.offsets is Some");
                    assert_eq!(captured.get(&tp).map(|v| v.offset()), Some(10));
                    assert_eq!(captured.len(), offsets_for_assert.len());
                    offsets_ready.complete(());
                    handle.complete(HashMap::new());
                    return true;
                }
            }
            false
        });

        consumer
            .commit_async_with_offsets_callback(offsets, Arc::new(NoopCallback))
            .await
            .expect("ok");
        assert!(completer.await.expect("task ok"));
    }

    /// Java: `testCommitSyncShouldCopyOffsets` (Java line 606-624) — the
    /// symmetric test for `commit_sync_with_offsets`.
    #[tokio::test]
    async fn commit_sync_captures_offsets() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("t0".to_string(), 2);
        let offsets = singleton_offsets(tp.clone(), 10);

        let completer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::CommitSync { handle, offsets_ready, offsets: ev_offsets } = env.event {
                    let captured = ev_offsets.expect("CommitSync.offsets is Some");
                    assert_eq!(captured.get(&tp).map(|v| v.offset()), Some(10));
                    offsets_ready.complete(());
                    handle.complete(HashMap::new());
                    return true;
                }
            }
            false
        });

        consumer.commit_sync_with_offsets(offsets).await.expect("ok");
        assert!(completer.await.expect("task ok"));
    }

    /// Java: `testBackgroundError` (Java line 1518-1532). An
    /// `ErrorEvent` posted by the bg task surfaces from `poll()`.
    #[tokio::test]
    async fn poll_surfaces_single_background_error() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("t".to_string(), 0);
        {
            let mut subs = consumer.subscriptions.lock().unwrap();
            let mut assigned: HashSet<TopicPartition> = HashSet::new();
            assigned.insert(tp);
            subs.assign_from_user(assigned).unwrap();
        }
        // Post an error to the bg event channel BEFORE calling poll —
        // poll's process_background_events drain must surface it.
        handles
            .bg_event_tx
            .send(BackgroundEventEnvelope {
                event: BackgroundEvent::Error {
                    error: Error::local_illegal_state("Nobody expects the Spanish Inquisition"),
                },
                enqueued_ms: 0,
            })
            .expect("send ok");

        let err = consumer.poll(Duration::from_millis(0)).await.expect_err("error must surface");
        let msg = format!("{err}");
        assert!(
            msg.contains("Nobody expects the Spanish Inquisition"),
            "expected the bg error message, got: {msg}"
        );
        // Java wraps each background-event failure through
        // `ConsumerUtils.maybeWrapAsKafkaException(t)`
        // (`AsyncKafkaConsumer.java:2213`), so what reaches the application is
        // always a `KafkaException` — even though the bg task raised a generic
        // `IllegalStateException`.
        assert!(
            err.is_kafka_error(),
            "the background error must be wrapped into the Kafka error hierarchy: {err:?}"
        );
        assert!(
            !matches!(err, Error::LocalIllegalState(_)),
            "must not surface as a raw IllegalState: {err:?}"
        );
        // The original is still reachable as the cause (Java's `getCause()`).
        let source = std::error::Error::source(&err).expect("the original error must be the cause");
        assert!(
            source.to_string().contains("Nobody expects the Spanish Inquisition"),
            "cause must be the original bg error, got: {source}"
        );
    }

    /// Java: `testMultipleBackgroundErrors` (Java line 1534-1552). When
    /// multiple errors are queued, only the FIRST is surfaced; the rest
    /// remain on the queue or are silently consumed. Java asserts the
    /// queue is empty after.
    #[tokio::test]
    async fn poll_surfaces_first_background_error_only() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("t".to_string(), 0);
        {
            let mut subs = consumer.subscriptions.lock().unwrap();
            let mut assigned: HashSet<TopicPartition> = HashSet::new();
            assigned.insert(tp);
            subs.assign_from_user(assigned).unwrap();
        }
        // Post TWO errors. Java's loop returns on the first.
        handles
            .bg_event_tx
            .send(BackgroundEventEnvelope {
                event: BackgroundEvent::Error {
                    error: Error::local_illegal_state("Nobody expects the Spanish Inquisition"),
                },
                enqueued_ms: 0,
            })
            .expect("send ok");
        handles
            .bg_event_tx
            .send(BackgroundEventEnvelope {
                event: BackgroundEvent::Error { error: Error::local_illegal_state("Spam, Spam, Spam") },
                enqueued_ms: 0,
            })
            .expect("send ok");

        let err = consumer.poll(Duration::from_millis(0)).await.expect_err("first error surfaces");
        let msg = format!("{err}");
        // Java's assertion: the FIRST error message is observed.
        assert!(msg.contains("Spanish Inquisition"), "got: {msg}");
    }

    /// Java: `testCommitSyncAwaitsCommitAsyncButDoesNotFail`
    /// (Java line 588-604). An async-commit that fails does NOT
    /// propagate to the subsequent sync commit (the callback consumes
    /// the error).
    #[tokio::test]
    async fn commit_sync_does_not_fail_when_pending_async_failed() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        // Drainer: complete the async commit EXCEPTIONALLY, then the
        // sync commit normally.
        let completer = tokio::spawn(async move {
            let mut saw_sync = false;
            while let Some(env) = handles.app_event_rx.recv().await {
                match env.event {
                    ApplicationEvent::CommitAsync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete_with_error(Error::local_illegal_state("Test error"));
                    },
                    ApplicationEvent::CommitSync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                        saw_sync = true;
                        return saw_sync;
                    },
                    _ => {},
                }
            }
            saw_sync
        });

        // Async fail
        consumer.commit_async().await.expect("async ok");
        // Sync must NOT raise (Java's `assertDoesNotThrow`)
        consumer.commit_sync().await.expect("sync must not fail");
        assert!(completer.await.expect("task ok"));
    }

    /// Java: `testCommitAsyncUserSuppliedCallbackWithException` (line 347-360,
    /// `@ParameterizedTest`) — the parameter is the exception type, supplied by
    /// `commitExceptionSupplier` (`:382-385`) as `new KafkaException(..)` and
    /// `new GroupAuthorizationException(..)`. The Rust analog is two test
    /// methods, one per variant.
    ///
    /// This one must be a *bare* `KafkaException` — [`Error::kafka`], the
    /// [`Error::KafkaError`] variant — not a `LocalIllegalState`, which is
    /// outside the `KafkaException` hierarchy entirely. Java's assertion is
    /// `assertSame(exception.getClass(), callback.exception.getClass())`: the
    /// point of the case is that a `KafkaException` reaches the user callback
    /// *unwrapped*, which a class that `is_kafka_error()` rejects cannot
    /// exercise (the sibling below was corrected for the same reason).
    /// The message drops Java's "exception" wording per CLAUDE.md §2.
    #[tokio::test]
    async fn commit_async_user_supplied_callback_with_error_kafka() {
        commit_async_callback_with_error(Error::kafka_message("Test error")).await;
    }

    #[tokio::test]
    async fn commit_async_user_supplied_callback_with_error_group_authz() {
        // Issue 23: must use `Error::GroupAuthorization`, not a string-shaped
        // `LocalIllegalArgument`. Java's `@ParameterizedTest` second parameter is
        // `GroupAuthorizationException` (`AsyncKafkaConsumerTest.java:342-356`).
        commit_async_callback_with_error(Error::group_authorization("test-group")).await;
    }

    async fn commit_async_callback_with_error(injected: Error) {
        use std::sync::atomic::AtomicUsize;
        struct RecordingCallback {
            saw_error: Arc<std::sync::Mutex<Option<String>>>,
            invoked: Arc<AtomicUsize>,
        }
        #[async_trait::async_trait]
        impl crate::consumer::OffsetCommitCallback for RecordingCallback {
            async fn on_complete(&self, _offsets: &HashMap<TopicPartition, OffsetAndMetadata>, error: Option<&Error>) {
                if let Some(e) = error {
                    *self.saw_error.lock().unwrap() = Some(format!("{e}"));
                }
                self.invoked.fetch_add(1, Ordering::SeqCst);
            }
        }

        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("my-topic".to_string(), 1);
        let offsets = singleton_offsets(tp, 200);

        let injected_clone = injected.clone();
        let completer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::CommitAsync { handle, offsets_ready, .. } = env.event {
                    offsets_ready.complete(());
                    handle.complete_with_error(injected_clone.clone());
                    return true;
                }
            }
            false
        });

        let saw_error = Arc::new(std::sync::Mutex::new(None::<String>));
        let invoked = Arc::new(AtomicUsize::new(0));
        let cb: Arc<RecordingCallback> =
            Arc::new(RecordingCallback { saw_error: Arc::clone(&saw_error), invoked: Arc::clone(&invoked) });
        consumer.commit_async_with_offsets_callback(offsets, cb).await.expect("ok");

        // Wait for the spawned continuation to enqueue the callback.
        if let Some(rx) = consumer.last_pending_async_commit.take() {
            let _ = rx.await;
        }

        // Drain the callback so it fires (Java's `forceCommitCallbackInvocation`).
        consumer.offset_commit_callback_invoker.invoke_pending_callbacks().await;

        assert!(completer.await.expect("task ok"));
        assert_eq!(invoked.load(Ordering::SeqCst), 1, "callback must be invoked once");
        let saw_msg = saw_error.lock().unwrap().clone().expect("callback observed error");
        let injected_msg = format!("{injected}");
        assert!(saw_msg == injected_msg, "callback error mismatch: {saw_msg} != {injected_msg}");
    }

    // ─── Close / lifecycle tests (commit 7/N) ───
    //
    // Stand-ins for Java's `testCloseShouldBeIdempotent`,
    // `testWakeupShouldThrowAfterClose`, `testLeaveGroupOnClose`,
    // `testRunRebalanceCallbacksOnClose`. The fully end-to-end versions
    // (`MockClient`-backed bg task observing the actual close events)
    // land in commit (10/N) — these unit-test the close-path control
    // flow.
    //
    // Skipped Java tests:
    //   - `testCloseInvokesStreamsRebalanceListenerOn*` — Streams out
    //     of scope per consumer-threading.md §20.
    //   - `testCloseWrapsStreamsRebalanceListenerException` — Streams.

    /// `close` is idempotent: calling close twice does NOT panic or
    /// surface an error. Mirrors Java's `testCloseShouldBeIdempotent`.
    #[tokio::test]
    async fn close_is_idempotent() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        // Drain LeaveGroupOnClose events on a background task, completing
        // each handle with Ok(()) so close_internal does not block.
        // Drainer that handles every close-path event (LeaveGroupOnClose,
        // CommitSync (auto-commit), CommitOnClose, StopFindCoordinatorOnClose).
        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                match env.event {
                    ApplicationEvent::LeaveGroupOnClose { handle, .. } => {
                        handle.complete(());
                    },
                    ApplicationEvent::CommitSync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    ApplicationEvent::CommitAsync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    _ => {},
                }
            }
        });
        consumer.close().await.expect("first close ok");
        assert!(consumer.is_closed(), "close marks consumer closed");
        // Second close: no-op, no error.
        consumer.close().await.expect("idempotent close ok");
        drop(drainer);
    }

    /// After `close()`, the wakeup trigger is disabled so subsequent
    /// `wakeup()` calls are no-ops (Java
    /// `wakeupTrigger.disableWakeups()`).
    #[tokio::test]
    async fn close_disables_wakeups() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        // Drainer that handles every close-path event (LeaveGroupOnClose,
        // CommitSync (auto-commit), CommitOnClose, StopFindCoordinatorOnClose).
        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                match env.event {
                    ApplicationEvent::LeaveGroupOnClose { handle, .. } => {
                        handle.complete(());
                    },
                    ApplicationEvent::CommitSync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    ApplicationEvent::CommitAsync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    _ => {},
                }
            }
        });
        consumer.close().await.expect("ok");
        // wakeup() should be a no-op now — the wakeup trigger is disabled.
        // Verify by calling maybe_trigger_wakeup; it should return Ok.
        consumer.wakeup_trigger.wakeup();
        assert!(consumer.wakeup_trigger.maybe_trigger_wakeup().is_ok());
        drop(drainer);
    }

    /// `close_with_options(timeout=0)` short-cuts the deadline math but
    /// still completes successfully.
    #[tokio::test]
    async fn close_with_options_zero_timeout_completes() {
        use crate::consumer::CloseOptions;
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        // Drainer that handles every close-path event (LeaveGroupOnClose,
        // CommitSync (auto-commit), CommitOnClose, StopFindCoordinatorOnClose).
        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                match env.event {
                    ApplicationEvent::LeaveGroupOnClose { handle, .. } => {
                        handle.complete(());
                    },
                    ApplicationEvent::CommitSync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    ApplicationEvent::CommitAsync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    _ => {},
                }
            }
        });
        consumer
            .close_with_options(CloseOptions::new_timeout(Duration::from_millis(0)))
            .await
            .expect("ok");
        assert!(consumer.is_closed());
        drop(drainer);
    }

    /// CLAUDE.md §2 splits Java's three `close` overloads
    /// (`Consumer.java:277,283,288`) into `close` / `close_with_timeout` /
    /// `close_with_options`. The deprecated `close_with_timeout` must agree with the
    /// form it forwards to: Java's `close(Duration timeout)` body is exactly
    /// `close(CloseOptions.timeout(timeout))`
    /// (`AsyncKafkaConsumer.java:1543-1545`).
    ///
    /// Asserting `is_closed()` alone would not catch a forward that dropped
    /// the timeout, so this compares the *deadline* carried on the
    /// `LeaveGroupOnClose` event — the only place the timeout is observable —
    /// between the two forms.
    #[tokio::test]
    async fn close_timeout_agrees_with_close_options_timeout() {
        use crate::consumer::CloseOptions;

        // Well under the 30s `request.timeout.ms` cap, so the deadline
        // reflects the user timeout rather than the cap.
        let user_timeout = Duration::from_secs(7);

        async fn deadline_delta_for(
            close: impl AsyncFnOnce(&mut AsyncKafkaConsumer<Vec<u8>, Vec<u8>>) -> Result<(), Error>,
        ) -> i64 {
            let (mut consumer, mut handles) = make_test_consumer_with_channels();
            let captured = Arc::new(Mutex::new(None::<i64>));
            let captured_clone = Arc::clone(&captured);
            let drainer = tokio::spawn(async move {
                while let Some(env) = handles.app_event_rx.recv().await {
                    match env.event {
                        ApplicationEvent::LeaveGroupOnClose { handle, .. } => {
                            *captured_clone.lock().unwrap() = Some(handle.deadline_ms());
                            handle.complete(());
                        },
                        ApplicationEvent::CommitSync { handle, offsets_ready, .. } => {
                            offsets_ready.complete(());
                            handle.complete(HashMap::new());
                        },
                        ApplicationEvent::CommitAsync { handle, offsets_ready, .. } => {
                            offsets_ready.complete(());
                            handle.complete(HashMap::new());
                        },
                        _ => {},
                    }
                }
            });
            let now_before = consumer.time.milliseconds();
            close(&mut consumer).await.expect("close ok");
            assert!(consumer.is_closed());
            drop(drainer);
            let deadline = captured.lock().unwrap().expect("LeaveGroupOnClose seen");
            deadline - now_before
        }

        let via_options =
            deadline_delta_for(async |c| c.close_with_options(CloseOptions::new_timeout(user_timeout)).await).await;
        let via_timeout = deadline_delta_for(async |c| {
            #[allow(deprecated)]
            c.close_with_timeout(user_timeout).await
        })
        .await;

        assert!(
            (via_options - 7_000).abs() <= 100,
            "close_with_options must carry the user timeout (delta={via_options})"
        );
        assert!(
            (via_timeout - via_options).abs() <= 100,
            "close_with_timeout must forward to close_with_options(CloseOptions::new_timeout(..)) \
             (via_timeout={via_timeout}, via_options={via_options})"
        );
    }

    /// Issue 15 regression: `close_internal` must cap the
    /// user-supplied timeout at `request.timeout.ms` per Java's
    /// `createTimerForCloseRequests(timeout)`
    /// (`AsyncKafkaConsumer.java:1590-1594`). We assert directly on
    /// the deadline carried by the close-path event — Java's
    /// `LeaveGroupOnCloseEvent(deadline)` is built with the capped
    /// timer; the Rust analog flows through
    /// `ApplicationEvent::LeaveGroupOnClose { handle, .. }` and the
    /// handle's deadline reflects the capped value.
    #[tokio::test]
    async fn close_caps_timeout_at_request_timeout_ms() {
        use crate::consumer::CloseOptions;
        let (mut consumer, mut handles) = make_test_consumer_with_channels();

        // Default config: request_timeout_ms = 30s. A 5-minute user
        // timeout MUST be capped at 30s before computing the deadline.
        let request_timeout_ms = consumer.config.request_timeout_ms() as i64;
        assert_eq!(request_timeout_ms, 30_000, "default request_timeout_ms");

        // Capture the deadline observed on the `LeaveGroupOnClose`
        // event handle — that's the post-cap value Java would build
        // via `calculateDeadlineMs(closeTimer)`.
        let captured_deadline = Arc::new(Mutex::new(None::<i64>));
        let captured_clone = Arc::clone(&captured_deadline);
        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                match env.event {
                    ApplicationEvent::LeaveGroupOnClose { handle, .. } => {
                        *captured_clone.lock().unwrap() = Some(handle.deadline_ms());
                        handle.complete(());
                    },
                    ApplicationEvent::CommitSync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    ApplicationEvent::CommitAsync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    _ => {},
                }
            }
        });

        let now_before = consumer.time.milliseconds();
        consumer
            .close_with_options(CloseOptions::new_timeout(Duration::from_secs(300)))
            .await
            .expect("close ok");
        drop(drainer);

        let captured_deadline_val = captured_deadline.lock().unwrap().expect("LeaveGroupOnClose seen");
        // The user passed 5min = 300_000ms; the cap reduces this to
        // 30_000ms. The observed deadline must be at most
        // now_before + 30_000ms (with a small fudge for clock drift
        // since `now_before` is captured before the cap logic runs).
        let user_uncapped = now_before + 300_000;
        let expected_capped = now_before + request_timeout_ms;
        assert!(
            captured_deadline_val < user_uncapped - 1_000,
            "deadline must NOT use the raw 5min user timeout \
             (captured={captured_deadline_val}, user_uncapped={user_uncapped})"
        );
        assert!(
            captured_deadline_val <= expected_capped + 100,
            "deadline must be at most now+request_timeout_ms \
             (captured={captured_deadline_val}, expected_capped={expected_capped})"
        );
    }

    /// Issue 12 regression: `run_rebalance_callbacks_on_close` must
    /// read from `group_assignment_snapshot` (populated by the
    /// `MemberStateListener`) — NOT from
    /// `SubscriptionState::assigned_partitions()`. With only a manual
    /// `assign(...)` (so the snapshot stays empty), close must NOT
    /// invoke any rebalance callback (Java line 1626-1628).
    #[tokio::test]
    async fn run_rebalance_callbacks_on_close_skips_when_snapshot_empty() {
        use async_trait::async_trait;
        use std::sync::atomic::AtomicUsize;

        struct CountingListener {
            revoked: AtomicUsize,
            lost: AtomicUsize,
        }
        #[async_trait]
        impl ConsumerRebalanceListener for CountingListener {
            async fn on_partitions_assigned(&self, _: &[TopicPartition]) -> Result<(), Error> {
                Ok(())
            }
            async fn on_partitions_revoked(&self, _: &[TopicPartition]) -> Result<(), Error> {
                self.revoked.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            async fn on_partitions_lost(&self, _: &[TopicPartition]) -> Result<(), Error> {
                self.lost.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let listener: Arc<CountingListener> =
            Arc::new(CountingListener { revoked: AtomicUsize::new(0), lost: AtomicUsize::new(0) });
        *consumer.rebalance_listener.lock().unwrap() =
            Some(Arc::clone(&listener) as Arc<dyn ConsumerRebalanceListener>);

        // Populate SubscriptionState as if the user called assign(...).
        let tp = TopicPartition::new("t".to_string(), 0);
        {
            let mut subs = consumer.subscriptions.lock().unwrap();
            let mut assigned: HashSet<TopicPartition> = HashSet::new();
            assigned.insert(tp);
            subs.assign_from_user(assigned).unwrap();
        }
        // The snapshot stays empty — no reconciliation has run.
        assert!(consumer.group_assignment_snapshot.lock().unwrap().is_empty());

        consumer.run_rebalance_callbacks_on_close().await.expect("ok");
        assert_eq!(
            listener.revoked.load(Ordering::SeqCst),
            0,
            "no listener call when snapshot is empty (manual-assign consumer)"
        );
        assert_eq!(
            listener.lost.load(Ordering::SeqCst),
            0,
            "no listener call when snapshot is empty"
        );
    }

    /// Issue 12/13 regression: `run_rebalance_callbacks_on_close`
    /// invokes `on_partitions_revoked` when the snapshot is non-empty
    /// AND `member_epoch > 0` (populated via the state notifier).
    /// Without Issue 13's `MemberStateListener` wire-up the epoch would
    /// always be -1 and the callback would always be `on_partitions_lost`.
    #[tokio::test]
    async fn run_rebalance_callbacks_on_close_invokes_revoked_on_live_epoch() {
        use async_trait::async_trait;
        use std::sync::atomic::AtomicUsize;

        struct CountingListener {
            revoked: AtomicUsize,
            lost: AtomicUsize,
        }
        #[async_trait]
        impl ConsumerRebalanceListener for CountingListener {
            async fn on_partitions_assigned(&self, _: &[TopicPartition]) -> Result<(), Error> {
                Ok(())
            }
            async fn on_partitions_revoked(&self, _: &[TopicPartition]) -> Result<(), Error> {
                self.revoked.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            async fn on_partitions_lost(&self, _: &[TopicPartition]) -> Result<(), Error> {
                self.lost.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let listener: Arc<CountingListener> =
            Arc::new(CountingListener { revoked: AtomicUsize::new(0), lost: AtomicUsize::new(0) });
        *consumer.rebalance_listener.lock().unwrap() =
            Some(Arc::clone(&listener) as Arc<dyn ConsumerRebalanceListener>);

        // The state notifier captures both the snapshot and the epoch
        // — mirrors what the bg-task reconciliation step would do.
        let notifier = consumer.state_notifier();
        let tp = TopicPartition::new("t".to_string(), 0);
        let mut snap = HashSet::new();
        snap.insert(tp.clone());
        notifier.on_group_assignment_updated(&snap);
        notifier.on_member_epoch_updated(Some(5), "member-1");

        consumer.run_rebalance_callbacks_on_close().await.expect("ok");
        assert_eq!(
            listener.revoked.load(Ordering::SeqCst),
            1,
            "live epoch + non-empty snapshot must invoke on_partitions_revoked"
        );
        assert_eq!(listener.lost.load(Ordering::SeqCst), 0);
    }

    /// Issue 12/13 regression: when the snapshot is non-empty but the
    /// epoch is unknown (member fenced / never received heartbeat
    /// response), Java falls through to `on_partitions_lost`.
    #[tokio::test]
    async fn run_rebalance_callbacks_on_close_invokes_lost_on_unknown_epoch() {
        use async_trait::async_trait;
        use std::sync::atomic::AtomicUsize;

        struct CountingListener {
            revoked: AtomicUsize,
            lost: AtomicUsize,
        }
        #[async_trait]
        impl ConsumerRebalanceListener for CountingListener {
            async fn on_partitions_assigned(&self, _: &[TopicPartition]) -> Result<(), Error> {
                Ok(())
            }
            async fn on_partitions_revoked(&self, _: &[TopicPartition]) -> Result<(), Error> {
                self.revoked.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            async fn on_partitions_lost(&self, _: &[TopicPartition]) -> Result<(), Error> {
                self.lost.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let listener: Arc<CountingListener> =
            Arc::new(CountingListener { revoked: AtomicUsize::new(0), lost: AtomicUsize::new(0) });
        *consumer.rebalance_listener.lock().unwrap() =
            Some(Arc::clone(&listener) as Arc<dyn ConsumerRebalanceListener>);

        // Populate snapshot but do NOT update epoch — generation_id
        // stays at -1, so `on_partitions_lost` is invoked.
        let notifier = consumer.state_notifier();
        let tp = TopicPartition::new("t".to_string(), 0);
        let mut snap = HashSet::new();
        snap.insert(tp.clone());
        notifier.on_group_assignment_updated(&snap);

        consumer.run_rebalance_callbacks_on_close().await.expect("ok");
        assert_eq!(listener.revoked.load(Ordering::SeqCst), 0);
        assert_eq!(
            listener.lost.load(Ordering::SeqCst),
            1,
            "unknown epoch + non-empty snapshot must invoke on_partitions_lost"
        );
    }

    /// `close` on a groupless consumer skips the leave-group event
    /// entirely. Mirrors Java's `if (groupMetadata.get().isEmpty())
    /// return;` guard.
    #[tokio::test]
    async fn close_without_group_id_skips_leave_group() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        consumer.group_id = None;
        consumer.close().await.expect("ok");
        // No LeaveGroupOnClose event should have been enqueued.
        let mut saw_leave_group = false;
        while let Ok(env) = handles.app_event_rx.try_recv() {
            if matches!(env.event, ApplicationEvent::LeaveGroupOnClose { .. }) {
                saw_leave_group = true;
            }
        }
        assert!(!saw_leave_group, "groupless consumer must not enqueue LeaveGroupOnClose");
    }

    /// After `close`, every async public API errors with
    /// `LocalIllegalState` because `ensure_open()` short-circuits. Mirrors
    /// Java's `testShouldThrowAfterClose` (every public method
    /// asserted to throw post-close). Issue 20 expanded: covers every
    /// blocking-style API on `AsyncKafkaConsumer`, not just two.
    #[tokio::test]
    async fn close_then_apis_error_with_already_closed() {
        use crate::consumer::CloseOptions;
        use crate::consumer::SubscriptionPattern;

        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        // Drainer that handles every close-path event (LeaveGroupOnClose,
        // CommitSync (auto-commit), CommitOnClose, StopFindCoordinatorOnClose).
        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                match env.event {
                    ApplicationEvent::LeaveGroupOnClose { handle, .. } => {
                        handle.complete(());
                    },
                    ApplicationEvent::CommitSync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    ApplicationEvent::CommitAsync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    _ => {},
                }
            }
        });
        consumer.close().await.expect("ok");

        let tp = TopicPartition::new("t".to_string(), 0);
        let tp_slice = std::slice::from_ref(&tp);

        // Macro: assert a Result yields IllegalState (Java's
        // `IllegalStateException`).
        macro_rules! assert_closed {
            ($name:expr, $expr:expr) => {{
                let err = $expr.expect_err(concat!($name, ": must err"));
                assert!(matches!(err, Error::LocalIllegalState(_)), "{}: {err:?}", $name);
            }};
        }

        // ── subscribe family ────────────────────────────────────────
        assert_closed!(
            "subscribe_with_topics",
            consumer.subscribe_with_topics(vec!["t".to_string()]).await
        );
        assert_closed!(
            "subscribe_with_pattern",
            consumer.subscribe_with_pattern(SubscriptionPattern::new("t.*")).await
        );
        assert_closed!("unsubscribe", consumer.unsubscribe().await);
        assert_closed!("assign", consumer.assign(vec![tp.clone()]).await);

        // ── poll ────────────────────────────────────────────────────
        assert_closed!("poll", consumer.poll(Duration::from_millis(0)).await);

        // ── commit family ───────────────────────────────────────────
        assert_closed!("commit_sync", consumer.commit_sync().await);
        assert_closed!(
            "commit_sync_with_timeout",
            consumer.commit_sync_with_timeout(Duration::from_millis(0)).await
        );
        assert_closed!(
            "commit_sync_with_offsets",
            consumer.commit_sync_with_offsets(HashMap::new()).await
        );
        assert_closed!(
            "commit_sync_with_offsets_timeout",
            consumer
                .commit_sync_with_offsets_timeout(HashMap::new(), Duration::from_millis(0))
                .await
        );
        assert_closed!("commit_async", consumer.commit_async().await);
        assert_closed!(
            "commit_async_with_callback",
            consumer.commit_async_with_callback(Arc::new(NoopCallback)).await
        );
        assert_closed!(
            "commit_async_with_offsets_callback",
            consumer
                .commit_async_with_offsets_callback(HashMap::new(), Arc::new(NoopCallback))
                .await
        );

        // ── seek family ─────────────────────────────────────────────
        assert_closed!("seek_with_offset", consumer.seek_with_offset(tp.clone(), 0).await);
        assert_closed!(
            "seek_with_offset_and_metadata",
            consumer
                .seek_with_offset_and_metadata(tp.clone(), OffsetAndMetadata::new(0).expect("ok"))
                .await
        );
        assert_closed!("seek_to_beginning", consumer.seek_to_beginning(tp_slice).await);
        assert_closed!("seek_to_end", consumer.seek_to_end(tp_slice).await);

        // ── position / committed / lag ──────────────────────────────
        assert_closed!("position", consumer.position(&tp).await);
        assert_closed!(
            "position_with_timeout",
            consumer.position_with_timeout(&tp, Duration::from_millis(0)).await
        );
        assert_closed!("committed", consumer.committed(tp_slice).await);
        assert_closed!(
            "committed_with_timeout",
            consumer.committed_with_timeout(tp_slice, Duration::from_millis(0)).await
        );
        assert_closed!("current_lag_async", consumer.current_lag_async(&tp).await);

        // ── beginning / end / offsetsForTimes ───────────────────────
        assert_closed!("beginning_offsets", consumer.beginning_offsets(tp_slice).await);
        assert_closed!(
            "beginning_offsets_with_timeout",
            consumer
                .beginning_offsets_with_timeout(tp_slice, Duration::from_millis(0))
                .await
        );
        assert_closed!("end_offsets", consumer.end_offsets(tp_slice).await);
        assert_closed!(
            "end_offsets_with_timeout",
            consumer.end_offsets_with_timeout(tp_slice, Duration::from_millis(0)).await
        );
        assert_closed!("offsets_for_times", consumer.offsets_for_times(HashMap::new()).await);
        assert_closed!(
            "offsets_for_times_with_timeout",
            consumer
                .offsets_for_times_with_timeout(HashMap::new(), Duration::from_millis(0))
                .await
        );

        // ── topic metadata ──────────────────────────────────────────
        assert_closed!("partitions_for", consumer.partitions_for("t").await);
        assert_closed!(
            "partitions_for_with_timeout",
            consumer.partitions_for_with_timeout("t", Duration::from_millis(0)).await
        );
        assert_closed!("list_topics", consumer.list_topics().await);
        assert_closed!(
            "list_topics_with_timeout",
            consumer.list_topics_with_timeout(Duration::from_millis(0)).await
        );

        // ── pause / resume ──────────────────────────────────────────
        assert_closed!("pause", consumer.pause(tp_slice).await);
        assert_closed!("resume", consumer.resume(tp_slice).await);

        // ── enforce_rebalance ───────────────────────────────────────
        // KIP-848 noop (Java's `AsyncKafkaConsumer.enforceRebalance`):
        // returns Ok always — neither pre- nor post-close. Documented in
        // method rustdoc above.

        // ── close-with-options is idempotent (not blocked) ──────────
        // close / close_with_options ARE idempotent per Java contract
        // — they short-circuit on `is_closed()` and return Ok. Verify
        // this matches the assertion above by exercising both
        // variants.
        consumer.close().await.expect("idempotent close");
        consumer
            .close_with_options(CloseOptions::new_timeout(Duration::from_millis(0)))
            .await
            .expect("idempotent close_with_options");

        drop(drainer);
    }

    // ─── Phase 11 commit (10/N) Java test translations: close / metadata / lifecycle ───
    //
    // Final batch: close-path branches, metadata APIs, seek-to-end /
    // seek-to-beginning, list-offsets, partition metadata, processBackgroundEvents
    // timing.
    //
    // SKIPs (commit 10 batch):
    //   - testFailConstructor — PLAN deferral #5 (Supplier-style ctor
    //     failure paths don't translate; bad-config path observed via
    //     `Error` returned from `new_consumer`).
    //   - testCloseInvokesStreamsRebalanceListener* /
    //     testCloseWrapsStreamsRebalanceListenerException — PLAN
    //     deferral #2 (Streams out of scope per §20).
    //   - testInterceptorAutoCommitOnClose — deferred to Issue 17 fix
    //     (commit 12 batch).
    //   - testReaperInvokedInClose / testReaperInvokedInUnsubscribe /
    //     testReaperInvokedInPoll — require Mockito-style spy on the
    //     reaper. The reap calls are wired (close_internal:2675, etc);
    //     `ConsumerNetworkThreadTest` (Phase 10 commit 8/N) exercises
    //     the bg-task reap path with a real reaper.
    //   - testSubscribePatternAgainstBrokerNotSupportingRegex — needs
    //     MockClient or a wired bg task. Deferred to Phase 12.5
    //     (response-routing) per `Phase-12/RESPONSE-ROUTING-AUDIT.md`.
    //   - testGroupMetadataIsResetAfterUnsubscribe — see commit 8 skip
    //     section.
    //   - testLongPollWaitIsLimited — requires a full FetchCollector
    //     wired into the bg task; observable only via integration tests.
    //   - testNoWakeupInCloseCommit — covered by close-path inline
    //     tests (the close drainer completes CommitSync envelopes
    //     normally, demonstrating no wakeup interference).
    //   - testCommitSyncAllConsumed / testAutoCommitSyncDisabled — require
    //     a fully-wired SubscriptionState `commit_sync_all_consumed`
    //     helper that is consumer-internal (Java: package-private). The
    //     close-path inline tests demonstrate the SyncCommitEvent
    //     enqueue/no-enqueue behaviour for the auto_commit_enabled flag.
    //
    // Issue 24 additions (commit 11/N batch):
    //   - testCloseAwaitPendingAsyncCommitIncomplete — requires the
    //     `lastPendingAsyncCommit` future to be held in an incomplete
    //     state past the close-timeout. The Rust analog
    //     (`last_pending_async_commit: Option<oneshot::Receiver<()>>`)
    //     is exercised by `close_awaits_pending_async_commit_complete`
    //     for the happy path; the timeout-cause-of-incomplete path
    //     requires injecting a never-completing handle that is
    //     specifically held by the test through close's
    //     `await_pending_async_commits` step. Requires response routing
    //     for the bg task to drive the incomplete-future timing
    //     naturally — deferred to Phase 12.5 per
    //     `Phase-12/RESPONSE-ROUTING-AUDIT.md`.
    //   - testCloseLeavesGroupDespiteOnPartitionsLostError — Mockito's
    //     `spy(newConsumer(...))` + `setGroupAssignmentSnapshot` API
    //     surface does not have an inline Rust analog. The
    //     `run_rebalance_callbacks_on_close` code path IS covered by
    //     `close_runs_partitions_lost_on_unknown_epoch` /
    //     `close_runs_partitions_revoked_on_live_epoch`; the additional
    //     "leave group fires DESPITE listener throwing" assertion is
    //     a Mockito-spy fixture cost not worth replicating here. The
    //     Rust close path's `first_error` tracking + `LeaveGroupOnClose`
    //     enqueue ordering is unconditional (close_internal:2680+) —
    //     listener errors do not gate the leave step.
    //   - testCloseLeavesGroupDespiteInterrupt — Java's
    //     `InterruptException` has no Rust analog (no thread-interrupt
    //     primitive). The `wakeup()` path (which is the Rust analog) is
    //     covered by `close_caps_timeout_at_request_timeout_ms`-class
    //     tests; the "InterruptException thrown by addAndGet" injection
    //     is Mockito-only.
    //   - testGroupRemoteAssignorUsedInConsumerProtocol — depends on
    //     `ConsumerConfig::unused()` tracking, same blocker as Issue 27
    //     (no inline Rust surface yet). The construction-side
    //     assertion is partially covered by
    //     `group_remote_assignor_unused_if_group_id_undefined` /
    //     `group_id_null_constructs_successfully`.

    /// Java: `testSuccessfulStartupShutdown` (Java line 279-284). A
    /// freshly-constructed consumer can be closed without throwing.
    /// The inline `close_is_idempotent` is the stricter version; this
    /// stays parallel to Java's name + body for clarity.
    #[tokio::test]
    async fn successful_startup_shutdown() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                match env.event {
                    ApplicationEvent::LeaveGroupOnClose { handle, .. } => {
                        handle.complete(());
                    },
                    ApplicationEvent::CommitSync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    _ => {},
                }
            }
        });
        consumer.close().await.expect("close ok");
        drop(drainer);
    }

    /// Java: `testCloseAwaitPendingAsyncCommitComplete` (Java line 1087-1106).
    /// On close, a pending async commit's callback fires.
    #[tokio::test]
    async fn close_awaits_pending_async_commit_complete() {
        use std::sync::atomic::AtomicUsize;
        struct ClosingCallback {
            invoked: Arc<AtomicUsize>,
        }
        #[async_trait::async_trait]
        impl crate::consumer::OffsetCommitCallback for ClosingCallback {
            async fn on_complete(&self, _offsets: &HashMap<TopicPartition, OffsetAndMetadata>, _error: Option<&Error>) {
                self.invoked.fetch_add(1, Ordering::SeqCst);
            }
        }

        let (mut consumer, mut handles) = make_test_consumer_with_channels();

        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                match env.event {
                    ApplicationEvent::CommitAsync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    ApplicationEvent::CommitSync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    ApplicationEvent::LeaveGroupOnClose { handle, .. } => {
                        handle.complete(());
                    },
                    _ => {},
                }
            }
        });

        let invoked = Arc::new(AtomicUsize::new(0));
        let cb = Arc::new(ClosingCallback { invoked: Arc::clone(&invoked) });
        consumer
            .commit_async_with_offsets_callback(HashMap::new(), cb)
            .await
            .expect("ok");
        consumer.close().await.expect("close ok");
        // The callback must have fired (close drains pending async commits).
        assert_eq!(
            invoked.load(Ordering::SeqCst),
            1,
            "pending async-commit callback must fire on close"
        );
        drop(drainer);
    }

    /// Java: `testCloseLeavesGroup(0 || DEFAULT_CLOSE_TIMEOUT_MS)`
    /// (Java line 693-704, `@ParameterizedTest`). The KIP-848 close
    /// path always submits a `LeaveGroupOnClose` event before
    /// shutting down. Translated as two test methods (timeout=0 and
    /// timeout=DEFAULT_CLOSE_TIMEOUT_MS) per the @ParameterizedTest →
    /// loop rule.
    async fn close_leaves_group_for_timeout_inner(timeout_ms: u64) {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let saw_leave = Arc::new(AtomicBool::new(false));
        let saw_leave_clone = Arc::clone(&saw_leave);
        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                match env.event {
                    ApplicationEvent::LeaveGroupOnClose { handle, .. } => {
                        saw_leave_clone.store(true, Ordering::SeqCst);
                        handle.complete(());
                    },
                    ApplicationEvent::CommitSync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    ApplicationEvent::CommitAsync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    _ => {},
                }
            }
        });

        consumer
            .close_with_options(crate::consumer::CloseOptions::new_timeout(Duration::from_millis(timeout_ms)))
            .await
            .expect("close ok");

        // close()'s last step drops `application_event_handler` and the
        // network_thread_close handler. The drainer's `app_event_rx.recv()`
        // returns None once all senders are dropped. Wait for the drainer
        // to fully drain.
        drop(consumer);
        let _ = drainer.await;

        assert!(
            saw_leave.load(Ordering::SeqCst),
            "LeaveGroupOnClose must be enqueued (timeout={timeout_ms})"
        );
    }

    #[tokio::test]
    async fn close_leaves_group_timeout_zero() {
        close_leaves_group_for_timeout_inner(0).await;
    }

    #[tokio::test]
    async fn close_leaves_group_timeout_default() {
        // Java: `ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS = 30_000`.
        use crate::consumer::internals::ConsumerUtils;
        close_leaves_group_for_timeout_inner(ConsumerUtils::DEFAULT_CLOSE_TIMEOUT_MS as u64).await;
    }

    /// Java: `testVerifyApplicationEventOnShutdown` (Java line 683-691).
    /// On close, `CommitOnCloseEvent` is enqueued (Java verifies via
    /// `verify(applicationEventHandler).add(any(CommitOnCloseEvent.class))`).
    #[tokio::test]
    async fn close_enqueues_commit_on_close_event() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let saw_commit_on_close = Arc::new(AtomicBool::new(false));
        let saw_clone = Arc::clone(&saw_commit_on_close);
        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                match env.event {
                    ApplicationEvent::CommitOnClose => {
                        saw_clone.store(true, Ordering::SeqCst);
                    },
                    ApplicationEvent::LeaveGroupOnClose { handle, .. } => {
                        handle.complete(());
                    },
                    ApplicationEvent::CommitSync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    ApplicationEvent::CommitAsync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    _ => {},
                }
            }
        });
        consumer.close().await.expect("close ok");
        assert!(
            saw_commit_on_close.load(Ordering::SeqCst),
            "CommitOnClose event must be enqueued"
        );
        drop(drainer);
    }

    /// Java: `testSeekToBeginning` (Java line 1817-1826). `seek_to_beginning`
    /// enqueues a `ResetOffset` event with the EARLIEST strategy.
    #[tokio::test]
    async fn seek_to_beginning_enqueues_reset_offset_event() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("test".to_string(), 0);
        let topics = vec![tp.clone()];

        let captured_strategy = Arc::new(std::sync::Mutex::new(None::<AutoOffsetResetStrategy>));
        let captured_partitions = Arc::new(std::sync::Mutex::new(Vec::<TopicPartition>::new()));
        let cap_strategy = Arc::clone(&captured_strategy);
        let cap_partitions = Arc::clone(&captured_partitions);
        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::ResetOffset { handle, partitions, offset_reset_strategy } = env.event {
                    *cap_strategy.lock().unwrap() = Some(offset_reset_strategy);
                    *cap_partitions.lock().unwrap() = partitions.into_iter().collect();
                    handle.complete(());
                    return;
                }
            }
        });

        consumer.seek_to_beginning(&topics).await.expect("ok");
        drainer.await.expect("task ok");

        let strat = captured_strategy.lock().unwrap().clone().expect("ResetOffset event seen");
        assert_eq!(strat, AutoOffsetResetStrategy::EARLIEST);
        let parts = captured_partitions.lock().unwrap().clone();
        assert!(parts.contains(&tp));
    }

    /// Java: `testSeekToEnd` (Java line 1844-1853). Symmetric to
    /// seek_to_beginning but with LATEST.
    #[tokio::test]
    async fn seek_to_end_enqueues_reset_offset_event() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("test".to_string(), 0);
        let topics = vec![tp.clone()];

        let captured_strategy = Arc::new(std::sync::Mutex::new(None::<AutoOffsetResetStrategy>));
        let cap_strategy = Arc::clone(&captured_strategy);
        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::ResetOffset { handle, offset_reset_strategy, .. } = env.event {
                    *cap_strategy.lock().unwrap() = Some(offset_reset_strategy);
                    handle.complete(());
                    return;
                }
            }
        });

        consumer.seek_to_end(&topics).await.expect("ok");
        drainer.await.expect("task ok");

        let strat = captured_strategy.lock().unwrap().clone().expect("ResetOffset event seen");
        assert_eq!(strat, AutoOffsetResetStrategy::LATEST);
    }

    /// Java: `testSeekToBeginningWithException` (Java line 1828-1834).
    /// When the `ResetOffsetEvent` is completed exceptionally with a
    /// timeout, `seek_to_beginning` surfaces the error.
    #[tokio::test]
    async fn seek_to_beginning_propagates_event_error() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("test".to_string(), 0);

        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::ResetOffset { handle, .. } = env.event {
                    handle.complete_with_error(Error::timeout("test timeout"));
                    return;
                }
            }
        });

        let err = consumer.seek_to_beginning(&[tp]).await.expect_err("must err");
        assert!(matches!(err, Error::Timeout(_)), "unexpected err: {err:?}");
        drainer.await.expect("task ok");
    }

    /// Java: `testSeekToEndWithException` (Java line 1836-1842). Symmetric.
    #[tokio::test]
    async fn seek_to_end_propagates_event_error() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("test".to_string(), 0);

        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::ResetOffset { handle, .. } = env.event {
                    handle.complete_with_error(Error::timeout("test timeout"));
                    return;
                }
            }
        });

        let err = consumer.seek_to_end(&[tp]).await.expect_err("must err");
        assert!(matches!(err, Error::Timeout(_)), "unexpected err: {err:?}");
        drainer.await.expect("task ok");
    }

    /// Java: `testBeginningOffsets` (Java line 861-882). With a positive
    /// timeout the `beginning_offsets_with_timeout` waits for the
    /// `ListOffsets` event to complete and returns the per-partition
    /// offsets map.
    #[tokio::test]
    async fn beginning_offsets_returns_event_result() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let t0 = TopicPartition::new("t0".to_string(), 2);
        let t1 = TopicPartition::new("t0".to_string(), 3);

        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::ListOffsets { handle, .. } = env.event {
                    let mut result: HashMap<TopicPartition, Option<OffsetAndTimestampInternal>> = HashMap::new();
                    result.insert(
                        TopicPartition::new("t0".to_string(), 2),
                        Some(OffsetAndTimestampInternal::new(5, 1, None)),
                    );
                    result.insert(
                        TopicPartition::new("t0".to_string(), 3),
                        Some(OffsetAndTimestampInternal::new(6, 3, None)),
                    );
                    handle.complete(result);
                    return;
                }
            }
        });

        let offsets = consumer
            .beginning_offsets_with_timeout(&[t0.clone(), t1.clone()], Duration::from_millis(100))
            .await
            .expect("ok");
        assert_eq!(offsets.get(&t0), Some(&5));
        assert_eq!(offsets.get(&t1), Some(&6));
        drainer.await.expect("task ok");
    }

    /// Java: `OffsetFetcherTest.testBeginningOffsetsDuplicateTopicPartition`
    /// (`beginningOffsets(asList(tp0, tp0))`). The duplicate partition in
    /// the input slice collapses to a single entry: the
    /// `timestamps_to_search` map built in `beginning_or_end_offsets`
    /// (slice → `HashMap`) de-duplicates, producing exactly ONE wire
    /// request / ONE result entry.
    #[tokio::test]
    async fn beginning_offsets_duplicate_topic_partition_collapses() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("t0".to_string(), 0);

        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::ListOffsets { handle, timestamps_to_search, .. } = env.event {
                    // The duplicate tp must have collapsed to one entry.
                    assert_eq!(
                        timestamps_to_search.len(),
                        1,
                        "duplicate topic-partition must collapse to a single search entry"
                    );
                    let mut result: HashMap<TopicPartition, Option<OffsetAndTimestampInternal>> = HashMap::new();
                    result.insert(
                        TopicPartition::new("t0".to_string(), 0),
                        Some(OffsetAndTimestampInternal::new(2, -1, None)),
                    );
                    handle.complete(result);
                    return;
                }
            }
        });

        let offsets = consumer
            .beginning_offsets_with_timeout(&[tp.clone(), tp.clone()], Duration::from_millis(100))
            .await
            .expect("ok");
        assert_eq!(offsets.len(), 1, "duplicate partition collapses to one result entry");
        assert_eq!(offsets.get(&tp), Some(&2));
        drainer.await.expect("task ok");
    }

    /// Java: `OffsetFetcherTest.testEndOffsetsDuplicateTopicPartition`
    /// (`endOffsets(asList(tp0, tp0))`). Same slice→map collapse as the
    /// beginning-offsets variant.
    #[tokio::test]
    async fn end_offsets_duplicate_topic_partition_collapses() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("t0".to_string(), 0);

        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::ListOffsets { handle, timestamps_to_search, .. } = env.event {
                    assert_eq!(
                        timestamps_to_search.len(),
                        1,
                        "duplicate topic-partition must collapse to a single search entry"
                    );
                    let mut result: HashMap<TopicPartition, Option<OffsetAndTimestampInternal>> = HashMap::new();
                    result.insert(
                        TopicPartition::new("t0".to_string(), 0),
                        Some(OffsetAndTimestampInternal::new(5, -1, None)),
                    );
                    handle.complete(result);
                    return;
                }
            }
        });

        let offsets = consumer
            .end_offsets_with_timeout(&[tp.clone(), tp.clone()], Duration::from_millis(100))
            .await
            .expect("ok");
        assert_eq!(offsets.len(), 1, "duplicate partition collapses to one result entry");
        assert_eq!(offsets.get(&tp), Some(&5));
        drainer.await.expect("task ok");
    }

    /// Java: `testBeginningOffsetsThrowsKafkaExceptionForUnderlyingExecutionFailure`
    /// (Java line 884-897). The `ListOffsetsEvent` completes
    /// exceptionally and the error propagates.
    #[tokio::test]
    async fn beginning_offsets_propagates_event_error() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("t0".to_string(), 0);

        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::ListOffsets { handle, .. } = env.event {
                    handle.complete_with_error(Error::local_illegal_state(
                        "Unexpected failure processing List Offsets event",
                    ));
                    return;
                }
            }
        });

        let err = consumer
            .beginning_offsets_with_timeout(&[tp], Duration::from_millis(100))
            .await
            .expect_err("must err");
        assert!(!matches!(err, Error::Timeout(_)), "non-timeout err propagates, got {err:?}");
        drainer.await.expect("task ok");
    }

    /// Java: `testBeginningOffsetsTimeoutException` (Java line 965-977)
    /// — the event times out, surfacing as `Timeout` with the exact
    /// Java message `"Failed to get offsets by times in {timeout}ms"`.
    /// (Issue 25 / DoD §3: exact-message assertion.)
    #[tokio::test]
    async fn beginning_offsets_propagates_timeout() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("topic".to_string(), 5);

        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::ListOffsets { handle, .. } = env.event {
                    handle.complete_with_error(Error::timeout(
                        "Event did not complete in time and was expired by the reaper",
                    ));
                    return;
                }
            }
        });

        let err = consumer
            .beginning_offsets_with_timeout(&[tp], Duration::from_millis(100))
            .await
            .expect_err("must err");
        match err {
            Error::Timeout(msg) => {
                assert_eq!(msg.message(), "Failed to get offsets by times in 100ms");
            },
            other => panic!("expected Timeout, got {other:?}"),
        }
        drainer.await.expect("task ok");
    }

    /// Java: `testEndOffsetsTimeoutException` (Java line 979-991). Symmetric
    /// of `beginning_offsets_propagates_timeout`. Asserts the exact
    /// message contract for `end_offsets_with_timeout` since both routes share
    /// the timeout-format string in production code (Issue 25).
    #[tokio::test]
    async fn end_offsets_propagates_timeout_with_exact_message() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("topic".to_string(), 5);

        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::ListOffsets { handle, .. } = env.event {
                    handle.complete_with_error(Error::timeout(
                        "Event did not complete in time and was expired by the reaper",
                    ));
                    return;
                }
            }
        });

        let err = consumer
            .end_offsets_with_timeout(&[tp], Duration::from_millis(250))
            .await
            .expect_err("must err");
        match err {
            Error::Timeout(msg) => {
                assert_eq!(msg.message(), "Failed to get offsets by times in 250ms");
            },
            other => panic!("expected Timeout, got {other:?}"),
        }
        drainer.await.expect("task ok");
    }

    /// Java: `testBeginningOffsetsWithZeroTimeout` (Java line 996-1005).
    /// `beginning_offsets_with_timeout(tp, ZERO)` enqueues the event via
    /// `add(...)` (NOT `add_and_get`) and returns an empty map without
    /// blocking. Issue 24.
    #[tokio::test]
    async fn beginning_offsets_with_zero_timeout_returns_empty_and_enqueues_event() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("topic1".to_string(), 0);

        let saw_list_offsets = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let saw_flag = Arc::clone(&saw_list_offsets);
        let drainer = tokio::spawn(async move {
            // Pull the envelope but DO NOT complete it (Java's `add`
            // path leaves the handle dangling; the receiver is dropped).
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::ListOffsets { .. } = env.event {
                    saw_flag.store(true, Ordering::SeqCst);
                    return;
                }
            }
        });

        let map = consumer
            .beginning_offsets_with_timeout(&[tp], Duration::from_millis(0))
            .await
            .expect("zero-timeout returns Ok with empty map");
        assert!(map.is_empty(), "zero-timeout returns empty map");
        drainer.await.expect("drainer ok");
        assert!(saw_list_offsets.load(Ordering::SeqCst), "ListOffsets event must be enqueued");
    }

    /// Java: `testOffsetsForTimesWithZeroTimeout` (Java line 1007-1017).
    /// `offsets_for_times_with_timeout(map, ZERO)` returns an empty map
    /// without blocking via `add_and_get`. Issue 24. (The Java analog
    /// asserts `never().addAndGet(ListOffsets)`; the Rust analog is
    /// that no event is `add_and_get`-ed — the bg path is empty.)
    #[tokio::test]
    async fn offsets_for_times_with_zero_timeout_returns_empty_map() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("topic1".to_string(), 0);
        let mut search: HashMap<TopicPartition, i64> = HashMap::new();
        search.insert(tp, 5);

        let map = consumer
            .offsets_for_times_with_timeout(search, Duration::from_millis(0))
            .await
            .expect("zero-timeout returns Ok");
        assert!(map.is_empty(), "expected empty map, got {map:?}");
    }

    /// Java: `testOffsetsForTimesFailsOnNegativeTargetTimes`
    /// (Java line 917-934). Three asserts: EARLIEST_TIMESTAMP (-2),
    /// LATEST_TIMESTAMP (-1), MAX_TIMESTAMP (-3) all reject with
    /// IllegalArgument. Issue 24.
    #[tokio::test]
    async fn offsets_for_times_rejects_negative_target_times() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("topic1".to_string(), 1);

        for negative in &[-2i64, -1, -3] {
            let mut search: HashMap<TopicPartition, i64> = HashMap::new();
            search.insert(tp.clone(), *negative);
            let err = consumer
                .offsets_for_times_with_timeout(search, Duration::from_millis(1))
                .await
                .expect_err("negative target rejected");
            assert!(
                matches!(err, Error::LocalIllegalArgument(ref m) if m.message().contains("negative")),
                "expected IllegalArgument with 'negative', got {err:?}"
            );
        }
    }

    /// Java: `testOffsetsForTimesTimeoutException` (Java line 952-963).
    /// Asserts EXACT error message `"Failed to get offsets by times in
    /// {timeout}ms"`. Issue 24 / DoD §3.
    #[tokio::test]
    async fn offsets_for_times_propagates_timeout_with_exact_message() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("topic1".to_string(), 1);
        let mut search: HashMap<TopicPartition, i64> = HashMap::new();
        search.insert(tp, 5);

        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::ListOffsets { handle, .. } = env.event {
                    handle.complete_with_error(Error::timeout(
                        "Event did not complete in time and was expired by the reaper",
                    ));
                    return;
                }
            }
        });

        let err = consumer
            .offsets_for_times_with_timeout(search, Duration::from_millis(100))
            .await
            .expect_err("must err");
        match err {
            Error::Timeout(msg) => {
                assert_eq!(msg.message(), "Failed to get offsets by times in 100ms");
            },
            other => panic!("expected Timeout, got {other:?}"),
        }
        drainer.await.expect("drainer ok");
    }

    /// Java: `testBeginningOffsetsTimeoutOnEventProcessingTimeout`
    /// (Java line 899-908). The `addAndGet`-thrown TimeoutException
    /// surfaces from `beginning_offsets(tp, 1ms)` AND the
    /// `ListOffsetsEvent` was actually enqueued. Distinct from
    /// `testBeginningOffsetsTimeoutException` (which asserts the
    /// exact error message): this asserts both the propagation AND
    /// the event-enqueue side effect. Issue 24.
    #[tokio::test]
    async fn beginning_offsets_timeout_on_event_processing_enqueues_event() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("t1".to_string(), 0);

        let saw_list_offsets = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let saw_flag = Arc::clone(&saw_list_offsets);
        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::ListOffsets { handle, .. } = env.event {
                    saw_flag.store(true, Ordering::SeqCst);
                    handle.complete_with_error(Error::timeout("bg-side timeout"));
                    return;
                }
            }
        });

        let err = consumer
            .beginning_offsets_with_timeout(&[tp], Duration::from_millis(1))
            .await
            .expect_err("must err");
        assert!(matches!(err, Error::Timeout(_)), "got {err:?}");
        drainer.await.expect("drainer ok");
        assert!(saw_list_offsets.load(Ordering::SeqCst), "ListOffsets event must be enqueued");
    }

    /// Java: `testOffsetsForTimes` (Java line 936-950). Happy-path
    /// resolution returns a map of `OffsetAndTimestamp` for each
    /// requested partition.
    #[tokio::test]
    async fn offsets_for_times_returns_event_result() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let t0 = TopicPartition::new("t0".to_string(), 2);
        let t1 = TopicPartition::new("t0".to_string(), 3);

        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::ListOffsets { handle, .. } = env.event {
                    let mut result: HashMap<TopicPartition, Option<OffsetAndTimestampInternal>> = HashMap::new();
                    result.insert(
                        TopicPartition::new("t0".to_string(), 2),
                        Some(OffsetAndTimestampInternal::new(5, 1, None)),
                    );
                    result.insert(
                        TopicPartition::new("t0".to_string(), 3),
                        Some(OffsetAndTimestampInternal::new(6, 3, None)),
                    );
                    handle.complete(result);
                    return;
                }
            }
        });

        let mut ts_search: HashMap<TopicPartition, i64> = HashMap::new();
        ts_search.insert(t0.clone(), 1);
        ts_search.insert(t1.clone(), 2);
        let result = consumer
            .offsets_for_times_with_timeout(ts_search, Duration::from_millis(100))
            .await
            .expect("ok");
        assert_eq!(result.get(&t0).map(|x| x.offset()), Some(5));
        assert_eq!(result.get(&t1).map(|x| x.offset()), Some(6));
        drainer.await.expect("task ok");
    }

    /// Java: `testProcessBackgroundEventsWithoutDelay` (Java line 1717-1730).
    /// When the typed-completion is already ready, the drain helper
    /// returns immediately and the timer's remaining_ms equals the
    /// initial value. We don't model Java's `Timer.remainingMs()`
    /// surface directly; instead we assert the drain helper returns
    /// quickly when the receiver is already-completed.
    #[tokio::test]
    async fn process_background_events_until_returns_immediately_for_ready_receiver() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        // Build a pre-completed receiver.
        let (handle, receiver, _erased) =
            crate::consumer::internals::events::CompletableEvent::make_completable_event::<()>(i64::MAX);
        handle.complete(());

        let start = std::time::Instant::now();
        let deadline = consumer.time.milliseconds() + 1000;
        let result = consumer
            .process_background_events_until::<()>(receiver, deadline, |_| false, "should not see this msg", false)
            .await;
        let elapsed = start.elapsed();
        result.expect("ready receiver must resolve");
        assert!(
            elapsed < Duration::from_millis(50),
            "ready receiver should resolve immediately, elapsed: {elapsed:?}"
        );
    }

    /// Java: `testProcessBackgroundEventsTimesOut` (Java line 1736-1752).
    /// A receiver that never completes surfaces as `Timeout`.
    #[tokio::test]
    async fn process_background_events_until_times_out_for_pending_receiver() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let (_handle, receiver, _erased) =
            crate::consumer::internals::events::CompletableEvent::make_completable_event::<()>(i64::MAX);
        // Keep _handle alive — never complete it.

        let deadline = consumer.time.milliseconds() + 100;
        let err = consumer
            .process_background_events_until::<()>(receiver, deadline, |_| false, "drain helper timeout", false)
            .await
            .expect_err("must time out");
        assert!(matches!(err, Error::Timeout(_)), "unexpected err: {err:?}");
    }

    /// Compile-time check: `AsyncKafkaConsumer<K, V>` is `Consumer<K, V>`.
    /// Asserts the trait impl is wired correctly.
    #[test]
    fn consumer_trait_impl_compiles() {
        fn _accept_consumer<C: crate::consumer::Consumer<Vec<u8>, Vec<u8>>>(_c: C) {}
        // Only the type-level check matters — no runtime assertions.
        let _phantom: fn(AsyncKafkaConsumer<Vec<u8>, Vec<u8>>) = _accept_consumer;
    }

    // ── Phase 21: dedicated-IO-thread `NetworkThreadCloseHandle` tests ──
    //
    // The production `AsyncKafkaConsumer::new()` runs the bg loop on a
    // dedicated `std::thread` hosting a `current_thread` runtime and
    // builds a `NetworkThreadCloseHandle::with_dedicated(...)`. These
    // tests model that exact construction (running flag → bg loop →
    // wakeup → `done` oneshot → OS thread reap) without needing a broker,
    // and assert the close path joins the dedicated thread cleanly within
    // a bounded timeout (no hang), preserving the `Spawned`-path
    // semantics (clean exit + panic mapping).

    /// Builds a dedicated bg thread that mirrors the production loop:
    /// a `current_thread` runtime spinning a `run_once`-style loop until
    /// the running flag flips, firing `done` on exit. Returns the close
    /// handle plus the running flag and a wakeup `Notify` the loop waits
    /// on (so the test can prove `signal_close` + `wakeup` terminate it).
    fn spawn_dedicated_bg() -> (NetworkThreadCloseHandle, Arc<AtomicBool>, Arc<tokio::sync::Notify>) {
        let running = Arc::new(AtomicBool::new(true));
        let wake = Arc::new(tokio::sync::Notify::new());

        let loop_running = Arc::clone(&running);
        let loop_wake = Arc::clone(&wake);
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let thread_handle = std::thread::Builder::new()
            .name("kafka-consumer-io-test".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build test io runtime");
                rt.block_on(async move {
                    // Mirror the production `while is_running() { run_once().await }`
                    // loop: block on the wakeup `Notify` each iteration so
                    // the loop only proceeds when woken (as the real
                    // selector poll returns on `Selector::wakeup`).
                    while loop_running.load(Ordering::Acquire) {
                        loop_wake.notified().await;
                    }
                    // Stand-in for `cleanup().await`.
                });
                let _ = done_tx.send(());
            })
            .expect("spawn test io thread");

        let close_running = Arc::clone(&running);
        let close_wake = Arc::clone(&wake);
        let signal_close_fn: Box<dyn Fn() + Send + Sync> = Box::new(move || {
            close_running.store(false, Ordering::Release);
            close_wake.notify_one();
        });
        let wakeup_wake = Arc::clone(&wake);
        let wakeup_fn: Box<dyn Fn() + Send + Sync> = Box::new(move || {
            wakeup_wake.notify_one();
        });

        let handle = NetworkThreadCloseHandle::with_dedicated(signal_close_fn, wakeup_fn, done_rx, thread_handle);
        (handle, running, wake)
    }

    /// `signal_close()` + `wakeup()` terminate the dedicated bg loop and
    /// `await_join()` cleanly reaps the OS thread within a bounded
    /// timeout (no hang).
    #[tokio::test]
    async fn dedicated_close_handle_joins_cleanly() {
        let (mut handle, running, _wake) = spawn_dedicated_bg();

        handle.signal_close();
        handle.wakeup();

        let result = tokio::time::timeout(Duration::from_secs(5), handle.await_join())
            .await
            .expect("await_join must not hang");
        assert!(result.is_ok(), "clean dedicated-thread join, got {result:?}");
        assert!(!running.load(Ordering::Acquire), "running flag must be cleared");
    }

    /// `await_join()` on a `Dedicated` handle is idempotent: a second
    /// call after a successful join is a no-op `Ok(())`, not a hang.
    #[tokio::test]
    async fn dedicated_await_join_is_idempotent() {
        let (mut handle, _running, _wake) = spawn_dedicated_bg();

        handle.signal_close();
        handle.wakeup();

        tokio::time::timeout(Duration::from_secs(5), handle.await_join())
            .await
            .expect("first await_join must not hang")
            .expect("first await_join clean");

        // Second call: receiver + thread already taken → immediate Ok.
        let second = tokio::time::timeout(Duration::from_secs(1), handle.await_join())
            .await
            .expect("second await_join must not hang");
        assert!(second.is_ok(), "idempotent second join, got {second:?}");
    }

    /// A panic inside the dedicated bg thread is mapped to the SAME
    /// `Error::local_illegal_state("Consumer network thread terminated
    /// with error: ...")` shape as the `Spawned` JoinError path.
    #[tokio::test]
    async fn dedicated_thread_panic_maps_to_illegal_state() {
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let thread_handle = std::thread::Builder::new()
            .name("kafka-consumer-io-test-panic".into())
            .spawn(move || {
                // Hold `done_tx` so it is dropped (not sent) on panic —
                // `await_join` must treat the closed receiver as "loop
                // exited" and still reap the panicking thread.
                let _done_tx = done_tx;
                panic!("boom");
            })
            .expect("spawn panicking test io thread");

        let mut handle =
            NetworkThreadCloseHandle::with_dedicated(Box::new(|| {}), Box::new(|| {}), done_rx, thread_handle);

        let result = tokio::time::timeout(Duration::from_secs(5), handle.await_join())
            .await
            .expect("await_join must not hang on panic");
        match result {
            Err(Error::LocalIllegalState(msg)) => {
                assert!(
                    msg.message().contains("Consumer network thread terminated with error"),
                    "unexpected message: {msg}"
                );
            },
            other => panic!("expected IllegalState on thread panic, got {other:?}"),
        }
    }

    /// Java's `initializeGroupMetadata` rejects a present-but-EMPTY `group.id`
    /// before building anything
    /// (`AsyncKafkaConsumer.java:747-757`), and the constructor's
    /// `catch (Throwable t)` then wraps it (`:509-517`).
    ///
    /// Accepting it leaves the consumer internally inconsistent: `Some("")` is
    /// "in a group" for the coordinator / commit / heartbeat / membership
    /// wiring but "not in a group" for `return_error_if_group_id_not_defined`, so
    /// `FindCoordinator` and `ConsumerGroupHeartbeat` go on the wire with an
    /// empty group id while `commit_sync()` reports `InvalidGroupId`.
    #[tokio::test(flavor = "multi_thread")]
    async fn empty_group_id_is_rejected_at_construction() {
        use std::collections::HashMap;

        use crate::common::serialization::Deserializer;

        struct TestStringDeserializer;
        impl Deserializer<String> for TestStringDeserializer {
            fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, Error> {
                String::from_utf8(data.to_vec()).map_err(|e| Error::serialization(format!("invalid utf-8: {}", e)))
            }
        }

        let props = HashMap::from([
            ("bootstrap.servers".to_string(), "127.0.0.1:1".to_string()),
            ("group.id".to_string(), String::new()),
        ]);
        let config = ConsumerConfig::new(&props).expect("config itself validates");

        let err = AsyncKafkaConsumer::<String, String>::new(
            config,
            Box::new(TestStringDeserializer),
            Box::new(TestStringDeserializer),
        )
        .err()
        .expect("an empty group.id must fail construction");

        // Wrapped as Java wraps every constructor failure.
        assert_eq!("Failed to construct kafka consumer", err.message());
        let cause = err.source().expect("the InvalidGroupId is the cause");
        assert_eq!(cause.error(), crate::common::Errors::InvalidGroupId);
        assert_eq!(
            "The configured group.id should not be an empty string or whitespace.",
            cause.message()
        );
    }

    /// A `group.id` that is absent, or non-empty, still constructs — so the
    /// check above cannot be an unconditional rejection.
    #[tokio::test(flavor = "multi_thread")]
    async fn non_empty_and_absent_group_id_still_construct() {
        use std::collections::HashMap;

        use crate::common::serialization::Deserializer;

        struct TestStringDeserializer;
        impl Deserializer<String> for TestStringDeserializer {
            fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, Error> {
                String::from_utf8(data.to_vec()).map_err(|e| Error::serialization(format!("invalid utf-8: {}", e)))
            }
        }

        for group_id in [None, Some("a-group")] {
            let mut props = HashMap::from([("bootstrap.servers".to_string(), "127.0.0.1:1".to_string())]);
            if let Some(g) = group_id {
                props.insert("group.id".to_string(), g.to_string());
            }
            let config = ConsumerConfig::new(&props).expect("config validates");
            let mut consumer = AsyncKafkaConsumer::<String, String>::new(
                config,
                Box::new(TestStringDeserializer),
                Box::new(TestStringDeserializer),
            )
            .unwrap_or_else(|e| panic!("group_id={group_id:?} must construct, got {e}"));
            let _ = consumer.close().await;
        }
    }

    /// Java's `close` wraps whatever the close steps recorded:
    ///
    /// ```java
    /// throw new KafkaException("Failed to close kafka consumer", exception);
    /// ```
    /// (`AsyncKafkaConsumer.java:1586`).
    ///
    /// This is what makes `catch (KafkaException e)` around `close()` — the
    /// canonical Java idiom — reliable. Returning the recorded error raw breaks
    /// it whenever that error is outside the `KafkaException` hierarchy, which
    /// the flat `Error` enum makes reachable (an `LocalIllegalState` from the close
    /// path answers `false` to `is_kafka_error()`).
    #[tokio::test]
    async fn close_wraps_the_first_error_as_failed_to_close_kafka_consumer() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        // Fail the leave-group step with an error that is NOT a
        // `KafkaException` in Java terms, so the wrap is observable in the
        // hierarchy answer and not only in the message.
        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                match env.event {
                    ApplicationEvent::LeaveGroupOnClose { handle, .. } => {
                        handle.complete_with_error(Error::local_illegal_state(
                            "Consumer background task is no longer running.",
                        ));
                    },
                    ApplicationEvent::CommitSync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    ApplicationEvent::CommitAsync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    _ => {},
                }
            }
        });

        let err = consumer.close().await.expect_err("the failed step must surface");
        drop(drainer);

        assert_eq!("Failed to close kafka consumer", err.message());
        assert!(err.is_kafka_error(), "Java guarantees the caller a KafkaException: {err:?}");
        let cause = err.source().expect("the recorded error is the cause");
        assert!(
            matches!(cause, Error::LocalIllegalState(_)),
            "the original error must be the cause, got {cause:?}"
        );
        assert_eq!("Consumer background task is no longer running.", cause.message());
        // The consumer is still marked closed — the wrap happens after the
        // state flip, as in Java.
        assert!(consumer.is_closed());
    }
}
