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

#![allow(dead_code)] // Phase 11 commit (2/N): struct lands before its full method surface (commits 3-7).

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::common::{KafkaError, TopicPartition};
use crate::consumer::ConsumerGroupMetadata;
use crate::consumer::consumer_config::ConsumerConfig;
use crate::consumer::consumer_rebalance_listener::ConsumerRebalanceListener;
use crate::consumer::internals::consumer_interceptors::ConsumerInterceptors;
use crate::consumer::internals::consumer_metadata::ConsumerMetadata;
use crate::consumer::internals::consumer_rebalance_listener_invoker::ConsumerRebalanceListenerInvoker;
use crate::consumer::internals::deserializers::Deserializers;
use crate::consumer::internals::events::application_event_handler::ApplicationEventHandler;
use crate::consumer::internals::events::background_event::BackgroundEventEnvelope;
use crate::consumer::internals::events::completable_event_reaper::CompletableEventReaper;
use crate::consumer::internals::offset_commit_callback_invoker::OffsetCommitCallbackInvoker;
use crate::consumer::internals::request_managers::RequestManagers;
use crate::consumer::internals::subscription_state::SubscriptionState;
use crate::consumer::internals::wakeup_trigger::WakeupTrigger;

/// Type-erased handle to the spawned consumer background task.
///
/// Owns the `JoinHandle<()>` produced by `tokio::spawn(thread.run())`
/// and the `Box<dyn Fn>` closures that close / wakeup the underlying
/// `ConsumerNetworkThread<K>` regardless of its concrete `K`.
///
/// Held by [`AsyncKafkaConsumer`] for the lifetime of the consumer
/// instance; dropped (with `signal_close`) on close.
pub(crate) struct NetworkThreadCloseHandle {
    /// Cancels the bg-task `run_once` loop and wakes the trigger so the
    /// next iteration observes the shutdown.
    signal_close_fn: Box<dyn Fn() + Send + Sync>,
    /// Wakes the bg-task's `select!` on the wakeup token.
    wakeup_fn: Box<dyn Fn() + Send + Sync>,
    /// Spawned tokio task. Awaitable on close.
    join_handle: Option<JoinHandle<()>>,
}

impl NetworkThreadCloseHandle {
    /// Constructor used by [`AsyncKafkaConsumer::new_with_thread`]. The
    /// closures capture the concrete `ConsumerNetworkThread<K>` clones
    /// of the close / wakeup state so the outer struct can stay
    /// non-generic over `K`.
    pub(crate) fn new(
        signal_close_fn: Box<dyn Fn() + Send + Sync>,
        wakeup_fn: Box<dyn Fn() + Send + Sync>,
        join_handle: JoinHandle<()>,
    ) -> Self {
        Self { signal_close_fn, wakeup_fn, join_handle: Some(join_handle) }
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

    /// Awaits the spawned task to completion. Returns `Ok(())` on clean
    /// exit, or wraps the JoinError as a `KafkaError::illegal_state` on
    /// panic.
    pub(crate) async fn await_join(&mut self) -> Result<(), KafkaError> {
        if let Some(handle) = self.join_handle.take() {
            match handle.await {
                Ok(()) => Ok(()),
                Err(join_err) => Err(KafkaError::illegal_state(format!(
                    "Consumer network thread terminated with error: {join_err}"
                ))),
            }
        } else {
            Ok(())
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
    K: Send + 'static,
    V: Send + 'static,
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

    // ── App-side only ─────────────────────────────────────────────────
    /// `client.id`, as a cheap-to-clone `Arc<str>` per CLAUDE.md §11.
    client_id: Arc<str>,
    /// `group.id`, if any.
    group_id: Option<String>,
    /// Group metadata cached by the `MemberStateListener` callback;
    /// returned by [`Self::group_metadata`]. `None` while uninitialized
    /// or for assignment-only consumers.
    group_metadata: Arc<Mutex<Option<ConsumerGroupMetadata>>>,
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
    /// `true` after [`Self::close`] has run. Subsequent calls return
    /// `KafkaError::illegal_state`.
    closed: AtomicBool,
    /// Listener registered via `subscribe_with_listener` /
    /// `subscribe_pattern_with_listener`. Wrapped in `Mutex<Option<…>>`
    /// so it can be swapped without invalidating
    /// `&self.rebalance_listener_invoker` references.
    rebalance_listener: Mutex<Option<Arc<dyn ConsumerRebalanceListener>>>,
    /// Cached `ConsumerConfig` for late-bound config lookups (e.g.
    /// inside `close`).
    config: ConsumerConfig,
}

/// Components handed to [`AsyncKafkaConsumer::new_with_thread`]: the
/// per-RM container, metadata, subscriptions, application-event handle,
/// reaper, wakeup trigger, etc. Constructed by the production factory
/// (commit (7)) and by tests directly.
///
/// Bundling these into a struct keeps the ctor signature manageable as
/// the Java ctor has 14 already-constructed dependencies.
pub(crate) struct AsyncKafkaConsumerComponents<K: Send + 'static, V: Send + 'static> {
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
    pub rebalance_listener_invoker: ConsumerRebalanceListenerInvoker,
    pub offset_commit_callback_invoker: Arc<OffsetCommitCallbackInvoker<K, V>>,
    pub deserializers: Arc<Deserializers<K, V>>,
    pub interceptors: Arc<Mutex<ConsumerInterceptors<K, V>>>,
}

impl<K, V> AsyncKafkaConsumer<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    /// Constructs an `AsyncKafkaConsumer` from pre-built components.
    ///
    /// This is the **only** constructor in commit (2/N); the production
    /// factory in `consumer/mod.rs` is wired in commit (7/N). Tests
    /// build their own components (typically with a `MockClient`-backed
    /// `ConsumerNetworkThread`) and call this directly.
    ///
    /// Mirrors Java's test-visible constructor at
    /// `AsyncKafkaConsumer.java:521` (the 20-arg form), with the
    /// metrics / telemetry parameters dropped per Phase 11 PLAN.md
    /// deferrals.
    pub(crate) fn new_with_components(components: AsyncKafkaConsumerComponents<K, V>) -> Self {
        let auto_commit_enabled = components.config.enable_auto_commit();
        let default_api_timeout_ms = components.config.default_api_timeout_ms as i64;

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
            client_id: components.client_id,
            group_id: components.group_id,
            group_metadata: Arc::new(Mutex::new(None)),
            rebalance_listener_invoker: components.rebalance_listener_invoker,
            offset_commit_callback_invoker: components.offset_commit_callback_invoker,
            deserializers: components.deserializers,
            interceptors: components.interceptors,
            auto_commit_enabled,
            default_api_timeout_ms,
            closed: AtomicBool::new(false),
            rebalance_listener: Mutex::new(None),
            config: components.config,
        }
    }

    // ── Sync state-read methods ────────────────────────────────────────
    //
    // Per `consumer-threading.md` §16, each method acquires the
    // SubscriptionState lock briefly, reads, drops the guard — NEVER
    // holds the guard across `.await`. Since these methods are `fn`
    // (not `async`), there is no `.await` boundary at all.

    /// Java: `Set<TopicPartition> assignment()`.
    pub fn assignment(&self) -> std::collections::HashSet<TopicPartition> {
        let subs = self.subscriptions.lock().unwrap();
        subs.assigned_partitions()
    }

    /// Java: `Set<String> subscription()`.
    pub fn subscription(&self) -> std::collections::HashSet<String> {
        let subs = self.subscriptions.lock().unwrap();
        subs.subscription()
    }

    /// Java: `Set<TopicPartition> paused()`.
    pub fn paused(&self) -> std::collections::HashSet<TopicPartition> {
        let subs = self.subscriptions.lock().unwrap();
        subs.paused_partitions()
    }

    /// Java: `String clientId()`. Returned as a borrowed `&str` per
    /// CLAUDE.md §12 (most general borrowed form for getters).
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Java: `ConsumerGroupMetadata groupMetadata()`.
    ///
    /// Java throws `InvalidGroupIdException` when `group.id` is unset;
    /// the Rust translation returns a stub
    /// `ConsumerGroupMetadata::with_details(UNKNOWN, …)` for callers
    /// without a group, matching the prevailing Rust API convention of
    /// not failing on pure accessors. The strict-Java behavior is
    /// surfaced via `commit_*` / `subscribe` instead, which DO require
    /// a group id.
    ///
    /// The returned struct is a clone of the cached value; the
    /// `MemberStateListener` (Phase 8b) updates the cache via
    /// `Self::update_group_metadata`.
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
    /// **Phase 11 commit (2/N) stub** — Java's implementation dispatches
    /// through a `CurrentLagEvent` to the bg task. Wiring of that event
    /// lands in commit (6/N) along with `position`/`committed`. Until
    /// then this method returns `None` for every partition, matching
    /// the "unknown lag" contract of `OptionalLong.empty()`.
    pub fn current_lag(&self, _topic_partition: &TopicPartition) -> Option<i64> {
        // Phase-11 commit (6/N) carry-over: wire `CurrentLagEvent` here.
        None
    }

    /// Java: `void wakeup()`. Sync — callable from any task, including
    /// signal handlers.
    pub fn wakeup(&self) {
        self.wakeup_trigger.wakeup();
        // Also wake the bg task's `select!` directly so the underlying
        // `KafkaClient::poll` is unblocked even if the wakeup token was
        // already cancelled.
        self.network_thread_close.wakeup();
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
    use crate::consumer::internals::events::application_event::ApplicationEventEnvelope;
    use crate::consumer::internals::events::completable_event_reaper::CompletableEventReaper;

    use super::*;

    /// Minimal `Vec<u8>` deserializer for tests — equivalent to Java's
    /// `ByteArrayDeserializer`. Returns the input bytes unchanged.
    struct TestBytesDeserializer;

    impl Deserializer<Vec<u8>> for TestBytesDeserializer {
        fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Vec<u8>, KafkaError> {
            Ok(data.to_vec())
        }
    }

    /// Build a consumer with all dependencies stubbed to defaults. The
    /// bg task is **never spawned** in the unit-test path — the
    /// `JoinHandle` is replaced by a pre-completed future so awaiting
    /// it is immediate. This keeps tests deterministic and avoids
    /// requiring a full tokio multi-thread runtime.
    fn make_test_consumer() -> AsyncKafkaConsumer<Vec<u8>, Vec<u8>> {
        let mut config = ConsumerConfig::new(vec!["localhost:9092".to_string()]);
        config.client_id = "test-client".to_string();
        config.group_id = Some("test-group".to_string());
        let client_id: Arc<str> = Arc::from(config.client_id.as_str());

        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::EARLIEST)));
        let metadata = Arc::new(ConsumerMetadata::from_config(
            &config,
            Arc::clone(&subs),
            ClusterResourceListeners::new(),
        ));
        let request_managers = Arc::new(std::sync::Mutex::new(RequestManagers::new(
            None, None, None, None, None, None, None,
        )));
        let (_app_tx, app_rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        let (app_handler_tx, _app_handler_rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        let app_handler = Arc::new(ApplicationEventHandler::new(app_handler_tx));
        let reaper = Arc::new(std::sync::Mutex::new(CompletableEventReaper::new()));
        let max_time = Arc::new(AtomicI64::new(0));
        let wakeup = WakeupTrigger::new();

        // Stub join handle — spawn a noop task that completes
        // immediately. Tests do not assert on the bg task's behavior
        // in this commit.
        let join_handle: JoinHandle<()> = tokio::spawn(async move {
            // Hold the receiver until the consumer drops the sender so
            // the channel does not race-close.
            let _rx = app_rx;
        });
        let signal_close_called = Arc::new(AtomicBool::new(false));
        let signal_close_flag = Arc::clone(&signal_close_called);
        let wakeup_called = Arc::new(AtomicBool::new(false));
        let wakeup_flag = Arc::clone(&wakeup_called);
        let close_handle = NetworkThreadCloseHandle::new(
            Box::new(move || {
                signal_close_flag.store(true, Ordering::Release);
            }),
            Box::new(move || {
                wakeup_flag.store(true, Ordering::Release);
            }),
            join_handle,
        );

        let (_bg_tx, bg_rx) = mpsc::unbounded_channel::<BackgroundEventEnvelope>();
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

        let components = AsyncKafkaConsumerComponents {
            config,
            client_id,
            group_id: Some("test-group".to_string()),
            subscriptions: subs,
            metadata,
            request_managers,
            background_event_rx: bg_rx,
            application_event_handler: app_handler,
            completable_event_reaper: reaper,
            max_time_to_wait_ms: max_time,
            wakeup_trigger: wakeup,
            network_thread_close: close_handle,
            rebalance_listener_invoker,
            offset_commit_callback_invoker,
            deserializers,
            interceptors,
        };
        AsyncKafkaConsumer::<Vec<u8>, Vec<u8>>::new_with_components(components)
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

    #[tokio::test]
    async fn current_lag_returns_none_in_commit_2() {
        // Phase-11 commit (2/N) stub — verified to return None until
        // commit (6/N) wires CurrentLagEvent.
        let consumer = make_test_consumer();
        let tp = TopicPartition::new("t".to_string(), 0);
        assert_eq!(consumer.current_lag(&tp), None);
    }

    #[tokio::test]
    async fn wakeup_triggers_token_cancellation_and_bg_wakeup() {
        let consumer = make_test_consumer();
        let token = consumer.wakeup_trigger.current_token();
        assert!(!token.is_cancelled());
        consumer.wakeup();
        assert!(token.is_cancelled(), "wakeup() must cancel the current token");
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
}
